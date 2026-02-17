use crate::config::Fhir;
use crate::fhir::mapper::EntryRequestType::{ConditionalCreate, Delete, UpdateAsCreate};
use crate::fhir::mapper::{
    MappingError, MessageAccessError, MessageType, bundle_entry, conditional_reference,
    get_repeat_value, message_type, parse_component, parse_date, parse_datetime, parse_field,
    parse_subcomponents, patch_bundle_entry, resource_ref,
};
use anyhow::anyhow;
use fhir_model::r4b::codes::{AddressType, AdministrativeGender, IdentifierUse, NameUse};
use fhir_model::r4b::resources::{
    BundleEntry, Organization, OrganizationBuilder, ParametersParameter, ParametersParameterValue,
    PatientDeceased, PatientMultipleBirth, ResourceType,
};
use fhir_model::r4b::resources::{Parameters, Patient};
use fhir_model::r4b::types::{
    Address, CodeableConcept, Coding, Extension, FieldExtension, Period, Reference,
};
use fhir_model::r4b::types::{ExtensionValue, HumanName};
use fhir_model::r4b::types::{Identifier, Meta};
use fhir_model::{BuilderError, Date};
use hl7_parser::Message;
use hl7_parser::message::{Field, Segment};
use log::{error, warn};
use regex::Regex;
use std::fmt::Debug;
use std::vec;

pub(super) fn map(msg: &Message, config: Fhir) -> Result<Vec<BundleEntry>, MappingError> {
    let msg_type = message_type(msg);

    match msg_type.map_err(MessageAccessError::MessageTypeError)? {
        MessageType::Admit
        | MessageType::Registration
        | MessageType::PreAdmit
        | MessageType::ChangeOutpatientToInpatient
        | MessageType::ChangeInpatientToOutpatient
        | MessageType::PatientUpdate => {
            let patient = map_patient(msg, &config)?;
            // update-as-create
            Ok(vec![bundle_entry(patient, UpdateAsCreate)?])
        }
        MessageType::Transfer | MessageType::Discharge | MessageType::ChangePersonData => {
            let patient = map_patient(msg, &config)?;
            // conditional-create
            Ok(vec![bundle_entry(patient, ConditionalCreate)?])
        }
        MessageType::PatientMerge | MessageType::MergePatientRecords => {
            // create fhir-patch
            let (identifier, patch) = create_patient_merge(msg, &config)?;
            Ok(vec![patch_bundle_entry(
                identifier,
                &ResourceType::Patient,
                &patch,
            )?])
        }
        // todo error?
        MessageType::CancelAdmitVisit
        | MessageType::CancelTransfer
        | MessageType::CancelDischarge
        | MessageType::PendingAdmit
        | MessageType::CancelPendingAdmit => {
            // ignore
            Ok(vec![])
        }
        MessageType::DeletePersonInformation => {
            let patient = map_patient(msg, &config)?;
            // delete
            Ok(vec![bundle_entry(patient, Delete)?])
        }
        other => Err(MappingError::from(anyhow!("Invalid message type: {other}"))),
    }
}

fn map_addresses(msg: &Message) -> Result<Vec<Option<Address>>, MappingError> {
    let mut res = vec![];

    if let Some(addr_field) = &parse_field(msg, "PID", 11)? {
        // for addr_field in fields {
        let mut addr = Address::builder().r#type(AddressType::Both).build()?;

        // line
        if let Some(lines) = parse_subcomponents(addr_field, 1).ok().flatten() {
            addr.line = lines.into_iter().map(Some).collect();
        }
        // city
        if let Some(city) = parse_component(addr_field, 3).ok().flatten() {
            addr.city = Some(city);
        }
        // postal code
        if let Some(postal_code) = parse_component(addr_field, 5).ok().flatten() {
            addr.postal_code = Some(postal_code);
        }
        // country
        if let Some(country) = parse_component(addr_field, 6).ok().flatten() {
            addr.country = Some(country);
        }

        res.push(Some(addr));
    }

    Ok(res)
}

fn create_patient_merge(
    msg: &Message,
    config: &Fhir,
) -> Result<(Parameters, Identifier), MappingError> {
    let params = Parameters::builder()
        .parameter(vec![Some(
            ParametersParameter::builder()
                .name("operation".to_string())
                .part(vec![
                    Some(
                        ParametersParameter::builder()
                            .name("type".to_string())
                            .value(ParametersParameterValue::Code("add".to_string()))
                            .build()?,
                    ),
                    Some(
                        ParametersParameter::builder()
                            .name("path".to_string())
                            .value(ParametersParameterValue::String(
                                ResourceType::Patient.to_string(),
                            ))
                            .build()?,
                    ),
                    Some(
                        ParametersParameter::builder()
                            .name("name".to_string())
                            .value(ParametersParameterValue::String("link".to_string()))
                            .build()?,
                    ),
                    Some(
                        ParametersParameter::builder()
                            .name("value".to_string())
                            .part(vec![
                                Some(
                                    ParametersParameter::builder()
                                        .name("other".to_string())
                                        .value(ParametersParameterValue::Reference(
                                            Reference::builder()
                                                .reference(conditional_reference(
                                                    &ResourceType::Patient,
                                                    &create_patient_identifier(msg, config)?,
                                                )?)
                                                .r#type(ResourceType::Patient.to_string())
                                                .build()?,
                                        ))
                                        .build()?,
                                ),
                                Some(
                                    ParametersParameter::builder()
                                        .name("type".to_string())
                                        .value(ParametersParameterValue::Code(
                                            "replaced-by".to_string(),
                                        ))
                                        .build()?,
                                ),
                            ])
                            .build()?,
                    ),
                ])
                .build()?,
        )])
        .build()?;

    Ok((
        params,
        Identifier::builder()
            .system(config.person.system.to_string())
            .value(parse_field(msg, "MRG", 1)?.ok_or(anyhow!(
                "Failed to map Patient merge: Missing MRG.1 segment"
            ))?)
            .build()?,
    ))
}

fn create_patient_identifier(msg: &Message, config: &Fhir) -> Result<Identifier, MappingError> {
    Identifier::builder()
        .r#use(IdentifierUse::Usual)
        .system(config.person.system.to_owned())
        .value(
            parse_field(msg, "PID", 2)?
                .ok_or(MappingError::Other(anyhow!("empty pid value PID.2")))?,
        )
        .assigner(
            Reference::builder()
                .display("UKGM -Universitätsklinikum Marburg".to_string())
                .identifier(
                    Identifier::builder()
                        .value(config.facility_id.to_string())
                        .system("http://fhir.de/sid/arge-ik/iknr".to_string())
                        .build()?,
                )
                .build()?,
        )
        .build()
        .map_err(MappingError::from)
}

/// Erzeugt Patienten-Identifier
///
/// * Ein PID-Identifier ist min. notwendig
/// * Zusätzlich werden weitere Identifier aus Gesundheitskassendaten *(IN1-Segmente)* erzeugt
///  werden, falls dies vorhanden sind.
///
/// _Hinweis:_ Es gibt HL7 Nachrichten, die in denen IN1 Segmente fehlen.
///
fn create_patient_identifiers(
    msg: &Message,
    config: &Fhir,
) -> Result<Vec<Option<Identifier>>, MappingError> {
    let mut res: Vec<Option<Identifier>>;

    // init result vector: one for PID and x for IN1 count
    let in1_count = msg.segment_count("IN1");
    if in1_count > 0 {
        res = Vec::with_capacity(in1_count + 1);
    } else {
        res = Vec::with_capacity(1);
    }

    // mandatory PID identifier
    match create_patient_identifier(msg, config) {
        Ok(pid_identifier) => res.push(Some(pid_identifier)),
        Err(err) =>
        // mandatory identifier - if it is missing, mapping patient does not make sense
        {
            return Err(err);
        }
    }

    // create optional identifiers from insurance data
    if in1_count > 0 {
        for idx in 1..in1_count + 1 {
            if let Some(segment_by_index) = msg.segment_n("IN1", idx) {
                match map_versicherungsdaten(segment_by_index, config)? {
                    Some(ident) => res.push(Some(ident)),
                    None => {}
                }
            }
        }
    }

    Ok(res)
}

fn map_patient(msg: &Message, config: &Fhir) -> Result<Patient, MappingError> {
    // patient resource
    let mut patient = Patient::builder()
        .meta(
            Meta::builder()
                .profile(vec![Some(config.person.profile.to_owned())])
                .build()?,
        )
        .identifier(create_patient_identifiers(msg, config)?)
        .address(map_addresses(msg)?)
        .name(map_name(msg)?)
        .build()?;

    // birth_date
    if let Some(b) = &parse_field(msg, "PID", 7)? {
        patient.birth_date = Some(parse_date(b)?)
    }
    // gender
    if let Some(g) = &parse_field(msg, "PID", 8)? {
        patient.gender = Some(map_gender(g));
    }
    // marital_status
    patient.marital_status = map_marital_status(msg)?;
    // deceased flag
    patient.deceased = map_deceased(msg)?;

    patient.multiple_birth = map_multiple_birth(msg)?;

    Ok(patient)
}

fn map_deceased(msg: &Message) -> Result<Option<PatientDeceased>, MappingError> {
    // patient vital status
    let death_time = parse_field(msg, "PID", 29)?;
    let death_confirm = parse_field(msg, "PID", 30)?;

    match (death_time, death_confirm) {
        (Some(death_time), _) => Ok(Some(PatientDeceased::DateTime(parse_datetime(
            death_time.as_str(),
        )?))),
        (None, Some(confirm)) => Ok(Some(PatientDeceased::Boolean(confirm == "Y"))),
        _ => Ok(None),
    }
}

fn map_multiple_birth(msg: &Message) -> Result<Option<PatientMultipleBirth>, MappingError> {
    let is_multi_birth = &parse_field(msg, "PID", 24)?;
    let multi_birth_number = &parse_field(msg, "PID", 25)?;
    let msg_id = &parse_field(msg, "MSH", 10)?;

    #[derive(Debug, PartialEq, Eq)]
    enum MultiBirthFlags {
        Yes,
        No,
        None,
        Unsupported(String),
    }

    let multi_birth_flag: MultiBirthFlags = match is_multi_birth {
        Some(is_multi_birth) => match is_multi_birth.as_str() {
            "J" => MultiBirthFlags::Yes,
            "N" => MultiBirthFlags::No,
            _ => MultiBirthFlags::Unsupported(is_multi_birth.to_string()),
        },
        None => MultiBirthFlags::None,
    };

    match (multi_birth_flag, multi_birth_number) {
        // nur Mehrlingsgeburt-Kennung vorhanden
        (multi_birth_flag, None) => match multi_birth_flag {
            MultiBirthFlags::Yes => Ok(Some(PatientMultipleBirth::Boolean(true))),
            MultiBirthFlags::No => Ok(Some(PatientMultipleBirth::Boolean(false))),
            MultiBirthFlags::None => Ok(None),
            MultiBirthFlags::Unsupported(some_value) => {
                warn!(
                    "MSG-ID {:?}: Unsupported multi-birth flag value '{:?}'!",
                    msg_id, some_value
                );
                Ok(None)
            }
        },

        (multi_birth_flag, Some(multi_birth_number)) => {
            match multi_birth_flag {
                MultiBirthFlags::No => warn!(
                    "MSH-ID {:?}: Multi-birth flag is 'N' but birth number is present!",
                    msg_id
                ),
                MultiBirthFlags::Yes => (),
                MultiBirthFlags::Unsupported(some_value) => {
                    warn!(
                        "MSH-ID {:?}: Multi-birth flag is '{:?}' but birth number is present!",
                        msg_id, some_value
                    )
                }
                MultiBirthFlags::None => warn!(
                    "MSH-ID {:?}: Multi-birth flag is empty but birth number is present!",
                    msg_id
                ),
            }

            match multi_birth_number.parse::<i32>() {
                Ok(number) => Ok(Some(PatientMultipleBirth::Integer(number))),
                Err(e) => Err(MappingError::Other(anyhow!(
                    "Invalid multi-birth number: {}",
                    e
                ))),
            }
        }
    }
}

fn map_marital_status(msg: &Message) -> Result<Option<CodeableConcept>, MappingError> {
    // marital status
    if let Some(status) = &parse_field(msg, "PID", 16)? {
        parse_component(status, 1)
            .map_err(MessageAccessError::from)?
            .map(|m| {
                match m.as_str() {
                    "A" | "E" => Coding::builder()
                        .system(
                            "http://terminology.hl7.org/CodeSystem/v3-MaritalStatus".to_string(),
                        )
                        .code("L".to_string())
                        .display("Legally Separated".to_string())
                        .build(),
                    "D" => Coding::builder()
                        .system(
                            "http://terminology.hl7.org/CodeSystem/v3-MaritalStatus".to_string(),
                        )
                        .code("D".to_string())
                        .display("Divorced".to_string())
                        .build(),
                    "M" => Coding::builder()
                        .system(
                            "http://terminology.hl7.org/CodeSystem/v3-MaritalStatus".to_string(),
                        )
                        .code("M".to_string())
                        .display("Married".to_string())
                        .build(),
                    "S" => Coding::builder()
                        .system(
                            "http://terminology.hl7.org/CodeSystem/v3-MaritalStatus".to_string(),
                        )
                        .code("S".to_string())
                        .display("Never Married".to_string())
                        .build(),
                    "W" => Coding::builder()
                        .system(
                            "http://terminology.hl7.org/CodeSystem/v3-MaritalStatus".to_string(),
                        )
                        .code("W".to_string())
                        .display("Widowed".to_string())
                        .build(),
                    "C" => Coding::builder()
                        .system(
                            "http://terminology.hl7.org/CodeSystem/v3-MaritalStatus".to_string(),
                        )
                        .code("C".to_string())
                        .display("Common Law".to_string())
                        .build(),
                    "G" | "P" | "R" => Coding::builder()
                        .system(
                            "http://terminology.hl7.org/CodeSystem/v3-MaritalStatus".to_string(),
                        )
                        .code("T".to_string())
                        .display("Domestic partner".to_string())
                        .build(),
                    "N" => Coding::builder()
                        .system(
                            "http://terminology.hl7.org/CodeSystem/v3-MaritalStatus".to_string(),
                        )
                        .code("A".to_string())
                        .display("Annulled".to_string())
                        .build(),
                    "I" => Coding::builder()
                        .system(
                            "http://terminology.hl7.org/CodeSystem/v3-MaritalStatus".to_string(),
                        )
                        .code("I".to_string())
                        .display("Interlocutory".to_string())
                        .build(),
                    "B" => Coding::builder()
                        .system(
                            "http://terminology.hl7.org/CodeSystem/v3-MaritalStatus".to_string(),
                        )
                        .code("U".to_string())
                        .display("Unmarried".to_string())
                        .build(),
                    _a => Coding::builder()
                        .system(
                            "http://terminology.hl7.org/CodeSystem/v3-MaritalStatus".to_string(),
                        )
                        .code("UNK".to_string())
                        .display("Unknown".to_string())
                        .build(),
                }
                .and_then(|c| CodeableConcept::builder().coding(vec![Some(c)]).build())
                .map_err(MappingError::from)
            })
            .transpose()
    } else {
        Ok(None)
    }
}

fn map_gender(gender: &str) -> AdministrativeGender {
    match gender {
        "F" => AdministrativeGender::Female,
        "M" => AdministrativeGender::Male,
        "U" => AdministrativeGender::Other,
        _ => AdministrativeGender::Unknown,
    }
}

fn map_name(v2_msg: &Message) -> Result<Vec<Option<HumanName>>, MappingError> {
    let mut names = vec![];

    if let Some(name_field) = &parse_field(v2_msg, "PID", 5)? {
        let name_use = parse_component(name_field, 7)
            .map_err(MessageAccessError::from)?
            .and_then(|u| match u.as_str() {
                "L" => Some(NameUse::Official),
                "M" | "B" => Some(NameUse::Maiden),
                _ => None,
            });

        let mut name = HumanName::builder()
            // todo: parse multiple names
            .given(
                parse_component(name_field, 2)
                    .map_err(MessageAccessError::from)?
                    .map(|e| vec![Some(e)])
                    .unwrap_or_default(),
            )
            .build()?;

        name.r#use = name_use;
        name.family = parse_component(name_field, 1).map_err(MessageAccessError::from)?;

        // prefix
        if let Some(prefix) = parse_component(name_field, 6).map_err(MessageAccessError::from)? {
            name.prefix = vec![Some(prefix)];
            name.prefix_ext = vec![Some(field_extension(
                "http://hl7.org/fhir/StructureDefinition/iso21090-EN-qualifier".into(),
                ExtensionValue::Code("AC".into()),
            )?)];
        }

        // namenszusatz
        if let Some(namenszusatz) =
            parse_component(name_field, 4).map_err(MessageAccessError::from)?
        {
            name.family_ext = Some(field_extension(
                "http://fhir.de/StructureDefinition/humanname-namenszusatz".into(),
                ExtensionValue::String(namenszusatz),
            )?);
        }

        // vorsatzwort
        if let Some(vorsatzwort) =
            parse_component(name_field, 5).map_err(MessageAccessError::from)?
        {
            name.family_ext = Some(field_extension(
                "http://hl7.org/fhir/StructureDefinition/humanname-own-prefix".into(),
                ExtensionValue::String(vorsatzwort),
            )?);
        }

        names.push(Some(name));

        // maiden name
        if let Some(maiden_name) = &parse_field(v2_msg, "PID", 6)? {
            names.push(Some(
                HumanName::builder()
                    .r#use(NameUse::Maiden)
                    .family(maiden_name.into())
                    .build()?,
            ))
        }
    }

    Ok(names)
}

static GKV10_VALID: once_cell::sync::Lazy<Regex> =
    once_cell::sync::Lazy::new(|| Regex::new(r"^[A-Z][0-9]{9}$").unwrap());

fn map_versicherungsdaten(
    in1: &Segment,
    config: &Fhir,
) -> Result<Option<Identifier>, MappingError> {
    // Versicherungsnummer
    let insurance_number = match in1.field(36) {
        Some(f) if !f.is_empty() => f.raw_value(),
        _ => return Ok(None),
    };

    let mut result = Identifier::builder()
        .value(insurance_number.to_string())
        .r#use(IdentifierUse::Official)
        .build()
        .map_err(MappingError::from)?;

    match get_repeat_value(in1, 3, 0, 1) {
        None => {
            println!("no insurance company id found - cannot add assigner")
        }
        Some(id) => {
            match resource_ref(
                &ResourceType::Organization,
                id.as_str(),
                "http://fhir.de/sid/arge-ik/iknr",
            ) {
                Ok(reference) => result.assigner = Some(reference),
                Err(err) => {
                    error! {"{}", err}
                }
            };
        }
    };

    if GKV10_VALID.is_match(insurance_number) {
        // GKV
        result.system = Some("http://fhir.de/sid/gkv/kvid-10".to_string());
        result.r#type = Some(
            CodeableConcept::builder()
                .coding(vec![Some(
                    Coding::builder()
                        .code("KVZ10".to_string())
                        .system("http://fhir.de/CodeSystem/identifier-type-de-basis".to_string())
                        .build()?,
                )])
                .build()?,
        );
    } else {
        // OTHER INSURANCE NUMBER! vor 2012 waren 9 - 12 Stellen ohne führenden Buchstaben valide.
        result.system = Some(config.person.other_insurance_system.to_string());
    }

    match try_set_identifier_period(in1, &mut result) {
        Ok(_) => {}
        Err(map_err) => return Err(map_err),
    }

    Ok(Some(result))
}

fn try_set_identifier_period(in1: &Segment, result: &mut Identifier) -> Result<bool, MappingError> {
    // Gültigkeitszeitraum
    let start = match in1
        .field(12)
        .filter(|f| !f.is_empty())
        .map(|f| parse_date(f.raw_value()))
        .transpose()
    {
        Ok(Some(start)) => Some(start),

        Err(e) => return Err(MappingError::FormattingError(e)),

        Ok(None) => None,
    };

    let end = match in1
        .field(13)
        .filter(|f| !f.is_empty())
        .map(|f| parse_date(f.raw_value()))
        .transpose()
    {
        Ok(Some(start)) => Some(start),

        Err(e) => return Err(MappingError::FormattingError(e)),

        Ok(None) => None,
    };

    if start.is_some() || end.is_some() {
        let mut period = Period::builder().build()?;

        if let Some(start) = start {
            period.start = Some(fhir_model::DateTime::Date(start))
        }

        if let Some(end) = end {
            period.end = Some(fhir_model::DateTime::Date(end))
        }

        result.period = Some(period);
    }
    Ok(true)
}
fn field_extension(url: String, ext_value: ExtensionValue) -> Result<FieldExtension, BuilderError> {
    FieldExtension::builder()
        .extension(vec![
            Extension::builder().url(url).value(ext_value).build()?,
        ])
        .build()
}

#[cfg(test)]
mod tests {
    use crate::config::{FallConfig, Fhir, PatientConfig, ResourceConfig};
    use crate::fhir::mapper::MappingError;
    use crate::fhir::patient::{
        create_patient_identifiers, create_patient_merge, map, map_multiple_birth,
        map_versicherungsdaten, try_set_identifier_period,
    };
    use fhir_model::r4b::codes::HTTPVerb::Delete;
    use fhir_model::r4b::resources::{
        BundleEntryRequest, ParametersParameter, ParametersParameterValue, PatientMultipleBirth,
        ResourceType,
    };
    use fhir_model::r4b::types::{Coding, Identifier, Reference};
    use fhir_model::time::Month;
    use fhir_model::{Date, DateTime, time};
    use hl7_parser::message::Segment;
    use hl7_parser::{Message, parser};
    use rstest::rstest;
    use std::fmt::Debug;

    #[test]
    fn test_multibirth_empty() {
        let msg = Message::parse_with_lenient_newlines(r#"MSH|^~\&|ORBIS|KH|WEBEPA|KH|20251102212117||ADT^A08^ADT_A01|12332112|P|2.5||123788998|NE|NE||8859/1
EVN|A08|202511022120||11036_123456789|ZZZZZZZZ|202511022120
PID|1|9999999|9999999|88888888|Nachname^SäuglingVorname^^^^^L||202511022120|M|||Strasse. 1&Strasse.&1^^Stadt^^30000^DE^L~^^Stadt^^^^BDL||0000000000000^PRN^PH^^^00000^0000000^^^^^000000000000|||U|||||12345678^^^KH^VN~1234567^^^KH^PT||Stadt|||DE||||N"#, true).unwrap();
        let actual = map_multiple_birth(&msg).unwrap();
        assert_eq!(actual, None);
    }

    #[rstest]
    #[case("J", "1", None)]
    #[case("J", "2", None)]
    #[case("J", "", Some(true))]
    #[case("N", "", Some(false))]
    #[case("N", "1", None)]
    #[case("O", "1", None)]
    #[case("", "1", None)]
    fn test_multibirth_number_ok(
        #[case] multibirth_flag: String,
        #[case] multibirth_num: String,
        #[case] expect_bool_result: Option<bool>,
    ) {
        let input = format!(
            r#"MSH|^~\&|ORBIS|KH|WEBEPA|KH|20251102212117||ADT^A08^ADT_A01|12332112|P|2.5||123788998|NE|NE||8859/1
EVN|A08|202511022120||11036_123456789|ZZZZZZZZ|202511022120
PID|1|9999999|9999999|88888888|Nachname^SäuglingVorname^^^^^L||202511022120|M|||Strasse. 1&Strasse.&1^^Stadt^^30000^DE^L~^^Stadt^^^^BDL||0000000000000^PRN^PH^^^00000^0000000^^^^^000000000000|||U|||||12345678^^^KH^VN~1234567^^^KH^PT||Stadt|{}|{}|DE||||N"#,
            multibirth_flag, multibirth_num
        );
        let msg = Message::parse_with_lenient_newlines(&input, true).unwrap();
        let actual = map_multiple_birth(&msg).unwrap().unwrap();

        match expect_bool_result {
            Some(true) => {
                assert_eq!(actual, PatientMultipleBirth::Boolean(true));
            }
            Some(false) => {
                assert_eq!(actual, PatientMultipleBirth::Boolean(false));
            }
            None => {
                assert_eq!(
                    actual,
                    PatientMultipleBirth::Integer(multibirth_num.parse().unwrap())
                );
            }
        }
    }

    #[rstest]
    #[case("J", "a")]
    #[should_panic]
    fn test_multibirth_number_fail(
        #[case] multibirth_flag: String,
        #[case] multibirth_num: String,
    ) {
        let input = format!(
            r#"MSH|^~\&|ORBIS|KH|WEBEPA|KH|20251102212117||ADT^A08^ADT_A01|12332112|P|2.5||123788998|NE|NE||8859/1
EVN|A08|202511022120||11036_123456789|ZZZZZZZZ|202511022120
PID|1|9999999|9999999|88888888|Nachname^SäuglingVorname^^^^^L||202511022120|M|||Strasse. 1&Strasse.&1^^Stadt^^30000^DE^L~^^Stadt^^^^BDL||0000000000000^PRN^PH^^^00000^0000000^^^^^000000000000|||U|||||12345678^^^KH^VN~1234567^^^KH^PT||Stadt|{}|{}|DE||||N"#,
            multibirth_flag, multibirth_num
        );
        let msg = Message::parse_with_lenient_newlines(&input, true).unwrap();
        let actual = map_multiple_birth(&msg).unwrap().unwrap();

        assert_eq!(actual, PatientMultipleBirth::Integer(1));
    }

    #[test]
    fn test_create_patient_merge() {
        let config = test_config();

        let msg =
                Message::parse_with_lenient_newlines(r#"MSH|^~\&|ORBIS|KH|WEBEPA|KH|20230912105234||ADT^A40^ADT_A39|12345678|P|2.5||123456789|NE|NE||8859/1
EVN|A40|202309121052||00000_123456789|XXXXX|202309121052
PID|1|1234567|1234567||Musterfrau^Maxi^^^^^L|||F|||^^^^^^L||^ ^ ^^^^^^^^^|||U||||||||||DE||||N
MRG|09876543|||09876543|||Musterfrau^Maxi^^^^^L"#, true)
                    .unwrap();

        // act
        let (params, _) = create_patient_merge(&msg, &config).unwrap();

        // get value parameters from result
        let values: Vec<ParametersParameter> = params
            .parameter
            .iter()
            .flatten()
            .filter_map(|p| {
                if p.name == "operation" {
                    Some(p.part.iter().flatten())
                } else {
                    None
                }
            })
            .flatten()
            .find_map(|p| {
                if p.name == "value" {
                    Some(p.part.clone().into_iter().flatten().collect())
                } else {
                    None
                }
            })
            .unwrap();

        let other = values.first().unwrap();
        let m_type = values.get(1).unwrap();

        assert_eq!(
                *other,
                ParametersParameter::builder()
                    .name("other".to_string())
                    .value(ParametersParameterValue::Reference(
                        Reference::builder()
                            .r#type(ResourceType::Patient.to_string())
                            .reference("Patient?identifier=https://fhir.diz.uni-marburg.de/sid/patient-id|1234567".to_string())
                            .build()
                            .unwrap()
                    ))
                    .build()
                    .unwrap()
            );

        assert_eq!(
            *m_type,
            ParametersParameter::builder()
                .name("type".to_string())
                .value(ParametersParameterValue::Code("replaced-by".to_string()))
                .build()
                .unwrap()
        );
    }
    #[test]
    fn test_delete_patient() {
        let config = test_config();

        let msg = Message::parse_with_lenient_newlines(r#"MSH|^~\&|ORBIS|KH|WEBEPA|KH|20221121142711||ADT^A29^ADT_A21|71546182|P|2.5||684450133|NE|NE||8859/1
EVN|A29|202211211427||12127_684450133|MEDCO-TOBL|202211211427
PID|1|1234567|1234567||Test-UCH^Endoprothese^^^^^L~Test^^^^^^B||19450201|M|||Baldinger Strasse&Baldinger Strasse^^Marburg^^35037^DE^L|||||S||||||||||DE||||N"#, true)
                .unwrap();

        let entry = map(&msg, config.clone()).unwrap();

        assert_eq!(
            entry.first().unwrap().request,
            Some(
                BundleEntryRequest::builder()
                    .url(format!(
                        "{}?identifier={}|1234567",
                        &ResourceType::Patient,
                        config.person.system
                    ))
                    .method(Delete)
                    .build()
                    .unwrap()
            )
        );
    }

    fn test_config() -> Fhir {
        Fhir {
            facility_id: "260620431".to_string(),
            person: PatientConfig {
                profile: Default::default(),
                system: "https://fhir.diz.uni-marburg.de/sid/patient-id".to_string(),
                other_insurance_system:
                    "https://fhir.diz.uni-marburg.de/sid/patient-other-insurance-id".to_string(),
            },
            fall: FallConfig::default(),
        }
    }

    #[test]
    fn test_map_versicherung_missing_insurance_number() {
        let msg = Message::parse_with_lenient_newlines(r#"MSH|^~\&|ORBIS||RECAPP|ORBIS|201111280725||ADT^A04|11657277|P|2.5|||||DE||DE
IN1|1||AOK HSA HESSEN|AOK - Die Gesundheitskasse in Hessen-|Musterstrasse 1^^Musterort^^66666^D||||AOK^1^^^1&gesetzlich||||||50001|Mustermann^Max||19500118|Mustergasse 10^^Musterort^^33333^D|||2|||||||201108220723||R||||||||||||M| ^^^^^D  |||||454874316^^^^^^^20150630"#, true).unwrap();
        let in1 = msg.segment("IN1").unwrap();

        let result = map_versicherungsdaten(in1, &test_config()).unwrap();

        // Assert
        assert!(result.is_none());
    }

    #[test]
    fn test_map_versicherungsdaten() {
        let msg = Message::parse_with_lenient_newlines(r#"MSH|^~\&|ORBIS||RECAPP|ORBIS|201111280725||ADT^A04|11657277|P|2.5|||||DE||DE
EVN|A04|201111280722|201111280722||TEST
PID|1|111111|111111||Mustermann^Max|Mustermann|19500118|M|||Mustergasse 10^^Musterort^^33333^DE||012345/12346^^PH|||M|kl|||||||N||DE
NK1|1|Fr. Müller, Miriam|14^Ehefrau| |s.Pat.
PV1|1|O|NEPPOLAMB^^^NEP^NEP^000000|R||||44444ARZT^Arzt^Hans Jürgen^^Praxis^^Dr. med.|44444ARZT^Arzt^Hans Jürgen^^Praxis^^Dr. med.|N||||||N|||20900000||K|||HSA||||||||||||||||9||||200703280736|||||||A
IN1|1||555555555^^^^NII~22222^^^^NIIP~AOK|AOK - Die Gesundheitskasse in Hessen-|Musterstrasse 1^^Musterort^^66666^D||||AOK^1^^^1&gesetzlich|||20020120|20091231||50001|Mustermann^Max||19500118|Mustergasse 10^^Musterort^^33333^D|||2|||||||201108220723||R|||||A454874316|||||||M| ^^^^^D  |||||A454874316^^^^^^^20150630
"#, true).unwrap();

        //helper_print_hl7_message(&msg);

        let result = &map_versicherungsdaten(msg.segment("IN1").unwrap(), &test_config())
            .unwrap()
            .unwrap();
        assert_eq!(result.value.as_ref().unwrap(), "A454874316");
        assert_eq!(
            result.system.as_ref().unwrap(),
            "http://fhir.de/sid/gkv/kvid-10"
        );
        assert_eq!(
            result.r#type.as_ref().unwrap().coding[0]
                .as_ref()
                .unwrap()
                .code,
            Some("KVZ10".to_string())
        );

        assert_eq!(
            result.r#type.as_ref().unwrap().coding[0]
                .as_ref()
                .unwrap()
                .system,
            Some("http://fhir.de/CodeSystem/identifier-type-de-basis".to_string())
        );

        // IN-12
        let expected_start =
            Date::Date(time::Date::from_calendar_date(2002, Month::January, 20).unwrap());
        if let DateTime::Date(actual) = result.period.as_ref().unwrap().start.as_ref().unwrap() {
            assert_eq!(&expected_start, actual);
        }
        // IN-13
        let expected_end =
            Date::Date(time::Date::from_calendar_date(2009, Month::December, 31).unwrap());
        if let DateTime::Date(actual) = result.period.as_ref().unwrap().end.as_ref().unwrap() {
            assert_eq!(&expected_end, actual);
        }

        match result.assigner.as_ref() {
            None => assert!(false, "assigner is expected"),
            Some(assigner_ref) => {
                assert_eq!(
                    "Organization?identifier=http://fhir.de/sid/arge-ik/iknr|555555555",
                    assigner_ref.reference.as_ref().unwrap()
                );
                //assert!(false, "assigner reference present but needs more testing.")
                //TODO! may be separate test
            }
        }
    }

    #[test]
    fn test_map_insurance_skip_none() {
        let msg = Message::parse_with_lenient_newlines(
            r#"MSH|^~\&|ORBIS||RECAPP|ORBIS|201111280725||ADT^A04|11657277|P|2.5|||||DE||DE
EVN|A04|201111280722|201111280722||TEST
PID|1|111111|111111||Mustermann^Max|Mustermann|19500118|M|||Mustergasse 10^^Musterort^^33333^DE||012345/12346^^PH|||M|kl|||||||N||DE
NK1|1|Fr. Müller, Miriam|14^Ehefrau| |s.Pat.
PV1|1|O|NEPPOLAMB^^^NEP^NEP^000000|R||||44444ARZT^Arzt^Hans Jürgen^^Praxis^^Dr. med.|44444ARZT^Arzt^Hans Jürgen^^Praxis^^Dr. med.|N||||||N|||20900000||K|||HSA||||||||||||||||9||||200703280736|||||||A
IN1|1||666666666^^^^NII~BG BAU MITTE^^^^XX|BG der Bauwirtschaft - BV Mitte|Viktoriastr. 21&Viktoriastr.&21^^Wuppertal^^42115^DE^L||12345612^PRN^PH^^^0000^3333^^^^^12345612~11111111111^PRN^FX^^^0000^1111111^^^^^11111111111||Träger der ges. Unfallversicherer^26^^^2&Berufsgenossenschaft^^NII~Träger der ges. Unfallversicherer^26^^^^^U|||||||Max^Mustermann||19620115|Musterstreasse. 1&Musterstreasse.&1^^Berlin^^10115^DE^L|||N|||||||||M||||||||||||M|Musterstreasse. 1&Musterstreasse.&1^^Berlin^^10115^DE^L
IN2|1||12345TES^TEST GmbH||||||||||||||||||||||||||^PC^0.0||||DE|||N|||kl|||||||Beruf-des-Pateinten|||||||||||||||||0123 45678|||||||Test GmbH
IN1|2||777777777^^^^NII~BG HM HAUPT^^^^XX|BGHM - Hauptverwaltung|Musterstreasse. 1&Musterstreasse.&1^^Berlin^^10115^DE^L||000000000001^PRN^PH^^^0000^0000^^^^^000000000001~1313131331313^PRN^FX^^^00000^00000000^^^^^1313131331313||Träger der ges. Unfallversicherer^26^^^2&Berufsgenossenschaft^^NII~Träger der ges. Unfallversicherer^26^^^^^U||||||10001|Max^Mustermann||19620115|Musterstreasse. 1&Musterstreasse.&1^^Berlin^^10115^DE^L|||H|||||||||M||||||||||||M|Musterstreasse. 1&Musterstreasse.&1^^Berlin^^10115^DE^L
IN2|2||12345TES^TEST GmbH||||||||||||||||||||||||||^PC^0.0||||DE|||N|||kl|||||||Beruf-des-Pateinten|||||||||||||||||0123 45678|||||||Test GmbH
IN1|3||8888888888^^^^NII~P DEMO^^^^XX|Krankenversicherung a.G.|Musterstreasse. 1&Musterstreasse.&1^^Berlin^^10115^DE^L||0000000-0^PRN^PH^^^0000^111-0^^^^^0000000-0~0000000-2913^PRN^FX^^^0000^111-2913^^^^^0000000-2913~^NET^Internet^info@email.de||Private^6^^^8&Private Krankenkasse^^NII~Private^6^^^^^U|||||||Max^Mustermann||19620115|Musterstreasse. 1&Musterstreasse.&1^^Berlin^^10115^DE^L|||N|||||||||P|||||123123123|||||||M|Musterstreasse. 1&Musterstreasse.&1^^Berlin^^10115^DE^L|||||123123123^^^^^^^0236
IN2|3|123123123|12345TES^TEST GmbH||||||||||||||||||||||||||||||DE|||N|||kl|||||||Beruf-des-Pateinten|||||||||||||||||0123 45678|||||||Test GmbH
IN1|4||SELBST^^^^XX|Selbstzahler|Musterstreasse. 1&Musterstreasse.&1^^Berlin^^10115^DE^L||00000000^PRN^PH^^^000^000^^^^^00000000~00000000000^PRN^CP^^^0000^0000000^^^^^00000000000||Sonstige^5^^^6&Selbstzahler^^NII~Sonstige^5^^^^^U|||||||Max^Mustermann||19620115|Musterstreasse. 1&Musterstreasse.&1^^Berlin^^10115^DE^L|||N|||J|20251207|||||P||||||||||||M|Musterstreasse. 1&Musterstreasse.&1^^Berlin^^10115^DE^L
IN2|4||12345TES^TEST GmbH||||||||||||||||||||||||||^PC^0.0||||DE|||N|||kl|||||||Beruf-des-Pateinten|||||||||||||||||0123 45678|||||||Test GmbH
"#,
            true,
        ).unwrap();

        let config = test_config();
        let identifiers = create_patient_identifiers(&msg, &config).unwrap();
        assert_eq!(identifiers.len(), 2);
    }

    ///
    /// print segments to console for better readability
    /// # Arguments
    ///
    /// * `msg`: parsed Hl7 message
    ///
    /// returns: ()
    fn print_hl7_message(msg: &Message) {
        let _z = &msg
            .segments
            .iter()
            .map(|s| {
                println!("Segment {}", s.name);
                for idx in 0..s.fields.len() {
                    if s.fields[idx].is_empty() {
                        println!("{}-{} = ''", s.name, idx + 1);
                    } else if s.fields[idx].repeats.is_empty() || s.fields[idx].repeats.len() == 1 {
                        println!("{}-{} = {}", s.name, idx + 1, s.fields[idx].raw_value());
                    } else {
                        for i in 0..s.fields[idx].repeats.len() {
                            println!(
                                "{}-{}.{} = {}",
                                s.name,
                                idx + 1,
                                i + 1,
                                s.fields[idx].repeats[i].raw_value()
                            )
                        }
                    }
                }
            })
            .count();
    }

    #[test]
    fn test_patient_multiple_insurance() {
        let msg_full = Message::parse_with_lenient_newlines(r#"MSH|^~\&|ORBIS||RECAPP|ORBIS|201111280725||ADT^A04|11657277|P|2.5|||||DE||DE
EVN|A04|201111280722|201111280722||TEST
PID|1|111111|111111||Mustermann^Max|Mustermann|19500118|M|||Mustergasse 10^^Musterort^^33333^DE||012345/12346^^PH|||M|kl|||||||N||DE
NK1|1|Fr. Müller, Miriam|14^Ehefrau| |s.Pat.
PV1|1|O|NEPPOLAMB^^^NEP^NEP^000000|R||||44444ARZT^Arzt^Hans Jürgen^^Praxis^^Dr. med.|44444ARZT^Arzt^Hans Jürgen^^Praxis^^Dr. med.|N||||||N|||20900000||K|||HSA||||||||||||||||9||||200703280736|||||||A
IN1|1|105313145|AOK HESSEN|AOK Hessen|^^Marburg^^35039^D||||AOK^1^^^1&gesetzlich||||||50001|||||||1|||||||||R|||||454874316|||||||U|
IN2|1||||||||||||||||||||||||||||^PC^0^K
IN1|2||AOK HSA HESSEN|AOK - Die Gesundheitskasse in Hessen-|Musterstrasse 1^^Musterort^^66666^D||||AOK^1^^^1&gesetzlich||||20091231||50001|Mustermann^Max||19500118|Mustergasse 10^^Musterort^^33333^D|||2|||||||201108220723||R|||||K454874316|||||||M| ^^^^^D  |||||K454874316^^^^^^^20150630
IN2|2||R^Rentner||||||||||||||||||||||||||^PC^0^K"#, true).unwrap();
        let config = test_config();
        let identifiers = create_patient_identifiers(&msg_full, &config).unwrap();
        //print_hl7_message(&msg_full);
        assert_eq!(identifiers.len(), 3);

        assert_eq!(
            "454874316",
            identifiers[1].as_ref().unwrap().value.as_ref().unwrap()
        );
        assert_eq!(
            &test_config().person.other_insurance_system,
            identifiers[1].as_ref().unwrap().system.as_ref().unwrap()
        );

        assert_eq!(
            "K454874316",
            identifiers[2].as_ref().unwrap().value.as_ref().unwrap()
        );
        assert_eq!(
            "http://fhir.de/sid/gkv/kvid-10",
            identifiers[2].as_ref().unwrap().system.as_ref().unwrap()
        );
    }

    #[rstest]
    #[case("", "")]
    #[case("20260101", "20260101")]
    #[case("20260101", "")]
    #[case("", "20260101")]
    fn test_try_set_identifier_period(#[case] start_date: String, #[case] end_date: String) {
        let input = format!(
            r#"MSH|^~\&|ORBIS|KH|WEBEPA|KH|20251102212117||ADT^A08^ADT_A01|12332112|P|2.5||123788998|NE|NE||8859/1
IN1|1||AOK HSA HESSEN|AOK - Die Gesundheitskasse in Hessen-|Musterstrasse 1^^Musterort^^66666^D||||AOK^1^^^1&gesetzlich|||{}|{}||50001|Mustermann^Max||19500118|Mustergasse 10^^Musterort^^33333^D|||2|||||||201108220723||R||||||||||||M| ^^^^^D  |||||454874316^^^^^^^20150630"#,
            start_date, end_date
        );

        let msg = Message::parse_with_lenient_newlines(&input, true).unwrap();
        let in1 = msg.segment("IN1").unwrap();

        let mut ident = Identifier::builder().build().unwrap();

        match try_set_identifier_period(&in1, &mut ident) {
            Ok(_) => {
                assert!(true, "not expecting therfore ok!");

                if (start_date.is_empty() && end_date.is_empty()) {
                    assert!(ident.period.is_none())
                } else {
                    match start_date.is_empty() {
                        true => {
                            assert!(ident.period.as_ref().unwrap().start.is_none())
                        }
                        false => {
                            assert!(ident.period.as_ref().unwrap().start.is_some())
                        }
                    }

                    match end_date.is_empty() {
                        true => {
                            assert!(ident.period.as_ref().unwrap().end.is_none())
                        }
                        false => {
                            assert!(ident.period.as_ref().unwrap().end.is_some())
                        }
                    }
                }
            }
            Err(_) => {
                assert!(false, "was not expecting error but found one!")
            }
        }
    }

    #[rstest]
    #[case("20263101", "20260101")]
    #[case("20260101", "20263141")]
    fn test_try_set_identifier_period_expect_error(
        #[case] start_date: String,
        #[case] end_date: String,
    ) {
        let input = format!(
            r#"MSH|^~\&|ORBIS|KH|WEBEPA|KH|20251102212117||ADT^A08^ADT_A01|12332112|P|2.5||123788998|NE|NE||8859/1
IN1|1||AOK HSA HESSEN|AOK - Die Gesundheitskasse in Hessen-|Musterstrasse 1^^Musterort^^66666^D||||AOK^1^^^1&gesetzlich|||{}|{}||50001|Mustermann^Max||19500118|Mustergasse 10^^Musterort^^33333^D|||2|||||||201108220723||R||||||||||||M| ^^^^^D  |||||454874316^^^^^^^20150630"#,
            start_date, end_date
        );

        let msg = Message::parse_with_lenient_newlines(&input, true).unwrap();
        let in1 = msg.segment("IN1").unwrap();

        let mut ident = Identifier::builder().build().unwrap();

        match try_set_identifier_period(&in1, &mut ident) {
            Ok(_) => {
                assert!(false, "expecting error but found none!")
            }
            Err(FormattingError) => assert!(true),
            Err(e) => assert!(false, "was expecting 'ForamttingError'"),
        }
    }
}
