use crate::config::Fhir;
use crate::fhir::mapper::EntryRequestType::{ConditionalCreate, UpdateAsCreate};
use crate::fhir::mapper::{
    MappingError, MessageAccessError, MessageType, bundle_entry, conditional_reference,
    message_type, parse_component, parse_date, parse_datetime, parse_field, parse_subcomponents,
    patch_bundle_entry,
};
use anyhow::anyhow;
use fhir_model::BuilderError;
use fhir_model::r4b::codes::{AddressType, AdministrativeGender, IdentifierUse, NameUse};
use fhir_model::r4b::resources::{
    BundleEntry, ParametersParameter, ParametersParameterValue, PatientDeceased,
    PatientMultipleBirth, ResourceType,
};
use fhir_model::r4b::resources::{Parameters, Patient};
use fhir_model::r4b::types::{
    Address, CodeableConcept, Coding, Extension, FieldExtension, Reference,
};
use fhir_model::r4b::types::{ExtensionValue, HumanName};
use fhir_model::r4b::types::{Identifier, Meta};
use hl7_parser::Message;
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
    Ok(Identifier::builder()
        .r#use(IdentifierUse::Usual)
        .system(config.person.system.to_owned())
        .value(
            parse_field(msg, "PID", 2)?
                .ok_or(MappingError::Other(anyhow!("empty pid value PID.2")))?,
        )
        .build()?)
}

fn map_patient(msg: &Message, config: &Fhir) -> Result<Patient, MappingError> {
    // patient resource
    let mut patient = Patient::builder()
        .meta(
            Meta::builder()
                .profile(vec![Some(config.person.profile.to_owned())])
                .build()?,
        )
        .identifier(vec![Some(create_patient_identifier(msg, config)?)])
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

    enum MultiBirthFlags {
        Yes,
        No,
        None,
        Unsupported,
    }

    let multi_birth_flag: MultiBirthFlags = match is_multi_birth {
        Some(is_multi_birth) => match is_multi_birth.clone().as_str() {
            "J" => MultiBirthFlags::Yes,
            "N" => MultiBirthFlags::No,
            _ => MultiBirthFlags::Unsupported,
        },
        None => MultiBirthFlags::None,
    };

    match (multi_birth_flag, multi_birth_number) {
        // nur Mehrlingsgeburt-Kennung vorhanden
        (multi_birth_flag, None) => match multi_birth_flag {
            MultiBirthFlags::Yes => Ok(Some(PatientMultipleBirth::Boolean(true))),
            MultiBirthFlags::No => Ok(Some(PatientMultipleBirth::Boolean(false))),
            MultiBirthFlags::None => Ok(None),
            MultiBirthFlags::Unsupported => Err(MappingError::MessageContentUnexpected {
                field: "Multibirth flag PID.24".to_string(),
                expected_value: "`Y` or `N` or empty".to_string(),
            }),
        },
        // Mehrlingsgeburt nein aber Anzahl vorhanden -> Widersprüchlich!
        (MultiBirthFlags::No, Some(_)) => Err(MappingError::MessageContentUnexpected {
            field: "Multibirth flag PID.24".to_string(),
            expected_value: "`Y`".to_string(),
        }),
        // beides vorhanden - Reihenfolge plus Mehrlingsgeburt-Kennung
        (MultiBirthFlags::Yes, Some(multi_birth_number)) => {
            match multi_birth_number.parse::<i32>() {
                Ok(number) => Ok(Some(PatientMultipleBirth::Integer(number))),
                Err(e) => Err(MappingError::MessageContentUnexpected {
                    field: "Birth number PID.25".to_string(),
                    expected_value: "`Integer value`".to_string(),
                }),
            }
        }
        _ => Ok(None),
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

fn field_extension(url: String, ext_value: ExtensionValue) -> Result<FieldExtension, BuilderError> {
    FieldExtension::builder()
        .extension(vec![
            Extension::builder().url(url).value(ext_value).build()?,
        ])
        .build()
}

#[cfg(test)]
mod tests {
    use crate::config::{FallConfig, Fhir, ResourceConfig};
    use crate::fhir::patient::{create_patient_merge, map_multiple_birth};
    use fhir_model::r4b::resources::{
        ParametersParameter, ParametersParameterValue, PatientMultipleBirth, ResourceType,
    };
    use fhir_model::r4b::types::Reference;
    use hl7_parser::Message;
    use rstest::rstest;

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
    #[case("N", "12")]
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
        let config = Fhir {
            person: ResourceConfig {
                profile: Default::default(),
                system: "https://fhir.diz.uni-marburg.de/sid/patient-id".to_string(),
            },
            fall: FallConfig::default(),
        };

        let msg =
            Message::parse_with_lenient_newlines(r#"MSH|^~\&|ORBIS|KH|WEBEPA|KH|20230912105234||ADT^A40^ADT_A39|12345678|P|2.5||123456789|NE|NE||8859/1
EVN|A40|202309121052||00000_123456789|XXXXX|202309121052
PID|1|1234567|1234567||Musterfrau^Maxi^^^^^L|||F|||^^^^^^L||^ ^ ^^^^^^^^^|||U||||||||||DE||||N
MRG|09876543|||09876543|||Musterfrau^Maxi^^^^^L"#,true)
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
}
