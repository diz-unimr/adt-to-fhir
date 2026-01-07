use crate::config::Fhir;
use crate::fhir::mapper::{
    bundle_entry, message_type, parse_component, parse_date, parse_datetime, parse_field,
    parse_subcomponents, MappingError, MessageAccessError, MessageType,
};
use anyhow::anyhow;
use fhir_model::r4b::codes::{AddressType, AdministrativeGender, IdentifierUse, NameUse};
use fhir_model::r4b::resources::Patient;
use fhir_model::r4b::resources::{BundleEntry, PatientDeceased};
use fhir_model::r4b::types::{Address, CodeableConcept, Coding, Extension, FieldExtension};
use fhir_model::r4b::types::{ExtensionValue, HumanName};
use fhir_model::r4b::types::{Identifier, Meta};
use fhir_model::BuilderError;
use hl7_parser::Message;
use std::vec;

pub(super) fn map(map: &Message, config: Fhir) -> Result<Vec<BundleEntry>, MappingError> {
    let msg_type = message_type(map);

    match msg_type.map_err(MessageAccessError::MessageTypeError)? {
        MessageType::Admit
        | MessageType::Registration
        | MessageType::PreAdmit
        | MessageType::ChangeOutpatientToInpatient
        | MessageType::ChangeInpatientToOutpatient
        | MessageType::PatientUpdate => {
            let patient = map_patient(map, &config)?;
            // todo: update-as-create
            Ok(vec![bundle_entry(patient)?])
        }
        MessageType::Transfer | MessageType::Discharge | MessageType::ChangePersonData => {
            let patient = map_patient(map, &config)?;
            // todo: conditional-create
            Ok(vec![bundle_entry(patient)?])
        }
        MessageType::PatientMerge | MessageType::MergePatientRecords => {
            let patient = map_patient(map, &config)?;
            // todo: create fhir-patch
            Ok(vec![bundle_entry(patient)?])
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
            addr.line = lines.into_iter().map(|l| Some(l)).collect();
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

fn map_patient(msg: &Message, config: &Fhir) -> Result<Patient, MappingError> {
    // patient resource
    let mut patient = Patient::builder()
        .meta(
            Meta::builder()
                .profile(vec![Some(config.person.profile.to_owned())])
                .build()?,
        )
        .identifier(vec![Some(
            Identifier::builder()
                .r#use(IdentifierUse::Usual)
                .system(config.person.system.to_owned())
                .value(
                    parse_field(msg, "PID", 2)?
                        .ok_or(MappingError::Other(anyhow!("empty pid value PID.2")))?,
                )
                .build()?,
        )])
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

    Ok(patient)
}

fn map_deceased(msg: &Message) -> Result<Option<PatientDeceased>, MappingError> {
    // patient vital status
    let death_time = parse_field(msg, "PID", 29)?;
    let death_confirm = parse_field(msg, "PID", 29)?;

    match (death_time, death_confirm) {
        (Some(death_time), _) => Ok(Some(PatientDeceased::DateTime(parse_datetime(
            death_time.as_str(),
        )?))),
        (None, Some(confirm)) => Ok(Some(PatientDeceased::Boolean(confirm == "Y"))),
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
            .given(
                parse_component(name_field, 2)
                    .map(|e| vec![e])
                    .map_err(MessageAccessError::from)?,
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
