use crate::config::Fhir;
use crate::fhir::mapper::FormattingError::DateFormatError;
use crate::fhir::mapper::{
    bundle_entry, hl7_field, message_type, parse_date_string_to_datetime, subject_ref,
    MappingError, MessageTypeError,
};
use crate::fhir::mapper::{MessageAccessError, MessageType};
use anyhow::anyhow;
use fhir_model::r4b::codes::{EncounterStatus, IdentifierUse};
use fhir_model::r4b::resources::{BundleEntry, Encounter, EncounterHospitalization};
use fhir_model::r4b::types::{CodeableConcept, Coding, Identifier, Meta, Period};
use fhir_model::DateTime;
use hl7_parser::Message;

pub(super) fn map_encounter(
    v2_msg: &Message,
    config: Fhir,
) -> Result<Vec<BundleEntry>, MappingError> {
    let r: Vec<BundleEntry> = vec![];

    match message_type(&v2_msg).map_err(MessageAccessError::MessageTypeError)? {
        MessageType::Admit
        | MessageType::Transfer
        | MessageType::Discharge
        | MessageType::Registration
        | MessageType::PreAdmit => {
            let enc_admit = map_einrichtungskontakt(v2_msg, &config)?;
            // todo
            // ...

            Ok(vec![bundle_entry(enc_admit)?])
        }
        MessageType::CancelAdmitVisit | MessageType::CancelPendingAdmit => {
            // todo
            Ok(r)
        }
        _ => Ok(r),
    }
}

fn map_einrichtungskontakt(msg: &Message, config: &Fhir) -> Result<Encounter, MappingError> {
    let admit = Encounter::builder()
        .meta(map_meta(config)?)
        .identifier(vec![
            Some(
                Identifier::builder()
                    .system(config.fall.einrichtungskontakt.system.clone())
                    .value(map_visit_number(msg)?)
                    .r#use(IdentifierUse::Secondary)
                    .build()?,
            ),
            // common identifier is last
            Some(
                Identifier::builder()
                    .system(config.fall.system.clone())
                    .value(map_visit_number(msg)?)
                    .r#use(IdentifierUse::Official)
                    .r#type(
                        CodeableConcept::builder()
                            .coding(vec![Some(
                                Coding::builder()
                                    .system(
                                        "http://terminology.hl7.org/CodeSystem/v2-0203".to_string(),
                                    )
                                    .code("VN".to_string())
                                    .build()?,
                            )])
                            .build()?,
                    )
                    .build()?,
            ),
        ])
        .status(map_encounter_status(msg).map_err(MessageAccessError::MessageTypeError)?)
        .class(map_encounter_class(msg)?)
        // Kontaktebene
        .r#type(vec![Some(
            CodeableConcept::builder()
                .coding(vec![Some(
                    Coding::builder()
                        .system("http://fhir.de/CodeSystem/Kontaktebene".to_string())
                        .code("einrichtungskontakt".to_string())
                        .display("Einrichtungskontakt".to_string())
                        .build()?,
                )])
                .build()?,
        )])
        // Aufnahmeanlass
        .hospitalization(map_admit_source(msg)?)
        .subject(subject_ref(msg, config.person.system.clone())?)
        // todo .service_provider()
        .period(map_period(msg)?)
        .build()?;

    Ok(admit)
}

fn map_admit_source(msg: &Message) -> Result<EncounterHospitalization, MappingError> {
    todo!()
}

fn map_period(msg: &Message) -> Result<Period, MappingError> {
    let start: DateTime = parse_date_string_to_datetime(hl7_field(msg, "PV1", 44)?.as_str())?
        .to_string()
        .parse()
        .map_err(DateFormatError)?;

    let period = Period::builder().start(start.clone());

    let p = match hl7_field(msg, "PV1", 45) {
        Ok(end) => period.end(
            parse_date_string_to_datetime(end.as_str())?
                .to_string()
                .parse()
                .map_err(DateFormatError)?,
        ),
        Err(_) => {
            match message_type(msg).map_err(MessageAccessError::MessageTypeError)? {
                // A04 has no end date is assigned start date instead
                MessageType::Registration => period.end(start),
                _ => period,
            }
        }
    };

    Ok(p.build()?)
}

fn map_encounter_status(msg: &Message) -> Result<EncounterStatus, MessageTypeError> {
    match message_type(msg)? {
        MessageType::Discharge => Ok(EncounterStatus::Finished),
        _ => Ok(EncounterStatus::InProgress),
    }
}

fn map_visit_number(msg: &Message) -> Result<String, anyhow::Error> {
    match message_type(msg)? {
        MessageType::PendingAdmit => Ok(hl7_field(msg, "PID", 4)?),
        _ => Ok(hl7_field(msg, "PV1", 19)?),
    }
}

fn map_meta(config: &Fhir) -> Result<Meta, anyhow::Error> {
    Ok(Meta::builder()
        .profile(vec![Some(config.fall.profile.clone())])
        // todo hl7 / orbis adt?
        .source("#orbis".to_string())
        .build()?)
}

fn map_encounter_class(msg: &Message) -> Result<Coding, anyhow::Error> {
    let code = hl7_field(msg, "PV1", 2)?;
    match code.as_str() {
        "I" => Ok(Coding::builder()
            .code("IMP".to_string())
            .system("http://terminology.hl7.org/CodeSystem/v3-ActCode".to_string())
            .display("inpatient encounter".to_string())
            .build()?),
        "O" => Ok(Coding::builder()
            .code("AMB".to_string())
            .system("http://terminology.hl7.org/CodeSystem/v3-ActCode".to_string())
            .display("ambulatory".to_string())
            .build()?),
        "P" => Ok(Coding::builder()
            .code("PRENC".to_string())
            .system("http://terminology.hl7.org/CodeSystem/v3-ActCode".to_string())
            .display("pre-admission".to_string())
            .build()?),
        // todo ...
        _ => Err(anyhow!("Invalid encounter_class code: {}", code)),
    }
}
