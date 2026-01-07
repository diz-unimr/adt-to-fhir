use crate::config::Fhir;
use crate::fhir::mapper::{
    bundle_entry, message_type, parse_component, parse_datetime, parse_field, resource_ref,
    MappingError,
};
use crate::fhir::mapper::{MessageAccessError, MessageType};
use crate::fhir::resources::ResourceMap;
use anyhow::anyhow;
use fhir_model::r4b::codes::{EncounterStatus, IdentifierUse};
use fhir_model::r4b::resources::{BundleEntry, Encounter, EncounterHospitalization, ResourceType};
use fhir_model::r4b::types::{CodeableConcept, Coding, Identifier, Meta, Period, Reference};
use fhir_model::time::OffsetDateTime;
use fhir_model::DateTime;
use hl7_parser::Message;

pub(super) fn map(
    msg: &Message,
    config: Fhir,
    resources: &ResourceMap,
) -> Result<Vec<BundleEntry>, MappingError> {
    let r: Vec<BundleEntry> = vec![];

    if is_begleitperson(msg).is_ok_and(|v| v) {
        return Ok(r);
    }

    let msg_type = message_type(msg);
    match msg_type.map_err(MessageAccessError::MessageTypeError)? {
        MessageType::Admit
        | MessageType::Transfer
        | MessageType::Discharge
        | MessageType::Registration
        | MessageType::PreAdmit => {
            let enc_admit = map_einrichtungskontakt(msg, &config, resources)?;
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

fn is_begleitperson(msg: &Message) -> Result<bool, MessageAccessError> {
    Ok(parse_field(msg, "PV1", 2)?.is_some_and(|f| f == "H"))
}

fn map_einrichtungskontakt(
    msg: &Message,
    config: &Fhir,
    resources: &ResourceMap,
) -> Result<Encounter, MappingError> {
    let fab = parse_fab(msg)?;

    let mut admit = Encounter::builder()
        .meta(map_meta(config)?)
        .identifier(vec![
            Some(
                Identifier::builder()
                    .system(config.fall.einrichtungskontakt.system.clone())
                    .value(map_visit_number(msg)?)
                    .r#use(IdentifierUse::Usual)
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
        .class(map_encounter_class(msg)?)
        .r#type(map_encounter_type(msg)?)
        .subject(subject_ref(msg, &config.person.system)?)
        .period(map_period(msg)?)
        // set status depends on period.start / period.end
        .status(map_encounter_status(&map_period(msg)?))
        .build()?;

    // fab related
    if let Some(f) = fab {
        // fab schluessel
        admit.service_type = resources.map_fab_schluessel(&f)?;
        // service provider
        admit.service_provider = Some(fab_ref(&f)?);
    }

    // hospitalization admit source
    admit.hospitalization = map_admit_source(msg)?;

    Ok(admit)
}

fn map_encounter_type(msg: &Message) -> Result<Vec<Option<CodeableConcept>>, MappingError> {
    let mut coding = vec![Some(
        // Kontaktebene
        Coding::builder()
            .system("http://fhir.de/CodeSystem/Kontaktebene".to_string())
            .code("einrichtungskontakt".to_string())
            .display("Einrichtungskontakt".to_string())
            .build()?,
    )];

    if let Some(c) = map_kontaktart(msg)? {
        // Kontaktart
        coding.push(Some(c));
    }

    Ok(vec![Some(
        CodeableConcept::builder().coding(coding).build()?,
    )])
}

fn fab_ref(fab: &str) -> Result<Reference, MappingError> {
    resource_ref(
        &ResourceType::Organization,
        fab,
        "https://fhir.diz.uni-marburg.de/sid/department",
    )
}

fn subject_ref(msg: &Message, sid: &str) -> Result<Reference, MappingError> {
    let pid = parse_field(msg, "PID", 2)?.ok_or(anyhow!("missing pid value in PID.2"))?;

    resource_ref(&ResourceType::Patient, &pid, sid)
}

fn parse_fab(msg: &Message) -> Result<Option<String>, MessageAccessError> {
    if let Some(assigned_loc) = &parse_field(msg, "PV1", 3)? {
        let facility = parse_component(assigned_loc, 4)?;
        let location = parse_component(assigned_loc, 1)?;
        let loc_status = parse_component(assigned_loc, 5)?;
        // let kostenstelle = extract_repeat(assigned_loc, 6)?;

        // todo: kostenstelle lookup etc.
        return match (facility, location, loc_status) {
            // 1. wenn PV1-3.1 und PV1-3.4 Wert haben -> PV1-3.4
            (Some(f), Some(_), _) => Ok(Some(f)),
            // 2. wenn PV1-3.4 leer & PV1-3.1 hat Wert -> dann  PV1-3.1
            (None, Some(l), _) => Ok(Some(l)),
            // 3. wenn PV1-3.1 leer & PV1-3.4 hat Wert-> dann  PV1-3.5
            (Some(_), None, Some(st)) => Ok(Some(st)),
            _ => Ok(None),
        };
    }

    Ok(None)
}

fn map_admit_source(msg: &Message) -> Result<Option<EncounterHospitalization>, MappingError> {
    if let Some(source) = &parse_field(msg, "PV1", 4)? {
        let admit = parse_component(source, 1).map_err(MessageAccessError::ParseError)?;

        let coding = match admit.as_deref() {
        Some("E") => Ok(Coding::builder()
            .system("http://fhir.de/CodeSystem/dgkev/Aufnahmeanlass".to_string())
            .code("E".to_string())
            .display("Einweisung durch einen Arzt".to_string())
            .build()?),
        Some("Z") => Ok(Coding::builder()
            .system("http://fhir.de/CodeSystem/dgkev/Aufnahmeanlass".to_string())
            .code("Z".to_string())
            .display("Einweisung durch einen Zahnarzt".to_string())
            .build()?),
        Some("N") => Ok(Coding::builder()
            .system("http://fhir.de/CodeSystem/dgkev/Aufnahmeanlass".to_string())
            .code("N".to_string())
            .display("Notfall".to_string())
            .build()?),
        Some("R") => Ok(Coding::builder()
            .system("http://fhir.de/CodeSystem/dgkev/Aufnahmeanlass".to_string())
            .code("R".to_string())
            .display(
                "Aufnahme nach vorausgehender Behandlung in einer Rehabilitationseinrichtung"
                    .to_string(),
            )
            .build()?),
        Some("V") => Ok(Coding::builder()
            .system("http://fhir.de/CodeSystem/dgkev/Aufnahmeanlass".to_string())
            .code("V".to_string())
            .display(
                "Verlegung mit Behandlungsdauer im verlegenden Krankenhaus länger als 24 Stunden"
                    .to_string(),
            )
            .build()?),
        Some("A") => Ok(Coding::builder()
            .system("http://fhir.de/CodeSystem/dgkev/Aufnahmeanlass".to_string())
            .code("A".to_string())
            .display(
                "Verlegung mit Behandlungsdauer im verlegenden Krankenhaus bis zu 24 Stunden"
                    .to_string(),
            )
            .build()?),
        Some("G") => Ok(Coding::builder()
            .system("http://fhir.de/CodeSystem/dgkev/Aufnahmeanlass".to_string())
            .code("G".to_string())
            .display("Geburt".to_string())
            .build()?),
        Some("B") => Ok(Coding::builder()
            .system("http://fhir.de/CodeSystem/dgkev/Aufnahmeanlass".to_string())
            .code("B".to_string())
            .display("Begleitperson oder mitaufgenommene Pflegekraft".to_string())
            .build()?),

        Some(other) => Err(MappingError::Other(anyhow!(
            "Unknown code {} in PV1-4.1 for Encounter.hospitalization.admitSource",
            other
        ))),
        None => Err(MappingError::Other(anyhow!(
            "Missing PV1-4.1 field / component for Encounter.hospitalization.admitSource"
        ))),
    }?;

        return Ok(Some(
            EncounterHospitalization::builder()
                .admit_source(
                    CodeableConcept::builder()
                        .coding(vec![Some(coding)])
                        .build()?,
                )
                .build()?,
        ));
    }

    Ok(None)
}

fn map_period(msg: &Message) -> Result<Period, MappingError> {
    let start: DateTime = parse_datetime(
        parse_field(msg, "PV1", 44)?
            .ok_or(anyhow!("empty datetime in PV1.44"))?
            .as_str(),
    )?;
    let period = Period::builder().start(start.clone());

    let p = match parse_field(msg, "PV1", 45)? {
        Some(end) => period.end(parse_datetime(end.as_str())?),
        None => {
            match message_type(msg).map_err(MessageAccessError::MessageTypeError)? {
                // A04 has no end date is assigned start date instead
                MessageType::Registration => period.end(start),
                _ => period,
            }
        }
    };

    Ok(p.build()?)
}

fn map_encounter_status(period: &Period) -> EncounterStatus {
    match (period.start.clone(), period.end.clone()) {
        (None, None) => EncounterStatus::Unknown,
        (_, Some(_)) => EncounterStatus::Finished,
        (Some(start), _) => {
            if start.lt(&DateTime::DateTime(OffsetDateTime::now_utc().into())) {
                EncounterStatus::InProgress
            } else {
                EncounterStatus::Planned
            }
        }
    }
}

fn map_visit_number(msg: &Message) -> Result<String, anyhow::Error> {
    match message_type(msg)? {
        MessageType::PendingAdmit => {
            Ok(parse_field(msg, "PID", 4)?.ok_or(anyhow!("empty visit number in PID.4"))?)
        }
        _ => Ok(parse_field(msg, "PV1", 19)?.ok_or(anyhow!("empty visit number in PV1.19"))?),
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
    let code =
        parse_field(msg, "PV1", 2)?.ok_or(anyhow!("empty encounter_class value in PV1.2"))?;
    match code.as_str() {
        "I" => Ok(Coding::builder()
            .system("http://terminology.hl7.org/CodeSystem/v3-ActCode".to_string())
            .code("IMP".to_string())
            .display("inpatient encounter".to_string())
            .build()?),
        "O" => Ok(Coding::builder()
            .system("http://terminology.hl7.org/CodeSystem/v3-ActCode".to_string())
            .code("AMB".to_string())
            .display("ambulatory".to_string())
            .build()?),
        "P" => Ok(Coding::builder()
            .system("http://terminology.hl7.org/CodeSystem/v3-ActCode".to_string())
            .code("PRENC".to_string())
            .display("pre-admission".to_string())
            .build()?),
        // todo ... VR / SS / HH
        _ => Err(anyhow!("Invalid encounter_class code (PV1.2): {}", code)),
    }
}

fn map_kontaktart(msg: &Message) -> Result<Option<Coding>, MappingError> {
    if let Some(code) = parse_field(msg, "PV1", 2)? {
        match code.as_str() {
            // todo: the following are missing
            // O ("Ambulantes Operieren") => operation
            // I ("Normalstationär") => normalstationaer
            // I ("Intensivstationär") => intensivstationaer
            "I" => Ok(None),
            "O" => Ok(None),
            "H" => Ok(Some(
                Coding::builder()
                    .system("http://fhir.de/CodeSystem/kontaktart-de".to_string())
                    .code("begleitperson".to_string())
                    .display("Begleitperson".to_string())
                    .build()?,
            )),
            "TS" => Ok(Some(
                Coding::builder()
                    .system("http://fhir.de/CodeSystem/kontaktart-de".to_string())
                    .code("teilstationaer".to_string())
                    .display("Teilstationäre Behandlung".to_string())
                    .build()?,
            )),
            "NS" => Ok(Some(
                Coding::builder()
                    .system("http://fhir.de/CodeSystem/kontaktart-de".to_string())
                    .code("nachstationaer".to_string())
                    .display("Nachstationär".to_string())
                    .build()?,
            )),
            "UB" => Ok(Some(
                Coding::builder()
                    .system("http://fhir.de/CodeSystem/kontaktart-de".to_string())
                    .code("ub".to_string())
                    .display("Untersuchung und Behandlung".to_string())
                    .build()?,
            )),
            _ => Err(anyhow!("Invalid kontakt_art code (PV1.2): {}", code))
                .map_err(MappingError::Other)?,
        }
    } else {
        Ok(None)
    }
}
