use crate::config::Fhir;
use crate::error::{MappingError, MessageAccessError};
use crate::fhir::mapper::{EntryRequestType, bundle_entry, parse_datetime, resource_ref};
use crate::fhir::resources::ResourceMap;
use crate::fhir::terminology::AufnahmeGrundStelle;
use crate::hl7::parser::{
    MessageType, message_type, parse_component, parse_field, parse_field_value,
};
use anyhow::anyhow;
use fhir_model::DateTime;
use fhir_model::r4b::codes::{EncounterStatus, IdentifierUse};
use fhir_model::r4b::resources::{BundleEntry, Encounter, EncounterHospitalization, ResourceType};
use fhir_model::r4b::types::{
    CodeableConcept, Coding, Extension, ExtensionValue, Identifier, Meta, Period, Reference,
};
use fhir_model::time::OffsetDateTime;
use hl7_parser::Message;

enum EncounterType {
    Einrichtungskontakt,
    Fachabteilungskontakt,
    Versorgungsstellenkontakt,
}

impl From<&EncounterType> for Coding {
    fn from(t: &EncounterType) -> Self {
        match t {
            EncounterType::Einrichtungskontakt => Coding::builder()
                .system("http://fhir.de/CodeSystem/Kontaktebene".to_string())
                .code("einrichtungskontakt".to_string())
                .display("Einrichtungskontakt".to_string())
                .build()
                .expect("Kontaktebene coding"),
            EncounterType::Fachabteilungskontakt => Coding::builder()
                .system("http://fhir.de/CodeSystem/Kontaktebene".to_string())
                .code("abteilungskontakt".to_string())
                .display("Abteilungskontakt".to_string())
                .build()
                .expect("Kontaktebene coding"),
            EncounterType::Versorgungsstellenkontakt => Coding::builder()
                .system("http://fhir.de/CodeSystem/Kontaktebene".to_string())
                .code("versorgungsstellenkontakt".to_string())
                .display("Versorgungsstellenkontakt".to_string())
                .build()
                .expect("Kontaktebene coding"),
        }
    }
}

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
        MessageType::A01
        | MessageType::A02
        | MessageType::A03
        | MessageType::A04
        | MessageType::A05 => {
            let enc_admit = map_einrichtungskontakt(msg, &config)?;
            let enc_dep = map_abteilungskontakt(msg, &config, resources)?;
            // todo
            // ...

            Ok(vec![
                bundle_entry(enc_admit, EntryRequestType::UpdateAsCreate)?,
                bundle_entry(enc_dep, EntryRequestType::UpdateAsCreate)?,
            ])
        }
        MessageType::A11 | MessageType::A27 => {
            // todo
            Ok(r)
        }
        _ => Ok(r),
    }
}

fn is_begleitperson(msg: &Message) -> Result<bool, MessageAccessError> {
    Ok(parse_field_value(msg, "PV1", 2)?.is_some_and(|f| f == "H"))
}

fn map_einrichtungskontakt(msg: &Message, config: &Fhir) -> Result<Encounter, MappingError> {
    // base encounter
    let mut enc = base_encounter(msg, config, &EncounterType::Einrichtungskontakt)?;

    // hospitalization admit source
    enc.hospitalization = map_admit_source(msg)?;

    // TODO map Aufnahmegrund (reasonCode)?

    // serviceProvider -> Hospital
    enc.service_provider = Some(
        Reference::builder()
            .reference(format!(
                "Organization?identifier=http://fhir.de/sid/arge-ik/iknr|{}",
                config.facility_id
            ))
            .build()?,
    );
    // Aufnahmegrund
    if let Ok(ext) = map_aufnahme_entlassung(msg) {
        enc.extension = ext;
    }

    // todo: Entlassgrund

    Ok(enc)
}

fn map_aufnahme_entlassung(msg: &Message) -> Result<Vec<Extension>, MappingError> {
    let mut result = vec![];

    // Aufnahmegrund
    // 1. und 2. Stelle
    if let Some(erste_und_zweite) = parse_field(msg, "PV2", 3)
        .ok()
        .flatten()
        .and_then(|f| parse_component(f, 1))
        .as_ref()
        .map(|c| AufnahmeGrundStelle::ErsteUndZweite(c))
        .and_then(Option::<Coding>::from)
        .map(|c| {
            Extension::builder()
                .url("ErsteUndZweiteStelle".to_string())
                .value(ExtensionValue::Coding(c))
                .build()
        })
    {
        result.push(erste_und_zweite?);
    }

    // 3. und 4. Stelle
    if let Some((Some(dritte), Some(vierte))) = msg
        .query("PV1.4[2].1")
        .filter(|r| r.raw_value().chars().count() == 2)
        .map(|r| {
            let mut chars = r.raw_value().chars().take(2);
            (
                Option::<Coding>::from(AufnahmeGrundStelle::Dritte(
                    chars
                        .next()
                        .expect("Aufnahmegrund 3. Stelle")
                        .to_string()
                        .as_str(),
                )),
                Option::<Coding>::from(AufnahmeGrundStelle::Vierte(
                    chars
                        .next()
                        .expect("Aufnahmegrund 4. Stelle")
                        .to_string()
                        .as_str(),
                )),
            )
        })
    {
        result.push(
            Extension::builder()
                .url("DritteStelle".to_string())
                .value(ExtensionValue::Coding(dritte))
                .build()?,
        );
        result.push(
            Extension::builder()
                .url("VierteStelle".to_string())
                .value(ExtensionValue::Coding(vierte))
                .build()?,
        );
    }

    Ok(result)
}

fn map_abteilungskontakt(
    msg: &Message,
    config: &Fhir,
    resources: &ResourceMap,
) -> Result<Encounter, MappingError> {
    // base encounter
    let mut enc = base_encounter(msg, config, &EncounterType::Fachabteilungskontakt)?;

    let fab = parse_fab(msg)?;
    // fab related
    if let Some(f) = fab {
        // fab schluessel
        enc.service_type = resources.map_fab_schluessel(&f)?;
        // service provider
        enc.service_provider = Some(fab_ref(&f)?);
    }

    Ok(enc)
}

fn base_encounter(
    msg: &Message,
    config: &Fhir,
    enc_type: &EncounterType,
) -> Result<Encounter, MappingError> {
    let visit_number = visit_number(msg)?;

    let admit = Encounter::builder()
        .meta(map_meta(config)?)
        .identifier(vec![
            // identifier for Einrichtungskontakt
            Some(map_level_identifier(enc_type, config, msg)?),
            // common identifier is last
            Some(map_default_identifier(
                config.fall.system.clone(),
                visit_number.to_string(),
            )?),
        ])
        .class(map_encounter_class(msg)?)
        .r#type(map_encounter_type(msg, enc_type)?)
        .subject(subject_ref(msg, &config.person.system)?)
        .period(map_period(msg)?)
        // set status depends on period.start / period.end
        .status(map_encounter_status(&map_period(msg)?))
        .build()?;

    Ok(admit)
}

fn map_default_identifier(system: String, value: String) -> Result<Identifier, MappingError> {
    Ok(Identifier::builder()
        .system(system)
        .value(value)
        .r#use(IdentifierUse::Official)
        .r#type(
            CodeableConcept::builder()
                .coding(vec![Some(
                    Coding::builder()
                        .system("http://terminology.hl7.org/CodeSystem/v2-0203".to_string())
                        .code("VN".to_string())
                        .build()?,
                )])
                .build()?,
        )
        .build()?)
}

/// Maps the [`IdentifierUse::Usual`] identifier depending on the [`EncounterType`].
fn map_level_identifier(
    encounter_type: &EncounterType,
    config: &Fhir,
    msg: &Message,
) -> Result<Identifier, MappingError> {
    let zbe_id = msg.query("ZBE.1.1").map(|r| r.raw_value()).ok_or(anyhow!(
        "Failed to create Identifier: ZBE-1.1 is missing or empty"
    ));
    let visit_number = visit_number(msg)?;

    let (system, value) = match encounter_type {
        EncounterType::Einrichtungskontakt => {
            (&config.fall.einrichtungskontakt.system, visit_number)
        }
        EncounterType::Fachabteilungskontakt => (&config.fall.abteilungskontakt.system, zbe_id?),
        EncounterType::Versorgungsstellenkontakt => {
            (&config.fall.versorgungsstellenkontakt.system, zbe_id?)
        }
    };

    Ok(Identifier::builder()
        .system(system.clone())
        .value(value.to_string())
        .r#use(IdentifierUse::Usual)
        .build()?)
}

fn map_encounter_type(
    msg: &Message,
    enc_type: &EncounterType,
) -> Result<Vec<Option<CodeableConcept>>, MappingError> {
    // Kontaktebene
    let mut coding = vec![Some(enc_type.into())];

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
    let pid = parse_field_value(msg, "PID", 2)?.ok_or(anyhow!("missing pid value in PID.2"))?;

    resource_ref(&ResourceType::Patient, &pid, sid)
}

fn parse_fab(msg: &Message) -> Result<Option<String>, MessageAccessError> {
    if let Some(assigned_loc) = parse_field(msg, "PV1", 3)? {
        let facility = parse_component(assigned_loc, 4);
        let location = parse_component(assigned_loc, 1);
        let loc_status = parse_component(assigned_loc, 5);
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
    let admit = msg.query("PV1.4.1").map(|r| r.raw_value());

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

    Ok(Some(
        EncounterHospitalization::builder()
            .admit_source(
                CodeableConcept::builder()
                    .coding(vec![Some(coding)])
                    .build()?,
            )
            .build()?,
    ))
}

fn map_period(msg: &Message) -> Result<Period, MappingError> {
    let start: DateTime = parse_datetime(
        parse_field_value(msg, "PV1", 44)?.ok_or(anyhow!("empty datetime in PV1.44"))?,
    )?;
    let period = Period::builder().start(start.clone());

    let p = match parse_field_value(msg, "PV1", 45)? {
        Some(end) => period.end(parse_datetime(end)?),
        None => {
            match message_type(msg).map_err(MessageAccessError::MessageTypeError)? {
                // A04 has no end date is assigned start date instead
                MessageType::A04 => period.end(start),
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

fn visit_number<'a>(msg: &'a Message) -> Result<&'a str, anyhow::Error> {
    match message_type(msg)? {
        MessageType::A14 => {
            Ok(parse_field_value(msg, "PID", 4)?.ok_or(anyhow!("empty visit number in PID.4"))?)
        }
        _ => Ok(parse_field_value(msg, "PV1", 19)?.ok_or(anyhow!("empty visit number in PV1.19"))?),
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
        parse_field_value(msg, "PV1", 2)?.ok_or(anyhow!("empty encounter_class value in PV1.2"))?;
    match code {
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
    if let Some(code) = parse_field_value(msg, "PV1", 2)? {
        match code {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{FallConfig, SystemConfig};
    use hl7_parser::Message;
    use rstest::rstest;
    use std::default::Default;

    #[rstest]
    #[case(EncounterType::Einrichtungskontakt, ("einrichtungskontakt","admit_id"))]
    #[case(EncounterType::Fachabteilungskontakt, ("abteilungskontakt","zbe_id"))]
    #[case(EncounterType::Versorgungsstellenkontakt, ("versorgungsstellenkontakt","zbe_id"))]
    fn test_map_level_identifier(#[case] level: EncounterType, #[case] expected: (&str, &str)) {
        let msg = r#"MSH|^~\&|ORBIS|KH|WEBEPA|KH|202208200651||ADT^A04^ADT_A04|65298857|P|2.5||640340718|NE|NE||8859/1
EVN|A08|202511022120||11036_123456789|ZZZZZZZZ|202511022120
PID|||||Schuster^Regine^^^^^L~Musterfrau^Regine^^^^^M|||||||||||||||||||||||||
PV1|1|I|^^^^KLINIKUM^|R^^HL7~01^Normalfall^301||||||N||||||N|||admit_id||K||||||||||||||||||2500|||||202208200618|||||||A
ZBE|zbe_id^SAP-ISH~615^MEDOS|20030901163000||UPDATE"#;
        let msg = Message::parse_with_lenient_newlines(msg, true).unwrap();

        let config = Fhir {
            fall: FallConfig {
                einrichtungskontakt: SystemConfig {
                    system: "einrichtungskontakt".into(),
                },
                abteilungskontakt: SystemConfig {
                    system: "abteilungskontakt".into(),
                },
                versorgungsstellenkontakt: SystemConfig {
                    system: "versorgungsstellenkontakt".into(),
                },
                profile: String::default(),
                system: String::default(),
            },
            person: Default::default(),
            facility_id: String::default(),
        };

        let expected = Identifier::builder()
            .system(expected.0.into())
            .value(expected.1.into())
            .r#use(IdentifierUse::Usual)
            .build()
            .unwrap();

        let identifier = map_level_identifier(&level, &config, &msg).unwrap();

        assert_eq!(identifier, expected);
    }
}
