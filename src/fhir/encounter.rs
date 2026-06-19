use crate::config::Fhir;
use crate::error::{MappingError, MessageAccessError, ParsingError};
use crate::fhir::encounter::EncounterType::{Fachabteilungskontakt, Versorgungsstellenkontakt};
use crate::fhir::location::{
    map_bed_location, map_room_location, map_ward_location, to_encounter_location,
};
use crate::fhir::mapper::{
    EntryRequestType, bundle_entry, get_cc_with_one_code, is_inpatient_location, map_visit_number,
    parse_datetime, parse_fab, resource_ref, subject_ref,
};
use crate::fhir::resources::{ResourceMap, is_valid_date};
use crate::fhir::terminology::{
    AufnahmeGrundStelle, EntlassgrundStelle, diagnose_role_coding, kontakt_diagnose_procedures,
};
use crate::hl7::parser::{
    MessageType, PID_21_1, PV1_2, PV1_3_1, PV1_3_2, PV1_3_3, PV1_4__2_1, PV1_4_1, PV1_36_1,
    PV1_39_1, PV1_40_1, PV1_44, PV1_45, PV2_3_1, ZBE_1_1, ZBE_2, ZBE_3, get_message_key,
    message_type, query,
};
use anyhow::anyhow;
use chrono::NaiveDate;
use fhir_model::DateTime;
use fhir_model::r4b::codes::{EncounterStatus, IdentifierUse};
use fhir_model::r4b::resources::{
    BundleEntry, Encounter, EncounterBuilder, EncounterDiagnosis, EncounterHospitalization,
    EncounterLocation, ResourceType,
};
use fhir_model::r4b::types::{
    CodeableConcept, Coding, Extension, ExtensionValue, Identifier, Meta, Period, Reference,
};
use fhir_model::time::OffsetDateTime;
use hl7_parser::Message;
use hl7_parser::message::Field;
use log::{Level, log};
use std::cmp::PartialEq;
use std::num::NonZeroU32;

#[derive(PartialEq, Debug)]
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
    let mut result: Vec<BundleEntry> = vec![];
    if is_begleitperson(msg).is_ok_and(|v| v) {
        return Ok(result);
    }

    let msg_type = message_type(msg);
    let message_type = msg_type.map_err(MessageAccessError::MessageTypeError)?;

    match message_type {
        MessageType::A01
        | MessageType::A02
        | MessageType::A03
        | MessageType::A04
        | MessageType::A05
        | MessageType::A06
        | MessageType::A07
        | MessageType::A08
        | MessageType::A13 => {
            let enc_admit = map_einrichtungskontakt(msg, &config, resources)?;
            result.push(bundle_entry(enc_admit, EntryRequestType::UpdateAsCreate)?);

            if let Some(enc_dep) = map_abteilungskontakt(msg, &config, resources)? {
                result.push(bundle_entry(enc_dep, EntryRequestType::UpdateAsCreate)?);
            }

            if let Some(care_site_enc) = map_versorgungsstellenkontakt(msg, &config, resources)? {
                result.push(bundle_entry(
                    care_site_enc,
                    EntryRequestType::UpdateAsCreate,
                )?);
            }
            Ok(result)
        }
        // create only basic encounter data for delete
        MessageType::A11 | MessageType::A27 | MessageType::A12 => {
            // A12 deletes only  Fachabteilungskontakt & Versorgungsstellenkontakt
            if message_type == MessageType::A11 || message_type == MessageType::A27 {
                let enc_admit =
                    base_encounter(msg, &config, resources, &EncounterType::Einrichtungskontakt)?
                        .build()?;
                result.push(bundle_entry(enc_admit, EntryRequestType::Delete)?)
            }

            result.push(bundle_entry(
                base_encounter(
                    msg,
                    &config,
                    resources,
                    &EncounterType::Fachabteilungskontakt,
                )?
                .build()?,
                EntryRequestType::Delete,
            )?);

            result.push(bundle_entry(
                base_encounter(
                    msg,
                    &config,
                    resources,
                    &EncounterType::Versorgungsstellenkontakt,
                )?
                .build()?,
                EntryRequestType::Delete,
            )?);

            Ok(result)
        }

        _ => Ok(result),
    }
}

fn is_begleitperson(msg: &Message) -> Result<bool, MessageAccessError> {
    Ok(query(msg, PV1_2).is_some_and(|f| f == "H"))
}

fn map_einrichtungskontakt(
    msg: &Message,
    config: &Fhir,
    resources: &ResourceMap,
) -> Result<Encounter, MappingError> {
    // base encounter
    let mut enc = base_encounter(msg, config, resources, &EncounterType::Einrichtungskontakt)?
        // serviceProvider -> Hospital
        .service_provider(
            Reference::builder()
                .reference(format!(
                    "Organization?identifier=http://fhir.de/sid/arge-ik/iknr|{}",
                    config.facility_id
                ))
                .build()?,
        )
        .build()?;

    // hospitalization admit source & discharge disposition (Entlassgrund)
    enc.hospitalization = map_hospitalization(msg)?;

    // Aufnahmegrund
    enc.extension = vec![
        Extension::builder()
            .url("http://fhir.de/StructureDefinition/Aufnahmegrund".to_string())
            .extension(map_aufnahmegrund(msg).unwrap_or_default())
            .build()?,
    ];

    if let Ok(diagnosis) = map_conditions(msg, config) {
        enc.diagnosis = diagnosis;
    }
    if let Ok(mothers_encounter) = map_mothers_encounter(msg, config) {
        enc.part_of = mothers_encounter
    }

    if let Some(bed_status) = query(msg, PV1_2)
        && bed_status == "NS"
    {
        // case status change 'nachstationär'
        // Here we do not want to change in-patient encounter status after discharge.
        // With bed status 'NS' we get only some additional ambulatory treatment,
        // which will be represented by ambulatory class 'Abteilungskontakt' and
        // 'Versorgungsstellenkontakt'
        enc.class.code = Some("IMP".to_string());
    }
    Ok(enc)
}

fn map_mothers_encounter(msg: &Message, config: &Fhir) -> Result<Option<Reference>, MappingError> {
    let mothers_enc_number = query(msg, PID_21_1);
    match mothers_enc_number {
        Some(mothers_enc_number) => Ok(Some(resource_ref(
            &ResourceType::Encounter,
            mothers_enc_number,
            config.fall.einrichtungskontakt.system.as_str(),
        )?)),
        None => Ok(None),
    }
}

fn map_aufnahmegrund(msg: &Message) -> Result<Vec<Extension>, MappingError> {
    let mut result = vec![];

    // Aufnahmegrund
    // 1. und 2. Stelle
    if let Some(erste_und_zweite) = query(msg, PV2_3_1)
        .map(AufnahmeGrundStelle::ErsteUndZweite)
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
    if let Some((Some(dritte), Some(vierte))) = query(msg, PV1_4__2_1)
        .filter(|r| r.chars().count() == 2)
        .map(|r| {
            let mut chars = r.chars().take(2);
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

fn map_entlassgrund(msg: &Message) -> Result<Vec<Extension>, MappingError> {
    let mut extension_components = vec![];

    // 1. und 2. Stelle
    if let Some(erste_und_zweite) = query(msg, PV1_36_1)
        .map(EntlassgrundStelle::ErsteUndZweite)
        .and_then(Option::<Coding>::from)
        .map(|c| {
            Extension::builder()
                .url("ErsteUndZweiteStelle".to_string())
                .value(ExtensionValue::Coding(c))
                .build()
        })
    {
        extension_components.push(erste_und_zweite?);
    }

    // 3. Stelle
    if let Some(dritte) = query(msg, PV1_40_1)
        .map(EntlassgrundStelle::Dritte)
        .and_then(Option::<Coding>::from)
        .map(|c| {
            Extension::builder()
                .url("DritteStelle".to_string())
                .value(ExtensionValue::Coding(c))
                .build()
        })
    {
        extension_components.push(dritte?);
    }
    if !extension_components.is_empty() {
        return Ok(vec![
            Extension::builder()
                .extension(extension_components)
                .url("http://fhir.de/StructureDefinition/Entlassungsgrund".to_string())
                .build()?,
        ]);
    }
    Ok(vec![])
}

fn map_abteilungskontakt(
    msg: &Message,
    config: &Fhir,
    resources: &ResourceMap,
) -> Result<Option<Encounter>, MappingError> {
    if let Some(service_type) = get_service_type(msg, resources)? {
        // base encounter
        let mut enc = base_encounter(
            msg,
            config,
            resources,
            &EncounterType::Fachabteilungskontakt,
        )?
        .build()?;

        enc.service_type = Some(service_type);
        if let Some(fab) = parse_fab(msg) {
            enc.service_provider = Some(fab_ref(fab, config)?);
        }

        Ok(Some(enc))
    } else {
        log!(
            Level::Debug,
            "Missing service type at msg-id '{}' - cannot build valid encounter at department level!",
            get_message_key(msg)?
        );
        Ok(None)
    }
}

fn get_service_type(
    msg: &Message,
    resources: &ResourceMap,
) -> Result<Option<CodeableConcept>, MappingError> {
    let system_fachabteilungs_schluessel: &str =
        "http://fhir.de/CodeSystem/dkgev/Fachabteilungsschluessel-erweitert";

    if let Some(fab) = parse_fab(msg) {
        match resources.map_fab_schluessel(fab) {
            Ok(Some(fab_from_short_name)) => return Ok(Some(fab_from_short_name)),
            Err(e) => return Err(e),
            Ok(None) => {}
        };
    }

    if let Some(fab_schluessel) = query(msg, PV1_39_1) {
        Ok(Some(get_cc_with_one_code(
            fab_schluessel.to_string(),
            system_fachabteilungs_schluessel.to_string(),
        )?))
    } else {
        Ok(None)
    }
}

fn base_encounter(
    msg: &Message,
    config: &Fhir,
    resources: &ResourceMap,
    enc_type: &EncounterType,
) -> Result<EncounterBuilder, MappingError> {
    let visit_number = map_visit_number(msg)?;

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
        .r#type(map_encounter_type(msg, enc_type, resources)?)
        .subject(subject_ref(msg, &config.person.system)?)
        .period(map_period(msg, enc_type)?)
        // set status depends on period.start / period.end
        .status(map_encounter_status(&map_period(msg, enc_type)?));

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
    let zbe_id = query(msg, ZBE_1_1).ok_or(MessageAccessError::Other(anyhow!(
        "Failed to create Identifier: ZBE-1.1 is missing or empty"
    )));
    let visit_number = map_visit_number(msg)?;

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
    resources: &ResourceMap,
) -> Result<Vec<Option<CodeableConcept>>, MappingError> {
    // Kontaktebene
    let mut coding = vec![Some(enc_type.into())];

    if let Some(c) = map_kontaktart(msg, resources, enc_type)? {
        // Kontaktart
        coding.push(Some(c));
    }

    Ok(vec![Some(
        CodeableConcept::builder().coding(coding).build()?,
    )])
}

fn fab_ref(fab: &str, config: &Fhir) -> Result<Reference, MappingError> {
    resource_ref(
        &ResourceType::Organization,
        fab,
        config.organization.department.system.as_str(),
    )
}

fn map_hospitalization(msg: &Message) -> Result<Option<EncounterHospitalization>, MappingError> {
    let discharge = map_entlassgrund(msg)?;

    let hospitalization = EncounterHospitalization::builder()
        .discharge_disposition(CodeableConcept::builder().extension(discharge).build()?);

    if let Some(admit_source) = map_admit_source(msg)? {
        return Ok(Some(
            hospitalization
                .admit_source(
                    CodeableConcept::builder()
                        .coding(vec![Some(admit_source)])
                        .build()?,
                )
                .build()?,
        ));
    }

    Ok(None)
}

fn map_admit_source(msg: &Message) -> Result<Option<Coding>, MappingError> {
    let code = query(msg, PV1_4_1).ok_or(MappingError::Other(anyhow!(
        "Missing PV1-4.1 field / component for Encounter.hospitalization.admitSource"
    )))?;

    let display = match code {
        "E" => Ok("Einweisung durch einen Arzt"),
        "Z" => Ok("Einweisung durch einen Zahnarzt"),
        "N" => Ok("Notfall"),
        "R" => Ok("Aufnahme nach vorausgehender Behandlung in einer Rehabilitationseinrichtung"),
        "V" => {
            Ok("Verlegung mit Behandlungsdauer im verlegenden Krankenhaus länger als 24 Stunden")
        }
        "A" => Ok("Verlegung mit Behandlungsdauer im verlegenden Krankenhaus bis zu 24 Stunden"),
        "G" => Ok("Geburt"),
        "B" => Ok("Begleitperson oder mitaufgenommene Pflegekraft"),
        other => Err(MappingError::Other(anyhow!(
            "Unknown code {} in PV1-4.1 for Encounter.hospitalization.admitSource",
            other
        ))),
    }?;

    Ok(Some(
        Coding::builder()
            .system("http://fhir.de/CodeSystem/dgkev/Aufnahmeanlass".to_string())
            .code(code.to_string())
            .display(display.to_string())
            .build()?,
    ))
}

fn map_period(msg: &Message, lvl: &EncounterType) -> Result<Period, MappingError> {
    let start: DateTime;
    let end: Option<DateTime>;
    match lvl {
        EncounterType::Einrichtungskontakt => {
            start = parse_datetime(query(msg, PV1_44).ok_or(anyhow!("empty datetime in PV1.44"))?)?;

            end = match query(msg, PV1_45) {
                Some(end) => Some(parse_datetime(end)?),
                None => None,
            };
        }
        EncounterType::Fachabteilungskontakt | EncounterType::Versorgungsstellenkontakt => {
            start = parse_datetime(query(msg, ZBE_2).ok_or(anyhow!("empty datetime in ZBE-2"))?)?;
            end = match query(msg, ZBE_3) {
                Some(end) => Some(parse_datetime(end)?),
                None => {
                    // A04 get never an end date form source system - therefore we use start date here as well
                    if MessageType::A04 == message_type(msg).map_err(MessageAccessError::from)? {
                        Some(start.clone())
                    } else {
                        None
                    }
                }
            };
        }
    }

    let mut period: Period = Period::builder().start(start).build()?;
    if end.is_some() {
        period.end = end;
    }

    Ok(period)
}

fn map_encounter_status(period: &Period) -> EncounterStatus {
    match (period.start.as_ref(), period.end.as_ref()) {
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

fn map_meta(config: &Fhir) -> Result<Meta, anyhow::Error> {
    Ok(Meta::builder()
        .profile(vec![Some(config.fall.profile.clone())])
        .source(config.meta_source.to_string())
        .build()?)
}

fn map_encounter_class(msg: &Message) -> Result<Coding, anyhow::Error> {
    let code = query(msg, PV1_2).ok_or(anyhow!("empty encounter_class value in PV1.2"))?;
    match code {
        "I" => Ok(Coding::builder()
            .system("http://terminology.hl7.org/CodeSystem/v3-ActCode".to_string())
            .code("IMP".to_string())
            .display("inpatient encounter".to_string())
            .build()?),
        "O" | "NS" | "VS" | "V" => Ok(Coding::builder()
            .system("http://terminology.hl7.org/CodeSystem/v3-ActCode".to_string())
            .code("AMB".to_string())
            .display("ambulatory".to_string())
            .build()?),
        "P" => Ok(Coding::builder()
            .system("http://terminology.hl7.org/CodeSystem/v3-ActCode".to_string())
            .code("PRENC".to_string())
            .display("pre-admission".to_string())
            .build()?),
        "TS" => Ok(Coding::builder()
            .system("http://terminology.hl7.org/CodeSystem/v3-ActCode".to_string())
            .code("SS".to_string())
            .display("short-stay".to_string())
            .build()?),
        _ => Err(anyhow!("Invalid encounter_class code (PV1.2): {}", code)),
    }
}

fn map_kontaktart(
    msg: &Message,
    resources: &ResourceMap,
    enc_type: &EncounterType,
) -> Result<Option<Coding>, MappingError> {
    if &Versorgungsstellenkontakt == enc_type || &Fachabteilungskontakt == enc_type {
        let is_valid_ward = is_ward_valid_icu(msg, resources);
        if is_valid_ward {
            return Ok(Some(
                Coding::builder()
                    .system("http://fhir.de/CodeSystem/kontaktart-de".to_string())
                    .code("intensivstationaer".to_string())
                    .display("Intensivstationär".to_string())
                    .build()?,
            ));
        }
    }

    if let Some(code) = query(msg, PV1_2) {
        match code {
            "I" | "O" => {
                if message_type(msg).ok() == Some(MessageType::A04) {
                    Ok(Some(
                        Coding::builder()
                            .system("http://fhir.de/CodeSystem/kontaktart-de".to_string())
                            .code("ub".to_string())
                            .display("Untersuchung und Behandlung".to_string())
                            .build()?,
                    ))
                } else {
                    Ok(None)
                }
            }
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
            "V" | "VS" => Ok(Some(
                Coding::builder()
                    .system("http://fhir.de/CodeSystem/kontaktart-de".to_string())
                    .code("vorstationaer".to_string())
                    .display("Vorstationär".to_string())
                    .build()?,
            )),
            _ => Err(anyhow!("Invalid kontakt_art code (PV1.2): {}", code))
                .map_err(MappingError::Other)?,
        }
    } else {
        Ok(None)
    }
}

fn is_ward_valid_icu(msg: &Message, resources: &ResourceMap) -> bool {
    query(msg, PV1_3_1)
        .and_then(|ward_id| resources.ward_map.get(ward_id))
        .is_some_and(|ward| {
            ward.is_icu
                && query(msg, ZBE_2)
                    .and_then(|zbe_start| {
                        let option = NaiveDate::parse_from_str(zbe_start, "%Y%m%d%H%M");
                        option.ok()
                    })
                    .is_some_and(|n_date| {
                        ward.valid_period
                            .iter()
                            .any(|period| is_valid_date(period, &n_date))
                    })
        })
}

fn map_versorgungsstellenkontakt(
    msg: &Message,
    config: &Fhir,
    resources: &ResourceMap,
) -> Result<Option<Encounter>, MappingError> {
    let mapped_locations = map_lvl_3_locations(msg, config, resources)?;
    if mapped_locations.is_empty() {
        return Ok(None);
    }
    let versorgungskontakt = base_encounter(msg, config, resources, &Versorgungsstellenkontakt)?
        .part_of(resource_ref(
            &ResourceType::Encounter,
            query(msg, ZBE_1_1)
                .ok_or(MessageAccessError::MissingMessageSegment("ZBE".to_string()))?,
            &config.fall.abteilungskontakt.system,
        )?)
        .location(mapped_locations)
        .status(map_encounter_status(&map_period(
            msg,
            &Versorgungsstellenkontakt,
        )?));

    let mut kontakt = versorgungskontakt
        .build()
        .map_err(MappingError::BuilderError)?;

    kontakt.service_provider = query(msg, PV1_3_1).and_then(|f| {
        resource_ref(
            &ResourceType::Organization,
            f,
            config.organization.ward.system.as_str(),
        )
        .ok()
    });

    Ok(Some(kontakt))
}

fn map_lvl_3_locations(
    msg: &Message,
    config: &Fhir,
    resources: &ResourceMap,
) -> Result<Vec<Option<EncounterLocation>>, MappingError> {
    let mut locations: Vec<Option<EncounterLocation>> = vec![];

    if let Some(department) = parse_fab(msg) {
        // department location should be always available
        locations.push(Some(to_encounter_location(map_ward_location(
            msg, department, config, resources,
        )?)?));

        if is_inpatient_location(msg)? {
            let ward = query(msg, PV1_3_1);
            let room = query(msg, PV1_3_2);
            let bed = query(msg, PV1_3_3);
            if let (Some(ward), Some(room)) = (ward, room) {
                locations.push(Some(to_encounter_location(map_room_location(
                    config, ward, room,
                )?)?));
            }

            if let (Some(ward), Some(room), Some(bed)) = (ward, room, bed) {
                locations.push(Some(to_encounter_location(map_bed_location(
                    config, ward, room, bed,
                )?)?));
            }
        }
        Ok(locations)
    } else {
        log!(
            Level::Debug,
            "Skipping 'Versorgungsstellenkontakt' - patient location is unknown at msg-id {}",
            get_message_key(msg)?
        );
        Ok(locations)
    }
}

fn map_conditions(
    msg: &Message,
    config: &Fhir,
) -> Result<Vec<Option<EncounterDiagnosis>>, MappingError> {
    let mut res = vec![];
    if msg.segment_count("DG1") > 0 {
        for dg1 in msg.segments().filter(|seg| seg.name.eq("DG1")) {
            let Some(row_number) = dg1.field(1) else {
                continue;
            };
            let Some(condition_typ) = dg1.field(6) else {
                continue;
            };
            let Some(priority) = dg1.field(15) else {
                continue;
            };
            let Some(condition_id) = dg1.field(20) else {
                continue;
            };

            if condition_id.is_empty() || priority.is_empty() || condition_typ.is_empty() {
                continue;
            }

            let priority_u32 = priority
                .raw_value()
                .parse::<f32>()
                .map_err(ParsingError::ParseFloatError)?
                .floor() as u32;

            let rank_nz = NonZeroU32::new(priority_u32).ok_or_else(|| {
                MappingError::Other(anyhow!(format!(
                    "DG1 entry row '{}': priority could not be parsed. value was '{}'",
                    row_number.raw_value(),
                    priority.raw_value()
                )))
            })?;

            let condition_reference = resource_ref(
                &ResourceType::Condition,
                map_bar_identifier(condition_id, priority)?.as_str(),
                &config.condition.system,
            )?;

            let codings =
                map_diagnose_local_codes(priority_u32, condition_typ.raw_value().to_string())?;

            // profile allows only one use from DiagnoseTyp and Diagnosesubtyp per entry.
            // multiple entries for same condition are allowed.
            // to ensure we do not violate constraint, we create only one single type use entries.
            for coding in codings {
                res.push(Some(
                    EncounterDiagnosis::builder()
                        .condition(condition_reference.clone())
                        .r#use(CodeableConcept::builder().coding(vec![coding]).build()?)
                        .rank(rank_nz)
                        .build()?,
                ));
            }
        }
    };
    Ok(res)
}
/// identifier value is build like 'condition-id-rank'
///
/// note:
/// Some ADT messages have only an integer as diagnosis priority,
/// but actually if we check BAR message it has '\<value\>.1'!
/// It seems .1 is only added to ADT message if .2 priority is present, too.
/// Since we build identifier from this value, we need to unify it
/// to standard, which are set by HL7 BAR messages.
fn map_bar_identifier(condition_id: &Field, priority: &Field) -> Result<String, MappingError> {
    let split_by_point = priority.raw_value().split(".").collect::<Vec<&str>>();

    match split_by_point.len() > 1 {
        false => Ok(format!(
            "{}-{}.1",
            condition_id.raw_value(),
            priority.raw_value()
        )),
        true => {
            match (
                split_by_point[1].parse::<u32>(),
                split_by_point[0].parse::<u32>(),
            ) {
                (Ok(_), Ok(_)) => Ok(format!(
                    "{}-{}",
                    condition_id.raw_value(),
                    priority.raw_value()
                )),

                (Err(e), _) => Err(MappingError::FormattingError(e.into())),
                (_, Err(e)) => Err(MappingError::FormattingError(e.into())),
            }
        }
    }
}

fn map_diagnose_local_codes(
    priority: u32,
    condition_type_local: String,
) -> Result<Vec<Option<Coding>>, MappingError> {
    let mut result = vec![];

    let is_main_condition = priority < 2;
    // not supported by 2026 profile
    if is_main_condition {
        result.push(diagnose_role_coding("CC"));
    } else {
        result.push(diagnose_role_coding("CM"));
    };

    match condition_type_local.as_str() {
        // Aufnahmediagnose
        "AD" | "Aufn." => {
            result.push(diagnose_role_coding("AD"));
        }

        // Einweisungsdiagnose
        "ED" | "Einw." => {
            result.push(kontakt_diagnose_procedures("referral-diagnosis"));
        }

        // Behandlungsdiagnose
        "BD" => {
            result.push(kontakt_diagnose_procedures("treatment-diagnosis"));

            // not supported by 2026 profile
            if is_main_condition {
                result.push(kontakt_diagnose_procedures("hospital-main-diagnosis"));
            }
        }

        // Entlassungsdiagnose
        "EL" | "Entl." => {
            result.push(diagnose_role_coding("DD"));
        }

        // Postoperative Diagnose
        "PO" | "Post" => {
            result.push(kontakt_diagnose_procedures("surgery-diagnosis"));
            // not supported by 2026 profile
            result.push(diagnose_role_coding("post-op"));
        }

        // DRG Diagnose
        "DD" => {
            // not supported by 2026 profile
            if is_main_condition {
                result.push(kontakt_diagnose_procedures("principle-DRG"));
            } else {
                // not supported by 2026 profile
                result.push(kontakt_diagnose_procedures("secondary-DRG"));
            }
        }

        // Abrechungsdiagnose
        "AR" | "Abr" => {
            // not supported by 2026 profile
            result.push(diagnose_role_coding("billing"));
        }

        // Präoperative Diagnose
        "PR" | "Präop" => {
            result.push(kontakt_diagnose_procedures("surgery-diagnosis"));
            // not supported by 2026 profile
            result.push(diagnose_role_coding("pre-op"));
        }

        // Fachabteilungs-Aufnahmediagnose & Behandlungsdiagnose & Entlassungsdiagnose
        "FB" | "FA" | "FE" | "FA En" | "FA Be" => {
            if is_main_condition {
                result.push(kontakt_diagnose_procedures("department-main-diagnosis"));
            }
            match condition_type_local.as_str() {
                "FA" => {
                    result.push(diagnose_role_coding("AD"));
                }
                "FB" | "FA Be" => {
                    result.push(kontakt_diagnose_procedures("treatment-diagnosis"));
                }
                "FE" | "FA En" => {
                    result.push(diagnose_role_coding("DD"));
                }
                _ => {
                    // other ignore - since we are here in department context
                }
            }
        }
        _ => {
            if !condition_type_local.is_empty() {
                return Err(MessageAccessError::UnsupportedContentError(
                    condition_type_local,
                    "DG1-6.1".to_string(),
                )
                .into());
            }
            if condition_type_local.is_empty() {
                return Err(MessageAccessError::UnsupportedContentError(
                    "empty value".to_string(),
                    "DG1-6.1".to_string(),
                )
                .into());
            }
        }
    }

    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{FallConfig, LocationConfig, PatientConfig, SystemConfig};
    use crate::error::MessageAccessError::UnsupportedContentError;
    use crate::test_utils::tests::{get_dummy_resources, get_test_config, read_test_resource};
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
            person: PatientConfig::default(),
            facility_id: String::default(),
            location: LocationConfig::default(),
            meta_source: String::default(),
            condition: Default::default(),
            observation: Default::default(),
            organization: Default::default(),
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

    #[test]
    fn map_lvl_3_locations_test() {
        let msg = Message::parse_with_lenient_newlines(r#"MSH|^~\&|ORBIS|KH|WEBEPA|KH|20251102212117||ADT^A08^ADT_A01|12332112|P|2.5||123788998|NE|NE||8859/1
EVN|A08|202511022120||11036_123456789|ZZZZZZZZ|202511022120
PID|1|9999999|9999999|88888888|Nachname^Vorname^^^^^L||20251102|M|||Strasse. 1&Strasse.&1^^Stadt^^30000^DE^L~^^Stadt^^^^BDL||0000000000000^PRN^PH^^^00000^0000000^^^^^000000000000|||U|||||12345678^^^KH^VN~1234567^^^KH^PT||Stadt|J|1|DE|||201103240800|Y
PV1|1|I|POL1234^BSP-2-2^2^POL^KLINIKUM^961640|R^^HL7~01^Normalfall^11||||^^^^^^^^^L^^^^^^^^^^^^^^^^^^^^^^^^^^^BSNR||N||||||N|||88888888||K|||||||||||||||01|||0800|9||||202511022120|202511022120||||||A
ZBE|55555555^ORBIS|202511022120|202511022120|UPDATE
"#, true).unwrap();
        let actual =
            map_versorgungsstellenkontakt(&msg, &get_test_config(), &get_dummy_resources())
                .unwrap()
                .unwrap();

        assert_eq!(actual.location.len(), 3);
    }

    #[test]
    fn map_entlassgrund_test() {
        let msg = Message::parse_with_lenient_newlines(r#"MSH|^~\&|ORBIS|KH|WEBEPA|KH|20251102212117||ADT^A08^ADT_A01|12332112|P|2.5||123788998|NE|NE||8859/1
EVN|A08|202511022120||11036_123456789|ZZZZZZZZ|202511022120
PID|1|9999999|9999999|88888888|Nachname^Vorname^^^^^L||20251102|M|||Strasse. 1&Strasse.&1^^Stadt^^30000^DE^L~^^Stadt^^^^BDL||0000000000000^PRN^PH^^^00000^0000000^^^^^000000000000|||U|||||12345678^^^KH^VN~1234567^^^KH^PT||Stadt|J|1|DE|||201103240800|Y
PV1|1|I|POL1234^BSP-2-2^2^POL^KLINIKUM^961640|R^^HL7~01^Normalfall^11||||^^^^^^^^^L^^^^^^^^^^^^^^^^^^^^^^^^^^^BSNR||N||||||N|||88888888||K|||||||||||||||01|||0800|9||||202511022120|202511022120||||||A
ZBE|55555555^ORBIS|202511022120|202511022120|UPDATE
"#, true).unwrap();

        let expected = vec![
            Extension::builder()
                .url("ErsteUndZweiteStelle".to_string())
                .value(ExtensionValue::Coding(
                    Coding::builder()
                        .system(
                            "http://fhir.de/CodeSystem/dkgev/EntlassungsgrundErsteUndZweiteStelle"
                                .into(),
                        )
                        .code("01".into())
                        .display("Behandlung regulär beendet".into())
                        .build()
                        .unwrap(),
                ))
                .build()
                .unwrap(),
            Extension::builder()
                .url("DritteStelle".to_string())
                .value(ExtensionValue::Coding(
                    Coding::builder()
                        .system(
                            "http://fhir.de/CodeSystem/dkgev/EntlassungsgrundDritteStelle".into(),
                        )
                        .code("9".into())
                        .display("keine Angabe".into())
                        .build()
                        .unwrap(),
                ))
                .build()
                .unwrap(),
        ];

        let actual = map_entlassgrund(&msg).unwrap();

        assert!(actual.len() == 1);

        assert_eq!(actual.first().unwrap().extension, expected);
    }

    #[test]
    fn map_condition_identifier_test() {
        let binding = read_test_resource("a03_test.hl7");
        let msg = Message::parse_with_lenient_newlines(binding.as_str(), true).unwrap();
        let result = &map_conditions(&msg, &get_test_config());

        // every condition has 2 entries, therefore increase index by 2
        assert!(result.is_ok());
        let first_entry = result
            .as_ref()
            .unwrap()
            .first()
            .unwrap()
            .as_ref()
            .unwrap()
            .condition
            .reference
            .as_ref()
            .unwrap();
        assert!(
            first_entry.ends_with("|12345677-1.1"),
            "but fount {}",
            first_entry
        );

        let second_entry = result
            .as_ref()
            .unwrap()
            .get(2)
            .unwrap()
            .as_ref()
            .unwrap()
            .condition
            .reference
            .as_ref()
            .unwrap();
        assert!(
            second_entry.ends_with("|12345678-2.1"),
            "but fount {}",
            second_entry
        );
        let third_entry = result
            .as_ref()
            .unwrap()
            .get(4)
            .unwrap()
            .as_ref()
            .unwrap()
            .condition
            .reference
            .as_ref()
            .unwrap();
        assert!(
            third_entry.ends_with("|12345679-2.2"),
            "but fount {}",
            third_entry
        );
    }

    #[rstest]
    #[case("asdf")]
    #[case("a.1")]
    #[case("1.b")]
    #[case("0")]
    #[case("-1")]
    fn invalid_condition_ref_nan(#[case] prio_value: String) {
        let input = format!(
            r#"MSH|^~\&|ORBIS|KH|RECAPP|ORBIS|202111230904||ADT^A03|62325574|P|2.5|||||D||DE
EVN|A03|202111230904|202111230904||Muster
PID|1|1396227|1396227||Test^Anton||19510704|M|||Teststr. 26^^Wetzlar^^35578^D^L||0151/123123123^^CP|||M|or|||||||N||SYR
DG1|1||K42.9^Hernia umbilicalis ohne Einklemmung und ohne Gangrän^icd10gm2022||20230101131500|Aufn.|||||||||{}|ABCDEFGH^^^^^^^^^^^^^^^^^^^^^^KCH||||12345677|U
"#,
            prio_value
        );
        let msg = Message::parse_with_lenient_newlines(input.as_str(), true).unwrap();
        let x = &map_conditions(&msg, &get_test_config());
        match (x, prio_value.as_str()) {
            (Ok(_), _) => panic!("ParseFloatError was expected but result was OK!"),
            (Err(MappingError::Other(_)), "0") => {
                println!("got MappingError for zero rank as expected");
            }
            (Err(MappingError::Other(_)), "-1") => {
                println!("got MappingError for negativ rank as expected");
            }
            (Err(MappingError::FormattingError(ParsingError::ParseFloatError(_))), _) => {
                println!("got ParseFloatError as expected");
            }
            (Err(c), _) => panic!("ParseFloatError was expected but found => '{}'", c),
        }
    }

    #[test]
    fn unsupported_condition_type() {
        let input = r#"MSH|^~\&|ORBIS|KH|RECAPP|ORBIS|202111230904||ADT^A03|62325574|P|2.5|||||D||DE
EVN|A03|202111230904|202111230904||Muster
PID|1|1396227|1396227||Test^Anton||19510704|M|||Teststr. 26^^Wetzlar^^35578^D^L||0151/123123123^^CP|||M|or|||||||N||SYR
DG1|1||K42.9^Hernia umbilicalis ohne Einklemmung und ohne Gangrän^icd10gm2022||20230101131500|do-not-know|||||||||1|ABCDEFGH^^^^^^^^^^^^^^^^^^^^^^KCH||||12345677|U
"#;
        let msg = Message::parse_with_lenient_newlines(input, true).unwrap();
        let x = &map_conditions(&msg, &get_test_config());
        match x {
            Ok(_) => panic!("we have an unsupported condition type - this is not OK!"),

            Err(MappingError::MessageError(UnsupportedContentError(_, _))) => {
                println!("got UnsupportedContentError as expected");
            }
            Err(c) => panic!("UnsupportedContentError was expected but found {}", c),
        }
    }

    #[test]
    fn test_mothers_encounter_ref() {
        let hl7 = read_test_resource("a08_test.hl7");
        let msg = Message::parse_with_lenient_newlines(&hl7, true).expect("parse hl7 failed");

        let config = get_test_config();

        let result = map(&msg, config.clone(), &get_dummy_resources());

        assert!(result.is_ok());

        let enc: Option<Encounter> = result
            .unwrap()
            .first()
            .unwrap()
            .resource
            .clone()
            .unwrap()
            .try_into()
            .ok();

        match enc {
            None => {
                panic!("encounter resource expected!")
            }
            Some(enc) => {
                println!("{:?}", enc);
                match enc.part_of.clone() {
                    Some(r) => {
                        let ref_value = r.reference.as_ref().unwrap().to_string();
                        let expected = resource_ref(
                            &ResourceType::Encounter,
                            "12345678",
                            config.fall.einrichtungskontakt.system.as_str(),
                        )
                        .unwrap();

                        assert_eq!(ref_value, expected.reference.as_ref().unwrap().as_str())
                    }
                    _ => {
                        panic!("expected 'part_of' value as reference to mothers encounter")
                    }
                }
            }
        }
    }

    #[test]
    fn test_service_type_by_pv1_39() {
        let input = r#"MSH|^~\&|ORBIS|KH|WEBEPA|KH|20251102212117||ADT^A08^ADT_A01|12332112|P|2.5||123788998|NE|NE||8859/1
EVN|A08|202511022120||11036_123456789|ZZZZZZZZ|202511022120
PID|1|9999999|9999999|88888888|Nachname^Vorname^^^^^L||20251102|M|||Strasse. 1&Strasse.&1^^Stadt^^30000^DE^L~^^Stadt^^^^B DL||0000000000000^PRN^PH^^^00000^0000000^^^^^000000000000|||U|||||12345678^^^KH^VN~1234567^^^KH^PT||Stadt|J|1|DE|||201103240800|Y
PV1|1|I|^^^KLINIKUM^961640|R^^HL7~01^Normalfall^11||||^^^^^^^^^L^^^^^^^^^^^^^^^^^^^^^^^^^^^BSNR||N||||||N|||88888888||K|||||||||||||||01|||0800|9||||202511022120|202511022120||||||A
ZBE|55555555^ORBIS|202511022120|202511022120|UPDATE
"#;
        let msg = Message::parse_with_lenient_newlines(input, true).unwrap();

        let actual = get_service_type(&msg, &get_dummy_resources())
            .unwrap()
            .and_then(|c| {
                c.coding
                    .first()
                    .and_then(|c| c.as_ref())
                    .and_then(|c| c.code.clone())
            });

        assert_eq!(actual, Some("0800".into()));
    }

    #[test]
    fn test_service_type_by_pv1_3() {
        let input = r#"MSH|^~\&|ORBIS|KH|WEBEPA|KH|20251102212117||ADT^A08^ADT_A01|12332112|P|2.5||123788998|NE|NE||8859/1
EVN|A08|202511022120||11036_123456789|ZZZZZZZZ|202511022120
PID|1|9999999|9999999|88888888|Nachname^Vorname^^^^^L||20251102|M|||Strasse. 1&Strasse.&1^^Stadt^^30000^DE^L~^^Stadt^^^^BDL||0000000000000^PRN^PH^^^00000^0000000^^^^^000000000000|||U|||||12345678^^^KH^VN~1234567^^^KH^PT||Stadt|J|1|DE|||201103240800|Y
PV1|1|I|POL1234^BSP-2-2^2^POL^KLINIKUM^961640|R^^HL7~01^Normalfall^11||||^^^^^^^^^L^^^^^^^^^^^^^^^^^^^^^^^^^^^BSNR||N||||||N|||88888888||K|||||||||||||||01||||9||||202511022120|202511022120||||||A
ZBE|55555555^ORBIS|202511022120|202511022120|UPDATE
"#;
        let msg = Message::parse_with_lenient_newlines(input, true).unwrap();

        let actual = get_service_type(&msg, &get_dummy_resources())
            .unwrap()
            .and_then(|c| {
                c.coding
                    .first()
                    .and_then(|c| c.as_ref())
                    .and_then(|c| c.code.clone())
            });

        assert_eq!(actual, Some("0800".into()));
    }

    #[test]
    fn test_service_type_unknown_department() {
        let input = r#"MSH|^~\&|ORBIS|KH|WEBEPA|KH|20251102212117||ADT^A08^ADT_A01|12332112|P|2.5||123788998|NE|NE||8859/1
EVN|A08|202511022120||11036_123456789|ZZZZZZZZ|202511022120
PID|1|9999999|9999999|88888888|Nachname^Vorname^^^^^L||20251102|M|||Strasse. 1&Strasse.&1^^Stadt^^30000^DE^L~^^Stadt^^^^BDL||0000000000000^PRN^PH^^^00000^0000000^^^^^000000000000|||U|||||12345678^^^KH^VN~1234567^^^KH^PT||Stadt|J|1|DE|||201103240800|Y
PV1|1|I|POL1234^BSP-2-2^2^XXX^KLINIKUM^961640|R^^HL7~01^Normalfall^11||||^^^^^^^^^L^^^^^^^^^^^^^^^^^^^^^^^^^^^BSNR||N||||||N|||88888888||K|||||||||||||||01||||9||||202511022120|202511022120||||||A
ZBE|55555555^ORBIS|202511022120|202511022120|UPDATE
"#;
        let msg = Message::parse_with_lenient_newlines(input, true).unwrap();

        let actual = get_service_type(&msg, &get_dummy_resources());

        assert!(matches!(
            actual,
            Err(MappingError::MissingResourceError {
                resource: _,
                value: _
            })
        ));
    }

    #[test]
    fn encounter_identifier_type_entries_different_systems() {
        let input = r#"MSH|^~\&|ORBIS|KH|RECAPP|ORBIS|202111221030||ADT^A01|62293727|P|2.5||123456789|NE|NE||8859/1
EVN|A01|202111221030|202111221029||EIDAMN
PID|1|1499653|1499653||Test^Meinrad^^Graf^von^Dr.^L|Test|202301181003|M|||Test Str.  27^^Bad Test^^57334^D^L||02752/1672^^PH|||M|rk|||||||N||D||||N|
NK1|1|Fr. Test|14^Ehefrau||s.Pat.||||||||||U|^YYYYMMDDHHMMSS|||||||||||||||||^^^ORBIS^PN~^^^ORBIS^PI~^^^ORBIS^PT
PV1|1|I|POLPOLAMB^^^POL^POLPOL^945400^^^|R^^HL7~01^Normalfall^301||||||N||||||N|||00000000||K|||||||||||||||01||||9||||202211101359|202211101359||||||AIN1|1|102171012|KKH|KKH Allianz|^^Leipzig^^04017^D||||Ersatzkassen^13^^^1&gesetzlich|||||||Mustermann^Max||19470128|Mustergasse 10^^Musterort^^33333^D|||1|||||||201111090942||R||||||||||||M| |||||1234567890^^^^^^^20130331
PV2|||01^KH-Behandlung, vollstat.^301||||||202203040000|||||||||||||N||I||||||||||||N
IN2|1||||||||||||||||||||||||||||^PC^100^K
ZBE|30674176^ORBIS|202208221309||INSERT
"#;
        let msg = Message::parse_with_lenient_newlines(input, true).unwrap();

        let result = map(&msg, get_test_config(), &get_dummy_resources());

        result
            .map_err(|e| panic!("failed with error: {}", e.to_string()))
            .unwrap()
            .iter()
            .for_each(|entry| {
                if let Some(enc) = entry.resource.as_ref() {
                    match Encounter::try_from(enc.clone()).unwrap() {
                        Encounter(e) => {
                            let first_identifier: Identifier =
                                e.identifier.first().unwrap().clone().unwrap();

                            let first_identifier_type_coding: Option<&Coding> = first_identifier
                                .r#type
                                .as_ref()
                                .ok_or(None::<&Coding>)
                                .map(|a| a.coding.first())
                                .map(|a| a.unwrap().as_ref())
                                .ok()
                                .and_then(|a| a);

                            let second_identifier = e.identifier.last().unwrap().clone().unwrap();
                            let second_identifier_type_coding: Option<&Coding> = second_identifier
                                .r#type
                                .as_ref()
                                .ok_or(None::<&Coding>)
                                .unwrap()
                                .coding
                                .last()
                                .unwrap()
                                .as_ref();
                            if let (
                                Some(first_identifier_type_coding),
                                Some(second_identifier_type_coding),
                            ) = (first_identifier_type_coding, second_identifier_type_coding)
                            {
                                assert_ne!(
                                    first_identifier_type_coding.system.as_ref().unwrap(),
                                    second_identifier_type_coding.system.as_ref().unwrap()
                                );
                            } else {
                                assert!(first_identifier.value.is_some());
                                assert!(first_identifier_type_coding.is_none());
                                assert!(second_identifier_type_coding.is_some())
                            }
                        }
                    }
                };
            });
    }

    #[test]
    fn test_encounter_no_location() {
        let input = r#"MSH|^~\&|ORBIS|KH|WEBEPA|KH|20251102212117||ADT^A08^ADT_A01|12332112|P|2.5||123788998|NE|NE||8859/1
EVN|A08|202511022120||11036_123456789|ZZZZZZZZ|202511022120
PID|1|9999999|9999999|88888888|Nachname^Vorname^^^^^L||20251102|M|||Strasse. 1&Strasse.&1^^Stadt^^30000^DE^L~^^Stadt^^^^BDL||0000000000000^PRN^PH^^^00000^0000000^^^^^000000000000|||U|||||12345678^^^KH^VN~1234567^^^KH^PT||Stadt|J|1|DE|||201103240800|Y
PV1|1|I|^^^^KLINIKUM^000000|R^^HL7~01^Normalfall^11||||^^^^^^^^^L^^^^^^^^^^^^^^^^^^^^^^^^^^^BSNR||N||||||N|||88888888||K|||||||||||||||01||||9||||202511022120|202511022120||||||A
ZBE|55555555^ORBIS|202511022120|202511022120|UPDATE
"#;
        let msg = Message::parse_with_lenient_newlines(input, true).unwrap();

        let actual = map(&msg, get_test_config(), &get_dummy_resources()).unwrap();

        assert_eq!(actual.len(), 1);
    }

    #[test]
    fn test_encounter_intensiv_ward() {
        let input = r#"MSH|^~\&|ORBIS|KH|WEBEPA|KH|20251102212117||ADT^A08^ADT_A01|12332112|P|2.5||123788998|NE|NE||8859/1
EVN|A08|202511022120||11036_123456789|ZZZZZZZZ|202511022120
PID|1|9999999|9999999|88888888|Nachname^Vorname^^^^^L||20251102|M|||Strasse. 1&Strasse.&1^^Stadt^^30000^DE^L~^^Stadt^^^^BDL||0000000000000^PRN^PH^^^00000^0000000^^^^^000000000000|||U|||||12345678^^^KH^VN~1234567^^^KH^PT||Stadt|J|1|DE|||201103240800|Y
PV1|1|I|ANA^3^4^POL^KLINIKUM^000000|R^^HL7~01^Normalfall^11||||^^^^^^^^^L^^^^^^^^^^^^^^^^^^^^^^^^^^^BSNR||N||||||N|||88888888||K|||||||||||||||01||||9||||202511022120|202511022120||||||A
ZBE|55555555^ORBIS|202511022120|202511022120|UPDATE
"#;
        let msg = Message::parse_with_lenient_newlines(input, true).unwrap();

        let actual =
            map_versorgungsstellenkontakt(&msg, &get_test_config(), &get_dummy_resources())
                .unwrap()
                .unwrap();

        let type_coding = get_enc_type_coding(&actual, 1);
        assert_eq!(
            type_coding.code.clone().unwrap().as_str(),
            "intensivstationaer"
        );

        let actual = map_abteilungskontakt(&msg, &get_test_config(), &get_dummy_resources())
            .unwrap()
            .unwrap();

        let type_coding = get_enc_type_coding(&actual, 1);
        assert_eq!(
            type_coding.code.clone().unwrap().as_str(),
            "intensivstationaer"
        );

        let f = get_enc_type_coding(&actual, 0);
        assert_eq!(f.code.clone().unwrap().as_str(), "abteilungskontakt");

        let actual =
            map_einrichtungskontakt(&msg, &get_test_config(), &get_dummy_resources()).unwrap();

        let type_coding = actual
            .r#type
            .first()
            .unwrap()
            .as_ref()
            .unwrap()
            .coding
            .clone();
        assert_eq!(type_coding.clone().len(), 1);

        let f = type_coding.first().unwrap().as_ref().unwrap();
        assert_eq!(f.code.clone().unwrap().as_str(), "einrichtungskontakt");
    }

    fn get_enc_type_coding(actual: &Encounter, index: usize) -> Coding {
        let type_coding = actual
            .r#type
            .first()
            .unwrap()
            .as_ref()
            .unwrap()
            .coding
            .get(index)
            .unwrap()
            .as_ref()
            .unwrap()
            .clone();
        type_coding
    }

    #[test]
    fn test_encounter_invalid_bed_status() {
        let input = r#"MSH|^~\&|ORBIS|KH|WEBEPA|KH|20251102212117||ADT^A08^ADT_A01|12332112|P|2.5||123788998|NE|NE||8859/1
EVN|A08|202511022120||11036_123456789|ZZZZZZZZ|202511022120
PID|1|9999999|9999999|88888888|Nachname^Vorname^^^^^L||20251102|M|||Strasse. 1&Strasse.&1^^Stadt^^30000^DE^L~^^Stadt^^^^BDL||0000000000000^PRN^PH^^^00000^0000000^^^^^000000000000|||U|||||12345678^^^KH^VN~1234567^^^KH^PT||Stadt|J|1|DE|||201103240800|Y
PV1|1|INVALID|^^^^KLINIKUM^000000|R^^HL7~01^Normalfall^11||||^^^^^^^^^L^^^^^^^^^^^^^^^^^^^^^^^^^^^BSNR||N||||||N|||88888888||K|||||||||||||||01||||9||||202511022120|202511022120||||||A
ZBE|55555555^ORBIS|202511022120|202511022120|UPDATE
"#;
        let msg = Message::parse_with_lenient_newlines(input, true).unwrap();

        let actual = map(&msg, get_test_config(), &get_dummy_resources());
        assert!(actual.is_err());
        assert_eq!(
            actual.unwrap_err().to_string(),
            MappingError::Other(anyhow!("Invalid encounter_class code (PV1.2): INVALID"))
                .to_string()
        );
    }

    #[test]
    fn test_nachstationaer() {
        let hl7 = read_test_resource("a07_nachstationaer_test.hl7");
        let msg = Message::parse_with_lenient_newlines(&hl7, true).expect("parse hl7 failed");

        let res = get_dummy_resources();
        let abteilung_result = map_abteilungskontakt(&msg, &get_test_config(), &res)
            .unwrap()
            .unwrap();
        assert_eq!(
            abteilung_result
                .r#type
                .get(0)
                .unwrap()
                .as_ref()
                .unwrap()
                .coding
                .clone()
                .get(1)
                .unwrap()
                .as_ref()
                .unwrap()
                .code
                .as_ref()
                .unwrap(),
            "nachstationaer"
        );
        assert_eq!(abteilung_result.class.code.as_ref().unwrap(), "AMB");

        let einrichtung_result = map_einrichtungskontakt(&msg, &get_test_config(), &res).unwrap();
        assert_eq!(
            einrichtung_result
                .r#type
                .get(0)
                .unwrap()
                .as_ref()
                .unwrap()
                .coding
                .clone()
                .get(1)
                .unwrap()
                .as_ref()
                .unwrap()
                .code
                .as_ref()
                .unwrap(),
            "nachstationaer"
        );
        assert_eq!(einrichtung_result.class.code.as_ref().unwrap(), "IMP");
    }

    #[test]
    fn test_teilsstationaer() {
        let hl7 = read_test_resource("a06_teilsstationaer_test.hl7");
        let msg = Message::parse_with_lenient_newlines(&hl7, true).expect("parse hl7 failed");
        let res = get_dummy_resources();
        let abteilung_result = map_abteilungskontakt(&msg, &get_test_config(), &res)
            .unwrap()
            .unwrap();
        assert_eq!(
            abteilung_result
                .r#type
                .get(0)
                .unwrap()
                .as_ref()
                .unwrap()
                .coding
                .clone()
                .get(1)
                .unwrap()
                .as_ref()
                .unwrap()
                .code
                .as_ref()
                .unwrap(),
            "teilstationaer"
        );
        assert_eq!(abteilung_result.class.code.as_ref().unwrap(), "SS");

        let einrichtung_result = map_einrichtungskontakt(&msg, &get_test_config(), &res).unwrap();
        assert_eq!(
            einrichtung_result
                .r#type
                .get(0)
                .unwrap()
                .as_ref()
                .unwrap()
                .coding
                .clone()
                .get(1)
                .unwrap()
                .as_ref()
                .unwrap()
                .code
                .as_ref()
                .unwrap(),
            "teilstationaer"
        );
        assert_eq!(einrichtung_result.class.code.as_ref().unwrap(), "SS");
    }
    #[test]
    fn test_diagnose_multiple_use() {
        let hl7 = read_test_resource("a08_test.hl7");
        let msg = Message::parse_with_lenient_newlines(&hl7, true).expect("parse hl7 failed");

        let config = get_test_config();
        let result = map_conditions(&msg, &config).ok().unwrap();
        assert!(result.iter().all(|a| a.is_some()));
        let mut index = 0;
        result.iter().for_each(|entry| {
            let entry = entry
                .as_ref()
                .unwrap()
                .clone()
                .r#use
                .unwrap()
                .coding
                .clone();

            let count_diag_type = entry
                .iter()
                .filter(|a| {
                    a.is_some()
                        && a.as_ref().is_some_and(|a| {
                            a.system.clone().is_some_and(|s| {
                                s.eq("http://fhir.de/CodeSystem/KontaktDiagnoseProzedur")
                            })
                        })
                })
                .count();
            assert!(
                count_diag_type < 2,
                "diagnose type count at index {} was {}",
                index,
                count_diag_type
            );

            let count_diag_sub_type = entry
                .iter()
                .filter(|a| {
                    a.is_some()
                        && a.as_ref().is_some_and(|a| {
                            a.system.clone().is_some_and(|s| {
                                s.eq("http://terminology.hl7.org/CodeSystem/diagnosis-role")
                            })
                        })
                })
                .count();
            assert!(
                count_diag_type < 2,
                "diagnose sub type count at index {} was {}",
                index,
                count_diag_sub_type
            );

            index += 1;
        });

        //                    .eq("http://fhir.de/ValueSet/Diagnosesubtyp")
    }
}
