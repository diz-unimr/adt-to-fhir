use crate::config::Fhir;
use crate::error::{FormattingError, MappingError, MessageAccessError};
use crate::fhir::mapper::{
    EntryRequestType, bundle_entry, is_inpatient_location, map_bed_location, map_room_location,
    map_ward_location, parse_datetime, parse_fab, resource_ref,
};
use crate::fhir::resources::ResourceMap;
use crate::fhir::terminology::{
    AufnahmeGrundStelle, EntlassgrundStelle, diagnose_role_coding, kontakt_diagnose_procedures,
};
use crate::hl7::parser::{MessageType, message_type, query};
use anyhow::anyhow;
use fhir_model::DateTime;
use fhir_model::r4b::codes::{EncounterStatus, IdentifierUse};
use fhir_model::r4b::resources::{
    BundleEntry, Encounter, EncounterBuilder, EncounterDiagnosis, EncounterHospitalization,
    EncounterLocation, Location, ResourceType,
};
use fhir_model::r4b::types::{
    CodeableConcept, Coding, Extension, ExtensionValue, Identifier, Meta, Period, Reference,
};
use fhir_model::time::OffsetDateTime;
use hl7_parser::Message;
use hl7_parser::message::Field;
use std::num::NonZeroU32;

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
            let care_site_enc = map_versorgungsstellenkontakt(msg, &config, resources)?;
            // todo
            // ...

            Ok(vec![
                bundle_entry(enc_admit, EntryRequestType::UpdateAsCreate)?,
                bundle_entry(enc_dep, EntryRequestType::UpdateAsCreate)?,
                bundle_entry(care_site_enc, EntryRequestType::UpdateAsCreate)?,
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
    Ok(query(msg, "PV1.2").is_some_and(|f| f == "H"))
}

fn map_einrichtungskontakt(msg: &Message, config: &Fhir) -> Result<Encounter, MappingError> {
    // base encounter
    let mut enc = base_encounter(msg, config, &EncounterType::Einrichtungskontakt)?
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

    Ok(enc)
}

fn map_aufnahmegrund(msg: &Message) -> Result<Vec<Extension>, MappingError> {
    let mut result = vec![];

    // Aufnahmegrund
    // 1. und 2. Stelle
    if let Some(erste_und_zweite) = query(msg, "PV2.3.1")
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
    if let Some((Some(dritte), Some(vierte))) = query(msg, "PV1.4[2].1")
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
    let mut result = vec![];

    // 1. und 2. Stelle
    if let Some(erste_und_zweite) = query(msg, "PV1.36.1")
        .map(EntlassgrundStelle::ErsteUndZweite)
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

    // 3. Stelle
    if let Some(dritte) = query(msg, "PV1.40.1")
        .map(EntlassgrundStelle::Dritte)
        .and_then(Option::<Coding>::from)
        .map(|c| {
            Extension::builder()
                .url("DritteStelle".to_string())
                .value(ExtensionValue::Coding(c))
                .build()
        })
    {
        result.push(dritte?);
    }

    Ok(result)
}

fn map_abteilungskontakt(
    msg: &Message,
    config: &Fhir,
    resources: &ResourceMap,
) -> Result<Encounter, MappingError> {
    // base encounter
    let mut enc = base_encounter(msg, config, &EncounterType::Fachabteilungskontakt)?.build()?;

    let fab = parse_fab(msg)?;
    // fab related
    if let Some(f) = fab {
        // fab schluessel
        enc.service_type = resources.map_fab_schluessel(f)?;
        // service provider
        enc.service_provider = Some(fab_ref(f)?);
    }

    Ok(enc)
}

fn base_encounter(
    msg: &Message,
    config: &Fhir,
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
        .r#type(map_encounter_type(msg, enc_type)?)
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
    let zbe_id = query(msg, "ZBE.1.1").ok_or(MessageAccessError::Other(anyhow!(
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
    let pid = query(msg, "PID.2").ok_or(anyhow!("missing pid value in PID.2"))?;

    resource_ref(&ResourceType::Patient, pid, sid)
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
    let code = query(msg, "PV1.4.1").ok_or(MappingError::Other(anyhow!(
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
            start =
                parse_datetime(query(msg, "PV1.44").ok_or(anyhow!("empty datetime in PV1.44"))?)?;

            end = match query(msg, "PV1.45") {
                Some(end) => Some(parse_datetime(end)?),
                None => None,
            };
        }
        EncounterType::Fachabteilungskontakt | EncounterType::Versorgungsstellenkontakt => {
            start = parse_datetime(query(msg, "ZBE.2").ok_or(anyhow!("empty datetime in ZBE-2"))?)?;
            end = match query(msg, "ZBE.3") {
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

fn map_visit_number<'a>(msg: &'a Message) -> Result<&'a str, anyhow::Error> {
    match message_type(msg)? {
        MessageType::A14 => Ok(query(msg, "PID.4").ok_or(anyhow!("empty visit number in PID.4"))?),
        _ => Ok(query(msg, "PV1.19").ok_or(anyhow!("empty visit number in PV1.19"))?),
    }
}

fn map_meta(config: &Fhir) -> Result<Meta, anyhow::Error> {
    Ok(Meta::builder()
        .profile(vec![Some(config.fall.profile.clone())])
        .source(config.meta_source.to_string())
        .build()?)
}

fn map_encounter_class(msg: &Message) -> Result<Coding, anyhow::Error> {
    let code = query(msg, "PV1.2").ok_or(anyhow!("empty encounter_class value in PV1.2"))?;
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
    if let Some(code) = query(msg, "PV1.2") {
        match code {
            // todo: the following are missing
            // O ("Ambulantes Operieren") => operation
            // I ("Normalstationär") => normalstationaer
            // I ("Intensivstationär") => intensivstationaer
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
            _ => Err(anyhow!("Invalid kontakt_art code (PV1.2): {}", code))
                .map_err(MappingError::Other)?,
        }
    } else {
        Ok(None)
    }
}

fn map_versorgungsstellenkontakt(
    msg: &Message,
    config: &Fhir,
    resources: &ResourceMap,
) -> Result<Encounter, MappingError> {
    let versorgungskontakt =
        base_encounter(msg, config, &EncounterType::Versorgungsstellenkontakt)?
            .part_of(resource_ref(
                &ResourceType::Encounter,
                query(msg, "ZBE.1")
                    .ok_or(MessageAccessError::MissingMessageSegment("ZBE".to_string()))?,
                &config.fall.abteilungskontakt.system,
            )?)
            .service_provider(
                parse_fab(msg)?
                    .and_then(|f| fab_ref(f).ok())
                    .ok_or(MappingError::Other(anyhow!("missing service provider")))?,
            )
            .location(map_lvl_3_locations(msg, config, resources)?)
            .status(map_encounter_status(&map_period(
                msg,
                &EncounterType::Versorgungsstellenkontakt,
            )?));

    versorgungskontakt
        .build()
        .map_err(MappingError::BuilderError)
}

fn map_lvl_3_locations(
    msg: &Message,
    config: &Fhir,
    resources: &ResourceMap,
) -> Result<Vec<Option<EncounterLocation>>, MappingError> {
    let mut locations: Vec<Option<EncounterLocation>> = vec![];

    if let Some(department) = parse_fab(msg)? {
        // department location should be always available
        locations.push(Some(
            map_ward_location(msg, department, config, resources)?.to_encounter_location()?,
        ));

        if is_inpatient_location(msg)? {
            let ward = query(msg, "PV1.3.1");
            let room = query(msg, "PV1.3.2");
            let bed = query(msg, "PV1.3.3");
            if let (Some(ward), Some(room)) = (ward, room) {
                locations.push(Some(
                    map_room_location(config, ward, room)?.to_encounter_location()?,
                ));
            }

            if let (Some(ward), Some(room), Some(bed)) = (ward, room, bed) {
                locations.push(Some(
                    map_bed_location(config, ward, room, bed)?.to_encounter_location()?,
                ));
            }
        }
        Ok(locations)
    } else {
        Err(MappingError::Other(anyhow!(
            "could not determinate patient location"
        )))
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
                .map_err(FormattingError::ParseFloatError)?
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

            res.push(Some(
                EncounterDiagnosis::builder()
                    .condition(condition_reference)
                    .r#use(CodeableConcept::builder().coding(codings).build()?)
                    .rank(rank_nz)
                    .build()?,
            ));
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

            result.push(diagnose_role_coding("post-op"));
        }

        // DRG Diagnose
        "DD" => {
            if is_main_condition {
                result.push(kontakt_diagnose_procedures("principle-DRG"));
            } else {
                result.push(kontakt_diagnose_procedures("secondary-DRG"));
            }
        }

        // Abrechungsdiagnose
        "AR" | "Abr" => {
            result.push(diagnose_role_coding("billing"));
        }

        // Präoperative Diagnose
        "PR" | "Präop" => {
            result.push(kontakt_diagnose_procedures("surgery-diagnosis"));
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
                return Err(MessageAccessError::UnsupportedContentError(format!(
                    "Unsupported value at DG1-6.1 '{}'",
                    condition_type_local
                ))
                .into());
            }
            if condition_type_local.is_empty() {
                return Err(MessageAccessError::UnsupportedContentError(format!(
                    "Unsupported empty value at DG1-6.1 '{}'",
                    condition_type_local
                ))
                .into());
            }
        }
    }

    Ok(result)
}

trait ToEncounterLocation<EncounterLocation> {
    fn to_encounter_location(&self) -> EncounterLocation;
}

impl ToEncounterLocation<Result<EncounterLocation, MappingError>> for Location {
    fn to_encounter_location(&self) -> Result<EncounterLocation, MappingError> {
        if let Some(identifier) = self
            .identifier
            .first()
            .ok_or(MappingError::Other(anyhow!("failed to access identifier")))?
            .clone()
        {
            return Ok(EncounterLocation::builder()
                .physical_type(
                    self.physical_type
                        .clone()
                        .ok_or(MappingError::Other(anyhow!(
                            "physical type ist missing".to_string()
                        )))?,
                )
                .location(Reference::builder().identifier(identifier).build()?)
                .build()?);
        };
        Err(MappingError::Other(anyhow!("failed to access identifier")))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{FallConfig, LocationConfig, PatientConfig, SystemConfig};
    use crate::error::FormattingError::ParseFloatError;
    use crate::test_utils::tests::{get_dummy_resources, get_test_config, read_test_resource};
    use hl7_parser::Message;
    use rstest::rstest;
    use std::default::Default;
    use std::num::NonZero;
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
            map_versorgungsstellenkontakt(&msg, &get_test_config(), &get_dummy_resources());

        assert_eq!(actual.unwrap().location.len(), 3);
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

        assert_eq!(actual, expected);
    }

    #[rstest]
    #[case(0, "CC,department-main-diagnosis,DD", 1)]
    #[case(1, "CM,treatment-diagnosis", 2)]
    #[case(2, "CC,AD", 1)]
    fn map_conditions_test(
        #[case] entry_index: usize,
        #[case] codings_expected: String,
        #[case] rank_expected: u32,
    ) {
        let binding = read_test_resource("a08_test.hl7");
        let msg = Message::parse_with_lenient_newlines(binding.as_str(), true).unwrap();
        let result = &map_conditions(&msg, &get_test_config()).ok().unwrap();

        assert_eq!(result.len(), 6);

        let first_entry = result.get(entry_index).unwrap().as_ref().unwrap();
        assert_eq!(first_entry.rank, NonZero::new(rank_expected));

        codings_expected.split(',').for_each(|s| {
            assert!(
                first_entry
                    .r#use
                    .as_ref()
                    .unwrap()
                    .coding
                    .iter()
                    .find(|c| c.as_ref().unwrap().code.as_ref().unwrap() == s)
                    .is_some()
            );
        });

        let amount_of_uses = codings_expected.split(',').count();
        assert_eq!(
            amount_of_uses,
            first_entry.r#use.as_ref().unwrap().coding.len()
        );
    }
    #[test]
    fn map_condition_identifier_test() {
        let binding = read_test_resource("a03_test.hl7");
        let msg = Message::parse_with_lenient_newlines(binding.as_str(), true).unwrap();
        let result = &map_conditions(&msg, &get_test_config());

        assert_eq!(result.is_ok(), true);
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
            .get(1)
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
            .get(2)
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
        match x {
            Ok(_) => panic!("ParseFloatError was expected but result was OK!"),
            Err(MappingError::FormattingError(ParseFloatError(l))) => {
                println!("got ParseFloatError as expected {:#?}", l);
            }
            Err(c) => panic!("ParseFloatError was expected but found {}", c),
        }
    }
}
