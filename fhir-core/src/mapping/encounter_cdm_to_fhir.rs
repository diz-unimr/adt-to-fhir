use crate::canonical_model::canonical_encounter::EncounterCdm;
use crate::fhir_error::FhirMappingError::UnsupportedValueError;
use crate::fhir_error::{ContentError, FhirMappingError};
use crate::mapping::encounter_cdm_to_fhir::EncounterLevel::Versorgungsstellenkontakt;
use crate::mapping::misc::{parse_datetime_from_offset, resource_ref};
use adt_config::config::Fhir;

use fhir_model::BuilderError;
use fhir_model::r4b::codes::{EncounterStatus, IdentifierUse};
use fhir_model::r4b::resources::{BundleEntry, Encounter, EncounterBuilder, ResourceType};
use fhir_model::r4b::types::{CodeableConcept, Coding, Identifier, Meta, Period};
use fhir_model::time::OffsetDateTime;

#[derive(PartialEq, Debug)]
enum EncounterLevel {
    Einrichtungskontakt,
    Fachabteilungskontakt,
    Versorgungsstellenkontakt,
}

pub fn map_default_identifier_enc(
    system: String,
    value: String,
) -> Result<Identifier, BuilderError> {
    Identifier::builder()
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
        .build()
}
pub fn map_meta_encounter(config: &Fhir) -> Result<Meta, BuilderError> {
    Ok(Meta::builder()
        .profile(vec![Some(config.fall.profile.clone())])
        .source(config.meta_source.to_string())
        .build()?)
}

pub fn map(input: EncounterCdm, config: &Fhir) -> Result<Vec<BundleEntry>, ContentError> {
    let mut result: Vec<BundleEntry> = vec![];
    let mut encounter: Vec<Encounter> = vec![];

    if let Some(lvl_1_enc) = map_lvl1(&input, config)? {
        encounter.push(lvl_1_enc);
    }
    if let Some(lvl_2_enc) = map_lvl2(&input, config)? {
        encounter.push(lvl_2_enc);
    }
    if let Some(lvl_3_enc) = map_lvl3(&input, config)? {
        encounter.push(lvl_3_enc);
    }

    panic!("not implemented")
}

fn map_lvl1(input: &EncounterCdm, adt_config: &Fhir) -> Result<Option<Encounter>, ContentError> {
    todo!()
}

fn map_lvl2(input: &EncounterCdm, adt_config: &Fhir) -> Result<Option<Encounter>, ContentError> {
    todo!()
}

fn map_lvl3(input: &EncounterCdm, adt_config: &Fhir) -> Result<Option<Encounter>, ContentError> {
    todo!()
}

fn base_encounter(
    cdm: &EncounterCdm,
    config: &Fhir,
    enc_type: &EncounterLevel,
) -> Result<EncounterBuilder, FhirMappingError> {
    let period = map_period(cdm, enc_type)?;

    let admit = Encounter::builder()
        .meta(map_meta_encounter(config)?)
        .identifier(vec![
            // identifier for Einrichtungskontakt
            Some(map_level_identifier(enc_type, config, cdm)?),
            // common identifier is last
            Some(map_default_identifier_enc(
                config.fall.system.clone(),
                cdm.visit_number.clone(),
            )?),
        ])
        .class(map_encounter_class(cdm)?)
        .r#type(map_encounter_type(cdm, enc_type)?)
        .subject(resource_ref(
            &ResourceType::Patient,
            cdm.pid.as_str(),
            config.person.system.as_str(),
        )?)
        .period(period.clone())
        // set status depends on period.start / period.end
        .status(map_encounter_status(&period));

    Ok(admit)
}
fn map_encounter_status(period: &Period) -> EncounterStatus {
    match (period.start.as_ref(), period.end.as_ref()) {
        (None, None) => EncounterStatus::Unknown,
        (_, Some(_)) => EncounterStatus::Finished,
        (Some(_), _) => EncounterStatus::InProgress,
    }
}
fn map_period(cdm: &EncounterCdm, lvl: &EncounterLevel) -> Result<Period, ContentError> {
    let start: OffsetDateTime;
    let end: Option<OffsetDateTime>;
    match lvl {
        EncounterLevel::Einrichtungskontakt => {
            start = cdm.admission_datetime;
            end = cdm.discharge
        }
        EncounterLevel::Fachabteilungskontakt | Versorgungsstellenkontakt => {
            start = cdm.movement_start;
            end = match cdm.movement_end {
                // if end of movement is available use it
                Some(_) => cdm.movement_end,
                None => {
                    if cdm.bed_status == "NS" || cdm.map_as_registration {
                        // fixme: cleaner impl. set data absent reason
                        Some(cdm.movement_start)
                    } else {
                        None
                    }
                }
            };
        }
    }

    let mut period: Period = Period::builder()
        .start(parse_datetime_from_offset(&start)?)
        .build()?;
    if let Some(e) = end {
        period.end = Some(parse_datetime_from_offset(&e)?);
    }

    Ok(period)
}

fn map_level_identifier(
    encounter_type: &EncounterLevel,
    config: &Fhir,
    cdm: &EncounterCdm,
) -> Result<Identifier, BuilderError> {
    let (system, value) = match encounter_type {
        EncounterLevel::Einrichtungskontakt => {
            (&config.fall.einrichtungskontakt.system, &cdm.visit_number)
        }
        EncounterLevel::Fachabteilungskontakt => {
            (&config.fall.abteilungskontakt.system, &cdm.movement_id)
        }
        EncounterLevel::Versorgungsstellenkontakt => (
            &config.fall.versorgungsstellenkontakt.system,
            &cdm.movement_id,
        ),
    };

    Ok(Identifier::builder()
        .system(system.clone())
        .value(value.to_string())
        .r#use(IdentifierUse::Usual)
        .build()?)
}

fn map_encounter_class(cdm: &EncounterCdm) -> Result<Coding, FhirMappingError> {
    let code = cdm.bed_status.as_ref();
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
            .display("short stay".to_string())
            .build()?),
        _ => Err(FhirMappingError::UnsupportedValueError {
            details: format!("Invalid encounter_class code (PV1.2): {}", code),
        }),
    }
}

fn map_encounter_type(
    cdm: &EncounterCdm,
    enc_type: &EncounterLevel,
) -> anyhow::Result<Vec<Option<CodeableConcept>>> {
    // Kontaktebene
    let kontaktebene = CodeableConcept::builder()
        .coding(vec![Some(enc_type.into())])
        .build()?;

    let kontaktart: Option<CodeableConcept> = {
        if let Some(art) = map_kontaktart(cdm, enc_type)? {
            // Kontaktart
            Some(CodeableConcept::builder().coding(vec![Some(art)]).build()?)
        } else {
            None
        }
    };
    if let Some(kontaktart) = kontaktart {
        Ok(vec![Some(kontaktebene), Some(kontaktart)])
    } else {
        Ok(vec![Some(kontaktebene)])
    }
}

fn map_kontaktart(
    cdm: &EncounterCdm,
    enc_type: &EncounterLevel,
) -> Result<Option<Coding>, FhirMappingError> {
    if &Versorgungsstellenkontakt == enc_type {
        if cdm.is_icu_location {
            return Ok(Some(
                Coding::builder()
                    .system("http://fhir.de/CodeSystem/kontaktart-de".to_string())
                    .code("intensivstationaer".to_string())
                    .display("Intensivstationär".to_string())
                    .build()?,
            ));
        }
    }

    match cdm.bed_status.as_ref() {
        "I" | "O" => {
            if cdm.map_as_registration {
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
        _ => Err(UnsupportedValueError {
            details: format!("bed status code '{}'", cdm.bed_status),
        }),
    }
}

impl From<&EncounterLevel> for Coding {
    fn from(t: &EncounterLevel) -> Self {
        match t {
            EncounterLevel::Einrichtungskontakt => Coding::builder()
                .system("http://fhir.de/CodeSystem/Kontaktebene".to_string())
                .code("einrichtungskontakt".to_string())
                .display("Einrichtungskontakt".to_string())
                .build()
                .expect("Kontaktebene coding"),
            EncounterLevel::Fachabteilungskontakt => Coding::builder()
                .system("http://fhir.de/CodeSystem/Kontaktebene".to_string())
                .code("abteilungskontakt".to_string())
                .display("Abteilungskontakt".to_string())
                .build()
                .expect("Kontaktebene coding"),
            EncounterLevel::Versorgungsstellenkontakt => Coding::builder()
                .system("http://fhir.de/CodeSystem/Kontaktebene".to_string())
                .code("versorgungsstellenkontakt".to_string())
                .display("Versorgungsstellenkontakt".to_string())
                .build()
                .expect("Kontaktebene coding"),
        }
    }
}
