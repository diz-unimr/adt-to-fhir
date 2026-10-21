use crate::canonical_model::canonical_encounter::{AdmissionType, EncounterCdm};
use crate::canonical_model::canonical_location::LocationCdm;
use crate::canonical_model::canonical_meta::ProcessingOperation;
use crate::fhir_error::FhirMappingError::UnsupportedValueError;
use crate::fhir_error::{ContentError, FhirMappingError, ParsingError};
use crate::mapping::encounter_cdm_to_fhir::EncounterLevel::{
    Fachabteilungskontakt, Versorgungsstellenkontakt,
};
use crate::mapping::fab_mapping::map_fab_schluessel;
use crate::mapping::location_cdm_to_fhir;
use crate::mapping::location_cdm_to_fhir::map_to_fhir;
use crate::mapping::misc::{
    EntryRequestType, bundle_entry, coding_data_absent_reason_unsupported, get_cc_with_one_code,
    parse_datetime_from_offset, resource_ref,
};
use crate::mapping::terminology::{
    AufnahmeGrundStelle, diagnose_role_coding, entlassgrund_dritte_stelle,
    entlassgrund_erste_und_zweite_stelle, kontakt_diagnose_procedures,
};
use adt_config::config::Fhir;
use adt_config::resources::ResourceMap;
use anyhow::anyhow;
use fhir_model::BuilderError;
use fhir_model::r4b::codes::ActionPrecheckBehavior::No;
use fhir_model::r4b::codes::{EncounterLocationStatus, EncounterStatus, IdentifierUse};
use fhir_model::r4b::resources::{
    BundleEntry, Encounter, EncounterBuilder, EncounterDiagnosis, EncounterHospitalization,
    EncounterLocation, Location, ResourceType,
};
use fhir_model::r4b::types::{
    CodeableConcept, Coding, Extension, ExtensionValue, Identifier, Meta, Period, Reference,
};
use fhir_model::time::OffsetDateTime;
use log::{Level, log};
use std::num::NonZeroU32;

#[derive(PartialEq, Debug)]
enum EncounterLevel {
    Einrichtungskontakt,
    Fachabteilungskontakt,
    Versorgungsstellenkontakt,
}

pub fn map(
    encounter_cdm: &EncounterCdm,
    config: &Fhir,
    resources: &ResourceMap,
) -> Result<Vec<BundleEntry>, FhirMappingError> {
    let mut result = vec![];

    if let Some(operation) = map_operation(&encounter_cdm.meta.operation_lv1) {
        let res = map_einrichtungskontakt(encounter_cdm, config, resources)?;
        result.push(bundle_entry(res, operation, config)?);
    }

    if let Some(operation) = map_operation(&encounter_cdm.meta.operation_lv2) {
        if let Some(res) = map_abteilungskontakt(encounter_cdm, config, resources)? {
            result.push(bundle_entry(res, operation, config)?);
        }
    }

    if let Some(operation) = map_operation(&encounter_cdm.meta.operation_lv3) {
        if let Some(res) = map_versorgungsstellenkontakt(encounter_cdm, config)? {
            result.push(bundle_entry(res, operation, config)?);
        }
    }

    Ok(result)
}

fn map_operation(input: &ProcessingOperation) -> Option<EntryRequestType> {
    match input {
        ProcessingOperation::UpdateAsCreate => Some(EntryRequestType::UpdateAsCreate),
        ProcessingOperation::CreateIfNotExists => Some(EntryRequestType::ConditionalCreate),
        ProcessingOperation::Delete => Some(EntryRequestType::Delete),
        ProcessingOperation::Patch => None,
        ProcessingOperation::Skip => None,
    }
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

fn map_einrichtungskontakt(
    cdm: &EncounterCdm,
    config: &Fhir,
    resources: &ResourceMap,
) -> Result<Encounter, FhirMappingError> {
    // base encounter
    let mut enc = base_encounter(cdm, config, &EncounterLevel::Einrichtungskontakt)?
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
    enc.hospitalization = map_hospitalization(cdm)?;

    // Aufnahmegrund
    if let Some(aufnahmegrund) = map_aufnahmegrund(cdm)? {
        enc.extension = vec![
            Extension::builder()
                .url("http://fhir.de/StructureDefinition/Aufnahmegrund".to_string())
                .extension(aufnahmegrund)
                .build()?,
        ];
    }

    enc.diagnosis = map_conditions(cdm, config)?;

    enc.part_of = map_mothers_encounter(cdm, config)?;

    Ok(enc)
}

fn map_aufnahmegrund(cdm: &EncounterCdm) -> Result<Option<Vec<Extension>>, FhirMappingError> {
    let mut result = vec![];

    // 1. und 2. Stelle
    if let Some(code) = &cdm.admission_reason_1_2
        && let Some(coding) = Option::<Coding>::from(AufnahmeGrundStelle::ErsteUndZweite(code))
    {
        result.push(coding_extension("ErsteUndZweiteStelle", coding)?);
    }

    // 3. und 4. Stelle: genau zwei Zeichen erforderlich
    if let Some(code) = &cdm.admission_reason_3_4 {
        let mut chars = code.chars();

        if let (Some(c3), Some(c4), None) = (chars.next(), chars.next(), chars.next()) {
            let (s3, s4) = (c3.to_string(), c4.to_string());

            if let Some(dritte) = Option::<Coding>::from(AufnahmeGrundStelle::Dritte(&s3))
                && let Some(vierte) = Option::<Coding>::from(AufnahmeGrundStelle::Vierte(&s4))
            {
                result.push(coding_extension("DritteStelle", dritte)?);
                result.push(coding_extension("VierteStelle", vierte)?);
            }
        } else {
            return Err(UnsupportedValueError {
                details: format!("admission reason has more than 2 digits -> '{}'", code),
            });
        }
    }

    Ok((!result.is_empty()).then_some(result))
}

fn coding_extension(url: &str, coding: Coding) -> Result<Extension, FhirMappingError> {
    Ok(Extension::builder()
        .url(url.to_string())
        .value(ExtensionValue::Coding(coding))
        .build()?)
}

fn map_hospitalization(
    cdm: &EncounterCdm,
) -> Result<Option<EncounterHospitalization>, FhirMappingError> {
    if cdm.bed_status == "O" {
        return Ok(None);
    }

    let discharge = map_entlassgrund(cdm)?;
    let admit_source = map_admit_source(cdm)?;

    match (discharge, admit_source) {
        (None, None) => Ok(None),
        (Some(discharge), Some(admit_source)) => Ok(Some(
            EncounterHospitalization::builder()
                .discharge_disposition(CodeableConcept::builder().extension(discharge).build()?)
                .admit_source(
                    CodeableConcept::builder()
                        .coding(vec![Some(admit_source)])
                        .build()?,
                )
                .build()?,
        )),
        (Some(discharge), None) => {
            // if discharge is present - admission source is mandatory by profile -> fallback data absent reason
            Ok(Some(
                EncounterHospitalization::builder()
                    .discharge_disposition(CodeableConcept::builder().extension(discharge).build()?)
                    .admit_source(coding_data_absent_reason_unsupported()?)
                    .build()?,
            ))
        }
        (None, Some(admit_source)) => Ok(Some(
            EncounterHospitalization::builder()
                .admit_source(
                    CodeableConcept::builder()
                        .coding(vec![Some(admit_source)])
                        .build()?,
                )
                .build()?,
        )),
    }
}

fn map_entlassgrund(cdm: &EncounterCdm) -> anyhow::Result<Option<Vec<Extension>>> {
    let mut extension_components = vec![];

    // 1. und 2. Stelle
    if let Some(discharge_reason_12) = &cdm.discharge_reason_12 {
        if let Some(c) = entlassgrund_erste_und_zweite_stelle(&*discharge_reason_12) {
            let erste_und_zweite = Extension::builder()
                .url("ErsteUndZweiteStelle".to_string())
                .value(ExtensionValue::Coding(c))
                .build();
            extension_components.push(erste_und_zweite?);
        }
    }

    // 3. Stelle
    if let Some(discharge_reason_3) = &cdm.discharge_reason_3 {
        if let Some(c) = entlassgrund_dritte_stelle(&*discharge_reason_3) {
            let dritte = Extension::builder()
                .url("DritteStelle".to_string())
                .value(ExtensionValue::Coding(c))
                .build();

            extension_components.push(dritte?);
        }
    }
    if !extension_components.is_empty() {
        return Ok(Some(vec![
            Extension::builder()
                .extension(extension_components)
                .url("http://fhir.de/StructureDefinition/Entlassungsgrund".to_string())
                .build()?,
        ]));
    }
    Ok(None)
}

/// currently we do not have full support of this dataitem in our hl7 messages
/// only export __G__ for birth and __N__ for emergency
fn map_admit_source(cdm: &EncounterCdm) -> anyhow::Result<Option<Coding>> {
    let admission_type = cdm.admission_type;

    if let Some(pv2_3_1) = &cdm.admission_reason_1_2
        && (pv2_3_1.eq("06") | pv2_3_1.eq("05"))
    {
        Ok(Some(
            Coding::builder()
                .system("http://fhir.de/CodeSystem/dgkev/Aufnahmeanlass".to_string())
                .code("G".to_string())
                .display("Geburt".to_string())
                .build()?,
        ))
    } else {
        match admission_type {
            // A->'Unfall/Notarztwagen', E-> 'Notfall ohne Einweisung'
            Some(AdmissionType::AccidentAmbulance) | Some(AdmissionType::Emergency) => Ok(Some(
                Coding::builder()
                    .system("http://fhir.de/CodeSystem/dgkev/Aufnahmeanlass".to_string())
                    .code("N".to_string())
                    .display("Notfall".to_string())
                    .build()?,
            )),
            _ => Ok(None),
        }
    }
}
fn map_mothers_encounter(
    cdm: &EncounterCdm,
    config: &Fhir,
) -> Result<Option<Reference>, BuilderError> {
    match &cdm.mothers_enc_number {
        Some(mothers_enc_number) => Ok(Some(resource_ref(
            &ResourceType::Encounter,
            mothers_enc_number,
            config.fall.einrichtungskontakt.system.as_str(),
        )?)),
        None => Ok(None),
    }
}

fn map_conditions(
    cdm: &EncounterCdm,
    config: &Fhir,
) -> Result<Vec<Option<EncounterDiagnosis>>, FhirMappingError> {
    let mut res = vec![];
    if let Some(conditions) = &cdm.diagnosis
        && !conditions.is_empty()
    {
        for condition in conditions {
            let priority_u32 = condition
                .priority
                .parse::<f32>()
                .map_err(|err| ContentError::ParsingError(err.into()))?
                .floor() as u32;

            let rank_nz = NonZeroU32::new(priority_u32)
                .ok_or_else(|| {
                    ParsingError::Other(anyhow!(format!(
                        "DG1 entry row '{}': priority could not be parsed. value was '{}'",
                        condition.ordinal_number, condition.priority,
                    )))
                })
                .map_err(|err| ContentError::ParsingError(err.into()))?;

            let condition_reference = resource_ref(
                &ResourceType::Condition,
                map_bar_identifier(condition.id.as_str(), condition.priority.as_str())?.as_str(),
                &config.condition.system,
            )?;

            let codings = map_diagnose_local_codes(priority_u32, &condition.condition_typ)?;

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
fn map_bar_identifier(condition_id: &str, priority: &str) -> anyhow::Result<String> {
    let split_by_point = priority.split(".").collect::<Vec<&str>>();

    match split_by_point.len() > 1 {
        false => Ok(format!("{}-{}.1", condition_id, priority)),
        true => {
            match (
                split_by_point[1].parse::<u32>(),
                split_by_point[0].parse::<u32>(),
            ) {
                (Ok(_), Ok(_)) => Ok(format!("{}-{}", condition_id, priority)),

                (Err(e), _) => Err(e.into()),
                (_, Err(e)) => Err(e.into()),
            }
        }
    }
}

fn map_diagnose_local_codes(
    priority: u32,
    condition_type_local: &str,
) -> anyhow::Result<Vec<Option<Coding>>> {
    let mut result = vec![];

    let is_main_condition = priority < 2;
    // not supported by 2026 profile
    if is_main_condition {
        result.push(diagnose_role_coding("CC"));
    } else {
        result.push(diagnose_role_coding("CM"));
    };

    match condition_type_local {
        // Aufnahmediagnose
        "AD" | "Aufn." => {
            result.push(diagnose_role_coding("AD"));
        }

        // Einweisungsdiagnose
        "ED" | "Einw." => {
            result.push(kontakt_diagnose_procedures("referral-diagnosis"));
        }

        // Behandlungsdiagnose
        "BD" | "Beh." => {
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
            match condition_type_local {
                "FA" | "FA Au" => {
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
                Err(UnsupportedValueError {
                    details: format!("local condition type value '{}'", condition_type_local),
                })?;
            }
            if condition_type_local.is_empty() {
                Err(UnsupportedValueError {
                    details: "local condition type is empty!".to_string(),
                })?;
            }
        }
    }

    Ok(result)
}

fn map_abteilungskontakt(
    cdm: &EncounterCdm,
    config: &Fhir,
    resources: &ResourceMap,
) -> Result<Option<Encounter>, FhirMappingError> {
    if let Some(service_type) = get_service_type(cdm, resources, config)? {
        // base encounter
        let mut enc = base_encounter(cdm, config, &Fachabteilungskontakt)?
            .part_of(resource_ref(
                &ResourceType::Encounter,
                cdm.visit_number.as_str(),
                &config.fall.einrichtungskontakt.system,
            )?)
            .build()?;

        enc.service_type = Some(service_type);
        if let Some(fab) = &cdm.department_name {
            enc.service_provider = Some(resource_ref(
                &ResourceType::Organization,
                fab.as_str(),
                config.organization.department.system.as_str(),
            )?);
        }

        Ok(Some(enc))
    } else {
        log!(
            Level::Debug,
            "Missing service type at msg-id '{}' - cannot build valid encounter at department level!",
            cdm.id()
        );
        Ok(None)
    }
}

fn get_service_type(
    cdm: &EncounterCdm,
    resources: &ResourceMap,
    config: &Fhir,
) -> anyhow::Result<Option<CodeableConcept>> {
    let system_fachabteilungs_schluessel: &str =
        "http://fhir.de/CodeSystem/dkgev/Fachabteilungsschluessel-erweitert";

    if let Some(fab) = &cdm.department_name {
        match map_fab_schluessel(fab.as_str(), cdm.id(), config, resources) {
            Ok(Some(fab_from_short_name)) => return Ok(Some(fab_from_short_name)),
            Ok(None) => {}
            Err(e) => {
                if let FhirMappingError::MissingResourceError { resource, value } = &e {
                    return Err(anyhow!(
                        "map servicetype failed due missing resource {} entry at {}",
                        resource,
                        value
                    ));
                } else {
                    return Err(anyhow!("map servicetype failed unexpected".to_string()));
                }
            }
        };
    }

    if let Some(fab_schluessel) = &cdm.clinical_department_code {
        Ok(Some(get_cc_with_one_code(
            fab_schluessel.to_string(),
            system_fachabteilungs_schluessel.to_string(),
        )?))
    } else {
        Ok(None)
    }
}

fn map_versorgungsstellenkontakt(
    cdm: &EncounterCdm,
    config: &Fhir,
) -> Result<Option<Encounter>, FhirMappingError> {
    if let Some(locations) = &cdm.locations {
        let encounter_locations = locations
            .iter()
            .map(|loc_cdm| {
                let location = location_cdm_to_fhir::map_to_fhir(loc_cdm, config)?;
                let encounter_location = to_encounter_location(location).map(Some)?;

                if let Some(mut el) = encounter_location {
                    if loc_cdm.is_movement_finished {
                        el.status = Some(EncounterLocationStatus::Completed)
                    } else {
                        el.status = Some(EncounterLocationStatus::Active)
                    }
                    Ok::<Option<EncounterLocation>, FhirMappingError>(Some(el))
                } else {
                    Ok(None)
                }
            })
            .collect::<Result<Vec<_>, _>>()?;
        if encounter_locations.is_empty() {
            return Ok(None);
        }

        let versorgungskontakt = base_encounter(cdm, config, &Versorgungsstellenkontakt)?
            .part_of(resource_ref(
                &ResourceType::Encounter,
                cdm.movement_id.as_str(),
                &config.fall.abteilungskontakt.system,
            )?)
            .location(encounter_locations)
            .status(map_encounter_status(&map_period(
                cdm,
                &Versorgungsstellenkontakt,
            )?));

        let mut kontakt = versorgungskontakt.build()?;

        if let Some(ward) = &cdm.ward_short_name {
            kontakt.service_provider = Some(resource_ref(
                &ResourceType::Organization,
                ward,
                config.organization.ward.system.as_str(),
            )?);
        }

        Ok(Some(kontakt))
    } else {
        Ok(None)
    }
}

pub fn to_encounter_location(location: Location) -> Result<EncounterLocation, ContentError> {
    let identifier = location
        .identifier
        .first()
        .and_then(|i| i.as_ref())
        .ok_or_else(|| ContentError::MissingValueError {
            property: "failed to access location.identifier".to_string(),
        })?;

    let identifier_value =
        identifier
            .value
            .as_deref()
            .ok_or_else(|| ContentError::MissingValueError {
                property: "failed to access location.identifier.value".to_string(),
            })?;

    let system = identifier
        .system
        .as_deref()
        .ok_or_else(|| ContentError::MissingValueError {
            property: "failed to access location.identifier.system".to_string(),
        })?;

    let phys_type =
        location
            .physical_type
            .clone()
            .ok_or_else(|| ContentError::MissingValueError {
                property: "failed to access location.pyhsical_type".to_string(),
            })?;

    Ok(EncounterLocation::builder()
        .physical_type(phys_type)
        .location(resource_ref(
            &ResourceType::Location,
            identifier_value,
            system,
        )?)
        .build()?)
}
