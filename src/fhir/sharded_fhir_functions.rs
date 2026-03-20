use crate::config::{FallConfig, Fhir, LocationConfig, PatientConfig};
use crate::error::{MappingError, MessageAccessError};
use crate::fhir::resources::{Department, ResourceMap};
use crate::hl7::parser::{parse_component, parse_field, parse_repeating_field_component_value};
use fhir_model::BuilderError;
use fhir_model::r4b::types::{CodeableConcept, Coding};
use hl7_parser::Message;
use std::collections::HashMap;

pub fn is_inpatient_location(msg: &Message) -> Result<bool, MappingError> {
    Ok(parse_field(msg, "PV1", 2)?
        .map(|s| s.raw_value() == "I")
        .is_some()
        && parse_repeating_field_component_value(msg, "PV1", 3, 5)?
            .map(|v| v == "KLINIKUM")
            .is_some())
}

pub fn get_cc_with_one_code(code: String, system: String) -> Result<CodeableConcept, BuilderError> {
    CodeableConcept::builder()
        .coding(vec![Some(
            Coding::builder()
                .code(code.to_string())
                .system(system.to_string())
                .build()?,
        )])
        .build()
}

pub fn parse_fab(msg: &Message) -> Result<Option<String>, MessageAccessError> {
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

pub fn get_test_config() -> Fhir {
    Fhir {
        facility_id: "260620431".to_string(),
        meta_source: "test".to_string(),
        person: PatientConfig {
            profile: "https://www.medizininformatik-initiative.de/fhir/core/modul-person/StructureDefinition/Patient|2025.0.0".to_string(),
            system: "https://fhir.diz.uni-marburg.de/sid/patient-id".to_string(),
            other_insurance_system: "https://fhir.diz.uni-marburg.de/sid/patient-other-insurance-id".to_string()
        },
        fall: FallConfig {
            profile: "https://www.medizininformatik-initiative.de/fhir/core/modul-fall/StructureDefinition/KontaktGesundheitseinrichtung|2025.0.0".to_string(),
            system: "https://fhir.diz.uni-marburg.de/sid/encounter-id".to_string(),
            einrichtungskontakt: Default::default(),
            abteilungskontakt: Default::default(),
            versorgungsstellenkontakt: Default::default(),
            institut_kennzeichen: "123456789".to_string(),
            institut_kennzeichen_system: "http://fhir.de/sid/arge-ik/iknr".to_string()
        },
        location: LocationConfig {
            system_ward: "https://fhir.diz.uni-marburg.de/sid/location-caresite-id".to_string(),
            system_room: "https://fhir.diz.uni-marburg.de/sid/location-room-id".to_string(),
            system_bed: "https://fhir.diz.uni-marburg.de/sid/location-bed-id".to_string(),
        },
    }
}
pub fn get_dummy_resources() -> ResourceMap {
    ResourceMap {
        department_map: HashMap::from([
            (
                "POL".to_string(),
                Department {
                    abteilungs_bezeichnung: "Pneumologie".to_string(),
                    fachabteilungs_schluessel: "0800".to_string(),
                },
            ),
            (
                "KJM".to_string(),
                Department {
                    abteilungs_bezeichnung: "Kinder- und Jugendmedizin".to_string(),
                    fachabteilungs_schluessel: "1000".to_string(),
                },
            ),
        ]),
        location_map: Default::default(),
    }
}
