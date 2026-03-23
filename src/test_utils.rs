#[cfg(test)]
pub(crate) mod tests {
    use crate::config::{FallConfig, Fhir, LocationConfig, PatientConfig};
    use crate::fhir::resources::{Department, ResourceMap};
    use std::collections::HashMap;

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
}
