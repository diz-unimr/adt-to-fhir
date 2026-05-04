#[cfg(test)]
pub(crate) mod tests {
    use crate::config::{
        FallConfig, Fhir, LocationConfig, ObservationConfig, PatientConfig, SystemConfig,
    };
    use crate::fhir::resources::{Department, ResourceMap, Ward};
    use fhir_model::WrongResourceType;
    use fhir_model::r4b::resources::{Bundle, BundleEntry, Resource};
    use fhir_model::r4b::types::Meta;
    use std::collections::HashMap;
    use std::fs;
    use std::path::PathBuf;

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
                einrichtungskontakt: SystemConfig {system: "einrichtungskontakt-id".to_string()},
                abteilungskontakt: SystemConfig {system: "abteilungskontakt-id".to_string()},
                versorgungsstellenkontakt: SystemConfig {system: "versorgungsstellenkontakt-id".to_string()},
            },
            location: LocationConfig {
                system_ward: "https://fhir.diz.uni-marburg.de/sid/location-caresite-id".to_string(),
                system_room: "https://fhir.diz.uni-marburg.de/sid/location-room-id".to_string(),
                system_bed: "https://fhir.diz.uni-marburg.de/sid/location-bed-id".to_string(),
            },
            condition: SystemConfig {system: "https://fhir.diz.uni-marburg.de/sid/condition-id".to_string()},
            observation: ObservationConfig {
                system: "https://fhir.diz.uni-marburg.de/sid/observation-id".to_string(),
                profile_weight:"https://www.medizininformatik-initiative.de/fhir/ext/modul-icu/StructureDefinition/mii-pr-icu-muv-koerpergewicht|2026.0.1".to_string(),
                profile_head_circumference: "https://www.medizininformatik-initiative.de/fhir/ext/modul-icu/StructureDefinition/mii-pr-icu-kopfumfang|2026.0.1".to_string(),
                profile_vital_status: "https://www.medizininformatik-initiative.de/fhir/core/modul-person/StructureDefinition/Vitalstatus|2026.0.0".to_string(),
                profile_height: "https://www.medizininformatik-initiative.de/fhir/ext/modul-icu/StructureDefinition/mii-pr-icu-muv-koerpergroesse|2026.0.0".to_string()

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
                (
                    "URO".to_string(),
                    Department {
                        abteilungs_bezeichnung: "Urologie und Kinderurologie".to_string(),
                        fachabteilungs_schluessel: "2200".to_string(),
                    },
                ),
            ]),
            location_map: Default::default(),
            ward_map: HashMap::from([
                (
                    "ANA".to_string(),
                    Ward {
                        display: "Aneasthesie u. Intensivtherapie".to_string(),
                        is_icu: true,
                    },
                ),
                (
                    "IDIST1I".to_string(),
                    Ward {
                        display: "IDIST1I".to_string(),
                        is_icu: true,
                    },
                ),
                (
                    "IDIST121".to_string(),
                    Ward {
                        display: "Iterdisziplinaere Station 121".to_string(),
                        is_icu: false,
                    },
                ),
            ]),
        }
    }

    pub(crate) fn resource_from<T: TryFrom<Resource, Error = WrongResourceType>>(
        e: &BundleEntry,
    ) -> Result<T, WrongResourceType> {
        let r = e.resource.clone().unwrap();
        T::try_from(r)
    }

    pub(crate) fn filter_resources<T: TryFrom<Resource, Error = WrongResourceType>>(
        bundle: &Bundle,
    ) -> Vec<T> {
        bundle
            .entry
            .iter()
            .flatten()
            .filter_map(|e| resource_from::<T>(e).ok())
            .collect()
    }

    pub(crate) fn has_profile(meta: &Meta, profile: &str) -> bool {
        meta.profile.iter().flatten().any(|m| m == profile)
    }

    pub fn read_test_resource(file_name: &str) -> String {
        let mut file_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        file_path.push("resources/test");
        file_path.push(file_name);

        fs::read_to_string(file_path.display().to_string())
            .unwrap_or_else(|_| panic!("Test resource not found: {}", file_path.display()))
    }
}
