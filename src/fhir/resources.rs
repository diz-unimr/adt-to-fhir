use crate::config::{CheckMode, Fhir};
use crate::error::MappingError;
use crate::error::MappingError::MissingResourceError;
use anyhow::Context;
use chrono::NaiveDate;
use fhir_model::r4b::resources::CodeSystem;
use fhir_model::r4b::types::{CodeableConcept, Coding};
use log::{Level, log};
use serde::Deserialize;
use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
#[derive(Clone)]
pub(crate) struct Location {
    desc: String,
    /// Fachabteilungskürzel
    fachabteilungs_kuerzel: String,
    /// Abteilungsbezeichnung
    abteilungs_bezeichnung: String,
    /// Fachabteilungsschlüssel
    fachabteilungs_schluessel: String,
}

/// Fachabteilung
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
#[derive(Clone)]
pub(crate) struct Department {
    /// Fachabteilungsschlüssel
    pub(crate) fachabteilungs_schluessel: String,
    /// Abteilungsbezeichnung
    pub(crate) abteilungs_bezeichnung: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
#[derive(Clone)]
pub(crate) struct Ward {
    pub(crate) display: String,
    #[serde(default)]
    pub(crate) is_icu: bool,
    pub(crate) valid_period: Vec<ValidPeriod>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
#[derive(Clone, Eq, Hash, PartialEq)]
pub(crate) struct ValidPeriod {
    pub(crate) valid_from: NaiveDate,

    pub(crate) valid_to: Option<NaiveDate>,
}

/// Mappings for Fachabteilung (encounter department and location)
pub(crate) struct ResourceMap {
    /// Map with key: Fachabteilungsschlüssel
    pub(crate) department_map: HashMap<String, Department>,
    /// Map with key: Stationskürzel
    pub(crate) ward_map: HashMap<String, Ward>,
    /// Map medical department id (Fachabteilungschluessel) as key to its official name
    pub(crate) department_id_map: HashMap<String, String>,
}

impl ResourceMap {
    /// Creates a new [`ResourceMap`] instance.
    ///
    /// The instance is initialized with data from external json files from
    /// `resources/mapping`:
    ///
    /// [department_map](ResourceMap::department_map): `InfoByAbteilungskuerzel.json`
    ///
    /// [ward_map](ResourceMap::ward_map): `InfoStation.json`
    pub(crate) fn new() -> Result<Self, anyhow::Error> {
        Ok(ResourceMap {
            department_map: init_department_map()?,
            ward_map: init_ward_map()?,
            department_id_map: init_departments_id_map()?,
        })
    }

    /// Maps a given Fachabteilungsschlüssel to a Department
    /// by doing a lookup on the department data map.
    ///
    /// If the lookup is successful a single [`Coding`] from
    /// [FachabteilungsschluesselErweitert ValueSet](https://simplifier.net/resolve?scope=de.basisprofil.r4@1.5.4&canonical=http://fhir.de/ValueSet/dkgev/Fachabteilungsschluessel-erweitert)
    /// is returned as part of the [`CodeableConcept`].
    pub(crate) fn map_fab_schluessel(
        &self,
        code: &str,
        msg_id: &str,
        config: &Fhir,
        resources: &ResourceMap,
    ) -> Result<Option<CodeableConcept>, MappingError> {
        let key = self.find_key(code);

        if let Some(code) = key {
            let dep = match self.department_map.get(code.as_str()) {
                Some(dep) => dep,
                None => {
                    error_if_strict(config, &code, msg_id)?; // gibt Err zurück (Strict) oder Ok(()) nach Logging (Lenient)
                    return Ok(None);
                }
            };

            if dep.fachabteilungs_schluessel.is_empty() {
                log!(
                    Level::Error,
                    "Fachabteilungsschlüssel für '{}' ist leer in Mapping Datei bitte nachtragen",
                    code
                );
                return Ok(None);
            }

            let department_id_display = match resources
                .department_id_map
                .get(&dep.fachabteilungs_schluessel)
            {
                None => {
                    return Err(MissingResourceError {
                        resource: "Fachabteilungsschluessel-erweitert.json".to_string(),
                        value: format!(
                            "department {} -> key {}",
                            code, &dep.fachabteilungs_schluessel
                        ),
                    });
                }
                Some(d) => d,
            };

            Ok(Some(
                CodeableConcept::builder()
                    .coding(vec![Some(
                    Coding::builder()
                        .system(
                            "http://fhir.de/CodeSystem/dkgev/Fachabteilungsschluessel-erweitert"
                                .to_string(),
                        )
                        .code(dep.fachabteilungs_schluessel.to_string())
                        .display(department_id_display.to_string())
                        .build()?,
                )])
                    .build()?,
            ))
        } else {
            match error_if_strict(config, code, msg_id) {
                Ok(c) => Ok(c),
                Err(e) => Err(e),
            }
        }
    }

    fn find_key(&self, code: &str) -> Option<String> {
        let search_code: Option<String>;

        if self.department_map.contains_key(code) {
            search_code = Some(code.to_string());
        } else {
            if let Some(sub_3) = code.get(0..3)
                && self.department_map.contains_key(sub_3)
            {
                search_code = Some(sub_3.to_string())
            } else {
                if let Some(sub_4) = code.get(0..4)
                    && self.department_map.contains_key(sub_4)
                {
                    search_code = Some(sub_4.to_string())
                } else if let Some(sub_5) = code.get(0..5)
                    && self.department_map.contains_key(sub_5)
                {
                    search_code = Some(sub_5.to_string())
                } else {
                    search_code = None
                }
            }
        }
        search_code
    }
}

fn error_if_strict(
    config: &Fhir,
    code: &str,
    msg_id: &str,
) -> Result<Option<CodeableConcept>, MappingError> {
    match config.check_mode {
        CheckMode::Strict => Err(MissingResourceError {
            resource: "Fachabteilungsschlüssel".to_string(),
            value: code.to_string(),
        }),
        CheckMode::Lenient => {
            log!(
                Level::Error,
                "Fachabteilungsschlüssel der Nachricht {} fehlt für Code '{}' setze '3700 Sonstige Fachabteilung 3700'",
                msg_id,
                code
            );
            Ok(Some(
                CodeableConcept::builder()
                    .coding(vec![Some(
                        Coding::builder()
                            .system(
                                "http://fhir.de/CodeSystem/dkgev/Fachabteilungsschluessel-erweitert"
                                    .to_string(),
                            )
                            .code("3700".to_string())
                            .display("Sonstige Fachabteilung".to_string())
                            .build()?,
                    )])
                    .build()?,
            ))
        }
    }
}

pub(crate) fn is_valid_date(period: &ValidPeriod, date: &NaiveDate) -> bool {
    date.ge(&period.valid_from)
        && (period.valid_to.is_none() || date.le(&period.valid_to.unwrap_or(NaiveDate::MAX)))
}

fn init_department_map() -> Result<HashMap<String, Department>, anyhow::Error> {
    let resource_data = read_mapping_resource("InfoByAbteilungskuerzel.json")?;

    Ok(serde_json::from_str(&resource_data)?)
}

fn init_ward_map() -> Result<HashMap<String, Ward>, anyhow::Error> {
    let resource_data = read_mapping_resource("InfoStation.json")?;

    Ok(serde_json::from_str(&resource_data)?)
}

fn read_mapping_resource(file_name: &str) -> Result<String, anyhow::Error> {
    let mut file_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    file_path.push("resources/mapping");
    file_path.push(file_name);

    Ok(fs::read_to_string(file_path.display().to_string())?)
}

fn init_departments_id_map() -> Result<HashMap<String, String>, anyhow::Error> {
    let resource_data = read_mapping_resource("Fachabteilungsschluessel-erweitert.json")
        .context("Konnte Fachabteilungsschluessel-erweitert.json nicht lesen")?;

    let code_system: CodeSystem = serde_json::from_str(&resource_data)
        .context("Fachabteilungsschluessel-erweitert.json ist kein valides CodeSystem")?;

    code_system
        .concept
        .iter()
        .flatten() // Option<T> in der Liste überspringen statt unwrap()
        .map(|concept| {
            let code = concept.code.clone();
            let display = concept
                .display
                .clone()
                .with_context(|| format!("Kein 'display' für Code '{}'", code))?;
            Ok((code, display))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fhir::resources::{Department, ResourceMap};
    use crate::test_utils::tests::{get_dummy_resources, get_test_config};
    use std::collections::HashMap;

    #[test]
    fn test_map_fab_schluessel() {
        let mut config = get_test_config();
        let resources = ResourceMap {
            department_map: HashMap::from([
                (
                    "POL".to_string(),
                    Department {
                        abteilungs_bezeichnung: "Pneumologie".to_string(),
                        fachabteilungs_schluessel: "0800".to_string(),
                    },
                ),
                (
                    "MICRO".to_string(),
                    Department {
                        abteilungs_bezeichnung: "Microbiologie".to_string(),
                        fachabteilungs_schluessel: "3700".to_string(),
                    },
                ),
            ]),
            ward_map: Default::default(),
            department_id_map: get_dummy_resources().department_id_map.clone(),
        };

        let expected = Coding::builder()
            .system("http://fhir.de/CodeSystem/dkgev/Fachabteilungsschluessel-erweitert".into())
            .code("0800".into())
            .display("Pneumologie".into())
            .build()
            .unwrap();

        let actual = resources
            .map_fab_schluessel("POL", "1234", &config, &resources)
            .unwrap()
            .unwrap()
            .coding
            .first()
            .unwrap()
            .clone()
            .unwrap();

        assert_eq!(actual, expected);

        let actual = resources
            .map_fab_schluessel("POLAMB", "1234", &config, &resources)
            .unwrap()
            .unwrap()
            .coding
            .first()
            .unwrap()
            .clone()
            .unwrap();

        assert_eq!(actual, expected);

        let expected = Coding::builder()
            .system("http://fhir.de/CodeSystem/dkgev/Fachabteilungsschluessel-erweitert".into())
            .code("3700".into())
            .display("Sonstige Fachabteilung".into())
            .build()
            .unwrap();
        let actual = resources
            .map_fab_schluessel("MICROYXZ", "1234", &config, &resources)
            .unwrap()
            .unwrap()
            .coding
            .first()
            .unwrap()
            .clone()
            .unwrap();

        assert_eq!(actual, expected);

        match resources.map_fab_schluessel("does not exist", "1234", &config, &resources) {
            Ok(result) => panic!(
                "check mode strict should produce an error! but got: {:?}",
                result
            ),
            Err(MappingError::MissingResourceError {
                resource: _,
                value: v,
            }) => {
                assert_eq!(v, "does not exist", "Unexpected value");
            }

            Err(error) => panic!("did not expect this error {:?}", error),
        }

        config.check_mode = CheckMode::Lenient;
        match resources.map_fab_schluessel("does not exist", "1234", &config, &resources) {
            Ok(result) => {
                let actual = result.unwrap().coding.first().unwrap().clone().unwrap();
                let expected = Coding::builder()
                    .system(
                        "http://fhir.de/CodeSystem/dkgev/Fachabteilungsschluessel-erweitert".into(),
                    )
                    .code("3700".into())
                    .display("Sonstige Fachabteilung".into())
                    .build()
                    .unwrap();
                assert_eq!(actual, expected);
            }

            Err(error) => panic!(
                "CheckMode lenient should not produce an error but got: {:?}",
                error
            ),
        }
    }
    #[test]
    fn test_init_ward_map() {
        let m = init_ward_map().unwrap();

        assert!(!m.get("POLST22").unwrap().is_icu);
        assert!(!m.get("POLST12").unwrap().is_icu);
        assert!(
            m.get("POLST12")
                .unwrap()
                .valid_period
                .iter()
                .all(|a| a.valid_to.is_none())
        );
        assert!(m.get("ANA").unwrap().is_icu);
        assert!(
            m.get("ANA2")
                .unwrap()
                .valid_period
                .iter()
                .all(|a| a.valid_to.is_some())
        );

        assert_eq!(
            m.get("ANA2")
                .unwrap()
                .valid_period
                .iter()
                .find(|v| v.valid_to.is_some())
                .unwrap()
                .valid_to,
            Some(NaiveDate::from_ymd_opt(1984, 2, 1).unwrap())
        );
    }

    #[test]
    fn test_init_department_map() {
        let r = ResourceMap::new().unwrap();
        assert!(!r.department_map.is_empty());
        assert!(!r.ward_map.is_empty());
    }
}
