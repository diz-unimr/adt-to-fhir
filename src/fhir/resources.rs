use crate::error::MappingError;
use fhir_model::r4b::types::{CodeableConcept, Coding};
use serde::Deserialize;
use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;
use time::OffsetDateTime;

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
    #[serde(with = "time::serde::iso8601")]
    pub(crate) valid_from: OffsetDateTime,
    #[serde(with = "time::serde::iso8601::option", default)]
    pub(crate) valid_to: Option<OffsetDateTime>,
}

/// Mappings for Fachabteilung (encounter department and location)
pub(crate) struct ResourceMap {
    /// Map with key: Fachabteilungsschlüssel
    pub(crate) department_map: HashMap<String, Department>,
    /// Map with key: Stationskürzel
    pub(crate) ward_map: HashMap<String, Ward>,
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
    ) -> Result<Option<CodeableConcept>, MappingError> {
        let dep = self
            .department_map
            .get(code)
            .ok_or(MappingError::MissingResourceError {
                resource: "Fachabteilungsschlüssel".into(),
                value: code.into(),
            })?;
        if dep.fachabteilungs_schluessel.is_empty() {
            return Ok(None);
        }

        Ok(Some(
            CodeableConcept::builder()
                .coding(vec![Some(
                    Coding::builder()
                        .system(
                            "http://fhir.de/CodeSystem/dkgev/Fachabteilungsschluessel-erweitert"
                                .to_string(),
                        )
                        .code(dep.fachabteilungs_schluessel.to_string())
                        .display(dep.abteilungs_bezeichnung.to_string())
                        .build()?,
                )])
                .build()?,
        ))
    }
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fhir::resources::{Department, ResourceMap};
    use std::collections::HashMap;
    use time::format_description::well_known::Iso8601;

    #[test]
    fn test_map_fab_schluessel() {
        let resources = ResourceMap {
            department_map: HashMap::from([(
                "POL".to_string(),
                Department {
                    abteilungs_bezeichnung: "Pneumologie".to_string(),
                    fachabteilungs_schluessel: "0800".to_string(),
                },
            )]),
            ward_map: Default::default(),
        };

        let expected = Coding::builder()
            .system("http://fhir.de/CodeSystem/dkgev/Fachabteilungsschluessel-erweitert".into())
            .code("0800".into())
            .display("Pneumologie".into())
            .build()
            .unwrap();

        let actual = resources
            .map_fab_schluessel("POL")
            .unwrap()
            .unwrap()
            .coding
            .first()
            .unwrap()
            .clone()
            .unwrap();

        assert_eq!(actual, expected);
    }
    #[test]
    fn test_init_ward_map() {
        let r = init_ward_map();
        let description: &Iso8601 = &Iso8601;

        match r {
            Ok(m) => {
                assert!(!m.get("POLST22").unwrap().is_icu);
                assert!(!m.get("POLST12").unwrap().is_icu);
                assert!(m.get("POLST12").unwrap().valid_to.is_none());
                assert!(m.get("ANA").unwrap().is_icu);
                assert!(m.get("ANA2").unwrap().valid_to.is_some());

                assert_eq!(
                    m.get("ANA2").unwrap().valid_to,
                    Some(OffsetDateTime::parse("1984-02-01T00:00:00+01:00", description).unwrap())
                );
            }
            Err(e) => {
                panic!("could not initialize resource map - reason: {}", e);
            }
        }
    }

    #[test]
    fn test_init_department_map() {
        let r = init_department_map();
        match r {
            Ok(_) => {}
            Err(e) => {
                panic!("could not initialize resource map - reason: {}", e);
            }
        }
    }
}
