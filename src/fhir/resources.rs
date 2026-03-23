use crate::error::MappingError;
use anyhow::anyhow;
use fhir_model::r4b::types::{CodeableConcept, Coding};
use serde::de;
use serde_derive::Deserialize;
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
    // Kennzeichen Intensivstation (ICU)
    #[serde(deserialize_with = "deserialize_bool")]
    ist_intensiv_station: bool,
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

/// Mappings for Fachabteilung (encounter department and location)
#[derive(Clone)]
pub(crate) struct ResourceMap {
    /// Map with key: Fachabteilungsschlüssel
    pub(crate) department_map: HashMap<String, Department>,
    /// Map with key: Kostenstelle
    pub(crate) location_map: HashMap<String, Location>,
}

impl ResourceMap {
    /// Creates a new [`ResourceMap`] instance.
    ///
    /// The instance is initialized with data from external json files from
    /// `resources/mapping`:
    ///
    /// [department_map](ResourceMap::department_map): `InfoByAbteilungskuerzel.json`
    ///
    /// [location_map](ResourceMap::location_map): `InfoByKostenstelle.json`
    pub(crate) fn new() -> Result<Self, anyhow::Error> {
        Ok(ResourceMap {
            department_map: init_department_map()?,
            location_map: init_location_map()?,
        })
    }

    /// Maps a given Fachabteilungsschlüssel to a Department
    /// by doing a lookup on the department data map.
    ///
    /// If the lookup is successfull a single [`Coding`] from
    /// [FachabteilungsschluesselErweitert ValueSet](https://simplifier.net/resolve?scope=de.basisprofil.r4@1.5.4&canonical=http://fhir.de/ValueSet/dkgev/Fachabteilungsschluessel-erweitert)
    /// is returned as part of the [`CodeableConcept`].
    pub(crate) fn map_fab_schluessel(
        &self,
        code: &str,
    ) -> Result<Option<CodeableConcept>, MappingError> {
        let dep = self
            .department_map
            .get(code)
            .ok_or(MappingError::Other(anyhow!(
                "Fachabteilungsschlüssel {} not found",
                code
            )))?;

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

fn init_location_map() -> Result<HashMap<String, Location>, anyhow::Error> {
    let resource_data = read_mapping_resource("InfoByKostenstelle.json")?;

    Ok(serde_json::from_str(&resource_data)?)
}

fn init_department_map() -> Result<HashMap<String, Department>, anyhow::Error> {
    let resource_data = read_mapping_resource("InfoByAbteilungskuerzel.json")?;

    Ok(serde_json::from_str(&resource_data)?)
}

fn read_mapping_resource(file_name: &str) -> Result<String, anyhow::Error> {
    let mut file_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    file_path.push("resources/mapping");
    file_path.push(file_name);

    Ok(fs::read_to_string(file_path.display().to_string())?)
}

fn deserialize_bool<'de, D>(deserializer: D) -> Result<bool, D::Error>
where
    D: de::Deserializer<'de>,
{
    let s: &str = de::Deserialize::deserialize(deserializer)?;
    match s {
        "1" => Ok(true),
        "" | "0" => Ok(false),
        _ => Err(de::Error::unknown_variant(s, &["", "0", "1"])),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fhir::resources::{Department, ResourceMap};
    use std::collections::HashMap;

    #[test]
    fn test_deserialize_bool() {
        let resources = ResourceMap {
            department_map: HashMap::from([(
                "POL".to_string(),
                Department {
                    abteilungs_bezeichnung: "Pneumologie".to_string(),
                    fachabteilungs_schluessel: "0800".to_string(),
                },
            )]),
            location_map: Default::default(),
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
}
