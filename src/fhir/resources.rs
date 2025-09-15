use crate::fhir::mapper::MappingError;
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
struct Location {
    desc: String,
    fachabteilungs_kuerzel: String,
    abteilungs_bezeichnung: String,
    fachabteilungs_schluessel: String,
    #[serde(deserialize_with = "deserialize_bool")]
    ist_intensiv_station: bool,
}

// todo: might be derived from Location
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
#[derive(Clone)]
struct Department {
    fachabteilungs_schluessel: String,
    abteilungs_bezeichnung: String,
}

#[derive(Clone)]
pub(crate) struct ResourceMap {
    department_map: HashMap<String, Department>,
    location_map: HashMap<String, Location>,
}

impl ResourceMap {
    pub(crate) fn new() -> Result<Self, anyhow::Error> {
        Ok(ResourceMap {
            department_map: init_department_map()?,
            location_map: init_location_map()?,
        })
    }

    pub(crate) fn map_fab_schluessel(&self, code: &str) -> Result<CodeableConcept, MappingError> {
        let dep = self
            .department_map
            .get(code)
            .ok_or(MappingError::Other(anyhow!(
                "Fachabteilungsschlüssel {} not found",
                code
            )))?;

        Ok(CodeableConcept::builder()
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
            .build()?)
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
        _ => Err(de::Error::unknown_variant(s, &["", "1"])),
    }
}
