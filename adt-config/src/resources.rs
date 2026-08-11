use anyhow::Context;
use chrono::NaiveDate;
use fhir_model::r4b::resources::CodeSystem;
use serde::Deserialize;
use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
#[derive(Clone)]
pub struct Location {
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
pub struct Department {
    /// Fachabteilungsschlüssel
    pub fachabteilungs_schluessel: String,
    /// Abteilungsbezeichnung
    pub abteilungs_bezeichnung: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
#[derive(Clone)]
pub struct Ward {
    pub display: String,
    #[serde(default)]
    pub is_icu: bool,
    pub valid_period: Vec<ValidPeriod>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
#[derive(Clone, Eq, Hash, PartialEq)]
pub struct ValidPeriod {
    pub valid_from: NaiveDate,
    pub valid_to: Option<NaiveDate>,
}

/// Mappings for Fachabteilung (encounter department and location)
pub struct ResourceMap {
    /// Map with key: Fachabteilungsschlüssel
    pub department_map: HashMap<String, Department>,
    /// Map with key: Stationskürzel
    pub ward_map: HashMap<String, Ward>,
    /// Map medical department id (Fachabteilungschluessel) as key to its official name
    pub department_id_map: HashMap<String, String>,
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
    pub fn new() -> Result<Self, anyhow::Error> {
        Ok(ResourceMap {
            department_map: init_department_map()?,
            ward_map: init_ward_map()?,
            department_id_map: init_departments_id_map()?,
        })
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
