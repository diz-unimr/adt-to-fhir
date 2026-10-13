use adt_config::config::{CheckMode, Fhir};
use adt_config::resources::{Department, ResourceMap, ValidPeriod};
use chrono::NaiveDate;

use crate::fhir_error::FhirMappingError;
use crate::fhir_error::FhirMappingError::MissingResourceError;
use fhir_model::r4b::types::{CodeableConcept, Coding};
use log::{Level, log};
use std::collections::HashMap;

/// Maps a given Fachabteilungsschlüssel to a Department
/// by doing a lookup on the department data map.
///
/// If the lookup is successful a single [`Coding`] from
/// [FachabteilungsschluesselErweitert ValueSet](https://simplifier.net/resolve?scope=de.basisprofil.r4@1.5.4&canonical=http://fhir.de/ValueSet/dkgev/Fachabteilungsschluessel-erweitert)
/// is returned as part of the [`CodeableConcept`].
pub fn map_fab_schluessel(
    code: &str,
    msg_id: &str,
    config: &Fhir,
    resources: &ResourceMap,
) -> Result<Option<CodeableConcept>, FhirMappingError> {
    let key = find_key(&resources.department_map, code);

    if let Some(code) = key {
        let dep = match resources.department_map.get(code.as_str()) {
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
        let res = error_if_strict(config, code, msg_id)?;
        Ok(res)
    }
}

fn find_key(department_map: &HashMap<String, Department>, code: &str) -> Option<String> {
    let search_code: Option<String>;

    if department_map.contains_key(code) {
        search_code = Some(code.to_string());
    } else {
        if let Some(sub_3) = code.get(0..3)
            && department_map.contains_key(sub_3)
        {
            search_code = Some(sub_3.to_string())
        } else {
            if let Some(sub_4) = code.get(0..4)
                && department_map.contains_key(sub_4)
            {
                search_code = Some(sub_4.to_string())
            } else if let Some(sub_5) = code.get(0..5)
                && department_map.contains_key(sub_5)
            {
                search_code = Some(sub_5.to_string())
            } else {
                search_code = None
            }
        }
    }
    search_code
}

fn error_if_strict(
    config: &Fhir,
    code: &str,
    msg_id: &str,
) -> Result<Option<CodeableConcept>, FhirMappingError> {
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

pub fn is_valid_date(period: &ValidPeriod, date: &NaiveDate) -> bool {
    date.ge(&period.valid_from)
        && (period.valid_to.is_none() || date.le(&period.valid_to.unwrap_or(NaiveDate::MAX)))
}

#[cfg(test)]
mod tests {
    use super::*;

    use adt_config::test_utils::tests::{get_dummy_resources, get_test_config};
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

        let actual = map_fab_schluessel("POL", "1234", &config, &resources)
            .unwrap()
            .unwrap()
            .coding
            .first()
            .unwrap()
            .clone()
            .unwrap();

        assert_eq!(actual, expected);

        let actual = map_fab_schluessel("POLAMB", "1234", &config, &resources)
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
        let actual = map_fab_schluessel("MICROYXZ", "1234", &config, &resources)
            .unwrap()
            .unwrap()
            .coding
            .first()
            .unwrap()
            .clone()
            .unwrap();

        assert_eq!(actual, expected);

        match map_fab_schluessel("does not exist", "1234", &config, &resources) {
            Ok(result) => panic!(
                "check mode strict should produce an error! but got: {:?}",
                result
            ),
            Err(MissingResourceError {
                resource: _,
                value: v,
            }) => {
                assert_eq!(v, "does not exist", "Unexpected value");
            }

            Err(error) => panic!("did not expect this error {:?}", error),
        }

        config.check_mode = CheckMode::Lenient;
        match map_fab_schluessel("does not exist", "1234", &config, &resources) {
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
}
