use crate::config::Fhir;
use crate::error::{FormattingError, MappingError, MessageAccessError};
use crate::fhir::mapper::{
    EntryRequestType, build_usual_identifier, bundle_entry, get_cc_with_one_code, parse_datetime,
};
use crate::fhir::patient::map_deceased;
use crate::hl7::parser::{
    MessageType, PID_PID, PV1_VISIT_ID, ZBE_BEGINN_OF_MOVEMENT, ZNG_BODY_HEIGHT,
    ZNG_HEAD_CIRCUMFERENCE, ZNG_WEIGHT, message_type, query,
};
use anyhow::anyhow;
use fhir_model::r4b::codes::ObservationStatus;
use fhir_model::r4b::resources::{
    BundleEntry, Observation, ObservationBuilder, ObservationEffective, ObservationValue,
};
use fhir_model::r4b::types::{CodeableConcept, Coding, Identifier, Meta, Quantity};
use hl7_parser::Message;
use std::sync::LazyLock;

const LOINC_PATIENT_DISPOSITION: &str = "67162-8";
const LOINC_BODY_WEIGHT: &str = "29463-7";
const LOINC_BODY_HEIGHT: &str = "8302-2";
const LOINC_HEAD_CIRCUMFERENCE: &str = "9843-4";
const SNOMED_BODYSITE_HEAD: &str = "69536005";
const SNOMED_VERSION: &str = "http://snomed.info/sct/900000000000207008/version/20241101";
const SNOMED_SYSTEM: &str = "http://snomed.info/sct";
const LOINC_SYSTEM: &str = "http://loinc.org";
const VITAL_SIGNS_CATEGORY_SYSTEM: &str =
    "http://terminology.hl7.org/CodeSystem/observation-category";
const VITAL_SIGNS_CATEGORY_CODE: &str = "vital-signs";
const SURVEY_CATEGORY_CODE: &str = "survey";
const UCUM_SYSTEM: &str = "http://unitsofmeasure.org";

const CODING_PATIENT_DISPOSITION: LazyLock<Vec<Option<Coding>>> = LazyLock::new(|| {
    vec![
        Coding::builder()
            .code(LOINC_PATIENT_DISPOSITION.to_string())
            .display("Patient Disposition".to_string())
            .system(LOINC_SYSTEM.to_string())
            .build()
            .ok(),
    ]
});

const CODING_HEAD_CIRCUMFERENCE: LazyLock<Vec<Option<Coding>>> = LazyLock::new(|| {
    vec![
        Coding::builder()
            .code(LOINC_HEAD_CIRCUMFERENCE.to_string())
            .system(LOINC_SYSTEM.to_string())
            .display("Head Occipital-frontal circumference".to_string())
            .build()
            .ok(),
        Coding::builder()
            .code("363811000".to_string())
            .system(SNOMED_SYSTEM.to_string())
            .display("Head circumference measure (observable entity)".to_string())
            .version(SNOMED_VERSION.to_string())
            .build()
            .ok(),
    ]
});

const CODING_BODY_WEIGHT: LazyLock<Vec<Option<Coding>>> = LazyLock::new(|| {
    vec![
        Coding::builder()
            .code(LOINC_BODY_WEIGHT.to_string())
            .system(LOINC_SYSTEM.to_string())
            .display("Body weight".to_string())
            .build()
            .ok(),
        Coding::builder()
            .code("8339-4".to_string())
            .system(LOINC_SYSTEM.to_string())
            .display("Birth weight Measured".to_string())
            .build()
            .ok(),
        Coding::builder()
            .code("27113001".to_string())
            .system(SNOMED_SYSTEM.to_string())
            .display("Body weight (observable entity)".to_string())
            .version(SNOMED_VERSION.to_string())
            .build()
            .ok(),
    ]
});

const CODING_BODY_HEIGHT: LazyLock<Vec<Option<Coding>>> = LazyLock::new(|| {
    vec![
        Coding::builder()
            .code(LOINC_BODY_HEIGHT.to_string())
            .system(LOINC_SYSTEM.to_string())
            .display("Body height".to_string())
            .build()
            .ok(),
        Coding::builder()
            .code("89269-5".to_string())
            .system(LOINC_SYSTEM.to_string())
            .display("Body height Measured --at birth".to_string())
            .build()
            .ok(),
        Coding::builder()
            .code("1153637007".to_string())
            .system(SNOMED_SYSTEM.to_string())
            .display("Body height (observable entity)".to_string())
            .version(SNOMED_VERSION.to_string())
            .build()
            .ok(),
    ]
});
const IS_ALIVE_CODING: LazyLock<Vec<Option<Coding>>> = LazyLock::new(|| {
    vec![Coding::builder().code("L".to_string()).system("https://www.medizininformatik-initiative.de/fhir/core/modul-person/CodeSystem/Vitalstatus".to_string()).build().ok()]
});

fn get_basic_observation_builder(msg: &Message) -> Result<ObservationBuilder, MappingError> {
    Ok(Observation::builder()
        .status(ObservationStatus::Final)
        .effective(ObservationEffective::DateTime(parse_datetime(
            query(msg, ZBE_BEGINN_OF_MOVEMENT).ok_or(MessageAccessError::Other(anyhow!(
                "ZBE.2 dateTime value missing!"
            )))?,
        )?)))
}

pub(crate) fn map(msg: &Message, config: &Fhir) -> Result<Vec<BundleEntry>, MappingError> {
    let mut result: Vec<BundleEntry> = vec![];
    let pid = query(msg, PID_PID);
    let visit = query(msg, PV1_VISIT_ID);

    if let (Some(pid), Some(visit)) = (pid, visit) {
        if let Some(is_alive) = map_vital_status(msg, config, pid, visit)? {
            result.push(bundle_entry(is_alive, EntryRequestType::UpdateAsCreate)?);
        }

        if let Some(head) = map_head_circumference(msg, config, pid, visit)? {
            result.push(bundle_entry(head, EntryRequestType::UpdateAsCreate)?);
        }

        if let Some(weight) = map_body_weight(msg, config, pid, visit)? {
            result.push(bundle_entry(weight, EntryRequestType::UpdateAsCreate)?);
        }

        if let Some(length) = map_body_length(msg, config, pid, visit)? {
            result.push(bundle_entry(length, EntryRequestType::UpdateAsCreate)?);
        }
    }
    Ok(result)
}

fn map_vital_status(
    msg: &Message,
    config: &Fhir,
    pid: &str,
    visit: &str,
) -> Result<Option<Observation>, MappingError> {
    if map_deceased(msg)?.is_none() {
        return match message_type(msg).ok() {
            // is alive observation will be created at patient admission,
            // discharge, movement, registration
            Some(MessageType::A01)
            | Some(MessageType::A02)
            | Some(MessageType::A03)
            | Some(MessageType::A04) => Ok(Some(
                get_basic_observation_builder(msg)?
                    .category(vec![Some(get_cc_with_one_code(
                        SURVEY_CATEGORY_CODE.into(),
                        VITAL_SIGNS_CATEGORY_SYSTEM.into(),
                    )?)])
                    .identifier(vec![Some(build_usual_identifier(
                        vec![LOINC_PATIENT_DISPOSITION, pid, visit],
                        config.observation.system.clone(),
                    )?)])
                    .meta(
                        Meta::builder()
                            .profile(vec![Some(config.observation.profile_vital_status.clone())])
                            .build()?,
                    )
                    .code(
                        CodeableConcept::builder()
                            .coding(CODING_PATIENT_DISPOSITION.clone())
                            .build()?,
                    )
                    .value(ObservationValue::CodeableConcept(
                        CodeableConcept::builder()
                            .coding(IS_ALIVE_CODING.clone())
                            .build()?,
                    ))
                    .build()?,
            )),
            // current message type should not create a vital status observation
            _ => Ok(None),
        };
    }
    // patient is deceased therefore no vital status observation
    Ok(None)
}

fn map_body_length(
    msg: &Message,
    config: &Fhir,
    pid: &str,
    visit: &str,
) -> Result<Option<Observation>, MappingError> {
    if let Some(quantity_value) = query(msg, ZNG_BODY_HEIGHT)
        .map(|val| val.parse::<f64>().map_err(FormattingError::ParseFloatError))
        .transpose()?
    {
        return Ok(Some(
            get_birth_obs_builder(
                msg,
                build_usual_identifier(
                    vec![LOINC_BODY_HEIGHT, pid, visit],
                    config.observation.system.clone(),
                )?,
                quantity_value,
                "cm".to_string(),
                config.observation.profile_height.to_string(),
            )?
            .code(
                CodeableConcept::builder()
                    .coding(CODING_BODY_HEIGHT.clone())
                    .build()?,
            )
            .build()?,
        ));
    }
    Ok(None)
}

fn map_body_weight(
    msg: &Message,
    config: &Fhir,
    pid: &str,
    visit: &str,
) -> Result<Option<Observation>, MappingError> {
    if let Some(quantity_value) = query(msg, ZNG_WEIGHT)
        .map(|val| val.parse::<f64>().map_err(FormattingError::ParseFloatError))
        .transpose()?
    {
        let identifier = build_usual_identifier(
            vec![LOINC_BODY_WEIGHT, pid, visit],
            config.observation.system.clone(),
        )?;

        return Ok(Some(
            get_birth_obs_builder(
                msg,
                identifier,
                quantity_value,
                "g".to_string(),
                config.observation.profile_weight.to_string(),
            )?
            .code(
                CodeableConcept::builder()
                    .coding(CODING_BODY_WEIGHT.clone())
                    .build()?,
            )
            .build()?,
        ));
    }
    Ok(None)
}

fn map_head_circumference(
    msg: &Message,
    config: &Fhir,
    pid: &str,
    visit: &str,
) -> Result<Option<Observation>, MappingError> {
    if let Some(quantity_value) = query(msg, ZNG_HEAD_CIRCUMFERENCE)
        .map(|val| val.parse::<f64>().map_err(FormattingError::ParseFloatError))
        .transpose()?
    {
        let identifier = build_usual_identifier(
            vec![LOINC_HEAD_CIRCUMFERENCE, pid, visit],
            config.observation.system.clone(),
        )?;
        return Ok(Some(
            get_birth_obs_builder(
                msg,
                identifier,
                quantity_value,
                "cm".to_string(),
                config.observation.profile_head_circumference.to_string(),
            )?
            .body_site(get_cc_with_one_code(
                SNOMED_BODYSITE_HEAD.to_string(),
                SNOMED_SYSTEM.to_string(),
            )?)
            .code(
                CodeableConcept::builder()
                    .coding(CODING_HEAD_CIRCUMFERENCE.clone())
                    .build()?,
            )
            .build()?,
        ));
    }
    Ok(None)
}

fn get_birth_obs_builder(
    msg: &Message,
    identifier: Identifier,
    quantity_value: f64,
    unit: String,
    profile: String,
) -> Result<ObservationBuilder, MappingError> {
    Ok(get_basic_observation_builder(msg)?
        .meta(Meta::builder().profile(vec![Some(profile)]).build()?)
        .identifier(vec![Some(identifier)])
        .category(vec![Some(get_cc_with_one_code(
            VITAL_SIGNS_CATEGORY_CODE.to_string(),
            VITAL_SIGNS_CATEGORY_SYSTEM.to_string(),
        )?)])
        .value(ObservationValue::Quantity(
            Quantity::builder()
                .value(quantity_value)
                .system(UCUM_SYSTEM.to_string())
                .code(unit)
                .build()?,
        )))
}

#[cfg(test)]
mod tests {
    use crate::fhir::observation::{
        CODING_BODY_HEIGHT, CODING_BODY_WEIGHT, CODING_HEAD_CIRCUMFERENCE,
        CODING_PATIENT_DISPOSITION, LOINC_BODY_HEIGHT, LOINC_BODY_WEIGHT, LOINC_HEAD_CIRCUMFERENCE,
        LOINC_PATIENT_DISPOSITION, map,
    };
    use crate::test_utils::tests::{get_test_config, read_test_resource};
    use fhir_model::r4b::resources::{Observation, ObservationValue, Resource};
    use hl7_parser::Message;
    use rstest::rstest;
    use std::collections::HashSet;

    #[rstest]
    #[case("a08_test.hl7", 3)]
    #[case("a03_test.hl7", 1)]
    fn map_birth_data_test(#[case] test_file: &str, #[case] obs_count_expected: usize) {
        let hl7 = read_test_resource(test_file);
        let msg = Message::parse_with_lenient_newlines(&hl7, true).expect("parse hl7 failed");
        let config = get_test_config();
        let expected_resource_count = obs_count_expected;

        let mapped = map(&msg, &config).unwrap();

        let mut used_codes: HashSet<String> = HashSet::new();
        let resources = mapped
            .iter()
            .map(|m| {
                match m.resource.clone() {
                    Some(r) => {
                        // validate observation
                        let obs: Observation = r.clone().try_into().unwrap();
                        let obs_code_value = obs
                            .code
                            .coding
                            .first()
                            .unwrap()
                            .as_ref()
                            .unwrap()
                            .code
                            .as_ref()
                            .unwrap()
                            .as_str();
                        match obs_code_value {
                            LOINC_BODY_WEIGHT => {
                                assert!(
                                    used_codes.insert(obs_code_value.to_string()),
                                    "each observation code may be created only once! code {} has been created already.",obs_code_value
                                );

                                if let ObservationValue::Quantity(q) = obs.value.clone().unwrap()
                                    && let Some(value) = q.value
                                {
                                    let expected = 3390f64;
                                    assert_expected_code(obs_code_value, value, &expected);
                                }
                            }
                            LOINC_HEAD_CIRCUMFERENCE => {
                                assert!(
                                    used_codes.insert(obs_code_value.to_string()),
                                    "each observation code may be created only once! code {} has been created already.",obs_code_value
                                );
                                if let ObservationValue::Quantity(q) = obs.value.clone().unwrap()
                                    && let Some(value) = q.value
                                {
                                    let expected = 48f64;
                                    assert_expected_code(obs_code_value, value, &expected);
                                }
                            }
                            LOINC_BODY_HEIGHT => {
                                assert!(
                                    used_codes.insert(obs_code_value.to_string()),
                                    "each observation code may be created only once! code {} has been created already.",obs_code_value
                                );
                                if let ObservationValue::Quantity(q) = obs.value.clone().unwrap()
                                    && let Some(value) = q.value
                                {
                                    let expected = 51f64;
                                    assert_expected_code(obs_code_value, value, &expected);
                                }
                            }
                            LOINC_PATIENT_DISPOSITION=>{
                                assert!(
                                    used_codes.insert(obs_code_value.to_string()),
                                    "each observation code may be created only once! code {} has been created already.",obs_code_value
                                );
                                if let ObservationValue::CodeableConcept(q) = obs.value.clone().unwrap()
                                    && let Some(value) = q.coding.first().unwrap().clone()
                                {
                                    assert_eq!(value.code.as_ref().unwrap().to_string(),"L".to_string())
                                }
                            }
                            _ => panic!("unexpected observation code {}", obs_code_value),
                        }
                        r
                    }
                    None => panic!("resource is missing at BundleEntry!"),
                }
            })
            .collect::<Vec<Resource>>();
        assert_eq!(
            resources.len(),
            expected_resource_count,
            "expected observation count was  {}!",
            expected_resource_count
        );
    }

    fn assert_expected_code(obs_code_value: &str, value: f64, expected: &f64) {
        assert!(
            value.eq(expected),
            "expected value '{}' to be of Quantity {} f64 but got value {}",
            obs_code_value,
            expected,
            value
        );
    }
    #[test]
    fn constant_initialized_some_values() {
        assert!(CODING_BODY_HEIGHT.clone().iter().all(|v| v.is_some()));
        assert_eq!(CODING_BODY_HEIGHT.clone().len(), 3);
        assert!(CODING_BODY_WEIGHT.clone().iter().all(|v| v.is_some()));
        assert_eq!(CODING_BODY_WEIGHT.clone().len(), 3);
        assert!(
            CODING_HEAD_CIRCUMFERENCE
                .clone()
                .iter()
                .all(|v| v.is_some())
        );
        assert_eq!(CODING_HEAD_CIRCUMFERENCE.clone().len(), 2);
        assert!(
            CODING_PATIENT_DISPOSITION
                .clone()
                .iter()
                .all(|v| v.is_some())
        );
        assert_eq!(CODING_PATIENT_DISPOSITION.clone().len(), 1);
    }
}
