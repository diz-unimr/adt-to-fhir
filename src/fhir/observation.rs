use crate::config::Fhir;
use crate::error::MappingError::FatalError;
use crate::error::{FormattingError, MappingError, MessageAccessError};
use crate::fhir::mapper::{
    EntryRequestType, build_usual_identifier, bundle_entry, get_cc_with_one_code, parse_datetime,
};
use crate::fhir::resources::ResourceMap;
use crate::hl7::parser::{
    PID_PID, PV1_VISIT_ID, ZBE_BEGINN_OF_MOVEMENT, ZNG_BODY_HEIGHT, ZNG_HEAD_CIRCUMFERENCE,
    ZNG_WEIGHT, query,
};
use anyhow::anyhow;
use fhir_model::r4b::codes::ObservationStatus;
use fhir_model::r4b::resources::{
    BundleEntry, Observation, ObservationEffective, ObservationValue,
};
use fhir_model::r4b::types::{CodeableConcept, Coding, Identifier, Meta, Quantity};
use hl7_parser::Message;
use std::sync::LazyLock;

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
const UCUM_SYSTEM: &str = "http://unitsofmeasure.org";

const CODING_HEAD_CIRCUMFERENCE: LazyLock<Vec<Option<Coding>>> = LazyLock::new(|| {
    vec![
        Coding::builder()
            .code(LOINC_HEAD_CIRCUMFERENCE.into())
            .system(LOINC_SYSTEM.into())
            .display("Head Occipital-frontal circumference".to_string())
            .build()
            .ok(),
        Coding::builder()
            .code("363811000".to_string())
            .system(SNOMED_SYSTEM.into())
            .display("Head circumference measure (observable entity)".to_string())
            .version(SNOMED_VERSION.into())
            .build()
            .ok(),
    ]
});

const CODING_BODY_WEIGHT: LazyLock<Vec<Option<Coding>>> = LazyLock::new(|| {
    vec![
        Coding::builder()
            .code(LOINC_BODY_WEIGHT.into())
            .system(LOINC_SYSTEM.into())
            .display("Body weight".to_string())
            .build()
            .ok(),
        Coding::builder()
            .code("8339-4".to_string())
            .system(LOINC_SYSTEM.into())
            .display("Birth weight Measured".into())
            .build()
            .ok(),
        Coding::builder()
            .code("27113001".to_string())
            .system(SNOMED_SYSTEM.into())
            .display("Body weight (observable entity)".to_string())
            .version(SNOMED_VERSION.into())
            .build()
            .ok(),
    ]
});

const CODING_BODY_HEIGHT: LazyLock<Vec<Option<Coding>>> = LazyLock::new(|| {
    vec![
        Coding::builder()
            .code(LOINC_BODY_HEIGHT.into())
            .system(LOINC_SYSTEM.into())
            .display("Body height".to_string())
            .build()
            .ok(),
        Coding::builder()
            .code("89269-5".to_string())
            .system(LOINC_SYSTEM.into())
            .display("Body height Measured --at birth".into())
            .build()
            .ok(),
        Coding::builder()
            .code("1153637007".to_string())
            .system(SNOMED_SYSTEM.into())
            .display("Body height (observable entity)".to_string())
            .version(SNOMED_VERSION.into())
            .build()
            .ok(),
    ]
});

pub(crate) fn map(
    msg: &Message,
    config: &Fhir,
    resources: &ResourceMap,
) -> Result<Vec<BundleEntry>, MappingError> {
    let mut result: Vec<BundleEntry> = vec![];

    let is_alife = map_is_alife(msg)?;
    if let Some(is_alife) = is_alife {
        result.push(bundle_entry(is_alife, EntryRequestType::UpdateAsCreate)?);
    }

    let head = build_vitals_status_observation(msg, config, ObsToBuild::HeadCircumference)?;
    if let Some(head) = head {
        result.push(bundle_entry(head, EntryRequestType::UpdateAsCreate)?);
    }

    let weight = build_vitals_status_observation(msg, config, ObsToBuild::BodyWeight)?;
    if let Some(weight) = weight {
        result.push(bundle_entry(weight, EntryRequestType::UpdateAsCreate)?);
    }
    let height = build_vitals_status_observation(msg, config, ObsToBuild::BodyLength)?;
    if let Some(height) = height {
        result.push(bundle_entry(height, EntryRequestType::UpdateAsCreate)?);
    }

    Ok(result)
}

enum ObsToBuild {
    VitalStatus,
    HeadCircumference,
    BodyWeight,
    BodyLength,
}
fn build_vitals_status_observation(
    msg: &Message,
    config: &Fhir,
    target: ObsToBuild,
) -> Result<Option<Observation>, MappingError> {
    let pid = query(msg, PID_PID);
    let visit = query(msg, PV1_VISIT_ID);
    let profile: String;
    let identifier: Option<Identifier>;
    let body_site: Option<CodeableConcept>;
    let quantity_value: Option<f64>;
    let coding: Vec<Option<Coding>>;
    let unit: Option<String>;
    if let (Some(pid), Some(visit)) = (pid, visit) {
        match target {
            ObsToBuild::VitalStatus => {
                //todo
                identifier = None;
                profile = config.observation.profile_vital_status.clone();
                body_site = None;
                quantity_value = None;
                //todo
                coding = vec![None];
                unit = None;
            }

            ObsToBuild::HeadCircumference => {
                quantity_value = query(msg, ZNG_HEAD_CIRCUMFERENCE)
                    .map(|val| val.parse::<f64>().map_err(FormattingError::ParseFloatError))
                    .transpose()?;

                identifier = Some(build_usual_identifier(
                    vec![LOINC_HEAD_CIRCUMFERENCE, pid, visit],
                    config.observation.system.clone(),
                )?);

                body_site = Some(get_cc_with_one_code(
                    SNOMED_BODYSITE_HEAD.into(),
                    SNOMED_SYSTEM.into(),
                )?);

                profile = config.observation.profile_head_circumference.clone();

                coding = CODING_HEAD_CIRCUMFERENCE.clone();
                unit = Some("cm".to_string());
            }

            ObsToBuild::BodyWeight => {
                identifier = Some(build_usual_identifier(
                    vec![LOINC_BODY_WEIGHT, pid, visit],
                    config.observation.system.clone(),
                )?);
                profile = config.observation.profile_weight.clone();
                body_site = None;
                quantity_value = query(msg, ZNG_WEIGHT)
                    .map(|val| val.parse::<f64>().map_err(FormattingError::ParseFloatError))
                    .transpose()?;
                coding = CODING_BODY_WEIGHT.clone();
                unit = Some("g".to_string());
            }
            ObsToBuild::BodyLength => {
                identifier = Some(build_usual_identifier(
                    vec![LOINC_BODY_HEIGHT, pid, visit],
                    config.observation.system.clone(),
                )?);
                profile = config.observation.profile_height.clone();
                body_site = None;
                quantity_value = query(msg, ZNG_BODY_HEIGHT)
                    .map(|val| val.parse::<f64>().map_err(FormattingError::ParseFloatError))
                    .transpose()?;
                coding = CODING_BODY_HEIGHT.clone();
                unit = Some("cm".to_string());
            }
        };

        let mut obs = Observation::builder()
            .status(ObservationStatus::Final)
            .category(vec![Some(get_cc_with_one_code(
                VITAL_SIGNS_CATEGORY_CODE.into(),
                VITAL_SIGNS_CATEGORY_SYSTEM.into(),
            )?)])
            .identifier(vec![identifier])
            .meta(Meta::builder().profile(vec![Some(profile)]).build()?)
            .effective(ObservationEffective::DateTime(parse_datetime(
                query(msg, ZBE_BEGINN_OF_MOVEMENT).ok_or(MessageAccessError::Other(anyhow!(
                    "ZBE.2 dateTime value missing!"
                )))?,
            )?))
            .code(CodeableConcept::builder().coding(coding).build()?)
            .build()?;

        if body_site.is_some() {
            obs.body_site = body_site;
        }
        if let Some(quantity_value) = quantity_value {
            if let Some(unit) = unit {
                obs.value = Some(ObservationValue::Quantity(
                    Quantity::builder()
                        .value(quantity_value)
                        .system(UCUM_SYSTEM.to_string())
                        .code(unit.to_string())
                        .build()?,
                ));
            } else {
                return Err(FatalError(
                    "every quantity should have a unit - this is a bug, \
                since we should know which values are mapped!"
                        .to_string(),
                ));
            }
        }

        return Ok(Some(obs));
    };

    Ok(None)
}
fn map_is_alife(msg: &Message) -> Result<Option<Observation>, MappingError> {
    // welche Nachrichten haben die Lebend-Kennung im PID-Segment
    Ok(None)
}

#[cfg(test)]
mod tests {
    use crate::fhir::observation::map;
    use crate::test_utils::tests::{get_dummy_resources, get_test_config, read_test_resource};
    use fhir_model::r4b::resources::{Observation, ObservationValue, Resource};
    use hl7_parser::Message;

    #[test]
    fn map_vital_test() {
        let hl7 = read_test_resource("a08_test.hl7");
        let msg = Message::parse_with_lenient_newlines(&hl7, true).expect("parse hl7 failed");
        let resource = get_dummy_resources();
        let config = get_test_config();
        let expected_resource_count = 3;

        let mapped = map(&msg, &config, &resource).unwrap();

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
                            "29463-7" => {
                                if let ObservationValue::Quantity(q) = obs.value.clone().unwrap()
                                    && let Some(value) = q.value
                                {
                                    let expected = 3390f64;
                                    assert_expected_code(obs_code_value, value, &expected);
                                }
                            }
                            "9843-4" => {
                                if let ObservationValue::Quantity(q) = obs.value.clone().unwrap()
                                    && let Some(value) = q.value
                                {
                                    let expected = 48f64;
                                    assert_expected_code(obs_code_value, value, &expected);
                                }
                            }
                            "8302-2" => {
                                if let ObservationValue::Quantity(q) = obs.value.clone().unwrap()
                                    && let Some(value) = q.value
                                {
                                    let expected = 51f64;
                                    assert_expected_code(obs_code_value, value, &expected);
                                }
                            }
                            _ => panic!(),
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
}
