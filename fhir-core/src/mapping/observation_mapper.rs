use crate::fhir_error::{ContentError, FhirMappingError};
use crate::mapping::misc::{
    EntryRequestType, build_usual_identifier, bundle_entry, get_cc_with_one_code,
    parse_datetime_from_offset, resource_ref,
};
use crate::model::obervation_dto::ObservationDto;
use adt_config::config::Fhir;

use fhir_model::r4b::codes::ObservationStatus;
use fhir_model::r4b::resources::{
    BundleEntry, Observation, ObservationBuilder, ObservationEffective, ObservationValue,
    ResourceType,
};
use fhir_model::r4b::types::{CodeableConcept, Coding, Identifier, Meta, Quantity};
use fhir_model::time::OffsetDateTime;
use std::ops::Div;

const LOINC_PATIENT_DISPOSITION: &str = "67162-8";
const LOINC_BODY_WEIGHT: &str = "29463-7";
const LOINC_BODY_HEIGHT: &str = "8302-2";
const LOINC_HEAD_CIRCUMFERENCE: &str = "9843-4";
const SNOMED_BODYSITE_HEAD: &str = "69536005";
const SNOMED_SYSTEM: &str = "http://snomed.info/sct";
const LOINC_SYSTEM: &str = "http://loinc.org";
const VITAL_SIGNS_CATEGORY_SYSTEM: &str =
    "http://terminology.hl7.org/CodeSystem/observation-category";
const VITAL_SIGNS_CATEGORY_CODE: &str = "vital-signs";
const SURVEY_CATEGORY_CODE: &str = "survey";
const UCUM_SYSTEM: &str = "http://unitsofmeasure.org";

pub fn map(input: &ObservationDto, config: &Fhir) -> Result<Vec<BundleEntry>, FhirMappingError> {
    let mut result: Vec<BundleEntry> = vec![];

    if let Some(is_alive) = map_vital_status(input, config)? {
        result.push(bundle_entry(
            is_alive,
            EntryRequestType::UpdateAsCreate,
            config,
        )?);
    }

    if let Some(head) = map_head_circumference(input, config)? {
        result.push(bundle_entry(
            head,
            EntryRequestType::UpdateAsCreate,
            config,
        )?);
    }

    if let Some(weight) = map_body_weight(input, config)? {
        result.push(bundle_entry(
            weight,
            EntryRequestType::UpdateAsCreate,
            config,
        )?);
    }

    if let Some(length) = map_body_length(input, config)? {
        result.push(bundle_entry(
            length,
            EntryRequestType::UpdateAsCreate,
            config,
        )?);
    }

    Ok(result)
}

fn get_vital_obs(data: &ObservationDto) -> Result<Vec<BundleEntry>, FhirMappingError> {
    todo!()
}
fn patient_disposition_coding() -> Vec<Option<Coding>> {
    vec![
        Coding::builder()
            .code(LOINC_PATIENT_DISPOSITION.to_string())
            .display("Patient Disposition".to_string())
            .system(LOINC_SYSTEM.to_string())
            .build()
            .ok(),
    ]
}

fn map_vital_status(
    data: &ObservationDto,
    config: &Fhir,
) -> Result<Option<Observation>, ContentError> {
    if let Some(isAlive) = data.is_alive
        && isAlive
    {
        let res = get_basic_observation_builder(data.effective_date)?
            .category(vec![Some(get_cc_with_one_code(
                SURVEY_CATEGORY_CODE.into(),
                VITAL_SIGNS_CATEGORY_SYSTEM.into(),
            )?)])
            .identifier(vec![Some(build_usual_identifier(
                vec![
                    LOINC_PATIENT_DISPOSITION,
                    data.pid.as_str(),
                    data.encounter_number.as_str(),
                ],
                config.observation.system.clone(),
            )?)])
            .meta(
                Meta::builder()
                    .source(config.meta_source.to_string())
                    .profile(vec![Some(config.observation.profile_vital_status.clone())])
                    .build()?,
            )
            .code(
                CodeableConcept::builder()
                    .coding(patient_disposition_coding())
                    .build()?,
            )
            .value(ObservationValue::CodeableConcept(
                CodeableConcept::builder()
                    .coding(is_alive_coding())
                    .build()?,
            ))
            .subject(resource_ref(
                &ResourceType::Patient,
                data.pid.as_str(),
                &config.person.system,
            )?)
            .encounter(resource_ref(
                &ResourceType::Encounter,
                data.encounter_number.as_str(),
                &config.fall.einrichtungskontakt.system,
            )?)
            .build()?;
        Ok(Some(res))
    } else {
        Ok(None)
    }
}
fn is_alive_coding() -> Vec<Option<Coding>> {
    vec![Coding::builder().code("L".to_string()).system("https://www.medizininformatik-initiative.de/fhir/core/modul-person/CodeSystem/Vitalstatus".to_string()).build().ok()]
}

fn get_basic_observation_builder(
    effective_datetime: OffsetDateTime,
) -> Result<ObservationBuilder, ContentError> {
    Ok(Observation::builder()
        .status(ObservationStatus::Final)
        .effective(ObservationEffective::DateTime(parse_datetime_from_offset(
            &effective_datetime,
        )?)))
}

fn map_body_length(
    input: &ObservationDto,
    config: &Fhir,
) -> Result<Option<Observation>, ContentError> {
    if let Some(quantity_value) = input.length {
        return Ok(Some(
            get_birth_obs_builder(
                input,
                config,
                build_usual_identifier(
                    vec![
                        LOINC_BODY_HEIGHT,
                        input.pid.as_str(),
                        input.encounter_number.as_str(),
                    ],
                    config.observation.system.clone(),
                )?,
                quantity_value as f64,
                "cm".to_string(),
                "centimeter".to_string(),
                config.observation.profile_height.to_string(),
            )?
            .code(
                CodeableConcept::builder()
                    .coding(vec![
                        Some(
                            Coding::builder()
                                .code(LOINC_BODY_HEIGHT.to_string())
                                .system(LOINC_SYSTEM.to_string())
                                .display("Body height".to_string())
                                .build()?,
                        ),
                        Some(
                            Coding::builder()
                                .code("89269-5".to_string())
                                .system(LOINC_SYSTEM.to_string())
                                .display("Body height Measured --at birth".to_string())
                                .build()?,
                        ),
                        Some(
                            Coding::builder()
                                .code("1153637007".to_string())
                                .system(SNOMED_SYSTEM.to_string())
                                // profile currently does not allow display and version
                                //.display("Body height (observable entity)".to_string())
                                //.version(SNOMED_VERSION.to_string())
                                .build()?,
                        ),
                    ])
                    .build()?,
            )
            .build()?,
        ));
    }
    Ok(None)
}

fn map_body_weight(
    input: &ObservationDto,
    config: &Fhir,
) -> Result<Option<Observation>, ContentError> {
    if let Some(quantity_value) = input.weight {
        let identifier = build_usual_identifier(
            vec![
                LOINC_BODY_WEIGHT,
                input.pid.as_str(),
                input.encounter_number.as_str(),
            ],
            config.observation.system.clone(),
        )?;

        // current profile has fixed unit to kg
        return Ok(Some(
            get_birth_obs_builder(
                input,
                config,
                identifier,
                (quantity_value as f64).div(1000f64),
                "kg".to_string(),
                "kilogram".to_string(),
                config.observation.profile_weight.to_string(),
            )?
            .code(
                CodeableConcept::builder()
                    .coding(vec![
                        Some(
                            Coding::builder()
                                .code(LOINC_BODY_WEIGHT.to_string())
                                .system(LOINC_SYSTEM.to_string())
                                .display("Body weight".to_string())
                                .build()?,
                        ),
                        /* currently only two ntries allowed
                        Coding::builder()
                        .code("8339-4".to_string())
                        .system(LOINC_SYSTEM.to_string())
                        .display("Birth weight Measured".to_string())
                        .build()
                        .ok(),*/
                        Some(
                            Coding::builder()
                                .code("27113001".to_string())
                                .system(SNOMED_SYSTEM.to_string())
                                // profile currently does not allow display and version
                                //.display("Body weight (observable entity)".to_string())
                                //.version(SNOMED_VERSION.to_string())
                                .build()?,
                        ),
                    ])
                    .build()?,
            )
            .build()?,
        ));
    }
    Ok(None)
}

fn map_head_circumference(
    input: &ObservationDto,
    config: &Fhir,
) -> Result<Option<Observation>, ContentError> {
    if let Some(quantity_value) = input.head_circumference {
        let identifier = build_usual_identifier(
            vec![
                LOINC_HEAD_CIRCUMFERENCE,
                input.pid.as_str(),
                input.encounter_number.as_str(),
            ],
            config.observation.system.clone(),
        )?;
        return Ok(Some(
            get_birth_obs_builder(
                input,
                config,
                identifier,
                quantity_value as f64,
                "cm".to_string(),
                "centimeter".to_string(),
                config.observation.profile_head_circumference.to_string(),
            )?
            .body_site(get_cc_with_one_code(
                SNOMED_BODYSITE_HEAD.to_string(),
                SNOMED_SYSTEM.to_string(),
            )?)
            .code(
                CodeableConcept::builder()
                    .coding(vec![
                        Some(
                            Coding::builder()
                                .code(LOINC_HEAD_CIRCUMFERENCE.to_string())
                                .system(LOINC_SYSTEM.to_string())
                                .display("Head Occipital-frontal circumference".to_string())
                                .build()?,
                        ),
                        Some(
                            Coding::builder()
                                .code("363812007".to_string())
                                .system(SNOMED_SYSTEM.to_string())
                                // profile currently does not allow display and version
                                //.display("Head circumference measure (observable entity)".to_string())
                                //.version(SNOMED_VERSION.to_string())
                                .build()?,
                        ),
                    ])
                    .build()?,
            )
            .build()?,
        ));
    }
    Ok(None)
}

fn get_birth_obs_builder(
    input: &ObservationDto,
    config: &Fhir,
    identifier: Identifier,
    quantity_value: f64,
    unit_code: String,
    unit: String,
    profile: String,
) -> Result<ObservationBuilder, ContentError> {
    Ok(get_basic_observation_builder(input.effective_date)?
        .meta(
            Meta::builder()
                .profile(vec![Some(profile)])
                .source(config.meta_source.to_string())
                .build()?,
        )
        .identifier(vec![Some(identifier)])
        .category(vec![Some(get_cc_with_one_code(
            VITAL_SIGNS_CATEGORY_CODE.to_string(),
            VITAL_SIGNS_CATEGORY_SYSTEM.to_string(),
        )?)])
        .value(ObservationValue::Quantity(
            Quantity::builder()
                .value(quantity_value)
                .system(UCUM_SYSTEM.to_string())
                .code(unit_code.clone())
                .unit(unit)
                .build()?,
        ))
        .subject(resource_ref(
            &ResourceType::Patient,
            input.pid.as_str(),
            &config.person.system,
        )?)
        .encounter(resource_ref(
            &ResourceType::Encounter,
            input.encounter_number.as_str(),
            &config.fall.einrichtungskontakt.system,
        )?))
}
