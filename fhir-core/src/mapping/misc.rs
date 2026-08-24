use crate::fhir_error::ContentError;
use chrono::{NaiveDateTime, TimeZone};
use chrono_tz::Europe::Berlin;
use fhir_model::DateFormatError::InvalidDate;
use fhir_model::r4b::resources::ResourceType;
use fhir_model::r4b::types::{CodeableConcept, Coding, Reference};
use fhir_model::time::OffsetDateTime;
use fhir_model::{BuilderError, DateTime, Instant};

pub fn parse_datetime(input: &str) -> Result<DateTime, ContentError> {
    let dt = NaiveDateTime::parse_from_str(input, "%Y%m%d%H%M")?;
    let dt_with_tz = Berlin
        .from_local_datetime(&dt)
        .earliest()
        .ok_or(InvalidDate)?;

    Ok(DateTime::DateTime(Instant(
        OffsetDateTime::from_unix_timestamp(dt_with_tz.timestamp())?,
    )))
}

pub fn resource_ref(
    res_type: &ResourceType,
    id: &str,
    system: &str,
) -> Result<Reference, BuilderError> {
    Ok(Reference::builder()
        .reference(format!("{res_type}?{}", identifier_search(system, id)))
        .build()?)
}

pub fn identifier_search(system: &str, value: &str) -> String {
    format!("identifier={system}|{value}")
}
pub fn get_cc_with_one_code(code: String, system: String) -> Result<CodeableConcept, BuilderError> {
    CodeableConcept::builder()
        .coding(vec![Some(
            Coding::builder()
                .code(code.to_string())
                .system(system.to_string())
                .build()?,
        )])
        .build()
}
