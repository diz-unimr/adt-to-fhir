use crate::fhir_error::{ContentError, ParsingError};
use chrono::{Datelike, NaiveDate, NaiveDateTime, TimeZone};
use chrono_tz::Europe::Berlin;
use fhir_model::DateFormatError::InvalidDate;
use fhir_model::r4b::resources::ResourceType;
use fhir_model::r4b::types::{
    CodeableConcept, Coding, Extension, ExtensionValue, FieldExtension, Reference,
};
use fhir_model::time::{Month, OffsetDateTime};
use fhir_model::{BuilderError, Date, DateTime, Instant, time};

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
/// FieldExtension with unsupported data absent reason entry
pub fn coding_data_absent_reason_unsupported() -> Result<CodeableConcept, BuilderError> {
    Ok(CodeableConcept::builder()
        .coding(vec![Some(
            Coding::builder()
                .code("unsupported".to_string())
                .system("http://terminology.hl7.org/CodeSystem/data-absent-reason".to_string())
                .build()?,
        )])
        .build()?)
}

pub fn field_extension(
    url: String,
    ext_value: ExtensionValue,
) -> Result<FieldExtension, BuilderError> {
    FieldExtension::builder()
        .extension(vec![
            Extension::builder().url(url).value(ext_value).build()?,
        ])
        .build()
}

pub fn parse_date(input: &str) -> Result<Date, ParsingError> {
    let dt = NaiveDate::parse_and_remainder(input, "%Y%m%d")?.0;

    let date = time::Date::from_calendar_date(
        dt.year(),
        Month::try_from(dt.month() as u8)?,
        dt.day() as u8,
    )?;
    Ok(Date::Date(date))
}
