use crate::fhir_error::ContentError::MissingValueError;
use crate::fhir_error::{ContentError, ParsingError};
use adt_config::config::Fhir;
use chrono::{Datelike, NaiveDate, NaiveDateTime, TimeZone};
use chrono_tz::Europe::Berlin;
use fhir_model::DateFormatError::InvalidDate;
use fhir_model::r4b::codes::HTTPVerb::Patch;
use fhir_model::r4b::codes::{HTTPVerb, IdentifierUse};
use fhir_model::r4b::resources::{
    BundleEntry, BundleEntryRequest, IdentifiableResource, Parameters, Resource, ResourceType,
};
use fhir_model::r4b::types::{
    CodeableConcept, Coding, Extension, ExtensionValue, FieldExtension, Identifier, Reference,
};
use fhir_model::time::{Month, OffsetDateTime};
use fhir_model::{BuilderError, Date, DateTime, Instant, time};
use std::slice;
use uuid::Uuid;

pub fn parse_datetime(input: &str) -> Result<DateTime, ParsingError> {
    let dt = NaiveDateTime::parse_from_str(input, "%Y%m%d%H%M")?;
    let dt_with_tz = Berlin
        .from_local_datetime(&dt)
        .earliest()
        .ok_or(InvalidDate)?;

    Ok(DateTime::DateTime(Instant(
        OffsetDateTime::from_unix_timestamp(dt_with_tz.timestamp())?,
    )))
}

pub fn upsert_reference(
    resource_type: &ResourceType,
    identifier: &Identifier,
) -> Result<String, ContentError> {
    Ok(format!(
        "{resource_type}?{}",
        identifier_search(
            identifier.system.as_deref().ok_or(MissingValueError {
                property: "identifier.system missing".to_string()
            })?,
            identifier.value.as_deref().ok_or(MissingValueError {
                property: "identifier.value missing".to_string()
            })?
        )
    ))
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

pub enum EntryRequestType {
    UpdateAsCreate,
    ConditionalCreate,
    Delete,
}

pub fn bundle_entry<T: IdentifiableResource + Clone>(
    resource: T,
    request_type: EntryRequestType,
    config: &Fhir,
) -> Result<BundleEntry, ContentError>
where
    Resource: From<T>,
{
    // resource
    let r = Resource::from(resource.clone());

    // identifier
    let identifier = resource
        .identifier()
        .iter()
        .flatten()
        .find(|&id| id.r#use.is_some_and(|u| u == IdentifierUse::Usual))
        .ok_or(MissingValueError {
            property: "missing identifier with use: 'usual'".to_string(),
        })?;

    // resource type
    let resource_type = r.resource_type();

    let request = bundle_entry_request(resource_type, identifier, request_type)?;

    let identifiers: Vec<Identifier> = resource.identifier().iter().flatten().cloned().collect();

    let full_url = full_url_from_identifiers(&identifiers, config);

    BundleEntry::builder()
        .resource(r)
        .request(request)
        .full_url(full_url)
        .build()
        .map_err(|e| e.into())
}

pub fn bundle_entry_request(
    resource_type: ResourceType,
    identifier: &Identifier,
    request_type: EntryRequestType,
) -> Result<BundleEntryRequest, ContentError> {
    Ok(match request_type {
        EntryRequestType::UpdateAsCreate => BundleEntryRequest::builder()
            .method(HTTPVerb::Put)
            .url(upsert_reference(&resource_type, identifier)?)
            .build()?,

        EntryRequestType::ConditionalCreate => BundleEntryRequest::builder()
            .method(HTTPVerb::Post)
            .url(resource_type.to_string())
            .if_none_exist(conditional_reference(identifier)?)
            .build()?,

        EntryRequestType::Delete => BundleEntryRequest::builder()
            .method(HTTPVerb::Delete)
            .url(upsert_reference(&resource_type, identifier)?)
            .build()?,
    })
}

pub fn patch_bundle_entry(
    resource: Parameters,
    resource_type: &ResourceType,
    identifier: &Identifier,
    config: &Fhir,
) -> Result<BundleEntry, ContentError> {
    let request = BundleEntryRequest::builder()
        .method(Patch)
        .url(upsert_reference(resource_type, identifier)?)
        .build()?;

    BundleEntry::builder()
        .resource(resource.into())
        .request(request)
        .full_url(full_url_from_identifiers(
            slice::from_ref(identifier),
            config,
        ))
        .build()
        .map_err(|e| e.into())
}

/// Erzeugt eine deterministische fullUrl aus den Identifier-Values einer Ressource.
/// Mehrere Identifier werden sortiert und konkateniert, damit die Reihenfolge
/// keinen Einfluss auf das Ergebnis hat.
pub fn full_url_from_identifiers(identifiers: &[Identifier], config: &Fhir) -> String {
    let namespace = Uuid::new_v5(&Uuid::NAMESPACE_DNS, config.facility_id.as_ref());

    let mut values: Vec<String> = identifiers
        .iter()
        .filter_map(|id| {
            // system + value kombinieren, damit gleiche value in unterschiedlichen
            // Systemen nicht kollidieren
            match (&id.system, &id.value) {
                (Some(system), Some(value)) => Some(format!("{}|{}", system, value)),
                (None, Some(value)) => Some(value.clone()),
                _ => None,
            }
        })
        .collect();

    // Sortieren für Determinismus, unabhängig von der Reihenfolge im Bundle
    values.sort();
    let input = values.join(";");

    let uuid = Uuid::new_v5(&namespace, input.as_bytes());
    format!("urn:uuid:{}", uuid)
}

pub fn conditional_reference(identifier: &Identifier) -> Result<String, ContentError> {
    Ok(identifier_search(
        identifier.system.as_deref().ok_or(MissingValueError {
            property: "identifier.system missing".to_string(),
        })?,
        identifier.value.as_deref().ok_or(MissingValueError {
            property: "identifier.system missing".to_string(),
        })?,
    ))
}

pub fn build_usual_identifier(
    value_components: Vec<&str>,
    system: String,
) -> Result<Identifier, BuilderError> {
    let identifier_value = value_components.join("_");

    Identifier::builder()
        .r#use(IdentifierUse::Usual)
        .system(system)
        .value(identifier_value)
        .build()
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
