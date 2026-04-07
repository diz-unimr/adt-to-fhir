use crate::config::Fhir;
use crate::error::{FormattingError, MappingError, MessageAccessError};
use crate::fhir::resources::ResourceMap;
use crate::fhir::{encounter, location, patient};
use crate::hl7::parser::query;
use anyhow::anyhow;
use chrono::{Datelike, NaiveDate, NaiveDateTime, TimeZone};
use chrono_tz::Europe::Berlin;
use fhir_model::DateFormatError::InvalidDate;
use fhir_model::r4b::codes::HTTPVerb::Patch;
use fhir_model::r4b::codes::{BundleType, HTTPVerb, IdentifierUse};
use fhir_model::r4b::resources::{
    Bundle, BundleEntry, BundleEntryRequest, IdentifiableResource, Location, Parameters, Resource,
    ResourceType,
};
use fhir_model::r4b::types::{CodeableConcept, Coding, Identifier, Meta, Reference};
use fhir_model::time::{Month, OffsetDateTime};
use fhir_model::{BuilderError, Instant};
use fhir_model::{Date, DateTime, time};
use hl7_parser::Message;

#[derive(Clone)]
pub(crate) struct FhirMapper {
    pub(crate) config: Fhir,
    pub(crate) resources: ResourceMap,
}

impl FhirMapper {
    pub(crate) fn new(config: Fhir) -> Result<Self, anyhow::Error> {
        Ok(FhirMapper {
            config,
            resources: ResourceMap::new()?,
        })
    }

    pub(crate) fn map(&self, msg: String) -> Result<Option<String>, anyhow::Error> {
        // deserialize
        let v2_msg = Message::parse_with_lenient_newlines(msg.as_str(), true)?;

        // map hl7 message
        let resources = self.map_resources(&v2_msg)?;

        if resources.is_empty() {
            return Ok(None);
        }

        let result = Bundle::builder()
            .r#type(BundleType::Transaction)
            .entry(resources)
            .build()?;

        // serialize
        let result = serde_json::to_string(&result).expect("failed to serialize output bundle");

        Ok(Some(result))
    }

    fn map_resources(&self, v2_msg: &Message) -> Result<Vec<Option<BundleEntry>>, MappingError> {
        let p = patient::map(v2_msg, self.config.clone())?;
        let e = encounter::map(v2_msg, self.config.clone(), &self.resources)?;
        let l = location::map(v2_msg, self.config.clone(), &self.resources)?;
        // TODO map observation
        let res = p.into_iter().chain(e).chain(l).map(Some).collect();

        Ok(res)
    }
}

pub(crate) enum EntryRequestType {
    UpdateAsCreate,
    ConditionalCreate,
    Delete,
}

pub(crate) fn bundle_entry<T: IdentifiableResource + Clone>(
    resource: T,
    request_type: EntryRequestType,
) -> Result<BundleEntry, MappingError>
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
        .ok_or(anyhow!("missing identifier with use: 'usual'"))?;

    // resource type
    let resource_type = r.resource_type();

    let request = bundle_entry_request(resource_type, identifier, request_type)?;

    BundleEntry::builder()
        .resource(r)
        .request(request)
        .build()
        .map_err(|e| e.into())
}

fn bundle_entry_request(
    resource_type: ResourceType,
    identifier: &Identifier,
    request_type: EntryRequestType,
) -> Result<BundleEntryRequest, MappingError> {
    Ok(match request_type {
        EntryRequestType::UpdateAsCreate => BundleEntryRequest::builder()
            .method(HTTPVerb::Put)
            .url(conditional_reference(&resource_type, identifier)?)
            .build()?,

        EntryRequestType::ConditionalCreate => BundleEntryRequest::builder()
            .method(HTTPVerb::Post)
            .url(resource_type.to_string())
            .if_none_exist(conditional_reference(&resource_type, identifier)?)
            .build()?,

        EntryRequestType::Delete => BundleEntryRequest::builder()
            .method(HTTPVerb::Delete)
            .url(conditional_reference(&resource_type, identifier)?)
            .build()?,
    })
}

pub(crate) fn patch_bundle_entry(
    resource: Parameters,
    resource_type: &ResourceType,
    identifier: &Identifier,
) -> Result<BundleEntry, MappingError> {
    let request = BundleEntryRequest::builder()
        .method(Patch)
        .url(conditional_reference(resource_type, identifier)?)
        .build()?;

    BundleEntry::builder()
        .resource(resource.into())
        .request(request)
        .build()
        .map_err(|e| e.into())
}

pub(crate) fn conditional_reference(
    resource_type: &ResourceType,
    identifier: &Identifier,
) -> Result<String, MappingError> {
    Ok(format!(
        "{resource_type}?{}",
        identifier_search(
            identifier
                .system
                .as_deref()
                .ok_or(anyhow!("identifier.system missing"))?,
            identifier
                .value
                .as_deref()
                .ok_or(anyhow!("identifier.value missing"))?
        )
    ))
}

fn identifier_search(system: &str, value: &str) -> String {
    format!("identifier={system}|{value}")
}

pub(crate) fn parse_datetime(input: &str) -> Result<DateTime, FormattingError> {
    let dt = NaiveDateTime::parse_from_str(input, "%Y%m%d%H%M")?;
    let dt_with_tz = Berlin
        .from_local_datetime(&dt)
        .earliest()
        .ok_or(InvalidDate)?;

    Ok(DateTime::DateTime(Instant(
        OffsetDateTime::from_unix_timestamp(dt_with_tz.timestamp())?,
    )))
}
pub(crate) fn resource_ref(
    res_type: &ResourceType,
    id: &str,
    system: &str,
) -> Result<Reference, MappingError> {
    Ok(Reference::builder()
        .reference(format!("{res_type}?{}", identifier_search(system, id)))
        .build()?)
}

pub(crate) fn parse_date(input: &str) -> Result<Date, FormattingError> {
    let dt = NaiveDate::parse_and_remainder(input, "%Y%m%d")?.0;
    let date = time::Date::from_calendar_date(
        dt.year(),
        Month::try_from(dt.month() as u8)?,
        dt.day() as u8,
    )?;
    Ok(Date::Date(date))
}

pub(crate) fn build_usual_identifier(
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

pub fn is_inpatient_location(msg: &Message) -> Result<bool, MappingError> {
    Ok(
        query(msg, "PV1.2") == Some("I")
            && query(msg, "PV1.3.5").map(|v| v == "KLINIKUM").is_some(),
    )
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

pub fn parse_fab<'a>(msg: &'a Message<'a>) -> Result<Option<&'a str>, MessageAccessError> {
    let facility = query(msg, "PV1.3.4");
    let location = query(msg, "PV1.3.1");
    let loc_status = query(msg, "PV1.3.5");
    // let kostenstelle = extract_repeat(assigned_loc, 6)?;

    // todo: kostenstelle lookup etc.
    match (facility, location, loc_status) {
        // 1. wenn PV1-3.1 und PV1-3.4 Wert haben -> PV1-3.4
        (Some(f), Some(_), _) => Ok(Some(f)),
        // 2. wenn PV1-3.4 leer & PV1-3.1 hat Wert -> dann  PV1-3.1
        (None, Some(l), _) => Ok(Some(l)),
        // 3. wenn PV1-3.1 leer & PV1-3.4 hat Wert-> dann  PV1-3.5
        (Some(_), None, Some(st)) => Ok(Some(st)),
        _ => Ok(None),
    }
}

pub(crate) fn get_meta() -> Result<Meta, MappingError> {
    Ok(Meta::builder().source("#orbis".to_string()).build()?)
}

static LOCATION_TYPE_SYSTEM: &str = "http://terminology.hl7.org/CodeSystem/location-physical-type";

pub(crate) fn create_locations(
    msg: &Message,
    config: &Fhir,
    resources: &ResourceMap,
) -> Result<Option<Vec<Location>>, MappingError> {
    let mut result: Vec<Location> = vec![];
    if is_inpatient_location(msg)? {
        let pv1_3_1 = query(msg, "PV1.3.1");
        let pv1_3_2 = query(msg, "PV1.3.2");
        let pv1_3_3 = query(msg, "PV1.3.3");

        if let Some(department) = parse_fab(msg)? {
            match (pv1_3_1, pv1_3_2, pv1_3_3) {
                (Some(_), None, None) => {
                    result.push(map_ward_location(msg, department, config, resources)?)
                }
                (Some(pv1_3_1), Some(pv1_3_2), None) => {
                    result.push(map_ward_location(msg, department, config, resources)?);
                    result.push(map_room_location(config, pv1_3_1, pv1_3_2)?)
                }
                (Some(pv1_3_1), Some(pv1_3_2), Some(pv1_3_3)) => {
                    result.push(map_ward_location(msg, department, config, resources)?);
                    result.push(map_room_location(config, pv1_3_1, pv1_3_2)?);
                    result.push(map_bed_location(config, pv1_3_1, pv1_3_2, pv1_3_3)?);
                }
                (_, _, _) => {}
            }
        }
    }
    if result.is_empty() {
        return Ok(None);
    }

    Ok(Some(result))
}

fn map_location_type_icu(
    pv1_3_1_value: &str,
    resources: &ResourceMap,
) -> Result<Option<Vec<Option<CodeableConcept>>>, MappingError> {
    if let Some(ward_entry) = resources.ward_map.get(pv1_3_1_value)
        && ward_entry.is_icu
    {
        Ok(Some(vec![Some(get_cc_with_one_code(
            "ICU".to_string(),
            "http://terminology.hl7.org/CodeSystem/v3-RoleCode".to_string(),
        )?)]))
    } else {
        Ok(None)
    }
}

pub(crate) fn map_ward_location(
    msg: &Message,
    department: &str,
    config: &Fhir,
    resources: &ResourceMap,
) -> Result<Location, MappingError> {
    let mut location = Location::builder()
        .meta(get_meta()?)
        .physical_type(get_cc_with_one_code(
            "wa".to_string(),
            config.location.system_ward.to_string(),
        )?)
        .identifier(vec![Some(build_usual_identifier(
            vec![department],
            config.location.system_ward.to_string(),
        )?)])
        .build()
        .map_err(MappingError::BuilderError)?;
    if let Some(icu_coding) = query(msg, "PV1.3.1")
        .and_then(|field_value| map_location_type_icu(field_value, resources).transpose())
    {
        location.r#type = icu_coding?;
    }
    Ok(location)
}

pub(crate) fn map_room_location(
    config: &Fhir,
    pv1_3_1: &str,
    pv1_3_2: &str,
) -> Result<Location, MappingError> {
    Location::builder()
        .meta(get_meta()?)
        .physical_type(get_cc_with_one_code(
            "ro".to_string(),
            LOCATION_TYPE_SYSTEM.to_string(),
        )?)
        .identifier(vec![Some(build_usual_identifier(
            vec![pv1_3_1, pv1_3_2],
            config.location.system_room.clone(),
        )?)])
        .build()
        .map_err(MappingError::BuilderError)
}

pub(crate) fn map_bed_location(
    config: &Fhir,
    pv1_3_1: &str,
    pv1_3_2: &str,
    pv1_3_3: &str,
) -> Result<Location, MappingError> {
    Location::builder()
        .meta(get_meta()?)
        .physical_type(get_cc_with_one_code(
            "bd".to_string(),
            LOCATION_TYPE_SYSTEM.to_string(),
        )?)
        .identifier(vec![Some(build_usual_identifier(
            vec![pv1_3_1, pv1_3_2, pv1_3_3],
            config.location.system_bed.to_string(),
        )?)])
        .build()
        .map_err(MappingError::BuilderError)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tests::read_test_resource;
    use fhir_model::DateTime::DateTime;
    use fhir_model::r4b::codes::HTTPVerb::Patch;
    use fhir_model::r4b::resources::{
        Bundle, BundleEntry, BundleEntryRequest, Encounter, Location, Parameters, Patient,
        Resource, ResourceType,
    };

    use crate::test_utils::tests::{get_dummy_resources, get_test_config, resource_from};
    use fhir_model::time;
    use fhir_model::time::{Month, OffsetDateTime, Time};

    #[test]
    fn test_parse_datetime() {
        // 2009-03-30 19:36
        let s = "200903301036";

        let parsed = parse_datetime(s).unwrap();

        let expected = DateTime(
            OffsetDateTime::new_utc(
                time::Date::from_calendar_date(2009, Month::March, 30).unwrap(),
                // local time is +2 (CEST) in this case
                Time::from_hms(8, 36, 0).unwrap(),
            )
            .into(),
        );

        assert_eq!(parsed, expected);
    }

    #[test]
    fn map_test() {
        let hl7 = read_test_resource("a01_test.hl7");

        let config = get_test_config();
        let mapper = FhirMapper {
            config: config.clone(),
            resources: get_dummy_resources(),
        };

        // act
        let mapped = mapper.map(hl7).unwrap();

        // map back to assert
        let bundle: Bundle = serde_json::from_str(mapped.unwrap().as_str()).unwrap();

        assert_eq!(bundle.entry.len(), 5);

        let patient: Vec<Patient> = bundle
            .entry
            .iter()
            .flatten()
            .filter_map(|e| resource_from(e).ok())
            .collect();
        let encounter: Vec<Encounter> = bundle
            .entry
            .iter()
            .flatten()
            .filter_map(|e| resource_from(e).ok())
            .collect();

        let location: Vec<Location> = bundle
            .entry
            .iter()
            .flatten()
            .filter_map(|e| resource_from(e).ok())
            .collect();

        // assert profiles set
        assert!(patient.iter().all(|p| {
            p.meta
                .as_ref()
                .unwrap()
                .profile
                .iter()
                .flatten()
                .find(|&pr| pr.as_str() == config.person.profile)
                .is_some()
        }));
        assert!(encounter.iter().all(|e| {
            e.meta
                .as_ref()
                .unwrap()
                .profile
                .iter()
                .flatten()
                .find(|&pr| pr.as_str() == config.fall.profile)
                .is_some()
        }));
    }

    #[test]
    fn test_patch_bundle_entry() {
        let entry = patch_bundle_entry(
            Parameters::builder().build().unwrap(),
            &ResourceType::Patient,
            &Identifier::builder()
                .system("system".to_string())
                .value("value".to_string())
                .build()
                .unwrap(),
        )
        .unwrap();

        assert_eq!(
            entry,
            BundleEntry::builder()
                .resource(Resource::from(Parameters::builder().build().unwrap()))
                .request(
                    BundleEntryRequest::builder()
                        .method(Patch)
                        .url("Patient?identifier=system|value".to_string())
                        .build()
                        .unwrap()
                )
                .build()
                .unwrap(),
        )
    }
}
