use crate::config::Fhir;
use crate::error::{FormattingError, MappingError, MessageAccessError};
use crate::fhir::resources::ResourceMap;
use crate::fhir::{encounter, location, observation, patient};
use crate::hl7::parser::query;
use anyhow::anyhow;
use chrono::{Datelike, NaiveDate, NaiveDateTime, TimeZone};
use chrono_tz::Europe::Berlin;
use fhir_model::DateFormatError::InvalidDate;
use fhir_model::r4b::codes::HTTPVerb::Patch;
use fhir_model::r4b::codes::{BundleType, HTTPVerb, IdentifierUse};
use fhir_model::r4b::resources::{
    Bundle, BundleEntry, BundleEntryRequest, IdentifiableResource, Parameters, Resource,
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
        let o = observation::map(v2_msg, &self.config)?;
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

#[cfg(test)]
mod tests {
    use super::*;
    use fhir_model::DateTime::DateTime;
    use fhir_model::r4b::codes::HTTPVerb::Patch;
    use fhir_model::r4b::resources::{
        Bundle, BundleEntry, BundleEntryRequest, Encounter, Parameters, Patient, Resource,
        ResourceType,
    };
    use std::str::FromStr;

    use crate::test_utils::tests::{
        filter_resources, get_dummy_resources, get_test_config, has_profile, read_test_resource,
    };
    use fhir_model::time;
    use fhir_model::time::{Month, OffsetDateTime, Time};
    use rstest::rstest;

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

        let patient: Vec<Patient> = filter_resources(&bundle);
        let encounter: Vec<Encounter> = filter_resources(&bundle);

        // assert profiles set
        assert!(
            patient
                .iter()
                .all(|p| has_profile(p.meta.as_ref().unwrap(), &config.person.profile))
        );
        assert!(
            encounter
                .iter()
                .all(|e| has_profile(e.meta.as_ref().unwrap(), &config.fall.profile))
        );
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

    #[rstest]
    #[case("A11", "DELETE", "", 3)]
    #[case("A12", "DELETE", "", 2)]
    #[case("A27", "DELETE", "", 3)]
    #[case("A04", "PUT", "PUT", 5)]
    #[case("A02", "PUT", "POST", 7)]
    fn map_request_and_encounter_type_test(
        #[case] msg_type: String,
        #[case] request_type_encounter: String,
        #[case] request_type_patient: String,
        #[case] resource_count: usize,
    ) {
        let hl7 = format!(
            r#"MSH|^~\&|ORBIS|KH|RECAPP|ORBIS|202111230904||ADT^{}_{}|62325574|P|2.5|||||D||DE
EVN|{}|202111230904|202111230904||Muster
PID|1|1396227|1396227||Test^Anton||19510704|M|||Teststr. 26^^Wetzlar^^35578^D^L||0151/123123123^^CP|||M|or|||||||N||SYR
PV1|1|I|UROST133^133-03^1^URO^KLINIKUM^900000|R^^HL7~01^Normalfall^301||UROST133^^^URO^KLINIKUM^900000||35576TEO^Test^Ulrike^^Frau^Dr. med.^Karl-Test-Ring 23^35576^Test^06441^45433^FÄ für Test|35576TEO^Test^Ulrike^^Frau^Dr. med.^Karl-Test-Ring 23^35576^Test^06441^45433^FÄ für Allgemeinmedizin|N||||||N|||23232323||K|||||||||||||||01|||2200|9||||202111190630|202111230904||||||A
PV2||xxx|02^KH-Behandlung, vollstat. nach vorstat.^301||||||202112030000||||||||||||N|||I||||||||||||N
ZBE|30674176^ORBIS|202111230904||DUMMY"#,
            msg_type, msg_type, msg_type
        );

        let config = get_test_config();
        let mapper = FhirMapper {
            config: config.clone(),
            resources: get_dummy_resources(),
        };

        let expected_request_type = HTTPVerb::from_str(request_type_encounter.as_str()).unwrap();

        // act
        let mapped = mapper.map(hl7).unwrap();
        let bundle: Bundle = serde_json::from_str(mapped.unwrap().as_str()).unwrap();

        bundle.entry.iter().for_each(|entry| {
            let entry_typ = entry
                .as_ref()
                .unwrap()
                .resource
                .as_ref()
                .unwrap()
                .resource_type();
            match entry_typ {
                ResourceType::Encounter => {
                    check_request_type(&msg_type, expected_request_type, entry);
                }
                ResourceType::Location => {}
                ResourceType::Patient => {
                    match msg_type.as_str() {
                        "A04" | "A02" => {}
                        _ => {
                            assert_eq!(
                                "For message type '{}' patient resource should not be created.",
                                msg_type
                            );
                        }
                    }

                    check_request_type(
                        &msg_type,
                        HTTPVerb::from_str(request_type_patient.as_str()).unwrap(),
                        entry,
                    );
                }
                _ => {
                    panic!(
                        "unexpected resource type '{}' at message type '{}",
                        entry_typ, msg_type
                    );
                }
            }
        });

        assert_eq!(
            bundle.entry.len(),
            resource_count,
            "For message type '{}' we expect {} resource to be created.",
            msg_type,
            resource_count
        );

        if msg_type == "A11" || msg_type == "A27" {
            assert!(
                bundle
                    .entry
                    .iter()
                    .find(|entry| {
                        entry
                            .as_ref()
                            .unwrap()
                            .request
                            .as_ref()
                            .unwrap()
                            .url
                            .eq(format!(
                                "Encounter?identifier={}|{}",
                                config.fall.einrichtungskontakt.system, "23232323"
                            )
                            .as_str())
                    })
                    .is_some()
            );
        }
        assert!(
            bundle
                .entry
                .iter()
                .find(|entry| {
                    entry
                        .as_ref()
                        .unwrap()
                        .request
                        .as_ref()
                        .unwrap()
                        .url
                        .eq(format!(
                            "Encounter?identifier={}|{}",
                            config.fall.abteilungskontakt.system, "30674176"
                        )
                        .as_str())
                })
                .is_some()
        );
        assert!(
            bundle
                .entry
                .iter()
                .find(|entry| {
                    entry
                        .as_ref()
                        .unwrap()
                        .request
                        .as_ref()
                        .unwrap()
                        .url
                        .eq(format!(
                            "Encounter?identifier={}|{}",
                            config.fall.versorgungsstellenkontakt.system, "30674176"
                        )
                        .as_str())
                })
                .is_some()
        )
    }

    fn check_request_type(
        msg_type: &String,
        expected_request_type: HTTPVerb,
        entry: &Option<BundleEntry>,
    ) {
        let resource_name = entry
            .as_ref()
            .unwrap()
            .resource
            .as_ref()
            .unwrap()
            .resource_type()
            .as_str();
        assert_eq!(
            expected_request_type,
            entry.as_ref().unwrap().request.as_ref().unwrap().method,
            "At msg_type {} resource {} must be send with {} request",
            msg_type,
            resource_name,
            expected_request_type
        );
        if expected_request_type == HTTPVerb::Post {
            assert!(
                entry
                    .as_ref()
                    .unwrap()
                    .request
                    .as_ref()
                    .unwrap()
                    .if_none_exist
                    .is_some(),
                "on msg type '{}' resource {} must be send with if-none-exists entry!",
                msg_type,
                resource_name
            );
        }
    }
}
