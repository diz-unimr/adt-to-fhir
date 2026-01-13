use crate::config::Fhir;
use crate::fhir::mapper::MessageAccessError::{MissingMessageField, MissingMessageSegment};
use crate::fhir::mapper::MessageType::*;
use crate::fhir::mapper::MessageTypeError::MissingMessageType;
use crate::fhir::resources::ResourceMap;
use crate::fhir::{encounter, patient};
use anyhow::anyhow;
use chrono::{Datelike, NaiveDate, NaiveDateTime, ParseError, TimeZone};
use chrono_tz::Europe::Berlin;
use fhir_model::r4b::codes::HTTPVerb::Patch;
use fhir_model::r4b::codes::{BundleType, HTTPVerb, IdentifierUse};
use fhir_model::r4b::resources::{
    Bundle, BundleEntry, BundleEntryRequest, IdentifiableResource, Parameters, Resource,
    ResourceType,
};
use fhir_model::r4b::types::{Identifier, Reference};
use fhir_model::time::error::InvalidFormatDescription;
use fhir_model::time::{Month, OffsetDateTime};
use fhir_model::DateFormatError::InvalidDate;
use fhir_model::{time, Date, DateTime};
use fhir_model::{BuilderError, DateFormatError, Instant};
use fmt::Display;
use hl7_parser::Message;
use std::fmt;
use std::str::FromStr;
use thiserror::Error;

#[derive(Debug, Error)]
pub(crate) enum MappingError {
    #[error(transparent)]
    MessageAccessError(#[from] MessageAccessError),
    #[error(transparent)]
    BuilderError(#[from] BuilderError),
    #[error(transparent)]
    FormattingError(#[from] FormattingError),
    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

#[derive(Debug, Error)]
pub enum FormattingError {
    #[error(transparent)]
    DateFormatError(#[from] DateFormatError),
    #[error(transparent)]
    ParseError(#[from] ParseError),
    #[error(transparent)]
    ParseDateError(#[from] time::error::Parse),
    #[error(transparent)]
    InvalidFormatError(#[from] InvalidFormatDescription),
    #[error(transparent)]
    ComponentRangeError(#[from] time::error::ComponentRange),
    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

#[derive(Debug, Error)]
pub enum MessageAccessError {
    #[error("Missing message segment {0}")]
    MissingMessageSegment(String),
    #[error("Missing message field {0} in segment {1}")]
    MissingMessageField(String, String),
    #[error(transparent)]
    MessageTypeError(#[from] MessageTypeError),
    #[error(transparent)]
    ParseError(#[from] hl7_parser::parser::ParseError),
}

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
        // TODO map observation
        let res = p.into_iter().chain(e).map(Some).collect();

        Ok(res)
    }
}

#[derive(PartialEq, Debug)]
pub enum MessageType {
    /// ADT A01
    Admit,
    /// ADT A02
    Transfer,
    /// ADT A03
    Discharge,
    /// ADT A04
    Registration,
    /// ADT A05
    PreAdmit,
    /// ADT A06
    ChangeOutpatientToInpatient,
    /// ADT A07
    ChangeInpatientToOutpatient,
    /// ADT A08
    PatientUpdate,
    /// ADT A11
    CancelAdmitVisit,
    /// ADT A12
    CancelTransfer,
    /// ADT A13
    CancelDischarge,
    /// ADT A14
    PendingAdmit,
    /// ADT A27
    CancelPendingAdmit,
    /// ADT A28
    AddPersonInformation,
    /// ADT A29
    DeletePersonInformation,
    /// ADT A31
    ChangePersonData,
    /// ADT A34
    PatientMerge,
    /// ADT A40
    MergePatientRecords,
    /// ADT A45
    PatientReassignmentToSingleCase,
    /// ADT A47
    PatientReassignmentToAllCases,
    /// ADT A50
    UpdateEncounterNumber,
}

impl Display for MessageType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Admit => write!(f, "A01"),
            Transfer => write!(f, "A02"),
            Discharge => write!(f, "A03"),
            Registration => write!(f, "A04"),
            PreAdmit => write!(f, "A05"),
            ChangeOutpatientToInpatient => write!(f, "A06"),
            ChangeInpatientToOutpatient => write!(f, "A07"),
            PatientUpdate => write!(f, "A08"),
            CancelAdmitVisit => write!(f, "A11"),
            CancelTransfer => write!(f, "A12"),
            CancelDischarge => write!(f, "A13"),
            PendingAdmit => write!(f, "A14"),
            CancelPendingAdmit => write!(f, "A27"),
            AddPersonInformation => write!(f, "A28"),
            DeletePersonInformation => write!(f, "A29"),
            ChangePersonData => write!(f, "A31"),
            PatientMerge => write!(f, "A34"),
            MergePatientRecords => write!(f, "A40"),
            PatientReassignmentToSingleCase => write!(f, "A45"),
            PatientReassignmentToAllCases => write!(f, "A47"),
            UpdateEncounterNumber => write!(f, "A50"),
        }
    }
}

#[derive(Debug, Error)]
pub enum MessageTypeError {
    #[error("Unknown message type: {0}")]
    UnknownMessageType(String),
    #[error("Missing message type: {0}")]
    MissingMessageType(String),
}

// todo refactor
impl FromStr for MessageType {
    type Err = MessageTypeError;

    fn from_str(s: &str) -> Result<Self, MessageTypeError> {
        match s {
            "A01" => Ok(Admit),
            "A02" => Ok(Transfer),
            "A03" => Ok(Discharge),
            "A04" => Ok(Registration),
            "A05" => Ok(PreAdmit),
            "A06" => Ok(ChangeOutpatientToInpatient),
            "A07" => Ok(ChangeInpatientToOutpatient),
            "A08" => Ok(PatientUpdate),
            "A11" => Ok(CancelAdmitVisit),
            "A12" => Ok(CancelTransfer),
            "A13" => Ok(CancelDischarge),
            "A14" => Ok(PendingAdmit),
            "A27" => Ok(CancelPendingAdmit),
            "A28" => Ok(AddPersonInformation),
            "A29" => Ok(DeletePersonInformation),
            "A31" => Ok(ChangePersonData),
            "A34" => Ok(PatientMerge),
            "A40" => Ok(MergePatientRecords),
            "A45" => Ok(PatientReassignmentToSingleCase),
            "A47" => Ok(PatientReassignmentToAllCases),
            "A50" => Ok(UpdateEncounterNumber),
            _ => Err(MessageTypeError::UnknownMessageType(s.to_string())),
        }
    }
}

pub(crate) fn message_type(msg: &Message) -> Result<MessageType, MessageTypeError> {
    MessageType::from_str(
        msg.segment("EVN")
            .ok_or(MissingMessageType("missing ENV segment".to_string()))?
            .field(1)
            .ok_or(MissingMessageType(
                "missing message type segment".to_string(),
            ))?
            .raw_value(),
    )
}

pub(crate) enum EntryRequestType {
    UpdateAsCreate,
    ConditionalCreate,
}

pub(crate) fn bundle_entry<T: IdentifiableResource + Clone>(
    resource: T,
    request_type: EntryRequestType,
) -> Result<BundleEntry, anyhow::Error>
where
    Resource: From<T>,
{
    // resource type
    let resource_type = Resource::from(resource.clone()).resource_type();

    // identifier
    let identifier = resource
        .identifier()
        .iter()
        .flatten()
        .find(|&id| id.r#use.is_some_and(|u| u == IdentifierUse::Usual))
        .ok_or(anyhow!("missing identifier with use: 'usual'"))?;

    let request = match request_type {
        EntryRequestType::UpdateAsCreate => BundleEntryRequest::builder()
            .method(HTTPVerb::Put)
            .url(conditional_reference(&resource_type, identifier)?)
            .build()?,

        EntryRequestType::ConditionalCreate => BundleEntryRequest::builder()
            .method(HTTPVerb::Post)
            .url(resource_type.to_string())
            .if_none_exist(identifier_search(
                identifier
                    .system
                    .as_deref()
                    .ok_or(anyhow!("identifier.system missing"))?,
                identifier
                    .value
                    .as_deref()
                    .ok_or(anyhow!("identifier.value missing"))?,
            ))
            .build()?,
    };

    BundleEntry::builder()
        .resource(resource.clone().into())
        .request(request)
        .build()
        .map_err(|e| e.into())
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

fn conditional_reference(
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

fn default_identifier(identifiers: Vec<Option<Identifier>>) -> Option<Identifier> {
    if identifiers.iter().flatten().count() == 1 {
        identifiers.into_iter().next()?
    } else {
        identifiers
            .iter()
            .flatten()
            .filter_map(|i| {
                // use USUAL identifier for now
                if i.r#use? == IdentifierUse::Usual {
                    Some(i.clone())
                } else {
                    None
                }
            })
            .next()
    }
}

pub(crate) fn parse_component(
    field: &str,
    component: usize,
) -> Result<Option<String>, hl7_parser::parser::ParseError> {
    let comp = hl7_parser::parser::parse_field(field)?
        .component(component)
        .map(|c| c.raw_value().to_string())
        .filter(|s| !s.is_empty());

    Ok(comp)
}

pub(crate) fn parse_subcomponents(
    field: &str,
    component: usize,
) -> Result<Option<Vec<String>>, hl7_parser::parser::ParseError> {
    let comp: Option<Vec<String>> = hl7_parser::parser::parse_field(field)?
        .component(component)
        .map(|c| {
            c.subcomponents
                .iter()
                .map(|s| s.raw_value().to_string())
                .filter(|s| !s.is_empty())
                .collect()
        });

    Ok(comp)
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
        .reference(format!(
            "{res_type}?identifier={}",
            identifier_search(system, id)
        ))
        .build()?)
}

pub(crate) fn parse_field(
    msg: &Message,
    segment: &str,
    field: usize,
) -> Result<Option<String>, MessageAccessError> {
    Ok(Some(
        msg.segment(segment)
            .ok_or(MissingMessageSegment(segment.to_string()))?
            .field(field)
            .ok_or(MissingMessageField(field.to_string(), segment.to_string()))?
            .raw_value()
            .to_string(),
    )
    .filter(|f| !f.is_empty()))
}

#[cfg(test)]
mod tests {
    use crate::config::{FallConfig, Fhir, ResourceConfig};
    use crate::fhir::mapper::Identifier;
    use crate::fhir::mapper::{
        parse_component, parse_datetime, parse_subcomponents, patch_bundle_entry, FhirMapper,
    };
    use crate::fhir::resources::{Department, ResourceMap};
    use crate::tests::read_test_resource;
    use fhir_model::r4b::codes::HTTPVerb::Patch;
    use fhir_model::r4b::resources::{
        Bundle, BundleEntry, BundleEntryRequest, Encounter, Parameters, Patient, Resource,
        ResourceType,
    };
    use fhir_model::time::{Month, OffsetDateTime, Time};
    use fhir_model::DateTime::DateTime;
    use fhir_model::{time, WrongResourceType};
    use std::collections::HashMap;

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

        let config = Fhir {
            person: ResourceConfig {
                profile: "https://www.medizininformatik-initiative.de/fhir/core/modul-person/StructureDefinition/Patient|2025.0.0".to_string(),
                system: "https://fhir.diz.uni-marburg.de/sid/patient-id".to_string(),
            },
            fall: FallConfig {
                profile: "https://www.medizininformatik-initiative.de/fhir/core/modul-fall/StructureDefinition/KontaktGesundheitseinrichtung|2025.0.0".to_string(),
                system: "https://fhir.diz.uni-marburg.de/sid/encounter-id".to_string(),
                einrichtungskontakt: Default::default(),
                abteilungskontakt: Default::default(),
                versorgungsstellenkontakt: Default::default(),
            },
        };
        let mapper = FhirMapper {
            config: config.clone(),
            resources: ResourceMap {
                department_map: HashMap::from([(
                    "POL".to_string(),
                    Department {
                        abteilungs_bezeichnung: "Pneumologie".to_string(),
                        fachabteilungs_schluessel: "0800".to_string(),
                    },
                )]),
                location_map: Default::default(),
            },
        };

        // act
        let mapped = mapper.map(hl7).unwrap();

        // map back to assert
        let bundle: Bundle = serde_json::from_str(mapped.unwrap().as_str()).unwrap();

        assert_eq!(bundle.entry.len(), 2);

        let patient: Vec<Patient> = bundle
            .entry
            .iter()
            .flatten()
            .filter_map(|e| to_patient(e).ok())
            .collect();
        let encounter: Vec<Encounter> = bundle
            .entry
            .iter()
            .flatten()
            .filter_map(|e| to_encounter(e).ok())
            .collect();

        // assert profiles set
        let p = patient.first().unwrap();
        let patient_profile = p
            .meta
            .as_ref()
            .unwrap()
            .profile
            .first()
            .unwrap()
            .as_ref()
            .unwrap()
            .as_str();
        let e = encounter.first().unwrap();
        let encounter_profile = e
            .meta
            .as_ref()
            .unwrap()
            .profile
            .first()
            .unwrap()
            .as_ref()
            .unwrap()
            .as_str();

        assert_eq!(patient_profile, config.person.profile.to_owned());
        assert_eq!(encounter_profile, config.fall.profile.to_owned());
    }

    fn to_patient(e: &BundleEntry) -> Result<Patient, WrongResourceType> {
        let r = e.resource.clone().unwrap();
        Patient::try_from(r)
    }

    fn to_encounter(e: &BundleEntry) -> Result<Encounter, WrongResourceType> {
        let r = e.resource.clone().unwrap();
        Encounter::try_from(r)
    }

    #[test]
    fn test_parse_component() {
        let comp = parse_component("Talstraße 16&Talstraße&16^^Holzhausen^^67184^DE^L", 3).unwrap();

        assert_eq!(comp, Some("Holzhausen".to_string()))
    }

    #[test]
    fn test_parse_subcomponent() {
        let sub =
            parse_subcomponents("Talstraße 16&Talstraße&16^^Holzhausen^^67184^DE^L", 1).unwrap();

        assert_eq!(
            sub,
            Some(vec![
                "Talstraße 16".to_string(),
                "Talstraße".to_string(),
                "16".to_string()
            ])
        )
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
