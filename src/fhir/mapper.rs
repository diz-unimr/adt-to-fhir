use crate::config::Fhir;
use crate::fhir;
use crate::fhir::mapper::MessageAccessError::{MissingMessageField, MissingMessageSegment};
use crate::fhir::mapper::MessageType::*;
use crate::fhir::mapper::MessageTypeError::MissingMessageType;
use crate::fhir::resources::ResourceMap;
use anyhow::anyhow;
use chrono::{Datelike, NaiveDateTime, ParseError, TimeZone};
use chrono_tz::Europe::Berlin;
use fhir::encounter::map_encounter;
use fhir::patient::map_patient;
use fhir_model::r4b::codes::{BundleType, HTTPVerb, IdentifierUse};
use fhir_model::r4b::resources::{
    Bundle, BundleEntry, BundleEntryRequest, IdentifiableResource, Resource, ResourceType,
};
use fhir_model::r4b::types::{Identifier, Reference};
use fhir_model::time::error::InvalidFormatDescription;
use fhir_model::time::{Month, OffsetDateTime};
use fhir_model::DateFormatError::InvalidDate;
use fhir_model::{time, Date, DateTime};
use fhir_model::{BuilderError, DateFormatError, Instant};
use hl7_parser::Message;
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
        // TODO parse hl7 string correctly
        // let v2_msg = Message::parse(msg.as_str()).unwrap();
        let v2_msg = Message::parse_with_lenient_newlines(msg.as_str(), true)?;
        // let msh = v2_msg.segment("MSH").unwrap();

        // let message_time = msh.field(7).unwrap();
        // let time: TimeStamp = message_time.raw_value().parse().unwrap();

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
        let p = map_patient(v2_msg, self.config.clone())?;
        let e = map_encounter(v2_msg, self.config.clone(), &self.resources)?;
        // TODO map observation
        let res = p.into_iter().chain(e).map(|p| Some(p)).collect();

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
    /// ADT A40
    MergePatientRecords,
    /// ADT A45
    PatientReassignmentToSingleCase,
    /// ADT A47
    PatientReassignmentToAllCases,
    /// ADT A50
    UpdateEncounterNumber,
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

// todo: request type parameter
pub(crate) fn bundle_entry<T: IdentifiableResource + Clone>(
    resource: T,
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

    BundleEntry::builder()
        .resource(resource.clone().into())
        .request(
            BundleEntryRequest::builder()
                .method(HTTPVerb::Put)
                .url(conditional_reference(
                    &resource_type,
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
        )
        .build()
        .map_err(|e| e.into())
}

fn conditional_reference(resource_type: &ResourceType, system: &str, value: &str) -> String {
    format!("{resource_type}?identifier={system}|{value}")
}

fn default_identifier(identifiers: Vec<Option<Identifier>>) -> Option<Identifier> {
    match identifiers.iter().flatten().count() == 1 {
        true => identifiers.into_iter().next().unwrap(),
        false => identifiers
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
            .next(),
    }
}

pub(crate) fn extract_repeat(
    field_value: &str,
    component: usize,
) -> Result<Option<String>, hl7_parser::parser::ParseError> {
    let repeat = hl7_parser::parser::parse_repeat(field_value)?;
    match repeat.component(component) {
        Some(c) => Ok(c.raw_value().to_string().parse().ok()),
        None => Ok(None),
    }
}

pub(crate) fn parse_date(input: &str) -> Result<Date, FormattingError> {
    let dt = NaiveDateTime::parse_and_remainder(input, "%Y%m%d%H%M")?.0;
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
        .reference(conditional_reference(res_type, system, id))
        .build()?)
}

pub(crate) fn hl7_field(
    msg: &Message,
    segment: &str,
    field: usize,
) -> Result<String, MessageAccessError> {
    Ok(msg
        .segment(segment)
        .ok_or(MissingMessageSegment(segment.to_string()))?
        .field(field)
        .ok_or(MissingMessageField(field.to_string(), segment.to_string()))?
        .raw_value()
        .to_string())
}

#[cfg(test)]
mod tests {
    use crate::config::{FallConfig, Fhir, ResourceConfig};
    use crate::fhir::mapper::{parse_datetime, FhirMapper};
    use crate::fhir::resources::{Department, ResourceMap};
    use crate::tests::read_test_resource;
    use fhir_model::r4b::resources::{Bundle, BundleEntry, Encounter, Patient};
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
}
