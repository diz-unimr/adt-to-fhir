use crate::config::{AppConfig, Fhir};
use crate::fhir;
use crate::fhir::encounter::EncounterError;
use crate::fhir::mapper::MessageType::*;
use crate::fhir::mapper::MessageTypeError::MissingMessageType;
use anyhow::anyhow;
use chrono::format::{DelayedFormat, StrftimeItems};
use chrono::{DateTime, NaiveDate, NaiveDateTime, Utc};
use fhir::encounter::map_encounter;
use fhir::patient::map_patient;
use fhir_model::r4b::codes::{BundleType, HTTPVerb, IdentifierUse};
use fhir_model::r4b::resources::{
    Bundle, BundleEntry, BundleEntryRequest, IdentifiableResource, Resource, ResourceType,
};
use fhir_model::r4b::types::{Identifier, Reference};
use hl7_parser::Message;
use std::error::Error;
use std::str::FromStr;
use thiserror::Error;

#[derive(Debug, Error)]
enum MappingError {
    #[error(transparent)]
    Other(#[from] anyhow::Error),
    #[error(transparent)]
    Encounter(#[from] EncounterError),
}

#[derive(Clone)]
pub(crate) struct FhirMapper {
    pub(crate) config: Fhir,
}

impl FhirMapper {
    pub(crate) fn new(config: AppConfig) -> Result<Self, Box<dyn Error>> {
        Ok(FhirMapper {
            config: config.fhir,
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
        let e = map_encounter(v2_msg, self.config.clone())?;
        // TODO map observation
        let res = p.into_iter().chain(e).map(|p| Some(p)).collect();

        Ok(res)
    }
}

#[derive(PartialEq, Debug)]
pub enum MessageType {
    Admit,
    Transfer,
    Discharge,
    Registration,
    PreAdmit,
    ChangeOutpatientToInpatient,
    ChangeInpatientToOutpatient,
    PatientUpdate,
    CancelAdmitVisit,
    CancelTransfer,
    CancelDischarge,
    PendingAdmit,
    CancelPendingAdmit,
    AddPersonInformation,
    DeletePersonInformation,
    ChangePersonData,
    MergePatientRecords,
    PatientReassignmentToSingleCase,
    PatientReassignmentToAllCases,
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
    Ok(MessageType::from_str(
        msg.segment("EVN")
            .ok_or(MissingMessageType("missing ENV segment".to_string()))?
            .field(1)
            .ok_or(MissingMessageType(
                "missing message type segment".to_string(),
            ))?
            .raw_value(),
    )?)
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
        .filter(|&id| id.r#use.is_some_and(|u| u == IdentifierUse::Usual))
        .next()
        .ok_or(anyhow!("missing identifier with use: 'usual'"))?;

    BundleEntry::builder()
        .resource(resource.clone().into())
        .request(
            BundleEntryRequest::builder()
                .method(HTTPVerb::Put)
                .url(conditional_reference(
                    resource_type,
                    identifier
                        .system
                        .clone()
                        .ok_or(anyhow!("identifier.system missing"))?
                        .to_owned(),
                    identifier
                        .value
                        .clone()
                        .ok_or(anyhow!("identifier.value missing"))?
                        .to_owned(),
                ))
                .build()?,
        )
        .build()
        .map_err(|e| e.into())
        .into()
}

fn conditional_reference(resource_type: ResourceType, system: String, value: String) -> String {
    format!("{resource_type}?identifier={system}|{value}")
}

fn resource_ref(res_type: ResourceType, identifiers: Vec<Option<Identifier>>) -> Option<Reference> {
    default_identifier(identifiers).map(|id| {
        Reference::builder()
            .reference(format!(
                "{}?identifier={}|{}",
                res_type,
                id.system.clone().unwrap(),
                id.value.clone().unwrap()
            ))
            .build()
            .unwrap()
    })
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
) -> Result<Option<String>, anyhow::Error> {
    let repeat = hl7_parser::parser::parse_repeat(field_value)?;
    match repeat.component(component) {
        Some(c) => Ok(c.raw_value().to_string().parse().ok()),
        None => Ok(None),
    }
}

pub(crate) fn parse_date_string_to_date(
    input: &str,
) -> Result<DelayedFormat<StrftimeItems>, anyhow::Error> {
    // Step 1: Parse the string into a NaiveDate
    let naive_date = NaiveDate::parse_from_str(input, "%Y%m%d")?;

    // Step 2: Create a NaiveDateTime (at midnight)
    let naive_datetime = naive_date
        .and_hms_opt(0, 0, 0)
        .ok_or(anyhow!("Invalid time when constructing datetime"))?;

    // Step 3: Convert to DateTime<Utc>
    let datetime_utc: DateTime<Utc> = DateTime::from_naive_utc_and_offset(naive_datetime, Utc);
    let date = datetime_utc.format("%Y-%m-%d");

    // Step 4: Extract just the date portion
    Ok(date)
}

pub(crate) fn parse_date_string_to_datetime(
    input: &str,
) -> Result<DelayedFormat<StrftimeItems>, anyhow::Error> {
    // Parse to NaiveDateTime using the correct format
    let naive_datetime = NaiveDateTime::parse_from_str(input, "%Y%m%d%H%M")?;

    // Convert to DateTime<Utc>
    let datetime_utc: DateTime<Utc> = DateTime::from_naive_utc_and_offset(naive_datetime, Utc);
    let date = datetime_utc.format("%Y-%m-%dT%H:%M:%SZ");
    Ok(date)
}

pub(crate) fn subject_ref(msg: &Message, sid: String) -> Result<Reference, anyhow::Error> {
    let pid = hl7_field(msg, "PID", 2)?;

    Ok(Reference::builder()
        .reference(conditional_reference(
            ResourceType::Patient,
            sid,
            pid.to_string(),
        ))
        .build()?)
}

pub(crate) fn hl7_field(
    msg: &Message,
    segment: &str,
    field: usize,
) -> Result<String, anyhow::Error> {
    Ok(msg
        .segment(segment)
        .ok_or(anyhow!("missing {segment} segment"))?
        .field(field)
        .ok_or(anyhow!("missing field {field}"))?
        .raw_value()
        .to_string())
}
