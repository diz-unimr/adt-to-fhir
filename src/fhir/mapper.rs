use crate::config::{AppConfig, Fhir};
use crate::fhir;
use crate::fhir::mapper::MessageType::*;
use anyhow::anyhow;
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

    pub(crate) fn map(&self, msg: String) -> Result<Option<String>, Box<dyn Error>> {
        // deserialize
        // TODO parse hl7 string correctly
        // let v2_msg = Message::parse(msg.as_str()).unwrap();
        let v2_msg = Message::parse_with_lenient_newlines(msg.as_str(), true).unwrap();
        // let msh = v2_msg.segment("MSH").unwrap();

        // let message_time = msh.field(7).unwrap();
        // let time: TimeStamp = message_time.raw_value().parse().unwrap();

        // map hl7 message
        let resources = self.map_resources(v2_msg)?;

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

    fn map_resources(&self, v2_msg: Message) -> Result<Vec<Option<BundleEntry>>, Box<dyn Error>> {
        // TODO replace example
        let p = map_patient(v2_msg.clone(), self.config.clone())?;
        let e = map_encounter(v2_msg, self.config.clone())?;
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

// impl Into<String> for MessageType {f
//     fn into(self) -> String {
//         self.to_string()
//     }
// }

impl FromStr for MessageType {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
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
            _ => Err(anyhow!("unknown message type")),
        }
    }
}

pub(crate) fn bundle_entry<T: IdentifiableResource + Clone>(
    resource: T,
) -> Result<BundleEntry, Box<dyn Error>>
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
        .ok_or("missing identifier with use: 'usual'")?;

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
                        .expect("identifier.system missing")
                        .to_owned(),
                    identifier
                        .value
                        .clone()
                        .expect("identifier.value missing")
                        .to_owned(),
                ))
                .build()
                .expect("failed to build BundleEntryRequest"),
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
