use crate::config::{AppConfig, Fhir};
use fhir_model::r4b::codes::{BundleType, HTTPVerb, IdentifierUse};
use fhir_model::r4b::resources::{Bundle, BundleEntry, BundleEntryRequest, Encounter, IdentifiableResource, Patient, Resource, ResourceType};
use fhir_model::r4b::types::{
    Identifier, Meta
    , Reference,
};
use fhir_model::BuilderError;
use hl7_parser::datetime::TimeStamp;
use hl7_parser::Message;
use std::error::Error;

#[derive(Clone)]
pub(crate) struct Mapper {
    pub(crate) config: Fhir,
}

impl Mapper {
    pub(crate) fn map(&self, msg: String) -> Result<Option<String>, Box<dyn Error>> {
        // deserialize
        // TODO parse hl7 string correctly
        // let v2_msg = Message::parse(msg.as_str()).unwrap();
        let v2_msg = Message::parse_with_lenient_newlines(msg.as_str(),true).unwrap();
        let msh = v2_msg.segment("MSH").unwrap();


        let message_time = msh.field(7).unwrap();
        let time: TimeStamp = message_time.raw_value().parse().unwrap();

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

fn resource_ref(res_type:ResourceType,identifiers: Vec<Option<Identifier>>) -> Option<Reference> {
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

impl Mapper {
    pub(crate) fn new(config: AppConfig) -> Result<Self, Box<dyn Error>> {
        Ok(Mapper {
            config: config.fhir,
        })
    }

    fn map_resources(&self, v2_msg: Message) -> Result<Vec<Option<BundleEntry>>, Box<dyn Error>> {
        // TODO replace example
        let p = self.map_patient(v2_msg)?;
        // let e = self.map_encounter()?;

        let res= vec![
            Some(bundle_entry(p)?),
            // Some(bundle_entry(e)?)
        ];

        Ok(res)
    }

    fn map_patient(&self, v2_msg: Message) -> Result<Patient, Box<dyn Error>> {

        let pid_seg = v2_msg.segment("PID").ok_or("missing PID segment")?;
        let pid = pid_seg.field(2).ok_or("missing Patient ID field")?;

        let builder= Patient::builder()
            .meta(Meta::builder().profile(vec![Some(self.config.person.profile.to_owned())]).build()?)
            .identifier(vec![Some(
                Identifier::builder()
                    .r#use(IdentifierUse::Usual)
                    .system(self.config.person.system.to_owned())
                    .value(pid.raw_value().to_owned())
                    .build()
                    .unwrap(),
            )]);
        // TODO

        Ok(builder.build()?)
    }

    fn map_encounter() -> Result<Encounter,BuilderError> {
        let e= Encounter::builder().build()?;
        Ok(e)
    }
}
