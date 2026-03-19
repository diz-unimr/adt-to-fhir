use crate::config::Fhir;
use crate::error::MappingError;

use crate::fhir::misc::is_inpatient_location;
use crate::fhir::resources::ResourceMap;
use crate::hl7::parser::{MessageType, message_type};

use fhir_model::r4b::resources::BundleEntry;

use hl7_parser::Message;

fn create_department_location(
    msg: &Message,
    config: &Fhir,
    resources: &ResourceMap,
) -> Result<Option<BundleEntry>, MappingError> {
    todo!("implement department as location")
}

fn create_room_location(
    msg: &Message,
    config: &Fhir,
    resources: &ResourceMap,
) -> Result<Option<BundleEntry>, MappingError> {
    todo!("implement room as location")
}

fn create_bed_location(
    msg: &Message,
    config: &Fhir,
    resources: &ResourceMap,
) -> Result<Option<BundleEntry>, MappingError> {
    todo!("implement bed as location")
}

fn create_care_site_location(
    msg: &Message,
    config: &Fhir,
    resources: &ResourceMap,
) -> Result<Option<BundleEntry>, MappingError> {
    todo!("implement care site as location")
}

pub(super) fn map(
    msg: &Message,
    config: Fhir,
    resources: &ResourceMap,
) -> Result<Vec<BundleEntry>, MappingError> {
    let mut r: Vec<BundleEntry> = vec![];
    if let Ok(msg_type) = message_type(msg) {
        match msg_type {
            MessageType::A02 | MessageType::A01 => {
                if let Some(entry) = create_department_location(msg, &config, resources)? {
                    r.push(entry);
                }
                if is_inpatient_location(msg)? {
                    if let Some(entry) = create_bed_location(msg, &config, resources)? {
                        r.push(entry);
                    }
                    if let Some(entry) = create_room_location(msg, &config, resources)? {
                        r.push(entry);
                    }
                }
            }
            MessageType::A04 => {
                if let Some(entry) = create_department_location(msg, &config, resources)? {
                    r.push(entry);
                }
            }
            _ => { // nothing },
            }
        }
    }
    Ok(r)
}
