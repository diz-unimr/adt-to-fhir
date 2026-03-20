use crate::config::Fhir;
use crate::error::MappingError;
use crate::fhir::resources::ResourceMap;
use crate::fhir::sharded_functions::{get_cc_with_one_code, is_inpatient_location, parse_fab};
use crate::hl7::parser::{MessageType, message_type, parse_repeating_field_component_value};

use crate::fhir::mapper::{EntryRequestType, build_usual_identifier, bundle_entry};
use fhir_model::r4b::resources::{BundleEntry, Location};
use fhir_model::r4b::types::{CodeableConcept, Meta};
use hl7_parser::Message;

static LOCATION_TYPE_SYSTEM: &str = "http://terminology.hl7.org/CodeSystem/location-physical-type";

fn create_department_location(
    msg: &Message,
    config: &Fhir,
    resources: &ResourceMap,
) -> Result<Option<BundleEntry>, MappingError> {
    if let Some(department) = parse_fab(msg)? {
        // department location should be always available
        let mut ward_location = Location::builder()
            .meta(get_meta()?)
            .physical_type(get_cc_with_one_code(
                "wa".to_string(),
                config.location.system_ward.to_string(),
            )?)
            .identifier(vec![Some(build_usual_identifier(
                vec![department],
                config.location.system_ward.to_string(),
            )?)])
            .build()?;

        if let Some(icu_type) = is_location_icu(msg, resources)? {
            ward_location.r#type = icu_type;
        }

        return Ok(Some(bundle_entry(
            ward_location,
            EntryRequestType::UpdateAsCreate,
        )?));
    }
    Ok(None)
}

fn is_location_icu(
    msg: &Message,
    resources: &ResourceMap,
) -> Result<Option<Vec<Option<CodeableConcept>>>, MappingError> {
    // todo
    // if true return: vec![get_cc_with_one_code("ICU".to_string(),"http://terminology.hl7.org/CodeSystem/v3-RoleCode".to_string())];
    Ok(None)
}

fn create_room_location(
    msg: &Message,
    config: &Fhir,
    resources: &ResourceMap,
) -> Result<Option<BundleEntry>, MappingError> {
    let pv1_3_1 = parse_repeating_field_component_value(msg, "PV1", 3, 1)?;
    let pv1_3_2 = parse_repeating_field_component_value(msg, "PV1", 3, 2)?;

    if let (Some(pv1_3_1), Some(pv1_3_2)) = (pv1_3_1, pv1_3_2) {
        let room_location = Location::builder()
            .meta(get_meta()?)
            .physical_type(get_cc_with_one_code(
                "ro".to_string(),
                LOCATION_TYPE_SYSTEM.to_string(),
            )?)
            .identifier(vec![Some(build_usual_identifier(
                vec![pv1_3_1, pv1_3_2],
                config.location.system_room.to_string(),
            )?)])
            .build()?;
        return Ok(Some(bundle_entry(
            room_location,
            EntryRequestType::UpdateAsCreate,
        )?));
    }
    Ok(None)
}

fn get_meta() -> Result<Meta, MappingError> {
    Ok(Meta::builder().source("#orbis".to_string()).build()?)
}

fn create_bed_location(msg: &Message, config: &Fhir) -> Result<Option<BundleEntry>, MappingError> {
    let pv1_3_1 = parse_repeating_field_component_value(msg, "PV1", 3, 1)?;
    let pv1_3_2 = parse_repeating_field_component_value(msg, "PV1", 3, 2)?;
    let pv1_3_3 = parse_repeating_field_component_value(msg, "PV1", 3, 3)?;

    if let (Some(pv1_3_1), Some(pv1_3_2), Some(pv1_3_3)) = (pv1_3_1, pv1_3_2, pv1_3_3) {
        let room_location = Location::builder()
            .meta(get_meta()?)
            .physical_type(get_cc_with_one_code(
                "ro".to_string(),
                LOCATION_TYPE_SYSTEM.to_string(),
            )?)
            .identifier(vec![Some(build_usual_identifier(
                vec![pv1_3_1, pv1_3_2, pv1_3_3],
                config.location.system_bed.to_string(),
            )?)])
            .build()?;
        return Ok(Some(bundle_entry(
            room_location,
            EntryRequestType::UpdateAsCreate,
        )?));
    }
    Ok(None)
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
                    if let Some(entry) = create_bed_location(msg, &config)? {
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
