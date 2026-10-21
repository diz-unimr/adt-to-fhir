use crate::hl7::parser::{MessageType, PV1_3_1, PV1_3_2, PV1_3_3, ZBE_3, message_type, query};
use crate::hl7::{is_ward_valid_icu, parse_department_name};
use crate::hl7_error::{Hl7MappingError, Hl7MessageAccessError};
use adt_config::resources::ResourceMap;
use fhir_core::canonical_model::canonical_location::{
    LocationCdm, LocationCdmBuilder, LocationCdmType,
};

use hl7_parser::Message;

pub fn map(msg: &Message, resources: &ResourceMap) -> Result<Vec<LocationCdm>, Hl7MappingError> {
    let msg_type = message_type(msg)?;
    match msg_type {
        // location changes only at patient movement and admission
        MessageType::A02 | MessageType::A01 => Ok(create_locations_new(
            msg,
            resources,
            &[
                LocationCdmType::Ward,
                LocationCdmType::Room,
                LocationCdmType::Bed,
            ],
        )?),

        // department stays the same - we have only a short contact at another location
        MessageType::A04 => Ok(create_locations_new(
            msg,
            resources,
            &[LocationCdmType::Ward],
        )?),
        _ => {
            // skip other messages, since they should not add any locations.
            // also delete is not necessary since locations stay in the system,
            // even if a patient movement is revoked.
            Ok(vec![])
        }
    }
}

pub(crate) fn map_location(
    typ: &LocationCdmType,
    msg: &Message,
    resources: &ResourceMap,
) -> Result<Option<LocationCdm>, Hl7MappingError> {
    // Ohne Station (PV1-3.1) gibt es auf keiner Ebene eine Location
    let Some(ward) = query(msg, PV1_3_1) else {
        return Ok(None);
    };

    let (name, parent) = match typ {
        LocationCdmType::Ward => (
            ward.to_string(),
            parse_department_name(msg).map(|d| d.to_string()),
        ),
        LocationCdmType::Room => {
            let Some(room) = query(msg, PV1_3_2) else {
                return Ok(None);
            };
            (format!("{ward}_{room}"), Some(ward.to_string()))
        }
        LocationCdmType::Bed => {
            let (Some(room), Some(bed)) = (query(msg, PV1_3_2), query(msg, PV1_3_3)) else {
                return Ok(None);
            };
            (
                format!("{ward}_{room}_{bed}"),
                Some(format!("{ward}_{room}")),
            )
        }
    };

    let is_location_completed = get_location_status(msg)?;

    let mut location = LocationCdmBuilder::default();
    location
        .typ(typ.clone())
        .name(name)
        .manging_organization(parent)
        .is_movement_finished(is_location_completed);

    if *typ == LocationCdmType::Ward {
        location.is_icu(is_ward_valid_icu(msg, resources));
    } else {
        location.is_icu(false);
    }

    location.build().map(Some).map_err(|err| err.into())
}

fn create_locations_new(
    msg: &Message,
    resources: &ResourceMap,
    targets: &[LocationCdmType],
) -> Result<Vec<LocationCdm>, Hl7MappingError> {
    if parse_department_name(msg).is_none() {
        return Ok(vec![]);
    }

    let locations = targets
        .iter()
        .filter_map(|typ| map_location(typ, msg, resources).transpose())
        .collect::<Result<Vec<_>, _>>()?;

    Ok(locations)
}

fn get_location_status(msg: &Message) -> anyhow::Result<bool, Hl7MessageAccessError> {
    match message_type(msg)? {
        MessageType::A04 | MessageType::A03 => Ok(true),
        _ => {
            let movement_end = query(msg, ZBE_3);
            if movement_end.is_some() {
                Ok(true)
            } else {
                Ok(false)
            }
        }
    }
}
