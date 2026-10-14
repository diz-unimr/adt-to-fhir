use crate::hl7::parser::{
    MessageType, PV1_2, PV1_3_1, PV1_3_2, PV1_3_3, PV1_3_5, message_type, query,
};
use crate::hl7::{is_ward_valid_icu, parse_department_name};
use crate::hl7_error::Hl7MappingError;
use adt_config::resources::ResourceMap;
use fhir_core::canonical_model::canonical_location::{
    LocationCdm, LocationCdmBuilder, LocationCdmBuilderError, LocationCdmType,
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
fn is_inpatient(msg: &Message) -> Result<bool, Hl7MappingError> {
    Ok(query(msg, PV1_2) == Some("I") && query(msg, PV1_3_5).map(|v| v == "KLINIKUM").is_some())
}

fn create_locations(
    msg: &Message,
    resources: &ResourceMap,
) -> Result<Vec<LocationCdm>, Hl7MappingError> {
    let mut result: Vec<LocationCdm> = vec![];
    if is_inpatient(msg)? {
        let pv1_3_1 = query(msg, PV1_3_1);
        let pv1_3_2 = query(msg, PV1_3_2);
        let pv1_3_3 = query(msg, PV1_3_3);

        if parse_department_name(msg).is_some() {
            match (pv1_3_1, pv1_3_2, pv1_3_3) {
                (Some(_), None, None) => {
                    if let Some(loc) = map_ward_location(msg, resources)? {
                        result.push(loc);
                    }
                }

                (Some(pv1_3_1), Some(pv1_3_2), None) => {
                    if let Some(loc) = map_ward_location(msg, resources)? {
                        result.push(loc);
                    }

                    result.push(map_room_location(pv1_3_1, pv1_3_2)?);
                }

                (Some(pv1_3_1), Some(pv1_3_2), Some(pv1_3_3)) => {
                    if let Some(loc) = map_ward_location(msg, resources)? {
                        result.push(loc);
                    }

                    result.push(map_room_location(pv1_3_1, pv1_3_2)?);

                    result.push(map_bed_location(pv1_3_1, pv1_3_2, pv1_3_3)?);
                }
                (_, _, _) => {}
            }
        }
    }

    Ok(result)
}

pub(crate) fn map_ward_location(
    msg: &Message,
    resources: &ResourceMap,
) -> Result<Option<LocationCdm>, LocationCdmBuilderError> {
    if let (department, Some(ward_id)) = (parse_department_name(msg), query(msg, PV1_3_1)) {
        let is_icu = is_ward_valid_icu(msg, resources);

        let mut location = LocationCdmBuilder::default();
        location.name(ward_id).is_icu(is_icu);
        location.typ(LocationCdmType::Ward);
        if let Some(dep_name) = department {
            location.manging_organization(Some(dep_name.to_string()));
        }

        Ok(Some(location.build()?))
    } else {
        Ok(None)
    }
}

pub(crate) fn map_room_location(
    pv1_3_1: &str,
    pv1_3_2: &str,
) -> Result<LocationCdm, LocationCdmBuilderError> {
    LocationCdmBuilder::default()
        .typ(LocationCdmType::Room)
        .name(format!("{}_{}", pv1_3_1, pv1_3_2))
        .manging_organization(Some(pv1_3_1.to_string()))
        .build()
}

pub(crate) fn map_bed_location(
    pv1_3_1: &str,
    pv1_3_2: &str,
    pv1_3_3: &str,
) -> Result<LocationCdm, LocationCdmBuilderError> {
    LocationCdmBuilder::default()
        .typ(LocationCdmType::Bed)
        .name(format!("{}_{}_{}", pv1_3_1, pv1_3_2, pv1_3_3))
        .manging_organization(Some(format!("{}_{}", pv1_3_1, pv1_3_2)))
        .build()
}

pub(crate) fn map_location(
    typ: &LocationCdmType,
    msg: &Message,
    resources: &ResourceMap,
) -> Result<Option<LocationCdm>, LocationCdmBuilderError> {
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

    let mut location = LocationCdmBuilder::default();
    location
        .typ(typ.clone())
        .name(name)
        .manging_organization(parent)
        .is_movement_finished(true);

    if *typ == LocationCdmType::Ward {
        location.is_icu(is_ward_valid_icu(msg, resources));
    } else {
        location.is_icu(false);
    }

    location.build().map(Some)
}

fn create_locations_new(
    msg: &Message,
    resources: &ResourceMap,
    targets: &[LocationCdmType],
) -> Result<Vec<LocationCdm>, Hl7MappingError> {
    if !is_inpatient(msg)? || parse_department_name(msg).is_none() {
        return Ok(vec![]);
    }

    let locations = targets
        .iter()
        .filter_map(|typ| map_location(typ, msg, resources).transpose())
        .collect::<Result<Vec<_>, _>>()?;

    Ok(locations)
}
