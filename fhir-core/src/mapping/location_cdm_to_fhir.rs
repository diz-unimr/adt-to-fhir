use crate::canonical_model::canonical_location::{LocationCdm, LocationCdmType};
use crate::fhir_error::FhirMappingError;
use crate::mapping::misc::{
    EntryRequestType, bundle_entry, get_cc_with_one_code, get_meta, resource_ref,
};
use adt_config::config::Fhir;

use fhir_model::r4b::codes::IdentifierUse;
use fhir_model::r4b::resources::{BundleEntry, Location, ResourceType};
use fhir_model::r4b::types::Identifier;

pub fn map(input: &Vec<LocationCdm>, config: &Fhir) -> Result<Vec<BundleEntry>, FhirMappingError> {
    let mapped_locations = input
        .into_iter()
        .map(|loc| map_to_fhir(loc, config))
        .collect::<Result<Vec<_>, _>>()?;

    let bundle_entries = mapped_locations
        .into_iter()
        .map(|res| bundle_entry(res, EntryRequestType::UpdateAsCreate, config))
        .collect::<Result<Vec<_>, _>>()?;

    Ok(bundle_entries)
}
static LOCATION_TYPE_SYSTEM: &str = "http://terminology.hl7.org/CodeSystem/location-physical-type";
pub fn map_to_fhir(input: &LocationCdm, config: &Fhir) -> Result<Location, FhirMappingError> {
    match input.typ {
        LocationCdmType::Ward => {
            let mut location = Location::builder()
                .meta(get_meta(config)?)
                .physical_type(get_cc_with_one_code(
                    "wa".to_string(),
                    LOCATION_TYPE_SYSTEM.to_string(),
                )?)
                .identifier(vec![Some(
                    Identifier::builder()
                        .r#use(IdentifierUse::Usual)
                        .system(config.location.system_ward.to_string())
                        .value(input.name.to_string())
                        .build()?,
                )])
                .build()?;
            if input.is_icu {
                location.r#type = vec![Some(get_cc_with_one_code(
                    "ICU".to_string(),
                    "http://terminology.hl7.org/CodeSystem/v3-RoleCode".to_string(),
                )?)]
            }
            if let Some(dep_id) = &input.manging_organization {
                location.managing_organization = Some(resource_ref(
                    &ResourceType::Organization,
                    dep_id.as_str(),
                    config.organization.ward.system.as_str(),
                )?)
            }

            Ok(location)
        }
        LocationCdmType::Room => Ok(Location::builder()
            .meta(get_meta(config)?)
            .physical_type(get_cc_with_one_code(
                "ro".to_string(),
                LOCATION_TYPE_SYSTEM.to_string(),
            )?)
            .identifier(vec![Some(
                Identifier::builder()
                    .r#use(IdentifierUse::Usual)
                    .system(config.location.system_room.to_string())
                    .value(input.name.to_string())
                    .build()?,
            )])
            .build()?),
        LocationCdmType::Bed => Ok(Location::builder()
            .meta(get_meta(config)?)
            .physical_type(get_cc_with_one_code(
                "bd".to_string(),
                LOCATION_TYPE_SYSTEM.to_string(),
            )?)
            .identifier(vec![Some(
                Identifier::builder()
                    .r#use(IdentifierUse::Usual)
                    .system(config.location.system_bed.to_string())
                    .value(input.name.to_string())
                    .build()?,
            )])
            .build()?),
    }
}
