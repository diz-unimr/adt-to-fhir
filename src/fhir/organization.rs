use crate::config::Fhir;
use crate::error::MappingError;
use fhir_model::r4b::codes::IdentifierUse;

use crate::fhir::mapper::{EntryRequestType, bundle_entry, get_cc_with_one_code, parse_fab};
use crate::hl7::parser::{PV1_WARD_NAME, query};
use fhir_model::r4b::resources::{BundleEntry, Organization};
use fhir_model::r4b::types::Identifier;
use hl7_parser::Message;

pub(crate) fn map(msg: &Message, config: &Fhir) -> Result<Vec<BundleEntry>, MappingError> {
    let mut result = vec![];
    if let Some(department_org) = map_department_org(msg, config)? {
        result.push(department_org)
    }
    if let Some(war_org) = map_ward_org(msg, config)? {
        result.push(war_org)
    }
    Ok(result)
}

fn map_department_org(msg: &Message, config: &Fhir) -> Result<Option<BundleEntry>, MappingError> {
    if let Some(fab_ref) = parse_fab(msg)? {
        Ok(Some(bundle_entry(
            Organization::builder()
                .identifier(vec![Some(
                    Identifier::builder()
                        .value(fab_ref.to_string())
                        .system(config.organization.department.system.to_string())
                        .r#use(IdentifierUse::Usual)
                        .build()?,
                )])
                .r#type(vec![Some(get_cc_with_one_code(
                    "dept".to_string(),
                    "http://terminology.hl7.org/CodeSystem/organization-type".to_string(),
                )?)])
                .build()?,
            EntryRequestType::UpdateAsCreate,
        )?))
    } else {
        Ok(None)
    }
}

fn map_ward_org(msg: &Message, config: &Fhir) -> Result<Option<BundleEntry>, MappingError> {
    // ward is often empty
    if let Some(ward_name) = query(msg, PV1_WARD_NAME)
        && !ward_name.is_empty()
    {
        Ok(Some(bundle_entry(
            Organization::builder()
                .identifier(vec![Some(
                    Identifier::builder()
                        .value(ward_name.to_string())
                        .system(config.organization.ward.system.to_string())
                        .r#use(IdentifierUse::Usual)
                        .build()?,
                )])
                .build()?,
            EntryRequestType::UpdateAsCreate,
        )?))
    } else {
        Ok(None)
    }
}
