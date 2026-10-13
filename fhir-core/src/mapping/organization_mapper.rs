use crate::fhir_error::ContentError;
use crate::mapping::misc::{
    EntryRequestType, bundle_entry, get_cc_with_one_code, get_meta, resource_ref,
};
use crate::model::orga_dto::{DepartmentDto, WardDto};
use adt_config::config::Fhir;
use adt_config::resources::ResourceMap;
use fhir_model::BuilderError;
use fhir_model::r4b::codes::IdentifierUse;
use fhir_model::r4b::resources::{BundleEntry, Organization, ResourceType};
use fhir_model::r4b::types::Identifier;

fn map_department_to_fhir(
    department: &DepartmentDto,
    config: &Fhir,
    resource_map: &ResourceMap,
) -> Result<Organization, BuilderError> {
    let mut organization = Organization::builder()
        .meta(get_meta(config)?)
        .identifier(vec![Some(
            Identifier::builder()
                .value(department.department_identifier.clone())
                .system(config.organization.department.system.to_string())
                .r#use(IdentifierUse::Usual)
                .build()?,
        )])
        .r#type(vec![Some(get_cc_with_one_code(
            "dept".to_string(),
            "http://terminology.hl7.org/CodeSystem/organization-type".to_string(),
        )?)])
        .build()?;

    // local department name may differ from official medical department name
    if let Some(department_entry) = resource_map
        .department_map
        .get(&department.department_identifier)
    {
        organization.name = Some(department_entry.abteilungs_bezeichnung.to_string());
    }
    Ok(organization)
}

fn map_ward_to_fhir(ward: &WardDto, config: &Fhir) -> Result<Organization, BuilderError> {
    Organization::builder()
        .meta(get_meta(config)?)
        .part_of(resource_ref(
            &ResourceType::Organization,
            ward.part_of_department.department_identifier.as_str(),
            config.organization.department.system.as_str(),
        )?)
        .identifier(vec![Some(
            Identifier::builder()
                .value(ward.ward_name.clone())
                .system(config.organization.ward.system.to_string())
                .r#use(IdentifierUse::Usual)
                .build()?,
        )])
        .r#type(vec![Some(get_cc_with_one_code(
            "other".to_string(),
            "http://terminology.hl7.org/CodeSystem/organization-type".to_string(),
        )?)])
        .build()
}

pub fn map(
    dep: Option<DepartmentDto>,
    ward: Option<WardDto>,
    config: &Fhir,
    resource_map: &ResourceMap,
) -> Result<Vec<BundleEntry>, ContentError> {
    let mut result = vec![];
    if let Some(dep) = dep {
        result.push(map_dep(&dep, config, resource_map)?);
    }
    if let Some(wd) = ward {
        result.push(map_ward(&wd, config)?);
    }

    Ok(result)
}
fn map_dep(
    dep: &DepartmentDto,
    config: &Fhir,
    resource_map: &ResourceMap,
) -> Result<BundleEntry, ContentError> {
    bundle_entry(
        map_department_to_fhir(dep, config, resource_map)?,
        EntryRequestType::UpdateAsCreate,
        config,
    )
}
fn map_ward(ward: &WardDto, config: &Fhir) -> Result<BundleEntry, ContentError> {
    bundle_entry(
        map_ward_to_fhir(ward, config)?,
        EntryRequestType::UpdateAsCreate,
        config,
    )
}
