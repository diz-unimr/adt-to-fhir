use crate::mapping::misc::{get_cc_with_one_code, get_meta, resource_ref};
use crate::model::orga_dto::{DepartmentDto, WardDto};
use adt_config::config::Fhir;
use adt_config::resources::ResourceMap;
use fhir_model::BuilderError;
use fhir_model::r4b::codes::IdentifierUse;
use fhir_model::r4b::resources::{Organization, ResourceType};
use fhir_model::r4b::types::Identifier;

pub fn map_department_dto(
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

pub fn map_ward_dto(ward: &WardDto, config: &Fhir) -> Result<Organization, BuilderError> {
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
