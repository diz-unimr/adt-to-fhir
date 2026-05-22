use crate::config::Fhir;
use crate::error::MappingError;
use fhir_model::r4b::codes::IdentifierUse;

use crate::fhir::mapper::{
    EntryRequestType, bundle_entry, get_cc_with_one_code, parse_fab, resource_ref,
};
use crate::hl7::parser::{PV1_3_1, query};
use fhir_model::r4b::resources::{BundleEntry, Organization, ResourceType};
use fhir_model::r4b::types::Identifier;
use hl7_parser::Message;

pub(crate) fn map(msg: &Message, config: &Fhir) -> Result<Vec<BundleEntry>, MappingError> {
    let mut result = vec![];
    if let Some(department_org) = map_department_org(msg, config)? {
        result.push(bundle_entry(
            department_org,
            EntryRequestType::UpdateAsCreate,
        )?)
    }
    if let Some(war_org) = map_ward_org(msg, config)? {
        result.push(bundle_entry(war_org, EntryRequestType::UpdateAsCreate)?)
    }
    Ok(result)
}

fn map_department_org(msg: &Message, config: &Fhir) -> Result<Option<Organization>, MappingError> {
    if let Some(fab_ref) = parse_fab(msg)? {
        Ok(Some(
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
        ))
    } else {
        Ok(None)
    }
}

fn map_ward_org(msg: &Message, config: &Fhir) -> Result<Option<Organization>, MappingError> {
    // ward is sometimes empty
    if let Some(ward_name) = query(msg, PV1_3_1) {
        if let Some(fab_ref) = parse_fab(msg)? {
            Ok(Some(
                Organization::builder()
                    .part_of(resource_ref(
                        &ResourceType::Organization,
                        fab_ref,
                        config.organization.department.system.as_str(),
                    )?)
                    .identifier(vec![Some(
                        Identifier::builder()
                            .value(ward_name.to_string())
                            .system(config.organization.ward.system.to_string())
                            .r#use(IdentifierUse::Usual)
                            .build()?,
                    )])
                    .r#type(vec![Some(get_cc_with_one_code(
                        "other".to_string(),
                        "http://terminology.hl7.org/CodeSystem/organization-type".to_string(),
                    )?)])
                    .build()?,
            ))
        } else {
            Ok(None)
        }
    } else {
        Ok(None)
    }
}
#[cfg(test)]
mod tests {
    use crate::fhir::organization::{map_department_org, map_ward_org};
    use crate::test_utils::tests::get_test_config;
    use hl7_parser::Message;

    #[test]
    fn check_none_results() {
        let input = r#"MSH|^~\&|ORBIS|KH|RECAPP|ORBIS|202111221030||ADT^A01|62293727|P|2.5||123456789|NE|NE||8859/1
EVN|A01|202111221030|202111221029||EIDAMN
PID|1|1499653|1499653||Test^Meinrad^^Graf^von^Dr.^L|Test|202301181003|M|||Test Str.  27^^Bad Test^^57334^D^L||02752/1672^^PH|||M|rk|||||||N||D||||N|
PV1|1|I|^^^^^945400^^^|R^^HL7~01^Normalfall^301||||||N||||||N|||00000000||K|||||||||||||||01||||9||||202211101359|202211101359||||||AIN1|1|102171012|KKH|KKH Allianz|^^Leipzig^^04017^D||||Ersatzkassen^13^^^1&gesetzlich|||||||Mustermann^Max||19470128|Mustergasse 10^^Musterort^^33333^D|||1|||||||201111090942||R||||||||||||M| |||||1234567890^^^^^^^20130331"#;

        let msg = Message::parse_with_lenient_newlines(input, true).unwrap();
        match map_ward_org(&msg, &get_test_config()) {
            Ok(Some(actual)) => {
                panic!("bundle should not be created")
            }
            Err(_) => {
                panic!("error is not expected")
            }
            Ok(None) => { // expect None}
            }
        }
        match map_department_org(&msg, &get_test_config()) {
            Ok(Some(actual)) => {
                panic!("expect None result")
            }
            Err(_) => {
                panic!("error is not expected")
            }
            Ok(None) => { //ok: expect None
            }
        }
    }

    #[test]
    fn test_map_org() {
        let input = r#"MSH|^~\&|ORBIS|KH|RECAPP|ORBIS|202111221030||ADT^A01|62293727|P|2.5||123456789|NE|NE||8859/1
EVN|A01|202111221030|202111221029||EIDAMN
PID|1|1499653|1499653||Test^Meinrad^^Graf^von^Dr.^L|Test|202301181003|M|||Test Str.  27^^Bad Test^^57334^D^L||02752/1672^^PH|||M|rk|||||||N||D||||N|
PV1|1|I|POLPOLAMB^^^POL^POLPOL^945400^^^|R^^HL7~01^Normalfall^301||||||N||||||N|||00000000||K|||||||||||||||01||||9||||202211101359|202211101359||||||AIN1|1|102171012|KKH|KKH Allianz|^^Leipzig^^04017^D||||Ersatzkassen^13^^^1&gesetzlich|||||||Mustermann^Max||19470128|Mustergasse 10^^Musterort^^33333^D|||1|||||||201111090942||R||||||||||||M| |||||1234567890^^^^^^^20130331"#;

        let msg = Message::parse_with_lenient_newlines(input, true).unwrap();

        match map_ward_org(&msg, &get_test_config()) {
            Ok(Some(actual)) => {
                assert!(!actual.identifier.is_empty());
                assert!(!actual.r#type.is_empty());
            }

            _ => {
                panic!("expect some result")
            }
        }
        match map_department_org(&msg, &get_test_config()) {
            Ok(Some(actual)) => {
                assert!(!actual.identifier.is_empty());
                assert!(!actual.r#type.is_empty());
            }

            _ => {
                panic!("expect some result")
            }
        }
    }
}
