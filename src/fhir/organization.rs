use crate::error::MappingError;
use adt_config::config::Fhir;

use crate::fhir::mapper::parse_fab;
use adt_config::resources::ResourceMap;
use fhir_core::mapping::misc::{EntryRequestType, bundle_entry};
use fhir_core::mapping::orga_mapping::{map_department_dto, map_ward_dto};
use fhir_core::model::orga_dto::{DepartmentDto, WardDto};
use fhir_model::r4b::resources::{BundleEntry, Organization};
use hl7_parser::Message;
use processor_hl7v2::hl7::parser::{PV1_3_1, query};

pub(crate) fn map(
    msg: &Message,
    config: &Fhir,
    resources: &ResourceMap,
) -> Result<Vec<BundleEntry>, MappingError> {
    let mut result = vec![];
    if let Some(department_org) = map_department_org(msg, config, resources)? {
        result.push(bundle_entry(
            department_org,
            EntryRequestType::UpdateAsCreate,
            config,
        )?)
    }
    if let Some(war_org) = map_ward_org(msg, config)? {
        result.push(bundle_entry(
            war_org,
            EntryRequestType::UpdateAsCreate,
            config,
        )?)
    }
    Ok(result)
}

fn map_department_org(
    msg: &Message,
    config: &Fhir,
    resources: &ResourceMap,
) -> Result<Option<Organization>, MappingError> {
    if let Some(fab_ref) = parse_fab(msg) {
        let department = DepartmentDto {
            department_identifier: fab_ref.to_string(),
        };

        Ok(Some(map_department_dto(&department, config, resources)?))
    } else {
        Ok(None)
    }
}

fn map_ward_org(msg: &Message, config: &Fhir) -> Result<Option<Organization>, MappingError> {
    // ward is sometimes empty
    if let Some(ward_name) = query(msg, PV1_3_1) {
        if let Some(fab_ref) = parse_fab(msg) {
            let ward_input = WardDto {
                ward_name: ward_name.to_string(),
                part_of_department: DepartmentDto {
                    department_identifier: fab_ref.to_string(),
                },
            };
            Ok(Some(map_ward_dto(&ward_input, config)?))
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
    use adt_config::test_utils::tests::{get_dummy_resources, get_test_config};
    use hl7_parser::Message;

    #[test]
    fn check_none_results() {
        let input = r#"MSH|^~\&|ORBIS|KH|RECAPP|ORBIS|202111221030||ADT^A01|62293727|P|2.5||123456789|NE|NE||8859/1
EVN|A01|202111221030|202111221029||EIDAMN
PID|1|1499653|1499653||Test^Meinrad^^Graf^von^Dr.^L|Test|202301181003|M|||Test Str.  27^^Bad Test^^57334^D^L||02752/1672^^PH|||M|rk|||||||N||D||||N|
PV1|1|I|^^^^^945400^^^|R^^HL7~01^Normalfall^301||||||N||||||N|||00000000||K|||||||||||||||01||||9||||202211101359|202211101359||||||AIN1|1|102171012|KKH|KKH Allianz|^^Leipzig^^04017^D||||Ersatzkassen^13^^^1&gesetzlich|||||||Mustermann^Max||19470128|Mustergasse 10^^Musterort^^33333^D|||1|||||||201111090942||R||||||||||||M| |||||1234567890^^^^^^^20130331"#;

        let msg = Message::parse_with_lenient_newlines(input, true).unwrap();
        match map_ward_org(&msg, &get_test_config()) {
            Ok(Some(_)) => {
                panic!("bundle should not be created")
            }
            Err(_) => {
                panic!("error is not expected")
            }
            Ok(None) => { // expect None}
            }
        }
        match map_department_org(&msg, &get_test_config(), &get_dummy_resources()) {
            Ok(Some(_)) => {
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
                assert!(actual.name.is_none());
            }

            _ => {
                panic!("expect some result")
            }
        }
        match map_department_org(&msg, &get_test_config(), &get_dummy_resources()) {
            Ok(Some(actual)) => {
                assert!(!actual.identifier.is_empty());
                assert!(!actual.r#type.is_empty());
                assert_eq!(actual.name, Some("Pneumologie".to_string()));
            }

            _ => {
                panic!("expect some result")
            }
        }
    }
}
