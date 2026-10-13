use crate::hl7::parse_department_name;
use crate::hl7::parser::{PV1_3_1, query};
use crate::hl7_error::Hl7MappingError;
use fhir_core::model::orga_dto::{DepartmentDto, WardDto};
use hl7_parser::Message;

pub fn map(msg: &Message) -> Result<(Option<DepartmentDto>, Option<WardDto>), Hl7MappingError> {
    let department_org = map_department_org(msg)?;
    let war_org = map_ward_org(msg)?;
    Ok((department_org, war_org))
}

fn map_department_org(msg: &Message) -> Result<Option<DepartmentDto>, Hl7MappingError> {
    if let Some(fab_ref) = parse_department_name(msg) {
        let department = DepartmentDto {
            department_identifier: fab_ref.to_string(),
        };

        Ok(Some(department))
    } else {
        Ok(None)
    }
}

fn map_ward_org(msg: &Message) -> Result<Option<WardDto>, Hl7MappingError> {
    // ward is sometimes empty
    if let Some(ward_name) = query(msg, PV1_3_1) {
        if let Some(fab_ref) = parse_department_name(msg) {
            let ward_input = WardDto {
                ward_name: ward_name.to_string(),
                part_of_department: DepartmentDto {
                    department_identifier: fab_ref.to_string(),
                },
            };
            Ok(Some(ward_input))
        } else {
            Ok(None)
        }
    } else {
        Ok(None)
    }
}
#[cfg(test)]
mod tests {
    use crate::hl7_to_organization;
    use hl7_parser::Message;

    #[test]
    fn check_none_results() {
        let input = r#"MSH|^~\&|ORBIS|KH|RECAPP|ORBIS|202111221030||ADT^A01|62293727|P|2.5||123456789|NE|NE||8859/1
EVN|A01|202111221030|202111221029||EIDAMN
PID|1|1499653|1499653||Test^Meinrad^^Graf^von^Dr.^L|Test|202301181003|M|||Test Str.  27^^Bad Test^^57334^D^L||02752/1672^^PH|||M|rk|||||||N||D||||N|
PV1|1|I|^^^^^945400^^^|R^^HL7~01^Normalfall^301||||||N||||||N|||00000000||K|||||||||||||||01||||9||||202211101359|202211101359||||||AIN1|1|102171012|KKH|KKH Allianz|^^Leipzig^^04017^D||||Ersatzkassen^13^^^1&gesetzlich|||||||Mustermann^Max||19470128|Mustergasse 10^^Musterort^^33333^D|||1|||||||201111090942||R||||||||||||M| |||||1234567890^^^^^^^20130331"#;

        let msg = Message::parse_with_lenient_newlines(input, true).unwrap();

        assert_eq!(hl7_to_organization::map(&msg).unwrap(), (None, None));
    }
}
