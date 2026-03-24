use crate::config::Fhir;
use crate::error::MappingError;
use crate::fhir::mapper::{
    EntryRequestType, bundle_entry, create_locations, map_ward_location, parse_fab,
};

use crate::hl7::parser::{MessageType, message_type};

use fhir_model::r4b::resources::BundleEntry;

use crate::fhir::resources::ResourceMap;
use hl7_parser::Message;

pub(super) fn map(
    msg: &Message,
    config: Fhir,
    resources: &ResourceMap,
) -> Result<Vec<BundleEntry>, MappingError> {
    let mut r: Vec<BundleEntry> = vec![];
    if let Ok(msg_type) = message_type(msg) {
        match msg_type {
            MessageType::A02 | MessageType::A01 => {
                if let Ok(Some(locations)) = create_locations(msg, &config, resources) {
                    for location in locations.iter() {
                        r.push(bundle_entry(
                            location.clone(),
                            EntryRequestType::ConditionalCreate,
                        )?);
                    }
                }
            }

            MessageType::A04 => {
                if let Some(department) = parse_fab(msg)? {
                    r.push(bundle_entry(
                        map_ward_location(msg, department, &config, resources)?,
                        EntryRequestType::ConditionalCreate,
                    )?);
                }
            }
            _ => { // skip other messages, since they should not add any locations.
            }
        }
    }
    Ok(r)
}
#[cfg(test)]
mod tests {
    use crate::fhir::location::map;
    use crate::test_utils::tests::{get_dummy_resources, get_test_config};
    use hl7_parser::Message;
    use rstest::rstest;

    #[rstest]
    #[case("", 0)]
    #[case("WARD_1^^^POL^POLPOL^945400^^^", 1)]
    #[case("WARD_1^room_1^^KJM^KLINIKUM^123445", 2)]
    #[case("WARD_1^room_1^bet_1^KJM^KLINIKUM^123445", 3)]
    fn test_map(#[case] pv1_3_value: String, #[case] expected_number_locations: usize) {
        let input = format!(
            r#"MSH|^~\&|ORBIS|KH|RECAPP|ORBIS|202111221030||ADT^A01|62293727|P|2.3|||||D||DE
EVN|A01|202111221030|202111221029||EIDAMN
PID|1|1499653|1499653||Test^Meinrad^^Graf^von^Dr.^L|Test|202301181003|M|||Test Str.  27^^Bad Test^^57334^D^L||02752/1672^^PH|||M|rk|||||||N||D||||N|
NK1|1|Fr. Test|14^Ehefrau||s.Pat.||||||||||U|^YYYYMMDDHHMMSS|||||||||||||||||^^^ORBIS^PN~^^^ORBIS^PI~^^^ORBIS^PT
PV1|1|I|{}|R^^HL7~01^Normalfall^301||||||N||||||N|||00000000||K|||||||||||||||01||||9||||202211101359|202211101359||||||AIN1|1|102171012|KKH|KKH Allianz|^^Leipzig^^04017^D||||Ersatzkassen^13^^^1&gesetzlich|||||||Mustermann^Max||19470128|Mustergasse 10^^Musterort^^33333^D|||1|||||||201111090942||R||||||||||||M| |||||1234567890^^^^^^^20130331
"#,
            pv1_3_value
        );
        let msg = Message::parse_with_lenient_newlines(&input, true).expect("parse hl7 failed");

        let result = map(&msg, get_test_config(), &get_dummy_resources()).expect("map failed");

        assert_eq!(result.len(), expected_number_locations);
    }
}
