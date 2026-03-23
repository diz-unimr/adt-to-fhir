use crate::config::Fhir;
use crate::error::MappingError;
use crate::fhir::mapper::{
    EntryRequestType, build_usual_identifier, bundle_entry, get_cc_with_one_code,
    is_inpatient_location, parse_fab,
};
use crate::fhir::resources::ResourceMap;
use crate::hl7::parser::{MessageType, message_type, parse_repeating_field_component_value};
use fhir_model::r4b::resources::{BundleEntry, Location};
use fhir_model::r4b::types::{CodeableConcept, Meta};
use hl7_parser::Message;

static LOCATION_TYPE_SYSTEM: &str = "http://terminology.hl7.org/CodeSystem/location-physical-type";

fn create_department_location(
    msg: &Message,
    config: &Fhir,
    resources: &ResourceMap,
) -> Result<Option<BundleEntry>, MappingError> {
    if let Some(department) = parse_fab(msg)? {
        // department location should be always available
        let mut ward_location = Location::builder()
            .meta(get_meta()?)
            .physical_type(get_cc_with_one_code(
                "wa".to_string(),
                config.location.system_ward.to_string(),
            )?)
            .identifier(vec![Some(build_usual_identifier(
                vec![department],
                config.location.system_ward.to_string(),
            )?)])
            .build()?;

        if let Some(icu_type) = is_location_icu(msg, resources)? {
            ward_location.r#type = icu_type;
        }

        return Ok(Some(bundle_entry(
            ward_location,
            EntryRequestType::UpdateAsCreate,
        )?));
    }
    Ok(None)
}

fn is_location_icu(
    msg: &Message,
    resources: &ResourceMap,
) -> Result<Option<Vec<Option<CodeableConcept>>>, MappingError> {
    // todo
    // if true return: vec![get_cc_with_one_code("ICU".to_string(),"http://terminology.hl7.org/CodeSystem/v3-RoleCode".to_string())];
    Ok(None)
}

fn create_room_location(
    msg: &Message,
    config: &Fhir,
    resources: &ResourceMap,
) -> Result<Option<BundleEntry>, MappingError> {
    let pv1_3_1 = parse_repeating_field_component_value(msg, "PV1", 3, 1)?;
    let pv1_3_2 = parse_repeating_field_component_value(msg, "PV1", 3, 2)?;

    if let (Some(pv1_3_1), Some(pv1_3_2)) = (pv1_3_1, pv1_3_2) {
        let room_location = Location::builder()
            .meta(get_meta()?)
            .physical_type(get_cc_with_one_code(
                "ro".to_string(),
                LOCATION_TYPE_SYSTEM.to_string(),
            )?)
            .identifier(vec![Some(build_usual_identifier(
                vec![pv1_3_1, pv1_3_2],
                config.location.system_room.to_string(),
            )?)])
            .build()?;
        return Ok(Some(bundle_entry(
            room_location,
            EntryRequestType::UpdateAsCreate,
        )?));
    }
    Ok(None)
}

fn get_meta() -> Result<Meta, MappingError> {
    Ok(Meta::builder().source("#orbis".to_string()).build()?)
}

fn create_bed_location(msg: &Message, config: &Fhir) -> Result<Option<BundleEntry>, MappingError> {
    let pv1_3_1 = parse_repeating_field_component_value(msg, "PV1", 3, 1)?;
    let pv1_3_2 = parse_repeating_field_component_value(msg, "PV1", 3, 2)?;
    let pv1_3_3 = parse_repeating_field_component_value(msg, "PV1", 3, 3)?;

    if let (Some(pv1_3_1), Some(pv1_3_2), Some(pv1_3_3)) = (pv1_3_1, pv1_3_2, pv1_3_3) {
        let room_location = Location::builder()
            .meta(get_meta()?)
            .physical_type(get_cc_with_one_code(
                "ro".to_string(),
                LOCATION_TYPE_SYSTEM.to_string(),
            )?)
            .identifier(vec![Some(build_usual_identifier(
                vec![pv1_3_1, pv1_3_2, pv1_3_3],
                config.location.system_bed.to_string(),
            )?)])
            .build()?;
        return Ok(Some(bundle_entry(
            room_location,
            EntryRequestType::UpdateAsCreate,
        )?));
    }
    Ok(None)
}

pub(super) fn map(
    msg: &Message,
    config: Fhir,
    resources: &ResourceMap,
) -> Result<Vec<BundleEntry>, MappingError> {
    let mut r: Vec<BundleEntry> = vec![];
    if let Ok(msg_type) = message_type(msg) {
        match msg_type {
            MessageType::A02 | MessageType::A01 => {
                if let Some(entry) = create_department_location(msg, &config, resources)? {
                    r.push(entry);
                }
                if is_inpatient_location(msg)? {
                    if let Some(entry) = create_bed_location(msg, &config)? {
                        r.push(entry);
                    }
                    if let Some(entry) = create_room_location(msg, &config, resources)? {
                        r.push(entry);
                    }
                }
            }
            MessageType::A04 => {
                if let Some(entry) = create_department_location(msg, &config, resources)? {
                    r.push(entry);
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
    use crate::fhir::test_utils::tests::{get_dummy_resources, get_test_config};
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
