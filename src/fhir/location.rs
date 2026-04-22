use crate::config::Fhir;
use crate::error::MappingError;
use crate::fhir::mapper::{
    EntryRequestType, build_usual_identifier, bundle_entry, get_cc_with_one_code, get_meta,
    is_inpatient_location, parse_fab,
};
use anyhow::anyhow;

use crate::hl7::parser::{MessageType, message_type, query};

use crate::fhir::resources::ResourceMap;
use fhir_model::r4b::resources::{BundleEntry, EncounterLocation, Location};
use fhir_model::r4b::types::{CodeableConcept, Reference};
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

static LOCATION_TYPE_SYSTEM: &str = "http://terminology.hl7.org/CodeSystem/location-physical-type";

pub(crate) fn create_locations(
    msg: &Message,
    config: &Fhir,
    resources: &ResourceMap,
) -> Result<Option<Vec<Location>>, MappingError> {
    let mut result: Vec<Location> = vec![];
    if is_inpatient_location(msg)? {
        let pv1_3_1 = query(msg, "PV1.3.1");
        let pv1_3_2 = query(msg, "PV1.3.2");
        let pv1_3_3 = query(msg, "PV1.3.3");

        if let Some(department) = parse_fab(msg)? {
            match (pv1_3_1, pv1_3_2, pv1_3_3) {
                (Some(_), None, None) => {
                    result.push(map_ward_location(msg, department, config, resources)?)
                }
                (Some(pv1_3_1), Some(pv1_3_2), None) => {
                    result.push(map_ward_location(msg, department, config, resources)?);
                    result.push(map_room_location(config, pv1_3_1, pv1_3_2)?)
                }
                (Some(pv1_3_1), Some(pv1_3_2), Some(pv1_3_3)) => {
                    result.push(map_ward_location(msg, department, config, resources)?);
                    result.push(map_room_location(config, pv1_3_1, pv1_3_2)?);
                    result.push(map_bed_location(config, pv1_3_1, pv1_3_2, pv1_3_3)?);
                }
                (_, _, _) => {}
            }
        }
    }
    if result.is_empty() {
        return Ok(None);
    }

    Ok(Some(result))
}

fn map_location_type_icu(
    pv1_3_1_value: &str,
    resources: &ResourceMap,
) -> Result<Option<Vec<Option<CodeableConcept>>>, MappingError> {
    if let Some(ward_entry) = resources.ward_map.get(pv1_3_1_value)
        && ward_entry.is_icu
    {
        Ok(Some(vec![Some(get_cc_with_one_code(
            "ICU".to_string(),
            "http://terminology.hl7.org/CodeSystem/v3-RoleCode".to_string(),
        )?)]))
    } else {
        Ok(None)
    }
}

pub(crate) fn map_ward_location(
    msg: &Message,
    department: &str,
    config: &Fhir,
    resources: &ResourceMap,
) -> Result<Location, MappingError> {
    let mut location = Location::builder()
        .meta(get_meta()?)
        .physical_type(get_cc_with_one_code(
            "wa".to_string(),
            config.location.system_ward.to_string(),
        )?)
        .identifier(vec![Some(build_usual_identifier(
            vec![department],
            config.location.system_ward.to_string(),
        )?)])
        .build()
        .map_err(MappingError::BuilderError)?;
    if let Some(icu_coding) = query(msg, "PV1.3.1")
        .and_then(|field_value| map_location_type_icu(field_value, resources).transpose())
    {
        location.r#type = icu_coding?;
    }
    Ok(location)
}

pub(crate) fn map_room_location(
    config: &Fhir,
    pv1_3_1: &str,
    pv1_3_2: &str,
) -> Result<Location, MappingError> {
    Location::builder()
        .meta(get_meta()?)
        .physical_type(get_cc_with_one_code(
            "ro".to_string(),
            LOCATION_TYPE_SYSTEM.to_string(),
        )?)
        .identifier(vec![Some(build_usual_identifier(
            vec![pv1_3_1, pv1_3_2],
            config.location.system_room.clone(),
        )?)])
        .build()
        .map_err(MappingError::BuilderError)
}

pub(crate) fn map_bed_location(
    config: &Fhir,
    pv1_3_1: &str,
    pv1_3_2: &str,
    pv1_3_3: &str,
) -> Result<Location, MappingError> {
    Location::builder()
        .meta(get_meta()?)
        .physical_type(get_cc_with_one_code(
            "bd".to_string(),
            LOCATION_TYPE_SYSTEM.to_string(),
        )?)
        .identifier(vec![Some(build_usual_identifier(
            vec![pv1_3_1, pv1_3_2, pv1_3_3],
            config.location.system_bed.to_string(),
        )?)])
        .build()
        .map_err(MappingError::BuilderError)
}

pub fn to_encounter_location(location: Location) -> Result<EncounterLocation, MappingError> {
    if let Some(identifier) = location
        .identifier
        .first()
        .ok_or(MappingError::Other(anyhow!("failed to access identifier")))?
        .clone()
    {
        return Ok(EncounterLocation::builder()
            .physical_type(
                location
                    .physical_type
                    .clone()
                    .ok_or(MappingError::Other(anyhow!(
                        "physical type ist missing".to_string()
                    )))?,
            )
            .location(Reference::builder().identifier(identifier).build()?)
            .build()?);
    };
    Err(MappingError::Other(anyhow!("failed to access identifier")))
}

#[cfg(test)]
mod tests {
    use crate::fhir::location::map;
    use crate::test_utils::tests::{get_dummy_resources, get_test_config, resource_from};
    use fhir_model::r4b::resources::Location;
    use hl7_parser::Message;
    use rstest::rstest;

    #[rstest]
    #[case("", "A01", 0)]
    #[case("WARD_1^^^POL^POLPOL^945400^^^", "A01", 1)]
    #[case("WARD_1^room_1^^KJM^KLINIKUM^123445", "A01", 2)]
    #[case("WARD_1^room_1^bet_1^KJM^KLINIKUM^123445", "A01", 3)]
    #[case("WARD_1^room_1^bet_1^KJM^KLINIKUM^123445", "A03", 0)]
    #[case("WARD_1^^^POL^POLPOL^945400^^^", "A02", 1)]
    #[case("WARD_1^room_1^^KJM^KLINIKUM^123445", "A02", 2)]
    #[case("WARD_1^room_1^bet_1^KJM^KLINIKUM^123445", "A02", 3)]
    #[case("WARD_1^room_1^bet_1^KJM^KLINIKUM^123445", "A04", 1)]
    fn test_map_location(
        #[case] pv1_3_value: String,
        #[case] adt_msg_typ: String,
        #[case] expected_number_locations: usize,
    ) {
        let input = format!(
            r#"MSH|^~\&|ORBIS|KH|RECAPP|ORBIS|202111221030||ADT^{}|62293727|P|2.3|||||D||DE
EVN|{}|202111221030|202111221029||EIDAMN
PID|1|1499653|1499653||Test^Meinrad^^Graf^von^Dr.^L|Test|202301181003|M|||Test Str.  27^^Bad Test^^57334^D^L||02752/1672^^PH|||M|rk|||||||N||D||||N|
NK1|1|Fr. Test|14^Ehefrau||s.Pat.||||||||||U|^YYYYMMDDHHMMSS|||||||||||||||||^^^ORBIS^PN~^^^ORBIS^PI~^^^ORBIS^PT
PV1|1|I|{}|R^^HL7~01^Normalfall^301||||||N||||||N|||00000000||K|||||||||||||||01||||9||||202211101359|202211101359||||||AIN1|1|102171012|KKH|KKH Allianz|^^Leipzig^^04017^D||||Ersatzkassen^13^^^1&gesetzlich|||||||Mustermann^Max||19470128|Mustergasse 10^^Musterort^^33333^D|||1|||||||201111090942||R||||||||||||M| |||||1234567890^^^^^^^20130331
"#,
            adt_msg_typ, adt_msg_typ, pv1_3_value
        );
        let msg = Message::parse_with_lenient_newlines(&input, true).expect("parse hl7 failed");

        let result = map(&msg, get_test_config(), &get_dummy_resources()).expect("map failed");

        assert_eq!(result.len(), expected_number_locations);

        match adt_msg_typ.as_str() {
            "A01" => {
                for i in 0..expected_number_locations {
                    let loca: Location =
                        resource_from(result.get(i).expect("one element expected"))
                            .expect("location expected");
                    let x = loca
                        .physical_type
                        .clone()
                        .expect("codeable concept should be there")
                        .coding
                        .first()
                        .expect("one element expected")
                        .clone()
                        .expect("code should be there");

                    match i {
                        0 => {
                            assert_eq!(
                                "wa",
                                x.code.as_ref().unwrap().as_str(),
                                "first location is a ward location"
                            )
                        }
                        1 => {
                            assert_eq!(
                                "ro",
                                x.code.as_ref().unwrap().as_str(),
                                "second location is a room location"
                            )
                        }
                        2 => {
                            assert_eq!(
                                "bd",
                                x.code.as_ref().unwrap().as_str(),
                                "third location is a bed location"
                            )
                        }
                        _ => {}
                    }
                }
            }
            "A04" => {
                let loca: Location = resource_from(result.first().expect("one element expected"))
                    .expect("location expected");
                let x = loca
                    .physical_type
                    .clone()
                    .expect("codeable concept should be there")
                    .coding
                    .first()
                    .expect("one element expected")
                    .clone()
                    .expect("code should be there");
                assert_eq!("wa", x.code.as_ref().unwrap().as_str());
            }
            _ => {}
        }
    }

    #[rstest]
    #[case("INTERDIST121^^^POL^KLINIKUM^123445", false)]
    #[case("INTERDIST121^room_1^bet_1^KJM^KLINIKUM^123445", false)]
    #[case("ANA^room_1^bet_1^KJM^KLINIKUM^123445", true)]
    fn test_map_location_type(#[case] pv1_3_value: String, #[case] expect_icu_type: bool) {
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

        let loca: Location = resource_from(result.first().expect("one element expected"))
            .expect("location expected");

        if expect_icu_type {
            let type_code = loca
                .r#type
                .first()
                .expect("location type is expected")
                .clone()
                .expect("codable concept expected")
                .coding
                .first()
                .expect("one code element expected")
                .clone();
            assert!(type_code.is_some());
            assert_eq!("ICU", type_code.unwrap().code.as_ref().unwrap());
        } else {
            assert!(loca.r#type.is_empty())
        }
    }
}
