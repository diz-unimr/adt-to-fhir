use crate::config::Fhir;
use crate::error::MappingError;
use crate::fhir::mapper::{
    EntryRequestType, build_usual_identifier, bundle_entry, get_cc_with_one_code, get_meta,
    is_inpatient_location, is_ward_valid_icu, parse_fab, resource_ref,
};
use crate::fhir::resources::ResourceMap;
use crate::hl7::parser::{MessageType, PV1_3_1, PV1_3_2, PV1_3_3, message_type, query};
use anyhow::anyhow;
use fhir_model::r4b::resources::{BundleEntry, EncounterLocation, Location, ResourceType};
use fhir_model::r4b::types::Reference;
use hl7_parser::Message;
use log::{Level, log};

pub(super) fn map(
    msg: &Message,
    config: &Fhir,
    resources: &ResourceMap,
) -> Result<Vec<BundleEntry>, MappingError> {
    let mut r: Vec<BundleEntry> = vec![];
    if let Ok(msg_type) = message_type(msg) {
        match msg_type {
            // location changes only at patient movement and admission
            MessageType::A02 | MessageType::A01 => {
                if let Ok(Some(locations)) = create_locations(msg, config, resources) {
                    for location in locations.iter() {
                        r.push(bundle_entry(
                            location.clone(),
                            EntryRequestType::UpdateAsCreate,
                            config,
                        )?);
                    }
                }
            }

            // department stays the same - we have only a short contact at another location
            MessageType::A04 => {
                if let Some(loc) = map_ward_location(msg, config, resources)? {
                    r.push(bundle_entry(loc, EntryRequestType::UpdateAsCreate, config)?);
                }
            }
            _ => {

                // skip other messages, since they should not add any locations.
                // also delete is not necessary since locations stay in the system,
                // even if a patient movement is revoked.
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
        let pv1_3_1 = query(msg, PV1_3_1);
        let pv1_3_2 = query(msg, PV1_3_2);
        let pv1_3_3 = query(msg, PV1_3_3);

        if parse_fab(msg).is_some() {
            match (pv1_3_1, pv1_3_2, pv1_3_3) {
                (Some(_), None, None) => {
                    if let Some(loc) = map_ward_location(msg, config, resources)? {
                        result.push(loc);
                    }
                }

                (Some(pv1_3_1), Some(pv1_3_2), None) => {
                    if let Some(loc) = map_ward_location(msg, config, resources)? {
                        result.push(loc);
                    }
                    if let Some(loc) = map_room_location(config, pv1_3_1, pv1_3_2)? {
                        result.push(loc);
                    }
                }

                (Some(pv1_3_1), Some(pv1_3_2), Some(pv1_3_3)) => {
                    if let Some(loc) = map_ward_location(msg, config, resources)? {
                        result.push(loc);
                    }
                    if let Some(loc) = map_room_location(config, pv1_3_1, pv1_3_2)? {
                        result.push(loc);
                    }

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
pub(crate) fn map_ward_location(
    msg: &Message,
    config: &Fhir,
    resources: &ResourceMap,
) -> Result<Option<Location>, MappingError> {
    if let (department, Some(ward_id)) = (parse_fab(msg), query(msg, PV1_3_1)) {
        let mut location = Location::builder()
            .meta(get_meta(config)?)
            .physical_type(get_cc_with_one_code(
                "wa".to_string(),
                LOCATION_TYPE_SYSTEM.to_string(),
            )?)
            .identifier(vec![Some(build_usual_identifier(
                vec![ward_id],
                config.location.system_ward.to_string(),
            )?)])
            .build()
            .map_err(MappingError::BuilderError)?;

        if is_ward_valid_icu(msg, resources) {
            location.r#type = vec![Some(get_cc_with_one_code(
                "ICU".to_string(),
                "http://terminology.hl7.org/CodeSystem/v3-RoleCode".to_string(),
            )?)];
        }
        if let Some(dep_id) = department {
            location.managing_organization = Some(resource_ref(
                &ResourceType::Organization,
                dep_id,
                config.organization.ward.system.as_str(),
            )?)
        }
        Ok(Some(location))
    } else {
        Ok(None)
    }
}

pub(crate) fn map_room_location(
    config: &Fhir,
    pv1_3_1: &str,
    pv1_3_2: &str,
) -> Result<Option<Location>, MappingError> {
    match Location::builder()
        .meta(get_meta(config)?)
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
    {
        Ok(location) => Ok(Some(location)),
        Err(e) => {
            log!(
                Level::Warn,
                "Mapping of 'location room' failed with: {:?}",
                e
            );
            Ok(None)
        }
    }
}

pub(crate) fn map_bed_location(
    config: &Fhir,
    pv1_3_1: &str,
    pv1_3_2: &str,
    pv1_3_3: &str,
) -> Result<Location, MappingError> {
    Location::builder()
        .meta(get_meta(config)?)
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
    use crate::hl7::parser::{PV1_3_1, query};
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

        let result = map(&msg, &get_test_config(), &get_dummy_resources()).expect("map failed");

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
ZBE|30674176^ORBIS|202208221309||INSERT
"#,
            pv1_3_value
        );
        let msg = Message::parse_with_lenient_newlines(&input, true).expect("parse hl7 failed");

        let result = map(&msg, &get_test_config(), &get_dummy_resources()).expect("map failed");

        let loca: Location = resource_from(result.first().expect("one element expected"))
            .unwrap_or_else(|_| panic!("location expected - location entry is {}", pv1_3_value));

        if expect_icu_type {
            let type_code = loca
                .r#type
                .first()
                .unwrap_or_else(|| {
                    panic!("location type expected - location entry is {}", pv1_3_value)
                })
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
        let x = loca
            .identifier
            .clone()
            .first()
            .unwrap()
            .clone()
            .unwrap()
            .value
            .as_ref()
            .unwrap()
            .clone();

        // check if identifier value is correct
        assert_eq!(x, query(&msg, PV1_3_1).unwrap());
    }
}
