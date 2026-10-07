use crate::error::MappingError;
use crate::fhir::{encounter, location, observation, organization};
use anyhow::Result;
use anyhow::anyhow;
use fhir_model::r4b::codes::BundleType;
use fhir_model::r4b::resources::{
    Bundle, BundleEntry,
    ResourceType,
};
use fhir_model::r4b::types::{CodeableConcept, Coding, Identifier, Meta, Reference};
use processor_hl7v2::hl7::parser::{
    PID_2, PV1_2, PV1_3_1, PV1_3_4, PV1_3_5, get_message_key, query,
};

use adt_config::config::Fhir;
use adt_config::resources::ResourceMap;
use fhir_core::mapping::misc::resource_ref;
use fhir_model::Instant;
use fhir_model::time::OffsetDateTime;
use hl7_parser::Message;
use log::{Level, log};
use processor_hl7v2::hl7_to_patient_dto;

pub(crate) struct FhirMapper {
    pub(crate) config: Fhir,
    pub(crate) resources: ResourceMap,
}

impl FhirMapper {
    pub(crate) fn new(config: Fhir) -> Result<Self, anyhow::Error> {
        Ok(FhirMapper {
            config,
            resources: ResourceMap::new()?,
        })
    }

    pub(crate) fn map(&self, msg: &str) -> Result<Option<String>, MappingError> {
        // deserialize

        let v2_msg = Message::parse_with_lenient_newlines(msg, true)?;

        // map hl7 message
        let resources = self.map_resources(&v2_msg)?;

        if resources.is_empty() {
            return Ok(None);
        }

        let result = Bundle::builder()
            .r#type(BundleType::Transaction)
            .entry(resources)
            .identifier(
                Identifier::builder()
                    .value(get_message_key(&v2_msg)?.to_string())
                    .system(self.config.bundle_identifier_system.to_string())
                    .build()?,
            )
            .meta(
                Meta::builder()
                    .last_updated(Instant(OffsetDateTime::now_utc()))
                    .build()?,
            )
            .build()?;

        // serialize
        let result = serde_json::to_string(&result).expect("failed to serialize output bundle");

        Ok(Some(result))
    }

    fn map_resources(&self, v2_msg: &Message) -> Result<Vec<Option<BundleEntry>>> {
        if query(v2_msg, PV1_2).is_some_and(|f| f == "H") {
            log!(
                Level::Info,
                "Skipping message id '{}' since it targets patients companion.",
                get_message_key(v2_msg)?
            );

            return Ok(vec![]);
        }

        if let Some(pat_raw) = hl7_to_patient_dto::map(v2_msg)? {
            if let Some(pat_entry) =
                fhir_core::mapping::patient_mapper::map(&pat_raw, &self.config)?
            {
                let p = vec![pat_entry];
                let e = encounter::map(v2_msg, &self.config, &self.resources)?;
                let l = location::map(v2_msg, &self.config, &self.resources)?;
                let obs = observation::map(v2_msg, &self.config)?;
                let org = organization::map(v2_msg, &self.config, &self.resources)?;
                let res = p
                    .into_iter()
                    .chain(e)
                    .chain(l)
                    .chain(obs)
                    .chain(org)
                    .map(Some)
                    .collect();

                Ok(res)
            } else {
                Ok(vec![])
            }
        } else {
            Ok(vec![])
        }
    }
}

pub fn is_inpatient_location(msg: &Message) -> Result<bool, MappingError> {
    Ok(query(msg, PV1_2) == Some("I") && query(msg, PV1_3_5).map(|v| v == "KLINIKUM").is_some())
}

pub fn parse_fab<'a>(msg: &'a Message<'a>) -> Option<&'a str> {
    let ward = query(msg, PV1_3_1);
    let department = query(msg, PV1_3_4);
    let location = query(msg, PV1_3_5);

    let bed_status = query(msg, PV1_2);
    match bed_status {
        None => None,
        Some("O") | Some("E") => {
            if department.is_some() {
                department
            } else {
                if let Some(loc) = location {
                    if loc != "KLINIKUM" {
                        Some(loc)
                    } else {
                        if let Some(w) = ward {
                            return Some(w);
                        }
                        // location unknown
                        None
                    }
                } else {
                    None
                }
            }
        }
        Some("I") | Some("VS") | Some("NS") | Some("TS") | Some("V") | Some("H") => department,
        Some("P") => {
            // todo: if planned encounter should be mapped - we need mapping here,
            None
        }

        _ => None,
    }
}

pub(crate) fn subject_ref(msg: &Message, sid: &str) -> Result<Reference, MappingError> {
    let pid = query(msg, PID_2).ok_or(anyhow!("missing pid value in PID.2"))?;

    resource_ref(&ResourceType::Patient, pid, sid).map_err(MappingError::BuilderError)
}

/// FieldExtension with unsupported data absent reason entry
pub(crate) fn coding_data_absent_reason_unsupported() -> Result<CodeableConcept, MappingError> {
    Ok(CodeableConcept::builder()
        .coding(vec![Some(
            Coding::builder()
                .code("unsupported".to_string())
                .system("http://terminology.hl7.org/CodeSystem/data-absent-reason".to_string())
                .build()?,
        )])
        .build()?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use adt_config::test_utils::tests::{
        filter_resources, get_dummy_resources, get_test_config, has_profile, read_test_resource,
    };
    use fhir_core::mapping::misc::{full_url_from_identifiers, parse_datetime, patch_bundle_entry};
    use fhir_model::DateTime::DateTime;
    use fhir_model::r4b::codes::HTTPVerb::{Delete, Patch};
    use fhir_model::r4b::resources::{
        Bundle, BundleEntry, BundleEntryRequest, Encounter, Parameters, Patient, Resource, ResourceType,
    };
    use fhir_model::time;
    use fhir_model::time::{Month, OffsetDateTime, Time};
    use insta::assert_json_snapshot;
    use processor_hl7v2::hl7_to_patient_dto::map;
    use rstest::rstest;
    use serde_json::Value;
    use std::slice;
    use std::str::FromStr;

    #[test]
    fn test_parse_datetime() {
        // 2009-03-30 19:36
        let s = "200903301036";

        let parsed = parse_datetime(s).unwrap();

        let expected = DateTime(
            OffsetDateTime::new_utc(
                time::Date::from_calendar_date(2009, Month::March, 30).unwrap(),
                // local time is +2 (CEST) in this case
                Time::from_hms(8, 36, 0).unwrap(),
            )
            .into(),
        );

        assert_eq!(parsed, expected);
    }

    #[test]
    fn map_test() {
        let hl7 = read_test_resource("a08_test.hl7");

        let config = get_test_config();
        let mapper = FhirMapper {
            config: config.clone(),
            resources: get_dummy_resources(),
        };

        // act
        let mapped = mapper.map(&hl7).unwrap();

        // map back to assert
        let bundle: Bundle = serde_json::from_str(mapped.unwrap().as_str()).unwrap();

        assert_eq!(bundle.entry.len(), 9);

        let patient: Vec<Patient> = filter_resources(&bundle);
        let encounter: Vec<Encounter> = filter_resources(&bundle);

        // assert profiles set
        assert!(
            patient
                .iter()
                .all(|p| has_profile(p.meta.as_ref().unwrap(), &config.person.profile))
        );
        assert!(
            encounter
                .iter()
                .all(|e| has_profile(e.meta.as_ref().unwrap(), &config.fall.profile))
        );
    }

    #[test]
    fn test_patch_bundle_entry() {
        let identifier = &Identifier::builder()
            .system("system".to_string())
            .value("value".to_string())
            .build()
            .unwrap();
        let entry = patch_bundle_entry(
            Parameters::builder().build().unwrap(),
            &ResourceType::Patient,
            identifier,
            &get_test_config(),
        )
        .unwrap();

        assert_eq!(
            entry,
            BundleEntry::builder()
                .full_url(full_url_from_identifiers(
                    slice::from_ref(identifier),
                    &get_test_config()
                ))
                .resource(Resource::from(Parameters::builder().build().unwrap()))
                .request(
                    BundleEntryRequest::builder()
                        .method(Patch)
                        .url("Patient?identifier=system|value".to_string())
                        .build()
                        .unwrap()
                )
                .build()
                .unwrap(),
        )
    }

    #[rstest]
    #[case("A11", "DELETE", "", 5)]
    #[case("A12", "DELETE", "", 4)]
    #[case("A27", "DELETE", "", 5)]
    #[case("A02", "PUT", "POST", 10)]
    fn map_request_and_encounter_type_test(
        #[case] msg_type: String,
        #[case] request_type_encounter: String,
        #[case] request_type_patient: String,
        #[case] resource_count: usize,
    ) {
        let hl7 = format!(
            r#"MSH|^~\&|ORBIS|KH|RECAPP|ORBIS|202111230904||ADT^{}_{}|62325574|P|2.5|||||D||DE
EVN|{}|202111230904|202111230904||Muster
PID|1|1396227|1396227||Test^Anton||19510704|M|||Teststr. 26^^Wetzlar^^35578^D^L||0151/123123123^^CP|||M|or|||||||N||SYR
PV1|1|I|UROST133^133-03^1^URO^KLINIKUM^900000|R^^HL7~01^Normalfall^301||UROST133^^^URO^KLINIKUM^900000||35576TEO^Test^Ulrike^^Frau^Dr. med.^Karl-Test-Ring 23^35576^Test^06441^45433^FÄ für Test|35576TEO^Test^Ulrike^^Frau^Dr. med.^Karl-Test-Ring 23^35576^Test^06441^45433^FÄ für Allgemeinmedizin|N||||||N|||23232323||K|||||||||||||||01|||2200|9||||202111190630|202111230904||||||A
PV2||xxx|02^KH-Behandlung, vollstat. nach vorstat.^301||||||202112030000||||||||||||N|||I||||||||||||N
ZBE|30674176^ORBIS|202111230904||DUMMY"#,
            msg_type, msg_type, msg_type
        );

        let config = get_test_config();
        let mapper = FhirMapper {
            config: config.clone(),
            resources: get_dummy_resources(),
        };

        let expected_request_type = HTTPVerb::from_str(request_type_encounter.as_str()).unwrap();

        // act
        let mapped = mapper.map(&hl7).unwrap();
        let bundle: Bundle = serde_json::from_str(mapped.unwrap().as_str()).unwrap();

        bundle.entry.iter().for_each(|entry| {
            let entry_typ = entry
                .as_ref()
                .unwrap()
                .resource
                .as_ref()
                .unwrap()
                .resource_type();
            match entry_typ {
                ResourceType::Encounter => {
                    check_request_type(&msg_type, expected_request_type, entry);
                }

                ResourceType::Location | ResourceType::Organization => {
                    check_request_type(&msg_type, HTTPVerb::Put, entry);
                }
                ResourceType::Observation => {
                    match msg_type.as_str() {
                        "A04" | "A03" | "A02" => {}
                        _ => {
                            assert_eq!(
                                "For message type '{}' patient resource should not be created.",
                                msg_type
                            );
                        }
                    }
                    check_request_type(&msg_type, HTTPVerb::Put, entry);
                }
                ResourceType::Patient => {
                    match msg_type.as_str() {
                        "A04" | "A02" => {}
                        _ => {
                            assert_eq!(
                                "For message type '{}' patient resource should not be created.",
                                msg_type
                            );
                        }
                    }

                    check_request_type(
                        &msg_type,
                        HTTPVerb::from_str(request_type_patient.as_str()).unwrap(),
                        entry,
                    );
                }
                _ => {
                    panic!(
                        "unexpected resource type '{}' at message type '{}",
                        entry_typ, msg_type
                    );
                }
            }
        });

        assert_eq!(
            bundle.entry.len(),
            resource_count,
            "For message type '{}' we expect {} resource to be created.",
            msg_type,
            resource_count
        );

        if msg_type == "A11" || msg_type == "A27" {
            assert!(
                bundle
                    .entry
                    .iter()
                    .find(|entry| {
                        entry
                            .as_ref()
                            .unwrap()
                            .request
                            .as_ref()
                            .unwrap()
                            .url
                            .eq(format!(
                                "Encounter?identifier={}|{}",
                                config.fall.einrichtungskontakt.system, "23232323"
                            )
                            .as_str())
                    })
                    .is_some()
            );
        }
        assert!(
            bundle
                .entry
                .iter()
                .find(|entry| {
                    entry
                        .as_ref()
                        .unwrap()
                        .request
                        .as_ref()
                        .unwrap()
                        .url
                        .eq(format!(
                            "Encounter?identifier={}|{}",
                            config.fall.abteilungskontakt.system, "30674176"
                        )
                        .as_str())
                })
                .is_some()
        );
        assert!(
            bundle
                .entry
                .iter()
                .find(|entry| {
                    entry
                        .as_ref()
                        .unwrap()
                        .request
                        .as_ref()
                        .unwrap()
                        .url
                        .eq(format!(
                            "Encounter?identifier={}|{}",
                            config.fall.versorgungsstellenkontakt.system, "30674176"
                        )
                        .as_str())
                })
                .is_some()
        )
    }

    fn check_request_type(
        msg_type: &String,
        expected_request_type: HTTPVerb,
        entry: &Option<BundleEntry>,
    ) {
        let resource_name = entry
            .as_ref()
            .unwrap()
            .resource
            .as_ref()
            .unwrap()
            .resource_type()
            .as_str();

        let url_value = entry
            .as_ref()
            .unwrap()
            .request
            .as_ref()
            .unwrap()
            .url
            .clone();

        assert_eq!(
            expected_request_type,
            entry.as_ref().unwrap().request.as_ref().unwrap().method,
            "At msg_type {} resource {} must be send with {} request",
            msg_type,
            resource_name,
            expected_request_type
        );

        if expected_request_type == HTTPVerb::Put {
            assert!(url_value.starts_with(resource_name));
        }
        if expected_request_type == HTTPVerb::Post {
            let if_not_exists = entry
                .as_ref()
                .unwrap()
                .request
                .as_ref()
                .unwrap()
                .if_none_exist
                .clone();
            assert!(
                if_not_exists.is_some(),
                "on msg type '{}' resource {} must be send with if-none-exists entry!",
                msg_type,
                resource_name
            );
            assert!(if_not_exists.unwrap().starts_with("identifier="));

            assert_eq!(
                url_value,
                entry
                    .as_ref()
                    .unwrap()
                    .resource
                    .as_ref()
                    .unwrap()
                    .resource_type()
                    .as_str()
            );
        }
    }

    #[rstest]
    #[case("O", "POLPOLAMB^^^POL^POLPOL^945400^^^", "POL")]
    #[case("O", "^^^^KLINIKUM", "")]
    #[case("O", "ACH^^^^KLINIKUM", "ACH")]
    #[case("I", "^^^NEUPOLAMB^NEUPOL^12335", "NEUPOLAMB")]
    #[case("I", "PRDFSENTL^^^PDR^KLINIKUM", "PDR")]
    #[case("O", "UROPOLXXX^^^^UROYYYYYYY^0^^^", "UROYYYYYYY")]
    #[case("I", "^^^NEUPOLAMB^NEUPOL^12335", "NEUPOLAMB")]
    #[case("TS", "NECTSDF^^^NEC^KLINIKUNM^12335", "NEC")]
    #[case("VS", "^^^HNOPOLAMB^HNOPOL^12335", "HNOPOLAMB")]
    #[case("NS", "^^^HNOPOLAMB^HNOPOL^12335", "HNOPOLAMB")]
    #[case("NS", "^^^GYN^KLINIKUM^12335", "GYN")]
    #[case("VS", "ANAFSGO^^^ANA^KLINIKUM^12335", "ANA")]
    #[case("H", "ANAFSGO^^^ANA^KLINIKUM^12335", "ANA")]
    fn test_parse_fab(#[case] bed_status: String, #[case] pv1_3: String, #[case] expected: &str) {
        let input = format!(
            r#"MSH|^~\&|ORBIS|KH|RECAPP|ORBIS|202111221030||ADT^A01|62293727|P|2.5||123456789|NE|NE||8859/1
EVN|A01|202111221030|202111221029||EIDAMN
PID|1|1499653|1499653||Test^Meinrad^^Graf^von^Dr.^L|Test|202301181003|M|||Test Str.  27^^Bad Test^^57334^D^L||02752/1672^^PH|||M|rk|||||||N||D||||N|
NK1|1|Fr. Test|14^Ehefrau||s.Pat.||||||||||U|^YYYYMMDDHHMMSS|||||||||||||||||^^^ORBIS^PN~^^^ORBIS^PI~^^^ORBIS^PT
PV1|1|{}|{}|R^^HL7~01^Normalfall^301||||||N||||||N|||00000000||K|||||||||||||||01||||9||||202211101359|202211101359||||||AIN1|1|102171012|KKH|KKH Allianz|^^Leipzig^^04017^D||||Ersatzkassen^13^^^1&gesetzlich|||||||Mustermann^Max||19470128|Mustergasse 10^^Musterort^^33333^D|||1|||||||201111090942||R||||||||||||M| |||||1234567890^^^^^^^20130331"#,
            bed_status, pv1_3
        );

        let msg = Message::parse_with_lenient_newlines(input.as_str(), true).unwrap();
        if expected.is_empty() {
            assert!(parse_fab(&msg).is_none());
        } else {
            assert_eq!(parse_fab(&msg), Some(expected));
        }
    }

    #[test]
    fn missing_encounter_start_datetime() {
        let input = r#"MSH|^~\&|ORBIS||RECAPP|ORBIS|201111280918||ADT^A02|11658910|P|2.5|||||DE||DE
EVN|A02|201111280915|201111280915||TEST
PID|1|111111|111111||Musterfrau^Marta|Mustergeburtsname|20090515|F|||Mustergasse 10^^Musterort^^33333^DE||012345/1234^^PH|||S|||||||Marburg|N||DE|Kindergartenkind
PV1|1|I|IDIST041^041-10^^KCH^^123444|R||IDIST041^041-13^1^KCH^^123444|||44444ARZT^Arzt^Hans Jürgen^^Praxis^^Dr. med.|N||||||N|||21600000||K||||||||||||||||||1300||||||||||||A
ZBE|44444444^ORBIS|202601280923||INSERT"#;
        let mapper = FhirMapper::new(get_test_config()).unwrap();
        let result = mapper.map(input);
        assert!(result.is_ok());

        let raw: Value = serde_json::from_str(&result.unwrap().unwrap()).unwrap();

        let b: Bundle = serde_json::from_value(raw).unwrap();
        assert!(!b.entry.is_empty());

        b.entry.iter().for_each(|entry| {
            let resource = entry.clone().unwrap().resource.unwrap();

            assert_ne!(
                resource.resource_type(),
                ResourceType::Encounter,
                "if start date time is missing we cannot create encounter resources"
            );
        });
    }
    #[test]
    fn a01_admission_snapshot_test() {
        let test_file = "a01_test.hl7";
        let binding = read_test_resource(test_file);

        let mapper = FhirMapper::new(get_test_config()).unwrap();
        match mapper.map(binding.as_str()) {
            Ok(Some(result)) => {
                let raw: Value = serde_json::from_str(&result).unwrap();
                let b: Bundle = serde_json::from_value(raw).unwrap();
                assert_json_snapshot!(b,{
                    ".meta.lastUpdated" =>
                        "2026-08-14T07:52:30.71553162Z"
                    }
                );
            }
            Ok(None) => {
                panic!("We should have been an error here - but got empty result!")
            }
            Err(e) => {
                panic!("test failed result Error: {}", e.to_string())
            }
        }
    }

    #[test]
    fn a02_move_snapshot_test() {
        let test_file = "a02_test.hl7";
        let binding = read_test_resource(test_file);

        let mapper = FhirMapper::new(get_test_config()).unwrap();
        match mapper.map(binding.as_str()) {
            Ok(Some(result)) => {
                let raw: Value = serde_json::from_str(&result).unwrap();
                let b: Bundle = serde_json::from_value(raw).unwrap();

                assert_json_snapshot!(b,{
                    ".meta.lastUpdated" =>
                        "2026-08-14T07:52:30.711028405Z"
                    }
                );
            }
            Ok(None) => {
                panic!("We should have been an error here - but got empty result!")
            }
            Err(e) => {
                panic!("test failed result Error: {}", e.to_string())
            }
        }
    }

    #[test]
    fn a03_disscharge_snapshot_test() {
        let test_file = "a03_test.hl7";
        let binding = read_test_resource(test_file);

        let mapper = FhirMapper::new(get_test_config()).unwrap();
        match mapper.map(binding.as_str()) {
            Ok(Some(result)) => {
                let raw: Value = serde_json::from_str(&result).unwrap();
                let b: Bundle = serde_json::from_value(raw).unwrap();
                assert_json_snapshot!(b, { ".meta.lastUpdated"
                    => "2026-08-14T07:52:30.711879467Z"}
                );
            }
            Ok(None) => {
                panic!("We should have been an error here - but got empty result!")
            }
            Err(e) => {
                panic!("test failed result Error: {}", e.to_string())
            }
        }
    }

    #[test]
    fn a04_treatment_snapshot_test() {
        let test_file = "a04_test.hl7";
        let binding = read_test_resource(test_file);

        let mapper = FhirMapper::new(get_test_config()).unwrap();
        match mapper.map(binding.as_str()) {
            Ok(Some(result)) => {
                let raw: Value = serde_json::from_str(&result).unwrap();
                let b: Bundle = serde_json::from_value(raw).unwrap();
                assert_json_snapshot!(b, {".meta.lastUpdated" => "2026-08-14T07:52:30.715157624Z"});
            }
            Ok(None) => {
                panic!("We should have been an error here - but got empty result!")
            }
            Err(e) => {
                panic!("test failed result Error: {}", e.to_string())
            }
        }
    }

    #[test]
    fn a05_post_inpatient_snapshot_test() {
        let test_file = "a05_ns_test.hl7";
        let binding = read_test_resource(test_file);

        let mapper = FhirMapper::new(get_test_config()).unwrap();
        match mapper.map(binding.as_str()) {
            Ok(Some(result)) => {
                let raw: Value = serde_json::from_str(&result).unwrap();
                let b: Bundle = serde_json::from_value(raw).unwrap();
                assert_json_snapshot!(b, {".meta.lastUpdated" => "2026-08-14T07:52:30.714882827Z"});
            }
            Ok(None) => {
                panic!("We should have been an error here - but got empty result!")
            }
            Err(e) => {
                panic!("test failed result Error: {}", e.to_string())
            }
        }
    }

    #[test]
    fn a06_shortstay_snapshot_test() {
        let test_file = "a06_teilsstationaer_test.hl7";
        let binding = read_test_resource(test_file);

        let mapper = FhirMapper::new(get_test_config()).unwrap();
        match mapper.map(binding.as_str()) {
            Ok(Some(result)) => {
                let raw: Value = serde_json::from_str(&result).unwrap();
                let b: Bundle = serde_json::from_value(raw).unwrap();
                assert_json_snapshot!(b, {".meta.lastUpdated" => "2026-08-14T07:52:30.71805125Z"});
            }
            Ok(None) => {
                panic!("We should have been an error here - but got empty result!")
            }
            Err(e) => {
                panic!("test failed result Error: {}", e.to_string())
            }
        }
    }

    #[test]
    fn a07_shortstay_snapshot_test() {
        let test_file = "a07_nachstationaer_test.hl7";
        let binding = read_test_resource(test_file);

        let mapper = FhirMapper::new(get_test_config()).unwrap();
        match mapper.map(binding.as_str()) {
            Ok(Some(result)) => {
                let raw: Value = serde_json::from_str(&result).unwrap();
                let b: Bundle = serde_json::from_value(raw).unwrap();
                assert_json_snapshot!(b, { ".meta.lastUpdated" => "2026-08-14T07:52:30.718090556Z"});
            }
            Ok(None) => {
                panic!("We should have been an error here - but got empty result!")
            }
            Err(e) => {
                panic!("test failed result Error: {}", e.to_string())
            }
        }
    }

    #[test]
    fn a08_update_snapshot_test() {
        let test_file = "a08_test.hl7";
        let binding = read_test_resource(test_file);

        let mapper = FhirMapper::new(get_test_config()).unwrap();
        match mapper.map(binding.as_str()) {
            Ok(Some(result)) => {
                let raw: Value = serde_json::from_str(&result).unwrap();
                let b: Bundle = serde_json::from_value(raw).unwrap();
                assert_json_snapshot!(b, {".meta.lastUpdated" => "2026-08-14T07:52:30.714534242Z"});
            }
            Ok(None) => {
                panic!("We should have been an error here - but got empty result!")
            }
            Err(e) => {
                panic!("test failed result Error: {}", e.to_string())
            }
        }
    }

    #[test]
    fn a11_snapshot_test() {
        let test_file = "a11_test.hl7";
        let binding = read_test_resource(test_file);

        let mapper = FhirMapper::new(get_test_config()).unwrap();
        match mapper.map(binding.as_str()) {
            Ok(Some(result)) => {
                let raw: Value = serde_json::from_str(&result).unwrap();
                let b: Bundle = serde_json::from_value(raw).unwrap();
                assert_json_snapshot!(b, { ".meta.lastUpdated" => "2026-08-14T07:52:30.71116015Z"});
            }
            Ok(None) => {
                panic!("We should have been an error here - but got empty result!")
            }
            Err(e) => {
                panic!("test failed result Error: {}", e.to_string())
            }
        }
    }

    #[test]
    fn a34_snapshot_test() {
        let test_file = "a34_test.hl7";
        let binding = read_test_resource(test_file);

        let mapper = FhirMapper::new(get_test_config()).unwrap();
        match mapper.map(binding.as_str()) {
            Ok(Some(result)) => {
                let raw: Value = serde_json::from_str(&result).unwrap();
                let b: Bundle = serde_json::from_value(raw).unwrap();
                assert_json_snapshot!(b, { ".meta.lastUpdated" => "2026-08-14T07:52:30.710562417Z"});
            }
            Ok(None) => {
                panic!("We should have been an error here - but got empty result!")
            }
            Err(e) => {
                panic!("test failed result Error: {}", e.to_string())
            }
        }
    }

    #[test]
    fn a38_snapshot_test() {
        let test_file = "a38_test.hl7";
        let binding = read_test_resource(test_file);

        let mapper = FhirMapper::new(get_test_config()).unwrap();
        match mapper.map(binding.as_str()) {
            Ok(Some(result)) => {
                let raw: Value = serde_json::from_str(&result).unwrap();
                let b: Bundle = serde_json::from_value(raw).unwrap();
                assert_json_snapshot!(b, {".meta.lastUpdated" => "2026-08-14T07:52:30.71123337Z"});
            }
            Ok(None) => {
                panic!("We should have been an error here - but got empty result!")
            }
            Err(e) => {
                panic!("test failed result Error: {}", e.to_string())
            }
        }
    }
    #[test]
    fn test_all_hl7_files() {
        let test_files = vec![
            "a01_test.hl7",
            "a02_test.hl7",
            "a03_test.hl7",
            "a04_test.hl7",
            "a04_test2.hl7",
            "a04_amb_notfall.hl7",
            "a05_ns_test.hl7",
            "a08_test.hl7",
            "a06_teilsstationaer_test.hl7",
            "a07_nachstationaer_test.hl7",
            "a11_test.hl7",
            "a34_test.hl7",
            "a38_test.hl7",
        ];
        for test_file in test_files {
            let binding = read_test_resource(test_file);

            let mapper = FhirMapper::new(get_test_config()).unwrap();
            match mapper.map(binding.as_str()) {
                Ok(Some(bundle)) => {
                    println!("file {} ", test_file);

                    let raw: Value = serde_json::from_str(&bundle).unwrap();

                    // for local testing uncomment
                    //
                    //                    assert!(
                    //                        validate_with_server(test_file, &raw, &IssueSeverity::Error),
                    //                        "FHIR validation failed!"
                    //                    );

                    let b: Bundle = serde_json::from_value(raw).unwrap();
                    b.entry.iter().for_each(|entry| {
                        let resource = entry.clone().unwrap().resource.unwrap();

                        if resource.resource_type() != ResourceType::Parameters {
                            let base = resource.as_base_resource();

                            let source = base.meta().clone().and_then(|m| m.source.clone());

                            assert_eq!(
                                source.as_deref(),
                                Some(get_test_config().meta_source.as_str()),
                                "meta.source stimmt nicht für Resource '{}' aus Datei '{}'",
                                resource.resource_type(),
                                test_file,
                            );

                            let has_identifier = resource.as_identifiable_resource().unwrap();
                            assert!(
                                has_identifier.identifier().iter().all(|i| i
                                    .clone()
                                    .unwrap()
                                    .value
                                    .is_some()),
                                "some identifier for input {} are missing value",
                                test_file
                            );
                        }
                    });
                }
                Ok(None) => panic!("empty bundle at input {}", test_file),
                Err(err) => {
                    panic!(
                        "FAILED processing input '{}' with error: {}",
                        test_file, err
                    )
                }
            }
        }
    }

    #[test]
    fn patient_merge_snapshot_test() {
        let config = &get_test_config();

        let msg =
            Message::parse_with_lenient_newlines(r#"MSH|^~\&|ORBIS|KH|WEBEPA|KH|20230912105234||ADT^A40^ADT_A39|12345678|P|2.5||123456789|NE|NE||8859/1
EVN|A40|202309121052||00000_123456789|XXXXX|202309121052
PID|1|1234567|1234567||Musterfrau^Maxi^^^^^L|||F|||^^^^^^L||^ ^ ^^^^^^^^^|||U||||||||||DE||||N
MRG|09876543|||09876543|||Musterfrau^Maxi^^^^^L"#, true)
                .unwrap();
        let result = fhir_core::mapping::patient_mapper::map(
            &hl7_to_patient_dto::map(&msg).unwrap().unwrap(),
            config,
        );
        let entry = result.unwrap().unwrap();
        insta::assert_json_snapshot!(entry);
    }

    #[test]
    fn test_delete_patient_snapshot() {
        let config = &get_test_config();

        let msg = Message::parse_with_lenient_newlines(r#"MSH|^~\&|ORBIS|KH|WEBEPA|KH|20221121142711||ADT^A29^ADT_A21|71546182|P|2.5||684450133|NE|NE||8859/1
EVN|A29|202211211427||12127_684450133|MEDCO-TOBL|202211211427
PID|1|1234567|1234567||Test-UCH^Endoprothese^^^^^L~Test^^^^^^B||19450201|M|||Baldinger Strasse&Baldinger Strasse^^Marburg^^35037^DE^L|||||S||||||||||DE||||N"#, true)
            .unwrap();

        let entry = fhir_core::mapping::patient_mapper::map(&map(&msg).unwrap().unwrap(), config)
            .unwrap()
            .unwrap();

        assert_eq!(
            entry.request,
            Some(
                BundleEntryRequest::builder()
                    .url(format!(
                        "{}?identifier={}|1234567",
                        &ResourceType::Patient,
                        config.person.system
                    ))
                    .method(Delete)
                    .build()
                    .unwrap()
            )
        );

        insta::assert_json_snapshot!(entry);
    }
}
