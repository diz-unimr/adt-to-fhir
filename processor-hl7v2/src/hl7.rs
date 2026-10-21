use crate::hl7::parser::MessageType::A14;
use crate::hl7::parser::{
    PID_4, PV1_2, PV1_3_1, PV1_3_4, PV1_3_5, PV1_19_1, ZBE_2, message_type, query,
};
use crate::hl7_error::Hl7MessageAccessError;
use adt_config::resources::ResourceMap;
use chrono::NaiveDate;
use fhir_core::mapping::fab_mapping::is_valid_date;
use hl7_parser::Message;
use std::string::ToString;

pub mod parser;

/// check if ward short name is a valid ICU at message time
pub fn is_ward_valid_icu(msg: &Message, resources: &ResourceMap) -> bool {
    query(msg, PV1_3_1)
        .and_then(|ward_id| resources.ward_map.get(ward_id))
        .is_some_and(|ward| {
            ward.is_icu
                && query(msg, ZBE_2)
                    .and_then(|zbe_start| {
                        let option = NaiveDate::parse_from_str(zbe_start, "%Y%m%d%H%M");
                        option.ok()
                    })
                    .is_some_and(|n_date| {
                        ward.valid_period
                            .iter()
                            .any(|period| is_valid_date(period, &n_date))
                    })
        })
}

/// get encounter number
pub fn map_visit_number<'a>(msg: &'a Message) -> Result<&'a str, Hl7MessageAccessError> {
    match message_type(msg)? {
        A14 => Ok(
            query(msg, PID_4).ok_or(Hl7MessageAccessError::MissingMessageValue(
                "PID.4".to_string(),
            ))?,
        ),
        _ => Ok(
            query(msg, PV1_19_1).ok_or(Hl7MessageAccessError::MissingMessageValue(
                "PV1.19".to_string(),
            ))?,
        ),
    }
}

/// returns encounter number if message contains one - use only for observation
pub fn map_visit_number_lenient<'a>(msg: &'a Message) -> Option<&'a str> {
    query(msg, PV1_19_1)
        .filter(|s| !s.is_empty())
        .or_else(|| query(msg, PID_4).filter(|s| !s.is_empty()))
}
pub fn is_begleitperson(msg: &Message) -> bool {
    query(msg, PV1_2).is_some_and(|f| f == "H")
}

pub fn parse_department_name<'a>(msg: &'a Message<'a>) -> Option<&'a str> {
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
#[cfg(test)]
mod test {
    use crate::hl7::parse_department_name;
    use hl7_parser::Message;
    use rstest::rstest;

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
            assert!(parse_department_name(&msg).is_none());
        } else {
            assert_eq!(parse_department_name(&msg), Some(expected));
        }
    }
}
