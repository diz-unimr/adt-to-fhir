use crate::hl7::parser::MessageType::A14;
use crate::hl7::parser::{PID_4, PV1_2, PV1_3_1, PV1_19_1, ZBE_2, message_type, query};
use crate::hl7_error::Hl7MessageAccessError;
use adt_config::resources::ResourceMap;
use chrono::NaiveDate;
use fhir_core::model::fab_mapping::is_valid_date;
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
