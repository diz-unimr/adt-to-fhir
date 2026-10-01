use crate::hl7::parser::{PV1_3_1, ZBE_2, query};
use adt_config::resources::ResourceMap;
use chrono::NaiveDate;
use fhir_core::model::fab_mapping::is_valid_date;
use hl7_parser::Message;

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
