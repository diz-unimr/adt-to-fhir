use crate::hl7::parser::MessageType::A14;
use crate::hl7::parser::{
    MessageType, PID_2, PID_4, PID_21_1, PV1_2, PV1_3_1, PV1_4__2_1, PV1_4_1, PV1_19_1, PV1_36_1,
    PV1_40_1, PV1_44, PV1_45, PV2_3_1, ZBE_1_1, ZBE_2, ZBE_3, get_message_key, message_type,
    parse_naive_datetime, query,
};
use crate::hl7_error::{Hl7MappingError, Hl7MessageAccessError, Hl7MessageParsingError};
use adt_config::resources::ResourceMap;
use anyhow::anyhow;
use chrono::NaiveDateTime;
use derive_builder::Builder;
use fhir_core::model::meta::{MappingOpEncounter, MappingOpEncounterBuilder, ProcessingOperation};

use crate::hl7::map_visit_number;
use fhir_core::model::encounter_dto::{Fall, Fall_Diagnose, Fall_DiagnoseBuilder, FallBuilder};
use hl7_parser::Message;
use log::{Level, log};
use std::num::NonZeroU32;

fn map(msg: &Message) -> Result<Option<Fall>, Hl7MappingError> {
    let message_type = message_type(msg)?;
    let id = get_message_key(msg)?.to_string();

    match message_type {
        MessageType::A01
        | MessageType::A02
        | MessageType::A03
        | MessageType::A04
        | MessageType::A05
        | MessageType::A06
        | MessageType::A07
        | MessageType::A08
        | MessageType::A13 => {
            let mut lvl_1_request_type = ProcessingOperation::UpdateAsCreate;
            if message_type == MessageType::A04 {
                // A04 hat eine eigene Bewegung-ID und kein Ende-Zeitpunkt. Einrichtungskontakt
                // darf nur angelegt werden, falls er fehlt, sonst würden wir eventuell beendete
                // Fälle wieder öffen!
                lvl_1_request_type = ProcessingOperation::CreateIfNotExists;
            }

            let operation = MappingOpEncounterBuilder::default()
                .id(id)
                .operation_lv1(lvl_1_request_type)
                .operation_lv2(ProcessingOperation::UpdateAsCreate)
                .operation_lv3(ProcessingOperation::UpdateAsCreate)
                .build()?;

            Ok(Some(extract_raw_data(msg, operation)?))
        }

        MessageType::A11 | MessageType::A27 | MessageType::A12 | MessageType::A38 => {
            let lvl_1_request_type = match message_type {
                // A12 deletes only  Fachabteilungskontakt & Versorgungsstellenkontakt
                MessageType::A12 => ProcessingOperation::Skip,
                _ => ProcessingOperation::Delete,
            };

            let operation = MappingOpEncounterBuilder::default()
                .id(id)
                .operation_lv1(lvl_1_request_type)
                .operation_lv2(ProcessingOperation::Delete)
                .operation_lv3(ProcessingOperation::Delete)
                .build()?;

            Ok(Some(extract_raw_data(msg, operation)?))
        }
        _ => {
            log!(Level::Info, "Unhandled message type {:?}", message_type);
            Ok(None)
        }
    }
}

fn extract_raw_data(msg: &Message, operation: MappingOpEncounter) -> Result<Fall, Hl7MappingError> {
    /*
     * mandatory properties
     */

    let pid = query(msg, PID_2).ok_or(Hl7MessageAccessError::MissingMessageValue(
        "PID-2".to_string(),
    ))?;
    let encounter_number = map_visit_number(msg)?;
    let admission_datetime = parse_naive_datetime(query(msg, PV1_44).ok_or(
        Hl7MessageAccessError::MissingMessageValue("PV1.44".to_string()),
    )?)?;
    let bed_status = query(msg, PV1_2).ok_or(Hl7MessageAccessError::MissingMessageValue(
        "PV1.2".to_string(),
    ))?;
    let movement_id = query(msg, ZBE_1_1).ok_or(Hl7MessageAccessError::MissingMessageValue(
        "ZBE1.1".to_string(),
    ))?;
    let movement_start = parse_naive_datetime(query(msg, ZBE_2).ok_or(
        Hl7MessageAccessError::MissingMessageValue("ZBE-2".to_string()),
    )?)?;

    /*
     * extended and optional properties
     */
    let discharge_datetime = query(msg, PV1_45);
    let movement_end = query(msg, ZBE_3);

    let current_ward_location = query(msg, PV1_3_1);

    // hospitalization
    let entlassgrund_1_u_2 = query(msg, PV1_36_1);
    let entlassgrund_3 = query(msg, PV1_40_1);

    // map_aufnahmegrund
    // Buchstabe
    let aufnahmeart = query(msg, PV1_4_1);

    // erste und zweite stelle (2 Ziffern)
    let aufnahmegrund_1_u_2 = query(msg, PV2_3_1);
    let aufnahmegrund_3_u_4 = query(msg, PV1_4__2_1);

    //map_conditions
    let diagnosis_list: Vec<Fall_Diagnose> = extract_diagnosis(msg)?;

    // map_mothers_encounter
    let mothers_encounter_number = query(msg, PID_21_1);

    let mut fall = FallBuilder::default()
        .meta(operation)
        .pid(pid)
        .visit_number(encounter_number)
        .admission_datetime(admission_datetime)
        .bed_status(bed_status)
        .movement_id(movement_id)
        .movement_start(movement_start)
        .build()?;

    if let Some(date_time) = discharge_datetime {
        fall.discharge = Some(parse_naive_datetime(date_time)?)
    }
    if let Some(date_time) = movement_end {
        fall.movement_end = Some(parse_naive_datetime(date_time)?);
    }
    if let Some(entlassgrund_1_u_2) = entlassgrund_1_u_2 {
        fall.discharge_reason_12 = Some(entlassgrund_1_u_2.to_string())
    }
    if let Some(entlassgrund_3) = entlassgrund_3 {
        fall.discharge_reason_3 = Some(entlassgrund_3.to_string())
    }
    if let Some(aufnahmeart) = aufnahmeart {
        fall.admission_type = Some(aufnahmeart.to_string());
    }
    if let Some(aufnahmegrund_1_u_2) = aufnahmegrund_1_u_2 {
        fall.admission_reason_1_2 = Some(aufnahmegrund_1_u_2.to_string());
    }
    if let Some(aufnahmegrund_3_u_4) = aufnahmegrund_3_u_4 {
        fall.admission_reason_3_4 = Some(aufnahmegrund_3_u_4.to_string());
    }
    if !diagnosis_list.is_empty() {
        fall.diagnosis = Some(diagnosis_list);
    }
    if let Some(mothers_encounter_number) = mothers_encounter_number {
        fall.mothers_enc_number = Some(mothers_encounter_number.to_string());
    }
    if let Some(current_ward_location) = current_ward_location {
        fall.ward_short_name = Some(current_ward_location.to_string());
    }
    Ok(fall)
}

fn extract_diagnosis(msg: &Message) -> Result<Vec<Fall_Diagnose>, Hl7MappingError> {
    let mut res = vec![];
    if msg.segment_count("DG1") > 0 {
        for dg1 in msg.segments().filter(|seg| seg.name.eq("DG1")) {
            let Some(row_number) = dg1.field(1) else {
                continue;
            };
            // local code for admission, discharge, ... other uses
            let Some(condition_typ) = dg1.field(6) else {
                continue;
            };
            let Some(priority) = dg1.field(15) else {
                continue;
            };
            let Some(condition_id) = dg1.field(20) else {
                continue;
            };

            if condition_id.is_empty() || priority.is_empty() || condition_typ.is_empty() {
                continue;
            }

            let priority_u32 = priority
                .raw_value()
                .parse::<f32>()
                .map_err(Hl7MessageParsingError::ParseFloatError)?
                .floor() as u32;

            let rank_nz = NonZeroU32::new(priority_u32).ok_or(Hl7MessageParsingError::Other(
                anyhow!("could not parse diagnosis rank into non zero value!"),
            ))?;

            let dg = Fall_DiagnoseBuilder::default()
                .priority(rank_nz)
                .condition_typ(condition_typ.raw_value())
                .id(condition_id.raw_value())
                .ordinal_number(
                    row_number
                        .raw_value()
                        .parse::<u32>()
                        .map_err(Hl7MessageParsingError::ParseIntError)?,
                )
                .build()
                .map_err(|e| Hl7MappingError::BuilderError {
                    builder_name: "Fall_DiagnoseBuilder".to_string(),
                    builder_error: e.to_string(),
                })?;

            res.push(dg);
        }
    };
    Ok(res)
}
#[cfg(test)]
mod tests {
    use super::*;
    use adt_config::test_utils::tests::read_test_resource;
    use rstest::rstest;

    #[rstest]
    #[case("a01_test.hl7")]
    #[case("a02_test.hl7")]
    #[case("a03_test.hl7")]
    #[case("a04_test.hl7")]
    #[case("a04_test2.hl7")]
    #[case("a04_amb_notfall.hl7")]
    #[case("a05_ns_test.hl7")]
    #[case("a06_teilsstationaer_test.hl7")]
    #[case("a07_nachstationaer_test.hl7")]
    #[case("a08_test.hl7")]
    #[case("a11_test.hl7")]
    #[case("a38_test.hl7")]
    pub fn aXX_test(#[case] test_file_name: String) {
        let binding = read_test_resource(test_file_name.as_str());
        let msg = Message::parse_with_lenient_newlines(binding.as_str(), true).unwrap();
        let result = map(&msg);
        match result {
            Ok(o) => {
                assert!(!o.unwrap().meta.id.is_empty())
            }
            Err(e) => {
                println!("{}", e);
                panic!("failed hl7 to DTO mapping")
            }
        }
    }

    #[test]
    pub fn a34_test() {
        let binding = read_test_resource("a34_test.hl7");
        let msg = Message::parse_with_lenient_newlines(binding.as_str(), true).unwrap();
        let result = map(&msg);
        match result {
            Ok(a) => {
                assert!(
                    a.is_none(),
                    "A34 has no encounter component and therefor result should be `OK(None)`"
                )
            }
            Err(_) => {
                panic!("failed hl7 mapping")
            }
        }
    }
}
