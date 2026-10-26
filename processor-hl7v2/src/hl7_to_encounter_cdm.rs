use crate::hl7::parser::{
    MessageType, PID_2, PID_21_1, PV1_2, PV1_3_1, PV1_4__1_1, PV1_4__1_3, PV1_4__2_1, PV1_36_1,
    PV1_39_1, PV1_40_1, PV1_44, PV1_45, PV2_3_1, ZBE_1_1, ZBE_2, ZBE_3, get_message_key,
    message_type, parse_to_datetime, query,
};
use crate::hl7::{is_ward_valid_icu, map_visit_number, parse_fab};
use crate::hl7_error::{Hl7MappingError, Hl7MessageAccessError, Hl7MessageParsingError};
use fhir_core::canonical_model::canonical_encounter::{
    EncounterCdm, EncounterCdmBuilder, EncounterDiagnosisCdm, EncounterDiagnosisCdmBuilder,
};
use fhir_core::canonical_model::canonical_meta::{
    MappingOpEncounter, MappingOpEncounterBuilder, ProcessingOperation,
};

use crate::hl7::parser::MessageType::A04;
use crate::hl7_to_location_cdm;
use adt_config::resources::ResourceMap;
use fhir_core::canonical_model::canonical_location::LocationCdmType;
use hl7_parser::Message;
use log::{Level, log};

pub fn map(
    msg: &Message,
    resource_map: &ResourceMap,
) -> Result<Option<EncounterCdm>, Hl7MappingError> {
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

            Ok(Some(extract_raw_data(msg, operation, resource_map)?))
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

            Ok(Some(extract_raw_data(msg, operation, resource_map)?))
        }
        _ => {
            log!(Level::Info, "Unhandled message type {:?}", message_type);
            Ok(None)
        }
    }
}

fn extract_raw_data(
    msg: &Message,
    operation: MappingOpEncounter,
    resources: &ResourceMap,
) -> Result<EncounterCdm, Hl7MappingError> {
    /*
     * mandatory properties
     */
    let pid = query(msg, PID_2).ok_or(Hl7MessageAccessError::MissingMessageValue(
        "PID-2".to_string(),
    ))?;
    let encounter_number = map_visit_number(msg)?;
    let admission_datetime = parse_to_datetime(query(msg, PV1_44).ok_or(
        Hl7MessageAccessError::MissingMessageValue("PV1.44".to_string()),
    )?)?;
    let bed_status = query(msg, PV1_2).ok_or(Hl7MessageAccessError::MissingMessageValue(
        "PV1.2".to_string(),
    ))?;
    let movement_id = query(msg, ZBE_1_1).ok_or(Hl7MessageAccessError::MissingMessageValue(
        "ZBE1.1".to_string(),
    ))?;
    let movement_start = parse_to_datetime(query(msg, ZBE_2).ok_or(
        Hl7MessageAccessError::MissingMessageValue("ZBE-2".to_string()),
    )?)?;

    let movement_end = match query(msg, ZBE_3) {
        Some(movement_end) => Some(parse_to_datetime(movement_end)?),
        None => None,
    };

    // hospitalization
    let aufnahmegrund_3_u_4 = match query(msg, PV1_4__2_1) {
        Some(value) => Some(value),
        None => {
            // fallback: sometimes first repeat is missing, then we try to map first entry
            // if value system is §301
            if let (Some(admission_typ_value), Some(admission_typ_system)) =
                (query(msg, PV1_4__1_1), query(msg, PV1_4__1_3))
            {
                if admission_typ_system == "301" {
                    Some(admission_typ_value)
                } else {
                    None
                }
            } else {
                None
            }
        }
    };

    //map_conditions
    let diagnosis_list = extract_diagnosis(msg)?;
    let diagnosis = (!diagnosis_list.is_empty()).then_some(diagnosis_list);

    // local department name may differ from official medical department name
    let department_short_name = parse_fab(msg);
    let department_long_name = match department_short_name {
        Some(fab) => resources
            .department_map
            .get(fab)
            .map(|b| b.abteilungs_bezeichnung.to_string()),
        None => None,
    };

    let clinical_dep_code = match query(msg, PV1_39_1) {
        Some(code_from_msg) => Some(code_from_msg.to_string()),
        None => match department_short_name {
            Some(fab) => resources
                .department_map
                .get(fab)
                .map(|b| b.fachabteilungs_schluessel.to_string()),
            None => None,
        },
    };

    let hl7_msg_type = message_type(msg)?;

    let locations = hl7_to_location_cdm::create_locations_new(
        msg,
        resources,
        &[
            LocationCdmType::Ward,
            LocationCdmType::Room,
            LocationCdmType::Bed,
        ],
    )?;

    let discharge_date = match query(msg, PV1_45) {
        Some(discharge_date) => Some(parse_to_datetime(discharge_date)?),
        None => None,
    };

    let fall = EncounterCdmBuilder::default()
        .meta(operation)
        .pid(pid)
        .visit_number(encounter_number)
        .admission_datetime(admission_datetime)
        .bed_status(bed_status)
        .movement_id(movement_id)
        .movement_start(movement_start)
        .movement_end(movement_end)
        .ward_short_name(query(msg, PV1_3_1).map(|a| a.to_string()))
        .department_long_name(department_long_name)
        .admission_reason_1_2(query(msg, PV2_3_1).map(|a| a.to_string()))
        .admission_reason_3_4(aufnahmegrund_3_u_4.map(|a| a.to_string()))
        .department_short_name(department_short_name.map(|s| s.to_string()))
        .discharge_reason_12(query(msg, PV1_36_1).map(|a| a.to_string()))
        .discharge_reason_3(query(msg, PV1_40_1).map(|a| a.to_string()))
        .discharge_date(discharge_date)
        .is_icu_location(is_ward_valid_icu(msg, resources))
        .is_registration(hl7_msg_type == A04)
        .clinical_department_code(clinical_dep_code)
        .mothers_enc_number(query(msg, PID_21_1).map(|a| a.to_string()))
        .locations(locations)
        .diagnosis(diagnosis)
        .build()
        .map_err(|err| Hl7MappingError::BuilderError {
            builder_name: "EncounterCdmBuilder".to_string(),
            builder_error: err.to_string(),
        })?;

    Ok(fall)
}

fn extract_diagnosis(msg: &Message) -> Result<Vec<EncounterDiagnosisCdm>, Hl7MappingError> {
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

            let dg = EncounterDiagnosisCdmBuilder::default()
                .priority(priority.raw_value())
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
    use adt_config::test_utils::tests::{get_dummy_resources, read_test_resource};
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
    pub fn a_xx_test(#[case] test_file_name: String) {
        let binding = read_test_resource(test_file_name.as_str());
        let msg = Message::parse_with_lenient_newlines(binding.as_str(), true).unwrap();
        let result = map(&msg, &get_dummy_resources());
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
        let result = map(&msg, &get_dummy_resources());
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
