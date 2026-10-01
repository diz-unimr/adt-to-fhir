use crate::hl7::is_ward_valid_icu;
use crate::hl7::parser::MessageType::A14;
use crate::hl7::parser::{
    PID_4, PID_21_1, PV1_2, PV1_4__2_1, PV1_4_1, PV1_19_1, PV1_36_1, PV1_40_1, PV1_44, PV1_45,
    PV2_3_1, ZBE_1_1, ZBE_2, ZBE_3, get_message_key, message_type, parse_naive_datetime, query,
};
use crate::hl7_error::Hl7MappingError::BuilderError;
use crate::hl7_error::{Hl7MappingError, Hl7MessageAccessError, Hl7MessageParsingError};
use adt_config::resources::ResourceMap;
use anyhow::anyhow;
use chrono::NaiveDateTime;
use derive_builder::Builder;
use futures::TryFutureExt;
use hl7_parser::Message;
use std::num::NonZeroU32;

#[derive(Debug, Clone, PartialEq, Builder)]
#[builder(setter(into))]
pub struct Fall {
    pub pid: String,
    pub visit_number: String,
    pub bed_status: String,

    pub admission_datetime: NaiveDateTime,
    pub admission_type: String,
    pub admission_reason_1_2: Option<String>,
    pub admission_reason_3_4: Option<String>,

    pub movement_id: String,
    pub movement_start: NaiveDateTime,
    pub movement_end: Option<NaiveDateTime>,
    pub is_icu_stay: bool,

    pub discharge: Option<NaiveDateTime>,
    pub discharge_reason_12: Option<String>,
    pub discharge_reason_3: Option<String>,
    pub diagnosis: Option<Vec<Fall_Diagnose>>,
    pub mothers_enc_number: Option<String>,
}

fn map(msg: &Message, resources: &ResourceMap) -> Result<Fall, Hl7MappingError> {
    /*
     * mandatory properties
     */
    let msg_type = message_type(msg)?;
    let id = get_message_key(msg)?.to_string();

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

    let is_valid_ICU_ward = is_ward_valid_icu(msg, resources);

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
        fall.admission_type = aufnahmeart.to_string();
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
    Ok(fall)
}
#[derive(Debug, Clone, PartialEq, Builder)]
#[builder(setter(into))]
pub struct Fall_Diagnose {
    pub ordinal_number: u32,
    pub id: String,
    pub condition_typ: String,
    pub priority: NonZeroU32,
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
                .map_err(|e| Hl7MessageParsingError::ParseFloatError(e.into()))?
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
                        .map_err(|e| Hl7MessageParsingError::ParseIntError(e.into()))?,
                )
                .build()
                .map_err(|e| BuilderError {
                    builder_name: "Fall_DiagnoseBuilder".to_string(),
                    builder_error: e.to_string(),
                })?;

            res.push(dg);
        }
    };
    Ok(res)
}

pub fn map_visit_number<'a>(msg: &'a Message) -> Result<&'a str, anyhow::Error> {
    match message_type(msg)? {
        A14 => Ok(query(msg, PID_4).ok_or(anyhow!("empty visit number in PID.4"))?),
        _ => Ok(query(msg, PV1_19_1).ok_or(anyhow!("empty visit number in PV1.19"))?),
    }
}
