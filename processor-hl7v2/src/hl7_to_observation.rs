use crate::hl7::parser::{MessageType, get_message_key, message_type};
use crate::hl7_error::Hl7MappingError;
use crate::hl7_to_patient_dto::hl7_to_patient_dto;
use fhir_core::model::meta::{MappingOpPerson, MappingTarget, ProcessingOperation};
use fhir_core::model::person_dto::PersonDto;
use hl7_parser::Message;

pub fn map(msg: &Message) -> Result<Option<PersonDto>, Hl7MappingError> {
    let msg_type = message_type(msg)?;
    let id = get_message_key(msg)?;

    let is_zng_present = msg.segment_count("ZNG") > 0;

    let person_dto = match msg_type {
        MessageType::A01
        | MessageType::A02
        | MessageType::A03
        | MessageType::A04
        | MessageType::A05 => {
            // create vitalstatus
            Some(hl7_to_patient_dto(
                msg,
                MappingOpPerson {
                    id: id.to_string(),
                    operation: MappingTarget::Observation(ProcessingOperation::UpdateAsCreate),
                },
            )?)
        }
        _ => None,
    };

    if is_zng_present && person_dto.is_none() {
        Ok(Some(hl7_to_patient_dto(
            msg,
            MappingOpPerson {
                id: id.to_string(),
                operation: MappingTarget::Observation_with_Zng(ProcessingOperation::UpdateAsCreate),
            },
        )?))
    } else {
        Ok(person_dto)
    }
}
