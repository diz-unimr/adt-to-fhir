use crate::error::MappingError;
use crate::hl7::parser::{parse_field, parse_repeating_field_component_value};
use hl7_parser::Message;

pub(crate) fn is_inpatient_location(msg: &Message) -> Result<bool, MappingError> {
    Ok(parse_field(msg, "PV1", 2)?
        .map(|s| s.raw_value() == "I")
        .is_some()
        && parse_repeating_field_component_value(msg, "PV1", 3, 5)?
            .map(|v| v == "KLINIKUM")
            .is_some())
}
