use crate::error::{MappingError, MessageAccessError};
use crate::hl7::parser::{parse_component, parse_field, parse_repeating_field_component_value};
use fhir_model::BuilderError;
use fhir_model::r4b::types::{CodeableConcept, Coding};
use hl7_parser::Message;

pub(crate) fn is_inpatient_location(msg: &Message) -> Result<bool, MappingError> {
    Ok(parse_field(msg, "PV1", 2)?
        .map(|s| s.raw_value() == "I")
        .is_some()
        && parse_repeating_field_component_value(msg, "PV1", 3, 5)?
            .map(|v| v == "KLINIKUM")
            .is_some())
}

pub(crate) fn get_cc_with_one_code(
    code: String,
    system: String,
) -> Result<CodeableConcept, BuilderError> {
    CodeableConcept::builder()
        .coding(vec![Some(
            Coding::builder()
                .code(code.to_string())
                .system(system.to_string())
                .build()?,
        )])
        .build()
}

pub(crate) fn parse_fab(msg: &Message) -> Result<Option<String>, MessageAccessError> {
    if let Some(assigned_loc) = parse_field(msg, "PV1", 3)? {
        let facility = parse_component(assigned_loc, 4);
        let location = parse_component(assigned_loc, 1);
        let loc_status = parse_component(assigned_loc, 5);
        // let kostenstelle = extract_repeat(assigned_loc, 6)?;

        // todo: kostenstelle lookup etc.
        return match (facility, location, loc_status) {
            // 1. wenn PV1-3.1 und PV1-3.4 Wert haben -> PV1-3.4
            (Some(f), Some(_), _) => Ok(Some(f)),
            // 2. wenn PV1-3.4 leer & PV1-3.1 hat Wert -> dann  PV1-3.1
            (None, Some(l), _) => Ok(Some(l)),
            // 3. wenn PV1-3.1 leer & PV1-3.4 hat Wert-> dann  PV1-3.5
            (Some(_), None, Some(st)) => Ok(Some(st)),
            _ => Ok(None),
        };
    }

    Ok(None)
}
