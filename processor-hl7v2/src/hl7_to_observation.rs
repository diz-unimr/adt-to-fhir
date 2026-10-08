use crate::hl7::map_visit_number_lenient;
use crate::hl7::parser::{
    MessageType, PID_2, PID_29, PID_30, ZBE_2, ZNG_6, ZNG_7, ZNG_11, message_type,
    parse_to_datetime, query,
};
use crate::hl7_error::{Hl7MappingError, Hl7MessageAccessError, Hl7MessageParsingError};
use fhir_core::model::obervation_dto::{ObservationDto, ObservationDtoBuilder};
use hl7_parser::Message;

pub fn map(msg: &Message) -> Result<Option<ObservationDto>, Hl7MappingError> {
    let msg_type = message_type(msg)?;

    let is_zng_present = msg.segment_count("ZNG") > 0;
    let is_dead = Some("J") == query(msg, PID_30) || query(msg, PID_29).is_some();

    if !is_zng_present && is_dead {
        // fast exit no vital status observation to create
        return Ok(None);
    }

    let do_create_is_alive = matches!(
        msg_type,
        MessageType::A01
            | MessageType::A02
            | MessageType::A03
            | MessageType::A04
            | MessageType::A05
    );

    if (!do_create_is_alive && !is_zng_present) {
        // if  no ZNG and skipping via msg typ are in place, no observation to map
        return Ok(None);
    }

    let mut builder = ObservationDtoBuilder::default();
    builder.pid(
        query(msg, PID_2)
            .ok_or_else(|| Hl7MessageAccessError::MissingMessageValue("PID_2".to_string()))?,
    );

    if let Some(visit_number) = map_visit_number_lenient(msg) {
        builder.encounter_number(visit_number);
    }

    builder.effective_date(parse_to_datetime(query(msg, ZBE_2).ok_or_else(|| {
        Hl7MessageAccessError::MissingMessageValue("ZBE_2".to_string())
    })?)?);
    if (do_create_is_alive) {
        builder.is_alive(!is_dead);
    }

    if is_zng_present {
        // birth observation data
        set_zng_values(msg, &mut builder)?;
    }

    let result = builder
        .build()
        .map_err(|err| Hl7MappingError::BuilderError {
            builder_name: "ObservationDtoBuilder".to_string(),
            builder_error: err.to_string(),
        })?;

    Ok(Some(result))
}

fn set_zng_values(
    msg: &Message,
    builder: &mut ObservationDtoBuilder,
) -> Result<(), Hl7MappingError> {
    if let Some(head) = parse_opt(query(msg, ZNG_11))? {
        builder.head_circumference(head);
    }
    if let Some(length) = parse_opt(query(msg, ZNG_6))? {
        builder.length(length);
    }
    if let Some(weight) = parse_opt(query(msg, ZNG_7))? {
        builder.weight(weight);
    }
    Ok(())
}

/// Parst einen optionalen Feldwert zu `usize`.
fn parse_opt(value: Option<&str>) -> Result<Option<usize>, Hl7MessageParsingError> {
    value
        .map(|v| v.parse::<usize>().map_err(Hl7MessageParsingError::from))
        .transpose()
}

#[cfg(test)]
mod tests {
    use super::*;
    use adt_config::test_utils::tests::read_test_resource;

    #[test]
    fn test_map_A08() {
        let hl7 = read_test_resource("a08_test.hl7");
        let msg = Message::parse_with_lenient_newlines(&hl7, true).expect("parse hl7 failed");
        let actual = map(&msg).expect("map failed").unwrap();

        assert!(actual.is_alive.is_none());
        assert_eq!(actual.encounter_number, "88888888".to_string());
        assert_eq!(actual.length, 51.into());
        assert_eq!(actual.weight, 3390.into());
        assert_eq!(actual.head_circumference, 48.into());
    }

    #[test]
    fn test_map_A02() {
        let hl7 = read_test_resource("a02_test.hl7");
        let msg = Message::parse_with_lenient_newlines(&hl7, true).expect("parse hl7 failed");
        let actual = map(&msg).expect("map failed").unwrap();

        assert_eq!(actual.is_alive, Some(true));
        assert_eq!(actual.encounter_number, "21600000".to_string());
        assert!(actual.length.is_none());
        assert!(actual.weight.is_none());
        assert!(actual.head_circumference.is_none());
    }
}
