use crate::hl7::parser::{
    ENV_1, MRG_1, MessageType, PID_2, PID_5, PID_16_1, PID_29, PID_30, get_message_key,
    message_type, parse_datetime, query,
};
pub use crate::hl7::parser::{field_repeats, repeat_component, repeat_subcomponents};
use crate::hl7_error::Hl7MessageAccessError::{
    MissingMessageSegment, MissingMessageValue, UnsupportedContentError,
};
use crate::hl7_error::{Hl7MappingError, Hl7MessageAccessError};
use adt_config::config::Fhir;
use anyhow::anyhow;
use derive_builder::UninitializedFieldError;
use fhir_core::mapping::patient::create_patient_merge;
use fhir_core::model::meta::Operation::Patch;
use fhir_core::model::meta::{MappingOp, MappingOpBuilder, Operation};
use fhir_core::model::person_dto::{
    AddressDto, AddressDtoBuilder, MaritalStatusDto, PersonDto, PersonDtoBuilder,
    PersonDtoBuilderError, PersonName, PersonNameBuilder, PersonNameBuilderError,
};
use fhir_model::BuilderError;
use fhir_model::r4b::resources::Parameters;
use fhir_model::r4b::types::Identifier;
use hl7_parser::Message;

pub fn hl7_to_patient_dto(
    msg: &Message,
    mapping_op: MappingOp,
) -> Result<PersonDto, Hl7MappingError> {
    let mut binding = PersonDtoBuilder::default();
    let mut patient_builder = binding
        .names(build_names(msg)?)
        .meta(mapping_op)
        .pid(query(msg, PID_2).map(String::from).ok_or(
            Hl7MessageAccessError::MissingMessageValue("PID.2".to_string()),
        )?)
        .address(address_from_hl7(msg));

    if let Some(marital_status) = query(msg, PID_16_1) {
        patient_builder.marital_status(MaritalStatusDto::from_hl7(marital_status));
    }

    if let is_dead = query(msg, PID_30) {
        match is_dead {
            Some("J") => {
                patient_builder.is_deceased_indicator(true);
            }
            _ => {}
        }
    }
    if let Some(death_time) = query(msg, PID_29) {
        patient_builder.time_of_death(Some(parse_datetime(death_time)?));
    }

    match patient_builder.build() {
        Ok(p) => Ok(p),
        Err(e) => match e {
            PersonDtoBuilderError::UninitializedField(error_text) => {
                Err(Hl7MappingError::BuilderUninitializedFieldError {
                    details: error_text.to_string(),
                })
            }
            PersonDtoBuilderError::ValidationError(details) => {
                Err(Hl7MappingError::InputValidationError {
                    resource: "PersonDto".to_string(),
                    details,
                })
            }

            _ => {
                log::error!("build patient failed unexpectedly: {}", e);
                Err(Hl7MappingError::Other(anyhow!("{}", e)))
            }
        },
    }
}

fn build_names(v2_msg: &Message) -> Result<Vec<Option<PersonName>>, PersonNameBuilderError> {
    let mut names = vec![];

    if let Some(name_fields) = field_repeats(v2_msg, PID_5) {
        for name_field in name_fields {
            let family_name = repeat_component(name_field, 2).map(|e| e.to_string());
            let given_name = repeat_component(name_field, 1).map(|ff| ff.to_string());
            let mut builder = PersonNameBuilder::default();
            let mut name = builder
                .family(family_name)
                .given_name(vec![given_name])
                .name_prefix(repeat_component(name_field, 6).map(|e| e.to_string()))
                .name_extension(repeat_component(name_field, 4).map(|e| e.to_string()))
                .name_affix(repeat_component(name_field, 5).map(|e| e.to_string()));

            name.is_maiden(repeat_component(name_field, 7).and_then(|u| match u {
                "L" => Some(false),
                "M" | "B" => Some(true),
                _ => None,
            }));

            names.push(Some(name.build()?));
        }
    }

    Ok(names)
}

fn address_from_hl7(msg: &Message) -> Vec<Option<AddressDto>> {
    let mut res = vec![];

    if let Some(addr_repeats) = field_repeats(msg, "PID.11") {
        for addr_elem in addr_repeats {
            let mut addr_builder = AddressDtoBuilder::default();

            // line
            if let Some(lines) = repeat_subcomponents(addr_elem, 1) {
                let x: Vec<Option<String>> =
                    lines.into_iter().map(|l| Some(l.to_string())).collect();
                addr_builder.street_and_number(x);
            }
            // city
            if let Some(city) = repeat_component(addr_elem, 3) {
                addr_builder.city(Some(city.to_string()));
            }
            // postal code
            if let Some(postal_code) = repeat_component(addr_elem, 5) {
                addr_builder.zip_code(Some(postal_code.to_string()));
            }
            // country
            if let Some(country) = repeat_component(addr_elem, 6) {
                addr_builder.country(Some(country.to_string()));
            }

            if let Ok(address) = addr_builder.build() {
                // street must have at least 1 line and city must also have a value
                res.push(Some(address));
            }
        }
    }
    res
}

pub(super) fn map(msg: &Message) -> Result<Option<PersonDto>, Hl7MappingError> {
    let msg_type = message_type(msg)?;
    let id = get_message_key(msg)?.to_string();

    match msg_type {
        MessageType::A01
        | MessageType::A04
        | MessageType::A05
        | MessageType::A06
        | MessageType::A07
        | MessageType::A08
        => {
            Ok(Some(hl7_to_patient_dto(msg,MappingOp { id, operation: Operation::UpdateAsCreate })?))
        }
        MessageType::A02 | MessageType::A03 | MessageType::A31 => {
            Ok(Some(hl7_to_patient_dto(msg,MappingOp { id, operation: Operation::CreateIfNotExists })?))
        }
        MessageType::A34 | MessageType::A40 => {
            Ok(Some(create_patient_merge_hl7(msg, MappingOp { id, operation: Patch })?))
        }
        // patient stays unchanged
        MessageType::A11
        | MessageType::A12
        // At A13 no changes expected - we could update patient here,
        // but an update follows shortly after this message with another message,
        // therefore we can safely skip this on.
        | MessageType::A13
        | MessageType::A14
        | MessageType::A21
        | MessageType::A22
        | MessageType::A27
        | MessageType::A28
        | MessageType::A38 => {
            // ignore

            // A11 & A27 should not create any patient resource
            Ok(None)
        }
        MessageType::A29 => {

            // todo:  in case of mapping error fallback to a minimal delete request without resource!
            Ok(Some(hl7_to_patient_dto(msg,MappingOp{id ,operation: Operation::Delete})?))
        }
        other => Err(Hl7MappingError::from(UnsupportedContentError(other.to_string(), ENV_1.to_string()))),
    }
}

fn create_patient_merge_hl7(
    msg: &Message,
    mapping_op: MappingOp,
) -> Result<PersonDto, Hl7MappingError> {
    let replaced_patient_id = query(msg, PID_2)
        .map(String::from)
        .ok_or(MissingMessageValue("PID.2".to_string()))?;
    let new_patient_id = query(msg, MRG_1)
        .map(String::from)
        .ok_or(MissingMessageSegment("MRG.1".to_string()))?;
    Ok(PersonDtoBuilder::default()
        .replaced_by_pid(new_patient_id)
        .pid(replaced_patient_id)
        .meta(mapping_op)
        .build()?)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn address_from_hl7_test() {
        let raw_msg = r#"MSH|^~\&|ORBIS|KH|RECAPP|ORBIS|202111221030||ADT^A01|62293727|P|2.5||123456789|NE|NE||8859/1
EVN|A01|202111221030|202111221029||EIDAMN
PID|1|1499653|1499653||Test^Meinrad^^Graf^von^Dr.^L|Test|202301181003|M|||Test Str.  27^^Bad Test^^57334^D^L||02752/1672^^PH|||M|rk|||||||N||D||||N|
NK1|1|Fr. Test|14^Ehefrau||s.Pat.||||||||||U|^YYYYMMDDHHMMSS|||||||||||||||||^^^ORBIS^PN~^^^ORBIS^PI~^^^ORBIS^PT
PV1|1|I|POLPOLAMB^^^POL^POLPOL^945400^^^|R^^HL7~01^Normalfall^301||||||N||||||N|||10000001||K|||||||||||||||01||||9||||202211101359|202211101359||||||AIN1|1|102171012|KKH|KKH Allianz|^^Leipzig^^04017^D||||Ersatzkassen^13^^^1&gesetzlich|||||||Mustermann^Max||19470128|Mustergasse 10^^Musterort^^33333^D|||1|||||||201111090942||R||||||||||||M| |||||1234567890^^^^^^^20130331
PV2|||01^KH-Behandlung, vollstat.^301||||||202203040000|||||||||||||N||I||||||||||||N
IN2|1||||||||||||||||||||||||||||^PC^100^K
DG1|1||K42.9^Hernia umbilicalis ohne Einklemmung und ohne Gangrän^icd10gm2022||20230101131500|Aufn.|||||||||1|ABCDEFGH^^^^^^^^^^^^^^^^^^^^^^KCH||||12345677|U
DG1|2||Z11^Spezielle Verfahren zur Untersuchung auf infektiöse und parasitäre Krankheiten^icd10gm2022||20230101131500|Entl.|||||||||2.1|ABCDEFGH^^^^^^^^^^^^^^^^^^^^^^KCH||||12345678|U
DG1|3||U99.0!^Spezielle Verfahren zur Untersuchung auf SARS-CoV-2^icd10gm2022||20230101131500|Entl.|||||||||2.2|ABCDEFGH^^^^^^^^^^^^^^^^^^^^^^KCH||||12345679|U
ZBE|30674176^ORBIS|202208221309||INSERT
ZNG||||||35|
"#;

        let msg = Message::parse_with_lenient_newlines(raw_msg, true).unwrap();
        let result = address_from_hl7(&msg);
        assert_eq!(result.len(), 1);
        let address = result.first().unwrap().clone().unwrap();
        assert_eq!(address.zip_code, Some("57334".to_string()));
        assert_eq!(address.city, Some("Bad Test".to_string()));
        assert_eq!(address.country, Some("D".to_string()));
        assert_eq!(
            address.street_and_number.first().unwrap().clone(),
            Some("Test Str.  27".to_string())
        );
    }
}
