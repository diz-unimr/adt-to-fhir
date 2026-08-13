pub use crate::hl7::parser::{field_repeats, repeat_component, repeat_subcomponents};
use anyhow::anyhow;

use crate::hl7::parser::{PID_2, query};

use crate::hl7_error::{Hl7MappingError, Hl7MessageAccessError};
use fhir_core::model::person_dto::{
    AddressDto, AddressDtoBuilder, PersonDto, PersonDtoBuilder, PersonDtoBuilderError,
};
use hl7_parser::Message;

pub fn map_hl7_to_dto(msg: &Message) -> Result<PersonDto, Hl7MappingError> {
    let mut patient_builder = PersonDtoBuilder::default();

    patient_builder.pid(query(msg, PID_2).map(String::from).ok_or(
        Hl7MessageAccessError::MissingMessageValue("PID.2".to_string()),
    )?);

    patient_builder.address(address_from_hl7(msg));

    match patient_builder.build() {
        Ok(p) => Ok(p),
        Err(e) => match e {
            PersonDtoBuilderError::UninitializedField(error_text) => {
                Err(Hl7MappingError::BuilderUninitializedFieldError {
                    builder_name: "PersonDtoBuilder".to_string(),
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
