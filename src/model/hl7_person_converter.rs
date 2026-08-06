use crate::hl7::parser::{field_repeats, repeat_component, repeat_subcomponents};
use crate::model::person_dto::Address;
use hl7_parser::Message;

impl Address {
    pub(crate) fn from_hl7(msg: &Message) -> Vec<Option<Address>> {
        let mut res = vec![];

        if let Some(addr_repeats) = field_repeats(msg, "PID.11") {
            for addr_elem in addr_repeats {
                let mut addr = Address::new();

                // line
                if let Some(lines) = repeat_subcomponents(addr_elem, 1) {
                    addr.street_and_number =
                        lines.into_iter().map(|l| Some(l.to_string())).collect();
                }
                // city
                if let Some(city) = repeat_component(addr_elem, 3) {
                    addr.city = Some(city.to_string());
                }
                // postal code
                if let Some(postal_code) = repeat_component(addr_elem, 5) {
                    addr.zip_code = Some(postal_code.to_string());
                }
                // country
                if let Some(country) = repeat_component(addr_elem, 6) {
                    addr.country = Some(country.to_string());
                }

                if !addr.street_and_number.is_empty()
                    && addr.street_and_number.iter().all(|l| l.is_some())
                    && addr.city.is_some()
                {
                    // street must have at least 1 line and city must also have a value
                    res.push(Some(addr));
                }
            }
        }
        res
    }
}
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn address_from_hl7() {
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
        let result = Address::from_hl7(&msg);
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
