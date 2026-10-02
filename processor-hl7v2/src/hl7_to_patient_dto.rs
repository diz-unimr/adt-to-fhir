use crate::hl7::parser::{
    ENV_1, MRG_1, MessageType, PID_2, PID_5, PID_16_1, PID_24, PID_25, PID_29, PID_30,
    get_message_key, message_type, parse_datetime, parse_naive_date, query, segment_value,
};
pub use crate::hl7::parser::{field_repeats, repeat_component, repeat_subcomponents};
use crate::hl7_error::Hl7MappingError;
use crate::hl7_error::Hl7MessageAccessError::{
    MissingMessageSegment, MissingMessageValue, UnsupportedContentError,
};
use anyhow::anyhow;
use fhir_core::mapping::patient_mapper::is_valid_gkv10;
use fhir_core::model::meta::ProcessingOperation::Patch;
use fhir_core::model::meta::{MappingOp, ProcessingOperation};
use fhir_core::model::person_dto::{
    AddressDto, AddressDtoBuilder, Insurance, InsuranceBuilder, InsuranceType, MaritalStatusDto,
    PersonDto, PersonDtoBuilder, PersonDtoBuilderError, PersonName, PersonNameBuilder,
    PersonNameBuilderError,
};

use hl7_parser::Message;
use hl7_parser::message::Segment;
use log::{Level, log};

pub fn hl7_to_patient_dto(
    msg: &Message,
    mapping_op: MappingOp,
) -> Result<PersonDto, Hl7MappingError> {
    let mut binding = PersonDtoBuilder::default();
    let patient_builder = binding
        .names(build_names(msg)?)
        .meta(mapping_op)
        .insurance(insurance_hl7(msg)?)
        .pid(
            query(msg, PID_2)
                .map(String::from)
                .ok_or(MissingMessageValue("PID.2".to_string()))?,
        )
        .address(address_from_hl7(msg));

    if let Some(marital_status) = query(msg, PID_16_1) {
        patient_builder.marital_status(MaritalStatusDto::from_hl7(marital_status));
    }

    if let Some("J") = query(msg, PID_30) {
        patient_builder.is_deceased_indicator(true);
    }
    if let Some(death_time) = query(msg, PID_29) {
        patient_builder.time_of_death(Some(parse_datetime(death_time)?));
    }

    if let Some("J") = query(msg, PID_24) {
        patient_builder.is_multiple_birth(true);
    }
    if let Some("N") = query(msg, PID_24) {
        patient_builder.is_multiple_birth(false);
    }
    if let Some(multi_birth_number) = query(msg, PID_25)
        && let Ok(birth_order) = multi_birth_number.parse::<u32>()
    {
        patient_builder.multiple_birth_order(Some(birth_order));
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
            let family_name = repeat_component(name_field, 1).map(|e| e.to_string());
            let given_name = repeat_component(name_field, 2).map(|ff| ff.to_string());
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

pub fn map(msg: &Message) -> Result<Option<PersonDto>, Hl7MappingError> {
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
            Ok(Some(hl7_to_patient_dto(msg,MappingOp { id, operation: ProcessingOperation::UpdateAsCreate })?))
        }
        MessageType::A02 | MessageType::A03 | MessageType::A31 => {
            Ok(Some(hl7_to_patient_dto(msg,MappingOp { id, operation: ProcessingOperation::CreateIfNotExists })?))
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
            Ok(Some(hl7_to_patient_dto(msg,MappingOp{id ,operation: ProcessingOperation::Delete})?))
        }
        other => Err(Hl7MappingError::from(UnsupportedContentError(other.to_string(), ENV_1.to_string()))),
    }
}

fn create_patient_merge_hl7(
    msg: &Message,
    mapping_op: MappingOp,
) -> Result<PersonDto, Hl7MappingError> {
    let new_patient_id = query(msg, PID_2)
        .map(String::from)
        .ok_or(MissingMessageValue("PID.2".to_string()))?;
    let old_pid = query(msg, MRG_1)
        .map(String::from)
        .ok_or(MissingMessageSegment(MRG_1.to_string()))?;
    Ok(PersonDtoBuilder::default()
        .replaced_by_pid(new_patient_id)
        .pid(old_pid)
        .meta(mapping_op)
        .build()?)
}

fn insurance_hl7(msg: &Message) -> Result<Vec<Option<Insurance>>, Hl7MappingError> {
    // extract optional insurance data for mapping
    msg.segments
        .iter()
        .filter(|s| s.name == "IN1")
        .map(|s| map_versicherungsdaten(s))
        .collect::<Result<Vec<Option<Insurance>>, Hl7MappingError>>()
}

fn map_versicherungsdaten(in1: &Segment) -> Result<Option<Insurance>, Hl7MappingError> {
    let mut result = InsuranceBuilder::default();
    // Versicherungsnummer
    let insurance_number = match in1.field(36) {
        Some(f) if !f.is_empty() => f.raw_value(),
        _ => return Ok(None),
    };

    // set assigner
    match segment_value(in1, 3, 1, 1) {
        None => {
            log!(
                Level::Warn,
                "for insurance '{}' no insurance company id found - \
            cannot add assigner",
                insurance_number
            )
        }
        Some(id) => {
            // assigner id - insurance company id
            result.assigner_id(id);
        }
    };
    result.insurance_number(insurance_number);

    if is_valid_gkv10(insurance_number) {
        result.insurance_type(InsuranceType::GKV_PKV);
    } else {
        // OTHER INSURANCE NUMBER! vor 2012 waren 9 - 12 Stellen ohne führenden Buchstaben valide.
        result.insurance_type(InsuranceType::Other);
    }

    if let Some(start) = in1
        .field(12)
        .filter(|f| !f.is_empty())
        .map(|f| parse_naive_date(f.raw_value()))
        .transpose()?
    {
        result.valid_from(start);
    }

    if let Some(end) = in1
        .field(13)
        .filter(|f| !f.is_empty())
        .map(|f| parse_naive_date(f.raw_value()))
        .transpose()?
    {
        result.valid_to(end);
    }

    Ok(Some(result.build()?))
}

#[cfg(test)]
mod tests {
    use super::*;

    use adt_config::test_utils::tests::get_test_config;
    use fhir_core::mapping::patient_mapper::{map_addresses_dto, map_name};
    use fhir_model::Date;
    use fhir_model::DateTime;

    use fhir_model::r4b::codes::{AddressType, IdentifierUse, NameUse};
    use fhir_model::r4b::resources::{
        ParametersParameter, ParametersParameterValue, PatientMultipleBirth, ResourceType,
    };
    use fhir_model::r4b::types::{
        Address, CodeableConcept, Coding, HumanName, Identifier, Period, Reference,
    };
    use fhir_model::time;
    use hl7_parser::Message;
    use rstest::rstest;

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

    #[test]
    fn test_multibirth_empty() {
        let msg = Message::parse_with_lenient_newlines(r#"MSH|^~\&|ORBIS|KH|WEBEPA|KH|20251102212117||ADT^A08^ADT_A01|12332112|P|2.5||123788998|NE|NE||8859/1
EVN|A08|202511022120||11036_123456789|ZZZZZZZZ|202511022120
PID|1|9999999|9999999|88888888|Nachname^SäuglingVorname^^^^^L||202511022120|M|||Strasse. 1&Strasse.&1^^Stadt^^30000^DE^L~^^Stadt^^^^BDL||0000000000000^PRN^PH^^^00000^0000000^^^^^000000000000|||U|||||12345678^^^KH^VN~1234567^^^KH^PT||Stadt|||DE||||N"#, true).unwrap();
        let actual = map(&msg).unwrap().unwrap();
        assert_eq!(actual.is_multiple_birth, None);
        assert_eq!(actual.multiple_birth_order, None);
    }

    #[rstest]
    #[case("J", "", Some(true))]
    #[case("N", "", Some(false))]
    #[case("", "", None)]
    fn test_multibirth_bool(
        #[case] multibirth_flag: String,
        #[case] multibirth_num: String,
        #[case] expect_bool_result: Option<bool>,
    ) {
        let input = format!(
            r#"MSH|^~\&|ORBIS|KH|WEBEPA|KH|20251102212117||ADT^A08^ADT_A01|12332112|P|2.5||123788998|NE|NE||8859/1
EVN|A08|202511022120||11036_123456789|ZZZZZZZZ|202511022120
PID|1|9999999|9999999|88888888|Nachname^SäuglingVorname^^^^^L||202511022120|M|||Strasse. 1&Strasse.&1^^Stadt^^30000^DE^L~^^Stadt^^^^BDL||0000000000000^PRN^PH^^^00000^0000000^^^^^000000000000|||U|||||12345678^^^KH^VN~1234567^^^KH^PT||Stadt|{}|{}|DE||||N"#,
            multibirth_flag, multibirth_num
        );
        let msg = Message::parse_with_lenient_newlines(&input, true).unwrap();
        let actual = fhir_core::mapping::patient_mapper::map_patient(
            &map(&msg).unwrap().unwrap(),
            &get_test_config(),
        )
        .unwrap()
        .multiple_birth
        .clone();

        match expect_bool_result {
            Some(true) => {
                assert_eq!(actual, Some(PatientMultipleBirth::Boolean(true)));
            }
            Some(false) => {
                assert_eq!(actual, Some(PatientMultipleBirth::Boolean(false)));
            }
            None => {
                assert!(actual.is_none());
            }
        }
    }

    #[test]
    fn test_multibirth_valid() {
        let input = r#"MSH|^~\&|ORBIS|KH|WEBEPA|KH|20251102212117||ADT^A08^ADT_A01|12332112|P|2.5||123788998|NE|NE||8859/1
EVN|A08|202511022120||11036_123456789|ZZZZZZZZ|202511022120
PID|1|9999999|9999999|88888888|Nachname^SäuglingVorname^^^^^L||202511022120|M|||Strasse. 1&Strasse.&1^^Stadt^^30000^DE^L~^^Stadt^^^^BDL||0000000000000^PRN^PH^^^00000^0000000^^^^^000000000000|||U|||||12345678^^^KH^VN~1234567^^^KH^PT||Stadt|J||DE||||N"#;
        let msg = Message::parse_with_lenient_newlines(&input, true).unwrap();
        let actual = fhir_core::mapping::patient_mapper::map_patient(
            &map(&msg).unwrap().unwrap(),
            &get_test_config(),
        )
        .unwrap()
        .multiple_birth
        .clone();

        assert_eq!(actual, Some(PatientMultipleBirth::Boolean(true)));

        let input = r#"MSH|^~\&|ORBIS|KH|WEBEPA|KH|20251102212117||ADT^A08^ADT_A01|12332112|P|2.5||123788998|NE|NE||8859/1
EVN|A08|202511022120||11036_123456789|ZZZZZZZZ|202511022120
PID|1|9999999|9999999|88888888|Nachname^SäuglingVorname^^^^^L||202511022120|M|||Strasse. 1&Strasse.&1^^Stadt^^30000^DE^L~^^Stadt^^^^BDL||0000000000000^PRN^PH^^^00000^0000000^^^^^000000000000|||U|||||12345678^^^KH^VN~1234567^^^KH^PT||Stadt|N||DE||||N"#;
        let msg = Message::parse_with_lenient_newlines(&input, true).unwrap();
        let actual = fhir_core::mapping::patient_mapper::map_patient(
            &map(&msg).unwrap().unwrap(),
            &get_test_config(),
        )
        .unwrap()
        .multiple_birth
        .clone();

        assert_eq!(actual, Some(PatientMultipleBirth::Boolean(false)));

        let input = r#"MSH|^~\&|ORBIS|KH|WEBEPA|KH|20251102212117||ADT^A08^ADT_A01|12332112|P|2.5||123788998|NE|NE||8859/1
EVN|A08|202511022120||11036_123456789|ZZZZZZZZ|202511022120
PID|1|9999999|9999999|88888888|Nachname^SäuglingVorname^^^^^L||202511022120|M|||Strasse. 1&Strasse.&1^^Stadt^^30000^DE^L~^^Stadt^^^^BDL||0000000000000^PRN^PH^^^00000^0000000^^^^^000000000000|||U|||||12345678^^^KH^VN~1234567^^^KH^PT||Stadt|J|2|DE||||N"#;
        let msg = Message::parse_with_lenient_newlines(&input, true).unwrap();
        let actual = fhir_core::mapping::patient_mapper::map_patient(
            &map(&msg).unwrap().unwrap(),
            &get_test_config(),
        )
        .unwrap()
        .multiple_birth
        .clone();

        assert_eq!(actual, Some(PatientMultipleBirth::Integer(2)));
    }

    #[rstest]
    #[case("J", "a")]
    #[should_panic]
    fn test_multibirth_number_fail(
        #[case] multibirth_flag: String,
        #[case] multibirth_num: String,
    ) {
        let input = format!(
            r#"MSH|^~\&|ORBIS|KH|WEBEPA|KH|20251102212117||ADT^A08^ADT_A01|12332112|P|2.5||123788998|NE|NE||8859/1
EVN|A08|202511022120||11036_123456789|ZZZZZZZZ|202511022120
PID|1|9999999|9999999|88888888|Nachname^SäuglingVorname^^^^^L||202511022120|M|||Strasse. 1&Strasse.&1^^Stadt^^30000^DE^L~^^Stadt^^^^BDL||0000000000000^PRN^PH^^^00000^0000000^^^^^000000000000|||U|||||12345678^^^KH^VN~1234567^^^KH^PT||Stadt|{}|{}|DE||||N"#,
            multibirth_flag, multibirth_num
        );

        let msg = Message::parse_with_lenient_newlines(&input, true).unwrap();
        let actual = fhir_core::mapping::patient_mapper::map_patient(
            &map(&msg).unwrap().unwrap(),
            &get_test_config(),
        );

        match actual {
            Err(ContentError) => {}
            Ok(_) => panic!("should fail since birth number is no number"),
        }
    }

    #[test]
    fn test_multibirth_number_valid_number() {
        let input = r#"MSH|^~\&|ORBIS|KH|WEBEPA|KH|20251102212117||ADT^A08^ADT_A01|12332112|P|2.5||123788998|NE|NE||8859/1
EVN|A08|202511022120||11036_123456789|ZZZZZZZZ|202511022120
PID|1|9999999|9999999|88888888|Nachname^SäuglingVorname^^^^^L||202511022120|M|||Strasse. 1&Strasse.&1^^Stadt^^30000^DE^L~^^Stadt^^^^BDL||0000000000000^PRN^PH^^^00000^0000000^^^^^000000000000|||U|||||12345678^^^KH^VN~1234567^^^KH^PT||Stadt|J|1|DE||||N"#;

        let msg = Message::parse_with_lenient_newlines(&input, true).unwrap();
        let actual = fhir_core::mapping::patient_mapper::map_patient(
            &map(&msg).unwrap().unwrap(),
            &get_test_config(),
        );

        match actual {
            Err(ContentError) => {
                panic!("should not fail since birth number and flag are valid")
            }
            Ok(p) => assert_eq!(p.multiple_birth, Some(PatientMultipleBirth::Integer(1))),
        }
    }

    #[test]
    fn test_create_patient_merge() {
        let config = &get_test_config();

        let msg =
                Message::parse_with_lenient_newlines(r#"MSH|^~\&|ORBIS|KH|WEBEPA|KH|20230912105234||ADT^A40^ADT_A39|12345678|P|2.5||123456789|NE|NE||8859/1
EVN|A40|202309121052||00000_123456789|XXXXX|202309121052
PID|1|1234567|1234567||Musterfrau^Maxi^^^^^L|||F|||^^^^^^L||^ ^ ^^^^^^^^^|||U||||||||||DE||||N
MRG|09876543|||09876543|||Musterfrau^Maxi^^^^^L"#, true)
                    .unwrap();

        // act
        let (params, _) = fhir_core::mapping::patient_mapper::create_patient_merge_dto(
            &create_patient_merge_hl7(
                &msg,
                MappingOp {
                    id: "42".to_string(),
                    operation: Patch,
                },
            )
            .unwrap(),
            config,
        )
        .unwrap()
        .unwrap();

        // get value parameters from result
        let values: Vec<ParametersParameter> = params
            .parameter
            .iter()
            .flatten()
            .filter_map(|p| {
                if p.name == "operation" {
                    Some(p.part.iter().flatten())
                } else {
                    None
                }
            })
            .flatten()
            .find_map(|p| {
                if p.name == "value" {
                    Some(p.part.clone().into_iter().flatten().collect())
                } else {
                    None
                }
            })
            .unwrap();

        let other = values.first().unwrap();
        let m_type = values.get(1).unwrap();

        assert_eq!(
                *other,
                ParametersParameter::builder()
                    .name("other".to_string())
                    .value(ParametersParameterValue::Reference(
                        Reference::builder()
                            .r#type(ResourceType::Patient.to_string())
                            .reference("Patient?identifier=https://fhir.diz.uni-marburg.de/sid/patient-id|1234567".to_string())
                            .build()
                            .unwrap()
                    ))
                    .build()
                    .unwrap()
            );

        assert_eq!(
            *m_type,
            ParametersParameter::builder()
                .name("type".to_string())
                .value(ParametersParameterValue::Code("replaced-by".to_string()))
                .build()
                .unwrap()
        );
    }

    #[test]
    fn test_map_versicherungsdaten() {
        let msg = Message::parse_with_lenient_newlines(r#"MSH|^~\&|ORBIS||RECAPP|ORBIS|201111280725||ADT^A04|11657277|P|2.5|||||DE||DE
EVN|A04|201111280722|201111280722||TEST
PID|1|111111|111111||Mustermann^Max|Mustermann|19500118|M|||Mustergasse 10^^Musterort^^33333^DE||012345/12346^^PH|||M|kl|||||||N||DE
NK1|1|Fr. Müller, Miriam|14^Ehefrau| |s.Pat.
PV1|1|O|NEPPOLAMB^^^NEP^NEP^000000|R||||44444ARZT^Arzt^Hans Jürgen^^Praxis^^Dr. med.|44444ARZT^Arzt^Hans Jürgen^^Praxis^^Dr. med.|N||||||N|||20900000||K|||HSA||||||||||||||||9||||200703280736|||||||A
IN1|1||555555555^^^^NII~22222^^^^NIIP~AOK|AOK - Die Gesundheitskasse in Hessen-|Musterstrasse 1^^Musterort^^66666^D||||AOK^1^^^1&gesetzlich|||20020120|20091231||50001|Mustermann^Max||19500118|Mustergasse 10^^Musterort^^33333^D|||2|||||||201108220723||R|||||A454874316|||||||M| ^^^^^D  |||||A454874316^^^^^^^20150630
"#, true).unwrap();

        let actual = fhir_core::mapping::patient_mapper::map_patient(
            &map(&msg).unwrap().unwrap(),
            &get_test_config(),
        )
        .unwrap()
        .identifier
        .get(1)
        .unwrap()
        .clone()
        .unwrap();

        // expected identifier
        let expected = Identifier::builder()
            .system("http://fhir.de/sid/gkv/kvid-10".into())
            .value("A454874316".into())
            .r#use(IdentifierUse::Official)
            .r#type(
                CodeableConcept::builder()
                    .coding(vec![Some(
                        Coding::builder()
                            .system("http://fhir.de/CodeSystem/identifier-type-de-basis".into())
                            .code("KVZ10".into())
                            .build()
                            .unwrap(),
                    )])
                    .build()
                    .unwrap(),
            )
            .period(
                Period::builder()
                    // IN-12
                    .start(DateTime::Date(Date::Date(
                        time::Date::from_calendar_date(2002, time::Month::January, 20).unwrap(),
                    )))
                    // IN-13
                    .end(DateTime::Date(Date::Date(
                        time::Date::from_calendar_date(2009, time::Month::December, 31).unwrap(),
                    )))
                    .build()
                    .unwrap(),
            )
            .assigner(
                Reference::builder()
                    .identifier(
                        Identifier::builder()
                            .system("http://fhir.de/sid/arge-ik/iknr".into())
                            .value("555555555".into())
                            .r#use(IdentifierUse::Official)
                            .r#type(
                                CodeableConcept::builder()
                                    .coding(vec![Some(
                                        Coding::builder()
                                            .system(
                                                "http://terminology.hl7.org/CodeSystem/v2-0203"
                                                    .into(),
                                            )
                                            .code("XX".into())
                                            .build()
                                            .unwrap(),
                                    )])
                                    .build()
                                    .unwrap(),
                            )
                            .build()
                            .unwrap(),
                    )
                    .build()
                    .unwrap(),
            )
            .build()
            .unwrap();

        assert_eq!(actual, expected);
    }

    #[test]
    fn test_map_insurance_skip_none() {
        let msg = Message::parse_with_lenient_newlines(
                r#"MSH|^~\&|ORBIS||RECAPP|ORBIS|201111280725||ADT^A04|11657277|P|2.5|||||DE||DE
EVN|A04|201111280722|201111280722||TEST
PID|1|111111|111111||Mustermann^Max|Mustermann|19500118|M|||Mustergasse 10^^Musterort^^33333^DE||012345/12346^^PH|||M|kl|||||||N||DE
NK1|1|Fr. Müller, Miriam|14^Ehefrau| |s.Pat.
PV1|1|O|NEPPOLAMB^^^NEP^NEP^000000|R||||44444ARZT^Arzt^Hans Jürgen^^Praxis^^Dr. med.|44444ARZT^Arzt^Hans Jürgen^^Praxis^^Dr. med.|N||||||N|||20900000||K|||HSA||||||||||||||||9||||200703280736|||||||A
IN1|1||666666666^^^^NII~BG BAU MITTE^^^^XX|BG der Bauwirtschaft - BV Mitte|Viktoriastr. 21&Viktoriastr.&21^^Wuppertal^^42115^DE^L||12345612^PRN^PH^^^0000^3333^^^^^12345612~11111111111^PRN^FX^^^0000^1111111^^^^^11111111111||Träger der ges. Unfallversicherer^26^^^2&Berufsgenossenschaft^^NII~Träger der ges. Unfallversicherer^26^^^^^U|||||||Max^Mustermann||19620115|Musterstreasse. 1&Musterstreasse.&1^^Berlin^^10115^DE^L|||N|||||||||M||||||||||||M|Musterstreasse. 1&Musterstreasse.&1^^Berlin^^10115^DE^L
IN2|1||12345TES^TEST GmbH||||||||||||||||||||||||||^PC^0.0||||DE|||N|||kl|||||||Beruf-des-Pateinten|||||||||||||||||0123 45678|||||||Test GmbH
IN1|2||777777777^^^^NII~BG HM HAUPT^^^^XX|BGHM - Hauptverwaltung|Musterstreasse. 1&Musterstreasse.&1^^Berlin^^10115^DE^L||000000000001^PRN^PH^^^0000^0000^^^^^000000000001~1313131331313^PRN^FX^^^00000^00000000^^^^^1313131331313||Träger der ges. Unfallversicherer^26^^^2&Berufsgenossenschaft^^NII~Träger der ges. Unfallversicherer^26^^^^^U||||||10001|Max^Mustermann||19620115|Musterstreasse. 1&Musterstreasse.&1^^Berlin^^10115^DE^L|||H|||||||||M||||||||||||M|Musterstreasse. 1&Musterstreasse.&1^^Berlin^^10115^DE^L
IN2|2||12345TES^TEST GmbH||||||||||||||||||||||||||^PC^0.0||||DE|||N|||kl|||||||Beruf-des-Pateinten|||||||||||||||||0123 45678|||||||Test GmbH
IN1|3||8888888888^^^^NII~P DEMO^^^^XX|Krankenversicherung a.G.|Musterstreasse. 1&Musterstreasse.&1^^Berlin^^10115^DE^L||0000000-0^PRN^PH^^^0000^111-0^^^^^0000000-0~0000000-2913^PRN^FX^^^0000^111-2913^^^^^0000000-2913~^NET^Internet^info@email.de||Private^6^^^8&Private Krankenkasse^^NII~Private^6^^^^^U|||||||Max^Mustermann||19620115|Musterstreasse. 1&Musterstreasse.&1^^Berlin^^10115^DE^L|||N|||||||||P|||||123123123|||||||M|Musterstreasse. 1&Musterstreasse.&1^^Berlin^^10115^DE^L|||||123123123^^^^^^^0236
IN2|3|123123123|12345TES^TEST GmbH||||||||||||||||||||||||||||||DE|||N|||kl|||||||Beruf-des-Pateinten|||||||||||||||||0123 45678|||||||Test GmbH
IN1|4||SELBST^^^^XX|Selbstzahler|Musterstreasse. 1&Musterstreasse.&1^^Berlin^^10115^DE^L||00000000^PRN^PH^^^000^000^^^^^00000000~00000000000^PRN^CP^^^0000^0000000^^^^^00000000000||Sonstige^5^^^6&Selbstzahler^^NII~Sonstige^5^^^^^U|||||||Max^Mustermann||19620115|Musterstreasse. 1&Musterstreasse.&1^^Berlin^^10115^DE^L|||N|||J|20251207|||||P||||||||||||M|Musterstreasse. 1&Musterstreasse.&1^^Berlin^^10115^DE^L
IN2|4||12345TES^TEST GmbH||||||||||||||||||||||||||^PC^0.0||||DE|||N|||kl|||||||Beruf-des-Pateinten|||||||||||||||||0123 45678|||||||Test GmbH
"#,
                true,
            ).unwrap();

        let actual = fhir_core::mapping::patient_mapper::map_patient(
            &map(&msg).unwrap().unwrap(),
            &get_test_config(),
        )
        .unwrap()
        .identifier
        .clone();
        assert_eq!(actual.len(), 2);
    }

    #[test]
    fn test_patient_multiple_insurance_select_kvid() {
        let msg = Message::parse_with_lenient_newlines(r#"MSH|^~\&|ORBIS||RECAPP|ORBIS|201111280725||ADT^A04|11657277|P|2.5|||||DE||DE
EVN|A04|201111280722|201111280722||TEST
PID|1|111111|111111||Mustermann^Max|Mustermann|19500118|M|||Mustergasse 10^^Musterort^^33333^DE||012345/12346^^PH|||M|kl|||||||N||DE
NK1|1|Fr. Müller, Miriam|14^Ehefrau| |s.Pat.
PV1|1|O|NEPPOLAMB^^^NEP^NEP^000000|R||||44444ARZT^Arzt^Hans Jürgen^^Praxis^^Dr. med.|44444ARZT^Arzt^Hans Jürgen^^Praxis^^Dr. med.|N||||||N|||20900000||K|||HSA||||||||||||||||9||||200703280736|||||||A
IN1|1||8888888888^^^^NII~P DEMO^^^^XX|AOK Hessen|^^Marburg^^35039^D||||AOK^1^^^1&gesetzlich||||||50001|||||||1|||||||||R|||||454874316|||||||U|
IN2|1||||||||||||||||||||||||||||^PC^0^K
IN1|2|00000001|5555555^^^^NII~P DEMO^^^^XX|AOK - Die Gesundheitskasse in Hessen-|Musterstrasse 1^^Musterort^^66666^D||||AOK^1^^^1&gesetzlich|||20091231|||50001|Mustermann^Max||19500118|Mustergasse 10^^Musterort^^33333^D|||2|||||||||R|||||K454874316|||||||M| ^^^^^D  |||||K454874316^^^^^^^20150630
IN2|2||R^Rentner||||||||||||||||||||||||||^PC^0^K"#, true).unwrap();
        let config = &get_test_config();

        let actual = fhir_core::mapping::patient_mapper::map_patient(
            &map(&msg).unwrap().unwrap(),
            &get_test_config(),
        )
        .unwrap()
        .identifier
        .clone();

        assert_eq!(actual.len(), 2);

        assert_eq!(
            "K454874316",
            actual[1].as_ref().unwrap().value.as_ref().unwrap(),
            "expect KVID10 IN1 value, since it is valid"
        );
        assert_eq!(
            "http://fhir.de/sid/gkv/kvid-10",
            actual[1].as_ref().unwrap().system.as_ref().unwrap()
        );

        assert_eq!(
            "5555555",
            actual[1]
                .as_ref()
                .unwrap()
                .assigner
                .as_ref()
                .unwrap()
                .identifier
                .as_ref()
                .unwrap()
                .value
                .as_ref()
                .unwrap()
        );
    }

    #[test]
    fn test_patient_multiple_insurance_kvid_is_outdated() {
        let msg = Message::parse_with_lenient_newlines(r#"MSH|^~\&|ORBIS||RECAPP|ORBIS|201111280725||ADT^A04|11657277|P|2.5|||||DE||DE
EVN|A04|201111280722|201111280722||TEST
PID|1|111111|111111||Mustermann^Max|Mustermann|19500118|M|||Mustergasse 10^^Musterort^^33333^DE||012345/12346^^PH|||M|kl|||||||N||DE
NK1|1|Fr. Müller, Miriam|14^Ehefrau| |s.Pat.
PV1|1|O|NEPPOLAMB^^^NEP^NEP^000000|R||||44444ARZT^Arzt^Hans Jürgen^^Praxis^^Dr. med.|44444ARZT^Arzt^Hans Jürgen^^Praxis^^Dr. med.|N||||||N|||20900000||K|||HSA||||||||||||||||9||||200703280736|||||||A
IN1|1||8888888888^^^^NII~P DEMO^^^^XX|AOK Hessen|^^Marburg^^35039^D||||AOK^1^^^1&gesetzlich||||||50001|||||||1|||||||||R|||||454874316|||||||U|
IN2|1||||||||||||||||||||||||||||^PC^0^K
IN1|2|00000001|5555555^^^^NII~P DEMO^^^^XX|AOK - Die Gesundheitskasse in Hessen-|Musterstrasse 1^^Musterort^^66666^D||||AOK^1^^^1&gesetzlich||||20091231||50001|Mustermann^Max||19500118|Mustergasse 10^^Musterort^^33333^D|||2|||||||||R|||||K454874316|||||||M| ^^^^^D  |||||K454874316^^^^^^^20150630
IN2|2||R^Rentner||||||||||||||||||||||||||^PC^0^K"#, true).unwrap();
        let config = &get_test_config();
        let identifiers = fhir_core::mapping::patient_mapper::map_patient(
            &map(&msg).unwrap().unwrap(),
            &get_test_config(),
        )
        .unwrap()
        .identifier
        .clone();

        assert_eq!(identifiers.len(), 2);

        assert_eq!(
            "454874316",
            identifiers[1].as_ref().unwrap().value.as_ref().unwrap(),
            "expect first IN1 segment, since KVID10 is outdated"
        );
        assert_eq!(
            &config.person.other_insurance_system,
            identifiers[1].as_ref().unwrap().system.as_ref().unwrap()
        );

        assert_eq!(
            "8888888888",
            identifiers[1]
                .as_ref()
                .unwrap()
                .assigner
                .as_ref()
                .unwrap()
                .identifier
                .as_ref()
                .unwrap()
                .value
                .as_ref()
                .unwrap()
        );
    }

    #[test]
    fn test_patient_multiple_insurance_select_second_first_is_outdated() {
        let msg = Message::parse_with_lenient_newlines(r#"MSH|^~\&|ORBIS||RECAPP|ORBIS|201111280725||ADT^A04|11657277|P|2.5|||||DE||DE
EVN|A04|201111280722|201111280722||TEST
PID|1|111111|111111||Mustermann^Max|Mustermann|19500118|M|||Mustergasse 10^^Musterort^^33333^DE||012345/12346^^PH|||M|kl|||||||N||DE
NK1|1|Fr. Müller, Miriam|14^Ehefrau| |s.Pat.
PV1|1|O|NEPPOLAMB^^^NEP^NEP^000000|R||||44444ARZT^Arzt^Hans Jürgen^^Praxis^^Dr. med.|44444ARZT^Arzt^Hans Jürgen^^Praxis^^Dr. med.|N||||||N|||20900000||K|||HSA||||||||||||||||9||||200703280736|||||||A
IN1|1||8888888888^^^^NII~P DEMO^^^^XX|AOK Hessen|^^Marburg^^35039^D||||AOK^1^^^1&gesetzlich||||20110518||50001|||||||1|||||||||R|||||123456789|||||||U|
IN2|1||||||||||||||||||||||||||||^PC^0^K
IN1|2|00000001|5555555^^^^NII~P DEMO^^^^XX|AOK - Die Gesundheitskasse in Hessen-|Musterstrasse 1^^Musterort^^66666^D||||AOK^1^^^1&gesetzlich||||||50001|Mustermann^Max||19500118|Mustergasse 10^^Musterort^^33333^D|||2|||||||||R|||||454874316|||||||M| ^^^^^D  |||||454874316^^^^^^^20150630
IN2|2||R^Rentner||||||||||||||||||||||||||^PC^0^K"#, true).unwrap();
        let config = &get_test_config();
        let identifiers = fhir_core::mapping::patient_mapper::map_patient(
            &map(&msg).unwrap().unwrap(),
            &get_test_config(),
        )
        .unwrap()
        .identifier
        .clone();

        assert_eq!(identifiers.len(), 2);

        assert_eq!(
            "454874316",
            identifiers[1].as_ref().unwrap().value.as_ref().unwrap(),
            "expect second IN1 segment, since first IN1 is outdated - and no KVID10 is available"
        );
        assert_eq!(
            &config.person.other_insurance_system,
            identifiers[1].as_ref().unwrap().system.as_ref().unwrap()
        );

        assert_eq!(
            "5555555",
            identifiers[1]
                .as_ref()
                .unwrap()
                .assigner
                .as_ref()
                .unwrap()
                .identifier
                .as_ref()
                .unwrap()
                .value
                .as_ref()
                .unwrap()
        );
    }

    #[test]
    fn test_try_set_identifier_expect_error() {
        let raw = r#"MSH|^~\&|ORBIS|KH|WEBEPA|KH|20251102212117||ADT^A08^ADT_A01|12332112|P|2.5||123788998|NE|NE||8859/1
EVN|A08|202511022120||11036_123456789|ZZZZZZZZ|202511022120
PID|1|1212121|1212121|21600000|Sokolovski, Malina||19820101101139|F|||Hexengasse 1^^Traumstadt^^12345^D^L~Wettergasse 42^^Wetter^^54321^D^L||012345/1234^^PH~0123451234^^CP~max-muster.mann@web.de^^X.400|||S|ev||||12345~23456|||||D||||N
IN1|1||777777777^^^^NII~AOK HESSEN^^^^XX|AOK Hessen|Strasse 1&Strasse&1^^Stadt^^123456^DE^L||||AOK^1^^^1&gesetzliche Krankenkasse^^NII~AOK^1^^^^^U|||20011344||||||||||H|||||||||M|||||X000000000|||||||F||||||X000000000
IN2|1|X000000000|||||||||||||||||||||||||||^PC^100.0||||DE|||N||||||||||||||||||||||||||||||||||"#;

        let msg = Message::parse_with_lenient_newlines(&raw, true).unwrap();

        let data = &map(&msg);
        assert!(data.is_err(), "IN1.12 is invalid date format");

        let raw = r#"MSH|^~\&|ORBIS|KH|WEBEPA|KH|20251102212117||ADT^A08^ADT_A01|12332112|P|2.5||123788998|NE|NE||8859/1
EVN|A08|202511022120||11036_123456789|ZZZZZZZZ|202511022120
PID|1|1212121|1212121|21600000|Sokolovski, Malina||19820101101139|F|||Hexengasse 1^^Traumstadt^^12345^D^L~Wettergasse 42^^Wetter^^54321^D^L||012345/1234^^PH~0123451234^^CP~max-muster.mann@web.de^^X.400|||S|ev||||12345~23456|||||D||||N
IN1|1||777777777^^^^NII~AOK HESSEN^^^^XX|AOK Hessen|Strasse 1&Strasse&1^^Stadt^^123456^DE^L||||AOK^1^^^1&gesetzliche Krankenkasse^^NII~AOK^1^^^^^U||||20011344|||||||||H|||||||||M|||||X000000000|||||||F||||||X000000000
IN2|1|X000000000|||||||||||||||||||||||||||^PC^100.0||||DE|||N||||||||||||||||||||||||||||||||||"#;

        let msg = Message::parse_with_lenient_newlines(&raw, true).unwrap();

        let data = &map(&msg);
        assert!(data.is_err(), "IN1.13 is invalid date format");
    }

    #[test]
    #[test]
    fn test_map_addresses() {
        let msg = r#"MSH|^~\&|ORBIS|KH|WEBEPA|KH|202208200651||ADT^A04^ADT_A04|65298857|P|2.5||640340718|NE|NE||8859/1
EVN|A08|202511022120||11036_123456789|ZZZZZZZZ|202511022120
PID|1|1212121|1212121|21600000|Sokolovski, Malina||19820101101139|F|||Hexengasse 1^^Traumstadt^^12345^D^L~Wettergasse 42^^Wetter^^54321^D^L||012345/1234^^PH~0123451234^^CP~max-muster.mann@web.de^^X.400|||S|ev||||12345~23456|||||D||||N"#;
        let msg = Message::parse_with_lenient_newlines(msg, true).unwrap();

        // two addresses
        let expected = vec![
            Address::builder()
                .r#type(AddressType::Both)
                .line(vec![Some("Hexengasse 1".into())])
                .city("Traumstadt".into())
                .postal_code("12345".into())
                .country("D".into())
                .build()
                .unwrap(),
            Address::builder()
                .r#type(AddressType::Both)
                .line(vec![Some("Wettergasse 42".into())])
                .city("Wetter".into())
                .postal_code("54321".into())
                .country("D".into())
                .build()
                .unwrap(),
        ];
        let addresses: Vec<Address> = map_addresses_dto(&map(&msg).unwrap().unwrap())
            .unwrap()
            .into_iter()
            .flatten()
            .collect();

        assert_eq!(addresses, expected);
    }

    #[test]
    fn test_map_names() {
        let msg = r#"MSH|^~\&|ORBIS|KH|WEBEPA|KH|202208200651||ADT^A04^ADT_A04|65298857|P|2.5||640340718|NE|NE||8859/1
EVN|A08|202511022120||11036_123456789|ZZZZZZZZ|202511022120
PID||1234456|||Schuster^Regine^^^^^L~Musterfrau^Regine^^^^^M|||||||||||||||||||||||||"#;
        let msg = Message::parse_with_lenient_newlines(msg, true).unwrap();
        let actual = map_name(&map(&msg).unwrap().unwrap());

        let expected = vec![
            HumanName::builder()
                .r#use(NameUse::Official)
                .given(vec![Some("Regine".into())])
                .family("Schuster".into())
                .build()
                .unwrap(),
            HumanName::builder()
                .r#use(NameUse::Maiden)
                .given(vec![Some("Regine".into())])
                .family("Musterfrau".into())
                .build()
                .unwrap(),
        ];
        let names = actual
            .unwrap()
            .into_iter()
            .flatten()
            .collect::<Vec<HumanName>>();

        assert_eq!(names, expected);
    }
}
