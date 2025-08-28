use crate::config::Fhir;
use crate::fhir::mapper::{MessageType, bundle_entry};
use fhir_model::Date;
use fhir_model::r4b::codes::AddressType::Both;
use fhir_model::r4b::codes::IdentifierUse;
use fhir_model::r4b::codes::NarrativeStatus::Extensions;
use fhir_model::r4b::codes::SpecialValues::False;
use fhir_model::r4b::resources::Patient;
use fhir_model::r4b::resources::{BundleEntry, PatientDeceased};
use fhir_model::r4b::types::Extension;
use fhir_model::r4b::types::{Address, HumanNameInner};
use fhir_model::r4b::types::{
    AddressBuilder, AddressInner, ExtensionBuilder, ExtensionValue, ExtensionValueExtension,
    FieldExtension, FieldExtensionBuilder, HumanNameBuilder, Identifier, Meta,
};
use fhir_model::r4b::types::{ExtensionInner, HumanName};
use hl7_parser::Message;
use hl7_parser::message::{Field, Repeat};
use log::info;
use serde::de::Unexpected::Str;
use std::error::Error;
use std::str::FromStr;
use std::vec;

pub(super) fn map_patient(
    v2_msg: Message,
    config: Fhir,
) -> Result<Vec<BundleEntry>, Box<dyn Error>> {
    // todo refactor to fn
    let message_type: MessageType = MessageType::from_str(
        v2_msg
            .segment("EVN")
            .ok_or("missing ENV segment")?
            .field(1)
            .ok_or("missing message type segment")?
            .raw_value(),
    )?;

    // todo check message type if necessary for patient mapping
    let addr_builder = AddressBuilder::default();
    let pid_seg = v2_msg.segment("PID").ok_or("missing PID segment")?;
    let pid = pid_seg.field(2).ok_or("missing Patient ID field")?;
    let date_of_birth_date = pid_seg.field(7).ok_or("missing Patient date field")?;
    let gender = pid_seg.field(8).ok_or("missing Patient Gender field")?;
    let address = pid_seg.field(11).ok_or("missing Patient Gender field")?;
    let name = pid_seg
        .field(5)
        .ok_or("missing Patient MartialStaus field")?;

    //let martial_staus = pid_seg.field(16).ok_or("missing Patient MartialStaus field")?;
    let address: Address = AddressInner {
        id: None,
        extension: vec![],
        r#use: None,
        use_ext: None,
        r#type: None,
        type_ext: None,
        text: None,
        text_ext: None,
        line: vec![extract_repeat(address.raw_value(), 1)?],
        line_ext: vec![],
        city: extract_repeat(address.raw_value(), 3)?,
        city_ext: None,
        district: None,
        district_ext: None,
        state: None,
        state_ext: None,
        postal_code: extract_repeat(address.raw_value(), 5)?,
        postal_code_ext: None,
        country: extract_repeat(address.raw_value(), 4)?,
        country_ext: None,
        period: None,
        period_ext: None,
    }
    .into();

    let extension: Extension = ExtensionInner {
        id: None,
        extension: vec![],
        url: "http://hl7.org/fhir/StructureDefinition/humanname-own-name".to_string(),
        value: Some(ExtensionValue::String(
            extract_repeat(name.raw_value(), 1)?.unwrap().to_string(),
        )),
        value_ext: None,
    }
    .into();

    let fieldExtension = FieldExtensionBuilder::default()
        .extension(vec![Some(extension).unwrap()])
        .build();

    let mut humanname: HumanName = HumanNameInner {
        id: None,
        extension: vec![],
        r#use: None,
        use_ext: None,
        text: None,
        text_ext: None,
        family: extract_repeat(name.raw_value(), 1)?,
        family_ext: Some(fieldExtension?),
        given: vec![extract_repeat(name.raw_value(), 2)?],
        given_ext: vec![],
        prefix: vec![extract_repeat(name.raw_value(), 6)?],
        prefix_ext: vec![],
        suffix: vec![],
        suffix_ext: vec![],
        period: None,
        period_ext: None,
    }
    .into();

    // patient vital status
    let death_date_time = pid_seg.field(29).ok_or("missing Patient deathTime field")?;
    let death_confirm = pid_seg
        .field(30)
        .ok_or("missing Patient deathConfirm field")?;
    let deceased_confirm =
        if death_confirm.raw_value().to_owned() == "Y" && !death_date_time.is_empty() {
            // Replace `is_deceased` with your condition
            Some(PatientDeceased::DateTime(
                death_date_time.raw_value().to_owned().parse()?,
            )) // or Some(false) depending on your logic
        } else if death_confirm.raw_value().to_owned() == "Y" && death_date_time.is_empty() {
            Some(PatientDeceased::Boolean(true)) // or Some(false) if you want to explicitly set it to alive
        } else {
            None
        };

    // Create Address
    let builder = Patient::builder()
        .meta(
            Meta::builder()
                .profile(vec![Some(config.person.profile.to_owned())])
                .build()?,
        )
        .identifier(vec![Some(
            Identifier::builder()
                .r#use(IdentifierUse::Usual)
                .system(config.person.system.to_owned())
                .value(pid.raw_value().to_owned())
                .build()
                .unwrap(),
        )])
        .birth_date(date_of_birth_date.raw_value().to_owned().parse()?)
        .gender(gender.raw_value().to_owned().parse()?)
        .address(vec![Some(address)])
        .deceased(deceased_confirm.unwrap())
        .name(vec![Some(humanname)]);
    // TODO
    let p = builder.build()?;
    Ok(vec![bundle_entry(p)?])
}

fn extract_repeat(field_value: &str, component: usize) -> Result<Option<String>, Box<dyn Error>> {
    let repeat = hl7_parser::parser::parse_repeat(field_value)?;
    if !repeat.is_empty() {
        Ok(repeat
            .component(component)
            .map(|c| c.raw_value().to_string().parse().ok())
            .flatten())
    } else {
        Ok(None)
    }
}

pub(super) fn map_a01(v2_msg: Message, config: Fhir) -> Result<Vec<BundleEntry>, Box<dyn Error>> {
    todo!("implement")
}

#[cfg(test)]
mod tests {
    use crate::config::{Fhir, ResourceConfig};
    use crate::fhir::patient::map_patient;
    use fhir_model::r4b::resources::{BundleEntry, Patient};
    use hl7_parser::Message;

    #[test]
    fn map_test() {
        let hl7 = r#"MSH|^~\&|ORBIS|KH|WEBEPA|KH|20251102212117||ADT^A08^ADT_A01|12332112|P|2.5||123788998|NE|NE||8859/1
EVN|A08|202511022120||11036_123456789|ZZZZZZZZ|202511022120
PID|1|9999999|9999999|88888888|Nachname^SäuglingVorname^^^^^L||202511022120|M|||Strasse. 1&Strasse.&1^^Stadt^^30000^DE^L~^^Stadt^^^^BDL||0000000000000^PRN^PH^^^00000^0000000^^^^^000000000000|||U|||||12345678^^^KH^VN~1234567^^^KH^PT||Stadt|J|1|DE||||N
PV1|1|I|KJMST042^BSP-2-2^^KJM^KLINIKUM^961640|R^^HL7~01^Normalfall^11||KJMST042^BSP-1-1^^KJM^KLINIKUM^961640||^^^^^^^^^L^^^^^^^^^^^^^^^^^^^^^^^^^^^BSNR||N||||||N|||88888888||K|||||||||||||||01|||1000|9||||202511022120|202511022120||||||A
PV2|||06^Geburt^11||||||202511022120|||Versicherten Nr. der Mutter 0000000000||||||||||N||I||||||||||||Y
DG1|1||Z38.0^Einling, Geburt im Krankenhaus^icd10gm2023||0000000000000|FA En|||||||||1|BBBBBB^^^^^^^^^^^^^^^^^^^^^^GEB||||12340005|U
DG1|2||Z38.0^Einling, Geburt im Krankenhaus^icd10gm2023||0000000000000|FA Be|||||||||2|BBBBBB^^^^^^^^^^^^^^^^^^^^^^KJM||||12340007|U
DG1|3||Z38.0^Einling, Geburt im Krankenhaus^icd10gm2023||0000000000000|Aufn.|||||||||1|BBBBBB^^^^^^^^^^^^^^^^^^^^^^GEB||||12340009|U
DG1|4||Z38.0^Einling, Geburt im Krankenhaus^icd10gm2023||0000000000000|FA Be|||||||||1|BBBBBB^^^^^^^^^^^^^^^^^^^^^^GEB||||12340001|U
IN1|1||000000000^^^^NII~Krankenkasse^^^^XX|Krankenkasse|Strasse 1&Strasse&1^^Stadt^^1000^DE^L||000000000000^PRN^PH^^^00000^0000000^^^^^000000000000||Krankenkasse^1^^^1&gesetzliche Krankenkasse^^NII~Krankenkasse^1^^^^^U|||||||Nachname^Vorname||19340101|Strasse. 1&Strasse.&1^^Stadt^^30000^DE^L|||H|||||||||F||||||||||||F|||||||||AndereStadt
IN2|1||||||||||||||||||||||||||||^PC^100.0||||DE|||N|||ev||||||||||||||||||||||||00000 0000000
ZBE|55555555^ORBIS|202511022120|202511022120|UPDATE
ZNG|1|N|N|Normal|L|48|3390|||Gesundes Neugeborenes"#;

        let config = Fhir {
            person: ResourceConfig {
                profile: "https://www.medizininformatik-initiative.de/fhir/core/modul-person/StructureDefinition/Patient|2025.0.0".to_string(),
                system: "https://fhir.diz.uni-marburg.de/sid/patient-id".to_string(),
            },
            fall: Default::default(),
        };

        // act
        let res = map_patient(
            Message::parse_with_lenient_newlines(hl7, true).unwrap(),
            config.clone(),
        );

        let ok = res.unwrap();
        let entry = ok.first().unwrap();

        // assert profile set
        let p = to_patient(entry.clone());
        let profile = p
            .meta
            .as_ref()
            .unwrap()
            .profile
            .first()
            .unwrap()
            .as_ref()
            .unwrap()
            .as_str();

        assert_eq!(profile, config.person.profile.to_owned());
    }

    fn to_patient(e: BundleEntry) -> Patient {
        let r = e.resource.unwrap();
        Patient::try_from(r).unwrap()
    }
}
