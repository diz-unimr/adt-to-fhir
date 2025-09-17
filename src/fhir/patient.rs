use crate::config::Fhir;
use crate::fhir::mapper::{bundle_entry, extract_repeat, parse_date, parse_datetime, MessageType};
use anyhow::anyhow;
use fhir_model::r4b::codes::AddressType::Both;
use fhir_model::r4b::codes::{AdministrativeGender, IdentifierUse};
use fhir_model::r4b::resources::Patient;
use fhir_model::r4b::resources::{BundleEntry, PatientDeceased};
use fhir_model::r4b::types::Extension;
use fhir_model::r4b::types::{Address, HumanNameInner};
use fhir_model::r4b::types::{
    AddressBuilder, AddressInner, ExtensionValue, FieldExtensionBuilder, Identifier, Meta,
};
use fhir_model::r4b::types::{ExtensionInner, HumanName};
use hl7_parser::Message;
use std::error::Error;
use std::str::FromStr;
use std::vec;

pub(super) fn map_patient(
    v2_msg: &Message,
    config: Fhir,
) -> Result<Vec<BundleEntry>, anyhow::Error> {
    // todo refactor to fn
    let message_type: MessageType = MessageType::from_str(
        v2_msg
            .segment("EVN")
            .ok_or(anyhow!("missing ENV segment"))?
            .field(1)
            .ok_or(anyhow!("missing message type segment"))?
            .raw_value(),
    )?;

    // todo check message type if necessary for patient mapping
    let addr_builder = AddressBuilder::default();
    let pid_seg = v2_msg
        .segment("PID")
        .ok_or(anyhow!("missing PID segment"))?;
    let pid = pid_seg
        .field(2)
        .ok_or(anyhow!("missing Patient ID field"))?;
    let date_of_birth_date = pid_seg
        .field(7)
        .ok_or(anyhow!("missing Patient date field"))?;
    let gender = pid_seg
        .field(8)
        .ok_or(anyhow!("missing Patient Gender field"))?;
    let address = pid_seg
        .field(11)
        .ok_or(anyhow!("missing Patient Gender field"))?;
    let name = pid_seg
        .field(5)
        .ok_or(anyhow!("missing Patient MartialStaus field"))?;

    //let martial_staus = pid_seg.field(16).ok_or("missing Patient MartialStaus field")?;
    let address: Address = AddressInner {
        id: None,
        extension: vec![],
        r#use: None,
        use_ext: None,
        r#type: Some(Both),
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
    // Create Extension for Family
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

    let humanname: HumanName = HumanNameInner {
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

    //let input = "19920220";

    // patient vital status
    let death_date_time = pid_seg
        .field(29)
        .ok_or(anyhow!("missing Patient deathTime field"))?;
    let death_confirm = pid_seg
        .field(30)
        .ok_or(anyhow!("missing Patient deathConfirm field"))?;

    let deceased_confirm = if death_confirm.raw_value().to_owned() == "Y" {
        if !death_date_time.is_empty() {
            PatientDeceased::DateTime(parse_datetime(death_date_time.raw_value())?) // period
        } else if death_date_time.is_empty() {
            PatientDeceased::Boolean(true) //
        } else {
            PatientDeceased::Boolean(false)
        }
    } else {
        PatientDeceased::Boolean(false)
    };

    // Replace `is_deceased` with your condition
    // Replace `is_deceased` with your condition

    // let deceased_confirm = match (
    //     death_confirm.raw_value().to_owned().as_str(),
    //     death_date_time.is_empty(),
    // ) {
    //     ("Y", false) => PatientDeceased::DateTime(death_date_time.raw_value().to_owned().parse()?),
    //     ("Y", true) => PatientDeceased::Boolean(true),
    //     _ => PatientDeceased::Boolean(false),
    // };

    let admin_gender: AdministrativeGender = match (gender.raw_value()) {
        "F" => AdministrativeGender::Female,
        "M" => AdministrativeGender::Male,
        "U" => AdministrativeGender::Other,
        _ => AdministrativeGender::Unknown,
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
        //.birth_date(birth_date.to_string().parse().unwrap())
        .birth_date(parse_date(date_of_birth_date.raw_value())?)
        .gender(admin_gender)
        .address(vec![Some(address)])
        .name(vec![Some(humanname)])
        .deceased(deceased_confirm);
    let p = builder.build()?;
    Ok(vec![bundle_entry(p)?])
}

pub(super) fn map_a01(v2_msg: Message, config: Fhir) -> Result<Vec<BundleEntry>, Box<dyn Error>> {
    todo!("implement")
}
