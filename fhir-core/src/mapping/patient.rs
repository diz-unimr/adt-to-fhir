use crate::fhir_error::ContentError;
use crate::fhir_error::ContentError::MissingValueError;
use crate::model::person_dto::PersonDto;
use adt_config::config::Fhir;
use chrono::TimeZone;
use chrono::{Datelike, NaiveDateTime};
use chrono_tz::Europe::Berlin;

use crate::mapping::misc::{get_cc_with_one_code, identifier_search};
use fhir_model::DateFormatError::InvalidDate;
use fhir_model::r4b::codes::{AddressType, IdentifierUse};
use fhir_model::r4b::resources::{
    Parameters, ParametersParameter, ParametersParameterValue, ResourceType,
};
use fhir_model::r4b::types::{Address, CodeableConcept, Coding, Identifier, Reference};
use fhir_model::time::OffsetDateTime;
use fhir_model::{BuilderError, DateTime, Instant};

fn map_addresses_dto(dto: &PersonDto) -> Result<Vec<Option<Address>>, BuilderError> {
    let mut res = vec![];

    for elem in dto.address.clone() {
        let mut addr = Address::builder().r#type(AddressType::Both).build()?;

        if let Some(addr_elem) = elem {
            // line

            addr.line = addr_elem.street_and_number.clone();

            // city
            if let Some(city) = addr_elem.city {
                addr.city = Some(city.to_string());
            }
            // postal code
            if let Some(postal_code) = addr_elem.zip_code {
                addr.postal_code = Some(postal_code.to_string());
            }
            // country
            if let Some(country) = addr_elem.country {
                addr.country = Some(country.to_string());
            }

            if !addr.line.is_empty() && addr.line.iter().all(|l| l.is_some()) && addr.city.is_some()
            {
                // street must have at least 1 line and city must also have a value
                res.push(Some(addr));
            }
        }
    }

    Ok(res)
}
fn create_patient_merge_dto(
    patient_dto: &PersonDto,
    config: &Fhir,
) -> Result<(Option<(Parameters, Identifier)>), ContentError> {
    match (patient_dto.pid.clone(), patient_dto.replaced_by_pid.clone()) {
        (replaced_patient_id, Some(new_pid)) => Ok(Some(create_patient_merge(
            replaced_patient_id,
            new_pid,
            config,
        )?)),
        (_, _) => Ok(None),
    }
}
pub fn create_patient_merge(
    replaced_patient_id: String,
    new_pid: String,
    config: &Fhir,
) -> Result<(Parameters, Identifier), crate::fhir_error::ContentError> {
    {
        let params = Parameters::builder()
            .parameter(vec![Some(
                ParametersParameter::builder()
                    .name("operation".to_string())
                    .part(vec![
                        Some(
                            ParametersParameter::builder()
                                .name("type".to_string())
                                .value(ParametersParameterValue::Code("add".to_string()))
                                .build()?,
                        ),
                        Some(
                            ParametersParameter::builder()
                                .name("path".to_string())
                                .value(ParametersParameterValue::String(
                                    ResourceType::Patient.to_string(),
                                ))
                                .build()?,
                        ),
                        Some(
                            ParametersParameter::builder()
                                .name("name".to_string())
                                .value(ParametersParameterValue::String("link".to_string()))
                                .build()?,
                        ),
                        Some(
                            ParametersParameter::builder()
                                .name("value".to_string())
                                .part(vec![
                                    Some(
                                        ParametersParameter::builder()
                                            .name("other".to_string())
                                            .value(ParametersParameterValue::Reference(
                                                Reference::builder()
                                                    .reference(upsert_reference(
                                                        &ResourceType::Patient,
                                                        &create_patient_identifier_pid(
                                                            new_pid.to_string(),
                                                            config,
                                                        )?,
                                                    )?)
                                                    .r#type(ResourceType::Patient.to_string())
                                                    .build()?,
                                            ))
                                            .build()?,
                                    ),
                                    Some(
                                        ParametersParameter::builder()
                                            .name("type".to_string())
                                            .value(ParametersParameterValue::Code(
                                                "replaced-by".to_string(),
                                            ))
                                            .build()?,
                                    ),
                                ])
                                .build()?,
                        ),
                    ])
                    .build()?,
            )])
            .build()?;

        Ok((
            params,
            Identifier::builder()
                .system(config.person.system.to_string())
                .value(replaced_patient_id.to_string())
                .build()?,
        ))
    }
}

pub fn create_patient_identifier_pid(
    pid: String,
    config: &Fhir,
) -> Result<Identifier, BuilderError> {
    Identifier::builder()
        .r#use(IdentifierUse::Usual)
        .system(config.person.system.to_owned())
        .value(pid)
        .r#type(get_cc_with_one_code(
            "MR".to_string(),
            "http://terminology.hl7.org/CodeSystem/v2-0203".to_string(),
        )?)
        .assigner(
            Reference::builder()
                .display("UKGM - Universitätsklinikum Marburg".to_string())
                .identifier(
                    Identifier::builder()
                        .value(config.facility_id.to_string())
                        .system("http://fhir.de/sid/arge-ik/iknr".to_string())
                        .build()?,
                )
                .build()?,
        )
        .build()
}

pub fn upsert_reference(
    resource_type: &ResourceType,
    identifier: &Identifier,
) -> Result<String, crate::fhir_error::ContentError> {
    Ok(format!(
        "{resource_type}?{}",
        identifier_search(
            identifier.system.as_deref().ok_or(MissingValueError {
                property: "identifier.system missing".to_string()
            })?,
            identifier.value.as_deref().ok_or(MissingValueError {
                property: "identifier.value missing".to_string()
            })?
        )
    ))
}

pub fn map_marital_status(value: &str) -> Result<Option<CodeableConcept>, BuilderError> {
    // marital status
    let marital_coding = match value {
        "A" | "E" => Coding::builder()
            .system("http://terminology.hl7.org/CodeSystem/v3-MaritalStatus".to_string())
            .code("L".to_string())
            .display("Legally Separated".to_string())
            .build(),
        "D" => Coding::builder()
            .system("http://terminology.hl7.org/CodeSystem/v3-MaritalStatus".to_string())
            .code("D".to_string())
            .display("Divorced".to_string())
            .build(),
        "M" => Coding::builder()
            .system("http://terminology.hl7.org/CodeSystem/v3-MaritalStatus".to_string())
            .code("M".to_string())
            .display("Married".to_string())
            .build(),
        "S" => Coding::builder()
            .system("http://terminology.hl7.org/CodeSystem/v3-MaritalStatus".to_string())
            .code("S".to_string())
            .display("Never Married".to_string())
            .build(),
        "W" => Coding::builder()
            .system("http://terminology.hl7.org/CodeSystem/v3-MaritalStatus".to_string())
            .code("W".to_string())
            .display("Widowed".to_string())
            .build(),
        "C" => Coding::builder()
            .system("http://terminology.hl7.org/CodeSystem/v3-MaritalStatus".to_string())
            .code("C".to_string())
            .display("Common Law".to_string())
            .build(),
        "G" | "P" | "R" => Coding::builder()
            .system("http://terminology.hl7.org/CodeSystem/v3-MaritalStatus".to_string())
            .code("T".to_string())
            .display("Domestic partner".to_string())
            .build(),
        "N" => Coding::builder()
            .system("http://terminology.hl7.org/CodeSystem/v3-MaritalStatus".to_string())
            .code("A".to_string())
            .display("Annulled".to_string())
            .build(),
        "I" => Coding::builder()
            .system("http://terminology.hl7.org/CodeSystem/v3-MaritalStatus".to_string())
            .code("I".to_string())
            .display("Interlocutory".to_string())
            .build(),
        "B" => Coding::builder()
            .system("http://terminology.hl7.org/CodeSystem/v3-MaritalStatus".to_string())
            .code("U".to_string())
            .display("Unmarried".to_string())
            .build(),
        _a => Coding::builder()
            .system("http://terminology.hl7.org/CodeSystem/v3-NullFlavor".to_string())
            .code("UNK".to_string())
            .display("Unknown".to_string())
            .build(),
    }?;

    Ok(Some(
        CodeableConcept::builder()
            .coding(vec![Some(marital_coding)])
            .build()?,
    ))
}

pub(crate) fn parse_datetime(input: &str) -> Result<DateTime, ContentError> {
    let dt = NaiveDateTime::parse_from_str(input, "%Y%m%d%H%M")?;
    let dt_with_tz = Berlin
        .from_local_datetime(&dt)
        .earliest()
        .ok_or(InvalidDate)?;

    Ok(DateTime::DateTime(Instant(
        OffsetDateTime::from_unix_timestamp(dt_with_tz.timestamp())?,
    )))
}
#[cfg(test)]
mod tests {}
