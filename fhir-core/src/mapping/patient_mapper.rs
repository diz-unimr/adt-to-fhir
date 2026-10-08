use crate::fhir_error::ContentError::MissingValueError;
use crate::fhir_error::{ContentError, FhirMappingError};
use crate::model::person_dto::{DtoDates, GenderDto, Insurance, MaritalStatusDto, PersonDto};
use adt_config::config::Fhir;
use anyhow::anyhow;
use chrono::NaiveDateTime;
use chrono::TimeZone;
use chrono_tz::Europe::Berlin;
use std::sync::LazyLock;

use crate::mapping::misc::{
    EntryRequestType, bundle_entry, field_extension, get_cc_with_one_code,
    get_period_from_date_time, parse_date, parse_naive_date_as_date_time,
    parse_naive_datetime_as_date, patch_bundle_entry, upsert_reference,
};
use crate::model::meta::ProcessingOperation;

use fhir_model::DateFormatError::InvalidDate;
use fhir_model::r4b::codes::{AddressType, AdministrativeGender, IdentifierUse, NameUse};
use fhir_model::r4b::resources::{
    BundleEntry, Parameters, ParametersParameter, ParametersParameterValue, Patient,
    PatientBuilder, PatientDeceased, PatientMultipleBirth, ResourceType,
};
use fhir_model::r4b::types::{
    Address, CodeableConcept, Coding, ExtensionValue, HumanName, Identifier, Meta, Period,
    Reference,
};
use fhir_model::time::OffsetDateTime;
use fhir_model::{BuilderError, DateTime, Instant};
use log::{Level, log};
use regex::Regex;

pub fn map(data: &PersonDto, config: &Fhir) -> Result<Option<BundleEntry>, FhirMappingError> {
    match &data.meta.operation {
        ProcessingOperation::UpdateAsCreate
        | ProcessingOperation::CreateIfNotExists
        | ProcessingOperation::Delete => {
            let pat = map_patient(data, config);
            match &data.meta.operation {
                ProcessingOperation::UpdateAsCreate => Ok(Some(bundle_entry(
                    pat?,
                    EntryRequestType::UpdateAsCreate,
                    config,
                )?)),
                ProcessingOperation::CreateIfNotExists => Ok(Some(bundle_entry(
                    pat?,
                    EntryRequestType::ConditionalCreate,
                    config,
                )?)),
                ProcessingOperation::Delete => match pat {
                    Ok(pat) => {
                        // return full mapped patient with delete request
                        Ok(Some(bundle_entry(pat, EntryRequestType::Delete, config)?))
                    }
                    Err(_) => {
                        // in case of mapping error, try map minimal necessary information to create delete request
                        if let Ok(pat_ident) = create_patient_identifiers(data, config) {
                            let min_data_pat =
                                PatientBuilder::default().identifier(pat_ident).build()?;
                            Ok(Some(bundle_entry(
                                min_data_pat,
                                EntryRequestType::Delete,
                                config,
                            )?))
                        } else {
                            Err(FhirMappingError::MissingContentError(MissingValueError {
                                property: "expected to create a DELETE patient request! \
                                even identifier creation failed - please check message content!"
                                    .to_string(),
                            }))
                        }
                    }
                },
                _ => Err(FhirMappingError::ProcessingFailed(anyhow!(
                    "map patient - unexpected operation at processing operation type- bugfix needed!"
                ))),
            }
        }

        ProcessingOperation::Patch => {
            if let Some((content, target_to_be_patched)) = create_patient_merge_dto(data, config)? {
                let patch = patch_bundle_entry(
                    content,
                    &ResourceType::Patient,
                    &target_to_be_patched,
                    config,
                )?;
                Ok(Some(patch))
            } else {
                // no data to patch
                Err(FhirMappingError::MissingContentError(
                    MissingValueError {
                        property:
                        "patient merge - data missing but tried to create patient merge - check messsage!".to_string()}
                ))
            }
        }
        ProcessingOperation::Skip => Ok(None),
        _ => Ok(None),
    }
}
pub fn map_patient(pat_data: &PersonDto, config: &Fhir) -> Result<Patient, ContentError> {
    // patient resource
    let mut patient = Patient::builder()
        .meta(
            Meta::builder()
                .profile(vec![Some(config.person.profile.to_owned())])
                .source(config.meta_source.to_string())
                .build()?,
        )
        .identifier(create_patient_identifiers(pat_data, config)?)
        .address(map_addresses_dto(pat_data)?)
        .name(map_name(pat_data)?)
        .gender(map_gender(&pat_data.gender))
        .build()?;

    // birth_date

    patient.birth_date = match pat_data.date_of_birth {
        None => None,
        Some(DtoDates::Date(date)) => parse_date(Some(date))?,
        Some(DtoDates::Datetime(datetime)) => Some(parse_naive_datetime_as_date(datetime)?),
    };

    // marital_status
    if let Some(ref marital_status) = pat_data.marital_status {
        patient.marital_status = Some(map_marital_status(marital_status.to_v3_code())?);
    }

    // deceased flag
    patient.deceased = map_deceased(pat_data)?;

    patient.multiple_birth = map_multiple_birth(pat_data)?;

    Ok(patient)
}

fn map_gender(input: &Option<GenderDto>) -> AdministrativeGender {
    match input {
        Some(GenderDto::Male) => AdministrativeGender::Male,
        Some(GenderDto::Female) => AdministrativeGender::Female,
        Some(GenderDto::Diverse) => AdministrativeGender::Other,
        None | Some(GenderDto::Unknown) => AdministrativeGender::Unknown,
    }
}

pub fn map_name(person: &PersonDto) -> Result<Vec<Option<HumanName>>, BuilderError> {
    let mut names = Vec::new();

    for name_entry in person.names.iter().flatten() {
        let name_use = match name_entry.is_maiden {
            Some(false) => Some(NameUse::Official),
            Some(true) => Some(NameUse::Maiden),
            _ => None,
        };

        if name_entry.family.is_none() {
            continue;
        }

        let mut name_build = HumanName::builder().build()?;
        name_build.r#use = name_use;
        name_build.family = name_entry.family.clone();
        if name_entry
            .given_name
            .iter()
            .any(|n| n.is_some() && n.iter().any(|nn| !nn.is_empty()))
        {
            name_build.given = name_entry.given_name.clone();
        }

        // prefix
        if let Some(prefix) = name_entry.name_prefix.clone() {
            name_build.prefix = vec![Some(prefix)];
            name_build.prefix_ext = vec![Some(field_extension(
                "http://hl7.org/fhir/StructureDefinition/iso21090-EN-qualifier".into(),
                ExtensionValue::Code("AC".into()),
            )?)];
        }

        // namenszusatz
        if let Some(namenszusatz) = name_entry.name_extension.clone() {
            name_build.family_ext = Some(field_extension(
                "http://fhir.de/StructureDefinition/humanname-namenszusatz".into(),
                ExtensionValue::String(namenszusatz),
            )?);
        }

        // vorsatzwort
        if let Some(vorsatzwort) = name_entry.name_affix.clone() {
            name_build.family_ext = Some(field_extension(
                "http://hl7.org/fhir/StructureDefinition/humanname-own-prefix".into(),
                ExtensionValue::String(vorsatzwort),
            )?);
        }

        names.push(Some(name_build));
    }

    Ok(names)
}

fn map_multiple_birth(pat_data: &PersonDto) -> Result<Option<PatientMultipleBirth>, ContentError> {
    let multi_birth_flag = pat_data.is_multiple_birth;
    let multi_birth_number = pat_data.multiple_birth_order;

    match (multi_birth_flag, multi_birth_number) {
        // nur Mehrlingsgeburt-Kennung vorhanden
        (Some(multi_birth_flag), None) => match multi_birth_flag {
            true => Ok(Some(PatientMultipleBirth::Boolean(true))),
            false => Ok(Some(PatientMultipleBirth::Boolean(false))),
        },

        (_multi_birth_flag, Some(multi_birth_number)) => Ok(Some(PatientMultipleBirth::Integer(
            multi_birth_number as i32,
        ))),
        (None, None) => Ok(None),
    }
}

fn map_deceased(data: &PersonDto) -> Result<Option<PatientDeceased>, ContentError> {
    // patient vital status
    let death_time = data.time_of_death.clone();
    let death_confirm = data.is_deceased_indicator;

    match (death_time, death_confirm) {
        (Some(DtoDates::Datetime(death_time)), _) => Ok(Some(PatientDeceased::DateTime(
            DateTime::DateTime(death_time.into()),
        ))),
        (Some(DtoDates::Date(death_time)), _) => Ok(Some(PatientDeceased::DateTime(
            parse_naive_date_as_date_time(death_time)?,
        ))),
        (None, Some(confirm)) => Ok(Some(PatientDeceased::Boolean(confirm))),
        _ => Ok(None),
    }
}

pub fn map_addresses_dto(dto: &PersonDto) -> Result<Vec<Option<Address>>, BuilderError> {
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
pub fn create_patient_merge_dto(
    patient_dto: &PersonDto,
    config: &Fhir,
) -> Result<(Option<(Parameters, Identifier)>), ContentError> {
    match (patient_dto.pid.clone(), patient_dto.replaced_by_pid.clone()) {
        (replaced_patient_id, Some(new_pid)) => Ok(Some(create_patient_merge(
            replaced_patient_id,
            new_pid,
            config,
        )?)),
        (_, _) => {
            log!(
                Level::Error,
                "failed to create a patient merge data - no pid found"
            );
            Ok(None)
        }
    }
}
pub fn create_patient_merge(
    replaced_patient_id: String,
    new_pid: String,
    config: &Fhir,
) -> Result<(Parameters, Identifier), ContentError> {
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

pub fn map_marital_status(value: &str) -> Result<CodeableConcept, BuilderError> {
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

    Ok(CodeableConcept::builder()
        .coding(vec![Some(marital_coding)])
        .build()?)
}

pub fn parse_datetime(input: &str) -> Result<DateTime, ContentError> {
    let dt = NaiveDateTime::parse_from_str(input, "%Y%m%d%H%M")?;
    let dt_with_tz = Berlin
        .from_local_datetime(&dt)
        .earliest()
        .ok_or(InvalidDate)?;

    Ok(DateTime::DateTime(Instant(
        OffsetDateTime::from_unix_timestamp(dt_with_tz.timestamp())?,
    )))
}

/// Erzeugt Patienten-Identifier
///
/// * Ein PID-Identifier ist min. notwendig
/// * Zusätzlich werden weitere Identifier aus Gesundheitskassendaten *(IN1-Segmente)* erzeugt
///   werden, falls dies vorhanden sind.
///
/// _Hinweis:_ Es gibt HL7 Nachrichten, die in denen IN1 Segmente fehlen.
///
fn create_patient_identifiers(
    dto: &PersonDto,
    config: &Fhir,
) -> Result<Vec<Option<Identifier>>, ContentError> {
    // mandatory PID identifier
    let mut identifiers = vec![Some(create_patient_identifier_pid(
        dto.pid.clone(),
        config,
    )?)];

    // create optional identifiers from insurance data
    let insurance_ids: Vec<Option<Identifier>> = dto
        .insurance
        .iter()
        .map(|s| {
            if let Some(versicherung) = s {
                map_versicherungsdaten(dto.meta.id.clone(), versicherung, config)
            } else {
                Ok(None)
            }
        })
        .collect::<Result<Vec<Option<Identifier>>, ContentError>>()?;

    let ids: Vec<_> = insurance_ids.into_iter().flatten().collect();

    // first pick is insurance number of 10 literals without expiration date
    // second pick is first number without expiration date
    // or first available
    const GKV10_SYSTEM: &str = "http://fhir.de/sid/gkv/kvid-10";
    let selected = ids
        .iter()
        .find(|v| {
            v.system.as_deref() == Some(GKV10_SYSTEM)
                && v.period.as_ref().and_then(|p| p.end.as_ref()).is_none()
        })
        .or_else(|| {
            ids.iter()
                .find(|v| v.period.as_ref().and_then(|p| p.end.as_ref()).is_none())
        })
        .or_else(|| ids.first())
        .cloned();

    if let Some(id) = selected {
        identifiers.push(Some(id));
    }

    Ok(identifiers)
}

fn map_versicherungsdaten(
    msg_id: String,
    insurance: &Insurance,
    config: &Fhir,
) -> Result<Option<Identifier>, ContentError> {
    // Versicherungsnummer
    let mut result = Identifier::builder()
        .value(insurance.insurance_number.to_string())
        .r#use(IdentifierUse::Official)
        .build()?;

    if insurance.assigner_id.is_empty() {
        log!(
            Level::Warn,
            "Message-Id {}: For insurance '{}' no insurance company id found - \
            cannot add assigner",
            msg_id,
            insurance.insurance_number
        );
        return Ok(None);
    } else {
        // set assigner
        let reference = Reference::builder()
            .identifier(
                Identifier::builder()
                    .system("http://fhir.de/sid/arge-ik/iknr".to_string())
                    .value(insurance.assigner_id.to_string())
                    .r#use(IdentifierUse::Official)
                    .r#type(
                        CodeableConcept::builder()
                            .coding(vec![
                                Coding::builder()
                                    .code("XX".to_string())
                                    .system(
                                        "http://terminology.hl7.org/CodeSystem/v2-0203".to_string(),
                                    )
                                    .build()
                                    .ok(),
                            ])
                            .build()?,
                    )
                    .build()?,
            )
            .build()?;
        result.assigner = Some(reference);
    }

    if is_valid_gkv10(insurance.insurance_number.as_str()) {
        // GKV
        result.system = Some("http://fhir.de/sid/gkv/kvid-10".to_string());
        result.r#type = Some(
            CodeableConcept::builder()
                .coding(vec![Some(
                    Coding::builder()
                        .code("KVZ10".to_string())
                        .system("http://fhir.de/CodeSystem/identifier-type-de-basis".to_string())
                        .build()?,
                )])
                .build()?,
        );
    } else {
        // OTHER INSURANCE NUMBER! vor 2012 waren 9 - 12 Stellen ohne führenden Buchstaben valide.
        result.system = Some(config.person.other_insurance_system.to_string());
    }

    result.period = get_insurance_period(insurance)?;

    Ok(Some(result))
}

pub fn get_insurance_period(insurance: &Insurance) -> Result<Option<Period>, ContentError> {
    match (insurance.valid_from, insurance.valid_to) {
        (Some(from), Some(to)) => {
            let start = parse_naive_date_as_date_time(from)?;
            let end = parse_naive_date_as_date_time(to)?;

            get_period_from_date_time(Some(start), Some(end))
        }
        (Some(from), None) => {
            let start = parse_naive_date_as_date_time(from)?;
            get_period_from_date_time(Some(start), None)
        }
        (None, Some(to)) => {
            let end = parse_naive_date_as_date_time(to)?;
            get_period_from_date_time(None, Some(end))
        }
        (None, None) => Ok(None),
    }
}

pub fn is_valid_gkv10(insurance_number: &str) -> bool {
    static RE: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"^[A-Z][0-9]{9}$").unwrap());
    RE.is_match(insurance_number)
}

#[cfg(test)]
mod tests {}
