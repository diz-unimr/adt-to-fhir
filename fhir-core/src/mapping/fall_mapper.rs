use crate::fhir_error::ContentError;
use crate::model::encounter_dto::Fall;
use adt_config::config::Fhir;
use fhir_model::BuilderError;
use fhir_model::r4b::codes::IdentifierUse;
use fhir_model::r4b::resources::BundleEntry;
use fhir_model::r4b::types::{CodeableConcept, Coding, Identifier, Meta};

pub fn map_default_identifier_enc(
    system: String,
    value: String,
) -> Result<Identifier, BuilderError> {
    Identifier::builder()
        .system(system)
        .value(value)
        .r#use(IdentifierUse::Official)
        .r#type(
            CodeableConcept::builder()
                .coding(vec![Some(
                    Coding::builder()
                        .system("http://terminology.hl7.org/CodeSystem/v2-0203".to_string())
                        .code("VN".to_string())
                        .build()?,
                )])
                .build()?,
        )
        .build()
}
pub fn map_meta_encounter(config: &Fhir) -> Result<Meta, anyhow::Error> {
    Ok(Meta::builder()
        .profile(vec![Some(config.fall.profile.clone())])
        .source(config.meta_source.to_string())
        .build()?)
}

pub fn map(input: Fall, config: &Fhir) -> Result<BundleEntry, ContentError> {
    panic!("not implemented")
}
