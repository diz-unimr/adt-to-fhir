use chrono::ParseError;
use fhir_model::time::error::ComponentRange;
use fhir_model::{BuilderError, DateFormatError};
use thiserror::Error;
#[derive(Debug, Error)]
pub enum FhirMappingError {
    #[error("failed to lookup resource {resource} with value {value}")]
    MissingResourceError { resource: String, value: String },
    #[error(transparent)]
    MissingContentError(#[from] ContentError),
    #[error(transparent)]
    Other(#[from] anyhow::Error),
}
#[derive(Debug, Error)]
pub enum ContentError {
    #[error("Mapping failed due value at {property} is missing or empty.")]
    MissingValueError { property: String },
    #[error(transparent)]
    BuilderError(#[from] BuilderError),
    #[error(transparent)]
    ParsingError(#[from] ParseError),
    #[error(transparent)]
    DateFormatError(#[from] DateFormatError),
    #[error(transparent)]
    ComponentRange(#[from] ComponentRange),
}
impl From<BuilderError> for FhirMappingError {
    fn from(err: BuilderError) -> Self {
        ContentError::from(err).into()
    }
}
impl FhirMappingError {
    pub(crate) fn name(&self) -> &str {
        match self {
            FhirMappingError::MissingContentError { .. } => "MissingContentError",

            FhirMappingError::MissingResourceError { .. } => "MissingResourceError",

            FhirMappingError::Other(_) => "Other",
        }
    }
}
