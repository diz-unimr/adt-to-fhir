use chrono::ParseError;
use fhir_model::time::error::{ComponentRange, InvalidFormatDescription};
use fhir_model::{BuilderError, DateFormatError, time};
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

impl From<ParseError> for ContentError {
    fn from(err: ParseError) -> Self {
        ContentError::from(err).into()
    }
}

#[derive(Debug, Error)]
pub enum ContentError {
    #[error("Mapping failed due value at {property} is missing or empty.")]
    MissingValueError { property: String },
    #[error(transparent)]
    BuilderError(#[from] BuilderError),
    #[error(transparent)]
    ParsingError(#[from] ParsingError),
    #[error(transparent)]
    DateFormatError(#[from] DateFormatError),
    #[error(transparent)]
    ComponentRange(#[from] ComponentRange),
}

#[derive(Debug, Error)]
pub(crate) enum ParsingError {
    #[error(transparent)]
    DateFormatError(#[from] DateFormatError),
    #[error(transparent)]
    ParseError(#[from] ParseError),
    #[error(transparent)]
    ParseDateError(#[from] time::error::Parse),
    #[error(transparent)]
    ParseIntError(#[from] std::num::ParseIntError),
    #[error(transparent)]
    ParseFloatError(#[from] std::num::ParseFloatError),
    #[error(transparent)]
    InvalidFormatError(#[from] InvalidFormatDescription),
    #[error(transparent)]
    ComponentRangeError(#[from] time::error::ComponentRange),
    #[error(transparent)]
    Other(#[from] anyhow::Error),
}
