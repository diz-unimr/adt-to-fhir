use chrono::ParseError;
use derive_builder::UninitializedFieldError;
use fhir_core::model::person_dto::{PersonDtoBuilderError, PersonNameBuilderError};
use fhir_model::time::error::InvalidFormatDescription;
use fhir_model::{BuilderError, DateFormatError, time};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum Hl7MappingError {
    #[error(transparent)]
    MessageError(#[from] Hl7MessageAccessError),
    #[error("failed to lookup resource {resource} with value {value}")]
    MissingResourceError { resource: String, value: String },
    #[error("builder {builder_name} failed with: {builder_error}")]
    BuilderError {
        builder_name: String,
        builder_error: String,
    },
    #[error(transparent)]
    Hl7MessageParseError(#[from] hl7_parser::parser::ParseError),
    #[error("builder misses mandatory field value for {details}")]
    BuilderUninitializedFieldError { details: String },
    #[error("builder validation failed at structure {resource} with message {details}")]
    InputValidationError { resource: String, details: String },
    #[error(transparent)]
    Hl7ParsingError(#[from] Hl7ParsingError),
    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

impl From<UninitializedFieldError> for Hl7MappingError {
    fn from(err: UninitializedFieldError) -> Self {
        Hl7MappingError::BuilderUninitializedFieldError {
            details: err.field_name().to_string(),
        }
    }
}
impl From<PersonDtoBuilderError> for Hl7MappingError {
    fn from(err: PersonDtoBuilderError) -> Self {
        Hl7MappingError::BuilderError {
            builder_name: "PersonDtoBuilder".to_string(),
            builder_error: err.to_string(),
        }
    }
}

impl From<PersonNameBuilderError> for Hl7MappingError {
    fn from(err: PersonNameBuilderError) -> Self {
        Hl7MappingError::BuilderError {
            builder_name: "PersonNameBuilderError".to_string(),
            builder_error: err.to_string(),
        }
    }
}

impl From<BuilderError> for Hl7MappingError {
    fn from(err: BuilderError) -> Self {
        Hl7MappingError::BuilderUninitializedFieldError {
            details: err.0.field_name().to_string(),
        }
    }
}
impl Hl7MappingError {
    pub(crate) fn name(&self) -> &str {
        match self {
            Hl7MappingError::MessageError(_) => "MessageError",
            Hl7MappingError::MissingResourceError { .. } => "MissingResourceError",
            Hl7MappingError::Hl7MessageParseError(_) => "Hl7ParseError",
            Hl7MappingError::Other(_) => "Other",
            Hl7MappingError::BuilderUninitializedFieldError { .. } => {
                "BuilderUninitializedFieldError"
            }
            Hl7MappingError::InputValidationError { .. } => "InputValidationError",
            Hl7MappingError::Hl7ParsingError(_) => "Hl7ParsingError",
            Hl7MappingError::BuilderError { .. } => "BuilderError",
        }
    }
}

#[derive(Debug, Error)]
pub enum Hl7ParsingError {
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
impl Hl7ParsingError {
    pub(crate) fn name(&self) -> &str {
        match self {
            Hl7ParsingError::DateFormatError(_) => "DateFormatError",
            Hl7ParsingError::ParseError(_) => "ParseError",
            Hl7ParsingError::ParseDateError(_) => "ParseDateError",
            Hl7ParsingError::ParseIntError(_) => "ParseIntError",
            Hl7ParsingError::ParseFloatError(_) => "ParseFloatError",
            Hl7ParsingError::InvalidFormatError(_) => "InvalidFormatError",
            Hl7ParsingError::ComponentRangeError(_) => "ComponentRangeError",

            Hl7ParsingError::Other(_) => "OtherError",
        }
    }
}
#[derive(Debug, Error)]
pub enum Hl7MessageAccessError {
    #[error("Missing message segment {0}")]
    MissingMessageSegment(String),
    #[error("Missing message field value at {0}")]
    MissingMessageValue(String),
    #[error("Message content '{0}' at {1} is unsupported")]
    UnsupportedContentError(String, String),
    #[error(transparent)]
    ParseError(#[from] hl7_parser::parser::ParseError),
    #[error(transparent)]
    Other(#[from] anyhow::Error),
}
