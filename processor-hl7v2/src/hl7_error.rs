use chrono::ParseError;
use derive_builder::UninitializedFieldError;
use fhir_core::model::encounter_dto::FallBuilderError;
use fhir_core::model::meta::MappingOpEncounterBuilderError;
use fhir_core::model::person_dto::{
    InsuranceBuilderError, PersonDtoBuilderError, PersonNameBuilderError,
};
use fhir_model::time::error::InvalidFormatDescription;
use fhir_model::{BuilderError, DateFormatError, time};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum Hl7MappingError {
    /// HL7 message is missing expectations any
    #[error(transparent)]
    MessageError(#[from] Hl7MessageAccessError),
    /// resource file is missing an expected value
    #[error("failed to lookup resource {resource} with value {value}")]
    MissingResourceError { resource: String, value: String },
    /// struct builder failed to build content
    #[error("builder {builder_name} failed with: {builder_error}")]
    BuilderError {
        builder_name: String,
        builder_error: String,
    },

    /// some mandatory property is empty
    #[error("builder misses mandatory field value for {details}")]
    BuilderUninitializedFieldError { details: String },
    #[error("builder validation failed at structure {resource} with message {details}")]

    /// hl7 content is unsupported or is missing expected structure
    InputValidationError { resource: String, details: String },

    /// raw HL7 content could not be parsed into target structure
    #[error(transparent)]
    Hl7ParsingError(#[from] Hl7MessageParsingError),

    /// all other problems
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

impl From<MappingOpEncounterBuilderError> for Hl7MappingError {
    fn from(err: MappingOpEncounterBuilderError) -> Self {
        Hl7MappingError::BuilderError {
            builder_name: "MappingOpEncounterBuilder".to_string(),
            builder_error: err.to_string(),
        }
    }
}

impl From<FallBuilderError> for Hl7MappingError {
    fn from(err: FallBuilderError) -> Self {
        Hl7MappingError::BuilderError {
            builder_name: "FallBuilderError".to_string(),
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

impl From<InsuranceBuilderError> for Hl7MappingError {
    fn from(err: InsuranceBuilderError) -> Self {
        Hl7MappingError::BuilderError {
            builder_name: "InsuranceBuilderError".to_string(),
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
pub enum Hl7MessageParsingError {
    /// HL7 Message could not be parsed
    #[error(transparent)]
    Hl7MessageParseError(#[from] hl7_parser::parser::ParseError),
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
impl Hl7MessageParsingError {
    pub(crate) fn name(&self) -> &str {
        match self {
            Hl7MessageParsingError::DateFormatError(_) => "DateFormatError",
            Hl7MessageParsingError::ParseError(_) => "ParseError",
            Hl7MessageParsingError::ParseDateError(_) => "ParseDateError",
            Hl7MessageParsingError::ParseIntError(_) => "ParseIntError",
            Hl7MessageParsingError::ParseFloatError(_) => "ParseFloatError",
            Hl7MessageParsingError::InvalidFormatError(_) => "InvalidFormatError",
            Hl7MessageParsingError::ComponentRangeError(_) => "ComponentRangeError",

            Hl7MessageParsingError::Other(_) => "OtherError",
            Hl7MessageParsingError::Hl7MessageParseError(_) => "Hl7MessageParseError",
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
