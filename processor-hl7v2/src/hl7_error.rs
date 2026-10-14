use chrono::ParseError;
use derive_builder::UninitializedFieldError;
use fhir_core::canonical_model::canonical_location::LocationCdmBuilderError;
use fhir_core::canonical_model::canonical_meta::MappingOpEncounterBuilderError;
use fhir_core::canonical_model::canonical_patient::{
    InsuranceBuilderError, PersonNameBuilderError,
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

impl From<MappingOpEncounterBuilderError> for Hl7MappingError {
    fn from(err: MappingOpEncounterBuilderError) -> Self {
        Hl7MappingError::BuilderError {
            builder_name: "MappingOpEncounterBuilder".to_string(),
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

impl From<LocationCdmBuilderError> for Hl7MappingError {
    fn from(err: LocationCdmBuilderError) -> Self {
        Hl7MappingError::BuilderError {
            builder_name: "LocationCdmBuilderError".to_string(),
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
