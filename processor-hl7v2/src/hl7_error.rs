use chrono::ParseError;
use fhir_model::time::error::InvalidFormatDescription;
use fhir_model::{BuilderError, DateFormatError, time};
use rdkafka::error::KafkaError;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum Hl7ProcessingError {
    #[error("kafka error: {0}")]
    Kafka(#[from] KafkaError),
    #[error(transparent)]
    Mapping(#[from] Hl7MappingError),
}

#[derive(Debug, Error)]
pub enum Hl7MappingError {
    #[error(transparent)]
    MessageError(#[from] Hl7MessageAccessError),
    #[error(transparent)]
    BuilderError(#[from] BuilderError),
    #[error("failed to lookup resource {resource} with value {value}")]
    MissingResourceError { resource: String, value: String },
    #[error(transparent)]
    Hl7ParseError(#[from] hl7_parser::parser::ParseError),
    #[error("builder of {builder_name} misses mandatory field value for {details}")]
    BuilderUninitializedFieldError {
        builder_name: String,
        details: String,
    },
    #[error("builder validation failed at structure {resource} with message {details}")]
    InputValidationError { resource: String, details: String },
    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

impl Hl7MappingError {
    pub(crate) fn name(&self) -> &str {
        match self {
            Hl7MappingError::MessageError(_) => "MessageError",
            Hl7MappingError::BuilderError(_) => "BuilderError",
            Hl7MappingError::MissingResourceError { .. } => "MissingResourceError",
            Hl7MappingError::Hl7ParseError(_) => "Hl7ParseError",
            Hl7MappingError::Other(_) => "Other",
            Hl7MappingError::BuilderUninitializedFieldError { .. } => {
                "BuilderUninitializedFieldError"
            }
            Hl7MappingError::InputValidationError { .. } => "InputValidationError",
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
pub(crate) enum Hl7MessageAccessError {
    #[error("Missing message segment {0}")]
    MissingMessageSegment(String),
    #[error("Missing message field value at {0}")]
    MissingMessageValue(String),
    #[error(transparent)]
    MessageTypeError(#[from] Hl7MessageTypeError),
    #[error("Message content '{0}' at {1} is unsupported")]
    UnsupportedContentError(String, String),
    #[error(transparent)]
    ParseError(#[from] hl7_parser::parser::ParseError),
    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

#[derive(Debug, Error)]
pub enum Hl7MessageTypeError {
    #[error("Unknown message type: {0}")]
    UnknownMessageType(String),
    #[error("Missing message type: {0}")]
    MissingMessageType(String),
}
