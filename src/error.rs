use chrono::ParseError;
use fhir_model::time::error::InvalidFormatDescription;
use fhir_model::{BuilderError, DateFormatError, time};
use rdkafka::error::KafkaError;
use thiserror::Error;

#[derive(Debug, Error)]
pub(crate) enum ProcessingError {
    #[error("kafka error: {0}")]
    Kafka(#[from] KafkaError),
    #[error(transparent)]
    Mapping(#[from] MappingError),
}

#[derive(Debug, Error)]
pub(crate) enum MappingError {
    #[error(transparent)]
    MessageError(#[from] MessageAccessError),
    #[error(transparent)]
    BuilderError(#[from] BuilderError),
    #[error(transparent)]
    FormattingError(#[from] ParsingError),
    #[error("failed to lookup resource {resource} with value {value}")]
    MissingResourceError { resource: String, value: String },
    #[error(transparent)]
    Hl7ParseError(#[from] hl7_parser::parser::ParseError),
    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

impl MappingError {
    pub(crate) fn name(&self) -> &str {
        match self {
            MappingError::MessageError(_) => "MessageError",
            MappingError::BuilderError(_) => "BuilderError",
            MappingError::FormattingError(_) => "FormattingError",
            MappingError::MissingResourceError { .. } => "MissingResourceError",
            MappingError::Hl7ParseError(_) => "Hl7ParseError",
            MappingError::Other(_) => "Other",
        }
    }
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

#[derive(Debug, Error)]
pub(crate) enum MessageAccessError {
    #[error("Missing message segment {0}")]
    MissingMessageSegment(String),
    #[error(transparent)]
    MessageTypeError(#[from] MessageTypeError),
    #[error("Message content '{0}' at {1} is unsupported")]
    UnsupportedContentError(String, String),
    #[error(transparent)]
    ParseError(#[from] hl7_parser::parser::ParseError),
    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

#[derive(Debug, Error)]
pub enum MessageTypeError {
    #[error("Unknown message type: {0}")]
    UnknownMessageType(String),
    #[error("Missing message type: {0}")]
    MissingMessageType(String),
}
