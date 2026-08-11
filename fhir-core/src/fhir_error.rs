use fhir_model::BuilderError;
use thiserror::Error;
#[derive(Debug, Error)]
pub enum FhirMappingError {
    #[error(transparent)]
    BuilderError(#[from] BuilderError),

    #[error("failed to lookup resource {resource} with value {value}")]
    MissingResourceError { resource: String, value: String },

    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

impl FhirMappingError {
    pub(crate) fn name(&self) -> &str {
        match self {
            FhirMappingError::BuilderError(_) => "BuilderError",

            FhirMappingError::MissingResourceError { .. } => "MissingResourceError",

            FhirMappingError::Other(_) => "Other",
        }
    }
}
