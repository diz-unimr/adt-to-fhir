use thiserror::Error;

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("failed to lookup resource {resource} with value {value}")]
    MissingResourceError { resource: String, value: String },
}

impl ConfigError {
    pub fn name(&self) -> &str {
        match self {
            ConfigError::MissingResourceError { .. } => "MissingResourceError",
        }
    }
}
