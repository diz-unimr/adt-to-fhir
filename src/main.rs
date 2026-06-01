extern crate core;

mod config;
mod error;
mod fhir;
mod hl7;
mod metrics;
mod processor;
pub mod test_utils;

use crate::fhir::mapper::FhirMapper;
use crate::metrics::init_meter_provider;
use crate::processor::{Context, Processor};
use config::AppConfig;
use log::{error, info};
use rdkafka::ClientConfig;
use std::process;
use std::sync::Arc;
use tokio::signal::unix::{SignalKind, signal};
use tokio_util::sync::CancellationToken;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() {
    // app config
    let config = match AppConfig::new() {
        Ok(config) => config,
        Err(e) => {
            println!("Failed to parse app settings: {e}");
            process::exit(1)
        }
    };

    // logging / tracing
    let filter = format!(
        "{}={level}",
        env!("CARGO_CRATE_NAME"),
        level = config.app.log_level
    );
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| filter.into()))
        .init();
    let meter_provider = init_meter_provider();

    // cancellation
    let cancel = CancellationToken::new();
    let cloned_token = cancel.clone();
    tokio::spawn(async move {
        let mut sigterm = signal(SignalKind::terminate()).unwrap();
        let mut sigint = signal(SignalKind::interrupt()).unwrap();

        tokio::select! {
            _ = sigterm.recv() => {
                info!("🛑 SIGTERM received. Shutting down consumers..");
                cloned_token.cancel();
            },
            _ = sigint.recv() => {
                info!("🛑 SIGINT received. Shutting down consumers..");
                cloned_token.cancel();
            }
        }
    });

    let ctx = Context {
        cancel,
        on_commit: None,
    };

    let mapper = Arc::new(FhirMapper::new(config.fhir).expect("failed to create mapper"));

    Processor::new(config.kafka, mapper, ctx).start().await;

    if let Err(e) = meter_provider.shutdown() {
        error!("Error shutting down meter provider: {e:?}");
    }
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::PathBuf;

    pub(crate) fn read_test_resource(file_name: &str) -> String {
        let mut file_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        file_path.push("resources/test");
        file_path.push(file_name);

        fs::read_to_string(file_path.display().to_string())
            .unwrap_or_else(|_| panic!("Test resource not found: {}", file_path.display()))
    }
}
