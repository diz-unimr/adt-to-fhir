extern crate core;
mod error;
mod fhir;
mod metrics;
mod processor;

use crate::fhir::mapper::FhirMapper;
use crate::metrics::init_meter_provider;
use crate::processor::{Context, Processor};
use adt_config::config::AppConfig;
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

    let meter_provider = init_meter_provider(&config.app.telemetry_endpoint)
        .expect("failed to initialize meter provider");

    // cancellation
    let cancel = CancellationToken::new();
    let cloned_token = cancel.clone();
    tokio::spawn(async move {
        let mut sigterm =
            signal(SignalKind::terminate()).expect("failed to register terminate signal handler");
        let mut sigint =
            signal(SignalKind::interrupt()).expect("failed to register interrupt signal handler");

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
    use adt_config::config::AppConfig;
    use config::Environment;
    use std::collections::HashMap;

    #[test]
    fn default_config_validates() {
        match AppConfig::new() {
            Ok(_) => {}
            Err(e) => {
                panic!("{}", e)
            }
        }
    }

    #[test]
    fn invalid_config_fails() {
        // override validated property with invalid data
        let source = Environment::default().source(Some({
            let mut env = HashMap::new();
            env.insert("kafka.num_partitions".into(), "0".into());
            env
        }));

        let c = adt_config::config::AppConfig::with_env(source);

        assert!(c.is_err());
    }
}
