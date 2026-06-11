use opentelemetry::global;
use opentelemetry::metrics::Counter;
use opentelemetry_otlp::{MetricExporter, WithExportConfig};
use opentelemetry_sdk::Resource;
use opentelemetry_sdk::metrics::{SdkMeterProvider, Temporality};
use std::sync::OnceLock;

static RECORD_COUNTER: OnceLock<Counter<u64>> = OnceLock::new();

pub(crate) fn record_counter() -> &'static Counter<u64> {
    RECORD_COUNTER.get_or_init(|| {
        global::meter("processor")
            .u64_counter("records_processed")
            .with_description("The number of records processed")
            .build()
    })
}

pub(crate) fn init_meter_provider(endpoint: &str) -> anyhow::Result<SdkMeterProvider> {
    let exporter = MetricExporter::builder()
        .with_tonic()
        .with_endpoint(endpoint)
        .with_temporality(Temporality::default())
        .build()?;

    let provider = SdkMeterProvider::builder()
        .with_resource(Resource::builder().with_service_name("adt-to-fhir").build())
        .with_periodic_exporter(exporter)
        .build();
    global::set_meter_provider(provider.clone());
    Ok(provider)
}

#[cfg(test)]
mod tests {
    use crate::metrics::{init_meter_provider, record_counter};
    use mock_collector::{MockServer, Protocol};

    #[tokio::test(flavor = "multi_thread")]
    async fn test_record_counter() {
        let server = MockServer::builder()
            .protocol(Protocol::Grpc)
            .start()
            .await
            .expect("Failed to start server");

        println!("Server started on {}", server.addr());

        // add metric
        let addr = format!("http://{}", server.addr());
        let provider = init_meter_provider(&addr).unwrap();
        record_counter().add(1, &[]);

        provider.shutdown().unwrap();

        println!("Metrics sent successfully!\n");
        println!("Performing assertions...");

        server
            .with_collector(|collector| {
                // single metric
                assert_eq!(collector.metric_count(), 1);

                // metric exists
                collector
                    .expect_metric_with_name("records_processed")
                    .with_resource_attributes([("service.name", "adt-to-fhir")])
                    .assert_exists();
            })
            .await;

        println!("\nAll assertions passed!");

        server.shutdown().await.unwrap();
        println!("Server shut down successfully");
    }
}
