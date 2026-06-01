use metrics::SdkMeterProvider;
use opentelemetry::global;
use opentelemetry::metrics::Counter;
use opentelemetry_sdk::{Resource, metrics};
use opentelemetry_stdout::MetricExporter;
use std::sync::OnceLock;

static RECORD_COUNTER: OnceLock<Counter<u64>> = OnceLock::new();

pub(crate) fn record_counter() -> &'static Counter<u64> {
    RECORD_COUNTER.get_or_init(|| {
        global::meter("processor")
            .u64_counter("records.processed")
            .with_description("The number of records processed")
            .build()
    })
}

pub(crate) fn init_meter_provider() -> SdkMeterProvider {
    let provider = SdkMeterProvider::builder()
        .with_periodic_exporter(MetricExporter::default())
        .with_resource(Resource::builder().with_service_name("adt-to-fhir").build())
        .build();
    global::set_meter_provider(provider.clone());
    provider
}
