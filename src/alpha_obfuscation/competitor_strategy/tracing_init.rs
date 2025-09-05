use opentelemetry::sdk::trace as sdktrace;
use opentelemetry_otlp::WithExportConfig;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

pub fn init_tracing(service_name: &str) -> anyhow::Result<()> {
    let tracer = opentelemetry_otlp::new_pipeline()
        .tracing()
        .with_trace_config(sdktrace::config().with_resource(opentelemetry::sdk::Resource::new(vec![
            opentelemetry::KeyValue::new("service.name", service_name.to_string()),
        ])))
        .install_batch(opentelemetry::runtime::Tokio)?;
    let otel = tracing_opentelemetry::layer().with_tracer(tracer);
    let fmt = tracing_subscriber::fmt::layer().with_target(false);
    tracing_subscriber::registry().with(otel).with(fmt).try_init()?;
    Ok(())
}
