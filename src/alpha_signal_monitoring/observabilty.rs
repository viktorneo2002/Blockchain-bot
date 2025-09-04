use metrics::{register_gauge, register_counter};
use metrics_exporter_prometheus::PrometheusBuilder;

pub fn init_metrics_and_logging() {
    // Initialize logging
    tracing_subscriber::fmt::init();
    
    // Initialize Prometheus metrics
    PrometheusBuilder::new()
        .install()
        .expect("failed to install metrics exporter");
    
    // Register core gauges
    register_gauge!("open_opportunities", || 0.0);
    register_gauge!("avg_decay_rate", || 0.0);
    
    // Register counters
    register_counter!("opportunities_detected");
    register_counter!("opportunities_executed");
    register_counter!("opportunities_expired");
}
