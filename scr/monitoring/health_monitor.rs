use std::collections::HashMap;
use std::sync::{Arc, RwLock};

#[derive(Debug, Clone)]
pub struct HealthMonitorConfig {
    pub alert_webhook: Option<String>,
    pub metrics_port: u16,
}

pub struct HealthMonitor {
    metrics: Arc<RwLock<HashMap<String, f64>>>,
}

impl HealthMonitor {
    pub fn new(_config: HealthMonitorConfig) -> Self {
        Self {
            metrics: Arc::new(RwLock::new(HashMap::new())),
        }
    }
    
    pub async fn record_metric(&self, key: &str, value: f64) {
        let mut metrics = self.metrics.write().unwrap();
        metrics.insert(key.to_string(), value);
    }
}
