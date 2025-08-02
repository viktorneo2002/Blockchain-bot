use std::sync::{Arc, RwLock};

#[derive(Debug, Clone)]
pub struct RiskManagerConfig {
    pub max_position_size: u64,
    pub max_daily_loss: u64,
    pub max_open_positions: usize,
}

pub struct RiskManager {
    max_position_size: u64,
    current_positions: Arc<RwLock<u64>>,
}

impl RiskManager {
    pub fn new(config: RiskManagerConfig) -> Self {
        Self {
            max_position_size: config.max_position_size,
            current_positions: Arc::new(RwLock::new(0)),
        }
    }
    
    pub async fn check_position_limits(&self, amount: u64) -> Result<bool, Box<dyn std::error::Error>> {
        let current = self.current_positions.read().map_err(|e| {
            Box::new(std::io::Error::new(std::io::ErrorKind::Other, format!("Failed to acquire read lock on current_positions: {}", e)))
        })?;
        Ok(*current + amount <= self.max_position_size)
    }
}
