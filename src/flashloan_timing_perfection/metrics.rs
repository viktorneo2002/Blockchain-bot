use lazy_static::lazy_static;
use prometheus::{self, IntCounter, IntGauge, Registry};
use std::sync::Arc;

lazy_static! {
    // Transaction metrics
    pub static ref TRANSACTIONS_SENT: IntCounter = register_int_counter!(
        "slot_boundary_transactions_sent_total",
        "Total number of transactions sent"
    ).unwrap();
    
    pub static ref TRANSACTIONS_CONFIRMED: IntCounter = register_int_counter!(
        "slot_boundary_transactions_confirmed_total",
        "Total number of transactions confirmed"
    ).unwrap();
    
    pub static ref TRANSACTIONS_FAILED: IntCounter = register_int_counter!(
        "slot_boundary_transactions_failed_total",
        "Total number of failed transactions"
    ).unwrap();
    
    // Slot timing metrics
    pub static ref SLOT_DRIFT_MS: IntGauge = register_int_gauge!(
        "slot_boundary_slot_drift_ms",
        "Drift from expected slot timing in milliseconds"
    ).unwrap();
    
    pub static ref CURRENT_LEADER: IntGauge = register_int_gauge!(
        "slot_boundary_current_leader",
        "Current slot leader (as u64 for metrics)"
    ).unwrap();
    
    // Performance metrics
    pub static ref SIMULATION_TIME_MS: IntGauge = register_int_gauge!(
        "slot_boundary_simulation_time_ms",
        "Time taken for transaction simulation in milliseconds"
    ).unwrap();
    
    pub static ref EXECUTION_LATENCY_MS: IntGauge = register_int_gauge!(
        "slot_boundary_execution_latency_ms",
        "Execution latency in milliseconds"
    ).unwrap();
}

/// Initialize metrics and register with the default registry
pub fn init_metrics() -> anyhow::Result<()> {
    let registry = Registry::new();
    
    // Register all metrics
    registry.register(Box::new(TRANSACTIONS_SENT.clone()))?;
    registry.register(Box::new(TRANSACTIONS_CONFIRMED.clone()))?;
    registry.register(Box::new(TRANSACTIONS_FAILED.clone()))?;
    registry.register(Box::new(SLOT_DRIFT_MS.clone()))?;
    registry.register(Box::new(CURRENT_LEADER.clone()))?;
    registry.register(Box::new(SIMULATION_TIME_MS.clone()))?;
    registry.register(Box::new(EXECUTION_LATENCY_MS.clone()))?;
    
    Ok(())
}
