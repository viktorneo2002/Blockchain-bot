use prometheus::{
    Counter, Gauge, Histogram, IntCounter, IntGauge, Registry, 
    register_counter_with_registry, register_gauge_with_registry,
    register_histogram_with_registry, register_int_counter_with_registry,
    register_int_gauge_with_registry, Opts, HistogramOpts,
};
use anyhow::{Result, anyhow};
use std::sync::Arc;
use tracing::{info, warn, error, debug, span, Level};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter, fmt};
use std::collections::HashMap;
use solana_sdk::pubkey::Pubkey;

use super::bundle_success_rate_calculator::{BundleSubmission, BundleStatus, FailureReason};

// Production-grade metrics collection for MEV bot
#[derive(Clone)]
pub struct MetricsCollector {
    registry: Arc<Registry>,
    
    // Bundle submission metrics
    bundle_submissions_total: IntCounter,
    bundle_successes_total: IntCounter,
    bundle_failures_total: IntCounter,
    
    // Tip metrics
    tip_distribution: Histogram,
    successful_tip_distribution: Histogram,
    tip_efficiency_ratio: Gauge,
    
    // Validator metrics
    validator_success_rates: Gauge,
    validator_avg_landing_times: Gauge,
    validator_rejection_rates: Gauge,
    
    // Network metrics
    network_congestion_level: Gauge,
    current_slot: IntGauge,
    slot_utilization: Gauge,
    
    // Performance metrics
    submission_latency: Histogram,
    calculation_duration: Histogram,
    database_operation_duration: Histogram,
    rpc_call_duration: Histogram,
    
    // Health metrics
    active_connections: IntGauge,
    error_rate: Gauge,
    circuit_breaker_state: IntGauge,
    
    // Business metrics
    total_revenue_lamports: Counter,
    missed_opportunities: IntCounter,
    profit_margin: Gauge,
}

impl MetricsCollector {
    pub fn new() -> Result<Self> {
        let registry = Arc::new(Registry::new());
        
        // Bundle metrics
        let bundle_submissions_total = register_int_counter_with_registry!(
            Opts::new("bundle_submissions_total", "Total number of bundle submissions"),
            registry.clone()
        )?;
        
        let bundle_successes_total = register_int_counter_with_registry!(
            Opts::new("bundle_successes_total", "Total number of successful bundle landings"),
            registry.clone()
        )?;
        
        let bundle_failures_total = register_int_counter_with_registry!(
            Opts::new("bundle_failures_total", "Total number of failed bundles"),
            registry.clone()
        )?;

        // Tip metrics with proper buckets for MEV analysis
        let tip_buckets = vec![
            1000.0, 5000.0, 10000.0, 25000.0, 50000.0, 100000.0, 
            250000.0, 500000.0, 1000000.0, 2500000.0, 5000000.0, 10000000.0
        ];
        
        let tip_distribution = register_histogram_with_registry!(
            HistogramOpts::new("tip_distribution_lamports", "Distribution of tips in lamports")
                .buckets(tip_buckets.clone()),
            registry.clone()
        )?;
        
        let successful_tip_distribution = register_histogram_with_registry!(
            HistogramOpts::new("successful_tip_distribution_lamports", "Distribution of successful tips")
                .buckets(tip_buckets),
            registry.clone()
        )?;

        let tip_efficiency_ratio = register_gauge_with_registry!(
            Opts::new("tip_efficiency_ratio", "Ratio of successful tip amount to total tip amount"),
            registry.clone()
        )?;

        // Validator performance metrics
        let validator_success_rates = register_gauge_with_registry!(
            Opts::new("validator_success_rates", "Success rates by validator"),
            registry.clone()
        )?;
        
        let validator_avg_landing_times = register_gauge_with_registry!(
            Opts::new("validator_avg_landing_times_ms", "Average landing times by validator"),
            registry.clone()
        )?;
        
        let validator_rejection_rates = register_gauge_with_registry!(
            Opts::new("validator_rejection_rates", "Rejection rates by validator"),
            registry.clone()
        )?;

        // Network metrics
        let network_congestion_level = register_gauge_with_registry!(
            Opts::new("network_congestion_level", "Current network congestion (0=low, 1=medium, 2=high)"),
            registry.clone()
        )?;
        
        let current_slot = register_int_gauge_with_registry!(
            Opts::new("current_slot", "Current Solana slot number"),
            registry.clone()
        )?;
        
        let slot_utilization = register_gauge_with_registry!(
            Opts::new("slot_utilization", "Current slot utilization percentage"),
            registry.clone()
        )?;

        // Performance metrics with appropriate buckets
        let latency_buckets = vec![0.001, 0.005, 0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0];
        
        let submission_latency = register_histogram_with_registry!(
            HistogramOpts::new("submission_latency_seconds", "Bundle submission latency")
                .buckets(latency_buckets.clone()),
            registry.clone()
        )?;
        
        let calculation_duration = register_histogram_with_registry!(
            HistogramOpts::new("calculation_duration_seconds", "Success rate calculation duration")
                .buckets(latency_buckets.clone()),
            registry.clone()
        )?;
        
        let database_operation_duration = register_histogram_with_registry!(
            HistogramOpts::new("database_operation_duration_seconds", "Database operation duration")
                .buckets(latency_buckets.clone()),
            registry.clone()
        )?;
        
        let rpc_call_duration = register_histogram_with_registry!(
            HistogramOpts::new("rpc_call_duration_seconds", "RPC call duration")
                .buckets(latency_buckets),
            registry.clone()
        )?;

        // Health metrics
        let active_connections = register_int_gauge_with_registry!(
            Opts::new("active_connections", "Number of active database connections"),
            registry.clone()
        )?;
        
        let error_rate = register_gauge_with_registry!(
            Opts::new("error_rate", "Current error rate percentage"),
            registry.clone()
        )?;
        
        let circuit_breaker_state = register_int_gauge_with_registry!(
            Opts::new("circuit_breaker_state", "Circuit breaker state (0=closed, 1=open, 2=half-open)"),
            registry.clone()
        )?;

        // Business metrics
        let total_revenue_lamports = register_counter_with_registry!(
            Opts::new("total_revenue_lamports", "Total revenue in lamports from successful bundles"),
            registry.clone()
        )?;
        
        let missed_opportunities = register_int_counter_with_registry!(
            Opts::new("missed_opportunities_total", "Total number of missed MEV opportunities"),
            registry.clone()
        )?;
        
        let profit_margin = register_gauge_with_registry!(
            Opts::new("profit_margin_percentage", "Current profit margin percentage"),
            registry.clone()
        )?;

        Ok(Self {
            registry,
            bundle_submissions_total,
            bundle_successes_total,
            bundle_failures_total,
            tip_distribution,
            successful_tip_distribution,
            tip_efficiency_ratio,
            validator_success_rates,
            validator_avg_landing_times,
            validator_rejection_rates,
            network_congestion_level,
            current_slot,
            slot_utilization,
            submission_latency,
            calculation_duration,
            database_operation_duration,
            rpc_call_duration,
            active_connections,
            error_rate,
            circuit_breaker_state,
            total_revenue_lamports,
            missed_opportunities,
            profit_margin,
        })
    }

    // Record bundle submission metrics
    pub fn record_bundle_submission(&self, submission: &BundleSubmission) {
        let _span = span!(Level::DEBUG, "record_bundle_metrics", bundle_id = %submission.bundle_id).entered();
        
        self.bundle_submissions_total.inc();
        self.tip_distribution.observe(submission.tip_lamports as f64);
        
        match submission.status {
            BundleStatus::Landed => {
                self.bundle_successes_total.inc();
                self.successful_tip_distribution.observe(submission.tip_lamports as f64);
                self.total_revenue_lamports.inc_by(submission.tip_lamports as f64);
                
                if let Some(landing_slot) = submission.landing_slot {
                    let landing_time_ms = (landing_slot - submission.slot) * 420; // 420ms per slot
                    debug!("Bundle {} landed in {} ms", submission.bundle_id, landing_time_ms);
                }
            },
            BundleStatus::Failed | BundleStatus::Rejected | BundleStatus::Expired | BundleStatus::DroppedByValidator => {
                self.bundle_failures_total.inc();
                
                if let Some(reason) = submission.failure_reason {
                    warn!("Bundle {} failed: {:?}", submission.bundle_id, reason);
                    
                    match reason {
                        FailureReason::CompetitorOutbid => self.missed_opportunities.inc(),
                        FailureReason::InsufficientTip => self.missed_opportunities.inc(),
                        _ => {}
                    }
                }
            },
            _ => {}
        }

        self.submission_latency.observe(submission.submission_latency_ms as f64 / 1000.0);
        
        debug!(
            "Recorded metrics for bundle {} - Status: {:?}, Tip: {} lamports, Validator: {}",
            submission.bundle_id, submission.status, submission.tip_lamports, submission.validator
        );
    }

    // Update validator-specific metrics
    pub fn update_validator_metrics(&self, validator: &Pubkey, success_rate: f64, avg_landing_time: f64, rejection_rate: f64) {
        let validator_str = validator.to_string();
        
        // Note: Prometheus doesn't support dynamic labels well, so in production
        // you'd want to use a more sophisticated approach like separate metric families
        // or external time-series database. This is a simplified implementation.
        
        info!(
            "Validator {} metrics - Success rate: {:.2}%, Avg landing: {:.1}ms, Rejection rate: {:.2}%",
            validator_str, success_rate * 100.0, avg_landing_time, rejection_rate * 100.0
        );
    }

    // Update network congestion metrics
    pub fn update_network_metrics(&self, slot: u64, congestion_level: f64, utilization: f64) {
        self.current_slot.set(slot as i64);
        self.network_congestion_level.set(congestion_level);
        self.slot_utilization.set(utilization);
        
        debug!("Network metrics - Slot: {}, Congestion: {:.2}, Utilization: {:.1}%", 
               slot, congestion_level, utilization * 100.0);
    }

    // Record performance timing metrics
    pub fn record_calculation_duration(&self, duration: Duration) {
        self.calculation_duration.observe(duration.as_secs_f64());
    }

    pub fn record_database_duration(&self, duration: Duration) {
        self.database_operation_duration.observe(duration.as_secs_f64());
    }

    pub fn record_rpc_duration(&self, duration: Duration) {
        self.rpc_call_duration.observe(duration.as_secs_f64());
    }

    // Update health metrics
    pub fn update_health_metrics(&self, connections: i64, error_rate: f64, circuit_breaker_state: i64) {
        self.active_connections.set(connections);
        self.error_rate.set(error_rate);
        self.circuit_breaker_state.set(circuit_breaker_state);
    }

    // Calculate and update business metrics
    pub fn update_business_metrics(&self, total_spent: f64, total_earned: f64) {
        let profit_margin = if total_spent > 0.0 {
            ((total_earned - total_spent) / total_spent) * 100.0
        } else {
            0.0
        };
        
        self.profit_margin.set(profit_margin);
        
        info!("Business metrics - Spent: {} SOL, Earned: {} SOL, Margin: {:.2}%",
              total_spent / 1_000_000_000.0, total_earned / 1_000_000_000.0, profit_margin);
    }

    // Get Prometheus registry for HTTP export
    pub fn registry(&self) -> Arc<Registry> {
        self.registry.clone()
    }

    // Export metrics in Prometheus format
    pub fn export_metrics(&self) -> Result<String> {
        let metric_families = self.registry.gather();
        let mut buffer = Vec::new();
        let encoder = prometheus::TextEncoder::new();
        
        encoder.encode(&metric_families, &mut buffer)
            .map_err(|e| anyhow!("Failed to encode metrics: {}", e))?;
        
        String::from_utf8(buffer)
            .map_err(|e| anyhow!("Failed to convert metrics to string: {}", e))
    }
}

// Structured logging configuration for production observability
pub struct LoggingConfig {
    pub level: String,
    pub json_format: bool,
    pub include_file_line: bool,
    pub include_thread_id: bool,
    pub log_to_file: Option<String>,
}

impl Default for LoggingConfig {
    fn default() -> Self {
        Self {
            level: "info".to_string(),
            json_format: true,
            include_file_line: true,
            include_thread_id: true,
            log_to_file: Some("mev_bot.log".to_string()),
        }
    }
}

pub fn setup_logging(config: LoggingConfig) -> Result<()> {
    let filter = EnvFilter::new(&config.level);
    
    let fmt_layer = if config.json_format {
        fmt::layer()
            .json()
            .with_current_span(true)
            .with_span_list(false)
            .with_file(config.include_file_line)
            .with_line_number(config.include_file_line)
            .with_thread_ids(config.include_thread_id)
            .boxed()
    } else {
        fmt::layer()
            .with_target(true)
            .with_file(config.include_file_line)
            .with_line_number(config.include_file_line)
            .with_thread_ids(config.include_thread_id)
            .boxed()
    };

    let subscriber = tracing_subscriber::registry()
        .with(filter)
        .with(fmt_layer);

    if let Some(log_file) = config.log_to_file {
        let file_appender = tracing_appender::rolling::daily("./logs", log_file);
        let (non_blocking, _guard) = tracing_appender::non_blocking(file_appender);
        
        let file_layer = fmt::layer()
            .json()
            .with_writer(non_blocking)
            .with_current_span(true);
            
        subscriber.with(file_layer).init();
    } else {
        subscriber.init();
    }

    info!("Logging initialized with level: {}", config.level);
    Ok(())
}

// Alert conditions and thresholds
#[derive(Debug, Clone)]
pub struct AlertThresholds {
    pub min_success_rate: f64,
    pub max_failure_rate: f64,
    pub max_tip_spike_ratio: f64,
    pub max_validator_rejection_rate: f64,
    pub max_response_time_ms: f64,
    pub max_error_rate: f64,
}

impl Default for AlertThresholds {
    fn default() -> Self {
        Self {
            min_success_rate: 0.7,        // 70% minimum success rate
            max_failure_rate: 0.3,        // 30% maximum failure rate
            max_tip_spike_ratio: 3.0,     // 3x tip spike threshold
            max_validator_rejection_rate: 0.2, // 20% max rejection rate
            max_response_time_ms: 2000.0, // 2 second max response time
            max_error_rate: 0.05,         // 5% max error rate
        }
    }
}

// Alert manager for production monitoring
pub struct AlertManager {
    thresholds: AlertThresholds,
    metrics: Arc<MetricsCollector>,
    alert_cooldowns: Arc<tokio::sync::Mutex<HashMap<String, std::time::Instant>>>,
    cooldown_duration: Duration,
}

impl AlertManager {
    pub fn new(metrics: Arc<MetricsCollector>, thresholds: AlertThresholds) -> Self {
        Self {
            thresholds,
            metrics,
            alert_cooldowns: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
            cooldown_duration: Duration::from_secs(300), // 5 minute cooldown
        }
    }

    pub async fn check_alerts(&self, current_stats: &super::bundle_success_rate_calculator::TimeWindowStats) {
        let _span = span!(Level::INFO, "alert_check").entered();
        
        // Check success rate alert
        if current_stats.success_rate < self.thresholds.min_success_rate {
            self.send_alert(
                "low_success_rate".to_string(),
                format!("Success rate dropped to {:.1}% (threshold: {:.1}%)", 
                       current_stats.success_rate * 100.0, 
                       self.thresholds.min_success_rate * 100.0),
                AlertSeverity::Critical
            ).await;
        }

        // Check failure rate alert
        let failure_rate = (current_stats.failed_bundles + current_stats.rejected_bundles) as f64 / 
                          current_stats.total_submissions as f64;
        
        if failure_rate > self.thresholds.max_failure_rate {
            self.send_alert(
                "high_failure_rate".to_string(),
                format!("Failure rate increased to {:.1}% (threshold: {:.1}%)", 
                       failure_rate * 100.0, 
                       self.thresholds.max_failure_rate * 100.0),
                AlertSeverity::Warning
            ).await;
        }

        // Check tip efficiency
        let tip_efficiency = if current_stats.total_tips_paid > 0 {
            (current_stats.successful_bundles as f64 * current_stats.average_tip) / 
            current_stats.total_tips_paid as f64
        } else {
            1.0
        };

        if tip_efficiency < 0.5 {
            self.send_alert(
                "low_tip_efficiency".to_string(),
                format!("Tip efficiency dropped to {:.1}%", tip_efficiency * 100.0),
                AlertSeverity::Warning
            ).await;
        }

        debug!("Alert check completed - Success rate: {:.2}%, Failure rate: {:.2}%, Tip efficiency: {:.2}%",
               current_stats.success_rate * 100.0, failure_rate * 100.0, tip_efficiency * 100.0);
    }

    async fn send_alert(&self, alert_type: String, message: String, severity: AlertSeverity) {
        // Check cooldown
        let mut cooldowns = self.alert_cooldowns.lock().await;
        if let Some(last_alert) = cooldowns.get(&alert_type) {
            if last_alert.elapsed() < self.cooldown_duration {
                return; // Still in cooldown
            }
        }
        
        cooldowns.insert(alert_type.clone(), std::time::Instant::now());
        drop(cooldowns);

        match severity {
            AlertSeverity::Critical => error!("🚨 CRITICAL ALERT [{}]: {}", alert_type, message),
            AlertSeverity::Warning => warn!("⚠️  WARNING ALERT [{}]: {}", alert_type, message),
            AlertSeverity::Info => info!("ℹ️  INFO ALERT [{}]: {}", alert_type, message),
        }

        // In production, you would integrate with alerting systems like:
        // - PagerDuty
        // - Slack webhooks
        // - Discord webhooks
        // - Email notifications
        // - SMS notifications
    }
}

#[derive(Debug, Clone, Copy)]
pub enum AlertSeverity {
    Critical,
    Warning,
    Info,
}

// Performance monitoring utilities
pub struct PerformanceMonitor {
    start_time: std::time::Instant,
    operation_name: String,
    metrics: Arc<MetricsCollector>,
}

impl PerformanceMonitor {
    pub fn new(operation_name: String, metrics: Arc<MetricsCollector>) -> Self {
        Self {
            start_time: std::time::Instant::now(),
            operation_name,
            metrics,
        }
    }

    pub fn finish(self) {
        let duration = self.start_time.elapsed();
        
        match self.operation_name.as_str() {
            "calculation" => self.metrics.record_calculation_duration(duration),
            "database" => self.metrics.record_database_duration(duration),
            "rpc" => self.metrics.record_rpc_duration(duration),
            _ => {}
        }
        
        debug!("Operation '{}' completed in {:.3}ms", 
               self.operation_name, duration.as_secs_f64() * 1000.0);
    }
}

// Macro for easy performance monitoring
#[macro_export]
macro_rules! monitor_performance {
    ($metrics:expr, $operation:expr, $block:block) => {{
        let monitor = PerformanceMonitor::new($operation.to_string(), $metrics.clone());
        let result = $block;
        monitor.finish();
        result
    }};
}

// Health check aggregator for system monitoring
pub struct HealthChecker {
    pub database_healthy: bool,
    pub rpc_healthy: bool,
    pub metrics_healthy: bool,
    pub last_check: std::time::Instant,
}

impl HealthChecker {
    pub fn new() -> Self {
        Self {
            database_healthy: false,
            rpc_healthy: false,
            metrics_healthy: false,
            last_check: std::time::Instant::now(),
        }
    }

    pub fn overall_health(&self) -> bool {
        self.database_healthy && self.rpc_healthy && self.metrics_healthy
    }

    pub fn health_score(&self) -> f64 {
        let mut score = 0.0;
        if self.database_healthy { score += 0.4; }
        if self.rpc_healthy { score += 0.4; }
        if self.metrics_healthy { score += 0.2; }
        score
    }

    pub fn update_component_health(&mut self, component: &str, healthy: bool) {
        match component {
            "database" => self.database_healthy = healthy,
            "rpc" => self.rpc_healthy = healthy,
            "metrics" => self.metrics_healthy = healthy,
            _ => {}
        }
        
        self.last_check = std::time::Instant::now();
        
        if !healthy {
            warn!("Component '{}' health check failed", component);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_metrics_collector_creation() {
        let metrics = MetricsCollector::new();
        assert!(metrics.is_ok());
    }

    #[test]
    fn test_alert_thresholds() {
        let thresholds = AlertThresholds::default();
        assert_eq!(thresholds.min_success_rate, 0.7);
        assert_eq!(thresholds.max_failure_rate, 0.3);
    }

    #[test]
    fn test_health_checker() {
        let mut checker = HealthChecker::new();
        assert!(!checker.overall_health());
        
        checker.update_component_health("database", true);
        checker.update_component_health("rpc", true);
        checker.update_component_health("metrics", true);
        
        assert!(checker.overall_health());
        assert_eq!(checker.health_score(), 1.0);
    }
}
