use solana_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::{
    pubkey::Pubkey,
    signature::Signature,
    transaction::Transaction,
    commitment_config::CommitmentConfig,
    clock::Slot,
};
use std::collections::{HashMap, VecDeque, HashSet};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::sync::{RwLock, RwLockReadGuard, RwLockWriteGuard};
use tokio::time::timeout;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use rand::{Rng, SeedableRng};
use rand_chacha::ChaCha20Rng;

#[derive(Error, Debug)]
pub enum CounterStrategyError {
    #[error("Lock timeout: {0}")]
    LockTimeout(String),
    #[error("Pattern detection failed: {0}")]
    PatternDetectionError(String),
    #[error("Resource limit exceeded: {0}")]
    ResourceLimitExceeded(String),
    #[error("Market conditions unsafe: {0}")]
    UnsafeMarketConditions(String),
    #[error("Configuration error: {0}")]
    ConfigurationError(String),
    #[error("Computation error: {0}")]
    ComputationError(String),
}

pub type Result<T> = std::result::Result<T, CounterStrategyError>;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CounterStrategyConfig {
    pub pattern_window_size: usize,
    pub competitor_tracking_limit: usize,
    pub pattern_confidence_threshold: f64,
    pub lock_timeout_ms: u64,
    pub max_priority_fee: u64,
    pub min_priority_fee: u64,
    pub max_compute_units: u64,
    pub mev_protection_threshold: u64,
    pub sandwich_detection_window_ms: u64,
    pub max_decoy_transactions: u8,
    pub cleanup_interval_seconds: u64,
    pub max_memory_mb: usize,
}

impl Default for CounterStrategyConfig {
    fn default() -> Self {
        Self {
            pattern_window_size: 100,
            competitor_tracking_limit: 50,
            pattern_confidence_threshold: 0.85,
            lock_timeout_ms: 100,
            max_priority_fee: 500_000,
            min_priority_fee: 5_000,
            max_compute_units: 1_400_000,
            mev_protection_threshold: 100_000,
            sandwich_detection_window_ms: 500,
            max_decoy_transactions: 3,
            cleanup_interval_seconds: 300,
            max_memory_mb: 512,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompetitorPattern {
    pub wallet: Pubkey,
    pub pattern_type: PatternType,
    pub confidence: f64,
    pub frequency: f64,
    pub avg_priority_fee: u64,
    pub success_rate: f64,
    pub last_seen: Instant,
    pub transaction_history: VecDeque<TransactionMetadata>,
    pub total_volume: u64,
    pub win_rate: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum PatternType {
    Sandwich,
    FrontRunner,
    Arbitrageur,
    Liquidator,
    HighFrequencyTrader,
    MEVBot,
    Unknown,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransactionMetadata {
    pub signature: String,
    pub timestamp: Instant,
    pub priority_fee: u64,
    pub compute_units: u64,
    pub target_program: Pubkey,
    pub transaction_size: usize,
    pub slot: Slot,
    pub success: bool,
}

#[derive(Debug, Clone)]
pub struct CounterStrategy {
    config: Arc<CounterStrategyConfig>,
    patterns: Arc<RwLock<HashMap<Pubkey, CompetitorPattern>>>,
    sandwich_detections: Arc<RwLock<VecDeque<SandwichAttempt>>>,
    mev_protection: Arc<RwLock<MEVProtectionState>>,
    response_strategies: Arc<RwLock<HashMap<PatternType, ResponseStrategy>>>,
    performance_metrics: Arc<RwLock<PerformanceMetrics>>,
    circuit_breaker: Arc<RwLock<CircuitBreaker>>,
    rng: Arc<Mutex<ChaCha20Rng>>,
    last_cleanup: Arc<RwLock<Instant>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SandwichAttempt {
    pub attacker: Pubkey,
    pub victim_tx: String,
    pub front_tx: String,
    pub back_tx: Option<String>,
    pub detected_at: Instant,
    pub pool: Pubkey,
    pub estimated_profit: u64,
}

#[derive(Debug, Clone)]
pub struct MEVProtectionState {
    pub active_protection: bool,
    pub decoy_transactions: Vec<Transaction>,
    pub obfuscation_level: u8,
    pub dynamic_routing: bool,
    pub priority_fee_randomization: (u64, u64),
    pub last_update: Instant,
}

#[derive(Debug, Clone)]
pub struct ResponseStrategy {
    pub base_priority_multiplier: f64,
    pub timing_offset_ms: i64,
    pub compute_unit_padding: u64,
    pub use_decoy_transactions: bool,
    pub adaptive_routing: bool,
    pub fee_escalation_rate: f64,
    pub max_retries: u8,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct PerformanceMetrics {
    pub total_competitors_tracked: usize,
    pub sandwich_attacks_prevented: u64,
    pub successful_counter_trades: u64,
    pub failed_counter_trades: u64,
    pub mev_extracted: u64,
    pub average_response_time_ms: f64,
    pub total_gas_spent: u64,
    pub total_profit: i64,
}

#[derive(Debug, Clone)]
struct CircuitBreaker {
    failure_count: u32,
    last_failure: Option<Instant>,
    is_open: bool,
    threshold: u32,
    reset_duration: Duration,
}

impl CounterStrategy {
    pub fn new(config: CounterStrategyConfig) -> Result<Self> {
        let mut response_strategies = HashMap::new();
        
        response_strategies.insert(PatternType::Sandwich, ResponseStrategy {
            base_priority_multiplier: 1.15,
            timing_offset_ms: -10,
            compute_unit_padding: 50_000,
            use_decoy_transactions: true,
            adaptive_routing: true,
            fee_escalation_rate: 1.25,
            max_retries: 3,
        });
        
        response_strategies.insert(PatternType::FrontRunner, ResponseStrategy {
            base_priority_multiplier: 1.2,
            timing_offset_ms: -15,
            compute_unit_padding: 75_000,
            use_decoy_transactions: true,
            adaptive_routing: true,
            fee_escalation_rate: 1.3,
            max_retries: 2,
        });
        
        response_strategies.insert(PatternType::Arbitrageur, ResponseStrategy {
            base_priority_multiplier: 1.1,
            timing_offset_ms: -5,
            compute_unit_padding: 25_000,
            use_decoy_transactions: false,
            adaptive_routing: true,
            fee_escalation_rate: 1.15,
            max_retries: 4,
        });
        
        response_strategies.insert(PatternType::MEVBot, ResponseStrategy {
            base_priority_multiplier: 1.25,
            timing_offset_ms: -20,
            compute_unit_padding: 100_000,
            use_decoy_transactions: true,
            adaptive_routing: true,
            fee_escalation_rate: 1.4,
            max_retries: 2,
        });
        
        let rng = ChaCha20Rng::from_entropy();
        
        Ok(Self {
            config: Arc::new(config),
            patterns: Arc::new(RwLock::new(HashMap::new())),
            sandwich_detections: Arc::new(RwLock::new(VecDeque::new())),
            mev_protection: Arc::new(RwLock::new(MEVProtectionState {
                active_protection: true,
                decoy_transactions: Vec::new(),
                obfuscation_level: 3,
                dynamic_routing: true,
                priority_fee_randomization: (1_000, 10_000),
                last_update: Instant::now(),
            })),
            response_strategies: Arc::new(RwLock::new(response_strategies)),
            performance_metrics: Arc::new(RwLock::new(PerformanceMetrics::default())),
            circuit_breaker: Arc::new(RwLock::new(CircuitBreaker {
                failure_count: 0,
                last_failure: None,
                is_open: false,
                threshold: 5,
                reset_duration: Duration::from_secs(60),
            })),
            rng: Arc::new(Mutex::new(rng)),
            last_cleanup: Arc::new(RwLock::new(Instant::now())),
        })
    }
    
    pub async fn analyze_transaction(&self, tx_meta: &TransactionMetadata) -> Result<Option<CompetitorPattern>> {
        self.check_circuit_breaker().await?;
        self.maybe_cleanup().await?;
        
        let mut patterns = self.acquire_write_lock(&self.patterns).await?;
        
        if patterns.len() >= self.config.competitor_tracking_limit {
            self.prune_inactive_competitors(&mut patterns)?;
        }
        
        let pattern_type = self.classify_transaction_pattern(tx_meta)?;
        let confidence = self.calculate_pattern_confidence(tx_meta, &pattern_type)?;
        
        if confidence >= self.config.pattern_confidence_threshold {
            let wallet = tx_meta.target_program;
            
            let pattern = patterns.entry(wallet).or_insert_with(|| CompetitorPattern {
                wallet,
                pattern_type: pattern_type.clone(),
                confidence: 0.0,
                frequency: 0.0,
                avg_priority_fee: 0,
                success_rate: 0.0,
                last_seen: Instant::now(),
                transaction_history: VecDeque::new(),
                total_volume: 0,
                win_rate: 0.0,
            });
            
            pattern.transaction_history.push_back(tx_meta.clone());
            if pattern.transaction_history.len() > self.config.pattern_window_size {
                pattern.transaction_history.pop_front();
            }
            
            pattern.total_volume = pattern.total_volume.saturating_add(tx_meta.priority_fee);
            
            self.update_pattern_metrics(pattern)?;
            
            if pattern.pattern_type == PatternType::Sandwich {
                let detection = self.detect_sandwich_attack(pattern, tx_meta)?;
                if let Some(detection) = detection {
                    let mut sandwich_detections = self.acquire_write_lock(&self.sandwich_detections).await?;
                    sandwich_detections.push_back(detection);
                    if sandwich_detections.len() > 1000 {
                        sandwich_detections.drain(0..500);
                    }
                }
            }
            
            return Ok(Some(pattern.clone()));
        }
        
        Ok(None)
    }
    
    pub async fn generate_counter_response(
        &self,
        competitor: &CompetitorPattern,
        original_priority_fee: u64,
        compute_units: u64,
    ) -> Result<CounterResponse> {
        self.check_circuit_breaker().await?;
        
        let strategies = self.acquire_read_lock(&self.response_strategies).await?;
        let strategy = strategies.get(&competitor.pattern_type)
            .or_else(|| strategies.get(&PatternType::Unknown))
            .ok_or_else(|| CounterStrategyError::ConfigurationError("No strategy found".to_string()))?;
        
        let mev_state = self.acquire_read_lock(&self.mev_protection).await?;
        
        let adjusted_priority_fee = self.calculate_competitive_fee(
            original_priority_fee,
            competitor.avg_priority_fee,
            strategy.base_priority_multiplier,
            strategy.fee_escalation_rate,
            competitor.success_rate,
        )?;
        
        let adjusted_compute_units = compute_units
            .saturating_add(strategy.compute_unit_padding)
            .min(self.config.max_compute_units);
        
        let timing_adjustment = if competitor.pattern_type == PatternType::FrontRunner {
            strategy.timing_offset_ms - (competitor.frequency * 5.0).min(50.0) as i64
        } else {
            strategy.timing_offset_ms
        };
        
        let jitter = self.generate_timing_jitter().await?;
        
        Ok(CounterResponse {
            priority_fee: adjusted_priority_fee,
            compute_units: adjusted_compute_units,
            timing_offset_ms: timing_adjustment + jitter,
            use_decoy: strategy.use_decoy_transactions && mev_state.active_protection,
            routing_strategy: if strategy.adaptive_routing {
                RoutingStrategy::Dynamic
            } else {
                RoutingStrategy::Direct
            },
            obfuscation_level: mev_state.obfuscation_level,
            max_retries: strategy.max_retries,
        })
    }
    
    pub async fn detect_mev_opportunity(
        &self,
        recent_transactions: &[TransactionMetadata],
        current_slot: Slot,
    ) -> Result<Option<MEVOpportunity>> {
        let patterns = self.acquire_read_lock(&self.patterns).await?;
        let mut opportunity_score = 0.0;
        let mut target_pool = None;
        
        for tx in recent_transactions.iter().rev().take(20) {
            if current_slot.saturating_sub(tx.slot) <= 2 {
                let tx_score = self.calculate_mev_score(tx, &patterns)?;
                if tx_score > opportunity_score {
                    opportunity_score = tx_score;
                    target_pool = Some(tx.target_program);
                }
            }
        }
        
        if opportunity_score > 0.7 && target_pool.is_some() {
            let expected_profit = self.estimate_mev_profit(recent_transactions, opportunity_score)?;
            
            Ok(Some(MEVOpportunity {
                pool: target_pool.unwrap(),
                expected_profit,
                confidence: opportunity_score,
                time_sensitive: true,
                priority_level: match opportunity_score {
                    x if x > 0.9 => PriorityLevel::Critical,
                    x if x > 0.8 => PriorityLevel::High,
                    x if x > 0.7 => PriorityLevel::Medium,
                    _ => PriorityLevel::Low,
                },
            }))
        } else {
            Ok(None)
        }
    }
    
    async fn check_circuit_breaker(&self) -> Result<()> {
        let mut breaker = self.acquire_write_lock(&self.circuit_breaker).await?;
        
        if let Some(last_failure) = breaker.last_failure {
            if last_failure.elapsed() > breaker.reset_duration {
                breaker.failure_count = 0;
                breaker.is_open = false;
                breaker.last_failure = None;
            }
        }
        
        if breaker.is_open {
            return Err(CounterStrategyError::UnsafeMarketConditions("Circuit breaker is open".to_string()));
        }
        
        Ok(())
    }
    
    async fn maybe_cleanup(&self) -> Result<()> {
        let mut last_cleanup = self.acquire_write_lock(&self.last_cleanup).await?;
        
        if last_cleanup.elapsed() > Duration::from_secs(self.config.cleanup_interval_seconds) {
            *last_cleanup = Instant::now();
            drop(last_cleanup);
            
            let mut patterns = self.acquire_write_lock(&self.patterns).await?;
            self.prune_inactive_competitors(&mut patterns)?;
            
            let mut sandwich_detections = self.acquire_write_lock(&self.sandwich_detections).await?;
            let cutoff = Instant::now() - Duration::from_secs(3600);
            sandwich_detections.retain(|d| d.detected_at > cutoff);
        }
        
        Ok(())
    }
    
    async fn acquire_read_lock<T>(&self, lock: &Arc<RwLock<T>>) -> Result<RwLockReadGuard<'_, T>> {
        timeout(
            Duration::from_millis(self.config.lock_timeout_ms),
            lock.read()
        ).await
        .map_err(|_| CounterStrategyError::LockTimeout("Read lock acquisition timeout".to_string()))
    }
    
    async fn acquire_write_lock<T>(&self, lock: &Arc<RwLock<T>>) -> Result<RwLockWriteGuard<'_, T>> {
        timeout(
            Duration::from_millis(self.config.lock_timeout_ms),
            lock.write()
        ).await
        .map_err(|_| CounterStrategyError::LockTimeout("Write lock acquisition timeout".to_string()))
    }
    
    fn classify_transaction_pattern(&self, tx_meta: &TransactionMetadata) -> Result<PatternType> {
        let fee_ratio = tx_meta.priority_fee as f64 / self.config.max_priority_fee as f64;
        let compute_ratio = tx_meta.compute_units as f64 / self.config.max_compute_units as f64;
        
        let pattern = match (fee_ratio, compute_ratio) {
            (f, c) if f > 0.8 && c > 0.7 => PatternType::MEVBot,
            (f, c) if f > 0.6 && c > 0.5 => PatternType::Sandwich,
            (f, c) if f > 0.5 && c < 0.3 => PatternType::FrontRunner,
            (f, _) if f > 0.4 => PatternType::Arbitrageur,
            (f, _) if f > 0.3 => PatternType::HighFrequencyTrader,
            _ => PatternType::Unknown,
        };
        
        Ok(pattern)
    }
    
    fn calculate_pattern_confidence(&self, tx_meta: &TransactionMetadata, pattern_type: &PatternType) -> Result<f64> {
        let base_confidence = match pattern_type {
            PatternType::MEVBot => 0.6,
            PatternType::Sandwich => 0.5,
            PatternType::FrontRunner => 0.4,
            PatternType::Arbitrageur => 0.5,
            PatternType::HighFrequencyTrader => 0.3,
            PatternType::Liquidator => 0.4,
            PatternType::Unknown => 0.1,
        };
        
        let fee_factor = (tx_meta.priority_fee as f64 / 100_000.0).min(0.3);
        let compute_factor = (tx_meta.compute_units as f64 / 500_000.0).min(0.2);
        let size_factor = if tx_meta.transaction_size > 1000 { 0.1 } else { 0.0 };
        
        Ok((base_confidence + fee_factor + compute_factor + size_factor).min(1.0))
    }
    
    fn update_pattern_metrics(&self, pattern: &mut CompetitorPattern) -> Result<()> {
        let history_len = pattern.transaction_history.len();
        if history_len == 0 {
            return Ok(());
        }
        
        let recent_window = Duration::from_secs(300);
        let now = Instant::now();
        
        let recent_txs: Vec<_> = pattern.transaction_history.iter()
            .filter(|tx| now.duration_since(tx.timestamp) < recent_window)
            .collect();
        
        if recent_txs.is_empty() {
            return Ok(());
        }
        
        let total_fee: u64 = recent_txs.iter()
            .map(|tx| tx.priority_fee)
            .sum();
        pattern.avg_priority_fee = total_fee.checked_div(recent_txs.len() as u64).unwrap_or(0);
        
        let successful_txs = recent_txs.iter().filter(|tx| tx.success).count();
        pattern.success_rate = successful_txs as f64 / recent_txs.len() as f64;
        
        let time_span = recent_txs.last()
            .and_then(|last| recent_txs.first().map(|first| 
                last.timestamp.duration_since(first.timestamp).as_secs_f64()
            ))
            .unwrap_or(1.0);
        
        pattern.frequency = recent_txs.len() as f64 / time_span.max(1.0);
        
        let alpha = 0.2;
        pattern.confidence = pattern.confidence * (1.0 - alpha) + pattern.success_rate * alpha;
        
        pattern.last_seen = now;
        
        Ok(())
    }
    
    fn detect_sandwich_attack(&self, pattern: &CompetitorPattern, tx_meta: &TransactionMetadata) -> Result<Option<SandwichAttempt>> {
        let recent_similar = pattern.transaction_history.iter()
            .filter(|tx| tx.target_program == tx_meta.target_program)
            .filter(|tx| tx_meta.timestamp.duration_since(tx.timestamp) < 
                Duration::from_millis(self.config.sandwich_detection_window_ms))
            .count();
        
        if recent_similar >= 2 {
            let estimated_profit = (tx_meta.priority_fee as f64 * 0.1) as u64;
            
            Ok(Some(SandwichAttempt {
                attacker: pattern.wallet,
                victim_tx: String::new(),
                front_tx: tx_meta.signature.clone(),
                back_tx: None,
                detected_at: Instant::now(),
                pool: tx_meta.target_program,
                estimated_profit,
            }))
        } else {
            Ok(None)
        }
    }
    
    fn calculate_competitive_fee(
        &self,
        base_fee: u64,
        competitor_avg_fee: u64,
        multiplier: f64,
        escalation_rate: f64,
        competitor_success_rate: f64,
    ) -> Result<u64> {
        let competitive_base = competitor_avg_fee.max(base_fee);
        let escalation_factor = 1.0 + (competitor_success_rate * escalation_rate - 1.0).max(0.0);
        let adjusted_fee = (competitive_base as f64 * multiplier * escalation_factor) as u64;
        
        let final_fee = adjusted_fee
            .min(self.config.max_priority_fee)
            .max(self.config.min_priority_fee);
        
        Ok(final_fee)
    }
    
    fn calculate_mev_score(&self, tx: &TransactionMetadata, patterns: &HashMap<Pubkey, CompetitorPattern>) -> Result<f64> {
        let base_score = (tx.priority_fee as f64 / self.config.max_priority_fee as f64).min(0.5);
        
        let competition_score = patterns.values()
            .filter(|p| p.last_seen.elapsed() < Duration::from_secs(30))
            .filter(|p| matches!(p.pattern_type, PatternType::MEVBot | PatternType::Arbitrageur))
            .count() as f64 / 10.0;
        
        let urgency_score = if tx.compute_units > 300_000 { 0.3 } else { 0.1 };
        
        Ok((base_score + competition_score.min(0.3) + urgency_score).min(1.0))
    }
    
    fn estimate_mev_profit(&self, transactions: &[TransactionMetadata], opportunity_score: f64) -> Result<u64> {
        let avg_fee: u64 = transactions.iter()
            .map(|tx| tx.priority_fee)
            .sum::<u64>()
            .checked_div(transactions.len() as u64)
            .unwrap_or(0);
        
        let base_profit = (avg_fee as f64 * opportunity_score * 10.0) as u64;
        Ok(base_profit.min(1_000_000_000))
    }
    
    async fn generate_timing_jitter(&self) -> Result<i64> {
        let mut rng = self.rng.lock()
            .map_err(|_| CounterStrategyError::LockTimeout("RNG lock poisoned".to_string()))?;
        
        let jitter: i64 = rng.gen_range(-20..=20);
        Ok(jitter)
    }
    
    fn prune_inactive_competitors(&self, patterns: &mut HashMap<Pubkey, CompetitorPattern>) -> Result<()> {
        let inactive_threshold = Duration::from_secs(300);
        let now = Instant::now();
        
        let initial_size = patterns.len();
        patterns.retain(|_, pattern| {
            now.duration_since(pattern.last_seen) < inactive_threshold
        });
        
        let pruned = initial_size - patterns.len();
        if pruned > 0 {
            log::debug!("Pruned {} inactive competitors", pruned);
        }
        
        Ok(())
    }
    
    pub async fn update_mev_protection(&self, level: u8, enable_dynamic_routing: bool) -> Result<()> {
        let mut protection = self.acquire_write_lock(&self.mev_protection).await?;
        protection.obfuscation_level = level.min(5);
        protection.dynamic_routing = enable_dynamic_routing;
        protection.last_update = Instant::now();
        Ok(())
    }
    
    pub async fn get_adaptive_response(
        &self,
        target_pool: &Pubkey,
        transaction_value: u64,
    ) -> Result<AdaptiveResponse> {
        let patterns = self.acquire_read_lock(&self.patterns).await?;
        let protection = self.acquire_read_lock(&self.mev_protection).await?;
        
        let active_competitors: Vec<_> = patterns.values()
            .filter(|p| p.last_seen.elapsed() < Duration::from_secs(60))
            .filter(|p| matches!(p.pattern_type, 
                PatternType::MEVBot | PatternType::FrontRunner | PatternType::Sandwich))
            .collect();
        
        let threat_level = self.calculate_threat_level(&active_competitors, transaction_value)?;
        
        let priority_fee_range = match threat_level {
            x if x > 0.8 => (50_000, 200_000),
            x if x > 0.5 => (20_000, 100_000),
            _ => (5_000, 50_000),
        };
        
        let compute_buffer = match threat_level {
            x if x > 0.8 => 150_000,
            x if x > 0.5 => 75_000,
            _ => 25_000,
        };
        
        let decoy_count = if threat_level > 0.7 { 
            2.min(self.config.max_decoy_transactions) 
        } else { 
            0 
        };
        
        Ok(AdaptiveResponse {
            priority_fee_range,
            compute_buffer,
            use_multiple_routes: protection.dynamic_routing && threat_level > 0.6,
            decoy_count,
            submission_delay_ms: if threat_level < 0.3 { 10 } else { 0 },
            threat_level,
        })
    }
    
    fn calculate_threat_level(
        &self,
        competitors: &[&CompetitorPattern],
        transaction_value: u64,
    ) -> Result<f64> {
        if competitors.is_empty() {
            return Ok(0.1);
        }
        
        let competitor_strength = competitors.iter()
            .map(|c| c.confidence * c.success_rate)
            .sum::<f64>() / competitors.len() as f64;
        
        let value_factor = (transaction_value as f64 / 1_000_000_000.0).min(1.0).powf(0.5);
        
        let mev_bot_factor = competitors.iter()
            .filter(|c| matches!(c.pattern_type, PatternType::MEVBot))
            .count() as f64 / 5.0;
        
        let threat_level = (competitor_strength * 0.4 + value_factor * 0.3 + mev_bot_factor * 0.3)
            .min(1.0)
            .max(0.0);
        
        Ok(threat_level)
    }
    
    pub async fn record_transaction_result(
        &self,
        tx_signature: &str,
        success: bool,
        profit: Option<i64>,
    ) -> Result<()> {
        let mut metrics = self.acquire_write_lock(&self.performance_metrics).await?;
        
        if success {
            metrics.successful_counter_trades += 1;
            if let Some(p) = profit {
                metrics.total_profit = metrics.total_profit.saturating_add(p);
            }
        } else {
            metrics.failed_counter_trades += 1;
            let mut breaker = self.acquire_write_lock(&self.circuit_breaker).await?;
            breaker.failure_count += 1;
            breaker.last_failure = Some(Instant::now());
            
            if breaker.failure_count >= breaker.threshold {
                breaker.is_open = true;
                log::warn!("Circuit breaker opened after {} failures", breaker.failure_count);
            }
        }
        
        Ok(())
    }
    
    pub async fn get_performance_metrics(&self) -> Result<PerformanceMetrics> {
        let metrics = self.acquire_read_lock(&self.performance_metrics).await?;
        Ok(metrics.clone())
    }
    
    pub async fn reset_circuit_breaker(&self) -> Result<()> {
        let mut breaker = self.acquire_write_lock(&self.circuit_breaker).await?;
        breaker.failure_count = 0;
        breaker.is_open = false;
        breaker.last_failure = None;
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CounterResponse {
    pub priority_fee: u64,
    pub compute_units: u64,
    pub timing_offset_ms: i64,
    pub use_decoy: bool,
    pub routing_strategy: RoutingStrategy,
    pub obfuscation_level: u8,
    pub max_retries: u8,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RoutingStrategy {
    Direct,
    Dynamic,
    MultiPath { paths: u8 },
    Stealth,
}

#[derive(Debug, Clone)]
pub struct MEVOpportunity {
    pub pool: Pubkey,
    pub expected_profit: u64,
    pub confidence: f64,
    pub time_sensitive: bool,
    pub priority_level: PriorityLevel,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PriorityLevel {
    Critical,
    High,
    Medium,
    Low,
}

#[derive(Debug, Clone)]
pub struct AdaptiveResponse {
    pub priority_fee_range: (u64, u64),
    pub compute_buffer: u64,
    pub use_multiple_routes: bool,
    pub decoy_count: u8,
    pub submission_delay_ms: u64,
    pub threat_level: f64,
}

pub struct CounterStrategyBuilder {
    config: CounterStrategyConfig,
}

impl CounterStrategyBuilder {
    pub fn new() -> Self {
        Self {
            config: CounterStrategyConfig::default(),
        }
    }
    
    pub fn with_pattern_window_size(mut self, size: usize) -> Self {
        self.config.pattern_window_size = size;
        self
    }
    
    pub fn with_competitor_limit(mut self, limit: usize) -> Self {
        self.config.competitor_tracking_limit = limit;
        self
    }
    
    pub fn with_confidence_threshold(mut self, threshold: f64) -> Self {
        self.config.pattern_confidence_threshold = threshold.max(0.0).min(1.0);
        self
    }
    
    pub fn with_max_priority_fee(mut self, fee: u64) -> Self {
        self.config.max_priority_fee = fee;
        self
    }
    
    pub fn with_mev_protection_threshold(mut self, threshold: u64) -> Self {
        self.config.mev_protection_threshold = threshold;
        self
    }
    
    pub fn build(self) -> Result<CounterStrategy> {
        CounterStrategy::new(self.config)
    }
}

impl Default for CounterStrategyBuilder {
    fn default() -> Self {
        Self::new()
    }
}

pub async fn analyze_market_conditions(
    rpc_client: &RpcClient,
    recent_slots: u64,
) -> Result<MarketConditions> {
    let slot = rpc_client.get_slot()
        .await
        .map_err(|e| CounterStrategyError::ComputationError(e.to_string()))?;
    
    let recent_blockhash = rpc_client.get_latest_blockhash()
        .await
        .map_err(|e| CounterStrategyError::ComputationError(e.to_string()))?;
    
    Ok(MarketConditions {
        current_slot: slot,
        recent_blockhash: recent_blockhash.to_string(),
        network_congestion: 0.5,
        mev_activity_level: 0.3,
        sandwich_risk_factor: 0.4,
        estimated_base_fee: 5_000,
        timestamp: Instant::now(),
    })
}

#[derive(Debug, Clone)]
pub struct MarketConditions {
    pub current_slot: Slot,
    pub recent_blockhash: String,
    pub network_congestion: f64,
    pub mev_activity_level: f64,
    pub sandwich_risk_factor: f64,
    pub estimated_base_fee: u64,
    pub timestamp: Instant,
}

pub fn calculate_optimal_submission_timing(
    threat_level: f64,
    slot_progress: f64,
    competitor_count: usize,
) -> Result<SubmissionTiming> {
    let ideal_slot_fraction = match threat_level {
        x if x > 0.8 => 0.1,
        x if x > 0.5 => 0.2,
        x if x > 0.3 => 0.3,
        _ => 0.5,
    };
    
    let competitor_adjustment = (competitor_count as f64 / 20.0).min(0.2);
    let target_slot_fraction = (ideal_slot_fraction - competitor_adjustment).max(0.05);
    
    let jitter_range = match threat_level {
        x if x > 0.7 => 50,
        x if x > 0.4 => 30,
        _ => 10,
    };
    
    Ok(SubmissionTiming {
        target_slot_fraction,
        jitter_ms: jitter_range,
        use_randomized_timing: threat_level > 0.5,
        priority_boost_at_fraction: 0.8,
    })
}

#[derive(Debug, Clone)]
pub struct SubmissionTiming {
    pub target_slot_fraction: f64,
    pub jitter_ms: u64,
    pub use_randomized_timing: bool,
    pub priority_boost_at_fraction: f64,
}

pub fn estimate_competitive_advantage(
    our_params: &CounterResponse,
    competitor_patterns: &[CompetitorPattern],
    market_conditions: &MarketConditions,
) -> Result<f64> {
    if competitor_patterns.is_empty() {
        return Ok(0.8);
    }
    
    let avg_competitor_fee = competitor_patterns.iter()
        .map(|p| p.avg_priority_fee)
        .sum::<u64>()
        .checked_div(competitor_patterns.len() as u64)
        .unwrap_or(market_conditions.estimated_base_fee);
    
    let fee_advantage = (our_params.priority_fee as f64 / avg_competitor_fee as f64).min(2.0);
    
    let timing_advantage = match our_params.timing_offset_ms {
        x if x < -10 => 1.2,
        x if x < 0 => 1.1,
        _ => 1.0,
    };
    
    let routing_advantage = match our_params.routing_strategy {
        RoutingStrategy::Stealth => 1.3,
        RoutingStrategy::MultiPath { .. } => 1.2,
        RoutingStrategy::Dynamic => 1.1,
        RoutingStrategy::Direct => 1.0,
    };
    
    let base_advantage = (fee_advantage * 0.4 + timing_advantage * 0.3 + routing_advantage * 0.3)
        .min(2.0)
        .max(0.1);
    
    let market_penalty = market_conditions.network_congestion * 0.2;
    
    Ok((base_advantage - market_penalty).max(0.1))
}

pub async fn execute_counter_strategy(
    strategy: &CounterStrategy,
    transaction: Transaction,
    params: &CounterResponse,
) -> Result<ExecutionResult> {
    let start_time = Instant::now();
    
    if params.timing_offset_ms > 0 {
        tokio::time::sleep(Duration::from_millis(params.timing_offset_ms as u64)).await;
    }
    
    let mut attempts = 0;
    let mut last_error = None;
    
    while attempts < params.max_retries {
        attempts += 1;
        
        match submit_with_strategy(&transaction, params, attempts).await {
            Ok(signature) => {
                let elapsed = start_time.elapsed();
                
                return Ok(ExecutionResult {
                    signature,
                    attempts,
                    total_time_ms: elapsed.as_millis() as u64,
                    final_priority_fee: params.priority_fee,
                    success: true,
                });
            }
            Err(e) => {
                last_error = Some(e);
                
                if attempts < params.max_retries {
                    let backoff = Duration::from_millis(100 * attempts as u64);
                    tokio::time::sleep(backoff).await;
                }
            }
        }
    }
    
    Err(last_error.unwrap_or_else(|| 
        CounterStrategyError::ComputationError("Max retries exceeded".to_string())
    ))
}

async fn submit_with_strategy(
    transaction: &Transaction,
    params: &CounterResponse,
    attempt: u8,
) -> Result<String> {
    let fee_escalation = 1.0 + (attempt as f64 * 0.1);
    let adjusted_fee = (params.priority_fee as f64 * fee_escalation) as u64;
    
    Ok(format!("simulated_signature_{}", adjusted_fee))
}

#[derive(Debug, Clone)]
pub struct ExecutionResult {
    pub signature: String,
    pub attempts: u8,
    pub total_time_ms: u64,
    pub final_priority_fee: u64,
    pub success: bool,
}

pub fn validate_counter_strategy_params(params: &CounterResponse, config: &CounterStrategyConfig) -> Result<()> {
    if params.priority_fee > config.max_priority_fee {
        return Err(CounterStrategyError::ConfigurationError(
            format!("Priority fee {} exceeds max {}", params.priority_fee, config.max_priority_fee)
        ));
    }
    
    if params.priority_fee < config.min_priority_fee {
        return Err(CounterStrategyError::ConfigurationError(
            format!("Priority fee {} below min {}", params.priority_fee, config.min_priority_fee)
        ));
    }
    
    if params.compute_units > config.max_compute_units {
        return Err(CounterStrategyError::ConfigurationError(
            format!("Compute units {} exceeds max {}", params.compute_units, config.max_compute_units)
        ));
    }
    
    if params.obfuscation_level > 5 {
        return Err(CounterStrategyError::ConfigurationError(
            "Obfuscation level must be between 0-5".to_string()
        ));
    }
    
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[tokio::test]
    async fn test_counter_strategy_creation() {
        let strategy = CounterStrategyBuilder::new()
            .with_pattern_window_size(50)
            .with_confidence_threshold(0.9)
            .build();
        
        assert!(strategy.is_ok());
    }
}
