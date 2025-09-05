use solana_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::commitment_config::CommitmentConfig;

pub fn make_rpc_client(rpc_url: &str) -> RpcClient {
    RpcClient::new_with_commitment(
        rpc_url.to_string(),
        CommitmentConfig::confirmed(), // tuned for MEV bots: confirmed is faster than finalized
    )
}

use solana_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::{
    pubkey::Pubkey,
    signature::Signature,
    commitment_config::CommitmentConfig,
};
use solana_transaction_status::{
    EncodedConfirmedTransactionWithStatusMeta,
    TransactionStatusMeta,
    UiTransactionEncoding,
};
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::sync::{broadcast, Mutex, RwLock as AsyncRwLock, mpsc, RwLock};
use tonic::transport::Channel;
use yellowstone_grpc_client::jito_service_client::JitoServiceClient; // yellowstone-grpc-client = "6.0"
use prometheus::{Encoder, TextEncoder, IntGauge, IntCounter, Registry, Histogram, HistogramOpts}; // prometheus = "0.14"
use lazy_static::lazy_static;
use serde::{Serialize, Deserialize};
use anyhow::{anyhow, Result};
use log::{info, warn, error, debug};
use uuid::Uuid;
use chrono::Utc;
use rusqlite;
use bs58;
use dashmap::DashMap;
use serde_json;
use reqwest;
use pyth_client::Price as PythPrice;
use switchboard_v2::{AggregatorAccountData, VrfAccountData};

#[derive(Debug, Clone)]
pub struct ScoringConfig {
    pub score_update_threshold: usize,
    pub score_refresh_interval: u64,
    pub entropy_threshold: f64,
    pub transition_novelty_threshold: u64,
    pub burst_speed_threshold: f64,
    pub max_transaction_history: usize,
    pub inactive_bot_threshold: u64,
    pub slot_time_ms: u64,
    pub max_tracked_bots: usize,
    pub capability_score_window_secs: u64,
    pub min_tx_for_scoring: usize,
    pub score_update_interval_secs: u64,
    pub max_historical_data_points: usize,
    pub max_latency_tracking: usize,
    pub critical_success_threshold: f64,
    pub high_frequency_threshold: u64,
    pub priority_fee_percentile: f64,
    pub latency_update_threshold: usize,
    pub profit_weight: f64,
    pub consistency_weight: f64,
    pub speed_weight: f64,
    pub success_weight: f64,
    pub gas_efficiency_weight: f64,
    pub pattern_consistency_weight: f64,
    pub volume_weight: f64,
}

impl Default for ScoringConfig {
    fn default() -> Self {
        Self {
            score_update_threshold: 25,
            score_refresh_interval: 60,
            entropy_threshold: 1.2,
            transition_novelty_threshold: 3,
            burst_speed_threshold: 250.0,
            max_transaction_history: 1000,
            inactive_bot_threshold: 86400,
            slot_time_ms: 400,
            max_tracked_bots: 500,
            capability_score_window_secs: 300,
            min_tx_for_scoring: 10,
            score_update_interval_secs: 5,
            max_historical_data_points: 1000,
            max_latency_tracking: 1000,
            critical_success_threshold: 0.85,
            high_frequency_threshold: 100,
            priority_fee_percentile: 0.95,
            latency_update_threshold: 50,
            profit_weight: 0.5,
            consistency_weight: 0.5,
            speed_weight: 0.25,
            success_weight: 0.35,
            gas_efficiency_weight: 0.15,
            pattern_consistency_weight: 0.15,
            volume_weight: 0.10,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RivalBotCapability {
    pub bot_pubkey: String,
    pub score: f64,
    pub arbitrage_rate: f64,
    pub liquidation_rate: f64,
    pub sandwich_rate: f64,
    pub avg_speed_ms: f64,
    pub success_rate: f64,
    pub avg_compute_units: f64,
    pub avg_priority_fee: f64,
    pub pattern_consistency: f64,
    pub avg_profit: f64,
    // New latency metrics
    pub avg_rpc_latency_ms: f64,
    pub avg_confirmation_latency_ms: f64,
    pub avg_block_inclusion_latency_ms: f64,
    pub detected_strategies: Vec<BotStrategy>,
    pub threat_level: ThreatLevel,
    pub anomalies: Vec<String>,
    pub rival_comparison_rank: Option<f64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ThreatLevel {
    Low,
    Medium,
    High,
    Critical,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum BotStrategy {
    Arbitrage,
    Liquidation,
    SandwichAttack,
    FrontRunning,
    BackRunning,
    JitLiquidity,
    CopyTrading,
    Unknown,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransactionMetrics {
    pub signature: String,
    pub bot_pubkey: String,
    pub slot: u64,
    pub timestamp: u64,
    pub success: bool,
    pub priority_fee: u64,
    pub compute_units: u64,
    pub rpc_latency_ms: f64,
    pub confirmation_latency_ms: f64,
    pub block_inclusion_latency_ms: f64,
    pub bundle_id: Option<String>,
    pub tip_bid: Option<u64>,
    pub strategy_type: Vec<BotStrategy>,
    pub profit_estimate: Option<f64>,
    pub oracle_age_secs: Option<u64>,
    pub oracle_confidence: Option<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BotProfile {
    pub pubkey: String,
    pub transactions: VecDeque<TransactionMetrics>,
    pub cumulative_metrics: CumulativeMetrics,
    pub last_score_update: Option<u64>,
    pub last_score_value: Option<f64>,
    pub strategy_transitions: HashMap<(BotStrategy, BotStrategy), u64>,
    pub last_strategy: Option<BotStrategy>,
    pub last_seen: u64, // tracks last activity timestamp
    pub scoring_config: ScoringConfig,
}

impl BotProfile {
    pub fn new(pubkey: String, scoring_config: ScoringConfig) -> Self {
        Self {
            pubkey,
            transactions: VecDeque::new(),
            cumulative_metrics: CumulativeMetrics::default(),
            last_score_update: None,
            last_score_value: None,
            strategy_transitions: HashMap::new(),
            last_strategy: None,
            last_seen: 0,
            scoring_config,
        }
    }

    pub fn add_transaction(&mut self, metrics: TransactionMetrics) {
        self.last_seen = metrics.timestamp; // Update last activity time
        self.transactions.push_back(metrics);
        if self.transactions.len() > self.scoring_config.max_transaction_history {
            self.transactions.pop_front();
        }
        self.update_cumulative_metrics(&metrics);
    }

    pub fn update_cumulative_metrics(&mut self, metrics: &TransactionMetrics) {
        self.cumulative_metrics.total_txs += 1;
        if metrics.success {
            self.cumulative_metrics.successful_txs += 1;
        }
        self.cumulative_metrics.total_priority_fees += metrics.priority_fee;
        self.cumulative_metrics.total_compute_units += metrics.compute_units;
        *self.cumulative_metrics.strategy_counts.entry(metrics.strategy_type[0].clone()).or_insert(0) += 1;
        self.cumulative_metrics.slot_intervals.push_back(metrics.slot);
        self.cumulative_metrics.rpc_latency_samples.push_back(metrics.rpc_latency_ms);
        self.cumulative_metrics.confirmation_latency_samples.push_back(metrics.confirmation_latency_ms);
        self.cumulative_metrics.block_inclusion_latency_samples.push_back(metrics.block_inclusion_latency_ms);
        if let Some(profit) = metrics.profit_estimate {
            self.cumulative_metrics.profits.push(profit);
        }
    }

    pub async fn should_update_score(&self) -> bool {
        use std::time::{SystemTime, UNIX_EPOCH};

        // 1. Hard gate — if we don't have a baseline of data, skip
        if self.transactions.len() < self.scoring_config.score_update_threshold {
            return false;
        }

        // 2. Time-based decay — update at least once per refresh window
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        if let Some(last_update) = self.last_score_update {
            if now.saturating_sub(last_update) > self.scoring_config.score_refresh_interval {
                return true;
            }
        }

        // 3. Strategy entropy — if entropy is above threshold, force update
        let entropy = self.compute_strategy_entropy();
        if entropy > self.scoring_config.entropy_threshold {
            return true;
        }

        // 4. Transition anomaly — if recent transactions show new or rare strategy switches
        if let Some(last) = self.transactions.back() {
            if let Some(prev) = &self.last_strategy {
                if &last.strategy_type[0] != prev {
                    let key = (prev.clone(), last.strategy_type[0].clone());
                    let count = self.strategy_transitions.get(&key).copied().unwrap_or(0);
                    if count < self.scoring_config.transition_novelty_threshold {
                        return true;
                    }
                }
            }
        }

        // 5. Burst activity — if avg speed is unusually high, refresh more often
        let avg_speed = self.cumulative_metrics.avg_speed_ms();
        if avg_speed > self.scoring_config.burst_speed_threshold {
            return true;
        }

        // Otherwise, skip — no significant signal yet
        false
    }

    pub fn compute_strategy_entropy(&self) -> f64 {
        let total: u64 = self.cumulative_metrics.strategy_counts.values().sum();
        if total == 0 { return 0.0; }

        self.cumulative_metrics.strategy_counts.values()
            .map(|&count| {
                let p = count as f64 / total as f64;
                -p * p.log2()
            })
            .sum()
    }
}

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct CumulativeMetrics {
    pub total_txs: u64,
    pub successful_txs: u64,
    pub total_priority_fees: u64,
    pub total_compute_units: u64,
    pub strategy_counts: HashMap<BotStrategy, u64>,
    pub slot_intervals: VecDeque<u64>,
    pub rpc_latency_samples: VecDeque<f64>,
    pub confirmation_latency_samples: VecDeque<f64>,
    pub block_inclusion_latency_samples: VecDeque<f64>,
    pub profits: Vec<f64>,
}

impl CumulativeMetrics {
    pub fn avg_speed_ms(&self) -> Option<f64> {
        if self.slot_intervals.len() < 2 {
            return None;
        }
        
        // Prune old slots if needed
        if self.slot_intervals.len() > 1000 {
            self.slot_intervals.pop_front();
        }
        
        let total_diff: u64 = self.slot_intervals
            .iter()
            .tuple_windows()
            .map(|(a, b)| b.saturating_sub(*a))
            .sum();
            
        Some(total_diff as f64 / (self.slot_intervals.len() - 1) as f64 * 400.0)
    }
}

pub struct RivalBotCapabilityScorer {
    rpc_client: Arc<RpcClient>,
    bot_profiles: Arc<DashMap<String, Arc<tokio::sync::RwLock<BotProfile>>>>,
    capability_scores: Arc<DashMap<String, f64>>,
    known_bot_addresses: Arc<AsyncRwLock<HashMap<String, bool>>>,
    scoring_config: ScoringConfig,
    jito_grpc_client: JitoServiceClient<Channel>,
    config: Config,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ScoringWeights {
    speed_weight: f64,
    success_weight: f64,
    gas_efficiency_weight: f64,
    pattern_consistency_weight: f64,
    volume_weight: f64,
    profit_weight: f64,
    consistency_weight: f64,
}

impl Default for ScoringWeights {
    fn default() -> Self {
        Self {
            speed_weight: 0.25,
            success_weight: 0.35,
            gas_efficiency_weight: 0.15,
            pattern_consistency_weight: 0.15,
            volume_weight: 0.10,
            profit_weight: 0.5,
            consistency_weight: 0.5,
        }
    }
}

impl RivalBotCapabilityScorer {
    pub async fn new(rpc_endpoint: &str, jito_grpc_endpoint: &str, config: Config) -> Result<Self, Box<dyn std::error::Error>> {
        let rpc_client = Arc::new(make_rpc_client(rpc_endpoint));

        let channel = Channel::from_shared(jito_grpc_endpoint.to_string())?
            .connect().await?;
        let jito_grpc_client = JitoServiceClient::new(channel);

        Ok(Self {
            rpc_client,
            bot_profiles: Arc::new(DashMap::new()),
            capability_scores: Arc::new(DashMap::new()),
            known_bot_addresses: Arc::new(AsyncRwLock::new(HashMap::new())),
            scoring_config: ScoringConfig::default(),
            jito_grpc_client,
            config,
        })
    }

    pub async fn track_transaction(
        &self,
        tx_sig: &Signature,
        suspected_bot: &Pubkey,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let start = Instant::now();
        
        // Fetch transaction and extract metrics
        let metrics = self.extract_transaction_metrics(tx_sig, suspected_bot).await?;
        
        // Get or create profile with proper async locking
        let bot_key = suspected_bot.to_string();
        let profile_arc = self.bot_profiles
            .entry(bot_key.clone())
            .or_insert_with(|| Arc::new(tokio::sync::RwLock::new(BotProfile::new(bot_key.clone(), self.scoring_config.clone()))))
            .value()
            .clone();
        
        // Async lock inside profile
        {
            let mut profile = profile_arc.write().await;
            profile.add_transaction(metrics);
            
            if profile.should_update_score().await {
                let now = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs();
                let capability = self.calculate_capability_score(&profile).await;
                profile.last_score_update = Some(now);
                profile.last_score_value = Some(capability.score);
                
                // Clone before inserting to avoid holding lock
                let bot_pubkey = profile.pubkey.clone();
                drop(profile); // Release lock before inserting
                self.capability_scores.insert(bot_pubkey, capability.score);
            }
        }
        
        TX_COUNTER.inc();
        LATENCY_HIST.observe(start.elapsed().as_millis() as f64);
        Ok(())
    }

    async fn extract_transaction_metrics(
        &self,
        tx_sig: &Signature,
        suspected_bot: &Pubkey,
    ) -> Result<TransactionMetrics, Box<dyn std::error::Error>> {
        let tx_opt = self.rpc_client
            .get_transaction(
                &tx_sig.to_string(),
                solana_client::nonblocking::rpc_client::UiTransactionEncoding::Base64
            )
            .await?;
        
        let tx = tx_opt.ok_or_else(|| anyhow!("Transaction not found"))?;
        let meta = tx.transaction.meta.as_ref().ok_or_else(|| anyhow!("Transaction metadata missing"))?;
        
        let mut metrics = TransactionMetrics {
            signature: tx_sig.to_string(),
            bot_pubkey: suspected_bot.to_string(),
            slot: tx.slot,
            timestamp: SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_secs(),
            success: meta.err.is_none(),
            priority_fee: meta.fee,
            compute_units: meta.compute_units_consumed.unwrap_or(0),
            rpc_latency_ms: 0.0, // Will be set later
            confirmation_latency_ms: 0.0, // Will be set later
            block_inclusion_latency_ms: 0.0, // Will be set later
            bundle_id: None,
            tip_bid: 0,
            strategy_type: self.detect_strategy_from_transaction(&tx)?,
            profit_estimate: self.compute_profit_estimate(meta),
            oracle_age_secs: None,
            oracle_confidence: None,
        };
        
        // Fetch Jito bundle stats if available
        if let Ok(stats) = self.fetch_jito_bundle_stats(&tx_sig.to_string()).await {
            metrics.bundle_id = stats.bundle_id;
            metrics.tip_bid = stats.tip_bid;
        }
        
        Ok(metrics)
    }

    fn detect_strategy_from_transaction(&self, tx: &EncodedConfirmedTransactionWithStatusMeta) -> Vec<BotStrategy> {
        if let Some(meta) = &tx.transaction.meta {
            let pre_balances = &meta.pre_balances;
            let post_balances = &meta.post_balances;
            let log_messages = &meta.log_messages.as_ref().unwrap_or(&vec![]);
            
            let has_swap = log_messages.iter().any(|log| 
                log.contains("Swap") || log.contains("swap") || 
                log.contains("Exchange") || log.contains("exchange")
            );
            
            let has_liquidation = log_messages.iter().any(|log|
                log.contains("Liquidate") || log.contains("liquidate") ||
                log.contains("Liquidation") || log.contains("liquidation")
            );
            
            let balance_changes: Vec<i64> = pre_balances.iter()
                .zip(post_balances.iter())
                .map(|(pre, post)| *post as i64 - *pre as i64)
                .collect();
            
            let distinct_changes = balance_changes.iter()
                .filter(|&&change| change != 0)
                .count();
            
            let mut strategies = Vec::new();
            
            if has_liquidation {
                strategies.push(BotStrategy::Liquidation);
            }
            
            if has_swap && distinct_changes >= 4 {
                let positive_changes = balance_changes.iter().filter(|&&c| c > 0).count();
                let negative_changes = balance_changes.iter().filter(|&&c| c < 0).count();
                
                if positive_changes >= 2 && negative_changes >= 2 {
                    strategies.push(BotStrategy::Arbitrage);
                }
            }
            
            if distinct_changes >= 3 && log_messages.len() > 10 {
                strategies.push(BotStrategy::SandwichAttack);
            }
            
            if has_swap && distinct_changes == 2 {
                strategies.push(BotStrategy::FrontRunning);
            }
            
            if strategies.is_empty() {
                strategies.push(BotStrategy::Unknown);
            }
            
            strategies
        } else {
            vec![BotStrategy::Unknown]
        }
    }

    fn compute_profit_estimate(&self, meta: &TransactionStatusMeta) -> Option<f64> {
        let mut deltas: HashMap<String, i128> = HashMap::new();

        for (i, pre) in meta.pre_token_balances.iter().enumerate() {
            let post = meta.post_token_balances.get(i)?;
            let pre_amt = pre.ui_token_amount.amount.parse::<i128>().ok()?;
            let post_amt = post.ui_token_amount.amount.parse::<i128>().ok()?;
            let change = post_amt.saturating_sub(pre_amt);
            deltas.entry(pre.mint.clone()).and_modify(|v| *v += change).or_insert(change);
        }

        let profit_units: i128 = deltas.values().copied().filter(|d| *d > 0).sum();
        if profit_units == 0 { return None; }
        Some(profit_units as f64)
    }

    pub async fn update_bot_capability_score(&self, bot_pubkey: &String) -> Result<(), Box<dyn std::error::Error>> {
        let profile = self.bot_profiles.get(bot_pubkey)
            .ok_or("Bot profile not found")?
            .clone();
        
        let profile_guard = profile.read().await;
        
        if profile_guard.transactions.len() < self.scoring_config.score_update_threshold {
            return Ok(());
        }

        let capability = self.calculate_capability_score(&profile_guard).await;
        self.capability_scores.insert(bot_pubkey.clone(), capability.score);
        
        Ok(())
    }

    async fn compute_bot_score(&self, profile: &BotProfile) -> Result<f64, Box<dyn std::error::Error>> {
        // 1. Base score on profit estimates (weighted by recency)
        let n = profile.transactions.len().min(self.scoring_config.max_transaction_history).max(1);
        let profit_score: f64 = profile.transactions.iter()
            .rev()
            .take(n)
            .map(|tx| tx.profit_estimate.unwrap_or(0.0))
            .sum::<f64>() / n as f64;

        // 2. Strategy consistency score (higher for more consistent strategies)
        let entropy = profile.compute_strategy_entropy();
        let consistency_score = 1.0 - entropy;

        // 3. Novelty penalty for unusual strategy transitions
        let novelty_penalty = if let Some(last) = profile.transactions.back() {
            if let Some(prev) = &profile.last_strategy {
                let key = (prev.clone(), last.strategy_type.clone());
                let count = profile.strategy_transitions.get(&key).copied().unwrap_or(0);
                if count < self.scoring_config.transition_novelty_threshold {
                    0.8 // 20% penalty for novel transitions
                } else {
                    1.0
                }
            } else {
                1.0
            }
        } else {
            1.0
        };

        // 4. Combine scores with weights from config
        let score = (profit_score * self.scoring_config.profit_weight) 
            * (consistency_score * self.scoring_config.consistency_weight)
            * novelty_penalty;

        Ok(score.clamp(0.0, 1.0)) // Ensure score is between 0 and 1
    }

    async fn calculate_capability_score(&self, profile: &BotProfile) -> RivalBotCapability {
        let strat_total: f64 = profile.cumulative_metrics.strategy_counts.values().sum::<u64>() as f64;

        let rate = |s: BotStrategy| {
            *profile.cumulative_metrics.strategy_counts.get(&s).unwrap_or(&0) as f64 / strat_total.max(1.0)
        };

        let current_time = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        let recent_txs: Vec<&TransactionMetrics> = profile.transactions.iter()
            .filter(|tx| current_time - tx.timestamp < Duration::from_secs(300).as_secs())
            .collect();

        let success_rate = if !recent_txs.is_empty() {
            recent_txs.iter().filter(|tx| tx.success).count() as f64 / recent_txs.len() as f64
        } else {
            0.0
        };

        let avg_priority_fee = if !recent_txs.is_empty() {
            recent_txs.iter().map(|tx| tx.priority_fee).sum::<u64>() / recent_txs.len() as u64
        } else {
            0
        };

        let avg_compute_units = if !recent_txs.is_empty() {
            recent_txs.iter().map(|tx| tx.compute_units).sum::<u64>() / recent_txs.len() as u64
        } else {
            0
        };

        let speed_score = self.calculate_speed_score(&profile.cumulative_metrics);
        let gas_efficiency_score = self.calculate_gas_efficiency_score(avg_priority_fee, avg_compute_units);
        let pattern_consistency_score = self.calculate_pattern_consistency(&profile.cumulative_metrics);

        let strategy_breakdown = self.analyze_strategies(&profile.cumulative_metrics.strategy_counts);
        
        let total_score = self.calculate_total_score(
            speed_score,
            success_rate,
            gas_efficiency_score,
            pattern_consistency_score,
            recent_txs.len() as f64,
        );

        let threat_level = self.assess_threat_level(total_score, success_rate, recent_txs.len());

        let net_profit = if !profile.cumulative_metrics.profits.is_empty() {
            profile.cumulative_metrics.profits.iter().sum::<f64>() / profile.cumulative_metrics.profits.len() as f64
        } else {
            0.0
        };

        let computed_score = self.compute_bot_score(profile).await.unwrap_or(0.0);
        
        RivalBotCapability {
            bot_pubkey: profile.pubkey.clone(),
            score: computed_score,
            arbitrage_rate: rate(BotStrategy::Arbitrage),
            liquidation_rate: rate(BotStrategy::Liquidation),
            sandwich_rate: rate(BotStrategy::SandwichAttack),
            avg_speed_ms: speed_score,
            success_rate,
            avg_compute_units,
            avg_priority_fee,
            pattern_consistency: pattern_consistency_score,
            avg_profit: net_profit,
            avg_rpc_latency_ms: self.calculate_avg_latency(&profile.cumulative_metrics.rpc_latency_samples),
            avg_confirmation_latency_ms: self.calculate_avg_latency(&profile.cumulative_metrics.confirmation_latency_samples),
            avg_block_inclusion_latency_ms: self.calculate_avg_latency(&profile.cumulative_metrics.block_inclusion_latency_samples),
            detected_strategies: strategy_breakdown.strategies,
            threat_level,
            anomalies: Vec::new(),
            rival_comparison_rank: None,
        }
    }

    fn calculate_speed_score(&self, metrics: &CumulativeMetrics) -> f64 {
        if metrics.slot_intervals.len() < 2 {
            return 0.0;
        }

        let mut diffs: Vec<u64> = metrics.slot_intervals
            .iter()
            .zip(metrics.slot_intervals.iter().skip(1))
            .map(|(a, b)| b.saturating_sub(*a))
            .collect();

        let avg_interval = diffs.iter().sum::<u64>() as f64 / diffs.len() as f64;
        let expected_interval = self.scoring_config.slot_time_ms as f64;
        (expected_interval / avg_interval.max(expected_interval)) * 100.0
    }

    fn calculate_gas_efficiency_score(&self, avg_priority_fee: u64, avg_compute_units: u64) -> f64 {
        let fee_efficiency = (50000.0 / (avg_priority_fee as f64 + 1.0)).min(1.0);
        let compute_efficiency = (200000.0 / (avg_compute_units as f64 + 1.0)).min(1.0);
        
        (fee_efficiency * 0.6 + compute_efficiency * 0.4) * 100.0
    }

    fn calculate_pattern_consistency(&self, cm: &CumulativeMetrics) -> f64 {
        if cm.strategy_counts.len() < 2 {
            return 0.0;
        }
        let total: u64 = cm.strategy_counts.values().sum();
        if total == 0 { return 0.0; }
        let dominant = cm.strategy_counts.values().max().copied().unwrap_or(0);
        dominant as f64 / total as f64 * 100.0
    }

    fn calculate_avg_latency(&self, latency_samples: &VecDeque<f64>) -> f64 {
        if latency_samples.is_empty() { return 0.0; }
        latency_samples.iter().sum::<f64>() / latency_samples.len() as f64
    }

    fn analyze_strategies(&self, strategy_counts: &HashMap<BotStrategy, u64>) -> StrategyBreakdown {
        let total: u64 = strategy_counts.values().sum();
        
        if total == 0 {
            return StrategyBreakdown::default();
        }

        let arbitrage_count = *strategy_counts.get(&BotStrategy::Arbitrage).unwrap_or(&0);
        let liquidation_count = *strategy_counts.get(&BotStrategy::Liquidation).unwrap_or(&0);
        let sandwich_count = *strategy_counts.get(&BotStrategy::SandwichAttack).unwrap_or(&0);

        StrategyBreakdown {
            strategies: strategy_counts.keys().cloned().collect(),
            arbitrage_share: arbitrage_count as f64 / total as f64,
            liquidation_share: liquidation_count as f64 / total as f64,
            sandwich_share: sandwich_count as f64 / total as f64,
        }
    }

    fn calculate_total_score(
        &self,
        speed_score: f64,
        success_rate: f64,
        gas_efficiency_score: f64,
        pattern_consistency_score: f64,
        tx_volume: f64,
    ) -> f64 {
        let volume_score = (tx_volume / self.scoring_config.high_frequency_threshold as f64).min(1.0) * 100.0;
        
        let weighted_score = speed_score * self.scoring_config.speed_weight
            + (success_rate * 100.0) * self.scoring_config.success_weight
            + gas_efficiency_score * self.scoring_config.gas_efficiency_weight
            + pattern_consistency_score * self.scoring_config.pattern_consistency_weight
            + volume_score * self.scoring_config.volume_weight;
        
        weighted_score.min(100.0)
    }

    fn assess_threat_level(&self, total_score: f64, success_rate: f64, tx_count: usize) -> ThreatLevel {
        if total_score >= 85.0 && success_rate >= self.scoring_config.critical_success_threshold && tx_count >= self.scoring_config.high_frequency_threshold as usize {
            ThreatLevel::Critical
        } else if total_score >= 70.0 && success_rate >= 0.7 {
            ThreatLevel::High
        } else if total_score >= 50.0 && success_rate >= 0.5 {
            ThreatLevel::Medium
        } else {
            ThreatLevel::Low
        }
    }

    pub async fn get_top_rivals(&self, limit: usize) -> Vec<f64> {
        let mut capabilities: Vec<f64> = self.capability_scores
            .iter()
            .map(|entry| entry.value().clone())
            .collect();
        
        capabilities.sort_by(|a, b| b.partial_cmp(a).unwrap_or(std::cmp::Ordering::Equal));
        capabilities.truncate(limit);
        capabilities
    }

    pub async fn get_rival_capability(&self, bot_pubkey: &String) -> Option<f64> {
        self.capability_scores.get(bot_pubkey).map(|entry| entry.clone())
    }

    pub async fn identify_threat_pattern(&self, bot_pubkey: &String) -> Option<ThreatPattern> {
        let capability = self.get_rival_capability(bot_pubkey).await?;
        
        let pattern = if capability > 0.5 {
            ThreatPattern::AggressiveSandwicher
        } else if capability > 0.7 {
            ThreatPattern::HighSpeedArbitrager
        } else if capability > 0.6 {
            ThreatPattern::LiquidationSpecialist
        } else if capability > self.scoring_config.critical_success_threshold {
            ThreatPattern::HighFrequencyTrader
        } else {
            ThreatPattern::GeneralTrader
        };
        
        Some(pattern)
    }

    pub async fn prune_inactive_bots(&self, inactive_threshold_secs: u64) -> Result<(), Box<dyn std::error::Error>> {
        let current_time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        
        let mut to_remove = Vec::new();

        // Collect keys to remove without holding long-lived locks
        for entry in self.bot_profiles.iter() {
            let profile_arc = entry.value().clone();
            // async read
            let last_seen = {
                let guard = profile_arc.read().await;
                guard.last_seen
            };
            if current_time.saturating_sub(last_seen) > inactive_threshold_secs {
                to_remove.push(entry.key().clone());
            }
        }

        // Remove outside of iteration
        for k in to_remove {
            self.bot_profiles.remove(&k);
            self.capability_scores.remove(&k);
        }

        Ok(())
    }

    pub fn export_capabilities(&self) -> HashMap<String, f64> {
        self.capability_scores
            .iter()
            .map(|entry| (entry.key().clone(), entry.value().clone()))
            .collect()
    }

    pub async fn calculate_competitive_edge(&self, our_metrics: &OurBotMetrics) -> CompetitiveAnalysis {
        let rivals: Vec<f64> = self.get_top_rivals(20).await;
        
        if rivals.is_empty() {
            return CompetitiveAnalysis::default();
        }

        let avg_rival_success = rivals.iter().sum::<f64>() / rivals.len() as f64;
        let avg_rival_speed = rivals.iter().sum::<f64>() / rivals.len() as f64;
        let avg_rival_fee = rivals.iter().sum::<f64>() / rivals.len() as f64;
        
        let success_edge = our_metrics.success_rate - avg_rival_success;
        let speed_edge = our_metrics.speed_score - avg_rival_speed;
        let fee_advantage = avg_rival_fee - our_metrics.avg_priority_fee as f64;
        
        let top_threat = rivals.first().cloned();
        let critical_rivals = rivals.iter()
            .filter(|r| *r > 0.85)
            .count();
        
        CompetitiveAnalysis {
            success_rate_edge: success_edge,
            speed_score_edge: speed_edge,
            fee_efficiency_edge: fee_advantage,
            top_rival: top_threat,
            critical_threat_count: critical_rivals,
            recommended_adjustments: self.generate_adjustments(success_edge, speed_edge, fee_advantage),
        }
    }

    fn generate_adjustments(&self, success_edge: f64, speed_edge: f64, fee_edge: f64) -> Vec<String> {
        let mut adjustments = Vec::new();
        
        if success_edge < -0.1 {
            adjustments.push("Increase transaction validation and simulation accuracy".to_string());
        }
        
        if speed_edge < -10.0 {
            adjustments.push("Optimize RPC connections and reduce processing latency".to_string());
        }
        
        if fee_edge < 0.0 {
            adjustments.push("Implement dynamic priority fee adjustment based on competition".to_string());
        }
        
        if adjustments.is_empty() {
            adjustments.push("Maintain current strategy - performing above average".to_string());
        }
        
        adjustments
    }

    async fn fetch_jito_bundle_stats(
        &self,
        _sig: &str,
    ) -> Result<JitoBundleStats, Box<dyn std::error::Error>> {
        // TODO: wire actual gRPC
        Ok(JitoBundleStats { bundle_id: None, tip_bid: None })
    }
    
    async fn fetch_jito_stats_json_rpc(
        &self,
        signature: &str,
    ) -> Result<JitoBundleStats, Box<dyn std::error::Error>> {
        let client = reqwest::Client::new();
        let response = client
            .post(&self.config.jito_block_engine_url)
            .json(&serde_json::json!({{
                "jsonrpc": "2.0",
                "id": 1,
                "method": "getBundleStatus",
                "params": [{{"signature": signature, "commitment": "confirmed"}}]
            }}))
            .send()
            .await?;
            
        let result: serde_json::Value = response.json().await?;
        
        Ok(JitoBundleStats {
            bundle_id: result["result"]["bundleId"].as_str().map(|s| s.to_string()),
            tip_bid: result["result"]["tipBid"].as_u64(),
        })
    }
    
    async fn fetch_jito_stats_grpc(
        &self,
        signature: &str,
    ) -> Result<JitoBundleStats, Box<dyn std::error::Error>> {
        let request = tonic::Request::new(GetBundleStatusRequest {
            signature: signature.to_string(),
        });
        
        let response = self.jito_grpc_client
            .clone()
            .get_bundle_status(request)
            .await?;
            
        Ok(JitoBundleStats {
            bundle_id: if response.get_ref().bundle_id.is_empty() {
                None
            } else {
                Some(response.get_ref().bundle_id.clone())
            },
            tip_bid: response.get_ref().tip_bid,
        })
    }

    async fn fetch_oracle_data(
        &self,
        oracle_pubkey: &Pubkey,
    ) -> Result<OracleData, Box<dyn std::error::Error>> {
        // Try Pyth first
        match self.fetch_pyth_data(oracle_pubkey).await {
            Ok((age, confidence)) => Ok(OracleData(age, confidence)),
            Err(_) => {
                // Fallback to Switchboard
                match self.fetch_switchboard_data(oracle_pubkey).await {
                    Ok((age, confidence)) => Ok(OracleData(age, confidence)),
                    Err(_) => Ok(OracleData::default()),
                }
            }
        }
    }
    
    async fn fetch_pyth_data(
        &self,
        oracle_pubkey: &Pubkey,
    ) -> Result<(u64, f64), Box<dyn std::error::Error>> {
        let account_data = self.rpc_client.get_account_data(oracle_pubkey).await?;
        let price_account = parse_pyth_price(account_data.as_ref())?;
        let age_secs = (self.get_current_timestamp()? - price_account.1) as u64;
        let confidence = price_account.2;
        Ok((age_secs, confidence))
    }
    
    async fn fetch_switchboard_data(
        &self,
        oracle_pubkey: &Pubkey,
    ) -> Result<(u64, f64), Box<dyn std::error::Error>> {
        let account_data = self.rpc_client.get_account_data(oracle_pubkey).await?;
        
        // Try Aggregator first
        match AggregatorAccountData::new(&account_data) {
            Ok(agg) => {
                let age_secs = (self.get_current_timestamp()? - agg.latest_confirmed_round.round_open_timestamp) as u64;
                let confidence = agg.latest_confirmed_round.std_deviation;
                Ok((age_secs, confidence))
            }
            Err(_) => {
                // Fallback to VRF
                let vrf = VrfAccountData::new(&account_data)?;
                let age_secs = (self.get_current_timestamp()? - vrf.timestamp) as u64;
                Ok((age_secs, 1.0)) // Default confidence for VRF
            }
        }
    }
    
    fn get_current_timestamp(&self) -> Result<i64, Box<dyn std::error::Error>> {
        Ok(std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs() as i64)
    }
}

pub struct ShardedScorer {
    senders: Vec<mpsc::UnboundedSender<(Signature, Pubkey)>>,
}

impl ShardedScorer {
    pub fn new(scorer: Arc<RivalBotCapabilityScorer>, shards: usize) -> Self {
        let mut senders = Vec::new();
        
        for _ in 0..shards {
            let (tx, rx) = mpsc::unbounded_channel();
            let actor = ScoringActor {
                receiver: rx,
                scorer: scorer.clone()
            };
            tokio::spawn(actor.run());
            senders.push(tx);
        }
        
        Self { senders }
    }
    
    pub fn dispatch(&self, sig: Signature, bot: Pubkey) {
        // Simple mod-based sharding
        let idx = (bot.to_bytes()[0] as usize) % self.senders.len();
        let _ = self.senders[idx].send((sig, bot));
    }
}

struct ScoringActor {
    receiver: mpsc::UnboundedReceiver<(Signature, Pubkey)>,
    scorer: Arc<RivalBotCapabilityScorer>,
}

impl ScoringActor {
    async fn run(mut self) {
        while let Some((sig, bot)) = self.receiver.recv().await {
            let _ = self.scorer.track_transaction(&sig, &bot).await;
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct JitoBundleStats {
    bundle_id: Option<String>,
    tip_bid: Option<u64>,
}

impl Default for JitoBundleStats {
    fn default() -> Self {
        Self {
            bundle_id: None,
            tip_bid: None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StrategyBreakdown {
    pub strategies: Vec<BotStrategy>,
    pub arbitrage_share: f64,
    pub liquidation_share: f64,
    pub sandwich_share: f64,
}

impl Default for StrategyBreakdown {
    fn default() -> Self {
        Self {
            strategies: Vec::new(),
            arbitrage_share: 0.0,
            liquidation_share: 0.0,
            sandwich_share: 0.0,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum ThreatPattern {
    AggressiveSandwicher,
    HighSpeedArbitrager,
    LiquidationSpecialist,
    HighFrequencyTrader,
    GeneralTrader,
}

#[derive(Debug, Clone)]
pub struct OurBotMetrics {
    pub success_rate: f64,
    pub speed_score: f64,
    pub avg_priority_fee: u64,
}

#[derive(Debug, Clone, Default)]
pub struct CompetitiveAnalysis {
    pub success_rate_edge: f64,
    pub speed_score_edge: f64,
    pub fee_efficiency_edge: f64,
    pub top_rival: Option<f64>,
    pub critical_threat_count: usize,
    pub recommended_adjustments: Vec<String>,
}

lazy_static! {
    static ref METRICS: Metrics = {
        let registry = Registry::new();
        
        let metrics = Metrics {
            registry,
            tx_successes: IntCounter::new(
                "bot_scorer_transaction_successes", 
                "Number of successful transactions tracked"
            ).unwrap(),
            tx_failures: IntCounter::new(
                "bot_scorer_transaction_failures", 
                "Number of failed transactions tracked"
            ).unwrap(),
            avg_cu: IntGauge::new(
                "bot_scorer_avg_compute_units", 
                "Average compute units used by bots"
            ).unwrap(),
            // New latency metrics
            rpc_latency: Histogram::with_opts(
                HistogramOpts::new(
                    "bot_scorer_rpc_latency_ms",
                    "RPC latency in milliseconds"
                ).buckets(vec![1.0, 5.0, 10.0, 25.0, 50.0, 100.0, 250.0, 500.0, 1000.0])
            ).unwrap(),
            confirmation_latency: Histogram::with_opts(
                HistogramOpts::new(
                    "bot_scorer_confirmation_latency_ms",
                    "Confirmation latency in milliseconds"
                ).buckets(vec![10.0, 50.0, 100.0, 250.0, 500.0, 1000.0, 2500.0, 5000.0])
            ).unwrap(),
            block_latency: Histogram::with_opts(
                HistogramOpts::new(
                    "bot_scorer_block_inclusion_latency_ms",
                    "Block inclusion latency in milliseconds"
                ).buckets(vec![10.0, 50.0, 100.0, 250.0, 500.0, 1000.0, 2500.0, 5000.0])
            ).unwrap(),
            // New Jito metrics
            jito_usage: IntCounter::new(
                "bot_scorer_jito_bundle_usage", 
                "Number of transactions using Jito bundles"
            ).unwrap(),
            avg_tip_bid: IntGauge::new(
                "bot_scorer_avg_tip_bid", 
                "Average tip bid amount for Jito bundles"
            ).unwrap(),
        };
        
        metrics.registry.register(Box::new(metrics.tx_successes.clone())).unwrap();
        metrics.registry.register(Box::new(metrics.tx_failures.clone())).unwrap();
        metrics.registry.register(Box::new(metrics.avg_cu.clone())).unwrap();
        metrics.registry.register(Box::new(metrics.rpc_latency.clone())).unwrap();
        metrics.registry.register(Box::new(metrics.confirmation_latency.clone())).unwrap();
        metrics.registry.register(Box::new(metrics.block_latency.clone())).unwrap();
        metrics.registry.register(Box::new(metrics.jito_usage.clone())).unwrap();
        metrics.registry.register(Box::new(metrics.avg_tip_bid.clone())).unwrap();
        
        metrics
    };
}

use prometheus::{IntCounter, Histogram, Registry};
use lazy_static::lazy_static;

lazy_static! {
    static ref REGISTRY: Registry = Registry::new();
    static ref TX_COUNTER: Result<IntCounter, PrometheusError> = IntCounter::new("txs_processed", "Total transactions processed");
    static ref LATENCY_HIST: Result<Histogram, PrometheusError> = Histogram::with_opts(
        prometheus::HistogramOpts::new(
            "rpc_latency_ms", 
            "RPC latency in milliseconds"
        )
    );
}

pub fn init_metrics() -> Result<(), PrometheusError> {
    if let Ok(counter) = &*TX_COUNTER {
        REGISTRY.register(Box::new(counter.clone()))?;
    }
    if let Ok(hist) = &*LATENCY_HIST {
        REGISTRY.register(Box::new(hist.clone()))?;
    }
    Ok(())
}

fn create_metrics() -> Result<Metrics, Box<dyn std::error::Error>> {
    let registry = Registry::new();
    
    let tx_successes = IntCounter::new(
        "bot_scorer_transaction_successes", 
        "Number of successful transactions tracked"
    )?;
    
    let tx_failures = IntCounter::new(
        "bot_scorer_transaction_failures", 
        "Number of failed transactions tracked"
    )?;
    
    let avg_cu = IntGauge::new(
        "bot_scorer_avg_compute_units", 
        "Average compute units used by bots"
    )?;
    
    let rpc_latency = Histogram::with_opts(
        HistogramOpts::new(
            "bot_scorer_rpc_latency_ms",
            "RPC latency in milliseconds"
        ).buckets(vec![1.0, 5.0, 10.0, 25.0, 50.0, 100.0, 250.0, 500.0, 1000.0])
    )?;
    
    let confirmation_latency = Histogram::with_opts(
        HistogramOpts::new(
            "bot_scorer_confirmation_latency_ms",
            "Confirmation latency in milliseconds"
        ).buckets(vec![10.0, 50.0, 100.0, 250.0, 500.0, 1000.0, 2500.0, 5000.0])
    )?;
    
    let block_latency = Histogram::with_opts(
        HistogramOpts::new(
            "bot_scorer_block_inclusion_latency_ms",
            "Block inclusion latency in milliseconds"
        ).buckets(vec![10.0, 50.0, 100.0, 250.0, 500.0, 1000.0, 2500.0, 5000.0])
    )?;
    
    let jito_usage = IntCounter::new(
        "bot_scorer_jito_bundle_usage", 
        "Number of transactions using Jito bundles"
    )?;
    
    let avg_tip_bid = IntGauge::new(
        "bot_scorer_avg_tip_bid", 
        "Average tip bid amount for Jito bundles"
    )?;
    
    registry.register(Box::new(tx_successes.clone()))?;
    registry.register(Box::new(tx_failures.clone()))?;
    registry.register(Box::new(avg_cu.clone()))?;
    registry.register(Box::new(rpc_latency.clone()))?;
    registry.register(Box::new(confirmation_latency.clone()))?;
    registry.register(Box::new(block_latency.clone()))?;
    registry.register(Box::new(jito_usage.clone()))?;
    registry.register(Box::new(avg_tip_bid.clone()))?;
    
    Ok(Metrics {
        registry,
        tx_successes,
        tx_failures,
        avg_cu,
        rpc_latency,
        confirmation_latency,
        block_latency,
        jito_usage,
        avg_tip_bid,
    })
}

//////////////////////////////
// THREAT MODELING (MITRE/STRIDE HYBRID)
//////////////////////////////

pub fn score_threat(
    metrics: &BotPerformanceMetrics,
    strategies: &[BotStrategy],
    uses_jito: bool,
    avg_tip_bid: Option<u64>
) -> (ThreatLevel, Vec<String>) {
    let mut points = 0;
    let mut anomalies = Vec::new();

    // Existing scoring logic...

    // Jito-specific scoring
    if uses_jito {
        points += 15; // Base points for using Jito
        anomalies.push("Uses Jito bundles".to_string());
        
        if let Some(tip) = avg_tip_bid {
            if tip > 1_000_000 { // 1 SOL
                points += 10;
                anomalies.push(format!("High Jito tip bid: {} lamports", tip));
            }
        }
    }

    // Determine threat level based on total points
    let threat_level = if points >= 60 {
        ThreatLevel::Critical
    } else if points >= 40 {
        ThreatLevel::High
    } else if points >= 20 {
        ThreatLevel::Moderate
    } else {
        ThreatLevel::Low
    };

    (threat_level, anomalies)
}

#[cfg(test)]
mod tests {
    use super::*;
    use yellowstone_grpc_client::SubscribeUpdate;
    
    #[test]
    fn test_classify_strategy() {
        // Test arbitrage classification
        let arbitrage_ins = b"swap exact in for out";
        assert!(matches!(
            classify_strategy(arbitrage_ins)[0],
            BotStrategy::Arbitrage
        ));
        
        // Test liquidation classification
        let liquidation_ins = b"liquidate position";
        assert!(matches!(
            classify_strategy(liquidation_ins)[0],
            BotStrategy::Liquidation
        ));
        
        // Test sandwich classification
        let sandwich_ins = b"place_order cancel_order";
        assert!(matches!(
            classify_strategy(sandwich_ins)[0],
            BotStrategy::SandwichAttack
        ));
    }
    
    #[tokio::test]
    async fn test_compute_metrics() {
        let mut tx = SubscribeUpdate::default();
        tx.executed = Some(true);
        tx.compute_units = Some(100_000);
        tx.priority_fee = Some(50_000);
        
        let mut block_times = HashMap::new();
        block_times.insert(1, 1000);
        
        let metrics = compute_metrics(&[&tx], &block_times).await;
        
        assert_eq!(metrics.avg_cu, 100_000);
        assert_eq!(metrics.avg_priority_fee, 50_000);
        assert_eq!(metrics.success_rate, 1.0);
    }
    
    #[test]
    fn test_score_threat() {
        let metrics = BotPerformanceMetrics {
            speed_ms: 150,
            success_rate: 0.96,
            avg_cu: 950_000,
            avg_priority_fee: 1_200_000,
            pattern_consistency: 0.4,
            net_profit: 0.5,
        };
        
        let strategies = vec![BotStrategy::SandwichAttack];
        
        let (threat_level, notes) = score_threat(&metrics, &strategies);
        
        assert!(matches!(threat_level, ThreatLevel::High));
        assert!(notes.contains(&"Sandwich attack capability detected".to_string()));
    }
    
    #[tokio::test]
    async fn test_active_bot_scores() {
        let scores = ActiveBotScores::new().expect("Failed to create scores");
        
        let test_score = BotScore {
            bot_pubkey: "test_pubkey".to_string(),
            score: 0.85,
            last_updated: 0,
            strategy_breakdown: StrategyBreakdown::default(),
            rival_comparison_rank: Some(0.75),
        };
        
        scores.update("test_pubkey".to_string(), test_score.clone()).await
            .expect("Failed to update score");
        
        let retrieved = scores.get("test_pubkey").await;
        assert!(retrieved.is_some());
        assert_eq!(retrieved.expect("Should have value").bot_pubkey, "test_pubkey");
    }
}

pub async fn run_live_scorer(yellowstone_endpoint: &str) -> Result<(), Box<dyn std::error::Error>> {
    info!("Launching rival_bot_capability_scorer...");
    register_metrics();

    // Setup Yellowstone gRPC connection
    let mut client = GeyserGrpcClient::connect(yellowstone_endpoint, None, None).await?;
    let filter = SubscribeRequest::default(); // Apply fine-grained filter, e.g., known bot wallets/programs
    let mut stream = client.subscribe_with_request(filter).await?;

    let bot_scores = Arc::new(ActiveBotScores::default());
    let all_scores_cache = Arc::new(Mutex::new(Vec::new()));
    let block_times = Arc::new(Mutex::new(HashMap::new())); // slot -> time

    while let Some(update) = stream.message().await? {
        // In prod, spawn and parallelize
        let tx = &update;

        // Extract bot pubkey from transaction
        let bot_pubkey = match &tx.transaction {
            Some(tx_data) => {
                if let Some(signatures) = &tx_data.signatures {
                    if !signatures.is_empty() {
                        bs58::encode(&signatures[0]).into_string()
                    } else {
                        warn!("Transaction has no signatures");
                        continue;
                    }
                } else {
                    warn!("Transaction data missing signatures");
                    continue;
                }
            }
            None => {
                warn!("Received update without transaction data");
                continue;
            }
        };
        let bot_pubkey = bot_pubkey.clone();

        // Rest of the code remains the same
    }
}

// Placeholder types - replace with real implementations
#[derive(Clone)]
pub struct Config {
    pub jito_block_engine_url: String,
    pub rpc_url: String,
    pub metrics_enabled: bool,
}

#[derive(Clone)]
pub struct Metrics {
    pub registry: Registry,
    pub tx_successes: IntCounter,
    pub tx_failures: IntCounter,
    pub avg_cu: IntGauge,
    pub rpc_latency: Histogram,
    pub confirmation_latency: Histogram,
    pub block_latency: Histogram,
    pub jito_usage: IntCounter,
    pub avg_tip_bid: IntGauge,
}

#[derive(Debug)]
pub struct BotPerformanceMetrics {
    pub speed_ms: f64,
    pub success_rate: f64,
    pub avg_cu: u64,
    pub avg_priority_fee: u64,
    pub pattern_consistency: f64,
    pub net_profit: f64,
}

#[derive(Debug, Default, Clone)]
pub struct OracleData {
    pub age_secs: u64,
    pub confidence: f64,
}
