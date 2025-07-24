use solana_client::rpc_client::RpcClient;
use solana_sdk::{
    pubkey::Pubkey,
    signature::Signature,
    commitment_config::CommitmentConfig,
};
use solana_transaction_status::{
    EncodedConfirmedTransactionWithStatusMeta,
    UiTransactionEncoding,
};
use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use dashmap::DashMap;
use serde::{Serialize, Deserialize};
use tokio::sync::RwLock as AsyncRwLock;

const MAX_TRACKED_BOTS: usize = 500;
const CAPABILITY_SCORE_WINDOW: Duration = Duration::from_secs(300);
const MIN_TX_FOR_SCORING: usize = 10;
const SCORE_UPDATE_INTERVAL: Duration = Duration::from_secs(5);
const MAX_HISTORICAL_DATA_POINTS: usize = 1000;
const CRITICAL_SUCCESS_THRESHOLD: f64 = 0.85;
const HIGH_FREQUENCY_THRESHOLD: u64 = 100;
const SLOT_TIME_MS: u64 = 400;
const PRIORITY_FEE_PERCENTILE: f64 = 0.95;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RivalBotCapability {
    pub pubkey: Pubkey,
    pub total_score: f64,
    pub speed_score: f64,
    pub success_rate: f64,
    pub gas_efficiency_score: f64,
    pub pattern_consistency_score: f64,
    pub arbitrage_success_rate: f64,
    pub liquidation_success_rate: f64,
    pub sandwich_attack_rate: f64,
    pub avg_priority_fee: u64,
    pub avg_compute_units: u64,
    pub tx_count_24h: u64,
    pub failed_tx_count_24h: u64,
    pub avg_latency_ms: f64,
    pub last_seen: u64,
    pub threat_level: ThreatLevel,
    pub detected_strategies: Vec<BotStrategy>,
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

#[derive(Debug, Clone)]
struct TransactionMetrics {
    signature: Signature,
    slot: u64,
    timestamp: u64,
    success: bool,
    priority_fee: u64,
    compute_units: u64,
    latency_ms: Option<f64>,
    strategy_type: BotStrategy,
    profit_estimate: Option<u64>,
}

#[derive(Debug)]
struct BotProfile {
    pubkey: Pubkey,
    transactions: VecDeque<TransactionMetrics>,
    pattern_buffer: VecDeque<BotStrategy>,
    success_window: VecDeque<bool>,
    last_update: Instant,
    cumulative_metrics: CumulativeMetrics,
}

#[derive(Debug, Default)]
struct CumulativeMetrics {
    total_txs: u64,
    successful_txs: u64,
    total_priority_fees: u64,
    total_compute_units: u64,
    strategy_counts: HashMap<BotStrategy, u64>,
    slot_intervals: VecDeque<u64>,
    latency_samples: VecDeque<f64>,
}

pub struct RivalBotCapabilityScorer {
    rpc_client: Arc<RpcClient>,
    bot_profiles: Arc<DashMap<Pubkey, Arc<AsyncRwLock<BotProfile>>>>,
    capability_scores: Arc<DashMap<Pubkey, RivalBotCapability>>,
    known_bot_addresses: Arc<RwLock<HashMap<Pubkey, SystemTime>>>,
    scoring_weights: ScoringWeights,
}

#[derive(Debug, Clone)]
struct ScoringWeights {
    speed_weight: f64,
    success_weight: f64,
    gas_efficiency_weight: f64,
    pattern_consistency_weight: f64,
    volume_weight: f64,
}

impl Default for ScoringWeights {
    fn default() -> Self {
        Self {
            speed_weight: 0.25,
            success_weight: 0.35,
            gas_efficiency_weight: 0.15,
            pattern_consistency_weight: 0.15,
            volume_weight: 0.10,
        }
    }
}

impl RivalBotCapabilityScorer {
    pub fn new(rpc_endpoint: &str) -> Self {
        let rpc_client = Arc::new(RpcClient::new_with_commitment(
            rpc_endpoint.to_string(),
            CommitmentConfig::confirmed(),
        ));

        Self {
            rpc_client,
            bot_profiles: Arc::new(DashMap::new()),
            capability_scores: Arc::new(DashMap::new()),
            known_bot_addresses: Arc::new(RwLock::new(HashMap::new())),
            scoring_weights: ScoringWeights::default(),
        }
    }

    pub async fn track_transaction(&self, tx_sig: &Signature, suspected_bot: &Pubkey) -> Result<(), Box<dyn std::error::Error>> {
        let tx_result = self.rpc_client.get_transaction(
            tx_sig,
            UiTransactionEncoding::Base64,
        )?;

        let metrics = self.extract_transaction_metrics(&tx_result, tx_sig)?;
        
        let profile = self.bot_profiles
            .entry(*suspected_bot)
            .or_insert_with(|| Arc::new(AsyncRwLock::new(BotProfile::new(*suspected_bot))))
            .clone();

        let mut profile_guard = profile.write().await;
        profile_guard.add_transaction(metrics);
        
        if profile_guard.should_update_score() {
            drop(profile_guard);
            self.update_bot_capability_score(suspected_bot).await?;
        }

        Ok(())
    }

    fn extract_transaction_metrics(
        &self,
        tx: &EncodedConfirmedTransactionWithStatusMeta,
        signature: &Signature,
    ) -> Result<TransactionMetrics, Box<dyn std::error::Error>> {
        let meta = tx.transaction.meta.as_ref()
            .ok_or("Transaction meta not found")?;
        
        let slot = tx.slot;
        let success = meta.err.is_none();
        let priority_fee = meta.fee.saturating_sub(5000);
        let compute_units = meta.compute_units_consumed.unwrap_or(0);
        
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)?
            .as_secs();

        let strategy_type = self.detect_strategy_from_transaction(tx);

        Ok(TransactionMetrics {
            signature: *signature,
            slot,
            timestamp,
            success,
            priority_fee,
            compute_units,
            latency_ms: None,
            strategy_type,
            profit_estimate: None,
        })
    }

    fn detect_strategy_from_transaction(&self, tx: &EncodedConfirmedTransactionWithStatusMeta) -> BotStrategy {
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
            
            if has_liquidation {
                return BotStrategy::Liquidation;
            }
            
            if has_swap && distinct_changes >= 4 {
                let positive_changes = balance_changes.iter().filter(|&&c| c > 0).count();
                let negative_changes = balance_changes.iter().filter(|&&c| c < 0).count();
                
                if positive_changes >= 2 && negative_changes >= 2 {
                    return BotStrategy::Arbitrage;
                }
            }
            
            if distinct_changes >= 3 && log_messages.len() > 10 {
                return BotStrategy::SandwichAttack;
            }
            
            if has_swap && distinct_changes == 2 {
                return BotStrategy::FrontRunning;
            }
        }
        
        BotStrategy::Unknown
    }

    async fn update_bot_capability_score(&self, bot_pubkey: &Pubkey) -> Result<(), Box<dyn std::error::Error>> {
        let profile = self.bot_profiles.get(bot_pubkey)
            .ok_or("Bot profile not found")?
            .clone();
        
        let profile_guard = profile.read().await;
        
        if profile_guard.transactions.len() < MIN_TX_FOR_SCORING {
            return Ok(());
        }

        let capability = self.calculate_capability_score(&profile_guard);
        self.capability_scores.insert(*bot_pubkey, capability);
        
        Ok(())
    }

    fn calculate_capability_score(&self, profile: &BotProfile) -> RivalBotCapability {
        let current_time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        let recent_txs: Vec<&TransactionMetrics> = profile.transactions.iter()
            .filter(|tx| current_time - tx.timestamp < CAPABILITY_SCORE_WINDOW.as_secs())
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
        let pattern_consistency_score = self.calculate_pattern_consistency(&profile.pattern_buffer);

        let strategy_breakdown = self.analyze_strategies(&profile.cumulative_metrics.strategy_counts);
        
        let total_score = self.calculate_total_score(
            speed_score,
            success_rate,
            gas_efficiency_score,
            pattern_consistency_score,
            recent_txs.len() as f64,
        );

        let threat_level = self.assess_threat_level(total_score, success_rate, recent_txs.len());

        RivalBotCapability {
            pubkey: profile.pubkey,
            total_score,
            speed_score,
            success_rate,
            gas_efficiency_score,
            pattern_consistency_score,
            arbitrage_success_rate: strategy_breakdown.arbitrage_rate,
            liquidation_success_rate: strategy_breakdown.liquidation_rate,
            sandwich_attack_rate: strategy_breakdown.sandwich_rate,
            avg_priority_fee,
            avg_compute_units,
            tx_count_24h: recent_txs.len() as u64,
            failed_tx_count_24h: recent_txs.iter().filter(|tx| !tx.success).count() as u64,
            avg_latency_ms: self.calculate_avg_latency(&profile.cumulative_metrics),
            last_seen: current_time,
            threat_level,
            detected_strategies: strategy_breakdown.strategies,
        }
    }

    fn calculate_speed_score(&self, metrics: &CumulativeMetrics) -> f64 {
        if metrics.slot_intervals.len() < 2 {
            return 0.0;
        }

        let avg_interval = metrics.slot_intervals.iter().sum::<u64>() as f64 
            / metrics.slot_intervals.len() as f64;
        
        let expected_interval = SLOT_TIME_MS as f64 / 1000.0;
        let speed_ratio = expected_interval / avg_interval.max(expected_interval);
        
        (speed_ratio * 100.0).min(100.0)
    }

    fn calculate_gas_efficiency_score(&self, avg_priority_fee: u64, avg_compute_units: u64) -> f64 {
        let fee_efficiency = (50000.0 / (avg_priority_fee as f64 + 1.0)).min(1.0);
        let compute_efficiency = (200000.0 / (avg_compute_units as f64 + 1.0)).min(1.0);
        
        (fee_efficiency * 0.6 + compute_efficiency * 0.4) * 100.0
    }

    fn calculate_pattern_consistency(&self, pattern_buffer: &VecDeque<BotStrategy>) -> f64 {
        if pattern_buffer.len() < 3 {
            return 0.0;
        }

        let mut pattern_counts: HashMap<&BotStrategy, usize> = HashMap::new();
                for strategy in pattern_buffer {
            *pattern_counts.entry(strategy).or_insert(0) += 1;
        }

        let dominant_count = pattern_counts.values().max().copied().unwrap_or(0);
        let consistency_ratio = dominant_count as f64 / pattern_buffer.len() as f64;
        
        consistency_ratio * 100.0
    }

    fn calculate_avg_latency(&self, metrics: &CumulativeMetrics) -> f64 {
        if metrics.latency_samples.is_empty() {
            return 0.0;
        }
        
        metrics.latency_samples.iter().sum::<f64>() / metrics.latency_samples.len() as f64
    }

    fn analyze_strategies(&self, strategy_counts: &HashMap<BotStrategy, u64>) -> StrategyBreakdown {
        let total_strategies: u64 = strategy_counts.values().sum();
        
        if total_strategies == 0 {
            return StrategyBreakdown::default();
        }

        let arbitrage_count = strategy_counts.get(&BotStrategy::Arbitrage).copied().unwrap_or(0);
        let liquidation_count = strategy_counts.get(&BotStrategy::Liquidation).copied().unwrap_or(0);
        let sandwich_count = strategy_counts.get(&BotStrategy::SandwichAttack).copied().unwrap_or(0);

        let mut strategies: Vec<BotStrategy> = strategy_counts
            .iter()
            .filter(|(_, &count)| count > total_strategies / 10)
            .map(|(strategy, _)| strategy.clone())
            .collect();
        
        strategies.sort_by_key(|s| std::cmp::Reverse(strategy_counts.get(s).copied().unwrap_or(0)));

        StrategyBreakdown {
            arbitrage_rate: arbitrage_count as f64 / total_strategies as f64,
            liquidation_rate: liquidation_count as f64 / total_strategies as f64,
            sandwich_rate: sandwich_count as f64 / total_strategies as f64,
            strategies,
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
        let volume_score = (tx_volume / HIGH_FREQUENCY_THRESHOLD as f64).min(1.0) * 100.0;
        
        let weighted_score = speed_score * self.scoring_weights.speed_weight
            + (success_rate * 100.0) * self.scoring_weights.success_weight
            + gas_efficiency_score * self.scoring_weights.gas_efficiency_weight
            + pattern_consistency_score * self.scoring_weights.pattern_consistency_weight
            + volume_score * self.scoring_weights.volume_weight;
        
        weighted_score.min(100.0)
    }

    fn assess_threat_level(&self, total_score: f64, success_rate: f64, tx_count: usize) -> ThreatLevel {
        if total_score >= 85.0 && success_rate >= CRITICAL_SUCCESS_THRESHOLD && tx_count >= HIGH_FREQUENCY_THRESHOLD as usize {
            ThreatLevel::Critical
        } else if total_score >= 70.0 && success_rate >= 0.7 {
            ThreatLevel::High
        } else if total_score >= 50.0 && success_rate >= 0.5 {
            ThreatLevel::Medium
        } else {
            ThreatLevel::Low
        }
    }

    pub async fn get_top_rivals(&self, limit: usize) -> Vec<RivalBotCapability> {
        let mut capabilities: Vec<RivalBotCapability> = self.capability_scores
            .iter()
            .map(|entry| entry.value().clone())
            .collect();
        
        capabilities.sort_by(|a, b| b.total_score.partial_cmp(&a.total_score).unwrap());
        capabilities.truncate(limit);
        capabilities
    }

    pub async fn get_rival_capability(&self, bot_pubkey: &Pubkey) -> Option<RivalBotCapability> {
        self.capability_scores.get(bot_pubkey).map(|entry| entry.clone())
    }

    pub async fn identify_threat_pattern(&self, bot_pubkey: &Pubkey) -> Option<ThreatPattern> {
        let capability = self.get_rival_capability(bot_pubkey).await?;
        
        let pattern = if capability.sandwich_attack_rate > 0.5 {
            ThreatPattern::AggressiveSandwicher
        } else if capability.arbitrage_success_rate > 0.7 && capability.speed_score > 80.0 {
            ThreatPattern::HighSpeedArbitrager
        } else if capability.liquidation_success_rate > 0.6 {
            ThreatPattern::LiquidationSpecialist
        } else if capability.success_rate > CRITICAL_SUCCESS_THRESHOLD && capability.tx_count_24h > HIGH_FREQUENCY_THRESHOLD {
            ThreatPattern::HighFrequencyTrader
        } else {
            ThreatPattern::GeneralTrader
        };
        
        Some(pattern)
    }

    pub async fn prune_inactive_bots(&self) {
        let current_time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        
        let inactive_threshold = 3600; // 1 hour
        
        let inactive_bots: Vec<Pubkey> = self.capability_scores
            .iter()
            .filter(|entry| current_time - entry.value().last_seen > inactive_threshold)
            .map(|entry| *entry.key())
            .collect();
        
        for bot in inactive_bots {
            self.bot_profiles.remove(&bot);
            self.capability_scores.remove(&bot);
        }
        
        if self.bot_profiles.len() > MAX_TRACKED_BOTS {
            let mut scores: Vec<(Pubkey, f64)> = self.capability_scores
                .iter()
                .map(|entry| (*entry.key(), entry.value().total_score))
                .collect();
            
            scores.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap());
            
            let to_remove = scores.len().saturating_sub(MAX_TRACKED_BOTS);
            for (bot, _) in scores.into_iter().take(to_remove) {
                self.bot_profiles.remove(&bot);
                self.capability_scores.remove(&bot);
            }
        }
    }

    pub fn export_capabilities(&self) -> HashMap<Pubkey, RivalBotCapability> {
        self.capability_scores
            .iter()
            .map(|entry| (*entry.key(), entry.value().clone()))
            .collect()
    }

    pub async fn calculate_competitive_edge(&self, our_metrics: &OurBotMetrics) -> CompetitiveAnalysis {
        let rivals: Vec<RivalBotCapability> = self.get_top_rivals(20).await;
        
        if rivals.is_empty() {
            return CompetitiveAnalysis::default();
        }

        let avg_rival_success = rivals.iter().map(|r| r.success_rate).sum::<f64>() / rivals.len() as f64;
        let avg_rival_speed = rivals.iter().map(|r| r.speed_score).sum::<f64>() / rivals.len() as f64;
        let avg_rival_fee = rivals.iter().map(|r| r.avg_priority_fee).sum::<u64>() / rivals.len() as u64;
        
        let success_edge = our_metrics.success_rate - avg_rival_success;
        let speed_edge = our_metrics.speed_score - avg_rival_speed;
        let fee_advantage = avg_rival_fee as f64 - our_metrics.avg_priority_fee as f64;
        
        let top_threat = rivals.first().cloned();
        let critical_rivals = rivals.iter()
            .filter(|r| r.threat_level == ThreatLevel::Critical)
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
}

impl BotProfile {
    fn new(pubkey: Pubkey) -> Self {
        Self {
            pubkey,
            transactions: VecDeque::with_capacity(MAX_HISTORICAL_DATA_POINTS),
            pattern_buffer: VecDeque::with_capacity(50),
            success_window: VecDeque::with_capacity(100),
            last_update: Instant::now(),
            cumulative_metrics: CumulativeMetrics::default(),
        }
    }

    fn add_transaction(&mut self, metrics: TransactionMetrics) {
        if self.transactions.len() >= MAX_HISTORICAL_DATA_POINTS {
            self.transactions.pop_front();
        }
        
        self.update_cumulative_metrics(&metrics);
        self.pattern_buffer.push_back(metrics.strategy_type.clone());
        if self.pattern_buffer.len() > 50 {
            self.pattern_buffer.pop_front();
        }
        
        self.success_window.push_back(metrics.success);
        if self.success_window.len() > 100 {
            self.success_window.pop_front();
        }
        
        self.transactions.push_back(metrics);
        self.last_update = Instant::now();
    }

    fn update_cumulative_metrics(&mut self, metrics: &TransactionMetrics) {
        self.cumulative_metrics.total_txs += 1;
        if metrics.success {
            self.cumulative_metrics.successful_txs += 1;
        }
        
        self.cumulative_metrics.total_priority_fees += metrics.priority_fee;
        self.cumulative_metrics.total_compute_units += metrics.compute_units;
        
        *self.cumulative_metrics.strategy_counts
            .entry(metrics.strategy_type.clone())
            .or_insert(0) += 1;
        
        if let Some(last_tx) = self.transactions.back() {
            let slot_diff = metrics.slot.saturating_sub(last_tx.slot);
            self.cumulative_metrics.slot_intervals.push_back(slot_diff);
            if self.cumulative_metrics.slot_intervals.len() > 100 {
                self.cumulative_metrics.slot_intervals.pop_front();
            }
        }
        
        if let Some(latency) = metrics.latency_ms {
            self.cumulative_metrics.latency_samples.push_back(latency);
            if self.cumulative_metrics.latency_samples.len() > 100 {
                self.cumulative_metrics.latency_samples.pop_front();
            }
        }
    }

    fn should_update_score(&self) -> bool {
        self.last_update.elapsed() >= SCORE_UPDATE_INTERVAL
    }
}

#[derive(Debug, Clone)]
pub struct StrategyBreakdown {
    pub arbitrage_rate: f64,
    pub liquidation_rate: f64,
    pub sandwich_rate: f64,
    pub strategies: Vec<BotStrategy>,
}

impl Default for StrategyBreakdown {
    fn default() -> Self {
        Self {
            arbitrage_rate: 0.0,
            liquidation_rate: 0.0,
            sandwich_rate: 0.0,
            strategies: Vec::new(),
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
    pub top_rival: Option<RivalBotCapability>,
    pub critical_threat_count: usize,
    pub recommended_adjustments: Vec<String>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use solana_sdk::signature::Keypair;

    #[tokio::test]
    async fn test_rival_scoring() {
        let scorer = RivalBotCapabilityScorer::new("https://api.mainnet-beta.solana.com");
        let test_bot = Keypair::new().pubkey();
        
        let capability = scorer.get_rival_capability(&test_bot).await;
        assert!(capability.is_none());
    }

    #[test]
    fn test_threat_level_assessment() {
        let scorer = RivalBotCapabilityScorer::new("https://api.mainnet-beta.solana.com");
        
        let threat = scorer.assess_threat_level(90.0, 0.9, 150);
        assert_eq!(threat, ThreatLevel::Critical);
        
        let threat = scorer.assess_threat_level(75.0, 0.75, 50);
        assert_eq!(threat, ThreatLevel::High);
        
        let threat = scorer.assess_threat_level(30.0, 0.3, 10);
        assert_eq!(threat, ThreatLevel::Low);
    }

    #[test]
    fn test_strategy_detection() {
        let scorer = RivalBotCapabilityScorer::new("https://api.mainnet-beta.solana.com");
        let mut strategy_counts = HashMap::new();
        
        strategy_counts.insert(BotStrategy::Arbitrage, 50);
        strategy_counts.insert(BotStrategy::Liquidation, 20);
        strategy_counts.insert(BotStrategy::SandwichAttack, 30);
        
        let breakdown = scorer.analyze_strategies(&strategy_counts);
        
        assert_eq!(breakdown.arbitrage_rate, 0.5);
        assert_eq!(breakdown.liquidation_rate, 0.2);
        assert_eq!(breakdown.sandwich_rate, 0.3);
        assert!(!breakdown.strategies.is_empty());
    }

    #[test]
    fn test_scoring_weights() {
        let weights = ScoringWeights::default();
        let total = weights.speed_weight + weights.success_weight + 
                   weights.gas_efficiency_weight + weights.pattern_consistency_weight + 
                   weights.volume_weight;
        
        assert!((total - 1.0).abs() < 0.001);
    }
}

