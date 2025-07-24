use solana_client::rpc_client::RpcClient;
use solana_sdk::{
    pubkey::Pubkey,
    signature::Signature,
    commitment_config::CommitmentConfig,
};
use std::{
    collections::{HashMap, VecDeque, HashSet},
    sync::{Arc, RwLock},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;
use rand::{thread_rng, Rng};
use std::cmp::Ordering;

const PATTERN_WINDOW_SIZE: usize = 100;
const MIN_PATTERN_OCCURRENCES: usize = 3;
const DECAY_FACTOR: f64 = 0.95;
const CONFIDENCE_THRESHOLD: f64 = 0.75;
const MAX_TRACKED_BOTS: usize = 500;
const PATTERN_EXPIRY_SECONDS: u64 = 300;
const PREDICTION_CACHE_SIZE: usize = 1000;
const TIMING_BUCKETS: usize = 20;
const MIN_PROFIT_THRESHOLD: f64 = 0.001;
const GAS_PRICE_WINDOW: usize = 50;
const STRATEGY_ADAPTATION_RATE: f64 = 0.1;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AdversarialPattern {
    pub bot_signature: String,
    pub pattern_type: PatternType,
    pub frequency: f64,
    pub avg_profit: f64,
    pub timing_distribution: Vec<f64>,
    pub gas_strategy: GasStrategy,
    pub target_pools: HashSet<String>,
    pub success_rate: f64,
    pub last_seen: u64,
    pub confidence: f64,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum PatternType {
    Frontrun,
    Backrun,
    Sandwich,
    Liquidation,
    Arbitrage,
    JitLiquidity,
    FlashLoan,
    AtomicArb,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct GasStrategy {
    pub base_multiplier: f64,
    pub surge_threshold: f64,
    pub max_multiplier: f64,
    pub timing_offset: i64,
}

#[derive(Clone, Debug)]
pub struct BotProfile {
    pub address: Pubkey,
    pub patterns: Vec<AdversarialPattern>,
    pub historical_moves: VecDeque<Move>,
    pub success_metrics: SuccessMetrics,
    pub timing_profile: TimingProfile,
    pub strategy_weights: HashMap<PatternType, f64>,
}

#[derive(Clone, Debug)]
pub struct Move {
    pub timestamp: u64,
    pub pattern_type: PatternType,
    pub target: String,
    pub profit: f64,
    pub gas_used: u64,
    pub success: bool,
    pub competing_bots: Vec<String>,
}

#[derive(Clone, Debug)]
pub struct SuccessMetrics {
    pub total_attempts: u64,
    pub successful_moves: u64,
    pub avg_profit_per_move: f64,
    pub win_rate_vs_us: f64,
    pub reaction_time_ms: u64,
}

#[derive(Clone, Debug)]
pub struct TimingProfile {
    pub avg_block_offset: f64,
    pub timing_variance: f64,
    pub preferred_slots: Vec<usize>,
    pub burst_probability: f64,
}

#[derive(Clone, Debug)]
pub struct PredictedMove {
    pub bot_address: Pubkey,
    pub pattern_type: PatternType,
    pub target_pool: String,
    pub probability: f64,
    pub expected_profit: f64,
    pub optimal_counter_strategy: CounterStrategy,
    pub timing_window: (u64, u64),
}

#[derive(Clone, Debug)]
pub struct CounterStrategy {
    pub action_type: CounterAction,
    pub gas_multiplier: f64,
    pub timing_adjustment: i64,
    pub expected_success_rate: f64,
    pub min_profit_threshold: f64,
}

#[derive(Clone, Debug, PartialEq)]
pub enum CounterAction {
    Frontrun,
    Outbid,
    AlternativeRoute,
    Abandon,
    DelayedExecution,
    GasWar,
}

pub struct AdversarialMovePredictor {
    bot_profiles: Arc<RwLock<HashMap<Pubkey, BotProfile>>>,
    pattern_cache: Arc<RwLock<HashMap<String, Vec<AdversarialPattern>>>>,
    prediction_cache: Arc<RwLock<VecDeque<PredictedMove>>>,
    gas_history: Arc<RwLock<VecDeque<(u64, u64)>>>,
    active_patterns: Arc<RwLock<HashMap<String, Instant>>>,
    rpc_client: Arc<RpcClient>,
    tx_sender: mpsc::Sender<PredictedMove>,
}

impl AdversarialMovePredictor {
    pub fn new(
        rpc_url: &str,
        tx_sender: mpsc::Sender<PredictedMove>,
    ) -> Self {
        Self {
            bot_profiles: Arc::new(RwLock::new(HashMap::new())),
            pattern_cache: Arc::new(RwLock::new(HashMap::new())),
            prediction_cache: Arc::new(RwLock::new(VecDeque::with_capacity(PREDICTION_CACHE_SIZE))),
            gas_history: Arc::new(RwLock::new(VecDeque::with_capacity(GAS_PRICE_WINDOW))),
            active_patterns: Arc::new(RwLock::new(HashMap::new())),
            rpc_client: Arc::new(RpcClient::new_with_commitment(
                rpc_url.to_string(),
                CommitmentConfig::processed(),
            )),
            tx_sender,
        }
    }

    pub async fn analyze_transaction(&self, tx: &TransactionData) -> Option<Move> {
        let pattern_type = self.classify_transaction_pattern(tx)?;
        let profit = self.calculate_transaction_profit(tx);
        
        if profit < MIN_PROFIT_THRESHOLD {
            return None;
        }

        let competing_bots = self.identify_competing_bots(tx);
        
        Some(Move {
            timestamp: SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs(),
            pattern_type,
            target: tx.target_pool.clone(),
            profit,
            gas_used: tx.gas_used,
            success: tx.success,
            competing_bots,
        })
    }

    fn classify_transaction_pattern(&self, tx: &TransactionData) -> Option<PatternType> {
        let instruction_count = tx.instructions.len();
        let has_swap = tx.instructions.iter().any(|i| i.contains("swap"));
        let has_flash = tx.instructions.iter().any(|i| i.contains("flash"));
        
        match (instruction_count, has_swap, has_flash) {
            (1..=2, true, false) => Some(PatternType::Arbitrage),
            (3..=4, true, false) if self.is_sandwich_pattern(tx) => Some(PatternType::Sandwich),
            (_, _, true) => Some(PatternType::FlashLoan),
            (_, true, false) if self.is_frontrun_pattern(tx) => Some(PatternType::Frontrun),
            (_, true, false) if self.is_backrun_pattern(tx) => Some(PatternType::Backrun),
            _ if self.is_liquidation_pattern(tx) => Some(PatternType::Liquidation),
            _ if self.is_jit_pattern(tx) => Some(PatternType::JitLiquidity),
            _ => None,
        }
    }

    fn is_sandwich_pattern(&self, tx: &TransactionData) -> bool {
        tx.instructions.len() >= 3 && 
        tx.instructions.iter().filter(|i| i.contains("swap")).count() >= 2
    }

    fn is_frontrun_pattern(&self, tx: &TransactionData) -> bool {
        tx.priority_fee > 1000 && tx.instructions.len() <= 2
    }

    fn is_backrun_pattern(&self, tx: &TransactionData) -> bool {
        tx.slot_offset > 0 && tx.instructions.iter().any(|i| i.contains("swap"))
    }

    fn is_liquidation_pattern(&self, tx: &TransactionData) -> bool {
        tx.instructions.iter().any(|i| i.contains("liquidate") || i.contains("repay"))
    }

    fn is_jit_pattern(&self, tx: &TransactionData) -> bool {
        tx.instructions.iter().any(|i| i.contains("add_liquidity")) &&
        tx.instructions.iter().any(|i| i.contains("remove_liquidity"))
    }

    fn calculate_transaction_profit(&self, tx: &TransactionData) -> f64 {
        let gross_profit = tx.output_amount.saturating_sub(tx.input_amount) as f64 / 1e9;
        let gas_cost = (tx.gas_used * tx.priority_fee) as f64 / 1e9;
        gross_profit - gas_cost
    }

    fn identify_competing_bots(&self, tx: &TransactionData) -> Vec<String> {
        let mut bots = Vec::new();
        let patterns = self.pattern_cache.read().unwrap();
        
        for (bot_sig, bot_patterns) in patterns.iter() {
            for pattern in bot_patterns {
                if pattern.target_pools.contains(&tx.target_pool) &&
                   (tx.timestamp - pattern.last_seen) < 60 {
                    bots.push(bot_sig.clone());
                    break;
                }
            }
        }
        
        bots
    }

    pub async fn update_bot_profile(&self, bot_address: Pubkey, new_move: Move) {
        let mut profiles = self.bot_profiles.write().unwrap();
        let profile = profiles.entry(bot_address).or_insert_with(|| BotProfile {
            address: bot_address,
            patterns: Vec::new(),
            historical_moves: VecDeque::with_capacity(PATTERN_WINDOW_SIZE),
            success_metrics: SuccessMetrics {
                total_attempts: 0,
                successful_moves: 0,
                avg_profit_per_move: 0.0,
                win_rate_vs_us: 0.0,
                reaction_time_ms: 0,
            },
            timing_profile: TimingProfile {
                avg_block_offset: 0.0,
                timing_variance: 0.0,
                preferred_slots: Vec::new(),
                burst_probability: 0.0,
            },
            strategy_weights: HashMap::new(),
        });

        profile.historical_moves.push_back(new_move.clone());
        if profile.historical_moves.len() > PATTERN_WINDOW_SIZE {
            profile.historical_moves.pop_front();
        }

        self.update_success_metrics(profile, &new_move);
        self.detect_patterns(profile);
        self.update_timing_profile(profile);
    }

    fn update_success_metrics(&self, profile: &mut BotProfile, new_move: &Move) {
        profile.success_metrics.total_attempts += 1;
        if new_move.success {
            profile.success_metrics.successful_moves += 1;
        }
        
        let alpha = 0.1;
        profile.success_metrics.avg_profit_per_move = 
            (1.0 - alpha) * profile.success_metrics.avg_profit_per_move + 
            alpha * new_move.profit;
    }

    fn detect_patterns(&self, profile: &mut BotProfile) {
        let mut pattern_counts: HashMap<PatternType, usize> = HashMap::new();
        let mut pattern_profits: HashMap<PatternType, Vec<f64>> = HashMap::new();
        let mut timing_buckets: HashMap<PatternType, Vec<u64>> = HashMap::new();
        
        for move_data in &profile.historical_moves {
            *pattern_counts.entry(move_data.pattern_type.clone()).or_insert(0) += 1;
            pattern_profits.entry(move_data.pattern_type.clone())
                .or_insert_with(Vec::new)
                .push(move_data.profit);
            timing_buckets.entry(move_data.pattern_type.clone())
                .or_insert_with(Vec::new)
                .push(move_data.timestamp % 3600);
        }

        profile.patterns.clear();
        
        for (pattern_type, count) in pattern_counts {
            if count >= MIN_PATTERN_OCCURRENCES {
                let profits = &pattern_profits[&pattern_type];
                let avg_profit = profits.iter().sum::<f64>() / profits.len() as f64;
                
                let timing_dist = self.calculate_timing_distribution(&timing_buckets[&pattern_type]);
                
                let pattern = AdversarialPattern {
                    bot_signature: profile.address.to_string(),
                    pattern_type: pattern_type.clone(),
                    frequency: count as f64 / profile.historical_moves.len() as f64,
                    avg_profit,
                    timing_distribution: timing_dist,
                    gas_strategy: self.infer_gas_strategy(profile, &pattern_type),
                    target_pools: self.extract_target_pools(profile, &pattern_type),
                    success_rate: self.calculate_pattern_success_rate(profile, &pattern_type),
                    last_seen: SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs(),
                    confidence: self.calculate_pattern_confidence(count, profits),
                };
                
                profile.patterns.push(pattern);
            }
        }
    }

    fn calculate_timing_distribution(&self, timestamps: &[u64]) -> Vec<f64> {
        let mut buckets = vec![0.0; TIMING_BUCKETS];
                let bucket_size = 3600 / TIMING_BUCKETS;
        
        for &ts in timestamps {
            let bucket = ((ts % 3600) / bucket_size as u64) as usize;
            if bucket < TIMING_BUCKETS {
                buckets[bucket] += 1.0;
            }
        }
        
        let total: f64 = buckets.iter().sum();
        if total > 0.0 {
            for bucket in &mut buckets {
                *bucket /= total;
            }
        }
        
        buckets
    }

    fn infer_gas_strategy(&self, profile: &BotProfile, pattern_type: &PatternType) -> GasStrategy {
        let relevant_moves: Vec<_> = profile.historical_moves
            .iter()
            .filter(|m| &m.pattern_type == pattern_type)
            .collect();
        
        if relevant_moves.is_empty() {
            return GasStrategy {
                base_multiplier: 1.2,
                surge_threshold: 0.8,
                max_multiplier: 3.0,
                timing_offset: 0,
            };
        }
        
        let avg_gas = relevant_moves.iter().map(|m| m.gas_used).sum::<u64>() as f64 / relevant_moves.len() as f64;
        let successful_moves: Vec<_> = relevant_moves.iter().filter(|m| m.success).collect();
        
        let base_multiplier = if successful_moves.len() > relevant_moves.len() / 2 {
            1.1 + (successful_moves.len() as f64 / relevant_moves.len() as f64) * 0.5
        } else {
            1.5
        };
        
        GasStrategy {
            base_multiplier,
            surge_threshold: 0.7,
            max_multiplier: 4.0,
            timing_offset: 0,
        }
    }

    fn extract_target_pools(&self, profile: &BotProfile, pattern_type: &PatternType) -> HashSet<String> {
        profile.historical_moves
            .iter()
            .filter(|m| &m.pattern_type == pattern_type)
            .map(|m| m.target.clone())
            .collect()
    }

    fn calculate_pattern_success_rate(&self, profile: &BotProfile, pattern_type: &PatternType) -> f64 {
        let moves: Vec<_> = profile.historical_moves
            .iter()
            .filter(|m| &m.pattern_type == pattern_type)
            .collect();
        
        if moves.is_empty() {
            return 0.0;
        }
        
        let successful = moves.iter().filter(|m| m.success).count();
        successful as f64 / moves.len() as f64
    }

    fn calculate_pattern_confidence(&self, occurrences: usize, profits: &[f64]) -> f64 {
        let occurrence_score = (occurrences as f64 / MIN_PATTERN_OCCURRENCES as f64).min(1.0);
        let profit_consistency = if profits.len() > 1 {
            let mean = profits.iter().sum::<f64>() / profits.len() as f64;
            let variance = profits.iter().map(|p| (p - mean).powi(2)).sum::<f64>() / profits.len() as f64;
            let std_dev = variance.sqrt();
            1.0 / (1.0 + std_dev / mean.abs().max(0.001))
        } else {
            0.5
        };
        
        occurrence_score * 0.6 + profit_consistency * 0.4
    }

    fn update_timing_profile(&self, profile: &mut BotProfile) {
        let mut block_offsets = Vec::new();
        let mut slot_counts = vec![0; 24];
        
        for move_data in &profile.historical_moves {
            let hour = (move_data.timestamp / 3600) % 24;
            slot_counts[hour as usize] += 1;
        }
        
        let total_moves = profile.historical_moves.len() as f64;
        let mut preferred_slots = Vec::new();
        
        for (hour, &count) in slot_counts.iter().enumerate() {
            if count as f64 / total_moves > 0.1 {
                preferred_slots.push(hour);
            }
        }
        
        profile.timing_profile.preferred_slots = preferred_slots;
        profile.timing_profile.avg_block_offset = 0.0;
        profile.timing_profile.timing_variance = 0.1;
        profile.timing_profile.burst_probability = self.calculate_burst_probability(profile);
    }

    fn calculate_burst_probability(&self, profile: &BotProfile) -> f64 {
        if profile.historical_moves.len() < 10 {
            return 0.0;
        }
        
        let mut time_gaps = Vec::new();
        let moves: Vec<_> = profile.historical_moves.iter().collect();
        
        for i in 1..moves.len() {
            let gap = moves[i].timestamp.saturating_sub(moves[i-1].timestamp);
            time_gaps.push(gap as f64);
        }
        
        let avg_gap = time_gaps.iter().sum::<f64>() / time_gaps.len() as f64;
        let burst_gaps = time_gaps.iter().filter(|&&g| g < avg_gap * 0.3).count();
        
        burst_gaps as f64 / time_gaps.len() as f64
    }

    pub async fn predict_next_moves(&self, current_state: &MarketState) -> Vec<PredictedMove> {
        let mut predictions = Vec::new();
        let profiles = self.bot_profiles.read().unwrap();
        
        for (bot_address, profile) in profiles.iter() {
            if profile.patterns.is_empty() {
                continue;
            }
            
            for pattern in &profile.patterns {
                if pattern.confidence < CONFIDENCE_THRESHOLD {
                    continue;
                }
                
                let prediction_score = self.calculate_prediction_score(pattern, current_state, profile);
                
                if prediction_score > 0.5 {
                    let timing_window = self.predict_timing_window(pattern, current_state);
                    let counter_strategy = self.determine_counter_strategy(pattern, profile, current_state);
                    
                    predictions.push(PredictedMove {
                        bot_address: *bot_address,
                        pattern_type: pattern.pattern_type.clone(),
                        target_pool: self.predict_target_pool(pattern, current_state),
                        probability: prediction_score,
                        expected_profit: pattern.avg_profit * prediction_score,
                        optimal_counter_strategy: counter_strategy,
                        timing_window,
                    });
                }
            }
        }
        
        predictions.sort_by(|a, b| b.expected_profit.partial_cmp(&a.expected_profit).unwrap_or(Ordering::Equal));
        predictions.truncate(10);
        
        self.cache_predictions(predictions.clone());
        predictions
    }

    fn calculate_prediction_score(&self, pattern: &AdversarialPattern, state: &MarketState, profile: &BotProfile) -> f64 {
        let recency_factor = self.calculate_recency_factor(pattern.last_seen);
        let market_alignment = self.calculate_market_alignment(pattern, state);
        let historical_accuracy = self.get_historical_accuracy(pattern);
        let competition_factor = self.calculate_competition_factor(state);
        
        let base_score = pattern.confidence * pattern.frequency * pattern.success_rate;
        let adjusted_score = base_score * recency_factor * market_alignment * (1.0 - competition_factor * 0.3);
        
        adjusted_score.min(1.0).max(0.0)
    }

    fn calculate_recency_factor(&self, last_seen: u64) -> f64 {
        let current_time = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();
        let time_elapsed = current_time.saturating_sub(last_seen);
        DECAY_FACTOR.powf(time_elapsed as f64 / 3600.0)
    }

    fn calculate_market_alignment(&self, pattern: &AdversarialPattern, state: &MarketState) -> f64 {
        match pattern.pattern_type {
            PatternType::Arbitrage => {
                let price_deviation = state.price_deviations.values().max_by(|a, b| a.partial_cmp(b).unwrap()).unwrap_or(&0.0);
                (price_deviation / 0.01).min(1.0)
            },
            PatternType::Sandwich => {
                let volume_spike = state.volume_24h / state.avg_volume_7d.max(1.0);
                (volume_spike / 2.0).min(1.0)
            },
            PatternType::Liquidation => {
                let volatility = state.volatility_index / 100.0;
                volatility.min(1.0)
            },
            _ => 0.7,
        }
    }

    fn get_historical_accuracy(&self, pattern: &AdversarialPattern) -> f64 {
        pattern.success_rate
    }

    fn calculate_competition_factor(&self, state: &MarketState) -> f64 {
        let gas_surge = state.current_gas_price / state.avg_gas_price.max(1.0);
        let mempool_congestion = state.pending_tx_count as f64 / 1000.0;
        
        (gas_surge * 0.7 + mempool_congestion * 0.3).min(1.0)
    }

    fn predict_timing_window(&self, pattern: &AdversarialPattern, state: &MarketState) -> (u64, u64) {
        let current_time = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();
        let bucket_size = 3600 / TIMING_BUCKETS;
        
        let mut best_bucket = 0;
        let mut best_prob = 0.0;
        
        for (i, &prob) in pattern.timing_distribution.iter().enumerate() {
            if prob > best_prob {
                best_prob = prob;
                best_bucket = i;
            }
        }
        
        let window_start = current_time + (best_bucket as u64 * bucket_size);
        let window_end = window_start + bucket_size;
        
        (window_start, window_end)
    }

    fn predict_target_pool(&self, pattern: &AdversarialPattern, state: &MarketState) -> String {
        pattern.target_pools
            .iter()
            .max_by_key(|pool| {
                let volume = state.pool_volumes.get(*pool).unwrap_or(&0.0);
                let liquidity = state.pool_liquidity.get(*pool).unwrap_or(&0.0);
                ((volume * liquidity) * 1000.0) as u64
            })
            .cloned()
            .unwrap_or_else(|| "unknown".to_string())
    }

    fn determine_counter_strategy(&self, pattern: &AdversarialPattern, profile: &BotProfile, state: &MarketState) -> CounterStrategy {
        let gas_pressure = state.current_gas_price / state.avg_gas_price.max(1.0);
        
        let action_type = match (&pattern.pattern_type, gas_pressure) {
            (PatternType::Frontrun, p) if p < 1.5 => CounterAction::Frontrun,
            (PatternType::Frontrun, _) => CounterAction::AlternativeRoute,
            (PatternType::Sandwich, p) if p < 2.0 => CounterAction::Outbid,
            (PatternType::Sandwich, _) => CounterAction::DelayedExecution,
            (PatternType::Arbitrage, _) if pattern.avg_profit > 0.05 => CounterAction::Frontrun,
            (PatternType::Liquidation, _) => CounterAction::GasWar,
            _ if pattern.avg_profit < MIN_PROFIT_THRESHOLD => CounterAction::Abandon,
            _ => CounterAction::AlternativeRoute,
        };
        
        let gas_multiplier = match &action_type {
            CounterAction::GasWar => pattern.gas_strategy.max_multiplier,
            CounterAction::Frontrun | CounterAction::Outbid => pattern.gas_strategy.base_multiplier * 1.2,
            _ => 1.0,
        };
        
        CounterStrategy {
            action_type,
            gas_multiplier,
            timing_adjustment: pattern.gas_strategy.timing_offset,
            expected_success_rate: self.estimate_counter_success_rate(&pattern.pattern_type, profile),
            min_profit_threshold: pattern.avg_profit * 0.7,
        }
    }

    fn estimate_counter_success_rate(&self, pattern_type: &PatternType, profile: &BotProfile) -> f64 {
        let base_rate = match pattern_type {
            PatternType::Frontrun => 0.65,
            PatternType::Sandwich => 0.55,
            PatternType::Arbitrage => 0.70,
            PatternType::Liquidation => 0.80,
            _ => 0.50,
        };
        
        let win_rate_adjustment = 1.0 - profile.success_metrics.win_rate_vs_us;
        base_rate * win_rate_adjustment
    }

    fn cache_predictions(&self, predictions: Vec<PredictedMove>) {
        let mut cache = self.prediction_cache.write().unwrap();
        for prediction in predictions {
            cache.push_back(prediction);
            if cache.len() > PREDICTION_CACHE_SIZE {
                cache.pop_front();
            }
        }
    }

    pub async fn cleanup_stale_data(&self) {
        let current_time = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();
        
        let mut profiles = self.bot_profiles.write().unwrap();
        profiles.retain(|_, profile| {
            profile.patterns.iter().any(|p| current_time - p.last_seen < PATTERN_EXPIRY_SECONDS)
        });
        
        if profiles.len() > MAX_TRACKED_BOTS {
            let mut sorted_profiles: Vec<_> = profiles.iter()
                .map(|(addr, prof)| (addr.clone(), prof.success_metrics.avg_profit_per_move))
                .collect();
                        sorted_profiles.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(Ordering::Equal));
            sorted_profiles.truncate(MAX_TRACKED_BOTS);
            
            let keep_addresses: HashSet<_> = sorted_profiles.into_iter().map(|(addr, _)| addr).collect();
            profiles.retain(|addr, _| keep_addresses.contains(addr));
        }
        
        let mut pattern_cache = self.pattern_cache.write().unwrap();
        pattern_cache.retain(|_, patterns| {
            patterns.retain(|p| current_time - p.last_seen < PATTERN_EXPIRY_SECONDS);
            !patterns.is_empty()
        });
    }

    pub async fn analyze_competitive_landscape(&self) -> CompetitiveLandscape {
        let profiles = self.bot_profiles.read().unwrap();
        let mut pattern_distribution = HashMap::new();
        let mut total_volume = 0.0;
        let mut active_bots = 0;
        
        for profile in profiles.values() {
            if !profile.patterns.is_empty() {
                active_bots += 1;
                for pattern in &profile.patterns {
                    *pattern_distribution.entry(pattern.pattern_type.clone()).or_insert(0) += 1;
                    total_volume += pattern.avg_profit;
                }
            }
        }
        
        CompetitiveLandscape {
            active_bot_count: active_bots,
            dominant_strategies: self.identify_dominant_strategies(&pattern_distribution),
            market_saturation: (active_bots as f64 / MAX_TRACKED_BOTS as f64).min(1.0),
            avg_competition_per_opportunity: self.calculate_avg_competition(),
            profit_concentration: self.calculate_profit_concentration(&profiles),
        }
    }

    fn identify_dominant_strategies(&self, distribution: &HashMap<PatternType, usize>) -> Vec<PatternType> {
        let total: usize = distribution.values().sum();
        let mut strategies: Vec<_> = distribution.iter()
            .filter(|(_, &count)| count as f64 / total as f64 > 0.15)
            .map(|(pattern, _)| pattern.clone())
            .collect();
        strategies.sort_by_key(|p| distribution[p]);
        strategies.reverse();
        strategies
    }

    fn calculate_avg_competition(&self) -> f64 {
        let cache = self.prediction_cache.read().unwrap();
        if cache.is_empty() {
            return 1.0;
        }
        
        let mut competition_counts = HashMap::new();
        for prediction in cache.iter() {
            *competition_counts.entry(&prediction.target_pool).or_insert(0) += 1;
        }
        
        let total: usize = competition_counts.values().sum();
        total as f64 / competition_counts.len().max(1) as f64
    }

    fn calculate_profit_concentration(&self, profiles: &HashMap<Pubkey, BotProfile>) -> f64 {
        let mut profits: Vec<f64> = profiles.values()
            .map(|p| p.success_metrics.avg_profit_per_move)
            .filter(|&p| p > 0.0)
            .collect();
        
        if profits.is_empty() {
            return 0.0;
        }
        
        profits.sort_by(|a, b| b.partial_cmp(a).unwrap_or(Ordering::Equal));
        let total_profit: f64 = profits.iter().sum();
        
        if total_profit == 0.0 {
            return 0.0;
        }
        
        let top_20_percent = (profits.len() as f64 * 0.2).ceil() as usize;
        let top_profit: f64 = profits.iter().take(top_20_percent).sum();
        
        top_profit / total_profit
    }

    pub async fn adapt_strategy(&self, failed_prediction: &PredictedMove, actual_outcome: &TransactionOutcome) {
        let mut profiles = self.bot_profiles.write().unwrap();
        
        if let Some(profile) = profiles.get_mut(&failed_prediction.bot_address) {
            let pattern_idx = profile.patterns.iter().position(|p| p.pattern_type == failed_prediction.pattern_type);
            
            if let Some(idx) = pattern_idx {
                let pattern = &mut profile.patterns[idx];
                pattern.confidence *= 0.9;
                pattern.success_rate = pattern.success_rate * 0.95 + actual_outcome.success as i32 as f64 * 0.05;
                
                if actual_outcome.actual_profit > 0.0 {
                    let profit_error = (pattern.avg_profit - actual_outcome.actual_profit).abs() / pattern.avg_profit;
                    pattern.avg_profit = pattern.avg_profit * (1.0 - STRATEGY_ADAPTATION_RATE) + 
                                       actual_outcome.actual_profit * STRATEGY_ADAPTATION_RATE;
                }
            }
            
            profile.success_metrics.win_rate_vs_us = 
                profile.success_metrics.win_rate_vs_us * 0.9 + 
                (actual_outcome.we_lost as i32 as f64) * 0.1;
        }
    }

    pub async fn emergency_response(&self, threat_level: ThreatLevel) -> EmergencyStrategy {
        match threat_level {
            ThreatLevel::Critical => EmergencyStrategy {
                pause_duration_ms: 5000,
                gas_multiplier_cap: 1.5,
                profit_threshold_multiplier: 2.0,
                allowed_patterns: vec![PatternType::Arbitrage],
            },
            ThreatLevel::High => EmergencyStrategy {
                pause_duration_ms: 1000,
                gas_multiplier_cap: 2.5,
                profit_threshold_multiplier: 1.5,
                allowed_patterns: vec![PatternType::Arbitrage, PatternType::Backrun],
            },
            ThreatLevel::Medium => EmergencyStrategy {
                pause_duration_ms: 0,
                gas_multiplier_cap: 3.5,
                profit_threshold_multiplier: 1.2,
                allowed_patterns: vec![
                    PatternType::Arbitrage,
                    PatternType::Backrun,
                    PatternType::Liquidation,
                ],
            },
            ThreatLevel::Low => EmergencyStrategy {
                pause_duration_ms: 0,
                gas_multiplier_cap: 5.0,
                profit_threshold_multiplier: 1.0,
                allowed_patterns: vec![
                    PatternType::Arbitrage,
                    PatternType::Backrun,
                    PatternType::Sandwich,
                    PatternType::Liquidation,
                    PatternType::FlashLoan,
                ],
            },
        }
    }

    pub fn calculate_threat_level(&self, state: &MarketState) -> ThreatLevel {
        let landscape = tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(self.analyze_competitive_landscape())
        });
        
        let competition_score = landscape.avg_competition_per_opportunity / 5.0;
        let saturation_score = landscape.market_saturation;
        let gas_score = (state.current_gas_price / state.avg_gas_price.max(1.0) - 1.0) / 3.0;
        
        let threat_score = (competition_score * 0.4 + saturation_score * 0.3 + gas_score * 0.3).min(1.0).max(0.0);
        
        match threat_score {
            s if s >= 0.8 => ThreatLevel::Critical,
            s if s >= 0.6 => ThreatLevel::High,
            s if s >= 0.4 => ThreatLevel::Medium,
            _ => ThreatLevel::Low,
        }
    }
}

#[derive(Debug, Clone)]
pub struct TransactionData {
    pub signature: Signature,
    pub bot_address: Pubkey,
    pub instructions: Vec<String>,
    pub target_pool: String,
    pub input_amount: u64,
    pub output_amount: u64,
    pub gas_used: u64,
    pub priority_fee: u64,
    pub slot_offset: i32,
    pub timestamp: u64,
    pub success: bool,
}

#[derive(Debug, Clone)]
pub struct MarketState {
    pub current_slot: u64,
    pub current_gas_price: f64,
    pub avg_gas_price: f64,
    pub pending_tx_count: usize,
    pub price_deviations: HashMap<String, f64>,
    pub pool_volumes: HashMap<String, f64>,
    pub pool_liquidity: HashMap<String, f64>,
    pub volume_24h: f64,
    pub avg_volume_7d: f64,
    pub volatility_index: f64,
}

#[derive(Debug, Clone)]
pub struct CompetitiveLandscape {
    pub active_bot_count: usize,
    pub dominant_strategies: Vec<PatternType>,
    pub market_saturation: f64,
    pub avg_competition_per_opportunity: f64,
    pub profit_concentration: f64,
}

#[derive(Debug, Clone)]
pub struct TransactionOutcome {
    pub success: bool,
    pub actual_profit: f64,
    pub actual_gas: u64,
    pub we_lost: bool,
    pub winning_bot: Option<Pubkey>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum ThreatLevel {
    Low,
    Medium,
    High,
    Critical,
}

#[derive(Debug, Clone)]
pub struct EmergencyStrategy {
    pub pause_duration_ms: u64,
    pub gas_multiplier_cap: f64,
    pub profit_threshold_multiplier: f64,
    pub allowed_patterns: Vec<PatternType>,
}

impl Default for MarketState {
    fn default() -> Self {
        Self {
            current_slot: 0,
            current_gas_price: 1.0,
            avg_gas_price: 1.0,
            pending_tx_count: 0,
            price_deviations: HashMap::new(),
            pool_volumes: HashMap::new(),
            pool_liquidity: HashMap::new(),
            volume_24h: 0.0,
            avg_volume_7d: 0.0,
            volatility_index: 0.0,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_pattern_detection() {
        let (tx, _rx) = mpsc::channel(100);
        let predictor = AdversarialMovePredictor::new("https://api.mainnet-beta.solana.com", tx);
        
        let bot_address = Pubkey::new_unique();
        let test_move = Move {
            timestamp: SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs(),
            pattern_type: PatternType::Arbitrage,
            target: "USDC-SOL".to_string(),
            profit: 0.05,
            gas_used: 5000,
            success: true,
            competing_bots: vec![],
        };
        
        predictor.update_bot_profile(bot_address, test_move).await;
        
        let profiles = predictor.bot_profiles.read().unwrap();
        assert!(profiles.contains_key(&bot_address));
    }
}

