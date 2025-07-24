use solana_sdk::{
    pubkey::Pubkey,
    signature::Signature,
    clock::UnixTimestamp,
};
use std::{
    collections::{HashMap, VecDeque, HashSet},
    sync::{Arc, RwLock},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use serde::{Deserialize, Serialize};
use bincode::{serialize, deserialize};

const BEHAVIOR_WINDOW_SIZE: usize = 1000;
const PATTERN_THRESHOLD: f64 = 0.85;
const MIN_TRANSACTIONS: usize = 10;
const DECAY_FACTOR: f64 = 0.95;
const MAX_TRACKED_BOTS: usize = 10000;
const CONFIDENCE_THRESHOLD: f64 = 0.7;
const SERIALIZATION_VERSION: u8 = 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum BotType {
    Arbitrage,
    Liquidator,
    Sandwich,
    JitLiquidator,
    Sniper,
    MarketMaker,
    Unknown,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum TransactionPattern {
    HighFrequency,
    BurstMode,
    Periodic,
    Reactive,
    Opportunistic,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BotBehavior {
    pub bot_type: BotType,
    pub confidence: f64,
    pub pattern: TransactionPattern,
    pub avg_gas_price: u64,
    pub success_rate: f64,
    pub avg_profit: f64,
    pub last_seen: u64,
    pub transaction_count: u64,
}

#[derive(Debug, Clone)]
pub struct TransactionMetrics {
    pub signature: Signature,
    pub sender: Pubkey,
    pub program_ids: Vec<Pubkey>,
    pub compute_units: u64,
    pub priority_fee: u64,
    pub slot: u64,
    pub timestamp: UnixTimestamp,
    pub success: bool,
    pub profit: Option<f64>,
    pub block_position: u32,
}

#[derive(Debug, Clone)]
struct BotProfile {
    pub address: Pubkey,
    pub transactions: VecDeque<TransactionMetrics>,
    pub behavior: BotBehavior,
    pub pattern_scores: HashMap<TransactionPattern, f64>,
    pub program_interactions: HashMap<Pubkey, u64>,
    pub timing_histogram: [u64; 24],
    pub gas_histogram: [u64; 100],
    pub last_update: Instant,
}

#[derive(Serialize, Deserialize)]
struct ExportedProfile {
    pub address: Pubkey,
    pub behavior: BotBehavior,
    pub top_programs: Vec<Pubkey>,
    pub version: u8,
}

pub struct BotBehaviorClassifier {
    profiles: Arc<RwLock<HashMap<Pubkey, BotProfile>>>,
    known_dex_programs: Vec<Pubkey>,
    known_lending_programs: Vec<Pubkey>,
    pattern_weights: HashMap<TransactionPattern, Vec<f64>>,
}

impl BotBehaviorClassifier {
    pub fn new() -> Self {
        let mut pattern_weights = HashMap::new();
        
        pattern_weights.insert(
            TransactionPattern::HighFrequency,
            vec![0.8, 0.1, 0.05, 0.03, 0.02],
        );
        pattern_weights.insert(
            TransactionPattern::BurstMode,
            vec![0.1, 0.7, 0.1, 0.05, 0.05],
        );
        pattern_weights.insert(
            TransactionPattern::Periodic,
            vec![0.05, 0.1, 0.7, 0.1, 0.05],
        );
        pattern_weights.insert(
            TransactionPattern::Reactive,
            vec![0.02, 0.05, 0.1, 0.73, 0.1],
        );
        pattern_weights.insert(
            TransactionPattern::Opportunistic,
            vec![0.03, 0.05, 0.05, 0.07, 0.8],
        );

        Self {
            profiles: Arc::new(RwLock::new(HashMap::with_capacity(MAX_TRACKED_BOTS))),
            known_dex_programs: vec![
                Pubkey::try_from("JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4").unwrap(),
                Pubkey::try_from("whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc").unwrap(),
                Pubkey::try_from("675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8").unwrap(),
                Pubkey::try_from("srmqPvymJeFKQ4zGQed1GFppgkRHL9kaELCbyksJtPX").unwrap(),
                Pubkey::try_from("9W959DqEETiGZocYWCQPaJ6sBmUzgfxXfqGeTEdp3aQP").unwrap(),
                Pubkey::try_from("CAMMCzo5YL8w4VFF8KVHrK22GGUsp5VTaW7grrKgrWqK").unwrap(),
            ],
            known_lending_programs: vec![
                Pubkey::try_from("FC81tbGt6JWRXidaWYFXxGnTk4VgobhJHATvTRVMqgWj").unwrap(),
                Pubkey::try_from("So1endDq2YkqhipRh3WViPa8hdiSpxWy6z3Z6tMCpAo").unwrap(),
                Pubkey::try_from("6LtLpnUFNByNXLyCoK9wA2MykKAmQNZKBdY8s47dehDc").unwrap(),
                Pubkey::try_from("MFv2hWf31Z9kbCa1snEPYctwafyhdvnV7FZnsebVacA").unwrap(),
                Pubkey::try_from("KLend2g3cP87fffoy8q1mQqGKjrxjC8boSyAYavgmjD").unwrap(),
            ],
            pattern_weights,
        }
    }

    pub fn process_transaction(&self, metrics: TransactionMetrics) -> Result<(), String> {
        let mut profiles = self.profiles.write().map_err(|_| "Lock poisoned")?;
        
        if profiles.len() >= MAX_TRACKED_BOTS && !profiles.contains_key(&metrics.sender) {
            self.evict_stale_profiles(&mut profiles);
        }

        let current_timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| "Time error")?
            .as_secs();

        let profile = profiles.entry(metrics.sender).or_insert_with(|| {
            BotProfile {
                address: metrics.sender,
                transactions: VecDeque::with_capacity(BEHAVIOR_WINDOW_SIZE),
                behavior: BotBehavior {
                    bot_type: BotType::Unknown,
                    confidence: 0.0,
                    pattern: TransactionPattern::Opportunistic,
                    avg_gas_price: 0,
                    success_rate: 0.0,
                    avg_profit: 0.0,
                    last_seen: current_timestamp,
                    transaction_count: 0,
                },
                pattern_scores: HashMap::new(),
                program_interactions: HashMap::new(),
                timing_histogram: [0; 24],
                gas_histogram: [0; 100],
                last_update: Instant::now(),
            }
        });

        profile.transactions.push_back(metrics.clone());
        if profile.transactions.len() > BEHAVIOR_WINDOW_SIZE {
            profile.transactions.pop_front();
        }

        profile.behavior.last_seen = current_timestamp;
        profile.behavior.transaction_count += 1;
        profile.last_update = Instant::now();

        for program in &metrics.program_ids {
            *profile.program_interactions.entry(*program).or_insert(0) += 1;
        }

        let hour = ((metrics.timestamp % 86400) / 3600).clamp(0, 23) as usize;
        profile.timing_histogram[hour] += 1;

        let gas_bucket = (metrics.priority_fee / 1000).min(99) as usize;
        profile.gas_histogram[gas_bucket] += 1;

        self.update_bot_classification(profile);
        Ok(())
    }

    fn update_bot_classification(&self, profile: &mut BotProfile) {
        if profile.transactions.len() < MIN_TRANSACTIONS {
            return;
        }

        let pattern = self.detect_transaction_pattern(profile);
        profile.behavior.pattern = pattern;

        let bot_type = self.classify_bot_type(profile);
        profile.behavior.bot_type = bot_type;

        profile.behavior.confidence = self.calculate_confidence(profile);
        
        self.update_metrics(profile);
    }

    fn detect_transaction_pattern(&self, profile: &BotProfile) -> TransactionPattern {
        let mut pattern_scores = HashMap::new();
        
        let time_intervals: Vec<u64> = profile.transactions
            .iter()
            .zip(profile.transactions.iter().skip(1))
            .map(|(a, b)| (b.timestamp.saturating_sub(a.timestamp)) as u64)
            .filter(|&x| x > 0)
            .collect();

        if time_intervals.is_empty() {
            return TransactionPattern::Opportunistic;
        }

        let sum: u64 = time_intervals.iter().sum();
        let avg_interval = sum.checked_div(time_intervals.len() as u64).unwrap_or(1);
        let std_dev = self.calculate_std_dev(&time_intervals, avg_interval as f64);
        let cv = if avg_interval > 0 { std_dev / avg_interval as f64 } else { 1.0 };

        pattern_scores.insert(
            TransactionPattern::HighFrequency,
            if avg_interval < 5 && cv < 0.3 { 0.9 } else { 0.1 },
        );

        pattern_scores.insert(
            TransactionPattern::BurstMode,
            self.detect_burst_pattern(&time_intervals),
        );

        pattern_scores.insert(
            TransactionPattern::Periodic,
            if cv < 0.2 && avg_interval > 30 { 0.85 } else { 0.15 },
        );

        pattern_scores.insert(
            TransactionPattern::Reactive,
            self.detect_reactive_pattern(profile),
        );

        pattern_scores.insert(
            TransactionPattern::Opportunistic,
            if cv > 0.5 { 0.8 } else { 0.2 },
        );

        profile.pattern_scores.clone_from(&pattern_scores);

        pattern_scores
            .iter()
            .max_by(|a, b| a.1.partial_cmp(b.1).unwrap_or(std::cmp::Ordering::Equal))
            .map(|(pattern, _)| *pattern)
            .unwrap_or(TransactionPattern::Opportunistic)
    }

    fn classify_bot_type(&self, profile: &BotProfile) -> BotType {
        let mut type_scores = HashMap::new();

        let dex_interactions = profile.program_interactions
            .iter()
            .filter(|(k, _)| self.known_dex_programs.contains(k))
            .map(|(_, v)| v)
            .sum::<u64>() as f64;

        let lending_interactions = profile.program_interactions
            .iter()
            .filter(|(k, _)| self.known_lending_programs.contains(k))
            .map(|(_, v)| v)
            .sum::<u64>() as f64;

        let total_interactions = profile.program_interactions.values().sum::<u64>() as f64;

        if total_interactions == 0.0 {
            return BotType::Unknown;
        }

        let dex_ratio = dex_interactions / total_interactions;
        let lending_ratio = lending_interactions / total_interactions;

        type_scores.insert(
            BotType::Arbitrage,
            if dex_ratio > 0.7 && profile.behavior.pattern == TransactionPattern::HighFrequency {
                0.9
            } else {
                dex_ratio * 0.5
            },
        );

        type_scores.insert(
            BotType::Liquidator,
            if lending_ratio > 0.6 && profile.behavior.pattern == TransactionPattern::Reactive {
                0.85
            } else {
                lending_ratio * 0.4
            },
        );

        type_scores.insert(
            BotType::Sandwich,
            self.detect_sandwich_behavior(profile),
        );

        type_scores.insert(
            BotType::JitLiquidator,
            if lending_ratio > 0.5 && profile.behavior.pattern == TransactionPattern::BurstMode {
                0.8
            } else {
                0.1
            },
        );

        type_scores.insert(
            BotType::Sniper,
            self.detect_sniper_behavior(profile),
        );

        type_scores.insert(
            BotType::MarketMaker,
            if dex_ratio > 0.5 && profile.behavior.pattern == TransactionPattern::Periodic {
                0.75
            } else {
                0.15
            },
        );

        type_scores
            .iter()
            .max_by(|a, b| a.1.partial_cmp(b.1).unwrap_or(std::cmp::Ordering::Equal))
            .filter(|(_, score)| **score > CONFIDENCE_THRESHOLD)
            .map(|(bot_type, _)| *bot_type)
            .unwrap_or(BotType::Unknown)
    }

    fn calculate_confidence(&self, profile: &BotProfile) -> f64 {
        let transaction_weight = (profile.transactions.len() as f64 / BEHAVIOR_WINDOW_SIZE as f64).min(1.0);
                let consistency_score = self.calculate_consistency_score(profile);
        let pattern_strength = self.calculate_pattern_strength(profile);
        
        (transaction_weight * 0.3 + consistency_score * 0.4 + pattern_strength * 0.3)
            .min(1.0)
            .max(0.0)
    }

    fn update_metrics(&self, profile: &mut BotProfile) {
        let successful_txs = profile.transactions.iter().filter(|t| t.success).count() as f64;
        let total_txs = profile.transactions.len().max(1) as f64;
        
        profile.behavior.success_rate = successful_txs / total_txs;

        let total_gas: u64 = profile.transactions.iter().map(|t| t.priority_fee).sum();
        profile.behavior.avg_gas_price = total_gas.checked_div(profile.transactions.len() as u64).unwrap_or(0);

        let profits: Vec<f64> = profile.transactions
            .iter()
            .filter_map(|t| t.profit)
            .collect();
        
        profile.behavior.avg_profit = if !profits.is_empty() {
            profits.iter().sum::<f64>() / profits.len() as f64
        } else {
            0.0
        };
    }

    fn calculate_std_dev(&self, values: &[u64], mean: f64) -> f64 {
        if values.len() < 2 {
            return 0.0;
        }
        
        let variance = values
            .iter()
            .map(|&x| {
                let diff = x as f64 - mean;
                diff * diff
            })
            .sum::<f64>() / values.len() as f64;
        
        variance.sqrt()
    }

    fn detect_burst_pattern(&self, intervals: &[u64]) -> f64 {
        if intervals.len() < 10 {
            return 0.1;
        }

        let mut burst_count = 0;
        let mut in_burst = false;
        let burst_threshold = 2;

        for &interval in intervals {
            if interval < burst_threshold {
                if !in_burst {
                    burst_count += 1;
                    in_burst = true;
                }
            } else {
                in_burst = false;
            }
        }

        let burst_ratio = burst_count as f64 / (intervals.len().max(5) / 5) as f64;
        (burst_ratio * 0.8).min(0.9)
    }

    fn detect_reactive_pattern(&self, profile: &BotProfile) -> f64 {
        let mut reaction_score = 0.0;
        let mut total_checks = 0;

        for window in profile.transactions.windows(3) {
            if window.len() < 3 {
                continue;
            }

            let time_diff_1 = window[1].timestamp.saturating_sub(window[0].timestamp).abs();
            let time_diff_2 = window[2].timestamp.saturating_sub(window[1].timestamp).abs();

            if time_diff_1 < 5 && time_diff_2 < 5 {
                let block_diff = window[2].slot.saturating_sub(window[0].slot);
                if block_diff <= 2 {
                    reaction_score += 1.0;
                }
            }
            total_checks += 1;
        }

        if total_checks > 0 {
            (reaction_score / total_checks as f64).min(0.9)
        } else {
            0.1
        }
    }

    fn detect_sandwich_behavior(&self, profile: &BotProfile) -> f64 {
        let mut sandwich_patterns = 0;
        let windows: Vec<_> = profile.transactions.iter().collect();

        for i in 0..windows.len().saturating_sub(2) {
            let tx1 = windows[i];
            let tx2 = windows[i + 1];
            let tx3 = windows.get(i + 2);

            if let Some(tx3) = tx3 {
                if tx1.slot == tx2.slot && tx2.slot == tx3.slot {
                    if tx1.block_position < tx2.block_position && tx2.block_position < tx3.block_position {
                        let same_programs = tx1.program_ids.iter()
                            .any(|p| tx3.program_ids.contains(p) && self.known_dex_programs.contains(p));
                        
                        if same_programs && tx1.profit.unwrap_or(0.0) > 0.0 && tx3.profit.unwrap_or(0.0) > 0.0 {
                            sandwich_patterns += 1;
                        }
                    }
                }
            }
        }

        let pattern_ratio = sandwich_patterns as f64 / profile.transactions.len().max(1) as f64;
        (pattern_ratio * 10.0).min(0.95)
    }

    fn detect_sniper_behavior(&self, profile: &BotProfile) -> f64 {
        let mut sniper_score = 0.0;
        
        let early_blocks: Vec<_> = profile.transactions
            .iter()
            .filter(|t| t.block_position < 10)
            .collect();

        let early_ratio = early_blocks.len() as f64 / profile.transactions.len().max(1) as f64;
        if early_ratio > 0.6 {
            sniper_score += 0.4;
        }

        let high_gas_txs = profile.transactions
            .iter()
            .filter(|t| t.priority_fee > 1000000)
            .count() as f64;

        let high_gas_ratio = high_gas_txs / profile.transactions.len().max(1) as f64;
        if high_gas_ratio > 0.5 {
            sniper_score += 0.3;
        }

        if profile.behavior.pattern == TransactionPattern::HighFrequency {
            sniper_score += 0.2;
        }

        let success_on_high_gas = profile.transactions
            .iter()
            .filter(|t| t.priority_fee > 1000000 && t.success)
            .count() as f64;

        if high_gas_txs > 0.0 {
            let high_gas_success_rate = success_on_high_gas / high_gas_txs;
            if high_gas_success_rate > 0.8 {
                sniper_score += 0.1;
            }
        }

        sniper_score.min(0.95)
    }

    fn calculate_consistency_score(&self, profile: &BotProfile) -> f64 {
        let gas_variance = self.calculate_gas_variance(profile);
        let timing_consistency = self.calculate_timing_consistency(profile);
        let program_consistency = self.calculate_program_consistency(profile);

        let gas_score = (1.0 - gas_variance.min(1.0)) * 0.3;
        let timing_score = timing_consistency * 0.3;
        let program_score = program_consistency * 0.4;

        gas_score + timing_score + program_score
    }

    fn calculate_pattern_strength(&self, profile: &BotProfile) -> f64 {
        let pattern_weights = match self.pattern_weights.get(&profile.behavior.pattern) {
            Some(weights) => weights,
            None => return 0.5,
        };
        
        let mut score = 0.0;
        
        let intervals: Vec<u64> = profile.transactions
            .iter()
            .zip(profile.transactions.iter().skip(1))
            .map(|(a, b)| (b.timestamp.saturating_sub(a.timestamp)) as u64)
            .filter(|&x| x > 0)
            .collect();

        if !intervals.is_empty() {
            let sum: u64 = intervals.iter().sum();
            let avg_interval = sum / intervals.len() as u64;
            
            let interval_score = match profile.behavior.pattern {
                TransactionPattern::HighFrequency => {
                    if avg_interval < 5 { 0.9 } else { 0.3 }
                }
                TransactionPattern::Periodic => {
                    let cv = self.calculate_std_dev(&intervals, avg_interval as f64) / avg_interval.max(1) as f64;
                    if cv < 0.2 { 0.9 } else { 0.3 }
                }
                _ => 0.5,
            };
            
            score += interval_score * weights.get(0).unwrap_or(&0.5);
            score += profile.behavior.success_rate * weights.get(1).unwrap_or(&0.5);
            
            let profit_weight = if profile.behavior.avg_profit > 0.0 { 0.8 } else { 0.2 };
            score += profit_weight * weights.get(2).unwrap_or(&0.5);
        }
        
        score.min(1.0)
    }

    fn calculate_gas_variance(&self, profile: &BotProfile) -> f64 {
        let gas_prices: Vec<f64> = profile.transactions
            .iter()
            .map(|t| t.priority_fee as f64)
            .collect();

        if gas_prices.len() < 2 {
            return 0.0;
        }

        let sum: f64 = gas_prices.iter().sum();
        let mean = sum / gas_prices.len() as f64;
        
        if mean == 0.0 {
            return 0.0;
        }

        let variance = gas_prices
            .iter()
            .map(|&x| (x - mean).powi(2))
            .sum::<f64>() / gas_prices.len() as f64;

        (variance.sqrt() / mean).min(1.0)
    }

    fn calculate_timing_consistency(&self, profile: &BotProfile) -> f64 {
        let total_txs = profile.timing_histogram.iter().sum::<u64>() as f64;
        
        if total_txs == 0.0 {
            return 0.0;
        }

        let mut entropy = 0.0;
        for &count in &profile.timing_histogram {
            if count > 0 {
                let p = count as f64 / total_txs;
                entropy -= p * p.log2();
            }
        }

        let max_entropy = (profile.timing_histogram.len() as f64).log2();
        if max_entropy > 0.0 {
            1.0 - (entropy / max_entropy).min(1.0)
        } else {
            0.0
        }
    }

    fn calculate_program_consistency(&self, profile: &BotProfile) -> f64 {
        let total_interactions = profile.program_interactions.values().sum::<u64>() as f64;
        
        if total_interactions == 0.0 {
            return 0.0;
        }

        let mut concentration = 0.0;
        for &count in profile.program_interactions.values() {
            let ratio = count as f64 / total_interactions;
            concentration += ratio * ratio;
        }

        concentration.min(1.0)
    }

    fn evict_stale_profiles(&self, profiles: &mut HashMap<Pubkey, BotProfile>) {
        let now = Instant::now();
        let stale_threshold = Duration::from_secs(3600);
        
        let mut stale_addresses: Vec<Pubkey> = profiles
            .iter()
            .filter(|(_, p)| now.duration_since(p.last_update) > stale_threshold)
            .map(|(addr, _)| *addr)
            .collect();

        if stale_addresses.len() < MAX_TRACKED_BOTS / 10 {
            let mut profiles_by_score: Vec<(Pubkey, f64)> = profiles
                .iter()
                .map(|(addr, p)| {
                    let age_seconds = now.duration_since(p.last_update).as_secs() as f64;
                    let recency_score = 1.0 / (age_seconds + 1.0);
                    let activity_score = (p.behavior.transaction_count as f64 / 1000.0).min(1.0);
                    let confidence_score = p.behavior.confidence;
                    (*addr, recency_score * 0.5 + activity_score * 0.3 + confidence_score * 0.2)
                })
                .collect();

            profiles_by_score.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));
            
            let evict_count = (MAX_TRACKED_BOTS / 20).min(profiles_by_score.len());
            stale_addresses.extend(
                profiles_by_score
                    .iter()
                    .take(evict_count)
                    .map(|(addr, _)| *addr)
            );
        }

        for addr in stale_addresses {
            profiles.remove(&addr);
        }
    }

    pub fn get_bot_behavior(&self, address: &Pubkey) -> Option<BotBehavior> {
        self.profiles
            .read()
            .ok()?
            .get(address)
            .map(|p| p.behavior.clone())
    }

    pub fn get_known_bots(&self, bot_type: BotType) -> Vec<(Pubkey, BotBehavior)> {
        match self.profiles.read() {
            Ok(profiles) => profiles
                .iter()
                .filter(|(_, p)| p.behavior.bot_type == bot_type && p.behavior.confidence >= CONFIDENCE_THRESHOLD)
                .map(|(addr, p)| (*addr, p.behavior.clone()))
                .collect(),
            Err(_) => Vec::new(),
        }
    }

    pub fn get_top_bots(&self, limit: usize) -> Vec<(Pubkey, BotBehavior)> {
        match self.profiles.read() {
            Ok(profiles) => {
                let mut bots: Vec<_> = profiles
                    .iter()
                    .filter(|(_, p)| p.behavior.confidence >= CONFIDENCE_THRESHOLD)
                    .map(|(addr, p)| (*addr, p.behavior.clone()))
                    .collect();

                bots.sort_by(|a, b| {
                    let score_a = a.1.confidence * a.1.success_rate * (a.1.transaction_count as f64).sqrt();
                    let score_b = b.1.confidence * b.1.success_rate * (b.1.transaction_count as f64).sqrt();
                                        score_b.partial_cmp(&score_a).unwrap_or(std::cmp::Ordering::Equal)
                });

                bots.into_iter().take(limit).collect()
            }
            Err(_) => Vec::new(),
        }
    }

    pub fn is_likely_bot(&self, address: &Pubkey, min_confidence: f64) -> bool {
        match self.profiles.read() {
            Ok(profiles) => profiles
                .get(address)
                .map(|p| p.behavior.confidence >= min_confidence && p.behavior.bot_type != BotType::Unknown)
                .unwrap_or(false),
            Err(_) => false,
        }
    }

    pub fn predict_next_action(&self, address: &Pubkey) -> Option<(u64, f64)> {
        let profiles = self.profiles.read().ok()?;
        let profile = profiles.get(address)?;
        
        if profile.transactions.len() < MIN_TRANSACTIONS {
            return None;
        }

        match profile.behavior.pattern {
            TransactionPattern::Periodic => {
                let intervals: Vec<u64> = profile.transactions
                    .iter()
                    .zip(profile.transactions.iter().skip(1))
                    .map(|(a, b)| b.timestamp.saturating_sub(a.timestamp) as u64)
                    .filter(|&x| x > 0)
                    .collect();
                
                if !intervals.is_empty() {
                    let sum: u64 = intervals.iter().sum();
                    let avg_interval = sum / intervals.len() as u64;
                    let last_tx = profile.transactions.back()?;
                    let predicted_time = last_tx.timestamp as u64 + avg_interval;
                    let confidence = profile.behavior.confidence * 0.85;
                    return Some((predicted_time, confidence));
                }
            }
            TransactionPattern::HighFrequency => {
                let last_tx = profile.transactions.back()?;
                let predicted_time = last_tx.timestamp as u64 + 2;
                let confidence = profile.behavior.confidence * 0.7;
                return Some((predicted_time, confidence));
            }
            _ => {}
        }
        
        None
    }

    pub fn analyze_competitive_advantage(&self, our_address: &Pubkey) -> HashMap<BotType, f64> {
        let mut advantages = HashMap::new();
        
        let profiles = match self.profiles.read() {
            Ok(p) => p,
            Err(_) => return advantages,
        };
        
        let our_profile = match profiles.get(our_address) {
            Some(p) => p,
            None => return advantages,
        };

        for bot_type in &[
            BotType::Arbitrage,
            BotType::Liquidator,
            BotType::Sandwich,
            BotType::JitLiquidator,
            BotType::Sniper,
            BotType::MarketMaker,
        ] {
            let competitors: Vec<&BotProfile> = profiles
                .values()
                .filter(|p| p.behavior.bot_type == *bot_type && p.address != *our_address)
                .collect();

            if competitors.is_empty() {
                advantages.insert(*bot_type, 1.0);
                continue;
            }

            let competitor_count = competitors.len() as f64;
            let avg_competitor_success = competitors
                .iter()
                .map(|p| p.behavior.success_rate)
                .sum::<f64>() / competitor_count;

            let avg_competitor_gas = competitors
                .iter()
                .map(|p| p.behavior.avg_gas_price)
                .sum::<u64>() / competitors.len() as u64;

            let our_success = our_profile.behavior.success_rate;
            let our_gas = our_profile.behavior.avg_gas_price;

            let success_advantage = if avg_competitor_success > 0.0 {
                our_success / avg_competitor_success
            } else {
                1.0
            };

            let gas_efficiency = if our_gas > 0 && avg_competitor_gas > 0 {
                avg_competitor_gas as f64 / our_gas as f64
            } else {
                1.0
            };

            let advantage = (success_advantage * 0.7 + gas_efficiency * 0.3).min(2.0).max(0.0);
            advantages.insert(*bot_type, advantage);
        }

        advantages
    }

    pub fn get_competitor_strategies(&self, bot_type: BotType) -> Vec<(Pubkey, HashMap<String, f64>)> {
        let profiles = match self.profiles.read() {
            Ok(p) => p,
            Err(_) => return Vec::new(),
        };
        
        let mut strategies = Vec::new();

        for (addr, profile) in profiles.iter() {
            if profile.behavior.bot_type != bot_type || profile.behavior.confidence < CONFIDENCE_THRESHOLD {
                continue;
            }

            let mut strategy_metrics = HashMap::new();
            
            strategy_metrics.insert("avg_gas_price".to_string(), profile.behavior.avg_gas_price as f64);
            strategy_metrics.insert("success_rate".to_string(), profile.behavior.success_rate);
            strategy_metrics.insert("avg_profit".to_string(), profile.behavior.avg_profit);
            strategy_metrics.insert("transaction_frequency".to_string(), 
                profile.behavior.transaction_count as f64 / 3600.0);
            
            let most_used_program = profile.program_interactions
                .iter()
                .max_by_key(|(_, count)| *count)
                .map(|(program, count)| {
                    let ratio = *count as f64 / profile.behavior.transaction_count.max(1) as f64;
                    strategy_metrics.insert("primary_program_ratio".to_string(), ratio);
                    *program
                });

            if let Some(program) = most_used_program {
                let program_index = self.known_dex_programs
                    .iter()
                    .position(|p| p == &program)
                    .unwrap_or(99) as f64;
                strategy_metrics.insert("primary_dex_index".to_string(), program_index);
            }

            let peak_hour = profile.timing_histogram
                .iter()
                .enumerate()
                .max_by_key(|(_, count)| *count)
                .map(|(hour, _)| hour)
                .unwrap_or(0) as f64;
            strategy_metrics.insert("peak_hour".to_string(), peak_hour);

            strategies.push((*addr, strategy_metrics));
        }

        strategies.sort_by(|a, b| {
            let score_a = a.1.get("success_rate").unwrap_or(&0.0) * a.1.get("avg_profit").unwrap_or(&0.0);
            let score_b = b.1.get("success_rate").unwrap_or(&0.0) * b.1.get("avg_profit").unwrap_or(&0.0);
            score_b.partial_cmp(&score_a).unwrap_or(std::cmp::Ordering::Equal)
        });

        strategies
    }

    pub fn export_classifier_state(&self) -> Result<Vec<u8>, String> {
        let profiles = self.profiles.read().map_err(|_| "Lock poisoned")?;
        
        let exportable_profiles: Vec<ExportedProfile> = profiles
            .iter()
            .filter(|(_, p)| p.behavior.confidence >= CONFIDENCE_THRESHOLD)
            .map(|(addr, p)| {
                let top_programs: Vec<Pubkey> = p.program_interactions
                    .iter()
                    .filter(|(_, count)| **count > 5)
                    .map(|(prog, _)| *prog)
                    .take(5)
                    .collect();
                
                ExportedProfile {
                    address: *addr,
                    behavior: p.behavior.clone(),
                    top_programs,
                    version: SERIALIZATION_VERSION,
                }
            })
            .collect();

        bincode::serialize(&exportable_profiles).map_err(|e| format!("Serialization error: {}", e))
    }

    pub fn import_classifier_state(&self, data: &[u8]) -> Result<usize, String> {
        let imported: Vec<ExportedProfile> = bincode::deserialize(data)
            .map_err(|e| format!("Deserialization error: {}", e))?;

        let mut profiles = self.profiles.write().map_err(|_| "Lock poisoned")?;
        let mut imported_count = 0;

        for export_profile in imported {
            if export_profile.version != SERIALIZATION_VERSION {
                continue;
            }

            if profiles.contains_key(&export_profile.address) {
                continue;
            }

            let mut profile = BotProfile {
                address: export_profile.address,
                transactions: VecDeque::with_capacity(BEHAVIOR_WINDOW_SIZE),
                behavior: export_profile.behavior,
                pattern_scores: HashMap::new(),
                program_interactions: HashMap::new(),
                timing_histogram: [0; 24],
                gas_histogram: [0; 100],
                last_update: Instant::now(),
            };

            for program in export_profile.top_programs {
                profile.program_interactions.insert(program, 10);
            }

            profiles.insert(export_profile.address, profile);
            imported_count += 1;
        }

        Ok(imported_count)
    }

    pub fn get_pattern_distribution(&self) -> HashMap<TransactionPattern, usize> {
        let profiles = match self.profiles.read() {
            Ok(p) => p,
            Err(_) => return HashMap::new(),
        };
        
        let mut distribution = HashMap::new();

        for profile in profiles.values() {
            if profile.behavior.confidence >= CONFIDENCE_THRESHOLD {
                *distribution.entry(profile.behavior.pattern).or_insert(0) += 1;
            }
        }

        distribution
    }

    pub fn get_gas_percentile(&self, address: &Pubkey, percentile: f64) -> Option<u64> {
        let profiles = self.profiles.read().ok()?;
        let profile = profiles.get(address)?;

        let mut gas_prices: Vec<u64> = profile.transactions
            .iter()
            .map(|t| t.priority_fee)
            .collect();

        if gas_prices.is_empty() {
            return None;
        }

        gas_prices.sort_unstable();
        let index = ((percentile / 100.0) * (gas_prices.len() - 1) as f64).round() as usize;
        Some(gas_prices[index.min(gas_prices.len() - 1)])
    }

    pub fn detect_coordinated_bots(&self) -> Vec<Vec<Pubkey>> {
        let profiles = match self.profiles.read() {
            Ok(p) => p,
            Err(_) => return Vec::new(),
        };
        
        let mut groups: Vec<Vec<Pubkey>> = Vec::new();
        let mut processed: HashSet<Pubkey> = HashSet::new();

        for (addr1, profile1) in profiles.iter() {
            if processed.contains(addr1) {
                continue;
            }

            let mut group = vec![*addr1];
            processed.insert(*addr1);

            for (addr2, profile2) in profiles.iter() {
                if addr1 == addr2 || processed.contains(addr2) {
                    continue;
                }

                if self.are_bots_coordinated(profile1, profile2) {
                    group.push(*addr2);
                    processed.insert(*addr2);
                }
            }

            if group.len() > 1 {
                groups.push(group);
            }
        }

        groups
    }

    fn are_bots_coordinated(&self, profile1: &BotProfile, profile2: &BotProfile) -> bool {
        if profile1.behavior.bot_type != profile2.behavior.bot_type {
            return false;
        }

        let timing_similarity = self.calculate_timing_similarity(profile1, profile2);
        let program_similarity = self.calculate_program_similarity(profile1, profile2);
        let pattern_match = profile1.behavior.pattern == profile2.behavior.pattern;

        timing_similarity > 0.8 && program_similarity > 0.7 && pattern_match
    }

    fn calculate_timing_similarity(&self, profile1: &BotProfile, profile2: &BotProfile) -> f64 {
        let total1 = profile1.timing_histogram.iter().sum::<u64>() as f64;
        let total2 = profile2.timing_histogram.iter().sum::<u64>() as f64;

        if total1 == 0.0 || total2 == 0.0 {
            return 0.0;
        }

        let mut similarity = 0.0;
        for i in 0..24 {
            let p1 = profile1.timing_histogram[i] as f64 / total1;
            let p2 = profile2.timing_histogram[i] as f64 / total2;
            similarity += (p1 * p2).sqrt();
        }

        similarity.min(1.0)
    }

    fn calculate_program_similarity(&self, profile1: &BotProfile, profile2: &BotProfile) -> f64 {
        let all_programs: HashSet<&Pubkey> = profile1.program_interactions.keys()
            .chain(profile2.program_interactions.keys())
            .collect();

        if all_programs.is_empty() {
            return 0.0;
        }

        let mut dot_product = 0.0;
        let mut norm1 = 0.0;
        let mut norm2 = 0.0;

        for program in all_programs {
            let v1 = profile1.program_interactions.get(program).unwrap_or(&0) * 1 as f64;
            let v2 = profile2.program_interactions.get(program).unwrap_or(&0) * 1 as f64;
            
            dot_product += v1 * v2;
            norm1 += v1 * v1;
            norm2 += v2 * v2;
        }

        if norm1 > 0.0 && norm2 > 0.0 {
            dot_product / (norm1.sqrt() * norm2.sqrt())
        } else {
            0.0
        }
    }

    pub fn cleanup_old_data(&self, max_age_hours: u64) -> Result<(), String> {
        let mut profiles = self.profiles.write().map_err(|_| "Lock poisoned")?;
        let cutoff_time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| "Time error")?
            .as_secs() - (max_age_hours * 3600);
        
                profiles.retain(|_, profile| {
            profile.behavior.last_seen > cutoff_time
        });

        for profile in profiles.values_mut() {
            let cutoff_timestamp = (SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs() - (max_age_hours * 3600)) as i64;
            
            profile.transactions.retain(|tx| tx.timestamp > cutoff_timestamp);
            
            // Recalculate metrics after cleanup
            if profile.transactions.len() >= MIN_TRANSACTIONS {
                self.update_bot_classification(profile);
            }
        }
        
        Ok(())
    }

    pub fn get_competitive_metrics(&self, address: &Pubkey) -> Option<HashMap<String, f64>> {
        let profiles = self.profiles.read().ok()?;
        let profile = profiles.get(address)?;
        
        let mut metrics = HashMap::new();
        
        // Calculate win rate against competitors
        let competitors = profiles
            .values()
            .filter(|p| p.behavior.bot_type == profile.behavior.bot_type && p.address != *address)
            .count() as f64;
        
        metrics.insert("total_competitors".to_string(), competitors);
        metrics.insert("confidence_rank".to_string(), self.calculate_confidence_rank(address, &profiles));
        metrics.insert("gas_efficiency_percentile".to_string(), self.calculate_gas_efficiency_percentile(profile, &profiles));
        metrics.insert("success_rate_percentile".to_string(), self.calculate_success_rate_percentile(profile, &profiles));
        
        // Calculate competitive edge score
        let edge_score = (metrics.get("gas_efficiency_percentile").unwrap_or(&50.0) * 0.3 +
                         metrics.get("success_rate_percentile").unwrap_or(&50.0) * 0.5 +
                         metrics.get("confidence_rank").unwrap_or(&50.0) * 0.2) / 100.0;
        
        metrics.insert("competitive_edge_score".to_string(), edge_score);
        
        Some(metrics)
    }

    fn calculate_confidence_rank(&self, address: &Pubkey, profiles: &HashMap<Pubkey, BotProfile>) -> f64 {
        let target_confidence = profiles.get(address).map(|p| p.behavior.confidence).unwrap_or(0.0);
        let target_type = profiles.get(address).map(|p| p.behavior.bot_type).unwrap_or(BotType::Unknown);
        
        let same_type_bots: Vec<f64> = profiles
            .values()
            .filter(|p| p.behavior.bot_type == target_type)
            .map(|p| p.behavior.confidence)
            .collect();
        
        if same_type_bots.is_empty() {
            return 100.0;
        }
        
        let better_count = same_type_bots.iter().filter(|&&c| c > target_confidence).count() as f64;
        ((1.0 - better_count / same_type_bots.len() as f64) * 100.0).max(0.0)
    }

    fn calculate_gas_efficiency_percentile(&self, profile: &BotProfile, profiles: &HashMap<Pubkey, BotProfile>) -> f64 {
        let same_type_gas: Vec<u64> = profiles
            .values()
            .filter(|p| p.behavior.bot_type == profile.behavior.bot_type)
            .map(|p| p.behavior.avg_gas_price)
            .filter(|&g| g > 0)
            .collect();
        
        if same_type_gas.is_empty() {
            return 50.0;
        }
        
        let better_count = same_type_gas.iter().filter(|&&g| g < profile.behavior.avg_gas_price).count() as f64;
        (better_count / same_type_gas.len() as f64 * 100.0).max(0.0).min(100.0)
    }

    fn calculate_success_rate_percentile(&self, profile: &BotProfile, profiles: &HashMap<Pubkey, BotProfile>) -> f64 {
        let same_type_success: Vec<f64> = profiles
            .values()
            .filter(|p| p.behavior.bot_type == profile.behavior.bot_type)
            .map(|p| p.behavior.success_rate)
            .collect();
        
        if same_type_success.is_empty() {
            return 50.0;
        }
        
        let worse_count = same_type_success.iter().filter(|&&s| s < profile.behavior.success_rate).count() as f64;
        (worse_count / same_type_success.len() as f64 * 100.0).max(0.0).min(100.0)
    }

    pub fn predict_competitor_moves(&self, bot_type: BotType, time_window: u64) -> Vec<(Pubkey, f64, String)> {
        let profiles = match self.profiles.read() {
            Ok(p) => p,
            Err(_) => return Vec::new(),
        };
        
        let current_time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        
        let mut predictions = Vec::new();
        
        for (addr, profile) in profiles.iter() {
            if profile.behavior.bot_type != bot_type || profile.behavior.confidence < CONFIDENCE_THRESHOLD {
                continue;
            }
            
            if let Some((predicted_time, confidence)) = self.predict_next_action(addr) {
                if predicted_time >= current_time && predicted_time <= current_time + time_window {
                    let action_type = match profile.behavior.pattern {
                        TransactionPattern::HighFrequency => "high_freq_trade",
                        TransactionPattern::BurstMode => "burst_attack",
                        TransactionPattern::Periodic => "scheduled_action",
                        TransactionPattern::Reactive => "reactive_trade",
                        TransactionPattern::Opportunistic => "opportunity_hunt",
                    };
                    
                    predictions.push((*addr, confidence, action_type.to_string()));
                }
            }
        }
        
        predictions.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
        predictions
    }

    pub fn get_real_time_threats(&self, our_address: &Pubkey, threat_window: u64) -> Vec<(Pubkey, String, f64)> {
        let profiles = match self.profiles.read() {
            Ok(p) => p,
            Err(_) => return Vec::new(),
        };
        
        let our_profile = match profiles.get(our_address) {
            Some(p) => p,
            None => return Vec::new(),
        };
        
        let current_time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        
        let mut threats = Vec::new();
        
        for (addr, profile) in profiles.iter() {
            if addr == our_address || profile.behavior.confidence < CONFIDENCE_THRESHOLD {
                continue;
            }
            
            // Check if competitor is active in same time window
            let recent_activity = profile.behavior.last_seen + threat_window > current_time;
            
            // Check if competitor targets same programs
            let program_overlap = profile.program_interactions
                .keys()
                .filter(|p| our_profile.program_interactions.contains_key(p))
                .count();
            
            let overlap_ratio = program_overlap as f64 / our_profile.program_interactions.len().max(1) as f64;
            
            if recent_activity && overlap_ratio > 0.5 {
                let threat_level = match profile.behavior.bot_type {
                    BotType::Sandwich => {
                        if our_profile.behavior.bot_type == BotType::Arbitrage {
                            0.9 // High threat - sandwich bots can attack arbitrage bots
                        } else {
                            0.5
                        }
                    }
                    BotType::Sniper => {
                        if profile.behavior.avg_gas_price > our_profile.behavior.avg_gas_price {
                            0.8 // High threat - they outbid us
                        } else {
                            0.4
                        }
                    }
                    _ => {
                        if profile.behavior.bot_type == our_profile.behavior.bot_type {
                            0.7 * profile.behavior.confidence // Direct competitor
                        } else {
                            0.3
                        }
                    }
                };
                
                let threat_type = format!("{:?}:{}", profile.behavior.bot_type, profile.behavior.pattern as u8);
                threats.push((*addr, threat_type, threat_level));
            }
        }
        
        threats.sort_by(|a, b| b.2.partial_cmp(&a.2).unwrap_or(std::cmp::Ordering::Equal));
        threats
    }

    pub fn optimize_gas_strategy(&self, address: &Pubkey, target_success_rate: f64) -> Option<u64> {
        let profiles = self.profiles.read().ok()?;
        let profile = profiles.get(address)?;
        
        if profile.transactions.len() < MIN_TRANSACTIONS {
            return None;
        }
        
        // Analyze successful transactions and their gas prices
        let mut success_gas_prices: Vec<(u64, bool)> = profile.transactions
            .iter()
            .map(|t| (t.priority_fee, t.success))
            .collect();
        
        success_gas_prices.sort_by_key(|&(gas, _)| gas);
        
        // Find minimum gas price that achieves target success rate
        let total = success_gas_prices.len() as f64;
        let mut optimal_gas = profile.behavior.avg_gas_price;
        
        for window_size in [10, 20, 50].iter() {
            if success_gas_prices.len() < *window_size {
                continue;
            }
            
            for i in 0..success_gas_prices.len() - window_size {
                let window = &success_gas_prices[i..i + window_size];
                let success_count = window.iter().filter(|(_, success)| *success).count() as f64;
                let window_success_rate = success_count / *window_size as f64;
                
                if window_success_rate >= target_success_rate {
                    let window_avg_gas = window.iter().map(|(gas, _)| gas).sum::<u64>() / *window_size as u64;
                    if window_avg_gas < optimal_gas {
                        optimal_gas = window_avg_gas;
                    }
                }
            }
        }
        
        // Add competitive adjustment based on current market
        let same_type_competitors = profiles
            .values()
            .filter(|p| p.behavior.bot_type == profile.behavior.bot_type && p.address != *address)
            .count();
        
        let competition_multiplier = 1.0 + (same_type_competitors as f64 * 0.05).min(0.5);
        let adjusted_gas = (optimal_gas as f64 * competition_multiplier) as u64;
        
        Some(adjusted_gas.max(1000)) // Minimum 1000 lamports
    }

    pub fn get_statistical_summary(&self) -> HashMap<String, f64> {
        let profiles = match self.profiles.read() {
            Ok(p) => p,
            Err(_) => return HashMap::new(),
        };
        
        let mut summary = HashMap::new();
        
        summary.insert("total_tracked_bots".to_string(), profiles.len() as f64);
        summary.insert("high_confidence_bots".to_string(), 
            profiles.values().filter(|p| p.behavior.confidence >= CONFIDENCE_THRESHOLD).count() as f64);
        
        for bot_type in &[
            BotType::Arbitrage,
            BotType::Liquidator,
            BotType::Sandwich,
            BotType::JitLiquidator,
            BotType::Sniper,
            BotType::MarketMaker,
        ] {
            let type_count = profiles.values()
                .filter(|p| p.behavior.bot_type == *bot_type)
                .count() as f64;
            summary.insert(format!("{:?}_count", bot_type), type_count);
        }
        
        let avg_confidence = profiles.values()
            .map(|p| p.behavior.confidence)
            .sum::<f64>() / profiles.len().max(1) as f64;
        summary.insert("avg_confidence".to_string(), avg_confidence);
        
        let avg_success_rate = profiles.values()
            .map(|p| p.behavior.success_rate)
            .sum::<f64>() / profiles.len().max(1) as f64;
        summary.insert("avg_success_rate".to_string(), avg_success_rate);
        
        summary
    }
}

impl Default for BotBehaviorClassifier {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_bot_classifier_creation() {
        let classifier = BotBehaviorClassifier::new();
        assert_eq!(classifier.get_statistical_summary().get("total_tracked_bots"), Some(&0.0));
    }
    
    #[test]
    fn test_pattern_detection() {
        let classifier = BotBehaviorClassifier::new();
        let sender = Pubkey::new_unique();
        
        for i in 0..20 {
            let metrics = TransactionMetrics {
                signature: Signature::default(),
                sender,
                program_ids: vec![Pubkey::new_unique()],
                compute_units: 100000,
                priority_fee: 1000000,
                slot: i * 2,
                timestamp: i * 5,
                success: true,
                profit: Some(0.01),
                block_position: 5,
            };
            
            let _ = classifier.process_transaction(metrics);
        }
        
        assert!(classifier.is_likely_bot(&sender, 0.5));
    }
}

