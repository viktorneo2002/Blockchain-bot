use solana_client::rpc_client::RpcClient;
use solana_sdk::{
    commitment_config::CommitmentConfig,
    pubkey::Pubkey,
    signature::{Keypair, Signer},
    transaction::Transaction,
};
use std::{
    collections::{HashMap, VecDeque},
    sync::{Arc, RwLock},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use rand::{thread_rng, Rng, SeedableRng};
use rand_chacha::ChaCha20Rng;
use serde::{Deserialize, Serialize};
use rayon::prelude::*;

const MAX_PATTERN_HISTORY: usize = 1000;
const NOISE_INJECTION_THRESHOLD: f64 = 0.85;
const PATTERN_DETECTION_WINDOW: u64 = 300; // 5 minutes
const MIN_TRADE_INTERVAL_MS: u64 = 50;
const MAX_TRADE_INTERVAL_MS: u64 = 5000;
const VOLUME_NOISE_FACTOR: f64 = 0.3;
const TIMING_JITTER_RANGE: f64 = 0.4;
const ORDER_SPLIT_THRESHOLD: u64 = 10_000_000; // 0.01 SOL
const MAX_SPLIT_ORDERS: usize = 7;
const PATTERN_ENTROPY_THRESHOLD: f64 = 0.7;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TradePattern {
    pub timestamp: u64,
    pub volume: u64,
    pub direction: TradeDirection,
    pub price_impact: f64,
    pub venue: String,
    pub signature_hash: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub enum TradeDirection {
    Buy,
    Sell,
}

#[derive(Debug, Clone)]
pub struct NoiseConfig {
    pub volume_randomization: bool,
    pub timing_randomization: bool,
    pub order_splitting: bool,
    pub venue_rotation: bool,
    pub pattern_breaking: bool,
    pub entropy_target: f64,
    pub max_slippage_bps: u16,
}

impl Default for NoiseConfig {
    fn default() -> Self {
        Self {
            volume_randomization: true,
            timing_randomization: true,
            order_splitting: true,
            venue_rotation: true,
            pattern_breaking: true,
            entropy_target: 0.85,
            max_slippage_bps: 30,
        }
    }
}

#[derive(Debug)]
pub struct PatternMetrics {
    pub entropy: f64,
    pub periodicity_score: f64,
    pub volume_consistency: f64,
    pub timing_regularity: f64,
    pub directional_bias: f64,
}

pub struct TradePatternNoiseInjector {
    config: NoiseConfig,
    pattern_history: Arc<RwLock<VecDeque<TradePattern>>>,
    rng: ChaCha20Rng,
    venue_weights: HashMap<String, f64>,
    pattern_metrics: Arc<RwLock<PatternMetrics>>,
    last_trade_time: Arc<RwLock<Instant>>,
    volume_distribution: Arc<RwLock<Vec<f64>>>,
}

impl TradePatternNoiseInjector {
    pub fn new(config: NoiseConfig) -> Self {
        let mut venue_weights = HashMap::new();
        venue_weights.insert("raydium".to_string(), 0.35);
        venue_weights.insert("orca".to_string(), 0.25);
        venue_weights.insert("serum".to_string(), 0.20);
        venue_weights.insert("jupiter".to_string(), 0.20);

        Self {
            config,
            pattern_history: Arc::new(RwLock::new(VecDeque::with_capacity(MAX_PATTERN_HISTORY))),
            rng: ChaCha20Rng::from_entropy(),
            venue_weights,
            pattern_metrics: Arc::new(RwLock::new(PatternMetrics {
                entropy: 1.0,
                periodicity_score: 0.0,
                volume_consistency: 0.0,
                timing_regularity: 0.0,
                directional_bias: 0.5,
            })),
            last_trade_time: Arc::new(RwLock::new(Instant::now())),
            volume_distribution: Arc::new(RwLock::new(Vec::new())),
        }
    }

    pub fn inject_noise(&mut self, original_volume: u64, direction: TradeDirection) -> Vec<NoiseInjectedTrade> {
        self.update_pattern_metrics();
        
        let metrics = self.pattern_metrics.read().unwrap();
        let needs_noise = metrics.entropy < self.config.entropy_target
            || metrics.periodicity_score > NOISE_INJECTION_THRESHOLD
            || metrics.volume_consistency > NOISE_INJECTION_THRESHOLD
            || metrics.timing_regularity > NOISE_INJECTION_THRESHOLD;
        drop(metrics);

        if !needs_noise {
            return vec![NoiseInjectedTrade {
                volume: original_volume,
                direction,
                venue: self.select_venue(),
                delay_ms: 0,
                is_decoy: false,
            }];
        }

        let mut trades = Vec::new();

        // Volume randomization
        let volume = if self.config.volume_randomization {
            self.randomize_volume(original_volume)
        } else {
            original_volume
        };

        // Order splitting
        if self.config.order_splitting && volume > ORDER_SPLIT_THRESHOLD {
            let splits = self.split_order(volume);
            for (i, split_volume) in splits.iter().enumerate() {
                let delay = if self.config.timing_randomization {
                    self.generate_timing_delay(i)
                } else {
                    i as u64 * 100
                };

                let venue = if self.config.venue_rotation {
                    self.select_venue_with_rotation(i)
                } else {
                    self.select_venue()
                };

                trades.push(NoiseInjectedTrade {
                    volume: *split_volume,
                    direction,
                    venue,
                    delay_ms: delay,
                    is_decoy: false,
                });
            }
        } else {
            let delay = if self.config.timing_randomization {
                self.generate_timing_delay(0)
            } else {
                0
            };

            trades.push(NoiseInjectedTrade {
                volume,
                direction,
                venue: self.select_venue(),
                delay_ms: delay,
                is_decoy: false,
            });
        }

        // Pattern breaking with decoy trades
        if self.config.pattern_breaking && self.should_inject_decoy() {
            let decoy_trades = self.generate_decoy_trades(original_volume, direction);
            trades.extend(decoy_trades);
        }

        self.update_volume_distribution(volume);
        trades
    }

    fn update_pattern_metrics(&self) {
        let history = self.pattern_history.read().unwrap();
        if history.len() < 10 {
            return;
        }

        let patterns: Vec<TradePattern> = history.iter().cloned().collect();
        drop(history);

        let entropy = self.calculate_entropy(&patterns);
        let periodicity = self.calculate_periodicity(&patterns);
        let volume_consistency = self.calculate_volume_consistency(&patterns);
        let timing_regularity = self.calculate_timing_regularity(&patterns);
        let directional_bias = self.calculate_directional_bias(&patterns);

        let mut metrics = self.pattern_metrics.write().unwrap();
        metrics.entropy = entropy;
        metrics.periodicity_score = periodicity;
        metrics.volume_consistency = volume_consistency;
        metrics.timing_regularity = timing_regularity;
        metrics.directional_bias = directional_bias;
    }

    fn calculate_entropy(&self, patterns: &[TradePattern]) -> f64 {
        let mut volume_bins: HashMap<u64, usize> = HashMap::new();
        let mut time_bins: HashMap<u64, usize> = HashMap::new();

        for pattern in patterns {
            let volume_bin = pattern.volume / 1_000_000_000; // 1 SOL bins
            let time_bin = pattern.timestamp / 60; // 1 minute bins
            
            *volume_bins.entry(volume_bin).or_insert(0) += 1;
            *time_bins.entry(time_bin).or_insert(0) += 1;
        }

        let total = patterns.len() as f64;
        let volume_entropy = volume_bins.values()
            .map(|&count| {
                let p = count as f64 / total;
                -p * p.log2()
            })
            .sum::<f64>();

        let time_entropy = time_bins.values()
            .map(|&count| {
                let p = count as f64 / total;
                -p * p.log2()
            })
            .sum::<f64>();

        (volume_entropy + time_entropy) / 2.0
    }

    fn calculate_periodicity(&self, patterns: &[TradePattern]) -> f64 {
        if patterns.len() < 20 {
            return 0.0;
        }

        let time_diffs: Vec<u64> = patterns.windows(2)
            .map(|w| w[1].timestamp.saturating_sub(w[0].timestamp))
            .collect();

        let mean_diff = time_diffs.iter().sum::<u64>() as f64 / time_diffs.len() as f64;
        let variance = time_diffs.iter()
            .map(|&d| {
                let diff = d as f64 - mean_diff;
                diff * diff
            })
            .sum::<f64>() / time_diffs.len() as f64;

        let std_dev = variance.sqrt();
        let cv = if mean_diff > 0.0 { std_dev / mean_diff } else { 1.0 };
        
        1.0 - cv.min(1.0)
    }

    fn calculate_volume_consistency(&self, patterns: &[TradePattern]) -> f64 {
        let volumes: Vec<u64> = patterns.iter().map(|p| p.volume).collect();
        let mean_volume = volumes.iter().sum::<u64>() as f64 / volumes.len() as f64;
        
        let variance = volumes.iter()
            .map(|&v| {
                let diff = v as f64 - mean_volume;
                diff * diff
            })
            .sum::<f64>() / volumes.len() as f64;

        let std_dev = variance.sqrt();
        let cv = if mean_volume > 0.0 { std_dev / mean_volume } else { 1.0 };
        
        1.0 - cv.min(1.0)
    }

    fn calculate_timing_regularity(&self, patterns: &[TradePattern]) -> f64 {
        if patterns.len() < 10 {
            return 0.0;
        }

        let mut hour_counts = vec![0u32; 24];
        for pattern in patterns {
            let hour = (pattern.timestamp / 3600) % 24;
            hour_counts[hour as usize] += 1;
        }

        let total = patterns.len() as f64;
        let max_concentration = hour_counts.iter().max().unwrap_or(&0);
        
        *max_concentration as f64 / total
    }

    fn calculate_directional_bias(&self, patterns: &[TradePattern]) -> f64 {
        let buy_count = patterns.iter()
            .filter(|p| p.direction == TradeDirection::Buy)
            .count() as f64;
        
        buy_count / patterns.len() as f64
    }

    fn randomize_volume(&mut self, original_volume: u64) -> u64 {
        let noise_range = original_volume as f64 * VOLUME_NOISE_FACTOR;
        let noise = self.rng.gen_range(-noise_range..=noise_range);
        let noisy_volume = (original_volume as f64 + noise).max(1.0) as u64;
        
        // Round to avoid suspicious precision
        let round_factor = 10_u64.pow((noisy_volume.ilog10() / 2).max(1));
        (noisy_volume / round_factor) * round_factor
    }

    fn split_order(&mut self, volume: u64) -> Vec<u64> {
        let num_splits = self.rng.gen_range(2..=MAX_SPLIT_ORDERS.min((volume / ORDER_SPLIT_THRESHOLD) as usize));
        let mut splits = Vec::with_capacity(num_splits);
        let mut remaining = volume;

        for i in 0..num_splits - 1 {
            let max_split = remaining / (num_splits - i) as u64;
            let min_split = max_split / 3;
            let split = self.rng.gen_range(min_split..=max_split);
            splits.push(split);
            remaining -= split;
        }
        splits.push(remaining);

        // Shuffle to avoid predictable patterns
        let mut indices: Vec<usize> = (0..splits.len()).collect();
        for i in (1..indices.len()).rev() {
            let j = self.rng.gen_range(0..=i);
            indices.swap(i, j);
        }
        
        indices.into_iter().map(|i| splits[i]).collect()
    }

    fn generate_timing_delay(&mut self, index: usize) -> u64 {
        let base_delay = MIN_TRADE_INTERVAL_MS + (index as u64 * 200);
        let jitter = (MAX_TRADE_INTERVAL_MS - MIN_TRADE_INTERVAL_MS) as f64 * TIMING_JITTER_RANGE;
        let random_delay = self.rng.gen_range(0.0..jitter) as u64;
        
        base_delay + random_delay
    }

    fn select_venue(&mut self) -> String {
        let rand_val = self.rng.gen_range(0.0..1.0);
        let mut cumulative = 0.0;
        
        for (venue, weight) in &self.venue_weights {
            cumulative += weight;
            if rand_val <= cumulative {
                return venue.clone();
            }
        }
        
        "raydium".to_string()
    }

    fn select_venue_with_rotation(&mut self, index: usize) -> String {
        let venues: Vec<String> = self.venue_weights.keys().cloned().collect();
        let rotation_offset = self.rng.gen_range(0..venues.len());
        venues[(index + rotation_offset) % venues.len()].clone()
    }

    fn should_inject_decoy(&mut self) -> bool {
        let metrics = self.pattern_metrics.read().unwrap();
        let detection_risk = (1.0 - metrics.entropy) * 0.3
            + metrics.periodicity_score * 0.3
            + metrics.volume_consistency * 0.2
            + metrics.timing_regularity * 0.2;
        
        self.rng.gen_bool(detection_risk.min(0.8))
    }

    fn generate_decoy_trades(&mut self, original_volume: u64, direction: TradeDirection) -> Vec<NoiseInjectedTrade> {
        let num_decoys = self.rng.gen_range(1..=3);
        let mut decoys = Vec::with_capacity(num_decoys);
        
        for i in 0..num_decoys {
            let decoy_volume = original_volume / (10 * (i + 1) as u64);
            let opposite_direction = match direction {
                TradeDirection::Buy => TradeDirection::Sell,
                TradeDirection::Sell => TradeDirection::Buy,
            };
            
            let decoy_direction = if self.rng.gen_bool(0.3) {
                opposite_direction
            } else {
                direction
            };
            
            decoys.push(NoiseInjectedTrade {
                volume: self.randomize_volume(decoy_volume),
                direction: decoy_direction,
                venue: self.select_venue(),
                delay_ms: self.rng.gen_range(100..3000),
                is_decoy: true,
            });
        }
        
        decoys
    }

    fn update_volume_distribution(&self, volume: u64) {
        let mut distribution = self.volume_distribution.write().unwrap();
        distribution.push(volume as f64);
        
        if distribution.len() > MAX_PATTERN_HISTORY {
            distribution.remove(0);
        }
    }

    pub fn record_trade(&self, pattern: TradePattern) {
        let mut history = self.pattern_history.write().unwrap();
        history.push_back(pattern);
        
        if history.len() > MAX_PATTERN_HISTORY {
            history.pop_front();
        }
        
        *self.last_trade_time.write().unwrap() = Instant::now();
    }

    pub fn get_pattern_health(&self) -> PatternHealth {
        let metrics = self.pattern_metrics.read().unwrap();
        let entropy_score = metrics.entropy / self.config.entropy_target;
        let periodicity_health = 1.0 - metrics.periodicity_score;
        let volume_health = 1.0 - metrics.volume_consistency;
        let timing_health = 1.0 - metrics.timing_regularity;
        
        let overall_health = (entropy_score * 0.4 
            + periodicity_health * 0.2 
            + volume_health * 0.2 
            + timing_health * 0.2).min(1.0);
        
        PatternHealth {
            overall_score: overall_health,
            entropy_score,
            periodicity_health,
            volume_health,
            timing_health,
            requires_intervention: overall_health < 0.7,
        }
    }

    pub fn adjust_noise_parameters(&mut self, market_volatility: f64, detection_pressure: f64) {
        let base_entropy_target = 0.85;
        self.config.entropy_target = (base_entropy_target + detection_pressure * 0.1).min(0.95);
        
        self.config.volume_randomization = detection_pressure > 0.3;
        self.config.timing_randomization = detection_pressure > 0.2;
        self.config.order_splitting = detection_pressure > 0.4;
        self.config.pattern_breaking = detection_pressure > 0.5;
        
        let volatility_factor = (market_volatility * 100.0).min(50.0) as u16;
        self.config.max_slippage_bps = 30 + volatility_factor;
    }

    pub fn calculate_noise_cost(&self, trades: &[NoiseInjectedTrade]) -> NoiseCost {
        let total_volume: u64 = trades.iter().map(|t| t.volume).sum();
        let decoy_volume: u64 = trades.iter()
            .filter(|t| t.is_decoy)
            .map(|t| t.volume)
            .sum();
        
        let num_splits = trades.iter().filter(|t| !t.is_decoy).count();
        let split_cost_bps = (num_splits as u16).saturating_sub(1) * 2;
        
        let timing_cost_bps = trades.iter()
            .map(|t| (t.delay_ms / 1000) as u16)
            .sum::<u16>()
            .min(10);
        
        let decoy_cost_bps = ((decoy_volume as f64 / total_volume as f64) * 100.0) as u16;
        
        NoiseCost {
            total_cost_bps: split_cost_bps + timing_cost_bps + decoy_cost_bps,
            split_cost_bps,
            timing_cost_bps,
            decoy_cost_bps,
            estimated_slippage_bps: self.config.max_slippage_bps,
        }
    }

    pub fn generate_signature_hash(&mut self, trade: &NoiseInjectedTrade) -> u64 {
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        std::hash::Hash::hash(&trade.volume, &mut hasher);
        std::hash::Hash::hash(&trade.venue, &mut hasher);
        std::hash::Hash::hash(&(trade.direction as u8), &mut hasher);
        std::hash::Hash::hash(&SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs(), &mut hasher);
        std::hash::Hasher::finish(&hasher)
    }

    pub fn validate_noise_effectiveness(&self, window_minutes: u64) -> NoiseEffectiveness {
        let history = self.pattern_history.read().unwrap();
        let current_time = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();
        let window_start = current_time - (window_minutes * 60);
        
        let recent_patterns: Vec<&TradePattern> = history.iter()
            .filter(|p| p.timestamp >= window_start)
            .collect();
        
        if recent_patterns.is_empty() {
            return NoiseEffectiveness {
                pattern_disruption_score: 1.0,
                timing_randomness: 1.0,
                volume_dispersion: 1.0,
                venue_distribution: 1.0,
                overall_effectiveness: 1.0,
            };
        }
        
        let pattern_disruption = self.calculate_pattern_disruption(&recent_patterns);
        let timing_randomness = self.calculate_timing_randomness(&recent_patterns);
        let volume_dispersion = self.calculate_volume_dispersion(&recent_patterns);
        let venue_distribution = self.calculate_venue_distribution(&recent_patterns);
        
        NoiseEffectiveness {
            pattern_disruption_score: pattern_disruption,
            timing_randomness,
            volume_dispersion,
            venue_distribution,
            overall_effectiveness: (pattern_disruption * 0.3 
                + timing_randomness * 0.3 
                + volume_dispersion * 0.2 
                + venue_distribution * 0.2),
        }
    }

    fn calculate_pattern_disruption(&self, patterns: &[&TradePattern]) -> f64 {
        if patterns.len() < 3 {
            return 1.0;
        }
        
        let mut disruption_scores = Vec::new();
        for window in patterns.windows(3) {
            let vol_diff_1 = (window[1].volume as f64 - window[0].volume as f64).abs();
            let vol_diff_2 = (window[2].volume as f64 - window[1].volume as f64).abs();
            let avg_volume = (window[0].volume + window[1].volume + window[2].volume) as f64 / 3.0;
            
            let disruption = ((vol_diff_1 + vol_diff_2) / 2.0) / avg_volume.max(1.0);
            disruption_scores.push(disruption.min(1.0));
        }
        
        disruption_scores.iter().sum::<f64>() / disruption_scores.len() as f64
    }

    fn calculate_timing_randomness(&self, patterns: &[&TradePattern]) -> f64 {
        if patterns.len() < 2 {
            return 1.0;
        }
        
        let intervals: Vec<u64> = patterns.windows(2)
            .map(|w| w[1].timestamp.saturating_sub(w[0].timestamp))
            .collect();
        
        let mean_interval = intervals.iter().sum::<u64>() as f64 / intervals.len() as f64;
        let variance = intervals.iter()
            .map(|&i| {
                let diff = i as f64 - mean_interval;
                diff * diff
            })
            .sum::<f64>() / intervals.len() as f64;
        
        let cv = if mean_interval > 0.0 {
            (variance.sqrt() / mean_interval).min(1.0)
        } else {
            1.0
        };
        
        cv
    }

    fn calculate_volume_dispersion(&self, patterns: &[&TradePattern]) -> f64 {
        let volumes: Vec<u64> = patterns.iter().map(|p| p.volume).collect();
        let mean_volume = volumes.iter().sum::<u64>() as f64 / volumes.len() as f64;
        
        let variance = volumes.iter()
            .map(|&v| {
                let diff = v as f64 - mean_volume;
                diff * diff
            })
            .sum::<f64>() / volumes.len() as f64;
        
        let cv = if mean_volume > 0.0 {
            (variance.sqrt() / mean_volume).min(1.0)
        } else {
            1.0
        };
        
        cv
    }

    fn calculate_venue_distribution(&self, patterns: &[&TradePattern]) -> f64 {
        let mut venue_counts: HashMap<&str, usize> = HashMap::new();
        for pattern in patterns {
            *venue_counts.entry(&pattern.venue).or_insert(0) += 1;
        }
        
        let total = patterns.len() as f64;
        let entropy: f64 = venue_counts.values()
            .map(|&count| {
                let p = count as f64 / total;
                -p * p.log2()
            })
            .sum();
        
        let max_entropy = (venue_counts.len() as f64).log2();
        if max_entropy > 0.0 {
            entropy / max_entropy
        } else {
            0.0
        }
    }

    pub fn optimize_for_market_conditions(&mut self, 
        liquidity: f64, 
        volatility: f64, 
        competition_level: f64) {
        
        // Adjust volume noise based on liquidity
        let volume_noise_adjustment = if liquidity < 0.3 {
            0.5 // Reduce noise in low liquidity
        } else if liquidity > 0.7 {
            1.2 // Increase noise in high liquidity
        } else {
            1.0
        };
        
        // Adjust timing based on volatility
        let timing_adjustment = if volatility > 0.7 {
            1.5 // More random timing in high volatility
        } else if volatility < 0.3 {
            0.8 // Less random timing in low volatility
        } else {
            1.0
        };
        
        // Adjust pattern breaking based on competition
        self.config.pattern_breaking = competition_level > 0.5;
        self.config.order_splitting = competition_level > 0.6 || liquidity > 0.5;
        
        // Update venue weights based on market conditions
        if competition_level > 0.7 {
            self.venue_weights.insert("raydium".to_string(), 0.25);
            self.venue_weights.insert("orca".to_string(), 0.25);
            self.venue_weights.insert("serum".to_string(), 0.25);
            self.venue_weights.insert("jupiter".to_string(), 0.25);
        }
    }
}

#[derive(Debug, Clone)]
pub struct NoiseInjectedTrade {
    pub volume: u64,
    pub direction: TradeDirection,
    pub venue: String,
    pub delay_ms: u64,
    pub is_decoy: bool,
}

#[derive(Debug, Clone)]
pub struct PatternHealth {
    pub overall_score: f64,
    pub entropy_score: f64,
    pub periodicity_health: f64,
    pub volume_health: f64,
    pub timing_health: f64,
    pub requires_intervention: bool,
}

#[derive(Debug, Clone)]
pub struct NoiseCost {
    pub total_cost_bps: u16,
    pub split_cost_bps: u16,
    pub timing_cost_bps: u16,
    pub decoy_cost_bps: u16,
    pub estimated_slippage_bps: u16,
}

#[derive(Debug, Clone)]
pub struct NoiseEffectiveness {
    pub pattern_disruption_score: f64,
    pub timing_randomness: f64,
    pub volume_dispersion: f64,
    pub venue_distribution: f64,
    pub overall_effectiveness: f64,
}

impl TradePatternNoiseInjector {
    pub fn emergency_pattern_break(&mut self) -> Vec<NoiseInjectedTrade> {
        // Emergency intervention when pattern detection risk is critical
        let mut emergency_trades = Vec::new();
        let num_trades = self.rng.gen_range(5..=10);
        
        for i in 0..num_trades {
            let volume = self.rng.gen_range(1_000_000..50_000_000); // 0.001 to 0.05 SOL
            let direction = if self.rng.gen_bool(0.5) {
                TradeDirection::Buy
            } else {
                TradeDirection::Sell
            };
            
            emergency_trades.push(NoiseInjectedTrade {
                volume: self.randomize_volume(volume),
                direction,
                venue: self.select_venue_with_rotation(i),
                delay_ms: self.rng.gen_range(50..2000),
                is_decoy: true,
            });
        }
        
        emergency_trades
    }

    pub fn calculate_optimal_noise_level(&self, 
        current_pnl: f64, 
        detection_risk: f64, 
        market_impact: f64) -> f64 {
        
        let pnl_factor = if current_pnl > 0.0 {
            1.0 + (current_pnl.min(0.1) * 2.0) // More noise when profitable
        } else {
            1.0 - (current_pnl.abs().min(0.05) * 2.0) // Less noise when losing
        };
        
        let risk_factor = 1.0 + (detection_risk * 3.0);
        let impact_factor = 1.0 - (market_impact.min(0.3) * 0.5);
        
        (pnl_factor * risk_factor * impact_factor).max(0.5).min(2.0)
    }

    pub fn get_next_trade_timing(&mut self, base_delay: u64) -> u64 {
        let metrics = self.pattern_metrics.read().unwrap();
        let timing_risk = metrics.timing_regularity;
        drop(metrics);
        
        if timing_risk > 0.8 {
            // High risk - maximum randomization
            let factor = self.rng.gen_range(0.1..3.0);
            (base_delay as f64 * factor) as u64
        } else if timing_risk > 0.5 {
            // Medium risk - moderate randomization
            let factor = self.rng.gen_range(0.5..1.5);
            (base_delay as f64 * factor) as u64
        } else {
            // Low risk - minimal randomization
            let jitter = base_delay / 10;
            base_delay + self.rng.gen_range(0..jitter)
        }
    }

    pub fn should_skip_trade(&mut self, consecutive_trades: usize) -> bool {
        let metrics = self.pattern_metrics.read().unwrap();
        let pattern_risk = metrics.periodicity_score;
        drop(metrics);
        
        let skip_probability = match consecutive_trades {
            0..=2 => 0.0,
            3..=5 => 0.1 + pattern_risk * 0.1,
            6..=8 => 0.2 + pattern_risk * 0.2,
            _ => 0.4 + pattern_risk * 0.3,
        };
        
        self.rng.gen_bool(skip_probability.min(0.7))
    }

    pub fn generate_anti_pattern_sequence(&mut self, 
        expected_volume: u64, 
        expected_direction: TradeDirection) -> Vec<NoiseInjectedTrade> {
        
        let mut sequence = Vec::new();
        
        // Generate opposite pattern
        let opposite_dir = match expected_direction {
            TradeDirection::Buy => TradeDirection::Sell,
            TradeDirection::Sell => TradeDirection::Buy,
        };
        
        // Create misleading volume pattern
        let volumes = vec![
            expected_volume * 2,
            expected_volume / 3,
            expected_volume * 5 / 4,
            expected_volume / 2,
        ];
        
        for (i, &vol) in volumes.iter().enumerate() {
            let direction = if i % 2 == 0 { opposite_dir } else { expected_direction };
            
            sequence.push(NoiseInjectedTrade {
                volume: self.randomize_volume(vol),
                direction,
                venue: self.select_venue_with_rotation(i),
                delay_ms: self.rng.gen_range(100..1500),
                is_decoy: true,
            });
        }
        
        sequence
    }

    pub fn adapt_to_competitor_patterns(&mut self, competitor_patterns: &[TradePattern]) {
        if competitor_patterns.is_empty() {
            return;
        }
        
        // Analyze competitor timing
        let competitor_intervals: Vec<u64> = competitor_patterns.windows(2)
            .map(|w| w[1].timestamp.saturating_sub(w[0].timestamp))
            .collect();
        
        if !competitor_intervals.is_empty() {
            let avg_interval = competitor_intervals.iter().sum::<u64>() / competitor_intervals.len() as u64;
            
            // Set our timing to be deliberately different
            let our_base_interval = if avg_interval < 1000 {
                avg_interval * 3
            } else {
                avg_interval / 2
            };
            
            // Adjust our timing parameters
            self.config.timing_randomization = true;
        }
        
        // Analyze competitor volumes and do opposite
        let avg_competitor_volume = competitor_patterns.iter()
            .map(|p| p.volume)
            .sum::<u64>() / competitor_patterns.len() as u64;
        
        // Update our volume distribution to be different
        let mut distribution = self.volume_distribution.write().unwrap();
        distribution.clear();
        distribution.push((avg_competitor_volume as f64 * 0.3).max(1_000_000.0));
        distribution.push((avg_competitor_volume as f64 * 2.5).min(1_000_000_000.0));
    }

    pub fn finalize_trade_batch(&mut self, trades: &mut Vec<NoiseInjectedTrade>) {
        // Final adjustments to ensure pattern obfuscation
        
        // Shuffle trade order while maintaining relative delays
        let mut indices: Vec<usize> = (0..trades.len()).collect();
        for i in (1..indices.len()).rev() {
            let j = self.rng.gen_range(0..=i);
            indices.swap(i, j);
        }
        
        let original_trades = trades.clone();
        for (new_idx, &old_idx) in indices.iter().enumerate() {
            trades[new_idx] = original_trades[old_idx].clone();
        }
        
        // Adjust delays to maintain execution order if needed
        let mut cumulative_delay = 0u64;
        for trade in trades.iter_mut() {
            trade.delay_ms = cumulative_delay + trade.delay_ms;
            cumulative_delay = trade.delay_ms;
        }
        
        // Final venue rotation check
        let venue_counts: HashMap<String, usize> = trades.iter()
            .fold(HashMap::new(), |mut map, trade| {
                *map.entry(trade.venue.clone()).or_insert(0) += 1;
                map
            });
        
        // Rebalance if any venue is overrepresented
        let max_per_venue = (trades.len() as f64 * 0.4).ceil() as usize;
        for (venue, count) in venue_counts {
            if count > max_per_venue {
                let excess = count - max_per_venue;
                let mut changed = 0;
                for trade in trades.iter_mut() {
                    if changed >= excess {
                        break;
                    }
                    if trade.venue == venue {
                        trade.venue = self.select_venue();
                        changed += 1;
                    }
                }
            }
        }
    }

    pub fn get_pattern_signature(&self) -> PatternSignature {
        let history = self.pattern_history.read().unwrap();
        let metrics = self.pattern_metrics.read().unwrap();
        
        PatternSignature {
            entropy: metrics.entropy,
            periodicity: metrics.periodicity_score,
            volume_consistency: metrics.volume_consistency,
            timing_regularity: metrics.timing_regularity,
            trade_count: history.len(),
            timestamp: SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs(),
        }
    }

    pub fn reset_pattern_history(&mut self) {
        self.pattern_history.write().unwrap().clear();
        self.volume_distribution.write().unwrap().clear();
        *self.last_trade_time.write().unwrap() = Instant::now();
        
        // Reset metrics
        let mut metrics = self.pattern_metrics.write().unwrap();
        metrics.entropy = 1.0;
        metrics.periodicity_score = 0.0;
        metrics.volume_consistency = 0.0;
        metrics.timing_regularity = 0.0;
        metrics.directional_bias = 0.5;
    }
}

#[derive(Debug, Clone)]
pub struct PatternSignature {
    pub entropy: f64,
    pub periodicity: f64,
    pub volume_consistency: f64,
    pub timing_regularity: f64,
    pub trade_count: usize,
    pub timestamp: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_noise_injection() {
        let config = NoiseConfig::default();
        let mut injector = TradePatternNoiseInjector::new(config);
        
        let trades = injector.inject_noise(100_000_000, TradeDirection::Buy);
        assert!(!trades.is_empty());
        assert!(trades.iter().all(|t| t.volume > 0));
    }

    #[test]
    fn test_pattern_metrics() {
        let config = NoiseConfig::default();
        let injector = TradePatternNoiseInjector::new(config);
        
        for i in 0..20 {
            injector.record_trade(TradePattern {
                timestamp: i * 60,
                volume: 50_000_000,
                direction: TradeDirection::Buy,
                price_impact: 0.001,
                venue: "raydium".to_string(),
                signature_hash: i,
            });
        }
        
        let health = injector.get_pattern_health();
        assert!(health.overall_score < 0.8); // Should detect pattern
    }

    #[test]
    fn test_emergency_break() {
        let config = NoiseConfig::default();
        let mut injector = TradePatternNoiseInjector::new(config);
        
        let emergency_trades = injector.emergency_pattern_break();
        assert!(emergency_trades.len() >= 5);
        assert!(emergency_trades.iter().all(|t| t.is_decoy));
    }
}
