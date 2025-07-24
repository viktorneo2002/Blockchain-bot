use std::collections::VecDeque;
use std::sync::{Arc, RwLock};
use std::f64;
use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Error, Debug)]
pub enum VpinError {
    #[error("Insufficient data for VPIN calculation")]
    InsufficientData,
    #[error("Invalid volume bucket size")]
    InvalidBucketSize,
    #[error("Calculation overflow")]
    CalculationOverflow,
    #[error("Lock poisoned")]
    LockPoisoned,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TradeData {
    pub price: f64,
    pub volume: f64,
    pub timestamp: u64,
    pub is_buy: bool,
    pub slot: u64,
}

#[derive(Debug, Clone)]
struct VolumeBucket {
    buy_volume: f64,
    sell_volume: f64,
    total_volume: f64,
    start_time: u64,
    end_time: u64,
    price_sum: f64,
    trade_count: u32,
}

impl VolumeBucket {
    fn new(timestamp: u64) -> Self {
        Self {
            buy_volume: 0.0,
            sell_volume: 0.0,
            total_volume: 0.0,
            start_time: timestamp,
            end_time: timestamp,
            price_sum: 0.0,
            trade_count: 0,
        }
    }

    fn add_trade(&mut self, trade: &TradeData) {
        if trade.is_buy {
            self.buy_volume += trade.volume;
        } else {
            self.sell_volume += trade.volume;
        }
        self.total_volume += trade.volume;
        self.price_sum += trade.price * trade.volume;
        self.trade_count += 1;
        self.end_time = trade.timestamp;
    }

    fn vwap(&self) -> f64 {
        if self.total_volume > 0.0 {
            self.price_sum / self.total_volume
        } else {
            0.0
        }
    }

    fn order_imbalance(&self) -> f64 {
        (self.buy_volume - self.sell_volume).abs()
    }
}

pub struct VpinCalculator {
    bucket_size: f64,
    window_size: usize,
    buckets: Arc<RwLock<VecDeque<VolumeBucket>>>,
    current_bucket: Arc<RwLock<Option<VolumeBucket>>>,
    vpin_cache: Arc<RwLock<Option<f64>>>,
    total_volume_processed: Arc<RwLock<f64>>,
    min_buckets_for_calculation: usize,
    decay_factor: f64,
    volume_threshold: f64,
}

impl VpinCalculator {
    pub fn new(bucket_size: f64, window_size: usize) -> Result<Self, VpinError> {
        if bucket_size <= 0.0 {
            return Err(VpinError::InvalidBucketSize);
        }
        
        Ok(Self {
            bucket_size,
            window_size,
            buckets: Arc::new(RwLock::new(VecDeque::with_capacity(window_size + 1))),
            current_bucket: Arc::new(RwLock::new(None)),
            vpin_cache: Arc::new(RwLock::new(None)),
            total_volume_processed: Arc::new(RwLock::new(0.0)),
            min_buckets_for_calculation: (window_size as f64 * 0.7) as usize,
            decay_factor: 0.94,
            volume_threshold: bucket_size * 0.001,
        })
    }

    pub fn add_trade(&self, trade: TradeData) -> Result<(), VpinError> {
        let mut current_bucket = self.current_bucket.write()
            .map_err(|_| VpinError::LockPoisoned)?;
        
        let mut buckets = self.buckets.write()
            .map_err(|_| VpinError::LockPoisoned)?;
        
        let mut total_volume = self.total_volume_processed.write()
            .map_err(|_| VpinError::LockPoisoned)?;

        if trade.volume < self.volume_threshold {
            return Ok(());
        }

        let mut remaining_volume = trade.volume;
        
        while remaining_volume > 0.0 {
            let bucket = current_bucket.get_or_insert_with(|| VolumeBucket::new(trade.timestamp));
            
            let space_in_bucket = self.bucket_size - bucket.total_volume;
            let volume_to_add = remaining_volume.min(space_in_bucket);
            
            let partial_trade = TradeData {
                price: trade.price,
                volume: volume_to_add,
                timestamp: trade.timestamp,
                is_buy: trade.is_buy,
                slot: trade.slot,
            };
            
            bucket.add_trade(&partial_trade);
            remaining_volume -= volume_to_add;
            *total_volume += volume_to_add;
            
            if bucket.total_volume >= self.bucket_size * 0.99 {
                if let Some(completed_bucket) = current_bucket.take() {
                    buckets.push_back(completed_bucket);
                    
                    if buckets.len() > self.window_size {
                        buckets.pop_front();
                    }
                    
                    self.invalidate_cache();
                }
            }
        }
        
        Ok(())
    }

    pub fn calculate_vpin(&self) -> Result<f64, VpinError> {
        let cache = self.vpin_cache.read()
            .map_err(|_| VpinError::LockPoisoned)?;
        
        if let Some(cached_value) = *cache {
            return Ok(cached_value);
        }
        
        drop(cache);
        
        let buckets = self.buckets.read()
            .map_err(|_| VpinError::LockPoisoned)?;
        
        if buckets.len() < self.min_buckets_for_calculation {
            return Err(VpinError::InsufficientData);
        }
        
        let n = buckets.len().min(self.window_size);
        let mut order_imbalances = Vec::with_capacity(n);
        let mut weights = Vec::with_capacity(n);
        
        for (i, bucket) in buckets.iter().rev().take(n).enumerate() {
            let imbalance = bucket.order_imbalance();
            order_imbalances.push(imbalance);
            weights.push(self.decay_factor.powi(i as i32));
        }
        
        let total_weight: f64 = weights.iter().sum();
        let weighted_sum: f64 = order_imbalances.iter()
            .zip(weights.iter())
            .map(|(imb, w)| imb * w)
            .sum();
        
        let weighted_mean = weighted_sum / total_weight;
        
        let variance: f64 = order_imbalances.iter()
            .zip(weights.iter())
            .map(|(imb, w)| {
                let diff = imb - weighted_mean;
                diff * diff * w
            })
            .sum::<f64>() / total_weight;
        
        let std_dev = variance.sqrt();
        let total_bucket_volume = n as f64 * self.bucket_size;
        
        let vpin = if total_bucket_volume > 0.0 && std_dev.is_finite() {
            (weighted_mean / total_bucket_volume).min(1.0).max(0.0)
        } else {
            0.0
        };
        
        let mut cache = self.vpin_cache.write()
            .map_err(|_| VpinError::LockPoisoned)?;
        *cache = Some(vpin);
        
        Ok(vpin)
    }

    pub fn get_toxicity_score(&self) -> Result<f64, VpinError> {
        let vpin = self.calculate_vpin()?;
        let buckets = self.buckets.read()
            .map_err(|_| VpinError::LockPoisoned)?;
        
        if buckets.is_empty() {
            return Ok(0.0);
        }
        
        let recent_buckets = buckets.iter().rev().take(5).collect::<Vec<_>>();
        if recent_buckets.is_empty() {
            return Ok(vpin);
        }
        
        let mut price_volatility = 0.0;
        let mut prev_vwap = recent_buckets[0].vwap();
        
        for bucket in recent_buckets.iter().skip(1) {
            let current_vwap = bucket.vwap();
            if prev_vwap > 0.0 {
                let return_val = (current_vwap - prev_vwap).abs() / prev_vwap;
                price_volatility += return_val;
            }
            prev_vwap = current_vwap;
        }
        
        price_volatility /= recent_buckets.len() as f64;
        
        let volume_concentration = recent_buckets.iter()
            .map(|b| {
                let buy_ratio = b.buy_volume / b.total_volume.max(1.0);
                (buy_ratio - 0.5).abs() * 2.0
            })
            .sum::<f64>() / recent_buckets.len() as f64;
        
        let toxicity = (0.5 * vpin + 0.3 * price_volatility + 0.2 * volume_concentration)
            .min(1.0)
            .max(0.0);
        
        Ok(toxicity)
    }

    pub fn get_directional_pressure(&self) -> Result<f64, VpinError> {
        let buckets = self.buckets.read()
            .map_err(|_| VpinError::LockPoisoned)?;
        
        if buckets.len() < 3 {
            return Ok(0.0);
        }
        
        let recent_buckets = buckets.iter().rev().take(10).collect::<Vec<_>>();
        
        let mut weighted_pressure = 0.0;
        let mut total_weight = 0.0;
        
        for (i, bucket) in recent_buckets.iter().enumerate() {
            let weight = 1.0 / (1.0 + i as f64 * 0.1);
            let pressure = (bucket.buy_volume - bucket.sell_volume) / bucket.total_volume.max(1.0);
            weighted_pressure += pressure * weight;
            total_weight += weight;
        }
        
        Ok((weighted_pressure / total_weight).max(-1.0).min(1.0))
    }

    pub fn get_volume_profile(&self) -> Result<VolumeProfile, VpinError> {
        let buckets = self.buckets.read()
            .map_err(|_| VpinError::LockPoisoned)?;
        
        let total_volume = self.total_volume_processed.read()
            .map_err(|_| VpinError::LockPoisoned)?;
        
        if buckets.is_empty() {
            return Ok(VolumeProfile::default());
        }
        
        let mut buy_volume = 0.0;
        let mut sell_volume = 0.0;
        let mut price_levels = Vec::new();
        
        for bucket in buckets.iter() {
            buy_volume += bucket.buy_volume;
            sell_volume += bucket.sell_volume;
            price_levels.push(bucket.vwap());
        }
        
        price_levels.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        
        let median_price = if !price_levels.is_empty() {
            price_levels[price_levels.len() / 2]
        } else {
            0.0
        };
        
        Ok(VolumeProfile {
            total_volume: *total_volume,
            buy_volume,
            sell_volume,
            median_price,
            bucket_count: buckets.len(),
        })
    }

    pub fn get_microstructure_metrics(&self) -> Result<MicrostructureMetrics, VpinError> {
        let vpin = self.calculate_vpin()?;
        let toxicity = self.get_toxicity_score()?;
        let pressure = self.get_directional_pressure()?;
        
        let buckets = self.buckets.read()
            .map_err(|_| VpinError::LockPoisoned)?;
        
        let kyle_lambda = if buckets.len() >= 2 {
            let recent = buckets.iter().rev().take(5).collect::<Vec<_>>();
            let mut price_impact = 0.0;
            
            for i in 1..recent.len() {
                let price_change = (recent[i-1].vwap() - recent[i].vwap()).abs();
                let volume = recent[i-1].total_volume;
                if volume > 0.0 {
                    price_impact += price_change / volume.sqrt();
                }
            }
            
            price_impact / (recent.len() - 1) as f64
        } else {
            0.0
        };
        
        let realized_spread = buckets.iter().rev().take(10)
            .map(|b| {
                let midpoint = b.vwap();
                let spread = b.order_imbalance() / b.total_volume.max(1.0) * midpoint * 0.001;
                spread
            })
            .sum::<f64>() / 10.0;
        
        Ok(MicrostructureMetrics {
            vpin,
            toxicity,
            directional_pressure: pressure,
            kyle_lambda,
            realized_spread,
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_millis() as u64)
                .unwrap_or(0),
        })
    }

        fn invalidate_cache(&self) {
        if let Ok(mut cache) = self.vpin_cache.write() {
            *cache = None;
        }
    }

    pub fn reset(&self) -> Result<(), VpinError> {
        let mut buckets = self.buckets.write()
            .map_err(|_| VpinError::LockPoisoned)?;
        let mut current_bucket = self.current_bucket.write()
            .map_err(|_| VpinError::LockPoisoned)?;
        let mut total_volume = self.total_volume_processed.write()
            .map_err(|_| VpinError::LockPoisoned)?;
        
        buckets.clear();
        *current_bucket = None;
        *total_volume = 0.0;
        self.invalidate_cache();
        
        Ok(())
    }

    pub fn get_bucket_stats(&self) -> Result<BucketStats, VpinError> {
        let buckets = self.buckets.read()
            .map_err(|_| VpinError::LockPoisoned)?;
        let current_bucket = self.current_bucket.read()
            .map_err(|_| VpinError::LockPoisoned)?;
        
        let completed_buckets = buckets.len();
        let current_bucket_volume = current_bucket.as_ref()
            .map(|b| b.total_volume)
            .unwrap_or(0.0);
        
        let avg_bucket_time = if buckets.len() > 1 {
            let total_time = buckets.iter()
                .map(|b| b.end_time - b.start_time)
                .sum::<u64>();
            total_time / buckets.len() as u64
        } else {
            0
        };
        
        Ok(BucketStats {
            completed_buckets,
            current_bucket_fill: current_bucket_volume / self.bucket_size,
            average_bucket_duration_ms: avg_bucket_time,
            bucket_size: self.bucket_size,
        })
    }

    pub fn get_flow_imbalance(&self) -> Result<FlowImbalance, VpinError> {
        let buckets = self.buckets.read()
            .map_err(|_| VpinError::LockPoisoned)?;
        
        if buckets.is_empty() {
            return Ok(FlowImbalance::default());
        }
        
        let window = buckets.iter().rev().take(20).collect::<Vec<_>>();
        
        let mut buy_momentum = 0.0;
        let mut sell_momentum = 0.0;
        let mut total_weight = 0.0;
        
        for (i, bucket) in window.iter().enumerate() {
            let weight = (1.0 - i as f64 / 20.0).powi(2);
            buy_momentum += bucket.buy_volume * weight;
            sell_momentum += bucket.sell_volume * weight;
            total_weight += weight;
        }
        
        let normalized_buy = buy_momentum / total_weight;
        let normalized_sell = sell_momentum / total_weight;
        let total_flow = normalized_buy + normalized_sell;
        
        let imbalance_ratio = if total_flow > 0.0 {
            (normalized_buy - normalized_sell) / total_flow
        } else {
            0.0
        };
        
        let flow_concentration = window.iter()
            .map(|b| {
                let total = b.buy_volume + b.sell_volume;
                if total > 0.0 {
                    let buy_ratio = b.buy_volume / total;
                    let sell_ratio = b.sell_volume / total;
                    -(buy_ratio * buy_ratio.ln() + sell_ratio * sell_ratio.ln())
                } else {
                    0.0
                }
            })
            .sum::<f64>() / window.len() as f64;
        
        Ok(FlowImbalance {
            buy_pressure: normalized_buy,
            sell_pressure: normalized_sell,
            imbalance_ratio,
            flow_entropy: flow_concentration,
            confidence: (window.len() as f64 / 20.0).min(1.0),
        })
    }

    pub fn get_adaptive_threshold(&self) -> Result<f64, VpinError> {
        let metrics = self.get_microstructure_metrics()?;
        let flow = self.get_flow_imbalance()?;
        
        let base_threshold = 0.4;
        let volatility_adjustment = metrics.kyle_lambda.min(0.3);
        let flow_adjustment = flow.imbalance_ratio.abs() * 0.2;
        let toxicity_penalty = metrics.toxicity * 0.3;
        
        Ok((base_threshold + volatility_adjustment + flow_adjustment + toxicity_penalty)
            .min(0.8)
            .max(0.2))
    }

    pub fn should_execute_trade(&self, is_buy: bool) -> Result<TradeDecision, VpinError> {
        let metrics = self.get_microstructure_metrics()?;
        let flow = self.get_flow_imbalance()?;
        let threshold = self.get_adaptive_threshold()?;
        
        let directional_edge = if is_buy {
            -flow.imbalance_ratio * 0.5
        } else {
            flow.imbalance_ratio * 0.5
        };
        
        let toxicity_penalty = metrics.toxicity * 0.7;
        let volatility_bonus = (metrics.kyle_lambda * 10.0).min(0.2);
        
        let signal_strength = (directional_edge + volatility_bonus - toxicity_penalty + 0.5)
            .max(0.0)
            .min(1.0);
        
        let should_trade = signal_strength > threshold && metrics.toxicity < 0.7;
        
        let size_multiplier = if should_trade {
            let base_size = 1.0 - metrics.toxicity;
            let confidence_adj = flow.confidence;
            let volatility_adj = 1.0 / (1.0 + metrics.kyle_lambda * 5.0);
            
            (base_size * confidence_adj * volatility_adj).max(0.1).min(1.0)
        } else {
            0.0
        };
        
        Ok(TradeDecision {
            should_trade,
            confidence: signal_strength,
            size_multiplier,
            risk_score: metrics.toxicity,
            expected_edge: if should_trade { 
                (signal_strength - threshold) * (1.0 - metrics.toxicity) 
            } else { 
                0.0 
            },
        })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VolumeProfile {
    pub total_volume: f64,
    pub buy_volume: f64,
    pub sell_volume: f64,
    pub median_price: f64,
    pub bucket_count: usize,
}

impl Default for VolumeProfile {
    fn default() -> Self {
        Self {
            total_volume: 0.0,
            buy_volume: 0.0,
            sell_volume: 0.0,
            median_price: 0.0,
            bucket_count: 0,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MicrostructureMetrics {
    pub vpin: f64,
    pub toxicity: f64,
    pub directional_pressure: f64,
    pub kyle_lambda: f64,
    pub realized_spread: f64,
    pub timestamp: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BucketStats {
    pub completed_buckets: usize,
    pub current_bucket_fill: f64,
    pub average_bucket_duration_ms: u64,
    pub bucket_size: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FlowImbalance {
    pub buy_pressure: f64,
    pub sell_pressure: f64,
    pub imbalance_ratio: f64,
    pub flow_entropy: f64,
    pub confidence: f64,
}

impl Default for FlowImbalance {
    fn default() -> Self {
        Self {
            buy_pressure: 0.0,
            sell_pressure: 0.0,
            imbalance_ratio: 0.0,
            flow_entropy: 0.0,
            confidence: 0.0,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TradeDecision {
    pub should_trade: bool,
    pub confidence: f64,
    pub size_multiplier: f64,
    pub risk_score: f64,
    pub expected_edge: f64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_vpin_calculator_initialization() {
        let calculator = VpinCalculator::new(1000.0, 50).unwrap();
        assert!(calculator.calculate_vpin().is_err());
    }

    #[test]
    fn test_trade_processing() {
        let calculator = VpinCalculator::new(100.0, 10).unwrap();
        
        for i in 0..100 {
            let trade = TradeData {
                price: 100.0 + (i as f64 * 0.1),
                volume: 25.0,
                timestamp: 1000 + i,
                is_buy: i % 2 == 0,
                slot: i,
            };
            calculator.add_trade(trade).unwrap();
        }
        
        let vpin = calculator.calculate_vpin().unwrap();
        assert!(vpin >= 0.0 && vpin <= 1.0);
    }

    #[test]
    fn test_toxicity_calculation() {
        let calculator = VpinCalculator::new(500.0, 20).unwrap();
        
        for i in 0..200 {
            let trade = TradeData {
                price: 100.0 + ((i as f64).sin() * 5.0),
                volume: 50.0,
                timestamp: 1000 + i * 10,
                is_buy: i % 3 != 0,
                slot: i,
            };
            calculator.add_trade(trade).unwrap();
        }
        
        let toxicity = calculator.get_toxicity_score().unwrap();
        assert!(toxicity >= 0.0 && toxicity <= 1.0);
    }

    #[test]
    fn test_trade_decision() {
        let calculator = VpinCalculator::new(1000.0, 30).unwrap();
        
        for i in 0..300 {
            let trade = TradeData {
                price: 100.0 + (i as f64 * 0.05),
                volume: 100.0,
                timestamp: 1000 + i * 5,
                is_buy: i % 5 < 3,
                slot: i,
            };
            calculator.add_trade(trade).unwrap();
        }
        
        let decision = calculator.should_execute_trade(true).unwrap();
        assert!(decision.confidence >= 0.0 && decision.confidence <= 1.0);
        assert!(decision.size_multiplier >= 0.0 && decision.size_multiplier <= 1.0);
    }
}

