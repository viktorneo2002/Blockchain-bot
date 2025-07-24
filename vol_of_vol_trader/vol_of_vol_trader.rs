//! Production-ready volatility-of-volatility arbitrage engine for Solana MEV bot
//! 
//! This module detects extreme market dislocations in implied volatility dynamics
//! across perpetual markets, DEX liquidity pools, and oracle feeds.
//! 
//! Key features:
//! - Sliding window volatility tracking with ring buffer
//! - Welford's algorithm for realized volatility computation
//! - Second-order volatility measurement (vol-of-vol)
//! - Anomaly detection for volatility regime changes
//! - Signal generation for trading pipeline integration

#![deny(unsafe_code)]
#![deny(warnings)]
#![allow(dead_code)]

use std::collections::VecDeque;
use std::time::{SystemTime, UNIX_EPOCH};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use blake3::Hasher;
use rust_decimal::Decimal;

/// Maximum number of volatility snapshots to retain in memory
const MAX_VOLATILITY_HISTORY: usize = 1440; // 24 hours at 1-minute intervals

/// Minimum number of observations required for volatility computation
const MIN_OBSERVATIONS: usize = 10;

/// Default volatility threshold multiplier for anomaly detection
const DEFAULT_VOL_THRESHOLD_MULTIPLIER: f64 = 2.0;

/// Slot drift tolerance for stale data detection
const MAX_SLOT_DRIFT: u64 = 10;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VolatilitySnapshot {
    pub slot: u64,
    pub timestamp: u64,
    pub price: Decimal,
    pub lp_fee_rate: Decimal,
    pub perp_iv: Decimal,
    pub oracle_price: Decimal,
    pub source: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum AnomalyType {
    LpFeeSpike,
    PerpIvCompression,
    OracleVolDivergence,
    RegimeChange,
    VolSpike,
    VolCompression,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VolatilityAnomaly {
    pub anomaly_type: AnomalyType,
    pub severity: f64,
    pub detected_at: u64,
    pub slot: u64,
    pub description: String,
    pub fingerprint: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VolOfVolSignal {
    pub timestamp: u64,
    pub slot: u64,
    pub source: String,
    pub realized_vol: f64,
    pub vol_of_vol: f64,
    pub anomaly: Option<VolatilityAnomaly>,
    pub confidence: f64,
    pub signal_strength: f64,
}

#[derive(Debug, Clone)]
struct VolatilityStats {
    mean: f64,
    variance: f64,
    count: usize,
    min: f64,
    max: f64,
}

impl VolatilityStats {
    fn new() -> Self {
        Self {
            mean: 0.0,
            variance: 0.0,
            count: 0,
            min: f64::MAX,
            max: f64::MIN,
        }
    }
}

#[derive(Debug, Error)]
pub enum VolTraderError {
    #[error("Insufficient data: need at least {min} observations, got {actual}")]
    InsufficientData { min: usize, actual: usize },
    
    #[error("Invalid price data: {reason}")]
    InvalidPrice { reason: String },
    
    #[error("Stale data detected: slot {slot} is {drift} slots behind")]
    StaleData { slot: u64, drift: u64 },
    
    #[error("Oracle feed error: {message}")]
    OracleFeed { message: String },
    
    #[error("Computation error: {details}")]
    Computation { details: String },
}

pub type Result<T> = std::result::Result<T, VolTraderError>;

pub struct VolOfVolTrader {
    volatility_history: VecDeque<VolatilitySnapshot>,
    realized_vol_window: VecDeque<f64>,
    vol_of_vol_stats: VolatilityStats,
    anomaly_cache: std::collections::HashMap<String, u64>,
    current_slot: u64,
    vol_threshold_multiplier: f64,
}

impl VolOfVolTrader {
    pub fn new() -> Self {
        Self {
            volatility_history: VecDeque::with_capacity(MAX_VOLATILITY_HISTORY),
            realized_vol_window: VecDeque::with_capacity(MAX_VOLATILITY_HISTORY),
            vol_of_vol_stats: VolatilityStats::new(),
            anomaly_cache: std::collections::HashMap::new(),
            current_slot: 0,
            vol_threshold_multiplier: DEFAULT_VOL_THRESHOLD_MULTIPLIER,
        }
    }

    pub fn with_threshold_multiplier(mut self, multiplier: f64) -> Self {
        self.vol_threshold_multiplier = multiplier;
        self
    }

    pub fn add_snapshot(&mut self, snapshot: VolatilitySnapshot) -> Result<()> {
        // Input sanitization
        self.validate_snapshot(&snapshot)?;
        
        // Update current slot
        self.current_slot = snapshot.slot;
        
        // Add to history with bounded growth
        if self.volatility_history.len() >= MAX_VOLATILITY_HISTORY {
            self.volatility_history.pop_front();
        }
        self.volatility_history.push_back(snapshot);
        
        Ok(())
    }

    fn validate_snapshot(&self, snapshot: &VolatilitySnapshot) -> Result<()> {
        // Check for stale data
        if self.current_slot > 0 && snapshot.slot + MAX_SLOT_DRIFT < self.current_slot {
            return Err(VolTraderError::StaleData {
                slot: snapshot.slot,
                drift: self.current_slot - snapshot.slot,
            });
        }

        // Validate price data
        if snapshot.price <= Decimal::ZERO {
            return Err(VolTraderError::InvalidPrice {
                reason: "Price must be positive".to_string(),
            });
        }

        if snapshot.oracle_price <= Decimal::ZERO {
            return Err(VolTraderError::InvalidPrice {
                reason: "Oracle price must be positive".to_string(),
            });
        }

        // Validate fee rate bounds
        if snapshot.lp_fee_rate < Decimal::ZERO || snapshot.lp_fee_rate > Decimal::ONE {
            return Err(VolTraderError::InvalidPrice {
                reason: "LP fee rate must be between 0 and 1".to_string(),
            });
        }

        Ok(())
    }

    pub fn compute_realized_volatility(&mut self) -> Result<f64> {
        if self.volatility_history.len() < MIN_OBSERVATIONS {
            return Err(VolTraderError::InsufficientData {
                min: MIN_OBSERVATIONS,
                actual: self.volatility_history.len(),
            });
        }

        let mut log_returns = Vec::new();
        let history: Vec<_> = self.volatility_history.iter().collect();
        
        for i in 1..history.len() {
            let prev_price = history[i-1].price.to_f64().unwrap_or(0.0);
            let curr_price = history[i].price.to_f64().unwrap_or(0.0);
            
            if prev_price > 0.0 && curr_price > 0.0 {
                let log_return = (curr_price / prev_price).ln();
                log_returns.push(log_return);
            }
        }

        if log_returns.is_empty() {
            return Err(VolTraderError::Computation {
                details: "No valid log returns computed".to_string(),
            });
        }

        // Welford's algorithm for online variance computation
        let realized_vol = self.welford_volatility(&log_returns)?;
        
        // Update vol-of-vol window
        if self.realized_vol_window.len() >= MAX_VOLATILITY_HISTORY {
            self.realized_vol_window.pop_front();
        }
        self.realized_vol_window.push_back(realized_vol);

        Ok(realized_vol)
    }

    fn welford_volatility(&self, returns: &[f64]) -> Result<f64> {
        if returns.is_empty() {
            return Err(VolTraderError::Computation {
                details: "Empty returns array".to_string(),
            });
        }

        let mut mean = 0.0;
        let mut m2 = 0.0;
        
        for (i, &value) in returns.iter().enumerate() {
            let delta = value - mean;
            mean += delta / (i + 1) as f64;
            let delta2 = value - mean;
            m2 += delta * delta2;
        }

        let variance = if returns.len() > 1 { m2 / (returns.len() - 1) as f64 } else { 0.0 };
        Ok(variance.sqrt())
    }

    pub fn compute_vol_of_vol(&mut self) -> Result<f64> {
        if self.realized_vol_window.len() < MIN_OBSERVATIONS {
            return Err(VolTraderError::InsufficientData {
                min: MIN_OBSERVATIONS,
                actual: self.realized_vol_window.len(),
            });
        }

        let vol_returns: Vec<f64> = self.realized_vol_window.iter().cloned().collect();
        let vol_of_vol = self.welford_volatility(&vol_returns)?;

        // Update vol-of-vol statistics
        self.update_vol_of_vol_stats(vol_of_vol);

        Ok(vol_of_vol)
    }

    fn update_vol_of_vol_stats(&mut self, vol_of_vol: f64) {
        let stats = &mut self.vol_of_vol_stats;
        stats.count += 1;
        stats.min = stats.min.min(vol_of_vol);
        stats.max = stats.max.max(vol_of_vol);

        let delta = vol_of_vol - stats.mean;
        stats.mean += delta / stats.count as f64;
        let delta2 = vol_of_vol - stats.mean;
        stats.variance += delta * delta2;
    }

    pub fn detect_volatility_anomaly(&mut self, snapshot: &VolatilitySnapshot) -> Result<Option<VolatilityAnomaly>> {
        if self.volatility_history.len() < MIN_OBSERVATIONS {
            return Ok(None);
        }

        let realized_vol = self.compute_realized_volatility()?;
        let vol_of_vol = self.compute_vol_of_vol()?;

        // Check for various anomaly types
        let anomaly_checks = vec![
            self.check_lp_fee_spike(snapshot, realized_vol),
            self.check_perp_iv_compression(snapshot, realized_vol),
            self.check_oracle_vol_divergence(snapshot, realized_vol),
            self.check_regime_change(realized_vol, vol_of_vol),
            self.check_vol_spike(realized_vol),
            self.check_vol_compression(realized_vol),
        ];

        for anomaly_result in anomaly_checks {
            if let Some(mut anomaly) = anomaly_result {
                // Generate fingerprint for deduplication
                let fingerprint = self.generate_anomaly_fingerprint(&anomaly);
                
                // Check if we've seen this anomaly recently
                if let Some(&last_seen) = self.anomaly_cache.get(&fingerprint) {
                    if snapshot.timestamp - last_seen < 300000 { // 5 minutes
                        continue;
                    }
                }

                anomaly.fingerprint = fingerprint.clone();
                self.anomaly_cache.insert(fingerprint, snapshot.timestamp);
                
                return Ok(Some(anomaly));
            }
        }

        Ok(None)
    }

    fn check_lp_fee_spike(&self, snapshot: &VolatilitySnapshot, realized_vol: f64) -> Option<VolatilityAnomaly> {
        if self.volatility_history.len() < 5 {
            return None;
        }

        let recent_fees: Vec<f64> = self.volatility_history
            .iter()
            .rev()
            .take(5)
            .map(|s| s.lp_fee_rate.to_f64().unwrap_or(0.0))
            .collect();

        let avg_fee = recent_fees.iter().sum::<f64>() / recent_fees.len() as f64;
        let current_fee = snapshot.lp_fee_rate.to_f64().unwrap_or(0.0);

        if current_fee > avg_fee * 3.0 && realized_vol < 0.1 {
            Some(VolatilityAnomaly {
                anomaly_type: AnomalyType::LpFeeSpike,
                severity: current_fee / avg_fee,
                detected_at: snapshot.timestamp,
                slot: snapshot.slot,
                description: format!("LP fee spike: {:.4} vs avg {:.4}", current_fee, avg_fee),
                fingerprint: String::new(),
            })
        } else {
            None
        }
    }

    fn check_perp_iv_compression(&self, snapshot: &VolatilitySnapshot, realized_vol: f64) -> Option<VolatilityAnomaly> {
        let perp_iv = snapshot.perp_iv.to_f64().unwrap_or(0.0);
        
        if perp_iv > 0.0 && realized_vol > perp_iv * 2.0 {
            Some(VolatilityAnomaly {
                anomaly_type: AnomalyType::PerpIvCompression,
                severity: realized_vol / perp_iv,
                detected_at: snapshot.timestamp,
                slot: snapshot.slot,
                description: format!("Perp IV compression: realized {:.4} vs implied {:.4}", realized_vol, perp_iv),
                fingerprint: String::new(),
            })
        } else {
            None
        }
    }

    fn check_oracle_vol_divergence(&self, snapshot: &VolatilitySnapshot, _realized_vol: f64) -> Option<VolatilityAnomaly> {
        let price_diff = (snapshot.price - snapshot.oracle_price).abs();
        let price_divergence = price_diff / snapshot.oracle_price;
        
        if price_divergence.to_f64().unwrap_or(0.0) > 0.05 {
            Some(VolatilityAnomaly {
                anomaly_type: AnomalyType::OracleVolDivergence,
                severity: price_divergence.to_f64().unwrap_or(0.0),
                detected_at: snapshot.timestamp,
                slot: snapshot.slot,
                description: format!("Oracle divergence: {:.4}%", price_divergence.to_f64().unwrap_or(0.0) * 100.0),
                fingerprint: String::new(),
            })
        } else {
            None
        }
    }

    fn check_regime_change(&self, realized_vol: f64, vol_of_vol: f64) -> Option<VolatilityAnomaly> {
        if self.vol_of_vol_stats.count < MIN_OBSERVATIONS {
            return None;
        }

        let vol_of_vol_std = (self.vol_of_vol_stats.variance / self.vol_of_vol_stats.count as f64).sqrt();
        let threshold = self.vol_of_vol_stats.mean + self.vol_threshold_multiplier * vol_of_vol_std;

        if vol_of_vol > threshold {
            Some(VolatilityAnomaly {
                anomaly_type: AnomalyType::RegimeChange,
                severity: (vol_of_vol - threshold) / vol_of_vol_std,
                detected_at: SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64,
                slot: self.current_slot,
                description: format!("Regime change detected: vol-of-vol {:.4} vs threshold {:.4}", vol_of_vol, threshold),
                fingerprint: String::new(),
            })
        } else {
            None
        }
    }

    fn check_vol_spike(&self, realized_vol: f64) -> Option<VolatilityAnomaly> {
        if self.realized_vol_window.len() < MIN_OBSERVATIONS {
            return None;
        }

        let recent_vols: Vec<f64> = self.realized_vol_window.iter().cloned().collect();
        let avg_vol = recent_vols.iter().sum::<f64>() / recent_vols.len() as f64;
        let vol_variance = recent_vols.iter().map(|v| (v - avg_vol).powi(2)).sum::<f64>() / recent_vols.len() as f64;
        let vol_std = vol_variance.sqrt();

        let threshold = avg_vol + self.vol_threshold_multiplier * vol_std;

        if realized_vol > threshold {
            Some(VolatilityAnomaly {
                anomaly_type: AnomalyType::VolSpike,
                severity: (realized_vol - threshold) / vol_std,
                detected_at: SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64,
                slot: self.current_slot,
                description: format!("Vol spike: {:.4} vs threshold {:.4}", realized_vol, threshold),
                fingerprint: String::new(),
            })
        } else {
            None
        }
    }

    fn check_vol_compression(&self, realized_vol: f64) -> Option<VolatilityAnomaly> {
        if self.realized_vol_window.len() < MIN_OBSERVATIONS {
            return None;
        }

        let recent_vols: Vec<f64> = self.realized_vol_window.iter().cloned().collect();
        let avg_vol = recent_vols.iter().sum::<f64>() / recent_vols.len() as f64;
        let vol_variance = recent_vols.iter().map(|v| (v - avg_vol).powi(2)).sum::<f64>() / recent_vols.len() as f64;
        let vol_std = vol_variance.sqrt();

        let threshold = avg_vol - self.vol_threshold_multiplier * vol_std;

        if realized_vol < threshold && threshold > 0.0 {
            Some(VolatilityAnomaly {
                anomaly_type: AnomalyType::VolCompression,
                severity: (threshold - realized_vol) / vol_std,
                detected_at: SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64,
                slot: self.current_slot,
                description: format!("Vol compression: {:.4} vs threshold {:.4}", realized_vol, threshold),
                fingerprint: String::new(),
            })
        } else {
            None
        }
    }

    fn generate_anomaly_fingerprint(&self, anomaly: &VolatilityAnomaly) -> String {
        let mut hasher = Hasher::new();
        hasher.update(format!("{:?}", anomaly.anomaly_type).as_bytes());
        hasher.update(&anomaly.slot.to_le_bytes());
        hasher.update(&(anomaly.severity as u64).to_le_bytes());
        format!("{:x}", hasher.finalize())
    }

    pub fn generate_vol_of_vol_signal(&mut self) -> Result<Option<VolOfVolSignal>> {
        if self.volatility_history.is_empty() {
            return Ok(None);
        }

        let latest_snapshot = self.volatility_history.back().unwrap();
        let realized_vol = self.compute_realized_volatility()?;
        let vol_of_vol = self.compute_vol_of_vol()?;
        let anomaly = self.detect_volatility_anomaly(latest_snapshot)?;

        // Signal generation logic
        let signal_strength = self.calculate_signal_strength(realized_vol, vol_of_vol, &anomaly);
        let confidence = self.calculate_confidence(realized_vol, vol_of_vol);

        // Only generate signal if strength is above threshold
        if signal_strength > 0.5 {
            Ok(Some(VolOfVolSignal {
                timestamp: latest_snapshot.timestamp,
                slot: latest_snapshot.slot,
                source: latest_snapshot.source.clone(),
                realized_vol,
                vol_of_vol,
                anomaly,
                confidence,
                signal_strength,
            }))
        } else {
            Ok(None)
        }
    }

    fn calculate_signal_strength(&self, realized_vol: f64, vol_of_vol: f64, anomaly: &Option<VolatilityAnomaly>) -> f64 {
        let mut strength = 0.0;

        // Base strength from vol-of-vol
        if self.vol_of_vol_stats.count > 0 {
            let vol_of_vol_zscore = (vol_of_vol - self.vol_of_vol_stats.mean) / 
                (self.vol_of_vol_stats.variance / self.vol_of_vol_stats.count as f64).sqrt();
            strength += vol_of_vol_zscore.abs() * 0.3;
        }

        // Strength from realized volatility
        if self.realized_vol_window.len() >= MIN_OBSERVATIONS {
            let avg_vol = self.realized_vol_window.iter().sum::<f64>() / self.realized_vol_window.len() as f64;
            let vol_ratio = realized_vol / avg_vol.max(0.001);
            strength += (vol_ratio - 1.0).abs() * 0.4;
        }

        // Anomaly boost
        if let Some(anomaly) = anomaly {
            strength += anomaly.severity * 0.3;
        }

        strength.min(1.0)
    }

    fn calculate_confidence(&self, _realized_vol: f64, _vol_of_vol: f64) -> f64 {
        let data_quality = (self.volatility_history.len() as f64 / MAX_VOLATILITY_HISTORY as f64).min(1.0);
        let recency_factor = if self.volatility_history.len() >= 2 {
            let latest = self.volatility_history.back().unwrap();
            let second_latest = &self.volatility_history[self.volatility_history.len() - 2];
            let time_diff = latest.timestamp - second_latest.timestamp;
            if time_diff < 120000 { 1.0 } else { 0.8 } // 2 minutes
        } else {
            0.5
        };

        (data_quality * 0.6 + recency_factor * 0.4).min(1.0)
    }
}

impl Default for VolOfVolTrader {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn create_test_snapshot(slot: u64, price: f64, lp_fee: f64, perp_iv: f64) -> VolatilitySnapshot {
        VolatilitySnapshot {
            slot,
            timestamp: SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64,
            price: Decimal::from_f64_retain(price).unwrap(),
            lp_fee_rate: Decimal::from_f64_retain(lp_fee).unwrap(),
            perp_iv: Decimal::from_f64_retain(perp_iv).unwrap(),
            oracle_price: Decimal::from_f64_retain(price * 1.001).unwrap(),
            source: "test_oracle".to_string(),
        }
    }

    #[test]
    fn test_vol_spike_detection() {
        let mut trader = VolOfVolTrader::new();
        
        // Add normal volatility data
        for i in 0..20 {
            let snapshot = create_test_snapshot(i, 100.0 + (i as f64 * 0.1), 0.001, 0.2);
            trader.add_snapshot(snapshot).unwrap();
        }

        // Add a volatility spike
        let spike_snapshot = create_test_snapshot(21, 150.0, 0.001, 0.2);
        trader.add_snapshot(spike_snapshot).unwrap();

        let anomaly = trader.detect_volatility_anomaly(&spike_snapshot).unwrap();
        assert!(anomaly.is_some());
        
        let anomaly = anomaly.unwrap();
        assert_eq!(anomaly.anomaly_type, AnomalyType::VolSpike);
        assert!(anomaly.severity > 0.0);
    }

    #[test]
    fn test_lp_fee_spike_detection() {
        let mut trader = VolOfVolTrader::new();
        
        // Add normal fee data
        for i in 0..10 {
            let snapshot = create_test_snapshot(i, 100.0, 0.001, 0.2);
            trader.add_snapshot(snapshot).unwrap();
        }

        // Add fee spike with low volatility
        let fee_spike_snapshot = create_test_snapshot(11, 100.1, 0.01, 0.2);
        trader.add_snapshot(fee_spike_snapshot).unwrap();

        let anomaly = trader.detect_volatility_anomaly(&fee_spike_snapshot).unwrap();
        assert!(anomaly.is_some());
        
        let anomaly = anomaly.unwrap();
        assert_eq!(anomaly.anomaly_type, AnomalyType::LpFeeSpike);
    }

    #[test]
    fn test_oracle_divergence_detection() {
        let mut trader = VolOfVolTrader::new();
        
        let mut divergent_snapshot = create_test_snapshot(1, 100.0, 0.001, 0.2);
        divergent_snapshot.oracle_price = Decimal::from_f64_retain(90.0).unwrap(); // 10% divergence
        
        trader.add_snapshot(divergent_snapshot.clone()).unwrap();

        let anomaly = trader.detect_volatility_anomaly(&divergent_snapshot).unwrap();
        assert!(anomaly.is_some());
        
        let anomaly = anomaly.unwrap();
        assert_eq!(anomaly.anomaly_type, AnomalyType::OracleVolDivergence);
        assert!(anomaly.severity > 0.05);
    }

    #[test]
    fn test_vol_compression_detection() {
        let mut trader = VolOfVolTrader::new();
        
        // Add high volatility data
        for i in 0..15 {
            let price = 100.0 + (i as f64 * 5.0); // High volatility
            let snapshot = create_test_snapshot(i, price, 0.001, 0.2);
            trader.add_snapshot(snapshot).unwrap();
        }

        // Add compressed volatility
        let compressed_snapshot = create_test_snapshot(16, 100.1, 0.001, 0.2);
        trader.add_snapshot(compressed_snapshot).unwrap();

        let anomaly = trader.detect_volatility_anomaly(&compressed_snapshot).unwrap();
        assert!(anomaly.is_some());
        
        let anomaly = anomaly.unwrap();
        assert_eq!(anomaly.anomaly_type, AnomalyType::VolCompression);
    }

    #[test]
    fn test_stale_data_rejection() {
        let mut trader = VolOfVolTrader::new();
        
        // Add current data
        let current_snapshot = create_test_snapshot(100, 100.0, 0.001, 0.2);
        trader.add_snapshot(current_snapshot).unwrap();

        // Try to add stale data
        let stale_snapshot = create_test_snapshot(80, 100.0, 0.001, 0.2);
        let result = trader.add_snapshot(stale_snapshot);
        
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), VolTraderError::StaleData { .. }));
    }

    #[test]
    fn test_signal_generation() {
        let mut trader = VolOfVolTrader::new();
        
        // Add enough data for signal generation
        for i in 0..30 {
            let price = 100.0 + (i as f64 * 2.0); // Moderate volatility
            let snapshot = create_test_snapshot(i, price, 0.001, 0.2);
            trader.add_snapshot(snapshot).unwrap();
        }

        let signal = trader.generate_vol_of_vol_signal().unwrap();
        assert!(signal.is_some());
        
        let signal = signal.unwrap();
        assert!(signal.confidence > 0.0);
        assert!(signal.realized_vol > 0.0);
        assert!(signal.vol_of_vol >= 0.0);
    }

    #[test]
    fn test_welford_algorithm() {
        let trader = VolOfVolTrader::new();
        let returns = vec![0.1, -0.05, 0.03, -0.02, 0.07];
        
        let volatility = trader.welford_volatility(&returns).unwrap();
        assert!(volatility > 0.0);
        assert!(volatility < 1.0);
    }

    #[test]
    fn test_insufficient_data_handling() {
        let mut trader = VolOfVolTrader::new();
        
        // Add insufficient data
        for i in 0..5 {
            let snapshot = create_test_snapshot(i, 100.0, 0.001, 0.2);
            trader.add_snapshot(snapshot).unwrap();
        }

        let result = trader.compute_realized_volatility();
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), VolTraderError::InsufficientData { .. }));
    }
}
