use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};
use serde::{Deserialize, Serialize};
use solana_sdk::pubkey::Pubkey;
use tokio::sync::RwLock as AsyncRwLock;

const PRICE_HISTORY_SIZE: usize = 1000;
const VOLATILITY_WINDOW_SIZES: [usize; 4] = [10, 30, 60, 120];
const REGIME_UPDATE_INTERVAL: Duration = Duration::from_millis(100);
const GARCH_ALPHA: f64 = 0.1;
const GARCH_BETA: f64 = 0.85;
const GARCH_OMEGA: f64 = 0.05;
const EMA_ALPHA: f64 = 0.94;
const REGIME_THRESHOLD_LOW: f64 = 0.0005;
const REGIME_THRESHOLD_MEDIUM: f64 = 0.002;
const REGIME_THRESHOLD_HIGH: f64 = 0.008;
const REGIME_THRESHOLD_EXTREME: f64 = 0.02;
const MIN_DATA_POINTS: usize = 50;
const PARKINSON_FACTOR: f64 = 0.361;
const GARMAN_KLASS_FACTOR: f64 = 0.511;
const YANG_ZHANG_FACTOR: f64 = 0.34;

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub enum VolatilityRegime {
    Low,
    Medium,
    High,
    Extreme,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct VolatilityMetrics {
    pub realized_volatility: f64,
    pub garch_volatility: f64,
    pub parkinson_volatility: f64,
    pub garman_klass_volatility: f64,
    pub yang_zhang_volatility: f64,
    pub composite_volatility: f64,
    pub volatility_of_volatility: f64,
    pub regime: VolatilityRegime,
    pub regime_confidence: f64,
    pub timestamp: u64,
}

#[derive(Debug, Clone)]
struct PriceData {
    pub price: f64,
    pub high: f64,
    pub low: f64,
    pub open: f64,
    pub close: f64,
    pub volume: f64,
    pub timestamp: u64,
}

#[derive(Debug)]
struct TokenVolatilityState {
    price_history: VecDeque<PriceData>,
    volatility_history: VecDeque<f64>,
    garch_variance: f64,
    ema_variance: f64,
    current_metrics: VolatilityMetrics,
    last_update: Instant,
    regime_history: VecDeque<(VolatilityRegime, u64)>,
}

pub struct VolatilityRegimeClassifier {
    states: Arc<AsyncRwLock<HashMap<Pubkey, TokenVolatilityState>>>,
    regime_weights: [f64; 5],
    adaptive_thresholds: Arc<RwLock<HashMap<Pubkey, [f64; 4]>>>,
}

impl VolatilityRegimeClassifier {
    pub fn new() -> Self {
        Self {
            states: Arc::new(AsyncRwLock::new(HashMap::new())),
            regime_weights: [0.25, 0.20, 0.20, 0.20, 0.15],
            adaptive_thresholds: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub async fn update_price(&self, token: Pubkey, price: f64, high: f64, low: f64, open: f64, volume: f64, timestamp: u64) -> Result<VolatilityMetrics, Box<dyn std::error::Error + Send + Sync>> {
        let mut states = self.states.write().await;
        let state = states.entry(token).or_insert_with(|| TokenVolatilityState {
            price_history: VecDeque::with_capacity(PRICE_HISTORY_SIZE),
            volatility_history: VecDeque::with_capacity(PRICE_HISTORY_SIZE),
            garch_variance: 0.01,
            ema_variance: 0.01,
            current_metrics: VolatilityMetrics {
                realized_volatility: 0.0,
                garch_volatility: 0.0,
                parkinson_volatility: 0.0,
                garman_klass_volatility: 0.0,
                yang_zhang_volatility: 0.0,
                composite_volatility: 0.0,
                volatility_of_volatility: 0.0,
                regime: VolatilityRegime::Low,
                regime_confidence: 1.0,
                timestamp,
            },
            last_update: Instant::now(),
            regime_history: VecDeque::with_capacity(100),
        });

        let price_data = PriceData {
            price,
            high,
            low,
            open,
            close: price,
            volume,
            timestamp,
        };

        state.price_history.push_back(price_data);
        if state.price_history.len() > PRICE_HISTORY_SIZE {
            state.price_history.pop_front();
        }

        if state.price_history.len() >= MIN_DATA_POINTS && 
           state.last_update.elapsed() >= REGIME_UPDATE_INTERVAL {
            let metrics = self.calculate_volatility_metrics(state, &token).await?;
            state.current_metrics = metrics;
            state.last_update = Instant::now();
            
            state.regime_history.push_back((metrics.regime, timestamp));
            if state.regime_history.len() > 100 {
                state.regime_history.pop_front();
            }
        }

        Ok(state.current_metrics)
    }

    async fn calculate_volatility_metrics(&self, state: &mut TokenVolatilityState, token: &Pubkey) -> Result<VolatilityMetrics, Box<dyn std::error::Error + Send + Sync>> {
        let prices: Vec<f64> = state.price_history.iter().map(|p| p.price).collect();
        
        let realized_vol = self.calculate_realized_volatility(&prices, 30);
        let garch_vol = self.update_garch_volatility(state, &prices);
        let parkinson_vol = self.calculate_parkinson_volatility(&state.price_history, 30);
        let gk_vol = self.calculate_garman_klass_volatility(&state.price_history, 30);
        let yz_vol = self.calculate_yang_zhang_volatility(&state.price_history, 30);
        
        let composite_vol = self.calculate_composite_volatility(
            realized_vol,
            garch_vol,
            parkinson_vol,
            gk_vol,
            yz_vol
        );

        state.volatility_history.push_back(composite_vol);
        if state.volatility_history.len() > PRICE_HISTORY_SIZE {
            state.volatility_history.pop_front();
        }

        let vol_of_vol = self.calculate_volatility_of_volatility(&state.volatility_history);
        
        let (regime, confidence) = self.classify_regime(composite_vol, vol_of_vol, state, token).await;

        Ok(VolatilityMetrics {
            realized_volatility: realized_vol,
            garch_volatility: garch_vol,
            parkinson_volatility: parkinson_vol,
            garman_klass_volatility: gk_vol,
            yang_zhang_volatility: yz_vol,
            composite_volatility: composite_vol,
            volatility_of_volatility: vol_of_vol,
            regime,
            regime_confidence: confidence,
            timestamp: state.price_history.back().map(|p| p.timestamp).unwrap_or(0),
        })
    }

    fn calculate_realized_volatility(&self, prices: &[f64], window: usize) -> f64 {
        if prices.len() < window + 1 {
            return 0.0;
        }

        let start_idx = prices.len().saturating_sub(window + 1);
        let returns: Vec<f64> = prices[start_idx..]
            .windows(2)
            .map(|w| (w[1] / w[0]).ln())
            .collect();

        if returns.is_empty() {
            return 0.0;
        }

        let mean = returns.iter().sum::<f64>() / returns.len() as f64;
        let variance = returns.iter()
            .map(|r| (r - mean).powi(2))
            .sum::<f64>() / returns.len() as f64;

        variance.sqrt()
    }

    fn update_garch_volatility(&self, state: &mut TokenVolatilityState, prices: &[f64]) -> f64 {
        if prices.len() < 2 {
            return state.garch_variance.sqrt();
        }

        let last_return = (prices[prices.len() - 1] / prices[prices.len() - 2]).ln();
        let squared_return = last_return.powi(2);

        state.garch_variance = GARCH_OMEGA + 
            GARCH_ALPHA * squared_return + 
            GARCH_BETA * state.garch_variance;

        state.ema_variance = EMA_ALPHA * state.ema_variance + (1.0 - EMA_ALPHA) * squared_return;

        state.garch_variance.sqrt()
    }

    fn calculate_parkinson_volatility(&self, price_data: &VecDeque<PriceData>, window: usize) -> f64 {
        if price_data.len() < window {
            return 0.0;
        }

        let start_idx = price_data.len().saturating_sub(window);
        let sum: f64 = price_data.iter()
            .skip(start_idx)
            .map(|p| {
                if p.low > 0.0 {
                    (p.high / p.low).ln().powi(2)
                } else {
                    0.0
                }
            })
            .sum();

        (PARKINSON_FACTOR * sum / window as f64).sqrt()
    }

    fn calculate_garman_klass_volatility(&self, price_data: &VecDeque<PriceData>, window: usize) -> f64 {
        if price_data.len() < window {
            return 0.0;
        }

        let start_idx = price_data.len().saturating_sub(window);
        let sum: f64 = price_data.iter()
            .skip(start_idx)
            .map(|p| {
                if p.low > 0.0 && p.open > 0.0 {
                    0.5 * (p.high / p.low).ln().powi(2) - 
                    (2.0 * 2.0_f64.ln() - 1.0) * (p.close / p.open).ln().powi(2)
                } else {
                    0.0
                }
            })
            .sum();

        (GARMAN_KLASS_FACTOR * sum / window as f64).abs().sqrt()
    }

    fn calculate_yang_zhang_volatility(&self, price_data: &VecDeque<PriceData>, window: usize) -> f64 {
        if price_data.len() < window + 1 {
            return 0.0;
        }

        let start_idx = price_data.len().saturating_sub(window);
        let data_slice: Vec<&PriceData> = price_data.iter().skip(start_idx).collect();

        let overnight_sum: f64 = data_slice.windows(2)
            .map(|w| {
                if w[0].close > 0.0 {
                    (w[1].open / w[0].close).ln().powi(2)
                } else {
                    0.0
                }
            })
            .sum();

        let open_close_sum: f64 = data_slice.iter()
            .map(|p| {
                if p.open > 0.0 {
                    (p.close / p.open).ln().powi(2)
                } else {
                    0.0
                }
            })
            .sum();

        let rogers_satchell_sum: f64 = data_slice.iter()
            .map(|p| {
                if p.open > 0.0 && p.low > 0.0 {
                    (p.high / p.close).ln() * (p.high / p.open).ln() +
                    (p.low / p.close).ln() * (p.low / p.open).ln()
                } else {
                    0.0
                }
            })
            .sum();

        let k = YANG_ZHANG_FACTOR;
        let variance = overnight_sum / (window as f64 - 1.0) +
                      k * open_close_sum / window as f64 +
                      (1.0 - k) * rogers_satchell_sum / window as f64;

        variance.abs().sqrt()
    }

    fn calculate_composite_volatility(&self, realized: f64, garch: f64, parkinson: f64, gk: f64, yz: f64) -> f64 {
        let weights = &self.regime_weights;
        let composite = weights[0] * realized + 
                       weights[1] * garch + 
                       weights[2] * parkinson + 
                       weights[3] * gk + 
                       weights[4] * yz;

        let adjustment_factor = 1.0 + 0.1 * (garch / realized.max(0.0001) - 1.0).tanh();
        composite * adjustment_factor
    }

        fn calculate_volatility_of_volatility(&self, volatility_history: &VecDeque<f64>) -> f64 {
        if volatility_history.len() < 20 {
            return 0.0;
        }

        let recent_vols: Vec<f64> = volatility_history.iter()
            .rev()
            .take(30)
            .copied()
            .collect();

        let mean_vol = recent_vols.iter().sum::<f64>() / recent_vols.len() as f64;
        let variance = recent_vols.iter()
            .map(|v| (v - mean_vol).powi(2))
            .sum::<f64>() / recent_vols.len() as f64;

        variance.sqrt() / mean_vol.max(0.0001)
    }

    async fn classify_regime(&self, composite_vol: f64, vol_of_vol: f64, state: &TokenVolatilityState, token: &Pubkey) -> (VolatilityRegime, f64) {
        let thresholds = self.get_adaptive_thresholds(token, state).await;
        
        let base_regime = if composite_vol < thresholds[0] {
            VolatilityRegime::Low
        } else if composite_vol < thresholds[1] {
            VolatilityRegime::Medium
        } else if composite_vol < thresholds[2] {
            VolatilityRegime::High
        } else {
            VolatilityRegime::Extreme
        };

        let vol_of_vol_adjustment = match vol_of_vol {
            v if v > 0.5 => 1,
            v if v > 0.3 => 0,
            _ => -1,
        };

        let regime_stability = self.calculate_regime_stability(state);
        let trend_strength = self.calculate_trend_strength(&state.price_history);
        let volume_profile = self.calculate_volume_profile(&state.price_history);

        let final_regime = self.adjust_regime(base_regime, vol_of_vol_adjustment, trend_strength, volume_profile);
        let confidence = self.calculate_regime_confidence(composite_vol, &thresholds, regime_stability, vol_of_vol);

        (final_regime, confidence)
    }

    async fn get_adaptive_thresholds(&self, token: &Pubkey, state: &TokenVolatilityState) -> [f64; 4] {
        let mut thresholds_guard = self.adaptive_thresholds.write().unwrap();
        
        if let Some(thresholds) = thresholds_guard.get(token) {
            *thresholds
        } else {
            let adaptive_thresholds = self.calculate_adaptive_thresholds(state);
            thresholds_guard.insert(*token, adaptive_thresholds);
            adaptive_thresholds
        }
    }

    fn calculate_adaptive_thresholds(&self, state: &TokenVolatilityState) -> [f64; 4] {
        if state.volatility_history.len() < 100 {
            return [REGIME_THRESHOLD_LOW, REGIME_THRESHOLD_MEDIUM, REGIME_THRESHOLD_HIGH, REGIME_THRESHOLD_EXTREME];
        }

        let mut sorted_vols: Vec<f64> = state.volatility_history.iter().copied().collect();
        sorted_vols.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));

        let percentiles = [20, 50, 80, 95];
        let mut thresholds = [0.0; 4];

        for (i, &p) in percentiles.iter().enumerate() {
            let idx = (sorted_vols.len() * p / 100).saturating_sub(1);
            thresholds[i] = sorted_vols[idx];
        }

        let base_thresholds = [REGIME_THRESHOLD_LOW, REGIME_THRESHOLD_MEDIUM, REGIME_THRESHOLD_HIGH, REGIME_THRESHOLD_EXTREME];
        for i in 0..4 {
            thresholds[i] = 0.7 * thresholds[i] + 0.3 * base_thresholds[i];
        }

        thresholds
    }

    fn calculate_regime_stability(&self, state: &TokenVolatilityState) -> f64 {
        if state.regime_history.len() < 10 {
            return 0.5;
        }

        let recent_regimes: Vec<VolatilityRegime> = state.regime_history.iter()
            .rev()
            .take(20)
            .map(|(r, _)| *r)
            .collect();

        let mut transitions = 0;
        for window in recent_regimes.windows(2) {
            if window[0] != window[1] {
                transitions += 1;
            }
        }

        1.0 - (transitions as f64 / (recent_regimes.len() - 1) as f64)
    }

    fn calculate_trend_strength(&self, price_history: &VecDeque<PriceData>) -> f64 {
        if price_history.len() < 50 {
            return 0.0;
        }

        let prices: Vec<f64> = price_history.iter()
            .rev()
            .take(50)
            .map(|p| p.price)
            .collect();

        let n = prices.len() as f64;
        let x_mean = (n - 1.0) / 2.0;
        let x_values: Vec<f64> = (0..prices.len()).map(|i| i as f64).collect();
        
        let y_mean = prices.iter().sum::<f64>() / n;
        
        let covariance: f64 = x_values.iter()
            .zip(prices.iter())
            .map(|(x, y)| (x - x_mean) * (y - y_mean))
            .sum();
        
        let x_variance: f64 = x_values.iter()
            .map(|x| (x - x_mean).powi(2))
            .sum();

        if x_variance == 0.0 {
            return 0.0;
        }

        let slope = covariance / x_variance;
        let r_squared = (covariance.powi(2)) / (x_variance * prices.iter().map(|y| (y - y_mean).powi(2)).sum::<f64>());

        slope.abs() * r_squared.sqrt()
    }

    fn calculate_volume_profile(&self, price_history: &VecDeque<PriceData>) -> f64 {
        if price_history.len() < 30 {
            return 1.0;
        }

        let recent_data: Vec<&PriceData> = price_history.iter()
            .rev()
            .take(30)
            .collect();

        let avg_volume = recent_data.iter()
            .map(|p| p.volume)
            .sum::<f64>() / recent_data.len() as f64;

        let volume_volatility = recent_data.iter()
            .map(|p| (p.volume - avg_volume).powi(2))
            .sum::<f64>() / recent_data.len() as f64;

        let volume_volatility = volume_volatility.sqrt() / avg_volume.max(1.0);

        let price_volume_correlation = self.calculate_price_volume_correlation(&recent_data);

        1.0 + 0.5 * volume_volatility + 0.3 * price_volume_correlation.abs()
    }

    fn calculate_price_volume_correlation(&self, data: &[&PriceData]) -> f64 {
        if data.len() < 2 {
            return 0.0;
        }

        let price_returns: Vec<f64> = data.windows(2)
            .map(|w| (w[1].price / w[0].price).ln())
            .collect();

        let volume_changes: Vec<f64> = data.windows(2)
            .map(|w| (w[1].volume / w[0].volume.max(1.0)).ln())
            .collect();

        let n = price_returns.len() as f64;
        let price_mean = price_returns.iter().sum::<f64>() / n;
        let volume_mean = volume_changes.iter().sum::<f64>() / n;

        let covariance: f64 = price_returns.iter()
            .zip(volume_changes.iter())
            .map(|(p, v)| (p - price_mean) * (v - volume_mean))
            .sum::<f64>() / n;

        let price_std = (price_returns.iter().map(|p| (p - price_mean).powi(2)).sum::<f64>() / n).sqrt();
        let volume_std = (volume_changes.iter().map(|v| (v - volume_mean).powi(2)).sum::<f64>() / n).sqrt();

        if price_std == 0.0 || volume_std == 0.0 {
            return 0.0;
        }

        covariance / (price_std * volume_std)
    }

    fn adjust_regime(&self, base_regime: VolatilityRegime, vol_adjustment: i32, trend_strength: f64, volume_profile: f64) -> VolatilityRegime {
        let regime_value = match base_regime {
            VolatilityRegime::Low => 0,
            VolatilityRegime::Medium => 1,
            VolatilityRegime::High => 2,
            VolatilityRegime::Extreme => 3,
        };

        let trend_adjustment = if trend_strength > 0.5 { 1 } else { 0 };
        let volume_adjustment = if volume_profile > 1.5 { 1 } else { 0 };

        let final_value = (regime_value + vol_adjustment + trend_adjustment + volume_adjustment)
            .max(0)
            .min(3);

        match final_value {
            0 => VolatilityRegime::Low,
            1 => VolatilityRegime::Medium,
            2 => VolatilityRegime::High,
            _ => VolatilityRegime::Extreme,
        }
    }

    fn calculate_regime_confidence(&self, volatility: f64, thresholds: &[f64; 4], stability: f64, vol_of_vol: f64) -> f64 {
        let distance_to_thresholds = thresholds.iter()
            .map(|&t| (volatility - t).abs())
            .fold(f64::MAX, f64::min);

        let threshold_confidence = 1.0 - (distance_to_thresholds / volatility.max(0.0001)).min(1.0);
        let stability_weight = 0.3;
        let vol_of_vol_penalty = (1.0 - vol_of_vol.min(1.0)) * 0.2;

        (threshold_confidence * (1.0 - stability_weight) + stability * stability_weight + vol_of_vol_penalty)
            .max(0.1)
            .min(1.0)
    }

    pub async fn get_current_metrics(&self, token: &Pubkey) -> Option<VolatilityMetrics> {
        let states = self.states.read().await;
        states.get(token).map(|state| state.current_metrics)
    }

    pub async fn get_regime(&self, token: &Pubkey) -> Option<VolatilityRegime> {
        self.get_current_metrics(token).await.map(|m| m.regime)
    }

    pub async fn get_multiple_regimes(&self, tokens: &[Pubkey]) -> HashMap<Pubkey, VolatilityRegime> {
        let states = self.states.read().await;
        tokens.iter()
            .filter_map(|token| {
                states.get(token).map(|state| (*token, state.current_metrics.regime))
            })
            .collect()
    }

    pub async fn is_regime_stable(&self, token: &Pubkey, min_stability: f64) -> bool {
        let states = self.states.read().await;
        if let Some(state) = states.get(token) {
            self.calculate_regime_stability(state) >= min_stability
        } else {
            false
        }
    }

    pub async fn get_regime_duration(&self, token: &Pubkey) -> Option<Duration> {
        let states = self.states.read().await;
        if let Some(state) = states.get(token) {
            if let Some((current_regime, start_time)) = state.regime_history.back() {
                let current_time = state.current_metrics.timestamp;
                let regime_matches = state.regime_history.iter()
                    .rev()
                    .take_while(|(r, _)| r == current_regime)
                    .last();
                
                if let Some((_, regime_start)) = regime_matches {
                    return Some(Duration::from_millis(current_time.saturating_sub(*regime_start)));
                }
            }
        }
        None
    }

    pub async fn cleanup_stale_data(&self, max_age: Duration) {
        let mut states = self.states.write().await;
        let now = Instant::now();
        
        states.retain(|_, state| {
            now.duration_since(state.last_update) < max_age
        });

        let mut thresholds = self.adaptive_thresholds.write().unwrap();
        let active_tokens: Vec<Pubkey> = states.keys().copied().collect();
        thresholds.retain(|token, _| active_tokens.contains(token));
    }

    pub async fn get_market_regime(&self) -> (VolatilityRegime, f64) {
        let states = self.states.read().await;
        if states.is_empty() {
            return (VolatilityRegime::Low, 0.0);
        }

        let mut regime_counts = [0u32; 4];
        let mut total_volatility = 0.0;
        let mut count = 0;

        for state in states.values() {
            match state.current_metrics.regime {
                VolatilityRegime::Low => regime_counts[0] += 1,
                VolatilityRegime::Medium => regime_counts[1] += 1,
                VolatilityRegime::High => regime_counts[2] += 1,
                VolatilityRegime::Extreme => regime_counts[3] += 1,
            }
            total_volatility += state.current_metrics.composite_volatility;
            count += 1;
        }

        let dominant_regime_idx = regime_counts.iter()
            .enumerate()
            .max_by_key(|(_, &count)| count)
            .map(|(idx, _)| idx)
            .unwrap_or(0);

                let regime = match dominant_regime_idx {
            0 => VolatilityRegime::Low,
            1 => VolatilityRegime::Medium,
            2 => VolatilityRegime::High,
            _ => VolatilityRegime::Extreme,
        };

        let avg_volatility = if count > 0 {
            total_volatility / count as f64
        } else {
            0.0
        };

        (regime, avg_volatility)
    }

    pub async fn get_cross_asset_correlation(&self, token1: &Pubkey, token2: &Pubkey, window: usize) -> f64 {
        let states = self.states.read().await;
        
        let state1 = match states.get(token1) {
            Some(s) => s,
            None => return 0.0,
        };
        
        let state2 = match states.get(token2) {
            Some(s) => s,
            None => return 0.0,
        };

        let min_len = state1.volatility_history.len().min(state2.volatility_history.len());
        if min_len < window {
            return 0.0;
        }

        let vols1: Vec<f64> = state1.volatility_history.iter()
            .rev()
            .take(window)
            .copied()
            .collect();
        
        let vols2: Vec<f64> = state2.volatility_history.iter()
            .rev()
            .take(window)
            .copied()
            .collect();

        self.calculate_correlation(&vols1, &vols2)
    }

    fn calculate_correlation(&self, data1: &[f64], data2: &[f64]) -> f64 {
        if data1.len() != data2.len() || data1.is_empty() {
            return 0.0;
        }

        let n = data1.len() as f64;
        let mean1 = data1.iter().sum::<f64>() / n;
        let mean2 = data2.iter().sum::<f64>() / n;

        let covariance: f64 = data1.iter()
            .zip(data2.iter())
            .map(|(x, y)| (x - mean1) * (y - mean2))
            .sum::<f64>() / n;

        let std1 = (data1.iter().map(|x| (x - mean1).powi(2)).sum::<f64>() / n).sqrt();
        let std2 = (data2.iter().map(|y| (y - mean2).powi(2)).sum::<f64>() / n).sqrt();

        if std1 == 0.0 || std2 == 0.0 {
            return 0.0;
        }

        covariance / (std1 * std2)
    }

    pub async fn get_regime_transition_probability(&self, token: &Pubkey, from_regime: VolatilityRegime, to_regime: VolatilityRegime) -> f64 {
        let states = self.states.read().await;
        
        let state = match states.get(token) {
            Some(s) => s,
            None => return 0.0,
        };

        if state.regime_history.len() < 20 {
            return 0.25; // Default equal probability
        }

        let mut transitions = HashMap::new();
        let mut regime_counts = HashMap::new();

        for window in state.regime_history.windows(2) {
            let from = window[0].0;
            let to = window[1].0;
            
            *transitions.entry((from, to)).or_insert(0) += 1;
            *regime_counts.entry(from).or_insert(0) += 1;
        }

        let transition_count = transitions.get(&(from_regime, to_regime)).copied().unwrap_or(0);
        let total_from_regime = regime_counts.get(&from_regime).copied().unwrap_or(1);

        transition_count as f64 / total_from_regime as f64
    }

    pub async fn predict_next_regime(&self, token: &Pubkey) -> Option<(VolatilityRegime, f64)> {
        let current_metrics = self.get_current_metrics(token).await?;
        let current_regime = current_metrics.regime;

        let states = self.states.read().await;
        let state = states.get(token)?;

        if state.volatility_history.len() < 50 {
            return Some((current_regime, 0.5));
        }

        let recent_trend = self.calculate_volatility_trend(&state.volatility_history);
        let regime_momentum = self.calculate_regime_momentum(state);
        
        let transition_probs = [
            self.get_regime_transition_probability(token, current_regime, VolatilityRegime::Low).await,
            self.get_regime_transition_probability(token, current_regime, VolatilityRegime::Medium).await,
            self.get_regime_transition_probability(token, current_regime, VolatilityRegime::High).await,
            self.get_regime_transition_probability(token, current_regime, VolatilityRegime::Extreme).await,
        ];

        let trend_adjustment = match recent_trend {
            t if t > 0.1 => [0.9, 1.0, 1.1, 1.2],
            t if t < -0.1 => [1.2, 1.1, 1.0, 0.9],
            _ => [1.0; 4],
        };

        let adjusted_probs: Vec<f64> = transition_probs.iter()
            .zip(trend_adjustment.iter())
            .map(|(p, a)| p * a)
            .collect();

        let total_prob: f64 = adjusted_probs.iter().sum();
        let normalized_probs: Vec<f64> = adjusted_probs.iter()
            .map(|p| p / total_prob)
            .collect();

        let (max_idx, &max_prob) = normalized_probs.iter()
            .enumerate()
            .max_by(|(_, a), (_, b)| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal))?;

        let predicted_regime = match max_idx {
            0 => VolatilityRegime::Low,
            1 => VolatilityRegime::Medium,
            2 => VolatilityRegime::High,
            _ => VolatilityRegime::Extreme,
        };

        let confidence = max_prob * (1.0 + regime_momentum).min(1.0);
        Some((predicted_regime, confidence))
    }

    fn calculate_volatility_trend(&self, volatility_history: &VecDeque<f64>) -> f64 {
        if volatility_history.len() < 20 {
            return 0.0;
        }

        let recent: Vec<f64> = volatility_history.iter()
            .rev()
            .take(20)
            .copied()
            .collect();

        let older: Vec<f64> = volatility_history.iter()
            .rev()
            .skip(20)
            .take(20)
            .copied()
            .collect();

        if older.is_empty() {
            return 0.0;
        }

        let recent_avg = recent.iter().sum::<f64>() / recent.len() as f64;
        let older_avg = older.iter().sum::<f64>() / older.len() as f64;

        (recent_avg - older_avg) / older_avg.max(0.0001)
    }

    fn calculate_regime_momentum(&self, state: &TokenVolatilityState) -> f64 {
        if state.regime_history.len() < 10 {
            return 0.0;
        }

        let current_regime = state.current_metrics.regime;
        let recent_regimes: Vec<VolatilityRegime> = state.regime_history.iter()
            .rev()
            .take(10)
            .map(|(r, _)| *r)
            .collect();

        let same_regime_count = recent_regimes.iter()
            .filter(|&&r| r == current_regime)
            .count();

        (same_regime_count as f64 / 10.0 - 0.5) * 2.0
    }

    pub async fn get_risk_adjusted_metrics(&self, token: &Pubkey) -> Option<RiskAdjustedMetrics> {
        let metrics = self.get_current_metrics(token).await?;
        let states = self.states.read().await;
        let state = states.get(token)?;

        let downside_volatility = self.calculate_downside_volatility(&state.price_history);
        let upside_volatility = self.calculate_upside_volatility(&state.price_history);
        let volatility_skew = upside_volatility / downside_volatility.max(0.0001);

        let var_95 = self.calculate_value_at_risk(&state.price_history, 0.95);
        let cvar_95 = self.calculate_conditional_value_at_risk(&state.price_history, 0.95);

        Some(RiskAdjustedMetrics {
            base_metrics: metrics,
            downside_volatility,
            upside_volatility,
            volatility_skew,
            value_at_risk_95: var_95,
            conditional_value_at_risk_95: cvar_95,
        })
    }

    fn calculate_downside_volatility(&self, price_history: &VecDeque<PriceData>) -> f64 {
        if price_history.len() < 30 {
            return 0.0;
        }

        let returns: Vec<f64> = price_history.iter()
            .rev()
            .take(30)
            .collect::<Vec<_>>()
            .windows(2)
            .map(|w| (w[0].price / w[1].price).ln())
            .filter(|&r| r < 0.0)
            .collect();

        if returns.is_empty() {
            return 0.0;
        }

        let mean = returns.iter().sum::<f64>() / returns.len() as f64;
        let variance = returns.iter()
            .map(|r| (r - mean).powi(2))
            .sum::<f64>() / returns.len() as f64;

        variance.sqrt()
    }

    fn calculate_upside_volatility(&self, price_history: &VecDeque<PriceData>) -> f64 {
        if price_history.len() < 30 {
            return 0.0;
        }

        let returns: Vec<f64> = price_history.iter()
            .rev()
            .take(30)
            .collect::<Vec<_>>()
            .windows(2)
            .map(|w| (w[0].price / w[1].price).ln())
            .filter(|&r| r > 0.0)
            .collect();

        if returns.is_empty() {
            return 0.0;
        }

        let mean = returns.iter().sum::<f64>() / returns.len() as f64;
        let variance = returns.iter()
            .map(|r| (r - mean).powi(2))
            .sum::<f64>() / returns.len() as f64;

        variance.sqrt()
    }

    fn calculate_value_at_risk(&self, price_history: &VecDeque<PriceData>, confidence: f64) -> f64 {
        if price_history.len() < 100 {
            return 0.0;
        }

        let mut returns: Vec<f64> = price_history.iter()
            .rev()
            .take(100)
            .collect::<Vec<_>>()
            .windows(2)
            .map(|w| (w[0].price / w[1].price).ln())
            .collect();

        returns.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        
        let index = ((1.0 - confidence) * returns.len() as f64) as usize;
        -returns[index.min(returns.len() - 1)]
    }

    fn calculate_conditional_value_at_risk(&self, price_history: &VecDeque<PriceData>, confidence: f64) -> f64 {
        if price_history.len() < 100 {
            return 0.0;
        }

        let mut returns: Vec<f64> = price_history.iter()
            .rev()
            .take(100)
            .collect::<Vec<_>>()
            .windows(2)
            .map(|w| (w[0].price / w[1].price).ln())
            .collect();

        returns.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        
        let var_index = ((1.0 - confidence) * returns.len() as f64) as usize;
        let tail_returns: Vec<f64> = returns.iter()
            .take(var_index + 1)
            .copied()
            .collect();

        if tail_returns.is_empty() {
            return 0.0;
        }

        -tail_returns.iter().sum::<f64>() / tail_returns.len() as f64
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RiskAdjustedMetrics {
    pub base_metrics: VolatilityMetrics,
    pub downside_volatility: f64,
    pub upside_volatility: f64,
    pub volatility_skew: f64,
    pub value_at_risk_95: f64,
    pub conditional_value_at_risk_95: f64,
}

impl Default for VolatilityRegimeClassifier {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_volatility_regime_classifier() {
        let classifier = VolatilityRegimeClassifier::new();
        let token = Pubkey::new_unique();
        
        for i in 0..100 {
            let price = 100.0 + (i as f64).sin() * 10.0;
            let high = price * 1.01;
            let low = price * 0.99;
            let open = price * 0.995;
            let volume = 1000000.0;
            let timestamp = 1000000 + i * 1000;
            
            let _ = classifier.update_price(token, price, high, low, open, volume, timestamp).await;
        }
        
        let metrics = classifier.get_current_metrics(&token).await;
        assert!(metrics.is_some());
    }
}

