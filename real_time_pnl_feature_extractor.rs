use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, RwLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use serde::{Deserialize, Serialize};
use parking_lot::Mutex;
use ordered_float::OrderedFloat;

const FEATURE_WINDOW_SIZES: [usize; 5] = [10, 50, 100, 500, 1000];
const MAX_HISTORY_SIZE: usize = 10000;
const EPSILON: f64 = 1e-10;
const RISK_FREE_RATE: f64 = 0.05 / 365.0 / 24.0 / 3600.0;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Trade {
    pub timestamp: u64,
    pub entry_price: f64,
    pub exit_price: f64,
    pub size: f64,
    pub trade_type: TradeType,
    pub gas_cost: f64,
    pub slippage: f64,
    pub latency_ms: u64,
    pub success: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum TradeType {
    Arbitrage,
    Liquidation,
    JitLiquidity,
    Sandwich,
    BackRun,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PnLFeatures {
    pub timestamp: u64,
    pub instant_pnl: f64,
    pub cumulative_pnl: f64,
    pub rolling_mean: HashMap<usize, f64>,
    pub rolling_std: HashMap<usize, f64>,
    pub sharpe_ratio: HashMap<usize, f64>,
    pub sortino_ratio: HashMap<usize, f64>,
    pub max_drawdown: HashMap<usize, f64>,
    pub win_rate: HashMap<usize, f64>,
    pub profit_factor: HashMap<usize, f64>,
    pub avg_win_size: HashMap<usize, f64>,
    pub avg_loss_size: HashMap<usize, f64>,
    pub trade_frequency: HashMap<usize, f64>,
    pub success_rate_by_type: HashMap<TradeType, f64>,
    pub pnl_by_type: HashMap<TradeType, f64>,
    pub gas_efficiency: f64,
    pub slippage_impact: f64,
    pub latency_percentiles: LatencyPercentiles,
    pub volatility_regime: VolatilityRegime,
    pub momentum_indicators: MomentumIndicators,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LatencyPercentiles {
    pub p50: f64,
    pub p90: f64,
    pub p95: f64,
    pub p99: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum VolatilityRegime {
    Low,
    Medium,
    High,
    Extreme,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MomentumIndicators {
    pub ema_12: f64,
    pub ema_26: f64,
    pub macd: f64,
    pub signal: f64,
    pub rsi: f64,
}

pub struct RealTimePnLFeatureExtractor {
    trade_history: Arc<Mutex<VecDeque<Trade>>>,
    pnl_history: Arc<Mutex<VecDeque<f64>>>,
    cumulative_pnl: Arc<RwLock<f64>>,
    feature_cache: Arc<RwLock<HashMap<u64, PnLFeatures>>>,
    trade_type_stats: Arc<RwLock<HashMap<TradeType, TradeTypeStats>>>,
    ema_states: Arc<Mutex<EmaStates>>,
}

#[derive(Clone)]
struct TradeTypeStats {
    success_count: u64,
    total_count: u64,
    total_pnl: f64,
}

struct EmaStates {
    ema_12: f64,
    ema_26: f64,
    signal: f64,
    gain_sum: f64,
    loss_sum: f64,
    period_count: u64,
}

impl RealTimePnLFeatureExtractor {
    pub fn new() -> Self {
        Self {
            trade_history: Arc::new(Mutex::new(VecDeque::with_capacity(MAX_HISTORY_SIZE))),
            pnl_history: Arc::new(Mutex::new(VecDeque::with_capacity(MAX_HISTORY_SIZE))),
            cumulative_pnl: Arc::new(RwLock::new(0.0)),
            feature_cache: Arc::new(RwLock::new(HashMap::with_capacity(1000))),
            trade_type_stats: Arc::new(RwLock::new(HashMap::new())),
            ema_states: Arc::new(Mutex::new(EmaStates {
                ema_12: 0.0,
                ema_26: 0.0,
                signal: 0.0,
                gain_sum: 0.0,
                loss_sum: 0.0,
                period_count: 0,
            })),
        }
    }

    pub async fn process_trade(&self, trade: Trade) -> Result<PnLFeatures, Box<dyn std::error::Error + Send + Sync>> {
        let pnl = self.calculate_trade_pnl(&trade);
        
        {
            let mut history = self.trade_history.lock();
            history.push_back(trade.clone());
            if history.len() > MAX_HISTORY_SIZE {
                history.pop_front();
            }
        }
        
        {
            let mut pnl_hist = self.pnl_history.lock();
            pnl_hist.push_back(pnl);
            if pnl_hist.len() > MAX_HISTORY_SIZE {
                pnl_hist.pop_front();
            }
        }
        
        {
            let mut cum_pnl = self.cumulative_pnl.write().unwrap();
            *cum_pnl += pnl;
        }
        
        self.update_trade_type_stats(&trade, pnl);
        self.update_ema_states(pnl);
        
        let features = self.extract_features(trade.timestamp, pnl).await?;
        
        {
            let mut cache = self.feature_cache.write().unwrap();
            if cache.len() >= 10000 {
                let oldest_key = cache.keys().min().cloned();
                if let Some(key) = oldest_key {
                    cache.remove(&key);
                }
            }
            cache.insert(trade.timestamp, features.clone());
        }
        
        Ok(features)
    }
    
    fn calculate_trade_pnl(&self, trade: &Trade) -> f64 {
        if !trade.success {
            return -trade.gas_cost;
        }
        
        let gross_pnl = (trade.exit_price - trade.entry_price) * trade.size;
        let slippage_cost = trade.slippage * trade.size * trade.entry_price;
        gross_pnl - slippage_cost - trade.gas_cost
    }
    
    fn update_trade_type_stats(&self, trade: &Trade, pnl: f64) {
        let mut stats = self.trade_type_stats.write().unwrap();
        let entry = stats.entry(trade.trade_type.clone()).or_insert(TradeTypeStats {
            success_count: 0,
            total_count: 0,
            total_pnl: 0.0,
        });
        
        entry.total_count += 1;
        if trade.success {
            entry.success_count += 1;
        }
        entry.total_pnl += pnl;
    }
    
    fn update_ema_states(&self, pnl: f64) {
        let mut states = self.ema_states.lock();
        
        const ALPHA_12: f64 = 2.0 / 13.0;
        const ALPHA_26: f64 = 2.0 / 27.0;
        const ALPHA_SIGNAL: f64 = 2.0 / 10.0;
        
        states.ema_12 = if states.period_count == 0 {
            pnl
        } else {
            ALPHA_12 * pnl + (1.0 - ALPHA_12) * states.ema_12
        };
        
        states.ema_26 = if states.period_count == 0 {
            pnl
        } else {
            ALPHA_26 * pnl + (1.0 - ALPHA_26) * states.ema_26
        };
        
        let macd = states.ema_12 - states.ema_26;
        
        states.signal = if states.period_count < 9 {
            macd
        } else {
            ALPHA_SIGNAL * macd + (1.0 - ALPHA_SIGNAL) * states.signal
        };
        
        if pnl > 0.0 {
            states.gain_sum = states.gain_sum * 13.0 / 14.0 + pnl / 14.0;
        } else {
            states.loss_sum = states.loss_sum * 13.0 / 14.0 + pnl.abs() / 14.0;
        }
        
        states.period_count += 1;
    }
    
    async fn extract_features(&self, timestamp: u64, instant_pnl: f64) -> Result<PnLFeatures, Box<dyn std::error::Error + Send + Sync>> {
        let pnl_history = self.pnl_history.lock().clone();
        let trade_history = self.trade_history.lock().clone();
        let cumulative_pnl = *self.cumulative_pnl.read().unwrap();
        
        let mut rolling_mean = HashMap::new();
        let mut rolling_std = HashMap::new();
        let mut sharpe_ratio = HashMap::new();
        let mut sortino_ratio = HashMap::new();
        let mut max_drawdown = HashMap::new();
        let mut win_rate = HashMap::new();
        let mut profit_factor = HashMap::new();
        let mut avg_win_size = HashMap::new();
        let mut avg_loss_size = HashMap::new();
        let mut trade_frequency = HashMap::new();
        
        for &window in &FEATURE_WINDOW_SIZES {
            if pnl_history.len() >= window {
                let window_pnls: Vec<f64> = pnl_history.iter()
                    .rev()
                    .take(window)
                    .copied()
                    .collect();
                
                let mean = Self::calculate_mean(&window_pnls);
                let std = Self::calculate_std(&window_pnls, mean);
                
                rolling_mean.insert(window, mean);
                rolling_std.insert(window, std);
                
                let sharpe = Self::calculate_sharpe_ratio(mean, std);
                sharpe_ratio.insert(window, sharpe);
                
                let sortino = Self::calculate_sortino_ratio(&window_pnls, mean);
                sortino_ratio.insert(window, sortino);
                
                let mdd = Self::calculate_max_drawdown(&window_pnls);
                max_drawdown.insert(window, mdd);
                
                let (wr, pf, aws, als) = Self::calculate_win_loss_metrics(&window_pnls);
                win_rate.insert(window, wr);
                profit_factor.insert(window, pf);
                avg_win_size.insert(window, aws);
                avg_loss_size.insert(window, als);
                
                let freq = Self::calculate_trade_frequency(&trade_history, window);
                trade_frequency.insert(window, freq);
            }
        }
        
        let success_rate_by_type = self.calculate_success_rates();
        let pnl_by_type = self.calculate_pnl_by_type();
        let gas_efficiency = Self::calculate_gas_efficiency(&trade_history);
        let slippage_impact = Self::calculate_slippage_impact(&trade_history);
        let latency_percentiles = Self::calculate_latency_percentiles(&trade_history);
        let volatility_regime = Self::determine_volatility_regime(&rolling_std);
        let momentum_indicators = self.calculate_momentum_indicators();
        
        Ok(PnLFeatures {
            timestamp,
            instant_pnl,
            cumulative_pnl,
            rolling_mean,
            rolling_std,
            sharpe_ratio,
            sortino_ratio,
            max_drawdown,
            win_rate,
            profit_factor,
            avg_win_size,
            avg_loss_size,
            trade_frequency,
            success_rate_by_type,
            pnl_by_type,
            gas_efficiency,
            slippage_impact,
            latency_percentiles,
            volatility_regime,
            momentum_indicators,
        })
    }
    
    fn calculate_success_rates(&self) -> HashMap<TradeType, f64> {
        let stats = self.trade_type_stats.read().unwrap();
        let mut rates = HashMap::new();
        
        for (trade_type, stat) in stats.iter() {
            let rate = if stat.total_count > 0 {
                stat.success_count as f64 / stat.total_count as f64
            } else {
                0.0
            };
            rates.insert(trade_type.clone(), rate);
        }
        
        rates
    }
    
    fn calculate_pnl_by_type(&self) -> HashMap<TradeType, f64> {
        let stats = self.trade_type_stats.read().unwrap();
        let mut pnls = HashMap::new();
        
        for (trade_type, stat) in stats.iter() {
            pnls.insert(trade_type.clone(), stat.total_pnl);
        }
        
        pnls
    }
    
    fn calculate_gas_efficiency(trades: &VecDeque<Trade>) -> f64 {
        if trades.is_empty() {
            return 0.0;
        }
        
               let total_gas: f64 = trades.iter().map(|t| t.gas_cost).sum();
        let profitable_trades: f64 = trades.iter()
            .filter(|t| t.success && (t.exit_price - t.entry_price) * t.size > t.gas_cost + t.slippage * t.size * t.entry_price)
            .map(|t| (t.exit_price - t.entry_price) * t.size - t.slippage * t.size * t.entry_price)
            .sum();
        
        if total_gas > EPSILON {
            profitable_trades / total_gas
        } else {
            0.0
        }
    }
    
    fn calculate_slippage_impact(trades: &VecDeque<Trade>) -> f64 {
        if trades.is_empty() {
            return 0.0;
        }
        
        let total_slippage: f64 = trades.iter()
            .map(|t| t.slippage * t.size * t.entry_price)
            .sum();
        
        let total_volume: f64 = trades.iter()
            .map(|t| t.size * t.entry_price)
            .sum();
        
        if total_volume > EPSILON {
            total_slippage / total_volume
        } else {
            0.0
        }
    }
    
    fn calculate_latency_percentiles(trades: &VecDeque<Trade>) -> LatencyPercentiles {
        if trades.is_empty() {
            return LatencyPercentiles {
                p50: 0.0,
                p90: 0.0,
                p95: 0.0,
                p99: 0.0,
            };
        }
        
        let mut latencies: Vec<f64> = trades.iter()
            .map(|t| t.latency_ms as f64)
            .collect();
        
        latencies.sort_by_key(|&x| OrderedFloat(x));
        
        LatencyPercentiles {
            p50: Self::percentile(&latencies, 0.50),
            p90: Self::percentile(&latencies, 0.90),
            p95: Self::percentile(&latencies, 0.95),
            p99: Self::percentile(&latencies, 0.99),
        }
    }
    
    fn determine_volatility_regime(rolling_std: &HashMap<usize, f64>) -> VolatilityRegime {
        let std_50 = rolling_std.get(&50).copied().unwrap_or(0.0);
        let std_100 = rolling_std.get(&100).copied().unwrap_or(0.0);
        
        let avg_std = (std_50 + std_100) / 2.0;
        
        if avg_std < 0.001 {
            VolatilityRegime::Low
        } else if avg_std < 0.005 {
            VolatilityRegime::Medium
        } else if avg_std < 0.02 {
            VolatilityRegime::High
        } else {
            VolatilityRegime::Extreme
        }
    }
    
    fn calculate_momentum_indicators(&self) -> MomentumIndicators {
        let states = self.ema_states.lock();
        
        let rs = if states.loss_sum > EPSILON {
            states.gain_sum / states.loss_sum
        } else if states.gain_sum > EPSILON {
            100.0
        } else {
            1.0
        };
        
        let rsi = 100.0 - (100.0 / (1.0 + rs));
        
        MomentumIndicators {
            ema_12: states.ema_12,
            ema_26: states.ema_26,
            macd: states.ema_12 - states.ema_26,
            signal: states.signal,
            rsi,
        }
    }
    
    pub async fn get_latest_features(&self) -> Option<PnLFeatures> {
        let cache = self.feature_cache.read().unwrap();
        cache.values()
            .max_by_key(|f| f.timestamp)
            .cloned()
    }
    
    pub fn get_feature_vector(&self, features: &PnLFeatures) -> Vec<f64> {
        let mut vector = Vec::with_capacity(256);
        
        vector.push(features.instant_pnl);
        vector.push(features.cumulative_pnl);
        
        for &window in &FEATURE_WINDOW_SIZES {
            vector.push(features.rolling_mean.get(&window).copied().unwrap_or(0.0));
            vector.push(features.rolling_std.get(&window).copied().unwrap_or(0.0));
            vector.push(features.sharpe_ratio.get(&window).copied().unwrap_or(0.0));
            vector.push(features.sortino_ratio.get(&window).copied().unwrap_or(0.0));
            vector.push(features.max_drawdown.get(&window).copied().unwrap_or(0.0));
            vector.push(features.win_rate.get(&window).copied().unwrap_or(0.0));
            vector.push(features.profit_factor.get(&window).copied().unwrap_or(0.0));
            vector.push(features.avg_win_size.get(&window).copied().unwrap_or(0.0));
            vector.push(features.avg_loss_size.get(&window).copied().unwrap_or(0.0));
            vector.push(features.trade_frequency.get(&window).copied().unwrap_or(0.0));
        }
        
        vector.push(features.gas_efficiency);
        vector.push(features.slippage_impact);
        vector.push(features.latency_percentiles.p50);
        vector.push(features.latency_percentiles.p90);
        vector.push(features.latency_percentiles.p95);
        vector.push(features.latency_percentiles.p99);
        
        vector.push(match features.volatility_regime {
            VolatilityRegime::Low => 0.0,
            VolatilityRegime::Medium => 0.33,
            VolatilityRegime::High => 0.67,
            VolatilityRegime::Extreme => 1.0,
        });
        
        vector.push(features.momentum_indicators.ema_12);
        vector.push(features.momentum_indicators.ema_26);
        vector.push(features.momentum_indicators.macd);
        vector.push(features.momentum_indicators.signal);
        vector.push(features.momentum_indicators.rsi / 100.0);
        
        for trade_type in [TradeType::Arbitrage, TradeType::Liquidation, TradeType::JitLiquidity, TradeType::Sandwich, TradeType::BackRun].iter() {
            vector.push(features.success_rate_by_type.get(trade_type).copied().unwrap_or(0.0));
            vector.push(features.pnl_by_type.get(trade_type).copied().unwrap_or(0.0));
        }
        
        vector
    }
    
    pub fn cleanup_old_data(&self, retention_period: Duration) {
        let cutoff_time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or(Duration::from_secs(0))
            .as_millis() as u64 - retention_period.as_millis() as u64;
        
        {
            let mut trades = self.trade_history.lock();
            trades.retain(|t| t.timestamp > cutoff_time);
        }
        
        {
            let mut cache = self.feature_cache.write().unwrap();
            cache.retain(|&ts, _| ts > cutoff_time);
        }
    }
    
    fn calculate_mean(values: &[f64]) -> f64 {
        if values.is_empty() {
            return 0.0;
        }
        values.iter().sum::<f64>() / values.len() as f64
    }
    
    fn calculate_std(values: &[f64], mean: f64) -> f64 {
        if values.len() < 2 {
            return 0.0;
        }
        
        let variance = values.iter()
            .map(|&x| (x - mean).powi(2))
            .sum::<f64>() / (values.len() - 1) as f64;
        
        variance.sqrt()
    }
    
    fn calculate_sharpe_ratio(mean_return: f64, std_dev: f64) -> f64 {
        if std_dev < EPSILON {
            return 0.0;
        }
        (mean_return - RISK_FREE_RATE) / std_dev
    }
    
    fn calculate_sortino_ratio(returns: &[f64], mean_return: f64) -> f64 {
        let downside_returns: Vec<f64> = returns.iter()
            .filter(|&&r| r < 0.0)
            .copied()
            .collect();
        
        if downside_returns.is_empty() {
            return 100.0;
        }
        
        let downside_deviation = Self::calculate_std(&downside_returns, 0.0);
        
        if downside_deviation < EPSILON {
            return 100.0;
        }
        
        (mean_return - RISK_FREE_RATE) / downside_deviation
    }
    
    fn calculate_max_drawdown(pnls: &[f64]) -> f64 {
        if pnls.is_empty() {
            return 0.0;
        }
        
        let mut cumulative = 0.0;
        let mut peak = 0.0;
        let mut max_dd = 0.0;
        
        for &pnl in pnls {
            cumulative += pnl;
            if cumulative > peak {
                peak = cumulative;
            }
            let drawdown = if peak > EPSILON {
                (peak - cumulative) / peak
            } else {
                0.0
            };
            if drawdown > max_dd {
                max_dd = drawdown;
            }
        }
        
        max_dd
    }
    
    fn calculate_win_loss_metrics(pnls: &[f64]) -> (f64, f64, f64, f64) {
        let wins: Vec<f64> = pnls.iter().filter(|&&p| p > 0.0).copied().collect();
        let losses: Vec<f64> = pnls.iter().filter(|&&p| p < 0.0).map(|&p| p.abs()).collect();
        
        let win_rate = if !pnls.is_empty() {
            wins.len() as f64 / pnls.len() as f64
        } else {
            0.0
        };
        
        let total_wins: f64 = wins.iter().sum();
        let total_losses: f64 = losses.iter().sum();
        
        let profit_factor = if total_losses > EPSILON {
            total_wins / total_losses
        } else if total_wins > 0.0 {
            100.0
        } else {
            0.0
        };
        
        let avg_win = if !wins.is_empty() {
            total_wins / wins.len() as f64
        } else {
            0.0
        };
        
        let avg_loss = if !losses.is_empty() {
            total_losses / losses.len() as f64
        } else {
            0.0
        };
        
        (win_rate, profit_factor, avg_win, avg_loss)
    }
    
    fn calculate_trade_frequency(trades: &VecDeque<Trade>, window_size: usize) -> f64 {
        if trades.is_empty() || window_size == 0 {
            return 0.0;
        }
        
        let recent_trades: Vec<&Trade> = trades.iter()
            .rev()
            .take(window_size.min(trades.len()))
            .collect();
        
        if recent_trades.len() < 2 {
            return 0.0;
        }
        
        let first_timestamp = recent_trades.last().unwrap().timestamp;
        let last_timestamp = recent_trades.first().unwrap().timestamp;
        
        if first_timestamp >= last_timestamp {
            return 0.0;
        }
        
        let time_span = (last_timestamp - first_timestamp) as f64 / 1000.0;
        
        if time_span > EPSILON {
            recent_trades.len() as f64 / time_span
        } else {
            0.0
        }
    }
    
    fn percentile(sorted_values: &[f64], p: f64) -> f64 {
        if sorted_values.is_empty() {
            return 0.0;
        }
        
        if sorted_values.len() == 1 {
            return sorted_values[0];
        }
        
        let index = (p * (sorted_values.len() - 1) as f64).max(0.0);
        let lower = index.floor() as usize;
        let upper = (lower + 1).min(sorted_values.len() - 1);
        let fraction = index - lower as f64;
        
        sorted_values[lower] * (1.0 - fraction) + sorted_values[upper] * fraction
    }
}

impl Default for RealTimePnLFeatureExtractor {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[tokio::test]
    async fn test_pnl_calculation() {
        let extractor = RealTimePnLFeatureExtractor::new();
        
        let trade = Trade {
            timestamp: 1000,
            entry_price: 100.0,
            exit_price: 105.0,
            size: 10.0,
            trade_type: TradeType::Arbitrage,
            gas_cost: 0.5,
            slippage: 0.001,
            latency_ms: 50,
            success: true,
        };
        
        let features = extractor.process_trade(trade).await.unwrap();
        assert!((features.instant_pnl - 48.5).abs() < 0.1);
    }
}

