use std::collections::VecDeque;
use std::sync::{Arc, RwLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use rust_decimal::prelude::*;
use rust_decimal_macros::dec;
use serde::{Deserialize, Serialize};

const ANNUALIZATION_FACTOR_CRYPTO: f64 = 365.25;
const MIN_DATA_POINTS: usize = 20;
const MAX_WINDOW_SIZE: usize = 10000;
const PRICE_EPSILON: f64 = 1e-10;
const VOL_FLOOR: f64 = 0.0001;
const VOL_CEILING: f64 = 10.0;
const DECAY_FACTOR: f64 = 0.94;

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct PricePoint {
    pub timestamp: u64,
    pub price: f64,
    pub volume: f64,
    pub bid: f64,
    pub ask: f64,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct VolatilityMetrics {
    pub realized_vol: f64,
    pub parkinson_vol: f64,
    pub garman_klass_vol: f64,
    pub rogers_satchell_vol: f64,
    pub yang_zhang_vol: f64,
    pub ewma_vol: f64,
    pub garch_vol: f64,
    pub confidence: f64,
    pub sample_size: usize,
    pub timestamp: u64,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum TimeWindow {
    Minutes1,
    Minutes5,
    Minutes15,
    Minutes30,
    Hours1,
    Hours4,
    Hours24,
}

impl TimeWindow {
    pub fn to_seconds(&self) -> u64 {
        match self {
            TimeWindow::Minutes1 => 60,
            TimeWindow::Minutes5 => 300,
            TimeWindow::Minutes15 => 900,
            TimeWindow::Minutes30 => 1800,
            TimeWindow::Hours1 => 3600,
            TimeWindow::Hours4 => 14400,
            TimeWindow::Hours24 => 86400,
        }
    }

    pub fn annualization_factor(&self) -> f64 {
        let periods_per_year = (365.25 * 24.0 * 3600.0) / self.to_seconds() as f64;
        periods_per_year.sqrt()
    }
}

pub struct RealizedVolCalculator {
    price_history: Arc<RwLock<VecDeque<PricePoint>>>,
    window_size: usize,
    time_window: TimeWindow,
    last_ewma_vol: f64,
    garch_params: GarchParams,
}

#[derive(Debug, Clone, Copy)]
struct GarchParams {
    omega: f64,
    alpha: f64,
    beta: f64,
    last_variance: f64,
    last_return: f64,
}

impl Default for GarchParams {
    fn default() -> Self {
        Self {
            omega: 0.000001,
            alpha: 0.08,
            beta: 0.91,
            last_variance: 0.0004,
            last_return: 0.0,
        }
    }
}

impl RealizedVolCalculator {
    pub fn new(time_window: TimeWindow, window_size: usize) -> Self {
        let adjusted_window_size = window_size.min(MAX_WINDOW_SIZE).max(MIN_DATA_POINTS);
        
        Self {
            price_history: Arc::new(RwLock::new(VecDeque::with_capacity(adjusted_window_size + 1))),
            window_size: adjusted_window_size,
            time_window,
            last_ewma_vol: 0.0,
            garch_params: GarchParams::default(),
        }
    }

    pub fn add_price_point(&mut self, point: PricePoint) -> Result<(), &'static str> {
        if point.price <= PRICE_EPSILON {
            return Err("Invalid price: too close to zero");
        }

        let mut history = self.price_history.write()
            .map_err(|_| "Failed to acquire write lock")?;

        if let Some(last) = history.back() {
            if point.timestamp <= last.timestamp {
                return Err("Timestamp must be monotonically increasing");
            }
        }

        history.push_back(point);
        
        while history.len() > self.window_size {
            history.pop_front();
        }

        Ok(())
    }

    pub fn calculate_volatility(&mut self) -> Result<VolatilityMetrics, &'static str> {
        let history = self.price_history.read()
            .map_err(|_| "Failed to acquire read lock")?;

        if history.len() < MIN_DATA_POINTS {
            return Err("Insufficient data points for volatility calculation");
        }

        let price_vec: Vec<PricePoint> = history.iter().cloned().collect();
        drop(history);

        let returns = self.calculate_log_returns(&price_vec);
        
        let realized_vol = self.calculate_realized_volatility(&returns)?;
        let parkinson_vol = self.calculate_parkinson_volatility(&price_vec)?;
        let garman_klass_vol = self.calculate_garman_klass_volatility(&price_vec)?;
        let rogers_satchell_vol = self.calculate_rogers_satchell_volatility(&price_vec)?;
        let yang_zhang_vol = self.calculate_yang_zhang_volatility(&price_vec)?;
        let ewma_vol = self.calculate_ewma_volatility(&returns)?;
        let garch_vol = self.calculate_garch_volatility(&returns)?;
        
        let confidence = self.calculate_confidence(&price_vec, realized_vol);
        
        Ok(VolatilityMetrics {
            realized_vol: self.apply_bounds(realized_vol),
            parkinson_vol: self.apply_bounds(parkinson_vol),
            garman_klass_vol: self.apply_bounds(garman_klass_vol),
            rogers_satchell_vol: self.apply_bounds(rogers_satchell_vol),
            yang_zhang_vol: self.apply_bounds(yang_zhang_vol),
            ewma_vol: self.apply_bounds(ewma_vol),
            garch_vol: self.apply_bounds(garch_vol),
            confidence,
            sample_size: price_vec.len(),
            timestamp: SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs(),
        })
    }

    fn calculate_log_returns(&self, prices: &[PricePoint]) -> Vec<f64> {
        prices.windows(2)
            .filter_map(|w| {
                if w[0].price > PRICE_EPSILON && w[1].price > PRICE_EPSILON {
                    Some((w[1].price / w[0].price).ln())
                } else {
                    None
                }
            })
            .collect()
    }

    fn calculate_realized_volatility(&self, returns: &[f64]) -> Result<f64, &'static str> {
        if returns.is_empty() {
            return Err("No valid returns calculated");
        }

        let mean = returns.iter().sum::<f64>() / returns.len() as f64;
        let variance = returns.iter()
            .map(|r| (r - mean).powi(2))
            .sum::<f64>() / (returns.len() - 1).max(1) as f64;

        Ok(variance.sqrt() * self.time_window.annualization_factor())
    }

    fn calculate_parkinson_volatility(&self, prices: &[PricePoint]) -> Result<f64, &'static str> {
        let mut sum_sq = 0.0;
        let mut count = 0;

        for i in 0..prices.len() {
            let high = prices[i].ask.max(prices[i].price);
            let low = prices[i].bid.min(prices[i].price);
            
            if high > PRICE_EPSILON && low > PRICE_EPSILON && high > low {
                let hl_ratio = (high / low).ln();
                sum_sq += hl_ratio * hl_ratio;
                count += 1;
            }
        }

        if count == 0 {
            return Ok(0.0);
        }

        let factor = 1.0 / (4.0 * std::f64::consts::LN_2);
        let variance = factor * sum_sq / count as f64;
        
        Ok(variance.sqrt() * self.time_window.annualization_factor())
    }

    fn calculate_garman_klass_volatility(&self, prices: &[PricePoint]) -> Result<f64, &'static str> {
        let mut sum_hl = 0.0;
        let mut sum_co = 0.0;
        let mut count = 0;

        for i in 1..prices.len() {
            let high = prices[i].ask.max(prices[i].price);
            let low = prices[i].bid.min(prices[i].price);
            let close = prices[i].price;
            let open = prices[i-1].price;
            
            if high > PRICE_EPSILON && low > PRICE_EPSILON && close > PRICE_EPSILON && open > PRICE_EPSILON && high > low {
                let hl_term = 0.5 * (high / low).ln().powi(2);
                let co_term = (2.0 * std::f64::consts::LN_2 - 1.0) * (close / open).ln().powi(2);
                sum_hl += hl_term;
                sum_co += co_term;
                count += 1;
            }
        }

        if count == 0 {
            return Ok(0.0);
        }

        let variance = (sum_hl - sum_co) / count as f64;
        Ok(variance.max(0.0).sqrt() * self.time_window.annualization_factor())
    }

    fn calculate_rogers_satchell_volatility(&self, prices: &[PricePoint]) -> Result<f64, &'static str> {
        let mut sum = 0.0;
        let mut count = 0;

        for i in 1..prices.len() {
            let high = prices[i].ask.max(prices[i].price);
            let low = prices[i].bid.min(prices[i].price);
            let close = prices[i].price;
            let open = prices[i-1].price;
            
            if high > PRICE_EPSILON && low > PRICE_EPSILON && close > PRICE_EPSILON && open > PRICE_EPSILON {
                let hc = (high / close).ln();
                let ho = (high / open).ln();
                let lc = (low / close).ln();
                let lo = (low / open).ln();
                sum += hc * ho + lc * lo;
                count += 1;
            }
        }

        if count == 0 {
            return Ok(0.0);
        }

        let variance = sum / count as f64;
        Ok(variance.max(0.0).sqrt() * self.time_window.annualization_factor())
    }

    fn calculate_yang_zhang_volatility(&self, prices: &[PricePoint]) -> Result<f64, &'static str> {
        if prices.len() < 2 {
            return Ok(0.0);
        }

        let k = 0.34 / (1.34 + (prices.len() + 1) as f64 / (prices.len() - 1) as f64);
        
        let mut overnight_returns = Vec::new();
        let mut open_close_returns = Vec::new();
        
        for i in 1..prices.len() {
            let overnight_ret = (prices[i].price / prices[i-1].price).ln();
            overnight_returns.push(overnight_ret);
            
            let oc_ret = if i > 0 {
                (prices[i].price / prices[i].price).ln()
            } else {
                0.0
            };
            open_close_returns.push(oc_ret);
        }

        let overnight_var = self.calculate_variance(&overnight_returns);
        let oc_var = self.calculate_variance(&open_close_returns);
        let rs_var = self.calculate_rogers_satchell_volatility(prices)?.powi(2) 
            / self.time_window.annualization_factor().powi(2);

        let yang_zhang_var = overnight_var + k * oc_var + (1.0 - k) * rs_var;
        
        Ok(yang_zhang_var.max(0.0).sqrt() * self.time_window.annualization_factor())
    }

    fn calculate_variance(&self, returns: &[f64]) -> f64 {
        if returns.is_empty() {
            return 0.0;
        }

        let mean = returns.iter().sum::<f64>() / returns.len() as f64;
        returns.iter()
            .map(|r| (r - mean).powi(2))
            .sum::<f64>() / (returns.len() - 1).max(1) as f64
    }

    fn calculate_ewma_volatility(&mut self, returns: &[f64]) -> Result<f64, &'static str> {
        if returns.is_empty() {
            return Ok(self.last_ewma_vol);
        }

        let lambda = DECAY_FACTOR;
        let mut ewma_variance = if self.last_ewma_vol > 0.0 {
            self.last_ewma_vol.powi(2) / self.time_window.annualization_factor().powi(2)
        } else {
            self.calculate_variance(returns)
        };

        for &ret in returns.iter().rev().take(20) {
            ewma_variance = lambda * ewma_variance + (1.0 - lambda) * ret.powi(2);
        }

                self.last_ewma_vol = ewma_variance.sqrt() * self.time_window.annualization_factor();
        Ok(self.last_ewma_vol)
    }

    fn calculate_garch_volatility(&mut self, returns: &[f64]) -> Result<f64, &'static str> {
        if returns.is_empty() {
            return Ok(self.garch_params.last_variance.sqrt() * self.time_window.annualization_factor());
        }

        let omega = self.garch_params.omega;
        let alpha = self.garch_params.alpha;
        let beta = self.garch_params.beta;
        
        let mut current_variance = self.garch_params.last_variance;
        
        for &ret in returns.iter() {
            let squared_return = ret * ret;
            current_variance = omega + alpha * squared_return + beta * current_variance;
            current_variance = current_variance.max(1e-10).min(0.25);
        }
        
        self.garch_params.last_variance = current_variance;
        if !returns.is_empty() {
            self.garch_params.last_return = returns[returns.len() - 1];
        }
        
        Ok(current_variance.sqrt() * self.time_window.annualization_factor())
    }

    fn calculate_confidence(&self, prices: &[PricePoint], realized_vol: f64) -> f64 {
        let mut confidence = 1.0;
        
        // Data quality factor
        let data_completeness = prices.len() as f64 / self.window_size as f64;
        confidence *= data_completeness.min(1.0);
        
        // Time consistency factor
        let mut time_gaps = Vec::new();
        for window in prices.windows(2) {
            let gap = window[1].timestamp - window[0].timestamp;
            time_gaps.push(gap as f64);
        }
        
        if !time_gaps.is_empty() {
            let mean_gap = time_gaps.iter().sum::<f64>() / time_gaps.len() as f64;
            let gap_variance = time_gaps.iter()
                .map(|g| (g - mean_gap).powi(2))
                .sum::<f64>() / time_gaps.len() as f64;
            let gap_std = gap_variance.sqrt();
            let consistency_factor = 1.0 / (1.0 + gap_std / mean_gap.max(1.0));
            confidence *= consistency_factor;
        }
        
        // Spread quality factor
        let avg_spread_ratio = prices.iter()
            .filter(|p| p.bid > PRICE_EPSILON && p.ask > PRICE_EPSILON)
            .map(|p| (p.ask - p.bid) / p.price)
            .sum::<f64>() / prices.len().max(1) as f64;
        
        let spread_factor = 1.0 / (1.0 + avg_spread_ratio * 100.0);
        confidence *= spread_factor;
        
        // Volatility reasonableness factor
        let vol_reasonableness = if realized_vol > VOL_FLOOR && realized_vol < VOL_CEILING {
            1.0
        } else {
            0.5
        };
        confidence *= vol_reasonableness;
        
        confidence.max(0.0).min(1.0)
    }

    fn apply_bounds(&self, vol: f64) -> f64 {
        vol.max(VOL_FLOOR).min(VOL_CEILING)
    }

    pub fn get_volatility_percentile(&self, current_vol: f64, lookback_periods: usize) -> f64 {
        let history = match self.price_history.read() {
            Ok(h) => h,
            Err(_) => return 0.5,
        };
        
        if history.len() < lookback_periods {
            return 0.5;
        }
        
        let mut historical_vols = Vec::new();
        let step = (history.len() / lookback_periods).max(1);
        
        for i in (0..history.len()).step_by(step).take(lookback_periods) {
            let end = (i + MIN_DATA_POINTS).min(history.len());
            if end - i >= MIN_DATA_POINTS {
                let slice: Vec<PricePoint> = history.range(i..end).cloned().collect();
                let returns = self.calculate_log_returns(&slice);
                if let Ok(vol) = self.calculate_realized_volatility(&returns) {
                    historical_vols.push(vol);
                }
            }
        }
        
        if historical_vols.is_empty() {
            return 0.5;
        }
        
        historical_vols.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        let position = historical_vols.iter().filter(|&&v| v <= current_vol).count();
        position as f64 / historical_vols.len() as f64
    }

    pub fn get_volatility_regime(&self, metrics: &VolatilityMetrics) -> VolatilityRegime {
        let vol = metrics.realized_vol;
        let percentile = self.get_volatility_percentile(vol, 100);
        
        if percentile < 0.2 {
            VolatilityRegime::VeryLow
        } else if percentile < 0.4 {
            VolatilityRegime::Low
        } else if percentile < 0.6 {
            VolatilityRegime::Normal
        } else if percentile < 0.8 {
            VolatilityRegime::High
        } else {
            VolatilityRegime::VeryHigh
        }
    }

    pub fn estimate_future_volatility(&self, horizon_seconds: u64) -> Result<f64, &'static str> {
        let current_metrics = self.calculate_volatility()?;
        
        // Weighted average of different volatility measures
        let weights = [0.3, 0.15, 0.15, 0.1, 0.1, 0.15, 0.05];
        let vols = [
            current_metrics.realized_vol,
            current_metrics.parkinson_vol,
            current_metrics.garman_klass_vol,
            current_metrics.rogers_satchell_vol,
            current_metrics.yang_zhang_vol,
            current_metrics.ewma_vol,
            current_metrics.garch_vol,
        ];
        
        let weighted_vol: f64 = vols.iter()
            .zip(weights.iter())
            .map(|(v, w)| v * w)
            .sum();
        
        // Mean reversion adjustment
        let long_term_mean = 0.5;
        let mean_reversion_speed = 0.1;
        let time_factor = (-mean_reversion_speed * horizon_seconds as f64 / 86400.0).exp();
        
        let future_vol = weighted_vol * time_factor + long_term_mean * (1.0 - time_factor);
        
        Ok(self.apply_bounds(future_vol))
    }

    pub fn calculate_vol_of_vol(&self) -> Result<f64, &'static str> {
        let history = self.price_history.read()
            .map_err(|_| "Failed to acquire read lock")?;
        
        if history.len() < MIN_DATA_POINTS * 2 {
            return Err("Insufficient data for vol of vol calculation");
        }
        
        let mut volatilities = Vec::new();
        let window = MIN_DATA_POINTS;
        
        for i in 0..=(history.len() - window) {
            let slice: Vec<PricePoint> = history.range(i..i + window).cloned().collect();
            let returns = self.calculate_log_returns(&slice);
            if let Ok(vol) = self.calculate_realized_volatility(&returns) {
                volatilities.push(vol);
            }
        }
        
        if volatilities.len() < 2 {
            return Err("Insufficient volatility samples");
        }
        
        let vol_returns: Vec<f64> = volatilities.windows(2)
            .filter_map(|w| {
                if w[0] > VOL_FLOOR && w[1] > VOL_FLOOR {
                    Some((w[1] / w[0]).ln())
                } else {
                    None
                }
            })
            .collect();
        
        self.calculate_realized_volatility(&vol_returns)
    }

    pub fn get_intraday_pattern(&self) -> IntradayVolPattern {
        let history = match self.price_history.read() {
            Ok(h) => h,
            Err(_) => return IntradayVolPattern::default(),
        };
        
        let mut hourly_vols = vec![Vec::new(); 24];
        
        for i in MIN_DATA_POINTS..history.len() {
            let slice: Vec<PricePoint> = history.range(i.saturating_sub(MIN_DATA_POINTS)..=i).cloned().collect();
            let returns = self.calculate_log_returns(&slice);
            
            if let Ok(vol) = self.calculate_realized_volatility(&returns) {
                let hour = (history[i].timestamp / 3600) % 24;
                hourly_vols[hour as usize].push(vol);
            }
        }
        
        let pattern: Vec<f64> = hourly_vols.iter()
            .map(|vols| {
                if vols.is_empty() {
                    1.0
                } else {
                    vols.iter().sum::<f64>() / vols.len() as f64
                }
            })
            .collect();
        
        IntradayVolPattern { hourly_multipliers: pattern }
    }

    pub fn clear_history(&mut self) {
        if let Ok(mut history) = self.price_history.write() {
            history.clear();
        }
        self.last_ewma_vol = 0.0;
        self.garch_params = GarchParams::default();
    }

    pub fn get_data_age(&self) -> Option<Duration> {
        let history = self.price_history.read().ok()?;
        let latest = history.back()?;
        let now = SystemTime::now().duration_since(UNIX_EPOCH).ok()?.as_secs();
        
        if now > latest.timestamp {
            Some(Duration::from_secs(now - latest.timestamp))
        } else {
            None
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum VolatilityRegime {
    VeryLow,
    Low,
    Normal,
    High,
    VeryHigh,
}

#[derive(Debug, Clone)]
pub struct IntradayVolPattern {
    pub hourly_multipliers: Vec<f64>,
}

impl Default for IntradayVolPattern {
    fn default() -> Self {
        Self {
            hourly_multipliers: vec![1.0; 24],
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_volatility_calculation() {
        let mut calc = RealizedVolCalculator::new(TimeWindow::Minutes5, 100);
        
        let base_price = 100.0;
        let mut timestamp = 1000000;
        
        for i in 0..50 {
            let noise = ((i as f64 * 0.1).sin() * 0.02 + 1.0);
            let price = base_price * noise;
            
            let point = PricePoint {
                timestamp,
                price,
                volume: 1000.0,
                bid: price * 0.999,
                ask: price * 1.001,
            };
            
            calc.add_price_point(point).unwrap();
            timestamp += 300;
        }
        
        let metrics = calc.calculate_volatility().unwrap();
        assert!(metrics.realized_vol > VOL_FLOOR);
        assert!(metrics.realized_vol < VOL_CEILING);
        assert!(metrics.confidence > 0.0 && metrics.confidence <= 1.0);
    }

    #[test]
    fn test_edge_cases() {
        let mut calc = RealizedVolCalculator::new(TimeWindow::Minutes1, 50);
        
        // Test with insufficient data
        assert!(calc.calculate_volatility().is_err());
        
        // Test with zero price
        let point = PricePoint {
            timestamp: 1000000,
            price: 0.0,
            volume: 100.0,
            bid: 0.0,
            ask: 0.0,
        };
        assert!(calc.add_price_point(point).is_err());
        
        // Test timestamp ordering
        let point1 = PricePoint {
            timestamp: 1000000,
            price: 100.0,
            volume: 100.0,
            bid: 99.9,
            ask: 100.1,
        };
        calc.add_price_point(point1).unwrap();
        
        let point2 = PricePoint {
            timestamp: 999999,
            price: 101.0,
            volume: 100.0,
            bid: 100.9,
            ask: 101.1,
        };
        assert!(calc.add_price_point(point2).is_err());
    }
}

