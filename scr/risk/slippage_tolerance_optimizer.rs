use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};
use solana_sdk::pubkey::Pubkey;
use solana_sdk::clock::UnixTimestamp;
use rust_decimal::prelude::*;
use rust_decimal::Decimal;
use anyhow::{Result, Context};
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock as AsyncRwLock;
use statistical::{mean, standard_deviation, median};

const MAX_HISTORICAL_TRADES: usize = 10000;
const VOLATILITY_WINDOW_SIZE: usize = 100;
const SLIPPAGE_ADJUSTMENT_FACTOR: f64 = 0.15;
const MIN_SLIPPAGE_BPS: u64 = 10; // 0.1%
const MAX_SLIPPAGE_BPS: u64 = 500; // 5%
const COMPETITION_DETECTION_WINDOW: Duration = Duration::from_millis(500);
const FAILURE_RATE_THRESHOLD: f64 = 0.4;
const SUCCESS_RATE_BOOST_FACTOR: f64 = 1.05;
const FAILURE_PENALTY_FACTOR: f64 = 0.92;
const VOLATILITY_SMOOTHING_ALPHA: f64 = 0.3;
const MARKET_DEPTH_IMPACT_THRESHOLD: f64 = 0.02;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TradeResult {
    pub timestamp: UnixTimestamp,
    pub token_pair: (Pubkey, Pubkey),
    pub amount_in: u64,
    pub expected_out: u64,
    pub actual_out: u64,
    pub slippage_bps: u64,
    pub success: bool,
    pub market_volatility: f64,
    pub competition_detected: bool,
    pub execution_time_ms: u64,
}

#[derive(Debug, Clone)]
pub struct MarketMetrics {
    pub recent_price_volatility: f64,
    pub bid_ask_spread_bps: u64,
    pub liquidity_depth: u64,
    pub recent_volume: u64,
    pub competition_intensity: f64,
}

#[derive(Debug, Clone)]
pub struct OptimizationParams {
    pub base_slippage_bps: u64,
    pub volatility_multiplier: f64,
    pub competition_multiplier: f64,
    pub size_impact_multiplier: f64,
    pub success_rate_target: f64,
}

impl Default for OptimizationParams {
    fn default() -> Self {
        Self {
            base_slippage_bps: 50,
            volatility_multiplier: 2.5,
            competition_multiplier: 1.8,
            size_impact_multiplier: 1.5,
            success_rate_target: 0.85,
        }
    }
}

pub struct SlippageToleranceOptimizer {
    historical_trades: Arc<RwLock<VecDeque<TradeResult>>>,
    market_metrics_cache: Arc<AsyncRwLock<HashMap<(Pubkey, Pubkey), MarketMetrics>>>,
    optimization_params: Arc<RwLock<OptimizationParams>>,
    volatility_ema: Arc<RwLock<HashMap<(Pubkey, Pubkey), f64>>>,
    success_rate_tracker: Arc<RwLock<HashMap<(Pubkey, Pubkey), (u64, u64)>>>,
    competition_tracker: Arc<RwLock<HashMap<(Pubkey, Pubkey), VecDeque<Instant>>>>,
}

impl SlippageToleranceOptimizer {
    pub fn new() -> Self {
        Self {
            historical_trades: Arc::new(RwLock::new(VecDeque::with_capacity(MAX_HISTORICAL_TRADES))),
            market_metrics_cache: Arc::new(AsyncRwLock::new(HashMap::new())),
            optimization_params: Arc::new(RwLock::new(OptimizationParams::default())),
            volatility_ema: Arc::new(RwLock::new(HashMap::new())),
            success_rate_tracker: Arc::new(RwLock::new(HashMap::new())),
            competition_tracker: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub async fn calculate_optimal_slippage(
        &self,
        token_in: &Pubkey,
        token_out: &Pubkey,
        amount_in: u64,
        current_price: Decimal,
        liquidity_depth: u64,
    ) -> Result<u64> {
        let token_pair = (*token_in, *token_out);
        
        // Get market metrics
        let market_metrics = self.get_or_calculate_market_metrics(&token_pair, current_price, liquidity_depth).await?;
        
        // Calculate base slippage from optimization params
        let params = self.optimization_params.read()
            .map_err(|e| anyhow::anyhow!("Failed to read optimization params: {}", e))?;
        
        let mut slippage_bps = params.base_slippage_bps;
        
        // Adjust for volatility
        let volatility_adjustment = self.calculate_volatility_adjustment(&market_metrics, &params);
        slippage_bps = ((slippage_bps as f64) * volatility_adjustment) as u64;
        
        // Adjust for competition
        let competition_adjustment = self.calculate_competition_adjustment(&token_pair, &market_metrics, &params)?;
        slippage_bps = ((slippage_bps as f64) * competition_adjustment) as u64;
        
        // Adjust for trade size impact
        let size_adjustment = self.calculate_size_impact(amount_in, liquidity_depth, &params);
        slippage_bps = ((slippage_bps as f64) * size_adjustment) as u64;
        
        // Adjust based on historical success rate
        let success_adjustment = self.calculate_success_rate_adjustment(&token_pair)?;
        slippage_bps = ((slippage_bps as f64) * success_adjustment) as u64;
        
        // Apply bounds
        slippage_bps = slippage_bps.max(MIN_SLIPPAGE_BPS).min(MAX_SLIPPAGE_BPS);
        
        Ok(slippage_bps)
    }

    pub async fn record_trade_result(&self, result: TradeResult) -> Result<()> {
        let token_pair = result.token_pair;
        
        // Update historical trades
        {
            let mut trades = self.historical_trades.write()
                .map_err(|e| anyhow::anyhow!("Failed to write historical trades: {}", e))?;
            
            if trades.len() >= MAX_HISTORICAL_TRADES {
                trades.pop_front();
            }
            trades.push_back(result.clone());
        }
        
        // Update success rate tracker
        {
            let mut tracker = self.success_rate_tracker.write()
                .map_err(|e| anyhow::anyhow!("Failed to write success rate tracker: {}", e))?;
            
            let (successes, total) = tracker.entry(token_pair).or_insert((0, 0));
            *total += 1;
            if result.success {
                *successes += 1;
            }
        }
        
        // Update competition tracker if competition detected
        if result.competition_detected {
            let mut competition = self.competition_tracker.write()
                .map_err(|e| anyhow::anyhow!("Failed to write competition tracker: {}", e))?;
            
            let tracker = competition.entry(token_pair).or_insert_with(VecDeque::new);
            tracker.push_back(Instant::now());
            
            // Clean old entries
            let cutoff = Instant::now() - COMPETITION_DETECTION_WINDOW;
            while let Some(front) = tracker.front() {
                if *front < cutoff {
                    tracker.pop_front();
                } else {
                    break;
                }
            }
        }
        
        // Update volatility EMA
        self.update_volatility_ema(&token_pair, result.market_volatility)?;
        
        // Adapt optimization parameters if needed
        self.adapt_parameters().await?;
        
        Ok(())
    }

    async fn get_or_calculate_market_metrics(
        &self,
        token_pair: &(Pubkey, Pubkey),
        current_price: Decimal,
        liquidity_depth: u64,
    ) -> Result<MarketMetrics> {
        let cache = self.market_metrics_cache.read().await;
        
        if let Some(metrics) = cache.get(token_pair) {
            return Ok(metrics.clone());
        }
        drop(cache);
        
        // Calculate fresh metrics
        let volatility = self.calculate_current_volatility(token_pair)?;
        let competition = self.calculate_competition_intensity(token_pair)?;
        
        let metrics = MarketMetrics {
            recent_price_volatility: volatility,
            bid_ask_spread_bps: self.estimate_spread_from_volatility(volatility),
            liquidity_depth,
            recent_volume: self.estimate_recent_volume(token_pair)?,
            competition_intensity: competition,
        };
        
        let mut cache = self.market_metrics_cache.write().await;
        cache.insert(*token_pair, metrics.clone());
        
        Ok(metrics)
    }

    fn calculate_volatility_adjustment(&self, metrics: &MarketMetrics, params: &OptimizationParams) -> f64 {
        let normalized_volatility = (metrics.recent_price_volatility / 0.02).min(3.0);
        1.0 + (normalized_volatility * params.volatility_multiplier * SLIPPAGE_ADJUSTMENT_FACTOR)
    }

    fn calculate_competition_adjustment(
        &self,
        token_pair: &(Pubkey, Pubkey),
        metrics: &MarketMetrics,
        params: &OptimizationParams,
    ) -> Result<f64> {
        let competition_factor = metrics.competition_intensity.min(1.0);
        Ok(1.0 + (competition_factor * params.competition_multiplier * SLIPPAGE_ADJUSTMENT_FACTOR))
    }

    fn calculate_size_impact(&self, amount_in: u64, liquidity_depth: u64, params: &OptimizationParams) -> f64 {
        if liquidity_depth == 0 {
            return params.size_impact_multiplier;
        }
        
        let impact_ratio = (amount_in as f64) / (liquidity_depth as f64);
        if impact_ratio > MARKET_DEPTH_IMPACT_THRESHOLD {
            1.0 + ((impact_ratio / MARKET_DEPTH_IMPACT_THRESHOLD).ln() * params.size_impact_multiplier * SLIPPAGE_ADJUSTMENT_FACTOR)
        } else {
            1.0
        }
    }

    fn calculate_success_rate_adjustment(&self, token_pair: &(Pubkey, Pubkey)) -> Result<f64> {
        let tracker = self.success_rate_tracker.read()
            .map_err(|e| anyhow::anyhow!("Failed to read success rate tracker: {}", e))?;
        
        if let Some((successes, total)) = tracker.get(token_pair) {
            if *total > 10 {
                let success_rate = (*successes as f64) / (*total as f64);
                if success_rate < FAILURE_RATE_THRESHOLD {
                    return Ok(1.0 / FAILURE_PENALTY_FACTOR);
                } else if success_rate > 0.9 {
                    return Ok(SUCCESS_RATE_BOOST_FACTOR);
                }
            }
        }
        
        Ok(1.0)
    }

    fn calculate_current_volatility(&self, token_pair: &(Pubkey, Pubkey)) -> Result<f64> {
        let trades = self.historical_trades.read()
            .map_err(|e| anyhow::anyhow!("Failed to read historical trades: {}", e))?;
        
        let recent_trades: Vec<&TradeResult> = trades.iter()
            .filter(|t| t.token_pair == *token_pair)
            .take(VOLATILITY_WINDOW_SIZE)
            .collect();
        
        if recent_trades.len() < 5 {
            return Ok(0.02); // Default volatility
        }
        
        let price_changes: Vec<f64> = recent_trades.windows(2)
            .map(|w| {
                let price1 = (w[0].actual_out as f64) / (w[0].amount_in as f64);
                let price2 = (w[1].actual_out as f64) / (w[1].amount_in as f64);
                ((price2 - price1) / price1).abs()
            })
            .collect();
        
        if price_changes.is_empty() {
            return Ok(0.02);
        }
        
        Ok(standard_deviation(&price_changes).unwrap_or(0.02))
    }

    fn calculate_competition_intensity(&self, token_pair: &(Pubkey, Pubkey)) -> Result<f64> {
        let competition = self.competition_tracker.read()
            .map_err(|e| anyhow::anyhow!("Failed to read competition tracker: {}", e))?;
        
        if let Some(tracker) = competition.get(token_pair) {
            let intensity = (tracker.len() as f64) / 10.0;
            Ok(intensity.min(1.0))
        } else {
            Ok(0.0)
        }
    }

    fn estimate_spread_from_volatility(&self, volatility: f64) -> u64 {
        ((volatility * 10000.0) as u64).max(5).min(100)
    }

    fn estimate_recent_volume(&self, token_pair: &(Pubkey, Pubkey)) -> Result<u64> {
        let trades = self.historical_trades.read()
            .map_err(|e| anyhow::anyhow!("Failed to read historical trades: {}", e))?;
        
        let volume: u64 = trades.iter()
            use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};
use solana_sdk::pubkey::Pubkey;
use solana_sdk::clock::UnixTimestamp;
use rust_decimal::prelude::*;
use rust_decimal::Decimal;
use anyhow::{Result, Context};
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock as AsyncRwLock;

const MAX_HISTORICAL_TRADES: usize = 10000;
const VOLATILITY_WINDOW_SIZE: usize = 100;
const SLIPPAGE_ADJUSTMENT_FACTOR: f64 = 0.15;
const MIN_SLIPPAGE_BPS: u64 = 10; // 0.1%
const MAX_SLIPPAGE_BPS: u64 = 500; // 5%
const COMPETITION_DETECTION_WINDOW: Duration = Duration::from_millis(500);
const FAILURE_RATE_THRESHOLD: f64 = 0.4;
const SUCCESS_RATE_BOOST_FACTOR: f64 = 1.05;
const FAILURE_PENALTY_FACTOR: f64 = 0.92;
const VOLATILITY_SMOOTHING_ALPHA: f64 = 0.3;
const MARKET_DEPTH_IMPACT_THRESHOLD: f64 = 0.02;
const PRICE_PRECISION: u32 = 9;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TradeResult {
    pub timestamp: UnixTimestamp,
    pub token_pair: (Pubkey, Pubkey),
    pub amount_in: u64,
    pub expected_out: u64,
    pub actual_out: u64,
    pub slippage_bps: u64,
    pub success: bool,
    pub market_volatility: f64,
    pub competition_detected: bool,
    pub execution_time_ms: u64,
}

#[derive(Debug, Clone)]
pub struct MarketMetrics {
    pub recent_price_volatility: f64,
    pub bid_ask_spread_bps: u64,
    pub liquidity_depth: u64,
    pub recent_volume: u64,
    pub competition_intensity: f64,
}

#[derive(Debug, Clone)]
pub struct OptimizationParams {
    pub base_slippage_bps: u64,
    pub volatility_multiplier: f64,
    pub competition_multiplier: f64,
    pub size_impact_multiplier: f64,
    pub success_rate_target: f64,
}

impl Default for OptimizationParams {
    fn default() -> Self {
        Self {
            base_slippage_bps: 50,
            volatility_multiplier: 2.5,
            competition_multiplier: 1.8,
            size_impact_multiplier: 1.5,
            success_rate_target: 0.85,
        }
    }
}

pub struct SlippageToleranceOptimizer {
    historical_trades: Arc<RwLock<VecDeque<TradeResult>>>,
    market_metrics_cache: Arc<AsyncRwLock<HashMap<(Pubkey, Pubkey), MarketMetrics>>>,
    optimization_params: Arc<RwLock<OptimizationParams>>,
    volatility_ema: Arc<RwLock<HashMap<(Pubkey, Pubkey), f64>>>,
    success_rate_tracker: Arc<RwLock<HashMap<(Pubkey, Pubkey), (u64, u64)>>>,
    competition_tracker: Arc<RwLock<HashMap<(Pubkey, Pubkey), VecDeque<Instant>>>>,
    price_history: Arc<RwLock<HashMap<(Pubkey, Pubkey), VecDeque<(UnixTimestamp, Decimal)>>>>,
}

impl SlippageToleranceOptimizer {
    pub fn new() -> Self {
        Self {
            historical_trades: Arc::new(RwLock::new(VecDeque::with_capacity(MAX_HISTORICAL_TRADES))),
            market_metrics_cache: Arc::new(AsyncRwLock::new(HashMap::new())),
            optimization_params: Arc::new(RwLock::new(OptimizationParams::default())),
            volatility_ema: Arc::new(RwLock::new(HashMap::new())),
            success_rate_tracker: Arc::new(RwLock::new(HashMap::new())),
            competition_tracker: Arc::new(RwLock::new(HashMap::new())),
            price_history: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub async fn calculate_optimal_slippage(
        &self,
        token_in: &Pubkey,
        token_out: &Pubkey,
        amount_in: u64,
        current_price: Decimal,
        liquidity_depth: u64,
    ) -> Result<u64> {
        let token_pair = (*token_in, *token_out);
        
        // Record current price
        self.record_price(&token_pair, current_price)?;
        
        // Get market metrics
        let market_metrics = self.get_or_calculate_market_metrics(&token_pair, current_price, liquidity_depth).await?;
        
        // Calculate base slippage from optimization params
        let params = self.optimization_params.read()
            .map_err(|e| anyhow::anyhow!("Failed to read optimization params: {}", e))?;
        
        let mut slippage_bps = params.base_slippage_bps;
        
        // Adjust for volatility
        let volatility_adjustment = self.calculate_volatility_adjustment(&market_metrics, &params);
        slippage_bps = ((slippage_bps as f64) * volatility_adjustment) as u64;
        
        // Adjust for competition
        let competition_adjustment = self.calculate_competition_adjustment(&token_pair, &market_metrics, &params)?;
        slippage_bps = ((slippage_bps as f64) * competition_adjustment) as u64;
        
        // Adjust for trade size impact
        let size_adjustment = self.calculate_size_impact(amount_in, liquidity_depth, &params);
        slippage_bps = ((slippage_bps as f64) * size_adjustment) as u64;
        
        // Adjust based on historical success rate
        let success_adjustment = self.calculate_success_rate_adjustment(&token_pair)?;
        slippage_bps = ((slippage_bps as f64) * success_adjustment) as u64;
        
        // Apply bounds
        slippage_bps = slippage_bps.max(MIN_SLIPPAGE_BPS).min(MAX_SLIPPAGE_BPS);
        
        Ok(slippage_bps)
    }

    pub async fn record_trade_result(&self, result: TradeResult) -> Result<()> {
        let token_pair = result.token_pair;
        
        // Update historical trades
        {
            let mut trades = self.historical_trades.write()
                .map_err(|e| anyhow::anyhow!("Failed to write historical trades: {}", e))?;
            
            if trades.len() >= MAX_HISTORICAL_TRADES {
                trades.pop_front();
            }
            trades.push_back(result.clone());
        }
        
        // Update success rate tracker
        {
            let mut tracker = self.success_rate_tracker.write()
                .map_err(|e| anyhow::anyhow!("Failed to write success rate tracker: {}", e))?;
            
            let (successes, total) = tracker.entry(token_pair).or_insert((0, 0));
            *total += 1;
            if result.success {
                *successes += 1;
            }
        }
        
        // Update competition tracker if competition detected
        if result.competition_detected {
            let mut competition = self.competition_tracker.write()
                .map_err(|e| anyhow::anyhow!("Failed to write competition tracker: {}", e))?;
            
            let tracker = competition.entry(token_pair).or_insert_with(VecDeque::new);
            tracker.push_back(Instant::now());
            
            // Clean old entries
            let cutoff = Instant::now() - COMPETITION_DETECTION_WINDOW;
            while let Some(front) = tracker.front() {
                if *front < cutoff {
                    tracker.pop_front();
                } else {
                    break;
                }
            }
        }
        
        // Update volatility EMA
        self.update_volatility_ema(&token_pair, result.market_volatility)?;
        
        // Adapt optimization parameters if needed
        self.adapt_parameters().await?;
        
        Ok(())
    }

    fn record_price(&self, token_pair: &(Pubkey, Pubkey), price: Decimal) -> Result<()> {
        let mut price_history = self.price_history.write()
            .map_err(|e| anyhow::anyhow!("Failed to write price history: {}", e))?;
        
        let history = price_history.entry(*token_pair).or_insert_with(|| VecDeque::with_capacity(VOLATILITY_WINDOW_SIZE));
        
        let timestamp = chrono::Utc::now().timestamp();
        history.push_back((timestamp, price));
        
        // Keep only recent prices
        while history.len() > VOLATILITY_WINDOW_SIZE {
            history.pop_front();
        }
        
        Ok(())
    }

    async fn get_or_calculate_market_metrics(
        &self,
        token_pair: &(Pubkey, Pubkey),
        current_price: Decimal,
        liquidity_depth: u64,
    ) -> Result<MarketMetrics> {
        let cache = self.market_metrics_cache.read().await;
        
        if let Some(metrics) = cache.get(token_pair) {
            return Ok(metrics.clone());
        }
        drop(cache);
        
        // Calculate fresh metrics
        let volatility = self.calculate_current_volatility(token_pair)?;
        let competition = self.calculate_competition_intensity(token_pair)?;
        
        let metrics = MarketMetrics {
            recent_price_volatility: volatility,
            bid_ask_spread_bps: self.estimate_spread_from_volatility(volatility),
            liquidity_depth,
            recent_volume: self.estimate_recent_volume(token_pair)?,
            competition_intensity: competition,
        };
        
        let mut cache = self.market_metrics_cache.write().await;
        cache.insert(*token_pair, metrics.clone());
        
        Ok(metrics)
    }

    fn calculate_volatility_adjustment(&self, metrics: &MarketMetrics, params: &OptimizationParams) -> f64 {
        let normalized_volatility = (metrics.recent_price_volatility / 0.02).min(3.0);
        1.0 + (normalized_volatility * params.volatility_multiplier * SLIPPAGE_ADJUSTMENT_FACTOR)
    }

    fn calculate_competition_adjustment(
        &self,
        token_pair: &(Pubkey, Pubkey),
        metrics: &MarketMetrics,
        params: &OptimizationParams,
    ) -> Result<f64> {
        let competition_factor = metrics.competition_intensity.min(1.0);
        Ok(1.0 + (competition_factor * params.competition_multiplier * SLIPPAGE_ADJUSTMENT_FACTOR))
    }

    fn calculate_size_impact(&self, amount_in: u64, liquidity_depth: u64, params: &OptimizationParams) -> f64 {
        if liquidity_depth == 0 {
            return params.size_impact_multiplier;
        }
        
        let impact_ratio = (amount_in as f64) / (liquidity_depth as f64);
        if impact_ratio > MARKET_DEPTH_IMPACT_THRESHOLD {
            1.0 + ((impact_ratio / MARKET_DEPTH_IMPACT_THRESHOLD).ln() * params.size_impact_multiplier * SLIPPAGE_ADJUSTMENT_FACTOR)
        } else {
            1.0
        }
    }

    fn calculate_success_rate_adjustment(&self, token_pair: &(Pubkey, Pubkey)) -> Result<f64> {
        let tracker = self.success_rate_tracker.read()
            .map_err(|e| anyhow::anyhow!("Failed to read success rate tracker: {}", e))?;
        
        if let Some((successes, total)) = tracker.get(token_pair) {
            if *total > 10 {
                let success_rate = (*successes as f64) / (*total as f64);
                if success_rate < FAILURE_RATE_THRESHOLD {
                    return Ok(1.0 / FAILURE_PENALTY_FACTOR);
                } else if success_rate > 0.9 {
                    return Ok(SUCCESS_RATE_BOOST_FACTOR);
                }
            }
        }
        
        Ok(1.0)
    }

    fn calculate_current_volatility(&self, token_pair: &(Pubkey, Pubkey)) -> Result<f64> {
        let price_history = self.price_history.read()
            .map_err(|e| anyhow::anyhow!("Failed to read price history: {}", e))?;
        
        if let Some(history) = price_history.get(token_pair) {
            if history.len() < 5 {
                return Ok(0.02); // Default volatility
            }
            
            let prices: Vec<f64> = history.iter()
                .map(|(_, p)| p.to_f64().unwrap_or(0.0))
                .collect();
            
            // Calculate returns
            let mut returns = Vec::new();
            for i in 1..prices.len() {
                if prices[i-1] > 0.0 {
                    let return_val = (prices[i] - prices[i-1]) / prices[i-1];
                    returns.push(return_val);
                }
            }
            
            if returns.is_empty() {
                return Ok(0.02);
            }
            
            // Calculate standard deviation
            let mean = returns.iter().sum::<f64>() / returns.len() as f64;
            let variance = returns.iter()
                .map(|r| (r - mean).powi(2))
                .sum::<f64>() / returns.len() as f64;
            let std_dev = variance.sqrt();
            
            Ok(std_dev.max(0.001).min(0.1))
        } else {
            Ok(0.02) // Default volatility
        }
    }

    fn calculate_competition_intensity(&self, token_pair: &(Pubkey, Pubkey)) -> Result<f64> {
                let competition = self.competition_tracker.read()
            .map_err(|e| anyhow::anyhow!("Failed to read competition tracker: {}", e))?;
        
        if let Some(tracker) = competition.get(token_pair) {
            let intensity = (tracker.len() as f64) / 10.0;
            Ok(intensity.min(1.0))
        } else {
            Ok(0.0)
        }
    }

    fn estimate_spread_from_volatility(&self, volatility: f64) -> u64 {
        ((volatility * 10000.0) as u64).max(5).min(100)
    }

    fn estimate_recent_volume(&self, token_pair: &(Pubkey, Pubkey)) -> Result<u64> {
        let trades = self.historical_trades.read()
            .map_err(|e| anyhow::anyhow!("Failed to read historical trades: {}", e))?;
        
        let volume: u64 = trades.iter()
            .filter(|t| t.token_pair == *token_pair)
            .take(100)
            .map(|t| t.amount_in)
            .sum();
        
        Ok(volume)
    }

    fn update_volatility_ema(&self, token_pair: &(Pubkey, Pubkey), new_volatility: f64) -> Result<()> {
        let mut ema_map = self.volatility_ema.write()
            .map_err(|e| anyhow::anyhow!("Failed to write volatility EMA: {}", e))?;
        
        let ema = ema_map.entry(*token_pair).or_insert(new_volatility);
        *ema = (VOLATILITY_SMOOTHING_ALPHA * new_volatility) + ((1.0 - VOLATILITY_SMOOTHING_ALPHA) * *ema);
        
        Ok(())
    }

    async fn adapt_parameters(&self) -> Result<()> {
        let trades = self.historical_trades.read()
            .map_err(|e| anyhow::anyhow!("Failed to read historical trades: {}", e))?;
        
        if trades.len() < 100 {
            return Ok(());
        }
        
        let recent_trades: Vec<TradeResult> = trades.iter()
            .rev()
            .take(500)
            .cloned()
            .collect();
        
        drop(trades);
        
        let success_rate = recent_trades.iter()
            .filter(|t| t.success)
            .count() as f64 / recent_trades.len() as f64;
        
        let avg_execution_time = recent_trades.iter()
            .map(|t| t.execution_time_ms as f64)
            .sum::<f64>() / recent_trades.len() as f64;
        
        let competition_rate = recent_trades.iter()
            .filter(|t| t.competition_detected)
            .count() as f64 / recent_trades.len() as f64;
        
        let mut params = self.optimization_params.write()
            .map_err(|e| anyhow::anyhow!("Failed to write optimization params: {}", e))?;
        
        // Adapt base slippage based on success rate
        if success_rate < params.success_rate_target - 0.05 {
            params.base_slippage_bps = (params.base_slippage_bps as f64 * 1.1) as u64;
        } else if success_rate > params.success_rate_target + 0.05 {
            params.base_slippage_bps = (params.base_slippage_bps as f64 * 0.95) as u64;
        }
        
        // Adapt competition multiplier
        if competition_rate > 0.3 {
            params.competition_multiplier = (params.competition_multiplier * 1.05).min(3.0);
        } else if competition_rate < 0.1 {
            params.competition_multiplier = (params.competition_multiplier * 0.95).max(1.0);
        }
        
        // Adapt volatility multiplier based on market conditions
        let avg_volatility = self.calculate_average_volatility()?;
        if avg_volatility > 0.03 {
            params.volatility_multiplier = (params.volatility_multiplier * 1.02).min(4.0);
        } else if avg_volatility < 0.01 {
            params.volatility_multiplier = (params.volatility_multiplier * 0.98).max(1.5);
        }
        
        // Ensure parameters stay within bounds
        params.base_slippage_bps = params.base_slippage_bps.max(MIN_SLIPPAGE_BPS).min(MAX_SLIPPAGE_BPS / 2);
        
        Ok(())
    }

    fn calculate_average_volatility(&self) -> Result<f64> {
        let volatility_map = self.volatility_ema.read()
            .map_err(|e| anyhow::anyhow!("Failed to read volatility EMA: {}", e))?;
        
        if volatility_map.is_empty() {
            return Ok(0.02);
        }
        
        let avg = volatility_map.values().sum::<f64>() / volatility_map.len() as f64;
        Ok(avg)
    }

    pub async fn get_dynamic_slippage_for_market_conditions(
        &self,
        token_in: &Pubkey,
        token_out: &Pubkey,
        amount_in: u64,
        current_price: Decimal,
        liquidity_depth: u64,
        urgency_factor: f64,
    ) -> Result<u64> {
        let base_slippage = self.calculate_optimal_slippage(
            token_in,
            token_out,
            amount_in,
            current_price,
            liquidity_depth,
        ).await?;
        
        // Apply urgency factor (1.0 = normal, >1.0 = urgent trade)
        let adjusted_slippage = ((base_slippage as f64) * urgency_factor.max(0.5).min(2.0)) as u64;
        
        Ok(adjusted_slippage.max(MIN_SLIPPAGE_BPS).min(MAX_SLIPPAGE_BPS))
    }

    pub async fn analyze_slippage_efficiency(&self, token_pair: (Pubkey, Pubkey)) -> Result<SlippageAnalysis> {
        let trades = self.historical_trades.read()
            .map_err(|e| anyhow::anyhow!("Failed to read historical trades: {}", e))?;
        
        let relevant_trades: Vec<TradeResult> = trades.iter()
            .filter(|t| t.token_pair == token_pair)
            .cloned()
            .collect();
        
        drop(trades);
        
        if relevant_trades.is_empty() {
            return Ok(SlippageAnalysis::default());
        }
        
        let successful_trades: Vec<&TradeResult> = relevant_trades.iter()
            .filter(|t| t.success)
            .collect();
        
        let avg_slippage = relevant_trades.iter()
            .map(|t| t.slippage_bps as f64)
            .sum::<f64>() / relevant_trades.len() as f64;
        
        let actual_slippage_rates: Vec<f64> = successful_trades.iter()
            .filter_map(|t| {
                if t.expected_out > 0 {
                    let expected = t.expected_out as f64;
                    let actual = t.actual_out as f64;
                    Some(((expected - actual).abs() / expected * 10000.0))
                } else {
                    None
                }
            })
            .collect();
        
        let avg_actual_slippage = if !actual_slippage_rates.is_empty() {
            actual_slippage_rates.iter().sum::<f64>() / actual_slippage_rates.len() as f64
        } else {
            0.0
        };
        
        let efficiency_score = if avg_slippage > 0.0 {
            (avg_actual_slippage / avg_slippage).min(1.0)
        } else {
            1.0
        };
        
        Ok(SlippageAnalysis {
            avg_set_slippage_bps: avg_slippage as u64,
            avg_actual_slippage_bps: avg_actual_slippage as u64,
            success_rate: successful_trades.len() as f64 / relevant_trades.len() as f64,
            efficiency_score,
            sample_size: relevant_trades.len(),
        })
    }

    pub fn get_recommended_slippage_bounds(&self, token_pair: &(Pubkey, Pubkey)) -> Result<(u64, u64)> {
        let params = self.optimization_params.read()
            .map_err(|e| anyhow::anyhow!("Failed to read optimization params: {}", e))?;
        
        let volatility = self.volatility_ema.read()
            .map_err(|e| anyhow::anyhow!("Failed to read volatility EMA: {}", e))?
            .get(token_pair)
            .copied()
            .unwrap_or(0.02);
        
        let min_bound = (params.base_slippage_bps as f64 * (1.0 - volatility * 10.0).max(0.5)) as u64;
        let max_bound = (params.base_slippage_bps as f64 * (1.0 + volatility * 20.0).min(3.0)) as u64;
        
        Ok((
            min_bound.max(MIN_SLIPPAGE_BPS),
            max_bound.min(MAX_SLIPPAGE_BPS),
        ))
    }

    pub async fn emergency_slippage_override(&self, multiplier: f64) -> Result<()> {
        let mut params = self.optimization_params.write()
            .map_err(|e| anyhow::anyhow!("Failed to write optimization params: {}", e))?;
        
        let original_base = params.base_slippage_bps;
        params.base_slippage_bps = ((original_base as f64) * multiplier.max(0.5).min(3.0)) as u64;
        params.base_slippage_bps = params.base_slippage_bps.max(MIN_SLIPPAGE_BPS).min(MAX_SLIPPAGE_BPS);
        
        Ok(())
    }

    pub fn reset_pair_statistics(&self, token_pair: &(Pubkey, Pubkey)) -> Result<()> {
        self.success_rate_tracker.write()
            .map_err(|e| anyhow::anyhow!("Failed to write success rate tracker: {}", e))?
            .remove(token_pair);
        
        self.volatility_ema.write()
            .map_err(|e| anyhow::anyhow!("Failed to write volatility EMA: {}", e))?
            .remove(token_pair);
        
        self.competition_tracker.write()
            .map_err(|e| anyhow::anyhow!("Failed to write competition tracker: {}", e))?
            .remove(token_pair);
        
        self.price_history.write()
            .map_err(|e| anyhow::anyhow!("Failed to write price history: {}", e))?
            .remove(token_pair);
        
        Ok(())
    }

    pub async fn get_market_condition_score(&self, token_pair: &(Pubkey, Pubkey)) -> Result<f64> {
        let volatility = self.volatility_ema.read()
            .map_err(|e| anyhow::anyhow!("Failed to read volatility EMA: {}", e))?
            .get(token_pair)
            .copied()
            .unwrap_or(0.02);
        
        let competition = self.calculate_competition_intensity(token_pair)?;
        
        let success_rate = {
            let tracker = self.success_rate_tracker.read()
                .map_err(|e| anyhow::anyhow!("Failed to read success rate tracker: {}", e))?;
            
            if let Some((successes, total)) = tracker.get(token_pair) {
                if *total > 0 {
                    (*successes as f64) / (*total as f64)
                } else {
                    0.5
                }
            } else {
                0.5
            }
        };
        
        // Score calculation: lower volatility, lower competition, higher success rate = better
        let volatility_score = 1.0 - (volatility * 10.0).min(1.0);
        let competition_score = 1.0 - competition;
        let success_score = success_rate;
        
        Ok((volatility_score * 0.4 + competition_score * 0.3 + success_score * 0.3).max(0.0).min(1.0))
    }

    pub fn get_optimization_metrics(&self) -> Result<OptimizationMetrics> {
        let params = self.optimization_params.read()
            .map_err(|e| anyhow::anyhow!("Failed to read optimization params: {}", e))?;
        
        let trades = self.historical_trades.read()
            .map_err(|e| anyhow::anyhow!("Failed to read historical trades: {}", e))?;
        
        let total_trades = trades.len();
        let successful_trades = trades.iter().filter(|t| t.success).count();
        let avg_execution_time = if !trades.is_empty() {
            trades.iter().map(|t| t.execution_time_ms as f64).sum::<f64>() / trades.len() as f64
        } else {
            0.0
        };
        
        Ok(OptimizationMetrics {
            total_trades_analyzed: total_trades,
            overall_success_rate: if total_trades > 0 { successful_trades as f64 / total_trades as f64 } else { 0.0 },
            current_base_slippage_bps: params.base_slippage_bps,
            avg_execution_time_ms: avg_execution_time,
            volatility_multiplier: params.volatility_multiplier,
            competition_multiplier: params.competition_multiplier,
        })
    }
}

#[derive(Debug, Clone, Default)]
pub struct SlippageAnalysis {
    pub avg_set_slippage_bps: u64,
    pub avg_actual_slippage_bps: u64,
    pub success_rate: f64,
    pub efficiency_score: f64,
    pub sample_size: usize,
}

#[derive(Debug, Clone)]
pub struct OptimizationMetrics {
    pub total_trades_analyzed: usize,
        pub overall_success_rate: f64,
    pub current_base_slippage_bps: u64,
    pub avg_execution_time_ms: f64,
    pub volatility_multiplier: f64,
    pub competition_multiplier: f64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use solana_sdk::pubkey::Pubkey;

    #[tokio::test]
    async fn test_slippage_calculation() {
        let optimizer = SlippageToleranceOptimizer::new();
        let token_in = Pubkey::new_unique();
        let token_out = Pubkey::new_unique();
        
        let slippage = optimizer.calculate_optimal_slippage(
            &token_in,
            &token_out,
            1000000,
            Decimal::from_str("100.0").unwrap(),
            10000000,
        ).await.unwrap();
        
        assert!(slippage >= MIN_SLIPPAGE_BPS);
        assert!(slippage <= MAX_SLIPPAGE_BPS);
    }

    #[tokio::test]
    async fn test_trade_recording() {
        let optimizer = SlippageToleranceOptimizer::new();
        let token_pair = (Pubkey::new_unique(), Pubkey::new_unique());
        
        let result = TradeResult {
            timestamp: 1234567890,
            token_pair,
            amount_in: 1000000,
            expected_out: 2000000,
            actual_out: 1980000,
            slippage_bps: 50,
            success: true,
            market_volatility: 0.02,
            competition_detected: false,
            execution_time_ms: 100,
        };
        
        optimizer.record_trade_result(result).await.unwrap();
        
        let trades = optimizer.historical_trades.read().unwrap();
        assert_eq!(trades.len(), 1);
    }

    #[tokio::test]
    async fn test_volatility_calculation() {
        let optimizer = SlippageToleranceOptimizer::new();
        let token_pair = (Pubkey::new_unique(), Pubkey::new_unique());
        
        // Record multiple prices
        for i in 0..10 {
            let price = Decimal::from_str(&format!("{}.0", 100 + i)).unwrap();
            optimizer.record_price(&token_pair, price).unwrap();
        }
        
        let volatility = optimizer.calculate_current_volatility(&token_pair).unwrap();
        assert!(volatility > 0.0);
        assert!(volatility <= 0.1);
    }

    #[tokio::test]
    async fn test_market_condition_score() {
        let optimizer = SlippageToleranceOptimizer::new();
        let token_pair = (Pubkey::new_unique(), Pubkey::new_unique());
        
        let score = optimizer.get_market_condition_score(&token_pair).await.unwrap();
        assert!(score >= 0.0);
        assert!(score <= 1.0);
    }

    #[tokio::test]
    async fn test_emergency_override() {
        let optimizer = SlippageToleranceOptimizer::new();
        
        let initial_params = optimizer.optimization_params.read().unwrap().clone();
        let initial_base = initial_params.base_slippage_bps;
        
        optimizer.emergency_slippage_override(2.0).await.unwrap();
        
        let new_params = optimizer.optimization_params.read().unwrap().clone();
        assert_eq!(new_params.base_slippage_bps, (initial_base as f64 * 2.0) as u64);
    }

    #[tokio::test]
    async fn test_slippage_bounds() {
        let optimizer = SlippageToleranceOptimizer::new();
        let token_pair = (Pubkey::new_unique(), Pubkey::new_unique());
        
        let (min, max) = optimizer.get_recommended_slippage_bounds(&token_pair).unwrap();
        assert!(min >= MIN_SLIPPAGE_BPS);
        assert!(max <= MAX_SLIPPAGE_BPS);
        assert!(min <= max);
    }

    #[tokio::test]
    async fn test_competition_detection() {
        let optimizer = SlippageToleranceOptimizer::new();
        let token_pair = (Pubkey::new_unique(), Pubkey::new_unique());
        
        // Simulate multiple competition detections
        for _ in 0..5 {
            let result = TradeResult {
                timestamp: chrono::Utc::now().timestamp(),
                token_pair,
                amount_in: 1000000,
                expected_out: 2000000,
                actual_out: 1980000,
                slippage_bps: 50,
                success: false,
                market_volatility: 0.02,
                competition_detected: true,
                execution_time_ms: 50,
            };
            optimizer.record_trade_result(result).await.unwrap();
        }
        
        let intensity = optimizer.calculate_competition_intensity(&token_pair).unwrap();
        assert!(intensity > 0.0);
    }

    #[tokio::test]
    async fn test_success_rate_adjustment() {
        let optimizer = SlippageToleranceOptimizer::new();
        let token_pair = (Pubkey::new_unique(), Pubkey::new_unique());
        
        // Record multiple successful trades
        for i in 0..20 {
            let result = TradeResult {
                timestamp: chrono::Utc::now().timestamp() + i,
                token_pair,
                amount_in: 1000000,
                expected_out: 2000000,
                actual_out: 1990000,
                slippage_bps: 50,
                success: true,
                market_volatility: 0.015,
                competition_detected: false,
                execution_time_ms: 80,
            };
            optimizer.record_trade_result(result).await.unwrap();
        }
        
        let adjustment = optimizer.calculate_success_rate_adjustment(&token_pair).unwrap();
        assert!(adjustment >= 1.0); // Should boost slippage for high success rate
    }

    #[tokio::test]
    async fn test_size_impact_calculation() {
        let optimizer = SlippageToleranceOptimizer::new();
        let params = OptimizationParams::default();
        
        // Small trade
        let small_impact = optimizer.calculate_size_impact(1000, 1000000, &params);
        assert_eq!(small_impact, 1.0);
        
        // Large trade
        let large_impact = optimizer.calculate_size_impact(50000, 1000000, &params);
        assert!(large_impact > 1.0);
    }

    #[tokio::test]
    async fn test_parameter_adaptation() {
        let optimizer = SlippageToleranceOptimizer::new();
        let token_pair = (Pubkey::new_unique(), Pubkey::new_unique());
        
        // Record many failed trades to trigger adaptation
        for i in 0..150 {
            let result = TradeResult {
                timestamp: chrono::Utc::now().timestamp() + i,
                token_pair,
                amount_in: 1000000,
                expected_out: 2000000,
                actual_out: 1950000,
                slippage_bps: 30,
                success: i % 3 == 0, // 33% success rate
                market_volatility: 0.025,
                competition_detected: i % 5 == 0,
                execution_time_ms: 100,
            };
            optimizer.record_trade_result(result).await.unwrap();
        }
        
        let params = optimizer.optimization_params.read().unwrap().clone();
        assert!(params.base_slippage_bps > 50); // Should have increased from default
    }
}

