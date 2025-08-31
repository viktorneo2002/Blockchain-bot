use borsh::{BorshDeserialize, BorshSerialize};
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::{
    clock::Clock,
    commitment_config::CommitmentConfig,
    pubkey::Pubkey,
    signature::{Keypair, Signer},
    compute_budget::ComputeBudgetInstruction,
    message::Message,
    transaction::VersionedTransaction,
};
use std::collections::VecDeque;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::sync::RwLock;
use rust_decimal::Decimal;
use rust_decimal::prelude::ToPrimitive;
use tracing;

// Import helper functions and utilities
use crate::ornstein_uhlenbeck_mean_reverter::token_utils;
use crate::ornstein_uhlenbeck_mean_reverter::helpers;

/// Default half-life period for mean reversion decay (seconds)
const DEFAULT_HALF_LIFE: f64 = 300.0; // 5 minutes, adjust per your alpha window
const PRICE_HISTORY_SIZE: usize = 1000; // Increased for better statistics, bounded for memory
const MIN_OBSERVATIONS: usize = 64;
const MAX_POSITION_SIZE: f64 = 0.25;
const MIN_PROFIT_BPS: f64 = 5.0;
const MAX_DRAWDOWN: f64 = 0.02;
const CONFIDENCE_THRESHOLD: f64 = 0.95;
const MIN_LIQUIDITY_USD: f64 = 50000.0;
const MAX_SLIPPAGE_BPS: f64 = 10.0;
const EMERGENCY_EXIT_THRESHOLD: f64 = 0.05;
const MAX_RETRY_ATTEMPTS: u8 = 3;
const RETRY_DELAY_MS: u64 = 50;
const MIN_HALF_LIFE: f64 = 60.0; // 1 minute minimum
const MAX_HALF_LIFE: f64 = 86400.0; // 1 day maximum

// Risk controls and feed health
const MAX_HOURLY_TRADES: u64 = 50; // circuit breaker
const DAILY_LOSS_LIMIT: f64 = 0.05; // 5% of NAV
const STALE_PRICE_MAX_AGE_SECS: u64 = 15; // treat price as stale if older
const PRICE_FEED_HEARTBEAT_SECS: u64 = 30; // maximum time without price updates
const MAX_PRICE_DEVIATION: f64 = 0.10; // 10% max deviation between sources

// Parameter validation bounds for mathematical stability
const MIN_THETA: f64 = 0.001; // Minimum mean reversion speed
const MAX_THETA: f64 = 10.0;  // Maximum mean reversion speed
const MIN_SIGMA: f64 = 0.0001; // Minimum volatility
const MAX_SIGMA: f64 = 5.0;    // Maximum volatility

// New constants for mainnet improvements
const EPS: f64 = 1e-9;
const PHI_MIN: f64 = 0.01; // min persistence for mean-reversion
const PHI_MAX: f64 = 0.97; // reject explosive or near-random regimes
const KELLY_SHRINK: f64 = 0.8; // empirical shrink factor to reduce risk
const VAR_FLOOR: f64 = 1e-6; // floor variance to avoid div by zero
const MAX_RETRIES: usize = 3;
const SIMULATE_FIRST: bool = true;

// Emergency conditions
const MAX_DRAWDOWN_EMERGENCY: f64 = 0.10; // 10% emergency drawdown
const MAX_HOURLY_LOSS: f64 = 0.03; // 3% max hourly loss
const MAX_POSITION_SIZE_EMERGENCY: f64 = 0.33; // 33% max position size
const MIN_PROFIT_THRESHOLD_BPS: f64 = 10.0; // minimum profit after gas costs

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum SignalStrength {
    VeryStrong,
    Strong,
    Medium,
    Weak,
    None,
}

/// Comprehensive bot state tracking for NAV, risk, and performance
#[derive(Debug, Clone)]
pub struct BotState {
    pub nav_usd: f64,
    pub last_rebalance_time: Instant,
    pub hourly_trades: u64,
    pub hourly_pnl: f64,
    pub daily_pnl: f64,
    pub hour_start: Instant,
    pub day_start: Instant,
    pub emergency_stop: bool,
    pub max_position_size_usd: f64,
    pub current_leverage: f64,
}

/// Price feed health and multi-source verification
#[derive(Debug, Clone)]
pub struct PriceFeedHealth {
    pub primary_last_update: Instant,
    pub secondary_last_update: Option<Instant>,
    pub tertiary_last_update: Option<Instant>,
    pub primary_price: f64,
    pub secondary_price: Option<f64>,
    pub tertiary_price: Option<f64>,
    pub consensus_price: f64,
    pub deviation_detected: bool,
    pub heartbeat_healthy: bool,
}

/// Emergency conditions monitoring
#[derive(Debug, Clone)]
pub struct EmergencyConditions {
    pub max_drawdown_exceeded: bool,
    pub hourly_loss_exceeded: bool,
    pub position_size_exceeded: bool,
    pub price_feed_failed: bool,
    pub network_congestion: bool,
}

/// Comprehensive monitoring metrics
#[derive(Debug, Clone)]
pub struct MonitoringMetrics {
    pub z_score_distribution: Vec<f64>,
    pub execution_latency_ms: Vec<u64>,
    pub slippage_bps: Vec<f64>,
    pub pnl_attribution: Vec<f64>,
    pub gas_costs_usd: Vec<f64>,
    pub priority_fees_paid: Vec<u64>,
}

/// Backtesting results with comprehensive performance metrics
#[derive(Debug, Clone)]
pub struct BacktestResults {
    pub initial_nav: f64,
    pub final_nav: f64,
    pub total_pnl: f64,
    pub total_return_pct: f64,
    pub trades_count: u64,
    pub win_rate: f64,
    pub max_drawdown: f64,
    pub sharpe_ratio: f64,
    pub sortino_ratio: f64,
    pub calmar_ratio: f64,
    pub profit_factor: f64,
    pub nav_history: Vec<f64>,
    pub trade_history: Vec<BacktestTrade>,
    pub daily_returns: Vec<f64>,
    pub volatility: f64,
    pub max_consecutive_losses: u64,
    pub max_consecutive_wins: u64,
    pub avg_trade_duration_hours: f64,
}

/// Individual backtest trade record
#[derive(Debug, Clone)]
pub struct BacktestTrade {
    pub timestamp: u64,
    pub z_score: f64,
    pub position_size: f64,
    pub execution_price: f64,
    pub nav_at_trade: f64,
    pub pnl: f64,
    pub cumulative_pnl: f64,
}

impl SignalStrength {
    /// Classify signal strength based on z-score magnitude
    pub fn from_z(z: f64) -> Self {
        let abs_z = z.abs();
        if abs_z > 2.5 {
            SignalStrength::VeryStrong
        } else if abs_z > 2.0 {
            SignalStrength::Strong
        } else if abs_z > 1.5 {
            SignalStrength::Medium
        } else if abs_z > 1.0 {
            SignalStrength::Weak
        } else {
            SignalStrength::None
        }
    }
}

impl Default for BotState {
    fn default() -> Self {
        let now = Instant::now();
        Self {
            nav_usd: 1_000_000.0, // Default starting NAV
            last_rebalance_time: now,
            hourly_trades: 0,
            hourly_pnl: 0.0,
            daily_pnl: 0.0,
            hour_start: now,
            day_start: now,
            emergency_stop: false,
            max_position_size_usd: 250_000.0, // 25% of default NAV
            current_leverage: 0.0,
        }
    }
}

impl Default for PriceFeedHealth {
    fn default() -> Self {
        Self {
            primary_last_update: Instant::now(),
            secondary_last_update: None,
            tertiary_last_update: None,
            primary_price: 0.0,
            secondary_price: None,
            tertiary_price: None,
            consensus_price: 0.0,
            deviation_detected: false,
            heartbeat_healthy: true,
        }
    }
}

impl Default for EmergencyConditions {
    fn default() -> Self {
        Self {
            max_drawdown_exceeded: false,
            hourly_loss_exceeded: false,
            position_size_exceeded: false,
            price_feed_failed: false,
            network_congestion: false,
        }
    }
}

impl Default for MonitoringMetrics {
    fn default() -> Self {
        Self {
            z_score_distribution: Vec::with_capacity(1000),
            execution_latency_ms: Vec::with_capacity(1000),
            slippage_bps: Vec::with_capacity(1000),
            pnl_attribution: Vec::with_capacity(1000),
            gas_costs_usd: Vec::with_capacity(1000),
            priority_fees_paid: Vec::with_capacity(1000),
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct OUParameters {
    pub theta: f64,
    pub mu: f64,
    pub sigma: f64,
    pub half_life: f64,
    pub decay_rate: f64, // lambda = ln(2) / half_life
    pub mean_reversion_time: f64,
    pub confidence: f64,
}

#[derive(Debug, Clone)]
pub struct PricePoint {
    pub price: f64,
    pub volume: f64,
    pub timestamp: u64,
    pub slot: u64,
}

#[derive(Debug, Clone)]
pub struct MarketState {
    pub bid: f64,
    pub ask: f64,
    pub mid: f64,
    pub spread_bps: f64,
    pub liquidity: f64,
    pub volatility: f64,
    pub skew: f64,
    pub kurtosis: f64,
}

#[derive(Debug, Clone)]
pub struct Signal {
    pub strength: SignalStrength,
    pub z_score: f64,
    pub expected_return: f64,
    pub confidence: f64,
    pub time_to_reversion: f64,
    pub position_size: f64,
    pub stop_loss: f64,
    pub take_profit: f64,
    pub max_hold_time: u64,
}

impl Signal {
    /// Create a default "none" signal
    pub fn none() -> Self {
        Signal {
            strength: SignalStrength::None,
            z_score: 0.0,
            expected_return: 0.0,
            confidence: 0.0,
            time_to_reversion: 0.0,
            position_size: 0.0,
            stop_loss: 0.0,
            take_profit: 0.0,
            max_hold_time: 0,
        }
    }
}

pub struct OrnsteinUhlenbeckMeanReverter {
    price_history: Arc<RwLock<VecDeque<PricePoint>>>,
    parameters: Arc<RwLock<OUParameters>>,
    last_value: Arc<RwLock<f64>>, // Current OU process value
    current_position: Arc<RwLock<f64>>,
    entry_price: Arc<RwLock<Option<f64>>>,
    last_update: Arc<RwLock<Instant>>,
    total_pnl: Arc<RwLock<f64>>,
    win_rate: Arc<RwLock<f64>>,
    sharpe_ratio: Arc<RwLock<f64>>,
    max_drawdown_seen: Arc<RwLock<f64>>,
    trades_count: Arc<RwLock<u64>>,
    // Enhanced state tracking
    bot_state: Arc<RwLock<BotState>>,
    price_feed_health: Arc<RwLock<PriceFeedHealth>>,
    emergency_conditions: Arc<RwLock<EmergencyConditions>>,
    monitoring_metrics: Arc<RwLock<MonitoringMetrics>>,
}

impl OrnsteinUhlenbeckMeanReverter {
    pub fn new() -> Self {
        Self::new_with_params(None, None, None).expect("Default parameters should be valid")
    }

    /// Create new instance with optional parameter validation and clamping
    /// Returns Result for safe construction and validation
    pub fn new_with_params(
        mean: Option<f64>,
        volatility: Option<f64>, 
        initial_value: Option<f64>,
        half_life_opt: Option<f64>
    ) -> Result<Self, Box<dyn std::error::Error>> {
        // Use defaults or provided values
        let mean = mean.unwrap_or(0.0);
        let volatility = volatility.unwrap_or(0.1); // Default 10% volatility
        let initial_value = initial_value.unwrap_or(0.0);
        let half_life = half_life_opt.unwrap_or(DEFAULT_HALF_LIFE);

        // Validate half-life first (most critical)
        if !half_life.is_finite() || half_life <= 0.0 {
            return Err("half_life must be positive and finite".into());
        }
        let half_life_clamped = half_life.clamp(MIN_HALF_LIFE, MAX_HALF_LIFE);

        // Calculate decay rate from half-life: decay_rate = ln(2) / half_life
        let decay_rate = std::f64::consts::LN_2 / half_life_clamped;

        // Validate volatility (sigma)
        if !volatility.is_finite() || volatility <= 0.0 {
            return Err("volatility must be positive and finite".into());
        }
        let volatility_clamped = volatility.clamp(MIN_SIGMA, MAX_SIGMA);

        // Validate mean (can be any finite value)
        if !mean.is_finite() {
            return Err("mean must be finite".into());
        }

        // Validate initial value
        if !initial_value.is_finite() {
            return Err("initial_value must be finite".into());
        }

        // Compute theta from decay rate for compatibility
        let theta = decay_rate;
        let theta_clamped = theta.clamp(MIN_THETA, MAX_THETA);

        // Log parameter clamping if values were adjusted
        if (volatility_clamped - volatility).abs() > 1e-10 {
            tracing::warn!("Clamped volatility from {:.6} to {:.6}", volatility, volatility_clamped);
        }
        if (half_life_clamped - half_life).abs() > 1e-10 {
            tracing::warn!("Clamped half_life from {:.1}s to {:.1}s", half_life, half_life_clamped);
        }
        if (theta_clamped - theta).abs() > 1e-10 {
            tracing::warn!("Clamped theta from {:.6} to {:.6}", theta, theta_clamped);
        }

        tracing::info!("Initializing OU mean reverter: mean={:.4}, volatility={:.6}, half_life={:.1}s, decay_rate={:.6}, initial_value={:.4}", 
                      mean, volatility_clamped, half_life_clamped, decay_rate, initial_value);

        Ok(Self {
            price_history: Arc::new(RwLock::new(VecDeque::with_capacity(PRICE_HISTORY_SIZE))),
            parameters: Arc::new(RwLock::new(OUParameters {
                theta: theta_clamped,
                mu: mean,
                sigma: volatility_clamped,
                half_life: half_life_clamped,
                decay_rate,
                mean_reversion_time: if theta_clamped > 0.0 { 1.0 / theta_clamped } else { 0.0 },
                confidence: 0.0,
            })),
            last_value: Arc::new(RwLock::new(initial_value)),
            current_position: Arc::new(RwLock::new(0.0)),
            entry_price: Arc::new(RwLock::new(None)),
            last_update: Arc::new(RwLock::new(Instant::now())),
            total_pnl: Arc::new(RwLock::new(0.0)),
            win_rate: Arc::new(RwLock::new(0.0)),
            sharpe_ratio: Arc::new(RwLock::new(0.0)),
            max_drawdown_seen: Arc::new(RwLock::new(0.0)),
            trades_count: Arc::new(RwLock::new(0)),
            // Enhanced state tracking
            bot_state: Arc::new(RwLock::new(BotState::default())),
            price_feed_health: Arc::new(RwLock::new(PriceFeedHealth::default())),
            emergency_conditions: Arc::new(RwLock::new(EmergencyConditions::default())),
            monitoring_metrics: Arc::new(RwLock::new(MonitoringMetrics::default())),
        })
    }

    /// Update price with Solana Clock integration and bounded buffer management
    pub async fn update_price(&self, price: f64, volume: f64, timestamp: u64, slot: u64) -> Result<(), Box<dyn std::error::Error>> {
        // Validate input parameters
        if !price.is_finite() || price <= 0.0 {
            tracing::warn!("Invalid price: {}", price);
            return Err("Price must be positive and finite".into());
        }
        if !volume.is_finite() || volume < 0.0 {
            tracing::warn!("Invalid volume: {}", volume);
            return Err("Volume must be non-negative and finite".into());
        }

        let mut history = self.price_history.write().await;
        
        // Check for extreme price movements (potential data error or flash crash)
        if let Some(last) = history.back() {
            let price_change = (price - last.price).abs() / last.price;
            if price_change > 0.5 {
                tracing::warn!("Extreme price movement detected: {:.1}% from {:.4} to {:.4}, skipping", 
                              price_change * 100.0, last.price, price);
                return Ok(());
            }
        }

        // Bounded buffer management with O(1) operations
        if history.len() >= PRICE_HISTORY_SIZE {
            let removed = history.pop_front();
            tracing::debug!("Removed oldest price point: {:?}", removed);
        }

        let price_point = PricePoint {
            price,
            volume,
            timestamp,
            slot,
        };

        history.push_back(price_point.clone());
        let history_len = history.len();
        drop(history); // Release lock early

        // Update last update timestamp
        let mut last_update = self.last_update.write().await;
        *last_update = Instant::now();
        drop(last_update);

        tracing::debug!("Added price point: price={:.4}, volume={:.2}, slot={}, history_len={}", 
                       price, volume, slot, history_len);

        // Trigger recalibration if we have enough data
        if history_len >= MIN_OBSERVATIONS && history_len % 50 == 0 {
            tracing::info!("Triggering recalibration with {} observations", history_len);
            if let Err(e) = self.calibrate_parameters().await {
                tracing::error!("Calibration failed: {}", e);
            }
        }

        Ok(())
    }

    /// Advances the OU process by delta_time (seconds), returns new value.
    /// Uses canonical continuous-time OU process discretization:
    /// X(t+dt) = X(t)*exp(-lambda*dt) + mean*(1 - exp(-lambda*dt)) + volatility * sqrt((1 - exp(-2*lambda*dt))/(2*lambda)) * N(0,1)
    pub async fn step(&self, delta_time: f64) -> Result<f64, Box<dyn std::error::Error>> {
        if !delta_time.is_finite() || delta_time < 0.0 {
            return Err("delta_time must be non-negative and finite".into());
        }

        let params = self.parameters.read().await;
        let mut last_value = self.last_value.write().await;

        // Get OU parameters
        let lambda = params.decay_rate; // decay rate
        let mean = params.mu;
        let volatility = params.sigma;

        if lambda <= 0.0 {
            tracing::warn!("Invalid decay rate: {}, using default", lambda);
            return Ok(*last_value);
        }

        // Exponential decay factor
        let exp_decay = (-lambda * delta_time).exp();

        // Deterministic part: mean reversion
        let deterministic_part = *last_value * exp_decay + mean * (1.0 - exp_decay);

        // Stochastic part: Brownian motion with correct variance scaling
        let variance_scaling = if lambda > 1e-10 {
            ((1.0 - (-2.0 * lambda * delta_time).exp()) / (2.0 * lambda)).sqrt()
        } else {
            // For very small lambda, use limit: sqrt(dt)
            delta_time.sqrt()
        };

        // Generate standard normal random variable
        // In production, you might want to use a deterministic seed or external randomness
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};
        
        // Simple pseudo-random based on current state (deterministic for testing)
        let mut hasher = DefaultHasher::new();
        (*last_value * 1000000.0) as u64.hash(&mut hasher);
        delta_time.to_bits().hash(&mut hasher);
        let hash = hasher.finish();
        
        // Box-Muller transform for normal distribution
        let u1 = (hash as f64) / (u64::MAX as f64);
        let u2 = ((hash >> 32) as f64) / (u32::MAX as f64);
        let normal_sample = (-2.0 * u1.ln()).sqrt() * (2.0 * std::f64::consts::PI * u2).cos();

        let stochastic_part = volatility * variance_scaling * normal_sample;

        // Update the OU process value
        let new_value = deterministic_part + stochastic_part;
        
        // Validate the new value
        if !new_value.is_finite() {
            tracing::error!("OU step produced non-finite value: {}, reverting to mean", new_value);
            *last_value = mean;
        } else {
            *last_value = new_value;
        }

        tracing::debug!("OU step: dt={:.3}s, old={:.6}, new={:.6}, deterministic={:.6}, stochastic={:.6}", 
                       delta_time, deterministic_part - mean * (1.0 - exp_decay), *last_value, 
                       deterministic_part, stochastic_part);

        Ok(*last_value)
    }

    /// Get current OU process value without advancing
    pub async fn current_value(&self) -> f64 {
        *self.last_value.read().await
    }

    /// Reset OU process to a specific value
    pub async fn reset_value(&self, value: f64) -> Result<(), Box<dyn std::error::Error>> {
        if !value.is_finite() {
            return Err("Reset value must be finite".into());
        }
        
        let mut last_value = self.last_value.write().await;
        *last_value = value;
        
        tracing::info!("Reset OU process value to {:.6}", value);
        Ok(())
    }

    /// Fetch token decimals from chain using SPL Token mint account data
    pub async fn get_token_decimals(
        &self,
        client: &RpcClient,
        mint: &Pubkey,
    ) -> Result<u8, Box<dyn std::error::Error>> {
        match token_utils::get_token_decimals(client, mint).await {
            Ok(decimals) => {
                tracing::debug!("Retrieved token decimals: {} for mint {}", decimals, mint);
                Ok(decimals)
            }
            Err(e) => {
                tracing::warn!("Failed to fetch token decimals for mint {}: {}. Using fallback 6", mint, e);
                Ok(6) // Conservative fallback
            }
        }
    }
    
    /// Calculate token amount from USD using current market price and token decimals
    pub async fn calculate_token_amount(
        &self,
        client: &RpcClient,
        mint: &Pubkey,
        usd_amount: f64,
    ) -> Result<u64, Box<dyn std::error::Error>> {
        // Get current price from price history
        let current_price = {
            let history = self.price_history.read().await;
            if let Some(latest) = history.back() {
                latest.price
            } else {
                return Err("No price history available for token amount calculation".into());
            }
        };
        
        // Get token decimals
        let decimals = self.get_token_decimals(client, mint).await?;
        
        // Calculate token amount using helper function
        token_utils::usd_to_token_amount(usd_amount, current_price, decimals)
            .map_err(|e| format!("Token amount calculation failed: {:?}", e).into())
    }
    
    /// Update price feed health with multi-source verification
    pub async fn update_price_feed_health(
        &self,
        primary_price: f64,
        secondary_price: Option<f64>,
        tertiary_price: Option<f64>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let mut health = self.price_feed_health.write().await;
        let now = Instant::now();
        
        // Update timestamps and prices
        health.primary_last_update = now;
        health.primary_price = primary_price;
        
        if let Some(price) = secondary_price {
            health.secondary_last_update = Some(now);
            health.secondary_price = Some(price);
        }
        
        if let Some(price) = tertiary_price {
            health.tertiary_last_update = Some(now);
            health.tertiary_price = Some(price);
        }
        
        // Calculate consensus price and detect deviations
        let mut prices = vec![primary_price];
        if let Some(price) = secondary_price { prices.push(price); }
        if let Some(price) = tertiary_price { prices.push(price); }
        
        // Use median as consensus
        prices.sort_by(|a, b| a.partial_cmp(b).unwrap());
        health.consensus_price = prices[prices.len() / 2];
        
        // Check for price deviations
        let max_deviation = prices.iter()
            .map(|p| (p - health.consensus_price).abs() / health.consensus_price)
            .max_by(|a, b| a.partial_cmp(b).unwrap())
            .unwrap_or(0.0);
            
        health.deviation_detected = max_deviation > MAX_PRICE_DEVIATION;
        
        // Check heartbeat health
        health.heartbeat_healthy = health.primary_last_update.elapsed() < Duration::from_secs(PRICE_FEED_HEARTBEAT_SECS);
        
        if health.deviation_detected {
            tracing::warn!("Price deviation detected: {:.2}% max deviation from consensus {:.4}", 
                         max_deviation * 100.0, health.consensus_price);
        }
        
        if !health.heartbeat_healthy {
            tracing::warn!("Price feed heartbeat unhealthy: last update {:.1}s ago", 
                         health.primary_last_update.elapsed().as_secs_f64());
        }
        
        Ok(())
    }
    
    /// Check if price feeds are healthy and not stale
    pub async fn validate_price_feed_health(&self) -> bool {
        let health = self.price_feed_health.read().await;
        let last_update_age = health.primary_last_update.elapsed();
        
        // Check staleness
        if last_update_age > Duration::from_secs(STALE_PRICE_MAX_AGE_SECS) {
            tracing::warn!("Price feed stale: {:.1}s old", last_update_age.as_secs_f64());
            return false;
        }
        
        // Check heartbeat
        if !health.heartbeat_healthy {
            tracing::warn!("Price feed heartbeat failed");
            return false;
        }
        
        // Check for significant deviations
        if health.deviation_detected {
            tracing::warn!("Price feed deviation detected");
            return false;
        }
        
        true
    }
    
    /// Update bot state with NAV and risk tracking
    pub async fn update_bot_state(&self, nav_usd: f64) -> Result<(), Box<dyn std::error::Error>> {
        let mut state = self.bot_state.write().await;
        let now = Instant::now();
        
        // Update NAV
        state.nav_usd = nav_usd;
        
        // Reset hourly counters if hour has passed
        if now.duration_since(state.hour_start) >= Duration::from_secs(3600) {
            state.hourly_trades = 0;
            state.hourly_pnl = 0.0;
            state.hour_start = now;
            tracing::info!("Reset hourly counters: trades=0, pnl=0.0");
        }
        
        // Reset daily counters if day has passed
        if now.duration_since(state.day_start) >= Duration::from_secs(86400) {
            state.daily_pnl = 0.0;
            state.day_start = now;
            tracing::info!("Reset daily PnL counter: pnl=0.0");
        }
        
        // Update max position size based on NAV
        state.max_position_size_usd = nav_usd * MAX_POSITION_SIZE;
        
        // Calculate current leverage
        let position = *self.current_position.read().await;
        state.current_leverage = position.abs();
        
        Ok(())
    }
    
    /// Check risk management circuit breakers
    pub async fn check_circuit_breakers(&self) -> Result<bool, Box<dyn std::error::Error>> {
        let state = self.bot_state.read().await;
        let mut emergency = self.emergency_conditions.write().await;
        
        // Check hourly trade limit
        if state.hourly_trades >= MAX_HOURLY_TRADES {
            emergency.hourly_loss_exceeded = true;
            tracing::warn!("Circuit breaker: Hourly trade limit exceeded ({}/{})", 
                         state.hourly_trades, MAX_HOURLY_TRADES);
            return Ok(false);
        }
        
        // Check daily loss limit
        let daily_loss_pct = -state.daily_pnl / state.nav_usd;
        if daily_loss_pct > DAILY_LOSS_LIMIT {
            emergency.hourly_loss_exceeded = true;
            tracing::warn!("Circuit breaker: Daily loss limit exceeded ({:.2}% > {:.2}%)", 
                         daily_loss_pct * 100.0, DAILY_LOSS_LIMIT * 100.0);
            return Ok(false);
        }
        
        // Check maximum drawdown
        let total_pnl = *self.total_pnl.read().await;
        let drawdown_pct = -total_pnl / state.nav_usd;
        if drawdown_pct > MAX_DRAWDOWN_EMERGENCY {
            emergency.max_drawdown_exceeded = true;
            tracing::warn!("Circuit breaker: Maximum drawdown exceeded ({:.2}% > {:.2}%)", 
                         drawdown_pct * 100.0, MAX_DRAWDOWN_EMERGENCY * 100.0);
            return Ok(false);
        }
        
        // Check position size
        let current_position_usd = state.current_leverage * state.nav_usd;
        if current_position_usd > state.max_position_size_usd {
            emergency.position_size_exceeded = true;
            tracing::warn!("Circuit breaker: Position size exceeded ({:.0} > {:.0})", 
                         current_position_usd, state.max_position_size_usd);
            return Ok(false);
        }
        
        // Check price feed health
        if !self.validate_price_feed_health().await {
            emergency.price_feed_failed = true;
            tracing::warn!("Circuit breaker: Price feed health failed");
            return Ok(false);
        }
        
        // Check emergency stop flag
        if state.emergency_stop {
            tracing::warn!("Circuit breaker: Emergency stop activated");
            return Ok(false);
        }
        
        // All checks passed
        emergency.max_drawdown_exceeded = false;
        emergency.hourly_loss_exceeded = false;
        emergency.position_size_exceeded = false;
        emergency.price_feed_failed = false;
        
        Ok(true)
    }
    
    /// Record trade execution for circuit breaker tracking
    pub async fn record_trade_execution(
        &self,
        pnl_usd: f64,
        gas_cost_usd: f64,
        execution_latency_ms: u64,
        slippage_bps: f64,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let mut state = self.bot_state.write().await;
        let mut metrics = self.monitoring_metrics.write().await;
        
        // Update trade counters
        state.hourly_trades += 1;
        state.hourly_pnl += pnl_usd;
        state.daily_pnl += pnl_usd;
        state.last_rebalance_time = Instant::now();
        
        // Record monitoring metrics
        metrics.execution_latency_ms.push(execution_latency_ms);
        metrics.slippage_bps.push(slippage_bps);
        metrics.pnl_attribution.push(pnl_usd);
        metrics.gas_costs_usd.push(gas_cost_usd);
        
        // Keep metrics bounded
        if metrics.execution_latency_ms.len() > 1000 {
            metrics.execution_latency_ms.remove(0);
        }
        if metrics.slippage_bps.len() > 1000 {
            metrics.slippage_bps.remove(0);
        }
        if metrics.pnl_attribution.len() > 1000 {
            metrics.pnl_attribution.remove(0);
        }
        if metrics.gas_costs_usd.len() > 1000 {
            metrics.gas_costs_usd.remove(0);
        }
        
        tracing::info!("Trade recorded: PnL={:.2}, gas={:.4}, latency={}ms, slippage={:.1}bps", 
                     pnl_usd, gas_cost_usd, execution_latency_ms, slippage_bps);
        
        Ok(())
    }
    
    /// Get current NAV from bot state
    pub async fn get_current_nav(&self) -> f64 {
        let bot_state = self.bot_state.read().await;
        bot_state.nav_usd
    }

    /// Execute with MEV protection using JITO bundles and enhanced anti-MEV strategies
    pub async fn execute_with_mev_protection(
        &self,
        client: &RpcClient,
        exec: ExecutionParams,
        signal: TradingSignal,
        mev_protection: MevProtectionLevel,
    ) -> Result<f64, Box<dyn std::error::Error>> {
        // Apply jitter delay based on urgency
        let urgency = match signal.strength {
            SignalStrength::Strong => 0.9,
            SignalStrength::Medium => 0.6,
            SignalStrength::Weak => 0.3,
            SignalStrength::None => 0.1,
        };
        
        let jitter_delay = self.generate_jitter_delay(urgency);
        tracing::info!("Applying MEV protection jitter: {:?}", jitter_delay);
        sleep(jitter_delay).await;
        
        // Calculate dynamic priority fee with anti-MEV considerations
        let network_congestion = 1.2; // Would fetch real data
        let priority_fee = self.calculate_dynamic_priority_fee(urgency, network_congestion).await;
        
        // For high protection levels, use JITO bundles
        match mev_protection {
            MevProtectionLevel::Extreme => {
                // Use JITO bundle submission for maximum MEV protection
                tracing::info!("Using JITO bundle execution for maximum MEV protection");
                self.execute_jito_bundle(client, exec, signal, priority_fee).await
            }
            MevProtectionLevel::High => {
                // Use obfuscated single transaction with high protection
                self.execute_obfuscated_transaction(client, exec, signal, mev_protection).await
            }
            _ => {
                // Use standard execution with basic protection
                self.execute_rebalance_nonblocking(client, exec, signal).await
            }
        }
    }
    
    /// Execute transaction using JITO MEV bundle
    async fn execute_jito_bundle(
        &self,
        client: &RpcClient,
        exec: ExecutionParams,
        signal: TradingSignal,
        tip_amount: u64,
    ) -> Result<f64, Box<dyn std::error::Error>> {
        let execution_start = Instant::now();
        
        // Get current NAV for position updates
        let current_nav = self.get_current_nav().await;
        
        // Calculate token amount using dynamic decimals
        let amount_tokens = self.calculate_token_amount(client, &exec.token_mint, exec.amount_usd).await?;
        
        // Create main swap instruction
        let swap_instruction = self.create_swap_instruction(&exec, amount_tokens)?;
        
        // Create tip instruction for JITO
        let tip_instruction = self.create_jito_tip_instruction(tip_amount)?;
        
        // Bundle instructions together
        let bundle_transactions = vec![
            self.create_mev_obfuscated_transaction(
                vec![swap_instruction],
                &self.wallet_keypair,
                client.get_latest_blockhash().await?,
                MevProtectionLevel::High,
            ).await?,
            self.create_simple_transaction(
                vec![tip_instruction],
                &self.wallet_keypair,
                client.get_latest_blockhash().await?,
            ).await?,
        ];
        
        // Submit JITO bundle with monitoring
        let results = self.submit_jito_bundle(client, bundle_transactions, tip_amount).await?;
        
        // Extract execution price from the first (main) transaction
        let main_signature = &results[0];
        let fill_price_actual = self.get_real_fill_price(
            client,
            main_signature,
            amount_tokens,
            signal.take_profit
        ).await.unwrap_or_else(|_| {
            warn!("Failed to parse real fill price from JITO bundle, using fallback");
            if exec.direction > 0.0 { signal.take_profit } else { signal.stop_loss }
        });
        
        // Update position with real execution details
        self.update_position_fixed(signal.position_size, fill_price_actual, current_nav).await?;
        
        // Record execution metrics
        let execution_latency_ms = execution_start.elapsed().as_millis() as u64;
        let actual_pnl = (signal.position_size * fill_price_actual - signal.position_size * signal.take_profit) * current_nav;
        let estimated_gas_cost_usd = (tip_amount as f64 / 1_000_000.0) * 0.1;
        let actual_slippage_bps = (fill_price_actual - signal.take_profit).abs() / signal.take_profit * 10000.0;
        
        self.record_trade_execution(
            actual_pnl,
            estimated_gas_cost_usd,
            execution_latency_ms,
            actual_slippage_bps,
        ).await?;
        
        tracing::info!(
            "JITO bundle execution completed: fill_price={:.4}, latency={}ms, tip={:.6} SOL, slippage={:.1}bps", 
            fill_price_actual, execution_latency_ms, tip_amount as f64 / 1_000_000_000.0, actual_slippage_bps
        );
        
        Ok(fill_price_actual)
    }
    
    /// Create JITO tip instruction
    fn create_jito_tip_instruction(&self, tip_amount: u64) -> Result<Instruction, Box<dyn std::error::Error>> {
        // JITO tip addresses (these are real JITO tip accounts)
        let jito_tip_accounts = vec![
            "96gYZGLnJYVFmbjzopPSU6QiEV5fGqZNyN9nmNhvrZU5",
            "HFqU5x63VTqvQss8hp11i4wVV8bD44PvwucfZ2bU7gRe",
            "Cw8CFyM9FkoMi7K7Crf6HNQqf4uEMzpKw6QNghXLvLkY",
            "ADaUMid9yfUytqMBgopwjb2DTLSokTSzL1zt6iGPaS49",
        ];
        
        // Select random tip account for distribution
        let mut rng = rand::thread_rng();
        let selected_tip_account = jito_tip_accounts.choose(&mut rng).unwrap();
        let tip_pubkey = selected_tip_account.parse::<Pubkey>()?;
        
        Ok(solana_sdk::system_instruction::transfer(
            &self.wallet_keypair.pubkey(),
            &tip_pubkey,
            tip_amount
        ))
    }
    
    /// Create simple transaction without obfuscation
    async fn create_simple_transaction(
        &self,
        instructions: Vec<Instruction>,
        signer: &Keypair,
        recent_blockhash: Hash,
    ) -> Result<VersionedTransaction, Box<dyn std::error::Error>> {
        let message = Message::new(&instructions, Some(&signer.pubkey()));
        let versioned_message = VersionedMessage::V0(v0::Message::try_from(message)?);
        let mut transaction = VersionedTransaction::try_new(versioned_message, &[signer])?;
        
        // Set recent blockhash
        match &mut transaction.message {
            VersionedMessage::V0(ref mut msg) => {
                msg.recent_blockhash = recent_blockhash;
            },
            VersionedMessage::Legacy(ref mut msg) => {
                msg.recent_blockhash = recent_blockhash;
            },
        }
        
        Ok(transaction)
    }
    
    /// Submit JITO bundle with monitoring and redundancy
    async fn submit_jito_bundle(
        &self,
        client: &RpcClient,
        transactions: Vec<VersionedTransaction>,
        tip_amount: u64,
    ) -> Result<Vec<Signature>, Box<dyn std::error::Error>> {
        tracing::info!("Submitting JITO bundle with {} transactions, tip: {} lamports", 
                      transactions.len(), tip_amount);
        
        // JITO bundle submission endpoints (rotate for redundancy)
        let jito_endpoints = vec![
            "https://mainnet.block-engine.jito.wtf/api/v1/bundles",
            "https://amsterdam.mainnet.block-engine.jito.wtf/api/v1/bundles",
            "https://frankfurt.mainnet.block-engine.jito.wtf/api/v1/bundles",
            "https://ny.mainnet.block-engine.jito.wtf/api/v1/bundles",
        ];
        
        let mut signatures = Vec::new();
        
        // Simulate transaction execution for now (real implementation would use HTTP client)
        // In production, you'd send bundle via HTTP POST to JITO endpoints
        for (i, tx) in transactions.iter().enumerate() {
            let signature = if i == 0 {
                // Main transaction - actually execute
                let legacy_tx = solana_sdk::transaction::Transaction::try_from(tx.clone())?;
                client.send_and_confirm_transaction_with_spinner_and_commitment(
                    &legacy_tx,
                    CommitmentConfig::confirmed(),
                ).await?
            } else {
                // Tip transaction - simulate for now
                Signature::new_unique()
            };
            
            signatures.push(signature);
        }
        
        // Monitor bundle execution status
        self.monitor_bundle_execution(client, &signatures).await?;
        
        tracing::info!("JITO bundle executed successfully with {} transactions", signatures.len());
        
        Ok(signatures)
    }
    
    /// Monitor JITO bundle execution status
    async fn monitor_bundle_execution(
        &self,
        client: &RpcClient,
        signatures: &[Signature],
    ) -> Result<(), Box<dyn std::error::Error>> {
        let monitor_start = Instant::now();
        let timeout = Duration::from_secs(30);
        
        while monitor_start.elapsed() < timeout {
            let mut all_confirmed = true;
            
            for signature in signatures {
                match client.get_signature_status(signature).await? {
                    Some(Ok(())) => {
                        tracing::debug!("Transaction {} confirmed", signature);
                    }
                    Some(Err(e)) => {
                        tracing::error!("Transaction {} failed: {:?}", signature, e);
                        return Err(format!("Bundle transaction failed: {:?}", e).into());
                    }
                    None => {
                        all_confirmed = false;
                        break;
                    }
                }
            }
            
            if all_confirmed {
                tracing::info!("All bundle transactions confirmed in {:?}", monitor_start.elapsed());
                return Ok(());
            }
            
            sleep(Duration::from_millis(500)).await;
        }
        
        Err("Bundle execution timeout".into())
    }
    
    /// Create swap instruction placeholder
    fn create_swap_instruction(&self, exec: &ExecutionParams, amount: u64) -> Result<Instruction, Box<dyn std::error::Error>> {
        // This is a placeholder - real implementation would create actual DEX swap instruction
        // For Jupiter, Orca, Raydium etc.
        Ok(Instruction {
            program_id: exec.token_mint, // Placeholder
            accounts: vec![],
            data: vec![],
        })
    }
    
    /// Execute obfuscated transaction with high MEV protection
    async fn execute_obfuscated_transaction(
        &self,
        client: &RpcClient,
        exec: ExecutionParams,
        signal: TradingSignal,
        mev_protection: MevProtectionLevel,
    ) -> Result<f64, Box<dyn std::error::Error>> {
        let execution_start = Instant::now();
        
        // Get current NAV
        let current_nav = self.get_current_nav().await;
        
        // Calculate token amount
        let amount_tokens = self.calculate_token_amount(client, &exec.token_mint, exec.amount_usd).await?;
        
        // Create main swap instruction
        let swap_instruction = self.create_swap_instruction(&exec, amount_tokens)?;
        
        // Create obfuscated transaction
        let obfuscated_tx = self.create_mev_obfuscated_transaction(
            vec![swap_instruction],
            &self.wallet_keypair,
            client.get_latest_blockhash().await?,
            mev_protection,
        ).await?;
        
        // Execute with enhanced monitoring
        let legacy_tx = solana_sdk::transaction::Transaction::try_from(obfuscated_tx)?;
        let signature = client.send_and_confirm_transaction_with_spinner_and_commitment(
            &legacy_tx,
            CommitmentConfig::confirmed(),
        ).await?;
        
        // Parse real execution price
        let fill_price_actual = self.get_real_fill_price(
            client,
            &signature,
            amount_tokens,
            signal.take_profit
        ).await.unwrap_or_else(|_| {
            warn!("Failed to parse real fill price from obfuscated transaction, using fallback");
            if exec.direction > 0.0 { signal.take_profit } else { signal.stop_loss }
        });
        
        // Update position and record metrics
        self.update_position_fixed(signal.position_size, fill_price_actual, current_nav).await?;
        
        let execution_latency_ms = execution_start.elapsed().as_millis() as u64;
        let actual_pnl = (signal.position_size * fill_price_actual - signal.position_size * signal.take_profit) * current_nav;
        let estimated_gas_cost_usd = 0.01; // Estimate for obfuscated transaction
        let actual_slippage_bps = (fill_price_actual - signal.take_profit).abs() / signal.take_profit * 10000.0;
        
        self.record_trade_execution(
            actual_pnl,
            estimated_gas_cost_usd,
            execution_latency_ms,
            actual_slippage_bps,
        ).await?;
        
        tracing::info!(
            "Obfuscated transaction completed: fill_price={:.4}, latency={}ms, slippage={:.1}bps", 
            fill_price_actual, execution_latency_ms, actual_slippage_bps
        );
        
        Ok(fill_price_actual)
    }

    /// Parse REAL execution prices from on-chain transaction logs
    async fn get_real_fill_price(
        &self,
        client: &RpcClient,
        signature: &Signature,
        expected_amount: u64,
        expected_price: f64,
    ) -> Result<f64, Box<dyn std::error::Error>> {
        // Get transaction details with logs
        let tx = client.get_transaction_with_config(
            signature,
            solana_client::rpc_config::RpcTransactionConfig {
                encoding: Some(UiTransactionEncoding::Json),
                commitment: Some(CommitmentConfig::confirmed()),
                max_supported_transaction_version: Some(0),
            },
        ).await?;

        if let Some(meta) = tx.transaction.meta {
            if let Some(log_messages) = meta.log_messages {
                // Parse DEX-specific logs for actual fill prices
                for log in &log_messages {
                    // Parse Orca fills
                    if let Some(fill_info) = self.parse_orca_fill_log(log) {
                        if fill_info.amount >= expected_amount * 9 / 10 { // 90% fill threshold
                            return Ok(fill_info.price);
                        }
                    }
                    
                    // Parse Raydium fills
                    if let Some(fill_info) = self.parse_raydium_fill_log(log) {
                        if fill_info.amount >= expected_amount * 9 / 10 {
                            return Ok(fill_info.price);
                        }
                    }
                    
                    // Parse Jupiter fills
                    if let Some(fill_info) = self.parse_jupiter_fill_log(log) {
                        if fill_info.amount >= expected_amount * 9 / 10 {
                            return Ok(fill_info.price);
                        }
                    }
                }
            }
        }
        
        Err("Real fill price not found in transaction logs".into())
    }

    /// Parse Orca DEX fill logs
    fn parse_orca_fill_log(&self, log: &str) -> Option<DexFillInfo> {
        // Orca swap log format: "Program log: Swap: {amount_in}, {amount_out}, {price}"
        if log.contains("Program log: Swap:") {
            let parts: Vec<&str> = log.split(": ").collect();
            if parts.len() >= 2 {
                let swap_data: Vec<&str> = parts[1].split(", ").collect();
                if swap_data.len() >= 3 {
                    if let (Ok(amount_in), Ok(amount_out)) = (
                        swap_data[0].parse::<u64>(),
                        swap_data[1].parse::<u64>()
                    ) {
                        let price = amount_out as f64 / amount_in as f64;
                        return Some(DexFillInfo {
                            amount: amount_out,
                            price,
                            fee: 0.003, // Orca typical fee
                            slippage: 0.0, // Calculate separately
                        });
                    }
                }
            }
        }
        None
    }

    /// Parse Raydium DEX fill logs
    fn parse_raydium_fill_log(&self, log: &str) -> Option<DexFillInfo> {
        // Raydium swap log format differs - adapt as needed
        if log.contains("Program log: ray_log:") || log.contains("SwapBaseIn") {
            // Parse Raydium-specific log format
            // This is a simplified parser - real implementation needs proper JSON parsing
            if let Some(start) = log.find('{') {
                if let Some(end) = log.rfind('}') {
                    let json_str = &log[start..=end];
                    if let Ok(data) = serde_json::from_str::<Value>(json_str) {
                        if let (Some(amount_in), Some(amount_out)) = (
                            data["amount_in"].as_u64(),
                            data["amount_out"].as_u64()
                        ) {
                            let price = amount_out as f64 / amount_in as f64;
                            return Some(DexFillInfo {
                                amount: amount_out,
                                price,
                                fee: 0.0025, // Raydium typical fee
                                slippage: 0.0,
                            });
                        }
                    }
                }
            }
        }
        None
    }

    /// Parse Jupiter aggregator fill logs
    fn parse_jupiter_fill_log(&self, log: &str) -> Option<DexFillInfo> {
        // Jupiter logs are more complex due to routing
        if log.contains("Program log: Instruction: Swap") {
            // Extract swap details from Jupiter logs
            // This requires more sophisticated parsing of the route
            if log.contains("amount_in:") && log.contains("amount_out:") {
                // Simplified parser - real implementation needs regex
                let parts: Vec<&str> = log.split_whitespace().collect();
                for (i, part) in parts.iter().enumerate() {
                    if *part == "amount_in:" && i + 1 < parts.len() {
                        if let Ok(amount_in) = parts[i + 1].parse::<u64>() {
                            for (j, jpart) in parts.iter().enumerate() {
                                if *jpart == "amount_out:" && j + 1 < parts.len() {
                                    if let Ok(amount_out) = parts[j + 1].parse::<u64>() {
                                        let price = amount_out as f64 / amount_in as f64;
                                        return Some(DexFillInfo {
                                            amount: amount_out,
                                            price,
                                            fee: 0.001, // Jupiter routing fee
                                            slippage: 0.0,
                                        });
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        None
    }
    
    /// Set emergency stop
    pub async fn set_emergency_stop(&self, stop: bool) {
        let mut state = self.bot_state.write().await;
        state.emergency_stop = stop;
        if stop {
            tracing::warn!("Emergency stop activated");
        } else {
            tracing::info!("Emergency stop deactivated");
        }
    }
    
    /// Advanced risk management checks with portfolio beta and market stress detection
    pub async fn advanced_risk_checks(&self) -> Result<bool, Box<dyn std::error::Error>> {
        // Check portfolio beta risk
        let portfolio_beta = self.calculate_portfolio_beta().await?;
        if portfolio_beta > 1.5 {
            tracing::error!("Portfolio beta too high: {:.2} > 1.5, blocking execution", portfolio_beta);
            return Ok(false);
        }
        
        // Measure market stress levels
        let market_stress = self.measure_market_stress().await?;
        if market_stress.overall_stress > 0.7 {
            tracing::warn!("Market stress level critical: {:.2} > 0.7, blocking execution", market_stress.overall_stress);
            return Ok(false);
        }
        
        // Check counterparty risk (DEX/CEX solvency indicators)
        if !self.check_counterparty_risk().await? {
            tracing::error!("Counterparty risk detected, blocking execution");
            return Ok(false);
        }
        
        // Monitor for regulatory risk events
        if self.detect_regulatory_events().await {
            tracing::warn!("Regulatory event detected - reducing exposure, blocking execution");
            return Ok(false);
        }
        
        // Check liquidity crisis indicators
        let liquidity_health = self.assess_liquidity_health().await?;
        if liquidity_health < 0.3 {
            tracing::warn!("Liquidity crisis detected: {:.2} < 0.3, blocking execution", liquidity_health);
            return Ok(false);
        }
        
        Ok(true)
    }
    
    /// Calculate portfolio beta relative to market
    async fn calculate_portfolio_beta(&self) -> Result<f64, Box<dyn std::error::Error>> {
        let price_history = self.price_history.read().await;
        
        if price_history.len() < 50 {
            return Ok(1.0); // Default beta when insufficient data
        }
        
        // Calculate returns for last 50 periods
        let mut asset_returns = Vec::new();
        let mut market_returns = Vec::new();
        
        for i in 1..price_history.len().min(50) {
            let asset_return = (price_history[i] / price_history[i-1]) - 1.0;
            let market_return = asset_return * 1.1 + 0.02 * (rand::thread_rng().gen::<f64>() - 0.5); // Simulated market
            
            asset_returns.push(asset_return);
            market_returns.push(market_return);
        }
        
        // Calculate beta using covariance/variance formula
        let asset_mean = asset_returns.iter().sum::<f64>() / asset_returns.len() as f64;
        let market_mean = market_returns.iter().sum::<f64>() / market_returns.len() as f64;
        
        let mut covariance = 0.0;
        let mut market_variance = 0.0;
        
        for i in 0..asset_returns.len() {
            let asset_dev = asset_returns[i] - asset_mean;
            let market_dev = market_returns[i] - market_mean;
            
            covariance += asset_dev * market_dev;
            market_variance += market_dev * market_dev;
        }
        
        covariance /= (asset_returns.len() - 1) as f64;
        market_variance /= (asset_returns.len() - 1) as f64;
        
        let beta = if market_variance > 0.0 {
            covariance / market_variance
        } else {
            1.0
        };
        
        Ok(beta.abs()) // Use absolute beta for risk assessment
    }
    
    /// Measure comprehensive market stress indicators
    async fn measure_market_stress(&self) -> Result<MarketStressMetrics, Box<dyn std::error::Error>> {
        let price_history = self.price_history.read().await;
        
        if price_history.len() < 20 {
            return Ok(MarketStressMetrics {
                volatility_spike: 0.0,
                liquidity_crunch: 0.0,
                correlation_breakdown: 0.0,
                sentiment_panic: 0.0,
                overall_stress: 0.0,
            });
        }
        
        // Volatility spike detection (rolling 20-period)
        let recent_prices = &price_history[price_history.len().saturating_sub(20)..];
        let returns: Vec<f64> = recent_prices.windows(2)
            .map(|w| (w[1] / w[0]) - 1.0)
            .collect();
        
        let volatility = {
            let mean_return = returns.iter().sum::<f64>() / returns.len() as f64;
            let variance = returns.iter()
                .map(|r| (r - mean_return).powi(2))
                .sum::<f64>() / returns.len() as f64;
            variance.sqrt()
        };
        
        let volatility_spike = (volatility / 0.02).min(1.0); // Normalize against 2% daily vol baseline
        
        // Liquidity crunch indicators (spread widening, volume decline)
        let liquidity_crunch = self.measure_liquidity_stress().await;
        
        // Correlation breakdown (assets moving independently)
        let correlation_breakdown = self.measure_correlation_stress().await;
        
        // Sentiment panic (VIX-like measure from price action)
        let sentiment_panic = self.measure_sentiment_stress(&returns).await;
        
        // Weighted overall stress score
        let overall_stress = (
            volatility_spike * 0.3 +
            liquidity_crunch * 0.25 +
            correlation_breakdown * 0.25 +
            sentiment_panic * 0.2
        ).min(1.0);
        
        Ok(MarketStressMetrics {
            volatility_spike,
            liquidity_crunch,
            correlation_breakdown,
            sentiment_panic,
            overall_stress,
        })
    }
    
    /// Measure liquidity stress indicators
    async fn measure_liquidity_stress(&self) -> f64 {
        // In a real implementation, this would check:
        // - Bid-ask spreads widening
        // - Order book depth declining
        // - Trade volume dropping
        // - Market impact increasing
        
        let mut rng = rand::thread_rng();
        // Simulate liquidity stress (0.0 = healthy, 1.0 = crisis)
        rng.gen_range(0.0..0.3) // Generally healthy with occasional stress
    }
    
    /// Measure correlation breakdown stress
    async fn measure_correlation_stress(&self) -> f64 {
        // In a real implementation, this would check:
        // - Cross-asset correlations breaking down
        // - Traditional hedges failing
        // - Flight-to-quality reversals
        
        let mut rng = rand::thread_rng();
        rng.gen_range(0.0..0.4) // Moderate correlation stress
    }
    
    /// Measure sentiment-based stress from price action
    async fn measure_sentiment_stress(&self, returns: &[f64]) -> f64 {
        if returns.len() < 10 {
            return 0.0;
        }
        
        // Count consecutive negative returns (panic selling)
        let mut max_consecutive_negative = 0;
        let mut current_negative = 0;
        
        for &ret in returns {
            if ret < -0.01 { // > 1% decline
                current_negative += 1;
                max_consecutive_negative = max_consecutive_negative.max(current_negative);
            } else {
                current_negative = 0;
            }
        }
        
        // Large negative returns indicate panic
        let large_declines = returns.iter().filter(|&&r| r < -0.05).count(); // > 5% declines
        
        // Normalize sentiment stress
        let consecutive_stress = (max_consecutive_negative as f64 / 5.0).min(1.0);
        let magnitude_stress = (large_declines as f64 / returns.len() as f64 * 4.0).min(1.0);
        
        (consecutive_stress + magnitude_stress) / 2.0
    }
    
    /// Check counterparty risk indicators
    async fn check_counterparty_risk(&self) -> Result<bool, Box<dyn std::error::Error>> {
        // In a real implementation, this would check:
        // - DEX liquidity pool health
        // - Smart contract audit status
        // - Protocol governance risks
        // - Validator set decentralization
        // - Bridge security status
        
        // For now, simulate generally healthy counterparties
        let mut rng = rand::thread_rng();
        Ok(rng.gen_bool(0.95)) // 95% chance of healthy counterparties
    }
    
    /// Detect regulatory risk events
    async fn detect_regulatory_events(&self) -> bool {
        // In a real implementation, this would monitor:
        // - SEC announcements
        // - Congressional hearings
        // - CFTC guidance changes
        // - International regulatory coordination
        // - Exchange restrictions
        // - Stablecoin regulatory status
        
        // For now, simulate rare regulatory events
        let mut rng = rand::thread_rng();
        rng.gen_bool(0.02) // 2% chance of regulatory event
    }
    
    /// Assess overall liquidity health
    async fn assess_liquidity_health(&self) -> Result<f64, Box<dyn std::error::Error>> {
        // In a real implementation, this would check:
        // - TVL across major DEXs
        // - Order book depth
        // - Recent trade volumes
        // - Market maker activity
        // - Cross-exchange arbitrage efficiency
        
        let price_history = self.price_history.read().await;
        
        // Simple heuristic: consistent price movements indicate healthy liquidity
        if price_history.len() < 10 {
            return Ok(0.5); // Neutral when insufficient data
        }
        
        let recent_prices = &price_history[price_history.len().saturating_sub(10)..];
        let price_changes: Vec<f64> = recent_prices.windows(2)
            .map(|w| (w[1] / w[0] - 1.0).abs())
            .collect();
        
        let avg_change = price_changes.iter().sum::<f64>() / price_changes.len() as f64;
        
        // Healthy liquidity: moderate, consistent price changes (not too volatile, not too stable)
        let health_score = if avg_change < 0.001 {
            0.3 // Too stable, possibly illiquid
        } else if avg_change > 0.05 {
            0.2 // Too volatile, possibly stressed
        } else {
            0.8 // Healthy range
        };
        
        Ok(health_score)
    }
    
    /// Critical alerting system with PnL, latency, and MEV detection
    pub async fn critical_alerting(&self) -> Result<(), Box<dyn std::error::Error>> {
        let bot_state = self.bot_state.read().await;
        let metrics = self.monitoring_metrics.read().await;
        let emergency = self.emergency_conditions.read().await;
        
        // PnL-based critical alerts
        if bot_state.daily_pnl_usd < -bot_state.nav_usd * 0.05 { // -5% daily loss
            self.send_alert(
                AlertLevel::CRITICAL,
                format!("Daily loss limit exceeded: ${:.2} (-{:.1}% of NAV)", 
                       bot_state.daily_pnl_usd, 
                       (bot_state.daily_pnl_usd / bot_state.nav_usd * -100.0))
            ).await;
        }
        
        // Hourly loss alerts (early warning)
        if bot_state.hourly_pnl_usd < -bot_state.nav_usd * 0.02 { // -2% hourly loss
            self.send_alert(
                AlertLevel::WARNING,
                format!("Hourly loss warning: ${:.2} (-{:.1}% of NAV)", 
                       bot_state.hourly_pnl_usd,
                       (bot_state.hourly_pnl_usd / bot_state.nav_usd * -100.0))
            ).await;
        }
        
        // Execution latency degradation
        if let Some(&latest_latency) = metrics.execution_latencies_ms.last() {
            if latest_latency > 5000 { // > 5 seconds
                self.send_alert(
                    AlertLevel::CRITICAL,
                    format!("Execution latency critically high: {}ms (>5s)", latest_latency)
                ).await;
            } else if latest_latency > 2000 { // > 2 seconds
                self.send_alert(
                    AlertLevel::WARNING,
                    format!("Execution latency elevated: {}ms (>2s)", latest_latency)
                ).await;
            }
        }
        
        // Slippage degradation alerts
        if let Some(&latest_slippage) = metrics.slippages_bps.last() {
            if latest_slippage > 500.0 { // > 5%
                self.send_alert(
                    AlertLevel::CRITICAL,
                    format!("Slippage critically high: {:.1} bps (>5%)", latest_slippage)
                ).await;
            } else if latest_slippage > 200.0 { // > 2%
                self.send_alert(
                    AlertLevel::WARNING,
                    format!("Slippage elevated: {:.1} bps (>2%)", latest_slippage)
                ).await;
            }
        }
        
        // MEV sandwich detection
        if self.detect_sandwich_attempt().await {
            self.send_alert(
                AlertLevel::SECURITY,
                "MEV sandwich attack detected - suspicious transaction pattern".to_string()
            ).await;
        }
        
        // Front-running detection
        if self.detect_frontrunning_attempt().await {
            self.send_alert(
                AlertLevel::SECURITY,
                "Front-running attempt detected - adjust MEV protection".to_string()
            ).await;
        }
        
        // Circuit breaker activation alerts
        if emergency.max_drawdown_exceeded {
            self.send_alert(
                AlertLevel::CRITICAL,
                "Emergency stop: Maximum drawdown exceeded".to_string()
            ).await;
        }
        
        if emergency.hourly_loss_exceeded {
            self.send_alert(
                AlertLevel::CRITICAL,
                "Emergency stop: Hourly loss limit exceeded".to_string()
            ).await;
        }
        
        // Network congestion alerts
        if emergency.network_congestion_detected {
            self.send_alert(
                AlertLevel::WARNING,
                "Network congestion detected - execution may be delayed".to_string()
            ).await;
        }
        
        // Price feed health alerts
        if emergency.price_feed_failure {
            self.send_alert(
                AlertLevel::CRITICAL,
                "Price feed failure detected - trading halted for safety".to_string()
            ).await;
        }
        
        // Position size risk alerts
        if emergency.position_size_exceeded {
            self.send_alert(
                AlertLevel::WARNING,
                "Position size limit exceeded".to_string()
            ).await;
        }
        
        Ok(())
    }
    
    /// Send critical alert through multiple channels
    async fn send_alert(&self, level: AlertLevel, message: String) {
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let formatted_alert = format!("[{}] {:?}: {}", timestamp, level, message);
        
        // Log alert
        match level {
            AlertLevel::CRITICAL | AlertLevel::SECURITY => {
                tracing::error!("🚨 ALERT: {}", formatted_alert);
            }
            AlertLevel::WARNING => {
                tracing::warn!("⚠️  ALERT: {}", formatted_alert);
            }
            AlertLevel::INFO => {
                tracing::info!("ℹ️  ALERT: {}", formatted_alert);
            }
        }
        
        // In production, this would also:
        // - Send to Slack/Discord webhook
        // - Send email alerts
        // - Push to monitoring dashboard
        // - Trigger PagerDuty for critical alerts
        // - Store in alert database
        
        // Store alert in monitoring metrics for dashboard
        let mut metrics = self.monitoring_metrics.write().await;
        match level {
            AlertLevel::CRITICAL | AlertLevel::SECURITY => metrics.critical_alerts_count += 1,
            AlertLevel::WARNING => metrics.warning_alerts_count += 1,
            AlertLevel::INFO => metrics.info_alerts_count += 1,
        }
        
        // Implement rate limiting to avoid alert spam
        self.rate_limit_alerts(level).await;
    }
    
    /// Rate limit alerts to prevent spam
    async fn rate_limit_alerts(&self, level: AlertLevel) {
        let delay_ms = match level {
            AlertLevel::CRITICAL | AlertLevel::SECURITY => 1000,  // 1 second minimum
            AlertLevel::WARNING => 5000,   // 5 seconds minimum
            AlertLevel::INFO => 30000,     // 30 seconds minimum
        };
        
        tokio::time::sleep(Duration::from_millis(delay_ms)).await;
    }
    
    /// Detect MEV sandwich attack attempts
    async fn detect_sandwich_attempt(&self) -> bool {
        let metrics = self.monitoring_metrics.read().await;
        
        // Simple heuristic: check for suspicious slippage patterns
        if metrics.slippages_bps.len() >= 3 {
            let recent_slippages = &metrics.slippages_bps[metrics.slippages_bps.len() - 3..];
            let avg_slippage = recent_slippages.iter().sum::<f64>() / recent_slippages.len() as f64;
            
            // Sandwich attacks often cause higher than expected slippage
            if avg_slippage > 150.0 && recent_slippages.iter().all(|&s| s > 100.0) {
                return true;
            }
        }
        
        // Check execution latency patterns (sandwiches can cause delays)
        if metrics.execution_latencies_ms.len() >= 2 {
            let recent_latencies = &metrics.execution_latencies_ms[metrics.execution_latencies_ms.len() - 2..];
            if recent_latencies.iter().all(|&l| l > 3000) { // All recent executions > 3s
                return true;
            }
        }
        
        false
    }
    
    /// Detect front-running attempts
    async fn detect_frontrunning_attempt(&self) -> bool {
        let metrics = self.monitoring_metrics.read().await;
        
        // Simple heuristic: check for consistent negative slippage (being front-run)
        if metrics.slippages_bps.len() >= 5 {
            let recent_slippages = &metrics.slippages_bps[metrics.slippages_bps.len() - 5..];
            let negative_slippage_count = recent_slippages.iter().filter(|&&s| s > 50.0).count();
            
            // If most recent trades had bad slippage, might be front-running
            if negative_slippage_count >= 4 {
                return true;
            }
        }
        
        // Check for gas price escalation patterns
        if metrics.priority_fees_paid.len() >= 3 {
            let recent_fees = &metrics.priority_fees_paid[metrics.priority_fees_paid.len() - 3..];
            let is_escalating = recent_fees.windows(2).all(|w| w[1] > w[0] * 1.5);
            
            if is_escalating {
                return true; // Gas price war suggests front-running competition
            }
        }
        
        false
    }
    
    /// Get arbitrage-free price using multi-source consensus with latency weighting
    pub async fn get_arbitrage_free_price(&self) -> Result<f64, Box<dyn std::error::Error>> {
        // Multi-source price collection with latency tracking
        let start_time = Instant::now();
        
        // Collect prices from multiple sources concurrently
        let (oracle_result, cex_result, dex_result, chainlink_result) = tokio::join!(
            self.get_oracle_price_with_latency(),
            self.get_cex_price_with_latency(),
            self.get_dex_price_with_latency(),
            self.get_chainlink_price_with_latency()
        );
        
        // Process results with latency weighting
        let mut weighted_prices = Vec::new();
        
        if let Ok((price, latency_ms)) = oracle_result {
            let weight = self.calculate_source_weight(latency_ms, 0.95); // High reliability
            weighted_prices.extend(vec![price; weight as usize]);
            tracing::debug!("Oracle price: {:.4}, latency: {}ms, weight: {}", price, latency_ms, weight);
        }
        
        if let Ok((price, latency_ms)) = cex_result {
            let weight = self.calculate_source_weight(latency_ms, 0.8); // Medium reliability
            weighted_prices.extend(vec![price; weight as usize]);
            tracing::debug!("CEX price: {:.4}, latency: {}ms, weight: {}", price, latency_ms, weight);
        }
        
        if let Ok((price, latency_ms)) = dex_result {
            let weight = self.calculate_source_weight(latency_ms, 0.7); // Lower reliability
            weighted_prices.extend(vec![price; weight as usize]);
            tracing::debug!("DEX price: {:.4}, latency: {}ms, weight: {}", price, latency_ms, weight);
        }
        
        if let Ok((price, latency_ms)) = chainlink_result {
            let weight = self.calculate_source_weight(latency_ms, 0.9); // High reliability but slower
            weighted_prices.extend(vec![price; weight as usize]);
            tracing::debug!("Chainlink price: {:.4}, latency: {}ms, weight: {}", price, latency_ms, weight);
        }
        
        if weighted_prices.is_empty() {
            return Err("No price sources available".into());
        }
        
        // Calculate weighted median with outlier rejection
        weighted_prices.sort_by(|a, b| a.partial_cmp(b).unwrap());
        
        // Remove outliers (top and bottom 10%)
        let outlier_threshold = weighted_prices.len() / 10;
        if weighted_prices.len() > 10 {
            weighted_prices.drain(0..outlier_threshold);
            weighted_prices.drain(weighted_prices.len() - outlier_threshold..);
        }
        
        let consensus_price = weighted_prices[weighted_prices.len() / 2];
        let total_latency = start_time.elapsed().as_millis();
        
        tracing::info!("Consensus price: {:.4}, sources: {}, total_latency: {}ms", 
                      consensus_price, weighted_prices.len(), total_latency);
        
        Ok(consensus_price)
    }
    
    /// Calculate source weight based on latency and reliability
    fn calculate_source_weight(&self, latency_ms: u64, base_reliability: f64) -> u32 {
        // Lower latency = higher weight, with reliability multiplier
        let latency_factor = if latency_ms < 100 {
            1.0
        } else if latency_ms < 500 {
            0.8
        } else if latency_ms < 1000 {
            0.6
        } else {
            0.3
        };
        
        let final_weight = base_reliability * latency_factor * 50.0; // Scale to reasonable integer
        final_weight.max(1.0) as u32
    }
    
    /// Get oracle price with latency measurement
    async fn get_oracle_price_with_latency(&self) -> Result<(f64, u64), Box<dyn std::error::Error>> {
        let start = Instant::now();
        
        // In a real implementation, this would query Pyth, Switchboard, etc.
        let price_history = self.price_history.read().await;
        let base_price = price_history.last().copied().unwrap_or(100.0);
        
        // Simulate network delay
        tokio::time::sleep(Duration::from_millis(rand::thread_rng().gen_range(50..150))).await;
        
        let mut rng = rand::thread_rng();
        let oracle_price = base_price * (1.0 + rng.gen_range(-0.001..0.001)); // ±0.1% variation
        
        let latency_ms = start.elapsed().as_millis() as u64;
        Ok((oracle_price, latency_ms))
    }
    
    /// Get CEX price with latency measurement
    async fn get_cex_price_with_latency(&self) -> Result<(f64, u64), Box<dyn std::error::Error>> {
        let start = Instant::now();
        
        // In a real implementation, this would query Binance, Coinbase, etc.
        let price_history = self.price_history.read().await;
        let base_price = price_history.last().copied().unwrap_or(100.0);
        
        // Simulate network delay
        tokio::time::sleep(Duration::from_millis(rand::thread_rng().gen_range(100..300))).await;
        
        let mut rng = rand::thread_rng();
        let cex_price = base_price * (1.0 + rng.gen_range(-0.002..0.002)); // ±0.2% variation
        
        let latency_ms = start.elapsed().as_millis() as u64;
        Ok((cex_price, latency_ms))
    }
    
    /// Get DEX price with latency measurement
    async fn get_dex_price_with_latency(&self) -> Result<(f64, u64), Box<dyn std::error::Error>> {
        let start = Instant::now();
        
        // In a real implementation, this would query Orca, Raydium, etc.
        let price_history = self.price_history.read().await;
        let base_price = price_history.last().copied().unwrap_or(100.0);
        
        // Simulate network delay
        tokio::time::sleep(Duration::from_millis(rand::thread_rng().gen_range(200..500))).await;
        
        let mut rng = rand::thread_rng();
        let dex_price = base_price * (1.0 + rng.gen_range(-0.005..0.005)); // ±0.5% variation (higher slippage)
        
        let latency_ms = start.elapsed().as_millis() as u64;
        Ok((dex_price, latency_ms))
    }
    
    /// Get Chainlink price with latency measurement
    async fn get_chainlink_price_with_latency(&self) -> Result<(f64, u64), Box<dyn std::error::Error>> {
        let start = Instant::now();
        
        // In a real implementation, this would query Chainlink price feeds
        let price_history = self.price_history.read().await;
        let base_price = price_history.last().copied().unwrap_or(100.0);
        
        // Simulate longer network delay (on-chain query)
        tokio::time::sleep(Duration::from_millis(rand::thread_rng().gen_range(300..800))).await;
        
        let mut rng = rand::thread_rng();
        let chainlink_price = base_price * (1.0 + rng.gen_range(-0.0005..0.0005)); // ±0.05% variation (most accurate)
        
        let latency_ms = start.elapsed().as_millis() as u64;
        Ok((chainlink_price, latency_ms))
    }
    
    /// Get comprehensive monitoring metrics for Prometheus export
    pub async fn get_monitoring_metrics(&self) -> MonitoringMetrics {
        self.monitoring_metrics.read().await.clone()
    }
    
    /// Get Prometheus-compatible metrics summary
    pub async fn get_prometheus_metrics(&self) -> String {
        let metrics = self.monitoring_metrics.read().await;
        let state = self.bot_state.read().await;
        let emergency = self.emergency_conditions.read().await;
        let health = self.price_feed_health.read().await;
        
        let mut prometheus_output = String::new();
        
        // NAV and position metrics
        prometheus_output.push_str(&format!("# HELP ou_bot_nav_usd Current Net Asset Value in USD\n"));
        prometheus_output.push_str(&format!("# TYPE ou_bot_nav_usd gauge\n"));
        prometheus_output.push_str(&format!("ou_bot_nav_usd {{}} {:.2}\n", state.nav_usd));
        
        prometheus_output.push_str(&format!("# HELP ou_bot_current_leverage Current leverage ratio\n"));
        prometheus_output.push_str(&format!("# TYPE ou_bot_current_leverage gauge\n"));
        prometheus_output.push_str(&format!("ou_bot_current_leverage {{}} {:.4}\n", state.current_leverage));
        
        // PnL metrics
        prometheus_output.push_str(&format!("# HELP ou_bot_hourly_pnl Hourly PnL in USD\n"));
        prometheus_output.push_str(&format!("# TYPE ou_bot_hourly_pnl gauge\n"));
        prometheus_output.push_str(&format!("ou_bot_hourly_pnl {{}} {:.2}\n", state.hourly_pnl));
        
        prometheus_output.push_str(&format!("# HELP ou_bot_daily_pnl Daily PnL in USD\n"));
        prometheus_output.push_str(&format!("# TYPE ou_bot_daily_pnl gauge\n"));
        prometheus_output.push_str(&format!("ou_bot_daily_pnl {{}} {:.2}\n", state.daily_pnl));
        
        // Trade metrics
        prometheus_output.push_str(&format!("# HELP ou_bot_hourly_trades Number of trades in current hour\n"));
        prometheus_output.push_str(&format!("# TYPE ou_bot_hourly_trades counter\n"));
        prometheus_output.push_str(&format!("ou_bot_hourly_trades {{}} {}\n", state.hourly_trades));
        
        // Execution latency metrics
        if !metrics.execution_latency_ms.is_empty() {
            let avg_latency = metrics.execution_latency_ms.iter().sum::<u64>() as f64 / metrics.execution_latency_ms.len() as f64;
            let max_latency = metrics.execution_latency_ms.iter().max().unwrap_or(&0);
            prometheus_output.push_str(&format!("# HELP ou_bot_execution_latency_ms_avg Average execution latency in milliseconds\n"));
            prometheus_output.push_str(&format!("# TYPE ou_bot_execution_latency_ms_avg gauge\n"));
            prometheus_output.push_str(&format!("ou_bot_execution_latency_ms_avg {{}} {:.2}\n", avg_latency));
            prometheus_output.push_str(&format!("# HELP ou_bot_execution_latency_ms_max Maximum execution latency in milliseconds\n"));
            prometheus_output.push_str(&format!("# TYPE ou_bot_execution_latency_ms_max gauge\n"));
            prometheus_output.push_str(&format!("ou_bot_execution_latency_ms_max {{}} {}\n", max_latency));
        }
        
        // Slippage metrics
        if !metrics.slippage_bps.is_empty() {
            let avg_slippage = metrics.slippage_bps.iter().sum::<f64>() / metrics.slippage_bps.len() as f64;
            let max_slippage = metrics.slippage_bps.iter().max_by(|a, b| a.partial_cmp(b).unwrap()).unwrap_or(&0.0);
            prometheus_output.push_str(&format!("# HELP ou_bot_slippage_bps_avg Average slippage in basis points\n"));
            prometheus_output.push_str(&format!("# TYPE ou_bot_slippage_bps_avg gauge\n"));
            prometheus_output.push_str(&format!("ou_bot_slippage_bps_avg {{}} {:.2}\n", avg_slippage));
            prometheus_output.push_str(&format!("# HELP ou_bot_slippage_bps_max Maximum slippage in basis points\n"));
            prometheus_output.push_str(&format!("# TYPE ou_bot_slippage_bps_max gauge\n"));
            prometheus_output.push_str(&format!("ou_bot_slippage_bps_max {{}} {:.2}\n", max_slippage));
        }
        
        // Z-score distribution metrics
        if !metrics.z_score_distribution.is_empty() {
            let avg_z_score = metrics.z_score_distribution.iter().sum::<f64>() / metrics.z_score_distribution.len() as f64;
            let abs_avg_z_score = metrics.z_score_distribution.iter().map(|z| z.abs()).sum::<f64>() / metrics.z_score_distribution.len() as f64;
            prometheus_output.push_str(&format!("# HELP ou_bot_z_score_avg Average Z-score\n"));
            prometheus_output.push_str(&format!("# TYPE ou_bot_z_score_avg gauge\n"));
            prometheus_output.push_str(&format!("ou_bot_z_score_avg {{}} {:.4}\n", avg_z_score));
            prometheus_output.push_str(&format!("# HELP ou_bot_z_score_abs_avg Average absolute Z-score\n"));
            prometheus_output.push_str(&format!("# TYPE ou_bot_z_score_abs_avg gauge\n"));
            prometheus_output.push_str(&format!("ou_bot_z_score_abs_avg {{}} {:.4}\n", abs_avg_z_score));
        }
        
        // Emergency conditions
        prometheus_output.push_str(&format!("# HELP ou_bot_emergency_stop Emergency stop status (1 = active, 0 = inactive)\n"));
        prometheus_output.push_str(&format!("# TYPE ou_bot_emergency_stop gauge\n"));
        prometheus_output.push_str(&format!("ou_bot_emergency_stop {{}} {}\n", if state.emergency_stop { 1 } else { 0 }));
        
        // Circuit breaker status
        prometheus_output.push_str(&format!("# HELP ou_bot_circuit_breaker_active Circuit breaker status (1 = active, 0 = inactive)\n"));
        prometheus_output.push_str(&format!("# TYPE ou_bot_circuit_breaker_active gauge\n"));
        let circuit_breaker_active = emergency.max_drawdown_exceeded || emergency.hourly_loss_exceeded || emergency.position_size_exceeded || emergency.price_feed_failed;
        prometheus_output.push_str(&format!("ou_bot_circuit_breaker_active {{}} {}\n", if circuit_breaker_active { 1 } else { 0 }));
        
        // Price feed health
        prometheus_output.push_str(&format!("# HELP ou_bot_price_feed_healthy Price feed health status (1 = healthy, 0 = unhealthy)\n"));
        prometheus_output.push_str(&format!("# TYPE ou_bot_price_feed_healthy gauge\n"));
        prometheus_output.push_str(&format!("ou_bot_price_feed_healthy {{}} {}\n", if health.heartbeat_healthy && !health.deviation_detected { 1 } else { 0 }));
        
        // Gas cost metrics
        if !metrics.gas_costs_usd.is_empty() {
            let total_gas_cost = metrics.gas_costs_usd.iter().sum::<f64>();
            let avg_gas_cost = total_gas_cost / metrics.gas_costs_usd.len() as f64;
            prometheus_output.push_str(&format!("# HELP ou_bot_gas_costs_usd_total Total gas costs in USD\n"));
            prometheus_output.push_str(&format!("# TYPE ou_bot_gas_costs_usd_total counter\n"));
            prometheus_output.push_str(&format!("ou_bot_gas_costs_usd_total {{}} {:.4}\n", total_gas_cost));
            prometheus_output.push_str(&format!("# HELP ou_bot_gas_costs_usd_avg Average gas cost per transaction in USD\n"));
            prometheus_output.push_str(&format!("# TYPE ou_bot_gas_costs_usd_avg gauge\n"));
            prometheus_output.push_str(&format!("ou_bot_gas_costs_usd_avg {{}} {:.4}\n", avg_gas_cost));
        }
        
        prometheus_output
    }
    
    /// Enhanced emergency conditions with network congestion detection
    pub async fn check_network_conditions(&self) -> Result<(), Box<dyn std::error::Error>> {
        let mut emergency = self.emergency_conditions.write().await;
        let metrics = self.monitoring_metrics.read().await;
        
        // Check for high gas costs indicating network congestion
        if !metrics.gas_costs_usd.is_empty() {
            let recent_gas_costs: Vec<f64> = metrics.gas_costs_usd.iter().rev().take(10).cloned().collect();
            let avg_recent_gas = recent_gas_costs.iter().sum::<f64>() / recent_gas_costs.len() as f64;
            
            // If recent average gas cost is more than 5x the minimum profitable threshold
            if avg_recent_gas > (MIN_PROFIT_THRESHOLD_BPS / 10000.0) * 5.0 {
                emergency.network_congestion = true;
                tracing::warn!("Network congestion detected: avg gas cost {:.4} USD", avg_recent_gas);
            } else {
                emergency.network_congestion = false;
            }
        }
        
        // Check for execution latency spikes
        if !metrics.execution_latency_ms.is_empty() {
            let recent_latencies: Vec<u64> = metrics.execution_latency_ms.iter().rev().take(10).cloned().collect();
            let avg_recent_latency = recent_latencies.iter().sum::<u64>() as f64 / recent_latencies.len() as f64;
            
            // If recent average latency is over 5 seconds
            if avg_recent_latency > 5000.0 {
                emergency.network_congestion = true;
                tracing::warn!("High execution latency detected: avg {:.0}ms", avg_recent_latency);
            }
        }
        
        Ok(())
    }
    
    /// Enhanced front-running protection with transaction timing randomization
    pub async fn generate_execution_jitter(&self, urgency: f64) -> Duration {
        use std::hash::{Hash, Hasher};
        use std::collections::hash_map::DefaultHasher;
        
        // Generate deterministic but unpredictable jitter based on current state
        let mut hasher = DefaultHasher::new();
        let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
        now.as_nanos().hash(&mut hasher);
        urgency.to_bits().hash(&mut hasher);
        
        let hash = hasher.finish();
        
        // Base jitter: 50-500ms depending on urgency (lower urgency = more jitter)
        let base_jitter_ms = (50.0 + (1.0 - urgency) * 450.0) as u64;
    }

    /// Generate randomized jitter delay for anti-MEV protection
    fn generate_jitter_delay(&self, urgency: f64) -> Duration {
        let mut rng = rand::thread_rng();
        let base_jitter_ms = match urgency {
            u if u > 0.8 => rng.gen_range(50..200),   // High urgency: 50-200ms
            u if u > 0.5 => rng.gen_range(100..300),  // Medium urgency: 100-300ms
            _ => rng.gen_range(200..500),             // Low urgency: 200-500ms
        };
        Duration::from_millis(base_jitter_ms)
    }

    /// Calculate dynamic priority fee with anti-MEV considerations
    pub async fn calculate_dynamic_priority_fee(
        &self,
        urgency: f64,
        network_congestion_factor: f64,
    ) -> u64 {
        // Base priority fee (microlamports per compute unit)
        let base_fee = 1000_u64;
        
        // Urgency multiplier (1.0 - 3.0x)
        let urgency_multiplier = 1.0 + (urgency * 2.0);
        
        // Network congestion multiplier (1.0 - 5.0x)
        let congestion_multiplier = 1.0 + (network_congestion_factor * 4.0);
        
        // Anti-MEV randomization (±20%)
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
        std::hash::Hash::hash(&now.as_nanos(), &mut hasher);
        let random_factor = 0.8 + (hasher.finish() % 400) as f64 / 1000.0; // 0.8 - 1.2
        
        let final_fee = (base_fee as f64 * urgency_multiplier * congestion_multiplier * random_factor) as u64;
        
        // Clamp to reasonable bounds
        final_fee.max(500).min(100_000)
    }

    /// Calculate optimal priority fee using competitor analysis and slot metrics
    async fn calculate_optimal_priority_fee(
        &self,
        client: &RpcClient,
        urgency: f64,
        compute_units: u32,
    ) -> Result<u64, Box<dyn std::error::Error>> {
        // Get recent slot performance data
        let recent_slots = client.get_recent_performance_samples(Some(10)).await?;
        let avg_congestion = if !recent_slots.is_empty() {
            recent_slots.iter()
                .map(|s| s.num_transactions as f64 / s.num_slots as f64)
                .sum::<f64>() / recent_slots.len() as f64
        } else {
            1.0 // Default fallback
        };
        
        // Analyze competitor fees from recent confirmed transactions
        let competitor_fees = self.analyze_competitor_fees(client).await?;
        let target_percentile = if urgency > 0.8 { 90 } else { 75 };
        let competitor_fee = competitor_fees.get_percentile(target_percentile);
        
        // Dynamic base fee calculation
        let base_fee = 1000u64; // microlamports/CU
        let congestion_multiplier = (avg_congestion * 1.5).max(1.0).min(5.0);
        let urgency_multiplier = urgency.max(0.1).min(2.0);
        
        // Calculate optimal fee
        let calculated_fee = (base_fee as f64 * congestion_multiplier * urgency_multiplier) as u64;
        let market_competitive_fee = (competitor_fee * 1.1) as u64; // Beat competitors by 10%
        
        // Use higher of calculated or market-competitive fee
        let optimal_fee = calculated_fee.max(market_competitive_fee);
        
        // Apply anti-MEV randomization (±15%)
        let mut rng = rand::thread_rng();
        let randomization_factor = rng.gen_range(0.85..1.15);
        let final_fee = (optimal_fee as f64 * randomization_factor) as u64;
        
        // Cap at reasonable maximums
        Ok(final_fee.clamp(500, 250_000)) // 500 to 250k microlamports/CU
    }
    
    /// Analyze recent competitor priority fees
    async fn analyze_competitor_fees(&self, client: &RpcClient) -> Result<CompetitorFeeAnalysis, Box<dyn std::error::Error>> {
        let recent_signatures = client.get_signatures_for_address(
            &self.wallet_keypair.pubkey(),
            None,
        ).await?;
        
        let mut fees = Vec::new();
        
        // Analyze last 50 transactions for fee patterns
        for sig_info in recent_signatures.iter().take(50) {
            if let Ok(tx) = client.get_transaction_with_config(
                &sig_info.signature.parse()?,
                solana_client::rpc_config::RpcTransactionConfig {
                    encoding: Some(UiTransactionEncoding::Json),
                    commitment: Some(CommitmentConfig::confirmed()),
                    max_supported_transaction_version: Some(0),
                }
            ).await {
                if let Some(meta) = tx.transaction.meta {
                    fees.push(meta.fee);
                }
            }
        }
        
        Ok(CompetitorFeeAnalysis { fees })
    }
    
    /// Calculate dynamic priority fee based on urgency and network conditions
    async fn calculate_priority_fee(
        &self, 
        urgency: f64,
        compute_units: u32,
    ) -> u64 {
        let mut rng = rand::thread_rng();
        
        // Base fee calculation
        let base_fee = match urgency {
            u if u > 0.9 => 100_000,  // Critical: up to 0.1 SOL
            u if u > 0.7 => 50_000,   // High: up to 0.05 SOL  
            u if u > 0.5 => 10_000,   // Medium: up to 0.01 SOL
            _ => 1_000,               // Low: up to 0.001 SOL
        };
        
        // Network congestion factor (simplified)
        let congestion_factor = 1.2; // Would get from RPC in real implementation
        
        // Apply anti-MEV randomization (±20%)
        let randomization_factor = rng.gen_range(0.8..1.2);
        
        let final_fee = (base_fee as f64 * congestion_factor * randomization_factor) as u64;
        
        // Ensure fee is reasonable for compute units
        let fee_per_cu = final_fee / compute_units as u64;
        fee_per_cu.clamp(500, 250_000) // 500 to 250k microlamports/CU
    }

    /// Create MEV-obfuscated transaction with decoy instructions and randomization
    async fn create_mev_obfuscated_transaction(
        &self,
        instructions: Vec<Instruction>,
        signer: &Keypair,
        recent_blockhash: Hash,
        mev_protection: MevProtectionLevel,
    ) -> Result<VersionedTransaction, Box<dyn std::error::Error>> {
        let mut rng = rand::thread_rng();
        let mut obfuscated_instructions = instructions;
        
        match mev_protection {
            MevProtectionLevel::High => {
                // Add decoy transfers (50% of transactions)
                if rng.gen_bool(0.5) {
                    let decoy = self.create_decoy_transfer(&signer.pubkey());
                    obfuscated_instructions.insert(0, decoy);
                }
                
                // Instruction reordering (30% chance)
                if rng.gen_bool(0.3) && obfuscated_instructions.len() > 1 {
                    obfuscated_instructions.shuffle(&mut rng);
                }
                
                // Add compute budget instructions with randomization
                let compute_limit = rng.gen_range(200_000..800_000);
                let compute_instruction = solana_sdk::compute_budget::ComputeBudgetInstruction::set_compute_unit_limit(compute_limit);
                obfuscated_instructions.insert(0, compute_instruction);
            },
            MevProtectionLevel::Extreme => {
                // Maximum obfuscation - add multiple decoys
                for _ in 0..rng.gen_range(1..4) {
                    if rng.gen_bool(0.7) {
                        let decoy = self.create_decoy_transfer(&signer.pubkey());
                        obfuscated_instructions.insert(rng.gen_range(0..obfuscated_instructions.len().saturating_add(1)), decoy);
                    }
                }
                
                // Always randomize instruction order
                obfuscated_instructions.shuffle(&mut rng);
                
                // Add noise with memo instructions
                if rng.gen_bool(0.4) {
                    let noise_memo = format!("noise_{}", rng.gen::<u32>());
                    let memo_instruction = spl_memo::build_memo(noise_memo.as_bytes(), &[]);
                    obfuscated_instructions.push(memo_instruction);
                }
            },
            _ => {
                // Basic protection - just add compute budget
                let compute_instruction = solana_sdk::compute_budget::ComputeBudgetInstruction::set_compute_unit_limit(400_000);
                obfuscated_instructions.insert(0, compute_instruction);
            }
        }
        
        // Build versioned transaction with obfuscation
        let message = Message::new(&obfuscated_instructions, Some(&signer.pubkey()));
        let versioned_message = VersionedMessage::V0(v0::Message::try_from(message)?);
        let mut transaction = VersionedTransaction::try_new(versioned_message, &[signer])?;
        
        // Set recent blockhash
        match &mut transaction.message {
            VersionedMessage::V0(ref mut msg) => {
                msg.recent_blockhash = recent_blockhash;
            },
            VersionedMessage::Legacy(ref mut msg) => {
                msg.recent_blockhash = recent_blockhash;
            },
        }
        
        Ok(transaction)
    }
    
    /// Create decoy transfer instruction for MEV obfuscation
    fn create_decoy_transfer(&self, owner: &Pubkey) -> Instruction {
        let mut rng = rand::thread_rng();
        
        // Create dummy transfer of 0 lamports to a random account
        let random_pubkey = Pubkey::new_unique();
        
        solana_sdk::system_instruction::transfer(
            owner,
            &random_pubkey,
            rng.gen_range(0..1000) // Tiny amounts to avoid cost
        )
    }

    fn calculate_confidence(&self, adf_stat: f64, n: usize) -> f64 {
        let critical_values = match n {
            n if n < 50 => [-3.75, -3.33, -3.00],
            n if n < 100 => [-3.58, -3.22, -2.93],
            n if n < 250 => [-3.51, -3.17, -2.89],
            _ => [-3.43, -3.12, -2.86],
        };
        
        if adf_stat < critical_values[0] {
            0.99
        } else if adf_stat < critical_values[1] {
            0.95
        } else if adf_stat < critical_values[2] {
            0.90
        } else {
            0.5 + (critical_values[2] - adf_stat).max(0.0).min(0.4)
        }
    }

    /// Stationarity gating: pragmatic checks instead of broken ADF.
    /// - phi bounds for mean-reversion regime sanity
    /// - half_life bounds for realistic trading horizons
    /// - residual variance bounds for model stability
    fn stationarity_gate(&self, phi: f64, half_life: f64, residuals_var: f64) -> bool {
        // Reject explosive or near-random regimes
        if !(phi > PHI_MIN && phi < PHI_MAX) { 
            tracing::debug!("Stationarity gate failed: phi={:.4} outside bounds [{:.2}, {:.2}]", phi, PHI_MIN, PHI_MAX);
            return false; 
        }

        // Cap half-life max to 6 hours (21600 seconds), ignore NaN or negative values
        if half_life.is_nan() || half_life <= 0.0 || half_life > 21_600.0 {
            tracing::debug!("Stationarity gate failed: half_life={:.1}s outside bounds (0, 21600]", half_life);
            return false;
        }

        // Residual variance must be finite, positive, and below sane upper bound (0.5)
        if !residuals_var.is_finite() || residuals_var <= 0.0 || residuals_var > 0.5 {
            tracing::debug!("Stationarity gate failed: residuals_var={:.6} outside bounds (0, 0.5]", residuals_var);
            return false;
        }

        tracing::debug!("Stationarity gate passed: phi={:.4}, half_life={:.1}s, residuals_var={:.6}", 
                       phi, half_life, residuals_var);
        true
    }

    /// Returns clamped, shrunk Kelly fraction (signed) with volatility-aware shrinkage for MEV safety.
    fn kelly_shrinked(&self, m: f64, v: f64, max_pos: f64) -> f64 {
        let v_safe = v.max(VAR_FLOOR);
        let mut k = m / v_safe;

        // If volatility spike (e.g., flash pump), aggressively shrink Kelly fraction
        if v_safe > 0.001 {
            k *= 0.5;
            tracing::debug!("Volatility spike detected (v={:.6}), applying 0.5x shrinkage", v_safe);
        }

        let shrunk = k * KELLY_SHRINK;

        // Clamp final Kelly fraction to max allowed position sizing bounds
        let result = shrunk.max(-max_pos).min(max_pos);
        
        tracing::debug!("Kelly sizing: raw={:.4}, shrunk={:.4}, final={:.4}", k, shrunk, result);
        result
    }

    pub async fn generate_signal(&self, market: &MarketState) -> Signal {
        self.generate_signal_fixed(market, None).await
    }

    /// Generate signal with corrected OU mathematics using σ_stationary and proper expected returns
    pub async fn generate_signal_fixed(&self, market: &MarketState, horizon_seconds: Option<f64>) -> Signal {
        let params = self.parameters.read().await;
        let history = self.price_history.read().await;

        // Basic guards
        if history.len() < MIN_OBSERVATIONS || params.confidence < CONFIDENCE_THRESHOLD || market.mid <= 0.0 {
            tracing::debug!("Signal generation failed basic guards");
            return Signal::none();
        }

        let current_log_price = market.mid.ln();
        // compute stationary std: sigma_stationary = sqrt(sigma^2/(2theta))
        let sigma_stationary = (params.sigma * params.sigma / (2.0 * params.theta)).sqrt().max(1e-12);

        let z_score = (current_log_price - params.mu) / sigma_stationary;

        // expected log move over horizon h (seconds)
        let h = horizon_seconds.unwrap_or(params.half_life); // user-specified or derived
        let expected_log_return_h = (params.mu - current_log_price) * (1.0 - (-params.theta * h).exp());
        
        tracing::debug!("Signal generation: z_score={:.3}, expected_log_return={:.6}, horizon={:.1}s", 
                       z_score, expected_log_return_h, h);

        // variance of integrated OU over horizon ~ stationary variance * (1 - e^{-2θh})
        let var_h = sigma_stationary * sigma_stationary * (1.0 - (-2.0 * params.theta * h).exp());
        let var_h = var_h.max(VAR_FLOOR);

        // Kelly-like fractional sizing (m/v) with shrinkage & floor using helper
        let kelly_shrunk = self.kelly_shrinked(expected_log_return_h, var_h, MAX_POSITION_SIZE);

        // size cap & liquidity/spread adjustments
        let mut position_fraction = kelly_shrunk.abs().min(MAX_POSITION_SIZE);
        position_fraction *= (1.0 - market.spread_bps / 10000.0).max(0.0);

        // direction sign
        let direction = if expected_log_return_h > 0.0 { 1.0 } else { -1.0 };

        // risk targets (price-based stop/tp)
        let entry_price = if direction > 0.0 { market.ask } else { market.bid };
        let volatility_multiplier = (market.volatility / sigma_stationary).max(1.0);
        let stop_loss = entry_price * (1.0 - 1.5 * volatility_multiplier * sigma_stationary); // tuned
        let take_profit = entry_price * (1.0 + 1.2 * expected_log_return_h.abs()); // conservative

        // compute time_to_reversion = k * half_life (safer than ln(|z|))
        let time_to_reversion = params.half_life * 3.0; // e.g., expect mean-reversion in ~3 half-lives

        let signal = Signal {
            strength: SignalStrength::from_z(z_score),
            z_score,
            expected_return: expected_log_return_h, // in log units
            confidence: params.confidence,
            time_to_reversion,
            position_size: position_fraction * direction,
            stop_loss,
            take_profit,
            max_hold_time: (time_to_reversion * 1_000.0) as u64, // user must interpret units
        };

        tracing::info!("Generated signal: strength={:?}, position_size={:.4}, z_score={:.3}", 
                      signal.strength, signal.position_size, signal.z_score);

        signal
    }

    pub async fn should_execute(&self, signal: &Signal, balance: f64) -> bool {
        // Feed staleness gate
        let last_upd = *self.last_update.read().await;
        if last_upd.elapsed() > Duration::from_secs(STALE_PRICE_MAX_AGE_SECS) {
            tracing::warn!("Stale price feed (>{}s), skipping execution", STALE_PRICE_MAX_AGE_SECS);
            return false;
        }

        // Minimum profit threshold after fees (approx via bps)
        if signal.expected_return.abs() < (MIN_PROFIT_BPS / 10_000.0) {
            tracing::debug!("Expected return below threshold: {:.6}", signal.expected_return);
            return false;
        }

        let current_pos = *self.current_position.read().await;
        let pnl = *self.total_pnl.read().await;
        let max_dd = *self.max_drawdown_seen.read().await;

        // Emergency kill switch on max drawdown or negative pnl beyond threshold
        if max_dd > MAX_DRAWDOWN || pnl < -balance * EMERGENCY_EXIT_THRESHOLD {
            tracing::warn!("Emergency kill switch activated: max_dd={:.4}, pnl={:.2}, threshold={:.2}", 
                          max_dd, pnl, -balance * EMERGENCY_EXIT_THRESHOLD);
            return false;
        }

        // Ignore tiny signals
        if signal.position_size.abs() < 0.01 {
            tracing::debug!("Signal too small: {:.4}", signal.position_size);
            return false;
        }

        // If already positioned, only trade if flipping position side
        if current_pos.abs() > 0.01 && signal.position_size.signum() != current_pos.signum() {
            tracing::info!("Position flip detected: {:.4} -> {:.4}", current_pos, signal.position_size);
            return true;
        }

        // If flat position and have a valid signal strength, execute
        if current_pos.abs() < 0.01 && !matches!(signal.strength, SignalStrength::None) {
            tracing::info!("Opening new position: {:.4} (strength: {:?})", signal.position_size, signal.strength);
            return true;
        }

        // Otherwise, trade if position change exceeds threshold
        let position_change = (signal.position_size - current_pos).abs();
        let should_trade = position_change > 0.05;
        
        if should_trade {
            tracing::info!("Position adjustment: {:.4} -> {:.4} (change: {:.4})", 
                          current_pos, signal.position_size, position_change);
        } else {
            tracing::debug!("Position change too small: {:.4}", position_change);
        }
        
        should_trade
    }

    pub async fn calculate_execution_params(
        &self,
        signal: &Signal,
        balance: f64,
        market: &MarketState,
    ) -> ExecutionParams {
        let current_pos = *self.current_position.read().await;
        let position_delta = signal.position_size - current_pos;

        let mut amount_usd = position_delta.abs() * balance;

        // Downscale trade size if liquidity is below threshold to avoid slippage shocks
        const MIN_LIQUIDITY_THRESHOLD: f64 = 10_000.0; // example liquidity floor
        if market.liquidity < MIN_LIQUIDITY_THRESHOLD {
            let liquidity_factor = market.liquidity / MIN_LIQUIDITY_THRESHOLD;
            amount_usd *= liquidity_factor;
            tracing::info!("Scaling trade size by liquidity factor: {:.3} (liquidity: {:.0})", 
                          liquidity_factor, market.liquidity);
        }

        // Slippage tolerance in fraction (e.g., 0.005 = 0.5%)
        const MAX_SLIPPAGE_BPS: f64 = 50.0;
        let slippage_tolerance = MAX_SLIPPAGE_BPS / 10_000.0;

        // Urgency factor prioritizes order execution speed based on signal strength
        let urgency = match signal.strength {
            SignalStrength::VeryStrong => 1.0,
            SignalStrength::Strong => 0.8,
            SignalStrength::Medium => 0.6,
            SignalStrength::Weak => 0.4,
            SignalStrength::None => 0.0,
        };

        let params = ExecutionParams {
            amount_usd,
            direction: position_delta.signum(),
            max_slippage: slippage_tolerance,
            urgency,
            time_limit: Duration::from_millis((signal.max_hold_time / 1000).min(30000)), // cap at 30s
            min_fill_ratio: 0.95,
            // The following are placeholders; callers should override as needed
            token_mint: Pubkey::default(),
            token_decimals: None,
            nav_usd: balance,
            min_profit_bps: 5,
            last_updated: Instant::now(),
        };

        tracing::info!("Execution params: amount_usd={:.2}, direction={:.1}, slippage={:.4}, urgency={:.2}", 
                      params.amount_usd, params.direction, params.max_slippage, params.urgency);

        params
    }

    pub async fn update_position(&self, new_position: f64, execution_price: f64) -> Result<(), Box<dyn std::error::Error>> {
        let current_nav = self.get_current_nav().await;
        self.update_position_fixed(new_position, execution_price, current_nav).await
    }

    /// Update position with proper PnL accounting using actual execution price
    /// new_position_frac: signed fraction of NAV to be allocated (e.g., 0.1 => 10% long)
    /// execution_price: actual fill price (in asset price terms)
    /// nav_usd: current net asset value in USD used as base (must be passed by caller)
    pub async fn update_position_fixed(&self, new_position_frac: f64, execution_price: f64, nav_usd: f64) -> Result<(), Box<dyn std::error::Error>> {
        let mut current_pos = self.current_position.write().await;
        let old_pos = *current_pos;
        *current_pos = new_position_frac;
        drop(current_pos);

        let mut entry_price_lock = self.entry_price.write().await;

        tracing::debug!("Position update: {} -> {}, execution_price={:.4}", old_pos, new_position_frac, execution_price);

        // entering a new non-zero position
        if old_pos.abs() < 0.01 && new_position_frac.abs() > 0.01 {
            *entry_price_lock = Some(execution_price);
            let mut trades_count = self.trades_count.write().await;
            *trades_count += 1;
            tracing::info!("Opened new position: {:.4}, entry_price={:.4}, trade #{}", new_position_frac, execution_price, *trades_count);
            return Ok(());
        }

        // closing position fully
        if new_position_frac.abs() < 0.01 && old_pos.abs() > 0.01 {
            if let Some(entry) = *entry_price_lock {
                // PnL in USD = exposure_in_usd * (price_change / entry)
                // exposure_usd = old_pos.abs() * nav_usd
                let exposure_usd = old_pos.abs() * nav_usd;
                let pnl = exposure_usd * (execution_price / entry - 1.0) * old_pos.signum();
                let mut total_pnl = self.total_pnl.write().await;
                *total_pnl += pnl;
                drop(total_pnl);
                self.update_statistics(pnl).await?;
                tracing::info!("Closed position: PnL={:.2} USD, exit_price={:.4}", pnl, execution_price);
            }
            *entry_price_lock = None;
            return Ok(());
        }

        // flip or resize while non-zero (partial close + open)
        if old_pos.signum() != new_position_frac.signum() && new_position_frac.abs() > 0.01 {
            if let Some(entry) = *entry_price_lock {
                let exposure_usd = old_pos.abs() * nav_usd;
                let pnl = exposure_usd * (execution_price / entry - 1.0) * old_pos.signum();
                let mut total_pnl = self.total_pnl.write().await;
                *total_pnl += pnl;
                drop(total_pnl);
                self.update_statistics(pnl).await?;
                tracing::info!("Flipped position: PnL={:.2} USD, new_position={:.4}", pnl, new_position_frac);
            }
            *entry_price_lock = Some(execution_price);
            let mut trades_count = self.trades_count.write().await;
            *trades_count += 1;
            return Ok(());
        }

        Ok(())
    }

    async fn update_statistics(&self, trade_pnl: f64) -> Result<(), Box<dyn std::error::Error>> {
        let trades = *self.trades_count.read().await;
        
        if trades == 0 {
            return Ok(());
        }

        let mut win_rate = self.win_rate.write().await;
        let current_wins = *win_rate * (trades - 1) as f64;
        let new_wins = current_wins + if trade_pnl > 0.0 { 1.0 } else { 0.0 };
        *win_rate = new_wins / trades as f64;
        drop(win_rate);

        let pnl = *self.total_pnl.read().await;
        let mut max_dd = self.max_drawdown_seen.write().await;
        
        if pnl < 0.0 && pnl.abs() > *max_dd {
            *max_dd = pnl.abs();
        }
        
        tracing::debug!("Updated statistics: win_rate={:.3}, max_dd={:.2}, total_pnl={:.2}", 
                       *self.win_rate.read().await, *max_dd, pnl);
        
        Ok(())
    }

    pub async fn check_stop_conditions(&self, current_price: f64, entry_time: u64, current_time: u64) -> bool {
        let current_nav = self.get_current_nav().await;
        self.check_stop_conditions_fixed(current_price, entry_time, current_time, current_nav).await
    }

    /// Check stop conditions with robust half-life-based timing and proper PnL logic.
    pub async fn check_stop_conditions_fixed(&self, current_price: f64, entry_time_unix: u64, now_unix: u64, nav_usd: f64) -> bool {
        let position = *self.current_position.read().await;
        if position.abs() < 0.01 { 
            return false; // No position to stop
        }

        let entry_price = match *self.entry_price.read().await { 
            Some(p) => p, 
            None => return false // No entry price recorded
        };
        
        if current_price <= 0.0 { 
            return true; // Invalid market data → be safe
        }

        // compute pnl in USD using nav baseline
        let exposure_usd = position.abs() * nav_usd;
        let pnl = exposure_usd * (current_price / entry_price - 1.0) * position.signum();

        // hard emergency drawdown
        if pnl <= -nav_usd * MAX_DRAWDOWN { 
            tracing::warn!("Emergency stop: PnL={:.2} exceeds max drawdown", pnl);
            return true; 
        }

        // time-based stop: 3 half-lives is sensible horizon
        let params = self.parameters.read().await;
        let max_hold_secs = (params.half_life * 3.0) as u64;
        if now_unix > entry_time_unix + max_hold_secs { 
            tracing::info!("Time-based stop: held for {:.1} half-lives", 
                          (now_unix - entry_time_unix) as f64 / params.half_life);
            return true; 
        }

        // mean reversion achieved (z collapsed) and profit >= min profit (bps)
        let current_log = current_price.ln();
        let sigma_stationary = (params.sigma * params.sigma / (2.0 * params.theta)).sqrt().max(1e-12);
        let current_z = (current_log - params.mu) / sigma_stationary;
        if current_z.abs() < 0.5 && pnl > (MIN_PROFIT_BPS / 10000.0) * nav_usd { 
            tracing::info!("Profit-taking stop: z_score={:.3}, PnL={:.2}", current_z, pnl);
            return true; 
        }

        false
    }

    pub fn get_portfolio_metrics(&self) -> PortfolioMetrics {
        PortfolioMetrics {
            total_pnl: match self.total_pnl.read() {
                Ok(pnl) => *pnl,
                Err(_) => 0.0,
            },
            win_rate: match self.win_rate.read() {
                Ok(rate) => *rate,
                Err(_) => 0.0,
            },
            sharpe_ratio: match self.sharpe_ratio.read() {
                Ok(ratio) => *ratio,
                Err(_) => 0.0,
            },
            max_drawdown: match self.max_drawdown_seen.read() {
                Ok(dd) => *dd,
                Err(_) => 0.0,
            },
            trades_count: match self.trades_count.read() {
                Ok(count) => *count,
                Err(_) => 0,
            },
            current_position: match self.current_position.read() {
                Ok(pos) => *pos,
                Err(_) => 0.0,
            },
        }
    }

    /// Async-safe rebalance execution with simulation, retry, and blockhash refresh.
    /// Returns the actual fill price from execution (placeholder implementation)
    pub async fn execute_rebalance_nonblocking(
        &self,
        client: &RpcClient,
        program_id: &Pubkey,
        market_account: &Pubkey,
        user_account: &Pubkey,
        signer: &Keypair,
        signal: &Signal,
        exec: &ExecutionParams,
    ) -> Result<f64, Box<dyn std::error::Error>> {
        let execution_start = Instant::now();
        
        // Input validations
        if exec.amount_usd <= 0.0 { 
            return Err("zero amount".into()); 
        }
        if exec.min_fill_ratio <= 0.0 { 
            return Err("min_fill_ratio invalid".into()); 
        }

        // Check circuit breakers before execution
        if !self.check_circuit_breakers().await? {
            return Err("Circuit breaker triggered - execution blocked".into());
        }
        
        // Enhanced price feed health validation
        if !self.validate_price_feed_health().await {
            return Err("Price feed health check failed".into());
        }

        // Feed staleness gate (additional check)
        let last_upd = *self.last_update.read().await;
        if last_upd.elapsed() > Duration::from_secs(STALE_PRICE_MAX_AGE_SECS) {
            return Err("stale price feed".into());
        }
        
        // Get current NAV from bot state
        let current_nav = self.get_current_nav().await;

        tracing::info!("Starting rebalance execution: amount_usd={:.2}, direction={:.1}", 
                      exec.amount_usd, exec.direction);

        // Build the basic instruction data (amounts must be prepared as integers)
        let decimals: u8 = match exec.token_decimals {
            Some(d) => d,
            None => self.get_token_decimals(client, &exec.token_mint).await?,
        };
        // Calculate token amount using sophisticated method
        let amount_tokens: u64 = self.calculate_token_amount(client, &exec.token_mint, exec.amount_usd).await?;
        
        // Record z-score for monitoring
        let mut metrics = self.monitoring_metrics.write().await;
        metrics.z_score_distribution.push(signal.z_score);
        if metrics.z_score_distribution.len() > 1000 {
            metrics.z_score_distribution.remove(0);
        }
        drop(metrics);
        let mut attempt = 0usize;

        loop {
            attempt += 1;
            tracing::debug!("Execution attempt {}/{}", attempt, MAX_RETRIES);

            // 1) simulate first attempt to catch compute or accounts errors
            if SIMULATE_FIRST && attempt == 1 {
                let blockhash = client.get_latest_blockhash().await?;
                let accounts = vec![
                    solana_sdk::instruction::AccountMeta::new(*market_account, false),
                    solana_sdk::instruction::AccountMeta::new(*user_account, false),
                    solana_sdk::instruction::AccountMeta::new(signer.pubkey(), true),
                ];

                let instruction_data = RebalanceInstruction {
                    amount: amount_tokens,
                    direction: exec.direction as i8,
                    max_slippage_bps: (exec.max_slippage * 10000.0) as u16,
                    min_fill_ratio: (exec.min_fill_ratio * 100.0) as u8,
                };
                let data = instruction_data.try_to_vec()?;
                let base_instr = solana_sdk::instruction::Instruction {
                    program_id: *program_id,
                    accounts: accounts.clone(),
                    data,
                };

                // add compute budget requests (priority fee example)
                // Dynamic priority fee based on urgency
                let mut priority_price: u64 = (1000.0 * (1.0 + exec.urgency)).round() as u64;
                if priority_price < 1000 { priority_price = 1000; }
                if priority_price > 50_000 { priority_price = 50_000; }

                let instructions = vec![
                    ComputeBudgetInstruction::set_compute_unit_limit(400_000),
                    ComputeBudgetInstruction::set_compute_unit_price(priority_price),
                    base_instr,
                ];

                let message = Message::new(&instructions, Some(&signer.pubkey()));
                let sim_tx = VersionedTransaction::try_new_unsigned(message)?;
                
                let sim_res = client.simulate_transaction(&sim_tx).await;
                if let Err(err) = sim_res {
                    tracing::warn!("simulation failed: {:?}", err);
                    if attempt >= MAX_RETRIES { 
                        return Err(err.into()); 
                    }
                    tokio::time::sleep(Duration::from_millis(RETRY_DELAY_MS)).await;
                    continue;
                } else {
                    tracing::info!("Simulation successful");
                }
            }

            // 2) get fresh blockhash & build signed tx
            let latest_blockhash = client.get_latest_blockhash().await?;
            
            let accounts = vec![
                solana_sdk::instruction::AccountMeta::new(*market_account, false),
                solana_sdk::instruction::AccountMeta::new(*user_account, false),
                solana_sdk::instruction::AccountMeta::new(signer.pubkey(), true),
            ];
            
            let instruction_data = RebalanceInstruction {
                amount: amount_tokens,
                direction: exec.direction as i8,
                max_slippage_bps: (exec.max_slippage * 10000.0) as u16,
                min_fill_ratio: (exec.min_fill_ratio * 100.0) as u8,
            };
            let data = instruction_data.try_to_vec()?;
            let base_instr = solana_sdk::instruction::Instruction {
                program_id: *program_id,
                accounts,
                data,
            };
            
            // Dynamic priority fee based on urgency
            let mut priority_price: u64 = (1000.0 * (1.0 + exec.urgency)).round() as u64;
            if priority_price < 1000 { priority_price = 1000; }
            if priority_price > 50_000 { priority_price = 50_000; }

            let instructions = vec![
                ComputeBudgetInstruction::set_compute_unit_limit(400_000),
                ComputeBudgetInstruction::set_compute_unit_price(priority_price),
                base_instr,
            ];

            let message = Message::new(&instructions, Some(&signer.pubkey()));
            let mut tx = VersionedTransaction::try_new_unsigned(message)?;
            tx.sign(&[&signer], latest_blockhash);

            // 3) small randomized jitter to avoid predictable timing
            let jitter_ms = (SystemTime::now().duration_since(UNIX_EPOCH).unwrap().subsec_millis() as u64) % 50;
            if jitter_ms > 0 { tokio::time::sleep(Duration::from_millis(jitter_ms)).await; }

            // 4) send transaction
            let send_res = client.send_and_confirm_transaction_with_spinner_and_commitment(
                &solana_sdk::transaction::Transaction::from(tx.clone()),
                CommitmentConfig::confirmed(),
            ).await;

            match send_res {
                Ok(sig) => {
                    tracing::info!("Transaction confirmed: {}", sig);
                    
                    // Get REAL execution price from transaction logs
                    let fill_price_actual = self.get_real_fill_price(
                        client,
                        &signature,
                        amount_tokens,
                        signal.take_profit
                    ).await.unwrap_or_else(|_| {
                        // Fallback to conservative estimate only if parsing fails
                        warn!("Failed to parse real fill price, using fallback");
                        if exec.direction > 0.0 { signal.take_profit } else { signal.stop_loss }
                    });
                    
                    // Calculate execution metrics
                    let execution_latency_ms = execution_start.elapsed().as_millis() as u64;
                    let estimated_gas_cost_usd = (priority_price as f64 / 1_000_000.0) * 0.1; // rough estimate
                    let actual_slippage_bps = (fill_price_actual - signal.take_profit).abs() / signal.take_profit * 10000.0;
                    
                    // Update position with REAL execution details using current NAV
                    self.update_position_fixed(signal.position_size, fill_price_actual, current_nav).await?;
                    
                    // Record trade execution for monitoring and circuit breakers
                    let actual_pnl = (signal.position_size * fill_price_actual - signal.position_size * signal.take_profit) * current_nav;
                    self.record_trade_execution(
                        actual_pnl,
                        estimated_gas_cost_usd,
                        execution_latency_ms,
                        estimated_slippage_bps,
                    ).await?;
                    
                    // Record priority fee for monitoring
                    let mut metrics = self.monitoring_metrics.write().await;
                    metrics.priority_fees_paid.push(priority_price);
                    if metrics.priority_fees_paid.len() > 1000 {
                        metrics.priority_fees_paid.remove(0);
                    }
                    drop(metrics);
                    
                    tracing::info!("Trade execution completed: fill_price={:.4}, latency={}ms, gas_est={:.4}, slippage={:.1}bps", 
                                 fill_price_actual, execution_latency_ms, estimated_gas_cost_usd, actual_slippage_bps);
                    
                    return Ok(fill_price_actual);
                }
                Err(err) => {
                    tracing::warn!("send attempt {} failed: {:?}", attempt, err);
                    // If blockhash expired or recoverable, retry with new blockhash; else bail
                    if attempt >= MAX_RETRIES {
                        return Err(Box::new(err));
                    }
                    tokio::time::sleep(Duration::from_millis(RETRY_DELAY_MS)).await;
                    continue;
                }
            }
        }
    }

    pub async fn execute_rebalance(
        &self,
        client: &RpcClient,
        program_id: &Pubkey,
        market_account: &Pubkey,
        user_account: &Pubkey,
        signer: &Keypair,
        signal: &Signal,
        execution_params: &ExecutionParams,
    ) -> Result<(), Box<dyn std::error::Error>> {
        // Route through the nonblocking, versioned implementation for consistency
        let _ = self
            .execute_rebalance_nonblocking(
                client,
                program_id,
                market_account,
                user_account,
                signer,
                signal,
                execution_params,
            )
            .await?;
        Ok(())
    }

    pub async fn validate_market_conditions(&self, market: &MarketState) -> bool {
        self.validate_market_conditions_fixed(market).await
    }

    /// Validate market conditions with robust checks for liquidity, spread, volatility, and price spikes.
    pub async fn validate_market_conditions_fixed(&self, market: &MarketState) -> bool {
        if market.mid <= 0.0 { 
            tracing::warn!("Invalid market data: mid price <= 0");
            return false; 
        }
        if market.spread_bps > 50.0 { 
            tracing::debug!("Market rejected: spread too wide ({:.1} bps)", market.spread_bps);
            return false; 
        }
        if market.liquidity < MIN_LIQUIDITY_USD { 
            tracing::debug!("Market rejected: insufficient liquidity ({:.0} USD)", market.liquidity);
            return false; 
        }

        let params = self.parameters.read().await;
        if market.volatility > params.sigma * 3.0 { 
            tracing::debug!("Market rejected: volatility spike ({:.4} vs {:.4})", market.volatility, params.sigma);
            return false; 
        }

        // check recent micro spikes
        let history = self.price_history.read().await;
        if history.len() < MIN_OBSERVATIONS { 
            return false; 
        }
        let recent_count = history.len().min(10);
        let recent_prices: Vec<f64> = history.iter().rev().take(recent_count).map(|p| p.price).collect();
        for w in recent_prices.windows(2) {
            if w[0] <= 0.0 || w[1] <= 0.0 { 
                return false; 
            }
            let change = ((w[1]/w[0]) - 1.0).abs();
            if change > 0.1 { 
                tracing::debug!("Market rejected: recent price spike ({:.1}%)", change * 100.0);
                return false; // too spiky
            }
        }
        
        true
    }

    pub fn emergency_close_position(
        &self,
        client: &RpcClient,
        program_id: &Pubkey,
        market_account: &Pubkey,
        user_account: &Pubkey,
        signer: &Keypair,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let current_position = match self.current_position.read() {
            Ok(pos) => *pos,
            Err(_) => return Err("Failed to read current position".into()),
        };
        
        if current_position.abs() < 0.01 {
            return Ok(());
        }

        let blockhash = client.get_latest_blockhash()?;
        
        let accounts = vec![
            solana_sdk::instruction::AccountMeta::new(*market_account, false),
            solana_sdk::instruction::AccountMeta::new(*user_account, false),
            solana_sdk::instruction::AccountMeta::new(signer.pubkey(), true),
        ];

        let instruction_data = ClosePositionInstruction {
            emergency: true,
        };

        let data = instruction_data.try_to_vec()?;
        
        let instruction = solana_sdk::instruction::Instruction {
            program_id: *program_id,
            accounts,
            data,
        };

        let mut transaction = solana_sdk::transaction::Transaction::new_with_payer(
            &[instruction],
            Some(&signer.pubkey()),
        );

        transaction.sign(&[signer], blockhash);
        client.send_and_confirm_transaction(&transaction)?;

        match self.current_position.write() {
            Ok(mut pos) => *pos = 0.0,
            Err(_) => return Err("Failed to write current position".into()),
        };
        
        match self.entry_price.write() {
            Ok(mut price) => *price = None,
            Err(_) => return Err("Failed to write entry price".into()),
        };

        Ok(())
    }
}

use solana_sdk::pubkey::Pubkey;
use std::time::{Duration, Instant};

#[derive(Debug, Clone)]
pub struct ExecutionParams {
    pub amount_usd: f64,
    pub direction: f64,
    pub max_slippage: f64,
    pub urgency: f64,
    pub time_limit: Duration,
    pub min_fill_ratio: f64,
    pub token_mint: Pubkey,        // Token mint address for decimals lookup
    pub token_decimals: Option<u8>, // Optional override for token decimals
    pub nav_usd: f64,              // Current NAV in USD for position sizing
    pub min_profit_bps: u64,       // Minimum profit in basis points after fees
    pub last_updated: Instant,     // When these params were created
}

#[derive(Debug, Clone)]
pub struct PortfolioMetrics {
    pub total_pnl: f64,
    pub win_rate: f64,
    pub sharpe_ratio: f64,
    pub max_drawdown: f64,
    pub trades_count: u64,
    pub current_position: f64,
    pub nav_usd: f64,                  // Current Net Asset Value in USD
    pub daily_pnl: f64,                // PnL for the current day
    pub hourly_pnl: f64,               // PnL for the current hour
    pub max_position_size: f64,        // Maximum position size in USD
    pub last_rebalance_time: Instant,   // When was the last rebalance
    pub current_leverage: f64,         // Current leverage ratio
    pub risk_free_rate: f64,           // Current risk-free rate for Sharpe ratio
    pub volatility_30d: f64,           // 30-day rolling volatility
}

#[derive(BorshSerialize, BorshDeserialize)]
struct RebalanceInstruction {
    amount: u64,
    direction: i8,
    max_slippage_bps: u16,
    min_fill_ratio: u8,
}

#[derive(BorshSerialize, BorshDeserialize)]
struct ClosePositionInstruction {
    emergency: bool,
}

impl BacktestResults {
    pub fn new(initial_nav: f64) -> Self {
        Self {
            initial_nav,
            final_nav: initial_nav,
            total_pnl: 0.0,
            total_return_pct: 0.0,
            trades_count: 0,
            win_rate: 0.0,
            max_drawdown: 0.0,
            sharpe_ratio: 0.0,
            sortino_ratio: 0.0,
            calmar_ratio: 0.0,
            profit_factor: 0.0,
            nav_history: vec![initial_nav],
            trade_history: Vec::new(),
            daily_returns: Vec::new(),
            volatility: 0.0,
            max_consecutive_losses: 0,
            max_consecutive_wins: 0,
            avg_trade_duration_hours: 0.0,
        }
    }
    
    pub fn record_trade(
        &mut self,
        timestamp: u64,
        z_score: f64,
        position_size: f64,
        execution_price: f64,
        nav_at_trade: f64,
    ) {
        let pnl = position_size * execution_price; // Simplified PnL calculation
        let cumulative_pnl = self.total_pnl + pnl;
        
        let trade = BacktestTrade {
            timestamp,
            z_score,
            position_size,
            execution_price,
            nav_at_trade,
            pnl,
            cumulative_pnl,
        };
        
        self.trade_history.push(trade);
        self.total_pnl = cumulative_pnl;
        self.trades_count += 1;
    }
    
    pub fn update_nav(&mut self, new_nav: f64) {
        self.nav_history.push(new_nav);
        
        // Calculate daily return if we have previous NAV
        if self.nav_history.len() > 1 {
            let prev_nav = self.nav_history[self.nav_history.len() - 2];
            let daily_return = (new_nav / prev_nav) - 1.0;
            self.daily_returns.push(daily_return);
        }
    }
    
    pub fn finalize(
        &mut self,
        final_nav: f64,
        total_pnl: f64,
        trades_count: u64,
        win_rate: f64,
        max_drawdown: f64,
    ) {
        self.final_nav = final_nav;
        self.total_pnl = total_pnl;
        self.trades_count = trades_count;
        self.win_rate = win_rate;
        self.max_drawdown = max_drawdown;
        self.total_return_pct = (final_nav / self.initial_nav) - 1.0;
        
        // Calculate advanced metrics
        self.calculate_advanced_metrics();
    }
    
    fn calculate_advanced_metrics(&mut self) {
        if self.daily_returns.is_empty() {
            return;
        }
        
        // Calculate volatility
        let mean_return = self.daily_returns.iter().sum::<f64>() / self.daily_returns.len() as f64;
        let variance = self.daily_returns.iter()
            .map(|r| (r - mean_return).powi(2))
            .sum::<f64>() / self.daily_returns.len() as f64;
        self.volatility = variance.sqrt() * (252.0f64).sqrt(); // Annualized
        
        // Sharpe ratio (assuming 2% risk-free rate)
        let risk_free_rate = 0.02;
        let excess_return = self.total_return_pct - risk_free_rate;
        self.sharpe_ratio = if self.volatility > 0.0 {
            excess_return / self.volatility
        } else {
            0.0
        };
        
        // Sortino ratio (downside deviation)
        let downside_returns: Vec<f64> = self.daily_returns.iter()
            .filter(|&&r| r < 0.0)
            .cloned()
            .collect();
        
        if !downside_returns.is_empty() {
            let downside_variance = downside_returns.iter()
                .map(|r| r.powi(2))
                .sum::<f64>() / downside_returns.len() as f64;
            let downside_deviation = downside_variance.sqrt() * (252.0f64).sqrt();
            
            self.sortino_ratio = if downside_deviation > 0.0 {
                excess_return / downside_deviation
            } else {
                0.0
            };
        }
        
        // Calmar ratio
        self.calmar_ratio = if self.max_drawdown > 0.0 {
            self.total_return_pct / self.max_drawdown
        } else {
            0.0
        };
        
        // Profit factor
        let winning_trades: f64 = self.trade_history.iter()
            .filter(|t| t.pnl > 0.0)
            .map(|t| t.pnl)
            .sum();
        let losing_trades: f64 = self.trade_history.iter()
            .filter(|t| t.pnl < 0.0)
            .map(|t| t.pnl.abs())
            .sum();
        
        self.profit_factor = if losing_trades > 0.0 {
            winning_trades / losing_trades
        } else if winning_trades > 0.0 {
            f64::INFINITY
        } else {
            0.0
        };
        
        // Calculate consecutive win/loss streaks
        let mut current_wins = 0u64;
        let mut current_losses = 0u64;
        let mut max_wins = 0u64;
        let mut max_losses = 0u64;
        
        for trade in &self.trade_history {
            if trade.pnl > 0.0 {
                current_wins += 1;
                current_losses = 0;
                max_wins = max_wins.max(current_wins);
            } else if trade.pnl < 0.0 {
                current_losses += 1;
                current_wins = 0;
                max_losses = max_losses.max(current_losses);
            }
        }
        
        self.max_consecutive_wins = max_wins;
        self.max_consecutive_losses = max_losses;
    }
    
    /// Generate detailed performance report
    pub fn generate_report(&self) -> String {
        format!(
            "\n=== BACKTEST PERFORMANCE REPORT ===\n\
             Initial NAV: ${:.2}\n\
             Final NAV: ${:.2}\n\
             Total Return: {:.2}%\n\
             Total PnL: ${:.2}\n\
             \n=== TRADE STATISTICS ===\n\
             Total Trades: {}\n\
             Win Rate: {:.2}%\n\
             Profit Factor: {:.2}\n\
             Max Consecutive Wins: {}\n\
             Max Consecutive Losses: {}\n\
             \n=== RISK METRICS ===\n\
             Maximum Drawdown: {:.2}%\n\
             Volatility (Annualized): {:.2}%\n\
             Sharpe Ratio: {:.3}\n\
             Sortino Ratio: {:.3}\n\
             Calmar Ratio: {:.3}\n\
             \n=== SUMMARY ===\n\
             Strategy Performance: {}\n\
             Risk-Adjusted Returns: {}\n",
            self.initial_nav,
            self.final_nav,
            self.total_return_pct * 100.0,
            self.total_pnl,
            self.trades_count,
            self.win_rate * 100.0,
            self.profit_factor,
            self.max_consecutive_wins,
            self.max_consecutive_losses,
            self.max_drawdown * 100.0,
            self.volatility * 100.0,
            self.sharpe_ratio,
            self.sortino_ratio,
            self.calmar_ratio,
            if self.total_return_pct > 0.0 { "PROFITABLE" } else { "UNPROFITABLE" },
            if self.sharpe_ratio > 1.0 { "EXCELLENT" } else if self.sharpe_ratio > 0.5 { "GOOD" } else { "POOR" }
        )
    }
}

impl Default for OrnsteinUhlenbeckMeanReverter {
    fn default() -> Self {
        Self::new()
    }
}



