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
use std::time::{Duration, Instant};
use tokio::sync::RwLock;
use rust_decimal::Decimal;
use rust_decimal::prelude::ToPrimitive;
use tracing;

// Import the helper function
use crate::ornstein_uhlenbeck_mean_reverter::helpers::usd_to_token_amount;

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

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum SignalStrength {
    VeryStrong,
    Strong,
    Medium,
    Weak,
    None,
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

    async fn calibrate_parameters(&self) -> Result<(), Box<dyn std::error::Error>> {
        self.calibrate_parameters_ar1().await
    }

    /// Calibrate OU parameters by regressing log-prices as AR(1) with intercept.
    /// Uses simple OLS on x_t = c + φ x_{t-1} + ε and recovers θ, μ, σ, σ_stationary.
    /// If timestamps are reasonably regular, Δt uses median delta; otherwise
    /// consider upgrading to exact-discretization MLE.
    async fn calibrate_parameters_ar1(&self) -> Result<(), Box<dyn std::error::Error>> {
        let history = self.price_history.read().await;
        if history.len() < MIN_OBSERVATIONS { return Ok(()); }

        tracing::info!("Starting AR(1) calibration with {} observations", history.len());

        // extract log-prices and timestamps
        let xs: Vec<f64> = history.iter().map(|p| {
            if p.price <= 0.0 { 0.0 } else { p.price.ln() }
        }).collect();

        // build regression pairs (y = x_t, x = x_{t-1})
        let n = xs.len();
        if n < 3 { return Ok(()); }

        let mut x_prev = Vec::with_capacity(n - 1);
        let mut x_cur = Vec::with_capacity(n - 1);
        let mut dt_seconds = Vec::with_capacity(n - 1);
        
        // iterate history by index for dt
        for i in 1..history.len() {
            let cur = history[i].price;
            let prev = history[i - 1].price;
            if cur <= 0.0 || prev <= 0.0 { continue; }
            x_prev.push(prev.ln());
            x_cur.push(cur.ln());
            let dt = (history[i].timestamp as i64 - history[i - 1].timestamp as i64).max(1) as f64;
            dt_seconds.push(dt);
        }
        
        if x_prev.len() < MIN_OBSERVATIONS { return Ok(()); }

        // median Δt (seconds) as representative sampling interval
        dt_seconds.sort_by(|a,b| a.partial_cmp(b).unwrap());
        let median_dt = dt_seconds[dt_seconds.len()/2];
        tracing::debug!("Median sampling interval: {:.2} seconds", median_dt);

        // OLS solve for [c, phi] in x_t = c + phi * x_{t-1} + eps
        let n_eff = x_prev.len() as f64;
        let mean_x = x_prev.iter().sum::<f64>() / n_eff;
        let mean_y = x_cur.iter().sum::<f64>() / n_eff;

        let cov_xy = x_prev.iter().zip(x_cur.iter())
            .map(|(xp, yc)| (xp - mean_x) * (yc - mean_y)).sum::<f64>();
        let var_x = x_prev.iter().map(|xp| (xp - mean_x).powi(2)).sum::<f64>();

        let phi = if var_x.abs() < EPS { 0.0 } else { cov_xy / var_x };

        // clamp phi to (0,1) to avoid nonsense
        let phi_clamped = phi.max(PHI_MIN).min(PHI_MAX);
        let c = mean_y - phi_clamped * mean_x;

        // residual variance (σ_epsilon^2)
        let residuals_var = x_prev.iter().zip(x_cur.iter())
            .map(|(xp, yc)| {
                let pred = c + phi_clamped * xp;
                (yc - pred).powi(2)
            }).sum::<f64>() / (n_eff - 2.0).max(1.0);

        // map AR(1) -> OU parameters (Δt = median_dt seconds)
        let dt = median_dt; // seconds per sample
        // theta: continuous mean-reversion speed
        let theta = (-phi_clamped.ln() / dt).max(1e-9);
        // long-run mean mu = c / (1 - phi)
        let mu = c / (1.0 - phi_clamped);
        // diffusion σ: from discrete residual variance: Var(ε) = σ^2 * (1 - e^{-2θΔt})/(2θ)
        let denom = (1.0 - (-2.0 * theta * dt).exp()) / (2.0 * theta);
        let sigma = if denom.abs() < EPS {
            (residuals_var.max(VAR_FLOOR)).sqrt()
        } else {
            (residuals_var.max(VAR_FLOOR) * (2.0 * theta) / (1.0 - (-2.0 * theta * dt).exp())).sqrt()
        };
        let sigma_stationary = (sigma * sigma / (2.0 * theta)).sqrt().max(1e-12);

        // half-life in same time units as dt: t_half = ln(2)/theta
        let half_life = (2.0f64.ln() / theta).max(MIN_HALF_LIFE).min(MAX_HALF_LIFE);

        tracing::info!("AR(1) calibration results: phi={:.4}, theta={:.6}, mu={:.4}, sigma={:.4}, half_life={:.2}s", 
                      phi_clamped, theta, mu, sigma, half_life);

        // write params
        let mut params = self.parameters.write().await;
        params.theta = theta;
        params.mu = mu;
        params.sigma = sigma;
        params.half_life = half_life;
        params.mean_reversion_time = 1.0 / theta;
        params.confidence = 0.5; // conservative default — overruled by stationarity_gate
        drop(params);

        // stationarity gate: requires phi substantially < 1 and half-life bounded
        if !self.stationarity_gate(phi_clamped, half_life, residuals_var) {
            // keep confidence low to avoid trading when unstable
            let mut params = self.parameters.write().await;
            params.confidence = 0.0;
            tracing::warn!("Stationarity gate failed - setting confidence to 0");
        } else {
            let mut params = self.parameters.write().await;
            params.confidence = 0.95; // reasonable default; can be tuned
            tracing::info!("Stationarity gate passed - setting confidence to 0.95");
        }

        Ok(())
    }

    fn calculate_robust_mean(&self, data: &[f64]) -> f64 {
        let mut sorted = data.to_vec();
        sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        
        let trim_pct = 0.1;
        let trim_count = (data.len() as f64 * trim_pct) as usize;
        
        let trimmed = &sorted[trim_count..sorted.len() - trim_count];
        trimmed.iter().sum::<f64>() / trimmed.len() as f64
    }

    fn calculate_robust_std(&self, data: &[f64]) -> f64 {
        let mean = self.calculate_robust_mean(data);
        let variance = data.iter()
            .map(|x| (x - mean).powi(2))
            .sum::<f64>() / (data.len() - 1) as f64;
        variance.sqrt()
    }

    fn calculate_autocorrelation(&self, returns: &[f64], lag: usize) -> f64 {
        if returns.len() <= lag {
            return 0.0;
        }

        let mean = returns.iter().sum::<f64>() / returns.len() as f64;
        let variance = returns.iter()
            .map(|r| (r - mean).powi(2))
            .sum::<f64>() / returns.len() as f64;

        if variance < 1e-10 {
            return 0.0;
        }

        let covariance = returns[..returns.len() - lag].iter()
            .zip(returns[lag..].iter())
            .map(|(r1, r2)| (r1 - mean) * (r2 - mean))
            .sum::<f64>() / (returns.len() - lag) as f64;

        covariance / variance
    }

    fn augmented_dickey_fuller(&self, prices: &[f64]) -> f64 {
        let diffs: Vec<f64> = prices.windows(2)
            .map(|w| w[1] - w[0])
            .collect();
        
        let lagged: Vec<f64> = prices[..prices.len() - 1].to_vec();
        
        let mean_diff = diffs.iter().sum::<f64>() / diffs.len() as f64;
        let mean_lag = lagged.iter().sum::<f64>() / lagged.len() as f64;
        
        let numerator: f64 = diffs.iter().zip(lagged.iter())
            .map(|(d, l)| (d - mean_diff) * (l - mean_lag))
            .sum();
        
        let denominator: f64 = lagged.iter()
            .map(|l| (l - mean_lag).powi(2))
            .sum();
        
        if denominator < 1e-10 {
            return 0.0;
        }
        
        let beta = numerator / denominator;
        let residuals: Vec<f64> = diffs.iter().zip(lagged.iter())
            .map(|(d, l)| d - beta * l)
            .collect();
        
        let se_beta = (residuals.iter().map(|r| r.powi(2)).sum::<f64>() 
            / (residuals.len() as f64 - 1.0) / denominator).sqrt();
        
        if se_beta < 1e-10 {
            return 0.0;
        }
        
        beta / se_beta
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
        };

        tracing::info!("Execution params: amount_usd={:.2}, direction={:.1}, slippage={:.4}, urgency={:.2}", 
                      params.amount_usd, params.direction, params.max_slippage, params.urgency);

        params
    }

    pub async fn update_position(&self, new_position: f64, execution_price: f64) -> Result<(), Box<dyn std::error::Error>> {
        self.update_position_fixed(new_position, execution_price, 1_000_000.0).await
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
        self.check_stop_conditions_fixed(current_price, entry_time, current_time, 1_000_000.0).await
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
        // Input validations
        if exec.amount_usd <= 0.0 { 
            return Err("zero amount".into()); 
        }
        if exec.min_fill_ratio <= 0.0 { 
            return Err("min_fill_ratio invalid".into()); 
        }

        tracing::info!("Starting rebalance execution: amount_usd={:.2}, direction={:.1}", 
                      exec.amount_usd, exec.direction);

        // Build the basic instruction data (amounts must be prepared as integers)
        let amount_tokens: u64 = usd_to_token_amount(exec.amount_usd, 6); // 6 decimals for typical SPL token
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
                let instructions = vec![
                    ComputeBudgetInstruction::set_compute_unit_limit(400_000),
                    ComputeBudgetInstruction::set_compute_unit_price(1_000), // lamports per CU
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
            
            let instructions = vec![
                ComputeBudgetInstruction::set_compute_unit_limit(400_000),
                ComputeBudgetInstruction::set_compute_unit_price(1_000),
                base_instr,
            ];

            let message = Message::new(&instructions, Some(&signer.pubkey()));
            let mut tx = VersionedTransaction::try_new_unsigned(message)?;
            tx.sign(&[&signer], latest_blockhash);

            // 3) send transaction
            let send_res = client.send_and_confirm_transaction_with_spinner_and_commitment(
                &solana_sdk::transaction::Transaction::from(tx.clone()),
                CommitmentConfig::confirmed(),
            ).await;

            match send_res {
                Ok(sig) => {
                    tracing::info!("Transaction confirmed: {}", sig);
                    
                    // NOTE: In a real implementation, you must obtain real fill price from 
                    // on-chain event or oracle feed. For now, use a conservative proxy.
                    let fill_price_proxy = if exec.direction > 0.0 { 
                        // buying: use ask as conservative fill estimate
                        signal.take_profit // or get from market data
                    } else { 
                        // selling: use bid as conservative fill estimate  
                        signal.stop_loss // or get from market data
                    };
                    
                    // Update position with actual execution details
                    self.update_position_fixed(signal.position_size, fill_price_proxy, exec.amount_usd * 10.0).await?;
                    
                    return Ok(fill_price_proxy);
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
        let current_slot = client.get_slot()?;
        let blockhash = client.get_latest_blockhash()?;
        
        let accounts = vec![
            solana_sdk::instruction::AccountMeta::new(*market_account, false),
            solana_sdk::instruction::AccountMeta::new(*user_account, false),
            solana_sdk::instruction::AccountMeta::new(signer.pubkey(), true),
            solana_sdk::instruction::AccountMeta::new_readonly(solana_sdk::sysvar::clock::id(), false),
        ];

        let instruction_data = RebalanceInstruction {
            amount: (execution_params.amount_usd * 1e9) as u64,
            direction: execution_params.direction as i8,
            max_slippage_bps: (execution_params.max_slippage * 10000.0) as u16,
            min_fill_ratio: (execution_params.min_fill_ratio * 100.0) as u8,
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

        for attempt in 0..MAX_RETRY_ATTEMPTS {
            match client.send_and_confirm_transaction_with_spinner_and_commitment(
                &transaction,
                CommitmentConfig::processed(),
            ) {
                Ok(signature) => {
                    self.update_position(signal.position_size, signal.expected_return);
                    return Ok(());
                }
                Err(e) => {
                    if attempt < MAX_RETRY_ATTEMPTS - 1 {
                        tokio::time::sleep(Duration::from_millis(RETRY_DELAY_MS)).await;
                        continue;
                    }
                    return Err(e.into());
                }
            }
        }

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

#[derive(Debug, Clone)]
pub struct ExecutionParams {
    pub amount_usd: f64,
    pub direction: f64,
    pub max_slippage: f64,
    pub urgency: f64,
    pub time_limit: Duration,
    pub min_fill_ratio: f64,
}

#[derive(Debug, Clone)]
pub struct PortfolioMetrics {
    pub total_pnl: f64,
    pub win_rate: f64,
    pub sharpe_ratio: f64,
    pub max_drawdown: f64,
    pub trades_count: u64,
    pub current_position: f64,
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

impl Default for OrnsteinUhlenbeckMeanReverter {
    fn default() -> Self {
        Self::new()
    }
}

