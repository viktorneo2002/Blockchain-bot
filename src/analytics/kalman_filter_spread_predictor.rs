use std::collections::HashMap;
use std::sync::Arc;
use parking_lot::RwLock;
use nalgebra::{DMatrix, DVector};
use solana_sdk::pubkey::Pubkey;
use anyhow::{Result, anyhow};
use serde::{Deserialize, Serialize};

const MAX_SPREAD_HISTORY: usize = 100;
const KALMAN_DT: f64 = 0.001; // 1ms update interval
const MIN_OBSERVATIONS: usize = 10;
const OUTLIER_THRESHOLD: f64 = 4.0;
const SPREAD_PREDICTION_HORIZON: usize = 5;
const CONFIDENCE_DECAY_RATE: f64 = 0.98;
const MAX_COVARIANCE: f64 = 1000.0;
const MIN_COVARIANCE: f64 = 1e-6;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SpreadObservation {
    pub timestamp_ms: u64,
    pub bid_price: f64,
    pub ask_price: f64,
    pub mid_price: f64,
    pub spread: f64,
    pub volume_imbalance: f64,
    pub liquidity_depth: f64,
}

#[derive(Debug, Clone)]
pub struct KalmanState {
    pub state: DVector<f64>,
    pub covariance: DMatrix<f64>,
    pub last_update: u64,
    pub innovation: f64,
    pub innovation_covariance: f64,
    pub mahalanobis_distance: f64,
}

#[derive(Debug, Clone)]
pub struct SpreadPrediction {
    pub predicted_spread: f64,
    pub confidence_interval: (f64, f64),
    pub prediction_horizon_ms: u64,
    pub model_confidence: f64,
    pub trend_strength: f64,
    pub volatility_estimate: f64,
}

pub struct KalmanFilterSpreadPredictor {
    states: Arc<RwLock<HashMap<(Pubkey, Pubkey), KalmanState>>>,
    observations: Arc<RwLock<HashMap<(Pubkey, Pubkey), Vec<SpreadObservation>>>>,
    process_noise: DMatrix<f64>,
    measurement_noise: DMatrix<f64>,
    state_transition: DMatrix<f64>,
    observation_matrix: DMatrix<f64>,
    adaptive_factor: f64,
    outlier_count: Arc<RwLock<HashMap<(Pubkey, Pubkey), u32>>>,
}

impl KalmanFilterSpreadPredictor {
    pub fn new() -> Self {
        let state_dim = 6; // [spread, spread_velocity, spread_acceleration, volume_imbalance, liquidity, trend]
        let obs_dim = 4; // [spread, volume_imbalance, liquidity_depth, mid_price_change]
        
        let mut process_noise = DMatrix::zeros(state_dim, state_dim);
        process_noise[(0, 0)] = 0.001; // spread noise
        process_noise[(1, 1)] = 0.01;  // velocity noise
        process_noise[(2, 2)] = 0.1;   // acceleration noise
        process_noise[(3, 3)] = 0.05;  // volume imbalance noise
        process_noise[(4, 4)] = 0.02;  // liquidity noise
        process_noise[(5, 5)] = 0.005; // trend noise
        
        let mut measurement_noise = DMatrix::zeros(obs_dim, obs_dim);
        measurement_noise[(0, 0)] = 0.01;  // spread measurement noise
        measurement_noise[(1, 1)] = 0.05;  // volume imbalance measurement noise
        measurement_noise[(2, 2)] = 0.03;  // liquidity measurement noise
        measurement_noise[(3, 3)] = 0.02;  // mid price change noise
        
        let mut state_transition = DMatrix::zeros(state_dim, state_dim);
        // Position update
        state_transition[(0, 0)] = 1.0;
        state_transition[(0, 1)] = KALMAN_DT;
        state_transition[(0, 2)] = 0.5 * KALMAN_DT * KALMAN_DT;
        // Velocity update
        state_transition[(1, 1)] = 1.0;
        state_transition[(1, 2)] = KALMAN_DT;
        // Acceleration update
        state_transition[(2, 2)] = 0.99; // Slight decay
        // Volume imbalance
        state_transition[(3, 3)] = 0.95;
        // Liquidity
        state_transition[(4, 4)] = 0.98;
        // Trend
        state_transition[(5, 5)] = 0.999;
        state_transition[(5, 1)] = 0.01; // Trend influenced by velocity
        
        let mut observation_matrix = DMatrix::zeros(obs_dim, state_dim);
        observation_matrix[(0, 0)] = 1.0; // Observe spread directly
        observation_matrix[(1, 3)] = 1.0; // Observe volume imbalance
        observation_matrix[(2, 4)] = 1.0; // Observe liquidity
        observation_matrix[(3, 1)] = 0.1; // Mid price change relates to spread velocity
        observation_matrix[(3, 5)] = 0.5; // Mid price change relates to trend
        
        Self {
            states: Arc::new(RwLock::new(HashMap::new())),
            observations: Arc::new(RwLock::new(HashMap::new())),
            process_noise,
            measurement_noise,
            state_transition,
            observation_matrix,
            adaptive_factor: 1.0,
            outlier_count: Arc::new(RwLock::new(HashMap::new())),
        }
    }
    
    pub fn update(&self, pair: (Pubkey, Pubkey), observation: SpreadObservation) -> Result<()> {
        let mut observations = self.observations.write();
        let obs_vec = observations.entry(pair).or_insert_with(Vec::new);
        
        if obs_vec.len() >= MAX_SPREAD_HISTORY {
            obs_vec.remove(0);
        }
        
        let mid_price_change = if obs_vec.is_empty() {
            0.0
        } else {
            let last_mid = obs_vec.last().unwrap().mid_price;
            (observation.mid_price - last_mid) / last_mid
        };
        
        obs_vec.push(observation.clone());
        drop(observations);
        
        let measurement = DVector::from_vec(vec![
            observation.spread,
            observation.volume_imbalance,
            observation.liquidity_depth,
            mid_price_change,
        ]);
        
        let mut states = self.states.write();
        let state = states.entry(pair).or_insert_with(|| {
            self.initialize_state(&observation)
        });
        
        self.kalman_update(state, measurement, observation.timestamp_ms)?;
        
        Ok(())
    }
    
    fn initialize_state(&self, observation: &SpreadObservation) -> KalmanState {
        let initial_state = DVector::from_vec(vec![
            observation.spread,
            0.0, // Initial velocity
            0.0, // Initial acceleration
            observation.volume_imbalance,
            observation.liquidity_depth,
            0.0, // Initial trend
        ]);
        
        let mut initial_covariance = DMatrix::identity(6, 6);
        initial_covariance *= 0.1;
        
        KalmanState {
            state: initial_state,
            covariance: initial_covariance,
            last_update: observation.timestamp_ms,
            innovation: 0.0,
            innovation_covariance: 1.0,
            mahalanobis_distance: 0.0,
        }
    }
    
    fn kalman_update(&self, state: &mut KalmanState, measurement: DVector<f64>, timestamp_ms: u64) -> Result<()> {
        let dt = (timestamp_ms.saturating_sub(state.last_update)) as f64 / 1000.0;
        if dt <= 0.0 {
            return Ok(());
        }
        
        // Prediction step
        let predicted_state = &self.state_transition * &state.state;
        let predicted_covariance = &self.state_transition * &state.covariance * self.state_transition.transpose() 
            + &self.process_noise * dt;
        
        // Innovation calculation
        let predicted_measurement = &self.observation_matrix * &predicted_state;
        let innovation = measurement - predicted_measurement;
        
        // Innovation covariance
        let innovation_covariance = &self.observation_matrix * &predicted_covariance * self.observation_matrix.transpose() 
            + &self.measurement_noise;
        
        // Check for numerical stability
        let inv_innovation_cov = innovation_covariance.clone().try_inverse()
            .ok_or_else(|| anyhow!("Innovation covariance matrix is singular"))?;
        
        // Kalman gain
        let kalman_gain = &predicted_covariance * self.observation_matrix.transpose() * inv_innovation_cov;
        
        // State update
        state.state = predicted_state + &kalman_gain * &innovation;
        
        // Covariance update (Joseph form for numerical stability)
        let identity = DMatrix::identity(6, 6);
        let update_matrix = &identity - &kalman_gain * &self.observation_matrix;
        state.covariance = update_matrix * &predicted_covariance * update_matrix.transpose() 
            + &kalman_gain * &self.measurement_noise * kalman_gain.transpose();
        
        // Bound covariance values
        for i in 0..state.covariance.nrows() {
            for j in 0..state.covariance.ncols() {
                state.covariance[(i, j)] = state.covariance[(i, j)].clamp(MIN_COVARIANCE, MAX_COVARIANCE);
            }
        }
        
        // Calculate Mahalanobis distance for outlier detection
        let mahalanobis = (innovation.transpose() * &inv_innovation_cov * &innovation)[(0, 0)].sqrt();
        
        if mahalanobis > OUTLIER_THRESHOLD {
            let mut outlier_count = self.outlier_count.write();
            *outlier_count.entry((state.last_update, timestamp_ms)).or_insert(0) += 1;
            
            // Adapt process noise if too many outliers
            if *outlier_count.get(&(state.last_update, timestamp_ms)).unwrap_or(&0) > 5 {
                state.covariance *= 1.5;
            }
        }
        
        state.innovation = innovation[0];
        state.innovation_covariance = innovation_covariance[(0, 0)];
        state.mahalanobis_distance = mahalanobis;
        state.last_update = timestamp_ms;
        
        Ok(())
    }
    
    pub fn predict_spread(&self, pair: (Pubkey, Pubkey), horizon_ms: u64) -> Result<SpreadPrediction> {
        let states = self.states.read();
        let state = states.get(&pair)
            .ok_or_else(|| anyhow!("No state found for pair"))?;
        
        let observations = self.observations.read();
        let obs_history = observations.get(&pair)
            .ok_or_else(|| anyhow!("No observations found for pair"))?;
        
        if obs_history.len() < MIN_OBSERVATIONS {
            return Err(anyhow!("Insufficient observations: {} < {}", obs_history.len(), MIN_OBSERVATIONS));
        }
        
        let horizon_steps = (horizon_ms as f64 / (KALMAN_DT * 1000.0)).ceil() as usize;
        let mut predicted_state = state.state.clone();
        let mut predicted_covariance = state.covariance.clone();
        
        // Multi-step prediction
        for step in 0..horizon_steps {
            predicted_state = &self.state_transition * &predicted_state;
            predicted_covariance = &self.state_transition * &predicted_covariance * self.state_transition.transpose() 
                + &self.process_noise * (KALMAN_DT * (step + 1) as f64);
        }
        
        let predicted_spread = predicted_state[0];
        let spread_variance = predicted_covariance[(0, 0)];
        let spread_std = spread_variance.sqrt();
        
        // Calculate confidence based on innovation and covariance
        let model_confidence = self.calculate_model_confidence(state, obs_history);
        
        // Trend strength from velocity and acceleration
        let trend_strength = (state.state[1].abs() + 0.1 * state.state[2].abs()) / (spread_std + 1e-6);
        
        // Volatility estimate from recent observations
        let volatility_estimate = self.estimate_volatility(obs_history);
        
        // Confidence interval (95%)
        let confidence_interval = (
            predicted_spread - 1.96 * spread_std,
            predicted_spread + 1.96 * spread_std
        );
        
        Ok(SpreadPrediction {
            predicted_spread,
            confidence_interval,
            prediction_horizon_ms: horizon_ms,
            model_confidence,
            trend_strength,
            volatility_estimate,
        })
    }
    
    fn calculate_model_confidence(&self, state: &KalmanState, observations: &[SpreadObservation]) -> f64 {
        let innovation_factor = (-state.innovation.abs() / (state.innovation_covariance.sqrt() + 1e-6)).exp();
        let mahalanobis_factor = (-state.mahalanobis_distance / OUTLIER_THRESHOLD).exp();
        
        let recent_accuracy = if observations.len() >= 10 {
            let recent_obs = &observations[observations.len() - 10..];
            let mut accuracy = 0.0;
            for i in 1..recent_obs.len() {
                use solana_sdk::{
    account::Account,
    clock::Clock,
    pubkey::Pubkey,
    sysvar::Sysvar,
};
use std::{
    collections::VecDeque,
    sync::{Arc, RwLock},
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use borsh::{BorshDeserialize, BorshSerialize};
use fixed::types::I80F48;
use raydium_amm::state::{AmmInfo, Loadable};
use serum_dex::state::Market;

const MAX_PRICE_HISTORY: usize = 256;
const MIN_SAMPLES_FOR_CALCULATION: usize = 32;
const RISK_THRESHOLD: f64 = 0.025;
const MAX_POSITION_SIZE_BPS: u64 = 500; // 5% max position
const MIN_PROFIT_THRESHOLD_BPS: u64 = 15; // 0.15% min profit
const CONFIDENCE_THRESHOLD: f64 = 0.85;
const MAX_LEVERAGE: f64 = 3.0;
const COOLDOWN_DURATION_MS: u64 = 500;

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Signal {
    Buy { confidence: f64, size_ratio: f64 },
    Sell { confidence: f64, size_ratio: f64 },
    Hold,
}

#[derive(Debug, Clone, Copy, BorshSerialize, BorshDeserialize)]
pub struct PricePoint {
    pub price: f64,
    pub volume: f64,
    pub timestamp: u64,
    pub spread_bps: u16,
}

#[derive(Debug, Clone)]
pub struct OUParameters {
    pub theta: f64,      // mean reversion speed
    pub mu: f64,         // long-term mean
    pub sigma: f64,      // volatility
    pub dt: f64,         // time increment
    pub last_update: u64,
}

#[derive(Debug)]
pub struct OrnsteinUhlenbeckStrategy {
    price_history: Arc<RwLock<VecDeque<PricePoint>>>,
    parameters: Arc<RwLock<OUParameters>>,
    last_signal_time: Arc<RwLock<u64>>,
    cumulative_pnl: Arc<RwLock<f64>>,
    win_rate: Arc<RwLock<f64>>,
    total_trades: Arc<RwLock<u64>>,
    winning_trades: Arc<RwLock<u64>>,
}

impl OrnsteinUhlenbeckStrategy {
    pub fn new() -> Self {
        Self {
            price_history: Arc::new(RwLock::new(VecDeque::with_capacity(MAX_PRICE_HISTORY))),
            parameters: Arc::new(RwLock::new(OUParameters {
                theta: 0.15,
                mu: 0.0,
                sigma: 0.02,
                dt: 1.0 / 3600.0,
                last_update: 0,
            })),
            last_signal_time: Arc::new(RwLock::new(0)),
            cumulative_pnl: Arc::new(RwLock::new(0.0)),
            win_rate: Arc::new(RwLock::new(0.0)),
            total_trades: Arc::new(RwLock::new(0)),
            winning_trades: Arc::new(RwLock::new(0)),
        }
    }

    pub fn update_price(&self, price: f64, volume: f64, spread_bps: u16) -> Result<(), &'static str> {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| "Time error")?
            .as_millis() as u64;

        let mut history = self.price_history.write().map_err(|_| "Lock error")?;
        
        if history.len() >= MAX_PRICE_HISTORY {
            history.pop_front();
        }

        history.push_back(PricePoint {
            price,
            volume,
            timestamp,
            spread_bps,
        });

        drop(history);

        if self.should_recalibrate(timestamp) {
            self.recalibrate_parameters()?;
        }

        Ok(())
    }

    pub fn get_signal(&self) -> Result<Signal, &'static str> {
        let history = self.price_history.read().map_err(|_| "Lock error")?;
        
        if history.len() < MIN_SAMPLES_FOR_CALCULATION {
            return Ok(Signal::Hold);
        }

        let current_time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| "Time error")?
            .as_millis() as u64;

        let last_signal = *self.last_signal_time.read().map_err(|_| "Lock error")?;
        
        if current_time - last_signal < COOLDOWN_DURATION_MS {
            return Ok(Signal::Hold);
        }

        let latest = history.back().ok_or("No price data")?;
        let params = self.parameters.read().map_err(|_| "Lock error")?;

        let log_returns = self.calculate_log_returns(&history)?;
        let current_deviation = (latest.price.ln() - params.mu) / params.sigma;
        
        let mean_reversion_probability = self.calculate_mean_reversion_probability(
            current_deviation,
            params.theta,
            params.dt
        );

        let volume_signal = self.analyze_volume_profile(&history)?;
        let spread_signal = self.analyze_spread_dynamics(&history)?;
        
        let composite_signal = mean_reversion_probability * 0.6 
            + volume_signal * 0.25 
            + spread_signal * 0.15;

        let risk_adjusted_size = self.calculate_position_size(
            current_deviation.abs(),
            latest.spread_bps,
            composite_signal.abs()
        );

        if composite_signal > CONFIDENCE_THRESHOLD && current_deviation < -1.5 {
            *self.last_signal_time.write().map_err(|_| "Lock error")? = current_time;
            Ok(Signal::Buy { 
                confidence: composite_signal,
                size_ratio: risk_adjusted_size
            })
        } else if composite_signal < -CONFIDENCE_THRESHOLD && current_deviation > 1.5 {
            *self.last_signal_time.write().map_err(|_| "Lock error")? = current_time;
            Ok(Signal::Sell { 
                confidence: composite_signal.abs(),
                size_ratio: risk_adjusted_size
            })
        } else {
            Ok(Signal::Hold)
        }
    }

    fn calculate_log_returns(&self, history: &VecDeque<PricePoint>) -> Result<Vec<f64>, &'static str> {
        if history.len() < 2 {
            return Err("Insufficient data");
        }

        let mut returns = Vec::with_capacity(history.len() - 1);
        let prices: Vec<f64> = history.iter().map(|p| p.price).collect();

        for i in 1..prices.len() {
            if prices[i - 1] > 0.0 && prices[i] > 0.0 {
                returns.push((prices[i] / prices[i - 1]).ln());
            }
        }

        Ok(returns)
    }

    fn calculate_mean_reversion_probability(&self, deviation: f64, theta: f64, dt: f64) -> f64 {
        let mean_reversion_strength = 1.0 - (-theta * dt).exp();
        let normalized_deviation = deviation.tanh();
        
        let base_probability = mean_reversion_strength * normalized_deviation.abs();
        let direction_factor = if deviation > 0.0 { -1.0 } else { 1.0 };
        
        direction_factor * base_probability * (1.0 + 0.2 * deviation.abs().min(3.0))
    }

    fn analyze_volume_profile(&self, history: &VecDeque<PricePoint>) -> Result<f64, &'static str> {
        let recent_window = 20.min(history.len());
        let mut volume_weighted_price = 0.0;
        let mut total_volume = 0.0;

        for i in history.len().saturating_sub(recent_window)..history.len() {
            let point = &history[i];
            volume_weighted_price += point.price * point.volume;
            total_volume += point.volume;
        }

        if total_volume == 0.0 {
            return Ok(0.0);
        }

        let vwap = volume_weighted_price / total_volume;
        let current_price = history.back().ok_or("No price data")?.price;
        
        let volume_ma = total_volume / recent_window as f64;
        let current_volume = history.back().ok_or("No volume data")?.volume;
        let volume_ratio = (current_volume / volume_ma).min(3.0);
        
        let price_deviation = (current_price - vwap) / vwap;
        Ok(-price_deviation * volume_ratio.sqrt())
    }

    fn analyze_spread_dynamics(&self, history: &VecDeque<PricePoint>) -> Result<f64, &'static str> {
        let recent_window = 10.min(history.len());
        let mut spread_sum = 0u64;
        
        for i in history.len().saturating_sub(recent_window)..history.len() {
            spread_sum += history[i].spread_bps as u64;
        }

        let avg_spread = spread_sum as f64 / recent_window as f64;
        let current_spread = history.back().ok_or("No spread data")?.spread_bps as f64;
        
        let spread_ratio = current_spread / avg_spread.max(1.0);
        
        if spread_ratio > 1.5 {
            Ok(-0.5 * spread_ratio.min(2.0))
        } else if spread_ratio < 0.7 {
            Ok(0.3 * (1.0 / spread_ratio).min(1.5))
        } else {
            Ok(0.0)
        }
    }

    fn calculate_position_size(&self, deviation: f64, spread_bps: u16, confidence: f64) -> f64 {
        let base_size = (MAX_POSITION_SIZE_BPS as f64 / 10000.0) * confidence;
        
        let kelly_fraction = {
            let win_rate = *self.win_rate.read().unwrap_or(&RwLock::new(0.5)).get_mut();
            let avg_win_loss_ratio = 1.2;
            
            if win_rate > 0.0 && win_rate < 1.0 {
                let p = win_rate;
                let b = avg_win_loss_ratio;
                ((p * (b + 1.0) - 1.0) / b).max(0.0).min(0.25)
            } else {
                0.1
            }
        };
        
        let volatility_adjustment = (-deviation.abs() / 3.0).exp();
        let spread_adjustment = 1.0 / (1.0 + (spread_bps as f64 / 100.0));
        
        (base_size * kelly_fraction * volatility_adjustment * spread_adjustment)
            .min(MAX_POSITION_SIZE_BPS as f64 / 10000.0)
            .max(0.0)
    }

    fn should_recalibrate(&self, current_time: u64) -> bool {
        let params = self.parameters.read().unwrap();
        current_time - params.last_update > 300_000 // 5 minutes
    }

    fn recalibrate_parameters(&self) -> Result<(), &'static str> {
        let history = self.price_history.read().map_err(|_| "Lock error")?;
        
        if history.len() < MIN_SAMPLES_FOR_CALCULATION {
            return Ok(());
        }

        let log_returns = self.calculate_log_returns(&history)?;
        
        if log_returns.is_empty() {
            return Ok(());
        }

        let mean = log_returns.iter().sum::<f64>() / log_returns.len() as f64;
        let variance = log_returns.iter()
            .map(|r| (r - mean).powi(2))
            .sum::<f64>() / log_returns.len() as f64;
        
        let autocorrelation = self.calculate_autocorrelation(&log_returns, 1)?;
        
        let new_theta = (-autocorrelation.ln()).max(0.01).min(1.0);
        let new_sigma = variance.sqrt() * (2.0 * new_theta).sqrt();
        let new_mu = mean / new_theta;

        let mut params = self.parameters.write().map_err(|_| "Lock error")?;
        params.theta = 0.7 * params.theta + 0.3 * new_theta;
        params.sigma = 0.7 * params.sigma + 0.3 * new_sigma;
        params.mu = 0.7 * params.mu + 0.3 * new_mu;
        params.last_update = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| "Time error")?
            .as_millis() as u64;

        Ok(())
    }

    fn calculate_autocorrelation(&self, returns: &[f64], lag: usize) -> Result<f64, &'static str> {
        if returns.len() <= lag {
            return Err("Insufficient data for autocorrelation");
        }

        let mean = returns.iter().sum::<f64>() / returns.len() as f64;
        let variance = returns.iter()
            .map(|r| (r - mean).powi(2))
            .sum::<f64>() / returns.len() as f64;

                if variance == 0.0 {
            return Ok(0.0);
        }

        let mut covariance = 0.0;
        for i in lag..returns.len() {
            covariance += (returns[i] - mean) * (returns[i - lag] - mean);
        }
        covariance /= (returns.len() - lag) as f64;

        Ok(covariance / variance)
    }

    pub fn update_trade_outcome(&self, pnl: f64) -> Result<(), &'static str> {
        let mut cumulative = self.cumulative_pnl.write().map_err(|_| "Lock error")?;
        let mut total = self.total_trades.write().map_err(|_| "Lock error")?;
        let mut winning = self.winning_trades.write().map_err(|_| "Lock error")?;
        let mut win_rate = self.win_rate.write().map_err(|_| "Lock error")?;

        *cumulative += pnl;
        *total += 1;
        
        if pnl > 0.0 {
            *winning += 1;
        }

        if *total > 0 {
            *win_rate = *winning as f64 / *total as f64;
        }

        Ok(())
    }

    pub fn get_risk_metrics(&self) -> Result<RiskMetrics, &'static str> {
        let history = self.price_history.read().map_err(|_| "Lock error")?;
        let params = self.parameters.read().map_err(|_| "Lock error")?;
        
        let sharpe_ratio = self.calculate_sharpe_ratio(&history)?;
        let max_drawdown = self.calculate_max_drawdown(&history)?;
        let current_volatility = params.sigma;
        
        Ok(RiskMetrics {
            sharpe_ratio,
            max_drawdown,
            current_volatility,
            win_rate: *self.win_rate.read().map_err(|_| "Lock error")?,
            total_trades: *self.total_trades.read().map_err(|_| "Lock error")?,
        })
    }

    fn calculate_sharpe_ratio(&self, history: &VecDeque<PricePoint>) -> Result<f64, &'static str> {
        let returns = self.calculate_log_returns(history)?;
        
        if returns.is_empty() {
            return Ok(0.0);
        }

        let mean_return = returns.iter().sum::<f64>() / returns.len() as f64;
        let std_dev = (returns.iter()
            .map(|r| (r - mean_return).powi(2))
            .sum::<f64>() / returns.len() as f64)
            .sqrt();

        if std_dev == 0.0 {
            return Ok(0.0);
        }

        let annualized_return = mean_return * 252.0 * 24.0;
        let annualized_std = std_dev * (252.0 * 24.0).sqrt();
        
        Ok(annualized_return / annualized_std)
    }

    fn calculate_max_drawdown(&self, history: &VecDeque<PricePoint>) -> Result<f64, &'static str> {
        if history.len() < 2 {
            return Ok(0.0);
        }

        let prices: Vec<f64> = history.iter().map(|p| p.price).collect();
        let mut max_drawdown = 0.0;
        let mut peak = prices[0];

        for &price in &prices[1..] {
            if price > peak {
                peak = price;
            }
            let drawdown = (peak - price) / peak;
            if drawdown > max_drawdown {
                max_drawdown = drawdown;
            }
        }

        Ok(max_drawdown)
    }

    pub fn validate_market_conditions(&self, market_data: &MarketData) -> Result<bool, &'static str> {
        if market_data.bid_liquidity < 1000.0 || market_data.ask_liquidity < 1000.0 {
            return Ok(false);
        }

        let spread_bps = ((market_data.ask - market_data.bid) / market_data.mid) * 10000.0;
        if spread_bps > 50.0 {
            return Ok(false);
        }

        let history = self.price_history.read().map_err(|_| "Lock error")?;
        if history.len() < MIN_SAMPLES_FOR_CALCULATION {
            return Ok(false);
        }

        let recent_volatility = self.calculate_recent_volatility(&history)?;
        if recent_volatility > 0.1 {
            return Ok(false);
        }

        Ok(true)
    }

    fn calculate_recent_volatility(&self, history: &VecDeque<PricePoint>) -> Result<f64, &'static str> {
        let window = 20.min(history.len());
        let recent_prices: Vec<f64> = history.iter()
            .rev()
            .take(window)
            .map(|p| p.price)
            .collect();

        if recent_prices.len() < 2 {
            return Ok(0.0);
        }

        let mut returns = Vec::new();
        for i in 1..recent_prices.len() {
            returns.push((recent_prices[i] / recent_prices[i-1]).ln());
        }

        let mean = returns.iter().sum::<f64>() / returns.len() as f64;
        let variance = returns.iter()
            .map(|r| (r - mean).powi(2))
            .sum::<f64>() / returns.len() as f64;

        Ok(variance.sqrt())
    }

    pub fn calculate_optimal_entry(&self, signal: &Signal, market_data: &MarketData) -> Result<OrderParams, &'static str> {
        match signal {
            Signal::Buy { confidence, size_ratio } => {
                let base_size = market_data.available_balance * size_ratio;
                let adjusted_size = self.apply_risk_limits(base_size, market_data)?;
                
                let limit_price = market_data.bid * (1.0 + 0.0001);
                let stop_loss = limit_price * (1.0 - RISK_THRESHOLD);
                let take_profit = limit_price * (1.0 + MIN_PROFIT_THRESHOLD_BPS as f64 / 10000.0 * 2.0);

                Ok(OrderParams {
                    side: OrderSide::Buy,
                    size: adjusted_size,
                    limit_price,
                    stop_loss: Some(stop_loss),
                    take_profit: Some(take_profit),
                    time_in_force: TimeInForce::IOC,
                })
            },
            Signal::Sell { confidence, size_ratio } => {
                let base_size = market_data.position_size * size_ratio;
                let adjusted_size = self.apply_risk_limits(base_size, market_data)?;
                
                let limit_price = market_data.ask * (1.0 - 0.0001);
                let stop_loss = limit_price * (1.0 + RISK_THRESHOLD);
                let take_profit = limit_price * (1.0 - MIN_PROFIT_THRESHOLD_BPS as f64 / 10000.0 * 2.0);

                Ok(OrderParams {
                    side: OrderSide::Sell,
                    size: adjusted_size,
                    limit_price,
                    stop_loss: Some(stop_loss),
                    take_profit: Some(take_profit),
                    time_in_force: TimeInForce::IOC,
                })
            },
            Signal::Hold => Err("No trade signal"),
        }
    }

    fn apply_risk_limits(&self, size: f64, market_data: &MarketData) -> Result<f64, &'static str> {
        let max_position = market_data.available_balance * MAX_POSITION_SIZE_BPS as f64 / 10000.0;
        let leverage_adjusted = size.min(market_data.available_balance * MAX_LEVERAGE);
        
        let liquidity_constraint = (market_data.bid_liquidity + market_data.ask_liquidity) * 0.05;
        
        Ok(size.min(max_position).min(leverage_adjusted).min(liquidity_constraint))
    }

    pub fn get_execution_priority(&self, signal: &Signal) -> ExecutionPriority {
        match signal {
            Signal::Buy { confidence, .. } | Signal::Sell { confidence, .. } => {
                if *confidence > 0.95 {
                    ExecutionPriority::Critical
                } else if *confidence > 0.9 {
                    ExecutionPriority::High
                } else {
                    ExecutionPriority::Normal
                }
            },
            Signal::Hold => ExecutionPriority::Low,
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct RiskMetrics {
    pub sharpe_ratio: f64,
    pub max_drawdown: f64,
    pub current_volatility: f64,
    pub win_rate: f64,
    pub total_trades: u64,
}

#[derive(Debug, Clone)]
pub struct MarketData {
    pub bid: f64,
    pub ask: f64,
    pub mid: f64,
    pub bid_liquidity: f64,
    pub ask_liquidity: f64,
    pub available_balance: f64,
    pub position_size: f64,
}

#[derive(Debug, Clone)]
pub struct OrderParams {
    pub side: OrderSide,
    pub size: f64,
    pub limit_price: f64,
    pub stop_loss: Option<f64>,
    pub take_profit: Option<f64>,
    pub time_in_force: TimeInForce,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum OrderSide {
    Buy,
    Sell,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum TimeInForce {
    IOC,
    FOK,
    GTC,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum ExecutionPriority {
    Low,
    Normal,
    High,
    Critical,
}

impl Default for OrnsteinUhlenbeckStrategy {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_strategy_initialization() {
        let strategy = OrnsteinUhlenbeckStrategy::new();
        assert!(strategy.price_history.read().unwrap().is_empty());
    }

    #[test]
    fn test_price_update() {
        let strategy = OrnsteinUhlenbeckStrategy::new();
        assert!(strategy.update_price(100.0, 1000.0, 10).is_ok());
        assert_eq!(strategy.price_history.read().unwrap().len(), 1);
    }

    #[test]
    fn test_signal_generation() {
        let strategy = OrnsteinUhlenbeckStrategy::new();
        
        for i in 0..MIN_SAMPLES_FOR_CALCULATION {
            let price = 100.0 + (i as f64 * 0.1);
            strategy.update_price(price, 1000.0, 10).unwrap();
        }
        
        let signal = strategy.get_signal().unwrap();
        assert!(matches!(signal, Signal::Hold | Signal::Buy { .. } | Signal::Sell { .. }));
    }
}

