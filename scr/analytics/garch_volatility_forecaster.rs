use std::collections::VecDeque;
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};
use serde::{Deserialize, Serialize};
use thiserror::Error;

const MIN_OBSERVATIONS: usize = 100;
const MAX_OBSERVATIONS: usize = 1000;
const MAX_ITERATIONS: usize = 500;
const CONVERGENCE_TOLERANCE: f64 = 1e-8;
const INITIAL_OMEGA: f64 = 0.00001;
const INITIAL_ALPHA: f64 = 0.1;
const INITIAL_BETA: f64 = 0.85;
const MIN_VARIANCE: f64 = 1e-10;
const MAX_VARIANCE: f64 = 10.0;
const FORECAST_HORIZON: usize = 20;
const UPDATE_INTERVAL_MS: u64 = 100;

#[derive(Debug, Error)]
pub enum GarchError {
    #[error("Insufficient data points: {0} < {1}")]
    InsufficientData(usize, usize),
    #[error("Invalid parameters: omega={omega}, alpha={alpha}, beta={beta}")]
    InvalidParameters { omega: f64, alpha: f64, beta: f64 },
    #[error("Convergence failed after {0} iterations")]
    ConvergenceFailed(usize),
    #[error("Numerical instability detected")]
    NumericalInstability,
    #[error("Invalid forecast horizon: {0}")]
    InvalidHorizon(usize),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GarchParameters {
    pub omega: f64,
    pub alpha: f64,
    pub beta: f64,
    pub log_likelihood: f64,
    pub aic: f64,
    pub bic: f64,
    pub persistence: f64,
    pub unconditional_variance: f64,
}

#[derive(Debug, Clone)]
pub struct VolatilityForecast {
    pub timestamp: u64,
    pub horizon: Vec<f64>,
    pub confidence_intervals: Vec<(f64, f64)>,
    pub annualized_vol: f64,
    pub half_life: f64,
    pub vix_equivalent: f64,
}

#[derive(Debug)]
struct OptimizationState {
    params: [f64; 3],
    gradient: [f64; 3],
    hessian: [[f64; 3]; 3],
    step_size: f64,
    iteration: usize,
}

pub struct GarchVolatilityForecaster {
    price_returns: Arc<RwLock<VecDeque<f64>>>,
    timestamps: Arc<RwLock<VecDeque<u64>>>,
    current_params: Arc<RwLock<Option<GarchParameters>>>,
    conditional_variances: Arc<RwLock<Vec<f64>>>,
    last_update: Arc<RwLock<Instant>>,
    update_lock: Arc<tokio::sync::Mutex<()>>,
}

impl GarchVolatilityForecaster {
    pub fn new() -> Self {
        Self {
            price_returns: Arc::new(RwLock::new(VecDeque::with_capacity(MAX_OBSERVATIONS))),
            timestamps: Arc::new(RwLock::new(VecDeque::with_capacity(MAX_OBSERVATIONS))),
            current_params: Arc::new(RwLock::new(None)),
            conditional_variances: Arc::new(RwLock::new(Vec::with_capacity(MAX_OBSERVATIONS))),
            last_update: Arc::new(RwLock::new(Instant::now())),
            update_lock: Arc::new(tokio::sync::Mutex::new(())),
        }
    }

    pub async fn add_price_observation(&self, price: f64, timestamp: u64) -> Result<(), GarchError> {
        let _lock = self.update_lock.lock().await;
        
        let should_update = {
            let mut returns = self.price_returns.write().unwrap();
            let mut timestamps_guard = self.timestamps.write().unwrap();
            
            if let Some(&last_timestamp) = timestamps_guard.back() {
                if timestamp <= last_timestamp {
                    return Ok(());
                }
            }
            
            if returns.len() > 0 {
                let last_price = returns.iter().sum::<f64>() + price;
                let log_return = (price / last_price).ln();
                
                if log_return.is_finite() && log_return.abs() < 0.5 {
                    returns.push_back(log_return);
                    timestamps_guard.push_back(timestamp);
                    
                    if returns.len() > MAX_OBSERVATIONS {
                        returns.pop_front();
                        timestamps_guard.pop_front();
                    }
                }
            } else {
                returns.push_back(0.0);
                timestamps_guard.push_back(timestamp);
            }
            
            returns.len() >= MIN_OBSERVATIONS
        };
        
        if should_update {
            let last_update = *self.last_update.read().unwrap();
            if last_update.elapsed() > Duration::from_millis(UPDATE_INTERVAL_MS) {
                self.estimate_parameters_internal()?;
                *self.last_update.write().unwrap() = Instant::now();
            }
        }
        
        Ok(())
    }

    fn estimate_parameters_internal(&self) -> Result<(), GarchError> {
        let returns = self.price_returns.read().unwrap();
        if returns.len() < MIN_OBSERVATIONS {
            return Err(GarchError::InsufficientData(returns.len(), MIN_OBSERVATIONS));
        }
        
        let returns_vec: Vec<f64> = returns.iter().copied().collect();
        let mut state = self.initialize_optimization(&returns_vec);
        
        let mut converged = false;
        for _ in 0..MAX_ITERATIONS {
            let prev_params = state.params;
            
            self.compute_gradient_and_hessian(&mut state, &returns_vec)?;
            self.update_parameters(&mut state)?;
            
            let param_change = ((state.params[0] - prev_params[0]).powi(2) +
                              (state.params[1] - prev_params[1]).powi(2) +
                              (state.params[2] - prev_params[2]).powi(2)).sqrt();
            
            if param_change < CONVERGENCE_TOLERANCE {
                converged = true;
                break;
            }
            
            state.iteration += 1;
        }
        
        if !converged {
            return Err(GarchError::ConvergenceFailed(MAX_ITERATIONS));
        }
        
        let params = self.finalize_parameters(&state, &returns_vec)?;
        *self.current_params.write().unwrap() = Some(params);
        
        Ok(())
    }

    fn initialize_optimization(&self, returns: &[f64]) -> OptimizationState {
        let variance = returns.iter().map(|&r| r * r).sum::<f64>() / returns.len() as f64;
        
        OptimizationState {
            params: [
                INITIAL_OMEGA.max(variance * 0.01),
                INITIAL_ALPHA,
                INITIAL_BETA,
            ],
            gradient: [0.0; 3],
            hessian: [[0.0; 3]; 3],
            step_size: 0.1,
            iteration: 0,
        }
    }

    fn compute_gradient_and_hessian(&self, state: &mut OptimizationState, returns: &[f64]) -> Result<(), GarchError> {
        let n = returns.len();
        let (omega, alpha, beta) = (state.params[0], state.params[1], state.params[2]);
        
        if !self.validate_parameters(omega, alpha, beta) {
            return Err(GarchError::InvalidParameters { omega, alpha, beta });
        }
        
        let mut variances = vec![0.0; n];
        let unconditional_var = omega / (1.0 - alpha - beta).max(0.001);
        variances[0] = unconditional_var;
        
        for t in 1..n {
            variances[t] = omega + alpha * returns[t-1].powi(2) + beta * variances[t-1];
            variances[t] = variances[t].clamp(MIN_VARIANCE, MAX_VARIANCE);
        }
        
        state.gradient = [0.0; 3];
        state.hessian = [[0.0; 3]; 3];
        
        let mut d_variances_d_omega = vec![0.0; n];
        let mut d_variances_d_alpha = vec![0.0; n];
        let mut d_variances_d_beta = vec![0.0; n];
        
        d_variances_d_omega[0] = 1.0 / (1.0 - alpha - beta).max(0.001);
        d_variances_d_alpha[0] = omega / (1.0 - alpha - beta).powi(2).max(0.001);
        d_variances_d_beta[0] = omega / (1.0 - alpha - beta).powi(2).max(0.001);
        
        for t in 1..n {
            d_variances_d_omega[t] = 1.0 + beta * d_variances_d_omega[t-1];
            d_variances_d_alpha[t] = returns[t-1].powi(2) + beta * d_variances_d_alpha[t-1];
            d_variances_d_beta[t] = variances[t-1] + beta * d_variances_d_beta[t-1];
        }
        
        for t in 0..n {
            let inv_var = 1.0 / variances[t];
            let scaled_return = returns[t].powi(2) * inv_var;
            
            state.gradient[0] += d_variances_d_omega[t] * inv_var * (scaled_return - 1.0);
            state.gradient[1] += d_variances_d_alpha[t] * inv_var * (scaled_return - 1.0);
            state.gradient[2] += d_variances_d_beta[t] * inv_var * (scaled_return - 1.0);
            
            let inv_var_sq = inv_var * inv_var;
            state.hessian[0][0] += d_variances_d_omega[t].powi(2) * inv_var_sq * (1.0 - 2.0 * scaled_return);
            state.hessian[0][1] += d_variances_d_omega[t] * d_variances_d_alpha[t] * inv_var_sq * (1.0 - 2.0 * scaled_return);
            state.hessian[0][2] += d_variances_d_omega[t] * d_variances_d_beta[t] * inv_var_sq * (1.0 - 2.0 * scaled_return);
            state.hessian[1][1] += d_variances_d_alpha[t].powi(2) * inv_var_sq * (1.0 - 2.0 * scaled_return);
            state.hessian[1][2] += d_variances_d_alpha[t] * d_variances_d_beta[t] * inv_var_sq * (1.0 - 2.0 * scaled_return);
            state.hessian[2][2] += d_variances_d_beta[t].powi(2) * inv_var_sq * (1.0 - 2.0 * scaled_return);
        }
        
        state.hessian[1][0] = state.hessian[0][1];
        state.hessian[2][0] = state.hessian[0][2];
        state.hessian[2][1] = state.hessian[1][2];
        
        for i in 0..3 {
            state.gradient[i] *= -0.5;
            for j in 0..3 {
                state.hessian[i][j] *= -0.5;
            }
        }
        
        *self.conditional_variances.write().unwrap() = variances;
        
        Ok(())
    }

    fn update_parameters(&self, state: &mut OptimizationState) -> Result<(), GarchError> {
        let mut hessian_inv = [[0.0; 3]; 3];
        let det = self.matrix_determinant_3x3(&state.hessian);
        
        if det.abs() < 1e-10 {
            return Err(GarchError::NumericalInstability);
        }
        
        self.matrix_inverse_3x3(&state.hessian, &mut hessian_inv, det);
        
        let mut direction = [0.0; 3];
        for i in 0..3 {
            for j in 0..3 {
                direction[i] -= hessian_inv[i][j] * state.gradient[j];
            }
        }
        
        let mut step = state.step_size;
        let mut found_valid = false;
        
        for _ in 0..10 {
            let new_omega = (state.params[0] + step * direction[0]).max(1e-8);
            let new_alpha = (state.params[1] + step * direction[1]).clamp(0.0, 0.999);
            let new_beta = (state.params[2] + step * direction[2]).clamp(0.0, 0.999);
            
            if self.validate_parameters(new_omega, new_alpha, new_beta) {
                state.params = [new_omega, new_alpha, new_beta];
                found_valid = true;
                break;
            }
            
            step *= 0.5;
        }
        
        if !found_valid {
            return Err(GarchError::NumericalInstability);
        }
        
        if state.iteration > 10 {
            state.step_size = (state.step_size * 0.95).max(0.01);
        }
        
        Ok(())
    }

        fn finalize_parameters(&self, state: &OptimizationState, returns: &[f64]) -> Result<GarchParameters, GarchError> {
        let (omega, alpha, beta) = (state.params[0], state.params[1], state.params[2]);
        let n = returns.len() as f64;
        
        let variances = self.conditional_variances.read().unwrap();
        let mut log_likelihood = 0.0;
        
        for (t, &ret) in returns.iter().enumerate() {
            let var = variances[t];
            log_likelihood -= 0.5 * ((ret * ret) / var + var.ln() + std::f64::consts::LN_2PI);
        }
        
        let num_params = 3.0;
        let aic = -2.0 * log_likelihood + 2.0 * num_params;
        let bic = -2.0 * log_likelihood + num_params * n.ln();
        
        let persistence = alpha + beta;
        let unconditional_variance = if persistence < 0.999 {
            omega / (1.0 - persistence)
        } else {
            omega / 0.001
        };
        
        Ok(GarchParameters {
            omega,
            alpha,
            beta,
            log_likelihood,
            aic,
            bic,
            persistence,
            unconditional_variance,
        })
    }

    pub fn forecast_volatility(&self, horizon: usize) -> Result<VolatilityForecast, GarchError> {
        if horizon == 0 || horizon > FORECAST_HORIZON {
            return Err(GarchError::InvalidHorizon(horizon));
        }
        
        let params = self.current_params.read().unwrap()
            .clone()
            .ok_or(GarchError::InsufficientData(0, MIN_OBSERVATIONS))?;
        
        let returns = self.price_returns.read().unwrap();
        let timestamps = self.timestamps.read().unwrap();
        let variances = self.conditional_variances.read().unwrap();
        
        if returns.is_empty() || variances.is_empty() {
            return Err(GarchError::InsufficientData(0, MIN_OBSERVATIONS));
        }
        
        let last_return = returns.back().copied().unwrap_or(0.0);
        let last_variance = variances.last().copied().unwrap_or(params.unconditional_variance);
        let current_timestamp = timestamps.back().copied().unwrap_or(0);
        
        let mut forecast_variances = Vec::with_capacity(horizon);
        let mut confidence_intervals = Vec::with_capacity(horizon);
        
        let mut h_t = last_variance;
        let mut cumulative_variance = 0.0;
        
        for t in 0..horizon {
            if t == 0 {
                h_t = params.omega + params.alpha * last_return.powi(2) + params.beta * last_variance;
            } else {
                h_t = params.omega + (params.alpha + params.beta) * h_t;
            }
            
            h_t = h_t.clamp(MIN_VARIANCE, MAX_VARIANCE);
            forecast_variances.push(h_t);
            
            cumulative_variance += h_t;
            let std_dev = cumulative_variance.sqrt();
            let z_score = 1.96;
            
            confidence_intervals.push((
                -z_score * std_dev,
                z_score * std_dev
            ));
        }
        
        let annualized_vol = (forecast_variances[0] * 252.0).sqrt();
        
        let half_life = if params.persistence < 0.999 {
            -(0.5_f64.ln()) / params.persistence.ln()
        } else {
            1000.0
        };
        
        let vix_equivalent = annualized_vol * 100.0;
        
        Ok(VolatilityForecast {
            timestamp: current_timestamp,
            horizon: forecast_variances,
            confidence_intervals,
            annualized_vol,
            half_life,
            vix_equivalent,
        })
    }

    pub fn get_risk_metrics(&self) -> Option<RiskMetrics> {
        let params = self.current_params.read().unwrap().as_ref()?.clone();
        let variances = self.conditional_variances.read().unwrap();
        
        if variances.is_empty() {
            return None;
        }
        
        let current_variance = variances.last().copied().unwrap_or(params.unconditional_variance);
        let std_dev = current_variance.sqrt();
        
        let var_95 = std_dev * 1.645;
        let var_99 = std_dev * 2.326;
        let cvar_95 = std_dev * 2.063;
        let cvar_99 = std_dev * 2.665;
        
        let volatility_ratio = current_variance / params.unconditional_variance;
        let mean_reversion_speed = -params.persistence.ln();
        
        Some(RiskMetrics {
            current_volatility: std_dev,
            var_95,
            var_99,
            cvar_95,
            cvar_99,
            volatility_ratio,
            mean_reversion_speed,
            persistence: params.persistence,
        })
    }

    pub fn get_market_regime(&self) -> MarketRegime {
        let risk_metrics = match self.get_risk_metrics() {
            Some(metrics) => metrics,
            None => return MarketRegime::Unknown,
        };
        
        let vol = risk_metrics.current_volatility;
        let vol_ratio = risk_metrics.volatility_ratio;
        
        if vol < 0.001 && vol_ratio < 0.5 {
            MarketRegime::LowVolatility
        } else if vol < 0.002 && vol_ratio < 1.5 {
            MarketRegime::Normal
        } else if vol < 0.005 && vol_ratio < 3.0 {
            MarketRegime::Elevated
        } else if vol < 0.01 {
            MarketRegime::HighVolatility
        } else {
            MarketRegime::Extreme
        }
    }

    pub fn get_volatility_percentile(&self, lookback_periods: usize) -> Option<f64> {
        let variances = self.conditional_variances.read().unwrap();
        
        if variances.len() < lookback_periods {
            return None;
        }
        
        let current_var = variances.last()?;
        let start_idx = variances.len().saturating_sub(lookback_periods);
        let historical = &variances[start_idx..];
        
        let rank = historical.iter()
            .filter(|&&v| v <= *current_var)
            .count() as f64;
        
        Some(rank / historical.len() as f64 * 100.0)
    }

    pub fn get_volatility_term_structure(&self) -> Option<Vec<(usize, f64)>> {
        let params = self.current_params.read().unwrap().as_ref()?.clone();
        let variances = self.conditional_variances.read().unwrap();
        
        if variances.is_empty() {
            return None;
        }
        
        let horizons = vec![1, 5, 10, 20, 50, 100];
        let mut term_structure = Vec::with_capacity(horizons.len());
        
        let last_variance = variances.last().copied().unwrap_or(params.unconditional_variance);
        
        for &h in &horizons {
            let forecast_var = if params.persistence < 0.999 {
                params.unconditional_variance + 
                (last_variance - params.unconditional_variance) * params.persistence.powi(h as i32)
            } else {
                last_variance
            };
            
            let annualized_vol = (forecast_var * 252.0).sqrt();
            term_structure.push((h, annualized_vol));
        }
        
        Some(term_structure)
    }

    pub fn calculate_volatility_smile(&self, moneyness_range: &[f64]) -> Vec<(f64, f64)> {
        let risk_metrics = match self.get_risk_metrics() {
            Some(metrics) => metrics,
            None => return vec![],
        };
        
        let base_vol = risk_metrics.current_volatility;
        let mut smile = Vec::with_capacity(moneyness_range.len());
        
        for &moneyness in moneyness_range {
            let smile_adjustment = if moneyness < 1.0 {
                let otm_factor = (1.0 - moneyness).abs();
                1.0 + 0.15 * otm_factor + 0.25 * otm_factor.powi(2)
            } else if moneyness > 1.0 {
                let otm_factor = (moneyness - 1.0).abs();
                1.0 + 0.10 * otm_factor + 0.15 * otm_factor.powi(2)
            } else {
                1.0
            };
            
            smile.push((moneyness, base_vol * smile_adjustment));
        }
        
        smile
    }

    fn validate_parameters(&self, omega: f64, alpha: f64, beta: f64) -> bool {
        omega > 0.0 && 
        alpha >= 0.0 && 
        beta >= 0.0 && 
        alpha + beta < 0.9999 &&
        omega.is_finite() &&
        alpha.is_finite() &&
        beta.is_finite()
    }

    fn matrix_determinant_3x3(&self, m: &[[f64; 3]; 3]) -> f64 {
        m[0][0] * (m[1][1] * m[2][2] - m[1][2] * m[2][1]) -
        m[0][1] * (m[1][0] * m[2][2] - m[1][2] * m[2][0]) +
        m[0][2] * (m[1][0] * m[2][1] - m[1][1] * m[2][0])
    }

    fn matrix_inverse_3x3(&self, m: &[[f64; 3]; 3], inv: &mut [[f64; 3]; 3], det: f64) {
        let inv_det = 1.0 / det;
        
        inv[0][0] = (m[1][1] * m[2][2] - m[1][2] * m[2][1]) * inv_det;
        inv[0][1] = (m[0][2] * m[2][1] - m[0][1] * m[2][2]) * inv_det;
        inv[0][2] = (m[0][1] * m[1][2] - m[0][2] * m[1][1]) * inv_det;
        
        inv[1][0] = (m[1][2] * m[2][0] - m[1][0] * m[2][2]) * inv_det;
        inv[1][1] = (m[0][0] * m[2][2] - m[0][2] * m[2][0]) * inv_det;
        inv[1][2] = (m[0][2] * m[1][0] - m[0][0] * m[1][2]) * inv_det;
        
        inv[2][0] = (m[1][0] * m[2][1] - m[1][1] * m[2][0]) * inv_det;
        inv[2][1] = (m[0][1] * m[2][0] - m[0][0] * m[2][1]) * inv_det;
        inv[2][2] = (m[0][0] * m[1][1] - m[0][1] * m[1][0]) * inv_det;
    }

    pub async fn get_adaptive_position_size(&self, base_size: f64, max_leverage: f64) -> f64 {
        let risk_metrics = match self.get_risk_metrics() {
            Some(metrics) => metrics,
            None => return base_size * 0.1,
        };
        
        let vol_percentile = self.get_volatility_percentile(100).unwrap_or(50.0);
        let regime = self.get_market_regime();
        
        let vol_scalar = match regime {
            MarketRegime::LowVolatility => 1.5,
            MarketRegime::Normal => 1.0,
            MarketRegime::Elevated => 0.7,
            MarketRegime::HighVolatility => 0.4,
            MarketRegime::Extreme => 0.2,
            MarketRegime::Unknown => 0.5,
        };
        
        let percentile_scalar = if vol_percentile > 90.0 {
            0.5
        } else if vol_percentile > 75.0 {
            0.7
        } else if vol_percentile < 25.0 {
            1.3
        } else {
            1.0
        };
        
        let kelly_fraction = 0.25;
        let sharpe_target = 2.0;
        let vol_target = sharpe_target / (252.0_f64.sqrt());
        
        let position_vol_scalar = (vol_target / risk_metrics.current_volatility).min(2.0).max(0.1);
        
        let final_size = base_size * vol_scalar * percentile_scalar * position_vol_scalar * kelly_fraction;
        
        (final_size).min(base_size * max_leverage)
    }
}

#[derive(Debug, Clone, Copy)]
pub struct RiskMetrics {
    pub current_volatility: f64,
    pub var_95: f64,
    pub var_99: f64,
    pub cvar_95: f64,
    pub cvar_99: f64,
    pub volatility_ratio: f64,
    pub mean_reversion_speed: f64,
    pub persistence: f64,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum MarketRegime {
    LowVolatility,
    Normal,
    Elevated,
    HighVolatility,
    Extreme,
    Unknown,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_garch_initialization() {
        let forecaster = GarchVolatilityForecaster::new();
        assert!(forecaster.current_params.read().unwrap().is_none());
    }

    #[tokio::test]
    async fn test_price_observation() {
        let forecaster = GarchVolatilityForecaster::new();
        let result = forecaster.add_price_observation(100.0, 1000).await;
        assert!(result.is_ok());
    }
}

