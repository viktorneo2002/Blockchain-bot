use nalgebra::{DMatrix, DVector};
use std::collections::VecDeque;
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};
use solana_sdk::pubkey::Pubkey;
use rayon::prelude::*;

const MAX_LAG_ORDER: usize = 5;
const MIN_OBSERVATIONS: usize = 100;
const REGULARIZATION_LAMBDA: f64 = 0.001;
const CONFIDENCE_THRESHOLD: f64 = 0.85;
const MAX_POSITION_SIZE: f64 = 0.1;
const RISK_TOLERANCE: f64 = 0.02;
const PREDICTION_HORIZON: usize = 3;
const COINTEGRATION_THRESHOLD: f64 = 0.03;
const GRANGER_CAUSALITY_PVALUE: f64 = 0.05;
const AIC_PENALTY: f64 = 2.0;
const UPDATE_FREQUENCY_MS: u64 = 100;
const OUTLIER_THRESHOLD: f64 = 3.5;
const EIGENVALUE_TOLERANCE: f64 = 1e-10;

#[derive(Clone, Debug)]
pub struct VARParameters {
    pub coefficients: Vec<DMatrix<f64>>,
    pub intercept: DVector<f64>,
    pub residual_covariance: DMatrix<f64>,
    pub lag_order: usize,
    pub aic_score: f64,
    pub last_update: Instant,
}

#[derive(Clone, Debug)]
pub struct MarketObservation {
    pub timestamp: u64,
    pub prices: DVector<f64>,
    pub volumes: DVector<f64>,
    pub spreads: DVector<f64>,
}

#[derive(Debug, Clone)]
pub struct VARPrediction {
    pub predicted_prices: Vec<DVector<f64>>,
    pub confidence_intervals: Vec<(DVector<f64>, DVector<f64>)>,
    pub arbitrage_signal: f64,
    pub position_sizes: DVector<f64>,
    pub risk_metric: f64,
    pub execution_priority: f64,
}

pub struct VectorAutoregressionEngine {
    observations: Arc<RwLock<VecDeque<MarketObservation>>>,
    var_params: Arc<RwLock<Option<VARParameters>>>,
    asset_pubkeys: Vec<Pubkey>,
    n_assets: usize,
    min_liquidity: f64,
    last_prediction: Arc<RwLock<Option<VARPrediction>>>,
}

impl VectorAutoregressionEngine {
    pub fn new(asset_pubkeys: Vec<Pubkey>, min_liquidity: f64) -> Self {
        let n_assets = asset_pubkeys.len();
        Self {
            observations: Arc::new(RwLock::new(VecDeque::with_capacity(MIN_OBSERVATIONS * 2))),
            var_params: Arc::new(RwLock::new(None)),
            asset_pubkeys,
            n_assets,
            min_liquidity,
            last_prediction: Arc::new(RwLock::new(None)),
        }
    }

    pub fn add_observation(&self, observation: MarketObservation) -> Result<(), &'static str> {
        let mut obs = self.observations.write().unwrap();
        
        if self.detect_outlier(&observation, &obs) {
            return Err("Outlier detected");
        }
        
        obs.push_back(observation);
        if obs.len() > MIN_OBSERVATIONS * 2 {
            obs.pop_front();
        }
        
        if obs.len() >= MIN_OBSERVATIONS {
            drop(obs);
            self.update_var_model()?;
        }
        
        Ok(())
    }

    fn detect_outlier(&self, observation: &MarketObservation, history: &VecDeque<MarketObservation>) -> bool {
        if history.len() < 20 {
            return false;
        }
        
        let recent: Vec<&MarketObservation> = history.iter().rev().take(20).collect();
        let mut means = DVector::zeros(self.n_assets);
        let mut stds = DVector::zeros(self.n_assets);
        
        for i in 0..self.n_assets {
            let prices: Vec<f64> = recent.iter().map(|o| o.prices[i]).collect();
            means[i] = prices.iter().sum::<f64>() / prices.len() as f64;
            let variance = prices.iter().map(|p| (p - means[i]).powi(2)).sum::<f64>() / prices.len() as f64;
            stds[i] = variance.sqrt();
        }
        
        observation.prices.iter().enumerate().any(|(i, &price)| {
            (price - means[i]).abs() > OUTLIER_THRESHOLD * stds[i]
        })
    }

    fn update_var_model(&self) -> Result<(), &'static str> {
        let obs = self.observations.read().unwrap();
        if obs.len() < MIN_OBSERVATIONS {
            return Err("Insufficient observations");
        }
        
        let data_matrix = self.prepare_data_matrix(&obs);
        let optimal_lag = self.select_optimal_lag(&data_matrix)?;
        
        let (y, x) = self.construct_regression_matrices(&data_matrix, optimal_lag);
        let params = self.estimate_var_parameters(y, x, optimal_lag)?;
        
        *self.var_params.write().unwrap() = Some(params);
        Ok(())
    }

    fn prepare_data_matrix(&self, observations: &VecDeque<MarketObservation>) -> DMatrix<f64> {
        let n_obs = observations.len();
        let mut matrix = DMatrix::zeros(n_obs, self.n_assets * 3);
        
        for (i, obs) in observations.iter().enumerate() {
            for j in 0..self.n_assets {
                matrix[(i, j)] = obs.prices[j].ln();
                matrix[(i, self.n_assets + j)] = obs.volumes[j].ln().max(-10.0);
                matrix[(i, 2 * self.n_assets + j)] = obs.spreads[j];
            }
        }
        
        self.apply_differencing(matrix)
    }

    fn apply_differencing(&self, mut matrix: DMatrix<f64>) -> DMatrix<f64> {
        let n_rows = matrix.nrows();
        let n_cols = matrix.ncols();
        let mut diff_matrix = DMatrix::zeros(n_rows - 1, n_cols);
        
        for i in 1..n_rows {
            for j in 0..n_cols {
                diff_matrix[(i - 1, j)] = matrix[(i, j)] - matrix[(i - 1, j)];
            }
        }
        
        diff_matrix
    }

    fn select_optimal_lag(&self, data: &DMatrix<f64>) -> Result<usize, &'static str> {
        let mut best_aic = f64::INFINITY;
        let mut best_lag = 1;
        
        for lag in 1..=MAX_LAG_ORDER.min(data.nrows() / 10) {
            let (y, x) = self.construct_regression_matrices(data, lag);
            
            if let Ok(params) = self.estimate_var_parameters(y.clone(), x.clone(), lag) {
                if params.aic_score < best_aic {
                    best_aic = params.aic_score;
                    best_lag = lag;
                }
            }
        }
        
        Ok(best_lag)
    }

    fn construct_regression_matrices(&self, data: &DMatrix<f64>, lag: usize) -> (DMatrix<f64>, DMatrix<f64>) {
        let n_vars = self.n_assets;
        let t = data.nrows() - lag;
        
        let mut y = DMatrix::zeros(t, n_vars);
        let mut x = DMatrix::zeros(t, n_vars * lag + 1);
        
        for i in 0..t {
            for j in 0..n_vars {
                y[(i, j)] = data[(i + lag, j)];
            }
            
            x[(i, 0)] = 1.0;
            
            for l in 0..lag {
                for j in 0..n_vars {
                    x[(i, 1 + l * n_vars + j)] = data[(i + lag - l - 1, j)];
                }
            }
        }
        
        (y, x)
    }

    fn estimate_var_parameters(&self, y: DMatrix<f64>, x: DMatrix<f64>, lag: usize) -> Result<VARParameters, &'static str> {
        let xtx = &x.transpose() * &x;
        let regularization = DMatrix::identity(xtx.nrows(), xtx.ncols()) * REGULARIZATION_LAMBDA;
        let xtx_reg = xtx + regularization;
        
        let xtx_inv = self.robust_matrix_inverse(xtx_reg)?;
        let beta = xtx_inv * x.transpose() * &y;
        
        let residuals = &y - &x * &beta;
        let residual_covariance = self.calculate_residual_covariance(&residuals);
        
        let intercept = beta.row(0).transpose();
        let mut coefficients = Vec::with_capacity(lag);
        
        for l in 0..lag {
            let start_col = 1 + l * self.n_assets;
            let end_col = start_col + self.n_assets;
            let coef_matrix = beta.rows(start_col, self.n_assets).transpose();
            coefficients.push(coef_matrix);
        }
        
        let aic_score = self.calculate_aic(&residuals, lag);
        
        Ok(VARParameters {
            coefficients,
            intercept,
            residual_covariance,
            lag_order: lag,
            aic_score,
            last_update: Instant::now(),
        })
    }

    fn robust_matrix_inverse(&self, matrix: DMatrix<f64>) -> Result<DMatrix<f64>, &'static str> {
        let svd = matrix.svd(true, true);
        let u = svd.u.ok_or("SVD U matrix missing")?;
        let v_t = svd.v_t.ok_or("SVD V^T matrix missing")?;
        
        let mut inv_singular = DMatrix::zeros(svd.singular_values.len(), svd.singular_values.len());
        for (i, &s) in svd.singular_values.iter().enumerate() {
            if s.abs() > EIGENVALUE_TOLERANCE {
                inv_singular[(i, i)] = 1.0 / s;
            }
        }
        
        Ok(v_t.transpose() * inv_singular * u.transpose())
    }

    fn calculate_residual_covariance(&self, residuals: &DMatrix<f64>) -> DMatrix<f64> {
        let n = residuals.nrows() as f64;
        (residuals.transpose() * residuals) / n
    }

    fn calculate_aic(&self, residuals: &DMatrix<f64>, lag: usize) -> f64 {
        let n = residuals.nrows() as f64;
        let k = (self.n_assets * self.n_assets * lag + self.n_assets) as f64;
        let rss = residuals.iter().map(|r| r * r).sum::<f64>();
        
        n * (1.0 + (2.0 * std::f64::consts::PI).ln()) + n * (rss / n).ln() + AIC_PENALTY * k
    }

    pub fn generate_prediction(&self) -> Result<VARPrediction, &'static str> {
        let params = self.var_params.read().unwrap();
        let params = params.as_ref().ok_or("VAR model not initialized")?;
        
        if params.last_update.elapsed() > Duration::from_millis(UPDATE_FREQUENCY_MS * 10) {
            return Err("Model outdated");
        }
        
        let obs = self.observations.read().unwrap();
        if obs.is_empty() {
            return Err("No observations");
        }
        
        let recent_data = self.get_recent_data(&obs, params.lag_order);
        let predictions = self.forecast_multi_step(&recent_data, params, PREDICTION_HORIZON)?;
        let confidence_intervals = self.calculate_confidence_intervals(&predictions, params);
        
        let arbitrage_signal = self.detect_arbitrage_opportunity(&predictions, &recent_data);
        let position_sizes = self.calculate_position_sizes(&predictions, params, arbitrage_signal);
        let risk_metric = self.calculate_var_risk(&position_sizes, params);
        let execution_priority = self.calculate_execution_priority(arbitrage_signal, risk_metric);
        
        let prediction = VARPrediction {
            predicted_prices: predictions,
            confidence_intervals,
            arbitrage_signal,
            position_sizes,
            risk_metric,
            execution_priority,
        };
        
        *self.last_prediction.write().unwrap() = Some(prediction.clone());
        Ok(prediction)
    }

    fn get_recent_data(&self, observations: &VecDeque<MarketObservation>, lag: usize) -> Vec<DVector<f64>> {
        observations.iter()
            .rev()
            .take(lag)
            .map(|obs| {
                let mut vec = DVector::zeros(self.n_assets);
                for i in 0..self.n_assets {
                    vec[i] = obs.prices[i].ln();
                }
                vec
            })
            .collect::<Vec<_>>()
            .into_iter()
            .rev()
            .collect()
    }

    fn forecast_multi_step(&self, initial_data: &[DVector<f64>], params: &VARParameters, horizon: usize) -> Result<Vec<DVector<f64>>, &'static str> {
        let mut predictions = Vec::with_capacity(horizon);
        let mut recent = initial_data.to_vec();
        
                for _ in 0..horizon {
            let mut next_value = params.intercept.clone();
            
            for (lag_idx, coef_matrix) in params.coefficients.iter().enumerate() {
                if lag_idx < recent.len() {
                    next_value += coef_matrix * &recent[recent.len() - 1 - lag_idx];
                }
            }
            
            predictions.push(next_value.clone());
            recent.push(next_value);
            if recent.len() > params.lag_order {
                recent.remove(0);
            }
        }
        
        Ok(predictions)
    }

    fn calculate_confidence_intervals(&self, predictions: &[DVector<f64>], params: &VARParameters) -> Vec<(DVector<f64>, DVector<f64>)> {
        let cholesky = params.residual_covariance.cholesky().unwrap_or_else(|| {
            let regularized = &params.residual_covariance + DMatrix::identity(self.n_assets, self.n_assets) * 1e-8;
            regularized.cholesky().unwrap()
        });
        
        predictions.iter().enumerate().map(|(h, pred)| {
            let std_multiplier = 1.96 * ((h + 1) as f64).sqrt();
            let uncertainty = &cholesky * std_multiplier;
            
            let lower = pred - uncertainty.diagonal();
            let upper = pred + uncertainty.diagonal();
            
            (lower, upper)
        }).collect()
    }

    fn detect_arbitrage_opportunity(&self, predictions: &[DVector<f64>], recent_data: &[DVector<f64>]) -> f64 {
        if predictions.is_empty() || recent_data.is_empty() {
            return 0.0;
        }
        
        let current = &recent_data[recent_data.len() - 1];
        let predicted = &predictions[0];
        
        let price_changes = predicted - current;
        let max_change = price_changes.iter().map(|x| x.abs()).fold(0.0, f64::max);
        let divergence = self.calculate_price_divergence(predictions);
        
        let cointegration_score = self.check_cointegration(recent_data);
        let momentum_score = self.calculate_momentum_score(recent_data);
        
        let raw_signal = max_change * divergence * cointegration_score * momentum_score;
        
        (raw_signal * 100.0).tanh()
    }

    fn calculate_price_divergence(&self, predictions: &[DVector<f64>]) -> f64 {
        if predictions.len() < 2 {
            return 0.0;
        }
        
        let mut total_divergence = 0.0;
        let n_pairs = (self.n_assets * (self.n_assets - 1)) / 2;
        
        for i in 0..self.n_assets {
            for j in (i + 1)..self.n_assets {
                let ratio_current = predictions[0][i] - predictions[0][j];
                let ratio_future = predictions[predictions.len() - 1][i] - predictions[predictions.len() - 1][j];
                total_divergence += (ratio_future - ratio_current).abs();
            }
        }
        
        total_divergence / n_pairs as f64
    }

    fn check_cointegration(&self, data: &[DVector<f64>]) -> f64 {
        if data.len() < 20 {
            return 0.5;
        }
        
        let mut max_cointegration = 0.0;
        
        for i in 0..self.n_assets {
            for j in (i + 1)..self.n_assets {
                let series1: Vec<f64> = data.iter().map(|d| d[i]).collect();
                let series2: Vec<f64> = data.iter().map(|d| d[j]).collect();
                
                let correlation = self.calculate_correlation(&series1, &series2);
                let stationarity = self.test_stationarity(&series1, &series2);
                
                let cointegration_score = correlation.abs() * stationarity;
                max_cointegration = max_cointegration.max(cointegration_score);
            }
        }
        
        max_cointegration
    }

    fn calculate_correlation(&self, series1: &[f64], series2: &[f64]) -> f64 {
        let n = series1.len() as f64;
        let mean1 = series1.iter().sum::<f64>() / n;
        let mean2 = series2.iter().sum::<f64>() / n;
        
        let mut cov = 0.0;
        let mut var1 = 0.0;
        let mut var2 = 0.0;
        
        for i in 0..series1.len() {
            let diff1 = series1[i] - mean1;
            let diff2 = series2[i] - mean2;
            cov += diff1 * diff2;
            var1 += diff1 * diff1;
            var2 += diff2 * diff2;
        }
        
        if var1 > 0.0 && var2 > 0.0 {
            cov / (var1.sqrt() * var2.sqrt())
        } else {
            0.0
        }
    }

    fn test_stationarity(&self, series1: &[f64], series2: &[f64]) -> f64 {
        let spread: Vec<f64> = series1.iter().zip(series2).map(|(a, b)| a - b).collect();
        
        let mean = spread.iter().sum::<f64>() / spread.len() as f64;
        let variance = spread.iter().map(|x| (x - mean).powi(2)).sum::<f64>() / spread.len() as f64;
        let std_dev = variance.sqrt();
        
        let normalized_spread: Vec<f64> = spread.iter().map(|x| (x - mean) / std_dev).collect();
        
        let mut crossings = 0;
        for i in 1..normalized_spread.len() {
            if normalized_spread[i - 1] * normalized_spread[i] < 0.0 {
                crossings += 1;
            }
        }
        
        let crossing_rate = crossings as f64 / normalized_spread.len() as f64;
        (crossing_rate * 2.0).min(1.0)
    }

    fn calculate_momentum_score(&self, data: &[DVector<f64>]) -> f64 {
        if data.len() < 3 {
            return 0.5;
        }
        
        let mut momentum_scores = DVector::zeros(self.n_assets);
        
        for i in 0..self.n_assets {
            let mut short_term_return = 0.0;
            let mut long_term_return = 0.0;
            
            let short_window = 5.min(data.len() - 1);
            let long_window = 20.min(data.len() - 1);
            
            if short_window > 0 {
                short_term_return = (data[data.len() - 1][i] - data[data.len() - 1 - short_window][i]) / short_window as f64;
            }
            
            if long_window > 0 {
                long_term_return = (data[data.len() - 1][i] - data[data.len() - 1 - long_window][i]) / long_window as f64;
            }
            
            momentum_scores[i] = (short_term_return - long_term_return).tanh();
        }
        
        momentum_scores.iter().map(|x| x.abs()).sum::<f64>() / self.n_assets as f64
    }

    fn calculate_position_sizes(&self, predictions: &[DVector<f64>], params: &VARParameters, arbitrage_signal: f64) -> DVector<f64> {
        let mut positions = DVector::zeros(self.n_assets);
        
        if predictions.is_empty() || arbitrage_signal.abs() < 0.1 {
            return positions;
        }
        
        let expected_returns = &predictions[0] - &predictions[predictions.len() - 1];
        let risk_adjusted_returns = self.risk_adjust_returns(&expected_returns, params);
        
        let total_allocation = MAX_POSITION_SIZE * arbitrage_signal.abs();
        let weights = self.optimize_portfolio_weights(&risk_adjusted_returns, params);
        
        for i in 0..self.n_assets {
            positions[i] = weights[i] * total_allocation * arbitrage_signal.signum();
            positions[i] = positions[i].max(-MAX_POSITION_SIZE).min(MAX_POSITION_SIZE);
        }
        
        positions
    }

    fn risk_adjust_returns(&self, expected_returns: &DVector<f64>, params: &VARParameters) -> DVector<f64> {
        let vol_diagonal = params.residual_covariance.diagonal();
        let sharpe_ratios = expected_returns.component_div(&vol_diagonal.map(|v| v.sqrt()));
        
        sharpe_ratios.map(|r| r.tanh())
    }

    fn optimize_portfolio_weights(&self, risk_adjusted_returns: &DVector<f64>, params: &VARParameters) -> DVector<f64> {
        let cov_inv = self.robust_matrix_inverse(params.residual_covariance.clone()).unwrap_or_else(|_| {
            DMatrix::identity(self.n_assets, self.n_assets)
        });
        
        let raw_weights = &cov_inv * risk_adjusted_returns;
        let sum_weights = raw_weights.iter().map(|w| w.abs()).sum::<f64>();
        
        if sum_weights > 1e-6 {
            raw_weights.map(|w| w / sum_weights)
        } else {
            DVector::zeros(self.n_assets)
        }
    }

    fn calculate_var_risk(&self, position_sizes: &DVector<f64>, params: &VARParameters) -> f64 {
        let portfolio_variance = position_sizes.transpose() * &params.residual_covariance * position_sizes;
        let portfolio_std = portfolio_variance[(0, 0)].sqrt();
        
        let var_95 = portfolio_std * 1.645;
        let cvar_95 = portfolio_std * 2.063;
        
        (var_95 + cvar_95) / 2.0
    }

    fn calculate_execution_priority(&self, arbitrage_signal: f64, risk_metric: f64) -> f64 {
        let signal_strength = arbitrage_signal.abs();
        let risk_penalty = (-risk_metric / RISK_TOLERANCE).exp();
        let confidence = self.get_model_confidence();
        
        let raw_priority = signal_strength * risk_penalty * confidence;
        (raw_priority * 100.0).tanh()
    }

    fn get_model_confidence(&self) -> f64 {
        let params = self.var_params.read().unwrap();
        match params.as_ref() {
            Some(p) => {
                let model_age = p.last_update.elapsed().as_secs_f64();
                let age_penalty = (-model_age / 300.0).exp();
                let aic_confidence = (-p.aic_score / 1000.0).exp();
                
                (age_penalty + aic_confidence) / 2.0
            }
            None => 0.0,
        }
    }

    pub fn get_latest_prediction(&self) -> Option<VARPrediction> {
        self.last_prediction.read().unwrap().clone()
    }

    pub fn should_execute(&self, min_signal: f64, max_risk: f64) -> bool {
        if let Some(pred) = self.get_latest_prediction() {
            pred.arbitrage_signal.abs() >= min_signal && 
            pred.risk_metric <= max_risk &&
            pred.execution_priority >= CONFIDENCE_THRESHOLD
        } else {
            false
        }
    }

    pub fn get_asset_pubkeys(&self) -> &[Pubkey] {
        &self.asset_pubkeys
    }

    pub fn model_diagnostics(&self) -> Option<ModelDiagnostics> {
        let params = self.var_params.read().unwrap();
        params.as_ref().map(|p| {
            ModelDiagnostics {
                lag_order: p.lag_order,
                aic_score: p.aic_score,
                condition_number: self.calculate_condition_number(&p.residual_covariance),
                model_age: p.last_update.elapsed().as_secs_f64(),
                observation_count: self.observations.read().unwrap().len(),
            }
        })
    }

    fn calculate_condition_number(&self, matrix: &DMatrix<f64>) -> f64 {
        let svd = matrix.svd(false, false);
        let max_singular = svd.singular_values.iter().fold(0.0, |a, &b| a.max(b.abs()));
        let min_singular = svd.singular_values.iter().fold(f64::INFINITY, |a, &b| a.min(b.abs()));
        
        if min_singular > 0.0 {
            max_singular / min_singular
        } else {
            f64::INFINITY
        }
    }
}

#[derive(Debug, Clone)]
pub struct ModelDiagnostics {
    pub lag_order: usize,
    pub aic_score: f64,
    pub condition_number: f64,
    pub model_age: f64,
    pub observation_count: usize,
}

impl VARPrediction {
    pub fn get_best_opportunity(&self) -> Option<(usize, f64)> {
        self.position_sizes.iter()
            .enumerate()
            .max_by(|(_, a), (_, b)| a.abs().partial_cmp(&b.abs()).unwrap())
            .map(|(idx, &size)| (idx, size))
    }

    pub fn should_long(&self, asset_idx: usize) -> bool {
        asset_idx < self.position_sizes.len() && self.position_sizes[asset_idx] > 0.01
    }

    pub fn should_short(&self, asset_idx: usize) -> bool {
        asset_idx < self.position_sizes.len() && self.position_sizes[asset_idx] < -0.01
    }
}

