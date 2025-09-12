use std::collections::VecDeque;
use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::RwLock;
use tokio::sync::Mutex as AsyncMutex;

use serde::{Deserialize, Serialize};
use thiserror::Error;
use core::f64::consts::LN_2PI;

// === Γ1: Constants and numerics ===
const MIN_OBSERVATIONS: usize = 100;
const MAX_OBSERVATIONS: usize = 1000;
const MAX_ITERATIONS: usize = 500;
const CONVERGENCE_TOLERANCE: f64 = 1e-8;

// Hard numeric floors/ceilings to keep the model in a stable basin
const OMEGA_FLOOR: f64 = 1e-12;
const OMEGA_CEIL: f64 = 1e2;
const ALPHA_BETA_MAX_SUM: f64 = 0.999; // strict stationarity guard

const MIN_VARIANCE: f64 = 1e-12;
const MAX_VARIANCE: f64 = 100.0;

const FORECAST_HORIZON: usize = 20;
const UPDATE_INTERVAL_MS: u64 = 100;

// Microstructure/outlier guard for log returns
const MAX_ABS_LOG_RETURN: f64 = 0.20; // ~20% in one tick is noise/toxic for per-tick fitting

#[inline(always)]
fn phi_standard_normal(z: f64) -> f64 {
    const INV_SQRT_2PI: f64 = 0.3989422804014327;
    (-0.5 * z * z).exp() * INV_SQRT_2PI
}

// === Γ2: Types, errors, params, scratch ===
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
    params: [f64; 3],     // [omega, alpha, beta]
    gradient: [f64; 3],
    hessian: [[f64; 3]; 3],
    step_size: f64,
    iteration: usize,
}

#[derive(Default, Debug)]
struct Scratch {
    variances: Vec<f64>,
    d_omega: Vec<f64>,
    d_alpha: Vec<f64>,
    d_beta: Vec<f64>,
    r2: Vec<f64>,
    weights: Vec<f64>,
    work: Vec<f64>,
}

pub struct GarchVolatilityForecaster {
    // Prices/returns
    price_returns: Arc<RwLock<VecDeque<f64>>>,
    timestamps: Arc<RwLock<VecDeque<u64>>>,
    last_price: Arc<RwLock<Option<f64>>>,

    current_params: Arc<RwLock<Option<GarchParameters>>>,
    conditional_variances: Arc<RwLock<Vec<f64>>>,

    last_update: Arc<RwLock<Instant>>,
    update_lock: Arc<AsyncMutex<()>>,

    // Pre-allocated scratch
    scratch: Arc<RwLock<Scratch>>,

    // Config + variance targeting anchors
    config: Arc<RwLock<GarchConfig>>,
    ewma_var: Arc<RwLock<f64>>,  // target σ²
    ewma_beta: Arc<RwLock<f64>>, // smoothing for ewma_var
}

// Ω1 — Config switches
#[derive(Debug, Clone)]
pub struct GarchConfig {
    pub forgetting_lambda: f64,
    pub use_student_t: bool,
    pub nu_df: f64,
    pub use_bhhh: bool,
    pub variance_targeting: bool,
    pub update_interval_ms: u64,
    pub armijo_c1: f64,
    pub armijo_max_bt: usize,
    pub mad_clip: f64,
}

impl Default for GarchConfig {
    fn default() -> Self {
        Self {
            forgetting_lambda: 0.997,
            use_student_t: true,
            nu_df: 7.0,
            use_bhhh: true,
            variance_targeting: true,
            update_interval_ms: UPDATE_INTERVAL_MS,
            armijo_c1: 1e-8,
            armijo_max_bt: 12,
            mad_clip: 6.0,
        }
    }
}

// === Γ3: Constructor and ingestion ===
impl GarchVolatilityForecaster {
    pub fn new() -> Self {
        Self::with_config(GarchConfig::default())
    }

    pub fn with_config(cfg: GarchConfig) -> Self {
        let mut s = Scratch::default();
        s.variances.reserve_exact(MAX_OBSERVATIONS);
        s.d_omega.reserve_exact(MAX_OBSERVATIONS);
        s.d_alpha.reserve_exact(MAX_OBSERVATIONS);
        s.d_beta.reserve_exact(MAX_OBSERVATIONS);
        s.r2.reserve_exact(MAX_OBSERVATIONS);
        s.weights.reserve_exact(MAX_OBSERVATIONS);
        s.work.reserve_exact(MAX_OBSERVATIONS);

        Self {
            price_returns: Arc::new(RwLock::new(VecDeque::with_capacity(MAX_OBSERVATIONS))),
            timestamps: Arc::new(RwLock::new(VecDeque::with_capacity(MAX_OBSERVATIONS))),
            last_price: Arc::new(RwLock::new(None)),

            current_params: Arc::new(RwLock::new(None)),
            conditional_variances: Arc::new(RwLock::new(Vec::with_capacity(MAX_OBSERVATIONS))),

            last_update: Arc::new(RwLock::new(Instant::now())),
            update_lock: Arc::new(AsyncMutex::new(())),
            scratch: Arc::new(RwLock::new(s)),

            config: Arc::new(RwLock::new(cfg)),
            ewma_var: Arc::new(RwLock::new(1e-6)),
            ewma_beta: Arc::new(RwLock::new(0.98)),
        }
    }

    pub fn set_config(&self, cfg: GarchConfig) { *self.config.write() = cfg; }
        }
    }

    #[inline(always)]
    fn robust_clip_mad(&self, xr: &mut [f64], zmax: f64) {
        if xr.is_empty() { return; }
        let n = xr.len();
        let mut sc = self.scratch.write();
        sc.work.clear(); sc.work.extend_from_slice(xr);
        sc.work.sort_by(|a,b| a.partial_cmp(b).unwrap_or(core::cmp::Ordering::Equal));
        let med = sc.work[n/2];
        for i in 0..n { sc.work[i] = (xr[i] - med).abs(); }
        sc.work.sort_by(|a,b| a.partial_cmp(b).unwrap_or(core::cmp::Ordering::Equal));
        let mad = sc.work[n/2].max(1e-12);
        let scale = 1.4826 * mad;
        for v in xr.iter_mut() { let z = (*v - med) / scale; if z.abs() > zmax { *v = med + zmax * scale * z.signum(); } }
    }

    /// Ingest a midprice/trade price and timestamp; robust + EWMA update
    pub async fn add_price_observation(&self, price: f64, timestamp: u64) -> Result<(), GarchError> {
        if !price.is_finite() || price <= 0.0 { return Ok(()); }
        let _guard = self.update_lock.lock().await;
        let cfg_lambda_update = UPDATE_INTERVAL_MS; // keep default cadence; advanced cfg wired below

        {
            let mut ts = self.timestamps.write();
            if let Some(&last_ts) = ts.back() { if timestamp <= last_ts { return Ok(()); } }

            let mut last_p = self.last_price.write();
            let mut rets = self.price_returns.write();

            if let Some(prev) = *last_p {
                let mut lr = (price / prev).ln();
                if lr.abs() > MAX_ABS_LOG_RETURN { lr = lr.signum() * MAX_ABS_LOG_RETURN; }
                rets.push_back(lr);
                ts.push_back(timestamp);
                if rets.len() > MAX_OBSERVATIONS { rets.pop_front(); ts.pop_front(); }
                // EWMA variance anchor update for variance targeting
                let r2 = lr * lr;
                let beta = *self.ewma_beta.read();
                let mut v = self.ewma_var.write();
                *v = beta * *v + (1.0 - beta) * r2.max(1e-12);
            } else {
                ts.push_back(timestamp);
            }
            *last_p = Some(price);
        }

        // MAD-robustify tail if enough points
        if self.price_returns.read().len() >= MIN_OBSERVATIONS {
            let len = self.price_returns.read().len();
            let tail = len.min(256);
            let mut buf = Vec::with_capacity(tail);
            {
                let rets = self.price_returns.read();
                buf.extend(rets.iter().rev().take(tail).rev().copied());
            }
            self.robust_clip_mad(&mut buf, 6.0);
            {
                let mut rets = self.price_returns.write();
                let start = len - tail;
                for (i, v) in buf.into_iter().enumerate() { rets[start + i] = v; }
            }
        }

        let ready = { self.price_returns.read().len() >= MIN_OBSERVATIONS };
        if ready {
            let elapsed = self.last_update.read().elapsed();
            if elapsed >= Duration::from_millis(cfg_lambda_update) {
                self.estimate_parameters_internal()?;
                *self.last_update.write() = Instant::now();
            }
        }
        Ok(())
    }

    // === Δ4: Optimizer (projected Newton + Armijo, weighted LL) ===
    fn estimate_parameters_internal(&self) -> Result<(), GarchError> {
        let returns_vec: Vec<f64> = {
            let rets = self.price_returns.read();
            if rets.len() < MIN_OBSERVATIONS { return Err(GarchError::InsufficientData(rets.len(), MIN_OBSERVATIONS)); }
            rets.iter().copied().collect()
        };
        // ensure r2 cache exists
        {
            let mut sc = self.scratch.write();
            sc.r2.clear(); sc.r2.extend(returns_vec.iter().map(|&r| r*r));
        }
        let cfg = self.config.read().clone();
        let mut state = self.initialize_optimization(&returns_vec);
        let mut prev_ll = f64::NEG_INFINITY;
        let mut converged = false;
        for iter in 0..MAX_ITERATIONS {
            state.iteration = iter;
            self.compute_gradient_and_hessian(&mut state, &returns_vec)?;
            let det = self.matrix_determinant_3x3(&state.hessian);
            if det.abs() < 1e-16 { return Err(GarchError::NumericalInstability); }
            let mut inv_h = [[0.0; 3]; 3];
            self.matrix_inverse_3x3(&state.hessian, &mut inv_h, det);
            let mut direction = [0.0; 3];
            for i in 0..3 { let mut acc = 0.0; for j in 0..3 { acc -= inv_h[i][j] * state.gradient[j]; } direction[i] = acc; }
            let (omega, alpha, beta) = (state.params[0], state.params[1], state.params[2]);
            let base_ll = self.log_likelihood_weighted(omega, alpha, beta, &returns_vec, true)?;
            let gTd = self.dot3(&state.gradient, &direction);
            let mut step = state.step_size;
            let mut accepted = false;
            for _ in 0..cfg.armijo_max_bt {
                let mut cand_alpha = (alpha + step * direction[1]).max(0.0);
                let mut cand_beta  = (beta  + step * direction[2]).max(0.0);
                let sum = cand_alpha + cand_beta;
                if sum >= ALPHA_BETA_MAX_SUM { let scale = ALPHA_BETA_MAX_SUM / (sum + 1e-15); cand_alpha *= scale; cand_beta *= scale; }
                let cand_omega = if cfg.variance_targeting {
                    let target = *self.ewma_var.read();
                    ((1.0 - cand_alpha - cand_beta) * target).clamp(OMEGA_FLOOR, OMEGA_CEIL)
                } else { (omega + step * direction[0]).clamp(OMEGA_FLOOR, OMEGA_CEIL) };
                let cand_ll = self.log_likelihood_weighted(cand_omega, cand_alpha, cand_beta, &returns_vec, true)?;
                if cand_ll.is_finite() && cand_ll > base_ll + cfg.armijo_c1 * step * gTd { state.params = [cand_omega, cand_alpha, cand_beta]; accepted = true; break; }
                step *= 0.5;
            }
            if !accepted { return Err(GarchError::NumericalInstability); }
            let ll_now = self.log_likelihood_weighted(state.params[0], state.params[1], state.params[2], &returns_vec, true)?;
            let pchg = (direction[0]*step).hypot(direction[1]*step).hypot(direction[2]*step);
            if pchg < CONVERGENCE_TOLERANCE && (ll_now - prev_ll).abs() < 1e-7 { converged = true; break; }
            prev_ll = ll_now;
            if iter > 10 { state.step_size = (state.step_size * 0.95).max(0.02); }
        }
        if !converged { return Err(GarchError::ConvergenceFailed(MAX_ITERATIONS)); }
        let params = self.finalize_parameters(&state, &returns_vec)?; *self.current_params.write() = Some(params); Ok(())
    }

    fn initialize_optimization(&self, returns: &[f64]) -> OptimizationState {
        let variance = returns.iter().map(|&r| r * r).sum::<f64>() / (returns.len() as f64);
        // conservative, strictly stationary
        let mut alpha = 0.07; let mut beta = 0.90;
        if alpha + beta >= ALPHA_BETA_MAX_SUM { let s = ALPHA_BETA_MAX_SUM / (alpha + beta); alpha *= s; beta *= s; }
        let omega = (variance.max(MIN_VARIANCE) * (1.0 - alpha - beta)).clamp(OMEGA_FLOOR, OMEGA_CEIL);
        OptimizationState { params: [omega, alpha, beta], gradient: [0.0; 3], hessian: [[0.0; 3]; 3], step_size: 0.2, iteration: 0 }
    }

    #[inline(always)]
    fn validate_parameters(&self, omega: f64, alpha: f64, beta: f64) -> bool {
        omega.is_finite() && alpha.is_finite() && beta.is_finite() &&
        omega >= OMEGA_FLOOR && alpha >= 0.0 && beta >= 0.0 &&
        (alpha + beta) < ALPHA_BETA_MAX_SUM
    }

    #[inline(always)]
    fn prep_weights(&self, n: usize, lambda: f64, sc: &mut Scratch) {
        sc.weights.clear(); sc.weights.resize(n, 1.0);
        if (lambda - 1.0).abs() < 1e-12 { return; }
        let mut w = 1.0;
        for t in (0..n).rev() { sc.weights[t] = w; w *= lambda; }
        let sum: f64 = sc.weights.iter().sum(); if sum > 0.0 { let inv = 1.0 / sum; for v in sc.weights.iter_mut() { *v *= inv; } }
    }

    fn compute_gradient_and_hessian(&self, state: &mut OptimizationState, returns: &[f64]) -> Result<(), GarchError> {
        let n = returns.len();
        let (omega, alpha, beta) = (state.params[0], state.params[1], state.params[2]);
        if !self.validate_parameters(omega, alpha, beta) { return Err(GarchError::InvalidParameters { omega, alpha, beta }); }
        let cfg = self.config.read().clone();
        let mut sc = self.scratch.write();
        let Scratch { variances, d_omega, d_alpha, d_beta, r2, weights, .. } = &mut *sc;
        if r2.len() != n { r2.clear(); r2.extend(returns.iter().map(|&r| r*r)); }
        self.prep_weights(n, cfg.forgetting_lambda.clamp(0.95, 1.0), &mut sc);
        variances.clear(); d_omega.clear(); d_alpha.clear(); d_beta.clear();
        variances.resize(n, 0.0); d_omega.resize(n, 0.0); d_alpha.resize(n, 0.0); d_beta.resize(n, 0.0);
        let denom = (1.0 - alpha - beta).max(1e-6);
        let uncond = (omega / denom).clamp(MIN_VARIANCE, MAX_VARIANCE); variances[0] = uncond;
        d_omega[0] = 1.0 / denom; let denom2 = (denom*denom).max(1e-12); d_alpha[0] = omega/denom2; d_beta[0]=omega/denom2;
        for t in 1..n { let prev=variances[t-1]; let ht=(omega + alpha*r2[t-1] + beta*prev).clamp(MIN_VARIANCE, MAX_VARIANCE); variances[t]=ht; d_omega[t]=1.0 + beta*d_omega[t-1]; d_alpha[t]=r2[t-1] + beta*d_alpha[t-1]; d_beta[t]=prev + beta*d_beta[t-1]; }
        state.gradient=[0.0;3]; state.hessian=[[0.0;3];3];
        let use_t = cfg.use_student_t; let nu = cfg.nu_df.max(3.0);
        for t in 0..n {
            let ht=variances[t]; let wt=weights[t]; let r2_over_h=r2[t]/ht;
            let g_ht = if use_t { let a=-0.5/ht; let b=(nu+1.0)*0.5*(r2[t]/(nu*ht*ht)) / (1.0 + r2_over_h/nu); a + b } else { (r2_over_h - 1.0) / (2.0*ht) };
            let d=[d_omega[t], d_alpha[t], d_beta[t]]; for i in 0..3 { state.gradient[i] += wt*g_ht*d[i]; }
            if cfg.use_bhhh { for i in 0..3 { for j in i..3 { state.hessian[i][j] -= wt*(g_ht*d[i])*(g_ht*d[j]); } } }
            else { let inv=1.0/ht; let x=r2_over_h; let inv2=inv*inv*(if use_t { 1.0 - 2.0*x/(1.0 + x/nu) } else { 1.0 - 2.0*x }); for i in 0..3 { for j in i..3 { state.hessian[i][j] += wt*d[i]*d[j]*inv2 * (-0.5); } } }
        }
        state.hessian[1][0]=state.hessian[0][1]; state.hessian[2][0]=state.hessian[0][2]; state.hessian[2][1]=state.hessian[1][2];
        *self.conditional_variances.write() = sc.variances.clone();
        Ok(())
    }

    #[inline(always)]
    fn dot3(&self, a: &[f64;3], b: &[f64;3]) -> f64 { a[0]*b[0] + a[1]*b[1] + a[2]*b[2] }

    // Weighted log-likelihood (Gaussian or Student-t)
    fn log_likelihood_weighted(&self, omega: f64, alpha: f64, beta: f64, returns: &[f64], weighted: bool) -> Result<f64, GarchError> {
        if !self.validate_parameters(omega, alpha, beta) { return Err(GarchError::InvalidParameters { omega, alpha, beta }); }
        let cfg = self.config.read().clone(); let n=returns.len(); let mut sc=self.scratch.write();
        if sc.r2.len()!=n { sc.r2.clear(); sc.r2.extend(returns.iter().map(|&r| r*r)); }
        self.prep_weights(n, if weighted { cfg.forgetting_lambda } else { 1.0 }, &mut sc);
        let mut v=(omega/(1.0 - alpha - beta).max(1e-6)).clamp(MIN_VARIANCE, MAX_VARIANCE); let mut ll=0.0; let use_t=cfg.use_student_t; let nu=cfg.nu_df.max(3.0);
        let mut add = |wt: f64, r2: f64, v: f64| { if use_t { -0.5*v.ln() - (nu+1.0)*0.5*((1.0 + r2/(nu*v)).ln()) } else { -0.5*((r2/v) + v.ln() + LN_2PI) } * wt };
        ll += add(sc.weights[0], sc.r2[0], v);
        for t in 1..n { v=(omega + alpha*sc.r2[t-1] + beta*v).clamp(MIN_VARIANCE, MAX_VARIANCE); ll += add(sc.weights[t], sc.r2[t], v); }
        Ok(ll)
    }

    fn finalize_parameters(&self, state: &OptimizationState, returns: &[f64]) -> Result<GarchParameters, GarchError> {
        let (omega, alpha, beta) = (state.params[0], state.params[1], state.params[2]);
        let n = returns.len() as f64;
        let variances = self.conditional_variances.read();
        let mut log_likelihood = 0.0;
        for (t, &ret) in returns.iter().enumerate() {
            let var = variances[t].clamp(MIN_VARIANCE, MAX_VARIANCE);
            log_likelihood -= 0.5 * ((ret * ret) / var + var.ln() + std::f64::consts::LN_2PI);
        }
        let k = 3.0;
        let aic = -2.0 * log_likelihood + 2.0 * k;
        let bic = -2.0 * log_likelihood + k * n.ln();
        let persistence = alpha + beta;
        let unconditional_variance = (omega / (1.0 - persistence).max(1e-6)).clamp(MIN_VARIANCE, MAX_VARIANCE);
        Ok(GarchParameters { omega, alpha, beta, log_likelihood, aic, bic, persistence, unconditional_variance })
    }

    // Linear algebra helpers
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
}

// === Γ5: Forecasting and risk metrics ===
impl GarchVolatilityForecaster {
    pub fn forecast_volatility(&self, horizon: usize) -> Result<VolatilityForecast, GarchError> {
        if horizon == 0 || horizon > FORECAST_HORIZON { return Err(GarchError::InvalidHorizon(horizon)); }
        let params = self.current_params.read().clone().ok_or(GarchError::InsufficientData(0, MIN_OBSERVATIONS))?;
        let returns = self.price_returns.read();
        let timestamps = self.timestamps.read();
        let variances = self.conditional_variances.read();
        if returns.is_empty() || variances.is_empty() { return Err(GarchError::InsufficientData(0, MIN_OBSERVATIONS)); }

        let last_return = *returns.back().unwrap_or(&0.0);
        let last_variance = *variances.last().unwrap_or(&params.unconditional_variance);
        let current_timestamp = *timestamps.back().unwrap_or(&0);

        let mut forecast_variances = Vec::with_capacity(horizon);
        let mut confidence_intervals = Vec::with_capacity(horizon);
        let mut h_t = last_variance;
        let mut cumulative_variance = 0.0;
        for t in 0..horizon {
            if t == 0 {
                h_t = (params.omega + params.alpha * last_return.powi(2) + params.beta * last_variance).clamp(MIN_VARIANCE, MAX_VARIANCE);
            } else {
                h_t = (params.omega + (params.alpha + params.beta) * h_t).clamp(MIN_VARIANCE, MAX_VARIANCE);
            }
            forecast_variances.push(h_t);
            cumulative_variance += h_t;
            let std_dev = cumulative_variance.sqrt();
            let z = 1.96;
            confidence_intervals.push((-z * std_dev, z * std_dev));
        }

        let annualized_vol = (forecast_variances[0] * 252.0).sqrt();
        let half_life = if params.persistence < ALPHA_BETA_MAX_SUM { -(0.5_f64.ln()) / params.persistence.ln() } else { 1_000.0 };
        let vix_equivalent = annualized_vol * 100.0;
        Ok(VolatilityForecast { timestamp: current_timestamp, horizon: forecast_variances, confidence_intervals, annualized_vol, half_life, vix_equivalent })
    }

    pub fn get_risk_metrics(&self) -> Option<RiskMetrics> {
        let params = self.current_params.read().as_ref()?.clone();
        let variances = self.conditional_variances.read();
        if variances.is_empty() { return None; }
        let current_variance = *variances.last().unwrap_or(&params.unconditional_variance);
        let sigma = current_variance.sqrt();

        // Exact Normal VaR and ES (CVaR)
        let z95 = 1.6448536269514722;
        let z99 = 2.3263478740408408;
        let var_95 = z95 * sigma; let var_99 = z99 * sigma;
        let cvar_95 = sigma * (phi_standard_normal(z95) / (1.0 - 0.95));
        let cvar_99 = sigma * (phi_standard_normal(z99) / (1.0 - 0.99));

        let volatility_ratio = (current_variance / params.unconditional_variance).max(0.0);
        let mean_reversion_speed = -(params.persistence.ln()).max(0.0);
        Some(RiskMetrics { current_volatility: sigma, var_95, var_99, cvar_95, cvar_99, volatility_ratio, mean_reversion_speed, persistence: params.persistence })
    }

    pub fn get_market_regime(&self) -> MarketRegime {
        let Some(r) = self.get_risk_metrics() else { return MarketRegime::Unknown; };
        match (r.current_volatility, r.volatility_ratio) {
            (v, vr) if v < 0.0010 && vr < 0.5 => MarketRegime::LowVolatility,
            (v, vr) if v < 0.0020 && vr < 1.5 => MarketRegime::Normal,
            (v, vr) if v < 0.0050 && vr < 3.0 => MarketRegime::Elevated,
            (v,    _) if v < 0.0100           => MarketRegime::HighVolatility,
            _                                   => MarketRegime::Extreme,
        }
    }

    pub fn get_volatility_percentile(&self, lookback_periods: usize) -> Option<f64> {
        let variances = self.conditional_variances.read();
        if variances.len() < lookback_periods { return None; }
        let current_var = *variances.last()?;
        let start = variances.len().saturating_sub(lookback_periods);
        let hist = &variances[start..];
        let rank = hist.iter().filter(|&&v| v <= current_var).count() as f64;
        Some(rank / (hist.len() as f64) * 100.0)
    }

    pub fn get_volatility_term_structure(&self) -> Option<Vec<(usize, f64)>> {
        let params = self.current_params.read().as_ref()?.clone();
        let variances = self.conditional_variances.read();
        if variances.is_empty() { return None; }
        let horizons = [1, 5, 10, 20, 50, 100];
        let mut out = Vec::with_capacity(horizons.len());
        let last_var = *variances.last().unwrap_or(&params.unconditional_variance);
        for &h in &horizons {
            let forecast_var = params.unconditional_variance + (last_var - params.unconditional_variance) * params.persistence.powi(h as i32);
            out.push((h, (forecast_var * 252.0).sqrt()));
        }
        Some(out)
    }

    // === Γ6: Adaptive position sizing ===
    pub async fn get_adaptive_position_size(&self, base_size: f64, max_leverage: f64) -> f64 {
        let Some(r) = self.get_risk_metrics() else { return base_size * 0.1; };
        let vol_pct = self.get_volatility_percentile(100).unwrap_or(50.0);
        let regime = self.get_market_regime();
        let regime_scalar = match regime {
            MarketRegime::LowVolatility => 1.6,
            MarketRegime::Normal        => 1.0,
            MarketRegime::Elevated      => 0.7,
            MarketRegime::HighVolatility=> 0.45,
            MarketRegime::Extreme       => 0.25,
            MarketRegime::Unknown       => 0.6,
        };
        let pct_scalar = if vol_pct > 90.0 { 0.55 } else if vol_pct > 75.0 { 0.75 } else if vol_pct < 25.0 { 1.25 } else { 1.0 };
        let target_daily_vol = 0.012; // 1.2%/day budget
        let vol_scalar = (target_daily_vol / r.current_volatility).clamp(0.10, 2.00);
        let n = self.price_returns.read().len() as f64; let sample_scalar = if n < 250.0 { (n / 250.0).clamp(0.25, 1.0) } else { 1.0 };
        let raw = base_size * regime_scalar * pct_scalar * vol_scalar * sample_scalar;
        raw.min(base_size * max_leverage)
    }
}

// === Γ7: Types + tests ===
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
        let f = GarchVolatilityForecaster::new();
        assert!(f.current_params.read().is_none());
    }

    #[tokio::test]
    async fn test_price_observation_sequence() {
        let f = GarchVolatilityForecaster::new();
        // Seed a monotonic price path
        for (i, p) in (1..=120).zip((1..=120).map(|k| 100.0 + k as f64 * 0.01)) {
            f.add_price_observation(p, i as u64).await.unwrap();
        }
        assert!(f.price_returns.read().len() >= MIN_OBSERVATIONS - 1);
        if let Some(params) = f.current_params.read().clone() {
            assert!(params.omega > 0.0);
        }
    }
}

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

      LEGACY BLOCK REMOVED
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

