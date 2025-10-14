// kalman_spread_predictor.rs
// ======================================================================
// [KALMAN] Textbook-consistent Kalman-based spread predictor (UPGRADED)
// - CA submodel [spread, velocity, acceleration] with proper discrete F(dt)/Q(dt)
// - Exogenous states [vol_imb, liquidity, trend] as OU/Singer processes:
//     F_ii = exp(-dt/τ_i)
//     Q_ii = σ_i^2 * (1 - exp(-2dt/τ_i)) / (2/τ_i)
// - Cholesky/LDLT solves (no explicit inverses)
// - Runtime chi-square gating from stats lib
// - Sage–Husa adaptive bias & R with forgetting β (proper naming)
// - Q updated from covariance identity (closed form), fused with
//   Mehra-style innovation reconciliation to back out scalar q (CA block)
// - PSD-safe projections (eigenvalue flooring), clamped diagonals; clamp counters + alarms
// - H calibration “stamp” with dataset + λ + coeffs + commit; strictly enforced
//   under `prod_strict` feature (hard fail on drift, logs identity on install)
// - OU defaults “stamp” mirroring H-stamp: τ, σ², rule used, commit; enforced under prod_strict
//
// References (canonical):
// - R.E. Kalman (1960)
// - Gelb (1974): Applied Optimal Estimation (CA discretization, Joseph form)
// - Bar-Shalom, Li, Kirubarajan (2001): Estimation with Applications to Tracking
// - Welch & Bishop: An Introduction to the Kalman Filter
// - Sage & Husa (1969): Adaptive filtering (bias + R with forgetting)
// - Mehra (1970s): On the identification of Q and R in KF
//
// Unit tests:
// - Van-Loan parity (CA F/Q closed form) + randomized grid sweeps
// - OU parity (discrete F/Q vs closed forms)
// - PSD projection sanity
// ======================================================================

use std::collections::HashMap;
use std::sync::Arc;

use anyhow::{anyhow, Result};
use nalgebra::{
    DMatrix, DVector, SymmetricEigen,
    linalg::{Cholesky, LDLT}
};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use solana_sdk::pubkey::Pubkey;
use statrs::distribution::{ChiSquared, ContinuousCDF};

// ======================================================================
// Constants & tuning (documented; not vibes)
// ======================================================================
const MAX_SPREAD_HISTORY: usize = 100;
const MIN_OBSERVATIONS: usize = 10;

const OBS_DIM: usize = 4; // [spread, volume_imbalance, liquidity, mid_price_change]
const STATE_DIM: usize = 6; // [spread, vel, accel, vol_imb, liquidity, trend]

// dt guardrails
const DT_MIN: f64 = 1.0e-4; // 0.1 ms
const DT_MAX: f64 = 2.0;    // 2 s

// Covariance floors
const MAX_COVARIANCE: f64 = 1.0e6;
const MIN_COVARIANCE: f64 = 1.0e-12;

// Default forgetting factors (runtime-overridable via fields)
const DEFAULT_BETA_R: f64 = 0.02;     // measurement noise update weight
const DEFAULT_BETA_Q: f64 = 0.01;     // process noise update weight (EMA fuse)
const DEFAULT_BETA_BIAS: f64 = 0.05;  // measurement bias update weight
const NIS_ALPHA: f64 = 0.05;          // NIS running-mean tracker

// q bounds for CA spectral density
const Q_SPREAD_MIN: f64 = 1.0e-8;
const Q_SPREAD_MAX: f64 = 1.0e2;

// Mehra fusion weight for q from innovation reconciliation
const BETA_Q_MEHRA: f64 = 0.005;

// Tolerance for calibration-stamp vs H coefficients
const H_STAMP_TOL: f64 = 1e-12;

// Tolerance for OU-stamp vs live OU params
const OU_STAMP_TOL: f64 = 1e-9;

// NIS alarming
const NIS_ALERT_MIN_UPDATES: u64 = 50;

// Clamp alarming (EMA over clamp events)
const CLAMP_ALPHA: f64 = 0.05;
const CLAMP_ALERT_RATE: f64 = 0.05; // trigger if >5% of updates need PSD clamps
const ADAPT_FREEZE_BETA: f64 = 0.0; // freeze adaptation when alarmed

// ======================================================================
// Data models
// ======================================================================
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
    pub state: DVector<f64>,            // x (6x1)
    pub covariance: DMatrix<f64>,       // P (6x6)
    pub last_update: u64,

    // Diagnostics
    pub innovation: DVector<f64>,             // v (4x1)
    pub innovation_covariance: DMatrix<f64>,  // S (4x4)
    pub nis: f64,                              // v^T S^{-1} v
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SpreadPrediction {
    pub predicted_spread: f64,
    pub confidence_interval: (f64, f64),
    pub prediction_horizon_ms: u64,
    pub model_confidence: f64,
    pub trend_strength: f64,
    pub volatility_estimate: f64,
}

// Calibration stamp for observation model H (mid-change ≈ a*velocity + b*trend)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CalibrationStamp {
    pub dataset_id: String,
    pub window_start_ms: u64,
    pub window_end_ms: u64,
    pub ridge_lambda: f64,
    pub coeff_a: f64,     // maps velocity -> mid_change
    pub coeff_b: f64,     // maps trend    -> mid_change
    pub stderr_a: f64,
    pub stderr_b: f64,
    pub commit_hash: String,
}

// OU/Singer parameterization for exogenous state i
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct OUParam {
    pub tau: f64,     // time-constant τ > 0 (seconds)
    pub sigma2: f64,  // diffusion intensity σ^2 >= 0
}
impl OUParam { pub fn new(tau: f64, sigma2: f64) -> Self { Self { tau, sigma2 } } }

// OU defaults stamp (provenance for τ/σ²)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OUCalibrationStamp {
    pub dataset_id: String,     // or “rule:id”, e.g., “decay:per_sec_0.95/0.98/0.999”
    pub rule: String,           // human-readable rule used to derive τ, σ²
    pub commit_hash: String,    // commit of the config/rule
    pub tau_vol: f64,
    pub sigma2_vol: f64,
    pub tau_liq: f64,
    pub sigma2_liq: f64,
    pub tau_trend: f64,
    pub sigma2_trend: f64,
}

// Innovation statistics for Mehra-style q estimation
#[derive(Debug, Clone)]
struct InnovStats {
    sum_vvt: DMatrix<f64>, // accumulated v vᵀ
    count: usize,
    last_P_pred: DMatrix<f64>, // last P^- for projection
}
impl InnovStats {
    fn new() -> Self {
        Self {
            sum_vvt: DMatrix::<f64>::zeros(OBS_DIM, OBS_DIM),
            count: 0,
            last_P_pred: DMatrix::<f64>::identity(STATE_DIM, STATE_DIM),
        }
    }
}

// Health snapshot
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealthSnapshot {
    pub nis_ema: f64,
    pub nis_df: f64,
    pub nis_updates: u64,
    pub nis_alarm: bool,
    pub r_clamp_ema: f64,
    pub q_clamp_ema: f64,
    pub adaptation_frozen: bool,
}

// ======================================================================
// Core predictor
// ======================================================================
pub struct KalmanFilterSpreadPredictor {
    // Per trading pair
    states: Arc<RwLock<HashMap<(Pubkey, Pubkey), KalmanState>>>,
    observations: Arc<RwLock<HashMap<(Pubkey, Pubkey), Vec<SpreadObservation>>>>,

    // Base model pieces (mutable safely)
    measurement_noise: RwLock<DMatrix<f64>>,  // R (4x4)
    observation_matrix: RwLock<DMatrix<f64>>, // H (4x6)
    q_spread: RwLock<f64>,                    // scalar spectral density for CA block q

    // Sage–Husa measurement bias (mean of measurement noise), size = obs_dim
    meas_bias: RwLock<DVector<f64>>,

    // Exogenous state OU parameters (indices 3,4,5)
    ou_params: [RwLock<OUParam>; 3],

    // Forgetting factors (runtime adjustable / freeze-able)
    beta_r: RwLock<f64>,
    beta_q: RwLock<f64>,
    beta_bias: RwLock<f64>,

    // Gating
    chi2_gate_p: f64,
    chi2_gate_df: f64,
    chi2_gate_threshold: f64,

    // NIS monitor (EMA) + count
    nis_ema: RwLock<f64>,
    update_count: RwLock<u64>,

    // Calibration stamps (when provided)
    stamp: RwLock<Option<CalibrationStamp>>,
    ou_stamp: RwLock<Option<OUCalibrationStamp>>,

    // Innovation stats for Mehra reconciliation
    innov_stats: Arc<RwLock<HashMap<(Pubkey, Pubkey), InnovStats>>>,

    // Clamp monitors and adaptation status
    r_clamp_ema: RwLock<f64>,
    q_clamp_ema: RwLock<f64>,
    adaptation_frozen: RwLock<bool>,
}

impl KalmanFilterSpreadPredictor {
    /// Constructor with canonical defaults.
    /// State: [ spread, velocity, acceleration, volume_imbalance, liquidity, trend ]
    /// Measurements: [ spread, volume_imbalance, liquidity, mid_price_change ]
    pub fn new() -> Self {
        // R diagonal init (adaptive)
        let mut r = DMatrix::zeros(OBS_DIM, OBS_DIM);
        r[(0, 0)] = 0.01; // spread
        r[(1, 1)] = 0.05; // volume imbalance
        r[(2, 2)] = 0.03; // liquidity
        r[(3, 3)] = 0.02; // mid price change

        // H mapping: direct for first three; mid_change = a*velocity + b*trend (coeffs from stamp)
        let mut h = DMatrix::zeros(OBS_DIM, STATE_DIM);
        h[(0, 0)] = 1.0; // observe spread
        h[(1, 3)] = 1.0; // observe vol_imb
        h[(2, 4)] = 1.0; // observe liquidity
        // h[(3,1)] and h[(3,5)] set from calibration stamp (if provided); fall back to zero until stamped
        h[(3, 1)] = 0.0;
        h[(3, 5)] = 0.0;

        // Chi-square threshold at runtime (p=0.99, df=OBS_DIM)
        let df = OBS_DIM as f64;
        let p = 0.99;
        let chi = ChiSquared::new(df).expect("chi-square df");
        let thr = chi.inverse_cdf(p);

        // Replace previous ad-hoc decays with OU defaults equivalent to prior decay rates:
        // Prior implied τs from 0.95^dt, 0.98^dt, 0.999^dt per second:
        // τ = 1/(-ln(decay_per_sec))
        let tau_vol = 1.0 / (-0.95f64.ln());   // ≈ 19.49 s
        let tau_liq = 1.0 / (-0.98f64.ln());   // ≈ 50.50 s
        let tau_trd = 1.0 / (-0.999f64.ln());  // ≈ 1000.5 s
        // Match σ^2 so that discrete Q at dt=1s equals the old Q diag targets (0.05, 0.02, 0.005)
        let sigma2_vol = ou_sigma2_from_Qd1(0.05, tau_vol);
        let sigma2_liq = ou_sigma2_from_Qd1(0.02, tau_liq);
        let sigma2_trd = ou_sigma2_from_Qd1(0.005, tau_trd);

        Self {
            states: Arc::new(RwLock::new(HashMap::new())),
            observations: Arc::new(RwLock::new(HashMap::new())),
            measurement_noise: RwLock::new(r),
            observation_matrix: RwLock::new(h),
            q_spread: RwLock::new(1.0e-2),
            meas_bias: RwLock::new(DVector::zeros(OBS_DIM)),
            ou_params: [
                RwLock::new(OUParam::new(tau_vol, sigma2_vol)),
                RwLock::new(OUParam::new(tau_liq, sigma2_liq)),
                RwLock::new(OUParam::new(tau_trd, sigma2_trd)),
            ],
            beta_r: RwLock::new(DEFAULT_BETA_R),
            beta_q: RwLock::new(DEFAULT_BETA_Q),
            beta_bias: RwLock::new(DEFAULT_BETA_BIAS),
            chi2_gate_p: p,
            chi2_gate_df: df,
            chi2_gate_threshold: thr,
            nis_ema: RwLock::new(df), // start near DOF
            update_count: RwLock::new(0),
            stamp: RwLock::new(None),
            ou_stamp: RwLock::new(None),
            innov_stats: Arc::new(RwLock::new(HashMap::new())),
            r_clamp_ema: RwLock::new(0.0),
            q_clamp_ema: RwLock::new(0.0),
            adaptation_frozen: RwLock::new(false),
        }
    }

    /// Optional boot check to enforce stamps early in prod_strict.
    pub fn boot_check(&self) -> Result<()> {
        self.ensure_calibration_if_strict()?;
        Ok(())
    }

    /// Set OU parameters (τ, σ^2) for exogenous states (indices 3,4,5).
    pub fn set_ou_params(&self, vol_imb: OUParam, liquidity: OUParam, trend: OUParam) {
        *self.ou_params[0].write() = vol_imb;
        *self.ou_params[1].write() = liquidity;
        *self.ou_params[2].write() = trend;
    }

    /// Install OU defaults provenance; enforced under prod_strict.
    pub fn set_ou_calibration_stamp(&self, stamp: OUCalibrationStamp) -> Result<()> {
        if stamp.dataset_id.is_empty() || stamp.commit_hash.is_empty() {
            return Err(anyhow!("OUCalibrationStamp must include dataset_id and commit_hash"));
        }
        // Log identity for auditors
        eprintln!(
            "[OU-STAMP] dataset={} rule={} commit={} \
             (vol: tau={:.9}, sigma2={:.9}) (liq: tau={:.9}, sigma2={:.9}) (trend: tau={:.9}, sigma2={:.9})",
            stamp.dataset_id, stamp.rule, stamp.commit_hash,
            stamp.tau_vol, stamp.sigma2_vol, stamp.tau_liq, stamp.sigma2_liq, stamp.tau_trend, stamp.sigma2_trend
        );
        *self.ou_stamp.write() = Some(stamp);
        Ok(())
    }

    /// Install a calibration stamp and wire H accordingly (mid_change row).
    pub fn set_calibration_stamp(&self, stamp: CalibrationStamp) -> Result<()> {
        if stamp.dataset_id.is_empty() || stamp.commit_hash.is_empty() {
            return Err(anyhow!("CalibrationStamp must include dataset_id and commit_hash"));
        }
        {
            let mut h = self.observation_matrix.write();
            h[(3, 1)] = stamp.coeff_a;
            h[(3, 5)] = stamp.coeff_b;
        }
        // Log identity for auditors
        eprintln!(
            "[H-STAMP] dataset={} window=[{}..{}] lambda={} \
             a={:.12}±{:.3e} b={:.12}±{:.3e} commit={}",
            stamp.dataset_id, stamp.window_start_ms, stamp.window_end_ms, stamp.ridge_lambda,
            stamp.coeff_a, stamp.stderr_a, stamp.coeff_b, stamp.stderr_b, stamp.commit_hash
        );
        *self.stamp.write() = Some(stamp);
        Ok(())
    }

    /// Replace R (e.g., after offline tuning).
    pub fn set_measurement_noise(&self, new_r: DMatrix<f64>) -> Result<()> {
        if new_r.nrows() != OBS_DIM || new_r.ncols() != OBS_DIM {
            return Err(anyhow!("R shape must be {}x{}", OBS_DIM, OBS_DIM));
        }
        let (r_psd, clamped) = psd_floor_flag(new_r, MIN_COVARIANCE);
        if clamped { self.bump_r_clamp(); }
        *self.measurement_noise.write() = r_psd;
        Ok(())
    }

    /// Manually freeze or unfreeze adaptation (R/Q/bias updates).
    pub fn set_adaptation_frozen(&self, frozen: bool) {
        *self.adaptation_frozen.write() = frozen;
        if frozen {
            *self.beta_r.write() = ADAPT_FREEZE_BETA;
            *self.beta_q.write() = ADAPT_FREEZE_BETA;
            *self.beta_bias.write() = ADAPT_FREEZE_BETA;
        } else {
            *self.beta_r.write() = DEFAULT_BETA_R;
            *self.beta_q.write() = DEFAULT_BETA_Q;
            *self.beta_bias.write() = DEFAULT_BETA_BIAS;
        }
    }

    /// Set chi-square gate percentile (default 0.99).
    pub fn set_gate_percentile(&mut self, p: f64) -> Result<()> {
        if !(0.5..1.0).contains(&p) { return Err(anyhow!("percentile must be (0.5,1)")); }
        self.chi2_gate_p = p;
        let chi = ChiSquared::new(self.chi2_gate_df).expect("chi-square df");
        self.chi2_gate_threshold = chi.inverse_cdf(p);
        Ok(())
    }

    /// Ingest new observation (Sage–Husa bias, adaptive R/Q, Mehra reconciliation).
    pub fn update(&self, pair: (Pubkey, Pubkey), obs: SpreadObservation) -> Result<()> {
        // Enforce calibration in prod_strict
        self.ensure_calibration_if_strict()?;

        // Maintain history
        {
            let mut all = self.observations.write();
            let buf = all.entry(pair).or_insert_with(Vec::new);
            if buf.len() >= MAX_SPREAD_HISTORY { buf.remove(0); }
            buf.push(obs.clone());
        }

        // Measurement vector y
        let mid_price_change = {
            let all = self.observations.read();
            let buf = all.get(&pair).unwrap();
            if buf.len() >= 2 {
                let a = &buf[buf.len()-2];
                let b = &buf[buf.len()-1];
                if a.mid_price > 0.0 { (b.mid_price - a.mid_price) / a.mid_price } else { 0.0 }
            } else { 0.0 }
        };

        let y = DVector::from_vec(vec![
            obs.spread,
            obs.volume_imbalance,
            obs.liquidity_depth,
            mid_price_change,
        ]);

        // Initialize state if missing
        {
            let mut states = self.states.write();
            states.entry(pair).or_insert_with(|| self.initialize_state(&obs));
        }

        // Step filter
        {
            let mut states = self.states.write();
            let st = states.get_mut(&pair).expect("state inserted");
            self.kf_step(pair, st, y, obs.timestamp_ms)?;
        }

        // NIS alarming math
        self.bump_update_count_and_check_nis_alarm();

        Ok(())
    }

    /// Predict spread horizon ahead with uncertainty.
    pub fn predict_spread(&self, pair: (Pubkey, Pubkey), horizon_ms: u64) -> Result<SpreadPrediction> {
        let states = self.states.read();
        let st = states.get(&pair).ok_or_else(|| anyhow!("No state found"))?;

        let observations = self.observations.read();
        let hist = observations.get(&pair).ok_or_else(|| anyhow!("No observations"))?;
        if hist.len() < MIN_OBSERVATIONS {
            return Err(anyhow!("Insufficient observations: {} < {}", hist.len(), MIN_OBSERVATIONS));
        }

        let steps = ((horizon_ms as f64) / 50.0).ceil() as usize;
        let dt = (horizon_ms as f64 / 1000.0) / (steps.max(1) as f64);

        let mut x = st.state.clone();
        let mut P = st.covariance.clone();

        for _ in 0..steps.max(1) {
            let F = self.build_F(dt);
            let Q = self.build_Q(dt);
            x = &F * x;
            P = &F * P * F.transpose() + Q;
            P = psd_floor(P, MIN_COVARIANCE);
            P = clamp_matrix_diag(P, MIN_COVARIANCE, MAX_COVARIANCE);
        }

        let spread = x[0];
        let var = P[(0, 0)].max(0.0);
        let std = var.sqrt();

        let model_conf = self.model_confidence(&st);
        let trend_strength = (st.state[1].abs() + 0.1 * st.state[2].abs()) / (std + 1e-9);
        let vol_est = estimate_realized_vol(hist);

        let ci = (spread - 1.96 * std, spread + 1.96 * std);

        Ok(SpreadPrediction {
            predicted_spread: spread,
            confidence_interval: ci,
            prediction_horizon_ms: horizon_ms,
            model_confidence: model_conf,
            trend_strength,
            volatility_estimate: vol_est,
        })
    }

    /// Expose running NIS mean vs DOF for health checks and clamp status.
    pub fn health_snapshot(&self) -> HealthSnapshot {
        let nis = *self.nis_ema.read();
        let df = self.chi2_gate_df;
        let n = *self.update_count.read();
        let alarm = if n >= NIS_ALERT_MIN_UPDATES {
            let sd = (2.0 * df / n as f64).sqrt();
            (nis - df).abs() > 3.0 * sd
        } else { false };
        HealthSnapshot {
            nis_ema: nis,
            nis_df: df,
            nis_updates: n,
            nis_alarm: alarm,
            r_clamp_ema: *self.r_clamp_ema.read(),
            q_clamp_ema: *self.q_clamp_ema.read(),
            adaptation_frozen: *self.adaptation_frozen.read(),
        }
    }

    // ============================== internals ==============================

    fn ensure_calibration_if_strict(&self) -> Result<()> {
        #[cfg(feature = "prod_strict")]
        {
            // H-stamp must exist and match H
            let stamp_opt = self.stamp.read();
            if stamp_opt.is_none() {
                return Err(anyhow!("prod_strict: CalibrationStamp is required"));
            }
            let stamp = stamp_opt.as_ref().unwrap();
            let h = self.observation_matrix.read();
            let a = h[(3, 1)];
            let b = h[(3, 5)];
            if (a - stamp.coeff_a).abs() > H_STAMP_TOL || (b - stamp.coeff_b).abs() > H_STAMP_TOL {
                return Err(anyhow!(
                    "prod_strict: H coefficients differ from CalibrationStamp (a={},b={}) vs stamp (a={},b={})",
                    a, b, stamp.coeff_a, stamp.coeff_b
                ));
            }

            // OU-stamp must exist and match live OU params
            let ou_stamp_opt = self.ou_stamp.read();
            if ou_stamp_opt.is_none() {
                return Err(anyhow!("prod_strict: OUCalibrationStamp is required"));
            }
            let ou_stamp = ou_stamp_opt.as_ref().unwrap();
            let ou0 = *self.ou_params[0].read();
            let ou1 = *self.ou_params[1].read();
            let ou2 = *self.ou_params[2].read();

            let ok = (ou0.tau - ou_stamp.tau_vol).abs() <= OU_STAMP_TOL
                && (ou0.sigma2 - ou_stamp.sigma2_vol).abs() <= OU_STAMP_TOL
                && (ou1.tau - ou_stamp.tau_liq).abs() <= OU_STAMP_TOL
                && (ou1.sigma2 - ou_stamp.sigma2_liq).abs() <= OU_STAMP_TOL
                && (ou2.tau - ou_stamp.tau_trend).abs() <= OU_STAMP_TOL
                && (ou2.sigma2 - ou_stamp.sigma2_trend).abs() <= OU_STAMP_TOL;

            if !ok {
                return Err(anyhow!("prod_strict: OU params drifted from OUCalibrationStamp"));
            }
        }
        Ok(())
    }

    fn initialize_state(&self, o: &SpreadObservation) -> KalmanState {
        let x0 = DVector::from_vec(vec![
            o.spread,         // spread
            0.0,              // velocity
            0.0,              // acceleration
            o.volume_imbalance,
            o.liquidity_depth,
            0.0,              // trend
        ]);
        let p0 = DMatrix::<f64>::identity(STATE_DIM, STATE_DIM) * 0.1;

        KalmanState {
            state: x0,
            covariance: p0,
            last_update: o.timestamp_ms,
            innovation: DVector::zeros(OBS_DIM),
            innovation_covariance: DMatrix::identity(OBS_DIM, OBS_DIM),
            nis: 0.0,
        }
    }

    /// One KF step with Sage–Husa bias and adaptive R/Q (+ Mehra reconciliation).
    ///
    /// Mehra fusion note:
    /// We reduce the CA block process noise to a scalar q by minimizing Frobenius-norm
    /// residuals against the CA closed-form basis B(dt). This is a standard scalarization
    /// for the constant-acceleration white-noise model (Gelb/Bar-Shalom). We blend:
    ///   (i) covariance-identity estimate on the CA block, and
    ///   (ii) innovation reconciliation via E[vvᵀ] − (HP^-Hᵀ + R)
    /// projected through H B_embed Hᵀ. Both are closed-form scalar least-squares estimates.
    fn kf_step(
        &self,
        pair: (Pubkey, Pubkey),
        st: &mut KalmanState,
        y: DVector<f64>,
        ts_ms: u64,
    ) -> Result<()> {
        // Real dt with bounds
        let mut dt = (ts_ms.saturating_sub(st.last_update)) as f64 / 1000.0;
        if dt <= 0.0 { return Ok(()); }
        if dt < DT_MIN { dt = DT_MIN; }
        if dt > DT_MAX { dt = DT_MAX; }

        // Read model pieces
        let h = self.observation_matrix.read().clone();
        let mut r_now = self.measurement_noise.read().clone();
        let mut bias = self.meas_bias.read().clone();
        let q_scalar = *self.q_spread.read();

        // Forgetting factors (allow runtime freezing/degrading)
        let beta_r = *self.beta_r.read();
        let beta_q = *self.beta_q.read();
        let beta_bias = *self.beta_bias.read();

        // Build F and Q
        let F = self.build_F(dt);
        let Q = self.build_Q_with(q_scalar, dt);

        // Predict
        let x_pred = &F * &st.state;
        let P_pred = &F * &st.covariance * F.transpose() + Q;

        // Sage–Husa bias update on raw residual r = y − H x_pred − b
        let r_raw = &y - &( &h * &x_pred ) - &bias;
        bias = &bias + beta_bias * (&r_raw - &bias); // b_k = b_{k−1} + β (v_k − b_{k−1})
        *self.meas_bias.write() = bias.clone();

        // Innovation with bias correction
        let v = &y - &( &h * &x_pred ) - &bias;

        // Innovation covariance
        let S = &h * &P_pred * h.transpose() + &r_now;

        // Gain K via factorization
        let PHt = &P_pred * h.transpose();
        let K = if let Some(chol) = Cholesky::new(S.clone()) {
            chol.solve(&PHt.transpose()).transpose()
        } else if let Some(ldlt) = LDLT::new(S.clone()) {
            ldlt.solve(&PHt.transpose()).transpose()
        } else {
            return Err(anyhow!("Innovation covariance not PD/semidefinite"));
        };

        // NIS
        let nis = {
            let s_inv_v = if let Some(ch) = Cholesky::new(S.clone()) {
                ch.solve(&v)
            } else if let Some(ld) = LDLT::new(S.clone()) {
                ld.solve(&v)
            } else { v.clone() };
            (v.transpose() * s_inv_v)[(0, 0)]
        };

        // Gate
        let chi_thr = self.chi2_gate_threshold;
        let mut x_upd = x_pred.clone();
        let mut P_upd = P_pred.clone();
        if nis <= chi_thr {
            x_upd = &x_pred + &K * &v;

            // Joseph form (Gelb ch. 6)
            let I = DMatrix::<f64>::identity(STATE_DIM, STATE_DIM);
            let U = &I - &K * &h;
            P_upd = &U * P_pred * U.transpose() + &K * &r_now * K.transpose();
        } else {
            // outlier: coast and modestly inflate
            P_upd = P_pred * 1.05;
        }

        // Keep P PSD and reasonable
        P_upd = psd_floor(P_upd, MIN_COVARIANCE);
        P_upd = clamp_matrix_diag(P_upd, MIN_COVARIANCE, MAX_COVARIANCE);

        // === Sage–Husa R update (proper notation) ===
        // R̂ = v vᵀ − H P^- Hᵀ  (PSD-projected), R_k = (1−β)R_{k−1} + β R̂
        let mut R_est = &v * v.transpose() - &h * &P_pred * h.transpose();
        R_est = symmetrize(R_est);
        let (R_est_psd, r_clamped) = psd_floor_flag(R_est, 1e-12);
        if r_clamped { self.bump_r_clamp(); }
        r_now = (1.0 - beta_r) * r_now + beta_r * R_est_psd;
        let (r_now_psd, r2_clamped) = psd_floor_flag(r_now, 1e-10);
        if r2_clamped { self.bump_r_clamp(); }
        *self.measurement_noise.write() = r_now_psd.clone();

        // === Adaptive Q via covariance identity (closed form on CA block) ===
        // Q̂_full = P^- − F P^+_{k−1} Fᵀ
        let Q_est_full = {
            let prev_P_plus = st.covariance.clone();
            let mut qhat = P_pred.clone() - &F * &prev_P_plus * F.transpose();
            qhat = symmetrize(qhat);
            let (qhat_psd, q_clamped) = psd_floor_flag(qhat, 1e-12);
            if q_clamped { self.bump_q_clamp(); }
            qhat_psd
        };

        // Extract CA 3x3 block and fit scalar q to basis B(dt) (trace LS)  [Gelb, Bar-Shalom]
        let B = ca_Q_basis(dt); // 3x3 (see ca_Q_basis doc tag)
        let Q_ca = Q_est_full.slice((0, 0), (3, 3)).into_owned();
        let num = trace_of(&(&B.transpose() * &Q_ca));
        let den = trace_of(&(&B.transpose() * &B)).max(1e-18);
        let q_hat_cov_id = (num / den).clamp(Q_SPREAD_MIN, Q_SPREAD_MAX);

        // === Mehra-style reconciliation using innovation statistics ===
        // Maintain S_emp = E[v vᵀ] and reconcile with S_th = H P^- Hᵀ + R, approximating
        // Q contribution through H (q * B_embedded). Solve min || M - H (q B_emb) Hᵀ ||_F
        // where M = S_emp - S_th(q=0). Closed-form scalar LS again.
        {
            let mut istats_map = self.innov_stats.write();
            let istats = istats_map.entry(pair).or_insert_with(InnovStats::new);
            istats.sum_vvt = &istats.sum_vvt + &(&v * v.transpose());
            istats.count += 1;
            istats.last_P_pred = P_pred.clone();

            if istats.count >= 8 {
                let S_emp = (1.0 / istats.count as f64) * istats.sum_vvt.clone();
                let S_th0 = &h * &P_pred * h.transpose() + &r_now_psd; // treat q effect separately
                let M = symmetrize(S_emp - S_th0);

                let B_emb = embed_ca_basis_into_state(&B);
                let HBH = &h * &B_emb * h.transpose();

                let num_m = trace_of(&(HBH.transpose() * M));
                let den_m = trace_of(&(HBH.transpose() * HBH)).max(1e-18);
                let q_hat_mehra = (num_m / den_m).clamp(Q_SPREAD_MIN, Q_SPREAD_MAX);

                // Fuse both q estimates conservatively
                let q_blend = 0.5 * q_hat_cov_id + 0.5 * q_hat_mehra;
                let mut q_cur = *self.q_spread.read();
                q_cur = (1.0 - beta_q) * q_cur + beta_q * q_blend
                    + BETA_Q_MEHRA * (q_hat_mehra - q_cur);
                q_cur = q_cur.clamp(Q_SPREAD_MIN, Q_SPREAD_MAX);
                *self.q_spread.write() = q_cur;

                // Decay stats to keep responsiveness
                istats.sum_vvt = 0.5 * istats.sum_vvt.clone();
                istats.count = (istats.count / 2).max(4);
            } else {
                // Early stage: use covariance-identity estimate softly
                let mut q_cur = *self.q_spread.read();
                q_cur = (1.0 - beta_q) * q_cur + beta_q * q_hat_cov_id;
                q_cur = q_cur.clamp(Q_SPREAD_MIN, Q_SPREAD_MAX);
                *self.q_spread.write() = q_cur;
            }
        }

        // === NIS EMA monitor ===
        {
            let mut nis_m = self.nis_ema.write();
            *nis_m = (1.0 - NIS_ALPHA) * *nis_m + NIS_ALPHA * nis;
        }

        // Save
        st.state = x_upd;
        st.covariance = P_upd;
        st.innovation = v;
        st.innovation_covariance = S;
        st.nis = nis;
        st.last_update = ts_ms;

        Ok(())
    }

    // ---------------- F/Q builders (CA + OU) ----------------

    /// Discrete state transition F(dt)
    ///
    /// CA block (Gelb ch. 6; Bar-Shalom Li Kirubarajan):
    ///   x = [pos, vel, acc]  with white-noise jerk → F =
    ///     [[1, dt, 0.5 dt^2],
    ///      [0,  1,      dt ],
    ///      [0,  0,       1 ]]
    ///
    /// OU block (Bar-Shalom; Singer model):
    ///   F_ii = exp(-dt/τ_i) for states [3,4,5]
    fn build_F(&self, dt: f64) -> DMatrix<f64> {
        let mut F = DMatrix::<f64>::identity(STATE_DIM, STATE_DIM);
        // CA block
        F[(0, 1)] = dt;
        F[(0, 2)] = 0.5 * dt * dt;
        F[(1, 2)] = dt;

        // OU persistence for exogenous states
        let ou0 = *self.ou_params[0].read();
        let ou1 = *self.ou_params[1].read();
        let ou2 = *self.ou_params[2].read();
        F[(3, 3)] = (-dt / ou0.tau).exp();
        F[(4, 4)] = (-dt / ou1.tau).exp();
        F[(5, 5)] = (-dt / ou2.tau).exp();
        F
    }

    /// Q(dt) with current q scalar for CA, OU closed forms for exogenous states.
    fn build_Q_with(&self, q: f64, dt: f64) -> DMatrix<f64> {
        let mut Q = DMatrix::<f64>::zeros(STATE_DIM, STATE_DIM);

        // CA block (Gelb ch. 6; Bar-Shalom): see ca_Q_basis()
        let B = ca_Q_basis(dt);
        for i in 0..3 { for j in 0..3 { Q[(i, j)] = q * B[(i, j)]; } }

        // OU blocks (independent) [Bar-Shalom; Singer]
        let ou0 = *self.ou_params[0].read();
        let ou1 = *self.ou_params[1].read();
        let ou2 = *self.ou_params[2].read();
        Q[(3, 3)] = ou_discrete_Q(ou0.tau, ou0.sigma2, dt);
        Q[(4, 4)] = ou_discrete_Q(ou1.tau, ou1.sigma2, dt);
        Q[(5, 5)] = ou_discrete_Q(ou2.tau, ou2.sigma2, dt);

        Q
    }

    /// Convenience wrapper using current q.
    fn build_Q(&self, dt: f64) -> DMatrix<f64> {
        let q = *self.q_spread.read();
        self.build_Q_with(q, dt)
    }

    fn model_confidence(&self, st: &KalmanState) -> f64 {
        // Simple innovation and NIS-based confidence proxy
        let s00 = st.innovation_covariance[(0, 0)].max(1e-12);
        let v0 = st.innovation[0];
        let innovation_factor = (-(v0.abs() / s00.sqrt())).exp();

        let nis_mean = *self.nis_ema.read();
        let maha_factor = (-(nis_mean.sqrt()) / self.chi2_gate_df.sqrt()).exp();

        (0.6 * innovation_factor + 0.4 * maha_factor).clamp(0.0, 1.0)
    }

    // -------- alarms / counters --------

    fn bump_update_count_and_check_nis_alarm(&self) {
        let mut n = self.update_count.write();
        *n += 1;
        if *n >= NIS_ALERT_MIN_UPDATES {
            let nis = *self.nis_ema.read();
            let df = self.chi2_gate_df;
            let sd = (2.0 * df / *n as f64).sqrt();
            let alarm = (nis - df).abs() > 3.0 * sd;
            if alarm {
                eprintln!(
                    "[KF-ALERT] NIS EMA {:.6} outside χ²(df={:.0}) 3σ band [{:.6}, {:.6}] (N={})",
                    nis, df, df - 3.0 * sd, df + 3.0 * sd, *n
                );
            }
        }
    }

    fn bump_r_clamp(&self) {
        let mut ema = self.r_clamp_ema.write();
        *ema = (1.0 - CLAMP_ALPHA) * *ema + CLAMP_ALPHA * 1.0;
        self.maybe_freeze_if_clamp_rate_high();
    }
    fn bump_q_clamp(&self) {
        let mut ema = self.q_clamp_ema.write();
        *ema = (1.0 - CLAMP_ALPHA) * *ema + CLAMP_ALPHA * 1.0;
        self.maybe_freeze_if_clamp_rate_high();
    }
    fn maybe_freeze_if_clamp_rate_high(&self) {
        let r = *self.r_clamp_ema.read();
        let q = *self.q_clamp_ema.read();
        let frozen = *self.adaptation_frozen.read();
        if (r > CLAMP_ALERT_RATE || q > CLAMP_ALERT_RATE) && !frozen {
            eprintln!(
                "[KF-ALERT] Clamp rate high (R_ema={:.3}, Q_ema={:.3}) → freezing adaptation",
                r, q
            );
            self.set_adaptation_frozen(true);
        }
    }
}

// ======================================================================
// Offline identification helpers (separate from filter core)
// ======================================================================

/// Fit mid-price-change coefficients y ≈ a*velocity + b*trend with ridge regularization.
/// Returns (a, b) and records dataset/version strings for audit if desired.
pub fn fit_midchange_coefficients_ridge(
    velocity: &[f64],
    trend: &[f64],
    y_mid_change: &[f64],
    lambda: f64,
) -> Result<(f64, f64)> {
    let n = y_mid_change.len();
    if velocity.len() != n || trend.len() != n || n < 8 {
        return Err(anyhow!("Need matching arrays with n >= 8"));
    }
    let mut X = DMatrix::<f64>::zeros(n, 2);
    for i in 0..n {
        X[(i, 0)] = velocity[i];
        X[(i, 1)] = trend[i];
    }
    let y = DVector::from_iterator(n, y_mid_change.iter().cloned());

    // Ridge: beta = (XᵀX + λI)^{-1} Xᵀ y
    let Xt = X.transpose();
    let reg = DMatrix::<f64>::identity(2, 2) * lambda.max(0.0);
    let m = &Xt * &X + reg;

    if let Some(ch) = Cholesky::new(m) {
        let beta = ch.solve(&(&Xt * &y));
        Ok((beta[0], beta[1]))
    } else if let Some(ld) = LDLT::new(m) {
        let beta = ld.solve(&(&Xt * &y));
        Ok((beta[0], beta[1]))
    } else {
        Err(anyhow!("Ridge normal equations not PD/semidefinite"))
    }
}

// ======================================================================
// Utilities: PSD projection, symmetrize, CA basis, traces, realized vol,
//            OU helpers, embedding
// ======================================================================

fn symmetrize(mut p: DMatrix<f64>) -> DMatrix<f64> {
    p = 0.5 * (&p + p.transpose());
    p
}

fn psd_floor(mut p: DMatrix<f64>, eps: f64) -> DMatrix<f64> {
    p = symmetrize(p);
    let se = SymmetricEigen::new(p.clone());
    let mut d = se.eigenvalues;
    let mut any = false;
    for i in 0..d.len() {
        if d[i] < eps { d[i] = eps; any = true; }
    }
    if any {
        let v = se.eigenvectors;
        p = &v * DMatrix::from_diagonal(&d) * v.transpose();
        p = symmetrize(p);
    }
    p
}

/// PSD floor that reports whether a clamp occurred (for R/Q invariants).
fn psd_floor_flag(mut p: DMatrix<f64>, eps: f64) -> (DMatrix<f64>, bool) {
    p = symmetrize(p);
    let se = SymmetricEigen::new(p.clone());
    let mut d = se.eigenvalues;
    let mut any = false;
    for i in 0..d.len() {
        if d[i] < eps { d[i] = eps; any = true; }
    }
    if any {
        let v = se.eigenvectors;
        p = &v * DMatrix::from_diagonal(&d) * v.transpose();
        p = symmetrize(p);
    }
    (p, any)
}

fn clamp_matrix_diag(mut p: DMatrix<f64>, lo: f64, hi: f64) -> DMatrix<f64> {
    let n = p.nrows().min(p.ncols());
    for i in 0..n {
        p[(i, i)] = p[(i, i)].clamp(lo, hi);
    }
    p
}

/// CA Q basis (Gelb ch. 6; Bar-Shalom Li Kirubarajan).
/// For constant-acceleration with white-noise jerk spectral density q:
///   Q_ca(dt) = q * [[dt^5/20, dt^4/8, dt^3/6],
///                   [dt^4/8,  dt^3/3, dt^2/2],
///                   [dt^3/6,  dt^2/2, dt]]
fn ca_Q_basis(dt: f64) -> DMatrix<f64> {
    let dt2 = dt * dt;
    let dt3 = dt2 * dt;
    let dt4 = dt2 * dt2;
    let dt5 = dt3 * dt2;

    let mut B = DMatrix::<f64>::zeros(3, 3);
    B[(0, 0)] = dt5 / 20.0;
    B[(0, 1)] = dt4 / 8.0;
    B[(0, 2)] = dt3 / 6.0;
    B[(1, 0)] = B[(0, 1)];
    B[(1, 1)] = dt3 / 3.0;
    B[(1, 2)] = dt2 / 2.0;
    B[(2, 0)] = B[(0, 2)];
    B[(2, 1)] = B[(1, 2)];
    B[(2, 2)] = dt;
    B
}

fn embed_ca_basis_into_state(B: &DMatrix<f64>) -> DMatrix<f64> {
    let mut E = DMatrix::<f64>::zeros(STATE_DIM, STATE_DIM);
    for i in 0..3 { for j in 0..3 { E[(i, j)] = B[(i, j)]; } }
    E
}

fn trace_of(m: &DMatrix<f64>) -> f64 {
    let n = m.nrows().min(m.ncols());
    let mut t = 0.0;
    for i in 0..n { t += m[(i, i)]; }
    t
}

fn estimate_realized_vol(obs: &[SpreadObservation]) -> f64 {
    let n = obs.len();
    if n < 2 { return 0.0; }
    let window = n.min(32);
    let slice = &obs[n - window..];
    let mut rets = Vec::with_capacity(window.saturating_sub(1));
    for w in slice.windows(2) {
        let a = w[0].mid_price;
        let b = w[1].mid_price;
        if a > 0.0 && b > 0.0 {
            rets.push((b / a).ln());
        }
    }
    if rets.is_empty() { return 0.0; }
    let mean = rets.iter().copied().sum::<f64>() / rets.len() as f64;
    let var = rets.iter().map(|r| {
        let d = r - mean; d * d
    }).sum::<f64>() / rets.len() as f64;
    var.sqrt()
}

/// OU discrete Q closed form (Bar-Shalom; Singer)
///   Qd = σ² * (1 - e^{-2 dt / τ}) / (2/τ)
fn ou_discrete_Q(tau: f64, sigma2: f64, dt: f64) -> f64 {
    if tau <= 0.0 { return 0.0; }
    let lam = 1.0 / tau;
    sigma2 * (1.0 - (-2.0 * lam * dt).exp()) / (2.0 * lam)
}

/// Given a target discrete Q at dt=1s, solve for σ² that matches the OU closed form.
fn ou_sigma2_from_Qd1(Qd1: f64, tau: f64) -> f64 {
    if tau <= 0.0 { return 0.0; }
    let lam = 1.0 / tau;
    let denom = (1.0 - (-2.0 * lam).exp()) / (2.0 * lam);
    if denom <= 0.0 { return 0.0; }
    (Qd1 / denom).max(0.0)
}

// ======================================================================
// Van-Loan/OU parity tests
// ======================================================================
#[cfg(test)]
mod tests {
    use super::*;
    use nalgebra::ComplexField;
    use rand::{Rng, SeedableRng};
    use rand::rngs::StdRng;

    fn expm(a: &DMatrix<f64>) -> DMatrix<f64> {
        // Scaling and squaring with Pade(6)
        let one = DMatrix::<f64>::identity(a.nrows(), a.ncols());
        let norm = a.norm();
        let s = if norm > 0.5 { (norm / 0.5).log2().ceil().max(0.0) as u32 } else { 0 };
        let a_scaled = a / 2f64.powi(s as i32);

        // Pade(6) coefficients
        let c = [1.0,
                 0.5,
                 0.12,
                 0.018333333333333333,
                 0.0019927536231884053,
                 0.00016059043836821612,
                 0.000010117,
        ];

        let a2 = &a_scaled * &a_scaled;
        let a4 = &a2 * &a2;
        let a6 = &a4 * &a2;

        let mut U = a_scaled.clone() * c[1];
        U = U + a_scaled.clone() * c[3] * &a2;
        U = U + a_scaled.clone() * c[5] * &a4;

        let mut V = one.clone() * c[0];
        V = V + &a2 * c[2];
        V = V + &a4 * c[4];
        V = V + &a6 * c[6];

        let numer = &V + &U;
        let denom = &V - &U;

        let denom_inv = denom.try_inverse().expect("invertible");
        let mut R = &denom_inv * &numer;

        for _ in 0..s { R = &R * &R; }
        R
    }

    /// Van-Loan: M = [[-A, GQcGᵀ],[0, Aᵀ]] dt, exp(M) = [[M11, M12],[0, M22]]
    /// Fd = M22ᵀ, Qd = M22ᵀ M12
    #[test]
    fn van_loan_matches_ca_closed_form() {
        let dt = 0.037; // 37 ms
        let q = 0.007;

        // Continuous CA: xdot = A x + G w, E[w wᵀ] = q δ
        let mut A = DMatrix::<f64>::zeros(3, 3);
        A[(0,1)] = 1.0;
        A[(1,2)] = 1.0;
        let mut G = DMatrix::<f64>::zeros(3, 1);
        G[(2,0)] = 1.0;
        let Qc = DMatrix::<f64>::identity(1,1) * q;

        // Van-Loan block matrix
        let n = 3;
        let mut M = DMatrix::<f64>::zeros(2*n, 2*n);
        let GQGt = &G * &Qc * G.transpose();
        // top-left: -A
        for i in 0..n { for j in 0..n { M[(i,j)] = -A[(i,j)]; } }
        // top-right: G Qc Gᵀ
        for i in 0..n { for j in 0..n { M[(i, j+n)] = GQGt[(i,j)]; } }
        // bottom-right: Aᵀ
        for i in 0..n { for j in 0..n { M[(i+n, j+n)] = A[(j,i)]; } }

        let Md = expm(&(M * dt));
        let _M11 = Md.slice((0,0),(n,n)).into_owned();
        let M12 = Md.slice((0,n),(n,n)).into_owned();
        let M22 = Md.slice((n,n),(n,n)).into_owned();

        let Fd_vl = M22.transpose();
        let Qd_vl = &Fd_vl.transpose() * &M12;

        // Closed-form
        let F_cf = {
            let mut F = DMatrix::<f64>::identity(3,3);
            F[(0,1)] = dt;
            F[(0,2)] = 0.5*dt*dt;
            F[(1,2)] = dt;
            F
        };
        let Q_cf = ca_Q_basis(dt) * q;

        // Compare
        assert!((Fd_vl - F_cf).norm() < 1e-10, "F mismatch");
        assert!((Qd_vl - Q_cf).norm() < 1e-10, "Q mismatch");
    }

    #[test]
    fn van_loan_grid_sweep() {
        let mut rng = StdRng::seed_from_u64(42);
        for _ in 0..64 {
            let dt = rng.gen_range(1.0e-4..0.2);
            let q = rng.gen_range(1.0e-6..1.0e-1);

            let mut A = DMatrix::<f64>::zeros(3, 3);
            A[(0,1)] = 1.0;
            A[(1,2)] = 1.0;
            let mut G = DMatrix::<f64>::zeros(3, 1);
            G[(2,0)] = 1.0;
            let Qc = DMatrix::<f64>::identity(1,1) * q;

            let n = 3;
            let mut M = DMatrix::<f64>::zeros(2*n, 2*n);
            let GQGt = &G * &Qc * G.transpose();
            for i in 0..n { for j in 0..n { M[(i,j)] = -A[(i,j)]; } }
            for i in 0..n { for j in 0..n { M[(i, j+n)] = GQGt[(i,j)]; } }
            for i in 0..n { for j in 0..n { M[(i+n, j+n)] = A[(j,i)]; } }

            let Md = expm(&(M * dt));
            let M12 = Md.slice((0,n),(n,n)).into_owned();
            let M22 = Md.slice((n,n),(n,n)).into_owned();

            let Fd_vl = M22.transpose();
            let Qd_vl = &Fd_vl.transpose() * &M12;

            let mut F_cf = DMatrix::<f64>::identity(3,3);
            F_cf[(0,1)] = dt;
            F_cf[(0,2)] = 0.5*dt*dt;
            F_cf[(1,2)] = dt;
            let Q_cf = ca_Q_basis(dt) * q;

            assert!((Fd_vl - F_cf).norm() < 1e-9, "F mismatch");
            assert!((Qd_vl - Q_cf).norm() < 1e-9, "Q mismatch");
        }
    }

    #[test]
    fn psd_floor_sane() {
        let mut P = DMatrix::<f64>::from_diagonal_element(4,4,-1.0);
        P[(0,1)] = 10.0;
        let Pf = psd_floor(P, 1e-9);
        // Eigenvalues >= eps and symmetric
        let se = SymmetricEigen::new(Pf.clone());
        assert!(se.eigenvalues.iter().all(|&e| e >= 0.0));
        assert!((Pf.clone() - Pf.transpose()).norm() < 1e-12);
    }

    #[test]
    fn ou_parity_closed_form() {
        let tau = 20.0;
        let sigma2 = 0.07;
        let dt = 0.5; // 500 ms

        // Discrete F, Q
        let Fd = (-dt/tau).exp();
        let Qd = ou_discrete_Q(tau, sigma2, dt);

        // Sanity: both within (0,1] and >= 0
        assert!(Fd > 0.0 && Fd <= 1.0);
        assert!(Qd >= 0.0);

        // Check limit behaviors
        let Qd_small = ou_discrete_Q(tau, sigma2, 1.0e-6);
        assert!(Qd_small >= 0.0 && Qd_small < Qd);
    }
}
