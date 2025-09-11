use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use serde::{Deserialize, Serialize};
use rust_decimal::prelude::*;
use rust_decimal_macros::dec;

// ---- FastMap: fxhash when "fxhash" feature is on; std HashMap fallback ----
#[cfg(feature = "fxhash")]
use rustc_hash::FxHashMap as FastHashMap;
#[cfg(not(feature = "fxhash"))]
use std::collections::HashMap as FastHashMap;
type FastMap<K, V> = FastHashMap<K, V>;

// ---- RwLock backend: parking_lot or std, with macros that compile in both ----
#[cfg(feature = "parking_lot")]
use parking_lot::RwLock;
#[cfg(not(feature = "parking_lot"))]
use std::sync::RwLock;

#[cfg(feature = "parking_lot")]
macro_rules! rlock { ($l:expr) => { $l.read() } }
#[cfg(not(feature = "parking_lot"))]
macro_rules! rlock { ($l:expr) => { $l.read().unwrap() } }

// ==== ΩQ: P² streaming quantile estimator for VaR(5%) ====
#[derive(Debug, Clone)]
struct P2Quant {
    p: Decimal,           // target quantile, e.g. 0.05
    count: usize,
    q: [Decimal; 5],      // marker heights
    n: [i64; 5],          // marker positions
    np: [Decimal; 5],     // desired positions
    dn: [Decimal; 5],     // increments
    init: Vec<Decimal>,   // bootstrap until 5 samples
}

impl P2Quant {
    #[inline(always)]
    fn new(p: Decimal) -> Self {
        Self { p, count: 0, q: [Decimal::ZERO; 5], n: [0; 5], np: [Decimal::ZERO; 5], dn: [Decimal::ZERO; 5], init: Vec::with_capacity(5) }
    }
    #[inline(always)]
    fn quantile(&self) -> Option<Decimal> { if self.count >= 5 { Some(self.q[2]) } else { None } }

    #[inline(always)]
    fn update(&mut self, x: Decimal) {
        if self.count < 5 {
            self.init.push(x);
            self.count += 1;
            if self.count == 5 {
                self.init.sort_unstable();
                for i in 0..5 { self.q[i] = self.init[i]; self.n[i] = (i as i64) + 1; }
                let p = self.p;
                self.np = [Decimal::ONE,
                           dec!(1) + dec!(2)*p,
                           dec!(1) + dec!(4)*p,
                           dec!(3) + dec!(2)*p,
                           dec!(5)];
                self.dn = [Decimal::ZERO, p/dec!(2), p, (Decimal::ONE+p)/dec!(2), Decimal::ONE];
            }
            return;
        }

        // locate cell
        let k: i32 = if x < self.q[0] { self.q[0] = x; 0 }
                     else if x < self.q[1] { 0 }
                     else if x < self.q[2] { 1 }
                     else if x < self.q[3] { 2 }
                     else { self.q[4] = x; 3 };

        for i in 0..5 { self.n[i] += if (i as i32) <= k { 1 } else { 0 }; }
        for i in 0..5 { self.np[i] += self.dn[i]; }

        // adjust heights
        for i in 1..4 {
            let di = Decimal::from(self.n[i]) - self.np[i];
            if di.abs() >= Decimal::ONE {
                let d = if di > Decimal::ZERO { Decimal::ONE } else { -Decimal::ONE };
                let qi = self.q[i];
                let qi1 = self.q[i-1];
                let qi2 = self.q[i+1];

                let n_i  = Decimal::from(self.n[i]);
                let n_i1 = Decimal::from(self.n[i-1]);
                let n_i2 = Decimal::from(self.n[i+1]);

                let a = safe_div((n_i - n_i1 + d) * (qi2 - qi), (n_i2 - n_i), Decimal::ZERO);
                let b = safe_div((n_i2 - n_i - d) * (qi - qi1), (n_i - n_i1), Decimal::ZERO);
                let q_new = qi + safe_div(d, (n_i2 - n_i1), Decimal::ONE) * (a + b);

                let mut adj = q_new;
                if adj <= qi1 || adj >= qi2 {
                    adj = if d > Decimal::ZERO {
                        qi + safe_div(qi2 - qi, (n_i2 - n_i), Decimal::ONE)
                    } else {
                        qi - safe_div(qi - qi1, (n_i - n_i1), Decimal::ONE)
                    };
                }
                self.q[i] = adj;
                self.n[i] += if d > Decimal::ZERO { 1 } else { -1 };
            }
        }
        self.count += 1;
    }
}

#[cfg(feature = "parking_lot")]
macro_rules! wlock { ($l:expr) => { $l.write() } }
#[cfg(not(feature = "parking_lot"))]
macro_rules! wlock { ($l:expr) => { $l.write().unwrap() } }

// --------- numeric constants (cache once) ---------
const RISK_FREE_RATE: Decimal = dec!(0.0001); // 0.01% hourly
const MIN_OBSERVATIONS: usize = 30;
const MAX_OBSERVATIONS: usize = 1000;
const CONFIDENCE_LEVEL: Decimal = dec!(1.96); // 95% confidence
const MAX_POSITION_FRACTION: Decimal = dec!(0.25);
const MIN_SHARPE_THRESHOLD: Decimal = dec!(0.5);
const VOLATILITY_WINDOW: usize = 20;
const SHARPE_WINDOW: usize = 100;
const MAX_DRAWDOWN_TOLERANCE: Decimal = dec!(0.15);
const POSITION_SCALE_FACTOR: Decimal = dec!(0.7);
const MIN_TRADE_SIZE: Decimal = dec!(0.001);
const SHARPE_DECAY_FACTOR: Decimal = dec!(0.995);
const CORRELATION_WINDOW: usize = 50;
const MAX_CORRELATION: Decimal = dec!(0.7);
const DYNAMIC_ADJUSTMENT_RATE: Decimal = dec!(0.05);
const HOURS_PER_DAY: Decimal = dec!(24);

// ---- Ω0+ tunables for robust ingest, HAC lags, regime breaks, and Kelly/CVaR damp ----
const EWMA_LAMBDA: Decimal = dec!(0.94);          // recency for corr & robust σ guard
const HUBER_K_SIGMAS: Decimal = dec!(3);          // clamp returns at ±3σ on ingest
const NW_MAX_LAG: usize = 8;                      // Newey–West lags (intraday microstructure)
const PH_DELTA: Decimal = dec!(0.0002);           // Page–Hinkley insensitivity band
const PH_LAMBDA: Decimal = dec!(0.0100);          // PH alarm threshold (≈1% move of unit return)
const REGIME_COOLDOWN_SECS: u64 = 900;            // 15 min cool-off after regime break
const KELLY_CVAR_DAMP: Decimal = dec!(6.0);       // Kelly damp: 1 / (1 + KELLY_CVAR_DAMP * CVaR)
const DRAWDOWN_DAMP_POWER: i32 = 2;               // (1 - dd)^power multiplier (power ≥ 1)

// ---- Ω0++ tunables for CVaR/alloc smoothing ----
const CVAR_EWMA_ALPHA: Decimal = dec!(0.20);      // EWMA for shortfall when r <= VaR
const ALLOC_MAX_STEP_FRAC: Decimal = dec!(0.05);  // max 5% of total capital movement per rebalance
const ALLOC_DEADBAND_FRAC: Decimal = dec!(0.0025);// ignore micro diffs under 0.25% of total cap

// numerical floors/eps
const EPS: Decimal = dec!(1e-12);
const VAR_FLOOR: Decimal = dec!(1e-10);
const VOL_FLOOR: Decimal = dec!(1e-8);
const COST_FLOOR: Decimal = dec!(1e-9);

#[inline(always)]
fn clamp01(x: Decimal) -> Decimal {
    if x < Decimal::ZERO { Decimal::ZERO } else if x > Decimal::ONE { Decimal::ONE } else { x }
}

#[inline(always)]
fn safe_div(n: Decimal, d: Decimal, fallback: Decimal) -> Decimal {
    if d.abs() <= EPS { fallback } else { n / d }
}

#[inline(always)]
fn safe_sqrt(x: Decimal) -> Decimal {
    if x <= Decimal::ZERO { Decimal::ZERO } else { (x.max(VAR_FLOOR)).sqrt() }
}

#[inline(always)]
fn now_unix() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs()
}

#[inline(always)]
fn powi_decimal(mut x: Decimal, mut e: i32) -> Decimal {
    if e <= 0 { return Decimal::ONE; }
    let mut out = Decimal::ONE;
    while e > 0 {
        if (e & 1) == 1 { out *= x; }
        x *= x;
        e >>= 1;
    }
    out
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TradeResult {
    pub strategy_id: String,
    pub timestamp: u64,
    pub pnl: Decimal,
    pub capital_deployed: Decimal,
    pub gas_cost: Decimal,
    pub slippage: Decimal,
}

#[derive(Debug, Clone)]
pub struct StrategyMetrics {
    returns: VecDeque<Decimal>,
    timestamps: VecDeque<u64>,
    cumulative_return: Decimal,
    peak_value: Decimal,
    current_drawdown: Decimal,
    sharpe_ratio: Decimal,
    volatility: Decimal,
    win_rate: Decimal,
    total_trades: u64,
    winning_trades: u64,
    avg_win: Decimal,
    avg_loss: Decimal,
    last_update: u64,
    ewma_return: Decimal,
    ewma_variance: Decimal,
    last_cvar_5: Decimal,
    last_var_5: Decimal,
    // streaming tails
    p2q5: P2Quant,
    cvar5_ewma: Decimal,
    // regime detection and friction
    ewma_cost_rate: Decimal,
    ph_cum: Decimal,
    ph_min: Decimal,
    regime_alert_until: u64,
}

impl Default for StrategyMetrics {
    #[inline(always)]
    fn default() -> Self {
        Self {
            returns: VecDeque::with_capacity(MAX_OBSERVATIONS),
            timestamps: VecDeque::with_capacity(MAX_OBSERVATIONS),
            cumulative_return: Decimal::ONE,
            peak_value: Decimal::ONE,
            current_drawdown: Decimal::ZERO,
            sharpe_ratio: Decimal::ZERO,
            volatility: Decimal::ZERO,
            win_rate: Decimal::ZERO,
            total_trades: 0,
            winning_trades: 0,
            avg_win: Decimal::ZERO,
            avg_loss: Decimal::ZERO,
            last_update: 0,
            ewma_return: Decimal::ZERO,
            ewma_variance: Decimal::ZERO,
            last_cvar_5: Decimal::ZERO,
            last_var_5: Decimal::ZERO,
            p2q5: P2Quant::new(dec!(0.05)),
            cvar5_ewma: Decimal::ZERO,
            ewma_cost_rate: Decimal::ZERO,
            ph_cum: Decimal::ZERO,
            ph_min: Decimal::ZERO,
            regime_alert_until: 0,
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct PairCorr { rho: Decimal, ts: u64 }

#[derive(Debug, Clone)]
pub struct CorrelationMatrix {
    // adjacency: strategy -> (peer -> {rho, ts})
    edges: FastMap<String, FastMap<String, PairCorr>>,
    last_update: u64,
}

pub struct SharpeRatioMaximizer {
    strategy_metrics: Arc<RwLock<FastMap<String, StrategyMetrics>>>,
    correlation_matrix: Arc<RwLock<CorrelationMatrix>>,
    total_capital: Arc<RwLock<Decimal>>,
    allocated_capital: Arc<RwLock<FastMap<String, Decimal>>>,
    risk_budget: Arc<RwLock<Decimal>>,
    max_var: Decimal,
    lookback_period: Duration,
}

impl StrategyMetrics {
    #[inline(always)]
    fn new() -> Self { Self::default() }

    #[inline(always)]
    fn update(&mut self, result: &TradeResult) {
        if result.capital_deployed <= Decimal::ZERO { return; }

        // Real cost (gas+slip) with floor
        let total_cost = (result.gas_cost + result.slippage).max(COST_FLOOR);
        let raw_r = safe_div((result.pnl - total_cost), result.capital_deployed, Decimal::ZERO);

        // EWMA mean/var for robust clamp
        let alpha = dec!(0.06);
        let prev_mu = self.ewma_return;
        self.ewma_return   = alpha * raw_r + (Decimal::ONE - alpha) * self.ewma_return;
        let dev            = raw_r - self.ewma_return;
        self.ewma_variance = alpha * (dev * dev) + (Decimal::ONE - alpha) * self.ewma_variance;
        let ewma_sigma     = safe_sqrt(self.ewma_variance).max(VOL_FLOOR);

        // Winsorize at ±Kσ around EWMA mean
        let ksig = HUBER_K_SIGMAS * ewma_sigma;
        let upper = self.ewma_return + ksig;
        let lower = self.ewma_return - ksig;
        let mut r = raw_r;
        if r > upper { r = upper; } else if r < lower { r = lower; }

        // Record sample
        self.returns.push_back(r);
        self.timestamps.push_back(result.timestamp);
        if self.returns.len() > MAX_OBSERVATIONS {
            self.returns.pop_front();
            self.timestamps.pop_front();
        }

        // Compounding & drawdown
        self.cumulative_return *= (Decimal::ONE + r);
        if self.cumulative_return > self.peak_value { self.peak_value = self.cumulative_return; }
        self.current_drawdown = safe_div(self.peak_value - self.cumulative_return,
                                         self.peak_value.max(Decimal::ONE), Decimal::ZERO);

        // Trade stats
        self.total_trades = self.total_trades.saturating_add(1);
        if result.pnl > total_cost {
            self.winning_trades = self.winning_trades.saturating_add(1);
            let win_amount = (result.pnl - total_cost).max(Decimal::ZERO);
            let wins = Decimal::from(self.winning_trades);
            self.avg_win = safe_div(self.avg_win * (wins - Decimal::ONE) + win_amount, wins, self.avg_win);
        } else {
            let loss_amount = (total_cost - result.pnl).max(Decimal::ZERO);
            let losing_trades_u64 = self.total_trades.saturating_sub(self.winning_trades);
            let losing_trades = Decimal::from(losing_trades_u64.max(1));
            self.avg_loss = safe_div(self.avg_loss * (losing_trades - Decimal::ONE) + loss_amount,
                                     losing_trades, self.avg_loss);
        }
        self.win_rate = safe_div(Decimal::from(self.winning_trades),
                                 Decimal::from(self.total_trades.max(1)), Decimal::ZERO);

        // Cost EWMA (friction)
        let cost_rate = safe_div(total_cost, result.capital_deployed, Decimal::ZERO);
        self.ewma_cost_rate = alpha * cost_rate + (Decimal::ONE - alpha) * self.ewma_cost_rate;

        // Page–Hinkley regime detector
        let d = r - prev_mu - PH_DELTA;
        self.ph_cum += d;
        if self.ph_cum < self.ph_min { self.ph_min = self.ph_cum; }
        if (self.ph_cum - self.ph_min) > PH_LAMBDA {
            self.regime_alert_until = result.timestamp.saturating_add(REGIME_COOLDOWN_SECS);
            self.ph_cum = Decimal::ZERO; self.ph_min = Decimal::ZERO;
        }

        // Streaming tails (P² + EWMA shortfall)
        self.p2q5.update(r);
        if let Some(var5) = self.p2q5.quantile() {
            self.last_var_5 = var5.abs();
            if r <= var5 {
                let loss = (-r).max(Decimal::ZERO);
                self.cvar5_ewma = CVAR_EWMA_ALPHA * loss + (Decimal::ONE - CVAR_EWMA_ALPHA) * self.cvar5_ewma;
            }
            self.last_cvar_5 = self.cvar5_ewma.max(self.last_var_5);
        }

        self.calculate_metrics();
        self.last_update = result.timestamp;
    }

    #[inline(always)]
    fn calculate_metrics(&mut self) {
        let n = self.returns.len();
        if n < MIN_OBSERVATIONS { return; }

        // One-pass mean/var
        let mut mean = Decimal::ZERO;
        let mut m2 = Decimal::ZERO;
        let mut k = Decimal::ZERO;
        for r in self.returns.iter() {
            k += Decimal::ONE;
            let delta = *r - mean;
            mean += safe_div(delta, k, Decimal::ZERO);
            let delta2 = *r - mean;
            m2 += delta * delta2;
        }
        let denom = (Decimal::from(n) - Decimal::ONE).max(Decimal::ONE);
        let sample_var = safe_div(m2, denom, Decimal::ZERO).max(VAR_FLOOR);

        // HAC/Newey–West (Bartlett kernel up to NW_MAX_LAG)
        let nw_lags = NW_MAX_LAG.min(n.saturating_sub(1));
        let mut gamma0 = Decimal::ZERO;
        for &r in self.returns.iter() { let d = r - mean; gamma0 += d*d; }
        gamma0 = safe_div(gamma0, Decimal::from(n), Decimal::ZERO);

        let mut gammas = [Decimal::ZERO; 16];
        for lag in 1..=nw_lags.min(15) {
            let upto = n - lag;
            let mut g = Decimal::ZERO;
            for i in 0..upto {
                let d0 = self.returns[i] - mean;
                let d1 = self.returns[i + lag] - mean;
                g += d0 * d1;
            }
            gammas[lag] = safe_div(g, Decimal::from(upto), Decimal::ZERO);
        }
        let mut nw_var = gamma0;
        for lag in 1..=nw_lags.min(15) {
            let w = Decimal::ONE - safe_div(Decimal::from(lag), Decimal::from(nw_lags + 1), Decimal::ZERO);
            nw_var += dec!(2) * w * gammas[lag];
        }
        nw_var = nw_var.max(sample_var).max(VAR_FLOOR);

        // Conservative σ: max(HAC, EWMA)
        let ewma_vol = safe_sqrt(self.ewma_variance).max(VOL_FLOOR);
        let hac_vol  = safe_sqrt(nw_var).max(VOL_FLOOR);
        self.volatility = if hac_vol > ewma_vol { hac_vol } else { ewma_vol };

        // Sharpe (excess) hourly→daily
        let excess = mean - RISK_FREE_RATE;
        self.sharpe_ratio = safe_div(excess, self.volatility, Decimal::ZERO) * HOURS_PER_DAY;

        // Tails: prefer streaming; fallback to O(n) on warmup
        if let Some(var5) = self.p2q5.quantile() {
            self.last_var_5  = var5.abs();
            self.last_cvar_5 = self.cvar5_ewma.max(self.last_var_5);
        } else {
            let mut v: Vec<Decimal> = self.returns.iter().copied().collect();
            let kq = ((v.len() as f64) * 0.05).max(1.0) as usize;
            let idx = kq.min(v.len().saturating_sub(1));
            v.select_nth_unstable(idx);
            self.last_var_5 = v[idx].abs();
            let worst_k = idx;
            self.last_cvar_5 = if worst_k > 0 {
                safe_div(v[..worst_k].iter().copied().sum(), Decimal::from(worst_k), Decimal::ZERO)
                    .abs()
                    .max(self.last_var_5)
            } else { self.last_var_5 };
        }
    }

    #[inline(always)]
    fn get_kelly_fraction(&self) -> Decimal {
        // Fallback if insufficient loss stats
        if self.avg_loss <= EPS || self.win_rate <= EPS { return Decimal::ZERO; }

        let loss_rate = (Decimal::ONE - self.win_rate).max(Decimal::ZERO);
        let wl = safe_div(self.avg_win.max(EPS), self.avg_loss.max(EPS), Decimal::ZERO);
        let raw = safe_div(self.win_rate * wl - loss_rate, wl.max(EPS), Decimal::ZERO).max(Decimal::ZERO);

        // CVaR damp (tail-aware): 1 / (1 + λ * CVaR)
        let cvar = if self.last_cvar_5 > Decimal::ZERO { self.last_cvar_5 } else { safe_sqrt(self.ewma_variance).max(VOL_FLOOR) };
        let cvar_damp = safe_div(Decimal::ONE, (Decimal::ONE + KELLY_CVAR_DAMP * cvar), Decimal::ONE);

        // Drawdown damp: (1 - dd)^p
        let dd = clamp01(self.current_drawdown);
        let dd_damp = powi_decimal(Decimal::ONE - dd, DRAWDOWN_DAMP_POWER);

        clamp01(raw * cvar_damp * dd_damp).min(MAX_POSITION_FRACTION)
    }

    #[inline(always)]
    fn get_confidence_adjusted_sharpe(&self) -> Decimal {
        let n = self.returns.len();
        if n < MIN_OBSERVATIONS { return Decimal::ZERO; }
        let n_d = Decimal::from(n);
        let se = safe_div(Decimal::ONE, safe_sqrt(n_d), Decimal::ONE); // ~1/sqrt(n)
        (self.sharpe_ratio - (CONFIDENCE_LEVEL * se)).max(Decimal::ZERO)
    }
}

impl SharpeRatioMaximizer {
    #[inline(always)]
    pub fn get_system_health(&self) -> SystemHealth {
        // Build an atomic snapshot without relying on unwraps
        let metrics = rlock!(self.strategy_metrics);
        let total_capital = *rlock!(self.total_capital);
        let allocated_capital: Decimal = rlock!(self.allocated_capital).values().copied().sum();

        let mut active = 0usize;
        let mut sharpe_sum = Decimal::ZERO;
        let mut max_dd = Decimal::ZERO;

        for m in metrics.values() {
            if m.returns.len() >= MIN_OBSERVATIONS {
                active += 1;
                sharpe_sum += m.sharpe_ratio;
                if m.current_drawdown > max_dd { max_dd = m.current_drawdown; }
            }
        }

        let avg_sharpe = if active > 0 { safe_div(sharpe_sum, Decimal::from(active), Decimal::ZERO) } else { Decimal::ZERO };
        let utilization = safe_div(allocated_capital, total_capital.max(EPS), Decimal::ZERO);
        let status = if max_dd > dec!(0.25) { HealthStatus::Critical }
            else if avg_sharpe < MIN_SHARPE_THRESHOLD { HealthStatus::Warning }
            else { HealthStatus::Healthy };

        SystemHealth {
            status,
            active_strategies: active,
            total_capital,
            allocated_capital,
            utilization,
            average_sharpe: avg_sharpe,
            max_drawdown: max_dd,
            timestamp: now_unix(),
        }
    }
    pub fn new(total_capital: Decimal, max_var: Decimal) -> Self {
        Self {
            strategy_metrics: Arc::new(RwLock::new(FastMap::default())),
            correlation_matrix: Arc::new(RwLock::new(CorrelationMatrix {
                edges: FastMap::default(),
                last_update: 0,
            })),
            total_capital: Arc::new(RwLock::new(total_capital)),
            allocated_capital: Arc::new(RwLock::new(FastMap::default())),
            risk_budget: Arc::new(RwLock::new(total_capital * max_var)),
            max_var,
            lookback_period: Duration::from_secs(3600 * 24),
        }
    }

    pub fn record_trade(&self, result: TradeResult) -> Result<(), Box<dyn std::error::Error>> {
        {
            let mut metrics = wlock!(self.strategy_metrics);
            metrics.entry(result.strategy_id.clone()).or_default().update(&result);
        }
        self.update_correlations_for(&result.strategy_id);
        self.rebalance_allocations()?;
        Ok(())
    }

    pub fn get_position_size(&self, strategy_id: &str, opportunity_size: Decimal) -> Decimal {
        use std::collections::HashMap;

        let metrics = rlock!(self.strategy_metrics);
        let allocated = rlock!(self.allocated_capital);

        let available_capital = allocated.get(strategy_id).copied().unwrap_or(Decimal::ZERO);
        if available_capital <= MIN_TRADE_SIZE { return Decimal::ZERO; }

        if let Some(sm) = metrics.get(strategy_id) {
            let now = now_unix();
            if sm.regime_alert_until > 0 && now < sm.regime_alert_until { return Decimal::ZERO; }
            if sm.current_drawdown > MAX_DRAWDOWN_TOLERANCE { return Decimal::ZERO; }

            let csh = sm.get_confidence_adjusted_sharpe();
            if csh < MIN_SHARPE_THRESHOLD { return MIN_TRADE_SIZE.min(available_capital); }

            let kelly = sm.get_kelly_fraction();
            let sharpe_adj = clamp01(safe_div(csh, dec!(3), Decimal::ZERO));
            let vol_adj = clamp01(safe_div(dec!(0.02), (sm.volatility.max(VOL_FLOOR) + dec!(0.001)), Decimal::ONE));

            let cvar = if sm.last_cvar_5 > Decimal::ZERO { sm.last_cvar_5 } else { safe_sqrt(sm.ewma_variance).max(VOL_FLOOR) };
            let cvar_penalty = safe_div(Decimal::ONE, (Decimal::ONE + KELLY_CVAR_DAMP * cvar), Decimal::ONE);

            let cost_bias = (Decimal::ONE + sm.ewma_cost_rate).min(dec!(1.05));

            let position_fraction = clamp01(kelly * sharpe_adj * vol_adj * cvar_penalty * POSITION_SCALE_FACTOR / cost_bias);
            let max_position = (available_capital * position_fraction).max(MIN_TRADE_SIZE);
            max_position.min(opportunity_size)
        } else {
            MIN_TRADE_SIZE.min(available_capital)
        }
    }

    #[inline(always)]
    fn update_correlations(&self) {
        let metrics = rlock!(self.strategy_metrics);
        let ids: Vec<String> = metrics.keys().cloned().collect();
        drop(metrics);
        for id in ids { self.update_correlations_for(&id); }
    }

    #[inline(always)]
    fn update_correlations_for(&self, changed: &str) {
        let metrics = rlock!(self.strategy_metrics);
        if metrics.len() < 2 { return; }

        let mut corr = wlock!(self.correlation_matrix);
        let now = now_unix();

        if let Some(m1) = metrics.get(changed) {
            for (other_id, m2) in metrics.iter() {
                if other_id == changed { continue; }
                let rho = Self::correl_last_n_ewma_huber_shrunk(&m1.returns, &m2.returns, CORRELATION_WINDOW);
                let row_a = corr.edges.entry(changed.to_string()).or_default();
                row_a.insert(other_id.clone(), PairCorr { rho, ts: now });
                let row_b = corr.edges.entry(other_id.clone()).or_default();
                row_b.insert(changed.to_string(), PairCorr { rho, ts: now });
            }
            corr.last_update = now;
        }
    }

    /// EWMA + Huber clipping + shrinkage correlation over last w samples (newest→older).
    /// λ≈0.94; Huber κ=3σ clamps outliers safely.
    #[inline(always)]
    fn correl_last_n_ewma_huber_shrunk(r1: &VecDeque<Decimal>, r2: &VecDeque<Decimal>, w: usize) -> Decimal {
        let n = r1.len().min(r2.len()).min(w);
        if n < 10 { return Decimal::ZERO; }

        // Pass 1: EWMA means/variances (for σ used by Huber)
        let lambda = EWMA_LAMBDA; // dec!(0.94)
        let one_minus_lambda = Decimal::ONE - lambda;

        let mut wx = Decimal::ZERO; let mut wy = Decimal::ZERO;
        let mut wxx = Decimal::ZERO; let mut wyy = Decimal::ZERO;
        let mut wsum = Decimal::ZERO;

        let mut wt = Decimal::ONE; // newest weight
        for i in 0..n {
            let ix1 = r1.len() - 1 - i;
            let ix2 = r2.len() - 1 - i;
            let x = r1[ix1];
            let y = r2[ix2];
            let wgt = one_minus_lambda * wt;

            wx  += wgt * x;  wy  += wgt * y;
            wxx += wgt * x * x;  wyy += wgt * y * y;
            wsum += wgt;
            wt *= lambda;
        }
        if wsum <= EPS { return Decimal::ZERO; }

        let mx = safe_div(wx, wsum, Decimal::ZERO);
        let my = safe_div(wy, wsum, Decimal::ZERO);
        let varx = (wxx - mx * mx * wsum).max(VAR_FLOOR);
        let vary = (wyy - my * my * wsum).max(VAR_FLOOR);
        let sigx = safe_sqrt(varx).max(VOL_FLOOR);
        let sigy = safe_sqrt(vary).max(VOL_FLOOR);

        #[inline(always)]
        fn huber(dev: Decimal, k: Decimal) -> Decimal {
            let ad = dev.abs();
            if ad <= k { dev } else {
                let s = if dev > Decimal::ZERO { Decimal::ONE } else if dev < Decimal::ZERO { -Decimal::ONE } else { Decimal::ZERO };
                s * k
            }
        }

        let kx = dec!(3) * sigx;
        let ky = dec!(3) * sigy;

        // Pass 2: EWMA covariance of Huberized, then shrink + cap
        let mut cov_w = Decimal::ZERO;
        let mut wt2 = Decimal::ONE;
        let mut wsum2 = Decimal::ZERO;
        for i in 0..n {
            let ix1 = r1.len() - 1 - i;
            let ix2 = r2.len() - 1 - i;
            let dx = huber(r1[ix1] - mx, kx);
            let dy = huber(r2[ix2] - my, ky);
            let wgt = one_minus_lambda * wt2;
            cov_w += wgt * dx * dy;
            wsum2 += wgt;
            wt2 *= lambda;
        }
        if wsum2 <= EPS { return Decimal::ZERO; }

        let cov = safe_div(cov_w, wsum2, Decimal::ZERO);
        let mut rho = safe_div(cov, sigx * sigy, Decimal::ZERO);
        if rho > Decimal::ONE { rho = Decimal::ONE; } else if rho < -Decimal::ONE { rho = -Decimal::ONE; }

        // Shrink toward 0 using effective mass ≈ 1/(1-λ)
        let eff_n = safe_div(Decimal::ONE, (Decimal::ONE - lambda).max(dec!(0.02)), Decimal::from(16));
        let shrink = (dec!(10) / eff_n).min(dec!(0.5));
        let rho_shrunk = rho * (Decimal::ONE - shrink);

        // Soft cap extreme |ρ|
        if rho_shrunk > MAX_CORRELATION { MAX_CORRELATION }
        else if rho_shrunk < -MAX_CORRELATION { -MAX_CORRELATION }
        else { rho_shrunk }
    }

    fn calculate_correlation(&self, returns1: &VecDeque<Decimal>, returns2: &VecDeque<Decimal>) -> Decimal {
        let n = returns1.len().min(returns2.len()).min(CORRELATION_WINDOW);
        if n < 10 {
            return Decimal::ZERO;
        }

        let r1: Vec<Decimal> = returns1.iter().rev().take(n).copied().collect();
        let r2: Vec<Decimal> = returns2.iter().rev().take(n).copied().collect();

        let mean1 = r1.iter().sum::<Decimal>() / Decimal::from(n);
        let mean2 = r2.iter().sum::<Decimal>() / Decimal::from(n);

        let mut cov = Decimal::ZERO;
        let mut var1 = Decimal::ZERO;
        let mut var2 = Decimal::ZERO;

        for i in 0..n {
            let dev1 = r1[i] - mean1;
            let dev2 = r2[i] - mean2;
            cov += dev1 * dev2;
            var1 += dev1 * dev1;
            var2 += dev2 * dev2;
        }

        if var1 > Decimal::ZERO && var2 > Decimal::ZERO {
            cov / (var1.sqrt() * var2.sqrt())
        } else {
            Decimal::ZERO
        }
    }

    #[inline(always)]
    fn rebalance_allocations(&self) -> Result<(), Box<dyn std::error::Error>> {
        use std::collections::HashMap;

        let metrics = rlock!(self.strategy_metrics);
        let total_capital = *rlock!(self.total_capital);
        let corr = rlock!(self.correlation_matrix);
        let now = now_unix();

        #[derive(Clone)]
        struct Base { sid: String, score: Decimal }
        let mut base: Vec<Base> = Vec::with_capacity(metrics.len());

        for (sid, sm) in metrics.iter() {
            if sm.returns.len() < MIN_OBSERVATIONS { continue; }
            if sm.regime_alert_until > 0 && now < sm.regime_alert_until { continue; }
            let csh = sm.get_confidence_adjusted_sharpe();
            if csh < MIN_SHARPE_THRESHOLD || sm.current_drawdown > MAX_DRAWDOWN_TOLERANCE { continue; }

            let elapsed_hours = ((now.saturating_sub(sm.last_update)) / 3600) as i32;
            let recency_weight = SHARPE_DECAY_FACTOR.powi(elapsed_hours.clamp(0, 24 * 180));
            let kelly = sm.get_kelly_fraction();
            let vol_penalty = safe_div(Decimal::ONE, (Decimal::ONE + sm.volatility.max(VOL_FLOOR) * dec!(10)), Decimal::ONE);
            let win_rate_bonus = sm.win_rate * sm.win_rate;

            let pre = csh * kelly * vol_penalty * win_rate_bonus * recency_weight;
            if pre > Decimal::ZERO { base.push(Base { sid: sid.clone(), score: pre }); }
        }

        if base.is_empty() { return Ok(()); }
        base.sort_by(|a, b| b.score.cmp(&a.score).then_with(|| a.sid.cmp(&b.sid)));

        let mut chosen: Vec<(String, Decimal)> = Vec::with_capacity(base.len());
        let mut final_scores: HashMap<String, Decimal> = HashMap::new();
        let mut total_score = Decimal::ZERO;

        for cand in base.into_iter() {
            let mut corr_factor = Decimal::ONE;
            for (other_sid, _) in chosen.iter() {
                if let Some(rho_eff) = self.decayed_rho(&corr, &cand.sid, other_sid, now) {
                    corr_factor *= (Decimal::ONE - rho_eff.abs() * dec!(0.5));
                    if rho_eff < Decimal::ZERO {
                        corr_factor *= (Decimal::ONE + (-rho_eff) * dec!(0.15));
                    }
                }
            }
            corr_factor = clamp01(corr_factor);
            let adj = cand.score * corr_factor;
            if adj > Decimal::ZERO {
                total_score += adj;
                final_scores.insert(cand.sid.clone(), adj);
                chosen.push((cand.sid, adj));
            }
        }

        if total_score <= EPS { return Ok(()); }

        drop(corr);
        drop(metrics);

        let risk_budget = *rlock!(self.risk_budget);
        let metrics = rlock!(self.strategy_metrics);
        let mut targets: HashMap<String, Decimal> = HashMap::new();

        for (sid, adj_score) in final_scores.iter() {
            let weight = safe_div(*adj_score, total_score, Decimal::ZERO);
            let target = total_capital * weight * dec!(0.95);
            if let Some(sm) = metrics.get(sid) {
                let var_contrib = target * sm.volatility.max(VOL_FLOOR);
                let rb = risk_budget * weight;
                if var_contrib <= rb { targets.insert(sid.clone(), target); }
                else {
                    let risk_adj = safe_div(rb, sm.volatility.max(VOL_FLOOR), Decimal::ZERO);
                    targets.insert(sid.clone(), risk_adj);
                }
            }
        }

        let current = rlock!(self.allocated_capital);
        let mut updated = current.clone();
        drop(current);

        #[inline(always)]
        fn smooth_allocation(cur: Decimal, tgt: Decimal, total_capital: Decimal) -> Decimal {
            let diff = tgt - cur;
            if diff.abs() <= total_capital * ALLOC_DEADBAND_FRAC { return cur; }
            let step_lin = diff * DYNAMIC_ADJUSTMENT_RATE;
            let cap = total_capital * ALLOC_MAX_STEP_FRAC;
            let step = if step_lin.abs() > cap {
                cap * (if step_lin > Decimal::ZERO { Decimal::ONE } else { -Decimal::ONE })
            } else { step_lin };
            (cur + step).max(Decimal::ZERO)
        }

        for (sid, target) in targets.into_iter() {
            let cur = updated.get(&sid).copied().unwrap_or(Decimal::ZERO);
            updated.insert(sid, smooth_allocation(cur, target, total_capital));
        }

        let total_allocated: Decimal = updated.values().copied().sum();
        if total_allocated > total_capital && total_allocated > Decimal::ZERO {
            let scale = safe_div(total_capital, total_allocated, Decimal::ONE);
            for v in updated.values_mut() { *v *= scale; }
        }

        *wlock!(self.allocated_capital) = updated;
        Ok(())
    }

    pub fn get_strategy_allocation(&self, strategy_id: &str) -> Decimal {
        rlock!(self.allocated_capital)
            .get(strategy_id)
            .copied()
            .unwrap_or(Decimal::ZERO)
    }

    #[inline(always)]
    pub fn get_portfolio_metrics(&self) -> PortfolioMetrics {
        use std::collections::HashMap;

        // Snapshot
        let metrics = rlock!(self.strategy_metrics);
        let allocations = rlock!(self.allocated_capital);
        let total_capital = *rlock!(self.total_capital);
        let corr = rlock!(self.correlation_matrix);
        let now = now_unix();

        // Normalized weights (book view)
        let weights_std: HashMap<String, Decimal> = allocations.iter()
            .map(|(id, alloc)| (id.clone(), safe_div(*alloc, total_capital.max(EPS), Decimal::ZERO)))
            .collect();

        // Base mean/variance with decayed ρ
        let mut portfolio_return = Decimal::ZERO;
        let mut portfolio_variance = Decimal::ZERO;
        let mut active_strategies = 0usize;
        let mut total_trades = 0u64;
        let mut total_wins = 0u64;

        for (sid, sm) in metrics.iter() {
            if let Some(&w) = weights_std.get(sid) {
                if w > Decimal::ZERO && sm.returns.len() >= MIN_OBSERVATIONS {
                    let mean = sm.ewma_return;
                    let vol  = sm.volatility.max(VOL_FLOOR);
                    portfolio_return  += w * mean;
                    portfolio_variance += w * w * vol * vol;
                    active_strategies += 1;
                    total_trades = total_trades.saturating_add(sm.total_trades);
                    total_wins   = total_wins.saturating_add(sm.winning_trades);
                }
            }
        }

        // Cross terms: age-decayed ρ
        let ids: Vec<&String> = weights_std.keys().collect();
        for i in 0..ids.len() {
            for j in (i+1)..ids.len() {
                let s1 = ids[i]; let s2 = ids[j];
                let w1 = *weights_std.get(s1).unwrap_or(&Decimal::ZERO);
                let w2 = *weights_std.get(s2).unwrap_or(&Decimal::ZERO);
                if w1 <= Decimal::ZERO || w2 <= Decimal::ZERO { continue; }
                if let (Some(m1), Some(m2)) = (metrics.get(s1), metrics.get(s2)) {
                    let v1 = m1.volatility.max(VOL_FLOOR);
                    let v2 = m2.volatility.max(VOL_FLOOR);
                    let rho_eff = self.decayed_rho(&corr, s1, s2, now).unwrap_or(Decimal::ZERO);
                    portfolio_variance += dec!(2) * w1 * w2 * rho_eff * v1 * v2;
                }
            }
        }

        // Conservative realized variance on co-timed series with Neumaier compensation; take max(model, realized)
        let mut weights_fast: FastMap<String, Decimal> = FastMap::default();
        for (k, v) in &weights_std { weights_fast.insert(k.clone(), *v); }
        let synced = self.build_synced_portfolio_returns(&metrics, &weights_fast);

        let realized_var = if synced.len() >= MIN_OBSERVATIONS {
            let mut mean = Decimal::ZERO;
            let mut m2   = Decimal::ZERO;
            let mut k    = Decimal::ZERO;
            let mut comp = Decimal::ZERO; // Neumaier compensation
            for &r in &synced {
                k += Decimal::ONE;
                let delta = r - mean;
                mean += safe_div(delta, k, Decimal::ZERO);
                let delta2 = r - mean;
                let term = delta * delta2;
                let t = m2 + term;
                if m2.abs() >= term.abs() { comp += (m2 - t) + term; } else { comp += (term - t) + m2; }
                m2 = t;
            }
            let denom = (Decimal::from(synced.len()) - Decimal::ONE).max(Decimal::ONE);
            safe_div(m2 + comp, denom, Decimal::ZERO).max(VAR_FLOOR)
        } else { Decimal::ZERO };

        let model_vol    = safe_sqrt(portfolio_variance).max(VOL_FLOOR);
        let realized_vol = safe_sqrt(realized_var).max(VOL_FLOOR);
        let port_vol     = if realized_vol > model_vol { realized_vol } else { model_vol };

        let port_sharpe = safe_div((portfolio_return - RISK_FREE_RATE), port_vol, Decimal::ZERO) * HOURS_PER_DAY;

        // Hybrid VaR(5%) on co-timed series (EWMA-weighted + CF, conservative)
        let (value_at_risk, _cvar_unused) = self.calculate_portfolio_var_cvar_synced(&metrics, &weights_fast);

        PortfolioMetrics {
            total_capital,
            allocated_capital: allocations.values().copied().sum(),
            portfolio_return,
            portfolio_volatility: port_vol,
            portfolio_sharpe: port_sharpe,
            active_strategies,
            total_trades,
            win_rate: if total_trades > 0 {
                safe_div(Decimal::from(total_wins), Decimal::from(total_trades), Decimal::ZERO)
            } else { Decimal::ZERO },
            value_at_risk,
        }
    }

    #[inline(always)]
    fn build_synced_portfolio_returns_ts(
        &self,
        metrics: &FastMap<String, StrategyMetrics>,
        weights: &FastMap<String, Decimal>,
    ) -> Vec<(u64, Decimal)> {
        // For each strategy, aggregate newest-window returns per timestamp (dedup within a ts)
        let mut per_sid_ts: FastMap<String, FastMap<u64, Decimal>> = FastMap::default();
        let mut any = false;

        per_sid_ts.reserve(metrics.len().saturating_mul(2));
        for (sid, m) in metrics.iter() {
            let w = weights.get(sid).copied().unwrap_or(Decimal::ZERO);
            if w <= Decimal::ZERO || m.returns.is_empty() { continue; }
            any = true;

            let len = m.returns.len().min(SHARPE_WINDOW);
            let start = m.returns.len().saturating_sub(len);
            let mut agg: FastMap<u64, Decimal> = FastMap::default();
            agg.reserve(len);
            for i in start..m.returns.len() {
                let ts = m.timestamps[i];
                let r  = m.returns[i];
                *agg.entry(ts).or_insert(Decimal::ZERO) += r; // sum same-second hits
            }
            per_sid_ts.insert(sid.clone(), agg);
        }
        if !any { return Vec::new(); }

        // Merge across strategies per ts: (Σ w_i r_i(ts)) / (Σ w_i present at ts)
        let mut bucket: FastMap<u64, (Decimal, Decimal)> = FastMap::default();
        bucket.reserve(per_sid_ts.len().saturating_mul(SHARPE_WINDOW));

        for (sid, agg) in per_sid_ts.into_iter() {
            let w = *weights.get(&sid).unwrap_or(&Decimal::ZERO);
            if w <= Decimal::ZERO { continue; }
            for (ts, r) in agg.into_iter() {
                let e = bucket.entry(ts).or_insert((Decimal::ZERO, Decimal::ZERO));
                e.0 += w * r;  // sum_wr
                e.1 += w;      // sum_w (count this strategy once at ts)
            }
        }
        if bucket.is_empty() { return Vec::new(); }

        // Deterministic ascending by timestamp
        let mut out = Vec::with_capacity(bucket.len());
        for (ts, (sum_wr, sum_w)) in bucket.into_iter() {
            if sum_w > EPS {
                out.push((ts, safe_div(sum_wr, sum_w, sum_wr)));
            }
        }
        out.sort_unstable_by(|a,b| a.0.cmp(&b.0));
        out
    }

    #[inline(always)]
    fn build_synced_portfolio_returns(
        &self,
        metrics: &FastMap<String, StrategyMetrics>,
        weights: &FastMap<String, Decimal>,
    ) -> Vec<Decimal> {
        let v = self.build_synced_portfolio_returns_ts(metrics, weights);
        let mut out = Vec::with_capacity(v.len());
        for (_ts, r) in v { out.push(r); }
        out
    }

    #[inline(always)]
    fn calculate_portfolio_var_synced(
        &self,
        metrics: &FastMap<String, StrategyMetrics>,
        weights: &FastMap<String, Decimal>,
    ) -> Decimal {
        let (var, _cvar) = self.calculate_portfolio_var_cvar_synced(metrics, weights);
        var
    }

    #[inline(always)]
    fn calculate_portfolio_var_cvar_synced(
        &self,
        metrics: &FastMap<String, StrategyMetrics>,
        weights: &FastMap<String, Decimal>,
    ) -> (Decimal, Decimal) {
        // Build timestamped, co-timed, de-dup’d, renormalized series
        let mut ts_rets = self.build_synced_portfolio_returns_ts(metrics, weights);
        if ts_rets.len() < MIN_OBSERVATIONS {
            return (Decimal::ZERO, Decimal::ZERO);
        }

        // Assumes ascending by timestamp; if not, sort to be safe
        ts_rets.sort_unstable_by(|a,b| a.0.cmp(&b.0));

        // EWMA weights via Δ timestamps
        let lambda = EWMA_LAMBDA;                   // 0 < λ < 1
        let one_minus_lambda = Decimal::ONE - lambda;
        let max_ts = ts_rets.last().unwrap().0;

        let mut wsum = Decimal::ZERO;
        let mut wvec: Vec<Decimal> = Vec::with_capacity(ts_rets.len());

        let mut last_age = max_ts.saturating_sub(ts_rets[0].0) as i32;
        last_age = last_age.clamp(0, 24*3600);
        let mut wi = one_minus_lambda * powi_decimal(lambda, last_age).max(EPS);
        wvec.push(wi); wsum += wi;

        for k in 1..ts_rets.len() {
            let prev = ts_rets[k-1].0;
            let cur  = ts_rets[k].0;
            let delta = (max_ts.saturating_sub(cur) as i32)
                          .saturating_sub(max_ts.saturating_sub(prev) as i32)
                          .clamp(-24*3600, 24*3600);
            let adj = powi_decimal(lambda, delta.abs()).max(EPS);
            wi = if delta >= 0 { wi * adj } else { safe_div(wi, adj, wi) };
            wi = (one_minus_lambda * safe_div(wi, one_minus_lambda, wi)).max(EPS);
            wvec.push(wi); wsum += wi;
        }

        if wsum <= EPS { return (Decimal::ZERO, Decimal::ZERO); }
        for w in &mut wvec { *w = safe_div(*w, wsum, *w); }

        // Deterministic tuples and sort by (ret asc, ts asc)
        let mut pairs: Vec<(Decimal, u64, Decimal)> = Vec::with_capacity(ts_rets.len());
        for (i, (ts, r)) in ts_rets.iter().enumerate() { pairs.push((*r, *ts, wvec[i])); }
        pairs.sort_unstable_by(|a,b| a.0.partial_cmp(&b.0).unwrap().then(a.1.cmp(&b.1)));

        // Weighted empirical VaR(α) and CVaR with fractional last bucket
        let alpha = dec!(0.05);
        let mut cum_w = Decimal::ZERO;
        let mut var_emp = Decimal::ZERO;
        let mut tail_mass = Decimal::ZERO;
        let mut tail_sum  = Decimal::ZERO;

        for (ret, _ts, w) in &pairs {
            let new_cum = cum_w + *w;
            if new_cum < alpha {
                tail_mass += *w;
                tail_sum  += *w * *ret;
                cum_w = new_cum;
            } else {
                let need = (alpha - cum_w).max(Decimal::ZERO);
                if need > Decimal::ZERO {
                    let frac = safe_div(need, *w, Decimal::ZERO).min(Decimal::ONE);
                    tail_mass += need;
                    tail_sum  += need * *ret;
                }
                var_emp = ret.abs();
                cum_w = new_cum;
                break;
            }
        }

        if tail_mass <= EPS {
            let (ret_min, _, w_min) = pairs[0];
            var_emp = ret_min.abs();
            tail_mass = w_min;
            tail_sum  = w_min * ret_min;
        }

        let cvar_emp = {
            let denom = alpha.max(tail_mass.max(EPS));
            let mean_tail = safe_div(tail_sum, denom, tail_sum);
            (-mean_tail).max(var_emp)
        };

        // Weighted, Huberized CF leg (same weights)
        let mut mean = Decimal::ZERO; for (r, _ts, w) in &pairs { mean += *w * *r; }
        let mut var_raw = Decimal::ZERO; for (r, _ts, w) in &pairs { let d = *r - mean; var_raw += *w * d * d; }
        var_raw = var_raw.max(VAR_FLOOR);
        let sig = safe_sqrt(var_raw).max(VOL_FLOOR);
        let inv_sig = safe_div(Decimal::ONE, sig, Decimal::ZERO);

        #[inline(always)]
        fn huber_zz(z: Decimal, k: Decimal) -> Decimal { if z > k { k } else if z < -k { -k } else { z } }
        let mut m2 = Decimal::ZERO; let mut m3 = Decimal::ZERO; let mut m4 = Decimal::ZERO;
        let kcap = HUBER_K_SIGMAS;
        for (r, _ts, w) in &pairs {
            let z = (*r - mean) * inv_sig;
            let zh = huber_zz(z, kcap);
            let zh2 = zh * zh;
            m2 += *w * zh2; m3 += *w * zh2 * zh; m4 += *w * zh2 * zh2;
        }
        let m2_pos = m2.max(VAR_FLOOR);
        let s_root = safe_sqrt(m2_pos).max(VOL_FLOOR);
        let mut S = safe_div(m3, m2_pos * s_root, Decimal::ZERO);
        let mut K = safe_div(m4, m2_pos * m2_pos, Decimal::ZERO) - dec!(3);
        if S > dec!(5) { S = dec!(5); } else if S < dec!(-5) { S = dec!(-5); }
        if K > dec!(20){ K = dec!(20);} else if K < dec!(-5) { K = dec!(-5); }

        let z  = dec!(-1.644853627);
        let z2 = z * z; let z3 = z2 * z;
        let z_cf = z + (z2 - Decimal::ONE) * S / dec!(6) + (z3 - dec!(3)*z) * K / dec!(24) - (dec!(2)*z3 - dec!(5)*z) * S * S / dec!(36);
        let q_cf = mean + sig * z_cf;
        let var_cf = (-q_cf).max(Decimal::ZERO);

        let var_hybrid = if var_cf > var_emp { var_cf } else { var_emp };
        let cvar_cons = cvar_emp.max(var_hybrid);
        (var_hybrid, cvar_cons)
    }

    pub fn get_strategy_allocation(&self, strategy_id: &str) -> Decimal {
        rlock!(self.allocated_capital)
            .get(strategy_id)
            .copied()
            .unwrap_or(Decimal::ZERO)
    }

    #[inline(always)]
    pub fn should_trade(&self, strategy_id: &str, expected_return: Decimal, expected_cost: Decimal) -> bool {
        use std::collections::HashMap;

        let metrics = rlock!(self.strategy_metrics);
        if let Some(sm) = metrics.get(strategy_id) {
            let now = now_unix();
            if sm.regime_alert_until > 0 && now < sm.regime_alert_until { return false; }
            if sm.current_drawdown > MAX_DRAWDOWN_TOLERANCE { return false; }

            let net = expected_return - expected_cost.max(COST_FLOOR);
            if net <= Decimal::ZERO { return false; }

            let csh = sm.get_confidence_adjusted_sharpe();
            if csh < MIN_SHARPE_THRESHOLD && sm.returns.len() >= MIN_OBSERVATIONS { return false; }

            // friction + half-σ with cost bias
            let mut req = sm.volatility.max(VOL_FLOOR) * dec!(0.5) + expected_cost * dec!(1.5);
            req *= (Decimal::ONE + sm.ewma_cost_rate).min(dec!(1.05));

            let allocations = rlock!(self.allocated_capital);
            let total_capital = *rlock!(self.total_capital);
            let weights: HashMap<String, Decimal> = allocations.iter()
                .map(|(id, alloc)| (id.clone(), safe_div(*alloc, total_capital.max(EPS), Decimal::ZERO)))
                .collect();
            drop(allocations);

            let corr = rlock!(self.correlation_matrix);
            let mut portfolio_rho = Decimal::ZERO;
            for (other, w) in &weights {
                if other == strategy_id { continue; }
                if let Some(rho_eff) = self.decayed_rho(&corr, strategy_id, other, now) {
                    portfolio_rho += *w * rho_eff;
                }
            }
            drop(corr);

            let port = self.get_portfolio_metrics();
            if portfolio_rho > dec!(0.80) && port.value_at_risk > dec!(0.02) {
                req *= dec!(1.25); // +25% hurdle under crowding & hot VaR
            }

            if expected_return < req { return false; }

            let alloc = weights.get(strategy_id).copied().unwrap_or(Decimal::ZERO) * total_capital;
            alloc >= MIN_TRADE_SIZE
        } else {
            expected_return > expected_cost.max(COST_FLOOR) * dec!(3)
        }
    }

    #[inline(always)]
    fn decayed_rho(&self, corr: &CorrelationMatrix, a: &str, b: &str, now: u64) -> Option<Decimal> {
        if let Some(row) = corr.edges.get(a) {
            if let Some(pc) = row.get(b) {
                let age_hours = ((now.saturating_sub(pc.ts)) / 3600) as i32;
                let decay = SHARPE_DECAY_FACTOR.powi(age_hours.clamp(0, 24 * 365));
                return Some(pc.rho * decay);
            }
        }
        None
    }

    #[inline(always)]
    fn weighted_portfolio_rho(
        &self,
        corr: &CorrelationMatrix,
        candidate: &str,
        weights: &std::collections::HashMap<String, Decimal>,
        now: u64,
    ) -> Decimal {
        let mut acc = Decimal::ZERO;
        for (other, w) in weights.iter() {
            if other == candidate { continue; }
            if let Some(rho_eff) = self.decayed_rho(corr, candidate, other, now) {
                acc += *w * rho_eff;
            }
        }
        acc
    }

    #[inline(always)]
    fn gain_loss_ratio(&self, returns: &VecDeque<Decimal>) -> Decimal {
        if returns.is_empty() { return Decimal::ONE; }
        let mut gains = Decimal::ZERO;
        let mut losses = Decimal::ZERO;
        for &r in returns {
            if r >= Decimal::ZERO { gains += r; } else { losses += -r; }
        }
        // Avoid divide-by-zero; neutral GLR=1 if no losses
        safe_div(gains.max(EPS), losses.max(EPS), Decimal::ONE)
    }

    pub fn update_capital(&self, new_capital: Decimal) -> Result<(), Box<dyn std::error::Error>> {
        if new_capital <= Decimal::ZERO { return Err("Capital must be positive".into()); }
        let old_capital = *rlock!(self.total_capital);
        let scale_factor = safe_div(new_capital, old_capital.max(EPS), Decimal::ONE);

        *wlock!(self.total_capital) = new_capital;
        *wlock!(self.risk_budget) = new_capital * self.max_var;

        let mut allocations = wlock!(self.allocated_capital);
        for allocation in allocations.values_mut() { *allocation *= scale_factor; }
        Ok(())
    }

    pub fn emergency_stop(&self, strategy_id: &str) {
        let mut allocations = wlock!(self.allocated_capital);
        allocations.insert(strategy_id.to_string(), Decimal::ZERO);
    }

    #[inline(always)]
    pub fn get_risk_metrics(&self, strategy_id: &str) -> Option<RiskMetrics> {
        let metrics = self.strategy_metrics.read().ok()?;
        metrics.get(strategy_id).map(|m| {
            let sortino = self.calculate_sortino_ratio(&m.returns);
            let max_loss_streak = self.calculate_max_consecutive_losses(&m.returns);
            let calmar = if m.current_drawdown > Decimal::ZERO {
                safe_div(m.sharpe_ratio, m.current_drawdown, m.sharpe_ratio)
            } else { m.sharpe_ratio };

            RiskMetrics {
                volatility: m.volatility,
                downside_volatility: self.calculate_downside_volatility(&m.returns),
                sharpe_ratio: m.sharpe_ratio,
                sortino_ratio: sortino,
                calmar_ratio: calmar,
                max_drawdown: m.current_drawdown,
                value_at_risk: self.calculate_strategy_var(&m.returns),
                conditional_var: self.calculate_cvar(&m.returns),
                win_rate: m.win_rate,
                profit_factor: if m.avg_loss > EPS {
                    safe_div(m.avg_win * m.win_rate, m.avg_loss * (Decimal::ONE - m.win_rate), Decimal::MAX)
                } else { Decimal::MAX },
                max_consecutive_losses: max_loss_streak,
            }
        })
    }

    #[inline(always)]
    fn calculate_sortino_ratio(&self, returns: &VecDeque<Decimal>) -> Decimal {
        let n = returns.len();
        if n < MIN_OBSERVATIONS { return Decimal::ZERO; }

        // One pass: Welford-style mean + Neumaier-compensated downside semivariance
        let mut mean = Decimal::ZERO;
        let mut k = Decimal::ZERO;

        let mut down_sum = Decimal::ZERO;
        let mut down_comp = Decimal::ZERO; // Neumaier compensation

        for &r in returns {
            k += Decimal::ONE;
            mean += safe_div(r - mean, k, Decimal::ZERO);

            let dr = r - RISK_FREE_RATE;
            if dr < Decimal::ZERO {
                let term = dr * dr;
                let t = down_sum + term;
                if down_sum.abs() >= term.abs() { down_comp += (down_sum - t) + term; }
                else                            { down_comp += (term - t) + down_sum; }
                down_sum = t;
            }
        }

        let dn = Decimal::from(n.max(1));
        let dvar = safe_div(down_sum + down_comp, dn, Decimal::ZERO).max(VAR_FLOOR);
        let dvol = safe_sqrt(dvar).max(VOL_FLOOR);

        safe_div((mean - RISK_FREE_RATE), dvol, Decimal::ZERO) * HOURS_PER_DAY
    }

    #[inline(always)]
    fn calculate_downside_volatility(&self, returns: &VecDeque<Decimal>) -> Decimal {
        if returns.is_empty() { return Decimal::ZERO; }
        let mut cnt = Decimal::ZERO;
        let mut sumsq = Decimal::ZERO;
        let mut comp  = Decimal::ZERO; // Neumaier compensation
        for &r in returns {
            if r < Decimal::ZERO {
                cnt += Decimal::ONE;
                let term = r * r;
                let t = sumsq + term;
                if sumsq.abs() >= term.abs() { comp += (sumsq - t) + term; }
                else                          { comp += (term - t) + sumsq; }
                sumsq = t;
            }
        }
        if cnt <= Decimal::ZERO { return Decimal::ZERO; }
        safe_sqrt(safe_div(sumsq + comp, cnt, Decimal::ZERO).max(VAR_FLOOR))
    }

    #[inline(always)]
    fn calculate_max_consecutive_losses(&self, returns: &VecDeque<Decimal>) -> u32 {
        let mut max_l = 0u32;
        let mut cur = 0u32;
        for &r in returns {
            if r < Decimal::ZERO { cur += 1; max_l = max_l.max(cur); } else { cur = 0; }
        }
        max_l
    }

    // ---- O(n) VaR using select_nth_unstable ----
    #[inline(always)]
    fn calculate_strategy_var(&self, returns: &VecDeque<Decimal>) -> Decimal {
        let m = returns.len();
        if m < MIN_OBSERVATIONS { return Decimal::ZERO; }

        // 1) Empirical VaR(5%) on newest SHARPE_WINDOW samples (O(n))
        let k = m.min(SHARPE_WINDOW);
        let mut v: Vec<Decimal> = returns.iter().rev().take(k).copied().collect();
        let q_idx = ((v.len() as f64) * 0.05).floor() as usize;
        let idx = q_idx.clamp(0, v.len().saturating_sub(1));
        v.select_nth_unstable(idx);
        let var_emp = v[idx].abs();

        // 2) Cornish–Fisher adjusted VaR(5%) on same window (parametric, robust-capped)
        let n = Decimal::from(v.len());
        let mean = safe_div(v.iter().copied().sum(), n, Decimal::ZERO);
        let mut m2 = Decimal::ZERO; let mut m3 = Decimal::ZERO; let mut m4 = Decimal::ZERO;
        for &x in &v {
            let d = x - mean;
            let d2 = d * d; let d3 = d2 * d; let d4 = d3 * d;
            m2 += d2; m3 += d3; m4 += d4;
        }
        let var = safe_div(m2, n.max(Decimal::ONE), Decimal::ZERO).max(VAR_FLOOR);
        let sig = safe_sqrt(var).max(VOL_FLOOR);
        // standardized moments
        let inv_sig3 = safe_div(Decimal::ONE, sig * sig * sig, Decimal::ZERO);
        let inv_sig4 = safe_div(Decimal::ONE, sig * sig * sig * sig, Decimal::ZERO);
        let mut S = safe_div(m3, n, Decimal::ZERO) * inv_sig3;
        let mut K = safe_div(m4, n, Decimal::ZERO) * inv_sig4 - dec!(3);
        // Robust caps
        if S > dec!(5) { S = dec!(5); } else if S < dec!(-5) { S = dec!(-5); }
        if K > dec!(20) { K = dec!(20); } else if K < dec!(-5) { K = dec!(-5); }

        // z for 5% lower tail
        let z = dec!(-1.644853627);
        let z2 = z * z;
        let z3 = z2 * z;
        // Cornish–Fisher expansion
        let z_cf = z
            + (z2 - Decimal::ONE) * S / dec!(6)
            + (z3 - dec!(3)*z) * K / dec!(24)
            - (dec!(2)*z3 - dec!(5)*z) * S * S / dec!(36);

        // lower-tail quantile (negative), turn into positive VaR magnitude
        let q_cf = mean + sig * z_cf;
        let var_cf = (-q_cf).max(Decimal::ZERO);

        if var_cf > var_emp { var_cf } else { var_emp }
    }

    // ---- O(n) CVaR using select_nth_unstable ----
    #[inline(always)]
    fn calculate_cvar(&self, returns: &VecDeque<Decimal>) -> Decimal {
        let m = returns.len();
        if m < MIN_OBSERVATIONS { return Decimal::ZERO; }
        let k = m.min(SHARPE_WINDOW);
        let mut v: Vec<Decimal> = returns.iter().rev().take(k).copied().collect();
        let q_idx = ((v.len() as f64) * 0.05).floor() as usize;
        let idx = q_idx.clamp(0, v.len().saturating_sub(1));
        v.select_nth_unstable(idx);
        let worst = &v[..idx];
        if worst.is_empty() { Decimal::ZERO } else {
            safe_div(worst.iter().copied().sum(), Decimal::from(worst.len()), Decimal::ZERO).abs()
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PortfolioMetrics {
    pub total_capital: Decimal,
    pub allocated_capital: Decimal,
    pub portfolio_return: Decimal,
    pub portfolio_volatility: Decimal,
    pub portfolio_sharpe: Decimal,
    pub active_strategies: usize,
    pub total_trades: u64,
    pub win_rate: Decimal,
    pub value_at_risk: Decimal,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RiskMetrics {
    pub volatility: Decimal,
    pub downside_volatility: Decimal,
    pub sharpe_ratio: Decimal,
    pub sortino_ratio: Decimal,
    pub calmar_ratio: Decimal,
    pub max_drawdown: Decimal,
    pub value_at_risk: Decimal,
    pub conditional_var: Decimal,
    pub win_rate: Decimal,
    pub profit_factor: Decimal,
    pub max_consecutive_losses: u32,
}

impl SharpeRatioMaximizer {
    pub fn get_optimal_strategies(&self, n: usize) -> Vec<(String, Decimal)> {
        use std::collections::HashMap;

        let metrics = rlock!(self.strategy_metrics);
        let allocations = rlock!(self.allocated_capital);
        let total_cap = *rlock!(self.total_capital);
        let weights: HashMap<String, Decimal> = allocations.iter()
            .map(|(id, alloc)| (id.clone(), safe_div(*alloc, total_cap.max(EPS), Decimal::ZERO)))
            .collect();
        drop(allocations);

        let corr = rlock!(self.correlation_matrix);
        let now = now_unix();

        let mut ranked: Vec<(String, Decimal)> = Vec::new();
        for (sid, sm) in metrics.iter() {
            if sm.returns.len() < MIN_OBSERVATIONS { continue; }
            if sm.regime_alert_until > 0 && now < sm.regime_alert_until { continue; }

            let csh = sm.get_confidence_adjusted_sharpe();
            if csh <= Decimal::ZERO { continue; }

            // Weighted correlation to current portfolio (age-decayed)
            let port_rho = self.weighted_portfolio_rho(&corr, sid, &weights, now);

            let dd = clamp01(sm.current_drawdown);
            let base = csh * (sm.win_rate * sm.win_rate) * powi_decimal(Decimal::ONE - dd, DRAWDOWN_DAMP_POWER);
            // GLR boost (centered at ~2.0): sqrt(clamp(GLR/2, 0.5, 1.5))
            let glr = self.gain_loss_ratio(&sm.returns);
            let glr_norm = safe_div(glr, dec!(2), Decimal::ONE).max(dec!(0.5)).min(dec!(1.5));
            let glr_boost = safe_sqrt(glr_norm);
            let hedge_boost = if port_rho < Decimal::ZERO {
                (Decimal::ONE + (-port_rho).min(Decimal::ONE) * dec!(0.15))
            } else { Decimal::ONE };
            // Diversification pressure to slightly prefer less crowded names
            let diversify = (Decimal::ONE - port_rho.abs() * dec!(0.15)).max(dec!(0.70));

            let score = base * glr_boost * hedge_boost * diversify;
            if score > Decimal::ZERO { ranked.push((sid.clone(), score)); }
        }
        drop(corr);

        ranked.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
        ranked.truncate(n);
        ranked
    }

    pub fn calculate_portfolio_efficiency(&self) -> Decimal {
        let portfolio_metrics = self.get_portfolio_metrics();
        let metrics = rlock!(self.strategy_metrics);
        let now = now_unix();

        let mut best = Decimal::ZERO;
        for m in metrics.values() {
            if m.returns.len() >= MIN_OBSERVATIONS && !(m.regime_alert_until > 0 && now < m.regime_alert_until) {
                let csh = m.get_confidence_adjusted_sharpe();
                if csh > best { best = csh; }
            }
        }
        if best > Decimal::ZERO {
            safe_div(portfolio_metrics.portfolio_sharpe, best, Decimal::ZERO)
        } else {
            Decimal::ZERO
        }
    }

    #[inline(always)]
    fn project_capped_simplex(mut w: Vec<Decimal>, cap: Decimal) -> Vec<Decimal> {
        // Project onto { w >= 0, w <= cap, sum w = 1 } via bisection on tau
        let mut lo = -cap;
        let mut hi = Decimal::ONE + cap;
        for _ in 0..40 {
            let mid = (lo + hi) * dec!(0.5);
            let mut s = Decimal::ZERO;
            for &wi in &w {
                let z = (wi - mid).max(Decimal::ZERO).min(cap);
                s += z;
            }
            if s > Decimal::ONE { lo = mid; } else { hi = mid; }
        }
        let tau = (lo + hi) * dec!(0.5);
        for wi in &mut w { *wi = (*wi - tau).max(Decimal::ZERO).min(cap); }
        let sumw: Decimal = w.iter().copied().sum();
        if sumw > EPS { for wi in &mut w { *wi = safe_div(*wi, sumw, *wi); } }
        w
    }

    pub fn optimize_portfolio_weights(&self) -> HashMap<String, Decimal> {
        use std::collections::HashMap;

        let metrics = rlock!(self.strategy_metrics);
        let total_cap = *rlock!(self.total_capital);
        let corr = rlock!(self.correlation_matrix);
        let now = now_unix();

        // Eligible strategies
        let mut ids: Vec<String> = Vec::new();
        let mut mu: Vec<Decimal> = Vec::new();
        let mut vol: Vec<Decimal> = Vec::new();
        for (id, m) in metrics.iter() {
            if m.returns.len() >= MIN_OBSERVATIONS &&
               m.get_confidence_adjusted_sharpe() >= MIN_SHARPE_THRESHOLD &&
               m.current_drawdown <= MAX_DRAWDOWN_TOLERANCE &&
               !(m.regime_alert_until > 0 && now < m.regime_alert_until)
            {
                ids.push(id.clone());
                mu.push(m.ewma_return - RISK_FREE_RATE);
                vol.push(m.volatility.max(VOL_FLOOR));
            }
        }
        let n = ids.len();
        if n == 0 { return HashMap::new(); }

        // Start from current normalized weights; fallback to uniform
        let allocs = rlock!(self.allocated_capital);
        let mut w_prev: Vec<Decimal> = Vec::with_capacity(n);
        let mut sumw = Decimal::ZERO;
        for id in &ids { let a = allocs.get(id).copied().unwrap_or(Decimal::ZERO); w_prev.push(a); sumw += a; }
        drop(allocs);
        if sumw > EPS { for wi in &mut w_prev { *wi = safe_div(*wi, sumw, *wi); } } else { w_prev = vec![Decimal::ONE / Decimal::from(n); n]; }
        let cap = MAX_POSITION_FRACTION;

        let lambda = dec!(0.5); // turnover regularization
        let mut w = w_prev.clone();
        let mut eta = dec!(0.05);
        let max_iter = 200;
        let micro_band = dec!(0.000001); // no-trade band in weight space

        let obj = |w: &Vec<Decimal>| -> Decimal {
            let mut quad = Decimal::ZERO;
            for i in 0..n {
                for j in 0..n {
                    let rho = if i == j { Some(Decimal::ONE) } else { self.decayed_rho(&corr, &ids[i], &ids[j], now) };
                    if let Some(r) = rho { quad += w[i] * w[j] * r * vol[i] * vol[j]; }
                }
            }
            let mut lin = Decimal::ZERO; for i in 0..n { lin += w[i] * mu[i]; }
            let mut tv = Decimal::ZERO; for i in 0..n { let d = w[i] - w_prev[i]; tv += d * d; }
            dec!(0.5) * quad - lin + dec!(0.5) * lambda * tv
        };

        let mut prev_obj = obj(&w);
        for _ in 0..max_iter {
            // Gradient: Σ w − μ + λ (w − w_prev)
            let mut g = vec![Decimal::ZERO; n];
            for i in 0..n {
                let mut sig_w = Decimal::ZERO;
                for j in 0..n {
                    let rho = if i == j { Some(Decimal::ONE) } else { self.decayed_rho(&corr, &ids[i], &ids[j], now) };
                    if let Some(r) = rho { sig_w += w[j] * r * vol[i] * vol[j]; }
                }
                g[i] = sig_w - mu[i] + lambda * (w[i] - w_prev[i]);
            }

            // Diagonal preconditioning and micro no-trade band
            let mut w_try = w.clone();
            for i in 0..n {
                let pre = (vol[i] * vol[i]) + lambda;
                let step = eta * safe_div(g[i], pre, g[i]);
                let mut cand = w[i] - step;
                if (cand - w_prev[i]).abs() < micro_band { cand = w_prev[i]; }
                w_try[i] = cand;
            }
            w_try = Self::project_capped_simplex(w_try, cap);

            let cur_obj = obj(&w_try);
            if cur_obj <= prev_obj {
                w = w_try; prev_obj = cur_obj; eta = (eta * dec!(1.05)).min(dec!(0.25));
            } else {
                eta = (eta * dec!(0.5)).max(dec!(0.001));
            }

            let mut diff = Decimal::ZERO; for i in 0..n { diff += (w[i] - w_try[i]).abs(); }
            if diff < dec!(0.000001) { break; }
        }

        // Build allocations with a 5% reserve
        let mut out = HashMap::new();
        for i in 0..n { out.insert(ids[i].clone(), total_cap * w[i] * dec!(0.95)); }
        out
    }
}

impl SharpeRatioMaximizer {
    pub fn get_strategy_recommendation(&self, strategy_id: &str, market_volatility: Decimal) -> StrategyRecommendation {
        use std::collections::HashMap;

        let metrics = rlock!(self.strategy_metrics);
        let allocations = rlock!(self.allocated_capital);
        let total_cap = *rlock!(self.total_capital);
        let current_allocation = allocations.get(strategy_id).copied().unwrap_or(Decimal::ZERO);

        if let Some(sm) = metrics.get(strategy_id) {
            let now = now_unix();
            let mut action = Action::Hold;

            // Base conditions
            if sm.regime_alert_until > 0 && now < sm.regime_alert_until {
                action = Action::Exit;
            } else if sm.current_drawdown > MAX_DRAWDOWN_TOLERANCE {
                action = Action::Exit;
            } else {
                let csh = sm.get_confidence_adjusted_sharpe();
                let vol_ratio = safe_div(sm.volatility, market_volatility.max(dec!(0.001)), Decimal::ONE);
                if csh < MIN_SHARPE_THRESHOLD {
                    action = Action::Reduce;
                } else if csh > dec!(2.0) && vol_ratio < dec!(1.5) {
                    action = Action::Increase;
                } else {
                    action = Action::Hold;
                }
            }

            // Risk clamp: ensure allocation won't exceed risk budget by VAR proxy
            let rb_total = *rlock!(self.risk_budget);
            let var_per_unit = sm.volatility.max(VOL_FLOOR); // conservative proxy
            let max_risk_alloc = safe_div(rb_total, var_per_unit, Decimal::ZERO)
                .min(total_cap * MAX_POSITION_FRACTION);

            // Crowding & VaR gating: raise target if book crowded/hot
            let weights: HashMap<String, Decimal> = allocations.iter()
                .map(|(id, alloc)| (id.clone(), safe_div(*alloc, total_cap.max(EPS), Decimal::ZERO)))
                .collect();
            drop(allocations);

            let corr = rlock!(self.correlation_matrix);
            let mut port_rho = Decimal::ZERO;
            for (other, w) in &weights {
                if other == strategy_id { continue; }
                if let Some(rho_eff) = self.decayed_rho(&corr, strategy_id, other, now) {
                    port_rho += *w * rho_eff;
                }
            }
            drop(corr);

            let port = self.get_portfolio_metrics();
            let crowding_factor = if port_rho > dec!(0.80) && port.value_at_risk > dec!(0.02) { dec!(1.25) } else { Decimal::ONE };

            // Target sizing anchored to Kelly, then smoothed + deadband on notional
            let kelly = sm.get_kelly_fraction();
            let base_target = (total_cap * kelly * POSITION_SCALE_FACTOR).min(max_risk_alloc);
            let target_after_crowding = safe_div(base_target, crowding_factor, base_target);
            let dead = total_cap * ALLOC_DEADBAND_FRAC;
            let raw_target = match action {
                Action::Exit => Decimal::ZERO,
                Action::Reduce => (current_allocation * dec!(0.5)).min(max_risk_alloc),
                Action::Increase => current_allocation + (target_after_crowding - current_allocation) * DYNAMIC_ADJUSTMENT_RATE,
                Action::Hold => current_allocation,
            }.min(max_risk_alloc);
            let recommended_size = if (raw_target - current_allocation).abs() <= dead {
                current_allocation
            } else {
                raw_target.max(MIN_TRADE_SIZE)
            };

            let confidence = sm.get_confidence_adjusted_sharpe() / dec!(3.0);
            let risk_score = sm.volatility * (Decimal::ONE + sm.current_drawdown);

            StrategyRecommendation {
                strategy_id: strategy_id.to_string(),
                action,
                current_allocation,
                recommended_allocation: recommended_size,
                confidence,
                risk_score,
            }
        } else {
            StrategyRecommendation {
                strategy_id: strategy_id.to_string(),
                action: Action::Hold,
                current_allocation,
                recommended_allocation: MIN_TRADE_SIZE,
                confidence: Decimal::ZERO,
                risk_score: Decimal::ONE,
            }
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Action {
    Increase,
    Hold,
    Reduce,
    Exit,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StrategyRecommendation {
    pub strategy_id: String,
    pub action: Action,
    pub current_allocation: Decimal,
    pub recommended_allocation: Decimal,
    pub confidence: Decimal,
    pub risk_score: Decimal,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum HealthStatus {
    Healthy,
    Warning,
    Critical,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SystemHealth {
    pub status: HealthStatus,
    pub active_strategies: usize,
    pub total_capital: Decimal,
    pub allocated_capital: Decimal,
    pub utilization: Decimal,
    pub average_sharpe: Decimal,
    pub max_drawdown: Decimal,
    pub timestamp: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sharpe_ratio_maximizer_initialization() {
        let maximizer = SharpeRatioMaximizer::new(dec!(100000), dec!(0.02));
        assert_eq!(*rlock!(maximizer.total_capital), dec!(100000));
        assert_eq!(*rlock!(maximizer.risk_budget), dec!(2000));
    }

    #[test]
    fn test_position_sizing() {
        let maximizer = SharpeRatioMaximizer::new(dec!(100000), dec!(0.02));
        let size = maximizer.get_position_size("test_strategy", dec!(10000));
        assert!(size >= Decimal::ZERO);
    }

    #[test]
    fn test_trade_recording_and_health() {
        let maximizer = SharpeRatioMaximizer::new(dec!(100000), dec!(0.02));
        let result = TradeResult {
            strategy_id: "test_strategy".to_string(),
            timestamp: 1_000_000,
            pnl: dec!(100),
            capital_deployed: dec!(1000),
            gas_cost: dec!(10),
            slippage: dec!(5),
        };
        assert!(maximizer.record_trade(result).is_ok());
        let health = maximizer.get_system_health();
        assert!(matches!(health.status, HealthStatus::Healthy | HealthStatus::Warning | HealthStatus::Critical));
    }

    #[test]
    fn test_optimize_portfolio_weights_runs() {
        let maximizer = SharpeRatioMaximizer::new(dec!(100000), dec!(0.02));
        // Minimal smoke test: empty eligible set returns empty map
        let w = maximizer.optimize_portfolio_weights();
        assert!(w.is_empty());
    }

    #[test]
    fn test_hybrid_var_is_conservative() {
        use rust_decimal_macros::dec;
        let srm = SharpeRatioMaximizer::new(dec!(100000), dec!(0.02));

        // Craft returns with a fat negative outlier at the end (newest)
        let mut r = VecDeque::new();
        for _ in 0..200 { r.push_back(dec!(0.0005)); }
        r.push_back(dec!(-0.03)); // tail event

        let v_emp = {
            let k = r.len().min(SHARPE_WINDOW);
            let mut v: Vec<Decimal> = r.iter().rev().take(k).copied().collect();
            let q_idx = ((v.len() as f64) * 0.05).floor() as usize;
            let idx = q_idx.clamp(0, v.len().saturating_sub(1));
            v.select_nth_unstable(idx);
            v[idx].abs()
        };
        let v_hyb = srm.calculate_strategy_var(&r);
        assert!(v_hyb >= v_emp - dec!(1e-12));
    }

    #[test]
    fn test_project_capped_simplex_properties() {
        use rust_decimal_macros::dec;
        let cap = dec!(0.3);
        let w = vec![dec!(0.8), dec!(0.2), dec!(-0.1), dec!(0.5)];
        let p = super::project_capped_simplex(w, cap);
        let sum: Decimal = p.iter().copied().sum();
        assert!((sum - Decimal::ONE).abs() < dec!(1e-9));
        for &wi in &p {
            assert!(wi >= Decimal::ZERO - dec!(1e-12));
            assert!(wi <= cap + dec!(1e-12));
        }
    }
}

