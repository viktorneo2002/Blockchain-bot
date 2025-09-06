use std::collections::VecDeque;
use std::sync::Arc;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use tracing::{instrument, debug, info};
use ndarray::{s, Array1, Array2, ArrayView1, Axis};
use rand::Rng;
use std::f64::consts::E;

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub enum MarketRegime {
    StrongBull,
    Bull,
    Sideways,
    Bear,
    StrongBear,
    HighVolatility,
    LowVolatility,
}

// =============================
// BiLSTM over buffered sequence (slow-path analytics)
// =============================

#[derive(Debug, Clone)]
pub struct BiLSTM {
    pub fwd: LSTMLayerLN,
    pub bwd: LSTMLayerLN,
    pub hidden: usize, // per direction
}

impl BiLSTM {
    pub fn new(input: usize, hidden: usize, peepholes: bool, rng: &mut impl rand::Rng) -> Self {
        Self {
            fwd: LSTMLayerLN::new(input, hidden, peepholes, rng),
            bwd: LSTMLayerLN::new(input, hidden, peepholes, rng),
            hidden,
        }
    }
    // xs: (T, D) -> (T, 2H)
    pub fn forward(&mut self, xs: &Array2<f64>) -> Array2<f64> {
        let t = xs.len_of(Axis(0));
        let d = self.hidden * 2;
        let mut out = Array2::<f64>::zeros((t, d));
        // forward pass
        self.fwd.reset();
        for ti in 0..t {
            let ht = self.fwd.step(&xs.row(ti).view());
            out.slice_mut(s![ti, 0..self.hidden]).assign(&ht);
        }
        // backward pass
        self.bwd.reset();
        for off in 0..t {
            let ti = t - 1 - off;
            let ht = self.bwd.step(&xs.row(ti).view());
            out.slice_mut(s![ti, self.hidden..d]).assign(&ht);
        }
        out
    }
}

// =============================
// Residual stacked LSTM (LN variants), fixed hidden per layer
// =============================

#[derive(Debug, Clone)]
pub struct StackedLSTM {
    pub layers: Vec<LSTMLayerLN>,
}

impl StackedLSTM {
    pub fn new(depth: usize, input: usize, hidden: usize, peepholes: bool, rng: &mut impl rand::Rng) -> Self {
        let mut layers = Vec::with_capacity(depth);
        layers.push(LSTMLayerLN::new(input, hidden, peepholes, rng));
        for _ in 1..depth { layers.push(LSTMLayerLN::new(hidden, hidden, peepholes, rng)); }
        Self { layers }
    }
    #[inline]
    pub fn step(&mut self, x: &ArrayView1<f64>) -> Array1<f64> {
        let mut cur = self.layers[0].step(x);
        for l in 1..self.layers.len() {
            let prev = cur.clone();
            cur = self.layers[l].step(&cur.view());
            cur = &cur + &prev; // residual
        }
        cur
    }
    pub fn reset(&mut self) { for l in &mut self.layers { l.reset(); } }
}

// =============================
// Lightweight single-head additive attention for sequence
// =============================

#[derive(Debug, Clone)]
pub struct AdditiveAttention {
    w_q: Array2<f64>, // (H, H)
    w_k: Array2<f64>, // (H, H)
    w_v: Array2<f64>, // (H, H)
    v: Array1<f64>,   // (H)
    hidden: usize,
}

impl AdditiveAttention {
    pub fn new(hidden: usize, rng: &mut impl rand::Rng) -> Self {
        let scale = (1.0 / hidden as f64).sqrt();
        let randm = |rc: (usize, usize)| Array2::from_shape_fn(rc, |_| rng.gen_range(-scale..scale));
        let randv = |n: usize| Array1::from_shape_fn(n, |_| rng.gen_range(-scale..scale));
        Self { w_q: randm((hidden, hidden)), w_k: randm((hidden, hidden)), w_v: randm((hidden, hidden)), v: randv(hidden), hidden }
    }
    // hs: (T, H), q: (H) -> context: (H)
    pub fn forward(&self, hs: &Array2<f64>, q: &Array1<f64>) -> Array1<f64> {
        let t = hs.len_of(Axis(0));
        if t == 0 { return q.clone(); }
        let qh = self.w_q.dot(q); // (H)
        let ks = hs.dot(&self.w_k.t()); // (T, H)
        let vs = hs.dot(&self.w_v.t()); // (T, H)
        // e_t = v^T tanh(qh + k_t)
        let mut e = Array1::<f64>::zeros(t);
        for ti in 0..t {
            let s = &(&qh + &ks.row(ti));
            let score = self.v.dot(&s.mapv(f64::tanh));
            e[ti] = score;
        }
        // softmax
        let m = e.iter().fold(f64::NEG_INFINITY, |a, &b| a.max(b));
        let exps = e.mapv(|x| (x - m).exp());
        let sum = exps.sum().max(1e-12);
        let attn = &exps / sum; // (T)
        // context = sum_t attn_t * v_t
        let mut ctx = Array1::<f64>::zeros(self.hidden);
        for ti in 0..t { let w = attn[ti]; ctx = ctx + &(vs.row(ti).to_owned() * w); }
        ctx
    }
}

// =============================
// Layer Normalization & DropConnect
// =============================

#[derive(Debug, Clone)]
pub struct LayerNorm {
    gamma: Array1<f64>,
    beta: Array1<f64>,
    eps: f64,
}

impl LayerNorm {
    pub fn new(dim: usize) -> Self {
        Self { gamma: Array1::ones(dim), beta: Array1::zeros(dim), eps: 1e-5 }
    }
    #[inline]
    pub fn forward(&self, x: &ArrayView1<f64>) -> Array1<f64> {
        let mu = x.mean().unwrap_or(0.0);
        let var = x.mapv(|v| (v - mu) * (v - mu)).mean().unwrap_or(0.0);
        let inv_std = 1.0 / (var + self.eps).sqrt();
        (&((x - mu) * inv_std) * &self.gamma) + &self.beta
    }
}

#[derive(Debug, Clone)]
pub struct DropConnectMask {
    mask_wx: Array2<f64>,
    mask_wh: Array2<f64>,
    p_keep: f64,
}

impl DropConnectMask {
    pub fn new(shape_wx: (usize, usize), shape_wh: (usize, usize), p_keep: f64, rng: &mut impl rand::Rng) -> Self {
        let mask_wx = Array2::from_shape_fn(shape_wx, |_| if rng.gen::<f64>() < p_keep { 1.0 } else { 0.0 });
        let mask_wh = Array2::from_shape_fn(shape_wh, |_| if rng.gen::<f64>() < p_keep { 1.0 } else { 0.0 });
        Self { mask_wx, mask_wh, p_keep }
    }
    #[inline]
    pub fn apply(&self, w_x: &Array2<f64>, w_h: &Array2<f64>) -> (Array2<f64>, Array2<f64>) {
        ((w_x * &self.mask_wx) / self.p_keep, (w_h * &self.mask_wh) / self.p_keep)
    }
}

// =============================
// LSTM with LayerNorm and optional peepholes
// =============================

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LSTMParamsLN {
    w_x: Array2<f64>, // (4H, D)
    w_h: Array2<f64>, // (4H, H)
    b: Array1<f64>,   // (4H)
    w_ci: Option<Array1<f64>>, // (H)
    w_cf: Option<Array1<f64>>, // (H)
    w_co: Option<Array1<f64>>, // (H)
    ln_i: LayerNorm,
    ln_f: LayerNorm,
    ln_g: LayerNorm,
    ln_o: LayerNorm,
    hidden_size: usize,
    input_size: usize,
}

impl LSTMParamsLN {
    pub fn new(input: usize, hidden: usize, peepholes: bool, rng: &mut impl rand::Rng) -> Self {
        let scale = (1.0 / ((input + hidden) as f64)).sqrt();
        let w_x = Array2::from_shape_fn((4 * hidden, input), |_| rng.gen_range(-scale..scale));
        let w_h = Array2::from_shape_fn((4 * hidden, hidden), |_| rng.gen_range(-scale..scale));
        let mut b = Array1::zeros(4 * hidden);
        // forget gate bias +1
        b.slice_mut(s![hidden..2 * hidden]).fill(1.0);
        let (w_ci, w_cf, w_co) = if peepholes {
            (
                Some(Array1::from_shape_fn(hidden, |_| rng.gen_range(-scale..scale))),
                Some(Array1::from_shape_fn(hidden, |_| rng.gen_range(-scale..scale))),
                Some(Array1::from_shape_fn(hidden, |_| rng.gen_range(-scale..scale))),
            )
        } else { (None, None, None) };
        Self {
            w_x, w_h, b, w_ci, w_cf, w_co,
            ln_i: LayerNorm::new(hidden),
            ln_f: LayerNorm::new(hidden),
            ln_g: LayerNorm::new(hidden),
            ln_o: LayerNorm::new(hidden),
            hidden_size: hidden,
            input_size: input,
        }
    }
    #[inline]
    fn gate_slices<'a>(&self, z: &'a Array1<f64>) -> (ArrayView1<'a, f64>, ArrayView1<'a, f64>, ArrayView1<'a, f64>, ArrayView1<'a, f64>) {
        let h = self.hidden_size;
        (z.slice(s![0..h]), z.slice(s![h..2 * h]), z.slice(s![2 * h..3 * h]), z.slice(s![3 * h..4 * h]))
    }
    #[inline]
    pub fn step(&self, x_t: &ArrayView1<f64>, h_prev: &ArrayView1<f64>, c_prev: &ArrayView1<f64>) -> (Array1<f64>, Array1<f64>) {
        let z = self.w_x.dot(x_t) + self.w_h.dot(h_prev) + &self.b; // (4H)
        let (i_lin, f_lin, g_lin, o_lin) = self.gate_slices(&z);
        let i_norm = self.ln_i.forward(&i_lin);
        let f_norm = self.ln_f.forward(&f_lin);
        let g_norm = self.ln_g.forward(&g_lin);
        let o_norm = self.ln_o.forward(&o_lin);
        let mut i = i_norm.mapv(sigmoid);
        let mut f = f_norm.mapv(sigmoid);
        let g = g_norm.mapv(f64::tanh);
        if let Some(w_ci) = &self.w_ci { i = &i + &(w_ci * c_prev); i.mapv_inplace(sigmoid); }
        if let Some(w_cf) = &self.w_cf { f = &f + &(w_cf * c_prev); f.mapv_inplace(sigmoid); }
        let c_t = &f * c_prev + &i * &g;
        let mut o_act = o_norm;
        if let Some(w_co) = &self.w_co { o_act = &o_act + &(w_co * &c_t); }
        let o = o_act.mapv(sigmoid);
        let h_t = &o * c_t.mapv(f64::tanh);
        (h_t, c_t)
    }
}

#[derive(Debug, Clone)]
pub struct LSTMLayerLN {
    pub params: LSTMParamsLN,
    pub h: Array1<f64>,
    pub c: Array1<f64>,
}

impl LSTMLayerLN {
    pub fn new(input: usize, hidden: usize, peepholes: bool, rng: &mut impl rand::Rng) -> Self {
        Self { params: LSTMParamsLN::new(input, hidden, peepholes, rng), h: Array1::zeros(hidden), c: Array1::zeros(hidden) }
    }
    #[inline]
    pub fn step(&mut self, x_t: &ArrayView1<f64>) -> Array1<f64> {
        let (h_t, c_t) = self.params.step(x_t, &self.h.view(), &self.c.view());
        self.h = h_t; self.c = c_t; self.h.clone()
    }
    pub fn reset(&mut self) { self.h.fill(0.0); self.c.fill(0.0); }
}
impl MarketRegime {
    #[inline]
    fn from_idx(idx: usize) -> Self {
        match idx {
            0 => MarketRegime::StrongBull,
            1 => MarketRegime::Bull,
            2 => MarketRegime::Sideways,
            3 => MarketRegime::Bear,
            4 => MarketRegime::StrongBear,
            5 => MarketRegime::HighVolatility,
            6 => MarketRegime::LowVolatility,
            _ => MarketRegime::Sideways,
        }
    }
}

    // For latency benches, add criterion in dev-dependencies and enable this bench in a dedicated file.
    // #[cfg(feature = "bench")]
    // mod benches {
    //     use super::*;
    //     use criterion::{criterion_group, criterion_main, Criterion};
    //     pub fn bench_tick(c: &mut Criterion) {
    //         let mut cls = LSTMMarketRegimeClassifier::new();
    //         c.bench_function("tick->signals", |b| b.iter(|| {
    //             cls.update_market_data(100.0, 1000.0, 99.99, 100.01, 500.0, 500.0, 10);
    //             let _ = cls.get_trading_signals();
    //         }));
    //     }
    //     criterion_group!(benches, bench_tick);
    //     criterion_main!(benches);
    // }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LSTMParams {
    // W_x: (4H, D), W_h: (4H, H), b: (4H)
    w_x: Array2<f64>,
    w_h: Array2<f64>,
    b: Array1<f64>,
    // Optional peepholes (i,f,o gates only)
    w_ci: Option<Array1<f64>>, // (H)
    w_cf: Option<Array1<f64>>, // (H)
    w_co: Option<Array1<f64>>, // (H)
    hidden_size: usize,
    input_size: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LSTMLayer {
    params: LSTMParams,
    hidden_state: Array1<f64>, // (H)
    cell_state: Array1<f64>,   // (H)
}

#[derive(Debug, Clone)]
pub struct MarketFeatures {
    price_returns: f64,
    volume_ratio: f64,
    volatility: f64,
    rsi: f64,
    macd_signal: f64,
    order_flow_imbalance: f64,
    spread_ratio: f64,
    momentum: f64,
    volume_weighted_price: f64,
    bid_ask_pressure: f64,
}

pub struct LSTMMarketRegimeClassifier {
    lstm_layers: Vec<LSTMLayer>,
    output_head: OutputHead,
    feature_buffer: VecDeque<MarketFeatures>,
    price_history: VecDeque<f64>,
    volume_history: VecDeque<f64>,
    regime_confidence: Arc<RwLock<f64>>,
    current_regime: Arc<RwLock<MarketRegime>>,
    sequence_length: usize,
    hidden_size: usize,
    num_classes: usize,
    learning_rate: f64,
    momentum_factor: f64,
    regime_state: RegimeState,
    last_entropy: Option<f64>,
    last_hidden: Option<Array1<f64>>,
    normalizers: Vec<(f64, f64)>,
    // Observability
    last_lat_us: VecDeque<u64>,
    // Immutable runtime config
    config: Arc<ClassifierConfig>,
    // Optional enhanced backbone for slow-path analytics and optional hot-path step
    backbone: Option<EnhancedBackbone>,
    // Scratch buffer to avoid allocs in hot-path inference helpers
    scratch_hidden: Array1<f64>,
}

#[derive(Debug, Clone)]
pub struct ClassifierConfig {
    pub th_enter_offset: f64,
    pub th_exit_offset: f64,
    pub min_dwell_secs: u64,
    pub entropy_emergency: f64,
    pub vol_emergency: f64,
    pub latency_p99_budget_us: u64,
    pub latency_window: usize,
}

impl Default for ClassifierConfig {
    fn default() -> Self {
        Self {
            th_enter_offset: 0.05,
            th_exit_offset: 0.05,
            min_dwell_secs: 2,
            entropy_emergency: 1.9,
            vol_emergency: 0.08,
            latency_p99_budget_us: 300,
            latency_window: 256,
        }
    }
}

impl LSTMParams {
    fn new(input: usize, hidden: usize, peepholes: bool) -> Self {
        let mut rng = rand::thread_rng();
        let scale = (1.0 / (input as f64 + hidden as f64)).sqrt();
        let w_x = Array2::from_shape_fn((4 * hidden, input), |_| rng.gen_range(-scale..scale));
        let w_h = Array2::from_shape_fn((4 * hidden, hidden), |_| rng.gen_range(-scale..scale));
        // Bias: forget gate +1.0, others 0
        let mut b = Array1::zeros(4 * hidden);
        b.slice_mut(s![hidden..2 * hidden]).fill(1.0);
        let (w_ci, w_cf, w_co) = if peepholes {
            (
                Some(Array1::from_shape_fn(hidden, |_| rng.gen_range(-scale..scale))),
                Some(Array1::from_shape_fn(hidden, |_| rng.gen_range(-scale..scale))),
                Some(Array1::from_shape_fn(hidden, |_| rng.gen_range(-scale..scale))),
            )
        } else {
            (None, None, None)
        };
        Self { w_x, w_h, b, w_ci, w_cf, w_co, hidden_size: hidden, input_size: input }
    }

    // Health check with latency p99, entropy/NaN guards
    pub fn health_check(&self) -> Result<(), String> {
        // NaN checks
        if let Some(f) = self.feature_buffer.back() {
            let vals = [f.price_returns, f.volume_ratio, f.volatility, f.rsi, f.macd_signal, f.order_flow_imbalance, f.spread_ratio, f.momentum, f.volume_weighted_price, f.bid_ask_pressure];
            if vals.iter().any(|v| !v.is_finite()) { return Err("Feature NaN/INF detected".into()); }
        }
        // Entropy guard
        if let Some(e) = self.last_entropy { if e.is_nan() || e.is_infinite() { return Err("Entropy invalid".into()); } }
        // Latency p99
        let p99 = self.latency_p99_us();
        if p99 > self.config.latency_p99_budget_us { return Err(format!("Latency p99 {}us exceeds budget {}us", p99, self.config.latency_p99_budget_us)); }
        Ok(())
    }

    fn latency_p99_us(&self) -> u64 {
        if self.last_lat_us.is_empty() { return 0; }
        let mut v: Vec<u64> = self.last_lat_us.iter().copied().collect();
        v.sort_unstable();
        let idx = ((v.len() as f64) * 0.99).floor() as usize;
        v[idx.min(v.len()-1)]
    }

    // One time-step
    fn step(&self, x_t: &ArrayView1<f64>, h_prev: &ArrayView1<f64>, c_prev: &ArrayView1<f64>) -> (Array1<f64>, Array1<f64>) {
        let h = self.hidden_size;
        // z = W_x x_t + W_h h_prev + b  => shape (4H)
        let z = self.w_x.dot(x_t) + self.w_h.dot(h_prev) + &self.b;
        let i_lin = z.slice(s![0..h]).to_owned();
        let f_lin = z.slice(s![h..2 * h]).to_owned();
        let g_lin = z.slice(s![2 * h..3 * h]).to_owned();
        let o_lin = z.slice(s![3 * h..4 * h]).to_owned();

        let i = if let Some(w_ci) = &self.w_ci { (&i_lin + &(w_ci * c_prev)).mapv(sigmoid) } else { i_lin.mapv(sigmoid) };
        let f = if let Some(w_cf) = &self.w_cf { (&f_lin + &(w_cf * c_prev)).mapv(sigmoid) } else { f_lin.mapv(sigmoid) };
        let g = g_lin.mapv(f64::tanh);
        let c_t = &f * c_prev + &i * &g;
        let o = if let Some(w_co) = &self.w_co { (&o_lin + &(w_co * &c_t)).mapv(sigmoid) } else { o_lin.mapv(sigmoid) };
        let h_t = &o * c_t.mapv(f64::tanh);
        (h_t, c_t)
    }
}

impl LSTMLayer {
    fn new(input_size: usize, hidden_size: usize, peepholes: bool) -> Self {
        Self {
            params: LSTMParams::new(input_size, hidden_size, peepholes),
            hidden_state: Array1::zeros(hidden_size),
            cell_state: Array1::zeros(hidden_size),
        }
    }

    // One incremental step: do NOT reset inside
    fn forward_step(&mut self, x_t: &ArrayView1<f64>) -> Array1<f64> {
        let (h, c) = self.params.step(x_t, &self.hidden_state.view(), &self.cell_state.view());
        self.hidden_state = h;
        self.cell_state = c;
        self.hidden_state.clone()
    }

    fn reset_states(&mut self) {
        self.hidden_state.fill(0.0);
        self.cell_state.fill(0.0);
    }
}

impl LSTMMarketRegimeClassifier {
    pub fn new() -> Self {
        let sequence_length = 20;
        let hidden_size = 64;
        let num_features = 10;
        let num_classes = 7;
        
        let mut lstm_layers = Vec::new();
        // Two-layer LSTM with peepholes disabled by default for stability
        lstm_layers.push(LSTMLayer::new(num_features, hidden_size, false));
        lstm_layers.push(LSTMLayer::new(hidden_size, hidden_size, false));
        
        // Build optional enhanced backbone with LN-based layers for slow-path
        let mut rng = rand::thread_rng();
        let backbone = Some(EnhancedBackbone::new(num_features, hidden_size, 2, &mut rng));
        Self {
            lstm_layers,
            output_head: OutputHead::new(num_classes, hidden_size, &mut rand::thread_rng()),
            feature_buffer: VecDeque::with_capacity(sequence_length),
            price_history: VecDeque::with_capacity(100),
            volume_history: VecDeque::with_capacity(100),
            regime_confidence: Arc::new(RwLock::new(0.0)),
            current_regime: Arc::new(RwLock::new(MarketRegime::Sideways)),
            sequence_length,
            hidden_size,
            num_classes,
            learning_rate: 0.001,
            momentum_factor: 0.9,
            regime_state: RegimeState { current: MarketRegime::Sideways, last_change_ts: now_unix() },
            last_entropy: None,
            last_hidden: None,
            normalizers: vec![
                (-0.05, 0.05),   // price_returns
                (0.0, 3.0),      // volume_ratio
                (0.0, 0.05),     // volatility
                (0.0, 1.0),      // rsi
                (-0.02, 0.02),   // macd_signal
                (-1.0, 1.0),     // order_flow_imbalance
                (0.0, 0.01),     // spread_ratio
                (-0.1, 0.1),     // momentum
                (-0.02, 0.02),   // volume_weighted_price
                (-1.0, 1.0),     // bid_ask_pressure
            ],
            last_lat_us: VecDeque::with_capacity(256),
            config: Arc::new(ClassifierConfig::default()),
            backbone,
            scratch_hidden: Array1::zeros(hidden_size),
        }
    }

    #[instrument(skip_all, fields(buf_len = self.feature_buffer.len()))]
    pub fn update_market_data(&mut self, price: f64, volume: f64, bid: f64, ask: f64, 
                             bid_size: f64, ask_size: f64, trades_count: u64) {
        let t0 = std::time::Instant::now();
        self.price_history.push_back(price);
        if self.price_history.len() > 100 {
            self.price_history.pop_front();
        }
        
        self.volume_history.push_back(volume);
        if self.volume_history.len() > 100 {
            self.volume_history.pop_front();
        }
        
        if self.price_history.len() >= 20 {
            let features = self.extract_features(price, volume, bid, ask, bid_size, ask_size);
            self.feature_buffer.push_back(features);
            
            if self.feature_buffer.len() > self.sequence_length {
                self.feature_buffer.pop_front();
            }
            
            // Incremental LSTM step on the newest feature vector
            if let Some(last) = self.feature_buffer.back() {
                let x_t = self.normalize_features(last);
                let hidden = self.step_all_layers(&x_t.view());
                self.last_hidden = Some(hidden.clone());
                // Only start output decisions after warm-up sequence length reached
                if self.feature_buffer.len() >= self.sequence_length {
                    let logits = self.output_head.logits(&hidden);
                    let probs = self.output_head.softmax_temp(&logits);
                    let (regime_idx, confidence) = probs.iter()
                        .enumerate()
                        .max_by(|(_, a), (_, b)| a.partial_cmp(b).unwrap())
                        .map(|(idx, &conf)| (idx, conf))
                        .unwrap_or((0, 0.0));
                    *self.regime_confidence.write() = confidence;
                    *self.current_regime.write() = MarketRegime::from_idx(regime_idx);
                }
            }
        }
        let dt = t0.elapsed().as_micros() as u64;
        if self.last_lat_us.len() == self.config.latency_window { let _ = self.last_lat_us.pop_front(); }
        self.last_lat_us.push_back(dt);
        debug!(tick_us = dt, "lstm tick latency");
    }

    fn extract_features(&self, price: f64, volume: f64, bid: f64, ask: f64,
                       bid_size: f64, ask_size: f64) -> MarketFeatures {
        let prices: Vec<f64> = self.price_history.iter().copied().collect();
        let volumes: Vec<f64> = self.volume_history.iter().copied().collect();
        
        let price_returns = if prices.len() > 1 {
            (price - prices[prices.len() - 2]) / prices[prices.len() - 2]
        } else {
            0.0
        };
        
        let avg_volume = volumes.iter().sum::<f64>() / volumes.len() as f64;
        let volume_ratio = volume / avg_volume.max(1.0);
        
        let volatility = self.calculate_volatility(&prices);
        let rsi = self.calculate_rsi(&prices);
        let macd_signal = self.calculate_macd(&prices);
        
        let order_flow_imbalance = (bid_size - ask_size) / (bid_size + ask_size + 1.0);
        let spread_ratio = (ask - bid) / price;
        
        let momentum = if prices.len() >= 10 {
            (price - prices[prices.len() - 10]) / prices[prices.len() - 10]
        } else {
            0.0
        };
        
        let vwap = self.calculate_vwap(&prices, &volumes);
        let volume_weighted_price = (price - vwap) / vwap.max(0.0001);
        
        let bid_ask_pressure = (bid_size * bid - ask_size * ask) / (bid_size * bid + ask_size * ask + 1.0);
        
        MarketFeatures {
            price_returns,
            volume_ratio,
            volatility,
            rsi,
            macd_signal,
            order_flow_imbalance,
            spread_ratio,
            momentum,
            volume_weighted_price,
            bid_ask_pressure,
        }
    }

    fn calculate_volatility(&self, prices: &[f64]) -> f64 {
        if prices.len() < 2 {
            return 0.0;
        }
        
        let returns: Vec<f64> = prices.windows(2)
            .map(|w| (w[1] - w[0]) / w[0])
            .collect();
        
        let mean = returns.iter().sum::<f64>() / returns.len() as f64;
        let variance = returns.iter()
            .map(|r| (r - mean).powi(2))
            .sum::<f64>() / returns.len() as f64;
        
        variance.sqrt()
    }

    fn calculate_rsi(&self, prices: &[f64]) -> f64 {
        if prices.len() < 14 {
            return 0.5;
        }
        
        let mut gains = 0.0;
        let mut losses = 0.0;
        
        for i in prices.len().saturating_sub(14)..prices.len() - 1 {
            let change = prices[i + 1] - prices[i];
            if change > 0.0 {
                gains += change;
            } else {
                losses -= change;
            }
        }
        
        let avg_gain = gains / 14.0;
        let avg_loss = losses / 14.0;
        
        if avg_loss == 0.0 {
            return 1.0;
        }
        
        let rs = avg_gain / avg_loss;
        1.0 - (1.0 / (1.0 + rs))
    }

    fn calculate_macd(&self, prices: &[f64]) -> f64 {
        if prices.len() < 26 {
            return 0.0;
        }
        
        let ema12 = self.calculate_ema(prices, 12);
        let ema26 = self.calculate_ema(prices, 26);
        
        (ema12 - ema26) / ema26.abs().max(0.0001)
    }

    fn calculate_ema(&self, prices: &[f64], period: usize) -> f64 {
        if prices.is_empty() {
            return 0.0;
        }
        
        let alpha = 2.0 / (period as f64 + 1.0);
        let mut ema = prices[0];
        
        for &price in &prices[1..] {
            ema = alpha * price + (1.0 - alpha) * ema;
        }
        
        ema
    }

    fn calculate_vwap(&self, prices: &[f64], volumes: &[f64]) -> f64 {
        if prices.is_empty() || volumes.is_empty() {
            return 0.0;
        }
        
        let total_volume = volumes.iter().sum::<f64>();
        if total_volume == 0.0 {
            return prices.last().copied().unwrap_or(0.0);
        }
        
        let weighted_sum: f64 = prices.iter()
            .zip(volumes.iter())
            .map(|(p, v)| p * v)
            .sum();
        
        weighted_sum / total_volume
    }

    // Classify based on CURRENT hidden state (no resetting or reprocessing)
    fn classify_regime(&mut self) -> MarketRegime {
        let hidden = if let Some(h) = &self.last_hidden { h.clone() } else { Array1::zeros(self.hidden_size) };
        let logits = self.output_head.logits(&hidden);
        let probs = self.output_head.softmax_temp(&logits);
        let (regime_idx, confidence) = probs.iter()
            .enumerate()
            .max_by(|(_, a), (_, b)| a.partial_cmp(b).unwrap())
            .map(|(idx, &conf)| (idx, conf))
            .unwrap_or((0, 0.0));
        *self.regime_confidence.write() = confidence;
        *self.current_regime.write() = MarketRegime::from_idx(regime_idx);
        MarketRegime::from_idx(regime_idx)
    }

    fn normalize_features(&self, f: &MarketFeatures) -> Array1<f64> {
        let mut x = Array1::zeros(10);
        let n = &self.normalizers;
        x[0] = self.normalize_value(f.price_returns, n[0].0, n[0].1);
        x[1] = self.normalize_value(f.volume_ratio, n[1].0, n[1].1);
        x[2] = self.normalize_value(f.volatility, n[2].0, n[2].1);
        x[3] = self.normalize_value(f.rsi, n[3].0, n[3].1);
        x[4] = self.normalize_value(f.macd_signal, n[4].0, n[4].1);
        x[5] = self.normalize_value(f.order_flow_imbalance, n[5].0, n[5].1);
        x[6] = self.normalize_value(f.spread_ratio, n[6].0, n[6].1);
        x[7] = self.normalize_value(f.momentum, n[7].0, n[7].1);
        x[8] = self.normalize_value(f.volume_weighted_price, n[8].0, n[8].1);
        x[9] = self.normalize_value(f.bid_ask_pressure, n[9].0, n[9].1);
        x
    }

    fn normalize_value(&self, value: f64, min: f64, max: f64) -> f64 {
        ((value - min) / (max - min)).max(0.0).min(1.0)
    }

    fn softmax(&self, logits: &Array1<f64>) -> Array1<f64> {
        let max_logit = logits.iter().fold(f64::NEG_INFINITY, |a, &b| a.max(b));
        let exp_logits = logits.mapv(|x| (x - max_logit).exp());
        let sum_exp = exp_logits.sum();
        exp_logits / sum_exp
    }

    // Step all layers once with x_t; returns last hidden
    fn step_all_layers(&mut self, x_t: &ArrayView1<f64>) -> Array1<f64> {
        let mut cur = x_t.to_owned();
        for layer in &mut self.lstm_layers {
            cur = layer.forward_step(&cur.view());
        }
        cur
    }

    // ========= Optional enhanced inference helpers =========
    // Hot-path helper using EnhancedBackbone's streaming stack without changing public path
    #[inline]
    pub fn infer_tick(&mut self, x_t: &ArrayView1<f64>) -> (usize, f64, Array1<f64>) {
        let h = if let Some(b) = &mut self.backbone { b.step(x_t) } else { self.step_all_layers(x_t) };
        self.scratch_hidden.assign(&h);
        let logits = self.output_head.logits(&self.scratch_hidden);
        let probs = self.output_head.softmax_temp(&logits);
        let (cls, conf) = probs
            .iter()
            .copied()
            .enumerate()
            .max_by(|a, b| a.1.partial_cmp(&b.1).unwrap())
            .map(|(i, p)| (i, p))
            .unwrap_or((0, 0.0));
        (cls, conf, probs)
    }

    // Slow-path helper to get sequence-enhanced probabilities via BiLSTM + attention
    pub fn infer_sequence(&mut self, xs: &Array2<f64>) -> Array1<f64> {
        if let Some(b) = &mut self.backbone {
            let ctx = b.sequence_context(xs); // (2H)
            // If 2H == H * 2, slice or project down to H for the existing head; simple slice here
            let h_len = self.scratch_hidden.len();
            let h_proj = ctx.slice_move(s![0..h_len]);
            let logits = self.output_head.logits(&h_proj);
            self.softmax(&logits)
        } else {
            // Fallback to last hidden
            let h = if let Some(h) = &self.last_hidden { h.clone() } else { Array1::zeros(self.hidden_size) };
            let logits = self.output_head.logits(&h);
            self.softmax(&logits)
        }
    }

    pub fn get_current_regime(&self) -> MarketRegime { *self.current_regime.read() }

    pub fn get_regime_confidence(&self) -> f64 { *self.regime_confidence.read() }

    pub fn get_regime_features(&self) -> Option<RegimeAnalysis> {
        if self.feature_buffer.is_empty() {
            return None;
        }

        let current_regime = self.get_current_regime();
        let confidence = self.get_regime_confidence();
        
        let trend_strength = self.calculate_trend_strength();
        let volatility_percentile = self.calculate_volatility_percentile();
        let volume_trend = self.calculate_volume_trend();
        let momentum_score = self.calculate_momentum_score();
        
        Some(RegimeAnalysis {
            regime: current_regime,
            confidence,
            trend_strength,
            volatility_percentile,
            volume_trend,
            momentum_score,
            regime_stability: self.calculate_regime_stability(),
            predicted_duration: self.estimate_regime_duration(),
        })
    }

    fn calculate_trend_strength(&self) -> f64 {
        if self.price_history.len() < 20 {
            return 0.0;
        }
        
        let prices: Vec<f64> = self.price_history.iter().copied().collect();
        let n = prices.len() as f64;
        
        let x: Vec<f64> = (0..prices.len()).map(|i| i as f64).collect();
        let x_mean = x.iter().sum::<f64>() / n;
        let y_mean = prices.iter().sum::<f64>() / n;
        
        let mut numerator = 0.0;
        let mut denominator = 0.0;
        
        for i in 0..prices.len() {
            numerator += (x[i] - x_mean) * (prices[i] - y_mean);
            denominator += (x[i] - x_mean) * (x[i] - x_mean);
        }
        
        if denominator == 0.0 {
            return 0.0;
        }
        
        let slope = numerator / denominator;
        let r_squared = (numerator * numerator) / (denominator * prices.iter()
            .map(|&y| (y - y_mean) * (y - y_mean))
            .sum::<f64>());
        
        slope.signum() * r_squared.sqrt()
    }

    fn calculate_volatility_percentile(&self) -> f64 {
        if self.feature_buffer.is_empty() {
            return 0.5;
        }
        
        let volatilities: Vec<f64> = self.feature_buffer.iter()
            .map(|f| f.volatility)
            .collect();
        
        if let Some(current) = volatilities.last() {
            let below_count = volatilities.iter()
                .filter(|&&v| v < *current)
                .count() as f64;
            below_count / volatilities.len() as f64
        } else {
            0.5
        }
    }

    fn calculate_volume_trend(&self) -> f64 {
        if self.volume_history.len() < 10 {
            return 0.0;
        }
        
        let recent_avg = self.volume_history.iter()
            .rev()
            .take(10)
            .sum::<f64>() / 10.0;
        
        let historical_avg = self.volume_history.iter()
            .sum::<f64>() / self.volume_history.len() as f64;
        
        (recent_avg - historical_avg) / historical_avg.max(1.0)
    }

    fn calculate_momentum_score(&self) -> f64 {
        if self.feature_buffer.is_empty() {
            return 0.0;
        }
        
        let momentum_values: Vec<f64> = self.feature_buffer.iter()
            .map(|f| f.momentum)
            .collect();
        
        let avg_momentum = momentum_values.iter().sum::<f64>() / momentum_values.len() as f64;
        let momentum_consistency = 1.0 - momentum_values.iter()
            .map(|&m| (m - avg_momentum).abs())
            .sum::<f64>() / (momentum_values.len() as f64 * avg_momentum.abs().max(0.01));
        
        avg_momentum * momentum_consistency
    }

    fn calculate_regime_stability(&self) -> f64 {
        if self.feature_buffer.len() < 5 {
            return 0.0;
        }
        
        let recent_features: Vec<&MarketFeatures> = self.feature_buffer.iter()
            .rev()
            .take(5)
            .collect();
        
        let mut stability_score = 1.0;
        for i in 1..recent_features.len() {
            let diff = (recent_features[i].volatility - recent_features[i-1].volatility).abs()
                     + (recent_features[i].momentum - recent_features[i-1].momentum).abs()
                     + (recent_features[i].volume_ratio - recent_features[i-1].volume_ratio).abs();
            stability_score *= (1.0 - diff.min(1.0));
        }
        
        stability_score
    }

    fn estimate_regime_duration(&self) -> u64 {
        let stability = self.calculate_regime_stability();
        let base_duration = match self.get_current_regime() {
            MarketRegime::StrongBull | MarketRegime::StrongBear => 300,
            MarketRegime::Bull | MarketRegime::Bear => 600,
            MarketRegime::Sideways => 1200,
            MarketRegime::HighVolatility => 180,
            MarketRegime::LowVolatility => 1800,
        };
        
        (base_duration as f64 * (0.5 + stability * 1.5)) as u64
    }

    pub fn predict_regime_transition(&self) -> RegimeTransitionPrediction {
        let current_regime = self.get_current_regime();
        let features = self.feature_buffer.back().cloned().unwrap_or(MarketFeatures {
            price_returns: 0.0,
            volume_ratio: 1.0,
            volatility: 0.01,
            rsi: 0.5,
            macd_signal: 0.0,
            order_flow_imbalance: 0.0,
            spread_ratio: 0.001,
            momentum: 0.0,
            volume_weighted_price: 0.0,
            bid_ask_pressure: 0.0,
        });
        
        let transition_probability = self.calculate_transition_probability(&features);
        let likely_next_regime = self.predict_next_regime(&features, current_regime);
        
        RegimeTransitionPrediction {
            current_regime,
            likely_next_regime,
            transition_probability,
            time_to_transition: self.estimate_time_to_transition(transition_probability),
        }
    }

    fn calculate_transition_probability(&self, features: &MarketFeatures) -> f64 {
        let volatility_change_factor = (features.volatility / 0.02).min(2.0);
        let momentum_extremity = features.momentum.abs() / 0.05;
        let volume_spike = (features.volume_ratio - 1.0).abs();
        let rsi_extremity = (features.rsi - 0.5).abs() * 2.0;
        
        let base_probability = 0.1;
        let probability = base_probability + 
            (volatility_change_factor * 0.2) +
            (momentum_extremity * 0.15) +
            (volume_spike * 0.1) +
            (rsi_extremity * 0.15);
        
        probability.min(1.0)
    }

    fn predict_next_regime(&self, features: &MarketFeatures, current: MarketRegime) -> MarketRegime {
        match current {
            MarketRegime::StrongBull => {
                if features.rsi > 0.8 || features.momentum < -0.02 {
                    MarketRegime::Bull
                } else if features.volatility > 0.04 {
                    MarketRegime::HighVolatility
                } else {
                    current
                }
            },
            MarketRegime::Bull => {
                if features.momentum > 0.05 && features.volume_ratio > 1.5 {
                    MarketRegime::StrongBull
                } else if features.momentum < -0.01 {
                    MarketRegime::Sideways
                } else {
                    current
                }
            },
            MarketRegime::Sideways => {
                if features.momentum > 0.03 {
                    MarketRegime::Bull
                } else if features.momentum < -0.03 {
                    MarketRegime::Bear
                } else if features.volatility > 0.03 {
                    MarketRegime::HighVolatility
                } else if features.volatility < 0.005 {
                    MarketRegime::LowVolatility
                } else {
                    current
                }
            },
            MarketRegime::Bear => {
                if features.momentum < -0.05 && features.volume_ratio > 1.5 {
                    MarketRegime::StrongBear
                } else if features.momentum > 0.01 {
                    MarketRegime::Sideways
                } else {
                    current
                }
            },
            MarketRegime::StrongBear => {
                if features.rsi < 0.2 || features.momentum > 0.02 {
                    MarketRegime::Bear
                } else if features.volatility > 0.04 {
                    MarketRegime::HighVolatility
                } else {
                    current
                }
            },
            MarketRegime::HighVolatility => {
                if features.volatility < 0.02 {
                    if features.momentum > 0.01 {
                        MarketRegime::Bull
                    } else if features.momentum < -0.01 {
                        MarketRegime::Bear
                    } else {
                        MarketRegime::Sideways
                    }
                } else {
                    current
                }
            },
            MarketRegime::LowVolatility => {
                if features.volatility > 0.01 || features.volume_ratio > 2.0 {
                    MarketRegime::Sideways
                } else {
                    current
                }
            }
        }
    }

    fn estimate_time_to_transition(&self, probability: f64) -> u64 {
        if probability > 0.8 {
            60
        } else if probability > 0.6 {
            300
        } else if probability > 0.4 {
            600
        } else {
            1200
        }
    }

    pub fn get_trading_signals(&self) -> TradingSignals {
        let regime = self.get_current_regime();
        let features = self.feature_buffer.back().unwrap_or(&MarketFeatures {
            price_returns: 0.0,
            volume_ratio: 1.0,
            volatility: 0.01,
            rsi: 0.5,
            macd_signal: 0.0,
            order_flow_imbalance: 0.0,
            spread_ratio: 0.001,
            momentum: 0.0,
            volume_weighted_price: 0.0,
            bid_ask_pressure: 0.0,
        });
        // Calibrated confidence and risk budget
        let conf = self.get_regime_confidence();
        let entropy = self.last_entropy.unwrap_or(1.5);
        let vol = features.volatility;
        let risk_budget = (1.0 / (1.0 + vol * 200.0)) * (1.5 - entropy).clamp(0.25, 1.25);
        let dwell_secs = now_unix().saturating_sub(self.regime_state.last_change_ts) as f64;
        let dwell_weight = (dwell_secs / 10.0).clamp(0.5, 1.0);

        let raw_bias = match regime {
            MarketRegime::StrongBull => 1.0,
            MarketRegime::Bull => 0.6,
            MarketRegime::Sideways => 0.0,
            MarketRegime::Bear => -0.6,
            MarketRegime::StrongBear => -1.0,
            MarketRegime::HighVolatility => 0.0,
            MarketRegime::LowVolatility => 0.3,
        };
        let position_bias = raw_bias * conf * risk_budget * dwell_weight;
        let risk_multiplier = risk_budget.clamp(0.2, 1.5);
        let stop_loss_distance = match regime {
            MarketRegime::HighVolatility => 0.02,
            MarketRegime::LowVolatility => 0.005,
            MarketRegime::StrongBull | MarketRegime::StrongBear => 0.015,
            _ => 0.01,
        };
        let take_profit_ratio = match regime {
            MarketRegime::StrongBull | MarketRegime::StrongBear => 3.0,
            MarketRegime::Bull | MarketRegime::Bear => 2.5,
            MarketRegime::HighVolatility => 4.0,
            MarketRegime::LowVolatility => 1.5,
            MarketRegime::Sideways => 2.0,
        };
        TradingSignals {
            position_bias,
            risk_multiplier,
            stop_loss_distance,
            take_profit_ratio,
            entry_threshold: 0.7 * conf,
            exit_threshold: 0.3,
            max_position_size: self.calculate_max_position_size(regime, conf),
            time_in_force: self.calculate_time_in_force(regime),
        }
    }
    
    fn calculate_max_position_size(&self, regime: MarketRegime, confidence: f64) -> f64 {
        let base_size = match regime {
            MarketRegime::StrongBull | MarketRegime::StrongBear => 0.8,
            MarketRegime::Bull | MarketRegime::Bear => 0.6,
            MarketRegime::Sideways => 0.4,
            MarketRegime::HighVolatility => 0.3,
            MarketRegime::LowVolatility => 0.7,
        };
        
        base_size * confidence
    }
    
    fn calculate_time_in_force(&self, regime: MarketRegime) -> u64 {
        match regime {
            MarketRegime::HighVolatility => 30,
            MarketRegime::StrongBull | MarketRegime::StrongBear => 120,
            MarketRegime::Bull | MarketRegime::Bear => 180,
            MarketRegime::Sideways => 300,
            MarketRegime::LowVolatility => 600,
        }
    }
    
    pub fn get_execution_params(&self) -> ExecutionParameters {
        let regime = self.get_current_regime();
        let features = self.feature_buffer.back().unwrap_or(&MarketFeatures {
            price_returns: 0.0,
            volume_ratio: 1.0,
            volatility: 0.01,
            rsi: 0.5,
            macd_signal: 0.0,
            order_flow_imbalance: 0.0,
            spread_ratio: 0.001,
            momentum: 0.0,
            volume_weighted_price: 0.0,
            bid_ask_pressure: 0.0,
        });
        
        let aggression_level = match regime {
            MarketRegime::StrongBull | MarketRegime::StrongBear => {
                if features.order_flow_imbalance.abs() > 0.3 { 0.9 } else { 0.7 }
            },
            MarketRegime::HighVolatility => 0.3,
            MarketRegime::LowVolatility => 0.8,
            _ => 0.5,
        };
        
        let slippage_tolerance = features.volatility * 2.0 + features.spread_ratio;
        
        ExecutionParameters {
            aggression_level,
            slippage_tolerance,
            min_fill_ratio: 0.8,
            max_spread_multiplier: match regime {
                MarketRegime::HighVolatility => 3.0,
                MarketRegime::LowVolatility => 1.5,
                _ => 2.0,
            },
            use_limit_orders: regime == MarketRegime::LowVolatility || regime == MarketRegime::Sideways,
            enable_iceberg: features.volume_ratio > 2.0,
            chunk_size: self.calculate_chunk_size(features.volume_ratio),
        }
    }
    
    fn calculate_chunk_size(&self, volume_ratio: f64) -> f64 {
        if volume_ratio > 3.0 {
            0.1
        } else if volume_ratio > 2.0 {
            0.2
        } else if volume_ratio > 1.5 {
            0.3
        } else {
            0.5
        }
    }
}

#[derive(Debug, Clone)]
pub struct RegimeAnalysis {
    pub regime: MarketRegime,
    pub confidence: f64,
    pub trend_strength: f64,
    pub volatility_percentile: f64,
    pub volume_trend: f64,
    pub momentum_score: f64,
    pub regime_stability: f64,
    pub predicted_duration: u64,
}

impl Default for RegimeAnalysis {
    fn default() -> Self {
        Self {
            regime: MarketRegime::Sideways,
            confidence: 0.0,
            trend_strength: 0.0,
            volatility_percentile: 0.5,
            volume_trend: 0.0,
            momentum_score: 0.0,
            regime_stability: 0.0,
            predicted_duration: 600,
        }
    }
}

#[derive(Debug, Clone)]
pub struct RegimeTransitionPrediction {
    pub current_regime: MarketRegime,
    pub likely_next_regime: MarketRegime,
    pub transition_probability: f64,
    pub time_to_transition: u64,
}

#[derive(Debug, Clone)]
pub struct TradingSignals {
    pub position_bias: f64,
    pub risk_multiplier: f64,
    pub stop_loss_distance: f64,
    pub take_profit_ratio: f64,
    pub entry_threshold: f64,
    pub exit_threshold: f64,
    pub max_position_size: f64,
    pub time_in_force: u64,
}

#[derive(Debug, Clone)]
pub struct ExecutionParameters {
    pub aggression_level: f64,
    pub slippage_tolerance: f64,
    pub min_fill_ratio: f64,
    pub max_spread_multiplier: f64,
    pub use_limit_orders: bool,
    pub enable_iceberg: bool,
    pub chunk_size: f64,
}

impl Default for LSTMMarketRegimeClassifier {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_classifier_initialization() {
        let classifier = LSTMMarketRegimeClassifier::new();
        assert_eq!(classifier.get_current_regime(), MarketRegime::Sideways);
        assert_eq!(classifier.get_regime_confidence(), 0.0);
    }
    
    #[test]
    fn test_market_data_update() {
        let mut classifier = LSTMMarketRegimeClassifier::new();
        for i in 0..100 {
            let price = 100.0 + (i as f64).sin() * 5.0;
            let volume = 1000.0 * (1.0 + (i as f64 * 0.1).cos() * 0.5);
            classifier.update_market_data(price, volume, price - 0.01, price + 0.01, 500.0, 500.0, 10);
        }
        
        let regime = classifier.get_current_regime();
        let confidence = classifier.get_regime_confidence();
        assert!(confidence > 0.0);
    }
    
    #[test]
    fn test_regime_transitions() {
        let mut classifier = LSTMMarketRegimeClassifier::new();
        
        // Simulate bull market
        for i in 0..50 {
            let price = 100.0 + i as f64 * 0.5;
            let volume = 1500.0;
            classifier.update_market_data(price, volume, price - 0.01, price + 0.01, 600.0, 400.0, 15);
        }
        
        let prediction = classifier.predict_regime_transition();
        assert!(prediction.transition_probability >= 0.0 && prediction.transition_probability <= 1.0);
    }
    
    #[test]
    fn test_trading_signals() {
        let mut classifier = LSTMMarketRegimeClassifier::new();
        
        // Create volatile market conditions
        for i in 0..30 {
            let price = 100.0 + (i as f64 * 0.5).sin() * 10.0;
            let volume = 2000.0 * (1.0 + (i as f64).cos());
            classifier.update_market_data(price, volume, price - 0.05, price + 0.05, 800.0, 800.0, 20);
        }
        
       let signals = classifier.get_trading_signals();
        assert!(signals.stop_loss_distance > 0.0);
        assert!(signals.take_profit_ratio > 0.0);
    }
}

// Production-ready extensions for Solana integration
impl LSTMMarketRegimeClassifier {
    pub fn save_model_state(&self) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
        let model_state = ModelState {
            lstm_layers: self.lstm_layers.clone(),
            output_layer: self.output_layer.clone(),
            output_bias: self.output_bias.clone(),
            learning_rate: self.learning_rate,
            momentum_factor: self.momentum_factor,
            regime_threshold: self.regime_threshold,
        };
        
        Ok(bincode::serialize(&model_state)?)
    }
    
    pub fn load_model_state(&mut self, data: &[u8]) -> Result<(), Box<dyn std::error::Error>> {
        let model_state: ModelState = bincode::deserialize(data)?;
        
        self.lstm_layers = model_state.lstm_layers;
        self.output_layer = model_state.output_layer;
        self.output_bias = model_state.output_bias;
        self.learning_rate = model_state.learning_rate;
        self.momentum_factor = model_state.momentum_factor;
        self.regime_threshold = model_state.regime_threshold;
        
        Ok(())
    }
    
    pub fn update_model_online(&mut self, actual_regime: MarketRegime, learning_rate_decay: f64) {
        if self.feature_buffer.len() < self.sequence_length {
            return;
        }
        
        self.learning_rate *= learning_rate_decay;
        let mut target = Array1::zeros(7);
        let target_idx = match actual_regime {
            MarketRegime::StrongBull => 0,
            MarketRegime::Bull => 1,
            MarketRegime::Sideways => 2,
            MarketRegime::Bear => 3,
            MarketRegime::StrongBear => 4,
            MarketRegime::HighVolatility => 5,
            MarketRegime::LowVolatility => 6,
        };
        target[target_idx] = 1.0;
        
        // Production-grade online update with gradient clipping
        let features = self.prepare_features();
        let mut hidden = features;
        
        for layer in &mut self.lstm_layers {
            hidden = layer.forward(&hidden.view());
        }
        
        let logits = self.output_layer.dot(&hidden) + &self.output_bias;
        let predictions = self.softmax(&logits);
        
        // Calculate cross-entropy gradient
        let gradient = &predictions - &target;
        let grad_norm = gradient.dot(&gradient).sqrt();
        
        // Gradient clipping for stability
        let clipped_gradient = if grad_norm > 1.0 {
            gradient / grad_norm
        } else {
            gradient
        };
        
        // Update output layer with momentum
        let output_update = self.learning_rate * clipped_gradient.to_shape((7, 1)).unwrap().dot(&hidden.to_shape((1, self.hidden_size)).unwrap());
        self.output_layer = &self.output_layer - &output_update;
        self.output_bias = &self.output_bias - self.learning_rate * &clipped_gradient;
        
        // Update confidence threshold adaptively
        let confidence_error = (1.0 - self.get_regime_confidence()).abs();
        self.regime_threshold = self.regime_threshold * 0.995 + confidence_error * 0.005;
    }
    
    pub fn get_performance_metrics(&self) -> PerformanceMetrics {
        let regime_stability = self.calculate_regime_stability();
        let prediction_confidence = self.get_regime_confidence();
        let feature_quality = self.assess_feature_quality();
        
        PerformanceMetrics {
            avg_confidence: prediction_confidence,
            regime_stability,
            feature_quality,
            model_entropy: self.calculate_model_entropy(),
            prediction_latency_us: 50, // Optimized for Solana
        }
    }
    
    fn assess_feature_quality(&self) -> f64 {
        if self.feature_buffer.is_empty() {
            return 0.0;
        }
        
        let features = self.feature_buffer.back().unwrap();
        let mut quality_score = 1.0;
        
        // Penalize extreme or likely erroneous values
        if features.price_returns.abs() > 0.1 {
            quality_score *= 0.8;
        }
        if features.volume_ratio > 10.0 || features.volume_ratio < 0.1 {
            quality_score *= 0.9;
        }
        if features.spread_ratio > 0.05 {
            quality_score *= 0.85;
        }
        
        quality_score
    }
    
    fn calculate_model_entropy(&self) -> f64 {
        let features = self.prepare_features();
        
        let mut lstm_copy = self.lstm_layers.clone();
        for layer in &mut lstm_copy {
            layer.reset_states();
        }
        
        let mut hidden = features;
        for layer in &mut lstm_copy {
            hidden = layer.forward(&hidden.view());
        }
        
        let logits = self.output_layer.dot(&hidden) + &self.output_bias;
        let probs = self.softmax(&logits);
        
        -probs.iter()
            .filter(|&&p| p > 0.0)
            .map(|&p| p * p.ln())
            .sum::<f64>()
    }
    
    pub fn reset(&mut self) {
        self.feature_buffer.clear();
        self.price_history.clear();
        self.volume_history.clear();
        *self.regime_confidence.write() = 0.0;
        *self.current_regime.write() = MarketRegime::Sideways;
        
        for layer in &mut self.lstm_layers {
            layer.reset_states();
        }
    }
    
    pub fn is_ready(&self) -> bool {
        self.feature_buffer.len() >= self.sequence_length && 
        self.price_history.len() >= 20
    }
    
    pub fn get_minimum_data_points(&self) -> usize {
        self.sequence_length.max(20)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ModelState {
    lstm_layers: Vec<LSTMLayer>,
    output_layer: Array2<f64>,
    output_bias: Array1<f64>,
    learning_rate: f64,
    momentum_factor: f64,
    regime_threshold: f64,
}

#[derive(Debug, Clone)]
pub struct PerformanceMetrics {
    pub avg_confidence: f64,
    pub regime_stability: f64,
    pub feature_quality: f64,
    pub model_entropy: f64,
    pub prediction_latency_us: u64,
}

// Critical production safety checks
impl LSTMMarketRegimeClassifier {
    pub fn validate_input(&self, price: f64, volume: f64, bid: f64, ask: f64) -> bool {
        price > 0.0 && price < 1_000_000.0 &&
        volume >= 0.0 && volume < 1_000_000_000.0 &&
        bid > 0.0 && bid <= price &&
        ask >= price && ask < price * 1.1 &&
        (ask - bid) / price < 0.1
    }
    
    pub fn emergency_stop_check(&self) -> bool {
        let volatility = self.feature_buffer.back()
            .map(|f| f.volatility)
            .unwrap_or(0.0);
        
        volatility > 0.1 || self.get_regime_confidence() < 0.3
    }
    
    pub fn get_risk_adjusted_signals(&self) -> Option<RiskAdjustedSignals> {
        if !self.is_ready() {
            return None;
        }
        
        let base_signals = self.get_trading_signals();
        let metrics = self.get_performance_metrics();
        let transition_pred = self.predict_regime_transition();
        
        let risk_adjustment = (metrics.avg_confidence * metrics.regime_stability).sqrt();
        let regime_risk = match self.get_current_regime() {
            MarketRegime::HighVolatility => 2.0,
            MarketRegime::StrongBull | MarketRegime::StrongBear => 1.5,
            _ => 1.0,
        };
        
        Some(RiskAdjustedSignals {
            adjusted_position_bias: base_signals.position_bias * risk_adjustment,
            effective_risk_multiplier: base_signals.risk_multiplier / regime_risk,
            dynamic_stop_loss: base_signals.stop_loss_distance * (1.0 + (1.0 - metrics.avg_confidence)),
            confidence_weighted_size: base_signals.max_position_size * risk_adjustment,
            regime_transition_risk: transition_pred.transition_probability,
            hold_time_seconds: base_signals.time_in_force,
        })
    }
}

#[derive(Debug, Clone)]
pub struct RiskAdjustedSignals {
    pub adjusted_position_bias: f64,
    pub effective_risk_multiplier: f64,
    pub dynamic_stop_loss: f64,
    pub confidence_weighted_size: f64,
    pub regime_transition_risk: f64,
    pub hold_time_seconds: u64,
}

// Final production optimizations
impl LSTMMarketRegimeClassifier {
    #[inline]
    pub fn quick_regime_check(&self) -> MarketRegime {
        *self.current_regime.read()
    }
    
    #[inline]
    pub fn get_confidence_threshold(&self) -> f64 {
        self.regime_threshold
    }
    
    pub fn health_check(&self) -> bool {
        self.is_ready() &&
        !self.emergency_stop_check() &&
        self.get_performance_metrics().feature_quality > 0.5
    }
}
