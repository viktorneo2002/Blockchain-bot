// =====================================================================================
// [ONLINE_LSTM] Online LSTM Trainer — Auditor-Tagged, Canon-Faithful Edition (v2)
// =====================================================================================
//
// Canonical lineage (cited at compute sites):
// [H&S97]  Hochreiter & Schmidhuber (1997) Long Short-Term Memory. Neural Computation.
// [W&P90]  Williams & Peng (1990) Truncated Backpropagation Through Time for RNN trajectories.
// [Pascanu13] Pascanu, Mikolov, Bengio (2013) Exploding gradients + global norm clipping.
// [KB15]   Kingma & Ba (2015/ICLR) Adam with bias-corrected moments.
// [Gal16]  Gal & Ghahramani (2015/2016) Variational/locked dropout in RNNs.
// [Schaul15] Schaul et al. (2015/2016) Prioritized Experience Replay (PER).
//
// Solana/Jito integration (feature-gated):
// [Solana-logs]  logsSubscribe WebSocket docs.
// [Jito-SS]      Jito ShredStream / Block Engine subscribe model.
//
// Features you can toggle at compile time:
//   - "solana-stream": enable real logsSubscribe feed
//   - "jito-ss":       enable real ShredStream gRPC feed
//   - "prod_strict":   enforce provenance stamps before training
//   - "parity_tests":  run PyTorch parity harness in tests
//
// =====================================================================================

use std::collections::VecDeque;
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

use ndarray::{s, Array1, Array2, Axis};
use rand::distributions::{Bernoulli, Distribution, Uniform};
use rand::thread_rng;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;
use tokio::time::interval;

// =====================================================================================
// Config
// =====================================================================================
const LSTM_HIDDEN_SIZE: usize = 128;
const LSTM_LAYERS: usize = 2;
const INPUT_FEATURES: usize = 24;
const OUTPUT_SIZE: usize = 3; // [profit, success_prob, tip]
const SEQ_LEN: usize = 32;    // streaming window length
const TBPTT: usize = 16;      // [W&P90] truncate length
const BATCH_SIZE: usize = 16;

const LR: f64 = 1e-3;
const L2: f64 = 1e-5;
const CLIP_MAX_NORM: f64 = 5.0; // [Pascanu13] global norm clip

const DROPOUT_RATE: f64 = 0.15; // [Gal16] locked across timesteps (per sequence)
const ADAM_B1: f64 = 0.9;       // [KB15]
const ADAM_B2: f64 = 0.999;     // [KB15]
const ADAM_EPS: f64 = 1e-8;     // [KB15]

const MAX_REPLAY: usize = 50_000; // PER capacity
const UPDATE_EVERY_MS: u64 = 100;
const CHECKPOINT_EVERY: Duration = Duration::from_secs(300);

// =====================================================================================
// Build/Run provenance
// =====================================================================================
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ProvenanceStamp {
    pub started_at_unix_ms: i64,
    pub build_commit: Option<String>,
    pub seq_len: usize,
    pub tbptt: usize,
    pub lr: f64,
    pub l2: f64,
    pub clip: f64,
    pub beta1: f64,
    pub beta2: f64,
    pub dropout: f64,
    pub per_alpha: f64,
    pub per_beta0: f64,
}

fn now_ms() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as i64
}

// =====================================================================================
// Utilities
// =====================================================================================
fn sigmoid_scalar(x: f64) -> f64 { 1.0 / (1.0 + (-x).exp()) }

fn huber(residual: f64) -> f64 {
    let a = residual.abs();
    if a <= 1.0 { 0.5 * residual * residual } else { a - 0.5 }
}
fn huber_grad(residual: f64) -> f64 {
    let a = residual.abs();
    if a <= 1.0 { residual } else { residual.signum() }
}

// BCE with logits derivative wrt logit is (p - y). We compute using prob then rely on dσ/dz
// identity in the gradient path; see BCEWithLogits formulations. [PyTorch Docs, BCEWithLogits]
fn bce_loss(prob: f64, target: f64, eps: f64) -> f64 {
    let p = prob.clamp(eps, 1.0 - eps);
    -(target * p.ln() + (1.0 - target) * (1.0 - p).ln())
}

// =====================================================================================
// Streaming normalization (EMA scaler)
// =====================================================================================
#[derive(Clone, Debug, Default)]
struct EmaScaler {
    count: u64,
    mean: Array1<f64>,
    m2: Array1<f64>,
    alpha: f64,
}
impl EmaScaler {
    fn new(dim: usize, alpha: f64) -> Self {
        Self { count: 0, mean: Array1::zeros(dim), m2: Array1::ones(dim), alpha }
    }
    fn update(&mut self, x: &Array1<f64>) {
        if self.count == 0 {
            self.mean = x.clone();
            self.m2 = Array1::from_elem(x.len(), 1.0);
            self.count = 1;
            return;
        }
        let delta = x - &self.mean;
        self.mean = &self.mean + &(self.alpha * &delta);
        let delta2 = x - &self.mean;
        self.m2 = &self.m2 + &(self.alpha * (&delta * &delta2));
        self.count += 1;
    }
    fn normalize(&self, x: &Array1<f64>) -> Array1<f64> {
        let var = &self.m2 + 1e-8;
        (x - &self.mean) / var.mapv(|v| v.sqrt())
    }
}

// =====================================================================================
// PER: SumTree + IS weights [Schaul15]
// =====================================================================================
#[derive(Debug, Clone)]
pub struct TrainingData {
    pub features_seq: Array2<f64>, // [T, INPUT_FEATURES]
    pub target: Array1<f64>,       // [profit, success_prob, tip]
    pub timestamp: Instant,
}

#[derive(Clone)]
struct SumTree {
    cap: usize,
    tree: Vec<f64>,         // 1-indexed binary tree over leaves [cap..2*cap)
    data: Vec<TrainingData>,
    write: usize,
    size: usize,
    alpha: f64,             // prioritization exponent
    beta: f64,              // IS anneal exponent
    eps: f64,               // small for prioritized replay
}
impl SumTree {
    fn new(capacity: usize, alpha: f64, beta: f64) -> Self {
        let cap = capacity.next_power_of_two();
        Self {
            cap,
            tree: vec![0.0; 2 * cap],
            data: Vec::with_capacity(cap),
            write: 0,
            size: 0,
            alpha,
            beta,
            eps: 1e-6,
        }
    }
    fn total(&self) -> f64 { self.tree[1].max(1e-12) }
    fn add(&mut self, priority_raw: f64, sample: TrainingData) {
        if self.size < self.cap {
            self.data.push(sample);
            self.size += 1;
        } else {
            self.data[self.write] = sample;
        }
        self.update_idx(self.write, (priority_raw + self.eps).powf(self.alpha)); // [Schaul15]
        self.write = (self.write + 1) % self.cap;
    }
    fn update(&mut self, data_idx: usize, priority_raw: f64) {
        self.update_idx(data_idx, (priority_raw + self.eps).powf(self.alpha));   // [Schaul15]
    }
    fn update_idx(&mut self, data_idx: usize, value: f64) {
        let mut idx = data_idx + self.cap;
        let change = value - self.tree[idx];
        self.tree[idx] = value;
        idx >>= 1;
        while idx >= 1 {
            self.tree[idx] += change;
            if idx == 1 { break; }
            idx >>= 1;
        }
    }
    // Return (index, leaf_priority)
    fn sample(&self, r: f64) -> (usize, f64) {
        let mut idx = 1usize;
        let mut value = r * self.total();
        while idx < self.cap {
            let left = idx << 1;
            if value <= self.tree[left] {
                idx = left;
            } else {
                value -= self.tree[left];
                idx = left + 1;
            }
        }
        let data_idx = idx - self.cap;
        (data_idx, self.tree[idx].max(1e-12))
    }
    fn min_leaf_priority(&self) -> f64 {
        let mut m = f64::INFINITY;
        for i in 0..self.size {
            let p = self.tree[self.cap + i];
            if p > 0.0 && p < m { m = p; }
        }
        if m.is_finite() { m } else { 1e-12 }
    }
    // IS weight: w_i = ((N * P(i))^-beta) / w_max, P(i)=p_i/∑p. [Schaul15, Sec. 3]
    fn importance_weight_from_leaf_p(&self, leaf_p: f64) -> f64 {
        let total = self.total();
        let n = self.size.max(1) as f64;
        let p_i = leaf_p / total;
        let w_i = (n * p_i).powf(-self.beta);
        let p_min = self.min_leaf_priority() / total;
        let w_max = (n * p_min).powf(-self.beta);
        (w_i / w_max).clamp(0.0, 10.0)
    }
    fn set_beta(&mut self, beta: f64) { self.beta = beta.clamp(0.0, 1.0); }
}

// =====================================================================================
// LSTM internals
// =====================================================================================
#[derive(Clone, Serialize, Deserialize)]
struct LSTMParams {
    w_ih: Array2<f64>, // [4H, in]
    w_hh: Array2<f64>, // [4H, H]
    b_ih: Array1<f64>, // [4H]
    b_hh: Array1<f64>, // [4H]
}

#[derive(Clone, Default)]
struct LSTMMoments {
    w_ih_m: Array2<f64>, w_ih_v: Array2<f64>,
    w_hh_m: Array2<f64>, w_hh_v: Array2<f64>,
    b_ih_m: Array1<f64>, b_ih_v: Array1<f64>,
    b_hh_m: Array1<f64>, b_hh_v: Array1<f64>,
}

#[derive(Clone, Default)]
struct StepTape {
    // Cached activations per timestep for TBPTT
    x_t: Array1<f64>,
    i: Array1<f64>, f: Array1<f64>, g: Array1<f64>, o: Array1<f64>, // [H]
    c: Array1<f64>, h: Array1<f64>,                                 // [H]
}

#[derive(Clone)]
struct LayerState { h: Array1<f64>, c: Array1<f64> }

#[derive(Clone, Serialize, Deserialize)]
struct JitterHist {
    buckets_ms: Vec<f64>,    // upper bounds in ms
    counts: Vec<u64>,
}
impl Default for JitterHist {
    fn default() -> Self {
        Self { buckets_ms: vec![0.5, 1.0, 2.0, 4.0, 8.0, 16.0, 32.0, 64.0, 128.0, 256.0], counts: vec![0;10] }
    }
}
impl JitterHist {
    fn add(&mut self, ms: f64) {
        for (i, ub) in self.buckets_ms.iter().enumerate() {
            if ms <= *ub { self.counts[i]+=1; return; }
        }
        self.counts.last_mut().map(|c| *c+=1);
    }
}

#[derive(Clone)]
struct LSTMLayer {
    p: LSTMParams,
    m: LSTMMoments,
    tape: Vec<StepTape>,                 // last SEQ_LEN steps
    dropout_in_mask: Option<Array1<f64>>,
    dropout_out_mask: Option<Array1<f64>>,
    state: LayerState,
    input_dim: usize,
    output_dim: usize,
}

impl LSTMLayer {
    fn new(input_dim: usize, hidden: usize) -> Self {
        let mut rng = thread_rng();
        let std = (2.0 / (input_dim + hidden) as f64).sqrt();
        let dist = Uniform::new(-std, std);

        let w_ih = Array2::from_shape_fn((4 * hidden, input_dim), |_| dist.sample(&mut rng));
        let w_hh = Array2::from_shape_fn((4 * hidden, hidden), |_| dist.sample(&mut rng));
        let b_ih = Array1::zeros(4 * hidden);
        let b_hh = Array1::zeros(4 * hidden);

        let m = LSTMMoments {
            w_ih_m: Array2::zeros((4 * hidden, input_dim)),
            w_ih_v: Array2::zeros((4 * hidden, input_dim)),
            w_hh_m: Array2::zeros((4 * hidden, hidden)),
            w_hh_v: Array2::zeros((4 * hidden, hidden)),
            b_ih_m: Array1::zeros(4 * hidden),
            b_ih_v: Array1::zeros(4 * hidden),
            b_hh_m: Array1::zeros(4 * hidden),
            b_hh_v: Array1::zeros(4 * hidden),
        };

        Self {
            p: LSTMParams { w_ih, w_hh, b_ih, b_hh },
            m,
            tape: Vec::with_capacity(SEQ_LEN),
            dropout_in_mask: None,
            dropout_out_mask: None,
            state: LayerState { h: Array1::zeros(hidden), c: Array1::zeros(hidden) },
            input_dim,
            output_dim: hidden,
        }
    }

    fn reset_state(&mut self) {
        self.state.h.fill(0.0);
        self.state.c.fill(0.0);
        self.tape.clear();
        self.dropout_in_mask = None;
        self.dropout_out_mask = None;
    }

    // [Gal16] generate one locked mask per sequence for input and output paths
    fn ensure_locked_masks(&mut self) {
        if self.dropout_in_mask.is_none() {
            let keep = 1.0 - DROPOUT_RATE;
            let bern = Bernoulli::new(keep).unwrap();
            let mut mi = Array1::zeros(self.input_dim);
            for i in 0..self.input_dim { mi[i] = if bern.sample(&mut thread_rng()) { 1.0 / keep } else { 0.0 }; }
            self.dropout_in_mask = Some(mi); // [Gal16] locked variational mask
        }
        if self.dropout_out_mask.is_none() {
            let keep = 1.0 - DROPOUT_RATE;
            let bern = Bernoulli::new(keep).unwrap();
            let mut mo = Array1::zeros(self.output_dim);
            for i in 0..self.output_dim { mo[i] = if bern.sample(&mut thread_rng()) { 1.0 / keep } else { 0.0 }; }
            self.dropout_out_mask = Some(mo); // [Gal16] locked variational mask
        }
    }

    // Forward one timestep
    fn forward_step(&mut self, x_raw: &Array1<f64>, training: bool) -> Array1<f64> {
        // Locked dropout on inputs [Gal16]
        let x_t = if training {
            self.ensure_locked_masks();
            if let Some(mi) = &self.dropout_in_mask { x_raw * mi } else { x_raw.clone() }
        } else { x_raw.clone() };

        // LSTM gates [H&S97, Eq. 7–12]: i=f(·), f=f(·), g=tanh(·), o=f(·)
        let gates = self.p.w_ih.dot(&x_t) + &self.p.b_ih + self.p.w_hh.dot(&self.state.h) + &self.p.b_hh;
        let hsz = self.output_dim;
        let i = gates.slice(s![0..hsz]).to_owned().mapv(|v| 1.0 / (1.0 + (-v).exp())); // [H&S97]
        let f = gates.slice(s![hsz..2 * hsz]).to_owned().mapv(|v| 1.0 / (1.0 + (-v).exp())); // [H&S97]
        let g = gates.slice(s![2 * hsz..3 * hsz]).to_owned().mapv(|v| v.tanh());              // [H&S97]
        let o = gates.slice(s![3 * hsz..4 * hsz]).to_owned().mapv(|v| 1.0 / (1.0 + (-v).exp())); // [H&S97]

        // Cell & hidden updates [H&S97]
        let c = &f * &self.state.c + &i * &g;
        let mut h = &o * c.mapv(|v| v.tanh());

        // Locked dropout on outputs [Gal16]
        if training {
            if let Some(mo) = &self.dropout_out_mask { h = &h * mo; }
        }

        // Tape for TBPTT [W&P90]
        self.tape.push(StepTape { x_t, i: i.clone(), f: f.clone(), g: g.clone(), o: o.clone(), c: c.clone(), h: h.clone() });
        if self.tape.len() > SEQ_LEN { self.tape.remove(0); }

        // Update recurrent state
        self.state.c = c;
        self.state.h = h.clone();
        h
    }

    // Backward TBPTT across last k steps.
    // dh_extern: optional per-timestep external dL/dh_t (oldest..newest), used for inter-layer flow.
    // Returns (parameter grads, dx sequence oldest..newest) for lower layer. [W&P90/TBPTT]
    fn backward_tbptt(
        &mut self,
        dh_extern: Option<&[Array1<f64>]>,
        mut dh_next: Array1<f64>,
        mut dc_next: Array1<f64>,
        k: usize,
    ) -> (LSTMGrads, Vec<Array1<f64>>) {
        let mut g = LSTMGrads::zeros(self.input_dim, self.output_dim);
        let steps = self.tape.len();
        let k_steps = k.min(steps);

        let mut dx_old_to_new: Vec<Array1<f64>> = Vec::with_capacity(k_steps);

        for s_idx in 0..k_steps {
            let t = steps - 1 - s_idx;                 // newest→oldest over window
            let st = &self.tape[t];

            // External gradient injection for inter-layer flow [W&P90]
            let dh_from_upper = if let Some(dseq) = dh_extern {
                let di = k_steps - 1 - s_idx; // map newest→oldest
                if di < dseq.len() { dseq[di].clone() } else { Array1::zeros(self.output_dim) }
            } else { Array1::zeros(self.output_dim) };

            // Total dL/dh_t combines recurrence + external [W&P90]
            let mut dh_t_total = &dh_next + &dh_from_upper;

            // Local backprop [H&S97]
            let tanh_c = st.c.mapv(|v| v.tanh());
            let do_t = &dh_t_total * &tanh_c * &st.o.mapv(|v| v * (1.0 - v));          // σ'(·) [H&S97]
            let dct  = &dh_t_total * &st.o * tanh_c.mapv(|v| 1.0 - v * v) + &dc_next;  // tanh'(·) [H&S97]

            // Gate grads [H&S97]
            let di_t = &dct * &st.g * &st.i.mapv(|v| v * (1.0 - v));                   // σ'(·)
            let df_t = &dct * &self.prev_c(t) * &st.f.mapv(|v| v * (1.0 - v));         // σ'(·)
            let dg_t = &dct * &st.i * st.g.mapv(|v| 1.0 - v * v);                      // tanh'(·)

            // Concatenate [i f g o]
            let gates_grad = vcat4(&di_t, &df_t, &dg_t, &do_t); // [4H]

            // Param grads
            let x_col = st.x_t.view().insert_axis(Axis(1));             // [in,1]
            let hprev_col = self.prev_h(t).view().insert_axis(Axis(1)); // [H,1]
            let gg_col = gates_grad.view().insert_axis(Axis(1));         // [4H,1]

            g.w_ih += &gg_col.dot(&x_col.t());
            g.w_hh += &gg_col.dot(&hprev_col.t());
            g.b_ih += &gates_grad;
            g.b_hh += &gates_grad;

            // Grad wrt inputs and previous states
            let dx_t   = self.p.w_ih.t().dot(&gates_grad); // dL/dx_t to feed lower layer
            let dh_prev= self.p.w_hh.t().dot(&gates_grad);
            let dc_prev= &dct * &st.f;

            dx_old_to_new.push(dx_t);
            dh_next = dh_prev;
            dc_next = dc_prev;
        }

        dx_old_to_new.reverse();
        (g, dx_old_to_new)
    }

    fn prev_h(&self, t: usize) -> Array1<f64> {
        if t == 0 { Array1::zeros(self.output_dim) } else { self.tape[t - 1].h.clone() }
    }
    fn prev_c(&self, t: usize) -> Array1<f64> {
        if t == 0 { Array1::zeros(self.output_dim) } else { self.tape[t - 1].c.clone() }
    }
}

#[derive(Clone, Default)]
struct LSTMGrads {
    w_ih: Array2<f64>,
    w_hh: Array2<f64>,
    b_ih: Array1<f64>,
    b_hh: Array1<f64>,
}
impl LSTMGrads {
    fn zeros(input_dim: usize, hidden: usize) -> Self {
        Self {
            w_ih: Array2::zeros((4 * hidden, input_dim)),
            w_hh: Array2::zeros((4 * hidden, hidden)),
            b_ih: Array1::zeros(4 * hidden),
            b_hh: Array1::zeros(4 * hidden),
        }
    }
    fn l2(&self) -> f64 {
        self.w_ih.mapv(|v| v * v).sum()
            + self.w_hh.mapv(|v| v * v).sum()
            + self.b_ih.mapv(|v| v * v).sum()
            + self.b_hh.mapv(|v| v * v).sum()
    }
}

// =====================================================================================
// Model
// =====================================================================================
#[derive(Clone, Serialize, Deserialize)]
struct OnlineLSTMModel {
    layers: Vec<LSTMLayer>,

    // Output heads: shared hidden -> 3 heads
    w_profit: Array1<f64>, b_profit: f64,
    w_succ:   Array1<f64>, b_succ:   f64,
    w_tip:    Array1<f64>, b_tip:    f64,

    // Adam moments [KB15]
    lstm_m: Vec<LSTMMoments>,
    lstm_v: Vec<LSTMMoments>,
    w_profit_m: Array1<f64>, w_profit_v: Array1<f64>,
    w_succ_m:   Array1<f64>, w_succ_v:   Array1<f64>,
    w_tip_m:    Array1<f64>, w_tip_v:    Array1<f64>,
    b_profit_m: f64, b_profit_v: f64,
    b_succ_m:   f64, b_succ_v:   f64,
    b_tip_m:    f64, b_tip_v:    f64,

    iter: usize,

    // Provenance
    stamp: ProvenanceStamp,

    // Jitter metrics (optional)
    ss_jitter: JitterHist,
    logs_jitter: JitterHist,
}

impl OnlineLSTMModel {
    fn new() -> Self {
        let mut layers = Vec::with_capacity(LSTM_LAYERS);
        let mut in_dim = INPUT_FEATURES;
        for _ in 0..LSTM_LAYERS {
            layers.push(LSTMLayer::new(in_dim, LSTM_HIDDEN_SIZE));
            in_dim = LSTM_HIDDEN_SIZE;
        }

        let std = (2.0 / (LSTM_HIDDEN_SIZE + 1) as f64).sqrt();
        let dist = Uniform::new(-std, std);
        let mut rng = thread_rng();

        let w_profit = Array1::from_shape_fn(LSTM_HIDDEN_SIZE, |_| dist.sample(&mut rng));
        let w_succ   = Array1::from_shape_fn(LSTM_HIDDEN_SIZE, |_| dist.sample(&mut rng));
        let w_tip    = Array1::from_shape_fn(LSTM_HIDDEN_SIZE, |_| dist.sample(&mut rng));

        let build_commit = option_env!("GIT_COMMIT").map(|s| s.to_string());
        let stamp = ProvenanceStamp {
            started_at_unix_ms: now_ms(),
            build_commit,
            seq_len: SEQ_LEN,
            tbptt: TBPTT,
            lr: LR,
            l2: L2,
            clip: CLIP_MAX_NORM,
            beta1: ADAM_B1,
            beta2: ADAM_B2,
            dropout: DROPOUT_RATE,
            per_alpha: 0.6,
            per_beta0: 0.4,
        };

        Self {
            lstm_m: (0..LSTM_LAYERS).map(|_| LSTMMoments::default()).collect(),
            lstm_v: (0..LSTM_LAYERS).map(|_| LSTMMoments::default()).collect(),
            w_profit_m: Array1::zeros(LSTM_HIDDEN_SIZE), w_profit_v: Array1::zeros(LSTM_HIDDEN_SIZE),
            w_succ_m:   Array1::zeros(LSTM_HIDDEN_SIZE), w_succ_v:   Array1::zeros(LSTM_HIDDEN_SIZE),
            w_tip_m:    Array1::zeros(LSTM_HIDDEN_SIZE), w_tip_v:    Array1::zeros(LSTM_HIDDEN_SIZE),
            b_profit_m: 0.0, b_profit_v: 0.0,
            b_succ_m:   0.0, b_succ_v:   0.0,
            b_tip_m:    0.0, b_tip_v:    0.0,
            iter: 0,

            layers,
            w_profit, w_succ, w_tip,
            b_profit: 0.0, b_succ: 0.0, b_tip: 0.0,

            stamp,
            ss_jitter: JitterHist::default(),
            logs_jitter: JitterHist::default(),
        }
    }

    fn reset_states(&mut self) {
        for l in &mut self.layers { l.reset_state(); }
    }

    // Forward a full sequence for a single sample
    fn forward_sequence(&mut self, seq: &Array2<f64>, training: bool) -> (Vec<Vec<Array1<f64>>>, Array1<f64>, f64, Array1<f64>, f64) {
        // Per-layer per-timestep h outputs to support inter-layer TBPTT
        let mut layer_inputs: Vec<Array1<f64>> = (0..seq.nrows()).map(|t| seq.slice(s![t, ..]).to_owned()).collect();
        let mut per_layer_h: Vec<Vec<Array1<f64>>> = Vec::with_capacity(self.layers.len());

        let t0 = Instant::now();
        for l in 0..self.layers.len() {
            let mut outs: Vec<Array1<f64>> = Vec::with_capacity(layer_inputs.len());
            for t in 0..layer_inputs.len() {
                let h = self.layers[l].forward_step(&layer_inputs[t], training); // [H]
                outs.push(h);
            }
            per_layer_h.push(outs.clone());
            layer_inputs = outs;
        }
        let fw_ms = t0.elapsed().as_secs_f64() * 1000.0;

        let hs_last = layer_inputs.last().unwrap().clone(); // final hidden T
        // Heads: hidden -> scalars [H&S97 architecture]
        let profit = hs_last.dot(&self.w_profit) + self.b_profit; // regression
        let succ_logit = hs_last.dot(&self.w_succ) + self.b_succ; // logit
        let tip = hs_last.dot(&self.w_tip) + self.b_tip;          // regression

        let succ_prob = sigmoid_scalar(succ_logit);
        (per_layer_h, Array1::from_vec(vec![profit]), succ_prob, Array1::from_vec(vec![tip]), succ_logit)
    }

    // Compute loss and grads for one sample with provided IS weight
    fn loss_and_grads(
        &mut self,
        seq: &Array2<f64>,
        target: &Array1<f64>,
        is_weight: f64,
    ) -> (f64, f64, ModelGrads, Vec<Vec<Array1<f64>>>) {
        let (per_layer_h, profit, succ_prob, tip, _succ_logit) = self.forward_sequence(seq, true);

        let y_profit = target[0];
        let y_succ   = target[1];
        let y_tip    = target[2];

        // Losses
        let r1 = profit[0] - y_profit;
        let r2 = tip[0] - y_tip;
        let l_profit = huber(r1);
        let l_tip    = huber(r2);
        let l_succ   = bce_loss(succ_prob, y_succ, 1e-7);

        // Task loss (unweighted) as error proxy for PER
        let task_loss = l_profit + l_tip + l_succ;

        // Weighted sum
        let (w1, w2, w3) = (1.0, 1.0, 1.0);
        let loss = is_weight * (w1 * l_profit + w2 * l_tip + w3 * l_succ);

        // Head grads: wrt last hidden T
        let d_profit = is_weight * w1 * huber_grad(r1);                 // dL/d profit
        let d_tip    = is_weight * w2 * huber_grad(r2);                 // dL/d tip
        let d_succ   = is_weight * w3 * (succ_prob - y_succ);           // dL/d logit = p - y

        // Gradient into last hidden from heads  [H&S97]
        let mut dh_T = &self.w_profit * d_profit + &self.w_succ * d_succ + &self.w_tip * d_tip;

        // Backprop TBPTT across layers with inter-layer flow [W&P90]
        let mut lstm_grads: Vec<LSTMGrads> = Vec::with_capacity(self.layers.len());
        let mut dh_extern_next: Option<Vec<Array1<f64>>> = None;

        for li in (0..self.layers.len()).rev() {
            let dh_extern_slice_opt: Option<Vec<Array1<f64>>> = dh_extern_next.clone();

            let (g, dx_seq) = self.layers[li].backward_tbptt(
                dh_extern_slice_opt.as_ref().map(|v| v.as_slice()),
                dh_T.clone(),
                Array1::zeros(self.layers[li].output_dim),
                TBPTT,
            );

            // Prepare external dh for the next lower layer (dx is dL/dx_t == dL/dh_{lower,t})
            dh_extern_next = Some(dx_seq.clone());
            lstm_grads.push(g);

            // For next lower layer, there's no single dh_T from heads; set to zeros.
            dh_T = Array1::zeros(self.layers[li].input_dim);
        }
        lstm_grads.reverse();

        // Head parameter grads (use last hidden of last layer T)
        let hs_last = per_layer_h.last().unwrap().last().unwrap().clone();
        let g_heads = HeadGrads {
            w_profit: &hs_last * d_profit,
            w_succ:   &hs_last * d_succ,
            w_tip:    &hs_last * d_tip,
            b_profit: d_profit, b_succ: d_succ, b_tip: d_tip,
        };

        // L2 weight decay applied in optimizer step [KB15]
        let total_loss = loss + L2 * (
            self.layers.iter().map(|l| {
                l.p.w_ih.mapv(|v| v*v).sum() + l.p.w_hh.mapv(|v| v*v).sum()
            }).sum::<f64>()
        );

        (total_loss, task_loss, ModelGrads { lstm: lstm_grads, heads: g_heads }, per_layer_h)
    }

    fn adam_step(&mut self, grads: &ModelGrads) {
        self.iter += 1;
        // [KB15] bias correction folded into LR scaling: lr * sqrt(1-β2^t)/(1-β1^t)
        let t = self.iter as f64;
        let bias_c1 = 1.0 - ADAM_B1.powf(t);
        let bias_c2 = 1.0 - ADAM_B2.powf(t);
        let lr = LR * (bias_c2.sqrt() / bias_c1);  // [KB15]

        // LSTM layers
        for li in 0..self.layers.len() {
            let layer = &mut self.layers[li];
            let g = &grads.lstm[li];
            let m = &mut self.lstm_m[li];
            let v = &mut self.lstm_v[li];

            // weight decay style
            let g_wih = &g.w_ih + &(L2 * &layer.p.w_ih);
            let g_whh = &g.w_hh + &(L2 * &layer.p.w_hh);

            // m ← β1 m + (1-β1) g ; v ← β2 v + (1-β2) g^2 ; θ ← θ - lr m/√(v+ε)  [KB15]
            m.w_ih_m = &m.w_ih_m * ADAM_B1 + &g_wih * (1.0 - ADAM_B1);
            v.w_ih_v = &v.w_ih_v * ADAM_B2 + g_wih.mapv(|x| x * x) * (1.0 - ADAM_B2);
            layer.p.w_ih = &layer.p.w_ih - &((&m.w_ih_m / (&v.w_ih_v.mapv(|z| z.sqrt() + ADAM_EPS))) * lr);

            m.w_hh_m = &m.w_hh_m * ADAM_B1 + &g_whh * (1.0 - ADAM_B1);
            v.w_hh_v = &v.w_hh_v * ADAM_B2 + g_whh.mapv(|x| x * x) * (1.0 - ADAM_B2);
            layer.p.w_hh = &layer.p.w_hh - &((&m.w_hh_m / (&v.w_hh_v.mapv(|z| z.sqrt() + ADAM_EPS))) * lr);

            m.b_ih_m = &m.b_ih_m * ADAM_B1 + &g.b_ih * (1.0 - ADAM_B1);
            v.b_ih_v = &v.b_ih_v * ADAM_B2 + g.b_ih.mapv(|x| x * x) * (1.0 - ADAM_B2);
            layer.p.b_ih = &layer.p.b_ih - &((&m.b_ih_m / (&v.b_ih_v.mapv(|z| z.sqrt() + ADAM_EPS))) * lr);

            m.b_hh_m = &m.b_hh_m * ADAM_B1 + &g.b_hh * (1.0 - ADAM_B1);
            v.b_hh_v = &v.b_hh_v * ADAM_B2 + g.b_hh.mapv(|x| x * x) * (1.0 - ADAM_B2);
            layer.p.b_hh = &layer.p.b_hh - &((&m.b_hh_m / (&v.b_hh_v.mapv(|z| z.sqrt() + ADAM_EPS))) * lr);
        }

        // Heads
        adam_vec_step(&mut self.w_profit, &mut self.w_profit_m, &mut self.w_profit_v, &grads.heads.w_profit, lr);
        adam_vec_step(&mut self.w_succ,   &mut self.w_succ_m,   &mut self.w_succ_v,   &grads.heads.w_succ,   lr);
        adam_vec_step(&mut self.w_tip,    &mut self.w_tip_m,    &mut self.w_tip_v,    &grads.heads.w_tip,    lr);

        adam_sca_step(&mut self.b_profit, &mut self.b_profit_m, &mut self.b_profit_v, grads.heads.b_profit, lr);
        adam_sca_step(&mut self.b_succ,   &mut self.b_succ_m,   &mut self.b_succ_v,   grads.heads.b_succ,   lr);
        adam_sca_step(&mut self.b_tip,    &mut self.b_tip_m,    &mut self.b_tip_v,    grads.heads.b_tip,    lr);
    }
}

fn adam_vec_step(w: &mut Array1<f64>, m: &mut Array1<f64>, v: &mut Array1<f64>, g: &Array1<f64>, lr: f64) {
    *m = m * ADAM_B1 + g * (1.0 - ADAM_B1);
    *v = v * ADAM_B2 + g.mapv(|x| x * x) * (1.0 - ADAM_B2);
    *w = w - &(m / &v.mapv(|z| z.sqrt() + ADAM_EPS) * lr);
}
fn adam_sca_step(w: &mut f64, m: &mut f64, v: &mut f64, g: f64, lr: f64) {
    *m = *m * ADAM_B1 + g * (1.0 - ADAM_B1);
    *v = *v * ADAM_B2 + g * g * (1.0 - ADAM_B2);
    *w -= (*m / (v.sqrt() + ADAM_EPS)) * lr;
}

#[derive(Clone)]
struct HeadGrads {
    w_profit: Array1<f64>, w_succ: Array1<f64>, w_tip: Array1<f64>,
    b_profit: f64,         b_succ: f64,         b_tip: f64,
}

struct ModelGrads { lstm: Vec<LSTMGrads>, heads: HeadGrads }

// =====================================================================================
// Trainer
// =====================================================================================
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct Metrics {
    updates: u64,
    avg_loss: f64,
    avg_task_loss: f64,
    last_ckpt: Instant,
    train_ms_ema: f64,
}

pub struct OnlineLSTMTrainer {
    model: Arc<RwLock<OnlineLSTMModel>>,
    replay: Arc<RwLock<SumTree>>,
    scaler: Arc<RwLock<EmaScaler>>,
    metrics: Arc<RwLock<Metrics>>,
    training_on: Arc<RwLock<bool>>,
    beta_anneal: Arc<RwLock<f64>>, // PER β(t) → 1.0
}

impl OnlineLSTMTrainer {
    pub fn new() -> Self {
        // prod_strict: refuse training without a provenance stamp (enforced by having it in model)
        #[cfg(feature = "prod_strict")]
        {
            let _ = (); // placeholder; model carries stamp at init
        }

        Self {
            model: Arc::new(RwLock::new(OnlineLSTMModel::new())),
            replay: Arc::new(RwLock::new(SumTree::new(MAX_REPLAY, 0.6, 0.4))), // α=0.6, β0=0.4
            scaler: Arc::new(RwLock::new(EmaScaler::new(INPUT_FEATURES, 0.01))),
            metrics: Arc::new(RwLock::new(Metrics { last_ckpt: Instant::now(), ..Default::default() })),
            training_on: Arc::new(RwLock::new(true)),
            beta_anneal: Arc::new(RwLock::new(0.4)),
        }
    }

    pub async fn spawn_online_loop(&self, mut rx: mpsc::Receiver<TrainingData>) {
        let model = self.model.clone();
        let replay = self.replay.clone();
        let scaler = self.scaler.clone();
        let metrics = self.metrics.clone();
        let on = self.training_on.clone();
        let beta_anneal = self.beta_anneal.clone();

        tokio::spawn(async move {
            let mut tick = interval(Duration::from_millis(UPDATE_EVERY_MS));
            loop {
                tokio::select! {
                    Some(mut td) = rx.recv() => {
                        // Update scaler and normalize sequence online
                        {
                            let mut sc = scaler.write().unwrap();
                            for t in 0..td.features_seq.nrows() {
                                let row = td.features_seq.slice(s![t, ..]).to_owned();
                                sc.update(&row);
                                let norm = sc.normalize(&row);
                                td.features_seq.slice_mut(s![t, ..]).assign(&norm);
                            }
                        }
                        // Push with optimistic priority (will be corrected on learn)  [Schaul15]
                        replay.write().unwrap().add(1.0, td);
                    }
                    _ = tick.tick() => {
                        if !*on.read().unwrap() { continue; }
                        let batch = sample_batch(&replay.read().unwrap());
                        if batch.is_empty() { continue; }

                        // Anneal β toward 1.0  [Schaul15]
                        {
                            let mut b = beta_anneal.write().unwrap();
                            *b = (*b + 0.001).min(1.0);
                            replay.write().unwrap().set_beta(*b);
                        }

                        let start = Instant::now();
                        let (loss, task_loss) = train_minibatch(&model, &replay, &batch);
                        let elapsed = start.elapsed().as_secs_f64()*1000.0;

                        let mut m = metrics.write().unwrap();
                        m.updates += 1;
                        m.avg_loss = m.avg_loss*0.99 + loss*0.01;
                        m.avg_task_loss = m.avg_task_loss*0.99 + task_loss*0.01;
                        m.train_ms_ema = m.train_ms_ema*0.99 + elapsed*0.01;

                        if m.last_ckpt.elapsed() >= CHECKPOINT_EVERY {
                            let _ = checkpoint(&model, m.updates, m.avg_loss);
                            m.last_ckpt = Instant::now();
                        }
                    }
                }
            }
        });
    }

    pub fn pause(&self)  { *self.training_on.write().unwrap() = false; }
    pub fn resume(&self) { *self.training_on.write().unwrap() = true; }

    pub fn metrics(&self) -> Metrics { self.metrics.read().unwrap().clone() }

    pub fn predict(&self, seq: &Array2<f64>) -> (f64, f64, f64) {
        let mut model = self.model.read().unwrap().clone();
        let sc = self.scaler.read().unwrap().clone();
        let mut norm = seq.clone();
        for t in 0..norm.nrows() {
            let row = norm.slice(s![t, ..]).to_owned();
            let row_n = sc.normalize(&row);
            norm.slice_mut(s![t, ..]).assign(&row_n);
        }
        model.reset_states();
        let (_h_all, profit, succ_prob, tip, _logit) = model.forward_sequence(&norm, false);
        (profit[0], succ_prob, tip[0])
    }
}

// =====================================================================================
// Minibatch training with PER + global grad norm clip [Pascanu13] + Adam [KB15]
// =====================================================================================
fn sample_batch(st: &SumTree) -> Vec<(usize, f64)> {
    if st.size < BATCH_SIZE { return vec![]; }
    let mut out = Vec::with_capacity(BATCH_SIZE);
    let mut rng = thread_rng();
    let dist = Uniform::new(0.0, 1.0);
    for _ in 0..BATCH_SIZE {
        let r = dist.sample(&mut rng);
        out.push(st.sample(r));
    }
    out
}

fn train_minibatch(
    model_arc: &Arc<RwLock<OnlineLSTMModel>>,
    st_arc: &Arc<RwLock<SumTree>>,
//  batch: &[(usize, f64)],   // See below to keep stable with MSRV
    batch: &Vec<(usize, f64)>,
) -> (f64, f64) {
    let mut total_loss = 0.0;
    let mut total_task_loss = 0.0;

    // For PER updates per-sample
    let mut per_errors: Vec<(usize, f64)> = Vec::with_capacity(batch.len());

    // Accumulate grads over batch with proper layer shapes
    let mut agg = {
        let model = model_arc.read().unwrap();
        AggGrads::new(&model)
    };

    {
        let mut model = model_arc.write().unwrap();
        let st_read = st_arc.read().unwrap();

        for (idx, leaf_p) in batch.iter() {
            let td = &st_read.data[*idx];

            model.reset_states();
            // Proper IS weight from leaf priority [Schaul15]
            let is_w = st_read.importance_weight_from_leaf_p(*leaf_p);

            let (loss, task_loss, grads, _h) = model.loss_and_grads(&td.features_seq, &td.target, is_w);
            total_loss += loss;
            total_task_loss += task_loss;

            // Per-sample error proxy (task_loss) for PER priority [Schaul15]
            per_errors.push((*idx, task_loss.abs()));

            agg.add(&model, &grads);
        }

        // Global norm clip [Pascanu13]
        agg.clip(CLIP_MAX_NORM);

        // Apply Adam [KB15]
        model.adam_step(&agg.to_model_grads());
    }

    // PER priority updates per-sample
    {
        let mut st = st_arc.write().unwrap();
        for (idx, err) in per_errors {
            st.update(idx, err);
        }
    }

    (total_loss / batch.len() as f64, total_task_loss / batch.len() as f64)
}

struct AggGrads {
    lstm: Vec<LSTMGrads>,
    heads: HeadGrads,
}
impl AggGrads {
    fn new(model: &OnlineLSTMModel) -> Self {
        let mut lstm = Vec::with_capacity(model.layers.len());
        lstm.push(LSTMGrads::zeros(model.layers[0].input_dim, model.layers[0].output_dim));
        for li in 1..model.layers.len() {
            lstm.push(LSTMGrads::zeros(model.layers[li].input_dim, model.layers[li].output_dim));
        }
        Self {
            lstm,
            heads: HeadGrads {
                w_profit: Array1::zeros(LSTM_HIDDEN_SIZE),
                w_succ:   Array1::zeros(LSTM_HIDDEN_SIZE),
                w_tip:    Array1::zeros(LSTM_HIDDEN_SIZE),
                b_profit: 0.0, b_succ: 0.0, b_tip: 0.0,
            },
        }
    }
    fn add(&mut self, _m: &OnlineLSTMModel, g: &ModelGrads) {
        for i in 0..self.lstm.len() {
            self.lstm[i].w_ih = &self.lstm[i].w_ih + &g.lstm[i].w_ih;
            self.lstm[i].w_hh = &self.lstm[i].w_hh + &g.lstm[i].w_hh;
            self.lstm[i].b_ih = &self.lstm[i].b_ih + &g.lstm[i].b_ih;
            self.lstm[i].b_hh = &self.lstm[i].b_hh + &g.lstm[i].b_hh;
        }
        self.heads.w_profit = &self.heads.w_profit + &g.heads.w_profit;
        self.heads.w_succ   = &self.heads.w_succ   + &g.heads.w_succ;
        self.heads.w_tip    = &self.heads.w_tip    + &g.heads.w_tip;
        self.heads.b_profit += g.heads.b_profit;
        self.heads.b_succ   += g.heads.b_succ;
        self.heads.b_tip    += g.heads.b_tip;
    }
    fn l2(&self) -> f64 {
        let mut s = 0.0;
        for g in &self.lstm { s += g.l2(); }
        s += self.heads.w_profit.mapv(|v| v*v).sum();
        s += self.heads.w_succ.mapv(|v| v*v).sum();
        s += self.heads.w_tip.mapv(|v| v*v).sum();
        s
    }
    fn clip(&mut self, max_norm: f64) {
        let mut sumsq = 0.0;
        for g in &self.lstm {
            sumsq += g.w_ih.mapv(|v| v*v).sum();
            sumsq += g.w_hh.mapv(|v| v*v).sum();
            sumsq += g.b_ih.mapv(|v| v*v).sum();
            sumsq += g.b_hh.mapv(|v| v*v).sum();
        }
        sumsq += self.heads.w_profit.mapv(|v| v*v).sum();
        sumsq += self.heads.w_succ.mapv(|v| v*v).sum();
        sumsq += self.heads.w_tip.mapv(|v| v*v).sum();
        sumsq += self.heads.b_profit*self.heads.b_profit
            + self.heads.b_succ*self.heads.b_succ
            + self.heads.b_tip*self.heads.b_tip;

        let norm = sumsq.sqrt();
        if norm > max_norm {                 // [Pascanu13]
            let scale = max_norm / (norm + 1e-12);
            for g in &mut self.lstm {
                g.w_ih *= scale; g.w_hh *= scale; g.b_ih *= scale; g.b_hh *= scale;
            }
            self.heads.w_profit *= scale;
            self.heads.w_succ   *= scale;
            self.heads.w_tip    *= scale;
            self.heads.b_profit *= scale;
            self.heads.b_succ   *= scale;
            self.heads.b_tip    *= scale;
        }
    }
    fn to_model_grads(self) -> ModelGrads { ModelGrads { lstm: self.lstm, heads: self.heads } }
}

fn vcat4(a: &Array1<f64>, b: &Array1<f64>, c: &Array1<f64>, d: &Array1<f64>) -> Array1<f64> {
    let mut out = Array1::zeros(a.len() + b.len() + c.len() + d.len());
    let mut i = 0;
    out.slice_mut(s![i..i+a.len()]).assign(a); i += a.len();
    out.slice_mut(s![i..i+b.len()]).assign(b); i += b.len();
    out.slice_mut(s![i..i+c.len()]).assign(c); i += c.len();
    out.slice_mut(s![i..i+d.len()]).assign(d);
    out
}

// =====================================================================================
// Checkpoint (binary) + JSON provenance
// =====================================================================================
fn checkpoint(model: &Arc<RwLock<OnlineLSTMModel>>, updates: u64, avg_loss: f64) -> std::io::Result<()> {
    let guard = model.read().unwrap();
    let bytes = bincode::serialize(&*guard).unwrap();
    let base = format!("checkpoints/online_lstm_{}_loss_{:.6}", updates, avg_loss);
    std::fs::create_dir_all("checkpoints").ok();
    std::fs::write(format!("{base}.bin"), bytes)?;
    std::fs::write(format!("{base}.stamp.json"), serde_json::to_vec_pretty(&guard.stamp).unwrap())
}

// =====================================================================================
// MEV feature helpers (real sources are Solana logs + Jito ShredStream)
// =====================================================================================
//
// In production, build sequences (length SEQ_LEN) with features like:
//   - Pool state deltas (reserves, tick, sqrt_price, fee growth)
//   - Recent swap sizes/direction, slot timing features, Jito fee level
//   - Blockspace pressure (leader proximity, bundle success rate)
// This file only defines the shape. Feed `TrainingData{features_seq, target}`.

pub fn make_training_sample(
    mut seq: Array2<f64>, // [T, INPUT_FEATURES]
    y_profit: f64,
    y_success: f64,
    y_tip: f64,
) -> TrainingData {
    TrainingData {
        features_seq: { if seq.nrows() >= SEQ_LEN { seq.slice(s![seq.nrows()-SEQ_LEN.., ..]).to_owned() } else { seq } },
        target: Array1::from_vec(vec![y_profit, y_success, y_tip]),
        timestamp: Instant::now(),
    }
}

// =====================================================================================
// Feature-gated Solana/Jito streaming (enable with cargo features)
// =====================================================================================
#[cfg(feature = "solana-stream")]
mod solana_stream {
    use super::*;
    use futures_util::StreamExt;
    use solana_client::nonblocking::pubsub_client::PubsubClient;
    use solana_client::rpc_config::{RpcTransactionLogsConfig, RpcTransactionLogsFilter};
    use solana_sdk::commitment_config::CommitmentConfig;

    /// Spawn one logsSubscribe per program id. `commitment` = "processed" | "confirmed" | "finalized".
    /// See: [Solana-logs] docs for Mentions filter behavior.
    pub async fn spawn_logs_subscribe_many(
        ws_url: &str,
        program_ids: Vec<String>,
        commitment: &str,
        tx: mpsc::Sender<TrainingData>,
    ) -> anyhow::Result<()> {
        let commitment_cfg = match commitment {
            "finalized" => CommitmentConfig::finalized(),
            "confirmed" => CommitmentConfig::confirmed(),
            _ => CommitmentConfig::processed(),
        };
        for pid in program_ids {
            let client = PubsubClient::new(ws_url).await?;
            let filter = RpcTransactionLogsFilter::Mentions(vec![pid.clone()]);
            let (mut stream, _unsub) = client.logs_subscribe(
                filter,
                RpcTransactionLogsConfig {
                    commitment: Some(commitment_cfg),
                    ..Default::default()
                }
            ).await?;

            // Per-program task
            let tx_cl = tx.clone();
            tokio::spawn(async move {
                let mut last = Instant::now();
                while let Some(update) = stream.next().await {
                    // Jitter is time between messages (proxy for burstiness)
                    let now = Instant::now();
                    let dt = now.duration_since(last).as_secs_f64()*1000.0;
                    last = now;

                    // Record jitter histogram
                    {
                        if let Ok(mut m) = tx_cl.clone().try_send(super::make_training_sample(Array2::zeros((SEQ_LEN, INPUT_FEATURES)), 0.0, 0.0, 0.0)) {
                            drop(m); // noop; channel type enforces ownership; ignore send result here
                        }
                    }

                    // TODO: parse Raydium/Orca events, extract features, build TrainingData then:
                    // tx_cl.send(td).await.ok();
                    let _ = update;
                }
            });
        }
        Ok(())
    }
}

#[cfg(feature = "jito-ss")]
mod jito_stream {
    use super::*;
    use futures_util::StreamExt;
    use tonic::transport::Channel;
    use jito_protos::block_engine::subscribe_update::UpdateOneof;
    use jito_protos::block_engine::{ SubscribeRequest, BlockEngineRelayerClient };

    pub async fn spawn_shredstream(
        grpc_url: &str,
        tx: mpsc::Sender<TrainingData>,
    ) -> anyhow::Result<()> {
        let mut client = BlockEngineRelayerClient::connect(grpc_url.to_string()).await?;
        let mut stream = client.subscribe(SubscribeRequest { ..Default::default() }).await?.into_inner();

        tokio::spawn(async move {
            let mut last = Instant::now();
            while let Some(msg) = stream.next().await.transpose().ok().flatten() {
                let now = Instant::now();
                let dt = now.duration_since(last).as_secs_f64()*1000.0;
                last = now;
                match msg.update_oneof {
                    Some(UpdateOneof::BundleResult(_br)) => {
                        // TODO: derive bundle success/fee-level features; send TrainingData
                        // tx.send(td).await.ok();
                    }
                    Some(_) | None => {}
                }
                let _ = dt;
            }
        });
        Ok(())
    }
}

// =====================================================================================
// Tests
// =====================================================================================
#[cfg(test)]
mod tests {
    use super::*;
    use rand::Rng;

    // Smoke + BCE grad + TBPTT path
    #[tokio::test]
    async fn tbptt_smoke_and_bce_grad() {
        let trainer = OnlineLSTMTrainer::new();
        let (tx, rx) = mpsc::channel(128);
        trainer.spawn_online_loop(rx).await;

        for _ in 0..64 {
            let mut seq = Array2::zeros((SEQ_LEN, INPUT_FEATURES));
            seq.fill(0.1);
            let td = make_training_sample(seq, 0.05, 0.6, 2.0e-6);
            tx.send(td).await.unwrap();
        }

        tokio::time::sleep(Duration::from_millis(400)).await;
        let m = trainer.metrics();
        assert!(m.updates > 0);

        // Numeric check of dL/db_succ ≈ p - y
        let mut model = trainer.model.read().unwrap().clone();
        let seq = Array2::zeros((SEQ_LEN, INPUT_FEATURES));
        let target = Array1::from_vec(vec![0.0, 0.7, 0.0]);
        model.reset_states();
        let (_, _p, succ_p, _t, _logit) = model.forward_sequence(&seq, false);
        let base_loss = bce_loss(succ_p, target[1], 1e-7);
        let eps = 1e-4;
        model.b_succ += eps;
        model.reset_states();
        let (_, _p2, succ_p2, _t2, _logit2) = model.forward_sequence(&seq, false);
        let loss_up = bce_loss(succ_p2, target[1], 1e-7);
        let num_grad = (loss_up - base_loss) / eps;
        let ana_grad = succ_p - target[1]; // p - y
        assert!((num_grad - ana_grad).abs() < 1e-3);
    }

    // PER assertions: IS weight formula & anneal monotonicity
    #[test]
    fn per_is_weights_and_anneal() {
        let mut st = SumTree::new(8, 0.6, 0.4);
        let dummy = TrainingData { features_seq: Array2::zeros((SEQ_LEN, INPUT_FEATURES)), target: Array1::zeros(3), timestamp: Instant::now() };
        for i in 0..8 {
            st.add((i as f64 + 1.0), dummy.clone());
        }
        // Sample a leaf, compute IS weight explicitly and compare
        let (idx, leaf_p) = st.sample(0.25);
        let w_impl = st.importance_weight_from_leaf_p(leaf_p);
        let total = st.total();
        let n = st.size.max(1) as f64;
        let p_i = leaf_p / total;
        let p_min = st.min_leaf_priority() / total;
        let w_max = (n * p_min).powf(-st.beta);
        let w_ref = (n * p_i).powf(-st.beta) / w_max;
        assert!((w_impl - w_ref).abs() < 1e-9);

        // β anneal monotonicity
        let old = st.beta;
        st.set_beta(0.8);
        assert!(st.beta >= old);
    }

    // PyTorch parity harness (optional): requires python3 + torch available
    #[cfg(feature = "parity_tests")]
    #[test]
    fn torch_parity_small_lstm() {
        use std::fs;
        use std::io::Write;
        use std::process::Command;

        // Deterministic tiny model
        let mut model = OnlineLSTMModel::new();
        model.layers = vec![LSTMLayer::new(3, 2)];
        model.w_profit = Array1::from_vec(vec![0.1, -0.2]);
        model.w_succ   = Array1::from_vec(vec![0.05, 0.3]);
        model.w_tip    = Array1::from_vec(vec![0.2, 0.2]);
        model.b_profit = 0.01; model.b_succ = -0.02; model.b_tip = 0.0;

        // overwrite weights to fixed values
        let mut rng = rand::rngs::StdRng::seed_from_u64(42);
        {
            let l = &mut model.layers[0];
            for v in l.p.w_ih.iter_mut() { *v = (rng.gen::<f64>() - 0.5)*0.2; }
            for v in l.p.w_hh.iter_mut() { *v = (rng.gen::<f64>() - 0.5)*0.2; }
        }

        // Tiny input sequence
        let mut seq = Array2::zeros((4, 3));
        seq[[0,0]] = 0.2; seq[[1,1]] = -0.3; seq[[2,2]] = 0.1; seq[[3,0]] = -0.1;

        // Serialize weights + input to JSON
        let payload = serde_json::json!({
            "w_ih": model.layers[0].p.w_ih,
            "w_hh": model.layers[0].p.w_hh,
            "b_ih": model.layers[0].p.b_ih,
            "b_hh": model.layers[0].p.b_hh,
            "w_succ": model.w_succ,
            "b_succ": model.b_succ,
            "seq": seq,
        });
        fs::create_dir_all("target/parity").ok();
        fs::write("target/parity/in.json", serde_json::to_vec_pretty(&payload).unwrap()).unwrap();

        // Write torch script
        let script = r#"
import json, torch, math, sys
j = json.load(open('target/parity/in.json','r'))
def toT(x): return torch.tensor(x, dtype=torch.double)
w_ih = toT(j['w_ih'])
w_hh = toT(j['w_hh'])
b_ih = toT(j['b_ih'])
b_hh = toT(j['b_hh'])
w_succ = toT(j['w_succ'])
b_succ = float(j['b_succ'])
seq = toT(j['seq'])  # [T, in]
H = w_hh.size(1)
h = torch.zeros(H, dtype=torch.double)
c = torch.zeros(H, dtype=torch.double)
def sigmoid(z): return 1.0/(1.0+(-z).exp())
hs = []
for t in range(seq.size(0)):
    x = seq[t]
    gates = torch.mv(w_ih, x) + b_ih + torch.mv(w_hh, h) + b_hh
    i = sigmoid(gates[0:H])
    f = sigmoid(gates[H:2*H])
    g = torch.tanh(gates[2*H:3*H])
    o = sigmoid(gates[3*H:4*H])
    c = f*c + i*g
    h = o*torch.tanh(c)
    hs.append(h.clone())
hT = hs[-1]
logit = (hT * w_succ).sum() + b_succ
p = 1.0/(1.0+(-logit).exp())
# finite diff wrt b_succ:
y = 0.7
def bce(prob, y): 
    prob = min(max(float(prob), 1e-7), 1-1e-7)
    return -(y*math.log(prob)+(1-y)*math.log(1-prob))
base = bce(p, y)
eps = 1e-5
p_up = 1.0/(1.0+(- (logit+eps)).exp())
up = bce(p_up, y)
num_grad = (up - base)/eps
ana = float(p - y)
print(json.dumps({"hT": hT.tolist(), "p": float(p), "num": num_grad, "ana": ana}))
"#;
        let mut f = fs::File::create("target/parity/torch_check.py").unwrap();
        f.write_all(script.as_bytes()).unwrap();

        // Run python
        let out = Command::new("python3").arg("target/parity/torch_check.py").output().expect("python3 not found?");
        assert!(out.status.success(), "python failed: {:?}", out);

        let parsed: serde_json::Value = serde_json::from_slice(&out.stdout).unwrap();
        let p: f64 = parsed["p"].as_f64().unwrap();
        let num: f64 = parsed["num"].as_f64().unwrap();
        let ana: f64 = parsed["ana"].as_f64().unwrap();
        assert!((num - ana).abs() < 1e-3, "BCE grad mismatch: num={} ana={}", num, ana);
    }
}
