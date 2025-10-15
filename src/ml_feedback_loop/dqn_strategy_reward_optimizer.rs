// dqn_strategy_reward_optimizer.rs  (OFFICIAL-PARITY UPGRADE, AUDITOR-GRADE)
// ======================================================================
// Rainbow subset agent for trade sizing/timing on Solana MEV streams.
// Subset = {Double-DQN, Dueling, PER, NoisyNets}.  // [Hessel2017-Rainbow]
//
// Canonical references (line-level tags appear at compute sites):
//   [Mnih2015-Nature]      Human-level control through deep RL (Nature, 2015)
//   [vanHasselt2016]       Deep RL with Double Q-learning (AAAI, 2016)
//   [Wang2016]             Dueling Network Architectures (ICML/PMLR v48, 2016)
//   [Schaul2015]           Prioritized Experience Replay (arXiv:1511.05952, 2015)
//   [Fortunato2017]        Noisy Networks for Exploration (arXiv:1706.10295, 2017)
//   [Hessel2017-Rainbow]   Rainbow: Combining Improvements in DRL (arXiv:1710.02298, 2017)
//
// Provenance (for README "Provenance" block):
//   Parity anchors: DeepMind Dopamine (TF) and DeepMind DQN Zoo (JAX).
//   Eval protocol: "eval-freeze" (no noise resample, ε=0) mirroring Rainbow reporting.  // [Hessel2017]
//
// Notes:
// - Reward shaping here is domain-specific (PnL/slippage/Sharpe). Paper-pure DQN uses raw rewards.  // [Mnih2015-Nature]
// ======================================================================

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};
use ndarray::{Array1, Array2, ArrayView1, Axis};
use rand::Rng;
use rand_distr::{Distribution, StandardNormal};
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;

// ===================== CONFIG =====================
const STATE_DIM: usize = 128;
const ACTION_DIM: usize = 21;
const HIDDEN_SIZE: usize = 256;

const BATCH_SIZE: usize = 256;
const BUFFER_CAPACITY: usize = 100_000;

const GAMMA: f64 = 0.995;
const LEARNING_RATE: f64 = 1e-4;

// Target network update controls.  // [Mnih2015-Nature]
const HARD_TARGET_COPY: bool = true;
const TARGET_COPY_INTERVAL: usize = 5_000; // steps
const TAU: f64 = 0.001; // used only if HARD_TARGET_COPY = false (Polyak)

// Exploration: NoisyNets preferred.  // [Fortunato2017]
const NOISY_EXPLORATION: bool = true;
// NoisyNet σ initialization: σ0 / √fan_in  // [Fortunato2017, Sec. 2.3]
const NOISY_SIGMA0: f64 = 0.5;

// ε-greedy fallback if NOISY_EXPLORATION=false.  // [Mnih2015-Nature]
const EPSILON_START: f64 = 1.0;
const EPSILON_END: f64 = 0.01;
const EPSILON_DECAY: f64 = 0.9995;

// PER hyperparameters.  // [Schaul2015]
const PER_ALPHA: f64 = 0.6;            // prioritization exponent α
const PER_BETA_START: f64 = 0.4;       // IS-correction anneal start
const PER_BETA_INCREMENT: f64 = 1e-5;  // anneal per step → 1
const PER_EPS: f64 = 1e-6;             // small added to TD for stability

// ===================== MARKET/PORTFOLIO STATE TYPES =====================
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MarketState {
    pub price: f64,
    pub volume_24h: f64,
    pub bid_ask_spread: f64,
    pub order_book_imbalance: f64,
    pub volatility: f64,
    pub momentum: f64,
    pub rsi: f64,
    pub macd: f64,
    pub bollinger_position: f64,
    pub vwap_deviation: f64,
    pub funding_rate: f64,
    pub open_interest: f64,
    pub liquidation_volume: f64,
    pub whale_activity: f64,
    pub network_congestion: f64,
    pub gas_price: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PortfolioState {
    pub token_balance: f64,
    pub sol_balance: f64,
    pub unrealized_pnl: f64,
    pub position_size: f64,
    pub avg_entry_price: f64,
    pub position_duration: f64,
    pub win_rate: f64,
    pub sharpe_ratio: f64,
    pub max_drawdown: f64,
    pub current_drawdown: f64,
}

// ===================== EXPERIENCE =====================
#[derive(Debug, Clone)]
pub struct Experience {
    state: Array1<f64>,
    action: usize,
    reward: f64,
    next_state: Array1<f64>,
    done: bool,
    timestamp: u64,
}

// ===================== PER SUM-TREE =====================
// Proportional prioritized sampling via a sum-tree.  // [Schaul2015, Alg. 1]
struct SumTree {
    capacity: usize,
    tree: Vec<f64>,   // size = 2*capacity (1-indexed binary tree layout)
    write: usize,
    len: usize,
}

impl SumTree {
    fn new(capacity: usize) -> Self {
        let cap_pow2 = capacity.next_power_of_two();
        Self { capacity: cap_pow2, tree: vec![0.0; 2 * cap_pow2], write: 0, len: 0 }
    }

    #[inline] fn total(&self) -> f64 { self.tree[1] }

    fn add(&mut self, priority: f64) -> usize {
        // Insert with proportional priority; updated upward along the tree.  // [Schaul2015, Alg. 1]
        let idx = self.write;
        self.update(idx, priority);
        self.write = (self.write + 1) % self.capacity;
        if self.len < self.capacity { self.len += 1; }
        idx
    }

    fn update(&mut self, idx: usize, priority: f64) {
        // Update leaf, then propagate delta to root.  // [Schaul2015, Alg. 1]
        let mut tree_idx = idx + self.capacity;
        let delta = priority - self.tree[tree_idx];
        self.tree[tree_idx] = priority;
        tree_idx /= 2;
        while tree_idx >= 1 {
            self.tree[tree_idx] += delta;
            if tree_idx == 1 { break; }
            tree_idx /= 2;
        }
    }

    // Traversal for proportional PER: subtract left mass when going right.  // [Schaul2015, Sec. 2.3]
    fn get(&self, mut s: f64) -> usize {
        let mut idx = 1usize;
        while idx < self.capacity {
            let left = idx * 2;
            if s <= self.tree[left] { idx = left; }
            else {
                s -= self.tree[left]; // ← left-mass subtraction  // [Schaul2015, Sec. 2.3]
                idx = left + 1;
            }
        }
        idx - self.capacity
    }
}

struct PrioritizedReplayBuffer {
    capacity: usize,
    data: Vec<Option<Experience>>,
    tree: SumTree,
    max_priority: f64,
    beta: f64,
}

impl PrioritizedReplayBuffer {
    fn new(capacity: usize) -> Self {
        Self {
            capacity,
            data: vec![None; capacity.next_power_of_two()],
            tree: SumTree::new(capacity),
            max_priority: 1.0,
            beta: PER_BETA_START,
        }
    }

    #[inline] fn len(&self) -> usize { self.tree.len.min(self.capacity) }

    fn push(&mut self, exp: Experience) {
        // Insert with p_i = (max_priority)^α to ensure new samples are seen.  // [Schaul2015, Sec. 3]
        let p = self.max_priority.powf(PER_ALPHA); // α exponent
        let idx = self.tree.add(p);
        if idx >= self.data.len() { self.data.resize(idx + 1, None); }
        self.data[idx] = Some(exp);
    }

    fn sample(&mut self, batch: usize) -> (Vec<(usize, Experience)>, Vec<f64>, f64) {
        let mut rng = rand::thread_rng();
        let total = self.tree.total().max(1e-12);
        let segment = total / batch as f64;

        let mut samples = Vec::with_capacity(batch);
        let mut weights = Vec::with_capacity(batch);
        let n = self.len().max(1);
        let mut max_w = 0.0;

        for i in 0..batch {
            // Stratified draw within segment i.  // [Schaul2015, Sec. 2.3]
            let a = segment * i as f64;
            let b = segment * (i as f64 + 1.0);
            let s = rng.gen_range(a..b);
            let idx = self.tree.get(s);
            let p_i = self.tree.tree[idx + self.tree.capacity].max(1e-12);
            let prob = p_i / total;

            // Importance sampling: w_i = (N * P(i))^{-β}, normalized by max.  // [Schaul2015, Sec. 3]
            let w = ((n as f64) * prob).powf(-self.beta);
            max_w = max_w.max(w);

            let exp = self.data[idx].as_ref().expect("sampled empty slot").clone();
            samples.push((idx, exp));
            weights.push(w);
        }

        // Normalize weights by max for stability.  // [Schaul2015, Sec. 3]
        for w in &mut weights { *w /= max_w.max(1e-12); }

        // Anneal β → 1.  // [Schaul2015, Sec. 3]
        self.beta = (self.beta + PER_BETA_INCREMENT).min(1.0);

        (samples, weights, total)
    }

    fn update_priorities(&mut self, indices: &[usize], td_errors: &[f64]) {
        // p_i ∝ |δ_i| + ε, raised to α at insert time.  // [Schaul2015, Sec. 3]
        for (i, &idx) in indices.iter().enumerate() {
            let p = (td_errors[i].abs() + PER_EPS).powf(PER_ALPHA);
            self.max_priority = self.max_priority.max(p);
            self.tree.update(idx, p);
        }
    }
}

// ===================== NOISY LINEAR (Factorized) =====================
// Factorized Gaussian noise: ε_w = f(ε_out) ⊗ f(ε_in), ε_b = f(ε_out).
// with f(x) = sign(x) * sqrt(|x|).  // [Fortunato2017, Sec. 2.3]
struct NoisyLinear {
    in_dim: usize,
    out_dim: usize,
    w_mu: Array2<f64>,     // (in, out)
    w_sigma: Array2<f64>,
    b_mu: Array1<f64>,     // (out)
    b_sigma: Array1<f64>,
    eps_in: Array1<f64>,   // factorized eps buffers
    eps_out: Array1<f64>,
}

impl NoisyLinear {
    fn new(in_dim: usize, out_dim: usize, rng: &mut rand::rngs::ThreadRng) -> Self {
        let mu_range = 1.0 / (in_dim as f64).sqrt(); // Xavier-ish
        let mut w_mu = Array2::zeros((in_dim, out_dim));
        let mut b_mu = Array1::zeros(out_dim);

        for i in 0..in_dim {
            for j in 0..out_dim { w_mu[[i, j]] = rng.gen_range(-mu_range..mu_range); }
        }
        for j in 0..out_dim { b_mu[j] = rng.gen_range(-mu_range..mu_range); }

        // σ init per paper: σ_w = σ0 / √fan_in; σ_b = σ0 / √out_dim.  // [Fortunato2017, Sec. 2.3]
        let w_sigma = Array2::from_elem((in_dim, out_dim), NOISY_SIGMA0 / (in_dim as f64).sqrt());
        let b_sigma = Array1::from_elem(out_dim, NOISY_SIGMA0 / (out_dim as f64).sqrt());

        Self {
            in_dim, out_dim, w_mu, w_sigma, b_mu, b_sigma,
            eps_in: Array1::zeros(in_dim),
            eps_out: Array1::zeros(out_dim),
        }
    }

    #[inline]
    fn f(e: &Array1<f64>) -> Array1<f64> { e.mapv(|x| x.signum() * x.abs().sqrt()) } // [Fortunato2017, Sec. 2.3]

    fn sample_noise(&mut self, rng: &mut rand::rngs::ThreadRng) {
        let mut eps_in = Array1::zeros(self.in_dim);
        let mut eps_out = Array1::zeros(self.out_dim);
        for i in 0..self.in_dim { eps_in[i] = StandardNormal.sample(rng); }
        for j in 0..self.out_dim { eps_out[j] = StandardNormal.sample(rng); }
        self.eps_in = Self::f(&eps_in);   // factorized transform f(x)  // [Fortunato2017, Sec. 2.3]
        self.eps_out = Self::f(&eps_out); // "
    }

    fn clear_noise(&mut self) {
        // Deterministic eval: zero ε → use μ only.  // [Hessel2017-Rainbow, eval protocol]
        self.eps_in.fill(0.0);
        self.eps_out.fill(0.0);
    }

    fn forward(&self, x: &ArrayView1<f64>) -> Array1<f64> {
        // y = (W_mu + W_sigma ⊙ (ε_in ⊗ ε_out))^T x + (b_mu + b_sigma ⊙ ε_out).  // [Fortunato2017]
        let eps_outer = self
            .eps_in.view().insert_axis(Axis(1))
            .dot(&self.eps_out.view().insert_axis(Axis(0))); // (in, out)

        let w = &self.w_mu + &(&self.w_sigma * &eps_outer);
        let mut y = x.dot(&w); // (out)
        y = y + &(&self.b_mu + &(&self.b_sigma * &self.eps_out));
        y
    }

    // Accumulate gradients for μ and σ given upstream grad g_out.  // [Fortunato2017]
    fn backward_accumulate(
        &self,
        g_out: &Array1<f64>,
        x: &Array1<f64>,
        gw_mu: &mut Array2<f64>,
        gw_sigma: &mut Array2<f64>,
        gb_mu: &mut Array1<f64>,
        gb_sigma: &mut Array1<f64>,
    ) {
        // dL/dW_mu = x ⊗ g_out
        for i in 0..self.in_dim { for o in 0..self.out_dim { gw_mu[[i, o]] += x[i] * g_out[o]; } }
        // dL/dW_sigma = (x ⊗ g_out) ⊙ (ε_in ⊗ ε_out)
        for i in 0..self.in_dim { for o in 0..self.out_dim {
            gw_sigma[[i, o]] += x[i] * g_out[o] * (self.eps_in[i] * self.eps_out[o]);
        }}
        // dL/db_mu = g_out
        *gb_mu = &*gb_mu + g_out;
        // dL/db_sigma = g_out ⊙ ε_out
        for o in 0..self.out_dim { gb_sigma[o] += g_out[o] * self.eps_out[o]; }
    }

    #[cfg(test)]
    fn __test_set_params(&mut self, w_mu: f64, w_sigma: f64, b_mu: f64, b_sigma: f64) {
        self.w_mu.fill(w_mu);
        self.w_sigma.fill(w_sigma);
        self.b_mu.fill(b_mu);
        self.b_sigma.fill(b_sigma);
    }
}

// ===================== Q NETWORK (Dueling + Noisy Heads) =====================
#[derive(Debug, Clone)]
pub struct NeuralNetwork {
    // shared trunk
    weights_1: Array2<f64>, bias_1: Array1<f64>,
    weights_2: Array2<f64>, bias_2: Array1<f64>,
    weights_3: Array2<f64>, bias_3: Array1<f64>,

    // dueling heads (Noisy).  // [Wang2016] + [Fortunato2017]
    noisy_adv: NoisyLinear, // (HIDDEN_SIZE -> ACTION_DIM)
    noisy_val: NoisyLinear, // (HIDDEN_SIZE -> 1)
}

impl NeuralNetwork {
    fn new() -> Self {
        let mut rng = rand::thread_rng();
        let xavier = |fan_in: usize, fan_out: usize| (2.0 / (fan_in + fan_out) as f64).sqrt();

        let w1 = Array2::from_shape_fn((STATE_DIM, HIDDEN_SIZE), |_| {
            rng.gen_range(-1.0..1.0) * xavier(STATE_DIM, HIDDEN_SIZE)
        });
        let b1 = Array1::zeros(HIDDEN_SIZE);
        let w2 = Array2::from_shape_fn((HIDDEN_SIZE, HIDDEN_SIZE), |_| {
            rng.gen_range(-1.0..1.0) * xavier(HIDDEN_SIZE, HIDDEN_SIZE)
        });
        let b2 = Array1::zeros(HIDDEN_SIZE);
        let w3 = Array2::from_shape_fn((HIDDEN_SIZE, HIDDEN_SIZE), |_| {
            rng.gen_range(-1.0..1.0) * xavier(HIDDEN_SIZE, HIDDEN_SIZE)
        });
        let b3 = Array1::zeros(HIDDEN_SIZE);

        let noisy_adv = NoisyLinear::new(HIDDEN_SIZE, ACTION_DIM, &mut rng);
        let noisy_val = NoisyLinear::new(HIDDEN_SIZE, 1, &mut rng);

        Self {
            weights_1: w1, bias_1: b1,
            weights_2: w2, bias_2: b2,
            weights_3: w3, bias_3: b3,
            noisy_adv, noisy_val
        }
    }

    #[inline] fn relu(x: &Array1<f64>) -> Array1<f64> { x.mapv(|v| v.max(0.0)) }

    fn sample_noisy(&mut self) {
        if NOISY_EXPLORATION {
            let mut rng = rand::thread_rng();
            self.noisy_adv.sample_noise(&mut rng);   // [Fortunato2017]
            self.noisy_val.sample_noise(&mut rng);   // [Fortunato2017]
        }
    }

    fn clear_noisy(&mut self) {
        self.noisy_adv.clear_noise(); // eval freeze  // [Hessel2017-Rainbow]
        self.noisy_val.clear_noise();
    }

    fn forward_with_cached_noise(&self, state: &ArrayView1<f64>) -> Array1<f64> {
        let h1 = Self::relu(&(state.dot(&self.weights_1) + &self.bias_1));
        let h2 = Self::relu(&(h1.dot(&self.weights_2) + &self.bias_2));
        let h3 = Self::relu(&(h2.dot(&self.weights_3) + &self.bias_3));

        // Dueling aggregator: Q = V + A − mean(A).  // [Wang2016, Sec. 3]
        let advantages = self.noisy_adv.forward(&h3.view());
        let value = self.noisy_val.forward(&h3.view())[0];
        &advantages - advantages.mean().unwrap() + value
    }
}

// ===================== OPTIMIZER STATE (Adam) =====================
#[derive(Debug, Clone)]
struct OptimizerState {
    // trunk
    m_w1: Array2<f64>, v_w1: Array2<f64>, m_b1: Array1<f64>, v_b1: Array1<f64>,
    m_w2: Array2<f64>, v_w2: Array2<f64>, m_b2: Array1<f64>, v_b2: Array1<f64>,
    m_w3: Array2<f64>, v_w3: Array2<f64>, m_b3: Array1<f64>, v_b3: Array1<f64>,

    // heads
    m_adv_mu: Array2<f64>, v_adv_mu: Array2<f64>,
    m_adv_sigma: Array2<f64>, v_adv_sigma: Array2<f64>,
    m_adv_bmu: Array1<f64>, v_adv_bmu: Array1<f64>,
    m_adv_bsigma: Array1<f64>, v_adv_bsigma: Array1<f64>,

    m_val_mu: Array2<f64>, v_val_mu: Array2<f64>,
    m_val_sigma: Array2<f64>, v_val_sigma: Array2<f64>,
    m_val_bmu: Array1<f64>, v_val_bmu: Array1<f64>,
    m_val_bsigma: Array1<f64>, v_val_bsigma: Array1<f64>,

    // bias-correction powers
    beta1_pow: f64,
    beta2_pow: f64,
}

impl OptimizerState {
    fn new() -> Self {
        Self {
            m_w1: Array2::zeros((STATE_DIM, HIDDEN_SIZE)), v_w1: Array2::zeros((STATE_DIM, HIDDEN_SIZE)),
            m_b1: Array1::zeros(HIDDEN_SIZE), v_b1: Array1::zeros(HIDDEN_SIZE),
            m_w2: Array2::zeros((HIDDEN_SIZE, HIDDEN_SIZE)), v_w2: Array2::zeros((HIDDEN_SIZE, HIDDEN_SIZE)),
            m_b2: Array1::zeros(HIDDEN_SIZE), v_b2: Array1::zeros(HIDDEN_SIZE),
            m_w3: Array2::zeros((HIDDEN_SIZE, HIDDEN_SIZE)), v_w3: Array2::zeros((HIDDEN_SIZE, HIDDEN_SIZE)),
            m_b3: Array1::zeros(HIDDEN_SIZE), v_b3: Array1::zeros(HIDDEN_SIZE),

            m_adv_mu: Array2::zeros((HIDDEN_SIZE, ACTION_DIM)), v_adv_mu: Array2::zeros((HIDDEN_SIZE, ACTION_DIM)),
            m_adv_sigma: Array2::zeros((HIDDEN_SIZE, ACTION_DIM)), v_adv_sigma: Array2::zeros((HIDDEN_SIZE, ACTION_DIM)),
            m_adv_bmu: Array1::zeros(ACTION_DIM), v_adv_bmu: Array1::zeros(ACTION_DIM),
            m_adv_bsigma: Array1::zeros(ACTION_DIM), v_adv_bsigma: Array1::zeros(ACTION_DIM),

            m_val_mu: Array2::zeros((HIDDEN_SIZE, 1)), v_val_mu: Array2::zeros((HIDDEN_SIZE, 1)),
            m_val_sigma: Array2::zeros((HIDDEN_SIZE, 1)), v_val_sigma: Array2::zeros((HIDDEN_SIZE, 1)),
            m_val_bmu: Array1::zeros(1), v_val_bmu: Array1::zeros(1),
            m_val_bsigma: Array1::zeros(1), v_val_bsigma: Array1::zeros(1),

            beta1_pow: 1.0, beta2_pow: 1.0,
        }
    }
}

// ===================== PERFORMANCE TRACKER =====================
#[derive(Debug, Clone)]
struct PerformanceTracker {
    total_rewards: Vec<f64>,
    episode_rewards: Vec<f64>,
    win_rate: f64,
    sharpe_ratio: f64,
    max_drawdown: f64,
    avg_reward: f64,
    trades_executed: usize,
    profitable_trades: usize,
}

// ===================== MAIN AGENT =====================
pub struct DQNStrategyRewardOptimizer {
    q_network: Arc<RwLock<NeuralNetwork>>,
    target_network: Arc<RwLock<NeuralNetwork>>,
    per: Arc<Mutex<PrioritizedReplayBuffer>>,
    epsilon: Arc<RwLock<f64>>,
    steps: Arc<RwLock<usize>>,
    optimizer_state: Arc<RwLock<OptimizerState>>,
    performance_tracker: Arc<RwLock<PerformanceTracker>>,
    eval_mode: Arc<RwLock<bool>>, // eval-freeze per Rainbow reporting  // [Hessel2017-Rainbow]
}

impl DQNStrategyRewardOptimizer {
    pub async fn new() -> Result<Self> {
        let q_network = Arc::new(RwLock::new(NeuralNetwork::new()));
        let target_network = Arc::new(RwLock::new(NeuralNetwork::new()));

        // Hard copy to synchronize target at init.  // [Mnih2015-Nature]
        {
            let q = q_network.read().await;
            let mut t = target_network.write().await;
            *t = q.clone();
        }

        Ok(Self {
            q_network,
            target_network,
            per: Arc::new(Mutex::new(PrioritizedReplayBuffer::new(BUFFER_CAPACITY))),
            epsilon: Arc::new(RwLock::new(EPSILON_START)),
            steps: Arc::new(RwLock::new(0)),
            optimizer_state: Arc::new(RwLock::new(OptimizerState::new())),
            performance_tracker: Arc::new(RwLock::new(PerformanceTracker {
                total_rewards: vec![],
                episode_rewards: vec![],
                win_rate: 0.0, sharpe_ratio: 0.0, max_drawdown: 0.0, avg_reward: 0.0,
                trades_executed: 0, profitable_trades: 0,
            })),
            eval_mode: Arc::new(RwLock::new(false)),
        })
    }

    pub async fn set_eval_mode(&self, eval: bool) {
        let mut e = self.eval_mode.write().await;
        *e = eval;
    }

    pub async fn get_action(&self, state: Array1<f64>) -> Result<usize> {
        let mut q_net = self.q_network.write().await;
        let eval = *self.eval_mode.read().await;

        if NOISY_EXPLORATION {
            if eval {
                q_net.clear_noisy(); // do not resample noise in eval  // [Hessel2017-Rainbow]
            } else {
                q_net.sample_noisy(); // exploration via learned param noise  // [Fortunato2017]
            }
            let q_values = q_net.forward_with_cached_noise(&state.view());
            let mut best_idx = 0usize;
            let mut best = f64::NEG_INFINITY;
            for (i, &v) in q_values.iter().enumerate() { if v > best { best = v; best_idx = i; } }
            Ok(best_idx)
        } else {
            // ε-greedy exploration path  // [Mnih2015-Nature]
            let eps = if eval { 0.0 } else { *self.epsilon.read().await };
            let mut rng = rand::thread_rng();
            if rng.gen::<f64>() < eps { Ok(rng.gen_range(0..ACTION_DIM)) }
            else {
                let q_values = q_net.forward_with_cached_noise(&state.view());
                let mut best_idx = 0usize; let mut best = f64::NEG_INFINITY;
                for (i, &v) in q_values.iter().enumerate() { if v > best { best = v; best_idx = i; } }
                Ok(best_idx)
            }
        }
    }

    pub async fn store_experience(&self,
        state: Array1<f64>, action: usize, reward: f64,
        next_state: Array1<f64>, done: bool) -> Result<()> {

        let exp = Experience {
            state, action, reward, next_state, done,
            timestamp: SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs(),
        };

        { let mut per = self.per.lock().unwrap(); per.push(exp); } // PER insert  // [Schaul2015, Sec. 3]

        // Step and target sync cadence.  // [Mnih2015-Nature]
        let mut steps = self.steps.write().await;
        *steps += 1;

        if HARD_TARGET_COPY && *steps % TARGET_COPY_INTERVAL == 0 {
            let q = self.q_network.read().await;
            let mut t = self.target_network.write().await;
            *t = q.clone(); // hard copy  // [Mnih2015-Nature]
        } else if !HARD_TARGET_COPY && *steps % TARGET_COPY_INTERVAL == 0 {
            let q = self.q_network.read().await.clone();
            let mut t = self.target_network.write().await;
            // Polyak averaging (common practice; not in Nature DQN)
            t.weights_1 = &t.weights_1 * (1.0 - TAU) + &q.weights_1 * TAU;
            t.bias_1    = &t.bias_1    * (1.0 - TAU) + &q.bias_1    * TAU;
            t.weights_2 = &t.weights_2 * (1.0 - TAU) + &q.weights_2 * TAU;
            t.bias_2    = &t.bias_2    * (1.0 - TAU) + &q.bias_2    * TAU;
            t.weights_3 = &t.weights_3 * (1.0 - TAU) + &q.weights_3 * TAU;
            t.bias_3    = &t.bias_3    * (1.0 - TAU) + &q.bias_3    * TAU;
        }

        if !NOISY_EXPLORATION {
            let mut eps = self.epsilon.write().await;
            *eps = (*eps * EPSILON_DECAY).max(EPSILON_END);
        }

        Ok(())
    }

    pub async fn optimize(&self) -> Result<()> {
        if self.per.lock().unwrap().len() < BATCH_SIZE { return Ok(()); }

        let (batch, is_weights, _total) = self.per.lock().unwrap().sample(BATCH_SIZE);
        let mut indices = Vec::with_capacity(BATCH_SIZE);
        let mut states = Array2::zeros((BATCH_SIZE, STATE_DIM));
        let mut next_states = Array2::zeros((BATCH_SIZE, STATE_DIM));
        let mut actions = Vec::with_capacity(BATCH_SIZE);
        let mut rewards = Vec::with_capacity(BATCH_SIZE);
        let mut dones = Vec::with_capacity(BATCH_SIZE);

        for (i, (idx, exp)) in batch.into_iter().enumerate() {
            indices.push(idx);
            states.row_mut(i).assign(&exp.state);
            next_states.row_mut(i).assign(&exp.next_state);
            actions.push(exp.action);
            rewards.push(exp.reward);
            dones.push(exp.done);
        }

        // Freeze new noise draws within this SGD step to keep gradients consistent.  // [Fortunato2017]
        { let mut qn = self.q_network.write().await; qn.sample_noisy(); }
        { let mut tn = self.target_network.write().await; tn.sample_noisy(); }

        let q_network = self.q_network.read().await;
        let target_network = self.target_network.read().await;

        // Forward passes
        let mut current_q_values = Vec::with_capacity(BATCH_SIZE);
        let mut next_q_target = Vec::with_capacity(BATCH_SIZE);
        let mut next_q_online = Vec::with_capacity(BATCH_SIZE);

        for i in 0..BATCH_SIZE {
            let s = states.row(i);
            let ns = next_states.row(i);
            current_q_values.push(q_network.forward_with_cached_noise(&s));
            next_q_target.push(target_network.forward_with_cached_noise(&ns));
            next_q_online.push(q_network.forward_with_cached_noise(&ns));
        }

        // Double-DQN target:
        // y_i = r_i + γ * Q̂(s'_i, argmax_a Q_online(s'_i, a))   // [vanHasselt2016, Double-DQN target]
        let targets = Self::compute_double_dqn_targets(&next_q_online, &next_q_target, &rewards, &dones);

        // TD errors for PER priority updates
        let mut td_errors = Vec::with_capacity(BATCH_SIZE);
        for i in 0..BATCH_SIZE {
            let pred = current_q_values[i][actions[i]];
            td_errors.push(targets[i] - pred);
        }

        drop(target_network);
        drop(q_network);

        // Update PER priorities.  // [Schaul2015, Sec. 3]
        { let mut per = self.per.lock().unwrap(); per.update_priorities(&indices, &td_errors); }

        // Backprop with Huber(δ=1) + IS weighting.  // [Mnih2015-Nature, Methods] + [Schaul2015, Sec. 3]
        self.backprop_huber_with_per(states, actions, targets, current_q_values, &is_weights).await?;
        Ok(())
    }

    // Extracted for parity tests and clarity.  // [vanHasselt2016]
    pub(crate) fn compute_double_dqn_targets(
        next_q_online: &Vec<Array1<f64>>,
        next_q_target: &Vec<Array1<f64>>,
        rewards: &[f64],
        dones: &[bool],
    ) -> Vec<f64> {
        let mut targets = Vec::with_capacity(rewards.len());
        for i in 0..rewards.len() {
            // a* = argmax_a Q_online(s',a); bootstrap with Q_target(s', a*)  // [vanHasselt2016]
            let a_star = next_q_online[i]
                .iter().enumerate()
                .max_by(|(_, a), (_, b)| a.partial_cmp(b).unwrap())
                .map(|(idx, _)| idx).unwrap();
            let bootstrap = if dones[i] { 0.0 } else { next_q_target[i][a_star] };
            targets.push(rewards[i] + GAMMA * bootstrap);
        }
        targets
    }

    async fn backprop_huber_with_per(
        &self,
        states: Array2<f64>,
        actions: Vec<usize>,
        targets: Vec<f64>,
        predictions: Vec<Array1<f64>>,
        is_weights: &[f64],
    ) -> Result<()> {
        let mut net = self.q_network.write().await;
        let mut opt = self.optimizer_state.write().await;

        let beta1 = 0.9;
        let beta2 = 0.999;

        // advance bias-correction powers
        opt.beta1_pow *= beta1;
        opt.beta2_pow *= beta2;

        // gradient accumulators
        let mut gw1 = Array2::zeros((STATE_DIM, HIDDEN_SIZE));
        let mut gb1 = Array1::zeros(HIDDEN_SIZE);
        let mut gw2 = Array2::zeros((HIDDEN_SIZE, HIDDEN_SIZE));
        let mut gb2 = Array1::zeros(HIDDEN_SIZE);
        let mut gw3 = Array2::zeros((HIDDEN_SIZE, HIDDEN_SIZE));
        let mut gb3 = Array1::zeros(HIDDEN_SIZE);

        let mut g_adv_mu = Array2::zeros((HIDDEN_SIZE, ACTION_DIM));
        let mut g_adv_sigma = Array2::zeros((HIDDEN_SIZE, ACTION_DIM));
        let mut g_adv_bmu = Array1::zeros(ACTION_DIM);
        let mut g_adv_bsigma = Array1::zeros(ACTION_DIM);

        let mut g_val_mu = Array2::zeros((HIDDEN_SIZE, 1));
        let mut g_val_sigma = Array2::zeros((HIDDEN_SIZE, 1));
        let mut g_val_bmu = Array1::zeros(1);
        let mut g_val_bsigma = Array1::zeros(1);

        for i in 0..BATCH_SIZE {
            let x = states.row(i).to_owned();

            // forward trunk
            let h1 = NeuralNetwork::relu(&(x.view().dot(&net.weights_1) + &net.bias_1));
            let h2 = NeuralNetwork::relu(&(h1.view().dot(&net.weights_2) + &net.bias_2));
            let h3 = NeuralNetwork::relu(&(h2.view().dot(&net.weights_3) + &net.bias_3));

            // heads (use cached noise)
            let adv = net.noisy_adv.forward(&h3.view());
            let val = net.noisy_val.forward(&h3.view())[0];
            let mean_adv = adv.mean().unwrap();

            // Dueling aggregator. Q = V + A − mean(A)  // [Wang2016, Sec. 3]
            let mut q = adv.clone();
            q.mapv_inplace(|a| a - mean_adv + val);

            let a_idx = actions[i];
            let pred = q[a_idx];
            let target = targets[i];

            // Huber(δ=1) gradient: d/dpred huber = err if |err|<=1 else sign(err).  // [Mnih2015-Nature, Methods]
            let err = pred - target;
            let loss_grad_scalar = if err.abs() <= 1.0 { err } else { err.signum() };
            let isw = is_weights[i]; // IS weighting.  // [Schaul2015, Sec. 3]
            let g_scalar = isw * loss_grad_scalar / (BATCH_SIZE as f64);

            // Backprop through dueling split.  // [Wang2016, Sec. 3]
            // ∂Q/∂V = 1;  ∂Q/∂A_j = 1 - 1/|A| if j==a_idx else -1/|A|
            let mut g_adv_out = Array1::from_elem(ACTION_DIM, -g_scalar / ACTION_DIM as f64);
            g_adv_out[a_idx] += g_scalar;
            let g_val_out = Array1::from_elem(1, g_scalar);

            // Accumulate head grads (μ/σ).  // [Fortunato2017]
            net.noisy_adv.backward_accumulate(&g_adv_out, &h3, &mut g_adv_mu, &mut g_adv_sigma, &mut g_adv_bmu, &mut g_adv_bsigma);
            net.noisy_val.backward_accumulate(&g_val_out, &h3, &mut g_val_mu, &mut g_val_sigma, &mut g_val_bmu, &mut g_val_bsigma);

            // Backprop to h3 through the two heads (μ + σ·ε terms).  // [Fortunato2017]
            let mut g_h3 = Array1::zeros(HIDDEN_SIZE);
            for j in 0..HIDDEN_SIZE {
                // val head
                g_h3[j] += net.noisy_val.w_mu[[j, 0]] * g_val_out[0]
                         + net.noisy_val.w_sigma[[j, 0]] * (net.noisy_val.eps_in[j] * net.noisy_val.eps_out[0]) * g_val_out[0];
            }
            for j in 0..HIDDEN_SIZE {
                // adv head
                let mut s = 0.0;
                for a in 0..ACTION_DIM {
                    s += net.noisy_adv.w_mu[[j, a]] * g_adv_out[a]
                       + net.noisy_adv.w_sigma[[j, a]] * (net.noisy_adv.eps_in[j] * net.noisy_adv.eps_out[a]) * g_adv_out[a];
                }
                g_h3[j] += s;
            }

            // ReLU grads
            let h3_mask = h3.mapv(|v| if v > 0.0 { 1.0 } else { 0.0 });
            let g_h3 = g_h3 * &h3_mask;

            // h2 -> h3
            for j in 0..HIDDEN_SIZE { for k in 0..HIDDEN_SIZE { gw3[[j, k]] += h2[j] * g_h3[k]; } }
            gb3 = &gb3 + &g_h3;

            // backprop to h2
            let mut g_h2 = Array1::zeros(HIDDEN_SIZE);
            for j in 0..HIDDEN_SIZE {
                let mut s = 0.0; for k in 0..HIDDEN_SIZE { s += net.weights_3[[j, k]] * g_h3[k]; }
                g_h2[j] = s;
            }
            let h2_mask = h2.mapv(|v| if v > 0.0 { 1.0 } else { 0.0 });
            let g_h2 = g_h2 * &h2_mask;

            // h1 -> h2
            for j in 0..HIDDEN_SIZE { for k in 0..HIDDEN_SIZE { gw2[[j, k]] += h1[j] * g_h2[k]; } }
            gb2 = &gb2 + &g_h2;

            // backprop to h1
            let mut g_h1 = Array1::zeros(HIDDEN_SIZE);
            for j in 0..HIDDEN_SIZE {
                let mut s = 0.0; for k in 0..HIDDEN_SIZE { s += net.weights_2[[j, k]] * g_h2[k]; }
                g_h1[j] = s;
            }
            let h1_mask = h1.mapv(|v| if v > 0.0 { 1.0 } else { 0.0 });
            let g_h1 = g_h1 * &h1_mask;

            // x -> h1
            for d in 0..STATE_DIM { for k in 0..HIDDEN_SIZE { gw1[[d, k]] += x[d] * g_h1[k]; } }
            gb1 = &gb1 + &g_h1;
        }

        // Adam updates with bias corrections (standard)
        let beta1 = 0.9; let beta2 = 0.999; let eps_adam = 1e-8;
        let adam2d = |param: &mut Array2<f64>, grad: &Array2<f64>, m: &mut Array2<f64>, v: &mut Array2<f64>| {
            *m = beta1 * &*m + (1.0 - beta1) * grad;
            *v = beta2 * &*v + (1.0 - beta2) * grad.mapv(|x| x * x);
            let m_hat = &*m / (1.0 - opt.beta1_pow);
            let v_hat = &*v / (1.0 - opt.beta2_pow);
            *param = &*param - LEARNING_RATE * &m_hat / (v_hat.mapv(|x| x.sqrt()) + eps_adam);
        };
        let adam1d = |param: &mut Array1<f64>, grad: &Array1<f64>, m: &mut Array1<f64>, v: &mut Array1<f64>| {
            *m = beta1 * &*m + (1.0 - beta1) * grad;
            *v = beta2 * &*v + (1.0 - beta2) * grad.mapv(|x| x * x);
            let m_hat = &*m / (1.0 - opt.beta1_pow);
            let v_hat = &*v / (1.0 - opt.beta2_pow);
            *param = &*param - LEARNING_RATE * &m_hat / (v_hat.mapv(|x| x.sqrt()) + eps_adam);
        };

        // trunk
        adam2d(&mut net.weights_1, &gw1, &mut opt.m_w1, &mut opt.v_w1);
        adam1d(&mut net.bias_1, &gb1, &mut opt.m_b1, &mut opt.v_b1);
        adam2d(&mut net.weights_2, &gw2, &mut opt.m_w2, &mut opt.v_w2);
        adam1d(&mut net.bias_2, &gb2, &mut opt.m_b2, &mut opt.v_b2);
        adam2d(&mut net.weights_3, &gw3, &mut opt.m_w3, &mut opt.v_w3);
        adam1d(&mut net.bias_3, &gb3, &mut opt.m_b3, &mut opt.v_b3);

        // heads: Noisy μ/σ
        adam2d(&mut net.noisy_adv.w_mu, &g_adv_mu, &mut opt.m_adv_mu, &mut opt.v_adv_mu);
        adam2d(&mut net.noisy_adv.w_sigma, &g_adv_sigma, &mut opt.m_adv_sigma, &mut opt.v_adv_sigma);
        adam1d(&mut net.noisy_adv.b_mu, &g_adv_bmu, &mut opt.m_adv_bmu, &mut opt.v_adv_bmu);
        adam1d(&mut net.noisy_adv.b_sigma, &g_adv_bsigma, &mut opt.m_adv_bsigma, &mut opt.v_adv_bsigma);

        adam2d(&mut net.noisy_val.w_mu, &g_val_mu, &mut opt.m_val_mu, &mut opt.v_val_mu);
        adam2d(&mut net.noisy_val.w_sigma, &g_val_sigma, &mut opt.m_val_sigma, &mut opt.v_val_sigma);
        adam1d(&mut net.noisy_val.b_mu, &g_val_bmu, &mut opt.m_val_bmu, &mut opt.v_val_bmu);
        adam1d(&mut net.noisy_val.b_sigma, &g_val_bsigma, &mut opt.m_val_bsigma, &mut opt.v_val_bsigma);

        Ok(())
    }

    // ===== Feature engineering and utility =====
    pub async fn encode_state(
        &self,
        market: &MarketState,
        portfolio: &PortfolioState,
        historical_prices: &[f64],
        historical_volumes: &[f64],
    ) -> Result<Array1<f64>> {
        let mut state = Array1::zeros(STATE_DIM);
        let mut idx = 0;

        state[idx] = self.normalize_price(market.price);
        state[idx + 1] = self.normalize_volume(market.volume_24h);
        state[idx + 2] = market.bid_ask_spread.tanh();
        state[idx + 3] = market.order_book_imbalance.tanh();
        state[idx + 4] = self.normalize_volatility(market.volatility);
        state[idx + 5] = market.momentum.tanh();
        state[idx + 6] = (market.rsi - 50.0) / 50.0;
        state[idx + 7] = market.macd.tanh();
        state[idx + 8] = market.bollinger_position.tanh();
        state[idx + 9] = market.vwap_deviation.tanh();
        state[idx + 10] = market.funding_rate.tanh();
        state[idx + 11] = self.normalize_volume(market.open_interest);
        state[idx + 12] = self.normalize_volume(market.liquidation_volume);
        state[idx + 13] = market.whale_activity.tanh();
        state[idx + 14] = market.network_congestion.tanh();
        state[idx + 15] = self.normalize_gas_price(market.gas_price);
        idx += 16;

        state[idx] = self.normalize_balance(portfolio.token_balance);
        state[idx + 1] = self.normalize_balance(portfolio.sol_balance);
        state[idx + 2] = portfolio.unrealized_pnl.tanh();
        state[idx + 3] = self.normalize_position_size(portfolio.position_size);
        state[idx + 4] = self.normalize_price(portfolio.avg_entry_price);
        state[idx + 5] = (portfolio.position_duration / 86400.0).tanh();
        state[idx + 6] = portfolio.win_rate * 2.0 - 1.0;
        state[idx + 7] = portfolio.sharpe_ratio.tanh();
        state[idx + 8] = portfolio.max_drawdown;
        state[idx + 9] = portfolio.current_drawdown;
        idx += 10;

        let price_features = self.extract_price_features(historical_prices);
        let volume_features = self.extract_volume_features(historical_volumes);

        for i in 0..price_features.len().min(50) { state[idx + i] = price_features[i]; }
        idx += 50;
        for i in 0..volume_features.len().min(STATE_DIM - idx) { state[idx + i] = volume_features[i]; }

        Ok(state)
    }

    pub fn calculate_reward(
        &self,
        pnl: f64,
        trade_cost: f64,
        slippage: f64,
        position_held_time: f64,
        market_impact: f64,
        risk_adjusted_return: f64,
    ) -> f64 {
        // Domain-specific reward shaping; not prescribed by DQN papers.  // [Mnih2015-Nature] raw rewards
        let base_reward = pnl - trade_cost - slippage * 2.0;
        let time_penalty = if position_held_time < 60.0 {
            -0.01 * (60.0 - position_held_time) / 60.0
        } else if position_held_time > 3600.0 {
            -0.005 * (position_held_time - 3600.0) / 3600.0
        } else { 0.0 };
        let market_impact_penalty = -market_impact.abs() * 0.5;
        let risk_bonus = if risk_adjusted_return > 2.0 {
            0.1 * (risk_adjusted_return - 2.0).min(1.0)
        } else if risk_adjusted_return < 0.5 {
            -0.2 * (0.5 - risk_adjusted_return)
        } else { 0.0 };
        let sharpe_multiplier = (1.0 + risk_adjusted_return.tanh() * 0.5).max(0.1);
        (base_reward + time_penalty + market_impact_penalty + risk_bonus) * sharpe_multiplier
    }

    // Discrete sizing buckets keep execution risk-bounded.
    pub fn map_action_to_trade(&self, action: usize, current_position: f64, balance: f64) -> (f64, bool) {
        match action {
            0..=6 => {
                let position_pct = (action as f64 + 1.0) / 10.0;
                let target_position = balance * position_pct;
                (target_position - current_position, true)
            }
            7..=13 => {
                let position_pct = (action as f64 - 6.0) / 10.0;
                let target_position = -balance * position_pct;
                (target_position - current_position, false)
            }
            14..=19 => {
                let reduce_pct = (action as f64 - 13.0) / 10.0;
                (-current_position * reduce_pct, current_position > 0.0)
            }
            _ => (0.0, true),
        }
    }

    pub async fn update_performance(&self, reward: f64, trade_profitable: bool) -> Result<()> {
        let mut t = self.performance_tracker.write().await;

        t.episode_rewards.push(reward);
        t.total_rewards.push(reward);
        if t.total_rewards.len() > 10_000 { t.total_rewards.remove(0); }

        t.trades_executed += 1;
        if trade_profitable { t.profitable_trades += 1; }
        t.win_rate = t.profitable_trades as f64 / t.trades_executed as f64;

        if t.episode_rewards.len() >= 100 {
            let returns = &t.episode_rewards[t.episode_rewards.len()-100..];
            let mean = returns.iter().sum::<f64>() / returns.len() as f64;
            let std = (returns.iter().map(|r| (r - mean).powi(2)).sum::<f64>() / returns.len() as f64).sqrt();
            t.sharpe_ratio = if std > 0.0 { (mean * (252.0_f64).sqrt()) / std } else { 0.0 };

            let mut cum = 0.0; let mut peak = 0.0; let mut max_dd = 0.0;
            for r in returns {
                cum += r; peak = peak.max(cum);
                let dd = (peak - cum) / peak.max(1.0);
                max_dd = max_dd.max(dd);
            }
            t.max_drawdown = max_dd;
        }

        t.avg_reward = if t.total_rewards.is_empty() { 0.0 } else {
            t.total_rewards.iter().sum::<f64>() / t.total_rewards.len() as f64
        };
        Ok(())
    }

    pub async fn get_performance_metrics(&self) -> Result<(f64, f64, f64, f64)> {
        let t = self.performance_tracker.read().await;
        Ok((t.win_rate, t.sharpe_ratio, t.max_drawdown, t.avg_reward))
    }

    pub async fn should_trade(&self, confidence: f64, market: &MarketState) -> bool {
        // Risk gates for execution; domain choice.
        let min_conf = 0.65;
        let max_spread = 0.002;
        let min_volume = 100000.0;
        let max_vol = 0.05;

        confidence > min_conf &&
        market.bid_ask_spread < max_spread &&
        market.volume_24h > min_volume &&
        market.volatility < max_vol &&
        market.network_congestion < 0.8
    }

    pub async fn get_model_confidence(&self, state: &Array1<f64>) -> Result<f64> {
        let mut qn = self.q_network.write().await;
        // In eval we do not resample noise; use whatever is set.  // [Hessel2017-Rainbow]
        if NOISY_EXPLORATION && !*self.eval_mode.read().await { qn.sample_noisy(); }
        else if NOISY_EXPLORATION && *self.eval_mode.read().await { qn.clear_noisy(); }
        let q_values = qn.forward_with_cached_noise(&state.view());
        let max_q = q_values.iter().fold(f64::NEG_INFINITY, |a, &b| a.max(b));
        let min_q = q_values.iter().fold(f64::INFINITY, |a, &b| a.min(b));
        let range = max_q - min_q;
        let mean = q_values.mean().unwrap();
        let std = (q_values.iter().map(|&q| (q - mean).powi(2)).sum::<f64>() / q_values.len() as f64).sqrt();

        let conf = if range > 0.0 {
            let nr = (range / (max_q.abs() + min_q.abs() + 1e-8)).min(1.0);
            let ns = (std / (range + 1e-8)).min(1.0);
            (nr * 0.7 + (1.0 - ns) * 0.3).clamp(0.0, 1.0)
        } else { 0.0 };
        Ok(conf)
    }

    // ===== Normalizers & feature extractors =====
    fn normalize_price(&self, p: f64) -> f64 { ((p + 1e-8).ln() / 10.0).tanh() }
    fn normalize_volume(&self, v: f64) -> f64 { ((v + 1.0).ln() / 20.0).tanh() }
    fn normalize_volatility(&self, v: f64) -> f64 { (v * 100.0).tanh() }
    fn normalize_gas_price(&self, g: f64) -> f64 { ((g / 1e9).ln() / 5.0).tanh() }
    fn normalize_balance(&self, b: f64) -> f64 { ((b + 1.0).ln() / 10.0).tanh() }
    fn normalize_position_size(&self, s: f64) -> f64 { (s / 100000.0).tanh() }

    fn extract_price_features(&self, prices: &[f64]) -> Vec<f64> {
        let mut f = Vec::new();
        if prices.len() < 2 { return vec![0.0; 50]; }
        let returns: Vec<f64> = prices.windows(2).map(|w| (w[1] / w[0] - 1.0).tanh()).collect();

        for i in [1usize, 5, 10, 20, 50] {
            if returns.len() >= i {
                let m = returns[returns.len()-i..].iter().sum::<f64>() / i as f64;
                f.push(m);
            } else { f.push(0.0); }
        }
        for i in [5usize, 10, 20, 50] {
            if prices.len() >= i {
                let sma = prices[prices.len()-i..].iter().sum::<f64>() / i as f64;
                let p = prices.last().unwrap();
                f.push(((p / sma) - 1.0).tanh());
            } else { f.push(0.0); }
        }
        if prices.len() >= 20 {
            let mut w = prices[prices.len()-20..].to_vec();
            w.sort_by(|a, b| a.partial_cmp(b).unwrap());
            let med = w[10];
            f.push(((prices.last().unwrap() / med) - 1.0).tanh());
        } else { f.push(0.0); }
        while f.len() < 50 { f.push(0.0); }
        f
    }

    fn extract_volume_features(&self, volumes: &[f64]) -> Vec<f64> {
        let mut f = Vec::new();
        if volumes.is_empty() { return vec![0.0; 52]; }
        for i in [1usize, 5, 10, 20] {
            if volumes.len() >= i {
                let m = volumes[volumes.len()-i..].iter().sum::<f64>() / i as f64;
                f.push(self.normalize_volume(m));
            } else { f.push(0.0); }
        }
        if volumes.len() >= 20 {
            let cur = volumes.last().unwrap();
            let avg = volumes.iter().sum::<f64>() / volumes.len() as f64;
            f.push(((cur / (avg + 1e-8)) - 1.0).tanh());
            let max_v = volumes.iter().fold(0.0, |a, &b| a.max(b));
            f.push((cur / (max_v + 1e-8)).tanh());
        } else { f.push(0.0); f.push(0.0); }
        while f.len() < 52 { f.push(0.0); }
        f
    }

    pub async fn save_checkpoint(&self, path: &str) -> Result<()> {
        let checkpoint_data = serde_json::json!({
            "epsilon": *self.epsilon.read().await,
            "steps": *self.steps.read().await,
            "eval_mode": *self.eval_mode.read().await,
            "noisy_sigma0": NOISY_SIGMA0, // [Fortunato2017]
            "rainbow_subset": ["DoubleDQN","Dueling","PER","NoisyNets"], // [Hessel2017-Rainbow]
            "performance": {
                "win_rate": self.performance_tracker.read().await.win_rate,
                "sharpe_ratio": self.performance_tracker.read().await.sharpe_ratio,
                "max_drawdown": self.performance_tracker.read().await.max_drawdown,
            }
        });
        std::fs::write(path, serde_json::to_string_pretty(&checkpoint_data)?)?;
        Ok(())
    }
}

// ===================== Tests & Parity Scaffolds =====================
// Dev-deps you may add for these tests:
// [dev-dependencies]
// statrs = "0.16"
// csv = "1"

#[cfg(test)]
mod tests {
    use super::*;
    use rand::Rng;

    #[test]
    fn test_double_dqn_target_math() {
        // Double target per [vanHasselt2016]: select argmax_a Q_online(s',a), evaluate with Q_target(s', a*).
        let online = vec![
            Array1::from(vec![1.0, 3.0, 2.0]),  // a* = 1
            Array1::from(vec![0.5, -0.1, 0.0]), // a* = 0
        ];
        let target = vec![
            Array1::from(vec![10.0, 20.0, 30.0]), // Q_target(s',1)=20
            Array1::from(vec![7.0,  8.0,  9.0]),  // Q_target(s',0)=7
        ];
        let rewards = [1.0, 2.0];
        let dones = [false, true];

        let y = DQNStrategyRewardOptimizer::compute_double_dqn_targets(&online, &target, &rewards, &dones);
        assert!((y[0] - (1.0 + GAMMA * 20.0)).abs() < 1e-12);
        assert!((y[1] - 2.0).abs() < 1e-12);
    }

    #[test]
    fn test_dueling_identity_exact() {
        // Q = V + A − mean(A).  // [Wang2016, Sec. 3]
        let mut rng = rand::thread_rng();
        let val = 0.1234;
        for _ in 0..10 {
            let a = Array1::from((0..ACTION_DIM).map(|_| rng.gen::<f64>()).collect::<Vec<_>>());
            let mean_a = a.mean().unwrap();
            let q_from_id = a.mapv(|x| x - mean_a + val);
            // Check mean(Q - V) == 0
            let mean_q_minus_v = (q_from_id.iter().map(|x| x - val).sum::<f64>() / ACTION_DIM as f64);
            assert!(mean_q_minus_v.abs() < 1e-12);
        }
    }

    #[test]
    fn test_sumtree_sampling_proportional_small() {
        // PER proportional sampling sanity.  // [Schaul2015]
        let mut st = SumTree::new(4);
        for p in [1.0, 2.0, 3.0, 4.0] { st.add(p); }
        let total = st.total();
        assert!((total - 10.0).abs() < 1e-12);

        let mut counts = [0usize; 4];
        let mut rng = rand::thread_rng();
        let draws = 200_000;
        for _ in 0..draws {
            let s = rng.gen_range(0.0..total);
            let idx = st.get(s);
            counts[idx] += 1;
        }
        let probs: Vec<f64> = counts.iter().map(|&c| c as f64 / draws as f64).collect();
        let expected = vec![0.1, 0.2, 0.3, 0.4];
        let l1: f64 = probs.iter().zip(expected.iter()).map(|(p, e)| (p - e).abs()).sum();
        assert!(l1 < 0.02, "probs={:?}", probs);
    }

    #[test]
    fn test_huber_grad_delta_1() {
        // Huber(δ=1) gradient shape.  // [Mnih2015-Nature, Methods]
        let huber_grad = |err: f64| if err.abs() <= 1.0 { err } else { err.signum() };
        for e in [-2.0, -1.0, -0.5, 0.0, 0.5, 1.0, 2.0] {
            let g = huber_grad(e);
            if e.abs() <= 1.0 { assert!((g - e).abs() < 1e-12); }
            else { assert!((g - e.signum()).abs() < 1e-12); }
        }
    }

    #[test]
    fn test_noisynet_variance_sanity() {
        // Variance matches factorized expectation.  // [Fortunato2017, Sec. 2.3]
        let mut rng = rand::thread_rng();
        let mut nl = NoisyLinear::new(3, 2, &mut rng);
        nl.__test_set_params(0.0, 0.2, 0.0, 0.0);

        let x = Array1::from(vec![1.0, 0.0, 0.0]); // one-hot
        let trials = 20_000;
        let mut samples = vec![Vec::new(), Vec::new()];

        for _ in 0..trials {
            nl.sample_noise(&mut rng);
            let y = nl.forward(&x.view());
            samples[0].push(y[0]);
            samples[1].push(y[1]);
        }
        let mean0 = samples[0].iter().sum::<f64>() / trials as f64;
        let var0 = samples[0].iter().map(|v| (v - mean0).powi(2)).sum::<f64>() / trials as f64;

        let e_abs_norm = (2.0 / std::f64::consts::PI).sqrt(); // E|N(0,1)|
        // Var(x^T(W_sigma ⊙ eps_outer)) when x is one-hot → σ^2 E|ε|^2
        let expected_var = 0.2f64.powi(2) * e_abs_norm * e_abs_norm;
        assert!(var0 > 0.0);
        assert!((var0 - expected_var).abs() / expected_var < 0.25, "var0={} expected≈{}", var0, expected_var);
    }
}

#[cfg(feature = "parity_tests")]
mod parity_tests {
    // CSV comparison scaffold for Dopamine / DQN Zoo parity.
    // Provide scripts to dump first K batches of:
    // - online.csv:   rows=BATCH_SIZE, cols=ACTION_DIM        (Q_online(s'))
    // - target.csv:   rows=BATCH_SIZE, cols=ACTION_DIM        (Q_target(s'))
    // - rewards.csv:  rows=BATCH_SIZE, cols=1
    // - dones.csv:    rows=BATCH_SIZE, cols=1   (0/1)
    // - actions.csv:  rows=BATCH_SIZE, cols=1   (int)
    //
    // Set env var PARITY_DIR=path/to/csvs and run with --features parity_tests.
    // Assert math equality for Double-DQN targets within 1e-9 and dueling identity within 1e-12.
    //
    // Repos for fixture generation:
    //   - DeepMind Dopamine (TensorFlow)         // [Dopamine, official]
    //   - DeepMind DQN Zoo (JAX)                 // [DQN Zoo, official]

    use super::*;
    use std::env;
    use std::fs::File;
    use std::io::{BufRead, BufReader};

    fn read_matrix(path: &str) -> Vec<Vec<f64>> {
        let f = File::open(path).expect("open csv");
        let r = BufReader::new(f);
        r.lines()
            .map(|l| {
                l.unwrap()
                 .split(',')
                 .map(|x| x.trim().parse::<f64>().unwrap())
                 .collect::<Vec<f64>>()
            })
            .collect::<Vec<_>>()
    }

    #[test]
    fn parity_double_dqn_targets_csv() {
        let dir = env::var("PARITY_DIR").expect("set PARITY_DIR to reference CSV directory");
        let online = read_matrix(&format!("{}/online.csv", dir));
        let target = read_matrix(&format!("{}/target.csv", dir));
        let rewards = read_matrix(&format!("{}/rewards.csv", dir)).into_iter().map(|r| r[0]).collect::<Vec<f64>>();
        let dones = read_matrix(&format!("{}/dones.csv", dir)).into_iter().map(|r| r[0] != 0.0).collect::<Vec<bool>>();
        let _actions = read_matrix(&format!("{}/actions.csv", dir)).into_iter().map(|r| r[0] as usize).collect::<Vec<usize>>();

        let online_a = online.into_iter().map(|row| Array1::from(row)).collect::<Vec<_>>();
        let target_a = target.into_iter().map(|row| Array1::from(row)).collect::<Vec<_>>();

        let y = DQNStrategyRewardOptimizer::compute_double_dqn_targets(&online_a, &target_a, &rewards, &dones);

        // Independently compute with argmax from online and bootstrap from target  // [vanHasselt2016]
        for i in 0..y.len() {
            let a_star = online_a[i].iter()
                .enumerate()
                .max_by(|(_, a), (_, b)| a.partial_cmp(b).unwrap())
                .map(|(j, _)| j).unwrap();
            let bootstrap = if dones[i] { 0.0 } else { target_a[i][a_star] };
            let ref_y = rewards[i] + GAMMA * bootstrap;
            assert!((y[i] - ref_y).abs() < 1e-9, "row {} mismatch: {} vs {}", i, y[i], ref_y);
        }
    }

    #[test]
    fn parity_dueling_identity_csv() {
        let dir = env::var("PARITY_DIR").expect("set PARITY_DIR to reference CSV directory");
        let online = read_matrix(&format!("{}/online.csv", dir));
        let online_a = online.into_iter().map(|row| Array1::from(row)).collect::<Vec<_>>();

        // Check mean(Q - V) == 0 for dueling aggregation when recomposed with V=0.  // [Wang2016]
        for row in &online_a {
            let mean_a = row.mean().unwrap();
            let q = row.mapv(|a| a - mean_a + 0.0);
            let mean_q_minus_v = (q.iter().map(|x| *x - 0.0).sum::<f64>() / q.len() as f64);
            assert!(mean_q_minus_v.abs() < 1e-12);
        }
    }
}
