use std::collections::HashMap;

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use rand::{prelude::*, rngs::StdRng, SeedableRng};
use serde::{Deserialize, Serialize};
use solana_sdk::pubkey::Pubkey;
use rust_decimal::prelude::*;
use rust_decimal_macros::dec;
use indexmap::IndexMap;
use ahash::{AHasher, RandomState, AHashSet};
use smallvec::SmallVec;
use lru::LruCache;
use std::hash::{Hash, Hasher};
use tracing::{debug, info, instrument, span, Level};
use prometheus::{IntCounter, IntCounterVec, Gauge, Histogram, HistogramOpts, register_int_counter, register_int_counter_vec, register_gauge, register_histogram};
use std::sync::OnceLock;

// === Zobrist/TT helpers (splitmix64, constants, and load helper) ===
#[inline(always)]
fn smix64(mut x: u64) -> u64 {
    // SplitMix64 mixer (public domain); deterministic and fast
    x = x.wrapping_add(0x9E37_79B9_7F4A_7C15);
    let mut z = x;
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

const ZOBRIST_BASE: u64 = 0xA1B2_C3D4_E5F6_0011;
const ZOBRIST_K0:   u64 = 0x9E37_79B9_85EB_CA77;
const ZOBRIST_K1:   u64 = 0xC2B2_AE3D_27D4_EB4F;

#[inline(always)]
fn load_u64_le(bytes: &[u8]) -> u64 {
    let mut buf = [0u8; 8];
    buf.copy_from_slice(&bytes[0..8]);
    u64::from_le_bytes(buf)
}

// --- Metrics registrars (lazy singletons) ---
fn NODES_CREATED() -> &'static IntCounter {
    static INST: OnceLock<IntCounter> = OnceLock::new();
    INST.get_or_init(|| register_int_counter!("mcts_nodes_created", "Number of MCTS nodes created").unwrap())
}
fn ROLLOUTS() -> &'static IntCounter {
    static INST: OnceLock<IntCounter> = OnceLock::new();
    INST.get_or_init(|| register_int_counter!("mcts_rollouts", "Number of simulations/rollouts executed").unwrap())
}
fn SIM_MS() -> &'static Histogram {
    static INST: OnceLock<Histogram> = OnceLock::new();
    INST.get_or_init(|| register_histogram!("mcts_simulate_ms", "Simulation runtime in ms").unwrap())
}
fn ACTION_SEL() -> &'static IntCounterVec {
    static INST: OnceLock<IntCounterVec> = OnceLock::new();
    INST.get_or_init(|| register_int_counter_vec!("mcts_action_selection_counts", "Counts per action kind", &["action"]).unwrap())
}
fn LOOKUPS() -> &'static IntCounter {
    static INST: OnceLock<IntCounter> = OnceLock::new();
    INST.get_or_init(|| register_int_counter!("mcts_cache_lookups", "Transposition table lookups").unwrap())
}
fn HITS() -> &'static IntCounter {
    static INST: OnceLock<IntCounter> = OnceLock::new();
    INST.get_or_init(|| register_int_counter!("mcts_cache_hits", "Transposition table cache hits").unwrap())
}
fn CACHE_RATIO() -> &'static Gauge {
    static INST: OnceLock<Gauge> = OnceLock::new();
    INST.get_or_init(|| register_gauge!("mcts_cache_hit_ratio", "Cache hit ratio (hits/lookups)").unwrap())
}
fn ROOT_DIR_NOISE() -> &'static IntCounter {
    static INST: OnceLock<IntCounter> = OnceLock::new();
    INST.get_or_init(|| register_int_counter!("mcts_root_dirichlet_noise_applied", "Times root Dirichlet noise was applied").unwrap())
}

#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct MarketState {
    pub token_mint: Pubkey,
    pub price: u64,
    pub volume_24h: u64,
    pub liquidity: u64,
    pub momentum: i32,
    pub volatility: u32,
    pub spread_bps: u16,
    pub block_height: u64,
    pub timestamp: u64,
}

#[derive(Clone, Copy)]
struct TTEntry { mean: f64, m2: f64, n: u64, epoch: u64 }

// === [IMPROVED⁴: PIECE B7] widen action-shard in the transposition key
type TTKey = (u64, u16);

struct EvalCache { map: IndexMap<TTKey, TTEntry, RandomState>, cap: usize }

impl EvalCache {
    fn new(cap: usize) -> Self {
        Self {
            map: IndexMap::with_capacity_and_hasher(cap, RandomState::default()),
            cap,
        }
    }
    fn clear(&mut self) { self.map.clear(); }
    fn get(&mut self, k: TTKey) -> Option<f64> {
        if let Some((idx, v)) = self.map.get_full(&k) {
            // touch to mark as recently used: remove and reinsert at end
            let entry = TTEntry { mean: v.mean, m2: v.m2, n: v.n, epoch: v.epoch };
            self.map.shift_remove_index(idx);
            self.map.insert(k, entry);
            Some(v.mean)
        } else { None }
    }
    fn update(&mut self, k: TTKey, x: f64) {
        if let Some(v) = self.map.get_mut(&k) {
            v.n = v.n.saturating_add(1);
            let delta = x - v.mean;
            v.mean += delta / v.n as f64;
            v.m2 += delta * (x - v.mean);
            return;
        }
        if self.map.len() >= self.cap { let _ = self.map.shift_remove_index(0); }
        self.map.insert(k, TTEntry { mean: x, m2: 0.0, n: 1, epoch: 0 });
    }

    #[inline(always)]
    fn update_epoch(&mut self, k: TTKey, x: f64, epoch: u64) {
        if let Some(v) = self.map.get_mut(&k) {
            v.n = v.n.saturating_add(1);
            let delta = x - v.mean;
            v.mean += delta / v.n as f64;
            v.m2 += delta * (x - v.mean);
            v.epoch = epoch;
            return;
        }
        if self.map.len() >= self.cap { let _ = self.map.shift_remove_index(0); }
        self.map.insert(k, TTEntry { mean: x, m2: 0.0, n: 1, epoch });
    }

    #[inline(always)]
    fn peek_entry(&self, k: &TTKey) -> Option<TTEntry> { self.map.get(k).copied() }
}

#[cfg(test)]
mod prop {
    use super::*;
    use proptest::prelude::*;

    proptest! {
        #[test]
        fn no_panic_random_states(
            price in 1u64..1_000_000_000u64,
            volume in 0u64..50_000_000_000u64,
            liquidity in 1_000u64..50_000_000_000u64,
            momentum in -100i32..=100i32,
            volatility in 0u32..=200u32,
            spread in 0u16..=2000u16,
            bh in 0u64..1_000_000_000u64,
            ts in 1_600_000_000u64..2_200_000_000u64,
        ) {
            let s = MarketState {
                token_mint: Pubkey::new_unique(),
                price,
                volume_24h: volume,
                liquidity,
                momentum,
                volatility,
                spread_bps: spread,
                block_height: bh,
                timestamp: ts,
            };
            let mut m = MonteCarloTreeSearcher::new(MCTSConfig::default());
            let _ = m.search(s);
        }
    }

    #[test]
    fn determinism_with_seed() {
        let state = MarketState {
            token_mint: Pubkey::new_unique(),
            price: 1_234_567,
            volume_24h: 9_999_999,
            liquidity: 10_000_000,
            momentum: 10,
            volatility: 30,
            spread_bps: 50,
            block_height: 12345,
            timestamp: 1_800_000_000,
        };
        let cfg = MCTSConfig::default();
        let mut a = MonteCarloTreeSearcher::new(cfg.clone());
        a.set_seed(42);
        let mut b = MonteCarloTreeSearcher::new(cfg);
        b.set_seed(42);
        let act_a = a.search(state.clone());
        let act_b = b.search(state);
        // With same seed and config, decisions should match
        assert_eq!(format!("{:?}", act_a), format!("{:?}", act_b));
    }
}

// === [IMPROVED⁴: PIECE B8] Quantized Zobrist to stabilize TT keys
const Q_PRICE_LG2: u32   = 8;   // bucket = 256 price units
const Q_LIQ_LG2: u32     = 12;  // bucket = 4096 liquidity units
const Q_VOL_STEP: u32     = 2;   // bucket volatility in steps of 2
const Q_SPREAD_STEP: u16  = 2;   // bucket spread in steps of 2 bps
const Q_MOM_STEP: i32     = 2;   // bucket momentum in steps of 2
const TT_QUANTize: bool   = true; // on by default

#[inline(always)]
fn zobrist_market_state(s: &MarketState) -> u64 {
    let b = s.token_mint.as_ref();
    debug_assert_eq!(b.len(), 32);

    let (price_q, liq_q, vol_q, spr_q, mom_q) = if TT_QUANTize {
        (
            s.price >> Q_PRICE_LG2,
            s.liquidity >> Q_LIQ_LG2,
            (s.volatility / Q_VOL_STEP) * Q_VOL_STEP,
            ((s.spread_bps / Q_SPREAD_STEP) * Q_SPREAD_STEP) as u16,
            ((s.momentum / Q_MOM_STEP) * Q_MOM_STEP) as i32,
        )
    } else {
        (s.price, s.liquidity, s.volatility, s.spread_bps, s.momentum)
    };

    let mut z = ZOBRIST_BASE;

    // Mix 32B mint into 4 lanes
    z ^= smix64(load_u64_le(&b[0..8])   ^ ZOBRIST_K0);
    z ^= smix64(load_u64_le(&b[8..16])  ^ ZOBRIST_K1);
    z ^= smix64(load_u64_le(&b[16..24]) ^ ZOBRIST_K0.rotate_left(17));
    z ^= smix64(load_u64_le(&b[24..32]) ^ ZOBRIST_K1.rotate_left(29));

    // Mix quantized fields
    z ^= smix64(price_q.wrapping_mul(0x9E37_79B9));
    z ^= smix64((s.volume_24h >> 10).rotate_left(13));
    z ^= smix64(liq_q.rotate_left(7));
    z ^= smix64((mom_q as u64).wrapping_mul(0xC2B2_AE3D));
    z ^= smix64((vol_q as u64).wrapping_mul(0x1656_67B1));
    z ^= smix64((spr_q as u64).rotate_left(11));
    z ^= smix64(s.block_height ^ ZOBRIST_K0);
    z ^= smix64(s.timestamp    ^ ZOBRIST_K1);

    smix64(z ^ ZOBRIST_BASE)
}

#[inline(always)]
fn action_id(a: &TradingAction) -> u16 {
    // Tag the variant and mix parameters via splitmix; fold to 16 bits
    let (tag, x, y): (u64, u64, u64) = match a {
        TradingAction::Buy { amount, slippage_bps }  => (0xB1, *amount, *slippage_bps as u64),
        TradingAction::Sell { amount, slippage_bps } => (0xB2, *amount, *slippage_bps as u64),
        TradingAction::Hold                           => (0xB3, 0, 0),
        TradingAction::AddLiquidity { amount }        => (0xB4, *amount, 0),
        TradingAction::RemoveLiquidity { amount }     => (0xB5, *amount, 0),
    };
    let mix = smix64(tag ^ x.rotate_left(17) ^ y.rotate_left(29));
    let h = mix ^ (mix >> 16) ^ (mix >> 32) ^ (mix >> 48);
    (h & 0xFFFF) as u16
}

#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum TradingAction {
    Buy { amount: u64, slippage_bps: u16 },
    Sell { amount: u64, slippage_bps: u16 },
    Hold,
    AddLiquidity { amount: u64 },
    RemoveLiquidity { amount: u64 },
}

#[derive(Clone, Debug)]
pub struct MCTSNode {
    state: MarketState,
    action: Option<TradingAction>,
    parent: Option<usize>,
    children: Vec<usize>,
    visits: u64,
    total_reward: f64,
    m2: f64, // second moment accumulator for variance
    untried_actions: Vec<TradingAction>,
    is_terminal: bool,
    zkey: u64,
}

pub struct MCTSConfig {
    pub exploration_constant: f64,
    pub simulation_depth: usize,
    pub max_iterations: usize,
    pub time_limit_ms: u64,
    pub min_visits_threshold: u64,
    pub discount_factor: f64,
    pub volatility_weight: f64,
    pub momentum_weight: f64,
    pub liquidity_weight: f64,
    pub fee_bps: u16,
    pub royalty_bps: u16,
    pub inclusion_cost_frac: Decimal,
    pub tip_cost_frac: Decimal,
    pub priority_cost_frac: Decimal,
    pub impact_k: Decimal,
    pub impact_alpha: f64,
    pub liquidity_baseline: u64,
    // PUCT / selection
    pub c_puct: f64,
    pub dirichlet_alpha: f64,
    pub dirichlet_epsilon: f64,
    pub virtual_loss: f64,
    // Progressive widening
    pub widen_k: f64,
    pub widen_alpha: f64,
    // Rollout policy controls
    pub rollout_epsilon_start: f64,
    pub rollout_epsilon_min: f64,
    pub rollout_epsilon_decay: f64,
    pub rollout_entropy_weight: f64,
    // Transposition table capacity
    pub tt_capacity: usize,
    // Risk and discount
    pub risk_lambda: f64,
    pub discount_min: f64,
    pub discount_hyper_k: f64,
    // Inclusion model weights and tip penalty
    pub inclusion_tip_weight: f64,
    pub inclusion_congestion_weight: f64,
    pub inclusion_leader_weight: f64,
    pub tip_penalty_scale: f64,
    // Slot safety
    pub slot_safety_ms: u64,
    // === [IMPROVED¹⁵: M0] ===
    pub actionset_cache_capacity: usize,
    // === [IMPROVED¹⁵: M2] ===
    pub tt_evidence_halflife_slots: u32,
    // === [IMPROVED¹⁵: M4] ===
    pub c_puct_taper: f64,
    // === [IMPROVED¹⁵: M5] ===
    pub warm_snapshot_topk: usize,
    // === [IMPROVED¹⁵: M3] ===
    pub iter_guard_ms: u64,
}

impl Default for MCTSConfig {
    fn default() -> Self {
        Self {
            exploration_constant: 1.414,
            simulation_depth: 20,
            max_iterations: 10000,
            time_limit_ms: 50,
            min_visits_threshold: 10,
            discount_factor: 0.99,
            volatility_weight: 0.3,
            momentum_weight: 0.4,
            liquidity_weight: 0.3,
            fee_bps: 30,
            royalty_bps: 0,
            inclusion_cost_frac: dec!(0),
            tip_cost_frac: dec!(0),
            priority_cost_frac: dec!(0),
            impact_k: dec!(1.0),
            impact_alpha: 0.7,
            liquidity_baseline: 10_000_000,
            c_puct: 1.25,
            dirichlet_alpha: 0.3,
            dirichlet_epsilon: 0.15,
            virtual_loss: 0.01,
            widen_k: 2.0,
            widen_alpha: 0.5,
            rollout_epsilon_start: 0.15,
            rollout_epsilon_min: 0.01,
            rollout_epsilon_decay: 1e-4,
            rollout_entropy_weight: 0.05,
            tt_capacity: 100_000,
            risk_lambda: 0.0,
            discount_min: 0.2,
            discount_hyper_k: 0.0,
            inclusion_tip_weight: 0.5,
            inclusion_congestion_weight: 0.4,
            inclusion_leader_weight: 0.6,
            tip_penalty_scale: 1.0,
            slot_safety_ms: 120,
            actionset_cache_capacity: 16_384,
            tt_evidence_halflife_slots: 128,
            c_puct_taper: 0.3,
            warm_snapshot_topk: 8,
            iter_guard_ms: 1,
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub struct TimeProfile {
    pub slot_ms: u64,
}

impl Default for TimeProfile {
    fn default() -> Self {
        Self { slot_ms: 400 }
    }
}

#[derive(Clone, Copy, Debug)]
pub struct RuntimeContext {
    // Current tip you are willing to pay (lamports)
    pub tip_lamports: u64,
    // Network congestion in [0,1]
    pub congestion: f64,
    // Leader proximity in [0,1], where 1.0 means it's your leader soon/now
    pub leader_proximity: f64,
    // Remaining time until end-of-slot or auction deadline, in ms
    pub slot_remaining_ms: Option<u64>,
}

impl Default for RuntimeContext {
    fn default() -> Self {
        Self {
            tip_lamports: 0,
            congestion: 0.0,
            leader_proximity: 0.0,
            slot_remaining_ms: None,
        }
    }
}

pub struct MonteCarloTreeSearcher {
    nodes: Vec<MCTSNode>,
    config: MCTSConfig,
    rng: StdRng,
    tt: EvalCache,
    time: TimeProfile,
    rollout_counter: u64,
    runtime: RuntimeContext,
    // === [IMPROVED⁹: PIECE G3] optional deterministic root focus ===
    root_focus: Option<Focus>,
    // === [IMPROVED¹⁵: M0] ===
    actionset_cache: ActionSetCache,
    // === [IMPROVED¹⁵: M1] ===
    priors_lru: PriorsLRU,
    // === [IMPROVED¹⁵: M3] iteration stats ===
    it_n: u64,
    it_mean_ms: f64,
    it_m2_ms: f64,
    // === [IMPROVED¹⁵: M4] loop rails ===
    start_time_loop: Instant,
    budget_ms_loop: u64,
    // === [IMPROVED¹⁵: M6] scratch buffers ===
    scratch: Scratch,
}

impl MonteCarloTreeSearcher {
    pub fn new(config: MCTSConfig) -> Self {
        Self {
            nodes: Vec::with_capacity(100000),
            config,
            rng: StdRng::from_entropy(),
            tt: EvalCache::new(config.tt_capacity),
            time: TimeProfile::default(),
            rollout_counter: 0,
            runtime: RuntimeContext::default(),
            root_focus: None,
            actionset_cache: ActionSetCache::new(config.actionset_cache_capacity),
            priors_lru: PriorsLRU::new(16_384),
            it_n: 0, it_mean_ms: 0.0, it_m2_ms: 0.0,
            start_time_loop: Instant::now(),
            budget_ms_loop: 0,
            scratch: Scratch::new(),
        }
    }

    pub fn new_with_time(config: MCTSConfig, time: TimeProfile) -> Self {
        Self {
            nodes: Vec::with_capacity(100000),
            config,
            rng: StdRng::from_entropy(),
            tt: EvalCache::new(config.tt_capacity),
            time,
            rollout_counter: 0,
            runtime: RuntimeContext::default(),
            root_focus: None,
            actionset_cache: ActionSetCache::new(config.actionset_cache_capacity),
            priors_lru: PriorsLRU::new(16_384),
            it_n: 0, it_mean_ms: 0.0, it_m2_ms: 0.0,
            start_time_loop: Instant::now(),
            budget_ms_loop: 0,
        }
    }

    // ...

    fn expand(&mut self, node_idx: usize) -> usize {
        let untried = &self.nodes[node_idx].untried_actions;
        let action = if untried.len() == 1 { untried[0].clone() } else {
            let priors_all = self.priors_untried_cached(node_idx);
            let idx = self.weighted_sample_index(&priors_all);
            untried[idx].clone()
        };
        if let Some(pos) = self.nodes[node_idx].untried_actions.iter().position(|a| a == &action) {
            self.nodes[node_idx].untried_actions.swap_remove(pos);
        }

        let new_state = self.apply_action(&self.nodes[node_idx].state, &action);
        let new_node = MCTSNode {
            state: new_state.clone(),
            action: Some(action),
            parent: Some(node_idx),
            children: Vec::new(),
            visits: 0,
            total_reward: 0.0,
            m2: 0.0,
            untried_actions: self.legal_actions_cached(&new_state).into_vec(),
            is_terminal: self.is_terminal_state(&new_state),
            zkey: zobrist_market_state_cfg(&new_state, &self.config),
        };

        self.nodes.push(new_node);
        self.nodes.len() - 1
    }

    #[instrument(name = "mcts.search", skip_all, fields(iterations = 0))]
    pub fn search(&mut self, initial_state: MarketState) -> TradingAction {
        self.nodes.clear();
        self.tt.clear();

        let zkey0 = zobrist_market_state_cfg(&initial_state, &self.config);
        let root_node = MCTSNode {
            state: initial_state.clone(),
            action: None,
            parent: None,
            children: Vec::new(),
            visits: 0,
            total_reward: 0.0,
            m2: 0.0,
            untried_actions: self.legal_actions_cached(&initial_state).into_vec(),
            is_terminal: false,
            zkey: zkey0,
        };
        self.nodes.push(root_node);

        let start_time = Instant::now();
        let mut iterations = 0;
        let budget_ms = self.compute_time_budget_ms();
        self.start_time_loop = start_time;
        self.budget_ms_loop = budget_ms;
        self.iter_stats_reset();

        while iterations < self.config.max_iterations {
            let left = Self::time_left_ms(start_time, budget_ms) as f64;
            if left <= (self.config.iter_guard_ms as f64) + self.iter_expected_budget() { break; }
            let t_iter = Instant::now();

            let _span_sel = span!(Level::TRACE, "selection").entered();
            let leaf_idx = self.tree_policy(0);
            drop(_span_sel);

            let _span_sim = span!(Level::TRACE, "simulation").entered();
            let t0 = Instant::now();
            let reward = self.simulate(leaf_idx);
            SIM_MS().observe(t0.elapsed().as_secs_f64() * 1000.0);
            ROLLOUTS().inc();
            drop(_span_sim);

            let _span_bp = span!(Level::TRACE, "backprop").entered();
            self.backpropagate(leaf_idx, reward);
            drop(_span_bp);

            let iter_ms = t_iter.elapsed().as_secs_f64() * 1000.0;
            self.iter_stats_obs(iter_ms);
            iterations = iterations.saturating_add(1);
            if !self.nodes[0].children.is_empty() {
                let (best_ev, best_visits) = self.nodes[0].children.iter().map(|&ci| {
                    let c = &self.nodes[ci];
                    let ev = if c.visits > 0 { c.total_reward / c.visits as f64 } else { 0.0 };
                    (ev, c.visits)
                }).max_by(|a,b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal)).unwrap_or((0.0,0));
                debug!(best_ev, best_visits, iter = iterations, "root child status");
            }
        }

        self.get_best_action(0)
    }

    #[instrument(name = "mcts.tree_policy", skip_all)]
    fn tree_policy(&mut self, node_idx: usize) -> usize {
        let mut current_idx = node_idx;

        while !self.nodes[current_idx].is_terminal {
            let allowed = Self::allow_children(self.nodes[current_idx].visits, self.config.widen_k, self.config.widen_alpha);
            if !self.nodes[current_idx].untried_actions.is_empty() && self.nodes[current_idx].children.len() < allowed {
                return self.expand(current_idx);
            } else if !self.nodes[current_idx].children.is_empty() {
                let priors = self.compute_priors_for_children(current_idx);
                let priors = if current_idx == 0 { ROOT_DIR_NOISE().inc(); self.apply_dirichlet_noise(priors) } else { priors };
                // === [IMPROVED⁹: PIECE G3] deterministic root focus, no bias in priors/scores ===
                let next = if current_idx == 0 {
                    self.pick_root_child_det(current_idx, &priors)
                } else {
                    self.best_child_puct(current_idx, &priors)
                };
                if self.config.virtual_loss > 0.0 {
                    if let Some(child) = self.nodes.get_mut(next) {
                        child.total_reward -= self.config.virtual_loss;
                        child.visits = child.visits.saturating_add(1);
                    }
                }
                current_idx = next;
            } else {
                break;
            }
        }

        current_idx
    }

    // === [IMPROVED⁹: PIECE G3] helper: deterministically choose the focused root child if active ===
    #[inline(always)]
    fn pick_root_child_det(&mut self, root_idx: usize, priors: &[f64]) -> usize {
        if let Some(mut f) = self.root_focus {
            if f.ticks_left > 0 && self.nodes[root_idx].children.contains(&f.child_idx) {
                f.ticks_left -= 1;
                self.root_focus = if f.ticks_left == 0 { None } else { Some(f) };
                return f.child_idx;
            }
        }
        // fallback to unbiased PUCT
        self.best_child_puct(root_idx, priors)
    }

    fn puct_score(q: f64, n_parent: f64, n_child: f64, prior: f64, c_puct: f64) -> f64 {
        let u = c_puct * prior * (n_parent.sqrt() / (1.0 + n_child));
        q + u
    }

    fn best_child_puct(&mut self, node_idx: usize, priors: &[f64]) -> usize {
        let node = &self.nodes[node_idx];
        let n_parent = node.visits.max(1) as f64;
        let c_eff = self.effective_c_puct_tapered(node.visits, self.start_time_loop, self.budget_ms_loop);
        self.scratch.puct_bonus.clear();
        self.scratch.puct_bonus.reserve(node.children.len());
        node.children
            .iter()
            .enumerate()
            .map(|(i, &child_idx)| {
                let child = &self.nodes[child_idx];
                let mean = child.total_reward / (child.visits.max(1) as f64);
                let std = if child.visits > 1 {
                    let var = child.m2 / (child.visits as f64 - 1.0);
                    var.max(0.0).sqrt()
                } else { 0.0 };
                let q = mean - self.config.risk_lambda * std;
                let prior = priors.get(i).copied().unwrap_or(1.0 / node.children.len().max(1) as f64);
                let bonus = Self::puct_score(0.0, n_parent, child.visits as f64, prior, c_eff); // only the U term
                self.scratch.puct_bonus.push(bonus);
                (q + bonus, child_idx)
            })
            .max_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal))
            .map(|(_, idx)| idx)
            .unwrap_or(node_idx)
    }

    fn compute_priors_for_children(&self, node_idx: usize) -> Vec<f64> {
        let node = &self.nodes[node_idx];
        let actions: Vec<&TradingAction> = node.children.iter().filter_map(|&cidx| self.nodes[cidx].action.as_ref()).collect();
        let priors = self.compute_priors(&node.state, &actions);
        priors
    }

    fn compute_priors<A: std::borrow::Borrow<TradingAction>>(&self, state: &MarketState, actions: &[A]) -> Vec<f64> {
        let momentum = state.momentum as f64 / 100.0; // [-1,1]
        let spread = (state.spread_bps as f64 / 10_000.0).min(0.05);
        let vol = (state.volatility as f64 / 100.0).min(2.0);

        let mut scores: Vec<f64> = Vec::with_capacity(actions.len());
        for a in actions {
            let a = a.borrow();
            let s = match a {
                TradingAction::Buy { .. } => {
                    let up = (momentum.max(0.0)) * (1.0 - spread);
                    (1.0 + up).max(1e-6)
                }
                TradingAction::Sell { .. } => {
                    let dn = ((-momentum).max(0.0)) * (1.0 - spread);
                    (1.0 + dn).max(1e-6)
                }
                TradingAction::Hold => {
                    (0.5 + (vol * spread * 2.0)).max(1e-6)
                }
                TradingAction::AddLiquidity { .. } => {
                    (0.5 + (spread * 0.5)).max(1e-6)
                }
                TradingAction::RemoveLiquidity { .. } => {
                    (0.5 + (vol * 0.5 + spread * 0.5)).max(1e-6)
                }
            };
            scores.push(s);
        }
        Self::normalize(&scores)
    }

    fn normalize(v: &[f64]) -> Vec<f64> {
        let mut sum = 0.0;
        let mut tmp: Vec<f64> = Vec::with_capacity(v.len());
        for &x in v {
            let y = if x.is_finite() && x > 0.0 { x } else { 0.0 };
            sum += y;
            tmp.push(y);
        }
        if sum <= 0.0 {
            return vec![1.0 / (v.len().max(1) as f64); v.len()];
        }
        let eps = 1e-12f64;
        let mut sum2 = 0.0;
        for t in &mut tmp { *t = *t + eps; sum2 += *t; }
        let inv = 1.0 / sum2;
        for t in &mut tmp { *t *= inv; debug_assert!(t.is_finite()); }
        tmp
    }

    fn weighted_sample_index(&mut self, weights: &[f64]) -> usize {
        let mut r = self.rng.gen::<f64>();
        for (i, w) in weights.iter().enumerate() {
            r -= *w;
            if r <= 0.0 { return i; }
        }
        weights.len().saturating_sub(1)
    }

    fn apply_dirichlet_noise(&mut self, priors: Vec<f64>) -> Vec<f64> {
        let eps = self.config.dirichlet_epsilon.clamp(0.0, 1.0);
        if priors.is_empty() || eps == 0.0 { return priors; }
        let alpha = self.config.dirichlet_alpha.max(1e-6);
        let noise = self.sample_dirichlet(priors.len(), alpha);
        let mixed: Vec<f64> = priors.iter().zip(noise.iter()).map(|(p, n)| (1.0 - eps) * *p + eps * *n).collect();
        Self::normalize(&mixed)
    }

    fn sample_dirichlet(&mut self, k: usize, alpha: f64) -> Vec<f64> {
        let mut draws = Vec::with_capacity(k);
        for _ in 0..k { draws.push(self.sample_gamma(alpha)); }
        let sum: f64 = draws.iter().copied().sum::<f64>().max(1e-12);
        draws.into_iter().map(|x| x / sum).collect()
    }

    fn sample_gamma(&mut self, alpha: f64) -> f64 {
        // Marsaglia and Tsang with Box-Muller normal to avoid extra deps
        // For alpha < 1, boost: sample gamma(alpha+1) and multiply by U^{1/alpha}
        let u: f64 = self.rng.gen();
        if alpha < 1.0 {
            let g = self.sample_gamma(alpha + 1.0);
            let u2: f64 = if u > 0.0 { u } else { 1e-12 };
            return g * u2.powf(1.0 / alpha);
        }
        let d = alpha - 1.0 / 3.0;
        let c = (1.0 / (9.0 * d)).sqrt();
        loop {
            // Box-Muller transform
            let u1: f64 = self.rng.gen::<f64>().clamp(1e-12, 1.0);
            let u2: f64 = self.rng.gen::<f64>().clamp(1e-12, 1.0);
            let z = (-2.0 * u1.ln()).sqrt() * (2.0 * std::f64::consts::PI * u2).cos();
            let v = (1.0 + c * z).powi(3);
            if v <= 0.0 { continue; }
            let u3: f64 = self.rng.gen();
            if u3.ln() < 0.5 * z * z + d * (1.0 - v + v.ln()) { return d * v; }
        }
    }

    fn allow_children(visits: u64, k: f64, alpha: f64) -> usize {
        (k * (visits as f64).powf(alpha)).max(1.0) as usize
    }

    fn best_child(&self, node_idx: usize, exploration: f64) -> usize {
        let node = &self.nodes[node_idx];
        if node.children.is_empty() {
            return node_idx;
        }
        let parent_visits = node.visits.max(1);
        let ln_parent_visits = (parent_visits as f64).ln();

        node.children
            .iter()
            .copied()
            .max_by(|&a, &b| {
                let ucb_a = self.calculate_ucb(&self.nodes[a], ln_parent_visits, exploration);
                let ucb_b = self.calculate_ucb(&self.nodes[b], ln_parent_visits, exploration);
                ucb_a
                    .partial_cmp(&ucb_b)
                    .unwrap_or(std::cmp::Ordering::Equal)
                    .then_with(|| a.cmp(&b))
            })
            .unwrap_or(node_idx)
    }

    fn calculate_ucb(&self, node: &MCTSNode, ln_parent_visits: f64, exploration: f64) -> f64 {
        let v = node.visits.max(1) as f64;
        let exploitation = node.total_reward / v;
        let exploration_term = exploration * (ln_parent_visits / v).sqrt();
        exploitation + exploration_term
    }

    #[instrument(name = "mcts.simulate", skip_all)]
    fn simulate(&mut self, node_idx: usize) -> f64 {
        let mut state = self.nodes[node_idx].state.clone();
        let mut total_reward = 0.0;
        let mut t = 0u64;
        let hyper_k = self.config.discount_hyper_k;
        let mut discount = 1.0f64;

        for _ in 0..self.config.simulation_depth {
            if self.is_terminal_state(&state) {
                break;
            }

            let actions = self.legal_actions_cached(&state);
            if actions.is_empty() {
                break;
            }

            let action = self.select_simulation_action(&state, &actions);
            let reward = self.evaluate_action(&state, &action);
            total_reward += discount * reward;
            t = t.saturating_add(1);
            if hyper_k > 0.0 {
                discount = 1.0 / (1.0 + hyper_k * t as f64);
            } else {
                discount *= self.config.discount_factor;
                if discount < self.config.discount_min { discount = self.config.discount_min; }
            }

            state = self.apply_action(&state, &action);
        }

        total_reward
    }

    #[instrument(name = "mcts.backprop", skip_all)]
    fn backpropagate(&mut self, mut node_idx: usize, reward: f64) {
        while let Some(node) = self.nodes.get_mut(node_idx) {
            node.visits += 1;
            // Welford update
            let n = node.visits as f64;
            let delta = reward - (node.total_reward / (n - 1.0).max(1.0));
            node.total_reward += reward;
            let mean_new = node.total_reward / n;
            node.m2 += delta * (reward - mean_new);

            if let Some(parent_idx) = node.parent {
                node_idx = parent_idx;
            } else {
                break;
            }
        }
    }

    fn get_best_action(&self, node_idx: usize) -> TradingAction {
        let node = &self.nodes[node_idx];

        node.children.iter()
            .filter(|&&child_idx| self.nodes[child_idx].visits >= self.config.min_visits_threshold)
            .max_by_key(|&&child_idx| {
                let child = &self.nodes[child_idx];
                let win_rate = if child.visits > 0 {
                    (child.total_reward / child.visits as f64 * 10000.0) as i64
                } else {
                    0
                };
                (child.visits as i64, win_rate)
            })
            .and_then(|&child_idx| self.nodes[child_idx].action.clone())
            .unwrap_or(TradingAction::Hold)
    }

    fn get_legal_actions(&self, state: &MarketState) -> Vec<TradingAction> {
        let mut actions = vec![TradingAction::Hold];

        let base_amount = (state.liquidity / 100).max(1);
        let amounts = [
            (base_amount / 10).max(1),
            (base_amount / 4).max(1),
            (base_amount / 2).max(1),
            base_amount,
        ];
        let slippages = [10, 25, 50, 100];

        for &amount in &amounts {
            if amount < state.liquidity.saturating_div(2) && amount > 0 {
                for &slippage in &slippages {
                    actions.push(TradingAction::Buy { amount, slippage_bps: slippage });
                    actions.push(TradingAction::Sell { amount, slippage_bps: slippage });
                }
            }
        }

        if state.volatility > 50 && state.liquidity > 1_000_000 {
            actions.push(TradingAction::AddLiquidity { amount: base_amount / 5 });
        }
        if state.liquidity > 2_000_000 && state.volatility > 60 {
            actions.push(TradingAction::RemoveLiquidity { amount: base_amount / 6 });
        }

        actions
    }

    fn apply_action(&self, state: &MarketState, action: &TradingAction) -> MarketState {
        let mut new_state = state.clone();

        let vol_norm = (Decimal::from(state.volatility) / dec!(100)).min(dec!(2));
        let momentum_norm = Decimal::from(state.momentum) / dec!(100);
        let spread = (Decimal::from(state.spread_bps) / dec!(10000)).min(dec!(0.05));
        let liq_baseline = self.config.liquidity_baseline.max(1);
        let liq_norm = (Decimal::from(state.liquidity) / Decimal::from(liq_baseline)).min(dec!(2));

        let w_m = Decimal::from_f64_retain(self.config.momentum_weight).unwrap_or(dec!(0));
        let w_v = Decimal::from_f64_retain(self.config.volatility_weight).unwrap_or(dec!(0));
        let w_l = Decimal::from_f64_retain(self.config.liquidity_weight).unwrap_or(dec!(0));

        match action {
            TradingAction::Buy { amount, slippage_bps } => {
                let impact = {
                    if state.liquidity == 0 {
                        dec!(0.25)
                    } else {
                        let x = Decimal::from(*amount) / Decimal::from(state.liquidity);
                        let pow = x.to_f64().unwrap_or(0.0).powf(self.config.impact_alpha);
                        (self.config.impact_k * Decimal::from_f64_retain(pow).unwrap_or(Decimal::ZERO))
                            .min(dec!(0.25))
                    }
                };
                let slippage = Decimal::from(*slippage_bps) / dec!(10000);
                let fee_frac = Decimal::from(self.config.fee_bps as u32 + self.config.royalty_bps as u32) / dec!(10000);
                let cost = impact + slippage + fee_frac + self.config.inclusion_cost_frac + self.config.tip_cost_frac + self.config.priority_cost_frac;

                let pos_momentum = momentum_norm.max(Decimal::ZERO);
                let vol_term = (dec!(2) - vol_norm).max(Decimal::ZERO);
                let base = pos_momentum * w_m + vol_term * w_v + liq_norm * w_l;
                let reward_dec = base - cost - spread;

                if state.momentum > 50 && state.volatility < 30 {
                    reward_dec *= dec!(1.5);
                }

                let price_factor = Decimal::ONE + impact * (Decimal::ONE + slippage + fee_frac);
                let new_price = (Decimal::from(state.price) * price_factor).max(dec!(1));
                new_state.price = new_price.trunc().to_u64().unwrap_or(state.price);
                new_state.volume_24h = state.volume_24h.saturating_add(*amount);
                let mom_delta = (impact * dec!(100)).trunc().to_i32().unwrap_or(0);
                new_state.momentum = (state.momentum + mom_delta).clamp(-100, 100);
                let new_vol = (Decimal::from(state.volatility) * dec!(1.05)).trunc().to_u32().unwrap_or(state.volatility);
                new_state.volatility = new_vol.min(200);
            }
            TradingAction::Sell { amount, slippage_bps } => {
                let impact = {
                    if state.liquidity == 0 {
                        dec!(0.25)
                    } else {
                        let x = Decimal::from(*amount) / Decimal::from(state.liquidity);
                        let pow = x.to_f64().unwrap_or(0.0).powf(self.config.impact_alpha);
                        (self.config.impact_k * Decimal::from_f64_retain(pow).unwrap_or(Decimal::ZERO))
                            .min(dec!(0.25))
                    }
                };
                let slippage = Decimal::from(*slippage_bps) / dec!(10000);
                let fee_frac = Decimal::from(self.config.fee_bps as u32 + self.config.royalty_bps as u32) / dec!(10000);
                let cost = impact + slippage + fee_frac + self.config.inclusion_cost_frac + self.config.tip_cost_frac + self.config.priority_cost_frac;

                let neg_momentum = (-momentum_norm).max(Decimal::ZERO);
                let vol_term = vol_norm;
                let base = neg_momentum * w_m + vol_term * (w_v / dec!(2)) + liq_norm * w_l;
                let reward_dec = base - cost - spread;

                if state.momentum < -50 && state.volatility > 70 {
                    reward_dec *= dec!(1.8);
                }
                if state.spread_bps > 100 {
                    reward_dec *= dec!(1.2);
                }

                let price_factor = Decimal::ONE - impact * (Decimal::ONE + slippage + fee_frac);
                let price_factor = price_factor.max(dec!(0.5));
                let new_price = (Decimal::from(state.price) * price_factor).max(dec!(1));
                new_state.price = new_price.trunc().to_u64().unwrap_or(state.price);
                new_state.volume_24h = state.volume_24h.saturating_add(*amount);
                let mom_delta = (impact * dec!(100)).trunc().to_i32().unwrap_or(0);
                new_state.momentum = (state.momentum - mom_delta).clamp(-100, 100);
                let new_vol = (Decimal::from(state.volatility) * dec!(1.05)).trunc().to_u32().unwrap_or(state.volatility);
                new_state.volatility = new_vol.min(200);
            }
            TradingAction::AddLiquidity { amount } => {
                new_state.liquidity = state.liquidity.saturating_add(*amount);
                let new_vol = (Decimal::from(state.volatility) * dec!(0.95)).trunc().to_u32().unwrap_or(state.volatility);
                new_state.volatility = new_vol;
                let new_spread = (Decimal::from(state.spread_bps) * dec!(0.9)).trunc().to_u16().unwrap_or(state.spread_bps);
                new_state.spread_bps = new_spread;
            }
            TradingAction::RemoveLiquidity { amount } => {
                new_state.liquidity = state.liquidity.saturating_sub(*amount);
                let new_vol = (Decimal::from(state.volatility) * dec!(1.1)).trunc().to_u32().unwrap_or(state.volatility);
                new_state.volatility = new_vol.min(200);
                let new_spread = (Decimal::from(state.spread_bps) * dec!(1.1)).trunc().to_u16().unwrap_or(state.spread_bps);
                new_state.spread_bps = new_spread;
            }
            TradingAction::Hold => {}
        }

        new_state.block_height = state.block_height + 1;
        new_state.timestamp = state.timestamp.saturating_add(self.time.slot_ms);

        new_state
    }

    #[instrument(name = "mcts.eval", skip_all)]
    fn evaluate_action(&mut self, state: &MarketState, action: &TradingAction) -> f64 {
        let key = zobrist_market_state(state);
        let aid = action_id(action);
        let cached = self.tt.get((key, aid));
        LOOKUPS().inc();
        if let Some(val) = cached {
            HITS().inc();
            CACHE_RATIO().set(HITS().get() as f64 / LOOKUPS().get() as f64);
            return val;
        }

        // Normalized, bounded features
        let vol_norm = (Decimal::from(state.volatility) / dec!(100)).min(dec!(2)); // [0,2]
        let momentum_norm = (Decimal::from(state.momentum) / dec!(100)).max(dec!(-1)).min(dec!(1)); // [-1,1]
        let spread = (Decimal::from(state.spread_bps) / dec!(10000)).min(dec!(0.05)); // [0,0.05]
        let liq = Decimal::from(state.liquidity.max(1u64));
        let liq_scaled = (liq / Decimal::from(10_000_000u64)).min(Decimal::ONE);

        // Decimal weights
        let w_m = Decimal::from_f64_retain(self.config.momentum_weight).unwrap_or(dec!(0));
        let w_v = Decimal::from_f64_retain(self.config.volatility_weight).unwrap_or(dec!(0));
        let w_l = Decimal::from_f64_retain(self.config.liquidity_weight).unwrap_or(dec!(0));

        let mut reward_dec = Decimal::ZERO;

        match action {
            TradingAction::Buy { amount, slippage_bps } => {
                // impact = trade_impact(amount, liquidity, k=1.6, alpha=0.65)
                let impact = self.trade_impact_dec(*amount, state.liquidity, 1.6, 0.65);
                let slippage = Decimal::from(*slippage_bps) / dec!(10000);
                let costs = slippage + spread + impact
                    + self.config.inclusion_cost_frac + self.config.tip_cost_frac + self.config.priority_cost_frac
                    + Decimal::from(self.config.fee_bps as u32 + self.config.royalty_bps as u32) / dec!(10000);

                let momentum_score = momentum_norm.max(Decimal::ZERO) * w_m;
                let volatility_score = (Decimal::ONE - (vol_norm.min(dec!(1.5)) / dec!(1.5))) * w_v;
                let liquidity_score = liq_scaled * w_l;
                reward_dec = momentum_score + volatility_score + liquidity_score - costs;
            }
            TradingAction::Sell { amount, slippage_bps } => {
                // impact = trade_impact(amount, liquidity, k=1.4, alpha=0.65)
                let impact = self.trade_impact_dec(*amount, state.liquidity, 1.4, 0.65);
                let slippage = Decimal::from(*slippage_bps) / dec!(10000);
                let costs = slippage + spread + (impact * dec!(0.9))
                    + self.config.inclusion_cost_frac + self.config.tip_cost_frac + self.config.priority_cost_frac
                    + Decimal::from(self.config.fee_bps as u32 + self.config.royalty_bps as u32) / dec!(10000);

                let momentum_score = (-momentum_norm).max(Decimal::ZERO) * w_m;
                let volatility_score = (vol_norm.min(dec!(1.5)) / dec!(1.5)) * w_v * dec!(0.7);
                let liquidity_score = liq_scaled * w_l * dec!(0.9);
                reward_dec = momentum_score + volatility_score + liquidity_score - costs;
            }
            TradingAction::AddLiquidity { amount } => {
                let liq_ratio = if state.liquidity == 0 { Decimal::ONE } else { Decimal::from(*amount) / liq };
                let fee_apr = (Decimal::from(state.volume_24h) * dec!(0.003)) / liq;
                let vol_bonus = if state.volatility > 50 { dec!(0.2) } else { Decimal::ZERO };
                let il_penalty = (vol_norm * vol_norm) * dec!(0.1);
                reward_dec = fee_apr * liq_ratio + vol_bonus - il_penalty;
            }
            TradingAction::RemoveLiquidity { amount } => {
                let liq_ratio = if state.liquidity == 0 { Decimal::ONE } else { Decimal::from(*amount) / liq };
                let il_risk = vol_norm * liq_ratio;
                let mom_risk = if momentum_norm.abs() > dec!(0.8) { dec!(0.5) } else { Decimal::ZERO };
                reward_dec = -(il_risk + mom_risk);
            }
            TradingAction::Hold => {
                reward_dec = dec!(0.02);
                if state.volatility > 80 || state.spread_bps > 200 { reward_dec += dec!(0.12); }
            }
        }

        let spread_adjustment = (Decimal::from(state.spread_bps) / dec!(10000)).min(dec!(0.02));
        reward_dec *= Decimal::ONE - spread_adjustment;

        // Inclusion-aware adjustment and tip sensitivity guard
        let p_inc = self.p_inclusion();
        reward_dec *= Decimal::from_f64_retain(p_inc).unwrap_or(Decimal::ONE);
        let tip_pen = self.tip_penalty();
        reward_dec -= Decimal::from_f64_retain(tip_pen).unwrap_or(Decimal::ZERO);

        let reward_f64 = reward_dec.to_f64().unwrap_or(0.0);
        self.tt.update((key, aid), reward_f64);
        ACTION_SEL().with_label_values(&[match action {
            TradingAction::Buy { .. } => "buy",
            TradingAction::Sell { .. } => "sell",
            TradingAction::Hold => "hold",
            TradingAction::AddLiquidity { .. } => "add_liq",
            TradingAction::RemoveLiquidity { .. } => "rem_liq",
        }]).inc();
        reward_f64
    }

    // Concave impact curve helper with explicit k, alpha
    fn trade_impact_dec(&self, amount: u64, liquidity: u64, k: f64, alpha: f64) -> Decimal {
        if liquidity == 0 { return dec!(1); }
        let x = Decimal::from(amount) / Decimal::from(liquidity);
        let pow = x.to_f64().unwrap_or(0.0).powf(alpha);
        (Decimal::from_f64_retain(k).unwrap_or(dec!(1)) * Decimal::from_f64_retain(pow).unwrap_or(Decimal::ZERO)).min(dec!(0.25))
    }

    fn select_simulation_action(&mut self, state: &MarketState, actions: &[TradingAction]) -> TradingAction {
        // Epsilon-greedy with decaying epsilon and small entropy-aware bonus
        let eps0 = self.config.rollout_epsilon_start;
        let eps_min = self.config.rollout_epsilon_min;
        let decay = self.config.rollout_epsilon_decay;
        let t = self.rollout_counter as f64;
        let epsilon = (eps0 * (-decay * t).exp()).max(eps_min);
        self.rollout_counter = self.rollout_counter.saturating_add(1);

        if self.rng.gen::<f64>() < epsilon {
            return actions[self.rng.gen_range(0..actions.len())].clone();
        }

        let priors = self.compute_priors(state, actions);
        let entropy_weight = self.config.rollout_entropy_weight;
        let mut best_action = TradingAction::Hold;
        let mut best_score = f64::NEG_INFINITY;

        for (i, action) in actions.iter().enumerate() {
            let eval = self.evaluate_action(state, action);
            let diversity_bonus = entropy_weight * (1.0 - priors[i]);
            let score = eval + diversity_bonus;
            if score > best_score {
                best_score = score;
                best_action = action.clone();
            }
        }
        best_action
    }

    fn is_terminal_state(&self, state: &MarketState) -> bool {
        state.liquidity < 1000 || 
        state.price == 0 || 
        state.price > 1_000_000_000_000 ||
        state.volatility > 200 ||
        state.momentum.abs() >= 100
    }

    pub fn get_action_confidence(&self, node_idx: usize) -> HashMap<TradingAction, f64> {
        let node = &self.nodes[node_idx];
        let mut confidences = HashMap::new();
        
        for &child_idx in &node.children {
            let child = &self.nodes[child_idx];
            if let Some(action) = &child.action {
                let confidence = if child.visits > 0 {
                    (child.total_reward / child.visits as f64 + 1.0) / 2.0
                } else {
                    0.5
                };
                confidences.insert(action.clone(), confidence);
            }
        }
        
        confidences
    }

    pub fn get_expected_value(&self, node_idx: usize) -> f64 {
        let node = &self.nodes[node_idx];
        if node.visits > 0 {
            node.total_reward / node.visits as f64
        } else {
            0.0
        }
    }
}

fn action_hash(action: &TradingAction) -> usize {
    match action {
        TradingAction::Buy { amount, slippage_bps } => {
            ((*amount as usize) ^ (*slippage_bps as usize)) % 32
        },
        TradingAction::Sell { amount, slippage_bps } => {
            (((*amount as usize) ^ (*slippage_bps as usize)) + 8) % 32
        },
        TradingAction::Hold => 16,
        TradingAction::AddLiquidity { amount } => {
            ((*amount as usize) + 20) % 32
        },
        TradingAction::RemoveLiquidity { amount } => {
            ((*amount as usize) + 24) % 32
        },
    }
}

#[derive(Clone)]
pub struct ParallelMCTS {
    searchers: Vec<Arc<Mutex<MonteCarloTreeSearcher>>>,
    thread_count: usize,
}

impl ParallelMCTS {
    pub fn new(config: MCTSConfig, thread_count: usize) -> Self {
        let searchers = (0..thread_count)
            .map(|_| Arc::new(Mutex::new(MonteCarloTreeSearcher::new(config.clone()))))
            .collect();
        
        Self {
            searchers,
            thread_count,
        }
    }

    pub async fn search_parallel(&self, initial_state: MarketState) -> TradingAction {
        use tokio::task;

        let mut handles = Vec::with_capacity(self.thread_count);

        for searcher in &self.searchers {
            let s = searcher.clone();
            let st = initial_state.clone();
            handles.push(task::spawn_blocking(move || {
                let mut guard = s.lock().unwrap();
                // Run the search
                let _ = guard.search(st);
                // Extract root child stats to aggregate
                let root = 0usize;
                guard.nodes[root]
                    .children
                    .iter()
                    .map(|&ci| {
                        let child = &guard.nodes[ci];
                        (
                            child.action.clone().unwrap_or(TradingAction::Hold),
                            child.visits,
                            child.total_reward,
                        )
                    })
                    .collect::<Vec<(TradingAction, u64, f64)>>()
            }));
        }

        let mut agg: HashMap<TradingAction, (u64, f64)> = HashMap::new();
        for h in handles {
            if let Ok(stats) = h.await {
                for (a, v, r) in stats {
                    let e = agg.entry(a).or_insert((0u64, 0.0f64));
                    e.0 = e.0.saturating_add(v);
                    e.1 += r;
                }
            }
        }

        let min_cov = self.searchers.len() as u64; // require coverage by at least one child per worker
        agg.into_iter()
            .filter(|(_, (v, _))| *v >= min_cov)
            .max_by(|a, b| {
                let (v1, r1) = a.1;
                let (v2, r2) = b.1;
                let m1 = if v1 > 0 { r1 / v1 as f64 } else { f64::NEG_INFINITY };
                let m2 = if v2 > 0 { r2 / v2 as f64 } else { f64::NEG_INFINITY };
                m1.partial_cmp(&m2).unwrap_or(std::cmp::Ordering::Equal)
                    .then(v1.cmp(&v2))
            })
            .map(|(a, _)| a)
            .unwrap_or(TradingAction::Hold)
    }
}

pub struct AdaptiveMCTS {
    base_searcher: MonteCarloTreeSearcher,
    performance_history: Vec<f64>,
    config_adjustments: MCTSConfig,
}

impl AdaptiveMCTS {
    pub fn new(initial_config: MCTSConfig) -> Self {
        Self {
            base_searcher: MonteCarloTreeSearcher::new(initial_config.clone()),
            performance_history: Vec::with_capacity(1000),
            config_adjustments: initial_config,
        }
    }

    pub fn search_adaptive(&mut self, state: MarketState, last_reward: Option<f64>) -> TradingAction {
        if let Some(reward) = last_reward {
            self.performance_history.push(reward);
            self.adjust_parameters();
        }
        
        self.base_searcher.config = self.config_adjustments.clone();
        self.base_searcher.search(state)
    }

    fn adjust_parameters(&mut self) {
        if self.performance_history.len() < 10 {
            return;
        }
        
        let recent_performance = self.performance_history
            .iter()
            .rev()
            .take(10)
            .sum::<f64>() / 10.0;
        
        let overall_performance = self.performance_history
            .iter()
            .sum::<f64>() / self.performance_history.len() as f64;
        
        if recent_performance < overall_performance * 0.8 {
            self.config_adjustments.exploration_constant *= 1.1;
            self.config_adjustments.simulation_depth = 
                (self.config_adjustments.simulation_depth as f64 * 1.2) as usize;
        } else if recent_performance > overall_performance * 1.2 {
            self.config_adjustments.exploration_constant *= 0.95;
        }
        
        self.config_adjustments.exploration_constant = 
            self.config_adjustments.exploration_constant.clamp(0.5, 3.0);
        self.config_adjustments.simulation_depth = 
            self.config_adjustments.simulation_depth.clamp(10, 50);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_mcts_search() {
        let config = MCTSConfig::default();
        let mut searcher = MonteCarloTreeSearcher::new(config);
        
        let state = MarketState {
            token_mint: Pubkey::new_unique(),
            price: 1000000,
            volume_24h: 10000000,
            liquidity: 5000000,
            momentum: 20,
            volatility: 30,
            spread_bps: 50,
            block_height: 1000,
            timestamp: 1700000000,
        };
        
        let action = searcher.search(state);
        assert!(!matches!(action, TradingAction::Hold));
    }

    #[tokio::test]
    async fn test_parallel_search() {
        let config = MCTSConfig::default();
        let parallel_mcts = ParallelMCTS::new(config, 4);
        
        let state = MarketState {
            token_mint: Pubkey::new_unique(),
            price: 2000000,
            volume_24h: 20000000,
            liquidity: 10000000,
            momentum: -30,
            volatility: 60,
            spread_bps: 75,
            block_height: 2000,
            timestamp: 1700001000,
        };
        
        let action = parallel_mcts.search_parallel(state).await;
        assert!(matches!(
            action,
            TradingAction::Buy { .. } | TradingAction::Sell { .. } | TradingAction::Hold
        ));
    }
}
