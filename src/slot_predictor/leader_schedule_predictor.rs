use anyhow::{anyhow, Result};
use dashmap::DashMap;
use solana_client::{
    nonblocking::rpc_client::RpcClient,
    rpc_config::{RpcBlockConfig, RpcGetVoteAccountsConfig, RpcLeaderScheduleConfig},
};
use solana_sdk::{
    clock::{Epoch, Slot, DEFAULT_SLOTS_PER_EPOCH},
    commitment_config::CommitmentConfig,
    pubkey::Pubkey,
};
use std::{
    cmp::Ordering,
    collections::{BTreeMap, HashMap, HashSet, VecDeque},
    str::FromStr,
    sync::{
        atomic::{AtomicU64, Ordering as AtomicOrdering},
        Arc,
    },
    time::{Duration, Instant},
};
use futures::future;
use smallvec::SmallVec;
use std::ops::{Deref, DerefMut, Index};
use std::hash::{Hash, Hasher};
use parking_lot as pl;
use time::OffsetDateTime;

use tokio::{
    sync::{Mutex, RwLock},
    time::{interval, sleep},
};
use tracing::{debug, error, info, warn};
use metrics::{counter, gauge, histogram};
use serde::{Serialize, Deserialize};
use rand::{SeedableRng, rngs::SmallRng, Rng};
use std::{fs, path::Path};
use moka::sync::Cache;
use sled::Db;

// ========== AutoTuner for metrics-driven parameter adjustment ==========
pub struct AutoTuner {
    last_recalc: Instant,
    window: Duration,
    conf_sum: f64,
    conf_sum_sq: f64,
    conf_n: u64,
    pub ewma_alpha: f64,
    pub breaker_threshold: f64,
}

// ========== Relay success stats and weighting ==========
#[derive(Debug, Clone, Default)]
pub struct RelaySuccessStats { pub success_log: std::collections::HashMap<(LeaderId, RelayId), Vec<bool>> }
impl RelaySuccessStats {
    pub fn relay_weights(&self, leader: LeaderId) -> std::collections::HashMap<RelayId, f64> {
        let mut weights = std::collections::HashMap::new();
        for ((l, r), log) in &self.success_log {
            if *l == leader {
                let succ = log.iter().filter(|&&x| x).count() as f64;
                let rate = succ / (log.len().max(1) as f64);
                weights.insert(r.clone(), rate);
            }
        }
        weights
    }
    pub fn relay_failure_corr(&self, _slot: Slot) -> bool {
        let mut failed_relays = 0usize;
        for ((_l, _r), log) in &self.success_log { if matches!(log.last(), Some(false)) { failed_relays += 1; } }
        failed_relays >= 2
    }
}

pub fn adjusted_score(ev: f64, variance_estimate: f64) -> f64 { ev / (1.0 + variance_estimate.max(1e-6)) }

// ========== Slot latency model (95th percentile prediction) ==========
#[derive(Debug, Clone, Default)]
pub struct SlotLatencyModel { pub latency_logs: std::collections::HashMap<(LeaderId, RelayId), Vec<u64>> }
impl SlotLatencyModel {
    pub fn predict(&self, leader: LeaderId, relay: RelayId) -> u64 {
        if let Some(latencies) = self.latency_logs.get(&(leader, relay)) {
            if latencies.is_empty() { return 500; }
            let mut s = latencies.clone(); s.sort_unstable();
            let idx = ((0.95 * s.len() as f64).floor() as usize).min(s.len() - 1);
            s[idx]
        } else { 500 }
    }
}

// ========== K backup paths (simple greedy) ==========
pub fn find_backup_paths(
    leaders: Vec<LeaderId>,
    reliability: &std::collections::HashMap<(LeaderId, LeaderId), f64>,
    k: usize,
) -> Vec<Vec<LeaderId>> {
    // Build adjacency from reliability
    let mut adj: std::collections::HashMap<LeaderId, Vec<(LeaderId, f64)>> = std::collections::HashMap::new();
    for (&(a, b), &w) in reliability { adj.entry(a).or_default().push((b, w)); }
    for v in adj.values_mut() { v.sort_by(|x, y| y.1.partial_cmp(&x.1).unwrap_or(Ordering::Equal)); }
    let start = *leaders.get(0).unwrap_or(&leaders.first().cloned().unwrap_or(Pubkey::default()));
    let mut out = Vec::new();
    // Greedy: take top-k best 3-hop paths
    let mut frontier: Vec<(f64, Vec<LeaderId>)> = vec![(0.0, vec![start])];
    while !frontier.is_empty() && out.len() < k {
        let (score, path) = frontier.remove(0);
        let last = *path.last().unwrap();
        if let Some(neis) = adj.get(&last) {
            for &(nxt, w) in neis.iter().take(3) {
                if path.contains(&nxt) { continue; }
                let mut np = path.clone(); np.push(nxt);
                let ns = score + w;
                out.push(np.clone());
                if out.len() >= k { break; }
                frontier.push((ns, np));
            }
        }
    }
    out.truncate(k);
    out
}

// ========== Slot entropy score ==========
pub fn slot_entropy_score(blockhash_deltas: &[u64]) -> f64 {
    let mut freq: std::collections::HashMap<u64, u64> = std::collections::HashMap::new();
    for &d in blockhash_deltas { *freq.entry(d).or_insert(0) += 1; }
    let total = blockhash_deltas.len().max(1) as f64;
    freq.values().map(|&c| { let p = c as f64 / total; -p * p.log2() }).sum()
}

// ========== Sharded DB opener ==========
pub fn open_sharded_db(epoch: u64, slot: u64) -> Db {
    let path = format!("/db/epoch_{}/slot_{}", epoch, slot);
    sled::Config::new().path(path).open().expect("open sled db")
}

// ========== Slot-aware redundancy (temporal overlap probability) ==========
pub trait SlotOverlapPredictor: Send + Sync { fn overlap_prob(&self, slot_a: Slot, slot_b: Slot) -> f64; }

pub fn pick_staggered_backups(
    upcoming: &std::collections::BTreeMap<Slot, Pubkey>,
    slot: Slot,
    max_backups: usize,
    predictor: &dyn SlotOverlapPredictor,
) -> Vec<Pubkey> {
    use std::collections::HashSet;
    let mut cands: Vec<(f64, Pubkey)> = Vec::new();
    let start = slot.saturating_sub(4);
    let end = slot + 4;
    for (&s, pk) in upcoming.range(start..=end) {
        if s == slot { continue; }
        let p = predictor.overlap_prob(slot, s);
        if p > 0.1 { cands.push((p, *pk)); }
    }
    cands.sort_by(|a, b| b.0.partial_cmp(&a.0).unwrap_or(Ordering::Equal));
    let mut seen = HashSet::new();
    let mut res = Vec::new();
    for (_, pk) in cands { if seen.insert(pk) { res.push(pk); if res.len() >= max_backups { break; } } }
    res
}

// ========== Confidence-indexed tip pricing curve ==========
#[derive(Debug, Clone)]
pub struct TipCurve { pub base_tip_lamports: u64, pub k: f64, pub min_mult: f64, pub max_mult: f64 }
impl Default for TipCurve { fn default() -> Self { Self { base_tip_lamports: 10_000, k: 1.2, min_mult: 1.0, max_mult: 3.0 } } }
impl TipCurve { pub fn tip_for(&self, confidence: f64) -> u64 { let mult = (1.0 + (1.0 - confidence).powf(self.k)).clamp(self.min_mult, self.max_mult); (self.base_tip_lamports as f64 * mult).round() as u64 } pub fn tune(&mut self, recent_inclusion: f64, target: f64) { if recent_inclusion < target { self.k = (self.k * 1.05).min(3.0); } else { self.k = (self.k * 0.97).max(0.8); } } }

// ========== Ghost relay detection and quarantine ==========
#[derive(Debug, Clone)]
pub struct RelayHealth { pub ewma_ghost: f64, pub fail_streak: u32, pub quarantine_until: Option<Instant> }

pub struct GhostDetector { alpha: f64, ghost_threshold: f64, max_fail_streak: u32, quarantine_slots: u64, slot_to_instant: Arc<dyn Fn(u64) -> Instant + Send + Sync>, map: std::collections::HashMap<String, RelayHealth> }
impl GhostDetector { pub fn new<F>(alpha: f64, ghost_threshold: f64, max_fail_streak: u32, quarantine_slots: u64, f: F) -> Self where F: Fn(u64) -> Instant + Send + Sync + 'static { Self { alpha, ghost_threshold, max_fail_streak, quarantine_slots, slot_to_instant: Arc::new(f), map: std::collections::HashMap::new() } }
    pub fn observe(&mut self, relay: &str, ghosted: bool, current_slot: u64) { let e = self.map.entry(relay.to_string()).or_insert(RelayHealth { ewma_ghost: 0.1, fail_streak: 0, quarantine_until: None }); let a = self.alpha; let x = if ghosted { 1.0 } else { 0.0 }; e.ewma_ghost = a * x + (1.0 - a) * e.ewma_ghost; if ghosted { e.fail_streak = e.fail_streak.saturating_add(1); } else { e.fail_streak = 0; } if e.ewma_ghost > self.ghost_threshold || e.fail_streak >= self.max_fail_streak { e.quarantine_until = Some((self.slot_to_instant)(current_slot + self.quarantine_slots)); } }
    pub fn allowed(&self, relay: &str) -> bool { if let Some(h) = self.map.get(relay) { if let Some(until) = h.quarantine_until { return Instant::now() >= until; } } true }
}

// ========== Tiny MLP for nonlinear EV fusion (4 -> 8 -> 1) ==========
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MlpParams { pub w1: [[f32; 4]; 8], pub b1: [f32; 8], pub w2: [f32; 8], pub b2: f32 }
impl Default for MlpParams { fn default() -> Self { let mut rng = rand::rngs::SmallRng::from_entropy(); let mut w1 = [[0.0; 4]; 8]; for i in 0..8 { for j in 0..4 { w1[i][j] = rng.gen_range(-0.1..0.1); } } let b1 = [0.0; 8]; let mut w2 = [0.0; 8]; for i in 0..8 { w2[i] = rng.gen_range(-0.1..0.1); } let b2 = 0.0; Self { w1, b1, w2, b2 } } }

pub struct TinyMlp { p: MlpParams, lr: f32, l2: f32, path: String }
impl TinyMlp {
    pub fn load_or_new(path: impl Into<String>, lr: f32, l2: f32) -> Self { let path = path.into(); let p = std::fs::read(&path).ok().and_then(|b| bincode::deserialize::<MlpParams>(&b).ok()).unwrap_or_default(); Self { p, lr, l2, path } }
    pub fn infer(&self, x: [f32; 4]) -> f32 { let x1 = x; let mut h = [0.0f32; 8]; for i in 0..8 { let mut s = self.p.b1[i]; for j in 0..4 { s += self.p.w1[i][j] * x1[j]; } h[i] = s.max(0.0); } let mut y = self.p.b2; for i in 0..8 { y += self.p.w2[i] * h[i]; } 1.0 / (1.0 + (-y).exp()) }
    pub fn train(&mut self, x: [f32; 4], target: f32) { let x1 = x; let mut h = [0.0f32; 8]; let mut z1 = [0.0f32; 8]; for i in 0..8 { let mut s = self.p.b1[i]; for j in 0..4 { s += self.p.w1[i][j] * x1[j]; } z1[i] = s; h[i] = s.max(0.0); } let mut y_lin = self.p.b2; for i in 0..8 { y_lin += self.p.w2[i] * h[i]; } let y = 1.0 / (1.0 + (-y_lin).exp()); let dy = y - target; let d_sig = y * (1.0 - y); let d_out = dy * d_sig; for i in 0..8 { self.p.w2[i] -= self.lr * (d_out * h[i] + self.l2 * self.p.w2[i]); } self.p.b2 -= self.lr * d_out; let mut dh = [0.0f32; 8]; for i in 0..8 { dh[i] = d_out * self.p.w2[i] * if z1[i] > 0.0 { 1.0 } else { 0.0 }; } for i in 0..8 { for j in 0..4 { self.p.w1[i][j] -= self.lr * (dh[i] * x1[j] + self.l2 * self.p.w1[i][j]); } self.p.b1[i] -= self.lr * dh[i]; } if let Ok(bytes) = bincode::serialize(&self.p) { let _ = std::fs::write(&self.path, bytes); } }
}

// ========== Multi-epoch leader forecasting (entropy) ==========
#[derive(Debug, Default, Clone)]
pub struct LeaderEntropy { pub deviation_rate: f64, pub epoch_stability: f64 }

pub struct EntropyForecaster { alpha: f64, history: std::collections::HashMap<Pubkey, LeaderEntropy> }
impl EntropyForecaster { pub fn new(alpha: f64) -> Self { Self { alpha, history: std::collections::HashMap::new() } }
    pub fn observe_epoch(&mut self, per_validator: impl IntoIterator<Item=(Pubkey, u64, u64)>) { for (pk, sched, produced) in per_validator { let dev = if sched == 0 { 0.0 } else { (sched.saturating_sub(produced) as f64 / sched as f64).clamp(0.0, 1.0) }; let st = 1.0 - dev; let e = self.history.entry(pk).or_default(); e.deviation_rate = self.alpha * dev + (1.0 - self.alpha) * e.deviation_rate; e.epoch_stability = self.alpha * st + (1.0 - self.alpha) * e.epoch_stability; } }
    pub fn stability_weight(&self, pk: &Pubkey) -> f64 { self.history.get(pk).map(|e| e.epoch_stability.clamp(0.0, 1.0)).unwrap_or(0.8) }
}

// ========== Relay filter and bundling planner (MEV-optimized with redundancy) ==========
pub trait RelayFilter: Send + Sync { fn relays_ok_for(&self, leader: &Pubkey) -> Vec<String>; }

#[derive(Debug, Clone)]
pub struct BundlePlan { pub slot: Slot, pub primary: Pubkey, pub backups: Vec<Pubkey>, pub relays: Vec<String>, pub ev_score: f64, pub relay_send_before_ms: Vec<(String, f64)>, pub suggested_tip_lamports: u64 }

pub struct BundlingPlanner<R: RelayFilter> { relay: R, max_backups: usize, redundancy_window: u64 }
impl<R: RelayFilter> BundlingPlanner<R> {
    pub fn new(relay: R, max_backups: usize, redundancy_window: u64) -> Self { Self { relay, max_backups, redundancy_window } }
    pub fn build_plans(
        &self,
        upcoming: &[(Slot, Pubkey)],
        mut score_fn: impl FnMut(&Pubkey, Slot) -> f64,
        mut reliability_fn: impl FnMut(&Pubkey) -> f64,
    ) -> Vec<BundlePlan> {
        use std::collections::{BTreeMap, HashSet};
        let mut by_slot: BTreeMap<Slot, Pubkey> = BTreeMap::new();
        for (s, pk) in upcoming { by_slot.insert(*s, *pk); }
        let mut plans = Vec::new();
        for (&slot, &leader) in &by_slot {
            let base_ev = score_fn(&leader, slot);
            let base_rel = reliability_fn(&leader);
            let mut backups: Vec<Pubkey> = Vec::new();
            if base_rel < 0.7 {
                for delta in 1..=self.redundancy_window {
                    if let Some(&prev_leader) = by_slot.get(&slot.saturating_sub(delta)) { backups.push(prev_leader); }
                    if let Some(&next_leader) = by_slot.get(&(slot + delta)) { backups.push(next_leader); }
                    if backups.len() >= self.max_backups { break; }
                }

    // Build MEV-optimized bundle plans with redundancy and relay filtering
    pub async fn build_bundle_plans(&self, horizon_slots: u64, relay_filter: Arc<dyn RelayFilter + Send + Sync>, max_backups: usize, redundancy_window: u64, time_to_expiry_ms: u64) -> Result<Vec<BundlePlan>> {
        let leaders = self.get_upcoming_leaders(horizon_slots).await?;
        let upcoming: Vec<(Slot, Pubkey)> = leaders.iter().map(|li| (li.slot, li.pubkey)).collect();
        let planner = BundlingPlanner::new(relay_filter.as_ref(), max_backups, redundancy_window);
        let mut score_fn = |pk: &Pubkey, slot: Slot| -> f64 {
            let profit_w = if let Some(w) = &self.mev_weighter { w.slot_profitability(pk, slot) } else { 1.0 };
            let conf = 0.8; // cheap default; for exact call leader_confidence_for_slot
            let winp = if let Some(r) = &self.relay_obs { slot_win_probability(&self.winprob_cfg, r.as_ref(), pk) } else { 0.8 };
            (profit_w * conf * winp).clamp(0.0, 1.5)
        };
        let mut reliability_fn = |pk: &Pubkey| -> f64 { self.trust_profiler.trust_score(pk) };
        let mut plans = planner.build_plans(&upcoming, &mut score_fn, &mut reliability_fn);
        if let Some(rc) = &self.relay_cutoff { for p in plans.iter_mut() { let mut cutoffs = Vec::new(); for rel in &p.relays { let ms = rc.send_before_ms(rel); cutoffs.push((rel.clone(), ms)); } p.relay_send_before_ms = cutoffs; } }
        Ok(plans)
    }

    // Online training hook for the MLP using observed reward (0..1)
    pub fn train_ev_model(&self, reliability: f64, profitability: f64, trust: f64, confidence: f64, reward: f64) {
        let mut mlp = self.mlp.lock();
        mlp.train([reliability as f32, profitability as f32, trust as f32, confidence as f32], reward.clamp(0.0, 1.0) as f32);
    }
    pub fn set_relay_cutoff_calibrator(&mut self, cutoff: Arc<RelayCutoffCalibrator>) { self.relay_cutoff = Some(cutoff); }
    pub fn entropy_observe_epoch(&self, per_validator: impl IntoIterator<Item=(Pubkey, u64, u64)>) { self.entropy.lock().observe_epoch(per_validator); }
                let mut seen = HashSet::new(); backups.retain(|pk| seen.insert(*pk));
            }
            let mut ev = base_ev;
            for (i, b) in backups.iter().enumerate() { let factor = 0.8f64.powi((i as i32) + 1); ev += factor * score_fn(b, slot); }
            let relays = self.relay.relays_ok_for(&leader);
            plans.push(BundlePlan { slot, primary: leader, backups, relays, ev_score: ev, relay_send_before_ms: Vec::new(), suggested_tip_lamports: 0 });
        }
        plans.sort_by(|a, b| b.ev_score.partial_cmp(&a.ev_score).unwrap_or(Ordering::Equal));
        plans
    }
}

// ========== Relay cutoff calibrator (per-relay adaptive cutoffs) ==========
#[derive(Debug, Clone)]
pub struct RelayStats { pub ewma_accept_ms: f64, pub ewma_fail: f64, pub p95_accept_ms: f64, pub samples: u64, pub last_update: Instant }
impl Default for RelayStats { fn default() -> Self { Self { ewma_accept_ms: 250.0, ewma_fail: 0.0, p95_accept_ms: 300.0, samples: 0, last_update: Instant::now() } } }

pub struct RelayCutoffCalibrator { alpha: f64, window: Duration, stats: pl::Mutex<std::collections::HashMap<String, RelayStats>> }
impl RelayCutoffCalibrator {
    pub fn new(alpha: f64, window: Duration) -> Self { Self { alpha, window, stats: pl::Mutex::new(std::collections::HashMap::new()) } }
    pub fn observe(&self, relay: &str, accept_margin_ms: Option<f64>) {
        let mut map = self.stats.lock();
        let s = map.entry(relay.to_string()).or_default();
        s.samples += 1; s.last_update = Instant::now(); let a = self.alpha;
        match accept_margin_ms {
            Some(ms) => { s.ewma_accept_ms = a * ms + (1.0 - a) * s.ewma_accept_ms; s.ewma_fail = (1.0 - a) * s.ewma_fail; s.p95_accept_ms = 0.9 * s.p95_accept_ms + 0.1 * ms.max(s.p95_accept_ms); }
            None => { s.ewma_fail = a * 1.0 + (1.0 - a) * s.ewma_fail; }
        }
    }
    pub fn send_before_ms(&self, relay: &str) -> f64 {
        let map = self.stats.lock();
        if let Some(s) = map.get(relay) {
            let base = s.ewma_accept_ms.max(s.p95_accept_ms);
            let safety = 50.0 * s.ewma_fail.min(1.0);
            (base + safety).clamp(120.0, 600.0)
        } else { 300.0 }
    }
    pub fn schedule_send_at(&self, relay: &str, slot_deadline_ms: u64) -> Instant {
        let ms_before = self.send_before_ms(relay) as u64;
        Instant::now() + Duration::from_millis(slot_deadline_ms.saturating_sub(ms_before))
    }
}

// ========== Slot Win Probability (latency + ghost rate) ==========
#[derive(Debug, Clone)]
pub struct WinProbCfg { pub max_block_latency_ms: f64, pub ghost_penalty: f64, pub latency_weight: f64 }
impl Default for WinProbCfg { fn default() -> Self { Self { max_block_latency_ms: 350.0, ghost_penalty: 0.4, latency_weight: 0.6 } } }

pub trait RelayObservations: Send + Sync { fn block_latency_ms_p95(&self, leader: &Pubkey) -> Option<f64>; fn ghost_rate(&self, leader: &Pubkey) -> Option<f64>; }

pub fn slot_win_probability<R: RelayObservations>(cfg: &WinProbCfg, relay: &R, leader: &Pubkey) -> f64 {
    let lat = relay.block_latency_ms_p95(leader).unwrap_or(cfg.max_block_latency_ms);
    let lat_ok = (1.0 - (lat / cfg.max_block_latency_ms)).clamp(0.0, 1.0);
    let ghost = relay.ghost_rate(leader).unwrap_or(0.1).clamp(0.0, 1.0);
    let ghost_ok = (1.0 - ghost).clamp(0.0, 1.0);
    (cfg.latency_weight * lat_ok) + ((1.0 - cfg.latency_weight) * (1.0 - cfg.ghost_penalty + cfg.ghost_penalty * ghost_ok))
}

// ========== Hierarchical cache (L1 in-memory + L2 persisted) ==========
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PersistLeaderInfo { pub pubkey: Pubkey, pub slot: Slot, pub epoch: u64, pub stake: u64, pub commission: u8, pub last_vote: Option<Slot> }

pub struct HierCache { l1: Cache<Slot, LeaderInfo>, db: Db }
impl HierCache {
    pub fn new(l1_capacity: u64, path: &str) -> anyhow::Result<Self> {
        Ok(Self { l1: Cache::builder().max_capacity(l1_capacity).build(), db: sled::open(path)? })
    }
    pub fn get(&self, slot: Slot) -> Option<LeaderInfo> {
        if let Some(li) = self.l1.get(&slot) { return Some(li); }
        let key = format!("slot:{}", slot);
        if let Some(val) = self.db.get(key.as_bytes()).ok().flatten() {
            if let Ok(p) = bincode::deserialize::<PersistLeaderInfo>(&val) {
                let li = LeaderInfo { pubkey: p.pubkey, slot: p.slot, epoch: p.epoch, stake: p.stake, commission: p.commission, last_vote: p.last_vote };
                self.l1.insert(slot, li.clone());
                return Some(li);
            }
        }
        None
    }
    pub fn put(&self, li: &LeaderInfo) {
        self.l1.insert(li.slot, li.clone());
        let key = format!("slot:{}", li.slot);
        let p = PersistLeaderInfo { pubkey: li.pubkey, slot: li.slot, epoch: li.epoch, stake: li.stake, commission: li.commission, last_vote: li.last_vote };
        if let Ok(bytes) = bincode::serialize(&p) { let _ = self.db.insert(key.as_bytes(), bytes); }
    }
}

// ========== Sharded semaphores by epoch tier ==========
pub struct EpochSemaphores { current: Arc<tokio::sync::Semaphore>, next: Arc<tokio::sync::Semaphore>, future: Arc<tokio::sync::Semaphore> }
impl EpochSemaphores { pub fn new(cur: usize, next: usize, fut: usize) -> Self { Self { current: Arc::new(tokio::sync::Semaphore::new(cur)), next: Arc::new(tokio::sync::Semaphore::new(next)), future: Arc::new(tokio::sync::Semaphore::new(fut)) } }
    pub async fn acquire_current(&self) -> tokio::sync::OwnedSemaphorePermit { self.current.clone().acquire_owned().await.expect("semaphore") }
    pub async fn acquire_next(&self) -> tokio::sync::OwnedSemaphorePermit { self.next.clone().acquire_owned().await.expect("semaphore") }
    pub async fn acquire_future(&self) -> tokio::sync::OwnedSemaphorePermit { self.future.clone().acquire_owned().await.expect("semaphore") }
}

// ========== Latency-aware slot prediction with hash staleness ==========
pub trait HashStaleness: Send + Sync { fn recent_blockhash_age_ms(&self) -> Option<u64>; }

// ========== Hash freshness + adaptive prefetch threshold ==========
pub trait HashFreshness: Send + Sync { fn blockhash_age_ms(&self) -> Option<u64>; }

pub struct AdaptivePrefetcher { base_threshold_slots: u64, nominal_slot_ms: u64, hash: Arc<dyn HashFreshness + Send + Sync> }
impl AdaptivePrefetcher {
    pub fn new(base_threshold_slots: u64, nominal_slot_ms: u64, hash: Arc<dyn HashFreshness + Send + Sync>) -> Self { Self { base_threshold_slots, nominal_slot_ms, hash } }
    pub fn threshold_slots(&self) -> u64 {
        let base = self.base_threshold_slots as f64;
        if let Some(age) = self.hash.blockhash_age_ms() {
            let slots = age as f64 / self.nominal_slot_ms as f64;
            let factor = if slots > 2.0 { (slots / 2.0).min(2.0) } else { 1.0 };
            (base * factor) as u64
        } else { self.base_threshold_slots }
    }
}

// ========== RL Contextual Bandit Optimizer ==========
#[derive(Debug, Clone, Serialize, Deserialize)]
    pub fn score(&self, x: &ContextFeatures) -> f64 {
        self.w.w_reliability * x.reliability + self.w.w_mev * x.mev + self.w.w_trust * x.trust + self.w.w_confidence * x.confidence
    }
    pub fn choose(&mut self, _leader: &Pubkey, x: &ContextFeatures) -> (f64, bool) {
        let explore = self.rng.gen::<f64>() < self.eps;
        let mut s = self.score(x);
        if explore { s += self.rng.gen_range(-0.05..0.05); }
        (s.clamp(0.0, 1.0), explore)
    }
    pub fn update(&mut self, x: &ContextFeatures, reward: f64) {
        let pred = self.score(x);
        let err = (reward - pred).clamp(-1.0, 1.0);
        self.w.w_reliability = (self.w.w_reliability + self.alpha_lr * err * x.reliability).clamp(0.0, 1.0);
        self.w.w_mev        = (self.w.w_mev        + self.alpha_lr * err * x.mev       ).clamp(0.0, 1.0);
        self.w.w_trust      = (self.w.w_trust      + self.alpha_lr * err * x.trust     ).clamp(0.0, 1.0);
        self.w.w_confidence = (self.w.w_confidence + self.alpha_lr * err * x.confidence).clamp(0.0, 1.0);
        let sum = self.w.w_reliability + self.w.w_mev + self.w.w_trust + self.w.w_confidence;
        if sum > 0.0 { self.w.w_reliability /= sum; self.w.w_mev /= sum; self.w.w_trust /= sum; self.w.w_confidence /= sum; }
        if let Ok(bytes) = bincode::serialize(&self.w) { let _ = fs::write(&self.path, bytes); }
    }
    pub fn weights(&self) -> BanditWeights { self.w.clone() }
}

impl AutoTuner {
    pub fn new(window: Duration, ewma_alpha: f64, breaker_threshold: f64) -> Self {
        Self { last_recalc: Instant::now(), window, conf_sum: 0.0, conf_sum_sq: 0.0, conf_n: 0, ewma_alpha, breaker_threshold }
    }
    pub fn observe_confidence(&mut self, c: f64) {
        self.conf_sum += c;
        self.conf_sum_sq += c * c;
        self.conf_n += 1;
        histogram!("confidence.values", c);
        if self.last_recalc.elapsed() >= self.window && self.conf_n >= 10 {
            let mean = self.conf_sum / self.conf_n as f64;
            let var = (self.conf_sum_sq / self.conf_n as f64) - mean * mean;
            gauge!("confidence.mean", mean);
            gauge!("confidence.variance", var.max(0.0));
            if var > 0.05 {
                self.ewma_alpha = (self.ewma_alpha * 0.9).clamp(0.05, 0.5);
                self.breaker_threshold = (self.breaker_threshold + 0.05).min(0.85);
            } else {
                self.ewma_alpha = (self.ewma_alpha * 1.05).min(0.5);
                self.breaker_threshold = (self.breaker_threshold - 0.02).max(0.5);
            }
            gauge!("autotune.ewma_alpha", self.ewma_alpha);
            gauge!("autotune.breaker_threshold", self.breaker_threshold);
            self.last_recalc = Instant::now();
            self.conf_sum = 0.0;
            self.conf_sum_sq = 0.0;
            self.conf_n = 0;
        }
    }
}

const SCHEDULE_CACHE_TTL: Duration = Duration::from_secs(300);
const MAX_CACHED_EPOCHS: usize = 3;
const PREFETCH_THRESHOLD_SLOTS: u64 = 432;
const RPC_RETRY_ATTEMPTS: u32 = 5;
const RPC_RETRY_DELAY_MS: u64 = 100;
const LEADER_SCHEDULE_REFRESH_INTERVAL: Duration = Duration::from_secs(60);
const VOTE_ACCOUNT_REFRESH_INTERVAL: Duration = Duration::from_secs(120);
const MIN_STAKE_LAMPORTS: u64 = 1_000_000_000;

#[derive(Debug, Clone)]
pub struct LeaderInfo {
    pub pubkey: Pubkey,
    pub slot: Slot,
    pub epoch: Epoch,
    pub stake: u64,
    pub commission: u8,
    pub last_vote: Option<Slot>,
}

// ========== Adaptive Circuit Breaker (EWMA + Half-Open) ==========
#[derive(Debug, Clone)]
pub struct BreakerConfig {
    pub ewma_alpha: f64,
    pub open_threshold: f64,
    pub min_observations: u32,
    pub open_duration: Duration,
    pub half_open_probe_interval: Duration,
}

impl Default for BreakerConfig {
    fn default() -> Self {
        Self { ewma_alpha: 0.2, open_threshold: 0.6, min_observations: 20, open_duration: Duration::from_secs(15), half_open_probe_interval: Duration::from_secs(2) }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BrState { Closed, Open(Instant), HalfOpen(Instant) }

struct BreakerState { st: BrState }

pub struct AdaptiveBreaker {
    cfg: BreakerConfig,
    ewma_fail: f64,
    total_obs: u32,
    state: pl::Mutex<BreakerState>,
}

impl AdaptiveBreaker {
    pub fn new(cfg: BreakerConfig) -> Self { Self { cfg, ewma_fail: 0.0, total_obs: 0, state: pl::Mutex::new(BreakerState { st: BrState::Closed }) } }
    pub fn allow(&self) -> bool {
        let mut s = self.state.lock();
        match s.st {
            BrState::Closed => true,
            BrState::Open(until) => { if Instant::now() >= until { s.st = BrState::HalfOpen(Instant::now()); true } else { false } },
            BrState::HalfOpen(last) => { if Instant::now().duration_since(last) >= self.cfg.half_open_probe_interval { s.st = BrState::HalfOpen(Instant::now()); true } else { false } }
        }
    }
    pub fn on_success(&mut self) { self.update_ewma(false); let mut s = self.state.lock(); if matches!(s.st, BrState::HalfOpen(_)) { s.st = BrState::Closed; } }
    pub fn on_failure(&mut self) { self.update_ewma(true); if self.should_open() { let mut s = self.state.lock(); s.st = BrState::Open(Instant::now() + self.cfg.open_duration); } }
    fn update_ewma(&mut self, failure: bool) { let a = self.cfg.ewma_alpha; let x = if failure { 1.0 } else { 0.0 }; self.ewma_fail = a * x + (1.0 - a) * self.ewma_fail; self.total_obs = self.total_obs.saturating_add(1); }
    fn should_open(&self) -> bool { self.total_obs >= self.cfg.min_observations && self.ewma_fail >= self.cfg.open_threshold }
    pub fn ewma_failure_rate(&self) -> f64 { self.ewma_fail }
}

// ========== Confidence Model ==========
#[derive(Debug, Clone)]
pub struct LeaderConfidence { pub slot: Slot, pub leader: Pubkey, pub confidence: f64, pub components: ConfidenceComponents }

#[derive(Debug, Clone)]
pub struct ConfidenceComponents { pub rpc_consistency: f64, pub vote_freshness: f64, pub slot_drift: f64, pub fork_risk: f64 }

pub struct ConfidenceCalibrator { pub max_vote_lag: u64, pub max_slot_drift_ms: u64, pub fork_penalty_weight: f64 }

impl Default for ConfidenceCalibrator { fn default() -> Self { Self { max_vote_lag: 256, max_slot_drift_ms: 150, fork_penalty_weight: 0.3 } } }

impl ConfidenceCalibrator {
    pub fn compute(&self, endpoint_agreement: f64, vote_lag: Option<u64>, local_drift_ms: u64, fork_risk_est: f64) -> (f64, ConfidenceComponents) {
        let rpc_consistency = endpoint_agreement.clamp(0.0, 1.0);
        let vote = vote_lag.unwrap_or(u64::MAX);
        let vote_freshness = if vote == u64::MAX { 0.0 } else { (1.0 - (vote as f64 / self.max_vote_lag as f64)).clamp(0.0, 1.0) };
        let slot_drift = (1.0 - (local_drift_ms as f64 / self.max_slot_drift_ms as f64)).clamp(0.0, 1.0);
        let fork_inverse = (1.0 - fork_risk_est).clamp(0.0, 1.0);
        let mut composite = 0.45 * rpc_consistency + 0.30 * vote_freshness + 0.15 * slot_drift + 0.10 * fork_inverse;
        composite *= (1.0 - self.fork_penalty_weight * fork_risk_est).clamp(0.5, 1.0);
        (composite.clamp(0.0, 1.0), ConfidenceComponents { rpc_consistency, vote_freshness, slot_drift, fork_risk: fork_inverse })
    }
}

// ========== Validator Trust Profiling ==========
#[derive(Debug, Clone)]
pub struct ValidatorTrust { pub skip_rate: f64, pub commission_delta: i8, pub stake_delta_bp: i64, pub stability_score: f64 }

pub fn compute_validator_trust(
    history_slots_expected: u64,
    history_slots_missed: u64,
    prev_commission: u8,
    current_commission: u8,
    stake_history: &[u64], // oldest..newest, len>=2 preferred
) -> ValidatorTrust {
    let skip_rate = if history_slots_expected == 0 { 0.0 } else { (history_slots_missed as f64 / history_slots_expected as f64).clamp(0.0, 1.0) };
    let mut cd = (current_commission as i16) - (prev_commission as i16);
    cd = cd.clamp(-100, 100);
    let commission_delta = cd as i8;
    let stake_delta_bp = if stake_history.len() >= 2 {
        let oldest = *stake_history.first().unwrap();
        let newest = *stake_history.last().unwrap();
        if oldest == 0 { 0 } else { (((newest as i128 - oldest as i128) * 10_000i128) / oldest as i128) as i64 }
    } else { 0 };
    let skip_pen = (1.0 - skip_rate).clamp(0.0, 1.0);
    let comm_pen = (1.0 - (commission_delta.abs() as f64 / 50.0)).clamp(0.0, 1.0);
    let churn = (stake_delta_bp.abs() as f64 / 10_000.0).min(1.0);
    let churn_pen = (1.0 - churn).clamp(0.0, 1.0);
    let stability_score = (0.6 * skip_pen + 0.2 * comm_pen + 0.2 * churn_pen).clamp(0.0, 1.0);
    ValidatorTrust { skip_rate, commission_delta, stake_delta_bp, stability_score }
}

impl LeaderSchedulePredictor {
    pub async fn leader_confidence_for_slot(&self, slot: Slot, leader: Pubkey) -> LeaderConfidence {
        // Endpoint agreement approximation: lower agreement if endpoints disagree on slot by a wide margin
        let mut endpoint_agreement = 1.0f64;
        if let Some(pool) = &self.rpc_pool {
            if let Ok(samples) = pool.sample_current_slots().await {
                if !samples.is_empty() {
                    let min_s = samples.iter().map(|(_, s, _)| *s).min().unwrap_or(0);
                    let max_s = samples.iter().map(|(_, s, _)| *s).max().unwrap_or(0);
                    let spread = max_s.saturating_sub(min_s) as f64;
                    // Map spread to [0,1] where <=1 slot spread => ~1.0, >=10 slots => ~0.0
                    endpoint_agreement = (1.0 - (spread / 10.0)).clamp(0.0, 1.0);
                }
            }
        }
        let validators = self.validator_info.read().await;
        let vote_lag = validators.get(&leader).and_then(|v| v.last_vote).map(|lv| self.current_slot.load(AtomicOrdering::Acquire).saturating_sub(lv));
        // Estimate local drift using slot estimator vs. atomic current slot
        let predicted = self.slot_estimator.predicted_slot();
        let current = self.current_slot.load(AtomicOrdering::Acquire);
        let drift_slots = if predicted >= current { predicted - current } else { current - predicted };
        let local_drift_ms = drift_slots.saturating_mul(self.slot_estimator.nominal_slot_ms as u64);

        // Fork-aware risk using cluster-wide vote agreement in last 32 slots
        // Compute fraction of validators whose last_vote >= current_slot - 32
        let current_slot_val = self.current_slot.load(AtomicOrdering::Acquire);
        let window = 32u64;
        let agree_cnt = validators.values().filter(|v| v.last_vote.map(|lv| lv >= current_slot_val.saturating_sub(window)).unwrap_or(false)).count();
        let total_cnt = validators.len().max(1);
        let cluster_agreement = (agree_cnt as f64) / (total_cnt as f64);
        // Quadratic penalty when agreement < 70%
        let fork_risk_est = if cluster_agreement < 0.70 { let d = 0.70 - cluster_agreement; (d * d).clamp(0.0, 1.0) } else { 0.0 };
        let calib = ConfidenceCalibrator::default();
        let (mut score, comps) = calib.compute(endpoint_agreement, vote_lag, local_drift_ms, fork_risk_est);
        // Apply validator trust multiplier if available
        if let Some(v) = validators.get(&leader) {
            if let Some(trust) = &v.trust_profile {
                // penalize skip_rate > 10%
                if trust.skip_rate > 0.10 { score *= (1.0 - ((trust.skip_rate - 0.10) * 0.5)).clamp(0.7, 1.0); }
                score *= trust.stability_score.clamp(0.5, 1.0);
            }
        }
        {
            let mut at = self.auto_tuner.lock();
            at.observe_confidence(score);
        }
        LeaderConfidence { slot, leader, confidence: score, components: comps }
    }
}

// ========== SlotTicker and PrefetchController ==========
pub struct SlotTicker { nominal_slot_ms: u64, state: pl::Mutex<TickState> }
struct TickState { last_rpc_slot: Slot, last_rpc_at: Instant, ntp_offset_ms: i64 }
impl SlotTicker { pub fn new(nominal_slot_ms: u64) -> Self { Self { nominal_slot_ms, state: pl::Mutex::new(TickState { last_rpc_slot: 0, last_rpc_at: Instant::now(), ntp_offset_ms: 0 }) } }
    pub fn update_from_rpc(&self, rpc_slot: Slot) { let mut s = self.state.lock(); s.last_rpc_slot = rpc_slot; s.last_rpc_at = Instant::now(); }
    pub fn set_ntp_offset_ms(&self, offset_ms: i64) { self.state.lock().ntp_offset_ms = offset_ms; }
    pub fn predicted_slot(&self) -> Slot { let s = self.state.lock(); let elapsed_ms = Instant::now().duration_since(s.last_rpc_at).as_millis() as i64 + s.ntp_offset_ms; let delta_slots = if elapsed_ms <= 0 { 0 } else { (elapsed_ms as u64) / self.nominal_slot_ms }; s.last_rpc_slot.saturating_add(delta_slots) }
}

pub struct KalmanPrefetch { x: [f64; 2], p: [[f64; 2]; 2], r_meas: f64, q_proc: f64, last_t: Instant, window_slots: u64 }
impl KalmanPrefetch {
    pub fn new(initial_slot: Slot, nominal_ms_per_slot: f64, window_slots: u64) -> Self {
        Self { x: [initial_slot as f64, 1.0 / nominal_ms_per_slot], p: [[1.0, 0.0], [0.0, 1.0]], r_meas: 4.0, q_proc: 1e-4, last_t: Instant::now(), window_slots }
    }
    pub fn predict(&mut self) {
        let dt_ms = self.last_t.elapsed().as_millis().max(1) as f64; self.last_t = Instant::now(); let dt = dt_ms;
        self.x = [ self.x[0] + dt * self.x[1], self.x[1] ];
        let p00 = self.p[0][0] + dt * (self.p[1][0] + self.p[0][1]) + dt*dt * self.p[1][1] + self.q_proc;
        let p01 = self.p[0][1] + dt * self.p[1][1];
        let p10 = self.p[1][0] + dt * self.p[1][1];
        let p11 = self.p[1][1] + self.q_proc;
        self.p = [[p00, p01], [p10, p11]];
    }
    pub fn update(&mut self, measured_slot: Slot) {
        let z = measured_slot as f64;
        let y = z - self.x[0];
        let s = self.p[0][0] + self.r_meas;
        let k0 = self.p[0][0] / s; let k1 = self.p[1][0] / s;
        self.x[0] += k0 * y; self.x[1] += k1 * y;
        let p00 = (1.0 - k0) * self.p[0][0];
        let p01 = (1.0 - k0) * self.p[0][1];
        let p10 = self.p[1][0] - k1 * self.p[0][0];
        let p11 = self.p[1][1] - k1 * self.p[0][1];
        self.p = [[p00, p01], [p10, p11]];
    }
    pub fn lookahead_window(&self) -> u64 { let vel_var = self.p[1][1].max(1e-8); let factor = (1.0 + 10.0 * vel_var.sqrt()).clamp(1.0, 3.0); ((self.window_slots as f64) * factor) as u64 }
    pub fn predicted_slot(&self) -> Slot { self.x[0].max(0.0) as u64 }
}

pub struct PrefetchController { target_ms_per_slot: f64, window_slots: u64, last_slot: pl::Mutex<(Slot, Instant)>, kalman: pl::Mutex<KalmanPrefetch> }
impl PrefetchController {
    pub fn new(target_ms_per_slot: f64, window_slots: u64) -> Self { Self { target_ms_per_slot, window_slots, last_slot: pl::Mutex::new((0, Instant::now())), kalman: pl::Mutex::new(KalmanPrefetch::new(0, target_ms_per_slot, window_slots)) } }
    pub fn observe_slot(&self, slot: Slot) { let mut s = self.last_slot.lock(); *s = (slot, Instant::now()); let mut k = self.kalman.lock(); k.update(slot); }
    pub fn lookahead_slots(&self) -> u64 { let mut k = self.kalman.lock(); k.predict(); k.lookahead_window() }
}

// ========== Weighted Slot Estimator (RPC weighted median + local clock + NTP) ==========
#[derive(Debug, Clone)]
pub struct SlotEstimatorCfg { pub decay_lambda: f64, pub blend_alpha_min: f64, pub blend_alpha_max: f64 }
impl Default for SlotEstimatorCfg { fn default() -> Self { Self { decay_lambda: 0.02, blend_alpha_min: 0.2, blend_alpha_max: 0.8 } } }

pub struct WeightedSlotEstimator {
    pub nominal_slot_ms: u64,
    cfg: SlotEstimatorCfg,
    ntp_offset_ms: i64,
    last_rpc_slot: pl::Mutex<Slot>,
    last_rpc_at: pl::Mutex<Instant>,
}

impl WeightedSlotEstimator {
    pub fn new(nominal_slot_ms: u64, cfg: SlotEstimatorCfg, init_slot: Slot) -> Self {
        Self { nominal_slot_ms, cfg, ntp_offset_ms: 0, last_rpc_slot: pl::Mutex::new(init_slot), last_rpc_at: pl::Mutex::new(Instant::now()) }
    }
    pub fn set_ntp_offset_ms(&self, offset_ms: i64) { self.ntp_offset_ms = offset_ms; }
    pub fn observe_slot(&self, slot: Slot) { *self.last_rpc_slot.lock() = slot; *self.last_rpc_at.lock() = Instant::now(); }
    pub fn predicted_slot(&self) -> Slot {
        let slot = *self.last_rpc_slot.lock();
        let at = *self.last_rpc_at.lock();
        let elapsed_ms = at.elapsed().as_millis() as i64 + self.ntp_offset_ms;
        let delta_slots = if elapsed_ms <= 0 { 0 } else { (elapsed_ms as u64) / self.nominal_slot_ms };
        slot.saturating_add(delta_slots)
    }
    pub fn weighted_median_slot(&self, samples: &[(usize, Slot, f64)]) -> Slot {
        if samples.is_empty() { return self.predicted_slot(); }
        // weights: exp(-lambda * latency_ms)
        let lambda = self.cfg.decay_lambda.max(0.0001);
        let mut v: Vec<(Slot, f64)> = samples.iter().map(|&(_, s, l)| (s, (-(lambda * l)).exp())).collect();
        v.sort_by_key(|(s, _)| *s);
        let total_w: f64 = v.iter().map(|(_, w)| *w).sum::<f64>().max(1e-9);
        let mut acc = 0.0;
        for (s, w) in v { acc += w; if acc >= 0.5 * total_w { return s; } }
        self.predicted_slot()
    }
    pub fn update_from_pool_samples(&self, samples: &[(usize, Slot, f64)]) {
        if samples.is_empty() { return; }
        let median = self.weighted_median_slot(samples);
        // blend median with local monotonic prediction
        let local_pred = self.predicted_slot();
        let n = samples.len() as f64;
        let alpha = (self.cfg.blend_alpha_min + (self.cfg.blend_alpha_max - self.cfg.blend_alpha_min) * (1.0 - (1.0/ (1.0 + n/3.0)))).clamp(0.0, 1.0);
        let blended = if median >= local_pred {
            let diff = median - local_pred; local_pred.saturating_add(((diff as f64) * alpha) as u64)
        } else {
            let diff = local_pred - median; local_pred.saturating_sub(((diff as f64) * alpha) as u64)
        };
        self.observe_slot(blended);
    }
}

#[derive(Debug, Clone)]
struct EpochSchedule {
    pub epoch: Epoch,
    pub start_slot: Slot,
    pub end_slot: Slot,
    pub leader_schedule: Vec<(Slot, Pubkey)>,
    pub slot_leaders: HashMap<Slot, Pubkey>,
    // Compact per-validator storage: use a small fixed-capacity stack array and spill to heap on demand
    pub leader_slots: HashMap<Pubkey, SmallSlots>,
    pub cached_at: Instant,
}

// SmallVec-backed compact slots wrapper with Vec-like ergonomics
#[derive(Debug, Clone, Default)]
pub struct SmallSlots(SmallVec<[Slot; 64]>);

impl SmallSlots {
    pub fn new() -> Self { Self(SmallVec::new()) }
    pub fn with_capacity(n: usize) -> Self { Self(SmallVec::with_capacity(n)) }
    pub fn push(&mut self, slot: Slot) { self.0.push(slot) }
    pub fn len(&self) -> usize { self.0.len() }
    pub fn is_empty(&self) -> bool { self.0.is_empty() }
    pub fn iter(&self) -> impl Iterator<Item = &Slot> { self.0.iter() }
    pub fn clear(&mut self) { self.0.clear() }
    pub fn binary_search(&self, x: &Slot) -> Result<usize, usize> { self.0.binary_search(x) }
    pub fn next_after(&self, current: Slot) -> Option<Slot> {
        let idx = match self.0.binary_search(&current) { Ok(i) => i.saturating_add(1), Err(i) => i };
        self.0.get(idx).copied()
    }
    pub fn sort_unstable(&mut self) { self.0.sort_unstable() }
}

impl From<Vec<Slot>> for SmallSlots { fn from(v: Vec<Slot>) -> Self { Self(SmallVec::from_vec(v)) } }
impl Into<Vec<Slot>> for SmallSlots { fn into(self) -> Vec<Slot> { self.0.into_vec() } }
impl Deref for SmallSlots { type Target = [Slot]; fn deref(&self) -> &Self::Target { &self.0 } }
impl DerefMut for SmallSlots { fn deref_mut(&mut self) -> &mut Self::Target { &mut self.0 } }
impl<'a> IntoIterator for &'a SmallSlots { type Item = &'a Slot; type IntoIter = std::slice::Iter<'a, Slot>; fn into_iter(self) -> Self::IntoIter { self.0.iter() } }
impl IntoIterator for SmallSlots { type Item = Slot; type IntoIter = smallvec::IntoIter<[Slot; 64]>; fn into_iter(self) -> Self::IntoIter { self.0.into_iter() } }
impl Index<usize> for SmallSlots { type Output = Slot; fn index(&self, idx: usize) -> &Self::Output { &self.0[idx] } }

#[derive(Debug, Clone)]
struct ValidatorInfo {
    pub pubkey: Pubkey,
    pub stake: u64,
    pub commission: u8,
    pub last_vote: Option<Slot>,
    pub vote_account: Pubkey,
    // Trust profiling fields
    pub trust_profile: Option<ValidatorTrust>,
    pub prev_commission: Option<u8>,
    pub stake_history: SmallVec<[u64; 8]>,
    pub expected_slots_last_epoch: Option<u64>,
    pub missed_slots_last_epoch: Option<u64>,
}

pub struct LeaderSchedulePredictor {
    rpc_client: Arc<RpcClient>,
    rpc_endpoint_str: String,
    rpc_pool: Option<Arc<RpcPool>>, // optional hedged pool
    epoch_schedules: Arc<DashMap<Epoch, EpochSchedule>>,
    validator_info: Arc<RwLock<HashMap<Pubkey, ValidatorInfo>>>,
    current_slot: Arc<AtomicU64>,
    current_epoch: Arc<AtomicU64>,
    slots_per_epoch: u64,
    first_normal_epoch: u64,
    first_normal_slot: u64,
    leader_schedule_slot_offset: u64,
    identity_filter: Option<String>,
    leader_schedule_cache: Arc<RwLock<BTreeMap<Slot, LeaderInfo>>>,
    update_lock: Arc<Mutex<()>>,
    // Circuit breaker
    breaker_failures: Arc<AtomicU64>,
    breaker_tripped_until: Arc<Mutex<Option<Instant>>>,
    // New configurable policy
    config: PredictorConfig,
    breaker2: Arc<CircuitBreaker>,
    // Adaptive hedging helpers
    slot_ticker: Arc<SlotTicker>,
    prefetch_ctl: Arc<PrefetchController>,
    slot_estimator: Arc<WeightedSlotEstimator>,
    auto_tuner: pl::Mutex<AutoTuner>,
    trust_profiler: Arc<TrustProfiler>,
    mev_weighter: Option<Arc<SlotWeighter>>, // optional MEV-aware per-slot weighting
    // Progressive prefetch stage tracker for next epoch: 0,1,2 correspond to 10%,30%,70%
    next_epoch_stage: pl::Mutex<Option<(Epoch, u8)>>,
    // RL contextual bandit optimizer (optional online weighting)
    cb_opt: pl::Mutex<CbOptimizer>,
    // Latency-aware predicted slot wrapper
    latency_staleness: Option<Arc<dyn HashStaleness + Send + Sync>>, 
    winprob_cfg: WinProbCfg,
    relay_obs: Option<Arc<dyn RelayObservations + Send + Sync>>,
    hier_cache: Option<HierCache>,
    epoch_sems: EpochSemaphores,
    relay_cutoff: Option<Arc<RelayCutoffCalibrator>>,
    mlp: pl::Mutex<TinyMlp>,
    entropy: pl::Mutex<EntropyForecaster>,
    hash_freshness: Option<Arc<dyn HashFreshness + Send + Sync>>,
}

#[derive(Debug, Clone)]
pub struct PredictorConfig {
    pub rpc_retry_attempts: u32,
    pub base_backoff_ms: u64,
    pub backoff_cap_ms: u64,
    pub hedge_stagger_ms: u64,
    pub breaker_fail_threshold: u32,
    pub breaker_open_duration: Duration,
    pub max_inflight_fetches: usize,
}

impl Default for PredictorConfig {
    fn default() -> Self {
        Self {
            rpc_retry_attempts: 5,
            base_backoff_ms: 50,
            backoff_cap_ms: 2_000,
            hedge_stagger_ms: 25,
            breaker_fail_threshold: 8,
            breaker_open_duration: Duration::from_secs(30),
            max_inflight_fetches: 16,
        }
    }
}

// ========== Hedged RPC Pool ==========
#[derive(Clone)]
pub struct RpcEndpoint { pub url: String, pub client: Arc<RpcClient> }

#[derive(Debug, Clone)]
pub struct RpcPoolConfig { pub hedge_stagger_ms: u64, pub ewma_alpha: f64, pub rank_shuffle_top_k: usize }

impl Default for RpcPoolConfig { fn default() -> Self { Self { hedge_stagger_ms: 25, ewma_alpha: 0.2, rank_shuffle_top_k: 2 } } }

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BrState2 { Closed, Open, HalfOpen }

#[derive(Debug, Clone)]
struct DynBreakerCfg {
    ewma_alpha: f64,
    open_threshold: f64,
    min_obs: u32,
    base_open: Duration,
    max_open: Duration,
    half_open_probe_every: Duration,
    half_open_traffic_share: f64,
}

impl Default for DynBreakerCfg {
    fn default() -> Self {
        Self {
            ewma_alpha: 0.2,
            open_threshold: 0.6,
            min_obs: 30,
            base_open: Duration::from_secs(15),
            max_open: Duration::from_secs(120),
            half_open_probe_every: Duration::from_secs(2),
            half_open_traffic_share: 0.05,
        }
    }
}

struct DynBreaker {
    cfg: DynBreakerCfg,
    ewma_fail: f64,
    obs: u32,
    state: pl::Mutex<(BrState2, Instant, Duration, Instant)>, // (state, opened_at, open_dur, last_probe)
}

impl DynBreaker {
    fn new(cfg: DynBreakerCfg) -> Self {
        Self {
            cfg,
            ewma_fail: 0.0,
            obs: 0,
            state: pl::Mutex::new((BrState2::Closed, Instant::now(), Duration::from_secs(0), Instant::now() - Duration::from_secs(10))),
        }
    }
    fn allow(&self) -> bool {
        let mut st = self.state.lock();
        match st.0 {
            BrState2::Closed => true,
            BrState2::Open => {
                if Instant::now().duration_since(st.1) >= st.2.max(self.cfg.base_open) {
                    st.0 = BrState2::HalfOpen; st.3 = Instant::now(); true
                } else { false }
            }
            BrState2::HalfOpen => {
                if Instant::now().duration_since(st.3) >= self.cfg.half_open_probe_every { st.3 = Instant::now(); true } else { false }
            }
        }
    }
    fn on_success(&mut self) {
        let a = self.cfg.ewma_alpha; self.ewma_fail = (1.0 - a) * self.ewma_fail; self.obs = self.obs.saturating_add(1);
        let mut st = self.state.lock();
        if st.0 == BrState2::HalfOpen { st.0 = BrState2::Closed; st.2 = self.cfg.base_open; counter!("breaker.state_changes", 1, "to" => "Closed"); }
    }
    fn on_failure(&mut self) {
        let a = self.cfg.ewma_alpha; self.ewma_fail = a * 1.0 + (1.0 - a) * self.ewma_fail; self.obs = self.obs.saturating_add(1);
        if self.obs >= self.cfg.min_obs && self.ewma_fail >= self.cfg.open_threshold {
            let mut st = self.state.lock();
            if st.0 != BrState2::Open { st.0 = BrState2::Open; st.1 = Instant::now(); st.2 = st.2.saturating_mul(2).max(self.cfg.base_open).min(self.cfg.max_open); counter!("breaker.state_changes", 1, "to" => "Open"); }
        }
    }
    fn traffic_weight(&self) -> f64 { match self.state.lock().0 { BrState2::Closed => 1.0, BrState2::HalfOpen => self.cfg.half_open_traffic_share, BrState2::Open => 0.0 } }
}

#[derive(Debug, Clone)]
struct EpScore { ewma_latency_ms: f64, ewma_fail: f64, successes: u64, failures: u64, lat_hist: VecDeque<f64>, last_outlier_until: Option<Instant>, breaker: DynBreaker, tuned_alpha: f64 }

impl Default for EpScore { fn default() -> Self { Self { ewma_latency_ms: 180.0, ewma_fail: 0.0, successes: 0, failures: 0, lat_hist: VecDeque::with_capacity(64), last_outlier_until: None, breaker: DynBreaker::new(DynBreakerCfg::default()), tuned_alpha: 0.2 } } }

pub struct RpcPool { cfg: RpcPoolConfig, eps: Vec<RpcEndpoint>, scores: pl::Mutex<Vec<EpScore>> }

impl RpcPool {
    pub fn new(cfg: RpcPoolConfig, urls: Vec<String>, commitment: CommitmentConfig) -> Self {
        let eps: Vec<RpcEndpoint> = urls.into_iter().map(|url| RpcEndpoint { client: Arc::new(RpcClient::new_with_commitment(url.clone(), commitment)), url }).collect();
        let scores = vec![EpScore::default(); eps.len()];
        Self { cfg, eps, scores: pl::Mutex::new(scores) }
    }

    fn ranked_indices(&self) -> Vec<usize> {
        let scores = self.scores.lock();
        let mut idxs: Vec<usize> = (0..self.eps.len()).collect();
        idxs.sort_by(|&a, &b| {
            let sa = &scores[a];
            let sb = &scores[b];
            let wa = sa.breaker.traffic_weight().max(0.0001);
            let wb = sb.breaker.traffic_weight().max(0.0001);
            let ca = (0.7 * sa.ewma_latency_ms + 3000.0 * 0.3 * sa.ewma_fail) / wa;
            let cb = (0.7 * sb.ewma_latency_ms + 3000.0 * 0.3 * sb.ewma_fail) / wb;
            ca.partial_cmp(&cb).unwrap_or(std::cmp::Ordering::Equal)
        });
        // Outlier deprioritization window
        let now = Instant::now();
        idxs.retain(|i| {
            let allow_breaker = scores[*i].breaker.allow();
            if let Some(until) = scores[*i].last_outlier_until { allow_breaker && now >= until } else { allow_breaker }
        });
        // Rank shuffle window: randomly permute within top-k to avoid herding
        let k = self.cfg.rank_shuffle_top_k.min(idxs.len()).max(1);
        let mut topk = idxs[..k].to_vec();
        // lightweight shuffle using time-based jitter
        let jitter = (Instant::now().elapsed().subsec_nanos() as usize).max(1);
        for i in 0..topk.len() { let j = (jitter + i * 31) % topk.len(); topk.swap(i, j); }
        let mut rest = idxs[k..].to_vec();
        topk.append(&mut rest);
        topk
    }

    fn on_success(&self, i: usize, latency_ms: f64) {
        let mut scores = self.scores.lock();
        let s = &mut scores[i];
        let a = s.tuned_alpha;
        s.ewma_latency_ms = a * latency_ms + (1.0 - a) * s.ewma_latency_ms;
        s.ewma_fail = (1.0 - a) * s.ewma_fail;
        s.successes = s.successes.saturating_add(1);
        // breaker success
        s.breaker.on_success();
        // record latency history and outlier detection (95th percentile)
        if s.lat_hist.len() >= 128 { s.lat_hist.pop_front(); }
        s.lat_hist.push_back(latency_ms);
        let mut v: Vec<f64> = s.lat_hist.iter().copied().collect();
        v.sort_by(|x, y| x.partial_cmp(y).unwrap_or(std::cmp::Ordering::Equal));
        if !v.is_empty() {
            let p95_idx = ((v.len() as f64) * 0.95).floor() as usize;
            let p95 = v[p95_idx.min(v.len() - 1)];
            if latency_ms > p95.max(1.0) {
                // mark outlier: deprioritize for 5 minutes
                s.last_outlier_until = Some(Instant::now() + Duration::from_secs(300));
            }
        }
    }

    fn on_failure(&self, i: usize) {
        let mut scores = self.scores.lock();
        let s = &mut scores[i];
        let a = s.tuned_alpha;
        s.ewma_fail = a * 1.0 + (1.0 - a) * s.ewma_fail;
        s.failures = s.failures.saturating_add(1);
        // breaker fail
        s.breaker.on_failure();
    }

    pub async fn get_leader_schedule_hedged(
        &self,
        slot: Slot,
    ) -> anyhow::Result<Option<std::collections::HashMap<String, Vec<usize>>>> {
        use futures::{stream::FuturesUnordered, StreamExt};
        let order = self.ranked_indices();
        let base_stagger = Duration::from_millis(self.cfg.hedge_stagger_ms);
        let mut futs: FuturesUnordered<_> = FuturesUnordered::new();
        for (k, &i) in order.iter().enumerate() {
            let cli = self.eps[i].client.clone();
            // add small jitter to prevent predictable staggering
            let jitter_ms = (Instant::now().elapsed().subsec_nanos() as u64) % 7;
            let delay = base_stagger.saturating_mul(k as u32) + Duration::from_millis(jitter_ms);
            futs.push(async move {
                if k > 0 { tokio::time::sleep(delay).await; }
                let t0 = Instant::now();
                let res = cli.get_leader_schedule_with_commitment(Some(slot), CommitmentConfig::finalized()).await;
                (i, t0.elapsed(), res.map_err(anyhow::Error::from))
            });
        }

        while let Some(out) = futs.next().await {
            let (idx, elapsed, res) = out;
            match res {
                Ok(payload) => { self.on_success(idx, elapsed.as_millis() as f64); return Ok(payload); }
                Err(_e) => { self.on_failure(idx); /* continue */ }
            }
        }
        anyhow::bail!("all endpoints failed get_leader_schedule hedged request")
    }

    // Probe slots across endpoints and return samples (idx, slot, latency_ms)
    pub async fn sample_current_slots(&self) -> anyhow::Result<Vec<(usize, Slot, f64)>> {
        use futures::{stream::FuturesUnordered, StreamExt};
        let order = self.ranked_indices();
        if order.is_empty() { return Ok(vec![]); }
        let base_stagger = Duration::from_millis(self.cfg.hedge_stagger_ms);
        let mut futs: FuturesUnordered<_> = FuturesUnordered::new();
        for (k, &i) in order.iter().enumerate() {
            let cli = self.eps[i].client.clone();
            let jitter_ms = (Instant::now().elapsed().subsec_nanos() as u64) % 5;
            let delay = base_stagger.saturating_mul(k as u32) + Duration::from_millis(jitter_ms);
            futs.push(async move {
                if k > 0 { tokio::time::sleep(delay).await; }
                let t0 = Instant::now();
                let res = cli.get_slot().await;
                let dt = t0.elapsed();
                (i, dt, res)
            });
        }

        let mut out: Vec<(usize, Slot, f64)> = Vec::new();
        while let Some((idx, elapsed, res)) = futs.next().await {
            match res {
                Ok(slot) => { self.on_success(idx, elapsed.as_millis() as f64); out.push((idx, slot, elapsed.as_millis() as f64)); }
                Err(_e) => { self.on_failure(idx); }
            }
        }
        Ok(out)
    }

    pub fn apply_autotune(&self, ewma_alpha: f64, breaker_threshold: f64) {
        let mut scores = self.scores.lock();
        let a = ewma_alpha.clamp(0.05, 0.9);
        for s in scores.iter_mut() {
            s.tuned_alpha = a;
            s.breaker.cfg.open_threshold = breaker_threshold.clamp(0.3, 0.95);
        }
    }
    pub fn endpoint_urls(&self) -> Vec<String> { self.eps.iter().map(|e| e.url.clone()).collect() }
}

struct CircuitBreaker {
    consecutive_failures: std::sync::atomic::AtomicU32,
    open_until: pl::Mutex<Option<Instant>>,
}

impl CircuitBreaker {
    fn new() -> Self {
        Self { consecutive_failures: std::sync::atomic::AtomicU32::new(0), open_until: pl::Mutex::new(None) }
    }
    fn allow(&self) -> bool {
        if let Some(until) = *self.open_until.lock() { if Instant::now() < until { return false; } }
        true
    }
    fn on_success(&self) { self.consecutive_failures.store(0, std::sync::atomic::Ordering::Relaxed); *self.open_until.lock() = None; }
    fn on_failure(&self, threshold: u32, open_for: Duration) {
        let n = self.consecutive_failures.fetch_add(1, std::sync::atomic::Ordering::Relaxed) + 1;
        if n >= threshold { *self.open_until.lock() = Some(Instant::now() + open_for); }
    }
}

impl LeaderSchedulePredictor {
    pub async fn new(rpc_client: Arc<RpcClient>) -> Result<Self> {
        let epoch_info = rpc_client.get_epoch_info().await?;
        let epoch_schedule = rpc_client.get_epoch_schedule().await?;

        let slots_per_epoch = epoch_schedule.slots_per_epoch;
        let first_normal_epoch = epoch_schedule.first_normal_epoch;
        let first_normal_slot = epoch_schedule.first_normal_slot;
        let leader_schedule_slot_offset = epoch_schedule.leader_schedule_slot_offset;

        let predictor = Self {
            rpc_client,
            rpc_endpoint_str: String::new(),
            epoch_schedules: Arc::new(DashMap::new()),
            validator_info: Arc::new(RwLock::new(HashMap::new())),
            current_slot: Arc::new(AtomicU64::new(epoch_info.absolute_slot)),
            current_epoch: Arc::new(AtomicU64::new(epoch_info.epoch)),
            slots_per_epoch,
            first_normal_epoch,
            first_normal_slot,
            leader_schedule_slot_offset,
            identity_filter: None,
            leader_schedule_cache: Arc::new(RwLock::new(BTreeMap::new())),
            update_lock: Arc::new(Mutex::new(())),
            breaker_failures: Arc::new(AtomicU64::new(0)),
            breaker_tripped_until: Arc::new(Mutex::new(None)),
            config: PredictorConfig::default(),
            breaker2: Arc::new(CircuitBreaker::new()),
            rpc_pool: None,
            slot_ticker: Arc::new(SlotTicker::new(400)),
            prefetch_ctl: Arc::new(PrefetchController::new(400.0, 128)),
            slot_estimator: Arc::new(WeightedSlotEstimator::new(400, SlotEstimatorCfg::default(), epoch_info.absolute_slot)),
            auto_tuner: pl::Mutex::new(AutoTuner::new(Duration::from_secs(30), 0.2, 0.6)),
            trust_profiler: Arc::new(TrustProfiler::new(TrustCfg::default())),
            mev_weighter: None,
            next_epoch_stage: pl::Mutex::new(None),
            cb_opt: pl::Mutex::new(CbOptimizer::new("cb_weights.bin".to_string())),
            latency_staleness: None,
            winprob_cfg: WinProbCfg::default(),
            relay_obs: None,
            hier_cache: None,
            epoch_sems: EpochSemaphores::new(8, 4, 2),
            relay_cutoff: None,
            mlp: pl::Mutex::new(TinyMlp::load_or_new("ev_mlp.bin", 0.02, 1e-4)),
            entropy: pl::Mutex::new(EntropyForecaster::new(0.2)),
            hash_freshness: None,
        };

        predictor.initialize().await?;
        Ok(predictor)
    }

    pub async fn new_with_endpoint(rpc_client: Arc<RpcClient>, endpoint: String, config: Option<PredictorConfig>) -> Result<Self> {
        let mut me = Self::new(rpc_client).await?;
        me.rpc_endpoint_str = endpoint;
        if let Some(cfg) = config { me.config = cfg; }
        Ok(me)
    }

    pub fn set_rpc_pool(&mut self, urls: Vec<String>) {
        if urls.is_empty() { self.rpc_pool = None; return; }
        let pool = RpcPool::new(RpcPoolConfig::default(), urls, CommitmentConfig::finalized());
        self.rpc_pool = Some(Arc::new(pool));
    }

    async fn initialize(&self) -> Result<()> {
        self.update_validator_info().await?;
        // Refresh current slot/epoch from processed commitment
        let current_slot = self
            .rpc_client
            .get_slot_with_commitment(CommitmentConfig::processed())
            .await?;
        let current_epoch = self.slot_to_epoch(current_slot);
        self.current_slot.store(current_slot, AtomicOrdering::Release);
        self.current_epoch.store(current_epoch, AtomicOrdering::Release);

        // Pre-warm prev, current, next in parallel
        let prev = current_epoch.saturating_sub(1);
        let next = current_epoch + 1;
        let f1 = self.fetch_and_cache_epoch_schedule(prev);
        let f2 = self.fetch_and_cache_epoch_schedule(current_epoch);
        let f3 = self.fetch_and_cache_epoch_schedule(next);
        let _ = futures::future::join3(f1, f2, f3).await;
        Ok(())
    }

    pub async fn start_background_updater(self: Arc<Self>) {
        let schedule_updater = self.clone();
        tokio::spawn(async move {
            let mut interval = interval(LEADER_SCHEDULE_REFRESH_INTERVAL);
            loop {
                interval.tick().await;
                if let Err(e) = schedule_updater.update_schedules().await {
                    error!("Failed to update leader schedules: {}", e);
                }
            }
        });

        let validator_updater = self.clone();
        tokio::spawn(async move {
            let mut interval = interval(VOTE_ACCOUNT_REFRESH_INTERVAL);
            loop {
                interval.tick().await;
                if let Err(e) = validator_updater.update_validator_info().await {
                    error!("Failed to update validator info: {}", e);
                }
            }
        });

        let slot_updater = self.clone();
        tokio::spawn(async move {
            loop {
                match slot_updater.update_current_slot().await {
                    Ok(_) => sleep(Duration::from_millis(400)).await,
                    Err(e) => {
                        error!("Failed to update current slot: {}", e);
                        sleep(Duration::from_secs(1)).await;
                    }
                }
            }
        });
    }

    async fn update_current_slot(&self) -> Result<()> {
        let t0 = Instant::now();
        // Prefer pooled weighted estimation; fallback to single RPC
        let slot = if let Some(pool) = &self.rpc_pool {
            match pool.sample_current_slots().await {
                Ok(samples) if !samples.is_empty() => {
                    // Feed slot estimator with weighted-median sample set
                    self.slot_estimator.update_from_pool_samples(&samples);
                    // Use median directly for atomic current slot update
                    self.slot_estimator.weighted_median_slot(&samples)
                }
                _ => { let s = self.rpc_client.get_slot().await?; self.slot_estimator.observe_slot(s); s },
            }
        } else {
            let s = self.rpc_client.get_slot().await?; self.slot_estimator.observe_slot(s); s
        };
        let epoch = self.slot_to_epoch(slot);
        // Feed on-device ticker and prefetch controller
        self.slot_ticker.update_from_rpc(slot);
        self.prefetch_ctl.observe_slot(slot);
        let ms = t0.elapsed().as_millis() as f64;
        let ep_hash = self.hash_endpoint();
        histogram!("slot_update.ms", ms, "ep" => format!("{}", ep_hash));
        // Update atomic current slot
        self.current_slot.store(slot, AtomicOrdering::Release);
        self.current_epoch.store(epoch, AtomicOrdering::Release);
        Ok(())
    }

    async fn update_schedules(&self) -> Result<()> {
        let _lock = self.update_lock.lock().await;
        // Fetch current epoch and next epoch if missing or stale
        let current_epoch = self.current_epoch.load(AtomicOrdering::Acquire);
        let current_slot = self.current_slot.load(AtomicOrdering::Acquire);
        
        self.cleanup_old_epochs(current_epoch);

        // Refresh current and next if missing or stale
        for e in [current_epoch, current_epoch + 1] {
            let stale_or_missing = match self.epoch_schedules.get(&e) {
                Some(s) => s.cached_at.elapsed() >= SCHEDULE_CACHE_TTL,
                None => true,
            };
            if stale_or_missing {
                let permit = if e == current_epoch { self.epoch_sems.acquire_current().await } else { self.epoch_sems.acquire_next().await };
                let _ = self.fetch_and_cache_epoch_schedule(e).await;
                drop(permit);
            }
        }

        // Slot-aware prefetch and progressive warm stages for next epoch
        let next_start = self.epoch_to_slot(current_epoch + 1);
        let slots_left = next_start.saturating_sub(current_slot);
        let lookahead = self.prefetch_ctl.lookahead_slots();
        let mut threshold = (PREFETCH_THRESHOLD_SLOTS.saturating_div(2)).saturating_add(lookahead / 4);
        if let Some(hf) = &self.hash_freshness { // adapt based on blockhash staleness
            let base = threshold;
            if let Some(age) = hf.blockhash_age_ms() {
                let slots = age as f64 / 400.0f64; // nominal
                let factor = if slots > 2.0 { (slots / 2.0).min(2.0) } else { 1.0 };
                threshold = ((base as f64) * factor) as u64;
            }
        }
        // Progressive warm for next epoch at 10%, 30%, 70%
        let slots_per_epoch = self.slots_per_epoch;
        let epoch_start = self.epoch_to_slot(current_epoch);
        let progressed = current_slot.saturating_sub(epoch_start);
        let progress_ratio = (progressed as f64 / slots_per_epoch as f64).clamp(0.0, 1.0);
        let mut stage = self.next_epoch_stage.lock();
        let next_epoch = current_epoch + 1;
        let mut trigger = None;
        match *stage {
            Some((e, s)) if e == next_epoch => {
                if s == 0 && progress_ratio >= 0.10 { trigger = Some(1); }
                else if s == 1 && progress_ratio >= 0.30 { trigger = Some(2); }
                else if s == 2 && progress_ratio >= 0.70 { trigger = Some(3); }
            }
            _ => { if progress_ratio >= 0.10 { trigger = Some(1); } }
        }
        drop(stage);
        if let Some(new_stage) = trigger {
            let _permit = self.epoch_sems.acquire_next().await;
            let _ = self.fetch_and_cache_epoch_schedule(next_epoch).await;
            *self.next_epoch_stage.lock() = Some((next_epoch, new_stage));
        }

        // Determine how many future epochs to prewarm (N+2 or more) based on proximity and lookahead
        let mut max_ahead: u64 = 2;
        if slots_left <= threshold { max_ahead = 3; }
        if slots_left <= threshold / 2 { max_ahead = 4; }
        for ahead in 2..=max_ahead { let _p = self.epoch_sems.acquire_future().await; let _ = self.fetch_and_cache_epoch_schedule(current_epoch + ahead).await; }
        Ok(())
    }

    async fn fetch_and_cache_epoch_schedule(&self, epoch: Epoch) -> Result<EpochSchedule> {
        if !self.breaker2.allow() {
            counter!("leader_schedule.breaker.open", 1);
            return Err(anyhow!("circuit breaker open; refusing RPC fetch for epoch {}", epoch));
        }
        let mut attempt: u32 = 0;
        loop {
            let t0 = Instant::now();
            let res = self.fetch_epoch_schedule_internal(epoch).await;
            let ms = t0.elapsed().as_millis() as f64;
            let ep_hash = self.hash_endpoint();
            histogram!("leader_schedule.fetch_ms", ms, "epoch" => epoch.to_string(), "ep" => format!("{}", ep_hash));
            match res {
                Ok(s) => {
                    self.breaker2.on_success();
                    self.epoch_schedules.insert(epoch, s.clone());
                    counter!("leader_schedule.fetch_ok", 1, "epoch" => epoch.to_string(), "ep" => format!("{}", ep_hash));
                    return Ok(s);
                }
                Err(e) => {
                    counter!("leader_schedule.fetch_err", 1, "epoch" => epoch.to_string(), "ep" => format!("{}", ep_hash));
                    self.breaker2.on_failure(self.config.breaker_fail_threshold, self.config.breaker_open_duration);
                    attempt = attempt.saturating_add(1);
                    if attempt >= self.config.rpc_retry_attempts {
                        return Err(e);
                    }
                    let delay = self.jitter_backoff_ms(attempt);
                    warn!(epoch, attempt, delay_ms = delay, "retrying leader schedule fetch: {e}");
                    sleep(Duration::from_millis(delay)).await;
                }
            }
        }
    }

    fn hash_endpoint(&self) -> u64 {
        let mut h = std::collections::hash_map::DefaultHasher::new();
        self.rpc_endpoint_str.hash(&mut h);
        h.finish()
    }

    fn jitter_backoff_ms(&self, attempt: u32) -> u64 {
        // Exponential backoff with simple time-based jitter to avoid rand dep
        let base = self.config.base_backoff_ms;
        let cap = self.config.backoff_cap_ms;
        let exp = base.saturating_mul(1u64 << attempt.min(6));
        let maxd = exp.min(cap).max(base);
        let nanos = Instant::now().elapsed().subsec_nanos() as u64;
        base + (nanos % maxd)
    }

    async fn fetch_epoch_schedule_internal(&self, epoch: Epoch) -> Result<EpochSchedule> {
        // Respect getLeaderSchedule semantics: expects a slot within the target epoch.
        // Apply optional identity filter to reduce payload size.
        let slot_in_epoch = Some(self.epoch_to_slot(epoch));
        // If a hedged pool is configured, prefer it
        let schedule_opt = if let Some(pool) = &self.rpc_pool {
            // Anti-censorship: sample multiple endpoints and analyze divergence
            let samples = pool.sample_leader_schedules(slot_in_epoch.unwrap(), 4).await;
            let schedule_opt = samples.iter().find_map(|(_, s)| s.clone());
            // Analyze censorship and quarantine suspicious endpoints
            let report = analyze_censorship(epoch, &samples, self.epoch_to_slot(epoch));
            if !report.suspicious_endpoints.is_empty() {
                counter!("censorship.suspicious_endpoints", report.suspicious_endpoints.len() as u64, "epoch" => epoch.to_string());
                pool.quarantine_endpoints(&report.suspicious_endpoints, Duration::from_secs(300));
            }
            schedule_opt
        } else {
            let config = RpcLeaderScheduleConfig { identity: self.identity_filter.clone() };
            self.rpc_client
                .get_leader_schedule_with_commitment_and_config(
                    slot_in_epoch,
                    CommitmentConfig::finalized(),
                    Some(config),
                )
                .await?
        };

        let schedule_data = match schedule_opt {
            Some(data) => data,
            None => {
                // Likely outside RPC leader schedule cache window; surface clear error and do not busy-retry
                return Err(anyhow!(
                    "Leader schedule unavailable for epoch {} (likely outside RPC cache window)",
                    epoch
                ));
            }
        };

        let start_slot = self.epoch_to_slot(epoch);
        let end_slot = self.epoch_to_slot(epoch + 1).saturating_sub(1);
        // Compact per-validator storage: use a small fixed-capacity stack array and spill to heap on demand
        let mut leader_schedule = Vec::new();
        let mut slot_leaders = HashMap::new();
        let mut leader_slots: HashMap<Pubkey, SmallSlots> = HashMap::new();

        for (leader_str, slots) in schedule_data {
            let leader = Pubkey::from_str(&leader_str)?;
            for &slot_index in &slots {
                let absolute_slot = start_slot + slot_index as u64;
                leader_schedule.push((absolute_slot, leader));
                slot_leaders.insert(absolute_slot, leader);
                leader_slots.entry(leader)
                    .or_insert_with(SmallSlots::new)
                    .push(absolute_slot);
            }
        }

        leader_schedule.sort_by_key(|(slot, _)| *slot);
        // Ensure per-validator slots are sorted for O(log n) lookups
        for slots in leader_slots.values_mut() { slots.sort_unstable(); }

        Ok(EpochSchedule {
            epoch,
            start_slot,
            end_slot,
            leader_schedule,
            slot_leaders,
            leader_slots,
            cached_at: Instant::now(),
        })
    }

    async fn update_validator_info(&self) -> Result<()> {
        let vote_accounts = self.rpc_client
            .get_vote_accounts_with_commitment(CommitmentConfig::finalized())
            .await?;

        let mut validator_map = HashMap::new();

        for vote_account in vote_accounts.current.into_iter().chain(vote_accounts.delinquent) {
            if vote_account.activated_stake < MIN_STAKE_LAMPORTS {
                continue;
            }

            let vote_pubkey = Pubkey::from_str(&vote_account.vote_pubkey)?;
            let node_pubkey = Pubkey::from_str(&vote_account.node_pubkey)?;

            // Merge with previous entry to keep history and compute trust
            let mut prev = self.validator_info.read().await.get(&node_pubkey).cloned();
            let mut stake_history = prev.as_ref().map(|p| p.stake_history.clone()).unwrap_or_default();
            if stake_history.len() >= 8 { let _ = stake_history.remove(0); }
            stake_history.push(vote_account.activated_stake);
            let prev_comm = prev.as_ref().and_then(|p| p.prev_commission.or(Some(p.commission)) ).unwrap_or(vote_account.commission);
            // expected/missed slots from previous snapshot if available (placeholder: keep as-is unless you have analytics feed)
            let expected_slots = prev.as_ref().and_then(|p| p.expected_slots_last_epoch).unwrap_or(0);
            let missed_slots = prev.as_ref().and_then(|p| p.missed_slots_last_epoch).unwrap_or(0);

            let trust = Some(compute_validator_trust(
                expected_slots,
                missed_slots,
                prev_comm,
                vote_account.commission,
                stake_history.as_slice(),
            ));

            validator_map.insert(
                node_pubkey,
                ValidatorInfo {
                    pubkey: node_pubkey,
                    stake: vote_account.activated_stake,
                    commission: vote_account.commission,
                    last_vote: vote_account.last_vote,
                    vote_account: vote_pubkey,
                    trust_profile: trust,
                    prev_commission: Some(vote_account.commission),
                    stake_history,
                    expected_slots_last_epoch: prev.and_then(|p| p.expected_slots_last_epoch),
                    missed_slots_last_epoch: prev.and_then(|p| p.missed_slots_last_epoch),
                },
            );
        }

        // Swap in new map
        *self.validator_info.write().await = validator_map;

        // Build EWMA trust snapshots per validator and update profiler
        let epoch_now = self.current_epoch.load(AtomicOrdering::Acquire);
        let validators = self.validator_info.read().await;
        let current_slot = self.current_slot.load(AtomicOrdering::Acquire);
        let mut snapshots: Vec<(Pubkey, TrustSnapshot)> = Vec::with_capacity(validators.len());
        for (pk, vi) in validators.iter() {
            let last_vote_lag = vi.last_vote.map(|lv| current_slot.saturating_sub(lv)).unwrap_or(u64::MAX);
            let snap = TrustSnapshot {
                epoch: epoch_now,
                expected_slots: vi.expected_slots_last_epoch.unwrap_or(0),
                missed_slots: vi.missed_slots_last_epoch.unwrap_or(0),
                commission: vi.commission,
                stake: vi.stake,
                vote_lag_p95: last_vote_lag, // approximation; replace with real p95 when available
            };
            snapshots.push((*pk, snap));
        }
        drop(validators);
        self.trust_profiler.update_epoch(epoch_now, snapshots);
        Ok(())
    }

    pub async fn get_leader_at_slot(&self, slot: Slot) -> Result<LeaderInfo> {
        let epoch = self.slot_to_epoch(slot);
        // L2 persisted cache first (if enabled)
        if let Some(hc) = &self.hier_cache {
            if let Some(li) = hc.get(slot) { return Ok(li); }
        }
        // Fast path: use cached schedule if present
        if let Some(schedule) = self.epoch_schedules.get(&epoch) {
            if let Some(&leader) = schedule.slot_leaders.get(&slot) {
                let ep_hash = self.hash_endpoint();
                counter!("leader.at_slot.cache_hit", 1, "epoch" => epoch.to_string(), "ep" => format!("{}", ep_hash));
                // Update LRU by touching cached_at
                schedule.cached_at.elapsed(); // no-op to read; real LRU handled in cleanup using cached_at
                let validators = self.validator_info.read().await;
                let vi = validators.get(&leader);
                let li = LeaderInfo { pubkey: leader, slot, epoch, stake: vi.map(|v| v.stake).unwrap_or(0), commission: vi.map(|v| v.commission).unwrap_or(100), last_vote: vi.and_then(|v| v.last_vote) };
                if let Some(hc) = &self.hier_cache { hc.put(&li); }
                return Ok(li);
            } else {
                let ep_hash = self.hash_endpoint();
                counter!("leader.at_slot.cache_miss", 1, "epoch" => epoch.to_string(), "ep" => format!("{}", ep_hash));
            }
        }
        // Single-pass refresh
        // Ensure current epoch fetch does not block behind prefetches
        let _permit = self.epoch_sems.acquire_current().await;
        self.fetch_and_cache_epoch_schedule(epoch).await?;
        if let Some(schedule) = self.epoch_schedules.get(&epoch) {
            if let Some(&leader) = schedule.slot_leaders.get(&slot) {
                let validators = self.validator_info.read().await;
                let vi = validators.get(&leader);
                let li = LeaderInfo { pubkey: leader, slot, epoch, stake: vi.map(|v| v.stake).unwrap_or(0), commission: vi.map(|v| v.commission).unwrap_or(100), last_vote: vi.and_then(|v| v.last_vote) };
                if let Some(hc) = &self.hier_cache { hc.put(&li); }
                return Ok(li);
            }
        }
        Err(anyhow!("No leader for slot {} (epoch {})", slot, epoch))
    }

    pub async fn get_upcoming_leaders(&self, num_slots: u64) -> Result<Vec<LeaderInfo>> {
        use futures::{stream::FuturesUnordered, StreamExt};
        use tokio::sync::Semaphore;

        let capped = num_slots.min(8192);
        let current_slot = self.current_slot.load(AtomicOrdering::Acquire);
        let target_slots: Vec<Slot> = (0..capped).map(|o| current_slot + o).collect();

        // Phase 1: fast cache reads
        let mut results: BTreeMap<Slot, LeaderInfo> = BTreeMap::new();
        let mut misses: Vec<Slot> = Vec::new();
        {
            let cache = self.leader_schedule_cache.read().await;
            for &s in &target_slots {
                if let Some(li) = cache.get(&s) {
                    results.insert(s, li.clone());
                } else {
                    misses.push(s);
                }
            }
        }

        // Phase 2: bounded-concurrency fetch of misses
        if !misses.is_empty() {
            let max_in_flight = 16usize; // tune per RPC budget
            let sem = Arc::new(Semaphore::new(max_in_flight));
            let mut futs: FuturesUnordered<_> = FuturesUnordered::new();
            for s in misses {
                let sem_c = sem.clone();
                let fut = async move {
                    let _permit = sem_c.acquire_owned().await.map_err(|_| anyhow!("semaphore closed"))?;
                    self.get_leader_at_slot(s).await.map(|li| (s, li))
                };
                futs.push(fut);
            }
            while let Some(res) = futs.next().await {
                let (s, li) = res?;
                // Write-through cache with minimal contention
                {
                    let mut cache = self.leader_schedule_cache.write().await;
                    cache.insert(s, li.clone());
                }
                results.insert(s, li);
            }
        }

        // Phase 3: prune cache opportunistically
        {
            let cutoff = current_slot.saturating_sub(100);
            let mut cache = self.leader_schedule_cache.write().await;
            cache.retain(|&slot, _| slot >= cutoff);
        }

        Ok(target_slots.into_iter().filter_map(|s| results.remove(&s)).collect())
    }

    pub async fn get_leader_slots_in_range(
        &self,
        leader: &Pubkey,
        start_slot: Slot,
        end_slot: Slot,
    ) -> Result<Vec<Slot>> {
        let start_epoch = self.slot_to_epoch(start_slot);
        let end_epoch = self.slot_to_epoch(end_slot);
        let mut leader_slots = Vec::new();

        for epoch in start_epoch..=end_epoch {
            if !self.epoch_schedules.contains_key(&epoch) {
                self.fetch_and_cache_epoch_schedule(epoch).await?;
            }

            if let Some(schedule) = self.epoch_schedules.get(&epoch) {
                if let Some(slots) = schedule.leader_slots.get(leader) {
                    for &slot in slots {
                        if slot >= start_slot && slot <= end_slot {
                            leader_slots.push(slot);
                        }
                    }
                }
            }
        }

        leader_slots.sort_unstable();
        Ok(leader_slots)
    }

    pub async fn get_next_leader_slot(&self, leader: &Pubkey) -> Result<Option<Slot>> {
        let current_slot = self.current_slot.load(AtomicOrdering::Acquire);
        let current_epoch = self.current_epoch.load(AtomicOrdering::Acquire);

        for epoch in current_epoch..=current_epoch + 2 {
            if !self.epoch_schedules.contains_key(&epoch) {
                self.fetch_and_cache_epoch_schedule(epoch).await?;
            }

            if let Some(schedule) = self.epoch_schedules.get(&epoch) {
                if let Some(slots) = schedule.leader_slots.get(leader) {
                    if let Some(next) = slots.next_after(current_slot) { return Ok(Some(next)); }
                }
            }
        }

        Ok(None)
    }

    pub async fn get_leader_performance_stats(&self, leader: &Pubkey) -> Result<LeaderStats> {
        let validators = self.validator_info.read().await;
        let validator_info = validators.get(leader)
            .ok_or_else(|| anyhow!("Validator info not found for {}", leader))?;

        let current_slot = self.current_slot.load(AtomicOrdering::Acquire);
        let current_epoch = self.current_epoch.load(AtomicOrdering::Acquire);
        // Compute performance metrics
        let mut total_slots = 0u64;
        let mut upcoming_slots = 0u64;
        let mut recent_slots = Vec::new();

        for epoch in current_epoch.saturating_sub(1)..=current_epoch + 1 {
            if let Some(schedule) = self.epoch_schedules.get(&epoch) {
                if let Some(slots) = schedule.leader_slots.get(leader) {
                    total_slots += slots.len() as u64;
                    // Count upcoming slots
                    for &slot in slots {
                        match slot.cmp(&current_slot) {
                            Ordering::Greater => upcoming_slots += 1,
                            Ordering::Equal | Ordering::Less => {
                                if current_slot.saturating_sub(slot) <= 1000 {
                                    recent_slots.push(slot);
                                }
                            }
                        }
                    }
                }
            }
        }

        let vote_lag = validator_info.last_vote
            .map(|last_vote| current_slot.saturating_sub(last_vote))
            .unwrap_or(u64::MAX);

        Ok(LeaderStats {
            pubkey: *leader,
            stake: validator_info.stake,
            commission: validator_info.commission,
            last_vote: validator_info.last_vote,
            vote_lag,
            total_slots,
            upcoming_slots,
            recent_slots,
            performance_score: calculate_performance_score(validator_info.stake, vote_lag, validator_info.commission),
        })
    }

    pub async fn predict_optimal_leaders(&self, num_slots: u64) -> Result<Vec<OptimalLeader>> {
        let upcoming_leaders = self.get_upcoming_leaders(num_slots).await?;
        let validators = self.validator_info.read().await;
        let current_slot = self.current_slot.load(AtomicOrdering::Acquire);
        
        let mut optimal_leaders = Vec::new();
        let mut seen_leaders = HashSet::new();

        for leader_info in upcoming_leaders {
            if !seen_leaders.insert(leader_info.pubkey) {
                continue;
            }

            let validator_info = validators.get(&leader_info.pubkey);
            let vote_lag = validator_info
                .and_then(|v| v.last_vote)
                .map(|last_vote| current_slot.saturating_sub(last_vote))
                .unwrap_or(u64::MAX);

            let reliability_score = calculate_reliability_score(
                leader_info.stake,
                vote_lag,
                leader_info.commission,
            );

            let mev_score = calculate_mev_score(
                leader_info.stake,
                leader_info.commission,
                vote_lag,
            );

            // RL contextual bandit weighting
            let trust = self.trust_profiler.trust_score(&leader_info.pubkey);
            let conf = {
                // approximate confidence cheaply: map vote lag to [0,1]
                let lag_norm = (vote_lag as f64 / 256.0).clamp(0.0, 2.0);
                (1.0 - lag_norm).clamp(0.0, 1.0)
            };
            let x = ContextFeatures { reliability: reliability_score, mev: mev_score, trust, confidence: conf };
            let bandit_score = {
                let mut cb = self.cb_opt.lock();
                let (s, _explore) = cb.choose(&leader_info.pubkey, &x);
                s
            };
            // Tiny MLP nonlinear EV fusion
            let trust_v = self.trust_profiler.trust_score(&leader_info.pubkey).clamp(0.0, 1.0) as f32;
            let conf_v = (1.0 - (vote_lag as f64 / 256.0)).clamp(0.0, 1.0) as f32;
            let mlp_score = {
                let mlp = self.mlp.lock();
                mlp.infer([reliability_score as f32, mev_score as f32, trust_v, conf_v]) as f64
            };
            let mut combined_score = mlp_score;
            if let Some(relay) = &self.relay_obs {
                let wp = slot_win_probability(&self.winprob_cfg, relay.as_ref(), &leader_info.pubkey);
                // fold in MEV profitability (if available) and confidence approximation (from vote lag)
                let profit_w = if let Some(w) = &self.mev_weighter { w.slot_profitability(&leader_info.pubkey, leader_info.slot) } else { 1.0 };
                let conf = (1.0 - (vote_lag as f64 / 256.0)).clamp(0.0, 1.0);
                combined_score *= (wp * profit_w * conf).clamp(0.2, 1.5);
            }
            // Multi-epoch stability weight
            let stability_w = self.entropy.lock().stability_weight(&leader_info.pubkey);
            combined_score *= stability_w.clamp(0.5, 1.0);

            optimal_leaders.push(OptimalLeader {
                pubkey: leader_info.pubkey,
                next_slot: leader_info.slot,
                stake: leader_info.stake,
                commission: leader_info.commission,
                vote_lag,
                reliability_score,
                mev_score,
                combined_score,
            });
        }

        optimal_leaders.sort_by(|a, b| {
            b.combined_score.partial_cmp(&a.combined_score)
                .unwrap_or(Ordering::Equal)
        });

        Ok(optimal_leaders)
    }

    pub async fn get_epoch_transition_info(&self) -> Result<EpochTransitionInfo> {
        let current_slot = self.current_slot.load(AtomicOrdering::Acquire);
        let current_epoch = self.current_epoch.load(AtomicOrdering::Acquire);
        let next_epoch = current_epoch + 1;
        let next_epoch_start = self.epoch_to_slot(next_epoch);
        let slots_until_transition = next_epoch_start.saturating_sub(current_slot);

        let current_schedule = self.epoch_schedules.get(&current_epoch)
            .ok_or_else(|| anyhow!("Current epoch schedule not found"))?;
        // Fetch next epoch schedule if not cached
        let mut next_schedule = None;
        if slots_until_transition <= PREFETCH_THRESHOLD_SLOTS {
            if let Some(schedule) = self.epoch_schedules.get(&next_epoch) {
                next_schedule = Some(schedule.clone());
            } else {
                let schedule = self.fetch_and_cache_epoch_schedule(next_epoch).await?;
                next_schedule = Some(schedule);
            }
        }

        Ok(EpochTransitionInfo {
            current_epoch,
            next_epoch,
            current_slot,
            next_epoch_start_slot: next_epoch_start,
            slots_until_transition,
            current_epoch_leaders: current_schedule.leader_schedule.len(),
            next_epoch_leaders: next_schedule.as_ref().map(|s| s.leader_schedule.len()),
            transition_ready: next_schedule.is_some(),
        })
    }

    fn slot_to_epoch(&self, slot: Slot) -> Epoch {
        if slot < self.first_normal_slot {
            slot / DEFAULT_SLOTS_PER_EPOCH
        } else {
            ((slot - self.first_normal_slot) / self.slots_per_epoch) + self.first_normal_epoch
        }
    }

    fn epoch_to_slot(&self, epoch: Epoch) -> Slot {
        if epoch <= self.first_normal_epoch {
            epoch * DEFAULT_SLOTS_PER_EPOCH
        } else {
            (epoch - self.first_normal_epoch) * self.slots_per_epoch + self.first_normal_slot
        }
    }

    fn cleanup_old_epochs(&self, current_epoch: Epoch) {
        // Enforce LRU-like eviction based on cached_at timestamps
        let mut entries: Vec<(Epoch, Instant)> = self.epoch_schedules.iter().map(|e| (*e.key(), e.value().cached_at)).collect();
        // Keep current and next regardless
        let protect1 = current_epoch;
        let protect2 = current_epoch + 1;
        // Sort by last cached_at ascending (oldest first)
        entries.sort_by_key(|(_, t)| *t);
        while self.epoch_schedules.len() > MAX_CACHED_EPOCHS {
            if let Some((e, _)) = entries.first().copied() {
                if e == protect1 || e == protect2 { entries.remove(0); continue; }
                let _ = self.epoch_schedules.remove(&e);
                entries.remove(0);
            } else { break; }
        }
    }

    pub async fn is_leader_reliable(&self, leader: &Pubkey, threshold: f64) -> Result<bool> {
        let stats = self.get_leader_performance_stats(leader).await?;
        Ok(stats.performance_score >= threshold)
    }

    pub async fn get_current_leader(&self) -> Result<LeaderInfo> {
        let predicted = self.predicted_current_slot();
        // try predicted slot first to cut latency; fall back to last RPC slot
        if let Ok(li) = self.get_leader_at_slot(predicted).await { return Ok(li); }
        let current_slot = self.current_slot.load(AtomicOrdering::Acquire);
        self.get_leader_at_slot(current_slot).await
    }

    pub fn predicted_current_slot(&self) -> Slot {
        let mut s = self.slot_estimator.predicted_slot();
        if let Some(stale) = &self.latency_staleness {
            if let Some(age) = stale.recent_blockhash_age_ms() { if age > 1000 { s = s.saturating_add(2); } }
        }
        s
    }

    pub fn enable_hier_cache(&mut self, l1_capacity: u64, path: &str) -> Result<()> { self.hier_cache = Some(HierCache::new(l1_capacity, path)?); Ok(()) }
    pub fn set_relay_observations(&mut self, relay: Arc<dyn RelayObservations + Send + Sync>) { self.relay_obs = Some(relay); }
    pub fn set_hash_staleness(&mut self, staleness: Arc<dyn HashStaleness + Send + Sync>) { self.latency_staleness = Some(staleness); }
    pub fn set_hash_freshness(&mut self, hash: Arc<dyn HashFreshness + Send + Sync>) { self.hash_freshness = Some(hash); }

    pub fn get_cached_epochs(&self) -> Vec<Epoch> {
        self.epoch_schedules.iter()
            .map(|entry| *entry.key())
            .collect()
    }

    pub async fn force_refresh_epoch(&self, epoch: Epoch) -> Result<()> {
        self.fetch_and_cache_epoch_schedule(epoch).await?;
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct LeaderStats {
    pub pubkey: Pubkey,
    pub stake: u64,
    pub commission: u8,
    pub last_vote: Option<Slot>,
    pub vote_lag: u64,
    pub total_slots: u64,
    pub upcoming_slots: u64,
    pub recent_slots: Vec<Slot>,
    pub performance_score: f64,
}

#[derive(Debug, Clone)]
pub struct OptimalLeader {
    pub pubkey: Pubkey,
    pub next_slot: Slot,
    pub stake: u64,
    pub commission: u8,
    pub vote_lag: u64,
    pub reliability_score: f64,
    pub mev_score: f64,
    pub combined_score: f64,
}

#[derive(Debug, Clone)]
pub struct EpochTransitionInfo {
    pub current_epoch: Epoch,
    pub next_epoch: Epoch,
    pub current_slot: Slot,
    pub next_epoch_start_slot: Slot,
    pub slots_until_transition: u64,
    pub current_epoch_leaders: usize,
    pub next_epoch_leaders: Option<usize>,
    pub transition_ready: bool,
}

fn calculate_performance_score(stake: u64, vote_lag: u64, commission: u8) -> f64 {
    let stake_score = (stake as f64 / 1_000_000_000_000.0).min(100.0) / 100.0;
    let lag_score = (1.0 / (1.0 + (vote_lag as f64 / 150.0))).max(0.0);
    let commission_score = 1.0 - (commission as f64 / 100.0);
    
    stake_score * 0.4 + lag_score * 0.5 + commission_score * 0.1
}

fn calculate_reliability_score(stake: u64, vote_lag: u64, commission: u8) -> f64 {
    let base_score = calculate_performance_score(stake, vote_lag, commission);
    let lag_penalty = if vote_lag > 150 { 0.5 } else { 1.0 };
    base_score * lag_penalty
}

fn calculate_mev_score(stake: u64, commission: u8, vote_lag: u64) -> f64 {
    let stake_normalized = (stake as f64 / 1_000_000_000_000.0).min(100.0) / 100.0;
    let commission_factor = 1.0 - (commission as f64 / 100.0);
    let lag_factor = if vote_lag < 50 { 1.0 } else if vote_lag < 150 { 0.8 } else { 0.5 };
    
        (stake_normalized * 0.3 + commission_factor * 0.4 + lag_factor * 0.3).max(0.0).min(1.0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use solana_sdk::signature::Keypair;

    #[tokio::test]
    async fn test_epoch_calculations() {
        let rpc_client = Arc::new(RpcClient::new("https://api.mainnet-beta.solana.com".to_string()));
        let predictor = LeaderSchedulePredictor::new(rpc_client).await.unwrap();
        
        let slot = 432000;
        let epoch = predictor.slot_to_epoch(slot);
        let epoch_start = predictor.epoch_to_slot(epoch);
        
        assert!(epoch_start <= slot);
        assert!(predictor.epoch_to_slot(epoch + 1) > slot);
    }

    #[test]
    fn test_performance_calculations() {
        let stake = 10_000_000_000_000;
        let vote_lag = 50;
        let commission = 5;
        
        let perf_score = calculate_performance_score(stake, vote_lag, commission);
        assert!(perf_score > 0.0 && perf_score <= 1.0);
        
        let reliability_score = calculate_reliability_score(stake, vote_lag, commission);
        assert!(reliability_score > 0.0 && reliability_score <= 1.0);
        
        let mev_score = calculate_mev_score(stake, commission, vote_lag);
        assert!(mev_score > 0.0 && mev_score <= 1.0);
    }
}

pub struct LeaderSchedulePredictorBuilder {
    rpc_endpoint: String,
    commitment: CommitmentConfig,
}

impl LeaderSchedulePredictorBuilder {
    pub fn new() -> Self {
        Self {
            rpc_endpoint: "https://api.mainnet-beta.solana.com".to_string(),
            commitment: CommitmentConfig::confirmed(),
        }
    }

    pub fn with_rpc_endpoint(mut self, endpoint: String) -> Self {
        self.rpc_endpoint = endpoint;
        self
    }

    pub fn with_commitment(mut self, commitment: CommitmentConfig) -> Self {
        self.commitment = commitment;
        self
    }

    pub async fn build(self) -> Result<Arc<LeaderSchedulePredictor>> {
        let endpoint = self.rpc_endpoint.clone();
        let rpc_client = Arc::new(RpcClient::new_with_commitment(endpoint.clone(), self.commitment));
        let predictor = Arc::new(LeaderSchedulePredictor::new_with_endpoint(rpc_client, endpoint, None).await?);
        predictor.clone().start_background_updater().await;
        
        Ok(predictor)
    }
}

impl Default for LeaderSchedulePredictorBuilder {
    fn default() -> Self {
        Self::new()
    }
}

