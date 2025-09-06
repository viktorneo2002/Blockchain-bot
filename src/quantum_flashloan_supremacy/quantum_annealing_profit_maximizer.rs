// Adaptive LRU energy cache keyed by u128 bitset with hit-rate-based resizing and drift invalidation
struct EnergyCache {
    lru: RwLock<LruCache<u128, f64>>,
    cap: RwLock<usize>,
    hits: RwLock<u64>,
    misses: RwLock<u64>,
    last_rebalance: RwLock<Instant>,
    rebalance_every: Duration,
    min_cap: usize,
    max_cap: usize,
    last_h: RwLock<Vec<f64>>, // last local fields snapshot
    drift_threshold_l1: f64,
}

// Core quantum annealing scaffolding (bit-packed state)
#[derive(Clone)]
struct QaCore<'a> {
    n: usize,
    J: &'a [Vec<f64>],      // row-major
    h: &'a [f64],
    max_bundle: usize,
    opps: &'a [MEVOpportunity],
    id_to_idx: &'a HashMap<u64, usize>,
}

#[inline]
fn hard_constraints_ok(bits: u128, max_bundle: usize, opps: &[MEVOpportunity], map: &HashMap<u64, usize>) -> bool {
    if (bits.count_ones() as usize) > max_bundle { return false; }
    for (i, opp) in opps.iter().enumerate() {
        if ((bits >> i) & 1) == 1 {
            for dep in &opp.dependencies {
                if let Some(&d) = map.get(dep) { if ((bits >> d) & 1) == 0 { return false; } }
            }
        }
    }
    true
}

// ΔE = 2 s_i (h_i + Σ_j J[i][j] s_j) where neigh_sum[i] = Σ_j J[i][j] s_j
#[inline]
fn delta_energy_o1(i: usize, spins: u128, h: &[f64], neigh_sum: &[f64]) -> f64 {
    let s_i = if ((spins >> i) & 1) == 1 { 1.0 } else { -1.0 };
    2.0 * s_i * (h[i] + neigh_sum[i])
}

#[inline]
fn update_neigh_sum_after_flip(i: usize, spins_after: u128, neigh_sum: &mut [f64], J: &[Vec<f64>]) {
    let s_i_after = if ((spins_after >> i) & 1) == 1 { 1.0 } else { -1.0 };
    let s_i_before = -s_i_after;
    let delta = 2.0 * s_i_before;
    for k in 0..neigh_sum.len() { neigh_sum[k] += delta * J[k][i]; }
}

#[inline]
fn local_coupling(J: &[Vec<f64>], i: usize) -> f64 {
    if J.is_empty() { return 0.0; }
    let row = &J[i];
    let sum: f64 = row.iter().map(|x| x.abs()).sum();
    sum / (row.len().max(1) as f64)
}

#[inline]
fn adaptive_tunneling_prob_core(T: f64, iter: usize, stats: &AnnealStats, i: usize, J: &[Vec<f64>], pol: &TunnelingPolicy, m: f64) -> f64 {
    let stagn = iter.saturating_sub(stats.last_improve_iter);
    let stagn_term = (stagn as f64 / 1000.0) * pol.stagnation_boost;
    let coup_term = local_coupling(J, i) * pol.coupling_boost;
    let temp_term = (T / (T + 1.0)).min(0.15);
    let align_term = 0.5 + 0.5 * (1.0 - m.abs());
    (pol.base + stagn_term + coup_term + temp_term).min(pol.max) * align_term
}

// Main QA loop: SA + tunneling; O(1) ΔE; periodic transverse kicks bounded by budget.
fn anneal_core(
    core: &QaCore,
    temps: &[f64],
    init_spins: u128,
    initial_energy: f64,
    neigh_sum: &mut [f64],
    pol: &TunnelingPolicy,
    guard: &BudgetGuard,
    rng: &mut ChaCha20Rng,
) -> QaState {
    let mut state = QaState { spins: init_spins, n: core.n, energy: initial_energy, magnetization: magnetization_from_bits(init_spins, core.n), bundle_bits: init_spins };
    let mut stats = AnnealStats { last_improve_iter: 0, best_energy: state.energy };
    let start = Instant::now();
    let mut flips = 0usize;
    for (iter, &T) in temps.iter().enumerate() {
        if !guard.within(start, flips) { break; }
        // One sweep
        for _ in 0..core.n {
            if !guard.within(start, flips) { break; }
            let i = (rng.next_u64() as usize) % core.n;
            // Classical SA acceptance
            let dE = delta_energy_o1(i, state.spins, core.h, neigh_sum);
            let accept_classical = dE < 0.0 || rng.gen::<f64>() < (-dE / T).exp();
            // Quantum tunneling acceptance
            let p_q = adaptive_tunneling_prob_core(T, iter, &stats, i, core.J, pol, state.magnetization);
            let accept_quantum = rng.gen::<f64>() < p_q;
            if accept_classical || accept_quantum {
                let new_spins = state.spins ^ (1u128 << i);
                if hard_constraints_ok(new_spins, core.max_bundle, core.opps, core.id_to_idx) {
                    state.spins = new_spins;
                    state.energy += dE;
                    update_neigh_sum_after_flip(i, state.spins, neigh_sum, core.J);
                    state.magnetization = magnetization_from_bits(state.spins, state.n);
                    state.bundle_bits = state.spins;
                    flips = flips.saturating_add(1);
                    if state.energy < stats.best_energy { stats.best_energy = state.energy; stats.last_improve_iter = iter; }
                }
            }
        }
        // Periodic transverse kicks (every ~96 iters), bounded by time budget
        if iter % 96 == 0 {
            let max_kicks = (core.n.max(8) / 8).clamp(1, 8);
            for _ in 0..max_kicks {
                if !guard.within(start, flips) { break; }
                let i = (rng.next_u64() as usize) % core.n;
                let p_q = adaptive_tunneling_prob_core(T, iter, &stats, i, core.J, pol, state.magnetization);
                if rng.gen::<f64>() < p_q {
                    let new_spins = state.spins ^ (1u128 << i);
                    if hard_constraints_ok(new_spins, core.max_bundle, core.opps, core.id_to_idx) {
                        let dE = delta_energy_o1(i, state.spins, core.h, neigh_sum);
                        state.spins = new_spins;
                        state.energy += dE;
                        update_neigh_sum_after_flip(i, state.spins, neigh_sum, core.J);
                        state.magnetization = magnetization_from_bits(state.spins, state.n);
                        state.bundle_bits = state.spins;
                        flips = flips.saturating_add(1);
                        if state.energy < stats.best_energy { stats.best_energy = state.energy; stats.last_improve_iter = iter; }
                    }
                }
            }
        }
    }
    state
}

// Aggressive blockhash refresh cache with TTL and safe fallback
struct BlockhashCache {
    hash: parking_lot::RwLock<(Hash, Instant)>,
    ttl: Duration,
}

impl BlockhashCache {
    fn new(ttl: Duration) -> Self {
        Self {
            hash: parking_lot::RwLock::new((Hash::default(), Instant::now() - ttl)),
            ttl,
        }
    }

    #[inline]
    fn get(&self, rpc: &RpcClient) -> Hash {
        let mut g = self.hash.write();
        if g.1.elapsed() > self.ttl {
            if let Ok(h) = rpc.get_latest_blockhash() {
                *g = (h, Instant::now());
            } else {
                // keep existing cached hash if RPC fails; caller may retry on send
            }
        }
        g.0
    }
}

// Split a too-large instruction set across multiple transactions by simulated CU usage
fn split_instructions_by_cu(
    rpc: &RpcClient,
    payer: &Keypair,
    ixs: Vec<Instruction>,
    max_cu: u32,
    safety: f64,
) -> Vec<Vec<Instruction>> {
    let mut batches: Vec<Vec<Instruction>> = Vec::new();
    let mut cur: Vec<Instruction> = Vec::new();
    let mut cur_cu: u64 = 0;
    for ix in ixs {
        let bh = rpc.get_latest_blockhash().unwrap_or_default();
        let tx = Transaction::new_signed_with_payer(&[ix.clone()], Some(&payer.pubkey()), &[payer], bh);
        let used = rpc.simulate_transaction(&tx).ok().and_then(|s| s.value.units_consumed).unwrap_or(100_000) as f64;
        let cu_need = (used * safety).ceil() as u64;
        if cur_cu + cu_need > max_cu as u64 && !cur.is_empty() {
            batches.push(std::mem::take(&mut cur));
            cur_cu = 0;
        }
        cur.push(ix);
        cur_cu = cur_cu.saturating_add(cu_need);
    }
    if !cur.is_empty() { batches.push(cur); }
    batches
}

// Relay pool with variance clipping and normalization
struct RelayPool {
    relays: Vec<Relay>,
    min_w: f64,
    max_w: f64,
    max_ratio: f64, // prevent one relay from dominating, e.g., 0.6
}

impl RelayPool {
    fn normalize(&mut self) {
        for r in &mut self.relays { r.weight = r.weight.clamp(self.min_w, self.max_w); }
        let mut w: Vec<f64> = {
            let sum: f64 = self.relays.iter().map(|r| r.weight).sum::<f64>().max(1e-9);
            self.relays.iter().map(|r| r.weight / sum).collect()
        };
        let mut changed = true;
        while changed {
            changed = false;
            let mut excess = 0.0;
            for i in 0..w.len() {
                if w[i] > self.max_ratio { excess += w[i] - self.max_ratio; w[i] = self.max_ratio; changed = true; }
            }
            if changed {
                let remain: f64 = w.iter().sum();
                let scale = (1.0 - excess) / remain.max(1e-9);
                for i in 0..w.len() { w[i] *= scale; }
            }
        }
        // write back scaled weights (scaled to sum~100 for stability)
        for (i, r) in self.relays.iter_mut().enumerate() { r.weight = (w[i] * 100.0).max(self.min_w); }
    }
}

// Fee fusion: blend congestion proxy with percentile history
fn fused_cu_price(
    fee_window: &FeeWindow,
    recent_congestion: f64, // 0..1
    base_percentile: f64,   // 0.90..0.98
    default: u64,
    profit_cap_lamports: u64,
) -> u64 {
    let adj_pct = (base_percentile * (0.9 + 0.2*recent_congestion)).clamp(0.7, 0.99);
    let p = fee_window.percentile_value(adj_pct, default);
    let cap = (profit_cap_lamports as f64 * 0.06).round() as u64;
    p.clamp(1_000, cap.max(1_000))
}

// CMA-ES lite utilities for smarter parameter exploration (diagonal covariance)
struct CmaLite { sigma: f64, lr_cov: f64, cov: Vec<f64> }
impl CmaLite {
    fn new(dim: usize, sigma: f64, lr_cov: f64) -> Self { Self { sigma, lr_cov, cov: vec![1.0; dim] } }
    // Sample perturbation ~ N(0, sigma^2 * cov)
    fn sample(&self, rng: &mut ChaCha20Rng, out: &mut [f64]) {
        for (i, o) in out.iter_mut().enumerate() {
            let u1 = rng.gen::<f64>().clamp(1e-12, 1.0);
            let u2 = rng.gen::<f64>();
            let z = (-2.0 * u1.ln()).sqrt() * (2.0*std::f64::consts::PI*u2).cos();
            *o = z * self.sigma * self.cov[i].sqrt();
        }
    }
    // Update diagonal covariance from best-performing gradient-free signal
    fn update_cov(&mut self, delta: &[f64], reward: f64) {
        for (i, d) in delta.iter().enumerate() {
            let v = self.cov[i] + self.lr_cov * reward.signum() * d.abs();
            self.cov[i] = v.clamp(1e-4, 10.0);
        }
    }
}

fn perturb_local_fields_cma(fields: &mut [f64], cma: &mut CmaLite, rng: &mut ChaCha20Rng) -> Vec<f64> {
    let mut delta = vec![0.0; fields.len()];
    cma.sample(rng, &mut delta);
    for (f, d) in fields.iter_mut().zip(delta.iter()) { *f += *d; }
    delta
}


// Real-time write-set conflict detection in the hot loop
#[derive(Clone)]
struct IxMeta { writable: Vec<Pubkey> }

#[inline]
fn extract_writes(ix: &Instruction) -> IxMeta {
    IxMeta { writable: ix.accounts.iter().filter(|m| m.is_writable).map(|m| m.pubkey).collect() }
}

#[inline]
fn bundle_conflicts_fast(opps: &[MEVOpportunity], selected_bits: u128) -> bool {
    let mut seen: HashSet<Pubkey> = HashSet::with_capacity(64);
    for (i, opp) in opps.iter().enumerate() {
        if ((selected_bits >> i) & 1) == 0 { continue; }
        for ix in &opp.instructions {
            let meta = extract_writes(ix);
            for w in meta.writable { if !seen.insert(w) { return true; } }
        }
    }
    false
}

// Congestion-aware risk context and bundle evaluator (ROI-weighted)
struct RiskContext {
    congestion: f64,   // 0..1 from inclusion rate / fee window
    tip_eff_weight: f64,
}

fn evaluate_bundle_quality_roi(bundle: &[MEVOpportunity], risk: &RiskContext) -> i64 {
    let total_profit: i64 = bundle
        .iter()
        .map(|o| o.profit_estimate as i64 - (o.gas_estimate + o.priority_fee) as i64)
        .sum();
    let avg_success: f64 = if bundle.is_empty() {
        0.0
    } else {
        bundle.iter().map(|o| o.success_probability).sum::<f64>() / bundle.len() as f64
    };
    let tip_eff: f64 = if bundle.is_empty() {
        0.0
    } else {
        bundle
            .iter()
            .map(|o| (o.priority_fee as f64) / (o.gas_estimate.max(1) as f64))
            .sum::<f64>()
            / (bundle.len() as f64)
    };
    let congestion_penalty = 1.0 - 0.4 * risk.congestion.clamp(0.0, 1.0);
    let tip_bonus = (1.0 / (1.0 + tip_eff)).powf(risk.tip_eff_weight);
    ((total_profit as f64) * avg_success * congestion_penalty * tip_bonus) as i64
}

// Online logistic model for penalty scaling / acceptance adaptivity
#[derive(Clone)]
struct OnlineLogit { w: Vec<f64>, lr: f64, l2: f64 }
impl OnlineLogit {
    fn new(dim: usize, lr: f64, l2: f64) -> Self { Self { w: vec![0.0; dim], lr, l2 } }
    #[inline] fn sigmoid(z: f64) -> f64 { 1.0 / (1.0 + (-z).exp()) }
    fn predict(&self, x: &[f64]) -> f64 { let z: f64 = self.w.iter().zip(x).map(|(w,xi)| w*xi).sum(); Self::sigmoid(z) }
    fn update(&mut self, x: &[f64], y: f64) {
        let p = self.predict(x); let grad = p - y;
        for i in 0..self.w.len() { self.w[i] = self.w[i] * (1.0 - self.lr * self.l2) - self.lr * grad * x[i]; }
    }
}
#[inline]
fn penalty_scale_from_model(ol: &OnlineLogit, features: &[f64]) -> f64 {
    let p = ol.predict(features);
    (1.0 / p.max(0.05)).min(5.0)
}

// Soft-penalty context for model-guided scaling
#[derive(Clone, Copy, Debug)]
struct PenaltyCtx {
    pub current_slot: u64,
    pub slot_time_ms: u64,
    pub congestion: f64,    // 0..1
    pub cu_price_guess: u64,
    pub model_scale: f64,   // global multiplier for penalty
}

// Total Ising energy over bitset with soft penalties and caching
// E = -h·s - 1/2 s^T J s  (implemented as -h_i s_i - sum_{i<j} J_ij s_i s_j)
fn calculate_total_energy(
    spins: u128,
    n: usize,
    h: &[f64],
    J: &[Vec<f64>],
    opps: &[MEVOpportunity],
    cache: &EnergyCache,
) -> f64 {
    let key = bundle_key(spins);
    if let Some(e) = cache.get(key) { return e; }

    let mut e = 0.0f64;
    for i in 0..n {
        let s_i = if ((spins >> i) & 1) == 1 { 1.0 } else { -1.0 };
        e -= h.get(i).copied().unwrap_or(0.0) * s_i;
        for j in (i + 1)..n {
            let s_j = if ((spins >> j) & 1) == 1 { 1.0 } else { -1.0 };
            let Jij = J.get(i).and_then(|row| row.get(j)).copied().unwrap_or(0.0);
            e -= Jij * s_i * s_j;
        }
    }

    // Penalties: gas overflow and bundle size
    let mut total_gas: u64 = 0;
    let mut bundle_len: usize = 0;
    for i in 0..n {
        if ((spins >> i) & 1) == 1 {
            bundle_len = bundle_len.saturating_add(1);
            if let Some(opp) = opps.get(i) {
                total_gas = total_gas.saturating_add(opp.gas_estimate.saturating_add(opp.priority_fee));
            }
        }
    }
    if total_gas > GAS_WATERMARK {
        e += (total_gas - GAS_WATERMARK) as f64 / 1e6; // scale to keep magnitude reasonable
    }
    if bundle_len > MAX_BUNDLE_SIZE {
        e += (bundle_len - MAX_BUNDLE_SIZE) as f64 * 100.0;
    }

    cache.put(key, e);
    e
}

// Soft, model-guided penalty for a candidate flip in bitset representation
fn calculate_bundle_penalty_ctx(
    spins: u128,
    flip_idx: usize,
    n: usize,
    opps: &[MEVOpportunity],
    ctx: &PenaltyCtx,
    model: &OnlineLogit,
) -> f64 {
    // If bit is 0, flipping includes it; if 1, flipping removes it. We penalize only inclusion here.
    let will_include = ((spins >> flip_idx) & 1) == 0;
    if !will_include { return 0.0; }

    if let Some(opp) = opps.get(flip_idx) {
        // Feature engineering
        let slack_slots = opp.deadline_slot.saturating_sub(ctx.current_slot) as f64;
        let deadline_slack = 1.0 / (1.0 + slack_slots);
        let cu_price = (ctx.cu_price_guess as f64).max(1.0);
        let tip_per_cu = (opp.priority_fee as f64) / cu_price;
        let log_tip_per_cu = tip_per_cu.max(1e-6).ln();
        let log_cu = (opp.gas_estimate.max(1) as f64).ln();
        let net_profit = (opp.profit_estimate
            .saturating_sub(opp.gas_estimate.saturating_add(opp.priority_fee)) as f64) / 1e9;
        let dep_depth = opp.dependencies.len() as f64;

        // Fast write conflict proxy if we include this opp
        let mut conflicts = 0.0f64;
        use std::collections::HashSet;
        let mut writes: HashSet<Pubkey> = HashSet::with_capacity(64);
        for (i, o) in opps.iter().enumerate() {
            if i == flip_idx { continue; }
            if ((spins >> i) & 1) == 1 {
                for ix in &o.instructions {
                    for acc in &ix.accounts {
                        if acc.is_writable {
                            if !writes.insert(acc.pubkey) { conflicts += 1.0; }
                        }
                    }
                }
            }
        }
        // add candidate's writes
        for ix in &opp.instructions {
            for acc in &ix.accounts {
                if acc.is_writable {
                    if !writes.insert(acc.pubkey) { conflicts += 1.0; }
                }
            }
        }

        let x = [
            deadline_slack,
            log_tip_per_cu,
            log_cu,
            net_profit,
            conflicts,
            dep_depth,
            ctx.congestion,
        ];
        let p = model.predict(&x);
        let scale = (1.0 / p.max(0.05)).min(5.0) * ctx.model_scale;
        return 5.0 * scale;
    }
    0.0
}

// Latency-aware deadline guard model
struct LatencyModel { base_ms: u64, per_kb_ms: u64, rpc_variance_ms: u64 }
impl LatencyModel {
    fn estimate_ms(&self, tx_bytes: usize) -> u64 { self.base_ms + self.per_kb_ms * (((tx_bytes as u64) + 1023) / 1024) + self.rpc_variance_ms }
    fn guard_slots(&self, estimate_ms: u64, slot_time_ms: u64) -> u64 { ((estimate_ms as f64 / slot_time_ms as f64).ceil() as u64).clamp(1, 4) }
}
fn filter_by_deadline(opps: Vec<MEVOpportunity>, current_slot: u64, tx_size_bytes: usize, slot_time_ms: u64, lm: &LatencyModel) -> Vec<MEVOpportunity> {
    let est = lm.estimate_ms(tx_size_bytes); let guard = lm.guard_slots(est, slot_time_ms);
    opps.into_iter().filter(|o| o.deadline_slot.saturating_sub(current_slot) >= guard).collect()
}

// Adaptive tunneling policy
struct TunnelingPolicy { base: f64, max: f64, stagnation_boost: f64, coupling_boost: f64 }
struct AnnealStats { last_improve_iter: usize, best_energy: f64 }
#[inline] fn local_coupling_strength(J: &[Vec<f64>], i: usize) -> f64 { if J.is_empty(){return 0.0;} let mut s=0.0; for &v in &J[i]{ s+=v.abs(); } s / (J[i].len() as f64) }
fn adaptive_tunneling_prob(base_temp: f64, iter: usize, stats: &AnnealStats, i: usize, J: &[Vec<f64>], pol: &TunnelingPolicy) -> f64 {
    let stagnation_iters = iter.saturating_sub(stats.last_improve_iter); let stagnation_term = (stagnation_iters as f64 / 1000.0) * pol.stagnation_boost; let coupling_term = local_coupling_strength(J, i) * pol.coupling_boost; let temp_term = (base_temp / (base_temp + 1.0)).min(0.15); (pol.base + stagnation_term + coupling_term + temp_term).min(pol.max)
}

impl EnergyCache {
    fn new(initial: usize, min_cap: usize, max_cap: usize, rebalance_every: Duration, initial_h: Vec<f64>, drift_threshold_l1: f64) -> Self {
        Self {
            // NonZeroUsize::new is safe after max(1)
            lru: RwLock::new(LruCache::new(NonZeroUsize::new(initial.max(1)).unwrap())),
            cap: RwLock::new(initial.max(1)),
            hits: RwLock::new(0),
            misses: RwLock::new(0),
            last_rebalance: RwLock::new(Instant::now()),
            rebalance_every,
            min_cap: min_cap.max(1),
            max_cap: max_cap.max(min_cap.max(1)),
            last_h: RwLock::new(initial_h),
            drift_threshold_l1,
        }
    }

    #[inline]
    fn get(&self, k: u128) -> Option<f64> {
        let mut l = self.lru.write();
        let r = l.get(&k).copied();
        if r.is_some() { *self.hits.write() = self.hits.read().saturating_add(1); }
        else { *self.misses.write() = self.misses.read().saturating_add(1); }
        r
    }

    #[inline]
    fn put(&self, k: u128, v: f64) {
        self.lru.write().put(k, v);
    }

    #[inline]
    fn clear(&self) {
        self.lru.write().clear();
        *self.hits.write() = 0;
        *self.misses.write() = 0;
    }

    #[inline]
    fn len(&self) -> usize { self.lru.read().len() }

    fn invalidate_if_drift(&self, new_h: &[f64]) {
        let mut last = self.last_h.write();
        if last.len() == new_h.len() {
            let l1: f64 = last.iter().zip(new_h.iter()).map(|(a, b)| (a - b).abs()).sum();
            if l1 > self.drift_threshold_l1 {
                self.lru.write().clear();
                *last = new_h.to_vec();
                *self.hits.write() = 0;
                *self.misses.write() = 0;
            }
        } else {
            // Dimensions changed; safest to invalidate
            self.lru.write().clear();
            *last = new_h.to_vec();
            *self.hits.write() = 0;
            *self.misses.write() = 0;
        }
    }

    #[inline]
    fn hit_rate(&self) -> f64 {
        let h = *self.hits.read();
        let m = *self.misses.read();
        if h + m == 0 { 0.0 } else { h as f64 / (h + m) as f64 }
    }

    fn maybe_rebalance(&self) {
        use std::time::Instant;
        let now = Instant::now();
    
        // Check if it's time to rebalance
        let mut last = self.last_rebalance.write();
        if now.duration_since(*last) < self.rebalance_every {
            return;
        }
    
        // Fetch current hit/miss stats
        let hits = *self.hits.read();
        let misses = *self.misses.read();
        let total = hits + misses;
        if total < 128 {
            // Not enough samples to make a decision
            *last = now;
            return;
        }
    
        let hit_rate = hits as f64 / total as f64;
        let volatility = ((hits as f64 - misses as f64).abs() / total as f64).clamp(0.0, 1.0);
    
        // Read current capacity and cache
        let mut cap = self.cap.write();
        let mut lru = self.lru.write();
    
        // Compute saturation ratio
        let saturation = lru.len() as f64 / (*cap as f64).max(1.0);
    
        // Adaptive scaling factor
        let mut scale = 1.0;
        if hit_rate < 0.25 && saturation > 0.85 {
            scale = 1.35 + 0.1 * volatility; // grow aggressively under pressure
        } else if hit_rate > 0.75 && saturation < 0.5 {
            scale = 0.85 - 0.05 * volatility; // shrink if underutilized
        }
    
        let new_cap = ((*cap as f64) * scale).round() as usize;
        let new_cap = new_cap.clamp(self.min_cap, self.max_cap).max(1);
    
        // Only rebuild if capacity changes
        if new_cap != *cap {
            let mut new_lru = LruCache::new(NonZeroUsize::new(new_cap).unwrap());
            for (k, v) in lru.iter().map(|(k, v)| (*k, *v)).collect::<Vec<_>>() {
                new_lru.put(k, v);
            }
            *lru = new_lru;
            *cap = new_cap;
        }
    
        // Reset counters and timestamp
        *self.hits.write() = 0;
        *self.misses.write() = 0;
        *last = now;
    }
    

// ROI-aware cycle breaking: prune worst-ROI node per SCC, then run Kahn
fn topo_sort_cycle_prune(opps: &[MEVOpportunity]) -> Vec<usize> {
    let idx_of: HashMap<u64, usize> = opps.iter().enumerate().map(|(i,o)| (o.id, i)).collect();
    let n = opps.len();
    let mut g: Vec<Vec<usize>> = vec![Vec::new(); n];
    for (i,o) in opps.iter().enumerate() {
        for dep in &o.dependencies {
            if let Some(&d) = idx_of.get(dep) { g[d].push(i); }
        }
    }
    // Tarjan SCC
    let mut idx = 0usize;
    let mut indices = vec![usize::MAX; n];
    let mut low = vec![usize::MAX; n];
    let mut stack: Vec<usize> = Vec::new();
    let mut on = vec![false; n];
    let mut sccs: Vec<Vec<usize>> = Vec::new();
    fn strong(v: usize, idx: &mut usize, indices: &mut [usize], low: &mut [usize], stack: &mut Vec<usize>, on: &mut [bool], g: &[Vec<usize>], out: &mut Vec<Vec<usize>>) {
        indices[v] = *idx; low[v] = *idx; *idx += 1; stack.push(v); on[v] = true;
        for &w in &g[v] {
            if indices[w] == usize::MAX { strong(w, idx, indices, low, stack, on, g, out); low[v] = low[v].min(low[w]); }
            else if on[w] { low[v] = low[v].min(indices[w]); }
        }
        if low[v] == indices[v] {
            let mut comp = Vec::new();
            loop { let w = stack.pop().unwrap(); on[w] = false; comp.push(w); if w == v { break; } }
            out.push(comp);
        }
    }
    for v in 0..n { if indices[v] == usize::MAX { strong(v, &mut idx, &mut indices, &mut low, &mut stack, &mut on, &g, &mut sccs); } }
    // Remove the worst in each cyclic SCC
    let mut removed: HashSet<usize> = HashSet::new();
    for comp in sccs { if comp.len() > 1 {
        let mut worst = comp[0];
        let mut worst_score = f64::INFINITY;
        for &u in &comp {
            let net = opps[u].profit_estimate.saturating_sub(opps[u].gas_estimate + opps[u].priority_fee) as f64;
            let score = -(net.max(0.0) * opps[u].success_probability);
            if score < worst_score { worst_score = score; worst = u; }
        }
        removed.insert(worst);
    }}
    // Kahn on reduced graph
    let mut indeg = vec![0usize; n];
    for u in 0..n { if removed.contains(&u) { continue; } for &v in &g[u] { if !removed.contains(&v) { indeg[v]+=1; } } }
    let mut q = VecDeque::new();
    for (i,&d) in indeg.iter().enumerate() { if d==0 { q.push_back(i); } }
    let mut order = Vec::new();
    while let Some(u) = q.pop_front() {
        order.push(u);
        for &v in &g[u] { if removed.contains(&v) { continue; } indeg[v]-=1; if indeg[v]==0 { q.push_back(v); } }
    }
    order
}
 

// SIMD helper for fast dot products when n<=32
#[derive(Clone)]
struct QaSimd {
    n: usize,
    // Row-major J as SIMD lanes, padded to MAX_N_SIMD
    j_rows: [Simd<f64, MAX_N_SIMD>; MAX_N_SIMD],
    h_simd: Simd<f64, MAX_N_SIMD>,
}

impl QaSimd {
    fn new(J: &[Vec<f64>], h: &[f64]) -> Self {
        assert_eq!(J.len(), h.len());
        let n = J.len().min(MAX_N_SIMD);
        let mut j_rows: [Simd<f64, MAX_N_SIMD>; MAX_N_SIMD] = [Simd::splat(0.0); MAX_N_SIMD];
        let mut h_pad = [0.0f64; MAX_N_SIMD];
        for i in 0..n {
            let mut row = [0.0f64; MAX_N_SIMD];
            for j in 0..n { row[j] = J[i][j]; }
            j_rows[i] = Simd::from_array(row);
            h_pad[i] = h[i];
        }
        Self { n, j_rows, h_simd: Simd::from_array(h_pad) }
    }

    #[inline]
    fn spins_to_simd(spins_bits: u128, n: usize) -> Simd<f64, MAX_N_SIMD> {
        let mut arr = [0.0f64; MAX_N_SIMD];
        for i in 0..n {
            arr[i] = if ((spins_bits >> i) & 1) == 1 { 1.0 } else { -1.0 };
        }
        Simd::from_array(arr)
    }

    // ΔE = 2 s_i ( h_i + Σ_j J[i][j] s_j )
    #[inline]
    fn delta_energy(&self, i: usize, spins_bits: u128) -> f64 {
        let s = Self::spins_to_simd(spins_bits, self.n);
        let dot = (self.j_rows[i] * s).reduce_sum();
        let s_i = if ((spins_bits >> i) & 1) == 1 { 1.0 } else { -1.0 };
        2.0 * s_i * (self.h_simd[i] + dot)
    }

    // Apply flip and update neighbor sums using SIMD-backed J rows
    #[inline]
    fn apply_flip_neigh(&self, i: usize, spins_bits: &mut u128, energy: &mut f64, neigh_sum: &mut [f64]) {
        let dE = self.delta_energy(i, *spins_bits);
        *energy += dE;
        // flip
        *spins_bits ^= 1u128 << i;
        // update neigh_sum[k] += 2 * J[k][i] * s_i_before
        let s_i_after = if ((*spins_bits >> i) & 1) == 1 { 1.0 } else { -1.0 };
        let s_i_before = -s_i_after;
        let delta = 2.0 * s_i_before;
        for k in 0..self.n { neigh_sum[k] += delta * self.j_rows[k][i]; }
    }
}
 

#[inline]
fn profit_guard_lamports(opp_profit: u64, est_cost: u64, success_prob: f64) -> bool {
    if opp_profit <= est_cost { return false; }
    let net = (opp_profit - est_cost) as f64;
    net * success_prob >= (opp_profit as f64) * ROI_MIN_FRACTION
}

#[inline]
fn tip_efficiency_lamports_per_cu(priority_fee: u64, cu_limit: u32) -> f64 {
    (priority_fee as f64) / (cu_limit as f64).max(1.0)
}

#[inline]
fn bundle_key_from_spins(spins: u128) -> u128 { spins }

#[inline]
fn bundle_key_from_bundle(bundle: &[usize]) -> u128 {
    let mut k = 0u128;
    for &i in bundle { if i < 128 { k |= 1u128 << i; } }
    k
}

// Simple bundle key helper for external callers using raw bitsets
#[inline]
fn bundle_key(bits: u128) -> u128 { bits }

// Gaussian per-field perturbation using Box-Muller; relative sigma per magnitude
fn perturb_fields_gaussian(fields: &mut [f64], rng: &mut ChaCha20Rng) {
    for f in fields.iter_mut() {
        // Box-Muller
        let u1 = rng.gen::<f64>().clamp(1e-12, 1.0);
        let u2 = rng.gen::<f64>();
        let z0 = (-2.0 * u1.ln()).sqrt() * (2.0 * std::f64::consts::PI * u2).cos();
        let sigma = (f.abs() * 0.05).max(1e-6); // 5% relative noise
        *f += z0 * sigma;
    }
}

// Slot-limited parallel optimization helper
fn parallel_optimize_slot_limited<F>(
    replicas: usize,
    max_ms: u64,
    run_replica: F,
) -> Vec<QaState>
where
    F: Fn(usize) -> QaState + Send + Sync + Clone,
{
    use rayon::prelude::*;
    let start = std::time::Instant::now();
    (0..replicas)
        .into_par_iter()
        .map(|rid| {
            if start.elapsed().as_millis() > max_ms as u128 {
                return QaState { spins: 0, n: 0, energy: f64::INFINITY, magnetization: 0.0, bundle_bits: 0 };
            }
            run_replica.clone()(rid)
        })
        .collect()
}

// Slot and time budget enforcement with CU-aware anneal guard
struct BudgetGuard {
    slot_time_ms: u64,
    anneal_frac: f64,     // e.g., 0.40 of slot
    hard_ceiling_ms: u64, // e.g., 250ms absolute ceiling
    max_flips: usize,     // safety valve on iterations
}

impl BudgetGuard {
    #[inline]
    fn deadline_ms(&self) -> u64 { (self.slot_time_ms as f64 * self.anneal_frac).round() as u64 }
    #[inline]
    fn within(&self, start: std::time::Instant, flips_done: usize) -> bool {
        let elapsed = start.elapsed().as_millis() as u64;
        elapsed < self.hard_ceiling_ms.min(self.deadline_ms()) && flips_done < self.max_flips
    }
}

fn anneal_with_budget<F>(mut step: F, guard: &BudgetGuard)
where
    F: FnMut() -> bool, // returns true if accepted/improved step executed
{
    let start = std::time::Instant::now();
    let mut flips_done = 0usize;
    while guard.within(start, flips_done) {
        let _ = step();
        flips_done += 1;
    }
}

// Pre-validate accounts to avoid conflicting write sets inside bundle
fn has_conflicts(bundle: &[MEVOpportunity]) -> bool {
    let mut w: HashSet<Pubkey> = HashSet::new();
    for opp in bundle {
        for ix in &opp.instructions {
            for acc in &ix.accounts {
                if acc.is_writable {
                    if !w.insert(acc.pubkey) { return true; }
                }
            }
        }
    }
    false
}

// Produce a maximal conflict-free subset (greedy)
fn greedy_conflict_free(mut bundle: Vec<MEVOpportunity>) -> Vec<MEVOpportunity> {
    // simple descending profit order
    bundle.sort_by(|a,b| b.profit_estimate.cmp(&a.profit_estimate));
    let mut w: HashSet<Pubkey> = HashSet::new();
    let mut out = Vec::new();
    for opp in bundle.into_iter() {
        let mut ok = true;
        'outer: for ix in &opp.instructions {
            for acc in &ix.accounts {
                if acc.is_writable && w.contains(&acc.pubkey) { ok = false; break 'outer; }
            }
        }
        if ok {
            for ix in &opp.instructions {
                for acc in &ix.accounts { if acc.is_writable { w.insert(acc.pubkey); } }
            }
            out.push(opp);
            if out.len() >= MAX_BUNDLE_SIZE { break; }
        }
    }
    out
}

// Topological sort with cycle detection over dependency ids
fn topo_sort(opps: &[MEVOpportunity]) -> Option<Vec<usize>> {
    let idx_of: HashMap<u64, usize> = opps.iter().enumerate().map(|(i,o)| (o.id, i)).collect();
    let mut indeg = vec![0usize; opps.len()];
    let mut graph = vec![Vec::<usize>::new(); opps.len()];
    for (i,o) in opps.iter().enumerate() {
        for dep_id in &o.dependencies {
            if let Some(&d) = idx_of.get(dep_id) { graph[d].push(i); indeg[i]+=1; }
        }
    }
    let mut q = VecDeque::new();
    for (i,&d) in indeg.iter().enumerate() { if d==0 { q.push_back(i); } }
    let mut order = Vec::with_capacity(opps.len());
    while let Some(u) = q.pop_front() {
        order.push(u);
        for &v in &graph[u] { indeg[v]-=1; if indeg[v]==0 { q.push_back(v); } }
    }
    if order.len()==opps.len() { Some(order) } else { None }
}
use std::collections::{HashMap, BTreeMap, HashSet, VecDeque};
use std::sync::Arc;
use parking_lot::RwLock;
use std::time::{Duration, Instant};
use lru::LruCache;
use std::num::NonZeroUsize;
use solana_sdk::{
    pubkey::Pubkey,
    instruction::Instruction,
    compute_budget::ComputeBudgetInstruction,
    signature::Keypair,
    transaction::Transaction,
    hash::Hash,
};
use solana_client::rpc_client::RpcClient;
use rayon::prelude::*;
use tracing::{info, warn, instrument};
use rand::{Rng, SeedableRng, RngCore};
use rand_chacha::ChaCha20Rng;
use ordered_float::OrderedFloat;
use std::simd::{Simd, StdFloat};

const INITIAL_TEMPERATURE: f64 = 1000.0;
const MIN_TEMPERATURE: f64 = 0.001;
const COOLING_RATE: f64 = 0.995;
const MAX_ITERATIONS: usize = 10000;
const QUANTUM_TUNNELING_PROB: f64 = 0.05;
const REPLICA_COUNT: usize = 8;
const COUPLING_STRENGTH: f64 = 0.1;
const PROFIT_THRESHOLD: u64 = 50_000; // 0.00005 SOL minimum
const MAX_COMPUTE_UNITS: u32 = 1_400_000;
const PRIORITY_FEE_PERCENTILE: f64 = 0.95;
const SLOT_TIME_MS: u64 = 400;
const MAX_BUNDLE_SIZE: usize = 5;
const RISK_AVERSION_FACTOR: f64 = 0.15;
// Lamport-denominated penalties to keep the objective in a single unit system
const PENALTY_UNKNOWN_DEP_LAMPORTS: u64 = 25_000;       // 0.000025 SOL per unknown/missing dep
const PENALTY_MISSING_DEP_LAMPORTS: u64 = 50_000;       // 0.00005 SOL per missing required dep
const PENALTY_PER_SLOT_LATE_LAMPORTS: u64 = 5_000;      // 0.000005 SOL per slot past deadline
const PENALTY_PER_EXTRA_TX_LAMPORTS: u64 = 100_000;     // 0.0001 SOL per tx beyond MAX_BUNDLE_SIZE
const GAS_WATERMARK: u64 = 5_000_000;                   // lamports high watermark for soft gas pressure
const ROI_MIN_FRACTION: f64 = 0.05;                     // require at least 5% of expected profit margin after fees
const MAX_N_SIMD: usize = 32;                           // SIMD acceleration threshold

// Slot context shared across timing-sensitive routines
#[derive(Clone, Copy)]
struct SlotCtx { current_slot: u64, slot_time_ms: u64 }

// Sliding fee window with EWMA failure adjustment
struct FeeWindow {
    vals: VecDeque<u64>,
    max_len: usize,
    decay: f64,
    ewma_fail: f64,
}
impl FeeWindow {
    fn new(max_len: usize, decay: f64) -> Self {
        Self { vals: VecDeque::with_capacity(max_len), max_len, decay, ewma_fail: 0.0 }
    }
    #[inline]
    fn push(&mut self, v: u64) {
        if self.vals.len() == self.max_len { let _ = self.vals.pop_front(); }
        self.vals.push_back(v);
    }
    #[inline]
    fn record_failure(&mut self) { self.ewma_fail = self.ewma_fail * self.decay + (1.0 - self.decay); }
    #[inline]
    fn record_success(&mut self) { self.ewma_fail *= self.decay; }
    #[inline]
    fn pct(&self, base: f64) -> f64 { (base - 0.25 * self.ewma_fail).clamp(0.7, 0.98) }
    #[inline]
    fn risk_adjusted_percentile(&self, base: f64) -> f64 { self.pct(base) }
    #[inline]
    fn percentile_value(&self, pct: f64, default: u64) -> u64 {
        if self.vals.is_empty() { return default; }
        let mut v: Vec<u64> = self.vals.iter().copied().collect();
        v.sort_unstable();
        let idx = ((v.len() as f64 - 1.0) * pct).round() as usize;
        v[idx]
    }
}

// Budget planner with RPC simulation + windowed fee selection
struct BudgetPlanner {
    window: RwLock<FeeWindow>,
    safety_mult: f64,
    percentile_base: f64,
    max_cu: u32,
}
impl BudgetPlanner {
    fn new(window_len: usize, decay: f64, safety_mult: f64, max_cu: u32) -> Self {
        Self { window: RwLock::new(FeeWindow::new(window_len, decay)), safety_mult, percentile_base: PRIORITY_FEE_PERCENTILE, max_cu }
    }
    fn plan(
        &self,
        rpc: &RpcClient,
        ixs: &[Instruction],
        payer: &Keypair,
        _bh: Hash,
        profit_cap: u64,
        default_cu_price: u64,
    ) -> (u32, u64) {
        let bh = rpc.get_latest_blockhash().unwrap_or_default();
        let tx = Transaction::new_signed_with_payer(ixs, Some(&payer.pubkey()), &[payer], bh);
        let used = rpc
            .simulate_transaction(&tx)
            .ok()
            .and_then(|s| s.value.units_consumed)
            .unwrap_or(250_000) as f64;
        let cu_limit = ((used * self.safety_mult).ceil() as u32).clamp(50_000, self.max_cu);
        let pct = self.window.read().pct(self.percentile_base);
        let cu_price = self
            .window
            .read()
            .percentile_value(pct, default_cu_price)
            .min((profit_cap as f64 * 0.06).round() as u64)
            .max(1_000);
        (cu_limit, cu_price)
    }
    #[inline]
    fn record_failure(&self, observed_price: u64) {
        let mut w = self.window.write();
        w.record_failure();
        w.push(observed_price);
    }
    #[inline]
    fn record_success(&self, observed_price: u64) {
        let mut w = self.window.write();
        w.record_success();
        w.push(observed_price);
    }
}

// Sender policy for adaptive resend pacing
struct SenderPolicy { base_interval_ms: u64, max_interval_ms: u64, congestion_backoff: f64 }
impl SenderPolicy {
    #[inline]
    fn next_interval_ms(&self, failures: u32, slot_time_ms: u64, cu_limit: u32) -> u64 {
        let cu_factor = if cu_limit > 1_000_000 { 1.12 } else { 1.0 };
        let ms = (self.base_interval_ms as f64 * cu_factor * self.congestion_backoff.powi(failures as i32)) as u64;
        ms.clamp(80, self.max_interval_ms).min(slot_time_ms / 2)
    }
}

// Simple conflict detector for external bundles
fn write_conflicts(bundle: &[MEVOpportunity]) -> bool {
    let mut seen: HashSet<Pubkey> = HashSet::with_capacity(64);
    for opp in bundle {
        for ix in &opp.instructions {
            for acc in &ix.accounts {
                if acc.is_writable && !seen.insert(acc.pubkey) { return true; }
            }
        }
    }
    false
}

 

#[derive(Clone)]
struct QuantumState {
    bundle: Vec<usize>,
    energy: f64,
    magnetization: f64,
    spin_configuration: Vec<i8>,
}

// Bit-packed quantum annealing state (alternative compact representation)
// bit=1 => +1, bit=0 => -1
#[derive(Clone)]
struct QaState {
    spins: u128,
    n: usize,
    energy: f64,
    magnetization: f64, // average spin in [-1,1]
    bundle_bits: u128,   // mirrors spins; hook for hard constraints if needed
}

#[inline]
fn spin_sign(spins: u128, i: usize) -> f64 {
    if ((spins >> i) & 1) == 1 { 1.0 } else { -1.0 }
}

#[inline]
fn magnetization_from_bits(spins: u128, n: usize) -> f64 {
    // m = (count(+1) - count(-1))/n = (2*popcount - n)/n
    let mask = if n >= 128 { u128::MAX } else { (1u128 << n) - 1 };
    let ones = (spins & mask).count_ones() as i64;
    ((2 * ones as i64 - n as i64) as f64) / (n as f64)
}

#[inline]
fn adaptive_tunneling_prob_align(
    base_temp: f64,
    iter: usize,
    stats: &AnnealStats,
    i: usize,
    J: &[Vec<f64>],
    pol: &TunnelingPolicy,
    magnetization: f64,
) -> f64 {
    let base = adaptive_tunneling_prob(base_temp, iter, stats, i, J, pol);
    let align_term = 0.5 + 0.5 * (1.0 - magnetization.abs());
    (base * align_term).min(pol.max)
}

#[inline]
fn update_bundle_bits(state: &mut QaState) {
    // For now, bundle_bits mirrors spins; place to enforce hard constraints (dep closure, etc.)
    state.bundle_bits = state.spins;
    state.magnetization = magnetization_from_bits(state.spins, state.n);
}

// Slot-aware, adaptive “transverse field” perturbations for QaState.
// Flips up to k random spins with probability informed by coupling and stagnation, bounded by slot time.
fn apply_transverse_field_bits(
    state: &mut QaState,
    base_temp: f64,
    iter: usize,
    stats: &AnnealStats,
    J: &[Vec<f64>],
    pol: &TunnelingPolicy,
    slot_ctx: SlotCtx,
    rng: &mut ChaCha20Rng,
) {
    let max_flips = (state.n.max(8) / 8).clamp(1, 8);
    let flip_attempts = max_flips.min(((slot_ctx.slot_time_ms / 10) as usize).max(1));
    for _ in 0..flip_attempts {
        let i = (rng.next_u64() as usize) % state.n;
        let p = adaptive_tunneling_prob_align(base_temp, iter, stats, i, J, pol, state.magnetization);
        if rng.gen::<f64>() < p {
            state.spins ^= 1u128 << i;
        }
    }
    update_bundle_bits(state);
}

// Helper wrapper to align with tests naming
#[inline]
fn update_bundle(state: &mut QaState) { update_bundle_bits(state); }

// Prevent magnetization drift by periodic recompute
#[inline]
fn recompute_magnetization_bits(spins: u128, n: usize) -> f64 {
    let mask = if n >= 128 { u128::MAX } else { (1u128 << n) - 1 };
    let ones = (spins & mask).count_ones() as i64;
    ((2 * ones as i64 - n as i64) as f64) / (n as f64)
}

struct DriftTracker { recompute_every: usize, flips_since: usize }
impl DriftTracker {
    fn new(recompute_every: usize) -> Self { Self { recompute_every, flips_since: 0 } }
    #[inline] fn on_flip(&mut self) { self.flips_since = self.flips_since.saturating_add(1); }
    #[inline] fn should_recompute(&self) -> bool { self.flips_since >= self.recompute_every }
    #[inline] fn recompute(&mut self, state: &mut QaState) { state.magnetization = recompute_magnetization_bits(state.spins, state.n); self.flips_since = 0; }
}

// Quantum anneal optional path guard
struct QaOptions { enable_quantum_moves: bool, require_coupling_consistency: bool }
#[inline]
fn guarded_flip(opts: &QaOptions, has_J_h_snapshot: bool, perform_flip: impl FnOnce()) -> bool {
    if opts.enable_quantum_moves && (!opts.require_coupling_consistency || has_J_h_snapshot) { perform_flip(); true } else { false }
}

pub struct ScoreModel {
    // Learnable weights; initialize from sensible priors
    pub w_profit: f64,   // 0.45
    pub w_prob: f64,     // 0.30
    pub w_urgency: f64,  // 0.15
    pub w_type: f64,     // 0.10
    pub w_cueff: f64,    // 0.10
    pub type_alpha: std::collections::HashMap<OpportunityType, f64>,
    pub lr: f64,
    pub clamp: (f64, f64),
}

impl ScoreModel {
    pub fn new() -> Self {
        use OpportunityType::*;
        let mut type_alpha = std::collections::HashMap::new();
        type_alpha.insert(Liquidation, 1.6);
        type_alpha.insert(AtomicArb, 1.3);
        type_alpha.insert(Arbitrage, 1.2);
        type_alpha.insert(JitLiquidity, 1.05);
        type_alpha.insert(Sandwich, 0.8);
        Self { w_profit: 0.45, w_prob: 0.30, w_urgency: 0.15, w_type: 0.10, w_cueff: 0.10, type_alpha, lr: 0.01, clamp: (-2.0, 2.0) }
    }

    pub fn score(&self, opp: &MEVOpportunity, current_slot: u64) -> f64 {
        let net = opp.profit_estimate.saturating_sub(opp.gas_estimate + opp.priority_fee) as f64;
        let profit = (net / 1_000_000_000.0).max(0.0);
        let urgency = 1.0 / (1.0 + opp.deadline_slot.saturating_sub(current_slot) as f64);
        let typem = *self.type_alpha.get(&opp.opportunity_type).unwrap_or(&1.0);
        self.w_profit*profit + self.w_prob*opp.success_probability + self.w_urgency*urgency + self.w_type*typem
    }

    pub fn score_with_slot(&self, opp: &MEVOpportunity, current_slot: u64) -> f64 {
        let net = opp.profit_estimate.saturating_sub(opp.gas_estimate + opp.priority_fee) as f64;
        let profit = (net / 1_000_000_000.0).max(0.0);
        let urgency = 1.0 / (1.0 + opp.deadline_slot.saturating_sub(current_slot) as f64);
        let typem = *self.type_alpha.get(&opp.opportunity_type).unwrap_or(&1.0);
        let cu_eff = 1.0 / ((opp.gas_estimate + opp.priority_fee).max(1) as f64);
        let s = self.w_profit*profit + self.w_prob*opp.success_probability + self.w_urgency*urgency + self.w_type*typem + self.w_cueff*cu_eff;
        s.clamp(self.clamp.0, self.clamp.1)
    }

    // Online update toward realized P&L (profit_realized normalized), bounded and clipped
    pub fn update(&mut self, opp: &MEVOpportunity, current_slot: u64, realized_lamports: i64) {
        let target = (realized_lamports as f64 / 1_000_000_000.0).clamp(self.clamp.0, self.clamp.1);
        let pred = self.score_with_slot(opp, current_slot);
        let err = (target - pred).clamp(-0.5, 0.5); // gradient clipping
        self.w_profit = (self.w_profit + self.lr * err).clamp(-3.0, 3.0);
        self.w_prob   = (self.w_prob   + self.lr * err).clamp(-3.0, 3.0);
        self.w_urgency= (self.w_urgency+ self.lr * err*0.5).clamp(-3.0, 3.0);
        self.w_type   = (self.w_type   + self.lr * err*0.25).clamp(-3.0, 3.0);
        self.w_cueff  = (self.w_cueff  + self.lr * err*0.25).clamp(-3.0, 3.0);
        let alpha = self.type_alpha.entry(opp.opportunity_type.clone()).or_insert(1.0);
        *alpha = (*alpha + self.lr * err * 0.2).clamp(0.2, 3.0);
    }
}

pub struct QuantumAnnealingOptimizer {
    rpc_client: Arc<RpcClient>,
    opportunities: Arc<RwLock<Vec<MEVOpportunity>>>,
    coupling_matrix: Arc<RwLock<Vec<Vec<f64>>>>,
    local_fields: Arc<RwLock<Vec<f64>>>,
    temperature_schedule: Vec<f64>,
    rng: ChaCha20Rng,
    profit_cache: Arc<EnergyCache>,
    blockhash_cache: Arc<BlockhashCache>,
    planner: Arc<BudgetPlanner>,
    sender_policy: SenderPolicy,
    penalty_model: RwLock<OnlineLogit>,
    latency_model: LatencyModel,
    tunneling_policy: TunnelingPolicy,
}

impl QuantumAnnealingOptimizer {
    pub fn new(rpc_url: &str) -> Self {
        let mut temperature_schedule = Vec::with_capacity(MAX_ITERATIONS);
        let mut temp = INITIAL_TEMPERATURE;
        
        for _ in 0..MAX_ITERATIONS {
            temperature_schedule.push(temp);
            temp *= COOLING_RATE;
            if temp < MIN_TEMPERATURE {
                temp = MIN_TEMPERATURE;
            }
        }

        Self {
            rpc_client: Arc::new(RpcClient::new(rpc_url.to_string())),
            opportunities: Arc::new(RwLock::new(Vec::new())),
            coupling_matrix: Arc::new(RwLock::new(Vec::new())),
            local_fields: Arc::new(RwLock::new(Vec::new())),
            temperature_schedule,
            rng: ChaCha20Rng::from_entropy(),
            profit_cache: Arc::new(EnergyCache::new(8192, 1024, 32768, Duration::from_secs(10), Vec::new(), 50_000.0)),
            blockhash_cache: Arc::new(BlockhashCache::new(Duration::from_secs(20))),
            planner: Arc::new(BudgetPlanner::new(128, 0.9, 1.2, MAX_COMPUTE_UNITS)),
            sender_policy: SenderPolicy { base_interval_ms: 120, max_interval_ms: 280, congestion_backoff: 1.25 },
            penalty_model: RwLock::new(OnlineLogit::new(6, 0.02, 1e-4)),
            latency_model: LatencyModel { base_ms: 120, per_kb_ms: 20, rpc_variance_ms: 40 },
            tunneling_policy: TunnelingPolicy { base: 0.05, max: 0.25, stagnation_boost: 0.02, coupling_boost: 0.3 },
        }
    }

    pub fn update_opportunities(&self, new_opportunities: Vec<MEVOpportunity>) {
        let mut opps = self.opportunities.write();
        *opps = new_opportunities;
        
        let n = opps.len();
        let mut coupling = vec![vec![0.0; n]; n];
        let mut fields = vec![0.0; n];
        
        // First compute local fields in lamports
        for i in 0..n {
            fields[i] = self.calculate_local_field(&opps[i]);
        }
        // Then compute dimensionless couplings and scale to lamports using local field magnitudes
        for i in 0..n {
            for j in i+1..n {
                let base = self.calculate_coupling(&opps[i], &opps[j]); // dimensionless
                let avg_scale = 0.5 * (fields[i].abs() + fields[j].abs()); // lamports
                let scaled = base * avg_scale; // lamport-scaled coupling
                coupling[i][j] = scaled;
                coupling[j][i] = scaled;
            }
        }
        
        *self.coupling_matrix.write() = coupling;
        *self.local_fields.write() = fields;
        self.profit_cache.clear();
    }

    fn calculate_local_field(&self, opp: &MEVOpportunity) -> f64 {
        // All values in lamports; keep units consistent
        let base_profit = opp.profit_estimate as f64;
        let gas_cost = (opp.gas_estimate + opp.priority_fee) as f64;
        let success = opp.success_probability.clamp(0.0, 1.0);
        let liquidity_penalty = (base_profit * opp.liquidity_impact.max(0.0)) as f64;
        base_profit * success - gas_cost - liquidity_penalty
    }

    fn calculate_coupling(&self, opp1: &MEVOpportunity, opp2: &MEVOpportunity) -> f64 {
        let mut coupling = 0.0;
        
        if opp1.input_token == opp2.output_token || opp1.output_token == opp2.input_token {
            coupling += 0.3;
        }
        
        if opp1.dependencies.contains(&opp2.id) || opp2.dependencies.contains(&opp1.id) {
            coupling += 0.5;
        }
        
        if opp1.opportunity_type == opp2.opportunity_type {
            coupling -= 0.2;
        }
        
        let deadline_diff = (opp1.deadline_slot as i64 - opp2.deadline_slot as i64).abs();
        if deadline_diff < 10 {
            coupling += 0.1;
        }
        
        coupling * COUPLING_STRENGTH // dimensionless; scaled to lamports where used
    }

    pub fn optimize(&mut self) -> Vec<MEVOpportunity> {
        let opps = self.opportunities.read();
        if opps.is_empty() {
            return Vec::new();
        }
        
        let n = opps.len().min(32);
        // Snapshot slot context once per optimize call (no RPC in hot loop)
        let slot_ctx = SlotCtx { current_slot: self.rpc_client.get_slot().unwrap_or(0), slot_time_ms: SLOT_TIME_MS };
        drop(opps);
        self.optimize_with_ctx(slot_ctx)
    }

    pub fn optimize_with_ctx(&mut self, slot_ctx: SlotCtx) -> Vec<MEVOpportunity> {
        let opps = self.opportunities.read();
        if opps.is_empty() { return Vec::new(); }
        let n = opps.len().min(32);
        // Initialize temperature ladder for parallel tempering
        let mut temps = Vec::with_capacity(REPLICA_COUNT);
        let log_min = MIN_TEMPERATURE.ln();
        let log_max = INITIAL_TEMPERATURE.ln();
        for i in 0..REPLICA_COUNT {
            let frac = i as f64 / (REPLICA_COUNT as f64 - 1.0).max(1.0);
            temps.push((log_min + frac * (log_max - log_min)).exp());
        }

        // One anneal round with swaps between adjacent replicas
        let mut states: Vec<_> = (0..REPLICA_COUNT)
            .into_par_iter()
            .map(|replica_id| {
                self.run_quantum_annealing_with_temp(n, replica_id, slot_ctx, temps[replica_id])
            })
            .collect();

        // Attempt adjacent swaps (parallel tempering) once
        let mut swap_rng = ChaCha20Rng::seed_from_u64(slot_ctx.current_slot ^ 0xA5A5A5A5A5A5A5);
        for r in 0..(REPLICA_COUNT.saturating_sub(1)) {
            let t1 = temps[r];
            let t2 = temps[r+1];
            let e1 = states[r].energy;
            let e2 = states[r+1].energy;
            // Metropolis for swap: accept with prob exp((1/T1-1/T2)*(E1-E2))
            let delta = (1.0/t1 - 1.0/t2) * (e1 - e2);
            let accept_swap = delta >= 0.0 || swap_rng.gen::<f64>() < delta.exp();
            if accept_swap { states.swap(r, r+1); temps.swap(r, r+1); }
        }
        
        let best_state = states.into_iter()
            .min_by_key(|s| OrderedFloat(s.energy))
            .unwrap();
        
        best_state.bundle.into_iter()
            .filter_map(|idx| opps.get(idx).cloned())
            .collect()
    }

    #[instrument(skip(self))]
    fn run_quantum_annealing_with_temp(&self, n: usize, replica_id: usize, slot_ctx: SlotCtx, initial_temp: f64) -> QuantumState {
        // Deterministic seed per slot and replica
        let seed = (slot_ctx.current_slot as u64) ^ ((replica_id as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15));
        let mut rng = ChaCha20Rng::seed_from_u64(seed);
        // Snapshot matrices once
        let J = { self.coupling_matrix.read().clone() };
        let h = { self.local_fields.read().clone() };
        // Snapshot opp dependencies mapping for constraint checks
        let opps = self.opportunities.read();
        let mut idx_of: HashMap<u64, usize> = HashMap::new();
        for (i, o) in opps.iter().enumerate() { idx_of.insert(o.id, i); }

        // Initialize compact bitset state and neighbor sums
        let mut spins: u128 = 0;
        let mut magnetization = 0.0;
        for i in 0..n {
            if rng.gen::<bool>() { spins |= 1u128 << i; magnetization += 1.0; } else { magnetization -= 1.0; }
        }
        magnetization /= n as f64;
        let mut neigh_sum = vec![0.0f64; n];
        let use_simd = n <= MAX_N_SIMD;
        let qa_opt = if use_simd { Some(QaSimd::new(&J, &h)) } else { None };
        if let Some(qa) = &qa_opt {
            let s_simd = QaSimd::spins_to_simd(spins, n);
            for i in 0..n { neigh_sum[i] = (qa.j_rows[i] * s_simd).reduce_sum(); }
        } else {
            for i in 0..n {
                let mut sum = 0.0;
                for j in 0..n {
                    if i == j { continue; }
                    let s_j = if ((spins >> j) & 1) == 1 { 1.0 } else { -1.0 };
                    sum += J[i][j] * s_j;
                }
                neigh_sum[i] = sum;
            }
        }
        let mut energy = 0.0;
        for i in 0..n {
            let s_i = if ((spins >> i) & 1) == 1 { 1.0 } else { -1.0 };
            energy -= h[i] * s_i;
            for j in (i+1)..n {
                let s_j = if ((spins >> j) & 1) == 1 { 1.0 } else { -1.0 };
                energy -= J[i][j] * s_i * s_j;
            }
        }
        // Adaptive temperature based on acceptance rate
        let mut temperature = initial_temp;
        let t_min = MIN_TEMPERATURE;
        let t_max = INITIAL_TEMPERATURE;
        let anneal_start = Instant::now();
        let mut stats = AnnealStats { last_improve_iter: 0, best_energy: energy };

        for iter in 0..MAX_ITERATIONS {
            let quantum_prob = QUANTUM_TUNNELING_PROB * (1.0 - iter as f64 / MAX_ITERATIONS as f64);
            let mut accepted = 0usize;
            for _ in 0..n {
                let i = rng.gen_range(0..n);
                let s_i = if ((spins >> i) & 1) == 1 { 1.0 } else { -1.0 };
                let dE = 2.0 * s_i * (h[i] + neigh_sum[i]);
                // Hard constraints: bundle size and dependency closure after this flip
                let currently_on = ((spins >> i) & 1) == 1;
                let spins_after = spins ^ (1u128 << i);
                let proposed_count = spins_after.count_ones() as usize;
                let mut constraints_ok = proposed_count <= MAX_BUNDLE_SIZE;
                if constraints_ok && !currently_on {
                    if let Some(opp) = opps.get(i) {
                        for dep in &opp.dependencies {
                            if let Some(&d) = idx_of.get(dep) {
                                if ((spins_after >> d) & 1) == 0 { constraints_ok = false; break; }
                            }
                        }
                    }
                }
                let accept = constraints_ok && (dE < 0.0 || rng.gen::<f64>() < quantum_prob || rng.gen::<f64>() < (-dE / temperature).exp());
                if accept {
                    if let Some(qa) = &qa_opt {
                        qa.apply_flip_neigh(i, &mut spins, &mut energy, &mut neigh_sum);
                    } else {
                        energy += dE;
                        // flip bit
                        spins ^= 1u128 << i;
                        // update neigh_sum: neigh_sum[k] += 2 * J[k][i] * s_i
                        let delta = 2.0 * s_i;
                        for k in 0..n { neigh_sum[k] += delta * J[k][i]; }
                    }
                    // update magnetization
                    let s_after = -s_i;
                    magnetization += (s_after - s_i) / (n as f64);
                    accepted += 1;
                }
            }
            // Track improvement
            if energy < stats.best_energy { stats.best_energy = energy; stats.last_improve_iter = iter; }
            // Adaptive temperature based on acceptance rate of last sweep
            let acc_rate = (accepted as f64) / (n as f64);
            let m = if acc_rate > 0.7 { 0.98 } else if acc_rate < 0.3 { 1.02 } else { 1.0 };
            temperature = (temperature * m).clamp(t_min, t_max);
            info!(replica_id, acc_rate, temperature, energy, "anneal_sweep");

            // Slot-budget-aware transverse kicks: bound by half-slot time
            if iter % 100 == 0 {
                if anneal_start.elapsed().as_millis() < (slot_ctx.slot_time_ms / 2) as u128 {
                    for i in 0..n {
                        // Adaptive tunneling probability uses coupling/local stagnation
                        let p = adaptive_tunneling_prob(temperature, iter, &stats, i, &J, &self.tunneling_policy)
                            * (0.5 + 0.5 * (1.0 - magnetization.abs()));
                        if rng.gen::<f64>() < p {
                            let s_i = if ((spins >> i) & 1) == 1 { 1.0 } else { -1.0 };
                            if let Some(qa) = &qa_opt {
                                qa.apply_flip_neigh(i, &mut spins, &mut energy, &mut neigh_sum);
                            } else {
                                let dE = 2.0 * s_i * (h[i] + neigh_sum[i]);
                                energy += dE;
                                spins ^= 1u128 << i;
                                let delta = 2.0 * s_i;
                                for k in 0..n { neigh_sum[k] += delta * J[k][i]; }
                            }
                            magnetization += (-s_i - s_i) / (n as f64);
                            if energy < stats.best_energy { stats.best_energy = energy; stats.last_improve_iter = iter; }
                        }
                    }
                }
            }
        }

        // Build final state and apply deadline-only soft penalty using slot_ctx (no RPC here)
        let mut bundle = Vec::new();
        let mut spins_vec = vec![-1i8; n];
        for i in 0..n { let bit = ((spins >> i) & 1) == 1; if bit { bundle.push(i); spins_vec[i] = 1; } }
        let mut state = QuantumState { bundle, energy, magnetization, spin_configuration: spins_vec };
        state.energy += self.calculate_bundle_penalty_ctx(&state, slot_ctx);
        state
    }

    fn initialize_state(&self, n: usize, rng: &mut ChaCha20Rng) -> QuantumState {
        let spin_configuration: Vec<i8> = (0..n)
            .map(|_| if rng.gen::<bool>() { 1 } else { -1 })
            .collect();
        
        let mut state = QuantumState {
            bundle: Vec::new(),
            energy: 0.0,
            magnetization: 0.0,
            spin_configuration,
        };
        
        self.update_bundle(&mut state);
        state.energy = self.calculate_total_energy(&state);
        state
    }

    fn calculate_energy_delta(&self, state: &QuantumState, flip_idx: usize) -> f64 {
        // Legacy helper (not used in hot annealing path). No locks per flip in new path.
        let coupling = self.coupling_matrix.read();
        let fields = self.local_fields.read();
        
        let mut delta = -2.0 * state.spin_configuration[flip_idx] as f64 * fields[flip_idx];
        
        for j in 0..state.spin_configuration.len() {
            if j != flip_idx {
                delta -= 2.0 * state.spin_configuration[flip_idx] as f64 
                    * coupling[flip_idx][j] * state.spin_configuration[j] as f64;
            }
        }
        delta
    }

    fn calculate_total_energy(&self, state: &QuantumState) -> f64 {
        let key = bundle_key_from_bundle(&state.bundle);
        if let Some(cached) = self.profit_cache.get(key) { info!(cache_hit=true, energy=cached, "energy_cache"); return cached; }
        
        let coupling = self.coupling_matrix.read();
        let fields = self.local_fields.read();
        let opps = self.opportunities.read();
        
        let mut energy = 0.0;
        
        for i in 0..state.spin_configuration.len() {
            energy -= fields[i] * state.spin_configuration[i] as f64;
            
            for j in i+1..state.spin_configuration.len() {
                energy -= coupling[i][j] * state.spin_configuration[i] as f64 
                    * state.spin_configuration[j] as f64;
            }
        }
        
        let total_gas: u64 = state.bundle.iter()
            .filter_map(|&idx| opps.get(idx))
            .map(|opp| opp.gas_estimate + opp.priority_fee)
            .sum();
        
        if total_gas > GAS_WATERMARK { energy += (total_gas - GAS_WATERMARK) as f64; }
        
        if state.bundle.len() > MAX_BUNDLE_SIZE {
            energy += ((state.bundle.len() - MAX_BUNDLE_SIZE) as u64 * PENALTY_PER_EXTRA_TX_LAMPORTS) as f64;
        }
        
        self.profit_cache.put(key, energy);
        info!(cache_hit=false, energy, "energy_cache");
        energy
    }

    fn calculate_bundle_penalty_ctx(&self, state: &QuantumState, slot_ctx: SlotCtx) -> f64 {
        // Soft deadline penalty scaled by online logistic model; hard constraints enforced during flips
        let opps = self.opportunities.read();
        let mut penalty = 0.0;
        for &idx in &state.bundle {
            if let Some(opp) = opps.get(idx) {
                let slot_distance = slot_ctx.current_slot.saturating_sub(opp.deadline_slot) as f64;
                if slot_distance > 0.0 {
                    // Build features: [deadline_slack, log_tip_per_cu, log_cu, net_profit, write_conflicts, dep_depth]
                    let cu_price_guess = {
                        // read planner window percentile at current base
                        let w = self.planner.window.read();
                        w.percentile_value(w.risk_adjusted_percentile(PRIORITY_FEE_PERCENTILE), 1_000)
                    } as u64;
                    let log_tip_per_cu = (cu_price_guess as f64 + 1.0).ln();
                    let log_cu = (200_000f64 + 1.0).ln(); // heuristic during anneal
                    let net_profit = (opp.profit_estimate.saturating_sub(opp.gas_estimate + opp.priority_fee)) as f64;
                    let write_conflicts = 0.0f64; // already filtered structurally
                    let dep_depth = opp.dependencies.len() as f64;
                    let feat = [slot_distance, log_tip_per_cu, log_cu, net_profit, write_conflicts, dep_depth];
                    let scale = penalty_scale_from_model(&self.penalty_model.read(), &feat);
                    penalty += ((slot_distance as u64) * PENALTY_PER_SLOT_LATE_LAMPORTS) as f64 * scale;
                }
            }
        }
        penalty
    }

    fn update_bundle(&self, state: &mut QuantumState) {
        state.bundle = state.spin_configuration.iter()
            .enumerate()
            .filter(|(_, &spin)| spin == 1)
            .map(|(idx, _)| idx)
            .collect();
        
        state.magnetization = state.spin_configuration.iter()
            .map(|&s| s as f64)
            .sum::<f64>() / state.spin_configuration.len() as f64;
    }

    fn apply_transverse_field(&self, state: &mut QuantumState, strength: f64, rng: &mut ChaCha20Rng) {
        for i in 0..state.spin_configuration.len() {
            if rng.gen::<f64>() < strength * 0.1 {
                let superposition_prob = 0.5 + 0.5 * state.magnetization;
                state.spin_configuration[i] = if rng.gen::<f64>() < superposition_prob { 1 } else { -1 };
            }
        }
        self.update_bundle(state);
    }

    pub fn execute_optimal_bundle(&self, bundle: Vec<MEVOpportunity>, payer: &Keypair) -> Result<Vec<String>, Box<dyn std::error::Error>> {
        let mut signatures = Vec::new();
        let current_slot = self.rpc_client.get_slot()?;
        let _slot_ctx = SlotCtx { current_slot, slot_time_ms: SLOT_TIME_MS };
        let mut executed_ids = Vec::new();

        // Exclude near-deadline opportunities (guard = 1 slot) and sort by priority
        let guard = 1u64;
        let filtered: Vec<MEVOpportunity> = bundle
            .into_iter()
            .filter(|o| current_slot <= o.deadline_slot.saturating_sub(guard))
            .collect();
        if filtered.is_empty() { return Ok(signatures); }
        let sorted = self.sort_bundle_by_priority_with_slot(filtered, current_slot);
        // Order by dependencies (topological), hard fail on cycles
        let order_opt = topo_sort(&sorted);
        let indices = match order_opt { Some(ord) => ord, None => topo_sort_cycle_prune(&sorted) };
        let mut ordered_bundle: Vec<MEVOpportunity> = indices.into_iter().filter_map(|i| sorted.get(i).cloned()).collect();
        // Exclude conflicting write-sets to avoid bundle failures in Jito
        if has_conflicts(&ordered_bundle) {
            ordered_bundle = greedy_conflict_free(ordered_bundle);
        }
        if ordered_bundle.is_empty() { return Ok(signatures); }

        // Latency-aware filtering by dynamic guard slots (estimate using simple size proxy)
        let est_tx_bytes = sorted.iter().map(|o| o.instructions.len()).sum::<usize>() * 512; // rough estimate
        let filtered_latency = filter_by_deadline(sorted, current_slot, est_tx_bytes, SLOT_TIME_MS, &self.latency_model);
        if filtered_latency.is_empty() { return Ok(signatures); }

        // Attempt all-or-nothing atomic single-transaction route first (within CU cap)
        if filtered_latency.len() > 1 {
            let mut combined_instructions: Vec<solana_sdk::instruction::Instruction> = Vec::new();
            for opp in &filtered_latency { combined_instructions.extend(opp.instructions.clone()); }
            let provisional_bh = self.blockhash_cache.get(&self.rpc_client);
            let provisional_tx = Transaction::new_signed_with_payer(&combined_instructions, Some(&payer.pubkey()), &[payer], provisional_bh);
            if let Ok((cu_limit_sim, percentile_fee)) = dynamic_budget(&self.rpc_client, &provisional_tx, MAX_COMPUTE_UNITS, 1.25) {
                let cu_price = choose_cu_price_lamports_per_cu(percentile_fee, ordered_bundle.iter().map(|o| o.profit_estimate).sum());
                let mut instructions = vec![
                    ComputeBudgetInstruction::set_compute_unit_limit(cu_limit_sim),
                    ComputeBudgetInstruction::set_compute_unit_price(cu_price),
                ];
                instructions.append(&mut combined_instructions);
                let build_tx = |bh: solana_sdk::hash::Hash| {
                    Transaction::new_signed_with_payer(&instructions, Some(&payer.pubkey()), &[payer], bh)
                };
                if let Ok(sig) = resend_until_landed_adaptive(&self.rpc_client, build_tx, &self.sender_policy, SLOT_TIME_MS, 2_500, cu_limit_sim) {
                    signatures.push(sig);
                    // atomic success: all opp ids considered executed
                    for o in filtered_latency { executed_ids.push(o.id); }
                    return Ok(signatures);
                }
            }
        }

        // Optional: guard total bundle CU to a conservative cap and bundle-level ROI
        let mut bundle_cu_sum: u64 = 0;
        const BUNDLE_CU_CAP: u64 = 4_000_000; // conservative cap across txs
        let mut bundle_profit_sum: u64 = 0;
        let mut bundle_cost_sum: u64 = 0;
        
        // Real-time write-set conflict detection while iterating
        let mut seen_writes: HashSet<Pubkey> = HashSet::with_capacity(64);
        for opportunity in filtered_latency {
            if opportunity.deadline_slot < current_slot {
                continue;
            }
            if !self.check_dependencies(&opportunity, &executed_ids) {
                continue;
            }

            // Start with core instructions, then determine compute/fee using simulation + recent fees
            let mut core_instructions = opportunity.instructions.clone();
            // Hot loop conflict check: skip opps that write to already seen accounts
            let mut conflict = false;
            'ixscan: for ix in &core_instructions {
                let meta = extract_writes(ix);
                for w in meta.writable {
                    if seen_writes.contains(&w) { conflict = true; break 'ixscan; }
                }
            }
            if conflict { continue; }
            let provisional_bh = self.blockhash_cache.get(&self.rpc_client);
            // Use planner to set CU and price based on sliding window & failure EWMA
            let (mut cu_limit_sim, percentile_fee) = self.planner.plan(&self.rpc_client, &core_instructions, payer, provisional_bh, opportunity.profit_estimate, 1_000);
            // Enforce bundle CU cap
            if bundle_cu_sum + cu_limit_sim as u64 > BUNDLE_CU_CAP {
                // scale down or skip
                let remaining = BUNDLE_CU_CAP.saturating_sub(bundle_cu_sum) as u32;
                if remaining < 50_000 { continue; }
                cu_limit_sim = remaining;
            }
            let cu_price = choose_cu_price_lamports_per_cu(percentile_fee, opportunity.profit_estimate);
            // Per-tx ROI guard using estimated cost with simulated CU + price
            let est_cost = (opportunity.gas_estimate as u64).saturating_add(cu_price);
            if !profit_guard_lamports(opportunity.profit_estimate, est_cost, opportunity.success_probability) {
                continue;
            }
            let mut instructions = vec![
                ComputeBudgetInstruction::set_compute_unit_limit(cu_limit_sim),
                ComputeBudgetInstruction::set_compute_unit_price(cu_price),
            ];
            instructions.append(&mut core_instructions);

            // Build-and-resend with adaptive policy
            let build_tx = |bh: solana_sdk::hash::Hash| {
                Transaction::new_signed_with_payer(&instructions, Some(&payer.pubkey()), &[payer], bh)
            };
            match resend_until_landed_adaptive(&self.rpc_client, build_tx, &self.sender_policy, SLOT_TIME_MS, 2_000, cu_limit_sim) {
                Ok(signature) => {
                    signatures.push(signature);
                    executed_ids.push(opportunity.id);
                    bundle_cu_sum = bundle_cu_sum.saturating_add(cu_limit_sim as u64);
                    bundle_profit_sum = bundle_profit_sum.saturating_add(opportunity.profit_estimate);
                    bundle_cost_sum = bundle_cost_sum.saturating_add(est_cost);
                    // Update seen writes after successful inclusion of this opp
                    for ix in &instructions {
                        let meta = extract_writes(ix);
                        for w in meta.writable { let _ = seen_writes.insert(w); }
                    }
                    // Record success with observed fee per CU
                    self.planner.record_success(cu_price);
                    // Online penalty model update: features -> y=1
                    let deadline_slack = opportunity.deadline_slot.saturating_sub(current_slot) as f64;
                    let log_tip_per_cu = (cu_price as f64 + 1.0).ln();
                    let log_cu = (cu_limit_sim as f64 + 1.0).ln();
                    let net_profit = (opportunity.profit_estimate.saturating_sub(opportunity.gas_estimate + opportunity.priority_fee)) as f64;
                    let write_conflicts = 0.0f64; // already filtered
                    let dep_depth = opportunity.dependencies.len() as f64;
                    let feat = [deadline_slack, log_tip_per_cu, log_cu, net_profit, write_conflicts, dep_depth];
                    self.penalty_model.write().update(&feat, 1.0);
                    // Bundle-level ROI discipline: if bundle so far is below threshold, stop adding
                    let bundle_ok = (bundle_profit_sum > bundle_cost_sum)
                        && ((bundle_profit_sum - bundle_cost_sum) as f64 >= (bundle_profit_sum as f64) * ROI_MIN_FRACTION);
                    if !bundle_ok { break; }
                },
                Err(_) => { 
                    self.planner.record_failure(cu_price);
                    // Online penalty model update with y=0
                    let deadline_slack = opportunity.deadline_slot.saturating_sub(current_slot) as f64;
                    let log_tip_per_cu = (cu_price as f64 + 1.0).ln();
                    let log_cu = (cu_limit_sim as f64 + 1.0).ln();
                    let net_profit = (opportunity.profit_estimate.saturating_sub(opportunity.gas_estimate + opportunity.priority_fee)) as f64;
                    let write_conflicts = 0.0f64;
                    let dep_depth = opportunity.dependencies.len() as f64;
                    let feat = [deadline_slack, log_tip_per_cu, log_cu, net_profit, write_conflicts, dep_depth];
                    self.penalty_model.write().update(&feat, 0.0);
                    continue; 
                },
            }
        }

        Ok(signatures)
    }

    fn sort_bundle_by_priority(&self, mut bundle: Vec<MEVOpportunity>) -> Vec<MEVOpportunity> {
        bundle.sort_by(|a, b| {
            let a_score = self.calculate_priority_score(a);
            let b_score = self.calculate_priority_score(b);
            b_score.partial_cmp(&a_score).unwrap()
        });
        bundle
    }

    fn sort_bundle_by_priority_with_slot(&self, mut bundle: Vec<MEVOpportunity>, current_slot: u64) -> Vec<MEVOpportunity> {
        bundle.sort_by(|a, b| {
            let a_score = self.calculate_priority_score_with_slot(a, current_slot);
            let b_score = self.calculate_priority_score_with_slot(b, current_slot);
            b_score.total_cmp(&a_score)
        });
        bundle
    }

    fn calculate_priority_score(&self, opp: &MEVOpportunity) -> f64 {
        let profit_weight = 0.4;
        let probability_weight = 0.3;
        let urgency_weight = 0.2;
        let type_weight = 0.1;
        
        let normalized_profit = (opp.profit_estimate as f64 / 1e9).min(1.0);
        let urgency = 1.0; // placeholder when snapshot not provided
        
        let type_multiplier = match opp.opportunity_type {
            OpportunityType::Liquidation => 1.5,
            OpportunityType::AtomicArb => 1.3,
            OpportunityType::Arbitrage => 1.2,
            OpportunityType::JitLiquidity => 1.1,
            OpportunityType::Sandwich => 0.9,
        };
        
        normalized_profit * profit_weight + 
        opp.success_probability * probability_weight + 
        urgency * urgency_weight + 
        type_multiplier * type_weight
    }

    fn calculate_priority_score_with_slot(&self, opp: &MEVOpportunity, current_slot: u64) -> f64 {
        let profit_weight = 0.4;
        let probability_weight = 0.3;
        let urgency_weight = 0.2;
        let type_weight = 0.05;
        let cu_eff_weight = 0.05;
        
        let net = opp.profit_estimate.saturating_sub(opp.gas_estimate + opp.priority_fee) as f64;
        let normalized_profit = (net / 1e9).max(0.0).min(1.0);
        let urgency = 1.0 / (1.0 + (opp.deadline_slot.saturating_sub(current_slot)) as f64);
        let cu_eff = 1.0 / ((opp.gas_estimate + opp.priority_fee).max(1) as f64);
        
        let type_multiplier = match opp.opportunity_type {
            OpportunityType::Liquidation => 1.5,
            OpportunityType::AtomicArb => 1.3,
            OpportunityType::Arbitrage => 1.2,
            OpportunityType::JitLiquidity => 1.1,
            OpportunityType::Sandwich => 0.9,
        };
        
        normalized_profit * profit_weight +
        opp.success_probability * probability_weight +
        urgency * urgency_weight +
        type_multiplier * type_weight +
        cu_eff * cu_eff_weight
    }

    
}

pub fn sort_by_priority(mut v: Vec<MEVOpportunity>, current_slot: u64, model: &ScoreModel) -> Vec<MEVOpportunity> {
    v.sort_by(|a, b| model.score(b, current_slot).total_cmp(&model.score(a, current_slot)));
    v
}

// Dynamic fee and CU handling (bidirectional)
pub fn calculate_dynamic_priority_fee_bidirectional(
    base_fee: u64,
    profit_lamports: u64,
    congestion_estimate: f64, // 0..1 (from fee window or inclusion rate)
    urgency: f64,              // 0..1
) -> u64 {
    // Reduce fee when congestion is low and urgency is low; increase when both high.
    let want = base_fee as f64 * (1.0 + 1.5*congestion_estimate*urgency - 0.6*(1.0 - congestion_estimate)*(1.0 - urgency));
    let cap = (profit_lamports as f64 * 0.06).max(2_000.0);
    want.clamp(500.0, cap).round() as u64
}

// Retry/resubmission with adaptive policy and relay diversity hooks
pub struct Relay {
    pub rpc: RpcClient,
    pub weight: f64, // prioritize better relays; adjust online
}

pub fn submit_with_retry_diverse(
    relays: &mut [Relay],
    mut build_tx: impl FnMut(solana_sdk::hash::Hash) -> Transaction,
    policy: &SenderPolicy,
    slot_ctx: SlotCtx,
    max_ms: u64,
    fee_window: &mut FeeWindow,
) -> Result<String, Box<dyn std::error::Error>> {
    use std::time::{Duration, Instant};
    use rand::Rng;
    let start = Instant::now();
    let mut failures = 0u32;
    let mut rng = rand_chacha::ChaCha20Rng::from_entropy();
    while start.elapsed().as_millis() < max_ms as u128 {
        // sample relay proportional to weight
        let total_w: f64 = relays.iter().map(|r| r.weight).sum();
        let mut pick = rng.gen::<f64>() * total_w.max(1e-9);
        let mut idx = 0usize;
        for (i, r) in relays.iter().enumerate() {
            pick -= r.weight;
            if pick <= 0.0 { idx = i; break; }
        }
        let rpc = &relays[idx].rpc;
        let bh = rpc.get_latest_blockhash()?;
        let tx = build_tx(bh);
        match rpc.send_transaction(&tx) {
            Ok(sig) => {
                if rpc.confirm_transaction(&sig).unwrap_or(false) {
                    fee_window.record_success();
                    // gently boost relay weight on success
                    relays[idx].weight = (relays[idx].weight * 1.02).min(10.0);
                    return Ok(sig.to_string());
                }
            }
            Err(_) => {
                failures = failures.saturating_add(1);
                fee_window.record_failure();
                // gently decay weight on failure
                relays[idx].weight = (relays[idx].weight * 0.97).max(0.1);
            }
        }
        let sleep_ms = policy.next_interval_ms(failures, slot_ctx.slot_time_ms, 200_000);
        std::thread::sleep(Duration::from_millis(sleep_ms));
    }
    Err("not landed in time".into())
}

// Ghost-aware relay statistics
pub struct RelayStats {
    recent_failures: VecDeque<bool>, // fixed-size window
    ghosted: bool,
}

impl RelayStats {
    pub fn new() -> Self {
        Self { recent_failures: VecDeque::with_capacity(12), ghosted: false }
    }
    pub fn record(&mut self, ok: bool) {
        if self.recent_failures.len() == 12 { let _ = self.recent_failures.pop_front(); }
        self.recent_failures.push_back(!ok);
        let fail_recent = self.recent_failures.iter().rev().take(6).filter(|f| **f).count();
        self.ghosted = fail_recent >= 4; // >= 4/6 recent fails => mark as ghosted
    }
    #[inline]
    pub fn is_ghosted(&self) -> bool { self.ghosted }
}

pub struct RelayEntry {
    pub rpc: RpcClient,
    pub weight: f64,
    pub stats: RelayStats,
}

// Retry with diversity and soft filtering of ghosted relays
pub fn submit_with_retry_diverse_ghost_aware(
    relays: &mut [RelayEntry],
    mut build_tx: impl FnMut(solana_sdk::hash::Hash) -> Transaction,
    policy: &SenderPolicy,
    slot_ctx: SlotCtx,
    max_ms: u64,
    fee_window: &mut FeeWindow,
) -> Result<String, Box<dyn std::error::Error>> {
    use std::time::{Duration, Instant};
    use rand::Rng;
    use rand_chacha::ChaCha20Rng;
    let start = Instant::now();
    let mut failures = 0u32;
    let mut rng = ChaCha20Rng::from_entropy();
    while start.elapsed().as_millis() < max_ms as u128 {
        // choose candidates excluding ghosted relays; ensure at least one relay remains
        let candidates: Vec<usize> = relays
            .iter()
            .enumerate()
            .filter(|(_, r)| !r.stats.is_ghosted())
            .map(|(i, _)| i)
            .collect();
        let idxs: Vec<usize> = if candidates.is_empty() { (0..relays.len()).collect() } else { candidates };

        // weighted sampling
        let total_w: f64 = idxs.iter().map(|&i| relays[i].weight).sum::<f64>().max(1e-9);
        let mut pick = rng.gen::<f64>() * total_w;
        let mut idx = idxs[0];
        for &i in &idxs { pick -= relays[i].weight; if pick <= 0.0 { idx = i; break; } }

        let rpc = &relays[idx].rpc;
        let bh = rpc.get_latest_blockhash()?;
        let tx = build_tx(bh);
        match rpc.send_transaction(&tx) {
            Ok(sig) => {
                let ok = rpc.confirm_transaction(&sig).unwrap_or(false);
                relays[idx].stats.record(ok);
                if ok {
                    fee_window.record_success();
                    relays[idx].weight = (relays[idx].weight * 1.02).min(10.0);
                    return Ok(sig.to_string());
                } else {
                    failures = failures.saturating_add(1);
                    fee_window.record_failure();
                    relays[idx].weight = (relays[idx].weight * 0.97).max(0.1);
                }
            }
            Err(_) => {
                failures = failures.saturating_add(1);
                fee_window.record_failure();
                relays[idx].stats.record(false);
                relays[idx].weight = (relays[idx].weight * 0.95).max(0.1);
            }
        }
        let sleep_ms = policy.next_interval_ms(failures, slot_ctx.slot_time_ms, 200_000);
        std::thread::sleep(Duration::from_millis(sleep_ms));
    }
    Err("not landed in time".into())
}

// Single-RPC resend loop with adaptive pacing
fn resend_until_landed_adaptive(
    rpc: &RpcClient,
    mut build_tx: impl FnMut(solana_sdk::hash::Hash) -> Transaction,
    policy: &SenderPolicy,
    slot_time_ms: u64,
    max_ms: u64,
    cu_limit: u32,
) -> Result<String, Box<dyn std::error::Error>> {
    use std::time::{Duration, Instant};
    let start = Instant::now();
    let mut failures = 0u32;
    while start.elapsed().as_millis() < max_ms as u128 {
        let bh = rpc.get_latest_blockhash()?;
        let tx = build_tx(bh);
        match rpc.send_transaction(&tx) {
            Ok(sig) => {
                if rpc.confirm_transaction(&sig).unwrap_or(false) {
                    return Ok(sig.to_string());
                }
            }
            Err(_) => { failures = failures.saturating_add(1); }
        }
        let sleep_ms = policy.next_interval_ms(failures, slot_time_ms, cu_limit);
        std::thread::sleep(Duration::from_millis(sleep_ms));
    }
    Err("not landed in time".into())
}

// Simulate to choose CU limit and return a baseline percentile fee placeholder
fn dynamic_budget(
    rpc: &RpcClient,
    tx: &Transaction,
    max_cu: u32,
    safety_mult: f64,
) -> Result<(u32, u64), Box<dyn std::error::Error>> {
    let used = rpc
        .simulate_transaction(tx)
        .ok()
        .and_then(|s| s.value.units_consumed)
        .unwrap_or(250_000) as f64;
    let cu_limit = ((used * safety_mult).ceil() as u32).clamp(50_000, max_cu);
    // Without a fee window, return a conservative baseline
    let percentile_fee = 1_000u64;
    Ok((cu_limit, percentile_fee))
}

// Bound cu price by profit cap and maintain a safe floor
fn choose_cu_price_lamports_per_cu(percentile_fee: u64, profit_cap_lamports: u64) -> u64 {
    let cap = ((profit_cap_lamports as f64) * 0.06).round() as u64;
    percentile_fee.clamp(1_000, cap.max(1_000))
}

    fn check_dependencies(&self, opp: &MEVOpportunity, executed: &[u64]) -> bool {
        opp.dependencies.iter().all(|dep| executed.contains(dep))
    }

    fn calculate_dynamic_priority_fee(&self, opp: &MEVOpportunity) -> u64 {
        let base_fee = opp.priority_fee;
        let profit_ratio = opp.profit_estimate as f64 / (opp.gas_estimate + base_fee) as f64;
        
        if profit_ratio > 10.0 {
            (base_fee as f64 * 1.5) as u64
        } else if profit_ratio > 5.0 {
            (base_fee as f64 * 1.2) as u64
        } else {
            base_fee
        }
    }

    fn submit_with_retry(&self, transaction: &Transaction, max_retries: u32) -> Result<String, Box<dyn std::error::Error>> {
        let mut retry_count = 0;
        let mut last_error = None;
        
        while retry_count < max_retries {
            match self.rpc_client.send_and_confirm_transaction_with_spinner(transaction) {
                Ok(signature) => return Ok(signature.to_string()),
                Err(e) => {
                    last_error = Some(e);
                    retry_count += 1;
                    std::thread::sleep(Duration::from_millis(50 * retry_count as u64));
                }
            }
        }
        
        Err(Box::new(last_error.unwrap()))
    }

    pub fn parallel_optimize(&mut self, iterations: usize) -> Vec<Vec<MEVOpportunity>> {
        let mut best_bundles = Vec::new();
        let start_time = Instant::now();
        
        for _ in 0..iterations {
            if start_time.elapsed() > Duration::from_millis(SLOT_TIME_MS / 2) {
                break;
            }
            
            let bundle = self.optimize();
            if self.evaluate_bundle_quality(&bundle) > PROFIT_THRESHOLD {
                best_bundles.push(bundle);
            }
            
            self.perturb_parameters();
        }
        
        best_bundles.sort_by(|a, b| {
            let a_profit = self.calculate_bundle_profit(a);
            let b_profit = self.calculate_bundle_profit(b);
            b_profit.cmp(&a_profit)
        });
        
        best_bundles.truncate(3);
        best_bundles
    }

    fn evaluate_bundle_quality(&self, bundle: &[MEVOpportunity]) -> u64 {
        let total_profit: u64 = bundle.iter().map(|o| o.profit_estimate).sum();
        let total_cost: u64 = bundle.iter().map(|o| o.gas_estimate + o.priority_fee).sum();
        let avg_success: f64 = bundle.iter().map(|o| o.success_probability).sum::<f64>() / bundle.len().max(1) as f64;
        
        ((total_profit.saturating_sub(total_cost) as f64) * avg_success * (1.0 - RISK_AVERSION_FACTOR)) as u64
    }

    fn calculate_bundle_profit(&self, bundle: &[MEVOpportunity]) -> u64 {
        bundle.iter()
            .map(|o| o.profit_estimate.saturating_sub(o.gas_estimate + o.priority_fee))
            .sum()
    }

    fn perturb_parameters(&mut self) {
        let perturbation = self.rng.gen_range(-0.01..0.01);
        let mut fields = self.local_fields.write();
        
        for field in fields.iter_mut() {
            *field *= 1.0 + perturbation;
        }
    }

    pub fn adaptive_temperature_update(&mut self, success_rate: f64) {
        let adjustment = if success_rate > 0.7 {
            0.98
        } else if success_rate < 0.3 {
            1.02
        } else {
            1.0
        };
        
        for temp in self.temperature_schedule.iter_mut() {
            *temp *= adjustment;
            *temp = temp.clamp(MIN_TEMPERATURE, INITIAL_TEMPERATURE);
        }
    }

    pub fn get_optimization_metrics(&self) -> OptimizationMetrics {
        let opps = self.opportunities.read();
        let cache_size = self.profit_cache.len();
        OptimizationMetrics {
            total_opportunities: opps.len(),
            cache_hit_rate: (cache_size as f64 / opps.len().max(1) as f64).min(1.0),
            average_coupling: self.calculate_average_coupling(),
            temperature_range: (MIN_TEMPERATURE, INITIAL_TEMPERATURE),
            quantum_tunneling_active: true,
            accept_rate: 0.0,
            best_energy: 0.0,
            bundle_size: 0,
        }
    }

    fn calculate_average_coupling(&self) -> f64 {
        let coupling = self.coupling_matrix.read();
        if coupling.is_empty() {
            return 0.0;
        }
        
        let sum: f64 = coupling.iter()
            .flat_map(|row| row.iter())
            .sum();
        
        sum / (coupling.len() * coupling.len()) as f64
    }
}

#[derive(Debug)]
pub struct OptimizationMetrics {
    pub total_opportunities: usize,
    pub cache_hit_rate: f64,
    pub average_coupling: f64,
    pub temperature_range: (f64, f64),
    pub quantum_tunneling_active: bool,
    pub accept_rate: f64,
    pub best_energy: f64,
    pub bundle_size: usize,
}

// Adaptive temperature ladder helper
struct TempLadder {
    temps: Vec<f64>,
    min_t: f64,
    max_t: f64,
}

impl TempLadder {
    fn adapt(&mut self, accept_rate: f64) {
        let m = if accept_rate > 0.7 { 0.98 } else if accept_rate < 0.3 { 1.02 } else { 1.0 };
        for t in &mut self.temps {
            *t = (*t * m).clamp(self.min_t, self.max_t);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use solana_sdk::pubkey::Pubkey;

    fn mk_opp(id: u64, profit: u64, gas: u64, prio: u64, deadline_slot: u64, deps: Vec<u64>) -> MEVOpportunity {
        MEVOpportunity {
            id,
            opportunity_type: OpportunityType::Arbitrage,
            input_token: Pubkey::new_unique(),
            output_token: Pubkey::new_unique(),
            profit_estimate: profit,
            gas_estimate: gas,
            priority_fee: prio,
            instructions: vec![],
            deadline_slot,
            success_probability: 0.9,
            liquidity_impact: 0.0,
            slippage_tolerance: 0.0,
            dependencies: deps,
        }
    }

    #[test]
    fn test_quantum_annealing_convergence() {
        let mut optimizer = QuantumAnnealingOptimizer::new("https://api.mainnet-beta.solana.com");
        
        let test_opportunities = vec![
            MEVOpportunity {
                id: 1,
                opportunity_type: OpportunityType::Arbitrage,
                input_token: Pubkey::new_unique(),
                output_token: Pubkey::new_unique(),
                profit_estimate: 1_000_000_000,
                gas_estimate: 100_000,
                priority_fee: 50_000,
                instructions: vec![],
                deadline_slot: 1000,
                success_probability: 0.95,
                liquidity_impact: 0.01,
                slippage_tolerance: 0.005,
                dependencies: vec![],
            },
            MEVOpportunity {
                id: 2,
                opportunity_type: OpportunityType::Liquidation,
                input_token: Pubkey::new_unique(),
                output_token: Pubkey::new_unique(),
                profit_estimate: 2_000_000_000,
                gas_estimate: 200_000,
                priority_fee: 100_000,
                instructions: vec![],
                deadline_slot: 1000,
                success_probability: 0.85,
                liquidity_impact: 0.02,
                slippage_tolerance: 0.01,
                dependencies: vec![1],
            },
        ];
        
        optimizer.update_opportunities(test_opportunities);
        let result = optimizer.optimize();
        
        assert!(!result.is_empty());
        assert!(result.len() <= MAX_BUNDLE_SIZE);
    }

    #[test]
    fn test_topo_cycle_prune() {
        // 1->2, 2->3, 3->1 cycle; 2 has worst ROI => removed
        let opps = vec![
            mk_opp(1, 1_000_000, 50_000, 10_000, 1000, vec![3]),
            mk_opp(2, 100_000, 80_000, 5_000, 1000, vec![1]),
            mk_opp(3, 800_000, 40_000, 5_000, 1000, vec![2]),
        ];
        let order = topo_sort_cycle_prune(&opps);
        assert!(!order.is_empty());
        // Ensure 2 is not in the order (worst ROI removed)
        assert!(!order.iter().any(|&i| opps[i].id == 2));
    }

    #[test]
    fn test_energy_cache_resize_and_invalidate() {
        let mut h = vec![0.1; 16];
        let cache = EnergyCache::new(1024, 512, 8192, std::time::Duration::from_millis(1), h.clone(), 0.5);
        // fill misses then hits to trigger rebalance
        for i in 0..1500u128 {
            let k = i & 0xff;
            if cache.get(k).is_none() { cache.put(k, k as f64); }
        }
        cache.maybe_rebalance();
        // drift h strongly
        for v in &mut h { *v += 1.0; }
        cache.invalidate_if_drifted(&h);
        // entries should be gone
        for i in 0..256u128 { assert!(cache.get(i).is_none()); }
    }

    #[test]
    fn test_adaptive_tunneling_bounds() {
        let J = vec![vec![0.0; 8]; 8];
        let pol = TunnelingPolicy { base: 0.05, max: 0.25, stagnation_boost: 0.02, coupling_boost: 0.3 };
        let stats = AnnealStats { last_improve_iter: 0, best_energy: 0.0 };
        let p = super::adaptive_tunneling_prob_align(5.0, 5000, &stats, 0, &J, &pol, 0.0);
        assert!(p <= pol.max && p >= 0.0);
    }

    #[test]
    fn test_priority_model_monotonicity() {
        let model = ScoreModel::new();
        let now = 10_000;
        let hi = mk_opp(1, 2_000_000_000, 100_000, 10_000, now+1, vec![]);
        let lo = mk_opp(2, 100_000_000, 80_000, 10_000, now+10, vec![]);
        assert!(model.score(&hi, now) > model.score(&lo, now));
    }

    #[test]
    fn test_budget_guard_enforces_deadline() {
        let guard = BudgetGuard { slot_time_ms: 400, anneal_frac: 0.4, hard_ceiling_ms: 150, max_flips: 10_000 };
        let start = std::time::Instant::now();
        let mut flips = 0usize;
        while guard.within(start, flips) { flips += 1; }
        assert!(start.elapsed().as_millis() as u64 <= guard.hard_ceiling_ms + 20);
    }

    #[test]
    fn test_magnetization_recompute() {
        let mut s = QaState { spins: 0b1111, n: 4, energy: 0.0, magnetization: 0.0, bundle_bits: 0 };
        let mut d = DriftTracker::new(2);
        update_bundle(&mut s);
        let m0 = s.magnetization;
        s.spins ^= 1 << 0; d.on_flip();
        s.spins ^= 1 << 1; d.on_flip();
        assert!(d.should_recompute());
        d.recompute(&mut s);
        assert_ne!(m0, s.magnetization);
    }

    #[test]
    fn test_priority_scoring_complete_terms() {
        let model = ScoreModel::new();
        let now = 1_000u64;
        let hi = mk_opp(1, 2_000_000_000, 100_000, 10_000, now+1, vec![]);
        let lo = mk_opp(2, 100_000_000,   200_000, 50_000, now+50, vec![]);
        assert!(model.score_with_slot(&hi, now) > model.score_with_slot(&lo, now));
    }

    #[test]
    fn test_conflict_detector_flags_overlap() {
        let acc = Pubkey::new_unique();
        let ix1 = Instruction { program_id: Pubkey::new_unique(), accounts: vec![solana_sdk::instruction::AccountMeta{ pubkey: acc, is_signer:false, is_writable:true }], data: vec![] };
        let ix2 = Instruction { program_id: Pubkey::new_unique(), accounts: vec![solana_sdk::instruction::AccountMeta{ pubkey: acc, is_signer:false, is_writable:true }], data: vec![] };
        let o1 = MEVOpportunity{ id:1, opportunity_type: OpportunityType::Arbitrage, input_token: Pubkey::new_unique(), output_token: Pubkey::new_unique(), profit_estimate: 1, gas_estimate:1, priority_fee:1, instructions: vec![ix1], deadline_slot: 1_000, success_probability: 1.0, liquidity_impact:0.0, slippage_tolerance:0.0, dependencies: vec![] };
        let o2 = MEVOpportunity{ id:2, opportunity_type: OpportunityType::Arbitrage, input_token: Pubkey::new_unique(), output_token: Pubkey::new_unique(), profit_estimate: 1, gas_estimate:1, priority_fee:1, instructions: vec![ix2], deadline_slot: 1_000, success_probability: 1.0, liquidity_impact:0.0, slippage_tolerance:0.0, dependencies: vec![] };
        assert!(bundle_conflicts_fast(&[o1,o2], 0b11));
    }
}
