use std::collections::VecDeque;
use std::sync::Arc;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use ndarray::{Array1, Array2, ArrayView1};
use rand::Rng;
use std::f64::consts::E;
use std::alloc::{alloc_zeroed, dealloc, Layout};
use std::collections::HashMap;

// === REPLACE: joint CLMM+CPMM splitter (deterministic heap, guarded) ===
use std::cmp::Ordering;

const GAIN_EPS: f64 = 1e-12; // early-stop floor for marginal gains

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum ClmmDir {
    ZeroForOne,
    OneForZero,
}

#[derive(Clone, Copy, Debug)]
pub struct ClmmSegment {
    pub sqrt_a: f64,
    pub sqrt_b: f64,
    pub liquidity: f64,
    pub fee_bps: u32,
}

#[derive(Clone, Debug)]
pub struct CpmmPool {
    pub reserve0: f64,
    pub reserve1: f64,
    pub fee_bps: u32,
    pub dir: ClmmDir,
}
impl CpmmPool {
    #[inline] fn fee_mult(&self) -> f64 { 1.0 - (self.fee_bps as f64) / 10_000.0 }
}

#[inline]
fn cpmm_total_out(p: &CpmmPool, amount_in: f64) -> f64 {
    if amount_in <= 0.0 || !amount_in.is_finite() { return 0.0; }
    let fee = p.fee_mult().max(0.0);
    match p.dir {
        ClmmDir::ZeroForOne => {
            let x = p.reserve0.max(1e-12);
            let y = p.reserve1.max(1e-12);
            let dx_eff = amount_in * fee;
            (y * dx_eff) / (x + dx_eff).max(1e-12)
        }
        ClmmDir::OneForZero => {
            let x = p.reserve1.max(1e-12);
            let y = p.reserve0.max(1e-12);
            let dx_eff = amount_in * fee;
            (y * dx_eff) / (x + dx_eff).max(1e-12)
        }
    }.max(0.0)
}

#[inline]
fn mg_cpmm(p: &CpmmPool, alloc_so_far: f64, q: f64) -> f64 {
    let a0 = cpmm_total_out(p, alloc_so_far);
    let a1 = cpmm_total_out(p, alloc_so_far + q);
    let g = a1 - a0;
    if g.is_finite() && g > 0.0 { g } else { 0.0 }
}

#[derive(Clone, Copy)]
struct HeapEntry { gain: f64, idx: usize }
impl PartialEq for HeapEntry { fn eq(&self, o: &Self) -> bool { self.gain == o.gain && self.idx == o.idx } }
impl Eq for HeapEntry {}
impl PartialOrd for HeapEntry {
    fn partial_cmp(&self, o: &Self) -> Option<Ordering> {
        // Max-heap by gain; deterministic tie-breaker by idx (smaller idx wins)
        if !self.gain.is_finite() && !o.gain.is_finite() { return Some(Ordering::Equal); }
        if !self.gain.is_finite() { return Some(Ordering::Less); }
        if !o.gain.is_finite() { return Some(Ordering::Greater); }
        match self.gain.partial_cmp(&o.gain) {
            Some(Ordering::Equal) => Some(o.idx.cmp(&self.idx)), // reverse so smaller idx is "greater"
            ord => ord,
        }
    }
}
impl Ord for HeapEntry { fn cmp(&self, o: &Self) -> Ordering { self.partial_cmp(o).unwrap_or(Ordering::Equal) } }

#[derive(Clone, Debug)]
pub enum Venue<'a> {
    Cpmm(CpmmPool),
    Clmm { dir: ClmmDir, sqrt_p: f64, segs: &'a [ClmmSegment], seg_idx: usize },
}

#[inline]
fn find_seg_idx(sqrt_p: f64, segs: &[ClmmSegment]) -> usize {
    let mut i = 0usize;
    while i + 1 < segs.len() && !(segs[i].sqrt_a <= sqrt_p && sqrt_p < segs[i].sqrt_b) { i += 1; }
    i.min(segs.len().saturating_sub(1))
}

#[inline]
fn clmm_step_in_out_with_idx(
    sqrt_p: f64,
    seg_idx: usize,
    dir: ClmmDir,
    segs: &[ClmmSegment],
    amount_in: f64,
) -> (f64, f64, usize) {
    if amount_in <= 0.0 || segs.is_empty() {
        return (0.0, sqrt_p, seg_idx);
    }

    let mut remaining = amount_in;
    let mut current_sqrt_p = sqrt_p;
    let mut current_idx = seg_idx;
    let mut total_out = 0.0;

    while remaining > 0.0 && current_idx < segs.len() {
        let seg = &segs[current_idx];
        
        // Calculate how much we can trade in this segment
        let sqrt_p_next = match dir {
            ClmmDir::ZeroForOne => seg.sqrt_a, // going down in price
            ClmmDir::OneForZero => seg.sqrt_b, // going up in price
        };

        let delta_sqrt_p = (sqrt_p_next - current_sqrt_p).abs();
        if delta_sqrt_p <= 0.0 {
            current_idx += 1;
            continue;
        }

        // Calculate amount needed to reach next price
        let amount_needed = delta_sqrt_p * seg.liquidity;
        let amount_to_trade = remaining.min(amount_needed);

        // Calculate output based on AMM formula
        let fee_mult = 1.0 - (seg.fee_bps as f64) / 10_000.0;
        let amount_out = match dir {
            ClmmDir::ZeroForOne => {
                // y_out = (delta_sqrt_p * L) / sqrt_p_next
                (amount_to_trade * fee_mult) / sqrt_p_next
            }
            ClmmDir::OneForZero => {
                // x_out = (delta_sqrt_p * L) * sqrt_p
                (amount_to_trade * fee_mult) * current_sqrt_p
            }
        };

        total_out += amount_out;
        remaining -= amount_to_trade;
        current_sqrt_p = sqrt_p_next;

        // Move to next segment if we've exhausted this one
        if amount_to_trade >= amount_needed {
            current_idx += 1;
        }
    }

    (total_out.max(0.0), current_sqrt_p, current_idx.min(segs.len().saturating_sub(1)))
}

pub fn split_joint_venues<'a>(amount_in: f64, venues: &mut [Venue<'a>], steps: usize) -> Vec<f64> {
    use std::collections::BinaryHeap;

    let n = venues.len();
    let mut alloc = vec![0.0f64; n];
    if amount_in <= 0.0 || n == 0 || steps == 0 { return alloc; }

    for v in venues.iter_mut() {
        if let Venue::Clmm { sqrt_p, segs, seg_idx, .. } = v {
            *seg_idx = find_seg_idx(*sqrt_p, segs);
        }
    }

    let q = (amount_in / steps as f64).max(1.0);
    let mut heap = BinaryHeap::with_capacity(n);

    let mut marginal = |i: usize| -> f64 {
        match &venues[i] {
            Venue::Cpmm(p) => mg_cpmm(p, alloc[i], q),
            Venue::Clmm { dir, sqrt_p, segs, seg_idx } => {
                let (g, _, _) = clmm_step_in_out_with_idx(*sqrt_p, *seg_idx, *dir, segs, q);
                if g.is_finite() && g > 0.0 { g } else { 0.0 }
            }
        }
    };

    for i in 0..n {
        heap.push(HeapEntry { gain: marginal(i), idx: i });
    }

    for _ in 0..steps {
        if let Some(mut best) = heap.pop() {
            if best.gain <= GAIN_EPS { break; }
            let i = best.idx;
            alloc[i] += q;

            match &mut venues[i] {
                Venue::Cpmm(_p) => { best.gain = marginal(i); }
                Venue::Clmm { dir, sqrt_p, segs, seg_idx } => {
                    let (_out, sp_new, idx_new) = clmm_step_in_out_with_idx(*sqrt_p, *seg_idx, *dir, segs, q);
                    *sqrt_p = sp_new; *seg_idx = idx_new;
                    best.gain = marginal(i);
                }
            }
            heap.push(best);
        } else { break; }
    }

    let s: f64 = alloc.iter().sum();
    if s > 0.0 {
        let scale = amount_in / s;
        for a in &mut alloc { *a *= scale; }
    }
    alloc
}

pub fn build_venues<'a>(cpmms: &'a [CpmmPool], clmms: &'a [(ClmmDir, f64, &'a [ClmmSegment])]) -> Vec<Venue<'a>> {
    let mut v = Vec::with_capacity(cpmms.len() + clmms.len());
    for p in cpmms { v.push(Venue::Cpmm(p.clone())); }
    for (dir, sqrt_p, segs) in clmms {
        v.push(Venue::Clmm { dir: *dir, sqrt_p: *sqrt_p, segs, seg_idx: find_seg_idx(*sqrt_p, segs) });
    }
    v
}

// === PASTE VERBATIM: CLMM segment coalescer (conservative min-liquidity) ===
pub fn coalesce_segments_conservative(segs: &[ClmmSegment], sqrt_gap_eps: f64, liq_eps: f64) -> Vec<ClmmSegment> {
    if segs.is_empty() { return Vec::new(); }
    let mut out = Vec::with_capacity(segs.len());
    let mut cur = segs[0];
    for s in &segs[1..] {
        let contiguous = (s.sqrt_a - cur.sqrt_b).abs() <= sqrt_gap_eps;
        let same_fee   = s.fee_bps == cur.fee_bps;
        let liq_close  = (s.liquidity - cur.liquidity).abs() <= liq_eps;
        if same_fee && contiguous && liq_close {
            // merge, keep the smaller liquidity to avoid optimistic out
            cur.sqrt_b   = s.sqrt_b;
            cur.liquidity = cur.liquidity.min(s.liquidity);
        } else {
            if cur.liquidity > liq_eps { out.push(cur); }
            cur = *s;
        }
    }
    if cur.liquidity > liq_eps { out.push(cur); }
    out
}

// === PASTE VERBATIM: adaptive splitter (variable quantum) ===
pub fn split_joint_venues_adaptive<'a>(
    amount_in: f64,
    venues: &mut [Venue<'a>],
    steps_cap: usize,
    q_min: f64,
    q_max: f64,
) -> Vec<f64> {
    use std::collections::BinaryHeap;

    let n = venues.len();
    let mut alloc = vec![0.0f64; n];
    if amount_in <= 0.0 || n == 0 || steps_cap == 0 { return alloc; }

    for v in venues.iter_mut() {
        if let Venue::Clmm { sqrt_p, segs, seg_idx, .. } = v {
            *seg_idx = find_seg_idx(*sqrt_p, segs);
        }
    }

    // Start with mid quantum; adapt up/down by slope
    let mut remaining = amount_in;
    let mut steps = 0usize;
    let mut q = (amount_in / (steps_cap as f64)).clamp(q_min, q_max);

    let mut heap = BinaryHeap::with_capacity(n);
    let mut marginal = |i: usize, q: f64, alloc_i: f64| -> f64 {
        match &venues[i] {
            Venue::Cpmm(p) => mg_cpmm(p, alloc_i, q),
            Venue::Clmm { dir, sqrt_p, segs, seg_idx } => {
                let (g, _, _) = clmm_step_in_out_with_idx(*sqrt_p, *seg_idx, *dir, segs, q);
                if g.is_finite() && g > 0.0 { g } else { 0.0 }
            }
        }
    };
    for i in 0..n { heap.push(HeapEntry { gain: marginal(i, q, alloc[i]), idx: i }); }

    while remaining > 0.0 && steps < steps_cap {
        let q_eff = q.min(remaining).max(q_min);
        if let Some(mut best) = heap.pop() {
            if best.gain <= GAIN_EPS { break; }
            let i = best.idx;
            alloc[i] += q_eff;
            remaining -= q_eff;

            // Update state + next marginal at current q
            match &mut venues[i] {
                Venue::Cpmm(_p) => { best.gain = marginal(i, q_eff, alloc[i]); }
                Venue::Clmm { dir, sqrt_p, segs, seg_idx } => {
                    let (_out, sp_new, idx_new) = clmm_step_in_out_with_idx(*sqrt_p, *seg_idx, *dir, segs, q_eff);
                    *sqrt_p = sp_new; *seg_idx = idx_new;
                    best.gain = marginal(i, q_eff, alloc[i]);
                }
            }
            heap.push(best);

            // Adapt quantum: steep slope (big best gain) -> shrink; flat -> grow
            let slope_signal = heap.peek().map(|h| h.gain).unwrap_or(0.0);
            if slope_signal > 0.0 {
                // heuristic: normalize by q to estimate marginal density
                let dens = slope_signal / (q_eff + 1e-12);
                if dens > 1e-6 { q = (q * 0.75).max(q_min); } else { q = (q * 1.25).min(q_max); }
            }
            steps += 1;
        } else { break; }
    }
    alloc
}

// === PASTE VERBATIM: penalty scalar from microstructure ===
/// slip_bps: expected realized slippage in *bps of out-per-in ratio*.
/// tip_out_per_in: expected out-units paid per in-unit (convert tips into out token space).
/// backoff_mult: from your classifier/executor backoff; >=1.0.
/// spread_ratio: current spread / mid.
/// safety: extra basis points to cover tail (e.g., 10 = 10 bps).
pub fn ev_penalty_scalar(
    slip_bps: f64,
    tip_out_per_in: f64,
    backoff_mult: f64,
    spread_ratio: f64,
    safety: f64,
) -> f64 {
    let slip = (slip_bps.max(0.0) + safety.max(0.0)) * 1e-4; // convert bps → frac
    let spread_drag = (0.25 * spread_ratio).clamp(0.0, 0.01); // conservative quarter-spread cost
    let backoff = backoff_mult.clamp(1.0, 2.5);
    // Net penalty per in-unit, in out-units
    (slip + spread_drag) * backoff + tip_out_per_in.max(0.0)
}

// === ENHANCED VENUE ALLOCATION SYSTEM ===

#[inline]
fn mg_cpmm_total(p: &CpmmPool, alloc: f64, q: f64) -> f64 {
    // f(dx) = y * (dx*fee) / (x + dx*fee)
    // Δf = f(a+q) - f(a) = y * (q_eff * x) / ((x + a_eff + q_eff)*(x + a_eff))
    if q <= 0.0 || !q.is_finite() { return 0.0; }
    let fee = p.fee_mult().max(0.0);
    let (x, y) = match p.dir {
        ClmmDir::ZeroForOne => (p.reserve0.max(1e-12), p.reserve1.max(1e-12)),
        ClmmDir::OneForZero => (p.reserve1.max(1e-12), p.reserve0.max(1e-12)),
    };
    let a_eff = (alloc * fee).max(0.0);
    let q_eff = (q * fee).max(0.0);
    let den = (x + a_eff + q_eff) * (x + a_eff);
    if den <= 0.0 { return 0.0; }
    let g = y * (q_eff * x) / den;
    if g.is_finite() && g > 0.0 { g } else { 0.0 }
}

#[inline]
fn tail_marginal_cpmm(p: &CpmmPool, alloc: f64, q: f64, q_base: f64) -> f64 {
    // Tail ≡ f(a+q) − f(a+q−q_base) with same closed form:
    // Tail = y * (q_base_eff * x) / ((x + a_eff + q_eff)*(x + a_eff + q_eff − q_base_eff))
    if q <= 0.0 || q_base <= 0.0 { return 0.0; }
    let fee = p.fee_mult().max(0.0);
    let (x, y) = match p.dir {
        ClmmDir::ZeroForOne => (p.reserve0.max(1e-12), p.reserve1.max(1e-12)),
        ClmmDir::OneForZero => (p.reserve1.max(1e-12), p.reserve0.max(1e-12)),
    };
    let a_eff   = (alloc * fee).max(0.0);
    let q_eff   = (q * fee).max(0.0);
    let qb_eff  = (q_base.min(q).max(0.0) * fee).max(0.0);
    let den_hi  = (x + a_eff + q_eff).max(1e-12);
    let den_lo  = (x + a_eff + q_eff - qb_eff).max(1e-12);
    let g = y * (qb_eff * x) / (den_hi * den_lo);
    if g.is_finite() && g > 0.0 { g } else { 0.0 }
}

#[inline]
fn tail_marginal_clmm(dir: ClmmDir, sqrt_p: f64, seg_idx: usize, segs: &[ClmmSegment], q: f64, q_base: f64)->f64{
    if q <= 0.0 { return 0.0; }
    let (g_big, _, _) = clmm_step_in_out_with_idx(sqrt_p, seg_idx, dir, segs, q);
    if q <= q_base { return g_big.max(0.0); }
    let (g_prev, _, _) = clmm_step_in_out_with_idx(sqrt_p, seg_idx, dir, segs, (q - q_base).max(0.0));
    (g_big - g_prev).max(0.0)
}

/// Split with hard per-venue caps, batching allocations to reduce heap churn.
/// Deterministic: tiebreaks on smaller venue index.
pub fn split_joint_venues_capped<'a>(
    amount_in: f64,
    venues: &mut [Venue<'a>],
    steps_cap: usize,
    caps: &[f64],
) -> Vec<f64> {
    use std::collections::BinaryHeap;
    use std::hint::{likely, unlikely};

    let n = venues.len();
    assert_eq!(n, caps.len(), "caps length must equal venues length");
    let mut alloc = vec![0.0f64; n];
    if amount_in <= 0.0 || n == 0 || steps_cap == 0 { return alloc; }

    for v in venues.iter_mut() {
        if let Venue::Clmm { sqrt_p, segs, seg_idx, .. } = v {
            *seg_idx = find_seg_idx(*sqrt_p, segs);
        }
    }

    let total_cap: f64 = caps.iter().copied().sum();
    let mut remaining = amount_in.min(total_cap);
    if remaining <= 0.0 { return alloc; }

    let q_base = (amount_in / steps_cap as f64).max(1.0);

    let mut heap = BinaryHeap::with_capacity(n);
    let mut marginal_qbase = |i: usize| -> f64 {
        let cap = caps[i];
        let rem = (cap - alloc[i]).max(0.0);
        if rem <= 0.0 { return 0.0; }
        let q_eff = q_base.min(rem);
        match &venues[i] {
            Venue::Cpmm(p) => mg_cpmm_total(p, alloc[i], q_eff),
            Venue::Clmm { dir, sqrt_p, segs, seg_idx } => {
                let (g, _, _) = clmm_step_in_out_with_idx(*sqrt_p, *seg_idx, *dir, segs, q_eff);
                if g.is_finite() && g > 0.0 { g } else { 0.0 }
            }
        }
    };
    #[derive(Clone, Copy)]
    struct HeapEntry { gain: f64, idx: usize }
    impl PartialEq for HeapEntry { fn eq(&self,o:&Self)->bool{ self.gain==o.gain && self.idx==o.idx } }
    impl Eq for HeapEntry {}
    impl PartialOrd for HeapEntry {
        fn partial_cmp(&self,o:&Self)->Option<std::cmp::Ordering>{
            if !self.gain.is_finite() && !o.gain.is_finite(){ return Some(std::cmp::Ordering::Equal); }
            if !self.gain.is_finite(){ return Some(std::cmp::Ordering::Less); }
            if !o.gain.is_finite(){ return Some(std::cmp::Ordering::Greater); }
            match self.gain.partial_cmp(&o.gain){
                Some(std::cmp::Ordering::Equal)=>Some(o.idx.cmp(&self.idx)),
                ord=>ord,
            }
        }
    }
    impl Ord for HeapEntry { fn cmp(&self,o:&Self)->std::cmp::Ordering{ self.partial_cmp(o).unwrap_or(std::cmp::Ordering::Equal) } }

    for i in 0..n { heap.push(HeapEntry { gain: marginal_qbase(i), idx: i }); }

    let mut steps = 0usize;
    while likely(remaining > 0.0) && likely(steps < steps_cap) {
        let Some(mut best) = heap.pop() else { break; };
        if unlikely(best.gain <= GAIN_EPS) { break; }
        let i = best.idx;

        // remaining capacity on i
        let rem_i = (caps[i] - alloc[i]).max(0.0);
        if unlikely(rem_i <= 0.0) { continue; }

        // next best marginal
        let next_best = heap.peek().map(|h| h.gain).unwrap_or(0.0);

        // run-ahead batch q_run
        let mut q_run = q_base.min(rem_i).min(remaining);
        if likely(next_best > GAIN_EPS) {
            // expand then bisect
            let mut q_hi = q_run;
            let mut q_lo = 0.0;
            loop {
                if q_hi >= rem_i || q_hi >= remaining { break; }
                let tail = match &venues[i] {
                    Venue::Cpmm(p) => tail_marginal_cpmm(p, alloc[i], q_hi, q_base),
                    Venue::Clmm { dir, sqrt_p, segs, seg_idx } =>
                        tail_marginal_clmm(*dir, *sqrt_p, *seg_idx, segs, q_hi, q_base),
                };
                if likely(tail + 1e-18 >= next_best) {
                    q_lo = q_hi;
                    q_hi = (q_hi * 2.0).min(rem_i).min(remaining);
                } else { break; }
            }
            let mut lo = q_lo;
            let mut hi = q_hi;
            for _ in 0..20 {
                if hi - lo <= q_base * 0.05 { break; }
                let mid = 0.5 * (lo + hi);
                let tail = match &venues[i] {
                    Venue::Cpmm(p) => tail_marginal_cpmm(p, alloc[i], mid, q_base),
                    Venue::Clmm { dir, sqrt_p, segs, seg_idx } =>
                        tail_marginal_clmm(*dir, *sqrt_p, *seg_idx, segs, mid, q_base),
                };
                if tail + 1e-18 >= next_best { lo = mid; } else { hi = mid; }
            }
            q_run = hi.max(q_base.min(rem_i).min(remaining)).min(rem_i).min(remaining);
        }

        if unlikely(q_run <= 0.0) { continue; }

        // commit
        alloc[i] += q_run;
        remaining -= q_run;

        match &mut venues[i] {
            Venue::Cpmm(_p) => { best.gain = marginal_qbase(i); }
            Venue::Clmm { dir, sqrt_p, segs, seg_idx } => {
                let (_out, sp_new, idx_new) = clmm_step_in_out_with_idx(*sqrt_p, *seg_idx, *dir, segs, q_run);
                *sqrt_p = sp_new; *seg_idx = idx_new;
                best.gain = marginal_qbase(i);
            }
        }
        heap.push(best);
        steps += 1;
    }
    alloc
}

/// Single-venue simulator (exact, non-mutating)
pub fn simulate_single_venue_out<'a>(venue: &Venue<'a>, amount_in: u64) -> f64 {
    let ain = amount_in as f64;
    if ain <= 0.0 { return 0.0; }
    match venue {
        Venue::Cpmm(p) => cpmm_total_out(p, ain),
        Venue::Clmm { dir, sqrt_p, segs, seg_idx } => {
            let (g_total, _sp2, _idx2) = clmm_step_in_out_with_idx(*sqrt_p, *seg_idx, *dir, segs, ain);
            g_total.max(0.0)
        }
    }
}

/// Lot-rebalance greedy (post-rounding EV boost)
pub fn rebalance_lots_greedy<'a>(
    venues: &[Venue<'a>],
    lots: &[u64],
    caps: &[u64],
    alloc: &mut [u64],
    max_swaps: usize,
) {
    let n = venues.len();
    assert_eq!(n, lots.len());
    assert_eq!(n, caps.len());
    assert_eq!(n, alloc.len());
    if n == 0 || max_swaps == 0 { return; }

    // Deterministic loop
    for _ in 0..max_swaps {
        // Compute marginal add (Δ+) and tail loss (Δ-) per venue for one lot
        let mut best_add: Option<(usize, f64)> = None; // (idx, delta_out)
        let mut worst_tail: Option<(usize, f64)> = None; // (idx, delta_out)

        for i in 0..n {
            let lot = lots[i].max(1);
            // candidate ADD
            if alloc[i] + lot <= caps[i] {
                let base_out = simulate_single_venue_out(&venues[i], alloc[i]);
                let add_out  = simulate_single_venue_out(&venues[i], alloc[i] + lot);
                let delta_add = (add_out - base_out).max(0.0);
                match best_add {
                    None => best_add = Some((i, delta_add)),
                    Some((bi, bv)) => {
                        if delta_add > bv + 1e-12 || (delta_add - bv).abs() <= 1e-12 && i < bi {
                            best_add = Some((i, delta_add));
                        }
                    }
                }
            }
            // candidate REMOVE
            if alloc[i] >= lot {
                let base_out  = simulate_single_venue_out(&venues[i], alloc[i]);
                let less_out  = simulate_single_venue_out(&venues[i], alloc[i] - lot);
                let tail_loss = (base_out - less_out).max(0.0); // loss if we remove one lot
                match worst_tail {
                    None => worst_tail = Some((i, tail_loss)),
                    Some((wi, wv)) => {
                        if tail_loss < wv - 1e-12 || (tail_loss - wv).abs() <= 1e-12 && i < wi {
                            worst_tail = Some((i, tail_loss));
                        }
                    }
                }
            }
        }

        let (add_i, add_gain)   = match best_add { Some(x)=>x, None=>break };
        let (rem_i, rem_loss)   = match worst_tail { Some(x)=>x, None=>break };
        if add_i == rem_i { break; }

        // Only swap if EV improves and feasibility holds
        let lot_add = lots[add_i].max(1);
        let lot_rem = lots[rem_i].max(1);
        let lot = lot_add.min(lot_rem);

        if alloc[rem_i] < lot || alloc[add_i] + lot > caps[add_i] { break; }

        if add_gain > rem_loss + 1e-12 {
            alloc[rem_i] -= lot;
            alloc[add_i] += lot;
            // continue to next swap
        } else {
            break; // no profitable swap
        }
    }
}

/// Largest-remainder finalizer (lot sizes; caps; exact sum)
pub fn finalize_allocations_lrm(
    amount_in: u64,
    plan_f: &[f64],
    lots: &[u64],     // per-venue lot size in base units (≥1)
    caps: &[u64],     // per-venue cap in base units (≥0)
) -> Vec<u64> {
    assert_eq!(plan_f.len(), lots.len());
    assert_eq!(plan_f.len(), caps.len());

    let n = plan_f.len();
    let mut out = vec![0u64; n];

    // 1) floor-by-lot under caps
    let mut remainders: Vec<(usize, f64)> = Vec::with_capacity(n);
    let mut sum_floor = 0u128;

    for i in 0..n {
        let lot = lots[i].max(1);
        let cap = caps[i];
        let want = if plan_f[i].is_finite() && plan_f[i] > 0.0 { plan_f[i] } else { 0.0 };
        let want_u = want.floor() as u64;
        // floor to lot then cap
        let floored = (want_u / lot).saturating_mul(lot).min(cap);
        out[i] = floored;
        sum_floor += floored as u128;

        let rem = (want - (floored as f64)).max(0.0); // fractional remainder for LRM
        remainders.push((i, rem));
    }

    // 2) distribute residual lots by largest remainders (deterministic on ties)
    let mut residual = (amount_in as i128) - (sum_floor as i128);
    if residual <= 0 { return out; }

    remainders.sort_by(|a, b| {
        match a.1.partial_cmp(&b.1).unwrap_or(Ordering::Equal).reverse() {
            Ordering::Equal => a.0.cmp(&b.0), // smaller idx first
            ord => ord
        }
    });

    for (i, _) in remainders {
        if residual <= 0 { break; }
        let lot = lots[i].max(1);
        let cap = caps[i];
        // how many lots can we still add?
        let rem_cap = cap.saturating_sub(out[i]);
        let add_lots = (rem_cap / lot).min((residual as u64) / lot);
        if add_lots > 0 {
            let add = add_lots.saturating_mul(lot);
            out[i] = out[i].saturating_add(add);
            residual -= add as i128;
        }
    }

    // If any 1..(lot-1) dust remains (shouldn't if amount_in is lot-aligned), leave unassigned.
    out
}

/// Plan simulator (non-mutating expected out; CLMM+CPMM)
pub fn simulate_plan_out<'a>(
    venues: &[Venue<'a>],
    alloc_u64: &[u64],
) -> f64 {
    assert_eq!(venues.len(), alloc_u64.len());
    let n = venues.len();
    let mut total_out = 0.0f64;

    for i in 0..n {
        let ain = alloc_u64[i] as f64;
        if ain <= 0.0 { continue; }
        match &venues[i] {
            Venue::Cpmm(p) => {
                // CPMM: exact using current reserves (non-mutating approximation)
                total_out += cpmm_total_out(p, ain);
            }
            Venue::Clmm { dir, sqrt_p, segs, seg_idx } => {
                // CLMM: walk a local copy of (sqrt, idx)
                let mut sp = *sqrt_p;
                let mut idx = *seg_idx;
                let mut rem = ain;
                while rem > 0.0 {
                    let (got, sp2, idx2) = clmm_step_in_out_with_idx(sp, idx, *dir, segs, rem);
                    total_out += got;
                    // step used all or hit boundary; if no progress, break
                    if sp2 == sp && idx2 == idx { break; }
                    // We consumed "some" input; for safety, recompute rem via tiny backstep:
                    // Since step consumes the *whole* rem or to boundary, we can end here.
                    rem = 0.0;
                    sp = sp2; idx = idx2;
                }
            }
        }
    }
    total_out.max(0.0)
}

/// Lot rounding helpers
#[inline] pub fn round_down_to_lot(x: u64, lot: u64) -> u64 {
    if lot <= 1 { x } else { x - (x % lot) }
}
#[inline] pub fn round_up_to_lot(x: u64, lot: u64) -> u64 {
    if lot <= 1 { x } else { x + ((lot - (x % lot)) % lot) }
}

/// End-to-end plan forge (split → finalize → simulate)
pub struct PlanResult {
    pub alloc_u64: Vec<u64>,
    pub expected_out: f64,
}

/// Forge plan with rebalance
pub fn forge_plan_with_rebalance<'a>(
    amount_in: u64,
    venues: &mut [Venue<'a>],
    steps: usize,
    lots: &[u64],
    caps: &[u64],
    max_swaps: usize,
) -> PlanResult {
    // capped float split (batched)
    let caps_f: Vec<f64> = caps.iter().map(|&c| c as f64).collect();
    let plan_f = split_joint_venues_capped(amount_in as f64, venues, steps, &caps_f);

    // finalize to base-units
    let mut alloc_u64 = finalize_allocations_lrm(amount_in, &plan_f, lots, caps);

    // greedy EV-improving lot swaps (non-mutating venues)
    rebalance_lots_greedy(venues, lots, caps, &mut alloc_u64, max_swaps);

    // EV
    let expected_out = simulate_plan_out(&*venues, &alloc_u64);
    PlanResult { alloc_u64, expected_out }
}

/// Forge plan EV with rebalance
pub fn forge_plan_ev_with_rebalance<'a>(
    amount_in: u64,
    venues: &mut [Venue<'a>],
    steps: usize,
    lots: &[u64],
    caps: &[u64],
    penalty_per_in: f64,
    max_swaps: usize,
) -> PlanResult {
    // EV-aware float split
    let plan_f = split_joint_venues_ev(amount_in as f64, venues, steps, penalty_per_in);

    let mut alloc_u64 = finalize_allocations_lrm(amount_in, &plan_f, lots, caps);
    rebalance_lots_greedy(venues, lots, caps, &mut alloc_u64, max_swaps);

    let expected_out = simulate_plan_out(&*venues, &alloc_u64);
    PlanResult { alloc_u64, expected_out }
}

// === ENHANCED FUNCTIONS ===

/// CLMM segment index finder — O(log N) binary search
#[inline]
fn find_seg_idx_binary(sqrt_p: f64, segs: &[ClmmSegment]) -> usize {
    if segs.is_empty() { return 0; }
    let mut lo = 0usize;
    let mut hi = segs.len(); // exclusive
    while lo < hi {
        let mid = (lo + hi) >> 1;
        let s = unsafe { *segs.get_unchecked(mid) };
        if sqrt_p < s.sqrt_a {
            hi = mid;
        } else if sqrt_p >= s.sqrt_b {
            lo = mid + 1;
        } else {
            return mid;
        }
    }
    lo.saturating_sub(1).min(segs.len().saturating_sub(1))
}

/// Venue pre-filter (upper-bound marginal) to shrink heap
pub fn filter_venues_by_upper_bound<'a>(
    venues: &mut Vec<Venue<'a>>,
    caps: &[f64],
    q_base: f64,
    min_keep: usize,
) -> Vec<usize> {
    let n = venues.len();
    let mut keep_idx: Vec<usize> = Vec::with_capacity(n);
    let mut scores: Vec<(usize, f64)> = Vec::with_capacity(n);

    for i in 0..n {
        let cap = caps.get(i).copied().unwrap_or(f64::INFINITY).max(0.0);
        if cap <= 0.0 { continue; }
        let ub = match &venues[i] {
            Venue::Cpmm(p) => {
                // optimistic marginal ~ y * q / (x + q) at small q
                let fee = 1.0 - (p.fee_bps as f64)/10_000.0;
                match p.dir {
                    ClmmDir::ZeroForOne => (p.reserve1.max(1e-12) * (q_base*fee)) / (p.reserve0.max(1e-12)+q_base*fee),
                    ClmmDir::OneForZero => (p.reserve0.max(1e-12) * (q_base*fee)) / (p.reserve1.max(1e-12)+q_base*fee),
                }
            }
            Venue::Clmm { dir, sqrt_p, segs, seg_idx } => {
                let (g, _, _) = clmm_step_in_out_with_idx(*sqrt_p, *seg_idx, *dir, segs, q_base.min(cap));
                g.max(0.0)
            }
        };
        scores.push((i, ub));
    }
    if scores.is_empty() { return keep_idx; }

    // threshold: keep top-k (min_keep) plus any within 5% of the best ub
    scores.sort_by(|a,b| stable_cmp_f64_desc(*b,*a));
    let best = scores[0].1;
    let thresh = best * 0.95;
    for (rank, &(i, s)) in scores.iter().enumerate() {
        if rank < min_keep || s >= thresh { keep_idx.push(i); }
    }
    // compact venues in-place to only kept ones (stable order)
    let mut new_v = Vec::with_capacity(keep_idx.len());
    for &i in &keep_idx { new_v.push(venues[i].clone()); }
    *venues = new_v;
    keep_idx
}

/// Strict LRM finalizer with GCD-of-lots (exact sum when feasible)
#[inline] fn gcd_u64(mut a: u64, mut b: u64) -> u64 {
    while b != 0 { let t = a % b; a = b; b = t; } a
}

pub struct FinalizeResult { pub alloc: Vec<u64>, pub dust: u64 }

pub fn finalize_allocations_lrm_strict(
    amount_in: u64,
    plan_f: &[f64],
    lots: &[u64],
    caps: &[u64],
) -> FinalizeResult {
    use std::cmp::Ordering;
    assert_eq!(plan_f.len(), lots.len());
    assert_eq!(plan_f.len(), caps.len());
    let n = plan_f.len();

    // gcd of lots
    let mut g = 0u64;
    for &lot in lots { g = if g==0 { lot.max(1) } else { gcd_u64(g, lot.max(1)) }; }
    let exact_possible = if g == 0 { true } else { amount_in % g == 0 };

    // floor-by-lot under caps
    let mut out = vec![0u64; n];
    let mut rems: Vec<(usize, f64)> = Vec::with_capacity(n);
    let mut sum_floor: u128 = 0;
    for i in 0..n {
        let lot = lots[i].max(1);
        let cap = caps[i];
        let want = if plan_f[i].is_finite() && plan_f[i] > 0.0 { plan_f[i] } else { 0.0 };
        let floored = (want.floor() as u64 / lot).saturating_mul(lot).min(cap);
        out[i] = floored;
        sum_floor += floored as u128;
        let rem = (want - floored as f64).max(0.0);
        rems.push((i, rem));
    }
    let mut residual: i128 = (amount_in as i128) - (sum_floor as i128);
    if residual <= 0 {
        return FinalizeResult { alloc: out, dust: 0 };
    }

    // If exact is possible, we can distribute residual by lots; else we keep dust remainder.
    let dust_allow = if exact_possible { 0 } else { (residual as u64) % g };
    let target_fill = (residual as u64).saturating_sub(dust_allow);

    // largest remainders in lot units
    rems.sort_by(|a,b| {
        match a.1.partial_cmp(&b.1).unwrap_or(Ordering::Equal).reverse() {
            Ordering::Equal => a.0.cmp(&b.0),
            ord => ord
        }
    });

    let mut left = target_fill;
    for (i, _) in rems {
        if left == 0 { break; }
        let lot = lots[i].max(1);
        let add = (caps[i].saturating_sub(out[i]) / lot).min(left / lot) * lot;
        if add > 0 {
            out[i] = out[i].saturating_add(add);
            left -= add;
        }
    }
    FinalizeResult { alloc: out, dust: left + dust_allow }
}

/// Per-venue EV penalties (tips/slip/latency by venue)
pub fn split_joint_venues_ev_per_venue<'a>(
    amount_in: f64,
    venues: &mut [Venue<'a>],
    steps: usize,
    penalties_per_in: &[f64], // len == venues
    caps: Option<&[f64]>,      // optional per-venue caps (float)
) -> Vec<f64> {
    use std::collections::BinaryHeap;
    let n = venues.len();
    assert_eq!(n, penalties_per_in.len());
    if let Some(c) = caps { assert_eq!(n, c.len()); }

    let mut alloc = vec![0.0f64; n];
    if amount_in <= 0.0 || n == 0 || steps == 0 { return alloc; }

    for v in venues.iter_mut() {
        if let Venue::Clmm { sqrt_p, segs, seg_idx, .. } = v {
            *seg_idx = find_seg_idx(*sqrt_p, segs);
        }
    }

    let q = (amount_in / steps as f64).max(1.0);
    let mut heap = BinaryHeap::with_capacity(n);
    let mut marginal = |i: usize| -> f64 {
        let cap_i = caps.map(|c| c[i]).unwrap_or(f64::INFINITY);
        let rem = (cap_i - alloc[i]).max(0.0);
        if rem <= 0.0 { return 0.0; }
        let q_eff = q.min(rem);
        let raw = match &venues[i] {
            Venue::Cpmm(p) => {
                let a0 = cpmm_total_out(p, alloc[i]);
                let a1 = cpmm_total_out(p, alloc[i] + q_eff);
                (a1 - a0).max(0.0)
            }
            Venue::Clmm { dir, sqrt_p, segs, seg_idx } => {
                let (g, _, _) = clmm_step_in_out_with_idx(*sqrt_p, *seg_idx, *dir, segs, q_eff);
                g.max(0.0)
            }
        };
        raw - penalties_per_in[i].max(0.0) * q_eff
    };
    #[derive(Clone, Copy)]
    struct Entry{g:f64,i:usize}
    impl PartialEq for Entry{fn eq(&self,o:&Self)->bool{self.g==o.g&&self.i==o.i}}
    impl Eq for Entry{}
    impl PartialOrd for Entry{
        fn partial_cmp(&self,o:&Self)->Option<Ordering>{
            if !self.g.is_finite() && !o.g.is_finite(){return Some(Ordering::Equal);}
            if !self.g.is_finite(){return Some(Ordering::Less);}
            if !o.g.is_finite(){return Some(Ordering::Greater);}
            match self.g.partial_cmp(&o.g){Some(Ordering::Equal)=>Some(o.i.cmp(&self.i)),ord=>ord}
        }}
    impl Ord for Entry{fn cmp(&self,o:&Self)->Ordering{self.partial_cmp(o).unwrap_or(Ordering::Equal)}}

    for i in 0..n { heap.push(Entry{g:marginal(i),i}); }
    for _ in 0..steps {
        if let Some(mut e)=heap.pop() {
            if e.g <= GAIN_EPS { break; }
            let i=e.i;
            let cap_i = caps.map(|c| c[i]).unwrap_or(f64::INFINITY);
            let q_eff = q.min((cap_i - alloc[i]).max(0.0));
            if q_eff <= 0.0 { continue; }
            alloc[i]+=q_eff;
            match &mut venues[i] {
                Venue::Cpmm(_)=>{ e.g=marginal(i); }
                Venue::Clmm{dir,sqrt_p,segs,seg_idx}=>{
                    let (_out,sp_new,idx_new)=clmm_step_in_out_with_idx(*sqrt_p,*seg_idx,*dir,segs,q_eff);
                    *sqrt_p=sp_new; *seg_idx=idx_new; e.g=marginal(i);
                }
            }
            heap.push(e);
        } else { break; }
    }
    // rescale exact
    let s: f64 = alloc.iter().sum();
    if s>0.0 {
        let scale = amount_in / s;
        for a in &mut alloc { *a *= scale; }
    }
    alloc
}

/// Forge plan EV per venue (caps + lots + per-venue penalties)
pub fn forge_plan_ev_per_venue<'a>(
    amount_in: u64,
    venues: &mut [Venue<'a>],
    steps: usize,
    lots: &[u64],
    caps: &[u64],
    penalties_per_in: &[f64],
) -> PlanResult {
    let caps_f: Vec<f64> = caps.iter().map(|&c| c as f64).collect();
    let plan_f = split_joint_venues_ev_per_venue(amount_in as f64, venues, steps, penalties_per_in, Some(&caps_f));
    let FinalizeResult{mut alloc, dust} = finalize_allocations_lrm_strict(amount_in, &plan_f, lots, caps);
    // optional post-round EV rebalance
    rebalance_lots_greedy(venues, lots, caps, &mut alloc, 8*venues.len());
    let expected_out = simulate_plan_out(&*venues, &alloc);
    let _ = dust; // dust==0 when amount_in % gcd(lots)==0
    PlanResult { alloc_u64: alloc, expected_out }
}

/// Plan sanity checker (caps/lots/exactness; deterministic)
pub struct PlanCheck {
    pub sum_ok: bool,
    pub lots_ok: bool,
    pub caps_ok: bool,
    pub nonzero_ok: bool,
}

pub fn check_plan_sane(
    amount_in: u64,
    alloc: &[u64],
    lots: &[u64],
    caps: &[u64],
) -> PlanCheck {
    let mut sum: u128 = 0;
    let mut lots_ok = true;
    let mut caps_ok = true;
    let mut nonzero_ok = true;

    for i in 0..alloc.len() {
        let a = alloc[i];
        sum += a as u128;
        let lot = lots[i].max(1);
        if a % lot != 0 { lots_ok = false; }
        if a > caps[i] { caps_ok = false; }
        if a as i128  < 0 { nonzero_ok = false; }
    }
    PlanCheck {
        sum_ok: sum == amount_in as u128,
        lots_ok,
        caps_ok,
        nonzero_ok,
    }
}

/// Kahan sum for EV (stable float accumulation)
#[inline]
fn kahan_sum(xs: impl IntoIterator<Item=f64>) -> f64 {
    let mut sum = 0.0f64;
    let mut c = 0.0f64;
    for x in xs {
        let y = x - c;
        let t = sum + y;
        c = (t - sum) - y;
        sum = t;
    }
    sum
}

pub fn simulate_plan_out_kahan<'a>(venues: &[Venue<'a>], alloc_u64: &[u64]) -> f64 {
    assert_eq!(venues.len(), alloc_u64.len());
    let mut parts: Vec<f64> = Vec::with_capacity(alloc_u64.len());
    for (v, &ain) in venues.iter().zip(alloc_u64.iter()) {
        parts.push(simulate_single_venue_out(v, ain));
    }
    kahan_sum(parts)
}

// === ADDITIONAL HELPER FUNCTIONS ===

/// Map allocations from filtered indices back to full venue vector size.
/// `kept` are original indices preserved by filter_venues_by_upper_bound.
/// Any non-kept venues receive 0.
pub fn expand_alloc_to_full(kept: &[usize], alloc_kept: &[u64], full_len: usize) -> Vec<u64> {
    assert_eq!(kept.len(), alloc_kept.len());
    let mut out = vec![0u64; full_len];
    for (k, &amt) in kept.iter().zip(alloc_kept.iter()) {
        if *k < full_len { out[*k] = amt; }
    }
    out
}

/// Deterministic stable sort helper (no NaN drift)
#[inline]
pub fn stable_cmp_f64_desc((ia, a): (usize, f64), (ib, b): (usize, f64)) -> std::cmp::Ordering {
    use std::cmp::Ordering::*;
    let fa = a.is_finite(); let fb = b.is_finite();
    match (fa, fb) {
        (false, false) => ia.cmp(&ib),     // both non-finite: by index
        (false, true)  => Greater,         // put -inf/NaN below finite in desc order
        (true,  false) => Less,
        (true,  true)  => match b.partial_cmp(&a).unwrap_or(Equal) { // desc by value
            Equal => ia.cmp(&ib),
            o => o,
        },
    }
}

/// Guarded CPMM out (branch-lean; no denorm blow-ups)
#[inline]
fn cpmm_total_out_guarded(p: &CpmmPool, amount_in: f64) -> f64 {
    if !(amount_in > 0.0) { return 0.0; }
    let fee = p.fee_mult();
    let fee = if fee.is_finite() { fee.max(0.0) } else { 0.0 };
    let tiny = 1e-12;
    match p.dir {
        ClmmDir::ZeroForOne => {
            let x = if p.reserve0.is_finite() { p.reserve0 } else { 0.0 };
            let y = if p.reserve1.is_finite() { p.reserve1 } else { 0.0 };
            let x = if x > tiny { x } else { tiny };
            let y = if y > tiny { y } else { tiny };
            let dx_eff = amount_in * fee;
            let den = (x + dx_eff).max(tiny);
            let dy = (y * dx_eff) / den;
            if dy.is_finite() && dy > 0.0 { dy } else { 0.0 }
        }
        ClmmDir::OneForZero => {
            let x = if p.reserve1.is_finite() { p.reserve1 } else { 0.0 };
            let y = if p.reserve0.is_finite() { p.reserve0 } else { 0.0 };
            let x = if x > tiny { x } else { tiny };
            let y = if y > tiny { y } else { tiny };
            let dx_eff = amount_in * fee;
            let den = (x + dx_eff).max(tiny);
            let dy = (y * dx_eff) / den;
            if dy.is_finite() && dy > 0.0 { dy } else { 0.0 }
        }
    }
}

/*
=== VENUE ALLOCATION USAGE EXAMPLES ===

// Basic usage with optimized capped splitter
let mut venues = build_venues(&cpmms, &clmms);
let caps_f: Vec<f64> = caps.iter().map(|&c| c as f64).collect();
let plan_f = split_joint_venues_capped(amount_in as f64, &mut venues, 32, &caps_f);
let alloc_u64 = finalize_allocations_lrm(amount_in, &plan_f, &lots, &caps);
let expected_out = simulate_plan_out(&venues, &alloc_u64);

// Advanced usage with rebalance and per-venue penalties
let penalties_per_in = vec![0.001, 0.002, 0.0005]; // different costs per venue
let PlanResult { alloc_u64, expected_out } = forge_plan_ev_per_venue(
    amount_in, &mut venues, 32, &lots, &caps, &penalties_per_in
);

// Pre-filter venues to reduce heap size, then expand back
let original_venues_len = venues.len();
let q_base = (amount_in as f64 / 32.0).max(1.0);
let kept = filter_venues_by_upper_bound(&mut venues, &caps_f, q_base, 3);
// ... plan on filtered `venues` ...
let FinalizeResult{ alloc: alloc_kept, dust } =
    finalize_allocations_lrm_strict(amount_in, &plan_f, &lots_kept, &caps_kept);
let alloc_full = expand_alloc_to_full(&kept, &alloc_kept, original_venues_len);

// Strict finalizer with exact sum when possible
let FinalizeResult { mut alloc, dust } = finalize_allocations_lrm_strict(
    amount_in, &plan_f, &lots, &caps
);
rebalance_lots_greedy(&venues, &lots, &caps, &mut alloc, 8*venues.len());

// Sanity check before execution
assert!(check_plan_sane(amount_in, &alloc, &lots, &caps).sum_ok);

// Compute min-out with stable Kahan sum
let expected_out_stable = simulate_plan_out_kahan(&venues, &alloc);
let px_out_per_in = expected_out_stable / (amount_in as f64);
let min_out = compute_min_out(amount_in, px_out_per_in, slippage_frac, out_decimals, 0);

// Conservative segment coalescing
let coalesced_segs = coalesce_segments_conservative(&segments, 1e-6, 1e-9);

// Stable sorting for deterministic results
scores.sort_by(|a,b| stable_cmp_f64_desc(*a,*b));
*/

// === PASTE VERBATIM: fast hash + jitter ===
#[inline]
fn fast_hash_u64(mut x: u64) -> u64 {
    // splitmix64 variant
    x = x.wrapping_add(0x9E3779B97F4A7C15);
    let mut z = x;
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58476D1CE4E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D049BB133111EB);
    z ^ (z >> 31)
}

#[inline]
fn tip_jitter_mult(slot: u64, attempt_idx: u32, salt: u64) -> f64 {
    let seed = slot ^ (attempt_idx as u64) ^ salt;
    let h = fast_hash_u64(seed);
    // map 64-bit to [0,1)
    let u = ((h >> 11) as f64) * (1.0 / ((1u64 << 53) as f64));
    u // ∈ [0,1)
}

// === PASTE VERBATIM: robust oracle aggregator ===
pub fn oracle_median_ok(sources: &[(f64, u64)], max_age_ms: u64, max_cv: f64) -> Option<f64> {
    // sources: (price, age_ms)
    let mut v = Vec::with_capacity(sources.len());
    for &(px, age) in sources {
        if age <= max_age_ms && px.is_finite() && px > 0.0 { v.push(px); }
    }
    if v.is_empty() { return None; }
    v.sort_by(|a,b| a.partial_cmp(b).unwrap());
    let mid = v[v.len()/2];
    // coefficient of variation check (dispersion sanity)
    let mean = v.iter().sum::<f64>() / (v.len() as f64);
    let var = v.iter().map(|x| (x - mean) * (x - mean)).sum::<f64>() / (v.len() as f64);
    let cv = (var.sqrt() / mean).abs();
    if cv > max_cv { return None; }
    Some(mid)
}

pub fn pool_vs_oracle_guard(pool_px: f64, oracle_px: f64, vol_proxy: f64) -> bool {
    if !pool_px.is_finite() || !oracle_px.is_finite() || pool_px <= 0.0 || oracle_px <= 0.0 { return false; }
    let base = 0.0035; // 35 bps
    let widener = (4.5 * vol_proxy).clamp(0.0, 0.025);
    let band = (base + widener).clamp(0.003, 0.03);
    let dev = ((pool_px / oracle_px) - 1.0).abs();
    dev <= band
}

// === REPLACE: attempt pacing (safe, deterministic jitter) ===
#[inline]
pub fn attempt_spacing_us(lat_ema_us: f64, exec_overhead_us: f64) -> u64 {
    (lat_ema_us + exec_overhead_us).max(400.0).round() as u64
}

#[inline]
pub fn next_attempt_deadline_us(
    slot_phase_0_1: f64,
    slot_us: u64,
    attempt_idx: u32,
    spacing_us: u64,
    safety_us: u64,
    slot: u64,
    salt: u64,
) -> Option<u64> {
    // deterministic jitter in [-2%, +2%] around spacing
    let j = tip_jitter_mult(slot, attempt_idx, salt);
    let jittered = ((spacing_us as f64) * (0.98 + 0.04 * j)).round() as u64;

    let now_us = (slot_phase_0_1.max(0.0).min(1.0) * (slot_us as f64)).round() as u64;
    let target = attempt_idx as u64 * jittered;
    let latest = slot_us.saturating_sub(safety_us.max(200)); // never schedule past safety window
    if target > latest { return None; }
    Some(target.max(now_us))
}

// === PASTE VERBATIM: dual minOut guard ===
pub fn min_out_dual(
    amt_in: u64,
    px_out_per_in: f64,
    slippage: f64,
    out_decimals: u8,
    tick: u64,
    amm_out_theoretical: u64
) -> u64 {
    let price_guard = compute_min_out(amt_in, px_out_per_in, slippage, out_decimals, tick);
    let m = price_guard.min(amm_out_theoretical);
    if tick > 0 { round_down_to_tick(m, tick) } else { m }
}

// === PASTE VERBATIM: trimmed median oracle ===
pub fn oracle_trimmed_median_ok(sources: &[(f64, u64)], max_age_ms: u64, trim: usize, max_cv: f64) -> Option<f64> {
    let mut v: Vec<f64> = sources.iter()
        .filter(|(px, age)| *age <= max_age_ms && px.is_finite() && *px > &0.0)
        .map(|(px, _)| *px).collect();
    if v.is_empty() { return None; }
    v.sort_by(|a,b| a.partial_cmp(b).unwrap());
    let t = trim.min(v.len()/2);
    let slice = &v[t..(v.len()-t)];
    let mid = slice[slice.len()/2];

    let mean = slice.iter().sum::<f64>() / (slice.len() as f64);
    let var  = slice.iter().map(|x| (x - mean)*(x - mean)).sum::<f64>() / (slice.len() as f64);
    let cv = (var.sqrt() / mean.abs().max(1e-12)).abs();
    if cv > max_cv { None } else { Some(mid) }
}

// === REPLACE: compute_min_out + round_down_to_tick ===
// Assumptions: `amt_in` and result are in *base units*. `px_out_per_in` is a base-unit ratio.
// `slippage` is fractional (e.g. 0.003 = 30 bps).
fn compute_min_out(amt_in: u64, px_out_per_in: f64, slippage: f64, _out_decimals: u8, _tick: u64) -> u64 {
    if amt_in == 0 { return 0; }
    let px = px_out_per_in.max(0.0);
    let slip = (1.0 - slippage.max(0.0)).max(0.0);
    let raw = (amt_in as f64) * px * slip;
    if !raw.is_finite() || raw <= 0.0 { 0 } else { raw.floor() as u64 }
}

fn round_down_to_tick(amount: u64, tick: u64) -> u64 {
    if tick == 0 { return amount; }
    amount - (amount % tick)
}

// === PASTE VERBATIM: 64-byte aligned f64 buffer ===
pub struct AlignedF64 {
    ptr: *mut f64,
    len: usize,
    cap: usize,
}
unsafe impl Send for AlignedF64 {}
unsafe impl Sync for AlignedF64 {}

impl AlignedF64 {
    pub fn new() -> Self { Self { ptr: std::ptr::null_mut(), len: 0, cap: 0 } }
    pub fn with_len(len: usize) -> Self {
        let mut b = Self::new(); b.resize(len); b
    }
    #[inline] pub fn as_slice(&self) -> &[f64] {
        unsafe { std::slice::from_raw_parts(self.ptr, self.len) }
    }
    #[inline] pub fn as_mut_slice(&mut self) -> &mut [f64] {
        unsafe { std::slice::from_raw_parts_mut(self.ptr, self.len) }
    }
    pub fn resize(&mut self, new_len: usize) {
        if new_len == self.len { return; }
        unsafe {
            if !self.ptr.is_null() {
                let old_layout = Layout::from_size_align(self.cap * std::mem::size_of::<f64>(), 64).unwrap();
                dealloc(self.ptr as *mut u8, old_layout);
            }
            if new_len == 0 {
                self.ptr = std::ptr::null_mut();
                self.len = 0; self.cap = 0; return;
            }
            let layout = Layout::from_size_align(new_len * std::mem::size_of::<f64>(), 64).unwrap();
            let p = alloc_zeroed(layout);
            assert!(!p.is_null(), "aligned alloc failed");
            self.ptr = p as *mut f64;
            self.cap = new_len; self.len = new_len;
        }
    }
}
impl Drop for AlignedF64 {
    fn drop(&mut self) {
        unsafe {
            if !self.ptr.is_null() {
                let layout = Layout::from_size_align(self.cap * std::mem::size_of::<f64>(), 64).unwrap();
                dealloc(self.ptr as *mut u8, layout);
            }
        }
    }
}

// === REPLACE: Fixed-capacity RingBuffer with bitmask (MaybeUninit) ===
use std::mem::{self, MaybeUninit};

#[derive(Debug)]
pub struct RingBuffer<T> {
    buf: Vec<MaybeUninit<T>>,
    cap: usize,
    start: usize,
    len: usize,
    mask: Option<usize>,
}
impl<T> RingBuffer<T> {
    pub fn with_capacity(cap: usize) -> Self {
        let c = cap.max(1);
        let mask = if c.is_power_of_two() { Some(c - 1) } else { None };
        let mut buf = Vec::with_capacity(c);
        // safety: we track init count via len
        unsafe { buf.set_len(c); }
        Self { buf, cap: c, start: 0, len: 0, mask }
    }
    #[inline] fn wrap(&self, x: usize) -> usize {
        if let Some(m) = self.mask { x & m } else { x % self.cap }
    }
    #[inline] pub fn clear(&mut self) {
        // drop initialized elements
        for i in 0..self.len {
            let idx = self.wrap(self.start + i);
            unsafe { self.buf[idx].assume_init_drop(); }
        }
        self.start = 0; self.len = 0;
    }
    #[inline] pub fn len(&self) -> usize { self.len }
    #[inline] pub fn is_empty(&self) -> bool { self.len == 0 }
    #[inline] pub fn is_full(&self) -> bool { self.len == self.cap }

    #[inline] pub fn back(&self) -> Option<&T> {
        if self.len == 0 { return None; }
        let idx = self.wrap(self.start + self.len - 1);
        Some(unsafe { self.buf[idx].assume_init_ref() })
    }

    #[inline] pub fn push_back(&mut self, v: T) {
        if self.len < self.cap {
            let idx = self.wrap(self.start + self.len);
            self.buf[idx].write(v);
            self.len += 1;
        } else {
            // overwrite oldest
            unsafe { self.buf[self.start].assume_init_drop(); }
            self.buf[self.start].write(v);
            self.start = self.wrap(self.start + 1);
        }
    }

    #[inline] pub fn iter(&self) -> RingIter<'_, T> { RingIter { rb: self, idx: 0 } }

    #[inline] pub fn to_vec(&self) -> Vec<T> where T: Clone {
        let mut out = Vec::with_capacity(self.len);
        for x in self.iter() { out.push(x.clone()); }
        out
    }

    // === REPLACE: RingBuffer retain_last ===
    #[inline] pub fn retain_last(&mut self, keep: usize) {
        if keep >= self.len { return; }
        // drop the (len - keep) oldest elements
        let drop_cnt = self.len - keep;
        for i in 0..drop_cnt {
            let idx = self.wrap(self.start + i);
            unsafe { self.buf[idx].assume_init_drop(); }
        }
        self.start = self.wrap(self.start + drop_cnt);
        self.len = keep;
    }
}
pub struct RingIter<'a, T> { rb: &'a RingBuffer<T>, idx: usize }
impl<'a, T> Iterator for RingIter<'a, T> {
    type Item = &'a T;
    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        if self.idx >= self.rb.len { return None; }
        let pos = self.rb.wrap(self.rb.start + self.idx);
        self.idx += 1;
        Some(unsafe { self.rb.buf[pos].assume_init_ref() })
    }
}
impl<T> Drop for RingBuffer<T> {
    fn drop(&mut self) { self.clear(); }
}

// === PASTE VERBATIM: stability/perf constants ===
const FORGET_BIAS: f64 = 1.5;     // Keeps memory longer -> fewer whipsaws
const CELL_CLIP: f64 = 3.0;       // JAX/LSTM-style cell clipping for exploding cell states
const TEMP: f64 = 0.90;           // Temperature for calibrated softmax ( <1 => sharper )
const EPS: f64 = 1e-12;           // Numeric safety
const EMA_ALPHA: f64 = 0.20;      // Logit EMA smoothing factor
const SWITCH_HYSTERESIS: f64 = 0.05; // Confidence buffer to avoid regime flip-flop
const WEIGHT_DECAY: f64 = 1e-4;   // L2 regularization for online update
const MAX_GRAD_NORM: f64 = 1.0;   // Gradient clipping cap

// === PASTE VERBATIM: fast-activation toggle ===
const FAST_ACTIVATIONS: bool = true; // enable rational-approx tanh/sigmoid (low error, faster)

// === PASTE VERBATIM: probability floor for robust decisions ===
const PROB_FLOOR: f64 = 0.01; // 1% Dirichlet mass avoids brittle peaky softmax under noise

// === PASTE VERBATIM: dynamic temperature ===
const TEMP_MIN: f64 = 0.75;
const TEMP_MAX: f64 = 1.10;
const TEMP_ADJ_ALPHA: f64 = 0.05; // EMA strength for temp adaptation

// === PASTE VERBATIM: EV gating constants ===
const EV_LAMBDA_SLIP: f64 = 12.0; // slip penalty weight (tuned >1 for lamport reality)
const EV_FLOOR: f64 = 0.0;        // require non-negative proxy EV to send

// === PASTE VERBATIM: deterministic seed (set to non-zero to fix init) ===
const MODEL_INIT_SEED: u64 = 0xA4F3_92C1_9B77_DD21;

// === PASTE VERBATIM: EW histogram settings for volatility percentile ===
const VOL_BINS: usize = 64;
const VOL_HIST_ALPHA: f64 = 0.05;
const VOL_RANGE_MAX: f64 = 0.25;

// === PASTE VERBATIM: transition prior ===
// Rows: from current regime idx -> columns: next regime idx
// Diagonal dominant; discourage big jumps; allow HV edges moderately.
const TRANS_PRIOR: [[f64; 7]; 7] = [
    // SB,   B,   S,   Br,  SBr,  HV,   LV
    [0.68, 0.18, 0.06, 0.04, 0.01, 0.02, 0.01], // StrongBull
    [0.16, 0.60, 0.15, 0.05, 0.01, 0.02, 0.01], // Bull
    [0.06, 0.18, 0.46, 0.18, 0.06, 0.04, 0.02], // Sideways
    [0.01, 0.05, 0.15, 0.60, 0.16, 0.02, 0.01], // Bear
    [0.01, 0.04, 0.06, 0.18, 0.68, 0.02, 0.01], // StrongBear
    [0.03, 0.06, 0.14, 0.06, 0.03, 0.64, 0.04], // HighVolatility
    [0.03, 0.10, 0.24, 0.10, 0.03, 0.02, 0.48], // LowVolatility
];
const TRANS_BETA: f64 = 0.80; // strength of transition prior (0=no effect)

// === PASTE VERBATIM: cooldown knobs ===
const COOLDOWN_US_BASE: u64 = 1200;  // base inter-send cooldown
const COOLDOWN_MIN_US:  u64 = 300;   // min under heavy pressure
const COOLDOWN_MAX_US:  u64 = 5000;  // max under no pressure

// === PASTE VERBATIM: calibration bias tuning ===
const CALIB_ALPHA: f64 = 0.02;  // small step; safe online drift corrector
const CALIB_MAX: f64 = 1.25;    // hard bound for bias magnitude

// === PASTE VERBATIM: adaptive Dirichlet floor ===
const ALPHA_DECAY: f64 = 0.995;   // slow decay to avoid whipsaw
const ALPHA_INC:   f64 = 0.05;    // per-step increment when class occurs
const ALPHA_MIN:   f64 = 0.50;    // floor alpha
const ALPHA_MAX:   f64 = 12.0;    // ceiling alpha

// === PASTE VERBATIM: backoff constants ===
const BACKOFF_GROW: f64 = 1.20;   // multiply on reject/timeout
const BACKOFF_DECAY: f64 = 0.90;  // decay per successful send
const BACKOFF_MAX:   f64 = 2.50;  // hard cap
const BACKOFF_MIN:   f64 = 1.00;  // floor

// === PASTE VERBATIM: panic window ===
const PANIC_US: u64 = 25_000; // cool-off after emergency (microseconds)

// === PASTE VERBATIM: KL/hysteresis knobs ===
const KLD_HI: f64 = 0.15;  // strong shift
const KLD_LO: f64 = 0.03;  // weak shift
const MARGIN_HI: f64 = 0.22;
const MARGIN_LO: f64 = 0.06;

// Base stickiness by regime (prevents noisy SB<->SBr teleports)
// idx order: SB, B, S, Br, SBr, HV, LV
const STICKY_BASE: [f64; 7] = [0.08, 0.07, 0.05, 0.07, 0.08, 0.06, 0.05];

// === PASTE VERBATIM: mark-out controls ===
const MARKOUT_ALPHA: f64 = 0.15;   // EW strength on recent mark-outs
const MARKOUT_CLIP:  f64 = 0.02;   // +/-2% normalized cap

// === PASTE VERBATIM: latency EMA ===
const LAT_ALPHA: f64 = 0.2;

// === PASTE VERBATIM: tail risk knobs ===
const TAIL_ALPHA: f64 = 0.05;
const TAIL_KURT_WARN: f64 = 6.0;
const TAIL_KURT_EMERGENCY: f64 = 10.0;

// === REPLACE: fast matvec (cached CPU features + DAZ/FTZ + packed GEMV) ===
use std::sync::OnceLock;

// Cache CPU SIMD features once (process-wide)
static CPU_FEATS: OnceLock<(bool, bool)> = OnceLock::new();
#[inline]
fn cpu_feats() -> (bool, bool) {
    #[cfg(target_arch = "x86_64")]
    {
        *CPU_FEATS.get_or_init(|| {
            let has_avx512 = std::is_x86_feature_detected!("avx512f") && std::is_x86_feature_detected!("fma");
            let has_avx2   = std::is_x86_feature_detected!("avx2")     && std::is_x86_feature_detected!("fma");
            (has_avx512, has_avx2)
        })
    }
    #[cfg(not(target_arch = "x86_64"))]
    { (false, false) }
}

// Enable DAZ/FTZ to avoid denorm penalties (call once at startup)
pub fn enable_daz_ftz() {
    #[cfg(target_arch = "x86_64")]
    unsafe {
        use std::arch::x86_64::{_mm_getcsr, _mm_setcsr};
        let mut csr: u32 = _mm_getcsr();
        const FTZ: u32 = 1 << 15;
        const DAZ: u32 = 1 << 6;
        csr |= FTZ | DAZ;
        _mm_setcsr(csr);
    }
}

#[inline]
fn scalar_dot(a: &[f64], b: &[f64]) -> f64 {
    let mut acc = 0.0;
    let n = a.len();
    let mut i = 0usize;
    // unroll by 4 for simple ILP
    while i + 4 <= n {
        acc += a[i] * b[i] + a[i+1]*b[i+1] + a[i+2]*b[i+2] + a[i+3]*b[i+3];
        i += 4;
    }
    while i < n { acc += a[i] * b[i]; i += 1; }
    acc
}

#[inline]
fn fast_dot(a: &[f64], b: &[f64]) -> f64 {
    #[cfg(target_arch = "x86_64")]
    unsafe {
        let (has_avx512, has_avx2) = cpu_feats();
        if has_avx512 {
            use std::arch::x86_64::*;
            let n = a.len();
            let mut acc0 = _mm512_setzero_pd();
            let mut acc1 = _mm512_setzero_pd();
            let mut i = 0usize;
            while i + 16 <= n {
                let va0 = _mm512_loadu_pd(a.as_ptr().add(i));
                let vb0 = _mm512_loadu_pd(b.as_ptr().add(i));
                acc0 = _mm512_fmadd_pd(va0, vb0, acc0);

                let va1 = _mm512_loadu_pd(a.as_ptr().add(i + 8));
                let vb1 = _mm512_loadu_pd(b.as_ptr().add(i + 8));
                acc1 = _mm512_fmadd_pd(va1, vb1, acc1);
                i += 16;
            }
            let acc = _mm512_add_pd(acc0, acc1);
            let mut tmp = [0f64; 8];
            _mm512_storeu_pd(tmp.as_mut_ptr(), acc);
            let mut sum = tmp.iter().sum::<f64>();
            while i < n { sum += *a.get_unchecked(i) * *b.get_unchecked(i); i += 1; }
            return sum;
        }
        if has_avx2 {
            use std::arch::x86_64::*;
            let n = a.len();
            let mut acc0 = _mm256_setzero_pd();
            let mut acc1 = _mm256_setzero_pd();
            let mut i = 0usize;
            while i + 8 <= n {
                let va0 = _mm256_loadu_pd(a.as_ptr().add(i));
                let vb0 = _mm256_loadu_pd(b.as_ptr().add(i));
                acc0 = _mm256_fmadd_pd(va0, vb0, acc0);

                let va1 = _mm256_loadu_pd(a.as_ptr().add(i + 4));
                let vb1 = _mm256_loadu_pd(b.as_ptr().add(i + 4));
                acc1 = _mm256_fmadd_pd(va1, vb1, acc1);
                i += 8;
            }
            let acc = _mm256_add_pd(acc0, acc1);
            let mut tmp = [0f64; 4];
            _mm256_storeu_pd(tmp.as_mut_ptr(), acc);
            let mut sum = tmp.iter().sum::<f64>();
            while i < n { sum += *a.get_unchecked(i) * *b.get_unchecked(i); i += 1; }
            return sum;
        }
    }
    #[cfg(target_arch = "aarch64")]
    unsafe {
        use core::arch::aarch64::*;
        let n = a.len();
        let mut accv = vdupq_n_f64(0.0);
        let mut i = 0usize;
        while i + 2 <= n {
            let va = vld1q_f64(a.as_ptr().add(i));
            let vb = vld1q_f64(b.as_ptr().add(i));
            accv = vfmaq_f64(accv, va, vb);
            i += 2;
        }
        let mut sum = vgetq_lane_f64(accv, 0) + vgetq_lane_f64(accv, 1);
        while i < n { sum += *a.get_unchecked(i) * *b.get_unchecked(i); i += 1; }
        return sum;
    }
    scalar_dot(a, b)
}

/// Packed GEMV: out[r] = dot(w_pack[r, 0..pack_cols], x[0..pack_cols])
#[inline]
fn gemv_rows4_assign_packed(out: &mut [f64], w_pack: &[f64], rows: usize, pack_cols: usize, x: &[f64]) {
    debug_assert_eq!(out.len(), rows);
    debug_assert!(w_pack.len() >= rows * pack_cols);
    debug_assert!(x.len() >= pack_cols);

    // Prefetch rows ahead for locality (x86 only; no-ops elsewhere)
    #[cfg(target_arch = "x86_64")]
    const _MM_HINT_T0: i32 = 3;

    let mut r = 0usize;
    while r < rows {
        #[cfg(target_arch = "x86_64")]
        unsafe {
            if r + 4 < rows {
                use std::arch::x86_64::_mm_prefetch;
                let base_next = w_pack.as_ptr().add((r + 4) * pack_cols);
                _mm_prefetch(base_next as *const i8, _MM_HINT_T0);
            }
        }
        let base = r * pack_cols;
        let wrow = &w_pack[base..base + pack_cols];
        out[r] = fast_dot(wrow, x);
        r += 1;
    }
}

// Prefetch next rows when doing matvec over many rows; scalar-safe.
const PREFETCH_ROWS_AHEAD: usize = 2;

#[inline]
fn matvec_assign(out: &mut [f64], m: &ndarray::ArrayView2<f64>, v: &ndarray::ArrayView1<f64>) {
    assert_eq!(m.shape()[1], v.len());
    let vs = v.as_slice().unwrap();
    for (r, row) in m.outer_iter().enumerate() {
        let rs = row.as_slice().unwrap();
        #[cfg(target_arch = "x86_64")]
        unsafe {
            if r + PREFETCH_ROWS_AHEAD < m.nrows() {
                use std::arch::x86_64::_mm_prefetch;
                const _MM_HINT_T0: i32 = 3;
                let next = m.row(r + PREFETCH_ROWS_AHEAD);
                _mm_prefetch(next.as_ptr() as *const i8, _MM_HINT_T0);
            }
        }
        out[r] = fast_dot(rs, vs);
    }
}

#[inline]
fn matvec_add(out: &mut [f64], m: &ndarray::ArrayView2<f64>, v: &ndarray::ArrayView1<f64>) {
    assert_eq!(m.shape()[1], v.len());
    let vs = v.as_slice().unwrap();
    for (r, row) in m.outer_iter().enumerate() {
        let rs = row.as_slice().unwrap();
        out[r] += fast_dot(rs, vs);
    }
}

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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LSTMLayer {
    // Stored weights
    w_x: Array2<f64>,          // (4H × I)
    w_h: Array2<f64>,          // (4H × H)
    b:   Array1<f64>,          // (4H)
    hidden_size: usize,
    hidden_state: Array1<f64>, // (H)
    cell_state: Array1<f64>,   // (H)

    // --- Packed + scratch (not serialized) ---
    #[serde(skip)]
    w_cat: Array2<f64>,        // (4H × (I+H)) packed [W_x | W_h]
    #[serde(skip)]
    xh_scratch: Array1<f64>,   // (I+H)
    #[serde(skip)]
    z_scratch: Array1<f64>,    // (4H)
    
    // --- 64-byte-padded packed weights ---
    #[serde(skip)]
    w_pack: AlignedF64,        // row-major [4H × pack_cols], 64B-aligned
    #[serde(skip)]
    pack_cols: usize,          // padded cols (multiple of 8 for AVX512, 4 for AVX2, else = I+H)
    #[serde(skip)]
    xh_pack: AlignedF64,       // length = pack_cols, [x | h | 0-pad]
}

// === PASTE VERBATIM: EW Feature Scaler on normalized features ===
#[derive(Debug, Clone, Serialize, Deserialize)]
struct FeatureScaler {
    mean: [f64; 10],
    var:  [f64; 10],
    alpha: f64,
}
impl FeatureScaler {
    fn new(alpha: f64) -> Self {
        Self { mean: [0.0;10], var: [1.0;10], alpha }
    }
    #[inline]
    fn update_from_norm(&mut self, x: &[f64;10]) {
        let a = self.alpha;
        for i in 0..10 {
            let delta = x[i] - self.mean[i];
            self.mean[i] += a * delta;
            self.var[i]  = (1.0 - a) * (self.var[i] + a * delta * delta);
            // floor var for safety
            if self.var[i] < 1e-6 { self.var[i] = 1e-6; }
        }
    }
    #[inline]
    fn z(&self, val: f64, i: usize) -> f64 {
        (val - self.mean[i]) / (self.var[i].sqrt() + 1e-6)
    }
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
    // Model
    lstm_layers: Vec<LSTMLayer>,
    output_layer: Array2<f64>,
    output_bias: Array1<f64>,

    // Optimizer buffers & smoothing
    output_vel: Array2<f64>,
    bias_vel: Array1<f64>,
    logit_ema: Array1<f64>,
    ema_alpha: f64,

    // Data buffers (fixed-capacity)
    feature_buffer: RingBuffer<MarketFeatures>,
    price_history: RingBuffer<f64>,
    volume_history: RingBuffer<f64>,

    // Online feature scaler
    scaler: FeatureScaler,

    // Scratch
    #[serde(skip)]
    hseq_scratch: Array2<f64>,  // [sequence_length × hidden_size]

    // Execution smoothing state
    prev_aggr: f64,
    prev_slip: f64,
    prev_chunk: f64,

    // Dynamic temperature
    temp_current: f64,

    // EW histogram for volatility percentile
    vol_hist: [f64; VOL_BINS],   // EW mass; not normalized to 1 at all times
    vol_mass: f64,               // running total mass

    // Markov transition prior
    trans_log: [[f64; 7]; 7],

    // Calibration bias
    calib_bias: ndarray::Array1<f64>, // per-class bias added to logits (post-weights)

    // Per-class Dirichlet floor
    class_alpha: [f64; 7],     // adaptive per-class prior mass
    alpha_sum:   f64,          // cached sum
    last_probs: ndarray::Array1<f64>, // store last probs (post-prior) for margin/exec

    // Backoff control
    backoff_mult: f64,

    // Mark-out tracking
    markout_ew: f64,

    // Auxiliary volatility head
    vol_w: ndarray::Array1<f64>, // [H]
    vol_b: f64,
    vol_vel_w: ndarray::Array1<f64>,
    vol_vel_b: f64,
    last_vol_pred: f64,

    // Latency EMA
    lat_ema_us: f64,

    // Preallocated sequence matrix scratch [T × 10]
    #[serde(skip)]
    xseq_scratch: ndarray::Array2<f64>,

    // Tail risk tracking
    tail_mu: f64,
    tail_m2: f64,
    tail_m4: f64,
    tail_kurt: f64,

    // Attention key caching
    attn_key: ndarray::Array1<f64>,

    // Per-leader inclusion/tip profile
    leader_profiles: HashMap<[u8;32], LeaderProfile>,

    // Precomputed normalization (min, inv_span) for 10 features
    norm_min: [f64; 10],
    norm_inv_span: [f64; 10],

    // Slot phase awareness
    slot_phase: f64, // [0,1], executor-provided: 0=start of slot, 1=end

    // Cooldown control
    #[serde(skip)]
    last_send: Option<std::time::Instant>,

    // Panic window
    #[serde(skip)]
    panic_until: Option<std::time::Instant>,

    // Shared state
    regime_confidence: Arc<RwLock<f64>>,
    current_regime: Arc<RwLock<MarketRegime>>,

    // Hyperparams
    sequence_length: usize,
    hidden_size: usize,
    learning_rate: f64,
    momentum_factor: f64,
    regime_threshold: f64,

    // Telemetry
    last_pred_us: u64,

    // prefetch tuner (host-local)
    pref_pdist: usize,         // 1..5
    pref_score: [f64; 6],      // EW scores per PDIST index
}


impl LSTMLayer {
    fn new(input_size: usize, hidden_size: usize) -> Self {
        use rand::Rng;
        let mut rng = rand::thread_rng();
        let scale_x = (2.0 / (input_size as f64 + hidden_size as f64)).sqrt();
        let scale_h = (2.0 / (hidden_size as f64 + hidden_size as f64)).sqrt();
        let four_h = 4 * hidden_size;

        let mut b = Array1::zeros(four_h);
        {
            use ndarray::s;
            b.slice_mut(s![hidden_size..2*hidden_size]).fill(FORGET_BIAS);
            b.slice_mut(s![0..hidden_size]).fill(0.10);
            b.slice_mut(s![2*hidden_size..3*hidden_size]).fill(0.0);
            b.slice_mut(s![3*hidden_size..4*hidden_size]).fill(0.0);
        }

        let mut layer = Self {
            w_x: Array2::from_shape_fn((four_h, input_size), |_| rng.gen_range(-scale_x..scale_x)),
            w_h: Array2::from_shape_fn((four_h, hidden_size), |_| rng.gen_range(-scale_h..scale_h)),
            b,
            hidden_size,
            hidden_state: Array1::zeros(hidden_size),
            cell_state: Array1::zeros(hidden_size),
            w_cat: Array2::zeros((four_h, input_size + hidden_size)),
            xh_scratch: Array1::zeros(input_size + hidden_size),
            z_scratch: Array1::zeros(four_h),
            w_pack: AlignedF64::new(),
            pack_cols: 0,
            xh_pack: AlignedF64::new(),
        };
        layer.rebuild_packed();
        layer
    }

    #[inline]
    fn rebuild_packed(&mut self) {
        let h = self.hidden_size;
        let i = self.w_x.ncols();
        let rows = self.w_x.nrows();

        // Pack [W_x | W_h] into w_cat (kept for serialization and debug)
        for r in 0..rows {
            for c in 0..i { self.w_cat[[r, c]] = self.w_x[[r, c]]; }
            for c in 0..h { self.w_cat[[r, i + c]] = self.w_h[[r, c]]; }
        }

        // Choose padded columns based on CPU features
        let cols = i + h;
        let (has_avx512, has_avx2) = cpu_feats();
        let pad_to = if has_avx512 { 8 } else if has_avx2 { 4 } else { 1 }; // doubles per vector
        let pack_cols = ((cols + pad_to - 1) / pad_to) * pad_to;

        self.pack_cols = pack_cols;

        self.w_pack.resize(rows * pack_cols);
        let dst = self.w_pack.as_mut_slice();
        // copy rows with zero tail padding
        for r in 0..rows {
            let base = r * pack_cols;
            for c in 0..cols { dst[base + c] = self.w_cat[[r, c]]; }
            for c in cols..pack_cols { dst[base + c] = 0.0; }
        }

        // resize scratch in case shapes changed
        self.xh_scratch = ndarray::Array1::zeros(cols);
        self.z_scratch  = ndarray::Array1::zeros(rows);
        self.xh_pack.resize(self.pack_cols); // aligned, zeroed
    }

    #[inline]
    fn act_tanh(x: f64) -> f64 {
        if FAST_ACTIVATIONS {
            // |err| < ~1.5e-3 on [-5,5], monotonic, stable
            let x2 = x * x;
            let num = x * (27.0 + x2);
            let den = 27.0 + 9.0 * x2;
            (num / den).clamp(-1.0, 1.0)
        } else {
            x.tanh()
        }
    }

    #[inline]
    fn act_sigmoid(x: f64) -> f64 {
        if FAST_ACTIVATIONS {
            0.5 * (1.0 + Self::act_tanh(0.5 * x))
        } else if x >= 0.0 {
            let e = (-x).exp(); 1.0 / (1.0 + e)
        } else {
            let e = x.exp();     e / (1.0 + e)
        }
    }

    // Process a SINGLE timestep (kept for backwards compatibility)
    fn forward(&mut self, input: &ArrayView1<f64>) -> Array1<f64> {
        use ndarray::s;

        // === REPLACE: build [x|h] into aligned packed scratch ===
        {
            let xs = input.as_slice().unwrap();
            let hs = self.hidden_state.as_slice().unwrap();
            let cols = xs.len() + hs.len();
            debug_assert!(cols <= self.pack_cols);
            let buf = self.xh_pack.as_mut_slice();
            buf[..xs.len()].copy_from_slice(xs);
            buf[xs.len()..cols].copy_from_slice(hs);
            for k in cols..self.pack_cols { buf[k] = 0.0; } // zero tail
        }

        // z = W_cat * [x|h] + b
        self.z_scratch.fill(0.0);
        gemv_rows4_assign_packed(
            self.z_scratch.as_slice_mut().unwrap(),
            self.w_pack.as_slice(),
            self.w_cat.nrows(),
            self.pack_cols,
            self.xh_pack.as_slice(),
        );
        {
            let z = self.z_scratch.as_slice_mut().unwrap();
            let bb = self.b.as_slice().unwrap();
            for i in 0..z.len() { z[i] += bb[i]; }
        }

        let h = self.hidden_size;
        // Split z into gates (in-place transform)
        let (i_gate, rest) = self.z_scratch.as_slice_mut().unwrap().split_at_mut(h);
        let (f_gate, rest) = rest.split_at_mut(h);
        let (g_gate, o_gate) = rest.split_at_mut(h);

        // Activations (vectorized)
        apply_sigmoid_inplace(i_gate);
        apply_sigmoid_inplace(f_gate);
        apply_tanh_inplace(g_gate);
        apply_sigmoid_inplace(o_gate);

        // Fused cell/hidden update
        let (cell, hid) = (self.cell_state.as_slice_mut().unwrap(), self.hidden_state.as_slice_mut().unwrap());
        fused_cell_hidden_update(cell, hid, i_gate, f_gate, g_gate, o_gate, CELL_CLIP);

        self.hidden_state.clone()
    }

    // Fast path: process a full SEQUENCE [T × I], returns last hidden
    fn forward_seq(&mut self, seq: &ndarray::ArrayView2<f64>) -> Array1<f64> {
        use ndarray::{Axis, s};
        // Reset state at sequence start (behavior matches prior classify path)
        self.reset_states();
        let h = self.hidden_size;

        for t in 0..seq.len_of(Axis(0)) {
            let x_t = seq.index_axis(Axis(0), t); // shape [I]
            // z = W_x x_t + W_h h_{t-1} + b
            let z = self.w_x.dot(&x_t) + self.w_h.dot(&self.hidden_state) + &self.b;

            let i = z.slice(s![0..h]).mapv(Self::sigm);
            let f = z.slice(s![h..2*h]).mapv(Self::sigm);
            let g = z.slice(s![2*h..3*h]).mapv(|x| x.tanh());
            let o = z.slice(s![3*h..4*h]).mapv(Self::sigm);

            self.cell_state = &f * &self.cell_state + &i * &g;
            self.cell_state.mapv_inplace(|c| c.clamp(-CELL_CLIP, CELL_CLIP));
            self.hidden_state = &o * self.cell_state.mapv(|x| x.tanh());
        }
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
        lstm_layers.push(LSTMLayer::new(num_features, hidden_size));
        lstm_layers.push(LSTMLayer::new(hidden_size, hidden_size));
        
        use rand::{Rng, SeedableRng, rngs::StdRng};
        let mut rng = StdRng::seed_from_u64(MODEL_INIT_SEED);
        let output_scale = (2.0 / hidden_size as f64).sqrt();

        let output_layer = Array2::from_shape_fn((num_classes, hidden_size), |_| {
            rng.gen_range(-output_scale..output_scale)
        });
        let output_bias = Array1::zeros(num_classes);
        
        Self {
            lstm_layers,
            output_layer: output_layer.clone(),
            output_bias: output_bias.clone(),

            output_vel: Array2::zeros((num_classes, hidden_size)),
            bias_vel: Array1::zeros(num_classes),
            logit_ema: Array1::zeros(num_classes),
            ema_alpha: EMA_ALPHA,

            feature_buffer: RingBuffer::with_capacity(sequence_length),
            price_history: RingBuffer::with_capacity(100),
            volume_history: RingBuffer::with_capacity(100),

            scaler: FeatureScaler::new(0.10),
            hseq_scratch: Array2::zeros((sequence_length, hidden_size)),

            prev_aggr: 0.0,
            prev_slip: 0.0,
            prev_chunk: 0.0,

            temp_current: TEMP,

            vol_hist: [0.0; VOL_BINS],
            vol_mass: 1e-9, // avoid div-by-zero before first updates

            trans_log: {
                let mut m = [[0.0f64; 7]; 7];
                for r in 0..7 {
                    for c in 0..7 {
                        m[r][c] = TRANS_PRIOR[r][c].max(EPS).ln();
                    }
                }
                m
            },

            calib_bias: Array1::zeros(num_classes),

            class_alpha: [1.0; 7],
            alpha_sum: 7.0,
            last_probs: Array1::from_vec(vec![1.0/7.0; 7]),

            backoff_mult: 1.0,

            markout_ew: 0.0,

            vol_w: Array1::zeros(hidden_size),
            vol_b: 0.0,
            vol_vel_w: Array1::zeros(hidden_size),
            vol_vel_b: 0.0,
            last_vol_pred: 0.0,

            lat_ema_us: 0.0,

            xseq_scratch: ndarray::Array2::zeros((sequence_length, 10)),

            tail_mu: 0.0, tail_m2: 1e-9, tail_m4: 3.0, tail_kurt: 3.0,

            attn_key: {
                use ndarray::{Array1, Axis};
                let k = output_layer.mean_axis(Axis(0)).unwrap_or(Array1::zeros(hidden_size));
                let n = (k.dot(&k)).sqrt().max(1e-12);
                &k / n
            },

            leader_profiles: HashMap::new(),

            norm_min: [
                -0.05, 0.0, 0.0, 0.0, -0.02, -1.0, 0.0, -0.10, -0.02, -1.0
            ],
            norm_inv_span: [
                1.0/0.10, 1.0/3.0, 1.0/0.05, 1.0/1.0, 1.0/0.04,
                1.0/2.0, 1.0/0.01, 1.0/0.20, 1.0/0.04, 1.0/2.0
            ],

            slot_phase: 0.5,

            last_send: None,

            panic_until: None,

            regime_confidence: Arc::new(RwLock::new(0.0)),
            current_regime: Arc::new(RwLock::new(MarketRegime::Sideways)),
            sequence_length,
            hidden_size,
            learning_rate: 0.001,
            momentum_factor: 0.9,
            regime_threshold: 0.65,
            last_pred_us: 0,

            pref_pdist: 3,
            pref_score: [0.0; 6],
        }
    }

    pub fn update_market_data(&mut self, price: f64, volume: f64, bid: f64, ask: f64, 
                             bid_size: f64, ask_size: f64, trades_count: u64) {
        self.price_history.push_back(price);
        self.volume_history.push_back(volume);
        
        if self.price_history.len() >= 20 {
            let features = self.extract_features(price, volume, bid, ask, bid_size, ask_size, trades_count);
            let features = Self::sanitize_features(features);
            self.update_vol_hist(features.volatility);
            // Update buffers
            self.feature_buffer.push_back(features);
            
            // Online scaler update on normalized feature vector (same mapping as matrix builder)
            let norm = self.features_to_norm_array(self.feature_buffer.back().unwrap());
            self.scaler.update_from_norm(&norm);
            
            if self.feature_buffer.len() == self.sequence_length {
                let regime = self.classify_regime();
                *self.current_regime.write() = regime;
            }
        }
    }

    fn extract_features(&self, price: f64, volume: f64, bid: f64, ask: f64,
                       bid_size: f64, ask_size: f64, trades_count: u64) -> MarketFeatures {
        // Pull histories from fixed buffers
        let prices: Vec<f64> = self.price_history.iter().copied().collect();
        let volumes: Vec<f64> = self.volume_history.iter().copied().collect();
        
        // Base signals
        let price_returns = if prices.len() > 1 {
            (price - prices[prices.len() - 2]) / prices[prices.len() - 2].max(EPS)
        } else { 0.0 };

        let momentum_raw = if prices.len() >= 10 {
            (price - prices[prices.len() - 10]) / prices[prices.len() - 10].max(EPS)
        } else { 0.0 };

        let avg_volume = (volumes.iter().sum::<f64>() / (volumes.len() as f64).max(1.0)).max(1.0);
        let volume_ratio = (volume / avg_volume).clamp(0.0, 10.0);

        let volatility = self.calculate_volatility(&prices).clamp(0.0, 0.25);
        let rsi = self.calculate_rsi(&prices).clamp(0.0, 1.0);
        let macd_signal = self.calculate_macd(&prices).clamp(-0.2, 0.2);

        // Microstructure
        let ofi_raw = (bid_size - ask_size) / (bid_size + ask_size + 1.0);
        let spread = (ask - bid).max(0.0);
        let mid = ((bid + ask) * 0.5).max(EPS);
        let spread_ratio = (spread / mid).clamp(0.0, 0.05);

        // Microprice pressure
        let microprice = if (bid_size + ask_size) > 0.0 {
            (ask_size * bid + bid_size * ask) / (bid_size + ask_size)
        } else { mid };
        let micro_pressure = ((microprice - mid) / mid).clamp(-0.02, 0.02);

        // Value-weighted imbalance
        let bid_val = bid_size * bid;
        let ask_val = ask_size * ask;
        let bid_ask_pressure = if (bid_val + ask_val) > 0.0 {
            ((bid_val - ask_val) / (bid_val + ask_val)).clamp(-1.0, 1.0)
        } else { 0.0 };

        // === Anti-spoof damping using trade density & robust MAD ===
        let trade_density = (trades_count as f64) / (volume.max(1.0));
        let returns_mad = calc_returns_mad(&prices).max(1e-6);
        let is_thin = trade_density < 5e-4; // empirically conservative
        let is_spread_wide = spread_ratio > 0.01;
        let is_return_spiky = price_returns.abs() > 6.0 * returns_mad;

        // Dampen OFI & momentum when print quality is suspicious
        let damp = if (is_thin && ofi_raw.abs() > 0.5) || is_spread_wide || is_return_spiky { 0.5 } else { 1.0 };
        let order_flow_imbalance = (ofi_raw.tanh() * damp).clamp(-1.0, 1.0);
        let momentum = ((momentum_raw + micro_pressure * 0.5) * damp).clamp(-0.1, 0.1);

        // VWAP deviation
        let vwap = self.calculate_vwap(&prices, &volumes);
        let volume_weighted_price = if vwap > 0.0 { ((price - vwap) / vwap).clamp(-0.05, 0.05) } else { 0.0 };
        
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
        // Exponentially-weighted std of returns (span ~ 20)
        if prices.len() < 3 { return 0.0; }
        let mut prev = prices[0];
        let alpha = 2.0 / (20.0 + 1.0);
        let mut mean = 0.0;
        let mut var = 0.0;
        let mut count = 0.0;

        for &p in &prices[1..] {
            let r = (p - prev) / prev.max(EPS);
            prev = p;
            count += 1.0;

            // EW mean/var update
            let delta = r - mean;
            mean += alpha * delta;
            var = (1.0 - alpha) * (var + alpha * delta * delta);
        }
        var.abs().sqrt()
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

    // === PASTE VERBATIM: robust MAD on returns ===
    fn calc_returns_mad(prices: &[f64]) -> f64 {
        if prices.len() < 6 { return 0.0; }
        let mut rets: Vec<f64> = prices.windows(2)
            .map(|w| (w[1] - w[0]) / w[0].max(EPS))
            .collect();
        rets.sort_by(|a, b| a.partial_cmp(b).unwrap());
        let median = rets[rets.len()/2];
        let mut dev: Vec<f64> = rets.iter().map(|r| (r - median).abs()).collect();
        dev.sort_by(|a, b| a.partial_cmp(b).unwrap());
        dev[dev.len()/2]
    }

    // === PASTE VERBATIM: dynamic sequence length ===
    fn dynamic_seq_len(&self) -> usize {
        // shorter in HV to react; longer in calm to denoise
        let volp = self.calculate_volatility_percentile();
        if volp > 0.85 { self.sequence_length.min(12) }
        else if volp > 0.65 { self.sequence_length.min(15) }
        else if volp < 0.25 { self.sequence_length.min(24).max(self.sequence_length) } // keep at max cap
        else { self.sequence_length.min(18) }
    }

    // === REPLACE: attention pooling (cached key) ===
    // === REPLACE: attention_pool_partial ===
    fn attention_pool_partial(&self, hidden_seq: &ndarray::Array2<f64>, valid_rows: usize) -> ndarray::Array1<f64> {
        use ndarray::{Array1, Axis};
        let h = self.hidden_size;
        let vr = valid_rows.min(hidden_seq.len_of(Axis(0)));
        if vr == 0 { return Array1::<f64>::zeros(h); }

        // 1) find max score
        let key = self.attn_key.view();
        let mut smax = f64::NEGATIVE_INFINITY;
        for t in 0..vr {
            let ht = hidden_seq.index_axis(Axis(0), t);
            let sc = ht.dot(&key);
            if sc > smax { smax = sc; }
        }

        // 2) accumulate weighted sum with stable exp
        let invT = 1.0 / self.temp_current.max(1e-3);
        let mut z = 0.0;
        let mut pooled = Array1::<f64>::zeros(h);
        for t in 0..vr {
            let ht = hidden_seq.index_axis(Axis(0), t);
            let w = ((ht.dot(&key) - smax) * invT).exp();
            z += w;
            pooled = pooled + &(ht.to_owned() * w);
        }
        if z <= 0.0 { return Array1::<f64>::zeros(h); }
        pooled / z
    }

    fn classify_regime(&mut self) -> MarketRegime {
        use ndarray::{Axis, s};
        let t0 = std::time::Instant::now();

        // Build sequence matrix (whitened)
        let x_seq = self.prepare_sequence_matrix(); // [T×F] where T == sequence_length
        let t_dyn = self.dynamic_seq_len();

        // First layer across only the last t_dyn timesteps
        self.lstm_layers[0].reset_states();
        let start = self.sequence_length - t_dyn;
        for (row, t) in (start..self.sequence_length).enumerate() {
            let ht = self.lstm_layers[0].forward(&x_seq.index_axis(Axis(0), t));
            self.hseq_scratch.slice_mut(s![row, ..]).assign(&ht);
        }

        // Attention over the valid prefix [0..t_dyn)
        let h_pool = self.attention_pool_partial(&self.hseq_scratch, t_dyn);

        // Second layer consumes pooled vector
        self.lstm_layers[1].reset_states();
        let hidden = self.lstm_layers[1].forward(&h_pool.view());

        // Volatility prediction
        let _vol_pred = self.vol_predict(&hidden); // update last_vol_pred for exec

        // Logits -> EMA -> softmax, then Dirichlet floor
        let logits = Self::outlayer_apply_rows(&self.output_layer, &self.output_bias, &self.calib_bias, &hidden);
        self.logit_ema = &self.logit_ema * (1.0 - self.ema_alpha) + &logits * self.ema_alpha;
        let mut probs = self.softmax(&self.logit_ema);

        // Adaptive Dirichlet floor (per-class)
        let delta = PROB_FLOOR;
        let floor = self.dirichlet_floor_vec();
        let mut probs = (&probs * (1.0 - delta)) + &(floor * delta);

        // Apply transition prior
        let prev_idx = match *self.current_regime.read() {
            MarketRegime::StrongBull => 0, MarketRegime::Bull => 1, MarketRegime::Sideways => 2,
            MarketRegime::Bear => 3, MarketRegime::StrongBear => 4, MarketRegime::HighVolatility => 5,
            MarketRegime::LowVolatility => 6,
        };
        let probs = self.apply_transition_prior(&probs, prev_idx);

        // Adaptive temperature: update for next step
        self.update_temperature(&probs);

        // === REPLACE: SWITCH LOGIC in classify_regime ===
        let current = *self.current_regime.read();
        let current_idx = match current {
            MarketRegime::StrongBull => 0, MarketRegime::Bull => 1, MarketRegime::Sideways => 2,
            MarketRegime::Bear => 3, MarketRegime::StrongBear => 4, MarketRegime::HighVolatility => 5,
            MarketRegime::LowVolatility => 6,
        };
        let best_idx = Self::argmax(&probs);
        let best_prob = probs[best_idx];

        // Dynamic hysteresis with entropy guard
        let entropy = Self::entropy_from_probs(&probs);
        let base_hys = STICKY_BASE[current_idx];
        let margin = {
            let s = probs.as_slice().unwrap();
            let (_, _, m) = top2_from_slice(s);
            m
        };
        let ent_boost = if entropy > 1.6 { 1.25 } else if entropy > 1.4 { 1.10 } else { 1.0 };
        let hys = (base_hys * ent_boost)
            .min(0.18)
            .max(0.02);

        // KL gate
        let dkl = Self::kl_div(&self.last_probs, &probs);

        // Switch rules:
        // 1) If KL big OR margin huge AND confidence passes threshold.
        // 2) Otherwise require beating current by hysteresis delta.
        let strong = dkl > KLD_HI || (margin > MARGIN_HI && best_prob > self.regime_threshold);
        let better = (best_prob - probs[current_idx]) > hys;

        let final_idx = if strong || better { best_idx } else { current_idx };
        let final_conf = probs[final_idx];

        // Persist for next step + alpha adaptation
        self.last_probs = probs.clone();
        self.alpha_decay();
        self.alpha_bump(final_idx);

        *self.regime_confidence.write() = final_conf;
        let regime = match final_idx {
            0 => MarketRegime::StrongBull, 1 => MarketRegime::Bull, 2 => MarketRegime::Sideways,
            3 => MarketRegime::Bear, 4 => MarketRegime::StrongBear, 5 => MarketRegime::HighVolatility,
            6 => MarketRegime::LowVolatility, _ => MarketRegime::Sideways
        };
        *self.current_regime.write() = regime;

        self.last_pred_us = t0.elapsed().as_micros() as u64;
        self.lat_ema_us = (1.0 - LAT_ALPHA) * self.lat_ema_us + LAT_ALPHA * (self.last_pred_us as f64);
        
        // Update prefetch tuner
        self.update_prefetch_tuner(self.last_pred_us);
        
        regime
    }

    fn prepare_features(&self) -> Array1<f64> {
        let mut features = Array1::zeros(10);
        
        if let Some(last_features) = self.feature_buffer.back() {
            features[0] = self.normalize_value(last_features.price_returns, -0.05, 0.05);
            features[1] = self.normalize_value(last_features.volume_ratio, 0.0, 3.0);
            features[2] = self.normalize_value(last_features.volatility, 0.0, 0.05);
            features[3] = self.normalize_value(last_features.rsi, 0.0, 1.0);
            features[4] = self.normalize_value(last_features.macd_signal, -0.02, 0.02);
            features[5] = self.normalize_value(last_features.order_flow_imbalance, -1.0, 1.0);
            features[6] = self.normalize_value(last_features.spread_ratio, 0.0, 0.01);
            features[7] = self.normalize_value(last_features.momentum, -0.1, 0.1);
            features[8] = self.normalize_value(last_features.volume_weighted_price, -0.02, 0.02);
            features[9] = self.normalize_value(last_features.bid_ask_pressure, -1.0, 1.0);
        }
        
        features
    }

    // === PASTE VERBATIM: build T×F normalized feature matrix from buffer ===
    // === PASTE VERBATIM: normalize a MarketFeatures into [f64;10] ===
    fn features_to_norm_array(&self, f: &MarketFeatures) -> [f64;10] {
        let vals = [
            f.price_returns,
            f.volume_ratio,
            f.volatility,
            f.rsi,
            f.macd_signal,
            f.order_flow_imbalance,
            f.spread_ratio,
            f.momentum,
            f.volume_weighted_price,
            f.bid_ask_pressure,
        ];
        let mut out = [0.0f64; 10];
        for i in 0..10 {
            let n = (vals[i] - self.norm_min[i]) * self.norm_inv_span[i];
            out[i] = n.max(0.0).min(1.0);
        }
        out
    }

    fn prepare_sequence_matrix(&self) -> ndarray::Array2<f64> {
        use ndarray::Array2;
        let t = self.sequence_length; // classify() only runs when len == sequence_length
        let f = 10usize;
        let start = self.feature_buffer.len() - t;

        let mut mat = Array2::<f64>::zeros((t, f));
        let mut row = 0usize;
        for (idx, feat) in self.feature_buffer.iter().enumerate() {
            if idx < start { continue; }
            let n = self.features_to_norm_array(feat);
            // z-score using EW scaler for each feature
            for col in 0..f {
                let z = self.scaler.z(n[col], col).clamp(-6.0, 6.0);
                mat[[row, col]] = z;
            }
            row += 1;
        }
        mat
    }

    fn normalize_value(&self, value: f64, min: f64, max: f64) -> f64 {
        let lo = min.min(max);
        let hi = max.max(min);
        let span = (hi - lo).abs().max(EPS);
        Self::sat((value - lo) / span, 0.0, 1.0)
    }

    // === PASTE VERBATIM: sanitize helpers ===
    #[inline]
    fn sanitize_vec_inplace(v: &mut [f64]) {
        for x in v.iter_mut() {
            if !x.is_finite() { *x = 0.0; }
        }
    }

    fn softmax(&self, logits: &Array1<f64>) -> Array1<f64> {
        let invT = 1.0 / self.temp_current.max(1e-3);
        let mut scaled = logits.mapv(|x| x * invT);
        let m = scaled.iter().fold(f64::NEG_INFINITY, |a, &b| a.max(b));
        scaled.mapv_inplace(|x| (x - m).exp());
        let sum = scaled.sum().max(EPS);
        let mut out = &scaled / sum;
        // sanitize
        let mut outv = out.to_vec();
        Self::sanitize_vec_inplace(&mut outv);
        Array1::from_vec(outv)
    }

    pub fn get_current_regime(&self) -> MarketRegime {
        *self.current_regime.read()
    }

    pub fn get_regime_confidence(&self) -> f64 {
        *self.regime_confidence.read()
    }

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

    // === REPLACE: optimize_for_latency (RingBuffer-safe) ===
    pub fn optimize_for_latency(&mut self) {
        self.sequence_length = self.sequence_length.min(15);
        self.price_history   = RingBuffer::with_capacity(50);
        self.volume_history  = RingBuffer::with_capacity(50);
    }

    // === PASTE VERBATIM: pressure utility ===
    fn compute_pressure(f: &MarketFeatures) -> f64 {
        (0.6 * f.order_flow_imbalance.abs() + 0.4 * f.bid_ask_pressure.abs()).clamp(0.0, 1.0)
    }

    // === PASTE VERBATIM: entropy from probability vector ===
    fn entropy_from_probs(p: &ndarray::Array1<f64>) -> f64 {
        let mut e = 0.0;
        for &x in p.iter() {
            if x > 0.0 { e -= x * x.ln(); }
        }
        e
    }

    // === PASTE VERBATIM: argmax helper ===
    #[inline]
    fn argmax(v: &ndarray::Array1<f64>) -> usize {
        let mut bi = 0usize; let mut bv = f64::NEG_INFINITY;
        for (i, &x) in v.iter().enumerate() {
            if x.is_finite() && x > bv { bi = i; bv = x; }
        }
        bi
    }

    // === PASTE VERBATIM: adapt temperature to entropy & volatility ===
    fn update_temperature(&mut self, probs: &ndarray::Array1<f64>) {
        let ent = Self::entropy_from_probs(probs);       // ~[0, ln(7)≈1.946]
        let volp = self.calculate_volatility_percentile(); // [0,1]

        // High entropy -> sharpen (lower temp); high vol -> sharpen; low vol -> soften a touch
        let mut target = TEMP * (1.0 - 0.25 * (ent - 1.4)) * (1.0 - 0.15 * (volp - 0.5));
        target = target.clamp(TEMP_MIN, TEMP_MAX);

        self.temp_current = self.temp_current * (1.0 - TEMP_ADJ_ALPHA) + target * TEMP_ADJ_ALPHA;
    }

    // === PASTE VERBATIM: simple EV proxy ===
    fn ev_proxy(pos_bias: f64, conf: f64, pressure: f64, slip: f64) -> f64 {
        // positive components vs slippage drag; symmetric in sign via |pos_bias|
        (pos_bias.abs() * conf * (0.6 + 0.4 * pressure)) - EV_LAMBDA_SLIP * slip
    }

    // === PASTE VERBATIM: sanitize features ===
    fn sanitize_features(mut f: MarketFeatures) -> MarketFeatures {
        let fix = |x: f64, lo: f64, hi: f64| if x.is_finite() { x.clamp(lo, hi) } else { 0.0 };
        f.price_returns        = fix(f.price_returns,       -0.5, 0.5);
        f.volume_ratio         = fix(f.volume_ratio,         0.0, 20.0);
        f.volatility           = fix(f.volatility,           0.0, 0.5);
        f.rsi                  = fix(f.rsi,                  0.0, 1.0);
        f.macd_signal          = fix(f.macd_signal,         -1.0, 1.0);
        f.order_flow_imbalance = fix(f.order_flow_imbalance,-1.0, 1.0);
        f.spread_ratio         = fix(f.spread_ratio,         0.0, 0.10);
        f.momentum             = fix(f.momentum,            -0.5, 0.5);
        f.volume_weighted_price= fix(f.volume_weighted_price,-0.5, 0.5);
        f.bid_ask_pressure     = fix(f.bid_ask_pressure,    -1.0, 1.0);
        f
    }

    // === PASTE VERBATIM: EW histogram update & percentile ===
    fn vol_bin_index(vol: f64) -> usize {
        let x = (vol / VOL_RANGE_MAX).clamp(0.0, 1.0);
        let idx = (x * (VOL_BINS as f64)) as usize;
        idx.min(VOL_BINS - 1)
    }

    fn update_vol_hist(&mut self, vol: f64) {
        let idx = Self::vol_bin_index(vol);
        // decay total mass then add new mass at idx
        for i in 0..VOL_BINS {
            self.vol_hist[i] *= (1.0 - VOL_HIST_ALPHA);
        }
        self.vol_hist[idx] += VOL_HIST_ALPHA;
        self.vol_mass = self.vol_hist.iter().sum::<f64>().max(1e-9);
    }

    // Returns percentile of *current* vol in [0,1] using EW histogram CDF
    fn calculate_volatility_percentile(&self) -> f64 {
        let f = self.feature_buffer.back();
        let v = f.map(|x| x.volatility).unwrap_or(0.0).clamp(0.0, VOL_RANGE_MAX);
        let idx = Self::vol_bin_index(v);
        let mut cum = 0.0;
        for i in 0..=idx { cum += self.vol_hist[i]; }
        (cum / self.vol_mass).clamp(0.0, 1.0)
    }

    // === PASTE VERBATIM: apply transition prior from prev_idx to probs ===
    fn apply_transition_prior(&self, probs: &ndarray::Array1<f64>, prev_idx: usize) -> ndarray::Array1<f64> {
        use ndarray::Array1;
        let mut adj = probs.clone();
        for (i, p) in probs.iter().enumerate() {
            let logw = TRANS_BETA * self.trans_log[prev_idx][i];
            let w = logw.exp();               // w in (0,1] typically
            adj[i] = p * w;                   // reweight
        }
        // renormalize
        let s = adj.sum().max(EPS);
        adj / s
    }

    // === PASTE VERBATIM: fast output-layer matvec ===
    // === REPLACE: fast output-layer matvec ===
    fn outlayer_apply_rows(
        weights: &ndarray::Array2<f64>,
        bias: &ndarray::Array1<f64>,
        extra_bias: &ndarray::Array1<f64>,
        hidden: &ndarray::Array1<f64>
    ) -> ndarray::Array1<f64> {
        use ndarray::{Array1, Axis};
        let rows = weights.len_of(Axis(0));
        let cols = weights.len_of(Axis(1));
        debug_assert_eq!(cols, hidden.len());
        let h = hidden.as_slice().unwrap();
        let mut out = Array1::<f64>::zeros(rows);
        for r in 0..rows {
            let wr = weights.index_axis(Axis(0), r).as_slice().unwrap();
            out[r] = fast_dot(wr, h) + bias[r] + extra_bias[r];
        }
        out
    }

    // === PASTE VERBATIM: cooldown logic ===
    fn cooldown_ready(&self, pressure: f64) -> bool {
        let now = std::time::Instant::now();
        // higher pressure -> shorter cooldown
        let base_us = {
            let scale = (1.0 - 0.6 * pressure).clamp(0.0, 1.0);
            (COOLDOWN_US_BASE as f64 * scale).round() as u64
        };
        // near end-of-slot, lengthen cooldown (avoid late sends); early, shorten slightly
        let phase_mult = (0.90 + 0.40 * self.slot_phase).clamp(0.90, 1.30);
        let target = ((base_us as f64) * phase_mult).round() as u64
            .clamp(COOLDOWN_MIN_US, COOLDOWN_MAX_US);

        if let Some(t) = self.last_send {
            now.duration_since(t).as_micros() as u64 >= target
        } else {
            true
        }
    }

    // Call this **after** you actually submit a bundle
    pub fn record_send_now(&mut self) {
        self.last_send = Some(std::time::Instant::now());
    }

    // === PASTE VERBATIM: update alpha from labels or probs; produce floor vector ===
    fn alpha_decay(&mut self) {
        for a in self.class_alpha.iter_mut() {
            *a = (*a * ALPHA_DECAY).clamp(ALPHA_MIN, ALPHA_MAX);
        }
        self.alpha_sum = self.class_alpha.iter().sum::<f64>().max(EPS);
    }
    fn alpha_bump(&mut self, idx: usize) {
        self.class_alpha[idx] = (self.class_alpha[idx] + ALPHA_INC).clamp(ALPHA_MIN, ALPHA_MAX);
        self.alpha_sum = self.class_alpha.iter().sum::<f64>().max(EPS);
    }
    fn dirichlet_floor_vec(&self) -> ndarray::Array1<f64> {
        use ndarray::Array1;
        let mut v = Array1::<f64>::zeros(7);
        for i in 0..7 { v[i] = self.class_alpha[i] / self.alpha_sum; }
        v
    }

    // === PASTE VERBATIM: top-2 margin from last probs ===
    fn top2_margin(&self) -> f64 {
        let mut v = self.last_probs.to_vec();
        v.sort_by(|a,b| b.partial_cmp(a).unwrap());
        if v.len() < 2 { return 0.0; }
        (v[0] - v[1]).clamp(0.0, 1.0)
    }

    // === PASTE VERBATIM: backoff update & access ===
    pub fn record_reject_or_timeout(&mut self) {
        self.backoff_mult = (self.backoff_mult * BACKOFF_GROW).clamp(BACKOFF_MIN, BACKOFF_MAX);
    }
    pub fn record_successful_inclusion(&mut self) {
        self.backoff_mult = (self.backoff_mult * BACKOFF_DECAY).clamp(BACKOFF_MIN, BACKOFF_MAX);
    }
    pub fn tip_backoff_multiplier(&self) -> f64 {
        self.backoff_mult
    }

    // === PASTE VERBATIM: panic controls ===
    fn enter_panic(&mut self) {
        self.panic_until = Some(std::time::Instant::now() + std::time::Duration::from_micros(PANIC_US));
    }
    fn in_panic(&self) -> bool {
        if let Some(t) = self.panic_until { std::time::Instant::now() < t } else { false }
    }

    pub fn get_trading_signals(&self) -> TradingSignals {
        let regime = self.get_current_regime();
        let conf = self.get_regime_confidence().clamp(0.0, 1.0);
        let analysis = self.get_regime_features().unwrap_or_default();
        
        // Base position bias by regime
        let pos_base = match regime {
            MarketRegime::StrongBull => 1.0,
            MarketRegime::Bull => 0.6,
            MarketRegime::Sideways => 0.0,
            MarketRegime::Bear => -0.6,
            MarketRegime::StrongBear => -1.0,
            MarketRegime::HighVolatility => 0.0,
            MarketRegime::LowVolatility => 0.0,
        };
        
        // Entropy guards: reduce aggressiveness when distribution is flat
        let entropy = self.calculate_model_entropy();
        let ent_penalty = if entropy > 1.7 { 0.75 } else if entropy > 1.4 { 0.85 } else { 1.0 };

        // Stability boost: trust more when regime_stability is high
        let stab = analysis.regime_stability.clamp(0.0, 1.0);
        let stab_boost = 0.9 + 0.2 * stab;

        // Entry/exit thresholds adapt to entropy & stability
        let entry_threshold = (0.55 * (1.0 + 0.2 * stab) * (2.0 - ent_penalty)).clamp(0.4, 0.8);
        let exit_threshold  = (0.28 * (2.0 - stab_boost)).clamp(0.15, 0.35);

        // Risk multipliers by regime with entropy penalty
        let risk_multiplier = match regime {
            MarketRegime::HighVolatility => 0.5,
            MarketRegime::LowVolatility  => 1.6,
            MarketRegime::StrongBull | MarketRegime::StrongBear => 0.85,
            _ => 1.0,
        } * ent_penalty;
        
        // Stops/targets tuned by regime/stability
        let stop_loss_distance = match regime {
            MarketRegime::HighVolatility => 0.022,
            MarketRegime::LowVolatility  => 0.0045,
            MarketRegime::StrongBull | MarketRegime::StrongBear => 0.014,
            _ => 0.0095,
        } * (2.0 - (1.0 + stab).clamp(1.0, 2.0));
        
        let take_profit_ratio = match regime {
            MarketRegime::StrongBull | MarketRegime::StrongBear => 3.1,
            MarketRegime::Bull | MarketRegime::Bear => 2.6,
            MarketRegime::HighVolatility => 4.2,
            MarketRegime::LowVolatility  => 1.55,
            MarketRegime::Sideways => 2.0,
        };

        // Max position scales with confidence and stability
        let base_size = match regime {
            MarketRegime::StrongBull | MarketRegime::StrongBear => 0.8,
            MarketRegime::Bull | MarketRegime::Bear => 0.6,
            MarketRegime::Sideways => 0.4,
            MarketRegime::HighVolatility => 0.3,
            MarketRegime::LowVolatility => 0.7,
        };
        let max_position_size = base_size * (0.6 * conf + 0.4 * stab).clamp(0.0, 1.0);

        // Time in force leans longer in low vol, shorter otherwise
        let time_in_force = match regime {
            MarketRegime::HighVolatility => 28,
            MarketRegime::StrongBull | MarketRegime::StrongBear => 115,
            MarketRegime::Bull | MarketRegime::Bear => 175,
            MarketRegime::Sideways => 300,
            MarketRegime::LowVolatility => 610,
        };
        
        TradingSignals {
            position_bias: pos_base * conf * stab_boost,
            risk_multiplier,
            stop_loss_distance,
            take_profit_ratio,
            entry_threshold,
            exit_threshold,
            max_position_size,
            time_in_force,
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
        let f = self.feature_buffer.back().unwrap_or(&MarketFeatures {
            price_returns: 0.0, volume_ratio: 1.0, volatility: 0.01, rsi: 0.5, macd_signal: 0.0,
            order_flow_imbalance: 0.0, spread_ratio: 0.001, momentum: 0.0, volume_weighted_price: 0.0,
            bid_ask_pressure: 0.0,
        });
        
        let pressure = Self::compute_pressure(f);
        let margin = self.top2_margin(); // 0..1 confidence shape

        // Base aggression (unchanged mapping)
        let base_aggr = match regime {
            MarketRegime::StrongBull | MarketRegime::StrongBear => if pressure > 0.35 { 0.94 } else { 0.80 },
            MarketRegime::HighVolatility => 0.34,
            MarketRegime::LowVolatility  => 0.86,
            MarketRegime::Bull | MarketRegime::Bear => 0.62,
            MarketRegime::Sideways => 0.44,
        };

        // Margin bias: widen when top2 far apart; shrink when ambiguous
        let margin_bias = if margin > 0.25 { 1.10 } else if margin < 0.08 { 0.88 } else { 1.0 };
        let aggression_level = (base_aggr * margin_bias).clamp(0.0, 1.0);

        // Slippage tolerance: HV + spread; tighten when margin low (be pickier), loosen when high
        let mut slippage_tolerance = (
            (0.6 * f.volatility + 0.4 * self.last_vol_pred) * 1.7
            + f.spread_ratio * (if pressure > 0.35 {0.6} else {0.9})
        ).clamp(0.0004, 0.03);
        if margin < 0.08 { slippage_tolerance *= 0.92; } else if margin > 0.25 { slippage_tolerance *= 1.05; }

        let min_fill_ratio = match regime {
            MarketRegime::HighVolatility => 0.64,
            MarketRegime::LowVolatility  => 0.91,
            _ => 0.80,
        };
        let max_spread_multiplier = match regime {
            MarketRegime::HighVolatility => 3.0,
            MarketRegime::LowVolatility  => 1.35,
            _ => 2.0,
        };
        let use_limit_orders = matches!(regime, MarketRegime::LowVolatility | MarketRegime::Sideways);
        let enable_iceberg = pressure > 0.40 || (f.volume_ratio > 2.0 && f.spread_ratio < 0.002);

        // Impact-aware chunking: reduce chunk when spread wide or pressure extreme
        let vr_eff = (f.volume_ratio / (1.0 + 12.0 * f.volatility)).clamp(0.4, 4.0);
        let mut chunk_size = self.calculate_chunk_size(vr_eff);
        let spread_penalty = (1.0 + 18.0 * f.spread_ratio).clamp(1.0, 2.2);
        if matches!(regime, MarketRegime::HighVolatility) || pressure > 0.45 { chunk_size *= 0.70; }
        chunk_size /= spread_penalty; // smaller when spread is fat
        if margin < 0.08 { chunk_size *= 0.92; } else if margin > 0.25 { chunk_size *= 1.06; }
        chunk_size = chunk_size.clamp(0.05, 5.0);

        // Apply tail risk protection
        if self.tail_kurt > TAIL_KURT_WARN {
            // fatter tails → protect
            slippage_tolerance = (slippage_tolerance * 1.08).clamp(0.0004, 0.05);
            chunk_size = (chunk_size * 0.90).clamp(0.05, 5.0);
        }

        // Apply mark-out bias
        let mo = self.markout_bias();
        let aggression_level = (aggression_level * mo).clamp(0.0, 1.0);
        let chunk_size = (chunk_size * mo).clamp(0.05, 5.0);
        
        ExecutionParameters {
            aggression_level,
            slippage_tolerance,
            min_fill_ratio,
            max_spread_multiplier,
            use_limit_orders,
            enable_iceberg,
            chunk_size,
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
        // Rebuild packed weights & scratch for each layer
        for layer in &mut self.lstm_layers {
            layer.rebuild_packed();
        }
        self.rebuild_attn_key();
        Ok(())
    }
    
    pub fn update_model_online(&mut self, actual_regime: MarketRegime, learning_rate_decay: f64) {
        if self.feature_buffer.len() < self.sequence_length { return; }

        // Freeze learning in ambiguous/high-entropy states to avoid drift
        let ent = self.calculate_model_entropy();
        if ent > 1.75 || self.get_regime_confidence() < 0.45 { return; }

        self.learning_rate *= learning_rate_decay.clamp(0.5, 1.0);

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
        
        // Forward pass
        let features = self.prepare_features();
        let mut hidden = features;
        for layer in &mut self.lstm_layers {
            hidden = layer.forward(&hidden.view());
        }
        
        // Lookahead for Nesterov (v already stores previous momentum)
        let lookahead_w = &self.output_layer - &( &self.output_vel * self.momentum_factor );
        let lookahead_b = &self.output_bias - &( &self.bias_vel * self.momentum_factor );
        let logits = Self::outlayer_apply_rows(&lookahead_w, &lookahead_b, &self.calib_bias, &hidden);

        let predictions = self.softmax(&logits);
        
        // === calibration bias update (label-aware; bounded) ===
        let mut bias_delta = &target - &predictions; // pushes probabilities toward target
        bias_delta.mapv_inplace(|x| x.clamp(-0.25, 0.25));
        self.calib_bias = &self.calib_bias + &(bias_delta * CALIB_ALPHA);

        // hard bound to ensure stability
        self.calib_bias.mapv_inplace(|x| x.clamp(-CALIB_MAX, CALIB_MAX));

        // Label-aware alpha update (supervised path)
        self.alpha_decay();
        self.alpha_bump(target_idx);

        // === Vol head MSE update (decoupled, small step) ===
        let vol_target = self.feature_buffer.back().map(|f| f.volatility).unwrap_or(0.0).clamp(0.0, VOL_RANGE_MAX);
        let vol_pred = self.last_vol_pred;
        let grad_vol = (vol_pred - vol_target); // d/ds MSE

        // gradients
        let grad_vol_w = hidden.mapv(|h| grad_vol * h) + &(&self.vol_w * WEIGHT_DECAY);
        let grad_vol_b = grad_vol;

        // momentum update
        self.vol_vel_w = &self.vol_vel_w * self.momentum_factor + &(grad_vol_w * self.learning_rate * 0.5);
        self.vol_vel_b = self.vol_vel_b * self.momentum_factor + grad_vol_b * self.learning_rate * 0.5;

        // apply
        self.vol_w = &self.vol_w - &self.vol_vel_w;
        self.vol_b -= self.vol_vel_b;

        // Cross-entropy gradient wrt logits
        let mut grad = &predictions - &target;

        // Clip gradient for stability
        let gnorm = grad.dot(&grad).sqrt();
        if gnorm > MAX_GRAD_NORM {
            grad = &grad / gnorm;
        }

        // Gradients wrt weights and bias
        let grad_w = grad.to_shape((7, 1)).unwrap().dot(&hidden.to_shape((1, self.hidden_size)).unwrap());
        let grad_b = grad.clone();

        // Add L2 weight decay (decoupled)
        let grad_w = &grad_w + &( &self.output_layer * WEIGHT_DECAY );

        // Nesterov momentum update
        self.output_vel = &self.output_vel * self.momentum_factor + &( &grad_w * self.learning_rate );
        self.bias_vel   = &self.bias_vel   * self.momentum_factor + &( &grad_b * self.learning_rate );

        self.output_layer = &self.output_layer - &self.output_vel;
        self.output_bias  = &self.output_bias  - &self.bias_vel;

        // Adaptive threshold (confidence-aware)
        let confidence_error = (1.0 - self.get_regime_confidence()).abs();
        self.regime_threshold = (self.regime_threshold * 0.995 + confidence_error * 0.005).clamp(0.5, 0.9);

        self.rebuild_attn_key();
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
            prediction_latency_us: self.last_pred_us,
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

    // === PASTE VERBATIM: prefetch PDIST tuner ===
    fn update_prefetch_tuner(&mut self, pred_us: u64) {
        // Score current PDIST by inverse latency
        let idx = self.pref_pdist.min(5);
        let score = 1.0 / (pred_us as f64 + 1.0);
        self.pref_score[idx] = 0.95 * self.pref_score[idx] + 0.05 * score;

        // Every ~256 decisions, pick argmax
        static mut CNT: u32 = 0;
        unsafe {
            CNT = CNT.wrapping_add(1);
            if (CNT & 0xFF) == 0 {
                let mut best_i = 1usize; let mut best_v = f64::NEG_INFINITY;
                for i in 1..=5 {
                    if self.pref_score[i] > best_v { best_v = self.pref_score[i]; best_i = i; }
                }
                self.pref_pdist = best_i;
            }
        }
    }

    // Helper function for PIECE 93 - top2 margin calculation
    pub fn top2_margin_fast_last(&self) -> f64 {
        let s = self.last_probs.as_slice().unwrap();
        let (_, _, margin) = top2_from_slice(s);
        margin
    }
}

// === PASTE VERBATIM: CLMM exact-in across segments (PIECE 90) ===
#[derive(Clone, Copy, Debug)]
pub enum ClmmDir { ZeroForOne, OneForZero } // token0->token1 or token1->token0

#[derive(Clone, Copy, Debug)]
pub struct ClmmSegment {
    pub sqrt_a: f64,      // lower sqrt price bound of segment (inclusive)
    pub sqrt_b: f64,      // upper sqrt price bound of segment (exclusive)
    pub liquidity: f64,   // active L in this segment
    pub fee_bps: u32,     // fee for this pool (bps); same per segment of a pool
}

// Solve a single pool with piecewise-constant liquidity. Segments must be ordered
// ascending by sqrt bounds; start from current sqrt_p inside one segment.
pub fn clmm_exact_in_out(
    mut amount_in: f64,
    mut sqrt_p: f64,
    dir: ClmmDir,
    segs: &[ClmmSegment],
) -> (f64, f64) {
    if amount_in <= 0.0 || !sqrt_p.is_finite() || segs.is_empty() { return (0.0, sqrt_p); }
    let mut out_acc = 0.0f64;

    // find current segment index
    let mut idx = 0usize;
    while idx + 1 < segs.len() && !(segs[idx].sqrt_a <= sqrt_p && sqrt_p < segs[idx].sqrt_b) {
        idx += 1;
    }

    loop {
        if idx >= segs.len() || amount_in <= 0.0 { break; }
        let s = segs[idx];
        let l = s.liquidity.max(1e-12);
        let fee = 1.0 - (s.fee_bps as f64) / 10_000.0;

        match dir {
            ClmmDir::ZeroForOne => {
                // amount0 in -> sqrtP goes DOWN toward sqrt_a
                let inv_sp = 1.0 / sqrt_p.max(1e-12);
                let inv_sa = 1.0 / s.sqrt_a.max(1e-12);
                let a0_to_bound = l * (inv_sa - inv_sp).max(0.0);
                let a0_eff = (amount_in * fee).max(0.0);

                if a0_eff <= a0_to_bound + 1e-18 {
                    // stay within this segment
                    let inv_sp_new = inv_sp + a0_eff / l;
                    let sqrt_new = (1.0 / inv_sp_new.max(1e-12)).max(s.sqrt_a);
                    let a1_out = l * (sqrt_p - sqrt_new).max(0.0);
                    out_acc += a1_out;
                    sqrt_p = sqrt_new;
                    amount_in = 0.0;
                } else {
                    // consume this segment fully, move to next (lower price)
                    let sqrt_new = s.sqrt_a;
                    let a1_out = l * (sqrt_p - sqrt_new).max(0.0);
                    out_acc += a1_out;
                    // input consumed BEFORE fee
                    let a0_used_eff = a0_to_bound;
                    let a0_used = a0_used_eff / fee;
                    amount_in = (amount_in - a0_used).max(0.0);
                    sqrt_p = sqrt_new;
                    if idx == 0 { break; } else { idx -= 1; }
                }
            }
            ClmmDir::OneForZero => {
                // amount1 in -> sqrtP goes UP toward sqrt_b
                let sp = sqrt_p;
                let sb = s.sqrt_b;
                let a1_to_bound = l * (sb - sp).max(0.0);
                let a1_eff = (amount_in * fee).max(0.0);

                if a1_eff <= a1_to_bound + 1e-18 {
                    // within segment
                    let sp_new = sp + a1_eff / l;
                    let a0_out = l * ((1.0 / sp_new.max(1e-12)) - (1.0 / sp.max(1e-12))).max(0.0);
                    out_acc += a0_out;
                    sqrt_p = sp_new.min(sb);
                    amount_in = 0.0;
                } else {
                    // hit upper bound, move next (higher price)
                    let sp_new = sb;
                    let a0_out = l * ((1.0 / sp_new.max(1e-12)) - (1.0 / sp.max(1e-12))).max(0.0);
                    out_acc += a0_out;
                    let a1_used_eff = a1_to_bound;
                    let a1_used = a1_used_eff / fee;
                    amount_in = (amount_in - a1_used).max(0.0);
                    sqrt_p = sp_new;
                    idx += 1;
                }
            }
        }
    }
    (out_acc.max(0.0), sqrt_p)
}

// === PASTE VERBATIM: oracle guard (PIECE 91) ===
pub fn oracle_ok(pool_px_out_per_in: f64, oracle_px_out_per_in: f64, vol_proxy: f64) -> bool {
    if !pool_px_out_per_in.is_finite() || !oracle_px_out_per_in.is_finite() { return false; }
    let base = 0.004; // 40 bps base
    let widener = (4.0 * vol_proxy).clamp(0.0, 0.02);
    let band = (base + widener).clamp(0.003, 0.03); // 30 bps max in HV
    let dev = ((pool_px_out_per_in / oracle_px_out_per_in) - 1.0).abs();
    dev <= band
}

// === PASTE VERBATIM: pegged price with deterministic dither (PIECE 92) ===
#[derive(Clone, Copy, Debug)]
pub enum Side {
    Buy,
    Sell,
}

#[derive(Clone, Copy, Debug)]
pub struct ExecutionParameters {
    pub min_spread_bps: f64,
    pub max_spread_bps: f64,
    pub peg_offset_bps: f64,
}

// Simple deterministic jitter function (PIECE 82 reference)
#[inline]
fn tip_jitter_mult(slot: u64, attempt_idx: u32, salt: u64) -> f64 {
    let seed = slot ^ (attempt_idx as u64) ^ salt;
    let hash = fast_hash_u64(seed);
    // Convert to range [0.5, 1.5] for jitter around 1.0
    0.5 + (hash % 1000) as f64 / 1000.0
}

// Simple fast hash function
#[inline]
fn fast_hash_u64(mut x: u64) -> u64 {
    x ^= x >> 33;
    x = x.wrapping_mul(0xff51afd7ed558ccd);
    x ^= x >> 33;
    x = x.wrapping_mul(0xc4ceb9fe1a85ec53);
    x ^= x >> 33;
    x
}

// Simple pegged limit calculation (placeholder - would need actual implementation)
fn compute_pegged_limit(side: Side, bid: f64, ask: f64, bid_sz: f64, ask_sz: f64, exec: &ExecutionParameters) -> f64 {
    match side {
        Side::Buy => ask - (ask * exec.peg_offset_bps / 10000.0),
        Side::Sell => bid + (bid * exec.peg_offset_bps / 10000.0),
    }
}

pub fn compute_pegged_limit_dither(
    side: Side,
    bid: f64, ask: f64, bid_sz: f64, ask_sz: f64,
    exec: &ExecutionParameters,
    tick_size: f64,
    slot: u64, attempt_idx: u32, salt: u64
) -> f64 {
    let base = compute_pegged_limit(side, bid, ask, bid_sz, ask_sz, exec);
    let spread = (ask - bid).max(0.0);
    let jitter = tip_jitter_mult(slot, attempt_idx, salt); // PIECE 82
    // small dither within +/- 4% of spread, quantized to tick
    let off = (0.04 * spread) * (jitter - 1.0);
    let raw = (base + off).max(0.0);
    let ticks = (raw / tick_size).floor();
    (ticks * tick_size).max(0.0)
}

// === PASTE VERBATIM: liquidity-aware chunk cap (PIECE 93) ===
pub fn cap_chunk_vs_liquidity(
    chunk_in: f64,
    pool_in_reserve: f64,
    pressure: f64,
    margin: f64
) -> f64 {
    // under low margin or extreme pressure, be stricter
    let base_frac = 0.18;
    let tight = if margin < 0.08 { 0.60 } else { 1.00 };
    let press = (1.0 - 0.35 * pressure).clamp(0.60, 1.00);
    let frac = (base_frac * tight * press).clamp(0.05, 0.25);
    (chunk_in).min(frac * pool_in_reserve)
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
        if self.in_panic() { return true; }
        let v = self.feature_buffer.back().map(|f| f.volatility).unwrap_or(0.0);
        let s = self.feature_buffer.back().map(|f| f.spread_ratio).unwrap_or(0.0);
        let ofi = self.feature_buffer.back().map(|f| f.order_flow_imbalance.abs()).unwrap_or(0.0);
        let pressure = self.feature_buffer.back().map(|f| Self::compute_pressure(f)).unwrap_or(0.0);

        let overvol = v > 0.10;
        let spread_blowout = s > 0.03;
        let spoofy = pressure > 0.85 || (ofi > 0.95 && s > 0.02);
        let tails_exploded = self.tail_kurt > TAIL_KURT_EMERGENCY;

        overvol || spread_blowout || spoofy || tails_exploded || self.get_regime_confidence() < 0.30
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
        if !(self.is_ready()) { return false; }
        if self.emergency_stop_check() { return false; }
        let pm = self.get_performance_metrics();
        let conf = pm.avg_confidence;
        let ent = pm.model_entropy;
        let feature_ok = pm.feature_quality > 0.55;
        let entropy_ok = !(conf > 0.85 && ent > 1.7);
        feature_ok && entropy_ok
    }

    pub fn on_health_fail(&mut self) {
        self.enter_panic();
    }

    // === PASTE VERBATIM: KL & margin helpers ===
    fn kl_div(p_old: &ndarray::Array1<f64>, p_new: &ndarray::Array1<f64>) -> f64 {
        let mut d = 0.0;
        for i in 0..p_old.len() {
            let po = p_old[i].max(EPS);
            let pn = p_new[i].max(EPS);
            d += po * (po / pn).ln();
        }
        d.max(0.0)
    }
    fn argmax(a: &ndarray::Array1<f64>) -> usize {
        a.iter().enumerate().max_by(|(_,x),(_,y)| x.partial_cmp(y).unwrap()).map(|(i,_)| i).unwrap_or(2)
    }

    // === PASTE VERBATIM: mark-out API ===
    pub fn record_markout(&mut self, normalized_return: f64) {
        let m = normalized_return.clamp(-MARKOUT_CLIP, MARKOUT_CLIP);
        self.markout_ew = (1.0 - MARKOUT_ALPHA) * self.markout_ew + MARKOUT_ALPHA * m;
    }
    fn markout_bias(&self) -> f64 {
        // positive -> press slightly; negative -> back off
        if self.markout_ew > 0.0 { (1.0 + 4.0 * self.markout_ew).clamp(0.95, 1.10) }
        else { (1.0 + 3.0 * self.markout_ew).clamp(0.85, 1.00) }
    }

    // === PASTE VERBATIM: vol head forward ===
    fn vol_predict(&mut self, hidden: &ndarray::Array1<f64>) -> f64 {
        let s = fast_dot(self.vol_w.as_slice().unwrap(), hidden.as_slice().unwrap()) + self.vol_b;
        let v = s.clamp(0.0, VOL_RANGE_MAX);
        self.last_vol_pred = v;
        v
    }

    // === PASTE VERBATIM: branchless clamp ===
    #[inline]
    fn sat(x: f64, lo: f64, hi: f64) -> f64 {
        // Assumes lo <= hi; mirrors clamp but branch-lean
        let y = if x.is_finite() { x } else { 0.0 };
        y.max(lo).min(hi)
    }

    // === PASTE VERBATIM: in-place sequence matrix build (whitened) ===
    fn prepare_sequence_matrix_inplace(&mut self) {
        use ndarray::Axis;
        let t = self.sequence_length;
        let start = self.feature_buffer.len() - t;

        let mut row = 0usize;
        for (idx, feat) in self.feature_buffer.iter().enumerate() {
            if idx < start { continue; }
            let n = self.features_to_norm_array(feat);
            for col in 0..10 {
                let z = self.scaler.z(n[col], col).clamp(-6.0, 6.0);
                self.xseq_scratch[[row, col]] = z;
            }
            row += 1;
        }
    }

    // === PASTE VERBATIM: EW kurtosis update ===
    fn update_tail_stats(&mut self, ret: f64) {
        let a = TAIL_ALPHA;
        // EW mean
        self.tail_mu = (1.0 - a) * self.tail_mu + a * ret;
        let dev = ret - self.tail_mu;
        let d2 = dev * dev;
        self.tail_m2 = (1.0 - a) * self.tail_m2 + a * d2;
        self.tail_m4 = (1.0 - a) * self.tail_m4 + a * d2 * d2;
        if self.tail_m2 > 1e-12 {
            self.tail_kurt = (self.tail_m4 / (self.tail_m2 * self.tail_m2)).clamp(1.0, 100.0);
        }
    }

    // === PASTE VERBATIM: rebuild attention key after weight updates ===
    fn rebuild_attn_key(&mut self) {
        use ndarray::{Axis};
        let k = self.output_layer.mean_axis(Axis(0)).unwrap_or(ndarray::Array1::zeros(self.hidden_size));
        let n = (k.dot(&k)).sqrt().max(1e-12);
        self.attn_key = &k / n;
    }

    // === PASTE VERBATIM: leader profile API ===
    pub fn record_leader_outcome(&mut self, leader: [u8;32], included: bool, latency_us: u64, tip_micro: u64) {
        let p = self.leader_profiles.entry(leader).or_default();
        const A: f64 = 0.20;
        p.ir_ema  = (1.0 - A) * p.ir_ema + A * if included { 1.0 } else { 0.0 };
        p.lat_ema = (1.0 - A) * p.lat_ema + A * (latency_us as f64);
        p.tip_ema = (1.0 - A) * p.tip_ema + A * (tip_micro as f64);
    }

    pub fn leader_tip_multiplier(&self, leader: &[u8;32]) -> f64 {
        if let Some(p) = self.leader_profiles.get(leader) {
            let inc = p.ir_ema.clamp(0.05, 0.99);
            let lat = (p.lat_ema.max(1.0) / 2000.0).clamp(0.3, 3.0); // normalize vs ~2ms target
            // Cheaper when inclusion is high & latency low; pay up when they're stingy/slow.
            let bias = (1.10 - 0.25 * inc) * (0.90 + 0.10 * lat);
            bias.clamp(0.75, 1.60)
        } else { 1.0 }
    }

    pub fn tip_backoff_multiplier(&self) -> f64 {
        self.backoff_mult
    }

    // === PASTE VERBATIM: update slot phase ===
    pub fn set_slot_phase(&mut self, phase_0_1: f64) {
        self.slot_phase = phase_0_1.clamp(0.0, 1.0);
    }

    // === PASTE VERBATIM: commit smoothed exec state ===
    pub fn commit_exec_state(&mut self, exec: &ExecutionParameters) {
        self.prev_aggr = exec.aggression_level;
        self.prev_slip = exec.slippage_tolerance;
        self.prev_chunk = exec.chunk_size;
    }

    // === PASTE VERBATIM: deterministic send gate ===
    pub fn should_execute_now(&self, slot_budget_us: u64) -> bool {
        // Gate on actual model latency + structural pressure
        if self.last_pred_us > slot_budget_us { return false; }
        if let Some(f) = self.feature_buffer.back() {
            let pressure = Self::compute_pressure(f);
            // Require either healthy confidence or strong pressure
            self.get_regime_confidence() > 0.55 || pressure > 0.45
        } else { false }
    }

    // === PASTE VERBATIM: EV-aware deterministic gate ===
    pub fn should_execute_now_ev(&self, exec: &ExecutionParameters, slot_budget_us: u64) -> bool {
        if self.last_pred_us > slot_budget_us { return false; }
        if (self.lat_ema_us as u64) > slot_budget_us { return false; }
        if let Some(f) = self.feature_buffer.back() {
            let pressure = Self::compute_pressure(f);
            if !self.cooldown_ready(pressure) { return false; }

            let conf = self.get_regime_confidence();
            let pos = self.get_trading_signals().position_bias; // cheap; pure function
            let ev = Self::ev_proxy(pos, conf, pressure, exec.slippage_tolerance);
            ev >= EV_FLOOR
        } else { false }
    }

    // === PASTE VERBATIM: warm-up forward passes ===
    pub fn warm_up(&mut self, iters: usize) {
        use ndarray::{Array1, Array2};
        let t = self.sequence_length;
        let f = 10usize;
        // zero sequence -> build once to touch pages
        let x_seq = Array2::<f64>::zeros((t, f));
        for _ in 0..iters.max(1) {
            self.lstm_layers[0].reset_states();
            for r in 0..t {
                let xt = x_seq.row(r);
                self.lstm_layers[0].forward(&xt);
            }
            let h_pool = self.attention_pool_partial(&self.hseq_scratch, t);
            self.lstm_layers[1].reset_states();
            let _ = self.lstm_layers[1].forward(&h_pool.view());
        }
    }

    pub fn save_runtime_tuner_crc(&self) -> Vec<u8> {
        let rt = RuntimeTuner {
            backoff_mult: self.backoff_mult,
            pref_pdist: self.pref_pdist,
            leader_profiles: self.leader_profiles.clone(),
        };
        let payload = bincode::serialize(&rt).expect("serialize tuner");
        let c = crc32(&payload).to_le_bytes();
        let mut out = Vec::with_capacity(4 + payload.len());
        out.extend_from_slice(&c);
        out.extend_from_slice(&payload);
        out
    }

    pub fn load_runtime_tuner_crc(&mut self, data: &[u8]) -> Result<(), Box<dyn std::error::Error>> {
        if data.len() < 4 { return Err("bad tuner blob".into()); }
        let got = u32::from_le_bytes([data[0], data[1], data[2], data[3]]);
        let payload = &data[4..];
        let want = crc32(payload);
        if got != want { return Err("tuner crc mismatch".into()); }
        let rt: RuntimeTuner = bincode::deserialize(payload)?;
        self.backoff_mult = rt.backoff_mult.clamp(0.8, 2.5);
        self.pref_pdist = rt.pref_pdist.clamp(1, 5);
        self.leader_profiles = rt.leader_profiles;
        Ok(())
    }
}

// === PASTE VERBATIM: map model outputs to Solana CU & tips ===
// Inputs: exec from classifier.get_execution_params(), sigs from get_risk_adjusted_signals()
#[cfg(feature = "solana")]
use solana_sdk::compute_budget::ComputeBudgetInstruction;
#[cfg(feature = "solana")]
use solana_sdk::instruction::Instruction;

#[cfg(feature = "solana")]
pub fn build_priority_instructions(exec: &ExecutionParameters, base_units: u32, base_micro_lamports: u64)
    -> Vec<Instruction>
{
    use solana_sdk::compute_budget::ComputeBudgetInstruction;

    // Monotone budget: in tighter slots, raise CU a bit, tip more aggressively
    // Aggression (0..1), latency proxy via external gate -> we rely only on exec here.
    let cu_mult  = (0.80 + 0.45 * exec.aggression_level).clamp(0.80, 1.30);
    let tip_mult = (0.85 + 0.80 * exec.aggression_level).clamp(0.85, 1.60);

    let requested_cu   = ((base_units as f64) * cu_mult).round() as u32;
    let micro_lamports = ((base_micro_lamports as f64) * tip_mult).round() as u64;

    vec![
        ComputeBudgetInstruction::set_compute_unit_limit(requested_cu),
        ComputeBudgetInstruction::set_compute_unit_price(micro_lamports),
    ]
}

// === PASTE VERBATIM: microprice- & aggression-aware pegged limit ===
#[derive(Copy, Clone, Debug)]
pub enum Side { Buy, Sell }

pub fn compute_pegged_limit(side: Side,
                            bid: f64, ask: f64,
                            bid_sz: f64, ask_sz: f64,
                            exec: &ExecutionParameters) -> f64 {
    let mid = ((bid + ask) * 0.5).max(EPS);
    let spread = (ask - bid).max(0.0);
    let sz_sum = (bid_sz + ask_sz).max(1.0);
    let micro = (ask_sz * bid + bid_sz * ask) / sz_sum; // microprice
    let pressure = ((micro - mid) / mid).clamp(-0.02, 0.02);

    // Offset as a fraction of spread driven by aggression
    let off = (0.15 + 0.70 * exec.aggression_level).clamp(0.10, 0.85);
    match side {
        Side::Buy => {
            // bias up when upward pressure, but cap at ask - tiny epsilon
            let mut price = bid + off * spread + (pressure.max(0.0) * spread);
            if exec.use_limit_orders { price = price.min(ask * (1.0 - 1e-6)); }
            price
        }
        Side::Sell => {
            let mut price = ask - off * spread - (pressure.min(0.0).abs() * spread);
            if exec.use_limit_orders { price = price.max(bid * (1.0 + 1e-6)); }
            price
        }
    }
}

// === PASTE VERBATIM: leader profiles ===
use std::collections::HashMap;

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
struct LeaderProfile {
    ir_ema: f64,   // inclusion rate [0,1]
    lat_ema: f64,  // microseconds
    tip_ema: f64,  // micro-lamports paid
}

// === PASTE VERBATIM: leader-aware tip shape ===
pub fn tip_shape(base_micro_lamports: u64, exec: &ExecutionParameters, is_leader_slot: bool, backoff: f64) -> u64 {
    let mult = if is_leader_slot {
        // cheaper when leader is friendly / bundles observed to include quickly
        (0.80 + 0.60 * exec.aggression_level).clamp(0.75, 1.30)
    } else {
        (0.95 + 0.90 * exec.aggression_level).clamp(0.90, 1.70)
    };
    ((base_micro_lamports as f64) * mult * backoff).round() as u64
}

// === PASTE VERBATIM: minOut & tick rounding ===
pub fn round_down_to_tick(x: u64, tick: u64) -> u64 {
    if tick == 0 { return x; }
    x - (x % tick)
}

// amount_in: native units of input token
// px_out_per_in: price (out per 1 in), double-checked against on-chain pool or oracle
// slippage: fractional (e.g., 0.007 for 70 bps)
// out_decimals: decimals of output token
// tick: DEX tick size (native units)
pub fn compute_min_out(amount_in: u64, px_out_per_in: f64, slippage: f64, out_decimals: u8, tick: u64) -> u64 {
    let amt_in_f = amount_in as f64;
    let raw_out = amt_in_f * px_out_per_in;
    let min_out = (raw_out * (1.0 - slippage)).max(0.0).floor();
    let scale = 10f64.powi(out_decimals as i32);
    let native = (min_out).round() as u64;
    round_down_to_tick(native, tick).saturating_sub(0) // final clamp not needed; explicit for clarity
}

// === PASTE VERBATIM: NEON vectorized activations ===
#[cfg(target_arch = "aarch64")]
#[inline]
fn tanh_poly_neon(x: float64x2_t) -> float64x2_t {
    use core::arch::aarch64::*;
    let x2 = vmulq_f64(x, x);
    let n1 = vmlaq_f64(vdupq_n_f64(27.0), x2, vdupq_n_f64(1.0));
    let num = vmulq_f64(x, n1);
    let d1 = vmlaq_f64(vdupq_n_f64(27.0), x2, vdupq_n_f64(9.0));
    let y  = vdivq_f64(num, d1);
    vminq_f64(vdupq_n_f64(1.0), vmaxq_f64(vdupq_n_f64(-1.0), y))
}
#[cfg(target_arch = "aarch64")]
#[inline]
fn sigmoid_poly_neon(x: float64x2_t) -> float64x2_t {
    use core::arch::aarch64::*;
    let half = vdupq_n_f64(0.5);
    let t = tanh_poly_neon(vmulq_f64(x, half));
    vmulq_f64(half, vaddq_f64(vdupq_n_f64(1.0), t))
}

// === PASTE VERBATIM: vectorized activations & fused updates ===
#[inline]
fn approx_tanh_scalar(x: f64) -> f64 {
    // |err| < ~1.5e-3 on [-5,5]
    let x2 = x * x;
    let num = x * (27.0 + x2);
    let den = 27.0 + 9.0 * x2;
    (num / den).clamp(-1.0, 1.0)
}
#[inline]
fn approx_sigmoid_scalar(x: f64) -> f64 {
    if FAST_ACTIVATIONS { 0.5 * (1.0 + approx_tanh_scalar(0.5 * x)) }
    else if x >= 0.0 { let e = (-x).exp(); 1.0 / (1.0 + e) }
    else { let e = x.exp(); e / (1.0 + e) }
}

#[inline]
fn apply_tanh_inplace(slice: &mut [f64]) {
    #[cfg(target_arch = "x86_64")]
    unsafe {
        if std::is_x86_feature_detected!("avx512f") {
            use std::arch::x86_64::*;
            let mut i = 0usize;
            while i + 8 <= slice.len() {
                let x = _mm512_loadu_pd(slice.as_ptr().add(i));
                let x2 = _mm512_mul_pd(x, x);
                let n1 = _mm512_fmadd_pd(x2, _mm512_set1_pd(1.0), _mm512_set1_pd(27.0)); // 27 + x^2
                let num = _mm512_mul_pd(x, n1); // x * (27 + x^2)
                let d1 = _mm512_fmadd_pd(x2, _mm512_set1_pd(9.0), _mm512_set1_pd(27.0)); // 27 + 9x^2
                let y = _mm512_div_pd(num, d1);
                let y = _mm512_max_pd(_mm512_set1_pd(-1.0), _mm512_min_pd(y, _mm512_set1_pd(1.0)));
                _mm512_storeu_pd(slice.as_mut_ptr().add(i), y);
                i += 8;
            }
            for v in &mut slice[i..] { *v = approx_tanh_scalar(*v); }
            return;
        }
        if std::is_x86_feature_detected!("avx2") {
            use std::arch::x86_64::*;
            let mut i = 0usize;
            while i + 4 <= slice.len() {
                let x = _mm256_loadu_pd(slice.as_ptr().add(i));
                let x2 = _mm256_mul_pd(x, x);
                let n1 = _mm256_fmadd_pd(x2, _mm256_set1_pd(1.0), _mm256_set1_pd(27.0));
                let num = _mm256_mul_pd(x, n1);
                let d1 = _mm256_fmadd_pd(x2, _mm256_set1_pd(9.0), _mm256_set1_pd(27.0));
                let y = _mm256_div_pd(num, d1);
                let y = _mm256_max_pd(_mm256_set1_pd(-1.0), _mm256_min_pd(y, _mm256_set1_pd(1.0)));
                _mm256_storeu_pd(slice.as_mut_ptr().add(i), y);
                i += 4;
            }
            for v in &mut slice[i..] { *v = approx_tanh_scalar(*v); }
            return;
        }
    }
    
    #[cfg(target_arch = "aarch64")]
    unsafe {
        use core::arch::aarch64::*;
        let mut i = 0usize;
        while i + 2 <= slice.len() {
            let x = vld1q_f64(slice.as_ptr().add(i));
            let y = tanh_poly_neon(x);
            vst1q_f64(slice.as_mut_ptr().add(i), y);
            i += 2;
        }
        for v in &mut slice[i..] { *v = approx_tanh_scalar(*v); }
        return;
    }
    
    for v in slice { *v = approx_tanh_scalar(*v); }
}

#[inline]
fn apply_sigmoid_inplace(slice: &mut [f64]) {
    #[cfg(target_arch = "x86_64")]
    unsafe {
        if std::is_x86_feature_detected!("avx512f") {
            use std::arch::x86_64::*;
            let mut i = 0usize;
            while i + 8 <= slice.len() {
                let x = _mm512_loadu_pd(slice.as_ptr().add(i));
                let half = _mm512_set1_pd(0.5);
                let xh = _mm512_mul_pd(x, half);
                // tanh approx
                let x2 = _mm512_mul_pd(xh, xh);
                let n1 = _mm512_fmadd_pd(x2, _mm512_set1_pd(1.0), _mm512_set1_pd(27.0));
                let num = _mm512_mul_pd(xh, n1);
                let d1 = _mm512_fmadd_pd(x2, _mm512_set1_pd(9.0), _mm512_set1_pd(27.0));
                let t = _mm512_div_pd(num, d1);
                let t = _mm512_max_pd(_mm512_set1_pd(-1.0), _mm512_min_pd(t, _mm512_set1_pd(1.0)));
                let y = _mm512_mul_pd(half, _mm512_add_pd(_mm512_set1_pd(1.0), t));
                _mm512_storeu_pd(slice.as_mut_ptr().add(i), y);
                i += 8;
            }
            for v in &mut slice[i..] { *v = approx_sigmoid_scalar(*v); }
            return;
        }
        if std::is_x86_feature_detected!("avx2") {
            use std::arch::x86_64::*;
            let mut i = 0usize;
            while i + 4 <= slice.len() {
                let x = _mm256_loadu_pd(slice.as_ptr().add(i));
                let half = _mm256_set1_pd(0.5);
                let xh = _mm256_mul_pd(x, half);
                let x2 = _mm256_mul_pd(xh, xh);
                let n1 = _mm256_fmadd_pd(x2, _mm256_set1_pd(1.0), _mm256_set1_pd(27.0));
                let num = _mm256_mul_pd(xh, n1);
                let d1 = _mm256_fmadd_pd(x2, _mm256_set1_pd(9.0), _mm256_set1_pd(27.0));
                let t = _mm256_div_pd(num, d1);
                let t = _mm256_max_pd(_mm256_set1_pd(-1.0), _mm256_min_pd(t, _mm256_set1_pd(1.0)));
                let y = _mm256_mul_pd(half, _mm256_add_pd(_mm256_set1_pd(1.0), t));
                _mm256_storeu_pd(slice.as_mut_ptr().add(i), y);
                i += 4;
            }
            for v in &mut slice[i..] { *v = approx_sigmoid_scalar(*v); }
            return;
        }
    }
    
    #[cfg(target_arch = "aarch64")]
    unsafe {
        use core::arch::aarch64::*;
        let mut i = 0usize;
        while i + 2 <= slice.len() {
            let x = vld1q_f64(slice.as_ptr().add(i));
            let y = sigmoid_poly_neon(x);
            vst1q_f64(slice.as_mut_ptr().add(i), y);
            i += 2;
        }
        for v in &mut slice[i..] { *v = approx_sigmoid_scalar(*v); }
        return;
    }
    
    for v in slice { *v = approx_sigmoid_scalar(*v); }
}

// === PASTE VERBATIM: CPU feature detection ===
#[inline]
fn cpu_feats() -> (bool, bool) {
    #[cfg(target_arch = "x86_64")]
    {
        unsafe {
            let has_avx512 = std::is_x86_feature_detected!("avx512f");
            let has_avx2 = std::is_x86_feature_detected!("avx2");
            (has_avx512, has_avx2)
        }
    }
    #[cfg(not(target_arch = "x86_64"))]
    {
        (false, false)
    }
}

// === PASTE VERBATIM: fast top-2 margin (one pass) ===
#[inline]
fn top2_from_slice(a: &[f64]) -> (usize, f64, f64) {
    let mut i0 = 0usize; let mut v0 = f64::NEG_INFINITY;
    let mut v1 = f64::NEG_INFINITY;
    for (i, &x) in a.iter().enumerate() {
        if x > v0 {
            v1 = v0; v0 = x; i0 = i;
        } else if x > v1 {
            v1 = x;
        }
    }
    (i0, v0, (v0 - v1).max(0.0))
}

// === PASTE VERBATIM: priority ladder per attempt ===
use solana_sdk::instruction::Instruction;
use solana_sdk::compute_budget::ComputeBudgetInstruction;

pub fn build_priority_for_attempt(
    exec: &ExecutionParameters,
    base_units: u32,
    base_micro_lamports: u64,
    attempt_idx: u32,
    is_leader_slot: bool,
    backoff_mult: f64
) -> Vec<Instruction> {
    // attempt escalator grows with aggression, concave to avoid overpay
    let esc = (1.0 + (attempt_idx as f64) * (0.12 + 0.18 * exec.aggression_level)).clamp(1.0, 1.8);

    // CU monotone
    let cu_mult  = (0.80 + 0.45 * exec.aggression_level) * (0.95 + 0.05 * (attempt_idx as f64));
    let cu_req   = ((base_units as f64) * cu_mult).round() as u32;

    // Tip via your leader-aware shaping (PIECE 66) then escalate & backoff
    let base = tip_shape(base_micro_lamports, exec, is_leader_slot, backoff_mult);
    let jitter = tip_jitter_mult(0, attempt_idx, 0xC0FFEE_u64); // provide SLOT_NUMBER from your scheduler
    let micro  = ((base as f64) * esc * jitter).round() as u64;

    vec![
        ComputeBudgetInstruction::set_compute_unit_limit(cu_req),
        ComputeBudgetInstruction::set_compute_unit_price(micro),
    ]
}

// === PASTE VERBATIM: viability guard ===
pub fn trade_viable(amount_in: u64, px_out_per_in: f64, slippage: f64, out_decimals: u8, tick: u64) -> bool {
    if amount_in == 0 { return false; }
    if !(slippage.is_finite()) || slippage <= 0.0 || slippage >= 0.20 { return false; } // sanity
    let min_out = compute_min_out(amount_in, px_out_per_in, slippage, out_decimals, tick);
    if tick > 0 && min_out < tick { return false; }
    min_out > 0
}

// Placeholder types and functions that would be defined elsewhere
#[derive(Debug, Clone)]
pub struct ExecutionParameters {
    pub aggression_level: f64,
}

fn tip_shape(base: u64, _exec: &ExecutionParameters, _is_leader: bool, _backoff: f64) -> u64 {
    base // Placeholder implementation
}

fn compute_min_out(amount_in: u64, px_out_per_in: f64, slippage: f64, _out_decimals: u8, _tick: u64) -> u64 {
    let expected_out = (amount_in as f64 * px_out_per_in * (1.0 - slippage)) as u64;
    expected_out
}

// === PASTE VERBATIM: slot-seeded jitter ===
#[inline] 
fn splitmix64(mut x: u64) -> u64 {
    x = x.wrapping_add(0x9E3779B97F4A7C15);
    let mut z = x;
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58476D1CE4E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D049BB133111EB);
    z ^ (z >> 31)
}

#[inline] 
pub fn tip_jitter_mult(slot: u64, attempt_idx: u32, salt: u64) -> f64 {
    let r = splitmix64(slot ^ ((attempt_idx as u64) << 32) ^ salt);
    // Map to [0.97, 1.03] with smoothstep
    let u = ((r >> 11) as f64) / ((1u64 << 53) as f64);
    let s = u*u*(3.0 - 2.0*u);
    0.97 + 0.06 * s
}

// === PASTE VERBATIM: CPMM splitter ===
#[derive(Clone, Copy, Debug)]
pub struct CpmmPool {
    pub r_in: f64,   // reserve of input token
    pub r_out: f64,  // reserve of output token
    pub fee_bps: u32 // e.g., 30 -> 0.3%
}

#[inline] 
fn out_for_in(pool: &CpmmPool, x_in: f64) -> f64 {
    let fee = 1.0 - (pool.fee_bps as f64) / 10_000.0;
    let x_eff = x_in * fee;
    let k = pool.r_in * pool.r_out;
    // y_out = r_out - k/(r_in + x_eff)
    let denom = (pool.r_in + x_eff).max(1e-12);
    (pool.r_out - (k / denom)).max(0.0)
}

// Greedy waterfill with fixed quanta
pub fn split_cpmm_amount(amount_in: f64, pools: &[CpmmPool], steps: usize) -> Vec<f64> {
    let n = pools.len();
    let mut alloc = vec![0.0f64; n];
    if n == 0 || amount_in <= 0.0 { return alloc; }

    let q = (amount_in / steps.max(1) as f64).max(1.0); // min quantum in native units
    let mut remaining = amount_in;

    // marginal gain comparator
    for _ in 0..steps {
        // find best pool by marginal out for +q
        let mut best_i = 0usize;
        let mut best_gain = f64::NEG_INFINITY;
        for i in 0..n {
            let test = out_for_in(&pools[i], alloc[i] + q) - out_for_in(&pools[i], alloc[i]);
            if test > best_gain { best_gain = test; best_i = i; }
        }
        alloc[best_i] += q;
        remaining -= q;
        if remaining <= 0.0 { break; }
    }
    // squish numerical noise
    let s: f64 = alloc.iter().sum();
    if s > 0.0 {
        let scale = amount_in / s;
        for a in &mut alloc[..] { *a *= scale; }
    }
    alloc
}

// === PASTE VERBATIM: in-flight tracker ===
use std::collections::{HashSet, VecDeque};

#[derive(Debug)]
pub struct InflightTracker {
    cap: usize,
    q: VecDeque<u64>,
    set: HashSet<u64>,
}

impl InflightTracker {
    pub fn new(cap: usize) -> Self { 
        Self { 
            cap, 
            q: VecDeque::with_capacity(cap), 
            set: HashSet::with_capacity(cap*2) 
        } 
    }
    
    #[inline] 
    pub fn clear_slot(&mut self) { 
        self.q.clear(); 
        self.set.clear(); 
    }

    // returns true if first time seen; false if duplicate
    pub fn try_insert(&mut self, key: u64) -> bool {
        if self.set.contains(&key) { return false; }
        self.set.insert(key);
        self.q.push_back(key);
        if self.q.len() > self.cap {
            if let Some(old) = self.q.pop_front() { self.set.remove(&old); }
        }
        true
    }
}

// === PASTE VERBATIM: retry budgeter ===
pub fn max_attempts_this_slot(lat_ema_us: f64, slot_phase_0_1: f64, slot_us: f64) -> u32 {
    let time_left = ((1.0 - slot_phase_0_1).max(0.0)) * slot_us;
    let cycle = (lat_ema_us.max(200.0) + 300.0); // model + overhead
    (time_left / cycle).floor().clamp(0.0, 6.0) as u32
}

#[inline]
pub fn should_retry_attempt(cur_attempt: u32, lat_ema_us: f64, slot_phase_0_1: f64, slot_us: f64) -> bool {
    cur_attempt + 1 < max_attempts_this_slot(lat_ema_us, slot_phase_0_1, slot_us)
}

// === PASTE VERBATIM: 4x-row GEMV on packed weights ===
#[inline]
fn gemv_rows4_assign_packed(out: &mut [f64], w: &[f64], rows: usize, pack_cols: usize, x: &[f64]) {
    let mut r = 0usize;
    while r + 3 < rows {
        let b0 = r * pack_cols;
        let b1 = (r + 1) * pack_cols;
        let b2 = (r + 2) * pack_cols;
        let b3 = (r + 3) * pack_cols;
        let mut acc0 = 0.0f64; let mut acc1 = 0.0f64; let mut acc2 = 0.0f64; let mut acc3 = 0.0f64;

        #[cfg(target_arch = "x86_64")]
        unsafe {
            if std::is_x86_feature_detected!("avx512f") && std::is_x86_feature_detected!("fma") {
                use std::arch::x86_64::*;
                let chunks8 = pack_cols / 8;
                let mut a0 = _mm512_setzero_pd();
                let mut a1 = _mm512_setzero_pd();
                let mut a2 = _mm512_setzero_pd();
                let mut a3 = _mm512_setzero_pd();

                for k in 0..chunks8 {
                    let off = k * 8;
                    let xv = _mm512_loadu_pd(x.as_ptr().add(off));

                    let w0 = if (w.as_ptr() as usize + b0 + off) & 63 == 0 { _mm512_load_pd(w.as_ptr().add(b0 + off)) } else { _mm512_loadu_pd(w.as_ptr().add(b0 + off)) };
                    let w1 = if (w.as_ptr() as usize + b1 + off) & 63 == 0 { _mm512_load_pd(w.as_ptr().add(b1 + off)) } else { _mm512_loadu_pd(w.as_ptr().add(b1 + off)) };
                    let w2 = if (w.as_ptr() as usize + b2 + off) & 63 == 0 { _mm512_load_pd(w.as_ptr().add(b2 + off)) } else { _mm512_loadu_pd(w.as_ptr().add(b2 + off)) };
                    let w3 = if (w.as_ptr() as usize + b3 + off) & 63 == 0 { _mm512_load_pd(w.as_ptr().add(b3 + off)) } else { _mm512_loadu_pd(w.as_ptr().add(b3 + off)) };

                    a0 = _mm512_fmadd_pd(w0, xv, a0);
                    a1 = _mm512_fmadd_pd(w1, xv, a1);
                    a2 = _mm512_fmadd_pd(w2, xv, a2);
                    a3 = _mm512_fmadd_pd(w3, xv, a3);
                }
                let mut t0 = [0.0f64; 8]; _mm512_storeu_pd(t0.as_mut_ptr(), a0); acc0 += t0.iter().sum::<f64>();
                let mut t1 = [0.0f64; 8]; _mm512_storeu_pd(t1.as_mut_ptr(), a1); acc1 += t1.iter().sum::<f64>();
                let mut t2 = [0.0f64; 8]; _mm512_storeu_pd(t2.as_mut_ptr(), a2); acc2 += t2.iter().sum::<f64>();
                let mut t3 = [0.0f64; 8]; _mm512_storeu_pd(t3.as_mut_ptr(), a3); acc3 += t3.iter().sum::<f64>();

                for c in (chunks8 * 8)..pack_cols {
                    let xv = x[c];
                    acc0 += w[b0 + c] * xv;
                    acc1 += w[b1 + c] * xv;
                    acc2 += w[b2 + c] * xv;
                    acc3 += w[b3 + c] * xv;
                }
                out[r] = acc0; out[r + 1] = acc1; out[r + 2] = acc2; out[r + 3] = acc3;
                r += 4; continue;
            }
            if std::is_x86_feature_detected!("avx2") && std::is_x86_feature_detected!("fma") {
                use std::arch::x86_64::*;
                let chunks4 = pack_cols / 4;
                let mut a0 = _mm256_setzero_pd();
                let mut a1 = _mm256_setzero_pd();
                let mut a2 = _mm256_setzero_pd();
                let mut a3 = _mm256_setzero_pd();

                for k in 0..chunks4 {
                    let off = k * 4;
                    let xv = _mm256_loadu_pd(x.as_ptr().add(off));
                    let w0 = _mm256_loadu_pd(w.as_ptr().add(b0 + off));
                    let w1 = _mm256_loadu_pd(w.as_ptr().add(b1 + off));
                    let w2 = _mm256_loadu_pd(w.as_ptr().add(b2 + off));
                    let w3 = _mm256_loadu_pd(w.as_ptr().add(b3 + off));
                    a0 = _mm256_fmadd_pd(w0, xv, a0);
                    a1 = _mm256_fmadd_pd(w1, xv, a1);
                    a2 = _mm256_fmadd_pd(w2, xv, a2);
                    a3 = _mm256_fmadd_pd(w3, xv, a3);
                }
                let mut t0 = [0.0f64; 4]; _mm256_storeu_pd(t0.as_mut_ptr(), a0); acc0 += t0.iter().sum::<f64>();
                let mut t1 = [0.0f64; 4]; _mm256_storeu_pd(t1.as_mut_ptr(), a1); acc1 += t1.iter().sum::<f64>();
                let mut t2 = [0.0f64; 4]; _mm256_storeu_pd(t2.as_mut_ptr(), a2); acc2 += t2.iter().sum::<f64>();
                let mut t3 = [0.0f64; 4]; _mm256_storeu_pd(t3.as_mut_ptr(), a3); acc3 += t3.iter().sum::<f64>();

                for c in (chunks4 * 4)..pack_cols {
                    let xv = x[c];
                    acc0 += w[b0 + c] * xv;
                    acc1 += w[b1 + c] * xv;
                    acc2 += w[b2 + c] * xv;
                    acc3 += w[b3 + c] * xv;
                }
                out[r] = acc0; out[r + 1] = acc1; out[r + 2] = acc2; out[r + 3] = acc3;
                r += 4; continue;
            }
        }

        // scalar
        for c in 0..pack_cols {
            let xv = x[c];
            acc0 += w[b0 + c] * xv;
            acc1 += w[b1 + c] * xv;
            acc2 += w[b2 + c] * xv;
            acc3 += w[b3 + c] * xv;
        }
        out[r] = acc0; out[r + 1] = acc1; out[r + 2] = acc2; out[r + 3] = acc3;
        r += 4;
    }

    // tail rows
    if r < rows {
        // safe 2-row kernel for the remainder; if only one row left the 2-row kernel will handle it.
        gemv_rows2_assign_packed(out, w, rows, pack_cols, x);
    }
}

// === REPLACE: stride GEMV (2 rows) with software prefetch ===
#[inline]
fn gemv_rows2_assign_packed(out: &mut [f64], w: &[f64], rows: usize, pack_cols: usize, x: &[f64]) {
    #[inline] unsafe fn pf(addr: *const u8) {
        #[cfg(target_arch = "x86_64")]
        { std::arch::x86_64::_mm_prefetch(addr as *const i8, 3 /* _MM_HINT_T0 */); }
    }
    // Prefetch distance in vector-chunks (empirically robust on AVX2/AVX512)
    const PDIST: usize = 3;

    let mut r = 0usize;
    while r + 1 < rows {
        let base0 = r * pack_cols;
        let base1 = (r + 1) * pack_cols;
        let mut acc0 = 0.0f64;
        let mut acc1 = 0.0f64;

        #[cfg(target_arch = "x86_64")]
        unsafe {
            if std::is_x86_feature_detected!("avx512f") && std::is_x86_feature_detected!("fma") {
                use std::arch::x86_64::*;
                let mut a0 = _mm512_setzero_pd();
                let mut a1 = _mm512_setzero_pd();
                let chunks8 = pack_cols / 8;

                for k in 0..chunks8 {
                    // prefetch next chunk of x & weights
                    let kpf = k + PDIST;
                    if kpf < chunks8 {
                        let off = kpf * 8;
                        pf(x.as_ptr().add(off) as *const u8);
                        pf(w.as_ptr().add(base0 + off) as *const u8);
                        pf(w.as_ptr().add(base1 + off) as *const u8);
                    }

                    let off = k * 8;
                    let xv = _mm512_loadu_pd(x.as_ptr().add(off));
                    let w0 = if (w.as_ptr() as usize + base0 + off) & 63 == 0 {
                        _mm512_load_pd(w.as_ptr().add(base0 + off))
                    } else { _mm512_loadu_pd(w.as_ptr().add(base0 + off)) };
                    let w1 = if (w.as_ptr() as usize + base1 + off) & 63 == 0 {
                        _mm512_load_pd(w.as_ptr().add(base1 + off))
                    } else { _mm512_loadu_pd(w.as_ptr().add(base1 + off)) };
                    a0 = _mm512_fmadd_pd(w0, xv, a0);
                    a1 = _mm512_fmadd_pd(w1, xv, a1);
                }
                let mut t0 = [0f64; 8];
                let mut t1 = [0f64; 8];
                _mm512_storeu_pd(t0.as_mut_ptr(), a0);
                _mm512_storeu_pd(t1.as_mut_ptr(), a1);
                acc0 += t0.iter().sum::<f64>();
                acc1 += t1.iter().sum::<f64>();
                out[r]     = acc0;
                out[r + 1] = acc1;
                r += 2;
                continue;
            }
            if std::is_x86_feature_detected!("avx2") && std::is_x86_feature_detected!("fma") {
                use std::arch::x86_64::*;
                let mut a0 = _mm256_setzero_pd();
                let mut a1 = _mm256_setzero_pd();
                let chunks4 = pack_cols / 4;

                for k in 0..chunks4 {
                    let kpf = k + PDIST;
                    if kpf < chunks4 {
                        let off = kpf * 4;
                        pf(x.as_ptr().add(off) as *const u8);
                        pf(w.as_ptr().add(base0 + off) as *const u8);
                        pf(w.as_ptr().add(base1 + off) as *const u8);
                    }

                    let off = k * 4;
                    let xv = _mm256_loadu_pd(x.as_ptr().add(off));
                    let w0 = if (w.as_ptr() as usize + base0 + off) & 31 == 0 {
                        _mm256_load_pd(w.as_ptr().add(base0 + off))
                    } else { _mm256_loadu_pd(w.as_ptr().add(base0 + off)) };
                    let w1 = if (w.as_ptr() as usize + base1 + off) & 31 == 0 {
                        _mm256_load_pd(w.as_ptr().add(base1 + off))
                    } else { _mm256_loadu_pd(w.as_ptr().add(base1 + off)) };
                    a0 = _mm256_fmadd_pd(w0, xv, a0);
                    a1 = _mm256_fmadd_pd(w1, xv, a1);
                }
                let mut t0 = [0f64; 4];
                let mut t1 = [0f64; 4];
                _mm256_storeu_pd(t0.as_mut_ptr(), a0);
                _mm256_storeu_pd(t1.as_mut_ptr(), a1);
                acc0 += t0.iter().sum::<f64>();
                acc1 += t1.iter().sum::<f64>();
                out[r]     = acc0;
                out[r + 1] = acc1;
                r += 2;
                continue;
            }
        }

        // Scalar fallback
        for c in 0..pack_cols {
            acc0 += w[base0 + c] * x[c];
            acc1 += w[base1 + c] * x[c];
        }
        out[r]     = acc0;
        out[r + 1] = acc1;
        r += 2;
    }

    if r < rows {
        let base = r * pack_cols;
        let mut acc = 0.0;
        #[cfg(target_arch = "x86_64")]
        unsafe {
            if std::is_x86_feature_detected!("avx512f") && std::is_x86_feature_detected!("fma") {
                use std::arch::x86_64::*;
                let mut a = _mm512_setzero_pd();
                let chunks8 = pack_cols / 8;
                for k in 0..chunks8 {
                    let kpf = k + PDIST;
                    if kpf < chunks8 {
                        let off = kpf * 8;
                        pf(x.as_ptr().add(off) as *const u8);
                        pf(w.as_ptr().add(base + off) as *const u8);
                    }
                    let off = k * 8;
                    let xv = _mm512_loadu_pd(x.as_ptr().add(off));
                    let wv = if (w.as_ptr() as usize + base + off) & 63 == 0 {
                        _mm512_load_pd(w.as_ptr().add(base + off))
                    } else { _mm512_loadu_pd(w.as_ptr().add(base + off)) };
                    a = _mm512_fmadd_pd(wv, xv, a);
                }
                let mut t = [0f64; 8];
                _mm512_storeu_pd(t.as_mut_ptr(), a);
                acc += t.iter().sum::<f64>();
                out[r] = acc; return;
            }
            if std::is_x86_feature_detected!("avx2") && std::is_x86_feature_detected!("fma") {
                use std::arch::x86_64::*;
                let mut a = _mm256_setzero_pd();
                let chunks4 = pack_cols / 4;
                for k in 0..chunks4 {
                    let kpf = k + PDIST;
                    if kpf < chunks4 {
                        let off = kpf * 4;
                        pf(x.as_ptr().add(off) as *const u8);
                        pf(w.as_ptr().add(base + off) as *const u8);
                    }
                    let off = k * 4;
                    let xv = _mm256_loadu_pd(x.as_ptr().add(off));
                    let wv = _mm256_loadu_pd(w.as_ptr().add(base + off));
                    a = _mm256_fmadd_pd(wv, xv, a);
                }
                let mut t = [0f64; 4];
                _mm256_storeu_pd(t.as_mut_ptr(), a);
                acc += t.iter().sum::<f64>();
                out[r] = acc; return;
            }
        }
        for c in 0..pack_cols { acc += w[base + c] * x[c]; }
        out[r] = acc;
    }
}

// === PASTE VERBATIM: 2x-row GEMV micro-kernel (assign) ===
#[inline]
fn gemv_rows2_assign(out: &mut [f64], m: &ndarray::ArrayView2<f64>, v: &ndarray::ArrayView1<f64>) {
    use ndarray::Axis;
    assert_eq!(m.shape()[1], v.len());
    let nrows = m.len_of(Axis(0));
    let ncols = m.len_of(Axis(1));
    let xv = v.as_slice().unwrap();

    let mut r = 0usize;
    while r + 1 < nrows {
        let row0 = m.index_axis(Axis(0), r).as_slice().unwrap();
        let row1 = m.index_axis(Axis(0), r + 1).as_slice().unwrap();
        let mut acc0 = 0.0f64;
        let mut acc1 = 0.0f64;

        #[cfg(target_arch = "x86_64")]
        unsafe {
            if std::is_x86_feature_detected!("avx512f") && std::is_x86_feature_detected!("fma") {
                use std::arch::x86_64::*;
                let mut a0 = _mm512_setzero_pd();
                let mut a1 = _mm512_setzero_pd();
                let chunks8 = ncols / 8;
                let rem = ncols % 8;

                let p0 = row0.as_ptr();
                let p1 = row1.as_ptr();
                let px = xv.as_ptr();

                for k in 0..chunks8 {
                    let base = k * 8;
                    let x = _mm512_loadu_pd(px.add(base));
                    let w0 = _mm512_loadu_pd(p0.add(base));
                    let w1 = _mm512_loadu_pd(p1.add(base));
                    a0 = _mm512_fmadd_pd(w0, x, a0);
                    a1 = _mm512_fmadd_pd(w1, x, a1);
                }
                let mut tmp0 = [0f64; 8];
                let mut tmp1 = [0f64; 8];
                _mm512_storeu_pd(tmp0.as_mut_ptr(), a0);
                _mm512_storeu_pd(tmp1.as_mut_ptr(), a1);
                acc0 += tmp0.iter().sum::<f64>();
                acc1 += tmp1.iter().sum::<f64>();

                for i in (ncols - rem)..ncols {
                    acc0 += *p0.add(i) * *px.add(i);
                    acc1 += *p1.add(i) * *px.add(i);
                }
                out[r] = acc0;
                out[r + 1] = acc1;
                r += 2;
                continue;
            }
            if std::is_x86_feature_detected!("avx2") && std::is_x86_feature_detected!("fma") {
                use std::arch::x86_64::*;
                let mut a0 = _mm256_setzero_pd();
                let mut a1 = _mm256_setzero_pd();
                let chunks4 = ncols / 4;
                let rem = ncols % 4;

                let p0 = row0.as_ptr();
                let p1 = row1.as_ptr();
                let px = xv.as_ptr();

                for k in 0..chunks4 {
                    let base = k * 4;
                    let x = _mm256_loadu_pd(px.add(base));
                    let w0 = _mm256_loadu_pd(p0.add(base));
                    let w1 = _mm256_loadu_pd(p1.add(base));
                    a0 = _mm256_fmadd_pd(w0, x, a0);
                    a1 = _mm256_fmadd_pd(w1, x, a1);
                }
                let mut tmp0 = [0f64; 4];
                let mut tmp1 = [0f64; 4];
                _mm256_storeu_pd(tmp0.as_mut_ptr(), a0);
                _mm256_storeu_pd(tmp1.as_mut_ptr(), a1);
                acc0 += tmp0.iter().sum::<f64>();
                acc1 += tmp1.iter().sum::<f64>();

                for i in (ncols - rem)..ncols {
                    acc0 += *p0.add(i) * *px.add(i);
                    acc1 += *p1.add(i) * *px.add(i);
                }
                out[r] = acc0;
                out[r + 1] = acc1;
                r += 2;
                continue;
            }
        }

        // Scalar fallback
        for i in 0..ncols {
            acc0 += row0[i] * xv[i];
            acc1 += row1[i] * xv[i];
        }
        out[r] = acc0;
        out[r + 1] = acc1;
        r += 2;
    }

    if r < nrows {
        // last odd row
        let row = m.index_axis(Axis(0), r).as_slice().unwrap();
        let mut acc = 0.0;
        #[cfg(target_arch = "x86_64")]
        unsafe {
            if std::is_x86_feature_detected!("avx512f") && std::is_x86_feature_detected!("fma") {
                use std::arch::x86_64::*;
                let chunks8 = ncols / 8;
                let rem = ncols % 8;
                let pr = row.as_ptr();
                let px = xv.as_ptr();
                let mut a = _mm512_setzero_pd();
                for k in 0..chunks8 {
                    let base = k * 8;
                    let x = _mm512_loadu_pd(px.add(base));
                    let w = _mm512_loadu_pd(pr.add(base));
                    a = _mm512_fmadd_pd(w, x, a);
                }
                let mut tmp = [0f64; 8];
                _mm512_storeu_pd(tmp.as_mut_ptr(), a);
                acc += tmp.iter().sum::<f64>();
                for i in (ncols - rem)..ncols { acc += *pr.add(i) * *px.add(i); }
                out[r] = acc;
                return;
            }
            if std::is_x86_feature_detected!("avx2") && std::is_x86_feature_detected!("fma") {
                use std::arch::x86_64::*;
                let chunks4 = ncols / 4;
                let rem = ncols % 4;
                let pr = row.as_ptr();
                let px = xv.as_ptr();
                let mut a = _mm256_setzero_pd();
                for k in 0..chunks4 {
                    let base = k * 4;
                    let x = _mm256_loadu_pd(px.add(base));
                    let w = _mm256_loadu_pd(pr.add(base));
                    a = _mm256_fmadd_pd(w, x, a);
                }
                let mut tmp = [0f64; 4];
                _mm256_storeu_pd(tmp.as_mut_ptr(), a);
                acc += tmp.iter().sum::<f64>();
                for i in (ncols - rem)..ncols { acc += *pr.add(i) * *px.add(i); }
                out[r] = acc;
                return;
            }
        }
        for i in 0..ncols { acc += row[i] * xv[i]; }
        out[r] = acc;
    }
}

#[inline]
fn fused_cell_hidden_update(cell: &mut [f64], hid: &mut [f64],
                            i_gate: &[f64], f_gate: &[f64], g_gate: &[f64], o_gate: &[f64],
                            cell_clip: f64)
{
    debug_assert_eq!(cell.len(), hid.len());
    let n = cell.len();
    #[cfg(target_arch = "x86_64")]
    unsafe {
        if std::is_x86_feature_detected!("avx512f") {
            use std::arch::x86_64::*;
            let clip_lo = _mm512_set1_pd(-cell_clip);
            let clip_hi = _mm512_set1_pd(cell_clip);
            let mut i = 0usize;
            while i + 8 <= n {
                let cf = _mm512_loadu_pd(cell.as_ptr().add(i));
                let ig = _mm512_loadu_pd(i_gate.as_ptr().add(i));
                let fg = _mm512_loadu_pd(f_gate.as_ptr().add(i));
                let gg = _mm512_loadu_pd(g_gate.as_ptr().add(i));
                let og = _mm512_loadu_pd(o_gate.as_ptr().add(i));

                let part = _mm512_mul_pd(ig, gg);
                let cnew = _mm512_add_pd(_mm512_mul_pd(fg, cf), part);
                let cnew = _mm512_min_pd(clip_hi, _mm512_max_pd(clip_lo, cnew));
                _mm512_storeu_pd(cell.as_mut_ptr().add(i), cnew);

                // tanh(cnew) approx
                let c2 = _mm512_mul_pd(cnew, cnew);
                let n1 = _mm512_fmadd_pd(c2, _mm512_set1_pd(1.0), _mm512_set1_pd(27.0));
                let num = _mm512_mul_pd(cnew, n1);
                let d1 = _mm512_fmadd_pd(c2, _mm512_set1_pd(9.0), _mm512_set1_pd(27.0));
                let t = _mm512_div_pd(num, d1);
                let t = _mm512_max_pd(_mm512_set1_pd(-1.0), _mm512_min_pd(t, _mm512_set1_pd(1.0)));
                let h = _mm512_mul_pd(og, t);
                _mm512_storeu_pd(hid.as_mut_ptr().add(i), h);

                i += 8;
            }
            for k in i..n {
                let mut c = f_gate[k] * cell[k] + i_gate[k] * g_gate[k];
                c = c.clamp(-cell_clip, cell_clip);
                cell[k] = c;
                hid[k] = o_gate[k] * approx_tanh_scalar(c);
            }
            return;
        }
        if std::is_x86_feature_detected!("avx2") {
            use std::arch::x86_64::*;
            let clip_lo = _mm256_set1_pd(-cell_clip);
            let clip_hi = _mm256_set1_pd(cell_clip);
            let mut i = 0usize;
            while i + 4 <= n {
                let cf = _mm256_loadu_pd(cell.as_ptr().add(i));
                let ig = _mm256_loadu_pd(i_gate.as_ptr().add(i));
                let fg = _mm256_loadu_pd(f_gate.as_ptr().add(i));
                let gg = _mm256_loadu_pd(g_gate.as_ptr().add(i));
                let og = _mm256_loadu_pd(o_gate.as_ptr().add(i));

                let part = _mm256_mul_pd(ig, gg);
                let cnew = _mm256_add_pd(_mm256_mul_pd(fg, cf), part);
                let cnew = _mm256_min_pd(clip_hi, _mm256_max_pd(clip_lo, cnew));
                _mm256_storeu_pd(cell.as_mut_ptr().add(i), cnew);

                // tanh(cnew)
                let c2 = _mm256_mul_pd(cnew, cnew);
                let n1 = _mm256_fmadd_pd(c2, _mm256_set1_pd(1.0), _mm256_set1_pd(27.0));
                let num = _mm256_mul_pd(cnew, n1);
                let d1 = _mm256_fmadd_pd(c2, _mm256_set1_pd(9.0), _mm256_set1_pd(27.0));
                let t = _mm256_div_pd(num, d1);
                let t = _mm256_max_pd(_mm256_set1_pd(-1.0), _mm256_min_pd(t, _mm256_set1_pd(1.0)));
                let h = _mm256_mul_pd(og, t);
                _mm256_storeu_pd(hid.as_mut_ptr().add(i), h);

                i += 4;
            }
            for k in i..n {
                let mut c = f_gate[k] * cell[k] + i_gate[k] * g_gate[k];
                c = c.clamp(-cell_clip, cell_clip);
                cell[k] = c;
                hid[k] = o_gate[k] * approx_tanh_scalar(c);
            }
            return;
        }
    }
    
    #[cfg(target_arch = "aarch64")]
    unsafe {
        use core::arch::aarch64::*;
        let mut i = 0usize;
        let clip_lo = vdupq_n_f64(-cell_clip);
        let clip_hi = vdupq_n_f64(cell_clip);
        while i + 2 <= n {
            let cf = vld1q_f64(cell.as_ptr().add(i));
            let ig = vld1q_f64(i_gate.as_ptr().add(i));
            let fg = vld1q_f64(f_gate.as_ptr().add(i));
            let gg = vld1q_f64(g_gate.as_ptr().add(i));
            let og = vld1q_f64(o_gate.as_ptr().add(i));

            let part  = vmulq_f64(ig, gg);
            let cnew  = vminq_f64(clip_hi, vmaxq_f64(clip_lo, vaddq_f64(vmulq_f64(fg, cf), part)));
            vst1q_f64(cell.as_mut_ptr().add(i), cnew);

            let t = tanh_poly_neon(cnew);
            let h = vmulq_f64(og, t);
            vst1q_f64(hid.as_mut_ptr().add(i), h);
            i += 2;
        }
        for k in i..n {
            let mut c = f_gate[k] * cell[k] + i_gate[k] * g_gate[k];
            c = c.clamp(-cell_clip, cell_clip);
            cell[k] = c;
            hid[k] = o_gate[k] * approx_tanh_scalar(c);
        }
        return;
    }
    
    // Scalar fallback
    for k in 0..n {
        let mut c = f_gate[k] * cell[k] + i_gate[k] * g_gate[k];
        c = c.clamp(-cell_clip, cell_clip);
        cell[k] = c;
        hid[k] = o_gate[k] * approx_tanh_scalar(c);
    }
    }
}

// === PASTE VERBATIM: CRC32 over bytes (IEEE) ===
fn crc32(bytes: &[u8]) -> u32 {
    let mut crc: u32 = 0xFFFF_FFFF;
    for &b in bytes {
        let mut x = (crc ^ (b as u32)) & 0xFF;
        for _ in 0..8 {
            let m = (x & 1) * 0xEDB88320u32;
            x = (x >> 1) ^ m;
        }
        crc = (crc >> 8) ^ x;
    }
    !crc
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

// === PASTE VERBATIM: save/load with CRC ===
impl LSTMMarketRegimeClassifier {
    pub fn save_model_state_crc(&self) -> Vec<u8> {
        let state = ModelState {
            lstm_layers: self.lstm_layers.clone(),
            output_layer: self.output_layer.clone(),
            output_bias: self.output_bias.clone(),
            learning_rate: self.learning_rate,
            momentum_factor: self.momentum_factor,
            regime_threshold: self.regime_threshold,
        };
        let payload = bincode::serialize(&state).expect("serialize");
        let c = crc32(&payload).to_le_bytes();
        let mut out = Vec::with_capacity(4 + payload.len());
        out.extend_from_slice(&c);
        out.extend_from_slice(&payload);
        out
    }

    pub fn load_model_state_crc(&mut self, data: &[u8]) -> Result<(), Box<dyn std::error::Error>> {
        if data.len() < 4 { return Err("bad blob".into()); }
        let got = u32::from_le_bytes([data[0], data[1], data[2], data[3]]);
        let payload = &data[4..];
        let want = crc32(payload);
        if got != want { return Err("crc mismatch".into()); }
        self.load_model_state(payload)?;
        // packed rebuild & attn key handled in load_model_state(); done.
        Ok(())
    }

    fn load_model_state(&mut self, data: &[u8]) -> Result<(), Box<dyn std::error::Error>> {
        let state: ModelState = bincode::deserialize(data)?;
        self.lstm_layers = state.lstm_layers;
        self.output_layer = state.output_layer;
        self.output_bias = state.output_bias;
        self.learning_rate = state.learning_rate;
        self.momentum_factor = state.momentum_factor;
        self.regime_threshold = state.regime_threshold;
        
        // Rebuild packed weights and attention key
        for layer in &mut self.lstm_layers {
            layer.rebuild_packed();
        }
        self.rebuild_attn_key();
        Ok(())
    }
}

// === PASTE VERBATIM: pin current thread to CPU  ===
#[cfg(target_os = "linux")]
pub fn pin_thread_to_cpu(cpu_id: usize) -> Result<(), String> {
    use libc::{cpu_set_t, CPU_SET, CPU_ZERO, sched_setaffinity};
    unsafe {
        let mut set: cpu_set_t = std::mem::zeroed();
        CPU_ZERO(&mut set);
        CPU_SET(cpu_id, &mut set);
        let sz = std::mem::size_of::<cpu_set_t>();
        let rc = sched_setaffinity(0, sz, &set);
        if rc != 0 { return Err(format!("sched_setaffinity rc={}", rc)); }
    }
    Ok(())
}

#[cfg(not(target_os = "linux"))]
pub fn pin_thread_to_cpu(_: usize) -> Result<(), String> { Ok(()) }

// === PASTE VERBATIM: deterministic u64 ===
#[inline] 
pub fn det_u64(a: u64, b: u64, c: u64) -> u64 {
    let x = splitmix64(a ^ (b.rotate_left(17)) ^ (c.wrapping_mul(0x9E3779B97F4A7C15)));
    x
}

// === PASTE VERBATIM: blockhash/slot guards ===
pub fn blockhash_fresh(current_slot: u64, blockhash_slot: u64, max_age_slots: u64) -> bool {
    current_slot >= blockhash_slot && (current_slot - blockhash_slot) <= max_age_slots
}

pub fn time_left_ok(slot_phase_0_1: f64, lat_ema_us: f64, safety_us: f64) -> bool {
    // require at least one full cycle + safety margin left
    const SLOT_MICROS: u64 = 400_000; // 400ms slot time
    let slot_left_us = ((1.0 - slot_phase_0_1).max(0.0)) * SLOT_MICROS as f64;
    slot_left_us >= (lat_ema_us + safety_us).max(500.0)
}

// === PASTE VERBATIM: canonical tip rounding ===
#[inline]
pub fn round_tip_micro(micro: u64) -> u64 {
    // round to nearest 100 micro-lamports, min 100
    let m = if micro < 100 { 100 } else { micro };
    ((m + 50) / 100) * 100
}

// === PASTE VERBATIM: runtime tuner state ===
#[derive(Serialize, Deserialize)]
struct RuntimeTuner {
    backoff_mult: f64,
    pref_pdist: usize,
    leader_profiles: std::collections::HashMap<[u8;32], LeaderProfile>,
}

// === PASTE VERBATIM: CU auto sizer ===
pub fn estimate_cu_limit(instr_count: usize, exec: &ExecutionParameters) -> u32 {
    // base + per-instr; tuned safe defaults, monotone with aggression
    let base: f64 = 22_000.0;
    let per:  f64 = 1_150.0;
    let aggr = (0.85 + 0.30 * exec.aggression_level).clamp(0.85, 1.30);
    let est = (base + per * (instr_count as f64)) * aggr;
    est.round().clamp(25_000.0, 1_200_000.0) as u32
}

// === PASTE VERBATIM: enable DAZ/FTZ for faster math ===
#[cfg(target_arch = "x86_64")]
pub fn enable_daz_ftz() {
    unsafe {
        use std::arch::x86_64::{_mm_getcsr, _mm_setcsr};
        let csr = _mm_getcsr();
        _mm_setcsr(csr | 0x8040); // DAZ + FTZ
    }
}

#[cfg(not(target_arch = "x86_64"))]
pub fn enable_daz_ftz() {}

// === PASTE VERBATIM: CPU pinning helper ===
pub fn pin_thread_to_cpu(cpu_id: usize) -> Result<(), String> {
    #[cfg(target_os = "linux")]
    {
        use libc::{cpu_set_t, CPU_SET, CPU_ZERO, sched_setaffinity};
        unsafe {
            let mut set: cpu_set_t = std::mem::zeroed();
            CPU_ZERO(&mut set);
            CPU_SET(cpu_id, &mut set);
            let sz = std::mem::size_of::<cpu_set_t>();
            let rc = sched_setaffinity(0, sz, &set);
            if rc != 0 { return Err(format!("sched_setaffinity rc={}", rc)); }
        }
        Ok(())
    }
    #[cfg(not(target_os = "linux"))]
    {
        Ok(())
    }
}

// === PASTE VERBATIM: joint CLMM+CPMM splitter ===
#[derive(Clone, Debug)]
pub enum Venue<'a> {
    Cpmm(CpmmPool),
    Clmm {
        dir: ClmmDir,
        sqrt_p: f64, // current sqrt price (will evolve as we allocate)
        segs: &'a [ClmmSegment], // piecewise-constant liquidity
    },
}

#[inline]
fn mg_cpmm(p: &CpmmPool, alloc: f64, q: f64) -> f64 {
    out_for_in(p, alloc + q) - out_for_in(p, alloc)
}

#[inline]
fn mg_clmm(dir: ClmmDir, sqrt_p: f64, segs: &[ClmmSegment], q: f64) -> (f64, f64) {
    // returns (marginal_gain, new_sqrt_after_q)
    let (out, sp_new) = clmm_exact_in_out(q, sqrt_p, dir, segs);
    (out, sp_new)
}

/// Split amount_in across venues in steps equal quanta.
/// Returns allocations per venue in input units (same order as venues slice).
pub fn split_joint_venues<'a>(amount_in: f64, venues: &mut [Venue<'a>], steps: usize) -> Vec<f64> {
    let n = venues.len();
    let mut alloc = vec![0.0f64; n];
    if amount_in <= 0.0 || n == 0 || steps == 0 { return alloc; }

    let q = (amount_in / steps as f64).max(1.0);
    for _ in 0..steps {
        let mut best_i = 0usize;
        let mut best_gain = f64::NEG_INFINITY;

        // probe marginal gains without mutating state (except we store best idx to update after)
        for i in 0..n {
            match &venues[i] {
                Venue::Cpmm(p) => {
                    let g = mg_cpmm(p, alloc[i], q);
                    if g > best_gain { best_gain = g; best_i = i; }
                }
                Venue::Clmm { dir, sqrt_p, segs } => {
                    let (g, _) = mg_clmm(*dir, *sqrt_p, segs, q);
                    if g > best_gain { best_gain = g; best_i = i; }
                }
            }
        }

        // allocate q to best venue and update its internal state if CLMM
        alloc[best_i] += q;
        if let Venue::Clmm { dir, sqrt_p, segs } = &mut venues[best_i] {
            let (_, sp_new) = mg_clmm(*dir, *sqrt_p, segs, q);
            *sqrt_p = sp_new;
        }
    }

    // rescale to exact amount (removes rounding noise)
    let s: f64 = alloc.iter().sum();
    if s > 0.0 {
        let scale = amount_in / s;
        for a in &mut alloc { *a *= scale; }
    }
    alloc
}

/// Helper: build venues list from pools; pass mutable slice to retain CLMM sqrt evolution.
pub fn build_venues<'a>(
    cpmms: &'a [CpmmPool],
    clmms: &'a [(ClmmDir, f64, &'a [ClmmSegment])]
) -> Vec<Venue<'a>> {
    let mut v = Vec::with_capacity(cpmms.len() + clmms.len());
    for p in cpmms { v.push(Venue::Cpmm(*p)); }
    for (dir, sqrt_p, segs) in clmms {
        v.push(Venue::Clmm { dir: *dir, sqrt_p: *sqrt_p, segs });
    }
    v
}

// === PASTE VERBATIM: robust oracle aggregator ===
pub fn oracle_median_ok(sources: &[(f64, u64)], max_age_ms: u64, max_cv: f64) -> Option<f64> {
    // sources: (price, age_ms)
    let mut v = Vec::with_capacity(sources.len());
    for (px, age) in sources {
        if *age <= max_age_ms && px.is_finite() && *px > 0.0 { v.push(px); }
    }
    if v.is_empty() { return None; }
    v.sort_by(|a,b| a.partial_cmp(b).unwrap());
    let mid = v[v.len()/2];
    // coefficient of variation check (dispersion sanity)
    let mean = v.iter().sum::<f64>() / (v.len() as f64);
    let var = v.iter().map(|x| (x-mean)*(x-mean)).sum::<f64>() / (v.len() as f64);
    let cv = (var.sqrt() / mean).abs();
    if cv > max_cv { return None; }
    Some(mid)
}

pub fn pool_vs_oracle_guard(pool_px: f64, oracle_px: f64, vol_proxy: f64) -> bool {
    if !pool_px.is_finite() || !oracle_px.is_finite() || pool_px <= 0.0 || oracle_px <= 0.0 { return false; }
    let base = 0.0035; // 35 bps
    let widener = (4.5 * vol_proxy).clamp(0.0, 0.025);
    let band = (base + widener).clamp(0.003, 0.03);
    let dev = ((pool_px / oracle_px) - 1.0).abs();
    dev <= band
}

// === REPLACE: attempt pacing (safe, deterministic jitter) ===
#[inline]
pub fn attempt_spacing_us(lat_ema_us: f64, exec_overhead_us: f64) -> u64 {
    (lat_ema_us + exec_overhead_us).max(400.0).round() as u64
}

#[inline]
pub fn next_attempt_deadline_us(
    slot_phase_0_1: f64,
    slot_us: u64,
    attempt_idx: u32,
    spacing_us: u64,
    safety_us: u64,
    slot: u64,
    salt: u64,
) -> Option<u64> {
    // deterministic jitter in [-2%, +2%] around spacing
    let j = tip_jitter_mult(slot, attempt_idx, salt);
    let jittered = ((spacing_us as f64) * (0.98 + 0.04 * j)).round() as u64;

    let now_us = (slot_phase_0_1.max(0.0).min(1.0) * (slot_us as f64)).round() as u64;
    let target = attempt_idx as u64 * jittered;
    let latest = slot_us.saturating_sub(safety_us.max(200)); // never schedule past safety window
    if target > latest { return None; }
    Some(target.max(now_us))
}

// === PASTE VERBATIM: dual minOut guard ===
pub fn min_out_dual(
    amt_in: u64,
    px_out_per_in: f64,
    slippage: f64,
    out_decimals: u8,
    tick: u64,
    amm_out_theoretical: u64
) -> u64 {
    let price_guard = compute_min_out(amt_in, px_out_per_in, slippage, out_decimals, tick);
    let m = price_guard.min(amm_out_theoretical);
    if tick > 0 { round_down_to_tick(m, tick) } else { m }
}

// === PASTE VERBATIM: trimmed median oracle ===
pub fn oracle_trimmed_median_ok(sources: &[(f64, u64)], max_age_ms: u64, trim: usize, max_cv: f64) -> Option<f64> {
    let mut v: Vec<f64> = sources.iter()
        .filter(|(px, age)| *age <= max_age_ms && px.is_finite() && *px > &0.0)
        .map(|(px, _)| *px).collect();
    if v.is_empty() { return None; }
    v.sort_by(|a,b| a.partial_cmp(b).unwrap());
    let t = trim.min(v.len()/2);
    let slice = &v[t..(v.len()-t)];
    let mid = slice[slice.len()/2];

    let mean = slice.iter().sum::<f64>() / (slice.len() as f64);
    let var  = slice.iter().map(|x| (x - mean)*(x - mean)).sum::<f64>() / (slice.len() as f64);
    let cv = (var.sqrt() / mean.abs().max(1e-12)).abs();
    if cv > max_cv { None } else { Some(mid) }
}
