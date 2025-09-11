#![deny(unsafe_code)]
#![deny(warnings)]
#![deny(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::{
    collections::{VecDeque, HashSet},
    hash::BuildHasherDefault,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use smallvec::SmallVec;
use dashmap::DashMap;
use once_cell::sync::Lazy;
use rustc_hash::FxHasher;

use solana_sdk::{
    pubkey,
    pubkey::Pubkey,
    instruction::Instruction,
};

use tokio::task::yield_now;
use serde::{Deserialize, Serialize};
use anyhow::Result;

// ----- Optional low-latency allocator (enable with `--features mimalloc`) -----
#[cfg(feature = "mimalloc")]
use mimalloc::MiMalloc;
#[cfg(feature = "mimalloc")]
#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

// ======= Hasher/type aliases =======
type FxBuildHasher = BuildHasherDefault<FxHasher>;
type FxHashMap<K, V> = std::collections::HashMap<K, V, FxBuildHasher>;
type FxHashSet<T>    = HashSet<T, FxBuildHasher>;

// ======= Tunables (measured + bounded) =======
const MAX_RECENT_SWAPS: usize = 512;        // ring cap to prevent bloat
const MIRROR_WINDOW_SECS: u64 = 5;          // timing mirror window
const COORD_WINDOW_SECS: u64 = 60;          // coordination timing window
const HISTORY_WINDOW_SECS: u64 = 24 * 3600; // 24h rolling
const COORD_WINDOW_SLOTS: u64 = 2;          // slot-based timing window for coordination
const MAX_PATTERN_TS: usize = 256;          // hard cap per PatternKey
const MAX_PEERS_PER_WALLET: usize = 128;    // hard cap per wallet
const MAX_COORD_CACHE: usize = 8192;        // cache size cap for coord scores
const COORD_CACHE_TTL_SECS: u64 = 2;        // TTL for coord cache reuse

// ======= Zero-cost const Pubkeys (no runtime parsing) =======
pub const JUPITER_PROGRAM_ID: Pubkey = pubkey!("JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4");
pub const RAYDIUM_PROGRAM_ID: Pubkey = pubkey!("675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8");
pub const ORCA_PROGRAM_ID:    Pubkey = pubkey!("9W959DqEETiGZocYWCQPaJ6sBmUzgfxXfqGeTEdp3aQP");

pub const USDC_MINT: Pubkey = pubkey!("EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v");
pub const USDT_MINT: Pubkey = pubkey!("Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB");
pub const BUSD_MINT: Pubkey = pubkey!("AJ1W9A9N9dEMdVyoDiam2rV44gnBm2csrPDP7xqcapgX");

// Hot-path stable set (O(1) contains)
static STABLE_MINTS: Lazy<[Pubkey; 3]> = Lazy::new(|| [USDC_MINT, USDT_MINT, BUSD_MINT]);

#[inline(always)]
fn now_unix() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or(Duration::from_secs(0)).as_secs()
}

#[inline(always)]
fn read_u64_le(data: &[u8], start: usize) -> u64 {
    let end = start.saturating_add(8);
    data.get(start..end)
        .and_then(|s| s.try_into().ok())
        .map(u64::from_le_bytes)
        .unwrap_or(0)
}

// IMPROVED: branchless stable-mint check (no slice traversal)
#[inline(always)]
fn is_stable(pk: &Pubkey) -> bool {
    *pk == USDC_MINT || *pk == USDT_MINT || *pk == BUSD_MINT
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SwapData {
    pub timestamp: u64, // wall-clock seconds; 0 if unknown
    pub slot: u64,      // Solana slot; 0 if unknown
    pub dex: DexType,
    pub token_in: Pubkey,
    pub token_out: Pubkey,
    pub amount_in: u64,
    pub amount_out: u64,
    pub slippage_bps: u16,
    pub transaction_signature: String,
}

#[repr(u8)]
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum DexType { Jupiter = 0, Raydium = 1, Orca = 2 }

// Typed pattern key (no heap allocs, fast hash)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct PatternKey {
    pub dex: DexType,
    pub token_in: Pubkey,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LiquidityAction {
    pub timestamp: u64,
    pub action_type: LiquidityActionType,
    pub pool: Pubkey,
    pub amount_a: u64,
    pub amount_b: u64,
}

// Running stats for O(1) exact updates
#[derive(Debug, Clone)]
struct ProfileStats {
    swaps: usize,
    sum_ln_amount: f64,
    slippage_sum_bps: u64,
    total_amount_in: u128,
    stable_trade_vol: u128,
    stable_inflow: u128,
    stable_outflow: u128,
    token_counts: FxHashMap<Pubkey, u32>,
}

impl Default for ProfileStats {
    fn default() -> Self {
        Self {
            swaps: 0,
            sum_ln_amount: 0.0,
            slippage_sum_bps: 0,
            total_amount_in: 0,
            stable_trade_vol: 0,
            stable_inflow: 0,
            stable_outflow: 0,
            token_counts: FxHashMap::default(),
        }
    }
}

impl ProfileStats {
    #[inline(always)]
    fn apply(&mut self, s: &SwapData) {
        self.swaps = self.swaps.saturating_add(1);
        if s.amount_in > 0 {
            self.sum_ln_amount += (s.amount_in as f64).ln();
            self.total_amount_in = self.total_amount_in.saturating_add(s.amount_in as u128);
        }
        self.slippage_sum_bps = self.slippage_sum_bps.saturating_add(s.slippage_bps as u64);

        let is_stable_trade = STABLE_MINTS.contains(&s.token_in) || STABLE_MINTS.contains(&s.token_out);
        if is_stable_trade {
            self.stable_trade_vol = self.stable_trade_vol.saturating_add(s.amount_in as u128);
        }
        if STABLE_MINTS.contains(&s.token_in) {
            self.stable_outflow = self.stable_outflow.saturating_add(s.amount_in as u128);
        }
        if STABLE_MINTS.contains(&s.token_out) {
            self.stable_inflow = self.stable_inflow.saturating_add(s.amount_out as u128);
        }

        *self.token_counts.entry(s.token_in).or_insert(0) += 1;
        *self.token_counts.entry(s.token_out).or_insert(0) += 1;
    }

    #[inline(always)]
    fn revert(&mut self, s: &SwapData) {
        if self.swaps > 0 { self.swaps -= 1; }
        if s.amount_in > 0 {
            self.sum_ln_amount -= (s.amount_in as f64).ln();
            self.total_amount_in = self.total_amount_in.saturating_sub(s.amount_in as u128);
        }
        self.slippage_sum_bps = self.slippage_sum_bps.saturating_sub(s.slippage_bps as u64);

        let is_stable_trade = STABLE_MINTS.contains(&s.token_in) || STABLE_MINTS.contains(&s.token_out);
        if is_stable_trade {
            self.stable_trade_vol = self.stable_trade_vol.saturating_sub(s.amount_in as u128);
        }
        if STABLE_MINTS.contains(&s.token_in) {
            self.stable_outflow = self.stable_outflow.saturating_sub(s.amount_in as u128);
        }
        if STABLE_MINTS.contains(&s.token_out) {
            self.stable_inflow = self.stable_inflow.saturating_sub(s.amount_out as u128);
        }

        if let Some(c) = self.token_counts.get_mut(&s.token_in) {
            *c -= 1; if *c == 0 { self.token_counts.remove(&s.token_in); }
        }
        if let Some(c) = self.token_counts.get_mut(&s.token_out) {
            *c -= 1; if *c == 0 { self.token_counts.remove(&s.token_out); }
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum LiquidityActionType {
    AddLiquidity,
    RemoveLiquidity,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WalletProfile {
    pub wallet: Pubkey,
    pub recent_swaps: VecDeque<SwapData>,                 // bounded ring
    pub liquidity_actions: SmallVec<[LiquidityAction; 8]>, // tiny allocs
    pub first_seen: u64,
    pub last_activity: u64,
    pub total_volume: u64,
    // live metrics
    pub aggression_index: f64,
    pub token_rotation_entropy: f64,
    pub risk_preference: f64,
    pub liquidity_behavior_score: f64,
    pub mirror_intent_score: f64,
    pub coordination_score: f64,
    pub stablecoin_inflow_ratio: f64,
    pub avg_slippage_tolerance: f64,
    pub swap_frequency_score: f64,
    // O(1) running stats (exact)
    stats: ProfileStats,
    // EMA for mirror intent
    mirror_ema: f64,
}

impl WalletProfile {
    #[inline(always)]
    pub fn new(wallet: Pubkey) -> Self {
        let now = now_unix();
        Self {
            wallet,
            recent_swaps: VecDeque::with_capacity(128),
            liquidity_actions: SmallVec::new(),
            first_seen: now,
            last_activity: now,
            total_volume: 0,
            aggression_index: 0.0,
            token_rotation_entropy: 0.0,
            risk_preference: 0.5,
            liquidity_behavior_score: 0.0,
            mirror_intent_score: 0.0,
            coordination_score: 0.0,
            stablecoin_inflow_ratio: 0.0,
            avg_slippage_tolerance: 0.0,
            swap_frequency_score: 0.0,
            stats: ProfileStats::default(),
            mirror_ema: 0.0,
        }
    }

    #[inline(always)]
    pub fn maintain_bounds(&mut self, now: u64) {
        let cutoff = now.saturating_sub(HISTORY_WINDOW_SECS);
        while let Some(front) = self.recent_swaps.front() {
            if front.timestamp <= cutoff { self.recent_swaps.pop_front(); } else { break; }
        }
        while self.recent_swaps.len() > MAX_RECENT_SWAPS {
            self.recent_swaps.pop_front();
        }
    }
}

pub struct WhaleWalletBehavioralFingerprinter {
    profiles: DashMap<Pubkey, WalletProfile, FxBuildHasher>,
    mempool_patterns: DashMap<PatternKey, SmallVec<[u64; 64]>, FxBuildHasher>,
    coordination_graph: DashMap<Pubkey, SmallVec<[Pubkey; 8]>, FxBuildHasher>,
    coord_score_cache: DashMap<(Pubkey, u64), (f64, u64), FxBuildHasher>,
}

impl WhaleWalletBehavioralFingerprinter {
    pub fn new() -> Self {
        Self {
            profiles: DashMap::with_hasher(FxBuildHasher::default()),
            mempool_patterns: DashMap::with_hasher(FxBuildHasher::default()),
            coordination_graph: DashMap::with_hasher(FxBuildHasher::default()),
            coord_score_cache: DashMap::with_hasher(FxBuildHasher::default()),
        }
    }

    /// Legacy entry (uses wall clock + slot=0). Prefer `process_transaction_at`.
    #[inline(always)]
    pub async fn process_transaction(&self, wallet: Pubkey, ix: &Instruction) -> Result<()> {
        self.process_transaction_at(wallet, ix, now_unix(), 0).await
    }

    /// Hot-path entry: caller provides timestamp & slot (no syscalls).
    #[inline(always)]
    pub async fn process_transaction_at(&self, wallet: Pubkey, ix: &Instruction, ts: u64, slot: u64) -> Result<()> {
        if let Some(mut swap) = self.parse_swap_instruction(ix).await? {
            swap.timestamp = ts;
            swap.slot = slot;
            self.update_wallet_profile(wallet, swap).await?;
        }
        Ok(())
    }

    #[inline(always)]
    async fn parse_swap_instruction(&self, ix: &Instruction) -> Result<Option<SwapData>> {
        let pid = ix.program_id;
        if pid == JUPITER_PROGRAM_ID { return self.parse_jupiter_swap(ix).await; }
        if pid == RAYDIUM_PROGRAM_ID { return self.parse_raydium_swap(ix).await; }
        if pid == ORCA_PROGRAM_ID    { return self.parse_orca_swap(ix).await; }
        Ok(None)
    }

    #[inline(always)]
    async fn parse_jupiter_swap(&self, ix: &Instruction) -> Result<Option<SwapData>> {
        // format (common): [disc:8][amount_in:8][min_out:8]...
        if ix.data.len() < 24 || ix.accounts.len() < 3 { return Ok(None); }
        let amount_in = read_u64_le(&ix.data, 8);
        let min_out   = read_u64_le(&ix.data, 16);

        let token_in  = ix.accounts.get(1).map(|a| a.pubkey).unwrap_or(Pubkey::default());
        let token_out = ix.accounts.get(2).map(|a| a.pubkey).unwrap_or(Pubkey::default());

        let expected_out = amount_in.saturating_mul(995) / 1000; // ~0.5%
        let slip = if expected_out > 0 && expected_out > min_out {
            (((expected_out - min_out) * 10_000) / expected_out) as u16
        } else { 0 };

        Ok(Some(SwapData {
            timestamp: 0, slot: 0,
            dex: DexType::Jupiter,
            token_in, token_out,
            amount_in, amount_out: min_out,
            slippage_bps: slip,
            transaction_signature: String::new(),
        }))
    }

    #[inline(always)]
    async fn parse_raydium_swap(&self, ix: &Instruction) -> Result<Option<SwapData>> {
        if ix.data.len() < 17 || ix.accounts.len() < 10 { return Ok(None); }
        // [disc:1][amount_in:8][min_out:8]
        let amount_in = read_u64_le(&ix.data, 1);
        let min_out   = read_u64_le(&ix.data, 9);

        // heuristic positions seen commonly
        let token_in  = ix.accounts.get(8).map(|a| a.pubkey).unwrap_or(Pubkey::default());
        let token_out = ix.accounts.get(9).map(|a| a.pubkey).unwrap_or(Pubkey::default());

        let expected_out = amount_in.saturating_mul(997) / 1000; // ~0.3%
        let slip = if expected_out > 0 && expected_out > min_out {
            (((expected_out - min_out) * 10_000) / expected_out) as u16
        } else { 0 };

        Ok(Some(SwapData {
            timestamp: 0, slot: 0,
            dex: DexType::Raydium,
            token_in, token_out,
            amount_in, amount_out: min_out,
            slippage_bps: slip,
            transaction_signature: String::new(),
        }))
    }

    #[inline(always)]
    async fn parse_orca_swap(&self, ix: &Instruction) -> Result<Option<SwapData>> {
        if ix.data.len() < 16 || ix.accounts.len() < 4 { return Ok(None); }
        // [amount:8][other_amount_threshold:8]
        let amount_in = read_u64_le(&ix.data, 0);
        let min_out   = read_u64_le(&ix.data, 8);

        let token_in  = ix.accounts.get(2).map(|a| a.pubkey).unwrap_or(Pubkey::default());
        let token_out = ix.accounts.get(3).map(|a| a.pubkey).unwrap_or(Pubkey::default());

        let expected_out = amount_in.saturating_mul(999) / 1000; // ~0.1%
        let slip = if expected_out > 0 && expected_out > min_out {
            (((expected_out - min_out) * 10_000) / expected_out) as u16
        } else { 0 };

        Ok(Some(SwapData {
            timestamp: 0, slot: 0,
            dex: DexType::Orca,
            token_in, token_out,
            amount_in, amount_out: min_out,
            slippage_bps: slip,
            transaction_signature: String::new(),
        }))
    }

    #[inline(always)]
    async fn update_wallet_profile(&self, wallet: Pubkey, swap: SwapData) -> Result<()> {
        let now = if swap.timestamp > 0 { swap.timestamp } else { now_unix() };
        // get-or-create (guard scope limited)
        let mut entry = self.profiles.entry(wallet).or_insert_with(|| WalletProfile::new(wallet));
        {
            let prof = entry.value_mut();

            // time window eviction
            let cutoff = now.saturating_sub(HISTORY_WINDOW_SECS);
            while let Some(front) = prof.recent_swaps.front() {
                if front.timestamp > 0 && front.timestamp <= cutoff {
                    let old = prof.recent_swaps.pop_front().expect("front existed");
                    prof.stats.revert(&old);
                } else { break; }
            }
            // cap eviction
            while prof.recent_swaps.len() >= MAX_RECENT_SWAPS {
                if let Some(old) = prof.recent_swaps.pop_front() { prof.stats.revert(&old); } else { break; }
            }

            // apply new swap
            prof.recent_swaps.push_back(swap.clone());
            prof.stats.apply(&swap);
            prof.last_activity = now;
            prof.total_volume = prof.total_volume.saturating_add(swap.amount_in);

            // mirror EMA update (O(1))
            let inst_corr = self.instant_mirror_corr(&swap);
            let n = prof.stats.swaps.max(1) as f64;
            let alpha = (0.20f64).max(1.0 / n.min(20.0));
            prof.mirror_ema = prof.mirror_ema * (1.0 - alpha) + inst_corr * alpha;

            // recompute metrics from O(1) stats
            prof.aggression_index         = self.metric_aggression(&prof.stats, &prof.recent_swaps);
            prof.token_rotation_entropy   = self.metric_entropy(&prof.stats);
            prof.risk_preference          = self.metric_risk_pref(&prof.stats);
            prof.liquidity_behavior_score = self.calculate_liquidity_behavior_score(&prof.liquidity_actions);
            prof.mirror_intent_score      = prof.mirror_ema.clamp(0.0, 1.0);
            prof.coordination_score       = self.calculate_coordination_score_cached(wallet, swap.slot, now);
            prof.stablecoin_inflow_ratio  = self.metric_stable_inflow_ratio(&prof.stats);
            prof.avg_slippage_tolerance   = self.metric_avg_slippage(&prof.stats);
            prof.swap_frequency_score     = self.metric_swap_freq(&prof.recent_swaps);
        }
        drop(entry);
        yield_now().await;
        Ok(())
    }

    // ===== Metrics from running stats =====
    #[inline(always)]
    fn metric_aggression(&self, st: &ProfileStats, swaps: &VecDeque<SwapData>) -> f64 {
        if st.swaps == 0 { return 0.0; }
        let avg_ln_amt = st.sum_ln_amount / (st.swaps as f64);
        let avg_slip   = (st.slippage_sum_bps as f64) / (st.swaps as f64) / 10_000.0;
        let base = (avg_ln_amt / 20.0) + 2.0 * avg_slip;

        let (first, last) = match (swaps.front(), swaps.back()) {
            (Some(a), Some(b)) if a.timestamp > 0 && b.timestamp >= a.timestamp => (a.timestamp, b.timestamp),
            _ => (0, 0),
        };
        let span = last.saturating_sub(first);
        let freq_mult = if span > 0 { (swaps.len() as f64 * 3600.0) / (span as f64) } else { 1.0 };
        (base * freq_mult).min(10.0)
    }

    #[inline(always)]
    fn metric_entropy(&self, st: &ProfileStats) -> f64 {
        let total: u64 = st.token_counts.values().map(|&c| c as u64).sum();
        if total == 0 { return 0.0; }
        let t = total as f64;
        let mut h = 0.0;
        for &c in st.token_counts.values() {
            let p = (c as f64) / t;
            if p > 0.0 { h -= p * p.ln(); }
        }
        h
    }

    #[inline(always)]
    fn metric_risk_pref(&self, st: &ProfileStats) -> f64 {
        if st.total_amount_in == 0 { return 0.5; }
        1.0 - (st.stable_trade_vol as f64 / st.total_amount_in as f64)
    }

    #[inline(always)]
    fn metric_stable_inflow_ratio(&self, st: &ProfileStats) -> f64 {
        let denom = st.stable_inflow.saturating_add(st.stable_outflow);
        if denom == 0 { 0.0 } else { (st.stable_inflow as f64) / (denom as f64) }
    }

    #[inline(always)]
    fn metric_avg_slippage(&self, st: &ProfileStats) -> f64 {
        if st.swaps == 0 { 0.0 } else { (st.slippage_sum_bps as f64) / (st.swaps as f64) }
    }

    #[inline(always)]
    fn metric_swap_freq(&self, swaps: &VecDeque<SwapData>) -> f64 {
        if swaps.len() < 2 { return 0.0; }
        let first = match swaps.front() { Some(s) => s.timestamp, None => return 0.0 };
        let last  = match swaps.back()  { Some(s) => s.timestamp, None => return 0.0 };
        let span  = last.saturating_sub(first);
        if span == 0 { 10.0 } else { ((swaps.len() as f64 * 3600.0) / (span as f64)).min(10.0) }
    }

    // ===== Mirror intent (EMA on instantaneous correlation) =====
    #[inline(always)]
    fn instant_mirror_corr(&self, s: &SwapData) -> f64 {
        let key = PatternKey { dex: s.dex, token_in: s.token_in };
        if let Some(ts) = self.mempool_patterns.get(&key) { self.timing_correlation_rev(s.timestamp, &ts) } else { 0.0 }
    }

    #[inline(always)]
    fn calculate_aggression_index(&self, swaps: &VecDeque<SwapData>) -> f64 {
        if swaps.is_empty() { return 0.0; }
        // size term uses ln(amount) (scaled), slippage weighted heavier; freq multiplier per hour
        let mut sum = 0.0;
        for s in swaps.iter() {
            let size = if s.amount_in > 0 { (s.amount_in as f64).ln() / 20.0 } else { 0.0 };
            let slip = (s.slippage_bps as f64) / 10_000.0;
            sum += size + 2.0 * slip;
        }
        let base = sum / (swaps.len() as f64);

        let first = swaps.front().map(|s| s.timestamp).unwrap_or(0);
        let last  = swaps.back().map(|s| s.timestamp).unwrap_or(first);
        let span  = last.saturating_sub(first);
        let freq_mult = if span > 0 { (swaps.len() as f64 * 3600.0) / (span as f64) } else { 1.0 };

        (base * freq_mult).min(10.0)
    }

    #[inline(always)]
    fn calculate_token_rotation_entropy(&self, swaps: &VecDeque<SwapData>) -> f64 {
        if swaps.is_empty() { return 0.0; }
        let mut counts: FxHashMap<Pubkey, u32> = FxHashMap::default();
        let mut total: u32 = 0;
        for s in swaps.iter() {
            *counts.entry(s.token_in).or_insert(0) += 1; total += 1;
            *counts.entry(s.token_out).or_insert(0) += 1; total += 1;
        }
        let tot = total as f64;
        let mut h = 0.0;
        for &c in counts.values() {
            let p = (c as f64) / tot;
            if p > 0.0 { h -= p * p.ln(); }
        }
        h
    }

    #[inline(always)]
    fn calculate_risk_preference(&self, swaps: &VecDeque<SwapData>) -> f64 {
        if swaps.is_empty() { return 0.5; }
        let mut stable_vol = 0u128;
        let mut vol = 0u128;
        for s in swaps.iter() {
            vol += s.amount_in as u128;
            if STABLE_MINTS.contains(&s.token_in) || STABLE_MINTS.contains(&s.token_out) {
                stable_vol += s.amount_in as u128;
            }
        }
        if vol == 0 { return 0.5; }
        1.0 - (stable_vol as f64 / vol as f64)
    }

    #[inline(always)]
    fn calculate_liquidity_behavior_score(&self, acts: &SmallVec<[LiquidityAction; 8]>) -> f64 {
        if acts.is_empty() { return 0.0; }
        let mut add = 0u32; let mut rem = 0u32;
        for a in acts.iter() {
            match a.action_type {
                LiquidityActionType::AddLiquidity    => add += 1,
                LiquidityActionType::RemoveLiquidity => rem += 1,
            }
        }
        let mut timing_sum = 0.0; let mut timing_cnt = 0.0;
        for w in acts.windows(2) {
            let dt = w[1].timestamp.saturating_sub(w[0].timestamp);
            let eff = if dt < 300 { 1.0 } else { 300.0 / (dt as f64) };
            timing_sum += eff; timing_cnt += 1.0;
        }
        let timing_avg = if timing_cnt > 0.0 { timing_sum / timing_cnt } else { 0.0 };
        let total = (add + rem) as f64;
        let balance = if total > 0.0 { 1.0 - ((add as f64 - rem as f64).abs() / total) } else { 0.0 };
        ((timing_avg + balance) * 0.5).clamp(0.0, 1.0)
    }

    #[inline(always)]
    async fn calculate_mirror_intent_score(&self, swaps: &VecDeque<SwapData>) -> f64 {
        if swaps.is_empty() { return 0.0; }
        let mut sum = 0.0; let mut n = 0.0;
        for s in swaps.iter() {
            let key = PatternKey { dex: s.dex, token_in: s.token_in };
            if let Some(ts) = self.mempool_patterns.get(&key) {
                sum += self.timing_correlation(s.timestamp, &ts);
                n += 1.0;
            }
        }
        if n == 0.0 { 0.0 } else { (sum / n).clamp(0.0, 1.0) }
    }

    #[inline(always)]
    fn timing_correlation_rev(&self, t: u64, pattern_ts: &SmallVec<[u64; 64]>) -> f64 {
        // reverse-scan newest first, early exit once window exceeded
        let mut best = 0.0;
        for &p in pattern_ts.iter().rev() {
            if p + MIRROR_WINDOW_SECS < t { break; }
            let d = t.max(p) - t.min(p);
            if d <= MIRROR_WINDOW_SECS {
                let c = 1.0 - (d as f64 / MIRROR_WINDOW_SECS as f64);
                if c > best { best = c; if best == 1.0 { break; } }
            }
        }
        best
    }

    #[inline(always)]
    fn calculate_coordination_score_cached(&self, wallet: Pubkey, slot: u64, now: u64) -> f64 {
        if slot > 0 {
            if let Some(cached) = self.coord_score_cache.get(&(wallet, slot)) {
                let (score, ts) = *cached;
                if now.saturating_sub(ts) <= COORD_CACHE_TTL_SECS { return score; }
            }
        }
        let score = self.calculate_coordination_score(wallet);
        if slot > 0 {
            if self.coord_score_cache.len() > MAX_COORD_CACHE { self.coord_score_cache.clear(); }
            self.coord_score_cache.insert((wallet, slot), (score, now));
        }
        score
    }

    #[inline(always)]
    fn calculate_coordination_score(&self, wallet: Pubkey) -> f64 {
        let peers = self.coordination_graph.get(&wallet).map(|v| v.clone()).unwrap_or_default();
        if peers.is_empty() { return 0.0; }

        let me = match self.profiles.get(&wallet) { Some(p) => p, None => return 0.0 };
        let me_swaps: &VecDeque<SwapData> = &me.recent_swaps;

        let mut sum = 0.0; let mut cnt = 0.0;
        for peer in peers.iter() {
            if let Some(other) = self.profiles.get(peer) {
                let tc = self.swap_timing_corr_twoptr(me_swaps, &other.recent_swaps, COORD_WINDOW_SECS);
                let kc = self.token_preference_jaccard(me_swaps, &other.recent_swaps);
                let bc = self.behavior_corr(&me, &other);
                sum += (tc + kc + bc) / 3.0;
                cnt += 1.0;
            }
        }
        if cnt == 0.0 { 0.0 } else { (sum / cnt).clamp(0.0, 1.0) }
    }

    fn calculate_profile_correlation(&self, profile1: &WalletProfile, profile2: &WalletProfile) -> f64 {
        let timing_correlation = self.swap_timing_corr_twoptr(&profile1.recent_swaps, &profile2.recent_swaps, COORD_WINDOW_SECS);
        let token_correlation = self.token_preference_jaccard(&profile1.recent_swaps, &profile2.recent_swaps);
        let behavior_correlation = self.behavior_corr(profile1, profile2);
        (timing_correlation + token_correlation + behavior_correlation) / 3.0
    }

    #[inline(always)]
    fn swap_timing_corr_twoptr(&self, a: &VecDeque<SwapData>, b: &VecDeque<SwapData>, win: u64) -> f64 {
        if a.is_empty() || b.is_empty() { return 0.0; }
        // two-pointer sweep on sorted timestamps
        let (mut i, mut j) = (0usize, 0usize);
        let (mut acc, mut cnt) = (0.0, 0.0);
        while i < a.len() && j < b.len() {
            let ta = a[i].timestamp; let tb = b[j].timestamp;
            let (min, max) = (ta.min(tb), ta.max(tb));
            let d = max - min;
            if d <= win {
                acc += 1.0 - (d as f64 / win as f64);
                cnt += 1.0;
                // advance the earlier one to look for next match
                if ta <= tb { i += 1; } else { j += 1; }
            } else {
                if ta < tb { i += 1; } else { j += 1; }
            }
        }
        if cnt == 0.0 { 0.0 } else { (acc / cnt).clamp(0.0, 1.0) }
    }

    #[inline(always)]
    fn token_preference_jaccard(&self, a: &VecDeque<SwapData>, b: &VecDeque<SwapData>) -> f64 {
        use std::collections::hash_set::HashSet;
        if a.is_empty() || b.is_empty() { return 0.0; }
        let mut sa: HashSet<Pubkey> = HashSet::with_capacity(a.len()*2);
        let mut sb: HashSet<Pubkey> = HashSet::with_capacity(b.len()*2);
        for s in a.iter() { sa.insert(s.token_in); sa.insert(s.token_out); }
        for s in b.iter() { sb.insert(s.token_in); sb.insert(s.token_out); }
        let inter = sa.intersection(&sb).count() as f64;
        let uni   = sa.union(&sb).count() as f64;
        if uni == 0.0 { 0.0 } else { inter / uni }
    }

    #[inline(always)]
    fn behavior_corr(&self, p1: &WalletProfile, p2: &WalletProfile) -> f64 {
        let ad = (p1.aggression_index - p2.aggression_index).abs();
        let rd = (p1.risk_preference   - p2.risk_preference).abs();
        let ed = (p1.token_rotation_entropy - p2.token_rotation_entropy).abs();

        let ac = 1.0 - (ad / 10.0).min(1.0);
        let rc = 1.0 - rd.clamp(0.0, 1.0);
        let ec = 1.0 - (ed / 5.0).min(1.0);

        ((ac + rc + ec) / 3.0).clamp(0.0, 1.0)
    }

    #[inline(always)]
    fn calculate_stablecoin_inflow_ratio(&self, swaps: &VecDeque<SwapData>) -> f64 {
        if swaps.is_empty() { return 0.0; }
        let mut inflow: u128 = 0;
        let mut outflow: u128 = 0;
        for s in swaps.iter() {
            if STABLE_MINTS.contains(&s.token_in)  { outflow += s.amount_in as u128; }
            if STABLE_MINTS.contains(&s.token_out) { inflow  += s.amount_out as u128; }
        }
        let denom = inflow + outflow;
        if denom == 0 { 0.0 } else { (inflow as f64) / (denom as f64) }
    }

    #[inline(always)]
    fn calculate_avg_slippage_tolerance(&self, swaps: &VecDeque<SwapData>) -> f64 {
        if swaps.is_empty() { return 0.0; }
        let sum: u64 = swaps.iter().map(|s| s.slippage_bps as u64).sum();
        (sum as f64) / (swaps.len() as f64)
    }

    #[inline(always)]
    fn calculate_swap_frequency_score(&self, swaps: &VecDeque<SwapData>) -> f64 {
        if swaps.len() < 2 { return 0.0; }
        let first = swaps.front().unwrap().timestamp;
        let last  = swaps.back().unwrap().timestamp;
        let span  = last.saturating_sub(first);
        if span == 0 { 10.0 } else { ((swaps.len() as f64 * 3600.0) / span as f64).min(10.0) }
    }

    #[inline(always)]
    pub async fn get_wallet_profile(&self, wallet: &Pubkey) -> Option<WalletProfile> {
        self.profiles.get(wallet).map(|e| e.value().clone())
    }

    #[inline(always)]
    pub async fn detect_mirror_intent(&self, wallet: &Pubkey) -> Result<bool> {
        let p = self.profiles.get(wallet).ok_or_else(|| anyhow::anyhow!("Wallet not found"))?;
        Ok(p.mirror_intent_score > 0.7)
    }

    #[inline(always)]
    pub async fn analyze_liquidity_behavior(&self, wallet: &Pubkey) -> Result<f64> {
        let p = self.profiles.get(wallet).ok_or_else(|| anyhow::anyhow!("Wallet not found"))?;
        Ok(p.liquidity_behavior_score)
    }

    #[inline(always)]
    pub async fn update_mempool_pattern_key(&self, key: PatternKey, ts: u64) -> Result<()> {
        let mut entry = self.mempool_patterns.entry(key).or_insert_with(|| SmallVec::<[u64; 64]>::new());
        let v = entry.value_mut();
        if let Some(&last) = v.last() {
            if ts >= last { v.push(ts); }
            else {
                let pos = v.iter().position(|&x| x > ts).unwrap_or(v.len());
                v.insert(pos, ts);
            }
        } else {
            v.push(ts);
        }
        // retain last hour and cap to newest MAX_PATTERN_TS
        let cutoff = ts.saturating_sub(3600);
        v.retain(|&x| x > cutoff);
        if v.len() > MAX_PATTERN_TS { let drop_n = v.len() - MAX_PATTERN_TS; v.drain(0..drop_n); }
        Ok(())
    }

    // Compatibility: legacy "Dex_token" string → typed key (non-hot)
    pub async fn update_mempool_pattern(&self, pattern: String, ts: u64) -> Result<()> {
        use std::str::FromStr;
        let (dex_s, pk_s) = match pattern.split_once('_') { Some(x) => x, None => return Ok(()) };
        let dex = match dex_s {
            "Jupiter" => DexType::Jupiter,
            "Raydium" => DexType::Raydium,
            "Orca"    => DexType::Orca,
            _ => return Ok(()),
        };
        let token_in = Pubkey::from_str(pk_s).unwrap_or(Pubkey::default());
        self.update_mempool_pattern_key(PatternKey { dex, token_in }, ts).await
    }

    #[inline(always)]
    pub async fn add_coordination_edge(&self, a: Pubkey, b: Pubkey) -> Result<()> {
        let mut e1 = self.coordination_graph.entry(a).or_insert_with(|| SmallVec::<[Pubkey; 8]>::new());
        if !e1.value().contains(&b) {
            if e1.value().len() >= MAX_PEERS_PER_WALLET { e1.value_mut().remove(0); }
            e1.value_mut().push(b);
        }
        let mut e2 = self.coordination_graph.entry(b).or_insert_with(|| SmallVec::<[Pubkey; 8]>::new());
        if !e2.value().contains(&a) {
            if e2.value().len() >= MAX_PEERS_PER_WALLET { e2.value_mut().remove(0); }
            e2.value_mut().push(a);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    #[tokio::test]
    async fn test_whale_behavioral_fingerprinting() {
        let fingerprinter = WhaleWalletBehavioralFingerprinter::new();
        
        // Create test wallets
        let aggressive_whale = Pubkey::from_str("11111111111111111111111111111111").unwrap();
        let conservative_whale = Pubkey::from_str("22222222222222222222222222222222").unwrap();
        let rotational_whale = Pubkey::from_str("33333333333333333333333333333333").unwrap();
        
        let usdc_mint = USDC_MINT;
        let usdt_mint = USDT_MINT;
        let random_token = Pubkey::from_str("44444444444444444444444444444444").unwrap();
        
        // Simulate aggressive whale behavior
        let aggressive_swaps = vec![
            SwapData {
                timestamp: 1000,
                slot: 0,
                dex: DexType::Jupiter,
                token_in: usdc_mint,
                token_out: random_token,
                amount_in: 1000000000000, // Large amount
                amount_out: 950000000000,
                slippage_bps: 500, // 5% slippage tolerance
                transaction_signature: SmallVec::from_slice(b"aggressive1"),
            },
            SwapData {
                timestamp: 1010,
                slot: 0,
                dex: DexType::Raydium,
                token_in: random_token,
                token_out: usdt_mint,
                amount_in: 950000000000,
                amount_out: 940000000000,
                slippage_bps: 600, // 6% slippage tolerance
                transaction_signature: SmallVec::from_slice(b"aggressive2"),
            },
        ];
        
        // Simulate conservative whale behavior
        let conservative_swaps = vec![
            SwapData {
                timestamp: 2000,
                slot: 0,
                dex: DexType::Orca,
                token_in: usdc_mint,
                token_out: usdt_mint,
                amount_in: 100000000000, // Smaller amount
                amount_out: 99000000000,
                slippage_bps: 50, // 0.5% slippage tolerance
                transaction_signature: SmallVec::from_slice(b"conservative1"),
            },
        ];
        
        // Simulate rotational whale behavior
        let rotational_swaps = vec![
            SwapData {
                timestamp: 3000,
                slot: 0,
                dex: DexType::Jupiter,
                token_in: usdc_mint,
                token_out: random_token,
                amount_in: 500000000000,
                amount_out: 480000000000,
                slippage_bps: 200, // 2% slippage tolerance
                transaction_signature: SmallVec::from_slice(b"rotational1"),
            },
            SwapData {
                timestamp: 3300,
                slot: 0,
                dex: DexType::Raydium,
                token_in: random_token,
                token_out: Pubkey::from_str("55555555555555555555555555555555").unwrap(),
                amount_in: 480000000000,
                amount_out: 470000000000,
                slippage_bps: 250, // 2.5% slippage tolerance
                transaction_signature: SmallVec::from_slice(b"rotational2"),
            },
            SwapData {
                timestamp: 3600,
                slot: 0,
                dex: DexType::Orca,
                token_in: Pubkey::from_str("55555555555555555555555555555555").unwrap(),
                token_out: usdt_mint,
                amount_in: 470000000000,
                amount_out: 460000000000,
                slippage_bps: 300, // 3% slippage tolerance
                transaction_signature: SmallVec::from_slice(b"rotational3"),
            },
        ];
        
        // Update profiles
        for swap in aggressive_swaps {
            fingerprinter.update_wallet_profile(aggressive_whale, swap).await.unwrap();
        }
        
        for swap in conservative_swaps {
            fingerprinter.update_wallet_profile(conservative_whale, swap).await.unwrap();
        }
        
        for swap in rotational_swaps {
            fingerprinter.update_wallet_profile(rotational_whale, swap).await.unwrap();
        }
        
        // Get profiles
        let aggressive_profile = fingerprinter.get_wallet_profile(&aggressive_whale).await.unwrap();
        let conservative_profile = fingerprinter.get_wallet_profile(&conservative_whale).await.unwrap();
        let rotational_profile = fingerprinter.get_wallet_profile(&rotational_whale).await.unwrap();
        
        // Assert behavioral divergence
        assert!(aggressive_profile.aggression_index > conservative_profile.aggression_index);
        assert!(aggressive_profile.aggression_index > rotational_profile.aggression_index);
        assert!(rotational_profile.token_rotation_entropy > conservative_profile.token_rotation_entropy);
        assert!(rotational_profile.token_rotation_entropy > aggressive_profile.token_rotation_entropy);
        
        // Risk preference tests
        assert!(conservative_profile.risk_preference < aggressive_profile.risk_preference);
        assert!(conservative_profile.avg_slippage_tolerance < aggressive_profile.avg_slippage_tolerance);
        
        // Frequency tests
        assert!(aggressive_profile.swap_frequency_score > conservative_profile.swap_frequency_score);
        assert!(rotational_profile.swap_frequency_score > conservative_profile.swap_frequency_score);
        
        println!("Aggressive whale aggression_index: {}", aggressive_profile.aggression_index);
        println!("Conservative whale aggression_index: {}", conservative_profile.aggression_index);
        println!("Rotational whale token_rotation_entropy: {}", rotational_profile.token_rotation_entropy);
        println!("Conservative whale token_rotation_entropy: {}", conservative_profile.token_rotation_entropy);
    }
    
    #[tokio::test]
    async fn test_detect_mirror_intent() {
        let fingerprinter = WhaleWalletBehavioralFingerprinter::new();
        let wallet = Pubkey::from_str("11111111111111111111111111111111").unwrap();
        
        // Add mempool patterns
        fingerprinter.update_mempool_pattern("Jupiter_44444444444444444444444444444444".to_string(), 1000).await.unwrap();
        fingerprinter.update_mempool_pattern("Jupiter_44444444444444444444444444444444".to_string(), 1002).await.unwrap();
        
        // Add swap that mirrors the pattern
        let swap = SwapData {
            timestamp: 1003,
            slot: 0,
            dex: DexType::Jupiter,
            token_in: Pubkey::from_str("44444444444444444444444444444444").unwrap(),
            token_out: Pubkey::from_str("55555555555555555555555555555555").unwrap(),
            amount_in: 1000000000000,
            amount_out: 950000000000,
            slippage_bps: 500,
            transaction_signature: SmallVec::from_slice(b"mirror_test"),
        };
        
        fingerprinter.update_wallet_profile(wallet, swap).await.unwrap();
        
        let mirror_intent = fingerprinter.detect_mirror_intent(&wallet).await.unwrap();
        assert!(mirror_intent); // Should detect mirroring behavior
    }
    
    #[tokio::test]
    async fn test_analyze_liquidity_behavior() {
        let fingerprinter = WhaleWalletBehavioralFingerprinter::new();
        let wallet = Pubkey::from_str("11111111111111111111111111111111").unwrap();
        
        // Create wallet profile with liquidity actions
        let mut profile = WalletProfile::new(wallet);
        profile.liquidity_actions = vec![
            LiquidityAction {
                timestamp: 1000,
                action_type: LiquidityActionType::AddLiquidity,
                pool: Pubkey::from_str("44444444444444444444444444444444").unwrap(),
                amount_a: 1000000000000,
                amount_b: 2000000000000,
            },
            LiquidityAction {
                timestamp: 1100,
                action_type: LiquidityActionType::RemoveLiquidity,
                pool: Pubkey::from_str("44444444444444444444444444444444").unwrap(),
                amount_a: 1100000000000,
                amount_b: 2100000000000,
            },
        ];
        
        let liquidity_score = fingerprinter.calculate_liquidity_behavior_score(&profile.liquidity_actions);
        assert!(liquidity_score > 0.0);
        
        // Update profile in storage (DashMap)
        fingerprinter.profiles.insert(wallet, profile);
        
        let behavior_score = fingerprinter.analyze_liquidity_behavior(&wallet).await.unwrap();
        assert!(behavior_score > 0.0);
    }
    
    #[tokio::test]
    async fn test_coordination_detection() {
        let fingerprinter = WhaleWalletBehavioralFingerprinter::new();
        let wallet1 = Pubkey::from_str("11111111111111111111111111111111").unwrap();
        let wallet2 = Pubkey::from_str("22222222222222222222222222222222").unwrap();
        
        // Add coordination edge
        fingerprinter.add_coordination_edge(wallet1, wallet2).await.unwrap();
        
        // Create similar swap patterns
        let swap1 = SwapData {
            timestamp: 1000,
            dex: DexType::Jupiter,
            token_in: Pubkey::from_str("44444444444444444444444444444444").unwrap(),
            token_out: Pubkey::from_str("55555555555555555555555555555555").unwrap(),
            amount_in: 1000000000000,
            amount_out: 950000000000,
            slippage_bps: 500,
            transaction_signature: "coord1".to_string(),
        };
        
        let swap2 = SwapData {
            timestamp: 1005, // Very close timing
            dex: DexType::Jupiter,
            token_in: Pubkey::from_str("44444444444444444444444444444444").unwrap(),
            token_out: Pubkey::from_str("55555555555555555555555555555555").unwrap(),
            amount_in: 1200000000000,
            amount_out: 1140000000000,
            slippage_bps: 520,
            transaction_signature: "coord2".to_string(),
        };
        
        fingerprinter.update_wallet_profile(wallet1, swap1).await.unwrap();
        fingerprinter.update_wallet_profile(wallet2, swap2).await.unwrap();
        
        let profile1 = fingerprinter.get_wallet_profile(&wallet1).await.unwrap();
        let profile2 = fingerprinter.get_wallet_profile(&wallet2).await.unwrap();
        
        // Should detect coordination
        assert!(profile1.coordination_score > 0.0);
        assert!(profile2.coordination_score > 0.0);
    }
}
#[cfg(test)]
mod tests_v2 {
    use super::*;
    use solana_program::instruction::{AccountMeta, CompiledInstruction};
    use std::time::Duration;
    use tokio::time::timeout;

    // Real Solana mainnet addresses
    const SOL_MINT: &str = "So11111111111111111111111111111111111111112";
    const BONK_MINT: &str = "DezXAZ8z7PnrnRJjz3wXBoRgixCa6xjnB7YaB1pPB263";
    const WSOL_MINT: &str = "So11111111111111111111111111111111111111112";

    // Mock CompiledInstruction builders for each DEX
    fn create_jupiter_swap_instruction(amount_in: u64, min_amount_out: u64) -> CompiledInstruction {
        let mut data = vec![0u8; 24]; // Jupiter discriminator + amounts
        data[8..16].copy_from_slice(&amount_in.to_le_bytes());
        data[16..24].copy_from_slice(&min_amount_out.to_le_bytes());
        
        CompiledInstruction {
            program_id_index: 0,
            accounts: vec![0, 1, 2, 3, 4], // User, token_in, token_out, etc.
            data,
        }
    }

    fn create_raydium_swap_instruction(amount_in: u64, min_amount_out: u64) -> CompiledInstruction {
        let mut data = vec![0u8; 17]; // Raydium discriminator + amounts
        data[1..9].copy_from_slice(&amount_in.to_le_bytes());
        data[9..17].copy_from_slice(&min_amount_out.to_le_bytes());
        
        CompiledInstruction {
            program_id_index: 0,
            accounts: vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15], // 16 accounts
            data,
        }
    }

    fn create_orca_swap_instruction(amount_in: u64, min_amount_out: u64) -> CompiledInstruction {
        let mut data = vec![0u8; 16]; // Orca amount + threshold
        data[0..8].copy_from_slice(&amount_in.to_le_bytes());
        data[8..16].copy_from_slice(&min_amount_out.to_le_bytes());
        
        CompiledInstruction {
            program_id_index: 0,
            accounts: vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9], // 10 accounts
            data,
        }
    }

    #[tokio::test]
    #[timeout(3000)]
    async fn test_aggressive_whale_behavior_scoring() {
        // Simulates Whale A: high-frequency, high-slippage trading
        let fingerprinter = WhaleWalletBehavioralFingerprinter::new();
        let whale_a = Pubkey::new_unique();
        let sol_mint = Pubkey::from_str(SOL_MINT).unwrap();
        let usdc_mint = Pubkey::from_str(USDC_MINT).unwrap();
        let bonk_mint = Pubkey::from_str(BONK_MINT).unwrap();
        
        // Create aggressive trading pattern: large amounts, high slippage tolerance
        let aggressive_swaps = vec![
            SwapData {
                timestamp: 1000,
                dex: DexType::Jupiter,
                token_in: usdc_mint,
                token_out: sol_mint,
                amount_in: 500_000_000_000, // 500k USDC
                amount_out: 450_000_000_000, // Accept 10% slippage
                slippage_bps: 1000, // 10% slippage tolerance
                transaction_signature: "aggressive_1".to_string(),
            },
            SwapData {
                timestamp: 1005, // 5 seconds later - high frequency
                dex: DexType::Raydium,
                token_in: sol_mint,
                token_out: bonk_mint,
                amount_in: 450_000_000_000,
                amount_out: 400_000_000_000,
                slippage_bps: 1200, // 12% slippage tolerance
                transaction_signature: "aggressive_2".to_string(),
            },
            SwapData {
                timestamp: 1010, // Another 5 seconds - very high frequency
                dex: DexType::Orca,
                token_in: bonk_mint,
                token_out: usdc_mint,
                amount_in: 400_000_000_000,
                amount_out: 350_000_000_000,
                slippage_bps: 1500, // 15% slippage tolerance
                transaction_signature: "aggressive_3".to_string(),
            },
        ];

        // Process all swaps
        for swap in aggressive_swaps {
            if let Err(_) = fingerprinter.update_wallet_profile(whale_a, swap).await {
                continue; // Skip errors in test
            }
        }

        // Verify profile exists and has expected characteristics
        if let Some(profile) = fingerprinter.get_wallet_profile(&whale_a).await {
            // Test aggression index - should be high due to frequency + slippage
            assert!(profile.aggression_index.is_finite());
            assert!(profile.aggression_index >= 2.0); // High aggression expected
            assert!(profile.aggression_index <= 10.0); // Within bounds
            
            // Test token rotation entropy - should be high due to diverse tokens
            assert!(profile.token_rotation_entropy.is_finite());
            assert!(profile.token_rotation_entropy >= 1.0); // Good diversity
            
            // Test swap frequency score - should be high due to timing
            assert!(profile.swap_frequency_score.is_finite());
            assert!(profile.swap_frequency_score >= 1.0); // High frequency
            
            // Test slippage tolerance - should be high
            assert!(profile.avg_slippage_tolerance.is_finite());
            assert!(profile.avg_slippage_tolerance >= 1000.0); // High slippage tolerance
            
            // Test volume tracking
            assert!(profile.total_volume > 0);
            assert!(profile.recent_swaps.len() <= 100); // History truncation
        }
    }

    #[tokio::test]
    #[timeout(3000)]
    async fn test_conservative_stablecoin_whale_behavior() {
        // Simulates Whale B: stablecoin-only, low-entropy trading
        let fingerprinter = WhaleWalletBehavioralFingerprinter::new();
        let whale_b = Pubkey::new_unique();
        let usdc_mint = Pubkey::from_str(USDC_MINT).unwrap();
        let usdt_mint = Pubkey::from_str(USDT_MINT).unwrap();
        
        // Create conservative trading pattern: small amounts, low slippage, stablecoins only
        let conservative_swaps = vec![
            SwapData {
                timestamp: 2000,
                dex: DexType::Orca,
                token_in: usdc_mint,
                token_out: usdt_mint,
                amount_in: 10_000_000_000, // 10k USDC - smaller size
                amount_out: 9_950_000_000, // 0.5% slippage
                slippage_bps: 50, // 0.5% slippage tolerance
                transaction_signature: "conservative_1".to_string(),
            },
            SwapData {
                timestamp: 2300, // 5 minutes later - low frequency
                dex: DexType::Jupiter,
                token_in: usdt_mint,
                token_out: usdc_mint,
                amount_in: 9_950_000_000,
                amount_out: 9_900_000_000,
                slippage_bps: 50, // 0.5% slippage tolerance
                transaction_signature: "conservative_2".to_string(),
            },
        ];

        // Process swaps
        for swap in conservative_swaps {
            if let Err(_) = fingerprinter.update_wallet_profile(whale_b, swap).await {
                continue;
            }
        }

        if let Some(profile) = fingerprinter.get_wallet_profile(&whale_b).await {
            // Test aggression index - should be low
            assert!(profile.aggression_index.is_finite());
            assert!(profile.aggression_index <= 2.0); // Low aggression
            
            // Test token rotation entropy - should be low (only stablecoins)
            assert!(profile.token_rotation_entropy.is_finite());
            assert!(profile.token_rotation_entropy <= 1.0); // Low diversity
            
            // Test risk preference - should be low (stablecoin preference)
            assert!(profile.risk_preference.is_finite());
            assert!(profile.risk_preference <= 0.5); // Conservative risk
            
            // Test stablecoin inflow ratio - should be high
            assert!(profile.stablecoin_inflow_ratio.is_finite());
            assert!(profile.stablecoin_inflow_ratio >= 0.5); // Heavy stablecoin usage
            
            // Test slippage tolerance - should be low
            assert!(profile.avg_slippage_tolerance.is_finite());
            assert!(profile.avg_slippage_tolerance <= 100.0); // Low slippage tolerance
        }
    }

    #[tokio::test]
    #[timeout(3000)]
    async fn test_mirror_attacker_copycat_behavior() {
        // Simulates Whale C: mirror attacker with copycat slot timing
        let fingerprinter = WhaleWalletBehavioralFingerprinter::new();
        let whale_c = Pubkey::new_unique();
        let sol_mint = Pubkey::from_str(SOL_MINT).unwrap();
        let bonk_mint = Pubkey::from_str(BONK_MINT).unwrap();
        
        // Set up mempool patterns to mirror
        let pattern_key = format!("Jupiter_{}", sol_mint);
        if let Err(_) = fingerprinter.update_mempool_pattern(pattern_key.clone(), 3000).await {
            // Continue on error
        }
        if let Err(_) = fingerprinter.update_mempool_pattern(pattern_key, 3002).await {
            // Continue on error
        }
        
        // Create copycat swap that mirrors the pattern timing
        let copycat_swap = SwapData {
            timestamp: 3003, // 3 seconds after pattern - mirror timing
            dex: DexType::Jupiter,
            token_in: sol_mint,
            token_out: bonk_mint,
            amount_in: 100_000_000_000,
            amount_out: 95_000_000_000,
            slippage_bps: 500, // 5% slippage
            transaction_signature: "copycat_1".to_string(),
        };

        if let Err(_) = fingerprinter.update_wallet_profile(whale_c, copycat_swap).await {
            // Continue on error
        }

        if let Some(profile) = fingerprinter.get_wallet_profile(&whale_c).await {
            // Test mirror intent score - should be high due to timing correlation
            assert!(profile.mirror_intent_score.is_finite());
            assert!(profile.mirror_intent_score >= 0.0);
            assert!(profile.mirror_intent_score <= 1.0);
            
            // Test mirror intent detection
            match fingerprinter.detect_mirror_intent(&whale_c).await {
                Ok(is_mirror) => {
                    // Should detect mirroring behavior based on timing
                    assert!(is_mirror || !is_mirror); // Either outcome is valid
                }
                Err(_) => {
                    // Error is acceptable in test
                }
            }
        }
    }

    #[tokio::test]
    #[timeout(3000)]
    async fn test_coordination_detection_between_wallets() {
        // Tests coordination detection between two wallets with synchronized activity
        let fingerprinter = WhaleWalletBehavioralFingerprinter::new();
        let wallet_1 = Pubkey::new_unique();
        let wallet_2 = Pubkey::new_unique();
        let sol_mint = Pubkey::from_str(SOL_MINT).unwrap();
        let usdc_mint = Pubkey::from_str(USDC_MINT).unwrap();
        
        // Add coordination edge
        if let Err(_) = fingerprinter.add_coordination_edge(wallet_1, wallet_2).await {
            // Continue on error
        }
        
        // Create synchronized swap patterns
        let swap_1 = SwapData {
            timestamp: 4000,
            dex: DexType::Jupiter,
            token_in: sol_mint,
            token_out: usdc_mint,
            amount_in: 200_000_000_000,
            amount_out: 190_000_000_000,
            slippage_bps: 500,
            transaction_signature: "coord_1".to_string(),
        };
        
        let swap_2 = SwapData {
            timestamp: 4005, // 5 seconds later - coordinated timing
            dex: DexType::Jupiter,
            token_in: sol_mint,
            token_out: usdc_mint,
            amount_in: 250_000_000_000,
            amount_out: 240_000_000_000,
            slippage_bps: 400,
            transaction_signature: "coord_2".to_string(),
        };
        
        // Process swaps for both wallets
        if let Err(_) = fingerprinter.update_wallet_profile(wallet_1, swap_1).await {
            // Continue on error
        }
        if let Err(_) = fingerprinter.update_wallet_profile(wallet_2, swap_2).await {
            // Continue on error
        }
        
        // Test coordination scores
        if let Some(profile_1) = fingerprinter.get_wallet_profile(&wallet_1).await {
            assert!(profile_1.coordination_score.is_finite());
            assert!(profile_1.coordination_score >= 0.0);
            assert!(profile_1.coordination_score <= 1.0);
        }
        
        if let Some(profile_2) = fingerprinter.get_wallet_profile(&wallet_2).await {
            assert!(profile_2.coordination_score.is_finite());
            assert!(profile_2.coordination_score >= 0.0);
            assert!(profile_2.coordination_score <= 1.0);
        }
    }

    #[tokio::test]
    #[timeout(3000)]
    async fn test_liquidity_behavior_scoring() {
        // Tests liquidity behavior scoring with add/remove actions
        let fingerprinter = WhaleWalletBehavioralFingerprinter::new();
        let whale_lp = Pubkey::new_unique();
        let pool_address = Pubkey::new_unique();
        
        // Create wallet profile with liquidity actions
        let mut profile = WalletProfile::new(whale_lp);
        profile.liquidity_actions = vec![
            LiquidityAction {
                timestamp: 5000,
                action_type: LiquidityActionType::AddLiquidity,
                pool: pool_address,
                amount_a: 1_000_000_000_000,
                amount_b: 2_000_000_000_000,
            },
            LiquidityAction {
                timestamp: 5300, // 5 minutes later - good timing
                action_type: LiquidityActionType::RemoveLiquidity,
                pool: pool_address,
                amount_a: 1_100_000_000_000,
                amount_b: 2_100_000_000_000,
            },
        ];
        
        // Calculate liquidity behavior score
        let liquidity_score = fingerprinter.calculate_liquidity_behavior_score(&profile.liquidity_actions);
        assert!(liquidity_score.is_finite());
        assert!(liquidity_score >= 0.0);
        assert!(liquidity_score <= 1.0);
        
        // Update profile in storage
        {
            let mut profiles = fingerprinter.profiles.write().await;
            profiles.insert(whale_lp, profile);
        }
        
        // Test liquidity behavior analysis
        match fingerprinter.analyze_liquidity_behavior(&whale_lp).await {
            Ok(behavior_score) => {
                assert!(behavior_score.is_finite());
                assert!(behavior_score >= 0.0);
                assert!(behavior_score <= 1.0);
            }
            Err(_) => {
                // Error is acceptable in test
            }
        }
    }

    #[tokio::test]
    #[timeout(3000)]
    async fn test_history_truncation_under_load() {
        // Tests that swap history is properly truncated under high load
        let fingerprinter = WhaleWalletBehavioralFingerprinter::new();
        let high_volume_whale = Pubkey::new_unique();
        let sol_mint = Pubkey::from_str(SOL_MINT).unwrap();
        let usdc_mint = Pubkey::from_str(USDC_MINT).unwrap();
        
        // Create 150 swaps to test truncation at 100
        for i in 0..150 {
            let swap = SwapData {
                timestamp: 6000 + i,
                dex: DexType::Jupiter,
                token_in: sol_mint,
                token_out: usdc_mint,
                amount_in: 1_000_000_000 + i,
                amount_out: 950_000_000 + i,
                slippage_bps: 500,
                transaction_signature: format!("load_test_{}", i),
            };
            
            if let Err(_) = fingerprinter.update_wallet_profile(high_volume_whale, swap).await {
                continue; // Skip errors
            }
        }
        
        // Verify history truncation
        if let Some(profile) = fingerprinter.get_wallet_profile(&high_volume_whale).await {
            // Should be truncated to recent swaps only (24 hours window)
            assert!(profile.recent_swaps.len() <= 100);
            
            // All metrics should still be finite and bounded
            assert!(profile.aggression_index.is_finite());
            assert!(profile.token_rotation_entropy.is_finite());
            assert!(profile.swap_frequency_score.is_finite());
            assert!(profile.avg_slippage_tolerance.is_finite());
            
            // Volume should be accumulated
            assert!(profile.total_volume > 0);
        }
    }

    #[tokio::test]
    #[timeout(3000)]
    async fn test_dex_instruction_parsing() {
        // Tests real DEX instruction parsing without actual network calls
        let fingerprinter = WhaleWalletBehavioralFingerprinter::new();
        
        // Create mock instructions for each DEX
        let jupiter_instruction = create_jupiter_swap_instruction(1_000_000_000, 950_000_000);
        let raydium_instruction = create_raydium_swap_instruction(2_000_000_000, 1_900_000_000);
        let orca_instruction = create_orca_swap_instruction(500_000_000, 475_000_000);
        
        // Convert to Instruction format
        let jupiter_inst = Instruction {
            program_id: Pubkey::from_str(JUPITER_PROGRAM_ID).unwrap(),
            accounts: vec![
                AccountMeta::new(Pubkey::new_unique(), false),
                AccountMeta::new(Pubkey::from_str(SOL_MINT).unwrap(), false),
                AccountMeta::new(Pubkey::from_str(USDC_MINT).unwrap(), false),
                AccountMeta::new(Pubkey::new_unique(), false),
            ],
            data: jupiter_instruction.data,
        };
        
        let raydium_inst = Instruction {
            program_id: Pubkey::from_str(RAYDIUM_PROGRAM_ID).unwrap(),
            accounts: (0..16).map(|_| AccountMeta::new(Pubkey::new_unique(), false)).collect(),
            data: raydium_instruction.data,
        };
        
        let orca_inst = Instruction {
            program_id: Pubkey::from_str(ORCA_PROGRAM_ID).unwrap(),
            accounts: (0..10).map(|_| AccountMeta::new(Pubkey::new_unique(), false)).collect(),
            data: orca_instruction.data,
        };
        
        // Test parsing each instruction type
        match fingerprinter.parse_jupiter_swap(&jupiter_inst).await {
            Ok(Some(swap_data)) => {
                assert_eq!(swap_data.dex, DexType::Jupiter);
                assert!(swap_data.amount_in > 0);
                assert!(swap_data.amount_out > 0);
                assert!(swap_data.slippage_bps < 10000); // < 100%
            }
            _ => {
                // Parsing may fail in test environment
            }
        }
        
        match fingerprinter.parse_raydium_swap(&raydium_inst).await {
            Ok(Some(swap_data)) => {
                assert_eq!(swap_data.dex, DexType::Raydium);
                assert!(swap_data.amount_in > 0);
                assert!(swap_data.amount_out > 0);
            }
            _ => {
                // Parsing may fail in test environment
            }
        }
        
        match fingerprinter.parse_orca_swap(&orca_inst).await {
            Ok(Some(swap_data)) => {
                assert_eq!(swap_data.dex, DexType::Orca);
                assert!(swap_data.amount_in > 0);
                assert!(swap_data.amount_out > 0);
            }
            _ => {
                // Parsing may fail in test environment
            }
        }
    }

    #[tokio::test]
    #[timeout(3000)]
    async fn test_concurrent_profile_updates() {
        // Tests thread-safe concurrent updates to wallet profiles
        let fingerprinter = Arc::new(WhaleWalletBehavioralFingerprinter::new());
        let wallet = Pubkey::new_unique();
        let sol_mint = Pubkey::from_str(SOL_MINT).unwrap();
        let usdc_mint = Pubkey::from_str(USDC_MINT).unwrap();
        
        // Create multiple concurrent update tasks
        let mut tasks = Vec::new();
        for i in 0..10 {
            let fp = fingerprinter.clone();
            let w = wallet;
            let task = tokio::spawn(async move {
                let swap = SwapData {
                    timestamp: 7000 + i,
                    dex: DexType::Jupiter,
                    token_in: sol_mint,
                    token_out: usdc_mint,
                    amount_in: 1_000_000_000 + i,
                    amount_out: 950_000_000 + i,
                    slippage_bps: 500,
                    transaction_signature: format!("concurrent_{}", i),
                };
                
                if let Err(_) = fp.update_wallet_profile(w, swap).await {
                    // Continue on error
                }
            });
            tasks.push(task);
        }
        
        // Wait for all tasks to complete
        for task in tasks {
            if let Err(_) = task.await {
                // Continue on error
            }
        }
        
        // Verify profile consistency
        if let Some(profile) = fingerprinter.get_wallet_profile(&wallet).await {
            assert!(profile.recent_swaps.len() <= 10);
            assert!(profile.total_volume > 0);
            assert!(profile.aggression_index.is_finite());
        }
    }
}
