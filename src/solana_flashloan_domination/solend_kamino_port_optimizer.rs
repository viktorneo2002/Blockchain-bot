// === COST/PRICE globals ===
// === [IMPROVED: PIECE LUT-SELECTOR v56] ===
impl PortOptimizer {
    /// Collect all unique account keys referenced by a plan of Instructions.
    fn plan_account_keys(ixs: &[solana_sdk::instruction::Instruction]) -> std::collections::BTreeSet<solana_sdk::pubkey::Pubkey> {
        use std::collections::BTreeSet;
        let mut set = BTreeSet::new();
        for ix in ixs {
            set.insert(ix.program_id);
            for am in &ix.accounts { set.insert(am.pubkey); }
        }
        set
    }

    /// Fetch a lookup table account from RPC.
    async fn fetch_lut(&self, addr: &solana_sdk::pubkey::Pubkey) -> Result<solana_sdk::message::v0::AddressLookupTableAccount> {
        let resp = self.rpc_client.get_address_lookup_table(addr).await?;
        match resp.value {
            Some(lut) => Ok(lut),
            None => Err(anyhow::anyhow!("LUT not found: {addr}")),
        }
    }

    /// Select up to `max_luts` LUTs (from `candidates`) that maximize address coverage for `ixs`.
    /// Only LUTs adding at least `min_new_addrs` new addresses are selected (greedy).
    pub async fn select_luts_for_plan(
        &self,
        ixs: &[solana_sdk::instruction::Instruction],
        candidate_lut_addrs: &[solana_sdk::pubkey::Pubkey],
        max_luts: usize,
        min_new_addrs: usize,
    ) -> Result<Vec<solana_sdk::message::v0::AddressLookupTableAccount>> {
        use futures_util::future::join_all;
        use std::collections::BTreeSet;

        if candidate_lut_addrs.is_empty() || max_luts == 0 { return Ok(Vec::new()); }

        // Fetch all LUTs concurrently
        let futs = candidate_lut_addrs.iter().map(|a| self.fetch_lut(a));
        let mut luts: Vec<solana_sdk::message::v0::AddressLookupTableAccount> = join_all(futs)
            .await
            .into_iter()
            .filter_map(|r| r.ok())
            .collect();

        if luts.is_empty() { return Ok(Vec::new()); }

        let needed: BTreeSet<solana_sdk::pubkey::Pubkey> = Self::plan_account_keys(ixs);

        // Greedy cover
        let mut chosen = Vec::new();
        let mut covered: BTreeSet<solana_sdk::pubkey::Pubkey> = BTreeSet::new();
        for _ in 0..max_luts {
            let (best_idx, best_gain) = luts
                .iter()
                .enumerate()
                .map(|(i, lut)| {
                    let gain = lut
                        .addresses
                        .iter()
                        .filter(|p| needed.contains(p) && !covered.contains(p))
                        .count();
                    (i, gain)
                })
                .max_by_key(|(_, g)| *g)
                .unwrap_or((0, 0));

            if best_gain < min_new_addrs { break; }
            let lut = luts.swap_remove(best_idx);
            covered.extend(lut.addresses.iter().cloned());
            chosen.push(lut);
        }
        Ok(chosen)
    }
}

// === [IMPROVED: PIECE PLAN-LRU v56] ===
impl PortOptimizer {
    /// Digest an instruction vec in a canonical way (program_id + metas + data).
    fn digest_plan(ixs: &[solana_sdk::instruction::Instruction]) -> solana_sdk::hash::Hash {
        use solana_sdk::hash::hashv;
        let mut chunks: Vec<&[u8]> = Vec::with_capacity(ixs.len() * 3);
        for ix in ixs {
            chunks.push(ix.program_id.as_ref());
            for am in &ix.accounts {
                chunks.push(am.pubkey.as_ref());
                let flags = [(am.is_writable as u8), (am.is_signer as u8)];
                chunks.push(&flags);
            }
            chunks.push(&ix.data);
        }
        hashv(&chunks)
    }

    /// Returns `true` if plan is fresh (send it), `false` if a near-duplicate was recently sent.
    async fn plan_lru_should_send(&self, ixs: &[solana_sdk::instruction::Instruction]) -> bool {
        use std::collections::{HashMap, VecDeque};
        use tokio::sync::Mutex as TokioMutex;
        static PLAN_LRU_V56: std::sync::OnceLock<TokioMutex<(VecDeque<(solana_sdk::hash::Hash,u64)>, HashMap<solana_sdk::hash::Hash,u64>)>>> = std::sync::OnceLock::new();
        const PLAN_LRU_CAP: usize = 256;
        const PLAN_DEDUP_SLOTS: u64 = 40;

        let (digest, slot_now) = {
            let d = Self::digest_plan(ixs);
            let s = if let Ok(b) = self.get_fresh_blockhash_bundle().await { b.now_slot }
                    else { self.rpc_client.get_slot().await.unwrap_or_default() };
            (d, s)
        };

        let lru = PLAN_LRU_V56.get_or_init(|| TokioMutex::new((VecDeque::new(), HashMap::new())));
        let mut guard = lru.lock().await;
        let (q, map) = (&mut guard.0, &mut guard.1);

        if let Some(&slot_prev) = map.get(&digest) {
            if slot_now.saturating_sub(slot_prev) <= PLAN_DEDUP_SLOTS { return false; }
        }
        // insert/update
        map.insert(digest, slot_now);
        q.push_back((digest, slot_now));
        // evict old
        while q.len() > PLAN_LRU_CAP {
            if let Some((old_d, _)) = q.pop_front() { map.remove(&old_d); }
        }
        true
    }
}

// === [IMPROVED: PIECE ATA-COALESCE v56] ===
impl PortOptimizer {
    /// Remove duplicate `create_associated_token_account` instructions targeting the same ATA.
    fn coalesce_create_atas(&self, ixs: &mut Vec<solana_sdk::instruction::Instruction>) {
        use std::collections::HashSet;
        let ata_program = spl_associated_token_account::id();
        let mut seen: HashSet<solana_sdk::pubkey::Pubkey> = HashSet::new();
        ixs.retain(|ix| {
            if ix.program_id != ata_program { return true; }
            if ix.accounts.len() < 2 { return true; }
            let ata = ix.accounts[1].pubkey;
            if seen.contains(&ata) { false } else { seen.insert(ata); true }
        });
    }
}

// === [IMPROVED: PIECE PLAN-BATCHER v56] ===
impl PortOptimizer {
    /// Greedy packer: merges plans while avoiding account write conflicts.
    pub fn batch_plans_nonconflicting(&self, plans: Vec<Vec<solana_sdk::instruction::Instruction>>) -> Vec<Vec<solana_sdk::instruction::Instruction>> {
        use std::collections::{BTreeSet, HashSet};

        fn keys_of(ixs: &[solana_sdk::instruction::Instruction]) -> (HashSet<solana_sdk::pubkey::Pubkey>, HashSet<solana_sdk::pubkey::Pubkey>) {
            let mut w = HashSet::new();
            let mut r = HashSet::new();
            for ix in ixs {
                for am in &ix.accounts {
                    if am.is_writable { w.insert(am.pubkey); } else { r.insert(am.pubkey); }
                }
            }
            (w, r)
        }

        let mut batches: Vec<(Vec<solana_sdk::instruction::Instruction>, HashSet<solana_sdk::pubkey::Pubkey>, HashSet<solana_sdk::pubkey::Pubkey>)> = Vec::new();
        'plans: for p in plans {
            let (pw, pr) = keys_of(&p);
            for (b_ixs, bw, br) in batches.iter_mut() {
                let conflict = !pw.is_disjoint(bw) || !pw.is_disjoint(br) || !bw.is_disjoint(&pr);
                if !conflict {
                    b_ixs.extend(p);
                    bw.extend(pw.iter());
                    br.extend(pr.iter());
                    continue 'plans;
                }
            }
            // new batch
            batches.push((p, pw, pr));
        }
        batches.into_iter().map(|(ixs, _, _)| ixs).collect()
    }
}

// === [IMPROVED: PIECE FEE-SCHEDULER v56] ===
impl PortOptimizer {
    /// Compute a multiplier from recent performance samples (5-slot window).
    async fn saturation_multiplier(&self) -> f64 {
        let samples = match self.rpc_client.get_recent_performance_samples(Some(5)).await {
            Ok(v) if !v.is_empty() => v,
            _ => return 1.0,
        };
        let avg_tps = samples.iter()
            .map(|s| (s.num_transactions as f64) / (s.sample_period_secs as f64))
            .sum::<f64>() / (samples.len() as f64);
        let baseline_tps = 3_000.0_f64;
        (avg_tps / baseline_tps).clamp(0.8, 2.0)
    }

    /// Quantile of recent priority fee (lamports) with small timeout; fall back to PRIORITY_FEE_LAMPORTS.
    async fn priority_fee_price_quantile(&self, quantile: f64) -> u64 {
        use tokio::time::timeout;
        let scope: Vec<solana_sdk::pubkey::Pubkey> = vec![
            solana_program::compute_budget::id(),
            solana_program::system_program::id(),
        ];
        let res = timeout(std::time::Duration::from_millis(200), self.rpc_client.get_recent_prioritization_fees(&scope)).await;
        if let Ok(Ok(fees)) = res {
            if !fees.is_empty() {
                let mut v: Vec<u64> = fees.into_iter().map(|f| f.prioritization_fee.max(1)).collect();
                v.sort_unstable();
                let k = ((v.len() as f64 * quantile).floor() as usize).clamp(0, v.len()-1);
                return v[k].max(1);
            }
        }
        PRIORITY_FEE_LAMPORTS.max(1)
    }

    /// Priority fee = quantile price × saturation multiplier (rounded).
    pub async fn priority_fee_dynamic(&self, quantile: f64) -> u64 {
        let base = self.priority_fee_price_quantile(quantile).await.max(1);
        let mult = self.saturation_multiplier().await;
        ((base as f64) * mult).round().max(1.0) as u64
    }

    /// Drop-in replacement that uses dynamic fee but keeps the precise CU headroom logic.
    pub async fn precise_budget_ixs_dynamic(
        &self,
        ixs: &[solana_sdk::instruction::Instruction],
        desired_quantile: f64,
        extra_margin_ratio: f64,
        min_extra_units: u32,
    ) -> Result<(solana_sdk::instruction::Instruction, solana_sdk::instruction::Instruction)> {
        let generous = 1_400_000u32;
        let mut probe_ixs = Vec::with_capacity(ixs.len() + 2);
        probe_ixs.push(solana_sdk::compute_budget::ComputeBudgetInstruction::set_compute_unit_limit(generous));
        probe_ixs.push(solana_sdk::compute_budget::ComputeBudgetInstruction::set_compute_unit_price(1));
        probe_ixs.extend_from_slice(ixs);

        let bh = self.get_recent_blockhash_with_retry().await?;
        let tx = self.build_v0_tx(self.wallet.pubkey(), &probe_ixs, bh, &[])?;
        let sim = self.simulate_exact(&tx).await?;
        if let Some(err) = sim.err { anyhow::bail!("probe sim failed: {:?}", err); }
        let used = sim.units_consumed.unwrap_or(COMPUTE_UNITS as u64) as u32;

        let cu_limit = used
            .saturating_add(((used as f64) * extra_margin_ratio) as u32)
            .saturating_add(min_extra_units)
            .min(generous);

        let price = self.priority_fee_dynamic(desired_quantile).await;
        Ok((
            solana_sdk::compute_budget::ComputeBudgetInstruction::set_compute_unit_limit(cu_limit),
            solana_sdk::compute_budget::ComputeBudgetInstruction::set_compute_unit_price(price),
        ))
    }
}

// === [IMPROVED: PIECE SEND-PLAN v56] ===
impl PortOptimizer {
    /// Helper: forwarder to existing v0 builder with LUTs.
    fn build_v0_tx_with_luts(
        &self,
        payer: solana_sdk::pubkey::Pubkey,
        ixs: &[solana_sdk::instruction::Instruction],
        bh: solana_sdk::hash::Hash,
        luts: &[solana_sdk::message::v0::AddressLookupTableAccount],
        _alts: &[solana_sdk::message::v0::AddressLookupTableAccount],
    ) -> Result<solana_sdk::transaction::VersionedTransaction> {
        // Reuse the canonical builder that accepts a LUT slice (alts kept for signature parity)
        self.build_v0_tx(payer, ixs, bh, luts)
    }

    /// Fire a plan with auto LUT selection and dynamic compute budget.
    pub async fn send_plan_v0_smart(
        &self,
        mut ixs: Vec<solana_sdk::instruction::Instruction>,
        candidate_luts: &[solana_sdk::pubkey::Pubkey],
        timeout: std::time::Duration,
    ) -> Result<solana_sdk::signature::Signature> {
        // 0) Skip duplicate plans within a short slot window
        if !self.plan_lru_should_send(&ixs).await {
            anyhow::bail!("plan de-duplicated (recently sent identical instruction set)");
        }

        // 1) Coalesce duplicate ATA creates
        self.coalesce_create_atas(&mut ixs);

        // 2) Choose LUTs greedily (0..2 defaults; ≥4 new addrs threshold)
        let chosen_luts = self.select_luts_for_plan(&ixs, candidate_luts, 2, 4).await?;

        // 3) Dynamic compute-budget tuning (p90, +12% and +8k)
        let (cu_ix, fee_ix) = self.precise_budget_ixs_dynamic(&ixs, 0.90, 0.12, 8_000).await?;
        ixs.insert(0, fee_ix);
        ixs.insert(0, cu_ix);

        // 4) Build v0 with or without LUTs and sanity-sim
        let bh = self.get_recent_blockhash_with_retry().await?;
        let tx = if chosen_luts.is_empty() {
            self.build_v0_tx(self.wallet.pubkey(), &ixs, bh, &[])?
        } else {
            self.build_v0_tx_with_luts(self.wallet.pubkey(), &ixs, bh, &chosen_luts, &[])?
        };
        let sim = self.simulate_exact(&tx).await?;
        if let Some(err) = sim.err { anyhow::bail!("preflight sim failed: {:?}", err); }

        // 5) Slot-guarded send with automatic blockhash refresh
        let sig = self.send_v0_with_slot_guard(
            |bh2| {
                if chosen_luts.is_empty() {
                    self.build_v0_tx(self.wallet.pubkey(), &ixs, bh2, &[])
                } else {
                    self.build_v0_tx_with_luts(self.wallet.pubkey(), &ixs, bh2, &chosen_luts, &[])
                }
            },
            timeout,
            0,
            120,
        ).await?;
        Ok(sig)
    }
}

// Global EMA floor for CU price shaping (used by multiple paths)
static SURGE_EMA: OnceLock<AtomicU64> = OnceLock::new();
static UNITS_EMA: OnceLock<AtomicU64> = OnceLock::new();

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
enum FeeRegime { Calm, Elevated, Surge }

#[inline]
fn classify_regime(short_p90: u64, long_p90: u64) -> FeeRegime {
    let s = short_p90.max(1);
    let l = long_p90.max(1);
    if s >= 150_000 || (2 * s) >= (5 * l) { FeeRegime::Surge }
    else if s >= 50_000 || (10 * s) >= (14 * l) { FeeRegime::Elevated }
    else { FeeRegime::Calm }
}

#[inline] fn f64_load(a: &std::sync::atomic::AtomicU64) -> f64 { f64::from_bits(a.load(std::sync::atomic::Ordering::Relaxed)) }
#[inline] fn f64_store(a: &std::sync::atomic::AtomicU64, v: f64) { a.store(v.to_bits(), std::sync::atomic::Ordering::Relaxed); }

// P² single-quantile tracker (lock-guarded)
struct P2 {
    p: f64,
    n: [i64; 5], np: [f64; 5], dn: [f64; 5], q: [f64; 5], seed: [f64; 5],
    k: usize, initd: bool,
}
impl P2 {
    fn new(p: f64) -> Self {
        let dn = [0.0, p/2.0, p, (1.0+p)/2.0, 1.0];
        Self { p, n: [0;5], np: [0.0;5], dn, q: [0.0;5], seed: [0.0;5], k: 0, initd: false }
    }
    #[inline] fn value(&self) -> Option<f64> { if self.initd { Some(self.q[2]) } else if self.k>0 { let mut tmp = self.seed; tmp[..self.k].sort_by(|a,b| a.partial_cmp(b).unwrap()); Some(tmp[(self.k-1).min(2)]) } else { None } }
    fn observe(&mut self, x: f64) {
        if !self.initd {
            if self.k < 5 { self.seed[self.k]=x; self.k+=1; }
            if self.k == 5 {
                self.seed.sort_by(|a,b| a.partial_cmp(b).unwrap());
                self.q.copy_from_slice(&self.seed);
                self.n = [1,2,3,4,5];
                self.np = [1.0, 1.0+2.0*self.p, 1.0+4.0*self.p, 3.0+2.0*self.p, 5.0];
                self.initd = true;
            }
            return;
        }
        let k = if x < self.q[0] { self.q[0]=x; 0 }
        else if x < self.q[1] { 0 }
        else if x < self.q[2] { 1 }
        else if x < self.q[3] { 2 }
        else if x <= self.q[4] { 3 }
        else { self.q[4]=x; 3 };
        for i in (k+1)..5 { self.n[i]+=1; }
        for i in 0..5 { self.np[i]+=self.dn[i]; }
        for i in 1..4 {
            let d = self.np[i] - (self.n[i] as f64);
            if (d >= 1.0 && (self.n[i+1]-self.n[i])>1) || (d <= -1.0 && (self.n[i-1]-self.n[i])< -1) {
                let s = d.signum();
                let qi = self.q[i];
                let qip = qi + s * (
                    ((self.n[i]-self.n[i-1]) as f64 + s) * (self.q[i+1]-qi) / ((self.n[i+1]-self.n[i]) as f64)
                    + ((self.n[i+1]-self.n[i]) as f64 - s) * (qi - self.q[i-1]) / ((self.n[i]-self.n[i-1]) as f64)
                ) / ((self.n[i+1]-self.n[i-1]) as f64);
                let qlin = if s > 0.0 { qi + (self.q[i+1]-qi) / ((self.n[i+1]-self.n[i]) as f64) } else { qi - (self.q[i-1]-qi) / ((self.n[i-1]-self.n[i]) as f64) };
                self.q[i] = if self.q[i-1] < qip && qip < self.q[i+1] { qip } else { qlin };
                self.n[i] += s as i64;
            }
        }
    }

}

impl PortOptimizer {
    // Infer token program for a given mint by querying the mint account owner.
    async fn token_program_for_mint(&self, mint: &Pubkey) -> Result<Pubkey> {
        match self.rpc_client.get_account(mint).await {
            Ok(acc) => {
                let owner = acc.owner;
                let spl  = spl_token::id();
                // Token-2022 program id literal (avoid requiring crate dependency)
                let spl2: Pubkey = solana_sdk::pubkey!("TokenzQdBNbLqP5VEhDWt9yaWwGmt4Q5CwMkpw8sH1");
                if owner == spl || owner == spl2 { Ok(owner) } else { Ok(spl) }
            }
            Err(e) => {
                // Fallback to SPL Token if we cannot fetch the mint account
                warn!("token_program_for_mint: fallback SPL due to error: {e}");
                Ok(spl_token::id())
            }
        }
    }

    // Associated token address for a specific token program (SPL or Token-2022)
    fn ata_for(owner: &Pubkey, mint: &Pubkey, token_program: &Pubkey) -> Pubkey {
        spl_associated_token_account::get_associated_token_address_with_program_id(owner, mint, token_program)
    }
}

static P2P75: OnceLock<tokio::sync::Mutex<P2>> = OnceLock::new();
static P2P90: OnceLock<tokio::sync::Mutex<P2>> = OnceLock::new();

static LOG_MU:      OnceLock<std::sync::atomic::AtomicU64> = OnceLock::new();
static LOG_VAR:     OnceLock<std::sync::atomic::AtomicU64> = OnceLock::new();
static LOG_MAD:     OnceLock<std::sync::atomic::AtomicU64> = OnceLock::new();
static LOG_X_LAST:  OnceLock<std::sync::atomic::AtomicU64> = OnceLock::new();
static LOG_TS_LAST: OnceLock<std::sync::atomic::AtomicU64> = OnceLock::new();
static MONO_START:  OnceLock<std::time::Instant>            = OnceLock::new();

#[inline] fn mono_ms() -> u64 { MONO_START.get_or_init(std::time::Instant::now).elapsed().as_millis() as u64 }

#[inline]
fn to_permille(x: f64) -> u32 {
    if !x.is_finite() || x <= 0.0 { return 1000; }
    let p = (x * 1000.0).round();
    p.clamp(0.0, 8000.0) as u32
}

// === [IMPROVED: PIECE OBS v50] ===
// complete init; adds alpha, deadband%, dt_ms, P² p75/p90 gauges.
use prometheus::{Registry, IntCounter, Gauge, IntGauge, Histogram, HistogramOpts, Opts};

pub struct Observability {
    // base
    pub rebalance_attempts:   IntCounter,
    pub rebalance_success:    IntCounter,
    pub rebalance_failure:    IntCounter,
    pub tip_latency_ms:       Histogram,
    pub resubmits_by_reason:  prometheus::IntCounterVec,
    pub refreshes_by_reason:  prometheus::IntCounterVec,
    pub breaker_trips:        IntCounter,
    pub cu_limit_gauge:       IntGauge,
    pub cu_price_gauge:       IntGauge,
    pub preprofit_bps_gauge:  Gauge,
    pub height_margin_gauge:  IntGauge,
    pub escalations_total:    IntCounter,
    pub profit_skips:         IntCounter,
    pub sim_failures:         IntCounter,
    pub send_failures:        IntCounter,
    pub regime_gauge:         IntGauge,
    pub zscore_gauge:         Gauge,
    pub gas_est_hist:         Histogram,
    pub fee_paid_hist:        Histogram,
    // v46
    pub coalesce_bhash_hits:  IntCounter,
    pub coalesce_resolve_hits:IntCounter,
    pub fork_retries:         IntCounter,
    pub bundle_timeouts:      IntCounter,
    pub short_p90_gauge:      IntGauge,
    pub long_p90_gauge:       IntGauge,
    pub sim_ms_hist:          Histogram,
    // v47
    pub confirm_primary_win:  IntCounter,
    pub confirm_fast_win:     IntCounter,
    pub underpriced_escal:    IntCounter,
    pub pacer_ms_gauge:       IntGauge,
    pub cu_override_gauge:    IntGauge,
    pub tip_lamports_gauge:   IntGauge,
    // v48
    pub fee_slope_ps_gauge:   Gauge,
    pub bhash_stale_reuse:    IntCounter,
    pub resolve_fallbacks:    IntCounter,
    // v49
    pub alpha_gauge:          Gauge,
    pub deadband_pct_gauge:   IntGauge,
    // v50
    pub fee_dt_ms_gauge:      IntGauge,
    pub p2_p75_gauge:         IntGauge,
    pub p2_p90_gauge:         IntGauge,
}

impl Observability {
    pub fn new(reg: &Registry) -> Self {
        use prometheus::{linear_buckets, IntCounterVec};

        // base counters/gauges/hists
        let attempts  = IntCounter::new("rebalance_attempts","rebalance attempts").unwrap();
        let success   = IntCounter::new("rebalance_success","rebalance success").unwrap();
        let failure   = IntCounter::new("rebalance_failure","rebalance failure").unwrap();
        let trips     = IntCounter::new("breaker_trips","circuit breaker trips").unwrap();
        let resub     = IntCounterVec::new(Opts::new("tx_resubmits_labeled","resubmits by reason"), &["reason"]).unwrap();
        let refreshes = IntCounterVec::new(Opts::new("blockhash_refreshes_labeled","refreshes by reason"), &["reason"]).unwrap();
        let cu_limit  = IntGauge::new("cu_limit","compute-unit limit set").unwrap();
        let cu_price  = IntGauge::new("cu_price_lamports","compute-unit price in lamports").unwrap();
        let pre_bps   = Gauge::new("preprofit_bps","preflight expected profit (bps)").unwrap();
        let height_m  = IntGauge::new("last_valid_height_margin","last_valid_height - current_height").unwrap();
        let tip_latency = Histogram::with_opts(HistogramOpts::new("tip_latency_ms","tip landing latency (ms)")
            .buckets(linear_buckets(10.0,50.0,100).unwrap())).unwrap();
        let escal = IntCounter::new("fee_escalations_total","total fee escalations").unwrap();
        let pskip = IntCounter::new("profit_skips_total","skipped due to non-positive preprofit").unwrap();
        let sfail = IntCounter::new("sim_failures_total","simulation failures").unwrap();
        let xfail = IntCounter::new("send_failures_total","send/confirm failures").unwrap();
        let regime= IntGauge::new("fee_regime","0 calm, 1 elevated, 2 surge").unwrap();
        let zscore= Gauge::new("fee_zscore_abs","abs z-score on log-fee").unwrap();
        let gas_h = Histogram::with_opts(HistogramOpts::new("gas_est_lamports","estimated gas (lamports)")
            .buckets(linear_buckets(50_000.0,50_000.0,40).unwrap())).unwrap();
        let fee_h = Histogram::with_opts(HistogramOpts::new("fee_paid_lamports","final fee paid (lamports)")
            .buckets(linear_buckets(50_000.0,50_000.0,40).unwrap())).unwrap();

        // v46
        let bh_coh = IntCounter::new("bhash_coalesce_hits","blockhash coalesce immediate hits").unwrap();
        let rv_coh = IntCounter::new("resolve_coalesce_hits","resolve coalesce immediate hits").unwrap();
        let forkry = IntCounter::new("fork_retries_total","fork/min_context_slot retries").unwrap();
        let btimeo = IntCounter::new("bundle_timeouts_total","bundle fetch timeouts").unwrap();
        let sp90   = IntGauge::new("short_p90","short horizon p90").unwrap();
        let lp90   = IntGauge::new("long_p90","long horizon p90").unwrap();
        let sim_ms = Histogram::with_opts(HistogramOpts::new("sim_duration_ms","exact sim duration (ms)")
            .buckets(linear_buckets(2.0, 2.0, 100).unwrap())).unwrap();

        // v47
        let cprim = IntCounter::new("confirm_primary_win_total","confirm fanout: primary client won").unwrap();
        let cfast = IntCounter::new("confirm_fast_win_total","confirm fanout: fast client won").unwrap();
        let uescal= IntCounter::new("underpriced_escalations_total","send errors indicating underpriced/too-low fee").unwrap();
        let pacer = IntGauge::new("pacer_sleep_ms","slotguard pacer planned sleep (ms)").unwrap();
        let cuovr = IntGauge::new("cu_override","current CU override from hints").unwrap();
        let tipg  = IntGauge::new("tip_lamports","tip lamports for this send").unwrap();

        // v48
        let slopeg = Gauge::new("fee_slope_per_sec","d(log fee)/dt (per second)").unwrap();
        let stale  = IntCounter::new("bhash_stale_reuse_total","stale bundle reused after hedge timeout").unwrap();
        let rfall  = IntCounter::new("resolve_fallbacks_total","resolve full-account fallbacks taken").unwrap();

        // v49
        let alpha  = Gauge::new("fee_alpha","ct-EMA alpha (0..1)").unwrap();
        let dbpct  = IntGauge::new("fee_deadband_pct","effective deadband percent").unwrap();

        // v50
        let dtms  = IntGauge::new("fee_dt_ms","dt used for ct-EMA (ms)").unwrap();
        let p2p75 = IntGauge::new("fee_p2_p75","P² live p75").unwrap();
        let p2p90 = IntGauge::new("fee_p2_p90","P² live p90").unwrap();

        // register everything
        for m in [&attempts,&success,&failure,&trips,&cu_limit,&cu_price,&escal,&pskip,&sfail,&xfail,
                  &regime,&bh_coh,&rv_coh,&forkry,&btimeo,&sp90,&lp90,&cprim,&cfast,&uescal,
                  &pacer,&cuovr,&tipg,&stale,&rfall,&dbpct,&dtms,&p2p75,&p2p90] {
            reg.register(Box::new((*m).clone())).ok();
        }
        for m in [&pre_bps as &dyn prometheus::core::Collector, &tip_latency, &resub, &refreshes, &zscore, &gas_h, &fee_h, &sim_ms, &slopeg, &alpha] {
            reg.register(Box::new(m.clone())).ok();
        }

        Self {
            rebalance_attempts: attempts, rebalance_success: success, rebalance_failure: failure,
            tip_latency_ms: tip_latency, resubmits_by_reason: resub, refreshes_by_reason: refreshes,
            breaker_trips: trips, cu_limit_gauge: cu_limit, cu_price_gauge: cu_price,
            preprofit_bps_gauge: pre_bps, height_margin_gauge: height_m, escalations_total: escal,
            profit_skips: pskip, sim_failures: sfail, send_failures: xfail, regime_gauge: regime,
            zscore_gauge: zscore, gas_est_hist: gas_h, fee_paid_hist: fee_h,
            coalesce_bhash_hits: bh_coh, coalesce_resolve_hits: rv_coh, fork_retries: forkry,
            bundle_timeouts: btimeo, short_p90_gauge: sp90, long_p90_gauge: lp90, sim_ms_hist: sim_ms,
            confirm_primary_win: cprim, confirm_fast_win: cfast, underpriced_escal: uescal,
            pacer_ms_gauge: pacer, cu_override_gauge: cuovr, tip_lamports_gauge: tipg,
            fee_slope_ps_gauge: slopeg, bhash_stale_reuse: stale, resolve_fallbacks: rfall,
            alpha_gauge: alpha, deadband_pct_gauge: dbpct,
            fee_dt_ms_gauge: dtms, p2_p75_gauge: p2p75, p2_p90_gauge: p2p90,
        }
    }

    // setters
    #[inline] pub fn inc_attempt(&self) { self.rebalance_attempts.inc(); }
    #[inline] pub fn inc_success(&self) { self.rebalance_success.inc(); }
    #[inline] pub fn inc_failure(&self) { self.rebalance_failure.inc(); }
    #[inline] pub fn observe_tip_ms(&self, ms: f64) { self.tip_latency_ms.observe(ms.max(0.0)); }
    #[inline] pub fn inc_resubmit(&self, reason: &'static str) { self.resubmits_by_reason.with_label_values(&[reason]).inc(); }
    #[inline] pub fn inc_refresh(&self, reason: &'static str) { self.refreshes_by_reason.with_label_values(&[reason]).inc(); }
    #[inline] pub fn inc_trip(&self) { self.breaker_trips.inc(); }
    #[inline] pub fn set_cu(&self, cu: i64) { self.cu_limit_gauge.set(cu); }
    #[inline] pub fn set_price(&self, p: i64) { self.cu_price_gauge.set(p); }
    #[inline] pub fn set_preprofit_bps(&self, bps: f64) { self.preprofit_bps_gauge.set(bps); }
    #[inline] pub fn set_height_margin(&self, m: i64) { self.height_margin_gauge.set(m); }
    #[inline] pub fn inc_escalations(&self) { self.escalations_total.inc(); }
    #[inline] pub fn inc_profit_skip(&self) { self.profit_skips.inc(); }
    #[inline] pub fn inc_sim_failure(&self) { self.sim_failures.inc(); }
    #[inline] pub fn inc_send_failure(&self) { self.send_failures.inc(); }
    #[inline] pub fn set_regime(&self, r: FeeRegime) { self.regime_gauge.set(match r { FeeRegime::Calm=>0, FeeRegime::Elevated=>1, FeeRegime::Surge=>2 }); }
    #[inline] pub fn set_zscore_abs(&self, z: f64) { self.zscore_gauge.set(z.abs()); }
    #[inline] pub fn observe_gas_est(&self, lamports: u64) { self.gas_est_hist.observe(lamports as f64); }
    #[inline] pub fn observe_fee_paid(&self, lamports: u64) { self.fee_paid_hist.observe(lamports as f64); }
    #[inline] pub fn inc_bhash_coalesce(&self) { self.coalesce_bhash_hits.inc(); }
    #[inline] pub fn inc_resolve_coalesce(&self) { self.coalesce_resolve_hits.inc(); }
    #[inline] pub fn inc_fork_retry(&self) { self.fork_retries.inc(); }
    #[inline] pub fn inc_bundle_timeout(&self) { self.bundle_timeouts.inc(); }
    #[inline] pub fn set_short_p90(&self, v: u64) { self.short_p90_gauge.set(v as i64); }
    #[inline] pub fn set_long_p90(&self, v: u64) { self.long_p90_gauge.set(v as i64); }
    #[inline] pub fn observe_sim_ms(&self, ms: f64) { self.sim_ms_hist.observe(ms.max(0.0)); }
    #[inline] pub fn inc_confirm_primary_win(&self) { self.confirm_primary_win.inc(); }
    #[inline] pub fn inc_confirm_fast_win(&self) { self.confirm_fast_win.inc(); }
    #[inline] pub fn inc_underpriced_escal(&self) { self.underpriced_escal.inc(); }
    #[inline] pub fn set_pacer_ms(&self, ms: u64) { self.pacer_ms_gauge.set(ms as i64); }
    #[inline] pub fn set_cu_override(&self, cu: u32) { self.cu_override_gauge.set(cu as i64); }
    #[inline] pub fn set_tip_lamports(&self, t: u64) { self.tip_lamports_gauge.set(t as i64); }
    #[inline] pub fn set_fee_slope_ps(&self, s: f64) { self.fee_slope_ps_gauge.set(s.max(0.0)); }
    #[inline] pub fn inc_bhash_stale_reuse(&self) { self.bhash_stale_reuse.inc(); }
    #[inline] pub fn inc_resolve_fallback(&self) { self.resolve_fallbacks.inc(); }

    // v50 extras
    #[inline] pub fn set_alpha(&self, a: f64) { self.alpha_gauge.set(a.max(0.0).min(1.0)); }
    #[inline] pub fn set_deadband_pct(&self, pct: i64) { self.deadband_pct_gauge.set(pct.max(0)); }
    #[inline] pub fn set_dt_ms(&self, ms: u64) { self.fee_dt_ms_gauge.set(ms as i64); }
    #[inline] pub fn set_p2_quantiles(&self, p75: u64, p90: u64) {
        self.p2_p75_gauge.set(p75 as i64);
        self.p2_p90_gauge.set(p90 as i64);
    }
}

#![forbid(unsafe_code)]
#![deny(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::todo,
    clippy::unimplemented,
    rust_2018_idioms
)]
#![allow(clippy::too_many_arguments, clippy::large_enum_variant)]

use std::{
    collections::{HashMap, HashSet, VecDeque},
    str::FromStr,
    sync::Arc,
    time::{Duration, Instant},
};

use anyhow::{anyhow, Result};
use once_cell::sync::Lazy;
use std::sync::OnceLock;
use tokio::sync::RwLock as TokioRwLock;
use parking_lot::{Mutex, RwLock as PLRwLock};
use prometheus::{Histogram, HistogramOpts, IntCounter, Registry};
use rand::{Rng, SeedableRng};
use rand_chacha::ChaCha20Rng;
use dashmap::DashMap;
use sha2::{Digest, Sha256};
use smallvec::SmallVec;
use tokio::{
    sync::{mpsc, oneshot, RwLock},
    task::JoinHandle,
};
use tracing::{error, info, warn};
use solana_client::rpc_filter::RpcFilterType;
use std::collections::VecDeque;
use std::sync::{atomic::{AtomicU64, Ordering}, OnceLock};

// === [IMPROVED: PIECE INF v24] === optional fast hash maps without API churn
#[cfg(feature = "fast-hash")]
type FastMap<K, V> = hashbrown::HashMap<K, V, ahash::RandomState>;
#[cfg(not(feature = "fast-hash"))]
type FastMap<K, V> = std::collections::HashMap<K, V>;
#[cfg(feature = "fast-hash")]
type FastSet<T> = hashbrown::HashSet<T, ahash::RandomState>;
#[cfg(not(feature = "fast-hash"))]
type FastSet<T> = std::collections::HashSet<T>;

use anchor_client::solana_sdk::{
    commitment_config::CommitmentConfig,
    compute_budget::ComputeBudgetInstruction,
    hash::Hash,
    instruction::Instruction,
    message::Message,
    pubkey::Pubkey,
    signature::Keypair,
    signer::Signer,
    transaction::Transaction,
    versioned_message::VersionedMessage,
    versioned_transaction::VersionedTransaction,
    v0::Message as V0Message,
};

use bytes::BytesMut;
use bincode;
use blake3;
use bytemuck;

use solana_client::{
    nonblocking::rpc_client::RpcClient,
    rpc_config::{RpcSendTransactionConfig, RpcSignatureStatusConfig, RpcSimulateTransactionConfig},
};
use solana_program::{instruction::AccountMeta, program_pack::Pack, system_instruction};
use solana_sdk::address_lookup_table_account::AddressLookupTableAccount;

use spl_associated_token_account as spl_ata;
use spl_token::state::Account as TokenAccount;

use tch::{no_grad, CModule, Device, Kind, Tensor};

// ---------- Program IDs (zero overhead, parsed once) ----------
// === tiny ALT prune cache (slot-scoped) ===
pub static ALT_PRUNE_CACHE: Lazy<RwLock<std::collections::HashMap<AltCacheKey, (VersionedTransaction, usize, usize, u128)>>> = Lazy::new(|| RwLock::new(std::collections::HashMap::new()));

/// +++ NEXT: coalesced time budget checks (check every 8th iteration)
#[inline]
fn time_over(deadline_ms: u128, tick: &mut u32) -> bool {
    *tick = tick.wrapping_add(1);
    if (*tick & 7) != 0 { return false; }

// === [IMPROVED: PIECE INF v29a] === tiny "last rates" cache (feature-free)
static LAST_RATES: OnceLock<TokioRwLock<FastMap<Pubkey, (u128, u128)>>> = OnceLock::new();

#[inline]
async fn rates_get(pk: &Pubkey) -> Option<(u128, u128)> {
    if let Some(cell) = LAST_RATES.get() {
        let g = cell.read().await;
        g.get(pk).copied()
    } else { None }
}

#[inline]
async fn rates_put(pk: &Pubkey, s_raw: u128, b_raw: u128) {
    let cell = LAST_RATES.get_or_init(|| TokioRwLock::new(FastMap::default()));
    let mut w = cell.write().await;
    if w.len() > 120_000 { w.clear(); }
    w.insert(*pk, (s_raw, b_raw));
}

// SPW-last helpers for fast APY retune without recomputing from raw when unchanged
static SPW_LAST: OnceLock<TokioRwLock<Option<f64>>> = OnceLock::new();

#[inline]
async fn spw_last_get() -> Option<f64> {
    let cell = SPW_LAST.get_or_init(|| TokioRwLock::new(None));
    let g = cell.read().await;
    *g
}

#[inline]
async fn spw_last_set(v: f64) {
    let cell = SPW_LAST.get_or_init(|| TokioRwLock::new(None));
    let mut w = cell.write().await;
    *w = Some(v);
}

#[inline]
fn apy_retune(apy_percent_old: f64, spw_old: f64, spw_now: f64) -> f64 {
    // old apy is in percent; convert to rate per slot using log1p, then expm1 to new apy
    let apy_frac_old = (apy_percent_old / 100.0).clamp(-0.499_999, 10.0);
    let r_per_slot = (1.0 + apy_frac_old).ln() / spw_old.max(1.0);
    let apy_new = f64::exp_m1(r_per_slot * spw_now).clamp(-0.5, 10.0);
    apy_new * 100.0
}

impl PortOptimizer {
    // === [IMPROVED: PIECE FETCH v38] === fetchers (hedged RPC already in get_program_accounts_smart)
    pub async fn fetch_solend_markets(&self) -> Result<Vec<LendingMarket>> {
        const SLICE_LEN: usize = 352;

        let prev_cnt = {
            let g = self.markets.read().await;
            g.values().flat_map(|v| v.iter()).filter(|lm| matches!(lm.protocol, Protocol::Solend)).count()
        };
        let accounts_raw = self.get_program_accounts_smart(&*SOLEND_PROGRAM_ID, SLICE_LEN, prev_cnt).await?;

        // pre-dedup by Pubkey, keep longest data
        let mut acc_map: FastMap<Pubkey, Vec<u8>> = FastMap::with_capacity(accounts_raw.len());
        for (pk, acc) in accounts_raw {
            let e = acc_map.entry(pk).or_insert_with(Vec::new);
            if acc.data.len() > e.len() { *e = acc.data; }
        }

        let spw = self.estimate_slots_per_year_cached().await;

        use std::sync::Arc;
        let prev_idx: Arc<FastMap<Pubkey, LendingMarket>> = {
            let mut need: FastSet<Pubkey> = FastSet::default();
            need.extend(acc_map.keys().copied());
            let g = self.markets.read().await;
            let mut idx: FastMap<Pubkey, LendingMarket> = FastMap::with_capacity(need.len());
            for (_k, list) in g.iter() {
                for lm in list {
                    if need.contains(&lm.market_pubkey) { idx.entry(lm.market_pubkey).or_insert_with(|| lm.clone()); }
                }
            }
            Arc::new(idx)
        };

        use futures::{stream, StreamExt};
        let cores = std::thread::available_parallelism().map(|n| n.get()).unwrap_or(8);
        let desired = ((acc_map.len().max(1) + 63) / 64).max(8).min(64);
        let conc = desired.min(cores * 2);

        let mut out: Vec<LendingMarket> = Vec::with_capacity(acc_map.len());
        let mut n = 0usize;
        let mut s = stream::iter(acc_map.into_iter()).map({
            let prev_idx = Arc::clone(&prev_idx);
            move |(pk, data)| {
                let prev_idx = Arc::clone(&prev_idx);
                let spw_now = spw;
                async move {
                    if data.len() < SOLEND_MIN_BYTES { return None; }
                    let ver = data[0];
                    if ver == 0 || ver > 5 { return None; }

                    if let Some(fp) = fp64_solend(&data) {
                        if let Some(oldfp) = fp_get(&pk).await {
                            if oldfp == fp {
                                if let Some(mut prev) = prev_idx.get(&pk).cloned() {
                                    prev.supply_apy = prev.supply_apy.clamp(-50.0, 1_000.0);
                                    prev.borrow_apy  = prev.borrow_apy.clamp(-50.0, 1_000.0);
                                    return Some(prev);
                                }
                            }
                        }
                        let parsed = self.parse_solend_reserve_with_spw(&pk, &data, spw_now).await.ok()
                            .map(|mut m| { m.supply_apy = m.supply_apy.clamp(-50.0, 1_000.0); m.borrow_apy = m.borrow_apy.clamp(-50.0, 1_000.0); m });
                        if parsed.is_some() { fp_put(&pk, fp).await; }
                        parsed
                    } else { None }
                }
            }
        }).buffer_unordered(conc);

        use tokio::task;
        while let Some(maybe) = s.next().await {
            if let Some(m) = maybe { out.push(m); }
            n += 1;
            if (n & 0x3FFF) == 0 { task::yield_now().await; }
        }

        out.sort_unstable_by_key(|m| (m.token_mint, m.protocol, std::cmp::Reverse(apy_key_i64_pub(m.supply_apy)), m.market_pubkey));
        Ok(out)
    }

    pub async fn fetch_kamino_markets(&self) -> Result<Vec<LendingMarket>> {
        const SLICE_LEN: usize = 352;

        let prev_cnt = {
            let g = self.markets.read().await;
            g.values().flat_map(|v| v.iter()).filter(|lm| matches!(lm.protocol, Protocol::Kamino)).count()
        };
        let accounts_raw = self.get_program_accounts_smart(&*KAMINO_PROGRAM_ID, SLICE_LEN, prev_cnt).await?;

        let mut acc_map: FastMap<Pubkey, Vec<u8>> = FastMap::with_capacity(accounts_raw.len());
        for (pk, acc) in accounts_raw {
            let e = acc_map.entry(pk).or_insert_with(Vec::new);
            if acc.data.len() > e.len() { *e = acc.data; }
        }

        let spw = self.estimate_slots_per_year_cached().await;

        use std::sync::Arc;
        let prev_idx: Arc<FastMap<Pubkey, LendingMarket>> = {
            let mut need: FastSet<Pubkey> = FastSet::default();
            need.extend(acc_map.keys().copied());
            let g = self.markets.read().await;
            let mut idx: FastMap<Pubkey, LendingMarket> = FastMap::with_capacity(need.len());
            for (_k, list) in g.iter() {
                for lm in list {
                    if need.contains(&lm.market_pubkey) { idx.entry(lm.market_pubkey).or_insert_with(|| lm.clone()); }
                }
            }
            Arc::new(idx)
        };

        use futures::{stream, StreamExt};
        let cores = std::thread::available_parallelism().map(|n| n.get()).unwrap_or(8);
        let desired = ((acc_map.len().max(1) + 63) / 64).max(8).min(64);
        let conc = desired.min(cores * 2);

        let mut out: Vec<LendingMarket> = Vec::with_capacity(acc_map.len());
        let mut n = 0usize;
        let mut s = stream::iter(acc_map.into_iter()).map({
            let prev_idx = Arc::clone(&prev_idx);
            move |(pk, data)| {
                let prev_idx = Arc::clone(&prev_idx);
                let spw_now = spw;
                async move {
                    if data.len() < KAMINO_MIN_BYTES { return None; }
                    let ver = data[0];
                    if ver == 0 || ver > 10 { return None; }

                    if let Some(fp) = fp64_kamino(&data) {
                        if let Some(oldfp) = fp_get(&pk).await {
                            if oldfp == fp {
                                if let Some(mut prev) = prev_idx.get(&pk).cloned() {
                                    prev.supply_apy = prev.supply_apy.clamp(-50.0, 1_000.0);
                                    prev.borrow_apy  = prev.borrow_apy.clamp(-50.0, 1_000.0);
                                    return Some(prev);
                                }
                            }
                        }
                        let parsed = self.parse_kamino_market_with_spw(&pk, &data, spw_now).await.ok()
                            .map(|mut m| { m.supply_apy = m.supply_apy.clamp(-50.0, 1_000.0); m.borrow_apy = m.borrow_apy.clamp(-50.0, 1_000.0); m });
                        if parsed.is_some() { fp_put(&pk, fp).await; }
                        parsed
                    } else { None }
                }
            }
        }).buffer_unordered(conc);

        use tokio::task;
        while let Some(maybe) = s.next().await {
            if let Some(m) = maybe { out.push(m); }
            n += 1;
            if (n & 0x3FFF) == 0 { task::yield_now().await; }
        }

        out.sort_unstable_by_key(|m| (m.token_mint, m.protocol, std::cmp::Reverse(apy_key_i64_pub(m.supply_apy)), m.market_pubkey));
        Ok(out)
    }
}

// Fingerprint helpers and cache for FETCH v38
static FP_CACHE: OnceLock<TokioRwLock<FastMap<Pubkey, u64>>> = OnceLock::new();

#[inline]
fn fp64_solend(data: &[u8]) -> Option<u64> { Some(u64::from_le_bytes(blake3::hash(data).as_bytes()[..8].try_into().ok()?)) }
#[inline]
fn fp64_kamino(data: &[u8]) -> Option<u64> { Some(u64::from_le_bytes(blake3::hash(data).as_bytes()[..8].try_into().ok()?)) }

#[inline]
async fn fp_get(pk: &Pubkey) -> Option<u64> {
    if let Some(c) = FP_CACHE.get() { let g = c.read().await; g.get(pk).copied() } else { None }
}
#[inline]
async fn fp_put(pk: &Pubkey, fp: u64) {
    let cell = FP_CACHE.get_or_init(|| TokioRwLock::new(FastMap::default()));
    let mut w = cell.write().await;
    if w.len() > 200_000 { w.clear(); }
    w.insert(*pk, fp);
}

// Public APY key helper for sort key
#[inline]
fn apy_key_i64_pub(apy_pct: f64) -> i64 {
    let q = (apy_pct * 1e6).round();
    if q.is_finite() { q as i64 } else { 0 }
}

    // === [IMPROVED: PIECE PARSE v29] === fast parsers avoid APY math when rates unchanged
    pub async fn parse_solend_reserve_fast(
        &self,
        pk: &Pubkey,
        data: &[u8],
        spw_now: f64,
        prev_opt: Option<&LendingMarket>,
    ) -> Result<LendingMarket> {
        if data.len() < SOLEND_MIN_BYTES { anyhow::bail!("solend: too small: {}", data.len()); }
        let version = data[0];
        if !(1..=5).contains(&version) { anyhow::bail!("solend: bad version {version}"); }

        let token_mint          = self.pk_at(data, SOLEND_OFF_MINT)?;
        let available_liquidity = Self::le_u64(data, SOLEND_OFF_AVAIL)?;
        let borrowed_amount     = Self::le_u64(data, SOLEND_OFF_BORROW)?;
        let s_raw               = Self::le_u128(data, SOLEND_OFF_SPLYRT)?;
        let b_raw               = Self::le_u128(data, SOLEND_OFF_BORRT)?;

        let total_liq  = available_liquidity.saturating_add(borrowed_amount);
        let utilization= if total_liq > 0 { borrowed_amount as f64 / total_liq as f64 } else { 0.0 };

        let (supply_apy, borrow_apy) = if let Some((s0, b0)) = rates_get(pk).await {
            if s0 == s_raw && b0 == b_raw {
                if let Some(prev) = prev_opt {
                    if let Some(spw_old) = spw_last_get().await {
                        if (spw_now - spw_old).abs() / spw_old > 0.005 {
                            (apy_retune(prev.supply_apy, spw_old, spw_now), apy_retune(prev.borrow_apy, spw_old, spw_now))
                        } else { (prev.supply_apy, prev.borrow_apy) }
                    } else { (prev.supply_apy, prev.borrow_apy) }
                } else {
                    ( self.apy_from_rate_per_slot_with_spw(s_raw, spw_now)?,
                      self.apy_from_rate_per_slot_with_spw(b_raw, spw_now)? )
                }
            } else {
                ( self.apy_from_rate_per_slot_with_spw(s_raw, spw_now)?,
                  self.apy_from_rate_per_slot_with_spw(b_raw, spw_now)? )
            }
        } else {
            ( self.apy_from_rate_per_slot_with_spw(s_raw, spw_now)?,
              self.apy_from_rate_per_slot_with_spw(b_raw, spw_now)? )
        };

        rates_put(pk, s_raw, b_raw).await;

        Ok(LendingMarket {
            market_pubkey: *pk,
            token_mint,
            protocol: Protocol::Solend,
            supply_apy,
            borrow_apy,
            utilization,
            liquidity_available: available_liquidity,
            last_update: Instant::now(),
            layout_version: version,
        })
    }

    pub async fn parse_kamino_market_fast(
        &self,
        pk: &Pubkey,
        data: &[u8],
        spw_now: f64,
        prev_opt: Option<&LendingMarket>,
    ) -> Result<LendingMarket> {
        if data.len() < KAMINO_MIN_BYTES { anyhow::bail!("kamino: too small: {}", data.len()); }
        let version = data[0];
        if !(1..=10).contains(&version) { anyhow::bail!("kamino: bad version {version}"); }

        let token_mint          = self.pk_at(data, KAMINO_OFF_MINT)?;
        let available_liquidity = Self::le_u64(data, KAMINO_OFF_AVAIL)?;
        let borrowed_amount     = Self::le_u64(data, KAMINO_OFF_BORROW)?;
        let s_raw               = Self::le_u128(data, KAMINO_OFF_SPLYRT)?;
        let b_raw               = Self::le_u128(data, KAMINO_OFF_BORRT)?;

        let total_liq  = available_liquidity.saturating_add(borrowed_amount);
        let utilization= if total_liq > 0 { borrowed_amount as f64 / total_liq as f64 } else { 0.0 };

        let (supply_apy, borrow_apy) = if let Some((s0, b0)) = rates_get(pk).await {
            if s0 == s_raw && b0 == b_raw {
                if let Some(prev) = prev_opt {
                    if let Some(spw_old) = spw_last_get().await {
                        if (spw_now - spw_old).abs() / spw_old > 0.005 {
                            (apy_retune(prev.supply_apy, spw_old, spw_now), apy_retune(prev.borrow_apy, spw_old, spw_now))
                        } else { (prev.supply_apy, prev.borrow_apy) }
                    } else { (prev.supply_apy, prev.borrow_apy) }
                } else {
                    ( self.apy_from_rate_per_slot_with_spw(s_raw, spw_now)?,
                      self.apy_from_rate_per_slot_with_spw(b_raw, spw_now)? )
                }
            } else {
                ( self.apy_from_rate_per_slot_with_spw(s_raw, spw_now)?,
                  self.apy_from_rate_per_slot_with_spw(b_raw, spw_now)? )
            }
        } else {
            ( self.apy_from_rate_per_slot_with_spw(s_raw, spw_now)?,
              self.apy_from_rate_per_slot_with_spw(b_raw, spw_now)? )
        };

        rates_put(pk, s_raw, b_raw).await;

        Ok(LendingMarket {
            market_pubkey: *pk,
            token_mint,
            protocol: Protocol::Kamino,
            supply_apy,
            borrow_apy,
            utilization,
            liquidity_available: available_liquidity,
            last_update: Instant::now(),
            layout_version: version,
        })
    }
}
    now_ms() > deadline_ms
}

#[derive(Clone, Copy, Default, Debug)]
struct TipStat { succ_ew: f64, lat_ew_us: f64 }

#[inline]
fn alt_cache_key(
    payer: &Pubkey,
    bh: &Hash,
    ixs_salt: u64,
    alts: &[AddressLookupTableAccount],
) -> u64 {
    let mut h = blake3::Hasher::new();
    h.update(payer.as_ref());
    h.update(bh.as_ref());
    h.update(&ixs_salt.to_le_bytes());
    for alt in alts.iter() {
        let n = alt.addresses.len() as u32;
        h.update(&n.to_le_bytes());
        if let Some(first) = alt.addresses.first() { h.update(first.as_ref()); }
        if let Some(last)  = alt.addresses.last()  { h.update(last.as_ref());  }
    }
    let bytes = h.finalize();
    u64::from_le_bytes(bytes.as_bytes()[..8].try_into().unwrap())
}

impl PortOptimizer {
    // Quantization helpers for compute budget shaping
    #[inline]
    fn quantize_units_1024(&self, units: u32) -> u32 {
        ((units + 1023) / 1024) * 1024
    }
    #[inline]
    fn quantize_cu_price_64(&self, price: u64) -> u64 {
        ((price + 63) / 64) * 64
    }
    // === ALT need-set fingerprint cache (Piece 93B v15) ===
    #[inline]
    fn needset_fp(&self, ixs: &[Instruction]) -> u64 {
        let mut h = blake3::Hasher::new();
        let payer = self.wallet.pubkey();
        for ix in ixs {
            if ix.program_id == solana_sdk::compute_budget::id() || ix.program_id == solana_program::system_program::id() { continue; }
            for am in &ix.accounts {
                if am.pubkey == payer { continue; }
                h.update(am.pubkey.as_ref());
            }
        }
        u64::from_le_bytes(h.finalize().as_bytes()[..8].try_into().unwrap())
    }

    #[inline]
    fn alt_fingerprint(&self, alt: &AddressLookupTableAccount) -> u64 {
        let mut h = blake3::Hasher::new();
        h.update(alt.key.as_ref());
        let n = alt.addresses.len() as u32;
        h.update(&n.to_le_bytes());
        if let Some(first) = alt.addresses.first() { h.update(first.as_ref()); }
        if let Some(last)  = alt.addresses.last()  { h.update(last.as_ref()); }
        u64::from_le_bytes(h.finalize().as_bytes()[..8].try_into().unwrap())
    }

    #[inline]
    fn alt_need_get(&self, flow_salt: u64, need_fp: u64, alts_pool: &[AddressLookupTableAccount]) -> Option<Vec<AddressLookupTableAccount>> {
        let m = self.alt_plan_by_need.read();
        let fps = m.get(&(flow_salt, need_fp))?.clone();
        if fps.is_empty() { return None; }
        let mut out = Vec::with_capacity(fps.len());
        for fp in fps {
            if let Some(a) = alts_pool.iter().find(|x| self.alt_fingerprint(x) == fp) { out.push(a.clone()); }
        }
        if out.is_empty() { None } else { Some(out) }
    }

    #[inline]
    fn alt_need_put(&self, flow_salt: u64, need_fp: u64, chosen: &[AddressLookupTableAccount]) {
        use smallvec::SmallVec;
        const CAP: usize = 1024;
        let mut m = self.alt_plan_by_need.write();
        if m.len() >= CAP { if let Some(k) = m.keys().next().cloned() { m.remove(&k); } }
        let mut fps: SmallVec<[u64;6]> = SmallVec::new();
        for a in chosen { fps.push(self.alt_fingerprint(a)); }
        m.insert((flow_salt, need_fp), fps);
    }

    // === Inverted ALT address index (Piece 96B v15) ===
    fn index_alts(&self, alts: &[AddressLookupTableAccount]) {
        use smallvec::SmallVec;
        for a in alts {
            let alt_pk = a.key;
            for k in &a.addresses {
                self.alt_addr_index
                    .entry(*k)
                    .or_insert_with(|| SmallVec::new())
                    .push(alt_pk);
            }
        }
    }

    fn needed_keyset(&self, ixs: &[Instruction]) -> std::collections::HashSet<Pubkey> {
        use std::collections::HashSet;
        let payer = self.wallet.pubkey();
        let mut set: HashSet<Pubkey> = HashSet::new();
        for ix in ixs {
            if ix.program_id == solana_sdk::compute_budget::id() { continue; }
            for am in &ix.accounts {
                if am.pubkey != payer && am.pubkey != solana_program::system_program::id() { set.insert(am.pubkey); }
            }
        }
        set
    }

    fn prefilter_alts_via_index(&self, ixs: &[Instruction], alts_all: &[AddressLookupTableAccount]) -> Vec<AddressLookupTableAccount> {
        use std::collections::HashSet;
        let need = self.needed_keyset(ixs);
        let mut hit_alts: HashSet<Pubkey> = HashSet::new();
        for k in &need {
            if let Some(list) = self.alt_addr_index.get(k) { for p in list.iter() { hit_alts.insert(*p); } }
        }
        let mut out: Vec<_> = alts_all.iter().cloned().filter(|a| hit_alts.contains(&a.key)).collect();
        out.sort_by_key(|a| a.addresses.len());
        out
    }

    // === Tip router bandit scoring (Piece 95B v15) ===
    fn tip_pick_bandit(&self, candidates: &[Pubkey], salt: u64) -> Option<Pubkey> {
        if candidates.is_empty() { return None; }
        let mut best: Option<(f64, Pubkey)> = None;
        for &p in candidates {
            let s = self.tip_stats.get(&p).map(|v| *v).unwrap_or_default();
            let succ = if s.succ_ew > 0.0 { s.succ_ew } else { 0.5 };
            let lat  = if s.lat_ew_us > 0.0 { s.lat_ew_us } else { 600.0 };
            let score = succ / lat.max(1.0);
            let mut b = [0u8;8]; b.copy_from_slice(&p.to_bytes()[..8]);
            let jitter = ((salt ^ u64::from_le_bytes(b)) & 0xFF) as f64 / 1024.0;
            let scr = score * (1.0 + jitter);
            if best.map_or(true, |(bv, _)| scr > bv) { best = Some((scr, p)); }
        }
        best.map(|(_,p)| p)
    }

    fn note_tip_result(&self, dest: Pubkey, ok: bool, latency_us: u64) {
        let mut new = self.tip_stats.get(&dest).map(|v| *v).unwrap_or_default();
        let (a_s, a_l) = (0.30, 0.20);
        new.succ_ew = (1.0-a_s)*new.succ_ew + a_s*if ok {1.0} else {0.0};
        new.lat_ew_us = (1.0-a_l)*new.lat_ew_us + a_l*(latency_us as f64);
        self.tip_stats.insert(dest, new);
    }

    // === Opcode 3 helper (Piece 97 v15) ===
    #[inline]
    fn cb_accdata_ix(&self, bytes_exact: u32) -> Instruction {
        let step = 32 * 1024u32;
        let want = ((bytes_exact + step - 1) / step) * step;
        let mut data = Vec::with_capacity(5);
        data.push(3u8);
        data.extend_from_slice(&want.min(512*1024).to_le_bytes());
        Instruction { program_id: solana_sdk::compute_budget::id(), accounts: vec![], data }
    }

    #[inline]
    fn jittered_fee(&self, cu_price: u64, payer: &Pubkey, bh: &Hash) -> u64 {
        let mut h = blake3::Hasher::new();
        h.update(payer.as_ref()); h.update(bh.as_ref());
        let bytes = h.finalize();
        let seed = u64::from_le_bytes(bytes.as_bytes()[..8].try_into().unwrap());
        let jitter = ((seed % 7) as i64 - 3) * 25; // -75..+75
        cu_price.saturating_add_signed(jitter)
    }

    #[inline]
    fn alts_plan_for_core(
        &self,
        core_ixs: &[Instruction],
        payer: Pubkey,
        bh: Hash,
        alts_all: &[AddressLookupTableAccount],
        tip_acc_opt: Option<Pubkey>,
    ) -> SmallVec<[AddressLookupTableAccount; 16]> {
        let mut core_plus: SmallVec<[Instruction; 16]> = SmallVec::new();
        core_plus.extend_from_slice(core_ixs);
        if let Some(tip_acc) = tip_acc_opt {
            core_plus.push(system_instruction::transfer(&payer, &tip_acc, 1));
        }
        let v = self.greedy_min_alts(&core_plus, alts_all);
        let mut out: SmallVec<[AddressLookupTableAccount; 16]> = SmallVec::new();
        out.extend(v.into_iter());
        out
    }

    #[inline]
    fn parse_num_after(&self, s: &str, pat: &str) -> Option<u32> {
        if let Some(idx) = s.find(pat) {
            let mut j = idx + pat.len();
            let bytes = s.as_bytes();
            while j < bytes.len() && bytes[j].is_ascii_whitespace() { j += 1; }
            let mut val: u32 = 0;
            let mut hit = false;
            while j < bytes.len() {
                let b = bytes[j];
                if b.is_ascii_digit() { val = val.saturating_mul(10).saturating_add((b - b'0') as u32); j += 1; hit = true; }
                else if b == b',' || b == b'_' { j += 1; }
                else { break; }
            }
            if hit && val > 0 { return Some(val); }
        }
        None
    }

    #[inline]
    fn parse_num_after_any(&self, s: &str, pats: &[&str]) -> Option<u32> {
        for p in pats { if let Some(v) = self.parse_num_after(s, p) { return Some(v); } }
        None
    }

    #[inline]
    fn strip_ansi<'a>(&self, s: &'a str) -> std::borrow::Cow<'a, str> {
        let bytes = s.as_bytes();
        if !bytes.iter().any(|&c| c == 0x1b) { return std::borrow::Cow::Borrowed(s); }
        let mut out = String::with_capacity(s.len());
        let mut i = 0usize;
        while i < bytes.len() {
            if bytes[i] == 0x1b {
                i += 1;
                if i < bytes.len() && bytes[i] == b'[' {
                    i += 1; while i < bytes.len() && bytes[i] != b'm' { i += 1; }
                    if i < bytes.len() { i += 1; }
                    continue;
                }
            }
            out.push(bytes[i] as char); i += 1;
        }
        std::borrow::Cow::Owned(out)
    }

    /// +++ NEXT: ascii-folded contains (no alloc, safe)
    #[inline]
    fn contains_ci(&self, s: &str, pat: &str) -> bool {
        let sb = s.as_bytes();
        let pb = pat.as_bytes();
        if pb.is_empty() { return true; }
        if sb.len() < pb.len() { return false; }
        #[inline]
        fn fold(x: u8) -> u8 { if (b'A'..=b'Z').contains(&x) { x | 0x20 } else { x } }
        let first = fold(pb[0]);
        let mut i = 0usize;
        while i + pb.len() <= sb.len() {
            if fold(sb[i]) != first { i += 1; continue; }
            let mut ok = true; let mut j = 1usize;
            while j < pb.len() {
                if fold(sb[i + j]) != fold(pb[j]) { ok = false; break; }
                j += 1;
            }
            if ok { return true; }
            i += 1;
        }
        false
    }

    #[inline]
    fn parse_last_num_if_hinted(&self, s: &str) -> Option<u32> {
        let s = self.strip_ansi(s);
        let sv = s.as_ref();
        if !(self.contains_ci(sv, "unit") || self.contains_ci(sv, "compute") || self.contains_ci(sv, "cu")) { return None; }
        let b = sv.as_bytes();
        let mut i = 0usize; let mut last: Option<u32> = None;
        while i < b.len() {
            while i < b.len() && !b[i].is_ascii_digit() { i += 1; }
            if i >= b.len() { break; }
            let mut v: u32 = 0; let mut hit = false;
            while i < b.len() {
                let c = b[i];
                if c.is_ascii_digit() { v = v.saturating_mul(10) + (c - b'0') as u32; hit = true; i += 1; }
                else if c == b',' || c == b'_' { i += 1; } else { break; }
            }
            if hit { last = Some(v); }
        }
        last
    }

    #[inline]
    fn filter_units(&self, v: u32) -> Option<u32> {
        if v == 0 { return None; }
        if v > MAX_COMPUTE_UNITS.saturating_mul(4) { return None; }
        Some(v)
    }
}
pub static SOLEND_PROGRAM_ID: Lazy<Pubkey> = Lazy::new(|| {
    Pubkey::from_str("So1endDq2YkqhipRh3WViPa8hdiSpxWy6z3Z6tMCpAo").expect("static SOLEND id")
});
pub static KAMINO_PROGRAM_ID: Lazy<Pubkey> = Lazy::new(|| {
    Pubkey::from_str("KLend2g3cP87fffoy8q1mQqGKjrxjC8boSyAYavgmjD").expect("static KAMINO id")
});

// ---------- Strong types to avoid unit bugs ----------
#[derive(Copy, Clone, Debug)]
pub struct Lamports(pub u64);
#[derive(Copy, Clone, Debug)]
pub struct Bps(pub u16);

#[derive(Clone)]
pub struct TipConfig {
    pub accounts: Vec<Pubkey>,
    pub strategy: TipStrategy,
    pub min_balance: u64,
}

// Tiny either for candidate chooser
enum Either<L, R> { Left(L), Right(R) }

// Per-flow learned compute budget EMA hint (units and optional heap).
#[derive(Clone, Debug, Default)]
pub struct FlowBudgetEma {
    pub units_ema: f64,
    pub heap_hint: Option<u32>,
}

// ----- Zero-GC warmed TorchScript batcher with micro-batching -----
// ----- Torch micro-batcher v2 (deadline gather + zero-copy slab) -----
// Reusable zero-copy slab
struct BufPool {
    cap: usize,
    pool: Mutex<Vec<BytesMut>>,
}
impl BufPool {
    fn new(capacity: usize, pool_size: usize) -> Self {
        let mut v = Vec::with_capacity(pool_size);
        for _ in 0..pool_size {
            let mut b = BytesMut::with_capacity(capacity);
            b.resize(capacity, 0);
            v.push(b);
        }
        Self { cap: capacity, pool: Mutex::new(v) }
    }
    fn get(&self, need: usize) -> BytesMut {
        let mut g = self.pool.lock();
        if let Some(mut b) = g.pop() {
            if b.capacity() < need { b.reserve(need - b.capacity()); }
            b.resize(need, 0);
            b
        } else {
            let mut b = BytesMut::with_capacity(need.max(self.cap));
            b.resize(need, 0);
            b
        }
    }
    fn put(&self, mut b: BytesMut) { b.truncate(self.cap); self.pool.lock().push(b); }
}

#[derive(Clone)]
pub struct InferenceItem {
    pub apy_spread_bps: Arc<[f32]>,
    pub utilization: Arc<[f32]>,
    pub fee_lamports: Arc<[f32]>,
    pub liq_ratio: Arc<[f32]>,
    pub resp: oneshot::Sender<Result<f32>>, // returns probability [0,1]
}

pub struct TorchBatcher {
    module: CModule,
    device: Device,
    max_batch: usize,
    max_wait: Duration,
    tx: mpsc::Sender<InferenceItem>,
    _worker: JoinHandle<()>,
}

impl TorchBatcher {
    pub fn load(path: &str, max_batch: usize, max_wait: Duration, prefer_gpu: bool) -> Result<Self> {
        let device = if prefer_gpu { Device::Cuda(0) } else { Device::Cpu };
        let mut module = CModule::load_on_device(path, device)?;
        module.set_eval();

        // Warm kernels
        no_grad(|| {
            let x = Tensor::zeros([1, 32, 4], (Kind::Float, device));
            let _ = module.forward_ts(&[x]);
        });

        let (tx, mut rx) = mpsc::channel::<InferenceItem>(2048);
        let module_arc = module.clone();
        let pool = Arc::new(BufPool::new(64 * 64 * 4 * std::mem::size_of::<f32>(), 32));
        let worker = tokio::spawn({
            let pool = pool.clone();
            async move {
                let module = module_arc;
                loop {
                    let Some(first) = rx.recv().await else { break; };
                    let mut batch = Vec::with_capacity(max_batch);
                    batch.push(first);

                    // deadline gather
                    let deadline = tokio::time::Instant::now() + max_wait;
                    while batch.len() < max_batch {
                        let remain = deadline.saturating_duration_since(tokio::time::Instant::now());
                        if remain.is_zero() { break; }
                        match tokio::time::timeout(remain, rx.recv()).await {
                            Ok(Some(item)) => batch.push(item),
                            _ => break,
                        }
                    }

                    // Find T
                    let t = batch.iter().fold(usize::MAX, |acc, b| acc
                        .min(b.apy_spread_bps.len())
                        .min(b.utilization.len())
                        .min(b.fee_lamports.len())
                        .min(b.liq_ratio.len())).max(8);
                    let bsz = batch.len();
                    let need_f32 = bsz * t * 4;
                    let mut buf = pool.get(need_f32 * std::mem::size_of::<f32>());
                    {
                        let mut idx = 0usize;
                        let slice: &mut [f32] = bytemuck::cast_mut(&mut *buf);
                        for it in &batch {
                            let n = it.apy_spread_bps.len();
                            let start = n.saturating_sub(t);
                            for i in start..n {
                                slice[idx] = it.apy_spread_bps[i]; idx += 1;
                                slice[idx] = it.utilization[i];     idx += 1;
                                slice[idx] = it.fee_lamports[i];    idx += 1;
                                slice[idx] = it.liq_ratio[i];       idx += 1;
                            }
                        }
                    }

                    // Inference
                    let x = Tensor::from_data_size(
                        buf.as_ptr() as *const f32,
                        &[bsz as i64, t as i64, 4],
                        (Kind::Float, device),
                    );
                    let y = no_grad(|| module.forward_ts(&[x]));

                    match y {
                        Ok(out) => {
                            let out = out.squeeze();
                            let probs: Vec<f32> = if out.dim() == 0 {
                                vec![f32::from(out.to_kind(Kind::Float)); bsz]
                            } else if out.size().len() == 2 && out.size()[1] == 1 {
                                (0..bsz).map(|i| out.double_value(&[i as i64, 0]) as f32).collect()
                            } else {
                                (0..bsz).map(|i| out.double_value(&[i as i64]) as f32).collect()
                            };
                            for (i, it) in batch.into_iter().enumerate() {
                                let _ = it.resp.send(Ok(probs.get(i).copied().unwrap_or(0.0).clamp(0.0, 1.0)));
                            }
                        }
                        Err(e) => {
                            for it in batch.into_iter() { let _ = it.resp.send(Err(anyhow!(format!("inference failed: {e}")))); }
                        }
                    }
                    pool.put(buf);
                }
            }
        });

        Ok(Self { module, device, max_batch, max_wait, tx, _worker: worker })
    }

    pub async fn infer(
        &self,
        apy_spread_bps: Arc<[f32]>,
        utilization: Arc<[f32]>,
        fee_lamports: Arc<[f32]>,
        liq_ratio: Arc<[f32]>,
    ) -> Result<f32> {
        let (resp_tx, resp_rx) = oneshot::channel();
        self.tx.send(InferenceItem { apy_spread_bps, utilization, fee_lamports, liq_ratio, resp: resp_tx })
            .await.map_err(|_| anyhow!("batcher closed"))?;
        resp_rx.await.map_err(|_| anyhow!("inference dropped"))?
    }
}

// ----- TorchScript Trigger Model (inference-only) -----
pub struct TriggerModel {
    module: CModule,
    device: Device,
}

impl TriggerModel {
    pub fn load(path: &str) -> anyhow::Result<Self> {
        let device = Device::Cpu;
        let module = CModule::load_on_device(path, device)?;
        Ok(Self { module, device })
    }

    // Inputs are rolling sequences; output is probability [0,1]
    pub fn infer(
        &self,
        apy_spread_bps_seq: &[f32],
        utilization_seq: &[f32],
        fee_lamports_seq: &[f32],
        liq_ratio_seq: &[f32],
    ) -> anyhow::Result<f32> {
        let t = apy_spread_bps_seq
            .len()
            .min(utilization_seq.len())
            .min(fee_lamports_seq.len())
            .min(liq_ratio_seq.len());
        let t = t.max(8);
        let a_len = apy_spread_bps_seq.len();
        let u_len = utilization_seq.len();
        let f_len = fee_lamports_seq.len();
        let l_len = liq_ratio_seq.len();
        let start = a_len.min(u_len).min(f_len).min(l_len).saturating_sub(t);
        let mut buf = Vec::with_capacity(t * 4);
        for i in start..start + t {
            buf.push(apy_spread_bps_seq[a_len - t + (i - start)]);
            buf.push(utilization_seq[u_len - t + (i - start)]);
            buf.push(fee_lamports_seq[f_len - t + (i - start)]);
            buf.push(liq_ratio_seq[l_len - t + (i - start)]);
        }
        let x = Tensor::of_slice(&buf)
            .reshape([1, t as i64, 4])
            .to_device(self.device);
        let y = self.module.forward_ts(&[x])?;
        let p: f32 = f32::from(y.squeeze());
        Ok(p.clamp(0.0, 1.0))
    }
}

#[derive(Clone)]
pub enum TipStrategy { RoundRobin, Fixed(usize) }

#[derive(Clone, Debug)]
pub struct TipStats {
    pub account: Pubkey,
    pub ema: f64,         // latency level (ms)
    pub emv: f64,         // latency abs-dev
    pub ghost_rate: f64,  // failures/attempts
    pub weight: f64,
    pub attempts: u64,
    pub failures: u64,
    pub last_update: Instant,
}

#[derive(Clone)]
pub struct TipRouter {
    inner: Arc<RwLock<Vec<TipStats>>>,
    alpha: f64,      // level smoothing
    beta: f64,       // volatility smoothing
    floor_ms: f64,
    ceil_ms: f64,
    min_weight: f64,
    half_life: Duration,   // decay for old stats
    hampel_k: f64,         // outlier clamp factor
}

impl TipRouter {
    pub fn new(accounts: Vec<Pubkey>) -> Self {
        let now = Instant::now();
        let v = accounts.into_iter().map(|a| TipStats {
            account: a, ema: 800.0, emv: 100.0, ghost_rate: 0.0, weight: 1.0,
            attempts: 1, failures: 0, last_update: now
        }).collect();
        let me = Self {
            inner: Arc::new(RwLock::new(v)),
            alpha: 0.22, beta: 0.12,
            floor_ms: 80.0, ceil_ms: 3500.0,
            min_weight: 0.01,
            half_life: Duration::from_secs(120),
            hampel_k: 3.0,
        };
        me.reweight_now();
        me
    }

    fn reweight_now(&self) {
        let mut g = self.inner.blocking_write();
        Self::recompute_weights_locked(&mut g, self.min_weight);
    }

    pub async fn record_attempt(&self, acct: Pubkey, success: bool, latency_ms: Option<f64>) {
        let mut tips = self.inner.write().await;
        // Hampel clamp reference
        let med = median(tips.iter().map(|t| t.ema).collect());
        let mad = median(tips.iter().map(|t| (t.ema - med).abs()).collect()).max(1.0);

        if let Some(t) = tips.iter_mut().find(|t| t.account == acct) {
            // time decay for attempts/failures
            let dt = t.last_update.elapsed();
            let decay = (0.5_f64).powf(dt.as_secs_f64() / self.half_life.as_secs_f64());
            t.attempts = ((t.attempts as f64) * decay).max(1.0) as u64;
            t.failures = ((t.failures as f64) * decay).max(0.0) as u64;
            t.last_update = Instant::now();

            t.attempts = t.attempts.saturating_add(1);
            if !success { t.failures = t.failures.saturating_add(1); }
            t.ghost_rate = (t.failures as f64) / (t.attempts as f64);

            if let Some(ms) = latency_ms {
                let ms = ms.clamp(self.floor_ms, self.ceil_ms);
                let clamp_hi = med + self.hampel_k * mad;
                let ms = ms.min(clamp_hi);
                t.ema = self.alpha * ms + (1.0 - self.alpha) * t.ema;
                t.emv = self.beta * (ms - t.ema).abs() + (1.0 - self.beta) * t.emv;
            }
        }
        Self::recompute_weights_locked(&mut tips, self.min_weight);
    }

    fn recompute_weights_locked(tips: &mut [TipStats], min_weight: f64) {
        // Weight ∝ (1 - ghost)^2 / (ema + 2*emv)
        let k = 2.0;
        let mut sum = 0.0;
        for t in tips.iter_mut() {
            let reliab = (1.0 - t.ghost_rate).max(0.0).powi(2);
            t.weight = (reliab / (t.ema + k * t.emv).max(1.0)).max(min_weight);
            sum += t.weight;
        }
        if sum > 0.0 { for t in tips.iter_mut() { t.weight /= sum; } }
    }

    pub async fn pick(&self) -> Pubkey {
        use rand_chacha::ChaCha20Rng;
        use rand::SeedableRng;
        let mut rng = ChaCha20Rng::from_entropy();
        let tips = self.inner.read().await;
        if tips.is_empty() { return Pubkey::new_unique(); }
        let r: f64 = rng.gen();
        let mut acc = 0.0;
        for t in tips.iter() { acc += t.weight; if r <= acc { return t.account; } }
        tips.last().map(|t| t.account).unwrap_or_else(Pubkey::new_unique)
    }

    pub fn pick_sync(&self) -> Pubkey {
        use rand_chacha::ChaCha20Rng;
        use rand::SeedableRng;
        let mut rng = ChaCha20Rng::from_entropy();
        let tips = self.inner.blocking_read();
        if tips.is_empty() { return Pubkey::new_unique(); }
        let r: f64 = rng.gen();
        let mut acc = 0.0;
        for t in tips.iter() { acc += t.weight; if r <= acc { return t.account; } }
        tips.last().map(|t| t.account).unwrap_or_else(Pubkey::new_unique)
    }

    pub fn pick_seeded(&self, payer: &Pubkey, bh: &Hash) -> Pubkey {
        let mut hasher = blake3::Hasher::new();
        hasher.update(payer.as_ref());
        hasher.update(bh.as_ref());
        let mut seed = [0u8; 32];
        seed.copy_from_slice(hasher.finalize().as_bytes());
        let mut rng = ChaCha20Rng::from_seed(seed);
        let tips = self.inner.blocking_read();
        if tips.is_empty() { return Pubkey::new_unique(); }
        let r: f64 = rng.gen();
        let mut acc = 0.0;
        for t in tips.iter() { acc += t.weight; if r <= acc { return t.account; } }
        tips.last().map(|t| t.account).unwrap_or_else(Pubkey::new_unique)
    }
}

fn median(mut v: Vec<f64>) -> f64 {
    if v.is_empty() { return 0.0; }
    v.sort_by(|a,b| a.partial_cmp(b).unwrap());
    let m = v.len()/2;
    if v.len()%2==0 { (v[m-1]+v[m]) * 0.5 } else { v[m] }
}

// === global constants ===
const MAX_UDP_TX_BYTES: usize = 1232;
const UDP_SAFETY_MARGIN: usize = 24;
const MAX_COMPUTE_UNITS: u32 = 1_400_000;
const DEFAULT_PRIORITY_FEE_PER_CU: u64 = 50_000;
const BALANCE_RESERVE_LAMPORTS: u64 = 500_000;

// ----- Module-level instruction encoding helpers -----
// Anchor-compliant discriminator for instruction names: sha256("global:{ix}")[0..8]
fn anchor_discriminator(ix_name: &str) -> [u8; 8] {
    let mut hasher = Sha256::new();
    hasher.update(format!("global:{}", ix_name).as_bytes());
    let hash = hasher.finalize();
    let mut out = [0u8; 8];
    out.copy_from_slice(&hash[..8]);
    out
}

#[derive(BorshSerialize)]
struct DepositArgs {
    amount: u64,
}

#[derive(BorshSerialize)]
struct WithdrawArgs {
    amount: u64,
}

fn encode_anchor_ix(ix_name: &str, args: impl BorshSerialize) -> Result<Vec<u8>> {
    let mut data = anchor_discriminator(ix_name).to_vec();
    let mut args_data = Vec::with_capacity(16);
    args
        .serialize(&mut args_data)
        .map_err(|e| anyhow!(format!("borsh encode failed: {e}")))?;
    data.extend_from_slice(&args_data);
    Ok(data)
}

// Solend (non-Anchor) registry of verified tags
const SOLEND_DEPOSIT_TAG: u8 = 4; // Verified from Solend program source/tests
const SOLEND_WITHDRAW_TAG: u8 = 5; // Verified from Solend program source/tests
// Token-lending style flash loan opcodes (verify per protocol and lock with tests)
const FLASHLOAN_TAG: u8 = 10;
const FLASHREPAY_TAG: u8 = 11;

#[derive(Clone, Debug)]
pub struct LendingMarket {
    pub market_pubkey: Pubkey,
    pub token_mint: Pubkey,
    pub protocol: Protocol,
    pub supply_apy: f64,
    pub borrow_apy: f64,
    pub utilization: f64,
    pub liquidity_available: u64,
    pub last_update: Instant,
    pub layout_version: u8,
}

#[derive(Clone, Debug, PartialEq)]
pub enum Protocol {
    Solend,
    Kamino,
}

pub struct PortOptimizer {
    rpc_client: Arc<RpcClient>,
    // Optional faster RPC for hedged sims (may be None)
    rpc_fast: Option<Arc<RpcClient>>,
    wallet: Arc<Keypair>,
    markets: Arc<RwLock<HashMap<String, Vec<LendingMarket>>>>,
    active_positions: Arc<RwLock<HashMap<String, Position>>>,
    tip_router: Arc<RwLock<Option<TipRouter>>>,
    // Piece K/M: flow budget EMA and duplicate message digest guard
    flow_budget_ema: PLRwLock<HashMap<u64, FlowBudgetEma>>, // key: flow_salt
    recent_msg_digests: PLRwLock<(VecDeque<[u8;32]>, HashSet<[u8;32]>)>,
    // === [IMPROVED: PIECE 93A v15] === ALT plan cache keyed by need-set fingerprint
    alt_plan_by_need: PLRwLock<HashMap<(u64, u64), smallvec::SmallVec<[u64;6]>>>,
    // === [IMPROVED: PIECE 95A v15] === Tip router bandit stats
    tip_stats: DashMap<Pubkey, TipStat>,
    // === [IMPROVED: PIECE 96A v15] === Inverted ALT address index
    alt_addr_index: DashMap<Pubkey, smallvec::SmallVec<[Pubkey;6]>>,
}

#[derive(Clone, Debug)]
pub struct Position {
    pub token_mint: Pubkey,
    pub protocol: Protocol,
    pub amount: u64,
    pub entry_apy: f64,
    pub last_rebalance: Instant,
}

#[derive(Clone, Debug)]
pub struct ReserveMeta {
    pub program_id: Pubkey,
    pub reserve: Pubkey,
    pub lending_market: Pubkey,
    pub lending_market_authority: Pubkey,
    pub liquidity_mint: Pubkey,
    pub liquidity_supply_vault: Pubkey,
    pub collateral_mint: Pubkey,
    pub flash_capacity_hint: Option<u64>,
}

impl ReserveMeta {
    pub fn available_flash_capacity(&self) -> Result<u64> {
        Ok(self.flash_capacity_hint.unwrap_or(1_000_000_000_000))
    }
}

impl PortOptimizer {
    // --------- Fast digest and UDP guard (moved out of BufPool) ---------
    #[inline]
    fn digest_v0_fast(&self, tx: &VersionedTransaction) -> Option<[u8; 32]> {
        bincode::serialize(tx).ok().map(|b| {
            let mut out = [0u8; 32];
            out.copy_from_slice(blake3::hash(&b).as_bytes());
            out
        })
    }

    #[inline]
    fn near_udp_cap(&self, len: usize) -> bool {
        len + 12 + UDP_SAFETY_MARGIN >= MAX_UDP_TX_BYTES
    }

    /// returns true if newly inserted, false if seen recently
    fn seen_or_insert_digest(&self, d: [u8;32]) -> bool {
        let mut g = self.recent_msg_digests.write();
        let (ring, set) = (&mut g.0, &mut g.1);
        if set.contains(&d) { return false; }
        if ring.len() == 512 {
            if let Some(old) = ring.pop_front() { set.remove(&old); }
        }
        ring.push_back(d);
        set.insert(d);
        true
    }
    // ... (rest of the code remains the same)

    // ---------------- Piece K: Per-flow CU/heap EMA ---------------
    const EMA_ALPHA: f64 = 0.22;
    const UNITS_HEADROOM: f64 = 1.12;
    const HEAP_HEADROOM:  f64 = 1.15;
    const MAX_UNITS: u32 = 1_400_000;
        &self,
        amount: u64,
        apy_percent: f64,
        utilization: f64,
        liq_available: u64,
    ) -> f64 {
        let u = utilization.clamp(0.0, 1.0);
        let util_penalty = (u * u) * Self::UTIL_RISK_FACTOR;
        let deficit = 0f64.max(amount as f64 - liq_available as f64);
        let capacity_penalty = deficit * Self::CAPACITY_RISK_FACTOR;
        apy_percent - util_penalty - capacity_penalty
    }

    pub fn pick_best_market_weighted(
        &self,
        amount: u64,
        candidates: &[LendingMarket],
    ) -> Option<LendingMarket> {
        candidates
            .iter()
            .filter(|m| m.liquidity_available > 0)
            .max_by(|a, b| {
                let sa = self.market_score(amount, a.supply_apy, a.utilization, a.liquidity_available);
                let sb = self.market_score(amount, b.supply_apy, b.utilization, b.liquidity_available);
                sa.partial_cmp(&sb).unwrap()
            })
            .cloned()
    }
    pub fn new(rpc_url: &str, wallet: Keypair) -> Self {
        let rpc_client = Arc::new(RpcClient::new_with_commitment(
            rpc_url.to_string(),
            CommitmentConfig::confirmed(),
        ));

        Self {
            rpc_client,
            rpc_fast: None,
            wallet: Arc::new(wallet),
            markets: Arc::new(RwLock::new(HashMap::new())),
            active_positions: Arc::new(RwLock::new(HashMap::new())),
            tip_router: Arc::new(RwLock::new(None)),
            flow_budget_ema: PLRwLock::new(HashMap::new()),
            recent_msg_digests: PLRwLock::new((VecDeque::with_capacity(256), HashSet::with_capacity(256))),
            alt_plan_by_need: PLRwLock::new(HashMap::new()),
            tip_stats: DashMap::new(),
            alt_addr_index: DashMap::new(),
        }

    // ----- Flashloan-based atomic rebalance (generic token-lending style) -----
    #[derive(Clone)]
    pub struct FlashPlan {
        pub flash_program: Pubkey,
        pub reserve: Pubkey,
        pub liquidity_supply_vault: Pubkey,
        pub lending_market: Pubkey,
        pub market_authority: Pubkey,
        pub token_mint: Pubkey,
        pub amount: u64,
    }

    // Build flash loan ix (non-Anchor example; verify tags per protocol and lock by tests)
    fn build_flash_loan_ix(&self, plan: &FlashPlan, user_flash_token_ata: Pubkey) -> Result<Instruction> {
        let mut data = vec![FLASHLOAN_TAG];
        data.extend_from_slice(&plan.amount.to_le_bytes());
        let accounts = vec![
            AccountMeta::new(plan.reserve, false),
            AccountMeta::new(plan.liquidity_supply_vault, false),
            AccountMeta::new(user_flash_token_ata, false),
            AccountMeta::new_readonly(plan.lending_market, false),
            AccountMeta::new_readonly(plan.market_authority, false),
            AccountMeta::new_readonly(spl_token::id(), false),
            AccountMeta::new_readonly(solana_program::sysvar::clock::id(), false),
        ];
        Ok(Instruction { program_id: plan.flash_program, accounts, data })
    }

    fn build_flash_repay_ix(&self, plan: &FlashPlan, user_flash_token_ata: Pubkey, repay_amount: u64) -> Result<Instruction> {
        let mut data = vec![FLASHREPAY_TAG];
        data.extend_from_slice(&repay_amount.to_le_bytes());
        let accounts = vec![
            AccountMeta::new(plan.reserve, false),
            AccountMeta::new(plan.liquidity_supply_vault, false),
            AccountMeta::new(user_flash_token_ata, false),
            AccountMeta::new_readonly(plan.lending_market, false),
            AccountMeta::new_readonly(plan.market_authority, false),
            AccountMeta::new_readonly(spl_token::id(), false),
            AccountMeta::new_readonly(solana_program::sysvar::clock::id(), false),
        ];
        Ok(Instruction { program_id: plan.flash_program, accounts, data })
    }

    fn protocol_from_program_id(&self, program_id: &Pubkey) -> Protocol {
        if *program_id == *KAMINO_PROGRAM_ID { Protocol::Kamino } else { Protocol::Solend }
    }

    // Compute precise repay amount from on-chain fee bps
    fn compute_flash_repay_amount(&self, amount: u64, fee_bps: u64) -> u64 {
        amount.saturating_add(((amount as u128) * (fee_bps as u128) / 10_000u128) as u64)
    }

    // ---------------- Budget ixs + robust percentile fees ----------------
    pub async fn percentile_cu_price(&self, floor_lamports_per_cu: u64, p: f64) -> u64 {
        let mut price = floor_lamports_per_cu.max(1);
        if let Ok(fees) = self.rpc_client.get_recent_prioritization_fees(&[]).await {
            let mut v: Vec<u64> = fees
                .into_iter()
                .map(|f| f.prioritization_fee)
                .filter(|&u| u > 0)
                .collect();
            if !v.is_empty() {
                v.sort_unstable();
                // trimmed quantile to resist spikes
                let trim = (v.len() as f64 * 0.05).floor() as usize;
                let lo = trim.min(v.len());
                let hi = v.len().saturating_sub(trim);
                let v = &v[lo..hi];
                if !v.is_empty() {
                    let idx = ((v.len() as f64 - 1.0) * p).round() as usize;
                    price = price.max(v[idx.min(v.len() - 1)]);
                }
            }
        }
        price
    }

    /// Program-scoped prioritization fee with 5% trimmed quantile.
    pub async fn percentile_cu_price_scoped(
        &self,
        programs: &[Pubkey],
        floor_lamports_per_cu: u64,
        p: f64,
    ) -> u64 {
        let mut price = floor_lamports_per_cu.max(1);
        if let Ok(fees) = self.rpc_client.get_recent_prioritization_fees(programs).await {
            let mut v: Vec<u64> = fees
                .into_iter()
                .map(|f| f.prioritization_fee)
                .filter(|&u| u > 0)
                .collect();
            if !v.is_empty() {
                v.sort_unstable();
                let trim = (v.len() as f64 * 0.05).floor() as usize;
                let lo = trim.min(v.len());
                let hi = v.len().saturating_sub(trim);
                let v = &v[lo..hi];
                if !v.is_empty() {
                    let idx = ((v.len() as f64 - 1.0) * p.clamp(0.0, 1.0)).round() as usize;
                    price = price.max(v[idx.min(v.len() - 1)]);
                }
            }
        }
        price
    }

    /// Keep only ALTs that actually contain accounts referenced by `ixs`.
    pub fn greedy_min_alts(
        &self,
        ixs: &[Instruction],
        alts_all: &[AddressLookupTableAccount],
    ) -> Vec<AddressLookupTableAccount> {
        if alts_all.is_empty() { return Vec::new(); }
        let mut needed: HashSet<Pubkey> = HashSet::with_capacity(ixs.len() * 8);
        for ix in ixs {
            for am in &ix.accounts { needed.insert(am.pubkey); }
        }
        alts_all
            .iter()
            .cloned()
            .filter(|alt| alt.addresses.iter().any(|k| needed.contains(k)))
            .collect()
    }

    /// Build a v0 transaction with provided ALTs.
    pub fn build_v0_tx(
        &self,
        payer: Pubkey,
        ixs: &[Instruction],
        bh: Hash,
        alts: &[AddressLookupTableAccount],
    ) -> Result<VersionedTransaction> {
        let msg = V0Message::try_compile(&payer, ixs, alts, bh)
            .map_err(|e| anyhow!(format!("v0 compile failed: {e:?}")))?;
        let vmsg = VersionedMessage::V0(msg);
        VersionedTransaction::try_new(vmsg, &[&*self.wallet])
            .map_err(|e| anyhow!(format!("v0 sign failed: {e:?}")))
    }

    fn build_legacy_tx(
        &self,
        payer: Pubkey,
        ixs: &[Instruction],
        bh: Hash,
    ) -> Result<(Transaction, usize)> {
        let msg = Message::new(ixs, Some(&payer));
        let mut tx = Transaction::new_unsigned(msg);
        tx.try_sign(&[&*self.wallet], bh)?;
        let len = bincode::serialized_size(&tx)? as usize;
        Ok((tx, len))
    }

    /// Choose shortest viable encoding under UDP cap: v0 (ALT-pruned) vs legacy.
    pub fn choose_best_tx_sync(
        &self,
        payer: Pubkey,
        ixs: &[Instruction],
        bh: Hash,
        alts_all: &[AddressLookupTableAccount],
    ) -> Result<Either<(VersionedTransaction, usize, usize), (Transaction, usize)>> {
        let v0 = self.best_subset_v0_sync(payer, ixs, bh, alts_all);
        let legacy = self.build_legacy_tx(payer, ixs, bh).ok();
        match (v0, legacy) {
            (Some((vtx, vlen, kept)), Some((ltx, llen))) => {
                let v_ok = vlen <= MAX_UDP_TX_BYTES;
                let l_ok = llen <= MAX_UDP_TX_BYTES;
                match (v_ok, l_ok) {
                    (true, true) => {
                        if vlen <= llen { Ok(Either::Left((vtx, vlen, kept))) } else { Ok(Either::Right((ltx, llen))) }
                    }
                    (true, false) => Ok(Either::Left((vtx, vlen, kept))),
                    (false, true) => Ok(Either::Right((ltx, llen))),
                    (false, false) => Ok(Either::Left((vtx, vlen, kept))),
                }
            }
            (Some((vtx, vlen, kept)), None) => Ok(Either::Left((vtx, vlen, kept))),
            (None, Some((ltx, llen)))      => Ok(Either::Right((ltx, llen))),
            _ => Err(anyhow!("no viable transaction encoding")),
        }
    }

    /// ALT subset optimizer: cached + prefilter + dominance + stable order + diversified frontier (sketch-penalized)
    /// + exact (≤12) + ranked 2-step (optionally parallel) + beam-3 fallback, UDP-aware, memo + dynamic soft deadline.
    pub fn best_subset_v0_sync(
        &self,
        payer: Pubkey,
        ixs: &[Instruction],
        bh: Hash,
        alts_all: &[AddressLookupTableAccount],
    ) -> Option<(VersionedTransaction, usize, usize)> {
        // Tunables
        const ALT_CACHE_TTL_MS: u128 = 800;
        const HARD_CAP: usize = 128;
        const BASE_BUDGET_MS: u128 = 6;
        const MAX_BUDGET_MS:  u128 = 12;
        const BNB_FUDGE: usize = 8;

        let t_start = now_ms();

        // Degenerate fast paths
        if alts_all.is_empty() {
            return self.estimate_len_v0_sync(payer, ixs, bh, &[]).map(|(tx, l)| (tx, l, 0));
        }
        if alts_all.len() == 1 {
            return self.estimate_len_v0_sync(payer, ixs, bh, alts_all).map(|(tx, l)| (tx, l, 1));
        }

        // Cache
        let salt = self.ixs_fee_salt(ixs);
        let key  = alt_cache_key(&payer, &bh, salt, alts_all);
        let key_fp: u64 = salt;                                       /// +++ NEXT (M.v3.1 domain sep)
        // +++ NEXT (R): opportunistic try_read to avoid blocking; L1 guard
        let skip_global = l1_recent_miss(key_fp);
        if !skip_global {
            if let Ok(g) = ALT_PRUNE_CACHE.try_read() {
                if let Some((cached_tx, cached_len, kept, ts)) = g.get(&key) {
                    if now_ms().saturating_sub(*ts) <= ALT_CACHE_TTL_MS {
                        l1_put(key_fp, &(cached_tx.clone(), *cached_len, *kept));
                        return Some((cached_tx.clone(), *cached_len, *kept));
                    }
                } else {
                    l1_note_miss(key_fp);
                }
            } else {
                l1_note_miss(key_fp);
            }
        }

        // Compact key index (u16) with deterministic ordering
        use smallvec::SmallVec;
        use std::collections::{HashMap, HashSet};

        let mut used_set: HashSet<Pubkey> = HashSet::with_capacity(ixs.len() * 4);
        for ix in ixs {
            used_set.insert(ix.program_id);
            for a in &ix.accounts { used_set.insert(a.pubkey); }
        }
        let mut used_vec: Vec<Pubkey> = used_set.iter().cloned().collect();
        used_vec.sort_unstable();
        let mut idx_of: HashMap<Pubkey, u16> = HashMap::with_capacity(used_vec.len());
        let mut rev: SmallVec<[Pubkey; 128]> = SmallVec::new();
        for (i, k) in used_vec.into_iter().enumerate() {
            if i >= u16::MAX as usize { break; }
            idx_of.insert(k, i as u16);
            rev.push(k);
        }

        #[inline]
        fn mk_cov_u16(alt: &AddressLookupTableAccount, idx_of: &HashMap<Pubkey, u16>) -> SmallVec<[u16; 48]> {
            let mut v: SmallVec<[u16; 48]> = SmallVec::new();
            for k in &alt.addresses { if let Some(&ix) = idx_of.get(k) { v.push(ix); } }
            if v.is_empty() { return v; }
            v.sort_unstable(); v.dedup(); v
        }

        #[inline]
        fn is_subset_sorted_u16(a: &[u16], b: &[u16]) -> bool {
            let (mut i, mut j) = (0, 0);
            while i < a.len() && j < b.len() {
                if a[i] == b[j] { i += 1; j += 1; } else if a[i] > b[j] { j += 1; } else { return false; }
            }
            i == a.len()
        }

        // Build (ALT, cov_u16)
        let mut pairs: Vec<(AddressLookupTableAccount, SmallVec<[u16; 48]>)> = Vec::with_capacity(alts_all.len());
        for alt in alts_all {
            let cov = mk_cov_u16(alt, &idx_of);
            if !cov.is_empty() { pairs.push((alt.clone(), cov)); }
        }
        if pairs.is_empty() {
            return self.estimate_len_v0_sync(payer, ixs, bh, &[]).map(|(tx, l)| (tx, l, 0));
        }
        if pairs.len() == 1 {
            let (a, _) = &pairs[0];
            return self.estimate_len_v0_sync(payer, ixs, bh, std::slice::from_ref(a)).map(|(tx, l)| (tx, l, 1));
        }

        // +++ NEXT: dedup by exact coverage via lexicographic order (collision-free)
        pairs.sort_by(|(a1, c1), (a2, c2)| {
            c1.len().cmp(&c2.len())
                .then_with(|| c1.cmp(c2))
                .then_with(|| a1.key.cmp(&a2.key))
        });
        pairs.dedup_by(|(a_prev, c_prev), (a_cur, c_cur)| {
            if c_prev == c_cur { true } else { false }
        });
        if pairs.is_empty() {
            return self.estimate_len_v0_sync(payer, ixs, bh, &[]).map(|(tx, l)| (tx, l, 0));
        }

        // Exact bitset path for ≤256 keys
        #[derive(Clone, Copy, Default)]
        struct Bits256 { a:u64,b:u64,c:u64,d:u64 }
        impl Bits256 {
            #[inline] fn or(self,o:Self)->Self{Self{a:self.a|o.a,b:self.b|o.b,c:self.c|o.c,d:self.d|o.d}}
            #[inline] fn overlap(self,o:Self)->u32{ (self.a&o.a).count_ones()+(self.b&o.b).count_ones()+(self.c&o.c).count_ones()+(self.d&o.d).count_ones() }
            #[inline] fn subset_of(self, o:Self)->bool{ (self.a & !o.a)==0 && (self.b & !o.b)==0 && (self.c & !o.c)==0 && (self.d & !o.d)==0 }
        }
        #[inline] fn set_bit(mut bits:Bits256, idx:u16)->Bits256{ let lane = (idx as usize)>>6; let off = (idx as u64)&63; match lane {0=>bits.a|=1<<off,1=>bits.b|=1<<off,2=>bits.c|=1<<off,_=>bits.d|=1<<off} bits }
        let used_le_256 = rev.len() <= 256;
        let exact_bits: Option<Vec<Bits256>> = if used_le_256 {
            let mut v = Vec::with_capacity(pairs.len());
            for (_, cov) in &pairs { let mut b=Bits256::default(); for &ix in cov { b=set_bit(b, ix); } v.push(b); }
            Some(v)
        } else { None };

        // Dominance prune (bitset fast-lane or two-pointer)
        {
            let mut keep = vec![true; pairs.len()];
            if let Some(ref bits) = exact_bits {
                let sizes: Vec<usize> = pairs.iter().map(|p| p.1.len()).collect();
                for i in 0..pairs.len() {
                    if !keep[i] { continue; }
                    for j in 0..pairs.len() {
                        if i==j || !keep[j] { continue; }
                        if sizes[i] <= sizes[j] && bits[i].subset_of(bits[j]) { keep[i]=false; break; }
                    }
                }
            } else {
                for i in 0..pairs.len() {
                    if !keep[i] { continue; }
                    for j in 0..pairs.len() {
                        if i == j || !keep[j] { continue; }
                        if pairs[i].1.len() <= pairs[j].1.len() && is_subset_sorted_u16(&pairs[i].1, &pairs[j].1) { keep[i] = false; break; }
                    }
                }
            }
            let mut compact: Vec<(AddressLookupTableAccount, SmallVec<[u16; 48]>)> = Vec::with_capacity(pairs.len());
            for (i, p) in pairs.into_iter().enumerate() { if keep[i] { compact.push(p); } }
            pairs = compact;
            if pairs.is_empty() { return self.estimate_len_v0_sync(payer, ixs, bh, &[]).map(|(tx, l)| (tx, l, 0)); }
        }

        // Stable order by ALT key
        pairs.sort_by_key(|(alt, _)| alt.key);

        // Rarity weights on compact index space
        let mut freq: Vec<u32> = vec![0; rev.len()];
        for (_, cov) in &pairs { for &ix in cov { freq[ix as usize] += 1; } }

        // +++ NEXT: preseed ALTs that carry unique keys (freq==1)
        let mut forced_idx: Vec<usize> = Vec::new();
        let mut forced_mark: Vec<bool> = vec![false; pairs.len()];
        for (i, (_alt, cov)) in pairs.iter().enumerate() {
            if cov.iter().any(|&k| freq[k as usize] == 1) {
                forced_mark[i] = true;
                forced_idx.push(i);
            }
        }
        forced_idx.sort_unstable();

        // 256-bit hashless sketch for >256-key case
        #[derive(Clone, Copy, Default)]
        struct CovSketch { a: u64, b: u64, c: u64, d: u64 }
        #[inline] fn mix64(mut x: u64) -> u64 { x = x.wrapping_add(0x9E3779B97F4A7C15); let mut z = x; z = (z ^ (z >> 30)).wrapping_mul(0xBF58476D1CE4E5B9); z = (z ^ (z >> 27)).wrapping_mul(0x94D049BB133111EB); z ^ (z >> 31) }
        impl CovSketch { #[inline] fn or(self,o:Self)->Self{Self{a:self.a|o.a,b:self.b|o.b,c:self.c|o.c,d:self.d|o.d}} #[inline] fn overlap(self,o:Self)->u32{ (self.a&o.a).count_ones()+(self.b&o.b).count_ones()+(self.c&o.c).count_ones()+(self.d&o.d).count_ones() } }
        #[inline] fn sketch_idx(idx: u16, salt: u64) -> CovSketch { let s0 = mix64(salt ^ 0xA4D19DAB52A713C5u64 ^ idx as u64); let s1 = mix64(s0 ^ 0x94D049BB133111EBu64); let mut a=0u64; let mut b=0u64; let mut c=0u64; let mut d=0u64; a|=1u64<<(s0&63); b|=1u64<<((s0>>8)&63); c|=1u64<<(s1&63); d|=1u64<<((s1>>8)&63); CovSketch{a,b,c,d} }
        #[inline] fn sketch_vec_u16(v:&[u16], salt:u64)->CovSketch{ let mut s=CovSketch::default(); for &ix in v { s = s.or(sketch_idx(ix, salt)); } s }

        // +++ NEXT: smooth rarity curve: ~56 down to 8 as freq grows
        let key_value = |f: u32| -> i32 {
            let x = (f as f64 + 1.0).log2();
            let s = 56.0 - 12.0 * x;
            s.max(8.0).min(56.0) as i32
        };
        let base_score_idx = |i: usize| -> i32 {
            let hits = pairs[i].1.len() as i32;
            let rare: i32 = pairs[i].1.iter().map(|&k| key_value(freq[k as usize])).sum();
            rare + hits.saturating_mul(4) - 20
        };

        let sketches: Option<Vec<CovSketch>> = if used_le_256 { None } else { Some(pairs.iter().map(|(_, cov)| sketch_vec_u16(cov, salt)).collect()) };

        // Diversified frontier selection with deterministic order + forced seeding
        let mut pool: Vec<(i32, usize)> = (0..pairs.len())
            .filter(|i| !forced_mark[*i])
            .map(|i| (base_score_idx(i), i))
            .collect();
        pool.sort_by_key(|&(s, i)| (std::cmp::Reverse(s), pairs[i].0.key));
        let frontier_target = pairs.len().min(24).max(16);
        let max_keep = pairs.len().min(HARD_CAP);

        let mut kept_idx: Vec<usize> = Vec::with_capacity(max_keep);
        kept_idx.extend(forced_idx.iter().copied());

        const LAMBDA_BITS: i32 = 3;
        let mut covered_exact = Bits256::default();
        let mut covered_sketch = CovSketch::default();
        if let (true, Some(ref ex)) = (used_le_256, exact_bits.as_ref()) {
            for &i in &forced_idx { covered_exact = covered_exact.or(ex[i]); }
        } else if let Some(ref sk) = sketches {
            for &i in &forced_idx { covered_sketch = covered_sketch.or(sk[i]); }
        }

        if kept_idx.len() >= max_keep {
            kept_idx.truncate(max_keep);
        } else {
            while kept_idx.len() < max_keep && !pool.is_empty() {
                let mut best: Option<(i32, usize, usize)> = None;
                for (pi, &(s, i)) in pool.iter().enumerate() {
                    let adj = if let (true, Some(ref ex)) = (used_le_256, exact_bits.as_ref()) {
                        s - LAMBDA_BITS * (covered_exact.overlap(ex[i]) as i32)
                    } else {
                        s - LAMBDA_BITS * (covered_sketch.overlap(sketches.as_ref().unwrap()[i]) as i32)
                    };
                    if best.is_none()
                        || adj > best.unwrap().0
                        || (adj == best.unwrap().0 && pairs[i].0.key < pairs[best.unwrap().2].0.key)
                    { best = Some((adj, pi, i)); }
                }
                let (_adj, pi, i) = best.unwrap();
                if let (true, Some(ref ex)) = (used_le_256, exact_bits.as_ref()) { covered_exact = covered_exact.or(ex[i]); }
                else { covered_sketch = covered_sketch.or(sketches.as_ref().unwrap()[i]); }
                kept_idx.push(i);
                pool.swap_remove(pi);
                if kept_idx.len() >= frontier_target { break; }
            }
        }
        kept_idx.sort_unstable();
        let mut alts_pref: SmallVec<[AddressLookupTableAccount; 16]> = SmallVec::new();
        for i in &kept_idx { alts_pref.push(pairs[*i].0.clone()); }

        // Memo (mask -> (len, kept)) + tiny fail-memo (Q)
        struct LocalLenMemo(std::collections::HashMap<u128, (usize, usize)>);
        impl LocalLenMemo { #[inline] fn new() -> Self { Self(std::collections::HashMap::with_capacity(256)) } #[inline] fn get(&self, m: u128) -> Option<(usize, usize)> { self.0.get(&m).cloned() } #[inline] fn put(&mut self, m: u128, l: usize, kept: usize) { self.0.insert(m, (l, kept)); } }
        /// +++ NEXT: tiny memo for failed masks (open addressing)
        struct TinyFail { keys: [u128; 128], tags: [u16; 128], used: [u8; 128] }
        impl TinyFail {
            #[inline] fn new() -> Self { Self { keys: [0;128], tags: [0;128], used: [0;128] } }
            #[inline] fn tag(k: u128) -> u16 { let x = (k as u64) ^ ((k >> 64) as u64); ((x ^ (x >> 15)).wrapping_mul(0x9E37) & 0xFFFF) as u16 }
            #[inline] fn idx(k: u128) -> usize { let x = (k as u64) ^ ((k >> 64) as u64); (x.wrapping_mul(0xD6E8_F1C4_27A3_94B5) as usize) & (128 - 1) }
            #[inline] fn has(&self, k: u128) -> bool { let mut i = Self::idx(k); let t = Self::tag(k); for _ in 0..128 { if self.used[i]==0 { return false; } if self.tags[i]==t && self.keys[i]==k { return true; } i = (i+1) & (128-1); } false }
            #[inline] fn put(&mut self, k: u128) { let mut i = Self::idx(k); let t = Self::tag(k); for _ in 0..128 { if self.used[i]==0 || (self.tags[i]==t && self.keys[i]==k) { self.keys[i]=k; self.tags[i]=t; self.used[i]=1; return; } i = (i+1) & (128-1); } self.keys[0]=k; self.tags[0]=t; self.used[0]=1; }
        }
        let mut memo = LocalLenMemo::new();
        let mut memo_fail = TinyFail::new();
        #[inline] fn bit(mask: u128, idx: usize) -> bool { ((mask >> idx) & 1) == 1 }
        #[inline] fn pop(mask: u128) -> u32 { mask.count_ones() }
        #[inline] fn tri_better(len_a: usize, kept_a: usize, mask_a: u128, len_b: usize, kept_b: usize, mask_b: u128) -> bool { (len_a, kept_a, pop(mask_a)) < (len_b, kept_b, pop(mask_b)) }

        // Length measure (bytes-only estimate optional)
        let mut measure_len = |mask: u128| -> Option<(usize, usize)> {
            if memo_fail.has(mask) { return None; }
            if let Some((l, kept)) = memo.get(mask) { return Some((l, kept)); }
            let mut picked: SmallVec<[AddressLookupTableAccount; 16]> = SmallVec::new();
            picked.reserve(mask.count_ones() as usize);
            for (i, alt) in alts_pref.iter().enumerate() { if bit(mask, i) { picked.push(alt.clone()); } }
            #[cfg(feature = "alt_fast_len")]
            let l = match self.estimate_len_v0_bytes_only_sync(payer, ixs, bh, &picked) { Some(x) => x, None => { memo_fail.put(mask); return None; } };
            #[cfg(not(feature = "alt_fast_len"))]
            let (_tx, l) = match self.estimate_len_v0_sync(payer, ixs, bh, &picked) { Some((t,x)) => (t,x), None => { memo_fail.put(mask); return None; } };
            let kept = picked.len();
            memo.put(mask, l, kept);
            Some((l, kept))
        };

        // Finalizer build
        let mut build_tx_for = |mask: u128| -> Option<(VersionedTransaction, usize, usize)> {
            let mut picked: SmallVec<[AddressLookupTableAccount; 16]> = SmallVec::new();
            for (i, alt) in alts_pref.iter().enumerate() { if bit(mask, i) { picked.push(alt.clone()); } }
            let (tx, l) = self.estimate_len_v0_sync(payer, ixs, bh, &picked)?;
            Some((tx, l, picked.len()))
        };

        // Safety clamp
        let n = alts_pref.len();
        if n > HARD_CAP {
            let mut all_mask = 0u128; for i in 0..HARD_CAP.min(n) { all_mask |= 1u128 << i; }
            if let Some((l, kept)) = measure_len(all_mask) {
                let (tx, _, _) = build_tx_for(all_mask)?;
                let mut g = ALT_PRUNE_CACHE.write();
                if g.len() > 64 { if let Some((&old_k, _)) = g.iter().min_by_key(|(_, v)| v.3) { g.remove(&old_k); } }
                g.insert(key, (tx.clone(), l, kept, now_ms()));
                return Some((tx, l, kept));
            }
            return None;
        }

        let full_mask: u128 = if n == HARD_CAP { u128::MAX } else { (1u128 << n) - 1 };
        let (base_len, base_cnt) = measure_len(full_mask)?;
        let mut best_len  = base_len;
        let mut best_cnt  = base_cnt;
        let mut best_mask = full_mask;

        // Dynamic soft deadline
        let mut budget_ms: u128 = BASE_BUDGET_MS;
        if best_len > MAX_UDP_TX_BYTES { let over = best_len - MAX_UDP_TX_BYTES; budget_ms = (BASE_BUDGET_MS + (over as u128) / 40).min(MAX_BUDGET_MS); }
        let deadline = t_start + budget_ms;
        let mut _time_tick: u32 = 0;                             /// +++ NEXT (P)

        if best_len + 64 <= MAX_UDP_TX_BYTES {
            let (tx, l, kept) = build_tx_for(best_mask)?;
            cache_put_alt_sampled(&key, key_fp, &tx, l, kept);           /// +++ NEXT
            return Some((tx, l, kept));
        }

        // Exact search (≤12) via Gray-code
        if n <= 12 {
            let end = 1u128 << n;
            for g in 1..end {
                if time_over(deadline, &mut _time_tick) { break; }
                let m = g ^ (g >> 1);
                if let Some((l, kept)) = measure_len(m) {
                    if tri_better(l, kept, m, best_len, best_cnt, best_mask) { best_len = l; best_cnt = kept; best_mask = m; }
                }
            }
            let (tx, l, kept) = build_tx_for(best_mask)?;
            cache_put_alt_sampled(&key, key_fp, &tx, l, kept);           /// +++ NEXT
            return Some((tx, l, kept));
        }

        // UDP-aware single-drops (mask-first, optional parallel)
        let udp_score = |l: usize| -> usize { if l > MAX_UDP_TX_BYTES { usize::MAX - (l - MAX_UDP_TX_BYTES) } else { MAX_UDP_TX_BYTES - l } };
        let mut len_after_drop: Vec<Option<usize>> = vec![None; n];
        let mut deltas: Vec<(usize, usize)> = Vec::with_capacity(n);
        #[cfg(feature = "parallel_alt_search")]
        {
            use rayon::prelude::*;
            let drops: Vec<(usize, usize)> = (0..n).into_par_iter().filter_map(|i| { let m = full_mask & !(1u128 << i); measure_len(m).map(|(l, _k)| (i, l)) }).collect();
            for (i, l) in drops { len_after_drop[i] = Some(l); deltas.push((i, udp_score(l))); }
        }
        #[cfg(not(feature = "parallel_alt_search"))]
        {
            for i in 0..n {
                if time_over(deadline, &mut _time_tick) { break; }
                let m = full_mask & !(1u128 << i);
                if let Some((l, _k)) = measure_len(m) {
                    len_after_drop[i] = Some(l);
                    deltas.push((i, udp_score(l)));

                    // +++ NEXT: perfect-fit early accept
                    if l <= MAX_UDP_TX_BYTES.saturating_sub(8) {
                        let (tx2, l2, kept2) = build_tx_for(m)?;
                        cache_put_alt_sampled(&key, key_fp, &tx2, l2, kept2);
                        return Some((tx2, l2, kept2));
                    }
                }
            }
        }

        let overshoot = best_len.saturating_sub(MAX_UDP_TX_BYTES);
        let k1_target = if overshoot > 512 { 16 } else if overshoot > 256 { 12 } else if overshoot > 64 { 10 } else { 8 };
        // +++ NEXT (S): gain-density ordering for seeds
        let mut scored: Vec<(usize, usize, i32)> = Vec::with_capacity(deltas.len());
        for &(i, s) in &deltas {
            let freq_sum: i32 = pairs[i].1.iter().map(|&k| freq[k as usize] as i32).sum();
            let penalty = 1 + (freq_sum / (pairs[i].1.len().max(1) as i32));
            let density = (s as i32) / penalty.max(1);
            scored.push((i, s, density));
        }
        scored.sort_by_key(|&(i, s, d)| (std::cmp::Reverse(d), std::cmp::Reverse(s), i));
        let k1 = scored.len().min(k1_target);
        let top1: Vec<(usize, usize)> = scored.into_iter().take(k1).map(|(i, s, _)| (i, s)).collect();

        'pairs: for a in 0..top1.len() {
            for b in (a + 1)..top1.len() {
                if time_over(deadline, &mut _time_tick) { break 'pairs; }
                let i = top1[a].0; let j = top1[b].0; if i == j { continue; }
                let gi = len_after_drop[i].map(|l| base_len.saturating_sub(l)).unwrap_or(0);
                let gj = len_after_drop[j].map(|l| base_len.saturating_sub(l)).unwrap_or(0);
                let optimistic = base_len.saturating_sub(gi + gj);
                if optimistic + BNB_FUDGE >= best_len { continue; }
                let m = full_mask & !(1u128 << i) & !(1u128 << j);
                if let Some((l2, kept2)) = measure_len(m) { if l2 < best_len || (l2 == best_len && kept2 < best_cnt) { best_len = l2; best_cnt = kept2; best_mask = m; if best_len + 16 <= MAX_UDP_TX_BYTES { break 'pairs; } } }
            }
        }

        // Beam-3 + BnB
        if best_len > MAX_UDP_TX_BYTES.saturating_sub(8) && top1.len() >= 3 {
            let k3 = top1.len().min(6);
            'triples: for a in 0..k3 {
                for b in (a + 1)..k3 {
                    for c in (b + 1)..k3 {
                        if time_over(deadline, &mut _time_tick) { break 'triples; }
                        let i = top1[a].0; let j = top1[b].0; let k = top1[c].0;
                        let gi = len_after_drop[i].map(|l| base_len.saturating_sub(l)).unwrap_or(0);
                        let gj = len_after_drop[j].map(|l| base_len.saturating_sub(l)).unwrap_or(0);
                        let gk = len_after_drop[k].map(|l| base_len.saturating_sub(l)).unwrap_or(0);
                        let optimistic = base_len.saturating_sub(gi + gj + gk);
                        if optimistic + BNB_FUDGE >= best_len { continue; }
                        let m = full_mask & !(1u128 << i) & !(1u128 << j) & !(1u128 << k);
                        if let Some((l3, kept3)) = measure_len(m) { if l3 < best_len || (l3 == best_len && kept3 < best_cnt) { best_len = l3; best_cnt = kept3; best_mask = m; if best_len + 16 <= MAX_UDP_TX_BYTES { break 'triples; } } }
                    }
                }
            }
        }

        // Guarded 4-drop pass when overshoot is large
        if best_len > MAX_UDP_TX_BYTES.saturating_sub(8) && top1.len() >= 4 && overshoot > 256 {
            let k4 = top1.len().min(6);
            'quads: for a in 0..k4 {
                for b in (a + 1)..k4 {
                    for c in (b + 1)..k4 {
                        for d in (c + 1)..k4 {
                            if time_over(deadline, &mut _time_tick) { break 'quads; }
                            let i = top1[a].0; let j = top1[b].0; let k = top1[c].0; let t = top1[d].0;
                            let gi = len_after_drop[i].map(|l| base_len.saturating_sub(l)).unwrap_or(0);
                            let gj = len_after_drop[j].map(|l| base_len.saturating_sub(l)).unwrap_or(0);
                            let gk = len_after_drop[k].map(|l| base_len.saturating_sub(l)).unwrap_or(0);
                            let gt = len_after_drop[t].map(|l| base_len.saturating_sub(l)).unwrap_or(0);
                            let optimistic = base_len.saturating_sub(gi + gj + gk + gt);
                            if optimistic + BNB_FUDGE >= best_len { continue; }
                            let m = full_mask & !(1u128 << i) & !(1u128 << j) & !(1u128 << k) & !(1u128 << t);
                            if let Some((l4, kept4)) = measure_len(m) { if l4 < best_len || (l4 == best_len && kept4 < best_cnt) { best_len = l4; best_cnt = kept4; best_mask = m; if best_len + 16 <= MAX_UDP_TX_BYTES { break 'quads; } } }
                        }
                    }
                }
            // end quads
        }

        // Final refinement: hill-climb drop-one if still near/over cap
        if best_len > MAX_UDP_TX_BYTES.saturating_sub(8) {
            let mut cur_mask = best_mask;
            let mut improved = true;
            while improved && cur_mask != 0 {
                if time_over(deadline, &mut _time_tick) { break; }
                improved = false;
                let mut m = cur_mask;
                while m != 0 {
                    let i = m.trailing_zeros();
                    let trial = cur_mask & !(1u128 << i);
                    if let Some((l, kept)) = measure_len(trial) {
                        if l < best_len && l <= MAX_UDP_TX_BYTES {
                            best_len = l; best_cnt = kept; best_mask = trial; improved = true; break;
                        }
                    }
                    m &= m - 1;
                }
                if improved { cur_mask = best_mask; }
            }
        }

        // Build from best mask and cache
        let (tx, l, kept) = build_tx_for(best_mask)?;
        cache_put_alt_sampled(&key, key_fp, &tx, l, kept);
        Some((tx, l, kept))
    }

    /// Exact v0 simulation that returns units_consumed (if parsable) and logs.
    pub async fn simulate_units_and_logs(&self, tx: &VersionedTransaction) -> Result<(Option<u32>, Vec<String>)> {
        let cfg = RpcSimulateTransactionConfig { sig_verify: false, replace_recent_blockhash: true, ..Default::default() };
        let out = self.rpc_client.simulate_transaction_with_config(tx, cfg).await?;
        let mut units = out.value.units_consumed.map(|u| u as u32);

        if units.is_none() {
            if let Some(ls) = out.value.logs.as_ref() {
                // Consolidated vendor patterns; scan tail in reverse to bound work
                const KEYS: &[&str] = &[
                    "consumed ", "compute units: ", "units consumed: ", "total units consumed: ",
                    "compute units consumed: ", "Total compute units consumed: ", "units total: ",
                    "CU total: ", "units=", "used: ", "used=", "cu_used=", "cu: ", "total cu: ",
                ];
                let tail = 256usize.min(ls.len());
                for line in ls[ls.len()-tail..].iter().rev() {
                    let clean = self.strip_ansi(line);
                    if let Some(v) = self.parse_num_after_any(&clean, KEYS).and_then(|n| self.filter_units(n)) {
                        units = Some(v); break;
                    }
                    if units.is_none() {
                        if let Some(v) = self.parse_last_num_if_hinted(&clean).and_then(|n| self.filter_units(n)) {
                            units = Some(v); break;
                        }
                    }
                }
            }
        }
        Ok((units, out.value.logs.unwrap_or_default()))
    }

    /// Adjust compute/heap budgets based on common simulation errors. (widened cues + quantized bumps)
    pub fn adjust_budget_from_sim_error(&self, logs: &[String], units0: u32, heap0: Option<u32>) -> (u32, Option<u32>, bool) {
        let mut units = units0;
        let mut heap  = heap0;
        let mut changed = false;

        let mut bump_units = 0.0;
        let mut need_heap  = false;
        let mut drop_heap  = false;

        for s in logs {
            let l = s.as_str();
            if l.contains("computational budget exceeded")
                || l.contains("max compute units exceeded")
                || l.contains("Program failed to complete")
                || l.contains("Exceeded maximum number of instructions")
                || l.contains("budget exhausted")
            { bump_units = bump_units.max(0.35); }

            if l.contains("insufficient compute units") { bump_units = bump_units.max(0.20); }

            if l.contains("Heap") || l.contains("heap") || l.contains("request heap") || l.contains("HeapLimitExceeded") { need_heap = true; }
            if l.contains("Transaction too large")
                || l.contains("packet too large")
                || l.contains("too many account keys")
                || l.contains("account keys length")
                || l.contains("would exceed packet size")
            { drop_heap = true; }

            if l.contains("AccountInUse") || l.contains("write lock") { bump_units = bump_units.max(0.10); }
        }

        if bump_units == 0.0 && !need_heap && !drop_heap { return (units, heap, false); }

        if drop_heap && heap.is_some() { heap = None; changed = true; }

        if bump_units > 0.0 && units < MAX_COMPUTE_UNITS {
            let mut next = ((units as f64) * (1.0 + bump_units)).ceil() as u32;
            next = ((next + 1023) / 1024) * 1024; // quantize to 1k
            if next <= units { next = (units + 1024).min(MAX_COMPUTE_UNITS); }
            units = next.min(MAX_COMPUTE_UNITS);
            if units != units0 { changed = true; }
        }

        if !drop_heap && need_heap {
            let base = heap.unwrap_or(131_072);
            let next = base.max(131_072).saturating_mul(2).min(512_000);
            if heap != Some(next) { heap = Some(next); changed = true; }
        }
        (units, heap, changed)
    }

    /// === [IMPROVED: PIECE 1 v3] ===
    /// exact v0 sim with bounded backoff + jitter; resilient to transient RPC hiccups.
    pub async fn simulate_exact_v0_ok(&self, tx: &VersionedTransaction) -> Result<bool> {
        let cfg = RpcSimulateTransactionConfig { sig_verify: false, replace_recent_blockhash: true, ..Default::default() };

        // deterministic per-tx jitter (0..40ms) to avoid retry herding
        let j = {
            let bytes = bincode::serialize(tx).unwrap_or_default();
            let h = blake3::hash(&bytes);
            (u32::from_le_bytes(h.as_bytes()[..4].try_into().unwrap()) % 41) as u64
        };

        let mut attempt = 0;
        let mut timeout_ms = 280u64; // tight first budget
        loop {
            attempt += 1;
            let fut = self.rpc_client.simulate_transaction_with_config(tx, cfg.clone());
            match tokio::time::timeout(std::time::Duration::from_millis(timeout_ms + j), fut).await {
                Ok(Ok(out)) => return Ok(out.value.err.is_none()),
                Ok(Err(e)) => {
                    let msg = e.to_string();
                    let transient = msg.contains("Too many requests")
                        || msg.contains("Rate limit")
                        || msg.contains("deadline exceeded")
                        || msg.contains("Connection reset")
                        || msg.contains("timeout")
                        || msg.contains("502")
                        || msg.contains("503")
                        || msg.contains("504");
                    if transient && attempt < 2 {
                        timeout_ms = 420;
                        continue;
                    }
                    return Err(e.into());
                }
                Err(_) => {
                    if attempt < 2 {
                        timeout_ms = 420;
                        continue;
                    }
                    return Err(anyhow::anyhow!("simulate timeout"));
                }
            }
        }
    }

    /// === [IMPROVED: PIECE 1 v3] ===
    /// returns units_consumed with the same retry logic; robust log fallback if field missing.
    pub async fn simulate_exact_v0_units(&self, tx: &VersionedTransaction) -> Result<Option<u32>> {
        let cfg = RpcSimulateTransactionConfig { sig_verify: false, replace_recent_blockhash: true, ..Default::default() };

        let j = {
            let bytes = bincode::serialize(tx).unwrap_or_default();
            let h = blake3::hash(&bytes);
            (u32::from_le_bytes(h.as_bytes()[0..4].try_into().unwrap()) % 41) as u64
        };

        let mut attempt = 0;
        let mut timeout_ms = 280u64;
        let out = loop {
            attempt += 1;
            let fut = self.rpc_client.simulate_transaction_with_config(tx, cfg.clone());
            match tokio::time::timeout(std::time::Duration::from_millis(timeout_ms + j), fut).await {
                Ok(Ok(o)) => break o,
                Ok(Err(e)) => {
                    let msg = e.to_string();
                    let transient = msg.contains("Too many requests")
                        || msg.contains("Rate limit")
                        || msg.contains("deadline exceeded")
                        || msg.contains("Connection reset")
                        || msg.contains("timeout")
                        || msg.contains("502")
                        || msg.contains("503")
                        || msg.contains("504");
                    if transient && attempt < 2 {
                        timeout_ms = 420;
                        continue;
                    }
                    return Err(e.into());
                }
                Err(_) => {
                    if attempt < 2 { timeout_ms = 420; continue; }
                    return Err(anyhow::anyhow!("simulate timeout"));
                }
            }
        };

        if let Some(u) = out.value.units_consumed { return Ok(Some(u as u32)); }

        // fallback parse from logs (multiple vendor stylings)
        let mut units: Option<u32> = None;
        if let Some(ls) = out.value.logs.as_ref() {
            const KEYS: &[&str] = &[
                "consumed ", "compute units: ", "units consumed: ", "total units consumed: ",
                "compute units consumed: ", "Total compute units consumed: ", "units total: ",
                "CU total: ", "units=", "used: ", "used=", "cu_used=", "cu: ", "total cu: ",
            ];
            let tail = 256usize.min(ls.len());
            for line in ls[ls.len() - tail..].iter().rev() {
                let clean = self.strip_ansi(line);
                if let Some(v) = self.parse_num_after_any(&clean, KEYS).and_then(|n| self.filter_units(n)) {
                    units = Some(v); break;
                }
                if units.is_none() {
                    if let Some(v) = self.parse_last_num_if_hinted(&clean).and_then(|n| self.filter_units(n)) {
                        units = Some(v); break;
                    }
                }
            }
        }
        Ok(units)
    }

    /// === [IMPROVED: PIECE 2 v3] ===
    /// ultra-stable flow salt: skips CB/tips/payer, collapses dup metas, mixes head+tail+stride checksum.
    pub fn ixs_fee_salt(&self, ixs: &[Instruction]) -> u64 {
        use solana_program::system_instruction::SystemInstruction;
        let mut hasher = blake3::Hasher::new();
        let payer = self.wallet.pubkey();

        for ix in ixs {
            // ignore budgets and pure system transfer (tip)
            if ix.program_id == solana_sdk::compute_budget::id() { continue; }
            if ix.program_id == solana_program::system_program::id() {
                let is_tip = bincode::deserialize::<SystemInstruction>(&ix.data)
                    .ok()
                    .map(|si| matches!(si, SystemInstruction::Transfer { .. }))
                    .unwrap_or(false);
                if is_tip { continue; }
            }

            hasher.update(ix.program_id.as_ref());

            // collapse accidental consecutive dup metas and ignore payer meta
            let mut last: Option<(Pubkey, bool, bool)> = None;
            let mut kept = 0u32;
            for am in &ix.accounts {
                if am.pubkey == payer { continue; }
                let cur = (am.pubkey, am.is_signer, am.is_writable);
                if Some(cur) == last { continue; }
                hasher.update(am.pubkey.as_ref());
                hasher.update(&[am.is_signer as u8, am.is_writable as u8]);
                last = Some(cur);
                kept = kept.saturating_add(1);
            }
            hasher.update(&kept.to_le_bytes());

            if !ix.data.is_empty() {
                let len = ix.data.len() as u32;
                hasher.update(&len.to_le_bytes());
                let head = &ix.data[..ix.data.len().min(8)];
                let tail = if ix.data.len() > 8 { &ix.data[ix.data.len() - 8..] } else { head };
                hasher.update(head);
                hasher.update(tail);
                let mut x: u8 = 0; let mut i = 0usize; while i < ix.data.len() { x ^= ix.data[i]; i = i.saturating_add(16); }
                hasher.update(&[x]);
            }
        }
        let bytes = hasher.finalize();
        u64::from_le_bytes(bytes.as_bytes()[..8].try_into().unwrap())
    }

    /// === [IMPROVED: PIECE 3 v3] ===
    /// vol-aware headroom + two-sided dither + dynamic down-ramp limiter.
    pub fn apply_flow_ema(&self, salt: u64, guess_units: u32, guess_heap: Option<u32>) -> (u32, Option<u32>) {
        let mut map = self.flow_budget_ema.write();
        let ent = map.entry(salt).or_default();
        let prev = if ent.units_ema == 0.0 { guess_units as f64 } else { ent.units_ema };

        if ent.units_ema == 0.0 {
            ent.units_ema = guess_units as f64;
            ent.heap_hint = guess_heap;
        } else {
            let r = ((guess_units as f64) / ent.units_ema).max(1e-9);
            let logdiff = r.ln().abs().min(1.5);
            let alpha_up = (0.12 + 0.16 * (logdiff / 1.5)).min(0.28);
            let alpha_dn = (0.06 + 0.06 * (logdiff / 1.5)).min(0.12);
            if (guess_units as f64) > ent.units_ema {
                ent.units_ema = (1.0 - alpha_up) * ent.units_ema + alpha_up * (guess_units as f64);
            } else {
                ent.units_ema = (1.0 - alpha_dn) * ent.units_ema + alpha_dn * (guess_units as f64);
            }
            if let Some(h) = guess_heap { if ent.heap_hint.map_or(true, |old| h > old) { ent.heap_hint = Some(h); } }
        }

        let r_now = ((guess_units as f64) / prev.max(1.0)).max(1e-9);
        let logdiff = r_now.ln().abs().min(0.7);
        let mut head = Self::UNITS_HEADROOM * (1.0 + 0.18 * logdiff.exp().min(1.6) - 0.18);
        let lo = Self::UNITS_HEADROOM * 0.95;
        let hi = Self::UNITS_HEADROOM * 1.25;
        if head < lo { head = lo; } else if head > hi { head = hi; }

        let mut target = (ent.units_ema * head).ceil().max(20_000.0) as u32;

        // two-sided anti-herding dither around 256-CU boundaries
        let q: u32 = 256;
        let seed = (salt as u32).wrapping_mul(0x9E37_79B9);
        let jitter = seed & (q - 1);
        let half = q / 2;
        if jitter >= half { target = target.saturating_add(jitter - half); } else { target = target.saturating_sub(half - jitter); }

        let mut units = ((target + (q - 1)) / q) * q;

        // dynamic down-ramp: tighter when volatility is high
        let drop_cap = 1.0 - (0.08 + 0.10 * (logdiff / 0.7).min(1.0));
        let floor_drop = ((prev as f64) * drop_cap) as u32;
        if units < floor_drop { units = floor_drop; }

        (units.min(Self::MAX_UNITS).max(20_000), ent.heap_hint)
    }

    /// === [IMPROVED: PIECE 4 v3] ===
    /// EMA update with continuous alpha from |ln error| + Huber-style clipping.
    pub fn note_units_after_sim(&self, salt: u64, consumed: u32) {
        let mut map = self.flow_budget_ema.write();
        let ent = map.entry(salt).or_default();
        let c = consumed.min(Self::MAX_UNITS).max(20_000) as f64;

        if ent.units_ema == 0.0 {
            ent.units_ema = c;
            return;
        }

        let mut r = (c / ent.units_ema).max(1e-9);
        if r > 3.0 { r = 3.0; }
        if r < 1.0/3.0 { r = 1.0/3.0; }

        let abs_ln = r.ln().abs();
        let alpha = (0.08 + 0.18 * (abs_ln / (0.69)).min(1.0)).min(0.26);
        ent.units_ema = (1.0 - alpha) * ent.units_ema + alpha * c;
    }

    /// === [IMPROVED: PIECE 5 v3] ===
    /// budget/tip canon: merges CB (incl. opcode=3), ignores 0-lamport tips, clamps heap, CBs before tip.
    pub fn dedup_budget_and_tip(&self, ixs: Vec<Instruction>) -> Vec<Instruction> {
        use solana_program::system_instruction::SystemInstruction;

        let mut max_limit: Option<u32> = None;
        let mut max_price: Option<u64> = None;
        let mut max_heap:  Option<u32> = None;
        let mut max_acc_data: Option<u32> = None;
        let mut last_tip: Option<Instruction> = None;

        for ix in &ixs {
            if ix.program_id == solana_sdk::compute_budget::id() {
                if let Some(tag) = ix.data.first().copied() {
                    match tag {
                        0 => { // CU limit
                            if let Some(u) = ix.data.get(1..5).and_then(|d| bincode::deserialize::<u32>(d).ok()) {
                                max_limit = Some(max_limit.map_or(u.min(1_400_000), |cur| cur.max(u).min(1_400_000)));
                            }
                        }
                        1 => { // CU price
                            if let Some(p) = ix.data.get(1..9).and_then(|d| bincode::deserialize::<u64>(d).ok()) {
                                let p = p.max(1);
                                max_price = Some(max_price.map_or(p, |cur| cur.max(p)));
                            }
                        }
                        2 => { // heap
                            if let Some(h) = ix.data.get(1..5).and_then(|d| bincode::deserialize::<u32>(d).ok()) {
                                max_heap = Some(max_heap.map_or(h.min(512_000), |cur| cur.max(h).min(512_000)));
                            }
                        }
                        3 => { // accounts data limit (undocumented wire)
                            if let Some(a) = ix.data.get(1..5).and_then(|d| bincode::deserialize::<u32>(d).ok()) {
                                max_acc_data = Some(max_acc_data.map_or(a, |cur| cur.max(a)));
                            }
                        }
                        _ => {}
                    }
                }
            } else if ix.program_id == solana_program::system_program::id() {
                if let Ok(si) = bincode::deserialize::<SystemInstruction>(&ix.data) {
                    if let SystemInstruction::Transfer { lamports } = si {
                        if lamports > 0 { last_tip = Some(ix.clone()); }
                    }
                }
            }
        }

        let mut out: Vec<Instruction> = Vec::with_capacity(ixs.len());
        if let Some(u) = max_limit { out.push(ComputeBudgetInstruction::set_compute_unit_limit(u)); }
        if let Some(p) = max_price { out.push(ComputeBudgetInstruction::set_compute_unit_price(p)); }
        if let Some(h) = max_heap  { out.push(ComputeBudgetInstruction::request_heap_frame(h)); }
        if let Some(a) = max_acc_data {
            let mut data = Vec::with_capacity(1 + 4);
            data.push(3u8);
            data.extend_from_slice(&a.to_le_bytes());
            out.push(Instruction { program_id: solana_sdk::compute_budget::id(), accounts: vec![], data });
        }
        if let Some(t) = last_tip { out.push(t); }

        for ix in ixs.into_iter() {
            let is_cb  = ix.program_id == solana_sdk::compute_budget::id();
            let is_tip = if ix.program_id == solana_program::system_program::id() {
                bincode::deserialize::<SystemInstruction>(&ix.data)
                    .ok()
                    .map(|si| matches!(si, SystemInstruction::Transfer { .. }))
                    .unwrap_or(false)
            } else { false };
            if !is_cb && !is_tip { out.push(ix); }
        }
        out
    }

    /// ++ Ensure one canonical CB set (max values) and only the last tip; order-agnostic.
    #[inline]
    pub fn budget_ixs_for(
        &self,
        unit_limit: u32,
        cu_price: u64,
        heap_frame_bytes: Option<u32>,
    ) -> Vec<Instruction> {
        let mut out: SmallVec<[Instruction; 3]> = SmallVec::new();
        out.push(ComputeBudgetInstruction::set_compute_unit_limit(unit_limit.min(1_400_000)));
        out.push(ComputeBudgetInstruction::set_compute_unit_price(cu_price.max(1)));
        if let Some(bytes) = heap_frame_bytes { out.push(ComputeBudgetInstruction::request_heap_frame(bytes)); }
        out.into_vec()
    }

    /// ++ Drop any pre-existing compute budget/tip ixs so we always control them.
    pub fn normalize_budget_ixs(&self, mut ixs: Vec<Instruction>) -> Vec<Instruction> {
        use solana_program::system_instruction::SystemInstruction;
        ixs.retain(|ix| {
            if ix.program_id == solana_sdk::compute_budget::id() { return false; }
            if ix.program_id == solana_program::system_program::id() {
                return !bincode::deserialize::<SystemInstruction>(&ix.data)
                    .ok()
                    .map(|si| matches!(si, SystemInstruction::Transfer{..}))
                    .unwrap_or(false);
            }
            true
        });
        ixs
    }

    // --------- Size estimators (sync) for best-of chooser ---------
    #[inline]
    fn estimate_len_v0_sync(
        &self,
        payer: Pubkey,
        ixs: &[Instruction],
        bh: Hash,
        alts: &[AddressLookupTableAccount],
    ) -> Option<(VersionedTransaction, usize)> {
        self.build_v0_tx(payer, ixs, bh, alts)
            .ok()
            .and_then(|tx| bincode::serialize(&tx).ok().map(|b| (tx, b.len())))
    }

    #[inline]
    fn estimate_len_legacy_sync(
        &self,
        payer: Pubkey,
        ixs: &[Instruction],
        _bh: Hash,
    ) -> Option<(Transaction, usize)> {
        let msg = Message::new(ixs, Some(&payer));
        let tx = Transaction::new_unsigned(msg);
        bincode::serialize(&tx).ok().map(|b| (tx, b.len()))
    }

    // --------- Static lamport outlay and balance/profit clamp ---------
    #[inline]
    fn static_lamport_outlay(&self, ixs: &[Instruction]) -> u64 {
        use solana_program::system_instruction::SystemInstruction;
        use bincode::deserialize;
        ixs.iter()
            .filter_map(|ix| {
                if ix.program_id == solana_program::system_program::id() {
                    deserialize::<SystemInstruction>(&ix.data).ok().and_then(|si| match si {
                        SystemInstruction::Transfer { lamports } => Some(lamports),
                        _ => None,
                    })
                } else {
                    None
                }
            })
            .sum()
    }

    #[inline]
    fn clamp_cu_price_to_balance_and_profit(
        &self,
        payer_balance: u64,
        units_budget: u32,
        desired: u64,
        static_outlay: u64,
        max_priority_fee_lamports: Option<u64>,
    ) -> Result<u64> {
        if units_budget == 0 { return Ok(desired); }
        let free = payer_balance
            .saturating_sub(static_outlay)
            .saturating_sub(BALANCE_RESERVE_LAMPORTS);
        let max_by_balance = free / (units_budget as u64);
        if max_by_balance == 0 { return Err(anyhow!("insufficient balance for priority fee")); }
        let max_by_profit = max_priority_fee_lamports
            .map(|lim| lim / (units_budget as u64))
            .unwrap_or(u64::MAX);
        Ok(desired.min(max_by_balance).min(max_by_profit))
    }

    pub async fn execute_rebalance_flash_atomic(
        &self,
        position: &Position,
        _src_meta: &ReserveMeta,
        dst_meta: &ReserveMeta,
        amount: u64,
        alts: &[AddressLookupTableAccount],
    ) -> Result<solana_sdk::signature::Signature> {
        let user = self.wallet.pubkey();
        let user_flash_ata = spl_associated_token_account::get_associated_token_address(&user, &position.token_mint);

        // Flash plan on the destination program
        let plan = FlashPlan {
            flash_program: dst_meta.program_id,
            reserve: dst_meta.reserve,
            liquidity_supply_vault: dst_meta.liquidity_supply_vault,
            lending_market: dst_meta.lending_market,
            market_authority: dst_meta.lending_market_authority,
            token_mint: position.token_mint,
            amount,
        };

        // Legs
        let flash_start_ix = self.build_flash_loan_ix(&plan, user_flash_ata)?;
        // TODO: swap 5bps for on-chain read of fee_bps when wired
        let repay_amount = self.compute_flash_repay_amount(amount, 5);
        let flash_repay_ix = self.build_flash_repay_ix(&plan, user_flash_ata, repay_amount)?;
        let withdraw_old_ix = self.build_withdraw_instruction(position).await?;

        let dst_protocol = self.protocol_from_program_id(&dst_meta.program_id);
        let dst_stub = LendingMarket {
            market_pubkey: dst_meta.reserve,
            token_mint: position.token_mint,
            protocol: dst_protocol,
            supply_apy: 0.0, borrow_apy: 0.0,
            utilization: 0.0, liquidity_available: 0,
            last_update: Instant::now(),
            layout_version: 1,
        };
        let deposit_new_ix = self.build_deposit_instruction(position, &dst_stub).await?;

        // Core legs first
        let mut core_ixs = vec![flash_start_ix, withdraw_old_ix, deposit_new_ix, flash_repay_ix];

        // Zero-alloc core snapshot for the closure
        let core_ixs_arc: Arc<[Instruction]> = {
            let mut v = self.normalize_budget_ixs(core_ixs);
            v.shrink_to_fit();
            Arc::from(v)
        };

        // Sender v3 style using existing slot-guard API: we rebuild budgets per fresh blockhash
        let payer = self.wallet.pubkey();
        let dst_program = dst_meta.program_id;
        let build_with_hash = move |bh: Hash| {
            // Normalize (drop any preexisting CB ixs) and prepend final budget ixs
            // Use robust percentile for price and a conservative CU cap
            let fut = async {
                let flow_salt = self.ixs_fee_salt(&core_ixs_arc);
                let units_est = self.estimate_units(&core_ixs_arc).await;
                let mut units_guess = ((units_est as f64) * 1.15).ceil() as u32;
                // Scoped percentile for tighter pricing
                let price_base =  self.percentile_cu_price_scoped(&[dst_program, spl_token::id()], PRIORITY_FEE_LAMPORTS, 0.90).await;
                let (units0, heap0) = self.apply_flow_ema(flow_salt, units_guess, None);

                // Tip seeded by payer+bh (stable within slot)
                let mut tip_ix: Option<Instruction> = None;
                if let Some(router) = self.tip_router.read().await.as_ref() {
                    let tip_acc = router.pick_seeded(&payer, &bh);
                    let tip_base = self.percentile_cu_price_scoped(&[dst_program, spl_token::id()], PRIORITY_FEE_LAMPORTS, 0.75).await.max(10_000);
                    tip_ix = Some(system_instruction::transfer(&payer, &tip_acc, tip_base));
                }

                // Balance/profit clamps
                let payer_balance = self.rpc_client.get_balance(&payer).await.unwrap_or(0);
                let static_outlay = self.static_lamport_outlay(&core_ixs_arc);
                let mut cu_price = self.clamp_cu_price_to_balance_and_profit(
                    payer_balance, units0, price_base, static_outlay, None,
                )?;
                // Optional extra profit guard (no-op if huge bound)
                cu_price = self.profit_guard_cu_price(u64::MAX, units0, cu_price, static_outlay).unwrap_or(cu_price);
                // Jitter the fee slightly based on payer+blockhash
                cu_price = self.jittered_fee(cu_price, &payer, &bh);

                // Build ixs on stack via SmallVec
                let mut ixs_v0: SmallVec<[Instruction; 8]> = SmallVec::new();
                let mut budget = self.budget_ixs_for(units0, cu_price, heap0);
                ixs_v0.extend(budget.drain(..));
                if let Some(ix) = tip_ix { ixs_v0.push(ix); }
                ixs_v0.extend(core_ixs_arc.iter().cloned());

                // Subset-pruned v0 candidate
                let tip_acc_for_plan = if let Some(router) = self.tip_router.read().await.as_ref() { Some(router.pick_seeded(&payer, &bh)) } else { None };
                let alts_min = self.alts_plan_for_core(&core_ixs_arc, payer, bh, alts, tip_acc_for_plan);
                let cand_v0 = self.best_subset_v0_sync(payer, &ixs_v0, bh, &alts_min)
                    .ok_or_else(|| anyhow!("failed to build any v0 subset"))?;
                let (mut tx, mut tx_len, kept_cnt) = (cand_v0.0, cand_v0.1, cand_v0.2);

                // UDP squeeze: if near cap, try removing TIP first, then HEAP
                if self.near_udp_cap(tx_len) {
                    // try without tip
                    let mut ixs_try: SmallVec<[Instruction; 8]> = SmallVec::new();
                    let mut budget2 = self.budget_ixs_for(units0, cu_price, heap0);
                    // do not push tip here
                    ixs_try.extend(budget2.drain(..));
                    ixs_try.extend(core_ixs_arc.iter().cloned());
                    if let Some((tx2, l2, _)) = self.best_subset_v0_sync(payer, &ixs_try, bh, &alts_min) {
                        if l2 <= MAX_UDP_TX_BYTES { tx = tx2; tx_len = l2; }
                    }

                    // then try without heap if still near cap
                    if heap0.is_some() && self.near_udp_cap(tx_len) {
                        let mut ixs_try2: SmallVec<[Instruction; 8]> = SmallVec::new();
                        let mut budget3 = self.budget_ixs_for(units0, cu_price, None);
                        ixs_try2.extend(budget3.drain(..));
                        ixs_try2.extend(core_ixs_arc.iter().cloned());
                        if let Some((tx3, l3, _)) = self.best_subset_v0_sync(payer, &ixs_try2, bh, &alts_min) {
                            if l3 <= MAX_UDP_TX_BYTES { tx = tx3; tx_len = l3; }
                        }
                    }
                }

                // Learn units on exact sim & guard duplicates
                if let Ok(Some(u)) = self.simulate_exact_v0_units(&tx).await { self.note_units_after_sim(flow_salt, u); }
                if let Some(d) = self.digest_v0_fast(&tx) {
                    if !self.seen_or_insert_digest(d) {
                        let ladder = if self.near_udp_cap(tx_len) { [0u64, 60, 180, 300] } else { [0u64, 60, 180] };
                        for step in ladder {
                            let price2 = cu_price.saturating_add(step);
                            let (u2, h2) = self.apply_flow_ema(flow_salt, units0, heap0);
                            let mut ixs_v0b: SmallVec<[Instruction; 8]> = SmallVec::new();
                            let mut b2 = self.budget_ixs_for(u2, price2, h2);
                            ixs_v0b.extend(b2.drain(..));
                            if let Some(ix) = tip_ix.clone() { ixs_v0b.push(ix); }
                            ixs_v0b.extend(core_ixs_arc.iter().cloned());
                            if let Some((tx2, l2, _)) = self.best_subset_v0_sync(payer, &ixs_v0b, bh, &alts_min) { tx = tx2; tx_len = l2; break; }
                        }
                    }
                }
                Ok::<_, anyhow::Error>(tx)
            };
            // We must block in this closure; use block_in_place
            tokio::task::block_in_place(|| tokio::runtime::Handle::current().block_on(fut))
        };

        let sig = self
            .send_v0_with_slot_guard(build_with_hash, Duration::from_secs(45), 200, 120)
            .await?;
        Ok(sig)
    }

    // Resilient flash rebalance across multiple sources with metrics and circuit breaker
    pub async fn execute_flash_rebalance_resilient(
        &self,
        position: &Position,
        flash_sources: &[ReserveMeta],
        dst_meta: &ReserveMeta,
        amount: u64,
        tip_router: &TipRouter,
        obs: &Observability,
        breaker: &mut CircuitBreaker,
    ) -> Result<solana_sdk::signature::Signature> {
        obs.rebalance_attempts.inc();
        if flash_sources.is_empty() {
            return Err(anyhow!("no flash sources available"));
        }

        // Split across sources greedily with de-dup by reserve, then sort by (fee_bps ASC, capacity DESC)
        let mut uniq = std::collections::HashSet::new();
        let mut remaining = amount;
        let mut chunks: Vec<(ReserveMeta, u64)> = Vec::new();
        for src in flash_sources.iter().cloned() {
            if !uniq.insert(src.reserve) { continue; }
            if remaining == 0 { break; }
            let cap = src.available_flash_capacity()?;
            let take = remaining.min(cap);
            if take > 0 { chunks.push((src, take)); remaining -= take; }
        }
        if remaining > 0 {
            warn!(missing = remaining, "insufficient flash capacity across sources");
            return Err(anyhow!("insufficient flash capacity"));
        }

        chunks.sort_by(|(a, av), (b, bv)| {
            let fa = a.flash_fee_bps().unwrap_or(0);
            let fb = b.flash_fee_bps().unwrap_or(0);
            fa.cmp(&fb).then(bv.cmp(av))
        });

        // Build instructions (fee-aware start legs first) using a single ATA
        let payer = self.wallet.pubkey();
        let user_flash_ata = spl_associated_token_account::get_associated_token_address(&payer, &position.token_mint);
        let mut ixs: SmallVec<[Instruction; 16]> = SmallVec::new();
        let mut _repay_sum = 0u64;
        for (src, take) in chunks.iter() {
            let fee_bps = src.flash_fee_bps()?;
            let repay = self.compute_flash_repay_amount(*take, fee_bps);
            _repay_sum = _repay_sum.saturating_add(repay);
            ixs.push(self.build_flash_loan_ix(&FlashPlan {
                flash_program: src.reserve_owner_program(),
                reserve: src.reserve,
                liquidity_supply_vault: src.liquidity_supply_vault,
                lending_market: src.lending_market,
                market_authority: src.lending_market_authority,
                token_mint: position.token_mint,
                amount: *take,
            }, user_flash_ata)?);
        }

        // Core legs in the middle (withdraw old, deposit new)
        let withdraw_old = self.build_withdraw_instruction(position).await?;
        let dst_protocol = self.protocol_from_program_id(&dst_meta.program_id);
        let dst_stub = LendingMarket {
            market_pubkey: dst_meta.reserve,
            token_mint: position.token_mint,
            protocol: dst_protocol,
            supply_apy: 0.0, borrow_apy: 0.0,
            utilization: 0.0, liquidity_available: 0,
            last_update: Instant::now(),
            layout_version: 1,
        };
        let deposit_new = self.build_deposit_instruction(position, &dst_stub).await?;
        ixs.push(withdraw_old);
        ixs.push(deposit_new);

        // Repay legs last (mirroring starts' order)
        for (src, take) in chunks.iter() {
            let fee_bps = src.flash_fee_bps()?;
            let repay = self.compute_flash_repay_amount(*take, fee_bps);
            ixs.push(self.build_flash_repay_ix(&FlashPlan {
                flash_program: src.reserve_owner_program(),
                reserve: src.reserve,
                liquidity_supply_vault: src.liquidity_supply_vault,
                lending_market: src.lending_market,
                market_authority: src.lending_market_authority,
                token_mint: position.token_mint,
                amount: *take,
            }, user_flash_ata, repay)?);
        }

        // Freeze to Arc once for deterministic build path
        let core_ixs: Arc<[Instruction]> = {
            let mut v = ixs.into_vec();
            v.shrink_to_fit();
            Arc::from(v)
        };

        // ALT-aware builder with seeded tip and scoped fee percentiles
        let alts_empty: [AddressLookupTableAccount; 0] = [];
        let build_with_hash = {
            let core_ixs = core_ixs.clone();
            let dst_pid = dst_meta.program_id;
            move |bh: Hash| {
                let this = self;
                let core_ixs = core_ixs.clone();
                let payer = this.wallet.pubkey();
                let fut = async move {
                    // Deterministic tip selection per slot
                    let mut tip_ix: Option<Instruction> = None;
                    if let Some(router) = this.tip_router.read().await.as_ref() {
                        let tip_acc = router.pick_seeded(&payer, &bh);
                        let tip_amt = this
                            .percentile_cu_price_scoped(&[dst_pid, spl_token::id()], DEFAULT_PRIORITY_FEE_PER_CU, 0.75)
                            .await
                            .max(10_000);
                        tip_ix = Some(system_instruction::transfer(&payer, &tip_acc, tip_amt));
                    }

                    // Baseline CU and price
                    let units_guess = this.optimize_compute_units_rough(&core_ixs).await.min(MAX_COMPUTE_UNITS);
                    let base = this
                        .percentile_cu_price_scoped(&[dst_pid, spl_token::id()], DEFAULT_PRIORITY_FEE_PER_CU, 0.90)
                        .await;
                    let mut cu_price0 = base;

                    // Clamp by balance/profit
                    let payer_balance = this.rpc_client.get_balance(&payer).await.unwrap_or(0);
                    let static_outlay = this.static_lamport_outlay(&core_ixs);
                    let mut cu_price = this
                        .clamp_cu_price_to_balance_and_profit(payer_balance, units_guess, cu_price0, static_outlay, None)
                        .unwrap_or(cu_price0);
                    cu_price = this.jittered_fee(cu_price, &payer, &bh);

                    // Build with budget + optional tip
                    let mut ixs_budgeted: SmallVec<[Instruction; 8]> = SmallVec::new();
                    ixs_budgeted.push(ComputeBudgetInstruction::set_compute_unit_limit(units_guess));
                    ixs_budgeted.push(ComputeBudgetInstruction::set_compute_unit_price(cu_price));
                    if let Some(ix) = tip_ix.clone() { ixs_budgeted.push(ix); }
                    ixs_budgeted.extend(core_ixs.iter().cloned());

                    // ALT plan via need-set cache with fast prefilter
                    let need_fp = this.needset_fp(&ixs_budgeted);
                    let mut alts_pref = this.greedy_min_alts(&ixs_budgeted, &alts_empty);
                    if let Some(cached) = this.alt_need_get(0, need_fp, &alts_pref) { // flow_salt=0 scoped since not tracked here
                        alts_pref = cached;
                    } else {
                        this.alt_need_put(0, need_fp, &alts_pref);
                    }
                    let alts_min = alts_pref;
                    let cand = this.best_subset_v0_sync(payer, &ixs_budgeted, bh, &alts_min)
                        .ok_or_else(|| anyhow!("failed to build any v0 candidate"))?;
                    let mut vtx = cand.0;
                    let mut tx_len = cand.1;

                    // UDP squeeze: try dropping TIP first (no heap requested in this builder)
                    if this.near_udp_cap(tx_len) {
                        let mut pre: SmallVec<[Instruction; 8]> = SmallVec::new();
                        let mut b2 = this.budget_ixs_for(units_guess, cu_price, None);
                        pre.extend(b2.drain(..));
                        // no tip here
                        pre.extend(core_ixs.iter().cloned());
                        let alts_min = this.greedy_min_alts(&pre, &alts_empty);
                        if let Some((tx2, l2, _)) = this.best_subset_v0_sync(payer, &pre, bh, &alts_min) {
                            if l2 <= MAX_UDP_TX_BYTES { vtx = tx2; tx_len = l2; }
                        }
                    }

                    // Learn units via exact sim; duplicate digest guard with slight nudge
                    if let Ok(Some(u)) = this.simulate_exact_v0_units(&vtx).await { this.note_units_after_sim(0, u); }
                    if let Some(d) = this.digest_v0_fast(&vtx) {
                        if !this.seen_or_insert_digest(d) {
                            let ladder = if this.near_udp_cap(tx_len) { [0u64, 60, 180, 300] } else { [0u64, 60, 180] };
                            for step in ladder {
                                let mut pre: SmallVec<[Instruction; 8]> = SmallVec::new();
                                pre.push(ComputeBudgetInstruction::set_compute_unit_limit(units_guess));
                                pre.push(ComputeBudgetInstruction::set_compute_unit_price(cu_price.saturating_add(step)));
                                if let Some(ix) = tip_ix { pre.push(ix); }
                                pre.extend(core_ixs.iter().cloned());
                                let alts_min = this.alts_plan_for_core(&core_ixs, payer, bh, &[], tip_ix.as_ref().map(|ix| if let solana_program::system_instruction::SystemInstruction::Transfer{..} = bincode::deserialize::<solana_program::system_instruction::SystemInstruction>(&ix.data).ok().unwrap_or(solana_program::system_instruction::SystemInstruction::AdvanceNonceAccount) { ix.accounts[1].pubkey } else { payer } ));
                                if let Some((tx2, _l2, _)) = this.best_subset_v0_sync(payer, &pre, bh, &alts_min) { vtx = tx2; break; }
                            }
                        }
                    }
                    Ok::<_, anyhow::Error>(vtx)
                };
                tokio::task::block_in_place(|| tokio::runtime::Handle::current().block_on(fut))
            }
        };

        // Exact simulation safety (fast fail)
        let (bh_now, _) = self.rpc_client.get_latest_blockhash().await?;
        let vtx_preview = build_with_hash(bh_now)?;
        if let Err(e) = self.simulate_exact_v0(&vtx_preview).await {
            error!(?e, "flash rebalance sim failed");
            let ok_to_continue = breaker.record(false);
            if !ok_to_continue { return Err(anyhow!("breaker open: too many failures")); }
            return Err(anyhow!(format!("sim failed: {e}")));
        }

        // Send with slot guard and measure tip latency
        let t0 = Instant::now();
        let sig = self
            .send_v0_with_slot_guard(build_with_hash, Duration::from_secs(45), 200, 120)
            .await?;
        let ms = t0.elapsed().as_millis() as f64;
        obs.tip_latency_ms.observe(ms);
        info!(%sig, "flash rebalance executed");
        obs.rebalance_success.inc();
        Ok(sig)
    }
    }

    pub async fn build_bundle_from_plans(
        &self,
        mut plans: Vec<Vec<Instruction>>,
        alts: &[AddressLookupTableAccount],
        tip_account: Pubkey,
        tip_lamports: u64,
    ) -> Result<Vec<VersionedTransaction>> {
        self.ensure_tip_funds(tip_lamports).await?;
        let bh = self.get_recent_blockhash_with_retry().await?;
        let mut out = Vec::with_capacity(plans.len());

        for mut ixs in plans.drain(..) {
            let (cu_ix, fee_ix) = self.dynamic_budget_ixs(COMPUTE_UNITS, PRIORITY_FEE_LAMPORTS).await;
            ixs.splice(0..0, [cu_ix, fee_ix].into_iter());

            // add tip last
            ixs.push(system_instruction::transfer(&self.wallet.pubkey(), &tip_account, tip_lamports));
            let ixs = Self::dedup_budget_and_tip(ixs);

            // compute plan-specific minimal ALTs once
            let alts_plan = self.greedy_min_alts(&ixs, alts);
            let (tx0, len0) = match self.estimate_len_v0_sync(self.wallet.pubkey(), &ixs, bh, &alts_plan) {
                Some((vtx, l)) => (vtx, l),
                None => (self.build_v0_tx(self.wallet.pubkey(), &ixs, bh, &alts_plan)?, 0),
            };

            let vtx = if len0 > 0 && self.near_udp_cap(len0) {
                // drop tip when near UDP cap
                let mut ixs2: Vec<Instruction> = ixs
                    .into_iter()
                    .filter(|ix| ix.program_id != solana_program::system_program::id())
                    .collect();
                let ixs2 = Self::dedup_budget_and_tip(ixs2);
                self.build_v0_tx(self.wallet.pubkey(), &ixs2, bh, &alts_plan)?
            } else { tx0 };

            out.push(vtx);
        }
        Ok(out)
    }

    fn select_tip_account(&self, cfg: &TipConfig, counter: &mut usize) -> Pubkey {
        match cfg.strategy {
            TipStrategy::RoundRobin => {
                // Prefer latency-weighted router if initialized; else fallback to round-robin
                if let Some(router) = self.tip_router.blocking_read().as_ref() {
                    return router.pick_sync();
                }
                let idx = *counter % cfg.accounts.len();
                *counter += 1;
                cfg.accounts[idx]
            }
            TipStrategy::Fixed(i) => cfg.accounts[i.min(cfg.accounts.len() - 1)],
        }
    }

    async fn ensure_tip_router(&self, cfg: &TipConfig) {
        let mut guard = self.tip_router.write().await;
        if guard.is_none() {
            *guard = Some(TipRouter::new(cfg.accounts.clone()));
        }
    }

    async fn observe_tip_landing(&self, account: Pubkey, sent_at: Instant, confirmed_at: Instant) {
        let ms = (confirmed_at.saturating_duration_since(sent_at).as_millis() as f64).max(0.0);
        if let Some(router) = self.tip_router.read().await.as_ref() {
            router.record_attempt(account, true, Some(ms)).await;
        }
    }

    async fn ensure_tip_funds(&self, min_balance: u64) -> Result<()> {
        let bal = self.rpc_client.get_balance(&self.wallet.pubkey()).await?;
        if bal < min_balance {
            return Err(anyhow!(format!(
                "insufficient balance for tipping: have {}, need {}",
                bal, min_balance
            )));
        }
        Ok(())
    }

    async fn confirm_sig_with_backoff(
        &self,
        sig: &solana_sdk::signature::Signature,
        timeout: Duration,
    ) -> Result<bool> {
        let start = Instant::now();
        let cfg = RpcSignatureStatusConfig { search_transaction_history: true };
        let mut delay = Duration::from_millis(160);
        loop {
            if start.elapsed() >= timeout { return Ok(false); }
            match self.rpc_client.get_signature_statuses_with_config(&[*sig], cfg).await {
                Ok(resp) => {
                    if let Some(Some(st)) = resp.value.first() { return Ok(st.err.is_none()); }
                }
                Err(_) => { /* transient */ }
            }
            tokio::time::sleep(delay).await;
            delay = std::cmp::min(delay * 2, Duration::from_millis(1000));
        }
    }
    }

    pub async fn run(&self) -> Result<()> {
        use tokio::time::{interval_at, Instant, Duration, MissedTickBehavior};
        use futures::FutureExt;
        use std::sync::{OnceLock, atomic::{AtomicU64, Ordering}};
        use std::time::{SystemTime, UNIX_EPOCH};

        static LAST_LOG_TS: OnceLock<AtomicU64> = OnceLock::new();
        #[inline]
        fn can_log_now() -> bool {
            let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_secs();
            let a = LAST_LOG_TS.get_or_init(|| AtomicU64::new(0));
            let prev = a.load(Ordering::Relaxed);
            if now.saturating_sub(prev) >= 5 { a.store(now, Ordering::Relaxed); true } else { false }
        }

        let me = self.wallet.pubkey();
        let mut z: u64 = 0; for &b in me.as_ref().iter().take(8) { z = (z << 8) | (b as u64); }
        let start = Instant::now() + Duration::from_millis((z % 700) as u64);

        let mut iv = interval_at(start, Duration::from_secs(5));
        iv.set_missed_tick_behavior(MissedTickBehavior::Skip);

        loop {
            iv.tick().await;
            z = z.wrapping_mul(6364136223846793005).wrapping_add(1);
            let jitter = (z % 101) as i64 - 50;
            if jitter != 0 { tokio::time::sleep(Duration::from_millis(jitter.unsigned_abs() as u64)).await; }

            let fut = self.optimize_cycle().catch_unwind();
            match tokio::time::timeout(Duration::from_secs(6), fut).await {
                Ok(Ok(Ok(_)))    => {}
                Ok(Ok(Err(e)))   => if can_log_now() { eprintln!("[warn] optimize_cycle error: {e:?}"); }
                Ok(Err(_panic))  => if can_log_now() { eprintln!("[warn] optimize_cycle panicked; continuing"); }
                Err(_)           => if can_log_now() { eprintln!("[warn] optimize_cycle timed out (cancelled)"); }
            }
        }
    }

    async fn optimize_cycle(&self) -> Result<()> {
        self.update_market_data().await?;
        
        let positions = self.active_positions.read().await.clone();
        let markets = self.markets.read().await.clone();
        
        for (token_mint, position) in positions.iter() {
            if let Some(token_markets) = markets.get(token_mint) {
                if let Some(opportunity) = self.find_arbitrage_opportunity(position, token_markets).await? {
                    self.execute_rebalance(position, opportunity).await?;
                }
            }
        }
        
        Ok(())
    }

    async fn update_market_data(&self) -> Result<()> {
        // Fetch concurrently and rebuild map keyed by token mint
        let (solend_markets, kamino_markets) = tokio::try_join!(
            self.fetch_solend_markets(),
            self.fetch_kamino_markets(),
        )?;
        let mut new_map: HashMap<String, Vec<LendingMarket>> = HashMap::new();
        for m in solend_markets.into_iter().chain(kamino_markets.into_iter()) {
            let key = m.token_mint.to_string();
            new_map.entry(key).or_default().push(m);
        }
        let mut markets = self.markets.write().await;
        *markets = new_map;
        Ok(())
    }

    async fn fetch_solend_markets(&self) -> Result<Vec<LendingMarket>> {
        let program_id = *SOLEND_PROGRAM_ID;
        let accounts = self.rpc_client.get_program_accounts(&program_id).await?;
        
        let mut markets = Vec::new();
        
        for (pubkey, account) in accounts {
            if account.data.len() >= 600 {
                let market = self.parse_solend_reserve(&pubkey, &account.data).await?;
                markets.push(market);
            }
        }
        
        Ok(markets)
    }

    async fn fetch_kamino_markets(&self) -> Result<Vec<LendingMarket>> {
        let program_id = *KAMINO_PROGRAM_ID;
        let accounts = self.rpc_client.get_program_accounts(&program_id).await?;
        
        let mut markets = Vec::new();
        
        for (pubkey, account) in accounts {
            if account.data.len() >= 800 {
                let market = self.parse_kamino_market(&pubkey, &account.data).await?;
                markets.push(market);
            }
        }
        
        Ok(markets)
    }

    // ---- Layout guards (document the wire you touch) ----
    const SOLEND_OFF_MINT: usize = 32;
    const SOLEND_OFF_AVAIL: usize = 64;   // u64
    const SOLEND_OFF_BORROW: usize = 72;  // u64
    const SOLEND_OFF_SPLYRT: usize = 232; // u128
    const SOLEND_OFF_BORRT: usize = 248;  // u128
    const SOLEND_MIN_BYTES: usize = SOLEND_OFF_BORRT + 16; // 264

    const KAMINO_OFF_MINT: usize = 32;
    const KAMINO_OFF_AVAIL: usize = 80;   // u64
    const KAMINO_OFF_BORROW: usize = 88;  // u64
    const KAMINO_OFF_SPLYRT: usize = 296; // u128
    const KAMINO_OFF_BORRT: usize = 312;  // u128
    const KAMINO_MIN_BYTES: usize = KAMINO_OFF_BORRT + 16; // 328

    #[inline]
    fn le_u64(data: &[u8], off: usize) -> Result<u64> {
        let s = data.get(off..off+8).ok_or_else(|| anyhow!(format!("slice u64 @{}", off)))?;
        Ok(u64::from_le_bytes(s.try_into()?))
    }
    #[inline]
    fn le_u128(data: &[u8], off: usize) -> Result<u128> {
        let s = data.get(off..off+16).ok_or_else(|| anyhow!(format!("slice u128 @{}", off)))?;
        Ok(u128::from_le_bytes(s.try_into()?))
    }
    #[inline]
    fn pk_at(&self, data: &[u8], off: usize) -> Result<Pubkey> {
        let s = data.get(off..off+32).ok_or_else(|| anyhow!(format!("slice pk @{}", off)))?;
        Ok(Pubkey::new_from_array(s.try_into()?))
    }

    async fn parse_solend_reserve_with_spw(&self, pubkey: &Pubkey, data: &[u8], spw: f64) -> Result<LendingMarket> {
        if data.len() < SOLEND_MIN_BYTES { anyhow::bail!("solend: too small: {}", data.len()); }
        let version = data[0];
        if version == 0 || version > 5 { anyhow::bail!("solend: unexpected version {}", version); }

        let token_mint          = self.pk_at(data, SOLEND_OFF_MINT)?;
        let available_liquidity = Self::le_u64(data, SOLEND_OFF_AVAIL)?;
        let borrowed_amount     = Self::le_u64(data, SOLEND_OFF_BORROW)?;
        let supply_rate_raw     = Self::le_u128(data, SOLEND_OFF_SPLYRT)?;
        let borrow_rate_raw     = Self::le_u128(data, SOLEND_OFF_BORRT)?;

        let total_liquidity = available_liquidity.saturating_add(borrowed_amount);
        let utilization = if total_liquidity > 0 { borrowed_amount as f64 / total_liquidity as f64 } else { 0.0 };
        let supply_apy = self.apy_from_rate_per_slot_with_spw(supply_rate_raw, spw)?;
        let borrow_apy = self.apy_from_rate_per_slot_with_spw(borrow_rate_raw, spw)?;
        Ok(LendingMarket { market_pubkey: *pubkey, token_mint, protocol: Protocol::Solend, supply_apy, borrow_apy, utilization, liquidity_available: available_liquidity, last_update: Instant::now(), layout_version: version })
    }

    async fn parse_kamino_market_with_spw(&self, pubkey: &Pubkey, data: &[u8], spw: f64) -> Result<LendingMarket> {
        if data.len() < KAMINO_MIN_BYTES { anyhow::bail!("kamino: too small: {}", data.len()); }
        let version = data[0];
        if version == 0 || version > 10 { anyhow::bail!("kamino: unexpected version {}", version); }

        let token_mint          = self.pk_at(data, KAMINO_OFF_MINT)?;
        let available_liquidity = Self::le_u64(data, KAMINO_OFF_AVAIL)?;
        let borrowed_amount     = Self::le_u64(data, KAMINO_OFF_BORROW)?;
        let supply_rate_raw     = Self::le_u128(data, KAMINO_OFF_SPLYRT)?;
        let borrow_rate_raw     = Self::le_u128(data, KAMINO_OFF_BORRT)?;

        let total_liquidity = available_liquidity.saturating_add(borrowed_amount);
        let utilization = if total_liquidity > 0 { borrowed_amount as f64 / total_liquidity as f64 } else { 0.0 };
        let supply_apy = self.apy_from_rate_per_slot_with_spw(supply_rate_raw, spw)?;
        let borrow_apy = self.apy_from_rate_per_slot_with_spw(borrow_rate_raw, spw)?;
        Ok(LendingMarket { market_pubkey: *pubkey, token_mint, protocol: Protocol::Kamino, supply_apy, borrow_apy, utilization, liquidity_available: available_liquidity, last_update: Instant::now(), layout_version: version })
    }

    // Legacy signatures (unchanged)
    async fn parse_solend_reserve(&self, pubkey: &Pubkey, data: &[u8]) -> Result<LendingMarket> {
        let spw = self.estimate_slots_per_year_cached().await;
        self.parse_solend_reserve_with_spw(pubkey, data, spw).await
    }
    async fn parse_kamino_market(&self, pubkey: &Pubkey, data: &[u8]) -> Result<LendingMarket> {
        let spw = self.estimate_slots_per_year_cached().await;
        self.parse_kamino_market_with_spw(pubkey, data, spw).await
    }

    // === [IMPROVED: PIECE 6 v6] === sticky cached estimate with small-sample smoothing
    static SPW_CACHE: OnceLock<tokio::sync::RwLock<(Instant, f64)>> = OnceLock::new();
    async fn estimate_slots_per_year_cached(&self) -> f64 {
        use tokio::sync::RwLock;
        const TTL: Duration = Duration::from_secs(60);
        let cell = SPW_CACHE.get_or_init(|| RwLock::new((Instant::now() - TTL*2, 63_072_000.0)));

        // fast path
        {
            let g = cell.read().await;
            if g.0.elapsed() < TTL { return g.1; }
        }

        // refresh (EWMA over ≤5 samples)
        let mut spw = 63_072_000.0;
        if let Ok(mut samples) = self.rpc_client.get_recent_performance_samples(Some(5)).await {
            let mut num = 0.0; let mut den = 0.0; let mut w = 1.0;
            for s in samples.drain(..) {
                let sps = (s.num_slots as f64) / (s.sample_period_secs as f64).max(1.0);
                num += (31_536_000.0 * sps) * w; den += w; w *= 0.75;
            }
            let cand = if den > 0.0 { num / den } else { spw };
            if cand.is_finite() && (1.0..200_000_000.0).contains(&cand) { spw = cand; }
        } else {
            // fallback: wall-time delta over 10k slots
            let sample = 10_000u64;
            if let Ok(end_slot) = self.rpc_client.get_slot().await {
                if let (Ok(t1), Ok(t0)) = (
                    self.rpc_client.get_block_time(end_slot).await,
                    self.rpc_client.get_block_time(end_slot.saturating_sub(sample)).await,
                ) {
                    if let (Some(t1), Some(t0)) = (t1, t0) {
                        let dt = (t1 - t0).max(1) as f64;
                        spw = 31_536_000.0 * (sample as f64 / dt);
                    }
                }
            }
        }

        *cell.write().await = (Instant::now(), spw);
        spw
    }

    #[inline]
    fn apy_from_rate_per_slot_with_spw(&self, rate_per_slot_1e18: u128, spw: f64) -> Result<f64> {
        let r = (rate_per_slot_1e18 as f64) / 1e18;
        if !(r >= 0.0 && r.is_finite()) { return Err(anyhow!(format!("invalid per-slot rate: {}", r))); }
        let apy = f64::exp_m1(r * spw).clamp(-0.5, 10.0); // [-50%, +1000%]
        if !apy.is_finite() { return Err(anyhow!("apy non-finite")); }
        Ok(apy * 100.0)
    }

    pub async fn apy_continuous_from_rate_per_slot_1e18(&self, rate_per_slot_1e18: u128) -> Result<f64> {
        let spw = self.estimate_slots_per_year_cached().await;
        self.apy_from_rate_per_slot_with_spw(rate_per_slot_1e18, spw)
    }

    // Backwards-compat helper for tests and legacy callers (sync fallback).
    // Uses continuous compounding with a constant slots/year when async not available.
    pub fn calculate_apy_from_rate(&self, rate_per_slot_1e18: u64) -> f64 {
        let r = (rate_per_slot_1e18 as f64) / 1e18;
        if !(r >= 0.0 && r.is_finite()) { return 0.0; }
        let spw = 63_072_000.0;
        let apy = (r * spw).exp() - 1.0;
        let apy = apy.clamp(-0.5, 10.0);
        if !apy.is_finite() { return 0.0; }
        apy * 100.0
    }

    // === [IMPROVED: PIECE BH UTILS v40] === hedged (hash,height) pair
    pub async fn get_fresh_blockhash(&self) -> Result<(Hash, u64)> {
        let fetch = async {
            let bh = self.rpc_client.get_latest_blockhash();
            let h  = self.rpc_client.get_block_height();
            tokio::try_join!(bh, h)
        };
        let hedged = if let Some(fast) = &self.rpc_fast {
            tokio::select! {
                r1 = fetch => r1,
                r2 = async {
                    let bh = fast.get_latest_blockhash();
                    let h  = fast.get_block_height();
                    tokio::try_join!(bh, h)
                } => r2,
            }
        } else { fetch.await };

        match hedged {
            Ok((bh, h)) => Ok((bh, h)),
            Err(e) => Err(anyhow::anyhow!(format!("fresh blockhash/height failed: {e}"))),
        }
    }

    async fn find_arbitrage_opportunity(
        &self,
        position: &Position,
        markets: &[LendingMarket],
    ) -> Result<Option<LendingMarket>> {
        // current market
        let current = markets
            .iter()
            .find(|m| m.protocol == position.protocol)
            .ok_or_else(|| anyhow!("Current market not found"))?;

        // in-place best alt, hard gate on liquidity
        let mut best_alt: Option<&LendingMarket> = None;
        let mut best_diff_bps: i64 = 0;
        for m in markets {
            if m.protocol == position.protocol { continue; }
            if m.liquidity_available < position.amount { continue; }
            let diff_bps = (((m.supply_apy - current.supply_apy) * 100.0).round()) as i64;
            if diff_bps > best_diff_bps { best_diff_bps = diff_bps; best_alt = Some(m); }
        }
        if best_diff_bps as u64 <= REBALANCE_THRESHOLD_BPS { return Ok(None); }
        let alt = match best_alt { Some(a) => a, None => return Ok(None) };

        // optional reserve meta gates
        if let Some(meta) = self.reserve_meta_for(alt.market_pubkey).await.transpose()? {
            let fee_bps = meta.flash_fee_bps().unwrap_or(5);
            let cap     = meta.available_flash_capacity().unwrap_or(u64::MAX / 4);
            if position.amount > cap { return Ok(None); }
            if (best_diff_bps as u64) <= (fee_bps + 2) { return Ok(None); }
        }

        // gas-aware precheck: rough CU + fee floor from autoscale (no RPC if cached)
        let mut rough_cu = self.optimize_compute_units_rough(&[]).await.max(200_000);
        rough_cu = self.quantize_units_1024(rough_cu).min(1_400_000);
        let cu_price_floor = self.priority_fee_autoscale(PRIORITY_FEE_LAMPORTS).await.max(PRIORITY_FEE_LAMPORTS);
        let pre_cost = (rough_cu as u64).saturating_mul(self.quantize_cu_price_64(cu_price_floor));
        let daily_rate_diff = ((alt.supply_apy - current.supply_apy) / 100.0) / 365.0;
        let daily_profit_lamports = (position.amount as f64) * daily_rate_diff; // position.amount is in lamports
        if (daily_profit_lamports as u64) <= pre_cost / 10 {
            return Ok(None);
        }

        // exact path: build ixs and estimate cost precisely
        let withdraw_ix = self.build_withdraw_instruction(position).await?;
        let deposit_ix  = self.build_deposit_instruction(position, alt).await?;
        let gas_cost = self
            .estimate_rebalance_cost_lamports(&[withdraw_ix, deposit_ix], 1.0)
            .await
            .unwrap_or(PRIORITY_FEE_LAMPORTS);

        let expected_profit = self.calculate_expected_profit(
            position.amount, current.supply_apy, alt.supply_apy, gas_cost,
        );
        if expected_profit > MIN_PROFIT_BPS as f64 { Ok(Some(alt.clone())) } else { Ok(None) }
    }

    // Optional: map from market pubkey to ReserveMeta if available; default None keeps behavior.
    async fn reserve_meta_for(&self, _market: Pubkey) -> Result<Option<ReserveMeta>> {
        Ok(None)
    }

    async fn estimate_rebalance_cost(&self, amount: u64) -> Result<u64> {
        let base_fee = PRIORITY_FEE_LAMPORTS * 2;
        let size_multiplier = (amount as f64 / 1e9).max(1.0);
        Ok((base_fee as f64 * size_multiplier) as u64)
    }

    // === [IMPROVED: PIECE 7 v3] ===
            }
            let q75v = P2P75.get().unwrap().lock().await.value().unwrap_or(PRIORITY_FEE_LAMPORTS as f64);
            cu_price_live = q75v.round() as u64;
        }
    }

    // 3) Log-domain ct-EMA with simple anti-windup and deadband
    let ema_cell = SURGE_EMA.get_or_init(|| AtomicU64::new(PRIORITY_FEE_LAMPORTS));
    let ema_floor = ema_cell.load(Ordering::Relaxed).max(PRIORITY_FEE_LAMPORTS);
    let tri = cu_price_live.max(ema_floor).max(1);
    let x = (tri as f64).ln().max(0.0);
        tip_account: Option<Pubkey>,
        alts: &[AddressLookupTableAccount],
    ) -> Result<VersionedTransaction> {
        // 1) CU + price (quantized)
        let mut baseline_cu = self.optimize_compute_units_rough(&ixs).await;
        baseline_cu = self.quantize_units_1024(baseline_cu).min(1_400_000);

        let ema = SURGE_EMA.get_or_init(|| AtomicU64::new(0)).load(Ordering::Relaxed);
        let mut cu_price = self
            .quantize_cu_price_64(self.priority_fee_autoscale(PRIORITY_FEE_LAMPORTS).await.max(ema));

        let payer = self.wallet.pubkey();
        let mut pref = vec![ComputeBudgetInstruction::set_compute_unit_limit(baseline_cu)];
        if cu_price > 0 { pref.push(ComputeBudgetInstruction::set_compute_unit_price(cu_price)); }
        if include_tip {
            if let Some(tip_acc) = tip_account {
                let tip = self.dynamic_priority_fee().await.unwrap_or(PRIORITY_FEE_LAMPORTS);
                pref.push(system_instruction::transfer(&payer, &tip_acc, tip));
            }
        }
        ixs.splice(0..0, pref);
        ixs = Self::dedup_budget_and_tip(ixs);

        // 2) assemble & bin-fit
        let bh = self.get_recent_blockhash_with_retry().await?;
        let mut build = |ixs_in: &[Instruction]| -> Option<(VersionedTransaction, usize)> {
            self.estimate_len_v0_sync(payer, ixs_in, bh, alts)
                .or_else(|| self.build_v0_tx(payer, ixs_in, bh, alts).ok().map(|tx| (tx, 0)))
        };

        if let Some((tx, len)) = build(&ixs) {
            if len == 0 || !self.near_udp_cap(len) { return Ok(tx); }
        }

        // Try without tip first if near cap
        let mut ixs_no_tip: Vec<Instruction> = ixs
            .iter()
            .filter(|ix| ix.program_id != solana_program::system_program::id())
            .cloned()
            .collect();
        ixs_no_tip = Self::dedup_budget_and_tip(ixs_no_tip);

        if let Some((tx2, len2)) = build(&ixs_no_tip) {
            if len2 == 0 || !self.near_udp_cap(len2) { return Ok(tx2); }
            // Try acc-data CB if it will fit
            let acc_ix = self.cb_accdata_ix(256_000);
            if self.will_fit_with_ix(payer, &ixs_no_tip, &acc_ix, bh, alts) {
                let mut ixs3 = ixs_no_tip; ixs3.insert(0, acc_ix);
                return self.build_v0_tx(payer, &ixs3, bh, alts);
            } else {
                return Ok(tx2);
            }
        }

        // Fall back to core only (no tip and deduped budget)
        let mut ixs_core: Vec<Instruction> = ixs
            .iter()
            .filter(|ix| ix.program_id != solana_program::system_program::id())
            .cloned()
            .collect();
        ixs_core = Self::dedup_budget_and_tip(ixs_core);

        if let Some((tx3, _)) = build(&ixs_core) { return Ok(tx3); }
        self.build_v0_tx(payer, &ixs_core, bh, alts)
    }

    // === [IMPROVED: PIECE 9 v3] ===
    pub async fn priority_fee_spread_scoped(
        &self,
        scope: &[Pubkey],
        p_lo: f64,
        p_hi: f64,
    ) -> Option<[u64; 3]> {
        if let Ok(fees) = self.rpc_client.get_recent_prioritization_fees(scope).await {
            if fees.is_empty() { return None; }
            let mut vals: Vec<u64> = fees.iter().map(|f| f.prioritization_fee).collect();
            vals.sort_unstable();
            let ix = |p: f64| -> usize { ((vals.len() as f64 * p).floor() as usize).clamp(0, vals.len() - 1) };
            let p50 = vals[ix(0.50)];
            let plo = vals[ix(p_lo)];
            let phi = vals[ix(p_hi)];
            let step1 = plo.saturating_sub(p50).max(0);
            let step2 = phi.saturating_sub(plo).max(60);
            let step3 = (step2.saturating_mul(2)).max(180);
            return Some([step1, step2, step3]);
        }
        None
    }
}

impl PortOptimizer {
    // === [IMPROVED: PIECE 11A v3] ===
    pub async fn simulate_units_and_logs(
        &self,
        tx: &VersionedTransaction,
    ) -> Result<(Option<u32>, Vec<String>)> {
        use tokio::time::{timeout, Duration};
        let cfg = RpcSimulateTransactionConfig { sig_verify: false, replace_recent_blockhash: true, ..Default::default() };

        let hedged = async {
            if let Some(fast) = &self.rpc_fast {
                tokio::select! {
                    r1 = self.rpc_client.simulate_transaction_with_config(tx, cfg) => r1,
                    r2 = fast.simulate_transaction_with_config(tx, cfg) => r2,
                }
            } else {
                self.rpc_client.simulate_transaction_with_config(tx, cfg).await
            }
        };
        let out = match timeout(Duration::from_millis(350), hedged).await {
            Ok(Ok(o))  => o,
            Ok(Err(e)) => return Err(e.into()),
            Err(_)     => timeout(Duration::from_millis(520), self.rpc_client.simulate_transaction_with_config(tx, cfg))
                            .await
                            .map_err(|_| anyhow::anyhow!("simulate timeout"))??,
        };

        let mut units = out.value.units_consumed.map(|u| u as u32);
        if units.is_none() {
            if let Some(ls) = out.value.logs.as_ref() {
                const KEYS: &[&str] = &[
                    "consumed ","compute units: ","units consumed: ","total units consumed: ",
                    "compute units consumed: ","total compute units consumed: ",
                    "units total: ","cu total: ","units=","used: ","used=","cu_used=","cu: ",
                ];
                let tail = 256usize.min(ls.len());
                for line in ls[ls.len()-tail..].iter().rev() {
                    let has_esc = line.as_bytes().iter().any(|&b| b == 0x1B);
                    let clean = if has_esc { self.strip_ansi(line) } else { line.clone() };
                    if let Some(v) = self.parse_num_after_any(&clean, KEYS).and_then(|n| self.filter_units(n)) { units = Some(v); break; }
                    if units.is_none() {
                        if let Some(v) = self.parse_last_num_if_hinted(&clean).and_then(|n| self.filter_units(n)) { units = Some(v); break; }
                    }
                }
            }
        }
        Ok((units, out.value.logs.unwrap_or_default()))
    }

    // === [IMPROVED: PIECE 11B v3] ===
    pub fn adjust_budget_from_sim_error(
        &self,
        logs: &[String],
        base_units: u32,
        base_heap: Option<u32>,
    ) -> (u32, Option<u32>, bool) {
        let mut units = base_units;
        let mut heap  = base_heap;
        let mut changed = false;

        let joined = logs.iter().fold(String::new(), |mut s, l| { s.push_str(l); s.push('\n'); s });

        if joined.contains("ComputationalBudgetExceeded") || joined.contains("max compute units exceeded") {
            units = (units as f64 * 1.18).ceil() as u32; // bump 18%
            changed = true;
        }
        if joined.contains("HeapFrame") || joined.contains("heap") && joined.contains("insufficient") {
            let next = heap.unwrap_or(0).max(64_000).saturating_mul(2).min(512_000);
            if Some(next) != heap { heap = Some(next); changed = true; }
        }
        (units, heap, changed)
    }

    // === [IMPROVED: PIECE ADJ v20] ===
    // No-op guard + single lowercase pass; returns (units, heap, acc_bytes_opt, changed)
    pub fn adjust_budget_from_sim_error_v2(
        &self,
        logs: &[String],
        units0: u32,
        heap0: Option<u32>,
    ) -> (u32, Option<u32>, Option<u32>, bool) {
        let mut units = units0;
        let mut heap  = heap0;
        let mut acc: Option<u32> = None;
        let mut changed = false;

        let mut bump_units = 0.0f64;
        let mut need_heap  = false;
        let mut drop_heap  = false;
        let mut saw_acc_phrase = false;

        for s in logs {
            let l = s.as_str();
            let ll = l.to_ascii_lowercase();
            if l.contains("computational budget exceeded")
                || l.contains("max compute units exceeded")
                || l.contains("Program failed to complete")
                || l.contains("Exceeded maximum number of instructions")
                || l.contains("budget exhausted")
            { bump_units = bump_units.max(0.35); }

            if l.contains("insufficient compute units") { bump_units = bump_units.max(0.20); }
            if l.contains("Heap") || l.contains("heap") || l.contains("request heap") || l.contains("HeapLimitExceeded") { need_heap = true; }

            if l.contains("Transaction too large")
                || l.contains("packet too large")
                || l.contains("too many account keys")
                || l.contains("account keys length")
                || l.contains("would exceed packet size")
            { drop_heap = true; }

            if ll.contains("accounts data") || ll.contains("accounts-data") || ll.contains("acc_data") { saw_acc_phrase = true; }

            if l.contains("AccountInUse") || l.contains("write lock") { bump_units = bump_units.max(0.10); }
        }

        if bump_units == 0.0 && !need_heap && !drop_heap && !saw_acc_phrase { return (units0, heap0, None, false); }

        if saw_acc_phrase {
            if let Some(b) = self.parse_loaded_accounts_bytes_wide(logs) {
                acc = Some(self.pad_accounts_bytes(b));
                changed = true;
            }
        }
        if drop_heap && heap.is_some() { heap = None; changed = true; }

        if bump_units > 0.0 && units < MAX_COMPUTE_UNITS {
            let mut next = ((units as f64) * (1.0 + bump_units)).ceil() as u32;
            next = ((next + 1023) / 1024) * 1024;
            if next <= units { next = (units + 1024).min(MAX_COMPUTE_UNITS); }
            units = next.min(MAX_COMPUTE_UNITS);
            if units != units0 { changed = true; }
        }

        if !drop_heap && need_heap {
            let base = heap.unwrap_or(131_072);
            let next = base.max(131_072).saturating_mul(2).min(512_000);
            if heap != Some(next) { heap = Some(next); changed = true; }
        }
        (units, heap, acc, changed)
    }

    // Parse wide set of vendor log patterns for loaded accounts data bytes
    fn parse_loaded_accounts_bytes_wide(&self, logs: &[String]) -> Option<u32> {
        // Search from the end; tolerate various key phrases and formats
        const KEYS: &[&str] = &[
            "accounts data",
            "accounts-data",
            "acc_data",
            "loaded accounts data",
            "total accounts data",
            "accounts data size",
        ];
        for line in logs.iter().rev() {
            let clean = self.strip_ansi(line);
            let lc = clean.to_ascii_lowercase();
            if KEYS.iter().any(|k| lc.contains(k)) {
                // Extract last integer-like token from the line
                let mut num: Option<u64> = None;
                let mut cur = String::new();
                for ch in lc.chars() {
                    if ch.is_ascii_digit() { cur.push(ch); }
                    else {
                        if !cur.is_empty() { if let Ok(v) = cur.parse::<u64>() { num = Some(v); } cur.clear(); }
                    }
                }
                if num.is_none() && !cur.is_empty() { if let Ok(v) = cur.parse::<u64>() { num = Some(v); } }
                if let Some(v) = num { return u32::try_from(v.min(512_000)).ok(); }
            }
        }
        None
    }

    // Quantize to 32KiB steps, clamp to [32KiB, 512KiB]
    fn pad_accounts_bytes(&self, bytes_exact: u32) -> u32 {
        let step = 32 * 1024u32;
        let mut want = ((bytes_exact + step - 1) / step) * step;
        want = want.clamp(step, 512 * 1024);
        want
    }

    // === [IMPROVED: PIECE U-FIT v20] === residual UDP helpers
    #[inline]
    fn residual_udp_room(&self, base_len: usize) -> isize {
        MAX_UDP_TX_BYTES as isize - base_len as isize
    }
    /// If sum of top `k` single-drop gains < bytes we must shed, it cannot fit.
    #[inline]
    fn cb_residual_hopeless(&self, room: isize, top_gains: &[usize], k: usize) -> bool {
        let need = (-room).max(0) as usize;
        let have: usize = top_gains.iter().take(k).copied().sum();
        have < need
    }

    // === [IMPROVED: PIECE RPC v26] === Hedged processed->confirmed with sliced fallback and optional filters
    pub async fn get_program_accounts_smart(
        &self,
        program_id: &Pubkey,
        slice_len: usize,
        prev_estimate: usize,
    ) -> Result<Vec<(Pubkey, solana_sdk::account::Account)>> {
        self.get_program_accounts_smart_filt(program_id, slice_len, prev_estimate, None).await
    }

    pub async fn get_program_accounts_smart_filt(
        &self,
        program_id: &Pubkey,
        slice_len: usize,
        prev_estimate: usize,
        filters: Option<Vec<RpcFilterType>>,
    ) -> Result<Vec<(Pubkey, solana_sdk::account::Account)>> {
        use tokio::time::{timeout, Duration};
        use solana_account_decoder::{UiAccountEncoding, UiDataSlice};
        use solana_client::rpc_config::{RpcAccountInfoConfig, RpcProgramAccountsConfig};
        use anchor_client::solana_sdk::commitment_config::CommitmentConfig;

        let call_cfg = |cli: &solana_client::nonblocking::rpc_client::RpcClient,
                        commitment: CommitmentConfig,
                        slice: bool| async move {
            let account_config = RpcAccountInfoConfig {
                encoding: Some(UiAccountEncoding::Base64),
                data_slice: slice.then_some(UiDataSlice { offset: 0, length: slice_len }),
                commitment: Some(commitment),
                ..Default::default()
            };
            let cfg = RpcProgramAccountsConfig { filters: filters.clone(), account_config, with_context: None };
            cli.get_program_accounts_with_config(program_id, cfg).await
        };

        // processed + sliced hedge
        let processed = async {
            if let Some(fast) = &self.rpc_fast {
                tokio::select! {
                    r1 = call_cfg(&self.rpc_client, CommitmentConfig::processed(), true) => r1,
                    r2 = call_cfg(fast,               CommitmentConfig::processed(), true) => r2,
                }
            } else {
                call_cfg(&self.rpc_client, CommitmentConfig::processed(), true).await
            }
        };
        let mut first = match timeout(Duration::from_millis(650), processed).await {
            Ok(Ok(v))  => v,
            _          => Vec::new(),
        };

        // quality thresholds vs last book (both lower and upper)
        let prev = prev_estimate.max(8);
        let low  = (prev / 3).max(8);           // too few
        let high = prev.saturating_mul(4) + 64; // suspiciously many

        if first.is_empty() || first.len() < low || first.len() > high {
            // confirmed + sliced hedge (800ms)
            let confirmed = async {
                if let Some(fast) = &self.rpc_fast {
                    tokio::select! {
                        r1 = call_cfg(&self.rpc_client, CommitmentConfig::confirmed(), true) => r1,
                        r2 = call_cfg(fast,               CommitmentConfig::confirmed(), true) => r2,
                    }
                } else {
                    call_cfg(&self.rpc_client, CommitmentConfig::confirmed(), true).await
                }
            };
            if let Ok(Ok(v)) = timeout(Duration::from_millis(800), confirmed).await {
                if !v.is_empty() { first = v; }
            }
        }

        if !first.is_empty() && first.len() >= low && first.len() <= high {
            return Ok(first);
        }

        // full confirmed fallback (2s)
        let full = async {
            if let Some(fast) = &self.rpc_fast {
                tokio::select! {
                    r1 = call_cfg(&self.rpc_client, CommitmentConfig::confirmed(), false) => r1,
                    r2 = call_cfg(fast,               CommitmentConfig::confirmed(), false) => r2,
                }
            } else {
                call_cfg(&self.rpc_client, CommitmentConfig::confirmed(), false).await
            }
        };
        let res = timeout(Duration::from_secs(2), full)
            .await
            .map_err(|_| anyhow::anyhow!("get_program_accounts (fallback) timeout"))??;
        Ok(res)
    }

    // === [IMPROVED: PIECE TIP v38] === surge ring+EMA, RAII reserve, balance-aware clamp
}

// TIP budget and surge caches (module scope statics)
static TIP_BAL_CACHE: OnceLock<tokio::sync::RwLock<(std::time::Instant, u64)>> = OnceLock::new();
static PENDING_TIP_BUDGET: OnceLock<AtomicU64> = OnceLock::new();
static FEE_SURGE_CACHE: OnceLock<tokio::sync::RwLock<(std::time::Instant, u64)>> = OnceLock::new();
static SURGE_RING: OnceLock<tokio::sync::RwLock<VecDeque<u64>>> = OnceLock::new();
static SURGE_EMA: OnceLock<AtomicU64> = OnceLock::new();

const SURGE_RING_CAP: usize = 24;
const PAYER_SAFETY_FLOOR: u64 = 50_000;
const SURGE_FRACTION_NUM: u64 = 1; // 1/3
const SURGE_FRACTION_DEN: u64 = 3;

#[inline]
pub fn tip_budget_reserve(lamports: u64) {
    PENDING_TIP_BUDGET.get_or_init(|| AtomicU64::new(0)).fetch_add(lamports, Ordering::Relaxed);
}
#[inline]
pub fn tip_budget_release(lamports: u64) {
    // Saturating CAS loop (never underflow)
    let a = PENDING_TIP_BUDGET.get_or_init(|| AtomicU64::new(0));
    let mut cur = a.load(Ordering::Relaxed);
    loop {
        let dec = cur.min(lamports);
        match a.compare_exchange(cur, cur.saturating_sub(dec), Ordering::Relaxed, Ordering::Relaxed) {
            Ok(_) => break,
            Err(now) => cur = now,
        }
    }
}

// RAII guard (panic-safe)
pub struct TipReservation(u64);
impl TipReservation {
    pub fn new(lamports: u64) -> Self { tip_budget_reserve(lamports); TipReservation(lamports) }
    pub fn amount(&self) -> u64 { self.0 }
    pub fn into_inner(self) -> u64 { let v = self.0; std::mem::forget(self); v }
}
impl Drop for TipReservation { fn drop(&mut self) { tip_budget_release(self.0); } }

#[inline]
async fn surge_ring_push(v: u64) {
    let cell = SURGE_RING.get_or_init(|| tokio::sync::RwLock::new(VecDeque::with_capacity(SURGE_RING_CAP)));
    let mut w = cell.write().await;
    if w.len() >= SURGE_RING_CAP { w.pop_front(); }
    w.push_back(v);
    // EMA (~35% step)
    let ema = SURGE_EMA.get_or_init(|| AtomicU64::new(v));
    let old = ema.load(Ordering::Relaxed);
    let next = old + ((v.saturating_sub(old)) * 35 / 100);
    ema.store(next.max(1), Ordering::Relaxed);
}
#[inline]
async fn surge_ring_p90() -> Option<u64> {
    let cell = SURGE_RING.get_or_init(|| tokio::sync::RwLock::new(VecDeque::with_capacity(SURGE_RING_CAP)));
    let g = cell.read().await;
    if g.is_empty() { return None; }
    let mut v: Vec<u64> = g.iter().copied().collect();
    v.sort_unstable();
    let idx = ((v.len() as f64 * 0.90).floor() as usize).clamp(0, v.len()-1);
    Some(v[idx])
}

impl PortOptimizer {
    async fn fee_surge_allowance_lamports(&self) -> u64 {
        use std::time::{Duration, Instant};
        if let Some(cell) = FEE_SURGE_CACHE.get() {
            let g = cell.read().await;
            if g.0.elapsed() <= Duration::from_millis(300) { return g.1; }
        }

        // Representative scope: CB + System (+ optional protocols if available elsewhere)
        let mut scope = vec![solana_program::compute_budget::id(), solana_program::system_program::id()];
        // Note: If SOLEND_PROGRAM_ID/KAMINO_PROGRAM_ID are defined elsewhere, they can be appended here.

        use tokio::time::timeout;
        let sample = async {
            if let Some(fast) = &self.rpc_fast {
                tokio::select! {
                    r1 = self.rpc_client.get_recent_prioritization_fees(&scope) => r1,
                    r2 = fast.get_recent_prioritization_fees(&scope)           => r2,
                }
            } else {
                self.rpc_client.get_recent_prioritization_fees(&scope).await
            }
        };

        let mut p90_price: Option<u64> = None;
        if let Ok(Ok(fees)) = timeout(std::time::Duration::from_millis(150), sample).await {
            if !fees.is_empty() {
                let mut vals: Vec<u64> = fees.iter().map(|f| f.prioritization_fee).collect();
                vals.sort_unstable();
                let idx = ((vals.len() as f64 * 0.90).floor() as usize).clamp(0, vals.len() - 1);
                p90_price = Some(vals[idx].max(1));
            }
        }
        if let Some(p) = p90_price { surge_ring_push(p).await; }

        // robust price = max(live p90, ring p90, EMA)
        let ema = SURGE_EMA.get_or_init(|| AtomicU64::new(0)).load(Ordering::Relaxed);
        let robust_price = p90_price.into_iter().chain(surge_ring_p90().await).chain((ema > 0).then_some(ema)).max().unwrap_or(0);

        let lamports = if robust_price > 0 {
            let units = ((200_000u32 + 1023) & !1023).min(1_400_000) as u64;
            let cu_price = self.quantize_cu_price_64(robust_price);
            let x = (units.saturating_mul(cu_price) as f64 * 1.05).ceil() as u64;
            x.min(600_000)
        } else { 0 };

        let cell = FEE_SURGE_CACHE.get_or_init(|| tokio::sync::RwLock::new((Instant::now(), lamports)));
        *cell.write().await = (Instant::now(), lamports);
        lamports
    }

    async fn ensure_tip_funds(&self, min_balance: u64) -> Result<()> {
        use anchor_client::solana_sdk::commitment_config::CommitmentConfig;
        use std::time::Duration;

        if min_balance == 0 { return Ok(()); }

        let me = self.wallet.pubkey();
        let reserved = PENDING_TIP_BUDGET.get_or_init(|| AtomicU64::new(0)).load(Ordering::Relaxed);

        // 250ms hot cache short-circuit
        if let Some(cell) = TIP_BAL_CACHE.get() {
            let g = cell.read().await;
            if g.0.elapsed() <= Duration::from_millis(250) && g.1 >= (min_balance + reserved + PAYER_SAFETY_FLOOR) {
                return Ok(());
            }
        }

        async fn get_bal(cli: &solana_client::nonblocking::rpc_client::RpcClient, who: &Pubkey, com: CommitmentConfig) -> Result<u64> {
            Ok(cli.get_balance_with_commitment(who, com).await?.value)
        }

        // processed hedge
        let processed = if let Some(fast) = &self.rpc_fast {
            tokio::select! {
                r1 = get_bal(&self.rpc_client, &me, CommitmentConfig::processed()) => r1,
                r2 = get_bal(fast,             &me, CommitmentConfig::processed()) => r2,
            }.ok()
        } else {
            get_bal(&self.rpc_client, &me, CommitmentConfig::processed()).await.ok()
        };

        // confirmed hedge if needed
        let confirmed = if processed.is_none() {
            Some(if let Some(fast) = &self.rpc_fast {
                tokio::select! {
                    r1 = get_bal(&self.rpc_client, &me, CommitmentConfig::confirmed()) => r1,
                    r2 = get_bal(fast,             &me, CommitmentConfig::confirmed()) => r2,
                }?
            } else {
                get_bal(&self.rpc_client, &me, CommitmentConfig::confirmed()).await?
            })
        } else { None };

        let bal = match (processed, confirmed) {
            (Some(p), Some(c)) => c.max(p),
            (Some(p), None)    => p,
            (None, Some(c))    => c,
            (None, None)       => 0,
        };

        // clamp surge to unreserved balance slice
        let surge_raw = self.fee_surge_allowance_lamports().await;
        let unreserved = bal.saturating_sub(reserved + PAYER_SAFETY_FLOOR);
        let surge_cap = (unreserved / SURGE_FRACTION_DEN).saturating_mul(SURGE_FRACTION_NUM);
        let surge = surge_raw.min(surge_cap);

        let need = min_balance.saturating_add(reserved).saturating_add(surge).saturating_add(PAYER_SAFETY_FLOOR);

        let cell = TIP_BAL_CACHE.get_or_init(|| tokio::sync::RwLock::new((std::time::Instant::now(), bal)));
        *cell.write().await = (std::time::Instant::now(), bal);

        if bal < need {
            let deficit = need.saturating_sub(bal);
            anyhow::bail!("insufficient balance: have {bal}, reserved {reserved}, surge {surge} (raw {surge_raw}), floor {PAYER_SAFETY_FLOOR}, need {need} (short {deficit})");
        }
        Ok(())
    }

    // === [IMPROVED: PIECE CONFIRM v38] === dual-node probe with smarter escalation
    async fn confirm_sig_with_backoff(
        &self,
        sig: &solana_sdk::signature::Signature,
        timeout: std::time::Duration,
    ) -> Result<bool> {
        use solana_client::rpc_config::RpcSignatureStatusConfig;
        use solana_transaction_status::TransactionConfirmationStatus;
        use tokio::time::{sleep, Instant, Duration};

        let start = Instant::now();
        let mut use_history = false;
        let mut did_finality_probe = false;

        #[inline]
        async fn check_once(
            cli: &solana_client::nonblocking::rpc_client::RpcClient,
            sig: &solana_sdk::signature::Signature,
            hist: bool,
        ) -> Result<Option<solana_transaction_status::TransactionStatus>> {
            let cfg = RpcSignatureStatusConfig { search_transaction_history: hist };
            let r = cli.get_signature_statuses_with_config(&[*sig], cfg).await?;
            Ok(r.value.into_iter().next().flatten())
        }

        // dual-node instant probe (no-history)
        let instant = if let Some(fast) = &self.rpc_fast {
            tokio::select! {
                r1 = check_once(&self.rpc_client, sig, false) => r1,
                r2 = check_once(fast,             sig, false) => r2,
            }
        } else {
            check_once(&self.rpc_client, sig, false).await
        };
        if let Ok(Some(st)) = instant {
            if st.err.is_some() { return Ok(false); }
            if matches!(st.confirmation_status, Some(TransactionConfirmationStatus::Confirmed)|Some(TransactionConfirmationStatus::Finalized))
                || st.confirmations.unwrap_or(0) > 0 { return Ok(true); }
        }

        // jittered escalation gate
        let mut seed = { let b=sig.as_ref(); let mut z=0u64; for &x in b.iter().take(8){ z=(z<<8)|(x as u64); } (z%37) as u64 };
        let mut delay = Duration::from_millis(120 + (seed % 40)); // 120–159ms
        let escalate_at = Duration::from_millis(900 + (seed % 200)); // 900–1099ms

        loop {
            let elapsed = start.elapsed();
            if elapsed >= timeout { return Ok(false); }
            if !use_history && elapsed >= escalate_at { use_history = true; }

            let res = if let Some(fast) = &self.rpc_fast {
                tokio::select! {
                    r1 = check_once(&self.rpc_client, sig, use_history) => r1,
                    r2 = check_once(fast,             sig, use_history) => r2,
                }
            } else {
                check_once(&self.rpc_client, sig, use_history).await
            };

            match res {
                Ok(Some(st)) => {
                    if st.err.is_some() { return Ok(false); }
                    if matches!(st.confirmation_status, Some(TransactionConfirmationStatus::Confirmed)|Some(TransactionConfirmationStatus::Finalized))
                        || st.confirmations.unwrap_or(0) > 0 { return Ok(true); }
                }
                Ok(None) => {
                    if !did_finality_probe && elapsed + Duration::from_millis(600) >= timeout {
                        did_finality_probe = true;
                        if let Ok(r) = self.rpc_client.get_signature_statuses_with_history(&[*sig]).await {
                            if let Some(Some(st)) = r.value.first() {
                                if st.err.is_some() { return Ok(false); }
                                if matches!(st.confirmation_status, Some(TransactionConfirmationStatus::Finalized)) { return Ok(true); }
                            }
                        }
                    }
                }
                Err(_) => {}
            }

            sleep(delay).await;
            delay = std::cmp::min(delay * 2, Duration::from_millis(1_000));
            seed = (seed * 1103515245 + 12345) & 0x7FFF;
            delay += Duration::from_millis((seed % 7 + 1) as u64);
        }
    }

    // === [IMPROVED: PIECE OPT v38] === cooldown + bounded maps + state-FP skip + failure backoff
    async fn optimize_cycle(&self) -> Result<()> {
        self.update_market_data().await?;

        // 1) positions snapshot (>0 only, deterministic)
        let mut pos_list: Vec<(String, Position)> = {
            let g = self.active_positions.read().await;
            g.iter().filter(|(_, p)| p.amount > 0).map(|(k, v)| (k.clone(), v.clone())).collect()
        };
        pos_list.sort_unstable_by(|a, b| a.0.cmp(&b.0));
        if pos_list.is_empty() { return Ok(()); }

        use tokio::sync::RwLock as TokioRwLock;
        static LAST_ACTION_TS: OnceLock<TokioRwLock<FastMap<String, std::time::Instant>>> = OnceLock::new();
        static LAST_EVAL_FP:  OnceLock<TokioRwLock<FastMap<String, (u64, std::time::Instant)>>> = OnceLock::new();
        static FAIL_BACKOFF:  OnceLock<TokioRwLock<FastMap<String, (u32, std::time::Instant)>>> = OnceLock::new();

        const REBAL_COOLDOWN_MS: u64 = 1500;
        const EVAL_TTL_MS: u64 = 500;
        const COOLDOWN_MAP_SOFT_MAX: usize = 8_192;
        const EVAL_MAP_SOFT_MAX: usize = 8_192;
        const FAIL_SOFT_MAX: usize = 8_192;

        #[inline]
        fn apy_key_i64(apy_pct: f64) -> i64 {
            let q = (apy_pct * 1e6).round();
            if q.is_finite() { q as i64 } else { 0 }
        }
        #[inline]
        fn bundle_fp(amount: u64, mkts: &[LendingMarket]) -> u64 {
            let mut best_supply = i64::MIN;
            let mut best_borrow = i64::MAX;
            for m in mkts {
                let s = apy_key_i64(m.supply_apy);
                let b = apy_key_i64(m.borrow_apy);
                if s > best_supply { best_supply = s; }
                if b < best_borrow { best_borrow = b; }
            }
            let s = best_supply as u64;
            let b = best_borrow as u64;
            amount ^ s.rotate_left(13) ^ b.rotate_left(29) ^ (mkts.len() as u64).wrapping_mul(0x9E3779B97F4A7C15)
        }

        let now = std::time::Instant::now();
        let cooldown = LAST_ACTION_TS.get_or_init(|| TokioRwLock::new(FastMap::default()));
        let lastfp  = LAST_EVAL_FP.get_or_init(|| TokioRwLock::new(FastMap::default()));
        let failmap = FAIL_BACKOFF.get_or_init(|| TokioRwLock::new(FastMap::default()));

        let bundles: Vec<(String, Position, Vec<LendingMarket>)> = {
            let g = self.markets.read().await;
            let cd = cooldown.read().await;
            let lf = lastfp.read().await;
            let fb = failmap.read().await;

            let mut out = Vec::with_capacity(pos_list.len());
            for (mint, pos) in pos_list.into_iter() {
                if let Some(ts) = cd.get(&mint) {
                    if now.duration_since(*ts).as_millis() as u64 <= REBAL_COOLDOWN_MS { continue; }
                }
                // failure backoff: exponentially extend cooldown briefly
                if let Some((count, last)) = fb.get(&mint) {
                    let extra = (1u64 << (*count as u64).min(4)) * 200; // 200ms, 400, 800, 1600, 3200
                    if now.duration_since(*last).as_millis() as u64 <= extra { continue; }
                }
                let v = match g.get(&mint) { Some(v) if !v.is_empty() => v, _ => continue };
                let fp = bundle_fp(pos.amount, v);
                if let Some((prev_fp, ts)) = lf.get(&mint) {
                    if *prev_fp == fp && now.duration_since(*ts).as_millis() as u64 <= EVAL_TTL_MS { continue; }
                }
                out.push((mint, pos, v.clone()));
            }
            out
        };
        if bundles.is_empty() { return Ok(()); }

        // 3) concurrency
        use futures::{stream, StreamExt};
        let cores = std::thread::available_parallelism().map(|n| n.get()).unwrap_or(8);
        let desired = ((bundles.len() + 3) / 4).max(8).min(64);
        let conc = desired.min(cores * 2);

        let cdw = LAST_ACTION_TS.get().unwrap().clone();
        let lfw = LAST_EVAL_FP.get().unwrap().clone();
        let fbw = FAIL_BACKOFF.get().unwrap().clone();

        let mut futs = stream::iter(bundles.into_iter())
            .map(|(mint, position, mkts)| async move {
                {
                    let mut w = lfw.write().await;
                    if w.len() > EVAL_MAP_SOFT_MAX { w.clear(); }
                    w.insert(mint.clone(), (bundle_fp(position.amount, &mkts), std::time::Instant::now()));
                }
                match self.find_arbitrage_opportunity(&position, &mkts).await {
                    Ok(Some(target)) => {
                        match self.execute_rebalance(&position, target).await {
                            Ok(_) => {
                                let mut w = cdw.write().await;
                                if w.len() > COOLDOWN_MAP_SOFT_MAX { w.clear(); }
                                w.insert(mint.clone(), std::time::Instant::now());
                                // success → reset failures
                                let mut fw = fbw.write().await;
                                fw.remove(&mint);
                            }
                            Err(e) => {
                                eprintln!("[warn] rebalance failed: {e:?}");
                                let mut fw = fbw.write().await;
                                let (cnt, _) = fw.get(&mint).copied().unwrap_or((0, std::time::Instant::now()));
                                if fw.len() > FAIL_SOFT_MAX { fw.clear(); }
                                fw.insert(mint, (cnt.saturating_add(1), std::time::Instant::now()));
                            }
                        }
                    }
                    Ok(None) => { // soft success → reduce failure count
                        let mut fw = fbw.write().await;
                        if let Some((cnt, _)) = fw.get_mut(&mint) { *cnt = cnt.saturating_sub(1); }
                    }
                    Err(e) => {
                        eprintln!("[warn] find_arbitrage_opportunity error: {e:?}");
                        let mut fw = fbw.write().await;
                        let (cnt, _) = fw.get(&mint).copied().unwrap_or((0, std::time::Instant::now()));
                        if fw.len() > FAIL_SOFT_MAX { fw.clear(); }
                        fw.insert(mint, (cnt.saturating_add(1), std::time::Instant::now()));
                    }
                }
            })
            .buffer_unordered(conc);

        while futs.next().await.is_some() {}
        Ok(())
    }

    // === [IMPROVED: PIECE FEE v38] === v0 sim units + ring/EMA blend + units-EMA fallback
}

static UNITS_EMA: OnceLock<AtomicU64> = OnceLock::new();

impl PortOptimizer {
    pub async fn estimate_rebalance_cost_lamports(
        &self,
        ixs: &[Instruction],
        tip_multiplier: f64,
    ) -> Result<u64> {
        use std::collections::HashSet;

        let payer = self.wallet.pubkey();
        let bh = self.get_recent_blockhash_with_retry().await?;
        let tx0 = self.build_v0_tx(payer, ixs, bh, &[])?;
        let (units_opt, _logs) = self.simulate_units_and_logs(&tx0).await.unwrap_or((None, vec![]));

        // units (sim or EMA fallback)
        let units_sim = units_opt.map(|u| u as u64);
        let units_est = if let Some(u) = units_sim {
            let ema = UNITS_EMA.get_or_init(|| AtomicU64::new(u));
            let old = ema.load(Ordering::Relaxed);
            let next = if old == 0 { u } else { old + ((u.saturating_sub(old)) * 25 / 100) }; // 25% step
            ema.store(next, Ordering::Relaxed);
            u
        } else {
            UNITS_EMA.get_or_init(|| AtomicU64::new(COMPUTE_UNITS as u64)).load(Ordering::Relaxed).max(COMPUTE_UNITS as u64)
        };
        let units = self.quantize_units_1024(units_est as u32).min(1_400_000) as u64;

        // scope programs
        let mut scope: HashSet<Pubkey> = HashSet::new();
        scope.insert(solana_program::compute_budget::id());
        scope.insert(solana_program::system_program::id());
        for ix in ixs { scope.insert(ix.program_id); }

        // hedged, bounded sampling
        use tokio::time::timeout;
        let fees_res = async {
            let vec_scope: Vec<Pubkey> = scope.iter().copied().collect();
            if let Some(fast) = &self.rpc_fast {
                tokio::select! {
                    r1 = self.rpc_client.get_recent_prioritization_fees(&vec_scope) => r1,
                    r2 = fast.get_recent_prioritization_fees(&vec_scope)           => r2,
                }
            } else {
                self.rpc_client.get_recent_prioritization_fees(&vec_scope).await
            }
        };
        let mut cu_price_live = PRIORITY_FEE_LAMPORTS;
        if let Ok(Ok(fees)) = timeout(std::time::Duration::from_millis(150), fees_res).await {
            if !fees.is_empty() {
                let mut vals: Vec<u64> = fees.iter().map(|f| f.prioritization_fee).collect();
                vals.sort_unstable();
                let p75 = vals[(vals.len()*75/100).min(vals.len()-1)];
                cu_price_live = p75.max(PRIORITY_FEE_LAMPORTS);
            }
        }

        // blend with ring+EMA
        let ema = SURGE_EMA.get_or_init(|| AtomicU64::new(0)).load(Ordering::Relaxed);
        let mut ring_p90 = 0u64;
        if let Some(cell) = SURGE_RING.get() {
            let g = cell.read().await;
            if !g.is_empty() {
                let mut v: Vec<u64> = g.iter().copied().collect();
                v.sort_unstable();
                ring_p90 = v[((v.len() as f64 * 0.90).floor() as usize).clamp(0, v.len()-1)];
            }
        }
        let cu_price = self.quantize_cu_price_64(cu_price_live.max(ema).max(ring_p90));

        // lamports = CU*price + tip
        let base = units.saturating_mul(cu_price);
        let tip  = (self.dynamic_priority_fee().await.unwrap_or(PRIORITY_FEE_LAMPORTS) as f64 * tip_multiplier) as u64;
        Ok(((base.saturating_add(tip)) as f64 * 1.05).ceil() as u64)
    }
}

// ----- Observability and Circuit Breaker -----
pub struct Observability {
    pub rebalance_attempts: IntCounter,
    pub rebalance_success: IntCounter,
    pub rebalance_failure: IntCounter,
    pub tip_latency_ms: Histogram,
}

impl Observability {
    pub fn new(reg: &Registry) -> Self {
        let attempts = IntCounter::new("rebalance_attempts", "Number of rebalance attempts").unwrap();
        let success = IntCounter::new("rebalance_success", "Number of successful rebalances").unwrap();
        let failure = IntCounter::new("rebalance_failure", "Number of failed rebalances").unwrap();
        let tip_latency = Histogram::with_opts(HistogramOpts::new("tip_latency_ms", "Tip landing latency (ms)")).unwrap();
        reg.register(Box::new(attempts.clone())).ok();
        reg.register(Box::new(success.clone())).ok();
        reg.register(Box::new(failure.clone())).ok();
        reg.register(Box::new(tip_latency.clone())).ok();
        Self { rebalance_attempts: attempts, rebalance_success: success, rebalance_failure: failure, tip_latency_ms: tip_latency }
    }
}

pub struct CircuitBreaker {
    failures: std::collections::VecDeque<Instant>,
    window: Duration,
    max_failures: usize,
}

impl CircuitBreaker {
    pub fn new(window: Duration, max_failures: usize) -> Self {
        Self { failures: std::collections::VecDeque::new(), window, max_failures }
    }
    pub fn record(&mut self, ok: bool) -> bool {
        let now = Instant::now();
        if ok { self.failures.clear(); return true; }
        self.failures.push_back(now);
        while let Some(&t) = self.failures.front() {
            if now.duration_since(t) > self.window { self.failures.pop_front(); } else { break; }
        }
        self.failures.len() < self.max_failures
    }
}

// ----- ReserveMeta helper stubs for flash fee/capacity (to be wired with on-chain layouts) -----
impl ReserveMeta {
    pub fn flash_fee_bps(&self) -> Result<u64> { Ok(5) } // TODO: replace with on-chain config
    pub fn available_flash_capacity(&self) -> Result<u64> { Ok(u64::MAX / 4) } // TODO
    pub fn reserve_owner_program(&self) -> Pubkey { self.program_id }
}

    fn calculate_expected_profit(
        &self,
        amount: u64,
        current_apy: f64,
        target_apy: f64,
        gas_cost: u64,
    ) -> f64 {
        let daily_rate_diff = (target_apy - current_apy) / 365.0 / 100.0;
        let daily_profit = (amount as f64) * daily_rate_diff;
        let gas_cost_tokens = gas_cost as f64 / 1e9;
        (daily_profit - gas_cost_tokens) / (amount as f64) * 10000.0
    }

    async fn execute_rebalance(
        &self,
        position: &Position,
        target_market: LendingMarket,
    ) -> Result<()> {
        // 1) Build core instructions
        let withdraw_ix = self.build_withdraw_instruction(position).await?;
        let deposit_ix = self.build_deposit_instruction(position, &target_market).await?;

        // 2) Dynamic CU and priority fee (p75 with floor)
        let baseline_cu = self
            .optimize_compute_units_rough(&[withdraw_ix.clone(), deposit_ix.clone()])
            .await;
        let (cu_ix, fee_ix) = self.dynamic_budget_ixs(baseline_cu, PRIORITY_FEE_LAMPORTS).await;

        // Optional: send a small tip to a stable tip account (can plug your rotation)
        let tip_ix = system_instruction::transfer(
            &self.wallet.pubkey(),
            &Pubkey::from_str("JitoTip111111111111111111111111111111111")?,
            self.dynamic_priority_fee().await.unwrap_or(PRIORITY_FEE_LAMPORTS),
        );

        let ixs = vec![cu_ix, fee_ix, tip_ix, withdraw_ix, deposit_ix];

        // 3) Build v0 tx with empty ALTs for now
        let build_with_hash = |bh: Hash| {
            self.build_v0_tx(self.wallet.pubkey(), &ixs, bh, &[])
        };

        // 4) Simulate exactly the transaction to be sent
        let bh = self.get_recent_blockhash_with_retry().await?;
        let tx_sim = build_with_hash(bh)?;
        let sim_res = self.simulate_exact(&tx_sim).await?;
        if let Some(err) = sim_res.err {
            return Err(anyhow!("preflight sim failed: {:?}", err));
        }

        // 5) Send and confirm with slot-aware blockhash lifecycle
        match self
            .send_v0_with_slot_guard(
                |bh2| self.build_v0_tx(self.wallet.pubkey(), &ixs, bh2, &[]),
                Duration::from_secs(45),
                200,
                120,
            )
            .await
        {
            Ok(signature) => {
                println!("Rebalance executed: {signature}");
                self.update_position(position, target_market).await?;
                Ok(())
            }
            Err(e) => {
                eprintln!("Rebalance failed: {e:?}");
                Err(e)
            }
        }
    }

    async fn build_withdraw_instruction(&self, position: &Position) -> Result<Instruction> {
        match position.protocol {
            Protocol::Solend => {
                // Resolve Reserve data and derive only the documented market authority PDA
                let program_id = *SOLEND_PROGRAM_ID;
                let meta = self
                    .resolve_reserve_by_mint(&program_id, &position.token_mint)
                    .await?;

                let user = self.wallet.pubkey();
                let user_collateral_ata = spl_associated_token_account::get_associated_token_address(&user, &meta.collateral_mint);
                let user_destination_liquidity_ata = spl_associated_token_account::get_associated_token_address(&user, &meta.liquidity_mint);

                self.build_solend_withdraw_ix(
                    user_collateral_ata,
                    user_destination_liquidity_ata,
                    position.amount,
                    &meta,
                    program_id,
                )
            }
            Protocol::Kamino => {
                let program_id = *KAMINO_PROGRAM_ID;
                let instruction_data = self.encode_kamino_withdraw(position.amount)?;
                let accounts = self.get_withdraw_accounts(position).await?;
                Ok(Instruction { program_id, accounts, data: instruction_data })
            }
        }
    }

    async fn build_deposit_instruction(
        &self,
        position: &Position,
        target_market: &LendingMarket,
    ) -> Result<Instruction> {
        match target_market.protocol {
            Protocol::Solend => {
                // Resolve reserve and vaults from on-chain state, derive only documented market authority PDA
                let program_id = *SOLEND_PROGRAM_ID;
                let meta = self
                    .resolve_reserve_by_mint(&program_id, &position.token_mint)
                    .await?;

                // User ATAs: source is liquidity mint ATA, destination is collateral mint ATA
                let user = self.wallet.pubkey();
                let user_source_token_ata = spl_associated_token_account::get_associated_token_address(&user, &meta.liquidity_mint);
                let user_collateral_ata = spl_associated_token_account::get_associated_token_address(&user, &meta.collateral_mint);

                // Build full instruction using verified accounts
                self.build_solend_deposit_ix(
                    user_source_token_ata,
                    user_collateral_ata,
                    position.amount,
                    &meta,
                    program_id,
                )
            }
            Protocol::Kamino => {
                let program_id = *KAMINO_PROGRAM_ID;
                let instruction_data = self.encode_kamino_deposit(position.amount)?;
                let accounts = self.get_deposit_accounts(position, target_market).await?;
                Ok(Instruction { program_id, accounts, data: instruction_data })
            }
        }
    }

    // Kamino (Anchor program) encoders using module-level helpers
    fn encode_kamino_deposit(&self, amount: u64) -> Result<Vec<u8>> {
        encode_anchor_ix("deposit", DepositArgs { amount })
    }

    fn encode_kamino_withdraw(&self, amount: u64) -> Result<Vec<u8>> {
        encode_anchor_ix("withdraw", WithdrawArgs { amount })
    }

    fn encode_solend_deposit(&self, amount: u64) -> Result<Vec<u8>> {
        let mut data = vec![SOLEND_DEPOSIT_TAG];
        data.extend_from_slice(&amount.to_le_bytes());
        Ok(data)
    }

    fn encode_solend_withdraw(&self, amount: u64) -> Result<Vec<u8>> {
        let mut data = vec![SOLEND_WITHDRAW_TAG];
        data.extend_from_slice(&amount.to_le_bytes());
        Ok(data)
    }

    // ----- Account Resolution without Guesswork (Solend-like programs) -----
    // Documented PDA: [lending_market, b"authority"] -> lending market authority
    fn derive_lending_market_authority(lending_market: &Pubkey, program_id: &Pubkey) -> (Pubkey, u8) {
        Pubkey::find_program_address(&[lending_market.as_ref(), b"authority"], program_id)
    }

    // Scan program accounts and resolve Reserve for a given liquidity mint. Bounds-check slices before reading.
    pub async fn resolve_reserve_by_mint(
        &self,
        program_id: &Pubkey,
        token_mint: &Pubkey,
    ) -> Result<ReserveMeta> {
        let accounts = self
            .rpc_client
            .get_program_accounts(program_id)
            .await
            .map_err(|e| anyhow!(format!("get_program_accounts: {e}")))?;

        for (reserve_pubkey, acc) in accounts {
            // Replace offsets with verified layout offsets in golden tests
            if acc.data.len() < 224 { continue; }

            let liquidity_mint = Pubkey::new(
                acc.data
                    .get(32..64)
                    .ok_or_else(|| anyhow!("liquidity_mint slice"))?,
            );
            if &liquidity_mint != token_mint { continue; }

            let lending_market = Pubkey::new(
                acc.data
                    .get(96..128)
                    .ok_or_else(|| anyhow!("lending_market slice"))?,
            );
            let liquidity_supply_vault = Pubkey::new(
                acc.data
                    .get(128..160)
                    .ok_or_else(|| anyhow!("liquidity_supply vault slice"))?,
            );
            let collateral_mint = Pubkey::new(
                acc.data
                    .get(160..192)
                    .ok_or_else(|| anyhow!("collateral_mint slice"))?,
            );
            let collateral_supply_vault = Pubkey::new(
                acc.data
                    .get(192..224)
                    .ok_or_else(|| anyhow!("collateral_supply vault slice"))?,
            );

            let (authority, _bump) = Self::derive_lending_market_authority(&lending_market, program_id);
            return Ok(ReserveMeta {
                reserve: reserve_pubkey,
                lending_market,
                lending_market_authority: authority,
                liquidity_mint,
                liquidity_supply_vault,
                collateral_mint,
                collateral_supply_vault,
                program_id: *program_id,
            });
        }

        Err(anyhow!(format!(
            "reserve for mint {} not found in program {}",
            token_mint, program_id
        )))
    }

    pub async fn build_solend_deposit_ix(
        &self,
        user_source_token_ata: Pubkey,
        user_collateral_ata: Pubkey,
        amount: u64,
        meta: &ReserveMeta,
        program_id: Pubkey,
    ) -> Result<Instruction> {
        let data = self.encode_solend_deposit(amount)?;
        // Accounts per token-lending deposit reserve liquidity spec
        let accounts = vec![
            AccountMeta::new(meta.liquidity_supply_vault, false),
            AccountMeta::new(user_source_token_ata, false),
            AccountMeta::new(meta.reserve, false),
            AccountMeta::new(meta.lending_market, false),
            AccountMeta::new_readonly(meta.lending_market_authority, false),
            AccountMeta::new(meta.collateral_mint, false),
            AccountMeta::new(meta.collateral_supply_vault, false),
            AccountMeta::new(user_collateral_ata, false),
            AccountMeta::new_readonly(spl_token::id(), false),
            AccountMeta::new_readonly(solana_sdk::sysvar::rent::id(), false),
            AccountMeta::new_readonly(solana_sdk::sysvar::clock::id(), false),
        ];
        Ok(Instruction { program_id, accounts, data })
    }

    pub async fn build_solend_withdraw_ix(
        &self,
        user_collateral_ata: Pubkey,
        user_destination_liquidity_ata: Pubkey,
        amount: u64,
        meta: &ReserveMeta,
        program_id: Pubkey,
    ) -> Result<Instruction> {
        let data = self.encode_solend_withdraw(amount)?;
        // Accounts per token-lending redeem reserve collateral spec
        let accounts = vec![
            AccountMeta::new(user_collateral_ata, false),
            AccountMeta::new(meta.collateral_supply_vault, false),
            AccountMeta::new(meta.reserve, false),
            AccountMeta::new(meta.lending_market, false),
            AccountMeta::new_readonly(meta.lending_market_authority, false),
            AccountMeta::new(meta.liquidity_supply_vault, false),
            AccountMeta::new(user_destination_liquidity_ata, false),
            AccountMeta::new_readonly(spl_token::id(), false),
            AccountMeta::new_readonly(solana_sdk::sysvar::rent::id(), false),
            AccountMeta::new_readonly(solana_sdk::sysvar::clock::id(), false),
        ];
        Ok(Instruction { program_id, accounts, data })
    }

    async fn get_withdraw_accounts(&self, position: &Position) -> Result<Vec<AccountMeta>> {
        let user_pubkey = self.wallet.pubkey();
        
        match position.protocol {
            Protocol::Solend => {
                let (reserve_liquidity_supply, _) = Pubkey::find_program_address(
                    &[b"liquidity_supply", position.token_mint.as_ref()],
                    &Pubkey::from_str(SOLEND_PROGRAM_ID)?,
                );
                
                let (user_collateral, _) = Pubkey::find_program_address(
                    &[b"user_collateral", user_pubkey.as_ref(), position.token_mint.as_ref()],
                    &Pubkey::from_str(SOLEND_PROGRAM_ID)?,
                );
                
                Ok(vec![
                    AccountMeta::new(position.token_mint, false),
                    AccountMeta::new(reserve_liquidity_supply, false),
                    AccountMeta::new(user_collateral, false),
                    AccountMeta::new(user_pubkey, true),
                    AccountMeta::new_readonly(solana_sdk::pubkey::Pubkey::from_str(&spl_token::id().to_string()).unwrap(), false),
                ])
            }
            Protocol::Kamino => {
                let (market_authority, _) = Pubkey::find_program_address(
                    &[b"market_authority", position.token_mint.as_ref()],
                    &Pubkey::from_str(KAMINO_PROGRAM_ID)?,
                );
                
                let (reserve_vault, _) = Pubkey::find_program_address(
                    &[b"reserve_vault", position.token_mint.as_ref()],
                    &Pubkey::from_str(KAMINO_PROGRAM_ID)?,
                );
                
                Ok(vec![
                    AccountMeta::new(position.token_mint, false),
                    AccountMeta::new(market_authority, false),
                    AccountMeta::new(reserve_vault, false),
                    AccountMeta::new(user_pubkey, true),
                    AccountMeta::new_readonly(solana_sdk::pubkey::Pubkey::from_str(&spl_token::id().to_string()).unwrap(), false),
                ])
            }
        }
    }

    async fn get_deposit_accounts(
        &self,
        position: &Position,
        target_market: &LendingMarket,
    ) -> Result<Vec<AccountMeta>> {
        let user_pubkey = self.wallet.pubkey();
        
        match target_market.protocol {
            Protocol::Solend => {
                let (reserve_liquidity_supply, _) = Pubkey::find_program_address(
                    &[b"liquidity_supply", position.token_mint.as_ref()],
                    &Pubkey::from_str(SOLEND_PROGRAM_ID)?,
                );
                
                let (user_collateral, _) = Pubkey::find_program_address(
                    &[b"user_collateral", user_pubkey.as_ref(), position.token_mint.as_ref()],
                    &Pubkey::from_str(SOLEND_PROGRAM_ID)?,
                );
                
                Ok(vec![
                    AccountMeta::new(position.token_mint, false),
                    AccountMeta::new(reserve_liquidity_supply, false),
                    AccountMeta::new(user_collateral, false),
                    AccountMeta::new(user_pubkey, true),
                    AccountMeta::new_readonly(solana_sdk::pubkey::Pubkey::from_str(&spl_token::id().to_string()).unwrap(), false),
                ])
            }
            Protocol::Kamino => {
                let (market_authority, _) = Pubkey::find_program_address(
                    &[b"market_authority", position.token_mint.as_ref()],
                    &Pubkey::from_str(KAMINO_PROGRAM_ID)?,
                );
                
                let (reserve_vault, _) = Pubkey::find_program_address(
                    &[b"reserve_vault", position.token_mint.as_ref()],
                    &Pubkey::from_str(KAMINO_PROGRAM_ID)?,
                );
                
                Ok(vec![
                    AccountMeta::new(position.token_mint, false),
                    AccountMeta::new(market_authority, false),
                    AccountMeta::new(reserve_vault, false),
                    AccountMeta::new(user_pubkey, true),
                    AccountMeta::new_readonly(solana_sdk::pubkey::Pubkey::from_str(&spl_token::id().to_string()).unwrap(), false),
                ])
            }
        }
    }

    async fn get_recent_blockhash_with_retry(&self) -> Result<Hash> {
        let mut retries = 3;
        let mut last_error = None;
        
        while retries > 0 {
            match self.rpc_client.get_latest_blockhash().await {
                Ok(blockhash) => return Ok(blockhash),
                Err(e) => {
                    last_error = Some(e);
                    retries -= 1;
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            }
        }
        
        Err(anyhow!("Failed to get blockhash: {:?}", last_error))
    }

    async fn send_transaction_with_retry(&self, transaction: &Transaction) -> Result<String> {
        let mut retries = 5;
        let mut last_error = None;
        
        while retries > 0 {
            match self.rpc_client.send_transaction(transaction).await {
                Ok(signature) => {
                    if self.confirm_transaction(&signature).await? { return Ok(signature.to_string()); }
                }
                Err(e) => { if e.to_string().contains("AlreadyProcessed") { return Ok(transaction.signatures[0].to_string()); } last_error = Some(e); }
            }
            
            retries -= 1;
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
        
        Err(anyhow!("Failed to send transaction: {:?}", last_error))
    }

    async fn confirm_transaction(&self, signature: &solana_sdk::signature::Signature) -> Result<bool> {
        let start = Instant::now();
        let timeout = Duration::from_secs(30);
        
        while start.elapsed() < timeout {
            let resp = self.rpc_client.get_signature_statuses(&[*signature]).await?;
            if let Some(opt_st) = resp.value.first() { if let Some(st) = opt_st { return Ok(st.err.is_none()); } }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
        
        Ok(false)
    }

    // ----- Slot-aware blockhash lifecycle helpers -----

    async fn simulate_exact_v0(&self, tx: &VersionedTransaction) -> Result<()> {
        let res = self.simulate_exact(tx).await?;
        if let Some(err) = res.err {
            return Err(anyhow!(format!("simulation err: {:?}", err)));
        }
        Ok(())
    }

    async fn send_and_confirm_v0(
        &self,
        tx: &VersionedTransaction,
        timeout: Duration,
    ) -> Result<solana_sdk::signature::Signature> {
        let sig = self.rpc_client.send_transaction(tx).await?;
        let ok = self.confirm_sig_with_backoff(&sig, timeout).await?;
        if ok { Ok(sig) } else { Err(anyhow!("confirmation timeout")) }
    }

    // === [IMPROVED: PIECE LEGACY SEND v53] ===
    // Configured legacy send with min_context_slot + hedged RPC and jittered pacer
    async fn confirm_transaction(&self, sig: &solana_sdk::signature::Signature) -> Result<bool> {
        // Reuse the same confirmation backoff; 20s default is consistent with v0 path
        self.confirm_sig_with_backoff(sig, Duration::from_secs(20)).await
    }

    pub async fn send_transaction_with_retry(
        &self,
        tx: &solana_sdk::transaction::Transaction,
    ) -> Result<String> {
        let min_slot = self
            .get_fresh_blockhash_bundle()
            .await
            .ok()
            .map(|b| b.now_slot.saturating_sub(2));
        self.send_transaction_with_retry_cfg(tx, min_slot).await
    }

    pub async fn send_transaction_with_retry_cfg(
        &self,
        tx: &solana_sdk::transaction::Transaction,
        min_context_slot: Option<u64>,
    ) -> Result<String> {
        use tokio::time::{sleep, Duration as TDur};
        let mut last_err: Option<anyhow::Error> = None;
        let cfg = solana_client::rpc_config::RpcSendTransactionConfig {
            skip_preflight: true,
            preflight_commitment: None,
            max_retries: None,
            min_context_slot,
        };

        let try_send = |cfg: solana_client::rpc_config::RpcSendTransactionConfig| async {
            if let Some(fast) = &self.rpc_fast {
                tokio::select! {
                    r1 = self.rpc_client.send_transaction_with_config(tx, cfg.clone()) => r1,
                    r2 = fast.send_transaction_with_config(tx, cfg.clone())            => r2,
                }
            } else {
                self.rpc_client.send_transaction_with_config(tx, cfg).await
            }
        };

        for iter in 0..6u64 {
            match try_send(cfg.clone()).await {
                Ok(sig) => {
                    if self.confirm_transaction(&sig).await? { return Ok(sig.to_string()); }
                }
                Err(e) => {
                    let low = e.to_string().to_lowercase();
                    if low.contains("already processed") || low.contains("duplicate signature") {
                        return Ok(tx.signatures.get(0).map(|s| s.to_string()).unwrap_or_default());
                    }
                    if low.contains("account in use")                { sleep(TDur::from_millis(20)).await; }
                    if low.contains("too many requests")             { sleep(TDur::from_millis(45)).await; }
                    if low.contains("rate limit")                    { sleep(TDur::from_millis(55)).await; }
                    if low.contains("connection") || low.contains("broken pipe") || low.contains("timed out") { sleep(TDur::from_millis(25)).await; }
                    last_err = Some(anyhow::anyhow!(e));
                }
            }
            // jittered pacer 90–115ms
            let salt = self.wallet.pubkey().to_bytes()[31] as u64;
            let jitter = (salt ^ (iter.wrapping_mul(131))) % 26;
            sleep(TDur::from_millis(90 + jitter)).await;
        }
        Err(anyhow::anyhow!(format!("Failed to send transaction: {:?}", last_err)))
    }

    // === v41 → v42 compat shim (delegate old signature to new height-margin API) ===
    /// NEW API (v42): prefer this everywhere. Height-margin only.
    pub async fn send_v0_with_slot_guard_height<F>(
        &self,
        mut build_with_hash: F,
        max_wait: Duration,
        height_margin: u64,
    ) -> Result<solana_sdk::signature::Signature>
    where
        F: Send + 'static + FnMut(Hash) -> Result<VersionedTransaction>,
    {
        // Delegate to the private implementation by mapping height_margin to refresh_before_slots.
        self.send_v0_with_slot_guard_impl(build_with_hash, max_wait, 0, height_margin).await
    }

    /// OLD API (v41): keep for legacy callers; delegates to v42.
    #[deprecated(
        note = "Use send_v0_with_slot_guard_height(..., height_margin) — this wrapper forwards to v42."
    )]
    pub async fn send_v0_with_slot_guard<F>(
        &self,
        mut build_with_hash: F,
        max_wait: Duration,
        _hash_valid_for_slots: u64,
        refresh_before_slots: u64,
    ) -> Result<solana_sdk::signature::Signature>
    where
        F: Send + 'static + FnMut(Hash) -> Result<VersionedTransaction>,
    {
        // Direct mapping: treat old `refresh_before_slots` as a height-margin.
        self.send_v0_with_slot_guard_impl(build_with_hash, max_wait, _hash_valid_for_slots, refresh_before_slots).await
    }

    // Keep the v41 implementation body intact below a private helper to avoid deprecation clashes.
    #[allow(deprecated)]
    async fn send_v0_with_slot_guard_impl<F>(
        &self,
        mut build_with_hash: F,
        max_wait: Duration,
        _hash_valid_for_slots: u64,
        refresh_before_slots: u64,
    ) -> Result<solana_sdk::signature::Signature>
    where
        F: Send + 'static + FnMut(Hash) -> Result<VersionedTransaction>,
    {
        let start = Instant::now();
        let (mut bh, mut h0) = self.get_fresh_blockhash().await?;
        let mut tx = build_with_hash(bh)?;
        loop {
            if start.elapsed() >= max_wait {
                return Err(anyhow!("timeout sending v0 tx"));
            }
            if let Err(e) = self.simulate_exact_v0(&tx).await {
                let msg = e.to_string().to_lowercase();
                if msg.contains("blockhash") {
                    let pair = self.get_fresh_blockhash().await?;
                    bh = pair.0;
                    h0 = pair.1;
                    tx = build_with_hash(bh)?;
                    continue;
                }
                return Err(anyhow!(format!("simulation failed: {e}")));
            }

            match self.send_and_confirm_v0(&tx, Duration::from_secs(20)).await {
                Ok(sig) => return Ok(sig),
                Err(e) => {
                    let msg = e.to_string().to_lowercase();
                    if msg.contains("blockhash not found") || msg.contains("blockhashnotfound") {
                        let pair = self.get_fresh_blockhash().await?;
                        bh = pair.0;
                        h0 = pair.1;
                        tx = build_with_hash(bh)?;
                        continue;
                    }
                    let h_now = self.rpc_client.get_block_height().await.unwrap_or(h0);
                    if h_now.saturating_sub(h0) >= refresh_before_slots {
                        let pair = self.get_fresh_blockhash().await?;
                        bh = pair.0;
                        h0 = pair.1;
                        tx = build_with_hash(bh)?;
                        continue;
                    }
                    tokio::time::sleep(Duration::from_millis(200)).await;
                }
            }
        }
    }

    async fn update_position(
        &self,
        old_position: &Position,
        new_market: LendingMarket,
    ) -> Result<()> {
        let mut positions = self.active_positions.write().await;
        
        positions.insert(
            old_position.token_mint.to_string(),
            Position {
                token_mint: old_position.token_mint,
                protocol: new_market.protocol,
                amount: old_position.amount,
                entry_apy: new_market.supply_apy,
                last_rebalance: Instant::now(),
            },
        );
        
        Ok(())
    }

    pub async fn initialize_positions(&self, token_mints: Vec<Pubkey>) -> Result<()> {
        let mut positions = self.active_positions.write().await;
        
        for mint in token_mints {
            let balance = self.get_token_balance(&mint).await?;
            
            if balance > 0 {
                let best_market = self.find_best_market(&mint).await?;
                
                positions.insert(
                    mint.to_string(),
                    Position {
                        token_mint: mint,
                        protocol: best_market.protocol.clone(),
                        amount: balance,
                        entry_apy: best_market.supply_apy,
                        last_rebalance: Instant::now(),
                    },
                );
                
                self.execute_initial_deposit(&mint, &best_market, balance).await?;
            }
        }
        
        Ok(())
    }

    // === [IMPROVED: PIECE BALANCES v53] ===
    // token-agnostic ATA decoding (works for SPL Token and Token-2022), no panics
    async fn get_token_balance(&self, mint: &Pubkey) -> Result<u64> {
        let tp = self.token_program_for_mint(mint).await?;
        let ata = Self::ata_for(&self.wallet.pubkey(), mint, &tp);

        match self.rpc_client.get_account(&ata).await {
            Ok(acc) => {
                // Minimal header layout check: [0..32]=mint, [32..64]=owner, [64..72]=amount (LE)
                if acc.data.len() < 72 { return Ok(0); }
                let mint_bytes  = &acc.data[0..32];
                let owner_bytes = &acc.data[32..64];
                let amt_bytes   = &acc.data[64..72];
                if Pubkey::new(mint_bytes) != *mint || Pubkey::new(owner_bytes) != self.wallet.pubkey() {
                    return Ok(0);
                }
                let amt = amt_bytes.try_into().ok().map(u64::from_le_bytes).unwrap_or(0);
                Ok(amt)
            }
            Err(_) => Ok(0),
        }
    }

    async fn find_best_market(&self, mint: &Pubkey) -> Result<LendingMarket> {
        self.update_market_data().await?;
        let markets = self.markets.read().await;
        let key = mint.to_string();
        let candidates = markets.get(&key)
            .ok_or_else(|| anyhow!("No markets for token {}", key))?;
        let best = self
            .pick_best_market_weighted(0, candidates)
            .ok_or_else(|| anyhow!("No available markets for token {}", key))?;
        Ok(best)
    }

    async fn execute_initial_deposit(
        &self,
        mint: &Pubkey,
        market: &LendingMarket,
        amount: u64,
    ) -> Result<()> {
        let deposit_ix = self.build_deposit_instruction(
            &Position {
                token_mint: *mint,
                protocol: market.protocol.clone(),
                amount,
                entry_apy: market.supply_apy,
                last_rebalance: Instant::now(),
            },
            market,
        ).await?;

        // Versioned v0 send with dynamic budget, exact sim, stale-hash recovery
        let baseline = self.optimize_compute_units_rough(&[deposit_ix.clone()]).await;
        let (cu_ix, fee_ix) = self.dynamic_budget_ixs(baseline, PRIORITY_FEE_LAMPORTS).await;
        let ixs = vec![cu_ix, fee_ix, deposit_ix];
        let bh = self.get_recent_blockhash_with_retry().await?;
        let tx = self.build_v0_tx(self.wallet.pubkey(), &ixs, bh, &[])?;
        let sim = self.simulate_exact(&tx).await?;
        if let Some(err) = sim.err { return Err(anyhow!(format!("preflight sim failed: {:?}", err))); }
        let _sig = self
            .send_v0_with_slot_guard(
                |bh2| self.build_v0_tx(self.wallet.pubkey(), &ixs, bh2, &[]),
                Duration::from_secs(30),
                200,
                120,
            )
            .await?;
        Ok(())
    }

    pub async fn emergency_withdraw_all(&self) -> Result<()> {
        let positions = self.active_positions.read().await.clone();
        
        for (_, position) in positions {
            match self.emergency_withdraw(&position).await {
                Ok(_) => println!("Emergency withdraw successful for {}", position.token_mint),
                Err(e) => eprintln!("Emergency withdraw failed for {}: {:?}", position.token_mint, e),
            }
        }
        
        Ok(())
    }

    async fn emergency_withdraw(&self, position: &Position) -> Result<()> {
        let withdraw_ix = self.build_withdraw_instruction(position).await?;
        let baseline = self.optimize_compute_units_rough(&[withdraw_ix.clone()]).await;
        let (cu_ix, fee_ix) = self.dynamic_budget_ixs(baseline.max(COMPUTE_UNITS * 2), PRIORITY_FEE_LAMPORTS * 5).await;
        let ixs = vec![cu_ix, fee_ix, withdraw_ix];
        let bh = self.get_recent_blockhash_with_retry().await?;
        let tx = self.build_v0_tx(self.wallet.pubkey(), &ixs, bh, &[])?;
        let sim = self.simulate_exact(&tx).await?;
        if let Some(err) = sim.err { return Err(anyhow!(format!("preflight sim failed: {:?}", err))); }
        let _sig = self
            .send_v0_with_slot_guard(
                |bh2| self.build_v0_tx(self.wallet.pubkey(), &ixs, bh2, &[]),
                Duration::from_secs(30),
                200,
                120,
            )
            .await?;
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct OptimizationMetrics {
    pub total_rebalances: u64,
    pub successful_rebalances: u64,
    pub failed_rebalances: u64,
    pub total_profit_bps: f64,
    pub gas_spent: u64,
}

impl PortOptimizer {
    pub fn with_metrics(self) -> Self {
        self
    }

    pub async fn get_optimization_metrics(&self) -> OptimizationMetrics {
        OptimizationMetrics {
            total_rebalances: 0,
            successful_rebalances: 0,
            failed_rebalances: 0,
            total_profit_bps: 0.0,
            gas_spent: 0,
        }
    }

    async fn preflight_simulation(&self, transaction: &Transaction) -> Result<bool> {
        match self.rpc_client.simulate_transaction(transaction).await {
            Ok(result) => {
                if result.value.err.is_none() {
                    Ok(true)
                } else {
                    Ok(false)
                }
            }
            Err(_) => Ok(false),
        }
    }

    pub async fn monitor_health(&self) -> Result<()> {
        let positions = self.active_positions.read().await;
        
        for (token, position) in positions.iter() {
            let elapsed = position.last_rebalance.elapsed();
            if elapsed > Duration::from_secs(24 * 60 * 60) {
                eprintln!("Warning: Position {token} hasn't been rebalanced in 24h");
            }
        }
        
        Ok(())
    }

    async fn calculate_network_congestion(&self) -> Result<f64> {
        let recent_fees = self.rpc_client.get_recent_prioritization_fees(&[]).await?;
        let avg_fee = recent_fees.iter().map(|f| f.prioritization_fee).sum::<u64>() as f64 
            / recent_fees.len().max(1) as f64;
        
        Ok(avg_fee / 1000.0)
    }

    async fn dynamic_priority_fee(&self) -> Result<u64> {
        let congestion = self.calculate_network_congestion().await?;
        let base_fee = PRIORITY_FEE_LAMPORTS;
        
        let multiplier = if congestion > 100.0 {
            3.0
        } else if congestion > 50.0 {
            2.0
        } else {
            1.0
        };
        
        Ok((base_fee as f64 * multiplier) as u64)
    }

    pub async fn validate_market_data(&self, market: &LendingMarket) -> bool {
        if market.supply_apy > 1000.0 || market.supply_apy < 0.0 {
            return false;
        }
        
        if market.utilization > 1.0 || market.utilization < 0.0 {
            return false;
        }
        
        if market.last_update.elapsed() > Duration::from_secs(300) {
            return false;
        }
        
        true
    }

    async fn get_jito_tip(&self) -> u64 {
        let base_tip = 10000;
        let congestion = self.calculate_network_congestion().await.unwrap_or(1.0);
        (base_tip as f64 * (1.0 + congestion / 100.0)) as u64
    }

    pub async fn build_jito_bundle(
        &self,
        transactions: Vec<Transaction>,
    ) -> Result<Vec<Transaction>> {
        // Deprecated: prefer build_bundle_from_plans with canonical instruction builders.
        // Keep existing signature but avoid unsafe reconstruction; just prepend a tip transfer tx as a separate tx.
        let tip_amount = self.get_jito_tip().await;
        let tip_account = Pubkey::from_str("96gYZGLnJYVFmbjzopPSU6QiEV5fGqZNyN9nmNhvrZU5")?;
        let tip_ix = system_instruction::transfer(&self.wallet.pubkey(), &tip_account, tip_amount);
        let message = Message::new(&[tip_ix], Some(&self.wallet.pubkey()));
        let mut tip_tx = Transaction::new_unsigned(message);
        let bh = self.get_recent_blockhash_with_retry().await?;
        tip_tx.sign(&[self.wallet.as_ref()], bh);
        let mut out = Vec::with_capacity(transactions.len() + 1);
        out.push(tip_tx);
        out.extend(transactions.into_iter());
        Ok(out)
    }


    pub async fn optimize_compute_units(&self, transaction: &Transaction) -> u32 {
        match self.rpc_client.simulate_transaction(transaction).await {
            Ok(result) => {
                if let Some(units) = result.value.units_consumed {
                    (units as f64 * 1.2) as u32
                } else {
                    COMPUTE_UNITS
                }
            }
            Err(_) => COMPUTE_UNITS,
        }
    }

    async fn check_slippage(&self, position: &Position, market: &LendingMarket) -> Result<bool> {
        let expected_amount = position.amount;
        let max_slippage = (expected_amount as f64 * MAX_SLIPPAGE_BPS as f64) / 10000.0;
        
        let available = market.liquidity_available as f64;
        if available < expected_amount as f64 - max_slippage {
            return Ok(false);
        }
        
        Ok(true)
    }

    pub async fn start_with_config(
        rpc_url: &str,
        _ws_url: &str,
        keypair_path: &str,
        token_mints: Vec<String>,
    ) -> Result<()> {
        let wallet = Keypair::from_bytes(&std::fs::read(keypair_path)?)?;
        let optimizer = Self::new(rpc_url, wallet);
        
        let mints: Vec<Pubkey> = token_mints
            .iter()
            .map(|s| Pubkey::from_str(s))
            .collect::<Result<Vec<_>, _>>()?;
        
        optimizer.initialize_positions(mints).await?;
        
        let _monitor_handle = {
            let opt = optimizer.clone();
            tokio::spawn(async move {
                let mut interval = tokio::time::interval(Duration::from_secs(60));
                loop {
                    interval.tick().await;
                    if let Err(e) = opt.monitor_health().await {
                        eprintln!("Health monitor error: {e:?}");
                    }
                }
            })
        };
        
        tokio::select! {
            result = optimizer.run() => {
                if let Err(e) = result {
                    eprintln!("Optimizer error: {e:?}");
                }
            }
            _ = tokio::signal::ctrl_c() => {
                println!("Shutting down...");
                optimizer.emergency_withdraw_all().await?;
            }
        }
        
        Ok(())
    }
}

impl Clone for PortOptimizer {
    fn clone(&self) -> Self {
        Self {
            rpc_client: self.rpc_client.clone(),
            wallet: self.wallet.clone(),
            markets: self.markets.clone(),
            active_positions: self.active_positions.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_apy_calculation() {
        let optimizer = PortOptimizer::new("https://api.mainnet-beta.solana.com", Keypair::new());
        let rate = 1000000000000000u64;
        let apy = optimizer.calculate_apy_from_rate(rate);
        assert!(apy >= 0.0 && apy <= 1000.0);
    }

    #[tokio::test]
    async fn test_profit_calculation() {
        let optimizer = PortOptimizer::new("https://api.mainnet-beta.solana.com", Keypair::new());
        let profit = optimizer.calculate_expected_profit(
            1000000000,
            5.0,
            7.0,
            50000,
        );
        assert!(profit > 0.0);
    }
}
