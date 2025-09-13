use solana_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::commitment_config::CommitmentConfig;
use anyhow::{Context, Result};
use futures::future;

pub fn make_rpc_client(rpc_url: &str) -> RpcClient {
    RpcClient::new_with_commitment(
        rpc_url.to_string(),
        CommitmentConfig::confirmed(), // tuned for MEV bots: confirmed is faster than finalized
    )
}

// =============== ShardedScorer v2 (adaptive width, fairness guard) ===============
use futures::stream::FuturesUnordered;
use futures::StreamExt;

pub struct ShardedScorer {
    hi_txs: Vec<mpsc::Sender<(Signature, Pubkey)>>,
    lo_txs: Vec<mpsc::Sender<(Signature, Pubkey)>>,
}

impl ShardedScorer {
    pub fn new(scorer: Arc<RivalBotCapabilityScorer>, shards: usize, cap_per_q: usize) -> Self {
        assert!(shards > 0);
        let mut hi_txs = Vec::with_capacity(shards);
        let mut lo_txs = Vec::with_capacity(shards);
        for shard in 0..shards {
            let (hi_tx, mut hi_rx) = mpsc::channel::<(Signature, Pubkey)>(cap_per_q);
            let (lo_tx, mut lo_rx) = mpsc::channel::<(Signature, Pubkey)>(cap_per_q);
            let s = scorer.clone();
            tokio::spawn(async move {
                let cores = std::thread::available_parallelism().map(|n| n.get()).unwrap_or(8);
                let base_width = ((cores.max(4).min(32)) / 2).max(4).min(16);
                const BATCH_BASE: usize = 64;
                const BATCH_HOT: usize = 96;
                const HI_BURST_MAX: usize = 4;
                const DEDUP_CAP: usize = 8192;
                let mut buf: Vec<(Signature, Pubkey)> = Vec::with_capacity(BATCH_HOT);
                let mut hi_burst = 0usize;
                use std::collections::{HashSet, VecDeque};
                let mut seen: HashSet<Signature> = HashSet::with_capacity(DEDUP_CAP);
                let mut order: VecDeque<Signature> = VecDeque::with_capacity(DEDUP_CAP);
                let mut insert_seen = |sig: Signature| {
                    if seen.contains(&sig) { true } else {
                        if seen.len() >= DEDUP_CAP { if let Some(old) = order.pop_front() { seen.remove(&old); } }
                        seen.insert(sig); order.push_back(sig); false
                    }
                };
                loop {
                    let mut did_work = false; let mut hot = false;
                    // HI burst
                    if hi_burst < HI_BURST_MAX {
                        let mut pulled = 0usize;
                        while let Ok((sig, bot)) = hi_rx.try_recv() {
                            if insert_seen(sig) { super::metrics_hdr::record_dedup(shard, true); continue; }
                            buf.push((sig, bot)); pulled += 1; if buf.len() >= BATCH_HOT { break; }
                        }
                        if pulled > 0 { did_work = true; hi_burst += 1; hot = pulled >= 24; let width = if hot { (base_width + 4).min(32) } else { base_width }; drain_with_width(&s, &mut buf, width).await; }
                    }
                    // LO fairness
                    if !did_work || hi_burst >= HI_BURST_MAX {
                        let mut pulled = 0usize;
                        while let Ok((sig, bot)) = lo_rx.try_recv() {
                            if insert_seen(sig) { super::metrics_hdr::record_dedup(shard, false); continue; }
                            buf.push((sig, bot)); pulled += 1; if buf.len() >= BATCH_HOT { break; }
                        }
                        if pulled > 0 { did_work = true; hi_burst = 0; hot = hot || pulled >= 24; let width = if hot { (base_width + 4).min(32) } else { base_width }; drain_with_width(&s, &mut buf, width).await; }
                    }
                    if !did_work {
                        tokio::select! {
                            biased;
                            Some((sig,bot)) = hi_rx.recv() => {
                                if insert_seen(sig) { super::metrics_hdr::record_dedup(shard, true); }
                                else { buf.push((sig,bot)); }
                                hi_burst = (hi_burst + 1).min(HI_BURST_MAX);
                                while buf.len() < BATCH_BASE { if let Ok((s2,b2)) = hi_rx.try_recv() { if insert_seen(s2) { super::metrics_hdr::record_dedup(shard, true); } else { buf.push((s2,b2)); } } else { break } }
                                drain_with_width(&s, &mut buf, base_width).await;
                            }
                            Some((sig,bot)) = lo_rx.recv() => {
                                if insert_seen(sig) { super::metrics_hdr::record_dedup(shard, false); }
                                else { buf.push((sig,bot)); }
                                hi_burst = 0;
                                while buf.len() < BATCH_BASE { if let Ok((s2,b2)) = lo_rx.try_recv() { if insert_seen(s2) { super::metrics_hdr::record_dedup(shard, false); } else { buf.push((s2,b2)); } } else { break } }
                                drain_with_width(&s, &mut buf, base_width).await;
                            }
                            else => break,
                        }
                    }
                    tokio::task::yield_now().await;
                }
            });
            hi_txs.push(hi_tx); lo_txs.push(lo_tx);
        }
        Self { hi_txs, lo_txs }
    }
    #[inline] fn shard_index(&self, bot: &Pubkey) -> usize { let mut k = 1469598103934665603u64; for b in bot.to_bytes(){ k^=b as u64; k = k.wrapping_mul(1099511628211);} jump_consistent_hash(k, self.hi_txs.len() as i32) as usize }
    #[inline] pub fn dispatch_prio(&self, sig: Signature, bot: Pubkey) { let idx = self.shard_index(&bot); if self.hi_txs[idx].try_send((sig, bot)).is_err() { super::metrics_hdr::record_q_drop(idx, true); } }
    #[inline] pub fn dispatch(&self, sig: Signature, bot: Pubkey) { let idx = self.shard_index(&bot); if self.lo_txs[idx].try_send((sig, bot)).is_err() { super::metrics_hdr::record_q_drop(idx, false); } }
    #[inline] pub fn dispatch_many(&self, items: &[(Signature, Pubkey)], high: bool) { for (sig, bot) in items { if high { self.dispatch_prio(*sig,*bot);} else { self.dispatch(*sig,*bot);} } }
}

// =============== Priority Sharded Ingest (hi/lo queues) ===============
use futures::stream::FuturesUnordered;
use futures::StreamExt;

pub struct PriorityShardedScorer {
    hi_txs: Vec<mpsc::Sender<(Signature, Pubkey)>>,
    lo_txs: Vec<mpsc::Sender<(Signature, Pubkey)>>,
}

impl PriorityShardedScorer {
    pub fn new(scorer: Arc<RivalBotCapabilityScorer>, shards: usize, cap_per_shard: usize) -> Self {
        assert!(shards > 0);
        let mut hi_txs = Vec::with_capacity(shards);
        let mut lo_txs = Vec::with_capacity(shards);
        for _ in 0..shards {
            let (hi_tx, mut hi_rx) = mpsc::channel::<(Signature, Pubkey)>(cap_per_shard);
            let (lo_tx, mut lo_rx) = mpsc::channel::<(Signature, Pubkey)>(cap_per_shard);
            let s = scorer.clone();
            tokio::spawn(async move {
                const BATCH: usize = 64; const WIDTH: usize = 8;
                let mut buf: Vec<(Signature, Pubkey)> = Vec::with_capacity(BATCH);
                loop {
                    tokio::select! {
                        biased;
                        Some(item) = hi_rx.recv() => {
                            buf.clear(); buf.push(item);
                            while buf.len() < BATCH { match hi_rx.try_recv(){ Ok(x)=>buf.push(x), Err(_)=>break } }
                            drain_with_width(&s, &mut buf, WIDTH).await;
                        }
                        Some(item) = lo_rx.recv() => {
                            buf.clear(); buf.push(item);
                            while buf.len() < BATCH { match lo_rx.try_recv(){ Ok(x)=>buf.push(x), Err(_)=>break } }
                            drain_with_width(&s, &mut buf, WIDTH).await;
                        }
                        else => break,
                    }
                }
            });
            hi_txs.push(hi_tx); lo_txs.push(lo_tx);
        }
        Self { hi_txs, lo_txs }
    }
    #[inline] fn shard_index(&self, bot: &Pubkey) -> usize {
        let mut k = 1469598103934665603u64; for b in bot.to_bytes(){ k^=b as u64; k = k.wrapping_mul(1099511628211);} jump_consistent_hash(k, self.hi_txs.len() as i32) as usize
    }
    #[inline] pub fn dispatch_prio(&self, sig: Signature, bot: Pubkey) { let idx = self.shard_index(&bot); let _ = self.hi_txs[idx].try_send((sig, bot)); }
    #[inline] pub fn dispatch(&self, sig: Signature, bot: Pubkey) { let idx = self.shard_index(&bot); let _ = self.lo_txs[idx].try_send((sig, bot)); }
    #[inline] pub fn dispatch_many(&self, items: &[(Signature, Pubkey)], high: bool) { for (sig, bot) in items { if high { self.dispatch_prio(*sig,*bot);} else { self.dispatch(*sig,*bot);} } }
}

// =================== [PIECE 6: Hedged RPC (ULTRA+ v5)] ===================
use std::{sync::Arc, time::{Duration, Instant}};
use moka::future::Cache;
use tokio::sync::{Semaphore, RwLock as TokioRwLock};
use serde_json::Value;
use solana_client::{rpc_request::RpcRequest, rpc_config::RpcTransactionConfig};
use solana_transaction_status::UiTransactionEncoding;
use solana_sdk::signature::Signature;
use solana_transaction_status::EncodedConfirmedTransactionWithStatusMeta;
use fastrand::Rng;
use smallvec::SmallVec;

// =================== [PIECE 20α: P² Online Quantile (SmallVec bootstrap)] ===================
#[derive(Clone, Debug)]
struct P2Quantile {
    p: f64,
    boot: SmallVec<[f64; 5]>,
    q: [f64; 5],
    n: [f64; 5],
    np: [f64; 5],
    dn: [f64; 5],
    ready: bool,
}

impl P2Quantile {
    fn new(p: f64) -> Self {
        assert!((0.0..=1.0).contains(&p));
        Self { p, boot: SmallVec::new(), q: [0.0;5], n:[0.0;5], np:[0.0;5], dn:[0.0;5], ready: false }
    }
    #[inline] fn parabolic(&self, i: usize, d: f64) -> f64 {
        let q_i = self.q[i];
        q_i + d * (
            (self.n[i] - self.n[i-1] + d) * (self.q[i+1] - q_i) / (self.n[i+1]-self.n[i]) +
            (self.n[i+1] - self.n[i] - d) * (q_i - self.q[i-1]) / (self.n[i]  -self.n[i-1])
        ) / (self.n[i+1]-self.n[i-1])
    }
    #[inline] fn linear(&self, i: usize, d: f64) -> f64 {
        let j = (i as isize + d as isize) as usize;
        self.q[i] + d * (self.q[j] - self.q[i]) / (self.n[j] - self.n[i])
    }
    #[inline]
    fn observe(&mut self, x: f64) {
        if !self.ready {
            self.boot.push(x);
            if self.boot.len() < 5 { return; }
            self.boot.sort_by(|a,b| a.partial_cmp(b).unwrap());
            for i in 0..5 { self.q[i] = self.boot[i]; }
            self.n  = [0.0, 1.0, 2.0, 3.0, 4.0];
            self.np = [0.0, 2.0*self.p, 4.0*self.p, 2.0+2.0*self.p, 4.0];
            self.dn = [0.0, self.p/2.0, self.p, (1.0+self.p)/2.0, 1.0];
            self.boot.clear();
            self.ready = true;
            return;
        }
        let k = if x < self.q[0] { self.q[0]=x; 0 }
                else if x < self.q[1] { 0 }
                else if x < self.q[2] { 1 }
                else if x < self.q[3] { 2 }
                else if x <= self.q[4] { 3 } else { self.q[4]=x; 3 };
        for i in k+1..5 { self.n[i] += 1.0; }
        for i in 0..5 { self.np[i] += self.dn[i]; }
        for i in 1..4 {
            let d = self.np[i] - self.n[i];
            let left = self.n[i] - self.n[i-1];
            let right= self.n[i+1] - self.n[i];
            if (d >= 1.0 && right > 1.0) || (d <= -1.0 && left > 1.0) {
                let s = d.signum();
                let q_hat = self.parabolic(i, s);
                self.q[i] = if self.q[i-1] < q_hat && q_hat < self.q[i+1] { q_hat } else { self.linear(i, s) };
                self.n[i] += s;
            }
        }
    }
    #[inline] fn quantile(&self) -> f64 { if !self.ready { 0.0 } else { self.q[2] } }
    /// Optional deadband around current estimate to ignore micro-jitter.
    #[inline]
    fn observe_with_eps(&mut self, x: f64, eps_frac: f64) {
        if self.ready && eps_frac > 0.0 {
            let q2 = self.q[2];
            if q2.is_finite() && q2 > 0.0 {
                let lo = q2 * (1.0 - eps_frac);
                let hi = q2 * (1.0 + eps_frac);
                if x >= lo && x <= hi { return; }
            }
        }
        self.observe(x)
    }

    /// Soft reset when sustained drift persists for N updates; re-centers around q2.
    #[inline]
    fn observe_with_shift_reset(&mut self, x: f64, eps_frac: f64, drift_cap: usize) {
        if !self.ready { self.observe(x); return; }
        let q2 = self.q[2];
        if !q2.is_finite() || q2 <= 0.0 { self.observe(x); return; }
        let lo = q2 * (1.0 - eps_frac);
        let hi = q2 * (1.0 + eps_frac);
        // store drift counter in dn[0]
        if x < lo || x > hi { self.dn[0] = (self.dn[0] + 1.0).min(1e18); } else { self.dn[0] = 0.0; }
        if (self.dn[0] as usize) >= drift_cap {
            let mut v = [self.q[0], self.q[1], q2, self.q[3], self.q[4]];
            v[2] = (v[2] + x) * 0.5; v[1] = (v[1] + v[2]) * 0.5; v[3] = (v[2] + v[3]) * 0.5;
            for i in 0..5 { self.q[i] = v[i]; self.n[i] = i as f64; }
            self.np = [0.0, 2.0*self.p, 4.0*self.p, 2.0+2.0*self.p, 4.0];
            self.dn = [0.0, self.p/2.0, self.p, (1.0+self.p)/2.0, 1.0];
            self.ready = true; return;
        }
        self.observe_with_eps(x, eps_frac);
    }
}

// =================== [PIECE 22+++ : hdr metrics hooks (safe atomics + rolling max)] ===================
mod metrics_hdr {
    use std::sync::atomic::{AtomicU64, Ordering};

    static BH_MS_SUM: AtomicU64 = AtomicU64::new(0);
    static BH_MS_CNT: AtomicU64 = AtomicU64::new(0);
    static BH_MS_MAX: AtomicU64 = AtomicU64::new(0);
    static FEE_SUM:   AtomicU64 = AtomicU64::new(0);
    static FEE_CNT:   AtomicU64 = AtomicU64::new(0);

    const SLOTS: usize = 64;
    static BH_SHARD_SUM: [AtomicU64; SLOTS] = [AtomicU64::new(0); SLOTS];
    static BH_SHARD_CNT: [AtomicU64; SLOTS] = [AtomicU64::new(0); SLOTS];
    // Dedup counters per class (HI/LO)
    static HI_DEDUP: [AtomicU64; SLOTS] = [AtomicU64::new(0); SLOTS];
    static LO_DEDUP: [AtomicU64; SLOTS] = [AtomicU64::new(0); SLOTS];
    // Encoding unsupported counters [json, jsonp, b58, b64, b64z]
    static ENC_UNSUP: [AtomicU64; 5] = [AtomicU64::new(0); 5];

    #[inline]
    pub fn record_bh_ms(idx: usize, ms: f64) {
        let v = if ms.is_finite() && ms >= 0.0 { ms as u64 } else { 0 };
        BH_MS_SUM.fetch_add(v, Ordering::Relaxed);
        BH_MS_CNT.fetch_add(1, Ordering::Relaxed);
        // rolling max
        let mut cur = BH_MS_MAX.load(Ordering::Relaxed);
        while v > cur && BH_MS_MAX.compare_exchange_weak(cur, v, Ordering::Relaxed, Ordering::Relaxed).is_err() {
            cur = BH_MS_MAX.load(Ordering::Relaxed);
        }
        let j = idx & (SLOTS - 1);
        BH_SHARD_SUM[j].fetch_add(v, Ordering::Relaxed);
        BH_SHARD_CNT[j].fetch_add(1, Ordering::Relaxed);
    }

    #[inline]
    pub fn record_fee_heat(_idx: usize, mlcu: f64) {
        let v = if mlcu.is_finite() && mlcu >= 0.0 { mlcu as u64 } else { 0 };
        FEE_SUM.fetch_add(v, Ordering::Relaxed);
        FEE_CNT.fetch_add(1, Ordering::Relaxed);
    }

    #[inline] pub fn avg_bh_ms() -> f64 { (BH_MS_SUM.load(Ordering::Relaxed) as f64) / (BH_MS_CNT.load(Ordering::Relaxed).max(1) as f64) }
    #[inline] pub fn max_bh_ms() -> u64 { BH_MS_MAX.load(Ordering::Relaxed) }
    #[inline] pub fn avg_fee_mlcu() -> f64 { (FEE_SUM.load(Ordering::Relaxed) as f64) / (FEE_CNT.load(Ordering::Relaxed).max(1) as f64) }
    #[inline] pub fn record_enc_unsupported(kind_idx: usize) { if kind_idx < 5 { ENC_UNSUP[kind_idx].fetch_add(1, Ordering::Relaxed); } }
    #[inline] pub fn record_dedup(idx: usize, high: bool) {
        let j = idx & (SLOTS - 1);
        if high { HI_DEDUP[j].fetch_add(1, Ordering::Relaxed); } else { LO_DEDUP[j].fetch_add(1, Ordering::Relaxed); }
    }
}

struct Ep {
    cli: Arc<RpcClient>,
    ewma_ms: f64,
    err_p: f64,
    breaker_until: Instant,
    permits: Arc<Semaphore>,
    rate_backoff_until: Instant,
    rate_backoff_ms: u64,
    // inclusion-aware probes
    last_slot: u64,
    slot_lag_ewma: f64,
    bh_latency_ewma_ms: f64,
    fee_heat_ewma_mlcu: f64,
    // quantiles
    q_bh_p90: P2Quantile,
    q_bh_p99: P2Quantile,
    q_fee_p80: P2Quantile,
    // rate shaping (token bucket)
    tb_tokens: f64,
    tb_capacity: f64,
    tb_refill_per_ms: f64,
    tb_last: Instant,
    // UCB1 stats
    pulls: u64,
    succ: u64,
    ms_sum: f64,
    // learned encoding support mask (1 bit per encoding)
    enc_mask: u8,
}

impl Ep {
    fn score(&self, ms_per_slot: f64, w_lag: f64, w_bh: f64, w_fee: f64) -> f64 {
        // Soft-skip backends that are far behind by adding a large penalty.
        let lag_pen = if self.slot_lag_ewma > 2.5 { 50_000.0 } else { 0.0 };
        let mut s = self.ewma_ms * (1.0 + self.err_p.min(1.0)) + lag_pen;
        if Instant::now() < self.breaker_until { s += 10_000.0; }
        s + w_lag * (self.slot_lag_ewma * ms_per_slot) + w_bh * self.bh_latency_ewma_ms + w_fee * self.fee_heat_ewma_mlcu
    }
    #[inline]
    fn tb_try_take(&mut self) -> bool {
        let now = Instant::now();
        let elapsed_ms = now.saturating_duration_since(self.tb_last).as_millis() as f64;
        if elapsed_ms > 0.0 {
            self.tb_tokens = (self.tb_tokens + elapsed_ms * self.tb_refill_per_ms).min(self.tb_capacity);
            self.tb_last = now;
        }
        if self.tb_tokens >= 1.0 { self.tb_tokens -= 1.0; true } else { false }
    }
    #[inline]
    fn tb_penalize_rate(&mut self) {
        self.tb_capacity = (self.tb_capacity * 0.8).clamp(8.0, 256.0);
        self.tb_refill_per_ms = (self.tb_refill_per_ms * 0.85).clamp(0.005, 0.5);
    }
    #[inline]
    fn tb_reward_rate(&mut self) {
        self.tb_capacity = (self.tb_capacity * 1.02).clamp(8.0, 256.0);
        self.tb_refill_per_ms = (self.tb_refill_per_ms * 1.02).clamp(0.005, 0.5);
    }
    #[inline]
    fn supports(&self, enc: UiTransactionEncoding) -> bool { (self.enc_mask & (1u8 << enc_idx(enc))) != 0 }
    #[inline]
    fn mark_unsupported(&mut self, enc: UiTransactionEncoding) { let i = enc_idx(enc); if i < 8 { self.enc_mask &= !(1u8 << i); super::metrics_hdr::record_enc_unsupported(i); } }
}

#[derive(Clone)]
pub struct HedgedRpc {
    eps: Arc<Vec<tokio::sync::Mutex<Ep>>>,
    min_stagger: Duration,
    cache: Cache<String, Option<EncodedConfirmedTransactionWithStatusMeta>>,
    none_cache: Cache<String, ()>,
    ms_per_slot: Arc<TokioRwLock<f64>>,
    w_lag: f64,
    w_bh: f64,
    w_fee: f64,
    epsilon: f64,
    rng: Arc<Rng>,
    // global concurrency budget
    global_permits: Arc<Semaphore>,
    // UCB1 exploration strength (ms units)
    bandit_c: f64,
}

impl HedgedRpc {
    pub fn new(a: Arc<RpcClient>, b: Arc<RpcClient>, stagger: Duration) -> Self { Self::new_multi(vec![a,b], stagger) }
    pub fn new_multi(clients: Vec<Arc<RpcClient>>, stagger: Duration) -> Self {
        let eps = clients.into_iter().map(|cli| Ep {
            cli,
            ewma_ms: 35.0,
            err_p: 0.0,
            breaker_until: Instant::now(),
            permits: Arc::new(Semaphore::new(64)),
            rate_backoff_until: Instant::now(),
            rate_backoff_ms: 250,
            last_slot: 0,
            slot_lag_ewma: 0.0,
            bh_latency_ewma_ms: 35.0,
            fee_heat_ewma_mlcu: 0.0,
            q_bh_p90: P2Quantile::new(0.90),
            q_bh_p99: P2Quantile::new(0.99),
            q_fee_p80: P2Quantile::new(0.80),
            tb_tokens: 64.0,
            tb_capacity: 64.0,
            tb_refill_per_ms: 0.04,
            tb_last: Instant::now(),
            pulls: 1,
            succ: 1,
            ms_sum: 35.0,
            enc_mask: 0b1_1111,
        }).map(tokio::sync::Mutex::new).collect::<Vec<_>>();
        let this = Self {
            eps: Arc::new(eps),
            min_stagger: stagger.min(Duration::from_millis(25)),
            cache: Cache::builder().time_to_live(Duration::from_millis(650)).time_to_idle(Duration::from_millis(500)).max_capacity(100_000).build(),
            none_cache: Cache::builder().time_to_live(Duration::from_millis(320)).time_to_idle(Duration::from_millis(320)).max_capacity(150_000).build(),
            ms_per_slot: Arc::new(TokioRwLock::new(400.0)),
            w_lag: 0.35,
            w_bh: 0.25,
            w_fee: 0.02,
            epsilon: 0.05,
            rng: Arc::new(Rng::new()),
            global_permits: Arc::new(Semaphore::new(256)),
            bandit_c: 12.0,
        };
        this.spawn_probe_task();
        this
    }
    pub async fn get_tx_fast_cached(&self, sig: &Signature, enc: UiTransactionEncoding) -> Result<Option<EncodedConfirmedTransactionWithStatusMeta>> {
        let key = format!("{}#{}", sig, enc_to_key(enc));
        if self.none_cache.get(&key).is_some() { return Ok(None); }
        Ok(self.cache.get_with(key.clone(), self.fetch(sig.clone(), enc, key)).await)
    }
    pub async fn get_tx_fast(&self, sig: &Signature, enc: UiTransactionEncoding) -> Result<Option<EncodedConfirmedTransactionWithStatusMeta>> { (self.fetch(sig.clone(), enc, String::new())).await.ok_or(None) }
    async fn pick_order(&self) -> Vec<usize> {
        let msps = *self.ms_per_slot.read().await;
        // UCB1 bandit blended with classical score
        let mut T: f64 = 0.0;
        let mut stats: Vec<(usize, f64, f64, f64)> = Vec::with_capacity(self.eps.len());
        for (i, epm) in self.eps.iter().enumerate() {
            let ep = epm.lock().await;
            T += ep.pulls as f64;
            stats.push((i, ep.pulls as f64, (ep.ms_sum / (ep.pulls as f64)).max(1.0), ep.score(msps, self.w_lag, self.w_bh, self.w_fee)));
        }
        if T < 1.0 { T = 1.0; }
        let lnT = T.ln();
        let mut idx_sc: Vec<(usize, f64)> = stats.into_iter().map(|(i, n, mean_ms, sc)| {
            let explore = self.bandit_c * (lnT / n.max(1.0)).sqrt();
            (i, 0.60 * (mean_ms - explore) + 0.40 * sc)
        }).collect();
        idx_sc.sort_by(|a,b| a.1.partial_cmp(&b.1).unwrap());
        let mut order: Vec<usize> = idx_sc.into_iter().map(|(i,_s)| i).collect();
        if order.len() > 3 && self.rng.f64() < self.epsilon {
            let j = 3 + self.rng.usize(..(order.len()-3));
            order.swap(2, j);
        }
        order
    }
    fn fetch(&self, sig: Signature, enc: UiTransactionEncoding, cache_key: String) -> impl std::future::Future<Output = Option<EncodedConfirmedTransactionWithStatusMeta>> + Send + 'static {
        let eps = self.eps.clone(); let cap = self.min_stagger; let this = self.clone(); async move {
            let order = this.pick_order().await; if order.is_empty() { return None; }
            let a = order[0]; let b = *order.get(1).unwrap_or(&a); let c = *order.get(2).unwrap_or(&b); let d = *order.get(3).unwrap_or(&c);
            let msps = *this.ms_per_slot.read().await;
            let dynamic_floor = Duration::from_millis(((msps * 0.06).clamp(5.0, 25.0)) as u64);
            let (base1, base2, base3) = { let ep = eps[a].lock().await; let p99 = ep.q_bh_p99.quantile(); let ref_ms = if p99.is_finite() && p99 > 0.0 { p99 } else { ep.ewma_ms * 1.8 }; ((ref_ms*0.40).max(2.0), (ref_ms*0.75).max(4.0), (ref_ms*1.10).max(6.0)) };
            let j1 = 0.7 + this.rng.f64()*0.6; let j2 = 0.8 + this.rng.f64()*0.6; let j3 = 0.9 + this.rng.f64()*0.6;
            let floor = dynamic_floor.min(cap);
            let stag1 = floor.min(Duration::from_millis((base1*j1) as u64));
            let stag2 = floor.min(Duration::from_millis((base2*j2) as u64));
            let stag3 = floor.min(Duration::from_millis((base3*j3) as u64));

            // meltdown clamp
            let meltdown = crate::metrics_hdr::max_bh_ms() as f64 > 2.5 * msps;

            let f1 = Self::call_endpoint(eps.clone(), a, sig.clone(), enc, this.global_permits.clone());
            let f2 = async { tokio::time::sleep(stag1).await; Self::call_endpoint(eps.clone(), b, sig.clone(), enc, this.global_permits.clone()).await };
            if meltdown {
                let r = tokio::select! { r = f1 => if r.is_some() { r } else { f2.await } };
                if r.is_none() && !cache_key.is_empty() { let _ = this.none_cache.insert(cache_key, ()); }
                return r;
            }
            let f3 = async { tokio::time::sleep(stag2).await; Self::call_endpoint(eps.clone(), c, sig.clone(), enc, this.global_permits.clone()).await };
            let f4 = async { tokio::time::sleep(stag3).await; Self::call_endpoint(eps.clone(), d, sig.clone(), enc, this.global_permits.clone()).await };
            let r = tokio::select! { r = f1 => if r.is_some() { r } else { tokio::select! { r2 = f2 => if r2.is_some() { r2 } else { tokio::select! { r3 = f3 => if r3.is_some() { r3 } else { f4.await }, r4 = f4 => r4, } }, r3 = f3 => if r3.is_some() { r3 } else { f4.await }, r4 = f4 => r4, } } };
            if r.is_none() && !cache_key.is_empty() { let _ = this.none_cache.insert(cache_key, ()); }
            r
        } }
    async fn call_endpoint(eps: Arc<Vec<tokio::sync::Mutex<Ep>>>, idx: usize, sig: Signature, enc_req: UiTransactionEncoding, global: Arc<Semaphore>) -> Option<EncodedConfirmedTransactionWithStatusMeta> {
        {
            let mut ep = eps[idx].lock().await;
            if Instant::now() < ep.rate_backoff_until { return None; }
            if !ep.tb_try_take() { return None; }
        }
        let (cli, permits, mask) = { let ep = eps[idx].lock().await; (ep.cli.clone(), ep.permits.clone(), ep.enc_mask) };
        let (permit_local, permit_global) = match (permits.try_acquire_owned(), global.try_acquire_owned()) { (Ok(l), Ok(g)) => (l, g), _ => return None };
        // choose encoding respecting learned mask
        let mut enc = if (mask & (1u8 << enc_idx(enc_req))) != 0 { enc_req } else { enc_fallback_list().into_iter().find(|&e| (mask & (1u8 << enc_idx(e))) != 0).unwrap_or(UiTransactionEncoding::Base64) };
        let start = Instant::now();
        let timeout_ms = { let ep = eps[idx].lock().await; let p99 = ep.q_bh_p99.quantile(); ((if p99.is_finite() && p99 > 0.0 { p99 } else { ep.ewma_ms * 2.0 }) * 1.12).clamp(120.0, 1500.0) } as u64;
        let initial_budget = (timeout_ms as f64 * 0.45) as u64;
        // Confirmed leg
        let cfg_conf = RpcTransactionConfig { encoding: Some(enc), commitment: Some(CommitmentConfig::confirmed()), max_supported_transaction_version: Some(0), ..Default::default() };
        let res = tokio::time::timeout(Duration::from_millis(initial_budget), cli.get_transaction_with_config(&sig, cfg_conf)).await;
        {
            let dt_ms = start.elapsed().as_secs_f64()*1000.0;
            let mut ep = eps[idx].lock().await; ep.pulls += 1; ep.ms_sum += dt_ms;
        }
        // Handle confirmed leg quickly
        match res {
            Ok(Ok(Some(v))) => {
                let mut ep = eps[idx].lock().await; ep.err_p = (ep.err_p*0.85).max(0.0); ep.rate_backoff_ms=250; ep.tb_reward_rate(); ep.succ+=1; drop(permit_local); drop(permit_global); return Some(v)
            }
            Ok(Ok(None)) => { let mut ep = eps[idx].lock().await; ep.err_p = (ep.err_p + 0.02).min(1.0); }
            Ok(Err(e)) => {
                let msg = e.to_string();
                if msg.contains("unsupported") || msg.contains("encoding") {
                    let mut ep = eps[idx].lock().await; ep.mark_unsupported(enc);
                    if let Some(next_enc) = enc_fallback_list().into_iter().find(|&en| ep.supports(en)) { enc = next_enc; }
                } else if msg.contains("Too many request") || msg.contains("-32005") {
                    let mut ep = eps[idx].lock().await; let jitter = 0.5 + fastrand::f64(); let next = ((ep.rate_backoff_ms as f64 * 1.5).min(3000.0) * jitter) as u64; ep.rate_backoff_ms = next.max(250); ep.rate_backoff_until = Instant::now() + Duration::from_millis(ep.rate_backoff_ms); ep.tb_penalize_rate();
                } else { let mut ep = eps[idx].lock().await; if ep.err_p > 0.80 { ep.breaker_until = Instant::now() + Duration::from_millis(250); } }
            }
            Err(_) => { let mut ep = eps[idx].lock().await; ep.err_p=(ep.err_p+0.25).min(1.0); ep.breaker_until=Instant::now()+Duration::from_millis(250); drop(permit_local); drop(permit_global); return None }
        }
        // Finalized leg within remaining budget
        let elapsed = start.elapsed();
        let remaining = Duration::from_millis(timeout_ms).saturating_sub(elapsed);
        if remaining <= Duration::from_millis(20) { drop(permit_local); drop(permit_global); return None; }
        let cfg_fin = RpcTransactionConfig { encoding: Some(enc), commitment: Some(CommitmentConfig::finalized()), max_supported_transaction_version: Some(0), ..Default::default() };
        let fin = tokio::time::timeout(remaining, cli.get_transaction_with_config(&sig, cfg_fin)).await;
        let out = match fin {
            Ok(Ok(Some(v))) => { let mut ep = eps[idx].lock().await; ep.err_p=(ep.err_p*0.85).max(0.0); ep.rate_backoff_ms=250; ep.tb_reward_rate(); ep.succ+=1; Some(v) }
            Ok(Ok(None)) => { let mut ep = eps[idx].lock().await; ep.err_p=(ep.err_p+0.02).min(1.0); None }
            Ok(Err(e)) => { let msg = e.to_string(); if msg.contains("unsupported") || msg.contains("encoding") { let mut ep = eps[idx].lock().await; ep.mark_unsupported(enc); } else if msg.contains("Too many request") || msg.contains("-32005") { let mut ep = eps[idx].lock().await; let jitter = 0.5 + fastrand::f64(); let next = ((ep.rate_backoff_ms as f64 * 1.5).min(3000.0) * jitter) as u64; ep.rate_backoff_ms = next.max(250); ep.rate_backoff_until = Instant::now() + Duration::from_millis(ep.rate_backoff_ms); ep.tb_penalize_rate(); } else { let mut ep = eps[idx].lock().await; ep.err_p=(ep.err_p+0.20).min(1.0); } None }
            Err(_) => { let mut ep = eps[idx].lock().await; ep.err_p=(ep.err_p+0.25).min(1.0); ep.breaker_until=Instant::now()+Duration::from_millis(250); None }
        };
        drop(permit_local); drop(permit_global);
        out
    }
    pub async fn estimate_slot_ms(&self) -> Option<f64> { Some(*self.ms_per_slot.read().await) }

    #[derive(Clone, Debug)]
    pub struct EndpointSnapshot { pub index: usize, pub ewma_ms: f64, pub err_p: f64, pub slot_lag_ewma: f64, pub bh_latency_ewma_ms: f64, pub fee_heat_ewma_mlcu: f64, pub bh_p90_ms: f64, pub bh_p99_ms: f64, pub fee_p80_mlcu: f64, pub enc_mask: u8 }
    pub async fn endpoints_snapshot(&self) -> Vec<EndpointSnapshot> { let mut out = Vec::with_capacity(self.eps.len()); for (i, epm) in self.eps.iter().enumerate() { let ep = epm.lock().await; out.push(EndpointSnapshot { index: i, ewma_ms: ep.ewma_ms, err_p: ep.err_p, slot_lag_ewma: ep.slot_lag_ewma, bh_latency_ewma_ms: ep.bh_latency_ewma_ms, fee_heat_ewma_mlcu: ep.fee_heat_ewma_mlcu, bh_p90_ms: ep.q_bh_p90.quantile(), bh_p99_ms: ep.q_bh_p99.quantile(), fee_p80_mlcu: ep.q_fee_p80.quantile(), enc_mask: ep.enc_mask }); } out }
    fn spawn_probe_task(&self) {
        let eps = self.eps.clone(); let msps = self.ms_per_slot.clone(); let none = self.none_cache.clone();
        tokio::spawn(async move {
            let alpha = 0.3; let bh_alpha = 0.25; let fee_alpha = 0.2; let mut last_max_slot = 0u64;
            loop {
                let mut max_slot = 0u64;
                // probe endpoints
                for (i, epm) in eps.iter().enumerate() {
                    let (cli, permits) = { let ep = epm.lock().await; (ep.cli.clone(), ep.permits.clone()) };
                    if let Ok(permit) = permits.try_acquire_owned() {
                        let t0 = Instant::now();
                        let slot_res = cli.get_slot().await; let dt_slot = t0.elapsed();
                        let t1 = Instant::now(); let _ = cli.get_latest_blockhash().await; let dt_bh = t1.elapsed();
                        drop(permit);
                        // fee heat via raw RPC
                        let fee_mlcu = match cli.send::<Value>(RpcRequest::GetRecentPrioritizationFees, serde_json::json!([])).await {
                            Ok(Value::Array(a)) if !a.is_empty() => {
                                let mut v: Vec<f64> = a.iter().filter_map(|e| e.get("prioritizationFee").and_then(|x| x.as_u64()).map(|u| u as f64)).collect();
                                if v.is_empty() { 0.0 } else { v.sort_by(|x,y| x.partial_cmp(y).unwrap()); let idx = ((0.80f64 * ((v.len()-1) as f64)).round() as usize).min(v.len()-1); v[idx] }
                            }
                            _ => 0.0
                        };
                        if let Ok(slot) = slot_res { let mut ep = epm.lock().await; ep.last_slot = slot; let bh_ms = dt_bh.as_secs_f64()*1000.0; ep.bh_latency_ewma_ms = (1.0 - bh_alpha) * ep.bh_latency_ewma_ms + bh_alpha * bh_ms; ep.fee_heat_ewma_mlcu = (1.0 - fee_alpha) * ep.fee_heat_ewma_mlcu + fee_alpha * fee_mlcu; ep.q_bh_p90.observe_with_shift_reset(bh_ms, 0.02, 64);
                            ep.q_bh_p99.observe_with_shift_reset(bh_ms, 0.02, 64);
                            crate::metrics_hdr::record_bh_ms(i, bh_ms);
                            crate::metrics_hdr::record_fee_heat(i, if fee_mlcu>0.0 { fee_mlcu } else { 0.0 });
                            if slot > max_slot { max_slot = slot; }
                        }
                    }
                }
                for epm in eps.iter() { let mut ep = epm.lock().await; let lag = max_slot.saturating_sub(ep.last_slot) as f64; ep.slot_lag_ewma = (1.0 - alpha) * ep.slot_lag_ewma + alpha * lag; }
                if max_slot > last_max_slot { last_max_slot = max_slot; none.invalidate_all(); }
                // refresh ms_per_slot from best effort single sample
                if let Some(first) = eps.get(0) { if let Ok(ep) = first.try_lock() { let cli = ep.cli.clone(); drop(ep); if let Ok(mut s) = cli.get_recent_performance_samples(1).await { if let Some(sm) = s.pop() { if sm.num_slots>0 && sm.sample_period_secs>0 { let est = 1000.0*(sm.sample_period_secs as f64)/(sm.num_slots as f64); let mut g = msps.write().await; *g = est.clamp(300.0,1200.0); } } } } }
                tokio::time::sleep(Duration::from_millis(250)).await;
            }
        });
    }
}

#[inline]
fn enc_to_key(enc: UiTransactionEncoding) -> &'static str {
    match enc {
        UiTransactionEncoding::Json => "json",
        UiTransactionEncoding::JsonParsed => "jsonp",
        UiTransactionEncoding::Base58 => "b58",
        UiTransactionEncoding::Base64 => "b64",
        UiTransactionEncoding::Base64Zstd => "b64z",
        _ => "other",
    }
}

#[inline]
fn enc_idx(enc: UiTransactionEncoding) -> usize {
    match enc { UiTransactionEncoding::Json => 0, UiTransactionEncoding::JsonParsed => 1, UiTransactionEncoding::Base58 => 2, UiTransactionEncoding::Base64 => 3, UiTransactionEncoding::Base64Zstd => 4, _ => 0 }
}

#[inline]
fn enc_fallback_list() -> [UiTransactionEncoding; 5] {
    [UiTransactionEncoding::Base64, UiTransactionEncoding::Base64Zstd, UiTransactionEncoding::Base58, UiTransactionEncoding::Json, UiTransactionEncoding::JsonParsed]
}

/// Jump Consistent Hash: stable, minimal movement when shards change.
/// Ref: Lamping/Veasey (Google) — perfect for bot sharding.
#[inline]
pub fn jump_consistent_hash(key: u64, num_buckets: i32) -> i32 {
    let mut b = -1i64;
    let mut j = 0i64;
    let mut k = key as i64;
    while j < num_buckets as i64 {
        b = j;
        k = k.wrapping_mul(2862933555777941757).wrapping_add(1);
        let x = (((b + 1) as f64) * ((1u64 << 31) as f64 / (((k >> 33) + 1) as f64))) as i64;
        j = x;
    }
    b as i32
}

use solana_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::{
    pubkey::Pubkey,
    signature::Signature,
    commitment_config::CommitmentConfig,
};
use solana_transaction_status::{
    EncodedConfirmedTransactionWithStatusMeta,
    TransactionStatusMeta,
    UiTransactionEncoding,
};
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::sync::{broadcast, Mutex, RwLock as AsyncRwLock, mpsc, RwLock};
use tonic::transport::Channel;
use yellowstone_grpc_client::jito_service_client::JitoServiceClient; // yellowstone-grpc-client = "6.0"
use prometheus::{Encoder, TextEncoder, IntGauge, IntCounter, Registry, Histogram, HistogramOpts};
use lazy_static::lazy_static;
use serde::{Serialize, Deserialize};
use anyhow::{anyhow, Result};
use log::{info, warn, error, debug};
use uuid::Uuid;
use chrono::Utc;
use rusqlite;
use bs58;
use dashmap::DashMap;
use serde_json;
use reqwest;
use pyth_client::Price as PythPrice;
use switchboard_v2::{AggregatorAccountData, VrfAccountData};

// =================== [PIECE 5: Prometheus metrics registry & histograms] ===================
lazy_static! {
    pub static ref REGISTRY: Registry = Registry::new_custom(Some("rival_scorer".into()), None).unwrap();
    pub static ref TX_COUNTER: IntCounter = IntCounter::new("tx_tracked_total", "Tracked txs").unwrap();
    pub static ref AVG_CU_GAUGE: IntGauge = IntGauge::new("avg_cu", "Average compute units").unwrap();
    pub static ref LATENCY_HIST: Histogram = Histogram::with_opts(
        HistogramOpts::new("rpc_latency_ms", "RPC latency ms")
            .buckets(vec![1.0, 2.0, 5.0, 10.0, 25.0, 50.0, 100.0, 250.0, 500.0])
    ).unwrap();
    pub static ref INCLUSION_MS: Histogram = Histogram::with_opts(
        HistogramOpts::new("inclusion_latency_ms", "Block inclusion latency ms")
            .buckets(vec![10.0, 25.0, 50.0, 100.0, 250.0, 500.0, 1000.0, 2500.0, 5000.0])
    ).unwrap();
}

pub fn init_metrics() {
    let _ = REGISTRY.register(Box::new(TX_COUNTER.clone()));
    let _ = REGISTRY.register(Box::new(AVG_CU_GAUGE.clone()));
    let _ = REGISTRY.register(Box::new(LATENCY_HIST.clone()));
    let _ = REGISTRY.register(Box::new(INCLUSION_MS.clone()));
}

pub fn render_prometheus() -> String {
    let mf = REGISTRY.gather();
    let mut buf = Vec::new();
    let enc = TextEncoder::new();
    let _ = enc.encode(&mf, &mut buf);
    String::from_utf8_lossy(&buf).to_string()
}

#[derive(Debug, Clone)]
pub struct ScoringConfig {
    pub score_update_threshold: usize,
    pub score_refresh_interval: u64,
    pub entropy_threshold: f64,
    pub transition_novelty_threshold: u64,
    pub burst_speed_threshold: f64,
    pub max_transaction_history: usize,
    pub inactive_bot_threshold: u64,
    pub slot_time_ms: u64,
    pub max_tracked_bots: usize,
    pub capability_score_window_secs: u64,
    pub min_tx_for_scoring: usize,
    pub score_update_interval_secs: u64,
    pub max_historical_data_points: usize,
    pub max_latency_tracking: usize,
    pub critical_success_threshold: f64,
    pub high_frequency_threshold: u64,
    pub priority_fee_percentile: f64,
    pub latency_update_threshold: usize,
    pub profit_weight: f64,
    pub consistency_weight: f64,
    pub speed_weight: f64,
    pub success_weight: f64,
    pub gas_efficiency_weight: f64,
    pub pattern_consistency_weight: f64,
    pub volume_weight: f64,
}

impl Default for ScoringConfig {
    fn default() -> Self {
        Self {
            score_update_threshold: 25,
            score_refresh_interval: 60,
            entropy_threshold: 1.2,
            transition_novelty_threshold: 3,
            burst_speed_threshold: 250.0,
            max_transaction_history: 1000,
            inactive_bot_threshold: 86400,
            slot_time_ms: 400,
            max_tracked_bots: 500,
            capability_score_window_secs: 300,
            min_tx_for_scoring: 10,
            score_update_interval_secs: 5,
            max_historical_data_points: 1000,
            max_latency_tracking: 1000,
            critical_success_threshold: 0.85,
            high_frequency_threshold: 100,
            priority_fee_percentile: 0.95,
            latency_update_threshold: 50,
            profit_weight: 0.5,
            consistency_weight: 0.5,
            speed_weight: 0.25,
            success_weight: 0.35,
            gas_efficiency_weight: 0.15,
            pattern_consistency_weight: 0.15,
            volume_weight: 0.10,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RivalBotCapability {
    pub bot_pubkey: String,
    pub score: f64,
    pub arbitrage_rate: f64,
    pub liquidation_rate: f64,
    pub sandwich_rate: f64,
    pub avg_speed_ms: f64,
    pub success_rate: f64,
    pub avg_compute_units: f64,
    pub avg_priority_fee: f64,
    pub pattern_consistency: f64,
    pub avg_profit: f64,
    // New latency metrics
    pub avg_rpc_latency_ms: f64,
    pub avg_confirmation_latency_ms: f64,
    pub avg_block_inclusion_latency_ms: f64,
    pub detected_strategies: Vec<BotStrategy>,
    pub threat_level: ThreatLevel,
    pub anomalies: Vec<String>,
    pub rival_comparison_rank: Option<f64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ThreatLevel {
    Low,
    Medium,
    High,
    Critical,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum BotStrategy {
    Arbitrage,
    Liquidation,
    SandwichAttack,
    FrontRunning,
    BackRunning,
    JitLiquidity,
    CopyTrading,
    Unknown,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransactionMetrics {
    pub signature: String,
    pub bot_pubkey: String,
    pub slot: u64,
    pub timestamp: u64,
    pub success: bool,
    pub priority_fee: u64,
    pub compute_units: u64,
    pub rpc_latency_ms: f64,
    pub confirmation_latency_ms: f64,
    pub block_inclusion_latency_ms: f64,
    pub bundle_id: Option<String>,
    pub tip_bid: Option<u64>,
    pub strategy_type: Vec<BotStrategy>,
    pub profit_estimate: Option<f64>,
    pub oracle_age_secs: Option<u64>,
    pub oracle_confidence: Option<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BotProfile {
    pub pubkey: String,
    pub transactions: VecDeque<TransactionMetrics>,
    pub cumulative_metrics: CumulativeMetrics,
    // ===== PIECE 12Δ: decayed online metrics
    pub dec_success_ewma: f64,
    pub dec_fee_ewma: f64,
    pub dec_cu_ewma: f64,
    pub dec_speed_ms_ewma: f64,
    pub last_decayed_ts: u64,
    pub last_score_update: Option<u64>,
    pub last_score_value: Option<f64>,
    pub strategy_transitions: HashMap<(BotStrategy, BotStrategy), u64>,
    pub last_strategy: Option<BotStrategy>,
    pub last_seen: u64, // tracks last activity timestamp
    pub scoring_config: ScoringConfig,
    // half-lives (seconds)
    pub hl_success: f64,
    pub hl_fee: f64,
    pub hl_cu: f64,
    pub hl_speed: f64,
}

impl BotProfile {
    pub fn new(pubkey: String, scoring_config: ScoringConfig) -> Self {
        Self {
            pubkey,
            transactions: VecDeque::new(),
            cumulative_metrics: CumulativeMetrics::default(),
            dec_success_ewma: 0.0,
            dec_fee_ewma: 0.0,
            dec_cu_ewma: 0.0,
            dec_speed_ms_ewma: 0.0,
            last_decayed_ts: 0,
            last_score_update: None,
            last_score_value: None,
            strategy_transitions: HashMap::new(),
            last_strategy: None,
            last_seen: 0,
            scoring_config,
            hl_success: 60.0,
            hl_fee: 30.0,
            hl_cu: 30.0,
            hl_speed: 30.0,
        }
    }

    pub fn add_transaction(&mut self, metrics: TransactionMetrics) {
        self.last_seen = metrics.timestamp; // Update last activity time
        self.transactions.push_back(metrics);
        if self.transactions.len() > self.scoring_config.max_transaction_history {
            self.transactions.pop_front();
        }
        // Update cumulative and transitions
        let last = self.transactions.back().unwrap().clone();
        self.update_cumulative_metrics(&last);
        if let Some(prev) = self.last_strategy.take() {
            let cur = last.strategy_type[0].clone();
            *self.strategy_transitions.entry((prev.clone(), cur.clone())).or_insert(0) += 1;
            self.last_strategy = Some(cur);
        } else {
            self.last_strategy = Some(last.strategy_type[0].clone());
        }
    }

    pub fn update_cumulative_metrics(&mut self, metrics: &TransactionMetrics) {
        self.cumulative_metrics.total_txs += 1;
        if metrics.success {
            self.cumulative_metrics.successful_txs += 1;
        }
        self.cumulative_metrics.total_priority_fees += metrics.priority_fee;
        self.cumulative_metrics.total_compute_units += metrics.compute_units;
        *self.cumulative_metrics.strategy_counts.entry(metrics.strategy_type[0].clone()).or_insert(0) += 1;
        self.cumulative_metrics.slot_intervals.push_back(metrics.slot);
        if self.cumulative_metrics.slot_intervals.len() > 1024 {
            let _ = self.cumulative_metrics.slot_intervals.pop_front();
        }
        self.cumulative_metrics.rpc_latency_samples.push_back(metrics.rpc_latency_ms);
        self.cumulative_metrics.confirmation_latency_samples.push_back(metrics.confirmation_latency_ms);
        self.cumulative_metrics.block_inclusion_latency_samples.push_back(metrics.block_inclusion_latency_ms);
        if let Some(profit) = metrics.profit_estimate {
            self.cumulative_metrics.profits.push(profit);
        }
        // ===== PIECE 12Δ: decayed updates (half-life EWMAs)
        let prev = self.last_decayed_ts;
        let now = metrics.timestamp;
        let dt = if prev == 0 { 1 } else { now.saturating_sub(prev).max(1) } as f64;
        let alpha = |hl: f64| -> f64 { 1.0 - 2f64.powf(-(dt/hl).max(0.0)) };
        let a_s = alpha(self.hl_success);
        let a_f = alpha(self.hl_fee);
        let a_c = alpha(self.hl_cu);
        let a_v = alpha(self.hl_speed);
        let speed_now = self.cumulative_metrics.avg_speed_ms(self.scoring_config.slot_time_ms);
        self.dec_success_ewma = (1.0 - a_s) * self.dec_success_ewma + a_s * (metrics.success as u8 as f64);
        self.dec_fee_ewma     = (1.0 - a_f) * self.dec_fee_ewma     + a_f * (metrics.priority_fee as f64);
        self.dec_cu_ewma      = (1.0 - a_c) * self.dec_cu_ewma      + a_c * (metrics.compute_units as f64);
        self.dec_speed_ms_ewma= (1.0 - a_v) * self.dec_speed_ms_ewma+ a_v * speed_now.max(0.0);
        self.last_decayed_ts = now;
    }

    pub async fn should_update_score(&self) -> bool {
        use std::time::{SystemTime, UNIX_EPOCH};

        // 1. Hard gate — if we don't have a baseline of data, skip
        if self.transactions.len() < self.scoring_config.score_update_threshold {
            return false;
        }

        // 2. Time-based decay — update at least once per refresh window
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        if let Some(last_update) = self.last_score_update {
            if now.saturating_sub(last_update) > self.scoring_config.score_refresh_interval {
                return true;
            }
        }

        // 3. Strategy entropy — if entropy is above threshold, force update
        let entropy = self.compute_strategy_entropy();
        if entropy > self.scoring_config.entropy_threshold {
            return true;
        }

        // 4. Transition anomaly — if recent transactions show new or rare strategy switches
        if let Some(last) = self.transactions.back() {
            if let Some(prev) = &self.last_strategy {
                if &last.strategy_type[0] != prev {
                    let key = (prev.clone(), last.strategy_type[0].clone());
                    let count = self.strategy_transitions.get(&key).copied().unwrap_or(0);
                    if count < self.scoring_config.transition_novelty_threshold {
                        return true;
                    }
                }
            }
        }

        // 5. Burst activity — if avg speed is unusually high, refresh more often
        let avg_speed = self.cumulative_metrics.avg_speed_ms(self.scoring_config.slot_time_ms);
        if avg_speed > self.scoring_config.burst_speed_threshold {
            return true;
        }

        // Otherwise, skip — no significant signal yet
        false
    }

    pub fn compute_strategy_entropy(&self) -> f64 {
        let total: u64 = self.cumulative_metrics.strategy_counts.values().sum();
        if total == 0 { return 0.0; }

        self.cumulative_metrics.strategy_counts.values()
            .map(|&count| {
                let p = count as f64 / total as f64;
                -p * p.log2()
            })
            .sum()
    }
}

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct CumulativeMetrics {
    pub total_txs: u64,
    pub successful_txs: u64,
    pub total_priority_fees: u64,
    pub total_compute_units: u64,
    pub strategy_counts: HashMap<BotStrategy, u64>,
    pub slot_intervals: VecDeque<u64>,
    pub rpc_latency_samples: VecDeque<f64>,
    pub confirmation_latency_samples: VecDeque<f64>,
    pub block_inclusion_latency_samples: VecDeque<f64>,
    pub profits: Vec<f64>,
}

impl CumulativeMetrics {
    pub fn avg_speed_ms(&self, slot_time_ms: u64) -> f64 {
        if self.slot_intervals.len() < 2 { return 0.0; }
        let mut diffs = 0u64; let mut cnt = 0usize;
        for (a,b) in self.slot_intervals.iter().zip(self.slot_intervals.iter().skip(1)) {
            diffs = diffs.saturating_add(b.saturating_sub(*a));
            cnt += 1;
        }
        if cnt == 0 { return 0.0; }
        (diffs as f64 / cnt as f64) * (slot_time_ms as f64)
    }
}

pub struct RivalBotCapabilityScorer {
    rpc_client: Arc<RpcClient>,
    hedged: HedgedRpc,
    bot_profiles: Arc<DashMap<String, Arc<tokio::sync::RwLock<BotProfile>>>>,
    capability_scores: Arc<DashMap<String, f64>>,
    topk: Arc<tokio::sync::Mutex<TopK>>,
    known_bot_addresses: Arc<AsyncRwLock<HashMap<String, bool>>>,
    scoring_config: ScoringConfig,
    jito_grpc_client: JitoServiceClient<Channel>,
    config: Config,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ScoringWeights {
    speed_weight: f64,
    success_weight: f64,
    gas_efficiency_weight: f64,
    pattern_consistency_weight: f64,
    volume_weight: f64,
    profit_weight: f64,
    consistency_weight: f64,
}

impl Default for ScoringWeights {
    fn default() -> Self {
        Self {
            speed_weight: 0.25,
            success_weight: 0.35,
            gas_efficiency_weight: 0.15,
            pattern_consistency_weight: 0.15,
            volume_weight: 0.10,
            profit_weight: 0.5,
            consistency_weight: 0.5,
        }
    }
}

impl RivalBotCapabilityScorer {
    pub async fn new(rpc_endpoint: &str, jito_grpc_endpoint: &str, config: Config) -> Result<Self, Box<dyn std::error::Error>> {
        let rc1 = Arc::new(make_rpc_client(rpc_endpoint));
        let rc2 = Arc::new(make_rpc_client(rpc_endpoint));
        let rpc_client = rc1.clone();
        let hedged = HedgedRpc::new(rc1, rc2, Duration::from_millis(15));

        let channel = Channel::from_shared(jito_grpc_endpoint.to_string())?
            .connect().await?;
        let jito_grpc_client = JitoServiceClient::new(channel);

        Ok(Self {
            rpc_client,
            bot_profiles: Arc::new(DashMap::new()),
            capability_scores: Arc::new(DashMap::new()),
            topk: Arc::new(tokio::sync::Mutex::new(TopK::new(100))),
            known_bot_addresses: Arc::new(AsyncRwLock::new(HashMap::new())),
            scoring_config: ScoringConfig::default(),
            jito_grpc_client,
            config,
            hedged,
        })
    }

    pub async fn track_transaction(
        &self,
        tx_sig: &Signature,
        suspected_bot: &Pubkey,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let start = Instant::now();
        
        // Fetch transaction and extract metrics
        let metrics = self.extract_transaction_metrics(tx_sig, suspected_bot).await?;
        
        // Get or create profile with proper async locking
        let bot_key = suspected_bot.to_string();
        let profile_arc = self.bot_profiles
            .entry(bot_key.clone())
            .or_insert_with(|| Arc::new(tokio::sync::RwLock::new(BotProfile::new(bot_key.clone(), self.scoring_config.clone()))))
            .value()
            .clone();
        
        // Async lock inside profile
        {
            let mut profile = profile_arc.write().await;
            // PIECE 8: dynamic slot-time smoothing from hedger estimate
            if let Some(ms) = self.hedged.estimate_slot_ms().await {
                profile.scoring_config.slot_time_ms = ((profile.scoring_config.slot_time_ms as f64 * 0.8) + (ms * 0.2)) as u64;
            }
            profile.add_transaction(metrics);
            
            if profile.should_update_score().await {
                let now = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs();
                let capability = self.calculate_capability_score(&profile).await;
                profile.last_score_update = Some(now);
                profile.last_score_value = Some(capability.score);
                
                // Clone before inserting to avoid holding lock
                let bot_pubkey = profile.pubkey.clone();
                drop(profile); // Release lock before inserting
                self.capability_scores.insert(bot_pubkey.clone(), capability.score);
                // PIECE 9: feed TopK
                let mut tk = self.topk.lock().await;
                tk.update(bot_pubkey, capability.score);
            }
        }
        
        TX_COUNTER.inc();
        LATENCY_HIST.observe(start.elapsed().as_millis() as f64);
        Ok(())
    }

    async fn extract_transaction_metrics(
        &self,
        tx_sig: &Signature,
        suspected_bot: &Pubkey,
    ) -> Result<TransactionMetrics, Box<dyn std::error::Error>> {
        // PIECE 7: coalesced cached fetch via hedged RPC pool
        let tx_opt = self.hedged.get_tx_fast_cached(tx_sig, UiTransactionEncoding::JsonParsed).await?;
        
        let tx = tx_opt.ok_or_else(|| anyhow!("Transaction not found"))?;
        let meta = tx.transaction.meta.as_ref().ok_or_else(|| anyhow!("Transaction metadata missing"))?;
        
        let mut metrics = TransactionMetrics {
            signature: tx_sig.to_string(),
            bot_pubkey: suspected_bot.to_string(),
            slot: tx.slot,
            timestamp: SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_secs(),
            success: meta.err.is_none(),
            priority_fee: meta.fee,
            compute_units: meta.compute_units_consumed.unwrap_or(0),
            rpc_latency_ms: 0.0, // Will be set later
            confirmation_latency_ms: 0.0, // Will be set later
            block_inclusion_latency_ms: 0.0, // Will be set later
            bundle_id: None,
            tip_bid: None,
            strategy_type: self.detect_strategy_from_transaction(&tx)?,
            profit_estimate: self.compute_profit_estimate(meta),
            oracle_age_secs: None,
            oracle_confidence: None,
        };
        
        // Fetch Jito bundle stats if available
        if let Ok(stats) = self.fetch_jito_bundle_stats(&tx_sig.to_string()).await {
            metrics.bundle_id = stats.bundle_id;
            metrics.tip_bid = stats.tip_bid;
        }
        
        Ok(metrics)
    }

    fn detect_strategy_from_transaction(&self, tx: &EncodedConfirmedTransactionWithStatusMeta) -> Vec<BotStrategy> {
        if let Some(meta) = &tx.transaction.meta {
            let pre_balances = &meta.pre_balances;
            let post_balances = &meta.post_balances;
            let log_messages = &meta.log_messages.as_ref().unwrap_or(&vec![]);
            
            let has_swap = log_messages.iter().any(|log| 
                log.contains("Swap") || log.contains("swap") || 
                log.contains("Exchange") || log.contains("exchange")
            );
            
            let has_liquidation = log_messages.iter().any(|log|
                log.contains("Liquidate") || log.contains("liquidate") ||
                log.contains("Liquidation") || log.contains("liquidation")
            );
            
            let balance_changes: Vec<i64> = pre_balances.iter()
                .zip(post_balances.iter())
                .map(|(pre, post)| *post as i64 - *pre as i64)
                .collect();
            
            let distinct_changes = balance_changes.iter()
                .filter(|&&change| change != 0)
                .count();
            
            let mut strategies = Vec::new();
            
            if has_liquidation {
                strategies.push(BotStrategy::Liquidation);
            }
            
            if has_swap && distinct_changes >= 4 {
                let positive_changes = balance_changes.iter().filter(|&&c| c > 0).count();
                let negative_changes = balance_changes.iter().filter(|&&c| c < 0).count();
                
                if positive_changes >= 2 && negative_changes >= 2 {
                    strategies.push(BotStrategy::Arbitrage);
                }
            }
            
            if distinct_changes >= 3 && log_messages.len() > 10 {
                strategies.push(BotStrategy::SandwichAttack);
            }
            
            if has_swap && distinct_changes == 2 {
                strategies.push(BotStrategy::FrontRunning);
            }
            
            if strategies.is_empty() {
                strategies.push(BotStrategy::Unknown);
            }
            // PIECE 21: merge fast program-set classifier hints
            for x in classify_program_sets_fast(tx) { if !strategies.contains(&x) { strategies.push(x); } }
            
            strategies
        } else {
            vec![BotStrategy::Unknown]
        }
    }

    fn compute_profit_estimate(&self, meta: &TransactionStatusMeta) -> Option<f64> {
        use std::collections::HashMap;
        #[derive(Hash, Eq, PartialEq)] struct Key { acc: u16, mint: String }
        let mut pre: HashMap<Key, i128> = HashMap::new();
        let mut post: HashMap<Key, i128> = HashMap::new();
        if let Some(bals) = &meta.pre_token_balances {
            for b in bals { let amt = b.ui_token_amount.amount.parse::<i128>().ok()?; pre.insert(Key{acc:b.account_index as u16, mint:b.mint.clone()}, amt); }
        }
        if let Some(bals) = &meta.post_token_balances {
            for b in bals { let amt = b.ui_token_amount.amount.parse::<i128>().ok()?; post.insert(Key{acc:b.account_index as u16, mint:b.mint.clone()}, amt); }
        }
        let mut deltas_pos: i128 = 0;
        for (k, pre_amt) in pre.into_iter() { let post_amt = *post.get(&k).unwrap_or(&0); let d = post_amt.saturating_sub(pre_amt); if d > 0 { deltas_pos = deltas_pos.saturating_add(d); } }
        // mints only in post
        if let Some(pb) = &meta.post_token_balances {
            for b in pb { let k = Key{acc:b.account_index as u16, mint:b.mint.clone()}; if !meta.pre_token_balances.as_ref().map_or(false, |v| v.iter().any(|x| x.account_index as u16 == k.acc && x.mint == k.mint)) {
                    let amt = b.ui_token_amount.amount.parse::<i128>().ok()?; if amt > 0 { deltas_pos = deltas_pos.saturating_add(amt); }
                }
            }
        }
        if deltas_pos == 0 { None } else { Some(deltas_pos as f64) }
    }

    pub async fn update_bot_capability_score(&self, bot_pubkey: &String) -> Result<(), Box<dyn std::error::Error>> {
        let profile = self.bot_profiles.get(bot_pubkey)
            .ok_or("Bot profile not found")?
            .clone();
        
        let profile_guard = profile.read().await;
        
        if profile_guard.transactions.len() < self.scoring_config.score_update_threshold {
            return Ok(());
        }

        let capability = self.calculate_capability_score(&profile_guard).await;
        self.capability_scores.insert(bot_pubkey.clone(), capability.score);
        
        Ok(())
    }

    async fn compute_bot_score(&self, profile: &BotProfile) -> Result<f64, Box<dyn std::error::Error>> {
        // 1. Base score on profit estimates (weighted by recency)
        let n = profile.transactions.len().min(self.scoring_config.max_transaction_history).max(1);
        let profit_score: f64 = profile.transactions.iter()
            .rev()
            .take(n)
            .map(|tx| tx.profit_estimate.unwrap_or(0.0))
            .sum::<f64>() / n as f64;

        // 2. Strategy consistency score (higher for more consistent strategies)
        let entropy = profile.compute_strategy_entropy();
        let consistency_score = 1.0 - entropy;

        // 3. Novelty penalty for unusual strategy transitions
        let novelty_penalty = if let Some(last) = profile.transactions.back() {
            if let Some(prev) = &profile.last_strategy {
                let key = (prev.clone(), last.strategy_type.clone());
                let count = profile.strategy_transitions.get(&key).copied().unwrap_or(0);
                if count < self.scoring_config.transition_novelty_threshold {
                    0.8 // 20% penalty for novel transitions
                } else {
                    1.0
                }
            } else {
                1.0
            }
        } else {
            1.0
        };

        // 4. Combine scores with weights from config
        let score = (profit_score * self.scoring_config.profit_weight) 
            * (consistency_score * self.scoring_config.consistency_weight)
            * novelty_penalty;

        Ok(score.clamp(0.0, 1.0)) // Ensure score is between 0 and 1
    }

    async fn calculate_capability_score(&self, profile: &BotProfile) -> RivalBotCapability {
        let strat_total: f64 = profile.cumulative_metrics.strategy_counts.values().sum::<u64>() as f64;

        let rate = |s: BotStrategy| {
            *profile.cumulative_metrics.strategy_counts.get(&s).unwrap_or(&0) as f64 / strat_total.max(1.0)
        };

        let current_time = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        let recent_txs: Vec<&TransactionMetrics> = profile.transactions.iter()
            .filter(|tx| current_time - tx.timestamp < Duration::from_secs(300).as_secs())
            .collect();

        let success_rate = if !recent_txs.is_empty() {
            recent_txs.iter().filter(|tx| tx.success).count() as f64 / recent_txs.len() as f64
        } else {
            0.0
        };

        let avg_priority_fee = if !recent_txs.is_empty() {
            recent_txs.iter().map(|tx| tx.priority_fee).sum::<u64>() / recent_txs.len() as u64
        } else {
            0
        };

        let avg_compute_units = if !recent_txs.is_empty() {
            recent_txs.iter().map(|tx| tx.compute_units).sum::<u64>() / recent_txs.len() as u64
        } else {
            0
        };

        let speed_score = self.calculate_speed_score(&profile.cumulative_metrics);
        let gas_efficiency_score = self.calculate_gas_efficiency_score(avg_priority_fee, avg_compute_units);
        let pattern_consistency_score = self.calculate_pattern_consistency(&profile.cumulative_metrics);

        let strategy_breakdown = self.analyze_strategies(&profile.cumulative_metrics.strategy_counts);
        
        let total_score = self.calculate_total_score(
            speed_score,
            success_rate,
            gas_efficiency_score,
            pattern_consistency_score,
            recent_txs.len() as f64,
        );

        let threat_level = self.assess_threat_level(total_score, success_rate, recent_txs.len());

        let net_profit = if !profile.cumulative_metrics.profits.is_empty() {
            profile.cumulative_metrics.profits.iter().sum::<f64>() / profile.cumulative_metrics.profits.len() as f64
        } else {
            0.0
        };

        let computed_score = self.compute_bot_score(profile).await.unwrap_or(0.0);
        
        RivalBotCapability {
            bot_pubkey: profile.pubkey.clone(),
            score: computed_score,
            arbitrage_rate: rate(BotStrategy::Arbitrage),
            liquidation_rate: rate(BotStrategy::Liquidation),
            sandwich_rate: rate(BotStrategy::SandwichAttack),
            avg_speed_ms: speed_score,
            success_rate,
            avg_compute_units,
            avg_priority_fee,
            pattern_consistency: pattern_consistency_score,
            avg_profit: net_profit,
            avg_rpc_latency_ms: self.calculate_avg_latency(&profile.cumulative_metrics.rpc_latency_samples),
            avg_confirmation_latency_ms: self.calculate_avg_latency(&profile.cumulative_metrics.confirmation_latency_samples),
            avg_block_inclusion_latency_ms: self.calculate_avg_latency(&profile.cumulative_metrics.block_inclusion_latency_samples),
            detected_strategies: strategy_breakdown.strategies,
            threat_level,
            anomalies: Vec::new(),
            rival_comparison_rank: None,
        }
    }

    fn calculate_speed_score(&self, metrics: &CumulativeMetrics) -> f64 {
        if metrics.slot_intervals.len() < 2 {
            return 0.0;
        }

        let mut diffs: Vec<u64> = metrics.slot_intervals
            .iter()
            .zip(metrics.slot_intervals.iter().skip(1))
            .map(|(a, b)| b.saturating_sub(*a))
            .collect();

        let avg_interval = diffs.iter().sum::<u64>() as f64 / diffs.len() as f64;
        let expected_interval = self.scoring_config.slot_time_ms as f64;
        (expected_interval / avg_interval.max(expected_interval)) * 100.0
    }

    fn calculate_gas_efficiency_score(&self, avg_priority_fee: u64, avg_compute_units: u64) -> f64 {
        let fee_efficiency = (50000.0 / (avg_priority_fee as f64 + 1.0)).min(1.0);
        let compute_efficiency = (200000.0 / (avg_compute_units as f64 + 1.0)).min(1.0);
        
        (fee_efficiency * 0.6 + compute_efficiency * 0.4) * 100.0
    }

    fn calculate_pattern_consistency(&self, cm: &CumulativeMetrics) -> f64 {
        if cm.strategy_counts.len() < 2 {
            return 0.0;
        }
        let total: u64 = cm.strategy_counts.values().sum();
        if total == 0 { return 0.0; }
        let dominant = cm.strategy_counts.values().max().copied().unwrap_or(0);
        dominant as f64 / total as f64 * 100.0
    }

    fn calculate_avg_latency(&self, latency_samples: &VecDeque<f64>) -> f64 {
        if latency_samples.is_empty() { return 0.0; }
        latency_samples.iter().sum::<f64>() / latency_samples.len() as f64
    }

    fn analyze_strategies(&self, strategy_counts: &HashMap<BotStrategy, u64>) -> StrategyBreakdown {
        let total: u64 = strategy_counts.values().sum();
        
        if total == 0 {
            return StrategyBreakdown::default();
        }

        let arbitrage_count = *strategy_counts.get(&BotStrategy::Arbitrage).unwrap_or(&0);
        let liquidation_count = *strategy_counts.get(&BotStrategy::Liquidation).unwrap_or(&0);
        let sandwich_count = *strategy_counts.get(&BotStrategy::SandwichAttack).unwrap_or(&0);

        StrategyBreakdown {
            strategies: strategy_counts.keys().cloned().collect(),
            arbitrage_share: arbitrage_count as f64 / total as f64,
            liquidation_share: liquidation_count as f64 / total as f64,
            sandwich_share: sandwich_count as f64 / total as f64,
        }
    }

    fn calculate_total_score(
        &self,
        speed_score: f64,
        success_rate: f64,
        gas_efficiency_score: f64,
        pattern_consistency_score: f64,
        tx_volume: f64,
    ) -> f64 {
        let volume_score = (tx_volume / self.scoring_config.high_frequency_threshold as f64).min(1.0) * 100.0;
        
        let weighted_score = speed_score * self.scoring_config.speed_weight
            + (success_rate * 100.0) * self.scoring_config.success_weight
            + gas_efficiency_score * self.scoring_config.gas_efficiency_weight
            + pattern_consistency_score * self.scoring_config.pattern_consistency_weight
            + volume_score * self.scoring_config.volume_weight;
        
        weighted_score.min(100.0)
    }

    fn assess_threat_level(&self, total_score: f64, success_rate: f64, tx_count: usize) -> ThreatLevel {
        if total_score >= 85.0 && success_rate >= self.scoring_config.critical_success_threshold && tx_count >= self.scoring_config.high_frequency_threshold as usize {
            ThreatLevel::Critical
        } else if total_score >= 70.0 && success_rate >= 0.7 {
            ThreatLevel::High
        } else if total_score >= 50.0 && success_rate >= 0.5 {
            ThreatLevel::Medium
        } else {
            ThreatLevel::Low
        }
    }

    pub async fn get_top_rivals(&self, limit: usize) -> Vec<f64> {
        let mut capabilities: Vec<f64> = self.capability_scores
            .iter()
            .map(|entry| entry.value().clone())
            .collect();
        
        capabilities.sort_by(|a, b| b.partial_cmp(a).unwrap_or(std::cmp::Ordering::Equal));
        capabilities.truncate(limit);
        capabilities
    }

    pub async fn get_rival_capability(&self, bot_pubkey: &String) -> Option<f64> {
        self.capability_scores.get(bot_pubkey).map(|entry| entry.clone())
    }

    pub async fn identify_threat_pattern(&self, bot_pubkey: &String) -> Option<ThreatPattern> {
        let capability = self.get_rival_capability(bot_pubkey).await?;
        
        let pattern = if capability > 0.5 {
            ThreatPattern::AggressiveSandwicher
        } else if capability > 0.7 {
            ThreatPattern::HighSpeedArbitrager
        } else if capability > 0.6 {
            ThreatPattern::LiquidationSpecialist
        } else if capability > self.scoring_config.critical_success_threshold {
            ThreatPattern::HighFrequencyTrader
        } else {
            ThreatPattern::GeneralTrader
        };
        
        Some(pattern)
    }

    pub async fn prune_inactive_bots(&self, inactive_threshold_secs: u64) -> Result<(), Box<dyn std::error::Error>> {
        let current_time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        
        let mut to_remove = Vec::new();

        // Collect keys to remove without holding long-lived locks
        for entry in self.bot_profiles.iter() {
            let profile_arc = entry.value().clone();
            // async read
            let last_seen = {
                let guard = profile_arc.read().await;
                guard.last_seen
            };
            if current_time.saturating_sub(last_seen) > inactive_threshold_secs {
                to_remove.push(entry.key().clone());
            }
        }

        // Remove outside of iteration
        for k in to_remove {
            self.bot_profiles.remove(&k);
            self.capability_scores.remove(&k);
        }

        Ok(())
    }

    pub fn export_capabilities(&self) -> HashMap<String, f64> {
        self.capability_scores
            .iter()
            .map(|e| (e.key().clone(), *e.value()))
            .collect()
    }

    pub async fn calculate_competitive_edge(&self, our_metrics: &OurBotMetrics) -> CompetitiveAnalysis {
        let rivals: Vec<f64> = self.get_top_rivals(20).await;
        
        if rivals.is_empty() {
            return CompetitiveAnalysis::default();
        }

        let avg_rival_success = rivals.iter().sum::<f64>() / rivals.len() as f64;
        let avg_rival_speed = rivals.iter().sum::<f64>() / rivals.len() as f64;
        let avg_rival_fee = rivals.iter().sum::<f64>() / rivals.len() as f64;
        
        let success_edge = our_metrics.success_rate - avg_rival_success;
        let speed_edge = our_metrics.speed_score - avg_rival_speed;
        let fee_advantage = avg_rival_fee - our_metrics.avg_priority_fee as f64;
        
        let top_threat = rivals.first().cloned();
        let critical_rivals = rivals.iter()
            .filter(|r| *r > 0.85)
            .count();
        
        CompetitiveAnalysis {
            success_rate_edge: success_edge,
            speed_score_edge: speed_edge,
            fee_efficiency_edge: fee_advantage,
            top_rival: top_threat,
            critical_threat_count: critical_rivals,
            recommended_adjustments: self.generate_adjustments(success_edge, speed_edge, fee_advantage),
        }
    }

    fn generate_adjustments(&self, success_edge: f64, speed_edge: f64, fee_edge: f64) -> Vec<String> {
        let mut adjustments = Vec::new();
        
        if success_edge < -0.1 {
            adjustments.push("Increase transaction validation and simulation accuracy".to_string());
        }
        
        if speed_edge < -10.0 {
            adjustments.push("Optimize RPC connections and reduce processing latency".to_string());
        }
        
        if fee_edge < 0.0 {
            adjustments.push("Implement dynamic priority fee adjustment based on competition".to_string());
        }
        
        if adjustments.is_empty() {
            adjustments.push("Maintain current strategy - performing above average".to_string());
        }
        
        adjustments
    }

    async fn fetch_jito_bundle_stats(
        &self,
        _sig: &str,
    ) -> Result<JitoBundleStats, Box<dyn std::error::Error>> {
        // TODO: wire actual gRPC
        Ok(JitoBundleStats { bundle_id: None, tip_bid: None })
    }
    
    async fn fetch_jito_stats_json_rpc(
        &self,
        signature: &str,
    ) -> Result<JitoBundleStats, Box<dyn std::error::Error>> {
        let client = reqwest::Client::new();
        let response = client
            .post(&self.config.jito_block_engine_url)
            .json(&serde_json::json!({{
                "jsonrpc": "2.0",
                "id": 1,
                "method": "getBundleStatus",
                "params": [{{"signature": signature, "commitment": "confirmed"}}]
            }}))
            .send()
            .await?;
            
        let result: serde_json::Value = response.json().await?;
        
        Ok(JitoBundleStats {
            bundle_id: result["result"]["bundleId"].as_str().map(|s| s.to_string()),
            tip_bid: result["result"]["tipBid"].as_u64(),
        })
    }
    
    async fn fetch_jito_stats_grpc(
        &self,
        signature: &str,
    ) -> Result<JitoBundleStats, Box<dyn std::error::Error>> {
        let request = tonic::Request::new(GetBundleStatusRequest {
            signature: signature.to_string(),
        });
        
        let response = self.jito_grpc_client
            .clone()
            .get_bundle_status(request)
            .await?;
            
        Ok(JitoBundleStats {
            bundle_id: if response.get_ref().bundle_id.is_empty() {
                None
            } else {
                Some(response.get_ref().bundle_id.clone())
            },
            tip_bid: response.get_ref().tip_bid,
        })
    }

    async fn fetch_oracle_data(
        &self,
        oracle_pubkey: &Pubkey,
    ) -> Result<OracleData, Box<dyn std::error::Error>> {
        // Try Pyth first
        match self.fetch_pyth_data(oracle_pubkey).await {
            Ok((age, confidence)) => Ok(OracleData(age, confidence)),
            Err(_) => {
                // Fallback to Switchboard
                match self.fetch_switchboard_data(oracle_pubkey).await {
                    Ok((age, confidence)) => Ok(OracleData(age, confidence)),
                    Err(_) => Ok(OracleData::default()),
                }
            }
        }
    }
    
    async fn fetch_pyth_data(
        &self,
        oracle_pubkey: &Pubkey,
    ) -> Result<(u64, f64), Box<dyn std::error::Error>> {
        let account_data = self.rpc_client.get_account_data(oracle_pubkey).await?;
        let price_account = parse_pyth_price(account_data.as_ref())?;
        let age_secs = (self.get_current_timestamp()? - price_account.1) as u64;
        let confidence = price_account.2;
        Ok((age_secs, confidence))
    }
    
    async fn fetch_switchboard_data(
        &self,
        oracle_pubkey: &Pubkey,
    ) -> Result<(u64, f64), Box<dyn std::error::Error>> {
        let account_data = self.rpc_client.get_account_data(oracle_pubkey).await?;
        
        // Try Aggregator first
        match AggregatorAccountData::new(&account_data) {
            Ok(agg) => {
                let age_secs = (self.get_current_timestamp()? - agg.latest_confirmed_round.round_open_timestamp) as u64;
                let confidence = agg.latest_confirmed_round.std_deviation;
                Ok((age_secs, confidence))
            }
            Err(_) => {
                // Fallback to VRF
                let vrf = VrfAccountData::new(&account_data)?;
                let age_secs = (self.get_current_timestamp()? - vrf.timestamp) as u64;
                Ok((age_secs, 1.0)) // Default confidence for VRF
            }
        }
    }
    
    fn get_current_timestamp(&self) -> Result<i64, Box<dyn std::error::Error>> {
        Ok(std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs() as i64)
    }
}

pub struct ShardedScorer {
    senders: Vec<mpsc::UnboundedSender<(Signature, Pubkey)>>,
}

impl ShardedScorer {
    pub fn new(scorer: Arc<RivalBotCapabilityScorer>, shards: usize) -> Self {
        let mut senders = Vec::new();
        
        for _ in 0..shards {
            let (tx, rx) = mpsc::unbounded_channel();
            let actor = ScoringActor {
                receiver: rx,
                scorer: scorer.clone()
            };
            tokio::spawn(actor.run());
            senders.push(tx);
        }
        
        Self { senders }
    }
    
    pub fn dispatch(&self, sig: Signature, bot: Pubkey) {
        // Simple mod-based sharding
        let idx = (bot.to_bytes()[0] as usize) % self.senders.len();
        let _ = self.senders[idx].send((sig, bot));
    }
}

struct ScoringActor {
    receiver: mpsc::UnboundedReceiver<(Signature, Pubkey)>,
    scorer: Arc<RivalBotCapabilityScorer>,
}

impl ScoringActor {
    async fn run(mut self) {
        while let Some((sig, bot)) = self.receiver.recv().await {
            let _ = self.scorer.track_transaction(&sig, &bot).await;
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct JitoBundleStats {
    bundle_id: Option<String>,
    tip_bid: Option<u64>,
}

impl Default for JitoBundleStats {
    fn default() -> Self {
        Self {
            bundle_id: None,
            tip_bid: None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StrategyBreakdown {
    pub strategies: Vec<BotStrategy>,
    pub arbitrage_share: f64,
    pub liquidation_share: f64,
    pub sandwich_share: f64,
}

impl Default for StrategyBreakdown {
    fn default() -> Self {
        Self {
            strategies: Vec::new(),
            arbitrage_share: 0.0,
            liquidation_share: 0.0,
            sandwich_share: 0.0,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum ThreatPattern {
    AggressiveSandwicher,
    HighSpeedArbitrager,
    LiquidationSpecialist,
    HighFrequencyTrader,
    GeneralTrader,
}

#[derive(Debug, Clone)]
pub struct OurBotMetrics {
    pub success_rate: f64,
    pub speed_score: f64,
    pub avg_priority_fee: u64,
}

#[derive(Debug, Clone, Default)]
pub struct CompetitiveAnalysis {
    pub success_rate_edge: f64,
    pub speed_score_edge: f64,
    pub fee_efficiency_edge: f64,
    pub top_rival: Option<f64>,
    pub critical_threat_count: usize,
    pub recommended_adjustments: Vec<String>,
}

lazy_static! {
    static ref METRICS: Metrics = {
        let registry = Registry::new();
        
        let metrics = Metrics {
            registry,
            tx_successes: IntCounter::new(
                "bot_scorer_transaction_successes", 
                "Number of successful transactions tracked"
            ).unwrap(),
            tx_failures: IntCounter::new(
                "bot_scorer_transaction_failures", 
                "Number of failed transactions tracked"
            ).unwrap(),
            avg_cu: IntGauge::new(
                "bot_scorer_avg_compute_units", 
                "Average compute units used by bots"
            ).unwrap(),
            // New latency metrics
            rpc_latency: Histogram::with_opts(
                HistogramOpts::new(
                    "bot_scorer_rpc_latency_ms",
                    "RPC latency in milliseconds"
                ).buckets(vec![1.0, 5.0, 10.0, 25.0, 50.0, 100.0, 250.0, 500.0, 1000.0])
            ).unwrap(),
            confirmation_latency: Histogram::with_opts(
                HistogramOpts::new(
                    "bot_scorer_confirmation_latency_ms",
                    "Confirmation latency in milliseconds"
                ).buckets(vec![10.0, 50.0, 100.0, 250.0, 500.0, 1000.0, 2500.0, 5000.0])
            ).unwrap(),
            block_latency: Histogram::with_opts(
                HistogramOpts::new(
                    "bot_scorer_block_inclusion_latency_ms",
                    "Block inclusion latency in milliseconds"
                ).buckets(vec![10.0, 50.0, 100.0, 250.0, 500.0, 1000.0, 2500.0, 5000.0])
            ).unwrap(),
            // New Jito metrics
            jito_usage: IntCounter::new(
                "bot_scorer_jito_bundle_usage", 
                "Number of transactions using Jito bundles"
            ).unwrap(),
            avg_tip_bid: IntGauge::new(
                "bot_scorer_avg_tip_bid", 
                "Average tip bid amount for Jito bundles"
            ).unwrap(),
        };
        
        metrics.registry.register(Box::new(metrics.tx_successes.clone())).unwrap();
        metrics.registry.register(Box::new(metrics.tx_failures.clone())).unwrap();
        metrics.registry.register(Box::new(metrics.avg_cu.clone())).unwrap();
        metrics.registry.register(Box::new(metrics.rpc_latency.clone())).unwrap();
        metrics.registry.register(Box::new(metrics.confirmation_latency.clone())).unwrap();
        metrics.registry.register(Box::new(metrics.block_latency.clone())).unwrap();
        metrics.registry.register(Box::new(metrics.jito_usage.clone())).unwrap();
        metrics.registry.register(Box::new(metrics.avg_tip_bid.clone())).unwrap();
        
        metrics
    };
}

use prometheus::{IntCounter, Histogram, Registry};
use lazy_static::lazy_static;

lazy_static! {
    static ref REGISTRY: Registry = Registry::new();
    static ref TX_COUNTER: Result<IntCounter, PrometheusError> = IntCounter::new("txs_processed", "Total transactions processed");
    static ref LATENCY_HIST: Result<Histogram, PrometheusError> = Histogram::with_opts(
        prometheus::HistogramOpts::new(
            "rpc_latency_ms", 
            "RPC latency in milliseconds"
        )
    );
}

pub fn init_metrics() -> Result<(), PrometheusError> {
    if let Ok(counter) = &*TX_COUNTER {
        REGISTRY.register(Box::new(counter.clone()))?;
    }
    if let Ok(hist) = &*LATENCY_HIST {
        REGISTRY.register(Box::new(hist.clone()))?;
    }
    Ok(())
}

fn create_metrics() -> Result<Metrics, Box<dyn std::error::Error>> {
    let registry = Registry::new();
    
    let tx_successes = IntCounter::new(
        "bot_scorer_transaction_successes", 
        "Number of successful transactions tracked"
    )?;
    
    let tx_failures = IntCounter::new(
        "bot_scorer_transaction_failures", 
        "Number of failed transactions tracked"
    )?;
    
    let avg_cu = IntGauge::new(
        "bot_scorer_avg_compute_units", 
        "Average compute units used by bots"
    )?;
    
    let rpc_latency = Histogram::with_opts(
        HistogramOpts::new(
            "bot_scorer_rpc_latency_ms",
            "RPC latency in milliseconds"
        ).buckets(vec![1.0, 5.0, 10.0, 25.0, 50.0, 100.0, 250.0, 500.0, 1000.0])
    )?;
    
    let confirmation_latency = Histogram::with_opts(
        HistogramOpts::new(
            "bot_scorer_confirmation_latency_ms",
            "Confirmation latency in milliseconds"
        ).buckets(vec![10.0, 50.0, 100.0, 250.0, 500.0, 1000.0, 2500.0, 5000.0])
    )?;
    
    let block_latency = Histogram::with_opts(
        HistogramOpts::new(
            "bot_scorer_block_inclusion_latency_ms",
            "Block inclusion latency in milliseconds"
        ).buckets(vec![10.0, 50.0, 100.0, 250.0, 500.0, 1000.0, 2500.0, 5000.0])
    )?;
    
    let jito_usage = IntCounter::new(
        "bot_scorer_jito_bundle_usage", 
        "Number of transactions using Jito bundles"
    )?;
    
    let avg_tip_bid = IntGauge::new(
        "bot_scorer_avg_tip_bid", 
        "Average tip bid amount for Jito bundles"
    )?;
    
    registry.register(Box::new(tx_successes.clone()))?;
    registry.register(Box::new(tx_failures.clone()))?;
    registry.register(Box::new(avg_cu.clone()))?;
    registry.register(Box::new(rpc_latency.clone()))?;
    registry.register(Box::new(confirmation_latency.clone()))?;
    registry.register(Box::new(block_latency.clone()))?;
    registry.register(Box::new(jito_usage.clone()))?;
    registry.register(Box::new(avg_tip_bid.clone()))?;
    
    Ok(Metrics {
        registry,
        tx_successes,
        tx_failures,
        avg_cu,
        rpc_latency,
        confirmation_latency,
        block_latency,
        jito_usage,
        avg_tip_bid,
    })
}

//////////////////////////////
// THREAT MODELING (MITRE/STRIDE HYBRID)
//////////////////////////////

pub fn score_threat(
    metrics: &BotPerformanceMetrics,
    strategies: &[BotStrategy],
    uses_jito: bool,
    avg_tip_bid: Option<u64>
) -> (ThreatLevel, Vec<String>) {
    let mut points = 0;
    let mut anomalies = Vec::new();

    // Existing scoring logic...

    // Jito-specific scoring
    if uses_jito {
        points += 15; // Base points for using Jito
        anomalies.push("Uses Jito bundles".to_string());
        
        if let Some(tip) = avg_tip_bid {
            if tip > 1_000_000 { // 1 SOL
                points += 10;
                anomalies.push(format!("High Jito tip bid: {} lamports", tip));
            }
        }
    }

    // Determine threat level based on total points
    let threat_level = if points >= 60 {
        ThreatLevel::Critical
    } else if points >= 40 {
        ThreatLevel::High
    } else if points >= 20 {
        ThreatLevel::Moderate
    } else {
        ThreatLevel::Low
    };

    (threat_level, anomalies)
}

#[cfg(test)]
mod tests {
    use super::*;
    use yellowstone_grpc_client::SubscribeUpdate;
    
    #[test]
    fn test_classify_strategy() {
        // Test arbitrage classification
        let arbitrage_ins = b"swap exact in for out";
        assert!(matches!(
            classify_strategy(arbitrage_ins)[0],
            BotStrategy::Arbitrage
        ));
        
        // Test liquidation classification
        let liquidation_ins = b"liquidate position";
        assert!(matches!(
            classify_strategy(liquidation_ins)[0],
            BotStrategy::Liquidation
        ));
        
        // Test sandwich classification
        let sandwich_ins = b"place_order cancel_order";
        assert!(matches!(
            classify_strategy(sandwich_ins)[0],
            BotStrategy::SandwichAttack
        ));
    }
    
    #[tokio::test]
    async fn test_compute_metrics() {
        let mut tx = SubscribeUpdate::default();
        tx.executed = Some(true);
        tx.compute_units = Some(100_000);
        tx.priority_fee = Some(50_000);
        
        let mut block_times = HashMap::new();
        block_times.insert(1, 1000);
        
        let metrics = compute_metrics(&[&tx], &block_times).await;
        
        assert_eq!(metrics.avg_cu, 100_000);
        assert_eq!(metrics.avg_priority_fee, 50_000);
        assert_eq!(metrics.success_rate, 1.0);
    }
    
    #[test]
    fn test_score_threat() {
        let metrics = BotPerformanceMetrics {
            speed_ms: 150,
            success_rate: 0.96,
            avg_cu: 950_000,
            avg_priority_fee: 1_200_000,
            pattern_consistency: 0.4,
            net_profit: 0.5,
        };
        
        let strategies = vec![BotStrategy::SandwichAttack];
        
        let (threat_level, notes) = score_threat(&metrics, &strategies);
        
        assert!(matches!(threat_level, ThreatLevel::High));
        assert!(notes.contains(&"Sandwich attack capability detected".to_string()));
    }
    
    #[tokio::test]
    async fn test_active_bot_scores() {
        let scores = ActiveBotScores::new().expect("Failed to create scores");
        
        let test_score = BotScore {
            bot_pubkey: "test_pubkey".to_string(),
            score: 0.85,
            last_updated: 0,
            strategy_breakdown: StrategyBreakdown::default(),
            rival_comparison_rank: Some(0.75),
        };
        
        scores.update("test_pubkey".to_string(), test_score.clone()).await
            .expect("Failed to update score");
        
        let retrieved = scores.get("test_pubkey").await;
        assert!(retrieved.is_some());
        assert_eq!(retrieved.expect("Should have value").bot_pubkey, "test_pubkey");
    }
}

pub async fn run_live_scorer(yellowstone_endpoint: &str) -> Result<(), Box<dyn std::error::Error>> {
    info!("Launching rival_bot_capability_scorer...");
    register_metrics();

    // Setup Yellowstone gRPC connection
    let mut client = GeyserGrpcClient::connect(yellowstone_endpoint, None, None).await?;
    let filter = SubscribeRequest::default(); // Apply fine-grained filter, e.g., known bot wallets/programs
    let mut stream = client.subscribe_with_request(filter).await?;

    let bot_scores = Arc::new(ActiveBotScores::default());
    let all_scores_cache = Arc::new(Mutex::new(Vec::new()));
    let block_times = Arc::new(Mutex::new(HashMap::new())); // slot -> time

    while let Some(update) = stream.message().await? {
        // In prod, spawn and parallelize
        let tx = &update;

        // Extract bot pubkey from transaction
        let bot_pubkey = match &tx.transaction {
            Some(tx_data) => {
                if let Some(signatures) = &tx_data.signatures {
                    if !signatures.is_empty() {
                        bs58::encode(&signatures[0]).into_string()
                    } else {
                        warn!("Transaction has no signatures");
                        continue;
                    }
                } else {
                    warn!("Transaction data missing signatures");
                    continue;
                }
            }
            None => {
                warn!("Received update without transaction data");
                continue;
            }
        };
        let bot_pubkey = bot_pubkey.clone();

        // Rest of the code remains the same
    }
}

// Placeholder types - replace with real implementations
#[derive(Clone)]
pub struct Config {
    pub jito_block_engine_url: String,
    pub rpc_url: String,
    pub metrics_enabled: bool,
}

#[derive(Clone)]
pub struct Metrics {
    pub registry: Registry,
    pub tx_successes: IntCounter,
    pub tx_failures: IntCounter,
    pub avg_cu: IntGauge,
    pub rpc_latency: Histogram,
    pub confirmation_latency: Histogram,
    pub block_latency: Histogram,
    pub jito_usage: IntCounter,
    pub avg_tip_bid: IntGauge,
}

#[derive(Debug)]
pub struct BotPerformanceMetrics {
    pub speed_ms: f64,
    pub success_rate: f64,
    pub avg_cu: u64,
    pub avg_priority_fee: u64,
    pub pattern_consistency: f64,
    pub net_profit: f64,
}

#[derive(Debug, Default, Clone)]
pub struct OracleData {
    pub age_secs: u64,
    pub confidence: f64,
}
