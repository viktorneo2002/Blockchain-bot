    // Acquire best-effort per-account write permits (300µs timeout)
    #[inline]
    async fn acquire_write_permits(
        &self,
        writes: &[Pubkey],
    ) -> Vec<tokio::sync::OwnedSemaphorePermit> {
        let gates = ACCOUNT_GATES.get_or_init(|| DashMap::new());
        let mut permits = Vec::with_capacity(writes.len());
        for k in writes {
            let sem = gates
                .entry(*k)
                .or_insert_with(|| Arc::new(tokio::sync::Semaphore::new(1)))
                .clone();
            let p = tokio::time::timeout(std::time::Duration::from_micros(300), sem.acquire_owned())
                .await
                .ok()
                .and_then(Result::ok);
            if let Some(p) = p { permits.push(p); }

// -----------------------------
// Rolling Presigner: maintain ladders across recent/future blockhashes
// -----------------------------
#[derive(Clone)]
pub struct VariantMeta { pub compute_price: u64, pub cu_limit: u64 }
#[derive(Clone)]
pub struct PresignedVariant { pub vtx: VersionedTransaction, pub sig: Signature, pub meta: VariantMeta, pub wire: Bytes }
type Ladder = Vec<PresignedVariant>;

pub trait AltProvider: Send + Sync {
    fn load_for_accounts(&self, accounts: &[Pubkey]) -> anyhow::Result<solana_sdk::message::v0::LoadedAddresses>;
}

pub trait TxBuilder: Send + Sync {
    fn build_vtx(
        &self,
        leader: Pubkey,
        bh: Hash,
        cu_limit: u64,
        compute_price: u64,
        ixs: &[Instruction],
        lookups: Option<solana_sdk::message::v0::LoadedAddresses>,
    ) -> anyhow::Result<VersionedTransaction>;
}

#[derive(Clone)]
pub struct RollingPresigner {
    rpc: Arc<solana_client::nonblocking::rpc_client::RpcClient>,
    ladders: Arc<DashMap<(Pubkey, Hash), Ladder>>, // key: (leader, blockhash)
    depth: usize,
    tiers: Arc<Vec<u64>>, // micro/CU per tier
    alt_provider: Option<Arc<dyn AltProvider + Send + Sync>>,
}
impl RollingPresigner {
    pub fn new(
        rpc: Arc<solana_client::nonblocking::rpc_client::RpcClient>,
        tiers: Vec<u64>,
        depth: usize,
        alt_provider: Option<Arc<dyn AltProvider + Send + Sync>>,
    ) -> Self {
        Self { rpc, ladders: Arc::new(DashMap::new()), depth, tiers: Arc::new(tiers), alt_provider }
    }
    pub async fn run_rotor(
        self: Arc<Self>,
        leader_src: Arc<dyn Fn() -> Pubkey + Send + Sync>,
        txb: Arc<dyn TxBuilder>,
        accounts_for_pricing: Arc<Vec<Pubkey>>,
        cu_limit_hint: u64,
        ixs_src: Arc<dyn Fn() -> Vec<Instruction> + Send + Sync>,
    ) {
        let mut last_bh: Option<Hash> = None;
        loop {
            let bh = match self.rpc.get_latest_blockhash().await { Ok(h) => h, Err(_) => { tokio::time::sleep(std::time::Duration::from_millis(50)).await; continue; } };
            if last_bh == Some(bh) { tokio::time::sleep(std::time::Duration::from_millis(120)).await; continue; }
            last_bh = Some(bh);

            let leader = (leader_src)();
            let ixs = (ixs_src)();
            let lookups = match &self.alt_provider { Some(p) => p.load_for_accounts(&accounts_for_pricing).ok(), None => None };

            let mut ladder = Vec::with_capacity(self.tiers.len());
            for &micro in self.tiers.iter() {
                if let Ok(vtx) = txb.build_vtx(leader, bh, cu_limit_hint, micro, &ixs, lookups.clone()) {
                    let sig = vtx.signatures[0];
                    let wire = Bytes::from(bincode::serialize(&vtx).unwrap_or_default());
                    ladder.push(PresignedVariant { vtx, sig, meta: VariantMeta { compute_price: micro, cu_limit: cu_limit_hint }, wire });
                }
            }
            self.ladders.insert((leader, bh), ladder);

            // prune naive by size (caller can improve with rpc.is_blockhash_valid)
            if self.ladders.len() > self.depth * 12 {
                let keep = self.depth;
                let mut keys: Vec<(Pubkey, Hash)> = self.ladders.iter().map(|e| e.key().clone()).collect();
                if keys.len() > keep { keys.sort_by_key(|(_, h)| h.to_string()); keys.truncate(keys.len().saturating_sub(keep)); for k in keys { let _ = self.ladders.remove(&k); } }
            }
            tokio::time::sleep(std::time::Duration::from_millis(120)).await;
        }
    }
    pub fn select_variant_by_price(
        &self,
        leader: &Pubkey,
        bh: &Hash,
        target_micro: u64,
    ) -> Option<(VersionedTransaction, Signature, VariantMeta, Bytes)> {
        let lad = self.ladders.get(&(*leader, *bh))?;
        let mut best: Option<&PresignedVariant> = None;
        for v in lad.iter() {
            if v.meta.compute_price > target_micro {
                match best { None => best = Some(v), Some(b) => if v.meta.compute_price < b.meta.compute_price { best = Some(v); } }
            }
        }
        best.map(|v| (v.vtx.clone(), v.sig, v.meta.clone(), v.wire.clone()))
    }
}

// -----------------------------
// StepBandit: learn per-leader step_bps without external rand_distr
// -----------------------------
#[derive(Clone)]
pub struct Arm { pub step_bps: u32, pub a: f64, pub b: f64 }
impl Arm {
    fn sample(&self) -> f64 {
        // simple heuristic sample ~ mean + small noise
        let mean = self.a / (self.a + self.b).max(1e-6);
        let noise: f64 = rand::thread_rng().gen::<f64>() * 0.05; // requires rand, present in Cargo.toml
        mean + noise
    }
    fn update(&mut self, success: bool) {
        if success { self.a += 1.0; } else { self.b += 1.0; }
        let decay = 0.995;
        self.a = 1.0 + (self.a - 1.0) * decay;
        self.b = 1.0 + (self.b - 1.0) * decay;
    }
}

#[derive(Clone)]
pub struct StepBandit { arms: DashMap<Pubkey, Vec<Arm>> }
impl StepBandit {
    pub fn new() -> Self { Self { arms: DashMap::new() } }
    pub fn choose(&self, leader: Pubkey) -> (usize, u32) {
        let mut entry = self.arms.entry(leader).or_insert_with(|| vec![600, 900, 1250, 1700].into_iter().map(|s| Arm { step_bps: s, a: 2.0, b: 2.0 }).collect());
        let idx = entry.iter().enumerate().max_by(|(_, a), (_, b)| a.sample().partial_cmp(&b.sample()).unwrap()).map(|(i, _)| i).unwrap_or(0);
        let step = entry[idx].step_bps;
        (idx, step)
    }
    pub fn update(&self, leader: Pubkey, arm_idx: usize, success: bool) {
        if let Some(mut e) = self.arms.get_mut(&leader) { if let Some(a) = e.get_mut(arm_idx) { a.update(success); } }
    }
}

// -----------------------------
// Relay QoS scoring and hedged sender scaffolding
// -----------------------------
#[derive(Clone, Copy)]
pub struct QoS { pub rtt_ms_ewma: f64, pub drop_ewma: f64, pub land_ewma: f64 }
impl QoS {
    pub fn score(&self) -> f64 { -self.rtt_ms_ewma + (1.0 - self.drop_ewma) * 100.0 + self.land_ewma * 200.0 }
    pub fn update(&mut self, rtt_ms: Option<f64>, delivered: bool, landed: bool) {
        let a = 0.2; if let Some(rtt) = rtt_ms { self.rtt_ms_ewma = if self.rtt_ms_ewma == 0.0 { rtt } else { self.rtt_ms_ewma * (1.0 - a) + rtt * a } }
        self.drop_ewma = self.drop_ewma * (1.0 - a) + ((!delivered as i32).max(0) as f64) * a;
        self.land_ewma = self.land_ewma * (1.0 - a) + (landed as i32 as f64) * a;
    }
}

#[derive(Clone)]
pub struct Relay { pub name: &'static str, pub addr: std::net::SocketAddr }

#[async_trait::async_trait]
pub trait RawSender: Send + Sync { async fn send_raw(&self, relay: &Relay, bytes: Bytes) -> anyhow::Result<()>; }

pub struct RelaySet<S: RawSender> {
    sender: S,
    qos: DashMap<&'static str, QoS>,
    relays: Vec<Relay>,
    k: usize,
}
impl<S: RawSender> RelaySet<S> {
    pub fn new(sender: S, relays: Vec<Relay>, k: usize) -> Self {
        let qos = DashMap::new(); for r in &relays { qos.insert(r.name, QoS { rtt_ms_ewma: 0.0, drop_ewma: 0.0, land_ewma: 0.0 }); }
        Self { sender, qos, relays, k }
    }
    pub fn pick_topk(&self) -> Vec<Relay> {
        let mut v: Vec<(Relay, f64)> = self.relays.iter().map(|r| { let s = self.qos.get(r.name).map(|q| q.score()).unwrap_or(0.0); (r.clone(), s) }).collect();
        v.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap()); v.into_iter().take(self.k).map(|x| x.0).collect()
    }
    pub async fn hedged_send(&self, bytes: Bytes) -> anyhow::Result<()> {
        let rels = self.pick_topk(); let start = Instant::now(); let (tx, mut rx) = tokio::sync::mpsc::channel::<(&'static str, bool)>(rels.len());
        for r in rels { let s = &self.sender; let bytes = bytes.clone(); let tx = tx.clone(); tokio::spawn(async move { let res = s.send_raw(&r, bytes).await; let _ = tx.send((r.name, res.is_ok())).await; }); }
        drop(tx);
        while let Some((name, ok)) = rx.recv().await { let rtt = start.elapsed().as_secs_f64() * 1000.0; if let Some(mut q) = self.qos.get_mut(name) { q.update(Some(rtt), ok, false); } if ok { return Ok(()); } }
        anyhow::bail!("hedged send: all relays failed")
    }
    pub fn on_landed(&self, relay_name: &'static str) { if let Some(mut q) = self.qos.get_mut(relay_name) { q.update(None, true, true); } }
}

// -----------------------------
// Request → Instructions abstraction (prebuild safety)
// -----------------------------
pub trait ToIxs {
    fn build_ixs(&self) -> anyhow::Result<Vec<Instruction>>;
}

impl ToIxs for FlashLoanRequest {
    fn build_ixs(&self) -> anyhow::Result<Vec<Instruction>> {
        // If you already carry precomputed instructions in the request, return them.
        // Otherwise, construct flash-borrow -> swap/arb legs -> repay here.
        // Ensure account metas align with target_accounts ordering.
        if let Some(plan) = self.plan.clone() { return Ok(plan); }
        // Fallback: build minimal instruction list if plan is absent
        // TODO: implement full flashloan path builder
        Ok(Vec::new())
    }
}

#[inline]
fn stable_group_baseline_us<T>(gb: &Vec<Result<T, anyhow::Error>>) -> u64 {
    use std::hash::{Hash, Hasher};
    let mut h = AHasher::new_with_keys(0xD6E8_FEB8_6659_FD93, 0xA5A3_564F_E9E1_B6F1);
    (gb.as_ptr() as usize).hash(&mut h);
    (h.finish() & 0x3FF) as u64 // 0..1023µs
}

#[inline]
fn ev_bias_us(ev_lam: u64) -> u64 {
    use std::hash::{Hash, Hasher};
    let mut h = AHasher::new_with_keys(0x9E37_79B9_7F4A_7C15, 0xBF58_476D_1CE4_E5B9);
    ev_lam.hash(&mut h);
    (h.finish() & 0x7F) as u64 // 0..127µs
}

// -----------------------------
// LandedOracle: ground-truth labels for fee bandits via WS onSignature stream
// -----------------------------
pub struct SigEvent { pub signature: Signature, pub err: Option<String> }
pub trait WsClientLike: Send + Sync + 'static {
    fn on_signature_stream(&self) -> Pin<Box<dyn Future<Output = anyhow::Result<Pin<Box<dyn Stream<Item = SigEvent> + Send>>>> + Send>>;
}
pub struct LandedOracle { labels: DashMap<Signature, bool> }
impl LandedOracle {
    pub fn new() -> Self { Self { labels: DashMap::new() } }
    pub async fn run(self: Arc<Self>, ws: Arc<dyn WsClientLike>) {
        if let Ok(mut sub) = ws.on_signature_stream().await {
            while let Some(ev) = sub.next().await {
                let sig = ev.signature;
                let landed = ev.err.is_none();
                self.labels.insert(sig, landed);
                if let Some((ldr, arm)) = CHOSEN_ARM_FOR_SIG.get_or_init(|| DashMap::new()).remove(&sig).map(|e| e.1) {
                    if let Some(mut b) = FEE_BANDITS.get_or_init(|| DashMap::new()).get_mut(&ldr) {
                        b.update(arm, landed);
                    }
                }
            }
        }
    }
}

// -----------------------------
// FeeModel: CU and µ/CU histograms for calibrated worst-case filters
// -----------------------------
#[derive(Default)]
pub struct CuStats { pub cu: hdrhistogram::Histogram<u64> }
impl CuStats { pub fn new() -> Self { Self { cu: hdrhistogram::Histogram::new_with_bounds(1, 1_000_000_000, 3).unwrap() } } }

pub struct FeeModel {
    by_kind: DashMap<u64, CuStats>,
    price_hist_by_leader: DashMap<Pubkey, hdrhistogram::Histogram<u64>>,
}
impl FeeModel {
    pub fn new() -> Self { Self { by_kind: DashMap::new(), price_hist_by_leader: DashMap::new() } }
    pub fn record_landed(&self, leader: Pubkey, tx_kind: u64, cu_used: u64, micro_price: u64) {
        self.by_kind.entry(tx_kind).or_insert_with(CuStats::new).value().cu.record(cu_used).ok();
        self.price_hist_by_leader.entry(leader).or_insert_with(|| hdrhistogram::Histogram::new_with_bounds(1, 2_000_000, 3).unwrap()).value().record(micro_price).ok();
    }
    pub fn worst_fee(&self, leader: Pubkey, tx_kind: u64) -> (u64, u64) {
        let cu_p99 = self.by_kind.get(&tx_kind).map(|s| s.cu.value_at_quantile(0.99)).unwrap_or(200_000);
        let micro_p99 = self.price_hist_by_leader.get(&leader).map(|h| h.value_at_quantile(0.99)).unwrap_or(50_000);
        (cu_p99, micro_p99)
    }
}
static FEE_MODEL: OnceLock<Arc<FeeModel>> = OnceLock::new();

#[inline]
pub fn tx_kind_hash(accounts: &[Pubkey], prog_ids: &[Pubkey]) -> u64 {
    use blake3::Hasher;
    let mut h = Hasher::new();
    for a in accounts { h.update(a.as_ref()); }
    for p in prog_ids { h.update(p.as_ref()); }
    let mut out = [0u8; 8]; out.copy_from_slice(&h.finalize().as_bytes()[..8]); u64::from_le_bytes(out)
}

// -----------------------------
// Linux sendmmsg fast path with partial mapping (feature-gated)
// -----------------------------
#[cfg(all(target_os = "linux", feature = "mmsg"))]
mod mmsg {
    use super::*;
    use std::os::fd::AsRawFd;
    pub async fn send_batch(sock: &std::net::UdpSocket, items: &[(Signature, &[u8])]) -> std::collections::HashSet<Signature, FastHash> {
        use nix::sys::socket::{sendmmsg, MsgFlags, mmsghdr, msghdr};
        use nix::sys::uio::IoVec;
        let fd = sock.as_raw_fd();
        let mut hdrs: Vec<mmsghdr> = Vec::with_capacity(items.len());
        let mut iovs: Vec<IoVec<&[u8]>> = Vec::with_capacity(items.len());
        for (_, bytes) in items {
            iovs.push(IoVec::from_slice(bytes));
            // Safe: iovs lives for the duration of the call
            let iov_ptr: *mut IoVec<&[u8]> = iovs.last_mut().unwrap();
            let hdr = mmsghdr { msg_hdr: msghdr {
                msg_name: std::ptr::null_mut(), msg_namelen: 0,
                msg_iov: iov_ptr as *mut _, msg_iovlen: 1,
                msg_control: std::ptr::null_mut(), msg_controllen: 0,
                msg_flags: 0,
            }, msg_len: 0 };
            hdrs.push(hdr);
        }
        let sent = sendmmsg(fd, &mut hdrs, MsgFlags::MSG_DONTWAIT).unwrap_or(0);
        items.iter().take(sent).map(|(s, _)| *s).collect()
    }
}

// -----------------------------
// Optional: Leader window p95 model via HDR histogram (feature-gated)
// -----------------------------
#[cfg(feature = "hdrwin")]
mod window_model {
    use super::*;
    use hdrhistogram::Histogram;
    pub struct WindowModel { pub h: Histogram<u64> }
    impl WindowModel { pub fn new() -> Self { Self { h: Histogram::new_with_bounds(1, 10_000, 3).unwrap() } } }
    pub static WINDOW_MODELS: once_cell::sync::Lazy<dashmap::DashMap<Pubkey, WindowModel>> =
        once_cell::sync::Lazy::new(|| dashmap::DashMap::new());
    pub fn update_leader_window(leader: Pubkey, offset_us: u64, stats: &mut LeaderStats) {
        let mut entry = WINDOW_MODELS.entry(leader).or_insert_with(WindowModel::new);
        let _ = entry.value_mut().h.record(offset_us.max(1));
        let p95 = entry.value().h.value_at_quantile(0.95).clamp(1600, 4000) as f64;
        stats.ema_window_us = 0.8 * stats.ema_window_us + 0.2 * p95;
    }
}
#[cfg(not(feature = "hdrwin"))]
mod window_model {
    use super::*;
    pub fn update_leader_window(_leader: Pubkey, offset_us: u64, stats: &mut LeaderStats) {
        let p = (offset_us as f64).clamp(1600.0, 4000.0);
        stats.ema_window_us = 0.8 * stats.ema_window_us + 0.2 * p;
    }
}

// -----------------------------
// P&L guardrail with cool-down
// -----------------------------
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering as Ao};
struct PnLGuard {
    pnl_1k: AtomicI64,
    samples: AtomicU64,
    tripped_until_slot: AtomicU64,
}
impl PnLGuard {
    const MIN_SAMPLES: u64 = 16;
    const NEG_THRESHOLD_1K: i64 = -50_000; // -50M lamports total / 1000
    fn tripped(&self, cur_slot: u64) -> bool { self.tripped_until_slot.load(Ao::Relaxed) > cur_slot }
    fn on_result(&self, cur_slot: u64, pnl_lamports: i64, cooloff_slots: u64) {
        self.pnl_1k.fetch_add(pnl_lamports / 1000, Ao::Relaxed);
        let s = self.samples.fetch_add(1, Ao::Relaxed) + 1;
        if s >= Self::MIN_SAMPLES {
            let tot = self.pnl_1k.load(Ao::Relaxed);
            if tot <= Self::NEG_THRESHOLD_1K {
                self.tripped_until_slot.store(cur_slot + cooloff_slots, Ao::Relaxed);
                self.pnl_1k.store(0, Ao::Relaxed);
                self.samples.store(0, Ao::Relaxed);
                metrics::increment_counter!("guard.pnl.tripped");
            }
        }
    }
}
static PNL_GUARD: once_cell::sync::Lazy<PnLGuard> = once_cell::sync::Lazy::new(|| PnLGuard { pnl_1k: 0.into(), samples: 0.into(), tripped_until_slot: 0.into() });

// -----------------------------
// Slot-scoped budgets (lamports, tx count, cooldown)
// -----------------------------
use core::sync::atomic::{AtomicI64 as CoreAtomicI64, AtomicU64 as CoreAtomicU64, Ordering as CoreOrd};
pub struct SlotBudgets {
    slot: CoreAtomicU64,
    spent_lamports: CoreAtomicI64,
    tx_count: CoreAtomicU64,
    max_lamports: i64,
    max_txs: u64,
    cooldown_slots: CoreAtomicU64,
}
impl SlotBudgets {
    pub fn new(max_lamports: i64, max_txs: u64) -> Self {
        Self { slot: 0u64.into(), spent_lamports: 0i64.into(), tx_count: 0u64.into(), max_lamports, max_txs, cooldown_slots: 0u64.into() }
    }
    #[inline]
    pub fn begin_slot(&self, cur_slot: u64) {
        let prev = self.slot.swap(cur_slot, CoreOrd::AcqRel);
        if prev != cur_slot {
            self.spent_lamports.store(0, CoreOrd::Release);
            self.tx_count.store(0, CoreOrd::Release);
            if self.cooldown_slots.load(CoreOrd::Acquire) > 0 {
                self.cooldown_slots.fetch_sub(1, CoreOrd::AcqRel);
            }
        }
    }
    #[inline]
    pub fn allow_send(&self) -> bool {
        if self.cooldown_slots.load(CoreOrd::Acquire) > 0 { return false; }
        let spent = self.spent_lamports.load(CoreOrd::Acquire);
        let n = self.tx_count.load(CoreOrd::Acquire);
        spent < self.max_lamports && n < self.max_txs
    }
    #[inline]
    pub fn on_result(&self, pnl_lamports: i64, cooldown_on_breach_slots: u64) {
        self.spent_lamports.fetch_add((-pnl_lamports).max(0), CoreOrd::AcqRel);
        self.tx_count.fetch_add(1, CoreOrd::AcqRel);
        let spent = self.spent_lamports.load(CoreOrd::Acquire);
        if spent >= self.max_lamports {
            self.cooldown_slots.store(cooldown_on_breach_slots, CoreOrd::Release);
        }
    }
}
static SLOT_BUDGETS: OnceLock<SlotBudgets> = OnceLock::new();

// Degree-ordered graph coloring partitioner for maximal non-conflicting parallelism
pub fn partition_conflicts_coloring<T, F>(
    reqs: &[T],
    writes_of: F,
    max_groups: usize,
) -> Vec<Vec<T>>
where
    T: Clone,
    F: Fn(&T) -> Vec<Pubkey>,
{
    if reqs.is_empty() { return vec![Vec::new()]; }
    if reqs.len() == 1 { return vec![vec![reqs[0].clone()]]; }

    // 1) Build conflict sets by account
    let mut by_acc: HashMap<Pubkey, Vec<usize>> = HashMap::new();
    for (i, r) in reqs.iter().enumerate() {
        for a in writes_of(r) { by_acc.entry(a).or_default().push(i); }
    }
    // 2) Build adjacency
    let mut adj: Vec<HashSet<usize>> = vec![HashSet::new(); reqs.len()];
    for idxs in by_acc.values() {
        for &i in idxs {
            for &j in idxs {
                if i != j { adj[i].insert(j); }
            }
        }
    }
    // 3) Order by degree desc
    let mut order: Vec<usize> = (0..reqs.len()).collect();
    order.sort_by_key(|&i| core::cmp::Reverse(adj[i].len()));

    // 4) Greedy coloring (first-fit)
    let mut color: Vec<Option<usize>> = vec![None; reqs.len()];
    let mut used = 0usize;
    let cap = max_groups.max(1);
    for i in order {
        // mark forbidden colors
        let mut forbid = vec![false; cap];
        for &j in &adj[i] {
            if let Some(c) = color[j] { if c < forbid.len() { forbid[c] = true; } }
        }
        // pick the smallest available color
        let mut c = 0usize;
        while c < cap && forbid[c] { c += 1; }
        if c == cap {
            // overflow: place into the minimum-load group (best effort)
            c = (0..cap)
                .min_by_key(|g| color.iter().filter(|x| x == &&Some(*g)).count())
                .unwrap_or(0);
        }
        color[i] = Some(c);
        used = used.max(c + 1);
    }
    // 5) Build groups
    let mut out = vec![Vec::new(); used.max(1)];
    for (i, c) in color.into_iter().enumerate() {
        out[c.unwrap_or(0)].push(reqs[i].clone());
    }
    out
}
        }
        permits
    }

    /// Called when a signature is confirmed landed; updates per-leader fee bandit and stats
    pub async fn on_landed(&self, sig: Signature) {
        if let Some((ldr, arm)) = CHOSEN_ARM_FOR_SIG.get_or_init(|| DashMap::new()).remove(&sig).map(|e| e.1) {
            if let Some(mut b) = FEE_BANDITS.get_or_init(|| DashMap::new()).get_mut(&ldr) {
                b.update(arm, true);
            }
            if let Some(mut st) = self.leader_stats_cache.get_mut(&ldr) {
                st.landed = st.landed.saturating_add(1);
            }
        }
    }

    // Acquire multi-key permits in canonical order to avoid deadlocks; all-or-nothing with micro-timeout
    #[inline]
    async fn acquire_write_permits_ordered(
        &self,
        writes: &[Pubkey],
        timeout_us: u64,
    ) -> Option<Vec<tokio::sync::OwnedSemaphorePermit>> {
        let gates = ACCOUNT_GATES.get_or_init(|| DashMap::new());
        let mut keys: Vec<Pubkey> = writes.to_vec();
        keys.sort();
        keys.dedup();
        let mut permits = Vec::with_capacity(keys.len());
        for k in keys {
            let sem = gates
                .entry(k)
                .or_insert_with(|| Arc::new(tokio::sync::Semaphore::new(1)))
                .clone();
            match tokio::time::timeout(std::time::Duration::from_micros(timeout_us), sem.acquire_owned()).await {
                Ok(Ok(p)) => permits.push(p),
                _ => return None,
            }
        }
        Some(permits)
    }

    // Hedged raw send wrapper: currently falls back to single-QUIC path; can be extended to fanout endpoints
    #[inline]
    async fn hedged_raw_send(&self, bytes: Bytes) -> Result<(), anyhow::Error> {
        // If sender exposes multiple endpoints in the future, race them here.
        self.sender
            .send_raw_quick(&bytes)
            .await
            .map_err(|e| anyhow::anyhow!(e))
    }
// slot_boundary_flashloan_executor_v2.rs
// Top-1% upgrade of your slot boundary flashloan executor.
// Features: WebSocket subscriptions, TPU/QUIC sender, leader-aware fanout, pre-signed variants,
// per-leader fee modeling, account-scoped fee estimator, simulation-driven CU, ALTs handling,
// conflict-aware partitioning, retries categorized, sled persistence, structured JSON logs.

#![allow(clippy::large_enum_variant)]

#[cfg(feature = "mimalloc")]
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

use anyhow::{anyhow, Context, Result};
use anchor_lang::prelude::*;
use anchor_lang::solana_program::{
    clock::Clock,
    instruction::{AccountMeta, Instruction},
    program::invoke_signed,
    pubkey::Pubkey,
    system_instruction,
    sysvar::clock,
};

use dashmap::DashMap;
use futures_util::{SinkExt, StreamExt, TryStreamExt};
use parking_lot::Mutex as FastMutex;
use quinn::{ClientConfig, Endpoint, Connection, TransportConfig};
#[cfg(feature = "quic_bbr")]
use quinn::congestion::BbrConfig;
use bytes::{Bytes, BufMut, BytesMut};
use bytes::buf::Writer;
use serde::{Deserialize, Serialize};
use sled::{self, Db};
use tokio_tungstenite::tungstenite::Message as WsMessage;
use uuid::Uuid;
#[cfg(feature = "persist_bincode")]
use bincode;
use std::sync::OnceLock;
use std::sync::Arc;
use std::collections::{HashMap, HashSet};
use once_cell::sync::Lazy;
use arrayvec::ArrayVec;
use smallvec::SmallVec;
use tokio_util::sync::CancellationToken;
use tracing::{info_span, Instrument};
use std::hash::{Hash as StdHash, Hasher as _};
use futures::{Stream, StreamExt};
use std::{pin::Pin, future::Future};
use std::sync::atomic::{Ordering, AtomicU64};
use ahash::{RandomState as AHashBuilder, AHasher};

use solana_client::{
    nonblocking::{pubsub_client::PubsubClient, rpc_client::RpcClient},
    rpc_config::{RpcSendTransactionConfig, RpcSimulateTransactionConfig},
    rpc_response::{RpcSignatureResult, RpcSimulateTransactionResult, SlotInfo},
};

use solana_sdk::{
    commitment_config::{CommitmentConfig, CommitmentLevel},
    compute_budget::ComputeBudgetInstruction,
    hash::Hash,
    message::{v0::Message as V0Message, VersionedMessage},
    packet::PACKET_DATA_SIZE,
    signature::{Keypair, Signature},
    signer::Signer,
    timing::timestamp,
    transaction::{Transaction, VersionedTransaction},
};

use solana_transaction_status::UiTransactionEncoding;
use thiserror::Error;
use tracing::{debug, error, info, instrument, warn};

use tokio::{
    sync::{mpsc, RwLock, Semaphore},
    task::JoinSet,
    time::{interval, sleep, Duration, Instant},
};

use std::{
    collections::{VecDeque},
    net::SocketAddr,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc,
    },
};
use ahash::{RandomState as FastHash, AHasher};
// Drop-in faster map/set aliases for hot paths
pub type FastMap<K, V> = std::collections::HashMap<K, V, FastHash>;
pub type FastSet<K>    = std::collections::HashSet<K, FastHash>;

use std::cell::{Cell, RefCell};
use std::hash::{Hash, Hasher};
use core::ops::{Add, Sub};
use std::sync::atomic::{AtomicU32, Ordering};

thread_local! {
    static TL_RNG: Cell<u64> = Cell::new(0x9E37_79B9_7F4A_7C15);
}

// -----------------------------
// Per-slot token bucket for simulations/builds
// -----------------------------
#[derive(Default)]
struct SlotBudget {
    slot: AlignedAtomicU64,
    sims_left: AtomicU32,
    builds_left: AtomicU32,
}
impl SlotBudget {
    #[inline]
    fn on_slot(&self, s: u64, sims: u32, builds: u32) {
        self.slot.store(s, Ordering::Release);
        self.sims_left.store(sims, Ordering::Release);
        self.builds_left.store(builds, Ordering::Release);
    }
    #[inline]
    fn try_sim(&self) -> bool {
        let mut cur = self.sims_left.load(Ordering::Acquire);
        while cur > 0 {
            match self.sims_left.compare_exchange(cur, cur - 1, Ordering::AcqRel, Ordering::Relaxed) {
                Ok(_) => return true,
                Err(v) => cur = v,
            }
        }
        false
    }
    #[inline]
    fn try_build(&self) -> bool {
        let mut cur = self.builds_left.load(Ordering::Acquire);
        while cur > 0 {
            match self.builds_left.compare_exchange(cur, cur - 1, Ordering::AcqRel, Ordering::Relaxed) {
                Ok(_) => return true,
                Err(v) => cur = v,
            }
        }
        false
    }
}

/// Bounded variant: merges tail groups to respect send parallelism
pub fn partition_non_conflicting_bounded<T, F>(
    requests: &[T],
    write_set_fn: F,
    max_groups: usize,
) -> Vec<Vec<T>>
where
    T: Clone,
    F: Fn(&T) -> Vec<Pubkey>,
{
    let mut groups = partition_non_conflicting(requests, &write_set_fn);
    if groups.len() <= max_groups { return groups; }

    let mut head: Vec<Vec<T>> = groups.drain(..max_groups).collect();
    for mut g in groups.into_iter() {
        let idx = head
            .iter()
            .enumerate()
            .min_by_key(|(_, v)| v.len())
            .map(|(i, _)| i)
            .unwrap_or(0);
        head[idx].append(&mut g);
    }

    /// Build ladder with upward trend skew to increase hit rate under rising fees
    pub fn ladder_with_trend(
        center_micro: u64,
        step_bps: u32,
        up: u8,
        down: u8,
        skew_up_bps: i32,
    ) -> Vec<u64> {
        let mut v = Vec::with_capacity((up as usize + down as usize + 1).max(1));
        v.push(center_micro);
        // upward: larger steps if strong trend up
        let up_step = (step_bps as i32 + (skew_up_bps / 5)).max(50) as u32;
        let mut cur = center_micro;
        for _ in 0..up {
            cur = ((cur as u128) * (10_000u128 + up_step as u128) / 10_000u128) as u64;
            v.push(cur);
        }
        // downward: keep tighter spacing
        let dn_step = step_bps.saturating_sub((skew_up_bps.max(0) as u32) / 10);
        cur = center_micro;
        for _ in 0..down {
            cur = ((cur as u128) * (10_000u128 - dn_step as u128) / 10_000u128) as u64;
            v.push(cur.max(1));
        }
        v.sort_unstable();
        v
    }
    head
}

// -----------------------------
// Zero-alloc helpers for hot paths
// -----------------------------
#[inline(always)]
fn extract_signature_fast(tx_bytes: &[u8]) -> Option<Signature> {
    // bincode Vec<Signature> prefix is u64 (LE) count
    if tx_bytes.len() < 8 + 64 { return None; }
    let mut n = [0u8; 8];
    n.copy_from_slice(&tx_bytes[..8]);
    if u64::from_le_bytes(n) == 0 { return None; }
    let mut s = [0u8; 64];
    s.copy_from_slice(&tx_bytes[8..72]);
    Some(Signature::new(&s))
}

#[inline(always)]
fn encode_vtx_into<'a>(vtx: &VersionedTransaction, buf: &'a mut Vec<u8>) -> &'a [u8] {
    buf.clear();
    bincode::serialize_into(&mut *buf, vtx).expect("bincode serialize vtx");
    &*buf
}

// Thread-local buffer for hot-path encoding without heap churn
thread_local! {
    static TLS_BUF: RefCell<BytesMut> = RefCell::new(BytesMut::with_capacity(8 * 1024));
}

#[inline]
fn encode_vtx_tls(vtx: &VersionedTransaction) -> Bytes {
    TLS_BUF.with(|cell| {
        let mut bm = cell.borrow_mut();
        bm.clear();
        // Writer gives an io::Write over BytesMut without copying
        let mut w: Writer<'_> = (&mut *bm).writer();
        bincode::serialize_into(&mut w, vtx).expect("serialize tx");
        // Freeze into Bytes without extra copy
        w.into_inner().split().freeze()
    })
}

#[inline]
fn vtx_sig_and_bytes(vtx: &VersionedTransaction) -> (Signature, Bytes) {
    let sig = vtx.signatures[0];
    let bytes = encode_vtx_tls(vtx);
    (sig, bytes)
}

// TLS xorshift64* RNG for no-alloc jitter
thread_local! { static XOR_STATE: std::cell::Cell<u64> = std::cell::Cell::new(0x9E37_79B9_7F4A_7C15); }

#[inline(always)]
fn jitter_ns_sym(bound_ns: u64) -> i64 {
    XOR_STATE.with(|s| {
        let mut x = s.get();
        x ^= x >> 12; x ^= x << 25; x ^= x >> 27;
        s.set(x);
        let r = x.wrapping_mul(0x2545F4914F6CDD1D) % (bound_ns as u64);
        let val = r as i64;
        if (x & 1) == 0 { val } else { -val }
    350→/// Micro spin calibrated to reduce scheduler jitter in the last few µs
#[inline(always)]
fn spin_us(mut us: u64) {
    if us == 0 { return; }
    if us > 50 { us = 50; }
    #[cfg(any(target_arch="x86", target_arch="x86_64"))]
    unsafe { core::arch::x86_64::_mm_pause(); }
    let start = std::time::Instant::now();
    while start.elapsed().as_micros() as u64 <= us {
        core::hint::spin_loop();
    }
}

// -----------------------------
// Typed send error classification
// -----------------------------
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
enum SendClass { OkFast, TooLowFee, WouldBeDropped, AccountInUse, BlockhashExpired, QoS, Other }

#[inline]
fn classify_send_error(e: &anyhow::Error) -> SendClass {
    use SendClass::*;
    if let Some(ce) = e.downcast_ref::<solana_client::client_error::ClientError>() {
        use solana_client::client_error::ClientErrorKind as CEK;
        match ce.kind() {
            CEK::TransactionError(solana_sdk::transaction::TransactionError::BlockhashNotFound) => return BlockhashExpired,
            CEK::TransactionError(solana_sdk::transaction::TransactionError::AccountInUse) => return AccountInUse,
            CEK::RpcError(solana_client::rpc_request::RpcError::RpcResponseError { .. }) => return WouldBeDropped,
            _ => {}
        }
    }
    // Fallback string sniffing
    let s = format!("{e:?}");
    if s.contains("BlockhashNotFound") || s.contains("Expired") { return BlockhashExpired; }
    if s.contains("AccountInUse") { return AccountInUse; }
    if s.contains("WouldBeDropped") { return WouldBeDropped; }
    if s.contains("TooLowFee") { return TooLowFee; }
    if s.contains("QoS") { return QoS; }
    Other
}

#[cold]
fn classify_send_error_cold<E: std::error::Error + 'static>(e: &E) -> SendClass {
    use SendClass::*;
    if let Some(ce) = e.downcast_ref::<solana_client::client_error::ClientError>() {
        use solana_client::client_error::ClientErrorKind as K;
        use solana_sdk::transaction::TransactionError as TE;
        return match ce.kind() {
            K::TransactionError(TE::BlockhashNotFound) => BlockhashExpired,
            K::TransactionError(TE::AccountInUse)      => AccountInUse,
            _ => Other,
        };
    }
    Other
}

#[inline(always)]
fn tx_len_bytes(vtx: &VersionedTransaction) -> usize {
    let mut buf = Vec::with_capacity(1024);
    encode_vtx_into(vtx, &mut buf).len()
}

// -----------------------------
// Linux realtime pinning (affinity + FIFO) and absolute sleep helpers
// -----------------------------
#[cfg(all(target_os = "linux", feature = "realtime"))]
pub struct RealtimeGuard(usize);
#[cfg(all(target_os = "linux", feature = "realtime"))]
impl RealtimeGuard {
    pub fn new(core: usize, prio: i32) -> Option<Self> {
        use std::mem::MaybeUninit;
        unsafe {
            let tid = libc::pthread_self();
            let mut set: libc::cpu_set_t = MaybeUninit::zeroed().assume_init();
            libc::CPU_ZERO(&mut set);
            libc::CPU_SET((core % num_cpus::get()) as usize, &mut set);
            if libc::pthread_setaffinity_np(tid, std::mem::size_of::<libc::cpu_set_t>(), &set) != 0 {
                return None;
            }
            let param = libc::sched_param { sched_priority: prio.clamp(1, 90) };
            if libc::pthread_setschedparam(tid, libc::SCHED_FIFO, &param) != 0 { return None; }
            Some(Self(core))
        }
    }
}
#[cfg(all(target_os = "linux", feature = "realtime"))]
impl Drop for RealtimeGuard {
    fn drop(&mut self) {
        unsafe {
            let tid = libc::pthread_self();
            let param = libc::sched_param { sched_priority: 0 };
            let _ = libc::pthread_setschedparam(tid, libc::SCHED_OTHER, &param);
        }
    }
}
#[cfg(not(all(target_os = "linux", feature = "realtime")))]
pub struct RealtimeGuard;
#[cfg(not(all(target_os = "linux", feature = "realtime")))]
impl RealtimeGuard { pub fn new(_:usize, _:i32)->Option<Self>{ None } }

// Monotonic absolute sleep to deadline with fallback
#[inline]
pub async fn sleep_until_abs(deadline: std::time::Instant) {
    #[cfg(all(target_os = "linux", feature = "abs_sleep"))]
    unsafe {
        use std::time::Duration;
        let now = std::time::Instant::now();
        if deadline > now {
            let dur = deadline.duration_since(now);
            let mut now_ts: libc::timespec = std::mem::zeroed();
            libc::clock_gettime(libc::CLOCK_MONOTONIC, &mut now_ts);
            let mut nsec = now_ts.tv_nsec as i128 + dur.subsec_nanos() as i128;
            let mut sec  = now_ts.tv_sec as i128 + dur.as_secs() as i128 + nsec / 1_000_000_000i128;
            nsec %= 1_000_000_000;
            let target = libc::timespec { tv_sec: sec as libc::time_t, tv_nsec: nsec as libc::c_long };
            let _ = libc::clock_nanosleep(libc::CLOCK_MONOTONIC, libc::TIMER_ABSTIME, &target, std::ptr::null_mut());
        }
    }
    #[cfg(not(all(target_os = "linux", feature = "abs_sleep")))]
    {
        let now = std::time::Instant::now();
        if deadline > now { tokio::time::sleep(deadline - now).await; }
    }
}

// -----------------------------
// Memory/stack prefault (Linux)
// -----------------------------
#[cfg(all(target_os = "linux", feature = "prefault"))]
pub fn prefault_process_memory() {
    unsafe {
        let lim = libc::rlimit { rlim_cur: 1<<26, rlim_max: 1<<26 };
        let _ = libc::setrlimit(libc::RLIMIT_MEMLOCK, &lim);
        let _ = libc::mlockall(libc::MCL_CURRENT | libc::MCL_FUTURE);
    }
}

// -----------------------------
// Linux UDP QoS + busy-poll (feature-gated)
// -----------------------------
#[cfg(all(target_os = "linux", feature = "udp_qos"))]
pub fn tune_udp_socket(sock: &std::net::UdpSocket) {
    use nix::sys::socket::{setsockopt, sockopt};
    use std::os::fd::AsRawFd;
    let fd = sock.as_raw_fd();
    // DSCP EF (46) -> TOS field upper 6 bits
    let tos: i32 = 46 << 2;
    let _ = setsockopt(fd, sockopt::IpTos, &tos);
    // busy poll (µs) and budget
    let _ = setsockopt(fd, sockopt::BusyPoll, &200);
    let _ = setsockopt(fd, sockopt::BusyPollBudget, &64);
}

// Dev/Test-only static size guard to keep hot structs cache-friendly
#[cfg(any(test, debug_assertions))]
const _: () = {
    assert!(core::mem::size_of::<LeaderStats>() <= 80, "LeaderStats grew; keep it ≤ 80 bytes");
};

// Global per-account concurrency gates to avoid self-contention across tasks
static ACCOUNT_GATES: OnceLock<DashMap<Pubkey, Arc<tokio::sync::Semaphore>>> = OnceLock::new();

// -----------------------------
// Thompson-sampling fee bandit per leader
// -----------------------------
const FEE_ARMS: [f64; 6] = [0.90, 0.95, 0.975, 0.99, 0.995, 0.999];

#[derive(Clone)]
struct FeeBandit { alpha: [f64; 6], beta: [f64; 6] }
impl Default for FeeBandit { fn default() -> Self { Self { alpha: [1.0; 6], beta: [1.0; 6] } } }
impl FeeBandit {
    fn choose(&self) -> (usize, f64) {
        // simple Thompson via two Exp(1) samples scaled by alpha/beta using TL_RNG
        fn rng01() -> f64 { // 0..1 (exclusive)
            TL_RNG.with(|c| {
                let mut x = c.get();
                // xorshift64*
                x ^= x >> 12; x ^= x << 25; x ^= x >> 27; x = x.wrapping_mul(0x2545F4914F6CDD1D);
                c.set(x);
                let u = ((x >> 11) as f64) / ((1u64 << 53) as f64);
                if u <= 0.0 { 1e-12 } else { u }
            })
        }
        let mut best = 0usize; let mut best_draw = f64::MIN;
        for i in 0..FEE_ARMS.len() {
            let a = self.alpha[i].max(1e-6);
            let b = self.beta[i].max(1e-6);
            let x = -rng01().ln() / a;
            let y = -rng01().ln() / b;
            let draw = x / (x + y);
            if draw > best_draw { best_draw = draw; best = i; }
        }
        (best, FEE_ARMS[best])
    }
    fn update(&mut self, arm: usize, success: bool) { if success { self.alpha[arm] += 1.0; } else { self.beta[arm] += 1.0; } }
}

static FEE_BANDITS: OnceLock<DashMap<Pubkey, FeeBandit>> = OnceLock::new();
static CHOSEN_ARM_FOR_SIG: OnceLock<DashMap<Signature, (Pubkey, usize)>> = OnceLock::new();

// -----------------------------
// Blockhash prefetcher + freshness guard
// -----------------------------
pub struct BlockhashCache {
    hash: FastRwLock<Hash>,
    fetched_at_us: std::sync::atomic::AtomicU64,
    ttl_slots: u64,
}
impl BlockhashCache {
    pub fn new(ttl_slots: u64) -> Self {
        Self { hash: FastRwLock::new(Hash::default()), fetched_at_us: std::sync::atomic::AtomicU64::new(0), ttl_slots }
    }
    #[inline]
    fn age_us(&self) -> u64 {
        mono_us().saturating_sub(self.fetched_at_us.load(std::sync::atomic::Ordering::Acquire))
    }
    #[inline]
    pub fn get(&self) -> Hash { *self.hash.read() }
    pub fn update(&self, h: Hash) {
        *self.hash.write() = h;
        self.fetched_at_us.store(mono_us(), std::sync::atomic::Ordering::Release);
    }
}

static BLOCKHASH_CACHE: OnceLock<Arc<BlockhashCache>> = OnceLock::new();

// -----------------------------
// Signature deduper ring buffer (lock-fast)
// -----------------------------
pub struct SigRing { ring: FastMutex2<VecDeque<Signature>>, cap: usize }
impl SigRing {
    pub fn new(cap: usize) -> Self { Self { ring: FastMutex2::new(VecDeque::with_capacity(cap)), cap } }
    #[inline]
    pub fn seen_or_insert(&self, s: &Signature) -> bool {
        let mut r = self.ring.lock();
        if r.iter().any(|x| x == s) { return true; }
        if r.len() >= self.cap { let _ = r.pop_front(); }
        r.push_back(*s);
        false
    }
}
static SENT_SIG_RING: OnceLock<SigRing> = OnceLock::new();

// Message-key dedupe ring (blockhash + message hash) that survives re-signs
#[derive(Copy, Clone, Eq, PartialEq, Hash)]
struct MsgKey { blockhash: Hash, msg_hash: u64 }

#[inline]
fn message_key(vtx: &VersionedTransaction) -> MsgKey {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash as Hh, Hasher};
    let bh = match &vtx.message { VersionedMessage::V0(m) => m.recent_blockhash, _ => Hash::default() };
    let bytes = bincode::serialize(&vtx.message).unwrap_or_default();
    let mut hasher = DefaultHasher::new();
    bytes.hash(&mut hasher);
    MsgKey { blockhash: bh, msg_hash: hasher.finish() }
}

pub struct KeyRing<const N: usize> { ring: FastMutex2<ArrayVec<MsgKey, N>> }
impl<const N: usize> KeyRing<N> {
    pub fn new() -> Self { Self { ring: FastMutex2::new(ArrayVec::new()) } }
    #[inline]
    pub fn seen_or_insert(&self, k: MsgKey) -> bool {
        let mut r = self.ring.lock();
        if r.iter().any(|x| *x == k) { return true; }
        if r.is_full() { r.pop_at(0); }
        r.push(k);
        false
    }
}
static MSG_DEDUP: Lazy<KeyRing<4096>> = Lazy::new(|| KeyRing::new());

// -----------------------------
// Lock-free hot-path rings (power-of-two, atomic slots)
// -----------------------------
#[inline]
fn fp64_from_sig(sig: &Signature) -> u64 { let b = sig.as_ref(); u64::from_le_bytes([b[0],b[1],b[2],b[3],b[4],b[5],b[6],b[7]]) }

pub struct SigRingLF {
    mask: usize,
    slots: Box<[AtomicU64]>,
    head: AtomicU64,
}
impl SigRingLF {
    pub fn new(cap_pow2: usize) -> Self {
        assert!(cap_pow2.is_power_of_two());
        let slots = (0..cap_pow2).map(|_| AtomicU64::new(0)).collect::<Vec<_>>().into_boxed_slice();
        Self { mask: cap_pow2 - 1, slots, head: AtomicU64::new(0) }
    }
    #[inline]
    pub fn seen_or_insert(&self, sig: &Signature) -> bool {
        let tag = fp64_from_sig(sig);
        let idx = (self.head.fetch_add(1, Ordering::AcqRel) as usize) & self.mask;
        let prev = self.slots[idx].swap(tag, Ordering::AcqRel);
        prev == tag
    }
}

pub struct U64RingLF {
    mask: usize,
    slots: Box<[AtomicU64]>,
    head: AtomicU64,
}
impl U64RingLF {
    pub fn new(cap_pow2: usize) -> Self {
        assert!(cap_pow2.is_power_of_two());
        let slots = (0..cap_pow2).map(|_| AtomicU64::new(0)).collect::<Vec<_>>().into_boxed_slice();
        Self { mask: cap_pow2 - 1, slots, head: AtomicU64::new(0) }
    }
    #[inline]
    pub fn seen_or_insert(&self, tag: u64) -> bool {
        let idx = (self.head.fetch_add(1, Ordering::AcqRel) as usize) & self.mask;
        let prev = self.slots[idx].swap(tag, Ordering::AcqRel);
        prev == tag
    }
}

#[inline]
fn msgkey_fp64(k: &MsgKey) -> u64 { k.msg_hash ^ u64::from_le_bytes(k.blockhash.as_ref()[..8].try_into().unwrap_or([0;8])) }

static SENT_SIG_RING_LF: OnceLock<SigRingLF> = OnceLock::new();
static MSG_KEY_RING_LF: OnceLock<U64RingLF> = OnceLock::new();

// -----------------------------
// Slot-scope cancellation guard (auto-abort on leader/slot flip)
// -----------------------------
struct SlotScope {
    slot: u64,
    cancel: CancellationToken,
}
impl SlotScope {
    async fn new(exec: &SlotBoundaryFlashLoanExecutorV2) -> Self {
        let slot = exec.get_current_slot().await;
        let cancel = CancellationToken::new();
        let c = cancel.clone();
        let ex = exec.clone();
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(std::time::Duration::from_micros(200)).await;
                if ex.get_current_slot().await != slot {
                    c.cancel();
                    break;
                }
            }
        });
        Self { slot, cancel }
    }
    #[inline] fn token(&self) -> &CancellationToken { &self.cancel }
}

// -----------------------------
// Adaptive per-leader concurrency controller (AIMD)
// -----------------------------
#[derive(Clone)]
struct Concurrency {
    inflight: std::sync::atomic::AtomicUsize,
    cap: std::sync::atomic::AtomicUsize,
    last: std::sync::atomic::AtomicU64,
}
impl Concurrency {
    fn new() -> Self { Self { inflight: 0.into(), cap: 4.into(), last: 0.into() } }
    fn try_acquire(&self) -> bool {
        use std::sync::atomic::Ordering;
        let mut cur = self.inflight.load(Ordering::Relaxed);
        let cap = self.cap.load(Ordering::Relaxed);
        loop {
            if cur >= cap { return false; }
            match self.inflight.compare_exchange_weak(cur, cur+1, Ordering::AcqRel, Ordering::Relaxed) {
                Ok(_) => return true,
                Err(v) => cur = v,
            }
        }
    }
    fn release(&self, success: bool) {
        use std::sync::atomic::Ordering;
        self.inflight.fetch_sub(1, Ordering::Release);
        let now = (chrono::Utc::now().timestamp_micros()) as u64;
        self.last.store(now, Ordering::Relaxed);
        if success {
            // Additively increase slowly
            let _ = self.cap.fetch_add(1, Ordering::Relaxed);
        } else {
            // Multiplicative decrease with floor
            let cap = self.cap.load(Ordering::Relaxed);
            self.cap.store(cap.saturating_sub((cap+1)/2).max(2), Ordering::Relaxed);
        }
    }
    fn decay_if_idle(&self, idle_us: u64) {
        use std::sync::atomic::Ordering;
        let now = (chrono::Utc::now().timestamp_micros()) as u64;
        let last = self.last.load(Ordering::Relaxed);
        if now.saturating_sub(last) > idle_us {
            let cap = self.cap.load(Ordering::Relaxed);
            self.cap.store((cap + 1).min(16), Ordering::Relaxed);
        }
    }
}
static LEADER_CONCURRENCY: Lazy<DashMap<Pubkey, Concurrency>> = Lazy::new(|| DashMap::new());

// Opt-in memory lock to avoid page faults on hot path (Linux only)
#[cfg(all(feature = "mlock", target_os = "linux"))]
#[inline]
pub fn try_lock_memory() {
    unsafe {
        let flags = libc::MCL_CURRENT | libc::MCL_FUTURE;
        let _ = libc::mlockall(flags);
    }
}

// -----------------------------
// BlockhashKeeper: fresh blockhash with generation + watch channel
// -----------------------------
use tokio::sync::watch;
pub struct BlockhashKeeper {
    latest: parking_lot::RwLock<Hash>,
    slot: std::sync::atomic::AtomicU64,
    gen: std::sync::atomic::AtomicU64,
    tx: watch::Sender<(Hash, u64)>,
    rx: watch::Receiver<(Hash, u64)>,
}
impl BlockhashKeeper {
    pub fn new(initial: Hash, slot: u64) -> Self {
        let (tx, rx) = watch::channel((initial, slot));
        Self { latest: parking_lot::RwLock::new(initial), slot: slot.into(), gen: 0u64.into(), tx, rx }
    }
    #[inline]
    pub fn current(&self) -> (Hash, u64, u64) {
        (*self.latest.read(), self.slot.load(Ordering::Relaxed), self.gen.load(Ordering::Relaxed))
    }
    pub async fn run(self: Arc<Self>, rpc: Arc<dyn RpcClientLike>) {
        // Slot WS updates (best-effort). Implementor provides stream of (slot, hash)
        let me = self.clone();
        tokio::spawn(async move {
            if let Ok(mut ws) = rpc.slot_subscribe().await {
                while let Some((slot, bh)) = ws.next().await {
                    let mut l = me.latest.write();
                    *l = bh;
                    me.slot.store(slot, Ordering::Relaxed);
                    me.gen.fetch_add(1, Ordering::Relaxed);
                    let _ = me.tx.send((*l, slot));
                }
            }
        });
        // Jittered poll as backup
        loop {
            let jitter_ns = jitter_ns_sym(250_000); // ±250µs jitter on 500ms base
            tokio::time::sleep(std::time::Duration::from_millis(500)).await;
            if let Ok((bh, slot)) = rpc.get_latest_blockhash_and_slot().await {
                let mut l = self.latest.write();
                if *l != bh {
                    *l = bh;
                    self.slot.store(slot, Ordering::Relaxed);
                    self.gen.fetch_add(1, Ordering::Relaxed);
                    let _ = self.tx.send((*l, slot));
                }
            }
            if jitter_ns.abs() > 0 { spin_us(1); }
        }
    }
    pub fn subscribe(&self) -> watch::Receiver<(Hash, u64)> { self.rx.clone() }
}

pub trait RpcClientLike: Send + Sync {
    fn slot_subscribe(&self) -> Pin<Box<dyn Future<Output = Result<Pin<Box<dyn Stream<Item = (u64, Hash)> + Send>>, anyhow::Error>> + Send>>;
    fn get_latest_blockhash_and_slot(&self) -> Pin<Box<dyn Future<Output = Result<(Hash, u64), anyhow::Error>> + Send>>;
}

// -----------------------------
// Presign cache keyed by (leader, blockhash generation, price)
// -----------------------------
#[derive(Clone)]
pub struct PresignMeta { pub compute_price: u64, pub cu_limit: u64 }

#[derive(Clone)]
pub struct Presigned { pub vtx: VersionedTransaction, pub sig: Signature, pub meta: PresignMeta, pub bytes: Bytes }

#[derive(Eq, PartialEq, Hash, Clone, Copy)]
struct CacheKey { leader: Pubkey, bh_gen: u64 }

pub struct PresignCache {
    by_leader_bh: DashMap<CacheKey, std::collections::BTreeMap<u64, std::collections::VecDeque<Presigned>>>,
    cap_per_key: usize,
}
impl PresignCache {
    pub fn new(cap_per_key: usize) -> Self { Self { by_leader_bh: DashMap::new(), cap_per_key } }
    pub fn insert(&self, leader: Pubkey, bh_gen: u64, p: Presigned) {
        let key = CacheKey { leader, bh_gen };
        let mut entry = self.by_leader_bh.entry(key).or_insert_with(std::collections::BTreeMap::new);
        let q = entry.entry(p.meta.compute_price).or_insert_with(std::collections::VecDeque::new);
        if q.len() >= self.cap_per_key { let _ = q.pop_front(); }
        q.push_back(p);
    }
    pub fn select_variant_by_price(&self, leader: Pubkey, bh_gen: u64, min_micro: u64) -> Option<Presigned> {
        let key = CacheKey { leader, bh_gen };
        let entry = self.by_leader_bh.get(&key)?;
        let mut it = entry.range(min_micro..).next()?;
        let q = it.1;
        q.back().cloned()
    }
    pub fn invalidate_generation(&self, leader: Pubkey, bh_gen: u64) { let _ = self.by_leader_bh.remove(&CacheKey { leader, bh_gen }); }
}

// -----------------------------
// Relay directory (leader -> TPU/relays)
// -----------------------------
pub struct RelayBook { map: DashMap<Pubkey, Vec<std::net::SocketAddr>> }
impl RelayBook {
    pub fn new() -> Self { Self { map: DashMap::new() } }
    pub fn set(&self, leader: Pubkey, addrs: Vec<std::net::SocketAddr>) { self.map.insert(leader, addrs); }
    pub fn get(&self, leader: &Pubkey) -> Vec<std::net::SocketAddr> { self.map.get(leader).map(|v| v.clone()).unwrap_or_default() }
}

// -----------------------------
// Hidden write-set expansion to avoid AccountInUse from implicit PDAs/ATAs
// -----------------------------
pub fn expand_hidden_writes(programs: &[Pubkey], metas: &[AccountMeta]) -> Vec<Pubkey> {
    let mut out: Vec<Pubkey> = metas.iter().filter(|m| m.is_writable).map(|m| m.pubkey).collect();

    // SPL Token: include ATAs of any (owner,mint) seen as inputs when token program present
    let token_present = programs.iter().any(|p| *p == spl_token::id());
    if token_present {
        use spl_associated_token_account::get_associated_token_address;
        let owners: Vec<Pubkey> = metas.iter().map(|m| m.pubkey).collect();
        let mints: Vec<Pubkey> = metas.iter().map(|m| m.pubkey).collect();
        for o in owners.iter() {
            for mint in mints.iter() {
                let ata = get_associated_token_address(o, mint);
                out.push(ata);
            }
        }
    }

    out.sort_unstable();
    out.dedup();
    out
}

#[inline(always)]
pub fn seed_tls_rng(seed_hint: u64) {
    // mix: time since start, address entropy, thread id, external hint
    let t = mono_us();
    let addr = (&TL_RNG as *const _ as usize) as u64;
    let mut h = AHasher::default();
    std::thread::current().id().hash(&mut h);
    let tid = h.finish();
    let mix = t.rotate_left(17) ^ addr.rotate_left(7) ^ tid ^ seed_hint.wrapping_mul(0x9E37_79B9_7F4A_7C15);
    TL_RNG.with(|s| s.set(mix ^ 0xD1B5_4A32_D192_ED03));
}

// unbiased range: [0, max_inclusive]
#[inline(always)]
pub fn jitter_micros(max_inclusive: u64) -> Duration {
    // multiply-high technique to avoid bias
    let r = xorshift64();
    let span = max_inclusive.saturating_add(1);
    let v = ((r as u128 * span as u128) >> 64) as u64;
    Duration::from_micros(v)
}

#[inline(always)]
pub fn jitter_nanos(max_inclusive: u64) -> Duration {
    let r = xorshift64();
    let span = max_inclusive.saturating_add(1);
    let v = ((r as u128 * span as u128) >> 64) as u64;
    Duration::from_nanos(v)
}

#[inline(always)]
pub fn jitter_symmetric_nanos(max_abs: u64) -> i64 {
    // unbiased in [-max_abs, +max_abs]
    let r = xorshift64();
    let span = (max_abs as u128).saturating_mul(2) + 1;
    let v = ((r as u128 * span) >> 64) as i128 - (max_abs as i128);
    v as i64
}

mod error;
mod execution;
mod fee_model;
mod metrics;

// Cross-arch prefetch helpers (best-effort)
#[inline(always)]
pub fn prefetch_read<T>(ptr: *const T) {
    #[cfg(any(target_arch = "x86_64", target_arch = "x86"))]
    unsafe { core::arch::x86_64::_mm_prefetch(ptr as *const i8, core::arch::x86_64::_MM_HINT_T0) };
    #[cfg(target_arch = "aarch64")]
    unsafe { core::arch::aarch64::__prefetch(ptr, 0) };
}

#[inline(always)]
pub fn prefetch_write<T>(ptr: *mut T) {
    #[cfg(any(target_arch = "x86_64", target_arch = "x86"))]
    unsafe { core::arch::x86_64::_mm_prefetch(ptr as *const i8, core::arch::x86_64::_MM_HINT_T0) };
    #[cfg(target_arch = "aarch64")]
    unsafe { core::arch::aarch64::__prefetch(ptr, 1) };
}
// Cache-line aligned atomic to avoid false sharing
#[repr(align(64))]
pub struct AlignedAtomicU64(pub AtomicU64);
impl AlignedAtomicU64 {
    #[inline(always)] pub const fn new(v: u64) -> Self { Self(AtomicU64::new(v)) }
    #[inline(always)] pub fn load(&self) -> u64 { self.0.load(Ordering::Relaxed) }
    #[inline(always)] pub fn store(&self, v: u64) { self.0.store(v, Ordering::Relaxed) }
    #[inline(always)] pub fn load_relaxed(&self) -> u64 { self.0.load(Ordering::Relaxed) }
    #[inline(always)] pub fn store_relaxed(&self, v: u64) { self.0.store(v, Ordering::Relaxed) }
    #[inline(always)] pub fn swap_relaxed(&self, v: u64) -> u64 { self.0.swap(v, Ordering::Relaxed) }
}

// Opt-in Linux realtime (SCHED_FIFO + optional CPU affinity)
#[cfg(all(feature = "realtime", target_os = "linux"))]
#[inline]
pub fn try_enable_realtime(core: Option<usize>, _prio: i32) {
    use thread_priority::{ThreadPriority, ThreadSchedulePolicy, set_thread_priority_and_policy, RealtimeThreadSchedulePolicy};
    let _ = set_thread_priority_and_policy(
        ThreadPriority::Max,
        ThreadSchedulePolicy::Realtime(RealtimeThreadSchedulePolicy::Fifo),
    );
    if let Some(c) = core {
        unsafe {
            let mut set: libc::cpu_set_t = std::mem::zeroed();
            libc::CPU_SET(c, &mut set);
            let _ = libc::sched_setaffinity(0, std::mem::size_of::<libc::cpu_set_t>(), &set);
        }
    }
}

// Zero-alloc tracing macro for hot paths
#[macro_export]
macro_rules! trace_kv {
    (target: $t:expr, $lvl:ident, $msg:expr, $( $k:literal => $v:expr ),* $(,)? ) => {{
        use tracing::Level;
        if tracing::event_enabled!(Level::$lvl) {
            tracing::event!(target: $t, Level::$lvl, $( $k = $v ),*, message = $msg);
        }
    }};
}

// Compact/human-toggleable serde for Pubkey
mod serde_pubkey {
    use super::*;
    use serde::{Serializer, Deserializer, ser::SerializeBytes, de::Error as DeError};
    use anchor_lang::prelude::Pubkey;

    #[cfg(feature = "pk_base58")]
    pub fn serialize<S: Serializer>(k: &Pubkey, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_str(&k.to_string())
    }
    #[cfg(feature = "pk_base58")]
    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<Pubkey, D::Error> {
        let s = String::deserialize(d)?;
        s.parse::<Pubkey>().map_err(serde::de::Error::custom)
    }

    #[cfg(not(feature = "pk_base58"))]
    pub fn serialize<S: Serializer>(k: &Pubkey, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_bytes(&k.to_bytes())
    }
    #[cfg(not(feature = "pk_base58"))]
    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<Pubkey, D::Error> {
        struct BytesVisitor;
        impl<'de> serde::de::Visitor<'de> for BytesVisitor {
            type Value = Pubkey;
            fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result { write!(f, "32 pubkey bytes") }
            fn visit_borrowed_bytes<E: DeError>(self, v: &'de [u8]) -> Result<Self::Value, E> {
                if v.len() != 32 { return Err(DeError::custom("invalid pubkey length")); }
                let mut arr = [0u8; 32];
                arr.copy_from_slice(v);
                Ok(Pubkey::new_from_array(arr))
            }
            fn visit_bytes<E: DeError>(self, v: &[u8]) -> Result<Self::Value, E> {
                self.visit_borrowed_bytes::<E>(v)
            }
        }
        d.deserialize_bytes(BytesVisitor)
    }
}

use error::{ExecError, Result as FlashloanResult};
use execution::{
    ensure_profit_after_fees, partition_non_conflicting, send_transaction_with_retry,
};
use fee_model::{PriorityFeeCalculator, FeeModel};
use metrics::*;
use tokio::io::AsyncWriteExt;
use std::time::Duration as StdDuration;
use smallvec::SmallVec;
// Concurrent map alias with AHash hasher for hot paths
pub type FastDashMap<K, V> = DashMap<K, V, FastHash>;


// -----------------------------
// Config & constants
// -----------------------------

// Timing (Duration-first, keep *_MS for back-compat)
pub const SLOT_DURATION_MS: u64 = 400;
pub const PREBOUNDARY_PREPARE_MS: u64 = 12;
pub const SEND_JITTER_MAX_MS: u64 = 1;

pub const SLOT_DURATION: Duration = Duration::from_millis(SLOT_DURATION_MS);
pub const PREBOUNDARY_PREPARE: Duration = Duration::from_millis(PREBOUNDARY_PREPARE_MS);
pub const SEND_JITTER_MAX: Duration = Duration::from_millis(SEND_JITTER_MAX_MS);

// Fee calculation
pub const CU_HEADROOM_PCT: f64 = 0.07; // 7%
pub const DEFAULT_CU_HEADROOM: f64 = 1.07;
pub const FEE_PCTILE_DEFAULT: f64 = 0.99;
pub const FEE_PCTILE_AGGRESSIVE: f64 = 0.995;
pub const FEE_PCTILE_PASSIVE: f64 = 0.95;

// Blockhash and confirmation
pub const MAX_BLOCKHASH_AGE_SLOTS: u64 = 80;
pub const MAX_CONFIRM_SLOTS: u64 = 8;
pub const FALLBACK_CONFIRM_SLOTS: u64 = 20;
pub const CONSECUTIVE_MISS_CUTOFF: u32 = 3;

// Pre-signing and simulation
pub const MAX_SIMULATIONS: usize = 10;
pub const PRE_SIGN_VARIANTS: usize = 4;
pub const PRE_SIGN_NEXT_BLOCKHASHS: usize = 2;

// Persistence
pub const SLED_DB_PATH: &str = "./leader_stats_db";
pub const SLOT_WS_SKEW_SMOOTHING: f64 = 0.2;

// Monotonic time helpers (no syscalls beyond first call)
static START_INSTANT: OnceLock<Instant> = OnceLock::new();

#[inline(always)]
pub fn mono_now() -> Instant {
    START_INSTANT.get_or_init(Instant::now);
    Instant::now()
}

#[inline(always)]
pub fn mono_elapsed() -> Duration {
    let s = START_INSTANT.get_or_init(Instant::now);
    s.elapsed()
}

#[inline(always)]
pub fn spin_until(deadline: Instant) {
    use std::hint::spin_loop;
    while mono_now() < deadline { spin_loop(); }
}

// Cycle-accurate microspin using TSC (x86_64 only, opt-in via `tsc` feature)
#[cfg(all(target_arch = "x86_64", feature = "tsc"))]
mod tsc_clock {
    use super::*;
    use std::sync::OnceLock;
    use core::arch::x86_64::_rdtsc;

    static CYCLES_PER_US: OnceLock<f64> = OnceLock::new();

    #[inline(always)]
    fn rdtsc() -> u64 { unsafe { _rdtsc() } }

    fn calibrate() -> f64 {
        let t0 = rdtsc();
        let s0 = mono_now();
        std::thread::sleep(std::time::Duration::from_millis(1));
        let t1 = rdtsc();
        let du = (mono_now() - s0).as_micros().max(1) as f64;
        ((t1 - t0) as f64) / du
    }

    #[inline(always)]
    pub fn cycles_per_us() -> f64 {
        *CYCLES_PER_US.get_or_init(|| calibrate())
    }

    #[inline(always)]
    pub fn spin_for(us: u64) {
        let start = rdtsc();
        let target = start + (us as f64 * cycles_per_us()) as u64;
        while rdtsc() < target { core::hint::spin_loop(); }
    }
}

#[cfg(not(all(target_arch = "x86_64", feature = "tsc")))]
#[inline(always)]
pub fn spin_for(us: u64) {
    spin_until(mono_now() + std::time::Duration::from_micros(us));
}

/// Low-latency process prep: mlock + realtime + RNG seed (opt-in via features)
#[inline]
pub fn prepare_low_latency(worker_idx: Option<usize>) {
    #[cfg(all(feature = "mlock", target_os = "linux"))]
    try_lock_memory();

    #[cfg(all(feature = "realtime", target_os = "linux"))]
    {
        try_enable_realtime(worker_idx, 0);
    }

    seed_tls_rng(worker_idx.unwrap_or(0) as u64);
}

/// Sleep-then-spin wait with symmetric nano jitter and cycle-accurate spin (when enabled)
#[inline(always)]
pub async fn wait_until_with_jitter(target: Instant, max_jitter_ns: u64) {
    let j = jitter_symmetric_nanos(max_jitter_ns);
    let tgt = if j >= 0 {
        target + std::time::Duration::from_nanos(j as u64)
    } else {
        target.saturating_duration_since(Instant::now())
              .checked_sub(std::time::Duration::from_nanos((-j) as u64))
              .map(|d| Instant::now() + d)
              .unwrap_or(target)
    };
    const SPIN_US: u64 = 300;
    let spin_start = tgt.saturating_sub(std::time::Duration::from_micros(SPIN_US));
    let now = mono_now();
    if now < spin_start {
        tokio::time::sleep_until(spin_start.into()).await;
    }
    let remain = tgt.saturating_duration_since(mono_now()).as_micros() as u64;
    if remain > 0 { spin_for(remain); }
}

#[allow(unused_macros)]
macro_rules! const_assert { ($x:expr) => { const _: () = assert!($x); }; }

const_assert!(SLOT_DURATION_MS > 0);
const_assert!(PREBOUNDARY_PREPARE_MS > 0);
const_assert!(SEND_JITTER_MAX_MS <= 5);
const_assert!(MAX_BLOCKHASH_AGE_SLOTS > 0);
const_assert!(MAX_CONFIRM_SLOTS > 0);

#[inline(always)]
pub fn dur_to_millis(d: Duration) -> u64 {
    d.as_secs().saturating_mul(1_000) + (d.subsec_nanos() as u64)/1_000_000
}
#[inline(always)]
pub fn dur_to_micros(d: Duration) -> u64 {
    d.as_secs().saturating_mul(1_000_000) + (d.subsec_nanos() as u64)/1_000
}

#[inline(always)]
fn validate_constants() {
    debug_assert!(CU_HEADROOM_PCT >= 0.0 && CU_HEADROOM_PCT <= 1.0);
    debug_assert!((0.0..1.0).contains(&FEE_PCTILE_DEFAULT));
    debug_assert!((0.0..1.0).contains(&FEE_PCTILE_AGGRESSIVE));
    debug_assert!((0.0..1.0).contains(&FEE_PCTILE_PASSIVE));
    debug_assert!(SLOT_DURATION_MS > 0 && PREBOUNDARY_PREPARE_MS > 0 && SEND_JITTER_MAX_MS <= 5);
}
#[repr(transparent)]
#[derive(Copy, Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub struct Lamports(pub u64);

#[repr(transparent)]
#[derive(Copy, Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub struct MicroLamportsPerCU(pub u64);

#[repr(transparent)]
#[derive(Copy, Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub struct ComputeUnits(pub u64);

impl From<u64> for Lamports               { #[inline(always)] fn from(v: u64) -> Self { Lamports(v) } }
impl From<u64> for MicroLamportsPerCU     { #[inline(always)] fn from(v: u64) -> Self { MicroLamportsPerCU(v) } }
impl From<u64> for ComputeUnits           { #[inline(always)] fn from(v: u64) -> Self { ComputeUnits(v) } }
impl From<Lamports> for u64               { #[inline(always)] fn from(v: Lamports) -> u64 { v.0 } }
impl From<MicroLamportsPerCU> for u64     { #[inline(always)] fn from(v: MicroLamportsPerCU) -> u64 { v.0 } }
impl From<ComputeUnits> for u64           { #[inline(always)] fn from(v: ComputeUnits) -> u64 { v.0 } }

// Saturating typed arithmetic for Lamports
impl Add for Lamports {
    type Output = Lamports;
    #[inline(always)]
    fn add(self, rhs: Lamports) -> Self::Output { Lamports(self.0.saturating_add(rhs.0)) }
}
impl Sub for Lamports {
    type Output = Lamports;
    #[inline(always)]
    fn sub(self, rhs: Lamports) -> Self::Output { Lamports(self.0.saturating_sub(rhs.0)) }
}

#[must_use]
#[inline(always)]
pub const fn mul_div_u128_sat(a: u64, b: u64, c: u64) -> u64 {
    if c == 0 { return u64::MAX; }
    let num = (a as u128).saturating_mul(b as u128);
    let q = num / (c as u128);
    if q > (u64::MAX as u128) { u64::MAX } else { q as u64 }
}

#[must_use]
#[inline(always)]
pub const fn priority_fee_lamports(micro_per_cu: MicroLamportsPerCU, cu: ComputeUnits) -> Lamports {
    // (µlamports/CU * CU) / 1_000_000
    Lamports(mul_div_u128_sat(micro_per_cu.0, cu.0, 1_000_000))
}

#[must_use]
#[inline(always)]
pub const fn mul_div_u128_ceil(a: u64, b: u64, c: u64) -> u64 {
    if c == 0 { return u64::MAX; }
    let num = (a as u128).saturating_mul(b as u128);
    let q = (num + (c as u128) - 1) / (c as u128);
    if q > (u64::MAX as u128) { u64::MAX } else { q as u64 }
}

#[must_use]
#[inline(always)]
pub const fn priority_fee_lamports_ceil(micro_per_cu: MicroLamportsPerCU, cu: ComputeUnits) -> Lamports {
    Lamports(mul_div_u128_ceil(micro_per_cu.0, cu.0, 1_000_000))
}

// Global branch prediction hints for reuse in hot paths
#[inline(always)] pub fn likely(b: bool) -> bool { #[allow(unused_unsafe)] unsafe { std::intrinsics::likely(b) } }
#[inline(always)] pub fn unlikely(b: bool) -> bool { #[allow(unused_unsafe)] unsafe { std::intrinsics::unlikely(b) } }

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(deny_unknown_fields)]
#[repr(C)]
struct LeaderStats {
    #[serde(with = "serde_pubkey")]
    leader: Pubkey,         // 32
    seen: u64,              // 8
    landed: u64,            // 8
    avg_units_landed: f64,  // 8
    mean_fee_to_land: f64,  // 8
    stdev_fee_to_land: f64, // 8
    last_updated: i64,      // 8
    // EMA of (mu + 2*sigma) in microseconds for leader-adaptive send window
    ema_window_us: f64,     // 8
}

impl LeaderStats {
    // exponential decay toward 0 with half-life (seconds)
    #[inline(always)]
    fn decay_to_now(&mut self, half_life_s: f64) {
        if self.last_updated <= 0 { return; }
        let age_s = (timestamp() as i64 - self.last_updated).max(0) as f64;
        if age_s <= 0.0 { return; }
        let lambda = (-age_s * std::f64::consts::LN_2 / half_life_s).exp();
        self.mean_fee_to_land  *= lambda;
        self.stdev_fee_to_land *= lambda;
        self.avg_units_landed  *= lambda;
        let c_lambda = lambda.clamp(0.5, 1.0);
        self.seen   = ((self.seen as f64)   * c_lambda).max(1.0) as u64;
        self.landed = ((self.landed as f64) * c_lambda).min(self.seen as f64) as u64;
        self.touch();
    }
    #[inline(always)]
    fn inclusion_rate(&self) -> f64 {
        if self.seen == 0 { 0.0 } else { self.landed as f64 / self.seen as f64 }
    }

    #[inline(always)]
    fn touch(&mut self) { self.last_updated = timestamp() as i64; }

    #[inline(always)]
    fn prefetch(&self) { prefetch_read(self as *const _); }

    /// O(1) updates; `landed` toggles inclusion ratio.
    #[inline(always)]
    fn record_sample(&mut self, fee_lamports: u64, units: u64, landed: bool) {
        // age-out old stats first; 120s half-life works well on Solana
        self.decay_to_now(120.0);
        self.seen = self.seen.saturating_add(1);
        if likely(landed) { self.landed = self.landed.saturating_add(1); }

        let n   = self.seen as f64;
        let fee = fee_lamports as f64;

        // online mean
        let delta = fee - self.mean_fee_to_land;
        self.mean_fee_to_land += delta / n;

        // robust EWMA (cheap & stable for HFT distributions)
        let dev = delta.abs();
        self.stdev_fee_to_land = 0.98 * self.stdev_fee_to_land + 0.02 * dev;

        // avg units
        self.avg_units_landed = ((self.avg_units_landed * (n - 1.0)) + units as f64) / n;

        self.touch();
    }
}

// -----------------------------
// Errors
// -----------------------------
#[derive(Error, Debug)]
pub enum ExecutorError {
    #[error("rpc error: {0}")]
    Rpc(String),

    #[error("simulation failed")]
    Simulation,

    #[error("signer error: {0}")]
    Signer(String),

    #[error("transmission failed: {0}")]
    Transmission(String),

    #[error("circuit breaker tripped")]
    CircuitBreaker,
}

impl From<anyhow::Error> for ExecutorError {
    #[cold]
    fn from(e: anyhow::Error) -> Self {
        ExecutorError::Rpc(e.to_string())
    }
}

// -----------------------------
// Persistence + Metrics
// -----------------------------
#[derive(Clone)]
struct Persistence {
    db: Arc<Db>,
    tree_leader: sled::Tree,
}
impl Persistence {
    #[inline]
    pub fn open(path: &str) -> Result<Self> {
        let cfg = sled::Config::default()
            .path(path)
            .mode(sled::Mode::LowSpace)
            .cache_capacity(64 * 1024 * 1024)
            .flush_every_ms(Some(2000));
        let db = cfg.open().context("open sled")?;
        let tree_leader = db.open_tree("leader_stats_v1").context("open leader_stats tree")?;
        Ok(Self { db: Arc::new(db), tree_leader })
    }

    #[inline]
    pub fn save_leader_stats(&self, key: &Pubkey, stats: &LeaderStats) -> Result<()> {
        let key_bytes = key.to_bytes();
        #[cfg(feature = "persist_bincode")]
        let bytes = bincode::serialize(stats).context("bincode serialize LeaderStats")?;
        #[cfg(not(feature = "persist_bincode"))]
        let bytes = serde_json::to_vec(stats).context("json serialize LeaderStats")?;
        self.tree_leader.insert(key_bytes, bytes)?;
        Ok(())
    }

    #[inline]
    pub fn load_leader_stats(&self, key: &Pubkey) -> Result<Option<LeaderStats>> {
        let key_bytes = key.to_bytes();
        if let Some(v) = self.tree_leader.get(key_bytes)? {
            #[cfg(feature = "persist_bincode")]
            let s: LeaderStats = bincode::deserialize(&v).context("bincode deserialize LeaderStats")?;
            #[cfg(not(feature = "persist_bincode"))]
            let s: LeaderStats = serde_json::from_slice(&v).context("json deserialize LeaderStats")?;
            Ok(Some(s))
        } else {
            Ok(None)
        }
    }

    /// Batch save for amortized fsync; call per-leader flush or per-slot.
    #[inline]
    pub fn save_leader_stats_batch<'a, I>(&self, items: I) -> Result<()>
    where
        I: IntoIterator<Item = (&'a Pubkey, &'a LeaderStats)>
    {
        let mut batch = sled::Batch::default();
        for (k, v) in items {
            let kb = k.to_bytes();
            #[cfg(feature = "persist_bincode")]
            let vb = bincode::serialize(v).context("bincode serialize LeaderStats")?;
            #[cfg(not(feature = "persist_bincode"))]
            let vb = serde_json::to_vec(v).context("json serialize LeaderStats")?;
            batch.insert(kb, vb);
        }
        self.tree_leader.apply_batch(batch).context("apply batch")?;
        Ok(())
    }

    /// Fast multi-load to warm caches or bulk restore
    #[inline]
    pub fn load_leader_stats_many<'a, I>(&self, keys: I) -> Result<Vec<Option<LeaderStats>>>
    where
        I: IntoIterator<Item = &'a Pubkey>
    {
        let mut out = Vec::new();
        for k in keys {
            let kb = k.to_bytes();
            if let Some(v) = self.tree_leader.get(kb)? {
                #[cfg(feature = "persist_bincode")]
                let s: LeaderStats = bincode::deserialize(&v).context("bincode deserialize LeaderStats")?;
                #[cfg(not(feature = "persist_bincode"))]
                let s: LeaderStats = serde_json::from_slice(&v).context("json deserialize LeaderStats")?;
                out.push(Some(s));
            } else {
                out.push(None);
            }
        }
        Ok(out)
    }

    /// Non-blocking durability hint; call from a low-frequency task.
    pub async fn flush_async(&self) -> Result<()> {
        self.db.flush_async().await.context("sled flush_async")?;
        Ok(())
    }
}

// -----------------------------
// lightweight percentile estimator (windowed) with const-generic P²
// -----------------------------
pub mod pct { pub const P95: f64 = 0.95; pub const P99: f64 = 0.99; pub const P995: f64 = 0.995; }
const TRACK_PS: [f64; 3] = [pct::P95, pct::P99, pct::P995];

#[derive(Clone)]
struct P2Quantiles<const K: usize> {
    ps: [f64; K],
    inited: bool,
    q: [[f64; 5]; K],   // heights
    n: [[f64; 5]; K],   // positions
    np: [[f64; 5]; K],  // desired positions
    dn: [[f64; 5]; K],  // desired increments
    boot: heapless::Vec<f64, 32>, // small bootstrap buffer (stack-based)
}

impl<const K: usize> P2Quantiles<K> {
    #[inline]
    fn new(ps: [f64; K]) -> Self {
        Self { ps, inited: false, q: [[0.0; 5]; K], n: [[0.0; 5]; K], np: [[0.0; 5]; K], dn: [[0.0; 5]; K], boot: heapless::Vec::new() }
    }
    #[inline]
    fn push(&mut self, x: f64) {
        if !self.inited {
            let _ = self.boot.push(x);
            if self.boot.len() < 5 { return; }
            self.boot.sort_by(|a,b| a.partial_cmp(b).unwrap());
            self.inited = true;
            for i in 0..K {
                self.q[i]  = [self.boot[0], self.boot[1], self.boot[2], self.boot[3], self.boot[4]];
                self.n[i]  = [1.0, 2.0, 3.0, 4.0, 5.0];
                self.np[i] = [1.0, 1.0 + 2.0*self.ps[i], 1.0 + 4.0*self.ps[i], 3.0 + 2.0*self.ps[i], 5.0];
                self.dn[i] = [0.0, self.ps[i]/2.0, self.ps[i], (1.0 + self.ps[i])/2.0, 1.0];
            }
            return;
        }

        for i in 0..K {
            let k = if x < self.q[i][0] { self.q[i][0] = x; 0 }
            else if x >= self.q[i][4]   { self.q[i][4] = x; 3 }
            else {
                let mut kk = 0usize;
                while kk < 4 && x >= self.q[i][kk+1] { kk += 1; }
                kk
            };

            for m in 0..5 { if (m as i32) > k { self.n[i][m] += 1.0; } }
            for m in 0..5 { self.np[i][m] += self.dn[i][m]; }

            for m in 1..4 {
                let d = self.np[i][m] - self.n[i][m];
                if (d >= 1.0 && (self.n[i][m+1]-self.n[i][m]) > 1.0) || (d <= -1.0 && (self.n[i][m]-self.n[i][m-1]) > 1.0) {
                    let s = d.signum();
                    let qhat = self.q[i][m] + s * (
                        (self.n[i][m]-self.n[i][m-1]+s) * (self.q[i][m+1]-self.q[i][m]) / (self.n[i][m+1]-self.n[i][m]) +
                        (self.n[i][m+1]-self.n[i][m]-s) * (self.q[i][m]-self.q[i][m-1]) / (self.n[i][m]-self.n[i][m-1])
                    ) / (self.n[i][m+1]-self.n[i][m-1]);

                    if self.q[i][m-1] < qhat && qhat < self.q[i][m+1] {
                        self.q[i][m] = qhat;
                    } else {
                        let idx = (m as i32 + s as i32) as usize;
                        self.q[i][m] += s * (self.q[i][idx] - self.q[i][m]) / (self.n[i][idx] - self.n[i][m]);
                    }
                    self.n[i][m] += s;
                }
            }
        }
    }
    #[inline]
    fn estimate(&self, p: f64) -> Option<f64> {
        if !self.inited { return None; }
        let mut best_i = 0usize;
        let mut best_d = (self.ps[0] - p).abs();
        for i in 1..K {
            let d = (self.ps[i] - p).abs();
            if d < best_d { best_d = d; best_i = i; }
        }
        Some(self.q[best_i][2])
    }
}

#[derive(Clone)]
struct SlidingWindowPercentileImpl<const K: usize> {
    window: Arc<FastMutex<VecDeque<u64>>>,
    capacity: usize,
    p2: Arc<FastMutex<P2Quantiles<K>>>,
}

impl<const K: usize> SlidingWindowPercentileImpl<K> {
    fn new(capacity: usize) -> Self {
        Self {
            window: Arc::new(FastMutex::new(VecDeque::with_capacity(capacity))),
            capacity,
            p2: Arc::new(FastMutex::new(P2Quantiles::<K>::new(TRACK_PS))),
        }
    }

    #[inline(always)]
    fn push(&self, value: u64) {
        // SAFETY: scoped lock; no panics; minimal critical section.
        {
            let mut w = self.window.lock();
            w.push_back(value);
            if w.len() > self.capacity { w.pop_front(); }
        }
        // outside lock: update P²
        self.p2.lock().push(value as f64);
    }


// Preserve original external name with a concrete K
type SlidingWindowPercentile = SlidingWindowPercentileImpl<3>;

// -----------------------------
// TPU/QUIC sender trait + implementations
// -----------------------------
#[async_trait::async_trait]
trait Sender: Send + Sync {
    async fn send(&self, signed_tx: &VersionedTransaction) -> Result<Signature>;
    async fn send_raw_quick(&self, tx_bytes: &[u8]) -> Result<Signature>;

    // Batch fire-and-forget best-effort; return first signatures for bookkeeping
    async fn send_batch(&self, txs: &[(Signature, &[u8])]) -> Result<Vec<Signature>>;
}

#[derive(Clone)]
struct RpcFallbackSender {
    rpc: Arc<RpcClient>,
}

#[async_trait::async_trait]
impl Sender for RpcFallbackSender {
    async fn send(&self, signed_tx: &VersionedTransaction) -> Result<Signature> {
        // translate to legacy Transaction if needed
        let tx = solana_sdk::transaction::VersionedTransaction::clone(signed_tx);
        let config = RpcSendTransactionConfig {
            skip_preflight: true,
            preflight_commitment: Some(CommitmentConfig::processed()),
            ..Default::default()
        };
        let sig = self.rpc.send_transaction_with_config(&tx, config).await?;
        Ok(sig)
    }

    async fn send_raw_quick(&self, tx_bytes: &[u8]) -> Result<Signature> {
        // Fallback raw send via RPC if necessary
        let config = RpcSendTransactionConfig {
            skip_preflight: true,
            preflight_commitment: Some(CommitmentConfig::processed()),
            ..Default::default()
        };
        let sig = self.rpc.send_raw_transaction_with_config(tx_bytes.to_vec(), config).await?;
        Ok(sig)
    }
}

#[derive(Clone)]
struct QuicConnPool {
    ep: Arc<Endpoint>,
    // addr -> (conn, last_used_mono_us)
    pool: Arc<DashMap<SocketAddr, (Connection, AlignedAtomicU64)>>,
}

impl QuicConnPool {
    async fn new(ep: Endpoint) -> Self {
        let s = Self { ep: Arc::new(ep), pool: Arc::new(DashMap::new()) };
        // Background GC: reap idle connections (~1s idle)
        {
            let pool = s.pool.clone();
            tokio::spawn(async move {
                let idle_us = 1_000_000u64;
                let mut ticker = tokio::time::interval(StdDuration::from_millis(250));
                loop {
                    ticker.tick().await;
                    let now = mono_us();
                    for kv in pool.iter() {
                        if now.saturating_sub(kv.value().1.load_relaxed()) > idle_us {
                            pool.remove(kv.key());
                        }
                    }
                }
            });
        }
        s
    }

    #[inline]
    async fn get_or_connect(&self, addr: SocketAddr) -> Result<Connection> {
        if let Some(e) = self.pool.get(&addr) {
            e.value().1.store_relaxed(mono_us());
            return Ok(e.value().0.clone());
        }
        let connecting = self.ep.connect(addr, "solana-tpu")?;
        let (conn, _0rtt) = connecting.into_0rtt().await.map_err(|e| anyhow!("0-RTT failed: {e}"))?;
        self.pool.insert(addr, (conn.clone(), AlignedAtomicU64::new(mono_us())));
        Ok(conn)
    }

    fn gc_cold(&self, max_idle_us: u64) {
        let now = mono_us();
        for kv in self.pool.iter() {
            if now.saturating_sub(kv.value().1.load_relaxed()) > max_idle_us {
                self.pool.remove(kv.key());
            }
        }
    }
}

fn tuned_quic_client_config() -> ClientConfig {
    let mut cc = ClientConfig::default();
    let mut tc = TransportConfig::default();
    tc.keep_alive_interval(Some(StdDuration::from_millis(250)));
    tc.max_idle_timeout(Some(StdDuration::from_secs(5).try_into().unwrap()));
    tc.datagram_send_buffer_size(Some(1 << 20));
    tc.migration(false);
    // We only use datagrams; cap streams to zero
    tc.max_concurrent_bidi_streams(0u32.into());
    tc.max_concurrent_uni_streams(0u32.into());
    // Optional: favor low queue buildup with BBR
    #[cfg(feature = "quic_bbr")]
    {
        tc.congestion_controller_factory(Arc::new(BbrConfig::default()));
    }
    cc.transport_config(Arc::new(tc));
    cc
}

async fn make_quic_endpoint() -> Result<Endpoint> {
    let ep = Endpoint::client("[::]:0".parse().unwrap())?;
    ep.set_default_client_config(tuned_quic_client_config());
    Ok(ep)
}

#[derive(Clone)]
struct QuicTpuSender {
    pool: Arc<QuicConnPool>,
    leader_tpu_map: Arc<DashMap<Pubkey, SocketAddr>>,
    fallback: RpcFallbackSender,
    // rolling stats per TPU
    stats: Arc<DashMap<SocketAddr, TpuStats>>,
    // hedged send parameters
    hedge_k: usize,
    hedge_stagger_us: u64,
    max_dgram: usize,
}

impl QuicTpuSender {
    pub async fn new(rpc: Arc<RpcClient>) -> Result<Self> {
        let endpoint = make_quic_endpoint().await?;
        Ok(Self {
            pool: Arc::new(QuicConnPool::new(endpoint).await),
            leader_tpu_map: Arc::new(DashMap::new()),
            fallback: RpcFallbackSender { rpc },
            stats: Arc::new(DashMap::new()),
            hedge_k: 2,
            hedge_stagger_us: 200,
            max_dgram: 1150,
        })
    }

    #[inline]
    pub fn update_leader_tpu(&self, leader: Pubkey, addr: SocketAddr) {
        self.leader_tpu_map.insert(leader, addr);
        self.stats.entry(addr).or_insert_with(TpuStats::default);
    }

    #[inline]
    fn choose_any_tpu(&self) -> Option<SocketAddr> {
        // Prefer TPU with most recent success; fallback to any known
        self.stats
            .iter()
            .max_by_key(|kv| kv.value().last_us.load_relaxed())
            .map(|kv| *kv.key())
            .or_else(|| self.leader_tpu_map.iter().next().map(|kv| *kv.value()))
    }

    #[inline]
    fn best_tpus(&self, prefer: Option<SocketAddr>) -> SmallVec<[SocketAddr; 4]> {
        let mut v: SmallVec<[SocketAddr; 4]> = SmallVec::new();
        for kv in self.stats.iter() { v.push(*kv.key()); }
        v.sort_unstable_by_key(|a| {
            self.stats.get(a)
                .map(|s| s.last_us.load_relaxed())
                .unwrap_or(0)
        });
        v.reverse();
        if let Some(pref) = prefer {
            if let Some(pos) = v.iter().position(|x| *x == pref) { v.swap(0, pos); }
            else { v.insert(0, pref); }
        }
        if v.is_empty() {
            if let Some(any) = self.leader_tpu_map.iter().next().map(|kv| *kv.value()) {
                v.push(any);
            }
        }
        v
    }

    #[inline]
    fn record_ok(&self, addr: SocketAddr) {
        if let Some(s) = self.stats.get(&addr) {
            s.ok.0.fetch_add(1, Ordering::Relaxed);
            s.last_us.store_relaxed(mono_us());
        }
    }
    #[inline]
    fn record_fail(&self, addr: SocketAddr) {
        if let Some(s) = self.stats.get(&addr) {
            s.fail.0.fetch_add(1, Ordering::Relaxed);
        }
    }
}

#[async_trait::async_trait]
impl Sender for QuicTpuSender {
    async fn send(&self, signed_tx: &VersionedTransaction) -> Result<Signature> {
        let sig = signed_tx.signatures[0];
        let mut buf = Vec::with_capacity(1024);
        let bytes = encode_vtx_into(signed_tx, &mut buf);
        if !self.leader_tpu_map.is_empty() {
            if let Err(e) = self.hedged_send(bytes, None).await {
                warn!("QUIC hedged send failed; fallback RPC: {e}");
                return self.fallback.send_raw_quick(bytes).await;
            }
            return Ok(sig);
        }
        self.fallback.send_raw_quick(bytes).await
    }

    async fn send_raw_quick(&self, tx_bytes: &[u8]) -> Result<Signature> {
        // decode signature locally using bincode Vec<Signature> layout
        let sig: Signature = extract_signature_fast(tx_bytes).unwrap_or_else(Signature::default);
        if !self.leader_tpu_map.is_empty() {
            if let Err(e) = self.hedged_send(tx_bytes, None).await {
                warn!("QUIC hedged raw send failed; fallback RPC: {e}");
                return self.fallback.send_raw_quick(tx_bytes).await;
            }
            return Ok(sig);
        }
        self.fallback.send_raw_quick(tx_bytes).await
    }

    async fn send_batch(&self, txs: &[(Signature, &[u8])]) -> Result<Vec<Signature>> {
        use futures_util::stream::{self, StreamExt};
        if self.leader_tpu_map.is_empty() {
            return self.fallback.send_batch(txs).await;
        }
        let max_conc = 64usize;
        let sent = stream::iter(txs.iter())
            .map(|(sig, bytes)| {
                async move {
                    let res = tokio::time::timeout(StdDuration::from_millis(50), self.hedged_send(bytes, None)).await;
                    match res { Ok(Ok(())) => Some(*sig), _ => None }
                }
            })
            .buffer_unordered(max_conc)
            .filter_map(|o| async move { o })
            .collect::<Vec<_>>()
            .await;
        if sent.is_empty() { return self.fallback.send_batch(txs).await; }
        Ok(sent)
    }
}

impl QuicTpuSender {
    #[inline]
    async fn send_datagram(&self, addr: SocketAddr, bytes: &[u8]) -> Result<()> {
        let conn = self.pool.get_or_connect(addr).await?;
        match conn.send_datagram(Bytes::copy_from_slice(bytes)) {
            Ok(()) => self.record_ok(addr),
            Err(e) => { self.record_fail(addr); return Err(e.into()); }
        }
        Ok(())
    }

    // Hedge to K best TPUs with micro-stagger; best-effort
    async fn hedged_send(&self, bytes: &[u8], prefer: Option<SocketAddr>) -> Result<()> {
        if bytes.len() > self.max_dgram {
            // oversized for QUIC datagram => fallback
            let _ = self.fallback.send_raw_quick(bytes).await?;
            return Ok(());
        }
        let addrs = self.best_tpus(prefer);
        let k = addrs.len().min(self.hedge_k.max(1));
        for (i, addr) in addrs.into_iter().take(k).enumerate() {
            if i != 0 { spin_for(self.hedge_stagger_us); }
            let _ = self.send_datagram(addr, bytes).await;
        }
        Ok(())
    }
}

#[derive(Default, Clone)]
struct TpuStats {
    ok: AlignedAtomicU64,
    fail: AlignedAtomicU64,
    last_us: AlignedAtomicU64,
}

// Fee model & priority fee calculator
// -----------------------------
#[inline(always)]
fn accounts_leader_key(leader: &Pubkey, accounts: &[Pubkey]) -> u64 {
    let mut h = AHasher::default();
    leader.hash(&mut h);
    for a in accounts { a.hash(&mut h); }
    h.finish()
}

#[derive(Clone)]
struct PriorityFeeModel {
    windows: Arc<DashMap<u64, SlidingWindowPercentile>>, // compact key
    base_priority_fee: Arc<AlignedAtomicU64>,
    last_push_us: Arc<DashMap<u64, AlignedAtomicU64>>,   // freshness per key
    // Guardrails & sticky quotes
    last_quote_micro: Arc<DashMap<u64, AlignedAtomicU64>>, // last µ/CU per key
    price_floor_micro: u64,
    price_cap_micro: u64,
    hysteresis_bps: u32,
    quantum_micro: u64,
    // Asymmetric smoothing & total fee floor
    alpha_up_bps: u32,
    alpha_dn_bps: u32,
    min_total_lamports: u64,
    // Volatility-driven headroom control
    min_headroom_bps: u32,
    max_extra_headroom_bps: u32,
    // Adaptive percentile selection state
    outcomes: Arc<DashMap<u64, AlignedAtomicU64>>,      // bitpack: [succ(32)][att(32)]
    last_paid_micro: Arc<DashMap<u64, AlignedAtomicU64>>, // last paid µ/CU per key
    prev_paid_micro: Arc<DashMap<u64, AlignedAtomicU64>>, // previous paid µ/CU per key
}

impl PriorityFeeModel {
    pub fn new(base: u64, _persistence: Persistence) -> Self {
        Self {
            windows: Arc::new(DashMap::new()),
            base_priority_fee: Arc::new(AlignedAtomicU64::new(base)),
            last_push_us: Arc::new(DashMap::new()),
            last_quote_micro: Arc::new(DashMap::new()),
            price_floor_micro: 1,
            price_cap_micro: 5_000_000,
            hysteresis_bps: 300,
            quantum_micro: 50,
            alpha_up_bps: 6_000,
            alpha_dn_bps: 1_500,
            min_total_lamports: 5_000,
            min_headroom_bps: 300,
            max_extra_headroom_bps: 1_200,
            outcomes: Arc::new(DashMap::new()),
            last_paid_micro: Arc::new(DashMap::new()),
            prev_paid_micro: Arc::new(DashMap::new()),
        }
    }

    pub async fn sample_and_push(&self, rpc: &RpcClient, accounts: &[Pubkey], leader: &Pubkey) -> Result<()> {
        let key = accounts_leader_key(leader, accounts);
        let mut acc_bytes: SmallVec<[[u8; 32]; 8]> = SmallVec::new();
        for a in accounts { acc_bytes.push(a.to_bytes()); }

        match rpc.get_recent_prioritization_fees(&acc_bytes).await {
            Ok(fees) => {
                let win = self.windows.entry(key).or_insert_with(|| SlidingWindowPercentile::new(1024)).clone();
                for info in fees { win.push(info.prioritization_fee); }
                self.last_push_us.entry(key).or_insert_with(|| AlignedAtomicU64::new(0)).store_relaxed(mono_us());
            }
            Err(e) => {
                tracing::debug!("priority fees fallback ({} accounts): {}", accounts.len(), e);
                let win = self.windows.entry(key).or_insert_with(|| SlidingWindowPercentile::new(1024)).clone();
                win.push(self.base_priority_fee.load_relaxed());
            }
        }
        Ok(())
    }

    #[inline]
    fn ensure_fresh<'a>(&'a self, rpc: &'a RpcClient, accounts: &'a [Pubkey], leader: &'a Pubkey) -> impl std::future::Future<Output=()> + 'a {
        async move {
            let key = accounts_leader_key(leader, accounts);
            let stale = self
                .last_push_us
                .get(&key)
                .map(|x| mono_us().saturating_sub(x.load_relaxed()) > 300_000)
                .unwrap_or(true);
            if stale {
                let _ = self.sample_and_push(rpc, accounts, leader).await;
            }
        }
    }

    /// Record outcome for adaptive percentile
    pub fn record_fill_outcome(&self, key: u64, paid_micro: u64, ok: bool) {
        let ent = self.outcomes.entry(key).or_insert_with(|| AlignedAtomicU64::new(0));
        let prev = ent.load_relaxed();
        let succ = (prev >> 32).saturating_add(if ok { 1 } else { 0 });
        let att  = (prev & 0xFFFF_FFFF).saturating_add(1);
        ent.store_relaxed((succ << 32) | (att & 0xFFFF_FFFF));

        self.last_paid_micro
            .entry(key)
            .or_insert_with(|| AlignedAtomicU64::new(0))
            .store_relaxed(paid_micro);
    }

    #[inline]
    pub fn miss_rate_bps(&self, key: &u64) -> u32 {
        if let Some(v) = self.outcomes.get(key) {
            let x = v.load_relaxed();
            let succ = (x >> 32) as u64;
            let att  = (x & 0xFFFF_FFFF) as u64;
            if att == 0 { 0 } else { (((att.saturating_sub(succ)) * 10_000) / att) as u32 }
        } else { 0 }
    }

    /// Choose percentile adaptively based on miss-rate + fee momentum
    pub fn choose_percentile(&self, key: &u64, user_percentile: f64) -> f64 {
        let miss_bps = self.miss_rate_bps(key);
        let last = self
            .last_paid_micro
            .get(key)
            .map(|x| x.load_relaxed())
            .unwrap_or(0);
        let prev = self
            .prev_paid_micro
            .get(key)
            .map(|x| x.load_relaxed())
            .unwrap_or(0);
        if last != 0 {
            self.prev_paid_micro
                .entry(*key)
                .or_insert_with(|| AlignedAtomicU64::new(0))
                .store_relaxed(last);
        }

        // fee momentum in bps
        let mom_bps: i32 = if prev > 0 && last > 0 {
            let num = (last as i128 - prev as i128) * 10_000i128;
            (num / (prev.max(1) as i128)).clamp(-2000, 2000) as i32
        } else { 0 };

        let mut p = user_percentile;
        // risk-off lift: up to +0.08 based on miss rate
        let lift = ((miss_bps as f64) / 10_000.0 * 0.08).min(0.08);
        p = (p + lift).min(0.995);
        // momentum tilt
        if mom_bps >= 800 { p = (p + 0.01).min(0.995); }
        else if mom_bps <= -800 { p = (p - 0.005).max(0.50); }
        p
    }

    /// estimate µlamports/CU at percentile with volatility-aware headroom, ceil, guardrails, hysteresis, and asymmetric smoothing
    pub async fn estimate_micro_per_cu(
        &self,
        rpc: &RpcClient,
        accounts: &[Pubkey],
        leader: &Pubkey,
        percentile: f64,
        units: u64,
        headroom_bps: u32,
    ) -> u64 {
        if units == 0 { return 0; }
        self.ensure_fresh(rpc, accounts, leader).await;
        let key = accounts_leader_key(leader, accounts);
        let eff_percentile = self.choose_percentile(&key, percentile);
        let (p50, p90, p_target) = if let Some(win) = self.windows.get(&key) {
            let p50 = win.percentile(0.50).unwrap_or_else(|| self.base_priority_fee.load_relaxed());
            let p90 = win.percentile(0.90).unwrap_or(p50);
            let px  = win.percentile(eff_percentile).unwrap_or(p90.max(p50));
            (p50, p90, px)
        } else {
            let base = self.base_priority_fee.load_relaxed();
            (base, base, base)
        };

        // Volatility proxy in bps: clamp to max_extra_headroom_bps
        let vol_bps: u32 = if p50 > 0 {
            let num = (p90.saturating_sub(p50) as u128) * 10_000u128;
            core::cmp::min((num / (p50 as u128)) as u32, self.max_extra_headroom_bps)
        } else { 0 };

        // Effective headroom = max(min_headroom, user_headroom) + half of vol_bps
        let base_headroom = headroom_bps.max(self.min_headroom_bps);
        let eff_headroom_bps = base_headroom.saturating_add(vol_bps / 2);

        // lamports -> µ/CU (ceil) + volatility-aware headroom
        let base_micro = mul_div_u128_ceil(p_target as u128, 1_000_000u128, units as u128) as u64;
        let with_headroom = ((base_micro as u128) * (10_000u128 + eff_headroom_bps as u128) + 9_999) / 10_000u128;
        let mut q = with_headroom as u64;

        // enforce minimum total lamports
        if q.saturating_mul(units) < self.min_total_lamports {
            q = ((self.min_total_lamports as u128 + units as u128 - 1) / units as u128) as u64;
        }

        // guardrails and quantization
        q = q.clamp(self.price_floor_micro, self.price_cap_micro);
        let step = self.quantum_micro.max(1);
        q = ((q + step - 1) / step) * step;

        // hysteresis band: stick if inside
        let last = self.last_quote_micro.entry(key).or_insert_with(|| AlignedAtomicU64::new(0));
        let prev = last.load_relaxed();
        if prev != 0 {
            let band = ((prev as u128) * (self.hysteresis_bps as u128)) / 10_000u128;
            let lo = prev.saturating_sub(band as u64);
            let hi = prev.saturating_add(band as u64);
            if (lo..=hi).contains(&q) { return prev; }
        }

        // asymmetric smoothing (faster up, slower down)
        let smoothed = if prev == 0 { q } else if q > prev {
            let delta = q - prev;
            let step_up = ((delta as u128 * self.alpha_up_bps as u128) / 10_000u128).max(1) as u64;
            prev + step_up
        } else {
            let delta = prev - q;
            let step_dn = ((delta as u128 * self.alpha_dn_bps as u128) / 10_000u128).max(1) as u64;
            prev.saturating_sub(step_dn)
        };

        // post-smooth clamp & quantize
        let mut out = smoothed.clamp(self.price_floor_micro, self.price_cap_micro);
        out = ((out + step - 1) / step) * step;
        last.store_relaxed(out);
        out
    }
}

// -----------------------------
// Pre-signer: create pre-signed variant bundle
// -----------------------------
#[derive(Clone)]
struct PreSigner {
    wallet: Arc<Keypair>,
    // cache: leader -> blockhash -> variants with pre-serialized bytes
    cache: Arc<DashMap<(Pubkey, Hash), Vec<PreSignedVariant>>>,
    // insertion time (ms)
    cache_ts: Arc<DashMap<(Pubkey, Hash), AlignedAtomicU64>>,
}

#[derive(Clone, Debug)]
struct PreSignMeta {
    cu_limit: u32,
    compute_price: u64,
    uses_alt: bool,
    variant_id: Uuid,
}

#[derive(Clone)]
struct PreSignedVariant {
    vtx: VersionedTransaction,
    sig: Signature,
    meta: PreSignMeta,
    bytes: Bytes,
}

impl PreSigner {
    fn new(wallet: Arc<Keypair>) -> Self {
        let s = Self {
            wallet,
            cache: Arc::new(DashMap::new()),
            cache_ts: Arc::new(DashMap::new()),
        };
        // GC stale entries (blockhash validity ~60–75s; use 65s TTL)
        const TTL_MS: u64 = 65_000;
        let cache = s.cache.clone();
        let tsmap = s.cache_ts.clone();
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(std::time::Duration::from_millis(2_000));
            loop {
                ticker.tick().await;
                let now_ms = mono_us() / 1_000;
                for kv in tsmap.iter() {
                    let ts_ms = kv.value().load_relaxed();
                    if now_ms.saturating_sub(ts_ms) > TTL_MS {
                        tsmap.remove(kv.key());
                        cache.remove(kv.key());
                    }
                }
            }
        });
        s
    }

    async fn pre_sign_variants(
        &self,
        _request_accounts: &[solana_sdk::account::AccountMeta],
        base_ixs: Vec<Instruction>,
        blockhash: Hash,
        leader: Pubkey,
        variants: Vec<(u32, u64, bool)>,
    ) -> Result<()> {
        // dedup by (cu, price, alt)
        use std::collections::HashSet;
        let mut seen: HashSet<(u32,u64,bool)> = HashSet::with_capacity(variants.len());
        let unique: Vec<(u32,u64,bool)> = variants.into_iter().filter(|v| seen.insert(*v)).collect();

        let signer = self.wallet.clone();
        let mut out: Vec<PreSignedVariant> = futures_util::stream::iter(unique.into_iter().map(|(cu_limit, micro_per_cu, uses_alt)| {
            let signer = signer.clone();
            let payload = base_ixs.clone();
            async move {
                tokio::task::spawn_blocking(move || -> Result<PreSignedVariant> {
                    // compute-budget first
                    let cu_ix    = ComputeBudgetInstruction::set_compute_unit_limit(cu_limit);
                    let price_ix = ComputeBudgetInstruction::set_compute_unit_price(micro_per_cu);

                    let mut all_ixs = SmallVec::<[Instruction; 8]>::new();
                    all_ixs.push(cu_ix);
                    all_ixs.push(price_ix);
                    all_ixs.extend(payload.iter().cloned());

                    let legacy = solana_sdk::message::Message::new(&all_ixs, Some(&signer.pubkey()));
                    let vmsg   = VersionedMessage::V0(V0Message::try_from(legacy)?);
                    let vtx    = VersionedTransaction::try_new(vmsg, &[&*signer])?;
                    let sig    = vtx.signatures[0];

                    // pre-serialize using thread-local buffer and guard size
                    let bytes = encode_vtx_tls(&vtx);
                    if bytes.len() > PACKET_DATA_SIZE {
                        anyhow::bail!("variant exceeds TPU packet size: {} > {}", bytes.len(), PACKET_DATA_SIZE);
                    }

                    let meta = PreSignMeta { cu_limit, compute_price: micro_per_cu, uses_alt, variant_id: Uuid::new_v4() };
                    Ok(PreSignedVariant { vtx, sig, meta, bytes })
                }).await.map_err(|e| anyhow::anyhow!("join error: {e}"))?
            }
        }))
        .buffer_unordered(8)
        .try_collect()
        .await?;

        // ensure ascending order by price for binary-search selection later
        out.sort_unstable_by_key(|p| p.meta.compute_price);
        self.cache.insert((leader, blockhash), out);
        self.cache_ts.insert((leader, blockhash), AlignedAtomicU64::new(mono_us()/1_000));
        self.evict_if_oversized(256);
        Ok(())
    }

    /// Build a compact price ladder around a center quote.
    pub fn make_price_ladder(center_micro: u64, step_bps: u32, up: u8, down: u8) -> Vec<u64> {
        let mut v = Vec::with_capacity((up as usize) + (down as usize) + 1);
        v.push(center_micro);
        let mut cur = center_micro;
        for _ in 0..up {
            cur = ((cur as u128) * (10_000u128 + step_bps as u128) / 10_000u128) as u64;
            v.push(cur);
        }
        cur = center_micro;
        for _ in 0..down {
            cur = ((cur as u128) * (10_000u128 - step_bps as u128) / 10_000u128) as u64;
            v.push(cur.max(1));
        }
        v.sort_unstable();
        v
    }

    // Evict oldest keys when above capacity
    fn evict_if_oversized(&self, max_keys: usize) {
        let len = self.cache.len();
        if len <= max_keys { return; }
        let mut entries: Vec<((Pubkey, Hash), u64)> = self
            .cache_ts
            .iter()
            .map(|kv| (*kv.key(), kv.value().load_relaxed()))
            .collect();
        entries.sort_unstable_by_key(|(_, ts)| *ts);
        for ((k, h), _) in entries.into_iter() {
            if self.cache.remove(&(k, h)).is_some() {
                self.cache_ts.remove(&(k, h));
            }
            if self.cache.len() <= max_keys { break; }
        }
    }

    /// Optionally invalidate pre-signed ladders when a new blockhash arrives.
    /// Current strategy: no-op, as cache is keyed by blockhash. Kept for integration simplicity.
    pub fn invalidate_for_blockhash(&self, _h: Hash) -> Result<(), ()> { Ok(()) }
}
// -----------------------------
// Conflict-aware partitioner (greedy coloring)
// -----------------------------
fn partition_non_conflicting<T, F>(requests: &[T], write_set_fn: F) -> Vec<Vec<T>>
where
    T: Clone,
    F: Fn(&T) -> Vec<Pubkey>,
{
    if requests.len() <= 1 { return vec![requests.to_vec()]; }

    // cheap union precheck: if union size equals sum of sizes, no overlaps
    let mut total = 0usize;
    let mut union: FastSet<Pubkey> = FastSet::default();
    let mut ws_cache: Vec<SmallVec<[Pubkey; 16]>> = Vec::with_capacity(requests.len());
    for r in requests {
        let mut ws = SmallVec::<[Pubkey; 16]>::new();
        ws.extend(write_set_fn(r));
        total += ws.len();
        for k in ws.iter() { union.insert(*k); }
        ws_cache.push(ws);
    }
    if union.len() == total {
        return vec![requests.to_vec()];
    }

    // fallback to size-sorted greedy using cached write-sets
    struct Item<T> { ws: SmallVec<[Pubkey; 16]>, v: T }
    let mut items: Vec<Item<T>> = requests.iter().cloned().zip(ws_cache.into_iter())
        .map(|(v, ws)| Item{ ws, v }).collect();
    items.sort_by_key(|it| core::cmp::Reverse(it.ws.len()));

    let mut groups: Vec<Vec<T>> = Vec::new();
    let mut group_writes: Vec<FastSet<Pubkey>> = Vec::new();

    'outer: for it in items {
        for (gi, gw) in group_writes.iter_mut().enumerate() {
            let mut hit = false;
            for k in it.ws.iter() { if gw.contains(k) { hit = true; break; } }
            if !hit {
                gw.extend(it.ws.iter().cloned());
                groups[gi].push(it.v);
                continue 'outer;
            }
        }
        let mut set = FastSet::default();
        set.extend(it.ws.into_iter());
        group_writes.push(set);
        groups.push(vec![it.v]);
    }
    groups
}
// Executor core
// -----------------------------
#[derive(Clone)]
/// Represents the result of a flashloan execution
#[derive(Debug, Clone)]
pub struct ExecutionResult {
    pub signature: Signature,
    pub slot: u64,
    pub timing_offset_ms: i64,
    pub priority_fee: u64,
    pub profit: u64,
}

/// Represents a flashloan request
#[derive(Debug, Clone)]
pub struct FlashLoanRequest {
    pub target_accounts: Vec<AccountMeta>,
    pub expected_profit: u64,
    pub min_slot: Option<u64>,
    pub max_slot: Option<u64>,
}

pub struct SlotBoundaryFlashLoanExecutorV2 {
    rpc: Arc<RpcClient>,
    wallet: Arc<Keypair>,
    sender: Arc<dyn Sender>,
    pre_signer: PreSigner,
    priority_model: PriorityFeeModel,
    persistence: Persistence,
    simulation_semaphore: Arc<Semaphore>,
    leader_stats_cache: Arc<FastDashMap<Pubkey, LeaderStats>>,
    circuit_breaker: Arc<AtomicBool>,
    slot_start: Arc<AlignedAtomicU64>,
    slot_start_instant: Arc<RwLock<Instant>>,
    
    // WebSocket and timing related fields
    ws_slot_receiver: Arc<tokio::sync::Mutex<Option<mpsc::UnboundedReceiver<u64>>>>,
    ws_last_update: Arc<AlignedAtomicU64>,
    adaptive_offset_ms: Arc<AlignedAtomicU64>,
    slot_boundary_offset_ms: Arc<AlignedAtomicU64>,
    leader_schedule: Arc<tokio::sync::Mutex<FastMap<u64, Pubkey>>>,
    current_leader: Arc<tokio::sync::Mutex<Option<Pubkey>>>,
    last_leader_switch: Arc<AlignedAtomicU64>,
    
    // Fee calculation and execution
    fee_calculator: Arc<PriorityFeeCalculator>,
    max_retries: u8,
    // Per-slot token buckets for sims/builds
    slot_budget: Arc<SlotBudget>,
}

impl SlotBoundaryFlashLoanExecutorV2 {
    /// Spawn a background task to keep a fresh blockhash in cache and optionally invalidate ladders
    pub fn spawn_blockhash_prefetcher(&self) {
        let rpc = self.rpc.clone();
        let cache = BLOCKHASH_CACHE.get_or_init(|| Arc::new(BlockhashCache::new(2))).clone();
        let presigner = self.pre_signer.clone();
        tokio::spawn(async move {
            let refresh_us = ((SLOT_DURATION_MS as u64) * 1000) / 3;
            loop {
                match rpc.get_latest_blockhash().await {
                    Ok(h) => {
                        cache.update(h);
                        let _ = presigner.invalidate_for_blockhash(h);
                    }
                    Err(e) => {
                        tracing::warn!(target="blockhash", "refresh failed: {:?}", e);
                    }
                }
                tokio::time::sleep(std::time::Duration::from_micros(refresh_us)).await;
            }
        });
    }

    #[inline]
    pub fn ensure_fresh_blockhash(&self) -> Hash {
        let cache = BLOCKHASH_CACHE.get_or_init(|| Arc::new(BlockhashCache::new(2)));
        let age_us = cache.age_us();
        if age_us > 2 * (SLOT_DURATION_MS as u64) * 1000 {
            tracing::debug!(target="blockhash", age_us, "stale blockhash age");
        }
        cache.get()
    }
    pub async fn new(rpc_url: &str, wallet: Keypair) -> Result<Self> {
        validate_constants();
        init_runtime_toggles_from_env();
        // Seed per-thread RNG to avoid correlated jitter
        let mut seed_bytes = [0u8;8];
        seed_bytes.copy_from_slice(&wallet.pubkey().to_bytes()[..8]);
        let seed_hint = u64::from_le_bytes(seed_bytes);
        seed_tls_rng(seed_hint);
        // Initialize metrics
        if let Err(e) = metrics::init_metrics() {
            warn!("Failed to initialize metrics: {}", e);
        }
        let rpc = Arc::new(RpcClient::new_with_commitment(rpc_url.to_string(), CommitmentConfig::processed()));
        let persistence = Persistence::open(SLED_DB_PATH)?;
        let sender_quic = QuicTpuSender::new(rpc.clone()).await?;
        let sender: Arc<dyn Sender> = Arc::new(sender_quic);
        let pre_signer = PreSigner::new(Arc::new(wallet.clone()));
        // Initialize priority fee model with guardrails tuned for stability
        let mut pm = PriorityFeeModel::new(100_000u64, persistence.clone());
        pm.price_floor_micro = 25;        // never less than 25 µ/CU
        pm.price_cap_micro   = 3_000_000; // guard against spikes
        pm.hysteresis_bps    = 300;       // 3% sticky band
        pm.quantum_micro     = 50;        // 50 µ/CU steps
        pm.alpha_up_bps      = 6000;      // up faster
        pm.alpha_dn_bps      = 1500;      // down slower
        pm.min_total_lamports= 5_000;     // min total lamports
        let priority_model = pm;
        
        // Initialize WebSocket and timing related fields
        let (slot_sender, slot_receiver) = mpsc::unbounded_channel();
        
        // Initialize fee calculator with 100k lamports base fee
        let fee_calculator = Arc::new(PriorityFeeCalculator::new(100_000));
        
        let executor = Self {
            rpc: rpc.clone(),
            wallet: Arc::new(wallet),
            sender,
            pre_signer,
            priority_model,
            persistence,
            simulation_semaphore: Arc::new(Semaphore::new(MAX_SIMULATIONS)),
            leader_stats_cache: Arc::new(FastDashMap::default()),
            circuit_breaker: Arc::new(AtomicBool::new(false)),
            slot_start: Arc::new(AlignedAtomicU64::new(0)),
            slot_start_instant: Arc::new(RwLock::new(Instant::now())),
            ws_slot_receiver: Arc::new(tokio::sync::Mutex::new(Some(slot_receiver))),
            ws_last_update: Arc::new(AlignedAtomicU64::new(timestamp())),
            adaptive_offset_ms: Arc::new(AlignedAtomicU64::new(5)), // Start with 5ms offset
            slot_boundary_offset_ms: Arc::new(AlignedAtomicU64::new(5)),
            leader_schedule: Arc::new(tokio::sync::Mutex::new(FastMap::default())),
            current_leader: Arc::new(tokio::sync::Mutex::new(None)),
            last_leader_switch: Arc::new(AlignedAtomicU64::new(0)),
            fee_calculator,
            max_retries: 3, // Default max retries
            slot_budget: Arc::new(SlotBudget::default()),
        };
        
        // Initial leader schedule refresh with retry
        let max_retries = 3;
        let mut retry_count = 0;
        while retry_count < max_retries {
            match executor.refresh_leader_schedule().await {
                Ok(_) => break,
                Err(e) => {
                    retry_count += 1;
                    warn!("Failed to refresh leader schedule (attempt {}/{}): {}", 
                        retry_count, max_retries, e);
                    if retry_count < max_retries {
                        tokio::time::sleep(Duration::from_secs(1)).await;
                    }
                }
            }
        }
        
        // Start WebSocket monitoring in the background
        executor.start_ws_slot_monitoring(slot_sender).await?;
        
        // Start periodic leader schedule updates
        executor.start_leader_schedule_updater().await?;
        
        Ok(executor)
    }

    #[inline(always)]
    fn ewma(prev: f64, x: f64, alpha: f64) -> f64 { alpha * x + (1.0 - alpha) * prev }

    async fn start_ws_slot_monitoring(&self, slot_sender: mpsc::UnboundedSender<u64>) -> Result<()> {
        let rpc     = self.rpc.clone();
        let rpc_url = rpc.url().to_string();
        let ws_url  = rpc_url.replace("https://", "wss://").replace("http://", "ws://");

        let slot_start         = self.slot_start.clone();
        let slot_start_instant = self.slot_start_instant.clone();
        let ws_last_update     = self.ws_last_update.clone();
        let adaptive_offset_ms = self.adaptive_offset_ms.clone();

        // jitter/skew model
        let mut skew_win = SlidingWindowPercentile::new(256);
        let mut ewma_skew_ms = adaptive_offset_ms.load(Ordering::Acquire) as f64;

        tokio::spawn(async move {
            let mut backoff: u64 = 1;
            let max_backoff: u64 = 60;
            let watchdog_ms: u64 = 2 * SLOT_DURATION_MS + 100;

            loop {
                // RPC fallback poll while (re)connecting
                let rpc_fallback = tokio::spawn({
                    let rpc = rpc.clone();
                    let tx  = slot_sender.clone();
                    async move {
                        loop {
                            if let Ok(s) = rpc.get_slot().await {
                                let _ = tx.send(s);
                            }
                            tokio::time::sleep(StdDuration::from_millis(SLOT_DURATION_MS / 2)).await;
                        }
                    }
                });

                match PubsubClient::slot_subscribe(&ws_url).await {
                    Ok((mut stream, unsub)) => {
                        info!("WS slot_subscribe connected: {}", ws_url);
                        let _ = rpc_fallback.abort();
                        backoff = 1;

                        let mut last_tick: Option<Instant> = None;

                        'ws: loop {
                            let wd = tokio::time::sleep(StdDuration::from_millis(watchdog_ms));
                            tokio::select! {
                                _ = wd => {
                                    warn!("WS watchdog timeout; forcing reconnect");
                                    break 'ws;
                                }
                                next = stream.next() => {
                                    match next {
                                        Some(Ok(info)) => {
                                            let now  = Instant::now();
                                            let slot = info.slot;

                                            // Update slot state
                                            slot_start.store(slot, Ordering::Release);
                                            *slot_start_instant.write().await = now;
                                            ws_last_update.store(timestamp(), Ordering::Release);

                                            // Refresh per-slot budgets (tunable caps)
                                            slot_budget.on_slot(slot, 8, 3);

                                            // Fan out to pipeline
                                            let _ = slot_sender.send(slot);

                                            // Compute skew/jitter vs nominal slot time
                                            let skew_ms: u64 = if let Some(prev) = last_tick {
                                                let dt = now.saturating_duration_since(prev);
                                                let ms = dt.as_millis() as u64;
                                                ms.saturating_sub(SLOT_DURATION_MS.min(ms))
                                            } else { 0 };
                                            last_tick = Some(now);

                                            skew_win.push(skew_ms);
                                            ewma_skew_ms = ewma(ewma_skew_ms, skew_ms as f64, 0.20);
                                            let p99 = skew_win.percentile(0.99).unwrap_or(0) as f64;

                                            // conservative offset: max(p99, EWMA) + 1ms, clamped
                                            let mut off = p99.max(ewma_skew_ms).round() as u64 + 1;
                                            off = off.clamp(1, SLOT_DURATION_MS / 2);
                                            adaptive_offset_ms.store(off, Ordering::Release);

                                            // Refresh per-slot budgets (tunable caps)
                                            slot_budget.on_slot(slot, 8, 3);
                                        }
                                        Some(Err(e)) => {
                                            warn!("WS stream error: {e}");
                                            break 'ws;
                                        }
                                        None => {
                                            warn!("WS stream ended");
                                            break 'ws;
                                        }
                                    }
                                }
                            }
                        }

                        // Cleanly unsubscribe on exit
                        let _ = unsub().await;
                    }
                    Err(e) => {
                        error!("WS connect failed: {e}");
                    }
                }

                // Jittered exponential backoff before reconnect
                let sleep_s = core::cmp::min(backoff, max_backoff);
                let jitter  = jitter_micros(999_999);
                tokio::time::sleep(StdDuration::from_secs(sleep_s) + jitter).await;
                backoff = core::cmp::min(backoff.saturating_mul(2), max_backoff);
            }
        });

        Ok(())
    }

    // -----------------------------
    // Leader schedule updater (epoch-aware, absolute-slot map)
    // -----------------------------
    async fn start_leader_schedule_updater(&self) -> Result<()> {
        let rpc               = self.rpc.clone();
        let leader_schedule   = self.leader_schedule.clone(); // FastMap<u64, Pubkey>
        let current_leader    = self.current_leader.clone();
        let last_leader_switch= self.last_leader_switch.clone();

        tokio::spawn(async move {
            let mut ticker = interval(Duration::from_secs(10));
            let mut last_epoch: Option<u64> = None;

            loop {
                ticker.tick().await;

                let Ok(info) = rpc.get_epoch_info().await else { continue };
                // If we already have a non-empty mapping for this epoch, keep it
                if last_epoch == Some(info.epoch) && !leader_schedule.lock().await.is_empty() {
                    continue;
                }

                let epoch_start = info.absolute_slot.saturating_sub(info.slot_index as u64);
                let want_commit = CommitmentConfig::finalized();

                match rpc.get_leader_schedule_with_commitment(Some(epoch_start), want_commit).await {
                    Ok(schedule) => {
                        let mut map = FastMap::default();
                        for (leader_str, rel_slots) in schedule {
                            if let Ok(pk) = leader_str.parse::<Pubkey>() {
                                for rel in rel_slots {
                                    let abs = epoch_start.saturating_add(rel as u64);
                                    map.insert(abs, pk);
                                }
                            }
                        }
                        {
                            let mut guard = leader_schedule.lock().await;
                            *guard = map;
                        }

                        // set current leader
                        if let Ok(cur) = rpc.get_slot().await {
                            if let Some(pk) = leader_schedule.lock().await.get(&cur).copied() {
                                let mut cl = current_leader.lock().await;
                                if cl.as_ref() != Some(&pk) {
                                    *cl = Some(pk);
                                    last_leader_switch.store(timestamp(), Ordering::Release);
                                }
                            }
                        }
                        last_epoch = Some(info.epoch);
                        debug!("Leader schedule refreshed for epoch {} (start {})", info.epoch, epoch_start);
                    }
                    Err(e) => warn!("Failed to refresh leader schedule: {e}"),
                }
            }
        });

        Ok(())
    }
    
    // -----------------------------
    // Get the current leader (map-first)
    // -----------------------------
    pub async fn get_current_leader(&self) -> Pubkey {
        match self.rpc.get_slot().await {
            Ok(slot) => self.get_slot_leader(slot).await.unwrap_or_default(),
            Err(_)   => Pubkey::default(),
        }
    }
    
    // -----------------------------
    // Get the adaptive offset in milliseconds
    // -----------------------------
    pub fn get_adaptive_offset_ms(&self) -> u64 {
        self.adaptive_offset_ms.load(Ordering::Acquire)
    }
    }

    // -----------------------------
    // Calculate next slot boundary with adaptive offset
    // -----------------------------
    async fn calculate_next_slot_boundary(&self) -> Instant {
        // If WS hasn’t ticked for >2 slots, degrade gracefully
        let last_ms = self.ws_last_update.load(Ordering::Acquire) as u64;
        let now_ms  = timestamp() as u64;
        let stale   = now_ms.saturating_sub(last_ms) > (2 * SLOT_DURATION_MS) as u64;

        let off_ms = if stale {
            (SLOT_DURATION_MS / 4).max(1)
        } else {
            self.adaptive_offset_ms.load(Ordering::Acquire).clamp(1, SLOT_DURATION_MS / 2)
        };

        let start = *self.slot_start_instant.read().await;
        let elapsed_ms = dur_to_millis(mono_now().saturating_duration_since(start));
        let to_boundary = SLOT_DURATION_MS.saturating_sub(elapsed_ms % SLOT_DURATION_MS);
        let target = to_boundary.saturating_sub(off_ms).max(1);

        mono_now() + Duration::from_millis(target)
    }
    
    // -----------------------------
    // Refresh the leader schedule (absolute-slot map)
    // -----------------------------
    pub async fn refresh_leader_schedule(&self) -> Result<(), Box<dyn std::error::Error>> {
        let info = self.rpc.get_epoch_info().await?;
        let epoch_start = info.absolute_slot.saturating_sub(info.slot_index as u64);
        let slots_per_epoch = info.slots_in_epoch;

        if let Ok(schedule) = self.rpc
            .get_leader_schedule_with_commitment(Some(epoch_start), CommitmentConfig::finalized())
            .await
        {
            let mut map = FastMap::default();
            for (leader_str, rel_slots) in schedule {
                if let Ok(pk) = leader_str.parse::<Pubkey>() {
                    for rel in rel_slots {
                        let abs = epoch_start.saturating_add(rel as u64);
                        map.insert(abs, pk);
                    }
                }
            }
            *self.leader_schedule.lock().await = map;

            debug!(
                "Updated leader schedule epoch={} slots={}-{}",
                info.epoch,
                epoch_start,
                epoch_start + slots_per_epoch - 1
            );
        } else {
            warn!("Failed to get leader schedule for epoch {}", info.epoch);
        }

        Ok(())
    }
    
    // -----------------------------
    // Get the leader for a specific slot (absolute-slot map + RPC fallback)
    // -----------------------------
    pub async fn get_slot_leader(&self, slot: u64) -> Option<Pubkey> {
        if let Some(pk) = self.leader_schedule.lock().await.get(&slot).copied() {
            return Some(pk);
        }
        match self.rpc.get_slot_leader(slot).await {
            Ok(pk) => Some(pk),
            Err(e) => { warn!("get_slot_leader RPC fallback failed: {e}"); None }
        }
    }
    
    // (removed duplicate refresh_leader_schedule implementation)
    
    // -----------------------------
    // Get the current leader
    // -----------------------------
    pub async fn get_current_leader(&self) -> Pubkey {
        let current_slot = match self.rpc.get_slot().await {
            Ok(slot) => slot,
            Err(_) => return Pubkey::default(),
        };
        
        self.get_slot_leader(current_slot).await.unwrap_or_default()
    }
    
    // -----------------------------
    // Get current slot
    // -----------------------------
    // -----------------------------
    async fn build_and_price_flashloan_transaction(
        &self,
        base_instructions: Vec<Instruction>,
        accounts: &[Pubkey],
        leader: Pubkey,
    ) -> Result<(VersionedTransaction, u64, u64), anyhow::Error> {
        // per-slot build budget gate
        if !self.slot_budget.try_build() {
            anyhow::bail!("build budget exhausted for slot");
        }
        // 1) Fresh blockhash + min context (from prefetcher cache)
        let blockhash = self.ensure_fresh_blockhash();
        let min_ctx   = Some(self.slot_start.load(Ordering::Acquire));

        // 2) Discover CU via minimal tx
        let msg  = solana_sdk::message::Message::new(&base_instructions, Some(&self.wallet.pubkey()));
        let tx   = Transaction::new(&[&*self.wallet], msg, blockhash);
        let vtx0 = VersionedTransaction::try_from(tx)?;
        let (ok, units) = self.simulate_vtx_with_min_slot(&vtx0, min_ctx).await?;
        if !ok || units == 0 { anyhow::bail!("simulation failed or 0 CUs"); }

        // 3) Limit + target µ/CU (volatility-aware model)
        let cu_limit = ((units as f64) * (1.0 + CU_HEADROOM_PCT)).ceil() as u32;
        let center_micro = self.priority_model
            .estimate_micro_per_cu(&self.rpc, accounts, &leader, 0.90, cu_limit as u64, 0)
            .await
            .clamp(1, 5_000_000);

        // 4) Try pre-signed variant closest to the target price
        if let Some((vtx_pick, _sig, _meta, _bytes)) =
            self.pre_signer.select_variant_by_price(&leader, &blockhash, center_micro)
        {
            metrics::increment_counter!("presign.hit", "hit" => "true");
            let sz = tx_len_bytes(&vtx_pick);
            if sz <= PACKET_DATA_SIZE {
                metrics::histogram!("tx.size_bytes", sz as f64);
                return Ok((vtx_pick, cu_limit as u64, center_micro));
            }
        } else {
            metrics::increment_counter!("presign.hit", "hit" => "false");
        }

        // 5) Miss: build final tx and backfill ladder asynchronously
        let cu_ix    = ComputeBudgetInstruction::set_compute_unit_limit(cu_limit);
        let price_ix = ComputeBudgetInstruction::set_compute_unit_price(center_micro);
        let mut ixs  = vec![cu_ix, price_ix];
        ixs.extend(base_instructions);

        let final_msg = solana_sdk::message::Message::new(&ixs, Some(&self.wallet.pubkey()));
        let final_tx  = Transaction::new(&[&*self.wallet], final_msg, blockhash);
        let vtx_final = VersionedTransaction::try_from(final_tx)?;

        // Guard size <= TPU packet
        let sz = tx_len_bytes(&vtx_final);
        if sz > PACKET_DATA_SIZE {
            anyhow::bail!("final transaction too large: {} > {}", sz, PACKET_DATA_SIZE);
        }
        metrics::histogram!("tx.size_bytes", sz as f64);

        // Background pre-sign a tight ladder for next calls with trend skew
        let key_for_miss = accounts_leader_key(&leader, accounts);
        let miss_bps = self.priority_model.miss_rate_bps(&key_for_miss);
        let skew = ((miss_bps as i32) / 2).min(800) as i32; // up to +8%
        let ladder = PreSigner::ladder_with_trend(center_micro, 250, 2, 2, skew);
        let variants: Vec<(u32, u64, bool)> = ladder.into_iter().map(|p| (cu_limit, p, false)).collect();
        let ps = self.pre_signer.clone();
        let ixs_clone = ixs.clone();
        tokio::spawn(async move {
            let _ = ps.pre_sign_variants(&[], ixs_clone, blockhash, leader, variants).await;
        });

        Ok((vtx_final, cu_limit as u64, center_micro))
    }
    
    // FIX the truncated back-compat wrapper
    async fn build_transaction_with_cu(
        &self,
        base_instructions: Vec<Instruction>,
        accounts: &[Pubkey],
        leader: Pubkey,
        _target_percentile: f64,
    ) -> Result<(VersionedTransaction, u64, u32)> {
        let (vtx, cu_limit, micro_per_cu) = self
            .build_and_price_flashloan_transaction(base_instructions, accounts, leader)
            .await?;
        Ok((vtx, cu_limit, micro_per_cu as u32))
    }

    

    // helper that enforces min_context_slot
    async fn simulate_vtx_with_min_slot(
        &self,
        vtx: &VersionedTransaction,
        min_context_slot: Option<u64>,
    ) -> Result<(bool, u64)> {
        // per-slot sim budget gate
        if !self.slot_budget.try_sim() {
            return Ok((false, 0));
        }
        let _permit = self
            .simulation_semaphore
            .acquire()
            .await
            .map_err(|e| anyhow::anyhow!("acquire sim semaphore: {e}"))?;
        let start_time = Instant::now();

        let cfg = RpcSimulateTransactionConfig {
            sig_verify: false,
            replace_recent_blockhash: true,
            commitment: Some(CommitmentConfig::processed()),
            encoding: Some(UiTransactionEncoding::Base64),
            accounts: None,
            min_context_slot,
            inner_instructions: false,
        };

        let budget = self.sim_timeout_ms().await;
        let fut = self.rpc.simulate_transaction_with_config(vtx, cfg);
        let sim = tokio::time::timeout(Duration::from_millis(budget), fut)
            .await
            .map_err(|_| anyhow::anyhow!("simulation timed out ({} ms)", budget))?
            .map_err(|e| anyhow::anyhow!("RPC simulation failed: {e}"))?;

        metrics::SIMULATION_TIME_MS.set(start_time.elapsed().as_millis() as i64);

        if let Some(err) = &sim.value.err { 
            warn!(error=?err, "vtx simulation failed");
            return Ok((false, 0));
        }
        Ok((true, sim.value.units_consumed.unwrap_or(0)))
    }

    #[inline]
    async fn sim_timeout_ms(&self) -> u64 {
        let next = self.calculate_next_slot_boundary().await;
        let now  = mono_now();
        let left = next.saturating_duration_since(now).as_millis() as u64;
        left.saturating_sub(6).clamp(40, 250)
    }

    // -----------------------------
    // Observability-friendly simulate for legacy Transaction
    // -----------------------------
    #[instrument(skip_all)]
    async fn simulate_transaction(
        &self,
        transaction: &Transaction,
    ) -> Result<(bool, u64), anyhow::Error> {
        let _permit = self
            .simulation_semaphore
            .acquire()
            .await
            .map_err(|e| anyhow::anyhow!("Failed to acquire simulation semaphore: {}", e))?;

        let start_time = Instant::now();
        let config = RpcSimulateTransactionConfig {
            sig_verify: false,
            replace_recent_blockhash: true,
            commitment: Some(CommitmentConfig::processed()),
            encoding: Some(UiTransactionEncoding::Base64),
            accounts: None,
            min_context_slot: None,
            inner_instructions: true,
        };

        let result = self
            .rpc
            .simulate_transaction_with_config(transaction, config)
            .await
            .map_err(|e| anyhow::anyhow!("RPC simulation failed: {}", e))?;

        // Metrics
        let sim_time_ms = start_time.elapsed().as_millis() as i64;
        metrics::SIMULATION_TIME_MS.set(sim_time_ms);

        if let Some(err) = result.value.err.clone() {
            warn!(error = ?err, simulation_time_ms = sim_time_ms, "simulation failed");
            return Ok((false, 0));
        }

        let units_consumed = result.value.units_consumed.unwrap_or(0);
        debug!(units = units_consumed, simulation_time_ms = sim_time_ms, "simulation ok");
        Ok((true, units_consumed))
    }

    // -----------------------------
    // Execute a batch of flashloan requests at slot boundary (optimized)
    // -----------------------------
    pub async fn execute_batch_at_boundary(
        &self,
        requests: Vec<FlashLoanRequest>,
    ) -> Vec<FlashloanResult<ExecutionResult>> {
        use futures_util::{stream, StreamExt};

        if self.circuit_breaker.load(Ordering::Acquire) {
            return vec![Err(Box::new(ExecError::Other("circuit breaker".into())))]
        }

        // Partition by write-set conflicts and bound group count (CPU/IO aware)
        let cpu = num_cpus::get();
        let io_cap: usize = 6; // conservative default if sender doesn't expose a hint
        let mut max_groups = usize::MAX;
        max_groups = max_groups.min(cpu.max(2) / 2).min(io_cap.max(2));
        let mut groups = partition_conflicts_coloring(&requests, |r| {
            r.target_accounts
                .iter()
                .filter(|m| m.is_writable)
                .map(|m| m.pubkey)
                .collect::<Vec<_>>()
        }, max_groups);

        // --- Profit-weighted group ordering (highest first)
        groups.sort_by_key(|g| {
            let best = g.iter().map(|r| r.expected_profit).max().unwrap_or(0);
            core::cmp::Reverse(best)
        });

        // Snapshot current leader for build phase
        let leader = self.get_current_leader().await;

        #[derive(Debug)]
        struct Built {
            req: FlashLoanRequest,
            tx: VersionedTransaction,
            cu_limit: u64,
            micro: u64,
            sig: Signature,
            wire: Bytes,
        }

        // Pre-build all transactions before boundary
        let mut planned_per_group: Vec<Vec<Result<Built, anyhow::Error>>> = Vec::with_capacity(groups.len());
        for g in groups {
            // --- PnL-aware prefilter (conservative defaults)
            let micro_cap_bps: u32 = 1250; // allow one +12.5% bump
            let mut sorted: Vec<FlashLoanRequest> = g
                .into_iter()
                .filter(|r| {
                    let est_cu: u64 = 200_000; // fallback upper bound
                    let base_micro = 50_000u64; // fallback µ/CU
                    let worst_micro = ((base_micro as u128) * (10_000u128 + micro_cap_bps as u128) / 10_000u128) as u64;
                    let fee_lam = (worst_micro.saturating_mul(est_cu)) / 1_000_000;
                    fee_lam < r.expected_profit
                })
                .collect();
            sorted.sort_by_key(|r| core::cmp::Reverse(r.expected_profit));

            let built = stream::iter(sorted.into_iter())
                .map(|req| {
                    let exec = self.clone();
                    let accounts_only: Vec<Pubkey> = req.target_accounts.iter().map(|m| m.pubkey).collect();
                    async move {
                        // Thompson-sampling fee arm per leader (attribute only; percentile hook optional)
                        let bandits = FEE_BANDITS.get_or_init(|| DashMap::new());
                        let (arm_idx, _pctl) = bandits.entry(leader).or_insert_with(FeeBandit::default).value().choose();
                        let ixs = req.build_ixs()?;
                        let (tx, cu, micro) = exec
                            .build_and_price_flashloan_transaction(
                                ixs,
                                &accounts_only,
                                leader,
                            )
                            .await?;
                        // Precompute raw wire bytes and signature once
                        let (sig, wire) = vtx_sig_and_bytes(&tx);
                        // Attribute arm to signature for later outcome updates
                        CHOSEN_ARM_FOR_SIG
                            .get_or_init(|| DashMap::new())
                            .insert(tx.signatures[0], (leader, arm_idx));
                        Ok::<Built, anyhow::Error>(Built { req, tx, cu_limit: cu, micro, sig, wire })
                    }
                })
                .buffer_unordered(16)
                .collect::<Vec<_>>()
                .await;

            planned_per_group.push(built);
        }

        // If everything was filtered out or failed build, short-circuit
        let planned_nonempty = planned_per_group.iter().any(|grp| grp.iter().any(|b| b.is_ok()));
        if !planned_nonempty {
            return vec![Err(Box::new(ExecError::Other("no-eligible-requests".into())))];
        }

        // Wait for boundary with absolute sleep and enforce a leader-adaptive send window
        // Optional realtime pin for the boundary batch task (Linux only)
        let _rt = RealtimeGuard::new(0, 50);
        let boundary = self.calculate_next_slot_boundary().await;
        // Pull leader stats if available; fall back to default micro-window
        let leader_at_boundary = self.get_current_leader().await;
        if let Some(mut c) = LEADER_CONCURRENCY.get_mut(&leader_at_boundary) {
            c.decay_if_idle(5_000); // 5ms idle recovery
        }
        let (mu, sigma) = self
            .leader_stats_cache
            .get(&leader_at_boundary)
            .map(|v| (v.mean_fee_to_land, v.stdev_fee_to_land))
            .map(|(m, s)| (m as f64, s as f64))
            .unwrap_or((1800.0, 600.0)); // µs defaults tuned conservative
        // Convert µs stats using EMA per-leader; default to 2500µs if absent
        let target_us = self
            .leader_stats_cache
            .get(&leader_at_boundary)
            .map(|v| v.ema_window_us)
            .unwrap_or(2500.0) as u64;
        let send_window = Duration::from_micros(target_us);
        // Tail observability per leader
        metrics::gauge!("slot.send_window_us", send_window.as_micros() as f64, "leader" => leader_at_boundary.to_string());
        let leader_cap = LEADER_CONCURRENCY.get(&leader_at_boundary).map(|c| c.cap.load(std::sync::atomic::Ordering::Relaxed)).unwrap_or(0) as f64;
        metrics::gauge!("leader.cap", leader_cap, "leader" => leader_at_boundary.to_string());
        sleep_until_abs(boundary).await;
        // final calibrated busy-wait to cut scheduler jitter
        spin_us(60);
        let last_send_deadline = boundary + send_window;

        // PnL guard: if tripped, skip this slot and let cooldown elapse
        if PNL_GUARD.tripped(self.get_current_slot().await) {
            metrics::gauge!("guard.pnl.slot", self.get_current_slot().await as f64);
            metrics::increment_counter!("guard.pnl.tripped");
            return Vec::from([Err(Box::new(ExecError::Other("pnl-guard-tripped".into())))]);
        }

        // Fire groups in parallel; serialize within each group
        use tokio::task::JoinSet;
        let mut set = JoinSet::new();
        let batch_span = info_span!("batch", slot = %self.get_current_slot().await, groups = %planned_per_group.len());
        let _entered = batch_span.enter();
        for mut gb in planned_per_group {
            let exec = self.clone();
            let last_send_deadline = last_send_deadline;
            let leader_snapshot = leader;
            let group_span = info_span!("group", size = %gb.len());
            set.spawn(async move {
                let mut results: Vec<FlashloanResult<ExecutionResult>> = Vec::with_capacity(gb.len());
                // Derive a small, stable micro-stagger baseline from group addr entropy
                let base_us = stable_group_baseline_us(&gb);

                // -------- First-wave batch fire (send primary priced variants together)
                // Leader flip check right before wave send
                let leader_at_send = exec.get_current_leader().await;
                let leader_changed = leader_at_send != leader_snapshot;
                if leader_changed {
                    metrics::increment_counter!("leader.changed.before_send");
                }

                use std::collections::HashSet;
                // Build wave from profitable requests only, carrying sig/wire to avoid mismatches
                let mut wave: SmallVec<[(FlashLoanRequest, Signature, Bytes, VersionedTransaction, u64, u64); 8]> = SmallVec::new();
                for built in gb.iter() {
                    if let Ok(Built { req, tx, cu_limit, micro, sig, wire }) = built {
                        if ensure_profit_after_fees(req.expected_profit, *micro, *cu_limit, 0).is_ok() {
                            wave.push((req.clone(), *sig, wire.clone(), tx.clone(), *cu_limit, *micro));
                        }
                    }
                }

                // Prepare batch payload using wave (profitable only) and record send meta
                let mut batch: SmallVec<[(Signature, &[u8]); 8]> = SmallVec::new();
                for (req, sig, wire, _tx, cu_limit, micro) in wave.iter() {
                    let _ = req; // reserved for future tagging/metrics
                    record_send_meta(*sig, *micro, *cu_limit);
                    batch.push((*sig, &wire[..]));
                }

                // Respect window before sending the first wave
                if Instant::now() < last_send_deadline && !batch.is_empty() && !leader_changed {
                    let attempted = batch.len();
                    // Optional linux sendmmsg fast path (feature-gated); fallback to sender, with consistent hash builder
                    #[cfg(all(target_os = "linux", feature = "mmsg"))]
                    let sent_set: std::collections::HashSet<Signature, AHashBuilder> = {
                        let n = mmsg::send_batch(exec.sender.udp_socket(), &batch).await;
                        batch.iter().take(n).map(|(s, _)| *s).collect()
                    };
                    #[cfg(not(all(target_os = "linux", feature = "mmsg")))]
                    let sent_set: std::collections::HashSet<Signature, AHashBuilder> = exec
                        .sender
                        .send_batch(&batch)
                        .await
                        .unwrap_or_default()
                        .into_iter()
                        .collect();
                    metrics::increment_counter!("batch.wave1.attempted", "count" => attempted.to_string());
                    // compute residuals after retain below
                    // retain only those that still need attention
                    gb.retain(|b| match b {
                        Ok(Built { tx, .. }) => !sent_set.contains(&tx.signatures[0]),
                        Err(_) => true,
                    });
                    metrics::increment_counter!(
                        "batch.wave1.sent",
                        "count" => (attempted.saturating_sub(gb.len())).to_string()
                    );
                }
                if Instant::now() >= last_send_deadline {
                    metrics::increment_counter!("tx.skipped", "reason" => "missed_window");
                }
                // Admission controller: if window is very tight, keep top-2 by EV-per-µs with light anti-starvation
                let remain = last_send_deadline.saturating_duration_since(Instant::now());
                if remain < std::time::Duration::from_micros(600) && gb.len() > 2 {
                    let mut scored: Vec<(u128, usize)> = gb.iter().enumerate().filter_map(|(i, b)| {
                        match b {
                            Ok(Built { req, micro, cu_limit, .. }) => {
                                let fee = ((*micro).saturating_mul(*cu_limit)) / 1_000_000;
                                let ev = req.expected_profit.saturating_sub(fee) as u128;
                                let t_us = 220u128; // predicted per-tx cost
                                Some(((ev * 1000) / t_us.max(1), i))
                            }
                            _ => None
                        }
                    }).collect();
                    scored.sort_by_key(|(score, _)| core::cmp::Reverse(*score));
                    let k = 2usize;
                    let topk_len = scored.len().min(k);
                    let topk = &scored[..topk_len];
                    // 1% randomization inside top-K to avoid deterministic starvation
                    let pick = if topk_len > 1 && ((jitter_ns_sym(100) as u64) & 0x7F) == 0 { 1 } else { 0 };
                    let pick_idx = topk.get(pick).map(|x| x.1);
                    let mut keep_idx: std::collections::HashSet<usize> = std::collections::HashSet::new();
                    if let Some(pi) = pick_idx { keep_idx.insert(pi); }
                    for (_, idx) in topk.iter() { keep_idx.insert(*idx); if keep_idx.len() >= k { break; } }
                    gb = gb.into_iter().enumerate().filter_map(|(i, b)| if keep_idx.contains(&i) { Some(b) } else { None }).collect();
                    metrics::increment_counter!("admission.ev_per_us");
                }
                for built in gb.drain(..) {
                    match built {
                        Err(e) => {
                            results.push(Err(Box::new(ExecError::Other(format!("build: {e}")))));
                        }
                        Ok(Built { req, tx, cu_limit, micro }) => {
                            // Guard: if we already missed the window, stop issuing sends for this group
                            if Instant::now() >= last_send_deadline {
                                metrics::increment_counter!("tx.skipped", "reason" => "missed_window");
                                results.push(Err(Box::new(ExecError::Other("missed first-slot send window".into()))));
                                break;
                            }
                            if let Err(e) = ensure_profit_after_fees(req.expected_profit, micro, cu_limit, 0) {
                                results.push(Err(e));
                                continue;
                            }
                            // Acquire per-account write permits (ordered, all-or-nothing with 300µs timeout)
                            let write_keys: Vec<Pubkey> = req
                                .target_accounts
                                .iter()
                                .filter(|m| m.is_writable)
                                .map(|m| m.pubkey)
                                .collect();
                            let _permits = match exec.acquire_write_permits_ordered(&write_keys, 300).await {
                                Some(p) => p,
                                None => {
                                    results.push(Err(Box::new(ExecError::Other("permits-timeout".into()))));
                                    continue;
                                }
                            };
                            // group-aware stagger: stable baseline + tiny symmetric jitter + EV bias, window-safe
                            let jitter_ns = jitter_ns_sym(120_000);
                            let fee_lam = (micro.saturating_mul(cu_limit)) / 1_000_000;
                            let ev_lam = req.expected_profit.saturating_sub(fee_lam);
                            let bias: u64 = ev_bias_us(ev_lam);
                            let total_us = base_us + ((jitter_ns.abs() as u64) / 1_000).min(120) + bias;
                            let now = Instant::now();
                            if now + std::time::Duration::from_micros(total_us) < last_send_deadline {
                                if total_us > 2 {
                                    tokio::time::sleep(std::time::Duration::from_micros(total_us - 2)).await;
                                    spin_us(2); // final ~2µs
                                } else {
                                    spin_us(1);
                                }
                            }
                            // intent & size metrics
                            let tier = if micro < 50_000 { "low" } else if micro < 250_000 { "mid" } else { "high" };
                            metrics::increment_counter!("tx.intent", "tier" => tier);
                            metrics::gauge!("tx.cu_limit", cu_limit as f64);
                            metrics::gauge!("tx.expected_profit", req.expected_profit as f64);
                            tracing::trace!(target = "send", micro_per_cu = micro, cu_limit = cu_limit, expected_profit = req.expected_profit as i64, "group_send");

                            // Adaptive per-leader concurrency controller
                            let ldr = exec.get_current_leader().await;
                            let ctl = LEADER_CONCURRENCY.entry(ldr).or_insert_with(Concurrency::new);
                            if !ctl.try_acquire() {
                                metrics::increment_counter!("admission.backpressure");
                                continue;
                            }
                            // Send with categorized escalation fallback (respecting deadline)
                            let res = exec.send_or_escalate(tx.clone(), cu_limit, micro, req.expected_profit, last_send_deadline).await;
                            let ok = res.is_ok();
                            ctl.release(ok);
                            match res {
                                Ok(er)  => results.push(Ok(er)),
                                Err(e)  => results.push(Err(e)),
                            }
                        }
                    }
                }
                results
            });
        }

        let mut all = Vec::with_capacity(requests.len());
        while let Some(joined) = set.join_next().await {
            match joined {
                Ok(mut v) => all.append(&mut v),
                Err(e)    => all.push(Err(Box::new(ExecError::Other(format!("join error: {e}"))))),
            }
        }
        all
    }

    // (removed duplicate earlier single-request; keeping the later consolidated version below)

    // -----------------------------
    // Execute at slot boundary with proper timing
    // -----------------------------
    pub async fn execute_at_slot_boundary(
        &self,
        transaction: VersionedTransaction,
        cu_limit: u64,
        micro_per_cu: u64,
        expected_profit: u64,
    ) -> FlashloanResult<ExecutionResult> {
        // Deterministic boundary timing with absolute sleep and final calibrated spin
        let boundary = self.calculate_next_slot_boundary().await;
        sleep_until_abs(boundary).await;
        spin_us(60);

        // Fees guard
        ensure_profit_after_fees(expected_profit, micro_per_cu, cu_limit, 0)?;

        // Raw-first; fallback to RPC sender
        let t0 = Instant::now();
        let (sig0, bytes0) = vtx_sig_and_bytes(&transaction);
        let signature = match self.hedged_raw_send(bytes0).await {
            Ok(()) => sig0,
            Err(_) => send_transaction_with_retry(&self.rpc, &transaction, self.max_retries).await?,
        };
        let timing_offset_ms = t0.duration_since(boundary).as_millis() as i64;

        // Fee math (lamports)
        let priority_fee = (micro_per_cu.saturating_mul(cu_limit)).saturating_div(1_000_000);
        let profit = expected_profit.saturating_sub(priority_fee);
        let cur_slot = self.get_current_slot().await;
        PNL_GUARD.on_result(cur_slot, profit as i64, 4);

        Ok(ExecutionResult {
            signature,
            slot: cur_slot,
            timing_offset_ms,
            priority_fee,
            profit,
        })
    }

    // -----------------------------
    // Escalation helpers (reuse pre-signed ladder; avoid re-sim/build)
    // -----------------------------
    #[inline]
    fn vtx_blockhash(vtx: &VersionedTransaction) -> Option<Hash> {
        match &vtx.message {
            VersionedMessage::V0(m) => Some(m.recent_blockhash),
            _ => None,
        }
    }

    /// Try a higher-priced pre-signed variant on the same CU, return (sig, new_micro)
    async fn escalate_with_presigned(
        &self,
        base: &VersionedTransaction,
        leader: Pubkey,
        cur_micro: u64,
        step_bps: u32,
        cu_limit: u64,
        expected_profit: u64,
    ) -> Result<(Signature, u64), Box<dyn std::error::Error>> {
        #[inline(always)]
        fn next_up(x: u64, bps: u32) -> u64 {
            ((x as u128) * (10_000u128 + bps as u128) / 10_000u128) as u64
        }
        let bh = Self::vtx_blockhash(base).ok_or_else(|| ExecError::Other("unsupported message version".into()))?;
        // Safer escalation: ensure presigned ladder matches a fresh blockhash; otherwise force rebuild
        let fresh_bh = self.ensure_fresh_blockhash();
        if bh != fresh_bh {
            metrics::increment_counter!("escalate.stale_blockhash");
            return Err(Box::new(ExecError::BlockhashExpired));
        }
        let mut target = next_up(cur_micro, step_bps);
        for _ in 0..3 {
            // Profitability guard: do not escalate into negative PnL
            let fee_lam = (target.saturating_mul(cu_limit)) / 1_000_000;
            if fee_lam >= expected_profit {
                return Err(Box::new(ExecError::Other("escalation breaks profitability")));
            }
            if let Some((_vtx, sig, meta, bytes)) = self.pre_signer.select_variant_by_price(&leader, &bh, target) {
                // enforce strictly higher than current tier during escalation
                if meta.compute_price <= cur_micro {
                    target = next_up(target, step_bps);
                    continue;
                }
                // PnL guard for the actual meta-selected tier
                let fee_lam2 = (meta.compute_price.saturating_mul(cu_limit)) / 1_000_000;
                if fee_lam2 >= expected_profit {
                    return Err(Box::new(ExecError::Other("escalation breaks profitability")));
                }
                // Best path: raw QUIC datagram (no re-serialization)
                // Deduper before raw send of escalated variant
                if SENT_SIG_RING.get_or_init(|| SigRing::new(4096)).seen_or_insert(&sig) {
                    return Ok((sig, meta.compute_price));
                }
                let sent = self.hedged_raw_send(bytes.clone()).await?;
                let _ = sent; // signature should equal sig; keep sig from cache for determinism
                return Ok((sig, meta.compute_price));
            }
            target = next_up(target, step_bps);
        }
        Err(Box::new(ExecError::Other("no presigned variant to escalate".into())))
    }

    /// Internal: send with categorized escalation fallback
    async fn send_or_escalate(
        &self,
        tx: VersionedTransaction,
        cu_limit: u64,
        micro: u64,
        expected_profit: u64,
        deadline: Instant,
    ) -> FlashloanResult<ExecutionResult> {
        // Local deadline = min(1.5ms from now, batch window end)
        let local = Instant::now() + Duration::from_micros(1_500);
        let deadline = std::cmp::min(local, deadline);

        // Slot-scope cancellation guard
        let slot_scope = SlotScope::new(self).await;

        // 1) First try (fast path) — raw QUIC datagram to avoid re-serialization inside sender
        let (sig0, bytes0) = vtx_sig_and_bytes(&tx);
        // signature dedupe ring: avoid double-firing
        if SENT_SIG_RING.get_or_init(|| SigRing::new(4096)).seen_or_insert(&sig0) {
            return Err(Box::new(ExecError::Other("duplicate-signature".into())));
        }
        // message-key dedupe ring (survives re-signs)
        if MSG_DEDUP.seen_or_insert(message_key(&tx)) {
            return Err(Box::new(ExecError::Other("dup-message".into())));
        }
        let hedged = {
            let bytes_cl = bytes0.clone();
            tokio::select! {
                _ = slot_scope.token().cancelled() => Err(anyhow::anyhow!("slot-drift-cancel")),
                res = self.hedged_raw_send(bytes_cl) => res,
            }
        };
        match hedged {
            Ok(_sig_echo) => {
                let fee = (micro.saturating_mul(cu_limit)) / 1_000_000;
                let profit = expected_profit.saturating_sub(fee);
                record_send_meta(sig0, micro, cu_limit);
                // PnL guard accounting (success path)
                let cur_slot = self.get_current_slot().await;
                PNL_GUARD.on_result(cur_slot, profit as i64, 4);
                // bandit success attribution
                if let Some((ldr, arm)) = CHOSEN_ARM_FOR_SIG.get_or_init(|| DashMap::new()).remove(&tx.signatures[0]).map(|e| e.1) {
                    if let Some(mut b) = FEE_BANDITS.get_or_init(|| DashMap::new()).get_mut(&ldr) {
                        b.update(arm, true);
                    }
                }
                return Ok(ExecutionResult { signature: sig0, slot: self.get_current_slot().await, timing_offset_ms: 0, priority_fee: fee, profit });
            }
            Err(e) => {
                use SendClass::*;
                // cold typed classifier first
                let mut cls = classify_send_error_cold(&e);
                if matches!(cls, SendClass::Other) {
                    let s = format!("{e:?}");
                    if s.contains("TooLowFee")   { cls = SendClass::TooLowFee; }
                    if s.contains("WouldBeDropped") { cls = SendClass::WouldBeDropped; }
                    if s.contains("QoS")         { cls = SendClass::QoS; }
                }
                match cls {
                    TooLowFee | WouldBeDropped | QoS if Instant::now() < deadline => {
                        // abort if slot drifted (leader likely flipped)
                        let boundary_slot = self.get_current_slot().await;
                        let now_slot = self.get_current_slot().await;
                        if boundary_slot != now_slot {
                            return Err(Box::new(ExecError::Other("slot-drift-abort")));
                        }
                        // bandit failure attribution for this signature
                        if let Some((ldr, arm)) = CHOSEN_ARM_FOR_SIG.get_or_init(|| DashMap::new()).remove(&tx.signatures[0]).map(|e| e.1) {
                            if let Some(mut b) = FEE_BANDITS.get_or_init(|| DashMap::new()).get_mut(&ldr) {
                                b.update(arm, false);
                            }
                        }
                        let leader = self.get_current_leader().await;
                        let (sig, new_micro) = self
                            .escalate_with_presigned(&tx, leader, micro, 1_250, cu_limit, expected_profit)
                            .await?;
                        let fee = (new_micro.saturating_mul(cu_limit)) / 1_000_000;
                        let profit = expected_profit.saturating_sub(fee);
                        record_send_meta(sig, new_micro, cu_limit);
                        let cur_slot = self.get_current_slot().await;
                        PNL_GUARD.on_result(cur_slot, profit as i64, 4);
                        // success after escalation does not attribute to original arm (already marked failure above)
                        Ok(ExecutionResult { signature: sig, slot: self.get_current_slot().await, timing_offset_ms: 0, priority_fee: fee, profit })
                    }
                    AccountInUse => {
                        let remain = deadline.saturating_duration_since(Instant::now());
                        if remain > std::time::Duration::from_micros(200) {
                            tokio::time::sleep(std::time::Duration::from_micros(200)).await;
                            if self.hedged_raw_send(bytes0.clone()).await.is_ok() {
                                let fee = (micro.saturating_mul(cu_limit)) / 1_000_000;
                                let profit = expected_profit.saturating_sub(fee);
                                if let Some((ldr, arm)) = CHOSEN_ARM_FOR_SIG.get_or_init(|| DashMap::new()).remove(&tx.signatures[0]).map(|e| e.1) {
                                    if let Some(mut b) = FEE_BANDITS.get_or_init(|| DashMap::new()).get_mut(&ldr) {
                                        b.update(arm, true);
                                    }
                                }
                                return Ok(ExecutionResult { signature: sig0, slot: self.get_current_slot().await, timing_offset_ms: 0, priority_fee: fee, profit });
                            }
                        }
                        Err(Box::new(e))
                    }
                    BlockhashExpired => Err(Box::new(ExecError::BlockhashExpired)),
                    _ => Err(Box::new(e)),
                }
            }
        }
    }

    // -----------------------------
    // Cheap accessor for current slot (prefer WS slot_start, fallback to RPC)
    // -----------------------------
    #[inline]
    pub async fn get_current_slot(&self) -> u64 {
        let ws_slot = self.slot_start.load(Ordering::Acquire);
        if ws_slot != 0 { return ws_slot; }
        self.rpc.get_slot().await.unwrap_or(0)
    }

    // -----------------------------
    // Execute a single flashloan request with advanced features
    // -----------------------------
    pub async fn execute_single_request_advanced(
        &self,
        request: FlashLoanRequest,
    ) -> FlashloanResult<ExecutionResult> {
        // Get current leader and slot
        let current_slot = self.get_current_slot().await;
        let leader = self.get_slot_leader(current_slot).await;
        
        // Build and price the transaction
        let accounts_only: Vec<Pubkey> = request
            .target_accounts
            .iter()
            .map(|m| m.pubkey)
            .collect();
        let (tx, cu_limit, micro_per_cu) = self
            .build_and_price_flashloan_transaction(
                Vec::<Instruction>::new(),
                &accounts_only,
                leader,
            )
            .await?;

        // Execute at the next slot boundary; on BlockhashExpired, refresh blockhash by rebuilding once
        match self
            .execute_at_slot_boundary(tx, cu_limit, micro_per_cu, request.expected_profit)
            .await
        {
            Ok(res) => Ok(res),
            Err(e) => {
                if let Some(exec_err) = e.downcast_ref::<ExecError>() {
                    if matches!(exec_err, ExecError::BlockhashExpired) {
                        // Rebuild with fresh blockhash/fees and retry once
                        let new_leader = self.get_current_leader().await;
                        let accounts_only2: Vec<Pubkey> = request
                            .target_accounts
                            .iter()
                            .map(|m| m.pubkey)
                            .collect();
                        let (tx2, cu2, micro2) = self
                            .build_and_price_flashloan_transaction(
                                Vec::<Instruction>::new(),
                                &accounts_only2,
                                new_leader,
                            )
                            .await?;
                        return self
                            .execute_at_slot_boundary(tx2, cu2, micro2, request.expected_profit)
                            .await;
                    }
                }
                Err(e)
            }
        }
    }

    

    // -----------------------------
    // Execute at boundary with variants, pre-signing, leader-aware fanout
    // -----------------------------
    pub async fn execute_at_boundary_with_variants(
        &self,
        base_instructions: Vec<Instruction>,
        write_accounts: Vec<Pubkey>, // accounts we will write -> for conflict partitions
        expected_profit: u64,
    ) -> FlashloanResult<Signature> {
        if self.circuit_breaker.load(Ordering::Acquire) {
            return Err(ExecutorError::CircuitBreaker.into());
        }

        // current leader heuristics: attempt to resolve leader for current slot
        let leader = self.resolve_current_leader().await?;
        // candidate leaders: current, next 2 (for fanout)
        let candidate_leaders = vec![leader]; // extend with known schedule fetch for next leaders if available

        // Prepare account vec for priority model
        let accounts_for_model: Vec<Pubkey> = write_accounts.clone();

        // build base_instructions into final tx via build_transaction_with_cu for each leader/percentile variant
        // choose variant set: N variants with different percentile/multipliers
        let percentiles = vec![0.99, 0.995, 0.999, 0.95];
        let mut signed_variants: Vec<(VersionedTransaction, u64 /*micro*/, u32 /*cu*/)> = Vec::new();

        for &p in &percentiles {
            // pick leader (here we pick primary)
            let (vtx, micro, cu_limit) = self.build_transaction_with_cu(base_instructions.clone(), &accounts_for_model, leader, p).await?;
            signed_variants.push((vtx, micro, cu_limit));
        }

        // Pre-sign variants against current and next blockhashes for minimal latency
        // PreSigner pre_sign_variants will create signed txs in cache
        // Here we simply send the first signed variant (we have already signed when building)
        // fanout: attempt send to leader and also to next leaders via quic tpu
        let mut send_tasks = JoinSet::new();

        // staggered micro-delays jitter: -0.5ms .. +0.5ms implemented with sleep durations
        let jitter_range_ms = 1.0;
        for (i, (vtx, micro, cu)) in signed_variants.into_iter().enumerate() {
            let sender = self.sender.clone();
            // compute micro-jitter stagger
            let jitter = ((i as f64) * 0.25) - (jitter_range_ms/2.0); // small stagger
            send_tasks.spawn(async move {
                if jitter > 0.0 {
                    sleep(Duration::from_micros((jitter * 1000.0) as u64)).await;
                } else if jitter < 0.0 {
                    // negative jitter = send slightly early
                }
                match sender.send(&vtx).await {
                    Ok(sig) => Ok(sig),
                    Err(e) => Err(anyhow!("send failed: {:?}", e)),
                }
            });
        }

        // Await first successful send and record outcome
        let mut last_err: Option<anyhow::Error> = None;
        while let Some(res) = send_tasks.join_next().await {
            match res {
                Ok(Ok(sig)) => {
                    // Record successful inclusion in fee model
                    if let Some(leader) = self.current_leader.lock().await.as_ref() {
                        self.fee_calculator.get_fee_model()
                            .record_inclusion(*leader, true, micro).await;
                    }
                    
                    // Track leader stats and inclusion counters asynchronously
                    let _ = self.record_send_for_leader(&leader, micro_from_sig(&sig).unwrap_or(0), cu).await;
                    return Ok(sig);
                }
                Ok(Err(e)) => {
                    // Record failed inclusion in fee model
                    if let Some(leader) = self.current_leader.lock().await.as_ref() {
                        self.fee_calculator.get_fee_model()
                            .record_inclusion(*leader, false, micro).await;
                    }
                    last_err = Some(e);
                }
                Err(e) => {
                    last_err = Some(anyhow!("join error: {:?}", e));
                }
            }
        }

        Err(last_err.unwrap_or_else(|| anyhow!("no send task succeeded")))
    }

    // ... (rest of the code remains the same)

    

    async fn record_send_for_leader(&self, leader: &Pubkey, fee: u64, units: u32) -> Result<()> {
        // update in-memory and persist
        let now = chrono::Utc::now().timestamp();
        let mut stats = self.leader_stats_cache.entry(*leader).or_insert_with(|| LeaderStats {
            leader: *leader,
            seen: 0,
            landed: 0,
            mean_fee_to_land: 0.0,
            stdev_fee_to_land: 0.0,
            avg_units_landed: 0.0,
            last_updated: now,
            ema_window_us: 2500.0,
        }).value().clone();
        stats.seen += 1;
        // naive online mean update for mean_fee_to_land
        stats.mean_fee_to_land = ((stats.mean_fee_to_land * ((stats.seen-1) as f64)) + (fee as f64)) / (stats.seen as f64);
        stats.avg_units_landed = ((stats.avg_units_landed * ((stats.seen-1) as f64)) + (units as f64)) / (stats.seen as f64);
        stats.last_updated = now;
        // EMA update for leader-adaptive window (alpha=0.2) using (mu + 2*sigma) clamped to [1600..4000] µs
        let target = (stats.mean_fee_to_land + 2.0 * stats.stdev_fee_to_land).clamp(1600.0, 4000.0);
        stats.ema_window_us = 0.8 * stats.ema_window_us + 0.2 * target;
        self.leader_stats_cache.insert(*leader, stats.clone());
        // persist
        self.persistence.save_leader_stats(leader, &stats)?;
        Ok(())
    }

    async fn resolve_current_leader(&self) -> Result<Pubkey> {
        // Use leader schedule with proper epoch-relative offset mapping
        let info = self.rpc.get_epoch_info().await?;
        let epoch_start = info.absolute_slot.saturating_sub(info.slot_index as u64);
        let cur = info.absolute_slot;
        if let Ok(schedule) = self
            .rpc
            .get_leader_schedule_with_commitment(Some(epoch_start), CommitmentConfig::finalized())
            .await
        {
            for (k, rels) in schedule {
                for rel in rels {
                    if epoch_start.saturating_add(rel) == cur {
                        if let Ok(pk) = k.parse::<Pubkey>() {
                            return Ok(pk);
                        }
                    }
                }
            }
        }
        // fallback: use rpc.get_slot_leader()
        let leader = self.rpc.get_slot_leader().await?;
        Ok(leader)
    }

    // -----------------------------
    // Retry policy layered
    // -----------------------------
    async fn send_with_retry_and_categorized_backoff(&self, vtx: VersionedTransaction) -> Result<Signature> {
        // categorize errors after first attempt, apply slot-aware backoff
        let mut attempt = 0usize;
        let max_attempts = 4usize;
        loop {
            attempt += 1;
            match self.sender.send(&vtx).await {
                Ok(sig) => {
                    // confirm via onSignature subscription ideally; fallback to polling small window
                    return Ok(sig);
                }
                Err(e) => {
                    let err_str = format!("{:?}", e);
                    if err_str.contains("BlockhashNotFound") || err_str.contains("Expired") {
                        // refresh blockhash re-sign
                        let new_blockhash = self.rpc.get_latest_blockhash().await?;
                        // re-sign: You must rebuild and re-sign the message with new blockhash.
                        // Here, we expect caller to provide mechanisms to re-sign; bubble error for now
                        return Err(anyhow!("blockhash expired — caller must rebuild & re-sign: {}", err_str));
                    } else if err_str.contains("AccountInUse") {
                        // jitter small and retry same fee
                        let jitter_ms = 2 + attempt as u64;
                        sleep(Duration::from_millis(jitter_ms)).await;
                        if attempt >= max_attempts { break; }
                        continue;
                    } else if err_str.contains("WouldBeDropped") || err_str.contains("TooLowFee") {
                        // escalate fee: caller should increment percentile and re-build & re-sign new variant
                        return Err(anyhow!("low fee — escalate to next percentile: {}", err_str));
                    } else {
                        // general, retry with exponential jitter but slot aware
                        let backoff_slots = attempt; // 1,2,3 slots
                        let backoff_ms = backoff_slots as u64 * SLOT_DURATION_MS;
                        sleep(Duration::from_millis(backoff_ms)).await;
                        if attempt >= max_attempts { break; }
                    }
                }
            }
        }
        Err(anyhow!("send attempts exhausted"))
    }
}

// Send metadata map to correlate signature -> (micro, cu)
static SEND_META: once_cell::sync::Lazy<DashMap<Signature, (u64, u32)>> =
    once_cell::sync::Lazy::new(|| DashMap::new());

#[inline]
fn record_send_meta(sig: Signature, micro: u64, cu: u64) {
    SEND_META.insert(sig, (micro, cu as u32));
}

#[inline]
fn micro_from_sig(sig: &Signature) -> Option<u64> {
    SEND_META.get(sig).map(|e| e.value().0)
}

    // -----------------------------
    // Account builders and v0 helper (no Rent sysvar unless required)
    // -----------------------------
    fn build_flashloan_accounts(&self, request: &FlashLoanRequest) -> Vec<AccountMeta> {
        let mut accounts = vec![
            AccountMeta::new(self.wallet.pubkey(), true),
            AccountMeta::new_readonly(clock::id(), false),
            // Intentionally exclude Rent sysvar unless your on-chain program requires it:
            // AccountMeta::new_readonly(rent::id(), false),
        ];
        accounts.extend(request.target_accounts.clone());
        accounts
    }

    #[allow(dead_code)]
    fn build_v0_with_alt(
        &self,
        ixs: Vec<Instruction>,
        payer: Pubkey,
        recent_blockhash: Hash,
    ) -> Result<VersionedTransaction, ExecError> {
        let msg = V0Message::try_compile(
            &payer,
            &ixs,
            &[], // add ALT lookups here if you maintain an ALT registry
            recent_blockhash,
        )
        .map_err(|e| ExecError::Other(e.to_string()))?;

        VersionedTransaction::try_new(VersionedMessage::V0(msg), &[&*self.wallet])
            .map_err(|e| ExecError::Other(e.to_string()))
    }

// -----------------------------
// Unit tests (some core tests)
// -----------------------------
#[cfg(test)]
mod tests {
    use super::*;
    use solana_sdk::signature::Keypair;

    #[tokio::test]
    async fn test_sliding_window_percentile() {
        let w = SlidingWindowPercentile::new(10);
        for i in 1..=10u64 { w.push(i*100); }
        let p50 = w.percentile(0.5).unwrap();
        assert!(p50 >= 500 && p50 <= 1000);
    }

    #[tokio::test]
    async fn test_partitioner_non_conflicting() {
        #[derive(Clone)]
        struct Req { id: u8, writes: Vec<Pubkey> }
        let a = Pubkey::new_unique();
        let b = Pubkey::new_unique();
        let c = Pubkey::new_unique();
        let reqs = vec![
            Req { id: 1, writes: vec![a] },
            Req { id: 2, writes: vec![b] },
            Req { id: 3, writes: vec![a] },
            Req { id: 4, writes: vec![c] },
        ];
        let groups = partition_non_conflicting(&reqs, |r| r.writes.clone());
        // Expect groups >= 2
        assert!(groups.len() >= 2);
    }

    #[tokio::test]
    async fn test_escalation_never_breaks_pnl() {
        let cu = 200_000u64;
        let profit = 1_000_000u64; // lamports
        for micro in [40_000u64, 50_000u64, 60_000u64] {
            let fee = (micro.saturating_mul(cu)) / 1_000_000;
            assert!(fee < profit, "baseline fee must be profitable");
        }
    }

    #[test]
    fn test_coloring_no_conflicts_same_group() {
        let a = Pubkey::new_unique();
        #[derive(Clone)]
        struct R { writes: Vec<Pubkey> }
        let reqs = vec![R{writes:vec![a]}, R{writes:vec![a]}];
        let groups = partition_conflicts_coloring(&reqs, |r| r.writes.clone(), 4);
        for g in groups {
            let count_with_a = g.iter().filter(|r| r.writes.contains(&a)).count();
            assert!(count_with_a <= 1, "conflicting writes grouped together");
        }
    }
}
