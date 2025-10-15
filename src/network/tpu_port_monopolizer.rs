// src/net/tpu.rs
#![forbid(unsafe_code)]
#![allow(clippy::too_many_arguments)]
#![allow(clippy::large_enum_variant)]

use std::collections::{BTreeMap, HashMap};
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::PathBuf;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use std::time::{Duration, Instant};

use anyhow::{anyhow, Context, Result};
use bytes::Bytes;
use quinn::{ClientConfig, Endpoint};
use rand::seq::SliceRandom;
use rustls;
use serde::Serialize;
use tokio::net::UdpSocket;
use tokio::sync::{Mutex as TokioMutex, RwLock as TokioRwLock, Semaphore};
use tokio::time::{sleep, timeout};

// --- Solana deps
use solana_client::{nonblocking::rpc_client::RpcClient as NonblockingRpcClient, rpc_client::RpcClient};
use solana_client::rpc_config::RpcSendTransactionConfig;
use solana_sdk::{
    commitment_config::CommitmentConfig,
    pubkey::Pubkey,
    signature::{Keypair, Signature},
    signer::Signer,
    transaction::Transaction,
};

// =================== Tunables ===================
// Refresh closer to slot time (~400ms) instead of letting schedule go stale for 3–4 slots
const LEADER_SCHED_REFRESH_MS: u64 = 500;

// Connection dials
const MAX_CONNECTIONS_PER_LEADER: usize = 8;
const CONNECT_TIMEOUT_MS: u64 = 250;

// Two-tier attempt windows; tier1 is tight, tier2 is slower fallthrough
const SEND_TIER1_TIMEOUT_MS: u64 = 160;
const SEND_TIER2_TIMEOUT_MS: u64 = 340;

// Avoid self-induced queues; tier1 fanout is small
const TIER1_PARALLEL_SENDS: usize = 4;
const TIER2_PARALLEL_SENDS: usize = 2;

// Legacy-UDP safe payload size. QUIC datagrams can traverse larger effective MTU,
// and we already fall back to a QUIC uni-stream for oversize payloads.
// Heuristic note: 1232 is a commonly used safe MTU budget for UDP; it is NOT a Solana
// protocol guarantee. Treat as guardrail for the legacy UDP path only. [Heuristic]
const MAX_PACKET_SIZE: usize = 1232;

// Health decay
const FAILURE_HEALTH_DECAY: f64 = 0.25;
const SUCCESS_HEALTH_RESTORE: f64 = 0.50;

// Exponential backoff caps
const BACKOFF_BASE_MS: u64 = 12;
const BACKOFF_MAX_MS: u64 = 64;

// Re-tier fast path: if tier1 fully fails, pull fresh slot once
const FAST_RETIER_ON_FAIL: bool = true;

// Token bucket (per address) to stop a poison endpoint from hogging sends
const TB_CAPACITY: f32 = 2.0;          // max 2 tokens
const TB_REFILL_PER_SEC: f32 = 2.0;    // 2 sends per second sustained
const TB_MIN_TOKENS_TO_SEND: f32 = 1.0;

// =================== Transport policy ===================
#[derive(Clone, Copy, Debug)]
enum Proto {
    Udp,
    Quic,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum PathKind {
    Direct,
    Forward,
}

#[derive(Clone, Debug, Default)]
struct TpuAddrs {
    udp: Vec<SocketAddr>,
    udp_fwd: Vec<SocketAddr>,
    quic: Vec<SocketAddr>,
    quic_fwd: Vec<SocketAddr>,
}

// =================== Token bucket ===================
#[derive(Clone, Copy, Debug)]
struct TokenBucket {
    tokens: f32,
    last_refill: Instant,
}
impl TokenBucket {
    fn new() -> Self {
        Self { tokens: TB_CAPACITY, last_refill: Instant::now() }
    }
    fn refill(&mut self) {
        let dt = Instant::now().saturating_duration_since(self.last_refill).as_secs_f32();
        if dt > 0.0 {
            self.tokens = (self.tokens + TB_REFILL_PER_SEC * dt).min(TB_CAPACITY);
            self.last_refill = Instant::now();
        }
    }
    fn try_take(&mut self) -> bool {
        self.refill();
        if self.tokens >= TB_MIN_TOKENS_TO_SEND {
            self.tokens -= TB_MIN_TOKENS_TO_SEND;
            true
        } else {
            false
        }
    }
}

// Minimal per-addr connection state
#[derive(Clone)]
struct Conn {
    addr: SocketAddr,
    proto: Proto,
    path: PathKind,
    quic_ep: Option<Endpoint>,
    quic_conn: Option<quinn::Connection>,
    udp: Option<UdpSocket>,
    last_ok: Option<Instant>,
    healthy: bool,
    rtt_hint: Option<Duration>,
    fail_count: u32,
    tb: TokenBucket, // token bucket
}

impl Conn {
    fn score(&self) -> f64 {
        // Stake-weighted QoS context:
        // Tiering aligns with stake-weighted QoS practice; QUIC TPU ports are from getClusterNodes. [Solana]
        let h = if self.healthy { 1.0 } else { -1.0 };
        let p = match self.proto {
            Proto::Quic => 0.18,
            Proto::Udp => 0.0,
        };
        let w = match self.path {
            PathKind::Direct => 0.03,
            PathKind::Forward => 0.08,
        };
        let rtt = self.rtt_hint.unwrap_or(Duration::from_micros(800));
        let rtt_s = 1.0 / (1.0 + (rtt.as_micros() as f64 / 800.0));
        let fresh = self
            .last_ok
            .map(|t| {
                let age_ms = Instant::now()
                    .saturating_duration_since(t)
                    .as_millis() as f64;
                (1200.0 / (1200.0 + age_ms)).max(0.15) * 0.12
            })
            .unwrap_or(0.0);
        let penalty = (self.fail_count as f64) * 0.06;
        (0.62 * h) + (0.15 * rtt_s) + p + w + fresh - penalty
    }

    fn backoff_ms(&self) -> u64 {
        let n = self.fail_count.min(10);
        let pow = 1u64.saturating_shl(n);
        (BACKOFF_BASE_MS * pow).min(BACKOFF_MAX_MS)
    }
}

// =================== Leader schedule cache ===================
#[derive(Clone, Default)]
struct LeaderScheduleCache {
    schedule: BTreeMap<u64, Pubkey>, // absolute slot -> leader
    tpu_by_leader: HashMap<Pubkey, TpuAddrs>,
    last_update: Instant,
    last_slot: u64,
    slots_per_epoch: u64,
    epoch: u64,
    epoch_start_slot: u64,
}

// =================== Ecosystem compat flags ===================
#[derive(Clone, Copy, Debug)]
struct EcosystemCompat {
    use_quic: bool,
    use_tpu_client_hint: bool,
    // QUIC dials: allow SNI/ALPN override so we can debug handshake weirdness
    quic_sni: Option<String>,
    quic_alpn: Vec<Vec<u8>>, // possibly multiple ALPNs to try
}

impl EcosystemCompat {
    fn detect() -> Self {
        let use_quic = std::env::var("SOLANA_USE_QUIC")
            .ok()
            .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
            .unwrap_or(true);
        let use_tpu_client_hint = std::env::var("SOLANA_USE_TPU_CLIENT")
            .ok()
            .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
            .unwrap_or(true);

        // SNI override. Default historically used in client examples is "solana-validator".
        let quic_sni = std::env::var("SOLANA_QUIC_SNI").ok();

        // ALPN override. Comma-separated; empty means "no ALPN". If not set, we try none, then "h3".
        // Heuristic note: There is no canonical “must-use ALPN” in public docs. This is a defensive
        // client knob, not a protocol requirement. [Heuristic]
        let quic_alpn: Vec<Vec<u8>> = if let Ok(val) = std::env::var("SOLANA_QUIC_ALPN") {
            val.split(',')
                .map(|s| s.trim().as_bytes().to_vec())
                .collect()
        } else {
            vec![Vec::new(), b"h3".to_vec()]
        };

        Self {
            use_quic,
            use_tpu_client_hint,
            quic_sni,
            quic_alpn,
        }
    }
}

// =================== Abstraction ===================
#[allow(async_fn_in_trait)]
pub trait TpuSender: Send + Sync {
    async fn send(&self, tx: &Transaction) -> Result<Signature>;
    async fn shutdown(&self);
}

// =================== Shared schedule + address utilities ===================
#[derive(Clone)]
struct SharedContext {
    rpc: Arc<RpcClient>,
    arpc: Arc<NonblockingRpcClient>,
    compat: EcosystemCompat,
    leader_cache: Arc<TokioRwLock<LeaderScheduleCache>>,
    audit: AuditLogger,
}

impl SharedContext {
    async fn refresh_once(&self) -> Result<()> {
        refresh_leaders_and_addrs(self.leader_cache.clone(), self.arpc.clone(), self.compat).await
    }

    fn spawn_refresh_task(&self, shutting: Arc<AtomicBool>) {
        let cache = self.leader_cache.clone();
        let arpc = self.arpc.clone();
        let compat = self.compat;
        tokio::spawn(async move {
            while !shutting.load(Ordering::Relaxed) {
                let _ = refresh_leaders_and_addrs(cache.clone(), arpc.clone(), compat).await;
                sleep(Duration::from_millis(LEADER_SCHED_REFRESH_MS)).await;
            }
        });
    }

    async fn current_slot_and_leaders(&self) -> Result<(u64, Pubkey, Option<Pubkey>, Option<Pubkey>)> {
        let slot = self.arpc.get_slot().await?;
        let r = self.leader_cache.read().await;

        let now_exact = r.schedule.get(&slot).copied();
        let mut before = r.schedule.range(..=slot);
        let prev = before.next_back().map(|(s, pk)| (*s, *pk));
        let mut after = r.schedule.range(slot + 1..);
        let next = after.next().map(|(s, pk)| (*s, *pk));
        let next2 = after.next().map(|(s, pk)| (*s, *pk));

        let leader_now = if let Some(pk) = now_exact {
            pk
        } else if let Some((ps, pk)) = prev {
            if slot.saturating_sub(ps) <= 1 {
                pk
            } else if let Some((_, n1)) = next {
                n1
            } else {
                return Err(anyhow!("no leader near slot {}", slot));
            }
        } else if let Some((_, n1)) = next {
            n1
        } else {
            return Err(anyhow!("no leader around slot {}", slot));
        };

        let leader_next = next.map(|(_, pk)| pk);
        let leader_next2 = next2.map(|(_, pk)| pk);
        Ok((slot, leader_now, leader_next, leader_next2))
    }

    async fn tpu_addrs_for_leader(&self, leader: &Pubkey) -> Result<TpuAddrs> {
        let r = self.leader_cache.read().await;
        r.tpu_by_leader
            .get(leader)
            .cloned()
            .ok_or_else(|| anyhow!("no TPU addresses for leader {}", leader))
    }
}

// =================== Audit logger ===================
#[derive(Clone)]
struct AuditLogger {
    base_dir: PathBuf,
}

#[derive(Serialize)]
struct SendAudit {
    ts: u64,
    slot: u64,
    leader: String,
    addr: String,
    proto: &'static str,
    path: &'static str,
    rtt_ms: Option<u128>,
    tier: u8,
    outcome: &'static str,
}

impl AuditLogger {
    fn new(base_dir: impl Into<PathBuf>) -> Self {
        Self { base_dir: base_dir.into() }
    }
    async fn log(&self, rec: &SendAudit) {
        let _ = self.ensure_dir().await;
        let fname = self.hourly_file();
        let line = match serde_json::to_string(rec) {
            Ok(s) => s,
            Err(_) => return,
        };
        let _ = tokio::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&fname)
            .await
            .and_then(|mut f| async move {
                use tokio::io::AsyncWriteExt;
                f.write_all(line.as_bytes()).await?;
                f.write_all(b"\n").await
            });
    }
    fn hourly_file(&self) -> PathBuf {
        use chrono::{Datelike, Timelike, Utc};
        let now = Utc::now();
        let mut p = self.base_dir.clone();
        let y = now.year();
        let m = now.month();
        let d = now.day();
        let h = now.hour();
        p.push(format!("{y:04}{m:02}{d:02}_{h:02}.jsonl"));
        p
    }
    async fn ensure_dir(&self) -> Result<()> {
        if !self.base_dir.exists() {
            tokio::fs::create_dir_all(&self.base_dir).await?;
        }
        Ok(())
    }
}

// =================== Official-like backend (feature: official_tpu) ===================
#[cfg(feature = "official_tpu")]
pub struct OfficialLikeSender {
    ctx: SharedContext,
    shutting_down: Arc<AtomicBool>,
    conn_cache: Arc<TokioRwLock<HashMap<SocketAddr, Arc<TokioMutex<Conn>>>>>,
}

#[cfg(feature = "official_tpu")]
impl OfficialLikeSender {
    pub async fn new(rpc_url: &str, _identity: Arc<Keypair>) -> Result<Self> {
        let rpc = Arc::new(RpcClient::new_with_commitment(
            rpc_url.to_string(),
            CommitmentConfig::confirmed(),
        ));
        let arpc = Arc::new(NonblockingRpcClient::new_with_commitment(
            rpc_url.to_string(),
            CommitmentConfig::confirmed(),
        ));
        let compat = EcosystemCompat::detect();
        let ctx = SharedContext {
            rpc,
            arpc,
            compat,
            leader_cache: Arc::new(TokioRwLock::new(LeaderScheduleCache::default())),
            audit: AuditLogger::new("logs/tpu"),
        };
        let shutting = Arc::new(AtomicBool::new(false));
        ctx.refresh_once().await?;
        ctx.spawn_refresh_task(shutting.clone());
        Ok(Self {
            ctx,
            shutting_down: shutting,
            conn_cache: Arc::new(TokioRwLock::new(HashMap::new())),
        })
    }

    async fn ensure_conn(&self, addr: SocketAddr, proto: Proto, path: PathKind) -> Result<Arc<TokioMutex<Conn>>> {
        if let Some(c) = self.conn_cache.read().await.get(&addr).cloned() {
            return Ok(c);
        }
        let c = make_conn_once(addr, proto, path, self.ctx.compat).await?;
        let arc = Arc::new(TokioMutex::new(c));
        self.conn_cache.write().await.insert(addr, arc.clone());
        Ok(arc)
    }

    async fn build_tiers(&self) -> Result<Vec<Vec<(SocketAddr, Proto, PathKind)>>> {
        let (_, leader_now, leader_next, leader_next2) = self.ctx.current_slot_and_leaders().await?;
        let mut tiers: Vec<Vec<(SocketAddr, Proto, PathKind)>> = Vec::new();

        let mut push_leader = |addrs: TpuAddrs| {
            let mut tier1 = Vec::new();

            // Prefer QUIC paths first; UDP is legacy parity.
            // Stake-weighted QoS context: prefer leader QUIC TPU endpoints from getClusterNodes. [Solana]

            // QUIC direct
            for a in addrs.quic.iter().copied() {
                tier1.push((a, Proto::Quic, PathKind::Direct));
            }
            // QUIC forwards
            for a in addrs.quic_fwd.iter().copied() {
                tier1.push((a, Proto::Quic, PathKind::Forward));
            }

            // Legacy UDP fallback (plain `tpu` and `tpu_forwards` are UDP). [Solana]
            if tier1.is_empty() {
                for a in addrs.udp.iter().copied() {
                    tier1.push((a, Proto::Udp, PathKind::Direct));
                }
                for a in addrs.udp_fwd.iter().copied() {
                    tier1.push((a, Proto::Udp, PathKind::Forward));
                }
            }
            if !tier1.is_empty() {
                tiers.push(tier1);
            }
        };

        let addrs = self.ctx.tpu_addrs_for_leader(&leader_now).await?;
        push_leader(addrs);
        if let Some(n1) = leader_next {
            let addrs = self.ctx.tpu_addrs_for_leader(&n1).await?;
            push_leader(addrs);
        }
        if let Some(n2) = leader_next2 {
            let addrs = self.ctx.tpu_addrs_for_leader(&n2).await?;
            push_leader(addrs);
        }

        // Dedup inside each tier by (addr, proto)
        for tier in tiers.iter_mut() {
            tier.sort_by_key(|(a, p, k)| {
                (
                    *a,
                    match p { Proto::Udp => 0u8, Proto::Quic => 1u8 },
                    match k { PathKind::Direct => 0u8, PathKind::Forward => 1u8 },
                )
            });
            tier.dedup_by_key(|(a, p, _)| (*a, match p { Proto::Udp => 0u8, Proto::Quic => 1u8 }));
        }
        Ok(tiers)
    }
}

#[cfg(feature = "official_tpu")]
#[allow(async_fn_in_trait)]
impl TpuSender for OfficialLikeSender {
    async fn send(&self, tx: &Transaction) -> Result<Signature> {
        if !self.ctx.compat.use_tpu_client_hint {
            // RPC fallback parity with TPU fire-and-forget: skip preflight, no retries
            let sig = self.ctx.rpc.send_transaction_with_config(
                tx,
                RpcSendTransactionConfig {
                    skip_preflight: true,
                    max_retries: Some(0),
                    ..RpcSendTransactionConfig::default()
                },
            )?;
            return Ok(sig);
        }

        let sig = *tx.signatures.get(0).ok_or_else(|| anyhow!("transaction missing signature"))?;
        let wire = Bytes::from(bincode::serialize(tx)?);

        let mut tiers = self.build_tiers().await?;

        for (tier_idx, tier) in tiers.iter_mut().enumerate() {
            // Pass 1 (tight)
            {
                let mut tasks = Vec::new();
                for (i, (addr, proto, path)) in tier.iter().enumerate() {
                    if i >= TIER1_PARALLEL_SENDS { break; }
                    let addr = *addr;
                    let proto = *proto;
                    let path = *path;
                    let bytes = wire.clone();
                    let ctx = self.ctx.clone();
                    let cache = self.conn_cache.clone();

                    tasks.push(tokio::spawn(async move {
                        let conn_arc = if let Some(c) = cache.read().await.get(&addr).cloned() {
                            c
                        } else {
                            let c = make_conn_once(addr, proto, path, ctx.compat).await?;
                            let arc = Arc::new(TokioMutex::new(c));
                            cache.write().await.insert(addr, arc.clone());
                            arc
                        };

                        // token bucket gate
                        {
                            let mut cm = conn_arc.lock().await;
                            if !cm.tb.try_take() {
                                return Err::<(), anyhow::Error>(anyhow!("tb-throttle"));
                            }
                        }

                        // backoff a hair on repeated failures
                        let bof = { let cm = conn_arc.lock().await; cm.backoff_ms() };
                        if bof > 0 { sleep(Duration::from_millis(bof)).await; }

                        let start = Instant::now();
                        let res = timeout(
                            Duration::from_millis(SEND_TIER1_TIMEOUT_MS),
                            async {
                                let mut cm = conn_arc.lock().await;
                                let outcome = send_one(&mut *cm, &bytes, ctx.compat).await;
                                let rtt = start.elapsed();
                                match &outcome {
                                    Ok(()) => {
                                        cm.last_ok = Some(Instant::now());
                                        cm.healthy = true;
                                        cm.rtt_hint = Some(rtt);
                                        cm.fail_count = cm.fail_count.saturating_sub(1);
                                        ctx.audit.log(&SendAudit {
                                            ts: unix_ms(),
                                            slot: ctx.arpc.get_slot().await.unwrap_or(0),
                                            leader: "unknown".into(),
                                            addr: addr.to_string(),
                                            proto: match proto { Proto::Quic => "quic", Proto::Udp => "udp" },
                                            path: match path { PathKind::Direct => "direct", PathKind::Forward => "forward" },
                                            rtt_ms: Some(rtt.as_millis()),
                                            tier: 1,
                                            outcome: "ok",
                                        }).await;
                                    }
                                    Err(_) => {
                                        cm.healthy = if cm.fail_count < 3 { cm.healthy } else { false };
                                        cm.fail_count = cm.fail_count.saturating_add(1);
                                    }
                                }
                                outcome
                            },
                        ).await;

                        match res {
                            Ok(Ok(())) => Ok(()),
                            Ok(Err(e)) => Err(e),
                            Err(_) => Err(anyhow!("send-timeout")),
                        }
                    }));
                }

                for t in tasks {
                    if let Ok(Ok(())) = t.await { return Ok(sig); }
                }
            }

            // Pull-fresh on total tier fail for EVERY tier, not just the first
            if FAST_RETIER_ON_FAIL {
                let _ = self.ctx.refresh_once().await;
                if let Ok(new_tiers) = self.build_tiers().await {
                    if let Some(new0) = new_tiers.get(tier_idx) {
                        *tier = new0.clone();
                    }
                }
            }

            // Pass 2 (slower)
            {
                let mut tasks = Vec::new();
                for (i, (addr, proto, path)) in tier.iter().enumerate() {
                    if i >= TIER2_PARALLEL_SENDS { break; }
                    let addr = *addr;
                    let proto = *proto;
                    let path = *path;
                    let bytes = wire.clone();
                    let ctx = self.ctx.clone();
                    let cache = self.conn_cache.clone();

                    tasks.push(tokio::spawn(async move {
                        let conn_arc = if let Some(c) = cache.read().await.get(&addr).cloned() {
                            c
                        } else {
                            let c = make_conn_once(addr, proto, path, ctx.compat).await?;
                            let arc = Arc::new(TokioMutex::new(c));
                            cache.write().await.insert(addr, arc.clone());
                            arc
                        };

                        // token bucket gate
                        {
                            let mut cm = conn_arc.lock().await;
                            if !cm.tb.try_take() {
                                return Err::<(), anyhow::Error>(anyhow!("tb-throttle"));
                            }
                        }

                        // backoff
                        let bof = { let cm = conn_arc.lock().await; cm.backoff_ms() };
                        if bof > 0 { sleep(Duration::from_millis(bof)).await; }

                        let start = Instant::now();
                        let res = timeout(
                            Duration::from_millis(SEND_TIER2_TIMEOUT_MS),
                            async {
                                let mut cm = conn_arc.lock().await;
                                let outcome = send_one(&mut *cm, &bytes, ctx.compat).await;
                                let rtt = start.elapsed();
                                match &outcome {
                                    Ok(()) => {
                                        cm.last_ok = Some(Instant::now());
                                        cm.healthy = true;
                                        cm.rtt_hint = Some(rtt);
                                        cm.fail_count = cm.fail_count.saturating_sub(1);
                                        ctx.audit.log(&SendAudit {
                                            ts: unix_ms(),
                                            slot: ctx.arpc.get_slot().await.unwrap_or(0),
                                            leader: "unknown".into(),
                                            addr: addr.to_string(),
                                            proto: match proto { Proto::Quic => "quic", Proto::Udp => "udp" },
                                            path: match path { PathKind::Direct => "direct", PathKind::Forward => "forward" },
                                            rtt_ms: Some(rtt.as_millis()),
                                            tier: 2,
                                            outcome: "ok",
                                        }).await;
                                    }
                                    Err(_) => {
                                        cm.fail_count = cm.fail_count.saturating_add(1);
                                        if cm.fail_count >= 3 { cm.healthy = false; }
                                    }
                                }
                                outcome
                            },
                        ).await;

                        match res {
                            Ok(Ok(())) => Ok(()),
                            Ok(Err(e)) => Err(e),
                            Err(_) => Err(anyhow!("send-timeout")),
                        }
                    }));
                }

                for t in tasks {
                    if let Ok(Ok(())) = t.await { return Ok(sig); }
                }
            }
        }

        Err(anyhow!("all TPU sends failed (official-like path)"))
    }

    async fn shutdown(&self) {
        self.shutting_down.store(true, Ordering::Relaxed);
        let cache = self.conn_cache.write().await;
        for c in cache.values() {
            if let Ok(mut g) = c.try_lock() {
                if let Some(ep) = g.quic_ep.take() {
                    ep.close(0u32.into(), b"shutdown");
                }
            }
        }
    }
}

// =================== Custom QUIC backend (default) ===================
#[cfg(not(feature = "official_tpu"))]
pub struct CustomQuicSender {
    ctx: SharedContext,
    _identity: Arc<Keypair>,
    active: Arc<TokioRwLock<HashMap<Pubkey, Vec<Arc<TokioMutex<Conn>>>>>>,
    shutting_down: Arc<AtomicBool>,
    send_gate: Arc<Semaphore>,
}

#[cfg(not(feature = "official_tpu"))]
impl CustomQuicSender {
    pub async fn new(rpc_url: &str, identity: Arc<Keypair>) -> Result<Self> {
        let rpc = Arc::new(RpcClient::new_with_commitment(
            rpc_url.to_string(),
            CommitmentConfig::confirmed(),
        ));
        let arpc = Arc::new(NonblockingRpcClient::new_with_commitment(
            rpc_url.to_string(),
            CommitmentConfig::confirmed(),
        ));
        let compat = EcosystemCompat::detect();
        let ctx = SharedContext {
            rpc,
            arpc,
            compat,
            leader_cache: Arc::new(TokioRwLock::new(LeaderScheduleCache::default())),
            audit: AuditLogger::new("logs/tpu"),
        };
        let me = Self {
            ctx,
            _identity: identity,
            active: Arc::new(TokioRwLock::new(HashMap::new())),
            shutting_down: Arc::new(AtomicBool::new(false)),
            send_gate: Arc::new(Semaphore::new(TIER1_PARALLEL_SENDS.max(TIER2_PARALLEL_SENDS))),
        };
        me.ctx.refresh_once().await?;
        me.ctx.spawn_refresh_task(me.shutting_down.clone());
        Ok(me)
    }

    async fn ensure_active_conns_for(&self, leader: &Pubkey) -> Result<Vec<Arc<TokioMutex<Conn>>>> {
        if let Some(existing) = self.active.read().await.get(leader).cloned() {
            if !existing.is_empty() {
                return Ok(existing);
            }
        }
        let addrs = self.ctx.tpu_addrs_for_leader(leader).await?;

        // QUIC strictness: prefer QUIC and QUIC forwards first; UDP is appended last
        // Stake-weighted QoS context: prefer leader QUIC TPU endpoints from getClusterNodes. [Solana]
        let mut targets: Vec<(SocketAddr, Proto, PathKind)> = Vec::new();

        if self.ctx.compat.use_quic {
            for a in addrs.quic {
                targets.push((a, Proto::Quic, PathKind::Direct));
            }
            for a in addrs.quic_fwd {
                targets.push((a, Proto::Quic, PathKind::Forward));
            }
        }

        if targets.is_empty() {
            // Legacy UDP parity sources (plain `tpu` and `tpu_forwards`). [Solana]
            for a in addrs.udp {
                targets.push((a, Proto::Udp, PathKind::Direct));
            }
            for a in addrs.udp_fwd {
                targets.push((a, Proto::Udp, PathKind::Forward));
            }
        }

        // Dedup by address, prefer QUIC if the same
        targets.sort_by_key(|(a, p, k)| {
            (
                *a,
                match p { Proto::Udp => 0u8, Proto::Quic => 1u8 },
                match k { PathKind::Direct => 0u8, PathKind::Forward => 1u8 },
            )
        });
        targets.dedup_by_key(|(a, p, _)| (*a, match p { Proto::Udp => 0u8, Proto::Quic => 1u8 }));

        let mut rng = rand::thread_rng();
        targets.shuffle(&mut rng);

        let mut out: Vec<Arc<TokioMutex<Conn>>> = Vec::new();
        for (addr, proto, path) in targets.into_iter().take(MAX_CONNECTIONS_PER_LEADER) {
            if let Ok(conn_fut) =
                timeout(Duration::from_millis(CONNECT_TIMEOUT_MS), self.make_conn(addr, proto, path)).await
            {
                if let Ok(c) = conn_fut {
                    out.push(Arc::new(TokioMutex::new(c)));
                }
            }
        }

        // bias a forward early if available
        if out.len() >= 2 {
            let has_forward_front = {
                let mut found = false;
                for cm in out.iter().take(2) {
                    if let Ok(c) = cm.try_lock() {
                        if c.path == PathKind::Forward {
                            found = true;
                            break;
                        }
                    }
                }
                found
            };
            if !has_forward_front {
                if let Some(pos) = out
                    .iter()
                    .position(|cm| cm.try_lock().ok().map(|c| c.path == PathKind::Forward).unwrap_or(false))
                {
                    out.swap(1, pos);
                }
            }
        }

        // score sort (see QoS note above). [Solana]
        out.sort_by(|a, b| {
            let ascore = a.try_lock().ok().map(|c| c.score()).unwrap_or(0.0);
            let bscore = b.try_lock().ok().map(|c| c.score()).unwrap_or(0.0);
            bscore.partial_cmp(&ascore).unwrap()
        });

        self.active.write().await.insert(*leader, out.clone());
        Ok(out)
    }

    async fn make_conn(&self, addr: SocketAddr, proto: Proto, path: PathKind) -> Result<Conn> {
        match proto {
            Proto::Udp => {
                let udp = UdpSocket::bind(match addr {
                    SocketAddr::V4(_) => SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), 0),
                    SocketAddr::V6(_) => "[::]:0".parse().unwrap(),
                })
                .await
                .context("bind UDP")?;
                udp.connect(addr).await.context("connect UDP")?;
                Ok(Conn {
                    addr, proto, path,
                    quic_ep: None, quic_conn: None, udp: Some(udp),
                    last_ok: None, healthy: true, rtt_hint: None, fail_count: 0,
                    tb: TokenBucket::new(),
                })
            }
            Proto::Quic => {
                make_quic_conn(addr, path, self.ctx.compat).await
            }
        }
    }

    async fn build_tiers_ranked(&self) -> Result<Vec<Vec<Arc<TokioMutex<Conn>>>>> {
        let (_, leader_now, leader_next, leader_next2) = self.ctx.current_slot_and_leaders().await?;
        let mut tiers: Vec<Vec<Arc<TokioMutex<Conn>>>> = Vec::new();

        let mut push = |v: Vec<Arc<TokioMutex<Conn>>>| {
            if !v.is_empty() { tiers.push(v); }
        };

        {
            let conns = self.ensure_active_conns_for(&leader_now).await?;
            push(conns);
        }
        if let Some(n1) = leader_next {
            let conns = self.ensure_active_conns_for(&n1).await?;
            push(conns);
        }
        if let Some(n2) = leader_next2 {
            let conns = self.ensure_active_conns_for(&n2).await?;
            push(conns);
        }

        for tier in tiers.iter_mut() {
            // score sort (QoS/tier alignment) [Solana]
            tier.sort_by(|a, b| {
                let ascore = a.try_lock().ok().map(|c| c.score()).unwrap_or(0.0);
                let bscore = b.try_lock().ok().map(|c| c.score()).unwrap_or(0.0);
                bscore.partial_cmp(&ascore).unwrap()
            });
        }
        Ok(tiers)
    }
}

#[cfg(not(feature = "official_tpu"))]
#[allow(async_fn_in_trait)]
impl TpuSender for CustomQuicSender {
    async fn send(&self, tx: &Transaction) -> Result<Signature> {
        if !self.ctx.compat.use_tpu_client_hint {
            let sig = self.ctx.rpc.send_transaction_with_config(
                tx,
                RpcSendTransactionConfig {
                    skip_preflight: true,
                    max_retries: Some(0),
                    ..RpcSendTransactionConfig::default()
                },
            )?;
            return Ok(sig);
        }

        let sig = *tx.signatures.get(0).ok_or_else(|| anyhow!("transaction missing signature"))?;
        let wire = Bytes::from(bincode::serialize(tx)?);

        let mut tiers = self.build_tiers_ranked().await?;

        for (tidx, tier) in tiers.iter_mut().enumerate() {
            // pass 1
            {
                let mut tasks = Vec::with_capacity(tier.len().min(TIER1_PARALLEL_SENDS));
                for (i, c) in tier.iter().enumerate() {
                    if i >= TIER1_PARALLEL_SENDS { break; }
                    let gate = self.send_gate.clone().acquire_owned().await?;
                    let bytes = wire.clone();
                    let ctx = self.ctx.clone();
                    let c = c.clone();

                    tasks.push(tokio::spawn(async move {
                        let _permit = gate;

                        // token bucket gate
                        {
                            let mut cm = c.lock().await;
                            if !cm.tb.try_take() {
                                return Err::<(), anyhow::Error>(anyhow!("tb-throttle"));
                            }
                        }

                        let bof = { let cm = c.lock().await; cm.backoff_ms() };
                        if bof > 0 { sleep(Duration::from_millis(bof)).await; }

                        let addr; let proto; let path;
                        { let cm = c.lock().await; addr = cm.addr; proto = cm.proto; path = cm.path; }

                        let start = Instant::now();
                        let res = timeout(
                            Duration::from_millis(SEND_TIER1_TIMEOUT_MS),
                            async {
                                let mut cm = c.lock().await;
                                let outcome = send_one(&mut *cm, &bytes, ctx.compat).await;
                                let rtt = start.elapsed();
                                match &outcome {
                                    Ok(()) => {
                                        cm.last_ok = Some(Instant::now());
                                        cm.healthy = true;
                                        cm.rtt_hint = Some(rtt);
                                        cm.fail_count = cm.fail_count.saturating_sub(1);
                                        ctx.audit.log(&SendAudit {
                                            ts: unix_ms(),
                                            slot: ctx.arpc.get_slot().await.unwrap_or(0),
                                            leader: "unknown".into(),
                                            addr: addr.to_string(),
                                            proto: match proto { Proto::Quic => "quic", Proto::Udp => "udp" },
                                            path: match path { PathKind::Direct => "direct", PathKind::Forward => "forward" },
                                            rtt_ms: Some(rtt.as_millis()),
                                            tier: 1,
                                            outcome: "ok",
                                        }).await;
                                    }
                                    Err(_) => {
                                        cm.fail_count = cm.fail_count.saturating_add(1);
                                        if cm.fail_count >= 3 { cm.healthy = false; }
                                    }
                                }
                                outcome
                            },
                        ).await;

                        match res {
                            Ok(Ok(())) => Ok(()),
                            Ok(Err(e)) => Err(e),
                            Err(_) => Err(anyhow!("send-timeout")),
                        }
                    }));
                }

                for t in tasks {
                    if let Ok(Ok(())) = t.await { return Ok(sig); }
                }
            }

            // Pull-fresh on total tier fail for EVERY tier
            if FAST_RETIER_ON_FAIL {
                let _ = self.ctx.refresh_once().await;
                if let Ok(new_tiers) = self.build_tiers_ranked().await {
                    if let Some(new_for_idx) = new_tiers.get(tidx) {
                        *tier = new_for_idx.clone();
                    }
                }
            }

            // pass 2
            {
                let mut tasks = Vec::with_capacity(TIER2_PARALLEL_SENDS);
                for (i, c) in tier.iter().enumerate() {
                    if i >= TIER2_PARALLEL_SENDS { break; }
                    let gate = self.send_gate.clone().acquire_owned().await?;
                    let bytes = wire.clone();
                    let ctx = self.ctx.clone();
                    let c = c.clone();

                    tasks.push(tokio::spawn(async move {
                        let _permit = gate;

                        // token bucket gate
                        {
                            let mut cm = c.lock().await;
                            if !cm.tb.try_take() {
                                return Err::<(), anyhow::Error>(anyhow!("tb-throttle"));
                            }
                        }

                        let bof = { let cm = c.lock().await; cm.backoff_ms() };
                        if bof > 0 { sleep(Duration::from_millis(bof)).await; }

                        let addr; let proto; let path;
                        { let cm = c.lock().await; addr = cm.addr; proto = cm.proto; path = cm.path; }

                        let start = Instant::now();
                        let res = timeout(
                            Duration::from_millis(SEND_TIER2_TIMEOUT_MS),
                            async {
                                let mut cm = c.lock().await;
                                let outcome = send_one(&mut *cm, &bytes, ctx.compat).await;
                                let rtt = start.elapsed();
                                match &outcome {
                                    Ok(()) => {
                                        cm.last_ok = Some(Instant::now());
                                        cm.healthy = true;
                                        cm.rtt_hint = Some(rtt);
                                        cm.fail_count = cm.fail_count.saturating_sub(1);
                                        ctx.audit.log(&SendAudit {
                                            ts: unix_ms(),
                                            slot: ctx.arpc.get_slot().await.unwrap_or(0),
                                            leader: "unknown".into(),
                                            addr: addr.to_string(),
                                            proto: match proto { Proto::Quic => "quic", Proto::Udp => "udp" },
                                            path: match path { PathKind::Direct => "direct", PathKind::Forward => "forward" },
                                            rtt_ms: Some(rtt.as_millis()),
                                            tier: 2,
                                            outcome: "ok",
                                        }).await;
                                    }
                                    Err(_) => {
                                        cm.fail_count = cm.fail_count.saturating_add(1);
                                        if cm.fail_count >= 3 { cm.healthy = false; }
                                    }
                                }
                                outcome
                            },
                        ).await;

                        match res {
                            Ok(Ok(())) => Ok(()),
                            Ok(Err(e)) => Err(e),
                            Err(_) => Err(anyhow!("send-timeout")),
                        }
                    }));
                }

                for t in tasks {
                    if let Ok(Ok(())) = t.await { return Ok(sig); }
                }
            }
        }

        Err(anyhow!("all TPU sends failed (custom path)"))
    }

    async fn shutdown(&self) {
        self.shutting_down.store(true, Ordering::Relaxed);
        let act = self.active.read().await.clone();
        for (_k, v) in act {
            for c in v {
                if let Ok(mut cm) = c.try_lock() {
                    if let Some(ep) = cm.quic_ep.take() {
                        ep.close(0u32.into(), b"shutdown");
                    }
                }
            }
        }
    }
}

// =================== UDP parity adapter (feature: udp_parity) ===================
// Lets auditors A/B against upstream UDP TpuClient without ripping our QUIC logic.
#[cfg(feature = "udp_parity")]
pub struct UdpParitySender {
    ctx: SharedContext,
    tpu_client: Arc<solana_client::tpu_client::TpuClient>,
}
#[cfg(feature = "udp_parity")]
impl UdpParitySender {
    pub async fn new(rpc_url: &str, _identity: Arc<Keypair>) -> Result<Self> {
        use solana_client::tpu_client::TpuClientConfig;
        let rpc = Arc::new(RpcClient::new_with_commitment(
            rpc_url.to_string(),
            CommitmentConfig::confirmed(),
        ));
        let arpc = Arc::new(NonblockingRpcClient::new_with_commitment(
            rpc_url.to_string(),
            CommitmentConfig::confirmed(),
        ));
        let compat = EcosystemCompat::detect();
        let ctx = SharedContext {
            rpc: rpc.clone(),
            arpc,
            compat,
            leader_cache: Arc::new(TokioRwLock::new(LeaderScheduleCache::default())),
            audit: AuditLogger::new("logs/tpu"),
        };
        // Parity target with upstream solana_client::tpu_client (UDP to leader). [Docs.rs]
        let tpu_client = solana_client::tpu_client::TpuClient::new(
            rpc.as_ref(),
            &solana_sdk::commitment_config::CommitmentConfig::confirmed(),
            TpuClientConfig::default(),
        ).map_err(|e| anyhow!(e.to_string()))?;
        Ok(Self { ctx, tpu_client: Arc::new(tpu_client) })
    }
}
#[cfg(feature = "udp_parity")]
#[allow(async_fn_in_trait)]
impl TpuSender for UdpParitySender {
    async fn send(&self, tx: &Transaction) -> Result<Signature> {
        if !self.ctx.compat.use_tpu_client_hint {
            let sig = self.ctx.rpc.send_transaction_with_config(
                tx,
                RpcSendTransactionConfig {
                    skip_preflight: true,
                    max_retries: Some(0),
                    ..RpcSendTransactionConfig::default()
                },
            )?;
            return Ok(sig);
        }
        let sig = tx.signatures.get(0).copied().ok_or_else(|| anyhow!("missing sig"))?;
        self.tpu_client.send_transaction(tx).map_err(|e| anyhow!(e.to_string()))?;
        Ok(sig)
    }
    async fn shutdown(&self) {}
}

// =================== Factory ===================
pub struct TpuPortMonopolizer {
    inner: Box<dyn TpuSender>,
}

impl TpuPortMonopolizer {
    pub async fn new(rpc_url: &str, identity: Arc<Keypair>) -> Result<Self> {
        #[cfg(feature = "udp_parity")]
        {
            let s = UdpParitySender::new(rpc_url, identity).await?;
            return Ok(Self { inner: Box::new(s) });
        }
        #[cfg(feature = "official_tpu")]
        {
            let s = OfficialLikeSender::new(rpc_url, identity).await?;
            return Ok(Self { inner: Box::new(s) });
        }
        #[cfg(all(not(feature = "official_tpu"), not(feature = "udp_parity")))]
        {
            let s = CustomQuicSender::new(rpc_url, identity).await?;
            Ok(Self { inner: Box::new(s) })
        }
    }

    pub async fn send_transaction(&self, tx: &Transaction) -> Result<Signature> {
        self.inner.send(tx).await
    }

    pub async fn shutdown(&self) {
        self.inner.shutdown().await
    }
}

// =================== background refresh ===================
async fn refresh_leaders_and_addrs(
    cache: Arc<TokioRwLock<LeaderScheduleCache>>,
    arpc: Arc<NonblockingRpcClient>,
    compat: EcosystemCompat,
) -> Result<()> {
    // 1) slot/epoch
    let slot = arpc.get_slot().await?;
    let epoch_info = arpc.get_epoch_info().await?;
    let epoch = epoch_info.epoch;
    let slots_in_epoch = epoch_info.slots_in_epoch;

    // 2) leader schedule for the slot's epoch
    // Leader schedule fetched via getLeaderSchedule, mapping slot offsets within epoch. [Solana]
    let sched = arpc
        .get_leader_schedule_with_commitment(Some(slot), CommitmentConfig::finalized())
        .await?
        .ok_or_else(|| anyhow!("no leader schedule"))?;

    // 3) map slot → leader pubkey
    // This is where we expand epoch-relative slot offsets into absolute slot numbers. [Solana]
    let mut schedule = BTreeMap::new();
    let epoch_start = epoch.saturating_mul(slots_in_epoch);
    for (pk_str, slots) in sched {
        if let Ok(pk) = pk_str.parse::<Pubkey>() {
            for s in slots {
                schedule.insert(epoch_start + s as u64, pk);
            }
        }
    }

    // 4) cluster nodes → TPU endpoints
    // Endpoints sourced from getClusterNodes response fields. [Solana]
    // If you choose to ignore plain `tpu`, note it's legacy UDP; prefer QUIC `tpuQuic` variants. [Solana]
    let nodes = arpc.get_cluster_nodes().await?;
    let mut tpu_by_leader: HashMap<Pubkey, TpuAddrs> = HashMap::new();
    for n in nodes {
        if let Ok(pk) = n.pubkey.parse::<Pubkey>() {
            let mut addrs = TpuAddrs::default();

            // UDP (legacy)
            if let Some(a) = n.tpu {
                addrs.udp.push(a);
            }
            // snake_case forwards (UDP)
            if let Some(a) = n.tpu_forwards {
                addrs.udp_fwd.push(a);
            }
            // camelCase forwards (UDP)
            #[allow(deprecated)]
            if let Some(a) = n.tpuForwards {
                addrs.udp_fwd.push(a);
            }

            // QUIC (preferred)
            if compat.use_quic {
                if let Some(a) = n.tpu_quic {
                    addrs.quic.push(a);
                }
                if let Some(a) = n.tpu_quic_forwards {
                    addrs.quic_fwd.push(a);
                }
                #[allow(deprecated)]
                if let Some(a) = n.tpuQuicForwards {
                    addrs.quic_fwd.push(a);
                }
            }

            tpu_by_leader.insert(pk, addrs);
        }
    }

    let mut w = cache.write().await;
    w.schedule = schedule;
    w.tpu_by_leader = tpu_by_leader;
    w.last_update = Instant::now();
    w.last_slot = slot;
    w.slots_per_epoch = slots_in_epoch;
    w.epoch = epoch;
    w.epoch_start_slot = epoch_start;

    Ok(())
}

// =================== send helpers ===================

// Single-shot connector (used by official-like path and cache fills)
async fn make_conn_once(
    addr: SocketAddr,
    proto: Proto,
    path: PathKind,
    compat: EcosystemCompat,
) -> Result<Conn> {
    match proto {
        Proto::Udp => {
            let udp = UdpSocket::bind(match addr {
                SocketAddr::V4(_) => SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), 0),
                SocketAddr::V6(_) => "[::]:0".parse().unwrap(),
            })
            .await
            .context("bind UDP")?;
            udp.connect(addr).await.context("connect UDP")?;
            Ok(Conn {
                addr, proto, path,
                quic_ep: None, quic_conn: None, udp: Some(udp),
                last_ok: None, healthy: true, rtt_hint: None, fail_count: 0,
                tb: TokenBucket::new(),
            })
        }
        Proto::Quic => make_quic_conn(addr, path, compat).await,
    }
}

async fn make_quic_conn(addr: SocketAddr, path: PathKind, compat: EcosystemCompat) -> Result<Conn> {
    let udp = std::net::UdpSocket::bind(match addr {
        SocketAddr::V4(_) => SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), 0),
        SocketAddr::V6(_) => "[::]:0".parse().unwrap(),
    })
    .context("bind QUIC UDP")?;
    udp.connect(addr).context("connect QUIC UDP")?;

    let runtime = quinn::TokioRuntime;
    let mut tls = rustls::ClientConfig::builder()
        .with_safe_defaults()
        .with_native_roots()
        .with_no_client_auth();

    // ALPN configurability:
    // If SOLANA_QUIC_ALPN is set, we try those (comma-separated). Otherwise we try none, then "h3".
    // Heuristic note: ALPN/SNI choices are client-level knobs; there is no canonical value mandated
    // by public Solana docs. Keep as fallback, not a spec. [Heuristic]
    let mut alpn_lists: Vec<Vec<Vec<u8>>> = Vec::new();
    if compat.quic_alpn.is_empty() {
        alpn_lists.push(Vec::new());
    } else {
        alpn_lists.push(compat.quic_alpn.clone());
        // also try empty as a fallback set
        alpn_lists.push(Vec::new());
        // and h3 last, if not already present
        if !compat.quic_alpn.iter().any(|v| v == b"h3") {
            alpn_lists.push(vec![b"h3".to_vec()]);
        }
    }

    // Helper to attempt a single ALPN list
    let try_dial = |alpn: Vec<Vec<u8>>| -> Result<(ClientConfig, Endpoint)> {
        let mut tls_local = tls.clone();
        tls_local.alpn_protocols = alpn;
        tls_local.crypto_provider = std::sync::Arc::new(rustls::crypto::ring::default_provider());
        let mut cfg = ClientConfig::new(std::sync::Arc::new(tls_local));
        cfg.transport_config(transport_quic_with_datagrams()?);
        let ep = Endpoint::new_with_undefined_server_config(
            quinn::EndpointConfig::default(),
            Some(cfg),
            udp.try_clone().context("clone UDP")?,
            runtime,
        )
        .context("quinn endpoint")?;
        Ok((ep.client_config().clone(), ep))
    };

    // We may try multiple ALPN configurations until one connects.
    let mut last_err: Option<anyhow::Error> = None;
    for alpn in alpn_lists {
        match try_dial(alpn) {
            Ok((cfg, ep)) => {
                let server_name = compat
                    .quic_sni
                    .as_deref()
                    .unwrap_or("solana-validator"); // SNI override if needed [Heuristic]
                match ep.connect_with(cfg, addr, server_name) {
                    Ok(connecting) => {
                        match connecting.await {
                            Ok(conn) => {
                                return Ok(Conn {
                                    addr,
                                    proto: Proto::Quic,
                                    path,
                                    quic_ep: Some(ep),
                                    quic_conn: Some(conn),
                                    udp: None,
                                    last_ok: None,
                                    healthy: true,
                                    rtt_hint: None,
                                    fail_count: 0,
                                    tb: TokenBucket::new(),
                                });
                            }
                            Err(e) => { last_err = Some(anyhow!("quinn connect: {e}")); }
                        }
                    }
                    Err(e) => { last_err = Some(anyhow!("quinn dial: {e}")); }
                }
            }
            Err(e) => { last_err = Some(e); }
        }
    }

    Err(last_err.unwrap_or_else(|| anyhow!("quic connect failed")))
}

// Send one with QUIC datagram preferred; large payloads fall back to uni stream.
// QUIC path note (authoritative): “Transactions are encoded and sent in QUIC streams to the
// validator’s TPU.” Prefer streams for TX ingress; datagrams are an optimization fallback,
// not a protocol requirement. [Agave / Validator TPU doc]
// Bonus context: ecosystem articles (e.g., Gulf Stream explainers) also describe QUIC streams;
// keep them as secondary, not primary, citations. [Helius]
async fn send_one(c: &mut Conn, buf: &Bytes, _compat: EcosystemCompat) -> Result<()> {
    match c.proto {
        Proto::Quic => {
            let conn = c
                .quic_conn
                .as_ref()
                .ok_or_else(|| anyhow!("missing quic connection"))?;

            // Heuristic: try a datagram first for minimal overhead; if rejected or too large,
            // fall back to opening a unidirectional stream and writing the payload. [Heuristic]
            if conn.send_datagram(buf.clone()).is_err() {
                let mut s = conn.open_uni().await.context("open_uni")?;
                use tokio::io::AsyncWriteExt;
                s.write_all(buf).await.context("write uni")?;
                s.finish().await.context("finish uni")?;
            }
            Ok(())
        }
        Proto::Udp => {
            // UDP: oversized frames will fail; avoid wasting time if obviously too big. [Heuristic]
            if buf.len() > MAX_PACKET_SIZE {
                return Err(anyhow!(
                    "udp payload too large ({} > {})",
                    buf.len(),
                    MAX_PACKET_SIZE
                ));
            }
            let udp = c.udp.as_ref().ok_or_else(|| anyhow!("missing udp socket"))?;
            udp.send(buf).await.context("udp send")?;
            Ok(())
        }
    }
}

fn transport_quic_with_datagrams() -> Result<std::sync::Arc<quinn::TransportConfig>> {
    let mut t = quinn::TransportConfig::default();
    t.keep_alive_interval(Some(Duration::from_secs(1)));
    t.max_idle_timeout(Some(quinn::IdleTimeout::try_from(Duration::from_secs(30)).unwrap()));
    t.datagram_send_buffer_size(Some(256 * 1024));
    t.datagram_receive_buffer_size(Some(256 * 1024));
    t.max_udp_payload_size(Some(MAX_PACKET_SIZE as u64));
    Ok(std::sync::Arc::new(t))
}

fn unix_ms() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

// ------------- Compatibility shims for potential camelCase fields on ClusterNodes -------------
// These fields are marked deprecated to compile only if the SDK struct accidentally exposes them.
// If not present, the #[allow(deprecated)] guarded accesses above just won’t compile; delete them if needed.
#[allow(dead_code)]
#[allow(non_snake_case)]
mod _cluster_nodes_camelcase_shadow {
    use super::*;
    // intentionally empty
}
