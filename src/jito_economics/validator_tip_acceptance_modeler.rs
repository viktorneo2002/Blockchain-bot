// jito_adapter.rs — Auditor-grade Jito adapter (gRPC send + BE JSON-RPC status + WS tip stream)
// Purpose: private-flow bundle send via relayer gRPC, then model “tip acceptance” using:
//   1) Block Engine inflight status (5m TTL; null ⇒ not found yet),
//   2) optional on-chain finality confirm,
//   3) a gRPC results stream to short-circuit polling when BE already decided,
//   4) live WS tip-floor telemetry as the primary prior for the model,
//   5) priority-fee CU price sampler fused into the tip chooser.
//
// Doc pins (keep these tiny at call-sites):
// - Bundles: max 5 tx; sequential & atomic; execute within the same slot (single-slot). (Jito “Low Latency Tx Send”)
// - Auctions: ~50 ms tick cadence; jitter polling to avoid phase lock. (Jito docs)
// - JSON-RPC Authentication: include `x-jito-auth: <uuid/key>` when required. (Jito docs)
// - Tip floors (GLOBAL): REST https://bundles.jito.wtf/api/v1/bundles/tip_floor, WS wss://bundles.jito.wtf/api/v1/bundles/tip_stream (Jito docs)
// - Inflight TTL: ~5 minutes; result:null ⇒ “not found yet”. (Jito docs)
// - Rate limits (default): ~1 rps per region per IP; rotate on 429/5xx with jitter. (Jito docs)
// - CU pricing: Solana RPC `getRecentPrioritizationFees` returns micro-lamports per CU snapshots. (Solana docs)

#![allow(dead_code)]

use std::{
    collections::{HashMap, HashSet, VecDeque},
    sync::Arc,
    time::{Duration, Instant},
};

use anyhow::{anyhow, Context, Result};
use futures::StreamExt;
use http::HeaderMap;
use once_cell::sync::Lazy;
use prometheus::{register_counter, register_gauge, register_gauge_vec, Counter, Gauge, GaugeVec};
use reqwest::{header::HeaderValue, Client as HttpClient, StatusCode};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use tokio::{sync::mpsc, task::JoinHandle, time::sleep};
use tokio::sync::Mutex;
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message;
use tonic::{
    codegen::InterceptedService,
    metadata::{AsciiMetadataKey, AsciiMetadataValue},
    service::Interceptor,
    transport::{Channel, ClientTlsConfig, Endpoint},
    Request, Status,
};

// ===== gRPC protos (pin this crate version/commit in Cargo for auditor traceability) =====
// Official repo: https://github.com/jito-labs/mev-protos
use jito_relayer_protos::searcher::searcher_service_client::SearcherServiceClient;
use jito_relayer_protos::searcher::{
    send_bundle_request::Bundle as ProtoBundle,
    subscribe_bundle_results_request::Filter as ResultsFilter,
    BundleResult, SendBundleRequest, SendBundleResponse, SubscribeBundleResultsRequest,
};

// logging
use tracing::{debug, error, info, warn};

// ===== Repo/commit provenance pins (must match Cargo.toml `rev`) =====
const MEV_PROTOS_REPO: &str = "https://github.com/jito-labs/mev-protos";
/// Pinned to a commit that includes current JSON-RPC paths and relayer schemas.
/// Auditor note: Cargo.toml `jito-relayer-protos` git dep must mirror this `rev`.
const MEV_PROTOS_COMMIT: &str = "46ead86a13a55a0ef2c139db96a8ee93bf7505e3"; // 2025-07-23
const JITO_JS_RPC_REPO: &str = "https://github.com/jito-labs/jito-js-rpc";

// ========================== Metrics (Prometheus) ==========================
static MET_WS_FRESHNESS_SECONDS: Lazy<Gauge> =
    Lazy::new(|| register_gauge!("jito_ws_tip_freshness_seconds", "Seconds since last WS tip msg").unwrap());
static MET_TIP_REST_FALLBACKS: Lazy<Counter> =
    Lazy::new(|| register_counter!("jito_tip_rest_fallbacks_total", "Count of REST tip_floor fallbacks").unwrap());
static MET_TIP_STALE_DENIED: Lazy<Counter> =
    Lazy::new(|| register_counter!("jito_tip_stale_denied_total", "Sends denied due to stale tip telemetry").unwrap());
static MET_REGION_PRIOR: Lazy<GaugeVec> = Lazy::new(|| {
    register_gauge_vec!(
        "jito_region_prior_score",
        "Region prior score (EMA fuse of landed/failed)",
        &["region"]
    ).unwrap()
});

// ========================== Config ==========================

#[derive(Clone, Debug)]
pub enum GrpcBundleEncoding {
    Bytes,   // transactions: Vec<Vec<u8>> (protobuf bytes)
    Base64,  // transactions carried as base64-encoded strings inside proto
}

#[derive(Clone, Debug)]
pub struct JitoConfig {
    /// BE JSON-RPC hosts (choose region by host; no "region" in JSON body)
    /// Example regional hosts (pin current official list in your ops docs):
    ///   https://mainnet.block-engine.jito.wtf
    ///   https://ny.mainnet.block-engine.jito.wtf
    ///   https://frankfurt.mainnet.block-engine.jito.wtf
    pub be_hosts: Vec<String>,

    /// Optional API key/token for BE JSON-RPC.
    /// Auth header at call-sites: x-jito-auth: <token>  (docs “Authentication”).
    pub be_api_key: Option<String>,

    /// Relayer gRPC endpoints. Example: ["https://mainnet.block-engine.jito.wtf:443"]
    pub relayer_grpc_urls: Vec<String>,

    /// Optional gRPC metadata header key/value (for deployments that require it).
    pub relayer_meta_header: Option<(String, String)>,

    /// HTTP keepalive seconds
    pub http_keepalive_secs: u64,

    /// JSON-RPC per-request timeout (ms)
    pub http_timeout_ms: u64,

    /// gRPC per-request deadline (ms). Near an auction slice (~50ms) for sends.
    pub grpc_send_timeout_ms: u64,

    /// How often to refresh tip accounts (seconds) if WS is unavailable
    pub tip_accounts_refresh_secs: u64,

    /// Prefer WS tip stream over REST tip_floor. Fallback to REST if WS fails.
    pub use_ws_tip_stream: bool,

    /// If REST fallback is used, call GLOBAL tip_floor endpoint (bundles.jito.wtf)
    pub use_rest_tip_floor: bool,

    /// Encoding used by your gRPC proto; safest rule is “bytes by default.”
    pub grpc_bundle_encoding: GrpcBundleEncoding,

    /// Optional: override for GLOBAL WS tip stream (defaults to bundles.jito.wtf)
    pub tip_stream_override: Option<String>,

    /// Optional: override for GLOBAL REST tip floor (defaults to bundles.jito.wtf)
    pub tip_rest_override: Option<String>,

    /// WS staleness cutoff in seconds. If WS older than this, fallback to REST immediately.
    pub ws_staleness_cutoff_secs: u64,

    /// Enforce Jito’s documented minimum tip (1000 lamports) before sendBundle.
    pub min_tip_lamports: u64,

    /// Backpressure: refuse new sends if *both* WS and REST are stale beyond this SLO window,
    /// unless override is enabled. Saves SOL during telemetry brownouts.
    pub deny_sends_when_tip_stale: bool,
    pub tip_stale_backpressure_secs: u64,

    /// Auto-adjust tip upward using WS p50 EMA fused with `getRecentPrioritizationFees`.
    pub auto_adjust_tip: bool,

    /// Default CU estimate if sampler present but no per-bundle CU known.
    pub default_cu_estimate: u32,
}

impl Default for JitoConfig {
    fn default() -> Self {
        Self {
            be_hosts: vec!["https://mainnet.block-engine.jito.wtf".to_string()],
            be_api_key: None,
            relayer_grpc_urls: vec!["https://mainnet.block-engine.jito.wtf:443".to_string()],
            relayer_meta_header: None,
            http_keepalive_secs: 30,
            http_timeout_ms: 1200,
            grpc_send_timeout_ms: 65,
            tip_accounts_refresh_secs: 300,
            use_ws_tip_stream: true,
            use_rest_tip_floor: true,
            grpc_bundle_encoding: GrpcBundleEncoding::Bytes, // bytes-first per proto
            tip_stream_override: None,
            tip_rest_override: None,
            ws_staleness_cutoff_secs: 5, // hard ~5s cutoff per auction cadence
            min_tip_lamports: 1000,      // Jito minimum; enforce pre-send.
            deny_sends_when_tip_stale: true,
            tip_stale_backpressure_secs: 8,
            auto_adjust_tip: true,
            default_cu_estimate: 200_000, // conservative baseline
        }
    }
}

// GLOBAL tip endpoints (per docs). Do NOT shard these by region.
const GLOBAL_TIP_FLOOR_REST: &str = "https://bundles.jito.wtf/api/v1/bundles/tip_floor";
const GLOBAL_TIP_STREAM_WS: &str = "wss://bundles.jito.wtf/api/v1/bundles/tip_stream";

// ========================== Errors ==========================

#[derive(thiserror::Error, Debug)]
pub enum TipTelemetryError {
    #[error("no fresh tip telemetry (WS stale > {ws_cutoff}s and REST disabled)")]
    Stale { ws_cutoff: u64 },
    #[error("failed to fetch tip telemetry via REST: {0}")]
    Rest(String),
}

// ========================== Interceptor ==========================

#[derive(Clone)]
struct StaticMetaInterceptor {
    kv: Option<(AsciiMetadataKey, AsciiMetadataValue)>,
}
impl Interceptor for StaticMetaInterceptor {
    fn call(&mut self, mut req: Request<()>) -> std::result::Result<Request<()>, Status> {
        if let Some((ref k, ref v)) = self.kv {
            req.metadata_mut().insert(k.clone(), v.clone());
        }
        Ok(req)
    }
}

// ========================== Token bucket (1 rps per host) ==========================
#[derive(Clone, Debug)]
struct TokenBucket {
    capacity: f64,
    tokens: f64,
    refill_per_sec: f64,
    last: Instant,
}
impl TokenBucket {
    fn new(capacity: f64, refill_per_sec: f64) -> Self {
        Self { capacity, tokens: capacity, refill_per_sec, last: Instant::now() }
    }
    fn available(&mut self) {
        let now = Instant::now();
        let dt = now.duration_since(self.last).as_secs_f64();
        self.tokens = (self.tokens + dt * self.refill_per_sec).min(self.capacity);
        self.last = now;
    }
    async fn take(&mut self, cost: f64) {
        loop {
            self.available();
            if self.tokens >= cost {
                self.tokens -= cost;
                return;
            }
            let wait_ms = 50 + fastrand::u64(0..150); // jitter
            sleep(Duration::from_millis(wait_ms)).await;
        }
    }
}

// ========================== Health EWMA per host (rotate away from hot regions) ==========================
#[derive(Clone, Debug)]
struct HostHealth {
    ewma_err: f64,        // exponentially-weighted error rate
    last_err_at: Option<Instant>,
}
impl HostHealth {
    fn new() -> Self { Self { ewma_err: 0.0, last_err_at: None } }
    fn note_err(&mut self) {
        self.ewma_err = 0.80 * self.ewma_err + 0.20; // quick bump
        self.last_err_at = Some(Instant::now());
    }
    fn note_ok(&mut self) {
        self.ewma_err *= 0.90; // decay on success
    }
}

// ========================== Tip telemetry state (WS-first) ==========================

#[derive(Debug, Clone, Default)]
pub struct TipFloor {
    pub p25: Option<f64>,
    pub p50: Option<f64>,
    pub p75: Option<f64>,
    pub p95: Option<f64>,
    pub ema: Option<f64>,
}

#[derive(Debug, Deserialize)]
struct TipFloorRestLike {
    #[serde(rename = "p25")] p25: Option<f64>,
    #[serde(rename = "p50")] p50: Option<f64>,
    #[serde(rename = "p75")] p75: Option<f64>,
    #[serde(rename = "p95")] p95: Option<f64>,
    #[serde(rename = "ema")] ema: Option<f64>,

    // drift variants supported transparently
    #[serde(rename = "landed_tips_25th_percentile")] p25_alt: Option<f64>,
    #[serde(rename = "landed_tips_50th_percentile")] p50_alt: Option<f64>,
    #[serde(rename = "landed_tips_75th_percentile")] p75_alt: Option<f64>,
    #[serde(rename = "landed_tips_95th_percentile")] p95_alt: Option<f64>,
    #[serde(rename = "ema_landed_tips_50th_percentile")] ema_alt: Option<f64>,
}
impl From<TipFloorRestLike> for TipFloor {
    fn from(v: TipFloorRestLike) -> Self {
        let pick = |a: Option<f64>, b: Option<f64>| a.or(b);
        Self {
            p25: pick(v.p25, v.p25_alt),
            p50: pick(v.p50, v.p50_alt),
            p75: pick(v.p75, v.p75_alt),
            p95: pick(v.p95, v.p95_alt),
            ema: pick(v.ema, v.ema_alt),
        }
    }
}

#[derive(Debug, Clone, Default)]
struct TipTelemetry {
    last: TipFloor,
    last_at: Option<Instant>,
    ema_alpha: f64, // 0<alpha<=1 for rolling EMA on p50
}
impl TipTelemetry {
    fn new(alpha: f64) -> Self {
        Self { ema_alpha: alpha.clamp(0.01, 1.0), ..Default::default() }
    }
    fn update_from(&mut self, incoming: TipFloor) {
        let mut merged = incoming.clone();
        if let (Some(prev), Some(now)) = (self.last.p50, incoming.p50) {
            let ema = self.ema_alpha * now + (1.0 - self.ema_alpha) * prev;
            merged.ema = Some(ema);
        } else if incoming.ema.is_none() {
            merged.ema = incoming.p50;
        }
        self.last = merged;
        self.last_at = Some(Instant::now());
    }
}

// ===== Region-scoped acceptance priors fused from results stream =====
#[derive(Debug, Clone, Default)]
struct RegionPrior {
    ema_boost: f64,      // positive when we’ve been landing
    ema_penalty: f64,    // positive when we’ve been failing/invalid
    last_update: Option<Instant>,
}
impl RegionPrior {
    fn score(&self) -> f64 {
        1.0 + self.ema_boost - self.ema_penalty
    }
    fn on_landed(&mut self) {
        self.ema_boost = 0.9 * self.ema_boost + 0.1;
        self.ema_penalty *= 0.9;
        self.last_update = Some(Instant::now());
    }
    fn on_failed(&mut self) {
        self.ema_penalty = 0.9 * self.ema_penalty + 0.1;
        self.ema_boost *= 0.9;
        self.last_update = Some(Instant::now());
    }
}

// Optional sampler to query Solana priority-fee market and blend with tip prior
#[async_trait::async_trait]
pub trait PriorityFeeSampler: Send + Sync {
    /// Return a suggested microLamports per CU price sampled from
    /// `getRecentPrioritizationFees` or equivalent.
    async fn sample_microlamports_per_cu(&self) -> Result<u64>;
}

// A concrete sampler using solana-client
pub struct RpcPriorityFeeSampler {
    rpc: solana_client::nonblocking::rpc_client::RpcClient,
}
impl RpcPriorityFeeSampler {
    pub fn new(url: &str) -> Self {
        Self { rpc: solana_client::nonblocking::rpc_client::RpcClient::new(url.to_string()) }
    }
}
#[async_trait::async_trait]
impl PriorityFeeSampler for RpcPriorityFeeSampler {
    async fn sample_microlamports_per_cu(&self) -> Result<u64> {
        // Solana docs: getRecentPrioritizationFees caches ~150 blocks. We’ll take the p50 of recent.
        // https://solana.com/docs/rpc/http/getrecentprioritizationfees
        let fees = self.rpc.get_recent_prioritization_fees(None).await?;
        // Simple robust pick: median of non-zero samples, else zero.
        let mut v: Vec<u64> = fees.into_iter().filter_map(|f| f.prioritization_fee).filter(|x| *x > 0).collect();
        if v.is_empty() { return Ok(0); }
        v.sort_unstable();
        Ok(v[v.len() / 2])
    }
}

// ========================== Adapter ==========================

pub struct JitoSender {
    // Pools
    grpc_pool: VecDeque<SearcherServiceClient<InterceptedService<Channel, StaticMetaInterceptor>>>,
    http_hosts: Vec<(String, HttpClient)>,
    be_api_bases: Vec<String>,

    // Rate-limiters aligned with http_hosts
    host_buckets: Vec<TokenBucket>,
    host_health: Vec<HostHealth>,

    // Round-robin indices
    http_rr: usize,
    grpc_rr: usize,

    // Tip accounts and WS telemetry
    tip_accounts: HashSet<String>,
    tip_accounts_fresh_at: Instant,
    tip_accounts_ttl: Duration,

    // Shared telemetry
    tip_telemetry: Arc<Mutex<TipTelemetry>>,
    tip_ws_task: Option<JoinHandle<()>>,
    tip_ws_last_alive: Arc<Mutex<Option<Instant>>>,

    // Terminal results from subscribe stream for short-circuiting polls
    // bundle_id -> ("Landed"|"Failed"|"Invalid", region_opt)
    terminal_by_bundle: HashMap<String, (String, Option<String>)>,

    // Region coherence (host-level) for ~5m (for your logging/affinity decisions)
    inflight_regions: HashMap<String, (String, Instant)>,

    // Region priors fused from stream outcomes
    region_priors: HashMap<String, RegionPrior>,

    // Optional priority fee sampler hook
    priority_fee_sampler: Option<Arc<dyn PriorityFeeSampler>>,

    // Dual-ID cache (uuid <-> bundle_id)
    uuid_to_bundle: HashMap<String, String>,
    bundle_to_uuid: HashMap<String, String>,

    // Config snapshot
    cfg: JitoConfig,
}

impl JitoSender {
    /// Connect, warm tip accounts, and start WS tip stream if enabled.
    pub async fn connect(cfg: JitoConfig) -> Result<Self> {
        // gRPC pool
        let mut grpc_pool = VecDeque::new();
        for url in &cfg.relayer_grpc_urls {
            let endpoint = Endpoint::from_shared(url.clone())?
                .tls_config(ClientTlsConfig::new())?;
            let channel = endpoint.connect().await?;
            let interceptor = if let Some((k, v)) = cfg.relayer_meta_header.clone() {
                let k = AsciiMetadataKey::from_bytes(k.as_bytes())
                    .map_err(|e| anyhow!("invalid relayer metadata key: {e}"))?;
                let v = AsciiMetadataValue::try_from(v)
                    .map_err(|e| anyhow!("invalid relayer metadata value: {e}"))?;
                StaticMetaInterceptor { kv: Some((k, v)) }
            } else {
                StaticMetaInterceptor { kv: None }
            };
            grpc_pool.push_back(SearcherServiceClient::with_interceptor(channel, interceptor));
        }
        if grpc_pool.is_empty() {
            return Err(anyhow!("no relayer_grpc_urls provided"));
        }

        // HTTP clients per host
        let mut http_hosts = Vec::new();
        let mut be_api_bases = Vec::new();
        for host in &cfg.be_hosts {
            let mut default_headers = HeaderMap::new();
            if let Some(api_key) = cfg.be_api_key.as_ref() {
                // Auth header provenance: Jito “Authentication” section shows x-jito-auth.
                default_headers.insert(
                    "x-jito-auth",
                    HeaderValue::from_str(api_key).map_err(|e| anyhow!("invalid x-jito-auth header: {e}"))?,
                );
                // Fallback for older infra that still honors X-API-KEY.
                let _ = default_headers.insert("X-API-KEY", HeaderValue::from_str(api_key).unwrap_or(HeaderValue::from_static("invalid")));
            }
            let http = HttpClient::builder()
                .default_headers(default_headers)
                .tcp_keepalive(Duration::from_secs(cfg.http_keepalive_secs))
                .pool_max_idle_per_host(8)
                .timeout(Duration::from_millis(cfg.http_timeout_ms))
                .build()?;

            http_hosts.push((host.clone(), http));
            be_api_bases.push(format!("{}/api/v1", host.trim_end_matches('/')));
        }

        // 1 rps default per BE host (Jito default limits)
        let host_buckets = (0..http_hosts.len()).map(|_| TokenBucket::new(1.0, 1.0)).collect::<Vec<_>>();
        let host_health = (0..http_hosts.len()).map(|_| HostHealth::new()).collect::<Vec<_>>();

        // Shared telemetry state
        let tip_telemetry = Arc::new(Mutex::new(TipTelemetry::new(0.25)));
        let tip_ws_last_alive = Arc::new(Mutex::new(None));

        let mut s = Self {
            grpc_pool,
            http_hosts,
            be_api_bases,
            host_buckets,
            host_health,
            http_rr: 0,
            grpc_rr: 0,
            tip_accounts: HashSet::new(),
            tip_accounts_fresh_at: Instant::now() - Duration::from_secs(10_000),
            tip_accounts_ttl: Duration::from_secs(cfg.tip_accounts_refresh_secs),
            tip_telemetry: tip_telemetry.clone(),
            tip_ws_task: None,
            tip_ws_last_alive: tip_ws_last_alive.clone(),
            terminal_by_bundle: HashMap::new(),
            inflight_regions: HashMap::new(),
            region_priors: HashMap::new(),
            priority_fee_sampler: None,
            uuid_to_bundle: HashMap::new(),
            bundle_to_uuid: HashMap::new(),
            cfg,
        };

        // Warm-load tip accounts
        s.refresh_tip_accounts().await
            .context("initial getTipAccounts")?;

        // Start WS tip stream as primary telemetry if enabled; fallback stays available.
        if s.cfg.use_ws_tip_stream {
            s.spawn_tip_ws_supervisor().await?;
        }

        Ok(s)
    }

    /// Optional: install a priority-fee sampler (e.g., pulls getRecentPrioritizationFees)
    pub fn set_priority_fee_sampler(&mut self, sampler: Arc<dyn PriorityFeeSampler>) {
        self.priority_fee_sampler = Some(sampler);
    }

    // ================== Tip chooser ==================
    /// Suggest tip by fusing WS p50 EMA with CU price from getRecentPrioritizationFees.
    /// If sampler is stale/unavailable, fall back to WS p50/ema; always respect min tip.
    async fn suggest_bundle_tip_lamports(&self, est_cu: Option<u32>) -> u64 {
        let min = self.cfg.min_tip_lamports.max(1000);
        let tt = self.tip_telemetry.lock().await;
        let p50 = tt.last.ema.or(tt.last.p50).unwrap_or(min as f64) as u64;

        // CU path
        let cu_price_ulam_per_cu = match &self.priority_fee_sampler {
            Some(s) => s.sample_microlamports_per_cu().await.unwrap_or(0),
            None => 0,
        };
        let cu_est = est_cu.unwrap_or(self.cfg.default_cu_estimate) as u64;
        let cu_fee_lamports = cu_price_ulam_per_cu.saturating_mul(cu_est) / 1_000_000;

        // Simple robust fusion: max(p50_ema, cu_fee), bounded by some sanity multiple of p95 if present
        let mut rec = p50.max(cu_fee_lamports).max(min);
        if let Some(p95) = tt.last.p95.map(|v| v as u64) {
            let cap = p95.saturating_mul(2); // don’t go wild if CU quotes spike
            if rec > cap { rec = cap; }
        }
        rec
    }

    /// Enforce min tip and optionally auto-adjust upward to suggested tip.
    async fn enforce_min_and_auto_tip(&self, mut tip_lamports: u64) -> Result<u64> {
        let min = self.cfg.min_tip_lamports.max(1000);
        if tip_lamports < min { tip_lamports = min; }
        if self.cfg.auto_adjust_tip {
            let rec = self.suggest_bundle_tip_lamports(None).await;
            if tip_lamports < rec {
                warn!(given=%tip_lamports, recommended=%rec, "auto-increasing tip to meet market conditions");
                tip_lamports = rec;
            }
        }
        Ok(tip_lamports)
    }

    // ================== gRPC: send (UUID) ==================
    /// gRPC sendBundle (proto: SearcherService::send_bundle)
    /// Bundles are max 5 tx; executed sequentially & atomically within one slot (single-slot). (Jito docs)
    pub async fn send_bundle_grpc(&mut self, txs: &[Vec<u8>], bundle_tip_lamports: u64) -> Result<String> {
        if txs.is_empty() { return Err(anyhow!("empty bundle")); }
        if txs.len() > 5 { return Err(anyhow!("gRPC bundle has {} txs; max is 5", txs.len())); }

        self.ensure_tip_freshness_for_send().await?;

        // Min + auto tip
        let adjusted_tip = self.enforce_min_and_auto_tip(bundle_tip_lamports).await?;

        let proto_bundle = match self.cfg.grpc_bundle_encoding {
            GrpcBundleEncoding::Bytes => ProtoBundle {
                transactions: txs.iter().cloned().collect(),
                ..Default::default()
            },
            GrpcBundleEncoding::Base64 => {
                let b64s: Vec<Vec<u8>> = txs.iter().map(|b| base64::encode(b).into_bytes()).collect();
                ProtoBundle { transactions: b64s, ..Default::default() }
            }
        };

        let mut req = SendBundleRequest { bundle: Some(proto_bundle), ..Default::default() };
        // Tip transfer lives inside txs → tip accounts; no mutation here.

        for attempt in 0..self.grpc_pool.len() {
            let idx = (self.grpc_rr + attempt) % self.grpc_pool.len();
            let client = self.grpc_pool.get_mut(idx).expect("pool index");
            let mut r = Request::new(req.clone());

            let deadline = std::time::SystemTime::now() + Duration::from_millis(self.cfg.grpc_send_timeout_ms);
            r.set_deadline(deadline);

            match client.send_bundle(r).await {
                Ok(res) => {
                    self.grpc_rr = (idx + 1) % self.grpc_pool.len();
                    let SendBundleResponse { uuid, .. } = res.into_inner();
                    let uuid = uuid.ok_or_else(|| anyhow!("jito send_bundle: missing uuid in response"))?;
                    info!(%uuid, tip_lamports=%adjusted_tip, "Jito gRPC send_bundle OK (uuid)");
                    // Record uuid; bundle_id may arrive later via JSON or result stream.
                    self.uuid_to_bundle.entry(uuid.clone()).or_insert_with(|| "".into());
                    return Ok(uuid);
                }
                Err(status) => {
                    warn!(code=?status.code(), relay=?self.relayer_grpc_urls.get(idx), "gRPC send_bundle error; rotating relayer");
                    match status.code() {
                        tonic::Code::Unavailable | tonic::Code::DeadlineExceeded => continue,
                        _ => return Err(anyhow!("gRPC send_bundle error: {status}")),
                    }
                }
            }
        }
        Err(anyhow!("all gRPC relayers failed for send_bundle"))
    }

    // =========== gRPC: subscribe results (short-circuit + priors fusion) ===========
    pub async fn subscribe_bundle_results(
        &mut self,
        filter_region: Option<String>,
        capacity: usize,
    ) -> Result<(JoinHandle<()>, mpsc::Receiver<BundleResult>)> {
        let idx = self.grpc_rr % self.grpc_pool.len();
        let client = self.grpc_pool.get_mut(idx).expect("pool index");

        let filter = ResultsFilter {
            bundle_id_prefix: vec![],
            region: filter_region.clone().unwrap_or_default(),
        };

        let request = SubscribeBundleResultsRequest { filter: Some(filter) };

        let mut stream = client
            .subscribe_bundle_results(request)
            .await
            .map_err(|e| anyhow!("subscribe_bundle_results failed: {e}"))?
            .into_inner();

        let (tx, rx) = mpsc::channel::<BundleResult>(capacity.max(64));
        let priors = Arc::new(Mutex::new(self.region_priors.clone()));
        let cache_uuid = Arc::new(Mutex::new(self.uuid_to_bundle.clone()));
        let cache_bundle = Arc::new(Mutex::new(self.bundle_to_uuid.clone()));
        let handle = {
            let priors = priors.clone();
            let cache_uuid = cache_uuid.clone();
            let cache_bundle = cache_bundle.clone();
            tokio::spawn(async move {
                while let Some(msg) = stream.message().await.unwrap_or(None) {
                    // map message to typed status where possible
                    if let (Some(bundle_id), Some(status_str)) = (msg.bundle_id.clone(), msg.status.clone()) {
                        let status = JitoTerminalStatus::from_str(&status_str);
                        // fuse priors
                        if let Ok(mut p) = priors.lock().await {
                            let r = p.entry(msg.region.clone()).or_default();
                            match status {
                                JitoTerminalStatus::Landed => r.on_landed(),
                                JitoTerminalStatus::Failed | JitoTerminalStatus::Invalid => r.on_failed(),
                                _ => {}
                            }
                            if let Some(region) = msg.region.clone() {
                                MET_REGION_PRIOR.with_label_values(&[&region]).set(r.score());
                            }
                        }
                        // Update uuid <-> bundle caches if uuid present
                        if let (Some(uuid), Some(bid)) = (msg.uuid.clone(), Some(bundle_id.clone())) {
                            if let Ok(mut m) = cache_uuid.lock().await { m.insert(uuid, bid.clone()); }
                            if let Ok(mut m) = cache_bundle.lock().await { m.insert(bid, msg.uuid.clone().unwrap_or_default()); }
                        }
                    }
                    let _ = tx.send(msg).await;
                }
            })
        };

        Ok((handle, rx))
    }

    /// Use this to note terminal results from the subscribe stream and fuse into priors.
    pub fn note_terminal_result(&mut self, bundle_id: &str, status: &str, region: Option<String>) {
        if matches!(status, "Landed" | "Failed" | "Invalid") {
            self.terminal_by_bundle.insert(bundle_id.to_string(), (status.to_string(), region.clone()));
            if let Some(r) = region {
                let entry = self.region_priors.entry(r.clone()).or_default();
                match status {
                    "Landed" => entry.on_landed(),
                    "Failed" | "Invalid" => entry.on_failed(),
                    _ => {}
                }
                MET_REGION_PRIOR.with_label_values(&[&r]).set(entry.score());
            }
        }
    }

    // =========== JSON-RPC: sendBundle (bundle_id) with base64; NO region param ===========
    /// Jito JSON-RPC sendBundle. Spec glue: “max 5 tx, sequential + atomic, single-slot”.
    /// API: POST {BE}/api/v1/bundles with JSON-RPC "sendBundle" body. Encoding: base64 recommended.
    pub async fn send_bundle_jsonrpc_base64(&mut self, txs: &[Vec<u8>], bundle_tip_lamports: u64) -> Result<String> {
        if txs.is_empty() { return Err(anyhow!("sendBundle: empty bundle")); }
        if txs.len() > 5 { return Err(anyhow!("sendBundle: bundle has {} txs; max is 5", txs.len())); }

        self.ensure_tip_freshness_for_send().await?;
        let adjusted_tip = self.enforce_min_and_auto_tip(bundle_tip_lamports).await?;

        let b64s: Vec<String> = txs.iter().map(|b| base64::encode(b)).collect();
        // Hard assert: no “region” JSON param ever (docs do not specify it)
        let params = serde_json::json!([ b64s, { "encoding": "base64" } ]);

        let bundle_id = self
            .jsonrpc_sendbundle_special::<JsonRpcStringValue>("sendBundle", params)
            .await
            .and_then(|v| v.value.ok_or_else(|| anyhow!("sendBundle: missing bundle_id")))?;

        info!(%bundle_id, tip_lamports=%adjusted_tip, "Jito JSON-RPC sendBundle OK (bundle_id)");
        // record bundle id; if we learn a uuid later, we’ll pair it
        self.bundle_to_uuid.entry(bundle_id.clone()).or_insert_with(|| "".into());
        Ok(bundle_id)
    }

    // ================== Acceptance ladder (pure) ==================
    fn decide_acceptance(
        stream_terminal: Option<&str>,
        inflight: Option<&str>,
        statuses: Option<&str>,
    ) -> Option<(bool, &'static str)> {
        // Ladder: stream terminal → inflight → bundle statuses → None
        if let Some(s) = stream_terminal {
            return match s {
                "Landed" => Some((true, "Landed(stream)")),
                "Failed" => Some((false, "Failed(stream)")),
                "Invalid" => Some((false, "Invalid(stream)")),
                _ => None,
            };
        }
        if let Some(s) = inflight {
            return match s {
                "Landed" => Some((true, "Landed")),
                "Failed" => Some((false, "Failed")),
                "Invalid" => Some((false, "Invalid")),
                "Pending" => None,
                _ => None,
            };
        }
        if let Some(s) = statuses {
            // treat any non-empty confirmation as success; caller passes processed/confirmed/finalized
            return Some((s == "processed" || s == "confirmed" || s == "finalized", "BundleStatuses"));
        }
        None
    }

    // ================== Acceptance polling (host-coherent; 5m TTL) ==================
    /// getInflightBundleStatuses 5-minute window; result:null ⇒ not found yet.
    pub async fn poll_acceptance_jsonrpc(
        &mut self,
        id: &str,                 // bundle_id preferred; uuid allowed if mapping known
        external_deadline: Duration,
        poll_every: Duration,
    ) -> Result<(bool, String)> {
        let ttl = Duration::from_secs(300); // 5-minute inflight window
        let start = Instant::now();
        let mut invalid_grace = 3usize;

        // Resolve id if it is a uuid and we know its bundle_id
        let mut bundle_id = id.to_string();
        if bundle_id.len() == 36 { // naive UUID length check
            if let Some(bid) = self.uuid_to_bundle.get(&bundle_id).filter(|s| !s.is_empty()).cloned() {
                bundle_id = bid;
            } else {
                debug!(uuid=%id, "uuid->bundle_id mapping unknown yet; inflight/status may not resolve");
            }
        }

        loop {
            // Short-circuit using acceptance ladder
            let stream_term = self.terminal_by_bundle.get(&bundle_id).map(|(s, _)| s.as_str());

            // Bundle statuses as lower rung
            let statuses_decision = if let Some((conf, _slot)) = self.get_bundle_status_confirm(&bundle_id).await? {
                Some(conf)
            } else { None };

            if let Some((ok, reason)) = Self::decide_acceptance(stream_term, None, statuses_decision.as_deref()) {
                return Ok((ok, reason.into()));
            }

            if start.elapsed() > ttl {
                warn!(%bundle_id, "Inflight window expired (5m hard stop)");
                return Ok((false, "ExpiredWindow".into()));
            }
            if start.elapsed() > external_deadline {
                warn!(%bundle_id, "Acceptance polling timeout (external)");
                return Ok((false, "Timeout".into()));
            }

            match self.get_inflight_status(&bundle_id).await? {
                Some(item) => {
                    if let Some((ok, reason)) = Self::decide_acceptance(None, Some(item.status.as_str()), None) {
                        return Ok((ok, reason.into()));
                    }
                    if item.status.as_str() == "Invalid" && invalid_grace > 0 {
                        invalid_grace -= 1;
                        debug!(%bundle_id, grace=?invalid_grace, "Early Invalid; grace in effect");
                    }
                }
                None => {
                    // getInflightBundleStatuses returned result:null → not found yet.
                    debug!(%bundle_id, "Inflight status not found yet (result:null)");
                }
            }

            // Dynamic backoff coherence with auctions:
            // - Base jitter ±20%
            // - Cap to ~350 ms during hot p95 spikes, ~200 ms otherwise.
            let base_ms = poll_every.as_millis() as i64;
            let jitter_pct = 20;
            let jitter_span = (base_ms * jitter_pct / 100) as i64;
            let delta = fastrand::i64(-jitter_span..=jitter_span);
            let mut sleep_ms = (base_ms + delta).max(1) as u64;

            let tf = self.tip_telemetry.lock().await.last.clone();
            let hot = match (tf.p50, tf.p95) {
                (Some(p50), Some(p95)) => p95 > p50 * 1.6,
                _ => false
            };
            let cap = if hot { 350 } else { 200 };
            if sleep_ms > cap { sleep_ms = cap; }

            sleep(Duration::from_millis(sleep_ms)).await;

            // Opportunistic tip account refresh if WS isn’t running or we’re stale
            if self.tip_ws_task.is_none() && self.tip_accounts_fresh_at.elapsed() > self.tip_accounts_ttl {
                let _ = self.refresh_tip_accounts().await;
            }
        }
    }

    /// getInflightBundleStatuses (no region JSON field)
    pub async fn get_inflight_status(&mut self, bundle_id: &str) -> Result<Option<InflightStatusItem>> {
        let params = serde_json::json!([ [bundle_id] ]);
        let res: Option<InflightResp> =
            self.jsonrpc_maybe_result("getInflightBundleStatuses", params).await?;
        Ok(res.and_then(|r| r.value.into_iter().next()))
    }

    /// getBundleStatuses (no region JSON field)
    pub async fn get_bundle_status_confirm(
        &mut self,
        bundle_id: &str,
    ) -> Result<Option<(String, Option<u64>)>> {
        let params = serde_json::json!([ [bundle_id] ]);
        let resp = self.jsonrpc_call::<BundleStatusesResp>("getBundleStatuses", params).await?;
        Ok(resp.value.into_iter().next()
            .map(|x| (x.confirmation_status.unwrap_or_default(), x.slot)))
    }

    // ================== Tip accounts ==================
    /// getTipAccounts
    pub async fn refresh_tip_accounts(&mut self) -> Result<()> {
        let params = serde_json::json!([]);
        let value: TipAccountsResp = self.jsonrpc_call("getTipAccounts", params).await?;
        self.tip_accounts.clear();
        for acc in value { self.tip_accounts.insert(acc); }
        self.tip_accounts_fresh_at = Instant::now();
        Ok(())
    }

    pub fn is_tip_account(&self, pk: &str) -> bool {
        self.tip_accounts.contains(pk)
    }

    // ================== Tip floor (WS-first global, REST global fallback) ==================
    /// WS staleness cutoff enforced: if WS older than cfg.ws_staleness_cutoff_secs, fallback to REST.
    /// If both are stale/unavailable, return a typed error instead of serving ancient EMA.
    pub async fn get_tip_floor(&self) -> Result<TipFloor> {
        // If WS has delivered data recently, use it.
        if let Some(t) = *self.tip_ws_last_alive.lock().await {
            // Update freshness metric
            MET_WS_FRESHNESS_SECONDS.set(t.elapsed().as_secs_f64());
            if t.elapsed() <= Duration::from_secs(self.cfg.ws_staleness_cutoff_secs) {
                return Ok(self.tip_telemetry.lock().await.last.clone());
            }
        }

        // Fallback: GLOBAL REST tip_floor endpoint (bundles.jito.wtf)
        if self.cfg.use_rest_tip_floor {
            let url = self.cfg.tip_rest_override
                .clone()
                .unwrap_or_else(|| GLOBAL_TIP_FLOOR_REST.to_string());

            // Respect token bucket on the first BE host just to pace similarly.
            if let Some(bucket) = self.host_buckets.get(0).cloned() {
                let mut cloned = bucket.clone();
                cloned.take(1.0).await;
            }

            let (_, http) = &self.http_hosts[0];
            let r = http.get(&url).send().await?;
            if r.status().is_success() {
                MET_TIP_REST_FALLBACKS.inc();
                let raw: serde_json::Value = r.json().await?;
                let tf = parse_tip_floor_flexible(raw)?;
                // Update shared EMA state with REST value so downstream callers see something.
                let mut tt = self.tip_telemetry.lock().await;
                tt.update_from(tf.clone());
                // Mark REST as "alive" for the purpose of not thrashing WS fallback
                let mut la = self.tip_ws_last_alive.lock().await;
                *la = Some(Instant::now());
                MET_WS_FRESHNESS_SECONDS.set(0.0);
                return Ok(tf);
            } else {
                let code = r.status();
                let body = r.text().await.unwrap_or_default();
                return Err(TipTelemetryError::Rest(format!("HTTP {code}: {body}")).into());
            }
        }

        Err(TipTelemetryError::Stale { ws_cutoff: self.cfg.ws_staleness_cutoff_secs }.into())
    }

    // Backpressure: deny sends if both WS and REST are stale beyond SLO
    async fn ensure_tip_freshness_for_send(&self) -> Result<()> {
        if !self.cfg.deny_sends_when_tip_stale { return Ok(()); }
        let last = *self.tip_ws_last_alive.lock().await;
        if let Some(t) = last {
            if t.elapsed() <= Duration::from_secs(self.cfg.tip_stale_backpressure_secs) {
                return Ok(());
            }
        }
        MET_TIP_STALE_DENIED.inc();
        Err(anyhow!("tip telemetry stale beyond {}s; refusing send (override deny_sends_when_tip_stale=false to bypass)",
                    self.cfg.tip_stale_backpressure_secs))
    }

    // WS supervisor with auto-reconnect and randomized backoff.
    async fn spawn_tip_ws_supervisor(&mut self) -> Result<()> {
        let url = self.cfg.tip_stream_override
            .clone()
            .unwrap_or_else(|| GLOBAL_TIP_STREAM_WS.to_string());

        let telemetry_ptr = self.tip_telemetry.clone();
        let last_alive_ptr = self.tip_ws_last_alive.clone();

        // Set an initial timestamp so callers don't treat it as permanently stale pre-first message.
        {
            let mut la = last_alive_ptr.lock().await;
            *la = Some(Instant::now());
        }

        let handle = {
            let url = url.clone();
            tokio::spawn(async move {
                let mut delay_ms: u64 = 500;
                loop {
                    match connect_async(&url).await {
                        Ok((mut ws_stream, _)) => {
                            info!(%url, "tip_stream connected");
                            delay_ms = 500; // reset backoff on success
                            loop {
                                match ws_stream.next().await {
                                    Some(Ok(Message::Text(s))) => {
                                        if let Ok(v) = serde_json::from_str::<serde_json::Value>(&s) {
                                            if let Ok(tf) = parse_tip_floor_flexible(v) {
                                                let mut t = telemetry_ptr.lock().await;
                                                t.update_from(tf);
                                                let mut la = last_alive_ptr.lock().await;
                                                *la = Some(Instant::now());
                                                MET_WS_FRESHNESS_SECONDS.set(0.0);
                                            }
                                        }
                                    }
                                    Some(Ok(Message::Binary(b))) => {
                                        if let Ok(s) = std::str::from_utf8(&b) {
                                            if let Ok(v) = serde_json::from_str::<serde_json::Value>(s) {
                                                if let Ok(tf) = parse_tip_floor_flexible(v) {
                                                    let mut t = telemetry_ptr.lock().await;
                                                    t.update_from(tf);
                                                    let mut la = last_alive_ptr.lock().await;
                                                    *la = Some(Instant::now());
                                                    MET_WS_FRESHNESS_SECONDS.set(0.0);
                                                }
                                            }
                                        }
                                    }
                                    Some(Ok(Message::Ping(_))) => {}
                                    Some(Ok(Message::Pong(_))) => {}
                                    Some(Ok(Message::Close(_))) | None => {
                                        warn!("tip_stream closed; reconnecting");
                                        break;
                                    }
                                    Some(Err(e)) => {
                                        warn!(err=%e, "tip_stream read error; reconnecting");
                                        break;
                                    }
                                }
                            }
                        }
                        Err(e) => {
                            warn!(err=%e, %url, "tip_stream connect failed; backing off");
                        }
                    }
                    // randomized exponential backoff up to ~10s
                    let jitter = fastrand::u64(0..=delay_ms / 2);
                    let sleep_for = (delay_ms + jitter).min(10_000);
                    sleep(Duration::from_millis(sleep_for)).await;
                    delay_ms = (delay_ms * 2).min(10_000);
                }
            })
        };

        self.tip_ws_task = Some(handle);
        Ok(())
    }

    // ================== JSON-RPC plumbing with region-aware rotation + token bucket ==================
    async fn jsonrpc_call<T: DeserializeOwned>(
        &mut self,
        method: &str,
        params: serde_json::Value,
    ) -> Result<T> {
        self.jsonrpc_core::<T>(method, params, JsonNullMode::Error, UrlMode::Default).await?
            .ok_or_else(|| anyhow!("JSON-RPC {method}: null result"))
    }

    async fn jsonrpc_maybe_result<T: DeserializeOwned>(
        &mut self,
        method: &str,
        params: serde_json::Value
    ) -> Result<Option<T>> {
        self.jsonrpc_core::<T>(method, params, JsonNullMode::ReturnNone, UrlMode::Default).await
    }

    async fn jsonrpc_sendbundle_special<T: DeserializeOwned>(
        &mut self,
        _method: &str,
        params: serde_json::Value
    ) -> Result<T> {
        // sendBundle uses POST {base}/bundles with JSON-RPC body.
        self.jsonrpc_core::<T>("sendBundle", params, JsonNullMode::Error, UrlMode::SendBundle).await?
            .ok_or_else(|| anyhow!("JSON-RPC sendBundle: null result"))
    }

    async fn jsonrpc_core<T: DeserializeOwned>(
        &mut self,
        method: &str,
        params: serde_json::Value,
        null_mode: JsonNullMode,
        url_mode: UrlMode,
    ) -> Result<Option<T>> {
        let tries = self.http_hosts.len().max(1);

        // Host order: bias to lower error EWMA, preserve RR tie-break, add light shuffle to avoid stampede
        let mut order: Vec<usize> = (0..self.http_hosts.len()).collect();
        order.sort_by(|&a, &b| {
            let sa = self.host_health[a].ewma_err + fastrand::f64() * 0.01 + ((a + self.http_rr) % tries) as f64 * 1e-6;
            let sb = self.host_health[b].ewma_err + fastrand::f64() * 0.01 + ((b + self.http_rr) % tries) as f64 * 1e-6;
            sa.partial_cmp(&sb).unwrap()
        });

        for i in 0..tries {
            let idx = order[i % order.len()];
            let (host, http) = &self.http_hosts[idx];
            let base = &self.be_api_bases[idx];

            let url = match url_mode {
                UrlMode::Default => format!("{}/{}", base, method),
                UrlMode::SendBundle => format!("{}/bundles", base),
            };

            // JSON-RPC 2.0 body; NO undocumented region fields.
            let req = JsonRpcReq::new(method, match params.clone() {
                serde_json::Value::Array(v) => v,
                other => vec![other],
            });

            // Respect default BE rate limits: ~1 rps per region per IP. (Jito “Default Limits”)
            if let Some(bucket) = self.host_buckets.get_mut(idx) { bucket.take(1.0).await; }

            let resp = http
                .post(&url)
                .header("Content-Type", "application/json")
                // Auth header lives in default headers as x-jito-auth if provided (see connect).
                .json(&req)
                .send()
                .await;

            match resp {
                Ok(r) => {
                    if r.status().is_success() {
                        self.host_health[idx].note_ok();
                        let text = r.text().await?;
                        let parsed: JsonRpcResp<T> = serde_json::from_str(&text)
                            .with_context(|| format!("json parse failed for {method} @ {host}: {text}"))?;
                        if let Some(err) = parsed.error {
                            let data = err.data.unwrap_or(serde_json::Value::Null);
                            return Err(anyhow!("JSON-RPC {method} error {}: {} data={}", err.code, err.message, data));
                        }
                        match parsed.result {
                            Some(result) => {
                                self.http_rr = (idx + 1) % self.http_hosts.len();
                                return Ok(Some(result));
                            }
                            None => {
                                match null_mode {
                                    JsonNullMode::ReturnNone => {
                                        self.http_rr = (idx + 1) % self.http_hosts.len();
                                        return Ok(None);
                                    }
                                    JsonNullMode::Error => {
                                        return Err(anyhow!("JSON-RPC {method}: null result"));
                                    }
                                }
                            }
                        }
                    } else if r.status() == StatusCode::TOO_MANY_REQUESTS || r.status().is_server_error() {
                        // Inline doc: rotate region on 429/5xx with jitter. (Jito “Default Limits”)
                        self.host_health[idx].note_err();
                        let back_ms = 50 + fastrand::u64(0..150);
                        warn!(host=%host, method=%method, url=%url, "429/5xx; rotating region with jitter");
                        sleep(Duration::from_millis(back_ms)).await;
                        continue;
                    } else {
                        let code = r.status();
                        let body = r.text().await.unwrap_or_default();
                        return Err(anyhow!("HTTP {code} on {method} @ {host} url={url}: {body}"));
                    }
                }
                Err(e) => {
                    self.host_health[idx].note_err();
                    let back_ms = 25 + fastrand::u64(0..75);
                    warn!(method=%method, host=%host, url=%url, err=%e, "network error; rotating region");
                    sleep(Duration::from_millis(back_ms)).await;
                    continue;
                }
            }
        }
        Err(anyhow!("JSON-RPC {method}: all Block Engine hosts failed"))
    }

    // ================== helpers & wire types ==================

    #[allow(unused)]
    fn _lookup_region_for_logging_only(&mut self, bundle_id: &str) -> Option<String> {
        // prune old entries >5m
        let now = Instant::now();
        self.inflight_regions.retain(|_, (_, t)| now.duration_since(*t) < Duration::from_secs(300));
        self.inflight_regions.get(bundle_id).map(|(r, _)| r.clone())
    }

    // ================== Public convenience for logging ID pair ==================
    pub fn record_id_pair(&mut self, uuid: Option<&str>, bundle_id: Option<&str>) {
        match (uuid, bundle_id) {
            (Some(u), Some(b)) => { self.uuid_to_bundle.insert(u.to_string(), b.to_string()); self.bundle_to_uuid.insert(b.to_string(), u.to_string()); },
            (Some(u), None)    => { self.uuid_to_bundle.entry(u.to_string()).or_insert_with(|| "".into()); }
            (None, Some(b))    => { self.bundle_to_uuid.entry(b.to_string()).or_insert_with(|| "".into()); }
            (None, None)       => {}
        }
    }

    pub fn log_id_pair(uuid: Option<&str>, bundle_id: Option<&str>) {
        match (uuid, bundle_id) {
            (Some(u), Some(b)) => info!(uuid=%u, bundle_id=%b, "Bundle IDs (UUID != bundle_id)"),
            (Some(u), None)    => info!(uuid=%u, "Bundle UUID only"),
            (None, Some(b))    => info!(bundle_id=%b, "Bundle ID only"),
            (None, None)       => warn!("No IDs available"),
        }
    }

    /// Build gRPC request safely (public helper)
    pub fn build_grpc_send_request(txs: &[Vec<u8>], encoding: &GrpcBundleEncoding) -> Result<SendBundleRequest> {
        if txs.is_empty() { return Err(anyhow!("empty bundle")); }
        if txs.len() > 5 { return Err(anyhow!("gRPC bundle has {} txs; max is 5", txs.len())); }

        let bundle = match encoding {
            GrpcBundleEncoding::Bytes => ProtoBundle {
                transactions: txs.iter().cloned().collect(),
                ..Default::default()
            },
            GrpcBundleEncoding::Base64 => {
                let b64s: Vec<Vec<u8>> = txs.iter().map(|b| base64::encode(b).into_bytes()).collect();
                ProtoBundle { transactions: b64s, ..Default::default() }
            }
        };

        Ok(SendBundleRequest { bundle: Some(bundle), ..Default::default() })
    }
}

// =========== Types (serde shapes mirroring first-party JSON-RPC responses) ===========

#[derive(Debug, Serialize)]
struct JsonRpcReq<'a> {
    jsonrpc: &'static str,
    id: u64,
    method: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    params: Option<serde_json::Value>,
}
impl<'a> JsonRpcReq<'a> {
    fn new(method: &'a str, params: Vec<serde_json::Value>) -> Self {
        Self {
            jsonrpc: "2.0",
            id: 1,
            method,
            params: if params.is_empty() { None } else { Some(serde_json::Value::Array(params)) },
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)] // lock to first-party models
struct JsonRpcResp<T> {
    #[allow(dead_code)] jsonrpc: Option<String>,
    #[allow(dead_code)] id: Option<u64>,
    result: Option<T>,
    error: Option<JsonRpcErrorObj>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct JsonRpcErrorObj {
    code: i64,
    message: String,
    #[serde(default)]
    data: Option<serde_json::Value>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct InflightResp {
    #[allow(dead_code)] context: Option<serde_json::Value>,
    value: Vec<InflightStatusItem>,
}

#[derive(Debug, Deserialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct InflightStatusItem {
    pub bundle_id: String,
    /// "Pending" | "Failed" | "Landed" | "Invalid"
    pub status: String,
    #[serde(default)]
    pub landed_slot: Option<u64>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct BundleStatusesResp {
    #[allow(dead_code)] context: serde_json::Value,
    value: Vec<BundleStatusDetail>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct BundleStatusDetail {
    bundle_id: String,
    transactions: Vec<String>,
    slot: Option<u64>,
    confirmation_status: Option<String>, // processed | confirmed | finalized
    #[allow(dead_code)] err: Option<serde_json::Value>,
}

type TipAccountsResp = Vec<String>;

#[derive(Copy, Clone)]
enum JsonNullMode {
    Error,
    ReturnNone,
}

#[derive(Copy, Clone)]
enum UrlMode {
    Default,
    SendBundle,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct JsonRpcStringValue {
    #[serde(rename = "value")]
    pub value: Option<String>,
}

// Flexible tip_floor parser (REST or WS payloads)
fn parse_tip_floor_flexible(v: serde_json::Value) -> Result<TipFloor> {
    // Try direct REST-like mapping with drift support
    if let Ok(tf_like) = serde_json::from_value::<TipFloorRestLike>(v.clone()) {
        let tf: TipFloor = tf_like.into();
        if tf.p25.or(tf.p50).or(tf.p75).or(tf.p95).is_some() || tf.ema.is_some() {
            return Ok(tf);
        }
    }
    // Try generic object with p25/p50/p75/p95/ema
    let obj = v.as_object().ok_or_else(|| anyhow!("tip_floor: expected object"))?;
    let mut out = TipFloor::default();
    out.p25 = obj.get("p25").and_then(|x| x.as_f64());
    out.p50 = obj.get("p50").and_then(|x| x.as_f64());
    out.p75 = obj.get("p75").and_then(|x| x.as_f64());
    out.p95 = obj.get("p95").and_then(|x| x.as_f64());
    out.ema = obj.get("ema").and_then(|x| x.as_f64());

    if out.p25.or(out.p50).or(out.p75).or(out.p95).is_some() || out.ema.is_some() {
        return Ok(out);
    }
    Err(anyhow!("tip_floor: unrecognized schema"))
}

// ===== Typed terminal status for stream messages =====
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
enum JitoTerminalStatus { Pending, Landed, Failed, Invalid }
impl JitoTerminalStatus {
    fn from_str(s: &str) -> Self {
        match s {
            "Landed" => Self::Landed,
            "Failed" => Self::Failed,
            "Invalid" => Self::Invalid,
            _ => Self::Pending,
        }
    }
}

// ================== Tests: golden shapes + invariants ==================
#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn parse_inflight_resp_shape() {
        let sample = json!({
          "jsonrpc":"2.0","id":1,
          "result": {
            "context": null,
            "value": [
              { "bundle_id":"abc", "status":"Pending" }
            ]
          }
        });
        let parsed: JsonRpcResp<InflightResp> = serde_json::from_value(sample).unwrap();
        let v = parsed.result.unwrap().value;
        assert_eq!(v.len(), 1);
        assert_eq!(v[0].bundle_id, "abc");
        assert_eq!(v[0].status, "Pending");
    }

    #[test]
    fn parse_bundle_statuses_shape() {
        let sample = json!({
          "jsonrpc":"2.0","id":1,
          "result": {
            "context": {},
            "value": [
              { "bundle_id":"abc", "transactions":["..."], "slot":123, "confirmation_status":"finalized" }
            ]
          }
        });
        let parsed: JsonRpcResp<BundleStatusesResp> = serde_json::from_value(sample).unwrap();
        let v = parsed.result.unwrap().value;
        assert_eq!(v[0].bundle_id, "abc");
        assert_eq!(v[0].slot, Some(123));
        assert_eq!(v[0].confirmation_status.as_deref(), Some("finalized"));
    }

    #[test]
    fn golden_sendbundle_body_base64() {
        // Golden body reflects docs: params = [ base64Txs[], { "encoding":"base64" } ]
        let sb = JsonRpcReq::new("sendBundle", vec![
            json!(["dHgx","dHgy"]), json!({"encoding":"base64"})
        ]);
        let s = serde_json::to_string(&sb).unwrap();
        assert!(s.contains("\"sendBundle\""));
        assert!(s.contains("\"encoding\":\"base64\""));
        assert!(!s.contains("\"region\"")); // also asserts the invariant below
    }

    #[test]
    fn hard_assert_no_region_param_in_json_bodies() {
        // Build representative JSON-RPC bodies and assert "region" never appears.
        let sb = JsonRpcReq::new("sendBundle", vec![json!(["txA"]), json!({"encoding":"base64"})]);
        let inflight = JsonRpcReq::new("getInflightBundleStatuses", vec![json!([ "bid" ])]);
        let statuses = JsonRpcReq::new("getBundleStatuses", vec![json!([ "bid" ])]);

        let s1 = serde_json::to_string(&sb).unwrap();
        let s2 = serde_json::to_string(&inflight).unwrap();
        let s3 = serde_json::to_string(&statuses).unwrap();

        assert!(!s1.contains("\"region\""), "sendBundle body unexpectedly contains region");
        assert!(!s2.contains("\"region\""), "getInflightBundleStatuses body unexpectedly contains region");
        assert!(!s3.contains("\"region\""), "getBundleStatuses body unexpectedly contains region");
    }

    #[test]
    fn acceptance_ladder_smoke() {
        // stream terminal dominates
        assert_eq!(super::JitoSender::decide_acceptance(Some("Landed"), Some("Failed"), Some("finalized")), Some((true, "Landed(stream)")));
        // inflight next
        assert_eq!(super::JitoSender::decide_acceptance(None, Some("Failed"), None), Some((false, "Failed")));
        // statuses last
        assert_eq!(super::JitoSender::decide_acceptance(None, None, Some("finalized")), Some((true, "BundleStatuses")));
    }
}
