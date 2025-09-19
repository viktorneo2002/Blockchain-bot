use anchor_lang::prelude::*;
use arrayref::array_ref;
use borsh::{BorshDeserialize, BorshSerialize};
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_client::nonblocking::pubsub_client::PubsubClient;
use solana_client::rpc_config::{RpcSignatureSubscribeConfig, RpcTransactionLogsConfig, RpcTransactionLogsFilter};
use solana_client::rpc_response::{RpcPerfSample, RpcPrioritizationFee, RpcAddressLookupTableAccount, RpcSignatureResult};
use solana_transaction_status::UiTransactionEncoding;
use bincode;
use solana_sdk::commitment_config::CommitmentConfig;
use solana_sdk::{
    account::Account,
    clock::Clock,
    compute_budget::ComputeBudgetInstruction,
    hash::Hash,
    instruction::Instruction,
    pubkey::Pubkey,
    signature::Keypair,
};
use solana_client::rpc_config::RpcSendTransactionConfig;
use solana_sdk::{
    commitment_config::CommitmentLevel, 
    signature::Signature, 
    transaction::Transaction,
    address_lookup_table_account::AddressLookupTableAccount,
    message::{VersionedMessage, Message},
    message::v0::{self as v0, Message as V0Message},
    transaction::VersionedTransaction,
    packet::PACKET_DATA_SIZE,
    signer::Signer,
    instruction::AccountMeta,
};
use std::{
    collections::{BTreeMap, HashMap, HashSet, VecDeque},
    collections::hash_map::Entry,
    mem::MaybeUninit,
    sync::{Arc, atomic::{AtomicUsize, Ordering}},
    time::{Duration as StdDuration, Instant},
};
use tokio::sync::{mpsc, Semaphore};
use tokio::time::{sleep, Duration as TokioDuration};
use futures::stream::{FuturesUnordered, StreamExt};
use parking_lot::RwLock;
use spl_token::state::Mint as SplMint;

const TICK_WINDOW_SIZE: usize = 256;
const CLUSTER_MIN_SIZE: usize = 3;
const CLUSTER_MAX_DISTANCE: u64 = 5;
const VOLUME_THRESHOLD: u64 = 1_000_000_000; // 1000 USDC
const TICK_SPACING: u16 = 64;
const BASIS_POINT_MAX: u64 = 10000;
const SQRT_PRICE_LIMIT_X64: u128 = 79228162514264337593543950336;
const MIN_SQRT_PRICE_X64: u128 = 4295128739;
const MAX_SQRT_PRICE_X64: u128 = 1461446703485210103287273052203988822378723970342;
const CACHE_TTL_MS: u64 = 250; // short-lived state reuse within a slot
const SHIFT_PURGE_TICKS: i32 = 768; // if price regime shifts > ~12*64 ticks, drop tick-array cache for that pool
const DECIMALS_TTL_MS: u64 = 60_000;   // 60s – token mint decimals never change, but cache conservatively
const RECENT_OP_TTL_MS: u64 = 200;     // throttle identical opportunities within 200ms window
const MAX_BITMAP_CACHE: usize = 8_192;
const MAX_TICKARRAY_CACHE: usize = 16_384;
const MAX_DECIMALS_CACHE: usize = 1_024;
const MAX_OPP_SEEN_CACHE: usize = 8_192;
const SLOTMS_TTL_MS: u64 = 5_000;   // refresh measured slot length every 5s
const POOLSTATE_TTL_MS: u64 = 250;  // short-lived pool-state reuse (cross-pool arb)
const PAIR_INDEX_TTL_MS: u64 = 2_000;        // peers seldom change; refresh sparsely
const MAX_PAIR_INDEX_CACHE: usize = 8_192;    // pools across DEXes
const HEDGE_RPC_TIMEOUT_MS: u64 = 180;  // per-endpoint soft timeout for hedged calls
const MAX_RPC_RING: usize = 4;          // sane cap; prevents self-DOS
const BLOCKHASH_TTL_MS: u64 = 1_500;   // refresh well before expiry (~2s)
const FEE_HIST_TTL_MS: u64 = 500;      // read fee market ~2× per second
const MAX_TICKARRAY_HASH_CACHE: usize = 16_384;
const ANALYSIS_FASTPATH_TTL_MS: u64 = 300; // within one slot we can safely reuse analysis
const MAX_BUNDLES_PER_SLOT: usize = 12; // desk-tuned; adjust per relay capacity
const HEDGE_STAGGER_MS: u64 = 40;     // delay between hedged launches
const RPC_EWMA_ALPHA: f64 = 0.25;     // smoothing for health stats
const TICK_ARRAY_BLOCK: i32 = 64;     // ticks per on-chain array (fixed by CLAMM layout)
const MAX_OPS_PER_SLOT: usize = 4;    // desk cap: send only the best non-conflicting set
const CALIB_ALPHA0: f64 = 2.0;        // Beta prior α
const CALIB_BETA0: f64 = 3.0;         // Beta prior β
const CALIB_MIN_SAMPLES: u64 = 20;
const CU_EWMA_ALPHA: f64 = 0.22;      // learning rate for CU consumption
const CU_HEADROOM: f64 = 1.35;        // headroom over EWMA to avoid throttling
const CU_MIN_LIMIT: u32 = 180_000;    // absolute safety floor
const CU_MAX_LIMIT: u32 = 900_000;    // absolute safety cap
const LEADER_TTL_MS: u64 = 800;       // refresh within slot
const FEE_SPIKE_MULT: f64 = 1.70;     // "spike" = >170% of cached median
const DEFERRAL_PHASE_MAX: f64 = 0.35; // defer only early in slot
const MAX_TA_ADDR_CACHE: usize = 32_768;
const PNL_EWMA_ALPHA: f64 = 0.15;
const PNL_UP_LAMPORTS: i64 = 120_000;   // widen quota on sustained wins
const PNL_DN_LAMPORTS: i64 = -80_000;   // tighten quota on drawdown
const MAX_BUNDLES_HI: usize = 16;       // dynamic upper bound
const MAX_BUNDLES_LO: usize = 8;        // dynamic lower bound
const SLOTMS_ALPHA: f64 = 0.30; // smooth measured slot length
const RPC_SLOT_TTL_MS: u64 = 700;       // how often we refresh per-endpoint slot
const RPC_STALE_PENALTY: f64 = 4_000.0; // cost per slot lag (adds to ranking score)
const MAX_BITMAP_ADDR_CACHE: usize = 8_192;
const MAX_LUT_CACHE: usize = 1_024;
const LUT_TTL_MS: u64 = 10_000;      // 10s — ALTs rotate slowly
const MAX_LUTS_PER_TX: usize = 2;    // constrain to keep decode cheap
const POOL_COOLDOWN_MS: u64 = 120; // tiny; allows different-size/sides through
const PROG_HASH_TTL_MS: u64 = 5_000;
const RPC_INCL_ALPHA: f64 = 0.30;       // smoothing for inclusion success
const SEND_ORIGIN_TTL_MS: u64 = 12_000; // track which endpoint won a send
const RPC_INCL_PENALTY: f64 = 1_200.0;  // score penalty for poor inclusion
const PACKET_HEADROOM: usize = 48; // bytes of safety margin (QUIC framing, relayer quirks)
const LUT_USELESS_TTL_MS: u64 = 60_000; // 1 min demotion window
const LEADER_QUAL_ALPHA: f64 = 0.35;  // smoothing for leader fill-rate
const LEADER_MULT_MIN: f64 = 0.92;    // min multiplier for good leaders
const LEADER_MULT_MAX: f64 = 1.12;    // max multiplier for poor leaders
const LUT_NEG_TTL_MS: u64 = 60_000; // backoff for LUTs that return None/deactivated
const PAYER_MIN_BAL_LAMPORTS: u64 = 2_000_000; // ~0.002 SOL safety floor
const LOCKSET_TTL_MS: u64 = 900; // ~2–3 slots worst case; resets each slot anyway
const RUNG_WAIT_MS: u64 = 70; // give first rung a short head-start
const ORACLE_STALE_MS: u64 = 1_500;  // require sub-1.5s freshness
const ORACLE_DRIFT_MAX: f64 = 0.08;  // 8% drift guard (venue vs oracle)
const FAIL_BACKOFF_MS: u64 = 600; // per-accounts-hash window
const DYN_TO_MIN_MS: u64 = 80;
const DYN_TO_MAX_MS: u64 = 700;
const DYN_TO_ERR_BOOST: f64 = 900.0; // extra ms per 1.0 error EWMA
const FEE_HIST_TTL_BASE_MS: u64 = 1_600;
const FEE_HIST_TTL_MIN_MS:  u64 = 400;
const FEE_HIST_TTL_MAX_MS:  u64 = 4_000;
const RPC_CB_ERR_THRESH: f64 = 0.35;
const RPC_CB_INCL_THRESH: f64 = 0.45;
const RPC_CB_TTL_MS: u64 = 7_000;
const LEADER_FORECAST_TTL_MS: u64 = 2_000;
const LEADER_FORECAST_HORIZON: u64 = 16; // next 16 slots
const QUOTA_TILT_MAX: f64 = 0.25;        // ±25% adjustment of slot quota
const PROC_LAT_MIN_MS: u64 = 30;
const PROC_LAT_MAX_MS: u64 = 220;
const PROC_LAT_ALPHA:  f64 = 0.35;
const CU_RUNG_UPLIFTS: [f64; 3] = [1.00, 1.02, 1.05];
const POOL_HOT_PENALTY_MS: u64 = 180;   // window where we penalize (not block)
const POOL_HOT_PENALTY:   f64 = 0.18;   // 18% EV-density haircut
const GMA_MAX_BATCH: usize = 64;
const GMA_MIN_BATCH: usize = 8;
const FAIL_EWMA_ALPHA: f64 = 0.25;
const PUBSUB_CONNECT_TTL_MS: u64 = 10_000;  // retry window for WS connect
const SIG_SUB_TTL_MS:      u64 = 8_000;     // auto-unsub if no event by then
const TPU_FIRST_ENABLE: bool = true;
const TPU_SEND_TMO_MS: u64 = 220;
const STATUS_SWEEP_MS: u64 = 140;
const STATUS_SWEEP_MAX: usize = 64;
const HEAP_FRAME_THRESH_CU: u32 = 480_000; // add heap frame above this CU limit
const HEAP_FRAME_BYTES:     u32 = 64 * 1024;
const BLOCKHASH_AGE_REBUILD_MS: u64 = 3_000; // if older than 3s, refresh before send
const SIGNER_FLOOR_ALPHA: f64 = 0.25;      // learning rate of floor
const SIGNER_FLOOR_DECAY: f64 = 0.995;     // slow decay to avoid sticky high floors
const SIGNER_FLOOR_MIN:   u64 = 20;        // micro-lamports

// NEW: Optimization constants
const SLACK_EWMA_ALPHA: f64 = 0.20;        // learning rate for slippage slack
const SLACK_MIN: f64 = 0.0003;             // 3 bps floor
const SLACK_MAX: f64 = 0.025;              // 2.5% cap
const ALT_PLAN_TTL_MS: u64 = 15_000;       // ALT plan cache TTL
const PHASE_BUCKETS: usize = 8;            // 12.5% buckets for leader phase hints
const HINT_TTL_MS: u64 = 20_000;           // leader phase hint TTL
const READ_STAGGER_BIAS_MS: u64 = 7;       // read/send stagger bias
const ALT_MUST_HIT: &[&str] = &[
    "ComputeBudget111111111111111111111111111111",
    "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA",
    "11111111111111111111111111111111", // System Program
    "SysvarRent111111111111111111111111111111111", // Sysvar Rent
];

// NEW: Additional optimization constants
const MICRO_BIN_TTL_MS: u64 = 20_000;
const MICRO_BIN_ALPHA: f64 = 0.28;   // inclusion EWMA per micro bin
const MICRO_REG_TTL_MS: u64 = 900;
const CLUSTER_STALE_SLOTS: f64 = 2.0; // if best−median > 2 slots, prefer TPU for sends
const LAND_STREAK_TTL_MS: u64 = 8_000;
const LAND_STREAK_TAPER: f64 = 0.96;   // −4% micro after a short streak
const POOL_BURST_WINDOW_MS: u64 = 160;
const POOL_RUNG_SPACING_BOOST: f64 = 1.6;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, BorshSerialize, BorshDeserialize)]
pub struct TickData {
    pub tick_index: i32,
    pub sqrt_price_x64: u128,
    pub liquidity_net: i128,
    pub liquidity_gross: u128,
    pub fee_growth_outside_a: u128,
    pub fee_growth_outside_b: u128,
    pub timestamp: u64,
    pub block_height: u64,
}

#[derive(Debug, Clone)]
pub struct TickCluster {
    pub center_tick: i32,
    pub ticks: Vec<TickData>,
    pub total_liquidity: u128,
    pub price_range: (u128, u128),
    pub volume_24h: u64,
    pub concentration_score: f64,
    pub volatility_score: f64,
    pub opportunity_score: f64,
}

#[derive(Debug, Clone)]
pub struct ClusterAnalysis {
    pub cluster_id: u64,
    pub timestamp: u64,
    pub clusters: Vec<TickCluster>,
    pub dominant_cluster: Option<TickCluster>,
    pub liquidity_distribution: HashMap<i32, u128>,
    pub price_impact_map: BTreeMap<i32, f64>,
    pub mev_opportunities: Vec<MevOpportunity>,
    // NEW: desk-grade stability signals
    pub ema_volatility: f64,      // sticky market heat (0..1)
    pub ema_concentration: f64,   // sticky liquidity focus (0..1)
    pub persistence_score: f64,   // overlap vs. last dominant (0..1)
}

#[derive(Debug, Clone)]
pub struct MevOpportunity {
    pub opportunity_type: OpportunityType,
    pub tick_range: (i32, i32),
    pub expected_profit: u64,
    pub risk_score: f64,
    pub execution_probability: f64,
    pub gas_estimate: u64,
    pub priority_fee: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OpportunityType {
    Arbitrage,
    Liquidation,
    JitLiquidity,
    BackRun,
    Sandwich,
    CrossPoolArbitrage,
    TriangularArbitrage,
}

pub struct TickClusterAnalyzer {
    rpc_client: Arc<RpcClient>,
    // NEW: hedged ring (first = primary)
    rpc_ring: Arc<Vec<Arc<RpcClient>>>,
    // kept:
    tick_history: Arc<RwLock<VecDeque<TickData>>>,
    cluster_cache: Arc<RwLock<HashMap<Pubkey, ClusterAnalysis>>>,
    price_oracle: Arc<RwLock<HashMap<Pubkey, u64>>>,
    liquidity_threshold: u128,
    analysis_interval: Duration,
    // NEW: short-TTL caches to avoid duplicate fetch/parse inside a slot
    bitmap_cache: Arc<RwLock<HashMap<Pubkey, (u64, Arc<Vec<u8>>)>>>,
    tick_array_cache: Arc<RwLock<HashMap<Pubkey, (u64, Arc<Vec<TickData>>)>>>,
    // NEW: per-pool single-flight guard to avoid overlapping analysis on the same pool
    inflight_guards: Arc<RwLock<HashMap<Pubkey, Arc<tokio::sync::Semaphore>>>>,
    // NEW:
    token_decimals_cache: Arc<RwLock<HashMap<Pubkey, (u64, u8)>>>,
    recent_opps_seen: Arc<RwLock<HashMap<(u8, Pubkey, i32, i32), u64>>>, // (type, pool, lo, hi) -> ts
    // NEW:
    slot_ms_cache: Arc<RwLock<(u64, u64)>>,                         // (ts_ms, slot_ms)
    pool_state_cache: Arc<RwLock<HashMap<Pubkey, (u64, PoolState)>>>, // TTL pool state
    // NEW:
    pair_index_cache: Arc<RwLock<HashMap<(Pubkey, Pubkey), (u64, Arc<Vec<Pubkey>>)>>>,
    // NEW: fresh blockhash TTL
    blockhash_cache: Arc<RwLock<(u64, Hash)>>,
    // NEW: market-aware CU pricing
    fee_hist_cache: Arc<RwLock<(u64, u64)>>,   // (ts_ms, median_microLamportsPerCU)
    // NEW: tick-array content-hash cache
    tick_array_hash_cache: Arc<RwLock<HashMap<Pubkey, (u64, u64)>>>, // key -> (ts, fnv64(data))
    chunk_target: Arc<AtomicUsize>,                                   // adaptive multi-acct chunk
    // NEW: no-change fast-path
    pool_ticks_hash_cache: Arc<RwLock<HashMap<Pubkey, (u64, u64)>>>, // pool -> (ts, fnv64(ticks))
    // NEW: pool account hash skip
    pool_account_hash_cache: Arc<RwLock<HashMap<Pubkey, (u64, u64)>>>, // pool -> (ts, fnv64(data))
    // NEW: per-slot send quota
    slot_quota: Arc<RwLock<(u64 /*slot_ms epoch*/, usize /*count*/)>>, // simple rolling slot counter
    // NEW: hedged RPC v2
    rpc_stats: Arc<RwLock<Vec<(f64 /*ema_ms*/, f64 /*ema_err*/)>>>,
    // NEW: fee-quantile anchoring
    fee_hist_vec_cache: Arc<RwLock<(u64, Arc<Vec<u64>>)>>,
    // NEW: execution calibration
    exec_calib: Arc<RwLock<HashMap<(u8 /*type*/, u8 /*volBkt*/, u8 /*concBkt*/), (u64 /*succ*/, u64 /*fail*/)>>>,
    // NEW: slot-level de-dup
    slot_seen: Arc<RwLock<(u64 /*slot_epoch*/, HashSet<(u8, Pubkey, i32, i32)>)>>,
    // NEW: CU usage model
    cu_usage_ewma: Arc<RwLock<HashMap<(u8 /*type*/, u8 /*widthBkt*/), (f64 /*ewma*/, u64 /*n*/)>>>,
    // NEW: leader cache
    leader_cache: Arc<RwLock<(u64, Pubkey)>>,
    // NEW: per-op in-flight guard
    inflight_op_guards: Arc<RwLock<HashMap<(u8, Pubkey, i32, i32), Arc<tokio::sync::Semaphore>>>>,
    // NEW: tick-array PDA cache
    tick_array_addr_cache: Arc<RwLock<HashMap<(Pubkey, i32), Pubkey>>>,
    // NEW: PnL EWMA
    pnl_ewma: Arc<RwLock<f64>>,  // lamports
    // NEW: per-endpoint slot cache
    rpc_slot_cache: Arc<RwLock<Vec<(u64 /*ts_ms*/, f64 /*slot_ema*/)>>>,
    // NEW: bitmap→addresses cache
    bitmap_addr_cache: Arc<RwLock<HashMap<(Pubkey, u64 /*fnv64(bitmap)*/), (u64 /*ts*/, Arc<Vec<Pubkey>>)>>>,
    // NEW: ALT caches/registry
    alt_cache: Arc<RwLock<HashMap<Pubkey, (u64 /*ts*/, AddressLookupTableAccount)>>>,
    alt_registry: Arc<RwLock<Vec<Pubkey>>>, // the LUT Pubkeys you want us to consider
    // NEW: program hash cache for upgrade detection
    program_hash_cache: Arc<RwLock<HashMap<Pubkey, (u64, u64 /*fnv64(program data)*/)>>>,
    // NEW: per-pool micro-cooldown
    pool_last_send: Arc<RwLock<HashMap<Pubkey, u64>>>,
    // NEW: RPC inclusion EWMA tracking
    rpc_inclusion: Arc<RwLock<Vec<f64>>>, // EWMA( land_rate ) per endpoint, seeded 0.6
    send_origin: Arc<RwLock<HashMap<Signature, (u64 /*ts*/, usize /*endpoint idx*/, Pubkey /*leader*/)>>>,
    // NEW: ALT usefulness demotion
    alt_useless: Arc<RwLock<HashMap<Pubkey, u64>>>, // lut -> last_useless_ts
    // NEW: leader inclusion model
    leader_inclusion: Arc<RwLock<HashMap<Pubkey, f64>>>, // EWMA fill-rate per leader
    // NEW: ALT negative cache
    alt_negative: Arc<RwLock<HashMap<Pubkey, u64>>>, // lut -> last_negative_ts
    // NEW: per-slot account locks
    slot_account_locks: Arc<RwLock<(u64 /*slot_epoch*/, u64 /*ts_ms*/, HashSet<Pubkey>)>>,
    // NEW: fail signature backoff by account-key hash
    fail_hash_ttl: Arc<RwLock<HashMap<u64 /*fnv of writable set*/, u64 /*ts*/>>>,
    // NEW: circuit breaker for bad endpoints
    rpc_cb_until: Arc<RwLock<Vec<u64>>>, // epoch ms until which idx is demoted
    // NEW: payer registry for rotation
    payer_registry: Arc<RwLock<Vec<Pubkey>>>,
    // NEW: leader forecast cache
    leader_forecast_cache: Arc<RwLock<(u64 /*ts*/, Vec<Pubkey>)>>,
    // NEW: per-leader processed latency
    leader_proc_lat: Arc<RwLock<HashMap<Pubkey, f64>>>, // ms
    // NEW: fail EWMA by type
    fail_ewma_by_type: Arc<RwLock<HashMap<u8 /*type_code*/, f64 /*p_fail EWMA*/>>>,
    // NEW: pubsub for fast inclusion
    pubsub_ws_url: Arc<RwLock<Option<String>>>,
    pubsub:        Arc<RwLock<Option<Arc<PubsubClient>>>>,
    inflight_sigs: Arc<RwLock<HashMap<Signature, u64 /*ts_sent*/>>>,
    // NEW: TPU client
    tpu: Arc<RwLock<Option<Arc<solana_tpu_client::nonblocking::tpu_client::TpuClient>>>>,
    // NEW: ALT address index
    alt_addr_index: Arc<RwLock<HashMap<Pubkey, (u64 /*ts*/, Arc<HashSet<Pubkey>>)>>>,
    // NEW: signer fee floors
    signer_fee_floor: Arc<RwLock<HashMap<Pubkey, u64>>>, // learned micro floor
    
    // NEW: Optimization fields
    // 183: per-pool slippage slack learner
    pool_slack_ewma: Arc<RwLock<HashMap<Pubkey, f64>>>, // avg absolute rel slippage
    // 184: ALT plan cache
    alt_plan_cache: Arc<RwLock<HashMap<u64, (u64, Vec<Pubkey>)>>>, // hash -> (ts, plan)
    // 187: leader phase hint memory
    leader_phase_hint: Arc<RwLock<HashMap<(Pubkey, u8), (u64, u64)>>>, // (leader, phase) -> (ts, micro)
    // 187: extend send_origin to carry micro
    send_origin: Arc<RwLock<HashMap<Signature, (u64, usize, Pubkey, u64)>>>, // sig -> (ts, idx, leader, micro)
    
    // NEW: Additional optimization fields
    // 190: micro-bin inclusion learner
    micro_bin_incl: Arc<RwLock<HashMap<u64, (u64, f64)>>>, // micro -> (ts, p)
    // 191: endpoint concurrency governor
    rpc_inflight: Arc<Vec<AtomicUsize>>,
    // 192: global micro de-dupe
    micro_slot_registry: Arc<RwLock<(u64, u8, HashSet<u64>)>>, // slot_epoch, phase_bucket, micros
    // 195: cost guard taper
    land_streak: Arc<RwLock<HashMap<(Pubkey, u8), (u64, u32)>>>, // (leader, phase) -> (ts, count)
    // 196: pool-anti-burst
    pool_last_activity: Arc<RwLock<HashMap<Pubkey, u64>>>, // pool -> last_activity_ts
}

impl TickClusterAnalyzer {
    pub fn new(rpc_endpoint: &str) -> Self {
        let primary = Arc::new(RpcClient::new_with_commitment(
                rpc_endpoint.to_string(),
                CommitmentConfig::processed(),
        ));
        Self {
            rpc_client: primary.clone(),
            rpc_ring: Arc::new(vec![primary]),
            tick_history: Arc::new(RwLock::new(VecDeque::with_capacity(TICK_WINDOW_SIZE))),
            cluster_cache: Arc::new(RwLock::new(HashMap::new())),
            price_oracle: Arc::new(RwLock::new(HashMap::new())),
            liquidity_threshold: 50_000_000_000_000, // 50k USDC eq
            analysis_interval: StdDuration::from_millis(100),
            bitmap_cache: Arc::new(RwLock::new(HashMap::new())),
            tick_array_cache: Arc::new(RwLock::new(HashMap::new())),
            inflight_guards: Arc::new(RwLock::new(HashMap::new())),
            // NEW:
            token_decimals_cache: Arc::new(RwLock::new(HashMap::new())),
            recent_opps_seen: Arc::new(RwLock::new(HashMap::new())),
            // NEW:
            slot_ms_cache: Arc::new(RwLock::new((Self::now_millis(), 400))), // default 400ms
            pool_state_cache: Arc::new(RwLock::new(HashMap::new())),
            // NEW:
            pair_index_cache: Arc::new(RwLock::new(HashMap::new())),
            // NEW: fresh blockhash TTL
            blockhash_cache: Arc::new(RwLock::new((0, Hash::default()))),
            // NEW: market-aware CU pricing
            fee_hist_cache: Arc::new(RwLock::new((0, 0))),
            // NEW: tick-array content-hash cache
            tick_array_hash_cache: Arc::new(RwLock::new(HashMap::new())),
            chunk_target: Arc::new(AtomicUsize::new(100)), // start near common RPC cap
            // NEW: no-change fast-path
            pool_ticks_hash_cache: Arc::new(RwLock::new(HashMap::new())),
            // NEW: pool account hash skip
            pool_account_hash_cache: Arc::new(RwLock::new(HashMap::new())),
            // NEW: per-slot send quota
            slot_quota: Arc::new(RwLock::new((Self::now_millis() / 400, 0))), // will adapt to measured slot ms
            // NEW: hedged RPC v2
            rpc_stats: Arc::new(RwLock::new(vec![(400.0, 0.01)])), // seed with sane defaults
            // NEW: fee-quantile anchoring
            fee_hist_vec_cache: Arc::new(RwLock::new((0, Arc::new(Vec::new())))),
            // NEW: execution calibration
            exec_calib: Arc::new(RwLock::new(HashMap::new())),
            // NEW: slot-level de-dup
            slot_seen: Arc::new(RwLock::new((Self::now_millis() / 400, HashSet::new()))),
            // NEW: CU usage model
            cu_usage_ewma: Arc::new(RwLock::new(HashMap::new())),
            // NEW: leader cache
            leader_cache: Arc::new(RwLock::new((0, Pubkey::default()))),
            // NEW: per-op in-flight guard
            inflight_op_guards: Arc::new(RwLock::new(HashMap::new())),
            // NEW: tick-array PDA cache
            tick_array_addr_cache: Arc::new(RwLock::new(HashMap::new())),
            // NEW: PnL EWMA
            pnl_ewma: Arc::new(RwLock::new(0.0)),
            // NEW: per-endpoint slot cache
            rpc_slot_cache: Arc::new(RwLock::new(vec![(0, 0.0)])),
            // NEW: bitmap→addresses cache
            bitmap_addr_cache: Arc::new(RwLock::new(HashMap::new())),
            // NEW: ALT caches/registry
            alt_cache: Arc::new(RwLock::new(HashMap::new())),
            alt_registry: Arc::new(RwLock::new(Vec::new())),
            // NEW: program hash cache for upgrade detection
            program_hash_cache: Arc::new(RwLock::new(HashMap::new())),
            // NEW: per-pool micro-cooldown
            pool_last_send: Arc::new(RwLock::new(HashMap::new())),
            // NEW: RPC inclusion EWMA tracking
            rpc_inclusion: Arc::new(RwLock::new(vec![0.6])),
            send_origin: Arc::new(RwLock::new(HashMap::new())),
            // NEW: ALT usefulness demotion
            alt_useless: Arc::new(RwLock::new(HashMap::new())),
            // NEW: leader inclusion model
            leader_inclusion: Arc::new(RwLock::new(HashMap::new())),
            // NEW: ALT negative cache
            alt_negative: Arc::new(RwLock::new(HashMap::new())),
            // NEW: per-slot account locks
            slot_account_locks: Arc::new(RwLock::new((Self::now_millis() / 400, Self::now_millis(), HashSet::new()))),
            // NEW: fail signature backoff by account-key hash
            fail_hash_ttl: Arc::new(RwLock::new(HashMap::new())),
            // NEW: circuit breaker for bad endpoints
            rpc_cb_until: Arc::new(RwLock::new(vec![0])),
            // NEW: payer registry for rotation
            payer_registry: Arc::new(RwLock::new(Vec::new())),
            // NEW: leader forecast cache
            leader_forecast_cache: Arc::new(RwLock::new((0, Vec::new()))),
            // NEW: per-leader processed latency
            leader_proc_lat: Arc::new(RwLock::new(HashMap::new())),
            // NEW: fail EWMA by type
            fail_ewma_by_type: Arc::new(RwLock::new(HashMap::new())),
            // NEW: pubsub for fast inclusion
            pubsub_ws_url: Arc::new(RwLock::new(Some(rpc_endpoint
                .replace("https://", "wss://")
                .replace("http://",  "ws://")
                .replace(":8899",   ":8900")))),
            pubsub:        Arc::new(RwLock::new(None)),
            inflight_sigs: Arc::new(RwLock::new(HashMap::new())),
            // NEW: TPU client
            tpu: Arc::new(RwLock::new(None)),
            // NEW: ALT address index
            alt_addr_index: Arc::new(RwLock::new(HashMap::new())),
            // NEW: signer fee floors
            signer_fee_floor: Arc::new(RwLock::new(HashMap::new())),
            
            // NEW: Optimization fields
            pool_slack_ewma: Arc::new(RwLock::new(HashMap::new())),
            alt_plan_cache: Arc::new(RwLock::new(HashMap::new())),
            leader_phase_hint: Arc::new(RwLock::new(HashMap::new())),
            send_origin: Arc::new(RwLock::new(HashMap::new())),
            
            // NEW: Additional optimization fields
            micro_bin_incl: Arc::new(RwLock::new(HashMap::new())),
            rpc_inflight: Arc::new(vec![AtomicUsize::new(0)]),
            micro_slot_registry: Arc::new(RwLock::new((Self::now_millis()/400, 0, HashSet::new()))),
            land_streak: Arc::new(RwLock::new(HashMap::new())),
            pool_last_activity: Arc::new(RwLock::new(HashMap::new())),
        };
        analyzer.start_status_sweeper();
        analyzer.start_logs_monitor();
        analyzer
    }

    /// === ADD: multi-endpoint constructor (desk uses 2–4 endpoints) ===
    pub fn new_multi(rpc_endpoints: &[&str]) -> Self {
        let mut ring: Vec<Arc<RpcClient>> = Vec::with_capacity(rpc_endpoints.len().min(MAX_RPC_RING));
        for e in rpc_endpoints.iter().take(MAX_RPC_RING) {
            ring.push(Arc::new(RpcClient::new_with_commitment((*e).to_string(), CommitmentConfig::processed())));
        }
        let primary = ring.first().cloned().expect("at least one RPC endpoint required");
        Self {
            rpc_client: primary,
            rpc_ring: Arc::new(ring),
            tick_history: Arc::new(RwLock::new(VecDeque::with_capacity(TICK_WINDOW_SIZE))),
            cluster_cache: Arc::new(RwLock::new(HashMap::new())),
            price_oracle: Arc::new(RwLock::new(HashMap::new())),
            liquidity_threshold: 50_000_000_000_000,
            analysis_interval: StdDuration::from_millis(100),
            bitmap_cache: Arc::new(RwLock::new(HashMap::new())),
            tick_array_cache: Arc::new(RwLock::new(HashMap::new())),
            inflight_guards: Arc::new(RwLock::new(HashMap::new())),
            token_decimals_cache: Arc::new(RwLock::new(HashMap::new())),
            recent_opps_seen: Arc::new(RwLock::new(HashMap::new())),
            slot_ms_cache: Arc::new(RwLock::new((Self::now_millis(), 400))),
            pool_state_cache: Arc::new(RwLock::new(HashMap::new())),
            pair_index_cache: Arc::new(RwLock::new(HashMap::new())),
            blockhash_cache: Arc::new(RwLock::new((0, Hash::default()))),
            fee_hist_cache: Arc::new(RwLock::new((0, 0))),
            tick_array_hash_cache: Arc::new(RwLock::new(HashMap::new())),
            chunk_target: Arc::new(AtomicUsize::new(100)),
            pool_ticks_hash_cache: Arc::new(RwLock::new(HashMap::new())),
            pool_account_hash_cache: Arc::new(RwLock::new(HashMap::new())),
            slot_quota: Arc::new(RwLock::new((Self::now_millis() / 400, 0))),
            rpc_stats: Arc::new(RwLock::new(vec![(400.0, 0.01); ring.len()])),
            fee_hist_vec_cache: Arc::new(RwLock::new((0, Arc::new(Vec::new())))),
            // NEW: execution calibration
            exec_calib: Arc::new(RwLock::new(HashMap::new())),
            // NEW: slot-level de-dup
            slot_seen: Arc::new(RwLock::new((Self::now_millis() / 400, HashSet::new()))),
            // NEW: CU usage model
            cu_usage_ewma: Arc::new(RwLock::new(HashMap::new())),
            // NEW: leader cache
            leader_cache: Arc::new(RwLock::new((0, Pubkey::default()))),
            // NEW: per-op in-flight guard
            inflight_op_guards: Arc::new(RwLock::new(HashMap::new())),
            // NEW: tick-array PDA cache
            tick_array_addr_cache: Arc::new(RwLock::new(HashMap::new())),
            // NEW: PnL EWMA
            pnl_ewma: Arc::new(RwLock::new(0.0)),
            // NEW: per-endpoint slot cache
            rpc_slot_cache: Arc::new(RwLock::new(vec![(0, 0.0); ring.len()])),
            // NEW: bitmap→addresses cache
            bitmap_addr_cache: Arc::new(RwLock::new(HashMap::new())),
            // NEW: ALT caches/registry
            alt_cache: Arc::new(RwLock::new(HashMap::new())),
            alt_registry: Arc::new(RwLock::new(Vec::new())),
            // NEW: program hash cache for upgrade detection
            program_hash_cache: Arc::new(RwLock::new(HashMap::new())),
            // NEW: per-pool micro-cooldown
            pool_last_send: Arc::new(RwLock::new(HashMap::new())),
            // NEW: RPC inclusion EWMA tracking
            rpc_inclusion: Arc::new(RwLock::new(vec![0.6; ring.len()])),
            send_origin: Arc::new(RwLock::new(HashMap::new())),
            // NEW: ALT usefulness demotion
            alt_useless: Arc::new(RwLock::new(HashMap::new())),
            // NEW: leader inclusion model
            leader_inclusion: Arc::new(RwLock::new(HashMap::new())),
            // NEW: ALT negative cache
            alt_negative: Arc::new(RwLock::new(HashMap::new())),
            // NEW: per-slot account locks
            slot_account_locks: Arc::new(RwLock::new((Self::now_millis() / 400, Self::now_millis(), HashSet::new()))),
            // NEW: fail signature backoff by account-key hash
            fail_hash_ttl: Arc::new(RwLock::new(HashMap::new())),
            // NEW: circuit breaker for bad endpoints
            rpc_cb_until: Arc::new(RwLock::new(vec![0; ring.len()])),
            // NEW: payer registry for rotation
            payer_registry: Arc::new(RwLock::new(Vec::new())),
            // NEW: leader forecast cache
            leader_forecast_cache: Arc::new(RwLock::new((0, Vec::new()))),
            // NEW: per-leader processed latency
            leader_proc_lat: Arc::new(RwLock::new(HashMap::new())),
            // NEW: fail EWMA by type
            fail_ewma_by_type: Arc::new(RwLock::new(HashMap::new())),
            // NEW: pubsub for fast inclusion
            pubsub_ws_url: Arc::new(RwLock::new(Some(rpc_endpoints[0]
                .replace("https://", "wss://")
                .replace("http://",  "ws://")
                .replace(":8899",   ":8900")))),
            pubsub:        Arc::new(RwLock::new(None)),
            inflight_sigs: Arc::new(RwLock::new(HashMap::new())),
            // NEW: TPU client
            tpu: Arc::new(RwLock::new(None)),
            // NEW: ALT address index
            alt_addr_index: Arc::new(RwLock::new(HashMap::new())),
            // NEW: signer fee floors
            signer_fee_floor: Arc::new(RwLock::new(HashMap::new())),
            
            // NEW: Optimization fields
            pool_slack_ewma: Arc::new(RwLock::new(HashMap::new())),
            alt_plan_cache: Arc::new(RwLock::new(HashMap::new())),
            leader_phase_hint: Arc::new(RwLock::new(HashMap::new())),
            send_origin: Arc::new(RwLock::new(HashMap::new())),
            
            // NEW: Additional optimization fields
            micro_bin_incl: Arc::new(RwLock::new(HashMap::new())),
            rpc_inflight: Arc::new((0..ring.len()).map(|_| AtomicUsize::new(0)).collect()),
            micro_slot_registry: Arc::new(RwLock::new((Self::now_millis()/400, 0, HashSet::new()))),
            land_streak: Arc::new(RwLock::new(HashMap::new())),
            pool_last_activity: Arc::new(RwLock::new(HashMap::new())),
        };
        analyzer.start_status_sweeper();
        analyzer.start_logs_monitor();
        analyzer
    }

    /// === ADD: register/replace the active LUT list (call this at startup) ===
    pub fn set_alt_registry(&self, luts: Vec<Pubkey>) {
        *self.alt_registry.write() = luts;
    }

    /// === ADD BELOW (helpers): hazard-optimal micro solver (182) ===
    #[inline(always)]
    fn fee_hist_ecdf(&self) -> Arc<Vec<u64>> {
        // sorted, cached in self.fee_hist_vec_cache by earlier patches
        self.fee_hist_vec_cache.read().1.clone()
    }

    #[inline(always)]
    fn micro_optimal_from_hist(&self, ev_lamports: u64, cu_estimate: u64) -> u64 {
        let xs = self.fee_hist_ecdf();
        if xs.is_empty() || ev_lamports == 0 || cu_estimate == 0 { return 0; }
        // Use unique bins only; evaluate sparse grid (≤64 points) for speed.
        let mut uniq: Vec<u64> = Vec::with_capacity(xs.len());
        let mut last = 0u64;
        for &v in xs.iter() {
            if v != last { uniq.push(v); last = v; }
        }
        let n = uniq.len().min(64);
        let step = (uniq.len().max(1) + n - 1) / n;
        let mut best_m = uniq[0];
        let mut best_obj = i128::MIN;
        let ev = ev_lamports as i128;
        let cu = cu_estimate as i128;

        for (k, &m) in uniq.iter().step_by(step).enumerate() {
            // ECDF at m: fraction ≤ m
            let idx = xs.partition_point(|q| *q <= m) as i128;
            let p = (idx as f64 / xs.len().max(1) as f64).clamp(0.0, 1.0);
            // Slight tail sharpening late in slot
            let phase = self.slot_phase_0_to_1();
            let p_adj = (p.powf(1.0 + 0.6 * phase)).clamp(0.0, 1.0);
            let obj = (p_adj * ev as f64 - (m as i128 * cu / 1_000_000) as f64).round() as i128;
            if obj > best_obj { best_obj = obj; best_m = m; }
        }
        best_m.max(1)
    }

    /// === ADD BELOW (helpers): per-pool slippage-slack learner (183) ===
    #[inline(always)]
    fn note_exec_slippage(&self, pool: &Pubkey, rel_move: f64) {
        let mut w = self.pool_slack_ewma.write();
        let s = w.get(pool).cloned().unwrap_or(SLACK_MIN);
        let nm = (1.0 - SLACK_EWMA_ALPHA) * s + SLACK_EWMA_ALPHA * rel_move.abs();
        w.insert(*pool, nm.clamp(SLACK_MIN, SLACK_MAX));
    }

    #[inline(always)]
    fn sqrt_price_limit_for(&self, pool: &PoolState, side: SwapSide, max_rel_move: f64) -> u128 {
        let base = self.sqrt_price_to_price(pool.sqrt_price_x64).max(1e-18);
        let slack = self.pool_slack_ewma.read().get(&pool.address).cloned().unwrap_or(SLACK_MIN);
        let mv = (max_rel_move * (1.0 + slack)).clamp(0.0001, 0.50);
        let p_tgt = match side {
            SwapSide::Token0In => base * (1.0 - mv).max(1e-12),
            SwapSide::Token1In => base * (1.0 + mv),
        };
        self.price_to_sqrt_x64(p_tgt)
    }

    /// === ADD BELOW (helpers): ALT plan cache (184) ===
    #[inline(always)]
    fn fnv64_pubkeys<I: IntoIterator<Item = Pubkey>>(&self, it: I) -> u64 {
        let mut h = 0xcbf29ce484222325u64;
        for a in it {
            for b in a.to_bytes() { h ^= b as u64; h = h.wrapping_mul(0x100000001b3); }
        }
        h
    }

    /// === ADD: ALT plan cache with hash-based lookup table reuse (184) ===
    async fn choose_alts_for_accounts(&self, needed_accounts: &[Pubkey]) -> Vec<AddressLookupTableAccount> {
        let key = self.fnv64_pubkeys(needed_accounts.iter().cloned());
        let now = Self::now_millis();
        
        // Check cache first
        if let Some((ts, plan)) = self.alt_plan_cache.read().get(&key).cloned() {
            if now.saturating_sub(ts) <= ALT_PLAN_TTL_MS {
                let mut out = Vec::with_capacity(plan.len());
                for pk in plan {
                    if let Some(l) = self.fetch_alt_ttl(&pk).await { out.push(l); }
                }
                if !out.is_empty() { return out; }
            }
        }

        // Build needed set with must-hit accounts
        let mut needed: HashSet<Pubkey> = needed_accounts.iter().cloned().collect();
        for mh in self.must_hit_pubkeys() { 
            needed.insert(mh); 
        }

        // Greedy selection of ALTs
        let mut chosen = Vec::new();
        let registry = self.alt_registry.read().clone();
        
        for lut_pubkey in registry {
            if let Some(alt) = self.fetch_alt_ttl(&lut_pubkey).await {
                let addresses: HashSet<Pubkey> = alt.addresses.iter().cloned().collect();
                if addresses.intersection(&needed).count() > 0 {
                    chosen.push(alt);
                    // Remove covered accounts from needed set
                    needed = needed.difference(&addresses).cloned().collect();
                    if needed.is_empty() { break; }
                }
            }
        }

        // Store plan in cache
        self.alt_plan_cache.write().insert(key, (now, chosen.iter().map(|l| l.key).collect()));
        chosen
    }

    /// === ADD BELOW (helpers): leader-phase micro hint memory (187) ===
    #[inline(always)]
    fn phase_bucket(&self) -> u8 {
        let p = (self.slot_phase_0_to_1() * PHASE_BUCKETS as f64).floor() as i32;
        p.clamp(0, (PHASE_BUCKETS-1) as i32) as u8
    }

    #[inline(always)]
    fn note_leader_phase_hint(&self, leader: &Pubkey, micro: u64) {
        self.leader_phase_hint.write().insert((*leader, self.phase_bucket()), (Self::now_millis(), micro));
    }

    #[inline(always)]
    fn leader_phase_hint_micro(&self) -> Option<u64> {
        let leader = tokio::task::block_in_place(|| futures::executor::block_on(self.fetch_slot_leader_ttl()));
        let b = self.phase_bucket();
        if let Some((ts, m)) = self.leader_phase_hint.read().get(&(leader, b)).cloned() {
            if Self::now_millis().saturating_sub(ts) <= HINT_TTL_MS { return Some(m); }
        }
        None
    }

    #[inline(always)]
    fn note_send_origin_with_micro(&self, sig: &Signature, idx: usize, micro: u64) {
        let leader = tokio::task::block_in_place(|| futures::executor::block_on(self.fetch_slot_leader_ttl()));
        self.send_origin.write().insert(*sig, (Self::now_millis(), idx, leader, micro));
    }

    /// === ADD BELOW (helpers): signature-permuted RPC order (186) ===
    #[inline(always)]
    fn permuted_rpc_order_for_sig(&self, sig: &Signature) -> Vec<usize> {
        let mut order = self.rpc_rank_send();
        if order.len() <= 1 { return order; }
        let b = sig.as_ref();
        // 64-bit mix from first 8 bytes
        let k = u64::from_le_bytes([b[0],b[1],b[2],b[3],b[4],b[5],b[6],b[7]]).rotate_left(17) ^ 0x9E3779B185EBCA87;
        order.rotate_left((k as usize) % order.len());
        order
    }

    /// === ADD BELOW (helpers): must-hit pubkeys (189) ===
    fn must_hit_pubkeys(&self) -> Vec<Pubkey> {
        ALT_MUST_HIT.iter().filter_map(|s| Pubkey::from_str(s).ok()).collect()
    }

    /// === ADD BELOW (helpers): extract micro from compute budget instruction ===
    #[inline(always)]
    fn extract_micro_from_cb_ix(&self, ix: &Instruction) -> Option<u64> {
        if ix.program_id == solana_sdk::compute_budget::id() && ix.data.len() >= 9 {
            if let Some(&tag) = ix.data.first() {
                if tag == 3 { // SetComputeUnitPrice
                    if ix.data.len() >= 9 {
                        return Some(u64::from_le_bytes([
                            ix.data[1], ix.data[2], ix.data[3], ix.data[4],
                            ix.data[5], ix.data[6], ix.data[7], ix.data[8]
                        ]));
                    }
                }
            }
        }
        None
    }

    /// === ADD BELOW (helpers): broadcast with signature-permuted RPC order (186) ===
    async fn broadcast_tpu_first(&self, tx: &VersionedTransaction) -> Result<Signature, Box<dyn std::error::Error>> {
        let sig0 = tx.signatures.get(0).cloned().unwrap_or(Signature::default());
        let order = self.permuted_rpc_order_for_sig(&sig0);
        
        for (k, &i) in order.iter().enumerate() {
            let cli = self.rpc_ring[i].clone();
            if k > 0 { 
                tokio::time::sleep(TokioDuration::from_millis(self.hedge_stagger_ms() * k as u64)).await; 
            }
            if let Ok(sig) = cli.send_and_confirm_transaction_with_spinner_and_config(
                tx,
                CommitmentConfig::processed(),
                RpcSendTransactionConfig {
                    skip_preflight: false,
                    preflight_commitment: Some(CommitmentLevel::Processed),
                    max_retries: Some(0),
                    min_context_slot: None,
                }
            ).await {
                return Ok(sig);
            }
        }
        // Fallback on stale cluster - try TPU again
        if self.rpc_cluster_stale() {
            if let Some(tpu) = self.ensure_tpu().await {
                if let Ok(Ok(sig)) = tokio::time::timeout(TokioDuration::from_millis(120), tpu.send_transaction(tx.clone())).await {
                    let best = *self.rpc_rank_send().first().unwrap_or(&0);
                    self.note_send_origin_with_micro(&sig, best, 0);
                    return Ok(sig);
                }
            }
        }
        Err("All RPC endpoints failed".into())
    }

    /// === ADD BELOW (helpers): logs subscription monitor (185) ===
    fn start_logs_monitor(&self) {
        let this = self.clone();
        tokio::spawn(async move {
            if let Some(ps) = this.ensure_pubsub().await {
                let payers = this.payer_registry.read().clone();
                if payers.is_empty() { return; }
                let filt = RpcTransactionLogsFilter::Mentions(payers.iter().map(|p| p.to_string()).collect());
                if let Ok((mut sub, _)) = ps.logs_subscribe(filt, Some(RpcTransactionLogsConfig {
                    commitment: Some(solana_sdk::commitment_config::CommitmentConfig::processed()),
                })).await {
                    loop {
                        if let Ok(msg) = sub.recv().await {
                            let sig = msg.value.signature.parse::<Signature>().unwrap_or_default();
                            // If runtime returned error, tag as non-landed immediately.
                            if msg.value.err.is_some() {
                                this.record_sig_inclusion(&sig, false);
                                // also mark the writable set hash if we still have it in origin map
                                if let Some((_, _, _leader)) = this.send_origin.read().get(&sig).cloned().map(|(t,i,l,_)| (t,i,l)) {
                                    // (optional) nothing else needed; backoffs wired elsewhere on fail verdict
                                }
                            }
                        } else { break; }
                    }
                }
            }
        });
    }

    /// === ADD BELOW (helpers): micro-bin inclusion learner (190) ===
    #[inline(always)]
    fn note_micro_bin_result(&self, micro: u64, landed: bool) {
        let y = if landed { 1.0 } else { 0.0 };
        let now = Self::now_millis();
        let mut w = self.micro_bin_incl.write();
        let p = w.get(&micro).map(|(_,p)| *p).unwrap_or(0.55);
        let n = (1.0 - MICRO_BIN_ALPHA) * p + MICRO_BIN_ALPHA * y;
        w.insert(micro, (now, n.clamp(0.05, 0.98)));
    }

    #[inline(always)]
    fn micro_bin_best_neighbor(&self, micro: u64) -> u64 {
        // pick among {micro−1, micro, micro+1, micro+2} the highest inclusion EWMA (fresh)
        let now = Self::now_millis();
        let mut best = (micro, -1.0f64);
        let mut cand = [micro.saturating_sub(1), micro, micro.saturating_add(1), micro.saturating_add(2)];
        for &m in cand.iter() {
            if let Some((ts, p)) = self.micro_bin_incl.read().get(&m).cloned() {
                if now.saturating_sub(ts) <= MICRO_BIN_TTL_MS && p > best.1 { best = (m, p); }
            }
        }
        best.0
    }

    /// === ADD BELOW (helpers): endpoint concurrency governor (191) ===
    #[inline(always)]
    fn rpc_concurrency_cap(&self, idx: usize) -> usize {
        let (_ms, err) = self.rpc_stats.read().get(idx).cloned().unwrap_or((300.0, 0.02));
        let base = 6usize;
        let cut  = (err * 10.0).round() as isize; // 0..~10
        (base as isize - cut).clamp(2, 8) as usize
    }

    #[inline(always)]
    fn rpc_try_acquire(&self, idx: usize) -> bool {
        let cap = self.rpc_concurrency_cap(idx);
        let cur = self.rpc_inflight[idx].load(Ordering::Relaxed);
        if cur >= cap { return false; }
        self.rpc_inflight[idx].fetch_add(1, Ordering::Relaxed);
        true
    }

    #[inline(always)]
    fn rpc_release(&self, idx: usize) {
        self.rpc_inflight[idx].fetch_sub(1, Ordering::Relaxed);
    }

    /// === ADD BELOW (helpers): global micro de-dupe (192) ===
    #[inline(always)]
    fn reserve_unique_micro(&self, mut micro: u64) -> u64 {
        let ms = tokio::task::block_in_place(|| futures::executor::block_on(self.fetch_slot_ms())).max(200);
        let epoch = Self::now_millis()/ms;
        let phase_b = self.phase_bucket();
        let mut w = self.micro_slot_registry.write();
        if w.0 != epoch || w.1 != phase_b || Self::now_millis().saturating_sub(w.0*ms) > MICRO_REG_TTL_MS {
            *w = (epoch, phase_b, HashSet::new());
        }
        while w.2.contains(&micro) {
            micro = self.sparse_bin_hop_micro(micro.saturating_add(1));
        }
        w.2.insert(micro);
        micro
    }

    /// === ADD BELOW (helpers): SPL token source balance guard (193) ===
    #[inline(always)]
    async fn token_account_has_min(&self, acc: &Pubkey, min_amount: u64) -> bool {
        if let Ok(a) = self.rpc_client.get_account(acc).await {
            if let Ok(tok) = spl_token::state::Account::unpack_from_slice(&a.data) {
                return tok.amount >= min_amount;
            }
        }
        true // on RPC miss, be permissive
    }

    /// === ADD BELOW (helpers): fallback read path on stale cluster (194) ===
    #[inline(always)]
    fn rpc_cluster_stale(&self) -> bool {
        let slots = self.rpc_slot_cache.read().clone();
        if slots.len() < 2 { return false; }
        let mut v: Vec<f64> = slots.iter().map(|(_,s)| *s).collect();
        v.sort_by(|a,b| a.partial_cmp(b).unwrap());
        let med = v[v.len()/2];
        let best = *v.last().unwrap_or(&med);
        (best - med) > CLUSTER_STALE_SLOTS
    }

    /// === ADD BELOW (helpers): cost guard taper (195) ===
    #[inline(always)]
    fn note_land_streak(&self, landed: bool) {
        let leader = tokio::task::block_in_place(|| futures::executor::block_on(self.fetch_slot_leader_ttl()));
        let key = (leader, self.phase_bucket());
        let now = Self::now_millis();
        let mut w = self.land_streak.write();
        let (ts, c) = w.get(&key).cloned().unwrap_or((0,0));
        let c2 = if landed && now.saturating_sub(ts) <= LAND_STREAK_TTL_MS { c.saturating_add(1) } else if landed { 1 } else { 0 };
        w.insert(key, (now, c2));
    }

    #[inline(always)]
    fn micro_taper_from_streak(&self, micro: u64) -> u64 {
        let leader = tokio::task::block_in_place(|| futures::executor::block_on(self.fetch_slot_leader_ttl()));
        let key = (leader, self.phase_bucket());
        if let Some((ts, c)) = self.land_streak.read().get(&key).cloned() {
            if c >= 2 && Self::now_millis().saturating_sub(ts) <= LAND_STREAK_TTL_MS {
                return ((micro as f64) * LAND_STREAK_TAPER).round().max(1.0) as u64;
            }
        }
        micro
    }

    /// === ADD BELOW (helpers): pool-anti-burst (196) ===
    #[inline(always)]
    fn rung_spacing_ms_for_pool(&self, pool: &Pubkey) -> u64 {
        let base = self.rung_spacing_ms();
        if let Some(ts) = self.pool_last_activity.read().get(pool).cloned() {
            let dt = Self::now_millis().saturating_sub(ts);
            if dt <= POOL_BURST_WINDOW_MS {
                return ((base as f64) * POOL_RUNG_SPACING_BOOST).round() as u64;
            }
        }
        base
    }

    /// === ADD BELOW (helpers): best-of-two micro finalize (197) ===
    #[inline(always)]
    fn choose_micro_best_of_two(&self, a: u64, b: u64, ev_lamports: u64, cu_est: u64) -> u64 {
        if a == b { return a; }
        let xs = self.fee_hist_ecdf();
        if xs.is_empty() { return b; }
        let ecdf = |m: u64| -> f64 { (xs.partition_point(|q| *q <= m) as f64 / xs.len().max(1) as f64).clamp(0.0,1.0) };
        let pa = ecdf(a); let pb = ecdf(b);
        let ev = ev_lamports as f64;
        let cu = cu_est as f64 / 1_000_000.0;
        let va = pa*ev - (a as f64)*cu;
        let vb = pb*ev - (b as f64)*cu;
        if vb > va { b } else { a }
    }

    /// === ADD BELOW (helpers): writable extraction + reserve ===
    #[inline(always)]
    fn writable_accounts(ixs: &[Instruction]) -> Vec<Pubkey> {
        let mut set: HashSet<Pubkey> = HashSet::with_capacity(ixs.len() * 4);
        for ix in ixs {
            for m in &ix.accounts {
                if m.is_writable { set.insert(m.pubkey); }
            }
        }
        set.into_iter().collect()
    }

    #[inline(always)]
    fn reserve_account_locks(&self, accounts: &[Pubkey]) -> bool {
        let ms = tokio::task::block_in_place(|| futures::executor::block_on(self.fetch_slot_ms()));
        let slot_epoch = Self::now_millis() / ms.max(200);
        let now = Self::now_millis();
        let mut w = self.slot_account_locks.write();
        if w.0 != slot_epoch || now.saturating_sub(w.1) > LOCKSET_TTL_MS {
            *w = (slot_epoch, now, HashSet::new());
        }
        if accounts.iter().any(|k| w.2.contains(k)) { return false; }
        for k in accounts { w.2.insert(*k); }
        true
    }

    /// === ADD BELOW (helpers): signed tick drift EMA ===
    #[inline(always)]
    fn tick_drift_sign(&self, pool: &Pubkey) -> i32 {
        if let Some(a) = self.cluster_cache.read().get(pool) {
            if let Some(dc) = &a.dominant_cluster {
                // use center vs current tick sign as simple drift proxy this cycle
                return (a.liquidity_distribution.current_tick - dc.center_tick).signum();
            }
        }
        0
    }

    /// === ADD BELOW (helpers): oracle guard ===
    #[inline(always)]
    fn oracle_ok(&self, pool: &PoolState) -> bool {
        // Assume price_oracle cache stores (ts, value) in lamports/native ratio you already use
        if let Some((ts, val)) = self.price_oracle.read().get(&pool.token_mint_a).cloned() {
            if Self::now_millis().saturating_sub(ts) > ORACLE_STALE_MS { return false; }
            if let Some(drift) = self.oracle_drift_ratio(pool) {
                return drift <= ORACLE_DRIFT_MAX;
            }
            return true;
        }
        true
    }

    /// === ADD BELOW (helpers): classify AccountInUse ===
    #[inline(always)]
    fn is_account_in_use_err(e: &anyhow::Error) -> bool {
        let s = format!("{:?}", e);
        s.contains("AccountInUse") || s.contains("account in use")
    }

    /// === ADD BELOW (helpers): hash of writable accounts, gate + record on failure ===
    #[inline(always)]
    fn hash_writables(accounts: &[Pubkey]) -> u64 {
        let mut h = 0xcbf29ce484222325u64;
        for a in accounts {
            let b = a.to_bytes();
            // fold in 8B chunks
            for c in b.chunks(8) {
                let mut x = 0u64;
                for (i, &bb) in c.iter().enumerate() { x |= (bb as u64) << (i * 8); }
                h ^= x; h = h.wrapping_mul(0x100000001b3);
            }
        }
        h
    }

    #[inline(always)]
    fn fail_backoff_allows(&self, wr: &[Pubkey]) -> bool {
        let key = Self::hash_writables(wr);
        let now = Self::now_millis();
        let mut w = self.fail_hash_ttl.write();
        if let Some(ts) = w.get(&key).cloned() {
            if now.saturating_sub(ts) < FAIL_BACKOFF_MS { return false; }
        }
        true
    }

    #[inline(always)]
    fn note_failed_writable_set(&self, wr: &[Pubkey]) {
        let key = Self::hash_writables(wr);
        self.fail_hash_ttl.write().insert(key, Self::now_millis());
    }

    /// === ADD: wrapper for AccountInUse quick retry ===
    async fn broadcast_with_account_in_use_retry(&self, tx: &VersionedTransaction) -> Result<Signature, Box<dyn std::error::Error>> {
        match self.broadcast_versioned_transaction_hedged(tx).await {
            Ok(sig) => Ok(sig),
            Err(e) if format!("{:?}", e).contains("AccountInUse") => {
                tokio::time::sleep(TokioDuration::from_millis(12)).await;
                let sig = self.broadcast_versioned_transaction_hedged(tx).await?;
                Ok(sig)
            }
            Err(e) => Err(e),
        }
    }

    /// === ADD BELOW (helpers): per-endpoint timeout in ms ===
    #[inline(always)]
    fn rpc_dynamic_timeout_ms(&self, idx: usize) -> u64 {
        let (ema_ms, ema_err) = self.rpc_stats.read().get(idx).cloned().unwrap_or((300.0, 0.02));
        let ms = (2.6 * ema_ms + DYN_TO_ERR_BOOST * ema_err).round() as u64;
        ms.clamp(DYN_TO_MIN_MS, DYN_TO_MAX_MS)
    }

    /// === ADD BELOW (helpers): dynamic TTL from IQR & slot phase ===
    #[inline(always)]
    fn fee_hist_ttl_ms(&self) -> u64 {
        let (ts, xs) = self.fee_hist_vec_cache.read().clone();
        if xs.len() < 8 { return FEE_HIST_TTL_BASE_MS; }
        let pick = |p: f64| -> u64 {
            let i = ((xs.len() as f64 - 1.0) * p).round() as usize;
            xs[i]
        };
        let med = pick(0.50).max(1);
        let iqr = pick(0.75).saturating_sub(pick(0.25));
        let ratio = (iqr as f64) / (med as f64); // 0..∞
        let phase = self.slot_phase_0_to_1();    // 0..1
        let mult = 1.0 / (1.0 + 1.6 * ratio) * (1.0 - 0.5 * phase); // wider market / later slot → shorter TTL
        (FEE_HIST_TTL_BASE_MS as f64 * mult)
            .round()
            .clamp(FEE_HIST_TTL_MIN_MS as f64, FEE_HIST_TTL_MAX_MS as f64) as u64
    }

    /// === ADD BELOW (helpers): breaker helpers ===
    #[inline(always)]
    fn rpc_cb_penalty(&self, idx: usize) -> f64 {
        let until = *self.rpc_cb_until.read().get(idx).unwrap_or(&0);
        if Self::now_millis() < until { 1e9 } else { 0.0 } // push to the back
    }

    #[inline(always)]
    fn rpc_maybe_trip_cb(&self, idx: usize) {
        let (ema_ms, ema_err) = self.rpc_stats.read().get(idx).cloned().unwrap_or((300.0, 0.02));
        let incl = *self.rpc_inclusion.read().get(idx).unwrap_or(&0.6);
        if ema_err >= RPC_CB_ERR_THRESH && incl <= RPC_CB_INCL_THRESH {
            let mut w = self.rpc_cb_until.write();
            if let Some(u) = w.get_mut(idx) { *u = Self::now_millis().saturating_add(RPC_CB_TTL_MS); }
        }
    }

    /// === ADD BELOW (helpers): pick payer deterministically ===
    #[inline(always)]
    fn payer_index_for_slot(&self) -> Option<usize> {
        let payers = self.payer_registry.read();
        if payers.is_empty() { return None; }
        let leader = tokio::task::block_in_place(|| futures::executor::block_on(self.fetch_slot_leader_ttl()));
        let ms = tokio::task::block_in_place(|| futures::executor::block_on(self.fetch_slot_ms()));
        let slot_epoch = Self::now_millis() / ms.max(200);
        let mut x = Self::pubkey_u64(&leader) ^ slot_epoch;
        Some((x as usize) % payers.len())
    }

    /// === ADD BELOW (helpers): skip dense bins when nudging fee ===
    #[inline(always)]
    fn sparse_bin_hop_micro(&self, micro: u64) -> u64 {
        let (_, xs) = self.fee_hist_vec_cache.read().clone();
        if xs.is_empty() { return micro; }
        // binary search lower_bound
        let mut lo = 0usize; let mut hi = xs.len();
        while lo < hi {
            let mid = (lo + hi) / 2;
            if xs[mid] <= micro { lo = mid + 1; } else { hi = mid; }
        }
        let mut i = lo;
        // measure local run-length and skip overly dense bins
        while i < xs.len() {
            let v = xs[i];
            let mut j = i + 1;
            while j < xs.len() && xs[j] == v { j += 1; }
            let run = j - i; // density proxy
            if run <= 3 { return v.saturating_add(1); } // prefer sparse
            i = j;
        }
        micro
    }

    /// === ADD BELOW (helpers): rung spacing ms ===
    #[inline(always)]
    fn rung_spacing_ms(&self) -> u64 {
        let phase = self.slot_phase_0_to_1();
        let leader = tokio::task::block_in_place(|| futures::executor::block_on(self.fetch_slot_leader_ttl()));
        let q = *self.leader_inclusion.read().get(&leader).unwrap_or(&0.6);
        if phase >= 0.85 && q >= 0.55 { 4 }       // strong tail, good leader → closer rungs
        else if phase >= 0.85 { 6 }
        else if q < 0.45 { 8 }                    // weak leader → give more time
        else { 6 }
    }

    /// === ADD BELOW (helpers): opportunistic background refresh ===
    fn maybe_refresh_fee_hist_bg(&self) {
        let now = Self::now_millis();
        let (ts, xs) = self.fee_hist_vec_cache.read().clone();
        if now.saturating_sub(ts) <= self.fee_hist_ttl_ms() { return; }
        let this = self.clone();
        tokio::spawn(async move {
            // try primary then ring (reuse fetch pattern from fee_quantile_micro)
            let fetch = |cli: Arc<RpcClient>| async move { cli.get_recent_prioritization_fees().await };
            let mut vec = Vec::new();
            if let Ok(v) = fetch(this.rpc_client.clone()).await {
                vec = v.into_iter().map(|f| f.prioritization_fee).collect();
            } else {
                for c in this.rpc_ring.iter().skip(1) {
                    if let Ok(v) = fetch(c.clone()).await {
                        vec = v.into_iter().map(|f| f.prioritization_fee).collect();
                        break;
                    }
                }
            }
            if !vec.is_empty() {
                vec.sort_unstable();
                *this.fee_hist_vec_cache.write() = (Self::now_millis(), Arc::new(vec));
            }
        });
    }

    /// === ADD BELOW (helpers): inclusion-aware rung count ===
    #[inline(always)]
    fn ladder_levels(&self) -> usize {
        let phase = self.slot_phase_0_to_1();
        let q60 = tokio::task::block_in_place(|| futures::executor::block_on(self.fee_quantile_micro(0.60))) as f64;
        let q90 = tokio::task::block_in_place(|| futures::executor::block_on(self.fee_quantile_micro(0.90))) as f64;
        let spread = if q60 > 0.0 { (q90 - q60) / q60 } else { 0.0 };
        let leader = tokio::task::block_in_place(|| futures::executor::block_on(self.fetch_slot_leader_ttl()));
        let lq = *self.leader_inclusion.read().get(&leader).unwrap_or(&0.6);

        if phase >= 0.80 && (spread >= 0.45 || lq >= 0.55) { 3 }
        else if spread >= 0.65 { 3 }
        else { 2 }
    }

    /// register list of payers (pubkeys must correspond to provided Signers at callsites)
    pub fn set_payer_registry(&self, payers: Vec<Pubkey>) { 
        *self.payer_registry.write() = payers; 
    }

    /// === ADD BELOW (helpers): forecast next leaders with TTL ===
    #[inline(always)]
    async fn forecast_next_leaders(&self, horizon: u64) -> Vec<Pubkey> {
        let now = Self::now_millis();
        {
            let (ts, v) = self.leader_forecast_cache.read().clone();
            if now.saturating_sub(ts) <= LEADER_FORECAST_TTL_MS && !v.is_empty() { return v; }
        }
        let mut out = Vec::new();
        if let Ok(slot) = self.rpc_client.get_slot().await {
            if let Ok(v) = self.rpc_client.get_slot_leaders(slot, horizon).await {
                out = v;
            }
        }
        *self.leader_forecast_cache.write() = (now, out.clone());
        out
    }

    /// === ADD BELOW (helpers): quota tilt from forecasted leader quality ===
    #[inline(always)]
    async fn quota_tilt(&self) -> f64 {
        let fores = self.forecast_next_leaders(LEADER_FORECAST_HORIZON).await;
        if fores.is_empty() { return 1.0; }
        let li = self.leader_inclusion.read();
        let mut sum = 0.0;
        let mut n = 0.0;
        for pk in fores.iter().take(8) { // nearer leaders weigh more
            sum += *li.get(pk).unwrap_or(&0.6);
            n += 1.0;
        }
        if n == 0.0 { return 1.0; }
        let avg = (sum / n).clamp(0.2, 0.95);
        // Map avg ∈[0.2..0.95] to tilt ∈[1-QUOTA_TILT_MAX .. 1+QUOTA_TILT_MAX]
        let t = (avg - 0.55) / 0.35; // -1..+1 approx
        (1.0 + (t * QUOTA_TILT_MAX)).clamp(1.0 - QUOTA_TILT_MAX, 1.0 + QUOTA_TILT_MAX)
    }

    /// === ADD BELOW (helpers): record processed latency (call from fast_sig_processed) ===
    #[inline(always)]
    fn note_leader_processed_latency(&self, sent_ts: u64, processed_now: bool) {
        if !processed_now { return; }
        let leader = tokio::task::block_in_place(|| futures::executor::block_on(self.fetch_slot_leader_ttl()));
        let dt = Self::now_millis().saturating_sub(sent_ts).clamp(PROC_LAT_MIN_MS, PROC_LAT_MAX_MS) as f64;
        let mut w = self.leader_proc_lat.write();
        let e = w.entry(leader).or_insert(dt);
        *e = (1.0 - PROC_LAT_ALPHA) * *e + PROC_LAT_ALPHA * dt;
    }

    /// === ADD BELOW (helpers): dynamic RUNG_WAIT ===
    #[inline(always)]
    fn rung_wait_ms(&self) -> u64 {
        let leader = tokio::task::block_in_place(|| futures::executor::block_on(self.fetch_slot_leader_ttl()));
        let ms = *self.leader_proc_lat.read().get(&leader).unwrap_or(&(RUNG_WAIT_MS as f64));
        ms.round().clamp(40.0, 180.0) as u64
    }

    /// === ADD BELOW (helpers): rung-specific CU limit ===
    #[inline(always)]
    fn cu_limit_for_rung(&self, op: &MevOpportunity, rung_idx: usize) -> u32 {
        let base = self.gas_limit_for_op(op) as f64;
        let mult = CU_RUNG_UPLIFTS[rung_idx.min(CU_RUNG_UPLIFTS.len()-1)];
        (base * mult).round().clamp(CU_MIN_LIMIT as f64, CU_MAX_LIMIT as f64) as u32
    }

    /// === ADD BELOW (helpers): compute budget with explicit CU limit ===
    #[inline(always)]
    fn compute_budget_ixs_for_op_with_limit(&self, cu_limit: u32, cu_price_micro_lamports: u64) -> [Instruction; 2] {
        [
            ComputeBudgetInstruction::set_compute_unit_limit(cu_limit),
            ComputeBudgetInstruction::set_compute_unit_price(cu_price_micro_lamports),
        ]
    }

    /// === ADD BELOW (helpers): hotness penalty ===
    #[inline(always)]
    fn pool_hot_penalty(&self, pool: &Pubkey) -> f64 {
        if let Some(ts) = self.pool_last_send.read().get(pool).cloned() {
            let dt = Self::now_millis().saturating_sub(ts);
            if dt <= POOL_HOT_PENALTY_MS { return POOL_HOT_PENALTY; }
        }
        0.0
    }

    /// === ADD BELOW (helpers): score pool pair leg by effective price & fee ===
    #[inline(always)]
    fn leg_effective_ratio(&self, pool: &PoolState) -> f64 {
        // approx: on small sizes, use fee-only penalty; impact handled later by cluster functions
        let total_fee = (pool.fee_rate as f64 + pool.protocol_fee_rate as f64) / BASIS_POINT_MAX as f64;
        (1.0 - total_fee).clamp(0.90, 1.0)
    }

    /// === ADD BELOW (helpers): note fail/success per type (call alongside record_exec_outcome) ===
    #[inline(always)]
    fn note_type_outcome(&self, typ: OpportunityType, ok: bool) {
        let k = Self::type_code(typ);
        let mut w = self.fail_ewma_by_type.write();
        let p = w.get(&k).cloned().unwrap_or(0.30); // start modest
        let y = if ok { 0.0 } else { 1.0 };
        w.insert(k, (1.0 - FAIL_EWMA_ALPHA) * p + FAIL_EWMA_ALPHA * y);
    }

    /// === ADD BELOW (helpers): EV scaler ===
    #[inline(always)]
    fn ev_fail_scaler(&self, typ: OpportunityType) -> f64 {
        let k = Self::type_code(typ);
        let p = *self.fail_ewma_by_type.read().get(&k).unwrap_or(&0.30);
        (1.0 - p).clamp(0.6, 1.0) // 0.6..1.0 multiplier
    }

    /// === ADD BELOW (helpers): ensure pubsub client, subscribe signatures on send ===
    async fn ensure_pubsub(&self) -> Option<Arc<PubsubClient>> {
        if self.pubsub.read().as_ref().and_then(|o| o.as_ref()).is_some() { 
            return self.pubsub.read().clone();
        }
        let Some(url) = self.pubsub_ws_url.read().clone() else { return None; };
        let timeout_at = Self::now_millis().saturating_add(PUBSUB_CONNECT_TTL_MS);
        loop {
            match PubsubClient::new(&url).await {
                Ok(cli) => {
                    let arc = Arc::new(cli);
                    *self.pubsub.write() = Some(arc.clone());
                    return Some(arc);
                }
                Err(_) if Self::now_millis() < timeout_at => { tokio::time::sleep(TokioDuration::from_millis(200)).await; }
                Err(_) => return None,
            }
        }
    }

    #[inline(always)]
    fn sub_signature_fast(&self, sig: Signature) {
        let this = self.clone();
        self.inflight_sigs.write().insert(sig, Self::now_millis());
        tokio::spawn(async move {
            if let Some(ps) = this.ensure_pubsub().await {
                // one-off subscribe; auto-unsub on first response or TTL
                let cfg = RpcSignatureSubscribeConfig {
                    commitment: Some(solana_sdk::commitment_config::CommitmentConfig::processed()),
                    enable_received_notification: Some(true),
                };
                if let Ok((mut sub, _unsub)) = ps.signature_subscribe(&sig, Some(cfg)).await {
                    let start = Self::now_millis();
                    while let Ok(msg) = tokio::time::timeout(TokioDuration::from_millis(400), sub.recv()).await {
                        if let Ok(val) = msg {
                            let landed = val.value.err.is_none();
                            this.record_sig_inclusion(&sig, landed);
                            if let Some(ts) = this.send_origin.read().get(&sig).map(|x| x.0) {
                                this.note_leader_processed_latency(ts, true);
                            }
                            break;
                        }
                        if Self::now_millis().saturating_sub(start) > SIG_SUB_TTL_MS { break; }
                    }
                }
            }
            this.inflight_sigs.write().remove(&sig);
        });
    }

    /// === ADD BELOW (helpers): exact size of a versioned tx ===
    #[inline(always)]
    fn exact_tx_size(&self, vtx: &VersionedTransaction) -> usize {
        bincode::serialize(vtx).map(|v| v.len()).unwrap_or_else(|_| self.approx_tx_size(vtx))
    }

    /// === ADD BELOW (helpers): ensure async TPU client ===
    async fn ensure_tpu(&self) -> Option<Arc<solana_tpu_client::nonblocking::tpu_client::TpuClient>> {
        if !TPU_FIRST_ENABLE { return None; }
        if let Some(cli) = self.tpu.read().clone() { return Some(cli); }
        let ws = self.pubsub_ws_url.read().clone()?;
        let cfg = solana_tpu_client::tpu_client::TpuClientConfig::default(); // QUIC fanout + TPU/forwarders
        match solana_tpu_client::nonblocking::tpu_client::TpuClient::new_with_client(self.rpc_client.clone(), &ws, cfg).await {
            Ok(cli) => {
                let arc = Arc::new(cli);
                *self.tpu.write() = Some(arc.clone());
                Some(arc)
            }
            Err(_) => None,
        }
    }

    /// === ADD BELOW (helpers): broadcast via TPU first, fallback to hedged RPC ===
    async fn broadcast_tpu_first(&self, tx: &VersionedTransaction) -> Result<Signature, Box<dyn std::error::Error>> {
        if let Some(tpu) = self.ensure_tpu().await {
            // TPU path
            if let Ok(sig) = tokio::time::timeout(TokioDuration::from_millis(TPU_SEND_TMO_MS), tpu.send_transaction(tx.clone())).await {
                if let Ok(s) = sig {
                    // attribute to best-ranked RPC for inclusion EWMA purposes
                    let best = *self.rpc_rank_send().first().unwrap_or(&0);
                    self.note_send_origin(&s, best);
                    return Ok(s);
                }
            }
        }
        // fallback to your hedged RPC (with AccountInUse retry as you wired in #154)
        self.broadcast_versioned_transaction_hedged(tx).await
    }

    /// === ADD BELOW (helpers): start background status sweeper at construction-end ===
    fn start_status_sweeper(&self) {
        let this = self.clone();
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(TokioDuration::from_millis(STATUS_SWEEP_MS)).await;
                let now = Self::now_millis();
                let list: Vec<Signature> = this.inflight_sigs.read()
                    .iter()
                    .filter_map(|(s, ts)| if now.saturating_sub(*ts) <= SEND_ORIGIN_TTL_MS { Some(*s) } else { None })
                    .take(STATUS_SWEEP_MAX).collect();
                if list.is_empty() { continue; }
                let order = this.rpc_rank_read();
                let mut done: Vec<Signature> = Vec::new();
                'outer: for (k,&i) in order.iter().enumerate() {
                    let cli = this.rpc_ring[i].clone();
                    if k > 0 { tokio::time::sleep(TokioDuration::from_millis(this.hedge_stagger_ms() * k as u64)).await; }
                    if let Ok(Ok(resp)) = tokio::time::timeout(TokioDuration::from_millis(this.rpc_dynamic_timeout_ms(i)), cli.get_signature_statuses(&list)).await {
                        for (sig, st) in list.iter().zip(resp.value.iter()) {
                            if let Some(x) = st {
                                if x.err.is_some() { this.record_sig_inclusion(sig, false); done.push(*sig); }
                                else if x.confirmations.is_some() || x.confirmation_status.is_some() {
                                    this.record_sig_inclusion(sig, true); done.push(*sig);
                                }
                            }
                        }
                        break 'outer;
                    }
                }
                if !done.is_empty() {
                    let mut w = this.inflight_sigs.write();
                    for s in done { w.remove(&s); }
                }
            }
        });
    }

    /// === ADD BELOW (helpers): index a LUT account's addresses ===
    #[inline(always)]
    fn index_lut(&self, lut_pk: &Pubkey, lut: &AddressLookupTableAccount, now: u64) {
        let set: HashSet<Pubkey> = lut.addresses.iter().cloned().collect();
        self.alt_addr_index.write().insert(*lut_pk, (now, Arc::new(set)));
    }

    #[inline(always)]
    fn lut_covers(&self, lut_pk: &Pubkey, acc: &Pubkey) -> bool {
        if let Some((ts, set)) = self.alt_addr_index.read().get(lut_pk).cloned() {
            if Self::now_millis().saturating_sub(ts) <= LUT_TTL_MS { return set.contains(acc); }
        }
        false
    }

    /// === ADD BELOW (helpers): conditional heap-frame budget ix ===
    #[inline(always)]
    fn maybe_heap_frame_ix(&self, cu_limit: u32) -> Option<Instruction> {
        use solana_sdk::compute_budget::ComputeBudgetInstruction;
        if cu_limit >= HEAP_FRAME_THRESH_CU {
            Some(ComputeBudgetInstruction::request_heap_frame(HEAP_FRAME_BYTES))
        } else { None }
    }

    /// === ADD BELOW (helpers): guard recent blockhash age ===
    #[inline(always)]
    fn blockhash_fresh_enough(&self) -> bool {
        let (ts, _) = *self.blockhash_cache.read();
        Self::now_millis().saturating_sub(ts) < BLOCKHASH_AGE_REBUILD_MS
    }

    /// === ADD BELOW (helpers): update floor on send outcomes & choose eligible payer ===
    #[inline(always)]
    fn note_signer_floor(&self, payer: &Pubkey, micro_used: u64, landed: bool) {
        let mut w = self.signer_fee_floor.write();
        let f = *w.get(payer).unwrap_or(&SIGNER_FLOOR_MIN);
        let target = if landed { micro_used.max(f) } else { f.saturating_sub(1) };
        let mut nf = ((f as f64) * (1.0 - SIGNER_FLOOR_ALPHA) + (target as f64) * SIGNER_FLOOR_ALPHA) as u64;
        nf = (nf as f64 * SIGNER_FLOOR_DECAY) as u64;
        w.insert(*payer, nf.max(SIGNER_FLOOR_MIN));
    }

    #[inline(always)]
    fn pick_payer_with_floor(&self, micro: u64) -> Option<usize> {
        let payers = self.payer_registry.read();
        if payers.is_empty() { return None; }
        // deterministically rotate starting idx (as in #161)
        let seed_idx = self.payer_index_for_slot().unwrap_or(0);
        for off in 0..payers.len() {
            let idx = (seed_idx + off) % payers.len();
            let pk = &payers[idx];
            let floor = *self.signer_fee_floor.read().get(pk).unwrap_or(&SIGNER_FLOOR_MIN);
            if micro >= floor { return Some(idx); }
        }
        Some(seed_idx)
    }

    /// === ADD BELOW (helpers): adaptive GMA ===
    async fn rpc_get_multiple_accounts_adaptive(&self, keys: &[Pubkey]) -> anyhow::Result<Vec<Option<solana_account_decoder::UiAccount>>> {
        use tokio::time::timeout;
        let order = self.rpc_rank();
        let mut out: Vec<Option<solana_account_decoder::UiAccount>> = vec![None; keys.len()];
        let mut remaining: Vec<(usize, Pubkey)> = keys.iter().cloned().enumerate().collect();
        let mut batch = GMA_MAX_BATCH;

        while !remaining.is_empty() && batch >= GMA_MIN_BATCH {
            // take a chunk
            let chunk: Vec<(usize, Pubkey)> = remaining.drain(..remaining.len().min(batch)).collect();
            let kvec: Vec<Pubkey> = chunk.iter().map(|(_,k)| *k).collect();

            // hedge fetch
            let mut ok = false;
            for (k,&i) in order.iter().enumerate() {
                let cli = self.rpc_ring[i].clone();
                if k > 0 { sleep(TokioDuration::from_millis(self.hedge_stagger_ms() * k as u64 + READ_STAGGER_BIAS_MS)).await; }
                if !self.rpc_try_acquire(i) { continue; }
                let to_ms = self.rpc_dynamic_timeout_ms(i);
                let res = timeout(TokioDuration::from_millis(to_ms), cli.get_multiple_accounts(&kvec)).await;
                self.rpc_release(i);
                match res {
                    Ok(Ok(resp)) => {
                        for (cidx, acc) in resp.value.into_iter().enumerate() {
                            out[chunk[cidx].0] = acc;
                        }
                        self.rpc_refresh_slot_bg(i);
                        ok = true;
                        break;
                    }
                    Ok(Err(_)) | Err(_) => {
                        self.rpc_refresh_slot_bg(i);
                        continue;
                    }
                }
            }
            if !ok {
                // push back the chunk and reduce batch
                for it in chunk { remaining.push(it); }
                batch = (batch / 2).max(GMA_MIN_BATCH);
            }
        }
        if !remaining.is_empty() {
            // last-resort linear fetch
            for (idx, k) in remaining.into_iter() {
                for (kk,&i) in order.iter().enumerate() {
                    let cli = self.rpc_ring[i].clone();
                    if kk > 0 { sleep(TokioDuration::from_millis(self.hedge_stagger_ms() * kk as u64 + READ_STAGGER_BIAS_MS)).await; }
                    if !self.rpc_try_acquire(i) { continue; }
                    let res = self.rpc_ring[i].get_account(&k).await.map(|a| solana_client::rpc_response::Response{context: Default::default(), value: Some(a)} );
                    self.rpc_release(i);
                    if let Ok(Ok(resp)) = res {
                        out[idx] = resp.value.map(|a| solana_account_decoder::UiAccount{
                            lamports: a.lamports,
                            owner: a.owner,
                            executable: a.executable,
                            rent_epoch: a.rent_epoch,
                            data: solana_account_decoder::UiAccountData::Binary(base64::encode(a.data), solana_account_decoder::UiAccountEncoding::Base64),
                        });
                        break;
                    }
                }
            }
        }
        Ok(out)
    }

    pub async fn analyze_pool_ticks(&self, pool_address: &Pubkey) -> Result<ClusterAnalysis, Box<dyn std::error::Error>> {
        use tokio::time::timeout;

        // Per-pool single-flight
        let _guard = self.pool_guard(pool_address).acquire_owned().await?;

        // processed -> confirmed pool account
        let acc_processed = timeout(
            TokioDuration::from_millis(150),
            self.rpc_client.get_account_with_commitment(pool_address, CommitmentConfig::processed()),
        ).await??.value;
        let account = match acc_processed {
            Some(a) => a,
            None => {
                timeout(
                    TokioDuration::from_millis(250),
                    self.rpc_client.get_account_with_commitment(pool_address, CommitmentConfig::confirmed()),
                ).await??.value.ok_or("Pool account not found at confirmed")?
            }
        };

        // Decode pool (hash-skip in #95 below patches this, leave as-is here)
        let now = Self::now_millis();
        let acc_hash = Self::fnv1a64(&account.data);
        let pool_data = if let Some((_, prev_h)) = self.pool_account_hash_cache.read().get(pool_address).cloned() {
            if prev_h == acc_hash {
                if let Some((_, ps)) = self.pool_state_cache.read().get(pool_address).cloned() {
                    ps
                } else {
                    let ps = self.decode_pool_state(&account)?;
                    ps
                }
            } else {
                self.decode_pool_state(&account)?
            }
        } else {
            self.decode_pool_state(&account)?
        };
        self.pool_account_hash_cache.write().insert(*pool_address, (now, acc_hash));
        
        // Insert into TTL cache for cross-pool usage
        self.pool_state_cache.write().insert(*pool_address, (now, pool_data.clone()));
        self.prune_pool_state_cache();

        // Fetch bitmap + possibly purge tick-array cache on regime shift (from earlier passes)
        let tick_array_bitmap = self.fetch_tick_array_bitmap(pool_address).await?;
        if let Some(prev) = self.cluster_cache.read().get(pool_address) {
            if let Some(dc) = &prev.dominant_cluster {
                let drift = (pool_data.tick_current - dc.center_tick).abs();
                let purge_ticks = Self::shift_purge_threshold(prev.ema_volatility);
                if drift >= purge_ticks {
                    let addrs = self.enumerate_tick_array_addresses(pool_address, &tick_array_bitmap);
                    self.invalidate_tick_arrays_for(&addrs);
                }
            }
        }

        // Active ticks
        let active_ticks = self.fetch_active_ticks(pool_address, &tick_array_bitmap).await?;

        // prefetch next/prev arrays so subsequent cycles don't stall on boundary
        let spacing_pf = self.infer_tick_spacing(&active_ticks);
        self.prefetch_adjacent_tick_arrays(pool_address, &tick_array_bitmap, pool_data.tick_current, spacing_pf).await;

        // === FAST-PATH: if tick set didn't change recently, reuse last ClusterAnalysis ===
        let h = Self::hash_ticks(&active_ticks);
        if let Some((ts, prev_h)) = self.pool_ticks_hash_cache.read().get(pool_address).cloned() {
            if prev_h == h && now.saturating_sub(ts) <= ANALYSIS_FASTPATH_TTL_MS {
                if let Some(prev) = self.cluster_cache.read().get(pool_address).cloned() {
                    // Update timestamp and return
                    let mut cloned = prev.clone();
                    cloned.timestamp = Self::unix_time_now();
                    self.cluster_cache.write().insert(*pool_address, cloned.clone());
                    return Ok(cloned);
                }
            }
        }
        // Record latest hash
        self.pool_ticks_hash_cache.write().insert(*pool_address, (now, h));

        // Full path (unchanged from your last pass except for above fast-path)
        let clusters = self.identify_clusters(&active_ticks);
        let liquidity_distribution = self.calculate_liquidity_distribution(&active_ticks);
        let price_impact_map = self.calculate_price_impact(&clusters, &pool_data);

        // In-pool + cross-pool opportunities (you already wired these in earlier passes)
        let mut mev_opportunities = self.detect_mev_opportunities(&clusters, &pool_data);
        self.prefetch_peer_pool_states(pool_address, &pool_data).await;
        let mut x_arbs = self.detect_cross_pool_arbitrage_from_cache(pool_address, &pool_data, &clusters).await;
        mev_opportunities.append(&mut x_arbs);
        let mut tri_arbs = self.detect_triangular_arbitrage_from_cache(pool_address, &pool_data, &clusters).await;
        mev_opportunities.append(&mut tri_arbs);

        // Dominant & EMAs/persistence
        let dominant_now = self.find_dominant_cluster(&clusters);
        let (prev_ema_vol, prev_ema_conc, prev_dom) = {
            if let Some(prev) = self.cluster_cache.read().get(pool_address) {
                (prev.ema_volatility, prev.ema_concentration, prev.dominant_cluster.clone())
            } else { (0.0, 0.0, None) }
        };
        let (vol_now, conc_now) = if let Some(dc) = &dominant_now {
            (dc.volatility_score, dc.concentration_score)
        } else { (0.0, 0.0) };
        let ema_vol = Self::ema(prev_ema_vol, vol_now, 0.30);
        let ema_conc = Self::ema(prev_ema_conc, conc_now, 0.30);
        let persistence = if let (Some(prev_dc), Some(cur_dc)) = (prev_dom.as_ref(), dominant_now.as_ref()) {
            Self::cluster_overlap_ratio(prev_dc, cur_dc)
        } else { 1.0 };

        for o in mev_opportunities.iter_mut() {
            let soft_pen = (1.0 - persistence).clamp(0.0, 1.0);
            o.risk_score = (o.risk_score + soft_pen * 0.20).min(0.995);
            o.execution_probability = (o.execution_probability * (0.90 - 0.30 * soft_pen)).clamp(0.05, 0.99);
        }

        let analysis = ClusterAnalysis {
            cluster_id: self.generate_cluster_id(),
            timestamp: Self::unix_time_now(),
            dominant_cluster: dominant_now,
            clusters,
            liquidity_distribution,
            price_impact_map,
            mev_opportunities,
            ema_volatility: ema_vol,
            ema_concentration: ema_conc,
            persistence_score: persistence,
        };

        self.cluster_cache.write().insert(*pool_address, analysis.clone());
        Ok(analysis)
    }

    #[inline]
    fn identify_clusters(&self, ticks: &[TickData]) -> Vec<TickCluster> {
        if ticks.is_empty() { return Vec::new(); }
        let mut sorted = ticks.to_vec();
        sorted.sort_unstable_by_key(|t| t.tick_index);

        // 1) initial segmentation by gap
        let mut clusters: Vec<TickCluster> = Vec::with_capacity(16);
        let mut i = 0usize;
        while i < sorted.len() {
            let start = i;
            while i + 1 < sorted.len()
                && (sorted[i + 1].tick_index - sorted[i].tick_index) as u64 <= CLUSTER_MAX_DISTANCE
            { i += 1; }
            let end = i + 1;
            let len = end - start;
            if len >= CLUSTER_MIN_SIZE {
                let c = self.build_cluster(sorted[start..end].to_vec());
                if c.total_liquidity >= self.liquidity_threshold { clusters.push(c); }
            }
            i = end;
        }
        if clusters.len() <= 1 { return clusters; }

        // 2) adjacency merge pass:
        // merge neighboring clusters if the gap is small and liquidity density improves
        let mut merged: Vec<TickCluster> = Vec::with_capacity(clusters.len());
        let mut k = 0usize;
        while k < clusters.len() {
            let mut cur = clusters[k].clone();
            let mut advanced = false;
            while k + 1 < clusters.len() {
                let a_last = cur.ticks.last().unwrap().tick_index;
                let b_first = clusters[k + 1].ticks.first().unwrap().tick_index;
                let gap = (b_first - a_last) as u64;

                if gap > (CLUSTER_MAX_DISTANCE as u64 * 2) { break; }

                // Liquidity-density improvement test
                let dens_cur = (cur.total_liquidity as f64) / (cur.ticks.len().max(1) as f64);
                let next = clusters[k + 1].clone();
                let dens_next = (next.total_liquidity as f64) / (next.ticks.len().max(1) as f64);

                let mut joined_ticks = cur.ticks.clone();
                joined_ticks.extend_from_slice(&next.ticks);
                joined_ticks.sort_unstable_by_key(|t| t.tick_index);
                let joined = self.build_cluster(joined_ticks);

                let dens_join = (joined.total_liquidity as f64) / (joined.ticks.len().max(1) as f64);

                // Merge only if joined density improves and liquidity grows meaningfully
                if dens_join >= dens_cur.max(dens_next) * 1.02 && joined.total_liquidity > cur.total_liquidity {
                    cur = joined;
                    k += 1;
                    advanced = true;
                } else {
                    break;
                }
            }
            merged.push(cur);
            k += 1;
            if advanced { continue; }
        }

        // Sort by density desc for stable downstream behavior
        merged.sort_unstable_by(|a, b| {
            let da = (a.total_liquidity as u128).saturating_mul(1_000_000u128) / ((a.ticks.len() as u128).max(1));
            let db = (b.total_liquidity as u128).saturating_mul(1_000_000u128) / ((b.ticks.len() as u128).max(1));
            db.cmp(&da)
        });
        merged
    }

    fn build_cluster(&self, ticks: Vec<TickData>) -> TickCluster {
        // Liquidity-weighted median tick as the center (stable & economically meaningful)
        let mut pairs: Vec<(i32, u128)> = ticks.iter().map(|t| (t.tick_index, t.liquidity_gross)).collect();
        pairs.sort_unstable_by_key(|p| p.0);

        let total_liquidity: u128 = pairs.iter().map(|p| p.1).sum();
        let mut acc: u128 = 0;
        let mut center_tick = pairs.first().map(|p| p.0).unwrap_or(0);
        let half = total_liquidity / 2;
        for (ti, liq) in &pairs {
            acc = acc.saturating_add(*liq);
            if acc >= half { center_tick = *ti; break; }
        }

        let price_range = (
            ticks.first().unwrap().sqrt_price_x64,
            ticks.last().unwrap().sqrt_price_x64,
        );

        // Proxy 24h volume from local volatility & depth (robust without fee deltas)
        // V24h ≈ k * total_liquidity * local_vol (k chosen so thresholds match units)
        let volatility_score = self.calculate_volatility_score(&ticks); // 0..1
        let concentration_score = self.calculate_concentration_score(&ticks, total_liquidity);
        let volume_24h = ((total_liquidity as f64) * (volatility_score * 0.6 + concentration_score * 0.4)).sqrt() as u64;

        let opportunity_score = self.calculate_opportunity_score(
            concentration_score,
            volatility_score,
            volume_24h,
            total_liquidity,
        );

        TickCluster {
            center_tick,
            ticks,
            total_liquidity,
            price_range,
            volume_24h,
            concentration_score,
            volatility_score,
            opportunity_score,
        }
    }

    fn calculate_concentration_score(&self, ticks: &[TickData], total_liquidity: u128) -> f64 {
        if ticks.is_empty() || total_liquidity == 0 { return 0.0; }

        let mut w: Vec<f64> = ticks.iter().map(|t| t.liquidity_gross as f64).collect();
        w.sort_unstable_by(|a, b| a.partial_cmp(b).unwrap());

        let n = w.len() as f64;
        let sum: f64 = w.iter().sum();
        if sum <= 0.0 || n < 2.0 { return 0.0; }

        // Gini = (2 * Σ(i * x_i)) / (n * Σx) - (n + 1)/n
        let mut acc = 0.0f64;
        for (i, xi) in w.iter().enumerate() {
            acc += (i as f64 + 1.0) * *xi;
        }
        let g = (2.0 * acc) / (n * sum) - (n + 1.0) / n;
        g.clamp(0.0, 1.0)
    }

    fn calculate_volatility_score(&self, ticks: &[TickData]) -> f64 {
        if ticks.len() < 3 { return 0.0; }
        // Use log returns of price inferred from sqrtPriceX64
        let mut prev = None::<f64>;
        let mut n = 0.0f64;
        let mut mean = 0.0f64;
        let mut m2 = 0.0f64;

        for t in ticks {
            let p = self.sqrt_price_to_price(t.sqrt_price_x64).max(1e-18);
            if let Some(pp) = prev {
                let r = (p / pp).ln();
                n += 1.0;
                let delta = r - mean;
                mean += delta / n;
                m2 += delta * (r - mean);
            }
            prev = Some(p);
        }
        if n < 2.0 { return 0.0; }
        let var = (m2 / (n - 1.0)).max(0.0);
        let sigma = var.sqrt();

        // Map to [0,1] with gentle compression; tune 6.0 as "high vol"
        (sigma * 6.0).clamp(0.0, 1.0)
    }

    fn calculate_opportunity_score(&self, concentration: f64, volatility: f64, volume: u64, liquidity: u128) -> f64 {
        // Higher with dense liquidity, real flow, manageable vol; penalize tiny volumes
        let vol_score = (volatility.min(0.6) / 0.6);                  // 0..1
        let conc_score = concentration;                               // 0..1
        let liq_score = ((liquidity as f64).ln() / 35.0).clamp(0.0, 1.0);
        let flow_score = ((volume as f64).ln() / 25.0).clamp(0.0, 1.0);

        // Blend with mild convexity toward strong agreeing signals
        let raw = 0.35 * conc_score + 0.30 * vol_score + 0.20 * flow_score + 0.15 * liq_score;
        raw.clamp(0.0, 1.0)
    }

    fn detect_mev_opportunities(&self, clusters: &[TickCluster], pool_data: &PoolState) -> Vec<MevOpportunity> {
        // Oracle drift guard
        if let Some(d) = self.oracle_drift_ratio(pool_data) {
            if d > 0.025 { return Vec::new(); } // likely manipulation / desync — hard reject
        }

        let mut ops: Vec<MevOpportunity> = Vec::with_capacity(clusters.len() * 3);

        for c in clusters {
            if c.opportunity_score <= 0.7 { continue; }
            let spoof = Self::liq_flip_rate(&c.ticks); // 0..1
            let spoof_pen = (spoof * 0.35).min(0.35); // up to +0.35 risk / -0.15 execP

            if let Some(mut o) = self.check_arbitrage_opportunity(c, pool_data) {
                o.risk_score = (o.risk_score + spoof_pen).min(0.995);
                o.execution_probability = (o.execution_probability * (1.0 - spoof_pen * 0.45)).clamp(0.05, 0.99);
                // soft penalty for modest drift (1–2.5%)
                if let Some(d) = self.oracle_drift_ratio(pool_data) {
                    if d > 0.01 {
                        o.risk_score = (o.risk_score + d * 6.0).min(0.99);
                        o.execution_probability = (o.execution_probability - d * 2.5).max(0.2);
                    }
                }
                ops.push(o);
            }

            if c.volatility_score > 0.3 {
                if let Some(mut o) = self.check_sandwich_opportunity(c, pool_data) {
                    o.risk_score = (o.risk_score + spoof_pen * 1.2).min(0.998);
                    o.execution_probability = (o.execution_probability * (1.0 - spoof_pen * 0.55)).clamp(0.05, 0.99);
                    if let Some(d) = self.oracle_drift_ratio(pool_data) {
                        if d > 0.01 {
                            o.risk_score = (o.risk_score + d * 8.0).min(0.995);
                            o.execution_probability = (o.execution_probability - d * 3.0).max(0.15);
                        }
                    }
                    ops.push(o);
                }
            }

            if c.concentration_score > 0.85 {
                if let Some(mut o) = self.check_jit_liquidity_opportunity(c, pool_data) {
                    o.risk_score = (o.risk_score + spoof_pen * 0.6).min(0.99);
                    o.execution_probability = (o.execution_probability * (1.0 - spoof_pen * 0.35)).clamp(0.10, 0.99);
                    if let Some(d) = self.oracle_drift_ratio(pool_data) {
                        if d > 0.01 {
                            o.risk_score = (o.risk_score + d * 5.0).min(0.99);
                            o.execution_probability = (o.execution_probability - d * 2.0).max(0.25);
                        }
                    }
                    ops.push(o);
                }
                // BackRun thrives when focused & not hyper-volatile
                if let Some(mut o) = self.check_backrun_opportunity(c, pool_data) {
                    o.risk_score = (o.risk_score + spoof_pen * 0.9).min(0.99);
                    o.execution_probability = (o.execution_probability * (1.0 - spoof_pen * 0.4)).clamp(0.10, 0.99);
                    ops.push(o);
                }
            }
        }

        ops.retain(|o| {
            let cost_proxy = (o.priority_fee as f64) + (o.gas_estimate as f64) * 0.1;
            (o.expected_profit as f64) > cost_proxy * 1.6
        });

        // Sort high to low EV first
        ops.sort_unstable_by(|a, b| (b.expected_profit as i128).cmp(&(a.expected_profit as i128)));

        // De-duplicate including cross-type conflicts: keep higher EV density
        let mut pruned: Vec<MevOpportunity> = Vec::with_capacity(ops.len());
        for o in ops {
            let mut conflict = false;
            for p in &pruned {
                if Self::opps_conflict(&o, p) {
                    conflict = true;
                    break;
                }
            }
            if !conflict { pruned.push(o); }
        }

        // EV-density rank with stable tie-breaker on lower cost
        pruned.sort_unstable_by(|a, b| {
            let ca = 1.0 + a.priority_fee as f64 + a.gas_estimate as f64 * 0.2;
            let cb = 1.0 + b.priority_fee as f64 + b.gas_estimate as f64 * 0.2;
            let da = (a.expected_profit as f64) / ca;
            let db = (b.expected_profit as f64) / cb;
            match db.partial_cmp(&da).unwrap() {
                core::cmp::Ordering::Equal => {
                    match (ca as i128).cmp(&(cb as i128)) {
                        core::cmp::Ordering::Equal => Self::opp_tiebreak_hash(a).cmp(&Self::opp_tiebreak_hash(b)),
                        ord => ord,
                    }
                }
                ord => ord,
            }
        });

        // Recent-op throttle: de-spam identical opps
        let now = Self::now_millis();
        let mut uniq: Vec<MevOpportunity> = Vec::with_capacity(pruned.len());
        for o in pruned.into_iter() {
            let side_code: u8 = match o.opportunity_type {
                OpportunityType::Arbitrage | OpportunityType::CrossPoolArbitrage => 1, // ambiguous; we normalize
                OpportunityType::JitLiquidity => 2,
                OpportunityType::BackRun => 3,
                OpportunityType::Sandwich => 4,
                OpportunityType::Liquidation => 5,
            };
            let size_bkt = Self::amt_bucket((o.expected_profit as u128).saturating_add(o.priority_fee as u128));
            let key = (match o.opportunity_type {
                OpportunityType::Arbitrage => 1u8,
                OpportunityType::Liquidation => 2u8,
                OpportunityType::JitLiquidity => 3u8,
                OpportunityType::BackRun => 4u8,
                OpportunityType::Sandwich => 5u8,
                OpportunityType::CrossPoolArbitrage => 6u8,
            }, pool_data.token_vault_b /* make pool-unique combo (a+b) */, o.tick_range.0 ^ ((side_code as i32) << 1), o.tick_range.1 ^ (size_bkt as i32));

            let mut skip = false;
            {
                let seen = self.recent_opps_seen.read();
                if let Some(ts) = seen.get(&key) {
                    if now.saturating_sub(*ts) <= RECENT_OP_TTL_MS { skip = true; }
                }
            }
            if !skip {
                // Apply calibration multiplier AFTER spoof/persistence penalties and BEFORE EV guard
                if let Some(c) = clusters.iter().find(|c| c.ticks.iter().any(|t| t.tick_index >= o.tick_range.0 && t.tick_index <= o.tick_range.1)) {
                    o.execution_probability = (o.execution_probability * self.execp_calibration_multiplier(o.opportunity_type, c.volatility_score, c.concentration_score))
                        .clamp(0.05, 0.995);
                }

                // Slot-level throttle: once you've fired an opportunity in a slot, never spend quota on that exact window again
                let slot_key = (match o.opportunity_type {
                    OpportunityType::Arbitrage => 1u8,
                    OpportunityType::Liquidation => 2u8,
                    OpportunityType::JitLiquidity => 3u8,
                    OpportunityType::BackRun => 4u8,
                    OpportunityType::Sandwich => 5u8,
                    OpportunityType::CrossPoolArbitrage => 6u8,
                }, pool_data.token_vault_a, o.tick_range.0, o.tick_range.1);
                if !self.slot_throttle_insert(slot_key) { continue; }

                uniq.push(o);
                let mut w = self.recent_opps_seen.write();
                w.insert(key, now);
                if w.len() > MAX_OPP_SEEN_CACHE {
                    let take = w.len() / 8;
                    for k in w.keys().take(take).cloned().collect::<Vec<_>>() { w.remove(&k); }
                }
            }
        }
        Self::select_non_conflicting_ops(uniq, MAX_OPS_PER_SLOT)
    }

    fn check_arbitrage_opportunity(&self, cluster: &TickCluster, pool_data: &PoolState) -> Option<MevOpportunity> {
        let p_pool = self.sqrt_price_to_price(pool_data.sqrt_price_x64);
        let p_pool_human = tokio::task::block_in_place(|| futures::executor::block_on(self.pool_price_ratio_decimals(pool_data)));
        let p_cluster = self.sqrt_price_to_price(cluster.price_range.0);
        let rel_diff = ((p_pool - p_cluster).abs() / p_pool).max(0.0);
        if rel_diff <= 0.0010 { return None; } // slightly tighter than before

        // Direction by relative prices
        let side = if p_cluster < p_pool { SwapSide::Token1In } else { SwapSide::Token0In };

        // Cluster-aware optimal amount (bisection on piecewise price impact)
        let amt_in = self.cluster_optimal_amount_any(pool_data, cluster, rel_diff, side);
        if amt_in == 0 { return None; }

        // True impact using per-tick liquidity
        let impact = self.cluster_price_impact_any(amt_in, pool_data, cluster, side);
        if impact <= 0.0 || impact >= rel_diff { return None; }

        // Fees
        let fee_total = (pool_data.fee_rate as f64 + pool_data.protocol_fee_rate as f64) / BASIS_POINT_MAX as f64;

        // Notional — match side dimensions
        let notional = match side {
            SwapSide::Token0In => (amt_in as f64) * p_pool_human.max(1e-12),
            SwapSide::Token1In => (amt_in as f64).max(1e-12) / p_pool_human.max(1e-12),
        };

        // EV
        let gross = notional * (rel_diff - impact).max(0.0);
        let net = (gross * (1.0 - fee_total)).max(0.0);
        
        let floor = {
            // Read last EMAs/persistence if available
            let (ema_v, pers) = if let Some(prev) = self.cluster_cache.read().values().next() {
                (prev.ema_volatility, prev.persistence_score)
            } else { (cluster.volatility_score, 1.0) };
            Self::arb_min_ev_floor(ema_v, pers)
        };
        if (net as f64) < floor { return None; }

        // Desk-grade execution probability and CU pricing
        let drift = self.oracle_drift_ratio(pool_data).unwrap_or(0.0);
        let exec_p = Self::desk_exec_probability(
            cluster.volatility_score,
            cluster.concentration_score,
            drift,
            rel_diff,
            impact
        );
        let seed_hi = pool_data.sqrt_price_x64 as u64 ^ (cluster.center_tick as u64);
        let seed_lo = Self::unix_time_now();
        let mut base_micro = self.cu_price_micro_for_op(net as u64, 210_000, self.slot_phase_0_to_1());
        base_micro = self.cap_micro_by_ev(base_micro, net as u64, 210_000);
        let priority_fee = Self::jitter_micro(base_micro, seed_hi, seed_lo);

        Some(MevOpportunity {
            opportunity_type: OpportunityType::Arbitrage,
            tick_range: (cluster.ticks.first().unwrap().tick_index, cluster.ticks.last().unwrap().tick_index),
            expected_profit: net as u64,
            risk_score: (0.22 + cluster.volatility_score * 0.45).min(0.95),
            execution_probability: exec_p,
            gas_estimate: 210_000,
            priority_fee,
        })
    }

    fn check_sandwich_opportunity(&self, cluster: &TickCluster, pool_data: &PoolState) -> Option<MevOpportunity> {
        let dyn_thresh = Self::dynamic_volume_threshold_from_vol(cluster.volatility_score);
        if cluster.volume_24h < dyn_thresh * 12 { return None; }

        let p_pool = self.sqrt_price_to_price(pool_data.sqrt_price_x64);
        let p_pool_human = tokio::task::block_in_place(|| futures::executor::block_on(self.pool_price_ratio_decimals(pool_data)));
        let p_center = self.sqrt_price_to_price(cluster.price_range.0);
        let side = if p_center < p_pool { SwapSide::Token1In } else { SwapSide::Token0In };

        let victim = (dyn_thresh as f64 * 1.5) as u128;

        let front_impact = self.cluster_price_impact_any(victim, pool_data, cluster, side);
        if !(0.001..0.012).contains(&front_impact) { return None; }

        let fee_bps = (pool_data.fee_rate as f64 + pool_data.protocol_fee_rate as f64) / BASIS_POINT_MAX as f64;
        let notional = match side {
            SwapSide::Token0In => (victim as f64) * p_pool_human.max(1e-12),
            SwapSide::Token1In => (victim as f64).max(1e-12) / p_pool_human.max(1e-12),
        };

        let gross = notional * front_impact * 0.65;
        let net = (gross * (1.0 - 2.0 * fee_bps)).max(0.0);
        if net < 20_000.0 { return None; }

        // Desk-grade execution probability and CU pricing with jitter
        let drift = self.oracle_drift_ratio(pool_data).unwrap_or(0.0);
        let exec_p = Self::desk_exec_probability(
            cluster.volatility_score,
            cluster.concentration_score,
            drift,
            front_impact, // treat as rel_gap proxy
            front_impact * 0.6
        );
        let seed_hi = pool_data.sqrt_price_x64 as u64 ^ (cluster.center_tick as u64);
        let seed_lo = Self::unix_time_now();
        let mut base_micro = self.cu_price_micro_for_op(net as u64, 520_000, self.slot_phase_0_to_1());
        base_micro = self.cap_micro_by_ev(base_micro, net as u64, 520_000);
        let priority_fee = Self::jitter_micro(base_micro, seed_hi, seed_lo);

        Some(MevOpportunity {
            opportunity_type: OpportunityType::Sandwich,
            tick_range: (cluster.center_tick - 10, cluster.center_tick + 10),
            expected_profit: net as u64,
            risk_score: (0.55 + cluster.volatility_score * 0.35).min(0.98),
            execution_probability: exec_p,
            gas_estimate: 520_000,
            priority_fee,
        })
    }

    fn check_jit_liquidity_opportunity(&self, cluster: &TickCluster, pool_data: &PoolState) -> Option<MevOpportunity> {
        // require tight concentration + decent flow proxy
        if cluster.concentration_score <= 0.86 { return None; }
        if cluster.volume_24h == 0 { return None; }

        // Amount to add = target share of in-range liquidity
        let target_share = 0.18_f64; // desk-grade: step above noise providers
        let add_liq = ((cluster.total_liquidity as f64) * target_share / (1.0 - target_share)) as u128;
        if add_liq == 0 { return None; }

        // Expected fee rate
        let fee_rate = (pool_data.fee_rate as f64) / (BASIS_POINT_MAX as f64);
        // Liquidity share during event window
        let liq_share = (add_liq as f64) / ((pool_data.liquidity + add_liq) as f64);

        // Flow proxy in window (convert 24h to per-block ~400ms ~ 216k blocks/day)
        const BLOCKS_PER_DAY: f64 = 216_000.0;
        let flow_per_block = (cluster.volume_24h as f64) / BLOCKS_PER_DAY;

        // Expected fees this block
        let fees_block = flow_per_block * fee_rate * liq_share;

        // Priority fee reserve and gas
        let gas_est = 400_000u64;
        let expected_profit = fees_block.max(0.0) as u64;

        // Enforce per-block APY floor (fees per block over capital proxy ≳ x)
        // Capital proxy ~ add_liq converted to quote by pool price
        let p = self.sqrt_price_to_price(pool_data.sqrt_price_x64).max(1e-12);
        let capital_proxy = (add_liq as f64) * p.sqrt(); // coarse: liquidity ~ L ≈ sqrt(XY)
        let apy_block = if capital_proxy > 0.0 { (fees_block / capital_proxy) } else { 0.0 };

        // ~> require ~20% annualized over blocks (very conservative, adjusts to block scale)
        let annualized = apy_block * BLOCKS_PER_DAY;
        if annualized < 0.20 { return None; }

        Some(MevOpportunity {
            opportunity_type: OpportunityType::JitLiquidity,
            tick_range: (cluster.center_tick - 2, cluster.center_tick + 2),
            expected_profit: expected_profit,
            risk_score: (0.35 + (1.0 - cluster.concentration_score) * 0.5 + cluster.volatility_score * 0.2).min(0.95),
            execution_probability: (0.90 - cluster.volatility_score * 0.25).clamp(0.40, 0.95),
            gas_estimate: gas_est,
            priority_fee: (expected_profit as f64 * 0.15) as u64,
        })
    }

    /// === ADD BELOW (new detector): BackRun ===
    fn check_backrun_opportunity(&self, cluster: &TickCluster, pool_data: &PoolState) -> Option<MevOpportunity> {
        // Require focused liquidity and moderate vol (too high → toxic)
        if cluster.concentration_score < 0.82 { return None; }
        if cluster.volatility_score > 0.75 { return None; }

        // Expect a victim pulse ≈ flow_per_block * 1.2
        const BLOCKS_PER_DAY: f64 = 216_000.0;
        let flow_per_block = (cluster.volume_24h as f64) / BLOCKS_PER_DAY;
        if flow_per_block <= 0.0 { return None; }
        let victim = (flow_per_block * 1.2).max(VOLUME_THRESHOLD as f64 * 0.5) as u128;

        let p_pool = self.sqrt_price_to_price(pool_data.sqrt_price_x64);
        let p_pool_human = tokio::task::block_in_place(|| futures::executor::block_on(self.pool_price_ratio_decimals(pool_data)));
        let p_center = self.sqrt_price_to_price(cluster.price_range.0);
        let side = if p_center < p_pool { SwapSide::Token1In } else { SwapSide::Token0In };

        let impact = self.cluster_price_impact_any(victim, pool_data, cluster, side);
        if impact <= 0.0008 { return None; } // tiny edge → ignore

        let fee_bps = (pool_data.fee_rate as f64 + pool_data.protocol_fee_rate as f64) / BASIS_POINT_MAX as f64;
        let notional = match side {
            SwapSide::Token0In => (victim as f64) * p_pool_human.max(1e-12),
            SwapSide::Token1In => (victim as f64).max(1e-12) / p_pool_human.max(1e-12),
        };

        // One leg EV with decay (backrun is weaker than sandwich)
        let gross = notional * impact * 0.45;
        let net = (gross * (1.0 - fee_bps)).max(0.0);
        if net < 10_000.0 { return None; }

        // Exec prob & tip are handled by your desk_exec_probability + cu_price model (#47/#48)
        Some(MevOpportunity {
            opportunity_type: OpportunityType::BackRun,
            tick_range: (cluster.center_tick - 8, cluster.center_tick + 8),
            expected_profit: net as u64,
            risk_score: (0.40 + cluster.volatility_score * 0.30).min(0.95),
            execution_probability: 0.0, // set by #48 in your builder patch if you choose to
            gas_estimate: 260_000,
            priority_fee: 0,            // set by cu_price model with jitter if you choose to
        })
    }

    #[inline(always)]
    fn calculate_optimal_swap_amount(&self, cluster: &TickCluster, pool_data: &PoolState) -> u64 {
        self.optimal_amount_token0_in(
            pool_data.liquidity.max(1),
            pool_data.sqrt_price_x64,
            // use cluster-pool relative gap as target
            (self.sqrt_price_to_price(pool_data.sqrt_price_x64) - self.sqrt_price_to_price(cluster.price_range.0))
                .abs()
                / self.sqrt_price_to_price(pool_data.sqrt_price_x64)
        ) as u64
    }

    fn calculate_jit_liquidity_amount(&self, cluster: &TickCluster, pool_data: &PoolState) -> u128 {
        let current_liquidity = cluster.total_liquidity;
        let target_share = 0.15;
        let jit_liquidity = (current_liquidity as f64 * target_share / (1.0 - target_share)) as u128;
        
        jit_liquidity.min(pool_data.liquidity / 5)
    }

    fn estimate_jit_fee_earnings(&self, liquidity: u128, volume: u64, pool_data: &PoolState) -> u64 {
        let fee_rate = pool_data.fee_rate as f64 / BASIS_POINT_MAX as f64;
        let liquidity_share = liquidity as f64 / (pool_data.liquidity + liquidity) as f64;
        let expected_volume_share = volume as f64 * liquidity_share * 0.8;
        
        (expected_volume_share * fee_rate) as u64
    }

    fn estimate_price_impact(&self, amount: u64, liquidity: f64) -> f64 {
        let k = liquidity * liquidity;
        let delta = amount as f64;
        (delta / (liquidity + delta / 2.0)).min(0.5)
    }

    /// === ADD BELOW (helpers): infer tick spacing ===
    #[inline(always)]
    fn infer_tick_spacing(&self, ticks: &[TickData]) -> i32 {
        if ticks.len() < 3 { return TICK_SPACING as i32; }
        let mut g: i32 = 0;
        let mut prev = ticks[0].tick_index;
        for t in &ticks[1..] {
            let d = (t.tick_index - prev).abs().max(1);
            g = if g == 0 { d } else { self.gcd_i32(g, d) };
            prev = t.tick_index;
        }
        // Clamp to a reasonable set {1, 8, 16, 32, 64, 128, 256}
        let mut s = g;
        for cand in [1,8,16,32,64,128,256].iter() {
            if (s % cand) == 0 { s = *cand; }
        }
        s.max(1)
    }

    #[inline(always)]
    fn gcd_i32(mut a: i32, mut b: i32) -> i32 { 
        while b != 0 { 
            let t = a % b; 
            a = b; 
            b = t; 
        } 
        a.abs().max(1) 
    }

    fn calculate_liquidity_distribution(&self, ticks: &[TickData]) -> HashMap<i32, u128> {
        let mut distribution = HashMap::with_capacity(ticks.len().min(256));
        if ticks.is_empty() { return distribution; }
        let spacing = self.infer_tick_spacing(ticks);
        for t in ticks {
            let bucket = (t.tick_index / spacing) * spacing;
            let entry = distribution.entry(bucket).or_insert(0);
            *entry = entry.saturating_add(t.liquidity_gross);
        }
        distribution
    }

    fn calculate_price_impact(&self, clusters: &[TickCluster], pool_data: &PoolState) -> BTreeMap<i32, f64> {
        let mut impact = BTreeMap::new();
        let p_pool = self.sqrt_price_to_price(pool_data.sqrt_price_x64);

        for c in clusters {
            // Direction by where cluster sits vs pool
            let p_center = self.sqrt_price_to_price(c.price_range.0);
            let side = if p_center < p_pool { SwapSide::Token1In } else { SwapSide::Token0In };
            let pi = self.cluster_price_impact_any(VOLUME_THRESHOLD as u128, pool_data, c, side);

            // Derive spacing from cluster ticks and scale radius so we cover ~640 ticks @ 64 spacing
            let spacing = self.infer_tick_spacing(&c.ticks).max(1);
            let radius = (640 / spacing).clamp(6, 32); // tighter map for dense pools

            let center = c.center_tick;
            for d in -radius..=radius {
                let w = 1.0 / (1.0 + (d.abs() as f64 * 0.2));
                impact.insert(center + d * spacing, (pi * w).max(0.0));
            }
        }
        impact
    }

    fn find_dominant_cluster(&self, clusters: &[TickCluster]) -> Option<TickCluster> {
        clusters.iter()
            .max_by(|a, b| {
                // EV tilt: liquidity × opp_score with mild volatility penalty
                let sa = (a.total_liquidity as f64) * a.opportunity_score * (1.0 - a.volatility_score * 0.15);
                let sb = (b.total_liquidity as f64) * b.opportunity_score * (1.0 - b.volatility_score * 0.15);
                sa.partial_cmp(&sb).unwrap()
                    .then_with(|| a.center_tick.cmp(&b.center_tick)) // stable tie-break
            })
            .cloned()
    }

    fn calculate_cluster_volume(&self, ticks: &[TickData]) -> u64 {
        let fee_growth_sum: u128 = ticks.iter()
            .map(|t| t.fee_growth_outside_a + t.fee_growth_outside_b)
            .sum();
        
        (fee_growth_sum / 1000) as u64
    }

    async fn fetch_tick_array_bitmap(&self, pool_address: &Pubkey) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
        use tokio::time::{timeout, Duration};

        let now = Self::now_millis();
        let key = self.derive_tick_array_bitmap_address(pool_address);

        // Cache hit?
        if let Some((ts, data)) = self.bitmap_cache.read().get(&key).cloned() {
            if now.saturating_sub(ts) <= self.cache_ttl_ms() {
                return Ok((*data).clone());
            }
        }

        // Miss → fetch with fallback
        let acc = timeout(TokioDuration::from_millis(150),
            self.rpc_client.get_account_with_commitment(&key, CommitmentConfig::processed())
        ).await??.value.or_else(|| None);

        let account = match acc {
            Some(a) => a,
            None => {
                timeout(TokioDuration::from_millis(250),
                    self.rpc_client.get_account_with_commitment(&key, CommitmentConfig::confirmed())
                ).await??.value.ok_or("bitmap not found at confirmed")?
            }
        };

        let data = account.data.clone();
        // Put into cache
        {
            let mut w = self.bitmap_cache.write();
            w.insert(key, (now, Arc::new(data.clone())));
            if w.len() > MAX_BITMAP_CACHE {
                let take = w.len() / 8;
                for k in w.keys().take(take).cloned().collect::<Vec<_>>() { w.remove(&k); }
            }
        }
        Ok(data)
    }

    async fn fetch_active_ticks(&self, pool_address: &Pubkey, bitmap: &[u8]) -> Result<Vec<TickData>, Box<dyn std::error::Error>> {
        // Collect tick-array PDAs
        let mut addrs: Vec<Pubkey> = Vec::with_capacity(256);
        for (byte_idx, &byte) in bitmap.iter().enumerate() {
            if byte == 0 { continue; }
            let base = byte_idx * 8;
            for bit in 0..8 {
                if (byte >> bit) & 1 == 1 {
                    let idx = (base + bit) as i32;
                    addrs.push(self.derive_tick_array_address(pool_address, idx));
                }
            }
        }
        if addrs.is_empty() { return Ok(Vec::new()); }

        let ts = Self::unix_time_now();
        let now = Self::now_millis();

        // Split into cache hits / misses
        let mut hits: Vec<Arc<Vec<TickData>>> = Vec::with_capacity(addrs.len());
        let mut misses: Vec<Pubkey> = Vec::with_capacity(addrs.len());

        {
            let cache = self.tick_array_cache.read();
            for k in &addrs {
                if let Some((t, v)) = cache.get(k) {
                    if now.saturating_sub(*t) <= self.cache_ttl_ms() {
                        hits.push(v.clone());
                        continue;
                    }
                }
                misses.push(*k);
            }
        }

        let mut out: Vec<TickData> = Vec::with_capacity(addrs.len() * 16);
        for v in hits {
            out.extend_from_slice(&v);
        }

        if !misses.is_empty() {
            let mut ok_any = false;
            let mut elapsed_total = 0u64;
            let mut count_batches = 0u64;
            let chunk_sz = self.chunk_size();
            for chunk in misses.chunks(chunk_sz) {
                let t0 = Self::now_millis();
                let accounts = self.rpc_get_multiple_accounts(chunk).await;
                let elapsed = Self::now_millis().saturating_sub(t0);
                elapsed_total = elapsed_total.saturating_add(elapsed);
                count_batches += 1;

                match accounts {
                    Ok(accs) => {
                        ok_any = true;
                        for (i, maybe_acc) in accs.into_iter().enumerate() {
                    if let Some(acc) = maybe_acc {
                        if acc.data.is_empty() || acc.lamports == 0 { continue; }
                            let key = chunk[i];
                                let h = Self::fnv1a64(&acc.data);
                                // If hash unchanged and we still have parsed ticks cached, extend TTL and reuse
                                if let Some((_, prev_h)) = self.tick_array_hash_cache.read().get(&key).cloned() {
                                    if prev_h == h {
                                        if let Some((_, arc_ticks)) = self.tick_array_cache.read().get(&key).cloned() {
                                            out.extend_from_slice(&arc_ticks);
                                            // refresh TTL
                                            self.tick_array_cache.write().insert(key, (now, arc_ticks));
                                            continue;
                                        }
                                    }
                                }
                                // Parse once when hash changed or no parsed cache
                                if let Ok(mut ticks) = self.parse_tick_array(&acc.data, ts) {
                                    let arc = Arc::new(ticks.clone());
                                    self.tick_array_cache.write().insert(key, (now, arc.clone()));
                                    self.tickarray_hash_put(&key, now, h);
                            out.append(&mut ticks);
                        }
                    }
                }
                    }
                    Err(_) => { /* handled by adapt below */ }
                }
            }
            if count_batches > 0 {
                self.adapt_chunk_on_result(elapsed_total / count_batches, ok_any);
            }
        }

        Ok(out)
    }

    fn parse_tick_array(&self, data: &[u8], timestamp: u64) -> Result<Vec<TickData>, Box<dyn std::error::Error>> {
        const HEADER: usize = 8;
        const TICK_SIZE: usize = 88;

        if data.len() < HEADER { return Ok(Vec::new()); }
        let avail = data.len() - HEADER;
        let num_ticks = (avail / TICK_SIZE).min(88);

        let mut out = Vec::with_capacity(num_ticks);
        let mut off = HEADER;

        for _ in 0..num_ticks {
            let end = off + TICK_SIZE;
            if end > data.len() { break; }
            let td = &data[off..end];

            let tick = TickData {
                tick_index: i32::from_le_bytes(*array_ref![td, 0, 4]),
                sqrt_price_x64: u128::from_le_bytes(*array_ref![td, 4, 16]),
                liquidity_net: i128::from_le_bytes(*array_ref![td, 20, 16]),
                liquidity_gross: u128::from_le_bytes(*array_ref![td, 36, 16]),
                fee_growth_outside_a: u128::from_le_bytes(*array_ref![td, 52, 16]),
                fee_growth_outside_b: u128::from_le_bytes(*array_ref![td, 68, 16]),
                timestamp,
                block_height: 0,
            };

            // Sanity guard: skip entries where sqrt_price is out of domain to avoid NaNs
            if tick.liquidity_gross > 0 
                && tick.sqrt_price_x64 >= MIN_SQRT_PRICE_X64 
                && tick.sqrt_price_x64 <= MAX_SQRT_PRICE_X64 {
                out.push(tick);
            }
            off = end;
        }
        Ok(out)
    }

    fn decode_pool_state(&self, account: &Account) -> Result<PoolState, Box<dyn std::error::Error>> {
        if account.data.len() < 185 {
            return Err("Invalid pool account data".into());
        }
        let mut ps = PoolState {
            bump: account.data[0],
            token_mint_a: Pubkey::new_from_array(*array_ref![account.data, 1, 32]),
            token_mint_b: Pubkey::new_from_array(*array_ref![account.data, 33, 32]),
            token_vault_a: Pubkey::new_from_array(*array_ref![account.data, 65, 32]),
            token_vault_b: Pubkey::new_from_array(*array_ref![account.data, 97, 32]),
            fee_rate: u16::from_le_bytes(*array_ref![account.data, 129, 2]),
            protocol_fee_rate: u16::from_le_bytes(*array_ref![account.data, 131, 2]),
            liquidity: u128::from_le_bytes(*array_ref![account.data, 133, 16]),
            sqrt_price_x64: u128::from_le_bytes(*array_ref![account.data, 149, 16]),
            tick_current: i32::from_le_bytes(*array_ref![account.data, 165, 4]),
            token_a_amount: u64::from_le_bytes(*array_ref![account.data, 169, 8]),
            token_b_amount: u64::from_le_bytes(*array_ref![account.data, 177, 8]),
        };
        // Sanity guards
        ps.sqrt_price_x64 = ps.sqrt_price_x64
            .max(MIN_SQRT_PRICE_X64)
            .min(MAX_SQRT_PRICE_X64);
        if ps.fee_rate as u64 > BASIS_POINT_MAX || ps.protocol_fee_rate as u64 > BASIS_POINT_MAX {
            return Err("Fee bps out of range".into());
        }
        Ok(ps)
    }

    fn derive_tick_array_bitmap_address(&self, pool_address: &Pubkey) -> Pubkey {
        let (address, _) = Pubkey::find_program_address(
            &[b"tick_array_bitmap", pool_address.as_ref()],
            &orca_whirlpool::ID,
        );
        address
    }

    fn derive_tick_array_address(&self, pool_address: &Pubkey, tick_array_idx: i32) -> Pubkey {
        let (address, _) = Pubkey::find_program_address(
            &[
                b"tick_array",
                pool_address.as_ref(),
                &tick_array_idx.to_le_bytes(),
            ],
            &orca_whirlpool::ID,
        );
        address
    }

    #[inline(always)]
    fn sqrt_price_to_price(&self, sqrt_price_x64: u128) -> f64 {
        // Whirlpool/UniV3 uses Q64.64 for sqrtPriceX64.
        // price = (sqrt_price / 2^64)^2
        const SCALE: f64 = 18446744073709551616.0; // 2^64 as f64
        // Clamp to valid domain to avoid NaNs and crazy P
        let sp = sqrt_price_x64
            .max(MIN_SQRT_PRICE_X64)
            .min(MAX_SQRT_PRICE_X64) as f64;
        let s = sp / SCALE;
        s * s
    }

    #[inline(always)]
    fn price_impact_token0_in(&self, amount_in: u128, sqrt_price_x64: u128, liquidity: u128) -> f64 {
        // CLAMM token0-in approximation:
        // 1/√P' = 1/√P + Δx/L  -> √P' = 1 / (1/√P + Δx/L)
        if amount_in == 0 || liquidity == 0 { return 0.0; }
        const SCALE: f64 = 18446744073709551616.0; // 2^64
        let sp = sqrt_price_x64.max(MIN_SQRT_PRICE_X64).min(MAX_SQRT_PRICE_X64) as f64 / SCALE;
        let inv = 1.0 / sp;
        let dx_over_l = (amount_in as f64) / (liquidity as f64);
        let sp2 = 1.0 / (inv + dx_over_l);
        let p1 = sp * sp;
        let p2 = sp2 * sp2;
        ((p2 - p1) / p1).max(0.0)
    }

    #[inline(always)]
    fn optimal_amount_token0_in(&self, liquidity: u128, sqrt_price_x64: u128, rel_gap: f64) -> u128 {
        // Closed-form-ish chooser: push until half the gap is closed by CL slippage.
        // Solve price_impact_token0_in(dx) ≈ rel_gap * 0.5  ⇒ dx ≈ L * (1/√P' - 1/√P)
        const SCALE: f64 = 18446744073709551616.0;
        let sp = sqrt_price_x64 as f64 / SCALE;
        let p = sp * sp;
        let target = (rel_gap * 0.5).max(0.0005).min(0.02); // guardrails
        // We want p2 = p * (1 + target) ⇒ √P' = √(p2) = √P * √(1+target)
        let sp_target = sp * (1.0 + target).sqrt();
        let inv_current = 1.0 / sp;
        let inv_target  = 1.0 / sp_target;
        let dx_over_l = (inv_target - inv_current).abs().max(0.0);
        let dx = (dx_over_l * (liquidity as f64)).max(0.0);
        dx as u128
    }

    #[derive(Clone, Copy)]
    enum SwapSide { Token0In, Token1In }

    #[inline(always)]
    fn price_impact_any(&self, amount_in: u128, sqrt_price_x64: u128, liquidity: u128, side: SwapSide) -> f64 {
        if amount_in == 0 || liquidity == 0 { return 0.0; }
        const SCALE: f64 = 18446744073709551616.0; // 2^64
        let sp = (sqrt_price_x64.max(MIN_SQRT_PRICE_X64).min(MAX_SQRT_PRICE_X64) as f64) / SCALE;
        let p1 = sp * sp;

        let sp2 = match side {
            // Token0 in: 1/√P' = 1/√P + Δx/L  => √P' = 1 / (1/√P + Δx/L)
            SwapSide::Token0In => {
                let inv = 1.0 / sp;
                let dx_over_l = (amount_in as f64) / (liquidity as f64);
                1.0 / (inv + dx_over_l)
            }
            // Token1 in: √P' = √P + Δy/L
            SwapSide::Token1In => {
                let dy_over_l = (amount_in as f64) / (liquidity as f64);
                (sp + dy_over_l).max(1e-18)
            }
        };

        let p2 = sp2 * sp2;
        Self::monotone_clamp(0.0, ((p2 - p1) / p1).abs())
    }

    #[inline(always)]
    fn optimal_amount_any(&self, liquidity: u128, sqrt_price_x64: u128, rel_gap: f64, side: SwapSide) -> u128 {
        // Target: close ~50% of the observed gap via CL slippage (guarded 0.05–2%).
        let target = (rel_gap * 0.5).clamp(0.0005, 0.02);
        const SCALE: f64 = 18446744073709551616.0;

        let sp = (sqrt_price_x64 as f64) / SCALE;
        let sp_tgt = sp * (1.0 + target).sqrt(); // want p2 = p*(1+target)

        let dx_or_dy_over_l = match side {
            // For token0-in, solve: 1/√P' = 1/√P + Δx/L  => Δx/L = 1/√P' - 1/√P
            SwapSide::Token0In => (1.0 / sp_tgt) - (1.0 / sp),
            // For token1-in, solve: √P' = √P + Δy/L  => Δy/L = √P' - √P
            SwapSide::Token1In => sp_tgt - sp,
        }
        .abs()
        .max(0.0);

        (dx_or_dy_over_l * (liquidity as f64)) as u128
    }

    #[inline]
    fn oracle_drift_ratio(&self, pool: &PoolState) -> Option<f64> {
        // Expect price_oracle to hold per-mint prices in same quote (e.g., USD, 6dps). If either missing → None.
        let map = self.price_oracle.read();
        let pa = *map.get(&pool.token_mint_a)?;
        let pb = *map.get(&pool.token_mint_b)?;
        if pa == 0 || pb == 0 { return None; }

        let p_oracle = (pa as f64) / (pb as f64);
        let p_pool   = self.sqrt_price_to_price(pool.sqrt_price_x64).max(1e-18);
        Some(((p_pool - p_oracle).abs() / p_oracle).max(0.0))
    }

    /// === ADD BELOW (helpers): cluster piecewise CL impact (desk-grade) ===
    #[inline(always)]
    fn cluster_boundaries(&self, cluster: &TickCluster) -> Vec<(i32, u128, i128)> {
        // (tick_index, sqrt_price_x64_at_tick, liquidity_net_at_crossing)
        // Provided cluster.ticks already include sqrt at each tick and liquidity_net per Uniswap/Whirlpool semantics.
        let mut v: Vec<(i32, u128, i128)> = cluster
            .ticks
            .iter()
            .map(|t| (t.tick_index, t.sqrt_price_x64, t.liquidity_net))
            .collect();
        v.sort_unstable_by(|a, b| a.0.cmp(&b.0)); // ascending tick index (price)
        v
    }

    #[inline(always)]
    fn cluster_price_impact_any(
        &self,
        amount_in: u128,
        pool: &PoolState,
        cluster: &TickCluster,
        side: SwapSide,
    ) -> f64 {
        if amount_in == 0 { return 0.0; }
        const SCALE: f64 = 18446744073709551616.0; // 2^64

        let bounds = self.cluster_boundaries(cluster);
        if bounds.is_empty() { return 0.0; }

        // Current price
        let mut sa = (pool.sqrt_price_x64.max(MIN_SQRT_PRICE_X64).min(MAX_SQRT_PRICE_X64) as f64) / SCALE;
        let p1 = sa * sa;

        // Current in-range liquidity (given by pool state)
        let mut liq = (pool.liquidity as f64).max(1.0);

        // We walk tick-by-tick in the movement direction, consuming amount until exhausted.
        let mut rem = amount_in as f64;

        // Build directional boundary iterator
        let mut idxs: Vec<(f64, i128)> = bounds
            .iter()
            .map(|(_, spx64, lnet)| ((*spx64 as f64) / SCALE, *lnet))
            .collect();

        // Filter relevant side and order by sqrt
        match side {
            SwapSide::Token1In => {
                // Price increases toward higher sqrt
                idxs.retain(|(sb, _)| *sb > sa);
                idxs.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap());
            }
            SwapSide::Token0In => {
                // Price decreases toward lower sqrt
                idxs.retain(|(sb, _)| *sb < sa);
                idxs.sort_by(|a, b| b.0.partial_cmp(&a.0).unwrap());
            }
        }

        // Iterate segments
        for (sb, lnet) in idxs {
            if rem <= 0.0 { break; }
            // segment max consumable until next boundary
            match side {
                SwapSide::Token1In => {
                    let dy_max = liq * (sb - sa).max(0.0);
                    if rem <= dy_max {
                        // solve √P' = √P + Δy/L
                        let sp2 = sa + rem / liq;
                        let p2 = sp2 * sp2;
                        return Self::monotone_clamp(0.0, ((p2 - p1) / p1).abs());
                    } else {
                        rem -= dy_max;
                        sa = sb;
                        // crossing upward: add liquidity_net
                        liq = (liq + (*lnet as f64)).max(1.0);
                    }
                }
                SwapSide::Token0In => {
                    // token0-in moves price DOWN (sqrt decreases)
                    // Δx_max = L * (1/sa - 1/sb)
                    let inv_sa = 1.0 / sa;
                    let inv_sb = 1.0 / sb.max(1e-18);
                    let dx_max = liq * (inv_sa - inv_sb).max(0.0);
                    if rem <= dx_max {
                        // 1/√P' = 1/√P + Δx/L  → √P' = 1 / (1/√P + Δx/L)
                        let sp2 = 1.0 / (inv_sa + rem / liq);
                        let p2 = sp2 * sp2;
                        return Self::monotone_clamp(0.0, ((p2 - p1) / p1).abs());
                    } else {
                        rem -= dx_max;
                        sa = sb;
                        // crossing downward: reverse direction ⇒ subtract lnet
                        liq = (liq - (*lnet as f64)).max(1.0);
                    }
                }
            }
        }

        // If we ran out of boundaries inside cluster, consume the remainder within the last segment
        if rem > 0.0 {
            let sp2 = match side {
                SwapSide::Token1In => sa + rem / liq,
                SwapSide::Token0In => {
                    let inv_sa = 1.0 / sa;
                    1.0 / (inv_sa + rem / liq)
                }
            };
            let p2 = sp2 * sp2;
            return Self::monotone_clamp(0.0, ((p2 - p1) / p1).abs());
        }

        0.0
    }

    #[inline(always)]
    fn cluster_optimal_amount_any(
        &self,
        pool: &PoolState,
        cluster: &TickCluster,
        rel_gap: f64,
        side: SwapSide,
    ) -> u128 {
        // Monotone bisection on amount to target ~50% of observed gap via piecewise impact.
        let target = (rel_gap * 0.5).clamp(0.0005, 0.02);
        if target <= 0.0 { return 0; }

        // Start with a small bracket and expand exponentially until impact >= target or cap.
        let mut lo: f64 = 0.0;
        let mut hi: f64 = 1e6; // start at 1e6 units
        let mut imp = self.cluster_price_impact_any(hi as u128, pool, cluster, side);
        let cap: f64 = 1e18; // hard safety cap

        let mut expansions = 0;
        while imp < target && hi < cap && expansions < 32 {
            hi *= 2.0;
            imp = self.cluster_price_impact_any(hi as u128, pool, cluster, side);
            expansions += 1;
        }
        if imp < target { return hi.min(cap) as u128; } // even max couldn't reach target

        // Bisection
        for _ in 0..32 {
            let mid = 0.5 * (lo + hi);
            let im = self.cluster_price_impact_any(mid as u128, pool, cluster, side);
            if im < target { lo = mid; } else { hi = mid; }
        }
        hi as u128
    }

    /// === ADD BELOW (helpers): compute budget instructions from opportunity ===
    #[inline(always)]
    pub fn compute_budget_ixs_for_op(&self, op: &MevOpportunity, cu_price_micro_lamports: u64) -> [Instruction; 2] {
        let cu_limit = self.gas_limit_for_op(op);
        [
            ComputeBudgetInstruction::set_compute_unit_limit(cu_limit),
            ComputeBudgetInstruction::set_compute_unit_price(cu_price_micro_lamports),
        ]
    }

    /// === ADD BELOW (helpers): EMA + persistence ===
    #[inline(always)]
    fn ema(prev: f64, value: f64, alpha: f64) -> f64 {
        (alpha * value + (1.0 - alpha) * prev).clamp(0.0, 1.0)
    }

    #[inline(always)]
    fn cluster_overlap_ratio(a: &TickCluster, b: &TickCluster) -> f64 {
        // Approximate by tick-range overlap
        let a_lo = a.ticks.first().map(|t| t.tick_index).unwrap_or(a.center_tick);
        let a_hi = a.ticks.last().map(|t| t.tick_index).unwrap_or(a.center_tick);
        let b_lo = b.ticks.first().map(|t| t.tick_index).unwrap_or(b.center_tick);
        let b_hi = b.ticks.last().map(|t| t.tick_index).unwrap_or(b.center_tick);
        let lo = a_lo.max(b_lo);
        let hi = a_hi.min(b_hi);
        if hi <= lo { return 0.0; }
        let inter = (hi - lo) as f64;
        let union = ((a_hi - a_lo).abs() + (b_hi - b_lo).abs()) as f64 - inter;
        if union <= 0.0 { 0.0 } else { (inter / union).clamp(0.0, 1.0) }
    }

    /// === ADD BELOW (helpers): PoI + CU price shaping ===
    #[inline(always)]
    fn desk_exec_probability(vol_score: f64, conc_score: f64, drift: f64, rel_gap: f64, impact: f64) -> f64 {
        // Higher with lower vol, higher concentration; penalize drift and tiny residual edges.
        let residual = (rel_gap - impact).max(0.0);
        let base = 0.55
            + 0.30 * (1.0 - vol_score).clamp(0.0, 1.0)
            + 0.15 * conc_score.clamp(0.0, 1.0);
        let drift_pen = (drift * 3.0).min(0.25);
        let residual_bonus = (residual * 50.0).min(0.20);
        (base + residual_bonus - drift_pen).clamp(0.10, 0.99)
    }


    #[inline(always)]
    async fn cu_price_micro_for_op(&self, op_ev: u64, gas_est: u64, slot_phase: f64) -> u64 {
        let ev_per_cu = (op_ev as f64) / (gas_est.max(1) as f64);
        let late = slot_phase.clamp(0.0, 1.0);
        // phase-cubic ramp (kept)
        let ramp = if late <= 0.8 { 1.0 + (late / 0.8) } else { 2.0 + 3.0 * ((late - 0.8) / 0.2).powi(3) };

        // Map phase→quantile: early 0.40, mid 0.55, late 0.70, tail 0.85
        let q = if late < 0.25 { 0.40 } else if late < 0.75 { 0.55 } else if late < 0.95 { 0.70 } else { 0.85 };
        // Snapshot cached vector without await when possible
        let median_cached = { self.fee_hist_cache.read().1 };
        let anchor_q = self.fee_quantile_micro(q).await;
        let market_anchor = if anchor_q == 0 { 
            if median_cached == 0 { 1.0 } else { (median_cached as f64) / 1_000.0 } 
        } else { (anchor_q as f64) / 1_000.0 };

        // Base microprice
        let mut micro = (ev_per_cu * 0.26 * ramp * market_anchor).max(median_cached as f64 * 0.6).max(1.0);
        // Tail floor (kept from earlier pass)
        micro = micro.max(self.tail_micro_floor(slot_phase, median_cached) as f64);
        micro.min(300_000.0) as u64
    }

    /// === ADD BELOW (helpers): dynamic flow threshold ===
    #[inline(always)]
    fn dynamic_volume_threshold_from_vol(vol_0_to_1: f64) -> u64 {
        // Raise threshold in high-vol regimes to avoid toxic flow traps.
        // 1.0..2.4 × base depending on vol (smooth)
        let mult = 1.0 + 1.4 * vol_0_to_1.clamp(0.0, 1.0);
        (VOLUME_THRESHOLD as f64 * mult) as u64
    }

    /// === ADD BELOW (helpers): EV floor from heat & persistence ===
    #[inline(always)]
    fn arb_min_ev_floor(ema_vol: f64, persistence: f64) -> f64 {
        // Hot/unstable markets demand higher EV to pay for inclusion & slippage variance.
        // 5.5k → 10k across (vol ↑, persistence ↓).
        let base = 5_500.0;
        let v_up = 4_000.0 * ema_vol.clamp(0.0, 1.0);
        let p_dn = 2_500.0 * (1.0 - persistence.clamp(0.0, 1.0));
        base + v_up + p_dn
    }

    /// === ADD BELOW (helpers): decimals pow10 and decimal-aware price ===
    #[inline(always)]
    fn pow10_i32(exp: i32) -> f64 {
        match exp.clamp(-18, 18) {
            -18 => 1e-18, -17 => 1e-17, -16 => 1e-16, -15 => 1e-15, -14 => 1e-14, -13 => 1e-13,
            -12 => 1e-12, -11 => 1e-11, -10 => 1e-10, -9 => 1e-9, -8 => 1e-8, -7 => 1e-7, -6 => 1e-6,
            -5 => 1e-5, -4 => 1e-4, -3 => 1e-3, -2 => 1e-2, -1 => 1e-1, 0 => 1.0,
             1 => 1e1,  2 => 1e2,  3 => 1e3,  4 => 1e4,  5 => 1e5,  6 => 1e6,  7 => 1e7,  8 => 1e8,
             9 => 1e9, 10 => 1e10, 11 => 1e11, 12 => 1e12, 13 => 1e13, 14 => 1e14, 15 => 1e15,
            16 => 1e16, 17 => 1e17, 18 => 1e18, _ => unreachable!(),
        }
    }

    #[inline(always)]
    fn mint_decimals_from_data(data: &[u8]) -> Option<u8> {
        // Try SPL-Token first; fallback to raw offset 44 if unpack fails or layout differs
        if let Ok(m) = SplMint::unpack_from_slice(data) {
            return Some(m.decimals);
        }
        if data.len() > 45 { return Some(data[44]); } // common layout path
        None
    }

    #[inline(always)]
    async fn fetch_token_decimals(&self, mint: &Pubkey) -> u8 {
        let now = Self::now_millis();
        // Cache lookup
        if let Some((ts, d)) = self.token_decimals_cache.read().get(mint).cloned() {
            if now.saturating_sub(ts) <= DECIMALS_TTL_MS { return d; }
        }
        // Fetch and parse
        match self.rpc_client.get_account(mint).await {
            Ok(acc) => {
                if let Some(d) = Self::mint_decimals_from_data(&acc.data) {
                    self.token_decimals_cache.write().insert(*mint, (now, d));
                    // prune if oversized
                    if self.token_decimals_cache.read().len() > MAX_DECIMALS_CACHE {
                        // drop random 1/8th
                        let mut w = self.token_decimals_cache.write();
                        let take = w.len() / 8;
                        for k in w.keys().take(take).cloned().collect::<Vec<_>>() { w.remove(&k); }
                    }
                    d
                } else {
                    // conservative default
                    6
                }
            }
            Err(_) => 6,
        }
    }

    #[inline(always)]
    async fn pool_price_ratio_decimals(&self, pool: &PoolState) -> f64 {
        let p_raw = self.sqrt_price_to_price(pool.sqrt_price_x64);

        // Fast cache path
        let now = Self::now_millis();
        let ca = self.token_decimals_cache.read().get(&pool.token_mint_a).cloned();
        let cb = self.token_decimals_cache.read().get(&pool.token_mint_b).cloned();
        let mut da = ca.and_then(|(ts, d)| if now.saturating_sub(ts) <= DECIMALS_TTL_MS { Some(d) } else { None });
        let mut db = cb.and_then(|(ts, d)| if now.saturating_sub(ts) <= DECIMALS_TTL_MS { Some(d) } else { None });

        if da.is_none() || db.is_none() {
            // Single batched RTT
            let accounts = self.rpc_get_multiple_accounts(&[pool.token_mint_a, pool.token_mint_b]).await.unwrap_or_default();

            if let Some(Some(acc_a)) = accounts.get(0) {
                if let Some(d) = Self::mint_decimals_from_data(&acc_a.data) {
                    da = Some(d);
                    self.token_decimals_cache.write().insert(pool.token_mint_a, (now, d));
                }
            }
            if let Some(Some(acc_b)) = accounts.get(1) {
                if let Some(d) = Self::mint_decimals_from_data(&acc_b.data) {
                    db = Some(d);
                    self.token_decimals_cache.write().insert(pool.token_mint_b, (now, d));
                }
            }
        }

        let da = da.unwrap_or(6) as i32;
        let db = db.unwrap_or(6) as i32;
        p_raw * Self::pow10_i32(da - db)
    }

    /// === ADD BELOW (helpers): measured slot length & phase; TTL pool-state fetch ===
    #[inline(always)]
    async fn fetch_slot_ms(&self) -> u64 {
        let now = Self::now_millis();
        {
            let (ts, ms) = *self.slot_ms_cache.read();
            if now.saturating_sub(ts) <= SLOTMS_TTL_MS { return ms.max(200).min(1_000); }
        }
        let measured = match self.rpc_client.get_recent_performance_samples(Some(1)).await {
            Ok(samples) if !samples.is_empty() => {
                let RpcPerfSample { num_slots, sample_period_secs, .. } = samples[0];
                let slots = num_slots.max(1) as f64;
                let secs  = sample_period_secs.max(1) as f64;
                ((secs * 1000.0) / slots).round().clamp(200.0, 1000.0) as u64
            },
            _ => 400,
        } as f64;

        // EWMA with previous value
        let prev = self.slot_ms_cache.read().1 as f64;
        let smoothed = ((1.0 - SLOTMS_ALPHA) * prev + SLOTMS_ALPHA * measured).round() as u64;
        *self.slot_ms_cache.write() = (now, smoothed);
        smoothed
    }

    #[inline(always)]
    async fn slot_phase_0_to_1(&self) -> f64 {
        let ms = self.fetch_slot_ms().await;
        let period = ms.max(200);
        ((Self::now_millis() % period) as f64) / (period as f64)
    }

    #[inline(always)]
    async fn fetch_pool_state_ttl(&self, pool: &Pubkey) -> Option<PoolState> {
        let now = Self::now_millis();
        if let Some((ts, ps)) = self.pool_state_cache.read().get(pool).cloned() {
            if now.saturating_sub(ts) <= POOLSTATE_TTL_MS { return Some(ps); }
        }
        // processed -> confirmed fallback
        use tokio::time::{timeout, Duration};
        let acc = timeout(Duration::from_millis(150),
            self.rpc_client.get_account_with_commitment(pool, CommitmentConfig::processed())
        ).await.ok().and_then(|r| r.ok()).and_then(|v| v.value)
        .or_else(|| None);

        let account = match acc {
            Some(a) => a,
            None => {
                if let Ok(Ok(r)) = timeout(Duration::from_millis(250),
                    self.rpc_client.get_account_with_commitment(pool, CommitmentConfig::confirmed())
                ).await {
                    r.value?
                } else { return None; }
            }
        };
        if let Ok(ps) = self.decode_pool_state(&account) {
            self.pool_state_cache.write().insert(*pool, (now, ps.clone()));
            return Some(ps);
        }
        None
    }

    /// === ADD BELOW (helpers): pair key + index update/lookup ===
    #[inline(always)]
    fn ordered_pair(a: &Pubkey, b: &Pubkey) -> (Pubkey, Pubkey) {
        if a.to_bytes() <= b.to_bytes() { (*a, *b) } else { (*b, *a) }
    }

    #[inline(always)]
    fn index_add_pool(&self, pool_addr: &Pubkey, pool: &PoolState) {
        let now = Self::now_millis();
        let key = Self::ordered_pair(&pool.token_mint_a, &pool.token_mint_b);
        let mut w = self.pair_index_cache.write();
        let entry = w.entry(key).or_insert_with(|| (0, Arc::new(Vec::new())));
        let mut v = (**entry).1.as_ref().clone();
        if !v.contains(pool_addr) { v.push(*pool_addr); }
        *entry = (now, Arc::new(v));
        if w.len() > MAX_PAIR_INDEX_CACHE {
            let take = w.len() / 8;
            for k in w.keys().take(take).cloned().collect::<Vec<_>>() { w.remove(&k); }
        }
    }

    #[inline(always)]
    fn index_get_peers(&self, pool: &PoolState, this_pool: &Pubkey) -> Vec<Pubkey> {
        let now = Self::now_millis();
        let key = Self::ordered_pair(&pool.token_mint_a, &pool.token_mint_b);
        if let Some((ts, v)) = self.pair_index_cache.read().get(&key).cloned() {
            if now.saturating_sub(ts) <= PAIR_INDEX_TTL_MS {
                return v.iter().cloned().filter(|p| p != this_pool).collect();
            }
        }
        // cold path: fall back to all known pools of same pair from state cache
        let mut peers = Vec::with_capacity(8);
        for (addr, (_, ps)) in self.pool_state_cache.read().iter() {
            if addr == this_pool { continue; }
            if (ps.token_mint_a == pool.token_mint_a && ps.token_mint_b == pool.token_mint_b) ||
               (ps.token_mint_a == pool.token_mint_b && ps.token_mint_b == pool.token_mint_a) {
                peers.push(*addr);
            }
        }
        self.pair_index_cache.write().insert(key, (now, Arc::new(peers.clone())));
        peers
    }

    /// === ADD BELOW (helpers): cross-pool arb sizing & EV ===
    fn crosspool_direction(p_a: f64, p_b: f64) -> (SwapSide, SwapSide) {
        // Prices are tokenB per tokenA. If p_b < p_a: buy B on pool_b (token0-in), sell B on pool_a (token1-in).
        if p_b < p_a { (SwapSide::Token0In, SwapSide::Token1In) } else { (SwapSide::Token1In, SwapSide::Token0In) }
    }

    fn min_nonzero(a: u128, b: u128) -> u128 { if a == 0 { b } else if b == 0 { a } else { a.min(b) } }

    async fn detect_cross_pool_arbitrage_from_cache(
        &self,
        this_pool_addr: &Pubkey,
        this_pool: &PoolState,
        this_clusters: &[TickCluster],
    ) -> Vec<MevOpportunity> {
        let Some(this_dom) = self.find_dominant_cluster(this_clusters) else { return Vec::new(); };

        // maintain index
        self.index_add_pool(this_pool_addr, this_pool);

        let p_this_human = self.pool_price_ratio_decimals(this_pool).await;
        let p_this = self.sqrt_price_to_price(this_pool.sqrt_price_x64);

        let mut out = Vec::with_capacity(4);
        let peers = self.index_get_peers(this_pool, this_pool_addr);
        let mut seen_pairs: HashSet<(Pubkey, Pubkey)> = HashSet::with_capacity(peers.len());

        for other_addr in peers {
            // dedupe by ordered (this, other)
            let pair_key = if this_pool_addr.to_bytes() <= other_addr.to_bytes() {
                (*this_pool_addr, other_addr)
            } else {
                (other_addr, *this_pool_addr)
            };
            if !seen_pairs.insert(pair_key) { continue; }

            let Some(other_analysis) = self.cluster_cache.read().get(&other_addr) else { continue; };
            let Some(ref other_dom) = other_analysis.dominant_cluster else { continue; };
            let Some(other_pool) = self.fetch_pool_state_ttl(&other_addr).await else { continue; };

            // must be same mint ordering (we already normalized)
            if Self::ordered_pair(&other_pool.token_mint_a, &other_pool.token_mint_b)
               != Self::ordered_pair(&this_pool.token_mint_a, &this_pool.token_mint_b) {
                continue;
            }

            let p_other = self.sqrt_price_to_price(other_pool.sqrt_price_x64);
            let rel_diff = ((p_this - p_other).abs() / p_this).max(0.0);
            if rel_diff <= 0.0012 { continue; }

            let (side_other, side_this) = Self::crosspool_direction(p_this, p_other);
            let amt_other = self.cluster_optimal_amount_any(&other_pool, other_dom, rel_diff, side_other);
            let amt_this  = self.cluster_optimal_amount_any(this_pool, &this_dom, rel_diff, side_this);
            let amt_in = Self::min_nonzero(amt_other, amt_this);
            if amt_in == 0 { continue; }

            let imp_other = self.cluster_price_impact_any(amt_in, &other_pool, other_dom, side_other);
            let imp_this  = self.cluster_price_impact_any(amt_in, this_pool, &this_dom, side_this);
            if imp_other + imp_this >= rel_diff { continue; }

            let fee_total = ((this_pool.fee_rate as f64 + this_pool.protocol_fee_rate as f64)
                           + (other_pool.fee_rate as f64 + other_pool.protocol_fee_rate as f64))
                           / BASIS_POINT_MAX as f64;

            let p_human = p_this_human.max(1e-12);
            let notional = match side_other {
                SwapSide::Token0In => (amt_in as f64) * p_human,
                SwapSide::Token1In => (amt_in as f64).max(1e-12) / p_human,
            };

            let gross = notional * (rel_diff - imp_other - imp_this).max(0.0);
            let net = (gross * (1.0 - fee_total)).max(0.0);
            if net < 8_000.0 { continue; }

            let vol = (this_dom.volatility_score + other_dom.volatility_score) * 0.5;
            let conc = (this_dom.concentration_score + other_dom.concentration_score) * 0.5;
            let drift = self.oracle_drift_ratio(this_pool).unwrap_or(0.0);

            let exec_p = Self::desk_exec_probability(vol, conc, drift, rel_diff, imp_other + imp_this);
        let base_micro = self.cu_price_micro_for_op(net as u64, 380_000, self.slot_phase_0_to_1());
        let base_micro = self.cap_micro_by_ev(base_micro, net as u64, 380_000);
            let seed_hi = (this_pool.sqrt_price_x64 as u64) ^ (other_pool.sqrt_price_x64 as u64);
            let priority_fee = Self::jitter_micro(base_micro, seed_hi, Self::unix_time_now());

            out.push(MevOpportunity {
                opportunity_type: OpportunityType::CrossPoolArbitrage,
                tick_range: (
                    this_dom.ticks.first().unwrap().tick_index.min(other_dom.ticks.first().unwrap().tick_index),
                    this_dom.ticks.last().unwrap().tick_index.max(other_dom.ticks.last().unwrap().tick_index)
                ),
                expected_profit: net as u64,
                risk_score: (0.25 + vol * 0.45).min(0.95),
                execution_probability: exec_p,
                gas_estimate: 380_000,
                priority_fee,
            });
        }
        out
    }

    /// === ADD BELOW (helpers): triangular cross-DEX arbitrage detection ===
    async fn detect_triangular_arbitrage_from_cache(
        &self,
        this_addr: &Pubkey,
        this_pool: &PoolState,
        this_clusters: &[TickCluster],
    ) -> Vec<MevOpportunity> {
        // This pool is A↔B
        let a = this_pool.token_mint_a;
        let b = this_pool.token_mint_b;

        let Some(this_dom) = self.find_dominant_cluster(this_clusters) else { return Vec::new(); };
        let p_ab = self.pool_price_ratio_decimals(this_pool).await.max(1e-12);

        // Collect candidate C via pools that connect to B or A
        let mut c_candidates: HashSet<Pubkey> = HashSet::with_capacity(16);
        {
            for (_, (_, ps)) in self.pool_state_cache.read().iter() {
                if ps.token_mint_a == b { c_candidates.insert(ps.token_mint_b); }
                if ps.token_mint_b == b { c_candidates.insert(ps.token_mint_a); }
                if ps.token_mint_a == a { c_candidates.insert(ps.token_mint_b); }
                if ps.token_mint_b == a { c_candidates.insert(ps.token_mint_a); }
            }
        }

        let mut out = Vec::with_capacity(4);
        // Cap work
        for c in c_candidates.into_iter().take(12) {
            if c == a || c == b { continue; }

            // Fetch B↔C and C↔A pool states (any venue); pick the freshest per pair
            let mut bc_best: Option<(Pubkey, PoolState)> = None;
            let mut ca_best: Option<(Pubkey, PoolState)> = None;
            for (addr, (_, ps)) in self.pool_state_cache.read().iter() {
                if (ps.token_mint_a == b && ps.token_mint_b == c) || (ps.token_mint_a == c && ps.token_mint_b == b) {
                    bc_best = Some((*addr, ps.clone()));
                }
                if (ps.token_mint_a == c && ps.token_mint_b == a) || (ps.token_mint_a == a && ps.token_mint_b == c) {
                    ca_best = Some((*addr, ps.clone()));
                }
            }
            let (bc_addr, bc_pool) = match bc_best { Some(x) => x, None => continue };
            let (ca_addr, ca_pool) = match ca_best { Some(x) => x, None => continue };

            // Need clusters
            let bc_an = match self.cluster_cache.read().get(&bc_addr) { Some(x) => x, None => continue };
            let ca_an = match self.cluster_cache.read().get(&ca_addr) { Some(x) => x, None => continue };
            let (Some(bc_dom), Some(ca_dom)) = (&bc_an.dominant_cluster, &ca_an.dominant_cluster) else { continue };

            // Prices (human)
            let p_bc = self.pool_price_ratio_decimals(&bc_pool).await.max(1e-12);
            let p_ca = self.pool_price_ratio_decimals(&ca_pool).await.max(1e-12);

            // Loop product: A->B * B->C * C->A (all are token-out per token-in ratios)
            let loop_prod = p_ab * p_bc * p_ca;
            let rel = (loop_prod - 1.0).max(0.0);
            if rel <= 0.0015 { continue; } // desk cutoff

            // Directions
            let s_ab = Self::side_for_step(this_pool, &a);
            let s_bc = Self::side_for_step(&bc_pool, &b);
            let s_ca = Self::side_for_step(&ca_pool, &c);

            // Sizing (conservative min across legs)
            let amt_ab = self.cluster_optimal_amount_any(this_pool, &this_dom, rel, s_ab);
            let amt_bc = self.cluster_optimal_amount_any(&bc_pool, bc_dom, rel, s_bc);
            let amt_ca = self.cluster_optimal_amount_any(&ca_pool, ca_dom, rel, s_ca);
            let amt_in = *[amt_ab, amt_bc, amt_ca].iter().filter(|&&x| x > 0).min().unwrap_or(&0);
            if amt_in == 0 { continue; }

            // Impacts
            let imp_ab = self.cluster_price_impact_any(amt_in, this_pool, &this_dom, s_ab);
            let imp_bc = self.cluster_price_impact_any(amt_in, &bc_pool, bc_dom, s_bc);
            let imp_ca = self.cluster_price_impact_any(amt_in, &ca_pool, ca_dom, s_ca);
            if imp_ab + imp_bc + imp_ca >= rel { continue; }

            // Fees
            let fee_total = ((this_pool.fee_rate as f64 + this_pool.protocol_fee_rate as f64)
                           + (bc_pool.fee_rate as f64 + bc_pool.protocol_fee_rate as f64)
                           + (ca_pool.fee_rate as f64 + ca_pool.protocol_fee_rate as f64))
                           / BASIS_POINT_MAX as f64;

            // Notional in A
            let notional = (amt_in as f64).max(1e-12);

            let gross = notional * (rel - imp_ab - imp_bc - imp_ca).max(0.0);
            let net = (gross * (1.0 - fee_total)).max(0.0);
            if net < 12_000.0 { continue; }

            let vol = (this_dom.volatility_score + bc_dom.volatility_score + ca_dom.volatility_score) / 3.0;
            let conc = (this_dom.concentration_score + bc_dom.concentration_score + ca_dom.concentration_score) / 3.0;
            let drift = self.oracle_drift_ratio(this_pool).unwrap_or(0.0);

            let exec_p = Self::desk_exec_probability(vol, conc, drift, rel, imp_ab + imp_bc + imp_ca);
            let base_phase = self.slot_phase_0_to_1();
            let mut base_micro = self.cu_price_micro_for_op(net as u64, 520_000, base_phase);
            base_micro = self.cap_micro_by_ev(base_micro, net as u64, 520_000);
            let seed_hi = (this_pool.sqrt_price_x64 as u64) ^ (bc_pool.sqrt_price_x64 as u64) ^ (ca_pool.sqrt_price_x64 as u64);
            let priority_fee = Self::jitter_micro(base_micro, seed_hi, Self::unix_time_now());

            out.push(MevOpportunity {
                opportunity_type: OpportunityType::TriangularArbitrage,
                tick_range: (
                    this_dom.ticks.first().unwrap().tick_index.min(bc_dom.ticks.first().unwrap().tick_index.min(ca_dom.ticks.first().unwrap().tick_index)),
                    this_dom.ticks.last().unwrap().tick_index.max(bc_dom.ticks.last().unwrap().tick_index.max(ca_dom.ticks.last().unwrap().tick_index))
                ),
                expected_profit: net as u64,
                risk_score: (0.30 + vol * 0.40).min(0.96),
                execution_probability: exec_p,
                gas_estimate: 520_000,
                priority_fee,
            });
        }
        out
    }

    /// === ADD BELOW (helpers): liquidity_net flip rate ===
    #[inline(always)]
    fn liq_flip_rate(ticks: &[TickData]) -> f64 {
        if ticks.len() < 3 { return 0.0; }
        let mut flips = 0u32;
        let mut prev = ticks[0].liquidity_net.signum();
        for t in &ticks[1..] {
            let s = t.liquidity_net.signum();
            if s != 0 && prev != 0 && s != prev { flips += 1; }
            if s != 0 { prev = s; }
        }
        (flips as f64) / ((ticks.len() - 1) as f64)
    }

    /// === ADD BELOW (helpers): prune pool_state_cache ===
    #[inline(always)]
    fn prune_pool_state_cache(&self) {
        let mut w = self.pool_state_cache.write();
        let cap = 8_192;
        if w.len() <= cap { return; }
        let take = (w.len() - cap) + cap / 8;
        let mut items: Vec<(u64, Pubkey)> = w.iter().map(|(k,(ts,_))| (*ts, *k)).collect();
        items.sort_unstable_by_key(|x| x.0);
        for (_, k) in items.into_iter().take(take) { w.remove(&k); }
    }

    /// === ADD BELOW (helpers): tip jitter + cross-type conflict ===
    #[inline(always)]
    fn jitter_micro(base: u64, seed_hi: u64, seed_lo: u64) -> u64 {
        // xorshift* on a combined seed; ±5% jitter to break auction ties deterministically per send
        let mut x = seed_hi ^ (seed_lo.wrapping_mul(0x9E3779B97F4A7C15));
        x ^= x >> 12; x ^= x << 25; x ^= x >> 27;
        let z = x.wrapping_mul(0x2545F4914F6CDD1D);
        let r = (z as u64 % 101) as i64 - 50; // -50..+50
        let jitter_num = 1000 + r;            // 950..1050
        ((base as u128 * jitter_num as u128) / 1000u128).max(1) as u64
    }

    #[inline(always)]
    fn opps_conflict(a: &MevOpportunity, b: &MevOpportunity) -> bool {
        // Overlapping tick ranges AND one is Sandwich: treat as mutually exclusive with others.
        let overlap = !(a.tick_range.1 < b.tick_range.0 || b.tick_range.1 < a.tick_range.0);
        if !overlap { return false; }
        match (a.opportunity_type, b.opportunity_type) {
            (OpportunityType::Sandwich, _) | (_, OpportunityType::Sandwich) => true,
            // Arbitrage vs JIT in same narrow range can step on each other; block if range width < 24 ticks
            (OpportunityType::TriangularArbitrage, OpportunityType::JitLiquidity) |
            (OpportunityType::JitLiquidity, OpportunityType::TriangularArbitrage) |
            (OpportunityType::Arbitrage, OpportunityType::JitLiquidity) |
            (OpportunityType::JitLiquidity, OpportunityType::Arbitrage) |
            (OpportunityType::CrossPoolArbitrage, OpportunityType::JitLiquidity) |
            (OpportunityType::JitLiquidity, OpportunityType::CrossPoolArbitrage) => {
                let width = (a.tick_range.1 - a.tick_range.0).abs().min((b.tick_range.1 - b.tick_range.0).abs());
                width < 24
            }
            _ => false,
        }
    }

    /// === ADD BELOW (helpers): get or create per-pool semaphore ===
    #[inline(always)]
    fn pool_guard(&self, pool: &Pubkey) -> Arc<tokio::sync::Semaphore> {
        if let Some(s) = self.inflight_guards.read().get(pool).cloned() {
            return s;
        }
        let s = Arc::new(tokio::sync::Semaphore::new(1));
        self.inflight_guards.write().insert(*pool, s.clone());
        s
    }

    /// === ADD BELOW (helpers): hedged getMultipleAccounts v2 (staggered) ===
    async fn rpc_get_multiple_accounts(&self, keys: &[Pubkey]) -> Result<Vec<Option<Account>>, Box<dyn std::error::Error>> {
        use tokio::time::timeout;

        let order = self.rpc_rank();
        if order.len() == 1 {
            let t0 = Self::now_millis();
            let r = self.rpc_ring[order[0]].get_multiple_accounts(keys).await;
            self.rpc_update_stats(order[0], Self::now_millis().saturating_sub(t0), r.is_ok());
            return Ok(r?);
        }

        let mut futs = FuturesUnordered::new();
        for (k, &i) in order.iter().enumerate() {
            let cli = self.rpc_ring[i].clone();
            let kvec = keys.to_vec();
            // staggered launch
            if k > 0 { sleep(TokioDuration::from_millis(self.hedge_stagger_ms() * k as u64)).await; }
            futs.push(async move {
                let t0 = Self::now_millis();
                let to_ms = self.rpc_dynamic_timeout_ms(i);
                let res = timeout(Duration::from_millis(to_ms), cli.get_multiple_accounts(&kvec)).await;
                (i, t0, res)
            });
        }

        while let Some((idx, t0, res)) = futs.next().await {
            match res {
                Ok(Ok(v)) => {
                    self.rpc_update_stats(idx, Self::now_millis().saturating_sub(t0), true);
                    self.rpc_refresh_slot_bg(idx);
                    return Ok(v);
                }
                _ => {
                    self.rpc_update_stats(idx, Self::now_millis().saturating_sub(t0), false);
                    self.rpc_refresh_slot_bg(idx);
                    self.rpc_maybe_trip_cb(idx);
                }
            }
        }

        // last resort: best endpoint without timeout
        let best = order[0];
        let t0 = Self::now_millis();
        let r = self.rpc_ring[best].get_multiple_accounts(keys).await;
        self.rpc_update_stats(best, Self::now_millis().saturating_sub(t0), r.is_ok());
        Ok(r?)
    }

    /// === ADD BELOW (helpers): fresh blockhash ===
    #[inline(always)]
    async fn fresh_blockhash(&self) -> Hash {
        let now = Self::now_millis();
        if let Some((ts, h)) = self.blockhash_cache.read().clone().into() {
            if now.saturating_sub(ts) <= BLOCKHASH_TTL_MS && h != Hash::default() { return h; }
        }
        // Primary is fine for blockhash
        match self.rpc_client.get_latest_blockhash().await {
            Ok(h) => {
                *self.blockhash_cache.write() = (now, h);
                h
            }
            Err(_) => {
                // fall back to any ring member
                for cli in self.rpc_ring.iter().skip(1) {
                    if let Ok(h) = cli.get_latest_blockhash().await {
                        *self.blockhash_cache.write() = (now, h);
                        return h;
                    }
                }
                self.blockhash_cache.read().1
            }
        }
    }

    /// === ADD BELOW (helpers): hedged send_transaction_with_config across rpc_ring ===
    async fn broadcast_transaction_hedged(&self, tx: &Transaction) -> Result<Signature, Box<dyn std::error::Error>> {
        use tokio::time::timeout;

        let cfg = RpcSendTransactionConfig {
            skip_preflight: true,
            preflight_commitment: Some(CommitmentLevel::Processed),
            max_retries: Some(0),
            ..Default::default()
        };

        let order = self.rpc_rank();
        if order.len() == 1 {
            let sig = self.rpc_ring[order[0]].send_transaction_with_config(tx, cfg).await?;
            return Ok(sig);
        }

        let mut futs = FuturesUnordered::new();
        for (k, &i) in order.iter().enumerate() {
            let cli = self.rpc_ring[i].clone();
            let txc = tx.clone();
            if k > 0 { sleep(TokioDuration::from_millis(self.hedge_stagger_ms() * k as u64)).await; }
            futs.push(async move {
                let t0 = Self::now_millis();
                let to_ms = self.rpc_dynamic_timeout_ms(i);
                let res = timeout(TokioDuration::from_millis(to_ms), cli.send_transaction_with_config(&txc, cfg)).await;
                (i, t0, res)
            });
        }

        while let Some((idx, t0, res)) = futs.next().await {
            match res {
                Ok(Ok(sig)) => {
                    self.rpc_update_stats(idx, Self::now_millis().saturating_sub(t0), true);
                    self.rpc_refresh_slot_bg(idx);
                    return Ok(sig);
                }
                _ => {
                    self.rpc_update_stats(idx, Self::now_millis().saturating_sub(t0), false);
                    self.rpc_refresh_slot_bg(idx);
                    self.rpc_maybe_trip_cb(idx);
                }
            }
        }

        // last resort: best endpoint without timeout
        let best = order[0];
        let sig = self.rpc_ring[best].send_transaction_with_config(tx, cfg).await?;
        Ok(sig)
    }

    /// === ADD BELOW (helpers): hedged send for VersionedTransaction ===
    async fn broadcast_versioned_transaction_hedged(&self, tx: &VersionedTransaction) -> Result<Signature, Box<dyn std::error::Error>> {
        use tokio::time::timeout;

        let cfg = RpcSendTransactionConfig {
            skip_preflight: true,
            preflight_commitment: Some(CommitmentLevel::Processed),
            max_retries: Some(0),
            ..Default::default()
        };

        let order = self.rpc_rank();
        if order.len() == 1 {
            let sig = self.rpc_ring[order[0]].send_transaction(tx).await?;
            return Ok(sig);
        }

        let mut futs = FuturesUnordered::new();
        for (k, &i) in order.iter().enumerate() {
            let cli = self.rpc_ring[i].clone();
            let txc = tx.clone();
            if k > 0 { sleep(TokioDuration::from_millis(self.hedge_stagger_ms() * k as u64)).await; }
            futs.push(async move {
                let t0 = Self::now_millis();
                let to_ms = self.rpc_dynamic_timeout_ms(i);
                let res = timeout(TokioDuration::from_millis(to_ms), cli.send_transaction(&txc)).await;
                (i, t0, res)
            });
        }

        while let Some((idx, t0, res)) = futs.next().await {
            match res {
                Ok(Ok(sig)) => {
                    self.rpc_update_stats(idx, Self::now_millis().saturating_sub(t0), true);
                    self.rpc_refresh_slot_bg(idx);
                    return Ok(sig);
                }
                _ => {
                    self.rpc_update_stats(idx, Self::now_millis().saturating_sub(t0), false);
                    self.rpc_refresh_slot_bg(idx);
                    self.rpc_maybe_trip_cb(idx);
                }
            }
        }

        // last resort: best endpoint without timeout
        let best = order[0];
        let sig = self.rpc_ring[best].send_transaction(tx).await?;
        Ok(sig)
    }

    /// === ADD BELOW (helpers): median CU microprice from recent histogram ===
    #[inline(always)]
    async fn median_fee_micro(&self) -> u64 {
        let now = Self::now_millis();
        let (ts, val) = *self.fee_hist_cache.read();
        if now.saturating_sub(ts) <= FEE_HIST_TTL_MS { return val; }

        // Try primary; fall back to ring.
        let fetch = |cli: Arc<RpcClient>| async move { cli.get_recent_prioritization_fees().await };
        let mut best: Option<Vec<RpcPrioritizationFee>> = None;
        if let Ok(v) = fetch(self.rpc_client.clone()).await { best = Some(v); }
        if best.is_none() {
            for c in self.rpc_ring.iter().skip(1) {
                if let Ok(v) = fetch(c.clone()).await { best = Some(v); break; }
            }
        }
        let median = if let Some(v) = best {
            if v.is_empty() { 0 } else {
                let mut xs: Vec<u64> = v.into_iter().map(|f| f.prioritization_fee).collect();
                xs.sort_unstable();
                xs[xs.len()/2]
            }
        } else { 0 };
        *self.fee_hist_cache.write() = (now, median);
        median
    }

    /// === ADD BELOW (helpers): discretize amount to buckets ===
    #[inline(always)]
    fn amt_bucket(amount: u128) -> u8 {
        // log2 bucket: 0..63 fits common sizes; caps noise
        let mut x = amount;
        let mut b = 0u8;
        while x > 1 && b < 63 { x >>= 1; b += 1; }
        b
    }

    /// === ADD BELOW (helpers): monotone clamp ===
    #[inline(always)]
    fn monotone_clamp(prev: f64, next: f64) -> f64 {
        if next.is_finite() && next >= 0.0 { next } else { prev.max(0.0) }
    }

    /// === ADD BELOW (helpers): FNV-1a 64-bit (no extra deps) ===
    #[inline(always)]
    fn fnv1a64(bytes: &[u8]) -> u64 {
        let mut h: u64 = 0xcbf29ce484222325u64;
        for &b in bytes {
            h ^= b as u64;
            h = h.wrapping_mul(0x100000001b3u64);
        }
        h
    }

    /// === ADD BELOW (helpers): bounded insert into hash cache ===
    #[inline(always)]
    fn tickarray_hash_put(&self, key: &Pubkey, ts: u64, h: u64) {
        let mut w = self.tick_array_hash_cache.write();
        w.insert(*key, (ts, h));
        if w.len() > MAX_TICKARRAY_HASH_CACHE {
            let take = w.len() / 8;
            for k in w.keys().take(take).cloned().collect::<Vec<_>>() { w.remove(&k); }
        }
    }

    /// === ADD BELOW (helpers): adaptive chunk sizing ===
    #[inline(always)]
    fn chunk_size(&self) -> usize {
        self.chunk_target.load(Ordering::Relaxed).clamp(40, 128)
    }
    #[inline(always)]
    fn adapt_chunk_on_result(&self, elapsed_ms: u64, ok: bool) {
        if ok && elapsed_ms < (HEDGE_RPC_TIMEOUT_MS / 2) {
            let cur = self.chunk_target.load(Ordering::Relaxed);
            self.chunk_target.store((cur + 8).min(128), Ordering::Relaxed);
        } else if !ok || elapsed_ms > HEDGE_RPC_TIMEOUT_MS {
            let cur = self.chunk_target.load(Ordering::Relaxed);
            self.chunk_target.store((cur.saturating_sub(8)).max(40), Ordering::Relaxed);
        }
    }

    /// === ADD BELOW (helpers): slot-adaptive short TTL (cache freshness) ===
    #[inline(always)]
    fn cache_ttl_ms(&self) -> u64 {
        // Use measured slot length with a safety margin
        let ms = self.fetch_slot_ms().await;
        let ttl = if ms > 60 { ms - 60 } else { 150 };
        ttl.clamp(150, 350)
    }

    /// === ADD BELOW (helpers): prefetch peers' pool states via 1–RTT hedged getMultipleAccounts ===
    async fn prefetch_peer_pool_states(&self, this_pool_addr: &Pubkey, this_pool: &PoolState) {
        let peers = self.index_get_peers(this_pool, this_pool_addr);
        if peers.is_empty() { return; }
        // Keep it light: cap to 48 peers per pass
        let cap = peers.len().min(48);
        let slice = &peers[..cap];
        if let Ok(accs) = self.rpc_get_multiple_accounts(slice).await {
            let now = Self::now_millis();
            for (i, maybe_acc) in accs.into_iter().enumerate() {
                if let Some(acc) = maybe_acc {
                    if acc.data.is_empty() || acc.lamports == 0 { continue; }
                    if let Ok(ps) = self.decode_pool_state(&acc) {
                        self.pool_state_cache.write().insert(slice[i], (now, ps));
                    }
                }
            }
            self.prune_pool_state_cache();
        }
    }

    /// === ADD BELOW (helpers): prefetch adjacent tick-arrays based on heat & spacing ===
    #[inline(always)]
    fn tick_array_index_for_tick(tick: i32, spacing: i32) -> i32 {
        // Normalize to array block in units of real ticks
        let s = spacing.max(1);
        (tick / s).div_euclid(TICK_ARRAY_BLOCK)
    }

    async fn prefetch_adjacent_tick_arrays(
        &self,
        pool_addr: &Pubkey,
        bitmap: &[u8],
        current_tick: i32,
        spacing: i32,
    ) {
        let heat = self.cluster_cache.read().get(pool_addr).map(|a| a.ema_volatility).unwrap_or(0.0);
        let mut n_neg = if heat > 0.85 { 2 } else if heat > 0.55 { 2 } else { 1 };
        let mut n_pos = n_neg;
        let drift = self.tick_drift_sign(pool_addr);
        if drift > 0 { n_pos += 1; }
        if drift < 0 { n_neg += 1; }

        let base_idx = Self::tick_array_index_for_tick(current_tick, spacing);
        let mut addrs: Vec<Pubkey> = Vec::with_capacity((n_neg + n_pos) as usize * 2);

        for d in 1..=n_neg { // behind
            let idx = base_idx - d;
                let bit = (idx as i32).rem_euclid((bitmap.len() * 8) as i32) as usize;
            let byte = bit / 8; let mask = 1 << (bit % 8);
            if byte < bitmap.len() && (bitmap[byte] & mask as u8) != 0 {
                addrs.push(self.derive_tick_array_address(pool_addr, idx));
            }
        }
        for d in 1..=n_pos { // ahead
            let idx = base_idx + d;
            let bit = (idx as i32).rem_euclid((bitmap.len() * 8) as i32) as usize;
            let byte = bit / 8; let mask = 1 << (bit % 8);
            if byte < bitmap.len() && (bitmap[byte] & mask as u8) != 0 {
                addrs.push(self.derive_tick_array_address(pool_addr, idx));
            }
        }
        if addrs.is_empty() { return; }

        let now = Self::now_millis();
        let ttl = self.cache_ttl_ms();
        let to_fetch: Vec<Pubkey> = addrs.into_iter().filter(|k| {
            self.tick_array_cache.read().get(k).map(|(ts, _)| now.saturating_sub(*ts) > ttl/2).unwrap_or(true)
        }).collect();
        if to_fetch.is_empty() { return; }

        if let Ok(accs) = self.rpc_get_multiple_accounts(&to_fetch).await {
            let ts = Self::now_millis();
            for (i, maybe_acc) in accs.into_iter().enumerate() {
                if let Some(acc) = maybe_acc {
                    if acc.data.is_empty() || acc.lamports == 0 { continue; }
                    if let Ok(ticks) = self.parse_tick_array(&acc.data, ts) {
                        self.tick_array_cache.write().insert(to_fetch[i], (ts, Arc::new(ticks)));
                    }
                }
            }
        }
    }

    /// === ADD BELOW (helpers): vol→purge threshold in ticks ===
    #[inline(always)]
    fn shift_purge_threshold(ema_vol: f64) -> i32 {
        // Low vol → tolerate larger shifts (avoid needless purges), high vol → purge earlier
        let min_t = 384i32;   // 6 arrays
        let max_t = 1536i32;  // 24 arrays
        let v = ema_vol.clamp(0.0, 1.0);
        let t = (max_t as f64 - v * (max_t - min_t) as f64).round() as i32;
        t.max(min_t).min(max_t)
    }

    /// === ADD BELOW (helpers): deterministic seed from sig+slot to stabilize jitter across restarts ===
    #[inline(always)]
    async fn det_seed_from_hash_and_slot(&self, h: Hash, slot_phase: f64) -> u64 {
        let mut x = u64::from_le_bytes(h.to_bytes()[..8].try_into().unwrap());
        x ^= ((slot_phase.clamp(0.0, 1.0) * 1e6) as u64).wrapping_mul(0x9E37);
        let leader = self.fetch_slot_leader_ttl().await;
        x ^= Self::pubkey_u64(&leader);
        if let Some(idx) = self.payer_index_for_slot() {
            if let Some(pk) = self.payer_registry.read().get(idx) {
                x ^= Self::pubkey_u64(pk).rotate_left(17);
            }
        }
        x
    }

    /// === ADD BELOW (helpers): bucketing + learning + query ===
    #[inline(always)]
    fn width_bucket_from_op(op: &MevOpportunity) -> u8 {
        let w = (op.tick_range.1 - op.tick_range.0).unsigned_abs().max(1) as u32;
        let mut x = w;
        let mut b = 0u8;
        while x > 1 && b < 63 { x >>= 1; b += 1; }
        b.min(20)
    }

    #[inline(always)]
    fn type_code(t: OpportunityType) -> u8 {
        match t {
            OpportunityType::Arbitrage => 1,
            OpportunityType::CrossPoolArbitrage => 2,
            OpportunityType::Sandwich => 3,
            OpportunityType::BackRun => 4,
            OpportunityType::JitLiquidity => 5,
            OpportunityType::Liquidation => 6,
        }
    }

    /// record after you observe meta.computeUnitsConsumed for a sent tx
    fn note_cu_consumed(&self, op: &MevOpportunity, consumed: u32) {
        let key = (Self::type_code(op.opportunity_type), Self::width_bucket_from_op(op));
        let mut w = self.cu_usage_ewma.write();
        match w.entry(key) {
            Entry::Occupied(mut e) => {
                let (m, n) = *e.get();
                let mm = (1.0 - CU_EWMA_ALPHA) * m + CU_EWMA_ALPHA * (consumed as f64);
                *e.get_mut() = (mm, n.saturating_add(1));
            }
            Entry::Vacant(e) => { e.insert((consumed as f64, 1)); }
        }
    }

    #[inline(always)]
    fn cu_limit_learned(&self, op: &MevOpportunity) -> Option<u32> {
        let key = (Self::type_code(op.opportunity_type), Self::width_bucket_from_op(op));
        self.cu_usage_ewma
            .read()
            .get(&key)
            .map(|(m, _)| ((*m * CU_HEADROOM) as u32).clamp(CU_MIN_LIMIT, CU_MAX_LIMIT))
    }

    /// === ADD BELOW (helpers): fetch current slot leader with TTL ===
    #[inline(always)]
    async fn fetch_slot_leader_ttl(&self) -> Pubkey {
        let now = Self::now_millis();
        {
            let (ts, pk) = *self.leader_cache.read();
            if now.saturating_sub(ts) <= LEADER_TTL_MS && pk != Pubkey::default() { return pk; }
        }
        if let Ok(pk) = self.rpc_client.get_slot_leader().await {
            *self.leader_cache.write() = (now, pk);
            pk
        } else {
            self.leader_cache.read().1
        }
    }

    #[inline(always)]
    fn pubkey_u64(p: &Pubkey) -> u64 {
        let b = p.to_bytes();
        u64::from_le_bytes([b[0],b[1],b[2],b[3],b[4],b[5],b[6],b[7]])
    }

    /// === ADD BELOW (helpers): get/create op guard ===
    #[inline(always)]
    fn op_guard(&self, o: &MevOpportunity, pool_id: Pubkey) -> Arc<tokio::sync::Semaphore> {
        let key = (match o.opportunity_type {
            OpportunityType::Arbitrage => 1u8,
            OpportunityType::Liquidation => 2u8,
            OpportunityType::JitLiquidity => 3u8,
            OpportunityType::BackRun => 4u8,
            OpportunityType::Sandwich => 5u8,
            OpportunityType::CrossPoolArbitrage => 6u8,
            OpportunityType::TriangularArbitrage => 7u8,
        }, pool_id, o.tick_range.0, o.tick_range.1);

        if let Some(s) = self.inflight_op_guards.read().get(&key).cloned() { return s; }
        let s = Arc::new(tokio::sync::Semaphore::new(1));
        self.inflight_op_guards.write().insert(key, s.clone());
        s
    }

    /// === ADD BELOW (helpers): defer rule ===
    #[inline(always)]
    async fn defer_due_to_fee_spike(&self) -> bool {
        let phase = self.slot_phase_0_to_1().await;
        if phase > DEFERRAL_PHASE_MAX { return false; }
        let med = { self.fee_hist_cache.read().1 } as f64;
        if med == 0.0 { return false; }
        let q70 = self.fee_quantile_micro(0.70).await as f64;
        q70 > med * FEE_SPIKE_MULT
    }

    /// === ADD BELOW (helpers): cached PDA derivation for tick arrays ===
    #[inline(always)]
    fn derive_tick_array_address_cached(&self, pool_addr: &Pubkey, array_index: i32) -> Pubkey {
        let key = (*pool_addr, array_index);
        if let Some(pk) = self.tick_array_addr_cache.read().get(&key).cloned() { return pk; }
        let pk = self.derive_tick_array_address(pool_addr, array_index);
        let mut w = self.tick_array_addr_cache.write();
        w.insert(key, pk);
        if w.len() > MAX_TA_ADDR_CACHE {
            let take = w.len() / 8;
            for k in w.keys().take(take).cloned().collect::<Vec<_>>() { w.remove(&k); }
        }
        pk
    }

    /// === ADD BELOW (helpers): scale base micro by observed congestion (IQR/median) ===
    #[inline(always)]
    fn congestion_scaled_micro(&self, micro: u64) -> u64 {
        let (ts_vec, vec_cached) = self.fee_hist_vec_cache.read().clone();
        if vec_cached.len() < 8 { return micro; }
        // compute simple IQR
        let xs = vec_cached;
        let q = |p: f64| -> u64 {
            let idx = ((xs.len() as f64 - 1.0) * p).round() as usize;
            xs[idx]
        };
        let med = q(0.50).max(1);
        let iqr = q(0.75).saturating_sub(q(0.25));
        let ratio = (iqr as f64) / (med as f64);
        let mult = (1.0 + (ratio * 0.20)).clamp(0.90, 1.25); // widen base up to +25% under heavy congestion
        ((micro as f64) * mult) as u64
    }

    /// === ADD BELOW (helpers): record PnL and compute dynamic slot quota ===
    #[inline(always)]
    fn record_pnl_outcome(&self, delta_lamports: i64) {
        let mut w = self.pnl_ewma.write();
        *w = (1.0 - PNL_EWMA_ALPHA) * *w + PNL_EWMA_ALPHA * (delta_lamports as f64);
    }

    #[inline(always)]
    fn max_bundles_per_slot(&self) -> usize {
        let p = *self.pnl_ewma.read();
        let base = if p >= PNL_UP_LAMPORTS as f64 { MAX_BUNDLES_HI }
        else if p <= PNL_DN_LAMPORTS as f64 { MAX_BUNDLES_LO }
                   else { MAX_BUNDLES_PER_SLOT };
        // best-effort async → block_in_place to keep signature
        let tilt = tokio::task::block_in_place(|| futures::executor::block_on(self.quota_tilt()));
        ((base as f64) * tilt).round().clamp(4.0, 32.0) as usize
    }

    /// === ADD BELOW (helpers): side per step + tri scan ===
    #[inline(always)]
    fn side_for_step(pool: &PoolState, from_mint: &Pubkey) -> SwapSide {
        if &pool.token_mint_a == from_mint { SwapSide::Token0In } else { SwapSide::Token1In }
    }

    /// === ADD BELOW (helpers): opportunistic per-endpoint slot EWMA and ranking ===
    #[inline(always)]
    fn rpc_note_slot(&self, idx: usize, slot: u64) {
        let now = Self::now_millis();
        let mut w = self.rpc_slot_cache.write();
        if let Some((ts, ema)) = w.get_mut(idx) {
            let s = slot as f64;
            *ema = if *ts == 0 { s } else { 0.7 * *ema + 0.3 * s };
            *ts = now;
        }
    }

    #[inline(always)]
    fn rpc_best_slot(&self) -> f64 {
        self.rpc_slot_cache.read().iter().map(|(_, s)| *s).fold(0.0, f64::max)
    }

    /// === ADD BELOW (helpers): async slot refresh (non-blocking) ===
    fn rpc_refresh_slot_bg(&self, idx: usize) {
        if self.rpc_ring.len() <= idx { return; }
        let cli = self.rpc_ring[idx].clone();
        let this = self.clone();
        let (ts, _) = self.rpc_slot_cache.read().get(idx).cloned().unwrap_or((0, 0.0));
        if Self::now_millis().saturating_sub(ts) <= RPC_SLOT_TTL_MS { return; }
        tokio::spawn(async move {
            if let Ok(s) = tokio::time::timeout(TokioDuration::from_millis(60), cli.get_slot()).await {
                if let Ok(slot) = s { this.rpc_note_slot(idx, slot); }
            }
        });
    }

    /// === ADD BELOW (helpers): price<->sqrt helpers + limit computation ===
    #[inline(always)]
    fn price_to_sqrt_x64(price: f64) -> u128 {
        if !(price.is_finite()) || price <= 0.0 { return 1; }
        let s = price.sqrt();
        let q64 = (s * (1u128 << 64) as f64).clamp(1.0, (u128::MAX as f64) - 1.0);
        q64 as u128
    }

    #[inline(always)]
    fn sqrt_price_limit_for(&self, pool: &PoolState, side: SwapSide, max_rel_move: f64) -> u128 {
        let p0 = self.sqrt_price_to_price(pool.sqrt_price_x64).max(1e-18);
        let move_clamped = max_rel_move.clamp(0.0001, 0.50); // 1bp .. 50%
        let p_tgt = match side {
            SwapSide::Token0In => p0 * (1.0 - move_clamped).max(1e-12),
            SwapSide::Token1In => p0 * (1.0 + move_clamped),
        };
        Self::price_to_sqrt_x64(p_tgt)
    }

    /// === ADD BELOW (helpers): pick rung count from phase & fee spread ===
    #[inline(always)]
    fn ladder_levels(&self) -> usize {
        let phase = self.slot_phase_0_to_1().await;
        let q60 = tokio::task::block_in_place(|| futures::executor::block_on(self.fee_quantile_micro(0.60))) as f64;
        let q90 = tokio::task::block_in_place(|| futures::executor::block_on(self.fee_quantile_micro(0.90))) as f64;
        let spread = if q60 > 0.0 { (q90 - q60) / q60 } else { 0.0 };
        if phase >= 0.80 || spread >= 0.60 { 3 } else { 2 }
    }

    /// === ADD BELOW (helpers): quick simulate (hedged, tight timeout) returning CU ===
    async fn simulate_cu_quick(&self, tx: &Transaction) -> Option<u32> {
        use tokio::time::timeout;
        let cfg = RpcSimulateTransactionConfig {
            sig_verify: false,
            replace_recent_blockhash: true,
            ..Default::default()
        };
        let order = self.rpc_rank_read();
        for (k, &i) in order.iter().enumerate() {
            let cli = self.rpc_ring[i].clone();
            if k > 0 { sleep(TokioDuration::from_millis(self.hedge_stagger_ms() * k as u64)).await; }
            let to_ms = self.rpc_dynamic_timeout_ms(i).min(200);
            if let Ok(Ok(resp)) = timeout(Duration::from_millis(to_ms), cli.simulate_transaction_with_config(tx, cfg.clone())).await {
                self.rpc_refresh_slot_bg(i);
                return resp.value.units_consumed.map(|u| u as u32);
            } else {
                self.rpc_refresh_slot_bg(i);
            }
        }
        None
    }

    /// === ADD BELOW (helpers): quantile-based ladder using live fee histogram ===
    #[inline(always)]
    fn ladder_quantiles_for_phase(phase: f64) -> [f64; 3] {
        // earlier (cheap) → lower quantiles; later (hot) → higher quantiles
        let p = phase.clamp(0.0, 1.0);
        if p < 0.25 { [0.50, 0.60, 0.70] }
        else if p < 0.75 { [0.60, 0.70, 0.80] }
        else { [0.70, 0.80, 0.90] }
    }

    /// === ADD: quantile ladder (use this instead of the older jitter-only ladder) ===
    #[inline(always)]
    fn compute_budget_ixs_ladder_quantile(&self, op: &MevOpportunity, levels: usize) -> Vec<[Instruction; 2]> {
        let lv = levels.clamp(1, 3);
        let mut out = Vec::with_capacity(lv);

        let phase = self.slot_phase_0_to_1().await;
        let qs = Self::ladder_quantiles_for_phase(phase);
        // base from EV model (already quantile-aware inside), used as floor
        let base_from_ev = self.cu_price_micro_for_op(op.expected_profit as u64, op.gas_estimate as u64, phase);
        let median_cached = { self.fee_hist_cache.read().1 };

        for i in 0..lv {
            // anchor to explicit quantile rung
            let q_anchor = tokio::task::block_in_place(|| futures::executor::block_on(self.fee_quantile_micro(qs[i])));
            let mut micro = base_from_ev.max(q_anchor.max(median_cached) as u64);
            micro = self.cap_micro_by_ev(micro, op.expected_profit as u64, op.gas_estimate as u64);
            micro = self.congestion_scaled_micro(micro);
            micro = self.bin_hop_micro(micro);
            micro = ((micro as f64) * self.leader_tip_multiplier()).round() as u64;

            // deterministic jitter using blockhash/phase (see pass #106)
            let rb = { self.blockhash_cache.read().1 };
            let seed = self.det_seed_from_hash_and_slot(rb, phase);
            let jittered = Self::jitter_micro(micro, (op.expected_profit as u64) ^ seed ^ (i as u64 * 0x9E37), Self::unix_time_now().wrapping_add(i as u64 * 11));

            out.push(self.compute_budget_ixs_for_op(op, jittered));
        }
        out
    }

    /// === ADD BELOW (helpers): compute-budget ladder for multi-relay bundles ===
    #[inline(always)]
    fn compute_budget_ixs_ladder(&self, op: &MevOpportunity, levels: usize) -> Vec<[Instruction; 2]> {
        let lv = levels.clamp(1, 3);
        let mut out = Vec::with_capacity(lv);
        let base = self.cu_price_micro_for_op(op.expected_profit, op.gas_estimate as u64, self.slot_phase_0_to_1());
        for i in 0..lv {
            let rb = { self.blockhash_cache.read().1 }; // recent cached blockhash
            let seed = self.det_seed_from_hash_and_slot(rb, self.slot_phase_0_to_1());
            let jittered = Self::jitter_micro(base, (op.expected_profit as u64) ^ seed ^ (i as u64 * 0x9E37), Self::unix_time_now().wrapping_add(i as u64 * 7));
            out.push(self.compute_budget_ixs_for_op(op, jittered));
        }
        out
    }

    /// === ADD BELOW (helpers): tail floor ===
    #[inline(always)]
    fn tail_micro_floor(&self, phase: f64, median_micro: u64) -> u64 {
        if phase >= 0.95 {
            ((median_micro as f64) * 1.20 + 500.0).ceil() as u64
        } else if phase >= 0.90 {
            ((median_micro as f64) * 1.05 + 250.0).ceil() as u64
        } else { 1 }
    }

    /// === ADD BELOW (helpers): cap microCU by EV ===
    #[inline(always)]
    fn cap_micro_by_ev(&self, micro: u64, ev: u64, gas_est: u64) -> u64 {
        if gas_est == 0 { return micro; }
        let ev_per_cu = (ev as f64) / (gas_est as f64);
        let max_reasonable = (ev_per_cu * 0.9 * 1_000_000.0).max(1.0); // 0.9× EV/CU in microLamports
        micro.min(max_reasonable as u64)
    }

    /// === ADD BELOW (helpers): select non-conflicting highest EV-density ops ===
    #[inline(always)]
    fn ev_density(&self, op: &MevOpportunity) -> f64 {
        let c = 1.0 + op.priority_fee as f64 + op.gas_estimate as f64 * 0.2;
        let base = (op.expected_profit as f64) / c;
        let hot = self.pool_hot_penalty(&op.pool_id);
        base * (1.0 - hot)
    }

    /// === ADD BELOW (helpers): regime bucketing + multiplier ===
    #[inline(always)]
    fn bkt(x01: f64) -> u8 { ( (x01.clamp(0.0,1.0) * 4.0).round() as u8 ).min(4) }

    #[inline(always)]
    fn type_code(t: OpportunityType) -> u8 {
        match t {
            OpportunityType::Arbitrage => 1,
            OpportunityType::CrossPoolArbitrage => 2,
            OpportunityType::Sandwich => 3,
            OpportunityType::BackRun => 4,
            OpportunityType::JitLiquidity => 5,
            OpportunityType::Liquidation => 6,
            OpportunityType::TriangularArbitrage => 7,
        }
    }

    #[inline(always)]
    fn execp_calibration_multiplier(&self, typ: OpportunityType, vol: f64, conc: f64) -> f64 {
        let key = (Self::type_code(typ), Self::bkt(vol), Self::bkt(conc));
        let (succ, fail) = self.exec_calib.read().get(&key).cloned().unwrap_or((0, 0));
        let n = succ + fail;
        let mean = (succ as f64 + CALIB_ALPHA0) / ((n as f64) + CALIB_ALPHA0 + CALIB_BETA0);
        // before we have enough samples, keep multiplier mild; later trust the posterior
        let mult = if n < CALIB_MIN_SAMPLES { 1.0 } else { (mean / 0.55).clamp(0.75, 1.25) };
        mult
    }

    /// === ADD: record outcomes (call from executor after you learn if the send filled) ===
    fn record_exec_outcome(&self, typ: OpportunityType, vol: f64, conc: f64, filled: bool) {
        let key = (Self::type_code(typ), Self::bkt(vol), Self::bkt(conc));
        let mut w = self.exec_calib.write();
        match w.entry(key) {
            Entry::Occupied(mut e) => {
                let (s, f) = *e.get();
                if filled { *e.get_mut() = (s.saturating_add(1), f); }
                else { *e.get_mut() = (s, f.saturating_add(1)); }
            }
            Entry::Vacant(e) => {
                if filled { e.insert((1, 0)); } else { e.insert((0, 1)); }
            }
        }
    }

    fn select_non_conflicting_ops(&self, mut ops: Vec<MevOpportunity>, k: usize) -> Vec<MevOpportunity> {
        // Sort by EV-density desc (stable by lower cost when equal)
        ops.sort_unstable_by(|a, b| {
            let da = self.ev_density(a);
            let db = self.ev_density(b);
            match db.partial_cmp(&da).unwrap() {
                core::cmp::Ordering::Equal => {
                    let ca = 1.0 + a.priority_fee as f64 + a.gas_estimate as f64 * 0.2;
                    let cb = 1.0 + b.priority_fee as f64 + b.gas_estimate as f64 * 0.2;
                    (ca as i128).cmp(&(cb as i128))
                }
                ord => ord,
            }
        });

        let mut picked: Vec<MevOpportunity> = Vec::with_capacity(k.min(ops.len()));
        'outer: for o in ops.into_iter() {
            for p in &picked {
                if Self::opps_conflict(&o, p) { continue 'outer; }
            }
            picked.push(o);
            if picked.len() >= k { break; }
        }
        picked
    }

    /// === ADD BELOW (helpers): hash ticks deterministically (fnv over (idx, sqrt, lnet)) ===
    #[inline(always)]
    fn hash_ticks(ticks: &[TickData]) -> u64 {
        let mut h: u64 = 0xcbf29ce484222325u64;
        for t in ticks {
            // tick_index
            h ^= (t.tick_index as i64 as u64);
            h = h.wrapping_mul(0x100000001b3u64);
            // sqrt_price_x64
            h ^= t.sqrt_price_x64 as u64;
            h = h.wrapping_mul(0x100000001b3u64);
            // liquidity_net (signed -> u64)
            h ^= (t.liquidity_net as i64 as u64);
            h = h.wrapping_mul(0x100000001b3u64);
        }
        h
    }

    /// === ADD BELOW (helpers): slot quota check/reset using measured slot length ===
    #[inline(always)]
    async fn try_consume_slot_quota(&self, n: usize) -> bool {
        let ms = self.fetch_slot_ms().await;
        let slot_epoch = Self::now_millis() / ms.max(200);
        let mut w = self.slot_quota.write();
        if w.0 != slot_epoch {
            *w = (slot_epoch, 0);
        }
        if w.1 + n <= MAX_BUNDLES_PER_SLOT {
            w.1 += n;
            true
        } else {
            false
        }
    }

    /// === ADD BELOW (helpers): update & rank RPC endpoints by EWMA ===
    #[inline(always)]
    fn rpc_update_stats(&self, idx: usize, elapsed_ms: u64, ok: bool) {
        let mut w = self.rpc_stats.write();
        if let Some((ema_ms, ema_err)) = w.get_mut(idx) {
            let ms = elapsed_ms as f64;
            *ema_ms = (1.0 - RPC_EWMA_ALPHA) * *ema_ms + RPC_EWMA_ALPHA * ms;
            let err_val = if ok { 0.0 } else { 1.0 };
            *ema_err = (1.0 - RPC_EWMA_ALPHA) * *ema_err + RPC_EWMA_ALPHA * err_val;
        }
    }

    #[inline(always)]
    fn rpc_rank(&self) -> Vec<usize> {
        let stats = self.rpc_stats.read().clone();
        let slots = self.rpc_slot_cache.read().clone();
        let incl  = self.rpc_inclusion.read().clone();
        let best  = self.rpc_best_slot();
        let mut idxs: Vec<usize> = (0..stats.len()).collect();
        idxs.sort_unstable_by(|&i, &j| {
            let (mi, ei) = stats[i]; let (_, si) = slots.get(i).cloned().unwrap_or((0, 0.0)); let pi = (best - si).max(0.0);
            let (mj, ej) = stats[j]; let (_, sj) = slots.get(j).cloned().unwrap_or((0, 0.0)); let pj = (best - sj).max(0.0);
            let ii = *incl.get(i).unwrap_or(&0.6);
            let ij = *incl.get(j).unwrap_or(&0.6);
            let ci = mi + 600.0 * ei + RPC_STALE_PENALTY * pi + RPC_INCL_PENALTY * (0.8 - ii).max(0.0) + self.rpc_cb_penalty(i);
            let cj = mj + 600.0 * ej + RPC_STALE_PENALTY * pj + RPC_INCL_PENALTY * (0.8 - ij).max(0.0) + self.rpc_cb_penalty(j);
            ci.partial_cmp(&cj).unwrap()
        });
        idxs
    }

    /// === ADD BELOW (helpers): quantile microprice (0..1) from cached histogram ===
    #[inline(always)]
    async fn fee_quantile_micro(&self, q: f64) -> u64 {
        let now = Self::now_millis();
        let (ts, vec_cached) = self.fee_hist_vec_cache.read().clone();
        if now.saturating_sub(ts) <= self.fee_hist_ttl_ms() && !vec_cached.is_empty() {
            let xs = vec_cached;
            let idx = ((xs.len() as f64 - 1.0) * q.clamp(0.0, 1.0)).round() as usize;
            return xs[idx];
        }

        // Fetch once (prefer primary; hedge on ring if fail)
        let fetch = |cli: Arc<RpcClient>| async move { cli.get_recent_prioritization_fees().await };
        let mut xs: Vec<u64> = Vec::new();
        if let Ok(v) = fetch(self.rpc_client.clone()).await {
            xs = v.into_iter().map(|f| f.prioritization_fee).collect();
        } else {
            for c in self.rpc_ring.iter().skip(1) {
                if let Ok(v) = fetch(c.clone()).await {
                    xs = v.into_iter().map(|f| f.prioritization_fee).collect();
                    break;
                }
            }
        }
        if xs.is_empty() { return 0; }
        xs.sort_unstable();
        let arc = Arc::new(xs);
        *self.fee_hist_vec_cache.write() = (now, arc.clone());
        let idx = ((arc.len() as f64 - 1.0) * q.clamp(0.0, 1.0)).round() as usize;
        arc[idx]
    }

    /// === ADD BELOW (helpers): per-op compute unit estimator ===
    #[inline(always)]
    fn gas_limit_for_op(&self, op: &MevOpportunity) -> u32 {
        if let Some(lrn) = self.cu_limit_learned(op) { return lrn; }

        // Heuristic fallback (kept from pass #100)
        let base = match op.opportunity_type {
            OpportunityType::Arbitrage => 190_000u32,
            OpportunityType::CrossPoolArbitrage => 320_000u32,
            OpportunityType::Sandwich => 480_000u32,
            OpportunityType::BackRun => 230_000u32,
            OpportunityType::JitLiquidity => 260_000u32,
            OpportunityType::Liquidation => 300_000u32,
        };
        let width = (op.tick_range.1 - op.tick_range.0).unsigned_abs().max(1) as u32;
        let width_term = (width / 8).min(3) * 20_000;
        let risk_term = (op.risk_score * 60_000.0) as u32;
        base.saturating_add(width_term).saturating_add(risk_term).clamp(CU_MIN_LIMIT, CU_MAX_LIMIT)
    }

    /// === ADD BELOW (helpers): enumerate tick-array PDAs from bitmap ===
    #[inline(always)]
    fn enumerate_tick_array_addresses(&self, pool_address: &Pubkey, bitmap: &[u8]) -> Vec<Pubkey> {
        let ttl = self.cache_ttl_ms();
        let now = Self::now_millis();
        let h = Self::fnv1a64(bitmap);
        // Fast-path: cached list is fresh
        if let Some((ts, v)) = self.bitmap_addr_cache.read().get(&(*pool_address, h)).cloned() {
            if now.saturating_sub(ts) <= ttl { return (*v).clone(); }
        }

        // Compute
        let mut addrs: Vec<Pubkey> = Vec::with_capacity(256);
        let mut idx_base = 0usize;
        for chunk in bitmap.chunks_exact(8) {
            let mut w = 0u64;
            for (i, b) in chunk.iter().enumerate() { w |= (*b as u64) << (i * 8); }
            if w != 0 {
                let mut bits = w;
                while bits != 0 {
                    let tz = bits.trailing_zeros() as usize;
                    let idx = (idx_base + tz) as i32;
                    addrs.push(self.derive_tick_array_address_cached(pool_address, idx));
                    bits &= bits - 1;
                }
            }
            idx_base += 64;
        }
        let rem = bitmap.len() & 7;
        if rem != 0 {
            let start = bitmap.len() - rem;
            let mut w = 0u64;
            for i in 0..rem { w |= (bitmap[start + i] as u64) << (i * 8); }
            if w != 0 {
                let mut bits = w;
                while bits != 0 {
                    let tz = bits.trailing_zeros() as usize;
                    let idx = (idx_base + tz) as i32;
                    addrs.push(self.derive_tick_array_address_cached(pool_address, idx));
                    bits &= bits - 1;
                }
            }
        }

        let arc = Arc::new(addrs.clone());
        let mut w = self.bitmap_addr_cache.write();
        w.insert((*pool_address, h), (now, arc));
        if w.len() > MAX_BITMAP_ADDR_CACHE {
            // age-based trim: drop oldest 1/8
            let take = w.len() / 8;
            let mut items: Vec<_> = w.iter().map(|(k,(ts,_))| (*ts, k.clone())).collect();
            items.sort_unstable_by_key(|x| x.0);
            for (_, k) in items.into_iter().take(take) { w.remove(&k); }
        }
        addrs
    }

    #[inline(always)]
    fn invalidate_tick_arrays_for(&self, addrs: &[Pubkey]) {
        let mut cache = self.tick_array_cache.write();
        for k in addrs {
            cache.remove(k);
        }
    }

    /// === ADD BELOW (helpers): monotonic ms ===
    #[inline(always)]
    fn now_millis() -> u64 {
        use std::time::{SystemTime, UNIX_EPOCH};
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0)
    }

    /// === ADD BELOW (helpers): slot-epoch and slot-level throttle ===
    #[inline(always)]
    async fn current_slot_epoch(&self) -> u64 {
        let ms = self.fetch_slot_ms().await;
        (Self::now_millis() / ms.max(200))
    }

    /// === ADD BELOW (helpers): try_consume_slot_quota with dynamic limit ===
    #[inline(always)]
    async fn try_consume_slot_quota(&self, n: usize) -> bool {
        let ms = self.fetch_slot_ms().await;
        let slot_epoch = Self::now_millis() / ms.max(200);
        let mut w = self.slot_quota.write();
        if w.0 != slot_epoch { *w = (slot_epoch, 0); }
        let limit = self.max_bundles_per_slot();
        if w.1 + n <= limit {
            w.1 += n;
            true
        } else {
            false
        }
    }

    #[inline(always)]
    async fn slot_throttle_insert(&self, key: (u8, Pubkey, i32, i32)) -> bool {
        let epoch = self.current_slot_epoch().await;
        let mut w = self.slot_seen.write();
        if w.0 != epoch { *w = (epoch, HashSet::new()); }
        if w.1.contains(&key) { false } else { w.1.insert(key); true }
    }

    /// === ADD BELOW (helpers): send-or-skip decision ===
    #[inline(always)]
    fn should_send_now(&self, op: &MevOpportunity) -> bool {
        // Early slot: require strong EV-density; Late slot: allow moderate (tail inclusion is king)
        let phase = self.slot_phase_0_to_1().await;
        let c = 1.0 + op.priority_fee as f64 + op.gas_estimate as f64 * 0.2;
        let d = (op.expected_profit as f64) / c; // EV-density proxy

        if phase < 0.15 { d >= 6_000.0 }       // very early: only the best ideas
        else if phase < 0.50 { d >= 4_000.0 }  // early-mid
        else if phase < 0.85 { d >= 3_000.0 }  // mid-late
        else { d >= 2_000.0 }                  // tail: relax barrier — inclusion wins
    }

    /// === ADD BELOW (helpers): compact hash for a tie-breaker ===
    #[inline(always)]
    fn opp_tiebreak_hash(o: &MevOpportunity) -> u64 {
        let mut h = 0xcbf29ce484222325u64;
        h ^= (o.tick_range.0 as i64 as u64); h = h.wrapping_mul(0x100000001b3u64);
        h ^= (o.tick_range.1 as i64 as u64); h = h.wrapping_mul(0x100000001b3u64);
        h ^= (o.expected_profit as u64);      h = h.wrapping_mul(0x100000001b3u64);
        h
    }

    #[inline(always)]
    fn unix_time_now() -> u64 {
        use std::time::{SystemTime, UNIX_EPOCH};
        SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_secs()).unwrap_or(0)
    }


    fn generate_cluster_id(&self) -> u64 {
        use std::sync::atomic::{AtomicU64, Ordering};
        static CLUSTER_SEQ: AtomicU64 = AtomicU64::new(1);
        (Self::unix_time_now() << 20) ^ CLUSTER_SEQ.fetch_add(1, Ordering::Relaxed)
    }

    pub async fn update_price_oracle(&self, token_mint: &Pubkey, price: u64) {
        self.price_oracle.write().insert(*token_mint, price);
    }

    pub fn get_cached_analysis(&self, pool_address: &Pubkey) -> Option<ClusterAnalysis> {
        self.cluster_cache.read().get(pool_address).cloned()
    }

    pub async fn start_continuous_analysis(&self, pool_addresses: Vec<Pubkey>) -> mpsc::Receiver<ClusterAnalysis> {
        let (tx, rx) = mpsc::channel(256);
        let analyzer = Arc::new(self.clone());
        let addrs = Arc::new(pool_addresses);

        const MAX_CONCURRENCY: usize = 24; // keep your established limit
        const SCAN_FACTOR: usize = 2;      // scan up to 2× concurrency each cycle
        let sem = Arc::new(Semaphore::new(MAX_CONCURRENCY));

        tokio::spawn({
            let tx = tx.clone();
            async move {
                let mut offset: usize = 0;
                loop {
                    // Adaptive cadence (EMA vol) from cache
                    let mut heat_global = 0.0f64;
                    let mut scored: Vec<(f64, Pubkey)> = Vec::with_capacity(addrs.len());
                    {
                        let cache = analyzer.cluster_cache.read();
                        for addr in addrs.iter() {
                            if let Some(a) = cache.get(addr) {
                                heat_global = heat_global.max(a.ema_volatility);
                                let score = a.ema_volatility * 0.7 + a.persistence_score * 0.3;
                                scored.push((score, *addr));
                            } else {
                                // cold pools get baseline to avoid starvation
                                scored.push((0.15, *addr));
                            }
                        }
                    }
                    let ms = (150.0 - 110.0 * heat_global).clamp(40.0, 150.0) as u64;
                    tokio::time::sleep(TokioDuration::from_millis(ms)).await;

                    // Sort by score desc, then rotate start to avoid fixed ordering
                    scored.sort_unstable_by(|a, b| b.0.partial_cmp(&a.0).unwrap());
                    let n = scored.len().max(1);
                    offset = (offset + 1) % n;

                    let top_k = (MAX_CONCURRENCY * SCAN_FACTOR).min(n);
                    let mut futs = FuturesUnordered::new();
                    for i in 0..top_k {
                        let idx = (i + offset) % n;
                        let addr = scored[idx].1;
                        let a = analyzer.clone();
                        let txc = tx.clone();
                        let semc = sem.clone();

                        futs.push(async move {
                            let _permit = semc.acquire_owned().await.expect("semaphore");
                            if let Ok(an) = a.analyze_pool_ticks(&addr).await {
                                let _ = txc.send(an).await;
                            }
                        });
                    }
                    while futs.next().await.is_some() {}
                }
            }
        });

        rx
    }

    /// === ADD BELOW (helpers): fetch ALT with TTL + greedy chooser ===
    #[inline(always)]
    async fn fetch_alt_ttl(&self, lut: &Pubkey) -> Option<AddressLookupTableAccount> {
        let now = Self::now_millis();
        if let Some(ts) = self.alt_negative.read().get(lut).cloned() {
            if now.saturating_sub(ts) <= LUT_NEG_TTL_MS { return None; }
        }
        if let Some((ts, a)) = self.alt_cache.read().get(lut).cloned() {
            if now.saturating_sub(ts) <= LUT_TTL_MS { return Some(a); }
        }
        // try primary; fall back to ring
        let fetch = |cli: Arc<RpcClient>, key: Pubkey| async move { cli.get_address_lookup_table(&key).await };
        if let Ok(resp) = fetch(self.rpc_client.clone(), *lut).await {
            if let Some(RpcAddressLookupTableAccount { account, .. }) = resp.value {
                {
                    let mut w = self.alt_cache.write();
                    w.insert(*lut, (now, account.clone()));
                    if w.len() > MAX_LUT_CACHE {
                        let take = w.len() / 8;
                        let mut items: Vec<_> = w.iter().map(|(k,(ts,_))| (*ts, *k)).collect();
                        items.sort_unstable_by_key(|x| x.0);
                        for (_, k) in items.into_iter().take(take) { w.remove(&k); }
                    }
                }
                return Some(account);
            } else {
                self.alt_negative.write().insert(*lut, now);
                return None;
            }
        }
        for c in self.rpc_ring.iter().skip(1) {
            if let Ok(resp) = fetch(c.clone(), *lut).await {
                if let Some(RpcAddressLookupTableAccount { account, .. }) = resp.value {
                    self.alt_cache.write().insert(*lut, (now, account.clone()));
                    return Some(account);
                } else {
                    self.alt_negative.write().insert(*lut, now);
                    return None;
                }
            }
        }
        None
    }

    #[inline(always)]
    fn coverage_score(lut: &AddressLookupTableAccount, needed: &HashSet<Pubkey>) -> usize {
        lut.addresses.iter().filter(|a| needed.contains(a)).count()
    }

    async fn choose_alts_for_accounts(
        &self,
        needed_accounts: &[Pubkey],
    ) -> Vec<AddressLookupTableAccount> {
        let mut needed: HashSet<Pubkey> = needed_accounts.iter().cloned().collect();
        let luts = self.alt_registry.read().clone();
        let mut cands: Vec<(usize, AddressLookupTableAccount)> = Vec::with_capacity(luts.len());

        // fetch all ALTs (TTL) and score - skip recently-useless LUTs before fetching
        for lut_pk in luts.iter().take(8) {
            if let Some(ts) = self.alt_useless.read().get(lut_pk).cloned() {
                if Self::now_millis().saturating_sub(ts) <= LUT_USELESS_TTL_MS { continue; }
            }
            if let Some(lut) = self.fetch_alt_ttl(lut_pk).await {
                let sc = Self::coverage_score(&lut, &needed);
                if sc > 0 { cands.push((sc, lut)); } else {
                    self.alt_useless.write().insert(*lut_pk, Self::now_millis());
                }
            }
        }
        // greedy pick up to MAX_LUTS_PER_TX
        cands.sort_unstable_by(|a,b| b.0.cmp(&a.0));
        let mut chosen: Vec<AddressLookupTableAccount> = Vec::new();
        for (_, lut) in cands.into_iter() {
            // recompute marginal gain
            let gain = lut.addresses.iter().filter(|a| needed.contains(a)).count();
            if gain == 0 {
                self.alt_useless.write().insert(lut.key, Self::now_millis());
                continue;
            }
            chosen.push(lut.clone());
            for a in lut.addresses.iter() { needed.remove(a); }
            if chosen.len() >= MAX_LUTS_PER_TX { break; }
            if needed.is_empty() { break; }
        }
        chosen
    }

    /// === ADD BELOW (helpers): builders with/without ALTs + packet-size check ===
    #[inline(always)]
    fn approx_tx_size(versioned: &VersionedTransaction) -> usize {
        // approximate: signatures (64B each) + 1B header + message bytes
        let sigs = versioned.signatures.len() * 64;
        let msg_len = match &versioned.message {
            VersionedMessage::Legacy(m) => m.serialize().len(), // serde impl in sdk
            VersionedMessage::V0(m)     => m.serialize().len(),
        };
        1 + sigs + msg_len
    }

    #[inline(always)]
    fn build_legacy_tx(
        &self,
        payer: &Pubkey,
        ixs: &[Instruction],
        recent_blockhash: Hash,
        signers: &[&dyn Signer],
    ) -> VersionedTransaction {
        let msg = Message::new_with_blockhash(ixs, Some(payer), &recent_blockhash);
        let tx = Transaction::new(signers, msg, recent_blockhash);
        VersionedTransaction::from(tx)
    }

    async fn build_v0_tx_with_alts(
        &self,
        payer: &Pubkey,
        ixs: &[Instruction],
        recent_blockhash: Hash,
        extra_accounts_for_coverage: &[Pubkey],
        signers: &[&dyn Signer],
    ) -> VersionedTransaction {
        // Collect metas for greedy LUT selection (program ids + metas from ixs)
        let mut needed: HashSet<Pubkey> = ixs.iter().flat_map(|ix| {
            let mut v = Vec::with_capacity(ix.accounts.len()+1);
            v.push(ix.program_id);
            v.extend(ix.accounts.iter().map(|m| m.pubkey));
            v
        }).collect();
        for a in extra_accounts_for_coverage { needed.insert(*a); }

        let alts = self.choose_alts_for_accounts(&needed.into_iter().collect::<Vec<_>>()).await;

        let v0msg = V0Message::try_compile(
            payer,
            ixs,
            &recent_blockhash,
            &alts.iter().collect::<Vec<_>>()[..],
        ).expect("v0 compile");
        let tx = VersionedTransaction::try_new(VersionedMessage::V0(v0msg), signers)
            .expect("sign v0");
        tx
    }

    /// === ADD BELOW (helpers): smart builder choosing the smaller packet ===
    async fn build_smart_tx(
        &self,
        payer: &Pubkey,
        ixs: &[Instruction],
        recent_blockhash: Hash,
        extra_accounts_for_coverage: &[Pubkey],
        signers: &[&dyn Signer],
    ) -> VersionedTransaction {
        let v0 = self.build_v0_tx_with_alts(payer, ixs, recent_blockhash, extra_accounts_for_coverage, signers).await;
        let leg = self.build_legacy_tx(payer, ixs, recent_blockhash, signers);
        let s0 = Self::approx_tx_size(&v0);
        let s1 = Self::approx_tx_size(&leg);
        // Prefer the one that fits with headroom; else pick the smaller
        let lim = PACKET_DATA_SIZE.saturating_sub(PACKET_HEADROOM);
        if (s0 <= lim && s0 <= s1) || (s0 <= lim && s1 > lim) { v0 } else { leg }
    }

    /// === ADD BELOW (helpers): canonicalize metas in-place before building the message ===
    #[inline(always)]
    fn canonicalize_ixs(ixs: &mut Vec<Instruction>) {
        for ix in ixs.iter_mut() {
            // stable-dedupe by pubkey, keep the strongest mutability / signer flags
            use std::collections::HashMap;
            let mut map: HashMap<Pubkey, (bool,bool)> = HashMap::with_capacity(ix.accounts.len());
            for m in ix.accounts.iter() {
                map.entry(m.pubkey).and_modify(|e| {
                    e.0 |= m.is_signer;
                    e.1 |= m.is_writable;
                }).or_insert((m.is_signer, m.is_writable));
            }
            let mut metas: Vec<AccountMeta> = map.into_iter().map(|(k,(s,w))| {
                if w { AccountMeta::new(k, s) } else { AccountMeta::new_readonly(k, s) }
            }).collect();
            metas.sort_unstable_by_key(|m| ( !m.is_signer, !m.is_writable, m.pubkey )); // signers & writables first
            ix.accounts = metas;
        }
    }

    /// === ADD BELOW (helpers): detect upgrade & purge affected pool/tick caches ===
    async fn detect_program_upgrade_and_purge(&self, program_id: &Pubkey) {
        let now = Self::now_millis();
        if let Some((ts, _)) = self.program_hash_cache.read().get(program_id).cloned() {
            if now.saturating_sub(ts) <= PROG_HASH_TTL_MS { return; }
        }
        if let Ok(acc) = self.rpc_client.get_account(program_id).await {
            let h = Self::fnv1a64(&acc.data);
            let mut purge = false;
            {
                let mut w = self.program_hash_cache.write();
                purge = match w.get(program_id) {
                    Some((_, prev)) if *prev != h => true,
                    None => false,
                    _ => false,
                };
                w.insert(*program_id, (now, h));
            }
            if purge {
                // drop all tick-array caches and pool-state of pools (assume they're all affected by program upgrade)
                // invalidate tick arrays
                let mut ta = self.tick_array_cache.write();
                let keys: Vec<Pubkey> = ta.keys().cloned().collect();
                for k in keys {
                    ta.remove(&k);
                }
                // drop pool states (they'll re-warm)
                let mut ps = self.pool_state_cache.write();
                ps.clear();
            }
        }
    }

    /// === ADD BELOW (helpers): one-shot retry on blockhash expiry ===
    async fn try_broadcast_with_blockhash_retry<F>(
        &self,
        payer: &Pubkey,
        mut make_ixs: F,
        extra_accounts_for_coverage: &[Pubkey],
        signers: &[&dyn Signer],
    ) -> Result<Signature, Box<dyn std::error::Error>>
    where
        F: FnMut() -> Vec<Instruction>,
    {
        // attempt #1
        let rb1 = self.fresh_blockhash().await;
        let mut ixs = make_ixs();
        Self::canonicalize_ixs(&mut ixs);
        let mut tx = self.build_smart_tx(payer, &ixs, rb1, extra_accounts_for_coverage, signers).await;
        match self.broadcast_versioned_transaction_hedged(&tx.clone()).await {
            Ok(sig) => return Ok(sig),
            Err(e) => {
                let msg = format!("{:?}", e);
                if msg.contains("blockhash not found") || msg.contains("BlockhashNotFound") {
                    // refresh + rebuild + resend
                    let rb2 = self.fresh_blockhash().await;
                    let ixs2 = make_ixs();
                    let mut ixs2c = ixs2;
                    Self::canonicalize_ixs(&mut ixs2c);
                    tx = self.build_smart_tx(payer, &ixs2c, rb2, extra_accounts_for_coverage, signers).await;
                    return Ok(self.broadcast_versioned_transaction_hedged(&tx).await?);
                } else {
                    return Err(e);
                }
            }
        }
    }

    /// === ADD BELOW (helpers): cooldown check ===
    #[inline(always)]
    fn pool_cooldown_allows(&self, pool: &Pubkey) -> bool {
        let now = Self::now_millis();
        let mut w = self.pool_last_send.write();
        match w.get(pool).cloned() {
            Some(ts) if now.saturating_sub(ts) < POOL_COOLDOWN_MS => false,
            _ => { w.insert(*pool, now); true }
        }
    }

    /// === ADD BELOW (helpers): deterministic permutation of ring order per ladder rung ===
    #[inline(always)]
    fn permuted_rpc_order(&self, rung: usize) -> Vec<usize> {
        let mut order = self.rpc_rank();
        if order.len() <= 1 { return order; }
        // simple Feistel-like mix with rung as key
        let k = (rung as u64).wrapping_mul(0x9E3779B185EBCA87);
        order.rotate_left((k as usize) % order.len());
        order
    }

    /// === REPLACE: price_to_sqrt_x64(...) with exact Q64.64 mapping ===
    #[inline(always)]
    fn price_to_sqrt_x64(price: f64) -> u128 {
        if !price.is_finite() || price <= 0.0 { return 1; }
        let s = price.sqrt();
        let scaled = s * (1u128 << 64) as f64;
        let v = scaled.floor().clamp(1.0, (u128::MAX - 1) as f64);
        v as u128
    }

    /// === ADD: flashloan fee adjustment for EV calculation ===
    #[inline(always)]
    fn net_after_flash(
        &self,
        net_after_pools: u64,   // from detector
        notional_quote: u64,    // humanized notional in quote units
        flash_bps: u64,         // e.g., 30 = 0.30%
        fixed_fee_lamports: u64 // bridges/keepers if any
    ) -> i128 {
        let flash = (notional_quote as u128)
            .saturating_mul(flash_bps as u128)
            .saturating_div(10_000u128) as u64;
        (net_after_pools as i128)
            - (flash as i128)
            - (fixed_fee_lamports as i128)
    }

    /// === ADD BELOW (helpers): note send origin, record inclusion result, clean old ===
    #[inline(always)]
    fn note_send_origin(&self, sig: &Signature, idx: usize) {
        let leader = tokio::task::block_in_place(|| futures::executor::block_on(self.fetch_slot_leader_ttl()));
        self.send_origin.write().insert(*sig, (Self::now_millis(), idx, leader));
    }

    #[inline(always)]
    pub fn record_sig_inclusion(&self, sig: &Signature, landed: bool) {
        if let Some((ts, idx, leader, micro)) = self.send_origin.write().remove(sig) {
            if Self::now_millis().saturating_sub(ts) <= SEND_ORIGIN_TTL_MS {
                // endpoint inclusion
                let mut w = self.rpc_inclusion.write();
                if let Some(x) = w.get_mut(idx) {
                    let y = if landed { 1.0 } else { 0.0 };
                    *x = (1.0 - RPC_INCL_ALPHA) * *x + RPC_INCL_ALPHA * y;
                }
                // leader inclusion
                let mut lw = self.leader_inclusion.write();
                let e = lw.entry(leader).or_insert(0.6);
                let y = if landed { 1.0 } else { 0.0 };
                *e = (1.0 - LEADER_QUAL_ALPHA) * *e + LEADER_QUAL_ALPHA * y;
                // update hint on landed
                if landed {
                    self.note_leader_phase_hint(&leader, micro);
                }
                // update micro-bin inclusion tracking
                self.note_micro_bin_result(micro, landed);
                // maintain land streak
                self.note_land_streak(landed);
            }
        }
        // cleanup
        let mut w = self.send_origin.write();
        let keys: Vec<Signature> = w.iter()
            .filter_map(|(k,(t,_,_,_))| if Self::now_millis().saturating_sub(*t) > SEND_ORIGIN_TTL_MS { Some(*k) } else { None })
            .collect();
        for k in keys { w.remove(&k); }
    }

    /// === ADD BELOW (helpers): canonicalize ComputeBudget Ixs across the tx ===
    #[inline(always)]
    fn canonicalize_compute_budget(&self, ixs: &mut Vec<Instruction>) {
        use solana_sdk::compute_budget::id as compute_budget_program;
        use solana_sdk::compute_budget::ComputeBudgetInstruction;

        let mut last_limit: Option<u32> = None;
        let mut last_price: Option<u64> = None;
        // collect & strip all CB ixs
        ixs.retain(|ix| {
            if ix.program_id == compute_budget_program() {
                // decode by data prefix (opcode). We only need last values.
                if let Some((&tag, rest)) = ix.data.split_first() {
                    match tag {
                        0 => { // RequestUnitsDeprecated — ignore
                        }
                        1 => { // RequestHeapFrame
                        }
                        2 => { // SetComputeUnitLimit
                            if rest.len() >= 4 {
                                last_limit = Some(u32::from_le_bytes([rest[0],rest[1],rest[2],rest[3]]));
                            }
                        }
                        3 => { // SetComputeUnitPrice
                            if rest.len() >= 8 {
                                last_price = Some(u64::from_le_bytes([rest[0],rest[1],rest[2],rest[3],rest[4],rest[5],rest[6],rest[7]]));
                            }
                        }
                        _ => {}
                    }
                }
                false // strip
            } else { true }
        });
        // re-insert exactly two in front (if present)
        let mut head: Vec<Instruction> = Vec::with_capacity(2);
        if let Some(lim) = last_limit {
            head.push(ComputeBudgetInstruction::set_compute_unit_limit(lim));
        }
        if let Some(price) = last_price {
            head.push(ComputeBudgetInstruction::set_compute_unit_price(price));
        }
        if !head.is_empty() {
            let mut out = head;
            out.extend(ixs.drain(..));
            *ixs = out;
        }
    }

    /// === ADD BELOW (helpers): monotonic rung envelope — enforce strictly ascending micro across rungs ===
    #[inline(always)]
    fn compute_budget_ixs_ladder_quantile(&self, op: &MevOpportunity, levels: usize) -> Vec<[Instruction; 2]> {
        let lv = levels.clamp(1, 3);
        let mut out = Vec::with_capacity(lv);

        let phase = self.slot_phase_0_to_1();
        let qs = self.ladder_quantiles_for_phase(phase);
        let base_from_ev = self.cu_price_micro_for_op(op.expected_profit as u64, op.gas_estimate as u64, phase);
        let opt_m = self.micro_optimal_from_hist(op.expected_profit as u64, op.gas_estimate as u64);
        let base_from_ev = base_from_ev.max(opt_m);
        let median_cached = { self.fee_hist_cache.read().1 };

        let mut micros: Vec<u64> = Vec::with_capacity(lv);
        for i in 0..lv {
            let q_anchor = tokio::task::block_in_place(|| futures::executor::block_on(self.fee_quantile_micro(qs[i])));
            let mut micro = base_from_ev.max(q_anchor.max(median_cached) as u64);
            micro = self.cap_micro_by_ev(micro, op.expected_profit as u64, op.gas_estimate as u64);
            micro = self.congestion_scaled_micro(micro);
            micro = self.bin_hop_micro(micro);
            micro = self.sparse_bin_hop_micro(micro);
            // warm start near last landed; keep EV cap
            if let Some(h) = self.leader_phase_hint_micro() {
                micro = micro.max(h.saturating_sub(1));
            }
            micro = ((micro as f64) * self.leader_tip_multiplier()).round() as u64;
            // apply cost guard taper after consecutive lands
            micro = self.micro_taper_from_streak(micro);
            // choose best of hazard vs learned bin
            let m_hist = self.micro_optimal_from_hist(op.expected_profit as u64, op.gas_estimate as u64);
            let m_learn = self.micro_bin_best_neighbor(micro);
            micro = self.choose_micro_best_of_two(m_hist, m_learn, op.expected_profit as u64, op.gas_estimate as u64);

            // deterministic jitter
            let rb = { self.blockhash_cache.read().1 };
            let seed = tokio::task::block_in_place(|| futures::executor::block_on(self.det_seed_from_hash_and_slot(rb, phase)));
            let jittered = Self::jitter_micro(micro, (op.expected_profit as u64) ^ seed ^ (i as u64 * 0x9E37), Self::unix_time_now().wrapping_add(i as u64 * 11));
            micros.push(jittered);
        }

        // enforce strictly increasing ladder
        for i in 1..micros.len() {
            if micros[i] <= micros[i-1] { micros[i] = micros[i-1].saturating_add(1); }
        }

        for (ri, mut m) in micros.into_iter().enumerate() {
            m = self.reserve_unique_micro(m);
            out.push(self.compute_budget_ixs_for_op_with_limit(self.cu_limit_for_rung(op, ri), m));
        }
        out
    }

    /// === ADD BELOW (helpers): nudge micro price to next non-crowded bin from fee histogram ===
    #[inline(always)]
    fn bin_hop_micro(&self, micro: u64) -> u64 {
        let (_, vec_cached) = self.fee_hist_vec_cache.read().clone();
        if vec_cached.is_empty() { return micro; }
        // Find the smallest observed fee strictly greater than our base; hop to +1 over it.
        for &q in vec_cached.iter() {
            if q > micro { return q.saturating_add(1); }
        }
        micro
    }

    /// === ADD BELOW (helpers): multiplier from leader quality ===
    #[inline(always)]
    fn leader_tip_multiplier(&self) -> f64 {
        let leader = tokio::task::block_in_place(|| futures::executor::block_on(self.fetch_slot_leader_ttl()));
        let q = *self.leader_inclusion.read().get(&leader).unwrap_or(&0.6);
        // Map EWMA q in [0.2..0.95] → multiplier in [LEADER_MULT_MAX..LEADER_MULT_MIN]
        let t = (1.0 - q).clamp(0.0, 0.8);
        (LEADER_MULT_MIN + (LEADER_MULT_MAX - LEADER_MULT_MIN) * t).clamp(LEADER_MULT_MIN, LEADER_MULT_MAX)
    }

    /// === ADD BELOW (helpers): phase-aware stagger ===
    #[inline(always)]
    fn hedge_stagger_ms(&self) -> u64 {
        let p = self.slot_phase_0_to_1();
        if p >= 0.90 { 10 } else if p >= 0.75 { 20 } else { HEDGE_STAGGER_MS }
    }

    /// === ADD BELOW (helpers): distinct ranks ===
    #[inline(always)]
    fn rpc_rank_read(&self) -> Vec<usize> {
        let stats = self.rpc_stats.read().clone();
        let slots = self.rpc_slot_cache.read().clone();
        let best  = self.rpc_best_slot();
        let mut idxs: Vec<usize> = (0..stats.len()).collect();
        idxs.sort_unstable_by(|&i,&j| {
            let (mi, ei) = stats[i]; let (_, si) = slots.get(i).cloned().unwrap_or((0,0.0)); let pi = (best - si).max(0.0);
            let (mj, ej) = stats[j]; let (_, sj) = slots.get(j).cloned().unwrap_or((0,0.0)); let pj = (best - sj).max(0.0);
            let ci = mi + 600.0 * ei + RPC_STALE_PENALTY * pi;
            let cj = mj + 600.0 * ej + RPC_STALE_PENALTY * pj;
            ci.partial_cmp(&cj).unwrap()
        });
        idxs
    }

    #[inline(always)]
    fn rpc_rank_send(&self) -> Vec<usize> { self.rpc_rank() } // your existing rank includes inclusion

    /// === ADD BELOW (helpers): quick processed/fail status ===
    async fn fast_sig_processed(&self, sig: &Signature) -> Option<bool> {
        use tokio::time::timeout;
        let order = self.rpc_rank_read();
        for (k,&i) in order.iter().enumerate() {
            let cli = self.rpc_ring[i].clone();
            if k > 0 { sleep(Duration::from_millis(self.hedge_stagger_ms() * k as u64)).await; }
            if let Ok(Ok(resp)) = timeout(Duration::from_millis(140), cli.get_signature_statuses(&[*sig])).await {
                if let Some(st) = resp.value.first().and_then(|o| o.as_ref()) {
                    if st.err.is_some() { return Some(false); }
                    if st.confirmations.is_some() || st.confirmation_status.is_some() { return Some(true); }
                }
            }
        }
        None
    }

    /// === ADD BELOW (helpers): leader-based send gating ===
    #[inline(always)]
    fn leader_gate_send(&self) -> bool {
        let phase = self.slot_phase_0_to_1();
        if phase >= 0.80 { return true; } // tail always send if otherwise eligible
        let leader = tokio::task::block_in_place(|| futures::executor::block_on(self.fetch_slot_leader_ttl()));
        let q = *self.leader_inclusion.read().get(&leader).unwrap_or(&0.6);
        // If leader fill-rate is poor and early slot, conserve ammo
        if phase < 0.40 && q < 0.45 { return false; }
        true
    }

    /// === ADD BELOW (helpers): quick TTL balance check ===
    #[inline(always)]
    async fn payer_has_balance(&self, payer: &Pubkey) -> bool {
        let bal = match self.rpc_client.get_balance(payer).await { Ok(x) => x, Err(_) => return true };
        bal >= PAYER_MIN_BAL_LAMPORTS
    }

    /// === ADD BELOW (helpers): one-stop ladder builder/sender ===
    async fn build_and_send_ladder(
        &self,
        payer: &Pubkey,
        common_ixs: &[Instruction],
        budget_rungs: &[[Instruction; 2]],
        recent_blockhash: Hash,
        extra_accounts_for_coverage: &[Pubkey],
        signers: &[&dyn Signer],
    ) -> Result<Vec<Signature>, Box<dyn std::error::Error>> {
        let mut sigs = Vec::with_capacity(budget_rungs.len());
        for (rung_idx, cb) in budget_rungs.iter().enumerate() {
            // compose ix list [CB, CB] + common; canonicalize budgets & metas
            let mut ixs = Vec::with_capacity(common_ixs.len() + 3);
            ixs.push(cb[0].clone());
            ixs.push(cb[1].clone());
            // add heap frame if needed
            if let Some(hx) = self.maybe_heap_frame_ix(match cb[0].data.first() { Some(2) => { // tag=2 SetComputeUnitLimit
                u32::from_le_bytes([cb[0].data[1],cb[0].data[2],cb[0].data[3],cb[0].data[4]])
            } _ => 0 }) {
                ixs.push(hx);
            }
            ixs.extend_from_slice(common_ixs);
            self.canonicalize_compute_budget(&mut ixs);
            Self::canonicalize_ixs(&mut ixs);

            // check blockhash freshness
            if !self.blockhash_fresh_enough() {
                // refresh and rebuild rung's tx with a fresh hash
                let fresh = self.fresh_blockhash().await;
                let tx = self.build_smart_tx(payer, &ixs, fresh, extra_accounts_for_coverage, signers).await;
                let sig = self.broadcast_tpu_first(&VersionedTransaction::from(tx)).await?;
                sigs.push(sig);
                if rung_idx == 0 {
                    tokio::time::sleep(TokioDuration::from_millis(self.rung_wait_ms())).await;
                    if let Some(ok) = self.fast_sig_processed(&sigs[0]).await {
                        self.record_sig_inclusion(&sigs[0], ok);
                        if ok { break; }
                    }
                }
                if rung_idx + 1 < budget_rungs.len() {
                    if self.defer_due_to_fee_spike() { break; }
                    tokio::time::sleep(TokioDuration::from_millis(self.rung_spacing_ms())).await;
                }
                continue;
            }

            let tx = self.build_smart_tx(payer, &ixs, recent_blockhash, extra_accounts_for_coverage, signers).await;
            // permuted order per rung inside broadcast helper
            let sig = self.broadcast_tpu_first(&VersionedTransaction::from(tx)).await?;
            sigs.push(sig);
            // note origin with micro for leader phase hints
            if let Some(micro) = self.extract_micro_from_cb_ix(&cb[1]) {
                self.note_send_origin_with_micro(&sig, 0, micro);
            }
            // subscribe to fast inclusion
            self.sub_signature_fast(sig);

            // short-circuit: if first rung gets processed/fails-fast, avoid burning more rungs
            if rung_idx == 0 {
                tokio::time::sleep(TokioDuration::from_millis(self.rung_wait_ms())).await;
                if let Some(ok) = self.fast_sig_processed(&sigs[0]).await {
                    // record immediately; skip remaining rungs if processed (landed or definitively failed)
                    self.record_sig_inclusion(&sigs[0], ok);
                    if ok { break; } // landed → stop ladder
                }
            }
            if rung_idx + 1 < budget_rungs.len() {
                if self.defer_due_to_fee_spike() { break; } // save ammo when fee spikes mid-send
                // Record pool activity and use pool-aware spacing
                let pool_pubkey = Pubkey::default(); // TODO: extract from common_ixs
                self.pool_last_activity.write().insert(pool_pubkey, Self::now_millis());
                tokio::time::sleep(TokioDuration::from_millis(self.rung_spacing_ms_for_pool(&pool_pubkey))).await;
            }
        }
        Ok(sigs)
    }
}

#[derive(Debug, Clone)]
struct PoolState {
    bump: u8,
    token_mint_a: Pubkey,
    token_mint_b: Pubkey,
    token_vault_a: Pubkey,
    token_vault_b: Pubkey,
    fee_rate: u16,
    protocol_fee_rate: u16,
    liquidity: u128,
    sqrt_price_x64: u128,
    tick_current: i32,
    token_a_amount: u64,
    token_b_amount: u64,
}

impl Clone for TickClusterAnalyzer {
    fn clone(&self) -> Self {
        Self {
            rpc_client: Arc::clone(&self.rpc_client),
            rpc_ring: Arc::clone(&self.rpc_ring),
            tick_history: Arc::clone(&self.tick_history),
            cluster_cache: Arc::clone(&self.cluster_cache),
            price_oracle: Arc::clone(&self.price_oracle),
            liquidity_threshold: self.liquidity_threshold,
            analysis_interval: self.analysis_interval,
            bitmap_cache: Arc::clone(&self.bitmap_cache),
            tick_array_cache: Arc::clone(&self.tick_array_cache),
            inflight_guards: Arc::clone(&self.inflight_guards),
            token_decimals_cache: Arc::clone(&self.token_decimals_cache),
            recent_opps_seen: Arc::clone(&self.recent_opps_seen),
            // NEW:
            slot_ms_cache: Arc::clone(&self.slot_ms_cache),
            pool_state_cache: Arc::clone(&self.pool_state_cache),
            pair_index_cache: Arc::clone(&self.pair_index_cache),
            blockhash_cache: Arc::clone(&self.blockhash_cache),
            fee_hist_cache: Arc::clone(&self.fee_hist_cache),
            tick_array_hash_cache: Arc::clone(&self.tick_array_hash_cache),
            chunk_target: Arc::clone(&self.chunk_target),
            pool_ticks_hash_cache: Arc::clone(&self.pool_ticks_hash_cache),
            pool_account_hash_cache: Arc::clone(&self.pool_account_hash_cache),
            slot_quota: Arc::clone(&self.slot_quota),
            rpc_stats: Arc::clone(&self.rpc_stats),
            fee_hist_vec_cache: Arc::clone(&self.fee_hist_vec_cache),
            exec_calib: Arc::clone(&self.exec_calib),
            slot_seen: Arc::clone(&self.slot_seen),
            cu_usage_ewma: Arc::clone(&self.cu_usage_ewma),
            leader_cache: Arc::clone(&self.leader_cache),
            inflight_op_guards: Arc::clone(&self.inflight_op_guards),
            tick_array_addr_cache: Arc::clone(&self.tick_array_addr_cache),
            pnl_ewma: Arc::clone(&self.pnl_ewma),
            rpc_slot_cache: Arc::clone(&self.rpc_slot_cache),
            bitmap_addr_cache: Arc::clone(&self.bitmap_addr_cache),
            // NEW: ALT caches/registry
            alt_cache: Arc::clone(&self.alt_cache),
            alt_registry: Arc::clone(&self.alt_registry),
            // NEW: program hash cache for upgrade detection
            program_hash_cache: Arc::clone(&self.program_hash_cache),
            // NEW: per-pool micro-cooldown
            pool_last_send: Arc::clone(&self.pool_last_send),
            // NEW: RPC inclusion EWMA tracking
            rpc_inclusion: Arc::clone(&self.rpc_inclusion),
            send_origin: Arc::clone(&self.send_origin),
            // NEW: ALT usefulness demotion
            alt_useless: Arc::clone(&self.alt_useless),
            // NEW: leader inclusion model
            leader_inclusion: Arc::clone(&self.leader_inclusion),
            // NEW: ALT negative cache
            alt_negative: Arc::clone(&self.alt_negative),
            
            // NEW: Optimization fields
            pool_slack_ewma: Arc::clone(&self.pool_slack_ewma),
            alt_plan_cache: Arc::clone(&self.alt_plan_cache),
            leader_phase_hint: Arc::clone(&self.leader_phase_hint),
            send_origin: Arc::clone(&self.send_origin),
            
            // NEW: Additional optimization fields
            micro_bin_incl: Arc::clone(&self.micro_bin_incl),
            rpc_inflight: Arc::clone(&self.rpc_inflight),
            micro_slot_registry: Arc::clone(&self.micro_slot_registry),
            land_streak: Arc::clone(&self.land_streak),
            pool_last_activity: Arc::clone(&self.pool_last_activity),
        }
    }
}

mod orca_whirlpool {
    use super::*;
    pub const ID: Pubkey = solana_sdk::pubkey!("whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc");
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_tick_cluster_identification() {
        let analyzer = TickClusterAnalyzer::new("https://api.mainnet-beta.solana.com");
        
        let test_ticks = vec![
            TickData {
                tick_index: 100,
                sqrt_price_x64: 1000000000000000000,
                liquidity_net: 1000000,
                liquidity_gross: 1000000,
                fee_growth_outside_a: 0,
                fee_growth_outside_b: 0,
                timestamp: 1000,
                block_height: 1000,
            },
            TickData {
                tick_index: 102,
                sqrt_price_x64: 1000100000000000000,
                liquidity_net: 2000000,
                liquidity_gross: 2000000,
                fee_growth_outside_a: 100,
                fee_growth_outside_b: 100,
                timestamp: 1001,
                block_height: 1001,
            },
            TickData {
                tick_index: 104,
                sqrt_price_x64: 1000200000000000000,
                liquidity_net: 3000000,
                liquidity_gross: 3000000,
                fee_growth_outside_a: 200,
                fee_growth_outside_b: 200,
                timestamp: 1002,
                block_height: 1002,
            },
        ];
        
        let clusters = analyzer.identify_clusters(&test_ticks);
        assert!(!clusters.is_empty());
    }
    
    #[test]
    fn test_price_conversion() {
        let analyzer = TickClusterAnalyzer::new("https://api.mainnet-beta.solana.com");
        let sqrt_price_x64: u128 = 79228162514264337593543950336;
        let price = analyzer.sqrt_price_to_price(sqrt_price_x64);
        assert!(price > 0.0);
    }
}

