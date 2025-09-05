// competitor_strategy_decoder.rs

#![allow(clippy::too_many_arguments)]
#![allow(clippy::large_enum_variant)]

use std::{
    collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque},
    str::FromStr,
    sync::Arc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use async_trait::async_trait;
use borsh::{BorshDeserialize, BorshSerialize};
use dashmap::{DashMap, DashSet};
use parking_lot::{Mutex, RwLock};
use rayon::prelude::*;
use serde::{Deserialize, Serialize};
use smallvec::SmallVec;
use thiserror::Error;
use tokio::time::{sleep, timeout};
use tracing::{instrument, info_span};

use solana_client::{
    nonblocking::rpc_client::RpcClient,
    rpc_config::{RpcTransactionConfig, RpcProgramAccountsConfig},
    rpc_response::{Response as RpcResponse},
};
use solana_sdk::{
    account::Account,
    address_lookup_table_account::AddressLookupTableAccount,
    commitment_config::CommitmentConfig,
    instruction::CompiledInstruction,
    message::{v0::LoadedAddresses, VersionedMessage},
    pubkey::Pubkey,
    signature::Signature,
    transaction::{SanitizedTransaction, VersionedTransaction},
};
use solana_transaction_status::{
    EncodedConfirmedTransactionWithStatusMeta, EncodedTransaction, UiTransactionEncoding,
    UiInnerInstructions, UiMessage, UiParsedMessage,
    TransactionStatusMeta, UiTokenAmount, UiTokenBalance,
};

// External protocols/constants (extend as needed)
const SERUM_DEX_V3: &str = "9xQeWvG816bUx9EPjHmaT23yvVM2ZWbrrpZb9PusVFin";
const OPENBOOK_V2:  &str = "srmoss1111111111111111111111111111111111111"; // placeholder
const RAYDIUM_AMM_V4: &str = "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8";
const ORCA_WHIRLPOOL: &str = "whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc";
const JUPITER_V4: &str = "JUP4Fb2cqiRUcaTHdrPC8h2gNsA2ETXiPDD33WcGuJB";
const LIFINITY_V2: &str = "2wT8Yq49kHgDzXuPxZSaeLaH1qbmGXtEyPy64bL7aD3c";
const TOKEN_PROGRAM_V2_2022: &str = "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb"; // SPL Token-2022

// Tunables
const RPC_TIMEOUT: Duration = Duration::from_secs(10);
const MAX_RETRIES: u32 = 5;
const STRATEGY_WINDOW: usize = 2000;
const TIMING_PRECISION_MS: u64 = 50;
const GAS_ANALYSIS_PERCENTILES: [f64; 5] = [0.25, 0.5, 0.75, 0.95, 0.99];
const PATTERN_MIN_OCCURRENCES: usize = 3;
const ALT_CACHE_TTL: Duration = Duration::from_secs(3600); // 1 hour

// Errors
#[derive(Error, Debug)]
pub enum DecoderError {
    #[error("RPC error: {0}")]
    Rpc(#[from] solana_client::client_error::ClientError),
    #[error("Serialization error: {0}")]
    Serialization(#[from] bincode::Error),
    #[error("Invalid transaction encoding")]
    InvalidEncoding,
    #[error("Missing transaction metadata")]
    MissingMetadata,
    #[error("Decoding failed: {0}")]
    DecodingFailed(String),
    #[error("Timeout exceeded")]
    Timeout,
    #[error("Invalid instruction data")]
    InvalidInstruction,
    #[error("Address Lookup Table resolution failed")]
    AltResolution,
    #[error("Oracle error: {0}")]
    Oracle(String),
}

pub type Result<T> = std::result::Result<T, DecoderError>;

// Strategy taxonomy
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum StrategyType {
    Arbitrage { paths: Vec<TradePath>, profit_threshold: u64 },
    Sandwich { target_amount: u64, slippage_tolerance: u64 },
    Liquidation { protocol: String, collateral_ratio: u64 },
    JitLiquidity { pool: Pubkey, duration_ms: u64 },
    BackRun { target_signatures: Vec<Signature> },
    FrontRun { cu_price: u64, tip: u64 },
    FlashLoan { amount: u64, protocols: Vec<String> },
    AtomicArbitrage { dex_sequence: Vec<String> },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct TradePath {
    pub dex: String,
    pub pool: Pubkey,
    pub token_in: Pubkey,
    pub token_out: Pubkey,
    pub amount: u64,
}

// Decoded instruction with typed context
#[derive(Debug, Clone)]
pub struct DecodedInstruction {
    pub program_id: Pubkey,
    pub opcode: InstructionOpcode,
    pub accounts: SmallVec<[Pubkey; 8]>,
    pub data: Arc<[u8]>,
    pub roles: AccountRoles,
}

#[derive(Debug, Clone, Default)]
pub struct AccountRoles {
    pub user_atas: BTreeSet<Pubkey>,
    pub vaults: BTreeSet<Pubkey>,
    pub pool_accounts: BTreeSet<Pubkey>,
    pub authorities: BTreeSet<Pubkey>,
    pub fee_vaults: BTreeSet<Pubkey>,
}

#[derive(Debug, Clone)]
pub enum InstructionOpcode {
    // Commonized across protocols
    Swap,
    Deposit,
    Withdraw,
    IncreaseLiquidity,
    DecreaseLiquidity,
    Route,
    CLOBNewOrder,
    CLOBSettleFunds,
    FlashLoanBorrow,
    FlashLoanRepay,
    Transfer,
    CloseAccount,
    Unknown(u8),
}

// Transaction analysis product
#[derive(Debug, Clone)]
pub struct TransactionAnalysis {
    pub signature: Signature,
    pub slot: u64,
    pub timestamp: u64,
    pub strategy: StrategyType,
    pub pnl_lamports: i64,
    pub pnl_sol: f64,
    pub pnl_usd: f64,
    pub pnl_confidence: f32,
    pub cu_used: u64,
    pub cu_price: u64,
    pub tip: u64,
    pub fee_lamports: u64,
    pub instructions: Vec<DecodedInstruction>,
    pub bundle_rank: Option<u32>,
}

// Competitor profile and metrics
#[derive(Debug, Clone)]
pub struct StrategyMetrics {
    pub occurrences: usize,
    pub success_count: usize,
    pub total_profit_lamports: i128,
    pub avg_execution_time: Duration,
    pub cu_costs: Vec<u64>,
    pub timestamps_ms: Vec<u64>,
}

#[derive(Debug, Clone)]
pub struct TimingAnalysis {
    pub block_offset_distribution: HashMap<i32, usize>,
    pub submission_intervals: Vec<Duration>,
    pub preferred_leaders: HashSet<Pubkey>,
    pub burst_patterns: Vec<BurstPattern>,
}

#[derive(Debug, Clone)]
pub struct BurstPattern {
    pub tx_count: usize,
    pub duration: Duration,
    pub interval: Duration,
}

#[derive(Debug, Clone)]
pub struct GasAnalysis {
    pub cu_used: Vec<u64>,
    pub cu_price: Vec<u64>,
    pub tips: Vec<u64>,
    pub percentiles: HashMap<String, u64>,
    pub dynamic_pricing_model: Option<GasPricingModel>,
}

#[derive(Debug, Clone)]
pub struct GasPricingModel {
    pub base_multiplier_bps: u64,
    pub congestion_factor_bps: u64,
    pub competition_factor_bps: u64,
}

#[derive(Debug, Clone)]
pub struct CompetitorProfile {
    pub address: Pubkey,
    pub strategies: HashMap<StrategyType, StrategyMetrics>,
    pub timing_patterns: TimingAnalysis,
    pub gas_patterns: GasAnalysis,
    pub success_rate: f64,
    pub avg_profit_lamports: i64,
    pub last_seen: Instant,
    pub transaction_history: VecDeque<TransactionAnalysis>,
}

// Public insights
#[derive(Debug, Clone)]
pub struct CompetitorInsights {
    pub address: Pubkey,
    pub dominant_strategy: Option<StrategyType>,
    pub success_rate: f64,
    pub avg_profit_lamports: i64,
    pub gas_optimization: Option<GasOptimization>,
    pub timing_optimization: TimingOptimization,
    pub last_activity: Instant,
}

#[derive(Debug, Clone)]
pub struct GasOptimization {
    pub suggested_multiplier_bps: u64,
    pub min_cu_price: u64,
    pub max_cu_price: u64,
}

#[derive(Debug, Clone)]
pub struct TimingOptimization {
    pub preferred_submission_window: Duration,
    pub burst_detection: bool,
    pub preferred_leaders: HashSet<Pubkey>,
}

#[derive(Debug, Clone)]
pub struct EmergingPattern {
    pub pattern_signature: String,
    pub competitor_count: usize,
    pub total_occurrences: usize,
    pub avg_success_rate: f64,
    pub growth_rate: f64,
}

// Oracles abstraction
#[async_trait]
pub trait OracleBook: Send + Sync {
    async fn to_sol(&self, mint: &Pubkey, amount: i128, at_slot: u64) -> std::result::Result<f64, String>;
    async fn to_usd(&self, mint: &Pubkey, amount: i128, at_slot: u64) -> std::result::Result<f64, String>;
    fn quality(&self) -> f32; // confidence 0..1
}

// Program decoders
#[async_trait]
pub trait ProgramDecoder: Send + Sync {
    fn can_decode(&self, program_id: &Pubkey) -> bool;
    fn decode_instruction(
        &self,
        ix: &CompiledInstruction,
        accounts: &[Pubkey],
        raw_data: &[u8],
    ) -> Result<DecodedInstruction>;
    fn identify_strategy(&self, instructions: &[DecodedInstruction]) -> Option<StrategyType>;
}

// Percentile helper (interpolated)
fn percentile_interpolated(sorted: &[u64], p: f64) -> u64 {
    if sorted.is_empty() { return 0; }
    if sorted.len() == 1 { return sorted[0]; }
    let n = sorted.len() as f64;
    let rank = p.clamp(0.0, 1.0) * (n - 1.0);
    let lo = rank.floor() as usize;
    let hi = rank.ceil() as usize;
    if lo == hi { return sorted[lo]; }
    let w = rank - lo as f64;
    let lo_v = sorted[lo] as f64;
    let hi_v = sorted[hi] as f64;
    (lo_v + w * (hi_v - lo_v)) as u64
}

use crate::alpha_obfuscation::decoder_config::{HotReloadableConfig, DecoderConfig};
use crate::leader;
use crate::bundle;
use crate::alpha_obfuscation::{flow_graph, elasticity_model};

#[derive(Clone)]
struct CachedAlt {
    addresses: LoadedAddresses,
    expires_at: Instant,
}

pub struct CompetitorStrategyDecoder {
    rpc: Arc<RpcClient>,
    competitors: DashMap<Pubkey, CompetitorProfile>,
    program_decoders: Vec<Arc<dyn ProgramDecoder>>,
    oracles: Arc<dyn OracleBook>,
    leader_cache: Arc<leader::LeaderScheduleCache>,
    inclusion_stats: Arc<RwLock<leader::InclusionStats>>,
    bid_model: Arc<RwLock<bundle::BidCurveModel>>,
    alt_cache: DashMap<Signature, CachedAlt>,
    pending_fetches: DashSet<Signature>,
    rpc_client: Arc<RpcClient>,
    cache_ttl: Duration,
}

impl CompetitorStrategyDecoder {
    pub async fn new(
        rpc: Arc<RpcClient>,
        oracles: Arc<dyn OracleBook>,
    ) -> anyhow::Result<Self> {
        // Load decoder config with hot-reloading
        let config_path = "config/decoder_discriminators.toml";
        let mut decoder_config = HotReloadableConfig::new(config_path).await?;

        // Initialize decoders with config
        let mut decoders: Vec<Arc<dyn ProgramDecoder>> = Vec::with_capacity(8);
        decoders.push(Arc::new(SerumOpenBookDecoder::default()));
        decoders.push(Arc::new(RaydiumAmmV4Decoder::default()));
        decoders.push(Arc::new(OrcaWhirlpoolDecoder::default()));
        decoders.push(Arc::new(JupiterDecoder::default()));
        decoders.push(Arc::new(PhoenixDecoder::new(decoder_config.get().await.phoenix.clone())));
        decoders.push(Arc::new(MeteoraDlmmDecoder::new(decoder_config.get().await.meteora_dlmm.clone())));
        decoders.push(Arc::new(LifinityDecoder::new(decoder_config.get().await.lifinity.clone())));

        // Spawn config reload task
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(std::time::Duration::from_secs(30));
            loop {
                interval.tick().await;
                if let Ok(true) = decoder_config.reload_if_updated(config_path).await {
                    tracing::info!("Reloaded decoder discriminators config");
                }
            }
        });

        Ok(Self {
            rpc,
            competitors: DashMap::new(),
            program_decoders: decoders,
            oracles,
            leader_cache: Arc::new(leader::LeaderScheduleCache::new(64_000)),
            inclusion_stats: Arc::new(RwLock::new(leader::InclusionStats::new(10_000))),
            bid_model: Arc::new(RwLock::new(bundle::BidCurveModel::new(0.1, 0.01))),
            alt_cache: DashMap::new(),
            pending_fetches: DashSet::new(),
            rpc_client,
            cache_ttl: ALT_CACHE_TTL,
        })
    }

    #[instrument(name = "analyze_signature", skip_all, fields(sig = %sig, slot))]
    pub async fn analyze_signature(
        &self,
        sig: Signature,
        slot: u64,
    ) -> Result<TransactionAnalysis> {
        let span = info_span!("fetch_tx");
        let _g = span.enter();
        let start = std::time::Instant::now();
        let txw = timeout(
            RPC_TIMEOUT,
            self.fetch_transaction_with_retry(&sig),
        ).await.map_err(|_| DecoderError::Timeout)??;

        let meta = txw.transaction.meta.as_ref().ok_or(DecoderError::MissingMetadata)?;
        let timestamp = txw.block_time.unwrap_or(0).max(0) as u64;

        // Decode versioned message + resolve addresses
        let (vtx, loaded_addresses) = self.decode_versioned_with_alt_fallback(&txw.transaction).await?;

        // Collect inner instructions and logs for CU/tip parsing
        let inner_map = Self::build_inner_map(meta);
        let logs = meta.log_messages.as_ref();

        // Decode instructions via registered decoders
        let mut decoded = Vec::with_capacity(vtx.message.instructions.len());
        for (idx, ix) in vtx.message.instructions.iter().enumerate() {
            let pid = vtx.message.account_keys
                .get(ix.program_id_index as usize)
                .ok_or(DecoderError::InvalidInstruction)?;
            let accounts: SmallVec<[Pubkey; 8]> = ix
                .accounts
                .iter()
                .filter_map(|&i| vtx.message.account_keys.get(i as usize).copied())
                .collect();

            // Prefer exact decoder match
            if let Some(decoder) = self.program_decoders.iter().find(|d| d.can_decode(pid)) {
                let data = &ix.data;
                let di = decoder.decode_instruction(ix, &accounts, data)?;
                decoded.push(di);
            } else {
                // Fallback: mark unknown, still preserve for flow graph
                decoded.push(DecodedInstruction {
                    program_id: *pid,
                    opcode: InstructionOpcode::Unknown(255),
                    accounts,
                    data: Arc::from(ix.data.clone().into_boxed_slice()),
                    roles: AccountRoles::default(),
                });
            }

            // Consider attaching inner ixs by index if needed for evidence
            let _inner = inner_map.get(&idx);
        }

        // Identify strategy via ensemble
        let strategy = self.identify_strategy_ensemble(&decoded);

        // Extract CU, fee, tip
        let cu_used = meta.compute_units_consumed.unwrap_or(0) as u64;
        let cu_price = Self::extract_cu_price_from_logs(logs).unwrap_or_default();
        let tip = Self::extract_tip_from_logs(logs).unwrap_or_default();
        let fee_lamports = meta.fee as u64;

        // PnL attribution with SPL and lamports deltas + owner clustering
        let owner_set = Self::owner_candidates_from_message(&vtx);
        let (pnl_lamports, per_mint_owner_delta) = self.pnl_lamports(owner_set, meta)?;
        let (pnl_sol, pnl_usd, pnl_conf) = self.value_pnl(slot, meta, pnl_lamports, per_mint_owner_delta).await?;

        let analysis = TransactionAnalysis {
            signature: sig,
            slot,
            timestamp,
            strategy,
            pnl_lamports,
            pnl_sol,
            pnl_usd,
            pnl_confidence: pnl_conf,
            cu_used,
            cu_price,
            tip,
            fee_lamports,
            instructions: decoded,
            bundle_rank: Self::extract_bundle_rank_from_logs(logs),
        };

        // Flow graph analysis
        let flow_graph = flow_graph::build_flow_graph(&decoded);
        let cycles = flow_graph::detect_cycles(&flow_graph, 6);
        if !cycles.is_empty() {
            analysis.strategy = StrategyType::Arbitrage { paths: vec![], profit_threshold: 0 };
        }

        // Elasticity model observation
        if let Some(bundle_rank) = analysis.bundle_rank {
            let obs = elasticity_model::BidObs {
                cu_price: analysis.cu_price as f64,
                tip: analysis.tip as f64,
                congestion_idx: derive_congestion_idx(&meta),
                included: analysis.pnl_lamports >= 0,
            };
            self.bid_model.write().await.observe(&obs);
        }

        // Record leader inclusion stats
        if let Some(ldr) = self.leader_cache.get(slot).await.map(|x| x.leader) {
            let phase = leader::slot_phase_from_index(
                meta.transaction_index.unwrap_or(0) as usize,
                meta.transactions_in_block.unwrap_or(1) as usize,
            );
            self.inclusion_stats.write().await.record(ldr, phase, analysis.pnl_lamports > 0);
        }

        // Bundle/tip observation
        if let Some(rank) = analysis.bundle_rank {
            let obs = bundle::BundleObs {
                slot,
                bundle_rank: rank,
                tip_lamports: analysis.tip,
                cu_price: analysis.cu_price,
                cu_used: analysis.cu_used,
                included: analysis.pnl_lamports >= 0,
                pnl_usd: analysis.pnl_usd,
                congestion_idx: derive_congestion_idx(&meta),
            };
            self.bid_model.write().await.observe(&obs);
        }

        crate::observability::ANALYZER_LATENCY_MS.observe(start.elapsed().as_millis() as f64);
        Ok(analysis)
    }

    async fn fetch_transaction_with_retry(
        &self,
        signature: &Signature,
    ) -> Result<EncodedConfirmedTransactionWithStatusMeta> {
        let mut backoff_ms = 100u64;
        let cfg = RpcTransactionConfig {
            encoding: Some(UiTransactionEncoding::Base64),
            commitment: Some(CommitmentConfig::confirmed()),
            max_supported_transaction_version: Some(0),
        };
        for attempt in 0..=MAX_RETRIES {
            match self.rpc.get_transaction_with_config(signature, cfg.clone()).await {
                Ok(tx) => return Ok(tx),
                Err(e) if attempt < MAX_RETRIES => {
                    sleep(Duration::from_millis(backoff_ms)).await;
                    backoff_ms = (backoff_ms * 2).min(1_600);
                }
                Err(e) => return Err(e.into()),
            }
        }
        Err(DecoderError::Timeout)
    }

    async fn decode_versioned_with_alt_fallback(
        &self,
        etx: &EncodedTransactionWithStatusMeta,
    ) -> Result<(VersionedTransaction, LoadedAddresses)> {
        // Deserialize bytes -> VersionedTransaction (zero-copy not feasible across RPC here)
        match &etx.transaction {
            EncodedTransaction::Binary(s, _) | EncodedTransaction::LegacyBinary(s) => {
                let bytes = base64::decode(s).map_err(|_| DecoderError::InvalidEncoding)?;
                let vtx: VersionedTransaction = bincode::deserialize(&bytes)
                    .map_err(|e| DecoderError::Serialization(e))?;

                // Prefer ALT resolution provided by RPC meta where available.
                let loaded = if let Some(meta) = &etx.meta {
                    match &meta.loaded_addresses {
                        Some(las) => {
                            let mut la = LoadedAddresses::default();
                            // Convert Rpc's LoadedAddresses format to SDK format
                            la.writable = las.writable.clone().unwrap_or_default();
                            la.readonly = las.readonly.clone().unwrap_or_default();
                            la
                        }
                        None => LoadedAddresses::default(),
                    }
                } else {
                    LoadedAddresses::default()
                };

                if loaded == LoadedAddresses::default() {
                    if let Some(cached) = self.alt_cache.get(&vtx.message.signature()) {
                        if cached.expires_at > Instant::now() {
                            return Ok((vtx, cached.addresses.clone()));
                        }
                    }
                    
                    let alt = self.fetch_alt_addresses(&vtx.message.signature()).await?;
                    let decoded_with_alt = self.decode_versioned_with_alt(vtx, &alt).await?;
                    self.alt_cache.insert(vtx.message.signature(), CachedAlt {
                        addresses: decoded_with_alt.clone(),
                        expires_at: Instant::now() + self.cache_ttl,
                    });
                    Ok(decoded_with_alt)
                } else {
                    Ok((vtx, loaded))
                }
            }
            _ => Err(DecoderError::InvalidEncoding),
        }
    }

    async fn fetch_alt_addresses(&self, signature: &Signature) -> Result<LoadedAddresses> {
        // Check if already being fetched
        if self.pending_fetches.contains(signature) {
            return Err(anyhow::anyhow!("ALT fetch already in progress"));
        }

        self.pending_fetches.insert(*signature);
        
        let addresses = self.rpc_client
            .get_transaction(signature, UiTransactionEncoding::Json)
            .await?
            .transaction
            .message
            .loaded_addresses()
            .clone();

        self.pending_fetches.remove(signature);
        Ok(addresses)
    }

    async fn decode_versioned_with_alt(
        &self,
        vtx: &VersionedTransaction,
        alt: &LoadedAddresses,
    ) -> Result<(VersionedTransaction, LoadedAddresses)> {
        // Implementation to decode versioned transaction with ALT
        Ok((vtx.clone(), alt.clone()))
    }

    fn materialize_message(
        &self,
        vtx: &VersionedTransaction,
        loaded: &LoadedAddresses, 
    ) -> Result<(Vec<Pubkey>, Vec<CompiledInstruction>)> {
        let msg = &vtx.message;
        let (keys, ixs) = match msg {
            VersionedMessage::Legacy(leg) => (leg.account_keys.clone(), leg.instructions.clone()),
            VersionedMessage::V0(v0) => {
                let keys = v0.get_account_keys().into_iter().collect::<Vec<_>>();
                (keys, v0.instructions.clone())
            }
        };
        Ok((keys, ixs))
    }

    fn build_inner_map(meta: &TransactionStatusMeta) -> HashMap<usize, Vec<CompiledInstruction>> {
        let mut m = HashMap::<usize, Vec<CompiledInstruction>>::new();
        if let Some(inner) = &meta.inner_instructions {
            for UiInnerInstructions { index, instructions } in inner {
                let decoded: Vec<CompiledInstruction> = instructions
                    .iter()
                    .filter_map(|ui| ui.to_compiled_instruction().ok())
                    .collect();
                m.insert(*index as usize, decoded);
            }
        }
        m
    }

    fn identify_strategy_ensemble(&self, ixs: &[DecodedInstruction]) -> StrategyType {
        // Strong motifs override generic
        if self.is_flashloan(ixs) {
            let (amount, protocols) = self.flashloan_params(ixs);
            return StrategyType::FlashLoan { amount, protocols };
        }
        if self.is_jit(ixs) {
            let (pool, dur) = self.jit_params(ixs);
            return StrategyType::JitLiquidity { pool, duration_ms: dur };
        }
        if self.is_arbitrage(ixs) {
            let paths = self.trade_paths(ixs);
            let threshold = self.profit_threshold(&paths);
            return StrategyType::Arbitrage { paths, profit_threshold: threshold };
        }
        if self.is_sandwich(ixs) {
            let (amt, slip) = self.sandwich_params(ixs);
            return StrategyType::Sandwich { target_amount: amt, slippage_tolerance: slip };
        }
        let seq = self.dex_sequence(ixs);
        StrategyType::AtomicArbitrage { dex_sequence: seq }
    }

    fn is_arbitrage(&self, ixs: &[DecodedInstruction]) -> bool {
        let dex_interactions = ixs.iter().filter(|i| self.is_dex(&i.program_id)).count();
        dex_interactions >= 2 && self.has_profit_extraction(ixs)
    }
    fn is_sandwich(&self, ixs: &[DecodedInstruction]) -> bool {
        if ixs.len() < 3 { return false; }
        let first_swap = ixs.first().map(|i| matches!(i.opcode, InstructionOpcode::Swap | InstructionOpcode::Route)).unwrap_or(false);
        let last_swap = ixs.last().map(|i| matches!(i.opcode, InstructionOpcode::Swap | InstructionOpcode::Route)).unwrap_or(false);
        first_swap && last_swap
    }
    fn is_jit(&self, ixs: &[DecodedInstruction]) -> bool {
        let add = ixs.iter().any(|i| matches!(i.opcode, InstructionOpcode::IncreaseLiquidity | InstructionOpcode::Deposit));
        let rem = ixs.iter().any(|i| matches!(i.opcode, InstructionOpcode::DecreaseLiquidity | InstructionOpcode::Withdraw));
        add && rem
    }
    fn is_flashloan(&self, ixs: &[DecodedInstruction]) -> bool {
        let mut seen_borrow: Option<&DecodedInstruction> = None;
        for (k, ix) in ixs.iter().enumerate() {
            if matches!(ix.opcode, InstructionOpcode::FlashLoanBorrow) {
                seen_borrow = Some(ix);
                for later in ixs.iter().skip(k + 1) {
                    if matches!(later.opcode, InstructionOpcode::FlashLoanRepay) &&
                        !later.accounts.is_empty() && !ix.accounts.is_empty() &&
                        later.accounts[0] == ix.accounts[0] {
                        return true;
                    }
                }
            }
        }
        false
    }
    fn has_profit_extraction(&self, ixs: &[DecodedInstruction]) -> bool {
        ixs.iter().any(|i| matches!(i.opcode, InstructionOpcode::Transfer | InstructionOpcode::CloseAccount | InstructionOpcode::CLOBSettleFunds))
    }
    fn trade_paths(&self, ixs: &[DecodedInstruction]) -> Vec<TradePath> {
        ixs.iter()
            .filter(|i| matches!(i.opcode, InstructionOpcode::Swap | InstructionOpcode::Route))
            .filter_map(|ix| {
                let (pool, token_in, token_out) = (
                    ix.accounts.get(0).copied()?,
                    ix.accounts.get(1).copied()?,
                    ix.accounts.get(2).copied()?,
                );
                Some(TradePath {
                    dex: self.identify_dex(&ix.program_id),
                    pool,
                    token_in,
                    token_out,
                    amount: Self::amount_from_data(&ix.data),
                })
            })
            .collect()
    }
    fn profit_threshold(&self, paths: &[TradePath]) -> u64 {
        let vol: u64 = paths.iter().map(|p| p.amount).sum();
        (vol / 1000).max(10_000)
    }
    fn sandwich_params(&self, ixs: &[DecodedInstruction]) -> (u64, u64) {
        let mut amounts = Vec::with_capacity(ixs.len());
        for ix in ixs.iter() {
            amounts.push(Self::amount_from_data(&ix.data));
        }
        let target = *amounts.get(1).unwrap_or(&0);
        let slip = if amounts.len() >= 2 && amounts[0] > 0 {
            (((amounts[1].saturating_sub(amounts[0])) as f64 / amounts[0] as f64) * 10_000.0) as u64
        } else { 100 };
        (target, slip)
    }
    fn jit_params(&self, ixs: &[DecodedInstruction]) -> (Pubkey, u64) {
        let pool = ixs.iter().find(|i| matches!(i.opcode, InstructionOpcode::IncreaseLiquidity | InstructionOpcode::DecreaseLiquidity))
            .and_then(|i| i.accounts.first().copied())
            .unwrap_or_default();
        (pool, TIMING_PRECISION_MS * 4)
    }
    fn flashloan_params(&self, ixs: &[DecodedInstruction]) -> (u64, Vec<String>) {
        let amount = ixs.iter()
            .find(|i| matches!(i.opcode, InstructionOpcode::FlashLoanBorrow))
            .map(|i| Self::amount_from_data(&i.data))
            .unwrap_or(0);
        let mut set = HashSet::new();
        for i in ixs {
            let d = self.identify_dex(&i.program_id);
            if d != "Unknown" { set.insert(d); }
        }
        (amount, set.into_iter().collect())
    }
    fn dex_sequence(&self, ixs: &[DecodedInstruction]) -> Vec<String> {
        ixs.iter().filter(|i| self.is_dex(&i.program_id)).map(|i| self.identify_dex(&i.program_id))
            .filter(|d| d != "Unknown").collect()
    }

    fn is_dex(&self, program_id: &Pubkey) -> bool {
        self.program_decoders.iter().any(|d| d.can_decode(program_id))
    }
    fn identify_dex(&self, program_id: &Pubkey) -> String {
        match program_id.to_string().as_str() {
            SERUM_DEX_V3 => "Serum".to_string(),
            OPENBOOK_V2 => "OpenBook".to_string(),
            RAYDIUM_AMM_V4 => "Raydium".to_string(),
            ORCA_WHIRLPOOL => "Orca".to_string(),
            JUPITER_V4 => "Jupiter".to_string(),
            LIFINITY_V2 => "Lifinity".to_string(),
            _ => "Unknown".to_string(),
        }
    }
    fn amount_from_data(data: &[u8]) -> u64 {
        if data.len() < 8 { return 0; }
        u64::from_le_bytes(data[0..8].try_into().unwrap())
    }

    fn owner_candidates_from_message(vtx: &VersionedTransaction) -> HashSet<Pubkey> {
        let mut set = HashSet::new();
        for s in &vtx.signatures { let _ = s; }
        match &vtx.message {
            VersionedMessage::Legacy(m) => {
                // fee payer at index 0
                if let Some(payer) = m.account_keys.first() { set.insert(*payer); }
            }
            VersionedMessage::V0(m) => {
                if let Some(payer) = m.static_account_keys.first() { set.insert(*payer); }
            }
        }
        // Extend with common derivatives (A/TAs, vaults) via heuristics if needed
        set
    }

    fn pnl_lamports(
        &self,
        owners: HashSet<Pubkey>,
        meta: &TransactionStatusMeta,
    ) -> Result<(i64, Option<HashMap<(String, String), i128>)> {
        // 1) Owner lamport deltas
        let mut owner_lamport_delta: i128 = 0;
        if let (Some(pre_balances), Some(post_balances)) = (meta.pre_balances.as_ref(), meta.post_balances.as_ref()) {
            if pre_balances.len() == post_balances.len() {
                for (pre, post) in pre_balances.iter().zip(post_balances.iter()) {
                    let d = (*post as i128) - (*pre as i128);
                    if d != 0 && d > owner_lamport_delta {
                        owner_lamport_delta = d;
                    }
                }
            }
        }

        // 2) Token deltas per (mint, owner)
        let parse_token_balances = |tb_vec: &Option<Vec<UiTokenBalance>>| {
            tb_vec.clone().unwrap_or_default()
        };

        let pre_tokens = parse_token_balances(&meta.pre_token_balances);
        let post_tokens = parse_token_balances(&meta.post_token_balances);

        let mut pre_map = HashMap::new();
        for tb in pre_tokens {
            if tb.owner.is_empty() || tb.mint.is_empty() { continue; }
            if let Ok(owner) = Pubkey::from_str(&tb.owner) {
                if owners.contains(&owner) {
                    let amt = tb.ui_token_amount.amount.parse::<i128>().unwrap_or(0);
                    pre_map.insert((tb.mint, tb.owner), amt);
                }
            }
        }

        let mut post_map = HashMap::new();
        for tb in post_tokens {
            if tb.owner.is_empty() || tb.mint.is_empty() { continue; }
            if let Ok(owner) = Pubkey::from_str(&tb.owner) {
                if owners.contains(&owner) {
                    let amt = tb.ui_token_amount.amount.parse::<i128>().unwrap_or(0);
                    post_map.insert((tb.mint, tb.owner), amt);
                }
            }
        }

        let mut per_mint_owner_delta = HashMap::new();
        for ((mint, owner), &post_amt) in &post_map {
            let pre_amt = *pre_map.get(&(mint.clone(), owner.clone())).unwrap_or(&0);
            let delta = post_amt - pre_amt;
            if delta != 0 {
                per_mint_owner_delta.insert((mint.clone(), owner.clone()), delta);
            }
        }

        for ((mint, owner), &pre_amt) in &pre_map {
            if !post_map.contains_key(&(mint.clone(), owner.clone())) {
                per_mint_owner_delta.insert((mint.clone(), owner.clone()), -pre_amt);
            }
        }

        // 3) Inner instruction token transfers (placeholder for future implementation)
        
        Ok((owner_lamport_delta as i64, Some(per_mint_owner_delta)))
    }

    async fn value_pnl(
        &self,
        slot: u64,
        meta: &TransactionStatusMeta,
        pnl_lamports: i64,
        per_mint_owner_delta: Option<HashMap<(String, String), i128>>,
    ) -> Result<(f64, f64, f32)> {
        // Convert lamports->SOL and multi‑mint token deltas->SOL/USD via oracle
        let lamports_per_sol = 1_000_000_000f64;
        let pnl_sol = pnl_lamports as f64 / lamports_per_sol;

        // Token PnL aggregation (owner‑scoped) — placeholder for brevity:
        let mut token_sol_contrib = 0.0f64;
        if let Some(delta_map) = per_mint_owner_delta {
            for ((mint, _), delta) in delta_map {
                if let Ok(sol) = self.oracles.to_sol(&Pubkey::from_str(&mint).unwrap(), delta, slot).await {
                    token_sol_contrib += sol;
                }
            }
        }

        let sol_total = pnl_sol + token_sol_contrib;

        // Convert SOL to USD via oracle (or use direct USD per mint if available)
        let usd_total = sol_total * self.oracle_sol_usd(slot).await?;

        let confidence = self.oracles.quality();

        Ok((sol_total, usd_total, confidence))
    }

    async fn oracle_sol_usd(&self, _slot: u64) -> Result<f64> {
        // Your OracleBook impl can give SOL/USD; for now, a placeholder value with error channel
        Ok(150.0) // replace with real oracle
    }

    fn extract_cu_price_from_logs(logs: Option<&Vec<String>>) -> Option<u64> {
        logs.and_then(|ls| {
            // Parse compute-unit price logs (e.g., ComputeBudget::setComputeUnitPrice)
            for l in ls {
                if let Some(pos) = l.find("cu price: ") {
                    if let Ok(v) = l[pos + 10..].trim().parse::<u64>() {
                        return Some(v);
                    }
                }
            }
            None
        })
    }

    fn extract_tip_from_logs(logs: Option<&Vec<String>>) -> Option<u64> {
        logs.and_then(|ls| {
            // Heuristic: look for known tip program/account transfer logs; customize to your tip infra
            for l in ls {
                if let Some(pos) = l.find("tip: ") {
                    if let Ok(v) = l[pos + 5..].trim().parse::<u64>() {
                        return Some(v);
                    }
                }
            }
            None
        })
    }

    fn extract_bundle_rank_from_logs(logs: Option<&Vec<String>>) -> Option<u32> {
        logs.and_then(|ls| {
            for l in ls {
                if let Some(pos) = l.find("bundle_rank=") {
                    let s = &l[pos + 12..];
                    if let Some(end) = s.find(' ') {
                        if let Ok(v) = s[..end].parse::<u32>() {
                            return Some(v);
                        }
                    }
                }
            }
            None
        })
    }

    pub fn update_competitor_profile(&self, address: Pubkey, analysis: TransactionAnalysis) {
        let mut entry = self.competitors.entry(address).or_insert_with(|| CompetitorProfile {
            address,
            strategies: HashMap::new(),
            timing_patterns: TimingAnalysis {
                block_offset_distribution: HashMap::new(),
                submission_intervals: Vec::new(),
                preferred_leaders: HashSet::new(),
                burst_patterns: Vec::new(),
            },
            gas_patterns: GasAnalysis {
                cu_used: Vec::new(),
                cu_price: Vec::new(),
                tips: Vec::new(),
                percentiles: HashMap::new(),
                dynamic_pricing_model: None,
            },
            success_rate: 0.0,
            avg_profit_lamports: 0,
            last_seen: Instant::now(),
            transaction_history: VecDeque::with_capacity(STRATEGY_WINDOW),
        });

        entry.last_seen = Instant::now();

        let m = entry.strategies.entry(analysis.strategy.clone()).or_insert_with(|| StrategyMetrics {
            occurrences: 0,
            success_count: 0,
            total_profit_lamports: 0,
            avg_execution_time: Duration::from_millis(0),
            cu_costs: Vec::new(),
            timestamps_ms: Vec::new(),
        });

        m.occurrences += 1;
        if analysis.pnl_lamports > 0 { m.success_count += 1; }
        m.total_profit_lamports += analysis.pnl_lamports as i128;
        m.cu_costs.push(analysis.cu_used);
        m.timestamps_ms.push(analysis.timestamp);

        entry.gas_patterns.cu_used.push(analysis.cu_used);
        entry.gas_patterns.cu_price.push(analysis.cu_price);
        entry.gas_patterns.tips.push(analysis.tip);

        if entry.transaction_history.len() >= STRATEGY_WINDOW {
            entry.transaction_history.pop_front();
        }
        entry.transaction_history.push_back(analysis);

        self.update_timing_patterns(entry);
        self.update_gas_analysis(entry);
        self.calculate_success_metrics(entry);
    }

    fn update_timing_patterns(&self, profile: &mut CompetitorProfile) {
        if profile.transaction_history.len() < 2 { return; }

        let timestamps: Vec<u64> = profile.transaction_history.iter().map(|tx| tx.timestamp).collect();
        profile.timing_patterns.submission_intervals.clear();
        for w in timestamps.windows(2) {
            if let [prev, curr] = w {
                let iv = Duration::from_millis(curr.saturating_sub(*prev));
                profile.timing_patterns.submission_intervals.push(iv);
            }
        }

        // Preferred leaders: placeholder; wire to leader schedule map to derive validator pubkeys
        profile.timing_patterns.preferred_leaders.clear();
    }

    fn update_gas_analysis(&self, profile: &mut CompetitorProfile) {
        let n = profile.gas_patterns.cu_price.len();
        if n < 10 { return; }
        let mut price_sorted = profile.gas_patterns.cu_price.clone();
        price_sorted.par_sort_unstable();

        profile.gas_patterns.percentiles.clear();
        for p in GAS_ANALYSIS_PERCENTILES {
            let v = percentile_interpolated(&price_sorted, p);
            profile.gas_patterns.percentiles.insert(format!("p{}", (p * 100.0) as u32), v);
        }

        // Dynamic model (illustrative)
        let sum: u64 = price_sorted.iter().sum();
        let avg = sum / (n as u64).max(1);
        let var = price_sorted.par_iter().map(|&x| {
            let d = x as i128 - avg as i128;
            (d * d) as u128
        }).sum::<u128>() / (n as u128).max(1);
        let std = (var as f64).sqrt() as u64;

        profile.gas_patterns.dynamic_pricing_model = Some(GasPricingModel {
            base_multiplier_bps: 10_000 + (std.saturating_mul(10_000) / avg.max(1)),
            congestion_factor_bps: (price_sorted.last().copied().unwrap_or(avg) * 10_000) / avg.max(1),
            competition_factor_bps: 12_000,
        });
    }

    fn calculate_success_metrics(&self, profile: &mut CompetitorProfile) {
        let total_txs = profile.transaction_history.len();
        if total_txs == 0 { return; }

        let successful = profile.transaction_history.iter().filter(|tx| tx.pnl_lamports > 0).count();
        profile.success_rate = (successful as f64 / total_txs as f64 * 100.0).min(100.0);

        let total_pnl: i128 = profile.transaction_history.iter().map(|tx| tx.pnl_lamports as i128).sum();
        profile.avg_profit_lamports = if total_pnl >= 0 { (total_pnl / total_txs as i128) as i64 } else { 0 };
    }

    pub fn get_competitor_insights(&self, address: &Pubkey) -> Option<CompetitorInsights> {
        let profile = self.competitors.get(address)?;
        let dominant = profile.strategies.iter().max_by_key(|(_, m)| m.occurrences).map(|(s, _)| s.clone());

        let gas_opt = profile.gas_patterns.dynamic_pricing_model.as_ref().map(|m| {
            let p25 = *profile.gas_patterns.percentiles.get("p25").unwrap_or(&1_000);
            let p95 = *profile.gas_patterns.percentiles.get("p95").unwrap_or(&100_000);
            GasOptimization {
                suggested_multiplier_bps: m.base_multiplier_bps.saturating_mul(95) / 100,
                min_cu_price: p25,
                max_cu_price: p95,
            }
        });

        let timing_opt = TimingOptimization {
            preferred_submission_window: profile.timing_patterns.submission_intervals.iter().min().copied()
                .unwrap_or(Duration::from_millis(100)),
            burst_detection: !profile.timing_patterns.burst_patterns.is_empty(),
            preferred_leaders: profile.timing_patterns.preferred_leaders.clone(),
        };

        Some(CompetitorInsights {
            address: *address,
            dominant_strategy: dominant,
            success_rate: profile.success_rate,
            avg_profit_lamports: profile.avg_profit_lamports,
            gas_optimization: gas_opt,
            timing_optimization: timing_opt,
            last_activity: profile.last_seen,
        })
    }

    pub fn detect_emerging_patterns(&self) -> Vec<EmergingPattern> {
        let mut by_pattern: HashMap<String, Vec<&StrategyMetrics>> = HashMap::new();
        for profile in self.competitors.iter() {
            for (strategy, metrics) in &profile.strategies {
                if metrics.occurrences >= PATTERN_MIN_OCCURRENCES {
                    let key = format!("{:?}", strategy);
                    by_pattern.entry(key).or_default().push(metrics);
                }
            }
        }

        by_pattern.into_par_iter().filter_map(|(pattern, ms)| {
            if ms.len() < 2 { return None; }
            let total_occ: usize = ms.iter().map(|m| m.occurrences).sum();
            let succ: usize = ms.iter().map(|m| m.success_count).sum();
            let avg_succ = if total_occ > 0 { succ as f64 / total_occ as f64 } else { 0.0 };
            Some(EmergingPattern {
                pattern_signature: pattern,
                competitor_count: ms.len(),
                total_occurrences: total_occ,
                avg_success_rate: avg_succ,
                growth_rate: Self::growth_rate(&ms),
            })
        }).collect()
    }

    fn growth_rate(ms: &[&StrategyMetrics]) -> f64 {
        let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_millis() as u64;
        let recent = 3_600_000; // 1 hour
        let recent_cnt: usize = ms.par_iter().map(|m| m.timestamps_ms.iter().filter(|&&t| now.saturating_sub(t) < recent).count()).sum();
        let total: usize = ms.iter().map(|m| m.timestamps_ms.len()).sum();
        if total > 0 { (recent_cnt as f64 / total as f64 * 100.0).min(100.0) } else { 0.0 }
    }

    fn derive_congestion_idx(meta: &TransactionStatusMeta) -> f64 {
        // Implement congestion index calculation
        0.0
    }
}

pub struct BidCurveModel {
    validator_models: DashMap<Pubkey, elasticity_model::ElasticityModel>,
    global_model: RwLock<elasticity_model::ElasticityModel>,
}

impl BidCurveModel {
    pub fn new(learning_rate: f64, l2_penalty: f64) -> Self {
        Self {
            validator_models: DashMap::new(),
            global_model: RwLock::new(elasticity_model::ElasticityModel::new(learning_rate, l2_penalty)),
        }
    }

    pub async fn observe(&self, obs: &bundle::BundleObs) {
        // Update global model
        self.global_model.write().await.observe(&obs.to_bid_obs());
        
        // Update validator-specific models
        for validator in &obs.validators {
            self.validator_models
                .entry(*validator)
                .or_insert_with(|| elasticity_model::ElasticityModel::new(0.1, 0.01))
                .observe(&obs.to_bid_obs());
        }
    }

    pub async fn predict_prob(
        &self,
        validator: Option<&Pubkey>,
        cu_price: f64,
        tip: f64,
        congestion_idx: f64,
    ) -> f64 {
        match validator {
            Some(v) => self.validator_models
                .get(v)
                .map(|m| m.predict_prob(cu_price, tip, congestion_idx))
                .unwrap_or_else(|| self.global_model.read().await.predict_prob(cu_price, tip, congestion_idx)),
            None => self.global_model.read().await.predict_prob(cu_price, tip, congestion_idx),
        }
    }
}

/* ===================== Protocol decoders (typed) ===================== */

#[derive(Default)]
struct SerumOpenBookDecoder;
#[async_trait]
impl ProgramDecoder for SerumOpenBookDecoder {
    fn can_decode(&self, pid: &Pubkey) -> bool {
        let s = pid.to_string();
        s == SERUM_DEX_V3 || s == OPENBOOK_V2
    }
    fn decode_instruction(
        &self,
        ix: &CompiledInstruction,
        accounts: &[Pubkey],
        data: &[u8],
    ) -> Result<DecodedInstruction> {
        if data.is_empty() { return Err(DecoderError::InvalidInstruction); }
        let opcode = match data[0] {
            1 | 9 | 10 => InstructionOpcode::CLOBNewOrder,
            5 => InstructionOpcode::CLOBSettleFunds,
            _ => InstructionOpcode::Unknown(data[0]),
        };
        Ok(DecodedInstruction {
            program_id: accounts.get(ix.program_id_index as usize).copied().unwrap_or_default(),
            opcode,
            accounts: accounts.iter().copied().collect(),
            data: Arc::from(data.to_vec().into_boxed_slice()),
            roles: AccountRoles::default(),
        })
    }
    fn identify_strategy(&self, ixs: &[DecodedInstruction]) -> Option<StrategyType> {
        let new_order = ixs.iter().any(|i| matches!(i.opcode, InstructionOpcode::CLOBNewOrder));
        let settle = ixs.iter().any(|i| matches!(i.opcode, InstructionOpcode::CLOBSettleFunds));
        if new_order && settle {
            Some(StrategyType::AtomicArbitrage { dex_sequence: vec!["Serum".into()] })
        } else { None }
    }
}

#[derive(Default)]
struct RaydiumAmmV4Decoder;
#[async_trait]
impl ProgramDecoder for RaydiumAmmV4Decoder {
    fn can_decode(&self, pid: &Pubkey) -> bool { pid.to_string().as_str() == RAYDIUM_AMM_V4 }
    fn decode_instruction(
        &self,
        _ix: &CompiledInstruction,
        accounts: &[Pubkey],
        data: &[u8],
    ) -> Result<DecodedInstruction> {
        if data.len() < 8 { return Err(DecoderError::InvalidInstruction); }
        let discr = u64::from_le_bytes(data[0..8].try_into().unwrap());
        let opcode = match discr {
            0x8ed6b032768e93e3 | 0x904ba91a522e0b00 => InstructionOpcode::Swap,
            0x2169d72e022ba23e => InstructionOpcode::Deposit,
            0x183ed6e268ba06f2 => InstructionOpcode::Withdraw,
            _ => InstructionOpcode::Unknown((discr & 0xff) as u8),
        };
        Ok(DecodedInstruction {
            program_id: Pubkey::from_str(RAYDIUM_AMM_V4).unwrap(),
            opcode,
            accounts: accounts.iter().copied().collect(),
            data: Arc::from(data.to_vec().into_boxed_slice()),
            roles: AccountRoles::default(),
        })
    }
    fn identify_strategy(&self, ixs: &[DecodedInstruction]) -> Option<StrategyType> {
        let swaps = ixs.iter().filter(|i| matches!(i.opcode, InstructionOpcode::Swap)).count();
        if swaps > 0 {
            Some(StrategyType::AtomicArbitrage { dex_sequence: vec!["Raydium".into()] })
        } else { None }
    }
}

#[derive(Default)]
struct OrcaWhirlpoolDecoder;
#[async_trait]
impl ProgramDecoder for OrcaWhirlpoolDecoder {
    fn can_decode(&self, pid: &Pubkey) -> bool { pid.to_string().as_str() == ORCA_WHIRLPOOL }
    fn decode_instruction(
        &self,
        _ix: &CompiledInstruction,
        accounts: &[Pubkey],
        data: &[u8],
    ) -> Result<DecodedInstruction> {
        if data.is_empty() { return Err(DecoderError::InvalidInstruction); }
        let opcode = match data[0] {
            0x01 => InstructionOpcode::Swap,
            0x02 => InstructionOpcode::IncreaseLiquidity,
            0x03 => InstructionOpcode::DecreaseLiquidity,
            0x05 => InstructionOpcode::Transfer, // collect fees treated as transfer for flow graph
            0x08 => InstructionOpcode::CloseAccount, // close position
            _ => InstructionOpcode::Unknown(data[0]),
        };
        Ok(DecodedInstruction {
            program_id: Pubkey::from_str(ORCA_WHIRLPOOL).unwrap(),
            opcode,
            accounts: accounts.iter().copied().collect(),
            data: Arc::from(data.to_vec().into_boxed_slice()),
            roles: AccountRoles::default(),
        })
    }
    fn identify_strategy(&self, ixs: &[DecodedInstruction]) -> Option<StrategyType> {
        let add = ixs.iter().any(|i| matches!(i.opcode, InstructionOpcode::IncreaseLiquidity));
        let rem = ixs.iter().any(|i| matches!(i.opcode, InstructionOpcode::DecreaseLiquidity));
        if add && rem {
            let pool = ixs.first().and_then(|i| i.accounts.first().copied()).unwrap_or_default();
            Some(StrategyType::JitLiquidity { pool, duration_ms: TIMING_PRECISION_MS * 4 })
        } else if ixs.iter().any(|i| matches!(i.opcode, InstructionOpcode::Swap)) {
            Some(StrategyType::AtomicArbitrage { dex_sequence: vec!["Orca".into()] })
        } else { None }
    }
}

#[derive(Default)]
struct JupiterDecoder;
#[async_trait]
impl ProgramDecoder for JupiterDecoder {
    fn can_decode(&self, pid: &Pubkey) -> bool { pid.to_string().as_str() == JUPITER_V4 }
    fn decode_instruction(
        &self,
        _ix: &CompiledInstruction,
        accounts: &[Pubkey],
        data: &[u8],
    ) -> Result<DecodedInstruction> {
        if data.is_empty() { return Err(DecoderError::InvalidInstruction); }
        let opcode = InstructionOpcode::Route;
        Ok(DecodedInstruction {
            program_id: Pubkey::from_str(JUPITER_V4).unwrap(),
            opcode,
            accounts: accounts.iter().copied().collect(),
            data: Arc::from(data.to_vec().into_boxed_slice()),
            roles: AccountRoles::default(),
        })
    }
    fn identify_strategy(&self, ixs: &[DecodedInstruction]) -> Option<StrategyType> {
        let routes = ixs.iter().filter(|i| matches!(i.opcode, InstructionOpcode::Route)).count();
        if routes > 0 {
            Some(StrategyType::AtomicArbitrage { dex_sequence: vec!["Jupiter".into()] })
        } else { None }
    }
}

/* ===================== Observability (hooks) ===================== */
// Use your metrics/tracing crates; placeholders shown below.

pub mod obs {
    use super::*;
    pub fn record_analysis_latency(_dur_ms: u64) {}
    pub fn inc_decode_error(_kind: &'static str) {}
    pub fn record_fee_distribution(_cu_price: u64, _tip: u64) {}
}
