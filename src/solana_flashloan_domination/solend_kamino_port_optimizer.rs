use solana_sdk::message::Message;
use solana_sdk::{
    address_lookup_table_account::AddressLookupTableAccount,
    transaction::VersionedTransaction,
};
use solana_client::rpc_config::RpcSignatureStatusConfig;
use anchor_client::{
    solana_sdk::{
        commitment_config::CommitmentConfig,
        compute_budget::ComputeBudgetInstruction,
        instruction::Instruction,
        pubkey::Pubkey,
        signature::{Keypair, Signer},
        transaction::Transaction,
    },
    
};
use anyhow::{anyhow, Result};
use borsh::BorshSerialize;
use sha2::{Digest, Sha256};
use tch::{no_grad, CModule, Device, Kind, Tensor};
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_program::{
    instruction::AccountMeta,
    program_pack::Pack,
    system_instruction,
};
use solana_sdk::hash::Hash;
use spl_token::state::Account as TokenAccount;
use std::{
    collections::HashMap,
    str::FromStr,
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::sync::{RwLock, mpsc, oneshot};
use tokio::task::JoinHandle;
use rand::prelude::*;
use tracing::{info, warn, error};
use prometheus::{IntCounter, Histogram, Registry, HistogramOpts};

#[derive(Clone)]
pub struct TipConfig {
    pub accounts: Vec<Pubkey>,
    pub strategy: TipStrategy,
    pub min_balance: u64,
}

// ----- Zero-GC warmed TorchScript batcher with micro-batching -----
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
    pub fn load(path: &str, max_batch: usize, max_wait: Duration) -> Result<Self> {
        let device = Device::Cpu; // set to Device::Cuda(_) if deploying on GPU
        let mut module = CModule::load_on_device(path, device)?;

        // Warm-up to avoid first-call latency spike; allocate kernels and caches
        no_grad(|| {
            let x = Tensor::zeros([1, 32, 4], (Kind::Float, device));
            let _ = module.forward_ts(&[x]);
        });

        let (tx, mut rx) = mpsc::channel::<InferenceItem>(1024);
        let module_arc = module.clone();
        let worker = tokio::spawn(async move {
            let module = module_arc;
            let device = device;
            loop {
                // Collect micro-batch with timeout window
                let mut batch: Vec<InferenceItem> = Vec::with_capacity(max_batch);
                match rx.recv().await {
                    Some(first) => batch.push(first),
                    None => break, // channel closed
                }
                let start = tokio::time::Instant::now();
                while batch.len() < max_batch && start.elapsed() < max_wait {
                    match rx.try_recv() {
                        Ok(it) => batch.push(it),
                        Err(_) => tokio::time::sleep(Duration::from_micros(200)).await,
                    }
                }

                // Determine time window T from shortest series across features
                let mut t: usize = usize::MAX;
                for b in &batch {
                    t = t.min(b.apy_spread_bps.len()
                        .min(b.utilization.len())
                        .min(b.fee_lamports.len())
                        .min(b.liq_ratio.len()));
                }
                if t == usize::MAX { t = 0; }
                t = t.max(8);

                // Build a contiguous buffer (B, T, 4)
                let bsz = batch.len();
                let mut buf: Vec<f32> = Vec::with_capacity(bsz * t * 4);
                for it in &batch {
                    let n = it.apy_spread_bps.len();
                    let start_idx = n.saturating_sub(t);
                    for i in start_idx..n {
                        buf.push(it.apy_spread_bps[i]);
                        buf.push(it.utilization[i]);
                        buf.push(it.fee_lamports[i]);
                        buf.push(it.liq_ratio[i]);
                    }
                }

                // Inference (no_grad)
                let x = Tensor::of_slice(&buf)
                    .to_device(device)
                    .reshape([bsz as i64, t as i64, 4]);
                let y = no_grad(|| module.forward_ts(&[x]));

                match y {
                    Ok(out) => {
                        let out = out.squeeze();
                        // Extract per-item probability; handle scalar fallback
                        let mut probs: Vec<f32> = Vec::with_capacity(bsz);
                        if out.dim() == 0 {
                            probs.push(f32::from(out.to_kind(Kind::Float)));
                        } else {
                            for i in 0..bsz {
                                let p = out.double_value(&[i as i64]) as f32;
                                probs.push(p);
                            }
                        }
                        for (i, it) in batch.into_iter().enumerate() {
                            let p = probs.get(i).copied().unwrap_or(0.0).clamp(0.0, 1.0);
                            let _ = it.resp.send(Ok(p));
                        }
                    }
                    Err(e) => {
                        for it in batch.into_iter() {
                            let _ = it.resp.send(Err(anyhow!(format!("inference failed: {e}"))));
                        }
                    }
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
        let (resp_tx, resp_rx) = oneshot::channel::<Result<f32>>();
        let item = InferenceItem { apy_spread_bps, utilization, fee_lamports, liq_ratio, resp: resp_tx };
        self.tx.send(item).await.map_err(|_| anyhow!("batcher closed"))?;
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
    pub ema: f64,        // level
    pub emv: f64,        // volatility
    pub ghost_rate: f64, // failures/attempts
    pub weight: f64,
    pub attempts: u64,
    pub failures: u64,
}

#[derive(Clone)]
pub struct TipRouter {
    inner: Arc<RwLock<Vec<TipStats>>>,
    alpha: f64,    // level smoothing
    beta: f64,     // volatility smoothing
    floor_ms: f64,
    ceil_ms: f64,
    outlier_q: f64,
    min_weight: f64,
}

impl TipRouter {
    pub fn new(accounts: Vec<Pubkey>) -> Self {
        let v: Vec<TipStats> = accounts
            .into_iter()
            .map(|a| TipStats {
                account: a,
                ema: 800.0,
                emv: 100.0,
                ghost_rate: 0.0,
                weight: 1.0,
                attempts: 0,
                failures: 0,
            })
            .collect();
        let r = Self {
            inner: Arc::new(RwLock::new(v)),
            alpha: 0.2,
            beta: 0.1,
            floor_ms: 100.0,
            ceil_ms: 3000.0,
            outlier_q: 0.95,
            min_weight: 0.01,
        };
        // Normalize initial weights
        let mut g = r.inner.blocking_write();
        r.recompute_weights_locked(&mut g);
        drop(g);
        r
    }

    pub async fn record_attempt(&self, acct: Pubkey, success: bool, latency_ms: Option<f64>) {
        let mut tips = self.inner.write().await;
        if let Some(t) = tips.iter_mut().find(|t| t.account == acct) {
            t.attempts = t.attempts.saturating_add(1);
            if !success { t.failures = t.failures.saturating_add(1); }
            t.ghost_rate = if t.attempts > 0 { (t.failures as f64) / (t.attempts as f64) } else { 0.0 };
            if let Some(ms) = latency_ms {
                let ms = ms.clamp(self.floor_ms, self.ceil_ms);
                // Double-exponential-like smoothing
                t.ema = self.alpha * ms + (1.0 - self.alpha) * t.ema;
                let abs_dev = (ms - t.ema).abs();
                t.emv = self.beta * abs_dev + (1.0 - self.beta) * t.emv;
            }
        }
        self.recompute_weights_locked(&mut tips);
    }

    fn recompute_weights_locked(&self, tips: &mut [TipStats]) {
        // Weight ∝ 1 / (ema + k*emv), penalized by (1 - ghost_rate)^2
        let k = 2.0;
        let mut sum = 0.0;
        for t in tips.iter_mut() {
            let base = 1.0 / (t.ema + k * t.emv).max(1.0);
            let reliab = (1.0 - t.ghost_rate).max(0.0).powi(2);
            t.weight = (base * reliab).max(self.min_weight);
            sum += t.weight;
        }
        if sum > 0.0 { for t in tips.iter_mut() { t.weight /= sum; } }
    }

    pub async fn pick(&self) -> Pubkey {
        let tips = self.inner.read().await;
        if tips.is_empty() { return Pubkey::new_unique(); }
        let mut rng = thread_rng();
        let r: f64 = rng.gen();
        let mut acc = 0.0;
        for t in tips.iter() { acc += t.weight; if r <= acc { return t.account; } }
        tips.last().map(|t| t.account).unwrap_or_else(Pubkey::new_unique)
    }

    pub fn pick_sync(&self) -> Pubkey {
        let tips = self.inner.blocking_read();
        if tips.is_empty() { return Pubkey::new_unique(); }
        let mut rng = thread_rng();
        let r: f64 = rng.gen();
        let mut acc = 0.0;
        for t in tips.iter() { acc += t.weight; if r <= acc { return t.account; } }
        tips.last().map(|t| t.account).unwrap_or_else(Pubkey::new_unique)
    }
}

const SOLEND_PROGRAM_ID: &str = "So1endDq2YkqhipRh3WViPa8hdiSpxWy6z3Z6tMCpAo";
const KAMINO_PROGRAM_ID: &str = "KLend2g3cP87fffoy8q1mQqGKjrxjC8boSyAYavgmjD";
const PRIORITY_FEE_LAMPORTS: u64 = 50000;
const COMPUTE_UNITS: u32 = 400000;
const MIN_PROFIT_BPS: u64 = 10;
const REBALANCE_THRESHOLD_BPS: u64 = 25;
const MAX_SLIPPAGE_BPS: u64 = 50;

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
    wallet: Arc<Keypair>,
    markets: Arc<RwLock<HashMap<String, Vec<LendingMarket>>>>,
    active_positions: Arc<RwLock<HashMap<String, Position>>>,
    tip_router: Arc<RwLock<Option<TipRouter>>>,
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
    pub reserve: Pubkey,
    pub lending_market: Pubkey,
    pub lending_market_authority: Pubkey,
    pub liquidity_mint: Pubkey,
    pub liquidity_supply_vault: Pubkey,
    pub collateral_mint: Pubkey,
    pub collateral_supply_vault: Pubkey,
    pub program_id: Pubkey,
}

impl PortOptimizer {
    // ----- Liquidity-weighted market scoring (risk-aware) -----
    const UTIL_RISK_FACTOR: f64 = 120.0;       // weight of utilization penalty
    const CAPACITY_RISK_FACTOR: f64 = 0.000001; // penalty per lamport shortfall vs. move size

    pub fn market_score(
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
            wallet: Arc::new(wallet),
            markets: Arc::new(RwLock::new(HashMap::new())),
            active_positions: Arc::new(RwLock::new(HashMap::new())),
            tip_router: Arc::new(RwLock::new(None)),
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
        if *program_id == Pubkey::from_str(KAMINO_PROGRAM_ID).unwrap_or(Pubkey::default()) {
            Protocol::Kamino
        } else {
            Protocol::Solend
        }
    }

    // Compute precise repay amount from on-chain fee bps
    fn compute_flash_repay_amount(&self, amount: u64, fee_bps: u64) -> u64 {
        amount.saturating_add(((amount as u128) * (fee_bps as u128) / 10_000u128) as u64)
    }

    pub async fn execute_rebalance_flash_atomic(
        &self,
        position: &Position,
        src_meta: &ReserveMeta,
        dst_meta: &ReserveMeta,
        amount: u64,
        alts: &[AddressLookupTableAccount],
    ) -> Result<solana_sdk::signature::Signature> {
        let user = self.wallet.pubkey();
        let user_flash_ata = spl_associated_token_account::get_associated_token_address(&user, &position.token_mint);

        let plan = FlashPlan {
            flash_program: dst_meta.program_id,
            reserve: dst_meta.reserve,
            liquidity_supply_vault: dst_meta.liquidity_supply_vault,
            lending_market: dst_meta.lending_market,
            market_authority: dst_meta.lending_market_authority,
            token_mint: position.token_mint,
            amount,
        };

        let flash_start_ix = self.build_flash_loan_ix(&plan, user_flash_ata)?;
        // Estimate repay; ideally compute from on-chain reserve config
        let repay_amount = amount.saturating_add((amount as f64 * 0.0005) as u64);
        let flash_repay_ix = self.build_flash_repay_ix(&plan, user_flash_ata, repay_amount)?;

        let withdraw_old_ix = self.build_withdraw_instruction(position).await?;

        // Build deposit into destination protocol by constructing a minimal LM with correct protocol
        let dst_protocol = self.protocol_from_program_id(&dst_meta.program_id);
        let dst_stub = LendingMarket {
            market_pubkey: dst_meta.reserve,
            token_mint: position.token_mint,
            protocol: dst_protocol,
            supply_apy: 0.0,
            borrow_apy: 0.0,
            utilization: 0.0,
            liquidity_available: 0,
            last_update: Instant::now(),
            layout_version: 1,
        };
        let deposit_new_ix = self.build_deposit_instruction(position, &dst_stub).await?;

        let mut ixs = vec![flash_start_ix, withdraw_old_ix, deposit_new_ix, flash_repay_ix];
        let baseline_cu = self.optimize_compute_units(&ixs).await;
        let (cu_ix, fee_ix) = self.dynamic_budget_ixs(baseline_cu, PRIORITY_FEE_LAMPORTS).await;
        ixs.splice(0..0, [cu_ix, fee_ix]);

        let payer = self.wallet.pubkey();
        let build_with_hash = |bh: Hash| self.build_v0_tx(payer, &ixs, bh, alts);
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

        // Split across sources greedily
        let mut remaining = amount;
        let mut chunks: Vec<(ReserveMeta, u64)> = Vec::new();
        for src in flash_sources.iter().cloned() {
            if remaining == 0 { break; }
            let cap = src.available_flash_capacity()?;
            let take = remaining.min(cap);
            if take > 0 { chunks.push((src, take)); remaining -= take; }
        }
        if remaining > 0 {
            warn!(missing = remaining, "insufficient flash capacity across sources");
            return Err(anyhow!("insufficient flash capacity"));
        }

        // Build instructions
        let payer = self.wallet.pubkey();
        let mut ixs: Vec<Instruction> = Vec::new();
        let mut _repay_sum = 0u64;
        for (src, take) in chunks.iter() {
            let fee_bps = src.flash_fee_bps()?;
            let repay = self.compute_flash_repay_amount(*take, fee_bps);
            _repay_sum = _repay_sum.saturating_add(repay);
            let user_flash_ata = spl_associated_token_account::get_associated_token_address(&payer, &position.token_mint);
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

        // Core legs
        let withdraw_old = self.build_withdraw_instruction(position).await?;
        let dst_protocol = self.protocol_from_program_id(&dst_meta.program_id);
        let dst_stub = LendingMarket {
            market_pubkey: dst_meta.reserve,
            token_mint: position.token_mint,
            protocol: dst_protocol,
            supply_apy: 0.0,
            borrow_apy: 0.0,
            utilization: 0.0,
            liquidity_available: 0,
            last_update: Instant::now(),
            layout_version: 1,
        };
        let deposit_new = self.build_deposit_instruction(position, &dst_stub).await?;
        ixs.push(withdraw_old);
        ixs.push(deposit_new);

        // Repay legs
        for (src, take) in chunks.iter() {
            let fee_bps = src.flash_fee_bps()?;
            let repay = self.compute_flash_repay_amount(*take, fee_bps);
            let user_flash_ata = spl_associated_token_account::get_associated_token_address(&payer, &position.token_mint);
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

        // Assemble with autoscaled budget and latency‑weighted tip
        let tip_acc = tip_router.pick().await;
        let vtx = self.assemble_tx_v0(ixs, true, Some(tip_acc), &[]).await?;

        // Exact simulation safety
        if let Err(e) = self.simulate_exact_v0(&vtx).await {
            error!(?e, "flash rebalance sim failed");
            let ok_to_continue = breaker.record(false);
            if !ok_to_continue { return Err(anyhow!("breaker open: too many failures")); }
            return Err(anyhow!(format!("sim failed: {e}")));
        }

        // Send with slot guard and measure tip latency
        let t0 = Instant::now();
        let sig = self
            .send_v0_with_slot_guard(
                |bh| {
                    // Rebuild using same ixs pattern by reconstructing from vtx's message
                    // In practice, rebuild from the original ixs closure for determinism
                    let ixs_vec = vtx.message().instructions().to_vec();
                    self.build_v0_tx(self.wallet.pubkey(), &ixs_vec, bh, &[])
                },
                Duration::from_secs(45),
                200,
                120,
            )
            .await?;
        let ms = t0.elapsed().as_millis() as f64;
        obs.tip_latency_ms.observe(ms);
        tip_router.record_attempt(tip_acc, true, Some(ms)).await;
        info!(%sig, "flash rebalance executed");
        obs.rebalance_success.inc();
        Ok(sig)
    }
    }

    pub async fn build_bundle_from_plans(
        &self,
        plans: Vec<Vec<Instruction>>,
        alts: &[AddressLookupTableAccount],
        tip_account: Pubkey,
        tip_lamports: u64,
    ) -> Result<Vec<VersionedTransaction>> {
        self.ensure_tip_funds(tip_lamports).await?;
        let bh = self.get_recent_blockhash_with_retry().await?;
        let mut out = Vec::with_capacity(plans.len());
        for ixs in plans {
            let (cu_ix, fee_ix) = self.dynamic_budget_ixs(COMPUTE_UNITS, PRIORITY_FEE_LAMPORTS).await;
            let tip_ix = system_instruction::transfer(&self.wallet.pubkey(), &tip_account, tip_lamports);
            let full: Vec<Instruction> = [vec![cu_ix, fee_ix, tip_ix], ixs].concat();
            let vtx = self.build_v0_tx(self.wallet.pubkey(), &full, bh, alts)?;
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
        let mut interval = tokio::time::interval(Duration::from_secs(5));
        
        loop {
            interval.tick().await;
            
            if let Err(e) = self.optimize_cycle().await {
                eprintln!("Optimization cycle error: {e:?}");
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
        let program_id = Pubkey::from_str(SOLEND_PROGRAM_ID)?;
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
        let program_id = Pubkey::from_str(KAMINO_PROGRAM_ID)?;
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

    async fn parse_solend_reserve(&self, pubkey: &Pubkey, data: &[u8]) -> Result<LendingMarket> {
        if data.len() < 1024 { return Err(anyhow!(format!("solend reserve too small: {}", data.len()))); }
        let version = data[0];
        if version == 0 || version > 5 { return Err(anyhow!(format!("unexpected solend version: {}", version))); }
        let token_mint = Pubkey::new_from_array((*data.get(32..64).ok_or_else(|| anyhow!("mint slice"))?).try_into()?);
        let available_liquidity = u64::from_le_bytes((*data.get(64..72).ok_or_else(|| anyhow!("avail"))?).try_into()?);
        let borrowed_amount = u64::from_le_bytes((*data.get(72..80).ok_or_else(|| anyhow!("borrowed"))?).try_into()?);
        let supply_rate_raw = u128::from_le_bytes((*data.get(232..248).ok_or_else(|| anyhow!("supply"))?).try_into()?);
        let borrow_rate_raw = u128::from_le_bytes((*data.get(248..264).ok_or_else(|| anyhow!("borrow"))?).try_into()?);
        let total_liquidity = available_liquidity.saturating_add(borrowed_amount);
        let utilization = if total_liquidity > 0 { borrowed_amount as f64 / total_liquidity as f64 } else { 0.0 };
        let supply_apy = self.apy_continuous_from_rate_per_slot_1e18(supply_rate_raw).await?;
        let borrow_apy = self.apy_continuous_from_rate_per_slot_1e18(borrow_rate_raw).await?;
        Ok(LendingMarket { market_pubkey: *pubkey, token_mint, protocol: Protocol::Solend, supply_apy, borrow_apy, utilization, liquidity_available: available_liquidity, last_update: Instant::now(), layout_version: version })
    }

    async fn parse_kamino_market(&self, pubkey: &Pubkey, data: &[u8]) -> Result<LendingMarket> {
        if data.len() < 1024 { return Err(anyhow!(format!("kamino market too small: {}", data.len()))); }
        let version = data[0];
        if version == 0 || version > 10 { return Err(anyhow!(format!("unexpected kamino version: {}", version))); }
        let token_mint = Pubkey::new_from_array((*data.get(32..64).ok_or_else(|| anyhow!("mint slice"))?).try_into()?);
        let available_liquidity = u64::from_le_bytes((*data.get(80..88).ok_or_else(|| anyhow!("avail"))?).try_into()?);
        let borrowed_amount = u64::from_le_bytes((*data.get(88..96).ok_or_else(|| anyhow!("borrowed"))?).try_into()?);
        let supply_rate_raw = u128::from_le_bytes((*data.get(296..312).ok_or_else(|| anyhow!("supply"))?).try_into()?);
        let borrow_rate_raw = u128::from_le_bytes((*data.get(312..328).ok_or_else(|| anyhow!("borrow"))?).try_into()?);
        let total_liquidity = available_liquidity.saturating_add(borrowed_amount);
        let utilization = if total_liquidity > 0 { borrowed_amount as f64 / total_liquidity as f64 } else { 0.0 };
        let supply_apy = self.apy_continuous_from_rate_per_slot_1e18(supply_rate_raw).await?;
        let borrow_apy = self.apy_continuous_from_rate_per_slot_1e18(borrow_rate_raw).await?;
        Ok(LendingMarket { market_pubkey: *pubkey, token_mint, protocol: Protocol::Kamino, supply_apy, borrow_apy, utilization, liquidity_available: available_liquidity, last_update: Instant::now(), layout_version: version })
    }

    // Estimate slots/year from on-chain timing, with safe fallback
    async fn estimate_slots_per_year(&self) -> f64 {
        let sample = 10_000u64;
        if let (Ok(end_slot), Ok(start_slot)) = (
            self.rpc_client.get_slot().await,
            self.rpc_client.get_slot().await.map(|s| s.saturating_sub(sample)),
        ) {
            if let (Ok(t1_opt), Ok(t0_opt)) = (
                self.rpc_client.get_block_time(end_slot).await,
                self.rpc_client.get_block_time(start_slot).await,
            ) {
                if let (Some(t1), Some(t0)) = (t1_opt, t0_opt) {
                    let dt = (t1 - t0).max(1) as f64;
                    let slots = (end_slot - start_slot).max(1) as f64;
                    let slot_time = dt / slots; // seconds per slot
                    let sp_year = 31_536_000.0 / slot_time;
                    if sp_year.is_finite() && sp_year > 0.0 && sp_year < 200_000_000.0 {
                        return sp_year;
                    }
                }
            }
        }
        63_072_000.0
    }

    pub async fn apy_continuous_from_rate_per_slot_1e18(&self, rate_per_slot_1e18: u128) -> Result<f64> {
        let r = (rate_per_slot_1e18 as f64) / 1e18;
        if !(r >= 0.0 && r.is_finite()) {
            return Err(anyhow!(format!("invalid per-slot rate: {}", r)));
        }
        let spw = self.estimate_slots_per_year().await;
        let apy = (r * spw).exp() - 1.0; // continuous compounding
        let apy = apy.clamp(-0.5, 10.0); // [-50%, +1000%]
        if !apy.is_finite() { return Err(anyhow!("apy non-finite")); }
        Ok(apy * 100.0)
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

    async fn find_arbitrage_opportunity(
        &self,
        position: &Position,
        markets: &[LendingMarket],
    ) -> Result<Option<LendingMarket>> {
        let current_market = markets.iter()
            .find(|m| m.protocol == position.protocol)
            .ok_or_else(|| anyhow!("Current market not found"))?;
        
        let best_alternative = self.pick_best_market_weighted(
            position.amount,
            &markets
                .iter()
                .filter(|m| m.protocol != position.protocol)
                .cloned()
                .collect::<Vec<_>>()
        );
        
        if let Some(alt_market) = best_alternative.as_ref() {
            let apy_diff_bps = ((alt_market.supply_apy - current_market.supply_apy) * 100.0) as u64;
            
            if apy_diff_bps > REBALANCE_THRESHOLD_BPS {
                // Build the candidate withdraw + deposit instructions to get a realistic CU-based gas estimate
                let withdraw_ix = self.build_withdraw_instruction(position).await?;
                let deposit_ix = self.build_deposit_instruction(position, alt_market).await?;
                let gas_cost = self
                    .estimate_rebalance_cost_lamports(&[withdraw_ix, deposit_ix], 1.0)
                    .await
                    .unwrap_or(PRIORITY_FEE_LAMPORTS);

                let expected_profit = self.calculate_expected_profit(
                    position.amount,
                    current_market.supply_apy,
                    alt_market.supply_apy,
                    gas_cost,
                );
                
                if expected_profit > MIN_PROFIT_BPS as f64 {
                    return Ok(best_alternative);
                }
            }
        }
        
        Ok(None)
    }

    async fn estimate_rebalance_cost(&self, amount: u64) -> Result<u64> {
        let base_fee = PRIORITY_FEE_LAMPORTS * 2;
        let size_multiplier = (amount as f64 / 1e9).max(1.0);
        Ok((base_fee as f64 * size_multiplier) as u64)
    }

    // Realistic gas cost estimation from sim + fee percentiles (CU -> lamports)
    pub async fn estimate_rebalance_cost_lamports(
        &self,
        ixs: &[Instruction],
        tip_multiplier: f64,
    ) -> Result<u64> {
        // 1) Quick sim to get units_consumed using a legacy message (sufficient for CU estimate)
        let payer = self.wallet.pubkey();
        let legacy_tx = Transaction::new_unsigned(Message::new(ixs, Some(&payer)));
        let units = match self.rpc_client.simulate_transaction(&legacy_tx).await {
            Ok(sim) => sim.value.units_consumed.unwrap_or(COMPUTE_UNITS) as u64,
            Err(_) => COMPUTE_UNITS as u64,
        };

        // 2) Recent prioritization fees; choose p75 for high landing probability
        let mut cu_price = PRIORITY_FEE_LAMPORTS; // floor
        if let Ok(fees) = self.rpc_client.get_recent_prioritization_fees(&[]).await {
            if !fees.is_empty() {
                let mut vals: Vec<u64> = fees.iter().map(|f| f.prioritization_fee).collect();
                vals.sort_unstable();
                let p50 = vals[vals.len() / 2];
                let p75 = vals[(vals.len() * 75 / 100).min(vals.len() - 1)];
                cu_price = p75.max(p50).max(PRIORITY_FEE_LAMPORTS);
            }
        }

        // 3) Convert CU to lamports
        let mut lamports = units.saturating_mul(cu_price);

        // 4) Add a Jito tip based on congestion (optional)
        let tip = self.dynamic_priority_fee().await.unwrap_or(PRIORITY_FEE_LAMPORTS);
        lamports = lamports.saturating_add(((tip as f64) * tip_multiplier) as u64);

        // 5) Add a small safety margin (e.g., 5%)
        lamports = (lamports as f64 * 1.05).ceil() as u64;
        Ok(lamports)
    }

    pub async fn assemble_tx_v0(
        &self,
        mut ixs: Vec<Instruction>,
        include_tip: bool,
        tip_account: Option<Pubkey>,
        alts: &[AddressLookupTableAccount],
    ) -> Result<VersionedTransaction> {
        let baseline_cu = self.optimize_compute_units_rough(&ixs).await;
        let cu_price = self.priority_fee_autoscale(PRIORITY_FEE_LAMPORTS).await;
        let cu_ix = ComputeBudgetInstruction::set_compute_unit_limit(baseline_cu.min(1_400_000));
        let fee_ix = ComputeBudgetInstruction::set_compute_unit_price(cu_price);
        let mut pref = vec![cu_ix, fee_ix];
        if include_tip {
            if let Some(tip_acc) = tip_account {
                let tip = self.dynamic_priority_fee().await.unwrap_or(PRIORITY_FEE_LAMPORTS);
                pref.push(system_instruction::transfer(&self.wallet.pubkey(), &tip_acc, tip));
            }
        }
        ixs.splice(0..0, pref);
        ixs = Self::dedup_budget_and_tip(ixs);
        let bh = self.get_recent_blockhash_with_retry().await?;
        self.build_v0_tx(self.wallet.pubkey(), &ixs, bh, alts)
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
                let program_id = Pubkey::from_str(SOLEND_PROGRAM_ID)?;
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
                let program_id = Pubkey::from_str(KAMINO_PROGRAM_ID)?;
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
                let program_id = Pubkey::from_str(SOLEND_PROGRAM_ID)?;
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
                let program_id = Pubkey::from_str(KAMINO_PROGRAM_ID)?;
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
    pub async fn get_fresh_blockhash(&self) -> Result<(Hash, u64)> {
        let bh = self.rpc_client.get_latest_blockhash().await?;
        let height = self.rpc_client.get_block_height().await?;
        Ok((bh, height))
    }

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

    async fn get_token_balance(&self, mint: &Pubkey) -> Result<u64> {
        let token_account = spl_associated_token_account::get_associated_token_address(
            &self.wallet.pubkey(),
            mint,
        );
        
        match self.rpc_client.get_account(&token_account).await {
            Ok(account) => {
                let token_account_data = TokenAccount::unpack(&account.data)?;
                Ok(token_account_data.amount)
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
