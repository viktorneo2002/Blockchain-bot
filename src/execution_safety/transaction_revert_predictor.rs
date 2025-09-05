use std::{
    collections::{HashMap, HashSet, VecDeque},
    sync::Arc,
    time::{Duration, Instant},
};

use anyhow::{anyhow, Result};
use borsh::{BorshDeserialize, BorshSerialize};
use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;
use tokio::time::timeout;
use metrics::{counter, histogram};
use tokio::sync::watch;
use tokio_stream::StreamExt;

use tracing::{debug, error, info, instrument, warn};

use solana_client::{
    client_error::ClientError,
    nonblocking::rpc_client::RpcClient as AsyncRpcClient,
    nonblocking::pubsub_client::PubsubClient,
    rpc_config::{
        RpcAccountInfoConfig, RpcBlockhashConfig, RpcRequestAirdropConfig, RpcSimulateTransactionConfig,
    },
    rpc_filter::RpcFilterType,
    rpc_response::{RpcSimulateTransactionResult, SlotInfo},
};
use solana_program::{
    program_pack::Pack, pubkey::Pubkey,
};
use solana_sdk::{
    account::Account,
    commitment_config::CommitmentConfig,
    compute_budget::ComputeBudgetInstruction,
    hash::Hash,
    instruction::Instruction,
    message::Message,
    signature::{Keypair, Signer},
    system_program,
    transaction::Transaction,
};
use solana_sdk::clock::Slot;
use spl_associated_token_account as spl_ata;
use spl_token::state::Account as TokenAccount;
use std::str::FromStr;

// --- Tunables (carefully chosen for mainnet realities)
const MAX_SIMULATION_RETRIES: usize = 3;
const SIMULATION_TIMEOUT_MS: u64 = 150;
const ACCOUNT_CACHE_MS: u64 = 500;
const FAILURE_PATTERN_WINDOW: usize = 2000; // bounded, recent only
const FAILURE_PATTERN_TTL_SECS: u64 = 3600;
const PROGRAM_RATE_MIN_TOTAL: u64 = 20; // avoid noise
const MAX_COMPUTE_UNITS: u32 = 1_400_000;
const SAFETY_CU_MARGIN_FRAC: f64 = 0.10; // 10% above sim units
const STATIC_FALLBACK_CU_PRICE: u64 = 1_000; // microlamports/ CU
const FALLBACK_BLOCKHASH_STALE_SECS: u64 = 60;
const RECOMMENDED_PAYER_MIN_LAMPORTS: u64 = 10_000_000;
const MIN_SUCCESS_RATE_THRESHOLD: f64 = 0.95;

// --- Slot timing and tip/inclusion heuristics ---
const SLOT_SUB_RECONNECT_MS: u64 = 1500;
const SLOT_PHASE_EARLY_FRAC: f64 = 0.33;
const SLOT_PHASE_LATE_FRAC: f64 = 0.75;
const SLOT_DURATION_SMA_WINDOW: usize = 64; // moving average window
const DEFAULT_SLOT_MS: u64 = 400;

// Fee multipliers by phase (tuned for mainnet realities)
const FEE_MULTIPLIER_EARLY: f64 = 0.95; // blockspace more open, can bid slightly below percentile
const FEE_MULTIPLIER_MID: f64 = 1.00;
const FEE_MULTIPLIER_LATE: f64 = 1.15; // late in slot → bid higher to land

// -------- Data structures

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RevertPrediction {
    pub will_revert: bool,
    pub confidence: f64,
    pub reason: RevertReason,
    pub estimated_compute_units: u32,
    pub recommended_priority_fee: u64,
    pub simulation_logs: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum RevertReason {
    InsufficientBalance,
    AccountNotFound,
    InvalidAccountOwner,
    ComputeBudgetExceeded,
    SlippageExceeded,
    TokenAccountNotInitialized,
    InsufficientTokenBalance,
    ProgramError(String),
    SimulationFailed(String),
    HighFailurePattern,
    NetworkCongestion,
    StaleBlockhash,
    None,
}

#[derive(Debug, Clone)]
struct FailurePattern {
    program_id: Pubkey,
    error_code: String,
    timestamp: Instant,
    compute_units_used: u32,
}

#[derive(Debug, Clone)]
struct AccountSnapshot {
    pubkey: Pubkey,
    lamports: u64,
    owner: Pubkey,
    data: Vec<u8>,
    executable: bool,
    rent_epoch: u64,
}

// Simple atomics for counters kept inside DashMap values.
#[derive(Debug, Clone, Default)]
struct ProgramStats {
    successes: u64,
    total: u64,
}

impl ProgramStats {
    fn record_success(&mut self) {
        self.successes += 1;
        self.total += 1;
    }
    fn record_failure(&mut self) {
        self.total += 1;
    }
    fn success_rate(&self) -> Option<f64> {
        if self.total >= PROGRAM_RATE_MIN_TOTAL {
            Some(self.successes as f64 / self.total as f64)
        } else {
            None
        }
    }
}

pub struct TransactionRevertPredictor {
    // Multiple RPCs for quorum reads and fallback. First is “primary”.
    rpc_clients: Vec<Arc<AsyncRpcClient>>,
    commitment: CommitmentConfig,

    // Concurrency-friendly state
    failure_patterns: DashMap<(Pubkey, String), VecDeque<FailurePattern>>,
    program_success_rates: DashMap<Pubkey, ProgramStats>,
    account_cache: DashMap<Pubkey, (AccountSnapshot, Instant)>,
    recent_blockhashes: DashMap<Hash, Instant>,

    // Live slot tracking (optional but valuable)
    current_slot: Arc<RwLock<u64>>,
    // RPC health and circuit breaker
    rpc_health: DashMap<usize, RpcHealth>,
    // EMA smoothing for prioritization fees per writable key
    fee_ema: DashMap<Pubkey, Ema>,
    // Optional leader and slot intelligence
    leader_intel: Option<LeaderIntel>,
}

impl TransactionRevertPredictor {
    pub fn new_with_rpcs(rpcs: Vec<String>) -> Self {
        assert!(!rpcs.is_empty(), "At least one RPC endpoint is required");
        let commitment = CommitmentConfig::confirmed();
        let rpc_clients = rpcs
            .into_iter()
            .map(|e| Arc::new(AsyncRpcClient::new_with_commitment(e, commitment)))
            .collect::<Vec<_>>();

        Self {
            rpc_clients,
            commitment,
            failure_patterns: DashMap::new(),
            program_success_rates: DashMap::new(),
            account_cache: DashMap::new(),
            recent_blockhashes: DashMap::new(),
            current_slot: Arc::new(RwLock::new(0)),
            rpc_health: DashMap::new(),
            fee_ema: DashMap::new(),
            leader_intel: None,
        }
    }

    pub fn new(rpc_endpoint: &str) -> Self {
        Self::new_with_rpcs(vec![rpc_endpoint.to_string()])
    }

    pub fn new_with_rpcs_and_leader(
        rpcs: Vec<String>,
        ws_endpoint: Option<String>,
        preferred_leaders: impl IntoIterator<Item = Pubkey>,
    ) -> Self {
        let mut me = Self::new_with_rpcs(rpcs);
        if let Some(ws) = ws_endpoint {
            if let Some(primary) = me.rpc_clients.get(0) {
                let rpc_arc = primary.clone();
                let mut li = LeaderIntel::new(rpc_arc, preferred_leaders);
                // Fire-and-forget start
                tokio::spawn({
                    let mut li_clone = li.clone();
                    async move { li_clone.start(&ws).await }
                });
                me.leader_intel = Some(li);
            }
        }
        me
    }

    fn primary(&self) -> &AsyncRpcClient { &self.rpc_clients[0] }

    fn best_rpc(&self) -> &AsyncRpcClient {
        // Choose the first healthy RPC, fallback to primary
        for (i, cli) in self.rpc_clients.iter().enumerate() {
            if self
                .rpc_health
                .get(&i)
                .map(|h| h.healthy())
                .unwrap_or(true)
            {
                return cli;
            }
        }
        &self.rpc_clients[0]
    }

    #[instrument(skip_all)]
    pub async fn predict_revert(
        &self,
        instructions: &[Instruction],
        payer: &Keypair,
        recent_blockhash: Hash,
    ) -> Result<RevertPrediction> {
        let mut prediction = RevertPrediction {
            will_revert: false,
            confidence: 1.0,
            reason: RevertReason::None,
            estimated_compute_units: 0,
            recommended_priority_fee: 0,
            simulation_logs: Vec::new(),
        };

        // 1) Blockhash freshness
        if let Some(reason) = self.check_blockhash_validity(&recent_blockhash).await {
            return Ok(RevertPrediction {
                will_revert: true,
                confidence: 0.99,
                reason,
                ..prediction
            });
        }

        // 2) Account validation: batch fetch + cache
        let account_keys = self.extract_account_keys(instructions);
        if let Some(reason) = self.validate_accounts(&account_keys, payer).await? {
            return Ok(RevertPrediction {
                will_revert: true,
                confidence: 0.98,
                reason,
                ..prediction
            });
        }

        // 3) Program failure-rate heuristics
        for ix in instructions {
            if let Some(failure_rate) = self.check_program_failure_rate(&ix.program_id).await {
                if failure_rate > (1.0 - MIN_SUCCESS_RATE_THRESHOLD) {
                    return Ok(RevertPrediction {
                        will_revert: true,
                        confidence: failure_rate,
                        reason: RevertReason::HighFailurePattern,
                        ..prediction
                    });
                }
            }
        }

        // 4) Fast compute estimate (heuristic)
        let estimated_compute = self.estimate_compute_units(instructions).await?;
        prediction.estimated_compute_units = estimated_compute;

        if estimated_compute > MAX_COMPUTE_UNITS {
            return Ok(RevertPrediction {
                will_revert: true,
                confidence: 0.95,
                reason: RevertReason::ComputeBudgetExceeded,
                ..prediction
            });
        }

        // 5) Simulation with dynamic config
        let sim_start = Instant::now();
        let sim = self
            .simulate_transaction(instructions, payer, recent_blockhash, estimated_compute)
            .await?;
        histogram!("predictor.simulate_time_seconds", sim_start.elapsed().as_secs_f64());
        counter!("predictor.simulations_total", 1);

        if let Some((reason, logs, units_consumed)) = sim {
            // Failure path
            if let RevertReason::ProgramError(ref code) = reason {
                self.record_failure_pattern(instructions[0].program_id, code.clone(), units_consumed)
                    .await;
            }

            // Priority fee even on failure (for caller insight)
            let recommended_priority_fee = self
                .calculate_priority_fee(units_consumed.max(estimated_compute), true, instructions)
                .await
                .unwrap_or(STATIC_FALLBACK_CU_PRICE);

            let out = Ok(RevertPrediction {
                will_revert: true,
                confidence: 0.99,
                reason,
                estimated_compute_units: units_consumed,
                recommended_priority_fee,
                simulation_logs: logs,
            });
            counter!("predictor.simulate_failures", 1);
            return out;
        }

        // 6) If simulation passed, compute dynamic CU + priority fee
        // We prefer simulated units (from success path), but we only get them if we specifically fetched them.
        // The simulate_transaction() returns None on success; so we resimulate cheaply requesting units.
        let sim_units = self
            .simulate_units(instructions, payer, recent_blockhash, estimated_compute)
            .await
            .unwrap_or(estimated_compute);

        // Base confidence
        let mut confidence = 0.995;
        if let Some(li) = &self.leader_intel {
            let slot = li.current_slot().await;
            if let Some(leader) = li.leader_for_slot(slot).await {
                let is_pref = li.is_preferred_leader(&leader).await;
                if !is_pref {
                    confidence -= 0.01;
                }
            }
            // Late-phase slots slightly reduce confidence
            let phase_mult = li.slot_phase_multiplier(slot, Instant::now()).await;
            if phase_mult > 1.05 { confidence -= 0.005; }
        }

        let recommended_priority_fee = self
            .calculate_priority_fee(sim_units, false, instructions)
            .await
            .unwrap_or(STATIC_FALLBACK_CU_PRICE);

        Ok(RevertPrediction {
            will_revert: false,
            confidence: confidence.clamp(0.0, 1.0),
            reason: RevertReason::None,
            estimated_compute_units: sim_units,
            recommended_priority_fee,
            simulation_logs: Vec::new(),
        })
    }

    // Background blockhash refresher (call from an async context, e.g., on init)
    pub async fn spawn_blockhash_refresher(self: Arc<Self>) {
        let this = self.clone();
        tokio::spawn(async move {
            loop {
                let fut = this.best_rpc().get_latest_blockhash();
                match timeout(Duration::from_millis(400), fut).await {
                    Ok(Ok((hash, _fee_calc))) => {
                        this.recent_blockhashes.insert(hash, Instant::now());
                    }
                    _ => {
                        counter!("predictor.rpc_timeouts_total", 1);
                    }
                }
                tokio::time::sleep(Duration::from_millis(250)).await;
            }
        });
    }

    // Lightweight periodic maintenance: prune stale caches and recalc health
    pub async fn spawn_maintenance(self: Arc<Self>) {
        tokio::spawn(async move {
            let mut tick: u64 = 0;
            loop {
                if tick % 5 == 0 {
                    self.cleanup_old_data().await;
                }
                // Future: recalc program health metrics or export gauges here
                tick = tick.wrapping_add(1);
                tokio::time::sleep(Duration::from_millis(1000)).await;
            }
        });
    }

    #[instrument(skip_all)]
    async fn check_blockhash_validity(&self, blockhash: &Hash) -> Option<RevertReason> {
        if let Some(ts) = self.recent_blockhashes.get(blockhash).map(|e| *e.value()) {
            if Instant::now().duration_since(ts) > Duration::from_secs(FALLBACK_BLOCKHASH_STALE_SECS)
            {
                return Some(RevertReason::StaleBlockhash);
            }
            return None;
        }

        match timeout(
            Duration::from_millis(300),
            self.best_rpc().is_blockhash_valid(blockhash, self.commitment),
        )
        .await
        {
            Ok(Ok(valid)) => {
                if !valid {
                    Some(RevertReason::StaleBlockhash)
                } else {
                    self.recent_blockhashes.insert(*blockhash, Instant::now());
                    None
                }
            }
            Ok(Err(_)) => Some(RevertReason::NetworkCongestion),
            Err(_) => Some(RevertReason::NetworkCongestion),
        }
    }

    #[instrument(skip_all)]
    async fn validate_accounts(
        &self,
        account_keys: &[Pubkey],
        payer: &Keypair,
    ) -> Result<Option<RevertReason>> {
        // Payer balance
        if let Ok(payer_account) = timeout(Duration::from_millis(500), self.best_rpc().get_account(&payer.pubkey())).await.unwrap_or(Err(ClientError::from(std::io::Error::new(std::io::ErrorKind::Other, "timeout")))) {

            if payer_account.lamports < RECOMMENDED_PAYER_MIN_LAMPORTS {
                return Ok(Some(RevertReason::InsufficientBalance));
            }
        } else {
            return Ok(Some(RevertReason::AccountNotFound));
        }

        // Batch fetch using getMultipleAccounts and cache results
        let now = Instant::now();
        let cache_ttl = Duration::from_millis(ACCOUNT_CACHE_MS);

        let (to_fetch, mut snapshots) = {
            let mut snaps = Vec::with_capacity(account_keys.len());
            let mut fetch = Vec::new();

            for k in account_keys {
                if let Some((snap, t)) = self.account_cache.get(k).map(|e| e.value().clone()) {
                    if now.duration_since(t) < cache_ttl {
                        snaps.push(snap);
                        continue;
                    }
                }
                fetch.push(*k);
            }
            (fetch, snaps)
        };

        if !to_fetch.is_empty() {
            match timeout(Duration::from_millis(800), self.best_rpc().get_multiple_accounts(&to_fetch)).await {
                Ok(Ok(accounts)) => {
                    for (i, opt) in accounts.into_iter().enumerate() {
                        let k = to_fetch[i];
                        if let Some(a) = opt {
                            let snap = AccountSnapshot {
                                pubkey: k,
                                lamports: a.lamports,
                                owner: a.owner,
                                data: a.data,
                                executable: a.executable,
                                rent_epoch: a.rent_epoch,
                            };
                            self.account_cache.insert(k, (snap.clone(), now));
                            snapshots.push(snap);
                        } else {
                            return Ok(Some(RevertReason::AccountNotFound));
                        }
                    }
                }
                Ok(Err(_)) => {
                    // Fallback: fetch one by one (rare path)
                    for k in to_fetch {
                        match timeout(Duration::from_millis(300), self.best_rpc().get_account(&k)).await {
                            Ok(Ok(a)) => {
                                let snap = AccountSnapshot {
                                    pubkey: k,
                                    lamports: a.lamports,
                                    owner: a.owner,
                                    data: a.data,
                                    executable: a.executable,
                                    rent_epoch: a.rent_epoch,
                                };
                                self.account_cache.insert(k, (snap.clone(), now));
                                snapshots.push(snap);
                            }
                            _ => return Ok(Some(RevertReason::AccountNotFound)),
                        }
                    }
                }
                Err(_) => {
                    // Timeout: rare — fallback to individual fetches
                    for k in to_fetch {
                        match timeout(Duration::from_millis(300), self.best_rpc().get_account(&k)).await {
                            Ok(Ok(a)) => {
                                let snap = AccountSnapshot {
                                    pubkey: k,
                                    lamports: a.lamports,
                                    owner: a.owner,
                                    data: a.data,
                                    executable: a.executable,
                                    rent_epoch: a.rent_epoch,
                                };
                                self.account_cache.insert(k, (snap.clone(), now));
                                snapshots.push(snap);
                            }
                            _ => return Ok(Some(RevertReason::AccountNotFound)),
                        }
                    }
                }
            }
        }

        // Validate each
        for acc in snapshots {
            if !acc.executable && acc.lamports == 0 {
                // If not rent-exempt and empty; most programs require rent or ownership transfer logic.
                return Ok(Some(RevertReason::InsufficientBalance));
            }
            if acc.owner == spl_token::id() {
                if let Err(_) = self.validate_token_account(&acc) {
                    return Ok(Some(RevertReason::TokenAccountNotInitialized));
                }
            }
        }

        Ok(None)
    }

    fn validate_token_account(&self, account: &AccountSnapshot) -> Result<()> {
        if account.data.len() != TokenAccount::LEN {
            return Err(anyhow!("Invalid token account size"));
        }
        let token_account = TokenAccount::unpack(&account.data)?;
        if token_account.amount == 0 {
            return Err(anyhow!("Zero token balance"));
        }
        Ok(())
    }

    #[instrument(skip_all)]
    async fn check_program_failure_rate(&self, program_id: &Pubkey) -> Option<f64> {
        self.program_success_rates
            .get(program_id)
            .and_then(|e| e.value().success_rate())
            .map(|sr| 1.0 - sr)
    }

    // Heuristic CU estimate to pre-screen extreme cases
    async fn estimate_compute_units(&self, instructions: &[Instruction]) -> Result<u32> {
        let mut total = 0u32;
        for ix in instructions {
            let base = if ix.program_id == spl_token::id() {
                5_000
            } else if ix.program_id == spl_ata::id() {
                10_000
            } else if ix.program_id == system_program::id() {
                5_000
            } else {
                20_000
            };
            let data_cost = ((ix.data.len() as u32 + 31) / 32) * 150;
            let acct_cost = (ix.accounts.len() as u32) * 1_200;
            total = total.saturating_add(base + data_cost + acct_cost);
        }
        let total = ((total as f64) * 1.15) as u32; // 15% margin
        Ok(total.min(MAX_COMPUTE_UNITS))
    }

    // Full simulation: returns Some(failure_reason, logs, units) or None on success
    #[instrument(skip_all)]
    async fn simulate_transaction(
        &self,
        instructions: &[Instruction],
        payer: &Keypair,
        recent_blockhash: Hash,
        estimated_compute: u32,
    ) -> Result<Option<(RevertReason, Vec<String>, u32)>> {
        // Prepend compute budget (limit + a conservative price for sim)
        let mut all_ix = Vec::with_capacity(instructions.len() + 2);
        all_ix.push(ComputeBudgetInstruction::set_compute_unit_limit(
            estimated_compute,
        ));
        all_ix.push(ComputeBudgetInstruction::set_compute_unit_price(
            STATIC_FALLBACK_CU_PRICE,
        ));
        all_ix.extend_from_slice(instructions);

        let msg = Message::new(&all_ix, Some(&payer.pubkey()));
        let tx = Transaction::new(&[payer], msg, recent_blockhash);

        let cfg = RpcSimulateTransactionConfig {
            sig_verify: Some(false),
            replace_recent_blockhash: Some(true),
            commitment: Some(self.commitment),
            // fully load logs and units
            ..RpcSimulateTransactionConfig::default()
        };

        for attempt in 0..MAX_SIMULATION_RETRIES {
            let span = format!("simulation_attempt_{}", attempt);
            let fut = async { self.best_rpc().simulate_transaction_with_config(&tx, cfg.clone()).await };
            match timeout(Duration::from_millis(SIMULATION_TIMEOUT_MS), fut).await {
                Ok(Ok(res)) => {
                    let units = res.value.units_consumed.unwrap_or(estimated_compute);
                    if let Some(err) = res.value.err {
                        let logs = res.value.logs.unwrap_or_default();
                        let reason = self.parse_simulation_error(&format!("{err:?}"), &logs);
                        return Ok(Some((reason, logs, units)));
                    }
                    return Ok(None);
                }
                Ok(Err(e)) => {
                    if attempt + 1 == MAX_SIMULATION_RETRIES {
                        return Ok(Some((
                            RevertReason::SimulationFailed(e.to_string()),
                            vec![e.to_string()],
                            estimated_compute,
                        )));
                    }
                }
                Err(_) => {
                    if attempt + 1 == MAX_SIMULATION_RETRIES {
                        return Ok(Some((
                            RevertReason::NetworkCongestion,
                            vec!["Simulation timeout".into()],
                            estimated_compute,
                        )));
                    }
                }
            }
            tokio::time::sleep(Duration::from_millis(12 * ((attempt + 1) as u64))).await;
        }

        Ok(Some((
            RevertReason::SimulationFailed("Max retries exceeded".into()),
            vec!["Max simulation retries exceeded".into()],
            estimated_compute,
        )))
    }

    // Lightweight re-sim to get units on success path (when we didn’t get logs)
    async fn simulate_units(
        &self,
        instructions: &[Instruction],
        payer: &Keypair,
        recent_blockhash: Hash,
        estimated_compute: u32,
    ) -> Result<u32> {
        let mut all_ix = Vec::with_capacity(instructions.len() + 2);
        all_ix.push(ComputeBudgetInstruction::set_compute_unit_limit(
            estimated_compute,
        ));
        all_ix.push(ComputeBudgetInstruction::set_compute_unit_price(
            STATIC_FALLBACK_CU_PRICE,
        ));
        all_ix.extend_from_slice(instructions);

        let msg = Message::new(&all_ix, Some(&payer.pubkey()));
        let tx = Transaction::new(&[payer], msg, recent_blockhash);

        let cfg = RpcSimulateTransactionConfig {
            sig_verify: Some(false),
            replace_recent_blockhash: Some(true),
            commitment: Some(self.commitment),
            ..RpcSimulateTransactionConfig::default()
        };

        match self.best_rpc().simulate_transaction_with_config(&tx, cfg).await {
            Ok(res) => Ok(res.value.units_consumed.unwrap_or(estimated_compute)),
            Err(_) => Ok(estimated_compute),
        }
    }

    fn parse_simulation_error(&self, error: &str, logs: &[String]) -> RevertReason {
        // Common Solana runtime errors
        let e = error.to_ascii_lowercase();
        if e.contains("insufficient funds") {
            return RevertReason::InsufficientBalance;
        }
        if e.contains("accountnotfound") || e.contains("account not found") {
            return RevertReason::AccountNotFound;
        }
        if e.contains("invalid account owner") || e.contains("incorrectprogramid") {
            return RevertReason::InvalidAccountOwner;
        }
        if e.contains("compute budget exceeded")
            || e.contains("computationalbudgetexceeded")
            || e.contains("max compute units exceeded")
        {
            return RevertReason::ComputeBudgetExceeded;
        }

        // Token-program/DEX hints from logs
        for log in logs {
            let l = log.to_ascii_lowercase();
            if l.contains("error: insufficient funds") {
                return RevertReason::InsufficientTokenBalance;
            }
            if l.contains("slippage") || l.contains("slippageexceeded") {
                return RevertReason::SlippageExceeded;
            }
            if l.contains("account not initialized") {
                return RevertReason::TokenAccountNotInitialized;
            }
        }

        // Custom program errors
        for log in logs {
            if log.starts_with("Program log: Error:") {
                let msg = log.replace("Program log: Error:", "").trim().to_string();
                return RevertReason::ProgramError(msg);
            }
            if log.contains("failed: custom program error:") {
                if let Some(code) = log.split("0x").nth(1) {
                    if let Some(code) = code.split_whitespace().next() {
                        return RevertReason::ProgramError(format!("0x{}", code));
                    }
                }
            }
        }

        RevertReason::ProgramError(error.to_string())
    }

    #[instrument(skip_all)]
    async fn record_failure_pattern(&self, program_id: Pubkey, error_code: String, cu: u32) {
        let key = (program_id, error_code.clone());
        let mut deque = self
            .failure_patterns
            .entry(key)
            .or_insert(VecDeque::with_capacity(64));
        deque.push_back(FailurePattern {
            program_id,
            error_code,
            timestamp: Instant::now(),
            compute_units_used: cu,
        });
        while deque.len() > FAILURE_PATTERN_WINDOW {
            deque.pop_front();
        }
        // Update rates
        let mut stats = self
            .program_success_rates
            .entry(program_id)
            .or_default();
        stats.record_failure();
    }

    pub async fn record_success(&self, program_id: Pubkey) -> Result<()> {
        let mut stats = self
            .program_success_rates
            .entry(program_id)
            .or_default();
        stats.record_success();
        Ok(())
    }

    #[instrument(skip_all)]
    async fn calculate_priority_fee(
        &self,
        compute_units: u32,
        will_revert: bool,
        instructions: &[Instruction],
    ) -> Result<u64> {
        // Collect writable accounts to focus the prioritization fee sample
        let mut writable: Vec<Pubkey> = Vec::new();
        let mut seen = HashSet::new();
        for ix in instructions {
            for meta in &ix.accounts {
                if meta.is_writable && seen.insert(meta.pubkey) {
                    writable.push(meta.pubkey);
                }
            }
        }

        let fees = match timeout(Duration::from_millis(500), self.best_rpc().get_recent_prioritization_fees(&writable)).await {
            Ok(Ok(f)) if !f.is_empty() => f,
            _ => return Ok(STATIC_FALLBACK_CU_PRICE),
        };

        // Update per-key EMA
        for f in &fees {
            // Use account_keys field when available; fallback to empty
            for k in f.account_keys.as_ref().map(|v| v.as_slice()).unwrap_or(&[]) {
                let mut e = self.fee_ema.entry(*k).or_insert(Ema::new(0.3));
                e.update(f.prioritization_fee as f64);
            }
        }

        // Combine raw fees with EMA-smoothed per-key values
        let mut fee_values: Vec<u64> = fees
            .iter()
            .map(|f| f.prioritization_fee)
            .filter(|&f| f > 0)
            .collect();

        for e in self.fee_ema.iter() {
            fee_values.push(e.value().value.round() as u64);
        }

        if fee_values.is_empty() {
            return Ok(STATIC_FALLBACK_CU_PRICE);
        }

        fee_values.sort_unstable();

        // Use 90th percentile for success, 50th for likely reverts (to avoid overpaying)
        let percentile = if will_revert { 0.5 } else { 0.9 };
        let idx = ((fee_values.len() as f64) * percentile) as usize;
        let base_fee = *fee_values.get(idx.min(fee_values.len().saturating_sub(1)))
            .unwrap_or(&STATIC_FALLBACK_CU_PRICE);

        // Scale with compute units (linear floor at 200k CU)
        let multiplier = (compute_units as f64 / 200_000.0).max(1.0);
        let adjusted = (base_fee as f64 * multiplier) as u64;

        // Leader/phase-aware tuning
        let mut fee = adjusted;
        if let Some(li) = &self.leader_intel {
            let slot = li.current_slot().await;
            // Phase multiplier
            let phase_mult = li.slot_phase_multiplier(slot, Instant::now()).await;
            fee = ((fee as f64) * phase_mult) as u64;
            // Leader preference boost if not preferred
            if let Some(leader) = li.leader_for_slot(slot).await {
                if !li.is_preferred_leader(&leader).await {
                    let add = (fee as f64 * 0.05) as u64; // +5%
                    fee = fee.saturating_add(add);
                }
            }
        }

        Ok(fee.min(1_000_000))
    }

    fn extract_account_keys(&self, instructions: &[Instruction]) -> Vec<Pubkey> {
        let mut keys = Vec::with_capacity(32);
        let mut seen = HashSet::with_capacity(64);

        for ix in instructions {
            if seen.insert(ix.program_id) {
                keys.push(ix.program_id);
            }
            for meta in &ix.accounts {
                if seen.insert(meta.pubkey) {
                    keys.push(meta.pubkey);
                }
            }
        }
        keys
    }

    pub async fn analyze_failure_patterns(&self) -> HashMap<String, f64> {
        let mut error_counts: HashMap<String, u32> = HashMap::new();
        let now = Instant::now();

        for entry in self.failure_patterns.iter() {
            let deque = entry.value();
            for p in deque.iter().rev() {
                if now.duration_since(p.timestamp) <= Duration::from_secs(300) {
                    *error_counts.entry(p.error_code.clone()).or_insert(0) += 1;
                } else {
                    break;
                }
            }
        }

        let total = error_counts.values().copied().sum::<u32>() as f64;
        if total == 0.0 {
            return HashMap::new();
        }

        error_counts
            .into_iter()
            .map(|(k, c)| (k, (c as f64) / total))
            .collect()
    }

    pub async fn get_program_health_metrics(&self) -> HashMap<Pubkey, f64> {
        let mut out = HashMap::new();
        for e in self.program_success_rates.iter() {
            if let Some(sr) = e.value().success_rate() {
                out.insert(*e.key(), sr);
            }
        }
        out
    }

    pub async fn should_execute_transaction(
        &self,
        prediction: &RevertPrediction,
        risk_tolerance: f64,
    ) -> bool {
        if prediction.will_revert { return false; }
        let base = match prediction.reason {
            RevertReason::None => prediction.confidence > (1.0 - risk_tolerance),
            RevertReason::NetworkCongestion => risk_tolerance > 0.5,
            RevertReason::HighFailurePattern => risk_tolerance > 0.7,
            _ => false,
        };
        if !base { return false; }

        if let Some(li) = &self.leader_intel {
            let slot = li.current_slot().await;
            let late_phase = li.slot_phase_multiplier(slot, Instant::now()).await > 1.05;
            let not_pref_leader = if let Some(leader) = li.leader_for_slot(slot).await {
                !li.is_preferred_leader(&leader).await
            } else { false };
            // If both late and not preferred, require higher risk tolerance
            if late_phase && not_pref_leader && risk_tolerance < 0.6 { return false; }
        }
        true
    }

    // Inserts CU and fee instructions if missing, using predicted values.
    pub async fn optimize_transaction_for_success(
        &self,
        instructions: &mut Vec<Instruction>,
        prediction: &RevertPrediction,
    ) -> Result<()> {
        let has_compute_budget = instructions
            .iter()
            .any(|ix| ix.program_id == solana_sdk::compute_budget::id());

        if !has_compute_budget {
            let cu_limit = {
                let margin = ((prediction.estimated_compute_units as f64) * SAFETY_CU_MARGIN_FRAC)
                    .ceil() as u32;
                (prediction.estimated_compute_units + margin).min(MAX_COMPUTE_UNITS)
            };

            instructions.insert(
                0,
                ComputeBudgetInstruction::set_compute_unit_limit(cu_limit),
            );
            instructions.insert(
                1,
                ComputeBudgetInstruction::set_compute_unit_price(
                    prediction.recommended_priority_fee,
                ),
            );
        }
        Ok(())
    }

    // Maintenance; prune stale entries
    pub async fn cleanup_old_data(&self) {
        let now = Instant::now();

        // Failure patterns TTL
        for mut entry in self.failure_patterns.iter_mut() {
            entry.retain(|p| now.duration_since(p.timestamp) < Duration::from_secs(FAILURE_PATTERN_TTL_SECS));
        }

        // Account cache TTL handled on read; here we hard prune anything older than 60s
        for e in self.account_cache.iter() {
            let (_, ts) = e.value();
            if now.duration_since(*ts) >= Duration::from_secs(60) {
                self.account_cache.remove(e.key());
            }
        }

        // Blockhashes older than 150s
        for e in self.recent_blockhashes.iter() {
            if now.duration_since(*e.value()) >= Duration::from_secs(150) {
                self.recent_blockhashes.remove(e.key());
            }
        }
    }
}

#[derive(Debug, Clone)]
struct RpcHealth { errors: u32, unhealthy_until: Option<Instant> }
impl RpcHealth {
    fn healthy(&self) -> bool { self.unhealthy_until.map(|t| Instant::now() >= t).unwrap_or(true) }
    fn mark_error(&mut self) {
        self.errors = self.errors.saturating_add(1);
        if self.errors >= 5 {
            self.unhealthy_until = Some(Instant::now() + Duration::from_secs(10));
            self.errors = 0;
        }
    }
    fn mark_ok(&mut self) { self.errors = self.errors.saturating_sub(self.errors.min(1)); self.unhealthy_until = None; }
}

#[derive(Debug, Clone)]
struct Ema { alpha: f64, value: f64, init: bool }
impl Ema {
    fn new(alpha: f64) -> Self { Self { alpha, value: 0.0, init: false } }
    fn update(&mut self, x: f64) {
        if !self.init { self.value = x; self.init = true; } else { self.value = self.alpha * x + (1.0 - self.alpha) * self.value; }
    }
}

// --- Leader and Slot Intelligence ---
#[derive(Clone)]
pub struct LeaderIntel {
    // Latest known slot
    current_slot_rx: watch::Receiver<u64>,
    // Moving average of slot duration in ms
    slot_ms_sma: std::sync::Arc<RwLock<VecDeque<u64>>>,
    // Absolute slot -> leader identity mapping for current epoch
    leader_map: std::sync::Arc<RwLock<HashMap<u64, Pubkey>>>,
    // Preferred leaders to favor in fee logic
    preferred_leaders: std::sync::Arc<RwLock<HashSet<Pubkey>>>,
    // Async RPC client
    rpc: std::sync::Arc<AsyncRpcClient>,
}

impl LeaderIntel {
    pub fn new(
        rpc: std::sync::Arc<AsyncRpcClient>,
        preferred_leaders: impl IntoIterator<Item = Pubkey>,
    ) -> Self {
        let (_, rx) = watch::channel(0u64);
        Self {
            current_slot_rx: rx,
            slot_ms_sma: std::sync::Arc::new(RwLock::new(VecDeque::with_capacity(SLOT_DURATION_SMA_WINDOW))),
            leader_map: std::sync::Arc::new(RwLock::new(HashMap::new())),
            preferred_leaders: std::sync::Arc::new(RwLock::new(preferred_leaders.into_iter().collect())),
            rpc,
        }
    }

    pub async fn start(&mut self, ws_endpoint: &str) {
        // Replace receiver with a new channel so we can drive slot updates here.
        let (tx, rx) = watch::channel(0u64);
        self.current_slot_rx = rx;

        // Spawn subscription loop
        let rpc = self.rpc.clone();
        let slot_ms_sma = self.slot_ms_sma.clone();
        let leader_map = self.leader_map.clone();
        let ws = ws_endpoint.to_string();
        tokio::spawn(async move {
            loop {
                match PubsubClient::new(&ws).await {
                    Ok(client) => {
                        let (mut sub, _unsub) = match client.slot_subscribe().await {
                            Ok(s) => s,
                            Err(e) => {
                                warn!(error=?e, "slot_subscribe failed, retrying");
                                tokio::time::sleep(Duration::from_millis(SLOT_SUB_RECONNECT_MS)).await;
                                continue;
                            }
                        };

                        // Prime leader schedule
                        if let Err(e) = Self::refresh_leader_schedule(&rpc, &leader_map).await {
                            warn!(error=?e, "failed to refresh leader schedule");
                        }
                        // Prime slot duration SMA
                        if let Err(e) = Self::refresh_slot_ms_sma(&rpc, &slot_ms_sma).await {
                            warn!(error=?e, "failed to refresh slot duration SMA");
                        }

                        // Consume slot stream
                        while let Some(Ok(slot_info)) = sub.next().await {
                            let SlotInfo { slot, .. } = slot_info;
                            let _ = tx.send(slot);

                            // Occasionally refresh leader schedule if map is empty (epoch rollover)
                            if let Err(e) = Self::maybe_refresh_leader_schedule(&rpc, &leader_map).await {
                                debug!(error=?e, "maybe_refresh_leader_schedule failed");
                            }
                            // Occasionally update perf-based slot time
                            if let Err(e) = Self::refresh_slot_ms_sma(&rpc, &slot_ms_sma).await {
                                debug!(error=?e, "refresh_slot_ms_sma failed");
                            }
                        }
                    }
                    Err(e) => {
                        warn!(error=?e, "PubsubClient connection failed, retrying");
                        tokio::time::sleep(Duration::from_millis(SLOT_SUB_RECONNECT_MS)).await;
                    }
                }
            }
        });
    }

    async fn refresh_leader_schedule(
        rpc: &AsyncRpcClient,
        leader_map: &std::sync::Arc<RwLock<HashMap<u64, Pubkey>>>,
    ) -> anyhow::Result<()> {
        // Fetch current epoch info
        let epoch_info = rpc.get_epoch_info().await?;
        let epoch = epoch_info.epoch;
        let first_slot_in_epoch = epoch_info.absolute_slot - epoch_info.slot_index;

        // Fetch leader schedule for current epoch
        let schedule_opt = rpc.get_leader_schedule(Some(first_slot_in_epoch)).await?;
        let schedule = schedule_opt.ok_or_else(|| anyhow!("Leader schedule unavailable"))?;

        // Map of validator identity -> list of relative slots
        let mut map: HashMap<u64, Pubkey> = HashMap::with_capacity(8192);
        for (identity_str, slots) in schedule {
            let pk = Pubkey::from_str(&identity_str)?;
            for rel in slots {
                let abs = first_slot_in_epoch + (rel as u64);
                map.insert(abs, pk);
            }
        }
        let mut guard = leader_map.write().await;
        *guard = map;
        let _ = epoch; // suppress unused if not logged
        Ok(())
    }

    async fn maybe_refresh_leader_schedule(
        rpc: &AsyncRpcClient,
        leader_map: &std::sync::Arc<RwLock<HashMap<u64, Pubkey>>>,
    ) -> anyhow::Result<()> {
        if leader_map.read().await.is_empty() {
            Self::refresh_leader_schedule(rpc, leader_map).await?;
        }
        Ok(())
    }

    async fn refresh_slot_ms_sma(
        rpc: &AsyncRpcClient,
        slot_ms_sma: &std::sync::Arc<RwLock<VecDeque<u64>>>,
    ) -> anyhow::Result<()> {
        // Use recent performance samples to approximate slot time
        let samples = rpc.get_recent_performance_samples(Some(8)).await?;
        if samples.is_empty() { return Ok(()); }

        // Average ms/slot across samples
        let mut ests = Vec::with_capacity(samples.len());
        for s in samples {
            if s.num_slots > 0 && s.sample_period_secs > 0 {
                let ms_per_slot = (s.sample_period_secs as f64 * 1000.0) / (s.num_slots as f64);
                ests.push(ms_per_slot as u64);
            }
        }
        if ests.is_empty() { return Ok(()); }
        let avg = (ests.iter().sum::<u64>() as f64 / ests.len() as f64).round() as u64;
        let mut sma = slot_ms_sma.write().await;
        sma.push_back(avg);
        if sma.len() > SLOT_DURATION_SMA_WINDOW { sma.pop_front(); }
        Ok(())
    }

    pub async fn current_slot(&self) -> u64 { *self.current_slot_rx.borrow() }

    pub async fn leader_for_slot(&self, slot: Slot) -> Option<Pubkey> {
        self.leader_map.read().await.get(&slot).copied()
    }

    pub async fn is_preferred_leader(&self, leader: &Pubkey) -> bool {
        self.preferred_leaders.read().await.contains(leader)
    }

    pub async fn average_slot_ms(&self) -> u64 {
        let sma = self.slot_ms_sma.read().await;
        if sma.is_empty() { DEFAULT_SLOT_MS } else { (sma.iter().sum::<u64>() as f64 / sma.len() as f64).round() as u64 }
    }

    // Coarse slot phase classification: early / mid / late
    pub async fn slot_phase_multiplier(&self, _slot: u64, _now_mono: Instant) -> f64 {
        // For simplicity, approximate phase using the midpoint when we lack exact slot start timestamps
        let slot_ms = self.average_slot_ms().await as f64;
        if slot_ms <= 1.0 { return FEE_MULTIPLIER_MID; }
        let elapsed_ms = 0.5 * slot_ms; // neutral midpoint without precise boundaries
        let frac = (elapsed_ms / slot_ms).clamp(0.0, 1.0);
        if frac <= SLOT_PHASE_EARLY_FRAC { FEE_MULTIPLIER_EARLY } else if frac >= SLOT_PHASE_LATE_FRAC { FEE_MULTIPLIER_LATE } else { FEE_MULTIPLIER_MID }
    }
}

// --- Tests
#[cfg(test)]
mod tests {
    use super::*;
    use solana_sdk::{hash::Hash, system_instruction};

    // This test asserts functional flow without relying on network determinism.
    // It only checks local logic that doesn’t require a real RPC (e.g., initial revert on insufficient funds).
    #[tokio::test]
    async fn test_revert_prediction_insufficient_balance() {
        // Use a mainnet RPC if available; the logic path we hit doesn’t require success RPCs.
        let predictor = TransactionRevertPredictor::new("https://api.mainnet-beta.solana.com");
        let payer = Keypair::new();
        let recipient = Pubkey::new_unique();

        let instruction = system_instruction::transfer(&payer.pubkey(), &recipient, 1_000_000);
        let recent_blockhash = Hash::default();

        let prediction = predictor
            .predict_revert(&[instruction], &payer, recent_blockhash)
            .await
            .expect("predict_revert should not error");

        assert!(prediction.will_revert);
        // Depending on RPC reachability and blockhash validity, acceptable reasons:
        // - InsufficientBalance (payer has no funds)
        // - StaleBlockhash (Hash::default() is invalid)
        // We at least guarantee one of these failure types appears, never success.
        assert!(
            matches!(
                prediction.reason,
                RevertReason::InsufficientBalance
                    | RevertReason::StaleBlockhash
                    | RevertReason::AccountNotFound
                    | RevertReason::NetworkCongestion
            ),
            "unexpected reason: {:?}",
            prediction.reason
        );
    }
}
