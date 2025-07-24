use solana_client::{
    nonblocking::rpc_client::RpcClient,
    rpc_config::{RpcBlockConfig, RpcTransactionConfig},
    rpc_response::RpcConfirmedTransactionStatusWithSignature,
};
use solana_sdk::{
    commitment_config::{CommitmentConfig, CommitmentLevel},
    pubkey::Pubkey,
    signature::Signature,
    transaction::Transaction,
    compute_budget,
    system_program,
};
use solana_transaction_status::{
    EncodedConfirmedTransactionWithStatusMeta, EncodedTransaction, UiTransactionEncoding,
};
use std::{
    collections::{HashMap, HashSet, VecDeque},
    str::FromStr,
    sync::{
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
        Arc,
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use tokio::{
    sync::{mpsc, RwLock, Semaphore},
    time::{interval, sleep, timeout},
};
use dashmap::DashMap;
use futures::{stream::FuturesUnordered, StreamExt};
use bincode::deserialize;
use base64;
use rand::{thread_rng, seq::SliceRandom, Rng};

const MAX_CONCURRENT_RPC_REQUESTS: usize = 40;
const TRANSACTION_CACHE_SIZE: usize = 8192;
const SIGNATURE_BATCH_SIZE: usize = 64;
const MEMPOOL_SCAN_INTERVAL_MS: u64 = 50;
const MAX_RETRIES: u32 = 3;
const RETRY_DELAY_MS: u64 = 100;
const RPC_TIMEOUT_MS: u64 = 3000;
const SLOT_UPDATE_INTERVAL_MS: u64 = 400;
const CACHE_CLEANUP_INTERVAL_SEC: u64 = 30;
const MAX_SLOT_AGE: u64 = 150;
const MIN_PROFIT_MULTIPLIER: u64 = 2;

const RAYDIUM_V4: &str = "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8";
const ORCA_WHIRLPOOL: &str = "whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc";
const JUPITER_V6: &str = "JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4";
const SERUM_V3: &str = "9xQeWvG816bUx9EPjHmaT23yvVM2ZWbrrpZb9PusVFin";
const TOKEN_PROGRAM: &str = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA";

#[derive(Debug, Clone)]
pub struct MempoolTransaction {
    pub signature: Signature,
    pub slot: u64,
    pub timestamp: u64,
    pub instructions: Vec<ParsedInstruction>,
    pub accounts: Vec<Pubkey>,
    pub compute_units: u64,
    pub priority_fee: u64,
}

#[derive(Debug, Clone)]
pub struct ParsedInstruction {
    pub program_id: Pubkey,
    pub instruction_type: InstructionType,
    pub accounts: Vec<Pubkey>,
    pub data: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum InstructionType {
    Swap { amount_in: u64, min_amount_out: u64 },
    LiquidityProvision { amount: u64 },
    TokenTransfer { amount: u64 },
    Unknown,
}

#[derive(Debug, Clone)]
pub struct MEVOpportunity {
    pub opportunity_type: OpportunityType,
    pub target_transaction: Signature,
    pub estimated_profit: u64,
    pub priority: u8,
    pub expiry_slot: u64,
    pub required_compute_units: u64,
}

#[derive(Debug, Clone)]
pub enum OpportunityType {
    Sandwich { victim_tx: Signature, pool: Pubkey, profit_bps: u64 },
    Arbitrage { path: Vec<Pubkey>, input_amount: u64 },
    Liquidation { account: Pubkey, collateral: u64 },
    JIT { pool: Pubkey, amount: u64 },
}

pub struct MempoolShadowScanner {
    rpc_clients: Arc<Vec<Arc<RpcClient>>>,
    transaction_cache: Arc<DashMap<Signature, MempoolTransaction>>,
    pending_signatures: Arc<RwLock<VecDeque<Signature>>>,
    processed_signatures: Arc<DashMap<Signature, u64>>,
    opportunity_sender: mpsc::UnboundedSender<MEVOpportunity>,
    monitored_programs: Arc<RwLock<HashSet<Pubkey>>>,
    slot_tracker: Arc<AtomicU64>,
    is_running: Arc<AtomicBool>,
    semaphore: Arc<Semaphore>,
    metrics: Arc<ScannerMetrics>,
    rpc_index: Arc<AtomicUsize>,
}

#[derive(Default)]
pub struct ScannerMetrics {
    pub transactions_scanned: AtomicU64,
    pub opportunities_found: AtomicU64,
    pub rpc_failures: AtomicU64,
    pub avg_latency_ms: AtomicU64,
    pub total_requests: AtomicU64,
}

impl MempoolShadowScanner {
    pub fn new(
        rpc_endpoints: Vec<String>,
        opportunity_sender: mpsc::UnboundedSender<MEVOpportunity>,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        if rpc_endpoints.is_empty() {
            return Err("No RPC endpoints provided".into());
        }

        let rpc_clients: Vec<Arc<RpcClient>> = rpc_endpoints
            .into_iter()
            .filter_map(|endpoint| {
                if endpoint.trim().is_empty() {
                    None
                } else {
                    Some(Arc::new(RpcClient::new_with_commitment(
                        endpoint,
                        CommitmentConfig::processed(),
                    )))
                }
            })
            .collect();

        if rpc_clients.is_empty() {
            return Err("No valid RPC endpoints after filtering".into());
        }

        let mut monitored_programs = HashSet::new();
        for program_str in &[RAYDIUM_V4, ORCA_WHIRLPOOL, JUPITER_V6, SERUM_V3, TOKEN_PROGRAM] {
            if let Ok(pubkey) = Pubkey::from_str(program_str) {
                monitored_programs.insert(pubkey);
            }
        }

        Ok(Self {
            rpc_clients: Arc::new(rpc_clients),
            transaction_cache: Arc::new(DashMap::new()),
            pending_signatures: Arc::new(RwLock::new(VecDeque::with_capacity(TRANSACTION_CACHE_SIZE))),
            processed_signatures: Arc::new(DashMap::new()),
            opportunity_sender,
            monitored_programs: Arc::new(RwLock::new(monitored_programs)),
            slot_tracker: Arc::new(AtomicU64::new(0)),
            is_running: Arc::new(AtomicBool::new(false)),
            semaphore: Arc::new(Semaphore::new(MAX_CONCURRENT_RPC_REQUESTS)),
            metrics: Arc::new(ScannerMetrics::default()),
            rpc_index: Arc::new(AtomicUsize::new(0)),
        })
    }

    pub async fn start(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        if self.is_running.swap(true, Ordering::SeqCst) {
            return Err("Scanner already running".into());
        }
        
        let mut handles = vec![];

        for i in 0..3 {
            let scanner = self.clone_scanner();
            handles.push(tokio::spawn(async move {
                let _ = scanner.signature_fetch_loop(i).await;
            }));
        }

        for _ in 0..2 {
            let scanner = self.clone_scanner();
            handles.push(tokio::spawn(async move {
                let _ = scanner.transaction_processing_loop().await;
            }));
        }

        let scanner = self.clone_scanner();
        handles.push(tokio::spawn(async move {
            scanner.slot_tracking_loop().await;
        }));

        let scanner = self.clone_scanner();
        handles.push(tokio::spawn(async move {
            scanner.cache_maintenance_loop().await;
        }));

        futures::future::join_all(handles).await;
        Ok(())
    }

    async fn signature_fetch_loop(&self, worker_id: usize) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let mut interval = interval(Duration::from_millis(MEMPOOL_SCAN_INTERVAL_MS + worker_id as u64 * 10));
        
        while self.is_running.load(Ordering::SeqCst) {
            interval.tick().await;
            
            let current_slot = self.slot_tracker.load(Ordering::SeqCst);
            if current_slot == 0 {
                continue;
            }

            if let Some(client) = self.select_rpc_client() {
                match timeout(
                    Duration::from_millis(RPC_TIMEOUT_MS),
                    client.get_signatures_for_address(&system_program::id())
                ).await {
                    Ok(Ok(signatures)) => {
                        let _ = self.process_signatures(signatures, current_slot).await;
                    }
                    _ => {
                        self.metrics.rpc_failures.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }
        }
        Ok(())
    }

    async fn process_signatures(
        &self,
        signatures: Vec<RpcConfirmedTransactionStatusWithSignature>,
        current_slot: u64,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let mut pending = self.pending_signatures.write().await;
        let cutoff_slot = current_slot.saturating_sub(MAX_SLOT_AGE);
        
        for sig_info in signatures {
            if sig_info.slot < cutoff_slot {
                continue;
            }

            if let Ok(sig) = Signature::from_str(&sig_info.signature) {
                if self.processed_signatures.get(&sig).is_none() {
                    if pending.len() < TRANSACTION_CACHE_SIZE {
                        pending.push_back(sig);
                        self.processed_signatures.insert(sig, current_slot);
                    }
                }
            }
        }

        Ok(())
    }

    async fn transaction_processing_loop(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let mut futures = FuturesUnordered::new();
        
        while self.is_running.load(Ordering::SeqCst) {
            let signatures_to_process = {
                let mut pending = self.pending_signatures.write().await;
                let batch_size = pending.len().min(SIGNATURE_BATCH_SIZE);
                pending.drain(..batch_size).collect::<Vec<_>>()
            };

            if signatures_to_process.is_empty() && futures.is_empty() {
                sleep(Duration::from_millis(10)).await;
                continue;
            }

            for sig in signatures_to_process {
                if let Ok(permit) = self.semaphore.clone().try_acquire_owned() {
                    if let Some(client) = self.select_rpc_client() {
                        let scanner = self.clone_scanner();
                        futures.push(tokio::spawn(async move {
                            let result = scanner.fetch_and_parse_transaction(client, sig).await;
                            drop(permit);
                            result
                        }));
                    }
                }
            }

            while futures.len() > MAX_CONCURRENT_RPC_REQUESTS {
                if let Some(result) = futures.next().await {
                    self.handle_transaction_result(result).await;
                }
            }
        }
        Ok(())
    }

    async fn fetch_and_parse_transaction(
        &self,
        client: Arc<RpcClient>,
        signature: Signature,
    ) -> Option<(Signature, MempoolTransaction)> {
        let start = Instant::now();
        self.metrics.total_requests.fetch_add(1, Ordering::Relaxed);
        
        for attempt in 0..MAX_RETRIES {
            match timeout(
                Duration::from_millis(RPC_TIMEOUT_MS),
                client.get_transaction_with_config(
                    &signature,
                    RpcTransactionConfig {
                        encoding: Some(UiTransactionEncoding::Base64),
                        commitment: Some(CommitmentConfig::processed()),
                        max_supported_transaction_version: Some(0),
                    },
                )
            ).await {
                Ok(Ok(tx_response)) => {
                    let latency = start.elapsed().as_millis() as u64;
                    self.update_latency_metric(latency);
                    
                    if let Some(parsed_tx) = self.parse_transaction_safe(tx_response, signature).await {
                        self.metrics.transactions_scanned.fetch_add(1, Ordering::Relaxed);
                        return Some((signature, parsed_tx));
                    }
                    return None;
                }
                _ if attempt < MAX_RETRIES - 1 => {
                    sleep(Duration::from_millis(RETRY_DELAY_MS * (attempt as u64 + 1))).await;
                }
                _ => {
                    self.metrics.rpc_failures.fetch_add(1, Ordering::Relaxed);
                    return None;
                }
            }
        }
        None
    }

        async fn parse_transaction_safe(
        &self,
        tx_response: EncodedConfirmedTransactionWithStatusMeta,
        signature: Signature,
    ) -> Option<MempoolTransaction> {
        let meta = tx_response.transaction.meta?;
        let slot = tx_response.slot;
        
        let transaction_bytes = match &tx_response.transaction.transaction {
            EncodedTransaction::LegacyBinary(data) => base64::decode(data).ok()?,
            _ => return None,
        };

        let transaction = match deserialize::<Transaction>(&transaction_bytes) {
            Ok(tx) => tx,
            Err(_) => return None,
        };

        let monitored = self.monitored_programs.read().await;
        let mut instructions = Vec::new();
        let mut compute_units = 200_000u64;
        let mut priority_fee = 0u64;

        for (idx, instruction) in transaction.message.instructions.iter().enumerate() {
            let program_idx = instruction.program_id_index as usize;
            if program_idx >= transaction.message.account_keys.len() {
                continue;
            }
            
            let program_id = transaction.message.account_keys[program_idx];
            
            if program_id == compute_budget::id() {
                if let Some((units, fee)) = self.parse_compute_budget_safe(&instruction.data) {
                    compute_units = units;
                    priority_fee = priority_fee.max(fee);
                }
            }

            if monitored.contains(&program_id) {
                if let Some(parsed_inst) = self.parse_instruction_safe(
                    program_id,
                    instruction,
                    &transaction.message.account_keys,
                ).await {
                    instructions.push(parsed_inst);
                }
            }
        }

        if instructions.is_empty() {
            return None;
        }

        Some(MempoolTransaction {
            signature,
            slot,
            timestamp: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|d| d.as_millis() as u64)
                .unwrap_or(0),
            instructions,
            accounts: transaction.message.account_keys,
            compute_units,
            priority_fee,
        })
    }

    async fn parse_instruction_safe(
        &self,
        program_id: Pubkey,
        instruction: &solana_sdk::instruction::CompiledInstruction,
        account_keys: &[Pubkey],
    ) -> Option<ParsedInstruction> {
        let accounts: Vec<Pubkey> = instruction.accounts
            .iter()
            .filter_map(|&idx| {
                let index = idx as usize;
                if index < account_keys.len() {
                    Some(account_keys[index])
                } else {
                    None
                }
            })
            .collect();

        let instruction_type = match program_id.to_string().as_str() {
            RAYDIUM_V4 => self.parse_raydium_safe(&instruction.data),
            ORCA_WHIRLPOOL => self.parse_orca_safe(&instruction.data),
            JUPITER_V6 => self.parse_jupiter_safe(&instruction.data),
            SERUM_V3 => self.parse_serum_safe(&instruction.data),
            TOKEN_PROGRAM => self.parse_token_safe(&instruction.data),
            _ => InstructionType::Unknown,
        };

        Some(ParsedInstruction {
            program_id,
            instruction_type,
            accounts,
            data: instruction.data.clone(),
        })
    }

    fn parse_raydium_safe(&self, data: &[u8]) -> InstructionType {
        if data.is_empty() {
            return InstructionType::Unknown;
        }
        
        match data.get(0) {
            Some(&9) => {
                let amount_in = data.get(1..9)
                    .and_then(|slice| slice.try_into().ok())
                    .map(u64::from_le_bytes)
                    .unwrap_or(0);
                let min_amount_out = data.get(9..17)
                    .and_then(|slice| slice.try_into().ok())
                    .map(u64::from_le_bytes)
                    .unwrap_or(0);
                
                if amount_in > 0 && min_amount_out > 0 {
                    InstructionType::Swap { amount_in, min_amount_out }
                } else {
                    InstructionType::Unknown
                }
            }
            Some(&3) | Some(&4) => {
                let amount = data.get(1..9)
                    .and_then(|slice| slice.try_into().ok())
                    .map(u64::from_le_bytes)
                    .unwrap_or(0);
                
                if amount > 0 {
                    InstructionType::LiquidityProvision { amount }
                } else {
                    InstructionType::Unknown
                }
            }
            _ => InstructionType::Unknown,
        }
    }

    fn parse_orca_safe(&self, data: &[u8]) -> InstructionType {
        if data.len() < 8 {
            return InstructionType::Unknown;
        }
        
        match data.get(0..8) {
            Some(&[0xf8, 0xc6, 0x9e, 0x91, 0xe1, 0x75, 0x87, 0xc8]) => {
                let amount_in = data.get(8..16)
                    .and_then(|slice| slice.try_into().ok())
                    .map(u64::from_le_bytes)
                    .unwrap_or(0);
                let min_amount_out = data.get(16..24)
                    .and_then(|slice| slice.try_into().ok())
                    .map(u64::from_le_bytes)
                    .unwrap_or(0);
                
                if amount_in > 0 && min_amount_out > 0 {
                    InstructionType::Swap { amount_in, min_amount_out }
                } else {
                    InstructionType::Unknown
                }
            }
            Some(&[0x2e, 0x1e, 0x92, 0x75, 0x76, 0xf6, 0xb0, 0x85]) => {
                let amount = data.get(8..16)
                    .and_then(|slice| slice.try_into().ok())
                    .map(u64::from_le_bytes)
                    .unwrap_or(0);
                
                if amount > 0 {
                    InstructionType::LiquidityProvision { amount }
                } else {
                    InstructionType::Unknown
                }
            }
            _ => InstructionType::Unknown,
        }
    }

    fn parse_jupiter_safe(&self, data: &[u8]) -> InstructionType {
        if data.len() < 24 {
            return InstructionType::Unknown;
        }
        
        let amount_in = data.get(8..16)
            .and_then(|slice| slice.try_into().ok())
            .map(u64::from_le_bytes)
            .unwrap_or(0);
        let min_amount_out = data.get(16..24)
            .and_then(|slice| slice.try_into().ok())
            .map(u64::from_le_bytes)
            .unwrap_or(0);
        
        if amount_in > 0 && min_amount_out > 0 {
            InstructionType::Swap { amount_in, min_amount_out }
        } else {
            InstructionType::Unknown
        }
    }

    fn parse_serum_safe(&self, data: &[u8]) -> InstructionType {
        if data.len() < 26 {
            return InstructionType::Unknown;
        }
        
        let version = data.get(0..2)
            .and_then(|slice| slice.try_into().ok())
            .map(u16::from_le_bytes)
            .unwrap_or(0);
        let instruction = data.get(2..4)
            .and_then(|slice| slice.try_into().ok())
            .map(u16::from_le_bytes)
            .unwrap_or(0);
        
        if version == 0 && matches!(instruction, 2 | 3 | 4 | 5) {
            let amount_in = data.get(10..18)
                .and_then(|slice| slice.try_into().ok())
                .map(u64::from_le_bytes)
                .unwrap_or(0);
            let min_amount_out = data.get(18..26)
                .and_then(|slice| slice.try_into().ok())
                .map(u64::from_le_bytes)
                .unwrap_or(0);
            
            if amount_in > 0 && min_amount_out > 0 {
                InstructionType::Swap { amount_in, min_amount_out }
            } else {
                InstructionType::Unknown
            }
        } else {
            InstructionType::Unknown
        }
    }

    fn parse_token_safe(&self, data: &[u8]) -> InstructionType {
        match data.get(0) {
            Some(&3) => {
                let amount = data.get(1..9)
                    .and_then(|slice| slice.try_into().ok())
                    .map(u64::from_le_bytes)
                    .unwrap_or(0);
                
                if amount > 0 {
                    InstructionType::TokenTransfer { amount }
                } else {
                    InstructionType::Unknown
                }
            }
            _ => InstructionType::Unknown,
        }
    }

    fn parse_compute_budget_safe(&self, data: &[u8]) -> Option<(u64, u64)> {
        match data.get(0)? {
            &0 | &2 => {
                let units = data.get(1..5)?
                    .try_into().ok()
                    .map(u32::from_le_bytes)?;
                Some((units as u64, 0))
            }
            &1 => {
                let fee = data.get(1..9)?
                    .try_into().ok()
                    .map(u64::from_le_bytes)?;
                Some((200_000, fee))
            }
            &3 => {
                let microlamports = data.get(1..5)?
                    .try_into().ok()
                    .map(u32::from_le_bytes)?;
                let fee = (microlamports as u64).saturating_mul(200_000) / 1_000_000;
                Some((200_000, fee))
            }
            _ => None,
        }
    }

    async fn handle_transaction_result(
        &self,
        result: tokio::task::JoinHandle<Option<(Signature, MempoolTransaction)>>
    ) {
        match result.await {
            Ok(Some((sig, tx))) => {
                self.transaction_cache.insert(sig, tx.clone());
                
                if let Some(opportunity) = self.analyze_for_mev(&tx).await {
                    self.metrics.opportunities_found.fetch_add(1, Ordering::Relaxed);
                    let _ = self.opportunity_sender.send(opportunity);
                }
            }
            _ => {}
        }
    }

    async fn analyze_for_mev(&self, tx: &MempoolTransaction) -> Option<MEVOpportunity> {
        let current_slot = self.slot_tracker.load(Ordering::SeqCst);
        
        for instruction in &tx.instructions {
            match &instruction.instruction_type {
                InstructionType::Swap { amount_in, min_amount_out } => {
                    if let Some(opp) = self.analyze_swap_safe(
                        tx, instruction, *amount_in, *min_amount_out, current_slot
                    ).await {
                        return Some(opp);
                    }
                }
                InstructionType::LiquidityProvision { amount } => {
                    if let Some(opp) = self.analyze_liquidity_safe(
                        tx, instruction, *amount, current_slot
                    ).await {
                        return Some(opp);
                    }
                }
                _ => continue,
            }
        }
        None
    }

    async fn analyze_swap_safe(
        &self,
        tx: &MempoolTransaction,
        instruction: &ParsedInstruction,
        amount_in: u64,
        min_amount_out: u64,
        current_slot: u64,
    ) -> Option<MEVOpportunity> {
        if amount_in == 0 || min_amount_out == 0 || amount_in <= min_amount_out {
            return None;
        }

        let slippage_bps = amount_in.saturating_sub(min_amount_out)
            .saturating_mul(10000)
            .checked_div(amount_in)
            .unwrap_or(0);
        
        if slippage_bps < 100 {
            return None;
        }

        let pool = instruction.accounts.get(1)?.clone();
        let estimated_profit = self.calculate_sandwich_profit_safe(
            amount_in, slippage_bps, tx.priority_fee
        );
        
        if estimated_profit > tx.priority_fee.saturating_mul(MIN_PROFIT_MULTIPLIER) {
            Some(MEVOpportunity {
                opportunity_type: OpportunityType::Sandwich {
                    victim_tx: tx.signature,
                    pool,
                    profit_bps: slippage_bps,
                },
                target_transaction: tx.signature,
                estimated_profit,
                priority: self.calculate_priority_safe(estimated_profit, tx.priority_fee),
                expiry_slot: current_slot.saturating_add(MAX_SLOT_AGE),
                required_compute_units: 400_000,
            })
        } else {
            None
        }
    }

    async fn analyze_liquidity_safe(
        &self,
        tx: &MempoolTransaction,
        instruction: &ParsedInstruction,
        amount: u64,
        current_slot: u64,
    ) -> Option<MEVOpportunity> {
        if amount < 10_000_000 {
            return None;
        }
        
        let pool = instruction.accounts.first()?.clone();
        let estimated_profit = amount.saturating_mul(25) / 10000;
        
                       if estimated_profit > tx.priority_fee.saturating_mul(MIN_PROFIT_MULTIPLIER) {
            Some(MEVOpportunity {
                opportunity_type: OpportunityType::JIT { pool, amount },
                target_transaction: tx.signature,
                estimated_profit,
                priority: 7,
                expiry_slot: current_slot.saturating_add(100),
                required_compute_units: 300_000,
            })
        } else {
            None
        }
    }

    fn calculate_sandwich_profit_safe(&self, amount: u64, slippage_bps: u64, priority_fee: u64) -> u64 {
        let gross_profit = amount
            .saturating_mul(slippage_bps)
            .checked_div(20000)
            .unwrap_or(0);
        gross_profit.saturating_sub(priority_fee.saturating_mul(2))
    }

    fn calculate_priority_safe(&self, profit: u64, priority_fee: u64) -> u8 {
        let fee = priority_fee.max(1);
        let ratio = profit.saturating_div(fee);
        match ratio {
            0..=10 => 1,
            11..=50 => 3,
            51..=100 => 5,
            101..=500 => 7,
            _ => 9,
        }
    }

    async fn slot_tracking_loop(&self) {
        let mut interval = interval(Duration::from_millis(SLOT_UPDATE_INTERVAL_MS));
        let mut consecutive_failures = 0u32;
        
        while self.is_running.load(Ordering::SeqCst) {
            interval.tick().await;
            
            if let Some(client) = self.select_rpc_client() {
                match timeout(
                    Duration::from_millis(RPC_TIMEOUT_MS),
                    client.get_slot()
                ).await {
                    Ok(Ok(slot)) => {
                        self.slot_tracker.store(slot, Ordering::SeqCst);
                        consecutive_failures = 0;
                    }
                    _ => {
                        consecutive_failures = consecutive_failures.saturating_add(1);
                        self.metrics.rpc_failures.fetch_add(1, Ordering::Relaxed);
                        
                        if consecutive_failures > 10 {
                            sleep(Duration::from_secs(1)).await;
                        }
                    }
                }
            } else {
                sleep(Duration::from_secs(1)).await;
            }
        }
    }

    async fn cache_maintenance_loop(&self) {
        let mut interval = interval(Duration::from_secs(CACHE_CLEANUP_INTERVAL_SEC));
        
        while self.is_running.load(Ordering::SeqCst) {
            interval.tick().await;
            
            let current_slot = self.slot_tracker.load(Ordering::SeqCst);
            if current_slot == 0 {
                continue;
            }
            
            let cutoff_slot = current_slot.saturating_sub(MAX_SLOT_AGE * 2);
            
            self.transaction_cache.retain(|_, tx| tx.slot > cutoff_slot);
            self.processed_signatures.retain(|_, slot| *slot > cutoff_slot);
            
            let mut pending = self.pending_signatures.write().await;
            while pending.len() > TRANSACTION_CACHE_SIZE {
                pending.pop_front();
            }
        }
    }

    fn select_rpc_client(&self) -> Option<Arc<RpcClient>> {
        if self.rpc_clients.is_empty() {
            return None;
        }
        
        let index = self.rpc_index.fetch_add(1, Ordering::Relaxed) % self.rpc_clients.len();
        self.rpc_clients.get(index).cloned()
    }

    fn update_latency_metric(&self, latency_ms: u64) {
        let current = self.metrics.avg_latency_ms.load(Ordering::Relaxed);
        let new_avg = if current == 0 {
            latency_ms
        } else {
            (current.saturating_mul(9).saturating_add(latency_ms)) / 10
        };
        self.metrics.avg_latency_ms.store(new_avg, Ordering::Relaxed);
    }

    fn clone_scanner(&self) -> Self {
        Self {
            rpc_clients: self.rpc_clients.clone(),
            transaction_cache: self.transaction_cache.clone(),
            pending_signatures: self.pending_signatures.clone(),
            processed_signatures: self.processed_signatures.clone(),
            opportunity_sender: self.opportunity_sender.clone(),
            monitored_programs: self.monitored_programs.clone(),
            slot_tracker: self.slot_tracker.clone(),
            is_running: self.is_running.clone(),
            semaphore: self.semaphore.clone(),
            metrics: self.metrics.clone(),
            rpc_index: self.rpc_index.clone(),
        }
    }

    pub async fn stop(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.is_running.store(false, Ordering::SeqCst);
        Ok(())
    }

    pub fn get_metrics(&self) -> ScannerMetrics {
        ScannerMetrics {
            transactions_scanned: self.metrics.transactions_scanned.load(Ordering::Relaxed),
            opportunities_found: self.metrics.opportunities_found.load(Ordering::Relaxed),
            rpc_failures: self.metrics.rpc_failures.load(Ordering::Relaxed),
            avg_latency_ms: self.metrics.avg_latency_ms.load(Ordering::Relaxed),
            cache_size: self.transaction_cache.len(),
            current_slot: self.slot_tracker.load(Ordering::Relaxed),
            error_rate: self.calculate_error_rate(),
        }
    }

    fn calculate_error_rate(&self) -> f64 {
        let total = self.metrics.total_requests.load(Ordering::Relaxed);
        let failures = self.metrics.rpc_failures.load(Ordering::Relaxed);
        
        if total == 0 {
            0.0
        } else {
            (failures as f64 / total as f64) * 100.0
        }
    }

    pub async fn get_pending_count(&self) -> usize {
        self.pending_signatures.read().await.len()
    }

    pub async fn add_monitored_program(&self, program_id: Pubkey) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let mut programs = self.monitored_programs.write().await;
        if programs.len() >= 50 {
            return Err("Maximum monitored programs limit reached".into());
        }
        programs.insert(program_id);
        Ok(())
    }

    pub async fn remove_monitored_program(&self, program_id: &Pubkey) -> bool {
        self.monitored_programs.write().await.remove(program_id)
    }

    pub async fn get_monitored_programs(&self) -> Vec<Pubkey> {
        self.monitored_programs.read().await.iter().cloned().collect()
    }

    pub fn get_cached_transaction(&self, signature: &Signature) -> Option<MempoolTransaction> {
        self.transaction_cache.get(signature).map(|entry| entry.clone())
    }

    pub fn is_transaction_processed(&self, signature: &Signature) -> bool {
        self.processed_signatures.contains_key(signature)
    }

    pub async fn force_rescan(&self, signature: Signature) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let mut pending = self.pending_signatures.write().await;
        if pending.len() >= TRANSACTION_CACHE_SIZE {
            return Err("Pending queue is full".into());
        }
        
        self.processed_signatures.remove(&signature);
        self.transaction_cache.remove(&signature);
        pending.push_front(signature);
        Ok(())
    }

    pub async fn clear_caches(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let mut pending = self.pending_signatures.write().await;
        pending.clear();
        drop(pending);
        
        self.transaction_cache.clear();
        self.processed_signatures.clear();
        Ok(())
    }

    pub fn is_healthy(&self) -> bool {
        let current_slot = self.slot_tracker.load(Ordering::SeqCst);
        let avg_latency = self.metrics.avg_latency_ms.load(Ordering::Relaxed);
        let error_rate = self.calculate_error_rate();
        
        current_slot > 0 && avg_latency < 5000 && error_rate < 25.0
    }

    pub async fn get_health_status(&self) -> HealthStatus {
        HealthStatus {
            is_running: self.is_running.load(Ordering::SeqCst),
            is_healthy: self.is_healthy(),
            current_slot: self.slot_tracker.load(Ordering::SeqCst),
            avg_latency_ms: self.metrics.avg_latency_ms.load(Ordering::Relaxed),
            error_rate: self.calculate_error_rate(),
            pending_count: self.get_pending_count().await,
            cache_size: self.transaction_cache.len(),
            rpc_count: self.rpc_clients.len(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct ScannerMetrics {
    pub transactions_scanned: u64,
    pub opportunities_found: u64,
    pub rpc_failures: u64,
    pub avg_latency_ms: u64,
    pub cache_size: usize,
    pub current_slot: u64,
    pub error_rate: f64,
}

#[derive(Debug, Clone)]
pub struct HealthStatus {
    pub is_running: bool,
    pub is_healthy: bool,
    pub current_slot: u64,
    pub avg_latency_ms: u64,
    pub error_rate: f64,
    pub pending_count: usize,
    pub cache_size: usize,
    pub rpc_count: usize,
}

impl Clone for MempoolShadowScanner {
    fn clone(&self) -> Self {
        self.clone_scanner()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::sync::mpsc;

    #[tokio::test]
    async fn test_scanner_creation() {
        let (tx, _rx) = mpsc::unbounded_channel();
        let result = MempoolShadowScanner::new(vec![], tx);
        assert!(result.is_err());
        
        let (tx, _rx) = mpsc::unbounded_channel();
        let scanner = MempoolShadowScanner::new(
            vec!["https://api.mainnet-beta.solana.com".to_string()],
            tx,
        );
        assert!(scanner.is_ok());
    }

    #[test]
    fn test_compute_budget_parsing() {
        let (tx, _rx) = mpsc::unbounded_channel();
        let scanner = MempoolShadowScanner::new(
            vec!["https://api.mainnet-beta.solana.com".to_string()],
            tx,
        ).unwrap();
        
        let data = vec![0, 0x40, 0x0d, 0x03, 0x00];
        let result = scanner.parse_compute_budget_safe(&data);
        assert!(result.is_some());
        
        let data = vec![1, 0x64, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00];
        let result = scanner.parse_compute_budget_safe(&data);
        assert!(result.is_some());
        
        let data = vec![];
        let result = scanner.parse_compute_budget_safe(&data);
        assert!(result.is_none());
    }

    #[test]
    fn test_priority_calculation() {
        let (tx, _rx) = mpsc::unbounded_channel();
        let scanner = MempoolShadowScanner::new(
            vec!["https://api.mainnet-beta.solana.com".to_string()],
            tx,
        ).unwrap();
        
        assert_eq!(scanner.calculate_priority_safe(1000, 100), 1);
        assert_eq!(scanner.calculate_priority_safe(5000, 100), 3);
        assert_eq!(scanner.calculate_priority_safe(10000, 100), 5);
        assert_eq!(scanner.calculate_priority_safe(50000, 100), 7);
        assert_eq!(scanner.calculate_priority_safe(100000, 100), 9);
        assert_eq!(scanner.calculate_priority_safe(100, 0), 9);
    }

    #[test]
    fn test_sandwich_profit_calculation() {
        let (tx, _rx) = mpsc::unbounded_channel();
        let scanner = MempoolShadowScanner::new(
            vec!["https://api.mainnet-beta.solana.com".to_string()],
            tx,
        ).unwrap();
        
        let profit = scanner.calculate_sandwich_profit_safe(1_000_000, 1500, 5000);
        assert!(profit > 0);
        
        let profit = scanner.calculate_sandwich_profit_safe(1_000_000, 10, 5000);
        assert_eq!(profit, 0);
        
        let profit = scanner.calculate_sandwich_profit_safe(0, 1500, 5000);
        assert_eq!(profit, 0);
    }

    #[tokio::test]
    async fn test_health_check() {
        let (tx, _rx) = mpsc::unbounded_channel();
        let scanner = MempoolShadowScanner::new(
            vec!["https://api.mainnet-beta.solana.com".to_string()],
            tx,
        ).unwrap();
        
        assert!(!scanner.is_healthy());
        
        scanner.slot_tracker.store(100000, Ordering::SeqCst);
        scanner.metrics.avg_latency_ms.store(1000, Ordering::SeqCst);
        assert!(scanner.is_healthy());
    }

        #[test]
    fn test_error_rate_calculation() {
        let (tx, _rx) = mpsc::unbounded_channel();
        let scanner = MempoolShadowScanner::new(
            vec!["https://api.mainnet-beta.solana.com".to_string()],
            tx,
        ).unwrap();
        
        assert_eq!(scanner.calculate_error_rate(), 0.0);
        
        scanner.metrics.total_requests.store(100, Ordering::Relaxed);
        scanner.metrics.rpc_failures.store(10, Ordering::Relaxed);
        assert_eq!(scanner.calculate_error_rate(), 10.0);
        
        scanner.metrics.total_requests.store(1000, Ordering::Relaxed);
        scanner.metrics.rpc_failures.store(50, Ordering::Relaxed);
        assert_eq!(scanner.calculate_error_rate(), 5.0);
    }

    #[tokio::test]
    async fn test_monitored_programs() {
        let (tx, _rx) = mpsc::unbounded_channel();
        let scanner = MempoolShadowScanner::new(
            vec!["https://api.mainnet-beta.solana.com".to_string()],
            tx,
        ).unwrap();
        
        let initial_count = scanner.get_monitored_programs().await.len();
        assert!(initial_count >= 4);
        
        let new_program = Pubkey::new_unique();
        let result = scanner.add_monitored_program(new_program).await;
        assert!(result.is_ok());
        
        let programs = scanner.get_monitored_programs().await;
        assert_eq!(programs.len(), initial_count + 1);
        assert!(programs.contains(&new_program));
        
        let removed = scanner.remove_monitored_program(&new_program).await;
        assert!(removed);
        
        let programs = scanner.get_monitored_programs().await;
        assert_eq!(programs.len(), initial_count);
        assert!(!programs.contains(&new_program));
    }

    #[tokio::test]
    async fn test_force_rescan() {
        let (tx, _rx) = mpsc::unbounded_channel();
        let scanner = MempoolShadowScanner::new(
            vec!["https://api.mainnet-beta.solana.com".to_string()],
            tx,
        ).unwrap();
        
        let sig = Signature::new_unique();
        scanner.processed_signatures.insert(sig, 100);
        assert!(scanner.is_transaction_processed(&sig));
        
        let result = scanner.force_rescan(sig).await;
        assert!(result.is_ok());
        assert!(!scanner.is_transaction_processed(&sig));
    }

    #[tokio::test]
    async fn test_cache_operations() {
        let (tx, _rx) = mpsc::unbounded_channel();
        let scanner = MempoolShadowScanner::new(
            vec!["https://api.mainnet-beta.solana.com".to_string()],
            tx,
        ).unwrap();
        
        let sig = Signature::new_unique();
        let mock_tx = MempoolTransaction {
            signature: sig,
            slot: 100,
            timestamp: 1000,
            instructions: vec![],
            accounts: vec![],
            compute_units: 200_000,
            priority_fee: 1000,
        };
        
        scanner.transaction_cache.insert(sig, mock_tx.clone());
        let cached = scanner.get_cached_transaction(&sig);
        assert!(cached.is_some());
        assert_eq!(cached.unwrap().slot, 100);
        
        let result = scanner.clear_caches().await;
        assert!(result.is_ok());
        assert!(scanner.get_cached_transaction(&sig).is_none());
    }

    #[tokio::test]
    async fn test_health_status() {
        let (tx, _rx) = mpsc::unbounded_channel();
        let scanner = MempoolShadowScanner::new(
            vec!["https://api.mainnet-beta.solana.com".to_string()],
            tx,
        ).unwrap();
        
        let health = scanner.get_health_status().await;
        assert!(!health.is_running);
        assert!(!health.is_healthy);
        assert_eq!(health.current_slot, 0);
        assert_eq!(health.rpc_count, 1);
        
        scanner.is_running.store(true, Ordering::SeqCst);
        scanner.slot_tracker.store(100000, Ordering::SeqCst);
        scanner.metrics.avg_latency_ms.store(1000, Ordering::SeqCst);
        
        let health = scanner.get_health_status().await;
        assert!(health.is_running);
        assert!(health.is_healthy);
        assert_eq!(health.current_slot, 100000);
    }

    #[test]
    fn test_instruction_parsing() {
        let (tx, _rx) = mpsc::unbounded_channel();
        let scanner = MempoolShadowScanner::new(
            vec!["https://api.mainnet-beta.solana.com".to_string()],
            tx,
        ).unwrap();
        
        // Test Raydium parsing
        let data = vec![9, 0x10, 0x27, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 
                       0x20, 0x4e, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00];
        let inst = scanner.parse_raydium_safe(&data);
        match inst {
            InstructionType::Swap { amount_in, min_amount_out } => {
                assert_eq!(amount_in, 10000);
                assert_eq!(min_amount_out, 20000);
            }
            _ => panic!("Expected swap instruction"),
        }
        
        // Test empty data handling
        let inst = scanner.parse_raydium_safe(&[]);
        assert_eq!(inst, InstructionType::Unknown);
        
        // Test Orca parsing
        let data = vec![0xf8, 0xc6, 0x9e, 0x91, 0xe1, 0x75, 0x87, 0xc8,
                       0x64, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                       0x32, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00];
        let inst = scanner.parse_orca_safe(&data);
        match inst {
            InstructionType::Swap { amount_in, min_amount_out } => {
                assert_eq!(amount_in, 100);
                assert_eq!(min_amount_out, 50);
            }
            _ => panic!("Expected swap instruction"),
        }
        
        // Test Token Transfer parsing
        let data = vec![3, 0x64, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00];
        let inst = scanner.parse_token_safe(&data);
        match inst {
            InstructionType::TokenTransfer { amount } => {
                assert_eq!(amount, 100);
            }
            _ => panic!("Expected transfer instruction"),
        }
    }

    #[test]
    fn test_select_rpc_client() {
        let (tx, _rx) = mpsc::unbounded_channel();
        let scanner = MempoolShadowScanner::new(
            vec!["https://api.mainnet-beta.solana.com".to_string()],
            tx,
        ).unwrap();
        
        assert!(scanner.select_rpc_client().is_some());
        
        // Test with multiple clients
        let (tx, _rx) = mpsc::unbounded_channel();
        let scanner = MempoolShadowScanner::new(
            vec![
                "https://api.mainnet-beta.solana.com".to_string(),
                "https://solana-api.projectserum.com".to_string(),
            ],
            tx,
        ).unwrap();
        
        let mut clients = std::collections::HashSet::new();
        for _ in 0..20 {
            if let Some(client) = scanner.select_rpc_client() {
                clients.insert(format!("{:p}", client.as_ref()));
            }
        }
        assert!(clients.len() > 1);
    }

    #[tokio::test]
    async fn test_scanner_lifecycle() {
        let (tx, mut rx) = mpsc::unbounded_channel();
        let scanner = MempoolShadowScanner::new(
            vec!["https://api.mainnet-beta.solana.com".to_string()],
            tx,
        ).unwrap();
        
        assert!(!scanner.is_running.load(Ordering::SeqCst));
        
        // Start scanner in background
        let scanner_clone = scanner.clone();
        let handle = tokio::spawn(async move {
            let _ = scanner_clone.start().await;
        });
        
        // Wait a bit for scanner to start
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(scanner.is_running.load(Ordering::SeqCst));
        
        // Stop scanner
        let _ = scanner.stop().await;
        assert!(!scanner.is_running.load(Ordering::SeqCst));
        
        // Clean up
        drop(rx);
        let _ = tokio::time::timeout(Duration::from_secs(1), handle).await;
    }

    #[test]
    fn test_bounds_checking() {
        let (tx, _rx) = mpsc::unbounded_channel();
        let scanner = MempoolShadowScanner::new(
            vec!["https://api.mainnet-beta.solana.com".to_string()],
            tx,
        ).unwrap();
        
        // Test with data that's too small
        let data = vec![9, 0x10];
        let inst = scanner.parse_raydium_safe(&data);
        assert_eq!(inst, InstructionType::Unknown);
        
        // Test compute budget with insufficient data
        let data = vec![0, 0x40];
        let result = scanner.parse_compute_budget_safe(&data);
        assert!(result.is_none());
        
        // Test Serum with small data
        let data = vec![0x00, 0x00, 0x02, 0x00];
        let inst = scanner.parse_serum_safe(&data);
        assert_eq!(inst, InstructionType::Unknown);
    }
}

