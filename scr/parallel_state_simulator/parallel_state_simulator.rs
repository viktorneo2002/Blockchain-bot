use anyhow::{Context, Result};
use dashmap::DashMap;
use parking_lot::{Mutex, RwLock};
use rayon::prelude::*;
use solana_client::rpc_client::RpcClient;
use solana_program::{
    account_info::AccountInfo,
    clock::Slot,
    instruction::Instruction,
    pubkey::Pubkey,
    rent::Rent,
    sysvar::Sysvar,
};
use solana_sdk::{
    account::{Account, ReadableAccount},
    commitment_config::CommitmentConfig,
    compute_budget::ComputeBudgetInstruction,
    hash::Hash,
    message::{Message, VersionedMessage},
    signature::Signature,
    transaction::{Transaction, VersionedTransaction},
};
use std::{
    collections::{HashMap, HashSet},
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};
use tokio::sync::{mpsc, RwLock as TokioRwLock, Semaphore};

const MAX_PARALLEL_SIMULATIONS: usize = 32;
const ACCOUNT_CACHE_SIZE: usize = 10000;
const SIMULATION_TIMEOUT_MS: u64 = 50;
const MAX_COMPUTE_UNITS: u32 = 1_400_000;
const PRIORITY_FEE_PERCENTILE: f64 = 0.95;
const STATE_VERSION_LIFETIME_MS: u64 = 1000;

#[derive(Clone, Debug)]
pub struct AccountState {
    pub lamports: u64,
    pub data: Vec<u8>,
    pub owner: Pubkey,
    pub executable: bool,
    pub rent_epoch: u64,
}

impl From<&Account> for AccountState {
    fn from(account: &Account) -> Self {
        Self {
            lamports: account.lamports,
            data: account.data.clone(),
            owner: account.owner,
            executable: account.executable,
            rent_epoch: account.rent_epoch,
        }
    }
}

#[derive(Clone)]
pub struct SimulationResult {
    pub success: bool,
    pub compute_units_consumed: u64,
    pub logs: Vec<String>,
    pub modified_accounts: HashMap<Pubkey, AccountState>,
    pub profit: i64,
    pub gas_cost: u64,
    pub simulation_time_us: u64,
}

pub struct StateVersion {
    pub slot: Slot,
    pub blockhash: Hash,
    pub timestamp: Instant,
    pub accounts: Arc<DashMap<Pubkey, AccountState>>,
}

pub struct ParallelStateSimulator {
    rpc_client: Arc<RpcClient>,
    state_versions: Arc<TokioRwLock<Vec<Arc<StateVersion>>>>,
    account_locks: Arc<DashMap<Pubkey, Arc<Semaphore>>>,
    simulation_semaphore: Arc<Semaphore>,
    current_slot: Arc<AtomicU64>,
    is_running: Arc<AtomicBool>,
    priority_fee_cache: Arc<RwLock<f64>>,
}

impl ParallelStateSimulator {
    pub fn new(rpc_endpoint: &str) -> Result<Self> {
        let rpc_client = Arc::new(RpcClient::new_with_timeout_and_commitment(
            rpc_endpoint.to_string(),
            Duration::from_millis(1000),
            CommitmentConfig::processed(),
        ));

        Ok(Self {
            rpc_client,
            state_versions: Arc::new(TokioRwLock::new(Vec::new())),
            account_locks: Arc::new(DashMap::new()),
            simulation_semaphore: Arc::new(Semaphore::new(MAX_PARALLEL_SIMULATIONS)),
            current_slot: Arc::new(AtomicU64::new(0)),
            is_running: Arc::new(AtomicBool::new(true)),
            priority_fee_cache: Arc::new(RwLock::new(0.0)),
        })
    }

    pub async fn start(&self) -> Result<()> {
        self.update_state_version().await?;
        self.start_state_update_task();
        self.start_priority_fee_update_task();
        Ok(())
    }

    fn start_state_update_task(&self) {
        let state_versions = self.state_versions.clone();
        let rpc_client = self.rpc_client.clone();
        let current_slot = self.current_slot.clone();
        let is_running = self.is_running.clone();

        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_millis(400));
            
            while is_running.load(Ordering::Relaxed) {
                interval.tick().await;
                
                match rpc_client.get_slot() {
                    Ok(slot) => {
                        let prev_slot = current_slot.swap(slot, Ordering::SeqCst);
                        
                        if slot > prev_slot {
                            if let Ok(blockhash) = rpc_client.get_latest_blockhash() {
                                let new_version = Arc::new(StateVersion {
                                    slot,
                                    blockhash,
                                    timestamp: Instant::now(),
                                    accounts: Arc::new(DashMap::new()),
                                });
                                
                                let mut versions = state_versions.write().await;
                                versions.push(new_version);
                                
                                versions.retain(|v| {
                                    v.timestamp.elapsed().as_millis() < STATE_VERSION_LIFETIME_MS as u128
                                });
                            }
                        }
                    }
                    Err(_) => continue,
                }
            }
        });
    }

    fn start_priority_fee_update_task(&self) {
        let rpc_client = self.rpc_client.clone();
        let priority_fee_cache = self.priority_fee_cache.clone();
        let is_running = self.is_running.clone();

        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(10));
            
            while is_running.load(Ordering::Relaxed) {
                interval.tick().await;
                
                if let Ok(recent_fees) = rpc_client.get_recent_prioritization_fees(&[]) {
                    if !recent_fees.is_empty() {
                        let mut fees: Vec<u64> = recent_fees.iter()
                            .map(|f| f.prioritization_fee)
                            .collect();
                        fees.sort_unstable();
                        
                        let percentile_idx = ((fees.len() as f64 * PRIORITY_FEE_PERCENTILE) as usize)
                            .min(fees.len().saturating_sub(1));
                        
                        let percentile_fee = fees[percentile_idx] as f64;
                        *priority_fee_cache.write() = percentile_fee;
                    }
                }
            }
        });
    }

    async fn update_state_version(&self) -> Result<()> {
        let slot = self.rpc_client.get_slot()?;
        let blockhash = self.rpc_client.get_latest_blockhash()?;
        
        let new_version = Arc::new(StateVersion {
            slot,
            blockhash,
            timestamp: Instant::now(),
            accounts: Arc::new(DashMap::new()),
        });
        
        let mut versions = self.state_versions.write().await;
        versions.push(new_version);
        
        self.current_slot.store(slot, Ordering::SeqCst);
        Ok(())
    }

    pub async fn simulate_transactions_parallel(
        &self,
        transactions: Vec<VersionedTransaction>,
    ) -> Result<Vec<SimulationResult>> {
        let current_version = self.get_latest_state_version().await?;
        let chunks: Vec<_> = transactions.chunks(MAX_PARALLEL_SIMULATIONS).collect();
        let mut all_results = Vec::new();

        for chunk in chunks {
            let chunk_results: Vec<_> = chunk
                .par_iter()
                .map(|tx| {
                    let permit = self.simulation_semaphore.try_acquire();
                    if permit.is_err() {
                        return self.simulate_single_transaction(tx, &current_version);
                    }
                    
                    let result = self.simulate_single_transaction(tx, &current_version);
                    drop(permit);
                    result
                })
                .collect();

            for result in chunk_results {
                all_results.push(result?);
            }
        }

        Ok(all_results)
    }

    fn simulate_single_transaction(
        &self,
        transaction: &VersionedTransaction,
        state_version: &Arc<StateVersion>,
    ) -> Result<SimulationResult> {
        let start_time = Instant::now();
        let message = transaction.message();
        
        let account_keys = message.static_account_keys();
        let mut modified_accounts = HashMap::new();
        let mut total_compute_units = 0u64;
        
        let priority_fee = self.extract_priority_fee(transaction);
        let gas_cost = self.calculate_gas_cost(transaction, priority_fee);
        
        let writable_accounts: HashSet<_> = account_keys
            .iter()
            .enumerate()
            .filter(|(idx, _)| message.is_maybe_writable(*idx))
            .map(|(_, key)| key)
            .collect();

        for pubkey in &writable_accounts {
            let account_state = self.get_or_fetch_account(pubkey, state_version)?;
            modified_accounts.insert(**pubkey, account_state);
        }

        let fee_payer = &account_keys[0];
        if let Some(fee_payer_state) = modified_accounts.get_mut(fee_payer) {
            if fee_payer_state.lamports < gas_cost {
                return Ok(SimulationResult {
                    success: false,
                    compute_units_consumed: 0,
                    logs: vec!["Insufficient balance for fees".to_string()],
                    modified_accounts,
                    profit: -1,
                    gas_cost,
                    simulation_time_us: start_time.elapsed().as_micros() as u64,
                });
            }
            fee_payer_state.lamports = fee_payer_state.lamports.saturating_sub(gas_cost);
        }

        let instructions = match message {
            VersionedMessage::Legacy(msg) => &msg.instructions,
            VersionedMessage::V0(msg) => &msg.instructions,
        };

        for (idx, instruction) in instructions.iter().enumerate() {
            let program_id = account_keys[instruction.program_id_index as usize];
            
            let compute_units = self.estimate_compute_units(&program_id, instruction);
            total_compute_units += compute_units;
            
            if total_compute_units > MAX_COMPUTE_UNITS as u64 {
                return Ok(SimulationResult {
                    success: false,
                    compute_units_consumed: total_compute_units,
                    logs: vec![format!("Compute units exceeded at instruction {}", idx)],
                    modified_accounts,
                    profit: -(gas_cost as i64),
                    gas_cost,
                    simulation_time_us: start_time.elapsed().as_micros() as u64,
                });
            }

            if let Err(e) = self.simulate_instruction(
                instruction,
                account_keys,
                &mut modified_accounts,
                state_version,
            ) {
                return Ok(SimulationResult {
                    success: false,
                    compute_units_consumed: total_compute_units,
                    logs: vec![format!("Instruction {} failed: {}", idx, e)],
                    modified_accounts,
                    profit: -(gas_cost as i64),
                    gas_cost,
                    simulation_time_us: start_time.elapsed().as_micros() as u64,
                });
            }
        }

        let profit = self.calculate_profit(&modified_accounts, gas_cost);

        Ok(SimulationResult {
            success: true,
            compute_units_consumed: total_compute_units,
            logs: vec!["Simulation successful".to_string()],
            modified_accounts,
            profit,
            gas_cost,
            simulation_time_us: start_time.elapsed().as_micros() as u64,
        })
    }

    fn simulate_instruction(
        &self,
        instruction: &solana_sdk::instruction::CompiledInstruction,
        account_keys: &[Pubkey],
        modified_accounts: &mut HashMap<Pubkey, AccountState>,
        _state_version: &Arc<StateVersion>,
    ) -> Result<()> {
        let program_id = &account_keys[instruction.program_id_index as usize];
        
        match program_id.to_string().as_str() {
            "11111111111111111111111111111111" => {
                self.simulate_system_program(instruction, account_keys, modified_accounts)?;
            }
            "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA" => {
                self.simulate_token_program(instruction, account_keys, modified_accounts)?;
            }
            "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8" => {
                self.simulate_raydium_amm(instruction, account_keys, modified_accounts)?;
            }
            "whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc" => {
                self.simulate_orca_whirlpool(instruction, account_keys, modified_accounts)?;
            }
            _ => {
                return Ok(());
            }
        }
        
        Ok(())
    }

    fn simulate_system_program(
        &self,
        instruction: &solana_sdk::instruction::CompiledInstruction,
        account_keys: &[Pubkey],
        modified_accounts: &mut HashMap<Pubkey, AccountState>,
    ) -> Result<()> {
                if instruction.data.is_empty() {
            return Ok(());
        }
        
        let instruction_type = instruction.data[0];
        match instruction_type {
            0 => { // CreateAccount
                if instruction.accounts.len() < 2 {
                    return Err(anyhow::anyhow!("Invalid create account instruction"));
                }
                
                let from_idx = instruction.accounts[0] as usize;
                let to_idx = instruction.accounts[1] as usize;
                let from_pubkey = &account_keys[from_idx];
                let to_pubkey = &account_keys[to_idx];
                
                let lamports = u64::from_le_bytes(instruction.data[1..9].try_into()?);
                let space = u64::from_le_bytes(instruction.data[9..17].try_into()?);
                
                if let Some(from_account) = modified_accounts.get_mut(from_pubkey) {
                    if from_account.lamports < lamports {
                        return Err(anyhow::anyhow!("Insufficient lamports"));
                    }
                    from_account.lamports -= lamports;
                }
                
                let rent = Rent::default();
                let min_balance = rent.minimum_balance(space as usize);
                
                modified_accounts.insert(*to_pubkey, AccountState {
                    lamports: lamports.max(min_balance),
                    data: vec![0; space as usize],
                    owner: *program_id,
                    executable: false,
                    rent_epoch: 0,
                });
            }
            2 => { // Transfer
                if instruction.accounts.len() < 2 {
                    return Err(anyhow::anyhow!("Invalid transfer instruction"));
                }
                
                let from_idx = instruction.accounts[0] as usize;
                let to_idx = instruction.accounts[1] as usize;
                let from_pubkey = &account_keys[from_idx];
                let to_pubkey = &account_keys[to_idx];
                
                let lamports = u64::from_le_bytes(instruction.data[1..9].try_into()?);
                
                if let Some(from_account) = modified_accounts.get_mut(from_pubkey) {
                    if from_account.lamports < lamports {
                        return Err(anyhow::anyhow!("Insufficient lamports"));
                    }
                    from_account.lamports -= lamports;
                }
                
                if let Some(to_account) = modified_accounts.get_mut(to_pubkey) {
                    to_account.lamports += lamports;
                }
            }
            _ => {}
        }
        
        Ok(())
    }

    fn simulate_token_program(
        &self,
        instruction: &solana_sdk::instruction::CompiledInstruction,
        account_keys: &[Pubkey],
        modified_accounts: &mut HashMap<Pubkey, AccountState>,
    ) -> Result<()> {
        if instruction.data.is_empty() {
            return Ok(());
        }
        
        let instruction_type = instruction.data[0];
        match instruction_type {
            3 => { // Transfer
                if instruction.accounts.len() < 3 {
                    return Err(anyhow::anyhow!("Invalid token transfer"));
                }
                
                let source_idx = instruction.accounts[0] as usize;
                let dest_idx = instruction.accounts[1] as usize;
                let source_pubkey = &account_keys[source_idx];
                let dest_pubkey = &account_keys[dest_idx];
                
                let amount = u64::from_le_bytes(instruction.data[1..9].try_into()?);
                
                if let Some(source_account) = modified_accounts.get_mut(source_pubkey) {
                    if source_account.data.len() >= 72 {
                        let mut source_amount = u64::from_le_bytes(source_account.data[64..72].try_into()?);
                        if source_amount < amount {
                            return Err(anyhow::anyhow!("Insufficient token balance"));
                        }
                        source_amount -= amount;
                        source_account.data[64..72].copy_from_slice(&source_amount.to_le_bytes());
                    }
                }
                
                if let Some(dest_account) = modified_accounts.get_mut(dest_pubkey) {
                    if dest_account.data.len() >= 72 {
                        let mut dest_amount = u64::from_le_bytes(dest_account.data[64..72].try_into()?);
                        dest_amount += amount;
                        dest_account.data[64..72].copy_from_slice(&dest_amount.to_le_bytes());
                    }
                }
            }
            _ => {}
        }
        
        Ok(())
    }

    fn simulate_raydium_amm(
        &self,
        instruction: &solana_sdk::instruction::CompiledInstruction,
        account_keys: &[Pubkey],
        modified_accounts: &mut HashMap<Pubkey, AccountState>,
    ) -> Result<()> {
        if instruction.data.len() < 17 {
            return Ok(());
        }
        
        let instruction_type = instruction.data[0];
        if instruction_type == 9 { // Swap
            if instruction.accounts.len() < 17 {
                return Err(anyhow::anyhow!("Invalid Raydium swap"));
            }
            
            let amount_in = u64::from_le_bytes(instruction.data[1..9].try_into()?);
            let min_amount_out = u64::from_le_bytes(instruction.data[9..17].try_into()?);
            
            let pool_coin_token_account = &account_keys[instruction.accounts[4] as usize];
            let pool_pc_token_account = &account_keys[instruction.accounts[5] as usize];
            
            let pool_coin_state = modified_accounts.get(pool_coin_token_account);
            let pool_pc_state = modified_accounts.get(pool_pc_token_account);
            
            if let (Some(coin_state), Some(pc_state)) = (pool_coin_state, pool_pc_state) {
                if coin_state.data.len() >= 72 && pc_state.data.len() >= 72 {
                    let coin_reserve = u64::from_le_bytes(coin_state.data[64..72].try_into()?);
                    let pc_reserve = u64::from_le_bytes(pc_state.data[64..72].try_into()?);
                    
                    let amount_out = self.calculate_amm_output(amount_in, coin_reserve, pc_reserve, 25)?;
                    
                    if amount_out < min_amount_out {
                        return Err(anyhow::anyhow!("Slippage exceeded"));
                    }
                }
            }
        }
        
        Ok(())
    }

    fn simulate_orca_whirlpool(
        &self,
        instruction: &solana_sdk::instruction::CompiledInstruction,
        account_keys: &[Pubkey],
        modified_accounts: &mut HashMap<Pubkey, AccountState>,
    ) -> Result<()> {
        if instruction.data.len() < 17 {
            return Ok(());
        }
        
        let instruction_type = instruction.data[0];
        if instruction_type == 0x0b { // Swap
            if instruction.accounts.len() < 11 {
                return Err(anyhow::anyhow!("Invalid Orca swap"));
            }
            
            let amount = u64::from_le_bytes(instruction.data[1..9].try_into()?);
            let other_amount_threshold = u64::from_le_bytes(instruction.data[9..17].try_into()?);
            let sqrt_price_limit = instruction.data[17..25].try_into().ok();
            
            let whirlpool = &account_keys[instruction.accounts[0] as usize];
            let token_vault_a = &account_keys[instruction.accounts[4] as usize];
            let token_vault_b = &account_keys[instruction.accounts[5] as usize];
            
            if let Some(whirlpool_state) = modified_accounts.get(whirlpool) {
                if whirlpool_state.data.len() >= 312 {
                    let sqrt_price = u128::from_le_bytes(whirlpool_state.data[65..81].try_into()?);
                    let liquidity = u128::from_le_bytes(whirlpool_state.data[81..97].try_into()?);
                    
                    if liquidity == 0 {
                        return Err(anyhow::anyhow!("No liquidity"));
                    }
                    
                    if let Some(sqrt_limit) = sqrt_price_limit {
                        let limit = u128::from_le_bytes(sqrt_limit);
                        if limit > 0 && ((limit > sqrt_price) != (amount > 0)) {
                            return Err(anyhow::anyhow!("Price limit exceeded"));
                        }
                    }
                }
            }
        }
        
        Ok(())
    }

    fn calculate_amm_output(
        &self,
        amount_in: u64,
        reserve_in: u64,
        reserve_out: u64,
        fee_bps: u16,
    ) -> Result<u64> {
        if reserve_in == 0 || reserve_out == 0 {
            return Err(anyhow::anyhow!("Invalid reserves"));
        }
        
        let fee_multiplier = 10000u64.saturating_sub(fee_bps as u64);
        let amount_in_with_fee = amount_in.saturating_mul(fee_multiplier);
        let numerator = amount_in_with_fee.saturating_mul(reserve_out);
        let denominator = reserve_in.saturating_mul(10000).saturating_add(amount_in_with_fee);
        
        if denominator == 0 {
            return Err(anyhow::anyhow!("Division by zero"));
        }
        
        Ok(numerator / denominator)
    }

    fn estimate_compute_units(
        &self,
        program_id: &Pubkey,
        instruction: &solana_sdk::instruction::CompiledInstruction,
    ) -> u64 {
        match program_id.to_string().as_str() {
            "11111111111111111111111111111111" => match instruction.data.get(0) {
                Some(0) => 1500,  // CreateAccount
                Some(2) => 600,   // Transfer
                _ => 800,
            },
            "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA" => match instruction.data.get(0) {
                Some(3) => 4000,  // Transfer
                Some(7) => 5500,  // MintTo
                Some(8) => 5200,  // Burn
                _ => 4500,
            },
            "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8" => 85000,  // Raydium AMM
            "whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc" => 125000, // Orca Whirlpool
            "JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4" => 75000,  // Jupiter
            _ => 35000,
        }
    }

    fn extract_priority_fee(&self, transaction: &VersionedTransaction) -> u64 {
        let message = transaction.message();
        let instructions = match message {
            VersionedMessage::Legacy(msg) => &msg.instructions,
            VersionedMessage::V0(msg) => &msg.instructions,
        };
        
        for instruction in instructions {
            if let Ok(compute_budget_ix) = ComputeBudgetInstruction::try_from_slice(&instruction.data) {
                match compute_budget_ix {
                    ComputeBudgetInstruction::SetComputeUnitPrice(price) => return price,
                    _ => continue,
                }
            }
        }
        
        0
    }

    fn calculate_gas_cost(&self, transaction: &VersionedTransaction, priority_fee: u64) -> u64 {
        let base_fee = 5000u64;
        let signature_fee = transaction.signatures.len() as u64 * 5000;
        
        let message = transaction.message();
        let compute_units = match message {
            VersionedMessage::Legacy(msg) => {
                msg.instructions.len() as u64 * 200_000
            }
            VersionedMessage::V0(msg) => {
                msg.instructions.len() as u64 * 200_000
            }
        };
        
        let priority_cost = (priority_fee * compute_units) / 1_000_000;
        
        base_fee + signature_fee + priority_cost
    }

    fn calculate_profit(
        &self,
        modified_accounts: &HashMap<Pubkey, AccountState>,
        gas_cost: u64,
    ) -> i64 {
        let mut total_value_change = 0i64;
        
        for (pubkey, state) in modified_accounts {
            if state.owner == spl_token::id() && state.data.len() >= 72 {
                let mint = Pubkey::new(&state.data[0..32]);
                let amount = u64::from_le_bytes(state.data[64..72].try_into().unwrap_or([0u8; 8]));
                
                let token_price = self.get_token_price(&mint).unwrap_or(0.0);
                let value_change = (amount as f64 * token_price) as i64;
                total_value_change += value_change;
            }
        }
        
        total_value_change.saturating_sub(gas_cost as i64)
    }

    fn get_token_price(&self, mint: &Pubkey) -> Option<f64> {
        match mint.to_string().as_str() {
            "So11111111111111111111111111111111111111112" => Some(1.0), // WSOL
            "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v" => Some(1.0), // USDC
            "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB" => Some(1.0), // USDT
            _ => None,
        }
    }

        async fn get_latest_state_version(&self) -> Result<Arc<StateVersion>> {
        let versions = self.state_versions.read().await;
        versions
            .last()
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("No state version available"))
    }

    fn get_or_fetch_account(
        &self,
        pubkey: &Pubkey,
        state_version: &Arc<StateVersion>,
    ) -> Result<AccountState> {
        if let Some(account) = state_version.accounts.get(pubkey) {
            return Ok(account.clone());
        }
        
        let account = self.rpc_client
            .get_account_with_commitment(pubkey, CommitmentConfig::processed())?
            .value
            .ok_or_else(|| anyhow::anyhow!("Account not found: {}", pubkey))?;
        
        let account_state = AccountState::from(&account);
        state_version.accounts.insert(*pubkey, account_state.clone());
        
        Ok(account_state)
    }

    pub async fn simulate_bundle(
        &self,
        bundle: Vec<VersionedTransaction>,
    ) -> Result<BundleSimulationResult> {
        let start_time = Instant::now();
        let state_version = self.get_latest_state_version().await?;
        let mut cumulative_state = HashMap::new();
        let mut results = Vec::new();
        let mut total_profit = 0i64;
        let mut total_gas = 0u64;
        let mut bundle_success = true;

        for (idx, transaction) in bundle.iter().enumerate() {
            let mut tx_state = cumulative_state.clone();
            
            let result = self.simulate_transaction_with_state(
                transaction,
                &state_version,
                &mut tx_state,
            )?;
            
            if !result.success {
                bundle_success = false;
                results.push(result);
                break;
            }
            
            cumulative_state.extend(tx_state);
            total_profit += result.profit;
            total_gas += result.gas_cost;
            results.push(result);
        }

        Ok(BundleSimulationResult {
            success: bundle_success,
            individual_results: results,
            total_profit,
            total_gas_cost: total_gas,
            simulation_time_us: start_time.elapsed().as_micros() as u64,
            final_state: cumulative_state,
        })
    }

    fn simulate_transaction_with_state(
        &self,
        transaction: &VersionedTransaction,
        state_version: &Arc<StateVersion>,
        state: &mut HashMap<Pubkey, AccountState>,
    ) -> Result<SimulationResult> {
        let start_time = Instant::now();
        let message = transaction.message();
        let account_keys = message.static_account_keys();
        let mut total_compute_units = 0u64;
        
        let priority_fee = self.extract_priority_fee(transaction);
        let gas_cost = self.calculate_gas_cost(transaction, priority_fee);
        
        for (idx, pubkey) in account_keys.iter().enumerate() {
            if message.is_maybe_writable(idx) && !state.contains_key(pubkey) {
                let account_state = self.get_or_fetch_account(pubkey, state_version)?;
                state.insert(*pubkey, account_state);
            }
        }

        let fee_payer = &account_keys[0];
        if let Some(fee_payer_state) = state.get_mut(fee_payer) {
            if fee_payer_state.lamports < gas_cost {
                return Ok(SimulationResult {
                    success: false,
                    compute_units_consumed: 0,
                    logs: vec!["Insufficient balance for fees".to_string()],
                    modified_accounts: state.clone(),
                    profit: -1,
                    gas_cost,
                    simulation_time_us: start_time.elapsed().as_micros() as u64,
                });
            }
            fee_payer_state.lamports = fee_payer_state.lamports.saturating_sub(gas_cost);
        }

        let instructions = match message {
            VersionedMessage::Legacy(msg) => &msg.instructions,
            VersionedMessage::V0(msg) => &msg.instructions,
        };

        for (idx, instruction) in instructions.iter().enumerate() {
            let program_id = account_keys[instruction.program_id_index as usize];
            let compute_units = self.estimate_compute_units(&program_id, instruction);
            total_compute_units += compute_units;
            
            if total_compute_units > MAX_COMPUTE_UNITS as u64 {
                return Ok(SimulationResult {
                    success: false,
                    compute_units_consumed: total_compute_units,
                    logs: vec![format!("Compute units exceeded at instruction {}", idx)],
                    modified_accounts: state.clone(),
                    profit: -(gas_cost as i64),
                    gas_cost,
                    simulation_time_us: start_time.elapsed().as_micros() as u64,
                });
            }

            if let Err(e) = self.simulate_instruction(
                instruction,
                account_keys,
                state,
                state_version,
            ) {
                return Ok(SimulationResult {
                    success: false,
                    compute_units_consumed: total_compute_units,
                    logs: vec![format!("Instruction {} failed: {}", idx, e)],
                    modified_accounts: state.clone(),
                    profit: -(gas_cost as i64),
                    gas_cost,
                    simulation_time_us: start_time.elapsed().as_micros() as u64,
                });
            }
        }

        let profit = self.calculate_profit(state, gas_cost);

        Ok(SimulationResult {
            success: true,
            compute_units_consumed: total_compute_units,
            logs: vec!["Simulation successful".to_string()],
            modified_accounts: state.clone(),
            profit,
            gas_cost,
            simulation_time_us: start_time.elapsed().as_micros() as u64,
        })
    }

    pub async fn get_account_state(&self, pubkey: &Pubkey) -> Result<AccountState> {
        let state_version = self.get_latest_state_version().await?;
        self.get_or_fetch_account(pubkey, &state_version)
    }

    pub fn get_current_slot(&self) -> u64 {
        self.current_slot.load(Ordering::SeqCst)
    }

    pub fn get_priority_fee(&self) -> f64 {
        *self.priority_fee_cache.read()
    }

    pub async fn prefetch_accounts(&self, accounts: Vec<Pubkey>) -> Result<()> {
        let state_version = self.get_latest_state_version().await?;
        let chunk_size = 100;
        
        for chunk in accounts.chunks(chunk_size) {
            let accounts_data = self.rpc_client
                .get_multiple_accounts_with_commitment(chunk, CommitmentConfig::processed())?
                .value;
            
            for (pubkey, account_opt) in chunk.iter().zip(accounts_data.iter()) {
                if let Some(account) = account_opt {
                    let account_state = AccountState::from(account);
                    state_version.accounts.insert(*pubkey, account_state);
                }
            }
        }
        
        Ok(())
    }

    pub fn stop(&self) {
        self.is_running.store(false, Ordering::SeqCst);
    }
}

#[derive(Clone)]
pub struct BundleSimulationResult {
    pub success: bool,
    pub individual_results: Vec<SimulationResult>,
    pub total_profit: i64,
    pub total_gas_cost: u64,
    pub simulation_time_us: u64,
    pub final_state: HashMap<Pubkey, AccountState>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use solana_sdk::{
        signature::Keypair,
        signer::Signer,
        system_instruction,
    };

    #[tokio::test]
    async fn test_simulator_creation() {
        let simulator = ParallelStateSimulator::new("https://api.mainnet-beta.solana.com")
            .expect("Failed to create simulator");
        
        assert_eq!(simulator.get_current_slot(), 0);
    }

    #[tokio::test]
    async fn test_state_update() {
        let simulator = ParallelStateSimulator::new("https://api.mainnet-beta.solana.com")
            .expect("Failed to create simulator");
        
        simulator.start().await.expect("Failed to start simulator");
        tokio::time::sleep(Duration::from_secs(1)).await;
        
        assert!(simulator.get_current_slot() > 0);
    }

    #[tokio::test]
    async fn test_simple_transfer_simulation() {
        let simulator = ParallelStateSimulator::new("https://api.mainnet-beta.solana.com")
            .expect("Failed to create simulator");
        
        simulator.start().await.expect("Failed to start simulator");
        
        let from = Keypair::new();
        let to = Keypair::new();
        
        let ix = system_instruction::transfer(&from.pubkey(), &to.pubkey(), 1_000_000);
        let message = Message::new(&[ix], Some(&from.pubkey()));
        let tx = Transaction::new(&[&from], message, Hash::default());
        let versioned_tx = VersionedTransaction::from(tx);
        
        let results = simulator.simulate_transactions_parallel(vec![versioned_tx])
            .await
            .expect("Simulation failed");
        
        assert_eq!(results.len(), 1);
        assert!(!results[0].success); // Should fail due to insufficient balance
    }
}

