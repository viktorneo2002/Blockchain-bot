use std::sync::{Arc, RwLock};
use std::collections::{HashMap, VecDeque};
use std::time::{Duration, Instant};
use solana_sdk::{
    account::Account,
    clock::Slot,
    commitment_config::CommitmentLevel,
    compute_budget::ComputeBudgetInstruction,
    hash::Hash,
    instruction::Instruction,
    pubkey::Pubkey,
    signature::Keypair,
    signer::Signer,
    transaction::{Transaction, VersionedTransaction},
};
use solana_program::{
    program_pack::Pack,
    rent::Rent,
    system_instruction,
};
use spl_token::state::Account as TokenAccount;
use spl_token_2022::state::Account as Token2022Account;
use rayon::prelude::*;
use dashmap::DashMap;
use smallvec::SmallVec;
use tokio::sync::{mpsc, oneshot};

const MAX_SPECULATIVE_DEPTH: usize = 8;
const MAX_SIMULATION_TIME_MS: u64 = 50;
const MAX_CACHED_STATES: usize = 10000;
const LAMPORTS_PER_SOL: u64 = 1_000_000_000;
const MIN_PROFIT_THRESHOLD: u64 = 100_000;
const MAX_COMPUTE_UNITS: u32 = 1_400_000;
const PRIORITY_FEE_PERCENTILE: f64 = 0.95;
const STATE_CACHE_TTL_SLOTS: u64 = 5;

#[derive(Clone, Debug)]
pub struct AccountState {
    pub lamports: u64,
    pub owner: Pubkey,
    pub data: Vec<u8>,
    pub executable: bool,
    pub rent_epoch: u64,
}

#[derive(Clone, Debug)]
pub struct SimulationResult {
    pub profit: i64,
    pub gas_used: u64,
    pub priority_fee: u64,
    pub success: bool,
    pub state_changes: HashMap<Pubkey, AccountState>,
    pub logs: Vec<String>,
    pub consumed_compute_units: u32,
}

#[derive(Clone)]
pub struct SpeculativeState {
    pub slot: Slot,
    pub block_hash: Hash,
    pub accounts: Arc<DashMap<Pubkey, AccountState>>,
    pub timestamp: Instant,
}

pub struct ExecutionPath {
    pub transactions: Vec<VersionedTransaction>,
    pub cumulative_profit: i64,
    pub state: SpeculativeState,
    pub depth: usize,
}

pub struct SpeculativeExecutionEngine {
    state_cache: Arc<RwLock<HashMap<Hash, SpeculativeState>>>,
    execution_paths: Arc<RwLock<VecDeque<ExecutionPath>>>,
    profit_calculator: Arc<ProfitCalculator>,
    state_manager: Arc<StateManager>,
    max_parallel_simulations: usize,
}

struct ProfitCalculator {
    token_prices: Arc<DashMap<Pubkey, f64>>,
    base_fee: u64,
    priority_fee_cache: Arc<RwLock<Vec<u64>>>,
}

struct StateManager {
    checkpoints: Arc<DashMap<Hash, SpeculativeState>>,
    dirty_accounts: Arc<DashMap<Pubkey, AccountState>>,
}

impl AccountState {
    fn from_account(account: &Account) -> Self {
        Self {
            lamports: account.lamports,
            owner: account.owner,
            data: account.data.clone(),
            executable: account.executable,
            rent_epoch: account.rent_epoch,
        }
    }

    fn to_account(&self) -> Account {
        Account {
            lamports: self.lamports,
            owner: self.owner,
            data: self.data.clone(),
            executable: self.executable,
            rent_epoch: self.rent_epoch,
        }
    }
}

impl ProfitCalculator {
    fn new() -> Self {
        Self {
            token_prices: Arc::new(DashMap::new()),
            base_fee: 5000,
            priority_fee_cache: Arc::new(RwLock::new(Vec::with_capacity(1000))),
        }
    }

    fn calculate_transaction_profit(
        &self,
        pre_state: &HashMap<Pubkey, AccountState>,
        post_state: &HashMap<Pubkey, AccountState>,
        gas_used: u64,
        priority_fee: u64,
    ) -> i64 {
        let mut profit = 0i64;

        for (pubkey, post_account) in post_state {
            let pre_account = pre_state.get(pubkey);
            
            if let Some(pre) = pre_account {
                let lamport_diff = post_account.lamports as i64 - pre.lamports as i64;
                profit += lamport_diff;

                if post_account.owner == spl_token::id() || post_account.owner == spl_token_2022::id() {
                    profit += self.calculate_token_profit(pre, post_account, pubkey);
                }
            } else {
                profit += post_account.lamports as i64;
            }
        }

        let total_fee = (gas_used * self.base_fee + priority_fee) as i64;
        profit - total_fee
    }

    fn calculate_token_profit(
        &self,
        pre_state: &AccountState,
        post_state: &AccountState,
        token_mint: &Pubkey,
    ) -> i64 {
        if pre_state.data.len() < TokenAccount::LEN || post_state.data.len() < TokenAccount::LEN {
            return 0;
        }

        let pre_token = match TokenAccount::unpack(&pre_state.data) {
            Ok(account) => account,
            Err(_) => return 0,
        };

        let post_token = match TokenAccount::unpack(&post_state.data) {
            Ok(account) => account,
            Err(_) => return 0,
        };

        let amount_diff = post_token.amount as i64 - pre_token.amount as i64;
        if let Some(price) = self.token_prices.get(token_mint) {
            (amount_diff as f64 * *price) as i64
        } else {
            0
        }
    }

    fn get_optimal_priority_fee(&self) -> u64 {
        let cache = self.priority_fee_cache.read().unwrap();
        if cache.is_empty() {
            return 10_000;
        }

        let index = ((cache.len() as f64) * PRIORITY_FEE_PERCENTILE) as usize;
        cache[index.min(cache.len() - 1)]
    }
}

impl StateManager {
    fn new() -> Self {
        Self {
            checkpoints: Arc::new(DashMap::new()),
            dirty_accounts: Arc::new(DashMap::new()),
        }
    }

    fn create_checkpoint(&self, state: &SpeculativeState) -> Hash {
        let checkpoint_hash = state.block_hash;
        self.checkpoints.insert(checkpoint_hash, state.clone());
        checkpoint_hash
    }

    fn restore_checkpoint(&self, hash: &Hash) -> Option<SpeculativeState> {
        self.checkpoints.get(hash).map(|entry| entry.clone())
    }

    fn apply_state_changes(
        &self,
        base_state: &mut SpeculativeState,
        changes: &HashMap<Pubkey, AccountState>,
    ) {
        for (pubkey, account_state) in changes {
            base_state.accounts.insert(*pubkey, account_state.clone());
            self.dirty_accounts.insert(*pubkey, account_state.clone());
        }
    }

    fn get_account(&self, state: &SpeculativeState, pubkey: &Pubkey) -> Option<AccountState> {
        state.accounts.get(pubkey).map(|entry| entry.clone())
    }
}

impl SpeculativeExecutionEngine {
    pub fn new(max_parallel_simulations: usize) -> Self {
        Self {
            state_cache: Arc::new(RwLock::new(HashMap::new())),
            execution_paths: Arc::new(RwLock::new(VecDeque::new())),
            profit_calculator: Arc::new(ProfitCalculator::new()),
            state_manager: Arc::new(StateManager::new()),
            max_parallel_simulations,
        }
    }

    pub async fn simulate_transaction(
        &self,
        tx: &VersionedTransaction,
        state: &SpeculativeState,
    ) -> Result<SimulationResult, Box<dyn std::error::Error + Send + Sync>> {
        let start = Instant::now();
        let mut simulated_state = state.clone();
        let mut state_changes = HashMap::new();
        let mut logs = Vec::new();
        let mut consumed_compute_units = 0u32;

        let message = tx.message();
        let account_keys = message.static_account_keys();
        
        for (i, key) in account_keys.iter().enumerate() {
            if let Some(account) = self.state_manager.get_account(&simulated_state, key) {
                if message.is_maybe_writable(i) {
                    state_changes.insert(*key, account);
                }
            }
        }

        let instructions = message.instructions();
        for instruction in instructions {
            consumed_compute_units += self.estimate_compute_units(&instruction);
            if consumed_compute_units > MAX_COMPUTE_UNITS {
                return Ok(SimulationResult {
                    profit: 0,
                    gas_used: consumed_compute_units as u64,
                    priority_fee: 0,
                    success: false,
                    state_changes,
                    logs: vec!["Compute budget exceeded".to_string()],
                    consumed_compute_units,
                });
            }

            if let Err(e) = self.simulate_instruction(&instruction, &mut state_changes, &mut logs) {
                logs.push(format!("Instruction failed: {}", e));
                return Ok(SimulationResult {
                    profit: 0,
                    gas_used: consumed_compute_units as u64,
                    priority_fee: 0,
                    success: false,
                    state_changes,
                    logs,
                    consumed_compute_units,
                });
            }
        }

        let priority_fee = self.extract_priority_fee(tx);
        let gas_used = consumed_compute_units as u64;
        
        let pre_state: HashMap<Pubkey, AccountState> = state_changes.keys()
            .filter_map(|key| self.state_manager.get_account(state, key).map(|acc| (*key, acc)))
            .collect();

        let profit = self.profit_calculator.calculate_transaction_profit(
            &pre_state,
            &state_changes,
            gas_used,
            priority_fee,
        );

        if start.elapsed().as_millis() > MAX_SIMULATION_TIME_MS as u128 {
            logs.push("Simulation timeout".to_string());
        }

        Ok(SimulationResult {
            profit,
            gas_used,
            priority_fee,
            success: true,
            state_changes,
            logs,
            consumed_compute_units,
        })
    }

    pub async fn execute_speculative_path(
        &self,
        transactions: Vec<VersionedTransaction>,
        base_state: SpeculativeState,
    ) -> Result<Vec<SimulationResult>, Box<dyn std::error::Error + Send + Sync>> {
        let mut current_state = base_state;
        let mut results = Vec::with_capacity(transactions.len());
        let mut cumulative_profit = 0i64;

        for tx in transactions {
            let result = self.simulate_transaction(&tx, &current_state).await?;
            
            if !result.success || result.profit < MIN_PROFIT_THRESHOLD as i64 {
                break;
            }

            self.state_manager.apply_state_changes(&mut current_state, &result.state_changes);
            cumulative_profit += result.profit;
            results.push(result);
        }

        Ok(results)
    }

    pub async fn find_optimal_execution_sequence(
        &self,
        candidate_txs: Vec<VersionedTransaction>,
        base_state: SpeculativeState,
        max_depth: usize,
    ) -> Option<Vec<VersionedTransaction>> {
        let depth = max_depth.min(MAX_SPECULATIVE_DEPTH);
        let mut best_sequence = Vec::new();
        let mut best_profit = 0i64;

        let chunks: Vec<_> = candidate_txs.chunks(self.max_parallel_simulations).collect();
        
        for chunk in chunks {
            let simulations: Vec<_> = chunk.par_iter()
                .filter_map(|tx| {
                    let state = base_state.clone();
                    tokio::task::block_in_place(|| {
                        tokio::runtime::Handle::current().block_on(async {
                            self.simulate_transaction(tx, &state).await.ok()
                        })
                    })
                })
                .collect();

            for (i, result) in simulations.iter().enumerate() {
                if result.success && result.profit > best_profit {
                    best_profit = result.profit;
                    best_sequence = vec![chunk[i].clone()];
                    
                    if depth > 1 {
                        let mut next_state = base_state.clone();
                        self.state_manager.apply_state_changes(&mut next_state, &result.state_changes);
                        
                        let remaining_txs: Vec<_> = candidate_txs.iter()
                            .filter(|tx| !std::ptr::eq(*tx, &chunk[i]))
                            .cloned()
                            .collect();
                        
                        if let Some(mut sub_sequence) = tokio::task::block_in_place(|| {
                            tokio::runtime::Handle::current().block_on(async {
                                self.find_optimal_execution_sequence(remaining_txs, next_state, depth - 1).await
                            })
                        }) {
                            best_sequence.append(&mut sub_sequence);
                        }
                    }
                }
            }
        }

        if best_sequence.is_empty() {
            None
        } else {
            Some(best_sequence)
        }
    }

        fn simulate_instruction(
        &self,
        instruction: &Instruction,
        state_changes: &mut HashMap<Pubkey, AccountState>,
        logs: &mut Vec<String>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let program_id = instruction.program_id;
        let accounts = &instruction.accounts;
        
        match program_id {
            id if id == &system_program::id() => {
                self.simulate_system_instruction(instruction, state_changes, logs)
            }
            id if id == &spl_token::id() || id == &spl_token_2022::id() => {
                self.simulate_token_instruction(instruction, state_changes, logs)
            }
            id if id == &spl_associated_token_account::id() => {
                self.simulate_ata_instruction(instruction, state_changes, logs)
            }
            _ => {
                self.simulate_generic_instruction(instruction, state_changes, logs)
            }
        }
    }

    fn simulate_system_instruction(
        &self,
        instruction: &Instruction,
        state_changes: &mut HashMap<Pubkey, AccountState>,
        logs: &mut Vec<String>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        use solana_sdk::system_instruction::SystemInstruction;
        
        let ix_type = bincode::deserialize::<SystemInstruction>(&instruction.data)?;
        
        match ix_type {
            SystemInstruction::Transfer { lamports } => {
                let from_pubkey = instruction.accounts[0].pubkey;
                let to_pubkey = instruction.accounts[1].pubkey;
                
                let mut from_account = state_changes.get(&from_pubkey)
                    .cloned()
                    .unwrap_or_else(|| AccountState {
                        lamports: 0,
                        owner: system_program::id(),
                        data: vec![],
                        executable: false,
                        rent_epoch: 0,
                    });
                
                let mut to_account = state_changes.get(&to_pubkey)
                    .cloned()
                    .unwrap_or_else(|| AccountState {
                        lamports: 0,
                        owner: system_program::id(),
                        data: vec![],
                        executable: false,
                        rent_epoch: 0,
                    });
                
                if from_account.lamports < lamports {
                    return Err("Insufficient lamports".into());
                }
                
                from_account.lamports -= lamports;
                to_account.lamports += lamports;
                
                state_changes.insert(from_pubkey, from_account);
                state_changes.insert(to_pubkey, to_account);
                
                logs.push(format!("Transfer: {} lamports from {} to {}", lamports, from_pubkey, to_pubkey));
            }
            _ => {
                logs.push("Unsupported system instruction".to_string());
            }
        }
        
        Ok(())
    }

    fn simulate_token_instruction(
        &self,
        instruction: &Instruction,
        state_changes: &mut HashMap<Pubkey, AccountState>,
        logs: &mut Vec<String>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        use spl_token::instruction::TokenInstruction;
        
        let ix_type = TokenInstruction::unpack(&instruction.data)?;
        
        match ix_type {
            TokenInstruction::Transfer { amount } => {
                let source_pubkey = instruction.accounts[0].pubkey;
                let dest_pubkey = instruction.accounts[1].pubkey;
                
                let mut source_account = state_changes.get(&source_pubkey)
                    .cloned()
                    .ok_or("Source account not found")?;
                
                let mut dest_account = state_changes.get(&dest_pubkey)
                    .cloned()
                    .ok_or("Destination account not found")?;
                
                let mut source_token = TokenAccount::unpack(&source_account.data)?;
                let mut dest_token = TokenAccount::unpack(&dest_account.data)?;
                
                if source_token.amount < amount {
                    return Err("Insufficient token balance".into());
                }
                
                source_token.amount -= amount;
                dest_token.amount += amount;
                
                source_account.data = TokenAccount::pack(source_token)?;
                dest_account.data = TokenAccount::pack(dest_token)?;
                
                state_changes.insert(source_pubkey, source_account);
                state_changes.insert(dest_pubkey, dest_account);
                
                logs.push(format!("Token transfer: {} from {} to {}", amount, source_pubkey, dest_pubkey));
            }
            TokenInstruction::TransferChecked { amount, decimals } => {
                let source_pubkey = instruction.accounts[0].pubkey;
                let dest_pubkey = instruction.accounts[2].pubkey;
                
                let mut source_account = state_changes.get(&source_pubkey)
                    .cloned()
                    .ok_or("Source account not found")?;
                
                let mut dest_account = state_changes.get(&dest_pubkey)
                    .cloned()
                    .ok_or("Destination account not found")?;
                
                let mut source_token = TokenAccount::unpack(&source_account.data)?;
                let mut dest_token = TokenAccount::unpack(&dest_account.data)?;
                
                if source_token.amount < amount {
                    return Err("Insufficient token balance".into());
                }
                
                source_token.amount -= amount;
                dest_token.amount += amount;
                
                source_account.data = TokenAccount::pack(source_token)?;
                dest_account.data = TokenAccount::pack(dest_token)?;
                
                state_changes.insert(source_pubkey, source_account);
                state_changes.insert(dest_pubkey, dest_account);
                
                logs.push(format!("Token transfer checked: {} (decimals: {})", amount, decimals));
            }
            _ => {
                logs.push("Token instruction simulated".to_string());
            }
        }
        
        Ok(())
    }

    fn simulate_ata_instruction(
        &self,
        instruction: &Instruction,
        state_changes: &mut HashMap<Pubkey, AccountState>,
        logs: &mut Vec<String>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let payer = instruction.accounts[0].pubkey;
        let ata = instruction.accounts[1].pubkey;
        
        let rent = Rent::default();
        let required_lamports = rent.minimum_balance(TokenAccount::LEN);
        
        let mut payer_account = state_changes.get(&payer)
            .cloned()
            .ok_or("Payer account not found")?;
        
        if payer_account.lamports < required_lamports {
            return Err("Insufficient lamports for ATA creation".into());
        }
        
        payer_account.lamports -= required_lamports;
        state_changes.insert(payer, payer_account);
        
        let ata_account = AccountState {
            lamports: required_lamports,
            owner: spl_token::id(),
            data: vec![0u8; TokenAccount::LEN],
            executable: false,
            rent_epoch: 0,
        };
        
        state_changes.insert(ata, ata_account);
        logs.push(format!("ATA created: {}", ata));
        
        Ok(())
    }

    fn simulate_generic_instruction(
        &self,
        instruction: &Instruction,
        state_changes: &mut HashMap<Pubkey, AccountState>,
        logs: &mut Vec<String>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        for (i, account_meta) in instruction.accounts.iter().enumerate() {
            if account_meta.is_writable {
                let account = state_changes.entry(account_meta.pubkey)
                    .or_insert_with(|| AccountState {
                        lamports: 0,
                        owner: instruction.program_id,
                        data: vec![0u8; 165],
                        executable: false,
                        rent_epoch: 0,
                    });
                
                logs.push(format!("Account {} marked as writable", account_meta.pubkey));
            }
        }
        
        logs.push(format!("Generic instruction for program {}", instruction.program_id));
        Ok(())
    }

    fn estimate_compute_units(&self, instruction: &Instruction) -> u32 {
        let base_cost = 150;
        let per_account_cost = 100;
        let data_cost = (instruction.data.len() as u32) * 2;
        
        let program_specific_cost = match instruction.program_id {
            id if id == system_program::id() => 300,
            id if id == spl_token::id() || id == spl_token_2022::id() => 2500,
            id if id == spl_associated_token_account::id() => 5000,
            _ => 10000,
        };
        
        base_cost + 
        (instruction.accounts.len() as u32 * per_account_cost) + 
        data_cost + 
        program_specific_cost
    }

    fn extract_priority_fee(&self, tx: &VersionedTransaction) -> u64 {
        let message = tx.message();
        let instructions = message.instructions();
        
        for instruction in instructions {
            if instruction.program_id == solana_sdk::compute_budget::id() {
                if let Ok(ix_type) = bincode::deserialize::<ComputeBudgetInstruction>(&instruction.data) {
                    match ix_type {
                        ComputeBudgetInstruction::SetComputeUnitPrice(microlamports) => {
                            return microlamports;
                        }
                        _ => continue,
                    }
                }
            }
        }
        
        self.profit_calculator.get_optimal_priority_fee()
    }

    pub fn update_token_price(&self, mint: Pubkey, price: f64) {
        self.profit_calculator.token_prices.insert(mint, price);
    }

    pub fn update_priority_fee_sample(&self, fee: u64) {
        let mut cache = self.profit_calculator.priority_fee_cache.write().unwrap();
        cache.push(fee);
        
        if cache.len() > 1000 {
            cache.remove(0);
        }
        
        cache.sort_unstable();
    }

    pub fn cleanup_stale_cache(&self, current_slot: Slot) {
        let mut cache = self.state_cache.write().unwrap();
        let mut states_to_remove = Vec::new();
        
        for (hash, state) in cache.iter() {
            if current_slot.saturating_sub(state.slot) > STATE_CACHE_TTL_SLOTS {
                states_to_remove.push(*hash);
            }
        }
        
        for hash in states_to_remove {
            cache.remove(&hash);
        }
        
        let checkpoints = &self.state_manager.checkpoints;
        checkpoints.retain(|_, state| {
            current_slot.saturating_sub(state.slot) <= STATE_CACHE_TTL_SLOTS
        });
    }

    pub fn get_cached_state(&self, block_hash: &Hash) -> Option<SpeculativeState> {
        self.state_cache.read().unwrap().get(block_hash).cloned()
    }

    pub fn cache_state(&self, state: SpeculativeState) {
        let mut cache = self.state_cache.write().unwrap();
        
        if cache.len() >= MAX_CACHED_STATES {
            if let Some(oldest_key) = cache.iter()
                .min_by_key(|(_, s)| s.timestamp)
                .map(|(k, _)| *k) {
                cache.remove(&oldest_key);
            }
        }
        
        cache.insert(state.block_hash, state);
    }

    pub async fn parallel_simulate_paths(
        &self,
        paths: Vec<ExecutionPath>,
    ) -> Vec<(ExecutionPath, Vec<SimulationResult>)> {
        let (tx, mut rx) = mpsc::channel(paths.len());
        
        for path in paths {
            let tx = tx.clone();
            let engine = self.clone();
            
            tokio::spawn(async move {
                let results = engine.execute_speculative_path(
                    path.transactions.clone(),
                    path.state.clone()
                ).await.unwrap_or_default();
                
                let _ = tx.send((path, results)).await;
            });
        }
        
        drop(tx);
        
        let mut all_results = Vec::new();
        while let Some(result) = rx.recv().await {
            all_results.push(result);
        }
        
        all_results.sort_by(|a, b| {
            let profit_a: i64 = a.1.iter().map(|r| r.profit).sum();
            let profit_b: i64 = b.1.iter().map(|r| r.profit).sum();
            profit_b.cmp(&profit_a)
        });
        
        all_results
    }

    pub fn create_initial_state(
        &self,
        slot: Slot,
        block_hash: Hash,
        accounts: HashMap<Pubkey, Account>,
    ) -> SpeculativeState {
        let account_states = Arc::new(DashMap::new());
        
        for (pubkey, account) in accounts {
            account_states.insert(pubkey, AccountState::from_account(&account));
        }
        
        SpeculativeState {
            slot,
            block_hash,
            accounts: account_states,
            timestamp: Instant::now(),
        }
    }
}

impl Clone for SpeculativeExecutionEngine {
    fn clone(&self) -> Self {
        Self {
            state_cache: Arc::clone(&self.state_cache),
            execution_paths: Arc::clone(&self.execution_paths),
            profit_calculator: Arc::clone(&self.profit_calculator),
            state_manager: Arc::clone(&self.state_manager),
            max_parallel_simulations: self.max_parallel_simulations,
        }
    }
}

