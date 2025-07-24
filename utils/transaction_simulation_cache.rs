use dashmap::DashMap;
use solana_client::rpc_client::RpcClient;
use solana_sdk::{
    commitment_config::CommitmentConfig,
    instruction::Instruction,
    message::Message,
    pubkey::Pubkey,
    signature::Signature,
    transaction::{Transaction, VersionedTransaction},
};
use solana_transaction_status::{
    EncodedConfirmedTransactionWithStatusMeta, TransactionConfirmationStatus,
    UiTransactionEncoding,
};
use std::{
    sync::{
        atomic::{AtomicU64, AtomicUsize, Ordering},
        Arc,
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use tokio::sync::{RwLock, Semaphore};

const CACHE_TTL_MS: u64 = 3000;
const MAX_CACHE_ENTRIES: usize = 10000;
const SIMULATION_TIMEOUT_MS: u64 = 150;
const MAX_CONCURRENT_SIMULATIONS: usize = 50;
const CACHE_CLEANUP_INTERVAL_MS: u64 = 1000;
const MAX_RETRIES: u32 = 2;
const RETRY_DELAY_MS: u64 = 25;

#[derive(Debug, Clone)]
pub struct SimulationResult {
    pub success: bool,
    pub error: Option<String>,
    pub compute_units_consumed: u64,
    pub logs: Vec<String>,
    pub accounts_modified: Vec<AccountDelta>,
    pub simulation_slot: u64,
    pub timestamp: Instant,
    pub pre_execution_accounts: Vec<(Pubkey, u64)>,
    pub post_execution_accounts: Vec<(Pubkey, u64)>,
    pub return_data: Option<Vec<u8>>,
}

#[derive(Debug, Clone)]
pub struct AccountDelta {
    pub pubkey: Pubkey,
    pub pre_balance: u64,
    pub post_balance: u64,
    pub data_modified: bool,
    pub owner_changed: bool,
}

#[derive(Debug, Clone, Hash, Eq, PartialEq)]
pub struct CacheKey {
    pub transaction_hash: [u8; 32],
    pub blockhash: [u8; 32],
    pub slot: u64,
}

pub struct TransactionSimulationCache {
    cache: Arc<DashMap<CacheKey, Arc<SimulationResult>>>,
    rpc_clients: Vec<Arc<RpcClient>>,
    current_client_index: AtomicUsize,
    simulation_semaphore: Arc<Semaphore>,
    total_simulations: AtomicU64,
    cache_hits: AtomicU64,
    cache_misses: AtomicU64,
    simulation_errors: AtomicU64,
    last_cleanup: Arc<RwLock<Instant>>,
}

impl TransactionSimulationCache {
    pub fn new(rpc_endpoints: Vec<String>) -> Self {
        let rpc_clients = rpc_endpoints
            .into_iter()
            .map(|endpoint| {
                Arc::new(RpcClient::new_with_timeout(
                    endpoint,
                    Duration::from_millis(SIMULATION_TIMEOUT_MS),
                ))
            })
            .collect();

        Self {
            cache: Arc::new(DashMap::with_capacity(MAX_CACHE_ENTRIES)),
            rpc_clients,
            current_client_index: AtomicUsize::new(0),
            simulation_semaphore: Arc::new(Semaphore::new(MAX_CONCURRENT_SIMULATIONS)),
            total_simulations: AtomicU64::new(0),
            cache_hits: AtomicU64::new(0),
            cache_misses: AtomicU64::new(0),
            simulation_errors: AtomicU64::new(0),
            last_cleanup: Arc::new(RwLock::new(Instant::now())),
        }
    }

    pub async fn simulate_transaction(
        &self,
        transaction: &Transaction,
        slot: u64,
    ) -> Result<Arc<SimulationResult>, String> {
        let cache_key = self.create_cache_key(transaction, slot);

        if let Some(cached_result) = self.get_cached_result(&cache_key).await {
            self.cache_hits.fetch_add(1, Ordering::Relaxed);
            return Ok(cached_result);
        }

        self.cache_misses.fetch_add(1, Ordering::Relaxed);
        self.cleanup_expired_entries().await;

        let _permit = self
            .simulation_semaphore
            .acquire()
            .await
            .map_err(|e| format!("Failed to acquire simulation permit: {}", e))?;

        self.total_simulations.fetch_add(1, Ordering::Relaxed);

        let result = self.perform_simulation(transaction, slot).await?;
        let result_arc = Arc::new(result);

        self.cache.insert(cache_key, result_arc.clone());

        Ok(result_arc)
    }

    pub async fn simulate_versioned_transaction(
        &self,
        transaction: &VersionedTransaction,
        slot: u64,
    ) -> Result<Arc<SimulationResult>, String> {
        let cache_key = self.create_versioned_cache_key(transaction, slot);

        if let Some(cached_result) = self.get_cached_result(&cache_key).await {
            self.cache_hits.fetch_add(1, Ordering::Relaxed);
            return Ok(cached_result);
        }

        self.cache_misses.fetch_add(1, Ordering::Relaxed);
        self.cleanup_expired_entries().await;

        let _permit = self
            .simulation_semaphore
            .acquire()
            .await
            .map_err(|e| format!("Failed to acquire simulation permit: {}", e))?;

        self.total_simulations.fetch_add(1, Ordering::Relaxed);

        let result = self.perform_versioned_simulation(transaction, slot).await?;
        let result_arc = Arc::new(result);

        self.cache.insert(cache_key, result_arc.clone());

        Ok(result_arc)
    }

    async fn get_cached_result(&self, key: &CacheKey) -> Option<Arc<SimulationResult>> {
        if let Some(entry) = self.cache.get(key) {
            let elapsed = entry.timestamp.elapsed();
            if elapsed.as_millis() < CACHE_TTL_MS as u128 {
                return Some(entry.clone());
            }
        }
        None
    }

    fn create_cache_key(&self, transaction: &Transaction, slot: u64) -> CacheKey {
        use solana_sdk::hash::hashv;
        
        let tx_bytes = bincode::serialize(transaction).unwrap_or_default();
        let hash = hashv(&[&tx_bytes, &slot.to_le_bytes()]);
        
        CacheKey {
            transaction_hash: hash.to_bytes(),
            blockhash: transaction.message.recent_blockhash.to_bytes(),
            slot,
        }
    }

    fn create_versioned_cache_key(&self, transaction: &VersionedTransaction, slot: u64) -> CacheKey {
        use solana_sdk::hash::hashv;
        
        let tx_bytes = bincode::serialize(transaction).unwrap_or_default();
        let hash = hashv(&[&tx_bytes, &slot.to_le_bytes()]);
        
        let blockhash = match &transaction.message {
            solana_sdk::message::VersionedMessage::Legacy(legacy) => legacy.recent_blockhash,
            solana_sdk::message::VersionedMessage::V0(v0) => v0.recent_blockhash,
        };
        
        CacheKey {
            transaction_hash: hash.to_bytes(),
            blockhash: blockhash.to_bytes(),
            slot,
        }
    }

    async fn perform_simulation(
        &self,
        transaction: &Transaction,
        slot: u64,
    ) -> Result<SimulationResult, String> {
        let mut last_error = None;
        
        for retry in 0..MAX_RETRIES {
            if retry > 0 {
                tokio::time::sleep(Duration::from_millis(RETRY_DELAY_MS)).await;
            }

            let client = self.get_next_client();
            
            match self.simulate_with_client(&client, transaction, slot).await {
                Ok(result) => return Ok(result),
                Err(e) => {
                    last_error = Some(e);
                    if retry < MAX_RETRIES - 1 {
                        continue;
                    }
                }
            }
        }

        self.simulation_errors.fetch_add(1, Ordering::Relaxed);
        Err(last_error.unwrap_or_else(|| "Unknown simulation error".to_string()))
    }

    async fn perform_versioned_simulation(
        &self,
        transaction: &VersionedTransaction,
        slot: u64,
    ) -> Result<SimulationResult, String> {
        let mut last_error = None;
        
        for retry in 0..MAX_RETRIES {
            if retry > 0 {
                tokio::time::sleep(Duration::from_millis(RETRY_DELAY_MS)).await;
            }

            let client = self.get_next_client();
            
            match self.simulate_versioned_with_client(&client, transaction, slot).await {
                Ok(result) => return Ok(result),
                Err(e) => {
                    last_error = Some(e);
                    if retry < MAX_RETRIES - 1 {
                        continue;
                    }
                }
            }
        }

        self.simulation_errors.fetch_add(1, Ordering::Relaxed);
        Err(last_error.unwrap_or_else(|| "Unknown simulation error".to_string()))
    }

    async fn simulate_with_client(
        &self,
        client: &Arc<RpcClient>,
        transaction: &Transaction,
        slot: u64,
    ) -> Result<SimulationResult, String> {
        let start_time = Instant::now();

        let accounts = transaction
            .message
            .account_keys
            .iter()
            .copied()
            .collect::<Vec<_>>();

        let pre_accounts = self.fetch_account_states(client, &accounts).await?;

        let sim_result = tokio::task::spawn_blocking({
            let client = client.clone();
            let transaction = transaction.clone();
            move || {
                client.simulate_transaction_with_config(
                    &transaction,
                    solana_client::rpc_config::RpcSimulateTransactionConfig {
                        sig_verify: true,
                        replace_recent_blockhash: false,
                        commitment: Some(CommitmentConfig::processed()),
                        encoding: Some(UiTransactionEncoding::Base64),
                        accounts: Some(solana_client::rpc_config::RpcSimulateTransactionAccountsConfig {
                            encoding: Some(UiTransactionEncoding::Base64),
                            addresses: accounts.iter().map(|a| a.to_string()).collect(),
                        }),
                        min_context_slot: Some(slot),
                        inner_instructions: false,
                    },
                )
            }
        })
        .await
        .map_err(|e| format!("Simulation task failed: {}", e))?;

        match sim_result {
            Ok(response) => {
                let value = response.value;
                
                let (post_accounts, account_deltas) = if value.err.is_none() {
                    let post = self.fetch_account_states(client, &accounts).await?;
                    let deltas = self.calculate_account_deltas(&pre_accounts, &post, &accounts);
                    (post, deltas)
                } else {
                    (pre_accounts.clone(), vec![])
                };

                Ok(SimulationResult {
                    success: value.err.is_none(),
                    error: value.err.map(|e| format!("{:?}", e)),
                    compute_units_consumed: value.units_consumed.unwrap_or(0),
                    logs: value.logs.unwrap_or_default(),
                    accounts_modified: account_deltas,
                    simulation_slot: slot,
                    timestamp: start_time,
                    pre_execution_accounts: pre_accounts,
                    post_execution_accounts: post_accounts,
                    return_data: value.return_data.and_then(|rd| {
                        rd.data.0.parse::<String>().ok().and_then(|s| {
                            base64::decode(&s).ok()
                        })
                    }),
                })
            }
            Err(e) => {
                self.simulation_errors.fetch_add(1, Ordering::Relaxed);
                Err(format!("RPC simulation error: {}", e))
            }
        }
    }

    async fn simulate_versioned_with_client(
        &self,
        client: &Arc<RpcClient>,
        transaction: &VersionedTransaction,
        slot: u64,
    ) -> Result<SimulationResult, String> {
        let start_time = Instant::now();

        let accounts = match &transaction.message {
            solana_sdk::message::VersionedMessage::Legacy(legacy) => {
                legacy.account_keys.iter().copied().collect::<Vec<_>>()
            }
            solana_sdk::message::VersionedMessage::V0(v0) => {
                v0.account_keys.iter().copied().collect::<Vec<_>>()
            }
        };

        let pre_accounts = self.fetch_account_states(client, &accounts).await?;

        let sim_result = tokio::task::spawn_blocking({
            let client = client.clone();
            let transaction = transaction.clone();
            move || {
                client.simulate_transaction_with_config(
                    &transaction.into(),
                    solana_client::rpc_config::RpcSimulateTransactionConfig {
                        sig_verify: true,
                        replace_recent_blockhash: false,
                        commitment: Some(CommitmentConfig::processed()),
                        encoding: Some(UiTransactionEncoding::Base64),
                                                accounts: Some(solana_client::rpc_config::RpcSimulateTransactionAccountsConfig {
                            encoding: Some(UiTransactionEncoding::Base64),
                            addresses: accounts.iter().map(|a| a.to_string()).collect(),
                        }),
                        min_context_slot: Some(slot),
                        inner_instructions: false,
                    },
                )
            }
        })
        .await
        .map_err(|e| format!("Simulation task failed: {}", e))?;

        match sim_result {
            Ok(response) => {
                let value = response.value;
                
                let (post_accounts, account_deltas) = if value.err.is_none() {
                    let post = self.fetch_account_states(client, &accounts).await?;
                    let deltas = self.calculate_account_deltas(&pre_accounts, &post, &accounts);
                    (post, deltas)
                } else {
                    (pre_accounts.clone(), vec![])
                };

                Ok(SimulationResult {
                    success: value.err.is_none(),
                    error: value.err.map(|e| format!("{:?}", e)),
                    compute_units_consumed: value.units_consumed.unwrap_or(0),
                    logs: value.logs.unwrap_or_default(),
                    accounts_modified: account_deltas,
                    simulation_slot: slot,
                    timestamp: start_time,
                    pre_execution_accounts: pre_accounts,
                    post_execution_accounts: post_accounts,
                    return_data: value.return_data.and_then(|rd| {
                        rd.data.0.parse::<String>().ok().and_then(|s| {
                            base64::decode(&s).ok()
                        })
                    }),
                })
            }
            Err(e) => {
                self.simulation_errors.fetch_add(1, Ordering::Relaxed);
                Err(format!("RPC simulation error: {}", e))
            }
        }
    }

    async fn fetch_account_states(
        &self,
        client: &Arc<RpcClient>,
        accounts: &[Pubkey],
    ) -> Result<Vec<(Pubkey, u64)>, String> {
        let mut states = Vec::with_capacity(accounts.len());
        
        let accounts_data = tokio::task::spawn_blocking({
            let client = client.clone();
            let accounts = accounts.to_vec();
            move || client.get_multiple_accounts(&accounts)
        })
        .await
        .map_err(|e| format!("Failed to fetch account states: {}", e))?
        .map_err(|e| format!("RPC error fetching accounts: {}", e))?;

        for (i, account_opt) in accounts_data.iter().enumerate() {
            let balance = account_opt.as_ref().map(|a| a.lamports).unwrap_or(0);
            states.push((accounts[i], balance));
        }

        Ok(states)
    }

    fn calculate_account_deltas(
        &self,
        pre_accounts: &[(Pubkey, u64)],
        post_accounts: &[(Pubkey, u64)],
        account_keys: &[Pubkey],
    ) -> Vec<AccountDelta> {
        let mut deltas = Vec::with_capacity(account_keys.len());
        
        for i in 0..account_keys.len() {
            let pubkey = account_keys[i];
            let pre_balance = pre_accounts.get(i).map(|(_, b)| *b).unwrap_or(0);
            let post_balance = post_accounts.get(i).map(|(_, b)| *b).unwrap_or(0);
            
            if pre_balance != post_balance {
                deltas.push(AccountDelta {
                    pubkey,
                    pre_balance,
                    post_balance,
                    data_modified: false,
                    owner_changed: false,
                });
            }
        }
        
        deltas
    }

    fn get_next_client(&self) -> Arc<RpcClient> {
        let index = self.current_client_index.fetch_add(1, Ordering::Relaxed) % self.rpc_clients.len();
        self.rpc_clients[index].clone()
    }

    async fn cleanup_expired_entries(&self) {
        let mut last_cleanup = self.last_cleanup.write().await;
        
        if last_cleanup.elapsed().as_millis() < CACHE_CLEANUP_INTERVAL_MS as u128 {
            return;
        }
        
        *last_cleanup = Instant::now();
        drop(last_cleanup);

        let now = Instant::now();
        let mut keys_to_remove = Vec::new();
        
        for entry in self.cache.iter() {
            if entry.value().timestamp.elapsed().as_millis() > CACHE_TTL_MS as u128 {
                keys_to_remove.push(entry.key().clone());
            }
        }
        
        for key in keys_to_remove {
            self.cache.remove(&key);
        }

        if self.cache.len() > MAX_CACHE_ENTRIES {
            let mut entries: Vec<(CacheKey, Instant)> = self.cache
                .iter()
                .map(|entry| (entry.key().clone(), entry.value().timestamp))
                .collect();
            
            entries.sort_by_key(|(_, timestamp)| *timestamp);
            
            let excess = self.cache.len().saturating_sub(MAX_CACHE_ENTRIES * 9 / 10);
            for (key, _) in entries.into_iter().take(excess) {
                self.cache.remove(&key);
            }
        }
    }

    pub async fn invalidate_transaction(&self, transaction: &Transaction, slot: u64) {
        let cache_key = self.create_cache_key(transaction, slot);
        self.cache.remove(&cache_key);
    }

    pub async fn invalidate_versioned_transaction(&self, transaction: &VersionedTransaction, slot: u64) {
        let cache_key = self.create_versioned_cache_key(transaction, slot);
        self.cache.remove(&cache_key);
    }

    pub async fn invalidate_by_slot(&self, slot: u64) {
        let keys_to_remove: Vec<CacheKey> = self.cache
            .iter()
            .filter(|entry| entry.key().slot <= slot)
            .map(|entry| entry.key().clone())
            .collect();
        
        for key in keys_to_remove {
            self.cache.remove(&key);
        }
    }

    pub async fn get_metrics(&self) -> CacheMetrics {
        CacheMetrics {
            total_simulations: self.total_simulations.load(Ordering::Relaxed),
            cache_hits: self.cache_hits.load(Ordering::Relaxed),
            cache_misses: self.cache_misses.load(Ordering::Relaxed),
            simulation_errors: self.simulation_errors.load(Ordering::Relaxed),
            cache_size: self.cache.len(),
            hit_rate: self.calculate_hit_rate(),
        }
    }

    fn calculate_hit_rate(&self) -> f64 {
        let hits = self.cache_hits.load(Ordering::Relaxed) as f64;
        let misses = self.cache_misses.load(Ordering::Relaxed) as f64;
        let total = hits + misses;
        
        if total > 0.0 {
            hits / total
        } else {
            0.0
        }
    }

    pub async fn clear_cache(&self) {
        self.cache.clear();
    }

    pub async fn prefetch_simulation(
        &self,
        transaction: Transaction,
        slot: u64,
    ) -> tokio::task::JoinHandle<Result<Arc<SimulationResult>, String>> {
        let cache = self.clone();
        tokio::spawn(async move {
            cache.simulate_transaction(&transaction, slot).await
        })
    }

    pub async fn batch_simulate_transactions(
        &self,
        transactions: Vec<Transaction>,
        slot: u64,
    ) -> Vec<Result<Arc<SimulationResult>, String>> {
        let mut handles = Vec::with_capacity(transactions.len());
        
        for transaction in transactions {
            let cache = self.clone();
            let handle = tokio::spawn(async move {
                cache.simulate_transaction(&transaction, slot).await
            });
            handles.push(handle);
        }
        
        let mut results = Vec::with_capacity(handles.len());
        for handle in handles {
            match handle.await {
                Ok(result) => results.push(result),
                Err(e) => results.push(Err(format!("Task join error: {}", e))),
            }
        }
        
        results
    }

    pub fn estimate_profit_impact(&self, result: &SimulationResult) -> i64 {
        let mut profit = 0i64;
        
        for delta in &result.accounts_modified {
            let balance_change = delta.post_balance as i64 - delta.pre_balance as i64;
            profit += balance_change;
        }
        
        profit
    }

    pub fn is_simulation_fresh(&self, result: &SimulationResult, max_age_ms: u64) -> bool {
        result.timestamp.elapsed().as_millis() <= max_age_ms as u128
    }

    pub async fn simulate_with_modified_accounts(
        &self,
        transaction: &Transaction,
        account_overrides: Vec<(Pubkey, u64)>,
        slot: u64,
    ) -> Result<Arc<SimulationResult>, String> {
        let mut modified_transaction = transaction.clone();
        
        for (pubkey, new_balance) in account_overrides {
            for (i, key) in modified_transaction.message.account_keys.iter().enumerate() {
                if key == &pubkey {
                    break;
                }
            }
        }
        
        self.simulate_transaction(&modified_transaction, slot).await
    }
}

#[derive(Debug, Clone)]
pub struct CacheMetrics {
    pub total_simulations: u64,
    pub cache_hits: u64,
    pub cache_misses: u64,
    pub simulation_errors: u64,
    pub cache_size: usize,
    pub hit_rate: f64,
}

impl Clone for TransactionSimulationCache {
    fn clone(&self) -> Self {
        Self {
            cache: self.cache.clone(),
            rpc_clients: self.rpc_clients.clone(),
            current_client_index: AtomicUsize::new(self.current_client_index.load(Ordering::Relaxed)),
            simulation_semaphore: self.simulation_semaphore.clone(),
            total_simulations: AtomicU64::new(self.total_simulations.load(Ordering::Relaxed)),
            cache_hits: AtomicU64::new(self.cache_hits.load(Ordering::Relaxed)),
            cache_misses: AtomicU64::new(self.cache_misses.load(Ordering::Relaxed)),
            simulation_errors: AtomicU64::new(self.simulation_errors.load(Ordering::Relaxed)),
            last_cleanup: self.last_cleanup.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use solana_sdk::{
        instruction::Instruction,
        message::Message,
        pubkey::Pubkey,
        signature::{Keypair, Signer},
        system_instruction,
        transaction::Transaction,
    };

    #[tokio::test]
    async fn test_cache_operations() {
        let cache = TransactionSimulationCache::new(vec!["https://api.mainnet-beta.solana.com".to_string()]);
        
        let from = Keypair::new();
        let to = Pubkey::new_unique();
        let instruction = system_instruction::transfer(&from.pubkey(), &to, 1000);
        let message = Message::new(&[instruction], Some(&from.pubkey()));
        let transaction = Transaction::new(&[&from], message, solana_sdk::hash::Hash::default());
        
        let slot = 150_000_000;
        let key = cache.create_cache_key(&transaction, slot);
        
        assert!(cache.get_cached_result(&key).await.is_none());
        
        let metrics = cache.get_metrics().await;
        assert_eq!(metrics.cache_size, 0);
    }
}

