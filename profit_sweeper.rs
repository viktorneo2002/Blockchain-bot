use anchor_client::{
    solana_sdk::{
        commitment_config::CommitmentConfig,
        compute_budget::ComputeBudgetInstruction,
        instruction::Instruction,
        native_token::LAMPORTS_PER_SOL,
        pubkey::Pubkey,
        signature::{Keypair, Signature},
        signer::Signer,
        system_instruction,
        transaction::Transaction,
    },
    Client, Program,
};
use anchor_lang::prelude::*;
use anyhow::{bail, Result};
use dashmap::DashMap;
use rayon::prelude::*;
use serde::{Deserialize, Serialize};
use solana_client::{
    nonblocking::rpc_client::RpcClient,
    rpc_config::{RpcSendTransactionConfig, RpcSimulateTransactionConfig},
    rpc_response::RpcSimulateTransactionResult,
};
use solana_program::{
    program_pack::Pack,
    system_program,
};
use solana_sdk::{
    commitment_config::CommitmentLevel,
    hash::Hash,
    transaction::VersionedTransaction,
};
use spl_associated_token_account::{
    get_associated_token_address,
    instruction::create_associated_token_account_idempotent,
};
use spl_token::{
    instruction::{close_account, transfer_checked},
    state::Account as TokenAccount,
};
use std::{
    collections::{HashMap, HashSet},
    str::FromStr,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc,
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::{
    sync::{Mutex, RwLock, Semaphore},
    time::{interval, sleep},
};

const PROFIT_DESTINATION: &str = "2uUU5vmZyEV3HrzdrAZZ2V9y3R1kP8QFpd5n2pwGvHnK";
const MIN_SWEEP_AMOUNT_SOL: u64 = 100_000_000; // 0.1 SOL
const MIN_SWEEP_AMOUNT_TOKEN: u64 = 1_000_000; // 1 USDC/USDT (6 decimals)
const MAX_RETRIES: u8 = 3;
const RETRY_DELAY_MS: u64 = 250;
const PRIORITY_FEE_PERCENTILE: u8 = 90;
const MAX_COMPUTE_UNITS: u32 = 200_000;
const SWEEP_INTERVAL_SECS: u64 = 30;
const BATCH_SIZE: usize = 10;
const MAX_CONCURRENT_SWEEPS: usize = 5;
const RENT_EXEMPT_RESERVE: u64 = 2_039_280; // ~0.00203928 SOL
const GAS_BUFFER_LAMPORTS: u64 = 10_000_000; // 0.01 SOL buffer
const SIMULATION_COMMITMENT: CommitmentLevel = CommitmentLevel::Processed;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SweepConfig {
    pub enabled: bool,
    pub interval_secs: u64,
    pub min_sol_amount: u64,
    pub min_token_amount: u64,
    pub priority_fee_cap: u64,
    pub batch_size: usize,
}

#[derive(Debug, Clone)]
pub struct SweepableAccount {
    pub pubkey: Pubkey,
    pub is_token_account: bool,
    pub mint: Option<Pubkey>,
    pub balance: u64,
    pub last_swept: u64,
}

#[derive(Debug)]
pub struct SweepResult {
    pub account: Pubkey,
    pub amount: u64,
    pub signature: Option<Signature>,
    pub success: bool,
    pub error: Option<String>,
}

pub struct ProfitSweeper {
    rpc_client: Arc<RpcClient>,
    payer: Arc<Keypair>,
    destination: Pubkey,
    accounts: Arc<RwLock<HashMap<Pubkey, SweepableAccount>>>,
    token_decimals: Arc<DashMap<Pubkey, u8>>,
    config: Arc<RwLock<SweepConfig>>,
    is_running: Arc<AtomicBool>,
    sweep_semaphore: Arc<Semaphore>,
    last_priority_fee: Arc<AtomicU64>,
    sweep_metrics: Arc<Mutex<SweepMetrics>>,
}

#[derive(Debug, Default)]
struct SweepMetrics {
    total_swept_sol: u64,
    total_swept_tokens: HashMap<Pubkey, u64>,
    successful_sweeps: u64,
    failed_sweeps: u64,
    last_sweep_timestamp: u64,
}

impl ProfitSweeper {
    pub async fn new(
        rpc_url: &str,
        payer: Keypair,
        config: SweepConfig,
    ) -> Result<Self> {
        let rpc_client = Arc::new(RpcClient::new_with_commitment(
            rpc_url.to_string(),
            CommitmentConfig::confirmed(),
        ));

        let destination = Pubkey::from_str(PROFIT_DESTINATION)?;

        Ok(Self {
            rpc_client,
            payer: Arc::new(payer),
            destination,
            accounts: Arc::new(RwLock::new(HashMap::new())),
            token_decimals: Arc::new(DashMap::new()),
            config: Arc::new(RwLock::new(config)),
            is_running: Arc::new(AtomicBool::new(false)),
            sweep_semaphore: Arc::new(Semaphore::new(MAX_CONCURRENT_SWEEPS)),
            last_priority_fee: Arc::new(AtomicU64::new(1_000)),
            sweep_metrics: Arc::new(Mutex::new(SweepMetrics::default())),
        })
    }

    pub async fn add_account(&self, account: SweepableAccount) {
        let mut accounts = self.accounts.write().await;
        accounts.insert(account.pubkey, account);
    }

    pub async fn remove_account(&self, pubkey: &Pubkey) {
        let mut accounts = self.accounts.write().await;
        accounts.remove(pubkey);
    }

    pub async fn start(&self) -> Result<()> {
        if self.is_running.load(Ordering::Relaxed) {
            bail!("Sweeper already running");
        }

        self.is_running.store(true, Ordering::Relaxed);
        let self_clone = Arc::new(self.clone());

        tokio::spawn(async move {
            let mut interval = interval(Duration::from_secs(SWEEP_INTERVAL_SECS));
            
            while self_clone.is_running.load(Ordering::Relaxed) {
                interval.tick().await;
                
                if let Err(e) = self_clone.sweep_all_accounts().await {
                    tracing::error!("Sweep cycle error: {:?}", e);
                }
            }
        });

        Ok(())
    }

    pub fn stop(&self) {
        self.is_running.store(false, Ordering::Relaxed);
    }

    async fn sweep_all_accounts(&self) -> Result<()> {
        let config = self.config.read().await;
        if !config.enabled {
            return Ok(());
        }

        let accounts = self.accounts.read().await.clone();
        let sweepable_accounts: Vec<_> = accounts
            .values()
            .filter(|acc| self.is_sweepable(acc, &config))
            .cloned()
            .collect();

        if sweepable_accounts.is_empty() {
            return Ok(());
        }

        let priority_fee = self.calculate_priority_fee().await?;
        self.last_priority_fee.store(priority_fee, Ordering::Relaxed);

        let chunks: Vec<_> = sweepable_accounts
            .chunks(config.batch_size)
            .map(|chunk| chunk.to_vec())
            .collect();

        let results = futures::future::join_all(
            chunks.into_iter().map(|chunk| {
                let self_clone = self.clone();
                async move { self_clone.sweep_batch(chunk, priority_fee).await }
            })
        ).await;

        self.update_metrics(&results).await;

        Ok(())
    }

    fn is_sweepable(&self, account: &SweepableAccount, config: &SweepConfig) -> bool {
        let current_time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        if current_time - account.last_swept < config.interval_secs {
            return false;
        }

        if account.is_token_account {
            account.balance >= config.min_token_amount
        } else {
            account.balance > config.min_sol_amount + RENT_EXEMPT_RESERVE + GAS_BUFFER_LAMPORTS
        }
    }

    async fn sweep_batch(
        &self,
        accounts: Vec<SweepableAccount>,
        priority_fee: u64,
    ) -> Vec<SweepResult> {
        let permits = Arc::new(self.sweep_semaphore.acquire_many(accounts.len() as u32).await.unwrap());
        
        let results = futures::future::join_all(
            accounts.into_iter().map(|account| {
                let self_clone = self.clone();
                let priority_fee = priority_fee;
                async move {
                    self_clone.sweep_single_account(account, priority_fee).await
                }
            })
        ).await;

        drop(permits);
        results
    }

    async fn sweep_single_account(
        &self,
        account: SweepableAccount,
        priority_fee: u64,
    ) -> SweepResult {
        let mut attempts = 0;
        let mut last_error = None;

        while attempts < MAX_RETRIES {
            match self.execute_sweep(&account, priority_fee).await {
                Ok(signature) => {
                    self.update_last_swept(&account.pubkey).await;
                    return SweepResult {
                        account: account.pubkey,
                        amount: account.balance,
                        signature: Some(signature),
                        success: true,
                        error: None,
                    };
                }
                Err(e) => {
                    last_error = Some(e.to_string());
                    attempts += 1;
                    
                    if attempts < MAX_RETRIES {
                        sleep(Duration::from_millis(RETRY_DELAY_MS * attempts as u64)).await;
                    }
                }
            }
        }

        SweepResult {
            account: account.pubkey,
            amount: account.balance,
            signature: None,
            success: false,
            error: last_error,
        }
    }

    async fn execute_sweep(
        &self,
        account: &SweepableAccount,
        priority_fee: u64,
    ) -> Result<Signature> {
        let balance = self.get_current_balance(account).await?;
        
        if account.is_token_account {
            self.sweep_token_account(account, balance, priority_fee).await
        } else {
            self.sweep_sol_account(account, balance, priority_fee).await
        }
    }

    async fn sweep_sol_account(
        &self,
        account: &SweepableAccount,
        balance: u64,
        priority_fee: u64,
    ) -> Result<Signature> {
        let config = self.config.read().await;
        
        let sweep_amount = balance.saturating_sub(RENT_EXEMPT_RESERVE + GAS_BUFFER_LAMPORTS);
        if sweep_amount < config.min_sol_amount {
            bail!("Balance too low to sweep");
        }

        let recent_blockhash = self.rpc_client.get_latest_blockhash().await?;
        
        let mut instructions = vec![
            ComputeBudgetInstruction::set_compute_unit_limit(MAX_COMPUTE_UNITS),
            ComputeBudgetInstruction::set_compute_unit_price(priority_fee),
            system_instruction::transfer(
                &account.pubkey,
                &self.destination,
                sweep_amount,
            ),
        ];

        let transaction = Transaction::new_signed_with_payer(
            &instructions,
            Some(&self.payer.pubkey()),
            &[&*self.payer],
            recent_blockhash,
        );

        self.simulate_and_send_transaction(transaction).await
    }

    async fn sweep_token_account(
        &self,
        account: &SweepableAccount,
        balance: u64,
        priority_fee: u64,
    ) -> Result<Signature> {
        let mint = account.mint.ok_or_else(|| anyhow::anyhow!("No mint for token account"))?;
        let decimals = self.get_token_decimals(&mint).await?;
        
        let dest_ata = get_associated_token_address(&self.destination, &mint);
        let recent_blockhash = self.rpc_client.get_latest_blockhash().await?;

        let mut instructions = vec![
            ComputeBudgetInstruction::set_compute_unit_limit(MAX_COMPUTE_UNITS),
            ComputeBudgetInstruction::set_compute_unit_price(priority_fee),
            create_associated_token_account_idempotent(
                &self.payer.pubkey(),
                &self.destination,
                &mint,
                &spl_token::id(),
            ),
            transfer_checked(
                &spl_token::id(),
                &account.pubkey,
                &mint,
                &dest_ata,
                &self.payer.pubkey(),
                &[],
                balance,
                decimals,
            )?,
        ];

        let close_instruction = close_account(
            &spl_token::id(),
            &account.pubkey,
            &self.payer.pubkey(),
                        &self.payer.pubkey(),
            &[],
        )?;
        instructions.push(close_instruction);

        let transaction = Transaction::new_signed_with_payer(
            &instructions,
            Some(&self.payer.pubkey()),
            &[&*self.payer],
            recent_blockhash,
        );

        self.simulate_and_send_transaction(transaction).await
    }

    async fn simulate_and_send_transaction(&self, transaction: Transaction) -> Result<Signature> {
        let versioned_tx = VersionedTransaction::from(transaction);
        
        let sim_config = RpcSimulateTransactionConfig {
            sig_verify: false,
            replace_recent_blockhash: false,
            commitment: Some(SIMULATION_COMMITMENT),
            accounts: None,
            min_context_slot: None,
            inner_instructions: false,
        };

        let sim_result = self.rpc_client
            .simulate_transaction_with_config(&versioned_tx, sim_config)
            .await?;

        if let Some(err) = sim_result.value.err {
            bail!("Simulation failed: {:?}", err);
        }

        let send_config = RpcSendTransactionConfig {
            skip_preflight: true,
            preflight_commitment: Some(CommitmentLevel::Processed),
            encoding: None,
            max_retries: Some(0),
            min_context_slot: None,
        };

        let signature = self.rpc_client
            .send_transaction_with_config(&versioned_tx.into(), send_config)
            .await?;

        Ok(signature)
    }

    async fn get_current_balance(&self, account: &SweepableAccount) -> Result<u64> {
        if account.is_token_account {
            let account_data = self.rpc_client.get_account(&account.pubkey).await?;
            let token_account = TokenAccount::unpack(&account_data.data)?;
            Ok(token_account.amount)
        } else {
            Ok(self.rpc_client.get_balance(&account.pubkey).await?)
        }
    }

    async fn get_token_decimals(&self, mint: &Pubkey) -> Result<u8> {
        if let Some(decimals) = self.token_decimals.get(mint) {
            return Ok(*decimals);
        }

        let mint_account = self.rpc_client.get_account(mint).await?;
        let mint_data = spl_token::state::Mint::unpack(&mint_account.data)?;
        
        self.token_decimals.insert(*mint, mint_data.decimals);
        Ok(mint_data.decimals)
    }

    async fn calculate_priority_fee(&self) -> Result<u64> {
        let recent_fees = self.rpc_client
            .get_recent_prioritization_fees(&[])
            .await?;

        if recent_fees.is_empty() {
            return Ok(1_000);
        }

        let mut fees: Vec<u64> = recent_fees
            .iter()
            .map(|f| f.prioritization_fee)
            .filter(|&f| f > 0)
            .collect();

        if fees.is_empty() {
            return Ok(1_000);
        }

        fees.sort_unstable();
        let percentile_index = (fees.len() * PRIORITY_FEE_PERCENTILE as usize) / 100;
        let fee = fees.get(percentile_index).copied().unwrap_or(1_000);
        
        let config = self.config.read().await;
        Ok(fee.min(config.priority_fee_cap))
    }

    async fn update_last_swept(&self, pubkey: &Pubkey) {
        let mut accounts = self.accounts.write().await;
        if let Some(account) = accounts.get_mut(pubkey) {
            account.last_swept = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs();
        }
    }

    async fn update_metrics(&self, results: &[Vec<SweepResult>]) {
        let mut metrics = self.sweep_metrics.lock().await;
        
        for batch in results {
            for result in batch {
                if result.success {
                    metrics.successful_sweeps += 1;
                    
                    if let Ok(accounts) = self.accounts.read().await.get(&result.account) {
                        if accounts.is_token_account {
                            if let Some(mint) = accounts.mint {
                                *metrics.total_swept_tokens.entry(mint).or_insert(0) += result.amount;
                            }
                        } else {
                            metrics.total_swept_sol += result.amount;
                        }
                    }
                } else {
                    metrics.failed_sweeps += 1;
                }
            }
        }
        
        metrics.last_sweep_timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
    }

    pub async fn get_metrics(&self) -> SweepMetrics {
        self.sweep_metrics.lock().await.clone()
    }

    pub async fn force_sweep(&self, pubkey: &Pubkey) -> Result<SweepResult> {
        let accounts = self.accounts.read().await;
        let account = accounts.get(pubkey)
            .ok_or_else(|| anyhow::anyhow!("Account not found"))?
            .clone();

        let priority_fee = self.calculate_priority_fee().await?;
        Ok(self.sweep_single_account(account, priority_fee).await)
    }

    pub async fn update_config(&self, new_config: SweepConfig) {
        let mut config = self.config.write().await;
        *config = new_config;
    }

    pub async fn add_token_accounts(&self, owner: &Pubkey, mints: &[Pubkey]) -> Result<()> {
        for mint in mints {
            let ata = get_associated_token_address(owner, mint);
            
            if let Ok(balance) = self.get_token_balance(&ata).await {
                if balance > 0 {
                    self.add_account(SweepableAccount {
                        pubkey: ata,
                        is_token_account: true,
                        mint: Some(*mint),
                        balance,
                        last_swept: 0,
                    }).await;
                }
            }
        }
        Ok(())
    }

    async fn get_token_balance(&self, ata: &Pubkey) -> Result<u64> {
        match self.rpc_client.get_account(ata).await {
            Ok(account) => {
                let token_account = TokenAccount::unpack(&account.data)?;
                Ok(token_account.amount)
            }
            Err(_) => Ok(0),
        }
    }

    pub async fn emergency_sweep_all(&self) -> Result<Vec<SweepResult>> {
        tracing::warn!("Executing emergency sweep of all accounts");
        
        let accounts = self.accounts.read().await;
        let all_accounts: Vec<_> = accounts.values().cloned().collect();
        
        let priority_fee = self.calculate_priority_fee().await?
            .saturating_mul(2)
            .min(100_000);
        
        let results = futures::future::join_all(
            all_accounts.into_iter().map(|account| {
                let self_clone = self.clone();
                async move {
                    self_clone.sweep_single_account(account, priority_fee).await
                }
            })
        ).await;
        
        self.update_metrics(&[results.clone()]).await;
        Ok(results)
    }

    pub fn is_running(&self) -> bool {
        self.is_running.load(Ordering::Relaxed)
    }
}

impl Clone for ProfitSweeper {
    fn clone(&self) -> Self {
        Self {
            rpc_client: Arc::clone(&self.rpc_client),
            payer: Arc::clone(&self.payer),
            destination: self.destination,
            accounts: Arc::clone(&self.accounts),
            token_decimals: Arc::clone(&self.token_decimals),
            config: Arc::clone(&self.config),
            is_running: Arc::clone(&self.is_running),
            sweep_semaphore: Arc::clone(&self.sweep_semaphore),
            last_priority_fee: Arc::clone(&self.last_priority_fee),
            sweep_metrics: Arc::clone(&self.sweep_metrics),
        }
    }
}

impl Default for SweepConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            interval_secs: SWEEP_INTERVAL_SECS,
            min_sol_amount: MIN_SWEEP_AMOUNT_SOL,
            min_token_amount: MIN_SWEEP_AMOUNT_TOKEN,
            priority_fee_cap: 50_000,
            batch_size: BATCH_SIZE,
        }
    }
}

impl Clone for SweepMetrics {
    fn clone(&self) -> Self {
        Self {
            total_swept_sol: self.total_swept_sol,
            total_swept_tokens: self.total_swept_tokens.clone(),
            successful_sweeps: self.successful_sweeps,
            failed_sweeps: self.failed_sweeps,
            last_sweep_timestamp: self.last_sweep_timestamp,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use solana_sdk::signature::Keypair;

    #[tokio::test]
    async fn test_sweeper_initialization() {
        let payer = Keypair::new();
        let config = SweepConfig::default();
        let sweeper = ProfitSweeper::new(
            "https://api.mainnet-beta.solana.com",
            payer,
            config,
        ).await.unwrap();
        
        assert!(!sweeper.is_running());
        assert_eq!(sweeper.destination, Pubkey::from_str(PROFIT_DESTINATION).unwrap());
    }

    #[tokio::test]
    async fn test_add_remove_accounts() {
        let payer = Keypair::new();
        let sweeper = ProfitSweeper::new(
            "https://api.mainnet-beta.solana.com",
            payer,
            SweepConfig::default(),
        ).await.unwrap();
        
        let test_account = SweepableAccount {
            pubkey: Pubkey::new_unique(),
            is_token_account: false,
            mint: None,
            balance: LAMPORTS_PER_SOL,
            last_swept: 0,
        };
        
        sweeper.add_account(test_account.clone()).await;
        
        {
            let accounts = sweeper.accounts.read().await;
            assert!(accounts.contains_key(&test_account.pubkey));
        }
        
        sweeper.remove_account(&test_account.pubkey).await;
        
        {
            let accounts = sweeper.accounts.read().await;
            assert!(!accounts.contains_key(&test_account.pubkey));
        }
    }

    #[tokio::test]
    async fn test_is_sweepable() {
        let payer = Keypair::new();
        let config = SweepConfig {
            enabled: true,
            interval_secs: 60,
            min_sol_amount: MIN_SWEEP_AMOUNT_SOL,
            min_token_amount: MIN_SWEEP_AMOUNT_TOKEN,
            priority_fee_cap: 50_000,
            batch_size: 10,
        };
        
        let sweeper = ProfitSweeper::new(
            "https://api.mainnet-beta.solana.com",
            payer,
            config.clone(),
        ).await.unwrap();
        
        let current_time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        
        let sweepable_sol = SweepableAccount {
            pubkey: Pubkey::new_unique(),
            is_token_account: false,
            mint: None,
            balance: MIN_SWEEP_AMOUNT_SOL + RENT_EXEMPT_RESERVE + GAS_BUFFER_LAMPORTS + 1,
            last_swept: current_time - 120,
        };
        
        assert!(sweeper.is_sweepable(&sweepable_sol, &config));
        
        let not_sweepable_recent = SweepableAccount {
            pubkey: Pubkey::new_unique(),
            is_token_account: false,
            mint: None,
            balance: LAMPORTS_PER_SOL * 10,
            last_swept: current_time - 30,
        };
        
        assert!(!sweeper.is_sweepable(&not_sweepable_recent, &config));
        
        let not_sweepable_low_balance = SweepableAccount {
            pubkey: Pubkey::new_unique(),
            is_token_account: false,
            mint: None,
            balance: MIN_SWEEP_AMOUNT_SOL / 2,
            last_swept: 0,
        };
        
        assert!(!sweeper.is_sweepable(&not_sweepable_low_balance, &config));
    }
}

