use anyhow::{anyhow, Result};
use borsh::{BorshDeserialize, BorshSerialize};
use solana_client::{
    nonblocking::rpc_client::RpcClient,
    rpc_config::RpcTransactionConfig,
};
use solana_program::{
    clock::Clock,
    pubkey::Pubkey,
    system_instruction,
};
use solana_sdk::{
    commitment_config::{CommitmentConfig, CommitmentLevel},
    compute_budget::ComputeBudgetInstruction,
    signature::{Keypair, Signature},
    signer::Signer,
    transaction::Transaction,
};
use solana_transaction_status::{
    EncodedConfirmedTransactionWithStatusMeta, UiTransactionEncoding,
};
use spl_associated_token_account::get_associated_token_address;
use spl_token::instruction as token_instruction;
use std::{
    collections::{HashMap, VecDeque},
    str::FromStr,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc,
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::sync::{Mutex, RwLock};

const COLD_VAULT_1: &str = "3MSyhQsSTvsfwM7AynfWYUXExLCq8Fpw2P5NAv7m6Kvf";
const COLD_VAULT_2: &str = "2grHtuFX1PuH7EnkRg8nkZC8yoUyGosmG8Anddicbu7f";
const COLD_VAULT_3: &str = "44Mdd71QKKKKPctzWL6BNByBmffXuLMAk7arwU59pfmP";
const COLD_VAULT_4: &str = "uBqrrJ9QFyFSXRrHe9sNtju8hzBLsurttEDD5apw1S9";

const MIN_DEPOSIT_THRESHOLD: u64 = 100_000_000; // 0.1 SOL minimum
const MAX_SINGLE_DEPOSIT: u64 = 100_000_000_000; // 100 SOL max per tx
const DEPOSIT_COOLDOWN_MS: u64 = 5000; // 5 seconds between deposits
const VAULT_ROTATION_INTERVAL: u64 = 3600; // 1 hour
const MAX_RETRIES: u32 = 5;
const COMPUTE_UNIT_LIMIT: u32 = 200_000;
const PRIORITY_FEE_MICROLAMPORTS: u64 = 1_000;
const CONFIRMATION_TIMEOUT: u64 = 30;
const RATE_LIMIT_WINDOW: u64 = 60; // 1 minute
const MAX_DEPOSITS_PER_WINDOW: u32 = 10;

#[derive(Debug, Clone, BorshSerialize, BorshDeserialize)]
pub struct VaultState {
    pub vault_pubkeys: [Pubkey; 4],
    pub current_vault_index: u8,
    pub last_rotation_slot: u64,
    pub total_deposited: u64,
    pub deposit_count: u64,
    pub last_deposit_timestamp: u64,
}

#[derive(Debug, Clone)]
pub struct DepositRecord {
    pub signature: Signature,
    pub amount: u64,
    pub vault_pubkey: Pubkey,
    pub timestamp: u64,
    pub slot: u64,
    pub token_mint: Option<Pubkey>,
    pub success: bool,
}

#[derive(Debug, Clone)]
pub struct VaultMetrics {
    pub total_balance: u64,
    pub deposits_24h: u64,
    pub deposits_1h: u64,
    pub average_deposit_size: u64,
    pub largest_deposit_24h: u64,
    pub vault_distribution: HashMap<Pubkey, u64>,
}

pub struct ColdVaultManager {
    rpc_client: Arc<RpcClient>,
    vault_state: Arc<RwLock<VaultState>>,
    deposit_history: Arc<Mutex<VecDeque<DepositRecord>>>,
    hot_wallet_keypair: Arc<Keypair>,
    metrics: Arc<RwLock<VaultMetrics>>,
    last_deposit_time: Arc<AtomicU64>,
    is_paused: Arc<AtomicBool>,
    rate_limiter: Arc<Mutex<RateLimiter>>,
}

struct RateLimiter {
    deposits_in_window: VecDeque<u64>,
    window_start: u64,
}

impl RateLimiter {
    fn new() -> Self {
        Self {
            deposits_in_window: VecDeque::new(),
            window_start: SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs(),
        }
    }

    fn check_and_update(&mut self) -> Result<()> {
        let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();
        
        if now - self.window_start > RATE_LIMIT_WINDOW {
            self.deposits_in_window.clear();
            self.window_start = now;
        }
        
        self.deposits_in_window.retain(|&t| now - t <= RATE_LIMIT_WINDOW);
        
        if self.deposits_in_window.len() >= MAX_DEPOSITS_PER_WINDOW as usize {
            return Err(anyhow!("Rate limit exceeded: {} deposits in window", self.deposits_in_window.len()));
        }
        
        self.deposits_in_window.push_back(now);
        Ok(())
    }
}

impl ColdVaultManager {
    pub async fn new(rpc_url: &str, hot_wallet_keypair: Arc<Keypair>) -> Result<Self> {
        let rpc_client = Arc::new(RpcClient::new_with_commitment(
            rpc_url.to_string(),
            CommitmentConfig {
                commitment: CommitmentLevel::Confirmed,
            },
        ));

        let vault_pubkeys = [
            Pubkey::from_str(COLD_VAULT_1)?,
            Pubkey::from_str(COLD_VAULT_2)?,
            Pubkey::from_str(COLD_VAULT_3)?,
            Pubkey::from_str(COLD_VAULT_4)?,
        ];

        let current_slot = rpc_client.get_slot().await?;

        let vault_state = VaultState {
            vault_pubkeys,
            current_vault_index: 0,
            last_rotation_slot: current_slot,
            total_deposited: 0,
            deposit_count: 0,
            last_deposit_timestamp: 0,
        };

        let metrics = VaultMetrics {
            total_balance: 0,
            deposits_24h: 0,
            deposits_1h: 0,
            average_deposit_size: 0,
            largest_deposit_24h: 0,
            vault_distribution: HashMap::new(),
        };

        Ok(Self {
            rpc_client,
            vault_state: Arc::new(RwLock::new(vault_state)),
            deposit_history: Arc::new(Mutex::new(VecDeque::with_capacity(10000))),
            hot_wallet_keypair,
            metrics: Arc::new(RwLock::new(metrics)),
            last_deposit_time: Arc::new(AtomicU64::new(0)),
            is_paused: Arc::new(AtomicBool::new(false)),
            rate_limiter: Arc::new(Mutex::new(RateLimiter::new())),
        })
    }

    pub async fn initialize(&self) -> Result<()> {
        self.update_metrics().await?;
        self.validate_hot_wallet_balance().await?;
        log::info!("Cold vault manager initialized successfully");
        Ok(())
    }

    pub async fn deposit_sol(&self, amount: u64) -> Result<Signature> {
        if self.is_paused.load(Ordering::Acquire) {
            return Err(anyhow!("Deposits are currently paused"));
        }

        if amount < MIN_DEPOSIT_THRESHOLD {
            return Err(anyhow!(
                "Deposit amount {} below minimum threshold {}",
                amount,
                MIN_DEPOSIT_THRESHOLD
            ));
        }

        if amount > MAX_SINGLE_DEPOSIT {
            return Err(anyhow!(
                "Deposit amount {} exceeds maximum {}",
                amount,
                MAX_SINGLE_DEPOSIT
            ));
        }

        self.enforce_cooldown().await?;
        self.rate_limiter.lock().await.check_and_update()?;

        let vault_pubkey = self.select_vault().await?;
        let signature = self.execute_sol_deposit(vault_pubkey, amount).await?;

        self.record_deposit(signature, amount, vault_pubkey, None).await?;
        self.update_metrics().await?;

        Ok(signature)
    }

    pub async fn deposit_spl_token(&self, token_mint: &Pubkey, amount: u64) -> Result<Signature> {
        if self.is_paused.load(Ordering::Acquire) {
            return Err(anyhow!("Deposits are currently paused"));
        }

        self.enforce_cooldown().await?;
        self.rate_limiter.lock().await.check_and_update()?;

        let vault_pubkey = self.select_vault().await?;
        let signature = self.execute_spl_deposit(vault_pubkey, token_mint, amount).await?;

        self.record_deposit(signature, amount, vault_pubkey, Some(*token_mint)).await?;

        Ok(signature)
    }

    async fn execute_sol_deposit(&self, vault_pubkey: Pubkey, amount: u64) -> Result<Signature> {
        let hot_wallet_balance = self.rpc_client
            .get_balance(&self.hot_wallet_keypair.pubkey())
            .await?;

        if hot_wallet_balance < amount + 10_000_000 {
            return Err(anyhow!(
                "Insufficient balance: have {}, need {}",
                hot_wallet_balance,
                amount + 10_000_000
            ));
        }

        let mut retries = 0;
        loop {
            match self.try_send_sol_deposit(&vault_pubkey, amount).await {
                Ok(signature) => {
                    let confirmed = self.await_confirmation(&signature).await?;
                    if confirmed {
                        return Ok(signature);
                    }
                }
                Err(e) => {
                    retries += 1;
                    if retries > MAX_RETRIES {
                        return Err(anyhow!("Max retries exceeded: {}", e));
                    }
                    log::warn!("Deposit attempt {} failed: {}", retries, e);
                    tokio::time::sleep(Duration::from_millis(1000 * retries as u64)).await;
                }
            }
        }
    }

    async fn try_send_sol_deposit(&self, vault_pubkey: &Pubkey, amount: u64) -> Result<Signature> {
        let recent_blockhash = self.rpc_client.get_latest_blockhash().await?;

        let compute_budget_ix = ComputeBudgetInstruction::set_compute_unit_limit(COMPUTE_UNIT_LIMIT);
        let priority_fee_ix = ComputeBudgetInstruction::set_compute_unit_price(PRIORITY_FEE_MICROLAMPORTS);
        let transfer_ix = system_instruction::transfer(
            &self.hot_wallet_keypair.pubkey(),
            vault_pubkey,
            amount,
        );

        let mut transaction = Transaction::new_with_payer(
            &[compute_budget_ix, priority_fee_ix, transfer_ix],
            Some(&self.hot_wallet_keypair.pubkey()),
        );

        transaction.sign(&[&*self.hot_wallet_keypair], recent_blockhash);

        let signature = self.rpc_client
            .send_transaction(&transaction)
            .await?;

        Ok(signature)
    }

    async fn execute_spl_deposit(
        &self,
        vault_pubkey: Pubkey,
        token_mint: &Pubkey,
        amount: u64,
    ) -> Result<Signature> {
        let source_ata = get_associated_token_address(
            &self.hot_wallet_keypair.pubkey(),
            token_mint,
        );
        let dest_ata = get_associated_token_address(&vault_pubkey, token_mint);

        let mut instructions = vec![
            ComputeBudgetInstruction::set_compute_unit_limit(COMPUTE_UNIT_LIMIT),
            ComputeBudgetInstruction::set_compute_unit_price(PRIORITY_FEE_MICROLAMPORTS),
        ];

        let dest_account = self.rpc_client.get_account(&dest_ata).await;
        if dest_account.is_err() {
            instructions.push(
                spl_associated_token_account::instruction::create_associated_token_account(
                    &self.hot_wallet_keypair.pubkey(),
                    &vault_pubkey,
                    token_mint,
                    &spl_token::id(),
                ),
            );
        }

        let decimals = self.get_token_decimals(token_mint).await?;
        instructions.push(token_instruction::transfer_checked(
            &spl_token::id(),
            &source_ata,
            token_mint,
            &dest_ata,
            &self.hot_wallet_keypair.pubkey(),
            &[],
            amount,
            decimals,
        )?);

        let recent_blockhash = self.rpc_client.get_latest_blockhash().await?;
        let mut transaction = Transaction::new_with_payer(
            &instructions,
            Some(&self.hot_wallet_keypair.pubkey()),
        );

        transaction.sign(&[&*self.hot_wallet_keypair], recent_blockhash);

        let signature = self.rpc_client.send_transaction(&transaction).await?;
        self.await_confirmation(&signature).await?;

        Ok(signature)
    }

    async fn get_token_decimals(&self, mint: &Pubkey) -> Result<u8> {
        let account = self.rpc_client.get_account(mint).await?;
        let mint_data = spl_token::state::Mint::unpack(&account.data)?;
                Ok(mint_data.decimals)
    }

    async fn await_confirmation(&self, signature: &Signature) -> Result<bool> {
        let start = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();
        
        loop {
            match self.rpc_client.get_signature_statuses(&[*signature]).await? {
                value if !value.is_empty() => {
                    if let Some(Some(status)) = value.get(0) {
                        if status.confirmations.is_some() || status.confirmation_status.is_some() {
                            return Ok(status.err.is_none());
                        }
                    }
                }
                _ => {}
            }

            let elapsed = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs() - start;
            if elapsed > CONFIRMATION_TIMEOUT {
                return Err(anyhow!("Transaction confirmation timeout"));
            }

            tokio::time::sleep(Duration::from_millis(500)).await;
        }
    }

    async fn select_vault(&self) -> Result<Pubkey> {
        let mut state = self.vault_state.write().await;
        let current_slot = self.rpc_client.get_slot().await?;

        if current_slot - state.last_rotation_slot > VAULT_ROTATION_INTERVAL * 2 {
            state.current_vault_index = ((state.current_vault_index as usize + 1) % 4) as u8;
            state.last_rotation_slot = current_slot;
            log::info!("Rotated to vault index {}", state.current_vault_index);
        }

        let vault_pubkey = state.vault_pubkeys[state.current_vault_index as usize];
        
        // Verify vault is accessible
        match self.rpc_client.get_account(&vault_pubkey).await {
            Ok(_) => Ok(vault_pubkey),
            Err(_) => {
                // Failover to next vault
                state.current_vault_index = ((state.current_vault_index as usize + 1) % 4) as u8;
                let fallback_vault = state.vault_pubkeys[state.current_vault_index as usize];
                log::warn!("Primary vault unavailable, using fallback vault {}", fallback_vault);
                Ok(fallback_vault)
            }
        }
    }

    async fn enforce_cooldown(&self) -> Result<()> {
        let last_deposit = self.last_deposit_time.load(Ordering::Acquire);
        let now = SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis() as u64;
        
        if last_deposit > 0 && now - last_deposit < DEPOSIT_COOLDOWN_MS {
            let wait_time = DEPOSIT_COOLDOWN_MS - (now - last_deposit);
            tokio::time::sleep(Duration::from_millis(wait_time)).await;
        }
        
        self.last_deposit_time.store(now, Ordering::Release);
        Ok(())
    }

    async fn record_deposit(
        &self,
        signature: Signature,
        amount: u64,
        vault_pubkey: Pubkey,
        token_mint: Option<Pubkey>,
    ) -> Result<()> {
        let timestamp = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();
        let slot = self.rpc_client.get_slot().await?;

        let record = DepositRecord {
            signature,
            amount,
            vault_pubkey,
            timestamp,
            slot,
            token_mint,
            success: true,
        };

        let mut history = self.deposit_history.lock().await;
        history.push_back(record.clone());
        if history.len() > 10000 {
            history.pop_front();
        }
        drop(history);

        let mut state = self.vault_state.write().await;
        state.total_deposited = state.total_deposited.saturating_add(amount);
        state.deposit_count += 1;
        state.last_deposit_timestamp = timestamp;

        log::info!(
            "Deposit recorded: {} lamports to {} (sig: {})",
            amount,
            vault_pubkey,
            signature
        );

        Ok(())
    }

    async fn update_metrics(&self) -> Result<()> {
        let mut metrics = self.metrics.write().await;
        let state = self.vault_state.read().await;
        let history = self.deposit_history.lock().await;
        
        let now = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();
        let cutoff_24h = now - 86400;
        let cutoff_1h = now - 3600;

        metrics.total_balance = 0;
        metrics.vault_distribution.clear();

        for vault in &state.vault_pubkeys {
            match self.rpc_client.get_balance(vault).await {
                Ok(balance) => {
                    metrics.total_balance += balance;
                    metrics.vault_distribution.insert(*vault, balance);
                }
                Err(e) => log::error!("Failed to get balance for {}: {}", vault, e),
            }
        }

        let recent_deposits: Vec<&DepositRecord> = history
            .iter()
            .filter(|d| d.timestamp > cutoff_24h && d.success)
            .collect();

        metrics.deposits_24h = recent_deposits.iter().map(|d| d.amount).sum();
        metrics.deposits_1h = recent_deposits
            .iter()
            .filter(|d| d.timestamp > cutoff_1h)
            .map(|d| d.amount)
            .sum();

        if !recent_deposits.is_empty() {
            metrics.average_deposit_size = metrics.deposits_24h / recent_deposits.len() as u64;
            metrics.largest_deposit_24h = recent_deposits
                .iter()
                .map(|d| d.amount)
                .max()
                .unwrap_or(0);
        }

        Ok(())
    }

    async fn validate_hot_wallet_balance(&self) -> Result<()> {
        let balance = self.rpc_client
            .get_balance(&self.hot_wallet_keypair.pubkey())
            .await?;

        if balance < MIN_DEPOSIT_THRESHOLD {
            log::warn!(
                "Hot wallet balance {} below minimum deposit threshold",
                balance
            );
        }

        Ok(())
    }

    pub async fn get_metrics(&self) -> VaultMetrics {
        self.metrics.read().await.clone()
    }

    pub async fn get_vault_state(&self) -> VaultState {
        self.vault_state.read().await.clone()
    }

    pub async fn get_deposit_history(&self, limit: usize) -> Vec<DepositRecord> {
        let history = self.deposit_history.lock().await;
        history.iter().rev().take(limit).cloned().collect()
    }

    pub async fn pause_deposits(&self) {
        self.is_paused.store(true, Ordering::Release);
        log::warn!("Deposits paused");
    }

    pub async fn resume_deposits(&self) {
        self.is_paused.store(false, Ordering::Release);
        log::info!("Deposits resumed");
    }

    pub async fn is_paused(&self) -> bool {
        self.is_paused.load(Ordering::Acquire)
    }

    pub async fn health_check(&self) -> Result<HealthStatus> {
        let mut issues = Vec::new();
        
        // Check hot wallet balance
        let hot_wallet_balance = self.rpc_client
            .get_balance(&self.hot_wallet_keypair.pubkey())
            .await?;
        
        if hot_wallet_balance < MIN_DEPOSIT_THRESHOLD * 10 {
            issues.push(format!(
                "Hot wallet balance low: {} SOL",
                hot_wallet_balance as f64 / 1e9
            ));
        }

        // Check vault accessibility
        let state = self.vault_state.read().await;
        for (idx, vault) in state.vault_pubkeys.iter().enumerate() {
            if self.rpc_client.get_account(vault).await.is_err() {
                issues.push(format!("Vault {} at index {} is inaccessible", vault, idx));
            }
        }

        // Check metrics freshness
        let metrics = self.metrics.read().await;
        if metrics.total_balance == 0 {
            issues.push("Metrics not updated or all vaults empty".to_string());
        }

        // Check rate limiting
        let rate_limiter = self.rate_limiter.lock().await;
        if rate_limiter.deposits_in_window.len() >= (MAX_DEPOSITS_PER_WINDOW - 2) as usize {
            issues.push("Approaching rate limit".to_string());
        }

        Ok(HealthStatus {
            is_healthy: issues.is_empty(),
            issues,
            hot_wallet_balance,
            total_vault_balance: metrics.total_balance,
            last_deposit: state.last_deposit_timestamp,
        })
    }

    pub async fn get_vault_balances(&self) -> Result<HashMap<Pubkey, u64>> {
        let mut balances = HashMap::new();
        let state = self.vault_state.read().await;
        
        for vault in &state.vault_pubkeys {
            match self.rpc_client.get_balance(vault).await {
                Ok(balance) => {
                    balances.insert(*vault, balance);
                }
                Err(e) => {
                    log::error!("Failed to get balance for {}: {}", vault, e);
                    balances.insert(*vault, 0);
                }
            }
        }
        
        Ok(balances)
    }

    pub async fn get_deposit_stats(&self, hours: u64) -> DepositStats {
        let history = self.deposit_history.lock().await;
        let cutoff = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs() - (hours * 3600);
        
        let recent: Vec<&DepositRecord> = history
            .iter()
            .filter(|d| d.timestamp > cutoff && d.success)
            .collect();

        let total_amount: u64 = recent.iter().map(|d| d.amount).sum();
        let count = recent.len() as u64;
        let average = if count > 0 { total_amount / count } else { 0 };
        
        let sol_deposits = recent.iter().filter(|d| d.token_mint.is_none()).count() as u64;
        let token_deposits = recent.iter().filter(|d| d.token_mint.is_some()).count() as u64;

        DepositStats {
            total_amount,
            count,
            average_amount: average,
            sol_deposits,
            token_deposits,
            time_window_hours: hours,
        }
    }

    pub async fn estimate_next_deposit_time(&self) -> u64 {
        let last = self.last_deposit_time.load(Ordering::Acquire);
        if last == 0 {
            return 0;
        }
        
        let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64;
        if now > last + DEPOSIT_COOLDOWN_MS {
            return 0;
        }
        
        (last + DEPOSIT_COOLDOWN_MS - now) / 1000
    }

    pub async fn can_deposit(&self, amount: u64) -> Result<bool> {
        if self.is_paused.load(Ordering::Acquire) {
            return Ok(false);
        }

        if amount < MIN_DEPOSIT_THRESHOLD || amount > MAX_SINGLE_DEPOSIT {
            return Ok(false);
        }

        let rate_limiter = self.rate_limiter.lock().await;
        if rate_limiter.deposits_in_window.len() >= MAX_DEPOSITS_PER_WINDOW as usize {
            return Ok(false);
        }

        let hot_wallet_balance = self.rpc_client
            .get_balance(&self.hot_wallet_keypair.pubkey())
            .await?;

        Ok(hot_wallet_balance >= amount + 10_000_000)
    }
}

#[derive(Debug, Clone)]
pub struct HealthStatus {
    pub is_healthy: bool,
    pub issues: Vec<String>,
    pub hot_wallet_balance: u64,
    pub total_vault_balance: u64,
    pub last_deposit: u64,
}

#[derive(Debug, Clone)]
pub struct DepositStats {
    pub total_amount: u64,
    pub count: u64,
    pub average_amount: u64,
    pub sol_deposits: u64,
    pub token_deposits: u64,
    pub time_window_hours: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_vault_pubkeys_valid() {
        assert!(Pubkey::from_str(COLD_VAULT_1).is_ok());
        assert!(Pubkey::from_str(COLD_VAULT_2).is_ok());
        assert!(Pubkey::from_str(COLD_VAULT_3).is_ok());
        assert!(Pubkey::from_str(COLD_VAULT_4).is_ok());
    }

    #[test]
    fn test_rate_limiter() {
        let mut limiter = RateLimiter::new();
        for _ in 0..MAX_DEPOSITS_PER_WINDOW {
            assert!(limiter.check_and_update().is_ok());
        }
        assert!(limiter.check_and_update().is_err());
    }
}

