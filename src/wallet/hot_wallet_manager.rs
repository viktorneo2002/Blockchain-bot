use anyhow::{anyhow, Context, Result};
use dashmap::DashMap;
use parking_lot::RwLock;
use rand::{seq::SliceRandom, thread_rng};
use solana_client::{
    nonblocking::rpc_client::RpcClient,
    rpc_config::RpcSendTransactionConfig,
};
use solana_sdk::{
    commitment_config::{CommitmentConfig, CommitmentLevel},
    compute_budget::ComputeBudgetInstruction,
    hash::Hash,
    instruction::Instruction,
    message::Message,
    native_token::LAMPORTS_PER_SOL,
    nonce::{self, State as NonceState},
    pubkey::Pubkey,
    signature::{Keypair, Signature, Signer},
    system_instruction,
    transaction::{Transaction, TransactionError},
};
use solana_transaction_status::UiTransactionEncoding;
use std::{
    collections::{HashMap, VecDeque},
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc,
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use tokio::{
    sync::{mpsc, Mutex, RwLock as TokioRwLock, Semaphore},
    time::{interval, sleep, timeout},
};
use tracing::{debug, error, info, warn};

#[derive(Debug, Clone)]
pub struct WalletConfig {
    pub min_balance: u64,
    pub optimal_balance: u64,
    pub max_balance: u64,
    pub rotation_interval_secs: u64,
    pub max_usage_per_interval: u32,
    pub health_check_interval_secs: u64,
    pub max_concurrent_wallets: usize,
    pub nonce_rent_lamports: u64,
    pub max_consecutive_failures: u64,
}

impl Default for WalletConfig {
    fn default() -> Self {
        Self {
            min_balance: 50_000_000,
            optimal_balance: 100_000_000,
            max_balance: 500_000_000,
            rotation_interval_secs: 300,
            max_usage_per_interval: 100,
            health_check_interval_secs: 30,
            max_concurrent_wallets: 50,
            nonce_rent_lamports: 1_447_680,
            max_consecutive_failures: 5,
        }
    }
}

#[derive(Debug, Clone)]
pub struct WalletState {
    pub keypair: Arc<Keypair>,
    pub balance: AtomicU64,
    pub nonce_account: Option<Pubkey>,
    pub nonce_authority: Option<Arc<Keypair>>,
    pub nonce_hash: Arc<RwLock<Option<Hash>>>,
    pub last_used: AtomicU64,
    pub usage_count: AtomicU64,
    pub is_healthy: AtomicBool,
    pub consecutive_failures: AtomicU64,
    pub created_at: u64,
    pub last_rotation: AtomicU64,
    pub pending_txs: Arc<DashMap<Signature, Instant>>,
}

pub struct TransactionMetrics {
    pub sent: AtomicU64,
    pub confirmed: AtomicU64,
    pub failed: AtomicU64,
    pub avg_confirmation_ms: AtomicU64,
    pub last_success: AtomicU64,
}

pub struct HotWalletManager {
    config: WalletConfig,
    rpc_client: Arc<RpcClient>,
    wallets: Arc<DashMap<Pubkey, Arc<WalletState>>>,
    available_wallets: Arc<TokioRwLock<VecDeque<Pubkey>>>,
    master_wallet: Arc<Keypair>,
    wallet_semaphore: Arc<Semaphore>,
    metrics: Arc<TransactionMetrics>,
    is_running: Arc<AtomicBool>,
    blockhash_cache: Arc<RwLock<(Hash, Instant)>>,
    blacklist: Arc<DashMap<Pubkey, u64>>,
    tx_queue: mpsc::UnboundedSender<(Transaction, Arc<WalletState>, Instant)>,
}

impl HotWalletManager {
    pub async fn new(
        rpc_client: Arc<RpcClient>,
        master_wallet: Arc<Keypair>,
        initial_wallets: usize,
        config: WalletConfig,
    ) -> Result<Arc<Self>> {
        let (tx_queue, rx) = mpsc::unbounded_channel();
        
        let manager = Arc::new(Self {
            config,
            rpc_client,
            wallets: Arc::new(DashMap::new()),
            available_wallets: Arc::new(TokioRwLock::new(VecDeque::new())),
            master_wallet,
            wallet_semaphore: Arc::new(Semaphore::new(initial_wallets)),
            metrics: Arc::new(TransactionMetrics {
                sent: AtomicU64::new(0),
                confirmed: AtomicU64::new(0),
                failed: AtomicU64::new(0),
                avg_confirmation_ms: AtomicU64::new(0),
                last_success: AtomicU64::new(0),
            }),
            is_running: Arc::new(AtomicBool::new(true)),
            blockhash_cache: Arc::new(RwLock::new((Hash::default(), Instant::now()))),
            blacklist: Arc::new(DashMap::new()),
            tx_queue,
        });

        manager.initialize_wallets(initial_wallets).await?;
        manager.spawn_background_tasks(rx);
        
        Ok(manager)
    }

    async fn initialize_wallets(&self, count: usize) -> Result<()> {
        let mut tasks = Vec::new();
        let batch_size = 5;
        
        for batch_start in (0..count).step_by(batch_size) {
            let batch_end = (batch_start + batch_size).min(count);
            
            for _ in batch_start..batch_end {
                let manager = self.clone();
                tasks.push(tokio::spawn(async move {
                    manager.create_and_fund_wallet().await
                }));
            }
            
            for task in tasks.drain(..) {
                task.await.context("Task join error")??;
            }
            
            if batch_end < count {
                sleep(Duration::from_millis(100)).await;
            }
        }
        
        info!("Initialized {} wallets successfully", count);
        Ok(())
    }

    async fn create_and_fund_wallet(&self) -> Result<()> {
        let wallet_keypair = Arc::new(Keypair::new());
        let nonce_keypair = Keypair::new();
        let nonce_authority = Arc::new(Keypair::new());
        
        let blockhash = self.get_fresh_blockhash().await?;
        
        let create_account_ix = system_instruction::create_nonce_account(
            &self.master_wallet.pubkey(),
            &nonce_keypair.pubkey(),
            &nonce_authority.pubkey(),
            self.config.nonce_rent_lamports,
        );
        
        let fund_wallet_ix = system_instruction::transfer(
            &self.master_wallet.pubkey(),
            &wallet_keypair.pubkey(),
            self.config.optimal_balance,
        );
        
        let mut tx = Transaction::new_with_payer(
            &[create_account_ix, fund_wallet_ix],
            Some(&self.master_wallet.pubkey()),
        );
        
        tx.sign(&[self.master_wallet.as_ref(), &nonce_keypair], blockhash);
        
        let config = RpcSendTransactionConfig {
            skip_preflight: true,
            preflight_commitment: Some(CommitmentLevel::Confirmed),
            encoding: Some(UiTransactionEncoding::Base64),
            max_retries: Some(2),
            min_context_slot: None,
        };
        
        let sig = self.rpc_client
            .send_transaction_with_config(&tx, config)
            .await
            .context("Failed to send wallet creation transaction")?;
        
        let start = Instant::now();
        let confirmed = self.confirm_transaction(&sig, Duration::from_secs(30)).await?;
        
        if confirmed {
            let wallet_state = Arc::new(WalletState {
                keypair: wallet_keypair.clone(),
                balance: AtomicU64::new(self.config.optimal_balance),
                nonce_account: Some(nonce_keypair.pubkey()),
                nonce_authority: Some(nonce_authority),
                nonce_hash: Arc::new(RwLock::new(None)),
                last_used: AtomicU64::new(0),
                usage_count: AtomicU64::new(0),
                is_healthy: AtomicBool::new(true),
                consecutive_failures: AtomicU64::new(0),
                created_at: SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs(),
                last_rotation: AtomicU64::new(SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs()),
                pending_txs: Arc::new(DashMap::new()),
            });
            
            let pubkey = wallet_keypair.pubkey();
            self.wallets.insert(pubkey, wallet_state);
            self.available_wallets.write().await.push_back(pubkey);
            
            debug!("Created wallet {} in {:?}", pubkey, start.elapsed());
            Ok(())
        } else {
            Err(anyhow!("Failed to confirm wallet creation transaction"))
        }
    }

    pub async fn get_wallet(&self) -> Result<Arc<WalletState>> {
        let _permit = self.wallet_semaphore
            .acquire()
            .await
            .context("Failed to acquire wallet semaphore")?;
        
        let mut retries = 0;
        const MAX_RETRIES: u32 = 20;
        
        loop {
            let mut available = self.available_wallets.write().await;
            
            while let Some(pubkey) = available.pop_front() {
                if self.blacklist.contains_key(&pubkey) {
                    continue;
                }
                
                if let Some(wallet) = self.wallets.get(&pubkey) {
                    if !wallet.is_healthy.load(Ordering::Acquire) {
                        continue;
                    }
                    
                    let usage = wallet.usage_count.load(Ordering::Acquire);
                    if usage >= self.config.max_usage_per_interval as u64 {
                        available.push_back(pubkey);
                        continue;
                    }
                    
                    wallet.usage_count.fetch_add(1, Ordering::AcqRel);
                    wallet.last_used.store(
                        SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs(),
                        Ordering::Release,
                    );
                    
                    available.push_back(pubkey);
                    drop(available);
                    
                    return Ok(wallet.clone());
                }
            }
            
            drop(available);
            
            retries += 1;
            if retries >= MAX_RETRIES {
                return Err(anyhow!("No healthy wallets available after {} retries", MAX_RETRIES));
            }
            
            sleep(Duration::from_millis(10 * retries as u64)).await;
        }
    }

    pub async fn send_transaction(
        self: &Arc<Self>,
        instructions: Vec<Instruction>,
        signers: Vec<&Keypair>,
        priority_fee: u64,
    ) -> Result<Signature> {
        let wallet = self.get_wallet().await?;
        let start = Instant::now();
        
        let mut final_instructions = vec![
            ComputeBudgetInstruction::set_compute_unit_price(priority_fee),
            ComputeBudgetInstruction::set_compute_unit_limit(1_400_000),
        ];
        
        if let (Some(nonce_account), Some(nonce_authority)) = (&wallet.nonce_account, &wallet.nonce_authority) {
            if let Some(nonce_hash) = *wallet.nonce_hash.read() {
                let advance_nonce_ix = system_instruction::advance_nonce_account(
                    nonce_account,
                    &nonce_authority.pubkey(),
                );
                final_instructions.insert(0, advance_nonce_ix);
            }
        }
        
        final_instructions.extend(instructions);
        
        let blockhash = if let Some(hash) = *wallet.nonce_hash.read() {
            hash
        } else {
            self.get_fresh_blockhash().await?
        };
        
        let message = Message::new_with_blockhash(
            &final_instructions,
            Some(&wallet.keypair.pubkey()),
            &blockhash,
        );
        
        let mut all_signers = vec![wallet.keypair.as_ref()];
        if let Some(nonce_auth) = &wallet.nonce_authority {
            all_signers.push(nonce_auth.as_ref());
        }
        all_signers.extend(signers);
        
        let mut tx = Transaction::new_unsigned(message);
        tx.sign(&all_signers, blockhash);
        
        let signature = tx.signatures[0];
        wallet.pending_txs.insert(signature, start);
        
        self.metrics.sent.fetch_add(1, Ordering::Relaxed);
        self.tx_queue.send((tx, wallet.clone(), start))?;
        
        Ok(signature)
    }

    async fn get_fresh_blockhash(&self) -> Result<Hash> {
        let (cached_hash, cached_time) = *self.blockhash_cache.read();
        
        if cached_time.elapsed() < Duration::from_secs(30) {
            return Ok(cached_hash);
        }
        
        let new_hash = self.rpc_client
            .get_latest_blockhash()
            .await
            .context("Failed to get latest blockhash")?;
        
        *self.blockhash_cache.write() = (new_hash, Instant::now());
        Ok(new_hash)
    }

    async fn confirm_transaction(&self, signature: &Signature, timeout_duration: Duration) -> Result<bool> {
        let start = Instant::now();
        
        while start.elapsed() < timeout_duration {
            match self.rpc_client.get_signature_status(signature).await {
                Ok(Some(Ok(_))) => return Ok(true),
                Ok(Some(Err(err))) => {
                    error!("Transaction failed: {:?}", err);
                    return Ok(false);
                }
                Ok(None) => {
                    sleep(Duration::from_millis(50)).await;
                }
                Err(e) => {
                    if start.elapsed() > timeout_duration - Duration::from_secs(5) {
                        return Err(anyhow!("RPC error confirming transaction: {}", e));
                    }
                    sleep(Duration::from_millis(100)).await;
                }
            }
        }
        
        Ok(false)
    }

    fn spawn_background_tasks(self: &Arc<Self>, mut rx: mpsc::UnboundedReceiver<(Transaction, Arc<WalletState>, Instant)>) {
        // Transaction sender task
        let manager = self.clone();
        tokio::spawn(async move {
            while manager.is_running.load(Ordering::Acquire) {
                let mut batch = Vec::new();
                
                // Collect batch
                let deadline = sleep(Duration::from_millis(10));
                tokio::pin!(deadline);
                
                loop {
                    tokio::select! {
                        Some(item) = rx.recv() => {
                            batch.push(item);
                            if batch.len() >= 10 {
                                break;
                            }
                        }
                        _ = &mut deadline => {
                            break;
                        }
                    }
                }
                
                if batch.is_empty() {
                    continue;
                }
                
                for (tx, wallet, start) in batch {
                    let rpc = manager.rpc_client.clone();
                    let metrics = manager.metrics.clone();
                    let wallet_clone = wallet.clone();
                    
                    tokio::spawn(async move {
                        let config = RpcSendTransactionConfig {
                            skip_preflight: true,
                            preflight_commitment: Some(CommitmentLevel::Processed),
                            encoding: Some(UiTransactionEncoding::Base64),
                            max_retries: Some(0),
                            min_context_slot: None,
                        };
                        
                        let sig = tx.signatures[0];
                        
                        match rpc.send_transaction_with_config(&tx, config).await {
                            Ok(_) => {
                                match timeout(Duration::from_secs(20), async {
                                    loop {
                                        match rpc.get_signature_status(&sig).await {
                                            Ok(Some(result)) => return result.is_ok(),
                                            Ok(None) => sleep(Duration::from_millis(50)).await,
                                            Err(_) => sleep(Duration::from_millis(100)).await,
                                        }
                                    }
                                }).await {
                                    Ok(true) => {
                                        metrics.confirmed.fetch_add(1, Ordering::Relaxed);
                                        wallet_clone.consecutive_failures.store(0, Ordering::Release);
                                        
                                        let duration_ms = start.elapsed().as_millis() as u64;
                                        let current_avg = metrics.avg_confirmation_ms.load(Ordering::Acquire);
                                        let new_avg = (current_avg * 9 + duration_ms) / 10;
                                        metrics.avg_confirmation_ms.store(new_avg, Ordering::Release);
                                        metrics.last_success.store(
                                            SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs(),
                                            Ordering::Release,
                                        );
                                    }
                                    _ => {
                                        metrics.failed.fetch_add(1, Ordering::Relaxed);
                                        let failures = wallet_clone.consecutive_failures.fetch_add(1, Ordering::AcqRel) + 1;
                                        
                                        if failures > 5 {
                                            wallet_clone.is_healthy.store(false, Ordering::Release);
                                        }
                                    }
                                }
                            }
                            Err(e) => {
                                error!("Failed to send transaction: {}", e);
                                metrics.failed.fetch_add(1, Ordering::Relaxed);
                                let failures = wallet_clone.consecutive_failures.fetch_add(1, Ordering::AcqRel) + 1;
                                
                                if failures > 3 {
                                    wallet_clone.is_healthy.store(false, Ordering::Release);
                                }
                            }
                        }
                        
                        wallet_clone.pending_txs.remove(&sig);
                    });
                }
            }
        });

        // Wallet health monitor
        let manager = self.clone();
        tokio::spawn(async move {
            let mut interval = interval(Duration::from_secs(manager.config.health_check_interval_secs));
            
            while manager.is_running.load(Ordering::Acquire) {
                interval.tick().await;
                
                let mut tasks = Vec::new();
                
                for entry in manager.wallets.iter() {
                    let wallet = entry.value().clone();
                    let rpc = manager.rpc_client.clone();
                    let config = manager.config.clone();
                    
                    tasks.push(tokio::spawn(async move {
                        let pubkey = wallet.keypair.pubkey();
                        
                        match timeout(Duration::from_secs(5), rpc.get_balance(&pubkey)).await {
                            Ok(Ok(balance)) => {
                                wallet.balance.store(balance, Ordering::Release);
                                
                                if balance < config.min_balance {
                                    warn!("Wallet {} balance too low: {} SOL", pubkey, balance as f64 / LAMPORTS_PER_SOL as f64);
                                    wallet.is_healthy.store(false, Ordering::Release);
                                } else if balance > config.max_balance {
                                    warn!("Wallet {} balance too high: {} SOL", pubkey, balance as f64 / LAMPORTS_PER_SOL as f64);
                                } else if wallet.consecutive_failures.load(Ordering::Acquire) == 0 {
                                    wallet.is_healthy.store(true, Ordering::Release);
                                }
                            }
                            _ => {
                                wallet.consecutive_failures.fetch_add(1, Ordering::AcqRel);
                            }
                        }
                        
                        // Update nonce hash
                        if let Some(nonce_account) = &wallet.nonce_account {
                            match timeout(Duration::from_secs(3), rpc.get_account(nonce_account)).await {
                                Ok(Ok(account)) => {
                                    if let Ok(NonceState::Initialized(data)) = bincode::deserialize::<NonceState>(&account.data) {
                                        *wallet.nonce_hash.write() = Some(data.blockhash());
                                    }
                                }
                                _ => {
                                    *wallet.nonce_hash.write() = None;
                                }
                            }
                        }
                        
                        // Clean old pending transactions
                        wallet.pending_txs.retain(|_, instant| instant.elapsed() < Duration::from_secs(60));
                    }));
                }
                
                for task in tasks {
                    let _ = task.await;
                }
            }
        });

        // Wallet rotation and rebalancing
        let manager = self.clone();
        tokio::spawn(async move {
            let mut interval = interval(Duration::from_secs(manager.config.rotation_interval_secs));
            
            while manager.is_running.load(Ordering::Acquire) {
                interval.tick().await;
                
                let current_time = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();
                
                // Clean expired blacklist entries
                manager.blacklist.retain(|_, expiry| *expiry > current_time);
                
                // Reset usage counts and rebalance
                let mut rebalance_tasks = Vec::new();
                
                for entry in manager.wallets.iter() {
                    let wallet = entry.value();
                    
                    // Reset usage count
                    wallet.usage_count.store(0, Ordering::Release);
                    wallet.last_rotation.store(current_time, Ordering::Release);
                    
                    // Check if rebalancing needed
                    let balance = wallet.balance.load(Ordering::Acquire);
                    
                    if balance < manager.config.min_balance && wallet.is_healthy.load(Ordering::Acquire) {
                        let transfer_amount = manager.config.optimal_balance.saturating_sub(balance);
                        
                        if transfer_amount > 0 {
                            let wallet_pubkey = wallet.keypair.pubkey();
                            let master = manager.master_wallet.clone();
                            let rpc = manager.rpc_client.clone();
                            
                            rebalance_tasks.push(tokio::spawn(async move {
                                let ix = system_instruction::transfer(
                                    &master.pubkey(),
                                    &wallet_pubkey,
                                    transfer_amount,
                                );
                                
                                match rpc.get_latest_blockhash().await {
                                    Ok(blockhash) => {
                                        let mut tx = Transaction::new_with_payer(&[ix], Some(&master.pubkey()));
                                        tx.sign(&[master.as_ref()], blockhash);
                                        
                                        let config = RpcSendTransactionConfig {
                                            skip_preflight: false,
                                            preflight_commitment: Some(CommitmentLevel::Confirmed),
                                            encoding: Some(UiTransactionEncoding::Base64),
                                            max_retries: Some(2),
                                            min_context_slot: None,
                                        };
                                        
                                        match rpc.send_transaction_with_config(&tx, config).await {
                                            Ok(sig) => {
                                                info!("Rebalanced wallet {} with {} lamports", wallet_pubkey, transfer_amount);
                                            }
                                            Err(e) => {
                                                error!("Failed to rebalance wallet {}: {}", wallet_pubkey, e);
                                            }
                                        }
                                    }
                                    Err(e) => {
                                        error!("Failed to get blockhash for rebalancing: {}", e);
                                    }
                                }
                            }));
                        }
                    }
                }
                
                for task in rebalance_tasks {
                    let _ = task.await;
                }
                
                // Shuffle available wallets for better distribution
                let mut available = manager.available_wallets.write().await;
                let mut wallets: Vec<_> = available.drain(..).collect();
                wallets.shuffle(&mut thread_rng());
                available.extend(wallets);
            }
        });

        // Cleanup unhealthy wallets
        let manager = self.clone();
        tokio::spawn(async move {
            let mut interval = interval(Duration::from_secs(600)); // 10 minutes
            
            while manager.is_running.load(Ordering::Acquire) {
                interval.tick().await;
                
                let unhealthy: Vec<_> = manager.wallets.iter()
                    .filter(|entry| {
                        let wallet = entry.value();
                        !wallet.is_healthy.load(Ordering::Acquire) ||
                        wallet.consecutive_failures.load(Ordering::Acquire) > manager.config.max_consecutive_failures
                    })
                    .map(|entry| entry.key().clone())
                    .collect();
                
                for pubkey in unhealthy {
                    if let Some((_, wallet)) = manager.wallets.remove(&pubkey) {
                        manager.available_wallets.write().await.retain(|&p| p != pubkey);
                        
                        // Try to drain wallet
                        let balance = wallet.balance.load(Ordering::Acquire);
                        if balance > manager.config.nonce_rent_lamports + 10_000 {
                            let transfer_amount = balance - manager.config.nonce_rent_lamports - 5_000;
                            
                            let ix = system_instruction::transfer(
                                &pubkey,
                                &manager.master_wallet.pubkey(),
                                transfer_amount,
                            );
                            
                            if let Ok(blockhash) = manager.rpc_client.get_latest_blockhash().await {
                                let mut tx = Transaction::new_with_payer(&[ix], Some(&pubkey));
                                tx.sign(&[wallet.keypair.as_ref()], blockhash);
                                
                                let _ = manager.rpc_client.send_transaction(&tx).await;
                            }
                        }
                        
                        warn!("Removed unhealthy wallet: {}", pubkey);
                    }
                }
                
                // Ensure minimum wallet count
                let current_count = manager.wallets.len();
                let min_count = manager.config.max_concurrent_wallets / 2;
                
                if current_count < min_count {
                    let to_add = min_count - current_count;
                    for _ in 0..to_add {
                        let _ = manager.create_and_fund_wallet().await;
                    }
                }
            }
        });
    }

    pub fn blacklist_wallet(&self, pubkey: &Pubkey, duration_secs: u64) {
        let expiry = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs() + duration_secs;
        self.blacklist.insert(*pubkey, expiry);
        
        if let Some(wallet) = self.wallets.get(pubkey) {
            wallet.is_healthy.store(false, Ordering::Release);
        }
    }

    pub async fn shutdown(self: Arc<Self>) -> Result<()> {
        info!("Initiating HotWalletManager shutdown");
        self.is_running.store(false, Ordering::Release);
        
        // Wait for pending transactions
        sleep(Duration::from_secs(2)).await;
        
        // Drain all wallets
        let drain_tasks: Vec<_> = self.wallets.iter()
            .filter_map(|entry| {
                let wallet = entry.value();
                let balance = wallet.balance.load(Ordering::Acquire);
                
                if balance > self.config.nonce_rent_lamports + 10_000 {
                    let transfer_amount = balance - self.config.nonce_rent_lamports - 5_000;
                    let pubkey = wallet.keypair.pubkey();
                    let keypair = wallet.keypair.clone();
                    let master = self.master_wallet.pubkey();
                    let rpc = self.rpc_client.clone();
                    
                    Some(tokio::spawn(async move {
                        let ix = system_instruction::transfer(&pubkey, &master, transfer_amount);
                        
                        if let Ok(blockhash) = rpc.get_latest_blockhash().await {
                            let mut tx = Transaction::new_with_payer(&[ix], Some(&pubkey));
                            tx.sign(&[keypair.as_ref()], blockhash);
                            
                            match rpc.send_and_confirm_transaction(&tx).await {
                                Ok(sig) => info!("Drained wallet {} of {} lamports: {}", pubkey, transfer_amount, sig),
                                Err(e) => error!("Failed to drain wallet {}: {}", pubkey, e),
                            }
                        }
                    }))
                } else {
                    None
                }
            })
            .collect();
        
        for task in drain_tasks {
            let _ = timeout(Duration::from_secs(10), task).await;
        }
        
        info!("HotWalletManager shutdown complete");
        Ok(())
    }

    pub async fn send_with_optimal_priority(
        self: &Arc<Self>,
        instructions: Vec<Instruction>,
        signers: Vec<&Keypair>,
        base_priority: u64,
    ) -> Result<Signature> {
        let recent_avg = self.metrics.avg_confirmation_ms.load(Ordering::Acquire);
        
        // Dynamic priority based on network conditions
        let priority_multiplier = match recent_avg {
            0..=1000 => 1,    // Fast network
            1001..=3000 => 2, // Normal network
            3001..=5000 => 3, // Slow network
            _ => 5,           // Very slow network
        };
        
        let optimal_priority = base_priority * priority_multiplier;
        self.send_transaction(instructions, signers, optimal_priority).await
    }

    pub async fn batch_send_parallel(
        self: &Arc<Self>,
        transactions: Vec<(Vec<Instruction>, Vec<&Keypair>, u64)>,
        max_parallel: usize,
    ) -> Vec<Result<Signature>> {
        let semaphore = Arc::new(Semaphore::new(max_parallel));
        let mut tasks = Vec::with_capacity(transactions.len());
        
        for (instructions, signers, priority) in transactions {
            let permit = semaphore.clone().acquire_owned().await.unwrap();
            let manager = self.clone();
            let signers_owned: Vec<Keypair> = signers.into_iter()
                .map(|k| Keypair::from_bytes(&k.to_bytes()).unwrap())
                .collect();
            
            tasks.push(tokio::spawn(async move {
                let _permit = permit;
                let signer_refs: Vec<&Keypair> = signers_owned.iter().collect();
                manager.send_transaction(instructions, signer_refs, priority).await
            }));
        }
        
        let mut results = Vec::with_capacity(tasks.len());
        for task in tasks {
            match task.await {
                Ok(result) => results.push(result),
                Err(e) => results.push(Err(anyhow!("Task join error: {}", e))),
            }
        }
        
        results
    }

    pub fn get_metrics(&self) -> HashMap<String, u64> {
        let mut metrics = HashMap::new();
        
        metrics.insert("total_sent".to_string(), self.metrics.sent.load(Ordering::Acquire));
        metrics.insert("total_confirmed".to_string(), self.metrics.confirmed.load(Ordering::Acquire));
        metrics.insert("total_failed".to_string(), self.metrics.failed.load(Ordering::Acquire));
        metrics.insert("avg_confirmation_ms".to_string(), self.metrics.avg_confirmation_ms.load(Ordering::Acquire));
        metrics.insert("last_success_timestamp".to_string(), self.metrics.last_success.load(Ordering::Acquire));
        
        let total_wallets = self.wallets.len() as u64;
        let healthy_wallets = self.wallets.iter()
            .filter(|entry| entry.value().is_healthy.load(Ordering::Acquire))
            .count() as u64;
        
        metrics.insert("total_wallets".to_string(), total_wallets);
        metrics.insert("healthy_wallets".to_string(), healthy_wallets);
        metrics.insert("blacklisted_wallets".to_string(), self.blacklist.len() as u64);
        
        let total_balance: u64 = self.wallets.iter()
            .map(|entry| entry.value().balance.load(Ordering::Acquire))
            .sum();
        
        metrics.insert("total_balance_lamports".to_string(), total_balance);
        
        let success_rate = if self.metrics.sent.load(Ordering::Acquire) > 0 {
            (self.metrics.confirmed.load(Ordering::Acquire) * 100) / self.metrics.sent.load(Ordering::Acquire)
        } else {
            0
        };
        
        metrics.insert("success_rate_percent".to_string(), success_rate);
        
        metrics
    }

    pub async fn get_wallet_info(&self, pubkey: &Pubkey) -> Option<HashMap<String, String>> {
        self.wallets.get(pubkey).map(|entry| {
            let wallet = entry.value();
            let mut info = HashMap::new();
            
            info.insert("pubkey".to_string(), pubkey.to_string());
            info.insert("balance".to_string(), wallet.balance.load(Ordering::Acquire).to_string());
            info.insert("is_healthy".to_string(), wallet.is_healthy.load(Ordering::Acquire).to_string());
            info.insert("usage_count".to_string(), wallet.usage_count.load(Ordering::Acquire).to_string());
            info.insert("consecutive_failures".to_string(), wallet.consecutive_failures.load(Ordering::Acquire).to_string());
            info.insert("pending_txs".to_string(), wallet.pending_txs.len().to_string());
            
            if let Some(nonce) = &wallet.nonce_account {
                info.insert("nonce_account".to_string(), nonce.to_string());
            }
            
            info
        })
    }

    pub async fn optimize_wallet_distribution(&self) -> Result<()> {
        let total_balance: u64 = self.wallets.iter()
            .map(|entry| entry.value().balance.load(Ordering::Acquire))
            .sum();
        
        let wallet_count = self.wallets.len() as u64;
        if wallet_count == 0 {
            return Ok(());
        }
        
        let target_balance = (total_balance / wallet_count).min(self.config.optimal_balance);
        
        let mut transfers = Vec::new();
        
        // Identify over-funded and under-funded wallets
        let mut overfunded = Vec::new();
        let mut underfunded = Vec::new();
        
        for entry in self.wallets.iter() {
            let wallet = entry.value();
            let balance = wallet.balance.load(Ordering::Acquire);
            
            if balance > target_balance + 10_000_000 {
                overfunded.push((wallet.keypair.pubkey(), balance - target_balance));
            } else if balance < target_balance - 10_000_000 && wallet.is_healthy.load(Ordering::Acquire) {
                underfunded.push((wallet.keypair.pubkey(), target_balance - balance));
            }
        }
        
        // Match overfunded with underfunded
        overfunded.sort_by_key(|&(_, excess)| std::cmp::Reverse(excess));
        underfunded.sort_by_key(|&(_, deficit)| std::cmp::Reverse(deficit));
        
        let mut i = 0;
        let mut j = 0;
        
        while i < overfunded.len() && j < underfunded.len() {
            let (from_pubkey, excess) = overfunded[i];
            let (to_pubkey, deficit) = underfunded[j];
            
            let transfer_amount = excess.min(deficit);
            
            if transfer_amount > 10_000 {
                transfers.push((from_pubkey, to_pubkey, transfer_amount));
                
                overfunded[i].1 -= transfer_amount;
                underfunded[j].1 -= transfer_amount;
                
                if overfunded[i].1 < 10_000 {
                    i += 1;
                }
                if underfunded[j].1 < 10_000 {
                    j += 1;
                }
            } else {
                break;
            }
        }
        
        // Execute transfers
        for (from, to, amount) in transfers {
            if let Some(from_wallet) = self.wallets.get(&from) {
                let ix = system_instruction::transfer(&from, &to, amount);
                
                match self.send_transaction(vec![ix], vec![from_wallet.keypair.as_ref()], 10_000).await {
                    Ok(sig) => info!("Redistributed {} lamports from {} to {}", amount, from, to),
                    Err(e) => warn!("Failed to redistribute funds: {}", e),
                }
            }
        }
        
        Ok(())
    }

    pub async fn emergency_pause(&self) {
        self.is_running.store(false, Ordering::Release);
        
        // Mark all wallets as unhealthy
        for entry in self.wallets.iter() {
            entry.value().is_healthy.store(false, Ordering::Release);
        }
        
        info!("Emergency pause activated - all operations halted");
    }

    pub async fn resume_operations(&self) {
        self.is_running.store(true, Ordering::Release);
        
        // Re-evaluate wallet health
        for entry in self.wallets.iter() {
            let wallet = entry.value();
            if wallet.consecutive_failures.load(Ordering::Acquire) < self.config.max_consecutive_failures {
                wallet.is_healthy.store(true, Ordering::Release);
            }
        }
        
        info!("Operations resumed");
    }
}

impl Clone for HotWalletManager {
    fn clone(&self) -> Self {
        Self {
            config: self.config.clone(),
            rpc_client: self.rpc_client.clone(),
            wallets: self.wallets.clone(),
            available_wallets: self.available_wallets.clone(),
            master_wallet: self.master_wallet.clone(),
            wallet_semaphore: self.wallet_semaphore.clone(),
            metrics: self.metrics.clone(),
            is_running: self.is_running.clone(),
            blockhash_cache: self.blockhash_cache.clone(),
            blacklist: self.blacklist.clone(),
            tx_queue: self.tx_queue.clone(),
        }
    }
}
