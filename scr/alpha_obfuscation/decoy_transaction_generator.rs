use anchor_client::{
    solana_sdk::{
        commitment_config::CommitmentConfig,
        compute_budget::ComputeBudgetInstruction,
        instruction::{AccountMeta, Instruction},
        native_token::LAMPORTS_PER_SOL,
        pubkey::Pubkey,
        signature::{Keypair, Signature},
        signer::Signer,
        system_instruction,
        transaction::Transaction,
    },
    Client, Cluster,
};
use anchor_lang::prelude::*;
use rand::{rngs::StdRng, Rng, SeedableRng};
use serde::{Deserialize, Serialize};
use solana_client::{
    nonblocking::rpc_client::RpcClient,
    rpc_config::{RpcSendTransactionConfig, RpcSimulateTransactionConfig},
};
use solana_transaction_status::UiTransactionEncoding;
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

const MAX_DECOY_WALLETS: usize = 20;
const MIN_DECOY_BALANCE: u64 = 50_000_000; // 0.05 SOL
const MAX_DECOY_AMOUNT: u64 = 100_000_000; // 0.1 SOL
const MIN_DECOY_AMOUNT: u64 = 1_000_000; // 0.001 SOL
const DECOY_COMPUTE_UNITS: u32 = 200_000;
const PRIORITY_FEE_MULTIPLIER: f64 = 1.2;
const MAX_RETRY_ATTEMPTS: u32 = 3;
const RETRY_DELAY_MS: u64 = 100;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DecoyConfig {
    pub enabled: bool,
    pub wallets: Vec<Keypair>,
    pub target_tps: f64,
    pub burst_mode: bool,
    pub burst_multiplier: f64,
    pub randomize_timing: bool,
    pub include_token_ops: bool,
    pub priority_fee_lamports: u64,
    pub simulation_required: bool,
}

#[derive(Debug, Clone)]
pub struct DecoyMetrics {
    pub transactions_sent: u64,
    pub transactions_confirmed: u64,
    pub transactions_failed: u64,
    pub total_fees_spent: u64,
    pub last_transaction_time: u64,
}

#[derive(Debug, Clone)]
pub enum DecoyType {
    SimpleTransfer,
    SplitTransfer,
    TokenTransfer,
    DummyProgram,
    CompositeTransaction,
}

pub struct DecoyTransactionGenerator {
    rpc_client: Arc<RpcClient>,
    config: Arc<RwLock<DecoyConfig>>,
    metrics: Arc<Mutex<DecoyMetrics>>,
    decoy_wallets: Arc<RwLock<Vec<Keypair>>>,
    transaction_queue: Arc<Mutex<VecDeque<Transaction>>>,
    rng: Arc<Mutex<StdRng>>,
    active: Arc<RwLock<bool>>,
}

impl DecoyTransactionGenerator {
    pub async fn new(
        rpc_url: &str,
        config: DecoyConfig,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let rpc_client = Arc::new(RpcClient::new_with_commitment(
            rpc_url.to_string(),
            CommitmentConfig::confirmed(),
        ));

        let decoy_wallets = if config.wallets.is_empty() {
            Self::generate_decoy_wallets(MAX_DECOY_WALLETS)
        } else {
            config.wallets.clone()
        };

        Ok(Self {
            rpc_client,
            config: Arc::new(RwLock::new(config)),
            metrics: Arc::new(Mutex::new(DecoyMetrics {
                transactions_sent: 0,
                transactions_confirmed: 0,
                transactions_failed: 0,
                total_fees_spent: 0,
                last_transaction_time: 0,
            })),
            decoy_wallets: Arc::new(RwLock::new(decoy_wallets)),
            transaction_queue: Arc::new(Mutex::new(VecDeque::new())),
            rng: Arc::new(Mutex::new(StdRng::from_entropy())),
            active: Arc::new(RwLock::new(false)),
        })
    }

    fn generate_decoy_wallets(count: usize) -> Vec<Keypair> {
        (0..count).map(|_| Keypair::new()).collect()
    }

    pub async fn start(&self) -> Result<(), Box<dyn std::error::Error>> {
        *self.active.write().await = true;

        let generator = self.clone_self();
        tokio::spawn(async move {
            if let Err(e) = generator.generation_loop().await {
                log::error!("Decoy generation loop error: {:?}", e);
            }
        });

        let sender = self.clone_self();
        tokio::spawn(async move {
            if let Err(e) = sender.sending_loop().await {
                log::error!("Decoy sending loop error: {:?}", e);
            }
        });

        Ok(())
    }

    pub async fn stop(&self) {
        *self.active.write().await = false;
    }

    async fn generation_loop(&self) -> Result<(), Box<dyn std::error::Error>> {
        let mut interval = interval(Duration::from_millis(100));

        while *self.active.read().await {
            interval.tick().await;

            let config = self.config.read().await.clone();
            if !config.enabled {
                continue;
            }

            let current_time = SystemTime::now()
                .duration_since(UNIX_EPOCH)?
                .as_millis() as u64;

            let target_interval_ms = (1000.0 / config.target_tps) as u64;
            let metrics = self.metrics.lock().await;
            
            if current_time - metrics.last_transaction_time < target_interval_ms {
                continue;
            }
            drop(metrics);

            let decoy_type = self.select_decoy_type(&config).await;
            match self.generate_decoy_transaction(decoy_type).await {
                Ok(tx) => {
                    let mut queue = self.transaction_queue.lock().await;
                    if queue.len() < 100 {
                        queue.push_back(tx);
                    }
                }
                Err(e) => log::warn!("Failed to generate decoy transaction: {:?}", e),
            }
        }

        Ok(())
    }

    async fn sending_loop(&self) -> Result<(), Box<dyn std::error::Error>> {
        while *self.active.read().await {
            let tx_opt = {
                let mut queue = self.transaction_queue.lock().await;
                queue.pop_front()
            };

            if let Some(tx) = tx_opt {
                tokio::spawn({
                    let generator = self.clone_self();
                    async move {
                        if let Err(e) = generator.send_transaction_with_retry(tx).await {
                            log::warn!("Failed to send decoy transaction: {:?}", e);
                        }
                    }
                });
            } else {
                sleep(Duration::from_millis(10)).await;
            }
        }

        Ok(())
    }

    async fn select_decoy_type(&self, config: &DecoyConfig) -> DecoyType {
        let mut rng = self.rng.lock().await;
        
        if config.include_token_ops && rng.gen_bool(0.3) {
            DecoyType::TokenTransfer
        } else if rng.gen_bool(0.1) {
            DecoyType::CompositeTransaction
        } else if rng.gen_bool(0.2) {
            DecoyType::SplitTransfer
        } else if rng.gen_bool(0.1) {
            DecoyType::DummyProgram
        } else {
            DecoyType::SimpleTransfer
        }
    }

    async fn generate_decoy_transaction(
        &self,
        decoy_type: DecoyType,
    ) -> Result<Transaction, Box<dyn std::error::Error>> {
        let recent_blockhash = self.rpc_client.get_latest_blockhash().await?;
        let config = self.config.read().await.clone();
        let priority_fee = self.calculate_priority_fee(&config).await;

        let instructions = match decoy_type {
            DecoyType::SimpleTransfer => self.create_simple_transfer().await?,
            DecoyType::SplitTransfer => self.create_split_transfer().await?,
            DecoyType::TokenTransfer => self.create_token_transfer().await?,
            DecoyType::DummyProgram => self.create_dummy_program_call().await?,
            DecoyType::CompositeTransaction => self.create_composite_transaction().await?,
        };

        let mut all_instructions = vec![
            ComputeBudgetInstruction::set_compute_unit_limit(DECOY_COMPUTE_UNITS),
            ComputeBudgetInstruction::set_compute_unit_price(priority_fee),
        ];
        all_instructions.extend(instructions);

        let wallets = self.decoy_wallets.read().await;
        let signer_index = {
            let mut rng = self.rng.lock().await;
            rng.gen_range(0..wallets.len().min(1).max(wallets.len()))
        };
        let signer = &wallets[signer_index];

        let tx = Transaction::new_signed_with_payer(
            &all_instructions,
            Some(&signer.pubkey()),
            &[signer],
            recent_blockhash,
        );

        Ok(tx)
    }

    async fn create_simple_transfer(&self) -> Result<Vec<Instruction>, Box<dyn std::error::Error>> {
        let wallets = self.decoy_wallets.read().await;
        let mut rng = self.rng.lock().await;
        
        let from_index = rng.gen_range(0..wallets.len());
        let mut to_index = rng.gen_range(0..wallets.len());
        while to_index == from_index && wallets.len() > 1 {
            to_index = rng.gen_range(0..wallets.len());
        }

        let amount = rng.gen_range(MIN_DECOY_AMOUNT..MAX_DECOY_AMOUNT);
        let from_pubkey = wallets[from_index].pubkey();
        let to_pubkey = wallets[to_index].pubkey();

        Ok(vec![system_instruction::transfer(
            &from_pubkey,
            &to_pubkey,
            amount,
        )])
    }

    async fn create_split_transfer(&self) -> Result<Vec<Instruction>, Box<dyn std::error::Error>> {
        let wallets = self.decoy_wallets.read().await;
        let mut rng = self.rng.lock().await;
        
        let from_index = rng.gen_range(0..wallets.len());
        let num_recipients = rng.gen_range(2..5);
        let total_amount = rng.gen_range(MIN_DECOY_AMOUNT * 2..MAX_DECOY_AMOUNT);
        
        let mut instructions = Vec::new();
        let from_pubkey = wallets[from_index].pubkey();
        
        for i in 0..num_recipients {
            let mut to_index = rng.gen_range(0..wallets.len());
            while to_index == from_index {
                to_index = rng.gen_range(0..wallets.len());
            }
            
            let amount = if i == num_recipients - 1 {
                total_amount / num_recipients + (total_amount % num_recipients)
            } else {
                total_amount / num_recipients
            };
            
            instructions.push(system_instruction::transfer(
                &from_pubkey,
                &wallets[to_index].pubkey(),
                amount,
            ));
        }

        Ok(instructions)
    }

    async fn create_token_transfer(&self) -> Result<Vec<Instruction>, Box<dyn std::error::Error>> {
        let wallets = self.decoy_wallets.read().await;
        let mut rng = self.rng.lock().await;
        
        let from_index = rng.gen_range(0..wallets.len());
        let amount = rng.gen_range(MIN_DECOY_AMOUNT..MAX_DECOY_AMOUNT);
        
        let dummy_token_program = Pubkey::from_str("TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA")?;
        let from_pubkey = wallets[from_index].pubkey();
        
        let instruction_data = vec![3, 0, 0, 0, 0, 0, 0, 0, 0]; // Dummy transfer instruction
        
        Ok(vec![Instruction {
            program_id: dummy_token_program,
            accounts: vec![
                AccountMeta::new(from_pubkey, true),
                AccountMeta::new(from_pubkey, false),
                AccountMeta::new_readonly(from_pubkey, false),
            ],
            data: instruction_data,
        }])
    }

    async fn create_dummy_program_call(&self) -> Result<Vec<Instruction>, Box<dyn std::error::Error>> {
        let wallets = self.decoy_wallets.read().await;
        let mut rng = self.rng.lock().await;
        
        let signer_index = rng.gen_range(0..wallets.len());
        let signer_pubkey = wallets[signer_index].pubkey();
        
                let dummy_program = Pubkey::new_unique();
        let instruction_data: Vec<u8> = (0..rng.gen_range(8..64))
            .map(|_| rng.gen::<u8>())
            .collect();
        
        Ok(vec![Instruction {
            program_id: dummy_program,
            accounts: vec![
                AccountMeta::new(signer_pubkey, true),
                AccountMeta::new_readonly(signer_pubkey, false),
            ],
            data: instruction_data,
        }])
    }

    async fn create_composite_transaction(&self) -> Result<Vec<Instruction>, Box<dyn std::error::Error>> {
        let mut instructions = Vec::new();
        let mut rng = self.rng.lock().await;
        
        let num_operations = rng.gen_range(2..5);
        
        for _ in 0..num_operations {
            if rng.gen_bool(0.7) {
                let transfer = self.create_simple_transfer().await?;
                instructions.extend(transfer);
            } else {
                let dummy = self.create_dummy_program_call().await?;
                instructions.extend(dummy);
            }
        }
        
        Ok(instructions)
    }

    async fn calculate_priority_fee(&self, config: &DecoyConfig) -> u64 {
        let base_fee = config.priority_fee_lamports;
        let mut rng = self.rng.lock().await;
        
        if config.burst_mode {
            let multiplier = 1.0 + (config.burst_multiplier - 1.0) * rng.gen::<f64>();
            (base_fee as f64 * multiplier) as u64
        } else {
            let variance = rng.gen_range(0.8..1.2);
            (base_fee as f64 * variance) as u64
        }
    }

    async fn send_transaction_with_retry(
        &self,
        transaction: Transaction,
    ) -> Result<Signature, Box<dyn std::error::Error>> {
        let config = self.config.read().await.clone();
        let mut attempts = 0;
        let mut last_error = None;

        while attempts < MAX_RETRY_ATTEMPTS {
            attempts += 1;

            if config.simulation_required {
                match self.simulate_transaction(&transaction).await {
                    Ok(success) => {
                        if !success {
                            return Err("Transaction simulation failed".into());
                        }
                    }
                    Err(e) => {
                        log::warn!("Simulation error: {:?}", e);
                        if attempts == MAX_RETRY_ATTEMPTS {
                            return Err(e);
                        }
                    }
                }
            }

            let send_config = RpcSendTransactionConfig {
                skip_preflight: !config.simulation_required,
                preflight_commitment: Some(CommitmentConfig::processed().commitment),
                encoding: Some(UiTransactionEncoding::Base64),
                max_retries: Some(0),
                min_context_slot: None,
            };

            match self.rpc_client.send_transaction_with_config(&transaction, send_config).await {
                Ok(signature) => {
                    let mut metrics = self.metrics.lock().await;
                    metrics.transactions_sent += 1;
                    metrics.last_transaction_time = SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .unwrap()
                        .as_millis() as u64;
                    
                    let fee = self.estimate_transaction_fee(&transaction);
                    metrics.total_fees_spent += fee;
                    
                    drop(metrics);

                    tokio::spawn({
                        let generator = self.clone_self();
                        let sig = signature;
                        async move {
                            generator.track_confirmation(sig).await;
                        }
                    });

                    return Ok(signature);
                }
                Err(e) => {
                    last_error = Some(e.to_string());
                    
                    if attempts < MAX_RETRY_ATTEMPTS {
                        sleep(Duration::from_millis(RETRY_DELAY_MS * attempts as u64)).await;
                    }
                }
            }
        }

        let mut metrics = self.metrics.lock().await;
        metrics.transactions_failed += 1;
        
        Err(format!("Failed after {} attempts: {:?}", attempts, last_error).into())
    }

    async fn simulate_transaction(&self, transaction: &Transaction) -> Result<bool, Box<dyn std::error::Error>> {
        let config = RpcSimulateTransactionConfig {
            sig_verify: false,
            replace_recent_blockhash: true,
            commitment: Some(CommitmentConfig::processed()),
            encoding: Some(UiTransactionEncoding::Base64),
            accounts: None,
            min_context_slot: None,
            inner_instructions: false,
        };

        match self.rpc_client.simulate_transaction_with_config(transaction, config).await {
            Ok(result) => Ok(result.value.err.is_none()),
            Err(e) => Err(e.into()),
        }
    }

    fn estimate_transaction_fee(&self, transaction: &Transaction) -> u64 {
        let signatures = transaction.signatures.len() as u64;
        let base_fee = 5000 * signatures;
        
        let priority_fee = transaction.message.instructions.iter()
            .find_map(|ix| {
                if ix.program_id == solana_sdk::compute_budget::id() {
                    if ix.data.len() >= 9 && ix.data[0] == 3 {
                        let fee_bytes = &ix.data[1..9];
                        Some(u64::from_le_bytes(fee_bytes.try_into().unwrap_or([0; 8])))
                    } else {
                        None
                    }
                } else {
                    None
                }
            })
            .unwrap_or(0);

        base_fee + (priority_fee * DECOY_COMPUTE_UNITS as u64 / 1_000_000)
    }

    async fn track_confirmation(&self, signature: Signature) {
        let start_time = SystemTime::now();
        let timeout = Duration::from_secs(30);

        loop {
            match self.rpc_client.get_signature_status(&signature).await {
                Ok(Some(status)) => {
                    let mut metrics = self.metrics.lock().await;
                    if status.satisfies_commitment(CommitmentConfig::confirmed()) {
                        metrics.transactions_confirmed += 1;
                        break;
                    }
                }
                Ok(None) => {
                    if SystemTime::now().duration_since(start_time).unwrap() > timeout {
                        let mut metrics = self.metrics.lock().await;
                        metrics.transactions_failed += 1;
                        break;
                    }
                }
                Err(e) => {
                    log::warn!("Error checking signature status: {:?}", e);
                    if SystemTime::now().duration_since(start_time).unwrap() > timeout {
                        break;
                    }
                }
            }
            
            sleep(Duration::from_millis(500)).await;
        }
    }

    pub async fn fund_decoy_wallets(
        &self,
        funding_keypair: &Keypair,
        amount_per_wallet: u64,
    ) -> Result<Vec<Signature>, Box<dyn std::error::Error>> {
        let wallets = self.decoy_wallets.read().await;
        let mut signatures = Vec::new();
        let recent_blockhash = self.rpc_client.get_latest_blockhash().await?;

        for wallet in wallets.iter() {
            let balance = self.rpc_client.get_balance(&wallet.pubkey()).await?;
            
            if balance < MIN_DECOY_BALANCE {
                let fund_amount = amount_per_wallet.max(MIN_DECOY_BALANCE - balance);
                
                let ix = system_instruction::transfer(
                    &funding_keypair.pubkey(),
                    &wallet.pubkey(),
                    fund_amount,
                );

                let tx = Transaction::new_signed_with_payer(
                    &[ix],
                    Some(&funding_keypair.pubkey()),
                    &[funding_keypair],
                    recent_blockhash,
                );

                match self.rpc_client.send_and_confirm_transaction(&tx).await {
                    Ok(sig) => signatures.push(sig),
                    Err(e) => log::error!("Failed to fund wallet {}: {:?}", wallet.pubkey(), e),
                }
            }
        }

        Ok(signatures)
    }

    pub async fn rebalance_wallets(&self) -> Result<(), Box<dyn std::error::Error>> {
        let wallets = self.decoy_wallets.read().await;
        let mut balances = Vec::new();
        
        for wallet in wallets.iter() {
            let balance = self.rpc_client.get_balance(&wallet.pubkey()).await?;
            balances.push((wallet.pubkey(), balance));
        }

        let total_balance: u64 = balances.iter().map(|(_, b)| b).sum();
        let target_balance = total_balance / wallets.len() as u64;
        let recent_blockhash = self.rpc_client.get_latest_blockhash().await?;

        let mut instructions = Vec::new();
        
        for i in 0..wallets.len() {
            let current_balance = balances[i].1;
            if current_balance > target_balance + MIN_DECOY_AMOUNT {
                let excess = current_balance - target_balance;
                
                for j in 0..wallets.len() {
                    if i != j && balances[j].1 < target_balance {
                        let needed = target_balance - balances[j].1;
                        let transfer_amount = excess.min(needed);
                        
                        if transfer_amount > MIN_DECOY_AMOUNT {
                            instructions.push(system_instruction::transfer(
                                &wallets[i].pubkey(),
                                &wallets[j].pubkey(),
                                transfer_amount,
                            ));
                            
                            balances[i].1 -= transfer_amount;
                            balances[j].1 += transfer_amount;
                            
                            if balances[i].1 <= target_balance + MIN_DECOY_AMOUNT {
                                break;
                            }
                        }
                    }
                }
            }
        }

        if !instructions.is_empty() {
            let chunks: Vec<_> = instructions.chunks(3).collect();
            
            for (idx, chunk) in chunks.iter().enumerate() {
                let signer_idx = idx % wallets.len();
                let tx = Transaction::new_signed_with_payer(
                    chunk,
                    Some(&wallets[signer_idx].pubkey()),
                    &[&wallets[signer_idx]],
                    recent_blockhash,
                );

                if let Err(e) = self.send_transaction_with_retry(tx).await {
                    log::error!("Rebalance transaction failed: {:?}", e);
                }
            }
        }

        Ok(())
    }

    pub async fn get_metrics(&self) -> DecoyMetrics {
        self.metrics.lock().await.clone()
    }

    pub async fn update_config(&self, new_config: DecoyConfig) {
        *self.config.write().await = new_config;
    }

    pub async fn add_decoy_wallet(&self, keypair: Keypair) -> Result<(), Box<dyn std::error::Error>> {
        let mut wallets = self.decoy_wallets.write().await;
        if wallets.len() < MAX_DECOY_WALLETS * 2 {
            wallets.push(keypair);
            Ok(())
        } else {
            Err("Maximum decoy wallet limit reached".into())
        }
    }

    pub async fn remove_inactive_wallets(&self, threshold_hours: u64) -> Result<usize, Box<dyn std::error::Error>> {
        let threshold_time = SystemTime::now()
            .duration_since(UNIX_EPOCH)?
            .as_secs() - (threshold_hours * 3600);
        
        let mut wallets = self.decoy_wallets.write().await;
        let initial_count = wallets.len();
        
        let mut active_wallets = Vec::new();
        for wallet in wallets.iter() {
            match self.get_last_activity_time(&wallet.pubkey()).await {
                Ok(last_activity) => {
                    if last_activity > threshold_time {
                        active_wallets.push(wallet.clone());
                    }
                }
                Err(_) => active_wallets.push(wallet.clone()),
            }
        }
        
        *wallets = active_wallets;
        Ok(initial_count - wallets.len())
    }

    async fn get_last_activity_time(&self, pubkey: &Pubkey) -> Result<u64, Box<dyn std::error::Error>> {
        let signatures = self.rpc_client
            .get_signatures_for_address(pubkey)
            .await?;
        
        if let Some(sig_info) = signatures.first() {
            Ok(sig_info.block_time.unwrap_or(0) as u64)
        } else {
            Ok(0)
        }
    }

    fn clone_self(&self) -> Self {
        Self {
            rpc_client: self.rpc_client.clone(),
            config: self.config.clone(),
            metrics: self.metrics.clone(),
            decoy_wallets: self.decoy_wallets.clone(),
            transaction_queue: self.transaction_queue.clone(),
            rng: self.rng.clone(),
            active: self.active.clone(),
        }
    }

    pub async fn emergency_stop(&self) {
        *self.active.write().await = false;
        let mut queue = self.transaction_queue.lock().await;
        queue.clear();
        log::warn!("Emergency stop activated for decoy transaction generator");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_decoy_generator_initialization() {
        let config = DecoyConfig {
            enabled: true,
            wallets: vec![],
            target_tps: 1.0,
            burst_mode: false,
            burst_multiplier: 1.0,
            randomize_timing: true,
            include_token_ops: false,
            priority_fee_lamports: 1000,
            simulation_required: false,
        };

        let generator = DecoyTransactionGenerator::new(
            "https://api.mainnet-beta.solana.com",
            config,
        ).await;

        assert!(generator.is_ok());
    }
}

