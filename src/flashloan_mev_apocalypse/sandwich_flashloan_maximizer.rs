use anchor_client::solana_sdk::{
    commitment_config::CommitmentConfig,
    compute_budget::ComputeBudgetInstruction,
    instruction::Instruction,
    pubkey::Pubkey,
    signature::{Keypair, Signature},
    signer::Signer,
    transaction::Transaction,
};
use anchor_lang::prelude::*;
use borsh::{BorshDeserialize, BorshSerialize};
use solana_client::{
    nonblocking::rpc_client::RpcClient,
    rpc_config::{RpcSendTransactionConfig, RpcTransactionConfig},
    rpc_response::RpcResult,
};
use solana_sdk::{
    hash::Hash,
    instruction::AccountMeta,
    system_instruction,
    transaction::VersionedTransaction,
};
use solana_transaction_status::{
    EncodedConfirmedTransactionWithStatusMeta, UiTransactionEncoding,
};
use spl_token::instruction as token_instruction;
use std::{
    collections::{HashMap, HashSet},
    str::FromStr,
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::{
    sync::{mpsc, RwLock},
    time::{interval, sleep},
};

const RAYDIUM_V4: &str = "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8";
const ORCA_WHIRLPOOL: &str = "whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc";
const FLASH_LOAN_PROGRAM: &str = "FLASHkPQQPuF9DmJPqfn8tBLbiK9HkBvootEWHsDrj2h";
const JUPITER_V6: &str = "JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4";

#[derive(Clone, Debug)]
pub struct SandwichOpportunity {
    pub target_tx: Signature,
    pub dex_program: Pubkey,
    pub token_a: Pubkey,
    pub token_b: Pubkey,
    pub pool: Pubkey,
    pub victim_amount: u64,
    pub victim_min_out: u64,
    pub is_buy: bool,
    pub expected_profit: u64,
    pub front_run_amount: u64,
    pub priority_fee: u64,
    pub compute_units: u32,
}

#[derive(Clone)]
pub struct PoolState {
    pub reserve_a: u64,
    pub reserve_b: u64,
    pub fee_bps: u16,
    pub last_update: Instant,
}

pub struct SandwichFlashLoanMaximizer {
    rpc_client: Arc<RpcClient>,
    wallet: Arc<Keypair>,
    pool_cache: Arc<RwLock<HashMap<Pubkey, PoolState>>>,
    pending_txs: Arc<RwLock<HashSet<Signature>>>,
    opportunity_sender: mpsc::Sender<SandwichOpportunity>,
    opportunity_receiver: mpsc::Receiver<SandwichOpportunity>,
}

impl SandwichFlashLoanMaximizer {
    pub fn new(rpc_url: &str, wallet: Keypair) -> Self {
        let (tx, rx) = mpsc::channel(1000);
        Self {
            rpc_client: Arc::new(RpcClient::new_with_commitment(
                rpc_url.to_string(),
                CommitmentConfig::confirmed(),
            )),
            wallet: Arc::new(wallet),
            pool_cache: Arc::new(RwLock::new(HashMap::new())),
            pending_txs: Arc::new(RwLock::new(HashSet::new())),
            opportunity_sender: tx,
            opportunity_receiver: rx,
        }
    }

    pub async fn run(&mut self) -> Result<()> {
        let mempool_monitor = self.spawn_mempool_monitor();
        let pool_updater = self.spawn_pool_updater();
        let executor = self.spawn_executor();

        tokio::select! {
            _ = mempool_monitor => {},
            _ = pool_updater => {},
            _ = executor => {},
        }

        Ok(())
    }

    fn spawn_mempool_monitor(&self) -> tokio::task::JoinHandle<()> {
        let rpc = self.rpc_client.clone();
        let sender = self.opportunity_sender.clone();
        let pool_cache = self.pool_cache.clone();
        let pending_txs = self.pending_txs.clone();

        tokio::spawn(async move {
            let mut interval = interval(Duration::from_millis(50));
            loop {
                interval.tick().await;

                if let Ok(signatures) = rpc
                    .get_signatures_for_address(
                        &Pubkey::from_str(RAYDIUM_V4).unwrap(),
                        Some(RpcTransactionConfig {
                            encoding: Some(UiTransactionEncoding::Base64),
                            commitment: Some(CommitmentConfig::processed()),
                            max_supported_transaction_version: Some(0),
                        }),
                    )
                    .await
                {
                    for sig_info in signatures.iter().take(20) {
                        let sig = Signature::from_str(&sig_info.signature).unwrap();
                        
                        let mut pending = pending_txs.write().await;
                        if pending.contains(&sig) {
                            continue;
                        }
                        pending.insert(sig);
                        drop(pending);

                        if let Ok(tx) = rpc.get_transaction(&sig, UiTransactionEncoding::Base64).await {
                            if let Some(opportunity) = analyze_transaction(&tx, &pool_cache).await {
                                let _ = sender.send(opportunity).await;
                            }
                        }
                    }
                }
            }
        })
    }

    fn spawn_pool_updater(&self) -> tokio::task::JoinHandle<()> {
        let rpc = self.rpc_client.clone();
        let pool_cache = self.pool_cache.clone();

        tokio::spawn(async move {
            let mut interval = interval(Duration::from_secs(5));
            loop {
                interval.tick().await;

                let pools = vec![
                    "58oQChx4yWmvKdwLLZzBi4ChoCc2fqCUWBkwMihLYQo2", // SOL/USDC
                    "7vfCXTUXx5WJV5JADk17DUJ4ksgau7utNKj4b963voxs", // ETH/USDC
                    "HZ1JovNiVvGrGNiiYvEozEVgZ58xaU3RKwX8eACQBCt3", // PYTH/USDC
                ];

                for pool_addr in pools {
                    if let Ok(pool_pubkey) = Pubkey::from_str(pool_addr) {
                        if let Ok(account) = rpc.get_account(&pool_pubkey).await {
                            if let Ok(pool_state) = parse_raydium_pool(&account.data) {
                                let mut cache = pool_cache.write().await;
                                cache.insert(pool_pubkey, pool_state);
                            }
                        }
                    }
                }
            }
        })
    }

    fn spawn_executor(&mut self) -> tokio::task::JoinHandle<()> {
        let rpc = self.rpc_client.clone();
        let wallet = self.wallet.clone();
        let mut receiver = self.opportunity_receiver.clone();

        tokio::spawn(async move {
            while let Some(opportunity) = receiver.recv().await {
                if opportunity.expected_profit < 1_000_000 {
                    continue;
                }

                let result = execute_sandwich(
                    &rpc,
                    &wallet,
                    &opportunity,
                ).await;

                match result {
                    Ok(profit) => {
                        println!("Sandwich executed! Profit: {} lamports", profit);
                    }
                    Err(e) => {
                        println!("Sandwich failed: {:?}", e);
                    }
                }
            }
        })
    }
}

async fn analyze_transaction(
    tx: &EncodedConfirmedTransactionWithStatusMeta,
    pool_cache: &Arc<RwLock<HashMap<Pubkey, PoolState>>>,
) -> Option<SandwichOpportunity> {
    let meta = tx.transaction.meta.as_ref()?;
    
    if meta.err.is_some() {
        return None;
    }

    let transaction = match &tx.transaction.transaction {
        solana_transaction_status::EncodedTransaction::Json(t) => return None,
        solana_transaction_status::EncodedTransaction::LegacyBinary(_) => return None,
        solana_transaction_status::EncodedTransaction::Binary(data, _) => {
            let bytes = base64::decode(data).ok()?;
            VersionedTransaction::try_from(bytes.as_slice()).ok()?
        }
        _ => return None,
    };

    let message = transaction.message;
    
    for (idx, instruction) in message.instructions().iter().enumerate() {
        let program_id = message.static_account_keys()[instruction.program_id_index as usize];
        
        if program_id == Pubkey::from_str(RAYDIUM_V4).ok()? {
            if let Some(swap_info) = parse_raydium_swap(&instruction.data) {
                let accounts = &instruction.accounts;
                if accounts.len() < 8 {
                    continue;
                }

                let pool = message.static_account_keys()[accounts[1] as usize];
                let cache = pool_cache.read().await;
                let pool_state = cache.get(&pool)?;

                let (token_a, token_b, is_buy) = if swap_info.amount_in > 0 {
                    (
                        message.static_account_keys()[accounts[4] as usize],
                        message.static_account_keys()[accounts[5] as usize],
                        true,
                    )
                } else {
                    (
                        message.static_account_keys()[accounts[5] as usize],
                        message.static_account_keys()[accounts[4] as usize],
                        false,
                    )
                };

                let impact = calculate_price_impact(
                    swap_info.amount_in,
                    pool_state.reserve_a,
                    pool_state.reserve_b,
                    pool_state.fee_bps,
                );

                if impact < 0.001 {
                    continue;
                }

                let optimal_front_run = calculate_optimal_sandwich_amount(
                    swap_info.amount_in,
                    pool_state.reserve_a,
                    pool_state.reserve_b,
                    pool_state.fee_bps,
                );

                let expected_profit = calculate_sandwich_profit(
                    optimal_front_run,
                    swap_info.amount_in,
                    pool_state.reserve_a,
                    pool_state.reserve_b,
                    pool_state.fee_bps,
                );

                let priority_fee = calculate_priority_fee(expected_profit);

                return Some(SandwichOpportunity {
                    target_tx: tx.transaction.signatures[0].parse().ok()?,
                    dex_program: program_id,
                    token_a,
                    token_b,
                    pool,
                    victim_amount: swap_info.amount_in,
                    victim_min_out: swap_info.minimum_out,
                    is_buy,
                    expected_profit,
                    front_run_amount: optimal_front_run,
                    priority_fee,
                    compute_units: 400_000,
                });
            }
        }
    }

    None
}

#[derive(Debug)]
struct SwapInfo {
    amount_in: u64,
    minimum_out: u64,
}

fn parse_raydium_swap(data: &[u8]) -> Option<SwapInfo> {
    if data.len() < 17 {
        return None;
    }

    if data[0] != 9 {
        return None;
    }

    let amount_in = u64::from_le_bytes(data[1..9].try_into().ok()?);
    let minimum_out = u64::from_le_bytes(data[9..17].try_into().ok()?);

    Some(SwapInfo {
        amount_in,
        minimum_out,
    })
}

fn parse_raydium_pool(data: &[u8]) -> Result<PoolState> {
    if data.len() < 752 {
        return Err(ProgramError::InvalidAccountData.into());
    }

    let reserve_a = u64::from_le_bytes(data[85..93].try_into().unwrap());
    let reserve_b = u64::from_le_bytes(data[101..109].try_into().unwrap());

    Ok(PoolState {
        reserve_a,
        reserve_b,
        fee_bps: 25,
        last_update: Instant::now(),
    })
}

fn calculate_price_impact(amount: u64, reserve_in: u64, reserve_out: u64, fee_bps: u16) -> f64 {
    let amount_with_fee = amount as f64 * (10000.0 - fee_bps as f64) / 10000.0;
    let new_reserve_in = reserve_in as f64 + amount_with_fee;
    let new_reserve_out = (reserve_in as f64 * reserve_out as f64) / new_reserve_in;
    
    let price_before = reserve_out as f64 / reserve_in as f64;
    let price_after = new_reserve_out / new_reserve_in;
    
    (price_before - price_after).abs() / price_before
}

fn calculate_optimal_sandwich_amount(
    victim_amount: u64,
    reserve_a: u64,
    reserve_b: u64,
    fee_bps: u16,
) -> u64 {
    let fee_factor = 1.0 - (fee_bps as f64 / 10000.0);
    let k = (reserve_a as f64) * (reserve_b as f64);
    
    let victim_impact = victim_amount as f64 * fee_factor;
    let optimal = ((k * victim_impact / reserve_a as f64).sqrt() - reserve_a as f64).max(0.0);
    
    (optimal * 0.95) as u64
}

fn calculate_sandwich_profit(
    front_amount: u64,
    victim_amount: u64,
    reserve_a: u64,
    reserve_b: u64,
    fee_bps: u16,
) -> u64 {
    let fee_factor = (10000 - fee_bps) as f64 / 10000.0;
    
    // Front-run swap
    let front_with_fee = (front_amount as f64) * fee_factor;
    let front_out = (reserve_b as f64 * front_with_fee) / (reserve_a as f64 + front_with_fee);
    
    // State after front-run
    let new_reserve_a = reserve_a as f64 + front_with_fee;
    let new_reserve_b = reserve_b as f64 - front_out;
    
    // Victim swap
    let victim_with_fee = (victim_amount as f64) * fee_factor;
    let victim_out = (new_reserve_b * victim_with_fee) / (new_reserve_a + victim_with_fee);
    
    // State after victim
    let final_reserve_a = new_reserve_a + victim_with_fee;
    let final_reserve_b = new_reserve_b - victim_out;
    
    // Back-run swap
    let back_out = (final_reserve_a * front_out * fee_factor) / (final_reserve_b + front_out * fee_factor);
    
    if back_out > front_amount as f64 {
        (back_out - front_amount as f64) as u64
    } else {
        0
    }
}

fn calculate_priority_fee(expected_profit: u64) -> u64 {
    let base_fee = 50_000;
    let competitive_multiplier = 0.15;
    let max_fee = expected_profit / 2;
    
    (base_fee + (expected_profit as f64 * competitive_multiplier) as u64).min(max_fee)
}

async fn execute_sandwich(
    rpc: &RpcClient,
    wallet: &Keypair,
    opportunity: &SandwichOpportunity,
) -> Result<u64> {
    let blockhash = rpc.get_latest_blockhash().await?;
    
    // Get flash loan
    let flash_loan_ix = create_flash_loan_instruction(
        &wallet.pubkey(),
        &opportunity.token_a,
        opportunity.front_run_amount,
    )?;
    
    // Front-run swap
    let front_run_ix = create_swap_instruction(
        &wallet.pubkey(),
        &opportunity.pool,
        &opportunity.token_a,
        &opportunity.token_b,
        opportunity.front_run_amount,
        opportunity.is_buy,
    )?;
    
    // Back-run swap
    let back_run_amount = calculate_back_run_amount(opportunity);
    let back_run_ix = create_swap_instruction(
        &wallet.pubkey(),
        &opportunity.pool,
        &opportunity.token_b,
        &opportunity.token_a,
        back_run_amount,
        !opportunity.is_buy,
    )?;
    
    // Repay flash loan
    let repay_ix = create_flash_loan_repay_instruction(
        &wallet.pubkey(),
        &opportunity.token_a,
        opportunity.front_run_amount,
    )?;
    
    // Build sandwich transaction
    let mut sandwich_tx = Transaction::new_with_payer(
        &[
            ComputeBudgetInstruction::set_compute_unit_limit(opportunity.compute_units),
            ComputeBudgetInstruction::set_compute_unit_price(opportunity.priority_fee),
            flash_loan_ix,
            front_run_ix,
            back_run_ix,
            repay_ix,
        ],
        Some(&wallet.pubkey()),
    );
    
    sandwich_tx.partial_sign(&[wallet], blockhash);
    
    // Send with custom config for MEV protection
    let config = RpcSendTransactionConfig {
        skip_preflight: true,
        preflight_commitment: Some(CommitmentConfig::confirmed()),
        encoding: Some(UiTransactionEncoding::Base64),
        max_retries: Some(0),
        min_context_slot: None,
    };
    
    let start_balance = get_token_balance(rpc, &wallet.pubkey(), &opportunity.token_a).await?;
    
    // Send sandwich transaction
    let sig = rpc.send_transaction_with_config(&sandwich_tx, config).await?;
    
    // Wait for confirmation with timeout
    let timeout = Duration::from_secs(30);
    let start = Instant::now();
    
    loop {
        if start.elapsed() > timeout {
            return Err(ProgramError::Custom(1).into());
        }
        
        match rpc.get_signature_status(&sig).await? {
            Some(status) => {
                if status.is_err() {
                    return Err(ProgramError::Custom(2).into());
                }
                if status.is_ok() {
                    break;
                }
            }
            None => {
                sleep(Duration::from_millis(100)).await;
            }
        }
    }
    
    let end_balance = get_token_balance(rpc, &wallet.pubkey(), &opportunity.token_a).await?;
    
    Ok(end_balance.saturating_sub(start_balance))
}

fn calculate_back_run_amount(opportunity: &SandwichOpportunity) -> u64 {
    // Calculate expected output from front-run
    let fee_factor = 0.9975; // 25 bps fee
    let front_output = opportunity.front_run_amount as f64 * fee_factor;
    
    // Add small buffer for slippage
    (front_output * 0.99) as u64
}

fn create_flash_loan_instruction(
    authority: &Pubkey,
    token_mint: &Pubkey,
    amount: u64,
) -> Result<Instruction> {
    let program_id = Pubkey::from_str(FLASH_LOAN_PROGRAM).unwrap();
    
    let (loan_account, _) = Pubkey::find_program_address(
        &[b"loan", authority.as_ref(), token_mint.as_ref()],
        &program_id,
    );
    
    let (pool_account, _) = Pubkey::find_program_address(
        &[b"pool", token_mint.as_ref()],
        &program_id,
    );
    
    let data = FlashLoanInstruction::Borrow { amount }.try_to_vec()?;
    
    Ok(Instruction {
        program_id,
        accounts: vec![
            AccountMeta::new(*authority, true),
            AccountMeta::new(loan_account, false),
            AccountMeta::new(pool_account, false),
            AccountMeta::new_readonly(*token_mint, false),
            AccountMeta::new_readonly(spl_token::id(), false),
        ],
        data,
    })
}

fn create_flash_loan_repay_instruction(
    authority: &Pubkey,
    token_mint: &Pubkey,
    amount: u64,
) -> Result<Instruction> {
    let program_id = Pubkey::from_str(FLASH_LOAN_PROGRAM).unwrap();
    
    let (loan_account, _) = Pubkey::find_program_address(
        &[b"loan", authority.as_ref(), token_mint.as_ref()],
        &program_id,
    );
    
    let (pool_account, _) = Pubkey::find_program_address(
        &[b"pool", token_mint.as_ref()],
        &program_id,
    );
    
    let fee = (amount as f64 * 0.0009) as u64; // 9 bps flash loan fee
    let total_repay = amount + fee;
    
    let data = FlashLoanInstruction::Repay { amount: total_repay }.try_to_vec()?;
    
    Ok(Instruction {
        program_id,
        accounts: vec![
            AccountMeta::new(*authority, true),
            AccountMeta::new(loan_account, false),
            AccountMeta::new(pool_account, false),
            AccountMeta::new_readonly(*token_mint, false),
            AccountMeta::new_readonly(spl_token::id(), false),
        ],
        data,
    })
}

fn create_swap_instruction(
    authority: &Pubkey,
    pool: &Pubkey,
    token_in: &Pubkey,
    token_out: &Pubkey,
    amount_in: u64,
    is_buy: bool,
) -> Result<Instruction> {
    let program_id = Pubkey::from_str(RAYDIUM_V4).unwrap();
    
    let (authority_pda, _) = Pubkey::find_program_address(
        &[b"amm_authority", pool.as_ref()],
        &program_id,
    );
    
    let user_token_in = get_associated_token_address(authority, token_in);
    let user_token_out = get_associated_token_address(authority, token_out);
    
    let pool_token_in = get_associated_token_address(&authority_pda, token_in);
    let pool_token_out = get_associated_token_address(&authority_pda, token_out);
    
    let minimum_out = (amount_in as f64 * 0.95) as u64; // 5% slippage tolerance
    
    let mut data = vec![9]; // Swap instruction discriminator
    data.extend_from_slice(&amount_in.to_le_bytes());
    data.extend_from_slice(&minimum_out.to_le_bytes());
    
    Ok(Instruction {
        program_id,
        accounts: vec![
            AccountMeta::new_readonly(spl_token::id(), false),
            AccountMeta::new(*pool, false),
            AccountMeta::new_readonly(authority_pda, false),
            AccountMeta::new_readonly(*authority, true),
            AccountMeta::new(user_token_in, false),
            AccountMeta::new(user_token_out, false),
            AccountMeta::new(pool_token_in, false),
            AccountMeta::new(pool_token_out, false),
        ],
        data,
    })
}

async fn get_token_balance(
    rpc: &RpcClient,
    owner: &Pubkey,
    mint: &Pubkey,
) -> Result<u64> {
    let token_account = get_associated_token_address(owner, mint);
    
    match rpc.get_token_account_balance(&token_account).await {
        Ok(balance) => Ok(balance.amount.parse().unwrap_or(0)),
        Err(_) => Ok(0),
    }
}

fn get_associated_token_address(owner: &Pubkey, mint: &Pubkey) -> Pubkey {
    let (address, _) = Pubkey::find_program_address(
        &[
            owner.as_ref(),
            spl_token::id().as_ref(),
            mint.as_ref(),
        ],
        &spl_associated_token_account::id(),
    )
    .0
}

#[derive(BorshSerialize, BorshDeserialize)]
enum FlashLoanInstruction {
    Borrow { amount: u64 },
    Repay { amount: u64 },
}

pub async fn start_sandwich_bot(rpc_url: &str, private_key: &[u8]) -> Result<()> {
    let wallet = Keypair::from_bytes(private_key)?;
    let mut bot = SandwichFlashLoanMaximizer::new(rpc_url, wallet);
    
    println!("Starting Sandwich Flash Loan Maximizer...");
    println!("Monitoring mempool for opportunities...");
    
    bot.run().await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_price_impact_calculation() {
        let impact = calculate_price_impact(1000000, 100000000, 100000000, 25);
        assert!(impact > 0.0 && impact < 0.01);
    }

    #[test]
    fn test_sandwich_profit_calculation() {
        let profit = calculate_sandwich_profit(
            1000000,
            5000000,
            100000000,
            100000000,
            25,
        );
        assert!(profit > 0);
    }

    #[test]
    fn test_optimal_sandwich_amount() {
        let optimal = calculate_optimal_sandwich_amount(
            10000000,
            100000000,
            100000000,
            25,
        );
        assert!(optimal > 0 && optimal < 50000000);
    }
}
    let fee_factor = 1.0 - (fee_bps as f64 / 10000.0);
    let k = (reserve_a as f64) * (reserve_b as f64);
    
    let victim_impact = victim_amount as f64 * fee_factor;
    let optimal = ((k * victim_impact / reserve_a as f64).sqrt() - reserve_a as f64).max(0.0);
    
    (optimal * 0.95) as u64
}

fn calculate_sandwich_profit(
    front_amount: u64,
    victim_amount: u64,
    reserve_a: u64,
    reserve_b: u64,
    fee_bps: u16,
) -> u64 {
    let fee_factor = (10000 - fee_bps) as f64 / 10000.0;
    
    // Front-run swap
    let front_with_fee = (front_amount as f64) * fee_factor;
    let front_out = (reserve_b as f64 * front_with_fee) / (reserve_a as f64 + front_with_fee);
    
    // State after front-run
    let new_reserve_a = reserve_a as f64 + front_with_fee;
    let new_reserve_b = reserve_b as f64 - front_out;
    
    // Victim swap
    let victim_with_fee = (victim_amount as f64) * fee_factor;
    let victim_out = (new_reserve_b * victim_with_fee) / (new_reserve_a + victim_with_fee);
    
    // State after victim
    let final_reserve_a = new_reserve_a + victim_with_fee;
    let final_reserve_b = new_reserve_b - victim_out;
    
    // Back-run swap
    let back_out = (final_reserve_a * front_out * fee_factor) / (final_reserve_b + front_out * fee_factor);
    
    if back_out > front_amount as f64 {
        (back_out - front_amount as f64) as u64
    } else {
        0
    }
}

fn calculate_priority_fee(expected_profit: u64) -> u64 {
    let base_fee = 50_000;
    let competitive_multiplier = 0.15;
    let max_fee = expected_profit / 2;
    
    (base_fee + (expected_profit as f64 * competitive_multiplier) as u64).min(max_fee)
}

async fn execute_sandwich(
    rpc: &RpcClient,
    wallet: &Keypair,
    opportunity: &SandwichOpportunity,
) -> Result<u64> {
    let blockhash = rpc.get_latest_blockhash().await?;
    
    // Get flash loan
    let flash_loan_ix = create_flash_loan_instruction(
        &wallet.pubkey(),
        &opportunity.token_a,
        opportunity.front_run_amount,
    )?;
    
    // Front-run swap
    let front_run_ix = create_swap_instruction(
        &wallet.pubkey(),
        &opportunity.pool,
        &opportunity.token_a,
        &opportunity.token_b,
        opportunity.front_run_amount,
        opportunity.is_buy,
    )?;
    
    // Back-run swap
    let back_run_amount = calculate_back_run_amount(opportunity);
    let back_run_ix = create_swap_instruction(
        &wallet.pubkey(),
        &opportunity.pool,
        &opportunity.token_b,
        &opportunity.token_a,
        back_run_amount,
        !opportunity.is_buy,
    )?;
    
    // Repay flash loan
    let repay_ix = create_flash_loan_repay_instruction(
        &wallet.pubkey(),
        &opportunity.token_a,
        opportunity.front_run_amount,
    )?;
    
    // Build sandwich transaction
    let mut sandwich_tx = Transaction::new_with_payer(
        &[
            ComputeBudgetInstruction::set_compute_unit_limit(opportunity.compute_units),
            ComputeBudgetInstruction::set_compute_unit_price(opportunity.priority_fee),
            flash_loan_ix,
            front_run_ix,
            back_run_ix,
            repay_ix,
        ],
        Some(&wallet.pubkey()),
    );
    
    sandwich_tx.partial_sign(&[wallet], blockhash);
    
    // Send with custom config for MEV protection
    let config = RpcSendTransactionConfig {
        skip_preflight: true,
        preflight_commitment: Some(CommitmentConfig::confirmed()),
        encoding: Some(UiTransactionEncoding::Base64),
        max_retries: Some(0),
        min_context_slot: None,
    };
    
    let start_balance = get_token_balance(rpc, &wallet.pubkey(), &opportunity.token_a).await?;
    
    // Send sandwich transaction
    let sig = rpc.send_transaction_with_config(&sandwich_tx, config).await?;
    
    // Wait for confirmation with timeout
    let timeout = Duration::from_secs(30);
    let start = Instant::now();
    
    loop {
        if start.elapsed() > timeout {
            return Err(ProgramError::Custom(1).into());
        }
        
        match rpc.get_signature_status(&sig).await? {
            Some(status) => {
                if status.is_err() {
                    return Err(ProgramError::Custom(2).into());
                }
                if status.is_ok() {
                    break;
                }
            }
            None => {
                sleep(Duration::from_millis(100)).await;
            }
        }
    }
    
    let end_balance = get_token_balance(rpc, &wallet.pubkey(), &opportunity.token_a).await?;
    
    Ok(end_balance.saturating_sub(start_balance))
}

fn calculate_back_run_amount(opportunity: &SandwichOpportunity) -> u64 {
    // Calculate expected output from front-run
    let fee_factor = 0.9975; // 25 bps fee
    let front_output = opportunity.front_run_amount as f64 * fee_factor;
    
    // Add small buffer for slippage
    (front_output * 0.99) as u64
}

fn create_flash_loan_instruction(
    authority: &Pubkey,
    token_mint: &Pubkey,
    amount: u64,
) -> Result<Instruction> {
    let program_id = Pubkey::from_str(FLASH_LOAN_PROGRAM).unwrap();
    
    let (loan_account, _) = Pubkey::find_program_address(
        &[b"loan", authority.as_ref(), token_mint.as_ref()],
        &program_id,
    );
    
    let (pool_account, _) = Pubkey::find_program_address(
        &[b"pool", token_mint.as_ref()],
        &program_id,
    );
    
    let data = FlashLoanInstruction::Borrow { amount }.try_to_vec()?;
    
    Ok(Instruction {
        program_id,
        accounts: vec![
            AccountMeta::new(*authority, true),
            AccountMeta::new(loan_account, false),
            AccountMeta::new(pool_account, false),
            AccountMeta::new_readonly(*token_mint, false),
            AccountMeta::new_readonly(spl_token::id(), false),
        ],
        data,
    })
}

fn create_flash_loan_repay_instruction(
    authority: &Pubkey,
    token_mint: &Pubkey,
    amount: u64,
) -> Result<Instruction> {
    let program_id = Pubkey::from_str(FLASH_LOAN_PROGRAM).unwrap();
    
    let (loan_account, _) = Pubkey::find_program_address(
        &[b"loan", authority.as_ref(), token_mint.as_ref()],
        &program_id,
    );
    
    let (pool_account, _) = Pubkey::find_program_address(
        &[b"pool", token_mint.as_ref()],
        &program_id,
    );
    
    let fee = (amount as f64 * 0.0009) as u64; // 9 bps flash loan fee
    let total_repay = amount + fee;
    
    let data = FlashLoanInstruction::Repay { amount: total_repay }.try_to_vec()?;
    
    Ok(Instruction {
        program_id,
        accounts: vec![
            AccountMeta::new(*authority, true),
            AccountMeta::new(loan_account, false),
            AccountMeta::new(pool_account, false),
            AccountMeta::new_readonly(*token_mint, false),
            AccountMeta::new_readonly(spl_token::id(), false),
        ],
        data,
    })
}

fn create_swap_instruction(
    authority: &Pubkey,
    pool: &Pubkey,
    token_in: &Pubkey,
    token_out: &Pubkey,
    amount_in: u64,
    is_buy: bool,
) -> Result<Instruction> {
    let program_id = Pubkey::from_str(RAYDIUM_V4).unwrap();
    
    let (authority_pda, _) = Pubkey::find_program_address(
        &[b"amm_authority", pool.as_ref()],
        &program_id,
    );
    
    let user_token_in = get_associated_token_address(authority, token_in);
    let user_token_out = get_associated_token_address(authority, token_out);
    
    let pool_token_in = get_associated_token_address(&authority_pda, token_in);
    let pool_token_out = get_associated_token_address(&authority_pda, token_out);
    
    let minimum_out = (amount_in as f64 * 0.95) as u64; // 5% slippage tolerance
    
    let mut data = vec![9]; // Swap instruction discriminator
    data.extend_from_slice(&amount_in.to_le_bytes());
    data.extend_from_slice(&minimum_out.to_le_bytes());
    
    Ok(Instruction {
        program_id,
        accounts: vec![
            AccountMeta::new_readonly(spl_token::id(), false),
            AccountMeta::new(*pool, false),
            AccountMeta::new_readonly(authority_pda, false),
            AccountMeta::new_readonly(*authority, true),
            AccountMeta::new(user_token_in, false),
            AccountMeta::new(user_token_out, false),
            AccountMeta::new(pool_token_in, false),
            AccountMeta::new(pool_token_out, false),
        ],
        data,
    })
}

async fn get_token_balance(
    rpc: &RpcClient,
    owner: &Pubkey,
    mint: &Pubkey,
) -> Result<u64> {
    let token_account = get_associated_token_address(owner, mint);
    
    match rpc.get_token_account_balance(&token_account).await {
        Ok(balance) => Ok(balance.amount.parse().unwrap_or(0)),
        Err(_) => Ok(0),
    }
}

fn get_associated_token_address(owner: &Pubkey, mint: &Pubkey) -> Pubkey {
    let (address, _) = Pubkey::find_program_address(
        &[
            owner.as_ref(),
            spl_token::id().as_ref(),
            mint.as_ref(),
        ],
        &spl_associated_token_account::id(),
    )
    .0
}

#[derive(BorshSerialize, BorshDeserialize)]
enum FlashLoanInstruction {
    Borrow { amount: u64 },
    Repay { amount: u64 },
}

pub async fn start_sandwich_bot(rpc_url: &str, private_key: &[u8]) -> Result<()> {
    let wallet = Keypair::from_bytes(private_key)?;
    let mut bot = SandwichFlashLoanMaximizer::new(rpc_url, wallet);
    
    println!("Starting Sandwich Flash Loan Maximizer...");
    println!("Monitoring mempool for opportunities...");
    
    bot.run().await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_price_impact_calculation() {
        let impact = calculate_price_impact(1000000, 100000000, 100000000, 25);
        assert!(impact > 0.0 && impact < 0.01);
    }

    #[test]
    fn test_sandwich_profit_calculation() {
        let profit = calculate_sandwich_profit(
            1000000,
            5000000,
            100000000,
            100000000,
            25,
        );
        assert!(profit > 0);
    }

    #[test]
    fn test_optimal_sandwich_amount() {
        let optimal = calculate_optimal_sandwich_amount(
            10000000,
            100000000,
            100000000,
            25,
        );
        assert!(optimal > 0 && optimal < 50000000);
    }
}
