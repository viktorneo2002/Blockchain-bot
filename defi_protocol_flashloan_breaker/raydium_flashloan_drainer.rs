use anchor_client::{
    solana_sdk::{
        commitment_config::CommitmentConfig,
        compute_budget::ComputeBudgetInstruction,
        instruction::{AccountMeta, Instruction},
        pubkey::Pubkey,
        signature::{Keypair, Signer},
        system_instruction,
        transaction::Transaction,
    },
    Client, Cluster,
};
use anchor_lang::prelude::*;
use solana_client::{
    nonblocking::rpc_client::RpcClient,
    rpc_config::{RpcSendTransactionConfig, RpcSimulateTransactionConfig},
};
use solana_sdk::{
    hash::Hash,
    native_token::LAMPORTS_PER_SOL,
    transaction::TransactionError,
};
use solana_transaction_status::UiTransactionEncoding;
use spl_associated_token_account::get_associated_token_address;
use spl_token::instruction as token_instruction;
use std::{
    collections::HashMap,
    str::FromStr,
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::{
    sync::{Mutex, RwLock},
    time::sleep,
};

const RAYDIUM_V4_PROGRAM_ID: &str = "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8";
const RAYDIUM_AUTHORITY_V4: &str = "5Q544fKrFoe6tsEbD7S8EmxGTJYAKtTVhAW5Q5pge4j1";
const TOKEN_PROGRAM_ID: &str = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA";
const WSOL_MINT: &str = "So11111111111111111111111111111111111111112";
const COMPUTE_BUDGET_PRICE: u64 = 1_000_000;
const MAX_COMPUTE_UNITS: u32 = 1_400_000;
const MIN_PROFIT_LAMPORTS: u64 = 10_000_000;
const MAX_SLIPPAGE_BPS: u16 = 100;
const RETRY_ATTEMPTS: u32 = 3;
const PRIORITY_FEE_PERCENTILE: f64 = 0.95;

#[derive(Clone, Debug)]
pub struct RaydiumPool {
    pub address: Pubkey,
    pub coin_mint: Pubkey,
    pub pc_mint: Pubkey,
    pub coin_vault: Pubkey,
    pub pc_vault: Pubkey,
    pub lp_mint: Pubkey,
    pub open_orders: Pubkey,
    pub target_orders: Pubkey,
    pub withdraw_queue: Pubkey,
    pub lp_vault: Pubkey,
    pub market_id: Pubkey,
    pub market_program_id: Pubkey,
    pub coin_reserve: u64,
    pub pc_reserve: u64,
    pub fee_numerator: u64,
    pub fee_denominator: u64,
}

#[derive(Clone)]
pub struct FlashLoanDrainer {
    rpc_client: Arc<RpcClient>,
    keypair: Arc<Keypair>,
    pools: Arc<RwLock<HashMap<Pubkey, RaydiumPool>>>,
    recent_blockhash: Arc<RwLock<Hash>>,
    priority_fee: Arc<RwLock<u64>>,
}

impl FlashLoanDrainer {
    pub fn new(rpc_url: &str, keypair: Keypair) -> Self {
        let rpc_client = Arc::new(RpcClient::new_with_commitment(
            rpc_url.to_string(),
            CommitmentConfig::processed(),
        ));

        Self {
            rpc_client,
            keypair: Arc::new(keypair),
            pools: Arc::new(RwLock::new(HashMap::new())),
            recent_blockhash: Arc::new(RwLock::new(Hash::default())),
            priority_fee: Arc::new(RwLock::new(COMPUTE_BUDGET_PRICE)),
        }
    }

    pub async fn start(&self) -> Result<()> {
        let blockhash_updater = self.clone();
        let pool_updater = self.clone();
        let priority_updater = self.clone();
        let arbitrage_scanner = self.clone();

        tokio::spawn(async move {
            loop {
                if let Err(e) = blockhash_updater.update_blockhash().await {
                    log::error!("Blockhash update error: {:?}", e);
                }
                sleep(Duration::from_millis(400)).await;
            }
        });

        tokio::spawn(async move {
            loop {
                if let Err(e) = pool_updater.update_pools().await {
                    log::error!("Pool update error: {:?}", e);
                }
                sleep(Duration::from_secs(30)).await;
            }
        });

        tokio::spawn(async move {
            loop {
                if let Err(e) = priority_updater.update_priority_fee().await {
                    log::error!("Priority fee update error: {:?}", e);
                }
                sleep(Duration::from_secs(10)).await;
            }
        });

        loop {
            if let Err(e) = arbitrage_scanner.scan_and_execute().await {
                log::error!("Arbitrage scan error: {:?}", e);
            }
            sleep(Duration::from_millis(50)).await;
        }
    }

    async fn update_blockhash(&self) -> Result<()> {
        let blockhash = self.rpc_client.get_latest_blockhash().await?;
        *self.recent_blockhash.write().await = blockhash;
        Ok(())
    }

    async fn update_priority_fee(&self) -> Result<()> {
        let recent_fees = self.rpc_client.get_recent_prioritization_fees(&[]).await?;
        if !recent_fees.is_empty() {
            let mut fees: Vec<u64> = recent_fees.iter().map(|f| f.prioritization_fee).collect();
            fees.sort_unstable();
            let index = ((fees.len() as f64) * PRIORITY_FEE_PERCENTILE) as usize;
            let priority_fee = fees.get(index).copied().unwrap_or(COMPUTE_BUDGET_PRICE);
            *self.priority_fee.write().await = priority_fee.max(COMPUTE_BUDGET_PRICE);
        }
        Ok(())
    }

    async fn update_pools(&self) -> Result<()> {
        let program_id = Pubkey::from_str(RAYDIUM_V4_PROGRAM_ID)?;
        let accounts = self.rpc_client.get_program_accounts(&program_id).await?;
        
        let mut pools = HashMap::new();
        for (pubkey, account) in accounts {
            if account.data.len() >= 752 {
                if let Ok(pool) = self.parse_pool_account(&pubkey, &account.data).await {
                    pools.insert(pubkey, pool);
                }
            }
        }
        
        *self.pools.write().await = pools;
        Ok(())
    }

    async fn parse_pool_account(&self, address: &Pubkey, data: &[u8]) -> Result<RaydiumPool> {
        let status = u64::from_le_bytes(data[0..8].try_into()?);
        if status != 1 {
            return Err(anyhow::anyhow!("Pool not initialized"));
        }

        let coin_vault = Pubkey::new(&data[72..104]);
        let pc_vault = Pubkey::new(&data[104..136]);
        let coin_mint = Pubkey::new(&data[360..392]);
        let pc_mint = Pubkey::new(&data[392..424]);
        let lp_mint = Pubkey::new(&data[424..456]);
        let open_orders = Pubkey::new(&data[456..488]);
        let market_id = Pubkey::new(&data[488..520]);
        let market_program_id = Pubkey::new(&data[520..552]);
        let target_orders = Pubkey::new(&data[552..584]);
        let withdraw_queue = Pubkey::new(&data[584..616]);
        let lp_vault = Pubkey::new(&data[616..648]);
        
        let coin_decimals = data[650];
        let pc_decimals = data[651];
        let coin_reserve = u64::from_le_bytes(data[656..664].try_into()?);
        let pc_reserve = u64::from_le_bytes(data[672..680].try_into()?);
        let fee_numerator = u64::from_le_bytes(data[704..712].try_into()?);
        let fee_denominator = u64::from_le_bytes(data[712..720].try_into()?);

        Ok(RaydiumPool {
            address: *address,
            coin_mint,
            pc_mint,
            coin_vault,
            pc_vault,
            lp_mint,
            open_orders,
            target_orders,
            withdraw_queue,
            lp_vault,
            market_id,
            market_program_id,
            coin_reserve,
            pc_reserve,
            fee_numerator,
            fee_denominator,
        })
    }

    async fn scan_and_execute(&self) -> Result<()> {
        let pools = self.pools.read().await;
        let candidates: Vec<_> = pools
            .values()
            .filter(|p| p.coin_reserve > 0 && p.pc_reserve > 0)
            .filter(|p| p.pc_mint == Pubkey::from_str(WSOL_MINT).unwrap())
            .cloned()
            .collect();

        for pool in candidates {
            if let Some(opportunity) = self.calculate_arbitrage_opportunity(&pool).await {
                if opportunity.profit > MIN_PROFIT_LAMPORTS {
                    match self.execute_flashloan_arbitrage(&pool, opportunity).await {
                        Ok(sig) => {
                            log::info!("Flashloan executed: {} profit: {}", sig, opportunity.profit);
                        }
                        Err(e) => {
                            log::debug!("Execution failed: {:?}", e);
                        }
                    }
                }
            }
        }
        Ok(())
    }

    async fn calculate_arbitrage_opportunity(&self, pool: &RaydiumPool) -> Option<ArbitrageOpportunity> {
        let amount_in = pool.pc_reserve / 10;
        let amount_out = self.calculate_swap_amount(
            amount_in,
            pool.pc_reserve,
            pool.coin_reserve,
            pool.fee_numerator,
            pool.fee_denominator,
        );

        let reverse_amount = self.calculate_swap_amount(
            amount_out,
            pool.coin_reserve - amount_out,
            pool.pc_reserve + amount_in,
            pool.fee_numerator,
            pool.fee_denominator,
        );

        if reverse_amount > amount_in {
            let profit = reverse_amount.saturating_sub(amount_in);
            let gas_estimate = 50_000;
            let net_profit = profit.saturating_sub(gas_estimate);
            
            if net_profit > MIN_PROFIT_LAMPORTS {
                return Some(ArbitrageOpportunity {
                    amount_in,
                    amount_out,
                    profit: net_profit,
                    pool_address: pool.address,
                });
            }
        }
        None
    }

    fn calculate_swap_amount(
        &self,
        amount_in: u64,
        reserve_in: u64,
        reserve_out: u64,
        fee_numerator: u64,
        fee_denominator: u64,
    ) -> u64 {
        if amount_in == 0 || reserve_in == 0 || reserve_out == 0 {
            return 0;
        }

        let amount_in_with_fee = (amount_in as u128)
            .checked_mul(fee_denominator.saturating_sub(fee_numerator) as u128)
            .unwrap_or(0);
        
        let numerator = amount_in_with_fee.checked_mul(reserve_out as u128).unwrap_or(0);
        let denominator = (reserve_in as u128)
            .checked_mul(fee_denominator as u128)
            .unwrap_or(1)
            .checked_add(amount_in_with_fee)
            .unwrap_or(1);

        (numerator / denominator) as u64
    }

    async fn execute_flashloan_arbitrage(
        &self,
        pool: &RaydiumPool,
        opportunity: ArbitrageOpportunity,
    ) -> Result<String> {
        let mut instructions = vec![];
        
        let priority_fee = *self.priority_fee.read().await;
        instructions.push(ComputeBudgetInstruction::set_compute_unit_price(priority_fee));
        instructions.push(ComputeBudgetInstruction::set_compute_unit_limit(MAX_COMPUTE_UNITS));

        let user_coin_account = get_associated_token_address(&self.keypair.pubkey(), &pool.coin_mint);
        let user_pc_account = get_associated_token_address(&self.keypair.pubkey(), &pool.pc_mint);

        instructions.push(self.build_swap_instruction(
            pool,
            opportunity.amount_in,
            opportunity.amount_out,
            SwapDirection::PcToCoin,
            user_pc_account,
            user_coin_account,
        ));

        instructions.push(self.build_swap_instruction(
            pool,
            opportunity.amount_out,
            opportunity.amount_in + opportunity.profit,
            SwapDirection::CoinToPc,
            user_coin_account,
            user_pc_account,
        ));

        let blockhash = *self.recent_blockhash.read().await;
        let transaction = Transaction::new_signed_with_payer(
            &instructions,
            Some(&self.keypair.pubkey()),
            &[&*self.keypair],
            blockhash,
        );

        let result = self.send_transaction_with_retry(&transaction).await?;
        Ok(result)
    }

    async fn send_transaction_with_retry(&self, transaction: &Transaction) -> Result<String> {
        let config = RpcSendTransactionConfig {
            skip_preflight: true,
            preflight_commitment: Some(CommitmentConfig::processed()),
            encoding: Some(UiTransactionEncoding::Base64),
            max_retries: Some(0),
            min_context_slot: None,
        };

        for attempt in 0..RETRY_ATTEMPTS {
            match self.rpc_client.send_transaction_with_config(transaction, config).await {
                Ok(signature) => {
                    return Ok(signature.to_string());
                }
                Err(e) => {
                    if attempt == RETRY_ATTEMPTS - 1 {
                        return Err(anyhow::anyhow!("Transaction failed: {:?}", e));
                    }
                    if self.is_retryable_error(&e) {
                        sleep(Duration::from_millis(50 * (attempt + 1) as u64)).await;
                    } else {
                        return Err(anyhow::anyhow!("Non-retryable error: {:?}", e));
                    }
                }
            }
        }
        Err(anyhow::anyhow!("Max retries exceeded"))
    }

    fn is_retryable_error(&self, error: &solana_client::client_error::ClientError) -> bool {
        use solana_client::client_error::ClientErrorKind;
        matches!(
            error.kind(),
            ClientErrorKind::Io(_) | ClientErrorKind::Reqwest(_) | ClientErrorKind::RpcError(_)
        )
    }

    fn build_swap_instruction(
        &self,
        pool: &RaydiumPool,
        amount_in: u64,
        amount_out: u64,
        direction: SwapDirection,
        user_source: Pubkey,
        user_destination: Pubkey,
    ) -> Instruction {
        let (source_vault, destination_vault) = match direction {
            SwapDirection::PcToCoin => (pool.pc_vault, pool.coin_vault),
            SwapDirection::CoinToPc => (pool.coin_vault, pool.pc_vault),
        };

        let mut data = vec![9]; // Swap instruction discriminator
        data.extend_from_slice(&amount_in.to_le_bytes());
        data.extend_from_slice(&amount_out.to_le_bytes());

        Instruction {
            program_id: Pubkey::from_str(RAYDIUM_V4_PROGRAM_ID).unwrap(),
            accounts: vec![
                AccountMeta::new_readonly(Pubkey::from_str(TOKEN_PROGRAM_ID).unwrap(), false),
                AccountMeta::new(pool.address, false),
                AccountMeta::new_readonly(Pubkey::from_str(RAYDIUM_AUTHORITY_V4).unwrap(), false),
                AccountMeta::new(pool.open_orders, false),
                AccountMeta::new(pool.target_orders, false),
                AccountMeta::new(source_vault, false),
                AccountMeta::new(destination_vault, false),
                AccountMeta::new_readonly(pool.market_id, false),
                AccountMeta::new(user_source, false),
                AccountMeta::new(user_destination, false),
                AccountMeta::new_readonly(self.keypair.pubkey(), true),
                AccountMeta::new_readonly(pool.market_program_id, false),
            ],
            data,
        }
    }

    pub async fn monitor_pool_state(&self, pool_address: &Pubkey) -> Result<()> {
        let account = self.rpc_client.get_account(pool_address).await?;
        if account.data.len() >= 752 {
            let updated_pool = self.parse_pool_account(pool_address, &account.data).await?;
            self.pools.write().await.insert(*pool_address, updated_pool);
        }
        Ok(())
    }

    pub async fn simulate_transaction(&self, transaction: &Transaction) -> Result<bool> {
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
            Err(_) => Ok(false),
        }
    }

    pub async fn get_token_balance(&self, token_account: &Pubkey) -> Result<u64> {
        match self.rpc_client.get_token_account_balance(token_account).await {
            Ok(balance) => Ok(balance.amount.parse::<u64>().unwrap_or(0)),
            Err(_) => Ok(0),
        }
    }

    pub async fn ensure_token_accounts(&self, mint: &Pubkey) -> Result<Pubkey> {
        let ata = get_associated_token_address(&self.keypair.pubkey(), mint);
        
        match self.rpc_client.get_account(&ata).await {
            Ok(_) => Ok(ata),
            Err(_) => {
                let create_ata_ix = spl_associated_token_account::instruction::create_associated_token_account(
                    &self.keypair.pubkey(),
                    &self.keypair.pubkey(),
                    mint,
                    &Pubkey::from_str(TOKEN_PROGRAM_ID).unwrap(),
                );
                
                let blockhash = self.rpc_client.get_latest_blockhash().await?;
                let transaction = Transaction::new_signed_with_payer(
                    &[create_ata_ix],
                    Some(&self.keypair.pubkey()),
                    &[&*self.keypair],
                    blockhash,
                );
                
                self.rpc_client.send_and_confirm_transaction(&transaction).await?;
                Ok(ata)
            }
        }
    }

    pub async fn calculate_optimal_amount(&self, pool: &RaydiumPool) -> u64 {
        let max_impact_bps = 50; // 0.5% max price impact
        let max_amount = (pool.pc_reserve * max_impact_bps) / 10000;
        let min_amount = MIN_PROFIT_LAMPORTS * 10;
        
        let mut optimal = min_amount;
        let mut best_profit = 0u64;
        
        let step = (max_amount - min_amount) / 20;
        for amount in (min_amount..=max_amount).step_by(step as usize) {
            let amount_out = self.calculate_swap_amount(
                amount,
                pool.pc_reserve,
                pool.coin_reserve,
                pool.fee_numerator,
                pool.fee_denominator,
            );
            
            let reverse_amount = self.calculate_swap_amount(
                amount_out,
                pool.coin_reserve - amount_out,
                pool.pc_reserve + amount,
                pool.fee_numerator,
                pool.fee_denominator,
            );
            
            let profit = reverse_amount.saturating_sub(amount);
            if profit > best_profit {
                best_profit = profit;
                optimal = amount;
            }
        }
        
        optimal
    }

    pub async fn execute_with_jito_bundle(&self, instructions: Vec<Instruction>) -> Result<String> {
        let tip_amount = (*self.priority_fee.read().await * 2).min(5_000_000);
        let tip_accounts = [
            "96gYZGLnJYVFmbjzopPSU6QiEV5fGqZNyN9nmNhvrZU5",
            "HFqU5x63VTqvQss8hp11i4wVV8bD44PvwucfZ2bU7gRe",
            "Cw8CFyM9FkoMi7K7Crf6HNQqf4uEMzpKw6QNghXLvLkY",
            "ADaUMid9yfUytqMBgopwjb2DTLSokTSzL1zt6iGPaS49",
            "DfXygSm4jCyNCybVYYK6DwvWqjKee8pbDmJGcLWNDXjh",
            "ADuUkR4vqLUMWXxW9gh6D6L8pMSawimctcNZ5pGwDcEt",
            "DttWaMuVvTiduZRnguLF7jNxTgiMBZ1hyAumKUiL2KRL",
            "3AVi9Tg9Uo68tJfuvoKvqKNWKkC5wPdSSdeBnizKZ6jT",
        ];
        
        let tip_account = Pubkey::from_str(tip_accounts[rand::random::<usize>() % tip_accounts.len()]).unwrap();
        let mut bundle_instructions = instructions;
        bundle_instructions.push(system_instruction::transfer(
            &self.keypair.pubkey(),
            &tip_account,
            tip_amount,
        ));
        
        let blockhash = *self.recent_blockhash.read().await;
        let transaction = Transaction::new_signed_with_payer(
            &bundle_instructions,
            Some(&self.keypair.pubkey()),
            &[&*self.keypair],
            blockhash,
        );
        
        self.send_transaction_with_retry(&transaction).await
    }

    pub async fn health_check(&self) -> bool {
        match self.rpc_client.get_slot().await {
            Ok(slot) => {
                log::debug!("Health check passed, current slot: {}", slot);
                true
            }
            Err(e) => {
                log::error!("Health check failed: {:?}", e);
                false
            }
        }
    }
}

#[derive(Clone, Copy)]
enum SwapDirection {
    PcToCoin,
    CoinToPc,
}

#[derive(Clone)]
struct ArbitrageOpportunity {
    amount_in: u64,
    amount_out: u64,
    profit: u64,
    pool_address: Pubkey,
}

type Result<T> = std::result::Result<T, anyhow::Error>;

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_swap_calculation() {
        let drainer = FlashLoanDrainer::new("https://api.mainnet-beta.solana.com", Keypair::new());
        let amount = drainer.calculate_swap_amount(1000000, 100000000, 100000000, 25, 10000);
        assert!(amount > 0);
        assert!(amount < 1000000);
    }

    #[tokio::test]
    async fn test_profit_calculation() {
        let pool = RaydiumPool {
            address: Pubkey::new_unique(),
            coin_mint: Pubkey::new_unique(),
            pc_mint: Pubkey::from_str(WSOL_MINT).unwrap(),
            coin_vault: Pubkey::new_unique(),
            pc_vault: Pubkey::new_unique(),
            lp_mint: Pubkey::new_unique(),
            open_orders: Pubkey::new_unique(),
            target_orders: Pubkey::new_unique(),
            withdraw_queue: Pubkey::new_unique(),
            lp_vault: Pubkey::new_unique(),
            market_id: Pubkey::new_unique(),
            market_program_id: Pubkey::new_unique(),
            coin_reserve: 1000000000,
            pc_reserve: 1000000000,
            fee_numerator: 25,
            fee_denominator: 10000,
        };

        let drainer = FlashLoanDrainer::new("https://api.mainnet-beta.solana.com", Keypair::new());
        let opportunity = drainer.calculate_arbitrage_opportunity(&pool).await;
        assert!(opportunity.is_none()); // Balanced pool should have no arbitrage
    }
}
