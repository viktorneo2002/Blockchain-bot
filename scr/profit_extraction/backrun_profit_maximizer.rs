use anchor_lang::prelude::*;
use anchor_spl::token::{Token, TokenAccount};
use solana_client::rpc_client::RpcClient;
use solana_sdk::{
    account::Account,
    commitment_config::CommitmentConfig,
    compute_budget::ComputeBudgetInstruction,
    instruction::Instruction,
    pubkey::Pubkey,
    signature::Keypair,
    signer::Signer,
    transaction::Transaction,
};
use std::str::FromStr;
use std::sync::Arc;
use tokio::sync::RwLock;
use rayon::prelude::*;
use borsh::{BorshDeserialize, BorshSerialize};
use bytemuck::{Pod, Zeroable};

const RAYDIUM_V4_PROGRAM: &str = "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8";
const ORCA_WHIRLPOOL_PROGRAM: &str = "whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc";
const WSOL_MINT: &str = "So11111111111111111111111111111111111111112";
const RENT_PROGRAM: &str = "SysvarRent111111111111111111111111111111111";

const MIN_PROFIT_LAMPORTS: u64 = 100_000;
const MAX_SLIPPAGE_BPS: u16 = 500;
const PRIORITY_FEE_LAMPORTS: u64 = 50_000;
const MAX_COMPUTE_UNITS: u32 = 400_000;

#[repr(C)]
#[derive(Clone, Copy, Pod, Zeroable)]
pub struct RaydiumPoolState {
    pub status: u64,
    pub nonce: u64,
    pub max_order: u64,
    pub depth: u64,
    pub base_decimal: u64,
    pub quote_decimal: u64,
    pub state: u64,
    pub reset_flag: u64,
    pub min_size: u64,
    pub vol_max_cut_ratio: u64,
    pub amount_wave_ratio: u64,
    pub base_lot_size: u64,
    pub quote_lot_size: u64,
    pub min_price_multiplier: u64,
    pub max_price_multiplier: u64,
    pub system_decimal_value: u64,
    pub min_separate_numerator: u64,
    pub min_separate_denominator: u64,
    pub trade_fee_numerator: u64,
    pub trade_fee_denominator: u64,
    pub pnl_numerator: u64,
    pub pnl_denominator: u64,
    pub swap_fee_numerator: u64,
    pub swap_fee_denominator: u64,
    pub base_need_take_pnl: u64,
    pub quote_need_take_pnl: u64,
    pub quote_total_pnl: u64,
    pub base_total_pnl: u64,
    pub pool_open_time: u64,
    pub punish_pc_amount: u64,
    pub punish_coin_amount: u64,
    pub ordebook_to_init_time: u64,
    pub swap_base_in_amount: u128,
    pub swap_quote_out_amount: u128,
    pub swap_base2_quote_fee: u64,
    pub swap_quote_in_amount: u128,
    pub swap_base_out_amount: u128,
    pub swap_quote2_base_fee: u64,
    pub base_vault: Pubkey,
    pub quote_vault: Pubkey,
    pub base_mint: Pubkey,
    pub quote_mint: Pubkey,
    pub lp_mint: Pubkey,
    pub open_orders: Pubkey,
    pub market_id: Pubkey,
    pub market_program_id: Pubkey,
    pub target_orders: Pubkey,
    pub withdraw_queue: Pubkey,
    pub lp_vault: Pubkey,
    pub owner: Pubkey,
    pub lp_reserve: u64,
    pub padding: [u64; 3],
}

#[derive(Debug, Clone)]
pub struct BackrunProfitMaximizer {
    rpc_client: Arc<RpcClient>,
    keypair: Arc<Keypair>,
    profit_threshold: Arc<RwLock<u64>>,
    slippage_tolerance: Arc<RwLock<u16>>,
    pool_cache: Arc<RwLock<Vec<PoolInfo>>>,
}

#[derive(Debug, Clone)]
struct PoolInfo {
    address: Pubkey,
    base_mint: Pubkey,
    quote_mint: Pubkey,
    base_reserve: u64,
    quote_reserve: u64,
    fee_numerator: u64,
    fee_denominator: u64,
    last_update: u64,
}

#[derive(Debug, Clone)]
pub struct BackrunTarget {
    pub transaction_id: String,
    pub pool_address: Pubkey,
    pub token_in: Pubkey,
    pub token_out: Pubkey,
    pub amount_in: u64,
    pub minimum_out: u64,
    pub user: Pubkey,
}

#[derive(Debug, Clone)]
pub struct OptimalBackrun {
    pub amount: u64,
    pub expected_profit: u64,
    pub gas_estimate: u64,
    pub execution_price: f64,
    pub instructions: Vec<Instruction>,
}

impl BackrunProfitMaximizer {
    pub fn new(rpc_url: &str, keypair: Keypair) -> Self {
        let rpc_client = RpcClient::new_with_commitment(
            rpc_url.to_string(),
            CommitmentConfig::confirmed(),
        );

        Self {
            rpc_client: Arc::new(rpc_client),
            keypair: Arc::new(keypair),
            profit_threshold: Arc::new(RwLock::new(MIN_PROFIT_LAMPORTS)),
            slippage_tolerance: Arc::new(RwLock::new(MAX_SLIPPAGE_BPS)),
            pool_cache: Arc::new(RwLock::new(Vec::new())),
        }
    }

    pub async fn calculate_optimal_backrun(
        &self,
        target: &BackrunTarget,
    ) -> Result<OptimalBackrun, Box<dyn std::error::Error>> {
        let pool_info = self.fetch_pool_info(&target.pool_address).await?;
        
        let victim_impact = self.calculate_price_impact(
            target.amount_in,
            &pool_info,
            target.token_in == pool_info.base_mint,
        );

        let post_victim_reserves = self.calculate_post_swap_reserves(
            &pool_info,
            target.amount_in,
            target.token_in == pool_info.base_mint,
        );

        let optimal_amount = self.find_optimal_amount(
            &post_victim_reserves,
            victim_impact,
            target.token_in == pool_info.base_mint,
        ).await?;

        let profit = self.calculate_profit(
            optimal_amount,
            &pool_info,
            &post_victim_reserves,
            target.token_in == pool_info.base_mint,
        );

        let gas_estimate = self.estimate_gas_cost().await?;
        
        if profit <= gas_estimate + *self.profit_threshold.read().await {
            return Err("Insufficient profit".into());
        }

        let instructions = self.build_backrun_instructions(
            &target.pool_address,
            optimal_amount,
            target.token_in == pool_info.base_mint,
        ).await?;

        let execution_price = if target.token_in == pool_info.base_mint {
            post_victim_reserves.1 as f64 / post_victim_reserves.0 as f64
        } else {
            post_victim_reserves.0 as f64 / post_victim_reserves.1 as f64
        };

        Ok(OptimalBackrun {
            amount: optimal_amount,
            expected_profit: profit - gas_estimate,
            gas_estimate,
            execution_price,
            instructions,
        })
    }

    async fn fetch_pool_info(&self, pool_address: &Pubkey) -> Result<PoolInfo, Box<dyn std::error::Error>> {
        let mut cache = self.pool_cache.write().await;
        
        if let Some(info) = cache.iter().find(|p| p.address == *pool_address) {
            let current_slot = self.rpc_client.get_slot()?;
            if current_slot - info.last_update < 10 {
                return Ok(info.clone());
            }
        }

        let account = self.rpc_client.get_account(pool_address)?;
        let pool_state = self.parse_raydium_pool(&account.data)?;

        let base_vault_account = self.rpc_client.get_account(&pool_state.base_vault)?;
        let quote_vault_account = self.rpc_client.get_account(&pool_state.quote_vault)?;

        let base_reserve = Self::unpack_token_amount(&base_vault_account.data)?;
        let quote_reserve = Self::unpack_token_amount(&quote_vault_account.data)?;

        let info = PoolInfo {
            address: *pool_address,
            base_mint: pool_state.base_mint,
            quote_mint: pool_state.quote_mint,
            base_reserve,
            quote_reserve,
            fee_numerator: pool_state.trade_fee_numerator,
            fee_denominator: pool_state.trade_fee_denominator,
            last_update: self.rpc_client.get_slot()?,
        };

        cache.retain(|p| p.address != *pool_address);
        cache.push(info.clone());

        Ok(info)
    }

    fn parse_raydium_pool(&self, data: &[u8]) -> Result<RaydiumPoolState, Box<dyn std::error::Error>> {
        if data.len() < std::mem::size_of::<RaydiumPoolState>() {
            return Err("Invalid pool data".into());
        }
        
        let pool_state = bytemuck::from_bytes::<RaydiumPoolState>(
            &data[..std::mem::size_of::<RaydiumPoolState>()]
        );
        
        Ok(*pool_state)
    }

    fn unpack_token_amount(data: &[u8]) -> Result<u64, Box<dyn std::error::Error>> {
        if data.len() < 72 {
            return Err("Invalid token account data".into());
        }
        
        let amount_bytes: [u8; 8] = data[64..72].try_into()?;
        Ok(u64::from_le_bytes(amount_bytes))
    }

    fn calculate_price_impact(
        &self,
        amount_in: u64,
        pool: &PoolInfo,
        is_base_to_quote: bool,
    ) -> f64 {
        let (reserve_in, reserve_out) = if is_base_to_quote {
            (pool.base_reserve, pool.quote_reserve)
        } else {
            (pool.quote_reserve, pool.base_reserve)
        };

        let amount_in_with_fee = amount_in * (pool.fee_denominator - pool.fee_numerator) / pool.fee_denominator;
        let amount_out = (amount_in_with_fee * reserve_out) / (reserve_in + amount_in_with_fee);
        
        let spot_price = reserve_out as f64 / reserve_in as f64;
        let execution_price = amount_out as f64 / amount_in as f64;
        
        (spot_price - execution_price).abs() / spot_price
    }

    fn calculate_post_swap_reserves(
        &self,
        pool: &PoolInfo,
        amount_in: u64,
        is_base_to_quote: bool,
    ) -> (u64, u64) {
        let (reserve_in, reserve_out) = if is_base_to_quote {
            (pool.base_reserve, pool.quote_reserve)
        } else {
            (pool.quote_reserve, pool.base_reserve)
        };

        let amount_in_with_fee = amount_in * (pool.fee_denominator - pool.fee_numerator) / pool.fee_denominator;
        let amount_out = (amount_in_with_fee * reserve_out) / (reserve_in + amount_in_with_fee);

        if is_base_to_quote {
            (reserve_in + amount_in, reserve_out - amount_out)
        } else {
            (reserve_out - amount_out, reserve_in + amount_in)
        }
    }

    async fn find_optimal_amount(
        &self,
        post_victim_reserves: &(u64, u64),
        victim_impact: f64,
        is_base_to_quote: bool,
    ) -> Result<u64, Box<dyn std::error::Error>> {
        let slippage_tolerance = *self.slippage_tolerance.read().await as f64 / 10000.0;
        
        let k = post_victim_reserves.0 as u128 * post_victim_reserves.1 as u128;
        let sqrt_k = (k as f64).sqrt();
        
        let price_ratio = if is_base_to_quote {
            post_victim_reserves.1 as f64 / post_victim_reserves.0 as f64
        } else {
            post_victim_reserves.0 as f64 / post_victim_reserves.1 as f64
        };

        let optimal_ratio = price_ratio * (1.0 + victim_impact * 0.5);
        let optimal_amount = (sqrt_k * optimal_ratio.sqrt() - post_victim_reserves.0 as f64).abs() as u64;

        let max_amount = (post_victim_reserves.0 as f64 * slippage_tolerance) as u64;
        Ok(optimal_amount.min(max_amount))
    }

    fn calculate_profit(
        &self,
        amount: u64,
        original_pool: &PoolInfo,
        post_victim_reserves: &(u64, u64),
        is_base_to_quote: bool,
    ) -> u64 {
        let (reserve_in_pv, reserve_out_pv) = if is_base_to_quote {
            (post_victim_reserves.1, post_victim_reserves.0)
        } else {
            *post_victim_reserves
        };

        let amount_with_fee = amount * (original_pool.fee_denominator - original_pool.fee_numerator) / original_pool.fee_denominator;
        let backrun_out = (amount_with_fee * reserve_out_pv) / (reserve_in_pv + amount_with_fee);
        
        let (new_base, new_quote) = if is_base_to_quote {
            (post_victim_reserves.0 - backrun_out, post_victim_reserves.1 + amount)
        } else {
            (post_victim_reserves.0 + amount, post_victim_reserves.1 - backrun_out)
        };

        let k_invariant = new_base as u128 * new_quote as u128;
        let original_k = original_pool.base_reserve as u128 * original_pool.quote_reserve as u128;
        
        if k_invariant < original_k {
            return 0;
        }

        let arbitrage_amount = if is_base_to_quote {
            let original_price = original_pool.quote_reserve as f64 / original_pool.base_reserve as f64;
            let new_price = new_quote as f64 / new_base as f64;
            ((new_price - original_price) * backrun_out as f64 / new_price) as u64
        } else {
            let original_price = original_pool.base_reserve as f64 / original_pool.quote_reserve as f64;
            let new_price = new_base as f64 / new_quote as f64;
            ((new_price - original_price) * backrun_out as f64 / new_price) as u64
        };

        arbitrage_amount.saturating_sub(amount)
    }

    async fn estimate_gas_cost(&self) -> Result<u64, Box<dyn std::error::Error>> {
        let recent_fees = self.rpc_client.get_recent_prioritization_fees(&[])?;
        
        let percentile_95 = if recent_fees.is_empty() {
            PRIORITY_FEE_LAMPORTS
        } else {
            let idx = (recent_fees.len() as f64 * 0.95) as usize;
            recent_fees.get(idx).map(|f| f.prioritization_fee).unwrap_or(PRIORITY_FEE_LAMPORTS)
        };

        let compute_price = percentile_95 * MAX_COMPUTE_UNITS as u64 / 1_000_000;
        let base_fee = 5000;
        
        Ok(base_fee + compute_price + percentile_95)
    }

    async fn build_backrun_instructions(
        &self,
        pool_address: &Pubkey,
        amount: u64,
        is_base_to_quote: bool,
    ) -> Result<Vec<Instruction>, Box<dyn std::error::Error>> {
        let pool_info = self.fetch_pool_info(pool_address).await?;
        let wallet = self.keypair.pubkey();
        
        let source_token = if is_base_to_quote {
            pool_info.base_mint
        } else {
            pool_info.quote_mint
        };

        let destination_token = if is_base_to_quote {
            pool_info.quote_mint
        } else {
            pool_info.base_mint
        };

        let user_source_ata = spl_associated_token_account::get_associated_token_address(
            &wallet,
            &source_token,
        );

        let user_destination_ata = spl_associated_token_account::get_associated_token_address(
            &wallet,
            &destination_token,
        );

        let mut instructions = vec![];

        instructions.push(
            ComputeBudgetInstruction::set_compute_unit_limit(MAX_COMPUTE_UNITS)
        );

        instructions.push(
            ComputeBudgetInstruction::set_compute_unit_price(
                self.estimate_gas_cost().await? / MAX_COMPUTE_UNITS as u64
            )
        );

        let pool_state = self.rpc_client.get_account_data(pool_address)?;
        let parsed_pool = self.parse_raydium_pool(&pool_state)?;

        let swap_instruction = self.build_raydium_swap_instruction(
            pool_address,
            &parsed_pool,
            &user_source_ata,
            &user_destination_ata,
            amount,
            is_base_to_quote,
        )?;

        instructions.push(swap_instruction);

        Ok(instructions)
    }

    fn build_raydium_swap_instruction(
        &self,
        pool_id: &Pubkey,
        pool_state: &RaydiumPoolState,
        user_source: &Pubkey,
        user_destination: &Pubkey,
        amount_in: u64,
        is_base_to_quote: bool,
    ) -> Result<Instruction, Box<dyn std::error::Error>> {
        let program_id = Pubkey::from_str(RAYDIUM_V4_PROGRAM)?;
        
        let (authority, nonce) = Pubkey::find_program_address(
            &[pool_id.as_ref()],
            &program_id,
        );

        let vault_source = if is_base_to_quote {
            pool_state.base_vault
        } else {
            pool_state.quote_vault
        };

        let vault_destination = if is_base_to_quote {
            pool_state.quote_vault
        } else {
            pool_state.base_vault
        };

        let minimum_out = self.calculate_minimum_out(amount_in, pool_state, is_base_to_quote)?;

        let mut data = vec![9]; // Raydium swap instruction discriminator
        data.extend_from_slice(&amount_in.to_le_bytes());
        data.extend_from_slice(&minimum_out.to_le_bytes());

        let accounts = vec![
            AccountMeta::new_readonly(spl_token::id(), false),
            AccountMeta::new(*pool_id, false),
            AccountMeta::new_readonly(authority, false),
            AccountMeta::new(pool_state.open_orders, false),
            AccountMeta::new(pool_state.target_orders, false),
            AccountMeta::new(vault_source, false),
            AccountMeta::new(vault_destination, false),
            AccountMeta::new_readonly(pool_state.market_program_id, false),
            AccountMeta::new(pool_state.market_id, false),
            AccountMeta::new(*user_source, false),
            AccountMeta::new(*user_destination, false),
            AccountMeta::new_readonly(self.keypair.pubkey(), true),
        ];

        Ok(Instruction {
            program_id,
            accounts,
            data,
        })
    }

    fn calculate_minimum_out(
        &self,
        amount_in: u64,
        pool_state: &RaydiumPoolState,
        is_base_to_quote: bool,
    ) -> Result<u64, Box<dyn std::error::Error>> {
        let (reserve_in, reserve_out) = if is_base_to_quote {
            (pool_state.swap_base_in_amount as u64, pool_state.swap_quote_out_amount as u64)
        } else {
            (pool_state.swap_quote_in_amount as u64, pool_state.swap_base_out_amount as u64)
        };

        if reserve_in == 0 {
            return Err("Invalid reserve state".into());
        }

        let amount_in_with_fee = amount_in
            .checked_mul(pool_state.trade_fee_denominator - pool_state.trade_fee_numerator)
            .ok_or("Overflow in fee calculation")?
            / pool_state.trade_fee_denominator;

        let numerator = amount_in_with_fee
            .checked_mul(reserve_out)
            .ok_or("Overflow in numerator")?;
        
        let denominator = reserve_in
            .checked_add(amount_in_with_fee)
            .ok_or("Overflow in denominator")?;

        let amount_out = numerator / denominator;
        let slippage = MAX_SLIPPAGE_BPS as u64;
        
        Ok(amount_out * (10000 - slippage) / 10000)
    }

    pub async fn execute_backrun(
        &self,
        backrun: &OptimalBackrun,
    ) -> Result<String, Box<dyn std::error::Error>> {
        let recent_blockhash = self.rpc_client.get_latest_blockhash()?;
        
        let mut transaction = Transaction::new_with_payer(
            &backrun.instructions,
            Some(&self.keypair.pubkey()),
        );

        transaction.sign(&[self.keypair.as_ref()], recent_blockhash);

        let config = solana_client::rpc_config::RpcSendTransactionConfig {
            skip_preflight: true,
            preflight_commitment: Some(CommitmentConfig::processed()),
            max_retries: Some(0),
            ..Default::default()
        };

        let signature = self.rpc_client.send_transaction_with_config(&transaction, config)?;
        Ok(signature.to_string())
    }

    pub async fn monitor_and_update(&self, success_rate: f64, avg_profit: u64) {
        let mut threshold = self.profit_threshold.write().await;
        let mut slippage = self.slippage_tolerance.write().await;

        if success_rate < 0.6 {
            *threshold = (*threshold as f64 * 1.2) as u64;
            *slippage = (*slippage as f64 * 0.9) as u16;
        } else if success_rate > 0.8 && avg_profit > MIN_PROFIT_LAMPORTS * 3 {
            *threshold = (*threshold as f64 * 0.95).max(MIN_PROFIT_LAMPORTS as f64) as u64;
            *slippage = ((*slippage as f64 * 1.05).min(MAX_SLIPPAGE_BPS as f64)) as u16;
        }
    }

    pub async fn batch_evaluate_targets(
        &self,
        targets: Vec<BackrunTarget>,
    ) -> Vec<(BackrunTarget, OptimalBackrun)> {
        let results: Vec<_> = targets
            .into_par_iter()
            .filter_map(|target| {
                let runtime = tokio::runtime::Handle::current();
                runtime.block_on(async {
                    match self.calculate_optimal_backrun(&target).await {
                        Ok(backrun) => Some((target, backrun)),
                        Err(_) => None,
                    }
                })
            })
            .collect();

        let mut profitable_results: Vec<_> = results
            .into_iter()
            .filter(|(_, backrun)| backrun.expected_profit > 0)
            .collect();

        profitable_results.sort_by(|a, b| {
            b.1.expected_profit.cmp(&a.1.expected_profit)
        });

        profitable_results
    }

    pub fn validate_pool_liquidity(&self, pool: &PoolInfo) -> bool {
        const MIN_LIQUIDITY_USD: u64 = 50_000_000_000; // $50k in lamports
        
        let total_liquidity = if pool.quote_mint == Pubkey::from_str(WSOL_MINT).unwrap() {
            pool.quote_reserve * 2
        } else {
            pool.base_reserve + pool.quote_reserve
        };

        total_liquidity >= MIN_LIQUIDITY_USD
    }

    pub async fn refresh_pool_cache(&self) -> Result<(), Box<dyn std::error::Error>> {
        let mut cache = self.pool_cache.write().await;
        let current_slot = self.rpc_client.get_slot()?;
        
        cache.retain(|pool| current_slot - pool.last_update < 100);
        
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use solana_sdk::signature::Keypair;

    #[tokio::test]
    async fn test_profit_calculation() {
        let keypair = Keypair::new();
        let maximizer = BackrunProfitMaximizer::new("https://api.mainnet-beta.solana.com", keypair);
        
        let pool_info = PoolInfo {
            address: Pubkey::new_unique(),
            base_mint: Pubkey::new_unique(),
            quote_mint: Pubkey::from_str(WSOL_MINT).unwrap(),
            base_reserve: 1_000_000_000_000,
            quote_reserve: 50_000_000_000,
            fee_numerator: 25,
            fee_denominator: 10000,
            last_update: 0,
        };

        let impact = maximizer.calculate_price_impact(
            100_000_000,
            &pool_info,
            true,
        );

        assert!(impact > 0.0 && impact < 1.0);
    }
}
