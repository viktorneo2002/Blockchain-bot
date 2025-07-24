use spl_token::solana_program::program_pack::Pack;
use solana_sdk::transaction::Message;
use std::str::FromStr;
use std::str::FromStr;
use anchor_lang::prelude::*;
use anchor_lang::solana_program::clock::Clock;
use anchor_spl::token::{Token, TokenAccount};
use arrayref::array_ref;
use bytemuck::{Pod, Zeroable};
use solana_client::rpc_client::RpcClient;
use solana_sdk::{
    commitment_config::CommitmentConfig,
    compute_budget::ComputeBudgetInstruction,
    instruction::Instruction,
    pubkey::Pubkey,
    signature::{Keypair, Signer},
    transaction::Transaction,
};
use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

const VWAP_WINDOW_SIZE: usize = 100;
const MAX_SLIPPAGE_BPS: u64 = 50; // 0.5%
const PRIORITY_FEE_LAMPORTS: u64 = 50000;
const MAX_COMPUTE_UNITS: u32 = 1_400_000;
const ORDER_EXPIRY_SLOTS: u64 = 150;
const DYNAMIC_FEE_MULTIPLIER: f64 = 1.5;
const MIN_PROFIT_BPS: u64 = 10;

#[derive(Debug, Clone, Copy, Pod, Zeroable)]
#[repr(C)]
pub struct VWAPDataPoint {
    pub price: u64,
    pub volume: u64,
    pub timestamp: i64,
    pub slot: u64,
}

#[derive(Debug, Clone)]
pub struct VWAPState {
    pub window: VecDeque<VWAPDataPoint>,
    pub current_vwap: f64,
    pub volume_sum: u64,
    pub price_volume_sum: u128,
    pub last_update_slot: u64,
    pub trend_coefficient: f64,
    pub volatility_score: f64,
}

#[derive(Debug, Clone)]
pub struct OrderParameters {
    pub size: u64,
    pub price_limit: u64,
    pub priority_fee: u64,
    pub expiry_slot: u64,
    pub max_slippage: u64,
    pub execution_type: ExecutionType,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ExecutionType {
    Aggressive,
    Passive,
    Balanced,
}

pub struct VWAPOptimizer {
    pub rpc_client: Arc<RpcClient>,
    pub keypair: Arc<Keypair>,
    pub vwap_state: Arc<Mutex<VWAPState>>,
    pub target_pool: Pubkey,
    pub base_mint: Pubkey,
    pub quote_mint: Pubkey,
    pub performance_metrics: Arc<Mutex<PerformanceMetrics>>,
}

#[derive(Debug, Default)]
pub struct PerformanceMetrics {
    pub total_volume: u64,
    pub successful_trades: u64,
    pub failed_trades: u64,
    pub total_profit: i64,
    pub average_slippage: f64,
    pub best_execution_price: u64,
    pub worst_execution_price: u64,
}

impl VWAPOptimizer {
    pub fn new(
        rpc_url: &str,
        keypair: Keypair,
        target_pool: Pubkey,
        base_mint: Pubkey,
        quote_mint: Pubkey,
    ) -> Self {
        let rpc_client = RpcClient::new_with_commitment(
            rpc_url.to_string(),
            CommitmentConfig::confirmed(),
        );

        Self {
            rpc_client: Arc::new(rpc_client),
            keypair: Arc::new(keypair),
            vwap_state: Arc::new(Mutex::new(VWAPState {
                window: VecDeque::with_capacity(VWAP_WINDOW_SIZE),
                current_vwap: 0.0,
                volume_sum: 0,
                price_volume_sum: 0,
                last_update_slot: 0,
                trend_coefficient: 0.0,
                volatility_score: 0.0,
            })),
            target_pool,
            base_mint,
            quote_mint,
            performance_metrics: Arc::new(Mutex::new(PerformanceMetrics::default())),
        }
    }

    pub fn update_vwap(&self, price: u64, volume: u64, slot: u64) -> Result<f64> {
        let mut state = self.vwap_state.lock().unwrap();
        let clock = Clock::get()?;
        
        let data_point = VWAPDataPoint {
            price,
            volume,
            timestamp: clock.unix_timestamp,
            slot,
        };

        state.window.push_back(data_point);
        state.price_volume_sum += (price as u128) * (volume as u128);
        state.volume_sum += volume;

        while state.window.len() > VWAP_WINDOW_SIZE {
            if let Some(old_point) = state.window.pop_front() {
                let old_contribution = (old_point.price as u128) * (old_point.volume as u128);
                state.price_volume_sum = state.price_volume_sum.saturating_sub(old_contribution);
                state.volume_sum = state.volume_sum.saturating_sub(old_point.volume);
            }
        }

        if state.volume_sum > 0 {
            state.current_vwap = (state.price_volume_sum as f64) / (state.volume_sum as f64);
        }

        state.last_update_slot = slot;
        self.calculate_trend_and_volatility(&mut state);

        Ok(state.current_vwap)
    }

    fn calculate_trend_and_volatility(&self, state: &mut VWAPState) {
        if state.window.len() < 10 {
            return;
        }

        let prices: Vec<f64> = state.window.iter()
            .map(|dp| dp.price as f64)
            .collect();

        let n = prices.len() as f64;
        let sum_x: f64 = (0..prices.len()).map(|i| i as f64).sum();
        let sum_y: f64 = prices.iter().sum();
        let sum_xy: f64 = prices.iter().enumerate()
            .map(|(i, p)| (i as f64) * p)
            .sum();
        let sum_x2: f64 = (0..prices.len()).map(|i| (i as f64).powi(2)).sum();

        state.trend_coefficient = (n * sum_xy - sum_x * sum_y) / (n * sum_x2 - sum_x.powi(2));

        let mean = sum_y / n;
        let variance = prices.iter()
            .map(|p| (p - mean).powi(2))
            .sum::<f64>() / n;
        state.volatility_score = variance.sqrt() / mean;
    }

    pub fn calculate_optimal_order_size(
        &self,
        available_balance: u64,
        market_depth: u64,
    ) -> Result<u64> {
        let state = self.vwap_state.lock().unwrap();
        
        let base_size = available_balance / 10;
        let volatility_adjustment = 1.0 - (state.volatility_score.min(0.5) * 0.8);
        let trend_adjustment = if state.trend_coefficient > 0.0 { 1.1 } else { 0.9 };
        
        let market_impact_factor = (base_size as f64) / (market_depth as f64);
        let impact_adjustment = 1.0 - market_impact_factor.min(0.3);
        
        let optimal_size = (base_size as f64 * volatility_adjustment * trend_adjustment * impact_adjustment) as u64;
        
        Ok(optimal_size.min(available_balance).max(1000))
    }

    pub fn determine_execution_type(&self, urgency_score: f64) -> ExecutionType {
        let state = self.vwap_state.lock().unwrap();
        
        if urgency_score > 0.8 || state.trend_coefficient > 0.05 {
            ExecutionType::Aggressive
        } else if urgency_score < 0.3 || state.volatility_score > 0.1 {
            ExecutionType::Passive
        } else {
            ExecutionType::Balanced
        }
    }

    pub fn calculate_price_limit(
        &self,
        current_price: u64,
        execution_type: ExecutionType,
        is_buy: bool,
    ) -> u64 {
        let state = self.vwap_state.lock().unwrap();
        let vwap = state.current_vwap;
        
        let base_adjustment = match execution_type {
            ExecutionType::Aggressive => 1.005,
            ExecutionType::Balanced => 1.002,
            ExecutionType::Passive => 0.999,
        };

        let trend_adjustment = 1.0 + (state.trend_coefficient * 0.001);
        let volatility_buffer = 1.0 + (state.volatility_score * 0.01);
        
        let total_adjustment = base_adjustment * trend_adjustment * volatility_buffer;
        
        let limit_price = if is_buy {
            (vwap * total_adjustment).min(current_price as f64 * 1.01)
        } else {
            (vwap / total_adjustment).max(current_price as f64 * 0.99)
        };
        
        limit_price as u64
    }

    pub fn calculate_dynamic_priority_fee(&self, network_congestion: f64) -> u64 {
        let base_fee = PRIORITY_FEE_LAMPORTS;
        let metrics = self.performance_metrics.lock().unwrap();
        
        let success_rate = if metrics.successful_trades + metrics.failed_trades > 0 {
            metrics.successful_trades as f64 / (metrics.successful_trades + metrics.failed_trades) as f64
        } else {
            0.5
        };
        
        let fee_multiplier = if success_rate < 0.7 {
            DYNAMIC_FEE_MULTIPLIER * (1.0 + network_congestion)
        } else {
            1.0 + (network_congestion * 0.5)
        };
        
        (base_fee as f64 * fee_multiplier) as u64
    }

    pub async fn execute_vwap_order(
        &self,
        amount: u64,
        is_buy: bool,
        urgency_score: f64,
    ) -> Result<ExecutionResult> {
        let current_slot = self.rpc_client.get_slot()?;
        let current_price = self.fetch_current_price().await?;
        let market_depth = self.fetch_market_depth().await?;
        
        self.update_vwap(current_price, amount, current_slot)?;
        
        let order_size = self.calculate_optimal_order_size(amount, market_depth)?;
        let execution_type = self.determine_execution_type(urgency_score);
        let price_limit = self.calculate_price_limit(current_price, execution_type, is_buy);
        let priority_fee = self.calculate_dynamic_priority_fee(0.5);
        
        let order_params = OrderParameters {
            size: order_size,
            price_limit,
            priority_fee,
            expiry_slot: current_slot + ORDER_EXPIRY_SLOTS,
            max_slippage: MAX_SLIPPAGE_BPS,
            execution_type,
        };
        
        let result = self.submit_optimized_order(order_params, is_buy).await?;
        self.update_performance_metrics(&result);
        
        Ok(result)
    }

    async fn submit_optimized_order(
        &self,
        params: OrderParameters,
        is_buy: bool,
    ) -> Result<ExecutionResult> {
        let instructions = self.build_swap_instructions(params, is_buy).await?;
        
        let compute_budget_ix = ComputeBudgetInstruction::set_compute_unit_limit(MAX_COMPUTE_UNITS);
        let priority_fee_ix = ComputeBudgetInstruction::set_compute_unit_price(params.priority_fee);
        
        let mut transaction = Transaction::new_with_payer(
            &[compute_budget_ix, priority_fee_ix],
            Some(&self.keypair.pubkey()),
        );
        
        transaction.instructions.extend(instructions);
        
        let recent_blockhash = self.rpc_client.get_latest_blockhash()?;
        transaction.sign(&[&*self.keypair], recent_blockhash);
        
        let start_time = Instant::now();
        let signature = self.rpc_client.send_and_confirm_transaction(&transaction)?;
        let execution_time = start_time.elapsed();
        
        let final_price = self.fetch_execution_price(&signature).await?;
        let slippage = self.calculate_slippage(params.price_limit, final_price);
        
        Ok(ExecutionResult {
            signature,
            executed_amount: params.size,
            execution_price: final_price,
            slippage_bps: slippage,
            execution_time,
            success: true,
        })
    }

    async fn build_swap_instructions(
        &self,
        params: OrderParameters,
        is_buy: bool,
    ) -> Result<Vec<Instruction>> {
        let user_base_account = self.get_or_create_token_account(self.base_mint).await?;
        let user_quote_account = self.get_or_create_token_account(self.quote_mint).await?;
        
        let pool_info = self.fetch_pool_info().await?;
        let (amount_in, minimum_out) = self.calculate_swap_amounts(
            params.size,
            params.price_limit,
            params.max_slippage,
            is_buy,
            &pool_info,
        )?;
        
        let swap_ix = self.create_swap_instruction(
            user_base_account,
            user_quote_account,
            amount_in,
            minimum_out,
            is_buy,
        )?;
        
        Ok(vec![swap_ix])
    }

    fn create_swap_instruction(
        &self,
        user_base_account: Pubkey,
        user_quote_account: Pubkey,
        amount_in: u64,
        minimum_out: u64,
        is_buy: bool,
    ) -> Result<Instruction> {
        let pool_keys = self.get_pool_keys()?;
        
        let (source_account, destination_account) = if is_buy {
            (user_quote_account, user_base_account)
        } else {
            (user_base_account, user_quote_account)
        };

        let accounts = vec![
            AccountMeta::new_readonly(pool_keys.amm_id, false),
            AccountMeta::new_readonly(pool_keys.amm_authority, false),
            AccountMeta::new(pool_keys.amm_open_orders, false),
            AccountMeta::new(pool_keys.amm_target_orders, false),
            AccountMeta::new(pool_keys.pool_coin_token_account, false),
            AccountMeta::new(pool_keys.pool_pc_token_account, false),
            AccountMeta::new_readonly(pool_keys.serum_program_id, false),
            AccountMeta::new(pool_keys.serum_market, false),
            AccountMeta::new(pool_keys.serum_bids, false),
            AccountMeta::new(pool_keys.serum_asks, false),
            AccountMeta::new(pool_keys.serum_event_queue, false),
            AccountMeta::new(pool_keys.serum_coin_vault_account, false),
            AccountMeta::new(pool_keys.serum_pc_vault_account, false),
            AccountMeta::new_readonly(pool_keys.serum_vault_signer, false),
            AccountMeta::new(source_account, false),
            AccountMeta::new(destination_account, false),
            AccountMeta::new_readonly(self.keypair.pubkey(), true),
            AccountMeta::new_readonly(spl_token::id(), false),
        ];

        let data = self.encode_swap_instruction_data(amount_in, minimum_out)?;
        
        Ok(Instruction {
            program_id: pool_keys.amm_program_id,
            accounts,
            data,
        })
    }

    fn encode_swap_instruction_data(&self, amount_in: u64, minimum_out: u64) -> Result<Vec<u8>> {
        let mut data = vec![9]; // Swap instruction discriminator
        data.extend_from_slice(&amount_in.to_le_bytes());
        data.extend_from_slice(&minimum_out.to_le_bytes());
        Ok(data)
    }

    fn calculate_swap_amounts(
        &self,
        size: u64,
        price_limit: u64,
        max_slippage: u64,
        is_buy: bool,
        pool_info: &PoolInfo,
    ) -> Result<(u64, u64)> {
        let state = self.vwap_state.lock().unwrap();
        let vwap = state.current_vwap;
        
        let price_impact = self.estimate_price_impact(size, pool_info);
        let adjusted_price = if is_buy {
            vwap * (1.0 + price_impact)
        } else {
            vwap * (1.0 - price_impact)
        };
        
        if (adjusted_price as u64 > price_limit && is_buy)
            || (adjusted_price as u64 < price_limit && !is_buy) 
        {
            return Err(ProgramError::Custom(1)); // Price exceeds limit
        }

        let slippage_factor = 1.0 - (max_slippage as f64 / 10000.0);
        
        let (amount_in, minimum_out) = if is_buy {
            let quote_amount = (size as f64 * adjusted_price) as u64;
            let min_base_out = (size as f64 * slippage_factor) as u64;
            (quote_amount, min_base_out)
        } else {
            let base_amount = size;
            let min_quote_out = (size as f64 * adjusted_price * slippage_factor) as u64;
            (base_amount, min_quote_out)
        };
        
        Ok((amount_in, minimum_out))
    }

    fn estimate_price_impact(&self, size: u64, pool_info: &PoolInfo) -> f64 {
        let base_liquidity = pool_info.base_reserve;
        let quote_liquidity = pool_info.quote_reserve;
        let current_price = quote_liquidity as f64 / base_liquidity as f64;
        
        let size_ratio = size as f64 / base_liquidity as f64;
        let k = base_liquidity as f64 * quote_liquidity as f64;
        
        let new_base = base_liquidity as f64 + size as f64;
        let new_quote = k / new_base;
        let new_price = new_quote / new_base;
        
        (new_price - current_price).abs() / current_price
    }

    async fn fetch_pool_info(&self) -> Result<PoolInfo> {
        let account_data = self.rpc_client.get_account_data(&self.target_pool)?;
        
        if account_data.len() < 752 {
            return Err(ProgramError::InvalidAccountData.into());
        }
        
        let status = u64::from_le_bytes(*array_ref![account_data, 0, 8]);
        let nonce = u8::from_le_bytes(*array_ref![account_data, 8, 1]);
        let max_order = u64::from_le_bytes(*array_ref![account_data, 9, 8]);
        let depth = u64::from_le_bytes(*array_ref![account_data, 17, 8]);
        let base_decimal = u64::from_le_bytes(*array_ref![account_data, 25, 8]);
        let quote_decimal = u64::from_le_bytes(*array_ref![account_data, 33, 8]);
        let state = u64::from_le_bytes(*array_ref![account_data, 41, 8]);
        let reset_flag = u64::from_le_bytes(*array_ref![account_data, 49, 8]);
        let min_size = u64::from_le_bytes(*array_ref![account_data, 57, 8]);
        let vol_max_cut_ratio = u64::from_le_bytes(*array_ref![account_data, 65, 8]);
        let amount_wave = u64::from_le_bytes(*array_ref![account_data, 73, 8]);
        let base_lot_size = u64::from_le_bytes(*array_ref![account_data, 81, 8]);
        let quote_lot_size = u64::from_le_bytes(*array_ref![account_data, 89, 8]);
        let min_price_multiplier = u64::from_le_bytes(*array_ref![account_data, 97, 8]);
        let max_price_multiplier = u64::from_le_bytes(*array_ref![account_data, 105, 8]);
        let system_decimal_value = u64::from_le_bytes(*array_ref![account_data, 113, 8]);
        let min_separate_numerator = u64::from_le_bytes(*array_ref![account_data, 121, 8]);
        let min_separate_denominator = u64::from_le_bytes(*array_ref![account_data, 129, 8]);
        let trade_fee_numerator = u64::from_le_bytes(*array_ref![account_data, 137, 8]);
        let trade_fee_denominator = u64::from_le_bytes(*array_ref![account_data, 145, 8]);
        let pnl_numerator = u64::from_le_bytes(*array_ref![account_data, 153, 8]);
        let pnl_denominator = u64::from_le_bytes(*array_ref![account_data, 161, 8]);
        let swap_fee_numerator = u64::from_le_bytes(*array_ref![account_data, 169, 8]);
        let swap_fee_denominator = u64::from_le_bytes(*array_ref![account_data, 177, 8]);
        let base_need_take_pnl = u64::from_le_bytes(*array_ref![account_data, 185, 8]);
        let quote_need_take_pnl = u64::from_le_bytes(*array_ref![account_data, 193, 8]);
        let quote_total_pnl = u64::from_le_bytes(*array_ref![account_data, 201, 8]);
        let base_total_pnl = u64::from_le_bytes(*array_ref![account_data, 209, 8]);
        let quote_total_deposited = u64::from_le_bytes(*array_ref![account_data, 217, 8]);
        let base_total_deposited = u64::from_le_bytes(*array_ref![account_data, 225, 8]);
        let swap_base_in_amount = u128::from_le_bytes(*array_ref![account_data, 233, 16]);
        let swap_quote_out_amount = u128::from_le_bytes(*array_ref![account_data, 249, 16]);
        let swap_base2_quote_fee = u64::from_le_bytes(*array_ref![account_data, 265, 8]);
        let swap_quote_in_amount = u128::from_le_bytes(*array_ref![account_data, 273, 16]);
        let swap_base_out_amount = u128::from_le_bytes(*array_ref![account_data, 289, 16]);
        let swap_quote2_base_fee = u64::from_le_bytes(*array_ref![account_data, 305, 8]);
        let base_vault = Pubkey::new_from_array(*array_ref![account_data, 313, 32]);
        let quote_vault = Pubkey::new_from_array(*array_ref![account_data, 345, 32]);
        let base_mint = Pubkey::new_from_array(*array_ref![account_data, 377, 32]);
        let quote_mint = Pubkey::new_from_array(*array_ref![account_data, 409, 32]);
        let lp_mint = Pubkey::new_from_array(*array_ref![account_data, 441, 32]);
        let open_orders = Pubkey::new_from_array(*array_ref![account_data, 473, 32]);
        let market_id = Pubkey::new_from_array(*array_ref![account_data, 505, 32]);
        let market_program_id = Pubkey::new_from_array(*array_ref![account_data, 537, 32]);
        let target_orders = Pubkey::new_from_array(*array_ref![account_data, 569, 32]);
        let withdraw_queue = Pubkey::new_from_array(*array_ref![account_data, 601, 32]);
        let lp_vault = Pubkey::new_from_array(*array_ref![account_data, 633, 32]);
        let amm_owner = Pubkey::new_from_array(*array_ref![account_data, 665, 32]);
        let lp_reserve = u64::from_le_bytes(*array_ref![account_data, 697, 8]);
        
        let base_vault_account = self.rpc_client.get_account(&base_vault)?;
        let quote_vault_account = self.rpc_client.get_account(&quote_vault)?;
        
        let base_reserve = u64::from_le_bytes(*array_ref![base_vault_account.data, 64, 8]);
        let quote_reserve = u64::from_le_bytes(*array_ref![quote_vault_account.data, 64, 8]);
        
        Ok(PoolInfo {
            base_reserve,
            quote_reserve,
            base_decimal: 10u64.pow(base_decimal as u32),
            quote_decimal: 10u64.pow(quote_decimal as u32),
            fee_numerator: trade_fee_numerator,
            fee_denominator: trade_fee_denominator,
            open_orders,
            target_orders,
            base_vault,
            quote_vault,
            market_id,
            market_program_id,
        })
    }

    async fn fetch_current_price(&self) -> Result<u64> {
        let pool_info = self.fetch_pool_info().await?;
        let price = (pool_info.quote_reserve as f64 / pool_info.base_reserve as f64) 
            * (pool_info.base_decimal as f64 / pool_info.quote_decimal as f64);
        Ok((price * 1e9) as u64) // Price in lamports precision
    }

    async fn fetch_market_depth(&self) -> Result<u64> {
        let pool_info = self.fetch_pool_info().await?;
        let market_account = self.rpc_client.get_account(&pool_info.market_id)?;
        
        let bids_address = Pubkey::new_from_array(*array_ref![market_account.data, 65, 32]);
        let asks_address = Pubkey::new_from_array(*array_ref![market_account.data, 97, 32]);
        
        let bids_data = self.rpc_client.get_account_data(&bids_address)?;
        let asks_data = self.rpc_client.get_account_data(&asks_address)?;
        
        let bid_depth = self.parse_orderbook_depth(&bids_data, true);
        let ask_depth = self.parse_orderbook_depth(&asks_data, false);
        
        Ok(bid_depth.min(ask_depth))
    }

    fn parse_orderbook_depth(&self, data: &[u8], is_bids: bool) -> u64 {
        if data.len() < 16 {
            return 0;
        }
        
        let mut total_depth = 0u64;
        let mut offset = 13; // Skip header
        
        while offset + 80 <= data.len() {
            let price_raw = u64::from_le_bytes(*array_ref![data, offset + 32, 8]);
            let size_raw = u64::from_le_bytes(*array_ref![data, offset + 40, 8]);
            let owner_slot = u8::from_le_bytes(*array_ref![data, offset + 68, 1]);
            
            if owner_slot != 0 && price_raw > 0 && size_raw > 0 {
                total_depth = total_depth.saturating_add(size_raw);
            }
            
            offset += 80;
        }
        
        total_depth
    }

    async fn get_or_create_token_account(&self, mint: Pubkey) -> Result<Pubkey> {
        let associated_token_address = spl_associated_token_account::get_associated_token_address(
            &self.keypair.pubkey(),
            &mint,
        );
        
        match self.rpc_client.get_account(&associated_token_address) {
            Ok(_) => Ok(associated_token_address),
            Err(_) => {
                let create_ix = spl_associated_token_account::instruction::create_associated_token_account(
                    &self.keypair.pubkey(),
                    &self.keypair.pubkey(),
                    &mint,
                    &spl_token::id(),
                );
                
                let recent_blockhash = self.rpc_client.get_latest_blockhash()?;
                let transaction = Transaction::new_signed_with_payer(
                    &[create_ix],
                    Some(&self.keypair.pubkey()),
                    &[&*self.keypair],
                    recent_blockhash,
                );
                
                self.rpc_client.send_and_confirm_transaction(&transaction)?;
                Ok(associated_token_address)
            }
        }
    }

    async fn fetch_execution_price(&self, signature: &solana_sdk::signature::Signature) -> Result<u64> {
        let transaction = self.rpc_client.get_transaction(
            signature,
            solana_transaction_status::UiTransactionEncoding::Base64,
        )?;
        let pre_balances = meta.pre_token_balances.as_ref().map(|v| v.as_ref()).unwrap_or(&vec![]);
        let post_balances = meta.post_token_balances.as_ref().map(|v| v.as_ref()).unwrap_or(&vec![]);
            let pre_balances = meta.pre_token_balances.unwrap_or_default();
            let post_balances = meta.post_token_balances.unwrap_or_default();
            
            let mut base_change = 0i64;
            let mut quote_change = 0i64;
            
            for (pre, post) in pre_balances.iter().zip(post_balances.iter()) {
                if pre.mint == self.base_mint.to_string() {
                    let pre_amount = pre.ui_token_amount.amount.parse::<i64>().unwrap_or(0);
                    let post_amount = post.ui_token_amount.amount.parse::<i64>().unwrap_or(0);
                    base_change = post_amount - pre_amount;
                } else if pre.mint == self.quote_mint.to_string() {
                    let pre_amount = pre.ui_token_amount.amount.parse::<i64>().unwrap_or(0);
                    let post_amount = post.ui_token_amount.amount.parse::<i64>().unwrap_or(0);
                    quote_change = post_amount - pre_amount;
                }
            }
            
            if base_change != 0 {
                let price = (quote_change.abs() as f64 / base_change.abs() as f64) * 1e9;
                return Ok(price as u64);
            }
        }
        
        self.fetch_current_price().await
    }

    fn calculate_slippage(&self, expected_price: u64, actual_price: u64) -> u64 {
        let diff = if expected_price > actual_price {
            expected_price - actual_price
        } else {
            actual_price - expected_price
        };
        
        (diff * 10000) / expected_price
    }

    fn update_performance_metrics(&self, result: &ExecutionResult) {
        let mut metrics = self.performance_metrics.lock().unwrap();
        
        if result.success {
            metrics.successful_trades += 1;
            metrics.total_volume += result.executed_amount;
            
            let slippage_weight = result.executed_amount as f64 / metrics.total_volume as f64;
            metrics.average_slippage = metrics.average_slippage * (1.0 - slippage_weight) 
                + (result.slippage_bps as f64 * slippage_weight);
            
            if metrics.best_execution_price == 0 || result.execution_price < metrics.best_execution_price {
                metrics.best_execution_price = result.execution_price;
            }
            if result.execution_price > metrics.worst_execution_price {
                metrics.worst_execution_price = result.execution_price;
            }
            
            let vwap_state = self.vwap_state.lock().unwrap();
            let profit = ((result.execution_price as i64 - vwap_state.current_vwap as i64) 
                * result.executed_amount as i64) / 1e9 as i64;
            metrics.total_profit += profit;
        } else {
            metrics.failed_trades += 1;
        }
    }

    fn get_pool_keys(&self) -> Result<PoolKeys> {
        // Raydium V4 AMM program
        let amm_program_id = Pubkey::from_str("675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8").unwrap();
        let serum_program_id = Pubkey::from_str("9xQeWvG816bUx9EPjHmaT23yvVM2ZWbrrpZb9PusVFin").unwrap();
        
        let (amm_authority, _nonce) = Pubkey::find_program_address(
            &[
                &self.target_pool.to_bytes(),
            ],
            &amm_program_id,
        );
        
        let pool_info = futures::executor::block_on(self.fetch_pool_info())?;
        
        Ok(PoolKeys {
            amm_id: self.target_pool,
            amm_authority,
            amm_open_orders: pool_info.open_orders,
            amm_target_orders: pool_info.target_orders,
            pool_coin_token_account: pool_info.base_vault,
            pool_pc_token_account: pool_info.quote_vault,
            serum_program_id,
            serum_market: pool_info.market_id,
            serum_bids: self.derive_serum_bids(&pool_info.market_id, &serum_program_id),
            serum_asks: self.derive_serum_asks(&pool_info.market_id, &serum_program_id),
            serum_event_queue: self.derive_serum_event_queue(&pool_info.market_id, &serum_program_id),
            serum_coin_vault_account: self.derive_serum_coin_vault(&pool_info.market_id, &serum_program_id),
            serum_pc_vault_account: self.derive_serum_pc_vault(&pool_info.market_id, &serum_program_id),
            serum_vault_signer: self.derive_serum_vault_signer(&pool_info.market_id, &serum_program_id),
            amm_program_id,
        })
    }

    fn derive_serum_bids(&self, market: &Pubkey, program: &Pubkey) -> Pubkey {
        let market_data = self.rpc_client.get_account_data(market).unwrap();
        Pubkey::new_from_array(*array_ref![market_data, 65, 32])
    }

    fn derive_serum_asks(&self, market: &Pubkey, program: &Pubkey) -> Pubkey {
        let market_data = self.rpc_client.get_account_data(market).unwrap();
        Pubkey::new_from_array(*array_ref![market_data, 97, 32])
    }

    fn derive_serum_event_queue(&self, market: &Pubkey, program: &Pubkey) -> Pubkey {
        let market_data = self.rpc_client.get_account_data(market).unwrap();
        Pubkey::new_from_array(*array_ref![market_data, 129, 32])
    }

    fn derive_serum_coin_vault(&self, market: &Pubkey, program: &Pubkey) -> Pubkey {
        let market_data = self.rpc_client.get_account_data(market).unwrap();
        Pubkey::new_from_array(*array_ref![market_data, 161, 32])
    }

    fn derive_serum_pc_vault(&self, market: &Pubkey, program: &Pubkey) -> Pubkey {
        let market_data = self.rpc_client.get_account_data(market).unwrap();
        Pubkey::new_from_array(*array_ref![market_data, 193, 32])
    }

    fn derive_serum_vault_signer(&self, market: &Pubkey, program: &Pubkey) -> Pubkey {
        let market_data = self.rpc_client.get_account_data(market).unwrap();
        let vault_signer_nonce = u64::from_le_bytes(*array_ref![market_data, 45, 8]);
        
        let seeds = &[market.as_ref(), &vault_signer_nonce.to_le_bytes()];
        let (vault_signer, _) = Pubkey::find_program_address(seeds, program);
        vault_signer
    }
        *self.performance_metrics.lock().unwrap()
        *self.performance_metrics.lock().unwrap()
        self.performance_metrics.lock().unwrap().clone()

    pub fn should_execute_trade(&self, opportunity_score: f64) -> bool {
        let state = self.vwap_state.lock().unwrap();
        let metrics = self.performance_metrics.lock().unwrap();
        
        let success_rate = if metrics.successful_trades + metrics.failed_trades > 0 {
            metrics.successful_trades as f64 / (metrics.successful_trades + metrics.failed_trades) as f64
        } else {
            0.5
        };
        
        let volatility_threshold = 0.15;
        let min_opportunity_score = 0.65;
        let min_success_rate = 0.6;
        
        state.volatility_score < volatility_threshold && 
        opportunity_score > min_opportunity_score &&
        success_rate > min_success_rate
    }

#[derive(Debug, Clone)]
pub struct PoolInfo {
    pub base_reserve: u64,
    pub quote_reserve: u64,
    pub base_decimal: u64,
    pub quote_decimal: u64,
    pub fee_numerator: u64,
    pub fee_denominator: u64,
    pub open_orders: Pubkey,
    pub target_orders: Pubkey,
    pub base_vault: Pubkey,
    pub quote_vault: Pubkey,
    pub market_id: Pubkey,
    pub market_program_id: Pubkey,
}

#[derive(Debug, Clone)]
pub struct PoolKeys {
    pub amm_id: Pubkey,
    pub amm_authority: Pubkey,
    pub amm_open_orders: Pubkey,
    pub amm_target_orders: Pubkey,
    pub pool_coin_token_account: Pubkey,
    pub pool_pc_token_account: Pubkey,
    pub serum_program_id: Pubkey,
    pub serum_market: Pubkey,
    pub serum_bids: Pubkey,
    pub serum_asks: Pubkey,
    pub serum_event_queue: Pubkey,
    pub serum_coin_vault_account: Pubkey,
    pub serum_pc_vault_account: Pubkey,
    pub serum_vault_signer: Pubkey,
    pub amm_program_id: Pubkey,
}

#[derive(Debug, Clone)]
pub struct ExecutionResult {
    pub signature: solana_sdk::signature::Signature,
    pub executed_amount: u64,
    pub execution_price: u64,
    pub slippage_bps: u64,
    pub execution_time: Duration,
    pub success: bool,
}

impl PerformanceMetrics {
    pub fn calculate_profit_factor(&self) -> f64 {
        if self.total_profit > 0 {
            let avg_trade_value = self.total_volume as f64 / self.successful_trades.max(1) as f64;
            (self.total_profit as f64) / avg_trade_value
        } else {
            0.0
        }
    }

    pub fn get_success_rate(&self) -> f64 {
        if self.successful_trades + self.failed_trades > 0 {
            self.successful_trades as f64 / (self.successful_trades + self.failed_trades) as f64
        } else {
            0.0
        }
    }
}
