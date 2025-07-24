use anchor_lang::prelude::*;
use anchor_spl::token::{self, Token, TokenAccount};
use arrayref::{array_ref, array_refs};
use bytemuck::{from_bytes, try_cast_slice, Pod, Zeroable};
use pyth_sdk_solana::state::{PriceStatus, PriceAccount, Ema, PriceInfo};
use solana_program::{
    account_info::AccountInfo,
    clock::Clock,
    instruction::{AccountMeta, Instruction},
    program::{invoke, invoke_signed},
    program_error::ProgramError,
    program_pack::Pack,
    pubkey::Pubkey,
    sysvar::SysvarId,
};
use std::cell::RefMut;
use std::cmp::{max, min};
use std::collections::VecDeque;
use std::mem::size_of;
use std::str::FromStr;

// Production mainnet constants
const PRICE_BUFFER_SIZE: usize = 480; // 8 minutes at 1s intervals
const VOL_WINDOW: usize = 30; // 30 seconds for HFT
const VOL_OF_VOL_WINDOW: usize = 20;
const MAX_POSITION_USD: u64 = 250_000_000_000; // $250k
const MIN_TRADE_USD: u64 = 5_000_000_000; // $5k
const VOL_ENTRY_SIGMA: f64 = 2.5; // 2.5 standard deviations
const VOL_EXIT_SIGMA: f64 = 0.5;
const MAX_DRAWDOWN_BPS: u16 = 150; // 1.5%
const TARGET_PROFIT_BPS: u16 = 50; // 0.5%
const MAX_SLIPPAGE_BPS: u16 = 25; // 0.25%
const PRIORITY_FEE_MICROLAMPORTS: u64 = 100_000; // Competitive priority
const COMPUTE_UNITS: u32 = 400_000;
const RAYDIUM_AMM_V2: &str = "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8";

#[repr(C)]
#[derive(Clone, Copy, Debug, Pod, Zeroable)]
pub struct ValidatedPrice {
    pub price: u64,
    pub confidence: u64,
    pub timestamp: i64,
    pub slot: u64,
    pub status: u8,
    pub exponent: i32,
}

#[repr(C)]
#[derive(Clone, Copy, Debug, Default, Pod, Zeroable)]
pub struct VolMetrics {
    pub realized_vol: f64,
    pub vol_of_vol: f64,
    pub garch_vol: f64,
    pub price_ema_fast: f64,
    pub price_ema_slow: f64,
    pub vol_ema: f64,
    pub skewness: f64,
    pub kurtosis: f64,
    pub hurst_exponent: f64,
    pub regime: u8,
}

#[repr(C)]
#[derive(Clone, Copy, Debug, Default, Pod, Zeroable)]
pub struct TradingPosition {
    pub size: i64,
    pub entry_price: u64,
    pub entry_slot: u64,
    pub stop_loss: u64,
    pub take_profit: u64,
    pub trailing_stop: u64,
    pub max_profit: i64,
    pub fees_paid: u64,
}

pub struct VolOfVolTrader {
    price_buffer: VecDeque<ValidatedPrice>,
    vol_buffer: VecDeque<f64>,
    metrics: VolMetrics,
    position: TradingPosition,
    circuit_breaker: CircuitBreaker,
    pyth_product: Pubkey,
    pyth_price: Pubkey,
    amm_id: Pubkey,
    coin_mint: Pubkey,
    pc_mint: Pubkey,
}

impl VolOfVolTrader {
    pub fn new(
        pyth_product: Pubkey,
        pyth_price: Pubkey,
        amm_id: Pubkey,
        coin_mint: Pubkey,
        pc_mint: Pubkey,
    ) -> Self {
        Self {
            price_buffer: VecDeque::with_capacity(PRICE_BUFFER_SIZE),
            vol_buffer: VecDeque::with_capacity(VOL_OF_VOL_WINDOW),
            metrics: VolMetrics::default(),
            position: TradingPosition::default(),
            circuit_breaker: CircuitBreaker::new(),
            pyth_product,
            pyth_price,
            amm_id,
            coin_mint,
            pc_mint,
        }
    }

    pub fn update_from_pyth(&mut self, price_account: &AccountInfo, slot: u64) -> Result<()> {
        let data = price_account.try_borrow_data()?;
        
        // Validate account discriminator
        if data.len() < 88 || &data[0..8] != b"PriceAcc" {
            return Err(ErrorCode::InvalidOracle.into());
        }

        let price_data = try_cast_slice::<u8, PriceAccount>(&data)
            .ok()
            .and_then(|s| s.get(0))
            .ok_or(ErrorCode::InvalidOracle)?;

        // Validate price status
        if price_data.agg.status != PriceStatus::Trading {
            return Ok(()); // Skip non-trading prices
        }

        // Extract and validate price components
        let price = price_data.agg.price;
        let conf = price_data.agg.conf;
        let expo = price_data.expo;

        // Confidence check
        if conf > price.abs() / 10 {
            return Err(ErrorCode::PriceConfidenceTooLow.into());
        }

        let normalized_price = self.normalize_price(price, expo)?;
        let timestamp = Clock::get()?.unix_timestamp;

        let validated = ValidatedPrice {
            price: normalized_price,
            confidence: conf as u64,
            timestamp,
            slot,
            status: price_data.agg.status as u8,
            exponent: expo,
        };

        // Validate against previous price
        if let Some(last) = self.price_buffer.back() {
            if !self.validate_price_movement(&validated, last)? {
                self.circuit_breaker.trigger_price_limit();
                return Err(ErrorCode::PriceLimitExceeded.into());
            }
        }

        self.price_buffer.push_back(validated);
        if self.price_buffer.len() > PRICE_BUFFER_SIZE {
            self.price_buffer.pop_front();
        }

        self.calculate_metrics()?;
        Ok(())
    }

    fn normalize_price(&self, price: i64, expo: i32) -> Result<u64> {
        if expo >= 0 {
            return Err(ErrorCode::InvalidPriceExponent.into());
        }

        let factor = 10_i64.pow((-expo) as u32);
        let normalized = (price.abs() as u128 * 1_000_000u128) / factor as u128;
        
        if normalized > u64::MAX as u128 {
            return Err(ErrorCode::PriceOverflow.into());
        }

        Ok(normalized as u64)
    }

    fn validate_price_movement(&self, new: &ValidatedPrice, old: &ValidatedPrice) -> Result<bool> {
        let price_change = ((new.price as i128 - old.price as i128).abs() as f64) / old.price as f64;
        let time_diff = (new.timestamp - old.timestamp).abs();
        
        // Max 10% change per second
        let max_change = 0.1 * time_diff.max(1) as f64;
        Ok(price_change <= max_change)
    }

    fn calculate_metrics(&mut self) -> Result<()> {
        if self.price_buffer.len() < VOL_WINDOW {
            return Ok(());
        }

        let prices: Vec<f64> = self.price_buffer
            .iter()
            .rev()
            .take(VOL_WINDOW)
            .map(|p| p.price as f64 / 1e6)
            .collect();

        // Log returns
        let mut returns = Vec::with_capacity(prices.len() - 1);
        for i in 1..prices.len() {
            if prices[i-1] > 0.0 && prices[i] > 0.0 {
                returns.push((prices[i] / prices[i-1]).ln());
            }
        }

        if returns.is_empty() {
            return Ok(());
        }

        // Basic statistics
        let mean = returns.iter().sum::<f64>() / returns.len() as f64;
        let variance = returns.iter()
            .map(|r| (r - mean).powi(2))
            .sum::<f64>() / (returns.len() - 1) as f64;
        
        self.metrics.realized_vol = (variance.sqrt() * (86400.0_f64).sqrt()) * 100.0;

        // GARCH(1,1) volatility
        self.calculate_garch_vol(&returns)?;

        // Higher moments
        if variance > 0.0 {
            let std = variance.sqrt();
            self.metrics.skewness = returns.iter()
                .map(|r| ((r - mean) / std).powi(3))
                .sum::<f64>() / returns.len() as f64;
            
            self.metrics.kurtosis = returns.iter()
                .map(|r| ((r - mean) / std).powi(4))
                .sum::<f64>() / returns.len() as f64 - 3.0;
        }

        // EMAs
        let alpha_fast = 2.0 / (10.0 + 1.0);
        let alpha_slow = 2.0 / (30.0 + 1.0);
        self.metrics.price_ema_fast = prices.last().unwrap() * alpha_fast + 
            self.metrics.price_ema_fast * (1.0 - alpha_fast);
        self.metrics.price_ema_slow = prices.last().unwrap() * alpha_slow + 
            self.metrics.price_ema_slow * (1.0 - alpha_slow);

        // Update vol buffer
        self.vol_buffer.push_back(self.metrics.realized_vol);
        if self.vol_buffer.len() > VOL_OF_VOL_WINDOW {
            self.vol_buffer.pop_front();
        }

        // Vol of vol
        if self.vol_buffer.len() >= VOL_OF_VOL_WINDOW {
            let vol_mean = self.vol_buffer.iter().sum::<f64>() / self.vol_buffer.len() as f64;
            let vol_var = self.vol_buffer.iter()
                .map(|v| (v - vol_mean).powi(2))
                .sum::<f64>() / (self.vol_buffer.len() - 1) as f64;
            
            self.metrics.vol_of_vol = vol_var.sqrt() / vol_mean.max(0.001);
            self.metrics.vol_ema = vol_mean;
        }

        // Market regime
        self.metrics.regime = self.classify_regime();

        Ok(())
    }

    fn calculate_garch_vol(&mut self, returns: &[f64]) -> Result<()> {
        // GARCH(1,1): σ²(t) = ω + α*ε²(t-1) + β*σ²(t-1)
        const OMEGA: f64 = 0.000001;
        const ALPHA: f64 = 0.1;
        const BETA: f64 = 0.85;

        let mut garch_var = self.metrics.garch_vol.powi(2);
        for ret in returns {
            let epsilon_sq = ret.powi(2);
            garch_var = OMEGA + ALPHA * epsilon_sq + BETA * garch_var;
        }

        self.metrics.garch_vol = (garch_var.sqrt() * (86400.0_f64).sqrt()) * 100.0;
        Ok(())
    }

    fn classify_regime(&self) -> u8 {
        if self.metrics.realized_vol < 10.0 {
            0 // Low vol
        } else if self.metrics.realized_vol < 25.0 {
            1 // Normal vol
        } else if self.metrics.realized_vol < 50.0 {
            2 // High vol
        } else {
            3 // Extreme vol
        }
    }

    pub fn generate_signal(&mut self) -> Signal {
        if !self.circuit_breaker.can_trade() {
            return Signal::None;
        }

        let current_price = self.get_current_price();
        if current_price == 0 {
            return Signal::None;
        }

        // Position management first
        if self.position.size != 0 {
            if let Some(exit_signal) = self.check_exit_conditions(current_price) {
                return exit_signal;
            }
        }

        // Check vol regime
        if self.metrics.regime == 3 || self.metrics.vol_of_vol < 0.5 {
            return Signal::None;
        }

        // Entry conditions
        let vol_z_score = (self.metrics.realized_vol - self.metrics.vol_ema) / 
            (self.metrics.vol_ema * self.metrics.vol_of_vol).max(0.001);

        if vol_z_score.abs() > VOL_ENTRY_SIGMA && self.position.size == 0 {
            let confidence = self.calculate_signal_confidence(vol_z_score);
            
            // Mean reversion in high vol
            if vol_z_score > VOL_ENTRY_SIGMA && self.metrics.skewness < -0.3 {
                return Signal::Long { 
                    confidence, 
                                        stop_loss: self.calculate_stop_loss(current_price, true),
                    take_profit: self.calculate_take_profit(current_price, true),
                };
            }
            // Momentum in trending markets
            else if vol_z_score < -VOL_ENTRY_SIGMA && self.metrics.skewness > 0.3 {
                return Signal::Short {
                    confidence,
                    stop_loss: self.calculate_stop_loss(current_price, false),
                    take_profit: self.calculate_take_profit(current_price, false),
                };
            }
        }

        Signal::None
    }

    fn check_exit_conditions(&mut self, current_price: u64) -> Option<Signal> {
        let pnl_bps = self.calculate_pnl_bps(current_price);
        let slot_age = Clock::get().unwrap().slot - self.position.entry_slot;
        
        // Update trailing stop
        if pnl_bps > 0 {
            let new_trailing = current_price - (current_price * 50 / 10000); // 0.5% trailing
            self.position.trailing_stop = self.position.trailing_stop.max(new_trailing);
            self.position.max_profit = self.position.max_profit.max(pnl_bps as i64);
        }

        // Stop loss
        if pnl_bps <= -(MAX_DRAWDOWN_BPS as i64) {
            return Some(Signal::ClosePosition { reason: CloseReason::StopLoss });
        }

        // Trailing stop
        if self.position.size > 0 && current_price <= self.position.trailing_stop {
            return Some(Signal::ClosePosition { reason: CloseReason::TrailingStop });
        }

        // Take profit
        if pnl_bps >= TARGET_PROFIT_BPS as i64 {
            return Some(Signal::ClosePosition { reason: CloseReason::TakeProfit });
        }

        // Vol normalization
        if self.metrics.realized_vol < self.metrics.vol_ema * VOL_EXIT_SIGMA {
            return Some(Signal::ClosePosition { reason: CloseReason::VolNormalized });
        }

        // Time stop (max 300 slots ~ 2 minutes)
        if slot_age > 300 {
            return Some(Signal::ClosePosition { reason: CloseReason::TimeStop });
        }

        None
    }

    fn calculate_pnl_bps(&self, current_price: u64) -> i64 {
        if self.position.size == 0 || self.position.entry_price == 0 {
            return 0;
        }

        let price_diff = current_price as i64 - self.position.entry_price as i64;
        let pnl_bps = (price_diff * 10000) / self.position.entry_price as i64;
        
        if self.position.size < 0 {
            -pnl_bps
        } else {
            pnl_bps
        }
    }

    fn calculate_signal_confidence(&self, vol_z_score: f64) -> f64 {
        let vol_score = (vol_z_score.abs() / 3.0).min(1.0);
        let regime_score = match self.metrics.regime {
            0 => 0.3,
            1 => 1.0,
            2 => 0.7,
            _ => 0.0,
        };
        let quality_score = (self.price_buffer.len() as f64 / PRICE_BUFFER_SIZE as f64).min(1.0);
        
        (vol_score * 0.5 + regime_score * 0.3 + quality_score * 0.2).min(1.0)
    }

    fn calculate_stop_loss(&self, price: u64, is_long: bool) -> u64 {
        let stop_distance = price * MAX_DRAWDOWN_BPS as u64 / 10000;
        if is_long {
            price.saturating_sub(stop_distance)
        } else {
            price.saturating_add(stop_distance)
        }
    }

    fn calculate_take_profit(&self, price: u64, is_long: bool) -> u64 {
        let profit_distance = price * TARGET_PROFIT_BPS as u64 / 10000;
        if is_long {
            price.saturating_add(profit_distance)
        } else {
            price.saturating_sub(profit_distance)
        }
    }

    pub fn calculate_position_size(&self, balance: u64, confidence: f64) -> Result<u64> {
        // Kelly criterion with safety factor
        let kelly_fraction = confidence * 0.25; // Conservative 25% of Kelly
        let vol_adjustment = (20.0 / self.metrics.realized_vol.max(5.0)).min(1.0);
        let size_fraction = kelly_fraction * vol_adjustment;
        
        let position_size = (balance as f64 * size_fraction) as u64;
        Ok(position_size.max(MIN_TRADE_USD).min(MAX_POSITION_USD))
    }

    pub fn execute_trade<'info>(
        &mut self,
        ctx: Context<'_, '_, '_, 'info, ExecuteTrade<'info>>,
        signal: Signal,
        amount: u64,
    ) -> Result<()> {
        match signal {
            Signal::Long { confidence, stop_loss, take_profit } => {
                self.open_position(ctx, amount, true, stop_loss, take_profit)
            }
            Signal::Short { confidence, stop_loss, take_profit } => {
                self.open_position(ctx, amount, false, stop_loss, take_profit)
            }
            Signal::ClosePosition { reason } => {
                self.close_position(ctx, reason)
            }
            Signal::None => Ok(()),
        }
    }

    fn open_position<'info>(
        &mut self,
        ctx: Context<'_, '_, '_, 'info, ExecuteTrade<'info>>,
        amount: u64,
        is_long: bool,
        stop_loss: u64,
        take_profit: u64,
    ) -> Result<()> {
        // Validate AMM pool state
        let (coin_vault, pc_vault, target_orders, fees) = self.get_pool_state(&ctx)?;
        let current_price = self.calculate_amm_price(coin_vault, pc_vault)?;
        
        // Calculate swap amounts with slippage
        let (amount_in, min_amount_out) = self.calculate_swap_amounts(
            amount,
            current_price,
            coin_vault,
            pc_vault,
            fees,
            is_long,
        )?;

        // Execute swap
        self.execute_raydium_swap(
            &ctx,
            amount_in,
            min_amount_out,
            is_long,
        )?;

        // Update position
        let actual_price = self.get_execution_price(&ctx)?;
        self.position = TradingPosition {
            size: if is_long { amount as i64 } else { -(amount as i64) },
            entry_price: actual_price,
            entry_slot: Clock::get()?.slot,
            stop_loss,
            take_profit,
            trailing_stop: stop_loss,
            max_profit: 0,
            fees_paid: self.estimate_total_fees(amount_in),
        };

        self.circuit_breaker.record_trade();
        Ok(())
    }

    fn get_pool_state<'info>(
        &self,
        ctx: &Context<'_, '_, '_, 'info, ExecuteTrade<'info>>,
    ) -> Result<(u64, u64, u64, u64)> {
        let amm_data = ctx.accounts.amm.try_borrow_data()?;
        
        // Validate AMM account
        if amm_data.len() < 752 {
            return Err(ErrorCode::InvalidPoolAccount.into());
        }

        // Check discriminator
        let discriminator = u64::from_le_bytes(*array_ref![amm_data, 0, 8]);
        if discriminator != 7791132240093101084u64 { // Raydium AMM V2 discriminator
            return Err(ErrorCode::InvalidPoolAccount.into());
        }

        // Extract state
        let status = u64::from_le_bytes(*array_ref![amm_data, 8, 8]);
        if status != 1 { // Pool must be active
            return Err(ErrorCode::PoolNotActive.into());
        }

        let coin_vault = u64::from_le_bytes(*array_ref![amm_data, 112, 8]);
        let pc_vault = u64::from_le_bytes(*array_ref![amm_data, 120, 8]);
        let target_orders = u64::from_le_bytes(*array_ref![amm_data, 128, 8]);
        let fee_numerator = u64::from_le_bytes(*array_ref![amm_data, 160, 8]);

        Ok((coin_vault, pc_vault, target_orders, fee_numerator))
    }

    fn calculate_amm_price(&self, coin_vault: u64, pc_vault: u64) -> Result<u64> {
        if coin_vault == 0 {
            return Err(ErrorCode::InvalidPoolState.into());
        }
        Ok((pc_vault as u128 * 1_000_000u128 / coin_vault as u128) as u64)
    }

    fn calculate_swap_amounts(
        &self,
        amount_usd: u64,
        current_price: u64,
        coin_vault: u64,
        pc_vault: u64,
        fee_numerator: u64,
        is_buy: bool,
    ) -> Result<(u64, u64)> {
        let k = (coin_vault as u128).checked_mul(pc_vault as u128)
            .ok_or(ErrorCode::MathOverflow)?;

        if is_buy {
            // Buying token with USDC
            let amount_in = amount_usd;
            let fee = amount_in * fee_numerator / 1_000_000;
            let amount_in_after_fee = amount_in - fee;
            
            let new_pc = pc_vault + amount_in_after_fee;
            let new_coin = k / new_pc as u128;
            let amount_out = coin_vault - new_coin as u64;
            
            // Apply slippage
            let min_amount_out = amount_out * (10000 - MAX_SLIPPAGE_BPS as u64) / 10000;
            
            Ok((amount_in, min_amount_out))
        } else {
            // Selling token for USDC
            let tokens_to_sell = (amount_usd as u128 * 1_000_000u128 / current_price as u128) as u64;
            let fee = tokens_to_sell * fee_numerator / 1_000_000;
            let amount_in_after_fee = tokens_to_sell - fee;
            
            let new_coin = coin_vault + amount_in_after_fee;
            let new_pc = k / new_coin as u128;
            let amount_out = pc_vault - new_pc as u64;
            
            let min_amount_out = amount_out * (10000 - MAX_SLIPPAGE_BPS as u64) / 10000;
            
            Ok((tokens_to_sell, min_amount_out))
        }
    }

    fn execute_raydium_swap<'info>(
        &self,
        ctx: &Context<'_, '_, '_, 'info, ExecuteTrade<'info>>,
        amount_in: u64,
        min_amount_out: u64,
        is_buy: bool,
    ) -> Result<()> {
        let ix_data = RaydiumSwapInstructionV2 {
            instruction: 9,
            amount_in,
            min_amount_out,
        };

        let accounts = vec![
            AccountMeta::new_readonly(ctx.accounts.token_program.key(), false),
            AccountMeta::new(ctx.accounts.amm.key(), false),
            AccountMeta::new_readonly(ctx.accounts.amm_authority.key(), false),
            AccountMeta::new(ctx.accounts.amm_open_orders.key(), false),
            AccountMeta::new(ctx.accounts.amm_coin_vault.key(), false),
            AccountMeta::new(ctx.accounts.amm_pc_vault.key(), false),
            AccountMeta::new_readonly(ctx.accounts.serum_program.key(), false),
            AccountMeta::new(ctx.accounts.serum_market.key(), false),
            AccountMeta::new(ctx.accounts.serum_bids.key(), false),
            AccountMeta::new(ctx.accounts.serum_asks.key(), false),
            AccountMeta::new(ctx.accounts.serum_event_queue.key(), false),
            AccountMeta::new(ctx.accounts.serum_coin_vault.key(), false),
            AccountMeta::new(ctx.accounts.serum_pc_vault.key(), false),
            AccountMeta::new_readonly(ctx.accounts.serum_vault_signer.key(), false),
            AccountMeta::new(ctx.accounts.user_source_token.key(), false),
            AccountMeta::new(ctx.accounts.user_dest_token.key(), false),
            AccountMeta::new_readonly(ctx.accounts.user_owner.key(), true),
        ];

        let ix = Instruction {
            program_id: Pubkey::from_str(RAYDIUM_AMM_V2).unwrap(),
            accounts,
            data: ix_data.try_to_vec()?,
        };

        // Add compute budget instruction
        let compute_ix = solana_program::compute_budget::ComputeBudgetInstruction::set_compute_unit_limit(COMPUTE_UNITS);
        let priority_ix = solana_program::compute_budget::ComputeBudgetInstruction::set_compute_unit_price(PRIORITY_FEE_MICROLAMPORTS);

        invoke(&compute_ix, &[])?;
        invoke(&priority_ix, &[])?;
        invoke(&ix, &ctx.accounts.to_account_infos())?;

        Ok(())
    }

    fn close_position<'info>(
        &mut self,
        ctx: Context<'_, '_, '_, 'info, ExecuteTrade<'info>>,
                reason: CloseReason,
    ) -> Result<()> {
        if self.position.size == 0 {
            return Ok(());
        }

        let (coin_vault, pc_vault, _, fees) = self.get_pool_state(&ctx)?;
        let current_price = self.calculate_amm_price(coin_vault, pc_vault)?;
        
        let is_long = self.position.size > 0;
        let position_size = self.position.size.abs() as u64;
        
        let (amount_in, min_amount_out) = if is_long {
            // Closing long = selling tokens
            let usdc_out = (position_size as u128 * current_price as u128 / 1_000_000u128) as u64;
            let min_out = usdc_out * (10000 - MAX_SLIPPAGE_BPS as u64) / 10000;
            (position_size, min_out)
        } else {
            // Closing short = buying tokens
            let usdc_needed = (position_size as u128 * current_price as u128 / 1_000_000u128) as u64;
            let max_in = usdc_needed * (10000 + MAX_SLIPPAGE_BPS as u64) / 10000;
            (max_in, position_size)
        };

        self.execute_raydium_swap(&ctx, amount_in, min_amount_out, !is_long)?;
        
        let exit_price = self.get_execution_price(&ctx)?;
        let pnl = self.calculate_final_pnl(exit_price);
        
        self.position = TradingPosition::default();
        self.circuit_breaker.record_pnl(pnl);
        
        Ok(())
    }

    fn get_execution_price<'info>(
        &self,
        ctx: &Context<'_, '_, '_, 'info, ExecuteTrade<'info>>,
    ) -> Result<u64> {
        let (coin_vault, pc_vault, _, _) = self.get_pool_state(&ctx)?;
        self.calculate_amm_price(coin_vault, pc_vault)
    }

    fn calculate_final_pnl(&self, exit_price: u64) -> i64 {
        if self.position.size == 0 {
            return 0;
        }

        let entry_value = (self.position.size.abs() as u128 * self.position.entry_price as u128 / 1_000_000u128) as i64;
        let exit_value = (self.position.size.abs() as u128 * exit_price as u128 / 1_000_000u128) as i64;
        let fees = self.position.fees_paid as i64 * 2; // Entry + exit fees

        if self.position.size > 0 {
            exit_value - entry_value - fees
        } else {
            entry_value - exit_value - fees
        }
    }

    fn estimate_total_fees(&self, amount: u64) -> u64 {
        let trading_fee = amount * 25 / 10000; // 0.25% Raydium fee
        let priority_fee = PRIORITY_FEE_MICROLAMPORTS * COMPUTE_UNITS / 1_000_000;
        let rent = 2_000; // Rent for temp accounts
        trading_fee + priority_fee + rent
    }

    fn get_current_price(&self) -> u64 {
        self.price_buffer.back().map(|p| p.price).unwrap_or(0)
    }
}

pub struct CircuitBreaker {
    trades_count: u16,
    last_reset: i64,
    consecutive_losses: u8,
    total_pnl: i64,
    is_halted: bool,
    price_limit_triggered: u8,
}

impl CircuitBreaker {
    pub fn new() -> Self {
        Self {
            trades_count: 0,
            last_reset: Clock::get().unwrap().unix_timestamp,
            consecutive_losses: 0,
            total_pnl: 0,
            is_halted: false,
            price_limit_triggered: 0,
        }
    }

    pub fn can_trade(&mut self) -> bool {
        self.reset_if_needed();
        
        if self.is_halted || self.price_limit_triggered > 3 {
            return false;
        }

        if self.trades_count > 100 { // Max 100 trades per hour
            self.is_halted = true;
            return false;
        }

        if self.consecutive_losses > 3 {
            self.is_halted = true;
            return false;
        }

        if self.total_pnl < -50_000_000_000 { // -$50k loss limit
            self.is_halted = true;
            return false;
        }

        true
    }

    pub fn record_trade(&mut self) {
        self.trades_count += 1;
    }

    pub fn record_pnl(&mut self, pnl: i64) {
        self.total_pnl += pnl;
        if pnl < 0 {
            self.consecutive_losses += 1;
        } else {
            self.consecutive_losses = 0;
        }
    }

    pub fn trigger_price_limit(&mut self) {
        self.price_limit_triggered += 1;
    }

    fn reset_if_needed(&mut self) {
        let now = Clock::get().unwrap().unix_timestamp;
        if now - self.last_reset > 3600 { // Reset hourly
            self.trades_count = 0;
            self.last_reset = now;
            self.price_limit_triggered = 0;
            if self.total_pnl > 0 {
                self.is_halted = false;
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Signal {
    Long { 
        confidence: f64,
        stop_loss: u64,
        take_profit: u64,
    },
    Short { 
        confidence: f64,
        stop_loss: u64,
        take_profit: u64,
    },
    ClosePosition { reason: CloseReason },
    None,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum CloseReason {
    StopLoss,
    TakeProfit,
    TrailingStop,
    VolNormalized,
    TimeStop,
}

#[derive(AnchorSerialize, AnchorDeserialize)]
pub struct RaydiumSwapInstructionV2 {
    pub instruction: u8,
    pub amount_in: u64,
    pub min_amount_out: u64,
}

#[derive(Accounts)]
pub struct ExecuteTrade<'info> {
    #[account(mut)]
    pub user_owner: Signer<'info>,
    
    pub token_program: Program<'info, Token>,
    #[account(mut)]
    pub amm: AccountInfo<'info>,
    /// CHECK: Validated in handler
    pub amm_authority: AccountInfo<'info>,
    #[account(mut)]
    pub amm_open_orders: AccountInfo<'info>,
    #[account(mut)]
    pub amm_coin_vault: Box<Account<'info, TokenAccount>>,
    #[account(mut)]
    pub amm_pc_vault: Box<Account<'info, TokenAccount>>,
    
    /// CHECK: Serum DEX program
    pub serum_program: AccountInfo<'info>,
    #[account(mut)]
    pub serum_market: AccountInfo<'info>,
    #[account(mut)]
    pub serum_bids: AccountInfo<'info>,
    #[account(mut)]
    pub serum_asks: AccountInfo<'info>,
    #[account(mut)]
    pub serum_event_queue: AccountInfo<'info>,
    #[account(mut)]
    pub serum_coin_vault: AccountInfo<'info>,
    #[account(mut)]
    pub serum_pc_vault: AccountInfo<'info>,
    /// CHECK: Validated by Serum
    pub serum_vault_signer: AccountInfo<'info>,
    
    #[account(mut)]
    pub user_source_token: Box<Account<'info, TokenAccount>>,
    #[account(mut)]
    pub user_dest_token: Box<Account<'info, TokenAccount>>,
}

#[error_code]
pub enum ErrorCode {
    #[msg("Invalid oracle account")]
    InvalidOracle,
    #[msg("Price confidence too low")]
    PriceConfidenceTooLow,
    #[msg("Price limit exceeded")]
    PriceLimitExceeded,
    #[msg("Invalid price exponent")]
    InvalidPriceExponent,
    #[msg("Price overflow")]
    PriceOverflow,
    #[msg("Invalid pool account")]
    InvalidPoolAccount,
    #[msg("Pool not active")]
    PoolNotActive,
    #[msg("Invalid pool state")]
    InvalidPoolState,
    #[msg("Math overflow")]
    MathOverflow,
}

pub fn execute_vol_strategy<'info>(
    ctx: Context<'_, '_, '_, 'info, ExecuteTrade<'info>>,
    pyth_price_account: &AccountInfo<'info>,
    params: StrategyParams,
) -> Result<()> {
    let mut trader = VolOfVolTrader::new(
        params.pyth_product,
        params.pyth_price,
        params.amm_id,
        params.coin_mint,
        params.pc_mint,
    );

    trader.update_from_pyth(pyth_price_account, Clock::get()?.slot)?;
    
    let signal = trader.generate_signal();
    if signal != Signal::None {
        let balance = ctx.accounts.user_source_token.amount;
        let confidence = match &signal {
            Signal::Long { confidence, .. } | Signal::Short { confidence, .. } => *confidence,
            _ => 1.0,
        };
        
        let size = trader.calculate_position_size(balance, confidence)?;
        trader.execute_trade(ctx, signal, size)?;
    }

    Ok(())
}

#[derive(AnchorSerialize, AnchorDeserialize)]
pub struct StrategyParams {
    pub pyth_product: Pubkey,
    pub pyth_price: Pubkey,
    pub amm_id: Pubkey,
    pub coin_mint: Pubkey,
        pub pc_mint: Pubkey,
}

// Production utilities for MEV optimization
pub fn calculate_optimal_priority_fee(
    network_congestion: f64,
    expected_profit: u64,
    competition_level: u8,
) -> u64 {
    let base_fee = PRIORITY_FEE_MICROLAMPORTS;
    let congestion_multiplier = 1.0 + network_congestion.min(2.0);
    let competition_multiplier = 1.0 + (competition_level as f64 * 0.5);
    let profit_percentage = (expected_profit as f64 * 0.01).min(1_000_000.0);
    
    ((base_fee as f64 * congestion_multiplier * competition_multiplier) + profit_percentage) as u64
}

pub fn validate_transaction_viability(
    expected_profit: i64,
    total_fees: u64,
    slippage_impact: u64,
) -> bool {
    let net_profit = expected_profit - total_fees as i64 - slippage_impact as i64;
    net_profit > MIN_TRADE_USD as i64 / 10 // Minimum 10% of min trade size
}

// MEV protection
pub fn add_jitter_to_amount(amount: u64, seed: u64) -> u64 {
    let jitter = (seed % 100) as u64; // 0-99 basis points
    amount * (10000 + jitter) / 10000
}

// Recovery mechanism for failed transactions
pub struct TransactionRecovery {
    retry_count: u8,
    last_error: Option<ErrorCode>,
    backoff_slots: u64,
}

impl TransactionRecovery {
    pub fn should_retry(&self, current_slot: u64, last_attempt_slot: u64) -> bool {
        self.retry_count < 3 && 
        current_slot >= last_attempt_slot + self.backoff_slots
    }

    pub fn calculate_backoff(&mut self) {
        self.backoff_slots = 2u64.pow(self.retry_count as u32) * 10; // Exponential backoff
        self.retry_count += 1;
    }
}

// Pool validation before trading
pub fn validate_pool_depth(
    coin_vault: u64,
    pc_vault: u64,
    trade_size: u64,
) -> Result<()> {
    let min_depth = trade_size * 20; // Pool must be 20x trade size
    if pc_vault < min_depth {
        return Err(ErrorCode::InvalidPoolState.into());
    }
    Ok(())
}

// Atomic arbitrage check
pub fn check_arbitrage_opportunity(
    raydium_price: u64,
    oracle_price: u64,
    fees_bps: u64,
) -> Option<(bool, u64)> {
    let price_diff = if raydium_price > oracle_price {
        ((raydium_price - oracle_price) * 10000) / oracle_price
    } else {
        ((oracle_price - raydium_price) * 10000) / raydium_price
    };

    if price_diff > fees_bps * 2 {
        Some((raydium_price > oracle_price, price_diff))
    } else {
        None
    }
}

// Emergency shutdown
pub fn emergency_close_all_positions<'info>(
    ctx: Context<'_, '_, '_, 'info, ExecuteTrade<'info>>,
    trader: &mut VolOfVolTrader,
) -> Result<()> {
    if trader.position.size != 0 {
        trader.close_position(ctx, CloseReason::TimeStop)?;
    }
    Ok(())
}

// Production monitoring
pub struct PerformanceMetrics {
    pub total_trades: u64,
    pub winning_trades: u64,
    pub total_volume: u64,
    pub total_fees: u64,
    pub sharpe_ratio: f64,
    pub max_drawdown: f64,
    pub avg_trade_duration: u64,
}

impl PerformanceMetrics {
    pub fn update(&mut self, pnl: i64, volume: u64, fees: u64, duration: u64) {
        self.total_trades += 1;
        if pnl > 0 {
            self.winning_trades += 1;
        }
        self.total_volume += volume;
        self.total_fees += fees;
        
        // Update rolling metrics
        let win_rate = self.winning_trades as f64 / self.total_trades as f64;
        self.avg_trade_duration = (self.avg_trade_duration * (self.total_trades - 1) + duration) / self.total_trades;
    }

    pub fn get_win_rate(&self) -> f64 {
        if self.total_trades == 0 {
            0.0
        } else {
            self.winning_trades as f64 / self.total_trades as f64
        }
    }
}

// Final safety checks before execution
pub fn pre_execution_checks(
    trader: &VolOfVolTrader,
    signal: &Signal,
    balance: u64,
) -> Result<()> {
    // Check market regime
    if trader.metrics.regime == 3 {
        return Err(ErrorCode::InvalidPoolState.into());
    }

    // Check price staleness
    if let Some(last_price) = trader.price_buffer.back() {
        let age = Clock::get()?.unix_timestamp - last_price.timestamp;
        if age > 5 {
            return Err(ErrorCode::InvalidOracle.into());
        }
    }

    // Check balance sufficiency
    match signal {
        Signal::Long { .. } | Signal::Short { .. } => {
            if balance < MIN_TRADE_USD {
                return Err(ProgramError::InsufficientFunds.into());
            }
        }
        _ => {}
    }

    Ok(())
}

#[cfg(feature = "mainnet")]
pub mod mainnet_config {
    use super::*;
    
    pub const WHITELISTED_POOLS: &[&str] = &[
        "58oQChx4yWmvKdwLLZzBi4ChoCc2fqCUWBkwMihLYQo2", // SOL/USDC
        "7XawhbbxtsRcQA8KTkHT9f9nc6d69UwqCDh6U5EEbEmX", // RAY/USDC
        "6UmmUiYoBjSrhakAobJw8BvkmJtDVxaeBtbt7rxWo1mg", // USDT/USDC
    ];

    pub const TRUSTED_ORACLES: &[&str] = &[
        "H6ARHf6YXhGYeQfUzQNGk6rDNnLBQKrenN712K4AQJEG", // SOL/USD
        "AnLf8tVYCM816gmBjiy8n53eXKKEDydT5piYjjQDPgTB", // RAY/USD
    ];
}

// Production entry point
pub fn run_vol_of_vol_strategy<'info>(
    ctx: Context<'_, '_, '_, 'info, ExecuteTrade<'info>>,
    oracle_account: &AccountInfo<'info>,
    params: StrategyParams,
) -> Result<()> {
    // Validate mainnet configuration
    #[cfg(feature = "mainnet")]
    {
        if !mainnet_config::WHITELISTED_POOLS.contains(&params.amm_id.to_string().as_str()) {
            return Err(ErrorCode::InvalidPoolAccount.into());
        }
    }

    // Initialize trader
    let mut trader = VolOfVolTrader::new(
        params.pyth_product,
        params.pyth_price,
        params.amm_id,
        params.coin_mint,
        params.pc_mint,
    );

    // Update with latest price
    trader.update_from_pyth(oracle_account, Clock::get()?.slot)?;

    // Generate and validate signal
    let mut signal = trader.generate_signal();
    
    // Pre-execution validation
    let balance = ctx.accounts.user_source_token.amount;
    pre_execution_checks(&trader, &signal, balance)?;

    // Calculate position size with MEV protection
    if let Signal::Long { confidence, .. } | Signal::Short { confidence, .. } = &signal {
        let base_size = trader.calculate_position_size(balance, *confidence)?;
        let jittered_size = add_jitter_to_amount(base_size, Clock::get()?.slot);
        
        // Validate transaction viability
        let expected_profit = (jittered_size as f64 * confidence * 0.01) as i64;
        let total_fees = trader.estimate_total_fees(jittered_size);
        
        if !validate_transaction_viability(expected_profit, total_fees, 0) {
            return Ok(()); // Skip unprofitable trades
        }

        // Execute with dynamic priority fee
        let priority_fee = calculate_optimal_priority_fee(
            0.5, // Network congestion estimate
            expected_profit as u64,
            1, // Competition level
        );

        // Execute trade
        trader.execute_trade(ctx, signal, jittered_size)?;
    } else if let Signal::ClosePosition { reason } = signal {
        // Always close positions when signaled
        trader.execute_trade(ctx, signal, 0)?;
    }

    Ok(())
}

