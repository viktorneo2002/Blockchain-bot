use solana_sdk::{
    instruction::{AccountMeta, Instruction},
    pubkey::Pubkey,
    account_info::AccountInfo,
    program_error::ProgramError,
};
use anchor_lang::{
    prelude::Program,
    solana_program::program::invoke,
    AnchorSerialize, AnchorDeserialize,
};
use drift::state::user::User;
use crate::{
    utils::market_metrics::MarketMetrics,
    core::confidence::ConfidenceScore,
    error::ArbitrageError,
};
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct LiquidationParams {
    pub partial_step: Option<f64>, // None = full liquidation, Some(0.1) = 10% step
    pub auction_market: Option<Pubkey>, // Optional auction market for CPI
}

pub struct LiquidationAdapter<'a> {
    program: &'a Program,
}

impl<'a> LiquidationAdapter<'a> {
    pub fn new(program: &'a Program) -> Self {
        Self { program }
    }

    pub fn build_liquidation_ix(
        &self,
        user: &User,
        params: &LiquidationParams,
    ) -> Result<Vec<Instruction>, ArbitrageError> {
        let mut ixs = Vec::new();
        
        // Partial liquidation logic
        if let Some(step) = params.partial_step {
            let amount = (user.debt as f64 * step) as u64;
            ixs.push(self.build_unwind_ix(user, amount)?);
        } else {
            // Full liquidation
            ixs.push(self.build_seize_ix(user)?);
        }

        // Auction integration if specified
        if let Some(auction_market) = params.auction_market {
            ixs.push(self.build_auction_bid_ix(user, auction_market)?);
        }

        Ok(ixs)
    }

    fn build_unwind_ix(&self, user: &User, amount: u64) -> Result<Instruction, ArbitrageError> {
        // Implementation depends on specific program
        Ok(Instruction::default())
    }

    fn build_seize_ix(&self, user: &User) -> Result<Instruction, ArbitrageError> {
        // Implementation depends on specific program
        Ok(Instruction::default())
    }

    fn build_auction_bid_ix(&self, user: &User, auction_market: Pubkey) -> Result<Instruction, ArbitrageError> {
        // Get simulated bid price from auction simulation
        let bid_price = self.simulate_auction_bid(user, auction_market)?;
        
        // CPI call to auction program
        let accounts = vec![
            AccountMeta::new(auction_market, false),
            // Add other required accounts
        ];
        
        Ok(Instruction {
            program_id: auction_market,
            accounts,
            data: AuctionInstruction::Bid {
                amount: bid_price,
                max_slippage_bps: 50, // 0.5% max slippage
            }.pack(),
        })
    }
    
    fn simulate_auction_bid(&self, user: &User, auction_market: Pubkey) -> Result<u64, ArbitrageError> {
        // Get market metrics for the auction
        let market_metrics = self.get_market_metrics(auction_market)?;
        
        // Calculate base bid price (90% of debt as fallback)
        let mut bid_price = user.debt.saturating_mul(90).checked_div(100).unwrap_or(0);
        
        // Adjust based on market conditions
        if let Some(metrics) = market_metrics {
            // Consider liquidity depth
            bid_price = bid_price.min(metrics.liquidity_depth);
            
            // Adjust based on volatility
            let volatility_adjustment = 1.0 - (metrics.volatility.min(0.5) / 2.0);
            bid_price = (bid_price as f64 * volatility_adjustment) as u64;
            
            // Apply confidence score if available
            if let Some(confidence) = &metrics.confidence {
                bid_price = confidence.adjust_bid_price(bid_price)?;
            }
        }
        
        // Ensure bid is at least minimum required
        Ok(bid_price.max(user.debt / 2)) // Never bid less than 50% of debt
    }
    
    fn get_market_metrics(&self, market: Pubkey) -> Result<Option<MarketMetrics>, ArbitrageError> {
        // Implementation would fetch from market data provider
        Ok(None)
    }
}

#[derive(anchor_lang::AnchorSerialize, anchor_lang::AnchorDeserialize)]
pub enum AuctionInstruction {
    Bid { amount: u64, max_slippage_bps: u16 },
}
