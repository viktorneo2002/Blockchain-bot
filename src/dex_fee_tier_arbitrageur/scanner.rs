use anyhow::{anyhow, Result};
use solana_sdk::pubkey::Pubkey;
use std::sync::Arc;
use tokio::sync::mpsc;
use tracing::{debug, info, warn};

use crate::config::Config;
use crate::pool_cache::{PoolCache, PoolState, Protocol};
use crate::simulator::{simulate_orca_swap, simulate_raydium_swap, estimate_arbitrage_profit};

/// Arbitrage opportunity found by scanner
#[derive(Debug, Clone)]
pub struct ArbitrageOpportunity {
    pub id: String,
    pub pool_a: Arc<PoolState>,
    pub pool_b: Arc<PoolState>,
    pub token_in: Pubkey,
    pub token_intermediate: Pubkey,
    pub token_out: Pubkey,
    pub amount_in: u128,
    pub expected_intermediate: u128,
    pub expected_out: u128,
    pub estimated_profit: i128,
    pub profit_bps: i32,
    pub path_type: PathType,
    pub discovered_slot: u64,
    pub discovered_timestamp: u64,
}

#[derive(Debug, Clone, Copy)]
pub enum PathType {
    OrcaToRaydium,
    RaydiumToOrca,
    OrcaToOrca,
    RaydiumToRaydium,
}

impl PathType {
    pub fn as_str(&self) -> &str {
        match self {
            PathType::OrcaToRaydium => "orca_to_raydium",
            PathType::RaydiumToOrca => "raydium_to_orca",
            PathType::OrcaToOrca => "orca_to_orca",
            PathType::RaydiumToRaydium => "raydium_to_raydium",
        }
    }
}

/// Scanner for finding arbitrage opportunities
pub struct Scanner {
    config: Arc<Config>,
    pool_cache: Arc<PoolCache>,
    opportunity_tx: mpsc::Sender<ArbitrageOpportunity>,
}

impl Scanner {
    pub fn new(
        config: Arc<Config>,
        pool_cache: Arc<PoolCache>,
        opportunity_tx: mpsc::Sender<ArbitrageOpportunity>,
    ) -> Self {
        Self {
            config,
            pool_cache,
            opportunity_tx,
        }
    }
    
    /// Main scanning loop
    pub async fn run(&self) -> Result<()> {
        info!("Starting arbitrage scanner");
        
        let mut interval = tokio::time::interval(
            tokio::time::Duration::from_millis(self.config.scan_interval_ms)
        );
        
        loop {
            interval.tick().await;
            
            if let Err(e) = self.scan_opportunities().await {
                warn!("Error scanning opportunities: {}", e);
            }
        }
    }
    
    /// Scan all pool pairs for opportunities
    async fn scan_opportunities(&self) -> Result<()> {
        let all_pools = self.pool_cache.get_all();
        let pools_by_pair = self.group_pools_by_token_pair(&all_pools);
        
        let mut opportunities_found = 0;
        
        for ((token_a, token_b), pools) in pools_by_pair.iter() {
            // Need at least 2 pools for arbitrage
            if pools.len() < 2 {
                continue;
            }
            
            // Check all pool pairs
            for i in 0..pools.len() {
                for j in i + 1..pools.len() {
                    if let Some(opp) = self.check_pool_pair(
                        &pools[i],
                        &pools[j],
                        *token_a,
                        *token_b,
                    ).await {
                        if opp.estimated_profit > self.config.min_profit_lamports as i128 {
                            opportunities_found += 1;
                            
                            info!(
                                "Found opportunity: {} profit={} lamports ({} bps) path={}",
                                opp.id,
                                opp.estimated_profit,
                                opp.profit_bps,
                                opp.path_type.as_str()
                            );
                            
                            // Send to executor
                            if let Err(e) = self.opportunity_tx.send(opp).await {
                                warn!("Failed to send opportunity: {}", e);
                            }
                        }
                    }
                }
            }
        }
        
        if opportunities_found > 0 {
            debug!("Found {} opportunities in this scan", opportunities_found);
        }
        
        Ok(())
    }
    
    /// Group pools by token pair
    fn group_pools_by_token_pair(
        &self,
        pools: &[Arc<PoolState>]
    ) -> std::collections::HashMap<(Pubkey, Pubkey), Vec<Arc<PoolState>>> {
        let mut grouped = std::collections::HashMap::new();
        
        for pool in pools {
            let pair = self.normalize_pair(pool.token_a, pool.token_b);
            grouped.entry(pair).or_insert_with(Vec::new).push(pool.clone());
        }
        
        grouped
    }
    
    /// Check a specific pool pair for arbitrage
    async fn check_pool_pair(
        &self,
        pool_a: &Arc<PoolState>,
        pool_b: &Arc<PoolState>,
        token_1: Pubkey,
        token_2: Pubkey,
    ) -> Option<ArbitrageOpportunity> {
        // Skip if either pool is stale
        if pool_a.is_stale(self.config.max_pool_age_ms) || 
           pool_b.is_stale(self.config.max_pool_age_ms) {
            return None;
        }
        
        // Skip if low liquidity
        if pool_a.calculate_value() < self.config.min_liquidity_threshold ||
           pool_b.calculate_value() < self.config.min_liquidity_threshold {
            return None;
        }
        
        // Try both directions
        let test_amount = self.config.default_trade_size_lamports;
        
        // Direction 1: token_1 -> token_2 (pool_a) -> token_1 (pool_b)
        if let Some(opp) = self.simulate_arbitrage_path(
            pool_a.clone(),
            pool_b.clone(),
            token_1,
            token_2,
            token_1,
            test_amount,
            true,
        ).await {
            return Some(opp);
        }
        
        // Direction 2: token_2 -> token_1 (pool_a) -> token_2 (pool_b)
        if let Some(opp) = self.simulate_arbitrage_path(
            pool_a.clone(),
            pool_b.clone(),
            token_2,
            token_1,
            token_2,
            test_amount,
            false,
        ).await {
            return Some(opp);
        }
        
        None
    }
    
    /// Simulate an arbitrage path
    async fn simulate_arbitrage_path(
        &self,
        pool_a: Arc<PoolState>,
        pool_b: Arc<PoolState>,
        token_in: Pubkey,
        token_intermediate: Pubkey,
        token_out: Pubkey,
        amount_in: u128,
        a_to_b_first: bool,
    ) -> Option<ArbitrageOpportunity> {
        // Simulate first swap
        let amount_intermediate = match pool_a.protocol {
            Protocol::OrcaWhirlpool => {
                simulate_orca_swap(
                    pool_a.sqrt_price,
                    pool_a.liquidity,
                    pool_a.fee_rate,
                    amount_in,
                    a_to_b_first,
                ).ok()?
            }
            Protocol::RaydiumV4 => {
                let (reserve_in, reserve_out) = if a_to_b_first {
                    (pool_a.reserve_a, pool_a.reserve_b)
                } else {
                    (pool_a.reserve_b, pool_a.reserve_a)
                };
                
                simulate_raydium_swap(
                    reserve_in,
                    reserve_out,
                    amount_in,
                    pool_a.trade_fee_numerator,
                    pool_a.trade_fee_denominator,
                ).ok()?
            }
        };
        
        if amount_intermediate == 0 {
            return None;
        }
        
        // Simulate second swap
        let amount_out = match pool_b.protocol {
            Protocol::OrcaWhirlpool => {
                simulate_orca_swap(
                    pool_b.sqrt_price,
                    pool_b.liquidity,
                    pool_b.fee_rate,
                    amount_intermediate,
                    !a_to_b_first,
                ).ok()?
            }
            Protocol::RaydiumV4 => {
                let (reserve_in, reserve_out) = if !a_to_b_first {
                    (pool_b.reserve_a, pool_b.reserve_b)
                } else {
                    (pool_b.reserve_b, pool_b.reserve_a)
                };
                
                simulate_raydium_swap(
                    reserve_in,
                    reserve_out,
                    amount_intermediate,
                    pool_b.trade_fee_numerator,
                    pool_b.trade_fee_denominator,
                ).ok()?
            }
        };
        
        if amount_out <= amount_in {
            return None; // No profit
        }
        
        // Calculate profit
        let gas_cost = self.config.estimated_compute_units * self.config.priority_fee_lamports / 1000;
        let tip_cost = self.config.jito_tip_lamports;
        
        let profit = estimate_arbitrage_profit(
            amount_in,
            amount_intermediate,
            amount_out,
            gas_cost as u128,
            tip_cost,
        );
        
        if profit <= 0 {
            return None;
        }
        
        // Calculate profit in basis points
        let profit_bps = ((profit as f64 / amount_in as f64) * 10000.0) as i32;
        
        // Determine path type
        let path_type = match (pool_a.protocol, pool_b.protocol) {
            (Protocol::OrcaWhirlpool, Protocol::RaydiumV4) => PathType::OrcaToRaydium,
            (Protocol::RaydiumV4, Protocol::OrcaWhirlpool) => PathType::RaydiumToOrca,
            (Protocol::OrcaWhirlpool, Protocol::OrcaWhirlpool) => PathType::OrcaToOrca,
            (Protocol::RaydiumV4, Protocol::RaydiumV4) => PathType::RaydiumToRaydium,
        };
        
        let opp = ArbitrageOpportunity {
            id: format!("{}-{}-{}", 
                pool_a.address.to_string()[..8].to_string(),
                pool_b.address.to_string()[..8].to_string(),
                PoolState::current_timestamp()
            ),
            pool_a,
            pool_b,
            token_in,
            token_intermediate,
            token_out,
            amount_in,
            expected_intermediate: amount_intermediate,
            expected_out: amount_out,
            estimated_profit: profit,
            profit_bps,
            path_type,
            discovered_slot: 0, // Will be filled by executor
            discovered_timestamp: PoolState::current_timestamp(),
        };
        
        Some(opp)
    }
    
    /// Normalize token pair (smaller pubkey first)
    fn normalize_pair(&self, token_a: Pubkey, token_b: Pubkey) -> (Pubkey, Pubkey) {
        if token_a < token_b {
            (token_a, token_b)
        } else {
            (token_b, token_a)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_path_type_string() {
        assert_eq!(PathType::OrcaToRaydium.as_str(), "orca_to_raydium");
        assert_eq!(PathType::RaydiumToOrca.as_str(), "raydium_to_orca");
        assert_eq!(PathType::OrcaToOrca.as_str(), "orca_to_orca");
        assert_eq!(PathType::RaydiumToRaydium.as_str(), "raydium_to_raydium");
    }
}
