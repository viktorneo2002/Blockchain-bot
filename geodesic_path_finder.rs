use std::collections::BinaryHeap;
use std::cmp::Ordering;
use parking_lot::RwLock;
use std::sync::Arc;
use rayon::prelude::*;
use solana_program::pubkey::Pubkey;
use fixed::types::U64F64;
use ahash::{AHashMap, AHashSet};

const MAX_PATH_LENGTH: usize = 5;
const MIN_PROFIT_THRESHOLD: u64 = 100_000;
const MAX_SLIPPAGE_BPS: u64 = 50;
const GAS_COST_LAMPORTS: u64 = 5_000;
const PRIORITY_FEE_MULTIPLIER: f64 = 1.5;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct PoolId(pub Pubkey);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct TokenMint(pub Pubkey);

#[derive(Debug, Clone, Copy)]
pub enum PoolType {
    RaydiumV4,
    OrcaWhirlpool,
    SerumV3,
    PhoenixV1,
    LifinityV2,
}

#[derive(Debug, Clone)]
pub struct PoolEdge {
    pub pool_id: PoolId,
    pub pool_type: PoolType,
    pub token_a: TokenMint,
    pub token_b: TokenMint,
    pub reserve_a: u64,
    pub reserve_b: u64,
    pub fee_bps: u16,
    pub tick_spacing: Option<i32>,
    pub current_tick: Option<i32>,
    pub liquidity: Option<u128>,
    pub sqrt_price: Option<u128>,
}

#[derive(Debug, Clone)]
struct PathNode {
    token: TokenMint,
    amount: u64,
    gas_cost: u64,
    path: Vec<(PoolId, bool)>,
    profit: i64,
}

impl Eq for PathNode {}

impl PartialEq for PathNode {
    fn eq(&self, other: &Self) -> bool {
        self.profit == other.profit && self.token == other.token
    }
}

impl Ord for PathNode {
    fn cmp(&self, other: &Self) -> Ordering {
        self.profit.cmp(&other.profit)
            .then_with(|| self.gas_cost.cmp(&other.gas_cost).reverse())
    }
}

impl PartialOrd for PathNode {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

pub struct GeodesicPathFinder {
    graph: Arc<RwLock<AHashMap<TokenMint, Vec<Arc<PoolEdge>>>>>,
    pool_cache: Arc<RwLock<AHashMap<PoolId, Arc<PoolEdge>>>>,
    sqrt_price_cache: Arc<RwLock<AHashMap<PoolId, u128>>>,
}

impl Default for GeodesicPathFinder {
    fn default() -> Self {
        Self::new()
    }
}

impl GeodesicPathFinder {
    pub fn new() -> Self {
        Self {
            graph: Arc::new(RwLock::new(AHashMap::with_capacity(1000))),
            pool_cache: Arc::new(RwLock::new(AHashMap::with_capacity(5000))),
            sqrt_price_cache: Arc::new(RwLock::new(AHashMap::with_capacity(5000))),
        }
    }

    pub fn update_pool(&self, edge: PoolEdge) {
        let pool_id = edge.pool_id;
        let edge_arc = Arc::new(edge.clone());
        
        {
            let mut cache = self.pool_cache.write();
            cache.insert(pool_id, edge_arc.clone());
        }

        if let Some(sqrt_price) = edge.sqrt_price {
            let mut sqrt_cache = self.sqrt_price_cache.write();
            sqrt_cache.insert(pool_id, sqrt_price);
        }

        let mut graph = self.graph.write();
        
        graph.entry(edge.token_a)
            .or_default()
            .push(edge_arc.clone());
            
        graph.entry(edge.token_b)
            .or_default()
            .push(edge_arc);
    }

    pub fn find_optimal_path(
        &self,
        start_token: TokenMint,
        start_amount: u64,
        target_tokens: &[TokenMint],
    ) -> Option<(Vec<(PoolId, bool)>, u64, i64)> {
        let graph = self.graph.read();
        let pool_cache = self.pool_cache.read();
        
        let mut heap = BinaryHeap::new();
        let mut best_paths: AHashMap<TokenMint, (u64, i64)> = AHashMap::new();
        
        heap.push(PathNode {
            token: start_token,
            amount: start_amount,
            gas_cost: 0,
            path: Vec::new(),
            profit: 0,
        });
        
        best_paths.insert(start_token, (start_amount, 0));
        
        while let Some(node) = heap.pop() {
            if node.path.len() >= MAX_PATH_LENGTH {
                continue;
            }
            
            if let Some(&(best_amount, best_profit)) = best_paths.get(&node.token) {
                if node.amount < best_amount || node.profit < best_profit {
                    continue;
                }
            }
            
            if let Some(edges) = graph.get(&node.token) {
                edges.iter().for_each(|edge| {
                    let (output_token, output_amount, is_reverse) = 
                        self.calculate_swap_output(edge, node.token, node.amount);
                    
                    if output_amount == 0 {
                        return;
                    }
                    
                    let gas_cost = node.gas_cost + self.calculate_gas_cost(&edge.pool_type);
                    
                    let profit = if output_token == start_token && !node.path.is_empty() {
                        output_amount as i64 - start_amount as i64 - gas_cost as i64
                    } else {
                        node.profit
                    };
                    
                    if profit > 0 || node.path.is_empty() {
                        let mut new_path = node.path.clone();
                        new_path.push((edge.pool_id, is_reverse));
                        
                        let should_add = best_paths.get(&output_token)
                            .map(|&(amt, prof)| output_amount > amt || profit > prof)
                            .unwrap_or(true);
                        
                        if should_add {
                            best_paths.insert(output_token, (output_amount, profit));
                            
                            heap.push(PathNode {
                                token: output_token,
                                amount: output_amount,
                                gas_cost,
                                path: new_path,
                                profit,
                            });
                        }
                    }
                });
            }
        }
        
        target_tokens.iter()
            .filter_map(|&token| {
                best_paths.get(&token).and_then(|&(amount, profit)| {
                    if profit > MIN_PROFIT_THRESHOLD as i64 {
                        heap.iter()
                            .find(|n| n.token == token && n.profit == profit)
                            .map(|n| (n.path.clone(), amount, profit))
                    } else {
                        None
                    }
                })
            })
            .max_by_key(|&(_, _, profit)| profit)
    }

    fn calculate_swap_output(
        &self,
        edge: &PoolEdge,
        input_token: TokenMint,
        input_amount: u64,
    ) -> (TokenMint, u64, bool) {
        let is_reverse = input_token == edge.token_b;
        let output_token = if is_reverse { edge.token_a } else { edge.token_b };
        
        let output_amount = match edge.pool_type {
            PoolType::RaydiumV4 | PoolType::OrcaWhirlpool => {
                self.calculate_cpmm_output(edge, input_amount, is_reverse)
            }
            PoolType::SerumV3 | PoolType::PhoenixV1 => {
                self.calculate_clob_output(edge, input_amount, is_reverse)
            }
            PoolType::LifinityV2 => {
                self.calculate_prorata_output(edge, input_amount, is_reverse)
            }
        };
        
        (output_token, output_amount, is_reverse)
    }

    fn calculate_cpmm_output(
        &self,
        edge: &PoolEdge,
        input_amount: u64,
        is_reverse: bool,
    ) -> u64 {
        let (reserve_in, reserve_out) = if is_reverse {
            (edge.reserve_b, edge.reserve_a)
        } else {
            (edge.reserve_a, edge.reserve_b)
        };
        
        if reserve_in == 0 || reserve_out == 0 {
            return 0;
        }
        
        let fee_multiplier = 10000u64.saturating_sub(edge.fee_bps as u64);
        let amount_in_with_fee = input_amount.saturating_mul(fee_multiplier);
        
        let numerator = amount_in_with_fee.saturating_mul(reserve_out);
        let denominator = reserve_in.saturating_mul(10000)
            .saturating_add(amount_in_with_fee);
        
        if denominator == 0 {
            return 0;
        }
        
        let output = numerator / denominator;
        
        let slippage = output.saturating_mul(MAX_SLIPPAGE_BPS) / 10000;
        output.saturating_sub(slippage)
    }

    fn calculate_clob_output(
        &self,
        edge: &PoolEdge,
        input_amount: u64,
        is_reverse: bool,
    ) -> u64 {
        if let (Some(sqrt_price), Some(liquidity)) = (edge.sqrt_price, edge.liquidity) {
            let sqrt_price_x64 = U64F64::from_bits(sqrt_price);
            let liquidity_u64 = liquidity as u64;
            
            if is_reverse {
                let price_impact = self.calculate_price_impact(
                    sqrt_price_x64,
                    liquidity_u64,
                    input_amount,
                    false
                );
                
                let effective_price = sqrt_price_x64.saturating_mul(
                    U64F64::from_num(1.0 - price_impact)
                );
                
                let output = U64F64::from_num(input_amount)
                    .saturating_div(effective_price)
                    .to_num::<u64>();
                
                output.saturating_mul(10000 - edge.fee_bps as u64) / 10000
            } else {
                let price_impact = self.calculate_price_impact(
                    sqrt_price_x64,
                    liquidity_u64,
                    input_amount,
                    true
                );
                
                let effective_price = sqrt_price_x64.saturating_mul(
                    U64F64::from_num(1.0 + price_impact)
                );
                
                let output = U64F64::from_num(input_amount)
                    .saturating_mul(effective_price)
                    .to_num::<u64>();
                
                output.saturating_mul(10000 - edge.fee_bps as u64) / 10000
            }
        } else {
            self.calculate_cpmm_output(edge, input_amount, is_reverse)
        }
    }

    fn calculate_prorata_output(
        &self,
        edge: &PoolEdge,
        input_amount: u64,
        is_reverse: bool,
    ) -> u64 {
        let (reserve_in, reserve_out) = if is_reverse {
            (edge.reserve_b, edge.reserve_a)
        } else {
            (edge.reserve_a, edge.reserve_b)
        };
        
        if reserve_in == 0 || reserve_out == 0 {
            return 0;
        }
        
        let oracle_price = if let Some(sqrt_price) = edge.sqrt_price {
            let price_x64 = U64F64::from_bits(sqrt_price);
            price_x64.saturating_mul(price_x64).to_num::<f64>()
        } else {
            reserve_out as f64 / reserve_in as f64
        };
        
        let base_output = (input_amount as f64 * oracle_price) as u64;
        let fee_adjusted = base_output.saturating_mul(10000 - edge.fee_bps as u64) / 10000;
        
        let available_liquidity = reserve_out.saturating_mul(95) / 100;
        fee_adjusted.min(available_liquidity)
    }

    fn calculate_price_impact(
        &self,
        sqrt_price: U64F64,
        liquidity: u64,
        amount: u64,
        is_buy: bool,
    ) -> f64 {
        let amount_f64 = amount as f64;
        let liquidity_f64 = liquidity as f64;
        let _price = sqrt_price.to_num::<f64>().powi(2);
        
        let depth_factor = (liquidity_f64 / 1e9).sqrt();
        let size_factor = amount_f64 / liquidity_f64;
        
        let base_impact = size_factor * if is_buy { 1.2 } else { 0.8 };
        let adjusted_impact = base_impact / depth_factor.max(1.0);
        
        adjusted_impact.min(0.1)
    }

    fn calculate_gas_cost(&self, pool_type: &PoolType) -> u64 {
        let base_cost = match pool_type {
            PoolType::RaydiumV4 => 35_000,
            PoolType::OrcaWhirlpool => 45_000,
            PoolType::SerumV3 => 85_000,
            PoolType::PhoenixV1 => 75_000,
            PoolType::LifinityV2 => 30_000,
        };
        
        (base_cost as f64 * PRIORITY_FEE_MULTIPLIER) as u64 + GAS_COST_LAMPORTS
    }

    pub fn find_arbitrage_cycles(
        &self,
        min_profit: u64,
        max_depth: usize,
    ) -> Vec<(Vec<(PoolId, bool)>, u64, i64)> {
        let graph = self.graph.read();
        let all_tokens: Vec<TokenMint> = graph.keys().cloned().collect();
        
        all_tokens.iter()
            .filter_map(|&start_token| {
                self.find_profitable_cycle(start_token, min_profit, max_depth)
            })
            .collect()
    }

    fn find_profitable_cycle(
        &self,
        start_token: TokenMint,
        min_profit: u64,
        max_depth: usize,
    ) -> Option<(Vec<(PoolId, bool)>, u64, i64)> {
        let graph = self.graph.read();
        let mut visited_pools = AHashSet::new();
        let mut best_cycle: Option<(Vec<(PoolId, bool)>, u64, i64)> = None;
        
        self.dfs_cycle(
            &graph,
            start_token,
            start_token,
            1_000_000_000,
            Vec::new(),
            0,
            0,
            &mut visited_pools,
            &mut best_cycle,
            min_profit,
            max_depth,
        );
        
        best_cycle
    }

    fn dfs_cycle(
        &self,
        graph: &AHashMap<TokenMint, Vec<Arc<PoolEdge>>>,
        start_token: TokenMint,
        current_token: TokenMint,
        current_amount: u64,
        path: Vec<(PoolId, bool)>,
        total_gas: u64,
        depth: usize,
        visited_pools: &mut AHashSet<PoolId>,
        best_cycle: &mut Option<(Vec<(PoolId, bool)>, u64, i64)>,
        min_profit: u64,
        max_depth: usize,
    ) {
        if depth > max_depth {
            return;
        }
        
        if current_token == start_token && depth >= 2 {
            let profit = current_amount as i64 - 1_000_000_000i64 - total_gas as i64;
            
            if profit > min_profit as i64 {
                if let Some((_, _, best_profit)) = best_cycle {
                    if profit > *best_profit {
                        *best_cycle = Some((path.clone(), current_amount, profit));
                    }
                } else {
                    *best_cycle = Some((path.clone(), current_amount, profit));
                }
            }
            return;
        }
        
        if let Some(edges) = graph.get(&current_token) {
            for edge in edges {
                if visited_pools.contains(&edge.pool_id) {
                    continue;
                }
                
                let (next_token, output_amount, is_reverse) = 
                    self.calculate_swap_output(edge, current_token, current_amount);
                
                if output_amount == 0 {
                    continue;
                }
                
                let gas_cost = self.calculate_gas_cost(&edge.pool_type);
                let new_total_gas = total_gas + gas_cost;
                
                if new_total_gas > current_amount / 100 {
                    continue;
                }
                
                visited_pools.insert(edge.pool_id);
                let mut new_path = path.clone();
                new_path.push((edge.pool_id, is_reverse));
                
                self.dfs_cycle(
                    graph,
                    start_token,
                    next_token,
                    output_amount,
                    new_path,
                    new_total_gas,
                    depth + 1,
                    visited_pools,
                    best_cycle,
                    min_profit,
                    max_depth,
                );
                
                visited_pools.remove(&edge.pool_id);
            }
        }
    }

    pub fn simulate_path_execution(
        &self,
        path: &[(PoolId, bool)],
        input_amount: u64,
    ) -> Result<(u64, Vec<u64>), &'static str> {
        let pool_cache = self.pool_cache.read();
        let mut current_amount = input_amount;
        let mut amounts = Vec::with_capacity(path.len() + 1);
        amounts.push(current_amount);
        
        for &(pool_id, is_reverse) in path {
            let pool = pool_cache.get(&pool_id)
                .ok_or("Pool not found in cache")?;
            
            let input_token = if is_reverse { pool.token_b } else { pool.token_a };
            let (_, output_amount, _) = self.calculate_swap_output(
                pool,
                input_token,
                current_amount,
            );
            
            if output_amount == 0 {
                return Err("Zero output detected in path");
            }
            
            current_amount = output_amount;
            amounts.push(current_amount);
        }
        
        Ok((current_amount, amounts))
    }

    pub fn validate_path_profitability(
        &self,
        path: &[(PoolId, bool)],
        input_amount: u64,
        expected_profit: i64,
    ) -> bool {
        match self.simulate_path_execution(path, input_amount) {
            Ok((final_amount, _)) => {
                let total_gas = path.iter()
                    .map(|(pool_id, _)| {
                        self.pool_cache.read()
                            .get(pool_id)
                            .map(|p| self.calculate_gas_cost(&p.pool_type))
                            .unwrap_or(0)
                    })
                    .sum::<u64>();
                
                let actual_profit = final_amount as i64 - input_amount as i64 - total_gas as i64;
                
                (actual_profit as f64) >= (expected_profit as f64 * 0.95)
            }
            Err(_) => false,
        }
    }

    pub fn get_path_metadata(
        &self,
        path: &[(PoolId, bool)],
    ) -> Vec<(PoolType, TokenMint, TokenMint, u16)> {
        let pool_cache = self.pool_cache.read();
        
        path.iter()
            .filter_map(|(pool_id, is_reverse)| {
                pool_cache.get(pool_id).map(|pool| {
                    let (token_in, token_out) = if *is_reverse {
                        (pool.token_b, pool.token_a)
                    } else {
                        (pool.token_a, pool.token_b)
                    };
                    (pool.pool_type, token_in, token_out, pool.fee_bps)
                })
            })
            .collect()
    }

    pub fn calculate_minimum_profitable_amount(
        &self,
        path: &[(PoolId, bool)],
        target_profit: u64,
    ) -> Option<u64> {
        let total_gas = self.calculate_path_gas_cost(path);
        let total_fees_bps = self.calculate_path_fees(path);
        
        let min_multiplier = 1.0 + (total_fees_bps as f64 / 10000.0);
        let break_even = (total_gas as f64 * min_multiplier) as u64;
        let min_amount = break_even + target_profit;
        
        let mut low = min_amount;
        let mut high = min_amount * 10;
        
        while low < high {
            let mid = (low + high) / 2;
            
            match self.simulate_path_execution(path, mid) {
                Ok((output, _)) => {
                    let profit = output.saturating_sub(mid).saturating_sub(total_gas);
                    
                    if profit >= target_profit {
                        high = mid;
                    } else {
                        low = mid + 1;
                    }
                }
                Err(_) => {
                    low = mid + 1;
                }
            }
        }
        
        if low < min_amount * 10 {
            Some(low)
        } else {
            None
        }
    }

    fn calculate_path_gas_cost(&self, path: &[(PoolId, bool)]) -> u64 {
        let pool_cache = self.pool_cache.read();
        
        path.iter()
            .filter_map(|(pool_id, _)| {
                pool_cache.get(pool_id)
                    .map(|pool| self.calculate_gas_cost(&pool.pool_type))
            })
            .sum()
    }

    fn calculate_path_fees(&self, path: &[(PoolId, bool)]) -> u64 {
        let pool_cache = self.pool_cache.read();
        
        path.iter()
            .filter_map(|(pool_id, _)| {
                pool_cache.get(pool_id).map(|pool| pool.fee_bps as u64)
            })
            .sum()
    }

    pub fn optimize_path_amount(
        &self,
        path: &[(PoolId, bool)],
        available_amount: u64,
    ) -> u64 {
        let pool_cache = self.pool_cache.read();
        let mut max_amount = available_amount;
        
        for (i, &(pool_id, is_reverse)) in path.iter().enumerate() {
            if let Some(pool) = pool_cache.get(&pool_id) {
                let (reserve_in, reserve_out) = if is_reverse {
                    (pool.reserve_b, pool.reserve_a)
                } else {
                    (pool.reserve_a, pool.reserve_b)
                };
                
                let max_input = reserve_in / 10;
                let _max_output = reserve_out / 10;
                
                if i == 0 {
                    max_amount = max_amount.min(max_input);
                } else {
                    match self.simulate_path_execution(&path[..i], max_amount) {
                        Ok((intermediate_amount, _)) => {
                            let scaled_max = (max_input as f64 * 
                                (max_amount as f64 / intermediate_amount as f64)) as u64;
                            max_amount = max_amount.min(scaled_max);
                        }
                        Err(_) => {
                            max_amount /= 2;
                        }
                    }
                }
            }
        }
        
        max_amount.saturating_mul(95) / 100
    }

    pub fn prune_stale_pools(&self, _max_age_slots: u64, _current_slot: u64) {
        let mut graph = self.graph.write();
        let mut pool_cache = self.pool_cache.write();
        let mut sqrt_cache = self.sqrt_price_cache.write();
        
        let stale_pools: Vec<PoolId> = pool_cache.iter()
            .filter_map(|(pool_id, pool)| {
                if pool.reserve_a == 0 || pool.reserve_b == 0 {
                    Some(*pool_id)
                } else {
                    None
                }
            })
            .collect();
        
        for pool_id in stale_pools {
            pool_cache.remove(&pool_id);
            sqrt_cache.remove(&pool_id);
            
            for edges in graph.values_mut() {
                edges.retain(|edge| edge.pool_id != pool_id);
            }
        }
        
        graph.retain(|_, edges| !edges.is_empty());
    }

    pub fn get_stats(&self) -> (usize, usize, usize) {
        let graph = self.graph.read();
        let pool_cache = self.pool_cache.read();
        
        let num_tokens = graph.len();
        let num_pools = pool_cache.len();
        let num_edges = graph.values().map(|v| v.len()).sum();
        
        (num_tokens, num_pools, num_edges)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_path_finding() {
        let finder = GeodesicPathFinder::new();
        let token_a = TokenMint(Pubkey::new_unique());
        let token_b = TokenMint(Pubkey::new_unique());
        
        finder.update_pool(PoolEdge {
            pool_id: PoolId(Pubkey::new_unique()),
            pool_type: PoolType::RaydiumV4,
            token_a,
            token_b,
            reserve_a: 1_000_000_000,
            reserve_b: 2_000_000_000,
            fee_bps: 25,
            tick_spacing: None,
            current_tick: None,
            liquidity: None,
            sqrt_price: None,
        });
        
        let path = finder.find_optimal_path(
            token_a,
            100_000_000,
            &[token_b],
        );
        
        assert!(path.is_some());
    }
}
