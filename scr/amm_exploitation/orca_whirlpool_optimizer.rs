use anchor_lang::prelude::*;
use arrayref::{array_ref, array_refs};
use borsh::{BorshDeserialize, BorshSerialize};
use primitive_types::U256;
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::{
    account::Account,
    commitment_config::CommitmentConfig,
    pubkey::Pubkey,
    signature::Keypair,
};
use std::{
    collections::{HashMap, BinaryHeap},
    sync::Arc,
    cmp::Ordering,
    str::FromStr,
};
use tokio::sync::RwLock;
use anyhow::Result;

const WHIRLPOOL_PROGRAM_ID: &str = "whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc";
const TICK_SPACING_SEED: &[u8] = b"tick_array";
const POSITION_SEED: &[u8] = b"position";
const MIN_TICK_INDEX: i32 = -443636;
const MAX_TICK_INDEX: i32 = 443636;
const TICK_ARRAY_SIZE: i32 = 88;
const FEE_RATE_MUL_VALUE: u128 = 1_000_000;
const PROTOCOL_FEE_RATE_MUL_VALUE: u128 = 10_000;
const Q64_RESOLUTION: u128 = 1 << 64;
const MAX_SQRT_PRICE: u128 = 79226673515401279992447579055;
const MIN_SQRT_PRICE: u128 = 4295048016;

#[derive(Clone, Debug, BorshSerialize, BorshDeserialize)]
pub struct WhirlpoolState {
    pub liquidity: u128,
    pub sqrt_price_x64: u128,
    pub tick_current_index: i32,
    pub protocol_fee_owed_a: u64,
    pub protocol_fee_owed_b: u64,
    pub token_mint_a: Pubkey,
    pub token_mint_b: Pubkey,
    pub token_vault_a: Pubkey,
    pub token_vault_b: Pubkey,
    pub fee_growth_global_a_x64: u128,
    pub fee_growth_global_b_x64: u128,
    pub fee_rate: u16,
    pub protocol_fee_rate: u16,
    pub whirlpool_bump: [u8; 1],
    pub tick_spacing: u16,
}

#[derive(Clone, Debug)]
pub struct TickData {
    pub initialized: bool,
    pub liquidity_net: i128,
    pub liquidity_gross: u128,
    pub fee_growth_outside_a_x64: u128,
    pub fee_growth_outside_b_x64: u128,
    pub reward_growths_outside_x64: [u128; 3],
}

#[derive(Clone, Debug)]
pub struct SwapQuote {
    pub amount_in: u64,
    pub amount_out: u64,
    pub sqrt_price_limit_x64: u128,
    pub other_amount_threshold: u64,
    pub sqrt_price_x64: u128,
    pub tick_current_index: i32,
    pub is_base_input: bool,
}

#[derive(Clone, Debug)]
pub struct OptimizedRoute {
    pub pools: Vec<Pubkey>,
    pub quotes: Vec<SwapQuote>,
    pub total_amount_out: u64,
    pub price_impact: f64,
    pub total_fees: u64,
    pub execution_price: f64,
}

#[derive(Debug)]
struct SwapStep {
    amount_in: u64,
    amount_out: u64,
    sqrt_price_next_x64: u128,
    fee_amount: u64,
}

impl PartialEq for OptimizedRoute {
    fn eq(&self, other: &Self) -> bool {
        self.total_amount_out == other.total_amount_out
    }
}

impl Eq for OptimizedRoute {}

impl PartialOrd for OptimizedRoute {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for OptimizedRoute {
    fn cmp(&self, other: &Self) -> Ordering {
        self.total_amount_out.cmp(&other.total_amount_out)
    }
}

pub struct OrcaWhirlpoolOptimizer {
    rpc_client: Arc<RpcClient>,
    pool_cache: Arc<RwLock<HashMap<Pubkey, WhirlpoolState>>>,
    tick_cache: Arc<RwLock<HashMap<Pubkey, HashMap<i32, TickData>>>>,
    route_cache: Arc<RwLock<HashMap<(Pubkey, Pubkey, u64), OptimizedRoute>>>,
}

impl OrcaWhirlpoolOptimizer {
    pub fn new(rpc_url: &str) -> Self {
        let rpc_client = Arc::new(RpcClient::new_with_commitment(
            rpc_url.to_string(),
            CommitmentConfig::processed(),
        ));

        Self {
            rpc_client,
            pool_cache: Arc::new(RwLock::new(HashMap::new())),
            tick_cache: Arc::new(RwLock::new(HashMap::new())),
            route_cache: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub async fn refresh_pool_state(&self, pool_address: &Pubkey) -> Result<WhirlpoolState> {
        let account = self.rpc_client.get_account(pool_address).await?;
        let pool_state = self.deserialize_whirlpool(&account.data)?;
        
        let mut cache = self.pool_cache.write().await;
        cache.insert(*pool_address, pool_state.clone());
        
        Ok(pool_state)
    }

    pub async fn get_optimized_swap_quote(
        &self,
        pool_address: &Pubkey,
        amount_in: u64,
        a_to_b: bool,
        slippage_tolerance: u16,
    ) -> Result<SwapQuote> {
        let pool_state = self.get_pool_state(pool_address).await?;
        let sqrt_price_limit = self.calculate_sqrt_price_limit(&pool_state, a_to_b, slippage_tolerance);
        
        let mut quote = SwapQuote {
            amount_in,
            amount_out: 0,
            sqrt_price_limit_x64: sqrt_price_limit,
            other_amount_threshold: 0,
            sqrt_price_x64: pool_state.sqrt_price_x64,
            tick_current_index: pool_state.tick_current_index,
            is_base_input: true,
        };

        self.compute_swap(&pool_state, &mut quote, a_to_b).await?;
        Ok(quote)
    }

    pub async fn find_optimal_route(
        &self,
        token_a: &Pubkey,
        token_b: &Pubkey,
        amount_in: u64,
        max_hops: usize,
    ) -> Result<OptimizedRoute> {
        let cache_key = (*token_a, *token_b, amount_in);
        
        {
            let cache = self.route_cache.read().await;
            if let Some(cached_route) = cache.get(&cache_key) {
                return Ok(cached_route.clone());
            }
        }

        let pools = self.discover_pools(token_a, token_b).await?;
        let mut best_routes = BinaryHeap::new();

        for pool in pools.iter() {
            let quote = self.get_optimized_swap_quote(pool, amount_in, true, 50).await?;
            let route = OptimizedRoute {
                pools: vec![*pool],
                quotes: vec![quote.clone()],
                total_amount_out: quote.amount_out,
                price_impact: self.calculate_price_impact(&quote),
                total_fees: self.calculate_total_fees(&[quote.clone()]),
                execution_price: quote.amount_out as f64 / amount_in as f64,
            };
            best_routes.push(route);
        }

        if max_hops > 1 {
            let multi_hop_routes = self.find_multi_hop_routes(token_a, token_b, amount_in, max_hops).await?;
            for route in multi_hop_routes {
                best_routes.push(route);
            }
        }

        let best_route = best_routes.pop().ok_or_else(|| anyhow::anyhow!("No route found"))?;
        
        let mut cache = self.route_cache.write().await;
        cache.insert(cache_key, best_route.clone());
        
        Ok(best_route)
    }

    async fn compute_swap(
        &self,
        pool: &WhirlpoolState,
        quote: &mut SwapQuote,
        a_to_b: bool,
    ) -> Result<()> {
        let mut amount_remaining = quote.amount_in;
        let mut amount_calculated = 0u64;
        let mut sqrt_price_x64 = pool.sqrt_price_x64;
        let mut liquidity = pool.liquidity;
        let mut tick_current_index = pool.tick_current_index;

        while amount_remaining > 0 && sqrt_price_x64 != quote.sqrt_price_limit_x64 {
            let next_tick_index = self.get_next_initialized_tick_index(
                &pool.token_mint_a,
                tick_current_index,
                pool.tick_spacing,
                a_to_b,
            ).await?;

            let sqrt_price_target = if let Some(next_tick) = next_tick_index {
                let next_sqrt_price = self.sqrt_price_from_tick_index(next_tick);
                if a_to_b {
                    next_sqrt_price.max(quote.sqrt_price_limit_x64)
                } else {
                    next_sqrt_price.min(quote.sqrt_price_limit_x64)
                }
            } else {
                quote.sqrt_price_limit_x64
            };

            let swap_step = self.compute_swap_step(
                sqrt_price_x64,
                sqrt_price_target,
                liquidity,
                amount_remaining,
                pool.fee_rate,
            )?;

            sqrt_price_x64 = swap_step.sqrt_price_next_x64;
            amount_remaining = amount_remaining.saturating_sub(swap_step.amount_in);
            amount_calculated = amount_calculated.saturating_add(swap_step.amount_out);

            if sqrt_price_x64 == sqrt_price_target && next_tick_index.is_some() {
                let next_tick = next_tick_index.unwrap();
                let tick_data = self.get_tick_data(&pool.token_mint_a, next_tick).await?;
                
                if tick_data.initialized {
                    liquidity = if a_to_b {
                        liquidity.saturating_sub(tick_data.liquidity_net.unsigned_abs())
                    } else {
                        liquidity.saturating_add(tick_data.liquidity_net.unsigned_abs())
                    };
                }
                
                tick_current_index = if a_to_b { next_tick - 1 } else { next_tick };
            }
        }

        quote.amount_out = amount_calculated;
        quote.sqrt_price_x64 = sqrt_price_x64;
        quote.tick_current_index = tick_current_index;

        Ok(())
    }

    fn compute_swap_step(
        &self,
        sqrt_price_current_x64: u128,
        sqrt_price_target_x64: u128,
        liquidity: u128,
        amount_remaining: u64,
        fee_rate: u16,
    ) -> Result<SwapStep> {
        let a_to_b = sqrt_price_current_x64 >= sqrt_price_target_x64;
        let amount_remaining_less_fee = self.mul_div_floor(
            amount_remaining as u128,
            FEE_RATE_MUL_VALUE - fee_rate as u128,
            FEE_RATE_MUL_VALUE,
        );

        let (amount_in, amount_out, sqrt_price_next_x64) = if a_to_b {
            let amount_in = self.get_amount_a_delta(
                sqrt_price_target_x64,
                sqrt_price_current_x64,
                liquidity,
                true,
            );
            
            if amount_remaining_less_fee >= amount_in {
                let amount_out = self.get_amount_b_delta(
                    sqrt_price_target_x64,
                    sqrt_price_current_x64,
                    liquidity,
                    false,
                );
                (amount_in, amount_out, sqrt_price_target_x64)
            } else {
                let sqrt_price_next_x64 = self.get_next_sqrt_price_a_up(
                    sqrt_price_current_x64,
                    liquidity,
                    amount_remaining_less_fee,
                    true,
                );
                let amount_out = self.get_amount_b_delta(
                    sqrt_price_next_x64,
                    sqrt_price_current_x64,
                    liquidity,
                    false,
                );
                (amount_remaining_less_fee, amount_out, sqrt_price_next_x64)
            }
        } else {
            let amount_in = self.get_amount_b_delta(
                sqrt_price_current_x64,
                sqrt_price_target_x64,
                liquidity,
                true,
            );
            
            if amount_remaining_less_fee >= amount_in {
                let amount_out = self.get_amount_a_delta(
                    sqrt_price_current_x64,
                    sqrt_price_target_x64,
                    liquidity,
                    false,
                );
                (amount_in, amount_out, sqrt_price_target_x64)
            } else {
                let sqrt_price_next_x64 = self.get_next_sqrt_price_b_up(
                    sqrt_price_current_x64,
                    liquidity,
                    amount_remaining_less_fee,
                    true,
                );
                                let amount_out = self.get_amount_a_delta(
                    sqrt_price_next_x64,
                    sqrt_price_current_x64,
                    liquidity,
                    false,
                );
                (amount_remaining_less_fee, amount_out, sqrt_price_next_x64)
            }
        };

        let fee_amount = amount_remaining.saturating_sub(amount_in);
        
        Ok(SwapStep {
            amount_in,
            amount_out,
            sqrt_price_next_x64,
            fee_amount,
        })
    }

    fn get_amount_a_delta(
        &self,
        sqrt_price_0_x64: u128,
        sqrt_price_1_x64: u128,
        liquidity: u128,
        round_up: bool,
    ) -> u64 {
        let (sqrt_price_lower, sqrt_price_upper) = if sqrt_price_0_x64 > sqrt_price_1_x64 {
            (sqrt_price_1_x64, sqrt_price_0_x64)
        } else {
            (sqrt_price_0_x64, sqrt_price_1_x64)
        };

        let numerator = liquidity.saturating_mul(sqrt_price_upper.saturating_sub(sqrt_price_lower));
        let denominator = sqrt_price_upper.saturating_mul(sqrt_price_lower) >> 64;

        if round_up {
            self.div_round_up(numerator, denominator) as u64
        } else {
            numerator.saturating_div(denominator) as u64
        }
    }

    fn get_amount_b_delta(
        &self,
        sqrt_price_0_x64: u128,
        sqrt_price_1_x64: u128,
        liquidity: u128,
        round_up: bool,
    ) -> u64 {
        let (sqrt_price_lower, sqrt_price_upper) = if sqrt_price_0_x64 > sqrt_price_1_x64 {
            (sqrt_price_1_x64, sqrt_price_0_x64)
        } else {
            (sqrt_price_0_x64, sqrt_price_1_x64)
        };

        let delta = sqrt_price_upper.saturating_sub(sqrt_price_lower);
        if round_up {
            self.mul_div_round_up(liquidity, delta, Q64_RESOLUTION) as u64
        } else {
            self.mul_div_floor(liquidity, delta, Q64_RESOLUTION) as u64
        }
    }

    fn get_next_sqrt_price_a_up(
        &self,
        sqrt_price_x64: u128,
        liquidity: u128,
        amount: u128,
        add: bool,
    ) -> u128 {
        if amount == 0 {
            return sqrt_price_x64;
        }

        let numerator = liquidity << 64;
        let product = amount.saturating_mul(sqrt_price_x64);

        if add {
            let denominator = numerator.saturating_add(product);
            self.mul_div_round_up(numerator, sqrt_price_x64, denominator)
        } else {
            let denominator = numerator.saturating_sub(product);
            self.mul_div_floor(numerator, sqrt_price_x64, denominator)
        }
    }

    fn get_next_sqrt_price_b_up(
        &self,
        sqrt_price_x64: u128,
        liquidity: u128,
        amount: u128,
        add: bool,
    ) -> u128 {
        let delta = self.mul_div_round_up(amount, Q64_RESOLUTION, liquidity);
        if add {
            sqrt_price_x64.saturating_add(delta)
        } else {
            sqrt_price_x64.saturating_sub(delta)
        }
    }

    fn sqrt_price_from_tick_index(&self, tick_index: i32) -> u128 {
        if tick_index >= 0 {
            self.get_sqrt_ratio_at_tick(tick_index)
        } else {
            let ratio = self.get_sqrt_ratio_at_tick(-tick_index);
            (u128::MAX / ratio) + if u128::MAX % ratio == 0 { 0 } else { 1 }
        }
    }

    fn get_sqrt_ratio_at_tick(&self, tick: i32) -> u128 {
        let abs_tick = tick.unsigned_abs();
        let mut ratio = if abs_tick & 0x1 != 0 {
            0xfffcb933bd6fad37aa2d162d1a594001
        } else {
            0x100000000000000000000000000000000
        };

        if abs_tick & 0x2 != 0 { ratio = (ratio * 0xfff97272373d413259a46990580e213a) >> 128; }
        if abs_tick & 0x4 != 0 { ratio = (ratio * 0xfff2e50f5f656932ef12357cf3c7fdcc) >> 128; }
        if abs_tick & 0x8 != 0 { ratio = (ratio * 0xffe5caca7e10e4e61c3624eaa0941cd0) >> 128; }
        if abs_tick & 0x10 != 0 { ratio = (ratio * 0xffcb9843d60f6159c9db58835c926644) >> 128; }
        if abs_tick & 0x20 != 0 { ratio = (ratio * 0xff973b41fa98c081472e6896dfb254c0) >> 128; }
        if abs_tick & 0x40 != 0 { ratio = (ratio * 0xff2ea16466c96a3843ec78b326b52861) >> 128; }
        if abs_tick & 0x80 != 0 { ratio = (ratio * 0xfe5dee046a99a2a811c461f1969c3053) >> 128; }
        if abs_tick & 0x100 != 0 { ratio = (ratio * 0xfcbe86c7900a88aedcffc83b479aa3a4) >> 128; }
        if abs_tick & 0x200 != 0 { ratio = (ratio * 0xf987a7253ac413176f2b074cf7815e54) >> 128; }
        if abs_tick & 0x400 != 0 { ratio = (ratio * 0xf3392b0822b70005940c7a398e4b70f3) >> 128; }
        if abs_tick & 0x800 != 0 { ratio = (ratio * 0xe7159475a2c29b7443b29c7fa6e889d9) >> 128; }
        if abs_tick & 0x1000 != 0 { ratio = (ratio * 0xd097f3bdfd2022b8845ad8f792aa5825) >> 128; }
        if abs_tick & 0x2000 != 0 { ratio = (ratio * 0xa9f746462d870fdf8a65dc1f90e061e5) >> 128; }
        if abs_tick & 0x4000 != 0 { ratio = (ratio * 0x70d869a156d2a1b890bb3df62baf32f7) >> 128; }
        if abs_tick & 0x8000 != 0 { ratio = (ratio * 0x31be135f97d08fd981231505542fcfa6) >> 128; }
        if abs_tick & 0x10000 != 0 { ratio = (ratio * 0x9aa508b5b7a84e1c677de54f3e99bc9) >> 128; }

        ratio >> 32
    }

    fn calculate_sqrt_price_limit(
        &self,
        pool: &WhirlpoolState,
        a_to_b: bool,
        slippage_tolerance: u16,
    ) -> u128 {
        let slippage_multiplier = if a_to_b {
            10000u128.saturating_sub(slippage_tolerance as u128)
        } else {
            10000u128.saturating_add(slippage_tolerance as u128)
        };

        let adjusted_sqrt_price = self.mul_div_floor(
            pool.sqrt_price_x64,
            slippage_multiplier,
            10000,
        );

        if a_to_b {
            adjusted_sqrt_price.max(MIN_SQRT_PRICE)
        } else {
            adjusted_sqrt_price.min(MAX_SQRT_PRICE)
        }
    }

    fn calculate_price_impact(&self, quote: &SwapQuote) -> f64 {
        let expected_price = quote.amount_in as f64 / quote.amount_out as f64;
        let actual_price = self.sqrt_price_x64_to_price(quote.sqrt_price_x64);
        ((actual_price - expected_price).abs() / expected_price) * 100.0
    }

    fn calculate_total_fees(&self, quotes: &[SwapQuote]) -> u64 {
        quotes.iter().map(|q| {
            self.mul_div_floor(q.amount_in as u128, 30, 10000) as u64
        }).sum()
    }

    fn sqrt_price_x64_to_price(&self, sqrt_price_x64: u128) -> f64 {
        let price_x128 = sqrt_price_x64.saturating_mul(sqrt_price_x64);
        price_x128 as f64 / (1u128 << 128) as f64
    }

    async fn get_pool_state(&self, pool_address: &Pubkey) -> Result<WhirlpoolState> {
        let cache = self.pool_cache.read().await;
        if let Some(pool) = cache.get(pool_address) {
            return Ok(pool.clone());
        }
        drop(cache);
        
        self.refresh_pool_state(pool_address).await
    }

    async fn get_tick_data(&self, pool_address: &Pubkey, tick_index: i32) -> Result<TickData> {
        let cache = self.tick_cache.read().await;
        if let Some(pool_ticks) = cache.get(pool_address) {
            if let Some(tick) = pool_ticks.get(&tick_index) {
                return Ok(tick.clone());
            }
        }
        drop(cache);

        let tick_array_start_index = self.tick_index_to_tick_array_start_index(tick_index, 64);
        let tick_array_address = self.get_tick_array_address(pool_address, tick_array_start_index).await?;
        let account = self.rpc_client.get_account(&tick_array_address).await?;
        
        let tick_data = self.deserialize_tick(&account.data, tick_index)?;
        
        let mut cache = self.tick_cache.write().await;
        cache.entry(*pool_address).or_insert_with(HashMap::new).insert(tick_index, tick_data.clone());
        
        Ok(tick_data)
    }

    async fn get_next_initialized_tick_index(
        &self,
        pool_address: &Pubkey,
        tick_index: i32,
        tick_spacing: u16,
        a_to_b: bool,
    ) -> Result<Option<i32>> {
        let search_direction = if a_to_b { -1 } else { 1 };
        let mut current_tick = tick_index + search_direction;
        
        while current_tick >= MIN_TICK_INDEX && current_tick <= MAX_TICK_INDEX {
            if current_tick % tick_spacing as i32 == 0 {
                let tick_data = self.get_tick_data(pool_address, current_tick).await?;
                if tick_data.initialized {
                    return Ok(Some(current_tick));
                }
            }
            current_tick += search_direction * tick_spacing as i32;
        }
        
        Ok(None)
    }

    async fn discover_pools(&self, token_a: &Pubkey, token_b: &Pubkey) -> Result<Vec<Pubkey>> {
        let program_id = Pubkey::from_str(WHIRLPOOL_PROGRAM_ID)?;
        let accounts = self.rpc_client.get_program_accounts(&program_id).await?;
        
        let mut pools = Vec::new();
        for (pubkey, account) in accounts {
            if account.data.len() >= 324 {
                if let Ok(pool) = self.deserialize_whirlpool(&account.data) {
                    if (pool.token_mint_a == *token_a && pool.token_mint_b == *token_b) ||
                       (pool.token_mint_a == *token_b && pool.token_mint_b == *token_a) {
                        pools.push(pubkey);
                    }
                }
            }
        }
        
        Ok(pools)
    }

    async fn find_multi_hop_routes(
        &self,
        token_a: &Pubkey,
        token_b: &Pubkey,
        amount_in: u64,
        max_hops: usize,
    ) -> Result<Vec<OptimizedRoute>> {
        let mut routes = Vec::new();
        let intermediate_tokens = self.get_common_intermediate_tokens().await?;
        
        for intermediate in intermediate_tokens {
            if intermediate == *token_a || intermediate == *token_b {
                continue;
            }
            
            let first_pools = self.discover_pools(token_a, &intermediate).await?;
            let second_pools = self.discover_pools(&intermediate, token_b).await?;
            
            for first_pool in first_pools {
                let first_quote = self.get_optimized_swap_quote(&first_pool, amount_in, true, 50).await?;
                
                for second_pool in &second_pools {
                    let second_quote = self.get_optimized_swap_quote(second_pool, first_quote.amount_out, true, 50).await?;
                    
                    let route = OptimizedRoute {
                        pools: vec![first_pool, *second_pool],
                        quotes: vec![first_quote.clone(), second_quote.clone()],
                        total_amount_out: second_quote.amount_out,
                        price_impact: self.calculate_price_impact(&first_quote) + self.calculate_price_impact(&second_quote),
                        total_fees: self.calculate_total_fees(&[first_quote.clone(), second_quote.clone()]),
                        execution_price: second_quote.amount_out as f64 / amount_in as f64,
                    };
                    
                    if route.price_impact < 5.0 && route.total_amount_out > 0 {
                        routes.push(route);
                    }
                }
            }
        }
        
                routes.sort_by(|a, b| b.total_amount_out.cmp(&a.total_amount_out));
        routes.truncate(10);
        
        Ok(routes)
    }

    async fn get_common_intermediate_tokens(&self) -> Result<Vec<Pubkey>> {
        Ok(vec![
            Pubkey::from_str("So11111111111111111111111111111111111111112")?, // SOL
            Pubkey::from_str("EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v")?, // USDC
            Pubkey::from_str("Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB")?, // USDT
            Pubkey::from_str("7vfCXTUXx5WJV5JADk17DUJ4ksgau7utNKj4b963voxs")?, // ETH
            Pubkey::from_str("mSoLzYCxHdYgdzU16g5QSh3i5K3z3KZK7ytfqcJm7So")?, // mSOL
        ])
    }

    async fn get_tick_array_address(&self, pool_address: &Pubkey, start_index: i32) -> Result<Pubkey> {
        let program_id = Pubkey::from_str(WHIRLPOOL_PROGRAM_ID)?;
        let start_index_bytes = start_index.to_le_bytes();
        
        let (tick_array_pda, _) = Pubkey::find_program_address(
            &[
                TICK_SPACING_SEED,
                pool_address.as_ref(),
                &start_index_bytes,
            ],
            &program_id,
        );
        
        Ok(tick_array_pda)
    }

    fn tick_index_to_tick_array_start_index(&self, tick_index: i32, tick_spacing: u16) -> i32 {
        let ticks_per_array = TICK_ARRAY_SIZE * tick_spacing as i32;
        let array_index = tick_index.div_euclid(ticks_per_array);
        array_index * ticks_per_array
    }

    fn deserialize_whirlpool(&self, data: &[u8]) -> Result<WhirlpoolState> {
        if data.len() < 324 {
            return Err(anyhow::anyhow!("Invalid whirlpool data"));
        }

        let data_slice = array_ref![data, 8, 316];
        let (
            liquidity,
            sqrt_price_x64,
            tick_current_index,
            protocol_fee_owed_a,
            protocol_fee_owed_b,
            token_mint_a,
            token_mint_b,
            token_vault_a,
            token_vault_b,
            fee_growth_global_a_x64,
            fee_growth_global_b_x64,
            fee_rate,
            protocol_fee_rate,
            whirlpool_bump,
            tick_spacing,
        ) = array_refs![
            data_slice,
            16, 16, 4, 8, 8,
            32, 32, 32, 32,
            16, 16, 2, 2, 1, 2
        ];

        Ok(WhirlpoolState {
            liquidity: u128::from_le_bytes(*liquidity),
            sqrt_price_x64: u128::from_le_bytes(*sqrt_price_x64),
            tick_current_index: i32::from_le_bytes(*tick_current_index),
            protocol_fee_owed_a: u64::from_le_bytes(*protocol_fee_owed_a),
            protocol_fee_owed_b: u64::from_le_bytes(*protocol_fee_owed_b),
            token_mint_a: Pubkey::from(*token_mint_a),
            token_mint_b: Pubkey::from(*token_mint_b),
            token_vault_a: Pubkey::from(*token_vault_a),
            token_vault_b: Pubkey::from(*token_vault_b),
            fee_growth_global_a_x64: u128::from_le_bytes(*fee_growth_global_a_x64),
            fee_growth_global_b_x64: u128::from_le_bytes(*fee_growth_global_b_x64),
            fee_rate: u16::from_le_bytes(*fee_rate),
            protocol_fee_rate: u16::from_le_bytes(*protocol_fee_rate),
            whirlpool_bump: *whirlpool_bump,
            tick_spacing: u16::from_le_bytes(*tick_spacing),
        })
    }

    fn deserialize_tick(&self, data: &[u8], tick_index: i32) -> Result<TickData> {
        let tick_offset = ((tick_index % TICK_ARRAY_SIZE) * 93 + 8) as usize;
        
        if data.len() < tick_offset + 93 {
            return Err(anyhow::anyhow!("Invalid tick data"));
        }

        let tick_slice = &data[tick_offset..tick_offset + 93];
        let (
            initialized,
            liquidity_net,
            liquidity_gross,
            fee_growth_outside_a_x64,
            fee_growth_outside_b_x64,
            reward_growths_outside_x64,
        ) = array_refs![
            tick_slice,
            1, 16, 16, 16, 16, 48
        ];

        Ok(TickData {
            initialized: initialized[0] != 0,
            liquidity_net: i128::from_le_bytes(*liquidity_net),
            liquidity_gross: u128::from_le_bytes(*liquidity_gross),
            fee_growth_outside_a_x64: u128::from_le_bytes(*fee_growth_outside_a_x64),
            fee_growth_outside_b_x64: u128::from_le_bytes(*fee_growth_outside_b_x64),
            reward_growths_outside_x64: [
                u128::from_le_bytes(*array_ref![reward_growths_outside_x64, 0, 16]),
                u128::from_le_bytes(*array_ref![reward_growths_outside_x64, 16, 16]),
                u128::from_le_bytes(*array_ref![reward_growths_outside_x64, 32, 16]),
            ],
        })
    }

    fn mul_div_floor(&self, a: u128, b: u128, denominator: u128) -> u128 {
        let product = U256::from(a) * U256::from(b);
        (product / U256::from(denominator)).as_u128()
    }

    fn mul_div_round_up(&self, a: u128, b: u128, denominator: u128) -> u128 {
        let product = U256::from(a) * U256::from(b);
        let result = product / U256::from(denominator);
        if product % U256::from(denominator) > U256::zero() {
            (result + U256::one()).as_u128()
        } else {
            result.as_u128()
        }
    }

    fn div_round_up(&self, a: u128, b: u128) -> u128 {
        if a % b > 0 {
            a / b + 1
        } else {
            a / b
        }
    }

    pub async fn optimize_position_range(
        &self,
        pool_address: &Pubkey,
        liquidity_amount: u128,
        price_range_percentage: u16,
    ) -> Result<(i32, i32)> {
        let pool = self.get_pool_state(pool_address).await?;
        let current_tick = pool.tick_current_index;
        let tick_spacing = pool.tick_spacing as i32;
        
        let price_range_factor = 10000 + price_range_percentage as u128;
        let upper_sqrt_price = self.mul_div_floor(pool.sqrt_price_x64, price_range_factor, 10000);
        let lower_sqrt_price = self.mul_div_floor(pool.sqrt_price_x64, 10000, price_range_factor);
        
        let upper_tick = self.sqrt_price_to_tick(upper_sqrt_price);
        let lower_tick = self.sqrt_price_to_tick(lower_sqrt_price);
        
        let tick_lower = (lower_tick / tick_spacing) * tick_spacing;
        let tick_upper = ((upper_tick / tick_spacing) + 1) * tick_spacing;
        
        Ok((tick_lower.max(MIN_TICK_INDEX), tick_upper.min(MAX_TICK_INDEX)))
    }

    fn sqrt_price_to_tick(&self, sqrt_price_x64: u128) -> i32 {
        let log_base = 1.0001f64.ln();
        let price = self.sqrt_price_x64_to_price(sqrt_price_x64);
        let tick = (price.ln() / log_base) as i32;
        tick.clamp(MIN_TICK_INDEX, MAX_TICK_INDEX)
    }

    pub async fn get_pool_metrics(&self, pool_address: &Pubkey) -> Result<PoolMetrics> {
        let pool = self.get_pool_state(pool_address).await?;
        let price = self.sqrt_price_x64_to_price(pool.sqrt_price_x64);
        let fee_percentage = pool.fee_rate as f64 / 10000.0;
        
        Ok(PoolMetrics {
            current_price: price,
            liquidity: pool.liquidity,
            volume_24h: 0, // Would need historical data
            fee_percentage,
            tick_spacing: pool.tick_spacing,
        })
    }

    pub async fn estimate_gas_cost(&self, num_ticks_crossed: u32) -> u64 {
        const BASE_COMPUTE_UNITS: u64 = 140_000;
        const COMPUTE_UNITS_PER_TICK: u64 = 20_000;
        
        BASE_COMPUTE_UNITS + (num_ticks_crossed as u64 * COMPUTE_UNITS_PER_TICK)
    }

    pub fn validate_swap_parameters(&self, amount_in: u64, slippage_tolerance: u16) -> Result<()> {
        if amount_in == 0 {
            return Err(anyhow::anyhow!("Amount in cannot be zero"));
        }
        
        if slippage_tolerance > 5000 {
            return Err(anyhow::anyhow!("Slippage tolerance too high"));
        }
        
        Ok(())
    }
}

#[derive(Clone, Debug)]
pub struct PoolMetrics {
    pub current_price: f64,
    pub liquidity: u128,
    pub volume_24h: u64,
    pub fee_percentage: f64,
    pub tick_spacing: u16,
}

