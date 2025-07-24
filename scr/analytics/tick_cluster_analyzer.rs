use anchor_lang::prelude::*;
use arrayref::array_ref;
use borsh::{BorshDeserialize, BorshSerialize};
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::{
    account::Account,
    clock::Clock,
    pubkey::Pubkey,
    signature::Keypair,
};
use std::{
    collections::{BTreeMap, HashMap, VecDeque},
    sync::{Arc, RwLock},
    time::{Duration, Instant},
};
use tokio::sync::mpsc;

const TICK_WINDOW_SIZE: usize = 256;
const CLUSTER_MIN_SIZE: usize = 3;
const CLUSTER_MAX_DISTANCE: u64 = 5;
const VOLUME_THRESHOLD: u64 = 1_000_000_000; // 1000 USDC
const TICK_SPACING: u16 = 64;
const BASIS_POINT_MAX: u64 = 10000;
const SQRT_PRICE_LIMIT_X64: u128 = 79228162514264337593543950336;
const MIN_SQRT_PRICE_X64: u128 = 4295128739;
const MAX_SQRT_PRICE_X64: u128 = 1461446703485210103287273052203988822378723970342;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, BorshSerialize, BorshDeserialize)]
pub struct TickData {
    pub tick_index: i32,
    pub sqrt_price_x64: u128,
    pub liquidity_net: i128,
    pub liquidity_gross: u128,
    pub fee_growth_outside_a: u128,
    pub fee_growth_outside_b: u128,
    pub timestamp: u64,
    pub block_height: u64,
}

#[derive(Debug, Clone)]
pub struct TickCluster {
    pub center_tick: i32,
    pub ticks: Vec<TickData>,
    pub total_liquidity: u128,
    pub price_range: (u128, u128),
    pub volume_24h: u64,
    pub concentration_score: f64,
    pub volatility_score: f64,
    pub opportunity_score: f64,
}

#[derive(Debug, Clone)]
pub struct ClusterAnalysis {
    pub cluster_id: u64,
    pub timestamp: u64,
    pub clusters: Vec<TickCluster>,
    pub dominant_cluster: Option<TickCluster>,
    pub liquidity_distribution: HashMap<i32, u128>,
    pub price_impact_map: BTreeMap<i32, f64>,
    pub mev_opportunities: Vec<MevOpportunity>,
}

#[derive(Debug, Clone)]
pub struct MevOpportunity {
    pub opportunity_type: OpportunityType,
    pub tick_range: (i32, i32),
    pub expected_profit: u64,
    pub risk_score: f64,
    pub execution_probability: f64,
    pub gas_estimate: u64,
    pub priority_fee: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OpportunityType {
    Arbitrage,
    Liquidation,
    JitLiquidity,
    BackRun,
    Sandwich,
}

pub struct TickClusterAnalyzer {
    rpc_client: Arc<RpcClient>,
    tick_history: Arc<RwLock<VecDeque<TickData>>>,
    cluster_cache: Arc<RwLock<HashMap<Pubkey, ClusterAnalysis>>>,
    price_oracle: Arc<RwLock<HashMap<Pubkey, u64>>>,
    liquidity_threshold: u128,
    analysis_interval: Duration,
}

impl TickClusterAnalyzer {
    pub fn new(rpc_endpoint: &str) -> Self {
        Self {
            rpc_client: Arc::new(RpcClient::new(rpc_endpoint.to_string())),
            tick_history: Arc::new(RwLock::new(VecDeque::with_capacity(TICK_WINDOW_SIZE))),
            cluster_cache: Arc::new(RwLock::new(HashMap::new())),
            price_oracle: Arc::new(RwLock::new(HashMap::new())),
            liquidity_threshold: 50_000_000_000_000, // 50k USDC equivalent
            analysis_interval: Duration::from_millis(100),
        }
    }

    pub async fn analyze_pool_ticks(&self, pool_address: &Pubkey) -> Result<ClusterAnalysis, Box<dyn std::error::Error>> {
        let account = self.rpc_client.get_account(pool_address).await?;
        let pool_data = self.decode_pool_state(&account)?;
        
        let tick_array_bitmap = self.fetch_tick_array_bitmap(pool_address).await?;
        let active_ticks = self.fetch_active_ticks(pool_address, &tick_array_bitmap).await?;
        
        let clusters = self.identify_clusters(&active_ticks);
        let liquidity_distribution = self.calculate_liquidity_distribution(&active_ticks);
        let price_impact_map = self.calculate_price_impact(&clusters, &pool_data);
        
        let mev_opportunities = self.detect_mev_opportunities(&clusters, &pool_data);
        
        let analysis = ClusterAnalysis {
            cluster_id: self.generate_cluster_id(),
            timestamp: Clock::get()?.unix_timestamp as u64,
            dominant_cluster: self.find_dominant_cluster(&clusters),
            clusters,
            liquidity_distribution,
            price_impact_map,
            mev_opportunities,
        };
        
        self.cluster_cache.write().unwrap().insert(*pool_address, analysis.clone());
        
        Ok(analysis)
    }

    fn identify_clusters(&self, ticks: &[TickData]) -> Vec<TickCluster> {
        let mut clusters = Vec::new();
        let mut sorted_ticks = ticks.to_vec();
        sorted_ticks.sort_by_key(|t| t.tick_index);
        
        let mut i = 0;
        while i < sorted_ticks.len() {
            let mut cluster_ticks = vec![sorted_ticks[i]];
            let mut j = i + 1;
            
            while j < sorted_ticks.len() && 
                  (sorted_ticks[j].tick_index - sorted_ticks[j-1].tick_index) as u64 <= CLUSTER_MAX_DISTANCE {
                cluster_ticks.push(sorted_ticks[j]);
                j += 1;
            }
            
            if cluster_ticks.len() >= CLUSTER_MIN_SIZE {
                let cluster = self.build_cluster(cluster_ticks);
                if cluster.total_liquidity >= self.liquidity_threshold {
                    clusters.push(cluster);
                }
            }
            
            i = j;
        }
        
        clusters
    }

    fn build_cluster(&self, ticks: Vec<TickData>) -> TickCluster {
        let center_tick = ticks[ticks.len() / 2].tick_index;
        let total_liquidity: u128 = ticks.iter().map(|t| t.liquidity_gross).sum();
        
        let price_range = (
            ticks.first().unwrap().sqrt_price_x64,
            ticks.last().unwrap().sqrt_price_x64,
        );
        
        let volume_24h = self.calculate_cluster_volume(&ticks);
        let concentration_score = self.calculate_concentration_score(&ticks, total_liquidity);
        let volatility_score = self.calculate_volatility_score(&ticks);
        let opportunity_score = self.calculate_opportunity_score(
            concentration_score,
            volatility_score,
            volume_24h,
            total_liquidity,
        );
        
        TickCluster {
            center_tick,
            ticks,
            total_liquidity,
            price_range,
            volume_24h,
            concentration_score,
            volatility_score,
            opportunity_score,
        }
    }

    fn calculate_concentration_score(&self, ticks: &[TickData], total_liquidity: u128) -> f64 {
        if ticks.is_empty() || total_liquidity == 0 {
            return 0.0;
        }
        
        let mut gini_sum = 0.0;
        let n = ticks.len() as f64;
        
        for (i, tick) in ticks.iter().enumerate() {
            let liquidity_ratio = tick.liquidity_gross as f64 / total_liquidity as f64;
            gini_sum += (2.0 * (i as f64 + 1.0) - n - 1.0) * liquidity_ratio;
        }
        
        1.0 - (gini_sum / n).abs()
    }

    fn calculate_volatility_score(&self, ticks: &[TickData]) -> f64 {
        if ticks.len() < 2 {
            return 0.0;
        }
        
        let prices: Vec<f64> = ticks.iter()
            .map(|t| self.sqrt_price_to_price(t.sqrt_price_x64))
            .collect();
        
        let mean = prices.iter().sum::<f64>() / prices.len() as f64;
        let variance = prices.iter()
            .map(|p| (p - mean).powi(2))
            .sum::<f64>() / prices.len() as f64;
        
        variance.sqrt() / mean
    }

    fn calculate_opportunity_score(&self, concentration: f64, volatility: f64, volume: u64, liquidity: u128) -> f64 {
        let volume_score = (volume as f64).ln() / 25.0;
        let liquidity_score = (liquidity as f64).ln() / 35.0;
        let volatility_adjusted = volatility.min(0.5) * 2.0;
        
        (concentration * 0.3 + volatility_adjusted * 0.3 + volume_score * 0.2 + liquidity_score * 0.2)
            .min(1.0)
            .max(0.0)
    }

    fn detect_mev_opportunities(&self, clusters: &[TickCluster], pool_data: &PoolState) -> Vec<MevOpportunity> {
        let mut opportunities = Vec::new();
        
        for cluster in clusters {
            if cluster.opportunity_score > 0.7 {
                if let Some(arb_opp) = self.check_arbitrage_opportunity(cluster, pool_data) {
                    opportunities.push(arb_opp);
                }
                
                if cluster.volatility_score > 0.3 {
                    if let Some(sandwich_opp) = self.check_sandwich_opportunity(cluster, pool_data) {
                        opportunities.push(sandwich_opp);
                    }
                }
                
                if cluster.concentration_score > 0.8 {
                    if let Some(jit_opp) = self.check_jit_liquidity_opportunity(cluster, pool_data) {
                        opportunities.push(jit_opp);
                    }
                }
            }
        }
        
        opportunities.sort_by(|a, b| {
            let a_score = a.expected_profit as f64 * a.execution_probability / (1.0 + a.risk_score);
            let b_score = b.expected_profit as f64 * b.execution_probability / (1.0 + b.risk_score);
            b_score.partial_cmp(&a_score).unwrap()
        });
        
        opportunities
    }

    fn check_arbitrage_opportunity(&self, cluster: &TickCluster, pool_data: &PoolState) -> Option<MevOpportunity> {
        let current_price = self.sqrt_price_to_price(pool_data.sqrt_price_x64);
        let cluster_price = self.sqrt_price_to_price(cluster.price_range.0);
        let price_diff = (current_price - cluster_price).abs() / current_price;
        
        if price_diff > 0.002 {
            let swap_amount = self.calculate_optimal_swap_amount(cluster, pool_data);
            let expected_profit = (swap_amount as f64 * price_diff * 0.7) as u64;
            
            if expected_profit > pool_data.protocol_fee_rate {
                return Some(MevOpportunity {
                    opportunity_type: OpportunityType::Arbitrage,
                    tick_range: (cluster.ticks.first().unwrap().tick_index, cluster.ticks.last().unwrap().tick_index),
                    expected_profit,
                    risk_score: 0.3 + cluster.volatility_score * 0.5,
                    execution_probability: 0.85 - cluster.volatility_score * 0.2,
                    gas_estimate: 300_000,
                    priority_fee: expected_profit / 20,
                });
            }
        }
        
        None
    }

    fn check_sandwich_opportunity(&self, cluster: &TickCluster, pool_data: &PoolState) -> Option<MevOpportunity> {
        if cluster.volume_24h < VOLUME_THRESHOLD * 10 {
            return None;
        }
        
        let liquidity_depth = cluster.total_liquidity as f64;
        let price_impact = self.estimate_price_impact(VOLUME_THRESHOLD, liquidity_depth);
        
        if price_impact > 0.001 && price_impact < 0.01 {
            let expected_profit = (VOLUME_THRESHOLD as f64 * price_impact * 0.5) as u64;
            
            return Some(MevOpportunity {
                opportunity_type: OpportunityType::Sandwich,
                tick_range: (cluster.center_tick - 10, cluster.center_tick + 10),
                expected_profit,
                risk_score: 0.6 + price_impact * 10.0,
                execution_probability: 0.7 - price_impact * 5.0,
                gas_estimate: 600_000,
                                priority_fee: expected_profit / 10,
            });
        }
        
        None
    }

    fn check_jit_liquidity_opportunity(&self, cluster: &TickCluster, pool_data: &PoolState) -> Option<MevOpportunity> {
        let tick_spacing_multiplier = (TICK_SPACING as f64 / 64.0).max(1.0);
        let concentration_threshold = 0.85 * tick_spacing_multiplier;
        
        if cluster.concentration_score > concentration_threshold {
            let liquidity_to_add = self.calculate_jit_liquidity_amount(cluster, pool_data);
            let fee_earnings = self.estimate_jit_fee_earnings(liquidity_to_add, cluster.volume_24h, pool_data);
            
            if fee_earnings > 1_000_000 {
                return Some(MevOpportunity {
                    opportunity_type: OpportunityType::JitLiquidity,
                    tick_range: (cluster.center_tick - 2, cluster.center_tick + 2),
                    expected_profit: fee_earnings,
                    risk_score: 0.4 + (1.0 - cluster.concentration_score) * 0.6,
                    execution_probability: 0.9 - cluster.volatility_score * 0.3,
                    gas_estimate: 400_000,
                    priority_fee: fee_earnings / 15,
                });
            }
        }
        
        None
    }

    fn calculate_optimal_swap_amount(&self, cluster: &TickCluster, pool_data: &PoolState) -> u64 {
        let available_liquidity = cluster.total_liquidity as f64;
        let sqrt_k = available_liquidity.sqrt();
        let price_ratio = self.sqrt_price_to_price(cluster.price_range.1) / self.sqrt_price_to_price(cluster.price_range.0);
        
        let optimal_amount = (sqrt_k * price_ratio.sqrt() * 0.3) as u64;
        optimal_amount.min(pool_data.token_a_amount / 10).max(VOLUME_THRESHOLD)
    }

    fn calculate_jit_liquidity_amount(&self, cluster: &TickCluster, pool_data: &PoolState) -> u128 {
        let current_liquidity = cluster.total_liquidity;
        let target_share = 0.15;
        let jit_liquidity = (current_liquidity as f64 * target_share / (1.0 - target_share)) as u128;
        
        jit_liquidity.min(pool_data.liquidity / 5)
    }

    fn estimate_jit_fee_earnings(&self, liquidity: u128, volume: u64, pool_data: &PoolState) -> u64 {
        let fee_rate = pool_data.fee_rate as f64 / BASIS_POINT_MAX as f64;
        let liquidity_share = liquidity as f64 / (pool_data.liquidity + liquidity) as f64;
        let expected_volume_share = volume as f64 * liquidity_share * 0.8;
        
        (expected_volume_share * fee_rate) as u64
    }

    fn estimate_price_impact(&self, amount: u64, liquidity: f64) -> f64 {
        let k = liquidity * liquidity;
        let delta = amount as f64;
        (delta / (liquidity + delta / 2.0)).min(0.5)
    }

    fn calculate_liquidity_distribution(&self, ticks: &[TickData]) -> HashMap<i32, u128> {
        let mut distribution = HashMap::new();
        
        for tick in ticks {
            let bucket = (tick.tick_index / (TICK_SPACING as i32)) * (TICK_SPACING as i32);
            *distribution.entry(bucket).or_insert(0) += tick.liquidity_gross;
        }
        
        distribution
    }

    fn calculate_price_impact(&self, clusters: &[TickCluster], pool_data: &PoolState) -> BTreeMap<i32, f64> {
        let mut impact_map = BTreeMap::new();
        
        for cluster in clusters {
            let center = cluster.center_tick;
            let liquidity = cluster.total_liquidity as f64;
            
            for offset in -10..=10 {
                let tick = center + offset;
                let distance_factor = 1.0 / (1.0 + (offset.abs() as f64 / 5.0));
                let base_impact = self.estimate_price_impact(VOLUME_THRESHOLD, liquidity);
                
                impact_map.insert(tick, base_impact * distance_factor);
            }
        }
        
        impact_map
    }

    fn find_dominant_cluster(&self, clusters: &[TickCluster]) -> Option<TickCluster> {
        clusters.iter()
            .max_by(|a, b| {
                let a_score = a.total_liquidity as f64 * a.opportunity_score;
                let b_score = b.total_liquidity as f64 * b.opportunity_score;
                a_score.partial_cmp(&b_score).unwrap()
            })
            .cloned()
    }

    fn calculate_cluster_volume(&self, ticks: &[TickData]) -> u64 {
        let fee_growth_sum: u128 = ticks.iter()
            .map(|t| t.fee_growth_outside_a + t.fee_growth_outside_b)
            .sum();
        
        (fee_growth_sum / 1000) as u64
    }

    async fn fetch_tick_array_bitmap(&self, pool_address: &Pubkey) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
        let bitmap_address = self.derive_tick_array_bitmap_address(pool_address);
        let account = self.rpc_client.get_account(&bitmap_address).await?;
        Ok(account.data)
    }

    async fn fetch_active_ticks(&self, pool_address: &Pubkey, bitmap: &[u8]) -> Result<Vec<TickData>, Box<dyn std::error::Error>> {
        let mut active_ticks = Vec::new();
        let clock = Clock::get()?;
        
        for (byte_idx, &byte) in bitmap.iter().enumerate() {
            if byte != 0 {
                for bit in 0..8 {
                    if byte & (1 << bit) != 0 {
                        let tick_array_idx = byte_idx * 8 + bit;
                        let tick_array_address = self.derive_tick_array_address(pool_address, tick_array_idx as i32);
                        
                        if let Ok(account) = self.rpc_client.get_account(&tick_array_address).await {
                            let ticks = self.parse_tick_array(&account.data, clock.unix_timestamp as u64)?;
                            active_ticks.extend(ticks);
                        }
                    }
                }
            }
        }
        
        Ok(active_ticks)
    }

    fn parse_tick_array(&self, data: &[u8], timestamp: u64) -> Result<Vec<TickData>, Box<dyn std::error::Error>> {
        let mut ticks = Vec::new();
        let tick_size = 88;
        let header_size = 8;
        
        if data.len() < header_size {
            return Ok(ticks);
        }
        
        let num_ticks = ((data.len() - header_size) / tick_size).min(88);
        
        for i in 0..num_ticks {
            let offset = header_size + i * tick_size;
            if offset + tick_size <= data.len() {
                let tick_data = &data[offset..offset + tick_size];
                
                let tick = TickData {
                    tick_index: i32::from_le_bytes(*array_ref![tick_data, 0, 4]),
                    sqrt_price_x64: u128::from_le_bytes(*array_ref![tick_data, 4, 16]),
                    liquidity_net: i128::from_le_bytes(*array_ref![tick_data, 20, 16]),
                    liquidity_gross: u128::from_le_bytes(*array_ref![tick_data, 36, 16]),
                    fee_growth_outside_a: u128::from_le_bytes(*array_ref![tick_data, 52, 16]),
                    fee_growth_outside_b: u128::from_le_bytes(*array_ref![tick_data, 68, 16]),
                    timestamp,
                    block_height: 0,
                };
                
                if tick.liquidity_gross > 0 {
                    ticks.push(tick);
                }
            }
        }
        
        Ok(ticks)
    }

    fn decode_pool_state(&self, account: &Account) -> Result<PoolState, Box<dyn std::error::Error>> {
        if account.data.len() < 324 {
            return Err("Invalid pool account data".into());
        }
        
        Ok(PoolState {
            bump: account.data[0],
            token_mint_a: Pubkey::new_from_array(*array_ref![account.data, 1, 32]),
            token_mint_b: Pubkey::new_from_array(*array_ref![account.data, 33, 32]),
            token_vault_a: Pubkey::new_from_array(*array_ref![account.data, 65, 32]),
            token_vault_b: Pubkey::new_from_array(*array_ref![account.data, 97, 32]),
            fee_rate: u16::from_le_bytes(*array_ref![account.data, 129, 2]),
            protocol_fee_rate: u16::from_le_bytes(*array_ref![account.data, 131, 2]),
            liquidity: u128::from_le_bytes(*array_ref![account.data, 133, 16]),
            sqrt_price_x64: u128::from_le_bytes(*array_ref![account.data, 149, 16]),
            tick_current: i32::from_le_bytes(*array_ref![account.data, 165, 4]),
            token_a_amount: u64::from_le_bytes(*array_ref![account.data, 169, 8]),
            token_b_amount: u64::from_le_bytes(*array_ref![account.data, 177, 8]),
        })
    }

    fn derive_tick_array_bitmap_address(&self, pool_address: &Pubkey) -> Pubkey {
        let (address, _) = Pubkey::find_program_address(
            &[b"tick_array_bitmap", pool_address.as_ref()],
            &orca_whirlpool::ID,
        );
        address
    }

    fn derive_tick_array_address(&self, pool_address: &Pubkey, tick_array_idx: i32) -> Pubkey {
        let (address, _) = Pubkey::find_program_address(
            &[
                b"tick_array",
                pool_address.as_ref(),
                &tick_array_idx.to_le_bytes(),
            ],
            &orca_whirlpool::ID,
        );
        address
    }

    fn sqrt_price_to_price(&self, sqrt_price_x64: u128) -> f64 {
        let sqrt_price = sqrt_price_x64 as f64 / (1u64 << 32) as f64;
        sqrt_price * sqrt_price
    }

    fn generate_cluster_id(&self) -> u64 {
        use std::time::{SystemTime, UNIX_EPOCH};
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        (nanos & 0xFFFFFFFFFFFFFFFF) as u64
    }

    pub async fn update_price_oracle(&self, token_mint: &Pubkey, price: u64) {
        self.price_oracle.write().unwrap().insert(*token_mint, price);
    }

    pub fn get_cached_analysis(&self, pool_address: &Pubkey) -> Option<ClusterAnalysis> {
        self.cluster_cache.read().unwrap().get(pool_address).cloned()
    }

    pub async fn start_continuous_analysis(&self, pool_addresses: Vec<Pubkey>) -> mpsc::Receiver<ClusterAnalysis> {
        let (tx, rx) = mpsc::channel(100);
        let analyzer = Arc::new(self.clone());
        
        tokio::spawn(async move {
            loop {
                for pool_address in &pool_addresses {
                    if let Ok(analysis) = analyzer.analyze_pool_ticks(pool_address).await {
                        let _ = tx.send(analysis).await;
                    }
                }
                tokio::time::sleep(analyzer.analysis_interval).await;
            }
        });
        
        rx
    }
}

#[derive(Debug, Clone)]
struct PoolState {
    bump: u8,
    token_mint_a: Pubkey,
    token_mint_b: Pubkey,
    token_vault_a: Pubkey,
    token_vault_b: Pubkey,
    fee_rate: u16,
    protocol_fee_rate: u16,
    liquidity: u128,
    sqrt_price_x64: u128,
    tick_current: i32,
    token_a_amount: u64,
    token_b_amount: u64,
}

impl Clone for TickClusterAnalyzer {
    fn clone(&self) -> Self {
        Self {
            rpc_client: Arc::clone(&self.rpc_client),
            tick_history: Arc::clone(&self.tick_history),
            cluster_cache: Arc::clone(&self.cluster_cache),
            price_oracle: Arc::clone(&self.price_oracle),
            liquidity_threshold: self.liquidity_threshold,
            analysis_interval: self.analysis_interval,
        }
    }
}

mod orca_whirlpool {
    use super::*;
    pub const ID: Pubkey = solana_sdk::pubkey!("whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc");
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_tick_cluster_identification() {
        let analyzer = TickClusterAnalyzer::new("https://api.mainnet-beta.solana.com");
        
        let test_ticks = vec![
            TickData {
                tick_index: 100,
                sqrt_price_x64: 1000000000000000000,
                liquidity_net: 1000000,
                liquidity_gross: 1000000,
                fee_growth_outside_a: 0,
                fee_growth_outside_b: 0,
                timestamp: 1000,
                block_height: 1000,
            },
            TickData {
                tick_index: 102,
                sqrt_price_x64: 1000100000000000000,
                liquidity_net: 2000000,
                liquidity_gross: 2000000,
                fee_growth_outside_a: 100,
                fee_growth_outside_b: 100,
                timestamp: 1001,
                block_height: 1001,
            },
            TickData {
                tick_index: 104,
                sqrt_price_x64: 1000200000000000000,
                liquidity_net: 3000000,
                liquidity_gross: 3000000,
                fee_growth_outside_a: 200,
                fee_growth_outside_b: 200,
                timestamp: 1002,
                block_height: 1002,
            },
        ];
        
        let clusters = analyzer.identify_clusters(&test_ticks);
        assert!(!clusters.is_empty());
    }
    
    #[test]
    fn test_price_conversion() {
        let analyzer = TickClusterAnalyzer::new("https://api.mainnet-beta.solana.com");
        let sqrt_price_x64: u128 = 79228162514264337593543950336;
        let price = analyzer.sqrt_price_to_price(sqrt_price_x64);
        assert!(price > 0.0);
    }
}

