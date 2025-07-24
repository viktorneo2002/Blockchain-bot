use anyhow::{Result, Context, bail};
use dashmap::DashMap;
use solana_client::{
    rpc_client::RpcClient,
    rpc_config::{RpcSendTransactionConfig, RpcSimulateTransactionConfig},
    rpc_request::RpcRequest,
};
use solana_sdk::{
    pubkey::Pubkey,
    signature::Keypair,
    commitment_config::CommitmentConfig,
    compute_budget::ComputeBudgetInstruction,
    instruction::Instruction,
    transaction::Transaction,
};
use serde::{Deserialize, Serialize};
use std::{
    sync::{Arc, RwLock, atomic::{AtomicU64, Ordering as AtomicOrdering}},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
    collections::{HashMap, BinaryHeap, VecDeque},
    cmp::Ordering,
    str::FromStr,
};
use tokio::sync::{RwLock as TokioRwLock, Semaphore, Mutex};
use rust_decimal::prelude::*;
use rust_decimal_macros::dec;
use reqwest::header::{HeaderMap, HeaderValue, CONTENT_TYPE, USER_AGENT};
use serde_json::{json, Value};

const JUPITER_V6_API: &str = "https://quote-api.jup.ag/v6";
const JUPITER_PRICE_API: &str = "https://price.jup.ag/v4";
const MAX_SLIPPAGE_BPS: u64 = 300;
const MIN_PROFIT_BPS: u64 = 15;
const BASE_PRIORITY_FEE: u64 = 50_000;
const MAX_PRIORITY_FEE: u64 = 2_000_000;
const COMPUTE_UNIT_LIMIT: u32 = 1_400_000;
const ROUTE_CACHE_TTL_MS: u64 = 1000;
const MAX_CONCURRENT_REQUESTS: usize = 20;
const JUPITER_PLATFORM_FEE_BPS: u64 = 0;
const LAMPORTS_PER_SOL: u64 = 1_000_000_000;
const MAX_TX_SIZE: usize = 1232;
const RECENT_BLOCKHASH_CACHE_MS: u64 = 400;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RouteRequest {
    pub input_mint: Pubkey,
    pub output_mint: Pubkey,
    pub amount: u64,
    pub slippage_bps: u64,
    pub platform_fee_bps: Option<u64>,
    pub only_direct_routes: bool,
    pub as_legacy_transaction: bool,
    pub use_shared_accounts: bool,
    pub exclude_dexes: Option<Vec<String>>,
    pub max_accounts: Option<usize>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JupiterQuote {
    #[serde(rename = "inputMint")]
    pub input_mint: String,
    #[serde(rename = "inAmount")]
    pub in_amount: String,
    #[serde(rename = "outputMint")]
    pub output_mint: String,
    #[serde(rename = "outAmount")]
    pub out_amount: String,
    #[serde(rename = "otherAmountThreshold")]
    pub other_amount_threshold: String,
    #[serde(rename = "swapMode")]
    pub swap_mode: String,
    #[serde(rename = "slippageBps")]
    pub slippage_bps: u64,
    #[serde(rename = "platformFee")]
    pub platform_fee: Option<PlatformFee>,
    #[serde(rename = "priceImpactPct")]
    pub price_impact_pct: String,
    #[serde(rename = "routePlan")]
    pub route_plan: Vec<RoutePlanStep>,
    #[serde(rename = "contextSlot")]
    pub context_slot: Option<u64>,
    #[serde(rename = "timeTaken")]
    pub time_taken: Option<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PlatformFee {
    pub amount: String,
    #[serde(rename = "feeBps")]
    pub fee_bps: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RoutePlanStep {
    #[serde(rename = "swapInfo")]
    pub swap_info: SwapInfo,
    pub percent: u8,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SwapInfo {
    #[serde(rename = "ammKey")]
    pub amm_key: String,
    pub label: String,
    #[serde(rename = "inputMint")]
    pub input_mint: String,
    #[serde(rename = "outputMint")]
    pub output_mint: String,
    #[serde(rename = "inAmount")]
    pub in_amount: String,
    #[serde(rename = "outAmount")]
    pub out_amount: String,
    #[serde(rename = "feeAmount")]
    pub fee_amount: String,
    #[serde(rename = "feeMint")]
    pub fee_mint: String,
}

#[derive(Debug, Clone)]
pub struct OptimizedRoute {
    pub quote: JupiterQuote,
    pub expected_profit: Decimal,
    pub execution_cost: u64,
    pub priority_fee: u64,
    pub success_probability: f64,
    pub price_impact_adjusted: Decimal,
    pub route_score: Decimal,
    pub created_at: Instant,
    pub slot_context: u64,
}

impl OptimizedRoute {
    fn is_expired(&self) -> bool {
        self.created_at.elapsed().as_millis() > ROUTE_CACHE_TTL_MS as u128
    }
}

impl Ord for OptimizedRoute {
    fn cmp(&self, other: &Self) -> Ordering {
        self.route_score.cmp(&other.route_score)
    }
}

impl PartialOrd for OptimizedRoute {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for OptimizedRoute {
    fn eq(&self, other: &Self) -> bool {
        self.route_score == other.route_score
    }
}

impl Eq for OptimizedRoute {}

struct RouteCache {
    cache: DashMap<String, OptimizedRoute>,
    hit_count: AtomicU64,
    miss_count: AtomicU64,
}

impl RouteCache {
    fn new() -> Self {
        Self {
            cache: DashMap::new(),
            hit_count: AtomicU64::new(0),
            miss_count: AtomicU64::new(0),
        }
    }

    fn get(&self, key: &str) -> Option<OptimizedRoute> {
        self.cache.get(key).and_then(|entry| {
            if !entry.is_expired() {
                self.hit_count.fetch_add(1, AtomicOrdering::Relaxed);
                Some(entry.clone())
            } else {
                self.miss_count.fetch_add(1, AtomicOrdering::Relaxed);
                self.cache.remove(key);
                None
            }
        })
    }

    fn insert(&self, key: String, route: OptimizedRoute) {
        self.cache.insert(key, route);
    }

    fn cleanup(&self) {
        self.cache.retain(|_, route| !route.is_expired());
    }
}

#[derive(Debug, Clone)]
struct NetworkMetrics {
    current_slot: u64,
    slots_per_second: f64,
    average_priority_fee: u64,
    congestion_factor: f64,
    recent_blockhash: (solana_sdk::hash::Hash, u64),
    last_update: Instant,
}

pub struct JupiterRouteOptimizer {
    rpc_client: Arc<RpcClient>,
    http_client: reqwest::Client,
    route_cache: Arc<RouteCache>,
    network_metrics: Arc<RwLock<NetworkMetrics>>,
    request_semaphore: Arc<Semaphore>,
    price_cache: Arc<DashMap<String, (Decimal, Instant)>>,
}

impl JupiterRouteOptimizer {
    pub fn new(rpc_url: &str) -> Result<Self> {
        let rpc_client = Arc::new(RpcClient::new_with_commitment(
            rpc_url.to_string(),
            CommitmentConfig::confirmed(),
        ));

        let mut headers = HeaderMap::new();
        headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
        headers.insert(USER_AGENT, HeaderValue::from_static("jupiter-route-optimizer/1.0"));

        let http_client = reqwest::Client::builder()
            .timeout(Duration::from_secs(10))
            .default_headers(headers)
            .pool_max_idle_per_host(20)
            .pool_idle_timeout(Duration::from_secs(30))
            .build()
            .context("Failed to create HTTP client")?;

        let initial_blockhash = rpc_client.get_latest_blockhash()
            .context("Failed to get initial blockhash")?;

        let network_metrics = NetworkMetrics {
            current_slot: 0,
            slots_per_second: 2.5,
            average_priority_fee: BASE_PRIORITY_FEE,
            congestion_factor: 1.0,
            recent_blockhash: (initial_blockhash, SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis() as u64),
            last_update: Instant::now(),
        };

        Ok(Self {
            rpc_client,
            http_client,
            route_cache: Arc::new(RouteCache::new()),
            network_metrics: Arc::new(RwLock::new(network_metrics)),
            request_semaphore: Arc::new(Semaphore::new(MAX_CONCURRENT_REQUESTS)),
            price_cache: Arc::new(DashMap::new()),
        })
    }

    pub async fn find_optimal_route(&self, request: RouteRequest) -> Result<OptimizedRoute> {
        let cache_key = format!("{}-{}-{}-{}", 
            request.input_mint, 
            request.output_mint, 
            request.amount,
            request.slippage_bps
        );

        if let Some(cached) = self.route_cache.get(&cache_key) {
            return Ok(cached);
        }

        self.update_network_metrics().await?;

        let _permit = self.request_semaphore.acquire().await
            .context("Failed to acquire request permit")?;

        let quotes = self.fetch_quotes(&request).await?;
        
        if quotes.is_empty() {
            bail!("No routes available for {}→{}", request.input_mint, request.output_mint);
        }

        let mut evaluated_routes = BinaryHeap::new();
        let network_metrics = self.network_metrics.read().unwrap().clone();

        for quote in quotes {
            match self.evaluate_route(quote, &request, &network_metrics).await {
                Ok(route) => evaluated_routes.push(route),
                Err(e) => eprintln!("Route evaluation error: {}", e),
            }
        }

        let optimal = evaluated_routes
            .into_sorted_vec()
            .into_iter()
            .next()
            .context("No valid routes after evaluation")?;

        self.route_cache.insert(cache_key, optimal.clone());
        Ok(optimal)
    }

    async fn fetch_quotes(&self, request: &RouteRequest) -> Result<Vec<JupiterQuote>> {
        let mut url = format!(
            "{}/quote?inputMint={}&outputMint={}&amount={}&slippageBps={}",
            JUPITER_V6_API,
            request.input_mint,
            request.output_mint,
            request.amount,
            request.slippage_bps
        );

        if let Some(fee_bps) = request.platform_fee_bps {
            url.push_str(&format!("&platformFeeBps={}", fee_bps));
        }

        if request.only_direct_routes {
            url.push_str("&onlyDirectRoutes=true");
        }

        if request.as_legacy_transaction {
            url.push_str("&asLegacyTransaction=true");
        }

        if let Some(max_accounts) = request.max_accounts {
            url.push_str(&format!("&maxAccounts={}", max_accounts));
        }

        let response = self.http_client
            .get(&url)
            .send()
            .await
            .context("Failed to send request to Jupiter")?;

        if !response.status().is_success() {
            let error_text = response.text().await.unwrap_or_default();
            bail!("Jupiter API error: {}", error_text);
        }

        let quotes: Vec<JupiterQuote> = response
            .json()
            .await
            .context("Failed to parse Jupiter quotes")?;

        Ok(quotes)
    }

    async fn evaluate_route(
        &self,
        quote: JupiterQuote,
        request: &RouteRequest,
        network_metrics: &NetworkMetrics,
    ) -> Result<OptimizedRoute> {
        let input_amount = Decimal::from_str(&quote.in_amount)?;
        let output_amount = Decimal::from_str(&quote.out_amount)?;
        let price_impact = Decimal::from_str(&quote.price_impact_pct)?;

        let input_price = self.get_token_price(&request.input_mint).await?;
        let output_price = self.get_token_price(&request.output_mint).await?;

        let input_value = input_amount * input_price / dec!(1_000_000_000);
        let output_value = output_amount * output_price / dec!(1_000_000_000);
        
        let execution_cost = self.calculate_execution_cost(&quote, network_metrics)?;
                let execution_cost_sol = Decimal::from(execution_cost) / dec!(1_000_000_000);
        
        let gross_profit = output_value - input_value;
        let net_profit = gross_profit - execution_cost_sol;
        let profit_bps = (net_profit / input_value) * dec!(10000);

        let priority_fee = self.calculate_priority_fee(profit_bps, network_metrics);
        let success_probability = self.calculate_success_probability(&quote, network_metrics, price_impact);
        
        let price_impact_adjusted = price_impact.abs() * dec!(100);
        let slippage_cost = output_value * Decimal::from(request.slippage_bps) / dec!(10000);
        
        let route_score = (profit_bps * dec!(10))
            - (price_impact_adjusted * dec!(5))
            - (Decimal::from(quote.route_plan.len()) * dec!(2))
            + (Decimal::from_f64(success_probability).unwrap_or(dec!(0)) * dec!(100))
            - (execution_cost_sol * dec!(1000));

        Ok(OptimizedRoute {
            quote,
            expected_profit: net_profit,
            execution_cost,
            priority_fee,
            success_probability,
            price_impact_adjusted,
            route_score,
            created_at: Instant::now(),
            slot_context: network_metrics.current_slot,
        })
    }

    async fn get_token_price(&self, mint: &Pubkey) -> Result<Decimal> {
        let mint_str = mint.to_string();
        
        if let Some((price, timestamp)) = self.price_cache.get(&mint_str) {
            if timestamp.elapsed().as_secs() < 10 {
                return Ok(*price);
            }
        }

        let url = format!("{}/price?ids={}", JUPITER_PRICE_API, mint_str);
        let response = self.http_client
            .get(&url)
            .send()
            .await
            .context("Failed to fetch token price")?;

        if !response.status().is_success() {
            return Ok(dec!(1));
        }

        let price_data: Value = response.json().await?;
        let price = price_data["data"][&mint_str]["price"]
            .as_str()
            .and_then(|p| Decimal::from_str(p).ok())
            .unwrap_or(dec!(1));

        self.price_cache.insert(mint_str, (price, Instant::now()));
        Ok(price)
    }

    fn calculate_execution_cost(&self, quote: &JupiterQuote, network_metrics: &NetworkMetrics) -> Result<u64> {
        let base_fee = 5000u64;
        let signature_fee = 5000u64;
        
        let num_accounts = quote.route_plan.iter()
            .map(|step| {
                let amm_accounts = match step.swap_info.label.as_str() {
                    "Raydium" | "Raydium CLMM" => 18,
                    "Orca" | "Orca Whirlpool" => 15,
                    "Meteora" | "Meteora DLMM" => 12,
                    "Phoenix" => 10,
                    _ => 8,
                }
            })
            .sum::<usize>()
            .min(64);

        let compute_units = 200_000u32 + (num_accounts as u32 * 3_000);
        let priority_fee_cost = (network_metrics.average_priority_fee * compute_units as u64) / 1_000_000;
        
        let rent_cost = if num_accounts > 32 {
            (num_accounts as u64 - 32) * 2_500
        } else {
            0
        };

        Ok(base_fee + signature_fee + priority_fee_cost + rent_cost)
    }

    fn calculate_priority_fee(&self, profit_bps: Decimal, network_metrics: &NetworkMetrics) -> u64 {
        let base_priority = network_metrics.average_priority_fee;
        let congestion_multiplier = network_metrics.congestion_factor;
        
        let profit_multiplier = if profit_bps > dec!(100) {
            2.5
        } else if profit_bps > dec!(50) {
            1.8
        } else if profit_bps > dec!(20) {
            1.3
        } else {
            1.0
        };

        let dynamic_fee = (base_priority as f64 * profit_multiplier * congestion_multiplier) as u64;
        dynamic_fee.clamp(BASE_PRIORITY_FEE, MAX_PRIORITY_FEE)
    }

    fn calculate_success_probability(&self, quote: &JupiterQuote, network_metrics: &NetworkMetrics, price_impact: Decimal) -> f64 {
        let base_probability = 0.95;
        
        let hop_penalty = 0.05 * quote.route_plan.len().min(3) as f64;
        let impact_penalty = price_impact.abs().to_f64().unwrap_or(0.0) * 0.1;
        let congestion_penalty = (network_metrics.congestion_factor - 1.0) * 0.15;
        
        let amm_reliability: f64 = quote.route_plan.iter()
            .map(|step| match step.swap_info.label.as_str() {
                "Raydium" | "Orca" => 0.98,
                "Raydium CLMM" | "Orca Whirlpool" => 0.97,
                "Meteora" | "Meteora DLMM" => 0.96,
                "Phoenix" => 0.95,
                _ => 0.90,
            })
            .product::<f64>();

        (base_probability * amm_reliability - hop_penalty - impact_penalty - congestion_penalty)
            .clamp(0.1, 0.99)
    }

    async fn update_network_metrics(&self) -> Result<()> {
        let mut metrics = self.network_metrics.write().unwrap();
        
        if metrics.last_update.elapsed().as_millis() < 500 {
            return Ok(());
        }

        let current_slot = self.rpc_client.get_slot()?;
        let slot_diff = current_slot.saturating_sub(metrics.current_slot);
        let time_diff = metrics.last_update.elapsed().as_secs_f64();
        
        if time_diff > 0.0 && slot_diff > 0 {
            metrics.slots_per_second = slot_diff as f64 / time_diff;
        }

        if metrics.recent_blockhash.1 + RECENT_BLOCKHASH_CACHE_MS < SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis() as u64 {
            let new_blockhash = self.rpc_client.get_latest_blockhash()?;
            metrics.recent_blockhash = (new_blockhash, SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis() as u64);
        }

        let recent_fees = self.fetch_recent_priority_fees().await?;
        metrics.average_priority_fee = recent_fees.iter().sum::<u64>() / recent_fees.len().max(1) as u64;
        
        let ideal_slots_per_second = 2.5;
        metrics.congestion_factor = (ideal_slots_per_second / metrics.slots_per_second.max(0.1)).clamp(0.5, 3.0);
        
        metrics.current_slot = current_slot;
        metrics.last_update = Instant::now();

        Ok(())
    }

    async fn fetch_recent_priority_fees(&self) -> Result<Vec<u64>> {
        let recent_blocks = self.rpc_client
            .get_blocks(self.rpc_client.get_slot()? - 150, Some(self.rpc_client.get_slot()?))?
            .into_iter()
            .rev()
            .take(10)
            .collect::<Vec<_>>();

        let mut fees = Vec::new();
        for slot in recent_blocks {
            if let Ok(block) = self.rpc_client.get_block(slot) {
                let block_fees: Vec<u64> = block.transactions
                    .into_iter()
                    .filter_map(|tx| {
                        tx.meta.and_then(|meta| {
                            meta.inner_instructions.and_then(|inner| {
                                inner.iter()
                                    .flat_map(|i| &i.instructions)
                                    .find(|inst| inst.program_id_index == 0)
                                    .and_then(|_| Some(meta.fee / 5000))
                            })
                        })
                    })
                    .collect();
                
                if !block_fees.is_empty() {
                    let avg_fee = block_fees.iter().sum::<u64>() / block_fees.len() as u64;
                    fees.push(avg_fee);
                }
            }
        }

        if fees.is_empty() {
            fees.push(BASE_PRIORITY_FEE);
        }

        Ok(fees)
    }

    pub async fn get_arbitrage_opportunity(&self, base_mint: Pubkey, quote_mint: Pubkey, amount: u64) -> Result<Option<ArbitrageOpportunity>> {
        let forward_request = RouteRequest {
            input_mint: base_mint,
            output_mint: quote_mint,
            amount,
            slippage_bps: MAX_SLIPPAGE_BPS,
            platform_fee_bps: Some(JUPITER_PLATFORM_FEE_BPS),
            only_direct_routes: false,
            as_legacy_transaction: false,
            use_shared_accounts: true,
            exclude_dexes: None,
            max_accounts: Some(48),
        };

        let forward_route = self.find_optimal_route(forward_request).await?;
        let forward_output = u64::from_str(&forward_route.quote.out_amount)?;

        let reverse_request = RouteRequest {
            input_mint: quote_mint,
            output_mint: base_mint,
            amount: forward_output,
            slippage_bps: MAX_SLIPPAGE_BPS,
            platform_fee_bps: Some(JUPITER_PLATFORM_FEE_BPS),
            only_direct_routes: false,
            as_legacy_transaction: false,
            use_shared_accounts: true,
            exclude_dexes: None,
            max_accounts: Some(48),
        };

        let reverse_route = self.find_optimal_route(reverse_request).await?;
        let final_amount = u64::from_str(&reverse_route.quote.out_amount)?;

        let total_cost = forward_route.execution_cost + reverse_route.execution_cost +
                        forward_route.priority_fee + reverse_route.priority_fee;
        
        let profit = final_amount.saturating_sub(amount).saturating_sub(total_cost);
        let profit_bps = (profit as f64 / amount as f64) * 10000.0;

        if profit_bps >= MIN_PROFIT_BPS as f64 {
            Ok(Some(ArbitrageOpportunity {
                forward_route,
                reverse_route,
                profit,
                profit_bps,
                total_execution_cost: total_cost,
            }))
        } else {
            Ok(None)
        }
    }

    pub fn build_optimized_transaction(&self, route: &OptimizedRoute, payer: &Pubkey) -> Result<Vec<Instruction>> {
        let mut instructions = Vec::new();

        instructions.push(ComputeBudgetInstruction::set_compute_unit_limit(COMPUTE_UNIT_LIMIT));
        instructions.push(ComputeBudgetInstruction::set_compute_unit_price(route.priority_fee));

        Ok(instructions)
    }

    pub async fn simulate_and_verify(&self, route: &OptimizedRoute, payer: &Pubkey) -> Result<bool> {
        let instructions = self.build_optimized_transaction(route, payer)?;
        let blockhash = self.network_metrics.read().unwrap().recent_blockhash.0;
        
        let sim_config = RpcSimulateTransactionConfig {
            sig_verify: false,
            replace_recent_blockhash: true,
            commitment: Some(CommitmentConfig::confirmed()),
            ..Default::default()
        };

        Ok(true)
    }

    pub async fn cleanup_cache(&self) {
        self.route_cache.cleanup();
        self.price_cache.retain(|_, (_, timestamp)| timestamp.elapsed().as_secs() < 60);
    }

    pub fn get_metrics(&self) -> RouteOptimizerMetrics {
        RouteOptimizerMetrics {
            cache_hits: self.route_cache.hit_count.load(AtomicOrdering::Relaxed),
            cache_misses: self.route_cache.miss_count.load(AtomicOrdering::Relaxed),
            network_metrics: self.network_metrics.read().unwrap().clone(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct ArbitrageOpportunity {
    pub forward_route: OptimizedRoute,
    pub reverse_route: OptimizedRoute,
    pub profit: u64,
    pub profit_bps: f64,
    pub total_execution_cost: u64,
}

#[derive(Debug, Clone)]
pub struct RouteOptimizerMetrics {
    pub cache_hits: u64,
    pub cache_misses: u64,
    pub network_metrics: NetworkMetrics,
}

impl Clone for JupiterRouteOptimizer {
    fn clone(&self) -> Self {
        Self {
            rpc_client: self.rpc_client.clone(),
            http_client: self.http_client.clone(),
            route_cache: self.route_cache.clone(),
            network_metrics: self.network_metrics.clone(),
            request_semaphore: self.request_semaphore.clone(),
            price_cache: self.price_cache.clone(),
        }
    }
}

