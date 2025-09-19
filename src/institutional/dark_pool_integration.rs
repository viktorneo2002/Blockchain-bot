use crate::types::{Order, OrderSide, Route, Pool, Protocol, DarkPoolError, DarkPoolConfig};
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_client::rpc_config::{RpcSendTransactionConfig, RpcSimulateTransactionConfig};
use solana_sdk::{
    pubkey::Pubkey,
    signature::{Keypair, Signature, Signer},
    transaction::Transaction,
    instruction::{Instruction, AccountMeta},
    commitment_config::CommitmentConfig,
    compute_budget::ComputeBudgetInstruction,
    system_instruction,
    message::Message,
    // v0 + LUT
    transaction::VersionedTransaction,
    message::v0::Message as V0Message,
    address_lookup_table::AddressLookupTableAccount,
};
use solana_transaction_status::UiTransactionEncoding;
use spl_associated_token_account::{get_associated_token_address, instruction::create_associated_token_account};
use std::{
    sync::{Arc, atomic::{AtomicU64, AtomicBool, Ordering}},
    collections::HashMap,
    time::{SystemTime, UNIX_EPOCH, Duration},
    str::FromStr,
};
use tokio::{
    sync::{RwLock, Semaphore},
    time::{sleep, interval},
};
use arrayref::array_ref;
use borsh::{BorshSerialize, BorshDeserialize};
use spl_token::instruction as token_instruction;
use log::{info, warn, error, debug};

const DEFAULT_RPC_URL: &str = "https://api.mainnet-beta.solana.com";
const MAX_SLIPPAGE_BPS: u16 = 500; // 5%
const MIN_LIQUIDITY_USD: u64 = 10_000;
const MAX_ROUTE_LENGTH: usize = 3;
const POOL_REFRESH_INTERVAL_MS: u64 = 30_000;
const ORDER_TIMEOUT_MS: u64 = 30_000;
const MAX_CONCURRENT_ORDERS: usize = 10;

// DEX Program IDs
const PHOENIX_PROGRAM_ID: &str = "PhoeNiXZ8ByJGLkxNfZRnkUfjvmuYqLR89jVFowvyE";
const WHIRLPOOL_PROGRAM_ID: &str = "whirLbMiicVdio4qvUfM5KAg6Ct8VwVSgEff7S9uEBk";
const SERUM_PROGRAM_ID: &str = "9xQeWvG816bUx9EPjHmaT23yvVM2ZWbrrpZb9PusVFin";
const RAYDIUM_PROGRAM_ID: &str = "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8";

#[derive(Clone)]
pub struct DarkPoolIntegration {
    rpc_client: Arc<RpcClient>,
    wallet: Arc<Keypair>,
    config: Arc<RwLock<DarkPoolConfig>>,
    pools: Arc<RwLock<HashMap<String, Arc<PoolInfo>>>>,
    pending_orders: Arc<RwLock<HashMap<[u8; 32], Order>>>,
    execution_semaphore: Arc<Semaphore>,
    circuit_breaker: Arc<CircuitBreaker>,
    metrics: Arc<Metrics>,
    shutdown: Arc<AtomicBool>,
}

#[derive(Debug)]
struct PoolInfo {
    address: Pubkey,
    protocol: Protocol,
    base_reserve: AtomicU64,
    quote_reserve: AtomicU64,
    fee_bps: u16,
    last_update: AtomicU64,
    success_count: AtomicU64,
    failure_count: AtomicU64,
}

struct CircuitBreaker {
    failure_count: AtomicU64,
    last_failure_time: AtomicU64,
    is_open: AtomicBool,
    config: DarkPoolConfig,
}

struct Metrics {
    total_orders: AtomicU64,
    successful_orders: AtomicU64,
    failed_orders: AtomicU64,
    total_volume: AtomicU64,
    total_gas_used: AtomicU64,
}

impl DarkPoolIntegration {
    pub async fn new(
        rpc_url: &str,
        wallet: Keypair,
        config: Option<DarkPoolConfig>,
    ) -> Result<Self, DarkPoolError> {
        let rpc_client = Arc::new(RpcClient::new_with_commitment(
            rpc_url.to_string(),
            CommitmentConfig::confirmed(),
        ));
        
        let config = config.unwrap_or_default();
        let circuit_breaker = Arc::new(CircuitBreaker::new(config.clone()));
        
        let integration = Self {
            rpc_client,
            wallet: Arc::new(wallet),
            config: Arc::new(RwLock::new(config)),
            pools: Arc::new(RwLock::new(HashMap::new())),
            pending_orders: Arc::new(RwLock::new(HashMap::new())),
            execution_semaphore: Arc::new(Semaphore::new(MAX_CONCURRENT_ORDERS)),
            circuit_breaker,
            metrics: Arc::new(Metrics::new()),
            shutdown: Arc::new(AtomicBool::new(false)),
        };
        
        // Start background tasks
        let pool_updater = integration.clone();
        tokio::spawn(async move {
            pool_updater.pool_update_loop().await;
        });
        
        let metrics_reporter = integration.clone();
        tokio::spawn(async move {
            metrics_reporter.report_metrics_loop().await;
        });
        
        let health_monitor = integration.clone();
        tokio::spawn(async move {
            health_monitor.monitor_pool_health().await;
        });
        
        Ok(integration)
    }

    pub async fn execute_order(&self, order: Order) -> Result<Signature, DarkPoolError> {
        if self.circuit_breaker.is_open() {
            return Err(DarkPoolError::CircuitBreakerOpen);
        }
        
        let _permit = self.execution_semaphore.acquire().await
            .map_err(|_| DarkPoolError::OrderTimeout)?;
        
        self.metrics.total_orders.fetch_add(1, Ordering::Relaxed);
        let order_id = self.generate_order_id(&order);
        
        // Add to pending orders
        self.pending_orders.write().await.insert(order_id, order.clone());
        
        let result = match self.execute_order_internal(order).await {
            Ok(signature) => {
                self.metrics.successful_orders.fetch_add(1, Ordering::Relaxed);
                self.circuit_breaker.record_success();
                Ok(signature)
            }
            Err(e) => {
                self.metrics.failed_orders.fetch_add(1, Ordering::Relaxed);
                self.circuit_breaker.record_failure();
                Err(e)
            }
        };
        
        // Remove from pending orders
        self.pending_orders.write().await.remove(&order_id);
        
        result
    }

    async fn execute_order_internal(&self, order: Order) -> Result<Signature, DarkPoolError> {
        // Expiry hard-stop
        if let Some(exp) = order.expires_at {
            let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();
            if now > exp { return Err(DarkPoolError::OrderExpired); }
        }

        // Build candidate routes
        let mut routes = self.find_best_routes(&order).await?;
        if routes.is_empty() { return Err(DarkPoolError::NoValidRoute); }

        // Try up to top 3 routes; enforce per-route freshness before firing
        let mut last_err: Option<DarkPoolError> = None;

        for (idx, route) in routes.drain(..).take(3).enumerate() {
            self.ensure_route_fresh(&route).await?;

            match self.execute_route(&order, &route).await {
                Ok(sig) => {
                    info!("Executed via route#{idx} (hops={})", route.path.len());
                    self.metrics.total_volume.fetch_add(order.amount, Ordering::Relaxed);
                    return Ok(sig);
                }
                Err(DarkPoolError::TransactionFailed(e)) => {
                    let cls = self.classify_exec_error(&e);
                    warn!("route#{idx} failed: class={cls} err={e}");
                    last_err = Some(DarkPoolError::TransactionFailed(e));

                    // Pivot logic:
                    // - price/liquidity/slippage → switch route immediately
                    // - contention → tiny sleep then try next route (not same)
                    // - blockhash/under_cu handled in execute_route; if still failed, switch
                    // - account_setup → next route (ATA helper already in path)
                    match cls {
                        "route_bad_price" | "account_setup" => continue,     // next route
                        "contention" | "blockhash" | "under_cu" | "other" => continue,
                        _ => continue,
                    }
                }
                Err(e) => {
                    warn!("route#{idx} failed: {:?}", e);
                    last_err = Some(e);
                    continue;
                }
            }
        }

        Err(last_err.unwrap_or(DarkPoolError::AllRoutesFailed))
    }

    async fn find_best_routes(&self, order: &Order) -> Result<Vec<Route>, DarkPoolError> {
        let pools = self.pools.read().await;
        let mut direct_routes = Vec::new();
        let mut multi_hop_routes = Vec::new();

        // Direct routes
        for (_key, pool_info) in pools.iter() {
            if self.matches_order(pool_info, order) {
                let est_price = self.calculate_price_with_slippage(
                    pool_info.base_reserve.load(Ordering::Relaxed),
                    pool_info.quote_reserve.load(Ordering::Relaxed),
                    order.amount,
                    pool_info.fee_bps,
                    order.side.clone(),
                );

                let pool = Pool {
                    address: pool_info.address,
                    protocol: pool_info.protocol.clone(),
                    base_mint: order.base_mint,
                    quote_mint: order.quote_mint,
                    fee_bps: pool_info.fee_bps,
                };

                direct_routes.push(Route {
                    path: vec![pool],
                    estimated_price: est_price,
                    estimated_gas: 0,
                });
            }
        }

        // Multi-hop if useful
        if direct_routes.is_empty() || self.should_check_multi_hop(&order).await {
            multi_hop_routes = self.find_multi_hop_routes(order, &pools).await?;
        }

        // Combine
        let mut all = direct_routes;
        all.extend(multi_hop_routes);

        // EV scoring (gas + staleness + reliability)
        for r in &mut all {
            let (ev_micro, gas_cu) = self.score_route_ev(order, r).await?;
            r.estimated_gas = gas_cu;
            r.estimated_price = ev_micro;
        }

        // Limit filter (post-EV)
        if let Some(limit_price) = order.limit_price {
            all.retain(|route| match order.side {
                OrderSide::Buy  => route.estimated_price <= limit_price,
                OrderSide::Sell => route.estimated_price >= limit_price,
            });
        }

        // Best first
        all.sort_by_key(|r| match order.side {
            OrderSide::Buy  => r.estimated_price,
            OrderSide::Sell => u64::MAX - r.estimated_price,
        });

        Ok(all)
    }

    async fn execute_route(&self, order: &Order, route: &Route) -> Result<Signature, DarkPoolError> {
        // Data-plane ixs (includes ATA ensures + per-hop minOut)
        let route_ixs = self.build_route_instructions(order, route).await?;

        // Base CU/tip from config + tiny adaptive scaler
        let hops = route.path.len() as u32;
        let base_cu = 70_000u32 + 110_000u32.saturating_mul(hops);
        let mut cu_limit = self.clamp_cu(base_cu);
        let mut cu_price = self.adaptive_cu_price().await;

        // Prepare compute budget + route ixs
        let mut all_ixs = self.build_with_compute_budget(&route_ixs, cu_limit, cu_price);

        // Optional: v0 compile using LUTs from env (DARKPOOL_LUTS=pubkey1,pubkey2,...)
        let mut use_v0 = false;
        let mut alt_accounts: Vec<AddressLookupTableAccount> = Vec::new();
        if let Ok(lut_env) = std::env::var("DARKPOOL_LUTS") {
            let keys = lut_env.split(',').filter(|s| !s.is_empty());
            for k in keys {
                if let Ok(pk) = Pubkey::from_str(k.trim()) {
                    if let Ok(resp) = self.rpc_client.get_address_lookup_table(&pk).await {
                        if let Some(alt) = resp.value { alt_accounts.push(alt); }
                    }
                }
            }
            use_v0 = !alt_accounts.is_empty();
        }

        // Build initial tx (legacy or v0) for simulate
        let mut recent_blockhash = self.rpc_client.get_latest_blockhash().await
            .map_err(|e| DarkPoolError::RpcError(e.to_string()))?;

        enum BuiltTx<'a> { Legacy(Transaction), V0(VersionedTransaction, &'a V0Message) }

        let mut v0_msg_storage: Option<V0Message> = None;
        let mut built = if use_v0 {
            let msg = V0Message::try_compile(
                &self.wallet.pubkey(),
                &all_ixs,
                &alt_accounts,
                recent_blockhash,
            ).map_err(|e| DarkPoolError::SerializationError(format!("v0 compile: {e}")))?;
            let tx = VersionedTransaction::try_new(msg.clone(), &[&*self.wallet])
                .map_err(|e| DarkPoolError::SerializationError(format!("v0 sign: {e}")))?;
            v0_msg_storage = Some(msg);
            BuiltTx::V0(tx, v0_msg_storage.as_ref().unwrap())
        } else {
            let tx = Transaction::new_signed_with_payer(
                &all_ixs,
                Some(&self.wallet.pubkey()),
                &[&*self.wallet],
                recent_blockhash,
            );
            BuiltTx::Legacy(tx)
        };

        // ---- Pre-send simulation (no sig verify) → auto-tune CU limit ----
        match &built {
            BuiltTx::Legacy(tx) => {
                if let Ok(sim) = self.rpc_client.simulate_transaction_with_config(
                    tx,
                    RpcSimulateTransactionConfig {
                        sig_verify: Some(false),
                        replace_recent_blockhash: Some(true),
                        commitment: Some(CommitmentConfig::processed()),
                        encoding: Some(UiTransactionEncoding::Base64),
                        ..Default::default()
                    }
                ).await {
                    if let Some(units) = sim.value.units_consumed {
                        let tuned = self.clamp_cu((units as u32).saturating_mul(12) / 10);
                        if tuned > cu_limit {
                            cu_limit = tuned;
                            all_ixs = self.build_with_compute_budget(&route_ixs, cu_limit, cu_price);
                        }
                    }
                }
            }
            BuiltTx::V0(tx, _msg) => {
                if let Ok(sim) = self.rpc_client.simulate_transaction_with_config(
                    tx,
                    RpcSimulateTransactionConfig {
                        sig_verify: Some(false),
                        replace_recent_blockhash: Some(true),
                        commitment: Some(CommitmentConfig::processed()),
                        encoding: Some(UiTransactionEncoding::Base64),
                        ..Default::default()
                    }
                ).await {
                    if let Some(units) = sim.value.units_consumed {
                        let tuned = self.clamp_cu((units as u32).saturating_mul(12) / 10);
                        if tuned > cu_limit {
                            cu_limit = tuned;
                            all_ixs = self.build_with_compute_budget(&route_ixs, cu_limit, cu_price);
                        }
                    }
                }
            }
        }

        // Rebuild tx with tuned CU
        recent_blockhash = self.rpc_client.get_latest_blockhash().await
            .map_err(|e| DarkPoolError::RpcError(e.to_string()))?;
        built = if use_v0 {
            let msg = V0Message::try_compile(
                &self.wallet.pubkey(),
                &all_ixs,
                &alt_accounts,
                recent_blockhash,
            ).map_err(|e| DarkPoolError::SerializationError(format!("v0 compile: {e}")))?;
            let tx = VersionedTransaction::try_new(msg.clone(), &[&*self.wallet])
                .map_err(|e| DarkPoolError::SerializationError(format!("v0 sign: {e}")))?;
            v0_msg_storage = Some(msg);
            BuiltTx::V0(tx, v0_msg_storage.as_ref().unwrap())
        } else {
            let tx = Transaction::new_signed_with_payer(
                &all_ixs,
                Some(&self.wallet.pubkey()),
                &[&*self.wallet],
                recent_blockhash,
            );
            BuiltTx::Legacy(tx)
        };

        // ---- Send with retries; fresh blockhash + re-sign each time ----
        let mut attempt: u32 = 0;
        let max_retries: u32 = 6;

        loop {
            attempt += 1;

            recent_blockhash = self.rpc_client.get_latest_blockhash().await
                .map_err(|e| DarkPoolError::RpcError(e.to_string()))?;

            let sig = match &built {
                BuiltTx::Legacy(_tx) => {
                    let tx = Transaction::new_signed_with_payer(
                        &all_ixs,
                        Some(&self.wallet.pubkey()),
                        &[&*self.wallet],
                        recent_blockhash,
                    );
                    match self.rpc_client.send_transaction_with_config(
                        &tx,
                        RpcSendTransactionConfig {
                            skip_preflight: true,
                            preflight_commitment: Some(CommitmentConfig::processed().commitment),
                            ..Default::default()
                        }
                    ).await {
                        Ok(s) => s,
                        Err(e) => {
                            if attempt >= max_retries {
                                return Err(DarkPoolError::TransactionFailed(e.to_string()));
                            }
                            let delay_ms = self.small_jitter_ms(40) + 60u64 * attempt as u64;
                            sleep(Duration::from_millis(delay_ms)).await;
                            if attempt == 3 || attempt == 5 {
                                cu_price = cu_price.saturating_add(50);
                                all_ixs = self.build_with_compute_budget(&route_ixs, cu_limit, cu_price);
                            }
                            continue;
                        }
                    }
                }
                BuiltTx::V0(_tx, _msg) => {
                    let msg = V0Message::try_compile(
                        &self.wallet.pubkey(),
                        &all_ixs,
                        &alt_accounts,
                        recent_blockhash,
                    ).map_err(|e| DarkPoolError::SerializationError(format!("v0 compile: {e}")))?;
                    let vtx = VersionedTransaction::try_new(msg, &[&*self.wallet])
                        .map_err(|e| DarkPoolError::SerializationError(format!("v0 sign: {e}")))?;
                    match self.rpc_client.send_transaction_with_config(
                        &vtx,
                        RpcSendTransactionConfig {
                            skip_preflight: true,
                            preflight_commitment: Some(CommitmentConfig::processed().commitment),
                            ..Default::default()
                        }
                    ).await {
                        Ok(s) => s,
                        Err(e) => {
                            if attempt >= max_retries {
                                return Err(DarkPoolError::TransactionFailed(e.to_string()));
                            }
                            let delay_ms = self.small_jitter_ms(40) + 60u64 * attempt as u64;
                            sleep(Duration::from_millis(delay_ms)).await;
                            if attempt == 3 || attempt == 5 {
                                cu_price = cu_price.saturating_add(50);
                                all_ixs = self.build_with_compute_budget(&route_ixs, cu_limit, cu_price);
                            }
                            continue;
                        }
                    }
                }
            };

            if self.get_transaction_confirmation(&sig).await? {
                for pool in &route.path {
                    let key = format!("{}:{:?}", pool.address, pool.protocol);
                    if let Some(pi) = self.pools.read().await.get(&key) {
                        pi.success_count.fetch_add(1, Ordering::Relaxed);
                    }
                }
                self.metrics.total_gas_used.fetch_add((cu_price as u64) * (cu_limit as u64), Ordering::Relaxed);
                self.metrics.total_volume.fetch_add(order.amount, Ordering::Relaxed);
                return Ok(sig);
            } else {
                for pool in &route.path {
                    let key = format!("{}:{:?}", pool.address, pool.protocol);
                    if let Some(pi) = self.pools.read().await.get(&key) {
                        pi.failure_count.fetch_add(1, Ordering::Relaxed);
                    }
                }
                if attempt >= max_retries {
                    return Err(DarkPoolError::TransactionFailed("confirmation failed".into()));
                }
                let delay_ms = self.small_jitter_ms(60) + 90u64 * attempt as u64;
                sleep(Duration::from_millis(delay_ms)).await;
                if attempt == 2 || attempt == 4 {
                    cu_price = cu_price.saturating_add(75);
                    all_ixs = self.build_with_compute_budget(&route_ixs, cu_limit, cu_price);
                }
            }
        }
    }

    async fn build_route_instructions(
        &self,
        order: &Order,
        route: &Route,
    ) -> Result<Vec<Instruction>, DarkPoolError> {
        let mut instructions = Vec::new();
        let mut current_mint = order.base_mint;
        let mut current_amount = order.amount;

        // Ensure source ATA exists for first hop input
        let src_ata_ixs = self.ensure_ata_if_needed(&self.wallet.pubkey(), &current_mint).await?;
        instructions.extend(src_ata_ixs);

        let max_slip = {
            // prefer order override, else config
            let cfg = self.config.read().await;
            u16::min(order.max_slippage_bps, cfg.max_slippage_bps)
        };

        for (i, pool) in route.path.iter().enumerate() {
            let is_last = i + 1 == route.path.len();
            let next_mint = if is_last { order.quote_mint } else { route.path[i + 1].base_mint };

            // Ensure destination ATA exists for this hop
            let dst_ata_ixs = self.ensure_ata_if_needed(&self.wallet.pubkey(), &next_mint).await?;
            instructions.extend(dst_ata_ixs);

            match pool.protocol {
                Protocol::Phoenix => {
                    let ix = self.build_phoenix_swap_instruction(
                        pool,
                        current_mint,
                        next_mint,
                        current_amount,
                        order.side.clone(),
                        max_slip,
                    ).await?;
                    instructions.push(ix);
                }
                Protocol::Whirlpool => {
                    let ix = self.build_whirlpool_swap_instruction(
                        pool,
                        current_mint,
                        next_mint,
                        current_amount,
                        order.side.clone(),
                        max_slip,
                    ).await?;
                    instructions.push(ix);
                }
                Protocol::Serum => {
                    let ix = self.build_serum_swap_instruction(
                        pool,
                        current_mint,
                        next_mint,
                        current_amount,
                        order.side.clone(),
                        max_slip,
                    ).await?;
                    instructions.push(ix);
                }
                Protocol::Raydium => {
                    let ix = self.build_raydium_swap_instruction(
                        pool,
                        current_mint,
                        next_mint,
                        current_amount,
                        order.side.clone(),
                        max_slip,
                    ).await?;
                    instructions.push(ix);
                }
            }

            // For next hop, estimate output with pool reserves to update amount
            if !is_last {
                let key = format!("{}:{:?}", pool.address, pool.protocol);
                let pool_info = self.pools.read().await
                    .get(&key).ok_or(DarkPoolError::PoolNotFound)?.clone();

                let est_out = self.estimate_output_amount(
                    pool_info.base_reserve.load(Ordering::Relaxed),
                    pool_info.quote_reserve.load(Ordering::Relaxed),
                    current_amount,
                    pool.fee_bps,
                    order.side.clone(),
                );

                // enforce per-hop minOut to avoid cascading slippage blowup
                let min_out = (est_out as u128 * (10_000u128 - max_slip as u128) / 10_000u128) as u64;
                if min_out == 0 {
                    return Err(DarkPoolError::NoLiquidity);
                }
                current_amount = min_out;
                current_mint = next_mint;
            } else {
                current_mint = next_mint;
            }
        }

        Ok(instructions)
    }

    async fn build_phoenix_swap_instruction(
        &self,
        pool: &Pool,
        input_mint: Pubkey,
        output_mint: Pubkey,
        amount: u64,
        _side: OrderSide,
        max_slippage_bps: u16,
    ) -> Result<Instruction, DarkPoolError> {
        let program_id = Pubkey::from_str(PHOENIX_PROGRAM_ID)
            .map_err(|e| DarkPoolError::InvalidPubkey(e.to_string()))?;

        let user_source = get_associated_token_address(&self.wallet.pubkey(), &input_mint);
        let user_destination = get_associated_token_address(&self.wallet.pubkey(), &output_mint);

        let accounts = vec![
            AccountMeta::new(pool.address, false),
            AccountMeta::new(user_source, false),
            AccountMeta::new(user_destination, false),
            AccountMeta::new_readonly(self.wallet.pubkey(), true),
            AccountMeta::new_readonly(spl_token::id(), false),
        ];

        #[derive(BorshSerialize)]
        struct PhoenixSwapData {
            instruction: u8,
            amount: u64,
            minimum_out: u64,
        }

        let minimum_out = (amount as u128 * (10_000u128 - max_slippage_bps as u128) / 10_000u128) as u64;

        let data = PhoenixSwapData { instruction: 1, amount, minimum_out };

        Ok(Instruction {
            program_id,
            accounts,
            data: data.try_to_vec()
                .map_err(|e| DarkPoolError::SerializationError(e.to_string()))?,
        })
    }

    async fn build_whirlpool_swap_instruction(
        &self,
        pool: &Pool,
        input_mint: Pubkey,
        output_mint: Pubkey,
        amount: u64,
        side: OrderSide,
        max_slippage_bps: u16,
    ) -> Result<Instruction, DarkPoolError> {
        let program_id = Pubkey::from_str(WHIRLPOOL_PROGRAM_ID)
            .map_err(|e| DarkPoolError::InvalidPubkey(e.to_string()))?;

        let user_source = get_associated_token_address(&self.wallet.pubkey(), &input_mint);
        let user_destination = get_associated_token_address(&self.wallet.pubkey(), &output_mint);

        let accounts = vec![
            AccountMeta::new_readonly(program_id, false),
            AccountMeta::new(self.wallet.pubkey(), true),
            AccountMeta::new(pool.address, false),
            AccountMeta::new(user_source, false),
            AccountMeta::new(user_destination, false),
            AccountMeta::new_readonly(spl_token::id(), false),
        ];

        #[derive(BorshSerialize)]
        struct WhirlpoolSwapData {
            amount: u64,
            other_amount_threshold: u64,
            sqrt_price_limit: u128,
            amount_specified_is_input: bool,
            a_to_b: bool,
        }

        let other_amount_threshold =
            (amount as u128 * (10_000u128 - max_slippage_bps as u128) / 10_000u128) as u64;

        let data = WhirlpoolSwapData {
            amount,
            other_amount_threshold,
            sqrt_price_limit: 0,
            amount_specified_is_input: true,
            a_to_b: matches!(side, OrderSide::Sell),
        };

        Ok(Instruction {
            program_id,
            accounts,
            data: data.try_to_vec()
                .map_err(|e| DarkPoolError::SerializationError(e.to_string()))?,
        })
    }

    async fn build_serum_swap_instruction(
        &self,
        pool: &Pool,
        input_mint: Pubkey,
        output_mint: Pubkey,
        amount: u64,
        side: OrderSide,
        _max_slippage_bps: u16,
    ) -> Result<Instruction, DarkPoolError> {
        let program_id = Pubkey::from_str(SERUM_PROGRAM_ID)
            .map_err(|e| DarkPoolError::InvalidPubkey(e.to_string()))?;

        let market_info = self.fetch_serum_market_info(&pool.address).await?;

        let user_source = get_associated_token_address(&self.wallet.pubkey(), &input_mint);
        let user_destination = get_associated_token_address(&self.wallet.pubkey(), &output_mint);

        let accounts = vec![
            AccountMeta::new(pool.address, false),
            AccountMeta::new(market_info.request_queue, false),
            AccountMeta::new(market_info.event_queue, false),
            AccountMeta::new(market_info.bids, false),
            AccountMeta::new(market_info.asks, false),
            AccountMeta::new(user_source, false),
            AccountMeta::new(user_destination, false),
            AccountMeta::new(market_info.coin_vault, false),
            AccountMeta::new(market_info.pc_vault, false),
            AccountMeta::new_readonly(market_info.vault_signer, false),
            AccountMeta::new(self.wallet.pubkey(), true),
            AccountMeta::new_readonly(spl_token::id(), false),
        ];

        #[derive(BorshSerialize)]
        struct SerumNewOrderV3 {
            side: u8,
            limit_price: u64,
            max_coin_qty: u64,
            max_native_pc_qty_including_fees: u64,
            self_trade_behavior: u8,
            order_type: u8,
            client_order_id: u64,
            limit: u16,
        }

        let (limit_price, max_coin_qty, max_pc_qty) =
            self.calculate_serum_order_params(&market_info, amount, side.clone()).await?;

        let data = SerumNewOrderV3 {
            side: match side { OrderSide::Buy => 0, OrderSide::Sell => 1 },
            limit_price,
            max_coin_qty,
            max_native_pc_qty_including_fees: max_pc_qty,
            self_trade_behavior: 0,
            order_type: 2, // IOC
            client_order_id: 0,
            limit: 10,
        };

        Ok(Instruction {
            program_id,
            accounts,
            data: data.try_to_vec()
                .map_err(|e| DarkPoolError::SerializationError(e.to_string()))?,
        })
    }

    async fn build_raydium_swap_instruction(
        &self,
        pool: &Pool,
        input_mint: Pubkey,
        output_mint: Pubkey,
        amount: u64,
        _side: OrderSide,
        max_slippage_bps: u16,
    ) -> Result<Instruction, DarkPoolError> {
        let program_id = Pubkey::from_str(RAYDIUM_PROGRAM_ID)
            .map_err(|e| DarkPoolError::InvalidPubkey(e.to_string()))?;

        let amm_info = self.fetch_raydium_amm_info(&pool.address).await?;

        let user_source = get_associated_token_address(&self.wallet.pubkey(), &input_mint);
        let user_destination = get_associated_token_address(&self.wallet.pubkey(), &output_mint);

        let accounts = vec![
            AccountMeta::new_readonly(spl_token::id(), false),
            AccountMeta::new(pool.address, false),
            AccountMeta::new_readonly(amm_info.amm_authority, false),
            AccountMeta::new(amm_info.amm_open_orders, false),
            AccountMeta::new(amm_info.amm_target_orders, false),
            AccountMeta::new(amm_info.pool_coin_token_account, false),
            AccountMeta::new(amm_info.pool_pc_token_account, false),
            AccountMeta::new_readonly(amm_info.serum_program_id, false),
            AccountMeta::new(amm_info.serum_market, false),
            AccountMeta::new(amm_info.serum_bids, false),
            AccountMeta::new(amm_info.serum_asks, false),
            AccountMeta::new(amm_info.serum_event_queue, false),
            AccountMeta::new(amm_info.serum_coin_vault, false),
            AccountMeta::new(amm_info.serum_pc_vault, false),
            AccountMeta::new_readonly(amm_info.serum_vault_signer, false),
            AccountMeta::new(user_source, false),
            AccountMeta::new(user_destination, false),
            AccountMeta::new_readonly(self.wallet.pubkey(), true),
        ];

        #[derive(BorshSerialize)]
        struct RaydiumSwapInstruction {
            instruction: u8,
            amount_in: u64,
            minimum_amount_out: u64,
        }

        let minimum_out = (amount as u128 * (10_000u128 - max_slippage_bps as u128) / 10_000u128) as u64;

        let data = RaydiumSwapInstruction {
            instruction: 9,
            amount_in: amount,
            minimum_amount_out: minimum_out,
        };

        Ok(Instruction {
            program_id,
            accounts,
            data: data.try_to_vec()
                .map_err(|e| DarkPoolError::SerializationError(e.to_string()))?,
        })
    }

    async fn fetch_serum_market_info(&self, market: &Pubkey) -> Result<SerumMarketInfo, DarkPoolError> {
        let account = self.rpc_client.get_account(market).await
            .map_err(|e| DarkPoolError::RpcError(e.to_string()))?;
        
        if account.data.len() < 400 {
            return Err(DarkPoolError::InvalidMarketData);
        }
        
        Ok(SerumMarketInfo {
            coin_vault: Pubkey::new_from_array(*array_ref![account.data, 64, 32]),
            pc_vault: Pubkey::new_from_array(*array_ref![account.data, 96, 32]),
            request_queue: Pubkey::new_from_array(*array_ref![account.data, 128, 32]),
            event_queue: Pubkey::new_from_array(*array_ref![account.data, 160, 32]),
            bids: Pubkey::new_from_array(*array_ref![account.data, 192, 32]),
            asks: Pubkey::new_from_array(*array_ref![account.data, 224, 32]),
            vault_signer: Pubkey::new_from_array(*array_ref![account.data, 352, 32]),
        })
    }

    async fn fetch_raydium_amm_info(&self, amm: &Pubkey) -> Result<RaydiumAmmInfo, DarkPoolError> {
        let account = self.rpc_client.get_account(amm).await
            .map_err(|e| DarkPoolError::RpcError(e.to_string()))?;
        
        if account.data.len() < 752 {
            return Err(DarkPoolError::InvalidMarketData);
        }
        
        Ok(RaydiumAmmInfo {
            amm_authority: Pubkey::new_from_array(*array_ref![account.data, 0, 32]),
            amm_open_orders: Pubkey::new_from_array(*array_ref![account.data, 40, 32]),
            amm_target_orders: Pubkey::new_from_array(*array_ref![account.data, 72, 32]),
            pool_coin_token_account: Pubkey::new_from_array(*array_ref![account.data, 104, 32]),
            pool_pc_token_account: Pubkey::new_from_array(*array_ref![account.data, 136, 32]),
            serum_program_id: Pubkey::new_from_array(*array_ref![account.data, 200, 32]),
            serum_market: Pubkey::new_from_array(*array_ref![account.data, 232, 32]),
            serum_bids: Pubkey::new_from_array(*array_ref![account.data, 296, 32]),
            serum_asks: Pubkey::new_from_array(*array_ref![account.data, 328, 32]),
            serum_event_queue: Pubkey::new_from_array(*array_ref![account.data, 360, 32]),
            serum_coin_vault: Pubkey::new_from_array(*array_ref![account.data, 392, 32]),
            serum_pc_vault: Pubkey::new_from_array(*array_ref![account.data, 424, 32]),
            serum_vault_signer: Pubkey::new_from_array(*array_ref![account.data, 456, 32]),
        })
    }

    async fn calculate_serum_order_params(
        &self,
        market_info: &SerumMarketInfo,
        amount: u64,
        side: OrderSide,
    ) -> Result<(u64, u64, u64), DarkPoolError> {
        let keys = vec![market_info.bids, market_info.asks];
        let accs = self.rpc_client.get_multiple_accounts(&keys).await
            .map_err(|e| DarkPoolError::RpcError(e.to_string()))?;

        let bids_data = accs.get(0).and_then(|o| o.as_ref()).ok_or(DarkPoolError::InvalidMarketData)?;
        let asks_data = accs.get(1).and_then(|o| o.as_ref()).ok_or(DarkPoolError::InvalidMarketData)?;

        let (best_bid, best_ask) = self.extract_serum_prices(&bids_data.data, &asks_data.data)?;

        let cfg = self.config.read().await;
        let bps = u64::from(cfg.max_slippage_bps);

        let limit_price = match side {
            OrderSide::Buy  => best_ask.saturating_mul(10_000 + bps) / 10_000,
            OrderSide::Sell => best_bid.saturating_mul(10_000 - bps) / 10_000,
        };

        let max_coin_qty = amount;
        let max_pc_qty   = amount.saturating_mul(limit_price) / 1_000_000;

        Ok((limit_price, max_coin_qty, max_pc_qty))
    }

    fn extract_serum_prices(&self, bids_data: &[u8], asks_data: &[u8]) -> Result<(u64, u64), DarkPoolError> {
        if bids_data.len() < 80 || asks_data.len() < 80 {
            return Err(DarkPoolError::InvalidMarketData);
        }
        
        // Extract best bid
        let best_bid = u64::from_le_bytes(*array_ref![bids_data, 72, 8]);
        
        // Extract best ask
        let best_ask = u64::from_le_bytes(*array_ref![asks_data, 72, 8]);
        
        if best_bid == 0 || best_ask == 0 {
            return Err(DarkPoolError::NoLiquidity);
        }
        
        Ok((best_bid, best_ask))
    }

    fn matches_order(&self, pool_info: &PoolInfo, order: &Order) -> bool {
        // Check if pool matches the order's mints
        let base_mint = order.base_mint;
        let quote_mint = order.quote_mint;
        
        // For now, we assume pools are correctly mapped
        // In production, you'd verify this against on-chain data
        true
    }

    fn calculate_price_with_slippage(
        &self,
        base_reserve: u64,
        quote_reserve: u64,
        amount: u64,
        fee_bps: u16,
        side: OrderSide,
    ) -> u64 {
        let k = base_reserve as u128 * quote_reserve as u128;
        
        match side {
            OrderSide::Buy => {
                let new_base = base_reserve - amount;
                if new_base == 0 {
                    return u64::MAX;
                }
                let new_quote = k / new_base as u128;
                let quote_needed = (new_quote - quote_reserve as u128) as u64;
                
                // Add fee
                let fee_amount = (quote_needed as u128 * fee_bps as u128) / 10000;
                ((quote_needed as u128 + fee_amount) * 1_000_000 / amount as u128) as u64
            }
            OrderSide::Sell => {
                let new_base = base_reserve + amount;
                let new_quote = k / new_base as u128;
                let quote_received = (quote_reserve as u128 - new_quote) as u64;
                
                // Subtract fee
                let fee_amount = (quote_received as u128 * fee_bps as u128) / 10000;
                ((quote_received as u128 - fee_amount) * 1_000_000 / amount as u128) as u64
            }
        }
    }

    fn estimate_output_amount(
        &self,
        base_reserve: u64,
        quote_reserve: u64,
        input_amount: u64,
        fee_bps: u16,
        side: OrderSide,
    ) -> u64 {
        let k = base_reserve as u128 * quote_reserve as u128;
        
        match side {
            OrderSide::Buy => {
                // When buying, input is quote, output is base
                let fee_amount = (input_amount as u128 * fee_bps as u128) / 10000;
                let input_after_fee = input_amount as u128 - fee_amount;
                
                let new_quote = quote_reserve as u128 + input_after_fee;
                let new_base = k / new_quote;
                (base_reserve as u128 - new_base) as u64
            }
            OrderSide::Sell => {
                // When selling, input is base, output is quote
                let fee_amount = (input_amount as u128 * fee_bps as u128) / 10000;
                let input_after_fee = input_amount as u128 - fee_amount;
                
                let new_base = base_reserve as u128 + input_after_fee;
                let new_quote = k / new_base;
                (quote_reserve as u128 - new_quote) as u64
            }
        }
    }

    async fn should_check_multi_hop(&self, order: &Order) -> bool {
        // Check if multi-hop might be beneficial
        order.amount > 1_000_000_000 || order.max_slippage_bps > 100
    }

    async fn find_multi_hop_routes(
        &self,
        order: &Order,
        pools: &HashMap<String, Arc<PoolInfo>>,
    ) -> Result<Vec<Route>, DarkPoolError> {
        let mut routes = Vec::new();
        
        // Common intermediate tokens (USDC, USDT, SOL)
        let intermediates = vec![
            Pubkey::from_str("EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v").unwrap(), // USDC
            Pubkey::from_str("Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB").unwrap(), // USDT
            Pubkey::from_str("So11111111111111111111111111111111111111112").unwrap(), // SOL
        ];
        
        for intermediate in intermediates {
            if intermediate == order.base_mint || intermediate == order.quote_mint {
                continue;
            }
            
            // Find pools for first hop (base -> intermediate)
            let first_hop_pools: Vec<_> = pools.values()
                .filter(|p| self.matches_pair(p, order.base_mint, intermediate))
                .collect();
            
            // Find pools for second hop (intermediate -> quote)
            let second_hop_pools: Vec<_> = pools.values()
                .filter(|p| self.matches_pair(p, intermediate, order.quote_mint))
                .collect();
            
            for first_pool in &first_hop_pools {
                for second_pool in &second_hop_pools {
                    let first_output = self.estimate_output_amount(
                        first_pool.base_reserve.load(Ordering::Relaxed),
                        first_pool.quote_reserve.load(Ordering::Relaxed),
                        order.amount,
                        first_pool.fee_bps,
                        order.side.clone(),
                    );
                    
                    let final_output = self.estimate_output_amount(
                        second_pool.base_reserve.load(Ordering::Relaxed),
                        second_pool.quote_reserve.load(Ordering::Relaxed),
                        first_output,
                        second_pool.fee_bps,
                        order.side.clone(),
                    );
                    
                    let estimated_price = (final_output * 1_000_000) / order.amount;
                    
                    routes.push(Route {
                        path: vec![
                            Pool {
                                address: first_pool.address,
                                protocol: first_pool.protocol.clone(),
                                base_mint: order.base_mint,
                                quote_mint: intermediate,
                                fee_bps: first_pool.fee_bps,
                            },
                            Pool {
                                address: second_pool.address,
                                protocol: second_pool.protocol.clone(),
                                base_mint: intermediate,
                                quote_mint: order.quote_mint,
                                fee_bps: second_pool.fee_bps,
                            },
                        ],
                        estimated_price,
                        estimated_gas: 400_000,
                    });
                }
            }
        }
        
        // Sort by best price
        routes.sort_by_key(|r| match order.side {
            OrderSide::Buy => r.estimated_price,
            OrderSide::Sell => u64::MAX - r.estimated_price,
        });
        
        Ok(routes.into_iter().take(5).collect())
    }

    fn matches_pair(&self, pool: &PoolInfo, mint_a: Pubkey, mint_b: Pubkey) -> bool {
        // In production, verify against actual pool data
        true
    }

    fn generate_order_id(&self, order: &Order) -> [u8; 32] {
        use sha2::{Sha256, Digest};
        
        let mut hasher = Sha256::new();
        hasher.update(&order.base_mint.to_bytes());
        hasher.update(&order.quote_mint.to_bytes());
        hasher.update(&order.amount.to_le_bytes());
        hasher.update(&order.created_at.to_le_bytes());
        hasher.update(&self.wallet.pubkey().to_bytes());
        
        let result = hasher.finalize();
        let mut id = [0u8; 32];
        id.copy_from_slice(&result);
        id
    }

    async fn pool_update_loop(&self) {
        let mut ticker = interval(Duration::from_millis(POOL_REFRESH_INTERVAL_MS));
        while !self.shutdown.load(Ordering::Relaxed) {
            ticker.tick().await;

            let snapshot = {
                let pools = self.pools.read().await;
                pools.values().cloned().collect::<Vec<_>>()
            };
            if snapshot.is_empty() { continue; }

            // Group by protocol
            let mut phoenix = Vec::new();
            let mut whirl = Vec::new();
            let mut serum = Vec::new();
            let mut raydium = Vec::new();

            for pi in snapshot {
                match pi.protocol {
                    Protocol::Phoenix   => phoenix.push(pi),
                    Protocol::Whirlpool => whirl.push(pi),
                    Protocol::Serum     => serum.push(pi),
                    Protocol::Raydium   => raydium.push(pi),
                }
            }

            // Batch refresh per protocol; errors are logged, not fatal
            if let Err(e) = self.refresh_batch_phoenix(&phoenix).await { warn!("phoenix batch: {e:?}"); }
            if let Err(e) = self.refresh_batch_whirlpool(&whirl).await { warn!("whirlpool batch: {e:?}"); }
            if let Err(e) = self.refresh_batch_serum(&serum).await { warn!("serum batch: {e:?}"); }
            if let Err(e) = self.refresh_batch_raydium(&raydium).await { warn!("raydium batch: {e:?}"); }
        }
    }

    async fn refresh_pool_reserves(&self, pool_info: Arc<PoolInfo>) -> Result<(), DarkPoolError> {
        match pool_info.protocol {
            Protocol::Phoenix => self.refresh_phoenix_pool(&pool_info).await,
            Protocol::Whirlpool => self.refresh_whirlpool_pool(&pool_info).await,
            Protocol::Serum => self.refresh_serum_pool(&pool_info).await,
            Protocol::Raydium => self.refresh_raydium_pool(&pool_info).await,
        }
    }

    async fn refresh_phoenix_pool(&self, pool_info: &PoolInfo) -> Result<(), DarkPoolError> {
        let account = self.rpc_client.get_account(&pool_info.address).await
            .map_err(|e| DarkPoolError::RpcError(e.to_string()))?;
        
        if account.data.len() >= 144 {
            let base_reserve = u64::from_le_bytes(*array_ref![account.data, 64, 8]);
            let quote_reserve = u64::from_le_bytes(*array_ref![account.data, 72, 8]);
            
            pool_info.base_reserve.store(base_reserve, Ordering::Relaxed);
            pool_info.quote_reserve.store(quote_reserve, Ordering::Relaxed);
            pool_info.last_update.store(
                SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs(),
                Ordering::Relaxed,
            );
        }
        
        Ok(())
    }

    async fn refresh_whirlpool_pool(&self, pool_info: &PoolInfo) -> Result<(), DarkPoolError> {
        let account = self.rpc_client.get_account(&pool_info.address).await
            .map_err(|e| DarkPoolError::RpcError(e.to_string()))?;
        
        if account.data.len() >= 324 {
            let liquidity = u128::from_le_bytes(*array_ref![account.data, 136, 16]);
            let sqrt_price = u128::from_le_bytes(*array_ref![account.data, 152, 16]);
            
            // Calculate reserves from liquidity and price
            let price = (sqrt_price * sqrt_price) >> 64;
            let base_reserve = ((liquidity << 64) / sqrt_price) as u64;
            let quote_reserve = ((liquidity * sqrt_price) >> 64) as u64;
            
            pool_info.base_reserve.store(base_reserve, Ordering::Relaxed);
            pool_info.quote_reserve.store(quote_reserve, Ordering::Relaxed);
            pool_info.last_update.store(
                SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs(),
                Ordering::Relaxed,
            );
        }
        
        Ok(())
    }

    async fn refresh_serum_pool(&self, pool_info: &PoolInfo) -> Result<(), DarkPoolError> {
        let market_account = self.rpc_client.get_account(&pool_info.address).await
            .map_err(|e| DarkPoolError::RpcError(e.to_string()))?;
        
        if market_account.data.len() >= 400 {
            let coin_vault = Pubkey::new_from_array(*array_ref![market_account.data, 64, 32]);
            let pc_vault = Pubkey::new_from_array(*array_ref![market_account.data, 96, 32]);
            
            let coin_account = self.rpc_client.get_account(&coin_vault).await
                .map_err(|e| DarkPoolError::RpcError(e.to_string()))?;
            let pc_account = self.rpc_client.get_account(&pc_vault).await
                .map_err(|e| DarkPoolError::RpcError(e.to_string()))?;
            
            if coin_account.data.len() >= 72 && pc_account.data.len() >= 72 {
                let base_reserve = u64::from_le_bytes(*array_ref![coin_account.data, 64, 8]);
                let quote_reserve = u64::from_le_bytes(*array_ref![pc_account.data, 64, 8]);
                
                pool_info.base_reserve.store(base_reserve, Ordering::Relaxed);
                pool_info.quote_reserve.store(quote_reserve, Ordering::Relaxed);
                pool_info.last_update.store(
                    SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs(),
                    Ordering::Relaxed,
                );
            }
        }
        
        Ok(())
    }

    async fn refresh_raydium_pool(&self, pool_info: &PoolInfo) -> Result<(), DarkPoolError> {
        let amm_account = self.rpc_client.get_account(&pool_info.address).await
            .map_err(|e| DarkPoolError::RpcError(e.to_string()))?;
        
        if amm_account.data.len() >= 752 {
            let pool_coin_token = Pubkey::new_from_array(*array_ref![amm_account.data, 104, 32]);
            let pool_pc_token = Pubkey::new_from_array(*array_ref![amm_account.data, 136, 32]);
            
            let coin_account = self.rpc_client.get_account(&pool_coin_token).await
                .map_err(|e| DarkPoolError::RpcError(e.to_string()))?;
            let pc_account = self.rpc_client.get_account(&pool_pc_token).await
                .map_err(|e| DarkPoolError::RpcError(e.to_string()))?;
            
            if coin_account.data.len() >= 72 && pc_account.data.len() >= 72 {
                let base_reserve = u64::from_le_bytes(*array_ref![coin_account.data, 64, 8]);
                let quote_reserve = u64::from_le_bytes(*array_ref![pc_account.data, 64, 8]);
                
                pool_info.base_reserve.store(base_reserve, Ordering::Relaxed);
                pool_info.quote_reserve.store(quote_reserve, Ordering::Relaxed);
                pool_info.last_update.store(
                    SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs(),
                    Ordering::Relaxed,
                );
            }
        }
        
        Ok(())
    }

    async fn refresh_pool_with_retries(&self, address: &Pubkey, protocol: Protocol) -> Result<(), DarkPoolError> {
        let mut retries = 0u32;
        let max_retries = 3u32;

        loop {
            // If existing, refresh in-place; else insert new
            let key = format!("{}:{:?}", address, protocol);
            if let Some(existing) = self.pools.read().await.get(&key).cloned() {
                match self.refresh_pool_reserves(existing.clone()).await {
                    Ok(_) => return Ok(()),
                    Err(e) => {
                        retries += 1;
                        if retries >= max_retries { return Err(e); }
                        sleep(Duration::from_millis(120 * retries as u64)).await;
                        continue;
                    }
                }
            } else {
                let pool_info = Arc::new(PoolInfo {
                    address: *address,
                    protocol: protocol.clone(),
                    base_reserve: AtomicU64::new(0),
                    quote_reserve: AtomicU64::new(0),
                    fee_bps: match protocol {
                        Protocol::Phoenix | Protocol::Whirlpool | Protocol::Serum => 30,
                        Protocol::Raydium => 25,
                    },
                    last_update: AtomicU64::new(0),
                    success_count: AtomicU64::new(0),
                    failure_count: AtomicU64::new(0),
                });
                match self.refresh_pool_reserves(pool_info.clone()).await {
                    Ok(_) => {
                        self.pools.write().await.insert(key, pool_info);
                        return Ok(());
                    }
                    Err(e) => {
                        retries += 1;
                        if retries >= max_retries { return Err(e); }
                        sleep(Duration::from_millis(120 * retries as u64)).await;
                    }
                }
            }
        }
    }

    async fn report_metrics_loop(&self) {
        let mut interval = interval(Duration::from_secs(60));
        
        while !self.shutdown.load(Ordering::Relaxed) {
            interval.tick().await;
            
            let total = self.metrics.total_orders.load(Ordering::Relaxed);
            let successful = self.metrics.successful_orders.load(Ordering::Relaxed);
            let failed = self.metrics.failed_orders.load(Ordering::Relaxed);
            let volume = self.metrics.total_volume.load(Ordering::Relaxed);
            let gas_used = self.metrics.total_gas_used.load(Ordering::Relaxed);
            
            let success_rate = if total > 0 {
                (successful as f64 / total as f64) * 100.0
            } else {
                0.0
            };
            
            info!(
                "Metrics - Orders: {} (Success: {:.2}%), Volume: {}, Gas: {}",
                total, success_rate, volume, gas_used
            );
            
            // Report pool health
            let pools = self.pools.read().await;
            for (key, pool_info) in pools.iter() {
                let pool_success = pool_info.success_count.load(Ordering::Relaxed);
                let pool_failure = pool_info.failure_count.load(Ordering::Relaxed);
                let pool_total = pool_success + pool_failure;
                
                if pool_total > 0 {
                    let pool_success_rate = (pool_success as f64 / pool_total as f64) * 100.0;
                    debug!(
                        "Pool {} - Success Rate: {:.2}%, Reserves: {}/{}",
                        key,
                        pool_success_rate,
                        pool_info.base_reserve.load(Ordering::Relaxed),
                        pool_info.quote_reserve.load(Ordering::Relaxed)
                    );
                }
            }
        }
    }

    pub async fn get_metrics(&self) -> (u64, u64, u64, u64) {
        (
            self.metrics.total_orders.load(Ordering::Relaxed),
            self.metrics.successful_orders.load(Ordering::Relaxed),
            self.metrics.failed_orders.load(Ordering::Relaxed),
            self.metrics.total_volume.load(Ordering::Relaxed),
        )
    }

    pub async fn get_pending_orders(&self) -> Vec<Order> {
        self.pending_orders.read().await.values().cloned().collect()
    }

    pub async fn cancel_order(&self, order_id: [u8; 32]) -> Result<(), DarkPoolError> {
        match self.pending_orders.write().await.remove(&order_id) {
            Some(_) => {
                info!("Order {:?} cancelled", order_id);
                Ok(())
            }
            None => Err(DarkPoolError::OrderNotFound),
        }
    }

    pub async fn health_check(&self) -> Result<bool, DarkPoolError> {
        // Check circuit breaker
        if self.circuit_breaker.is_open() {
            warn!("Circuit breaker is open");
            return Ok(false);
        }
        
        // Check RPC connection
        match self.rpc_client.get_health().await {
            Ok(_) => {}
            Err(e) => {
                error!("RPC health check failed: {}", e);
                return Ok(false);
            }
        }
        
        // Check pool freshness
        let pools = self.pools.read().await;
        let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();
        let mut stale_pools = 0;
        
        for (key, pool_info) in pools.iter() {
            let last_update = pool_info.last_update.load(Ordering::Relaxed);
            if now - last_update > 300 { // 5 minutes
                warn!("Pool {} has stale data", key);
                stale_pools += 1;
            }
        }
        
        if stale_pools > pools.len() / 2 {
            warn!("More than 50% of pools have stale data");
            return Ok(false);
        }
        
        Ok(true)
    }

    pub async fn monitor_pool_health(&self) {
        let mut interval = interval(Duration::from_secs(30));
        
        while !self.shutdown.load(Ordering::Relaxed) {
            interval.tick().await;
            
            let pools = self.pools.read().await;
            let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();
            
            for (key, pool) in pools.iter() {
                let last_update = pool.last_update.load(Ordering::Relaxed);
                if now - last_update > 120 { // 2 minutes
                    warn!("Pool {} is stale, refreshing...", key);
                    drop(pools);
                    
                    if let Err(e) = self.refresh_pool_with_retries(&pool.address, pool.protocol.clone()).await {
                        error!("Failed to refresh stale pool {}: {}", key, e);
                    }
                    
                    return;
                }
            }
        }
    }

    pub async fn get_pool_stats(&self) -> HashMap<String, serde_json::Value> {
        let pools = self.pools.read().await;
        let mut stats = HashMap::new();
        
        for (key, pool) in pools.iter() {
            let success_count = pool.success_count.load(Ordering::Relaxed);
            let failure_count = pool.failure_count.load(Ordering::Relaxed);
            let total = success_count + failure_count;
            let success_rate = if total > 0 {
                (success_count as f64 / total as f64) * 100.0
            } else {
                0.0
            };
            
            stats.insert(key.clone(), serde_json::json!({
                "protocol": format!("{:?}", pool.protocol),
                "base_reserve": pool.base_reserve.load(Ordering::Relaxed),
                "quote_reserve": pool.quote_reserve.load(Ordering::Relaxed),
                "fee_bps": pool.fee_bps,
                "success_rate": success_rate,
                "last_update": pool.last_update.load(Ordering::Relaxed),
            }));
        }
        
        stats
    }

    pub async fn get_transaction_confirmation(&self, signature: &Signature) -> Result<bool, DarkPoolError> {
        let start = SystemTime::now();
        let max_wait_ms: u64 = 9_000; // tighter
        let mut step: u64 = 32;

        loop {
            match self.rpc_client.get_signature_status(signature).await {
                Ok(Some(status)) => return Ok(status.is_ok()),
                Ok(None) => {}
                Err(_e) => {} // transient; fall through
            }
            let elapsed = SystemTime::now().duration_since(start).unwrap_or_default().as_millis() as u64;
            if elapsed >= max_wait_ms { return Ok(false); }
            sleep(Duration::from_millis(step)).await;
            step = (step.saturating_mul(2)).min(512);
        }
    }

    pub async fn estimate_gas_for_route(&self, route: &Route) -> Result<u64, DarkPoolError> {
        // per-protocol baseline CUs (empirical ballparks before simulate)
        const CU_CB_BASE: u64 = 30_000;   // compute budget overhead (limit+price)
        const CU_IX_BASE: u64 = 12_000;   // per non-CB instruction overhead
        const CU_ACCT:   u64 =  1_000;    // per account touch (avg)

        fn proto_cu(p: &Protocol) -> (u64, usize) {
            match p {
                Protocol::Phoenix   => (90_000, 6),  // market touch + user ATAs
                Protocol::Whirlpool => (120_000, 8), // sqrt math + tick arrays
                Protocol::Serum     => (160_000, 12),// event/bid/ask/vaults
                Protocol::Raydium   => (140_000, 12),
            }
        }

        let mut cu: u64 = CU_CB_BASE;
        let mut ix_count: usize = 0;
        let mut acct_count: usize = 2; // payer + token program (typical)

        for hop in &route.path {
            let (hop_cu, hop_accts) = proto_cu(&hop.protocol);
            cu = cu.saturating_add(hop_cu);
            ix_count += 1;
            acct_count += hop_accts;
        }

        cu = cu.saturating_add((ix_count as u64) * CU_IX_BASE)
             .saturating_add((acct_count as u64) * CU_ACCT);

        // pad 15% before simulate auto-tune catches exact usage
        Ok((cu.saturating_mul(115)).saturating_div(100))
    }

    pub async fn warm_up_pools(&self) -> Result<(), DarkPoolError> {
        info!("Warming up pool connections...");
        
        let pool_addresses = vec![
            ("8BnEgHoWFysVcuFFX7QztDmzuH8r5ZFvBP2sYwn1XrE", Protocol::Phoenix),
            ("7qbRF6YsyGuLUVs6Y1q64bdVrfe4ZcUUz1JRdoVNUJnm", Protocol::Whirlpool),
            ("9wFFyRfZBsuAha4YcuxcXLKwMxJR43S7fPfQLusDBzvT", Protocol::Serum),
            ("58oQChx4yWmvKdwLLZzBi4ChoCc2fqCUWBkwMihLYQo2", Protocol::Raydium),
        ];
        
        let mut success_count = 0;
        for (address_str, protocol) in pool_addresses {
            if let Ok(address) = Pubkey::from_str(address_str) {
                if self.refresh_pool_with_retries(&address, protocol).await.is_ok() {
                    success_count += 1;
                }
            }
        }
        
        info!("Warmed up {} pools", success_count);
        Ok(())
    }

    async fn refresh_batch_phoenix(&self, items: &[Arc<PoolInfo>]) -> Result<(), DarkPoolError> {
        if items.is_empty() { return Ok(()); }
        let keys: Vec<Pubkey> = items.iter().map(|p| p.address).collect();
        let accs = self.rpc_client.get_multiple_accounts(&keys).await
            .map_err(|e| DarkPoolError::RpcError(e.to_string()))?;
        let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();

        for (i, opt) in accs.into_iter().enumerate() {
            if let Some(acc) = opt {
                if acc.data.len() >= 80 {
                    // base @64..72, quote @72..80
                    let base = u64::from_le_bytes(*array_ref![acc.data, 64, 8]);
                    let quote = u64::from_le_bytes(*array_ref![acc.data, 72, 8]);
                    items[i].base_reserve.store(base, Ordering::Relaxed);
                    items[i].quote_reserve.store(quote, Ordering::Relaxed);
                    items[i].last_update.store(now, Ordering::Relaxed);
                }
            }
        }
        Ok(())
    }

    async fn refresh_batch_whirlpool(&self, items: &[Arc<PoolInfo>]) -> Result<(), DarkPoolError> {
        if items.is_empty() { return Ok(()); }
        let keys: Vec<Pubkey> = items.iter().map(|p| p.address).collect();
        let accs = self.rpc_client.get_multiple_accounts(&keys).await
            .map_err(|e| DarkPoolError::RpcError(e.to_string()))?;
        let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();

        for (i, opt) in accs.into_iter().enumerate() {
            if let Some(acc) = opt {
                if acc.data.len() >= 168 {
                    // liquidity @136..152 (u128), sqrt_price @152..168 (u128)
                    let liq = u128::from_le_bytes(*array_ref![acc.data, 136, 16]);
                    let sqrt_p = u128::from_le_bytes(*array_ref![acc.data, 152, 16]).max(1);
                    // approx reserves in ticks-wide pool
                    let base = ((liq << 64) / sqrt_p).min(u128::from(u64::MAX)) as u64;
                    let quote = ((liq * sqrt_p) >> 64).min(u128::from(u64::MAX)) as u64;

                    items[i].base_reserve.store(base, Ordering::Relaxed);
                    items[i].quote_reserve.store(quote, Ordering::Relaxed);
                    items[i].last_update.store(now, Ordering::Relaxed);
                }
            }
        }
        Ok(())
    }

    async fn refresh_batch_serum(&self, items: &[Arc<PoolInfo>]) -> Result<(), DarkPoolError> {
        if items.is_empty() { return Ok(()); }
        // Phase 1: get market accounts
        let mkeys: Vec<Pubkey> = items.iter().map(|p| p.address).collect();
        let markets = self.rpc_client.get_multiple_accounts(&mkeys).await
            .map_err(|e| DarkPoolError::RpcError(e.to_string()))?;

        // Gather vault pubkeys
        let mut coin_vaults: Vec<Pubkey> = Vec::with_capacity(items.len());
        let mut pc_vaults:   Vec<Pubkey> = Vec::with_capacity(items.len());
        let mut idx_map:     Vec<usize>  = Vec::with_capacity(items.len());

        for (i, opt) in markets.iter().enumerate() {
            if let Some(acc) = opt {
                if acc.data.len() >= 128 {
                    let coin = Pubkey::new_from_array(*array_ref![acc.data, 64, 32]);
                    let pc   = Pubkey::new_from_array(*array_ref![acc.data, 96, 32]);
                    coin_vaults.push(coin);
                    pc_vaults.push(pc);
                    idx_map.push(i);
                }
            }
        }
        // Phase 2: fetch both vault token accounts in bulk
        let mut all_vaults = coin_vaults.clone();
        all_vaults.extend(pc_vaults.iter().cloned());
        if all_vaults.is_empty() { return Ok(()); }

        let vaccts = self.rpc_client.get_multiple_accounts(&all_vaults).await
            .map_err(|e| DarkPoolError::RpcError(e.to_string()))?;
        let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();

        for (j, &i_pool) in idx_map.iter().enumerate() {
            // coin at j, pc at j + coin_vaults.len()
            let coin_opt = &vaccts[j];
            let pc_opt   = &vaccts[j + coin_vaults.len()];
            if let (Some(coin), Some(pc)) = (coin_opt, pc_opt) {
                if coin.data.len() >= 72 && pc.data.len() >= 72 {
                    let base = u64::from_le_bytes(*array_ref![coin.data, 64, 8]);
                    let quote= u64::from_le_bytes(*array_ref![pc.data,  64, 8]);
                    items[i_pool].base_reserve.store(base, Ordering::Relaxed);
                    items[i_pool].quote_reserve.store(quote, Ordering::Relaxed);
                    items[i_pool].last_update.store(now, Ordering::Relaxed);
                }
            }
        }
        Ok(())
    }

    async fn refresh_batch_raydium(&self, items: &[Arc<PoolInfo>]) -> Result<(), DarkPoolError> {
        if items.is_empty() { return Ok(()); }
        // Phase 1: fetch AMM accounts to discover pool token accounts
        let akeys: Vec<Pubkey> = items.iter().map(|p| p.address).collect();
        let amms = self.rpc_client.get_multiple_accounts(&akeys).await
            .map_err(|e| DarkPoolError::RpcError(e.to_string()))?;

        let mut coin_tas: Vec<Pubkey> = Vec::with_capacity(items.len());
        let mut pc_tas:   Vec<Pubkey> = Vec::with_capacity(items.len());
        let mut idx_map:  Vec<usize>  = Vec::with_capacity(items.len());

        for (i, opt) in amms.iter().enumerate() {
            if let Some(acc) = opt {
                if acc.data.len() >= 168 {
                    // pool coin @104..136, pool pc @136..168
                    let coin_ta = Pubkey::new_from_array(*array_ref![acc.data, 104, 32]);
                    let pc_ta   = Pubkey::new_from_array(*array_ref![acc.data, 136, 32]);
                    coin_tas.push(coin_ta);
                    pc_tas.push(pc_ta);
                    idx_map.push(i);
                }
            }
        }
        if coin_tas.is_empty() { return Ok(()); }

        // Phase 2: fetch token accounts in bulk
        let mut all_tas = coin_tas.clone();
        all_tas.extend(pc_tas.iter().cloned());
        let taccs = self.rpc_client.get_multiple_accounts(&all_tas).await
            .map_err(|e| DarkPoolError::RpcError(e.to_string()))?;
        let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();

        for (j, &i_pool) in idx_map.iter().enumerate() {
            let coin_opt = &taccs[j];
            let pc_opt   = &taccs[j + coin_tas.len()];
            if let (Some(coin), Some(pc)) = (coin_opt, pc_opt) {
                if coin.data.len() >= 72 && pc.data.len() >= 72 {
                    let base = u64::from_le_bytes(*array_ref![coin.data, 64, 8]);
                    let quote= u64::from_le_bytes(*array_ref![pc.data,  64, 8]);
                    items[i_pool].base_reserve.store(base, Ordering::Relaxed);
                    items[i_pool].quote_reserve.store(quote, Ordering::Relaxed);
                    items[i_pool].last_update.store(now, Ordering::Relaxed);
                }
            }
        }
        Ok(())
    }

    pub fn shutdown(&self) {
        self.shutdown.store(true, Ordering::Relaxed);
    }

    async fn ensure_route_fresh(&self, route: &Route) -> Result<(), DarkPoolError> {
        let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();
        let mut refreshed = 0usize;

        for hop in &route.path {
            let key = format!("{}:{:?}", hop.address, hop.protocol);
            if let Some(pi) = self.pools.read().await.get(&key).cloned() {
                let last = pi.last_update.load(Ordering::Relaxed);
                if now.saturating_sub(last) > 60 {
                    // refresh a stale hop only
                    self.refresh_pool_reserves(pi.clone()).await?;
                    refreshed += 1;
                }
            }
        }
        if refreshed > 0 { debug!("Refreshed {} stale hop(s) before execution", refreshed); }
        Ok(())
    }

    fn build_with_compute_budget(&self, route_ixs: &[Instruction], cu_limit: u32, cu_price: u64) -> Vec<Instruction> {
        let mut v = Vec::with_capacity(route_ixs.len() + 2);
        v.push(ComputeBudgetInstruction::set_compute_unit_limit(cu_limit));
        v.push(ComputeBudgetInstruction::set_compute_unit_price(cu_price));
        v.extend_from_slice(route_ixs);
        v
    }

    fn clamp_cu(&self, cu: u32) -> u32 {
        // Cluster hard cap ~1.4M. Keep sub-ceiling.
        cu.min(1_400_000).max(50_000)
    }

    async fn adaptive_cu_price(&self) -> u64 {
        // Base from config; add tiny bump if breaker shows pressure.
        let cfg = self.config.read().await;
        let base = cfg.priority_fee_lamports as u64;
        // If failure_count is elevated, nudge fee slightly.
        let fc = self.circuit_breaker.failure_count.load(Ordering::Relaxed);
        if fc >= 3 { base.saturating_add(75) } else { base }
    }

    fn small_jitter_ms(&self, range: u64) -> u64 {
        // Dependency-free jitter
        let nanos = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().subsec_nanos() as u64;
        let mix = nanos ^ (nanos << 13) ^ (nanos >> 7) ^ (nanos << 17);
        let r = if range == 0 { 1 } else { range };
        mix % r
    }

    async fn ensure_ata_if_needed(
        &self,
        owner: &Pubkey,
        mint: &Pubkey,
    ) -> Result<Vec<Instruction>, DarkPoolError> {
        let ata = get_associated_token_address(owner, mint);
        // if account exists, no-op; else create
        match self.rpc_client.get_account(&ata).await {
            Ok(_acc) => Ok(vec![]),
            Err(_) => {
                let ix = create_associated_token_account(
                    &self.wallet.pubkey(), // payer
                    owner,
                    mint,
                    &spl_token::id(),
                );
                Ok(vec![ix])
            }
        }
    }

    async fn score_route_ev(&self, order: &Order, route: &Route) -> Result<(u64, u64), DarkPoolError> {
        // Base from route (micro quote/base)
        let base_micro = route.estimated_price.max(1);
        let gas_cu = self.estimate_gas_for_route(route).await.unwrap_or(300_000);

        // Gas penalty → micro units
        let cu_price = {
            let cfg = self.config.read().await;
            cfg.priority_fee_lamports as u64
        } as u128;
        let amt = order.amount.max(1) as u128;
        let gas_cost_lamports = (gas_cu as u128) * cu_price;
        let gas_penalty_micro = ((gas_cost_lamports * 1_000_000u128) / amt) as u64;

        // Staleness & reliability penalties across hops
        let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();
        let mut stale_bps_total: u64 = 0;
        let mut unreliable_bps_total: u64 = 0;

        for hop in &route.path {
            let key = format!("{}:{:?}", hop.address, hop.protocol);
            if let Some(pi) = self.pools.read().await.get(&key) {
                // Staleness (after 30s, 0.5 bps per extra second, cap 50 bps)
                let age = now.saturating_sub(pi.last_update.load(Ordering::Relaxed));
                let stale_bps = if age <= 30 { 0 } else { ((age - 30) / 2).min(50) } as u64;
                stale_bps_total = stale_bps_total.saturating_add(stale_bps);

                // Reliability (up to 50 bps penalty when success rate low)
                let succ = pi.success_count.load(Ordering::Relaxed);
                let fail = pi.failure_count.load(Ordering::Relaxed);
                let total = succ + fail;
                let sr_q = if total == 0 { 7000u64 } else { (succ.saturating_mul(10_000) / total.max(1)) }; // default 70% if empty
                let unreliability_bps = ((10_000u64 - sr_q) as u128 * 50u128 / 10_000u128) as u64;
                unreliable_bps_total = unreliable_bps_total.saturating_add(unreliability_bps);
            }
        }

        let hops = route.path.len().max(1) as u64;
        let avg_stale_bps = stale_bps_total / hops;
        let avg_unreliable_bps = unreliable_bps_total / hops;

        // Convert bps → micro penalty on base price
        let stale_penalty_micro = (base_micro as u128 * (avg_stale_bps as u128) / 10_000u128) as u64;
        let reliab_penalty_micro = (base_micro as u128 * (avg_unreliable_bps as u128) / 10_000u128) as u64;

        let mut ev = base_micro;
        // Gas: worse for both sides (buy pays more, sell receives less)
        ev = match order.side {
            OrderSide::Buy  => ev.saturating_add(gas_penalty_micro),
            OrderSide::Sell => ev.saturating_sub(gas_penalty_micro),
        };
        // Staleness & reliability: always worsen price
        let total_pen = stale_penalty_micro.saturating_add(reliab_penalty_micro);
        ev = match order.side {
            OrderSide::Buy  => ev.saturating_add(total_pen),
            OrderSide::Sell => ev.saturating_sub(total_pen),
        };

        Ok((ev, gas_cu))
    }

    fn classify_exec_error(&self, msg: &str) -> &'static str {
        let m = msg.to_ascii_lowercase();
        // Slippage / price / liquidity
        if m.contains("slippage") || m.contains("too little received") || m.contains("insufficient output amount")
            || m.contains("insufficient liquidity") || m.contains("no liquidity") {
            return "route_bad_price";
        }
        // Structural conflicts
        if m.contains("accountinuse") || m.contains("would block") || m.contains("locks held") {
            return "contention";
        }
        // Blockhash / freshness issues
        if m.contains("blockhashnotfound") || m.contains("blockhash not found") {
            return "blockhash";
        }
        // Compute budget busts
        if m.contains("computebudget") || m.contains("computelimit") || m.contains("exceeded maximum number of instructions") {
            return "under_cu";
        }
        // Token account missing / owner mismatch
        if m.contains("owner mismatch") || m.contains("account not initialized") || m.contains("no such account") {
            return "account_setup";
        }
        "other"
    }
}

impl Drop for DarkPoolIntegration {
    fn drop(&mut self) {
        self.shutdown();
    }
}

// Helper structs
#[derive(Debug)]
struct SerumMarketInfo {
    coin_vault: Pubkey,
    pc_vault: Pubkey,
    request_queue: Pubkey,
    event_queue: Pubkey,
    bids: Pubkey,
    asks: Pubkey,
    vault_signer: Pubkey,
}

#[derive(Debug)]
struct RaydiumAmmInfo {
    amm_authority: Pubkey,
    amm_open_orders: Pubkey,
    amm_target_orders: Pubkey,
    pool_coin_token_account: Pubkey,
    pool_pc_token_account: Pubkey,
    serum_program_id: Pubkey,
    serum_market: Pubkey,
    serum_bids: Pubkey,
    serum_asks: Pubkey,
    serum_event_queue: Pubkey,
    serum_coin_vault: Pubkey,
    serum_pc_vault: Pubkey,
    serum_vault_signer: Pubkey,
}

impl CircuitBreaker {
    fn new(config: DarkPoolConfig) -> Self {
        Self {
            failure_count: AtomicU64::new(0),
            last_failure_time: AtomicU64::new(0),
            is_open: AtomicBool::new(false),
            config,
        }
    }

    fn is_open(&self) -> bool {
        if !self.is_open.load(Ordering::Relaxed) {
            return false;
        }
        
        let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();
        let last_failure = self.last_failure_time.load(Ordering::Relaxed);
        
        if now - last_failure > 60 {
            self.is_open.store(false, Ordering::Relaxed);
            self.failure_count.store(0, Ordering::Relaxed);
            false
        } else {
            true
        }
    }

    fn record_success(&self) {
        let current = self.failure_count.load(Ordering::Relaxed);
        if current > 0 {
            self.failure_count.fetch_sub(1, Ordering::Relaxed);
        }
    }

    fn record_failure(&self) {
        let count = self.failure_count.fetch_add(1, Ordering::Relaxed) + 1;
        
        if count >= 5 {
            self.is_open.store(true, Ordering::Relaxed);
            self.last_failure_time.store(
                SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs(),
                Ordering::Relaxed,
            );
        }
    }
}

impl Metrics {
    fn new() -> Self {
        Self {
            total_orders: AtomicU64::new(0),
            successful_orders: AtomicU64::new(0),
            failed_orders: AtomicU64::new(0),
            total_volume: AtomicU64::new(0),
            total_gas_used: AtomicU64::new(0),
        }
    }
}

impl Default for DarkPoolConfig {
    fn default() -> Self {
        Self {
            max_slippage_bps: MAX_SLIPPAGE_BPS,
            min_liquidity_usd: MIN_LIQUIDITY_USD,
            max_route_length: MAX_ROUTE_LENGTH,
            enable_multi_hop: true,
            priority_fee_lamports: 1000,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use solana_sdk::signer::keypair::Keypair;

    #[tokio::test]
    async fn test_circuit_breaker() {
        let config = DarkPoolConfig::default();
        let breaker = CircuitBreaker::new(config);
        
        assert!(!breaker.is_open());
        
        for _ in 0..5 {
            breaker.record_failure();
        }
        
        assert!(breaker.is_open());
        
        breaker.record_success();
        assert!(breaker.is_open());
    }

    #[tokio::test]
    async fn test_price_calculation() {
        let integration = DarkPoolIntegration::new(
            DEFAULT_RPC_URL,
            Keypair::new(),
            None,
        ).await.unwrap();
        
        let price = integration.calculate_price_with_slippage(
            1_000_000_000,
            2_000_000_000,
            100_000,
            30,
            OrderSide::Buy,
        );
        
        assert!(price > 0);
    }

    #[tokio::test]
    async fn test_order_id_generation() {
        let integration = DarkPoolIntegration::new(
            DEFAULT_RPC_URL,
            Keypair::new(),
            None,
        ).await.unwrap();
        
        let order = Order {
            base_mint: Pubkey::new_unique(),
            quote_mint: Pubkey::new_unique(),
            amount: 1_000_000,
            side: OrderSide::Buy,
            limit_price: None,
            max_slippage_bps: 100,
            expires_at: None,
            created_at: SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs(),
        };
        
        let id1 = integration.generate_order_id(&order);
        let id2 = integration.generate_order_id(&order);
        
        assert_eq!(id1, id2);
    }

    #[tokio::test]
    async fn test_estimate_output_amount() {
        let integration = DarkPoolIntegration::new(
            DEFAULT_RPC_URL,
            Keypair::new(),
            None,
        ).await.unwrap();
        
        let output = integration.estimate_output_amount(
            1_000_000_000,
            2_000_000_000,
            100_000,
            30,
            OrderSide::Buy,
        );
        
        assert!(output > 0);
        assert!(output < 100_000);
    }

    #[tokio::test]
    async fn test_metrics() {
        let integration = DarkPoolIntegration::new(
            DEFAULT_RPC_URL,
            Keypair::new(),
            None,
        ).await.unwrap();
        
        integration.metrics.total_orders.fetch_add(1, Ordering::Relaxed);
        integration.metrics.successful_orders.fetch_add(1, Ordering::Relaxed);
        
        let (total, successful, _, _) = integration.get_metrics().await;
        assert_eq!(total, 1);
        assert_eq!(successful, 1);
    }

    #[tokio::test]
    async fn test_pool_matching() {
        let integration = DarkPoolIntegration::new(
            DEFAULT_RPC_URL,
            Keypair::new(),
            None,
        ).await.unwrap();
        
        let pool_info = PoolInfo {
            address: Pubkey::new_unique(),
            protocol: Protocol::Phoenix,
            base_reserve: AtomicU64::new(1_000_000),
            quote_reserve: AtomicU64::new(2_000_000),
            fee_bps: 30,
            last_update: AtomicU64::new(0),
            success_count: AtomicU64::new(0),
            failure_count: AtomicU64::new(0),
        };
        
        let order = Order {
            base_mint: Pubkey::new_unique(),
            quote_mint: Pubkey::new_unique(),
            amount: 100_000,
            side: OrderSide::Buy,
            limit_price: None,
            max_slippage_bps: 100,
            expires_at: None,
            created_at: SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs(),
        };
        
        assert!(integration.matches_order(&pool_info, &order));
    }

    #[tokio::test]
    async fn test_gas_estimation() {
        let integration = DarkPoolIntegration::new(
            DEFAULT_RPC_URL,
            Keypair::new(),
            None,
        ).await.unwrap();
        
        let route = Route {
            path: vec![
                Pool {
                    address: Pubkey::new_unique(),
                    protocol: Protocol::Phoenix,
                    base_mint: Pubkey::new_unique(),
                    quote_mint: Pubkey::new_unique(),
                    fee_bps: 30,
                },
            ],
            estimated_price: 1_000_000,
            estimated_gas: 200_000,
        };
        
        let gas = integration.estimate_gas_for_route(&route).await.unwrap();
        assert!(gas > 0);
    }

    #[tokio::test]
    async fn test_shutdown() {
        let integration = DarkPoolIntegration::new(
            DEFAULT_RPC_URL,
            Keypair::new(),
            None,
        ).await.unwrap();
        
        assert!(!integration.shutdown.load(Ordering::Relaxed));
        integration.shutdown();
        assert!(integration.shutdown.load(Ordering::Relaxed));
    }
}
