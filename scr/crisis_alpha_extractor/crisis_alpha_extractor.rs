use anchor_lang::prelude::*;
use arrayref::array_ref;
use borsh::{BorshDeserialize, BorshSerialize};
use dashmap::DashMap;
use rayon::prelude::*;
use solana_client::{
    nonblocking::rpc_client::RpcClient,
    rpc_config::{RpcAccountInfoConfig, RpcProgramAccountsConfig},
    rpc_filter::{Memcmp, MemcmpEncodedBytes, RpcFilterType},
};
use solana_sdk::{
    account::Account,
    commitment_config::CommitmentConfig,
    compute_budget::ComputeBudgetInstruction,
    instruction::Instruction,
    pubkey::Pubkey,
    signature::{Keypair, Signature},
    signer::Signer,
    transaction::Transaction,
};
use std::{
    collections::{BTreeMap, VecDeque},
    str::FromStr,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};
use tokio::{
    sync::{mpsc, RwLock, Semaphore},
    time::{interval, sleep},
};

const RAYDIUM_V4: &str = "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8";
const ORCA_V2: &str = "9W959DqEETiGZocYWCQPaJ6sBmUzgfxXfqGeTEdp3aQP";
const SERUM_V3: &str = "9xQeWvG816bUx9EPjHmaT23yvVM2ZWbrrpZb9PusVFin";
const JUPITER_V6: &str = "JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4";

const MAX_SLIPPAGE_BPS: u16 = 300;
const MIN_PROFIT_BPS: u16 = 150;
const CRISIS_VOLATILITY_THRESHOLD: f64 = 0.15;
const LIQUIDATION_DISCOUNT: f64 = 0.05;
const MAX_CONCURRENT_TXS: usize = 128;
const TX_RETRY_ATTEMPTS: u8 = 3;
const COMPUTE_UNITS: u32 = 1_400_000;
const PRIORITY_FEE_PERCENTILE: f64 = 0.95;

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum CrisisType {
    Liquidation,
    VolatilitySpike,
    FlashCrash,
    NetworkCongestion,
    ProtocolFailure,
}

#[derive(Debug, Clone)]
pub struct CrisisOpportunity {
    pub crisis_type: CrisisType,
    pub token_mint: Pubkey,
    pub entry_price: u64,
    pub target_price: u64,
    pub volume: u64,
    pub confidence: f64,
    pub deadline: Instant,
    pub routes: Vec<ArbitrageRoute>,
}

#[derive(Debug, Clone)]
pub struct ArbitrageRoute {
    pub source_dex: Pubkey,
    pub target_dex: Pubkey,
    pub intermediate_token: Option<Pubkey>,
    pub expected_profit: u64,
    pub gas_estimate: u64,
}

#[derive(Debug, Clone)]
pub struct MarketState {
    pub price: u64,
    pub volume_24h: u64,
    pub volatility: f64,
    pub bid_ask_spread: u64,
    pub liquidity_depth: u64,
    pub last_update: Instant,
}

pub struct CrisisAlphaExtractor {
    rpc: Arc<RpcClient>,
    wallet: Arc<Keypair>,
    market_states: Arc<DashMap<Pubkey, MarketState>>,
    opportunity_queue: Arc<RwLock<VecDeque<CrisisOpportunity>>>,
    execution_semaphore: Arc<Semaphore>,
    priority_fee_cache: Arc<AtomicU64>,
    network_congestion: Arc<AtomicBool>,
    volatility_tracker: Arc<RwLock<BTreeMap<Pubkey, Vec<f64>>>>,
}

impl CrisisAlphaExtractor {
    pub fn new(rpc_url: &str, wallet: Keypair) -> Self {
        Self {
            rpc: Arc::new(RpcClient::new_with_commitment(
                rpc_url.to_string(),
                CommitmentConfig::processed(),
            )),
            wallet: Arc::new(wallet),
            market_states: Arc::new(DashMap::new()),
            opportunity_queue: Arc::new(RwLock::new(VecDeque::with_capacity(1024))),
            execution_semaphore: Arc::new(Semaphore::new(MAX_CONCURRENT_TXS)),
            priority_fee_cache: Arc::new(AtomicU64::new(50_000)),
            network_congestion: Arc::new(AtomicBool::new(false)),
            volatility_tracker: Arc::new(RwLock::new(BTreeMap::new())),
        }
    }

    pub async fn run(&self) -> Result<(), Box<dyn std::error::Error>> {
        let (tx, mut rx) = mpsc::channel::<CrisisOpportunity>(512);

        let monitor_handle = self.spawn_crisis_monitor(tx.clone());
        let volatility_handle = self.spawn_volatility_tracker();
        let liquidation_handle = self.spawn_liquidation_monitor(tx.clone());
        let network_handle = self.spawn_network_monitor();
        let executor_handle = self.spawn_executor();

        tokio::select! {
            _ = monitor_handle => {},
            _ = volatility_handle => {},
            _ = liquidation_handle => {},
            _ = network_handle => {},
            _ = executor_handle => {},
        }

        Ok(())
    }

    fn spawn_crisis_monitor(
        &self,
        tx: mpsc::Sender<CrisisOpportunity>,
    ) -> tokio::task::JoinHandle<()> {
        let rpc = Arc::clone(&self.rpc);
        let market_states = Arc::clone(&self.market_states);
        let volatility_tracker = Arc::clone(&self.volatility_tracker);

        tokio::spawn(async move {
            let mut interval = interval(Duration::from_millis(100));
            
            loop {
                interval.tick().await;
                
                let markets = Self::fetch_market_data(&rpc).await;
                
                for (token_mint, new_state) in markets {
                    if let Some(mut entry) = market_states.get_mut(&token_mint) {
                        let price_change = (new_state.price as f64 - entry.price as f64).abs() 
                            / entry.price as f64;
                        
                        if price_change > CRISIS_VOLATILITY_THRESHOLD {
                            if let Some(opportunity) = Self::analyze_crisis_opportunity(
                                &token_mint,
                                &entry,
                                &new_state,
                                &volatility_tracker,
                            ).await {
                                let _ = tx.send(opportunity).await;
                            }
                        }
                        
                        *entry = new_state;
                    } else {
                        market_states.insert(token_mint, new_state);
                    }
                }
            }
        })
    }

    fn spawn_volatility_tracker(&self) -> tokio::task::JoinHandle<()> {
        let market_states = Arc::clone(&self.market_states);
        let volatility_tracker = Arc::clone(&self.volatility_tracker);

        tokio::spawn(async move {
            let mut interval = interval(Duration::from_secs(1));
            
            loop {
                interval.tick().await;
                
                let mut tracker = volatility_tracker.write().await;
                
                for entry in market_states.iter() {
                    let token_mint = *entry.key();
                    let state = entry.value();
                    
                    let prices = tracker.entry(token_mint).or_insert_with(Vec::new);
                    prices.push(state.price as f64);
                    
                    if prices.len() > 300 {
                        prices.remove(0);
                    }
                    
                    if prices.len() >= 20 {
                        let volatility = Self::calculate_volatility(prices);
                        market_states.get_mut(&token_mint).map(|mut s| {
                            s.volatility = volatility;
                        });
                    }
                }
            }
        })
    }

    fn spawn_liquidation_monitor(
        &self,
        tx: mpsc::Sender<CrisisOpportunity>,
    ) -> tokio::task::JoinHandle<()> {
        let rpc = Arc::clone(&self.rpc);
        
        tokio::spawn(async move {
            let mut interval = interval(Duration::from_millis(250));
            
            loop {
                interval.tick().await;
                
                let liquidations = Self::scan_liquidation_opportunities(&rpc).await;
                
                for opportunity in liquidations {
                    let _ = tx.send(opportunity).await;
                }
            }
        })
    }

    fn spawn_network_monitor(&self) -> tokio::task::JoinHandle<()> {
        let rpc = Arc::clone(&self.rpc);
        let network_congestion = Arc::clone(&self.network_congestion);
        let priority_fee_cache = Arc::clone(&self.priority_fee_cache);

        tokio::spawn(async move {
            let mut interval = interval(Duration::from_secs(2));
            
            loop {
                interval.tick().await;
                
                if let Ok(recent_fees) = rpc.get_recent_prioritization_fees(&[]).await {
                    if !recent_fees.is_empty() {
                        let mut fees: Vec<u64> = recent_fees.iter()
                            .map(|f| f.prioritization_fee)
                            .collect();
                        fees.sort_unstable();
                        
                        let percentile_idx = ((fees.len() as f64) * PRIORITY_FEE_PERCENTILE) as usize;
                        let target_fee = fees[percentile_idx.min(fees.len() - 1)];
                        
                        priority_fee_cache.store(target_fee, Ordering::Relaxed);
                        
                        let avg_fee = fees.iter().sum::<u64>() / fees.len() as u64;
                        network_congestion.store(avg_fee > 100_000, Ordering::Relaxed);
                    }
                }
            }
        })
    }

    fn spawn_executor(&self) -> tokio::task::JoinHandle<()> {
        let rpc = Arc::clone(&self.rpc);
        let wallet = Arc::clone(&self.wallet);
        let opportunity_queue = Arc::clone(&self.opportunity_queue);
        let execution_semaphore = Arc::clone(&self.execution_semaphore);
        let priority_fee_cache = Arc::clone(&self.priority_fee_cache);

        tokio::spawn(async move {
            let mut interval = interval(Duration::from_millis(50));
            
            loop {
                interval.tick().await;
                
                let opportunities: Vec<CrisisOpportunity> = {
                    let mut queue = opportunity_queue.write().await;
                    let mut opps = Vec::new();
                    
                    while let Some(opp) = queue.pop_front() {
                        if opp.deadline > Instant::now() {
                            opps.push(opp);
                        }
                        if opps.len() >= 16 {
                            break;
                        }
                    }
                    
                    opps
                };

                let handles: Vec<_> = opportunities
                    .into_iter()
                    .map(|opportunity| {
                        let rpc = Arc::clone(&rpc);
                        let wallet = Arc::clone(&wallet);
                        let semaphore = Arc::clone(&execution_semaphore);
                        let priority_fee = priority_fee_cache.load(Ordering::Relaxed);
                        
                        tokio::spawn(async move {
                            let _permit = semaphore.acquire().await.unwrap();
                            Self::execute_opportunity(
                                &rpc,
                                &wallet,
                                opportunity,
                                priority_fee,
                            ).await
                        })
                    })
                    .collect();

                futures::future::join_all(handles).await;
            }
        })
    }

    async fn fetch_market_data(rpc: &RpcClient) -> Vec<(Pubkey, MarketState)> {
        let mut markets = Vec::new();
        
        let filters = vec![RpcFilterType::DataSize(388)];
        let config = RpcProgramAccountsConfig {
            filters: Some(filters),
            account_config: RpcAccountInfoConfig {
                encoding: Some(solana_account_decoder::UiAccountEncoding::Base64),
                commitment: Some(CommitmentConfig::processed()),
                ..Default::default()
            },
            ..Default::default()
        };

        if let Ok(accounts) = rpc.get_program_accounts_with_config(
            &Pubkey::from_str(RAYDIUM_V4).unwrap(),
            config,
        ).await {
            for (pubkey, account) in accounts {
                if let Some(state) = Self::parse_market_account(&account.data) {
                    markets.push((pubkey, state));
                }
            }
        }

        markets
    }

    fn parse_market_account(data: &[u8]) -> Option<MarketState> {
        if data.len() < 388 {
            return None;
        }

        let price = u64::from_le_bytes(*array_ref![data, 85, 8]);
        let volume = u64::from_le_bytes(*array_ref![data, 213, 8]);
        let liquidity = u64::from_le_bytes(*array_ref![data, 221, 8]);
        
                Some(MarketState {
            price,
            volume_24h: volume,
            volatility: 0.0,
            bid_ask_spread: 0,
            liquidity_depth: liquidity,
            last_update: Instant::now(),
        })
    }

    async fn analyze_crisis_opportunity(
        token_mint: &Pubkey,
        old_state: &MarketState,
        new_state: &MarketState,
        volatility_tracker: &Arc<RwLock<BTreeMap<Pubkey, Vec<f64>>>>,
    ) -> Option<CrisisOpportunity> {
        let price_drop = (old_state.price as f64 - new_state.price as f64) / old_state.price as f64;
        
        if price_drop.abs() < 0.05 {
            return None;
        }

        let tracker = volatility_tracker.read().await;
        let historical_volatility = tracker.get(token_mint)
            .map(|prices| Self::calculate_volatility(prices))
            .unwrap_or(0.0);

        let volume_spike = new_state.volume_24h as f64 / old_state.volume_24h.max(1) as f64;
        let liquidity_ratio = new_state.liquidity_depth as f64 / old_state.liquidity_depth.max(1) as f64;
        
        let confidence = (price_drop.abs() * 2.0 + volume_spike + (1.0 / liquidity_ratio)) / 4.0;
        
        if confidence < 0.7 {
            return None;
        }

        let routes = Self::find_arbitrage_routes(token_mint, new_state);
        
        if routes.is_empty() {
            return None;
        }

        Some(CrisisOpportunity {
            crisis_type: if price_drop > 0.2 { 
                CrisisType::FlashCrash 
            } else if historical_volatility > 0.3 {
                CrisisType::VolatilitySpike
            } else {
                CrisisType::ProtocolFailure
            },
            token_mint: *token_mint,
            entry_price: new_state.price,
            target_price: (new_state.price as f64 * (1.0 + price_drop.abs() * 0.3)) as u64,
            volume: (new_state.liquidity_depth as f64 * 0.1) as u64,
            confidence,
            deadline: Instant::now() + Duration::from_secs(30),
            routes,
        })
    }

    fn calculate_volatility(prices: &[f64]) -> f64 {
        if prices.len() < 2 {
            return 0.0;
        }

        let returns: Vec<f64> = prices.windows(2)
            .map(|w| (w[1] - w[0]) / w[0])
            .collect();

        let mean = returns.iter().sum::<f64>() / returns.len() as f64;
        let variance = returns.iter()
            .map(|r| (r - mean).powi(2))
            .sum::<f64>() / returns.len() as f64;

        variance.sqrt()
    }

    fn find_arbitrage_routes(token_mint: &Pubkey, state: &MarketState) -> Vec<ArbitrageRoute> {
        let mut routes = Vec::new();
        
        let dexes = [
            Pubkey::from_str(RAYDIUM_V4).unwrap(),
            Pubkey::from_str(ORCA_V2).unwrap(),
            Pubkey::from_str(SERUM_V3).unwrap(),
        ];

        let usdc = Pubkey::from_str("EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v").unwrap();
        let sol = Pubkey::from_str("So11111111111111111111111111111111111111112").unwrap();

        for i in 0..dexes.len() {
            for j in 0..dexes.len() {
                if i != j {
                    let gas_estimate = 5_000 + (state.liquidity_depth / 1_000_000).min(50_000);
                    let expected_profit = (state.price as f64 * 0.002 * state.volume_24h as f64 / 1e9) as u64;
                    
                    if expected_profit > gas_estimate * 2 {
                        routes.push(ArbitrageRoute {
                            source_dex: dexes[i],
                            target_dex: dexes[j],
                            intermediate_token: None,
                            expected_profit,
                            gas_estimate,
                        });

                        routes.push(ArbitrageRoute {
                            source_dex: dexes[i],
                            target_dex: dexes[j],
                            intermediate_token: Some(usdc),
                            expected_profit: expected_profit * 9 / 10,
                            gas_estimate: gas_estimate * 3 / 2,
                        });

                        routes.push(ArbitrageRoute {
                            source_dex: dexes[i],
                            target_dex: dexes[j],
                            intermediate_token: Some(sol),
                            expected_profit: expected_profit * 95 / 100,
                            gas_estimate: gas_estimate * 4 / 3,
                        });
                    }
                }
            }
        }

        routes.sort_by_key(|r| std::cmp::Reverse(r.expected_profit.saturating_sub(r.gas_estimate)));
        routes.truncate(5);
        routes
    }

    async fn scan_liquidation_opportunities(rpc: &RpcClient) -> Vec<CrisisOpportunity> {
        let mut opportunities = Vec::new();
        
        let lending_programs = [
            "So1endDq2YkqhipRh3WViPa8hdiSpxWy6z3Z6tMCpAo",
            "MFv2hWf31Z9kbCa1snEPYctwafyhdvnV7FZnsebVacA",
            "Port7uDYB3wk6GJAw4KT1WpTeMtSu9bTcChBHkX2LfR",
        ];

        for program in &lending_programs {
            if let Ok(program_id) = Pubkey::from_str(program) {
                let filters = vec![RpcFilterType::DataSize(256)];
                let config = RpcProgramAccountsConfig {
                    filters: Some(filters),
                    account_config: RpcAccountInfoConfig {
                        encoding: Some(solana_account_decoder::UiAccountEncoding::Base64),
                        commitment: Some(CommitmentConfig::processed()),
                        ..Default::default()
                    },
                    ..Default::default()
                };

                if let Ok(accounts) = rpc.get_program_accounts_with_config(&program_id, config).await {
                    for (pubkey, account) in accounts {
                        if let Some(opp) = Self::parse_liquidation_account(&pubkey, &account.data) {
                            opportunities.push(opp);
                        }
                    }
                }
            }
        }

        opportunities
    }

    fn parse_liquidation_account(pubkey: &Pubkey, data: &[u8]) -> Option<CrisisOpportunity> {
        if data.len() < 128 {
            return None;
        }

        let health_ratio = u64::from_le_bytes(*array_ref![data, 32, 8]);
        let collateral_value = u64::from_le_bytes(*array_ref![data, 40, 8]);
        let debt_value = u64::from_le_bytes(*array_ref![data, 48, 8]);
        
        if health_ratio > 11000 || collateral_value < 1_000_000 {
            return None;
        }

        let liquidation_bonus = collateral_value * 5 / 100;
        let expected_profit = liquidation_bonus.saturating_sub(50_000);
        
        if expected_profit < 100_000 {
            return None;
        }

        Some(CrisisOpportunity {
            crisis_type: CrisisType::Liquidation,
            token_mint: *pubkey,
            entry_price: debt_value,
            target_price: collateral_value,
            volume: debt_value,
            confidence: 0.95,
            deadline: Instant::now() + Duration::from_secs(10),
            routes: vec![ArbitrageRoute {
                source_dex: *pubkey,
                target_dex: Pubkey::from_str(JUPITER_V6).unwrap(),
                intermediate_token: None,
                expected_profit,
                gas_estimate: 100_000,
            }],
        })
    }

    async fn execute_opportunity(
        rpc: &RpcClient,
        wallet: &Keypair,
        opportunity: CrisisOpportunity,
        priority_fee: u64,
    ) -> Result<Signature, Box<dyn std::error::Error>> {
        let route = opportunity.routes.first()
            .ok_or("No route available")?;

        let mut instructions = vec![
            ComputeBudgetInstruction::set_compute_unit_limit(COMPUTE_UNITS),
            ComputeBudgetInstruction::set_compute_unit_price(priority_fee),
        ];

        match opportunity.crisis_type {
            CrisisType::Liquidation => {
                instructions.extend(Self::build_liquidation_instructions(
                    wallet,
                    &opportunity,
                    route,
                )?);
            }
            CrisisType::VolatilitySpike | CrisisType::FlashCrash => {
                instructions.extend(Self::build_arbitrage_instructions(
                    wallet,
                    &opportunity,
                    route,
                )?);
            }
            CrisisType::NetworkCongestion => {
                instructions.extend(Self::build_congestion_arbitrage_instructions(
                    wallet,
                    &opportunity,
                    route,
                )?);
            }
            CrisisType::ProtocolFailure => {
                instructions.extend(Self::build_recovery_instructions(
                    wallet,
                    &opportunity,
                    route,
                )?);
            }
        }

        let recent_blockhash = rpc.get_latest_blockhash().await?;
        let transaction = Transaction::new_signed_with_payer(
            &instructions,
            Some(&wallet.pubkey()),
            &[wallet],
            recent_blockhash,
        );

        let mut retries = 0;
        loop {
            match rpc.send_transaction(&transaction).await {
                Ok(sig) => return Ok(sig),
                Err(e) if retries < TX_RETRY_ATTEMPTS => {
                    retries += 1;
                    sleep(Duration::from_millis(100 * retries as u64)).await;
                }
                Err(e) => return Err(e.into()),
            }
        }
    }

    fn build_liquidation_instructions(
        wallet: &Keypair,
        opportunity: &CrisisOpportunity,
        route: &ArbitrageRoute,
    ) -> Result<Vec<Instruction>, Box<dyn std::error::Error>> {
        let mut instructions = Vec::new();

        let liquidate_ix = solana_sdk::instruction::Instruction {
            program_id: route.source_dex,
            accounts: vec![
                AccountMeta::new(wallet.pubkey(), true),
                AccountMeta::new(opportunity.token_mint, false),
                AccountMeta::new_readonly(route.target_dex, false),
            ],
            data: Self::encode_liquidation_data(opportunity.volume, opportunity.entry_price),
        };
        
        instructions.push(liquidate_ix);

        if let Some(intermediate) = &route.intermediate_token {
            let swap_ix = solana_sdk::instruction::Instruction {
                program_id: route.target_dex,
                accounts: vec![
                    AccountMeta::new(wallet.pubkey(), true),
                    AccountMeta::new(*intermediate, false),
                    AccountMeta::new(opportunity.token_mint, false),
                ],
                data: Self::encode_swap_data(opportunity.volume, 0),
            };
            instructions.push(swap_ix);
        }

        Ok(instructions)
    }

    fn build_arbitrage_instructions(
        wallet: &Keypair,
        opportunity: &CrisisOpportunity,
        route: &ArbitrageRoute,
    ) -> Result<Vec<Instruction>, Box<dyn std::error::Error>> {
        let mut instructions = Vec::new();
        
        let amount_in = opportunity.volume;
        let min_amount_out = (opportunity.target_price as f64 * 0.97) as u64;

        let buy_ix = solana_sdk::instruction::Instruction {
            program_id: route.source_dex,
            accounts: vec![
                AccountMeta::new(wallet.pubkey(), true),
                AccountMeta::new(opportunity.token_mint, false),
                AccountMeta::new_readonly(spl_token::id(), false),
            ],
            data: Self::encode_swap_data(amount_in, min_amount_out),
        };
        
        instructions.push(buy_ix);

        let sell_ix = solana_sdk::instruction::Instruction {
            program_id: route.target_dex,
            accounts: vec![
                AccountMeta::new(wallet.pubkey(), true),
                AccountMeta::new(opportunity.token_mint, false),
                AccountMeta::new_readonly(spl_token::id(), false),
            ],
            data: Self::encode_swap_data(amount_in, opportunity.target_price),
        };
        
        instructions.push(sell_ix);

        Ok(instructions)
    }

    fn build_congestion_arbitrage_instructions(
        wallet: &Keypair,
        opportunity: &CrisisOpportunity,
        route: &ArbitrageRoute,
    ) -> Result<Vec<Instruction>, Box<dyn std::error::Error>> {
        Self::build_arbitrage_instructions(wallet, opportunity, route)
    }

    fn build_recovery_instructions(
        wallet: &Keypair,
        opportunity: &CrisisOpportunity,
        route: &ArbitrageRoute,
    ) -> Result<Vec<Instruction>, Box<dyn std::error::Error>> {
        Self::build_arbitrage_instructions(wallet, opportunity, route)
    }

        fn encode_liquidation_data(amount: u64, min_collateral: u64) -> Vec<u8> {
        let mut data = vec![0xf7, 0x8a, 0x1d, 0x4e]; // Liquidation discriminator
        data.extend_from_slice(&amount.to_le_bytes());
        data.extend_from_slice(&min_collateral.to_le_bytes());
        data
    }

    fn encode_swap_data(amount_in: u64, min_amount_out: u64) -> Vec<u8> {
        let mut data = vec![0x22, 0x51, 0x8a, 0x9c]; // Swap discriminator
        data.extend_from_slice(&amount_in.to_le_bytes());
        data.extend_from_slice(&min_amount_out.to_le_bytes());
        data.extend_from_slice(&MAX_SLIPPAGE_BPS.to_le_bytes());
        data
    }

    pub async fn get_network_stats(&self) -> NetworkStats {
        NetworkStats {
            congested: self.network_congestion.load(Ordering::Relaxed),
            priority_fee: self.priority_fee_cache.load(Ordering::Relaxed),
            opportunities_queued: self.opportunity_queue.read().await.len(),
            active_executions: MAX_CONCURRENT_TXS - self.execution_semaphore.available_permits(),
        }
    }

    pub async fn emergency_shutdown(&self) {
        self.opportunity_queue.write().await.clear();
        self.market_states.clear();
        self.volatility_tracker.write().await.clear();
    }

    pub fn validate_opportunity(opportunity: &CrisisOpportunity) -> bool {
        if opportunity.volume == 0 || opportunity.entry_price == 0 {
            return false;
        }

        if opportunity.routes.is_empty() {
            return false;
        }

        if opportunity.deadline < Instant::now() {
            return false;
        }

        let expected_profit = opportunity.target_price.saturating_sub(opportunity.entry_price);
        let min_profit_threshold = opportunity.entry_price * MIN_PROFIT_BPS as u64 / 10_000;
        
        if expected_profit < min_profit_threshold {
            return false;
        }

        true
    }

    async fn update_priority_fee(&self, recent_performance: &[TxResult]) {
        let success_rate = recent_performance.iter()
            .filter(|r| r.success)
            .count() as f64 / recent_performance.len().max(1) as f64;

        let current_fee = self.priority_fee_cache.load(Ordering::Relaxed);
        
        let new_fee = if success_rate < 0.7 {
            (current_fee as f64 * 1.2).min(5_000_000.0) as u64
        } else if success_rate > 0.9 {
            (current_fee as f64 * 0.95).max(10_000.0) as u64
        } else {
            current_fee
        };

        self.priority_fee_cache.store(new_fee, Ordering::Relaxed);
    }

    fn calculate_optimal_size(
        opportunity: &CrisisOpportunity,
        available_capital: u64,
        risk_limit: f64,
    ) -> u64 {
        let max_size = opportunity.volume / 10;
        let risk_adjusted_size = (available_capital as f64 * risk_limit) as u64;
        let liquidity_constrained_size = opportunity.volume / 20;
        
        max_size.min(risk_adjusted_size).min(liquidity_constrained_size)
    }

    async fn verify_route_profitability(
        &self,
        route: &ArbitrageRoute,
        amount: u64,
    ) -> Result<bool, Box<dyn std::error::Error>> {
        let source_quote = self.get_dex_quote(&route.source_dex, amount, true).await?;
        let target_quote = self.get_dex_quote(&route.target_dex, source_quote, false).await?;
        
        let total_gas = route.gas_estimate + self.priority_fee_cache.load(Ordering::Relaxed);
        let net_profit = target_quote.saturating_sub(amount).saturating_sub(total_gas);
        
        Ok(net_profit > amount * MIN_PROFIT_BPS as u64 / 10_000)
    }

    async fn get_dex_quote(
        &self,
        dex: &Pubkey,
        amount: u64,
        is_buy: bool,
    ) -> Result<u64, Box<dyn std::error::Error>> {
        let account = self.rpc.get_account(dex).await?;
        
        if account.data.len() < 256 {
            return Err("Invalid DEX account data".into());
        }

        let reserve_a = u64::from_le_bytes(*array_ref![account.data, 64, 8]);
        let reserve_b = u64::from_le_bytes(*array_ref![account.data, 72, 8]);
        
        let (reserve_in, reserve_out) = if is_buy {
            (reserve_b, reserve_a)
        } else {
            (reserve_a, reserve_b)
        };

        let amount_with_fee = amount * 997;
        let numerator = amount_with_fee * reserve_out;
        let denominator = reserve_in * 1000 + amount_with_fee;
        
        Ok(numerator / denominator)
    }

    pub async fn get_performance_metrics(&self) -> PerformanceMetrics {
        let market_count = self.market_states.len();
        let avg_volatility = self.market_states.iter()
            .map(|e| e.value().volatility)
            .sum::<f64>() / market_count.max(1) as f64;

        PerformanceMetrics {
            markets_tracked: market_count,
            average_volatility: avg_volatility,
            opportunities_found: self.opportunity_queue.read().await.len(),
            network_congested: self.network_congestion.load(Ordering::Relaxed),
            current_priority_fee: self.priority_fee_cache.load(Ordering::Relaxed),
        }
    }

    fn estimate_transaction_size(instructions: &[Instruction]) -> usize {
        let base_size = 300;
        let per_instruction = 50;
        let per_account = 32;
        
        let total_accounts = instructions.iter()
            .flat_map(|ix| &ix.accounts)
            .count();
        
        base_size + (instructions.len() * per_instruction) + (total_accounts * per_account)
    }

    async fn preflight_transaction(
        &self,
        transaction: &Transaction,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let result = self.rpc.simulate_transaction(transaction).await?;
        
        if let Some(err) = result.value.err {
            return Err(format!("Simulation failed: {:?}", err).into());
        }

        if let Some(logs) = result.value.logs {
            for log in logs {
                if log.contains("insufficient") || log.contains("failed") {
                    return Err(format!("Preflight warning: {}", log).into());
                }
            }
        }

        Ok(())
    }

    pub fn calculate_risk_score(opportunity: &CrisisOpportunity) -> f64 {
        let base_risk = match opportunity.crisis_type {
            CrisisType::Liquidation => 0.2,
            CrisisType::VolatilitySpike => 0.4,
            CrisisType::FlashCrash => 0.6,
            CrisisType::NetworkCongestion => 0.3,
            CrisisType::ProtocolFailure => 0.8,
        };

        let volume_risk = 1.0 - (opportunity.volume as f64 / 1e9).min(1.0);
        let time_risk = (opportunity.deadline.duration_since(Instant::now()).as_secs() as f64 / 30.0).min(1.0);
        let confidence_adjustment = 1.0 - opportunity.confidence;

        (base_risk + volume_risk + time_risk + confidence_adjustment) / 4.0
    }

    fn prioritize_opportunities(opportunities: &mut VecDeque<CrisisOpportunity>) {
        let mut sorted: Vec<_> = opportunities.drain(..).collect();
        
        sorted.sort_by(|a, b| {
            let a_score = (a.confidence * 100.0) as i64 
                + (a.routes[0].expected_profit / 1_000_000) as i64
                - (Self::calculate_risk_score(a) * 50.0) as i64;
            
            let b_score = (b.confidence * 100.0) as i64
                + (b.routes[0].expected_profit / 1_000_000) as i64
                - (Self::calculate_risk_score(b) * 50.0) as i64;
            
            b_score.cmp(&a_score)
        });

        opportunities.extend(sorted);
    }

    pub async fn health_check(&self) -> HealthStatus {
        let markets_ok = !self.market_states.is_empty();
        let executor_ok = self.execution_semaphore.available_permits() > 0;
        let queue_ok = self.opportunity_queue.read().await.len() < 500;
        
        HealthStatus {
            is_healthy: markets_ok && executor_ok && queue_ok,
            markets_tracked: self.market_states.len(),
            queue_depth: self.opportunity_queue.read().await.len(),
            available_permits: self.execution_semaphore.available_permits(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct NetworkStats {
    pub congested: bool,
    pub priority_fee: u64,
    pub opportunities_queued: usize,
    pub active_executions: usize,
}

#[derive(Debug)]
struct TxResult {
    success: bool,
    profit: i64,
    gas_used: u64,
}

#[derive(Debug)]
pub struct PerformanceMetrics {
    pub markets_tracked: usize,
    pub average_volatility: f64,
    pub opportunities_found: usize,
    pub network_congested: bool,
    pub current_priority_fee: u64,
}

#[derive(Debug)]
pub struct HealthStatus {
    pub is_healthy: bool,
    pub markets_tracked: usize,
    pub queue_depth: usize,
    pub available_permits: usize,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_volatility_calculation() {
        let prices = vec![100.0, 102.0, 98.0, 101.0, 99.0];
        let volatility = CrisisAlphaExtractor::calculate_volatility(&prices);
        assert!(volatility > 0.0 && volatility < 1.0);
    }

    #[test]
    fn test_opportunity_validation() {
        let opportunity = CrisisOpportunity {
            crisis_type: CrisisType::VolatilitySpike,
            token_mint: Pubkey::new_unique(),
            entry_price: 100_000_000,
            target_price: 102_000_000,
            volume: 1_000_000_000,
            confidence: 0.85,
            deadline: Instant::now() + Duration::from_secs(30),
            routes: vec![ArbitrageRoute {
                source_dex: Pubkey::new_unique(),
                target_dex: Pubkey::new_unique(),
                intermediate_token: None,
                expected_profit: 2_000_000,
                gas_estimate: 50_000,
            }],
        };
        
        assert!(CrisisAlphaExtractor::validate_opportunity(&opportunity));
    }

    #[test]
    fn test_risk_score_calculation() {
        let opportunity = CrisisOpportunity {
            crisis_type: CrisisType::Liquidation,
            token_mint: Pubkey::new_unique(),
            entry_price: 100_000_000,
            target_price: 105_000_000,
            volume: 10_000_000_000,
            confidence: 0.95,
            deadline: Instant::now() + Duration::from_secs(20),
            routes: vec![],
        };
        
        let risk = CrisisAlphaExtractor::calculate_risk_score(&opportunity);
        assert!(risk >= 0.0 && risk <= 1.0);
    }
}

