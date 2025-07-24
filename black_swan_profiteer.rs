use anchor_lang::prelude::*;
use arrayref::{array_ref, array_refs};
use bytemuck::{cast_slice, from_bytes, Pod, Zeroable};
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::{
    commitment_config::CommitmentConfig,
    compute_budget::ComputeBudgetInstruction,
    instruction::{AccountMeta, Instruction},
    pubkey::Pubkey,
    signature::{Keypair, Signer},
    sysvar,
    transaction::Transaction,
};
use spl_token::instruction as token_instruction;
use std::{
    collections::HashMap,
    mem::size_of,
    str::FromStr,
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::{
    sync::{Mutex, RwLock},
    time::{interval, sleep},
};

const PYTH_PROGRAM_ID: &str = "FsJ3A3u2vn5cTVofAjvy6y5kwABJAqYWpe4975bi2epH";
const SERUM_DEX_V3: &str = "9xQeWvG816bUx9EPjHmaT23yvVM2ZWbrrpZb9PusVFin";
const MANGO_V4_ID: &str = "4MangoMjqJ2firMokCjjGgoK8d4MXcrgL7XJaL3w6fVg";
const DRIFT_PROTOCOL: &str = "dRiftyHA39MWEi3m9aunc5MzRF1JYuBsbn6VPcn33UH";

const MAX_SLIPPAGE_BPS: u64 = 500;
const MIN_PROFIT_BPS: u64 = 250;
const LIQUIDATION_DISCOUNT_BPS: u64 = 500;
const COMPUTE_UNITS: u32 = 1_400_000;
const PRIORITY_FEE_LAMPORTS: u64 = 50_000;

#[repr(C)]
#[derive(Clone, Copy, Debug, Default, Pod, Zeroable)]
struct SerumMarketState {
    account_flags: u64,
    own_address: [u64; 4],
    vault_signer_nonce: u64,
    base_mint: [u64; 4],
    quote_mint: [u64; 4],
    base_vault: [u64; 4],
    base_deposits_total: u64,
    base_fees_accrued: u64,
    quote_vault: [u64; 4],
    quote_deposits_total: u64,
    quote_fees_accrued: u64,
    quote_dust_threshold: u64,
    request_queue: [u64; 4],
    event_queue: [u64; 4],
    bids: [u64; 4],
    asks: [u64; 4],
    base_lot_size: u64,
    quote_lot_size: u64,
    fee_rate_bps: u64,
    referrer_rebate_accrued_total: u64,
}

#[repr(C)]
#[derive(Clone, Copy, Debug, Default, Pod, Zeroable)]
struct MangoAccount {
    group: Pubkey,
    owner: Pubkey,
    name: [u8; 32],
    delegate: Pubkey,
    account_num: u32,
    being_liquidated: u8,
    in_health_region: u8,
    bump: u8,
    padding: [u8; 1],
    net_deposits: i64,
    perp_spot_transfers: i64,
    health_region_begin_init_health: i64,
    frozen_until: u64,
    buyback_fees_accrued_current: u64,
    buyback_fees_accrued_previous: u64,
    buyback_fees_expiry_timestamp: u64,
    reserved: [u8; 208],
}

#[repr(C)]
#[derive(Clone, Copy, Debug, Default, Pod, Zeroable)]
struct DriftUser {
    authority: Pubkey,
    delegate: Pubkey,
    name: [u8; 32],
    spot_positions_account: Pubkey,
    perp_positions_account: Pubkey,
    orders_account: Pubkey,
    has_open_order: u8,
    orders_initialized: u8,
    is_bankrupt: u8,
    account_id: u8,
    padding: [u8; 12],
    total_deposits: u64,
    total_withdraws: u64,
    cumulative_spot_fees: u64,
    cumulative_perp_funding: i64,
    settled_perp_pnl: i64,
    lifetime_volume: u64,
    lifetime_perp_volume: u64,
    sub_account_id: u16,
    status: u8,
    liquidation_margin_freed: u64,
    last_active_slot: u64,
}

#[derive(Debug, Clone)]
struct BlackSwanEvent {
    event_type: EventType,
    protocol: Protocol,
    asset: Pubkey,
    severity: f64,
    opportunity_value: u64,
    timestamp: Instant,
}

#[derive(Debug, Clone, PartialEq)]
enum EventType {
    MassiveLiquidation,
    OracleDeviation,
    ProtocolExploit,
    FlashCrash,
    CascadingLiquidations,
}

#[derive(Debug, Clone, PartialEq)]
enum Protocol {
    Mango,
    Drift,
    Solend,
    Marginfi,
}

#[derive(Debug, Clone)]
struct PriceData {
    price: f64,
    confidence: f64,
    slot: u64,
    timestamp: Instant,
}

#[derive(Debug, Clone)]
struct LiquidationOpportunity {
    account: Pubkey,
    protocol: Protocol,
    collateral_mint: Pubkey,
    debt_mint: Pubkey,
    collateral_value: u64,
    debt_value: u64,
    max_liquidation_amount: u64,
    profit_estimate: u64,
}

pub struct BlackSwanProfiteer {
    rpc_client: Arc<RpcClient>,
    wallet: Arc<Keypair>,
    price_cache: Arc<RwLock<HashMap<Pubkey, PriceData>>>,
    liquidation_queue: Arc<Mutex<Vec<LiquidationOpportunity>>>,
    event_history: Arc<RwLock<Vec<BlackSwanEvent>>>,
    active_positions: Arc<RwLock<HashMap<Pubkey, Position>>>,
    market_cache: Arc<RwLock<HashMap<Pubkey, SerumMarketState>>>,
}

#[derive(Debug, Clone)]
struct Position {
    entry_price: f64,
    size: u64,
    asset: Pubkey,
    entry_time: Instant,
    stop_loss: f64,
}

impl BlackSwanProfiteer {
    pub fn new(rpc_url: &str, wallet: Keypair) -> Self {
        let rpc_client = Arc::new(RpcClient::new_with_commitment(
            rpc_url.to_string(),
            CommitmentConfig::confirmed(),
        ));

        Self {
            rpc_client,
            wallet: Arc::new(wallet),
            price_cache: Arc::new(RwLock::new(HashMap::new())),
            liquidation_queue: Arc::new(Mutex::new(Vec::new())),
            event_history: Arc::new(RwLock::new(Vec::new())),
            active_positions: Arc::new(RwLock::new(HashMap::new())),
            market_cache: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub async fn run(&self) -> Result<()> {
        let tasks = vec![
            tokio::spawn(self.clone().monitor_price_feeds()),
            tokio::spawn(self.clone().scan_liquidations()),
            tokio::spawn(self.clone().detect_black_swans()),
            tokio::spawn(self.clone().execute_opportunities()),
            tokio::spawn(self.clone().manage_positions()),
        ];

        futures::future::join_all(tasks).await;
        Ok(())
    }

    async fn monitor_price_feeds(self: Arc<Self>) {
        let mut interval = interval(Duration::from_millis(100));

        loop {
            interval.tick().await;
            let oracle_accounts = self.get_oracle_accounts().await;
            
            let mut account_requests = vec![];
            for (_, oracle) in &oracle_accounts {
                account_requests.push(oracle.clone());
            }

            match self.rpc_client.get_multiple_accounts(&account_requests).await {
                Ok(accounts) => {
                    for (i, account_opt) in accounts.iter().enumerate() {
                        if let Some(account) = account_opt {
                            if let Some(price_data) = self.parse_pyth_price(&account.data) {
                                let mut cache = self.price_cache.write().await;
                                cache.insert(oracle_accounts[i].0, price_data);
                            }
                        }
                    }
                }
                Err(e) => eprintln!("Failed to fetch oracle accounts: {:?}", e),
            }
        }
    }

    async fn scan_liquidations(self: Arc<Self>) {
        let mut interval = interval(Duration::from_millis(250));

        loop {
            interval.tick().await;

            let (mango_ops, drift_ops) = tokio::join!(
                self.scan_mango_liquidations(),
                self.scan_drift_liquidations(),
            );

            let mut all_ops = Vec::new();
            all_ops.extend(mango_ops);
            all_ops.extend(drift_ops);

            if !all_ops.is_empty() {
                let mut queue = self.liquidation_queue.lock().await;
                queue.extend(all_ops);
                queue.sort_by(|a, b| b.profit_estimate.cmp(&a.profit_estimate));
                queue.truncate(10);
            }
        }
    }

    async fn detect_black_swans(self: Arc<Self>) {
        let mut interval = interval(Duration::from_millis(50));
        let mut price_history: HashMap<Pubkey, Vec<f64>> = HashMap::new();

        loop {
            interval.tick().await;

            let prices = self.price_cache.read().await.clone();
            for (mint, price_data) in prices.iter() {
                let history = price_history.entry(*mint).or_insert_with(Vec::new);
                history.push(price_data.price);
                
                if history.len() > 100 {
                    history.remove(0);
                }

                if let Some(event) = self.analyze_price_movement(mint, history).await {
                    self.event_history.write().await.push(event.clone());
                    tokio::spawn(self.clone().handle_black_swan_event(event));
                }
            }
        }
    }

    async fn execute_opportunities(self: Arc<Self>) {
        loop {
            let opportunity = {
                let mut queue = self.liquidation_queue.lock().await;
                queue.pop()
            };

            if let Some(opp) = opportunity {
                if self.validate_opportunity(&opp).await {
                    match self.execute_liquidation(&opp).await {
                        Ok(sig) => println!("Liquidation executed: {}", sig),
                        Err(e) => eprintln!("Liquidation failed: {:?}", e),
                    }
                }
            }
            
            sleep(Duration::from_millis(10)).await;
        }
    }

    async fn manage_positions(self: Arc<Self>) {
        let mut interval = interval(Duration::from_secs(1));

        loop {
            interval.tick().await;

            let positions = self.active_positions.read().await.clone();
            let prices = self.price_cache.read().await;

            for (id, position) in positions.iter() {
                if let Some(price_data) = prices.get(&position.asset) {
                    if price_data.price <= position.stop_loss || 
                       self.should_take_profit(&position, price_data.price) {
                        tokio::spawn(self.clone().close_position(*id, position.clone()));
                    }
                }
            }
        }
    }

    async fn get_oracle_accounts(&self) -> Vec<(Pubkey, Pubkey)> {
        vec![
            (
                Pubkey::from_str("So11111111111111111111111111111111111111112").unwrap(),
                Pubkey::from_str("H6ARHf6YXhGYeQfUzQNGk6rDNnLBQKrenN712K4AQJEG").unwrap(),
            ),
            (
                Pubkey::from_str("EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v").unwrap(),
                Pubkey::from_str("Gnt27xtC473ZT2Mw5u8wZ68Z3gULkSTb5DuxJy7eJotD").unwrap(),
            ),
            (
                Pubkey::from_str("7vfCXTUXx5WJV5JADk17DUJ4ksgau7utNKj4b963voxs").unwrap(),
                Pubkey::from_str("GVXRSBjFk6e6J3NbVPXohDJetcTjaeeuykUpbQF8UoMU").unwrap(),
            ),
        ]
    }

    fn parse_pyth_price(&self, data: &[u8]) -> Option<PriceData> {
        if data.len() < 512 {
            return None;
        }

        let price_account = array_ref![data, 0, 512];
        let (_, _, price_type, exponent, _, _, _, aggregate) = 
            array_refs![price_account, 8, 8, 4, 4, 8, 8, 8, 464];

        if price_type != &[2, 0, 0, 0] {
            return None;
        }

        let price_comp = array_ref![aggregate, 0, 32];
        let (price_raw, conf_raw, _, slot_raw) = array_refs![price_comp, 8, 8, 8, 8];

        let price_val = i64::from_le_bytes(*price_raw);
        let conf_val = u64::from_le_bytes(*conf_raw);
                let price_val = i64::from_le_bytes(*price_raw);
        let conf_val = u64::from_le_bytes(*conf_raw);
        let exp = i32::from_le_bytes(*exponent);
        let slot = u64::from_le_bytes(*slot_raw);

        let price = (price_val as f64) * 10f64.powi(exp);
        let confidence = (conf_val as f64) * 10f64.powi(exp);

        Some(PriceData {
            price,
            confidence,
            slot,
            timestamp: Instant::now(),
        })
    }

    async fn scan_mango_liquidations(&self) -> Vec<LiquidationOpportunity> {
        let mut opportunities = Vec::new();
        let mango_program = Pubkey::from_str(MANGO_V4_ID).unwrap();

        let filters = vec![
            solana_client::rpc_filter::RpcFilterType::Memcmp(
                solana_client::rpc_filter::Memcmp::new_base58_encoded(0, &[245, 117, 108, 115]),
            ),
            solana_client::rpc_filter::RpcFilterType::DataSize(size_of::<MangoAccount>() as u64),
        ];

        match self.rpc_client.get_program_accounts_with_config(
            &mango_program,
            solana_client::rpc_config::RpcProgramAccountsConfig {
                filters: Some(filters),
                account_config: solana_client::rpc_config::RpcAccountInfoConfig {
                    encoding: Some(solana_client::rpc_config::UiAccountEncoding::Base64),
                    commitment: Some(CommitmentConfig::confirmed()),
                    ..Default::default()
                },
                ..Default::default()
            },
        ).await {
            Ok(accounts) => {
                for (pubkey, account) in accounts {
                    if let Some(opp) = self.parse_mango_account(&pubkey, &account.data).await {
                        if opp.profit_estimate > MIN_PROFIT_BPS * opp.max_liquidation_amount / 10000 {
                            opportunities.push(opp);
                        }
                    }
                }
            }
            Err(e) => eprintln!("Failed to scan Mango accounts: {:?}", e),
        }

        opportunities
    }

    async fn scan_drift_liquidations(&self) -> Vec<LiquidationOpportunity> {
        let mut opportunities = Vec::new();
        let drift_program = Pubkey::from_str(DRIFT_PROTOCOL).unwrap();

        let filters = vec![
            solana_client::rpc_filter::RpcFilterType::Memcmp(
                solana_client::rpc_filter::Memcmp::new_base58_encoded(0, &[156, 221, 88, 45]),
            ),
            solana_client::rpc_filter::RpcFilterType::DataSize(size_of::<DriftUser>() as u64),
        ];

        match self.rpc_client.get_program_accounts_with_config(
            &drift_program,
            solana_client::rpc_config::RpcProgramAccountsConfig {
                filters: Some(filters),
                account_config: solana_client::rpc_config::RpcAccountInfoConfig {
                    encoding: Some(solana_client::rpc_config::UiAccountEncoding::Base64),
                    commitment: Some(CommitmentConfig::confirmed()),
                    ..Default::default()
                },
                ..Default::default()
            },
        ).await {
            Ok(accounts) => {
                for (pubkey, account) in accounts {
                    if let Some(opp) = self.parse_drift_account(&pubkey, &account.data).await {
                        if opp.profit_estimate > MIN_PROFIT_BPS * opp.max_liquidation_amount / 10000 {
                            opportunities.push(opp);
                        }
                    }
                }
            }
            Err(e) => eprintln!("Failed to scan Drift accounts: {:?}", e),
        }

        opportunities
    }

    async fn parse_mango_account(&self, pubkey: &Pubkey, data: &[u8]) -> Option<LiquidationOpportunity> {
        if data.len() < size_of::<MangoAccount>() {
            return None;
        }

        let account: &MangoAccount = from_bytes(&data[0..size_of::<MangoAccount>()]);
        
        if account.being_liquidated != 0 {
            return None;
        }

        let health_data_offset = size_of::<MangoAccount>();
        if data.len() < health_data_offset + 32 {
            return None;
        }

        let health_bytes = &data[health_data_offset..health_data_offset + 16];
        let init_health = i128::from_le_bytes(health_bytes.try_into().ok()?);
        
        if init_health >= 0 {
            return None;
        }

        let collateral_value = account.net_deposits.saturating_add(100_000_000) as u64;
        let debt_value = init_health.abs() as u64;
        let max_liquidation = debt_value.saturating_mul(50) / 100;
        let profit = max_liquidation.saturating_mul(LIQUIDATION_DISCOUNT_BPS) / 10000;

        Some(LiquidationOpportunity {
            account: *pubkey,
            protocol: Protocol::Mango,
            collateral_mint: Pubkey::default(),
            debt_mint: Pubkey::default(),
            collateral_value,
            debt_value,
            max_liquidation_amount: max_liquidation,
            profit_estimate: profit,
        })
    }

    async fn parse_drift_account(&self, pubkey: &Pubkey, data: &[u8]) -> Option<LiquidationOpportunity> {
        if data.len() < size_of::<DriftUser>() {
            return None;
        }

        let user: &DriftUser = from_bytes(&data[0..size_of::<DriftUser>()]);
        
        if user.is_bankrupt != 0 || user.status != 0 {
            return None;
        }

        let positions_offset = size_of::<DriftUser>();
        if data.len() < positions_offset + 64 {
            return None;
        }

        let margin_ratio_bytes = &data[positions_offset..positions_offset + 8];
        let margin_ratio = u64::from_le_bytes(margin_ratio_bytes.try_into().ok()?);
        
        if margin_ratio > 1000 {
            return None;
        }

        let collateral_value = user.total_deposits;
        let debt_value = collateral_value.saturating_mul(11) / 10;
        let max_liquidation = debt_value.saturating_mul(40) / 100;
        let profit = max_liquidation.saturating_mul(LIQUIDATION_DISCOUNT_BPS) / 10000;

        Some(LiquidationOpportunity {
            account: *pubkey,
            protocol: Protocol::Drift,
            collateral_mint: Pubkey::default(),
            debt_mint: Pubkey::default(),
            collateral_value,
            debt_value,
            max_liquidation_amount: max_liquidation,
            profit_estimate: profit,
        })
    }

    async fn analyze_price_movement(&self, mint: &Pubkey, history: &[f64]) -> Option<BlackSwanEvent> {
        if history.len() < 10 {
            return None;
        }

        let current_price = *history.last()?;
        let avg_price = history.iter().sum::<f64>() / history.len() as f64;
        let std_dev = self.calculate_std_dev(history);
        
        if std_dev == 0.0 {
            return None;
        }

        let price_change_pct = ((current_price - avg_price) / avg_price).abs() * 100.0;
        let sigma_move = (current_price - avg_price).abs() / std_dev;

        if sigma_move > 4.0 || price_change_pct > 15.0 {
            let event_type = match (sigma_move, price_change_pct) {
                (_, pct) if pct > 30.0 => EventType::FlashCrash,
                (sigma, _) if sigma > 5.0 => EventType::OracleDeviation,
                _ => EventType::MassiveLiquidation,
            };

            let severity = (sigma_move / 4.0).min(2.0);
            let opportunity_value = (price_change_pct * 1_000_000.0) as u64;

            return Some(BlackSwanEvent {
                event_type,
                protocol: Protocol::Mango,
                asset: *mint,
                severity,
                opportunity_value,
                timestamp: Instant::now(),
            });
        }

        None
    }

    fn calculate_std_dev(&self, values: &[f64]) -> f64 {
        if values.is_empty() {
            return 0.0;
        }

        let mean = values.iter().sum::<f64>() / values.len() as f64;
        let variance = values.iter()
            .map(|x| (*x - mean).powi(2))
            .sum::<f64>() / values.len() as f64;
        variance.sqrt()
    }

    async fn handle_black_swan_event(self: Arc<Self>, event: BlackSwanEvent) {
        match event.event_type {
            EventType::FlashCrash => {
                if let Err(e) = self.execute_flash_crash_strategy(&event).await {
                    eprintln!("Flash crash strategy failed: {:?}", e);
                }
            }
            EventType::CascadingLiquidations => {
                if let Err(e) = self.execute_cascade_strategy(&event).await {
                    eprintln!("Cascade strategy failed: {:?}", e);
                }
            }
            EventType::OracleDeviation => {
                if let Err(e) = self.execute_oracle_arbitrage(&event).await {
                    eprintln!("Oracle arbitrage failed: {:?}", e);
                }
            }
            _ => {
                if let Err(e) = self.execute_general_strategy(&event).await {
                    eprintln!("General strategy failed: {:?}", e);
                }
            }
        }
    }

    async fn execute_flash_crash_strategy(&self, event: &BlackSwanEvent) -> Result<()> {
        let prices = self.price_cache.read().await;
        let price_data = prices.get(&event.asset)
            .ok_or_else(|| anyhow::anyhow!("Price not found"))?;

        let position_size = self.calculate_optimal_position_size(
            event.opportunity_value,
            price_data.price,
            event.severity
        );

        if position_size > 1_000_000 {
            let tx = self.build_spot_buy_transaction(
                &event.asset,
                position_size,
                price_data.price * 1.05
            ).await?;

            self.send_transaction_with_retry(tx, 3).await?;
        }

        Ok(())
    }

    async fn execute_cascade_strategy(&self, event: &BlackSwanEvent) -> Result<()> {
        let mut liquidations = self.liquidation_queue.lock().await;
        let ops: Vec<_> = liquidations.drain(..).take(5).collect();
        drop(liquidations);

        for liq in ops {
            let tx = self.build_liquidation_transaction(&liq).await?;
            let _ = self.send_transaction_with_retry(tx, 2).await;
            sleep(Duration::from_millis(50)).await;
        }

        Ok(())
    }

    async fn execute_oracle_arbitrage(&self, event: &BlackSwanEvent) -> Result<()> {
        let prices = self.price_cache.read().await;
        let price_data = prices.get(&event.asset)
            .ok_or_else(|| anyhow::anyhow!("Price not found"))?;

        if price_data.confidence > price_data.price * 0.05 {
            let size = 5_000_000u64;
            let profit = (price_data.confidence * 2.0 * size as f64) as u64;
            
            if profit > MIN_PROFIT_BPS * size / 10000 {
                let tx = self.build_arbitrage_transaction(
                    event.asset,
                    price_data.price - price_data.confidence,
                    price_data.price + price_data.confidence,
                    size
                ).await?;
                
                self.send_transaction_with_retry(tx, 5).await?;
            }
        }

        Ok(())
    }

    async fn execute_general_strategy(&self, event: &BlackSwanEvent) -> Result<()> {
        let position_size = (event.opportunity_value as f64 * 0.3) as u64;
        let tx = self.build_hedge_transaction(&event.asset, position_size).await?;
        self.send_transaction_with_retry(tx, 2).await?;
        Ok(())
    }

    fn calculate_optimal_position_size(&self, opportunity: u64, price: f64, severity: f64) -> u64 {
        let base_size = opportunity as f64 * 0.25;
        let risk_adjusted = base_size * (2.0 - severity).max(0.5);
        let max_position = 10_000_000.0;
        risk_adjusted.min(max_position) as u64
    }

    async fn validate_opportunity(&self, opp: &LiquidationOpportunity) -> bool {
        let account_info = match self.rpc_client.get_account(&opp.account).await {
            Ok(info) => info,
            Err(_) => return false,
        };

        match opp.protocol {
            Protocol::Mango => {
                if let Some(parsed) = self.parse_mango_account(&opp.account, &account_info.data).await {
                    parsed.profit_estimate >= opp.profit_estimate
                } else {
                    false
                }
            }
            Protocol::Drift => {
                if let Some(parsed) = self.parse_drift_account(&opp.account, &account_info.data).await {
                    parsed.profit_estimate >= opp.profit_estimate
                } else {
                    false
                }
            }
            _ => false,
        }
    }

        async fn execute_liquidation(&self, opp: &LiquidationOpportunity) -> Result<String> {
        let tx = self.build_liquidation_transaction(opp).await?;
        let signature = self.send_transaction_with_retry(tx, 3).await?;
        
        let position = Position {
            entry_price: 0.0,
            size: opp.max_liquidation_amount,
            asset: opp.collateral_mint,
            entry_time: Instant::now(),
            stop_loss: 0.0,
        };
        
        self.active_positions.write().await.insert(
            Pubkey::new_unique(),
            position
        );
        
        Ok(signature)
    }

    async fn build_liquidation_transaction(&self, opp: &LiquidationOpportunity) -> Result<Transaction> {
        let mut instructions = vec![
            ComputeBudgetInstruction::set_compute_unit_limit(COMPUTE_UNITS),
            ComputeBudgetInstruction::set_compute_unit_price(PRIORITY_FEE_LAMPORTS),
        ];

        match opp.protocol {
            Protocol::Mango => {
                instructions.push(self.build_mango_liquidation_ix(opp).await?);
            }
            Protocol::Drift => {
                instructions.push(self.build_drift_liquidation_ix(opp).await?);
            }
            _ => return Err(anyhow::anyhow!("Unsupported protocol")),
        }

        let recent_blockhash = self.rpc_client.get_latest_blockhash().await?;
        let tx = Transaction::new_signed_with_payer(
            &instructions,
            Some(&self.wallet.pubkey()),
            &[&*self.wallet],
            recent_blockhash,
        );

        Ok(tx)
    }

    async fn build_mango_liquidation_ix(&self, opp: &LiquidationOpportunity) -> Result<Instruction> {
        let mango_program = Pubkey::from_str(MANGO_V4_ID)?;
        let mango_group = Pubkey::from_str("78b8f4cGCwmZ9ysPFMWLaLTkkaYnUjwMJYStWe5RTSSX")?;
        
        let liqee_account_info = self.rpc_client.get_account(&opp.account).await?;
        let liqee_data: &MangoAccount = from_bytes(&liqee_account_info.data[0..size_of::<MangoAccount>()]);
        
        let accounts = vec![
            AccountMeta::new(mango_group, false),
            AccountMeta::new(opp.account, false),
            AccountMeta::new(self.wallet.pubkey(), true),
            AccountMeta::new(liqee_data.owner, false),
            AccountMeta::new_readonly(sysvar::instructions::id(), false),
        ];
        
        let mut data = vec![243, 131, 201, 20]; // Mango v4 liquidate_perp_market discriminator
        data.extend_from_slice(&opp.max_liquidation_amount.to_le_bytes());
        
        Ok(Instruction {
            program_id: mango_program,
            accounts,
            data,
        })
    }

    async fn build_drift_liquidation_ix(&self, opp: &LiquidationOpportunity) -> Result<Instruction> {
        let drift_program = Pubkey::from_str(DRIFT_PROTOCOL)?;
        let state = Pubkey::from_str("FExhvPycCCKzCQaMS1ywMGYH1mFQKQKyMxCNj5rDmqN6")?;
        
        let accounts = vec![
            AccountMeta::new_readonly(state, false),
            AccountMeta::new(self.wallet.pubkey(), true),
            AccountMeta::new(opp.account, false),
            AccountMeta::new_readonly(sysvar::rent::id(), false),
        ];
        
        let mut data = vec![218, 48, 227, 154, 42, 35, 149, 245]; // Drift liquidate_perp discriminator
        data.extend_from_slice(&0u16.to_le_bytes()); // market_index
        data.extend_from_slice(&opp.max_liquidation_amount.to_le_bytes());
        
        Ok(Instruction {
            program_id: drift_program,
            accounts,
            data,
        })
    }

    async fn build_spot_buy_transaction(&self, asset: &Pubkey, size: u64, max_price: f64) -> Result<Transaction> {
        let market_info = self.get_serum_market_info(asset).await?;
        let market_state = self.load_serum_market(&market_info.address).await?;
        
        let mut instructions = vec![
            ComputeBudgetInstruction::set_compute_unit_limit(COMPUTE_UNITS),
            ComputeBudgetInstruction::set_compute_unit_price(PRIORITY_FEE_LAMPORTS * 2),
        ];

        let limit_price = (max_price * 10f64.powi(market_info.decimals as i32)) as u64;
        let max_coin_qty = size / market_state.base_lot_size;
        let max_pc_qty = (size as f64 * max_price / market_state.quote_lot_size as f64) as u64;

        instructions.push(self.build_serum_new_order_v3_ix(
            &market_info,
            &market_state,
            NewOrderV3Params {
                side: Side::Buy,
                limit_price,
                max_coin_qty,
                max_pc_qty,
                order_type: OrderType::ImmediateOrCancel,
                client_order_id: 0,
                self_trade_behavior: SelfTradeBehavior::DecrementTake,
                limit: 10,
            }
        ).await?);

        let recent_blockhash = self.rpc_client.get_latest_blockhash().await?;
        Ok(Transaction::new_signed_with_payer(
            &instructions,
            Some(&self.wallet.pubkey()),
            &[&*self.wallet],
            recent_blockhash,
        ))
    }

    async fn get_serum_market_info(&self, asset: &Pubkey) -> Result<MarketInfo> {
        let markets = [
            MarketInfo {
                address: Pubkey::from_str("8BnEgHoWFysVcuFFX7QztDmzuH8r5ZFvyP3sYwn1XTh6")?,
                base_mint: Pubkey::from_str("So11111111111111111111111111111111111111112")?,
                quote_mint: Pubkey::from_str("EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v")?,
                decimals: 9,
            },
            MarketInfo {
                address: Pubkey::from_str("9wFFyRfZBsuAha4YcuxcXLKwMxJR43S7fPfQLusDBzvT")?,
                base_mint: Pubkey::from_str("So11111111111111111111111111111111111111112")?,
                quote_mint: Pubkey::from_str("EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v")?,
                decimals: 9,
            },
        ];

        markets.into_iter()
            .find(|m| m.base_mint == *asset || m.quote_mint == *asset)
            .ok_or_else(|| anyhow::anyhow!("Market not found"))
    }

    async fn load_serum_market(&self, market_addr: &Pubkey) -> Result<SerumMarketState> {
        let cached = self.market_cache.read().await.get(market_addr).cloned();
        if let Some(state) = cached {
            return Ok(state);
        }

        let account = self.rpc_client.get_account(market_addr).await?;
        let state: &SerumMarketState = from_bytes(&account.data[5..5 + size_of::<SerumMarketState>()]);
        
        self.market_cache.write().await.insert(*market_addr, *state);
        Ok(*state)
    }

    async fn build_serum_new_order_v3_ix(
        &self,
        market_info: &MarketInfo,
        market_state: &SerumMarketState,
        params: NewOrderV3Params,
    ) -> Result<Instruction> {
        let program_id = Pubkey::from_str(SERUM_DEX_V3)?;
        
        let accounts = vec![
            AccountMeta::new(market_info.address, false),
            AccountMeta::new(self.wallet.pubkey(), false),
            AccountMeta::new(self.wallet.pubkey(), false),
            AccountMeta::new(self.wallet.pubkey(), true),
            AccountMeta::new(Pubkey::new(&market_state.base_vault), false),
            AccountMeta::new(Pubkey::new(&market_state.quote_vault), false),
            AccountMeta::new_readonly(spl_token::id(), false),
            AccountMeta::new_readonly(sysvar::rent::id(), false),
        ];
        
        let mut data = vec![10, 0, 0, 0]; // new_order_v3 instruction discriminator
        data.extend_from_slice(&(params.side as u32).to_le_bytes());
        data.extend_from_slice(&params.limit_price.to_le_bytes());
        data.extend_from_slice(&params.max_coin_qty.to_le_bytes());
        data.extend_from_slice(&params.max_pc_qty.to_le_bytes());
        data.extend_from_slice(&(params.self_trade_behavior as u32).to_le_bytes());
        data.extend_from_slice(&(params.order_type as u32).to_le_bytes());
        data.extend_from_slice(&params.client_order_id.to_le_bytes());
        data.extend_from_slice(&params.limit.to_le_bytes());
        
        Ok(Instruction {
            program_id,
            accounts,
            data,
        })
    }

    async fn build_arbitrage_transaction(
        &self,
        asset: Pubkey,
        buy_price: f64,
        sell_price: f64,
        size: u64,
    ) -> Result<Transaction> {
        let mut instructions = vec![
            ComputeBudgetInstruction::set_compute_unit_limit(COMPUTE_UNITS),
            ComputeBudgetInstruction::set_compute_unit_price(PRIORITY_FEE_LAMPORTS * 3),
        ];

        let market_info = self.get_serum_market_info(&asset).await?;
        let market_state = self.load_serum_market(&market_info.address).await?;

        instructions.push(self.build_serum_new_order_v3_ix(
            &market_info,
            &market_state,
            NewOrderV3Params {
                side: Side::Buy,
                limit_price: (buy_price * 10f64.powi(market_info.decimals as i32)) as u64,
                max_coin_qty: size / market_state.base_lot_size,
                max_pc_qty: (size as f64 * buy_price * 1.01 / market_state.quote_lot_size as f64) as u64,
                order_type: OrderType::ImmediateOrCancel,
                client_order_id: 1,
                self_trade_behavior: SelfTradeBehavior::DecrementTake,
                limit: 10,
            }
        ).await?);

        instructions.push(self.build_serum_new_order_v3_ix(
            &market_info,
            &market_state,
            NewOrderV3Params {
                side: Side::Sell,
                limit_price: (sell_price * 10f64.powi(market_info.decimals as i32)) as u64,
                max_coin_qty: size / market_state.base_lot_size,
                max_pc_qty: 0,
                order_type: OrderType::ImmediateOrCancel,
                client_order_id: 2,
                self_trade_behavior: SelfTradeBehavior::DecrementTake,
                limit: 10,
            }
        ).await?);

        let recent_blockhash = self.rpc_client.get_latest_blockhash().await?;
        Ok(Transaction::new_signed_with_payer(
            &instructions,
            Some(&self.wallet.pubkey()),
            &[&*self.wallet],
            recent_blockhash,
        ))
    }

    async fn build_hedge_transaction(&self, asset: &Pubkey, size: u64) -> Result<Transaction> {
        let mut instructions = vec![
            ComputeBudgetInstruction::set_compute_unit_limit(COMPUTE_UNITS),
            ComputeBudgetInstruction::set_compute_unit_price(PRIORITY_FEE_LAMPORTS),
        ];

        let prices = self.price_cache.read().await;
        let current_price = prices.get(asset)
            .map(|p| p.price)
            .ok_or_else(|| anyhow::anyhow!("Price not found"))?;

        let hedge_size = size / 2;
        let market_info = self.get_serum_market_info(asset).await?;
        let market_state = self.load_serum_market(&market_info.address).await?;
        
        instructions.push(self.build_serum_new_order_v3_ix(
            &market_info,
            &market_state,
            NewOrderV3Params {
                side: Side::Sell,
                limit_price: (current_price * 0.98 * 10f64.powi(market_info.decimals as i32)) as u64,
                max_coin_qty: hedge_size / market_state.base_lot_size,
                max_pc_qty: 0,
                                order_type: OrderType::PostOnly,
                client_order_id: 3,
                self_trade_behavior: SelfTradeBehavior::DecrementTake,
                limit: 10,
            }
        ).await?);

        let recent_blockhash = self.rpc_client.get_latest_blockhash().await?;
        Ok(Transaction::new_signed_with_payer(
            &instructions,
            Some(&self.wallet.pubkey()),
            &[&*self.wallet],
            recent_blockhash,
        ))
    }

    async fn send_transaction_with_retry(&self, tx: Transaction, max_retries: u32) -> Result<String> {
        let mut retries = 0;
        let mut last_error = None;

        while retries < max_retries {
            match self.rpc_client.send_transaction_with_config(
                &tx,
                solana_client::rpc_config::RpcSendTransactionConfig {
                    skip_preflight: true,
                    preflight_commitment: Some(CommitmentConfig::confirmed()),
                    encoding: None,
                    max_retries: Some(0),
                    min_context_slot: None,
                }
            ).await {
                Ok(signature) => {
                    let start = Instant::now();
                    loop {
                        match self.rpc_client.get_signature_status(&signature).await {
                            Ok(Some(status)) => {
                                if status.is_ok() {
                                    return Ok(signature.to_string());
                                } else if status.is_err() {
                                    return Err(anyhow::anyhow!("Transaction failed: {:?}", status));
                                }
                            }
                            _ => {}
                        }
                        
                        if start.elapsed() > Duration::from_secs(30) {
                            break;
                        }
                        sleep(Duration::from_millis(500)).await;
                    }
                }
                Err(e) => {
                    last_error = Some(e);
                    retries += 1;
                    
                    if retries < max_retries {
                        sleep(Duration::from_millis(100 * retries as u64)).await;
                    }
                }
            }
        }

        Err(anyhow::anyhow!("Transaction failed after {} retries: {:?}", max_retries, last_error))
    }

    async fn close_position(&self, id: Pubkey, position: Position) {
        let prices = self.price_cache.read().await;
        if let Some(price_data) = prices.get(&position.asset) {
            match self.get_serum_market_info(&position.asset).await {
                Ok(market_info) => {
                    match self.load_serum_market(&market_info.address).await {
                        Ok(market_state) => {
                            let close_ix = match self.build_serum_new_order_v3_ix(
                                &market_info,
                                &market_state,
                                NewOrderV3Params {
                                    side: Side::Sell,
                                    limit_price: (price_data.price * 0.99 * 10f64.powi(market_info.decimals as i32)) as u64,
                                    max_coin_qty: position.size / market_state.base_lot_size,
                                    max_pc_qty: 0,
                                    order_type: OrderType::ImmediateOrCancel,
                                    client_order_id: 99,
                                    self_trade_behavior: SelfTradeBehavior::DecrementTake,
                                    limit: 10,
                                }
                            ).await {
                                Ok(ix) => ix,
                                Err(_) => return,
                            };

                            let instructions = vec![
                                ComputeBudgetInstruction::set_compute_unit_limit(COMPUTE_UNITS),
                                ComputeBudgetInstruction::set_compute_unit_price(PRIORITY_FEE_LAMPORTS * 2),
                                close_ix,
                            ];

                            if let Ok(recent_blockhash) = self.rpc_client.get_latest_blockhash().await {
                                let tx = Transaction::new_signed_with_payer(
                                    &instructions,
                                    Some(&self.wallet.pubkey()),
                                    &[&*self.wallet],
                                    recent_blockhash,
                                );

                                if let Ok(_) = self.send_transaction_with_retry(tx, 3).await {
                                    self.active_positions.write().await.remove(&id);
                                }
                            }
                        }
                        Err(_) => {}
                    }
                }
                Err(_) => {}
            }
        }
    }

    fn should_take_profit(&self, position: &Position, current_price: f64) -> bool {
        if position.entry_price == 0.0 {
            return false;
        }
        
        let profit_pct = ((current_price - position.entry_price) / position.entry_price) * 100.0;
        let time_held = Instant::now().duration_since(position.entry_time);
        
        profit_pct > 5.0 || 
        (profit_pct > 2.0 && time_held > Duration::from_secs(300)) ||
        (profit_pct > 1.0 && time_held > Duration::from_secs(600))
    }
}

#[derive(Debug, Clone)]
struct MarketInfo {
    address: Pubkey,
    base_mint: Pubkey,
    quote_mint: Pubkey,
    decimals: u8,
}

#[derive(Debug, Clone)]
struct NewOrderV3Params {
    side: Side,
    limit_price: u64,
    max_coin_qty: u64,
    max_pc_qty: u64,
    order_type: OrderType,
    client_order_id: u64,
    self_trade_behavior: SelfTradeBehavior,
    limit: u16,
}

#[derive(Debug, Clone, Copy, PartialEq)]
#[repr(u8)]
enum Side {
    Buy = 0,
    Sell = 1,
}

#[derive(Debug, Clone, Copy, PartialEq)]
#[repr(u8)]
enum OrderType {
    Limit = 0,
    ImmediateOrCancel = 1,
    PostOnly = 2,
}

#[derive(Debug, Clone, Copy, PartialEq)]
#[repr(u8)]
enum SelfTradeBehavior {
    DecrementTake = 0,
    CancelProvide = 1,
    AbortTransaction = 2,
}

impl Clone for BlackSwanProfiteer {
    fn clone(&self) -> Self {
        Self {
            rpc_client: Arc::clone(&self.rpc_client),
            wallet: Arc::clone(&self.wallet),
            price_cache: Arc::clone(&self.price_cache),
            liquidation_queue: Arc::clone(&self.liquidation_queue),
            event_history: Arc::clone(&self.event_history),
            active_positions: Arc::clone(&self.active_positions),
            market_cache: Arc::clone(&self.market_cache),
        }
    }
}

impl std::fmt::Debug for BlackSwanProfiteer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BlackSwanProfiteer")
            .field("wallet", &self.wallet.pubkey().to_string())
            .finish()
    }
}

pub use anyhow::Result;

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_black_swan_detection() {
        let keypair = Keypair::new();
        let profiteer = BlackSwanProfiteer::new("https://api.mainnet-beta.solana.com", keypair);
        
        let history = vec![100.0, 101.0, 99.0, 100.5, 98.0, 102.0, 50.0];
        let mint = Pubkey::new_unique();
        
        let event = profiteer.analyze_price_movement(&mint, &history).await;
        assert!(event.is_some());
        if let Some(e) = event {
            assert!(matches!(e.event_type, EventType::FlashCrash | EventType::MassiveLiquidation));
        }
    }

    #[tokio::test]
    async fn test_std_dev_calculation() {
        let profiteer = BlackSwanProfiteer::new("https://api.mainnet-beta.solana.com", Keypair::new());
        let values = vec![2.0, 4.0, 4.0, 4.0, 5.0, 5.0, 7.0, 9.0];
        let std_dev = profiteer.calculate_std_dev(&values);
        assert!((std_dev - 2.0).abs() < 0.1);
    }

    #[tokio::test] 
    async fn test_liquidation_parsing() {
        let profiteer = BlackSwanProfiteer::new("https://api.mainnet-beta.solana.com", Keypair::new());
        
        let mut mango_data = vec![0u8; size_of::<MangoAccount>() + 32];
        let mut drift_data = vec![0u8; size_of::<DriftUser>() + 64];
        
        let pubkey = Pubkey::new_unique();
        
        let mango_opp = profiteer.parse_mango_account(&pubkey, &mango_data).await;
        assert!(mango_opp.is_none());
        
        let drift_opp = profiteer.parse_drift_account(&pubkey, &drift_data).await;
        assert!(drift_opp.is_none());
    }

    #[test]
    fn test_position_profit_calculation() {
        let profiteer = BlackSwanProfiteer::new("https://api.mainnet-beta.solana.com", Keypair::new());
        
        let position = Position {
            entry_price: 100.0,
            size: 1000,
            asset: Pubkey::new_unique(),
            entry_time: Instant::now() - Duration::from_secs(400),
            stop_loss: 95.0,
        };
        
        assert!(profiteer.should_take_profit(&position, 106.0));
        assert!(profiteer.should_take_profit(&position, 102.5));
        assert!(!profiteer.should_take_profit(&position, 100.5));
    }
}

