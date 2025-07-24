use anchor_client::{
    solana_sdk::{
        commitment_config::CommitmentConfig,
        pubkey::Pubkey,
        signature::{Keypair, Signer},
        transaction::Transaction,
        instruction::{Instruction, AccountMeta},
        compute_budget::ComputeBudgetInstruction,
        system_program,
    },
    Client, Cluster,
};
use solana_client::{
    rpc_client::RpcClient,
    rpc_config::{RpcAccountInfoConfig, RpcProgramAccountsConfig},
    rpc_filter::{Memcmp, MemcmpEncodedBytes, RpcFilterType},
};
use spl_token::state::Account as TokenAccount;
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::{Duration, SystemTime, UNIX_EPOCH},
    str::FromStr,
};
use tokio::time::{interval, sleep};
use borsh::{BorshDeserialize, BorshSerialize};
use serde::{Deserialize, Serialize};
use base64;

const JUPITER_V6_PROGRAM_ID: Pubkey = solana_sdk::pubkey!("JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4");
const TOKEN_PROGRAM_ID: Pubkey = solana_sdk::pubkey!("TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA");
const RAYDIUM_AMM_V4: Pubkey = solana_sdk::pubkey!("675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8");
const ORCA_WHIRLPOOL: Pubkey = solana_sdk::pubkey!("whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc");
const STREAMFLOW_PROGRAM: Pubkey = solana_sdk::pubkey!("strmRqUCoQUgGUan5YhzUZa6KqdzwX5L6FpUxfmKg5m");
const BONFIDA_VESTING: Pubkey = solana_sdk::pubkey!("CChTq6PthWU6YZx7MUjSQHizkxfB68MdYb2a4nrrZmVd");

const MAX_SLIPPAGE_BPS: u16 = 300;
const MIN_PROFIT_BPS: u16 = 150;
const MAX_POSITION_SIZE_SOL: u64 = 100_000_000_000;
const UNLOCK_TRADE_WINDOW_SECONDS: i64 = 3600;
const PRIORITY_FEE_MICROLAMPORTS: u64 = 50_000;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct TokenUnlockEvent {
    token_mint: Pubkey,
    unlock_timestamp: i64,
    unlock_amount: u64,
    vesting_account: Pubkey,
    total_supply: u64,
    unlock_percentage: f64,
}

#[derive(Debug, Clone)]
struct TradeOpportunity {
    token_mint: Pubkey,
    action: TradeAction,
    size_sol: u64,
    expected_profit_bps: u16,
    unlock_event: TokenUnlockEvent,
    route: RouteInfo,
}

#[derive(Debug, Clone, PartialEq)]
enum TradeAction {
    Short,
    Close,
}

#[derive(Debug, Clone)]
struct RouteInfo {
    dex: DexType,
    pool: Pubkey,
    input_vault: Pubkey,
    output_vault: Pubkey,
}

#[derive(Debug, Clone, PartialEq)]
enum DexType {
    Raydium,
    Orca,
    Jupiter,
}

#[derive(BorshSerialize, BorshDeserialize)]
struct StreamflowTimelock {
    start_time: i64,
    end_time: i64,
    cliff: i64,
    cliff_amount: u64,
    amount_per_period: u64,
    period: i64,
    recipient: Pubkey,
    mint: Pubkey,
    escrow_tokens: Pubkey,
    withdrawn: u64,
}

#[repr(C)]
#[derive(Debug, Clone, Copy)]
struct RaydiumAmmInfo {
    status: u64,
    nonce: u64,
    order_num: u64,
    depth: u64,
    coin_decimals: u64,
    pc_decimals: u64,
    state: u64,
    reset_flag: u64,
    min_size: u64,
    vol_max_cut_ratio: u64,
    amount_wave_ratio: u64,
    coin_lot_size: u64,
    pc_lot_size: u64,
    min_price_multiplier: u64,
    max_price_multiplier: u64,
    system_decimals_value: u64,
    min_separate_denominator: u64,
    min_separate_numerator: u64,
    trade_fee_numerator: u64,
    trade_fee_denominator: u64,
    pnl_numerator: u64,
    pnl_denominator: u64,
    swap_fee_numerator: u64,
    swap_fee_denominator: u64,
    need_take_pnl_coin: u64,
    need_take_pnl_pc: u64,
    total_pnl_pc: u64,
    total_pnl_coin: u64,
    pool_open_time: u64,
    punish_pc_amount: u64,
    punish_coin_amount: u64,
    ordebook_to_init_time: u64,
    swap_coin_in_amount: u128,
    swap_pc_out_amount: u128,
    swap_acc_pc_fee: u64,
    swap_pc_in_amount: u128,
    swap_coin_out_amount: u128,
    swap_acc_coin_fee: u64,
    token_coin: Pubkey,
    token_pc: Pubkey,
    coin_mint: Pubkey,
    pc_mint: Pubkey,
    lp_mint: Pubkey,
    open_orders: Pubkey,
    market: Pubkey,
    serum_dex: Pubkey,
    target_orders: Pubkey,
    withdraw_queue: Pubkey,
    token_temp_lp: Pubkey,
    amm_owner: Pubkey,
    pnl_owner: Pubkey,
    vault_signer: Pubkey,
    lp_reserve: u64,
}

pub struct TokenUnlockScheduleTrader {
    rpc_client: Arc<RpcClient>,
    wallet: Arc<Keypair>,
    tracked_vesting_accounts: Arc<Mutex<HashMap<Pubkey, StreamflowTimelock>>>,
    active_positions: Arc<Mutex<HashMap<Pubkey, Position>>>,
    price_cache: Arc<Mutex<HashMap<Pubkey, PriceData>>>,
}

#[derive(Debug, Clone)]
struct Position {
    token_mint: Pubkey,
    entry_price: f64,
    size_tokens: u64,
    size_sol: u64,
    entry_timestamp: i64,
    unlock_event: TokenUnlockEvent,
}

#[derive(Debug, Clone)]
struct PriceData {
    price: f64,
    liquidity: u64,
    last_update: i64,
}

#[derive(Debug)]
struct PriceImpact {
    expected_drop_bps: u16,
    confidence: f64,
}

impl TokenUnlockScheduleTrader {
    pub fn new(rpc_url: &str, wallet: Keypair) -> Self {
        let rpc_client = Arc::new(RpcClient::new_with_commitment(
            rpc_url.to_string(),
            CommitmentConfig::confirmed(),
        ));

        Self {
            rpc_client,
            wallet: Arc::new(wallet),
            tracked_vesting_accounts: Arc::new(Mutex::new(HashMap::new())),
            active_positions: Arc::new(Mutex::new(HashMap::new())),
            price_cache: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub async fn run(&self) -> Result<(), Box<dyn std::error::Error>> {
        let mut tasks = vec![];

        let scanner = self.clone();
        tasks.push(tokio::spawn(async move {
            scanner.scan_vesting_accounts_loop().await
        }));

        let monitor = self.clone();
        tasks.push(tokio::spawn(async move {
            monitor.monitor_unlock_events_loop().await
        }));

        let trader = self.clone();
        tasks.push(tokio::spawn(async move {
            trader.execute_trades_loop().await
        }));

        let price_updater = self.clone();
        tasks.push(tokio::spawn(async move {
            price_updater.update_prices_loop().await
        }));

        futures::future::join_all(tasks).await;
        Ok(())
    }

    async fn scan_vesting_accounts_loop(&self) -> Result<(), Box<dyn std::error::Error>> {
        let mut interval = interval(Duration::from_secs(300));
        
        loop {
            interval.tick().await;
            if let Err(e) = self.scan_vesting_programs().await {
                eprintln!("Error scanning vesting accounts: {}", e);
            }
        }
    }

    async fn scan_vesting_programs(&self) -> Result<(), Box<dyn std::error::Error>> {
        let config = RpcProgramAccountsConfig {
            filters: Some(vec![RpcFilterType::DataSize(216)]),
            account_config: RpcAccountInfoConfig {
                encoding: Some(solana_account_decoder::UiAccountEncoding::Base64),
                ..Default::default()
            },
            ..Default::default()
        };
        
        let accounts = self.rpc_client.get_program_accounts_with_config(&STREAMFLOW_PROGRAM, config)?;
        
        for (pubkey, account) in accounts {
            if let Ok(timelock) = StreamflowTimelock::try_from_slice(&account.data) {
                let mut tracked = self.tracked_vesting_accounts.lock().unwrap();
                tracked.insert(pubkey, timelock);
            }
        }
        
        Ok(())
    }

    async fn monitor_unlock_events_loop(&self) -> Result<(), Box<dyn std::error::Error>> {
        let mut interval = interval(Duration::from_secs(60));
        
        loop {
            interval.tick().await;
            if let Err(e) = self.check_upcoming_unlocks().await {
                eprintln!("Error monitoring unlock events: {}", e);
            }
        }
    }

    async fn check_upcoming_unlocks(&self) -> Result<(), Box<dyn std::error::Error>> {
        let current_ts = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs() as i64;
        let tracked = self.tracked_vesting_accounts.lock().unwrap().clone();
        
        for (vesting_account, timelock) in tracked.iter() {
            let next_unlock_ts = self.calculate_next_unlock_timestamp(timelock, current_ts);
            
            if let Some(unlock_ts) = next_unlock_ts {
                let time_until_unlock = unlock_ts - current_ts;
                
                if time_until_unlock > 0 && time_until_unlock <= UNLOCK_TRADE_WINDOW_SECONDS {
                    let unlock_amount = self.calculate_unlock_amount(timelock, unlock_ts);
                    let total_supply = self.get_token_supply(&timelock.mint).await?;
                    
                    let unlock_event = TokenUnlockEvent {
                        token_mint: timelock.mint,
                        unlock_timestamp: unlock_ts,
                        unlock_amount,
                        vesting_account: *vesting_account,
                        total_supply,
                        unlock_percentage: (unlock_amount as f64 / total_supply as f64) * 100.0,
                    };
                    
                    if unlock_event.unlock_percentage >= 0.5 {
                        self.analyze_trade_opportunity(unlock_event).await?;
                    }
                }
            }
        }
        
        Ok(())
    }

    fn calculate_next_unlock_timestamp(&self, timelock: &StreamflowTimelock, current_ts: i64) -> Option<i64> {
        if current_ts < timelock.cliff {
            return Some(timelock.cliff);
        }
        
        if current_ts >= timelock.end_time {
            return None;
        }
        
        let time_since_cliff = current_ts - timelock.cliff;
        let periods_passed = time_since_cliff / timelock.period;
        let next_unlock_ts = timelock.cliff + (periods_passed + 1) * timelock.period;
        
        if next_unlock_ts <= timelock.end_time {
            Some(next_unlock_ts)
        } else {
            Some(timelock.end_time)
        }
    }

    fn calculate_unlock_amount(&self, timelock: &StreamflowTimelock, unlock_ts: i64) -> u64 {
        if unlock_ts < timelock.cliff {
            return 0;
        }
        
        if unlock_ts == timelock.cliff {
            return timelock.cliff_amount;
        }
        
        let periods = ((unlock_ts - timelock.cliff) / timelock.period).min(
            (timelock.end_time - timelock.cliff) / timelock.period
        );
        
        let periodic_amount = periods as u64 * timelock.amount_per_period;
        (timelock.cliff_amount + periodic_amount).saturating_sub(timelock.withdrawn)
    }

    async fn get_token_supply(&self, mint: &Pubkey) -> Result<u64, Box<dyn std::error::Error>> {
        let mint_account = self.rpc_client.get_account(mint)?;
        let mint_data = spl_token::state::Mint::unpack(&mint_account.data)?;
        Ok(mint_data.supply)
    }

        async fn analyze_trade_opportunity(&self, unlock_event: TokenUnlockEvent) -> Result<(), Box<dyn std::error::Error>> {
        let price_data = self.get_price_data(&unlock_event.token_mint).await?;
        
        if price_data.liquidity < 1_000_000_000 {
            return Ok(());
        }
        
        let impact = self.estimate_price_impact(&unlock_event, &price_data);
        
        if impact.expected_drop_bps >= MIN_PROFIT_BPS {
            let route = self.find_best_route(&unlock_event.token_mint).await?;
            let size_sol = self.calculate_position_size(&unlock_event, &price_data);
            
            let opportunity = TradeOpportunity {
                token_mint: unlock_event.token_mint,
                action: TradeAction::Short,
                size_sol,
                expected_profit_bps: impact.expected_drop_bps,
                unlock_event,
                route,
            };
            
            self.queue_trade_opportunity(opportunity).await?;
        }
        
        Ok(())
    }

    async fn get_price_data(&self, mint: &Pubkey) -> Result<PriceData, Box<dyn std::error::Error>> {
        let cache = self.price_cache.lock().unwrap();
        
        if let Some(data) = cache.get(mint) {
            let current_ts = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs() as i64;
            if current_ts - data.last_update < 60 {
                return Ok(data.clone());
            }
        }
        
        drop(cache);
        self.fetch_price_from_dex(mint).await
    }

    async fn fetch_price_from_dex(&self, mint: &Pubkey) -> Result<PriceData, Box<dyn std::error::Error>> {
        let wsol_mint = spl_token::native_mint::id();
        let mut best_price = 0.0;
        let mut best_liquidity = 0u64;
        
        // Check Raydium pools
        if let Ok(raydium_data) = self.get_raydium_price(mint, &wsol_mint).await {
            if raydium_data.1 > best_liquidity {
                best_price = raydium_data.0;
                best_liquidity = raydium_data.1;
            }
        }
        
        // Check Orca pools
        if let Ok(orca_data) = self.get_orca_price(mint, &wsol_mint).await {
            if orca_data.1 > best_liquidity {
                best_price = orca_data.0;
                best_liquidity = orca_data.1;
            }
        }
        
        let price_data = PriceData {
            price: best_price,
            liquidity: best_liquidity,
            last_update: SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs() as i64,
        };
        
        let mut cache = self.price_cache.lock().unwrap();
        cache.insert(*mint, price_data.clone());
        
        Ok(price_data)
    }

    async fn get_raydium_price(&self, token_mint: &Pubkey, quote_mint: &Pubkey) -> Result<(f64, u64), Box<dyn std::error::Error>> {
        let pool_seeds = &[
            &RAYDIUM_AMM_V4.to_bytes()[..],
            &token_mint.to_bytes()[..],
            &quote_mint.to_bytes()[..],
            b"pool_seed",
        ];
        
        let (pool_pubkey, _) = Pubkey::find_program_address(pool_seeds, &RAYDIUM_AMM_V4);
        
        match self.rpc_client.get_account(&pool_pubkey) {
            Ok(account) => {
                if account.data.len() >= std::mem::size_of::<RaydiumAmmInfo>() {
                    unsafe {
                        let amm_info = std::ptr::read(account.data.as_ptr() as *const RaydiumAmmInfo);
                        
                        let token_amount = self.get_token_balance(&amm_info.token_coin).await?;
                        let pc_amount = self.get_token_balance(&amm_info.token_pc).await?;
                        
                        let price = if token_amount > 0 {
                            (pc_amount as f64 / 1e9) / (token_amount as f64 / 10f64.powi(amm_info.coin_decimals as i32))
                        } else {
                            0.0
                        };
                        
                        let liquidity = pc_amount * 2;
                        Ok((price, liquidity))
                    }
                } else {
                    Ok((0.0, 0))
                }
            }
            Err(_) => Ok((0.0, 0))
        }
    }

    async fn get_orca_price(&self, token_mint: &Pubkey, quote_mint: &Pubkey) -> Result<(f64, u64), Box<dyn std::error::Error>> {
        let whirlpool_config = Pubkey::from_str("2LecshUwdy9xi7meFgHtFJQNSKk4KdTrcpvaB56dP2NQ")?;
        let pool_seeds = &[
            b"whirlpool",
            &whirlpool_config.to_bytes()[..],
            &token_mint.to_bytes()[..],
            &quote_mint.to_bytes()[..],
            &64u16.to_le_bytes()[..],
        ];
        
        let (pool_pubkey, _) = Pubkey::find_program_address(pool_seeds, &ORCA_WHIRLPOOL);
        
        match self.rpc_client.get_account(&pool_pubkey) {
            Ok(account) => {
                if account.data.len() >= 653 {
                    let sqrt_price = u128::from_le_bytes(account.data[65..81].try_into()?);
                    let liquidity = u128::from_le_bytes(account.data[81..97].try_into()?);
                    
                    let price = (sqrt_price as f64 / (1u64 << 64) as f64).powi(2);
                    Ok((price, liquidity as u64))
                } else {
                    Ok((0.0, 0))
                }
            }
            Err(_) => Ok((0.0, 0))
        }
    }

    async fn get_token_balance(&self, token_account: &Pubkey) -> Result<u64, Box<dyn std::error::Error>> {
        match self.rpc_client.get_account(token_account) {
            Ok(account) => {
                let token_account = TokenAccount::unpack(&account.data)?;
                Ok(token_account.amount)
            }
            Err(_) => Ok(0)
        }
    }

    fn estimate_price_impact(&self, unlock_event: &TokenUnlockEvent, price_data: &PriceData) -> PriceImpact {
        let unlock_value = unlock_event.unlock_amount as f64 * price_data.price;
        let liquidity_ratio = unlock_value / price_data.liquidity as f64;
        
        let base_impact = liquidity_ratio * 0.5;
        let market_depth_factor = (price_data.liquidity as f64 / 10_000_000_000.0).min(1.0);
        let time_factor = (UNLOCK_TRADE_WINDOW_SECONDS as f64 / 86400.0).min(1.0);
        
        let expected_drop = base_impact * (1.0 + (1.0 - market_depth_factor)) * (1.0 + time_factor);
        let expected_drop_bps = (expected_drop * 10_000.0) as u16;
        
        PriceImpact {
            expected_drop_bps,
            confidence: market_depth_factor,
        }
    }

    fn calculate_position_size(&self, unlock_event: &TokenUnlockEvent, price_data: &PriceData) -> u64 {
        let max_impact_size = (price_data.liquidity as f64 * 0.02) as u64;
        let unlock_size = (unlock_event.unlock_amount as f64 * price_data.price) as u64;
        let risk_adjusted_size = unlock_size.min(max_impact_size);
        
        risk_adjusted_size.min(MAX_POSITION_SIZE_SOL)
    }

    async fn find_best_route(&self, mint: &Pubkey) -> Result<RouteInfo, Box<dyn std::error::Error>> {
        let wsol_mint = spl_token::native_mint::id();
        
        // Try Raydium first
        if let Ok(raydium_route) = self.find_raydium_route(mint, &wsol_mint).await {
            return Ok(raydium_route);
        }
        
        // Fallback to Orca
        if let Ok(orca_route) = self.find_orca_route(mint, &wsol_mint).await {
            return Ok(orca_route);
        }
        
        Err("No route found".into())
    }

    async fn find_raydium_route(&self, token_mint: &Pubkey, quote_mint: &Pubkey) -> Result<RouteInfo, Box<dyn std::error::Error>> {
        let pool_seeds = &[
            &RAYDIUM_AMM_V4.to_bytes()[..],
            &token_mint.to_bytes()[..],
            &quote_mint.to_bytes()[..],
            b"pool_seed",
        ];
        
        let (pool_pubkey, _) = Pubkey::find_program_address(pool_seeds, &RAYDIUM_AMM_V4);
        
        let account = self.rpc_client.get_account(&pool_pubkey)?;
        unsafe {
            let amm_info = std::ptr::read(account.data.as_ptr() as *const RaydiumAmmInfo);
            
            Ok(RouteInfo {
                dex: DexType::Raydium,
                pool: pool_pubkey,
                input_vault: amm_info.token_coin,
                output_vault: amm_info.token_pc,
            })
        }
    }

    async fn find_orca_route(&self, token_mint: &Pubkey, quote_mint: &Pubkey) -> Result<RouteInfo, Box<dyn std::error::Error>> {
        let whirlpool_config = Pubkey::from_str("2LecshUwdy9xi7meFgHtFJQNSKk4KdTrcpvaB56dP2NQ")?;
        let pool_seeds = &[
            b"whirlpool",
            &whirlpool_config.to_bytes()[..],
            &token_mint.to_bytes()[..],
            &quote_mint.to_bytes()[..],
            &64u16.to_le_bytes()[..],
        ];
        
        let (pool_pubkey, _) = Pubkey::find_program_address(pool_seeds, &ORCA_WHIRLPOOL);
        
        let vault_a_seeds = &[pool_pubkey.as_ref(), token_mint.as_ref()];
        let vault_b_seeds = &[pool_pubkey.as_ref(), quote_mint.as_ref()];
        
        let (vault_a, _) = Pubkey::find_program_address(vault_a_seeds, &TOKEN_PROGRAM_ID);
        let (vault_b, _) = Pubkey::find_program_address(vault_b_seeds, &TOKEN_PROGRAM_ID);
        
        Ok(RouteInfo {
            dex: DexType::Orca,
            pool: pool_pubkey,
            input_vault: vault_a,
            output_vault: vault_b,
        })
    }

    async fn queue_trade_opportunity(&self, opportunity: TradeOpportunity) -> Result<(), Box<dyn std::error::Error>> {
        let current_ts = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs() as i64;
        let time_until_unlock = opportunity.unlock_event.unlock_timestamp - current_ts;
        
        if time_until_unlock > 300 && time_until_unlock <= UNLOCK_TRADE_WINDOW_SECONDS {
            self.execute_short_position(opportunity).await?;
        }
        
        Ok(())
    }

    async fn execute_trades_loop(&self) -> Result<(), Box<dyn std::error::Error>> {
        let mut interval = interval(Duration::from_secs(10));
        
        loop {
            interval.tick().await;
            if let Err(e) = self.manage_positions().await {
                eprintln!("Error managing positions: {}", e);
            }
        }
    }

    async fn manage_positions(&self) -> Result<(), Box<dyn std::error::Error>> {
        let positions = self.active_positions.lock().unwrap().clone();
        let current_ts = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs() as i64;
        
        for (mint, position) in positions {
            let time_since_unlock = current_ts - position.unlock_event.unlock_timestamp;
            
            if time_since_unlock > 3600 || time_since_unlock < -300 {
                self.close_position(&mint, &position).await?;
            } else {
                self.monitor_position(&mint, &position).await?;
            }
        }
        
        Ok(())
    }

    async fn execute_short_position(&self, opportunity: TradeOpportunity) -> Result<(), Box<dyn std::error::Error>> {
        let positions = self.active_positions.lock().unwrap();
        if positions.contains_key(&opportunity.token_mint) {
            return Ok(());
        }
        drop(positions);
        
        let user_wsol_ata = spl_associated_token_account::get_associated_token_address(
            &self.wallet.pubkey(),
            &spl_token::native_mint::id()
        );
        let user_token_ata = spl_associated_token_account::get_associated_token_address(
            &self.wallet.pubkey(),
            &opportunity.token_mint
        );
        
        let mut instructions = vec![];
        
                instructions.push(ComputeBudgetInstruction::set_compute_unit_limit(400_000));
        instructions.push(ComputeBudgetInstruction::set_compute_unit_price(PRIORITY_FEE_MICROLAMPORTS));
        
        let swap_instruction = match opportunity.route.dex {
            DexType::Raydium => self.build_raydium_swap_instruction(
                &opportunity.route,
                &user_wsol_ata,
                &user_token_ata,
                opportunity.size_sol,
                true
            )?,
            DexType::Orca => self.build_orca_swap_instruction(
                &opportunity.route,
                &user_wsol_ata,
                &user_token_ata,
                opportunity.size_sol,
                true
            )?,
            DexType::Jupiter => return Err("Jupiter routing not implemented".into()),
        };
        
        instructions.push(swap_instruction);
        
        let tx = self.build_transaction(instructions).await?;
        
        match self.send_transaction_with_retry(&tx, 3).await {
            Ok(sig) => {
                let tokens_out = self.calculate_tokens_out(opportunity.size_sol, &opportunity.route).await?;
                
                let position = Position {
                    token_mint: opportunity.token_mint,
                    entry_price: self.get_current_price(&opportunity.token_mint).await?,
                    size_tokens: tokens_out,
                    size_sol: opportunity.size_sol,
                    entry_timestamp: SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs() as i64,
                    unlock_event: opportunity.unlock_event,
                };
                
                let mut positions = self.active_positions.lock().unwrap();
                positions.insert(opportunity.token_mint, position);
                
                println!("Opened short position: {} - tx: {}", opportunity.token_mint, sig);
            }
            Err(e) => eprintln!("Failed to open position: {}", e),
        }
        
        Ok(())
    }

    fn build_raydium_swap_instruction(
        &self,
        route: &RouteInfo,
        user_source: &Pubkey,
        user_dest: &Pubkey,
        amount_in: u64,
        is_base_to_quote: bool,
    ) -> Result<Instruction, Box<dyn std::error::Error>> {
        let (authority, _) = Pubkey::find_program_address(
            &[&RAYDIUM_AMM_V4.to_bytes()[..]],
            &RAYDIUM_AMM_V4
        );
        
        let mut accounts = vec![
            AccountMeta::new_readonly(TOKEN_PROGRAM_ID, false),
            AccountMeta::new(route.pool, false),
            AccountMeta::new_readonly(authority, false),
            AccountMeta::new_readonly(Pubkey::from_str("5Q544fKrFoe6tsEbD7S8EmxGTJYAKtTVhAW5Q5pge4j1")?, false),
        ];
        
        if is_base_to_quote {
            accounts.extend([
                AccountMeta::new(route.input_vault, false),
                AccountMeta::new(route.output_vault, false),
                AccountMeta::new(*user_source, false),
                AccountMeta::new(*user_dest, false),
            ]);
        } else {
            accounts.extend([
                AccountMeta::new(route.output_vault, false),
                AccountMeta::new(route.input_vault, false),
                AccountMeta::new(*user_dest, false),
                AccountMeta::new(*user_source, false),
            ]);
        }
        
        accounts.push(AccountMeta::new_readonly(self.wallet.pubkey(), true));
        
        let mut data = vec![9];
        data.extend_from_slice(&amount_in.to_le_bytes());
        data.extend_from_slice(&0u64.to_le_bytes());
        
        Ok(Instruction {
            program_id: RAYDIUM_AMM_V4,
            accounts,
            data,
        })
    }

    fn build_orca_swap_instruction(
        &self,
        route: &RouteInfo,
        user_source: &Pubkey,
        user_dest: &Pubkey,
        amount_in: u64,
        is_a_to_b: bool,
    ) -> Result<Instruction, Box<dyn std::error::Error>> {
        let (tick_array_0, _) = Pubkey::find_program_address(
            &[b"tick_array", route.pool.as_ref(), &(-84480i32).to_le_bytes()],
            &ORCA_WHIRLPOOL
        );
        let (tick_array_1, _) = Pubkey::find_program_address(
            &[b"tick_array", route.pool.as_ref(), &0i32.to_le_bytes()],
            &ORCA_WHIRLPOOL
        );
        let (tick_array_2, _) = Pubkey::find_program_address(
            &[b"tick_array", route.pool.as_ref(), &84480i32.to_le_bytes()],
            &ORCA_WHIRLPOOL
        );
        let (oracle, _) = Pubkey::find_program_address(
            &[b"oracle", route.pool.as_ref()],
            &ORCA_WHIRLPOOL
        );
        
        let accounts = vec![
            AccountMeta::new_readonly(TOKEN_PROGRAM_ID, false),
            AccountMeta::new_readonly(self.wallet.pubkey(), true),
            AccountMeta::new(route.pool, false),
            AccountMeta::new(*user_source, false),
            AccountMeta::new(route.input_vault, false),
            AccountMeta::new(*user_dest, false),
            AccountMeta::new(route.output_vault, false),
            AccountMeta::new(tick_array_0, false),
            AccountMeta::new(tick_array_1, false),
            AccountMeta::new(tick_array_2, false),
            AccountMeta::new_readonly(oracle, false),
        ];
        
        let mut data = vec![0xf8];
        data.extend_from_slice(&amount_in.to_le_bytes());
        data.extend_from_slice(&0u64.to_le_bytes());
        data.extend_from_slice(&(u128::MAX / 2).to_le_bytes());
        data.push(if is_a_to_b { 1 } else { 0 });
        
        Ok(Instruction {
            program_id: ORCA_WHIRLPOOL,
            accounts,
            data,
        })
    }

    async fn calculate_tokens_out(&self, sol_amount: u64, route: &RouteInfo) -> Result<u64, Box<dyn std::error::Error>> {
        let price_data = match route.dex {
            DexType::Raydium => self.get_raydium_price(&route.pool.clone(), &spl_token::native_mint::id()).await?,
            DexType::Orca => self.get_orca_price(&route.pool.clone(), &spl_token::native_mint::id()).await?,
            _ => (0.0, 0),
        };
        
        let sol_value = sol_amount as f64 / 1e9;
        let tokens_out = (sol_value / price_data.0 * 0.997) as u64;
        
        Ok(tokens_out)
    }

    async fn close_position(&self, mint: &Pubkey, position: &Position) -> Result<(), Box<dyn std::error::Error>> {
        let route = self.find_best_route(mint).await?;
        
        let user_token_ata = spl_associated_token_account::get_associated_token_address(
            &self.wallet.pubkey(),
            mint
        );
        let user_wsol_ata = spl_associated_token_account::get_associated_token_address(
            &self.wallet.pubkey(),
            &spl_token::native_mint::id()
        );
        
        let mut instructions = vec![];
        
        instructions.push(ComputeBudgetInstruction::set_compute_unit_limit(400_000));
        instructions.push(ComputeBudgetInstruction::set_compute_unit_price(PRIORITY_FEE_MICROLAMPORTS));
        
        let swap_instruction = match route.dex {
            DexType::Raydium => self.build_raydium_swap_instruction(
                &route,
                &user_token_ata,
                &user_wsol_ata,
                position.size_tokens,
                false
            )?,
            DexType::Orca => self.build_orca_swap_instruction(
                &route,
                &user_token_ata,
                &user_wsol_ata,
                position.size_tokens,
                false
            )?,
            DexType::Jupiter => return Err("Jupiter routing not implemented".into()),
        };
        
        instructions.push(swap_instruction);
        
        let tx = self.build_transaction(instructions).await?;
        
        match self.send_transaction_with_retry(&tx, 3).await {
            Ok(sig) => {
                let mut positions = self.active_positions.lock().unwrap();
                positions.remove(mint);
                println!("Closed position: {} - tx: {}", mint, sig);
            }
            Err(e) => eprintln!("Failed to close position: {}", e),
        }
        
        Ok(())
    }

    async fn monitor_position(&self, mint: &Pubkey, position: &Position) -> Result<(), Box<dyn std::error::Error>> {
        let current_price = self.get_current_price(mint).await?;
        let pnl_bps = ((position.entry_price - current_price) / position.entry_price * 10_000.0) as i16;
        
        if pnl_bps < -150 || pnl_bps > 500 {
            self.close_position(mint, position).await?;
        }
        
        Ok(())
    }

    async fn get_current_price(&self, mint: &Pubkey) -> Result<f64, Box<dyn std::error::Error>> {
        let price_data = self.get_price_data(mint).await?;
        Ok(price_data.price)
    }

    async fn build_transaction(&self, instructions: Vec<Instruction>) -> Result<Transaction, Box<dyn std::error::Error>> {
        let recent_blockhash = self.rpc_client.get_latest_blockhash()?;
        
        let tx = Transaction::new_signed_with_payer(
            &instructions,
            Some(&self.wallet.pubkey()),
            &[&*self.wallet],
            recent_blockhash,
        );
        
        Ok(tx)
    }

    async fn send_transaction_with_retry(&self, tx: &Transaction, max_retries: u32) -> Result<String, Box<dyn std::error::Error>> {
        let mut retries = 0;
        
        loop {
            match self.rpc_client.send_and_confirm_transaction_with_spinner(tx) {
                Ok(sig) => return Ok(sig.to_string()),
                Err(e) => {
                    retries += 1;
                    if retries >= max_retries {
                        return Err(Box::new(e));
                    }
                    
                    let error_msg = e.to_string();
                    if error_msg.contains("BlockhashNotFound") || error_msg.contains("AlreadyProcessed") {
                        sleep(Duration::from_millis(500)).await;
                        continue;
                    }
                    
                    if error_msg.contains("InsufficientFunds") || error_msg.contains("AccountNotFound") {
                        return Err(Box::new(e));
                    }
                    
                    sleep(Duration::from_millis(200)).await;
                }
            }
        }
    }

    async fn update_prices_loop(&self) -> Result<(), Box<dyn std::error::Error>> {
        let mut interval = interval(Duration::from_secs(30));
        
        loop {
            interval.tick().await;
            
            let tracked = self.tracked_vesting_accounts.lock().unwrap().clone();
            let mut unique_mints = std::collections::HashSet::new();
            
            for timelock in tracked.values() {
                unique_mints.insert(timelock.mint);
            }
            
            for mint in unique_mints {
                if let Err(e) = self.fetch_price_from_dex(&mint).await {
                    eprintln!("Error updating price for {}: {}", mint, e);
                }
            }
        }
    }
}

impl Clone for TokenUnlockScheduleTrader {
    fn clone(&self) -> Self {
        Self {
            rpc_client: Arc::clone(&self.rpc_client),
            wallet: Arc::clone(&self.wallet),
            tracked_vesting_accounts: Arc::clone(&self.tracked_vesting_accounts),
            active_positions: Arc::clone(&self.active_positions),
            price_cache: Arc::clone(&self.price_cache),
        }
    }
}

