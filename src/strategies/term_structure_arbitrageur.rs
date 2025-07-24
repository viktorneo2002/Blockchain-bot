use anchor_lang::prelude::*;
use solana_client::{
    nonblocking::rpc_client::RpcClient,
    rpc_config::{RpcSendTransactionConfig, RpcTransactionConfig},
};
use solana_sdk::{
    commitment_config::{CommitmentConfig, CommitmentLevel},
    compute_budget::ComputeBudgetInstruction,
    instruction::{AccountMeta, Instruction},
    pubkey::Pubkey,
    signature::{Keypair, Signer},
    transaction::Transaction,
    sysvar,
};
use spl_token::state::Account as TokenAccount;
use pyth_sdk_solana::state::{PriceAccount, PriceStatus};
use switchboard_v2::AggregatorAccountData;
use std::{
    collections::HashMap,
    str::FromStr,
    sync::{Arc, RwLock},
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::time::sleep;
use borsh::{BorshDeserialize, BorshSerialize};

// Production mainnet program IDs
const SOLEND_PROGRAM_ID: &str = "So1endDq2YkqhipRh3WViPa8hdiSpxWy6z3Z6tMCpAo";
const KAMINO_PROGRAM_ID: &str = "KLend2g3cP87fffoy8q1mQqGKjrxjC8boSyAYavgmjD";
const MARGINFI_PROGRAM_ID: &str = "MFv2hWf31Z9kbCa1snEPYctwafyhdvnV7FZnsebVacA";
const DRIFT_PROGRAM_ID: &str = "dRiftyHA39MWEi3m9aunc5MzRF1JYuBsbn6VPcn33UH";

// Oracle program IDs
const PYTH_PROGRAM_ID: &str = "FsJ3A3u2vn5cTVofAjvy6y5kwABJAqYWpe4975bi2epH";
const SWITCHBOARD_V2_PROGRAM_ID: &str = "SW1TCH7qEPTdLsDHRgPuMQjbQxKdH2aBStViMFnt64f";

// Protocol constants
const SOLEND_LTV_PRECISION: u64 = 10000;
const KAMINO_PRECISION: u64 = 1_000_000_000;
const MARGINFI_PRICE_PRECISION: u64 = 1_000_000_000_000_000_000;

const MIN_PROFIT_BPS: u64 = 20;
const MAX_SLIPPAGE_BPS: u64 = 25;
const PRIORITY_FEE_LAMPORTS: u64 = 30_000;
const MAX_COMPUTE_UNITS: u32 = 1_400_000;

#[derive(Debug, Clone, PartialEq)]
pub enum Protocol {
    Solend,
    Kamino,
    MarginFi,
    Drift,
}

#[derive(Debug, Clone)]
pub struct MarketInfo {
    pub protocol: Protocol,
    pub market_pubkey: Pubkey,
    pub token_mint: Pubkey,
    pub supply_rate: u64,
    pub borrow_rate: u64,
    pub utilization_rate: u64,
    pub available_liquidity: u64,
    pub total_borrows: u64,
    pub total_deposits: u64,
    pub oracle_price: u64,
    pub oracle_confidence: u64,
    pub last_update_slot: u64,
}

#[derive(Debug, Clone)]
pub struct ArbitrageOpportunity {
    pub id: [u8; 32],
    pub borrow_market: MarketInfo,
    pub lend_market: MarketInfo,
    pub optimal_amount: u64,
    pub expected_profit: u64,
    pub profit_bps: u64,
    pub confidence_score: f64,
}

#[derive(Debug, Clone)]
pub struct ActivePosition {
    pub id: [u8; 32],
    pub borrow_market: Pubkey,
    pub lend_market: Pubkey,
    pub token_mint: Pubkey,
    pub amount: u64,
    pub entry_slot: u64,
    pub entry_timestamp: i64,
    pub borrow_rate_snapshot: u64,
    pub lend_rate_snapshot: u64,
    pub accumulated_interest: i64,
}

pub struct TermStructureArbitrageur {
    rpc_client: Arc<RpcClient>,
    wallet: Arc<Keypair>,
    markets: Arc<RwLock<HashMap<Pubkey, MarketInfo>>>,
    active_positions: Arc<RwLock<Vec<ActivePosition>>>,
    oracle_cache: Arc<RwLock<HashMap<Pubkey, OracleData>>>,
}

#[derive(Debug, Clone)]
struct OracleData {
    price: u64,
    confidence: u64,
    last_update_slot: u64,
    oracle_type: OracleType,
}

#[derive(Debug, Clone)]
enum OracleType {
    Pyth,
    Switchboard,
}

// Solend account structures
#[derive(BorshDeserialize, BorshSerialize, Debug)]
struct SolendReserve {
    pub version: u8,
    pub last_update: LastUpdate,
    pub lending_market: Pubkey,
    pub liquidity: ReserveLiquidity,
    pub collateral: ReserveCollateral,
    pub config: ReserveConfig,
}

#[derive(BorshDeserialize, BorshSerialize, Debug)]
struct LastUpdate {
    pub slot: u64,
    pub stale: bool,
}

#[derive(BorshDeserialize, BorshSerialize, Debug)]
struct ReserveLiquidity {
    pub mint_pubkey: Pubkey,
    pub mint_decimals: u8,
    pub supply_pubkey: Pubkey,
    pub fee_receiver: Pubkey,
    pub oracle_pubkey: Pubkey,
    pub oracle_option: u64,
    pub available_amount: u64,
    pub borrowed_amount_wads: u128,
    pub cumulative_borrow_rate_wads: u128,
    pub market_price: u128,
}

#[derive(BorshDeserialize, BorshSerialize, Debug)]
struct ReserveCollateral {
    pub mint_pubkey: Pubkey,
    pub mint_total_supply: u64,
    pub supply_pubkey: Pubkey,
}

#[derive(BorshDeserialize, BorshSerialize, Debug)]
struct ReserveConfig {
    pub optimal_utilization_rate: u8,
    pub loan_to_value_ratio: u8,
    pub liquidation_bonus: u8,
    pub liquidation_threshold: u8,
    pub min_borrow_rate: u8,
    pub optimal_borrow_rate: u8,
    pub max_borrow_rate: u8,
    pub fees: ReserveFees,
    pub deposit_limit: u64,
    pub borrow_limit: u64,
    pub fee_receiver: Pubkey,
    pub protocol_liquidation_fee: u8,
    pub protocol_take_rate: u8,
}

#[derive(BorshDeserialize, BorshSerialize, Debug)]
struct ReserveFees {
    pub borrow_fee_wad: u64,
    pub flash_loan_fee_wad: u64,
    pub host_fee_percentage: u8,
}

impl TermStructureArbitrageur {
    pub fn new(rpc_url: &str, wallet: Keypair) -> Self {
        let rpc_client = Arc::new(RpcClient::new_with_commitment(
            rpc_url.to_string(),
            CommitmentConfig {
                commitment: CommitmentLevel::Confirmed,
            },
        ));

        Self {
            rpc_client,
            wallet: Arc::new(wallet),
            markets: Arc::new(RwLock::new(HashMap::new())),
            active_positions: Arc::new(RwLock::new(Vec::new())),
            oracle_cache: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub async fn run(&self) -> Result<(), Box<dyn std::error::Error>> {
        let market_updater = self.spawn_market_updater();
        let oracle_updater = self.spawn_oracle_updater();
        let arbitrage_executor = self.spawn_arbitrage_executor();
        let position_manager = self.spawn_position_manager();

        tokio::try_join!(
            market_updater,
            oracle_updater,
            arbitrage_executor,
            position_manager
        )?;

        Ok(())
    }

    fn spawn_market_updater(&self) -> tokio::task::JoinHandle<Result<(), Box<dyn std::error::Error>>> {
        let rpc = self.rpc_client.clone();
        let markets = self.markets.clone();
        let oracle_cache = self.oracle_cache.clone();

        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_millis(100));
            
            loop {
                interval.tick().await;
                
                // Update Solend markets
                if let Ok(solend_markets) = Self::fetch_solend_markets(&rpc).await {
                    for (pubkey, reserve) in solend_markets {
                        if let Ok(market_info) = Self::convert_solend_to_market_info(
                            pubkey,
                            reserve,
                            &oracle_cache,
                        ).await {
                            markets.write().unwrap().insert(pubkey, market_info);
                        }
                    }
                }

                // Update Kamino markets
                if let Ok(kamino_markets) = Self::fetch_kamino_markets(&rpc).await {
                    for (pubkey, market_data) in kamino_markets {
                        if let Ok(market_info) = Self::parse_kamino_market(
                            pubkey,
                            market_data,
                            &oracle_cache,
                        ).await {
                            markets.write().unwrap().insert(pubkey, market_info);
                        }
                    }
                }

                // Update MarginFi markets
                if let Ok(marginfi_banks) = Self::fetch_marginfi_banks(&rpc).await {
                    for (pubkey, bank_data) in marginfi_banks {
                        if let Ok(market_info) = Self::parse_marginfi_bank(
                            pubkey,
                            bank_data,
                            &oracle_cache,
                        ).await {
                            markets.write().unwrap().insert(pubkey, market_info);
                        }
                    }
                }

                // Update Drift markets
                if let Ok(drift_markets) = Self::fetch_drift_markets(&rpc).await {
                    for (pubkey, market_data) in drift_markets {
                        if let Ok(market_info) = Self::parse_drift_market(
                            pubkey,
                            market_data,
                            &oracle_cache,
                        ).await {
                            markets.write().unwrap().insert(pubkey, market_info);
                        }
                    }
                }
            }
        })
    }

    fn spawn_oracle_updater(&self) -> tokio::task::JoinHandle<Result<(), Box<dyn std::error::Error>>> {
        let rpc = self.rpc_client.clone();
        let oracle_cache = self.oracle_cache.clone();

        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_millis(50));
            
            loop {
                interval.tick().await;
                
                // Update Pyth oracles
                let pyth_program = Pubkey::from_str(PYTH_PROGRAM_ID).unwrap();
                if let Ok(accounts) = rpc.get_program_accounts(&pyth_program) {
                    for (pubkey, account) in accounts {
                        if let Ok(price_account) = PriceAccount::try_deserialize(&mut &account.data[..]) {
                            if price_account.agg.status == PriceStatus::Trading {
                                let price = (price_account.agg.price as u64)
                                    .saturating_mul(10u64.pow((-price_account.expo) as u32));
                                let confidence = (price_account.agg.conf as u64)
                                    .saturating_mul(10u64.pow((-price_account.expo) as u32));
                                
                                oracle_cache.write().unwrap().insert(pubkey, OracleData {
                                    price,
                                    confidence,
                                    last_update_slot: price_account.valid_slot,
                                    oracle_type: OracleType::Pyth,
                                });
                            }
                        }
                    }
                }

                // Update Switchboard oracles
                let switchboard_program = Pubkey::from_str(SWITCHBOARD_V2_PROGRAM_ID).unwrap();
                if let Ok(accounts) = rpc.get_program_accounts(&switchboard_program) {
                    for (pubkey, account) in accounts {
                        if let Ok(aggregator) = AggregatorAccountData::try_deserialize(&mut &account.data[..]) {
                            let result = aggregator.get_result().unwrap_or_default();
                            if result.mantissa != 0 {
                                let price = (result.mantissa.abs() as u64)
                                    .saturating_mul(10u64.pow((-result.scale) as u32));
                                
                                oracle_cache.write().unwrap().insert(pubkey, OracleData {
                                    price,
                                    confidence: price / 100, // 1% confidence approximation
                                    last_update_slot: aggregator.latest_confirmed_round.round_open_slot,
                                    oracle_type: OracleType::Switchboard,
                                });
                            }
                        }
                    }
                }
            }
        })
    }

    fn spawn_arbitrage_executor(&self) -> tokio::task::JoinHandle<Result<(), Box<dyn std::error::Error>>> {
        let rpc = self.rpc_client.clone();
        let wallet = self.wallet.clone();
        let markets = self.markets.clone();
        let positions = self.active_positions.clone();

        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_millis(25));
            
            loop {
                interval.tick().await;
                
                let current_markets = markets.read().unwrap().clone();
                let opportunities = Self::find_arbitrage_opportunities(&current_markets);
                
                                for opportunity in opportunities.iter().take(5) {
                    if opportunity.profit_bps >= MIN_PROFIT_BPS && opportunity.confidence_score >= 0.95 {
                        match Self::execute_arbitrage(&rpc, &wallet, opportunity, &positions).await {
                            Ok(position) => {
                                println!("Executed arbitrage: {} bps profit", opportunity.profit_bps);
                                positions.write().unwrap().push(position);
                            }
                            Err(e) => {
                                eprintln!("Arbitrage execution failed: {:?}", e);
                            }
                        }
                    }
                }
                
                // Small delay between execution attempts
                sleep(Duration::from_millis(10)).await;
            }
        })
    }

    fn spawn_position_manager(&self) -> tokio::task::JoinHandle<Result<(), Box<dyn std::error::Error>>> {
        let rpc = self.rpc_client.clone();
        let wallet = self.wallet.clone();
        let positions = self.active_positions.clone();
        let markets = self.markets.clone();

        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(30));
            
            loop {
                interval.tick().await;
                
                let current_positions = positions.read().unwrap().clone();
                let current_markets = markets.read().unwrap();
                
                for position in current_positions.iter() {
                    // Check if position should be closed
                    if let Some(borrow_market) = current_markets.get(&position.borrow_market) {
                        if let Some(lend_market) = current_markets.get(&position.lend_market) {
                            let current_spread = lend_market.supply_rate as i64 - borrow_market.borrow_rate as i64;
                            
                            // Close if spread becomes negative or position is too old
                            let position_age = rpc.get_slot().unwrap_or(0) - position.entry_slot;
                            if current_spread < 0 || position_age > 432_000 { // ~2 days at 2 slots/sec
                                match Self::close_position(&rpc, &wallet, position).await {
                                    Ok(_) => {
                                        println!("Closed position: {:?}", position.id);
                                        positions.write().unwrap().retain(|p| p.id != position.id);
                                    }
                                    Err(e) => {
                                        eprintln!("Failed to close position: {:?}", e);
                                    }
                                }
                            }
                        }
                    }
                }
            }
        })
    }

    async fn fetch_solend_markets(
        rpc: &RpcClient,
    ) -> Result<Vec<(Pubkey, SolendReserve)>, Box<dyn std::error::Error>> {
        let program_id = Pubkey::from_str(SOLEND_PROGRAM_ID)?;
        let accounts = rpc.get_program_accounts(&program_id)?;
        
        let mut markets = Vec::new();
        for (pubkey, account) in accounts {
            // Solend reserves are 619 bytes
            if account.data.len() == 619 {
                if let Ok(reserve) = SolendReserve::try_from_slice(&account.data) {
                    if reserve.version == 1 {
                        markets.push((pubkey, reserve));
                    }
                }
            }
        }
        
        Ok(markets)
    }

    async fn convert_solend_to_market_info(
        pubkey: Pubkey,
        reserve: SolendReserve,
        oracle_cache: &Arc<RwLock<HashMap<Pubkey, OracleData>>>,
    ) -> Result<MarketInfo, Box<dyn std::error::Error>> {
        let oracle_data = oracle_cache.read().unwrap()
            .get(&reserve.liquidity.oracle_pubkey)
            .cloned()
            .unwrap_or(OracleData {
                price: reserve.liquidity.market_price as u64,
                confidence: 0,
                last_update_slot: 0,
                oracle_type: OracleType::Pyth,
            });

        let borrowed_amount = reserve.liquidity.borrowed_amount_wads / 10u128.pow(18);
        let available_amount = reserve.liquidity.available_amount;
        let total_deposits = available_amount + borrowed_amount as u64;
        
        let utilization_rate = if total_deposits > 0 {
            (borrowed_amount * 10000 / total_deposits as u128) as u64
        } else {
            0
        };

        let borrow_rate = Self::calculate_solend_borrow_rate(&reserve.config, utilization_rate);
        let supply_rate = Self::calculate_solend_supply_rate(borrow_rate, utilization_rate);

        Ok(MarketInfo {
            protocol: Protocol::Solend,
            market_pubkey: pubkey,
            token_mint: reserve.liquidity.mint_pubkey,
            supply_rate,
            borrow_rate,
            utilization_rate,
            available_liquidity: available_amount,
            total_borrows: borrowed_amount as u64,
            total_deposits,
            oracle_price: oracle_data.price,
            oracle_confidence: oracle_data.confidence,
            last_update_slot: reserve.last_update.slot,
        })
    }

    fn calculate_solend_borrow_rate(config: &ReserveConfig, utilization_rate: u64) -> u64 {
        let optimal_util = config.optimal_utilization_rate as u64 * 100; // Convert to basis points
        
        if utilization_rate <= optimal_util {
            let min_rate = config.min_borrow_rate as u64;
            let rate_range = config.optimal_borrow_rate as u64 - min_rate;
            min_rate + (rate_range * utilization_rate / optimal_util)
        } else {
            let optimal_rate = config.optimal_borrow_rate as u64;
            let max_rate = config.max_borrow_rate as u64;
            let excess_util = utilization_rate - optimal_util;
            let excess_util_rate = if optimal_util < 10000 {
                excess_util * 10000 / (10000 - optimal_util)
            } else {
                10000
            };
            optimal_rate + ((max_rate - optimal_rate) * excess_util_rate / 10000)
        }
    }

    fn calculate_solend_supply_rate(borrow_rate: u64, utilization_rate: u64) -> u64 {
        let protocol_take_rate = 100; // 1% in basis points
        borrow_rate * utilization_rate * (10000 - protocol_take_rate) / 100_000_000
    }

    async fn fetch_kamino_markets(
        rpc: &RpcClient,
    ) -> Result<Vec<(Pubkey, Vec<u8>)>, Box<dyn std::error::Error>> {
        let program_id = Pubkey::from_str(KAMINO_PROGRAM_ID)?;
        let accounts = rpc.get_program_accounts(&program_id)?;
        
        let mut markets = Vec::new();
        for (pubkey, account) in accounts {
            // Kamino market accounts have specific discriminator
            if account.data.len() >= 8 && &account.data[0..8] == b"KaminoMt" {
                markets.push((pubkey, account.data));
            }
        }
        
        Ok(markets)
    }

    async fn parse_kamino_market(
        pubkey: Pubkey,
        data: Vec<u8>,
        oracle_cache: &Arc<RwLock<HashMap<Pubkey, OracleData>>>,
    ) -> Result<MarketInfo, Box<dyn std::error::Error>> {
        // Kamino market layout
        let token_mint = Pubkey::new(&data[8..40]);
        let borrow_rate_curve = &data[40..168]; // 128 bytes for curve parameters
        let total_deposits = u64::from_le_bytes(data[168..176].try_into()?);
        let total_borrows = u64::from_le_bytes(data[176..184].try_into()?);
        let oracle_pubkey = Pubkey::new(&data[184..216]);
        
        let utilization_rate = if total_deposits > 0 {
            (total_borrows * 10000 / total_deposits) as u64
        } else {
            0
        };

        let borrow_rate = Self::calculate_kamino_rate(borrow_rate_curve, utilization_rate);
        let supply_rate = borrow_rate * utilization_rate * 95 / 10000; // 5% protocol fee

        let oracle_data = oracle_cache.read().unwrap()
            .get(&oracle_pubkey)
            .cloned()
            .unwrap_or_default();

        Ok(MarketInfo {
            protocol: Protocol::Kamino,
            market_pubkey: pubkey,
            token_mint,
            supply_rate,
            borrow_rate,
            utilization_rate,
            available_liquidity: total_deposits.saturating_sub(total_borrows),
            total_borrows,
            total_deposits,
            oracle_price: oracle_data.price,
            oracle_confidence: oracle_data.confidence,
            last_update_slot: u64::from_le_bytes(data[216..224].try_into()?),
        })
    }

    fn calculate_kamino_rate(curve_data: &[u8], utilization: u64) -> u64 {
        let curve_type = curve_data[0];
        
        match curve_type {
            0 => { // Linear curve
                let base_rate = u64::from_le_bytes(curve_data[8..16].try_into().unwrap());
                let slope = u64::from_le_bytes(curve_data[16..24].try_into().unwrap());
                base_rate + (slope * utilization / 10000)
            }
            1 => { // Kinked curve
                let base_rate = u64::from_le_bytes(curve_data[8..16].try_into().unwrap());
                let optimal_util = u64::from_le_bytes(curve_data[16..24].try_into().unwrap());
                let slope1 = u64::from_le_bytes(curve_data[24..32].try_into().unwrap());
                let slope2 = u64::from_le_bytes(curve_data[32..40].try_into().unwrap());
                
                if utilization <= optimal_util {
                    base_rate + (slope1 * utilization / optimal_util)
                } else {
                    base_rate + slope1 + (slope2 * (utilization - optimal_util) / (10000 - optimal_util))
                }
            }
            _ => 0,
        }
    }

    async fn fetch_marginfi_banks(
        rpc: &RpcClient,
    ) -> Result<Vec<(Pubkey, Vec<u8>)>, Box<dyn std::error::Error>> {
        let program_id = Pubkey::from_str(MARGINFI_PROGRAM_ID)?;
        let accounts = rpc.get_program_accounts(&program_id)?;
        
        let mut banks = Vec::new();
        for (pubkey, account) in accounts {
            // MarginFi bank discriminator
            if account.data.len() == 1696 && &account.data[0..8] == [142, 49, 166, 242, 50, 66, 97, 188] {
                banks.push((pubkey, account.data));
            }
        }
        
        Ok(banks)
    }

    async fn parse_marginfi_bank(
        pubkey: Pubkey,
        data: Vec<u8>,
        oracle_cache: &Arc<RwLock<HashMap<Pubkey, OracleData>>>,
    ) -> Result<MarketInfo, Box<dyn std::error::Error>> {
        let mint = Pubkey::new(&data[40..72]);
        let mint_decimals = data[72];
        
        // Asset share value (fixed point)
        let asset_share_value = i128::from_le_bytes(data[184..200].try_into()?);
        let liability_share_value = i128::from_le_bytes(data[200..216].try_into()?);
        
        // Total assets and liabilities
        let total_asset_shares = i128::from_le_bytes(data[216..232].try_into()?);
        let total_liability_shares = i128::from_le_bytes(data[232..248].try_into()?);
        
        let total_deposits = (total_asset_shares.saturating_mul(asset_share_value) / MARGINFI_PRICE_PRECISION as i128) as u64;
        let total_borrows = (total_liability_shares.saturating_mul(liability_share_value) / MARGINFI_PRICE_PRECISION as i128) as u64;
        
        let utilization_rate = if total_deposits > 0 {
            (total_borrows * 10000 / total_deposits) as u64
        } else {
            0
        };

        // Interest rate config
        let ir_config = &data[584..608];
        let borrow_rate = Self::calculate_marginfi_rate(ir_config, utilization_rate);
        let insurance_fee_rate = u64::from_le_bytes(data[616..624].try_into()?);
        let protocol_fee_rate = u64::from_le_bytes(data[624..632].try_into()?);
        
        let total_fee_rate = insurance_fee_rate + protocol_fee_rate;
        let supply_rate = borrow_rate * utilization_rate * (10000 - total_fee_rate) / 100_000_000;

        let oracle_pubkey = Pubkey::new(&data[120..152]);
        let oracle_data = oracle_cache.read().unwrap()
            .get(&oracle_pubkey)
            .cloned()
            .unwrap_or_default();

        Ok(MarketInfo {
            protocol: Protocol::MarginFi,
            market_pubkey: pubkey,
            token_mint: mint,
            supply_rate,
            borrow_rate,
            utilization_rate,
            available_liquidity: total_deposits.saturating_sub(total_borrows),
            total_borrows,
            total_deposits,
            oracle_price: oracle_data.price,
            oracle_confidence: oracle_data.confidence,
            last_update_slot: u64::from_le_bytes(data[8..16].try_into()?),
        })
    }

        fn calculate_marginfi_rate(config: &[u8], utilization: u64) -> u64 {
        let optimal_rate = u64::from_le_bytes(config[0..8].try_into().unwrap());
        let max_rate = u64::from_le_bytes(config[8..16].try_into().unwrap());
        let base_rate = u64::from_le_bytes(config[16..24].try_into().unwrap());
        
        if utilization <= 8000 { // 80% utilization
            base_rate + (optimal_rate - base_rate) * utilization / 8000
        } else {
            optimal_rate + (max_rate - optimal_rate) * (utilization - 8000) / 2000
        }
    }

    async fn fetch_drift_markets(
        rpc: &RpcClient,
    ) -> Result<Vec<(Pubkey, Vec<u8>)>, Box<dyn std::error::Error>> {
        let program_id = Pubkey::from_str(DRIFT_PROGRAM_ID)?;
        let accounts = rpc.get_program_accounts(&program_id)?;
        
        let mut markets = Vec::new();
        for (pubkey, account) in accounts {
            // Drift spot market discriminator
            if account.data.len() >= 776 && &account.data[0..8] == [234, 33, 67, 152, 9, 222, 145, 245] {
                markets.push((pubkey, account.data));
            }
        }
        
        Ok(markets)
    }

    async fn parse_drift_market(
        pubkey: Pubkey,
        data: Vec<u8>,
        oracle_cache: &Arc<RwLock<HashMap<Pubkey, OracleData>>>,
    ) -> Result<MarketInfo, Box<dyn std::error::Error>> {
        let mint = Pubkey::new(&data[32..64]);
        let vault = Pubkey::new(&data[64..96]);
        let oracle = Pubkey::new(&data[96..128]);
        
        let deposit_balance = i128::from_le_bytes(data[256..272].try_into()?);
        let borrow_balance = i128::from_le_bytes(data[272..288].try_into()?);
        
        let cumulative_deposit_interest = i128::from_le_bytes(data[288..304].try_into()?);
        let cumulative_borrow_interest = i128::from_le_bytes(data[304..320].try_into()?);
        
        let decimals = data[400] as u32;
        let precision = 10u64.pow(decimals);
        
        let total_deposits = (deposit_balance / precision as i128) as u64;
        let total_borrows = (borrow_balance / precision as i128) as u64;
        
        let utilization_rate = if total_deposits > 0 {
            (total_borrows * 10000 / total_deposits) as u64
        } else {
            0
        };

        let optimal_utilization = u32::from_le_bytes(data[432..436].try_into()?);
        let optimal_borrow_rate = u64::from_le_bytes(data[436..444].try_into()?);
        let max_borrow_rate = u64::from_le_bytes(data[444..452].try_into()?);
        
        let borrow_rate = if utilization_rate <= optimal_utilization as u64 {
            optimal_borrow_rate * utilization_rate / optimal_utilization as u64
        } else {
            let excess_util = utilization_rate - optimal_utilization as u64;
            let max_excess_util = 10000 - optimal_utilization as u64;
            optimal_borrow_rate + (max_borrow_rate - optimal_borrow_rate) * excess_util / max_excess_util
        };
        
        let insurance_fund_fee = u32::from_le_bytes(data[452..456].try_into()?);
        let supply_rate = borrow_rate * utilization_rate * (10000 - insurance_fund_fee as u64) / 100_000_000;

        let oracle_data = oracle_cache.read().unwrap()
            .get(&oracle)
            .cloned()
            .unwrap_or_default();

        Ok(MarketInfo {
            protocol: Protocol::Drift,
            market_pubkey: pubkey,
            token_mint: mint,
            supply_rate,
            borrow_rate,
            utilization_rate,
            available_liquidity: total_deposits.saturating_sub(total_borrows),
            total_borrows,
            total_deposits,
            oracle_price: oracle_data.price,
            oracle_confidence: oracle_data.confidence,
            last_update_slot: u64::from_le_bytes(data[16..24].try_into()?),
        })
    }

    fn find_arbitrage_opportunities(markets: &HashMap<Pubkey, MarketInfo>) -> Vec<ArbitrageOpportunity> {
        let mut opportunities = Vec::new();
        let mut markets_by_mint: HashMap<Pubkey, Vec<&MarketInfo>> = HashMap::new();
        
        for market in markets.values() {
            markets_by_mint.entry(market.token_mint).or_default().push(market);
        }
        
        for (mint, mint_markets) in markets_by_mint {
            if mint_markets.len() < 2 {
                continue;
            }
            
            for i in 0..mint_markets.len() {
                for j in i+1..mint_markets.len() {
                    let market_a = mint_markets[i];
                    let market_b = mint_markets[j];
                    
                    // Check both directions
                    if market_a.supply_rate > market_b.borrow_rate + MIN_PROFIT_BPS {
                        let spread = market_a.supply_rate - market_b.borrow_rate;
                        opportunities.push(Self::create_opportunity(market_b, market_a, spread));
                    }
                    
                    if market_b.supply_rate > market_a.borrow_rate + MIN_PROFIT_BPS {
                        let spread = market_b.supply_rate - market_a.borrow_rate;
                        opportunities.push(Self::create_opportunity(market_a, market_b, spread));
                    }
                }
            }
        }
        
        opportunities.sort_by(|a, b| b.profit_bps.cmp(&a.profit_bps));
        opportunities
    }

    fn create_opportunity(
        borrow_market: &MarketInfo,
        lend_market: &MarketInfo,
        spread: u64,
    ) -> ArbitrageOpportunity {
        let max_borrow = borrow_market.available_liquidity;
        let max_lend = lend_market.available_liquidity;
        let max_amount = max_borrow.min(max_lend);
        
        // Conservative sizing - use 10% of available liquidity
        let optimal_amount = (max_amount / 10).min(1_000_000_000_000); // Cap at 1M tokens
        
        let confidence_score = Self::calculate_confidence_score(borrow_market, lend_market);
        
        let mut id = [0u8; 32];
        id[0..16].copy_from_slice(&borrow_market.market_pubkey.to_bytes()[0..16]);
        id[16..32].copy_from_slice(&lend_market.market_pubkey.to_bytes()[0..16]);
        
        ArbitrageOpportunity {
            id,
            borrow_market: borrow_market.clone(),
            lend_market: lend_market.clone(),
            optimal_amount,
            expected_profit: optimal_amount * spread / 10000,
            profit_bps: spread,
            confidence_score,
        }
    }

    fn calculate_confidence_score(borrow_market: &MarketInfo, lend_market: &MarketInfo) -> f64 {
        let mut score = 1.0;
        
        // Reduce confidence for high utilization
        if borrow_market.utilization_rate > 9000 {
            score *= 0.8;
        }
        if lend_market.utilization_rate > 9000 {
            score *= 0.8;
        }
        
        // Reduce confidence for stale data
        let current_slot = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();
        if current_slot - borrow_market.last_update_slot as u64 > 60 {
            score *= 0.9;
        }
        if current_slot - lend_market.last_update_slot as u64 > 60 {
            score *= 0.9;
        }
        
        // Reduce confidence for high oracle deviation
        let price_diff = (borrow_market.oracle_price as i64 - lend_market.oracle_price as i64).abs();
        let avg_price = (borrow_market.oracle_price + lend_market.oracle_price) / 2;
        if price_diff > avg_price as i64 / 100 {
            score *= 0.7;
        }
        
        score
    }

    async fn execute_arbitrage(
        rpc: &RpcClient,
        wallet: &Keypair,
        opportunity: &ArbitrageOpportunity,
        positions: &Arc<RwLock<Vec<ActivePosition>>>,
    ) -> Result<ActivePosition, Box<dyn std::error::Error>> {
        let mut instructions = vec![
            ComputeBudgetInstruction::set_compute_unit_limit(MAX_COMPUTE_UNITS),
            ComputeBudgetInstruction::set_compute_unit_price(PRIORITY_FEE_LAMPORTS / 1000),
        ];
        
        // Build borrow instruction
        match opportunity.borrow_market.protocol {
            Protocol::Solend => {
                instructions.push(Self::build_solend_borrow_ix(
                    &opportunity.borrow_market,
                    opportunity.optimal_amount,
                    &wallet.pubkey(),
                )?);
            }
            Protocol::Kamino => {
                instructions.push(Self::build_kamino_borrow_ix(
                    &opportunity.borrow_market,
                    opportunity.optimal_amount,
                    &wallet.pubkey(),
                )?);
            }
            Protocol::MarginFi => {
                instructions.push(Self::build_marginfi_borrow_ix(
                    &opportunity.borrow_market,
                    opportunity.optimal_amount,
                    &wallet.pubkey(),
                )?);
            }
            Protocol::Drift => {
                instructions.push(Self::build_drift_borrow_ix(
                    &opportunity.borrow_market,
                    opportunity.optimal_amount,
                    &wallet.pubkey(),
                )?);
            }
        }
        
        // Build lend instruction
        match opportunity.lend_market.protocol {
            Protocol::Solend => {
                instructions.push(Self::build_solend_deposit_ix(
                    &opportunity.lend_market,
                    opportunity.optimal_amount,
                    &wallet.pubkey(),
                )?);
            }
            Protocol::Kamino => {
                instructions.push(Self::build_kamino_deposit_ix(
                    &opportunity.lend_market,
                    opportunity.optimal_amount,
                    &wallet.pubkey(),
                )?);
            }
            Protocol::MarginFi => {
                instructions.push(Self::build_marginfi_deposit_ix(
                    &opportunity.lend_market,
                    opportunity.optimal_amount,
                    &wallet.pubkey(),
                )?);
            }
            Protocol::Drift => {
                instructions.push(Self::build_drift_deposit_ix(
                    &opportunity.lend_market,
                    opportunity.optimal_amount,
                    &wallet.pubkey(),
                )?);
            }
        }
        
        let blockhash = rpc.get_latest_blockhash()?;
        let transaction = Transaction::new_signed_with_payer(
            &instructions,
            Some(&wallet.pubkey()),
            &[wallet],
            blockhash,
        );
        
        let config = RpcSendTransactionConfig {
            skip_preflight: false,
            preflight_commitment: Some(CommitmentLevel::Processed),
            encoding: None,
            max_retries: Some(2),
            min_context_slot: None,
        };
        
        let signature = rpc.send_transaction_with_config(&transaction, config)?;
        let current_slot = rpc.get_slot()?;
        
        let position = ActivePosition {
            id: opportunity.id,
            borrow_market: opportunity.borrow_market.market_pubkey,
            lend_market: opportunity.lend_market.market_pubkey,
            token_mint: opportunity.borrow_market.token_mint,
            amount: opportunity.optimal_amount,
            entry_slot: current_slot,
            entry_timestamp: SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs() as i64,
            borrow_rate_snapshot: opportunity.borrow_market.borrow_rate,
            lend_rate_snapshot: opportunity.lend_market.supply_rate,
            accumulated_interest: 0,
        };
        
        println!("Arbitrage executed: {} -> {}, amount: {}, tx: {}",
            opportunity.borrow_market.protocol as u8,
            opportunity.lend_market.protocol as u8,
            opportunity.optimal_amount,
            signature
        );
        
        Ok(position)
    }

    fn build_solend_borrow_ix(
        market: &MarketInfo,
        amount: u64,
        user: &Pubkey,
    ) -> Result<Instruction, Box<dyn std::error::Error>> {
        let program_id = Pubkey::from_str(SOLEND_PROGRAM_ID)?;
        
        // Derive PDAs
        let (lending_market_auth, _) = Pubkey::find_program_address(
            &[market.market_pubkey.as_ref()],
            &program_id,
        );
        
        let user_obligation = spl_associated_token_account::get_associated_token_address(
            user,
            &market.token_mint,
        );
        
        // Instruction discriminator for RefreshReserveAndBorrowObligationLiquidity
        let mut data = vec![10u8, 78u8, 253u8, 11u8, 107u8, 23u8, 132u8, 145u8];
        data.extend_from_slice(&amount.to_le_bytes());
        
        Ok(Instruction {
            program_id,
            accounts: vec![
                AccountMeta::new(market.market_pubkey, false),
                AccountMeta::new(lending_market_auth, false),
                AccountMeta::new(user_obligation, false),
                AccountMeta::new(*user, true),
                                AccountMeta::new_readonly(sysvar::clock::id(), false),
                AccountMeta::new(market.token_mint, false),
                AccountMeta::new_readonly(spl_token::id(), false),
            ],
            data,
        })
    }

    fn build_solend_deposit_ix(
        market: &MarketInfo,
        amount: u64,
        user: &Pubkey,
    ) -> Result<Instruction, Box<dyn std::error::Error>> {
        let program_id = Pubkey::from_str(SOLEND_PROGRAM_ID)?;
        
        let (lending_market_auth, _) = Pubkey::find_program_address(
            &[market.market_pubkey.as_ref()],
            &program_id,
        );
        
        let user_liquidity = spl_associated_token_account::get_associated_token_address(
            user,
            &market.token_mint,
        );
        
        let (collateral_mint, _) = Pubkey::find_program_address(
            &[b"collateral", market.market_pubkey.as_ref()],
            &program_id,
        );
        
        let user_collateral = spl_associated_token_account::get_associated_token_address(
            user,
            &collateral_mint,
        );
        
        // Instruction discriminator for RefreshReserveAndDepositReserveLiquidity
        let mut data = vec![227u8, 50u8, 232u8, 46u8, 197u8, 97u8, 137u8, 52u8];
        data.extend_from_slice(&amount.to_le_bytes());
        
        Ok(Instruction {
            program_id,
            accounts: vec![
                AccountMeta::new(market.market_pubkey, false),
                AccountMeta::new(lending_market_auth, false),
                AccountMeta::new(user_liquidity, false),
                AccountMeta::new(user_collateral, false),
                AccountMeta::new(collateral_mint, false),
                AccountMeta::new(*user, true),
                AccountMeta::new_readonly(sysvar::clock::id(), false),
                AccountMeta::new_readonly(spl_token::id(), false),
            ],
            data,
        })
    }

    fn build_kamino_borrow_ix(
        market: &MarketInfo,
        amount: u64,
        user: &Pubkey,
    ) -> Result<Instruction, Box<dyn std::error::Error>> {
        let program_id = Pubkey::from_str(KAMINO_PROGRAM_ID)?;
        
        let (market_authority, _) = Pubkey::find_program_address(
            &[b"market_authority", market.market_pubkey.as_ref()],
            &program_id,
        );
        
        let (obligation, _) = Pubkey::find_program_address(
            &[b"obligation", market.market_pubkey.as_ref(), user.as_ref()],
            &program_id,
        );
        
        let (reserve, _) = Pubkey::find_program_address(
            &[b"reserve", market.market_pubkey.as_ref(), market.token_mint.as_ref()],
            &program_id,
        );
        
        let (vault, _) = Pubkey::find_program_address(
            &[b"vault", reserve.as_ref()],
            &program_id,
        );
        
        let user_token = spl_associated_token_account::get_associated_token_address(
            user,
            &market.token_mint,
        );
        
        // Kamino borrow instruction discriminator
        let mut data = vec![169u8, 129u8, 67u8, 156u8, 208u8, 93u8, 152u8, 84u8];
        data.extend_from_slice(&amount.to_le_bytes());
        
        Ok(Instruction {
            program_id,
            accounts: vec![
                AccountMeta::new(*user, true),
                AccountMeta::new(market.market_pubkey, false),
                AccountMeta::new(market_authority, false),
                AccountMeta::new(obligation, false),
                AccountMeta::new(reserve, false),
                AccountMeta::new(vault, false),
                AccountMeta::new(user_token, false),
                AccountMeta::new_readonly(spl_token::id(), false),
                AccountMeta::new_readonly(sysvar::instructions::id(), false),
            ],
            data,
        })
    }

    fn build_kamino_deposit_ix(
        market: &MarketInfo,
        amount: u64,
        user: &Pubkey,
    ) -> Result<Instruction, Box<dyn std::error::Error>> {
        let program_id = Pubkey::from_str(KAMINO_PROGRAM_ID)?;
        
        let (market_authority, _) = Pubkey::find_program_address(
            &[b"market_authority", market.market_pubkey.as_ref()],
            &program_id,
        );
        
        let (reserve, _) = Pubkey::find_program_address(
            &[b"reserve", market.market_pubkey.as_ref(), market.token_mint.as_ref()],
            &program_id,
        );
        
        let (vault, _) = Pubkey::find_program_address(
            &[b"vault", reserve.as_ref()],
            &program_id,
        );
        
        let (collateral_mint, _) = Pubkey::find_program_address(
            &[b"collateral_mint", reserve.as_ref()],
            &program_id,
        );
        
        let user_token = spl_associated_token_account::get_associated_token_address(
            user,
            &market.token_mint,
        );
        
        let user_collateral = spl_associated_token_account::get_associated_token_address(
            user,
            &collateral_mint,
        );
        
        // Kamino deposit instruction discriminator
        let mut data = vec![242u8, 35u8, 198u8, 137u8, 82u8, 225u8, 242u8, 182u8];
        data.extend_from_slice(&amount.to_le_bytes());
        
        Ok(Instruction {
            program_id,
            accounts: vec![
                AccountMeta::new(*user, true),
                AccountMeta::new(market.market_pubkey, false),
                AccountMeta::new(market_authority, false),
                AccountMeta::new(reserve, false),
                AccountMeta::new(vault, false),
                AccountMeta::new(collateral_mint, false),
                AccountMeta::new(user_token, false),
                AccountMeta::new(user_collateral, false),
                AccountMeta::new_readonly(spl_token::id(), false),
                AccountMeta::new_readonly(sysvar::instructions::id(), false),
            ],
            data,
        })
    }

    fn build_marginfi_borrow_ix(
        market: &MarketInfo,
        amount: u64,
        user: &Pubkey,
    ) -> Result<Instruction, Box<dyn std::error::Error>> {
        let program_id = Pubkey::from_str(MARGINFI_PROGRAM_ID)?;
        
        let (marginfi_group, _) = Pubkey::find_program_address(
            &[b"marginfi_group"],
            &program_id,
        );
        
        let (marginfi_account, _) = Pubkey::find_program_address(
            &[b"marginfi_account", user.as_ref(), marginfi_group.as_ref()],
            &program_id,
        );
        
        let (bank_authority, _) = Pubkey::find_program_address(
            &[b"liquidity_vault_auth", market.market_pubkey.as_ref()],
            &program_id,
        );
        
        let (liquidity_vault, _) = Pubkey::find_program_address(
            &[b"liquidity_vault", market.market_pubkey.as_ref()],
            &program_id,
        );
        
        let user_token = spl_associated_token_account::get_associated_token_address(
            user,
            &market.token_mint,
        );
        
        // MarginFi borrow instruction discriminator
        let mut data = vec![228u8, 253u8, 131u8, 202u8, 207u8, 116u8, 89u8, 18u8];
        data.extend_from_slice(&amount.to_le_bytes());
        
        Ok(Instruction {
            program_id,
            accounts: vec![
                AccountMeta::new(marginfi_group, false),
                AccountMeta::new(marginfi_account, false),
                AccountMeta::new(*user, true),
                AccountMeta::new(market.market_pubkey, false),
                AccountMeta::new(bank_authority, false),
                AccountMeta::new(liquidity_vault, false),
                AccountMeta::new(user_token, false),
                AccountMeta::new_readonly(spl_token::id(), false),
            ],
            data,
        })
    }

    fn build_marginfi_deposit_ix(
        market: &MarketInfo,
        amount: u64,
        user: &Pubkey,
    ) -> Result<Instruction, Box<dyn std::error::Error>> {
        let program_id = Pubkey::from_str(MARGINFI_PROGRAM_ID)?;
        
        let (marginfi_group, _) = Pubkey::find_program_address(
            &[b"marginfi_group"],
            &program_id,
        );
        
        let (marginfi_account, _) = Pubkey::find_program_address(
            &[b"marginfi_account", user.as_ref(), marginfi_group.as_ref()],
            &program_id,
        );
        
        let (bank_authority, _) = Pubkey::find_program_address(
            &[b"liquidity_vault_auth", market.market_pubkey.as_ref()],
            &program_id,
        );
        
        let (liquidity_vault, _) = Pubkey::find_program_address(
            &[b"liquidity_vault", market.market_pubkey.as_ref()],
            &program_id,
        );
        
        let user_token = spl_associated_token_account::get_associated_token_address(
            user,
            &market.token_mint,
        );
        
        // MarginFi deposit instruction discriminator  
        let mut data = vec![143u8, 227u8, 25u8, 103u8, 144u8, 95u8, 174u8, 209u8];
        data.extend_from_slice(&amount.to_le_bytes());
        
        Ok(Instruction {
            program_id,
            accounts: vec![
                AccountMeta::new(marginfi_group, false),
                AccountMeta::new(marginfi_account, false),
                AccountMeta::new(*user, true),
                AccountMeta::new(market.market_pubkey, false),
                AccountMeta::new(bank_authority, false),
                AccountMeta::new(liquidity_vault, false),
                AccountMeta::new(user_token, false),
                AccountMeta::new_readonly(spl_token::id(), false),
            ],
            data,
        })
    }

    fn build_drift_borrow_ix(
        market: &MarketInfo,
        amount: u64,
        user: &Pubkey,
    ) -> Result<Instruction, Box<dyn std::error::Error>> {
        let program_id = Pubkey::from_str(DRIFT_PROGRAM_ID)?;
        
        let (state, _) = Pubkey::find_program_address(
            &[b"drift_state"],
            &program_id,
        );
        
        let (user_account, _) = Pubkey::find_program_address(
            &[b"user", user.as_ref()],
            &program_id,
        );
        
        let (spot_market_vault, _) = Pubkey::find_program_address(
            &[b"spot_market_vault", market.market_pubkey.as_ref()],
            &program_id,
        );
        
        let user_token = spl_associated_token_account::get_associated_token_address(
            user,
            &market.token_mint,
        );
        
        // Drift withdraw instruction discriminator
        let mut data = vec![183u8, 18u8, 70u8, 156u8, 148u8, 109u8, 161u8, 34u8];
        data.extend_from_slice(&0u16.to_le_bytes()); // market index
        data.extend_from_slice(&amount.to_le_bytes());
        data.push(0); // reduce only = false
        
        Ok(Instruction {
            program_id,
            accounts: vec![
                AccountMeta::new(state, false),
                AccountMeta::new(*user, true),
                AccountMeta::new(user_account, false),
                AccountMeta::new(market.market_pubkey, false),
                AccountMeta::new(spot_market_vault, false),
                AccountMeta::new(user_token, false),
                AccountMeta::new_readonly(spl_token::id(), false),
            ],
            data,
        })
    }

    fn build_drift_deposit_ix(
        market: &MarketInfo,
        amount: u64,
        user: &Pubkey,
    ) -> Result<Instruction, Box<dyn std::error::Error>> {
        let program_id = Pubkey::from_str(DRIFT_PROGRAM_ID)?;
        
        let (state, _) = Pubkey::find_program_address(
            &[b"drift_state"],
            &program_id,
        );
        
        let (user_account, _) = Pubkey::find_program_address(
            &[b"user", user.as_ref()],
            &program_id,
        );
        
        let (spot_market_vault, _) = Pubkey::find_program_address(
            &[b"spot_market_vault", market.market_pubkey.as_ref()],
            &program_id,
        );
        
        let user_token = spl_associated_token_account::get_associated_token_address(
            user,
            &market.token_mint,
        );
        
        // Drift deposit instruction discriminator
        let mut data = vec![134u8, 138u8, 246u8, 155u8, 83u8, 195u8, 131u8, 26u8];
                data.extend_from_slice(&0u16.to_le_bytes()); // market index
        data.extend_from_slice(&amount.to_le_bytes());
        data.push(0); // reduce only = false
        
        Ok(Instruction {
            program_id,
            accounts: vec![
                AccountMeta::new(state, false),
                AccountMeta::new(*user, true),
                AccountMeta::new(user_account, false),
                AccountMeta::new(market.market_pubkey, false),
                AccountMeta::new(spot_market_vault, false),
                AccountMeta::new(user_token, false),
                AccountMeta::new_readonly(spl_token::id(), false),
            ],
            data,
        })
    }

    async fn close_position(
        rpc: &RpcClient,
        wallet: &Keypair,
        position: &ActivePosition,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let mut instructions = vec![
            ComputeBudgetInstruction::set_compute_unit_limit(MAX_COMPUTE_UNITS),
            ComputeBudgetInstruction::set_compute_unit_price(PRIORITY_FEE_LAMPORTS / 1000),
        ];
        
        // Get market info to determine protocols
        let markets = Self::get_markets_for_position(rpc, position).await?;
        let (borrow_market, lend_market) = markets;
        
        // First withdraw from lending protocol
        match lend_market.protocol {
            Protocol::Solend => {
                instructions.push(Self::build_solend_withdraw_ix(
                    &lend_market,
                    position.amount,
                    &wallet.pubkey(),
                )?);
            }
            Protocol::Kamino => {
                instructions.push(Self::build_kamino_withdraw_ix(
                    &lend_market,
                    position.amount,
                    &wallet.pubkey(),
                )?);
            }
            Protocol::MarginFi => {
                instructions.push(Self::build_marginfi_withdraw_ix(
                    &lend_market,
                    position.amount,
                    &wallet.pubkey(),
                )?);
            }
            Protocol::Drift => {
                instructions.push(Self::build_drift_withdraw_ix(
                    &lend_market,
                    position.amount,
                    &wallet.pubkey(),
                )?);
            }
        }
        
        // Then repay to borrowing protocol
        match borrow_market.protocol {
            Protocol::Solend => {
                instructions.push(Self::build_solend_repay_ix(
                    &borrow_market,
                    position.amount,
                    &wallet.pubkey(),
                )?);
            }
            Protocol::Kamino => {
                instructions.push(Self::build_kamino_repay_ix(
                    &borrow_market,
                    position.amount,
                    &wallet.pubkey(),
                )?);
            }
            Protocol::MarginFi => {
                instructions.push(Self::build_marginfi_repay_ix(
                    &borrow_market,
                    position.amount,
                    &wallet.pubkey(),
                )?);
            }
            Protocol::Drift => {
                instructions.push(Self::build_drift_repay_ix(
                    &borrow_market,
                    position.amount,
                    &wallet.pubkey(),
                )?);
            }
        }
        
        let blockhash = rpc.get_latest_blockhash()?;
        let transaction = Transaction::new_signed_with_payer(
            &instructions,
            Some(&wallet.pubkey()),
            &[wallet],
            blockhash,
        );
        
        let config = RpcSendTransactionConfig {
            skip_preflight: false,
            preflight_commitment: Some(CommitmentLevel::Processed),
            encoding: None,
            max_retries: Some(3),
            min_context_slot: None,
        };
        
        let signature = rpc.send_transaction_with_config(&transaction, config)?;
        println!("Position closed: {}", signature);
        
        Ok(())
    }

    async fn get_markets_for_position(
        rpc: &RpcClient,
        position: &ActivePosition,
    ) -> Result<(MarketInfo, MarketInfo), Box<dyn std::error::Error>> {
        let borrow_account = rpc.get_account(&position.borrow_market)?;
        let lend_account = rpc.get_account(&position.lend_market)?;
        
        let borrow_market = Self::parse_market_account(&position.borrow_market, &borrow_account)?;
        let lend_market = Self::parse_market_account(&position.lend_market, &lend_account)?;
        
        Ok((borrow_market, lend_market))
    }

    fn parse_market_account(
        pubkey: &Pubkey,
        account: &solana_sdk::account::Account,
    ) -> Result<MarketInfo, Box<dyn std::error::Error>> {
        // Determine protocol by program owner
        let protocol = match account.owner.to_string().as_str() {
            SOLEND_PROGRAM_ID => Protocol::Solend,
            KAMINO_PROGRAM_ID => Protocol::Kamino,
            MARGINFI_PROGRAM_ID => Protocol::MarginFi,
            DRIFT_PROGRAM_ID => Protocol::Drift,
            _ => return Err("Unknown protocol".into()),
        };
        
        // Create basic market info - would need full parsing in production
        Ok(MarketInfo {
            protocol,
            market_pubkey: *pubkey,
            token_mint: Pubkey::default(), // Would parse from account data
            supply_rate: 0,
            borrow_rate: 0,
            utilization_rate: 0,
            available_liquidity: 0,
            total_borrows: 0,
            total_deposits: 0,
            oracle_price: 0,
            oracle_confidence: 0,
            last_update_slot: 0,
        })
    }

    fn build_solend_withdraw_ix(
        market: &MarketInfo,
        amount: u64,
        user: &Pubkey,
    ) -> Result<Instruction, Box<dyn std::error::Error>> {
        let program_id = Pubkey::from_str(SOLEND_PROGRAM_ID)?;
        
        let (lending_market_auth, _) = Pubkey::find_program_address(
            &[market.market_pubkey.as_ref()],
            &program_id,
        );
        
        let user_collateral = spl_associated_token_account::get_associated_token_address(
            user,
            &market.token_mint,
        );
        
        let user_liquidity = spl_associated_token_account::get_associated_token_address(
            user,
            &market.token_mint,
        );
        
        // Instruction discriminator for RedeemReserveCollateral
        let mut data = vec![234u8, 100u8, 95u8, 91u8, 218u8, 61u8, 58u8, 88u8];
        data.extend_from_slice(&amount.to_le_bytes());
        
        Ok(Instruction {
            program_id,
            accounts: vec![
                AccountMeta::new(market.market_pubkey, false),
                AccountMeta::new(user_collateral, false),
                AccountMeta::new(user_liquidity, false),
                AccountMeta::new(lending_market_auth, false),
                AccountMeta::new(*user, true),
                AccountMeta::new_readonly(sysvar::clock::id(), false),
                AccountMeta::new_readonly(spl_token::id(), false),
            ],
            data,
        })
    }

    fn build_solend_repay_ix(
        market: &MarketInfo,
        amount: u64,
        user: &Pubkey,
    ) -> Result<Instruction, Box<dyn std::error::Error>> {
        let program_id = Pubkey::from_str(SOLEND_PROGRAM_ID)?;
        
        let user_liquidity = spl_associated_token_account::get_associated_token_address(
            user,
            &market.token_mint,
        );
        
        let user_obligation = spl_associated_token_account::get_associated_token_address(
            user,
            &market.token_mint,
        );
        
        // Instruction discriminator for RepayObligationLiquidity
        let mut data = vec![67u8, 243u8, 175u8, 226u8, 201u8, 73u8, 44u8, 182u8];
        data.extend_from_slice(&amount.to_le_bytes());
        
        Ok(Instruction {
            program_id,
            accounts: vec![
                AccountMeta::new(market.market_pubkey, false),
                AccountMeta::new(user_liquidity, false),
                AccountMeta::new(user_obligation, false),
                AccountMeta::new(*user, true),
                AccountMeta::new_readonly(sysvar::clock::id(), false),
                AccountMeta::new_readonly(spl_token::id(), false),
            ],
            data,
        })
    }

    fn build_kamino_withdraw_ix(
        market: &MarketInfo,
        amount: u64,
        user: &Pubkey,
    ) -> Result<Instruction, Box<dyn std::error::Error>> {
        let program_id = Pubkey::from_str(KAMINO_PROGRAM_ID)?;
        
        let (market_authority, _) = Pubkey::find_program_address(
            &[b"market_authority", market.market_pubkey.as_ref()],
            &program_id,
        );
        
        let (reserve, _) = Pubkey::find_program_address(
            &[b"reserve", market.market_pubkey.as_ref(), market.token_mint.as_ref()],
            &program_id,
        );
        
        let (vault, _) = Pubkey::find_program_address(
            &[b"vault", reserve.as_ref()],
            &program_id,
        );
        
        let (collateral_mint, _) = Pubkey::find_program_address(
            &[b"collateral_mint", reserve.as_ref()],
            &program_id,
        );
        
        let user_token = spl_associated_token_account::get_associated_token_address(
            user,
            &market.token_mint,
        );
        
        let user_collateral = spl_associated_token_account::get_associated_token_address(
            user,
            &collateral_mint,
        );
        
        // Kamino withdraw instruction discriminator
        let mut data = vec![183u8, 61u8, 49u8, 23u8, 19u8, 120u8, 232u8, 92u8];
        data.extend_from_slice(&amount.to_le_bytes());
        
        Ok(Instruction {
            program_id,
            accounts: vec![
                AccountMeta::new(*user, true),
                AccountMeta::new(market.market_pubkey, false),
                AccountMeta::new(market_authority, false),
                AccountMeta::new(reserve, false),
                AccountMeta::new(vault, false),
                AccountMeta::new(collateral_mint, false),
                AccountMeta::new(user_collateral, false),
                AccountMeta::new(user_token, false),
                AccountMeta::new_readonly(spl_token::id(), false),
                AccountMeta::new_readonly(sysvar::instructions::id(), false),
            ],
            data,
        })
    }

    fn build_kamino_repay_ix(
        market: &MarketInfo,
        amount: u64,
        user: &Pubkey,
    ) -> Result<Instruction, Box<dyn std::error::Error>> {
        let program_id = Pubkey::from_str(KAMINO_PROGRAM_ID)?;
        
        let (market_authority, _) = Pubkey::find_program_address(
            &[b"market_authority", market.market_pubkey.as_ref()],
            &program_id,
        );
        
        let (obligation, _) = Pubkey::find_program_address(
            &[b"obligation", market.market_pubkey.as_ref(), user.as_ref()],
            &program_id,
        );
        
        let (reserve, _) = Pubkey::find_program_address(
            &[b"reserve", market.market_pubkey.as_ref(), market.token_mint.as_ref()],
            &program_id,
        );
        
        let (vault, _) = Pubkey::find_program_address(
            &[b"vault", reserve.as_ref()],
            &program_id,
        );
        
        let user_token = spl_associated_token_account::get_associated_token_address(
            user,
            &market.token_mint,
        );
        
        // Kamino repay instruction discriminator
        let mut data = vec![100u8, 223u8, 180u8, 115u8, 254u8, 218u8, 7u8, 37u8];
        data.extend_from_slice(&amount.to_le_bytes());
        
        Ok(Instruction {
            program_id,
            accounts: vec![
                AccountMeta::new(*user, true),
                AccountMeta::new(market.market_pubkey, false),
                AccountMeta::new(market_authority, false),
                AccountMeta::new(obligation, false),
                AccountMeta::new(reserve, false),
                AccountMeta::new(vault, false),
                AccountMeta::new(user_token, false),
                AccountMeta::new_readonly(spl_token::id(), false),
                AccountMeta::new_readonly(sysvar::instructions::id(), false),
            ],
            data,
        })
    }

    fn build_marginfi_withdraw_ix(
        market: &MarketInfo,
        amount: u64,
        user: &Pubkey,
    ) -> Result<Instruction, Box<dyn std::error::Error>> {
        let program_id = Pubkey::from_str(MARGINFI_PROGRAM_ID)?;
        
        let (marginfi_group, _) = Pubkey::find_program_address(
            &[b"marginfi_group"],
            &program_id,
        );
        
        let (marginfi_account, _) = Pubkey::find_program_address(
            &[b"marginfi_account", user.as_ref(), marginfi_group.as_ref()],
            &program_id,
        );
        
                let (bank_authority, _) = Pubkey::find_program_address(
            &[b"liquidity_vault_auth", market.market_pubkey.as_ref()],
            &program_id,
        );
        
        let (liquidity_vault, _) = Pubkey::find_program_address(
            &[b"liquidity_vault", market.market_pubkey.as_ref()],
            &program_id,
        );
        
        let user_token = spl_associated_token_account::get_associated_token_address(
            user,
            &market.token_mint,
        );
        
        // MarginFi withdraw instruction discriminator
        let mut data = vec![145u8, 176u8, 206u8, 9u8, 94u8, 149u8, 29u8, 178u8];
        data.extend_from_slice(&amount.to_le_bytes());
        data.push(1); // withdraw all = false
        
        Ok(Instruction {
            program_id,
            accounts: vec![
                AccountMeta::new(marginfi_group, false),
                AccountMeta::new(marginfi_account, false),
                AccountMeta::new(*user, true),
                AccountMeta::new(market.market_pubkey, false),
                AccountMeta::new(bank_authority, false),
                AccountMeta::new(liquidity_vault, false),
                AccountMeta::new(user_token, false),
                AccountMeta::new_readonly(spl_token::id(), false),
            ],
            data,
        })
    }

    fn build_marginfi_repay_ix(
        market: &MarketInfo,
        amount: u64,
        user: &Pubkey,
    ) -> Result<Instruction, Box<dyn std::error::Error>> {
        let program_id = Pubkey::from_str(MARGINFI_PROGRAM_ID)?;
        
        let (marginfi_group, _) = Pubkey::find_program_address(
            &[b"marginfi_group"],
            &program_id,
        );
        
        let (marginfi_account, _) = Pubkey::find_program_address(
            &[b"marginfi_account", user.as_ref(), marginfi_group.as_ref()],
            &program_id,
        );
        
        let (bank_authority, _) = Pubkey::find_program_address(
            &[b"liquidity_vault_auth", market.market_pubkey.as_ref()],
            &program_id,
        );
        
        let (liquidity_vault, _) = Pubkey::find_program_address(
            &[b"liquidity_vault", market.market_pubkey.as_ref()],
            &program_id,
        );
        
        let user_token = spl_associated_token_account::get_associated_token_address(
            user,
            &market.token_mint,
        );
        
        // MarginFi repay instruction discriminator
        let mut data = vec![44u8, 183u8, 23u8, 127u8, 87u8, 58u8, 215u8, 108u8];
        data.extend_from_slice(&amount.to_le_bytes());
        data.push(0); // repay all = false
        
        Ok(Instruction {
            program_id,
            accounts: vec![
                AccountMeta::new(marginfi_group, false),
                AccountMeta::new(marginfi_account, false),
                AccountMeta::new(*user, true),
                AccountMeta::new(market.market_pubkey, false),
                AccountMeta::new(bank_authority, false),
                AccountMeta::new(liquidity_vault, false),
                AccountMeta::new(user_token, false),
                AccountMeta::new_readonly(spl_token::id(), false),
            ],
            data,
        })
    }

    fn build_drift_withdraw_ix(
        market: &MarketInfo,
        amount: u64,
        user: &Pubkey,
    ) -> Result<Instruction, Box<dyn std::error::Error>> {
        let program_id = Pubkey::from_str(DRIFT_PROGRAM_ID)?;
        
        let (state, _) = Pubkey::find_program_address(
            &[b"drift_state"],
            &program_id,
        );
        
        let (user_account, _) = Pubkey::find_program_address(
            &[b"user", user.as_ref()],
            &program_id,
        );
        
        let (spot_market_vault, _) = Pubkey::find_program_address(
            &[b"spot_market_vault", market.market_pubkey.as_ref()],
            &program_id,
        );
        
        let (drift_signer, _) = Pubkey::find_program_address(
            &[b"drift_signer"],
            &program_id,
        );
        
        let user_token = spl_associated_token_account::get_associated_token_address(
            user,
            &market.token_mint,
        );
        
        // Drift withdraw instruction discriminator
        let mut data = vec![183u8, 18u8, 70u8, 156u8, 148u8, 109u8, 161u8, 34u8];
        data.extend_from_slice(&0u16.to_le_bytes()); // market index
        data.extend_from_slice(&amount.to_le_bytes());
        data.push(0); // reduce only = false
        
        Ok(Instruction {
            program_id,
            accounts: vec![
                AccountMeta::new(state, false),
                AccountMeta::new(*user, true),
                AccountMeta::new(user_account, false),
                AccountMeta::new(market.market_pubkey, false),
                AccountMeta::new(spot_market_vault, false),
                AccountMeta::new(drift_signer, false),
                AccountMeta::new(user_token, false),
                AccountMeta::new_readonly(spl_token::id(), false),
            ],
            data,
        })
    }

    fn build_drift_repay_ix(
        market: &MarketInfo,
        amount: u64,
        user: &Pubkey,
    ) -> Result<Instruction, Box<dyn std::error::Error>> {
        let program_id = Pubkey::from_str(DRIFT_PROGRAM_ID)?;
        
        let (state, _) = Pubkey::find_program_address(
            &[b"drift_state"],
            &program_id,
        );
        
        let (user_account, _) = Pubkey::find_program_address(
            &[b"user", user.as_ref()],
            &program_id,
        );
        
        let (spot_market_vault, _) = Pubkey::find_program_address(
            &[b"spot_market_vault", market.market_pubkey.as_ref()],
            &program_id,
        );
        
        let user_token = spl_associated_token_account::get_associated_token_address(
            user,
            &market.token_mint,
        );
        
        // Drift deposit instruction discriminator (same as deposit but used for repay)
        let mut data = vec![134u8, 138u8, 246u8, 155u8, 83u8, 195u8, 131u8, 26u8];
        data.extend_from_slice(&0u16.to_le_bytes()); // market index
        data.extend_from_slice(&amount.to_le_bytes());
        data.push(0); // reduce only = false
        
        Ok(Instruction {
            program_id,
            accounts: vec![
                AccountMeta::new(state, false),
                AccountMeta::new(*user, true),
                AccountMeta::new(user_account, false),
                AccountMeta::new(market.market_pubkey, false),
                AccountMeta::new(spot_market_vault, false),
                AccountMeta::new(user_token, false),
                AccountMeta::new_readonly(spl_token::id(), false),
            ],
            data,
        })
    }

    async fn update_oracle_cache(
        rpc: &RpcClient,
        oracle_cache: &Arc<RwLock<HashMap<Pubkey, OracleData>>>,
        oracle_pubkeys: Vec<Pubkey>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let accounts = rpc.get_multiple_accounts(&oracle_pubkeys)?;
        let mut cache = oracle_cache.write().unwrap();
        
        for (i, account_opt) in accounts.iter().enumerate() {
            if let Some(account) = account_opt {
                if let Ok(oracle_data) = Self::parse_oracle_account(&oracle_pubkeys[i], account) {
                    cache.insert(oracle_pubkeys[i], oracle_data);
                }
            }
        }
        
        Ok(())
    }

    fn parse_oracle_account(
        pubkey: &Pubkey,
        account: &solana_sdk::account::Account,
    ) -> Result<OracleData, Box<dyn std::error::Error>> {
        // Pyth oracle
        if account.owner == Pubkey::from_str("FsJ3A3u2vn5cTVofAjvy6y5kwABJAqYWpe4975bi2epH")? {
            if account.data.len() >= 48 {
                let magic = u32::from_le_bytes(account.data[0..4].try_into()?);
                if magic == 0xa1b2c3d4 {
                    let price = i64::from_le_bytes(account.data[208..216].try_into()?);
                    let confidence = u64::from_le_bytes(account.data[216..224].try_into()?);
                    let expo = i32::from_le_bytes(account.data[20..24].try_into()?);
                    
                    let normalized_price = if expo < 0 {
                        (price as u64) / 10u64.pow((-expo) as u32)
                    } else {
                        (price as u64) * 10u64.pow(expo as u32)
                    };
                    
                    return Ok(OracleData {
                        price: normalized_price,
                        confidence,
                        last_update_slot: u64::from_le_bytes(account.data[240..248].try_into()?),
                        oracle_type: OracleType::Pyth,
                    });
                }
            }
        }
        
        // Switchboard oracle
        if account.owner == Pubkey::from_str("SW1TCH7qEPTdLsDHRgPuMQjbQxKdH2aBStViMFnt64f")? {
            if account.data.len() >= 137 {
                let discriminator = &account.data[0..8];
                if discriminator == [217, 230, 65, 101, 201, 162, 27, 125] {
                    let result = i128::from_le_bytes(account.data[33..49].try_into()?);
                    let decimals = account.data[49];
                    let price = (result / 10i128.pow(decimals as u32)) as u64;
                    
                    return Ok(OracleData {
                        price,
                        confidence: 0,
                        last_update_slot: u64::from_le_bytes(account.data[65..73].try_into()?),
                        oracle_type: OracleType::Switchboard,
                    });
                }
            }
        }
        
        Err("Unknown oracle format".into())
    }

    pub fn get_metrics(&self) -> ArbitrageMetrics {
        let positions = self.active_positions.read().unwrap();
        let total_value_locked: u64 = positions.iter().map(|p| p.amount).sum();
        let active_positions = positions.len();
        
        let markets = self.markets.read().unwrap();
        let total_markets = markets.len();
        
        let avg_spread = if !markets.is_empty() {
            let total_spread: u64 = markets.values()
                .map(|m| m.supply_rate.saturating_sub(m.borrow_rate))
                .sum();
            total_spread / markets.len() as u64
        } else {
            0
        };
        
        ArbitrageMetrics {
            total_value_locked,
            active_positions,
            total_markets,
            average_spread_bps: avg_spread,
            last_update: SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs() as i64,
        }
    }
}

#[derive(Clone)]
pub struct ArbitrageMetrics {
    pub total_value_locked: u64,
    pub active_positions: usize,
    pub total_markets: usize,
    pub average_spread_bps: u64,
    pub last_update: i64,
}

impl Default for OracleData {
    fn default() -> Self {
        Self {
            price: 0,
            confidence: 0,
            last_update_slot: 0,
            oracle_type: OracleType::Pyth,
        }
    }
}

