use solana_client::{
    nonblocking::rpc_client::RpcClient,
    rpc_config::RpcProgramAccountsConfig,
};
use solana_sdk::{
    account::Account,
    commitment_config::CommitmentConfig,
    compute_budget::ComputeBudgetInstruction,
    instruction::{AccountMeta, Instruction},
    pubkey::Pubkey,
    signature::{Keypair, Signer},
    sysvar,
    transaction::Transaction,
};
use spl_associated_token_account::get_associated_token_address;
use std::{
    collections::HashMap,
    error::Error as StdError,
    fmt,
    str::FromStr,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};
use tokio::{
    sync::{Mutex, RwLock},
    time::interval,
};
use log::{debug, error, info, warn};

const RAYDIUM_AMM_V4: &str = "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8";
const ORCA_WHIRLPOOL: &str = "whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc";
const OPENBOOK_V3: &str = "srmqPvymJeFKQ4zGQed1GFppgkRHL9kaELCbyksJtPX";

const MIN_PROFIT_BPS: u64 = 15;
const MAX_SLIPPAGE_BPS: u64 = 300;
const COMPUTE_UNITS: u32 = 1_400_000;
const PRIORITY_FEE_MICROLAMPORTS: u64 = 25_000;
const STALE_PRICE_THRESHOLD_MS: u64 = 2000;
const MIN_LIQUIDITY_SOL: u64 = 100_000_000_000;

#[derive(Debug)]
pub enum ArbitrageError {
    RpcError(String),
    ParseError(String),
    InsufficientLiquidity,
    StalePrice,
    ExecutionFailed(String),
}

impl fmt::Display for ArbitrageError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::RpcError(e) => write!(f, "RPC error: {}", e),
            Self::ParseError(e) => write!(f, "Parse error: {}", e),
            Self::InsufficientLiquidity => write!(f, "Insufficient liquidity"),
            Self::StalePrice => write!(f, "Stale price data"),
            Self::ExecutionFailed(e) => write!(f, "Execution failed: {}", e),
        }
    }
}

impl StdError for ArbitrageError {}

type Result<T> = std::result::Result<T, ArbitrageError>;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct MarketId(pub Pubkey);

#[derive(Debug, Clone)]
pub struct TickSize {
    pub base_decimals: u8,
    pub quote_decimals: u8,
    pub base_lot_size: u64,
    pub quote_lot_size: u64,
    pub min_tick: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DexProtocol {
    Raydium,
    Orca,
    Openbook,
}

#[derive(Debug, Clone)]
pub struct MarketInfo {
    pub id: MarketId,
    pub base_mint: Pubkey,
    pub quote_mint: Pubkey,
    pub base_vault: Pubkey,
    pub quote_vault: Pubkey,
    pub tick_size: TickSize,
    pub protocol: DexProtocol,
    pub fee_bps: u16,
    pub authority: Option<Pubkey>,
}

#[derive(Debug, Clone)]
pub struct PriceData {
    pub bid: u64,
    pub ask: u64,
    pub last_slot: u64,
    pub timestamp: Instant,
    pub base_liquidity: u64,
    pub quote_liquidity: u64,
}

#[derive(Debug, Clone)]
pub struct ArbitrageOpportunity {
    pub buy_market: MarketId,
    pub sell_market: MarketId,
    pub size: u64,
    pub profit_bps: u64,
    pub buy_price: u64,
    pub sell_price: u64,
}

pub struct TickSizeArbitrageur {
    rpc: Arc<RpcClient>,
    payer: Arc<Keypair>,
    markets: Arc<RwLock<HashMap<MarketId, MarketInfo>>>,
    prices: Arc<RwLock<HashMap<MarketId, PriceData>>>,
    executing: Arc<AtomicBool>,
    last_slot: Arc<AtomicU64>,
}

impl TickSizeArbitrageur {
    pub fn new(rpc_url: &str, payer_keypair: Keypair) -> Self {
        let rpc = Arc::new(RpcClient::new_with_commitment(
            rpc_url.to_string(),
            CommitmentConfig::processed(),
        ));

        Self {
            rpc,
            payer: Arc::new(payer_keypair),
            markets: Arc::new(RwLock::new(HashMap::new())),
            prices: Arc::new(RwLock::new(HashMap::new())),
            executing: Arc::new(AtomicBool::new(false)),
            last_slot: Arc::new(AtomicU64::new(0)),
        }
    }

    pub async fn run(&self) -> Result<()> {
        info!("Starting tick size arbitrageur");
        
        let market_updater = self.clone();
        let price_updater = self.clone();
        let arbitrage_scanner = self.clone();

        let handles = vec![
            tokio::spawn(async move { market_updater.update_markets_loop().await }),
            tokio::spawn(async move { price_updater.update_prices_loop().await }),
            tokio::spawn(async move { arbitrage_scanner.scan_arbitrage_loop().await }),
        ];

        for handle in handles {
            handle.await.map_err(|e| ArbitrageError::ExecutionFailed(e.to_string()))??;
        }

        Ok(())
    }

    async fn update_markets_loop(&self) -> Result<()> {
        let mut interval = interval(Duration::from_secs(300));
        
        loop {
            interval.tick().await;
            
            if let Err(e) = self.refresh_markets().await {
                error!("Market refresh error: {:?}", e);
            }
        }
    }

    async fn update_prices_loop(&self) -> Result<()> {
        let mut interval = interval(Duration::from_millis(100));
        
        loop {
            interval.tick().await;
            
            if let Err(e) = self.update_all_prices().await {
                warn!("Price update error: {:?}", e);
            }
        }
    }

    async fn scan_arbitrage_loop(&self) -> Result<()> {
        let mut interval = interval(Duration::from_millis(50));
        
        loop {
            interval.tick().await;
            
            if self.executing.load(Ordering::Relaxed) {
                continue;
            }
            
            match self.find_best_opportunity().await {
                Ok(Some(opp)) if opp.profit_bps > MIN_PROFIT_BPS => {
                    self.executing.store(true, Ordering::Relaxed);
                    
                    match self.execute_arbitrage(&opp).await {
                        Ok(sig) => info!("Executed: {} profit: {}bps", sig, opp.profit_bps),
                        Err(e) => error!("Execution failed: {:?}", e),
                    }
                    
                    self.executing.store(false, Ordering::Relaxed);
                }
                Ok(_) => {},
                Err(e) => debug!("Opportunity scan error: {:?}", e),
            }
        }
    }

    async fn refresh_markets(&self) -> Result<()> {
        let (raydium, orca, openbook) = tokio::try_join!(
            self.fetch_raydium_markets(),
            self.fetch_orca_markets(),
            self.fetch_openbook_markets()
        )?;

        let mut markets = self.markets.write().await;
        markets.clear();

        for market in raydium.into_iter().chain(orca).chain(openbook) {
            markets.insert(market.id, market);
        }

        info!("Loaded {} markets", markets.len());
        Ok(())
    }

    async fn fetch_raydium_markets(&self) -> Result<Vec<MarketInfo>> {
        let program_id = Pubkey::from_str(RAYDIUM_AMM_V4)
            .map_err(|e| ArbitrageError::ParseError(e.to_string()))?;
        
        let accounts = self.rpc.get_program_accounts(&program_id).await
            .map_err(|e| ArbitrageError::RpcError(e.to_string()))?;
        
        let mut markets = Vec::new();
        for (pubkey, account) in accounts {
            if account.data.len() != 752 {
                continue;
            }

            let base_mint = Self::parse_pubkey(&account.data[73..105])?;
            let quote_mint = Self::parse_pubkey(&account.data[105..137])?;
            let base_vault = Self::parse_pubkey(&account.data[233..265])?;
            let quote_vault = Self::parse_pubkey(&account.data[265..297])?;
            let authority = Self::parse_pubkey(&account.data[137..169])?;
            
            let base_decimals = account.data[75];
            let quote_decimals = account.data[107];

            markets.push(MarketInfo {
                id: MarketId(pubkey),
                base_mint,
                quote_mint,
                base_vault,
                quote_vault,
                tick_size: TickSize {
                    base_decimals,
                    quote_decimals,
                    base_lot_size: 10u64.pow(base_decimals.saturating_sub(6) as u32),
                    quote_lot_size: 10u64.pow(quote_decimals.saturating_sub(6) as u32),
                    min_tick: 1,
                },
                protocol: DexProtocol::Raydium,
                fee_bps: 25,
                authority: Some(authority),
            });
        }
        
        Ok(markets)
    }

    async fn fetch_orca_markets(&self) -> Result<Vec<MarketInfo>> {
        let program_id = Pubkey::from_str(ORCA_WHIRLPOOL)
            .map_err(|e| ArbitrageError::ParseError(e.to_string()))?;
            
        let accounts = self.rpc.get_program_accounts(&program_id).await
            .map_err(|e| ArbitrageError::RpcError(e.to_string()))?;
        
        let mut markets = Vec::new();
        for (pubkey, account) in accounts {
            if account.data.len() < 653 {
                continue;
            }

            let token_a = Self::parse_pubkey(&account.data[101..133])?;
            let token_b = Self::parse_pubkey(&account.data[181..213])?;
            let token_vault_a = Self::parse_pubkey(&account.data[133..165])?;
            let token_vault_b = Self::parse_pubkey(&account.data[165..197])?;
            
            let tick_spacing = u16::from_le_bytes([account.data[213], account.data[214]]);
            let fee = u16::from_le_bytes([account.data[747], account.data[748]]);

            markets.push(MarketInfo {
                id: MarketId(pubkey),
                base_mint: token_a,
                quote_mint: token_b,
                base_vault: token_vault_a,
                quote_vault: token_vault_b,
                tick_size: TickSize {
                    base_decimals: 9,
                    quote_decimals: 9,
                    base_lot_size: 1,
                    quote_lot_size: 1,
                    min_tick: tick_spacing as u64,
                },
                protocol: DexProtocol::Orca,
                fee_bps: fee / 100,
                authority: None,
            });
        }
        
        Ok(markets)
    }

    async fn fetch_openbook_markets(&self) -> Result<Vec<MarketInfo>> {
        let program_id = Pubkey::from_str(OPENBOOK_V3)
            .map_err(|e| ArbitrageError::ParseError(e.to_string()))?;
            
        let accounts = self.rpc.get_program_accounts(&program_id).await
            .map_err(|e| ArbitrageError::RpcError(e.to_string()))?;
        
        let mut markets = Vec::new();
        for (pubkey, account) in accounts {
            if account.data.len() < 388 {
                continue;
            }

            let base_mint = Self::parse_pubkey(&account.data[53..85])?;
            let quote_mint = Self::parse_pubkey(&account.data[85..117])?;
            let base_vault = Self::parse_pubkey(&account.data[349..381])?;
            let quote_vault = Self::parse_pubkey(&account.data[381..413])?;
            
            let base_lot_size = u64::from_le_bytes(Self::get_bytes(&account.data[117..125])?);
            let quote_lot_size = u64::from_le_bytes(Self::get_bytes(&account.data[125..133])?);
            let fee_rate = u64::from_le_bytes(Self::get_bytes(&account.data[213..221])?);

            markets.push(MarketInfo {
                id: MarketId(pubkey),
                base_mint,
                quote_mint,
                base_vault,
                quote_vault,
                tick_size: TickSize {
                    base_decimals: 9,
                    quote_decimals: 6,
                    base_lot_size,
                    quote_lot_size,
                                        min_tick: quote_lot_size,
                },
                protocol: DexProtocol::Openbook,
                fee_bps: (fee_rate * 10000 / 1_000_000) as u16,
                authority: None,
            });
        }
        
        Ok(markets)
    }

    async fn update_all_prices(&self) -> Result<()> {
        let current_slot = self.rpc.get_slot().await
            .map_err(|e| ArbitrageError::RpcError(e.to_string()))?;
        self.last_slot.store(current_slot, Ordering::Relaxed);

        let markets = self.markets.read().await.clone();
        let mut price_updates = HashMap::new();

        for (id, market) in markets {
            let price_result = match market.protocol {
                DexProtocol::Raydium => self.fetch_raydium_price(&market).await,
                DexProtocol::Orca => self.fetch_orca_price(&market).await,
                DexProtocol::Openbook => self.fetch_openbook_price(&market).await,
            };
            
            if let Ok(mut price) = price_result {
                price.last_slot = current_slot;
                price_updates.insert(id, price);
            }
        }

        let mut prices = self.prices.write().await;
        prices.extend(price_updates);
        
        Ok(())
    }

    async fn fetch_raydium_price(&self, market: &MarketInfo) -> Result<PriceData> {
        let account = self.rpc.get_account(&market.id.0).await
            .map_err(|e| ArbitrageError::RpcError(e.to_string()))?;
        
        if account.data.len() < 225 {
            return Err(ArbitrageError::ParseError("Invalid Raydium account data".to_string()));
        }
        
        let base_reserve = u64::from_le_bytes(Self::get_bytes(&account.data[209..217])?);
        let quote_reserve = u64::from_le_bytes(Self::get_bytes(&account.data[217..225])?);
        
        if base_reserve == 0 || quote_reserve == 0 {
            return Err(ArbitrageError::InsufficientLiquidity);
        }
        
        let price = Self::calculate_price(
            quote_reserve,
            base_reserve,
            market.tick_size.quote_decimals,
            market.tick_size.base_decimals,
        )?;
        
        Ok(PriceData {
            bid: price.saturating_sub(price * market.fee_bps as u64 / 20000),
            ask: price.saturating_add(price * market.fee_bps as u64 / 20000),
            last_slot: 0,
            timestamp: Instant::now(),
            base_liquidity: base_reserve,
            quote_liquidity: quote_reserve,
        })
    }

    async fn fetch_orca_price(&self, market: &MarketInfo) -> Result<PriceData> {
        let account = self.rpc.get_account(&market.id.0).await
            .map_err(|e| ArbitrageError::RpcError(e.to_string()))?;
        
        if account.data.len() < 265 {
            return Err(ArbitrageError::ParseError("Invalid Orca account data".to_string()));
        }
        
        let sqrt_price_bytes = Self::get_bytes(&account.data[229..245])?;
        let liquidity_bytes = Self::get_bytes(&account.data[245..261])?;
        
        let sqrt_price = u128::from_le_bytes(sqrt_price_bytes.try_into()
            .map_err(|_| ArbitrageError::ParseError("Invalid sqrt price".to_string()))?);
        let liquidity = u128::from_le_bytes(liquidity_bytes.try_into()
            .map_err(|_| ArbitrageError::ParseError("Invalid liquidity".to_string()))?);
        
        if liquidity == 0 {
            return Err(ArbitrageError::InsufficientLiquidity);
        }
        
        let price = (sqrt_price.saturating_mul(sqrt_price) >> 64) as u64;
        let adjusted_price = Self::align_to_tick(price, market.tick_size.min_tick);
        
        Ok(PriceData {
            bid: adjusted_price.saturating_sub(adjusted_price * market.fee_bps as u64 / 20000),
            ask: adjusted_price.saturating_add(adjusted_price * market.fee_bps as u64 / 20000),
            last_slot: 0,
            timestamp: Instant::now(),
            base_liquidity: (liquidity / 1_000_000) as u64,
            quote_liquidity: (liquidity / 1_000_000) as u64,
        })
    }

    async fn fetch_openbook_price(&self, market: &MarketInfo) -> Result<PriceData> {
        let account = self.rpc.get_account(&market.id.0).await
            .map_err(|e| ArbitrageError::RpcError(e.to_string()))?;
        
        if account.data.len() < 349 {
            return Err(ArbitrageError::ParseError("Invalid Openbook account data".to_string()));
        }
        
        let bids_addr = Self::parse_pubkey(&account.data[285..317])?;
        let asks_addr = Self::parse_pubkey(&account.data[317..349])?;
        
        let (bids_result, asks_result) = tokio::join!(
            self.rpc.get_account(&bids_addr),
            self.rpc.get_account(&asks_addr)
        );
        
        let bids_account = bids_result.map_err(|e| ArbitrageError::RpcError(e.to_string()))?;
        let asks_account = asks_result.map_err(|e| ArbitrageError::RpcError(e.to_string()))?;
        
        let best_bid = self.parse_orderbook_top(&bids_account, true)?;
        let best_ask = self.parse_orderbook_top(&asks_account, false)?;
        
        if best_bid == 0 || best_ask == 0 {
            return Err(ArbitrageError::InsufficientLiquidity);
        }
        
        let bid_price = Self::lot_to_price(best_bid, &market.tick_size);
        let ask_price = Self::lot_to_price(best_ask, &market.tick_size);
        
        Ok(PriceData {
            bid: bid_price,
            ask: ask_price,
            last_slot: 0,
            timestamp: Instant::now(),
            base_liquidity: MIN_LIQUIDITY_SOL,
            quote_liquidity: MIN_LIQUIDITY_SOL,
        })
    }

    fn parse_orderbook_top(&self, account: &Account, is_bids: bool) -> Result<u64> {
        if account.data.len() < 16 {
            return Ok(0);
        }
        
        let leaf_count = u32::from_le_bytes(Self::get_bytes(&account.data[8..12])?);
        if leaf_count == 0 {
            return Ok(0);
        }
        
        let header_size = 16;
        let node_size = 72;
        let offset = header_size + if is_bids { 0 } else { (leaf_count.saturating_sub(1) as usize) * node_size };
        
        if offset + 16 > account.data.len() {
            return Ok(0);
        }
        
        Ok(u64::from_le_bytes(Self::get_bytes(&account.data[offset + 8..offset + 16])?))
    }

    async fn find_best_opportunity(&self) -> Result<Option<ArbitrageOpportunity>> {
        let markets = self.markets.read().await;
        let prices = self.prices.read().await;
        
        let now = Instant::now();
        let mut best_opportunity: Option<ArbitrageOpportunity> = None;
        let mut best_profit_bps = 0u64;
        
        for (id1, market1) in markets.iter() {
            let price1 = match prices.get(id1) {
                Some(p) if now.duration_since(p.timestamp).as_millis() < STALE_PRICE_THRESHOLD_MS as u128 => p,
                _ => continue,
            };
            
            if price1.base_liquidity < MIN_LIQUIDITY_SOL || price1.quote_liquidity < MIN_LIQUIDITY_SOL {
                continue;
            }
            
            for (id2, market2) in markets.iter() {
                if id1 == id2 || market1.protocol == market2.protocol {
                    continue;
                }
                
                if market1.base_mint != market2.base_mint || market1.quote_mint != market2.quote_mint {
                    continue;
                }
                
                let price2 = match prices.get(id2) {
                    Some(p) if now.duration_since(p.timestamp).as_millis() < STALE_PRICE_THRESHOLD_MS as u128 => p,
                    _ => continue,
                };
                
                if price2.base_liquidity < MIN_LIQUIDITY_SOL || price2.quote_liquidity < MIN_LIQUIDITY_SOL {
                    continue;
                }
                
                let (spread_bps, buy_market, sell_market, buy_price, sell_price) = 
                    if price1.ask < price2.bid {
                        let spread = ((price2.bid - price1.ask) * 10000) / price1.ask;
                        (spread, *id1, *id2, price1.ask, price2.bid)
                    } else if price2.ask < price1.bid {
                        let spread = ((price1.bid - price2.ask) * 10000) / price2.ask;
                        (spread, *id2, *id1, price2.ask, price1.bid)
                    } else {
                        continue;
                    };
                
                let total_fee_bps = (market1.fee_bps + market2.fee_bps) as u64;
                let net_profit_bps = spread_bps.saturating_sub(total_fee_bps);
                
                if net_profit_bps > best_profit_bps && net_profit_bps > MIN_PROFIT_BPS {
                    let size = self.calculate_optimal_size(
                        &markets[&buy_market],
                        &markets[&sell_market],
                        &prices[&buy_market],
                        &prices[&sell_market],
                    ).await?;
                    
                    if size > 0 {
                        best_opportunity = Some(ArbitrageOpportunity {
                            buy_market,
                            sell_market,
                            size,
                            profit_bps: net_profit_bps,
                            buy_price,
                            sell_price,
                        });
                        best_profit_bps = net_profit_bps;
                    }
                }
            }
        }
        
        Ok(best_opportunity)
    }

    async fn calculate_optimal_size(
        &self,
        buy_market: &MarketInfo,
        sell_market: &MarketInfo,
        buy_price: &PriceData,
        sell_price: &PriceData,
    ) -> Result<u64> {
        let wallet_balance = self.rpc.get_balance(&self.payer.pubkey()).await
            .map_err(|e| ArbitrageError::RpcError(e.to_string()))?;
        
        let max_spend = wallet_balance.saturating_sub(10_000_000).saturating_div(2);
        
        let max_buy_size = max_spend
            .saturating_mul(10u64.pow(buy_market.tick_size.base_decimals as u32))
            .saturating_div(buy_price.ask);
        
        let max_sell_size = sell_price.base_liquidity.saturating_div(10);
        
        let optimal_size = max_buy_size.min(max_sell_size);
        
        let buy_aligned = Self::align_to_lot(optimal_size, buy_market.tick_size.base_lot_size);
        let sell_aligned = Self::align_to_lot(buy_aligned, sell_market.tick_size.base_lot_size);
        
        Ok(sell_aligned)
    }

    async fn execute_arbitrage(&self, opp: &ArbitrageOpportunity) -> Result<String> {
        let markets = self.markets.read().await;
        let buy_market = markets.get(&opp.buy_market)
            .ok_or_else(|| ArbitrageError::ExecutionFailed("Buy market not found".to_string()))?;
        let sell_market = markets.get(&opp.sell_market)
            .ok_or_else(|| ArbitrageError::ExecutionFailed("Sell market not found".to_string()))?;
        
        let blockhash = self.rpc.get_latest_blockhash().await
            .map_err(|e| ArbitrageError::RpcError(e.to_string()))?;
        
        let mut instructions = vec![
            ComputeBudgetInstruction::set_compute_unit_limit(COMPUTE_UNITS),
            ComputeBudgetInstruction::set_compute_unit_price(PRIORITY_FEE_MICROLAMPORTS),
        ];
        
        let buy_ix = self.build_swap_instruction(buy_market, opp.size, true).await?;
        let sell_ix = self.build_swap_instruction(sell_market, opp.size, false).await?;
        
        instructions.push(buy_ix);
        instructions.push(sell_ix);
        
        let tx = Transaction::new_signed_with_payer(
            &instructions,
            Some(&self.payer.pubkey()),
            &[&*self.payer],
            blockhash,
        );
        
        let signature = self.rpc.send_and_confirm_transaction(&tx).await
            .map_err(|e| ArbitrageError::ExecutionFailed(e.to_string()))?;
        
        Ok(signature.to_string())
    }

    async fn build_swap_instruction(
        &self,
        market: &MarketInfo,
        amount: u64,
        is_buy: bool,
    ) -> Result<Instruction> {
        let user_base = get_associated_token_address(&self.payer.pubkey(), &market.base_mint);
        let user_quote = get_associated_token_address(&self.payer.pubkey(), &market.quote_mint);
        
        let (accounts, data) = match market.protocol {
                        DexProtocol::Raydium => self.build_raydium_swap(market, amount, is_buy, user_base, user_quote)?,
            DexProtocol::Orca => self.build_orca_swap(market, amount, is_buy, user_base, user_quote)?,
            DexProtocol::Openbook => self.build_openbook_swap(market, amount, is_buy, user_base, user_quote)?,
        };
        
        Ok(Instruction {
            program_id: Self::get_program_id(market.protocol)?,
            accounts,
            data,
        })
    }

    fn build_raydium_swap(
        &self,
        market: &MarketInfo,
        amount: u64,
        is_buy: bool,
        user_base: Pubkey,
        user_quote: Pubkey,
    ) -> Result<(Vec<AccountMeta>, Vec<u8>)> {
        let accounts = vec![
            AccountMeta::new_readonly(spl_token::ID, false),
            AccountMeta::new(market.id.0, false),
            AccountMeta::new_readonly(market.authority.unwrap_or(market.id.0), false),
            AccountMeta::new(market.base_vault, false),
            AccountMeta::new(market.quote_vault, false),
            AccountMeta::new(if is_buy { user_quote } else { user_base }, false),
            AccountMeta::new(if is_buy { user_base } else { user_quote }, false),
            AccountMeta::new_readonly(self.payer.pubkey(), true),
        ];
        
        let mut data = vec![9u8];
        data.extend_from_slice(&amount.to_le_bytes());
        data.extend_from_slice(&Self::calculate_min_output(amount, MAX_SLIPPAGE_BPS).to_le_bytes());
        
        Ok((accounts, data))
    }

    fn build_orca_swap(
        &self,
        market: &MarketInfo,
        amount: u64,
        is_buy: bool,
        user_base: Pubkey,
        user_quote: Pubkey,
    ) -> Result<(Vec<AccountMeta>, Vec<u8>)> {
        let accounts = vec![
            AccountMeta::new_readonly(spl_token::ID, false),
            AccountMeta::new_readonly(self.payer.pubkey(), true),
            AccountMeta::new(market.id.0, false),
            AccountMeta::new(if is_buy { user_quote } else { user_base }, false),
            AccountMeta::new(market.base_vault, false),
            AccountMeta::new(if is_buy { user_base } else { user_quote }, false),
            AccountMeta::new(market.quote_vault, false),
            AccountMeta::new_readonly(sysvar::clock::ID, false),
        ];
        
        let mut data = vec![0x0bu8];
        data.extend_from_slice(&amount.to_le_bytes());
        data.extend_from_slice(&Self::calculate_min_output(amount, MAX_SLIPPAGE_BPS).to_le_bytes());
        data.extend_from_slice(&u128::MAX.to_le_bytes());
        data.push(1u8);
        data.push(if is_buy { 1u8 } else { 0u8 });
        
        Ok((accounts, data))
    }

    fn build_openbook_swap(
        &self,
        market: &MarketInfo,
        amount: u64,
        is_buy: bool,
        user_base: Pubkey,
        user_quote: Pubkey,
    ) -> Result<(Vec<AccountMeta>, Vec<u8>)> {
        let accounts = vec![
            AccountMeta::new(market.id.0, false),
            AccountMeta::new_readonly(self.payer.pubkey(), true),
            AccountMeta::new(if is_buy { user_quote } else { user_base }, false),
            AccountMeta::new(if is_buy { user_base } else { user_quote }, false),
            AccountMeta::new(market.base_vault, false),
            AccountMeta::new(market.quote_vault, false),
            AccountMeta::new_readonly(spl_token::ID, false),
            AccountMeta::new_readonly(sysvar::rent::ID, false),
        ];
        
        let mut data = vec![10u8];
        data.push(if is_buy { 0u8 } else { 1u8 });
        data.extend_from_slice(&u64::MAX.to_le_bytes());
        data.extend_from_slice(&amount.to_le_bytes());
        data.extend_from_slice(&(amount * 2).to_le_bytes());
        data.push(0u8);
        data.push(2u8);
        data.extend_from_slice(&self.last_slot.load(Ordering::Relaxed).to_le_bytes());
        data.extend_from_slice(&65535u16.to_le_bytes());
        
        Ok((accounts, data))
    }

    fn get_program_id(protocol: DexProtocol) -> Result<Pubkey> {
        let id_str = match protocol {
            DexProtocol::Raydium => RAYDIUM_AMM_V4,
            DexProtocol::Orca => ORCA_WHIRLPOOL,
            DexProtocol::Openbook => OPENBOOK_V3,
        };
        
        Pubkey::from_str(id_str).map_err(|e| ArbitrageError::ParseError(e.to_string()))
    }

    fn parse_pubkey(data: &[u8]) -> Result<Pubkey> {
        if data.len() != 32 {
            return Err(ArbitrageError::ParseError("Invalid pubkey length".to_string()));
        }
        
        let mut bytes = [0u8; 32];
        bytes.copy_from_slice(data);
        Ok(Pubkey::from(bytes))
    }

    fn get_bytes(data: &[u8]) -> Result<[u8; 8]> {
        if data.len() != 8 {
            return Err(ArbitrageError::ParseError("Invalid data length for u64".to_string()));
        }
        
        let mut bytes = [0u8; 8];
        bytes.copy_from_slice(data);
        Ok(bytes)
    }

    fn calculate_price(
        quote_amount: u64,
        base_amount: u64,
        quote_decimals: u8,
        base_decimals: u8,
    ) -> Result<u64> {
        if base_amount == 0 {
            return Err(ArbitrageError::ParseError("Division by zero".to_string()));
        }
        
        let price = (quote_amount as u128)
            .saturating_mul(10u128.pow(base_decimals as u32))
            .checked_div(base_amount as u128)
            .ok_or_else(|| ArbitrageError::ParseError("Price calculation overflow".to_string()))?
            .checked_div(10u128.pow(quote_decimals as u32))
            .ok_or_else(|| ArbitrageError::ParseError("Price decimal adjustment overflow".to_string()))?;
        
        Ok(price as u64)
    }

    fn align_to_tick(price: u64, tick_size: u64) -> u64 {
        if tick_size <= 1 {
            return price;
        }
        (price / tick_size) * tick_size
    }

    fn align_to_lot(amount: u64, lot_size: u64) -> u64 {
        if lot_size == 0 {
            return 0;
        }
        (amount / lot_size) * lot_size
    }

    fn lot_to_price(lot_price: u64, tick_size: &TickSize) -> u64 {
        lot_price
            .saturating_mul(tick_size.quote_lot_size)
            .saturating_div(tick_size.base_lot_size)
    }

    fn calculate_min_output(input: u64, slippage_bps: u64) -> u64 {
        input.saturating_sub(input.saturating_mul(slippage_bps).saturating_div(10000))
    }
}

impl Clone for TickSizeArbitrageur {
    fn clone(&self) -> Self {
        Self {
            rpc: Arc::clone(&self.rpc),
            payer: Arc::clone(&self.payer),
            markets: Arc::clone(&self.markets),
            prices: Arc::clone(&self.prices),
            executing: Arc::clone(&self.executing),
            last_slot: Arc::clone(&self.last_slot),
        }
    }
}

pub fn calculate_profit_after_fees(
    buy_price: u64,
    sell_price: u64,
    amount: u64,
    buy_fee_bps: u16,
    sell_fee_bps: u16,
) -> i64 {
    let buy_cost = (amount as u128)
        .saturating_mul(buy_price as u128)
        .saturating_div(1_000_000_000u128) as u64;
    let buy_fee = buy_cost.saturating_mul(buy_fee_bps as u64).saturating_div(10_000);
    let total_cost = buy_cost.saturating_add(buy_fee);
    
    let sell_revenue = (amount as u128)
        .saturating_mul(sell_price as u128)
        .saturating_div(1_000_000_000u128) as u64;
    let sell_fee = sell_revenue.saturating_mul(sell_fee_bps as u64).saturating_div(10_000);
    let net_revenue = sell_revenue.saturating_sub(sell_fee);
    
    net_revenue as i64 - total_cost as i64
}

pub fn calculate_price_impact(
    amount: u64,
    liquidity: u64,
    fee_bps: u16,
) -> u64 {
    if liquidity == 0 {
        return u64::MAX;
    }
    
    let impact_bps = amount.saturating_mul(10_000).saturating_div(liquidity);
    impact_bps.saturating_add(fee_bps as u64)
}

pub async fn create_arbitrageur(
    rpc_url: &str,
    keypair_path: &str,
) -> Result<TickSizeArbitrageur> {
    let keypair_bytes = std::fs::read(keypair_path)
        .map_err(|e| ArbitrageError::ParseError(format!("Failed to read keypair: {}", e)))?;
    let payer = Keypair::from_bytes(&keypair_bytes)
        .map_err(|e| ArbitrageError::ParseError(format!("Invalid keypair: {}", e)))?;
    Ok(TickSizeArbitrageur::new(rpc_url, payer))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_calculate_profit() {
        let buy_price = 1_000_000_000;
        let sell_price = 1_010_000_000;
        let amount = 1_000_000_000;
        let profit = calculate_profit_after_fees(buy_price, sell_price, amount, 25, 25);
        assert!(profit > 0);
    }

    #[test]
    fn test_align_to_lot() {
        assert_eq!(TickSizeArbitrageur::align_to_lot(12345, 1000), 12000);
        assert_eq!(TickSizeArbitrageur::align_to_lot(999, 1000), 0);
        assert_eq!(TickSizeArbitrageur::align_to_lot(2000, 1000), 2000);
    }

    #[test]
    fn test_calculate_min_output() {
        assert_eq!(TickSizeArbitrageur::calculate_min_output(10000, 300), 9700);
        assert_eq!(TickSizeArbitrageur::calculate_min_output(10000, 0), 10000);
    }

    #[test]
    fn test_price_impact() {
        let impact = calculate_price_impact(1_000_000, 100_000_000, 25);
        assert_eq!(impact, 125);
    }
}

