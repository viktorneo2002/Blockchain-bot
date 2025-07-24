use anyhow::{anyhow, Result};
use borsh::{BorshDeserialize, BorshSerialize};
use dashmap::DashMap;
use mango::state::{MangoAccount, PerpMarket};
use phoenix::program::MarketHeader;
use rayon::prelude::*;
use serum_dex::state::{gen_vault_signer_key, Market, MarketState};
use solana_account_decoder::UiAccountEncoding;
use solana_client::{
    nonblocking::rpc_client::RpcClient,
    rpc_config::{RpcAccountInfoConfig, RpcProgramAccountsConfig},
    rpc_filter::{Memcmp, MemcmpEncodedBytes, RpcFilterType},
};
use solana_program::{
    clock::Slot,
    program_pack::Pack,
    pubkey::Pubkey,
};
use solana_sdk::{
    account::Account,
    commitment_config::CommitmentConfig,
    signature::Keypair,
};
use std::{
    collections::{HashMap, VecDeque},
    str::FromStr,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};
use tokio::{
    sync::{mpsc, RwLock},
    time::interval,
};

const SERUM_DEX_V3: &str = "9xQeWvG816bUx9EPjHmaT23yvVM2ZWbrrpZb9PusVFin";
const PHOENIX_V1: &str = "PhoeNiXZ8ByJGLkxNfZRnkUfjvmuYqLR89jjFHGqdXY";
const MANGO_V4: &str = "4MangoMjqJ2firMokCjjGgoK8d4MXcrgL7XJaL3w6fVg";
const MIN_LIQUIDITY_USD: f64 = 1000.0;
const MAX_SPREAD_BPS: u16 = 100;
const MIN_DEPTH_LEVELS: usize = 5;
const OPPORTUNITY_THRESHOLD_BPS: u16 = 10;
const MAX_SLIPPAGE_BPS: u16 = 50;
const ORDERBOOK_REFRESH_MS: u64 = 100;
const CACHE_TTL_MS: u64 = 500;

#[derive(Debug, Clone)]
pub struct OrderbookSnapshot {
    pub market: Pubkey,
    pub protocol: DexProtocol,
    pub bids: Vec<Order>,
    pub asks: Vec<Order>,
    pub timestamp: u64,
    pub slot: Slot,
    pub spread_bps: u16,
    pub mid_price: f64,
    pub bid_liquidity_usd: f64,
    pub ask_liquidity_usd: f64,
    pub imbalance_ratio: f64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DexProtocol {
    SerumV3,
    Phoenix,
    MangoV4,
}

#[derive(Debug, Clone)]
pub struct Order {
    pub price: f64,
    pub size: f64,
    pub owner: Option<Pubkey>,
}

#[derive(Debug, Clone)]
pub struct LiquidityOpportunity {
    pub market: Pubkey,
    pub protocol: DexProtocol,
    pub opportunity_type: OpportunityType,
    pub entry_price: f64,
    pub exit_price: f64,
    pub size: f64,
    pub profit_bps: u16,
    pub confidence: f64,
    pub expires_at: u64,
}

#[derive(Debug, Clone)]
pub enum OpportunityType {
    SpreadCapture,
    LiquidityImbalance,
    LargePriceMovement,
    CrossDexArbitrage,
}

pub struct OrderbookLiquidityDetector {
    rpc: Arc<RpcClient>,
    markets: Arc<DashMap<Pubkey, MarketInfo>>,
    orderbooks: Arc<DashMap<Pubkey, OrderbookSnapshot>>,
    opportunities: Arc<RwLock<VecDeque<LiquidityOpportunity>>>,
    price_feeds: Arc<DashMap<Pubkey, f64>>,
    running: Arc<AtomicBool>,
    last_update: Arc<AtomicU64>,
}

#[derive(Clone)]
struct MarketInfo {
    pubkey: Pubkey,
    protocol: DexProtocol,
    base_mint: Pubkey,
    quote_mint: Pubkey,
    base_decimals: u8,
    quote_decimals: u8,
    tick_size: f64,
    lot_size: f64,
}

impl OrderbookLiquidityDetector {
    pub async fn new(rpc_url: &str) -> Result<Self> {
        let rpc = Arc::new(RpcClient::new_with_commitment(
            rpc_url.to_string(),
            CommitmentConfig::processed(),
        ));

        Ok(Self {
            rpc,
            markets: Arc::new(DashMap::new()),
            orderbooks: Arc::new(DashMap::new()),
            opportunities: Arc::new(RwLock::new(VecDeque::with_capacity(1000))),
            price_feeds: Arc::new(DashMap::new()),
            running: Arc::new(AtomicBool::new(false)),
            last_update: Arc::new(AtomicU64::new(0)),
        })
    }

    pub async fn start(&self) -> Result<()> {
        self.running.store(true, Ordering::Release);
        
        let markets = self.discover_markets().await?;
        for market in markets {
            self.markets.insert(market.pubkey, market);
        }

        let (tx, mut rx) = mpsc::channel::<OrderbookUpdate>(1000);
        
        tokio::spawn(self.clone().orderbook_updater(tx.clone()));
        tokio::spawn(self.clone().opportunity_scanner());
        tokio::spawn(self.clone().price_feed_updater());
        tokio::spawn(self.clone().cache_cleaner());

        while let Some(update) = rx.recv().await {
            self.process_orderbook_update(update).await?;
        }

        Ok(())
    }

    async fn discover_markets(&self) -> Result<Vec<MarketInfo>> {
        let mut markets = Vec::new();

        // Discover Serum markets
        let serum_markets = self.discover_serum_markets().await?;
        markets.extend(serum_markets);

        // Discover Phoenix markets
        let phoenix_markets = self.discover_phoenix_markets().await?;
        markets.extend(phoenix_markets);

        // Discover Mango markets
        let mango_markets = self.discover_mango_markets().await?;
        markets.extend(mango_markets);

        Ok(markets)
    }

    async fn discover_serum_markets(&self) -> Result<Vec<MarketInfo>> {
        let program_id = Pubkey::from_str(SERUM_DEX_V3)?;
        let config = RpcProgramAccountsConfig {
            filters: Some(vec![RpcFilterType::DataSize(
                std::mem::size_of::<MarketState>() as u64
            )]),
            account_config: RpcAccountInfoConfig {
                encoding: Some(UiAccountEncoding::Base64),
                commitment: Some(CommitmentConfig::confirmed()),
                ..Default::default()
            },
            ..Default::default()
        };

        let accounts = self.rpc.get_program_accounts_with_config(&program_id, config).await?;
        let mut markets = Vec::new();

        for (pubkey, account) in accounts {
            if let Ok(market) = Market::load(&account, &program_id, false) {
                markets.push(MarketInfo {
                    pubkey,
                    protocol: DexProtocol::SerumV3,
                    base_mint: market.coin_mint,
                    quote_mint: market.pc_mint,
                    base_decimals: market.coin_decimals,
                    quote_decimals: market.pc_decimals,
                    tick_size: market.pc_lot_size as f64 / 10f64.powi(market.pc_decimals as i32),
                    lot_size: market.coin_lot_size as f64 / 10f64.powi(market.coin_decimals as i32),
                });
            }
        }

        Ok(markets)
    }

    async fn discover_phoenix_markets(&self) -> Result<Vec<MarketInfo>> {
        let program_id = Pubkey::from_str(PHOENIX_V1)?;
        let config = RpcProgramAccountsConfig {
            filters: Some(vec![RpcFilterType::Memcmp(Memcmp {
                offset: 0,
                bytes: MemcmpEncodedBytes::Bytes(vec![1]), // Phoenix market discriminator
                encoding: None,
            })]),
            account_config: RpcAccountInfoConfig {
                encoding: Some(UiAccountEncoding::Base64),
                commitment: Some(CommitmentConfig::confirmed()),
                ..Default::default()
            },
            ..Default::default()
        };

        let accounts = self.rpc.get_program_accounts_with_config(&program_id, config).await?;
        let mut markets = Vec::new();

        for (pubkey, account) in accounts {
            if account.data.len() >= std::mem::size_of::<MarketHeader>() {
                let header = MarketHeader::load(&account.data)?;
                markets.push(MarketInfo {
                    pubkey,
                    protocol: DexProtocol::Phoenix,
                    base_mint: header.base_params.mint_key,
                    quote_mint: header.quote_params.mint_key,
                    base_decimals: header.base_params.decimals,
                    quote_decimals: header.quote_params.decimals,
                    tick_size: header.get_tick_size_in_quote_atoms_per_base_unit() as f64,
                    lot_size: header.get_base_lot_size() as f64,
                });
            }
        }

        Ok(markets)
    }

    async fn discover_mango_markets(&self) -> Result<Vec<MarketInfo>> {
        let program_id = Pubkey::from_str(MANGO_V4)?;
        let accounts = self.rpc.get_program_accounts(&program_id).await?;
        let mut markets = Vec::new();

        for (pubkey, account) in accounts {
            if account.data.len() >= 8 && &account.data[0..8] == b"PerpMkt\0" {
                if let Ok(perp_market) = PerpMarket::try_from_slice(&account.data[8..]) {
                    markets.push(MarketInfo {
                        pubkey,
                        protocol: DexProtocol::MangoV4,
                        base_mint: perp_market.base_mint,
                        quote_mint: perp_market.quote_mint,
                        base_decimals: perp_market.base_decimals,
                        quote_decimals: perp_market.quote_decimals,
                        tick_size: perp_market.quote_lot_size as f64 / 10f64.powi(perp_market.quote_decimals as i32),
                        lot_size: perp_market.base_lot_size as f64 / 10f64.powi(perp_market.base_decimals as i32),
                    });
                }
            }
        }

        Ok(markets)
    }

    async fn orderbook_updater(self: Arc<Self>, tx: mpsc::Sender<OrderbookUpdate>) {
        let mut interval = interval(Duration::from_millis(ORDERBOOK_REFRESH_MS));
        
        while self.running.load(Ordering::Acquire) {
            interval.tick().await;
            
            let markets: Vec<MarketInfo> = self.markets.iter()
                .map(|entry| entry.value().clone())
                .collect();

            let updates = markets.par_iter()
                .filter_map(|market| {
                    match self.fetch_orderbook(market) {
                        Ok(snapshot) => Some(OrderbookUpdate {
                            market: market.pubkey,
                            snapshot,
                        }),
                        Err(_) => None,
                    }
                })
                .collect::<Vec<_>>();

            for update in updates {
                let _ = tx.send(update).await;
            }
        }
    }

    fn fetch_orderbook(&self, market: &MarketInfo) -> Result<OrderbookSnapshot> {
        let rt = tokio::runtime::Handle::current();
        let snapshot = rt.block_on(async {
            match market.protocol {
                DexProtocol::SerumV3 => self.fetch_serum_orderbook(market).await,
                DexProtocol::Phoenix => self.fetch_phoenix_orderbook(market).await,
                DexProtocol::MangoV4 => self.fetch_mango_orderbook(market).await,
            }
        })?;

        Ok(snapshot)
    }

    async fn fetch_serum_orderbook(&self, market_info: &MarketInfo) -> Result<OrderbookSnapshot> {
        let account = self.rpc.get_account(&market_info.pubkey).await?;
        let program_id = Pubkey::from_str(SERUM_DEX_V3)?;
        let market = Market::load(&account, &program_id, false)?;
        
        let bids_account = self.rpc.get_account(&market.bids).await?;
        let asks_account = self.rpc.get_account(&market.asks).await?;
        
        let mut bids = Vec::new();
        let mut asks = Vec::new();
        
        let bid_slab = market.load_bids_mut(&bids_account.data)?;
        for order in bid_slab.iter() {
            bids.push(Order {
                price: order.price() as f64 * market_info.tick_size,
                size: order.quantity() as f64 * market_info.lot_size,
                owner: Some(order.owner()),
            });
        }

                let ask_slab = market.load_asks_mut(&asks_account.data)?;
        for order in ask_slab.iter() {
            asks.push(Order {
                price: order.price() as f64 * market_info.tick_size,
                size: order.quantity() as f64 * market_info.lot_size,
                owner: Some(order.owner()),
            });
        }

        let (spread_bps, mid_price) = self.calculate_spread_and_mid(&bids, &asks);
        let bid_liquidity_usd = self.calculate_liquidity_usd(&bids, mid_price);
        let ask_liquidity_usd = self.calculate_liquidity_usd(&asks, mid_price);
        let imbalance_ratio = if bid_liquidity_usd > 0.0 {
            ask_liquidity_usd / bid_liquidity_usd
        } else {
            0.0
        };

        Ok(OrderbookSnapshot {
            market: market_info.pubkey,
            protocol: market_info.protocol,
            bids,
            asks,
            timestamp: chrono::Utc::now().timestamp_millis() as u64,
            slot: self.rpc.get_slot().await.unwrap_or(0),
            spread_bps,
            mid_price,
            bid_liquidity_usd,
            ask_liquidity_usd,
            imbalance_ratio,
        })
    }

    async fn fetch_phoenix_orderbook(&self, market_info: &MarketInfo) -> Result<OrderbookSnapshot> {
        let account = self.rpc.get_account(&market_info.pubkey).await?;
        let market_data = &account.data;
        
        let header = MarketHeader::load(market_data)?;
        let base_lot_size = header.get_base_lot_size();
        let quote_lot_size = header.get_quote_lot_size();
        
        let mut bids = Vec::new();
        let mut asks = Vec::new();

        let ladder_offset = std::mem::size_of::<MarketHeader>();
        if market_data.len() > ladder_offset {
            let ladder_data = &market_data[ladder_offset..];
            
            // Parse Phoenix orderbook ladder structure
            let mut offset = 0;
            while offset + 32 <= ladder_data.len() {
                let price_in_ticks = u64::from_le_bytes(ladder_data[offset..offset+8].try_into()?);
                let size_in_base_lots = u64::from_le_bytes(ladder_data[offset+8..offset+16].try_into()?);
                let side = ladder_data[offset+16];
                
                if price_in_ticks > 0 && size_in_base_lots > 0 {
                    let price = (price_in_ticks as f64 * quote_lot_size as f64) / 
                               (base_lot_size as f64 * 10f64.powi(market_info.quote_decimals as i32 - market_info.base_decimals as i32));
                    let size = size_in_base_lots as f64 * base_lot_size as f64 / 10f64.powi(market_info.base_decimals as i32);
                    
                    let order = Order {
                        price,
                        size,
                        owner: None,
                    };
                    
                    if side == 0 {
                        bids.push(order);
                    } else {
                        asks.push(order);
                    }
                }
                offset += 32;
            }
        }

        bids.sort_by(|a, b| b.price.partial_cmp(&a.price).unwrap());
        asks.sort_by(|a, b| a.price.partial_cmp(&b.price).unwrap());

        let (spread_bps, mid_price) = self.calculate_spread_and_mid(&bids, &asks);
        let bid_liquidity_usd = self.calculate_liquidity_usd(&bids, mid_price);
        let ask_liquidity_usd = self.calculate_liquidity_usd(&asks, mid_price);
        let imbalance_ratio = if bid_liquidity_usd > 0.0 {
            ask_liquidity_usd / bid_liquidity_usd
        } else {
            0.0
        };

        Ok(OrderbookSnapshot {
            market: market_info.pubkey,
            protocol: market_info.protocol,
            bids,
            asks,
            timestamp: chrono::Utc::now().timestamp_millis() as u64,
            slot: self.rpc.get_slot().await.unwrap_or(0),
            spread_bps,
            mid_price,
            bid_liquidity_usd,
            ask_liquidity_usd,
            imbalance_ratio,
        })
    }

    async fn fetch_mango_orderbook(&self, market_info: &MarketInfo) -> Result<OrderbookSnapshot> {
        let account = self.rpc.get_account(&market_info.pubkey).await?;
        let perp_market = PerpMarket::try_from_slice(&account.data[8..])?;
        
        let bids_account = self.rpc.get_account(&perp_market.bids).await?;
        let asks_account = self.rpc.get_account(&perp_market.asks).await?;
        
        let mut bids = Vec::new();
        let mut asks = Vec::new();

        // Parse Mango orderbook nodes
        let bid_nodes = self.parse_mango_book_side(&bids_account.data, true);
        for (price, size) in bid_nodes {
            bids.push(Order {
                price: price as f64 / 10f64.powi(perp_market.quote_decimals as i32 - perp_market.base_decimals as i32),
                size: size as f64 / 10f64.powi(perp_market.base_decimals as i32),
                owner: None,
            });
        }

        let ask_nodes = self.parse_mango_book_side(&asks_account.data, false);
        for (price, size) in ask_nodes {
            asks.push(Order {
                price: price as f64 / 10f64.powi(perp_market.quote_decimals as i32 - perp_market.base_decimals as i32),
                size: size as f64 / 10f64.powi(perp_market.base_decimals as i32),
                owner: None,
            });
        }

        let (spread_bps, mid_price) = self.calculate_spread_and_mid(&bids, &asks);
        let bid_liquidity_usd = self.calculate_liquidity_usd(&bids, mid_price);
        let ask_liquidity_usd = self.calculate_liquidity_usd(&asks, mid_price);
        let imbalance_ratio = if bid_liquidity_usd > 0.0 {
            ask_liquidity_usd / bid_liquidity_usd
        } else {
            0.0
        };

        Ok(OrderbookSnapshot {
            market: market_info.pubkey,
            protocol: market_info.protocol,
            bids,
            asks,
            timestamp: chrono::Utc::now().timestamp_millis() as u64,
            slot: self.rpc.get_slot().await.unwrap_or(0),
            spread_bps,
            mid_price,
            bid_liquidity_usd,
            ask_liquidity_usd,
            imbalance_ratio,
        })
    }

    fn parse_mango_book_side(&self, data: &[u8], is_bids: bool) -> Vec<(i64, i64)> {
        let mut orders = Vec::new();
        let node_size = 88; // Mango BookSideNode size
        
        for chunk in data.chunks_exact(node_size) {
            if chunk.len() >= 24 {
                let price = i64::from_le_bytes(chunk[0..8].try_into().unwrap_or([0u8; 8]));
                let quantity = i64::from_le_bytes(chunk[8..16].try_into().unwrap_or([0u8; 8]));
                
                if price > 0 && quantity > 0 {
                    orders.push((price, quantity));
                }
            }
        }
        
        if is_bids {
            orders.sort_by(|a, b| b.0.cmp(&a.0));
        } else {
            orders.sort_by(|a, b| a.0.cmp(&b.0));
        }
        
        orders
    }

    fn calculate_spread_and_mid(&self, bids: &[Order], asks: &[Order]) -> (u16, f64) {
        if bids.is_empty() || asks.is_empty() {
            return (u16::MAX, 0.0);
        }

        let best_bid = bids[0].price;
        let best_ask = asks[0].price;
        let mid_price = (best_bid + best_ask) / 2.0;
        let spread = best_ask - best_bid;
        let spread_bps = ((spread / mid_price) * 10000.0) as u16;

        (spread_bps, mid_price)
    }

    fn calculate_liquidity_usd(&self, orders: &[Order], reference_price: f64) -> f64 {
        orders.iter()
            .take(MIN_DEPTH_LEVELS)
            .map(|order| order.size * order.price)
            .sum()
    }

    async fn process_orderbook_update(&self, update: OrderbookUpdate) -> Result<()> {
        let old_snapshot = self.orderbooks.get(&update.market);
        self.orderbooks.insert(update.market, update.snapshot.clone());
        self.last_update.store(chrono::Utc::now().timestamp_millis() as u64, Ordering::Release);

        if let Some(old) = old_snapshot {
            self.detect_opportunities(&old, &update.snapshot).await?;
        }

        Ok(())
    }

    async fn detect_opportunities(&self, old: &OrderbookSnapshot, new: &OrderbookSnapshot) -> Result<()> {
        let mut opportunities = Vec::new();

        // Spread capture opportunity
        if new.spread_bps < MAX_SPREAD_BPS && new.spread_bps > OPPORTUNITY_THRESHOLD_BPS {
            if new.bid_liquidity_usd > MIN_LIQUIDITY_USD && new.ask_liquidity_usd > MIN_LIQUIDITY_USD {
                opportunities.push(LiquidityOpportunity {
                    market: new.market,
                    protocol: new.protocol,
                    opportunity_type: OpportunityType::SpreadCapture,
                    entry_price: new.bids[0].price,
                    exit_price: new.asks[0].price,
                    size: new.bids[0].size.min(new.asks[0].size),
                    profit_bps: new.spread_bps.saturating_sub(MAX_SLIPPAGE_BPS),
                    confidence: self.calculate_confidence(new),
                    expires_at: chrono::Utc::now().timestamp_millis() as u64 + 5000,
                });
            }
        }

        // Liquidity imbalance opportunity
        if new.imbalance_ratio < 0.5 || new.imbalance_ratio > 2.0 {
            let price_movement = ((new.mid_price - old.mid_price) / old.mid_price).abs() * 10000.0;
            if price_movement as u16 > OPPORTUNITY_THRESHOLD_BPS {
                opportunities.push(LiquidityOpportunity {
                    market: new.market,
                    protocol: new.protocol,
                    opportunity_type: OpportunityType::LiquidityImbalance,
                    entry_price: new.mid_price,
                    exit_price: if new.imbalance_ratio < 1.0 { 
                        new.mid_price * 1.002 
                    } else { 
                        new.mid_price * 0.998 
                    },
                    size: new.bid_liquidity_usd.min(new.ask_liquidity_usd) / new.mid_price,
                    profit_bps: (price_movement as u16).saturating_sub(MAX_SLIPPAGE_BPS),
                    confidence: self.calculate_confidence(new),
                    expires_at: chrono::Utc::now().timestamp_millis() as u64 + 3000,
                });
            }
        }

        // Large price movement opportunity
        let price_change = ((new.mid_price - old.mid_price) / old.mid_price).abs();
        if price_change > 0.005 {
            opportunities.push(LiquidityOpportunity {
                market: new.market,
                protocol: new.protocol,
                opportunity_type: OpportunityType::LargePriceMovement,
                entry_price: new.mid_price,
                exit_price: old.mid_price,
                size: new.bid_liquidity_usd.min(new.ask_liquidity_usd) / new.mid_price * 0.1,
                profit_bps: (price_change * 10000.0) as u16,
                confidence: self.calculate_confidence(new),
                expires_at: chrono::Utc::now().timestamp_millis() as u64 + 2000,
            });
        }

        if !opportunities.is_empty() {
            let mut opps = self.opportunities.write().await;
            for opp in opportunities {
                if opps.len() >= 1000 {
                    opps.pop_front();
                }
                opps.push_back(opp);
            }
        }

        Ok(())
    }

    fn calculate_confidence(&self, snapshot: &OrderbookSnapshot) -> f64 {
        let mut confidence = 1.0;

        // Spread quality
        if snapshot.spread_bps > MAX_SPREAD_BPS {
            confidence *= 0.5;
        }

        // Liquidity depth
        let total_liquidity = snapshot.bid_liquidity_usd + snapshot.ask_liquidity_usd;
        if total_liquidity < MIN_LIQUIDITY_USD * 2.0 {
            confidence *= 0.7;
        }

        // Order count
        let order_count = snapshot.bids.len() + snapshot.asks.len();
        if order_count < MIN_DEPTH_LEVELS * 2 {
            confidence *= 0.8;
        }

        // Imbalance
        if snapshot.imbalance_ratio < 0.2 || snapshot.imbalance_ratio > 5.0 {
            confidence *= 0.6;
        }

        confidence.max(0.1).min(1.0)
    }

    async fn opportunity_scanner(self: Arc<Self>) {
        let mut interval = interval(Duration::from_millis(100));
        
                while self.running.load(Ordering::Acquire) {
            interval.tick().await;
            
            let now = chrono::Utc::now().timestamp_millis() as u64;
            let mut opportunities = self.opportunities.write().await;
            
            // Remove expired opportunities
            opportunities.retain(|opp| opp.expires_at > now);
            
            // Cross-DEX arbitrage detection
            let markets: Vec<(Pubkey, MarketInfo)> = self.markets.iter()
                .map(|entry| (*entry.key(), entry.value().clone()))
                .collect();
            
            for i in 0..markets.len() {
                for j in i+1..markets.len() {
                    if markets[i].1.base_mint == markets[j].1.base_mint && 
                       markets[i].1.quote_mint == markets[j].1.quote_mint {
                        
                        if let (Some(ob1), Some(ob2)) = (
                            self.orderbooks.get(&markets[i].0),
                            self.orderbooks.get(&markets[j].0)
                        ) {
                            if let Some(arb) = self.detect_cross_dex_arbitrage(&ob1, &ob2) {
                                if opportunities.len() >= 1000 {
                                    opportunities.pop_front();
                                }
                                opportunities.push_back(arb);
                            }
                        }
                    }
                }
            }
        }
    }

    fn detect_cross_dex_arbitrage(&self, ob1: &OrderbookSnapshot, ob2: &OrderbookSnapshot) -> Option<LiquidityOpportunity> {
        if ob1.bids.is_empty() || ob1.asks.is_empty() || ob2.bids.is_empty() || ob2.asks.is_empty() {
            return None;
        }

        let buy_price_1 = ob1.asks[0].price;
        let sell_price_2 = ob2.bids[0].price;
        
        let buy_price_2 = ob2.asks[0].price;
        let sell_price_1 = ob1.bids[0].price;
        
        let profit_1_to_2 = (sell_price_2 - buy_price_1) / buy_price_1;
        let profit_2_to_1 = (sell_price_1 - buy_price_2) / buy_price_2;
        
        let (entry_price, exit_price, size, market, profit_ratio) = 
            if profit_1_to_2 > profit_2_to_1 && profit_1_to_2 > 0.001 {
                (
                    buy_price_1,
                    sell_price_2,
                    ob1.asks[0].size.min(ob2.bids[0].size),
                    ob1.market,
                    profit_1_to_2
                )
            } else if profit_2_to_1 > 0.001 {
                (
                    buy_price_2,
                    sell_price_1,
                    ob2.asks[0].size.min(ob1.bids[0].size),
                    ob2.market,
                    profit_2_to_1
                )
            } else {
                return None;
            };

        let profit_bps = (profit_ratio * 10000.0) as u16;
        if profit_bps <= OPPORTUNITY_THRESHOLD_BPS {
            return None;
        }

        Some(LiquidityOpportunity {
            market,
            protocol: ob1.protocol,
            opportunity_type: OpportunityType::CrossDexArbitrage,
            entry_price,
            exit_price,
            size,
            profit_bps: profit_bps.saturating_sub(MAX_SLIPPAGE_BPS),
            confidence: (self.calculate_confidence(ob1) + self.calculate_confidence(ob2)) / 2.0,
            expires_at: chrono::Utc::now().timestamp_millis() as u64 + 1000,
        })
    }

    async fn price_feed_updater(self: Arc<Self>) {
        let mut interval = interval(Duration::from_secs(5));
        let pyth_program = Pubkey::from_str("FsJ3A3u2vn5cTVofAjvy6y5kwABJAqYWpe4975bi2epH").unwrap();
        
        while self.running.load(Ordering::Acquire) {
            interval.tick().await;
            
            let price_accounts = vec![
                // SOL/USD
                Pubkey::from_str("H6ARHf6YXhGYeQfUzQNGk6rDNnLBQKrenN712K4AQJEG").unwrap(),
                // BTC/USD
                Pubkey::from_str("GVXRSBjFk6e6J3NbVPXohDJetcTjaeeuykUpbQF8UoMU").unwrap(),
                // ETH/USD
                Pubkey::from_str("JBu1AL4obBcCMqKBBxhpWCNUt136ijcuMZLFvTP7iWdB").unwrap(),
                // USDC/USD
                Pubkey::from_str("Gnt27xtC473ZT2Mw5u8wZ68Z3gULkSTb5DuxJy7eJotD").unwrap(),
            ];
            
            for price_account in price_accounts {
                if let Ok(account) = self.rpc.get_account(&price_account).await {
                    if account.data.len() >= 48 {
                        let price_data = &account.data[32..48];
                        let price = i64::from_le_bytes(price_data[0..8].try_into().unwrap_or([0u8; 8]));
                        let expo = i32::from_le_bytes(price_data[8..12].try_into().unwrap_or([0u8; 4]));
                        
                        if price > 0 {
                            let normalized_price = price as f64 * 10f64.powi(expo);
                            self.price_feeds.insert(price_account, normalized_price);
                        }
                    }
                }
            }
        }
    }

    async fn cache_cleaner(self: Arc<Self>) {
        let mut interval = interval(Duration::from_millis(CACHE_TTL_MS));
        
        while self.running.load(Ordering::Acquire) {
            interval.tick().await;
            
            let now = chrono::Utc::now().timestamp_millis() as u64;
            let stale_threshold = now - CACHE_TTL_MS;
            
            // Clean stale orderbooks
            let stale_markets: Vec<Pubkey> = self.orderbooks.iter()
                .filter(|entry| entry.value().timestamp < stale_threshold)
                .map(|entry| *entry.key())
                .collect();
            
            for market in stale_markets {
                self.orderbooks.remove(&market);
            }
            
            // Clean expired opportunities
            let mut opportunities = self.opportunities.write().await;
            opportunities.retain(|opp| opp.expires_at > now);
        }
    }

    pub async fn get_opportunities(&self, limit: usize) -> Vec<LiquidityOpportunity> {
        let opportunities = self.opportunities.read().await;
        opportunities.iter()
            .rev()
            .take(limit)
            .cloned()
            .collect()
    }

    pub async fn get_orderbook(&self, market: &Pubkey) -> Option<OrderbookSnapshot> {
        self.orderbooks.get(market).map(|entry| entry.value().clone())
    }

    pub fn get_markets(&self) -> Vec<(Pubkey, DexProtocol)> {
        self.markets.iter()
            .map(|entry| (entry.key().clone(), entry.value().protocol))
            .collect()
    }

    pub async fn stop(&self) {
        self.running.store(false, Ordering::Release);
    }

    pub fn is_running(&self) -> bool {
        self.running.load(Ordering::Acquire)
    }

    pub fn last_update_ms(&self) -> u64 {
        self.last_update.load(Ordering::Acquire)
    }
}

impl Clone for OrderbookLiquidityDetector {
    fn clone(&self) -> Self {
        Self {
            rpc: Arc::clone(&self.rpc),
            markets: Arc::clone(&self.markets),
            orderbooks: Arc::clone(&self.orderbooks),
            opportunities: Arc::clone(&self.opportunities),
            price_feeds: Arc::clone(&self.price_feeds),
            running: Arc::clone(&self.running),
            last_update: Arc::clone(&self.last_update),
        }
    }
}

#[derive(Debug)]
struct OrderbookUpdate {
    market: Pubkey,
    snapshot: OrderbookSnapshot,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_detector_initialization() {
        let detector = OrderbookLiquidityDetector::new("https://api.mainnet-beta.solana.com")
            .await
            .unwrap();
        assert!(!detector.is_running());
    }

    #[tokio::test]
    async fn test_spread_calculation() {
        let detector = OrderbookLiquidityDetector::new("https://api.mainnet-beta.solana.com")
            .await
            .unwrap();
        
        let bids = vec![
            Order { price: 99.5, size: 10.0, owner: None },
            Order { price: 99.0, size: 20.0, owner: None },
        ];
        
        let asks = vec![
            Order { price: 100.5, size: 10.0, owner: None },
            Order { price: 101.0, size: 20.0, owner: None },
        ];
        
        let (spread_bps, mid_price) = detector.calculate_spread_and_mid(&bids, &asks);
        assert_eq!(spread_bps, 100); // 1% spread = 100 bps
        assert_eq!(mid_price, 100.0);
    }

    #[tokio::test]
    async fn test_liquidity_calculation() {
        let detector = OrderbookLiquidityDetector::new("https://api.mainnet-beta.solana.com")
            .await
            .unwrap();
        
        let orders = vec![
            Order { price: 100.0, size: 10.0, owner: None },
            Order { price: 99.0, size: 20.0, owner: None },
            Order { price: 98.0, size: 30.0, owner: None },
        ];
        
        let liquidity = detector.calculate_liquidity_usd(&orders, 100.0);
        assert_eq!(liquidity, 5940.0); // 100*10 + 99*20 + 98*30
    }
}

