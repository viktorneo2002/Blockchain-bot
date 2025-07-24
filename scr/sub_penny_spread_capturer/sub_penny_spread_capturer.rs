use anchor_lang::prelude::*;
use anchor_spl::token::{self, Token, TokenAccount, Transfer};
use serum_dex::instruction::MarketInstruction;
use serum_dex::matching::{OrderType, Side};
use serum_dex::state::{Market, MarketState};
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_client::rpc_config::{RpcSendTransactionConfig, RpcTransactionConfig};
use solana_sdk::{
    commitment_config::{CommitmentConfig, CommitmentLevel},
    compute_budget::ComputeBudgetInstruction,
    instruction::Instruction,
    pubkey::Pubkey,
    signature::{Keypair, Signature},
    signer::Signer,
    transaction::Transaction,
};
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{Mutex, RwLock};
use tokio::time::{interval, sleep};

const MAX_SLIPPAGE_BPS: u64 = 5;
const MIN_PROFIT_THRESHOLD: u64 = 100_000;
const COMPUTE_UNIT_LIMIT: u32 = 1_400_000;
const PRIORITY_FEE_LAMPORTS: u64 = 50_000;
const MAX_RETRY_ATTEMPTS: u8 = 3;
const SLOT_SUBSCRIBE_DELAY_MS: u64 = 50;
const ORDER_BOOK_DEPTH: usize = 20;
const MAX_CONCURRENT_TXS: usize = 5;

#[derive(Debug, Clone)]
pub struct SubPennySpreadCapturer {
    rpc_client: Arc<RpcClient>,
    fallback_rpcs: Vec<Arc<RpcClient>>,
    wallet: Arc<Keypair>,
    markets: Arc<RwLock<HashMap<Pubkey, MarketData>>>,
    active_orders: Arc<Mutex<HashMap<Pubkey, OrderState>>>,
    profit_tracker: Arc<Mutex<ProfitMetrics>>,
}

#[derive(Debug, Clone)]
struct MarketData {
    market_address: Pubkey,
    base_mint: Pubkey,
    quote_mint: Pubkey,
    base_vault: Pubkey,
    quote_vault: Pubkey,
    request_queue: Pubkey,
    event_queue: Pubkey,
    bids: Pubkey,
    asks: Pubkey,
    base_lot_size: u64,
    quote_lot_size: u64,
    last_update: Instant,
}

#[derive(Debug, Clone)]
struct OrderState {
    market: Pubkey,
    side: Side,
    price: u64,
    size: u64,
    timestamp: Instant,
    tx_signature: Option<Signature>,
}

#[derive(Debug, Default)]
struct ProfitMetrics {
    total_profit: u64,
    successful_captures: u64,
    failed_attempts: u64,
    total_gas_spent: u64,
}

#[derive(Debug)]
struct SpreadOpportunity {
    market: Pubkey,
    bid_price: u64,
    ask_price: u64,
    available_size: u64,
    expected_profit: u64,
    spread_bps: u64,
}

impl SubPennySpreadCapturer {
    pub async fn new(
        rpc_url: &str,
        fallback_urls: Vec<&str>,
        wallet_keypair: Keypair,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let rpc_client = Arc::new(RpcClient::new_with_commitment(
            rpc_url.to_string(),
            CommitmentConfig {
                commitment: CommitmentLevel::Processed,
            },
        ));

        let fallback_rpcs = fallback_urls
            .into_iter()
            .map(|url| {
                Arc::new(RpcClient::new_with_commitment(
                    url.to_string(),
                    CommitmentConfig {
                        commitment: CommitmentLevel::Processed,
                    },
                ))
            })
            .collect();

        Ok(Self {
            rpc_client,
            fallback_rpcs,
            wallet: Arc::new(wallet_keypair),
            markets: Arc::new(RwLock::new(HashMap::new())),
            active_orders: Arc::new(Mutex::new(HashMap::new())),
            profit_tracker: Arc::new(Mutex::new(ProfitMetrics::default())),
        })
    }

    pub async fn add_market(&self, market_address: &str) -> Result<(), Box<dyn std::error::Error>> {
        let market_pubkey = Pubkey::from_str(market_address)?;
        let market_account = self.fetch_account_with_retry(&market_pubkey).await?;
        
        let market_state = Market::load(&market_account.data, &serum_dex::ID, true)?;
        
        let market_data = MarketData {
            market_address: market_pubkey,
            base_mint: Pubkey::new(market_state.coin_mint.as_ref()),
            quote_mint: Pubkey::new(market_state.pc_mint.as_ref()),
            base_vault: Pubkey::new(market_state.coin_vault.as_ref()),
            quote_vault: Pubkey::new(market_state.pc_vault.as_ref()),
            request_queue: Pubkey::new(market_state.req_q.as_ref()),
            event_queue: Pubkey::new(market_state.event_q.as_ref()),
            bids: Pubkey::new(market_state.bids.as_ref()),
            asks: Pubkey::new(market_state.asks.as_ref()),
            base_lot_size: market_state.coin_lot_size,
            quote_lot_size: market_state.pc_lot_size,
            last_update: Instant::now(),
        };

        let mut markets = self.markets.write().await;
        markets.insert(market_pubkey, market_data);
        
        Ok(())
    }

    pub async fn run(&self) -> Result<(), Box<dyn std::error::Error>> {
        let mut interval = interval(Duration::from_millis(SLOT_SUBSCRIBE_DELAY_MS));
        
        loop {
            interval.tick().await;
            
            let opportunities = self.scan_opportunities().await?;
            
            if !opportunities.is_empty() {
                let mut handles = vec![];
                
                for (idx, opportunity) in opportunities.into_iter().enumerate() {
                    if idx >= MAX_CONCURRENT_TXS {
                        break;
                    }
                    
                    let self_clone = self.clone();
                    let handle = tokio::spawn(async move {
                        if let Err(e) = self_clone.execute_capture(opportunity).await {
                            log::error!("Failed to execute capture: {:?}", e);
                        }
                    });
                    
                    handles.push(handle);
                }
                
                for handle in handles {
                    let _ = handle.await;
                }
            }
            
            self.cleanup_stale_orders().await;
        }
    }

    async fn scan_opportunities(&self) -> Result<Vec<SpreadOpportunity>, Box<dyn std::error::Error>> {
        let mut opportunities = Vec::new();
        let markets = self.markets.read().await;
        
        for (market_pubkey, market_data) in markets.iter() {
            if market_data.last_update.elapsed() > Duration::from_secs(5) {
                continue;
            }
            
            match self.analyze_market_spread(market_pubkey, market_data).await {
                Ok(Some(opportunity)) => opportunities.push(opportunity),
                Ok(None) => continue,
                Err(e) => {
                    log::warn!("Failed to analyze market {}: {:?}", market_pubkey, e);
                    continue;
                }
            }
        }
        
        opportunities.sort_by(|a, b| b.expected_profit.cmp(&a.expected_profit));
        opportunities.truncate(MAX_CONCURRENT_TXS);
        
        Ok(opportunities)
    }

    async fn analyze_market_spread(
        &self,
        market_pubkey: &Pubkey,
        market_data: &MarketData,
    ) -> Result<Option<SpreadOpportunity>, Box<dyn std::error::Error>> {
        let bids_account = self.fetch_account_with_retry(&market_data.bids).await?;
        let asks_account = self.fetch_account_with_retry(&market_data.asks).await?;
        
        let market_account = self.fetch_account_with_retry(market_pubkey).await?;
        let market_state = Market::load(&market_account.data, &serum_dex::ID, true)?;
        
        let bids = market_state.load_bids_mut(&bids_account.data)?;
        let asks = market_state.load_asks_mut(&asks_account.data)?;
        
        let best_bid = bids.find_max();
        let best_ask = asks.find_min();
        
        if let (Some(bid), Some(ask)) = (best_bid, best_ask) {
            let bid_price_lots = bid.price();
            let ask_price_lots = ask.price();
            
            if ask_price_lots <= bid_price_lots {
                return Ok(None);
            }
            
            let spread_lots = ask_price_lots - bid_price_lots;
            let spread_native = spread_lots * market_data.quote_lot_size;
            
            if spread_native < MIN_PROFIT_THRESHOLD {
                return Ok(None);
            }
            
            let available_size = std::cmp::min(bid.quantity(), ask.quantity());
            let size_native = available_size * market_data.base_lot_size;
            
            let expected_profit = self.calculate_profit(
                spread_native,
                size_native,
                market_data.quote_lot_size,
            );
            
            if expected_profit < MIN_PROFIT_THRESHOLD {
                return Ok(None);
            }
            
            let spread_bps = (spread_lots * 10000) / bid_price_lots;
            
            Ok(Some(SpreadOpportunity {
                market: *market_pubkey,
                bid_price: bid_price_lots,
                ask_price: ask_price_lots,
                available_size,
                expected_profit,
                spread_bps,
            }))
        } else {
            Ok(None)
        }
    }

    async fn execute_capture(
        &self,
        opportunity: SpreadOpportunity,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let mut active_orders = self.active_orders.lock().await;
        
        if active_orders.contains_key(&opportunity.market) {
            return Ok(());
        }
        
        let order_state = OrderState {
            market: opportunity.market,
            side: Side::Bid,
            price: opportunity.bid_price,
            size: opportunity.available_size,
            timestamp: Instant::now(),
            tx_signature: None,
        };
        
        active_orders.insert(opportunity.market, order_state.clone());
        drop(active_orders);
        
        let instructions = self.build_capture_instructions(&opportunity).await?;
        let signature = self.send_transaction_with_retry(instructions).await?;
        
        let mut active_orders = self.active_orders.lock().await;
        if let Some(order) = active_orders.get_mut(&opportunity.market) {
            order.tx_signature = Some(signature);
        }
        
        self.update_profit_metrics(opportunity.expected_profit, true).await;
        
        Ok(())
    }

    async fn build_capture_instructions(
        &self,
        opportunity: &SpreadOpportunity,
    ) -> Result<Vec<Instruction>, Box<dyn std::error::Error>> {
        let mut instructions = vec![];
        
        instructions.push(ComputeBudgetInstruction::set_compute_unit_limit(COMPUTE_UNIT_LIMIT));
        instructions.push(ComputeBudgetInstruction::set_compute_unit_price(PRIORITY_FEE_LAMPORTS));
        
        let markets = self.markets.read().await;
        let market_data = markets.get(&opportunity.market)
            .ok_or("Market data not found")?;
        
        let (base_ata, _) = Pubkey::find_program_address(
            &[
                &self.wallet.pubkey().to_bytes(),
                &token::ID.to_bytes(),
                &market_data.base_mint.to_bytes(),
            ],
            &anchor_spl::associated_token::ID,
        );
        
        let (quote_ata, _) = Pubkey::find_program_address(
            &[
                &self.wallet.pubkey().to_bytes(),
                &token::ID.to_bytes(),
                &market_data.quote_mint.to_bytes(),
            ],
            &anchor_spl::associated_token::ID,
        );
        
        let buy_instruction = serum_dex::instruction::new_order(
            &opportunity.market,
            &self.wallet.pubkey(),
            &quote_ata,
            &base_ata,
            &market_data.request_queue,
            &market_data.event_queue,
            &market_data.bids,
            &market_data.asks,
            &self.wallet.pubkey(),
            &market_data.base_vault,
            &market_data.quote_vault,
            &token::ID,
            &solana_sdk::sysvar::rent::ID,
            None,
            &serum_dex::ID,
            Side::Bid,
            opportunity.ask_price,
            opportunity.available_size,
            OrderType::ImmediateOrCancel,
            0,
            serum_dex::instruction::SelfTradeBehavior::AbortTransaction,
            u16::MAX,
            u64::MAX,
            120,
        )?;
        
        instructions.push(buy_instruction);
        
        let sell_instruction = serum_dex::instruction::new_order(
            &opportunity.market,
            &self.wallet.pubkey(),
            &quote_ata,
            &base_ata,
            &market_data.request_queue,
                        &market_data.event_queue,
            &market_data.bids,
            &market_data.asks,
            &self.wallet.pubkey(),
            &market_data.base_vault,
            &market_data.quote_vault,
            &token::ID,
            &solana_sdk::sysvar::rent::ID,
            None,
            &serum_dex::ID,
            Side::Ask,
            opportunity.bid_price,
            opportunity.available_size,
            OrderType::ImmediateOrCancel,
            0,
            serum_dex::instruction::SelfTradeBehavior::AbortTransaction,
            u16::MAX,
            u64::MAX,
            120,
        )?;
        
        instructions.push(sell_instruction);
        
        let settle_instruction = serum_dex::instruction::settle_funds(
            &serum_dex::ID,
            &opportunity.market,
            &token::ID,
            &self.wallet.pubkey(),
            &base_ata,
            &market_data.base_vault,
            &quote_ata,
            &market_data.quote_vault,
            None,
            &market_data.event_queue,
            None,
            None,
        )?;
        
        instructions.push(settle_instruction);
        
        Ok(instructions)
    }

    async fn send_transaction_with_retry(
        &self,
        instructions: Vec<Instruction>,
    ) -> Result<Signature, Box<dyn std::error::Error>> {
        let recent_blockhash = self.get_recent_blockhash_with_retry().await?;
        
        let transaction = Transaction::new_signed_with_payer(
            &instructions,
            Some(&self.wallet.pubkey()),
            &[&*self.wallet],
            recent_blockhash,
        );
        
        let config = RpcSendTransactionConfig {
            skip_preflight: true,
            preflight_commitment: Some(CommitmentLevel::Processed),
            encoding: None,
            max_retries: Some(0),
            min_context_slot: None,
        };
        
        let mut last_error = None;
        
        for attempt in 0..MAX_RETRY_ATTEMPTS {
            let rpc_client = if attempt == 0 {
                &self.rpc_client
            } else {
                &self.fallback_rpcs[(attempt - 1) % self.fallback_rpcs.len()]
            };
            
            match rpc_client.send_transaction_with_config(&transaction, config).await {
                Ok(signature) => {
                    self.confirm_transaction_with_timeout(&signature).await?;
                    return Ok(signature);
                }
                Err(e) => {
                    last_error = Some(e);
                    if attempt < MAX_RETRY_ATTEMPTS - 1 {
                        sleep(Duration::from_millis(50 * (attempt as u64 + 1))).await;
                    }
                }
            }
        }
        
        Err(last_error.unwrap().into())
    }

    async fn get_recent_blockhash_with_retry(&self) -> Result<solana_sdk::hash::Hash, Box<dyn std::error::Error>> {
        let mut last_error = None;
        
        for attempt in 0..MAX_RETRY_ATTEMPTS {
            let rpc_client = if attempt == 0 {
                &self.rpc_client
            } else {
                &self.fallback_rpcs[(attempt - 1) % self.fallback_rpcs.len()]
            };
            
            match rpc_client.get_latest_blockhash().await {
                Ok(blockhash) => return Ok(blockhash),
                Err(e) => {
                    last_error = Some(e);
                    if attempt < MAX_RETRY_ATTEMPTS - 1 {
                        sleep(Duration::from_millis(50)).await;
                    }
                }
            }
        }
        
        Err(last_error.unwrap().into())
    }

    async fn fetch_account_with_retry(
        &self,
        pubkey: &Pubkey,
    ) -> Result<solana_sdk::account::Account, Box<dyn std::error::Error>> {
        let mut last_error = None;
        
        for attempt in 0..MAX_RETRY_ATTEMPTS {
            let rpc_client = if attempt == 0 {
                &self.rpc_client
            } else {
                &self.fallback_rpcs[(attempt - 1) % self.fallback_rpcs.len()]
            };
            
            match rpc_client.get_account(pubkey).await {
                Ok(account) => return Ok(account),
                Err(e) => {
                    last_error = Some(e);
                    if attempt < MAX_RETRY_ATTEMPTS - 1 {
                        sleep(Duration::from_millis(50)).await;
                    }
                }
            }
        }
        
        Err(last_error.unwrap().into())
    }

    async fn confirm_transaction_with_timeout(
        &self,
        signature: &Signature,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let start_time = Instant::now();
        let timeout = Duration::from_secs(30);
        
        while start_time.elapsed() < timeout {
            match self.rpc_client.get_signature_status(signature).await {
                Ok(Some(status)) => {
                    if status.is_ok() {
                        return Ok(());
                    } else if status.is_err() {
                        return Err("Transaction failed".into());
                    }
                }
                Ok(None) => {
                    sleep(Duration::from_millis(250)).await;
                    continue;
                }
                Err(_) => {
                    sleep(Duration::from_millis(250)).await;
                    continue;
                }
            }
        }
        
        Err("Transaction confirmation timeout".into())
    }

    fn calculate_profit(
        &self,
        spread_native: u64,
        size_native: u64,
        quote_lot_size: u64,
    ) -> u64 {
        let gross_profit = (spread_native * size_native) / 1_000_000_000;
        
        let taker_fee_bps = 4;
        let maker_fee_bps = 0;
        let total_fee_bps = taker_fee_bps + maker_fee_bps;
        
        let fee_amount = (gross_profit * total_fee_bps) / 10_000;
        
        let gas_estimate = PRIORITY_FEE_LAMPORTS + 10_000;
        
        let slippage_amount = (gross_profit * MAX_SLIPPAGE_BPS) / 10_000;
        
        let net_profit = gross_profit.saturating_sub(fee_amount)
            .saturating_sub(gas_estimate)
            .saturating_sub(slippage_amount);
        
        net_profit
    }

    async fn update_profit_metrics(&self, profit: u64, success: bool) {
        let mut metrics = self.profit_tracker.lock().await;
        
        if success {
            metrics.total_profit += profit;
            metrics.successful_captures += 1;
        } else {
            metrics.failed_attempts += 1;
        }
        
        metrics.total_gas_spent += PRIORITY_FEE_LAMPORTS + 10_000;
    }

    async fn cleanup_stale_orders(&self) {
        let mut active_orders = self.active_orders.lock().await;
        let now = Instant::now();
        
        active_orders.retain(|_, order| {
            now.duration_since(order.timestamp) < Duration::from_secs(60)
        });
    }

    pub async fn get_profit_metrics(&self) -> ProfitMetrics {
        let metrics = self.profit_tracker.lock().await;
        metrics.clone()
    }

    pub async fn update_market_data(&self, market_pubkey: &Pubkey) -> Result<(), Box<dyn std::error::Error>> {
        let market_account = self.fetch_account_with_retry(market_pubkey).await?;
        let market_state = Market::load(&market_account.data, &serum_dex::ID, true)?;
        
        let mut markets = self.markets.write().await;
        if let Some(market_data) = markets.get_mut(market_pubkey) {
            market_data.base_lot_size = market_state.coin_lot_size;
            market_data.quote_lot_size = market_state.pc_lot_size;
            market_data.last_update = Instant::now();
        }
        
        Ok(())
    }

    pub async fn emergency_cancel_all(&self) -> Result<(), Box<dyn std::error::Error>> {
        let active_orders = self.active_orders.lock().await.clone();
        let markets = self.markets.read().await;
        
        for (market_pubkey, _) in active_orders.iter() {
            if let Some(market_data) = markets.get(market_pubkey) {
                let cancel_instruction = serum_dex::instruction::cancel_order_v2(
                    &serum_dex::ID,
                    market_pubkey,
                    &market_data.bids,
                    &market_data.asks,
                    &self.wallet.pubkey(),
                    &market_data.event_queue,
                    Side::Bid,
                    [0; 16],
                )?;
                
                match self.send_transaction_with_retry(vec![cancel_instruction]).await {
                    Ok(_) => log::info!("Cancelled order on market {}", market_pubkey),
                    Err(e) => log::error!("Failed to cancel order on market {}: {:?}", market_pubkey, e),
                }
            }
        }
        
        self.active_orders.lock().await.clear();
        Ok(())
    }

    pub async fn health_check(&self) -> bool {
        match self.rpc_client.get_health().await {
            Ok(_) => true,
            Err(_) => {
                for fallback in &self.fallback_rpcs {
                    if fallback.get_health().await.is_ok() {
                        return true;
                    }
                }
                false
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use solana_sdk::signature::Keypair;

    #[tokio::test]
    async fn test_profit_calculation() {
        let wallet = Keypair::new();
        let capturer = SubPennySpreadCapturer::new(
            "https://api.mainnet-beta.solana.com",
            vec![],
            wallet,
        ).await.unwrap();
        
        let profit = capturer.calculate_profit(1_000_000, 1_000_000_000, 1000);
        assert!(profit > 0);
    }
}

