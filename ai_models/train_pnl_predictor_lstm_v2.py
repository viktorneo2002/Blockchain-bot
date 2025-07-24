use anyhow::{Context, Result};
use anchor_lang::prelude::*;
use arrayref::{array_ref, array_refs};
use bytemuck::{Pod, Zeroable};
use ndarray::{Array3, ArrayView3, Axis};
use onnxruntime::{
    environment::Environment,
    session::Session,
    tensor::OrtOwnedTensor,
    GraphOptimizationLevel,
    LoggingLevel,
};
use solana_client::{
    nonblocking::rpc_client::RpcClient,
    rpc_config::{RpcSendTransactionConfig, RpcSimulateTransactionConfig},
};
use solana_sdk::{
    commitment_config::{CommitmentConfig, CommitmentLevel},
    compute_budget::ComputeBudgetInstruction,
    instruction::{AccountMeta, Instruction},
    pubkey::Pubkey,
    signature::{Keypair, Signature},
    signer::Signer,
    transaction::{Transaction, VersionedTransaction},
    message::{v0, VersionedMessage},
};
use spl_associated_token_account::get_associated_token_address;
use spl_token::state::Account as TokenAccount;
use std::{
    collections::VecDeque,
    mem::size_of,
    str::FromStr,
    sync::Arc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use tokio::{
    sync::{Mutex, RwLock},
    time::{interval, sleep, timeout},
};

const MODEL_PATH: &str = "pnl_predictor_lstm_v2.onnx";
const SEQUENCE_LENGTH: usize = 60;
const FEATURE_DIM: usize = 12;
const PREDICTION_THRESHOLD: f32 = 0.0032;
const MAX_POSITION_USDC: u64 = 100_000_000_000; // 100k USDC
const MIN_PROFIT_BPS: i32 = 18; // 0.18%
const STOP_LOSS_BPS: i32 = -7; // -0.07%
const MAX_SLIPPAGE_BPS: u32 = 3; // 0.03%
const JITO_TIP_LAMPORTS: u64 = 10_000; // 0.00001 SOL tip
const RAYDIUM_V4: &str = "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8";
const SERUM_V3: &str = "9xQeWvG816bUx9EPjHmaT23yvVM2ZWbrrpZb9PusVFin";
const JITO_BLOCK_ENGINE: &str = "https://ny.mainnet.block-engine.jito.wtf/api/v1/bundles";
const JITO_TIP_ACCOUNT: &str = "96gYZGLnJYVFmbjzopPSU6QiEV5fGqZNyN9nmNhvrZU5";

#[repr(C)]
#[derive(Clone, Copy, Pod, Zeroable)]
pub struct RaydiumAmm {
    pub status: u64,
    pub nonce: u64,
    pub max_order: u64,
    pub depth: u64,
    pub base_decimal: u64,
    pub quote_decimal: u64,
    pub state: u64,
    pub reset_flag: u64,
    pub min_size: u64,
    pub vol_max_cut_ratio: u64,
    pub amount_wave: u64,
    pub base_lot_size: u64,
    pub quote_lot_size: u64,
    pub min_price_multiplier: u64,
    pub max_price_multiplier: u64,
    pub system_decimal_value: u64,
    pub min_separate_numerator: u64,
    pub min_separate_denominator: u64,
    pub trade_fee_numerator: u64,
    pub trade_fee_denominator: u64,
    pub pnl_numerator: u64,
    pub pnl_denominator: u64,
    pub swap_fee_numerator: u64,
    pub swap_fee_denominator: u64,
    pub base_need_take_pnl: u64,
    pub quote_need_take_pnl: u64,
    pub quote_total_pnl: u64,
    pub base_total_pnl: u64,
    pub pool_open_time: u64,
    pub punish_pc_amount: u64,
    pub punish_coin_amount: u64,
    pub orderbook_to_init_time: u64,
    pub swap_base_in_amount: u128,
    pub swap_quote_out_amount: u128,
    pub swap_base2quote_fee: u64,
    pub swap_quote_in_amount: u128,
    pub swap_base_out_amount: u128,
    pub swap_quote2base_fee: u64,
    pub base_vault: Pubkey,
    pub quote_vault: Pubkey,
    pub base_mint: Pubkey,
    pub quote_mint: Pubkey,
    pub lp_mint: Pubkey,
    pub open_orders: Pubkey,
    pub market_id: Pubkey,
    pub market_program_id: Pubkey,
    pub target_orders: Pubkey,
    pub withdraw_queue: Pubkey,
    pub temp_lp_vault: Pubkey,
    pub owner: Pubkey,
    pub pnl_owner: Pubkey,
}

#[derive(Clone, Debug)]
struct MarketData {
    timestamp: u64,
    price: f64,
    volume_24h: f64,
    bid: f64,
    ask: f64,
    spread_bps: f64,
    liquidity: f64,
    volatility_1h: f64,
    price_change_5m: f64,
    rsi_14: f64,
    vwap: f64,
    order_flow_imbalance: f64,
}

struct LSTMPredictor {
    session: Arc<Mutex<Session<'static>>>,
    environment: Arc<Environment>,
    data_buffer: Arc<RwLock<VecDeque<MarketData>>>,
    predictions_cache: Arc<RwLock<VecDeque<(f32, u64)>>>,
}

impl LSTMPredictor {
    async fn new() -> Result<Self> {
        let environment = Arc::new(
            Environment::builder()
                .with_name("solana_lstm_predictor")
                .with_log_level(LoggingLevel::Error)
                .build()?
        );

        let session = Session::builder(&environment)?
            .with_optimization_level(GraphOptimizationLevel::Level3)?
            .with_intra_threads(8)?
            .with_model_from_file(MODEL_PATH)?;

        Ok(Self {
            session: Arc::new(Mutex::new(session)),
            environment,
            data_buffer: Arc::new(RwLock::new(VecDeque::with_capacity(SEQUENCE_LENGTH + 10))),
            predictions_cache: Arc::new(RwLock::new(VecDeque::with_capacity(100))),
        })
    }

    async fn predict(&self, data: MarketData) -> Result<f32> {
        let mut buffer = self.data_buffer.write().await;
        buffer.push_back(data.clone());
        while buffer.len() > SEQUENCE_LENGTH {
            buffer.pop_front();
        }
        drop(buffer);

        let buffer = self.data_buffer.read().await;
        if buffer.len() < SEQUENCE_LENGTH {
            return Ok(0.0);
        }

        let mut input_tensor = Array3::<f32>::zeros((1, SEQUENCE_LENGTH, FEATURE_DIM));
        for (i, mkt) in buffer.iter().enumerate() {
            let normalized_price = (mkt.price.ln() * 100.0) as f32;
            let normalized_volume = (mkt.volume_24h / 1e9).ln().max(0.0) as f32;
            
            input_tensor[[0, i, 0]] = normalized_price;
            input_tensor[[0, i, 1]] = normalized_volume;
            input_tensor[[0, i, 2]] = (mkt.spread_bps / 10.0) as f32;
            input_tensor[[0, i, 3]] = (mkt.liquidity / 1e8).ln().max(0.0) as f32;
            input_tensor[[0, i, 4]] = (mkt.volatility_1h / 100.0) as f32;
            input_tensor[[0, i, 5]] = (mkt.price_change_5m / 100.0).tanh() as f32;
            input_tensor[[0, i, 6]] = ((mkt.rsi_14 - 50.0) / 50.0) as f32;
            input_tensor[[0, i, 7]] = (mkt.vwap / mkt.price - 1.0).tanh() as f32;
            input_tensor[[0, i, 8]] = mkt.order_flow_imbalance.tanh() as f32;
            input_tensor[[0, i, 9]] = ((mkt.bid + mkt.ask) / 2.0 / 100.0).ln() as f32;
            input_tensor[[0, i, 10]] = ((mkt.timestamp % 86400000) as f32 / 86400000.0);
            input_tensor[[0, i, 11]] = ((mkt.timestamp % 3600000) as f32 / 3600000.0);
        }

        let session = self.session.lock().await;
        let outputs: Vec<OrtOwnedTensor<f32, _>> = session.run(vec![input_tensor.view()])?;
        
        if outputs.is_empty() || outputs[0].shape().is_empty() {
            return Err(anyhow::anyhow!("Invalid ONNX output"));
        }

        let prediction = outputs[0]
            .view()
            .as_slice()
            .and_then(|s| s.first())
            .copied()
            .unwrap_or(0.0);

        let mut cache = self.predictions_cache.write().await;
        cache.push_back((prediction, data.timestamp));
        while cache.len() > 100 {
            cache.pop_front();
        }

        Ok(prediction.tanh() * 2.0) // Scale to [-2, 2]
    }
}

struct SolanaHFTBot {
    rpc: Arc<RpcClient>,
    wallet: Arc<Keypair>,
    predictor: Arc<LSTMPredictor>,
    amm_id: Pubkey,
    amm_authority: Pubkey,
    amm_open_orders: Pubkey,
    amm_target_orders: Pubkey,
    amm_base_vault: Pubkey,
    amm_quote_vault: Pubkey,
    market_program: Pubkey,
    market_id: Pubkey,
    market_bids: Pubkey,
    market_asks: Pubkey,
    market_event_queue: Pubkey,
    market_base_vault: Pubkey,
    market_quote_vault: Pubkey,
    market_req_q: Pubkey,
    base_mint: Pubkey,
    quote_mint: Pubkey,
    position: Arc<RwLock<Position>>,
}

#[derive(Default, Debug)]
struct Position {
    base_amount: u64,
    quote_spent: u64,
    avg_entry_price: f64,
    entry_timestamp: u64,
    unrealized_pnl_bps: i32,
}

impl SolanaHFTBot {
    async fn new(rpc_url: &str, wallet: Keypair, amm_id: &str) -> Result<Self> {
        let rpc = Arc::new(RpcClient::new_with_commitment(
            rpc_url.to_string(),
            CommitmentConfig::confirmed(),
        ));

        let amm_pubkey = Pubkey::from_str(amm_id)?;
        let amm_data = rpc.get_account_data(&amm_pubkey).await?;
        
        if amm_data.len() < size_of::<RaydiumAmm>() {
            return Err(anyhow::anyhow!("Invalid AMM account data"));
        }

        let amm: &RaydiumAmm = bytemuck::from_bytes(&amm_data[..size_of::<RaydiumAmm>()]);
        
        let (amm_authority, _) = Pubkey::find_program_address(
            &[&amm_pubkey.to_bytes()[..32]],
            &Pubkey::from_str(RAYDIUM_V4)?,
        );

        let market_keys = Self::fetch_serum_market_keys(&rpc, &amm.market_id).await?;

        Ok(Self {
            rpc,
            wallet: Arc::new(wallet),
            predictor: Arc::new(LSTMPredictor::new().await?),
            amm_id: amm_pubkey,
            amm_authority,
            amm_open_orders: amm.open_orders,
            amm_target_orders: amm.target_orders,
            amm_base_vault: amm.base_vault,
            amm_quote_vault: amm.quote_vault,
            market_program: amm.market_program_id,
            market_id: amm.market_id,
            market_bids: market_keys.0,
            market_asks: market_keys.1,
            market_event_queue: market_keys.2,
            market_base_vault: market_keys.3,
            market_quote_vault: market_keys.4,
            market_req_q: market_keys.5,
            base_mint: amm.base_mint,
            quote_mint: amm.quote_mint,
            position: Arc::new(RwLock::new(Position::default())),
        })
    }

    async fn fetch_serum_market_keys(rpc: &RpcClient, market: &Pubkey) -> Result<(Pubkey, Pubkey, Pubkey, Pubkey, Pubkey, Pubkey)> {
        let market_data = rpc.get_account_data(market).await?;
        let bids = Pubkey::new(&market_data[8..40]);
        let asks = Pubkey::new(&market_data[40..72]);
        let event_q = Pubkey::new(&market_data[72..104]);
        let base_vault = Pubkey::new(&market_data[104..136]);
        let quote_vault = Pubkey::new(&market_data[136..168]);
        let req_q = Pubkey::new(&market_data[168..200]);
        
        Ok((bids, asks, event_q, base_vault, quote_vault, req_q))
    }

    async fn fetch_market_data(&self) -> Result<MarketData> {
        let amm_data = self.rpc.get_account_data(&self.amm_id).await?;
        let amm: &RaydiumAmm = bytemuck::from_bytes(&amm_data[..size_of::<RaydiumAmm>()]);
        
        let base_vault_data = self.rpc.get_account_data(&self.amm_base_vault).await?;
        let quote_vault_data = self.rpc.get_account_data(&self.amm_quote_vault).await?;
        
        let base_token_account: TokenAccount = TokenAccount::unpack(&base_vault_data)?;
        let quote_token_account: TokenAccount = TokenAccount::unpack(&quote_vault_data)?;
        
        let base_amount = base_token_account.amount;
        let quote_amount = quote_token_account.amount;
        
        let base_decimals = amm.base_decimal;
        let quote_decimals = amm.quote_decimal;
        
        let base_ui = base_amount as f64 / 10f64.powf(base_decimals as f64);
        let quote_ui = quote_amount as f64 / 10f64.powf(quote_decimals as f64);
        
        let price = if base_ui > 0.0 { quote_ui / base_ui } else { 0.0 };
        
        let k = base_amount as u128 * quote_amount as u128;
        let liquidity = 2.0 * (k as f64).sqrt() / 10f64.powf(quote_decimals as f64);
        
        let fee_bps = (amm.swap_fee_numerator as f64 / amm.swap_fee_denominator as f64) * 10000.0;
        let half_spread = price * fee_bps / 20000.0;
        
        let bid = price - half_spread;
        let ask = price + half_spread;
        
        let volume_24h = (amm.swap_base_in_amount + amm.swap_quote_in_amount) as f64;
        
        let buffer = self.predictor.data_buffer.read().await;
        let volatility = self.calculate_volatility(&buffer);
        let price_change_5m = self.calculate_price_change(&buffer, 5);
        let rsi = self.calculate_rsi(&buffer, price);
        let vwap = self.calculate_vwap(&buffer);
        let order_flow = self.calculate_order_flow_imbalance(amm);
        
        Ok(MarketData {
            timestamp: SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis() as u64,
            price,
            volume_24h,
            bid,
            ask,
            spread_bps: (ask - bid) / bid * 10000.0,
            liquidity,
            volatility_1h: volatility,
            price_change_5m,
            rsi_14: rsi,
            vwap,
            order_flow_imbalance: order_flow,
        })
    }

    fn calculate_volatility(&self, buffer: &VecDeque<MarketData>) -> f64 {
        if buffer.len() < 12 { return 0.5; }
        
        let prices: Vec<f64> = buffer.iter().rev().take(12).map(|d| d.price).collect();
        let mut returns = Vec::new();
        
        for i in 1..prices.len() {
            let ret = (prices[i] / prices[i-1]).ln();
            returns.push(ret);
        }
        
        let mean = returns.iter().sum::<f64>() / returns.len() as f64;
        let variance = returns.iter().map(|r| (r - mean).powi(2)).sum::<f64>() / returns.len() as f64;
        
        (variance.sqrt() * (252.0 * 24.0).sqrt()) * 100.0
    }

    fn calculate_price_change(&self, buffer: &VecDeque<MarketData>, minutes: usize) -> f64 {
        if buffer.len() < minutes { return 0.0; }
        
        let current = buffer.back().map(|d| d.price).unwrap_or(0.0);
        let past = buffer.get(buffer.len().saturating_sub(minutes)).map(|d| d.price).unwrap_or(current);
        
        if past > 0.0 { (current - past) / past * 100.0 } else { 0.0 }
    }

    fn calculate_rsi(&self, buffer: &VecDeque<MarketData>, current_price: f64) -> f64 {
        if buffer.len() < 14 { return 50.0; }
        
        let mut prices: Vec<f64> = buffer.iter().rev().take(14).map(|d| d.price).collect();
        prices.push(current_price);
        
        let mut gains = 0.0;
        let mut losses = 0.0;
        
        for i in 1..prices.len() {
            let change = prices[i] - prices[i-1];
            if change > 0.0 {
                gains += change;
            } else {
                losses -= change;
            }
        }
        
        let avg_gain = gains / 14.0;
        let avg_loss = losses / 14.0;
        
        if avg_loss == 0.0 { return 100.0; }
        
        let rs = avg_gain / avg_loss;
        100.0 - (100.0 / (1.0 + rs))
    }

    fn calculate_vwap(&self, buffer: &VecDeque<MarketData>) -> f64 {
        if buffer.is_empty() { return 0.0; }
        
        let mut sum_pv = 0.0;
        let mut sum_v = 0.0;
        
        for data in buffer.iter().rev().take(20) {
            let typical_price = (data.bid + data.ask + data.price) / 3.0;
            let volume = data.volume_24h / 24.0 / 60.0; // Approx volume per minute
            sum_pv += typical_price * volume;
            sum_v += volume;
        }
        
        if sum_v > 0.0 { sum_pv / sum_v } else { 0.0 }
    }

    fn calculate_order_flow_imbalance(&self, amm: &RaydiumAmm) -> f64 {
        let buy_volume = amm.swap_quote_in_amount as f64;
        let sell_volume = amm.swap_base_in_amount as f64 * 100.0; // Rough price estimate
        let total_volume = buy_volume + sell_volume;
        
        if total_volume > 0.0 {
            (buy_volume - sell_volume) / total_volume
        } else {
            0.0
        }
    }

    async fn execute_trade(&self, market_data: &MarketData, prediction: f32) -> Result<()> {
        let mut position = self.position.write().await;
        
        if position.base_amount == 0 && prediction > PREDICTION_THRESHOLD {
            if self.check_entry_conditions(&market_data, prediction) {
                drop(position);
                self.open_position(market_data).await?;
            }
        } else if position.base_amount > 0 {
            let pnl_bps = self.calculate_pnl_bps(&position, market_data);
            position.unrealized_pnl_bps = pnl_bps;
            
            if self.check_exit_conditions(&position, market_data, prediction, pnl_bps) {
                drop(position);
                self.close_position(market_data).await?;
            }
        }
        
        Ok(())
    }

    fn check_entry_conditions(&self, market_data: &MarketData, prediction: f32) -> bool {
        prediction > PREDICTION_THRESHOLD &&
        market_data.spread_bps < 8.0 &&
        market_data.volatility_1h < 3.0 &&
        market_data.liquidity > 250_000.0 &&
        market_data.rsi_14 > 20.0 && market_data.rsi_14 < 80.0 &&
        market_data.order_flow_imbalance > -0.2
    }

    fn check_exit_conditions(&self, position: &Position, market_data: &MarketData, prediction: f32, pnl_bps: i32) -> bool {
        let time_held = market_data.timestamp - position.entry_timestamp;
        
        pnl_bps >= MIN_PROFIT_BPS ||
        pnl_bps <= STOP_LOSS_BPS ||
        prediction < -PREDICTION_THRESHOLD * 0.8 ||
        (time_held > 180_000 && pnl_bps > 5) ||
        market_data.volatility_1h > 5.0 ||
        market_data.order_flow_imbalance < -0.5
    }

    fn calculate_pnl_bps(&self, position: &Position, market_data: &MarketData) -> i32 {
        if position.avg_entry_price > 0.0 {
            ((market_data.bid - position.avg_entry_price) / position.avg_entry_price * 10000.0) as i32
        } else {
            0
        }
    }

    async fn open_position(&self, market_data: &MarketData) -> Result<()> {
        let quote_balance = self.get_token_balance(&self.quote_mint).await?;
        let position_size = self.calculate_position_size(quote_balance, market_data);
        
        if position_size < 1_000_000 { // Min 1 USDC
            return Ok(());
        }
        
        let amount_in = position_size;
        let min_amount_out = self.calculate_min_amount_out(amount_in, market_data, true);
        
        let swap_ix = self.build_swap_instruction(amount_in, min_amount_out, true)?;
        let tx = self.build_jito_bundle(vec![swap_ix]).await?;
        
        let sig = self.send_jito_bundle(tx).await?;
        
        let mut position = self.position.write().await;
        position.base_amount = min_amount_out;
        position.quote_spent = amount_in;
        position.avg_entry_price = market_data.ask;
        position.entry_timestamp = market_data.timestamp;
        
        Ok(())
    }

    async fn close_position(&self, market_data: &MarketData) -> Result<()> {
        let position_data = self.position.read().await;
        let amount_in = position_data.base_amount;
        drop(position_data);
        
        if amount_in == 0 { return Ok(()); }
        
        let min_amount_out = self.calculate_min_amount_out(amount_in, market_data, false);
        
        let swap_ix = self.build_swap_instruction(amount_in, min_amount_out, false)?;
        let tx = self.build_jito_bundle(vec![swap_ix]).await?;
        
        let sig = self.send_jito_bundle(tx).await?;
        
        let mut position = self.position.write().await;
        *position = Position::default();
        
        Ok(())
    }

    fn calculate_position_size(&self, quote_balance: u64, market_data: &MarketData) -> u64 {
        let max_allowed = quote_balance.min(MAX_POSITION_USDC);
        let base_size = (max_allowed as f64 * 0.9) as u64;
        
        let vol_factor = 1.0 / (1.0 + market_data.volatility_1h / 200.0);
        let liq_factor = (market_data.liquidity / 500_000.0).min(1.0);
        let spread_factor = 1.0 / (1.0 + market_data.spread_bps / 50.0);
        let rsi_factor = 1.0 - ((market_data.rsi_14 - 50.0).abs() / 100.0);
        
        (base_size as f64 * vol_factor * liq_factor * spread_factor * rsi_factor) as u64
    }

    fn calculate_min_amount_out(&self, amount_in: u64, market_data: &MarketData, is_buy: bool) -> u64 {
        let price = if is_buy { market_data.ask } else { market_data.bid };
        let slippage_multiplier = 1.0 - (MAX_SLIPPAGE_BPS as f64 / 10000.0);
        
        if is_buy {
            ((amount_in as f64 / price) * slippage_multiplier * 1e9) as u64
        } else {
            ((amount_in as f64 * price / 1e9) * slippage_multiplier * 1e6) as u64
        }
    }

    fn build_swap_instruction(&self, amount_in: u64, min_amount_out: u64, is_buy: bool) -> Result<Instruction> {
        let user_source_ata = get_associated_token_address(
            &self.wallet.pubkey(),
            if is_buy { &self.quote_mint } else { &self.base_mint },
        );
        let user_dest_ata = get_associated_token_address(
            &self.wallet.pubkey(),
            if is_buy { &self.base_mint } else { &self.quote_mint },
        );
        
        let accounts = vec![
            AccountMeta::new_readonly(spl_token::id(), false),
            AccountMeta::new(self.amm_id, false),
            AccountMeta::new_readonly(self.amm_authority, false),
            AccountMeta::new(self.amm_open_orders, false),
            AccountMeta::new(self.amm_target_orders, false),
            AccountMeta::new(self.amm_base_vault, false),
            AccountMeta::new(self.amm_quote_vault, false),
            AccountMeta::new_readonly(self.market_program, false),
            AccountMeta::new(self.market_id, false),
            AccountMeta::new(self.market_bids, false),
            AccountMeta::new(self.market_asks, false),
            AccountMeta::new(self.market_event_queue, false),
            AccountMeta::new(self.market_base_vault, false),
            AccountMeta::new(self.market_quote_vault, false),
            AccountMeta::new_readonly(self.market_req_q, false),
            AccountMeta::new(user_source_ata, false),
            AccountMeta::new(user_dest_ata, false),
            AccountMeta::new_readonly(self.wallet.pubkey(), true),
        ];
        
        let mut data = vec![9u8]; // Raydium swap instruction discriminator
        data.extend_from_slice(&amount_in.to_le_bytes());
        data.extend_from_slice(&min_amount_out.to_le_bytes());
        
        Ok(Instruction {
            program_id: Pubkey::from_str(RAYDIUM_V4)?,
            accounts,
            data,
        })
    }

    async fn build_jito_bundle(&self, instructions: Vec<Instruction>) -> Result<VersionedTransaction> {
        let tip_ix = system_instruction::transfer(
            &self.wallet.pubkey(),
            &Pubkey::from_str(JITO_TIP_ACCOUNT)?,
            JITO_TIP_LAMPORTS,
        );
        
        let priority_fee_ix = ComputeBudgetInstruction::set_compute_unit_price(100_000);
        let compute_limit_ix = ComputeBudgetInstruction::set_compute_unit_limit(300_000);
        
        let mut all_instructions = vec![priority_fee_ix, compute_limit_ix, tip_ix];
        all_instructions.extend(instructions);
        
        let recent_blockhash = self.rpc.get_latest_blockhash().await?;
        
        let message = v0::Message::try_compile(
            &self.wallet.pubkey(),
            &all_instructions,
            &[],
            recent_blockhash,
        )?;
        
        Ok(VersionedTransaction::try_new(
            VersionedMessage::V0(message),
            &[&*self.wallet],
        )?)
    }

    async fn send_jito_bundle(&self, tx: VersionedTransaction) -> Result<Signature> {
        let client = reqwest::Client::builder()
            .timeout(Duration::from_millis(2000))
            .build()?;
        
        let bundle = serde_json::json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "sendBundle",
            "params": [[bs58::encode(&bincode::serialize(&tx)?).into_string()]]
        });
        
        let mut retries = 0;
        let max_retries = 3;
        
        while retries < max_retries {
            let response = client
                .post(JITO_BLOCK_ENGINE)
                .json(&bundle)
                .send()
                .await?;
            
            if response.status().is_success() {
                let bundle_id: serde_json::Value = response.json().await?;
                if let Some(result) = bundle_id.get("result") {
                    return self.wait_for_bundle_confirmation(&tx.signatures[0]).await;
                }
            }
            
            retries += 1;
            if retries < max_retries {
                sleep(Duration::from_millis(100)).await;
            }
        }
        
        // Fallback to regular RPC if Jito fails
        self.send_transaction_with_retry(tx).await
    }

    async fn wait_for_bundle_confirmation(&self, sig: &Signature) -> Result<Signature> {
        let start = Instant::now();
        let timeout = Duration::from_secs(30);
        
        while start.elapsed() < timeout {
            match self.rpc.get_signature_status(sig).await? {
                Some(status) => {
                    if status.is_ok() {
                        return Ok(*sig);
                    } else if status.is_err() {
                        return Err(anyhow::anyhow!("Transaction failed: {:?}", status));
                    }
                }
                None => {
                    sleep(Duration::from_millis(200)).await;
                }
            }
        }
        
        Err(anyhow::anyhow!("Bundle confirmation timeout"))
    }

    async fn send_transaction_with_retry(&self, tx: VersionedTransaction) -> Result<Signature> {
        let config = RpcSendTransactionConfig {
            skip_preflight: true,
            preflight_commitment: Some(CommitmentLevel::Processed),
            max_retries: Some(0),
            ..Default::default()
        };
        
        let sig = self.rpc.send_transaction_with_config(&tx.into(), config).await?;
        
        let start = Instant::now();
        while start.elapsed() < Duration::from_millis(5000) {
            match self.rpc.get_signature_status(&sig).await? {
                Some(status) if status.is_ok() => return Ok(sig),
                Some(status) if status.is_err() => {
                    return Err(anyhow::anyhow!("Transaction failed: {:?}", status))
                }
                _ => sleep(Duration::from_millis(100)).await,
            }
        }
        
        Err(anyhow::anyhow!("Transaction confirmation timeout"))
    }

    async fn get_token_balance(&self, mint: &Pubkey) -> Result<u64> {
        let ata = get_associated_token_address(&self.wallet.pubkey(), mint);
        
        match self.rpc.get_account_data(&ata).await {
            Ok(data) => {
                let account = TokenAccount::unpack(&data)?;
                Ok(account.amount)
            }
            Err(_) => Ok(0),
        }
    }

    pub async fn run(&self) -> Result<()> {
        let mut interval = interval(Duration::from_millis(100));
        let mut last_trade = Instant::now();
        let mut consecutive_errors = 0u32;
        let mut last_prediction_time = Instant::now();
        
        loop {
            interval.tick().await;
            
            let cycle_result = async {
                let market_data = self.fetch_market_data().await
                    .context("Failed to fetch market data")?;
                
                let prediction = if last_prediction_time.elapsed() > Duration::from_millis(50) {
                    let pred = self.predictor.predict(market_data.clone()).await
                        .context("Failed to get prediction")?;
                    last_prediction_time = Instant::now();
                    pred
                } else {
                    self.predictor.predictions_cache.read().await
                        .back()
                        .map(|(p, _)| *p)
                        .unwrap_or(0.0)
                };
                
                if last_trade.elapsed() > Duration::from_millis(500) {
                    self.execute_trade(&market_data, prediction).await
                        .context("Failed to execute trade")?;
                    last_trade = Instant::now();
                }
                
                Ok::<(), anyhow::Error>(())
            }.await;
            
            match cycle_result {
                Ok(_) => consecutive_errors = 0,
                Err(e) => {
                    consecutive_errors += 1;
                    eprintln!("Cycle error {}: {:?}", consecutive_errors, e);
                    
                    if consecutive_errors > 20 {
                        return Err(anyhow::anyhow!("Too many consecutive errors"));
                    }
                    
                    sleep(Duration::from_millis(consecutive_errors as u64 * 100)).await;
                }
            }
        }
    }
}

pub async fn run_hft_bot(
    rpc_url: &str,
    wallet_keypair: &[u8],
    amm_pool_id: &str,
) -> Result<()> {
    let keypair = Keypair::from_bytes(wallet_keypair)?;
    let bot = Arc::new(SolanaHFTBot::new(rpc_url, keypair, amm_pool_id).await?);
    
    let bot_handle = bot.clone();
    let main_task = tokio::spawn(async move {
        bot_handle.run().await
    });
    
    tokio::select! {
        result = main_task => {
            match result {
                Ok(Ok(_)) => Ok(()),
                Ok(Err(e)) => Err(e),
                Err(e) => Err(anyhow::anyhow!("Task panicked: {}", e)),
            }
        }
        _ = tokio::signal::ctrl_c() => {
            println!("\nShutdown signal received");
            let position = bot.position.read().await;
            if position.base_amount > 0 {
                println!("Closing position before shutdown...");
                drop(position);
                let market_data = bot.fetch_market_data().await?;
                bot.close_position(&market_data).await?;
            }
            Ok(())
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();
    
    let rpc_url = std::env::var("SOLANA_RPC_URL")
        .unwrap_or_else(|_| "https://api.mainnet-beta.solana.com".to_string());
    
    let private_key = std::env::var("WALLET_PRIVATE_KEY")
        .context("WALLET_PRIVATE_KEY environment variable required")?;
    
    let amm_pool_id = std::env::var("RAYDIUM_AMM_ID")
        .context("RAYDIUM_AMM_ID environment variable required")?;
    
    let wallet_bytes = bs58::decode(private_key)
        .into_vec()
        .context("Invalid base58 private key")?;
    
    println!("Starting Solana HFT Bot with LSTM predictions...");
    println!("RPC: {}", rpc_url);
    println!("AMM Pool: {}", amm_pool_id);
    
    run_hft_bot(&rpc_url, &wallet_bytes, &amm_pool_id).await
}
