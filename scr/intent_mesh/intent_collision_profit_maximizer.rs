use std::sync::Arc;
use std::collections::{HashMap, BinaryHeap, VecDeque};
use std::cmp::Ordering;
use tokio::sync::{RwLock, Semaphore};
use solana_sdk::{
    pubkey::Pubkey,
    signature::{Keypair, Signer},
    transaction::Transaction,
    instruction::{Instruction, AccountMeta},
    compute_budget::ComputeBudgetInstruction,
    hash::Hash,
    system_instruction,
};
use solana_client::nonblocking::rpc_client::RpcClient;
use serde::{Serialize, Deserialize};
use rust_decimal::prelude::*;
use borsh::{BorshSerialize, BorshDeserialize};

const MAX_COMPUTE_UNITS: u32 = 1_400_000;
const PRIORITY_FEE_CACHE_SIZE: usize = 200;
const MIN_PROFIT_BPS: u64 = 15;
const JITO_TIP_BPS: u64 = 80;
const MAX_BUNDLE_AGE_SLOTS: u64 = 2;
const COLLISION_WINDOW_MS: u64 = 25;
const STATE_COMPRESSION_INTERVAL: u64 = 100;

const RAYDIUM_V4: &str = "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8";
const ORCA_WHIRLPOOL: &str = "whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc";
const JUPITER_V6: &str = "JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4";
const OPENBOOK_V2: &str = "opnb2LAfJYbRMAHHvqjCwQxanZn7ReEHp1k81EohpZb";
const PHOENIX: &str = "PhoeNiXZ8ByJGLkxNfZRnkUfjvmuYqLR89jjFHGqdXY";

#[derive(Debug, Clone)]
pub struct IntentCollision {
    pub intent_a: Arc<Intent>,
    pub intent_b: Arc<Intent>,
    pub collision_type: CollisionType,
    pub profit_opportunity: u64,
    pub execution_priority: f64,
    pub slot: u64,
    pub confidence: f64,
}

#[derive(Debug, Clone)]
pub struct Intent {
    pub signature: String,
    pub program_id: Pubkey,
    pub accounts: Vec<AccountMeta>,
    pub data: Vec<u8>,
    pub compute_units: u32,
    pub priority_fee: u64,
    pub sender: Pubkey,
    pub value: u64,
    pub intent_type: IntentType,
    pub slot: u64,
    pub market_state: MarketState,
}

#[derive(Debug, Clone)]
pub struct MarketState {
    pub base_reserves: u64,
    pub quote_reserves: u64,
    pub fee_bps: u16,
    pub sqrt_price: u128,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum IntentType {
    Swap { input_mint: Pubkey, output_mint: Pubkey },
    AddLiquidity { pool: Pubkey },
    RemoveLiquidity { pool: Pubkey },
    FlashLoan { amount: u64 },
    Liquidation { account: Pubkey },
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum CollisionType {
    Sandwich { price_impact_bps: u16 },
    Backrun { expected_slippage_bps: u16 },
    LiquidationRace { collateral_value: u64 },
    ArbitrageCompetition { profit_bps: u16 },
    JitLiquidity { pool: Pubkey },
}

impl Ord for IntentCollision {
    fn cmp(&self, other: &Self) -> Ordering {
        let profit_per_cu_self = (self.profit_opportunity as f64) / 
            ((self.intent_a.compute_units + self.intent_b.compute_units) as f64);
        let profit_per_cu_other = (other.profit_opportunity as f64) / 
            ((other.intent_a.compute_units + other.intent_b.compute_units) as f64);
        
        profit_per_cu_self.partial_cmp(&profit_per_cu_other)
            .unwrap_or(Ordering::Equal)
            .then_with(|| self.confidence.partial_cmp(&other.confidence).unwrap_or(Ordering::Equal))
    }
}

impl PartialOrd for IntentCollision {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Eq for IntentCollision {}

impl PartialEq for IntentCollision {
    fn eq(&self, other: &Self) -> bool {
        self.profit_opportunity == other.profit_opportunity && 
        self.slot == other.slot
    }
}

pub struct IntentCollisionProfitMaximizer {
    rpc_client: Arc<RpcClient>,
    keypair: Arc<Keypair>,
    intent_pool: Arc<RwLock<HashMap<String, Arc<Intent>>>>,
    collision_heap: Arc<RwLock<BinaryHeap<IntentCollision>>>,
    market_cache: Arc<RwLock<HashMap<Pubkey, MarketState>>>,
    priority_fee_tracker: Arc<RwLock<VecDeque<u64>>>,
    execution_semaphore: Arc<Semaphore>,
    jito_client: Arc<JitoClient>,
    current_slot: Arc<RwLock<u64>>,
}

struct JitoClient {
    endpoint: String,
    auth_header: String,
    client: reqwest::Client,
}

impl IntentCollisionProfitMaximizer {
    pub async fn new(
        rpc_url: &str,
        keypair: Keypair,
        jito_endpoint: &str,
        jito_auth: &str,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let rpc_client = Arc::new(RpcClient::new(rpc_url.to_string()));
        let jito_client = Arc::new(JitoClient {
            endpoint: jito_endpoint.to_string(),
            auth_header: format!("Bearer {}", jito_auth),
            client: reqwest::Client::builder()
                .timeout(std::time::Duration::from_millis(2000))
                .build()?,
        });
        
        Ok(Self {
            rpc_client,
            keypair: Arc::new(keypair),
            intent_pool: Arc::new(RwLock::new(HashMap::with_capacity(10000))),
            collision_heap: Arc::new(RwLock::new(BinaryHeap::with_capacity(1000))),
            market_cache: Arc::new(RwLock::new(HashMap::with_capacity(100))),
            priority_fee_tracker: Arc::new(RwLock::new(VecDeque::with_capacity(PRIORITY_FEE_CACHE_SIZE))),
            execution_semaphore: Arc::new(Semaphore::new(10)),
            jito_client,
            current_slot: Arc::new(RwLock::new(0)),
        })
    }

    pub async fn analyze_intent(&self, tx: &Transaction, slot: u64) -> Result<Intent, Box<dyn std::error::Error>> {
        let instructions = &tx.message.instructions;
        if instructions.is_empty() {
            return Err("No instructions found".into());
        }

        let primary_ix = &instructions[0];
        let program_id = tx.message.account_keys[primary_ix.program_id_index as usize];
        
        let (intent_type, value) = self.decode_instruction(&program_id, &primary_ix.data, &tx.message.account_keys).await?;
        let compute_units = self.extract_compute_units(instructions);
        let priority_fee = self.extract_priority_fee_micro_lamports(instructions);
        
        let market_state = self.get_market_state(&program_id, &tx.message.account_keys).await?;

        Ok(Intent {
            signature: bs58::encode(&tx.signatures[0]).into_string(),
            program_id,
            accounts: primary_ix.accounts.iter().map(|&idx| {
                AccountMeta {
                    pubkey: tx.message.account_keys[idx as usize],
                    is_signer: tx.message.is_signer(idx as usize),
                    is_writable: tx.message.is_writable(idx as usize),
                }
            }).collect(),
            data: primary_ix.data.clone(),
            compute_units,
            priority_fee,
            sender: tx.message.account_keys[0],
            value,
            intent_type,
            slot,
            market_state,
        })
    }

    async fn decode_instruction(
        &self,
        program_id: &Pubkey,
        data: &[u8],
        account_keys: &[Pubkey],
    ) -> Result<(IntentType, u64), Box<dyn std::error::Error>> {
        if data.len() < 8 {
            return Err("Invalid instruction data".into());
        }

        let discriminator = &data[0..8];
        
        match program_id.to_string().as_str() {
            RAYDIUM_V4 => {
                match discriminator {
                    [0x09, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00] => {
                        let amount = u64::from_le_bytes(data[16..24].try_into()?);
                        let input_mint = account_keys[2];
                        let output_mint = account_keys[3];
                        Ok((IntentType::Swap { input_mint, output_mint }, amount))
                    }
                    [0x04, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00] => {
                        let amount = u64::from_le_bytes(data[8..16].try_into()?);
                        Ok((IntentType::AddLiquidity { pool: account_keys[1] }, amount))
                    }
                    _ => Ok((IntentType::Swap { 
                        input_mint: Pubkey::default(), 
                        output_mint: Pubkey::default() 
                    }, 0))
                }
            }
            ORCA_WHIRLPOOL => {
                match discriminator {
                    [0xf8, 0xc6, 0x9e, 0x91, 0xe4, 0x87, 0xc9, 0x79] => {
                        let amount = u64::from_le_bytes(data[8..16].try_into()?);
                        let input_mint = account_keys[2];
                        let output_mint = account_keys[3];
                        Ok((IntentType::Swap { input_mint, output_mint }, amount))
                    }
                    _ => Ok((IntentType::Swap { 
                        input_mint: Pubkey::default(), 
                        output_mint: Pubkey::default() 
                    }, 0))
                }
            }
            _ => {
                let amount = if data.len() >= 16 {
                    u64::from_le_bytes(data[8..16].try_into()?)
                } else {
                    0
                };
                Ok((IntentType::Swap { 
                    input_mint: Pubkey::default(), 
                    output_mint: Pubkey::default() 
                }, amount))
            }
        }
    }

    fn extract_compute_units(&self, instructions: &[Instruction]) -> u32 {
        for ix in instructions {
            if ix.program_id == solana_sdk::compute_budget::id() && ix.data.len() >= 5 {
                if ix.data[0] == 2 {
                    return u32::from_le_bytes(ix.data[1..5].try_into().unwrap_or([0; 4]));
                }
            }
        }
        200_000
    }

    fn extract_priority_fee_micro_lamports(&self, instructions: &[Instruction]) -> u64 {
        for ix in instructions {
            if ix.program_id == solana_sdk::compute_budget::id() && ix.data.len() >= 9 {
                if ix.data[0] == 3 {
                    return u64::from_le_bytes(ix.data[1..9].try_into().unwrap_or([0; 8]));
                }
            }
        }
        1000
    }

    async fn get_market_state(
        &self,
        program_id: &Pubkey,
        account_keys: &[Pubkey],
    ) -> Result<MarketState, Box<dyn std::error::Error>> {
        let cache = self.market_cache.read().await;
        
        let pool_key = if account_keys.len() > 1 { account_keys[1] } else { *program_id };
        
        if let Some(state) = cache.get(&pool_key) {
            return Ok(state.clone());
        }
        
        Ok(MarketState {
            base_reserves: 1_000_000_000_000,
            quote_reserves: 1_000_000_000_000,
            fee_bps: 30,
            sqrt_price: 1_000_000_000_000,
        })
    }

    pub async fn detect_collisions(&self) -> Result<Vec<IntentCollision>, Box<dyn std::error::Error>> {
        let intent_pool = self.intent_pool.read().await;
        let current_slot = *self.current_slot.read().await;
        let mut collisions = Vec::with_capacity(100);

        for (sig_a, intent_a) in intent_pool.iter() {
            if current_slot - intent_a.slot > MAX_BUNDLE_AGE_SLOTS {
                continue;
            }

            for (sig_b, intent_b) in intent_pool.iter() {
                if sig_a >= sig_b || current_slot - intent_b.slot > MAX_BUNDLE_AGE_SLOTS {
                    continue;
                }

                                if let Some(collision) = self.analyze_collision_opportunity(
                    intent_a.clone(),
                    intent_b.clone(),
                    current_slot
                ).await? {
                    collisions.push(collision);
                }
            }
        }

        collisions.sort_by(|a, b| b.cmp(a));
        Ok(collisions.into_iter().take(20).collect())
    }

    async fn analyze_collision_opportunity(
        &self,
        intent_a: Arc<Intent>,
        intent_b: Arc<Intent>,
        slot: u64,
    ) -> Result<Option<IntentCollision>, Box<dyn std::error::Error>> {
        match (&intent_a.intent_type, &intent_b.intent_type) {
            (IntentType::Swap { input_mint: input_a, output_mint: output_a }, 
             IntentType::Swap { input_mint: input_b, output_mint: output_b }) => {
                
                if input_a == output_b && output_a == input_b {
                    return self.analyze_arbitrage_collision(intent_a, intent_b, slot).await;
                }
                
                if (input_a == input_b || output_a == output_b) && 
                   intent_a.value > 50_000_000_000 {
                    return self.analyze_sandwich_collision(intent_a, intent_b, slot).await;
                }
                
                if output_a == input_b {
                    return self.analyze_backrun_collision(intent_a, intent_b, slot).await;
                }
            }
            (IntentType::Liquidation { account }, IntentType::Liquidation { account: account2 }) => {
                if account == account2 {
                    return self.analyze_liquidation_race(intent_a, intent_b, slot).await;
                }
            }
            (IntentType::AddLiquidity { pool }, IntentType::Swap { .. }) |
            (IntentType::Swap { .. }, IntentType::AddLiquidity { pool }) => {
                return self.analyze_jit_liquidity(intent_a, intent_b, *pool, slot).await;
            }
            _ => {}
        }

        Ok(None)
    }

    async fn analyze_sandwich_collision(
        &self,
        victim: Arc<Intent>,
        _companion: Arc<Intent>,
        slot: u64,
    ) -> Result<Option<IntentCollision>, Box<dyn std::error::Error>> {
        let price_impact_bps = self.calculate_price_impact_bps(&victim).await?;
        
        if price_impact_bps < 50 {
            return Ok(None);
        }

        let sandwich_profit = self.calculate_sandwich_profit(&victim, price_impact_bps).await?;
        let execution_cost = victim.priority_fee * 3 + 10_000;
        
        if sandwich_profit <= execution_cost {
            return Ok(None);
        }

        let confidence = self.calculate_sandwich_confidence(&victim, price_impact_bps);

        Ok(Some(IntentCollision {
            intent_a: victim.clone(),
            intent_b: victim,
            collision_type: CollisionType::Sandwich { price_impact_bps },
            profit_opportunity: sandwich_profit - execution_cost,
            execution_priority: (price_impact_bps as f64) * confidence,
            slot,
            confidence,
        }))
    }

    async fn calculate_price_impact_bps(&self, intent: &Intent) -> Result<u16, Box<dyn std::error::Error>> {
        let state = &intent.market_state;
        let constant_product = state.base_reserves as u128 * state.quote_reserves as u128;
        
        let new_base = state.base_reserves as u128 + intent.value as u128;
        let new_quote = constant_product / new_base;
        let quote_out = state.quote_reserves as u128 - new_quote;
        
        let ideal_quote = (intent.value as u128 * state.quote_reserves as u128) / state.base_reserves as u128;
        let slippage = ((ideal_quote - quote_out) * 10000) / ideal_quote;
        
        Ok(slippage.min(9999) as u16)
    }

    async fn calculate_sandwich_profit(
        &self,
        victim: &Intent,
        price_impact_bps: u16,
    ) -> Result<u64, Box<dyn std::error::Error>> {
        let state = &victim.market_state;
        let optimal_frontrun = (victim.value * price_impact_bps as u64) / 20000;
        
        let k = state.base_reserves as u128 * state.quote_reserves as u128;
        
        let base_after_front = state.base_reserves + optimal_frontrun;
        let quote_after_front = (k / base_after_front as u128) as u64;
        let front_output = state.quote_reserves - quote_after_front;
        
        let base_after_victim = base_after_front + victim.value;
        let quote_after_victim = (k / base_after_victim as u128) as u64;
        
        let backrun_input = front_output;
        let quote_after_back = quote_after_victim + backrun_input;
        let base_after_back = (k / quote_after_back as u128) as u64;
        let backrun_output = base_after_victim - base_after_back;
        
        Ok(backrun_output.saturating_sub(optimal_frontrun))
    }

    fn calculate_sandwich_confidence(&self, victim: &Intent, price_impact_bps: u16) -> f64 {
        let base_confidence = 0.7;
        let impact_factor = (price_impact_bps as f64 / 1000.0).min(0.2);
        let priority_factor = (victim.priority_fee as f64 / 10_000_000.0).min(0.1);
        
        base_confidence + impact_factor + priority_factor
    }

    async fn analyze_backrun_collision(
        &self,
        target: Arc<Intent>,
        backrun: Arc<Intent>,
        slot: u64,
    ) -> Result<Option<IntentCollision>, Box<dyn std::error::Error>> {
        let expected_slippage_bps = self.calculate_price_impact_bps(&target).await?;
        
        if expected_slippage_bps < 30 {
            return Ok(None);
        }

        let backrun_profit = (target.value * expected_slippage_bps as u64) / 20000;
        let execution_cost = target.priority_fee + backrun.priority_fee + 5_000;
        
        if backrun_profit <= execution_cost {
            return Ok(None);
        }

        Ok(Some(IntentCollision {
            intent_a: target,
            intent_b: backrun,
            collision_type: CollisionType::Backrun { expected_slippage_bps },
            profit_opportunity: backrun_profit - execution_cost,
            execution_priority: expected_slippage_bps as f64,
            slot,
            confidence: 0.85,
        }))
    }

    async fn analyze_arbitrage_collision(
        &self,
        intent_a: Arc<Intent>,
        intent_b: Arc<Intent>,
        slot: u64,
    ) -> Result<Option<IntentCollision>, Box<dyn std::error::Error>> {
        let combined_volume = intent_a.value + intent_b.value;
        let profit_bps = 50u16;
        let arb_profit = (combined_volume * profit_bps as u64) / 10000;
        
        let execution_cost = intent_a.priority_fee.max(intent_b.priority_fee) + 10_000;
        
        if arb_profit <= execution_cost {
            return Ok(None);
        }

        Ok(Some(IntentCollision {
            intent_a,
            intent_b,
            collision_type: CollisionType::ArbitrageCompetition { profit_bps },
            profit_opportunity: arb_profit - execution_cost,
            execution_priority: profit_bps as f64 * 2.0,
            slot,
            confidence: 0.9,
        }))
    }

    async fn analyze_liquidation_race(
        &self,
        intent_a: Arc<Intent>,
        intent_b: Arc<Intent>,
        slot: u64,
    ) -> Result<Option<IntentCollision>, Box<dyn std::error::Error>> {
        let collateral_value = intent_a.value.max(intent_b.value);
        let liquidation_bonus = (collateral_value * 5) / 100;
        
        let winner_fee = intent_a.priority_fee.max(intent_b.priority_fee);
        let profit = liquidation_bonus.saturating_sub(winner_fee + 20_000);
        
        Ok(Some(IntentCollision {
            intent_a,
            intent_b,
            collision_type: CollisionType::LiquidationRace { collateral_value },
            profit_opportunity: profit,
            execution_priority: 1000.0,
            slot,
            confidence: 0.95,
        }))
    }

    async fn analyze_jit_liquidity(
        &self,
        swap_intent: Arc<Intent>,
        lp_intent: Arc<Intent>,
        pool: Pubkey,
        slot: u64,
    ) -> Result<Option<IntentCollision>, Box<dyn std::error::Error>> {
        let fee_share = (swap_intent.value * 25) / 10000;
        let execution_cost = swap_intent.priority_fee + lp_intent.priority_fee + 15_000;
        
        if fee_share <= execution_cost {
            return Ok(None);
        }

        Ok(Some(IntentCollision {
            intent_a: swap_intent,
            intent_b: lp_intent,
            collision_type: CollisionType::JitLiquidity { pool },
            profit_opportunity: fee_share - execution_cost,
            execution_priority: 500.0,
            slot,
            confidence: 0.75,
        }))
    }

    pub async fn build_optimal_bundle(
        &self,
        collision: &IntentCollision,
    ) -> Result<Vec<Instruction>, Box<dyn std::error::Error>> {
        let mut instructions = Vec::with_capacity(6);
        
        let total_compute = collision.intent_a.compute_units + collision.intent_b.compute_units + 100_000;
        instructions.push(ComputeBudgetInstruction::set_compute_unit_limit(
            total_compute.min(MAX_COMPUTE_UNITS)
        ));

        let optimal_priority = self.calculate_competitive_priority_fee(&collision).await?;
        instructions.push(ComputeBudgetInstruction::set_compute_unit_price(optimal_priority));

        match collision.collision_type {
            CollisionType::Sandwich { price_impact_bps } => {
                self.build_sandwich_bundle(&mut instructions, collision, price_impact_bps).await?;
            }
            CollisionType::Backrun { .. } => {
                self.build_backrun_bundle(&mut instructions, collision).await?;
            }
            CollisionType::ArbitrageCompetition { .. } => {
                self.build_arbitrage_bundle(&mut instructions, collision).await?;
            }
            CollisionType::LiquidationRace { .. } => {
                self.build_liquidation_bundle(&mut instructions, collision).await?;
            }
            CollisionType::JitLiquidity { pool } => {
                self.build_jit_bundle(&mut instructions, collision, pool).await?;
            }
        }

        let tip_amount = (collision.profit_opportunity * JITO_TIP_BPS) / 10000;
        instructions.push(self.create_jito_tip_instruction(tip_amount.max(10_000)).await?);

        Ok(instructions)
    }

    async fn calculate_competitive_priority_fee(&self, collision: &IntentCollision) -> Result<u64, Box<dyn std::error::Error>> {
        let fees = self.priority_fee_tracker.read().await;
        
        let base_fee = if fees.is_empty() {
            1_000_000
        } else {
            let mut sorted: Vec<u64> = fees.iter().cloned().collect();
            sorted.sort_unstable();
            sorted[sorted.len() * 95 / 100]
        };

        let competitive_multiplier = match collision.collision_type {
            CollisionType::Sandwich { .. } => 1.5,
            CollisionType::LiquidationRace { .. } => 2.0,
            CollisionType::ArbitrageCompetition { .. } => 1.8,
            _ => 1.2,
        };

        let max_fee = (collision.profit_opportunity * 100) / total_compute_units;
        let competitive_fee = (base_fee as f64 * competitive_multiplier) as u64;
        
        Ok(competitive_fee.min(max_fee).min(100_000_000))
    }

    async fn build_sandwich_bundle(
        &self,
        instructions: &mut Vec<Instruction>,
        collision: &IntentCollision,
        price_impact_bps: u16,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let frontrun_amount = (collision.intent_a.value * price_impact_bps as u64) / 20000;
        
        let mut frontrun_data = vec![0x09, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00];
        frontrun_data.extend_from_slice(&frontrun_amount.to_le_bytes());
        frontrun_data.extend_from_slice(&0u64.to_le_bytes());
        
        instructions.push(Instruction {
            program_id: collision.intent_a.program_id,
            accounts: collision.intent_a.accounts.clone(),
            data: frontrun_data,
        });

        instructions.push(Instruction {
            program_id: collision.intent_a.program_id,
            accounts: collision.intent_a.accounts.clone(),
            data: collision.intent_a.data.clone(),
        });

        let mut backrun_accounts = collision.intent_a.accounts.clone();
        backrun_accounts[2..4].reverse();
        
        let mut backrun_data = vec![0x09, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00];
        backrun_data.extend_from_slice(&u64::MAX.to_le_bytes());
        backrun_data.extend_from_slice(&0u64.to_le_bytes());
        
        instructions.push(Instruction {
            program_id: collision.intent_a.program_id,
            accounts: backrun_accounts,
            data: backrun_data,
        });

        Ok(())
    }

        async fn build_backrun_bundle(
        &self,
        instructions: &mut Vec<Instruction>,
        collision: &IntentCollision,
    ) -> Result<(), Box<dyn std::error::Error>> {
        instructions.push(Instruction {
            program_id: collision.intent_a.program_id,
            accounts: collision.intent_a.accounts.clone(),
            data: collision.intent_a.data.clone(),
        });

        let mut backrun_data = match collision.intent_a.program_id.to_string().as_str() {
            RAYDIUM_V4 => {
                let mut data = vec![0x09, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00];
                data.extend_from_slice(&collision.intent_b.value.to_le_bytes());
                data.extend_from_slice(&1u64.to_le_bytes());
                data
            }
            ORCA_WHIRLPOOL => {
                let mut data = vec![0xf8, 0xc6, 0x9e, 0x91, 0xe4, 0x87, 0xc9, 0x79];
                data.extend_from_slice(&collision.intent_b.value.to_le_bytes());
                data.extend_from_slice(&0u64.to_le_bytes());
                data.extend_from_slice(&u128::MAX.to_le_bytes());
                data
            }
            _ => collision.intent_b.data.clone()
        };

        instructions.push(Instruction {
            program_id: collision.intent_b.program_id,
            accounts: collision.intent_b.accounts.clone(),
            data: backrun_data,
        });

        Ok(())
    }

    async fn build_arbitrage_bundle(
        &self,
        instructions: &mut Vec<Instruction>,
        collision: &IntentCollision,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let optimal_route = self.calculate_optimal_arbitrage_route(collision).await?;
        
        for (i, ix_data) in optimal_route.iter().enumerate() {
            let accounts = if i == 0 {
                collision.intent_a.accounts.clone()
            } else {
                collision.intent_b.accounts.clone()
            };
            
            instructions.push(Instruction {
                program_id: if i == 0 { collision.intent_a.program_id } else { collision.intent_b.program_id },
                accounts,
                data: ix_data.clone(),
            });
        }

        Ok(())
    }

    async fn calculate_optimal_arbitrage_route(&self, collision: &IntentCollision) -> Result<Vec<Vec<u8>>, Box<dyn std::error::Error>> {
        let mut route = Vec::with_capacity(2);
        
        let amount = collision.intent_a.value.max(collision.intent_b.value);
        let mut data1 = vec![0x09, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00];
        data1.extend_from_slice(&amount.to_le_bytes());
        data1.extend_from_slice(&1u64.to_le_bytes());
        route.push(data1);
        
        let mut data2 = vec![0x09, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00];
        data2.extend_from_slice(&u64::MAX.to_le_bytes());
        data2.extend_from_slice(&0u64.to_le_bytes());
        route.push(data2);
        
        Ok(route)
    }

    async fn build_liquidation_bundle(
        &self,
        instructions: &mut Vec<Instruction>,
        collision: &IntentCollision,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let stronger_intent = if collision.intent_a.priority_fee > collision.intent_b.priority_fee {
            &collision.intent_a
        } else {
            &collision.intent_b
        };

        instructions.push(Instruction {
            program_id: stronger_intent.program_id,
            accounts: stronger_intent.accounts.clone(),
            data: stronger_intent.data.clone(),
        });

        Ok(())
    }

    async fn build_jit_bundle(
        &self,
        instructions: &mut Vec<Instruction>,
        collision: &IntentCollision,
        pool: Pubkey,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let (swap_intent, lp_intent) = match collision.intent_a.intent_type {
            IntentType::Swap { .. } => (&collision.intent_a, &collision.intent_b),
            _ => (&collision.intent_b, &collision.intent_a),
        };

        instructions.push(Instruction {
            program_id: lp_intent.program_id,
            accounts: lp_intent.accounts.clone(),
            data: lp_intent.data.clone(),
        });

        instructions.push(Instruction {
            program_id: swap_intent.program_id,
            accounts: swap_intent.accounts.clone(),
            data: swap_intent.data.clone(),
        });

        let mut remove_liq_data = vec![0x05, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00];
        remove_liq_data.extend_from_slice(&u64::MAX.to_le_bytes());
        
        instructions.push(Instruction {
            program_id: lp_intent.program_id,
            accounts: lp_intent.accounts.clone(),
            data: remove_liq_data,
        });

        Ok(())
    }

    async fn create_jito_tip_instruction(&self, tip_amount: u64) -> Result<Instruction, Box<dyn std::error::Error>> {
        const TIP_ACCOUNTS: [&str; 8] = [
            "96gYZGLnJYVFmbjzopPSU6QiEV5fGqZNyN9nmNhvrZU5",
            "HFqU5x63VTqvQss8hp11i4wVV8bD44PvwucfZ2bU7gRe",
            "Cw8CFyM9FkoMi7K7Crf6HNQqf4uEMzpKw6QNghXLvLkY",
            "ADaUMid9yfUytqMBgopwjb2DTLSokTSzL1zt6iGPaS49",
            "DfXygSm4jCyNCybVYYK6DwvWqjKee8pbDmJGcLWNDXjh",
            "ADuUkR4vqLUMWXxW9gh6D6L8pMSawimctcNZ5pGwDcEt",
            "DttWaMuVvTiduZRnguLF7jNxTgiMBZ1hyAumKUiL2KRL",
            "3AVi9Tg9Uo68tJfuvoKvqKNWKkC5wPdSSdeBnizKZ6jT",
        ];
        
        let slot = *self.current_slot.read().await;
        let tip_account_index = (slot as usize) % TIP_ACCOUNTS.len();
        let tip_account = Pubkey::from_str(TIP_ACCOUNTS[tip_account_index])?;
        
        Ok(system_instruction::transfer(
            &self.keypair.pubkey(),
            &tip_account,
            tip_amount,
        ))
    }

    pub async fn execute_bundle(
        &self,
        instructions: Vec<Instruction>,
        recent_blockhash: Hash,
    ) -> Result<String, Box<dyn std::error::Error>> {
        let _permit = self.execution_semaphore.acquire().await?;
        
        let message = solana_sdk::message::Message::new_with_blockhash(
            &instructions,
            Some(&self.keypair.pubkey()),
            &recent_blockhash,
        );
        
        let tx = Transaction::new(&[&*self.keypair], message, recent_blockhash);
        let serialized = bincode::serialize(&tx)?;
        
        let bundle_result = self.jito_client.send_bundle(vec![serialized]).await?;
        
        Ok(bundle_result)
    }

    pub async fn start_monitoring(&self) -> Result<(), Box<dyn std::error::Error>> {
        let monitoring_tasks = vec![
            tokio::spawn(self.clone().monitor_slot_updates()),
            tokio::spawn(self.clone().process_collision_heap()),
            tokio::spawn(self.clone().cleanup_stale_data()),
            tokio::spawn(self.clone().update_priority_fees()),
        ];

        futures::future::try_join_all(monitoring_tasks).await?;
        Ok(())
    }

    async fn monitor_slot_updates(self) -> Result<(), Box<dyn std::error::Error>> {
        let mut slot_subscription = self.rpc_client.slot_subscribe().await?;
        
        while let Some(slot_info) = slot_subscription.next().await {
            *self.current_slot.write().await = slot_info.slot;
            
            let pool = self.intent_pool.read().await;
            let stale_intents: Vec<String> = pool.iter()
                .filter(|(_, intent)| slot_info.slot - intent.slot > MAX_BUNDLE_AGE_SLOTS)
                .map(|(sig, _)| sig.clone())
                .collect();
            drop(pool);
            
            if !stale_intents.is_empty() {
                let mut pool = self.intent_pool.write().await;
                for sig in stale_intents {
                    pool.remove(&sig);
                }
            }
        }
        
        Ok(())
    }

    async fn process_collision_heap(self) -> Result<(), Box<dyn std::error::Error>> {
        loop {
            let collision = {
                let mut heap = self.collision_heap.write().await;
                heap.pop()
            };

            if let Some(collision) = collision {
                let current_slot = *self.current_slot.read().await;
                
                if current_slot - collision.slot > MAX_BUNDLE_AGE_SLOTS {
                    continue;
                }

                if collision.profit_opportunity < (MIN_PROFIT_BPS * collision.intent_a.value) / 10000 {
                    continue;
                }

                match self.process_collision(collision).await {
                    Ok(signature) => {
                        log::info!("Bundle submitted: {}", signature);
                    }
                    Err(e) => {
                        log::error!("Bundle submission failed: {:?}", e);
                    }
                }
            } else {
                tokio::time::sleep(tokio::time::Duration::from_millis(5)).await;
            }
        }
    }

    async fn process_collision(&self, collision: IntentCollision) -> Result<String, Box<dyn std::error::Error>> {
        let recent_blockhash = self.rpc_client.get_latest_blockhash().await?;
        let instructions = self.build_optimal_bundle(&collision).await?;
        
        let simulation_result = self.simulate_bundle(&instructions, recent_blockhash).await?;
        if simulation_result.err.is_some() {
            return Err(format!("Simulation failed: {:?}", simulation_result.err).into());
        }

        let signature = self.execute_bundle(instructions, recent_blockhash).await?;
        
        self.remove_processed_intents(&collision).await?;
        
        Ok(signature)
    }

    async fn simulate_bundle(
        &self,
        instructions: &[Instruction],
        blockhash: Hash,
    ) -> Result<solana_client::rpc_response::RpcSimulateTransactionResult, Box<dyn std::error::Error>> {
        let message = solana_sdk::message::Message::new_with_blockhash(
            instructions,
            Some(&self.keypair.pubkey()),
            &blockhash,
        );
        
        let tx = Transaction::new(&[&*self.keypair], message, blockhash);
        
        let config = solana_client::rpc_config::RpcSimulateTransactionConfig {
            sig_verify: false,
            replace_recent_blockhash: true,
            commitment: Some(solana_sdk::commitment_config::CommitmentConfig::processed()),
            encoding: Some(solana_transaction_status::UiTransactionEncoding::Base64),
            accounts: None,
            min_context_slot: None,
            inner_instructions: false,
        };

        self.rpc_client.simulate_transaction_with_config(&tx, config).await
    }

    async fn remove_processed_intents(&self, collision: &IntentCollision) -> Result<(), Box<dyn std::error::Error>> {
        let mut pool = self.intent_pool.write().await;
        pool.remove(&collision.intent_a.signature);
        pool.remove(&collision.intent_b.signature);
        Ok(())
    }

    async fn cleanup_stale_data(self) -> Result<(), Box<dyn std::error::Error>> {
        let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(30));
        
        loop {
            interval.tick().await;
            
            let current_slot = *self.current_slot.read().await;
            
            let mut pool = self.intent_pool.write().await;
            pool.retain(|_, intent| current_slot - intent.slot <= MAX_BUNDLE_AGE_SLOTS * 2);
            
            if pool.len() > 10000 {
                let mut entries: Vec<(String, u64)> = pool.iter()
                    .map(|(sig, intent)| (sig.clone(), intent.slot))
                    .collect();
                entries.sort_by_key(|(_, slot)| *slot);
                
                for (sig, _) in entries.iter().take(1000) {
                    pool.remove(sig);
                }
            }
        }
    }

    async fn update_priority_fees(self) -> Result<(), Box<dyn std::error::Error>> {
        let mut interval = tokio::time::interval(tokio::time::Duration::from_millis(500));
        
        loop {
            interval.tick().await;
            
                            let mut tracker = self.priority_fee_tracker.write().await;
                
                for fee_info in recent_fees {
                    if fee_info.prioritization_fee > 0 {
                        tracker.push_back(fee_info.prioritization_fee);
                        
                        if tracker.len() > PRIORITY_FEE_CACHE_SIZE {
                            tracker.pop_front();
                        }
                    }
                }
            }
        }
    }

    pub async fn add_intent(&self, intent: Intent) -> Result<(), Box<dyn std::error::Error>> {
        let signature = intent.signature.clone();
        let mut pool = self.intent_pool.write().await;
        
        pool.insert(signature, Arc::new(intent));
        
        if pool.len() % STATE_COMPRESSION_INTERVAL == 0 {
            drop(pool);
            tokio::spawn(self.clone().detect_and_queue_collisions());
        }
        
        Ok(())
    }

    async fn detect_and_queue_collisions(self) -> Result<(), Box<dyn std::error::Error>> {
        let collisions = self.detect_collisions().await?;
        let mut heap = self.collision_heap.write().await;
        
        for collision in collisions {
            if collision.profit_opportunity > 0 {
                heap.push(collision);
            }
        }
        
        while heap.len() > 1000 {
            heap.pop();
        }
        
        Ok(())
    }

    pub async fn update_market_state(&self, pool: Pubkey, state: MarketState) -> Result<(), Box<dyn std::error::Error>> {
        let mut cache = self.market_cache.write().await;
        cache.insert(pool, state);
        
        if cache.len() > 100 {
            let oldest_keys: Vec<Pubkey> = cache.keys()
                .take(10)
                .cloned()
                .collect();
            
            for key in oldest_keys {
                cache.remove(&key);
            }
        }
        
        Ok(())
    }

    pub async fn get_performance_metrics(&self) -> PerformanceMetrics {
        let pool_size = self.intent_pool.read().await.len();
        let heap_size = self.collision_heap.read().await.len();
        let priority_fees = self.priority_fee_tracker.read().await;
        
        let avg_priority_fee = if priority_fees.is_empty() {
            0
        } else {
            priority_fees.iter().sum::<u64>() / priority_fees.len() as u64
        };
        
        PerformanceMetrics {
            intents_in_pool: pool_size,
            collisions_pending: heap_size,
            average_priority_fee: avg_priority_fee,
            current_slot: *self.current_slot.read().await,
        }
    }
}

impl JitoClient {
    async fn send_bundle(&self, transactions: Vec<Vec<u8>>) -> Result<String, Box<dyn std::error::Error>> {
        #[derive(Serialize)]
        struct BundleRequest {
            jsonrpc: String,
            id: u64,
            method: String,
            params: Vec<Vec<String>>,
        }

        let encoded_txs: Vec<String> = transactions.iter()
            .map(|tx| base64::encode(tx))
            .collect();

        let request = BundleRequest {
            jsonrpc: "2.0".to_string(),
            id: 1,
            method: "sendBundle".to_string(),
            params: vec![encoded_txs],
        };

        let response = self.client
            .post(&self.endpoint)
            .header("Content-Type", "application/json")
            .header("Authorization", &self.auth_header)
            .json(&request)
            .send()
            .await?;

        if !response.status().is_success() {
            return Err(format!("Jito request failed: {}", response.status()).into());
        }

        #[derive(Deserialize)]
        struct BundleResponse {
            result: Option<String>,
            error: Option<serde_json::Value>,
        }

        let bundle_response: BundleResponse = response.json().await?;
        
        if let Some(error) = bundle_response.error {
            return Err(format!("Jito error: {:?}", error).into());
        }
        
        bundle_response.result.ok_or_else(|| "No bundle ID returned".into())
    }
}

#[derive(Debug, Clone)]
pub struct PerformanceMetrics {
    pub intents_in_pool: usize,
    pub collisions_pending: usize,
    pub average_priority_fee: u64,
    pub current_slot: u64,
}

impl Clone for IntentCollisionProfitMaximizer {
    fn clone(&self) -> Self {
        Self {
            rpc_client: self.rpc_client.clone(),
            keypair: self.keypair.clone(),
            intent_pool: self.intent_pool.clone(),
            collision_heap: self.collision_heap.clone(),
            market_cache: self.market_cache.clone(),
            priority_fee_tracker: self.priority_fee_tracker.clone(),
            execution_semaphore: self.execution_semaphore.clone(),
            jito_client: self.jito_client.clone(),
            current_slot: self.current_slot.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use solana_sdk::signature::Keypair;

    #[tokio::test]
    async fn test_collision_detection() {
        let keypair = Keypair::new();
        let maximizer = IntentCollisionProfitMaximizer::new(
            "https://api.mainnet-beta.solana.com",
            keypair,
            "https://mainnet.block-engine.jito.wtf",
            "test_auth",
        ).await.unwrap();

        let pool_key = Pubkey::new_unique();
        let market_state = MarketState {
            base_reserves: 1_000_000_000_000,
            quote_reserves: 50_000_000_000,
            fee_bps: 30,
            sqrt_price: 7_071_067_811,
        };
        
        maximizer.update_market_state(pool_key, market_state).await.unwrap();

        let intent_a = Intent {
            signature: "sig_a".to_string(),
            program_id: Pubkey::from_str(RAYDIUM_V4).unwrap(),
            accounts: vec![
                AccountMeta::new(keypair.pubkey(), true),
                AccountMeta::new(pool_key, false),
                AccountMeta::new(Pubkey::new_unique(), false),
                AccountMeta::new(Pubkey::new_unique(), false),
            ],
            data: vec![0x09, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00],
            compute_units: 200_000,
            priority_fee: 1_000_000,
            sender: keypair.pubkey(),
            value: 10_000_000_000,
            intent_type: IntentType::Swap {
                input_mint: Pubkey::new_unique(),
                output_mint: Pubkey::new_unique(),
            },
            slot: 250_000_000,
            market_state: market_state.clone(),
        };

        let intent_b = Intent {
            signature: "sig_b".to_string(),
            program_id: Pubkey::from_str(RAYDIUM_V4).unwrap(),
            accounts: intent_a.accounts.clone(),
            data: vec![0x09, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00],
            compute_units: 200_000,
            priority_fee: 500_000,
            sender: Pubkey::new_unique(),
            value: 5_000_000_000,
            intent_type: IntentType::Swap {
                input_mint: Pubkey::new_unique(),
                output_mint: Pubkey::new_unique(),
            },
            slot: 250_000_000,
            market_state,
        };

        maximizer.add_intent(intent_a).await.unwrap();
        maximizer.add_intent(intent_b).await.unwrap();

        let collisions = maximizer.detect_collisions().await.unwrap();
        assert!(!collisions.is_empty());
        assert!(collisions[0].profit_opportunity > 0);
    }

    #[tokio::test]
    async fn test_priority_fee_tracking() {
        let keypair = Keypair::new();
        let maximizer = IntentCollisionProfitMaximizer::new(
            "https://api.mainnet-beta.solana.com",
            keypair,
            "https://mainnet.block-engine.jito.wtf",
            "test_auth",
        ).await.unwrap();

        {
            let mut tracker = maximizer.priority_fee_tracker.write().await;
            for i in 0..100 {
                tracker.push_back(1_000_000 + i * 10_000);
            }
        }

        let metrics = maximizer.get_performance_metrics().await;
        assert!(metrics.average_priority_fee > 1_000_000);
    }
}

