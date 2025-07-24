use solana_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::{
    account::Account,
    clock::Clock,
    commitment_config::CommitmentConfig,
    instruction::CompiledInstruction,
    message::Message,
    program_pack::Pack,
    pubkey::Pubkey,
    signature::Signature,
    sysvar,
    transaction::Transaction,
};
use solana_transaction_status::{
    EncodedConfirmedTransactionWithStatusMeta, UiTransactionEncoding,
    EncodedTransaction, UiMessage,
};
use spl_token::state::Account as TokenAccount;
use std::{
    collections::{HashMap, HashSet, VecDeque},
    str::FromStr,
    sync::{Arc, RwLock},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use borsh::{BorshDeserialize, BorshSerialize};
use serde::{Deserialize, Serialize};
use rayon::prelude::*;
use thiserror::Error;
use tokio::time::timeout;

const SERUM_DEX_V3: &str = "9xQeWvG816bUx9EPjHmaT23yvVM2ZWbrrpZb9PusVFin";
const RAYDIUM_AMM_V4: &str = "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8";
const ORCA_WHIRLPOOL: &str = "whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc";
const JUPITER_V4: &str = "JUP4Fb2cqiRUcaTHdrPC8h2gNsA2ETXiPDD33WcGuJB";
const LIFINITY_V2: &str = "2wT8Yq49kHgDzXuPxZSaeLaH1qbmGXtEyPy64bL7aD3c";

const STRATEGY_WINDOW: usize = 1000;
const PATTERN_MIN_OCCURRENCES: usize = 3;
const TIMING_PRECISION_MS: u64 = 50;
const GAS_ANALYSIS_PERCENTILES: [f64; 5] = [0.25, 0.5, 0.75, 0.95, 0.99];
const RPC_TIMEOUT: Duration = Duration::from_secs(10);
const MAX_RETRIES: u32 = 3;

#[derive(Error, Debug)]
pub enum DecoderError {
    #[error("RPC error: {0}")]
    Rpc(#[from] solana_client::client_error::ClientError),
    #[error("Serialization error: {0}")]
    Serialization(#[from] bincode::Error),
    #[error("Invalid transaction encoding")]
    InvalidEncoding,
    #[error("Missing transaction metadata")]
    MissingMetadata,
    #[error("Decoding failed: {0}")]
    DecodingFailed(String),
    #[error("Timeout exceeded")]
    Timeout,
    #[error("Invalid instruction data")]
    InvalidInstruction,
}

pub type Result<T> = std::result::Result<T, DecoderError>;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum StrategyType {
    Arbitrage { paths: Vec<TradePath>, profit_threshold: u64 },
    Sandwich { target_amount: u64, slippage_tolerance: u64 },
    Liquidation { protocol: String, collateral_ratio: u64 },
    JitLiquidity { pool: Pubkey, duration_ms: u64 },
    BackRun { target_signatures: Vec<Signature> },
    FrontRun { gas_multiplier: u64, priority_fee: u64 },
    FlashLoan { amount: u64, protocols: Vec<String> },
    AtomicArbitrage { dex_sequence: Vec<String> },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct TradePath {
    pub dex: String,
    pub pool: Pubkey,
    pub token_in: Pubkey,
    pub token_out: Pubkey,
    pub amount: u64,
}

#[derive(Debug, Clone)]
pub struct CompetitorProfile {
    pub address: Pubkey,
    pub strategies: HashMap<StrategyType, StrategyMetrics>,
    pub timing_patterns: TimingAnalysis,
    pub gas_patterns: GasAnalysis,
    pub success_rate: f64,
    pub avg_profit: u64,
    pub last_seen: Instant,
    pub transaction_history: VecDeque<TransactionAnalysis>,
}

#[derive(Debug, Clone)]
pub struct StrategyMetrics {
    pub occurrences: usize,
    pub success_count: usize,
    pub total_profit: i64,
    pub avg_execution_time: Duration,
    pub gas_costs: Vec<u64>,
    pub timestamps: Vec<u64>,
}

#[derive(Debug, Clone)]
pub struct TimingAnalysis {
    pub block_offset_distribution: HashMap<i32, usize>,
    pub submission_intervals: Vec<Duration>,
    pub preferred_slots: HashSet<u64>,
    pub burst_patterns: Vec<BurstPattern>,
}

#[derive(Debug, Clone)]
pub struct BurstPattern {
    pub tx_count: usize,
    pub duration: Duration,
    pub interval: Duration,
}

#[derive(Debug, Clone)]
pub struct GasAnalysis {
    pub base_fees: Vec<u64>,
    pub priority_fees: Vec<u64>,
    pub compute_units: Vec<u64>,
    pub percentiles: HashMap<String, u64>,
    pub dynamic_pricing_model: Option<GasPricingModel>,
}

#[derive(Debug, Clone)]
pub struct GasPricingModel {
    pub base_multiplier: u64,
    pub congestion_factor: u64,
    pub competition_factor: u64,
}

#[derive(Debug, Clone)]
pub struct TransactionAnalysis {
    pub signature: Signature,
    pub slot: u64,
    pub timestamp: u64,
    pub strategy: StrategyType,
    pub profit: i64,
    pub gas_used: u64,
    pub instructions: Vec<DecodedInstruction>,
}

#[derive(Debug, Clone)]
pub struct DecodedInstruction {
    pub program_id: Pubkey,
    pub instruction_type: String,
    pub accounts: Vec<Pubkey>,
    pub data: Vec<u8>,
}

pub struct CompetitorStrategyDecoder {
    rpc_client: Arc<RpcClient>,
    competitors: Arc<RwLock<HashMap<Pubkey, CompetitorProfile>>>,
    strategy_patterns: Arc<RwLock<HashMap<String, StrategyPattern>>>,
    program_decoders: HashMap<Pubkey, Box<dyn ProgramDecoder + Send + Sync>>,
}

#[derive(Debug, Clone)]
pub struct StrategyPattern {
    pub signature: Vec<InstructionPattern>,
    pub timing_constraints: TimingConstraints,
    pub profitability_metrics: ProfitabilityMetrics,
}

#[derive(Debug, Clone)]
pub struct InstructionPattern {
    pub program_id: Pubkey,
    pub min_accounts: usize,
    pub data_patterns: Vec<DataPattern>,
}

#[derive(Debug, Clone)]
pub struct DataPattern {
    pub offset: usize,
    pub pattern_type: PatternType,
}

#[derive(Debug, Clone)]
pub enum PatternType {
    Exact(Vec<u8>),
    Range { min: Vec<u8>, max: Vec<u8> },
    Discriminator(u64),
}

#[derive(Debug, Clone)]
pub struct TimingConstraints {
    pub min_interval: Duration,
    pub max_interval: Duration,
    pub preferred_slots: HashSet<u64>,
}

#[derive(Debug, Clone)]
pub struct ProfitabilityMetrics {
    pub min_profit: u64,
    pub avg_profit: u64,
    pub profit_variance: f64,
}

trait ProgramDecoder: Send + Sync {
    fn decode_instruction(&self, instruction: &CompiledInstruction, accounts: &[Pubkey]) -> Result<DecodedInstruction>;
    fn identify_strategy(&self, instructions: &[DecodedInstruction]) -> Option<StrategyType>;
}

struct SerumDecoder;
struct RaydiumDecoder;
struct OrcaDecoder;
struct JupiterDecoder;

impl CompetitorStrategyDecoder {
    pub fn new(rpc_client: Arc<RpcClient>) -> Self {
        let mut program_decoders: HashMap<Pubkey, Box<dyn ProgramDecoder + Send + Sync>> = HashMap::new();
        
        let serum_key = Pubkey::from_str(SERUM_DEX_V3).expect("Valid Serum pubkey");
        let raydium_key = Pubkey::from_str(RAYDIUM_AMM_V4).expect("Valid Raydium pubkey");
        let orca_key = Pubkey::from_str(ORCA_WHIRLPOOL).expect("Valid Orca pubkey");
        let jupiter_key = Pubkey::from_str(JUPITER_V4).expect("Valid Jupiter pubkey");
        
        program_decoders.insert(serum_key, Box::new(SerumDecoder));
        program_decoders.insert(raydium_key, Box::new(RaydiumDecoder));
        program_decoders.insert(orca_key, Box::new(OrcaDecoder));
        program_decoders.insert(jupiter_key, Box::new(JupiterDecoder));

        Self {
            rpc_client,
            competitors: Arc::new(RwLock::new(HashMap::new())),
            strategy_patterns: Arc::new(RwLock::new(HashMap::new())),
            program_decoders,
        }
    }

    pub async fn analyze_transaction(&self, signature: &Signature) -> Result<TransactionAnalysis> {
        let tx_result = timeout(
            RPC_TIMEOUT,
            self.fetch_transaction_with_retry(signature)
        ).await.map_err(|_| DecoderError::Timeout)??;
        
        let meta = tx_result.transaction.meta.ok_or(DecoderError::MissingMetadata)?;
        let slot = tx_result.slot;
        let timestamp = tx_result.block_time.unwrap_or(0) as u64;
        
        let decoded_tx = self.decode_transaction(tx_result.transaction.transaction)?;
        let instructions = self.decode_instructions(&decoded_tx)?;
        let strategy = self.identify_strategy(&instructions).ok_or_else(|| 
            DecoderError::DecodingFailed("Failed to identify strategy".to_string())
        )?;
        
        let profit = self.calculate_profit(&meta.pre_balances, &meta.post_balances);
        let gas_used = meta.fee;

        Ok(TransactionAnalysis {
            signature: *signature,
            slot,
            timestamp,
            strategy,
            profit,
            gas_used,
            instructions,
        })
    }

    async fn fetch_transaction_with_retry(&self, signature: &Signature) -> Result<EncodedConfirmedTransactionWithStatusMeta> {
        let mut retry_count = 0;
        loop {
            match self.rpc_client.get_transaction_with_config(
                signature,
                solana_client::rpc_config::RpcTransactionConfig {
                    encoding: Some(UiTransactionEncoding::Base64),
                    commitment: Some(CommitmentConfig::confirmed()),
                    max_supported_transaction_version: Some(0),
                }
            ).await {
                Ok(tx) => return Ok(tx),
                Err(e) if retry_count < MAX_RETRIES => {
                    retry_count += 1;
                    tokio::time::sleep(Duration::from_millis(100 * retry_count as u64)).await;
                }
                Err(e) => return Err(e.into()),
            }
        }
    }

    fn decode_transaction(&self, encoded_tx: EncodedTransaction) -> Result<Transaction> {
        match encoded_tx {
            EncodedTransaction::LegacyBinary(s) | EncodedTransaction::Binary(s, _) => {
                let bytes = base64::decode(s).map_err(|_| DecoderError::InvalidEncoding)?;
                bincode::deserialize::<Transaction>(&bytes).map_err(|e| e.into())
            }
            _ => Err(DecoderError::InvalidEncoding),
        }
    }

    fn calculate_profit(&self, pre_balances: &[u64], post_balances: &[u64]) -> i64 {
        if pre_balances.len() != post_balances.len() {
            return 0;
        }
        
        post_balances.iter()
            .zip(pre_balances.iter())
            .map(|(post, pre)| *post as i64 - *pre as i64)
            .max()
            .unwrap_or(0)
    }

    fn decode_instructions(&self, tx: &Transaction) -> Result<Vec<DecodedInstruction>> {
        let message = &tx.message;
        let mut decoded_instructions = Vec::with_capacity(message.instructions.len());

        for instruction in &message.instructions {
            let program_id = message.account_keys.get(instruction.program_id_index as usize)
                .ok_or(DecoderError::InvalidInstruction)?;
            
            let accounts: Vec<Pubkey> = instruction.accounts.iter()
                .map(|&idx| message.account_keys.get(idx as usize).copied())
                .collect::<Option<Vec<_>>>()
                .ok_or(DecoderError::InvalidInstruction)?;
            
            if let Some(decoder) = self.program_decoders.get(program_id) {
                decoded_instructions.push(decoder.decode_instruction(instruction, &accounts)?);
            }
        }

        Ok(decoded_instructions)
    }

    fn identify_strategy(&self, instructions: &[DecodedInstruction]) -> Option<StrategyType> {
        if self.is_arbitrage_pattern(instructions) {
            let paths = self.extract_trade_paths(instructions);
            let profit_threshold = self.calculate_profit_threshold(&paths);
            return Some(StrategyType::Arbitrage { paths, profit_threshold });
        }

                if self.is_sandwich_pattern(instructions) {
            let (target_amount, slippage) = self.extract_sandwich_params(instructions);
            return Some(StrategyType::Sandwich { 
                target_amount, 
                slippage_tolerance: slippage 
            });
        }

        if self.is_jit_pattern(instructions) {
            let (pool, duration) = self.extract_jit_params(instructions);
            return Some(StrategyType::JitLiquidity { pool, duration_ms: duration });
        }

        if self.is_flashloan_pattern(instructions) {
            let (amount, protocols) = self.extract_flashloan_params(instructions);
            return Some(StrategyType::FlashLoan { amount, protocols });
        }

        let dex_sequence = self.extract_dex_sequence(instructions);
        if !dex_sequence.is_empty() {
            Some(StrategyType::AtomicArbitrage { dex_sequence })
        } else {
            None
        }
    }

    fn is_arbitrage_pattern(&self, instructions: &[DecodedInstruction]) -> bool {
        let dex_interactions = instructions.iter()
            .filter(|i| self.is_dex_instruction(i))
            .count();
        
        dex_interactions >= 2 && self.has_profit_extraction(instructions)
    }

    fn is_sandwich_pattern(&self, instructions: &[DecodedInstruction]) -> bool {
        instructions.len() >= 3 && 
        instructions.first().map_or(false, |i| self.is_swap_instruction(i)) &&
        instructions.last().map_or(false, |i| self.is_swap_instruction(i)) &&
        instructions.first().and_then(|first| {
            instructions.last().map(|last| {
                first.accounts.iter().any(|a| last.accounts.contains(a))
            })
        }).unwrap_or(false)
    }

    fn is_jit_pattern(&self, instructions: &[DecodedInstruction]) -> bool {
        let has_add_liquidity = instructions.iter().any(|i| {
            i.instruction_type.contains("add_liquidity") || 
            i.instruction_type.contains("deposit") ||
            i.instruction_type.contains("increase_liquidity")
        });
        
        let has_remove_liquidity = instructions.iter().any(|i| {
            i.instruction_type.contains("remove_liquidity") || 
            i.instruction_type.contains("withdraw") ||
            i.instruction_type.contains("decrease_liquidity")
        });
        
        has_add_liquidity && has_remove_liquidity && instructions.len() >= 2
    }

    fn is_flashloan_pattern(&self, instructions: &[DecodedInstruction]) -> bool {
        instructions.iter().enumerate().any(|(i, inst)| {
            (inst.instruction_type.contains("flash_loan") || 
             inst.instruction_type.contains("borrow")) &&
            instructions.iter().skip(i + 1).any(|later| {
                (later.instruction_type.contains("repay") ||
                 later.instruction_type.contains("return")) &&
                later.accounts.iter().any(|acc| inst.accounts.contains(acc))
            })
        })
    }

    fn is_dex_instruction(&self, instruction: &DecodedInstruction) -> bool {
        self.program_decoders.contains_key(&instruction.program_id)
    }

    fn is_swap_instruction(&self, instruction: &DecodedInstruction) -> bool {
        instruction.instruction_type.contains("swap") ||
        instruction.instruction_type.contains("exchange") ||
        instruction.instruction_type.contains("route") ||
        instruction.instruction_type.contains("trade")
    }

    fn has_profit_extraction(&self, instructions: &[DecodedInstruction]) -> bool {
        instructions.iter().any(|i| {
            i.instruction_type.contains("transfer") ||
            i.instruction_type.contains("withdraw") ||
            i.instruction_type.contains("close_account") ||
            i.instruction_type.contains("settle")
        })
    }

    fn extract_trade_paths(&self, instructions: &[DecodedInstruction]) -> Vec<TradePath> {
        instructions.iter()
            .filter(|i| self.is_swap_instruction(i))
            .filter_map(|inst| {
                if inst.accounts.len() >= 4 && inst.data.len() >= 8 {
                    Some(TradePath {
                        dex: self.identify_dex(&inst.program_id),
                        pool: inst.accounts[0],
                        token_in: inst.accounts[1],
                        token_out: inst.accounts[2],
                        amount: self.extract_amount_from_data(&inst.data),
                    })
                } else {
                    None
                }
            })
            .collect()
    }

    fn calculate_profit_threshold(&self, paths: &[TradePath]) -> u64 {
        let total_volume: u64 = paths.iter().map(|p| p.amount).sum();
        (total_volume / 1000).max(10_000) // 0.1% of volume or 10k lamports minimum
    }

    fn extract_sandwich_params(&self, instructions: &[DecodedInstruction]) -> (u64, u64) {
        let amounts: Vec<u64> = instructions.iter()
            .filter_map(|i| {
                if i.data.len() >= 8 {
                    Some(self.extract_amount_from_data(&i.data))
                } else {
                    None
                }
            })
            .collect();
        
        let target_amount = amounts.get(1).copied().unwrap_or(0);
        let slippage = if amounts.len() >= 2 && amounts[0] > 0 {
            ((amounts[1].saturating_sub(amounts[0]) as f64 / amounts[0] as f64) * 10000.0) as u64
        } else {
            100 // 1% default
        };
        
        (target_amount, slippage)
    }

    fn extract_jit_params(&self, instructions: &[DecodedInstruction]) -> (Pubkey, u64) {
        let pool = instructions.iter()
            .find(|i| i.instruction_type.contains("liquidity"))
            .and_then(|i| i.accounts.first())
            .copied()
            .unwrap_or_else(Pubkey::default);
        
        let duration = TIMING_PRECISION_MS * 4; // 200ms default
        (pool, duration)
    }

    fn extract_flashloan_params(&self, instructions: &[DecodedInstruction]) -> (u64, Vec<String>) {
        let amount = instructions.iter()
            .find(|i| i.instruction_type.contains("flash") || i.instruction_type.contains("borrow"))
            .and_then(|i| {
                if i.data.len() >= 8 {
                    Some(self.extract_amount_from_data(&i.data))
                } else {
                    None
                }
            })
            .unwrap_or(0);
        
        let protocols = instructions.iter()
            .map(|i| self.identify_dex(&i.program_id))
            .collect::<HashSet<_>>()
            .into_iter()
            .filter(|p| p != "Unknown")
            .collect();
        
        (amount, protocols)
    }

    fn extract_dex_sequence(&self, instructions: &[DecodedInstruction]) -> Vec<String> {
        instructions.iter()
            .filter(|i| self.is_dex_instruction(i))
            .map(|i| self.identify_dex(&i.program_id))
            .filter(|dex| dex != "Unknown")
            .collect()
    }

    fn identify_dex(&self, program_id: &Pubkey) -> String {
        match program_id.to_string().as_str() {
            SERUM_DEX_V3 => "Serum".to_string(),
            RAYDIUM_AMM_V4 => "Raydium".to_string(),
            ORCA_WHIRLPOOL => "Orca".to_string(),
            JUPITER_V4 => "Jupiter".to_string(),
            LIFINITY_V2 => "Lifinity".to_string(),
            _ => "Unknown".to_string(),
        }
    }

    fn extract_amount_from_data(&self, data: &[u8]) -> u64 {
        if data.len() >= 8 {
            u64::from_le_bytes([
                data[0], data[1], data[2], data[3],
                data[4], data[5], data[6], data[7]
            ])
        } else {
            0
        }
    }

    pub fn update_competitor_profile(&self, address: Pubkey, analysis: TransactionAnalysis) {
        if let Ok(mut competitors) = self.competitors.write() {
            let profile = competitors.entry(address).or_insert_with(|| CompetitorProfile {
                address,
                strategies: HashMap::new(),
                timing_patterns: TimingAnalysis {
                    block_offset_distribution: HashMap::new(),
                    submission_intervals: Vec::new(),
                    preferred_slots: HashSet::new(),
                    burst_patterns: Vec::new(),
                },
                gas_patterns: GasAnalysis {
                    base_fees: Vec::new(),
                    priority_fees: Vec::new(),
                    compute_units: Vec::new(),
                    percentiles: HashMap::new(),
                    dynamic_pricing_model: None,
                },
                success_rate: 0.0,
                avg_profit: 0,
                last_seen: Instant::now(),
                transaction_history: VecDeque::with_capacity(STRATEGY_WINDOW),
            });

            profile.last_seen = Instant::now();
            
            let strategy_metrics = profile.strategies.entry(analysis.strategy.clone()).or_insert_with(|| StrategyMetrics {
                occurrences: 0,
                success_count: 0,
                total_profit: 0,
                avg_execution_time: Duration::from_millis(0),
                gas_costs: Vec::new(),
                timestamps: Vec::new(),
            });

            strategy_metrics.occurrences += 1;
            if analysis.profit > 0 {
                strategy_metrics.success_count += 1;
            }
            strategy_metrics.total_profit += analysis.profit;
            strategy_metrics.gas_costs.push(analysis.gas_used);
            strategy_metrics.timestamps.push(analysis.timestamp);

            profile.gas_patterns.base_fees.push(analysis.gas_used);
            
            if profile.transaction_history.len() >= STRATEGY_WINDOW {
                profile.transaction_history.pop_front();
            }
            profile.transaction_history.push_back(analysis);

            self.update_timing_patterns(profile);
            self.update_gas_analysis(profile);
            self.calculate_success_metrics(profile);
        }
    }

    fn update_timing_patterns(&self, profile: &mut CompetitorProfile) {
        if profile.transaction_history.len() < 2 {
            return;
        }

        let timestamps: Vec<u64> = profile.transaction_history.iter()
            .map(|tx| tx.timestamp)
            .collect();

        profile.timing_patterns.submission_intervals.clear();
        for window in timestamps.windows(2) {
            if let [prev, curr] = window {
                let interval = Duration::from_millis(curr.saturating_sub(*prev));
                profile.timing_patterns.submission_intervals.push(interval);
            }
        }

        let slots: Vec<u64> = profile.transaction_history.iter()
            .map(|tx| tx.slot)
            .collect();

        profile.timing_patterns.burst_patterns.clear();
        for window in slots.windows(PATTERN_MIN_OCCURRENCES) {
            if let Some(first) = window.first() {
                if let Some(last) = window.last() {
                    let slot_diff = last.saturating_sub(*first);
                    if slot_diff < 100 && slot_diff > 0 {
                        profile.timing_patterns.burst_patterns.push(BurstPattern {
                            tx_count: window.len(),
                            duration: Duration::from_millis(slot_diff * 400),
                            interval: Duration::from_millis((slot_diff * 400) / window.len() as u64),
                        });
                    }
                }
            }
        }

        profile.timing_patterns.preferred_slots.clear();
        for slot in slots.iter().take(100) {
            let slot_mod = slot % 64;
            profile.timing_patterns.preferred_slots.insert(slot_mod);
        }
    }

    fn update_gas_analysis(&self, profile: &mut CompetitorProfile) {
        if profile.gas_patterns.base_fees.len() < 10 {
            return;
        }

        let mut sorted_fees = profile.gas_patterns.base_fees.clone();
        sorted_fees.par_sort_unstable();

        profile.gas_patterns.percentiles.clear();
        for percentile in &GAS_ANALYSIS_PERCENTILES {
            let idx = ((sorted_fees.len() as f64 * percentile) as usize)
                .min(sorted_fees.len().saturating_sub(1));
            profile.gas_patterns.percentiles.insert(
                format!("p{}", (percentile * 100.0) as u32),
                sorted_fees[idx]
            );
        }

        let sum: u64 = sorted_fees.iter().sum();
        let avg_fee = sum / sorted_fees.len() as u64;
        
        let variance = sorted_fees.par_iter()
            .map(|&fee| {
                let diff = fee as i64 - avg_fee as i64;
                (diff * diff) as u64
            })
            .sum::<u64>() / sorted_fees.len() as u64;

        let std_dev = (variance as f64).sqrt() as u64;

        profile.gas_patterns.dynamic_pricing_model = Some(GasPricingModel {
            base_multiplier: 10000 + (std_dev * 10000 / avg_fee.max(1)),
            congestion_factor: (sorted_fees[sorted_fees.len() - 1] * 10000) / avg_fee.max(1),
            competition_factor: 12000, // 1.2x
        });
    }

    fn calculate_success_metrics(&self, profile: &mut CompetitorProfile) {
        let total_txs = profile.transaction_history.len();
        if total_txs == 0 {
            return;
        }

        let successful_txs = profile.transaction_history.iter()
            .filter(|tx| tx.profit > 0)
            .count();

                profile.success_rate = (successful_txs as f64 / total_txs as f64 * 100.0).min(100.0);

        let total_profit: i64 = profile.transaction_history.iter()
            .map(|tx| tx.profit)
            .sum();

        profile.avg_profit = if total_profit > 0 {
            (total_profit / total_txs as i64) as u64
        } else {
            0
        };
    }

    pub fn get_competitor_insights(&self, address: &Pubkey) -> Option<CompetitorInsights> {
        let competitors = self.competitors.read().ok()?;
        let profile = competitors.get(address)?;

        let dominant_strategy = profile.strategies.iter()
            .max_by_key(|(_, metrics)| metrics.occurrences)
            .map(|(strategy, _)| strategy.clone());

        let gas_optimization = profile.gas_patterns.dynamic_pricing_model.as_ref()
            .map(|model| GasOptimization {
                suggested_multiplier: model.base_multiplier.saturating_mul(95) / 100,
                min_priority_fee: profile.gas_patterns.percentiles.get("p25").copied().unwrap_or(1000),
                max_priority_fee: profile.gas_patterns.percentiles.get("p95").copied().unwrap_or(100000),
            });

        let timing_optimization = TimingOptimization {
            preferred_submission_window: profile.timing_patterns.submission_intervals.iter()
                .min()
                .copied()
                .unwrap_or(Duration::from_millis(100)),
            burst_detection: !profile.timing_patterns.burst_patterns.is_empty(),
            slot_preference: profile.timing_patterns.preferred_slots.clone(),
        };

        Some(CompetitorInsights {
            address: *address,
            dominant_strategy,
            success_rate: profile.success_rate,
            avg_profit: profile.avg_profit,
            gas_optimization,
            timing_optimization,
            last_activity: profile.last_seen,
        })
    }

    pub fn detect_emerging_patterns(&self) -> Vec<EmergingPattern> {
        let competitors = match self.competitors.read() {
            Ok(c) => c,
            Err(_) => return Vec::new(),
        };
        
        let mut pattern_candidates: HashMap<String, Vec<&StrategyMetrics>> = HashMap::new();

        for profile in competitors.values() {
            for (strategy, metrics) in &profile.strategies {
                if metrics.occurrences >= PATTERN_MIN_OCCURRENCES {
                    let key = format!("{:?}", strategy);
                    pattern_candidates.entry(key).or_default().push(metrics);
                }
            }
        }

        pattern_candidates.into_par_iter()
            .filter_map(|(pattern, metrics_list)| {
                if metrics_list.len() >= 2 {
                    let total_occurrences: usize = metrics_list.iter().map(|m| m.occurrences).sum();
                    let total_success: usize = metrics_list.iter().map(|m| m.success_count).sum();
                    let avg_success = if total_occurrences > 0 {
                        total_success as f64 / total_occurrences as f64
                    } else {
                        0.0
                    };

                    Some(EmergingPattern {
                        pattern_signature: pattern,
                        competitor_count: metrics_list.len(),
                        total_occurrences,
                        avg_success_rate: avg_success,
                        growth_rate: self.calculate_growth_rate(&metrics_list),
                    })
                } else {
                    None
                }
            })
            .collect()
    }

    fn calculate_growth_rate(&self, metrics_list: &[&StrategyMetrics]) -> f64 {
        let recent_window = 3600_000; // 1 hour in ms
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or(Duration::from_secs(0))
            .as_millis() as u64;

        let recent_count: usize = metrics_list.par_iter()
            .map(|m| m.timestamps.iter()
                .filter(|&&ts| now.saturating_sub(ts) < recent_window)
                .count()
            )
            .sum();

        let total_count: usize = metrics_list.iter()
            .map(|m| m.timestamps.len())
            .sum();

        if total_count > 0 {
            (recent_count as f64 / total_count as f64 * 100.0).min(100.0)
        } else {
            0.0
        }
    }
}

#[derive(Debug, Clone)]
pub struct CompetitorInsights {
    pub address: Pubkey,
    pub dominant_strategy: Option<StrategyType>,
    pub success_rate: f64,
    pub avg_profit: u64,
    pub gas_optimization: Option<GasOptimization>,
    pub timing_optimization: TimingOptimization,
    pub last_activity: Instant,
}

#[derive(Debug, Clone)]
pub struct GasOptimization {
    pub suggested_multiplier: u64,
    pub min_priority_fee: u64,
    pub max_priority_fee: u64,
}

#[derive(Debug, Clone)]
pub struct TimingOptimization {
    pub preferred_submission_window: Duration,
    pub burst_detection: bool,
    pub slot_preference: HashSet<u64>,
}

#[derive(Debug, Clone)]
pub struct EmergingPattern {
    pub pattern_signature: String,
    pub competitor_count: usize,
    pub total_occurrences: usize,
    pub avg_success_rate: f64,
    pub growth_rate: f64,
}

impl ProgramDecoder for SerumDecoder {
    fn decode_instruction(&self, instruction: &CompiledInstruction, accounts: &[Pubkey]) -> Result<DecodedInstruction> {
        if instruction.data.is_empty() {
            return Err(DecoderError::InvalidInstruction);
        }

        let instruction_type = match instruction.data[0] {
            0 => "initialize_market",
            1 => "new_order",
            2 => "match_orders",
            3 => "consume_events",
            4 => "cancel_order",
            5 => "settle_funds",
            6 => "cancel_order_by_client_id",
            7 => "disable_market",
            8 => "sweep_fees",
            9 => "new_order_v2",
            10 => "new_order_v3",
            11 => "cancel_order_v2",
            12 => "cancel_order_by_client_id_v2",
            13 => "send_take",
            14 => "close_open_orders",
            15 => "init_open_orders",
            16 => "prune",
            17 => "consume_events_permissioned",
            _ => return Err(DecoderError::InvalidInstruction),
        }.to_string();

        Ok(DecodedInstruction {
            program_id: Pubkey::from_str(SERUM_DEX_V3).map_err(|_| DecoderError::InvalidInstruction)?,
            instruction_type,
            accounts: accounts.to_vec(),
            data: instruction.data.clone(),
        })
    }

    fn identify_strategy(&self, instructions: &[DecodedInstruction]) -> Option<StrategyType> {
        let has_new_order = instructions.iter().any(|i| 
            i.instruction_type.contains("new_order")
        );
        let has_settle = instructions.iter().any(|i| 
            i.instruction_type == "settle_funds"
        );

        if has_new_order && has_settle {
            Some(StrategyType::AtomicArbitrage {
                dex_sequence: vec!["Serum".to_string()],
            })
        } else {
            None
        }
    }
}

impl ProgramDecoder for RaydiumDecoder {
    fn decode_instruction(&self, instruction: &CompiledInstruction, accounts: &[Pubkey]) -> Result<DecodedInstruction> {
        if instruction.data.len() < 8 {
            return Err(DecoderError::InvalidInstruction);
        }

        let discriminator = u64::from_le_bytes([
            instruction.data[0], instruction.data[1], instruction.data[2], instruction.data[3],
            instruction.data[4], instruction.data[5], instruction.data[6], instruction.data[7]
        ]);
        
        let instruction_type = match discriminator {
            0xe445a52e51cb9a1d => "initialize",
            0x09c1b2c13ceee167 => "initialize2",
            0x2169d72e022ba23e => "deposit",
            0x183ed6e268ba06f2 => "withdraw",
            0x8ed6b032768e93e3 => "swap_base_in",
            0x904ba91a522e0b00 => "swap_base_out",
            _ => return Err(DecoderError::InvalidInstruction),
        }.to_string();

        Ok(DecodedInstruction {
            program_id: Pubkey::from_str(RAYDIUM_AMM_V4).map_err(|_| DecoderError::InvalidInstruction)?,
            instruction_type,
            accounts: accounts.to_vec(),
            data: instruction.data.clone(),
        })
    }

    fn identify_strategy(&self, instructions: &[DecodedInstruction]) -> Option<StrategyType> {
        let swap_count = instructions.iter()
            .filter(|i| i.instruction_type.contains("swap"))
            .count();

        if swap_count > 0 {
            Some(StrategyType::AtomicArbitrage {
                dex_sequence: vec!["Raydium".to_string()],
            })
        } else {
            None
        }
    }
}

impl ProgramDecoder for OrcaDecoder {
    fn decode_instruction(&self, instruction: &CompiledInstruction, accounts: &[Pubkey]) -> Result<DecodedInstruction> {
        if instruction.data.len() < 1 {
            return Err(DecoderError::InvalidInstruction);
        }

        let instruction_type = match instruction.data[0] {
            0x00 => "initialize_pool",
            0x01 => "swap",
            0x02 => "increase_liquidity",
            0x03 => "decrease_liquidity",
            0x04 => "update_fees_and_rewards",
            0x05 => "collect_fees",
            0x06 => "collect_reward",
            0x07 => "open_position",
            0x08 => "close_position",
            0x09 => "collect_protocol_fees",
            0x0a => "initialize_tick_array",
            _ => return Err(DecoderError::InvalidInstruction),
        }.to_string();

        Ok(DecodedInstruction {
            program_id: Pubkey::from_str(ORCA_WHIRLPOOL).map_err(|_| DecoderError::InvalidInstruction)?,
            instruction_type,
            accounts: accounts.to_vec(),
            data: instruction.data.clone(),
        })
    }

    fn identify_strategy(&self, instructions: &[DecodedInstruction]) -> Option<StrategyType> {
        let has_liquidity_add = instructions.iter().any(|i| 
            i.instruction_type.contains("increase_liquidity") || 
            i.instruction_type.contains("open_position")
        );
        let has_liquidity_remove = instructions.iter().any(|i| 
            i.instruction_type.contains("decrease_liquidity") || 
            i.instruction_type.contains("close_position")
        );

        if has_liquidity_add && has_liquidity_remove {
            Some(StrategyType::JitLiquidity {
                pool: instructions.first()?.accounts.first().copied()?,
                duration_ms: TIMING_PRECISION_MS * 4,
            })
        } else if instructions.iter().any(|i| i.instruction_type.contains("swap")) {
            Some(StrategyType::AtomicArbitrage {
                dex_sequence: vec!["Orca".to_string()],
            })
        } else {
            None
        }
    }
}

impl ProgramDecoder for JupiterDecoder {
    fn decode_instruction(&self, instruction: &CompiledInstruction, accounts: &[Pubkey]) -> Result<DecodedInstruction> {
        if instruction.data.is_empty() {
            return Err(DecoderError::InvalidInstruction);
        }

        let instruction_type = if instruction.data[0] < 16 {
            match instruction.data[0] {
                0 => "route",
                1 => "route_with_token_ledger",
                2 => "exact_out_route",
                3 => "shared_accounts_route",
                4 => "shared_accounts_exact_out_route",
                5 => "shared_accounts_route_with_token_ledger",
                _ => "route",
            }
        } else {
            "route"
        }.to_string();

        Ok(DecodedInstruction {
            program_id: Pubkey::from_str(JUPITER_V4).map_err(|_| DecoderError::InvalidInstruction)?,
            instruction_type,
            accounts: accounts.to_vec(),
            data: instruction.data.clone(),
        })
    }

    fn identify_strategy(&self, instructions: &[DecodedInstruction]) -> Option<StrategyType> {
        let route_count = instructions.iter()
            .filter(|i| i.instruction_type.contains("route"))
            .count();

        if route_count > 0 {
            Some(StrategyType::AtomicArbitrage {
                dex_sequence: vec!["Jupiter".to_string()],
            })
        } else {
            None
        }
    }
}

