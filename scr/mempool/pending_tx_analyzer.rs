use {
    anchor_lang::prelude::*,
    dashmap::DashMap,
    rayon::prelude::*,
    serde::{Deserialize, Serialize},
    solana_client::{
        nonblocking::rpc_client::RpcClient,
        rpc_config::{RpcTransactionConfig, RpcSimulateTransactionConfig},
    },
    solana_sdk::{
        account::Account,
        clock::Slot,
        commitment_config::CommitmentConfig,
        compute_budget::ComputeBudgetInstruction,
        hash::Hash,
        instruction::{AccountMeta, Instruction},
        pubkey::Pubkey,
        signature::Signature,
        transaction::Transaction,
    },
    solana_transaction_status::{
        EncodedConfirmedTransactionWithStatusMeta, UiTransactionEncoding,
        parse_instruction::{parse_instruction, ParsedInstruction},
    },
    std::{
        collections::{HashMap, VecDeque},
        sync::{
            atomic::{AtomicBool, AtomicU64, Ordering},
            Arc, RwLock,
        },
        time::{Duration, Instant},
    },
    tokio::{
        sync::{mpsc, Mutex, Semaphore},
        time::{interval, sleep},
    },
};

const MAX_PENDING_TX_CACHE: usize = 10000;
const SIMULATION_TIMEOUT_MS: u64 = 50;
const MAX_CONCURRENT_SIMULATIONS: usize = 100;
const PROFIT_THRESHOLD_LAMPORTS: u64 = 1_000_000;
const MAX_PRIORITY_FEE_LAMPORTS: u64 = 5_000_000;
const TX_EXPIRY_SLOTS: u64 = 150;
const MIN_CONFIDENCE_SCORE: f64 = 0.85;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PendingTransaction {
    pub signature: Signature,
    pub transaction: Transaction,
    pub slot: Slot,
    pub timestamp: Instant,
    pub priority_fee: u64,
    pub compute_units: u64,
    pub accounts: Vec<Pubkey>,
    pub program_ids: Vec<Pubkey>,
    pub opportunity_type: OpportunityType,
    pub confidence_score: f64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum OpportunityType {
    Arbitrage,
    Sandwich,
    Liquidation,
    NftMint,
    TokenLaunch,
    Unknown,
}

#[derive(Debug, Clone)]
pub struct MEVOpportunity {
    pub target_tx: PendingTransaction,
    pub profit_estimate: u64,
    pub gas_estimate: u64,
    pub success_probability: f64,
    pub entry_price: u64,
    pub exit_price: u64,
    pub token_mint: Pubkey,
    pub pool_address: Pubkey,
    pub strategy: MEVStrategy,
}

#[derive(Debug, Clone, Copy)]
pub enum MEVStrategy {
    FrontRun,
    BackRun,
    Sandwich,
    AtomicArbitrage,
}

#[derive(Debug)]
pub struct SimulationResult {
    pub success: bool,
    pub profit: i64,
    pub gas_used: u64,
    pub logs: Vec<String>,
    pub account_changes: HashMap<Pubkey, (u64, u64)>,
}

pub struct PendingTxAnalyzer {
    rpc_client: Arc<RpcClient>,
    pending_cache: Arc<DashMap<Signature, PendingTransaction>>,
    opportunity_sender: mpsc::UnboundedSender<MEVOpportunity>,
    simulation_semaphore: Arc<Semaphore>,
    known_dex_programs: Arc<HashMap<Pubkey, DexProtocol>>,
    price_feeds: Arc<DashMap<Pubkey, PriceFeed>>,
    historical_success: Arc<DashMap<OpportunityType, SuccessMetrics>>,
    active: Arc<AtomicBool>,
    processed_count: Arc<AtomicU64>,
    profitable_count: Arc<AtomicU64>,
}

#[derive(Debug, Clone)]
struct DexProtocol {
    program_id: Pubkey,
    swap_discriminator: [u8; 8],
    pool_account_offset: usize,
    reserve_offsets: (usize, usize),
    fee_numerator: u64,
    fee_denominator: u64,
}

#[derive(Debug, Clone)]
struct PriceFeed {
    price: f64,
    confidence: f64,
    last_update: Instant,
    volume_24h: f64,
}

#[derive(Debug, Default)]
struct SuccessMetrics {
    attempts: AtomicU64,
    successes: AtomicU64,
    total_profit: AtomicU64,
    avg_gas: AtomicU64,
}

impl PendingTxAnalyzer {
    pub fn new(
        rpc_client: Arc<RpcClient>,
        opportunity_sender: mpsc::UnboundedSender<MEVOpportunity>,
    ) -> Self {
        let known_dex_programs = Self::initialize_dex_protocols();
        
        Self {
            rpc_client,
            pending_cache: Arc::new(DashMap::new()),
            opportunity_sender,
            simulation_semaphore: Arc::new(Semaphore::new(MAX_CONCURRENT_SIMULATIONS)),
            known_dex_programs: Arc::new(known_dex_programs),
            price_feeds: Arc::new(DashMap::new()),
            historical_success: Arc::new(DashMap::new()),
            active: Arc::new(AtomicBool::new(true)),
            processed_count: Arc::new(AtomicU64::new(0)),
            profitable_count: Arc::new(AtomicU64::new(0)),
        }
    }

    fn initialize_dex_protocols() -> HashMap<Pubkey, DexProtocol> {
        let mut protocols = HashMap::new();
        
        // Raydium
        protocols.insert(
            Pubkey::from_str("675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8").unwrap(),
            DexProtocol {
                program_id: Pubkey::from_str("675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8").unwrap(),
                swap_discriminator: [248, 198, 158, 145, 225, 117, 135, 200],
                pool_account_offset: 1,
                reserve_offsets: (64, 72),
                fee_numerator: 25,
                fee_denominator: 10000,
            }
        );
        
        // Orca
        protocols.insert(
            Pubkey::from_str("9W959DqEETiGZocYWCQPaJ6sBmUzgfxXfqGeTEdp3aQP").unwrap(),
            DexProtocol {
                program_id: Pubkey::from_str("9W959DqEETiGZocYWCQPaJ6sBmUzgfxXfqGeTEdp3aQP").unwrap(),
                swap_discriminator: [248, 140, 31, 168, 241, 157, 204, 226],
                pool_account_offset: 2,
                reserve_offsets: (80, 88),
                fee_numerator: 30,
                fee_denominator: 10000,
            }
        );
        
        protocols
    }

    pub async fn analyze_transaction(&self, tx: Transaction, slot: Slot) -> Option<MEVOpportunity> {
        let signature = tx.signatures[0];
        self.processed_count.fetch_add(1, Ordering::Relaxed);
        
        let pending_tx = self.extract_transaction_details(tx, slot)?;
        
        if pending_tx.priority_fee > MAX_PRIORITY_FEE_LAMPORTS {
            return None;
        }
        
        let opportunity_type = self.classify_opportunity(&pending_tx);
        if opportunity_type == OpportunityType::Unknown {
            return None;
        }
        
        let confidence = self.calculate_confidence_score(&pending_tx, &opportunity_type).await;
        if confidence < MIN_CONFIDENCE_SCORE {
            return None;
        }
        
        self.pending_cache.insert(signature, pending_tx.clone());
        
        match opportunity_type {
            OpportunityType::Arbitrage => self.analyze_arbitrage_opportunity(pending_tx).await,
            OpportunityType::Sandwich => self.analyze_sandwich_opportunity(pending_tx).await,
            OpportunityType::Liquidation => self.analyze_liquidation_opportunity(pending_tx).await,
            OpportunityType::NftMint => self.analyze_nft_opportunity(pending_tx).await,
            OpportunityType::TokenLaunch => self.analyze_token_launch_opportunity(pending_tx).await,
            _ => None,
        }
    }

    fn extract_transaction_details(&self, tx: Transaction, slot: Slot) -> Option<PendingTransaction> {
        let instructions = &tx.message.instructions;
        let mut priority_fee = 0u64;
        let mut compute_units = 200_000u64;
        
        for ix in instructions {
            if let Ok(compute_ix) = ComputeBudgetInstruction::try_from(ix) {
                match compute_ix {
                    ComputeBudgetInstruction::SetComputeUnitPrice(price) => priority_fee = price,
                    ComputeBudgetInstruction::SetComputeUnitLimit(units) => compute_units = units as u64,
                    _ => {}
                }
            }
        }
        
        let accounts: Vec<Pubkey> = tx.message.account_keys.clone();
        let program_ids: Vec<Pubkey> = instructions
            .iter()
            .map(|ix| accounts[ix.program_id_index as usize])
            .collect();
        
        Some(PendingTransaction {
            signature: tx.signatures[0],
            transaction: tx,
            slot,
            timestamp: Instant::now(),
            priority_fee,
            compute_units,
            accounts,
            program_ids,
            opportunity_type: OpportunityType::Unknown,
            confidence_score: 0.0,
        })
    }

    fn classify_opportunity(&self, tx: &PendingTransaction) -> OpportunityType {
        for program_id in &tx.program_ids {
            if self.known_dex_programs.contains_key(program_id) {
                if self.is_large_swap(tx) {
                    return OpportunityType::Sandwich;
                }
                return OpportunityType::Arbitrage;
            }
            
            if self.is_nft_mint(tx) {
                return OpportunityType::NftMint;
            }
            
            if self.is_liquidation(tx) {
                return OpportunityType::Liquidation;
            }
            
            if self.is_token_launch(tx) {
                return OpportunityType::TokenLaunch;
            }
        }
        
        OpportunityType::Unknown
    }

    fn is_large_swap(&self, tx: &PendingTransaction) -> bool {
        for ix in &tx.transaction.message.instructions {
            if ix.data.len() >= 16 {
                let amount_bytes = &ix.data[8..16];
                let amount = u64::from_le_bytes(amount_bytes.try_into().unwrap_or_default());
                if amount > 100_000_000_000 { // 100 SOL
                    return true;
                }
            }
        }
        false
    }

    fn is_nft_mint(&self, tx: &PendingTransaction) -> bool {
        const METAPLEX_PROGRAM: &str = "metaqbxxUerdq28cj1RbAWkYQm3ybzjb6a8bt518x1s";
        tx.program_ids.iter().any(|p| p.to_string() == METAPLEX_PROGRAM)
    }

    fn is_liquidation(&self, tx: &PendingTransaction) -> bool {
        const LIQUIDATION_DISCRIMINATORS: &[[u8; 8]] = &[
            [0xbc, 0x19, 0x64, 0x7f, 0x72, 0x17, 0x0f, 0x9f],
            [0x85, 0x32, 0x89, 0xb7, 0x0e, 0x94, 0x08, 0x3f],
        ];
        
        for ix in &tx.transaction.message.instructions {
            if ix.data.len() >= 8 {
                let discriminator = &ix.data[..8];
                if LIQUIDATION_DISCRIMINATORS.iter().any(|d| d == discriminator) {
                    return true;
                }
            }
        }
        false
    }

    fn is_token_launch(&self, tx: &PendingTransaction) -> bool {
        const TOKEN_PROGRAM: &str = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA";
        let has_token_program = tx.program_ids.iter().any(|p| p.to_string() == TOKEN_PROGRAM);
        let has_init_ix = tx.transaction.message.instructions.iter().any(|ix| {
            ix.data.len() >= 1 && ix.data[0] == 0 // InitializeMint instruction
        });
        
        has_token_program && has_init_ix
    }

    async fn calculate_confidence_score(
        &self,
        tx: &PendingTransaction,
        opportunity_type: &OpportunityType,
    ) -> f64 {
        let mut score = 0.7;
        
        if let Some(metrics) = self.historical_success.get(opportunity_type) {
            let success_rate = metrics.successes.load(Ordering::Relaxed) as f64
                / metrics.attempts.load(Ordering::Relaxed).max(1) as f64;
            score *= success_rate;
        }
        
            score += 0.1;
        } else if tx.priority_fee > 1_000_000 {
            score -= 0.1;
        }
        
        // Account validation score
        let valid_accounts = tx.accounts.iter().filter(|a| {
            !a.to_bytes().iter().all(|&b| b == 0)
        }).count();
        score *= (valid_accounts as f64 / tx.accounts.len() as f64).max(0.5);
        
        // Timing score - newer transactions have higher confidence
        let age_ms = tx.timestamp.elapsed().as_millis() as f64;
        if age_ms < 100.0 {
            score *= 1.1;
        } else if age_ms > 1000.0 {
            score *= 0.8;
        }
        
        score.min(1.0).max(0.0)
    }

    async fn analyze_arbitrage_opportunity(&self, tx: PendingTransaction) -> Option<MEVOpportunity> {
        let permit = self.simulation_semaphore.acquire().await.ok()?;
        
        let (pool_address, token_mint) = self.extract_swap_details(&tx)?;
        let current_price = self.get_pool_price(&pool_address).await?;
        
        let simulation = self.simulate_arbitrage(&tx, &pool_address, current_price).await?;
        drop(permit);
        
        if simulation.profit <= PROFIT_THRESHOLD_LAMPORTS as i64 {
            return None;
        }
        
        let opportunity = MEVOpportunity {
            target_tx: tx,
            profit_estimate: simulation.profit as u64,
            gas_estimate: simulation.gas_used,
            success_probability: self.calculate_success_probability(&simulation),
            entry_price: current_price,
            exit_price: self.calculate_exit_price(current_price, simulation.profit),
            token_mint,
            pool_address,
            strategy: MEVStrategy::AtomicArbitrage,
        };
        
        self.profitable_count.fetch_add(1, Ordering::Relaxed);
        Some(opportunity)
    }

    async fn analyze_sandwich_opportunity(&self, tx: PendingTransaction) -> Option<MEVOpportunity> {
        let permit = self.simulation_semaphore.acquire().await.ok()?;
        
        let (pool_address, token_mint) = self.extract_swap_details(&tx)?;
        let victim_amount = self.extract_swap_amount(&tx)?;
        
        if victim_amount < 10_000_000_000 { // Min 10 SOL for sandwich
            drop(permit);
            return None;
        }
        
        let current_price = self.get_pool_price(&pool_address).await?;
        let (frontrun_amount, backrun_amount) = self.calculate_sandwich_amounts(victim_amount, current_price);
        
        let frontrun_sim = self.simulate_frontrun(&tx, frontrun_amount, &pool_address).await?;
        let backrun_sim = self.simulate_backrun(&tx, backrun_amount, &pool_address).await?;
        drop(permit);
        
        let total_profit = (backrun_sim.profit - frontrun_sim.profit).max(0) as u64;
        let total_gas = frontrun_sim.gas_used + backrun_sim.gas_used;
        
        if total_profit <= PROFIT_THRESHOLD_LAMPORTS || total_profit <= total_gas * 2 {
            return None;
        }
        
        let opportunity = MEVOpportunity {
            target_tx: tx,
            profit_estimate: total_profit,
            gas_estimate: total_gas,
            success_probability: (frontrun_sim.success && backrun_sim.success) as u8 as f64 * 0.85,
            entry_price: current_price,
            exit_price: self.calculate_exit_price(current_price, total_profit as i64),
            token_mint,
            pool_address,
            strategy: MEVStrategy::Sandwich,
        };
        
        self.profitable_count.fetch_add(1, Ordering::Relaxed);
        Some(opportunity)
    }

    async fn analyze_liquidation_opportunity(&self, tx: PendingTransaction) -> Option<MEVOpportunity> {
        let permit = self.simulation_semaphore.acquire().await.ok()?;
        
        let liquidation_account = self.extract_liquidation_account(&tx)?;
        let collateral_info = self.fetch_collateral_info(&liquidation_account).await?;
        
        if collateral_info.health_factor > 1.05 {
            drop(permit);
            return None;
        }
        
        let liquidation_bonus = collateral_info.collateral_value * 5 / 100; // 5% bonus
        let gas_estimate = 300_000; // Higher gas for liquidations
        
        if liquidation_bonus <= gas_estimate {
            drop(permit);
            return None;
        }
        
        let simulation = self.simulate_liquidation(&tx, &liquidation_account).await?;
        drop(permit);
        
        let opportunity = MEVOpportunity {
            target_tx: tx,
            profit_estimate: simulation.profit as u64,
            gas_estimate: simulation.gas_used,
            success_probability: 0.9, // High probability if health < 1.05
            entry_price: collateral_info.collateral_value,
            exit_price: collateral_info.collateral_value + liquidation_bonus,
            token_mint: collateral_info.collateral_mint,
            pool_address: liquidation_account,
            strategy: MEVStrategy::BackRun,
        };
        
        self.profitable_count.fetch_add(1, Ordering::Relaxed);
        Some(opportunity)
    }

    async fn analyze_nft_opportunity(&self, tx: PendingTransaction) -> Option<MEVOpportunity> {
        let mint_account = self.extract_nft_mint(&tx)?;
        let collection_stats = self.fetch_collection_stats(&mint_account).await?;
        
        if collection_stats.floor_price < 1_000_000_000 { // Min 1 SOL floor
            return None;
        }
        
        let mint_price = self.extract_mint_price(&tx)?;
        let potential_profit = collection_stats.floor_price.saturating_sub(mint_price);
        
        if potential_profit <= PROFIT_THRESHOLD_LAMPORTS {
            return None;
        }
        
        let opportunity = MEVOpportunity {
            target_tx: tx,
            profit_estimate: potential_profit,
            gas_estimate: 200_000,
            success_probability: 0.7, // NFT mints are competitive
            entry_price: mint_price,
            exit_price: collection_stats.floor_price,
            token_mint: mint_account,
            pool_address: Pubkey::default(),
            strategy: MEVStrategy::FrontRun,
        };
        
        self.profitable_count.fetch_add(1, Ordering::Relaxed);
        Some(opportunity)
    }

    async fn analyze_token_launch_opportunity(&self, tx: PendingTransaction) -> Option<MEVOpportunity> {
        let token_mint = self.extract_new_token_mint(&tx)?;
        let initial_liquidity = self.extract_initial_liquidity(&tx)?;
        
        if initial_liquidity < 10_000_000_000 { // Min 10 SOL liquidity
            return None;
        }
        
        let snipe_amount = initial_liquidity / 100; // 1% of liquidity
        let expected_pump = snipe_amount * 10; // 10x potential
        
        let opportunity = MEVOpportunity {
            target_tx: tx,
            profit_estimate: expected_pump.saturating_sub(snipe_amount),
            gas_estimate: 250_000,
            success_probability: 0.6,
            entry_price: snipe_amount,
            exit_price: expected_pump,
            token_mint,
            pool_address: Pubkey::default(),
            strategy: MEVStrategy::FrontRun,
        };
        
        self.profitable_count.fetch_add(1, Ordering::Relaxed);
        Some(opportunity)
    }

    fn extract_swap_details(&self, tx: &PendingTransaction) -> Option<(Pubkey, Pubkey)> {
        for (i, ix) in tx.transaction.message.instructions.iter().enumerate() {
            let program_id = &tx.accounts[ix.program_id_index as usize];
            
            if let Some(dex) = self.known_dex_programs.get(program_id) {
                if ix.data.len() >= 8 && &ix.data[..8] == &dex.swap_discriminator {
                    let pool_account = tx.accounts.get(ix.accounts[dex.pool_account_offset] as usize)?;
                    let token_mint = tx.accounts.get(ix.accounts[dex.pool_account_offset + 1] as usize)?;
                    return Some((*pool_account, *token_mint));
                }
            }
        }
        None
    }

    fn extract_swap_amount(&self, tx: &PendingTransaction) -> Option<u64> {
        for ix in &tx.transaction.message.instructions {
            if ix.data.len() >= 16 {
                let amount_bytes = &ix.data[8..16];
                let amount = u64::from_le_bytes(amount_bytes.try_into().ok()?);
                if amount > 0 {
                    return Some(amount);
                }
            }
        }
        None
    }

    fn extract_liquidation_account(&self, tx: &PendingTransaction) -> Option<Pubkey> {
        for ix in &tx.transaction.message.instructions {
            if ix.data.len() >= 8 && self.is_liquidation_instruction(&ix.data[..8]) {
                return tx.accounts.get(ix.accounts[0] as usize).copied();
            }
        }
        None
    }

    fn is_liquidation_instruction(&self, discriminator: &[u8]) -> bool {
        const KNOWN_LIQUIDATION_DISCRIMINATORS: &[[u8; 8]] = &[
            [0xbc, 0x19, 0x64, 0x7f, 0x72, 0x17, 0x0f, 0x9f],
            [0x85, 0x32, 0x89, 0xb7, 0x0e, 0x94, 0x08, 0x3f],
            [0xa3, 0x45, 0x67, 0x89, 0xab, 0xcd, 0xef, 0x01],
        ];
        
        KNOWN_LIQUIDATION_DISCRIMINATORS.iter().any(|d| d == discriminator)
    }

    fn extract_nft_mint(&self, tx: &PendingTransaction) -> Option<Pubkey> {
        for ix in &tx.transaction.message.instructions {
            if ix.data.len() >= 1 && ix.data[0] == 0x01 { // CreateMetadataAccount
                return tx.accounts.get(ix.accounts[0] as usize).copied();
            }
        }
        None
    }

    fn extract_mint_price(&self, tx: &PendingTransaction) -> Option<u64> {
        for ix in &tx.transaction.message.instructions {
            if ix.program_id_index < tx.accounts.len() as u8 {
                let program = &tx.accounts[ix.program_id_index as usize];
                if program == &solana_sdk::system_program::ID && ix.data.len() >= 12 {
                    let amount = u64::from_le_bytes(ix.data[4..12].try_into().ok()?);
                    if amount > 0 {
                        return Some(amount);
                    }
                }
            }
        }
        None
    }

    fn extract_new_token_mint(&self, tx: &PendingTransaction) -> Option<Pubkey> {
        for ix in &tx.transaction.message.instructions {
            if ix.data.len() >= 1 && ix.data[0] == 0 { // InitializeMint
                return tx.accounts.get(ix.accounts[0] as usize).copied();
            }
        }
        None
    }

    fn extract_initial_liquidity(&self, tx: &PendingTransaction) -> Option<u64> {
        let mut total_sol = 0u64;
        
        for ix in &tx.transaction.message.instructions {
            if ix.program_id_index < tx.accounts.len() as u8 {
                let program = &tx.accounts[ix.program_id_index as usize];
                if program == &solana_sdk::system_program::ID && ix.data.len() >= 12 {
                    if let Ok(amount_bytes) = ix.data[4..12].try_into() {
                        total_sol += u64::from_le_bytes(amount_bytes);
                    }
                }
            }
        }
        
        if total_sol > 0 { Some(total_sol) } else { None }
    }

    async fn get_pool_price(&self, pool_address: &Pubkey) -> Option<u64> {
        if let Some(price_feed) = self.price_feeds.get(pool_address) {
            if price_feed.last_update.elapsed() < Duration::from_secs(1) {
                return Some((price_feed.price * 1e9) as u64);
            }
        }
        
        let account = self.rpc_client.get_account(pool_address).await.ok()?;
        let price = self.calculate_pool_price_from_data(&account.data)?;
        
        self.price_feeds.insert(
            *pool_address,
            PriceFeed {
                price: price as f64 / 1e9,
                confidence: 0.95,
                last_update: Instant::now(),
                volume_24h: 0.0,
            },
        );
        
        Some(price)
    }

    fn calculate_pool_price_from_data(&self, data: &[u8]) -> Option<u64> {
        if data.len() < 96 {
            return None;
        }
        
        let reserve_a = u64::from_le_bytes(data[64..72].try_into().ok()?);
        let reserve_b = u64::from_le_bytes(data[72..80].try_into().ok()?);
        
        if reserve_a == 0 || reserve_b == 0 {
            return None;
        }
        
        Some((reserve_b * 1_000_000_000) / reserve_a)
    }

        let price_impact = (victim_amount as f64 / 1e11) * 0.3; // 0.3% per 100 SOL
        let optimal_frontrun = (victim_amount as f64 * 0.15 * (1.0 + price_impact)) as u64;
        let optimal_backrun = (optimal_frontrun as f64 * 1.08) as u64;
        
        (optimal_frontrun.min(50_000_000_000), optimal_backrun.min(50_000_000_000))
    }

    fn calculate_exit_price(&self, entry_price: u64, profit: i64) -> u64 {
        if profit > 0 {
            let price_increase = (profit as f64 / entry_price as f64) * 1e9;
            entry_price + price_increase as u64
        } else {
            entry_price
        }
    }

    fn calculate_success_probability(&self, simulation: &SimulationResult) -> f64 {
        let mut probability = 0.5;
        
        if simulation.success {
            probability += 0.3;
        }
        
        if simulation.gas_used < 300_000 {
            probability += 0.1;
        }
        
        if simulation.profit > 10_000_000 {
            probability += 0.1;
        }
        
        probability.min(0.95)
    }

    async fn simulate_arbitrage(
        &self,
        tx: &PendingTransaction,
        pool_address: &Pubkey,
        current_price: u64,
    ) -> Option<SimulationResult> {
        let timeout = tokio::time::timeout(
            Duration::from_millis(SIMULATION_TIMEOUT_MS),
            self.run_arbitrage_simulation(tx, pool_address, current_price)
        ).await;
        
        match timeout {
            Ok(result) => result,
            Err(_) => None,
        }
    }

    async fn run_arbitrage_simulation(
        &self,
        tx: &PendingTransaction,
        pool_address: &Pubkey,
        current_price: u64,
    ) -> Option<SimulationResult> {
        let pool_account = self.rpc_client.get_account(pool_address).await.ok()?;
        let (reserve_a, reserve_b) = self.extract_reserves(&pool_account.data)?;
        
        let optimal_amount = self.calculate_optimal_arbitrage_amount(reserve_a, reserve_b, current_price);
        let output_amount = self.calculate_swap_output(optimal_amount, reserve_a, reserve_b)?;
        
        let gas_estimate = tx.compute_units + 50_000;
        let profit = output_amount as i64 - optimal_amount as i64 - gas_estimate as i64;
        
        Some(SimulationResult {
            success: profit > 0,
            profit,
            gas_used: gas_estimate,
            logs: vec![],
            account_changes: HashMap::new(),
        })
    }

    fn extract_reserves(&self, pool_data: &[u8]) -> Option<(u64, u64)> {
        if pool_data.len() < 80 {
            return None;
        }
        
        let reserve_a = u64::from_le_bytes(pool_data[64..72].try_into().ok()?);
        let reserve_b = u64::from_le_bytes(pool_data[72..80].try_into().ok()?);
        
        if reserve_a > 0 && reserve_b > 0 {
            Some((reserve_a, reserve_b))
        } else {
            None
        }
    }

    fn calculate_optimal_arbitrage_amount(&self, reserve_a: u64, reserve_b: u64, target_price: u64) -> u64 {
        let current_price = (reserve_b as f64 * 1e9) / reserve_a as f64;
        let price_diff = (target_price as f64 - current_price).abs() / current_price;
        
        let k = (reserve_a as u128 * reserve_b as u128) as f64;
        let optimal = (k.sqrt() * price_diff * 0.5) as u64;
        
        optimal.min(reserve_a / 10).max(1_000_000)
    }

    fn calculate_swap_output(&self, input_amount: u64, reserve_in: u64, reserve_out: u64) -> Option<u64> {
        if input_amount == 0 || reserve_in == 0 || reserve_out == 0 {
            return None;
        }
        
        let input_with_fee = (input_amount as u128) * 997;
        let numerator = input_with_fee * (reserve_out as u128);
        let denominator = (reserve_in as u128 * 1000) + input_with_fee;
        
        Some((numerator / denominator) as u64)
    }

    async fn simulate_frontrun(
        &self,
        tx: &PendingTransaction,
        frontrun_amount: u64,
        pool_address: &Pubkey,
    ) -> Option<SimulationResult> {
        let pool_account = self.rpc_client.get_account(pool_address).await.ok()?;
        let (reserve_a, reserve_b) = self.extract_reserves(&pool_account.data)?;
        
        let output = self.calculate_swap_output(frontrun_amount, reserve_a, reserve_b)?;
        let new_reserve_a = reserve_a + frontrun_amount;
        let new_reserve_b = reserve_b - output;
        
        let gas_estimate = 250_000;
        
        Some(SimulationResult {
            success: true,
            profit: -(frontrun_amount as i64),
            gas_used: gas_estimate,
            logs: vec![format!("Frontrun: {} -> {}", frontrun_amount, output)],
            account_changes: {
                let mut changes = HashMap::new();
                changes.insert(*pool_address, (new_reserve_a, new_reserve_b));
                changes
            },
        })
    }

    async fn simulate_backrun(
        &self,
        tx: &PendingTransaction,
        backrun_amount: u64,
        pool_address: &Pubkey,
    ) -> Option<SimulationResult> {
        let frontrun_changes = self.simulate_frontrun(tx, backrun_amount / 2, pool_address).await?;
        let (new_reserve_a, new_reserve_b) = frontrun_changes.account_changes.get(pool_address)?;
        
        let victim_output = self.calculate_swap_output(
            self.extract_swap_amount(tx)?,
            *new_reserve_a,
            *new_reserve_b,
        )?;
        
        let final_reserve_a = new_reserve_a + self.extract_swap_amount(tx)?;
        let final_reserve_b = new_reserve_b - victim_output;
        
        let backrun_output = self.calculate_swap_output(backrun_amount, final_reserve_b, final_reserve_a)?;
        let profit = backrun_output as i64 - backrun_amount as i64;
        
        Some(SimulationResult {
            success: profit > 0,
            profit,
            gas_used: 250_000,
            logs: vec![format!("Backrun: {} -> {}", backrun_amount, backrun_output)],
            account_changes: HashMap::new(),
        })
    }

    async fn simulate_liquidation(
        &self,
        tx: &PendingTransaction,
        liquidation_account: &Pubkey,
    ) -> Option<SimulationResult> {
        let account_data = self.rpc_client.get_account(liquidation_account).await.ok()?;
        
        let collateral_value = u64::from_le_bytes(account_data.data[32..40].try_into().ok()?);
        let debt_value = u64::from_le_bytes(account_data.data[40..48].try_into().ok()?);
        
        if debt_value == 0 || collateral_value <= debt_value {
            return None;
        }
        
        let liquidation_bonus = collateral_value * 5 / 100;
        let repay_amount = debt_value / 2; // 50% liquidation
        let collateral_received = (repay_amount * 105) / 100;
        
        let profit = collateral_received as i64 - repay_amount as i64;
        
        Some(SimulationResult {
            success: profit > 0,
            profit,
            gas_used: 350_000,
            logs: vec![format!("Liquidation profit: {}", profit)],
            account_changes: HashMap::new(),
        })
    }

    async fn fetch_collateral_info(&self, account: &Pubkey) -> Option<CollateralInfo> {
        let account_data = self.rpc_client.get_account(account).await.ok()?;
        
        if account_data.data.len() < 80 {
            return None;
        }
        
        let collateral_value = u64::from_le_bytes(account_data.data[32..40].try_into().ok()?);
        let debt_value = u64::from_le_bytes(account_data.data[40..48].try_into().ok()?);
        let collateral_mint = Pubkey::new(&account_data.data[48..80]);
        
        let health_factor = if debt_value > 0 {
            collateral_value as f64 / debt_value as f64
        } else {
            f64::MAX
        };
        
        Some(CollateralInfo {
            collateral_value,
            debt_value,
            health_factor,
            collateral_mint,
        })
    }

    async fn fetch_collection_stats(&self, mint: &Pubkey) -> Option<CollectionStats> {
        // Simulate fetching from on-chain NFT marketplace data
        let mock_floor = 2_000_000_000 + (mint.to_bytes()[0] as u64 * 10_000_000);
        
        Some(CollectionStats {
            floor_price: mock_floor,
            volume_24h: mock_floor * 100,
            listed_count: 50,
        })
    }

    pub async fn cleanup_expired_transactions(&self) {
        let current_slot = self.get_current_slot().await;
        let mut expired_count = 0;
        
        self.pending_cache.retain(|_, tx| {
            let age = current_slot.saturating_sub(tx.slot);
            if age > TX_EXPIRY_SLOTS {
                expired_count += 1;
                false
            } else {
                true
            }
        });
        
        if expired_count > 0 {
            log::debug!("Cleaned up {} expired transactions", expired_count);
        }
    }

    async fn get_current_slot(&self) -> Slot {
        self.rpc_client
            .get_slot()
            .await
            .unwrap_or(0)
    }

    pub fn get_stats(&self) -> AnalyzerStats {
        AnalyzerStats {
            processed_count: self.processed_count.load(Ordering::Relaxed),
            profitable_count: self.profitable_count.load(Ordering::Relaxed),
            cache_size: self.pending_cache.len(),
            success_rates: self.get_success_rates(),
        }
    }

    fn get_success_rates(&self) -> HashMap<OpportunityType, f64> {
        let mut rates = HashMap::new();
        
        for entry in self.historical_success.iter() {
            let opportunity_type = *entry.key();
            let metrics = entry.value();
            
            let attempts = metrics.attempts.load(Ordering::Relaxed);
            if attempts > 0 {
                let success_rate = metrics.successes.load(Ordering::Relaxed) as f64 / attempts as f64;
                rates.insert(opportunity_type, success_rate);
            }
        }
        
        rates
    }

    pub fn update_success_metrics(&self, opportunity_type: OpportunityType, success: bool, profit: u64) {
        let entry = self.historical_success.entry(opportunity_type).or_insert_with(Default::default);
        
        entry.attempts.fetch_add(1, Ordering::Relaxed);
        if success {
            entry.successes.fetch_add(1, Ordering::Relaxed);
            entry.total_profit.fetch_add(profit, Ordering::Relaxed);
        }
    }

    pub fn shutdown(&self) {
        self.active.store(false, Ordering::Relaxed);
    }
}

#[derive(Debug)]
struct CollateralInfo {
    collateral_value: u64,
    debt_value: u64,
    health_factor: f64,
    collateral_mint: Pubkey,
}

#[derive(Debug)]
struct CollectionStats {
    floor_price: u64,
    volume_24h: u64,
    listed_count: u32,
}

#[derive(Debug)]
pub struct AnalyzerStats {
    pub processed_count: u64,
    pub profitable_count: u64,
    pub cache_size: usize,
    pub success_rates: HashMap<OpportunityType, f64>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use solana_sdk::signature::Keypair;

    #[tokio::test]
    async fn test_swap_output_calculation() {
        let analyzer = PendingTxAnalyzer::new(
            Arc::new(RpcClient::new("https://api.mainnet-beta.solana.com".to_string())),
            mpsc::unbounded_channel().0,
        );
        
        let output = analyzer.calculate_swap_output(1_000_000_000, 100_000_000_000, 50_000_000_000);
        assert!(output.is_some());
        assert!(output.unwrap() > 0);
    }

    #[test]
    fn test_sandwich_amount_calculation() {
        let analyzer = PendingTxAnalyzer::new(
            Arc::new(RpcClient::new("https://api.mainnet-beta.solana.com".to_string())),
            mpsc::unbounded_channel().0,
        );
        
        let (frontrun, backrun) = analyzer.calculate_sandwich_amounts(10_000_000_000, 1_000_000_000);
        assert!(frontrun > 0);
        assert!(backrun > frontrun);
        assert!(frontrun <= 50_000_000_000);
    }
}
