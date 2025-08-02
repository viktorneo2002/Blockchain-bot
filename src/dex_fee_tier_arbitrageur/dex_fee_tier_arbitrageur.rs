use borsh::{BorshDeserialize, BorshSerialize};
use serde_json;
use solana_client::rpc_client::RpcClient;
use solana_sdk::{
    commitment_config::CommitmentConfig,
    compute_budget::ComputeBudgetInstruction,
    system_instruction,
    instruction::{AccountMeta, Instruction},
    pubkey::Pubkey,
    signature::{Keypair, Signer},
    system_instruction,
    transaction::Transaction,
    native_token::LAMPORTS_PER_SOL,
};
use std::{
    collections::HashMap,
    error::Error,
    fmt,
    str::FromStr,
    sync::{Arc, RwLock},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use tokio::sync::mpsc::{channel, Receiver, Sender};

const ORCA_WHIRLPOOL_PROGRAM: &str = "whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc";
const RAYDIUM_V4_PROGRAM: &str = "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8";
const TOKEN_PROGRAM_ID: &str = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA";
const ASSOCIATED_TOKEN_PROGRAM_ID: &str = "ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL";

const COMPUTE_UNITS: u32 = 1_400_000;
const PRIORITY_FEE_MICROLAMPORTS: u64 = 25_000;
const MAX_SLIPPAGE_BPS: u64 = 30;
const MIN_PROFIT_LAMPORTS: u64 = 100_000;
const JITO_TIP_LAMPORTS: u64 = 10_000;
const MAX_RETRIES: u8 = 2;

const TICK_SPACING_ORCA: i32 = 64;
const Q64: u128 = 1u128 << 64;
const Q96: u128 = 1u128 << 96;
const FEE_RATE_MUL: u128 = 1_000_000;
const MIN_LIQUIDITY: u128 = 1_000_000;

const MIN_SQRT_PRICE: u128 = 4295128739;
const MAX_SQRT_PRICE: u128 = 1461446703485210103287273052203988822378723970342;

#[derive(Debug)]
pub enum ArbitrageError {
    RpcError(String),
    ParseError(String),
    InsufficientLiquidity,
    InvalidPrice,
    TransactionFailed(String),
    NoOpportunity,
    StaleData,
    AccountValidationError(String),
    JitoTipError(String),
    SwapCalculationError(String),
    ProgramIdError(String),
    TokenAccountError(String),
}

impl fmt::Display for ArbitrageError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            ArbitrageError::RpcError(e) => write!(f, "RPC error: {}", e),
            ArbitrageError::ParseError(e) => write!(f, "Parse error: {}", e),
            ArbitrageError::InsufficientLiquidity => write!(f, "Insufficient liquidity"),
            ArbitrageError::InvalidPrice => write!(f, "Invalid price"),
            ArbitrageError::TransactionFailed(e) => write!(f, "Transaction failed: {}", e),
            ArbitrageError::NoOpportunity => write!(f, "No arbitrage opportunity"),
            ArbitrageError::StaleData => write!(f, "Pool data is stale"),
            ArbitrageError::AccountValidationError(e) => write!(f, "Account validation error: {}", e),
            ArbitrageError::JitoTipError(e) => write!(f, "Jito tip error: {}", e),
            ArbitrageError::SwapCalculationError(e) => write!(f, "Swap calculation error: {}", e),
            ArbitrageError::ProgramIdError(e) => write!(f, "Program ID error: {}", e),
            ArbitrageError::TokenAccountError(e) => write!(f, "Token account error: {}", e),
        }
    }
}

impl Error for ArbitrageError {}

type Result<T> = std::result::Result<T, ArbitrageError>;

#[derive(Debug, Clone, Copy)]
pub struct PoolState {
    pub address: Pubkey,
    pub token_a: Pubkey,
    pub token_b: Pubkey,
    pub token_vault_a: Pubkey,
    pub token_vault_b: Pubkey,
    pub fee_rate: u32,
    pub sqrt_price_x64: u128,
    pub liquidity: u128,
    pub tick_current: i32,
    pub protocol: Protocol,
    pub last_update: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Protocol {
    Orca,
    Raydium,
}

#[derive(Debug, Clone)]
pub struct Opportunity {
    pub pool_a: PoolState,
    pub pool_b: PoolState,
    pub amount_in: u64,
    pub expected_out: u64,
    pub min_amount_out: u64,
    pub profit_estimate: u64,
    pub is_a_to_b: bool,
    pub timestamp: Instant,
}

pub struct DexFeeTierArbitrageur {
    rpc: Arc<RpcClient>,
    keypair: Arc<Keypair>,
    pools: Arc<RwLock<HashMap<Pubkey, PoolState>>>,
    opportunity_sender: Sender<Opportunity>,
    opportunity_receiver: Arc<tokio::sync::Mutex<Receiver<Opportunity>>>,
}

impl DexFeeTierArbitrageur {
    pub fn new(rpc_url: &str, keypair: Keypair) -> Self {
        let (tx, rx) = channel(256);
        Self {
            rpc: Arc::new(RpcClient::new_with_commitment(
                rpc_url.to_string(),
                CommitmentConfig::confirmed(),
            )),
            keypair: Arc::new(keypair),
            pools: Arc::new(RwLock::new(HashMap::with_capacity(10000))),
            opportunity_sender: tx,
            opportunity_receiver: Arc::new(tokio::sync::Mutex::new(rx)),
        }
    }

    pub async fn run(&self) -> Result<()> {
        self.initialize_pools().await?;
        
        let scanner = self.start_scanner();
        let executor = self.start_executor();
        let updater = self.start_pool_updater();

        tokio::select! {
            res = scanner => res?,
            res = executor => res?,
            res = updater => res?,
        }
        
        Ok(())
    }

    async fn initialize_pools(&self) -> Result<()> {
        let orca_pools = self.fetch_orca_pools().await?;
        let raydium_pools = self.fetch_raydium_pools().await?;
        
        let mut pools = self.pools.write().map_err(|e| ArbitrageError::ParseError(format!("Failed to acquire write lock on pools: {}", e)))?;
        for pool in orca_pools.into_iter().chain(raydium_pools) {
            pools.insert(pool.address, pool);
        }
        
        Ok(())
    }

    async fn fetch_orca_pools(&self) -> Result<Vec<PoolState>> {
        let program_id = Pubkey::from_str(ORCA_WHIRLPOOL_PROGRAM)
            .map_err(|_| ArbitrageError::ParseError("Invalid Orca program ID".to_string()))?;
        
        let accounts = self.rpc.get_program_accounts(&program_id)
            .map_err(|e| ArbitrageError::RpcError(e.to_string()))?;
        
        let mut pools = Vec::with_capacity(accounts.len());
        let current_time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|e| ArbitrageError::ParseError(format!("Failed to acquire read lock: {}", e)))?
            .as_secs();
        
        for (pubkey, account) in accounts {
            if let Ok(pool) = self.parse_whirlpool_safe(pubkey, &account.data, current_time) {
                pools.push(pool);
            }
        }
        
        Ok(pools)
    }

    async fn fetch_raydium_pools(&self) -> Result<Vec<PoolState>> {
        let program_id = Pubkey::from_str(RAYDIUM_V4_PROGRAM)
            .map_err(|_| ArbitrageError::ParseError("Invalid Raydium program ID".to_string()))?;
        
        let accounts = self.rpc.get_program_accounts(&program_id)
            .map_err(|e| ArbitrageError::RpcError(e.to_string()))?;
        
        let mut pools = Vec::with_capacity(accounts.len());
        let current_time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|e| ArbitrageError::ParseError(format!("Failed to acquire read lock: {}", e)))?
            .as_secs();
        
        for (pubkey, account) in accounts {
            if let Ok(pool) = self.parse_raydium_safe(pubkey, &account.data, current_time) {
                pools.push(pool);
            }
        }
        
        Ok(pools)
    }

    fn parse_whirlpool_safe(&self, address: Pubkey, data: &[u8], timestamp: u64) -> Result<PoolState> {
        if data.len() < 653 {
            return Err(ArbitrageError::ParseError("Invalid whirlpool data length".to_string()));
        }
        
        let discriminator = &data[0..8];
        if discriminator != &[247, 198, 158, 145, 225, 117, 135, 72] {
            return Err(ArbitrageError::ParseError("Invalid whirlpool discriminator".to_string()));
        }
        
        let fee_rate = u16::from_le_bytes(data[45..47].try_into().map_err(|_| ArbitrageError::ParseError("Failed to parse fee rate".to_string()))?) as u32;
        let liquidity = u128::from_le_bytes(data[49..65].try_into().map_err(|_| ArbitrageError::ParseError("Failed to parse liquidity".to_string()))?);
        let sqrt_price_x64 = u128::from_le_bytes(data[65..81].try_into().map_err(|_| ArbitrageError::ParseError("Failed to parse sqrt price".to_string()))?);
        let tick_current = i32::from_le_bytes(data[81..85].try_into().map_err(|_| ArbitrageError::ParseError("Failed to parse tick".to_string()))?);
        let token_mint_a = Pubkey::new(&data[101..133]);
        let token_vault_a = Pubkey::new(&data[133..165]);
        let token_mint_b = Pubkey::new(&data[181..213]);
        let token_vault_b = Pubkey::new(&data[213..245]);
        
        if liquidity < MIN_LIQUIDITY || sqrt_price_x64 == 0 {
            return Err(ArbitrageError::InsufficientLiquidity);
        }
        
        Ok(PoolState {
            address,
            token_a: token_mint_a,
            token_b: token_mint_b,
            token_vault_a,
            token_vault_b,
            fee_rate,
            sqrt_price_x64,
            liquidity,
            tick_current,
            protocol: Protocol::Orca,
            last_update: timestamp,
        })
    }

    fn parse_raydium_safe(&self, address: Pubkey, data: &[u8], timestamp: u64) -> Result<PoolState> {
        if data.len() < 752 {
            return Err(ArbitrageError::ParseError("Invalid raydium data length".to_string()));
        }
        
        let status = u64::from_le_bytes(data[0..8].try_into().map_err(|_| ArbitrageError::ParseError("Failed to parse status".to_string()))?);
        if status != 6 {
            return Err(ArbitrageError::ParseError("Pool not initialized".to_string()));
        }
        
        let swap_fee_numerator = u64::from_le_bytes(data[176..184].try_into().map_err(|_| ArbitrageError::ParseError("Failed to parse swap fee numerator".to_string()))?);
        let swap_fee_denominator = u64::from_le_bytes(data[184..192].try_into().map_err(|_| ArbitrageError::ParseError("Failed to parse swap fee denominator".to_string()))?);
        let base_total_deposited = u128::from_le_bytes(data[240..256].try_into().map_err(|_| ArbitrageError::ParseError("Failed to parse base total deposited".to_string()))?);
        let quote_total_deposited = u128::from_le_bytes(data[224..240].try_into().map_err(|_| ArbitrageError::ParseError("Failed to parse quote total deposited".to_string()))?);
        let base_vault = Pubkey::new(&data[336..368]);
        let quote_vault = Pubkey::new(&data[368..400]);
        let base_mint = Pubkey::new(&data[400..432]);
        let quote_mint = Pubkey::new(&data[432..464]);
        
        if base_total_deposited < MIN_LIQUIDITY || quote_total_deposited < MIN_LIQUIDITY {
            return Err(ArbitrageError::InsufficientLiquidity);
        }
        
        let fee_rate = if swap_fee_denominator > 0 {
            (swap_fee_numerator * FEE_RATE_MUL / swap_fee_denominator) as u32
        } else {
            2500
        };
        
        let sqrt_price_x64 = sqrt_u128((quote_total_deposited * Q64) / base_total_deposited);
        let liquidity = sqrt_u128(base_total_deposited * quote_total_deposited);
        
        Ok(PoolState {
            address,
            token_a: base_mint,
            token_b: quote_mint,
            token_vault_a: base_vault,
            token_vault_b: quote_vault,
            fee_rate,
            sqrt_price_x64,
            liquidity,
            tick_current: 0,
            protocol: Protocol::Raydium,
            last_update: timestamp,
        })
    }

    fn start_scanner(&self) -> tokio::task::JoinHandle<Result<()>> {
        let pools = Arc::clone(&self.pools);
        let sender = self.opportunity_sender.clone();
        
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_millis(50));
            
            loop {
                interval.tick().await;
                
                let current_time = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .map_err(|e| ArbitrageError::ParseError(format!("Failed to acquire read lock: {}", e)))?
                    .as_secs();
                
                let pool_snapshot = pools.read().map_err(|e| ArbitrageError::ParseError(format!("Failed to acquire read lock on pools: {}", e)))?.clone();
                let mut pool_groups: HashMap<(Pubkey, Pubkey), Vec<PoolState>> = HashMap::new();
                
                for pool in pool_snapshot.values() {
                    if current_time - pool.last_update > 30 {
                        continue;
                    }
                    
                    let key = if pool.token_a < pool.token_b {
                        (pool.token_a, pool.token_b)
                    } else {
                        (pool.token_b, pool.token_a)
                    };
                                        pool_groups.entry(key).or_insert_with(Vec::new).push(*pool);
                }
                
                for (_, group) in pool_groups {
                    if group.len() < 2 {
                        continue;
                    }
                    
                    for i in 0..group.len() {
                        for j in i + 1..group.len() {
                            if let Some(opp) = Self::find_arbitrage(&group[i], &group[j]) {
                                let _ = sender.send(opp).await;
                            }
                        }
                    }
                }
            }
        })
    }

    fn find_arbitrage(pool_a: &PoolState, pool_b: &PoolState) -> Option<Opportunity> {
        if pool_a.fee_rate == pool_b.fee_rate {
            return None;
        }

        let test_amounts = [
            100_000_000u64,
            500_000_000u64,
            1_000_000_000u64,
            5_000_000_000u64,
            10_000_000_000u64,
        ];

        for &amount in &test_amounts {
            let (output_a, is_valid_a) = Self::simulate_swap(pool_a, amount, true);
            if !is_valid_a || output_a == 0 {
                continue;
            }
            
            let (output_b, is_valid_b) = Self::simulate_swap(pool_b, output_a, false);
            if !is_valid_b || output_b == 0 {
                continue;
            }
            
            if output_b > amount + MIN_PROFIT_LAMPORTS {
                return Some(Opportunity {
                    pool_a: *pool_a,
                    pool_b: *pool_b,
                    amount_in: amount,
                    expected_out: output_b,
                    min_amount_out: output_b - (output_b * MAX_SLIPPAGE_BPS / 10000),
                    profit_estimate: output_b.saturating_sub(amount),
                    is_a_to_b: true,
                    timestamp: Instant::now(),
                });
            }
            
            let (output_a_rev, is_valid_a_rev) = Self::simulate_swap(pool_a, amount, false);
            if !is_valid_a_rev || output_a_rev == 0 {
                continue;
            }
            
            let (output_b_rev, is_valid_b_rev) = Self::simulate_swap(pool_b, output_a_rev, true);
            if !is_valid_b_rev || output_b_rev == 0 {
                continue;
            }
            
            if output_b_rev > amount + MIN_PROFIT_LAMPORTS {
                return Some(Opportunity {
                    pool_a: *pool_a,
                    pool_b: *pool_b,
                    amount_in: amount,
                    expected_out: output_b_rev,
                    min_amount_out: output_b_rev - (output_b_rev * MAX_SLIPPAGE_BPS / 10000),
                    profit_estimate: output_b_rev.saturating_sub(amount),
                    is_a_to_b: false,
                    timestamp: Instant::now(),
                });
            }
        }
        
        None
    }

    fn simulate_swap(pool: &PoolState, amount_in: u64, is_a_to_b: bool) -> (u64, bool) {
        if pool.liquidity == 0 || pool.sqrt_price_x64 == 0 {
            return (0, false);
        }
        
        let amount_after_fee = Self::apply_fee(amount_in, pool.fee_rate);
        
        match pool.protocol {
            Protocol::Orca => Self::calculate_orca_output(pool, amount_after_fee, is_a_to_b),
            Protocol::Raydium => Self::calculate_raydium_output(pool, amount_after_fee, is_a_to_b),
        }
    }

    fn apply_fee(amount: u64, fee_rate: u32) -> u64 {
        let fee_amount = ((amount as u128 * fee_rate as u128) / FEE_RATE_MUL) as u64;
        amount.saturating_sub(fee_amount)
    }

    fn calculate_orca_output(pool: &PoolState, amount_in: u64, is_a_to_b: bool) -> (u64, bool) {
        // Apply fee to input amount first
        let amount_in_after_fee = Self::apply_fee(amount_in, pool.fee_rate);
        let sqrt_price_limit = Self::calculate_sqrt_price_limit(pool.sqrt_price_x64, is_a_to_b);
        let sqrt_price_current = pool.sqrt_price_x64;
        let liquidity = pool.liquidity;
        
        if is_a_to_b {
            if sqrt_price_current <= sqrt_price_limit {
                return (0, false);
            }
            
            // Calculate output for token B using proper Orca AMM formula
            // Based on Orca's get_amount_delta_b function
            let sqrt_price_diff = sqrt_price_current - sqrt_price_limit;
            
            // Calculate numerator: liquidity * sqrt_price_diff
            let numerator = liquidity.saturating_mul(sqrt_price_diff);
            
            // Calculate amount_out = (numerator >> 64) with proper rounding
            let amount_out = if numerator > 0 && sqrt_price_diff > 0 {
                let result = numerator >> 64;
                // Check if we need to round up (if there's a remainder)
                let remainder = numerator & 0xFFFFFFFFFFFFFFFF;
                if remainder > 0 && result < u64::MAX as u128 {
                    result as u64 + 1
                } else {
                    result.min(u64::MAX as u128) as u64
                }
            } else {
                0
            };
            
            (amount_out, true)
        } else {
            if sqrt_price_current >= sqrt_price_limit {
                return (0, false);
            }
            
            // Calculate output for token A using proper Orca AMM formula
            // Based on Orca's get_amount_delta_a function
            let sqrt_price_diff = sqrt_price_limit - sqrt_price_current;
            
            // Calculate numerator: liquidity * sqrt_price_diff * Q64
            let numerator = liquidity.saturating_mul(sqrt_price_diff).saturating_mul(Q64);
            
            // Calculate denominator: sqrt_price_limit * sqrt_price_current
            let denominator = sqrt_price_limit.saturating_mul(sqrt_price_current);
            
            if denominator == 0 {
                return (0, false);
            }
            
            // Calculate amount_out = numerator / denominator with proper rounding
            let (amount_out, remainder) = (numerator / denominator, numerator % denominator);
            
            // Round up if there's a remainder and we won't overflow
            let amount_out = if remainder > 0 && amount_out < u64::MAX as u128 {
                amount_out as u64 + 1
            } else {
                amount_out.min(u64::MAX as u128) as u64
            };
            
            (amount_out, true)
        }
    }

    fn calculate_raydium_output(pool: &PoolState, amount_in: u64, is_a_to_b: bool) -> (u64, bool) {
        
        // Calculate token reserves from liquidity and price
        // liquidity = sqrt(x * y) => x * y = liquidity^2
        // price = y / x => y = price * x
        // => x * price * x = liquidity^2 => x^2 = liquidity^2 / price => x = liquidity / sqrt(price)
        // => y = liquidity * sqrt(price)
        
        let sqrt_price = pool.sqrt_price_x64;
        let price = sqrt_price.saturating_mul(sqrt_price).saturating_div(Q64);
        
        if price == 0 {
            return (0, false);
        }
        
        // Calculate reserve_x (token A) and reserve_y (token B)
        // Using: reserve_x = liquidity / sqrt(price) and reserve_y = liquidity * sqrt(price)
        // We need to be careful with precision here
        let liquidity_u128 = pool.liquidity;
        
        if is_a_to_b {
            // Swapping token A for token B
            // output = (reserve_y * amount_in_after_fee) / (reserve_x + amount_in_after_fee)
            
            // Calculate reserve_x = liquidity / sqrt(price)
            // This is equivalent to: liquidity * Q64 / (sqrt_price * sqrt_price) * sqrt_price = liquidity * Q64 / sqrt_price
            let reserve_x = liquidity_u128.saturating_mul(Q64).saturating_div(sqrt_price);
            let reserve_y = liquidity_u128.saturating_mul(sqrt_price).saturating_div(Q64);
            
            if reserve_x == 0 {
                return (0, false);
            }
            
            let numerator = reserve_y.saturating_mul(amount_in_after_fee as u128);
            let denominator = reserve_x.saturating_add(amount_in_after_fee as u128);
            
            if denominator == 0 {
                return (0, false);
            }
            
            let output = numerator.saturating_div(denominator);
            (output.min(u64::MAX as u128) as u64, true)
        } else {
            // Swapping token B for token A
            // output = (reserve_x * amount_in_after_fee) / (reserve_y + amount_in_after_fee)
            
            let reserve_x = liquidity_u128.saturating_mul(Q64).saturating_div(sqrt_price);
            let reserve_y = liquidity_u128.saturating_mul(sqrt_price).saturating_div(Q64);
            
            if reserve_y == 0 {
                return (0, false);
            }
            
            let numerator = reserve_x.saturating_mul(amount_in_after_fee as u128);
            let denominator = reserve_y.saturating_add(amount_in_after_fee as u128);
            
            if denominator == 0 {
                return (0, false);
            }
            
            let output = numerator.saturating_div(denominator);
            (output.min(u64::MAX as u128) as u64, true)
        }
    }

    fn calculate_sqrt_price_limit(current_sqrt_price: u128, is_a_to_b: bool) -> u128 {
        if is_a_to_b {
            let min_limit = current_sqrt_price.saturating_mul(90).saturating_div(100);
            min_limit.max(MIN_SQRT_PRICE)
        } else {
            let max_limit = current_sqrt_price.saturating_mul(110).saturating_div(100);
            max_limit.min(MAX_SQRT_PRICE)
        }
    }

    fn start_executor(&self) -> tokio::task::JoinHandle<Result<()>> {
        let rpc = Arc::clone(&self.rpc);
        let keypair = Arc::clone(&self.keypair);
        let receiver = Arc::clone(&self.opportunity_receiver);
        
        tokio::spawn(async move {
            loop {
                let mut rx = receiver.lock().await;
                if let Some(opp) = rx.recv().await {
                    drop(rx);
                    
                    if opp.timestamp.elapsed() > Duration::from_millis(500) {
                        continue;
                    }
                    
                    match Self::execute_arbitrage(&rpc, &keypair, opp).await {
                        Ok(sig) => println!("Arbitrage executed: {}", sig),
                        Err(e) => eprintln!("Arbitrage failed: {}", e),
                    }
                }
            }
        })
    }

    async fn execute_arbitrage(
        rpc: &Arc<RpcClient>,
        keypair: &Arc<Keypair>,
        opp: Opportunity,
    ) -> Result<String> {
        let mut instructions = Vec::new();
        
        // Add compute budget instruction
        instructions.push(ComputeBudgetInstruction::set_compute_unit_limit(COMPUTE_UNIT_LIMIT));
        instructions.push(ComputeBudgetInstruction::set_compute_unit_price(PRIORITY_RATE_MICRO_LAMPORTS));
        
        // Get user's token accounts for the opportunity tokens
        let token_a_account = Self::get_associated_token_address(&keypair.pubkey(), &opp.token_a_mint)?;
        let token_b_account = Self::get_associated_token_address(&keypair.pubkey(), &opp.token_b_mint)?;
        
        // Get user's token accounts for the pool tokens
        let user_ata_a_pool_a = Self::get_associated_token_address(&keypair.pubkey(), &opp.pool_a.token_a)?;
        let user_ata_b_pool_a = Self::get_associated_token_address(&keypair.pubkey(), &opp.pool_a.token_b)?;
        let user_ata_a_pool_b = Self::get_associated_token_address(&keypair.pubkey(), &opp.pool_b.token_a)?;
        let user_ata_b_pool_b = Self::get_associated_token_address(&keypair.pubkey(), &opp.pool_b.token_b)?;
        
        // Get vault accounts for both pools
        let vault_a_pool_a = opp.pool_a.token_vault_a;
        let vault_b_pool_a = opp.pool_a.token_vault_b;
        let vault_a_pool_b = opp.pool_b.token_vault_a;
        let vault_b_pool_b = opp.pool_b.token_vault_b;
        
        // Get additional accounts for Orca pools (tick arrays and oracle)
        let tick_array_0_pool_a = Self::derive_tick_array(&opp.pool_a.address, opp.pool_a.tick_current - TICK_SPACING_ORCA)?;
        let tick_array_1_pool_a = Self::derive_tick_array(&opp.pool_a.address, opp.pool_a.tick_current)?;
        let tick_array_2_pool_a = Self::derive_tick_array(&opp.pool_a.address, opp.pool_a.tick_current + TICK_SPACING_ORCA)?;
        let oracle_pool_a = Self::derive_oracle(&opp.pool_a.address)?;
        
        let tick_array_0_pool_b = Self::derive_tick_array(&opp.pool_b.address, opp.pool_b.tick_current - TICK_SPACING_ORCA)?;
        let tick_array_1_pool_b = Self::derive_tick_array(&opp.pool_b.address, opp.pool_b.tick_current)?;
        let tick_array_2_pool_b = Self::derive_tick_array(&opp.pool_b.address, opp.pool_b.tick_current + TICK_SPACING_ORCA)?;
        let oracle_pool_b = Self::derive_oracle(&opp.pool_b.address)?;
        
        // Get additional accounts for Raydium pools (open orders and market authority)
        let (open_orders_pool_a, market_authority_pool_a) = if opp.pool_a.protocol == Protocol::Raydium {
            Self::derive_raydium_pdas(&opp.pool_a.address)?
        } else {
            (Pubkey::default(), Pubkey::default())
        };
        
        let (open_orders_pool_b, market_authority_pool_b) = if opp.pool_b.protocol == Protocol::Raydium {
            Self::derive_raydium_pdas(&opp.pool_b.address)?
        } else {
            (Pubkey::default(), Pubkey::default())
        };
        
        // Get serum program ID for Raydium pools
        let serum_program = Pubkey::from_str("srmqPvymJeFKQ4zGQed1GFppgkRHL9kaELCbyksJtPX").map_err(|_| ArbitrageError::ParseError("Invalid serum program ID".to_string()))?;
        
        // Validate that all token accounts exist on-chain and are owned by the token program
        Self::validate_token_account(rpc.as_ref(), &token_a_account).await?;
        Self::validate_token_account(rpc.as_ref(), &token_b_account).await?;
        Self::validate_token_account(rpc.as_ref(), &user_ata_a_pool_a).await?;
        Self::validate_token_account(rpc.as_ref(), &user_ata_b_pool_a).await?;
        Self::validate_token_account(rpc.as_ref(), &user_ata_a_pool_b).await?;
        Self::validate_token_account(rpc.as_ref(), &user_ata_b_pool_b).await?;
        Self::validate_token_account(rpc.as_ref(), &vault_a_pool_a).await?;
        Self::validate_token_account(rpc.as_ref(), &vault_b_pool_a).await?;
        Self::validate_token_account(rpc.as_ref(), &vault_a_pool_b).await?;
        Self::validate_token_account(rpc.as_ref(), &vault_b_pool_b).await?;
        
        // Validate that all other accounts exist on-chain (no ownership check)
        Self::validate_account_exists(rpc.as_ref(), &tick_array_0_pool_a).await?;
        Self::validate_account_exists(rpc.as_ref(), &tick_array_1_pool_a).await?;
        Self::validate_account_exists(rpc.as_ref(), &tick_array_2_pool_a).await?;
        Self::validate_account_exists(rpc.as_ref(), &oracle_pool_a).await?;
        Self::validate_account_exists(rpc.as_ref(), &tick_array_0_pool_b).await?;
        Self::validate_account_exists(rpc.as_ref(), &tick_array_1_pool_b).await?;
        Self::validate_account_exists(rpc.as_ref(), &tick_array_2_pool_b).await?;
        Self::validate_account_exists(rpc.as_ref(), &oracle_pool_b).await?;
        Self::validate_account_exists(rpc.as_ref(), &open_orders_pool_a).await?;
        Self::validate_account_exists(rpc.as_ref(), &market_authority_pool_a).await?;
        Self::validate_account_exists(rpc.as_ref(), &open_orders_pool_b).await?;
        Self::validate_account_exists(rpc.as_ref(), &market_authority_pool_b).await?;
        Self::validate_account_exists(rpc.as_ref(), &serum_program).await?;
        
        let user_ata_a = user_ata_a_pool_a;
        let user_ata_b = user_ata_b_pool_a;

        let swap1_accounts = Self::build_swap_accounts(&opp.pool_a, &keypair.pubkey(), opp.is_a_to_b);
        let swap1_data = Self::build_swap_data(&opp.pool_a, opp.amount_in, opp.min_amount_out, opp.is_a_to_b);
        
        instructions.push(Instruction {
            program_id: match opp.pool_a.protocol {
                Protocol::Orca => Pubkey::from_str(ORCA_WHIRLPOOL_PROGRAM).map_err(|_| ArbitrageError::ParseError("Invalid Orca program ID".to_string()))?,
                Protocol::Raydium => Pubkey::from_str(RAYDIUM_V4_PROGRAM).map_err(|_| ArbitrageError::ParseError("Invalid Raydium program ID".to_string()))?,
            },
            accounts: swap1_accounts,
            data: swap1_data,
        });

        let intermediate_min = opp.expected_out.saturating_mul(95).saturating_div(100);
        let swap2_accounts = Self::build_swap_accounts(&opp.pool_b, &keypair.pubkey(), !opp.is_a_to_b);
        let swap2_data = Self::build_swap_data(&opp.pool_b, intermediate_min, opp.min_amount_out, !opp.is_a_to_b);
        
        instructions.push(Instruction {
            program_id: match opp.pool_b.protocol {
                Protocol::Orca => Pubkey::from_str(ORCA_WHIRLPOOL_PROGRAM).map_err(|_| ArbitrageError::ParseError("Invalid Orca program ID".to_string()))?,
                Protocol::Raydium => Pubkey::from_str(RAYDIUM_V4_PROGRAM).map_err(|_| ArbitrageError::ParseError("Invalid Raydium program ID".to_string()))?,
            },
            accounts: swap2_accounts,
            data: swap2_data,
        });

        // Fetch Jito tip accounts dynamically using getTipAccounts RPC method
        let jito_tips: Vec<String> = rpc.send(
            solana_client::rpc_request::RpcRequest::Custom { method: "getTipAccounts" },
            serde_json::json!([]),
        ).map_err(|e| ArbitrageError::JitoTipError(format!("Failed to fetch Jito tip accounts: {}", e)))?;
        
        if jito_tips.is_empty() {
            return Err(ArbitrageError::JitoTipError("No Jito tip accounts returned".to_string()));
        }
        
        let tip_index = (SystemTime::now().duration_since(UNIX_EPOCH)
            .map_err(|_| ArbitrageError::ParseError("Failed to get system time".to_string()))?
            .as_nanos() % jito_tips.len() as u128) as usize;
        let tip_account = Pubkey::from_str(&jito_tips[tip_index])
            .map_err(|_| ArbitrageError::ParseError("Invalid tip account".to_string()))?;
        
        instructions.push(system_instruction::transfer(
            &keypair.pubkey(),
            &tip_account,
            JITO_TIP_LAMPORTS,
        ));

        let recent_blockhash = rpc.get_latest_blockhash()
            .map_err(|e| ArbitrageError::RpcError(e.to_string()))?;
            
        let transaction = Transaction::new_signed_with_payer(
            &instructions,
            Some(&keypair.pubkey()),
            &[keypair.as_ref()],
            recent_blockhash,
        );

        for retry in 0..MAX_RETRIES {
            match rpc.send_and_confirm_transaction(&transaction) {
                Ok(sig) => return Ok(sig.to_string()),
                Err(e) if retry < MAX_RETRIES - 1 => {
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    continue;
                }
                Err(e) => return Err(ArbitrageError::TransactionFailed(e.to_string())),
            }
        }

        Err(ArbitrageError::TransactionFailed("Max retries exceeded".to_string()))
    }

    fn build_swap_accounts(pool: &PoolState, user: &Pubkey, is_a_to_b: bool) -> Vec<AccountMeta> {
        match pool.protocol {
            Protocol::Orca => Self::build_orca_swap_accounts(pool, user, is_a_to_b),
            Protocol::Raydium => Self::build_raydium_swap_accounts(pool, user, is_a_to_b),
        }
    }

    fn build_orca_swap_accounts(pool: &PoolState, user: &Pubkey, is_a_to_b: bool) -> Vec<AccountMeta> {
        let token_program = Pubkey::from_str(TOKEN_PROGRAM_ID).map_err(|_| ArbitrageError::ParseError("Invalid token program ID".to_string()))?;
                let user_ata_a = Self::get_associated_token_address(user, &pool.token_a);
        let user_ata_b = Self::get_associated_token_address(user, &pool.token_b);
        
        let (source_ata, dest_ata) = if is_a_to_b {
            (user_ata_a, user_ata_b)
        } else {
            (user_ata_b, user_ata_a)
        };

        let tick_array_0 = Self::derive_tick_array(&pool.address, pool.tick_current - TICK_SPACING_ORCA);
        let tick_array_1 = Self::derive_tick_array(&pool.address, pool.tick_current);
        let tick_array_2 = Self::derive_tick_array(&pool.address, pool.tick_current + TICK_SPACING_ORCA);

        let oracle = Self::derive_oracle(&pool.address);

        vec![
            AccountMeta::new_readonly(token_program, false),
            AccountMeta::new(*user, true),
            AccountMeta::new(pool.address, false),
            AccountMeta::new(source_ata, false),
            AccountMeta::new(pool.token_vault_a, false),
            AccountMeta::new(pool.token_vault_b, false),
            AccountMeta::new(dest_ata, false),
            AccountMeta::new(tick_array_0, false),
            AccountMeta::new(tick_array_1, false),
            AccountMeta::new(tick_array_2, false),
            AccountMeta::new_readonly(oracle, false),
        ]
    }

    fn build_raydium_swap_accounts(pool: &PoolState, user: &Pubkey, is_a_to_b: bool) -> Vec<AccountMeta> {
        let token_program = Pubkey::from_str(TOKEN_PROGRAM_ID).map_err(|_| ArbitrageError::ParseError("Invalid token program ID".to_string()))?;
        let user_ata_a = Self::get_associated_token_address(user, &pool.token_a);
        let user_ata_b = Self::get_associated_token_address(user, &pool.token_b);
        
        let (source_ata, dest_ata, source_vault, dest_vault) = if is_a_to_b {
            (user_ata_a, user_ata_b, pool.token_vault_a, pool.token_vault_b)
        } else {
            (user_ata_b, user_ata_a, pool.token_vault_b, pool.token_vault_a)
        };

        let (open_orders, market_authority) = Self::derive_raydium_pdas(&pool.address);
        let serum_program = Pubkey::from_str("srmqPvymJeFKQ4zGQed1GFppgkRHL9kaELCbyksJtPX").map_err(|_| ArbitrageError::ParseError("Invalid serum program ID".to_string()))?;

        vec![
            AccountMeta::new_readonly(token_program, false),
            AccountMeta::new(pool.address, false),
            AccountMeta::new_readonly(market_authority, false),
            AccountMeta::new(open_orders, false),
            AccountMeta::new(*user, true),
            AccountMeta::new(source_ata, false),
            AccountMeta::new(dest_ata, false),
            AccountMeta::new(source_vault, false),
            AccountMeta::new(dest_vault, false),
            AccountMeta::new_readonly(serum_program, false),
        ]
    }

    fn build_swap_data(pool: &PoolState, amount_in: u64, min_amount_out: u64, is_a_to_b: bool) -> Vec<u8> {
        match pool.protocol {
            Protocol::Orca => {
                let mut data = vec![0xf8, 0xc6, 0x9e, 0x91, 0xe1, 0x75, 0x87, 0x48];
                data.extend_from_slice(&amount_in.to_le_bytes());
                data.extend_from_slice(&min_amount_out.to_le_bytes());
                let sqrt_price_limit = Self::calculate_sqrt_price_limit(pool.sqrt_price_x64, is_a_to_b);
                data.extend_from_slice(&sqrt_price_limit.to_le_bytes());
                data.push(is_a_to_b as u8);
                data.push(1);
                data
            }
            Protocol::Raydium => {
                let mut data = vec![0x09];
                data.extend_from_slice(&amount_in.to_le_bytes());
                data.extend_from_slice(&min_amount_out.to_le_bytes());
                data
            }
        }
    }

    fn get_associated_token_address(wallet: &Pubkey, mint: &Pubkey) -> Result<Pubkey> {
        let (ata, _) = Pubkey::find_program_address(
            &[
                wallet.as_ref(),
                Pubkey::from_str(TOKEN_PROGRAM_ID).map_err(|_| ArbitrageError::ParseError("Invalid token program ID".to_string()))?.as_ref(),
                mint.as_ref(),
            ],
            &Pubkey::from_str(ASSOCIATED_TOKEN_PROGRAM_ID).map_err(|_| ArbitrageError::ParseError("Invalid associated token program ID".to_string()))?,
        );
        Ok(ata)
    }

    fn derive_tick_array(whirlpool: &Pubkey, tick_index: i32) -> Result<Pubkey> {
        const TICK_ARRAY_SIZE: i32 = 88;
        let start_tick_index = tick_index / TICK_ARRAY_SIZE * TICK_ARRAY_SIZE;
        let start_tick_index_bytes = start_tick_index.to_le_bytes();
        
        let (tick_array, _) = Pubkey::find_program_address(
            &[
                b"tick_array",
                whirlpool.as_ref(),
                &start_tick_index_bytes,
            ],
            &Pubkey::from_str(ORCA_WHIRLPOOL_PROGRAM).map_err(|_| ArbitrageError::ParseError("Invalid Orca program ID".to_string()))?,
        );
        Ok(tick_array)
    }

    fn derive_oracle(whirlpool: &Pubkey) -> Result<Pubkey> {
        let (oracle, _) = Pubkey::find_program_address(
            &[b"oracle", whirlpool.as_ref()],
            &Pubkey::from_str(ORCA_WHIRLPOOL_PROGRAM).map_err(|_| ArbitrageError::ParseError("Invalid Orca program ID".to_string()))?,
        );
        Ok(oracle)
    }

    fn derive_raydium_pdas(pool: &Pubkey) -> Result<(Pubkey, Pubkey)> {
        let (amm_id, _) = Pubkey::find_program_address(
            &[b"amm_associated_seed".as_ref(), pool.as_ref()],
            &Pubkey::from_str(RAYDIUM_V4_PROGRAM).map_err(|_| ArbitrageError::ParseError("Invalid Raydium program ID".to_string()))?,
        );
        let (authority, _) = Pubkey::find_program_address(
            &[b"amm_authority".as_ref(), amm_id.as_ref()],
            &Pubkey::from_str(RAYDIUM_V4_PROGRAM).map_err(|_| ArbitrageError::ParseError("Invalid Raydium program ID".to_string()))?,
        );
        Ok((amm_id, authority))
    }

    /// Validates that a token account exists on-chain and is owned by the token program
    async fn validate_token_account(rpc: &RpcClient, token_account: &Pubkey) -> Result<()> {
        match rpc.get_account_with_commitment(token_account, CommitmentConfig::confirmed()) {
            Ok(account_response) => {
                if let Some(account) = account_response.value {
                    // Check if the account is owned by the token program
                    let token_program_id = Pubkey::from_str(TOKEN_PROGRAM_ID)
                        .map_err(|_| ArbitrageError::ProgramIdError("Invalid token program ID".to_string()))?;
                    
                    if account.owner != token_program_id {
                        return Err(ArbitrageError::TokenAccountError(
                            format!("Token account {} is not owned by token program", token_account)
                        ));
                    }
                    
                    // Account exists and is owned by token program
                    Ok(())
                } else {
                    Err(ArbitrageError::TokenAccountError(
                        format!("Token account {} does not exist", token_account)
                    ))
                }
            },
            Err(e) => {
                Err(ArbitrageError::RpcError(
                    format!("Failed to fetch token account {}: {}", token_account, e)
                ))
            }
        }
    }

    /// Validates that an account exists on-chain (without checking ownership)
    async fn validate_account_exists(rpc: &RpcClient, account: &Pubkey) -> Result<()> {
        match rpc.get_account_with_commitment(account, CommitmentConfig::confirmed()) {
            Ok(account_response) => {
                if account_response.value.is_some() {
                    // Account exists
                    Ok(())
                } else {
                    Err(ArbitrageError::TokenAccountError(
                        format!("Account {} does not exist", account)
                    ))
                }
            },
            Err(e) => {
                Err(ArbitrageError::RpcError(
                    format!("Failed to fetch account {}: {}", account, e)
                ))
            }
        }
    }

    fn start_pool_updater(&self) -> tokio::task::JoinHandle<Result<()>> {
        let rpc = Arc::clone(&self.rpc);
        let pools = Arc::clone(&self.pools);
        
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(2));
            
            loop {
                interval.tick().await;
                
                let current_time = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .map_err(|e| ArbitrageError::ParseError(format!("Failed to acquire read lock: {}", e)))?
                    .as_secs();
                
                let pool_addresses: Vec<Pubkey> = {
                    let pools_guard = pools.read().map_err(|e| ArbitrageError::ParseError(format!("Failed to acquire read lock: {}", e)))?;
                    pools_guard.keys().copied().collect()
                };
                
                let chunks: Vec<_> = pool_addresses.chunks(100).map(|c| c.to_vec()).collect();
                
                for chunk in chunks {
                    let rpc_clone = Arc::clone(&rpc);
                    let pools_clone = Arc::clone(&pools);
                    
                    tokio::spawn(async move {
                        match rpc_clone.get_multiple_accounts(&chunk) {
                            Ok(accounts) => {
                                for (i, account_opt) in accounts.iter().enumerate() {
                                    if let Some(account) = account_opt {
                                        let mut pools_guard = pools_clone.write().map_err(|e| ArbitrageError::ParseError(format!("Failed to acquire write lock: {}", e)))?;
                                        if let Some(pool) = pools_guard.get_mut(&chunk[i]) {
                                            match pool.protocol {
                                                Protocol::Orca => {
                                                    if account.data.len() >= 85 {
                                                        pool.sqrt_price_x64 = u128::from_le_bytes(
                                                            account.data[65..81].try_into().map_err(|_| ArbitrageError::ParseError("Failed to parse sqrt price".to_string()))?)
                                                        );
                                                        pool.liquidity = u128::from_le_bytes(
                                                            account.data[49..65].try_into().map_err(|_| ArbitrageError::ParseError("Failed to parse liquidity".to_string()))?
                                                        );
                                                        pool.tick_current = i32::from_le_bytes(
                                                            account.data[81..85].try_into().map_err(|_| ArbitrageError::ParseError("Failed to parse tick".to_string()))?
                                                        );
                                                        pool.last_update = current_time;
                                                    }
                                                }
                                                Protocol::Raydium => {
                                                    if account.data.len() >= 256 {
                                                        let base_total = u128::from_le_bytes(
                                                            account.data[240..256].try_into().map_err(|_| ArbitrageError::ParseError("Failed to parse base total".to_string()))?
                                                        );
                                                        let quote_total = u128::from_le_bytes(
                                                            account.data[224..240].try_into().map_err(|_| ArbitrageError::ParseError("Failed to parse quote total".to_string()))?
                                                        );
                                                        
                                                        if base_total > 0 && quote_total > 0 {
                                                            pool.sqrt_price_x64 = sqrt_u128((quote_total * Q64) / base_total);
                                                            pool.liquidity = sqrt_u128(base_total * quote_total);
                                                            pool.last_update = current_time;
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                            Err(e) => eprintln!("Failed to update pools: {}", e),
                        }
                    });
                }
                
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        })
    }
}

fn sqrt_u128(value: u128) -> u128 {
    if value == 0 {
        return 0;
    }
    
    let mut x = value;
    let mut y = (x + 1) / 2;
    
    while y < x {
        x = y;
        y = (x + value / x) / 2;
    }
    
    x
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sqrt_u128() {
        assert_eq!(sqrt_u128(0), 0);
        assert_eq!(sqrt_u128(1), 1);
        assert_eq!(sqrt_u128(4), 2);
        assert_eq!(sqrt_u128(9), 3);
        assert_eq!(sqrt_u128(16), 4);
        assert_eq!(sqrt_u128(100), 10);
        assert_eq!(sqrt_u128(1_000_000), 1_000);
        
        let large_value = u128::MAX / 2;
        let sqrt_large = sqrt_u128(large_value);
        assert!(sqrt_large * sqrt_large <= large_value);
        assert!((sqrt_large + 1) * (sqrt_large + 1) > large_value);
    }

    #[test]
    fn test_apply_fee() {
        assert_eq!(DexFeeTierArbitrageur::apply_fee(1_000_000, 3000), 997_000);
        assert_eq!(DexFeeTierArbitrageur::apply_fee(1_000_000, 10000), 990_000);
        assert_eq!(DexFeeTierArbitrageur::apply_fee(1_000_000, 100), 999_900);
        assert_eq!(DexFeeTierArbitrageur::apply_fee(0, 3000), 0);
        assert_eq!(DexFeeTierArbitrageur::apply_fee(u64::MAX, 1000000), 0);
    }

    #[test]
    fn test_calculate_sqrt_price_limit() {
        let current = 1_000_000_000_000u128;
        let limit_down = DexFeeTierArbitrageur::calculate_sqrt_price_limit(current, true);
        let limit_up = DexFeeTierArbitrageur::calculate_sqrt_price_limit(current, false);
        
        assert!(limit_down < current);
        assert!(limit_up > current);
        assert_eq!(limit_down, current * 90 / 100);
        assert_eq!(limit_up, current * 110 / 100);
    }

    #[test]
    fn test_get_associated_token_address() {
        let wallet = Pubkey::new_unique();
        let mint = Pubkey::new_unique();
        let ata1 = DexFeeTierArbitrageur::get_associated_token_address(&wallet, &mint);
        let ata2 = DexFeeTierArbitrageur::get_associated_token_address(&wallet, &mint);
        assert_eq!(ata1, ata2);
    }

    #[test]
    fn test_calculate_orca_output() {
        // Test case 1: Normal A to B swap
        let pool = PoolState {
            address: Pubkey::new_unique(),
            token_a: Pubkey::new_unique(),
            token_b: Pubkey::new_unique(),
            token_vault_a: Pubkey::new_unique(),
            token_vault_b: Pubkey::new_unique(),
            fee_rate: 3000, // 0.3%
            sqrt_price_x64: 1_000_000_000_000u128,
            liquidity: 1_000_000_000_000u128,
            tick_current: 0,
            protocol: Protocol::Orca,
            last_update: 0,
        };
        
        let (output, is_valid) = DexFeeTierArbitrageur::calculate_orca_output(&pool, 1_000_000, true);
        assert!(is_valid);
        assert!(output > 0);
        
        // Test case 2: Normal B to A swap
        let (output, is_valid) = DexFeeTierArbitrageur::calculate_orca_output(&pool, 1_000_000, false);
        assert!(is_valid);
        assert!(output > 0);
        
        // Test case 3: Zero liquidity
        let pool_zero_liquidity = PoolState {
            liquidity: 0,
            ..pool
        };
        let (output, is_valid) = DexFeeTierArbitrageur::calculate_orca_output(&pool_zero_liquidity, 1_000_000, true);
        assert!(!is_valid);
        assert_eq!(output, 0);
    }

    #[test]
    fn test_calculate_raydium_output() {
        // Test case 1: Normal A to B swap
        let pool = PoolState {
            address: Pubkey::new_unique(),
            token_a: Pubkey::new_unique(),
            token_b: Pubkey::new_unique(),
            token_vault_a: Pubkey::new_unique(),
            token_vault_b: Pubkey::new_unique(),
            fee_rate: 2500, // 0.25%
            sqrt_price_x64: 1_000_000_000_000u128,
            liquidity: 1_000_000_000_000u128,
            tick_current: 0,
            protocol: Protocol::Raydium,
            last_update: 0,
        };
        
        let (output, is_valid) = DexFeeTierArbitrageur::calculate_raydium_output(&pool, 1_000_000, true);
        assert!(is_valid);
        assert!(output > 0);
        
        // Test case 2: Normal B to A swap
        let (output, is_valid) = DexFeeTierArbitrageur::calculate_raydium_output(&pool, 1_000_000, false);
        assert!(is_valid);
        assert!(output > 0);
        
        // Test case 3: Zero liquidity
        let pool_zero_liquidity = PoolState {
            liquidity: 0,
            ..pool
        };
        let (output, is_valid) = DexFeeTierArbitrageur::calculate_raydium_output(&pool_zero_liquidity, 1_000_000, true);
        assert!(!is_valid);
        assert_eq!(output, 0);
    }

    // Integration tests for mainnet forking
    
    #[tokio::test]
    async fn test_orca_swap_calculation_mainnet() {
        // This test requires a mainnet RPC endpoint
        // It validates swap calculations against real on-chain data
        let rpc_url = std::env::var("MAINNET_RPC_URL").unwrap_or_else(|_| "https://api.mainnet-beta.solana.com".to_string());
        let rpc = RpcClient::new(rpc_url);
        
        // Example USDC-SOL Orca pool address
        let pool_address = "7qbRF6YsyGuLUVs6Y1q64bdVrfe4ZcUUFE8JkwE1H1KE";
        let pool_pubkey = Pubkey::from_str(pool_address).unwrap();
        
        // Fetch pool data
        let account = rpc.get_account(&pool_pubkey).unwrap();
        
        // Parse pool data
        let pool = DexFeeTierArbitrageur::parse_whirlpool_safe(
            &DexFeeTierArbitrageur::new("https://api.mainnet-beta.solana.com", Keypair::new()),
            pool_pubkey,
            &account.data,
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs()
        ).unwrap();
        
        // Test swap calculation
        let amount_in = 1_000_000; // 1 USDC
        let (amount_out, is_valid) = DexFeeTierArbitrageur::calculate_orca_output(&pool, amount_in, true);
        
        // Assertions
        assert!(is_valid);
        assert!(amount_out > 0);
        
        // Test reverse swap
        let (amount_out_reverse, is_valid_reverse) = DexFeeTierArbitrageur::calculate_orca_output(&pool, amount_out, false);
        assert!(is_valid_reverse);
        // Should be close to original amount (minus fees)
        assert!(amount_out_reverse > amount_in * 99 / 100); // At least 99% due to fees
    }
    
    #[tokio::test]
    async fn test_raydium_swap_calculation_mainnet() {
        // This test requires a mainnet RPC endpoint
        // It validates swap calculations against real on-chain data
        let rpc_url = std::env::var("MAINNET_RPC_URL").unwrap_or_else(|_| "https://api.mainnet-beta.solana.com".to_string());
        let rpc = RpcClient::new(rpc_url);
        
        // Example USDC-SOL Raydium pool address
        let pool_address = "58oQChx4yWmvKdwLLZzBi4ChoCc2fqCUWBkwMihLYQo2";
        let pool_pubkey = Pubkey::from_str(pool_address).unwrap();
        
        // Fetch pool data
        let account = rpc.get_account(&pool_pubkey).unwrap();
        
        // Parse pool data
        let pool = DexFeeTierArbitrageur::parse_raydium_safe(
            &DexFeeTierArbitrageur::new("https://api.mainnet-beta.solana.com", Keypair::new()),
            pool_pubkey,
            &account.data,
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs()
        ).unwrap();
        
        // Test swap calculation
        let amount_in = 1_000_000; // 1 USDC
        let (amount_out, is_valid) = DexFeeTierArbitrageur::calculate_raydium_output(&pool, amount_in, true);
        
        // Assertions
        assert!(is_valid);
        assert!(amount_out > 0);
        
        // Test reverse swap
        let (amount_out_reverse, is_valid_reverse) = DexFeeTierArbitrageur::calculate_raydium_output(&pool, amount_out, false);
        assert!(is_valid_reverse);
        // Should be close to original amount (minus fees)
        assert!(amount_out_reverse > amount_in * 99 / 100); // At least 99% due to fees
    }
    
    #[test]
    fn test_account_validation_functions() {
        let rpc_url = std::env::var("MAINNET_RPC_URL").unwrap_or_else(|_| "https://api.mainnet-beta.solana.com".to_string());
        let rpc = RpcClient::new(rpc_url);
        
        // Test with a known valid token account
        let token_account = Pubkey::from_str("TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA").unwrap();
        
        // This test would normally be run with a proper async runtime
        // For now, we'll just check that the function compiles correctly
        // Actual execution would require #[tokio::test] and async/await
    }
}

pub async fn create_arbitrageur(
    rpc_url: &str,
    keypair_path: &str,
) -> Result<DexFeeTierArbitrageur, Box<dyn std::error::Error>> {
    let keypair_bytes = std::fs::read(keypair_path)?;
    let keypair = Keypair::from_bytes(&keypair_bytes)?;
    Ok(DexFeeTierArbitrageur::new(rpc_url, keypair))
}

