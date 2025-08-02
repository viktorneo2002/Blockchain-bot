use solana_client::rpc_client::RpcClient;
use solana_sdk::{
    account::Account,
    commitment_config::CommitmentConfig,
    instruction::Instruction,
    pubkey::Pubkey,
    signature::Keypair,
    transaction::Transaction,
    compute_budget::ComputeBudgetInstruction,
    hash::Hash,
};
use solana_program::{
    program_error::ProgramError,
    program_pack::Pack,
};
use spl_token::state::{Account as TokenAccount, Mint};
use std::{
    collections::{HashMap, VecDeque},
    sync::{Arc, RwLock},
    time::{Duration, Instant},
    error::Error,
};
use tokio::sync::Mutex;
use borsh::{BorshDeserialize, BorshSerialize};
use serde::{Deserialize, Serialize};

const MAX_SIMULATION_RETRIES: u8 = 3;
const SIMULATION_TIMEOUT_MS: u64 = 100;
const FAILURE_PATTERN_WINDOW: usize = 1000;
const MIN_LAMPORTS_FOR_RENT: u64 = 890880;
const MAX_COMPUTE_UNITS: u32 = 1_400_000;
const SAFETY_MARGIN_COMPUTE_UNITS: u32 = 50_000;
const MIN_SUCCESS_RATE_THRESHOLD: f64 = 0.95;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RevertPrediction {
    pub will_revert: bool,
    pub confidence: f64,
    pub reason: RevertReason,
    pub estimated_compute_units: u32,
    pub recommended_priority_fee: u64,
    pub simulation_logs: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum RevertReason {
    InsufficientBalance,
    AccountNotFound,
    InvalidAccountOwner,
    ComputeBudgetExceeded,
    SlippageExceeded,
    TokenAccountNotInitialized,
    InsufficientTokenBalance,
    ProgramError(String),
    SimulationFailed(String),
    HighFailurePattern,
    NetworkCongestion,
    StaleBlockhash,
    None,
}

#[derive(Debug, Clone)]
struct FailurePattern {
    program_id: Pubkey,
    error_code: String,
    timestamp: Instant,
    compute_units_used: u32,
}

#[derive(Debug, Clone)]
struct AccountSnapshot {
    pubkey: Pubkey,
    lamports: u64,
    owner: Pubkey,
    data: Vec<u8>,
    executable: bool,
    rent_epoch: u64,
}

pub struct TransactionRevertPredictor {
    rpc_client: Arc<RpcClient>,
    failure_patterns: Arc<RwLock<VecDeque<FailurePattern>>>,
    account_cache: Arc<Mutex<HashMap<Pubkey, (AccountSnapshot, Instant)>>>,
    program_success_rates: Arc<RwLock<HashMap<Pubkey, (u64, u64)>>>,
    recent_blockhashes: Arc<RwLock<VecDeque<(Hash, Instant)>>>,
}

impl TransactionRevertPredictor {
    pub fn new(rpc_endpoint: &str) -> Self {
        let rpc_client = Arc::new(RpcClient::new_with_commitment(
            rpc_endpoint.to_string(),
            CommitmentConfig::confirmed(),
        ));

        Self {
            rpc_client,
            failure_patterns: Arc::new(RwLock::new(VecDeque::with_capacity(FAILURE_PATTERN_WINDOW))),
            account_cache: Arc::new(Mutex::new(HashMap::new())),
            program_success_rates: Arc::new(RwLock::new(HashMap::new())),
            recent_blockhashes: Arc::new(RwLock::new(VecDeque::with_capacity(150))),
        }
    }

    pub async fn predict_revert(
        &self,
        instructions: &[Instruction],
        payer: &Keypair,
        recent_blockhash: Hash,
    ) -> Result<RevertPrediction, Box<dyn Error>> {
        let mut prediction = RevertPrediction {
            will_revert: false,
            confidence: 1.0,
            reason: RevertReason::None,
            estimated_compute_units: 0,
            recommended_priority_fee: 0,
            simulation_logs: Vec::new(),
        };

        // Check blockhash freshness
        if let Some(reason) = self.check_blockhash_validity(&recent_blockhash).await {
            prediction.will_revert = true;
            prediction.reason = reason;
            prediction.confidence = 0.99;
            return Ok(prediction);
        }

        // Validate all accounts involved
        let account_keys = self.extract_account_keys(instructions);
        if let Some(reason) = self.validate_accounts(&account_keys, payer).await? {
            prediction.will_revert = true;
            prediction.reason = reason;
            prediction.confidence = 0.98;
            return Ok(prediction);
        }

        // Check program-specific patterns
        for instruction in instructions {
            if let Some(failure_rate) = self.check_program_failure_rate(&instruction.program_id).await {
                if failure_rate > (1.0 - MIN_SUCCESS_RATE_THRESHOLD) {
                    prediction.will_revert = true;
                    prediction.reason = RevertReason::HighFailurePattern;
                    prediction.confidence = failure_rate;
                    return Ok(prediction);
                }
            }
        }

        // Estimate compute units
        let estimated_compute = self.estimate_compute_units(instructions).await?;
        prediction.estimated_compute_units = estimated_compute;

        if estimated_compute > MAX_COMPUTE_UNITS - SAFETY_MARGIN_COMPUTE_UNITS {
            prediction.will_revert = true;
            prediction.reason = RevertReason::ComputeBudgetExceeded;
            prediction.confidence = 0.95;
            return Ok(prediction);
        }

        // Simulate transaction
        let simulation_result = self.simulate_transaction(
            instructions,
            payer,
            recent_blockhash,
            estimated_compute,
        ).await?;

        if let Some((reason, logs)) = simulation_result {
            prediction.will_revert = true;
            prediction.reason = reason;
            prediction.simulation_logs = logs;
            prediction.confidence = 0.99;
            
            // Record failure pattern
            if let RevertReason::ProgramError(ref error_code) = prediction.reason {
                self.record_failure_pattern(
                    instructions[0].program_id,
                    error_code.clone(),
                    estimated_compute,
                ).await;
            }
        }

        // Calculate recommended priority fee based on network conditions
        prediction.recommended_priority_fee = self.calculate_priority_fee(
            estimated_compute,
            prediction.will_revert,
        ).await?;

        Ok(prediction)
    }

    async fn check_blockhash_validity(&self, blockhash: &Hash) -> Option<RevertReason> {
        let recent_hashes = self.recent_blockhashes.read().map_err(|e| anyhow::anyhow!("Failed to acquire read lock on recent_blockhashes: {}", e)).ok()?;
        let now = Instant::now();
        
        for (hash, timestamp) in recent_hashes.iter() {
            if hash == blockhash {
                let age = now.duration_since(*timestamp);
                if age > Duration::from_secs(60) {
                    return Some(RevertReason::StaleBlockhash);
                }
                return None;
            }
        }
        
        // Blockhash not in cache, fetch from RPC
        drop(recent_hashes);
        
        match self.rpc_client.is_blockhash_valid(blockhash, CommitmentConfig::confirmed()) {
            Ok(valid) => {
                if !valid {
                    Some(RevertReason::StaleBlockhash)
                } else {
                    let mut hashes = self.recent_blockhashes.write().map_err(|e| anyhow::anyhow!("Failed to acquire write lock on recent_blockhashes: {}", e))?;
                    hashes.push_back((*blockhash, now));
                    if hashes.len() > 150 {
                        hashes.pop_front();
                    }
                    None
                }
            }
            Err(_) => Some(RevertReason::NetworkCongestion),
        }
    }

    async fn validate_accounts(
        &self,
        account_keys: &[Pubkey],
        payer: &Keypair,
    ) -> Result<Option<RevertReason>, Box<dyn Error>> {
        let mut cache = self.account_cache.lock().await;
        let now = Instant::now();
        let cache_duration = Duration::from_millis(500);

        for pubkey in account_keys {
            let account_info = if let Some((snapshot, timestamp)) = cache.get(pubkey) {
                if now.duration_since(*timestamp) < cache_duration {
                    snapshot.clone()
                } else {
                    let account = self.rpc_client.get_account(pubkey)?;
                    let snapshot = AccountSnapshot {
                        pubkey: *pubkey,
                        lamports: account.lamports,
                        owner: account.owner,
                        data: account.data.clone(),
                        executable: account.executable,
                        rent_epoch: account.rent_epoch,
                    };
                    cache.insert(*pubkey, (snapshot.clone(), now));
                    snapshot
                }
            } else {
                match self.rpc_client.get_account(pubkey) {
                    Ok(account) => {
                        let snapshot = AccountSnapshot {
                            pubkey: *pubkey,
                            lamports: account.lamports,
                            owner: account.owner,
                            data: account.data.clone(),
                            executable: account.executable,
                            rent_epoch: account.rent_epoch,
                        };
                        cache.insert(*pubkey, (snapshot.clone(), now));
                        snapshot
                    }
                    Err(_) => return Ok(Some(RevertReason::AccountNotFound)),
                }
            };

            // Validate account has minimum rent
            if account_info.lamports < MIN_LAMPORTS_FOR_RENT && !account_info.executable {
                return Ok(Some(RevertReason::InsufficientBalance));
            }

            // Validate token accounts if applicable
            if account_info.owner == spl_token::id() {
                if let Err(_) = self.validate_token_account(&account_info) {
                    return Ok(Some(RevertReason::TokenAccountNotInitialized));
                }
            }
        }

        // Check payer balance
        let payer_account = self.rpc_client.get_account(&payer.pubkey())?;
        if payer_account.lamports < 10_000_000 {
            return Ok(Some(RevertReason::InsufficientBalance));
        }

        Ok(None)
    }

    fn validate_token_account(&self, account: &AccountSnapshot) -> Result<(), Box<dyn Error>> {
        if account.data.len() != TokenAccount::LEN {
            return Err("Invalid token account size".into());
        }

        let token_account = TokenAccount::unpack(&account.data)?;
        
        if token_account.amount == 0 {
            return Err("Zero token balance".into());
        }

        Ok(())
    }

    async fn check_program_failure_rate(&self, program_id: &Pubkey) -> Option<f64> {
        let rates = self.program_success_rates.read().map_err(|e| anyhow::anyhow!("Failed to acquire read lock on program_success_rates: {}", e)).ok()?;
        if let Some((successes, total)) = rates.get(program_id) {
            if *total > 10 {
                let failure_rate = 1.0 - (*successes as f64 / *total as f64);
                return Some(failure_rate);
            }
        }
        None
    }

    async fn estimate_compute_units(&self, instructions: &[Instruction]) -> Result<u32, Box<dyn Error>> {
        let mut total_compute = 0u32;

        for instruction in instructions {
            let base_compute = match instruction.program_id {
                id if id == spl_token::id() => 5_000,
                id if id == spl_associated_token_account::id() => 10_000,
                _ => 20_000,
            };

            let data_compute = (instruction.data.len() as u32 / 32) * 100;
            let account_compute = instruction.accounts.len() as u32 * 1_000;

            total_compute += base_compute + data_compute + account_compute;
        }

        // Add safety margin
        total_compute = (total_compute as f64 * 1.2) as u32;

        Ok(total_compute.min(MAX_COMPUTE_UNITS))
    }

    async fn simulate_transaction(
        &self,
        instructions: &[Instruction],
        payer: &Keypair,
        recent_blockhash: Hash,
        compute_units: u32,
    ) -> Result<Option<(RevertReason, Vec<String>)>, Box<dyn Error>> {
        let mut all_instructions = vec![
            ComputeBudgetInstruction::set_compute_unit_limit(compute_units),
            ComputeBudgetInstruction::set_compute_unit_price(1000),
        ];
        all_instructions.extend_from_slice(instructions);

        let transaction = Transaction::new_signed_with_payer(
            &all_instructions,
            Some(&payer.pubkey()),
            &[payer],
            recent_blockhash,
        );

        for attempt in 0..MAX_SIMULATION_RETRIES {
            match tokio::time::timeout(
                Duration::from_millis(SIMULATION_TIMEOUT_MS),
                self.rpc_client.simulate_transaction(&transaction)
            ).await {
                Ok(Ok(result)) => {
                    if let Some(err) = result.err {
                        let logs = result.logs.clone().unwrap_or_else(|| vec![]);
                        let reason = self.parse_simulation_error(&err, &logs);
                        return Ok(Some((reason, logs)));
                    }
                    return Ok(None);
                }
                Ok(Err(e)) => {
                    if attempt == MAX_SIMULATION_RETRIES - 1 {
                        return Ok(Some((
                            RevertReason::SimulationFailed(e.to_string()),
                            vec![e.to_string()],
                        )));
                    }
                }
                Err(_) => {
                    if attempt == MAX_SIMULATION_RETRIES - 1 {
                        return Ok(Some((
                        return Ok(Some((
                            RevertReason::NetworkCongestion,
                            vec!["Simulation timeout".to_string()],
                        )));
                    }
                }
            }
            tokio::time::sleep(Duration::from_millis(10 * (attempt as u64 + 1))).await;
        }

        Ok(Some((
            RevertReason::SimulationFailed("Max retries exceeded".to_string()),
            vec!["Max simulation retries exceeded".to_string()],
        )))
    }

    fn parse_simulation_error(&self, error: &str, logs: &[String]) -> RevertReason {
        // Parse common Solana program errors
        if error.contains("insufficient funds") || error.contains("Insufficient funds") {
            return RevertReason::InsufficientBalance;
        }
        
        if error.contains("AccountNotFound") || error.contains("account not found") {
            return RevertReason::AccountNotFound;
        }
        
        if error.contains("invalid account owner") || error.contains("IncorrectProgramId") {
            return RevertReason::InvalidAccountOwner;
        }
        
        if error.contains("compute budget exceeded") || error.contains("ComputationalBudgetExceeded") {
            return RevertReason::ComputeBudgetExceeded;
        }

        // Parse token program specific errors
        for log in logs {
            if log.contains("Error: insufficient funds") {
                return RevertReason::InsufficientTokenBalance;
            }
            
            if log.contains("slippage") || log.contains("SlippageExceeded") {
                return RevertReason::SlippageExceeded;
            }
            
            if log.contains("Account not initialized") {
                return RevertReason::TokenAccountNotInitialized;
            }
        }

        // Check for custom program errors in logs
        for log in logs {
            if log.starts_with("Program log: Error:") {
                let error_msg = log.replace("Program log: Error:", "").trim().to_string();
                return RevertReason::ProgramError(error_msg);
            }
            
            if log.contains("failed: custom program error:") {
                if let Some(code) = log.split("0x").nth(1) {
                    if let Some(code) = code.split_whitespace().next() {
                        return RevertReason::ProgramError(format!("0x{}", code));
                    }
                }
            }
        }

        RevertReason::ProgramError(error.to_string())
    }

    async fn record_failure_pattern(
        &self,
        program_id: Pubkey,
        error_code: String,
        compute_units: u32,
    ) {
        let pattern = FailurePattern {
            program_id,
            error_code,
            timestamp: Instant::now(),
            compute_units_used: compute_units,
        };

        let mut patterns = self.failure_patterns.write().map_err(|e| anyhow::anyhow!("Failed to acquire write lock on failure_patterns: {}", e))?;
        patterns.push_back(pattern);
        
        if patterns.len() > FAILURE_PATTERN_WINDOW {
            patterns.pop_front();
        }

        // Update program success rates
        let mut rates = self.program_success_rates.write().map_err(|e| anyhow::anyhow!("Failed to acquire write lock on program_success_rates: {}", e))?;
        let (successes, total) = rates.entry(program_id).or_insert((0, 0));
        *total += 1;
    }

    pub async fn record_success(&self, program_id: Pubkey) -> Result<()> {
        let mut rates = self.program_success_rates.write().map_err(|e| anyhow::anyhow!("Failed to acquire write lock on program_success_rates: {}", e))?;
        let (successes, total) = rates.entry(program_id).or_insert((0, 0));
        *successes += 1;
        *total += 1;
        Ok(())
    }

    async fn calculate_priority_fee(
        &self,
        compute_units: u32,
        will_revert: bool,
    ) -> Result<u64, Box<dyn Error>> {
        // Get recent priority fees
        let recent_fees = match self.rpc_client.get_recent_prioritization_fees(&[]) {
            Ok(fees) => fees,
            Err(_) => return Ok(1000), // Default fallback
        };

        if recent_fees.is_empty() {
            return Ok(1000);
        }

        // Calculate percentile-based fee
        let mut fees: Vec<u64> = recent_fees
            .iter()
            .map(|f| f.prioritization_fee)
            .filter(|&f| f > 0)
            .collect();
        
        if fees.is_empty() {
            return Ok(1000);
        }

        fees.sort_unstable();
        
        // Use 75th percentile for normal, 90th for high-risk transactions
        let percentile_index = if will_revert {
            (fees.len() as f64 * 0.5) as usize  // Lower fee for likely reverts
        } else {
            (fees.len() as f64 * 0.9) as usize  // Higher fee for likely success
        };

        let base_fee = fees.get(percentile_index).copied().unwrap_or(1000);
        
        // Adjust based on compute units
        let compute_multiplier = (compute_units as f64 / 200_000.0).max(1.0);
        let adjusted_fee = (base_fee as f64 * compute_multiplier) as u64;

        Ok(adjusted_fee.min(1_000_000)) // Cap at 1M microlamports
    }

    fn extract_account_keys(&self, instructions: &[Instruction]) -> Vec<Pubkey> {
        let mut keys = Vec::new();
        let mut seen = std::collections::HashSet::new();

        for instruction in instructions {
            if seen.insert(instruction.program_id) {
                keys.push(instruction.program_id);
            }
            
            for account in &instruction.accounts {
                if seen.insert(account.pubkey) {
                    keys.push(account.pubkey);
                }
            }
        }

        keys
    }

    pub async fn analyze_failure_patterns(&self) -> HashMap<String, f64> {
        let patterns = self.failure_patterns.read().map_err(|e| anyhow::anyhow!("Failed to acquire read lock on failure_patterns: {}", e))?;
        let mut error_counts: HashMap<String, u32> = HashMap::new();
        let now = Instant::now();
        
        for pattern in patterns.iter() {
            let age = now.duration_since(pattern.timestamp);
            if age < Duration::from_secs(300) {  // Last 5 minutes
                *error_counts.entry(pattern.error_code.clone()).or_insert(0) += 1;
            }
        }

        let total = error_counts.values().sum::<u32>() as f64;
        
        Ok(error_counts
            .into_iter()
            .map(|(error, count)| (error, count as f64 / total))
            .collect())
    }

    pub async fn get_program_health_metrics(&self) -> HashMap<Pubkey, f64> {
        let rates = self.program_success_rates.read().map_err(|e| anyhow::anyhow!("Failed to acquire read lock on program_success_rates: {}", e))?;
        
        Ok(rates
            .iter()
            .filter(|(_, (_, total))| *total > 10)
            .map(|(program, (successes, total))| {
                (*program, *successes as f64 / *total as f64)
            })
            .collect())
    }

    pub async fn should_execute_transaction(
        &self,
        prediction: &RevertPrediction,
        risk_tolerance: f64,
    ) -> bool {
        if prediction.will_revert {
            return false;
        }

        // Additional safety checks based on risk tolerance
        match prediction.reason {
            RevertReason::None => prediction.confidence > (1.0 - risk_tolerance),
            RevertReason::NetworkCongestion => risk_tolerance > 0.5,
            RevertReason::HighFailurePattern => risk_tolerance > 0.7,
            _ => false,
        }
    }

    pub async fn optimize_transaction_for_success(
        &self,
        instructions: &mut Vec<Instruction>,
        prediction: &RevertPrediction,
    ) -> Result<(), Box<dyn Error>> {
        // Add compute budget instructions if not present
        let has_compute_budget = instructions.iter().any(|ix| {
            ix.program_id == solana_sdk::compute_budget::id()
        });

        if !has_compute_budget {
            let compute_limit = prediction.estimated_compute_units + SAFETY_MARGIN_COMPUTE_UNITS;
            let priority_fee = prediction.recommended_priority_fee;

            instructions.insert(0, ComputeBudgetInstruction::set_compute_unit_limit(compute_limit));
            instructions.insert(1, ComputeBudgetInstruction::set_compute_unit_price(priority_fee));
        }

        Ok(())
    }

    pub async fn cleanup_old_data(&self) {
        // Clean old failure patterns
        let mut patterns = self.failure_patterns.write().map_err(|e| anyhow::anyhow!("Failed to acquire write lock on failure_patterns: {}", e))?;
        let now = Instant::now();
        
        patterns.retain(|pattern| {
            now.duration_since(pattern.timestamp) < Duration::from_secs(3600)
        });

        // Clean old account cache
        let mut cache = self.account_cache.lock().await;
        let cache_threshold = Duration::from_secs(60);
        
        cache.retain(|_, (_, timestamp)| {
            now.duration_since(*timestamp) < cache_threshold
        });

        // Clean old blockhashes
        let mut blockhashes = self.recent_blockhashes.write().map_err(|e| anyhow::anyhow!("Failed to acquire write lock on recent_blockhashes: {}", e))?;
        
        blockhashes.retain(|(_, timestamp)| {
            now.duration_since(*timestamp) < Duration::from_secs(150)
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use solana_sdk::system_instruction;

    #[tokio::test]
    async fn test_revert_prediction() {
        let predictor = TransactionRevertPredictor::new("https://api.mainnet-beta.solana.com");
        let payer = Keypair::new();
        let recipient = Pubkey::new_unique();
        
        let instruction = system_instruction::transfer(
            &payer.pubkey(),
            &recipient,
            1_000_000,
        );

        let recent_blockhash = Hash::default();
        
        let prediction = predictor.predict_revert(
            &[instruction],
            &payer,
            recent_blockhash,
        ).await.map_err(|e| anyhow::anyhow!("Failed to clean old data: {}", e))?;

        assert!(prediction.will_revert);
        assert_eq!(prediction.reason, RevertReason::InsufficientBalance);
    }
}
