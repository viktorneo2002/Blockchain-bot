use solana_client::rpc_client::RpcClient;
use solana_sdk::{
    account::Account,
    commitment_config::CommitmentConfig,
    instruction::Instruction,
    pubkey::Pubkey,
    signature::Signature,
    transaction::Transaction,
};
use spl_token::state::{Account as TokenAccount, Mint};
use std::{
    collections::{HashMap, HashSet, VecDeque},
    str::FromStr,
    sync::{Arc, RwLock},
    time::{Duration, Instant},
};
use borsh::{BorshDeserialize, BorshSerialize};
use sha2::{Sha256, Digest};
use pyth_sdk_solana::state::SolanaPriceAccount;

const PYTH_PROGRAM_ID: &str = "FsJ3A3u2vn5cTVofAjvy6y5kwABJAqYWpe4975bi2epH";
const SERUM_PROGRAM_V3: &str = "9xQeWvG816bUx9EPjHmaT23yvVM2ZWbrrpZb9PusVFin";
const RAYDIUM_AMM_V4: &str = "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8";
const ORCA_WHIRLPOOL: &str = "whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc";
const TOKEN_PROGRAM: &str = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA";
const ASSOCIATED_TOKEN: &str = "ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL";
const JUPITER_V6: &str = "JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4";
const SYSTEM_PROGRAM: &str = "11111111111111111111111111111111";

const MAX_SLIPPAGE_BPS: u16 = 300;
const MIN_LIQUIDITY_USD: f64 = 50000.0;
const MAX_OWNERSHIP_PERCENT: f64 = 15.0;
const MIN_HOLDERS_COUNT: u64 = 500;
const MAX_TRANSACTION_HISTORY: usize = 10000;
const ANOMALY_SCORE_THRESHOLD: f64 = 0.8;
const CACHE_TTL_SECONDS: u64 = 300;

#[derive(Debug, Clone, BorshSerialize, BorshDeserialize)]
pub struct SecurityMetrics {
    pub risk_score: f64,
    pub is_verified: bool,
    pub liquidity_locked: bool,
    pub mint_disabled: bool,
    pub freeze_disabled: bool,
    pub ownership_renounced: bool,
    pub creation_timestamp: i64,
    pub holder_count: u64,
    pub transaction_count: u64,
    pub largest_holder_percent: f64,
    pub liquidity_usd: f64,
    pub last_update: Instant,
}

#[derive(Debug, Clone)]
pub struct TransactionPattern {
    pub timestamp: Instant,
    pub program_id: Pubkey,
    pub instruction_count: usize,
    pub signer_count: usize,
    pub compute_units: u64,
    pub accounts_touched: usize,
}

#[derive(Debug, Clone)]
struct CachedMetrics {
    metrics: SecurityMetrics,
    timestamp: Instant,
}

pub struct AntiPhishingShield {
    rpc_client: Arc<RpcClient>,
    trusted_programs: HashSet<Pubkey>,
    blacklisted_addresses: Arc<RwLock<HashSet<Pubkey>>>,
    token_security_cache: Arc<RwLock<HashMap<Pubkey, CachedMetrics>>>,
    transaction_history: Arc<RwLock<VecDeque<TransactionPattern>>>,
    anomaly_detector: AnomalyDetector,
    price_oracle: PriceOracle,
}

struct PriceOracle {
    rpc_client: Arc<RpcClient>,
    pyth_program: Pubkey,
    price_feeds: HashMap<String, Pubkey>,
}

struct AnomalyDetector {
    baseline_metrics: RwLock<BaselineMetrics>,
    detection_window: Duration,
}

#[derive(Default)]
struct BaselineMetrics {
    avg_instruction_count: f64,
    avg_signer_count: f64,
    avg_compute_units: f64,
    avg_accounts_touched: f64,
    std_dev_instruction_count: f64,
    std_dev_signer_count: f64,
    std_dev_compute_units: f64,
    std_dev_accounts_touched: f64,
}

impl AntiPhishingShield {
    pub fn new(rpc_url: &str) -> Self {
        let rpc_client = Arc::new(RpcClient::new_with_commitment(
            rpc_url.to_string(),
            CommitmentConfig::confirmed(),
        ));

        let trusted_programs = [
            SYSTEM_PROGRAM,
            TOKEN_PROGRAM,
            ASSOCIATED_TOKEN,
            SERUM_PROGRAM_V3,
            RAYDIUM_AMM_V4,
            ORCA_WHIRLPOOL,
            JUPITER_V6,
            PYTH_PROGRAM_ID,
        ].iter()
        .filter_map(|&p| Pubkey::from_str(p).ok())
        .collect();

        Self {
            rpc_client: rpc_client.clone(),
            trusted_programs,
            blacklisted_addresses: Arc::new(RwLock::new(HashSet::new())),
            token_security_cache: Arc::new(RwLock::new(HashMap::new())),
            transaction_history: Arc::new(RwLock::new(VecDeque::with_capacity(MAX_TRANSACTION_HISTORY))),
            anomaly_detector: AnomalyDetector::new(),
            price_oracle: PriceOracle::new(rpc_client),
        }
    }

    pub async fn validate_transaction(&self, transaction: &Transaction) -> Result<bool, Box<dyn std::error::Error>> {
        let instructions = &transaction.message.instructions;
        let account_keys = &transaction.message.account_keys;
        let mut risk_factors = Vec::new();

        for (idx, instruction) in instructions.iter().enumerate() {
            let program_id = account_keys[instruction.program_id_index as usize];
            
            if !self.is_trusted_program(&program_id) {
                let is_valid = self.validate_program(&program_id).await?;
                if !is_valid {
                    risk_factors.push(format!("Untrusted program at instruction {}", idx));
                }
            }

            if self.is_blacklisted(&program_id).await {
                return Ok(false);
            }

            let security_check = self.check_instruction_security(instruction, account_keys).await?;
            if !security_check {
                risk_factors.push(format!("Suspicious instruction pattern at {}", idx));
            }
        }

        let pattern = TransactionPattern {
            timestamp: Instant::now(),
            program_id: account_keys[0],
            instruction_count: instructions.len(),
            signer_count: transaction.signatures.len(),
            compute_units: self.estimate_compute_units(instructions),
            accounts_touched: self.count_unique_accounts(transaction),
        };

        if self.anomaly_detector.is_anomalous(&pattern) {
            risk_factors.push("Anomalous transaction pattern detected".to_string());
        }

        self.record_transaction_pattern(pattern).await;

        Ok(risk_factors.is_empty())
    }

    pub async fn validate_token(&self, mint_pubkey: &Pubkey) -> Result<SecurityMetrics, Box<dyn std::error::Error>> {
        if let Some(cached) = self.get_cached_metrics(mint_pubkey).await {
            if cached.timestamp.elapsed() < Duration::from_secs(CACHE_TTL_SECONDS) {
                return Ok(cached.metrics);
            }
        }

        let mint_account = self.rpc_client.get_account(mint_pubkey)?;
        let mint_data = Mint::unpack(&mint_account.data)?;
        
        let mut metrics = SecurityMetrics {
            risk_score: 0.0,
            is_verified: false,
            liquidity_locked: false,
            mint_disabled: mint_data.mint_authority.is_none(),
            freeze_disabled: mint_data.freeze_authority.is_none(),
            ownership_renounced: mint_data.mint_authority.is_none() && mint_data.freeze_authority.is_none(),
            creation_timestamp: self.get_account_creation_time(mint_pubkey).await?,
            holder_count: 0,
            transaction_count: 0,
            largest_holder_percent: 0.0,
            liquidity_usd: 0.0,
            last_update: Instant::now(),
        };

        if mint_data.mint_authority.is_some() {
            metrics.risk_score += 0.15;
        }
        if mint_data.freeze_authority.is_some() {
            metrics.risk_score += 0.1;
        }

        let token_accounts = self.get_token_accounts(mint_pubkey).await?;
        metrics.holder_count = token_accounts.len() as u64;

        let total_supply = mint_data.supply;
        if total_supply > 0 {
            let distribution = self.analyze_token_distribution(&token_accounts, total_supply);
            metrics.largest_holder_percent = distribution.largest_holder_percent;
            
            if metrics.largest_holder_percent > MAX_OWNERSHIP_PERCENT {
                metrics.risk_score += 0.25;
            }
            
            if distribution.top_10_percent > 50.0 {
                metrics.risk_score += 0.15;
            }
        }

        if metrics.holder_count < MIN_HOLDERS_COUNT {
            metrics.risk_score += 0.2;
        }

        let liquidity_data = self.calculate_total_liquidity(mint_pubkey).await?;
        metrics.liquidity_usd = liquidity_data;
        
        if metrics.liquidity_usd < MIN_LIQUIDITY_USD {
            metrics.risk_score += 0.3;
        }

        let age_days = (Instant::now().elapsed().as_secs() - metrics.creation_timestamp as u64) / 86400;
        if age_days < 7 {
            metrics.risk_score += 0.1;
        }

        metrics.risk_score = metrics.risk_score.min(1.0);
        
        self.cache_metrics(mint_pubkey, metrics.clone()).await;
        Ok(metrics)
    }

    pub async fn check_honeypot(&self, mint_pubkey: &Pubkey) -> Result<bool, Box<dyn std::error::Error>> {
        let security_metrics = self.validate_token(mint_pubkey).await?;
        
        if security_metrics.risk_score > ANOMALY_SCORE_THRESHOLD {
            return Ok(true);
        }

        let pools = self.find_liquidity_pools(mint_pubkey).await?;
        if pools.is_empty() {
            return Ok(true);
        }

        for pool in &pools {
            if self.is_pool_locked(pool).await? {
                return Ok(true);
            }
        }

        let sell_tax = self.estimate_sell_tax(mint_pubkey).await?;
        if sell_tax > 10.0 {
            return Ok(true);
        }

        Ok(false)
    }

    pub async fn detect_rug_pull(&self, transaction: &Transaction) -> Result<bool, Box<dyn std::error::Error>> {
        for instruction in &transaction.message.instructions {
            let program_id = transaction.message.account_keys[instruction.program_id_index as usize];
            
            if program_id == spl_token::id() {
                let instruction_type = if instruction.data.len() > 0 { instruction.data[0] } else { continue };
                
                match instruction_type {
                    7 => {
                        if let Some(amount) = self.extract_amount_from_instruction(&instruction.data) {
                            let mint = transaction.message.account_keys[instruction.accounts[1] as usize];
                            let mint_supply = self.get_token_supply(&mint).await?;
                            if amount as f64 / mint_supply as f64 > 0.05 {
                                return Ok(true);
                            }
                        }
                    },
                    4 | 8 => {
                        return Ok(true);
                    },
                    _ => {}
                }
            }
        }
        
        Ok(false)
    }

    async fn get_account_creation_time(&self, pubkey: &Pubkey) -> Result<i64, Box<dyn std::error::Error>> {
        let slot = self.rpc_client.get_slot()?;
        let timestamp = self.rpc_client.get_block_time(slot)?;
        Ok(timestamp)
    }

    async fn analyze_token_distribution(&self, accounts: &[Account], total_supply: u64) -> TokenDistribution {
        let mut balances: Vec<u64> = accounts.iter()
            .filter_map(|acc| {
                TokenAccount::unpack(&acc.data).ok().map(|ta| ta.amount)
            })
            .collect();
        
        balances.sort_by(|a, b| b.cmp(a));
        
        let largest_holder_percent = if total_supply > 0 && !balances.is_empty() {
            (balances[0] as f64 / total_supply as f64) * 100.0
        } else {
            0.0
        };
        
        let top_10_sum: u64 = balances.iter().take(10).sum();
        let top_10_percent = if total_supply > 0 {
            (top_10_sum as f64 / total_supply as f64) * 100.0
        } else {
            0.0
        };
        
        TokenDistribution {
            largest_holder_percent,
            top_10_percent,
        }
    }

    async fn calculate_total_liquidity(&self, mint: &Pubkey) -> Result<f64, Box<dyn std::error::Error>> {
        let pools = self.find_liquidity_pools(mint).await?;
        let mut total_liquidity = 0.0;
        
        for pool in pools {
                        if let Ok(liquidity) = self.calculate_pool_liquidity(&pool).await {
                total_liquidity += liquidity;
            }
        }
        
        Ok(total_liquidity)
    }

    async fn calculate_pool_liquidity(&self, pool: &Pubkey) -> Result<f64, Box<dyn std::error::Error>> {
        let pool_account = self.rpc_client.get_account(pool)?;
        let pool_data = &pool_account.data;
        
        if pool_account.owner == Pubkey::from_str(RAYDIUM_AMM_V4)? {
            return self.calculate_raydium_liquidity(pool_data).await;
        } else if pool_account.owner == Pubkey::from_str(ORCA_WHIRLPOOL)? {
            return self.calculate_orca_liquidity(pool_data).await;
        } else if pool_account.owner == Pubkey::from_str(SERUM_PROGRAM_V3)? {
            return self.calculate_serum_liquidity(pool).await;
        }
        
        Ok(0.0)
    }

    async fn calculate_raydium_liquidity(&self, pool_data: &[u8]) -> Result<f64, Box<dyn std::error::Error>> {
        if pool_data.len() < 752 {
            return Ok(0.0);
        }
        
        let pc_vault = Pubkey::new_from_array(pool_data[97..129].try_into()?);
        let coin_vault = Pubkey::new_from_array(pool_data[65..97].try_into()?);
        
        let pc_account = self.rpc_client.get_account(&pc_vault)?;
        let coin_account = self.rpc_client.get_account(&coin_vault)?;
        
        let pc_amount = if pc_account.data.len() >= 72 {
            u64::from_le_bytes(pc_account.data[64..72].try_into()?)
        } else {
            0
        };
        
        let coin_amount = if coin_account.data.len() >= 72 {
            u64::from_le_bytes(coin_account.data[64..72].try_into()?)
        } else {
            0
        };
        
        let pc_mint = Pubkey::new_from_array(pc_account.data[0..32].try_into()?);
        let coin_mint = Pubkey::new_from_array(coin_account.data[0..32].try_into()?);
        
        let pc_price = self.price_oracle.get_price(&pc_mint).await?;
        let coin_price = self.price_oracle.get_price(&coin_mint).await?;
        
        let pc_decimals = self.get_token_decimals(&pc_mint).await?;
        let coin_decimals = self.get_token_decimals(&coin_mint).await?;
        
        let pc_value = (pc_amount as f64 / 10f64.powi(pc_decimals as i32)) * pc_price;
        let coin_value = (coin_amount as f64 / 10f64.powi(coin_decimals as i32)) * coin_price;
        
        Ok(pc_value + coin_value)
    }

    async fn calculate_orca_liquidity(&self, pool_data: &[u8]) -> Result<f64, Box<dyn std::error::Error>> {
        if pool_data.len() < 324 {
            return Ok(0.0);
        }
        
        let token_a_vault = Pubkey::new_from_array(pool_data[65..97].try_into()?);
        let token_b_vault = Pubkey::new_from_array(pool_data[97..129].try_into()?);
        
        let vault_a_account = self.rpc_client.get_account(&token_a_vault)?;
        let vault_b_account = self.rpc_client.get_account(&token_b_vault)?;
        
        let amount_a = if vault_a_account.data.len() >= 72 {
            u64::from_le_bytes(vault_a_account.data[64..72].try_into()?)
        } else {
            0
        };
        
        let amount_b = if vault_b_account.data.len() >= 72 {
            u64::from_le_bytes(vault_b_account.data[64..72].try_into()?)
        } else {
            0
        };
        
        let mint_a = Pubkey::new_from_array(vault_a_account.data[0..32].try_into()?);
        let mint_b = Pubkey::new_from_array(vault_b_account.data[0..32].try_into()?);
        
        let price_a = self.price_oracle.get_price(&mint_a).await?;
        let price_b = self.price_oracle.get_price(&mint_b).await?;
        
        let decimals_a = self.get_token_decimals(&mint_a).await?;
        let decimals_b = self.get_token_decimals(&mint_b).await?;
        
        let value_a = (amount_a as f64 / 10f64.powi(decimals_a as i32)) * price_a;
        let value_b = (amount_b as f64 / 10f64.powi(decimals_b as i32)) * price_b;
        
        Ok(value_a + value_b)
    }

    async fn calculate_serum_liquidity(&self, market: &Pubkey) -> Result<f64, Box<dyn std::error::Error>> {
        let market_account = self.rpc_client.get_account(market)?;
        if market_account.data.len() < 388 {
            return Ok(0.0);
        }
        
        let base_vault = Pubkey::new_from_array(market_account.data[53..85].try_into()?);
        let quote_vault = Pubkey::new_from_array(market_account.data[85..117].try_into()?);
        
        let base_account = self.rpc_client.get_account(&base_vault)?;
        let quote_account = self.rpc_client.get_account(&quote_vault)?;
        
        let base_amount = u64::from_le_bytes(base_account.lamports.to_le_bytes());
        let quote_amount = u64::from_le_bytes(quote_account.lamports.to_le_bytes());
        
        let base_mint = Pubkey::new_from_array(market_account.data[117..149].try_into()?);
        let quote_mint = Pubkey::new_from_array(market_account.data[149..181].try_into()?);
        
        let base_price = self.price_oracle.get_price(&base_mint).await?;
        let quote_price = self.price_oracle.get_price(&quote_mint).await?;
        
        let base_decimals = self.get_token_decimals(&base_mint).await?;
        let quote_decimals = self.get_token_decimals(&quote_mint).await?;
        
        let base_value = (base_amount as f64 / 10f64.powi(base_decimals as i32)) * base_price;
        let quote_value = (quote_amount as f64 / 10f64.powi(quote_decimals as i32)) * quote_price;
        
        Ok(base_value + quote_value)
    }

    async fn get_token_decimals(&self, mint: &Pubkey) -> Result<u8, Box<dyn std::error::Error>> {
        if *mint == spl_token::native_mint::id() {
            return Ok(9);
        }
        
        let mint_account = self.rpc_client.get_account(mint)?;
        let mint_data = Mint::unpack(&mint_account.data)?;
        Ok(mint_data.decimals)
    }

    async fn is_pool_locked(&self, pool: &Pubkey) -> Result<bool, Box<dyn std::error::Error>> {
        let pool_account = self.rpc_client.get_account(pool)?;
        
        if pool_account.owner == Pubkey::from_str(RAYDIUM_AMM_V4)? {
            let status = pool_account.data[707];
            return Ok(status != 1);
        }
        
        Ok(false)
    }

    async fn estimate_sell_tax(&self, mint: &Pubkey) -> Result<f64, Box<dyn std::error::Error>> {
        let recent_txs = self.get_recent_token_transactions(mint, 20).await?;
        let mut sell_taxes = Vec::new();
        
        for tx in recent_txs {
            if let Ok(tax) = self.calculate_transaction_tax(&tx).await {
                if tax > 0.0 {
                    sell_taxes.push(tax);
                }
            }
        }
        
        if sell_taxes.is_empty() {
            return Ok(0.0);
        }
        
        let avg_tax = sell_taxes.iter().sum::<f64>() / sell_taxes.len() as f64;
        Ok(avg_tax)
    }

    async fn get_recent_token_transactions(&self, mint: &Pubkey, limit: usize) -> Result<Vec<Signature>, Box<dyn std::error::Error>> {
        let signatures = self.rpc_client.get_signatures_for_address_with_config(
            mint,
            solana_client::rpc_config::RpcSignaturesForAddressConfig {
                limit: Some(limit),
                ..Default::default()
            },
        )?;
        
        Ok(signatures.into_iter().map(|s| s.signature).collect())
    }

    async fn calculate_transaction_tax(&self, signature: &Signature) -> Result<f64, Box<dyn std::error::Error>> {
        let tx = self.rpc_client.get_transaction(&signature, Default::default())?;
        
        if let Some(transaction) = tx.transaction {
            let pre_balances = tx.meta.as_ref().map(|m| &m.pre_token_balances).unwrap_or(&vec![]);
            let post_balances = tx.meta.as_ref().map(|m| &m.post_token_balances).unwrap_or(&vec![]);
            
            for i in 0..pre_balances.len().min(post_balances.len()) {
                if let (Some(pre), Some(post)) = (pre_balances.get(i), post_balances.get(i)) {
                    if pre.owner == post.owner {
                        let pre_amount = pre.ui_token_amount.amount.parse::<u64>().unwrap_or(0);
                        let post_amount = post.ui_token_amount.amount.parse::<u64>().unwrap_or(0);
                        
                        if pre_amount > post_amount {
                            let expected_diff = pre_amount - post_amount;
                            let actual_diff = pre_amount.saturating_sub(post_amount);
                            
                            if actual_diff < expected_diff {
                                let tax_rate = (expected_diff - actual_diff) as f64 / expected_diff as f64 * 100.0;
                                return Ok(tax_rate);
                            }
                        }
                    }
                }
            }
        }
        
        Ok(0.0)
    }

    async fn find_liquidity_pools(&self, mint: &Pubkey) -> Result<Vec<Pubkey>, Box<dyn std::error::Error>> {
        let mut pools = Vec::new();
        
        for program_str in &[RAYDIUM_AMM_V4, ORCA_WHIRLPOOL] {
            let program_id = Pubkey::from_str(program_str)?;
            let filters = vec![
                solana_client::rpc_filter::RpcFilterType::Memcmp(
                    solana_client::rpc_filter::Memcmp::new_base58_encoded(
                        if program_id == Pubkey::from_str(RAYDIUM_AMM_V4)? { 192 } else { 8 },
                        &mint.to_bytes(),
                    ),
                ),
            ];
            
            if let Ok(accounts) = self.rpc_client.get_program_accounts_with_config(
                &program_id,
                solana_client::rpc_config::RpcProgramAccountsConfig {
                    filters: Some(filters),
                    ..Default::default()
                },
            ) {
                pools.extend(accounts.into_iter().map(|(pubkey, _)| pubkey));
            }
        }
        
        let serum_markets = self.find_serum_markets(mint).await?;
        pools.extend(serum_markets);
        
        Ok(pools)
    }

    async fn find_serum_markets(&self, mint: &Pubkey) -> Result<Vec<Pubkey>, Box<dyn std::error::Error>> {
        let serum_program = Pubkey::from_str(SERUM_PROGRAM_V3)?;
        let mut markets = Vec::new();
        
        let filters = vec![
            solana_client::rpc_filter::RpcFilterType::DataSize(388),
            solana_client::rpc_filter::RpcFilterType::Memcmp(
                solana_client::rpc_filter::Memcmp::new_base58_encoded(117, &mint.to_bytes()),
            ),
        ];
        
        if let Ok(accounts) = self.rpc_client.get_program_accounts_with_config(
            &serum_program,
            solana_client::rpc_config::RpcProgramAccountsConfig {
                filters: Some(filters.clone()),
                ..Default::default()
            },
        ) {
            markets.extend(accounts.into_iter().map(|(pubkey, _)| pubkey));
        }
        
        let quote_filters = vec![
            solana_client::rpc_filter::RpcFilterType::DataSize(388),
            solana_client::rpc_filter::RpcFilterType::Memcmp(
                solana_client::rpc_filter::Memcmp::new_base58_encoded(149, &mint.to_bytes()),
            ),
        ];
        
        if let Ok(accounts) = self.rpc_client.get_program_accounts_with_config(
            &serum_program,
            solana_client::rpc_config::RpcProgramAccountsConfig {
                filters: Some(quote_filters),
                ..Default::default()
            },
        ) {
            markets.extend(accounts.into_iter().map(|(pubkey, _)| pubkey));
        }
        
        Ok(markets)
    }

    async fn get_token_supply(&self, mint: &Pubkey) -> Result<u64, Box<dyn std::error::Error>> {
                let mint_account = self.rpc_client.get_account(mint)?;
        let mint_data = Mint::unpack(&mint_account.data)?;
        Ok(mint_data.supply)
    }

    fn extract_amount_from_instruction(&self, data: &[u8]) -> Option<u64> {
        if data.len() >= 9 {
            Some(u64::from_le_bytes(data[1..9].try_into().ok()?))
        } else {
            None
        }
    }

    async fn check_instruction_security(&self, instruction: &Instruction, account_keys: &[Pubkey]) -> Result<bool, Box<dyn std::error::Error>> {
        let program_id = account_keys[instruction.program_id_index as usize];
        
        if instruction.data.is_empty() {
            return Ok(false);
        }

        if program_id == spl_token::id() {
            let instruction_type = instruction.data[0];
            match instruction_type {
                0 | 1 => return Ok(true),
                3 => {
                    if instruction.accounts.len() >= 3 {
                        let mint_index = instruction.accounts[0] as usize;
                        if mint_index < account_keys.len() {
                            let mint = &account_keys[mint_index];
                            let current_supply = self.get_token_supply(mint).await.unwrap_or(0);
                            
                            if let Some(amount) = self.extract_amount_from_instruction(&instruction.data) {
                                if amount > current_supply / 100 {
                                    return Ok(false);
                                }
                            }
                        }
                    }
                },
                5 | 6 => return Ok(false),
                _ => {}
            }
        }

        if self.is_malicious_instruction_pattern(&instruction.data) {
            return Ok(false);
        }

        Ok(true)
    }

    fn is_malicious_instruction_pattern(&self, data: &[u8]) -> bool {
        if data.len() < 8 {
            return false;
        }

        const MALICIOUS_PATTERNS: &[[u8; 4]] = &[
            [0xff, 0xff, 0xff, 0xff],
            [0xde, 0xad, 0xbe, 0xef],
            [0x00, 0x00, 0x00, 0x00],
        ];

        for pattern in MALICIOUS_PATTERNS {
            if data.starts_with(pattern) {
                return true;
            }
        }

        if data.len() > 1024 {
            return true;
        }

        false
    }

    async fn is_blacklisted(&self, address: &Pubkey) -> bool {
        self.blacklisted_addresses.read().unwrap().contains(address)
    }

    fn is_trusted_program(&self, program_id: &Pubkey) -> bool {
        self.trusted_programs.contains(program_id)
    }

    async fn validate_program(&self, program_id: &Pubkey) -> Result<bool, Box<dyn std::error::Error>> {
        if self.is_blacklisted(program_id).await {
            return Ok(false);
        }

        if self.is_trusted_program(program_id) {
            return Ok(true);
        }

        let program_account = match self.rpc_client.get_account(program_id) {
            Ok(account) => account,
            Err(_) => return Ok(false),
        };

        if !program_account.executable {
            return Ok(false);
        }

        if program_account.owner != solana_sdk::bpf_loader::id() && 
           program_account.owner != solana_sdk::bpf_loader_deprecated::id() &&
           program_account.owner != solana_sdk::bpf_loader_upgradeable::id() {
            return Ok(false);
        }

        Ok(true)
    }

    fn estimate_compute_units(&self, instructions: &[Instruction]) -> u64 {
        let mut total_cu = 200u64;
        
        for instruction in instructions {
            let program_cu = match instruction.data.len() {
                0..=32 => 500,
                33..=64 => 750,
                65..=128 => 1200,
                129..=256 => 1800,
                257..=512 => 2500,
                _ => 3500,
            };
            
            let accounts_cu = instruction.accounts.len() as u64 * 200;
            let data_cu = (instruction.data.len() as u64 / 32) * 100;
            
            total_cu += program_cu + accounts_cu + data_cu;
        }
        
        total_cu.min(1_400_000)
    }

    fn count_unique_accounts(&self, transaction: &Transaction) -> usize {
        let mut unique_accounts = HashSet::new();
        
        for instruction in &transaction.message.instructions {
            for &account_index in &instruction.accounts {
                if (account_index as usize) < transaction.message.account_keys.len() {
                    unique_accounts.insert(transaction.message.account_keys[account_index as usize]);
                }
            }
        }
        
        unique_accounts.len()
    }

    async fn record_transaction_pattern(&self, pattern: TransactionPattern) {
        let mut history = self.transaction_history.write().unwrap();
        if history.len() >= MAX_TRANSACTION_HISTORY {
            history.pop_front();
        }
        history.push_back(pattern);
        
        if history.len() >= 100 {
            self.anomaly_detector.update_baseline(&history);
        }
    }

    async fn get_cached_metrics(&self, mint: &Pubkey) -> Option<CachedMetrics> {
        self.token_security_cache.read().unwrap().get(mint).cloned()
    }

    async fn cache_metrics(&self, mint: &Pubkey, metrics: SecurityMetrics) {
        let cached = CachedMetrics {
            metrics,
            timestamp: Instant::now(),
        };
        self.token_security_cache.write().unwrap().insert(*mint, cached);
    }

    async fn get_token_accounts(&self, mint: &Pubkey) -> Result<Vec<Account>, Box<dyn std::error::Error>> {
        let filters = vec![
            solana_client::rpc_filter::RpcFilterType::Memcmp(
                solana_client::rpc_filter::Memcmp::new_base58_encoded(0, &mint.to_bytes()),
            ),
            solana_client::rpc_filter::RpcFilterType::DataSize(165),
        ];

        let accounts = self.rpc_client.get_program_accounts_with_config(
            &spl_token::id(),
            solana_client::rpc_config::RpcProgramAccountsConfig {
                filters: Some(filters),
                account_config: solana_client::rpc_config::RpcAccountInfoConfig {
                    encoding: Some(solana_client::rpc_config::UiAccountEncoding::Base64),
                    commitment: Some(CommitmentConfig::confirmed()),
                    ..Default::default()
                },
                ..Default::default()
            },
        )?;

        Ok(accounts.into_iter().map(|(_, account)| account).collect())
    }

    pub fn add_to_blacklist(&self, address: Pubkey) {
        self.blacklisted_addresses.write().unwrap().insert(address);
    }

    pub fn remove_from_blacklist(&self, address: &Pubkey) {
        self.blacklisted_addresses.write().unwrap().remove(address);
    }

    pub fn add_trusted_program(&mut self, program_id: Pubkey) {
        self.trusted_programs.insert(program_id);
    }

    pub async fn validate_swap_safety(&self, input_mint: &Pubkey, output_mint: &Pubkey, amount: u64) -> Result<bool, Box<dyn std::error::Error>> {
        let input_metrics = self.validate_token(input_mint).await?;
        let output_metrics = self.validate_token(output_mint).await?;
        
        if input_metrics.risk_score > 0.6 || output_metrics.risk_score > 0.6 {
            return Ok(false);
        }
        
        let input_honeypot = self.check_honeypot(input_mint).await?;
        let output_honeypot = self.check_honeypot(output_mint).await?;
        
        if input_honeypot || output_honeypot {
            return Ok(false);
        }
        
        if amount > 0 {
            let input_supply = self.get_token_supply(input_mint).await?;
            let swap_percentage = (amount as f64 / input_supply as f64) * 100.0;
            if swap_percentage > 5.0 {
                return Ok(false);
            }
        }
        
        Ok(true)
    }

    pub async fn validate_slippage(&self, expected_amount: u64, min_amount: u64) -> bool {
        if expected_amount == 0 || min_amount > expected_amount {
            return false;
        }
        
        let slippage_bps = ((expected_amount - min_amount) as f64 / expected_amount as f64 * 10000.0) as u16;
        slippage_bps <= MAX_SLIPPAGE_BPS
    }
}

impl PriceOracle {
    fn new(rpc_client: Arc<RpcClient>) -> Self {
        let mut price_feeds = HashMap::new();
        
        price_feeds.insert("So11111111111111111111111111111111111111112".to_string(), 
            Pubkey::from_str("H6ARHf6YXhGYeQfUzQNGk6rDNnLBQKrenN712K4AQJEG").unwrap());
        price_feeds.insert("EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v".to_string(), 
            Pubkey::from_str("Gnt27xtC473ZT2Mw5u8wZ68Z3gULkSTb5DuxJy7eJotD").unwrap());
        price_feeds.insert("Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB".to_string(), 
            Pubkey::from_str("JBu1AL4obBcCMqKBBxhpWCNUt136ijcuMZLFvTP7iWdB").unwrap());
        
        Self {
            rpc_client,
            pyth_program: Pubkey::from_str(PYTH_PROGRAM_ID).unwrap(),
            price_feeds,
        }
    }

    async fn get_price(&self, mint: &Pubkey) -> Result<f64, Box<dyn std::error::Error>> {
        if *mint == spl_token::native_mint::id() {
            return Ok(self.get_sol_price().await?);
        }
        
        if let Some(price_feed) = self.price_feeds.get(&mint.to_string()) {
            return self.get_pyth_price(price_feed).await;
        }
        
        Ok(0.0)
    }

    async fn get_sol_price(&self) -> Result<f64, Box<dyn std::error::Error>> {
        let sol_feed = Pubkey::from_str("H6ARHf6YXhGYeQfUzQNGk6rDNnLBQKrenN712K4AQJEG")?;
        self.get_pyth_price(&sol_feed).await
    }

    async fn get_pyth_price(&self, price_feed: &Pubkey) -> Result<f64, Box<dyn std::error::Error>> {
        let account = self.rpc_client.get_account(price_feed)?;
        
        if account.data.len() < 48 {
            return Ok(0.0);
        }
        
        let price = i64::from_le_bytes(account.data[208..216].try_into()?);
        let expo = i32::from_le_bytes(account.data[228..232].try_into()?);
        
        if price <= 0 {
            return Ok(0.0);
        }
        
        Ok(price as f64 * 10f64.powi(expo))
    }
}

impl AnomalyDetector {
    fn new() -> Self {
        Self {
            baseline_metrics: RwLock::new(BaselineMetrics::default()),
            detection_window: Duration::from_secs(300),
        }
    }

    fn is_anomalous(&self, pattern: &TransactionPattern) -> bool {
        let baseline = self.baseline_metrics.read().unwrap();
        
        if baseline.avg_instruction_count == 0.0 {
            return false;
        }

        let instruction_z_score = (pattern.instruction_count as f64 - baseline.avg_instruction_count).abs() 
            / baseline.std_dev_instruction_count.max(1.0);
        let signer_z_score = (pattern.signer_count as f64 - baseline.avg_signer_count).abs() 
            / baseline.std_dev_signer_count.max(1.0);
        let compute_z_score = (pattern.compute_units as f64 - baseline.avg_compute_units).abs() 
            / baseline.std_dev_compute_units.max(1.0);
        let accounts_z_score = (pattern.accounts_touched as f64 - baseline.avg_accounts_touched).abs() 
            / baseline.std_dev_accounts_touched.max(1.0);
        
        let max_z_score = instruction_z_score.max(signer_z_score).max(compute_z_score).max(accounts_z_score);
        max_z_score > 3.5
    }

    fn update_baseline(&self, history: &VecDeque<TransactionPattern>) {
        if history.is_empty() {
            return;
        }

        let mut instruction_counts = Vec::new();
        let mut signer_counts = Vec::new();
        let mut compute_units = Vec::new();
        let mut accounts_touched = Vec::new();
        
        let cutoff_time = Instant::now() - self.detection_window;
        
        for pattern in history.iter() {
            if pattern.timestamp > cutoff_time {
                instruction_counts.push(pattern.instruction_count as f64);
                signer_counts.push(pattern.signer_count as f64);
                compute_units.push(pattern.compute_units as f64);
                accounts_touched.push(pattern.accounts_touched as f64);
            }
        }

        if instruction_counts.is_empty() {
            return;
        }

        let mut baseline = self.baseline_metrics.write().unwrap();
        
                baseline.avg_instruction_count = instruction_counts.iter().sum::<f64>() / instruction_counts.len() as f64;
        baseline.avg_signer_count = signer_counts.iter().sum::<f64>() / signer_counts.len() as f64;
        baseline.avg_compute_units = compute_units.iter().sum::<f64>() / compute_units.len() as f64;
        baseline.avg_accounts_touched = accounts_touched.iter().sum::<f64>() / accounts_touched.len() as f64;
        
        baseline.std_dev_instruction_count = Self::calculate_std_dev(&instruction_counts, baseline.avg_instruction_count);
        baseline.std_dev_signer_count = Self::calculate_std_dev(&signer_counts, baseline.avg_signer_count);
        baseline.std_dev_compute_units = Self::calculate_std_dev(&compute_units, baseline.avg_compute_units);
        baseline.std_dev_accounts_touched = Self::calculate_std_dev(&accounts_touched, baseline.avg_accounts_touched);
    }

    fn calculate_std_dev(values: &[f64], mean: f64) -> f64 {
        if values.len() <= 1 {
            return 0.0;
        }

        let variance = values.iter()
            .map(|x| (x - mean).powi(2))
            .sum::<f64>() / (values.len() - 1) as f64;
        
        variance.sqrt()
    }
}

#[derive(Debug)]
struct TokenDistribution {
    largest_holder_percent: f64,
    top_10_percent: f64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use solana_sdk::signature::Keypair;

    #[tokio::test]
    async fn test_anti_phishing_shield_initialization() {
        let shield = AntiPhishingShield::new("https://api.mainnet-beta.solana.com");
        assert!(shield.trusted_programs.contains(&spl_token::id()));
        assert!(shield.trusted_programs.contains(&Pubkey::from_str(RAYDIUM_AMM_V4).unwrap()));
    }

    #[tokio::test] 
    async fn test_validate_token_native_sol() {
        let shield = AntiPhishingShield::new("https://api.mainnet-beta.solana.com");
        let sol_mint = spl_token::native_mint::id();
        let result = shield.validate_token(&sol_mint).await;
        assert!(result.is_ok());
        if let Ok(metrics) = result {
            assert!(metrics.risk_score < 0.3);
            assert!(metrics.mint_disabled);
            assert!(metrics.freeze_disabled);
        }
    }

    #[tokio::test]
    async fn test_blacklist_functionality() {
        let shield = AntiPhishingShield::new("https://api.mainnet-beta.solana.com");
        let test_address = Keypair::new().pubkey();
        
        shield.add_to_blacklist(test_address);
        assert!(shield.is_blacklisted(&test_address).await);
        
        shield.remove_from_blacklist(&test_address);
        assert!(!shield.is_blacklisted(&test_address).await);
    }

    #[tokio::test]
    async fn test_validate_slippage() {
        let shield = AntiPhishingShield::new("https://api.mainnet-beta.solana.com");
        
        assert!(shield.validate_slippage(1000000, 970000).await);
        assert!(!shield.validate_slippage(1000000, 950000).await);
        assert!(!shield.validate_slippage(0, 100).await);
        assert!(!shield.validate_slippage(1000, 2000).await);
    }

    #[tokio::test]
    async fn test_anomaly_detection() {
        let detector = AnomalyDetector::new();
        let mut history = VecDeque::new();
        
        for i in 0..100 {
            history.push_back(TransactionPattern {
                timestamp: Instant::now(),
                program_id: Pubkey::new_unique(),
                instruction_count: 2,
                signer_count: 1,
                compute_units: 200000,
                accounts_touched: 5,
            });
        }
        
        detector.update_baseline(&history);
        
        let normal_pattern = TransactionPattern {
            timestamp: Instant::now(),
            program_id: Pubkey::new_unique(),
            instruction_count: 2,
            signer_count: 1,
            compute_units: 200000,
            accounts_touched: 5,
        };
        assert!(!detector.is_anomalous(&normal_pattern));
        
        let anomalous_pattern = TransactionPattern {
            timestamp: Instant::now(),
            program_id: Pubkey::new_unique(),
            instruction_count: 20,
            signer_count: 10,
            compute_units: 1400000,
            accounts_touched: 50,
        };
        assert!(detector.is_anomalous(&anomalous_pattern));
    }

    #[tokio::test]
    async fn test_compute_units_estimation() {
        let shield = AntiPhishingShield::new("https://api.mainnet-beta.solana.com");
        
        let instruction = Instruction {
            program_id: spl_token::id(),
            accounts: vec![0, 1, 2],
            data: vec![0; 64],
        };
        
        let cu = shield.estimate_compute_units(&[instruction]);
        assert!(cu > 0);
        assert!(cu <= 1_400_000);
    }

    #[tokio::test]
    async fn test_malicious_instruction_detection() {
        let shield = AntiPhishingShield::new("https://api.mainnet-beta.solana.com");
        
        assert!(shield.is_malicious_instruction_pattern(&[0xff, 0xff, 0xff, 0xff, 0x00]));
        assert!(shield.is_malicious_instruction_pattern(&[0xde, 0xad, 0xbe, 0xef, 0x00]));
        assert!(shield.is_malicious_instruction_pattern(&vec![0; 2000]));
        assert!(!shield.is_malicious_instruction_pattern(&[0x01, 0x02, 0x03, 0x04]));
    }

    #[tokio::test]
    async fn test_price_oracle() {
        let rpc_client = Arc::new(RpcClient::new("https://api.mainnet-beta.solana.com"));
        let oracle = PriceOracle::new(rpc_client);
        
        let sol_price = oracle.get_price(&spl_token::native_mint::id()).await;
        assert!(sol_price.is_ok());
        if let Ok(price) = sol_price {
            assert!(price > 0.0);
        }
    }

    #[tokio::test]
    async fn test_extract_amount() {
        let shield = AntiPhishingShield::new("https://api.mainnet-beta.solana.com");
        
        let data = vec![3, 100, 0, 0, 0, 0, 0, 0, 0];
        let amount = shield.extract_amount_from_instruction(&data);
        assert_eq!(amount, Some(100));
        
        let short_data = vec![3];
        let no_amount = shield.extract_amount_from_instruction(&short_data);
        assert_eq!(no_amount, None);
    }

    #[tokio::test]
    async fn test_token_distribution_analysis() {
        let shield = AntiPhishingShield::new("https://api.mainnet-beta.solana.com");
        
        let mut accounts = Vec::new();
        for i in 0..10 {
            let mut data = vec![0u8; 165];
            let amount = if i == 0 { 5000000000u64 } else { 100000000u64 };
            data[64..72].copy_from_slice(&amount.to_le_bytes());
            
            accounts.push(Account {
                lamports: 2039280,
                data,
                owner: spl_token::id(),
                executable: false,
                rent_epoch: 0,
            });
        }
        
        let total_supply = 5900000000u64;
        let distribution = shield.analyze_token_distribution(&accounts, total_supply).await;
        
        assert!(distribution.largest_holder_percent > 80.0);
        assert!(distribution.top_10_percent == 100.0);
    }

    #[tokio::test]
    async fn test_cache_functionality() {
        let shield = AntiPhishingShield::new("https://api.mainnet-beta.solana.com");
        let test_mint = Pubkey::new_unique();
        
        let metrics = SecurityMetrics {
            risk_score: 0.5,
            is_verified: true,
            liquidity_locked: true,
            mint_disabled: true,
            freeze_disabled: true,
            ownership_renounced: true,
            creation_timestamp: 1700000000,
            holder_count: 1000,
            transaction_count: 5000,
            largest_holder_percent: 5.0,
            liquidity_usd: 100000.0,
            last_update: Instant::now(),
        };
        
        shield.cache_metrics(&test_mint, metrics.clone()).await;
        let cached = shield.get_cached_metrics(&test_mint).await;
        
        assert!(cached.is_some());
        if let Some(cached_metrics) = cached {
            assert_eq!(cached_metrics.metrics.risk_score, 0.5);
            assert_eq!(cached_metrics.metrics.holder_count, 1000);
        }
    }
}

