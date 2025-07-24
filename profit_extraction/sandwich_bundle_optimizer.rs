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
    transaction::Transaction,
    hash::Hash,
};
use solana_transaction_status::UiTransactionEncoding;
use std::{
    collections::{HashMap, VecDeque},
    sync::{Arc, Mutex},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
    str::FromStr,
};
use tokio::sync::{mpsc, RwLock, Semaphore};
use borsh::{BorshDeserialize, BorshSerialize};
use sha2::{Sha256, Digest};

const RAYDIUM_AMM_V4: &str = "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8";
const ORCA_WHIRLPOOL: &str = "whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc";
const COMPUTE_UNITS: u32 = 1_400_000;
const MAX_PRIORITY_FEE_LAMPORTS: u64 = 50_000;
const MIN_PROFIT_LAMPORTS: u64 = 500_000;
const MAX_SLIPPAGE_BPS: u16 = 500;
const BUNDLE_EXPIRY_MS: u64 = 200;
const MAX_CONCURRENT_BUNDLES: usize = 10;
const PROFIT_SAFETY_MARGIN: f64 = 0.85;

#[derive(Debug, Clone)]
pub struct SandwichBundle {
    pub id: [u8; 32],
    pub victim_signature: Signature,
    pub front_run_tx: Transaction,
    pub back_run_tx: Transaction,
    pub expected_profit: u64,
    pub priority_fee: u64,
    pub pool_address: Pubkey,
    pub dex_program: DexProgram,
    pub created_at: Instant,
    pub expiry_slot: u64,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum DexProgram {
    RaydiumV4,
    OrcaWhirlpool,
}

#[derive(Debug, Clone)]
struct PoolReserves {
    token_a_reserve: u64,
    token_b_reserve: u64,
    fee_numerator: u64,
    fee_denominator: u64,
    sqrt_price_x64: Option<u128>,
    liquidity: Option<u128>,
    tick_current: Option<i32>,
}

#[derive(Debug)]
struct BundleMetrics {
    total_profit: u64,
    successful_bundles: u64,
    failed_bundles: u64,
    avg_execution_time_ms: f64,
    profit_per_gas: f64,
}

pub struct SandwichBundleOptimizer {
    rpc_client: Arc<RpcClient>,
    wallet: Arc<Keypair>,
    pool_cache: Arc<RwLock<HashMap<Pubkey, (PoolReserves, Instant)>>>,
    metrics: Arc<Mutex<BundleMetrics>>,
    bundle_semaphore: Arc<Semaphore>,
    active_bundles: Arc<RwLock<HashMap<[u8; 32], SandwichBundle>>>,
}

impl SandwichBundleOptimizer {
    pub fn new(rpc_endpoint: &str, wallet: Keypair) -> Self {
        let rpc_client = RpcClient::new_with_commitment(
            rpc_endpoint.to_string(),
            CommitmentConfig::confirmed(),
        );

        Self {
            rpc_client: Arc::new(rpc_client),
            wallet: Arc::new(wallet),
            pool_cache: Arc::new(RwLock::new(HashMap::new())),
            metrics: Arc::new(Mutex::new(BundleMetrics {
                total_profit: 0,
                successful_bundles: 0,
                failed_bundles: 0,
                avg_execution_time_ms: 0.0,
                profit_per_gas: 0.0,
            })),
            bundle_semaphore: Arc::new(Semaphore::new(MAX_CONCURRENT_BUNDLES)),
            active_bundles: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub async fn analyze_sandwich_opportunity(
        &self,
        victim_tx: &Transaction,
        pool_address: Pubkey,
        token_a: Pubkey,
        token_b: Pubkey,
        victim_amount: u64,
        victim_is_buy: bool,
    ) -> Result<Option<SandwichBundle>, Box<dyn std::error::Error + Send + Sync>> {
        let dex_program = self.identify_dex_program(&victim_tx)?;
        let pool_state = self.fetch_pool_state(&pool_address, dex_program).await?;
        
        let optimal_sandwich = self.calculate_optimal_sandwich(
            &pool_state,
            victim_amount,
            victim_is_buy,
            dex_program,
        )?;

        if optimal_sandwich.profit < MIN_PROFIT_LAMPORTS {
            return Ok(None);
        }

        let current_slot = self.rpc_client.get_slot().await?;
        let priority_fee = self.calculate_dynamic_priority_fee(
            optimal_sandwich.profit,
            current_slot,
        ).await?;

        let bundle = self.construct_sandwich_bundle(
            victim_tx,
            pool_address,
            token_a,
            token_b,
            optimal_sandwich,
            priority_fee,
            dex_program,
            current_slot,
        ).await?;

        Ok(Some(bundle))
    }

    fn identify_dex_program(&self, tx: &Transaction) -> Result<DexProgram, Box<dyn std::error::Error + Send + Sync>> {
        for instruction in &tx.message.instructions {
            let program_id = tx.message.account_keys[instruction.program_id_index as usize];
            
            if program_id == Pubkey::from_str(RAYDIUM_AMM_V4)? {
                return Ok(DexProgram::RaydiumV4);
            } else if program_id == Pubkey::from_str(ORCA_WHIRLPOOL)? {
                return Ok(DexProgram::OrcaWhirlpool);
            }
        }
        
        Err("Unsupported DEX program".into())
    }

    async fn fetch_pool_state(
        &self,
        pool_address: &Pubkey,
        dex_program: DexProgram,
    ) -> Result<PoolReserves, Box<dyn std::error::Error + Send + Sync>> {
        let cache = self.pool_cache.read().await;
        if let Some((state, timestamp)) = cache.get(pool_address) {
            if timestamp.elapsed() < Duration::from_millis(100) {
                return Ok(state.clone());
            }
        }
        drop(cache);

        let account_data = self.rpc_client.get_account_data(pool_address).await?;
        let pool_state = match dex_program {
            DexProgram::RaydiumV4 => self.parse_raydium_pool(&account_data)?,
            DexProgram::OrcaWhirlpool => self.parse_whirlpool(&account_data)?,
        };

        let mut cache = self.pool_cache.write().await;
        cache.insert(*pool_address, (pool_state.clone(), Instant::now()));

        Ok(pool_state)
    }

    fn parse_raydium_pool(&self, data: &[u8]) -> Result<PoolReserves, Box<dyn std::error::Error + Send + Sync>> {
        if data.len() < 752 {
            return Err("Invalid Raydium pool data".into());
        }

        Ok(PoolReserves {
            token_a_reserve: u64::from_le_bytes(data[328..336].try_into()?),
            token_b_reserve: u64::from_le_bytes(data[336..344].try_into()?),
            fee_numerator: u64::from_le_bytes(data[576..584].try_into()?),
            fee_denominator: 10000,
            sqrt_price_x64: None,
            liquidity: None,
            tick_current: None,
        })
    }

    fn parse_whirlpool(&self, data: &[u8]) -> Result<PoolReserves, Box<dyn std::error::Error + Send + Sync>> {
        if data.len() < 653 {
            return Err("Invalid Whirlpool data".into());
        }

        let sqrt_price_x64 = u128::from_le_bytes(data[65..81].try_into()?);
        let liquidity = u128::from_le_bytes(data[81..97].try_into()?);
        let tick_current = i32::from_le_bytes(data[97..101].try_into()?);
        let fee_rate = u16::from_le_bytes(data[101..103].try_into()?) as u64;

        Ok(PoolReserves {
            token_a_reserve: 0,
            token_b_reserve: 0,
            fee_numerator: fee_rate,
            fee_denominator: 1_000_000,
            sqrt_price_x64: Some(sqrt_price_x64),
            liquidity: Some(liquidity),
            tick_current: Some(tick_current),
        })
    }

    #[derive(Debug)]
    struct OptimalSandwich {
        front_run_amount: u64,
        expected_output: u64,
        profit: u64,
        gas_cost: u64,
    }

    fn calculate_optimal_sandwich(
        &self,
        pool: &PoolReserves,
        victim_amount: u64,
        victim_is_buy: bool,
        dex_program: DexProgram,
    ) -> Result<OptimalSandwich, Box<dyn std::error::Error + Send + Sync>> {
        match dex_program {
            DexProgram::RaydiumV4 => self.optimize_raydium_sandwich(pool, victim_amount, victim_is_buy),
            DexProgram::OrcaWhirlpool => self.optimize_whirlpool_sandwich(pool, victim_amount, victim_is_buy),
        }
    }

    fn optimize_raydium_sandwich(
        &self,
        pool: &PoolReserves,
        victim_amount: u64,
        victim_is_buy: bool,
    ) -> Result<OptimalSandwich, Box<dyn std::error::Error + Send + Sync>> {
        let (reserve_in, reserve_out) = if victim_is_buy {
            (pool.token_a_reserve, pool.token_b_reserve)
        } else {
            (pool.token_b_reserve, pool.token_a_reserve)
        };

        let fee_factor = pool.fee_denominator - pool.fee_numerator;
        let k = reserve_in as u128 * reserve_out as u128;

        let optimal_front_run = ((victim_amount as f64 * reserve_out as f64 * fee_factor as f64 
            / pool.fee_denominator as f64).sqrt() - reserve_in as f64).max(0.0) as u64;

        let front_run_capped = optimal_front_run.min(reserve_in / 20);

        let mut temp_reserve_in = reserve_in;
        let mut temp_reserve_out = reserve_out;

        let front_amount_with_fee = (front_run_capped as u128 * fee_factor as u128) / pool.fee_denominator as u128;
        let front_output = (front_amount_with_fee * temp_reserve_out as u128) 
            / (temp_reserve_in as u128 + front_amount_with_fee);

        temp_reserve_in += front_run_capped;
        temp_reserve_out -= front_output as u64;

        let victim_with_fee = (victim_amount as u128 * fee_factor as u128) / pool.fee_denominator as u128;
        let victim_output = (victim_with_fee * temp_reserve_out as u128) 
            / (temp_reserve_in as u128 + victim_with_fee);

        temp_reserve_in += victim_amount;
        temp_reserve_out -= victim_output as u64;

        let back_amount_with_fee = (front_output * fee_factor as u128) / pool.fee_denominator as u128;
        let back_output = (back_amount_with_fee * temp_reserve_in as u128) 
            / (temp_reserve_out as u128 + back_amount_with_fee);

        let profit = back_output.saturating_sub(front_run_capped as u128) as u64;
        let gas_cost = 100_000;

        Ok(OptimalSandwich {
            front_run_amount: front_run_capped,
            expected_output: front_output as u64,
            profit: profit.saturating_sub(gas_cost),
            gas_cost,
        })
    }

    fn optimize_whirlpool_sandwich(
        &self,
        pool: &PoolReserves,
        victim_amount: u64,
        victim_is_buy: bool,
    ) -> Result<OptimalSandwich, Box<dyn std::error::Error + Send + Sync>> {
        let sqrt_price = pool.sqrt_price_x64.ok_or("Missing sqrt price")?;
        let liquidity = pool.liquidity.ok_or("Missing liquidity")?;
        
        let price = (sqrt_price as f64 / (1u64 << 64) as f64).powi(2);
        let fee_rate = pool.fee_numerator as f64 / pool.fee_denominator as f64;

        let optimal_delta = (victim_amount as f64 * (1.0 - fee_rate)).sqrt();
        let front_run_amount = (optimal_delta * price.sqrt()) as u64;

        let simulated_profit = self.simulate_whirlpool_swap(
            sqrt_price,
            liquidity,
            front_run_amount,
            victim_amount,
            fee_rate,
            victim_is_buy,
        )?;

        let gas_cost = 120_000;

        Ok(OptimalSandwich {
            front_run_amount,
            expected_output: (front_run_amount as f64 / price) as u64,
            profit: simulated_profit.saturating_sub(gas_cost),
            gas_cost,
        })
    }

    fn simulate_whirlpool_swap(
        &self,
        sqrt_price: u128,
        liquidity: u128,
        front_amount: u64,
        victim_amount: u64,
        fee_rate: f64,
        victim_is_buy: bool,
    ) -> Result<u64, Box<dyn std::error::Error + Send + Sync>> {
        let price = (sqrt_price as f64 / (1u64 << 64) as f64).powi(2);
        
        let front_output = if victim_is_buy {
            (front_amount as f64 / price * (1.0 - fee_rate)) as u64
        } else {
            (front_amount as f64 * price * (1.0 - fee_rate)) as u64
        };

        let price_impact = front_amount as f64 / liquidity as f64;
        let new_price = price * (1.0 + price_impact * if victim_is_buy { 1.0 } else { -1.0 });

        let victim_impact = victim_amount as f64 / liquidity as f64;
        let post_victim_price = new_price * (1.0 + victim_impact * if victim_is_buy { 1.0 } else { -1.0 });

        let back_output = if victim_is_buy {
            (front_output as f64 * post_victim_price * (1.0 - fee_rate)) as u64
        } else {
            (front_output as f64 / post_victim_price * (1.0 - fee_rate)) as u64
        };

        Ok(back_output.saturating_sub(front_amount))
    }

    async fn calculate_dynamic_priority_fee(
        &self,
        expected_profit: u64,
        current_slot: u64,
    ) -> Result<u64, Box<dyn std::error::Error + Send + Sync>> {
        let recent_fees = self.rpc_client.get_recent_prioritization_fees(&[]).await?;
        
        let avg_fee = if !recent_fees.is_empty() {
            recent_fees.iter().map(|f| f.prioritization_fee).sum::<u64>() / recent_fees.len() as u64
        } else {
            5_000
        };

        let competitive_fee = avg_fee * 3 / 2;
        let profit_based_fee = (expected_profit * 12) / 100;
        let final_fee = competitive_fee.max(profit_based_fee).min(MAX_PRIORITY_FEE_LAMPORTS);

        Ok(final_fee)
    }

    async fn construct_sandwich_bundle(
        &self,
        victim_tx: &Transaction,
        pool_address: Pubkey,
        token_a: Pubkey,
        token_b: Pubkey,
        optimal: OptimalSandwich,
        priority_fee: u64,
        dex_program: DexProgram,
        current_slot: u64,
    ) -> Result<SandwichBundle, Box<dyn std::error::Error + Send + Sync>> {
        let victim_signature = self.extract_victim_signature(victim_tx)?;
        
        let front_run_tx = self.build_swap_transaction(
            pool_address,
            token_a,
            token_b,
            optimal.front_run_amount,
            self.calculate_min_output(optimal.expected_output),
            true,
            priority_fee,
            dex_program,
        ).await?;

        let back_run_tx = self.build_swap_transaction(
            pool_address,
            token_b,
            token_a,
            optimal.expected_output,
            self.calculate_min_output(optimal.front_run_amount + optimal.profit),
            false,
            priority_fee,
            dex_program,
        ).await?;

        let bundle_id = self.generate_bundle_id(&front_run_tx, &back_run_tx);

        Ok(SandwichBundle {
            id: bundle_id,
            victim_signature,
            front_run_tx,
            back_run_tx,
            expected_profit: optimal.profit,
            priority_fee,
            pool_address,
            dex_program,
            created_at: Instant::now(),
            expiry_slot: current_slot + 150,
        })
    }

    fn extract_victim_signature(&self, tx: &Transaction) -> Result<Signature, Box<dyn std::error::Error + Send + Sync>> {
        if tx.signatures.is_empty() {
            return Err("No signatures in victim transaction".into());
        }
        Ok(tx.signatures[0])
    }

    fn calculate_min_output(&self, expected: u64) -> u64 {
        let slippage_factor = 10000 - MAX_SLIPPAGE_BPS as u64;
        (expected * slippage_factor) / 10000
    }

    async fn build_swap_transaction(
        &self,
        pool: Pubkey,
        token_in: Pubkey,
        token_out: Pubkey,
        amount_in: u64,
        min_amount_out: u64,
        is_front_run: bool,
        priority_fee: u64,
        dex_program: DexProgram,
    ) -> Result<Transaction, Box<dyn std::error::Error + Send + Sync>> {
        let wallet_pubkey = self.wallet.pubkey();
        
        let source_ata = spl_associated_token_account::get_associated_token_address(&wallet_pubkey, &token_in);
        let dest_ata = spl_associated_token_account::get_associated_token_address(&wallet_pubkey, &token_out);

        let swap_instruction = match dex_program {
            DexProgram::RaydiumV4 => self.build_raydium_swap_ix(
                pool, source_ata, dest_ata, amount_in, min_amount_out,
            )?,
            DexProgram::OrcaWhirlpool => self.build_whirlpool_swap_ix(
                pool, source_ata, dest_ata, token_in, token_out, amount_in, min_amount_out,
            )?,
        };

        let compute_budget_ix = ComputeBudgetInstruction::set_compute_unit_limit(COMPUTE_UNITS);
        let priority_fee_ix = ComputeBudgetInstruction::set_compute_unit_price(priority_fee);

        let recent_blockhash = self.rpc_client.get_latest_blockhash().await?;

        let mut transaction = Transaction::new_with_payer(
            &[compute_budget_ix, priority_fee_ix, swap_instruction],
            Some(&wallet_pubkey),
        );

        transaction.sign(&[self.wallet.as_ref()], recent_blockhash);

        Ok(transaction)
    }

    fn build_raydium_swap_ix(
        &self,
        pool: Pubkey,
        source: Pubkey,
        destination: Pubkey,
        amount_in: u64,
        min_amount_out: u64,
    ) -> Result<Instruction, Box<dyn std::error::Error + Send + Sync>> {
        #[derive(BorshSerialize)]
        struct SwapInstruction {
            instruction: u8,
            amount_in: u64,
            min_amount_out: u64,
        }

        let data = SwapInstruction {
            instruction: 9,
            amount_in,
            min_amount_out,
        };

        Ok(Instruction {
            program_id: Pubkey::from_str(RAYDIUM_AMM_V4)?,
            accounts: vec![
                AccountMeta::new_readonly(spl_token::id(), false),
                AccountMeta::new(pool, false),
                AccountMeta::new_readonly(self.wallet.pubkey(), true),
                AccountMeta::new(source, false),
                AccountMeta::new(destination, false),
            ],
            data: data.try_to_vec()?,
        })
    }

    fn build_whirlpool_swap_ix(
        &self,
        pool: Pubkey,
        source: Pubkey,
        destination: Pubkey,
        token_in: Pubkey,
        token_out: Pubkey,
        amount: u64,
        min_amount_out: u64,
    ) -> Result<Instruction, Box<dyn std::error::Error + Send + Sync>> {
        #[derive(BorshSerialize)]
        struct WhirlpoolSwap {
            amount: u64,
            other_amount_threshold: u64,
            sqrt_price_limit: u128,
            amount_specified_is_input: bool,
            a_to_b: bool,
        }

        let data = WhirlpoolSwap {
            amount,
            other_amount_threshold: min_amount_out,
            sqrt_price_limit: 0,
            amount_specified_is_input: true,
            a_to_b: true,
        };

        Ok(Instruction {
            program_id: Pubkey::from_str(ORCA_WHIRLPOOL)?,
            accounts: vec![
                AccountMeta::new_readonly(spl_token::id(), false),
                AccountMeta::new_readonly(self.wallet.pubkey(), true),
                AccountMeta::new(pool, false),
                AccountMeta::new(source, false),
                AccountMeta::new(destination, false),
            ],
            data: data.try_to_vec()?,
        })
    }

    fn generate_bundle_id(&self, front_tx: &Transaction, back_tx: &Transaction) -> [u8; 32] {
        let mut hasher = Sha256::new();
        hasher.update(&front_tx.signatures[0].as_ref());
        hasher.update(&back_tx.signatures[0].as_ref());
        hasher.update(&SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos().to_le_bytes());
        
        let result = hasher.finalize();
        let mut bundle_id = [0u8; 32];
        bundle_id.copy_from_slice(&result);
        bundle_id
    }

    pub async fn execute_bundle(&self, bundle: SandwichBundle) -> Result<bool, Box<dyn std::error::Error + Send + Sync>> {
        let _permit = self.bundle_semaphore.acquire().await?;
        let start_time = Instant::now();

        if bundle.created_at.elapsed() > Duration::from_millis(BUNDLE_EXPIRY_MS) {
            self.update_metrics(false, 0, start_time);
            return Ok(false);
        }

        self.active_bundles.write().await.insert(bundle.id, bundle.clone());

        let simulation_config = RpcSimulateTransactionConfig {
            sig_verify: true,
            replace_recent_blockhash: true,
            commitment: Some(CommitmentConfig::processed()),
            encoding: Some(UiTransactionEncoding::Base64),
            accounts: None,
            min_context_slot: None,
        };

        let front_sim = self.rpc_client.simulate_transaction_with_config(
            &bundle.front_run_tx,
            simulation_config.clone(),
        ).await?;

        if front_sim.value.err.is_some() {
            self.active_bundles.write().await.remove(&bundle.id);
            self.update_metrics(false, 0, start_time);
            return Ok(false);
        }

        let send_config = RpcSendTransactionConfig {
            skip_preflight: true,
            preflight_commitment: Some(CommitmentLevel::Processed),
            encoding: Some(UiTransactionEncoding::Base64),
            max_retries: Some(2),
            min_context_slot: None,
        };

        let front_sig = self.rpc_client.send_transaction_with_config(
            &bundle.front_run_tx,
            send_config.clone(),
        ).await?;

        tokio::time::sleep(Duration::from_millis(50)).await;

        let back_sig = self.rpc_client.send_transaction_with_config(
            &bundle.back_run_tx,
            send_config,
        ).await?;

        let success = self.await_bundle_confirmation(front_sig, back_sig).await?;

        self.active_bundles.write().await.remove(&bundle.id);
        self.update_metrics(success, if success { bundle.expected_profit } else { 0 }, start_time);

        Ok(success)
    }

    async fn await_bundle_confirmation(
        &self,
        front_sig: Signature,
        back_sig: Signature,
    ) -> Result<bool, Box<dyn std::error::Error + Send + Sync>> {
        let timeout = Duration::from_millis(BUNDLE_EXPIRY_MS);
        let start = Instant::now();

        while start.elapsed() < timeout {
            let front_status = self.rpc_client.get_signature_status(&front_sig).await?;
            let back_status = self.rpc_client.get_signature_status(&back_sig).await?;

            match (front_status, back_status) {
                (Some(Ok(_)), Some(Ok(_))) => return Ok(true),
                (Some(Err(_)), _) | (_, Some(Err(_))) => return Ok(false),
                _ => tokio::time::sleep(Duration::from_millis(20)).await,
            }
        }

        Ok(false)
    }

    fn update_metrics(&self, success: bool, profit: u64, start_time: Instant) {
        let mut metrics = self.metrics.lock().unwrap();
        
        if success {
            metrics.successful_bundles += 1;
            metrics.total_profit += profit;
        } else {
            metrics.failed_bundles += 1;
        }

        let execution_time_ms = start_time.elapsed().as_millis() as f64;
        let total_bundles = metrics.successful_bundles + metrics.failed_bundles;
        
        metrics.avg_execution_time_ms = 
            ((metrics.avg_execution_time_ms * (total_bundles - 1) as f64) + execution_time_ms) / total_bundles as f64;

        if metrics.total_profit > 0 && metrics.successful_bundles > 0 {
            let total_gas = metrics.successful_bundles * 100_000;
            metrics.profit_per_gas = metrics.total_profit as f64 / total_gas as f64;
        }
    }

    pub async fn optimize_bundle_batch(
        &self,
        opportunities: Vec<(Transaction, Pubkey, Pubkey, Pubkey, u64, bool)>,
    ) -> Result<Vec<SandwichBundle>, Box<dyn std::error::Error + Send + Sync>> {
        let mut bundles = Vec::new();
        let mut handles = Vec::new();

        for (victim_tx, pool, token_a, token_b, amount, is_buy) in opportunities {
            let optimizer = self.clone();
            let handle = tokio::spawn(async move {
                optimizer.analyze_sandwich_opportunity(
                    &victim_tx,
                    pool,
                    token_a,
                    token_b,
                    amount,
                    is_buy,
                ).await
            });
            handles.push(handle);
        }

        for handle in handles {
            if let Ok(Ok(Some(bundle))) = handle.await {
                bundles.push(bundle);
            }
        }

        bundles.sort_by(|a, b| {
            let a_score = (a.expected_profit as f64 / a.priority_fee as f64) * PROFIT_SAFETY_MARGIN;
            let b_score = (b.expected_profit as f64 / b.priority_fee as f64) * PROFIT_SAFETY_MARGIN;
            b_score.partial_cmp(&a_score).unwrap()
        });

        Ok(bundles.into_iter().take(MAX_CONCURRENT_BUNDLES).collect())
    }

    pub async fn validate_and_execute(
        &self,
        bundle: SandwichBundle,
    ) -> Result<bool, Box<dyn std::error::Error + Send + Sync>> {
        let current_slot = self.rpc_client.get_slot().await?;
        
        if current_slot > bundle.expiry_slot {
            return Ok(false);
        }

        let pool_state = self.fetch_pool_state(&bundle.pool_address, bundle.dex_program).await?;
        let revalidated = self.revalidate_profitability(&bundle, &pool_state)?;

        if !revalidated {
            return Ok(false);
        }

        self.execute_bundle(bundle).await
    }

    fn revalidate_profitability(
        &self,
        bundle: &SandwichBundle,
        current_pool_state: &PoolReserves,
    ) -> Result<bool, Box<dyn std::error::Error + Send + Sync>> {
        let simulated = match bundle.dex_program {
            DexProgram::RaydiumV4 => {
                let (_, _, profit, _) = self.optimize_raydium_sandwich(
                    current_pool_state,
                    bundle.amount_in,
                    true,
                )?.into();
                profit
            },
            DexProgram::OrcaWhirlpool => {
                let (_, _, profit, _) = self.optimize_whirlpool_sandwich(
                    current_pool_state,
                    bundle.amount_in,
                    true,
                )?.into();
                profit
            },
        };

        Ok(simulated >= (bundle.expected_profit as f64 * PROFIT_SAFETY_MARGIN) as u64)
    }

    pub async fn monitor_active_bundles(&self) {
        loop {
            let now = Instant::now();
            let mut expired_ids = Vec::new();

            {
                let bundles = self.active_bundles.read().await;
                for (id, bundle) in bundles.iter() {
                    if bundle.created_at.elapsed() > Duration::from_millis(BUNDLE_EXPIRY_MS * 2) {
                        expired_ids.push(*id);
                    }
                }
            }

            if !expired_ids.is_empty() {
                let mut bundles = self.active_bundles.write().await;
                for id in expired_ids {
                    bundles.remove(&id);
                }
            }

            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    pub fn get_metrics(&self) -> BundleMetrics {
        let metrics = self.metrics.lock().unwrap();
        BundleMetrics {
            total_profit: metrics.total_profit,
            successful_bundles: metrics.successful_bundles,
            failed_bundles: metrics.failed_bundles,
            avg_execution_time_ms: metrics.avg_execution_time_ms,
            profit_per_gas: metrics.profit_per_gas,
        }
    }

    pub async fn emergency_shutdown(&self) {
        let active_bundles = self.active_bundles.read().await;
        if !active_bundles.is_empty() {
            log::warn!("Emergency shutdown with {} active bundles", active_bundles.len());
        }
        drop(active_bundles);
        
        self.pool_cache.write().await.clear();
    }
}

impl Clone for SandwichBundleOptimizer {
    fn clone(&self) -> Self {
        Self {
            rpc_client: Arc::clone(&self.rpc_client),
            wallet: Arc::clone(&self.wallet),
            pool_cache: Arc::clone(&self.pool_cache),
            metrics: Arc::clone(&self.metrics),
            bundle_semaphore: Arc::clone(&self.bundle_semaphore),
            active_bundles: Arc::clone(&self.active_bundles),
        }
    }
}

impl From<OptimalSandwich> for (u64, u64, u64, u64) {
    fn from(val: OptimalSandwich) -> Self {
        (val.front_run_amount, val.expected_output, val.profit, val.gas_cost)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use solana_sdk::signature::Keypair;

    #[tokio::test]
    async fn test_bundle_optimizer_initialization() {
        let wallet = Keypair::new();
        let optimizer = SandwichBundleOptimizer::new(
            "https://api.mainnet-beta.solana.com",
            wallet,
        );

        let metrics = optimizer.get_metrics();
        assert_eq!(metrics.total_profit, 0);
        assert_eq!(metrics.successful_bundles, 0);
        assert_eq!(metrics.failed_bundles, 0);
    }

    #[test]
    fn test_minimum_output_calculation() {
        let wallet = Keypair::new();
        let optimizer = SandwichBundleOptimizer::new(
            "https://api.mainnet-beta.solana.com",
            wallet,
        );

        let expected_output = 1_000_000_000;
        let min_output = optimizer.calculate_min_output(expected_output);
        
        assert_eq!(min_output, (expected_output * (10000 - MAX_SLIPPAGE_BPS as u64)) / 10000);
    }

    #[test]
    fn test_pool_reserves_parsing() {
        let wallet = Keypair::new();
        let optimizer = SandwichBundleOptimizer::new(
            "https://api.mainnet-beta.solana.com",
            wallet,
        );

        let mut mock_data = vec![0u8; 752];
        let reserve_a: u64 = 1_000_000_000_000;
        let reserve_b: u64 = 500_000_000_000;
        let fee_num: u64 = 25;

        mock_data[328..336].copy_from_slice(&reserve_a.to_le_bytes());
        mock_data[336..344].copy_from_slice(&reserve_b.to_le_bytes());
        mock_data[576..584].copy_from_slice(&fee_num.to_le_bytes());

        let pool_state = optimizer.parse_raydium_pool(&mock_data).unwrap();
        
        assert_eq!(pool_state.token_a_reserve, reserve_a);
        assert_eq!(pool_state.token_b_reserve, reserve_b);
        assert_eq!(pool_state.fee_numerator, fee_num);
        assert_eq!(pool_state.fee_denominator, 10000);
    }
}
