use anchor_client::{
    solana_sdk::{
        commitment_config::CommitmentConfig,
        instruction::{AccountMeta, Instruction},
        pubkey::Pubkey,
        signature::{Keypair, Signature},
        signer::Signer,
        system_program,
        transaction::Transaction,
    },
    Client, Cluster, Program,
};
use borsh::{BorshDeserialize, BorshSerialize};
use raydium_amm::instruction::{swap_base_in, SwapBaseInArgs};
use solana_client::{
    nonblocking::rpc_client::RpcClient,
    rpc_config::{RpcSendTransactionConfig, RpcTransactionConfig},
    rpc_response::RpcResult,
};
use solana_sdk::{
    compute_budget::ComputeBudgetInstruction,
    hash::Hash,
    instruction::InstructionError,
    native_token::LAMPORTS_PER_SOL,
    program_pack::Pack,
    transaction::TransactionError,
};
use solana_transaction_status::{
    EncodedConfirmedTransactionWithStatusMeta, UiTransactionEncoding,
};
use spl_associated_token_account::get_associated_token_address;
use spl_token::state::{Account as TokenAccount, Mint};
use std::{
    collections::{HashMap, HashSet},
    str::FromStr,
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::{
    sync::{Mutex, RwLock},
    time::{interval, sleep},
};

const RAYDIUM_V4: &str = "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8";
const WSOL_MINT: &str = "So11111111111111111111111111111111111111112";
const TOKEN_PROGRAM: &str = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA";
const RENT_PROGRAM: &str = "SysvarRent111111111111111111111111111111111";
const MAX_RETRIES: u8 = 3;
const BASE_PRIORITY_FEE: u64 = 100_000;
const MAX_PRIORITY_FEE: u64 = 5_000_000;
const SLIPPAGE_BPS: u64 = 500;
const MIN_LIQUIDITY_SOL: u64 = 5 * LAMPORTS_PER_SOL;
const MAX_BUY_SOL: u64 = 10 * LAMPORTS_PER_SOL;
const JITO_TIP_ACCOUNTS: [&str; 8] = [
    "96gYZGLnJYVFmbjzopPSU6QiEV5fGqZNyN9nmNhvrZU5",
    "HFqU5x63VTqvQss8hp11i4wVV8bD44PvwucfZ2bU7gRe",
    "Cw8CFyM9FkoMi7K7Crf6HNQqf4uEMzpKw6QNghXLvLkY",
    "ADaUMid9yfUytqMBgopwjb2DTLSokTSzL1zt6iGPaS49",
    "DfXygSm4jCyNCybVYYK6DwvWqjKee8pbDmJGcLWNDXjh",
    "ADuUkR4vqLUMWXxW9gh6D6L8pMSawimctcNZ5pGwDcEt",
    "DttWaMuVvTiduZRnguLF7jNxTgiMBZ1hyAumKUiL2KRL",
    "3AVi9Tg9Uo68tJfuvoKvqKNWKkC5wPdSSdeBnizKZ6jT",
];

#[derive(Debug, Clone)]
pub struct PoolInfo {
    pub amm_id: Pubkey,
    pub base_mint: Pubkey,
    pub quote_mint: Pubkey,
    pub base_vault: Pubkey,
    pub quote_vault: Pubkey,
    pub open_orders: Pubkey,
    pub target_orders: Pubkey,
    pub amm_authority: Pubkey,
    pub lp_mint: Pubkey,
    pub base_decimals: u8,
    pub quote_decimals: u8,
    pub base_reserve: u64,
    pub quote_reserve: u64,
    pub creation_time: u64,
}

#[derive(Debug, Clone, Copy)]
pub struct SwapConfig {
    pub amount_in: u64,
    pub min_amount_out: u64,
    pub slippage_bps: u64,
    pub priority_fee: u64,
    pub compute_units: u32,
}

pub struct FairLaunchSniper {
    rpc_client: Arc<RpcClient>,
    wallet: Arc<Keypair>,
    monitored_pools: Arc<RwLock<HashMap<Pubkey, PoolInfo>>>,
    blacklisted_tokens: Arc<RwLock<HashSet<Pubkey>>>,
    active_positions: Arc<RwLock<HashMap<Pubkey, u64>>>,
    jito_client: Option<Arc<RpcClient>>,
}

impl FairLaunchSniper {
    pub fn new(
        rpc_url: &str,
        wallet_keypair: Keypair,
        jito_url: Option<String>,
    ) -> Self {
        let rpc_client = Arc::new(RpcClient::new_with_commitment(
            rpc_url.to_string(),
            CommitmentConfig::confirmed(),
        ));

        let jito_client = jito_url.map(|url| {
            Arc::new(RpcClient::new_with_commitment(
                url,
                CommitmentConfig::confirmed(),
            ))
        });

        Self {
            rpc_client,
            wallet: Arc::new(wallet_keypair),
            monitored_pools: Arc::new(RwLock::new(HashMap::new())),
            blacklisted_tokens: Arc::new(RwLock::new(HashSet::new())),
            active_positions: Arc::new(RwLock::new(HashMap::new())),
            jito_client,
        }
    }

    pub async fn start(&self) -> Result<(), Box<dyn std::error::Error>> {
        let monitor_handle = self.spawn_pool_monitor();
        let sniper_handle = self.spawn_sniper_loop();
        
        tokio::select! {
            result = monitor_handle => result?,
            result = sniper_handle => result?,
        }
        
        Ok(())
    }

    fn spawn_pool_monitor(&self) -> tokio::task::JoinHandle<Result<(), Box<dyn std::error::Error + Send>>> {
        let rpc = self.rpc_client.clone();
        let monitored_pools = self.monitored_pools.clone();
        let blacklisted_tokens = self.blacklisted_tokens.clone();
        
        tokio::spawn(async move {
            let mut interval = interval(Duration::from_millis(100));
            let raydium_program = Pubkey::from_str(RAYDIUM_V4)?;
            
            loop {
                interval.tick().await;
                
                match Self::fetch_new_pools(&rpc, &raydium_program).await {
                    Ok(new_pools) => {
                        for pool in new_pools {
                            let blacklisted = blacklisted_tokens.read().await;
                            if !blacklisted.contains(&pool.base_mint) {
                                if Self::validate_pool(&pool).await {
                                    monitored_pools.write().await.insert(pool.amm_id, pool);
                                }
                            }
                        }
                    }
                    Err(e) => {
                        log::error!("Pool monitoring error: {:?}", e);
                        sleep(Duration::from_secs(1)).await;
                    }
                }
            }
        })
    }

    fn spawn_sniper_loop(&self) -> tokio::task::JoinHandle<Result<(), Box<dyn std::error::Error + Send>>> {
        let rpc = self.rpc_client.clone();
        let wallet = self.wallet.clone();
        let monitored_pools = self.monitored_pools.clone();
        let active_positions = self.active_positions.clone();
        let blacklisted_tokens = self.blacklisted_tokens.clone();
        let jito_client = self.jito_client.clone();
        
        tokio::spawn(async move {
            loop {
                let pools = monitored_pools.read().await.clone();
                
                for (amm_id, pool) in pools.iter() {
                    let positions = active_positions.read().await;
                    if positions.contains_key(&pool.base_mint) {
                        continue;
                    }
                    drop(positions);
                    
                    if Self::check_snipe_conditions(&pool).await {
                        match Self::execute_snipe(
                            &rpc,
                            &wallet,
                            &pool,
                            jito_client.as_ref(),
                        ).await {
                            Ok(signature) => {
                                log::info!("Snipe executed: {} for token {}", signature, pool.base_mint);
                                active_positions.write().await.insert(pool.base_mint, pool.quote_reserve);
                                monitored_pools.write().await.remove(amm_id);
                            }
                            Err(e) => {
                                log::error!("Snipe failed for {}: {:?}", pool.base_mint, e);
                                if Self::is_permanent_error(&e) {
                                    blacklisted_tokens.write().await.insert(pool.base_mint);
                                    monitored_pools.write().await.remove(amm_id);
                                }
                            }
                        }
                    }
                }
                
                sleep(Duration::from_millis(50)).await;
            }
        })
    }

    async fn fetch_new_pools(
        rpc: &RpcClient,
        program_id: &Pubkey,
    ) -> Result<Vec<PoolInfo>, Box<dyn std::error::Error>> {
        let accounts = rpc.get_program_accounts_with_config(
            program_id,
            solana_client::rpc_config::RpcProgramAccountsConfig {
                filters: Some(vec![
                    solana_client::rpc_filter::RpcFilterType::DataSize(752),
                ]),
                account_config: solana_client::rpc_config::RpcAccountInfoConfig {
                    encoding: Some(UiAccountEncoding::Base64),
                    commitment: Some(CommitmentConfig::confirmed()),
                    ..Default::default()
                },
                ..Default::default()
            },
        ).await?;
        
        let mut pools = Vec::new();
        let current_slot = rpc.get_slot().await?;
        
        for (pubkey, account) in accounts {
            if let Ok(pool_info) = Self::parse_pool_account(&pubkey, &account.data, current_slot) {
                pools.push(pool_info);
            }
        }
        
        Ok(pools)
    }

    fn parse_pool_account(
        amm_id: &Pubkey,
        data: &[u8],
        current_slot: u64,
    ) -> Result<PoolInfo, Box<dyn std::error::Error>> {
        let status = u64::from_le_bytes(data[0..8].try_into()?);
        if status != 1 {
            return Err("Pool not initialized".into());
        }
        
        let nonce = data[8];
        let max_order = u64::from_le_bytes(data[9..17].try_into()?);
        let depth = u64::from_le_bytes(data[17..25].try_into()?);
        let base_decimals = u64::from_le_bytes(data[25..33].try_into()?);
        let quote_decimals = u64::from_le_bytes(data[33..41].try_into()?);
        let state = u64::from_le_bytes(data[41..49].try_into()?);
        let reset_flag = u64::from_le_bytes(data[49..57].try_into()?);
        let min_size = u64::from_le_bytes(data[57..65].try_into()?);
        let vol_max_cut_ratio = u64::from_le_bytes(data[65..73].try_into()?);
        let amount_wave_ratio = u64::from_le_bytes(data[73..81].try_into()?);
        let base_lot_size = u64::from_le_bytes(data[81..89].try_into()?);
        let quote_lot_size = u64::from_le_bytes(data[89..97].try_into()?);
        let min_price_multiplier = u64::from_le_bytes(data[97..105].try_into()?);
        let max_price_multiplier = u64::from_le_bytes(data[105..113].try_into()?);
        let system_decimal_value = u64::from_le_bytes(data[113..121].try_into()?);
        let min_separate_numerator = u64::from_le_bytes(data[121..129].try_into()?);
        let min_separate_denominator = u64::from_le_bytes(data[129..137].try_into()?);
        let trade_fee_numerator = u64::from_le_bytes(data[137..145].try_into()?);
        let trade_fee_denominator = u64::from_le_bytes(data[145..153].try_into()?);
        let pnl_numerator = u64::from_le_bytes(data[153..161].try_into()?);
        let pnl_denominator = u64::from_le_bytes(data[161..169].try_into()?);
        let swap_fee_numerator = u64::from_le_bytes(data[169..177].try_into()?);
        let swap_fee_denominator = u64::from_le_bytes(data[177..185].try_into()?);
                let base_need_take_pnl = u64::from_le_bytes(data[185..193].try_into()?);
        let quote_need_take_pnl = u64::from_le_bytes(data[193..201].try_into()?);
        let quote_total_pnl = u64::from_le_bytes(data[201..209].try_into()?);
        let base_total_pnl = u64::from_le_bytes(data[209..217].try_into()?);
        let pool_open_time = u64::from_le_bytes(data[217..225].try_into()?);
        let punish_pc_amount = u64::from_le_bytes(data[225..233].try_into()?);
        let punish_coin_amount = u64::from_le_bytes(data[233..241].try_into()?);
        let ordebook_to_init_time = u64::from_le_bytes(data[241..249].try_into()?);
        let swap_base_in_amount = u128::from_le_bytes(data[249..265].try_into()?);
        let swap_quote_out_amount = u128::from_le_bytes(data[265..281].try_into()?);
        let swap_base2quote_fee = u64::from_le_bytes(data[281..289].try_into()?);
        let swap_quote_in_amount = u128::from_le_bytes(data[289..305].try_into()?);
        let swap_base_out_amount = u128::from_le_bytes(data[305..321].try_into()?);
        let swap_quote2base_fee = u64::from_le_bytes(data[321..329].try_into()?);
        let base_vault = Pubkey::new_from_array(data[329..361].try_into()?);
        let quote_vault = Pubkey::new_from_array(data[361..393].try_into()?);
        let base_mint = Pubkey::new_from_array(data[393..425].try_into()?);
        let quote_mint = Pubkey::new_from_array(data[425..457].try_into()?);
        let lp_mint = Pubkey::new_from_array(data[457..489].try_into()?);
        let open_orders = Pubkey::new_from_array(data[489..521].try_into()?);
        let market_id = Pubkey::new_from_array(data[521..553].try_into()?);
        let market_program_id = Pubkey::new_from_array(data[553..585].try_into()?);
        let target_orders = Pubkey::new_from_array(data[585..617].try_into()?);
        let withdraw_queue = Pubkey::new_from_array(data[617..649].try_into()?);
        let lp_vault = Pubkey::new_from_array(data[649..681].try_into()?);
        let amm_owner = Pubkey::new_from_array(data[681..713].try_into()?);
        let base_reserve = u64::from_le_bytes(data[713..721].try_into()?);
        let quote_reserve = u64::from_le_bytes(data[721..729].try_into()?);

        let (amm_authority, _) = Pubkey::find_program_address(
            &[
                &amm_id.to_bytes(),
                &[nonce],
            ],
            &Pubkey::from_str(RAYDIUM_V4)?,
        );

        Ok(PoolInfo {
            amm_id: *amm_id,
            base_mint,
            quote_mint,
            base_vault,
            quote_vault,
            open_orders,
            target_orders,
            amm_authority,
            lp_mint,
            base_decimals: base_decimals as u8,
            quote_decimals: quote_decimals as u8,
            base_reserve,
            quote_reserve,
            creation_time: pool_open_time,
        })
    }

    async fn validate_pool(pool: &PoolInfo) -> bool {
        if pool.quote_mint != Pubkey::from_str(WSOL_MINT).unwrap() {
            return false;
        }
        
        if pool.quote_reserve < MIN_LIQUIDITY_SOL {
            return false;
        }
        
        if pool.base_reserve == 0 || pool.quote_reserve == 0 {
            return false;
        }
        
        let price = (pool.quote_reserve as f64) / (pool.base_reserve as f64) 
            * 10f64.powi(pool.base_decimals as i32 - pool.quote_decimals as i32);
        
        if price < 0.0000001 || price > 1000.0 {
            return false;
        }
        
        true
    }

    async fn check_snipe_conditions(pool: &PoolInfo) -> bool {
        let current_time = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        
        let pool_age = current_time - pool.creation_time;
        if pool_age > 300 {
            return false;
        }
        
        if pool.quote_reserve < MIN_LIQUIDITY_SOL {
            return false;
        }
        
        let price = (pool.quote_reserve as f64) / (pool.base_reserve as f64);
        if price < 0.0000001 {
            return false;
        }
        
        true
    }

    async fn execute_snipe(
        rpc: &RpcClient,
        wallet: &Keypair,
        pool: &PoolInfo,
        jito_client: Option<&Arc<RpcClient>>,
    ) -> Result<Signature, Box<dyn std::error::Error>> {
        let amount_in = std::cmp::min(
            pool.quote_reserve / 10,
            MAX_BUY_SOL,
        );
        
        let amount_out = Self::calculate_amount_out(
            amount_in,
            pool.quote_reserve,
            pool.base_reserve,
            pool.base_decimals,
            pool.quote_decimals,
        );
        
        let min_amount_out = amount_out * (10000 - SLIPPAGE_BPS) / 10000;
        
        let config = SwapConfig {
            amount_in,
            min_amount_out,
            slippage_bps: SLIPPAGE_BPS,
            priority_fee: Self::calculate_priority_fee(pool),
            compute_units: 400_000,
        };
        
        let mut retries = 0;
        loop {
            match Self::build_and_send_swap(
                rpc,
                wallet,
                pool,
                &config,
                jito_client,
            ).await {
                Ok(sig) => return Ok(sig),
                Err(e) => {
                    retries += 1;
                    if retries >= MAX_RETRIES || Self::is_permanent_error(&e) {
                        return Err(e);
                    }
                    sleep(Duration::from_millis(100)).await;
                }
            }
        }
    }

    fn calculate_amount_out(
        amount_in: u64,
        quote_reserve: u64,
        base_reserve: u64,
        base_decimals: u8,
        quote_decimals: u8,
    ) -> u64 {
        let invariant = (quote_reserve as u128) * (base_reserve as u128);
        let new_quote_reserve = quote_reserve + amount_in;
        let new_base_reserve = invariant / (new_quote_reserve as u128);
        let amount_out = base_reserve - (new_base_reserve as u64);
        let fee = amount_out * 25 / 10000;
        amount_out - fee
    }

    fn calculate_priority_fee(pool: &PoolInfo) -> u64 {
        let base_fee = if pool.quote_reserve > 50 * LAMPORTS_PER_SOL {
            BASE_PRIORITY_FEE * 10
        } else if pool.quote_reserve > 20 * LAMPORTS_PER_SOL {
            BASE_PRIORITY_FEE * 5
        } else {
            BASE_PRIORITY_FEE * 2
        };
        
        std::cmp::min(base_fee, MAX_PRIORITY_FEE)
    }

    async fn build_and_send_swap(
        rpc: &RpcClient,
        wallet: &Keypair,
        pool: &PoolInfo,
        config: &SwapConfig,
        jito_client: Option<&Arc<RpcClient>>,
    ) -> Result<Signature, Box<dyn std::error::Error>> {
        let user_base_ata = get_associated_token_address(
            &wallet.pubkey(),
            &pool.base_mint,
        );
        
        let user_quote_ata = get_associated_token_address(
            &wallet.pubkey(),
            &pool.quote_mint,
        );
        
        let mut instructions = vec![];
        
        instructions.push(ComputeBudgetInstruction::set_compute_unit_limit(config.compute_units));
        instructions.push(ComputeBudgetInstruction::set_compute_unit_price(config.priority_fee));
        
        let base_ata_info = rpc.get_account(&user_base_ata).await;
        if base_ata_info.is_err() {
            instructions.push(
                spl_associated_token_account::instruction::create_associated_token_account(
                    &wallet.pubkey(),
                    &wallet.pubkey(),
                    &pool.base_mint,
                    &Pubkey::from_str(TOKEN_PROGRAM)?,
                )
            );
        }
        
        let swap_instruction = Instruction {
            program_id: Pubkey::from_str(RAYDIUM_V4)?,
            accounts: vec![
                AccountMeta::new_readonly(Pubkey::from_str(TOKEN_PROGRAM)?, false),
                AccountMeta::new(pool.amm_id, false),
                AccountMeta::new_readonly(pool.amm_authority, false),
                AccountMeta::new(pool.open_orders, false),
                AccountMeta::new(pool.target_orders, false),
                AccountMeta::new(pool.base_vault, false),
                AccountMeta::new(pool.quote_vault, false),
                AccountMeta::new_readonly(Pubkey::from_str("srmqPvymJeFKQ4zGQed1GFppgkRHL9kaELCbyksJtPX")?, false),
                AccountMeta::new_readonly(Pubkey::from_str("srmqPvymJeFKQ4zGQed1GFppgkRHL9kaELCbyksJtPX")?, false),
                AccountMeta::new(user_quote_ata, false),
                AccountMeta::new(user_base_ata, false),
                AccountMeta::new_readonly(wallet.pubkey(), true),
            ],
            data: Self::build_swap_instruction_data(config.amount_in, config.min_amount_out),
        };
        
        instructions.push(swap_instruction);
        
        if let Some(jito) = jito_client {
            let tip_account = Self::get_random_tip_account();
            instructions.push(
                solana_sdk::system_instruction::transfer(
                    &wallet.pubkey(),
                    &tip_account,
                    config.priority_fee * 10,
                )
            );
        }
        
        let recent_blockhash = rpc.get_latest_blockhash().await?;
        let transaction = Transaction::new_signed_with_payer(
            &instructions,
            Some(&wallet.pubkey()),
            &[wallet],
            recent_blockhash,
        );
        
        let client = jito_client.unwrap_or(rpc);
        let signature = client.send_transaction_with_config(
            &transaction,
            RpcSendTransactionConfig {
                skip_preflight: true,
                preflight_commitment: Some(CommitmentConfig::processed()),
                max_retries: Some(0),
                ..Default::default()
            },
        ).await?;
        
        Ok(signature)
    }

    fn build_swap_instruction_data(amount_in: u64, min_amount_out: u64) -> Vec<u8> {
        let mut data = vec![9];
        data.extend_from_slice(&amount_in.to_le_bytes());
        data.extend_from_slice(&min_amount_out.to_le_bytes());
        data
    }

    fn get_random_tip_account() -> Pubkey {
        let index = (std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos() as usize) % JITO_TIP_ACCOUNTS.len();
        Pubkey::from_str(JITO_TIP_ACCOUNTS[index]).unwrap()
    }

    fn is_permanent_error(error: &Box<dyn std::error::Error>) -> bool {
        let error_str = error.to_string();
        error_str.contains("insufficient funds") ||
        error_str.contains("already in use") ||
        error_str.contains("account not found") ||
        error_str.contains("invalid account data") ||
        error_str.contains("0x1")
    }

    pub async fn get_balance(&self, mint: &Pubkey) -> Result<u64, Box<dyn std::error::Error>> {
        let ata = get_associated_token_address(&self.wallet.pubkey(), mint);
        let account = self.rpc_client.get_account(&ata).await?;
        let token_account = TokenAccount::unpack(&account.data)?;
        Ok(token_account.amount)
    }

    pub async fn emergency_exit(&self, token_mint: &Pubkey) -> Result<(), Box<dyn std::error::Error>> {
        self.blacklisted_tokens.write().await.insert(*token_mint);
        self.active_positions.write().await.remove(token_mint);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_amount_out_calculation() {
        let amount_in = LAMPORTS_PER_SOL;
        let quote_reserve = 100 * LAMPORTS_PER_SOL;
        let base_reserve = 1_000_000 * 10u64.pow(9);
        let result = FairLaunchSniper::calculate_amount_out(
            amount_in,
            quote_reserve,
            base_reserve,
            9,
            9,
        );
        assert!(result > 0);
        assert!(result < base_reserve);
    }

        #[test]
    fn test_priority_fee_calculation() {
        let pool = PoolInfo {
            amm_id: Pubkey::new_unique(),
            base_mint: Pubkey::new_unique(),
            quote_mint: Pubkey::from_str(WSOL_MINT).unwrap(),
            base_vault: Pubkey::new_unique(),
            quote_vault: Pubkey::new_unique(),
            open_orders: Pubkey::new_unique(),
            target_orders: Pubkey::new_unique(),
            amm_authority: Pubkey::new_unique(),
            lp_mint: Pubkey::new_unique(),
            base_decimals: 9,
            quote_decimals: 9,
            base_reserve: 1_000_000 * 10u64.pow(9),
            quote_reserve: 100 * LAMPORTS_PER_SOL,
            creation_time: 0,
        };
        
        let fee = FairLaunchSniper::calculate_priority_fee(&pool);
        assert!(fee >= BASE_PRIORITY_FEE);
        assert!(fee <= MAX_PRIORITY_FEE);
    }

    #[test]
    fn test_swap_data_serialization() {
        let data = FairLaunchSniper::build_swap_instruction_data(
            LAMPORTS_PER_SOL,
            1_000_000 * 10u64.pow(9),
        );
        assert_eq!(data[0], 9);
        assert_eq!(data.len(), 17);
    }
}

