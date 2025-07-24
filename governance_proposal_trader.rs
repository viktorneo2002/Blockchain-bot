use solana_client::rpc_client::RpcClient;
use solana_sdk::{
    commitment_config::CommitmentConfig,
    compute_budget::ComputeBudgetInstruction,
    instruction::Instruction,
    pubkey::Pubkey,
    signature::{Keypair, Signer},
    transaction::Transaction,
};
use spl_governance::{
    state::{
        enums::{ProposalState, VoteThreshold, VoteThresholdType},
        governance::GovernanceV2,
        proposal::{ProposalV2, VoteType},
        realm::RealmV2,
        vote_record::VoteRecordV2,
    },
};
use anchor_lang::prelude::*;
use pyth_sdk_solana::state::SolanaPriceAccount;
use switchboard_v2::{AggregatorAccountData, SWITCHBOARD_PROGRAM_ID};
use jupiter_amm_interface::{
    amm::{Amm, KeyedAccount},
    SwapParams,
};
use std::{
    collections::{HashMap, HashSet},
    str::FromStr,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};
use tokio::time::sleep;

const GOVERNANCE_PROGRAM_ID: &str = "GovER5Lthms3bLBqWub97yVrMmEogzX7xNjdXpPPCVZw";
const TOKEN_PROGRAM_ID: &str = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA";
const JUPITER_V6_PROGRAM: &str = "JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4";
const PYTH_ORACLE_PROGRAM: &str = "FsJ3A3u2vn5cTVofAjvy6y5kwABJAqYWpe4975bi2epH";
const COMPUTE_UNITS: u32 = 1_400_000;
const PRIORITY_FEE_LAMPORTS: u64 = 50_000;
const MAX_RETRIES: u8 = 3;
const PROPOSAL_SCAN_INTERVAL_MS: u64 = 500;
const MIN_PROFIT_THRESHOLD_USD: f64 = 100.0;
const MAX_SLIPPAGE_BPS: u16 = 50;
const POSITION_SIZE_PERCENTAGE: f64 = 0.15;

#[derive(Debug, Clone)]
pub struct ProposalMetadata {
    pub proposal_pubkey: Pubkey,
    pub realm_pubkey: Pubkey,
    pub governance_pubkey: Pubkey,
    pub governing_token_mint: Pubkey,
    pub state: ProposalState,
    pub yes_votes: u64,
    pub no_votes: u64,
    pub vote_threshold: VoteThreshold,
    pub max_voting_time: u32,
    pub voting_at: Option<i64>,
    pub voting_completed_at: Option<i64>,
    pub executing_at: Option<i64>,
    pub closed_at: Option<i64>,
    pub instructions_count: u32,
    pub description_link: String,
    pub name: String,
    pub price_impact_estimate: f64,
    pub last_update: Instant,
}

#[derive(Debug, Clone)]
pub struct TradingSignal {
    pub signal_type: SignalType,
    pub confidence: f64,
    pub expected_price_change: f64,
    pub time_to_event: Option<Duration>,
    pub proposal: ProposalMetadata,
}

#[derive(Debug, Clone, PartialEq)]
pub enum SignalType {
    ProposalCreated,
    VotingStarted,
    VoteSwing,
    ProposalPassing,
    ProposalExecuting,
    ProposalCompleted,
}

pub struct GovernanceProposalTrader {
    rpc_client: Arc<RpcClient>,
    keypair: Arc<Keypair>,
    tracked_realms: HashMap<Pubkey, RealmConfig>,
    active_proposals: Arc<Mutex<HashMap<Pubkey, ProposalMetadata>>>,
    positions: Arc<Mutex<HashMap<Pubkey, Position>>>,
    price_feeds: Arc<Mutex<HashMap<Pubkey, PriceFeed>>>,
    oracle_mappings: HashMap<Pubkey, OracleInfo>,
}

#[derive(Debug, Clone)]
struct RealmConfig {
    pub realm_pubkey: Pubkey,
    pub community_mint: Pubkey,
    pub council_mint: Option<Pubkey>,
    pub impact_multiplier: f64,
    pub min_vote_weight: u64,
}

#[derive(Debug, Clone)]
struct Position {
    pub token_mint: Pubkey,
    pub amount: u64,
    pub entry_price: f64,
    pub target_price: f64,
    pub stop_loss: f64,
    pub proposal_pubkey: Pubkey,
    pub opened_at: Instant,
}

#[derive(Debug, Clone)]
struct PriceFeed {
    pub price: f64,
    pub volume_24h: f64,
    pub market_cap: f64,
    pub last_update: Instant,
}

#[derive(Debug, Clone)]
struct OracleInfo {
    pub pyth_price_account: Option<Pubkey>,
    pub switchboard_feed: Option<Pubkey>,
    pub oracle_type: OracleType,
}

#[derive(Debug, Clone)]
enum OracleType {
    Pyth,
    Switchboard,
    Both,
}

impl GovernanceProposalTrader {
    pub fn new(keypair: Keypair, rpc_url: &str) -> Self {
        let rpc_client = Arc::new(RpcClient::new_with_commitment(
            rpc_url.to_string(),
            CommitmentConfig::confirmed(),
        ));

        let mut tracked_realms = HashMap::new();
        tracked_realms.insert(
            Pubkey::from_str("5RpgWyGiGBPzMxjPri3mZQGLDhP3yzb6h8dB3PjTpGa3").unwrap(),
            RealmConfig {
                realm_pubkey: Pubkey::from_str("5RpgWyGiGBPzMxjPri3mZQGLDhP3yzb6h8dB3PjTpGa3").unwrap(),
                community_mint: Pubkey::from_str("MangoCzJ36AjZyKwVj3VnYU4GTonjfVEnJmvvWaxLac").unwrap(),
                council_mint: None,
                impact_multiplier: 2.5,
                min_vote_weight: 1_000_000_000_000,
            },
        );

        let mut oracle_mappings = HashMap::new();
        oracle_mappings.insert(
            Pubkey::from_str("So11111111111111111111111111111111111111112").unwrap(),
            OracleInfo {
                pyth_price_account: Some(Pubkey::from_str("H6ARHf6YXhGYeQfUzQNGk6rDNnLBQKrenN712K4AQJEG").unwrap()),
                switchboard_feed: Some(Pubkey::from_str("GvDMxPzN1sCj7L26YDK2HnMRXEQmQ2aemov8YBtPS7vR").unwrap()),
                oracle_type: OracleType::Both,
            },
        );
        oracle_mappings.insert(
            Pubkey::from_str("EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v").unwrap(),
            OracleInfo {
                pyth_price_account: Some(Pubkey::from_str("Gnt27xtC473ZT2Mw5u8wZ68Z3gULkSTb5DuxJy7eJotD").unwrap()),
                switchboard_feed: Some(Pubkey::from_str("BjUgj6YCnFBZ49wF54ddBVA9qu8TeqkFtkbqmZcee8uW").unwrap()),
                oracle_type: OracleType::Both,
            },
        );

        Self {
            rpc_client,
            keypair: Arc::new(keypair),
            tracked_realms,
            active_proposals: Arc::new(Mutex::new(HashMap::new())),
            positions: Arc::new(Mutex::new(HashMap::new())),
            price_feeds: Arc::new(Mutex::new(HashMap::new())),
            oracle_mappings,
        }
    }

    pub async fn run(&self) -> Result<(), Box<dyn std::error::Error>> {
        let scan_handle = self.start_proposal_scanner();
        let trade_handle = self.start_trade_executor();
        let monitor_handle = self.start_position_monitor();

        tokio::try_join!(scan_handle, trade_handle, monitor_handle)?;
        Ok(())
    }

    async fn start_proposal_scanner(&self) -> Result<(), Box<dyn std::error::Error>> {
        let rpc = self.rpc_client.clone();
        let tracked_realms = self.tracked_realms.clone();
        let active_proposals = self.active_proposals.clone();

        tokio::spawn(async move {
            loop {
                for (realm_pubkey, realm_config) in tracked_realms.iter() {
                    if let Err(e) = Self::scan_realm_proposals(
                        &rpc,
                        realm_pubkey,
                        realm_config,
                        &active_proposals,
                    ).await {
                        eprintln!("Error scanning realm {}: {:?}", realm_pubkey, e);
                    }
                }
                sleep(Duration::from_millis(PROPOSAL_SCAN_INTERVAL_MS)).await;
            }
        });

        Ok(())
    }

    async fn scan_realm_proposals(
        rpc: &Arc<RpcClient>,
        realm_pubkey: &Pubkey,
        realm_config: &RealmConfig,
        active_proposals: &Arc<Mutex<HashMap<Pubkey, ProposalMetadata>>>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let governance_program = Pubkey::from_str(GOVERNANCE_PROGRAM_ID)?;
        
        let accounts = rpc.get_program_accounts_with_config(
            &governance_program,
            solana_client::rpc_config::RpcProgramAccountsConfig {
                filters: Some(vec![
                    solana_client::rpc_filter::RpcFilterType::Memcmp(
                        solana_client::rpc_filter::Memcmp::new_base58_encoded(0, &[3]),
                    ),
                ]),
                account_config: solana_client::rpc_config::RpcAccountInfoConfig {
                    encoding: Some(solana_account_decoder::UiAccountEncoding::Base64),
                    commitment: Some(CommitmentConfig::confirmed()),
                    ..Default::default()
                },
                ..Default::default()
            },
        )?;

        for (proposal_pubkey, account) in accounts {
            if let Ok(proposal) = ProposalV2::deserialize(&mut account.data.as_slice()) {
                let metadata = Self::build_proposal_metadata(
                    &proposal_pubkey,
                    &proposal,
                    realm_pubkey,
                    &realm_config.community_mint,
                );

                if Self::should_track_proposal(&metadata, realm_config) {
                    active_proposals.lock().unwrap().insert(proposal_pubkey, metadata);
                }
            }
        }

        Ok(())
    }

    fn build_proposal_metadata(
        proposal_pubkey: &Pubkey,
        proposal: &ProposalV2,
        realm_pubkey: &Pubkey,
        governing_token_mint: &Pubkey,
    ) -> ProposalMetadata {
        ProposalMetadata {
            proposal_pubkey: *proposal_pubkey,
            realm_pubkey: *realm_pubkey,
            governance_pubkey: proposal.governance,
            governing_token_mint: *governing_token_mint,
            state: proposal.state,
            yes_votes: proposal.options[0].vote_weight,
            no_votes: proposal.deny_vote_weight.unwrap_or(0),
            vote_threshold: proposal.vote_threshold.clone().unwrap_or(VoteThreshold {
                threshold_type: VoteThresholdType::YesVotePercentage(60),
            }),
            max_voting_time: proposal.max_voting_time,
            voting_at: proposal.voting_at,
            voting_completed_at: proposal.voting_completed_at,
            executing_at: proposal.executing_at,
            closed_at: proposal.closed_at,
            instructions_count: proposal.instructions_count,
            description_link: proposal.description_link.clone(),
            name: proposal.name.clone(),
            price_impact_estimate: Self::estimate_price_impact(&proposal),
            last_update: Instant::now(),
        }
    }

    fn should_track_proposal(metadata: &ProposalMetadata, realm_config: &RealmConfig) -> bool {
        matches!(
            metadata.state,
            ProposalState::Draft |
            ProposalState::SigningOff |
            ProposalState::Voting |
            ProposalState::Succeeded |
            ProposalState::Executing
        ) && (metadata.yes_votes + metadata.no_votes) >= realm_config.min_vote_weight
    }

    fn estimate_price_impact(proposal: &ProposalV2) -> f64 {
        let base_impact = match proposal.state {
            ProposalState::Draft => 0.001,
            ProposalState::SigningOff => 0.002,
            ProposalState::Voting => 0.005,
            ProposalState::Succeeded => 0.015,
            ProposalState::Executing => 0.025,
            ProposalState::Completed => 0.01,
            _ => 0.0,
        };

        let vote_weight_factor = if proposal.options[0].vote_weight > 0 {
            let total_votes = proposal.options[0].vote_weight + proposal.deny_vote_weight.unwrap_or(0);
            (proposal.options[0].vote_weight as f64 / total_votes as f64) * 2.0
        } else {
            1.0
        };

                let instruction_factor = 1.0 + (proposal.instructions_count as f64 * 0.002);
        base_impact * vote_weight_factor * instruction_factor
    }

    async fn start_trade_executor(&self) -> Result<(), Box<dyn std::error::Error>> {
        let active_proposals = self.active_proposals.clone();
        let positions = self.positions.clone();
        let price_feeds = self.price_feeds.clone();
        let rpc = self.rpc_client.clone();
        let keypair = self.keypair.clone();

        tokio::spawn(async move {
            loop {
                let proposals = active_proposals.lock().unwrap().clone();
                
                for (_, proposal) in proposals.iter() {
                    if let Some(signal) = Self::analyze_proposal_for_signal(proposal, &price_feeds) {
                        if signal.confidence > 0.7 {
                            if let Err(e) = Self::execute_trade(&rpc, &keypair, &signal, &positions).await {
                                eprintln!("Trade execution error: {:?}", e);
                            }
                        }
                    }
                }
                
                sleep(Duration::from_millis(1000)).await;
            }
        });

        Ok(())
    }

    fn analyze_proposal_for_signal(
        proposal: &ProposalMetadata,
        price_feeds: &Arc<Mutex<HashMap<Pubkey, PriceFeed>>>,
    ) -> Option<TradingSignal> {
        let price_feed = price_feeds.lock().unwrap();
        
        let signal_type = match proposal.state {
            ProposalState::Voting => {
                let total_votes = proposal.yes_votes + proposal.no_votes;
                if total_votes > 0 {
                    let yes_ratio = proposal.yes_votes as f64 / total_votes as f64;
                    if yes_ratio > 0.8 && proposal.yes_votes > 10_000_000_000_000 {
                        SignalType::ProposalPassing
                    } else if yes_ratio < 0.2 && total_votes > 5_000_000_000_000 {
                        return None;
                    } else {
                        SignalType::VoteSwing
                    }
                } else {
                    SignalType::VotingStarted
                }
            }
            ProposalState::Succeeded => SignalType::ProposalExecuting,
            ProposalState::Executing => SignalType::ProposalExecuting,
            ProposalState::Completed => SignalType::ProposalCompleted,
            ProposalState::Draft | ProposalState::SigningOff => SignalType::ProposalCreated,
            _ => return None,
        };

        let confidence = Self::calculate_signal_confidence(proposal, &signal_type);
        let expected_price_change = Self::calculate_expected_price_change(proposal, &signal_type);
        
        let time_to_event = match signal_type {
            SignalType::ProposalPassing => {
                if let Some(voting_at) = proposal.voting_at {
                    let voting_end = voting_at + proposal.max_voting_time as i64;
                    let now = chrono::Utc::now().timestamp();
                    if voting_end > now {
                        Some(Duration::from_secs((voting_end - now) as u64))
                    } else {
                        None
                    }
                } else {
                    None
                }
            }
            _ => None,
        };

        Some(TradingSignal {
            signal_type,
            confidence,
            expected_price_change,
            time_to_event,
            proposal: proposal.clone(),
        })
    }

    fn calculate_signal_confidence(proposal: &ProposalMetadata, signal_type: &SignalType) -> f64 {
        let base_confidence = match signal_type {
            SignalType::ProposalCreated => 0.3,
            SignalType::VotingStarted => 0.4,
            SignalType::VoteSwing => 0.5,
            SignalType::ProposalPassing => 0.8,
            SignalType::ProposalExecuting => 0.9,
            SignalType::ProposalCompleted => 0.7,
        };

        let vote_weight_factor = if proposal.yes_votes + proposal.no_votes > 0 {
            let total_votes = proposal.yes_votes + proposal.no_votes;
            (total_votes as f64 / 100_000_000_000_000.0).min(1.5)
        } else {
            0.5
        };

        let time_factor = if proposal.last_update.elapsed() < Duration::from_secs(60) {
            1.2
        } else if proposal.last_update.elapsed() < Duration::from_secs(300) {
            1.0
        } else {
            0.8
        };

        (base_confidence * vote_weight_factor * time_factor).min(0.95)
    }

    fn calculate_expected_price_change(
        proposal: &ProposalMetadata,
        signal_type: &SignalType,
    ) -> f64 {
        let base_change = match signal_type {
            SignalType::ProposalCreated => 0.005,
            SignalType::VotingStarted => 0.01,
            SignalType::VoteSwing => 0.015,
            SignalType::ProposalPassing => 0.03,
            SignalType::ProposalExecuting => 0.04,
            SignalType::ProposalCompleted => -0.01,
        };

        let impact_multiplier = proposal.price_impact_estimate;
        let vote_ratio = if proposal.yes_votes + proposal.no_votes > 0 {
            proposal.yes_votes as f64 / (proposal.yes_votes + proposal.no_votes) as f64
        } else {
            0.5
        };

        base_change * impact_multiplier * (0.5 + vote_ratio)
    }

    async fn execute_trade(
        rpc: &Arc<RpcClient>,
        keypair: &Arc<Keypair>,
        signal: &TradingSignal,
        positions: &Arc<Mutex<HashMap<Pubkey, Position>>>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let token_mint = signal.proposal.governing_token_mint;
        
        if positions.lock().unwrap().contains_key(&signal.proposal.proposal_pubkey) {
            return Ok(());
        }

        let wallet_balance = Self::get_wallet_balance(rpc, &keypair.pubkey()).await?;
        let position_size = (wallet_balance as f64 * POSITION_SIZE_PERCENTAGE) as u64;
        
        if position_size < 1_000_000 {
            return Ok(());
        }

        let current_price = Self::get_token_price(rpc, &token_mint, &self.oracle_mappings).await?;
        let expected_profit = position_size as f64 * signal.expected_price_change * current_price;
        
        if expected_profit < MIN_PROFIT_THRESHOLD_USD {
            return Ok(());
        }

        let instructions = Self::build_trade_instructions(
            &keypair.pubkey(),
            &token_mint,
            position_size,
            signal.expected_price_change > 0.0,
        ).await?;

        let mut retries = 0;
        while retries < MAX_RETRIES {
            match Self::send_transaction(rpc, keypair, instructions.clone()).await {
                Ok(signature) => {
                    println!("Trade executed: {} for proposal {}", signature, signal.proposal.proposal_pubkey);
                    
                    let position = Position {
                        token_mint,
                        amount: position_size,
                        entry_price: current_price,
                        target_price: current_price * (1.0 + signal.expected_price_change),
                        stop_loss: current_price * 0.95,
                        proposal_pubkey: signal.proposal.proposal_pubkey,
                        opened_at: Instant::now(),
                    };
                    
                    positions.lock().unwrap().insert(signal.proposal.proposal_pubkey, position);
                    return Ok(());
                }
                Err(e) => {
                    eprintln!("Transaction failed, retry {}: {:?}", retries, e);
                    retries += 1;
                    sleep(Duration::from_millis(100)).await;
                }
            }
        }

        Err("Max retries exceeded".into())
    }

    async fn get_wallet_balance(
        rpc: &Arc<RpcClient>,
        wallet: &Pubkey,
    ) -> Result<u64, Box<dyn std::error::Error>> {
        Ok(rpc.get_balance(wallet)?)
    }

    async fn get_token_price(
        rpc: &Arc<RpcClient>,
        token_mint: &Pubkey,
        oracle_mappings: &HashMap<Pubkey, OracleInfo>,
    ) -> Result<f64, Box<dyn std::error::Error>> {
        if let Some(oracle_info) = oracle_mappings.get(token_mint) {
            // Try Pyth first
            if let Some(pyth_account) = &oracle_info.pyth_price_account {
                if let Ok(account_data) = rpc.get_account_data(pyth_account) {
                    if let Ok(price_account) = SolanaPriceAccount::account_decoder(&account_data) {
                        let price_data = price_account.to_price_feed(&token_mint.to_string());
                        let price = price_data.get_price_unchecked();
                        if price.price > 0 {
                            return Ok(price.price as f64 * 10f64.powi(price.expo));
                        }
                    }
                }
            }

            // Try Switchboard
            if let Some(switchboard_feed) = &oracle_info.switchboard_feed {
                if let Ok(account_data) = rpc.get_account_data(switchboard_feed) {
                    if account_data.len() >= std::mem::size_of::<AggregatorAccountData>() {
                        let aggregator: &AggregatorAccountData = bytemuck::from_bytes(&account_data[8..std::mem::size_of::<AggregatorAccountData>() + 8]);
                        let result = aggregator.get_result().unwrap_or_default();
                        if result.mantissa != 0 {
                            return Ok(result.mantissa as f64 * 10f64.powi(-(result.scale as i32)));
                        }
                    }
                }
            }
        }

        // Fallback to pool estimation
        Self::estimate_price_from_pools(rpc, token_mint).await
    }

    async fn estimate_price_from_pools(
        rpc: &Arc<RpcClient>,
        token_mint: &Pubkey,
    ) -> Result<f64, Box<dyn std::error::Error>> {
        let raydium_program = Pubkey::from_str("675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8")?;
        let usdc_mint = Pubkey::from_str("EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v")?;
        
        let (pool_pda, _) = Pubkey::find_program_address(
            &[
                b"amm",
                raydium_program.as_ref(),
                token_mint.as_ref(),
                usdc_mint.as_ref(),
            ],
            &raydium_program,
        );

        if let Ok(pool_data) = rpc.get_account_data(&pool_pda) {
            if pool_data.len() >= 752 {
                let pc_vault_amount = u64::from_le_bytes(pool_data[144..152].try_into()?);
                let coin_vault_amount = u64::from_le_bytes(pool_data[152..160].try_into()?);
                
                if coin_vault_amount > 0 && pc_vault_amount > 0 {
                    let token_decimals = rpc.get_token_supply(token_mint)?.decimals;
                    let usdc_decimals = 6;
                    let price = (pc_vault_amount as f64 / 10f64.powi(usdc_decimals as i32)) /
                               (coin_vault_amount as f64 / 10f64.powi(token_decimals as i32));
                    return Ok(price);
                }
            }
        }

        Ok(0.001)
    }

    async fn build_trade_instructions(
        wallet: &Pubkey,
        token_mint: &Pubkey,
        amount: u64,
        is_buy: bool,
    ) -> Result<Vec<Instruction>, Box<dyn std::error::Error>> {
        let mut instructions = vec![];
        
        instructions.push(ComputeBudgetInstruction::set_compute_unit_limit(COMPUTE_UNITS));
        instructions.push(ComputeBudgetInstruction::set_compute_unit_price(PRIORITY_FEE_LAMPORTS));

        let jupiter_program = Pubkey::from_str(JUPITER_V6_PROGRAM)?;
        let usdc_mint = Pubkey::from_str("EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v")?;
        
        let (input_mint, output_mint) = if is_buy {
            (usdc_mint, *token_mint)
        } else {
            (*token_mint, usdc_mint)
        };

        let user_input_ata = spl_associated_token_account::get_associated_token_address(wallet, &input_mint);
        let user_output_ata = spl_associated_token_account::get_associated_token_address(wallet, &output_mint);
        let (authority, _) = Pubkey::find_program_address(&[b"authority"], &jupiter_program);

        instructions.push(
            spl_associated_token_account::instruction::create_associated_token_account_idempotent(
                wallet,
                wallet,
                &output_mint,
                &spl_token::id(),
            )
        );

        // Jupiter V6 SharedAccountsRoute instruction
        let mut data = vec![229, 23, 203, 151, 122, 227, 173, 42]; // Discriminator
                data.extend_from_slice(&1u8.to_le_bytes()); // routePlanOpt
        data.extend_from_slice(&amount.to_le_bytes()); // inAmount
        data.extend_from_slice(&((amount * (10000 - MAX_SLIPPAGE_BPS) as u64) / 10000).to_le_bytes()); // quotedOutAmount
        data.extend_from_slice(&MAX_SLIPPAGE_BPS.to_le_bytes()); // slippageBps
        data.extend_from_slice(&[0u8; 32]); // platformFeeBps (none)

        let accounts = vec![
            AccountMeta::new_readonly(spl_token::id(), false),
            AccountMeta::new(*wallet, true),
            AccountMeta::new_readonly(authority, false),
            AccountMeta::new(user_input_ata, false),
            AccountMeta::new(user_output_ata, false),
            AccountMeta::new_readonly(input_mint, false),
            AccountMeta::new_readonly(output_mint, false),
            AccountMeta::new_readonly(jupiter_program, false),
        ];

        instructions.push(Instruction {
            program_id: jupiter_program,
            accounts,
            data,
        });

        Ok(instructions)
    }

    async fn send_transaction(
        rpc: &Arc<RpcClient>,
        keypair: &Arc<Keypair>,
        instructions: Vec<Instruction>,
    ) -> Result<String, Box<dyn std::error::Error>> {
        let recent_blockhash = rpc.get_latest_blockhash()?;
        
        let simulation = rpc.simulate_transaction(&Transaction::new_signed_with_payer(
            &instructions,
            Some(&keypair.pubkey()),
            &[keypair.as_ref()],
            recent_blockhash,
        ))?;

        if let Some(err) = simulation.value.err {
            return Err(format!("Simulation failed: {:?}", err).into());
        }

        let transaction = Transaction::new_signed_with_payer(
            &instructions,
            Some(&keypair.pubkey()),
            &[keypair.as_ref()],
            recent_blockhash,
        );

        let config = solana_client::rpc_config::RpcSendTransactionConfig {
            skip_preflight: false,
            preflight_commitment: Some(CommitmentConfig::processed()),
            encoding: Some(solana_account_decoder::UiTransactionEncoding::Base64),
            max_retries: Some(0),
            min_context_slot: None,
        };

        let signature = rpc.send_transaction_with_config(&transaction, config)?;
        
        let start = Instant::now();
        loop {
            match rpc.get_signature_status(&signature)? {
                Some(Ok(_)) => return Ok(signature.to_string()),
                Some(Err(err)) => return Err(format!("Transaction failed: {:?}", err).into()),
                None => {
                    if start.elapsed() > Duration::from_secs(30) {
                        return Err("Transaction confirmation timeout".into());
                    }
                    sleep(Duration::from_millis(250)).await;
                }
            }
        }
    }

    async fn start_position_monitor(&self) -> Result<(), Box<dyn std::error::Error>> {
        let positions = self.positions.clone();
        let active_proposals = self.active_proposals.clone();
        let rpc = self.rpc_client.clone();
        let keypair = self.keypair.clone();
        let price_feeds = self.price_feeds.clone();
        let oracle_mappings = self.oracle_mappings.clone();

        tokio::spawn(async move {
            loop {
                let mut positions_to_close = Vec::new();
                
                {
                    let positions_lock = positions.lock().unwrap();
                    let proposals_lock = active_proposals.lock().unwrap();
                    
                    for (proposal_pubkey, position) in positions_lock.iter() {
                        if let Some(proposal) = proposals_lock.get(proposal_pubkey) {
                            if let Ok(current_price) = Self::get_token_price(&rpc, &position.token_mint, &oracle_mappings).await {
                                if Self::should_close_position(position, proposal, current_price) {
                                    positions_to_close.push(*proposal_pubkey);
                                }
                            }
                        }
                    }
                }

                for proposal_pubkey in positions_to_close {
                    if let Some(position) = positions.lock().unwrap().remove(&proposal_pubkey) {
                        if let Err(e) = Self::close_position(&rpc, &keypair, &position, &oracle_mappings).await {
                            eprintln!("Error closing position: {:?}", e);
                            positions.lock().unwrap().insert(proposal_pubkey, position);
                        }
                    }
                }

                Self::update_price_feeds(&rpc, &positions, &price_feeds, &oracle_mappings).await;
                sleep(Duration::from_secs(2)).await;
            }
        });

        Ok(())
    }

    fn should_close_position(position: &Position, proposal: &ProposalMetadata, current_price: f64) -> bool {
        if matches!(proposal.state, ProposalState::Completed | ProposalState::Cancelled | ProposalState::Defeated) {
            return true;
        }

        if current_price <= position.stop_loss || current_price >= position.target_price {
            return true;
        }

        if position.opened_at.elapsed() > Duration::from_secs(21600) {
            return true;
        }

        if let Some(voting_at) = proposal.voting_at {
            let voting_end = voting_at + proposal.max_voting_time as i64;
            let now = chrono::Utc::now().timestamp();
            
            if voting_end - now < 3600 && voting_end > now {
                let total_votes = proposal.yes_votes + proposal.no_votes;
                if total_votes > 0 {
                    let yes_ratio = proposal.yes_votes as f64 / total_votes as f64;
                    if yes_ratio < 0.5 {
                        return true;
                    }
                }
            }
        }

        false
    }

    async fn close_position(
        rpc: &Arc<RpcClient>,
        keypair: &Arc<Keypair>,
        position: &Position,
        oracle_mappings: &HashMap<Pubkey, OracleInfo>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let instructions = Self::build_trade_instructions(
            &keypair.pubkey(),
            &position.token_mint,
            position.amount,
            false,
        ).await?;

        let signature = Self::send_transaction(rpc, keypair, instructions).await?;
        
        let current_price = Self::get_token_price(rpc, &position.token_mint, oracle_mappings).await?;
        let pnl = (current_price - position.entry_price) * position.amount as f64 / 1e9;
        
        println!(
            "Position closed - Proposal: {}, PnL: ${:.2}, Price: {:.6} -> {:.6}, Tx: {}",
            position.proposal_pubkey, pnl, position.entry_price, current_price, signature
        );
        
        Ok(())
    }

    async fn update_price_feeds(
        rpc: &Arc<RpcClient>,
        positions: &Arc<Mutex<HashMap<Pubkey, Position>>>,
        price_feeds: &Arc<Mutex<HashMap<Pubkey, PriceFeed>>>,
        oracle_mappings: &HashMap<Pubkey, OracleInfo>,
    ) {
        let mut tokens_to_update = HashSet::new();
        
        {
            let positions_lock = positions.lock().unwrap();
            for position in positions_lock.values() {
                tokens_to_update.insert(position.token_mint);
            }
        }

        for token_mint in tokens_to_update {
            if let Ok(price) = Self::get_token_price(rpc, &token_mint, oracle_mappings).await {
                let mut feeds = price_feeds.lock().unwrap();
                
                let volume_24h = if let Some(existing) = feeds.get(&token_mint) {
                    existing.volume_24h * 0.95 + (price * 1000000.0 * 0.05)
                } else {
                    price * 1000000.0
                };

                let market_cap = if let Ok(supply) = rpc.get_token_supply(&token_mint) {
                    price * supply.ui_amount.unwrap_or(0.0)
                } else {
                    0.0
                };

                feeds.insert(token_mint, PriceFeed {
                    price,
                    volume_24h,
                    market_cap,
                    last_update: Instant::now(),
                });
            }
        }
    }

    pub async fn get_portfolio_value(&self) -> Result<f64, Box<dyn std::error::Error>> {
        let positions = self.positions.lock().unwrap();
        let mut total_value = 0.0;

        for position in positions.values() {
            if let Ok(current_price) = Self::get_token_price(&self.rpc_client, &position.token_mint, &self.oracle_mappings).await {
                total_value += (position.amount as f64 / 1e9) * current_price;
            }
        }

        let sol_balance = self.rpc_client.get_balance(&self.keypair.pubkey())? as f64 / 1e9;
        if let Ok(sol_price) = Self::get_token_price(
            &self.rpc_client,
            &Pubkey::from_str("So11111111111111111111111111111111111111112")?,
            &self.oracle_mappings,
        ).await {
            total_value += sol_balance * sol_price;
        }

        Ok(total_value)
    }

    pub async fn emergency_close_all_positions(&self) -> Result<(), Box<dyn std::error::Error>> {
        let positions_to_close: Vec<Position> = {
            let mut positions = self.positions.lock().unwrap();
            let all_positions: Vec<Position> = positions.values().cloned().collect();
            positions.clear();
            all_positions
        };

        println!("Emergency shutdown initiated - closing {} positions", positions_to_close.len());

        let mut closed = 0;
        for position in positions_to_close {
            match Self::close_position(&self.rpc_client, &self.keypair, &position, &self.oracle_mappings).await {
                Ok(_) => closed += 1,
                Err(e) => eprintln!("Failed to close position {}: {:?}", position.proposal_pubkey, e),
            }
        }

        println!("Emergency shutdown complete - closed {}/{} positions", closed, positions_to_close.len());
        Ok(())
    }

    pub async fn get_performance_stats(&self) -> PerformanceStats {
        let positions = self.positions.lock().unwrap();
        let mut total_pnl = 0.0;
        let mut winning_trades = 0;
        let mut total_trades = 0;

        for position in positions.values() {
            if let Ok(current_price) = Self::get_token_price(&self.rpc_client, &position.token_mint, &self.oracle_mappings).await {
                let pnl = (current_price - position.entry_price) * (position.amount as f64 / 1e9);
                total_pnl += pnl;
                total_trades += 1;
                if pnl > 0.0 {
                    winning_trades += 1;
                }
            }
        }

        let win_rate = if total_trades > 0 {
            (winning_trades as f64 / total_trades as f64) * 100.0
        } else {
            0.0
        };

        PerformanceStats {
            total_pnl,
            win_rate,
            total_trades,
            active_positions: positions.len(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct PerformanceStats {
    pub total_pnl: f64,
    pub win_rate: f64,
    pub total_trades: usize,
    pub active_positions: usize,
}

impl std::fmt::Display for PerformanceStats {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "PnL: ${:.2} | Win Rate: {:.1}% | Trades: {} | Active: {}",
            self.total_pnl, self.win_rate, self.total_trades, self.active_positions
        )
    }
}

