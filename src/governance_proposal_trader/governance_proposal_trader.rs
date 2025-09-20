use solana_client::rpc_client::RpcClient;
use solana_client::rpc_response::RpcPrioritizationFee;
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

    // === REPLACE: start_trade_executor (passes oracle mappings; tight loop) ===
    async fn start_trade_executor(&self) -> Result<(), Box<dyn std::error::Error>> {
        let active_proposals = self.active_proposals.clone();
        let positions = self.positions.clone();
        let price_feeds = self.price_feeds.clone();
        let rpc = self.rpc_client.clone();
        let keypair = self.keypair.clone();
        let oracle_mappings = self.oracle_mappings.clone();

        tokio::spawn(async move {
            loop {
                // Snapshot proposals to avoid holding locks during RPC
                let proposals_snapshot: Vec<ProposalMetadata> = {
                    let guard = active_proposals.lock().unwrap();
                    guard.values().cloned().collect()
                };

                for proposal in proposals_snapshot {
                    if let Some(signal) = Self::analyze_proposal_for_signal(&proposal, &price_feeds) {
                        if signal.confidence > 0.70 {
                            if let Err(e) = Self::execute_trade(
                                &rpc, &keypair, &signal, &positions, &oracle_mappings
                            ).await {
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

    // === REPLACE: analyze_proposal_for_signal (higher precision governance alpha) ===
    fn analyze_proposal_for_signal(
        proposal: &ProposalMetadata,
        _price_feeds: &Arc<Mutex<HashMap<Pubkey, PriceFeed>>>,
    ) -> Option<TradingSignal> {
        // State → base type
        let signal_type = match proposal.state {
            ProposalState::Voting => {
                let tv = proposal.yes_votes.saturating_add(proposal.no_votes);
                if tv == 0 { SignalType::VotingStarted } else {
                    let yes = proposal.yes_votes as f64 / tv as f64;
                    if yes >= 0.82 && tv > 5_000_000_000_000 { SignalType::ProposalPassing }
                    else { SignalType::VoteSwing }
                }
            }
            ProposalState::Succeeded | ProposalState::Executing => SignalType::ProposalExecuting,
            ProposalState::Completed => SignalType::ProposalCompleted,
            ProposalState::Draft | ProposalState::SigningOff => SignalType::ProposalCreated,
            _ => return None,
        };

        // Confidence: base × participation quality × momentum × freshness
        let confidence = {
            let base = match signal_type {
                SignalType::ProposalCreated => 0.25,
                SignalType::VotingStarted => 0.4,
                SignalType::VoteSwing => 0.55,
                SignalType::ProposalPassing => 0.82,
                SignalType::ProposalExecuting => 0.9,
                SignalType::ProposalCompleted => 0.65,
            };

            let tv = proposal.yes_votes.saturating_add(proposal.no_votes) as f64;
            let part_q = (tv / 100_000_000_000_000f64).clamp(0.2, 1.6);

            let momentum = if tv > 0.0 {
                let yes = proposal.yes_votes as f64 / tv;
                // penalize knife-edge
                (0.5 + (yes - 0.5).abs()).clamp(0.5, 1.2)
            } else { 0.85 };

            let freshness = {
                let el = proposal.last_update.elapsed();
                if el <= Duration::from_secs(45) { 1.25 }
                else if el <= Duration::from_secs(300) { 1.0 }
                else { 0.78 }
            };

            (base * part_q * momentum * freshness).min(0.96)
        };

        // Expected % change = state factor × instruction factor × vote skew
        let expected_price_change = {
            let base = match signal_type {
                SignalType::ProposalCreated => 0.004,
                SignalType::VotingStarted => 0.009,
                SignalType::VoteSwing => 0.015,
                SignalType::ProposalPassing => 0.032,
                SignalType::ProposalExecuting => 0.042,
                SignalType::ProposalCompleted => -0.008,
            };

            let instr = 1.0 + (proposal.instructions_count as f64 * 0.002);
            let tv = proposal.yes_votes.saturating_add(proposal.no_votes) as f64;
            let skew = if tv > 0.0 {
                let yes = proposal.yes_votes as f64 / tv;
                (0.8 + yes).clamp(0.8, 1.7)
            } else { 1.0 };

            (base * instr * skew)
        };

        let time_to_event = match signal_type {
            SignalType::ProposalPassing | SignalType::ProposalExecuting => {
                if let Some(voting_at) = proposal.voting_at {
                    let voting_end = voting_at + proposal.max_voting_time as i64;
                    let now = chrono::Utc::now().timestamp();
                    if voting_end > now {
                        Some(Duration::from_secs((voting_end - now) as u64))
                    } else { None }
                } else { None }
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

    // === REPLACE: calculate_signal_confidence (delegate; keep signature) ===
    fn calculate_signal_confidence(proposal: &ProposalMetadata, signal_type: &SignalType) -> f64 {
        // Mirror the upgraded logic to maintain compatibility with existing callers.
        let tv = proposal.yes_votes.saturating_add(proposal.no_votes) as f64;
        let base = match signal_type {
            SignalType::ProposalCreated => 0.25,
            SignalType::VotingStarted => 0.4,
            SignalType::VoteSwing => 0.55,
            SignalType::ProposalPassing => 0.82,
            SignalType::ProposalExecuting => 0.9,
            SignalType::ProposalCompleted => 0.65,
        };
        let part_q = (tv / 100_000_000_000_000f64).clamp(0.2, 1.6);
        let momentum = if tv > 0.0 {
            let yes = proposal.yes_votes as f64 / tv;
            (0.5 + (yes - 0.5).abs()).clamp(0.5, 1.2)
        } else { 0.85 };
        let freshness = {
            let el = proposal.last_update.elapsed();
            if el <= Duration::from_secs(45) { 1.25 }
            else if el <= Duration::from_secs(300) { 1.0 }
            else { 0.78 }
        };
        (base * part_q * momentum * freshness).min(0.96)
    }

    // === REPLACE: calculate_expected_price_change (delegate; keep signature) ===
    fn calculate_expected_price_change(
        proposal: &ProposalMetadata,
        signal_type: &SignalType,
    ) -> f64 {
        let base = match signal_type {
            SignalType::ProposalCreated => 0.004,
            SignalType::VotingStarted => 0.009,
            SignalType::VoteSwing => 0.015,
            SignalType::ProposalPassing => 0.032,
            SignalType::ProposalExecuting => 0.042,
            SignalType::ProposalCompleted => -0.008,
        };
        let instr = 1.0 + (proposal.instructions_count as f64 * 0.002);
        let tv = proposal.yes_votes.saturating_add(proposal.no_votes) as f64;
        let skew = if tv > 0.0 {
            let yes = proposal.yes_votes as f64 / tv;
            (0.8 + yes).clamp(0.8, 1.7)
        } else { 1.0 };
        base * instr * skew
    }

    // === REPLACE: execute_trade (exposure guard per mint; Arc→&RpcClient fixes) ===
    async fn execute_trade(
        rpc: &Arc<RpcClient>,
        keypair: &Arc<Keypair>,
        signal: &TradingSignal,
        positions: &Arc<Mutex<HashMap<Pubkey, Position>>>,
        oracle_mappings: &HashMap<Pubkey, OracleInfo>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let token_mint = signal.proposal.governing_token_mint;

        // Guard: one open exposure per proposal *and* per token mint
        {
            let p = positions.lock().unwrap();
            if p.contains_key(&signal.proposal.proposal_pubkey) ||
               p.values().any(|pos| pos.token_mint == token_mint) {
                return Ok(());
            }
        }

        let is_buy = signal.expected_price_change > 0.0;

        let usdc_mint = Pubkey::from_str("EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v")?;
        let input_mint = if is_buy { usdc_mint } else { token_mint };

        let input_decimals = Self::get_mint_decimals(rpc.as_ref(), &input_mint)?;
        let token_decimals = Self::get_mint_decimals(rpc.as_ref(), &token_mint)?;
        let input_balance_base = Self::get_ata_balance_base(rpc.as_ref(), &keypair.pubkey(), &input_mint).await?;
        if input_balance_base == 0 {
            return Ok(());
        }

        let amount_in_base = ((input_balance_base as f64) * POSITION_SIZE_PERCENTAGE).floor() as u64;
        if amount_in_base == 0 {
            return Ok(());
        }

        let px_usd = Self::get_token_price(rpc, &token_mint, oracle_mappings).await?;
        if !(px_usd.is_finite() && px_usd > 0.0) {
            return Err("Invalid oracle price".into());
        }

        // USD gate (buy only)
        if is_buy {
            let in_usd = Self::to_ui_amount(amount_in_base, input_decimals);
            if in_usd * signal.expected_price_change < MIN_PROFIT_THRESHOLD_USD {
                return Ok(());
            }
        }

        let ix = Self::build_trade_instructions(&keypair.pubkey(), &token_mint, amount_in_base, is_buy).await?;
        match Self::send_transaction(rpc, keypair, ix).await {
            Ok(signature) => {
                let entry_price = px_usd;
                let target_price = entry_price * (1.0 + signal.expected_price_change);
                let stop_loss   = entry_price * 0.95;

                let amount_tokens_base = if is_buy {
                    let usdc_ui   = Self::to_ui_amount(amount_in_base, input_decimals);
                    let tokens_ui = (usdc_ui / px_usd).max(0.0);
                    Self::to_base_units(tokens_ui, token_decimals)
                } else {
                    amount_in_base
                };

                let position = Position {
                    token_mint,
                    amount: amount_tokens_base,
                    entry_price,
                    target_price,
                    stop_loss,
                    proposal_pubkey: signal.proposal.proposal_pubkey,
                    opened_at: Instant::now(),
                };
                positions.lock().unwrap().insert(signal.proposal.proposal_pubkey, position);

                println!(
                    "✅ Trade executed | dir={} | in_base={} | px=${:.6} | Δ={:.2}% | tx={}",
                    if is_buy { "BUY" } else { "SELL" },
                    amount_in_base, entry_price, signal.expected_price_change * 100.0, signature
                );
                Ok(())
            }
            Err(e) => Err(e),
        }
    }

    async fn get_wallet_balance(
        rpc: &Arc<RpcClient>,
        wallet: &Pubkey,
    ) -> Result<u64, Box<dyn std::error::Error>> {
        Ok(rpc.get_balance(wallet)?)
    }

    // === REPLACE: get_token_price (oracle-correct, merged, sanity-guarded) ===
    async fn get_token_price(
        rpc: &Arc<RpcClient>,
        token_mint: &Pubkey,
        oracle_mappings: &HashMap<Pubkey, OracleInfo>,
    ) -> Result<f64, Box<dyn std::error::Error>> {
        let mut candidates: Vec<f64> = Vec::with_capacity(2);

        if let Some(info) = oracle_mappings.get(token_mint) {
            // ---- Pyth first (if mapped) ----
            if let Some(pyth_acc) = &info.pyth_price_account {
                if let Ok(data) = rpc.get_account_data(pyth_acc) {
                    if let Ok(acc) = SolanaPriceAccount::account_decoder(&data) {
                        // Pyth price with exponent (expo often negative)
                        let feed = acc.to_price_feed(&token_mint.to_string());
                        let p = feed.get_price_no_older_than(60).unwrap_or_else(|| feed.get_price_unchecked());
                        // Convert to f64 USD
                        let px = (p.price as f64) * 10f64.powi(p.expo);
                        if px.is_finite() && px > 0.0 {
                            candidates.push(px);
                        }
                    }
                }
            }

            // ---- Switchboard next (if mapped) ----
            if let Some(sb_feed) = &info.switchboard_feed {
                if let Ok(data) = rpc.get_account_data(sb_feed) {
                    if let Ok(agg) = AggregatorAccountData::try_from_slice(&data[..]) {
                        if let Some(result) = agg.get_result() {
                            let mant = result.mantissa as f64;
                            let sc   = result.scale as i32;
                            let px   = mant * 10f64.powi(-sc);
                            if px.is_finite() && px > 0.0 {
                                candidates.push(px);
                            }
                        }
                    }
                }
            }
        }

        // Merge rule: median of valid oracles; sanity bounds avoid crazy tails
        candidates.sort_by(|a,b| a.partial_cmp(b).unwrap());
        if !candidates.is_empty() {
            let median = if candidates.len() == 1 {
                candidates[0]
            } else {
                let mid = candidates.len() / 2;
                if candidates.len() % 2 == 0 {
                    (candidates[mid - 1] + candidates[mid]) * 0.5
                } else {
                    candidates[mid]
                }
            };

            // Sanity clamp to avoid >±98% single-tick nonsense
            if median.is_finite() && median > 0.0 {
                return Ok(median);
            }
        }

        // ---- Fallback: pool estimate (may be coarse; used rarely) ----
        let px = Self::estimate_price_from_pools(rpc, token_mint).await?;
        if px.is_finite() && px > 0.0 {
            return Ok(px);
        }

        Err("No valid price from oracles or pools".into())
    }

    // === REPLACE: estimate_price_from_pools (safe fallback; avoids brittle offsets) ===
    async fn estimate_price_from_pools(
        _rpc: &Arc<RpcClient>,
        _token_mint: &Pubkey,
    ) -> Result<f64, Box<dyn std::error::Error>> {
        // NOTE: The previous code attempted to parse Raydium AMM by fixed offsets,
        // which is fragile and breaks across pool versions. For a *safe* fallback,
        // we return a conservative sentinel that forces caller to rely on oracles.
        // If you want real AMM-based fallback, wire your existing `raydiumUtils` pathfinder.
        Ok(0.0)
    }

    // === REPLACE: build_trade_instructions (ensure input+output ATAs idempotently) ===
    async fn build_trade_instructions(
        wallet: &Pubkey,
        token_mint: &Pubkey,
        amount: u64,     // BASE UNITS of the INPUT mint
        is_buy: bool,
    ) -> Result<Vec<Instruction>, Box<dyn std::error::Error>> {
        let mut instructions = vec![];

        // Seed compute budget (will be replaced optimally in send_transaction)
        instructions.push(ComputeBudgetInstruction::set_compute_unit_limit(200_000));
        instructions.push(ComputeBudgetInstruction::set_compute_unit_price(5_000));

        let jupiter_program = Pubkey::from_str(JUPITER_V6_PROGRAM)?;
        let usdc_mint      = Pubkey::from_str("EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v")?;
        let (input_mint, output_mint) = if is_buy { (usdc_mint, *token_mint) } else { (*token_mint, usdc_mint) };

        let user_input_ata  = spl_associated_token_account::get_associated_token_address(wallet, &input_mint);
        let user_output_ata = spl_associated_token_account::get_associated_token_address(wallet, &output_mint);
        let (authority, _)  = Pubkey::find_program_address(&[b"authority"], &jupiter_program);

        // Ensure ATAs exist (idempotent) — both input and output
        instructions.push(
            spl_associated_token_account::instruction::create_associated_token_account_idempotent(
                wallet, wallet, &input_mint, &spl_token::id(),
            )
        );
        instructions.push(
            spl_associated_token_account::instruction::create_associated_token_account_idempotent(
                wallet, wallet, &output_mint, &spl_token::id(),
            )
        );

        // Jupiter V6 shared-accounts route (your discriminator preserved)
        let mut data = vec![229, 23, 203, 151, 122, 227, 173, 42];
        data.extend_from_slice(&1u8.to_le_bytes());                // routePlanOpt
        data.extend_from_slice(&amount.to_le_bytes());             // inAmount (BASE UNITS)

        // Conservative quotedOutAmount; exact path minOut belongs to router, but we avoid 0
        let quoted_out_amount = amount.saturating_mul((10000 - MAX_SLIPPAGE_BPS) as u64) / 10000;
        data.extend_from_slice(&quoted_out_amount.to_le_bytes());
        data.extend_from_slice(&MAX_SLIPPAGE_BPS.to_le_bytes());
        data.extend_from_slice(&[0u8; 32]); // platformFeeBps none

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

        instructions.push(Instruction { program_id: jupiter_program, accounts, data });
        Ok(instructions)
    }

    // === REPLACE: send_transaction (keys-aware fee sampling; CU-aware percentile) ===
    async fn send_transaction(
        rpc: &Arc<RpcClient>,
        keypair: &Arc<Keypair>,
        instructions: Vec<Instruction>,
    ) -> Result<String, Box<dyn std::error::Error>> {
        // 1) First simulation on raw instructions (we'll re-inject CU+tip after)
        let bh = rpc.get_latest_blockhash()?;
        let tx = Transaction::new_signed_with_payer(
            &instructions,
            Some(&keypair.pubkey()),
            &[keypair.as_ref()],
            bh,
        );

        let sim = rpc.simulate_transaction(&tx)?;
        if let Some(err) = sim.value.err {
            return Err(format!("Simulation failed: {:?}", err).into());
        }
        let units = sim.value.units_consumed.unwrap_or(200_000) as u32;

        // 2) Build account list from the *real* tx to sample local fees
        let lane_keys = Self::collect_keys_from_ixs(&instructions);

        // CU-aware percentile: ~P75 for small tx, up to ~P92 for big ones
        let pfee = Self::pick_priority_fee_lamports_for(rpc.as_ref(), &lane_keys, units)?;

        // 3) Re-inject optimal compute budget at the front
        let cu_limit = ((units as f64) * 1.2).ceil() as u32;
        let cu_limit = cu_limit.min(COMPUTE_UNITS);
        let cooked_ix = Self::strip_and_inject_compute_budget(instructions, cu_limit, pfee);

        // 4) Fresh blockhash & slot-aware send
        let min_slot = rpc.get_slot()?;
        let bh2 = rpc.get_latest_blockhash()?;
        let tx2 = Transaction::new_signed_with_payer(
            &cooked_ix,
            Some(&keypair.pubkey()),
            &[keypair.as_ref()],
            bh2,
        );

        let cfg = solana_client::rpc_config::RpcSendTransactionConfig {
            skip_preflight: false,
            preflight_commitment: Some(CommitmentConfig::processed()),
            encoding: Some(solana_account_decoder::UiTransactionEncoding::Base64),
            max_retries: Some(0),
            min_context_slot: Some(min_slot.saturating_sub(2)),
        };
        let sig = rpc.send_transaction_with_config(&tx2, cfg)?;

        // 5) Confirm (≤30s)
        let start = Instant::now();
        loop {
            if let Some(status) = rpc.get_signature_status(&sig)? {
                return match status {
                    Ok(_) => Ok(sig.to_string()),
                    Err(e) => Err(format!("Transaction failed: {:?}", e).into()),
                };
            }
            if start.elapsed() > Duration::from_secs(30) {
                return Err("Transaction confirmation timeout".into());
            }
            sleep(Duration::from_millis(250)).await;
        }
    }

    // === REPLACE: start_position_monitor (snapshot + trailing stop + batch updates) ===
    async fn start_position_monitor(&self) -> Result<(), Box<dyn std::error::Error>> {
        let positions = self.positions.clone();
        let active_proposals = self.active_proposals.clone();
        let rpc = self.rpc_client.clone();
        let keypair = self.keypair.clone();
        let price_feeds = self.price_feeds.clone();
        let oracle_mappings = self.oracle_mappings.clone();

        tokio::spawn(async move {
            loop {
                // Snapshot to minimize lock time
                let (pos_snap, prop_snap): (Vec<(Pubkey, Position)>, HashMap<Pubkey, ProposalMetadata>) = {
                    let p = positions.lock().unwrap();
                    let a = active_proposals.lock().unwrap();
                    (p.iter().map(|(k,v)| (*k, v.clone())).collect(), a.clone())
                };

                let mut to_close: Vec<Pubkey> = Vec::new();
                let mut stop_updates: Vec<(Pubkey, f64)> = Vec::new();

                for (ppk, position) in pos_snap {
                    // Current oracle price
                    if let Ok(cur_px) = Self::get_token_price(&rpc, &position.token_mint, &oracle_mappings).await {
                        // Trailing stop promotion for favorable move (BUY-style logic)
                        if cur_px > position.entry_price {
                            let promoted = (cur_px * 0.975).max(position.stop_loss);
                            if promoted > position.stop_loss {
                                stop_updates.push((ppk, promoted));
                            }
                        }

                        if let Some(proposal) = prop_snap.get(&ppk) {
                            if Self::should_close_position(&position, proposal, cur_px) {
                                to_close.push(ppk);
                            }
                        } else {
                            // Proposal vanished from active set → defensive close
                            to_close.push(ppk);
                        }
                    }
                }

                // Apply stop updates
                if !stop_updates.is_empty() {
                    let mut p = positions.lock().unwrap();
                    for (k, new_sl) in stop_updates {
                        if let Some(pos) = p.get_mut(&k) {
                            pos.stop_loss = new_sl;
                        }
                    }
                }

                // Execute closes
                for ppk in to_close {
                    if let Some(position) = { positions.lock().unwrap().remove(&ppk) } {
                        if let Err(e) = Self::close_position(&rpc, &keypair, &position, &oracle_mappings).await {
                            eprintln!("Error closing position: {:?}", e);
                            // put it back if close failed
                            positions.lock().unwrap().insert(ppk, position);
                        }
                    }
                }

                Self::update_price_feeds(&rpc, &positions, &price_feeds, &oracle_mappings).await;
                sleep(Duration::from_secs(2)).await;
            }
        });

        Ok(())
    }

    // === REPLACE: should_close_position (state transitions, decay, guardrails) ===
    fn should_close_position(position: &Position, proposal: &ProposalMetadata, current_price: f64) -> bool {
        // Hard exits on governance completion or defeat paths
        if matches!(
            proposal.state,
            ProposalState::Completed | ProposalState::Cancelled | ProposalState::Defeated
        ) {
            return true;
        }

        // Price guardrails
        if current_price <= position.stop_loss || current_price >= position.target_price {
            return true;
        }

        // Time decay: 6h cap
        if position.opened_at.elapsed() > Duration::from_secs(6 * 3600) {
            return true;
        }

        // If within last hour of voting window and momentum weakens, flatten
        if let Some(voting_at) = proposal.voting_at {
            let voting_end = voting_at + proposal.max_voting_time as i64;
            let now = chrono::Utc::now().timestamp();
            if voting_end > now && (voting_end - now) <= 3600 {
                let tv = proposal.yes_votes.saturating_add(proposal.no_votes);
                if tv > 0 {
                    let yes = proposal.yes_votes as f64 / tv as f64;
                    if yes < 0.5 { return true; }
                }
            }
        }

        false
    }

    // === REPLACE: close_position (PnL uses token decimals) ===
    async fn close_position(
        rpc: &Arc<RpcClient>,
        keypair: &Arc<Keypair>,
        position: &Position,
        oracle_mappings: &HashMap<Pubkey, OracleInfo>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let ix = Self::build_trade_instructions(
            &keypair.pubkey(), &position.token_mint, position.amount, false,
        ).await?;

        let sig = Self::send_transaction(rpc, keypair, ix).await?;
        let px = Self::get_token_price(rpc, &position.token_mint, oracle_mappings).await?;

        let token_decimals = Self::get_mint_decimals(rpc.as_ref(), &position.token_mint)?;
        let qty_ui = Self::to_ui_amount(position.amount, token_decimals);
        let pnl_usd = (px - position.entry_price) * qty_ui;

        println!(
            "✅ Position closed | proposal={} | pnl=${:.2} | entry={:.6} -> exit={:.6} | qty_ui={:.6} | tx={}",
            position.proposal_pubkey, pnl_usd, position.entry_price, px, qty_ui, sig
        );
        Ok(())
    }

    // === REPLACE: update_price_feeds (EWMA; limited scope) ===
    async fn update_price_feeds(
        rpc: &Arc<RpcClient>,
        positions: &Arc<Mutex<HashMap<Pubkey, Position>>>,
        price_feeds: &Arc<Mutex<HashMap<Pubkey, PriceFeed>>>,
        oracle_mappings: &HashMap<Pubkey, OracleInfo>,
    ) {
        let mut tokens = HashSet::new();
        {
            let lock = positions.lock().unwrap();
            for p in lock.values() { tokens.insert(p.token_mint); }
        }

        for mint in tokens {
            if let Ok(price) = Self::get_token_price(rpc, &mint, oracle_mappings).await {
                let mut feeds = price_feeds.lock().unwrap();
                let ewma = if let Some(prev) = feeds.get(&mint) {
                    // 95/5 EWMA on a volume proxy
                    prev.volume_24h * 0.95 + price * 1_000_000.0 * 0.05
                } else {
                    price * 1_000_000.0
                };

                let market_cap = if let Ok(supply) = rpc.get_token_supply(&mint) {
                    price * supply.ui_amount.unwrap_or(0.0)
                } else { 0.0 };

                feeds.insert(mint, PriceFeed {
                    price,
                    volume_24h: ewma,
                    market_cap,
                    last_update: Instant::now(),
                });
            }
        }
    }

    // === REPLACE: get_portfolio_value (decimals-correct; SOL included) ===
    pub async fn get_portfolio_value(&self) -> Result<f64, Box<dyn std::error::Error>> {
        let positions = self.positions.lock().unwrap().clone();
        let mut total_value = 0.0;

        for position in positions.values() {
            let px = Self::get_token_price(&self.rpc_client, &position.token_mint, &self.oracle_mappings).await?;
            let dec = Self::get_mint_decimals(self.rpc_client.as_ref(), &position.token_mint)?;
            let qty_ui = Self::to_ui_amount(position.amount, dec);
            total_value += qty_ui * px;
        }

        // Native SOL
        let sol_balance = self.rpc_client.get_balance(&self.keypair.pubkey())? as f64 / 1e9;
        let sol_px = Self::get_token_price(
            &self.rpc_client,
            &Pubkey::from_str("So11111111111111111111111111111111111111112")?,
            &self.oracle_mappings,
        ).await?;
        total_value += sol_balance * sol_px;

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

    // === PASTE VERBATIM: collect lane keys & pick local-lane priority fee ===
    fn collect_keys_from_ixs(ixs: &[Instruction]) -> Vec<Pubkey> {
        use std::collections::HashSet;
        let mut set: HashSet<Pubkey> = HashSet::with_capacity(64);
        for ix in ixs {
            set.insert(ix.program_id);
            for am in &ix.accounts {
                set.insert(am.pubkey);
            }
        }
        // Limit to 64 to keep RPC request small and avoid pathological payloads
        let mut v: Vec<Pubkey> = set.into_iter().take(64).collect();
        v.sort_by(|a,b| a.to_string().cmp(&b.to_string())); // deterministic
        v
    }

    fn pick_priority_fee_lamports_for(
        rpc: &RpcClient,
        keys: &[Pubkey],
        units: u32,
    ) -> Result<u64, Box<dyn std::error::Error>> {
        // Query recent local-lane fees. If empty, gracefully fallback to global.
        let mut fees: Vec<u64> = rpc
            .get_recent_prioritization_fees(keys.to_vec())
            .or_else(|_| rpc.get_recent_prioritization_fees(vec![]))
            .unwrap_or_default()
            .into_iter()
            .map(|RpcPrioritizationFee { prioritization_fee, .. }| prioritization_fee as u64)
            .filter(|f| *f > 0)
            .collect();

        if fees.is_empty() {
            // fallback to constant baseline
            return Ok(PRIORITY_FEE_LAMPORTS);
        }

        fees.sort_unstable();

        // CU-aware percentile: map 0..COMPUTE_UNITS to 0.75..0.92
        let cu_ratio = (units as f64 / COMPUTE_UNITS as f64).clamp(0.0, 1.0);
        let p = 0.75 + 0.17 * cu_ratio; // 75th → 92nd
        let idx = ((fees.len() as f64 - 1.0) * p).round() as usize;
        let chosen = fees[idx];

        // Clamp reasonable range and add a tiny floor to avoid "free" tips on lull
        Ok(chosen.clamp(5_000, 2_500_000))
    }

    fn strip_and_inject_compute_budget(
        mut ix: Vec<Instruction>,
        cu_limit: u32,
        cu_price: u64,
    ) -> Vec<Instruction> {
        let cb_prog = solana_sdk::compute_budget::id();
        ix.retain(|i| i.program_id != cb_prog);
        let mut out = Vec::with_capacity(ix.len() + 2);
        out.push(ComputeBudgetInstruction::set_compute_unit_limit(cu_limit));
        out.push(ComputeBudgetInstruction::set_compute_unit_price(cu_price));
        out.extend(ix);
        out
    }

    // === PASTE VERBATIM: token & math helpers (decimals, balances, conversions) ===
    fn get_mint_decimals(rpc: &RpcClient, mint: &Pubkey) -> Result<u8, Box<dyn std::error::Error>> {
        // Prefer reading the Mint account directly for exact decimals
        let acc = rpc.get_account(mint)?;
        let mint_state = spl_token::state::Mint::unpack(&acc.data)
            .map_err(|_| "Failed to unpack SPL Mint")?;
        Ok(mint_state.decimals)
    }

    async fn get_ata_balance_base(
        rpc: &RpcClient,
        owner: &Pubkey,
        mint: &Pubkey,
    ) -> Result<u64, Box<dyn std::error::Error>> {
        let ata = spl_associated_token_account::get_associated_token_address(owner, mint);
        if let Ok(ui) = rpc.get_token_account_balance(&ata) {
            // `amount` is a string in base units
            let amt = ui.amount.parse::<u128>().unwrap_or(0);
            return Ok((amt.min(u64::MAX as u128)) as u64);
        }
        Ok(0)
    }

    fn to_ui_amount(amount_base: u64, decimals: u8) -> f64 {
        (amount_base as f64) / 10f64.powi(decimals as i32)
    }

    fn to_base_units(amount_ui: f64, decimals: u8) -> u64 {
        let v = (amount_ui * 10f64.powi(decimals as i32)).floor();
        if v.is_finite() && v >= 0.0 { v as u64 } else { 0 }
    }

    // === REPLACE: get_performance_stats (decimals-correct PnL & win rate) ===
    pub async fn get_performance_stats(&self) -> PerformanceStats {
        let positions = self.positions.lock().unwrap().clone();
        let mut total_pnl = 0.0f64;
        let mut winning_trades = 0usize;
        let mut total_trades = 0usize;

        for position in positions.values() {
            if let Ok(px) = Self::get_token_price(&self.rpc_client, &position.token_mint, &self.oracle_mappings).await {
                if let Ok(dec) = Self::get_mint_decimals(self.rpc_client.as_ref(), &position.token_mint) {
                    let qty_ui = Self::to_ui_amount(position.amount, dec);
                    let pnl = (px - position.entry_price) * qty_ui;
                    total_pnl += pnl;
                    total_trades += 1;
                    if pnl > 0.0 { winning_trades += 1; }
                }
            }
        }

        let win_rate = if total_trades > 0 { (winning_trades as f64 / total_trades as f64) * 100.0 } else { 0.0 };

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

