use solana_program::{
    account_info::{next_account_info, AccountInfo},
    entrypoint,
    entrypoint::ProgramResult,
    msg,
    program::{invoke, invoke_signed},
    program_error::ProgramError,
    program_pack::{IsInitialized, Pack, Sealed},
    pubkey::Pubkey,
    system_instruction,
    sysvar::{clock::Clock, rent::Rent, Sysvar},
};
use borsh::{BorshDeserialize, BorshSerialize};
use arrayref::{array_mut_ref, array_ref, array_refs, mut_array_refs};

const MIN_SIGNERS: usize = 2;
const MAX_SIGNERS: usize = 5;
const TREASURY_SEED: &[u8] = b"mev_treasury_v1";
const TRANSACTION_SEED: &[u8] = b"treasury_tx";
const PROFIT_DISTRIBUTION_BPS: u64 = 10000;
const MIN_PROFITABLE_AMOUNT: u64 = 1_000_000;
const DISTRIBUTION_COOLDOWN: i64 = 3600;

const SIGNER_1: &str = "uBqrrJ9QFyFSXRrHe9sNtju8hzBLsurttEDD5apw1S9";
const SIGNER_2: &str = "44Mdd71QKKKKPctzWL6BNByBmffXuLMAk7arwU59pfmP";

#[derive(BorshSerialize, BorshDeserialize, Debug, Clone, PartialEq)]
pub enum TreasuryInstruction {
    Initialize { signers: Vec<Pubkey>, threshold: u8 },
    ProposeTransaction { amount: u64, recipient: Pubkey, transaction_type: TransactionType },
    ApproveTransaction { transaction_id: u64 },
    ExecuteTransaction { transaction_id: u64 },
    DepositProfits,
    EmergencyWithdraw { amount: u64 },
    UpdateSigners { new_signers: Vec<Pubkey>, new_threshold: u8 },
    DistributeProfits,
}

#[derive(BorshSerialize, BorshDeserialize, Debug, Clone, PartialEq)]
pub enum TransactionType {
    Withdrawal,
    ProfitDistribution,
    EmergencyWithdraw,
    SignerUpdate,
}

#[derive(BorshSerialize, BorshDeserialize, Debug, Clone)]
pub struct TreasuryState {
    pub is_initialized: bool,
    pub signers: Vec<Pubkey>,
    pub threshold: u8,
    pub transaction_counter: u64,
    pub total_deposits: u64,
    pub total_withdrawals: u64,
    pub last_distribution_timestamp: i64,
    pub emergency_mode: bool,
    pub profit_shares: Vec<u16>,
}

#[derive(BorshSerialize, BorshDeserialize, Debug, Clone)]
pub struct PendingTransaction {
    pub is_initialized: bool,
    pub transaction_id: u64,
    pub proposer: Pubkey,
    pub amount: u64,
    pub recipient: Pubkey,
    pub transaction_type: TransactionType,
    pub approvals: Vec<Pubkey>,
    pub executed: bool,
    pub created_at: i64,
    pub expires_at: i64,
}

impl Sealed for TreasuryState {}
impl Sealed for PendingTransaction {}

impl IsInitialized for TreasuryState {
    fn is_initialized(&self) -> bool {
        self.is_initialized
    }
}

impl IsInitialized for PendingTransaction {
    fn is_initialized(&self) -> bool {
        self.is_initialized
    }
}

impl Pack for TreasuryState {
    const LEN: usize = 1 + 32 * MAX_SIGNERS + 1 + 8 + 8 + 8 + 8 + 1 + 2 * MAX_SIGNERS + 128;

    fn unpack_from_slice(src: &[u8]) -> Result<Self, ProgramError> {
        let src = array_ref![src, 0, TreasuryState::LEN];
        let (
            is_initialized,
            signers_data,
            threshold,
            transaction_counter,
            total_deposits,
            total_withdrawals,
            last_distribution_timestamp,
            emergency_mode,
            profit_shares_data,
            _padding,
        ) = array_refs![src, 1, 32 * MAX_SIGNERS, 1, 8, 8, 8, 8, 1, 2 * MAX_SIGNERS, 128];

        let is_initialized = is_initialized[0] != 0;
        let threshold = threshold[0];
        let transaction_counter = u64::from_le_bytes(*transaction_counter);
        let total_deposits = u64::from_le_bytes(*total_deposits);
        let total_withdrawals = u64::from_le_bytes(*total_withdrawals);
        let last_distribution_timestamp = i64::from_le_bytes(*last_distribution_timestamp);
        let emergency_mode = emergency_mode[0] != 0;

        let mut signers = Vec::new();
        for i in 0..MAX_SIGNERS {
            let signer_bytes = array_ref![signers_data, i * 32, 32];
            let signer = Pubkey::new_from_array(*signer_bytes);
            if signer != Pubkey::default() {
                signers.push(signer);
            }
        }

        let mut profit_shares = Vec::new();
        for i in 0..signers.len() {
            let share_bytes = array_ref![profit_shares_data, i * 2, 2];
            profit_shares.push(u16::from_le_bytes(*share_bytes));
        }

        Ok(TreasuryState {
            is_initialized,
            signers,
            threshold,
            transaction_counter,
            total_deposits,
            total_withdrawals,
            last_distribution_timestamp,
            emergency_mode,
            profit_shares,
        })
    }

    fn pack_into_slice(&self, dst: &mut [u8]) {
        let dst = array_mut_ref![dst, 0, TreasuryState::LEN];
        let (
            is_initialized_dst,
            signers_dst,
            threshold_dst,
            transaction_counter_dst,
            total_deposits_dst,
            total_withdrawals_dst,
            last_distribution_timestamp_dst,
            emergency_mode_dst,
            profit_shares_dst,
            _padding_dst,
        ) = mut_array_refs![dst, 1, 32 * MAX_SIGNERS, 1, 8, 8, 8, 8, 1, 2 * MAX_SIGNERS, 128];

        is_initialized_dst[0] = self.is_initialized as u8;
        threshold_dst[0] = self.threshold;
        *transaction_counter_dst = self.transaction_counter.to_le_bytes();
        *total_deposits_dst = self.total_deposits.to_le_bytes();
        *total_withdrawals_dst = self.total_withdrawals.to_le_bytes();
        *last_distribution_timestamp_dst = self.last_distribution_timestamp.to_le_bytes();
        emergency_mode_dst[0] = self.emergency_mode as u8;

        for (i, signer) in self.signers.iter().enumerate() {
            signers_dst[i * 32..(i + 1) * 32].copy_from_slice(&signer.to_bytes());
        }

        for (i, share) in self.profit_shares.iter().enumerate() {
            profit_shares_dst[i * 2..(i + 1) * 2].copy_from_slice(&share.to_le_bytes());
        }
    }
}

impl Pack for PendingTransaction {
    const LEN: usize = 1 + 8 + 32 + 8 + 32 + 1 + 32 * MAX_SIGNERS + 1 + 8 + 8 + 64;

    fn unpack_from_slice(src: &[u8]) -> Result<Self, ProgramError> {
        let src = array_ref![src, 0, PendingTransaction::LEN];
        let (
            is_initialized,
            transaction_id,
            proposer,
            amount,
            recipient,
            transaction_type,
            approvals_data,
            executed,
            created_at,
            expires_at,
            _padding,
        ) = array_refs![src, 1, 8, 32, 8, 32, 1, 32 * MAX_SIGNERS, 1, 8, 8, 64];

        let is_initialized = is_initialized[0] != 0;
        let transaction_id = u64::from_le_bytes(*transaction_id);
        let proposer = Pubkey::new_from_array(*proposer);
        let amount = u64::from_le_bytes(*amount);
        let recipient = Pubkey::new_from_array(*recipient);
        let transaction_type = match transaction_type[0] {
            0 => TransactionType::Withdrawal,
            1 => TransactionType::ProfitDistribution,
            2 => TransactionType::EmergencyWithdraw,
            3 => TransactionType::SignerUpdate,
            _ => return Err(ProgramError::InvalidAccountData),
        };
        let executed = executed[0] != 0;
        let created_at = i64::from_le_bytes(*created_at);
        let expires_at = i64::from_le_bytes(*expires_at);

        let mut approvals = Vec::new();
        for i in 0..MAX_SIGNERS {
            let approval_bytes = array_ref![approvals_data, i * 32, 32];
            let approval = Pubkey::new_from_array(*approval_bytes);
            if approval != Pubkey::default() {
                approvals.push(approval);
            }
        }

        Ok(PendingTransaction {
            is_initialized,
            transaction_id,
            proposer,
            amount,
            recipient,
            transaction_type,
            approvals,
            executed,
            created_at,
            expires_at,
        })
    }

    fn pack_into_slice(&self, dst: &mut [u8]) {
        let dst = array_mut_ref![dst, 0, PendingTransaction::LEN];
        let (
            is_initialized_dst,
            transaction_id_dst,
            proposer_dst,
            amount_dst,
            recipient_dst,
            transaction_type_dst,
            approvals_dst,
            executed_dst,
            created_at_dst,
            expires_at_dst,
            _padding_dst,
        ) = mut_array_refs![dst, 1, 8, 32, 8, 32, 1, 32 * MAX_SIGNERS, 1, 8, 8, 64];

        is_initialized_dst[0] = self.is_initialized as u8;
        *transaction_id_dst = self.transaction_id.to_le_bytes();
        proposer_dst.copy_from_slice(&self.proposer.to_bytes());
        *amount_dst = self.amount.to_le_bytes();
        recipient_dst.copy_from_slice(&self.recipient.to_bytes());
        transaction_type_dst[0] = match self.transaction_type {
            TransactionType::Withdrawal => 0,
            TransactionType::ProfitDistribution => 1,
            TransactionType::EmergencyWithdraw => 2,
            TransactionType::SignerUpdate => 3,
        };
        executed_dst[0] = self.executed as u8;
        *created_at_dst = self.created_at.to_le_bytes();
        *expires_at_dst = self.expires_at.to_le_bytes();

        for (i, approval) in self.approvals.iter().enumerate() {
            approvals_dst[i * 32..(i + 1) * 32].copy_from_slice(&approval.to_bytes());
        }
    }
}

entrypoint!(process_instruction);

pub fn process_instruction(
    program_id: &Pubkey,
    accounts: &[AccountInfo],
    instruction_data: &[u8],
) -> ProgramResult {
    let instruction = TreasuryInstruction::try_from_slice(instruction_data)
        .map_err(|_| ProgramError::InvalidInstructionData)?;

    match instruction {
        TreasuryInstruction::Initialize { signers, threshold } => {
            process_initialize(program_id, accounts, signers, threshold)
        }
        TreasuryInstruction::ProposeTransaction { amount, recipient, transaction_type } => {
            process_propose_transaction(program_id, accounts, amount, recipient, transaction_type)
        }
        TreasuryInstruction::ApproveTransaction { transaction_id } => {
            process_approve_transaction(program_id, accounts, transaction_id)
        }
        TreasuryInstruction::ExecuteTransaction { transaction_id } => {
            process_execute_transaction(program_id, accounts, transaction_id)
        }
        TreasuryInstruction::DepositProfits => {
            process_deposit_profits(program_id, accounts)
        }
        TreasuryInstruction::EmergencyWithdraw { amount } => {
            process_emergency_withdraw(program_id, accounts, amount)
        }
        TreasuryInstruction::UpdateSigners { new_signers, new_threshold } => {
            process_update_signers(program_id, accounts, new_signers, new_threshold)
        }
        TreasuryInstruction::DistributeProfits => {
            process_distribute_profits(program_id, accounts)
        }
    }
}

fn process_initialize(
    program_id: &Pubkey,
    accounts: &[AccountInfo],
    signers: Vec<Pubkey>,
    threshold: u8,
) -> ProgramResult {
    let account_info_iter = &mut accounts.iter();
    let treasury_account = next_account_info(account_info_iter)?;
    let initializer = next_account_info(account_info_iter)?;
    let _system_program = next_account_info(account_info_iter)?;
    let _rent = &Rent::from_account_info(next_account_info(account_info_iter)?)?;

    if !initializer.is_signer {
        return Err(ProgramError::MissingRequiredSignature);
    }

    if treasury_account.owner != program_id {
        return Err(ProgramError::IncorrectProgramId);
    }

        let mut treasury_state = TreasuryState::unpack_unchecked(&treasury_account.data.borrow())?;
    if treasury_state.is_initialized {
        return Err(ProgramError::AccountAlreadyInitialized);
    }

    if signers.len() < MIN_SIGNERS || signers.len() > MAX_SIGNERS {
        return Err(ProgramError::InvalidArgument);
    }

    if threshold as usize > signers.len() || threshold == 0 {
        return Err(ProgramError::InvalidArgument);
    }

    let authorized_signer_1 = Pubkey::new(&bs58::decode(SIGNER_1).into_vec().unwrap());
    let authorized_signer_2 = Pubkey::new(&bs58::decode(SIGNER_2).into_vec().unwrap());
    
    if !signers.contains(&authorized_signer_1) || !signers.contains(&authorized_signer_2) {
        return Err(ProgramError::InvalidArgument);
    }

    let equal_share = (PROFIT_DISTRIBUTION_BPS / signers.len() as u64) as u16;
    let mut profit_shares = vec![equal_share; signers.len()];
    let remainder = (PROFIT_DISTRIBUTION_BPS % signers.len() as u64) as u16;
    if remainder > 0 {
        profit_shares[0] += remainder;
    }

    treasury_state.is_initialized = true;
    treasury_state.signers = signers;
    treasury_state.threshold = threshold;
    treasury_state.transaction_counter = 0;
    treasury_state.total_deposits = 0;
    treasury_state.total_withdrawals = 0;
    treasury_state.last_distribution_timestamp = Clock::get()?.unix_timestamp;
    treasury_state.emergency_mode = false;
    treasury_state.profit_shares = profit_shares;

    TreasuryState::pack(treasury_state, &mut treasury_account.data.borrow_mut())?;
    msg!("Treasury initialized with {} signers, threshold: {}", treasury_state.signers.len(), threshold);
    Ok(())
}

fn process_propose_transaction(
    program_id: &Pubkey,
    accounts: &[AccountInfo],
    amount: u64,
    recipient: Pubkey,
    transaction_type: TransactionType,
) -> ProgramResult {
    let account_info_iter = &mut accounts.iter();
    let treasury_account = next_account_info(account_info_iter)?;
    let transaction_account = next_account_info(account_info_iter)?;
    let proposer = next_account_info(account_info_iter)?;
    let system_program = next_account_info(account_info_iter)?;
    let rent = &Rent::from_account_info(next_account_info(account_info_iter)?)?;
    let clock = &Clock::from_account_info(next_account_info(account_info_iter)?)?;

    if !proposer.is_signer {
        return Err(ProgramError::MissingRequiredSignature);
    }

    let treasury_state = TreasuryState::unpack(&treasury_account.data.borrow())?;
    
    if !treasury_state.signers.contains(proposer.key) {
        return Err(ProgramError::InvalidArgument);
    }

    if treasury_state.emergency_mode && !matches!(transaction_type, TransactionType::EmergencyWithdraw) {
        return Err(ProgramError::InvalidArgument);
    }

    let balance = treasury_account.lamports();
    let rent_exempt_reserve = rent.minimum_balance(treasury_account.data_len());
    let available_balance = balance.saturating_sub(rent_exempt_reserve);

    if amount > available_balance {
        return Err(ProgramError::InsufficientFunds);
    }

    if amount < MIN_PROFITABLE_AMOUNT && matches!(transaction_type, TransactionType::Withdrawal) {
        return Err(ProgramError::InvalidArgument);
    }

    let transaction_id = treasury_state.transaction_counter;
    let (transaction_pda, bump_seed) = Pubkey::find_program_address(
        &[
            TRANSACTION_SEED,
            treasury_account.key.as_ref(),
            &transaction_id.to_le_bytes(),
        ],
        program_id,
    );

    if transaction_pda != *transaction_account.key {
        return Err(ProgramError::InvalidAccountData);
    }

    let space = PendingTransaction::LEN;
    let lamports = rent.minimum_balance(space);

    invoke_signed(
        &system_instruction::create_account(
            proposer.key,
            transaction_account.key,
            lamports,
            space as u64,
            program_id,
        ),
        &[proposer.clone(), transaction_account.clone(), system_program.clone()],
        &[&[
            TRANSACTION_SEED,
            treasury_account.key.as_ref(),
            &transaction_id.to_le_bytes(),
            &[bump_seed],
        ]],
    )?;

    let pending_transaction = PendingTransaction {
        is_initialized: true,
        transaction_id,
        proposer: *proposer.key,
        amount,
        recipient,
        transaction_type,
        approvals: vec![*proposer.key],
        executed: false,
        created_at: clock.unix_timestamp,
        expires_at: clock.unix_timestamp + 86400,
    };

    PendingTransaction::pack(pending_transaction, &mut transaction_account.data.borrow_mut())?;

    let mut treasury_state = TreasuryState::unpack(&treasury_account.data.borrow())?;
    treasury_state.transaction_counter += 1;
    TreasuryState::pack(treasury_state, &mut treasury_account.data.borrow_mut())?;

    msg!("Transaction {} proposed", transaction_id);
    Ok(())
}

fn process_approve_transaction(
    _program_id: &Pubkey,
    accounts: &[AccountInfo],
    transaction_id: u64,
) -> ProgramResult {
    let account_info_iter = &mut accounts.iter();
    let treasury_account = next_account_info(account_info_iter)?;
    let transaction_account = next_account_info(account_info_iter)?;
    let approver = next_account_info(account_info_iter)?;
    let clock = &Clock::from_account_info(next_account_info(account_info_iter)?)?;

    if !approver.is_signer {
        return Err(ProgramError::MissingRequiredSignature);
    }

    let treasury_state = TreasuryState::unpack(&treasury_account.data.borrow())?;
    
    if !treasury_state.signers.contains(approver.key) {
        return Err(ProgramError::InvalidArgument);
    }

    let mut pending_transaction = PendingTransaction::unpack(&transaction_account.data.borrow())?;

    if pending_transaction.executed {
        return Err(ProgramError::InvalidArgument);
    }

    if pending_transaction.expires_at < clock.unix_timestamp {
        return Err(ProgramError::InvalidArgument);
    }

    if pending_transaction.approvals.contains(approver.key) {
        return Err(ProgramError::InvalidArgument);
    }

    pending_transaction.approvals.push(*approver.key);
    PendingTransaction::pack(pending_transaction, &mut transaction_account.data.borrow_mut())?;

    msg!("Transaction {} approved", transaction_id);
    Ok(())
}

fn process_execute_transaction(
    program_id: &Pubkey,
    accounts: &[AccountInfo],
    transaction_id: u64,
) -> ProgramResult {
    let account_info_iter = &mut accounts.iter();
    let treasury_account = next_account_info(account_info_iter)?;
    let transaction_account = next_account_info(account_info_iter)?;
    let recipient_account = next_account_info(account_info_iter)?;
    let executor = next_account_info(account_info_iter)?;
    let system_program = next_account_info(account_info_iter)?;
    let clock = &Clock::from_account_info(next_account_info(account_info_iter)?)?;

    if !executor.is_signer {
        return Err(ProgramError::MissingRequiredSignature);
    }

    let treasury_state = TreasuryState::unpack(&treasury_account.data.borrow())?;
    
    if !treasury_state.signers.contains(executor.key) {
        return Err(ProgramError::InvalidArgument);
    }

    let mut pending_transaction = PendingTransaction::unpack(&transaction_account.data.borrow())?;

    if pending_transaction.executed {
        return Err(ProgramError::InvalidArgument);
    }

    if pending_transaction.expires_at < clock.unix_timestamp {
        return Err(ProgramError::InvalidArgument);
    }

    if pending_transaction.approvals.len() < treasury_state.threshold as usize {
        return Err(ProgramError::InvalidArgument);
    }

    if *recipient_account.key != pending_transaction.recipient {
        return Err(ProgramError::InvalidArgument);
    }

    let (treasury_pda, treasury_bump) = Pubkey::find_program_address(
        &[TREASURY_SEED],
        program_id,
    );

    if treasury_pda == *treasury_account.key {
        invoke_signed(
            &system_instruction::transfer(
                treasury_account.key,
                recipient_account.key,
                pending_transaction.amount,
            ),
            &[treasury_account.clone(), recipient_account.clone(), system_program.clone()],
            &[&[TREASURY_SEED, &[treasury_bump]]],
        )?;
    } else {
        **treasury_account.lamports.borrow_mut() = treasury_account
            .lamports()
            .checked_sub(pending_transaction.amount)
            .ok_or(ProgramError::InsufficientFunds)?;
        **recipient_account.lamports.borrow_mut() = recipient_account
            .lamports()
            .checked_add(pending_transaction.amount)
            .ok_or(ProgramError::InvalidArgument)?;
    }

    pending_transaction.executed = true;
    PendingTransaction::pack(pending_transaction, &mut transaction_account.data.borrow_mut())?;

    let mut treasury_state = TreasuryState::unpack(&treasury_account.data.borrow())?;
    treasury_state.total_withdrawals = treasury_state.total_withdrawals.saturating_add(pending_transaction.amount);
    TreasuryState::pack(treasury_state, &mut treasury_account.data.borrow_mut())?;

    msg!("Transaction {} executed", transaction_id);
    Ok(())
}

fn process_deposit_profits(
    _program_id: &Pubkey,
    accounts: &[AccountInfo],
) -> ProgramResult {
    let account_info_iter = &mut accounts.iter();
    let treasury_account = next_account_info(account_info_iter)?;
    let depositor = next_account_info(account_info_iter)?;
    let system_program = next_account_info(account_info_iter)?;

    if !depositor.is_signer {
        return Err(ProgramError::MissingRequiredSignature);
    }

    let amount = depositor.lamports();
    if amount == 0 {
        return Err(ProgramError::InsufficientFunds);
    }

    invoke(
        &system_instruction::transfer(
            depositor.key,
            treasury_account.key,
            amount,
        ),
        &[depositor.clone(), treasury_account.clone(), system_program.clone()],
    )?;

    let mut treasury_state = TreasuryState::unpack(&treasury_account.data.borrow())?;
    treasury_state.total_deposits = treasury_state.total_deposits.saturating_add(amount);
    TreasuryState::pack(treasury_state, &mut treasury_account.data.borrow_mut())?;

    msg!("Deposited {} lamports", amount);
    Ok(())
}

fn process_emergency_withdraw(
    program_id: &Pubkey,
    accounts: &[AccountInfo],
    amount: u64,
) -> ProgramResult {
    let account_info_iter = &mut accounts.iter();
    let treasury_account = next_account_info(account_info_iter)?;
    let emergency_recipient = next_account_info(account_info_iter)?;
    let signer1 = next_account_info(account_info_iter)?;
    let signer2 = next_account_info(account_info_iter)?;
    let system_program = next_account_info(account_info_iter)?;

    if !signer1.is_signer || !signer2.is_signer {
        return Err(ProgramError::MissingRequiredSignature);
    }

    let authorized_signer_1 = Pubkey::new(&bs58::decode(SIGNER_1).into_vec().unwrap());
    let authorized_signer_2 = Pubkey::new(&bs58::decode(SIGNER_2).into_vec().unwrap());

    if (*signer1.key != authorized_signer_1 || *signer2.key != authorized_signer_2) &&
       (*signer1.key != authorized_signer_2 || *signer2.key != authorized_signer_1) {
        return Err(ProgramError::InvalidArgument);
    }

    let (treasury_pda, treasury_bump) = Pubkey::find_program_address(
        &[TREASURY_SEED],
        program_id,
    );

    if treasury_pda == *treasury_account.key {
        invoke_signed(
            &system_instruction::transfer(
                treasury_account.key,
                emergency_recipient.key,
                amount,
            ),
            &[treasury_account.clone(), emergency_recipient.clone(), system_program.clone()],
            &[&[TREASURY_SEED, &[treasury_bump]]],
        )?;
    } else {
        **treasury_account.lamports.borrow_mut() = treasury_account
            .lamports()
            .checked_sub(amount)
            .ok_or(ProgramError::InsufficientFunds)?;
        **emergency_recipient.lamports.borrow_mut() = emergency_recipient
            .lamports()
            .checked_add(amount)
            .ok_or(ProgramError::InvalidArgument)?;
    }

    let mut treasury_state = TreasuryState::unpack(&treasury_account.data.borrow())?;
    treasury_state.emergency_mode = true;
        treasury_state.total_withdrawals = treasury_state.total_withdrawals.saturating_add(amount);
    TreasuryState::pack(treasury_state, &mut treasury_account.data.borrow_mut())?;

    msg!("Emergency withdrawal of {} lamports", amount);
    Ok(())
}

fn process_update_signers(
    _program_id: &Pubkey,
    accounts: &[AccountInfo],
    new_signers: Vec<Pubkey>,
    new_threshold: u8,
) -> ProgramResult {
    let account_info_iter = &mut accounts.iter();
    let treasury_account = next_account_info(account_info_iter)?;
    let transaction_account = next_account_info(account_info_iter)?;

    let pending_transaction = PendingTransaction::unpack(&transaction_account.data.borrow())?;
    
    if !pending_transaction.executed {
        return Err(ProgramError::InvalidArgument);
    }

    if !matches!(pending_transaction.transaction_type, TransactionType::SignerUpdate) {
        return Err(ProgramError::InvalidArgument);
    }

    if new_signers.len() < MIN_SIGNERS || new_signers.len() > MAX_SIGNERS {
        return Err(ProgramError::InvalidArgument);
    }

    if new_threshold as usize > new_signers.len() || new_threshold == 0 {
        return Err(ProgramError::InvalidArgument);
    }

    let authorized_signer_1 = Pubkey::new(&bs58::decode(SIGNER_1).into_vec().unwrap());
    let authorized_signer_2 = Pubkey::new(&bs58::decode(SIGNER_2).into_vec().unwrap());
    
    if !new_signers.contains(&authorized_signer_1) || !new_signers.contains(&authorized_signer_2) {
        return Err(ProgramError::InvalidArgument);
    }

    let equal_share = (PROFIT_DISTRIBUTION_BPS / new_signers.len() as u64) as u16;
    let mut profit_shares = vec![equal_share; new_signers.len()];
    let remainder = (PROFIT_DISTRIBUTION_BPS % new_signers.len() as u64) as u16;
    if remainder > 0 {
        profit_shares[0] += remainder;
    }

    let mut treasury_state = TreasuryState::unpack(&treasury_account.data.borrow())?;
    treasury_state.signers = new_signers;
    treasury_state.threshold = new_threshold;
    treasury_state.profit_shares = profit_shares;
    TreasuryState::pack(treasury_state, &mut treasury_account.data.borrow_mut())?;

    msg!("Signers updated");
    Ok(())
}

fn process_distribute_profits(
    program_id: &Pubkey,
    accounts: &[AccountInfo],
) -> ProgramResult {
    let account_info_iter = &mut accounts.iter();
    let treasury_account = next_account_info(account_info_iter)?;
    let executor = next_account_info(account_info_iter)?;
    let clock = &Clock::from_account_info(next_account_info(account_info_iter)?)?;
    let rent = &Rent::from_account_info(next_account_info(account_info_iter)?)?;
    let system_program = next_account_info(account_info_iter)?;

    if !executor.is_signer {
        return Err(ProgramError::MissingRequiredSignature);
    }

    let mut treasury_state = TreasuryState::unpack(&treasury_account.data.borrow())?;
    
    if !treasury_state.signers.contains(executor.key) {
        return Err(ProgramError::InvalidArgument);
    }

    if clock.unix_timestamp - treasury_state.last_distribution_timestamp < DISTRIBUTION_COOLDOWN {
        return Err(ProgramError::InvalidArgument);
    }

    let balance = treasury_account.lamports();
    let rent_exempt_reserve = rent.minimum_balance(treasury_account.data_len());
    let distributable_amount = balance.saturating_sub(rent_exempt_reserve);

    if distributable_amount < MIN_PROFITABLE_AMOUNT {
        return Err(ProgramError::InsufficientFunds);
    }

    let signer_accounts: Vec<&AccountInfo> = accounts[5..5 + treasury_state.signers.len()].iter().collect();
    
    for (i, (signer_pubkey, share)) in treasury_state.signers.iter().zip(treasury_state.profit_shares.iter()).enumerate() {
        if i >= signer_accounts.len() {
            break;
        }
        
        let signer_account = signer_accounts[i];
        if signer_account.key != signer_pubkey {
            return Err(ProgramError::InvalidAccountData);
        }

        let distribution_amount = (distributable_amount as u128)
            .checked_mul(*share as u128)
            .ok_or(ProgramError::InvalidArgument)?
            .checked_div(PROFIT_DISTRIBUTION_BPS as u128)
            .ok_or(ProgramError::InvalidArgument)? as u64;

        if distribution_amount > 0 {
            let (treasury_pda, treasury_bump) = Pubkey::find_program_address(
                &[TREASURY_SEED],
                program_id,
            );

            if treasury_pda == *treasury_account.key {
                invoke_signed(
                    &system_instruction::transfer(
                        treasury_account.key,
                        signer_account.key,
                        distribution_amount,
                    ),
                    &[treasury_account.clone(), signer_account.clone(), system_program.clone()],
                    &[&[TREASURY_SEED, &[treasury_bump]]],
                )?;
            } else {
                **treasury_account.lamports.borrow_mut() = treasury_account
                    .lamports()
                    .checked_sub(distribution_amount)
                    .ok_or(ProgramError::InsufficientFunds)?;
                **signer_account.lamports.borrow_mut() = signer_account
                    .lamports()
                    .checked_add(distribution_amount)
                    .ok_or(ProgramError::InvalidArgument)?;
            }
        }
    }

    treasury_state.last_distribution_timestamp = clock.unix_timestamp;
    treasury_state.total_withdrawals = treasury_state.total_withdrawals.saturating_add(distributable_amount);
    TreasuryState::pack(treasury_state, &mut treasury_account.data.borrow_mut())?;

    msg!("Distributed {} lamports", distributable_amount);
    Ok(())
}

#[inline(always)]
pub fn validate_signer_fast(signer: &Pubkey, authorized_signers: &[Pubkey]) -> bool {
    authorized_signers.iter().any(|s| s == signer)
}

#[inline(always)]
pub fn check_sufficient_approvals(approvals: &[Pubkey], threshold: u8, signers: &[Pubkey]) -> bool {
    if approvals.len() < threshold as usize {
        return false;
    }
    approvals.iter().all(|approval| signers.contains(approval))
}

#[inline(always)]
pub fn validate_transaction_timing(created_at: i64, expires_at: i64, current_time: i64) -> bool {
    current_time >= created_at && current_time < expires_at
}

#[inline(always)]
pub fn get_available_balance(account: &AccountInfo, rent_exempt: u64) -> Option<u64> {
    account.lamports().checked_sub(rent_exempt)
}

#[inline(always)]
pub fn calculate_mev_profit_share(total_profit: u64, share_bps: u16) -> Result<u64, ProgramError> {
    let share = (total_profit as u128)
        .checked_mul(share_bps as u128)
        .ok_or(ProgramError::InvalidArgument)?
        .checked_div(PROFIT_DISTRIBUTION_BPS as u128)
        .ok_or(ProgramError::InvalidArgument)?;
    
    if share > u64::MAX as u128 {
        return Err(ProgramError::InvalidArgument);
    }
    
    Ok(share as u64)
}

#[inline(always)]
pub fn validate_mev_transaction(amount: u64, available_balance: u64, min_profitable_amount: u64) -> bool {
    amount > 0 && amount <= available_balance && amount >= min_profitable_amount
}

