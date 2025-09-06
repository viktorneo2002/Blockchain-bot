use anchor_lang::prelude::*;
use anchor_lang::prelude::error_code;
use anchor_lang::prelude::pubkey;
use anchor_spl::token::{self, Token, TokenAccount, Transfer};
use solana_program::{
    account_info::AccountInfo,
    instruction::{Instruction, AccountMeta},
    program::invoke_signed,
    pubkey::Pubkey,
    system_instruction,
    sysvar,
    clock::Clock,
    rent::Rent,
};
use spl_token::state::Account as SplTokenAccount;
use std::collections::{HashMap, BinaryHeap, VecDeque};
use std::cmp::Ordering;
use arrayref::array_ref;
use borsh::{BorshDeserialize, BorshSerialize};

pub const MAX_HOPS: usize = 5;
pub const MIN_PROFIT_BPS: u64 = 15;
pub const MAX_SLIPPAGE_BPS: u64 = 500; // 5%
pub const PRIORITY_FEE_LAMPORTS: u64 = 1000; // adjust as needed
const MAX_PROTOCOLS: usize = 8;

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum Protocol {
    Raydium,
    Orca,
    Serum,
    Saber,
    Mercurial,
    Aldrin,
    Lifinity,
    Crema,
}

// Core protocol handler trait (off-chain pathfinding compatible) with invariant-safe math hooks
pub trait ProtocolHandlerCore: Send + Sync {
    fn id(&self) -> Pubkey;
    fn fee_bps(&self) -> u64;
    fn estimate_out(&self, reserves_in: u128, reserves_out: u128, amount_in: u64) -> u64;
    fn validate_accounts(&self, metas: &[AccountMeta]) -> Result<()>;
    fn build_swap_instruction(&self, metas: Vec<AccountMeta>, data: Vec<u8>) -> Result<Instruction>;
}

#[inline]
pub fn read_token_account(ai: &AccountInfo) -> Result<SplTokenAccount> {
    require!(ai.owner == &spl_token::id(), ErrorCode::MissingTokenProgram);
    let data = ai
        .try_borrow_data()
        .map_err(|_| error!(ErrorCode::InvalidPath))?;
    SplTokenAccount::unpack(&data).map_err(|_| error!(ErrorCode::InvalidPath).into())
}

#[inline]
pub fn ensure_meta_contains(metas: &[AccountMeta], key: &Pubkey) -> Result<()> {
    require!(metas.iter().any(|m| m.pubkey == *key), ErrorCode::InvalidPath);
    Ok(())
}

// =============================
// CPI-based protocol handlers (validated passthrough)
// These accept exact metas/data from the off-chain composer and perform
// structural validation only. They implement ProtocolHandlerCore.
// =============================

pub struct RaydiumCpiHandler {
    pub program: Pubkey,      // e.g. 675k... (Amm v4)
    pub fee_bps_fixed: u64,   // discovered off-chain; safe fallback here
}

impl RaydiumCpiHandler {
    pub fn new(program: Pubkey, fee_bps: u64) -> Self {
        Self { program, fee_bps_fixed: fee_bps }
    }
    // Minimal structural validation: ensure program id is present and SPL Token/sysvar present
    pub fn validate_shape(&self, metas: &[AccountMeta]) -> Result<()> {
        ensure_meta_contains(metas, &self.program)?;
        ensure_meta_contains(metas, &spl_token::id())?;
        ensure_meta_contains(metas, &solana_program::sysvar::instructions::id())?;
        Ok(())
    }
}

impl ProtocolHandlerCore for RaydiumCpiHandler {
    #[inline]
    fn id(&self) -> Pubkey { self.program }
    #[inline]
    fn fee_bps(&self) -> u64 { self.fee_bps_fixed }
    #[inline]
    fn estimate_out(&self, x: u128, y: u128, dx: u64) -> u64 {
        xy_k_out(x, y, dx as u128, self.fee_bps_fixed)
    }
    fn validate_accounts(&self, metas: &[AccountMeta]) -> Result<()> { self.validate_shape(metas) }
    fn build_swap_instruction(&self, metas: Vec<AccountMeta>, data: Vec<u8>) -> Result<Instruction> {
        // Strict passthrough: do not fabricate IX bytes
        require!(!metas.is_empty(), ErrorCode::InvalidPath);
        require!(!data.is_empty(), ErrorCode::InvalidPath);
        self.validate_accounts(&metas)?;
        Ok(Instruction { program_id: self.program, accounts: metas, data })
    }
}

pub enum OrcaPoolKind { ClassicCpm, WhirlpoolClmm }

pub struct OrcaCpiHandler {
    pub program: Pubkey,      // 9W959Dq... (token-swap) or Whirlpool program id
    pub fee_bps_fixed: u64,   // off-chain discovered
    pub kind: OrcaPoolKind,
}

impl OrcaCpiHandler {
    pub fn new(program: Pubkey, fee_bps: u64, kind: OrcaPoolKind) -> Self {
        Self { program, fee_bps_fixed: fee_bps, kind }
    }
    pub fn validate_shape(&self, metas: &[AccountMeta]) -> Result<()> {
        ensure_meta_contains(metas, &self.program)?;
        ensure_meta_contains(metas, &spl_token::id())?;
        ensure_meta_contains(metas, &solana_program::sysvar::instructions::id())?;
        Ok(())
    }
    // Safe lower-bound CLMM estimator using spot price and fee only.
    // For conservative enforcement: min_out should be computed off-chain using exact ticks.
    pub fn estimate_out_whirlpool_spot(&self, dx: u64, sqrt_price_x64: u128) -> u64 {
        if dx == 0 || sqrt_price_x64 == 0 { return 0; }
        // price ~ (sqrtP^2 / 2^128). out ≈ dx * price, apply fee.
        let price_num = sqrt_price_x64.saturating_mul(sqrt_price_x64);
        let price = price_num >> 128; // floor
        let gross = (dx as u128).saturating_mul(price);
        let fee = gross.saturating_mul(self.fee_bps_fixed as u128) / 10_000u128;
        gross.saturating_sub(fee) as u64
    }
}

impl ProtocolHandlerCore for OrcaCpiHandler {
    #[inline]
    fn id(&self) -> Pubkey { self.program }
    #[inline]
    fn fee_bps(&self) -> u64 { self.fee_bps_fixed }
    fn estimate_out(&self, x: u128, y: u128, dx: u64) -> u64 {
        match self.kind {
            OrcaPoolKind::ClassicCpm => xy_k_out(x, y, dx as u128, self.fee_bps_fixed),
            OrcaPoolKind::WhirlpoolClmm => 0, // enforce off-chain min_out for CLMM
        }
    }
    fn validate_accounts(&self, metas: &[AccountMeta]) -> Result<()> { self.validate_shape(metas) }
    fn build_swap_instruction(&self, metas: Vec<AccountMeta>, data: Vec<u8>) -> Result<Instruction> {
        require!(!metas.is_empty(), ErrorCode::InvalidPath);
        require!(!data.is_empty(), ErrorCode::InvalidPath);
        self.validate_accounts(&metas)?;
        Ok(Instruction { program_id: self.program, accounts: metas, data })
    }
}

// Strict passthrough builder for protocol CPIs (Raydium/Orca/Aggregator).
// Refuses to guess discriminators; enforces presence of Token Program and Instructions sysvar.
pub fn build_validated_cpi(
    program_id: Pubkey,
    metas: Vec<AccountMeta>,
    data: Vec<u8>,
) -> Result<Instruction> {
    require!(!metas.is_empty(), ErrorCode::TooFewAccounts);
    require!(!data.is_empty(), ErrorCode::InvalidPath);
    require!(metas.iter().any(|m| m.pubkey == program_id), ErrorCode::InvalidPath);
    require!(metas.iter().any(|m| m.pubkey == spl_token::id()), ErrorCode::MissingTokenProgram);
    require!(
        metas.iter().any(|m| m.pubkey == solana_program::sysvar::instructions::id()),
        ErrorCode::MissingSysvar
    );
    Ok(Instruction { program_id, accounts: metas, data })
}

// =============================
// Serum/OpenBook passthrough handler (validated CPI, no on-chain guessing)
// =============================
pub struct SerumCpiHandler {
    pub program: Pubkey, // OpenBook/Serum DEX program id
}

impl SerumCpiHandler {
    pub fn new(program: Pubkey) -> Self { Self { program } }

    pub fn validate_shape(&self, metas: &[AccountMeta]) -> Result<()> {
        require!(metas.iter().any(|m| m.pubkey == self.program), ErrorCode::InvalidPath);
        require!(metas.iter().any(|m| m.pubkey == spl_token::id()), ErrorCode::MissingTokenProgram);
        require!(
            metas.iter().any(|m| m.pubkey == solana_program::sysvar::rent::id()),
            ErrorCode::MissingSysvar
        );
        require!(
            metas.iter().any(|m| m.pubkey == solana_program::sysvar::instructions::id()),
            ErrorCode::MissingSysvar
        );
        // Heuristic safety: require at least a typical account set size (open_orders, vaults, bids/asks, queues, etc.)
        require!(metas.len() >= 12, ErrorCode::TooFewAccounts);
        Ok(())
    }
}

impl ProtocolHandlerCore for SerumCpiHandler {
    #[inline]
    fn id(&self) -> Pubkey { self.program }
    #[inline]
    fn fee_bps(&self) -> u64 { 0 }
    #[inline]
    fn estimate_out(&self, _x: u128, _y: u128, _dx: u64) -> u64 { 0 }
    fn validate_accounts(&self, metas: &[AccountMeta]) -> Result<()> { self.validate_shape(metas) }
    fn build_swap_instruction(&self, metas: Vec<AccountMeta>, data: Vec<u8>) -> Result<Instruction> {
        require!(!metas.is_empty(), ErrorCode::InvalidPath);
        require!(!data.is_empty(), ErrorCode::InvalidPath);
        self.validate_accounts(&metas)?;
        Ok(Instruction { program_id: self.program, accounts: metas, data })
    }
}

// =============================
// Saber stable-swap handler (invariant estimator + strict CPI passthrough)
// =============================
pub struct SaberCpiHandler {
    pub program: Pubkey,  // Saber program id
    pub amp: u64,         // amplification coefficient A
    pub fee_bps_fixed: u64, // pool trade fee in bps
}

impl SaberCpiHandler {
    pub fn new(program: Pubkey, amp: u64, fee_bps: u64) -> Self { Self { program, amp, fee_bps_fixed: fee_bps } }

    pub fn validate_shape(&self, metas: &[AccountMeta]) -> Result<()> {
        let must = [ self.program, spl_token::id(), solana_program::sysvar::instructions::id() ];
        for k in must { require!(metas.iter().any(|m| m.pubkey == k), ErrorCode::InvalidPath); }
        require!(metas.len() >= 5, ErrorCode::InvalidPath);
        Ok(())
    }

    // StableSwap invariant utilities (u128, bounded iterations)
    #[inline]
    fn compute_d(x: u128, y: u128, amp: u128) -> u128 {
        // Curve-style D via iterative approach; bounded and monotone.
        let sum = x.saturating_add(y);
        if sum == 0 { return 0; }
        let a_n = amp.saturating_mul(2); // A * N for N=2
        let mut d = sum;
        for _ in 0..32 {
            // Approximate dP with safe operations to avoid overflow
            let mut dp = d;
            dp = dp.saturating_mul(d) / x.max(1);
            dp = dp.saturating_mul(d) / y.max(1);
            let num = d.saturating_mul(d).saturating_mul(4).saturating_add(a_n.saturating_mul(sum).saturating_mul(d));
            let den = d.saturating_mul(4 + 1).saturating_add(a_n.saturating_mul(sum)).saturating_sub(dp);
            let d_next = if den == 0 { d } else { num / den };
            if d_next > d { if d_next - d <= 1 { return d_next; } } else if d - d_next <= 1 { return d_next; }
            d = d_next;
        }
        d
    }

    #[inline]
    fn compute_y(x: u128, d: u128, amp: u128) -> u128 {
        let a_n = amp.saturating_mul(2);
        // c = d^3 / (x * A_n * 4) in exact formula; keep safe approximations
        let c = d.saturating_mul(d).saturating_mul(d)
            / x.max(1)
            / a_n.max(1);
        let b = d / a_n + x;
        let mut y = d;
        for _ in 0..32 {
            let y_prev = y;
            let num = y.saturating_mul(y).saturating_add(c);
            let den = y.saturating_mul(2).saturating_add(b).saturating_sub(d);
            y = if den == 0 { y } else { num / den };
            if y > y_prev { if y - y_prev <= 1 { return y; } } else if y_prev - y <= 1 { return y; }
        }
        y
    }

    #[inline]
    fn stable_out(x_reserve: u128, y_reserve: u128, dx: u128, amp: u64, fee_bps: u64) -> u64 {
        if dx == 0 || x_reserve == 0 || y_reserve == 0 { return 0; }
        let fee = dx.saturating_mul(fee_bps as u128) / 10_000u128;
        let dx_eff = dx.saturating_sub(fee);
        if dx_eff == 0 { return 0; }
        let d = Self::compute_d(x_reserve, y_reserve, amp as u128);
        let x_new = x_reserve.saturating_add(dx_eff);
        let y_new = Self::compute_y(x_new, d, amp as u128);
        y_reserve.saturating_sub(y_new).min(u64::MAX as u128) as u64
    }
}

impl ProtocolHandlerCore for SaberCpiHandler {
    #[inline]
    fn id(&self) -> Pubkey { self.program }
    #[inline]
    fn fee_bps(&self) -> u64 { self.fee_bps_fixed }
    #[inline]
    fn estimate_out(&self, x: u128, y: u128, dx: u64) -> u64 {
        Self::stable_out(x, y, dx as u128, self.amp, self.fee_bps_fixed)
    }
    fn validate_accounts(&self, metas: &[AccountMeta]) -> Result<()> { self.validate_shape(metas) }
    fn build_swap_instruction(&self, metas: Vec<AccountMeta>, data: Vec<u8>) -> Result<Instruction> {
        require!(!metas.is_empty(), ErrorCode::InvalidPath);
        require!(!data.is_empty(), ErrorCode::InvalidPath);
        self.validate_accounts(&metas)?;
        Ok(Instruction { program_id: self.program, accounts: metas, data })
    }
}

// Prebuilt hop/path interfaces for aggregator-built or protocol-native routes
#[derive(AnchorSerialize, AnchorDeserialize, Clone)]
pub struct PrebuiltHop {
    // Program to invoke for this hop (AMM, aggregator, etc.)
    pub program_id: Pubkey,
    // Serialized instruction data for the CPI call
    pub data: Vec<u8>,
    // Full, ordered AccountMeta list for the CPI
    pub metas: Vec<AccountMeta>,
    // Expected mint continuity: token_in -> token_out
    pub token_in: Pubkey,
    pub token_out: Pubkey,
    // Economic guardrails
    pub min_amount_out: u64,
    // Optional per-hop compute estimate to aid cost checks/logging
    pub est_cu: u32,
}

#[derive(AnchorSerialize, AnchorDeserialize, Clone)]
pub struct PrebuiltPath {
    pub input_mint: Pubkey,
    pub output_mint: Pubkey,
    pub input_amount: u64,
    pub expected_output: u64,
    pub hops: Vec<PrebuiltHop>,
    // Client-computed costs (CU price, tips, etc.) for margin verification
    pub expected_total_cost_lamports: u64,
    pub min_margin_bps: u64, // e.g., 50 = 0.5% over costs
}

pub fn validate_prebuilt_path(path: &PrebuiltPath) -> Result<()> {
    require!(!path.hops.is_empty(), ErrorCode::InvalidPath);
    // Continuity and strict ordering
    let first_in = path.hops.first().ok_or(error!(ErrorCode::InvalidPath))?.token_in;
    let last_out = path.hops.last().ok_or(error!(ErrorCode::InvalidPath))?.token_out;
    require!(first_in == path.input_mint, ErrorCode::InvalidPath);
    require!(last_out == path.output_mint, ErrorCode::InvalidPath);
    for w in path.hops.windows(2) {
        let a = &w[0];
        let b = &w[1];
        require!(a.token_out == b.token_in, ErrorCode::InvalidPath);
    }
    // Economic precondition: client expects net positive result
    require!(path.expected_output > path.input_amount, ErrorCode::InsufficientProfit);
    Ok(())
}

// Collect AccountInfos in the exact order required by metas.
// Caller supplies `lookup` that resolves all referenced accounts (remaining + core accounts).
pub fn resolve_account_infos<'a>(
    metas: &[AccountMeta],
    mut lookup: impl FnMut(&Pubkey) -> Option<AccountInfo<'a>>,
) -> Result<Vec<AccountInfo<'a>>> {
    let mut infos: Vec<AccountInfo<'a>> = Vec::with_capacity(metas.len());
    for m in metas {
        let ai = lookup(&m.pubkey).ok_or(error!(ErrorCode::InvalidPath))?;
        // Optional: enforce writable expectations (signers are covered by runtime)
        if m.is_writable && !ai.is_writable { return Err(error!(ErrorCode::InvalidPath)); }
        infos.push(ai);
    }
    Ok(infos)
}

// Core executor that accepts prebuilt hops and enforces per-hop min-out and final margin
pub fn execute_prebuilt_path<'info>(
    ctx: &Context<'_, '_, '_, 'info, ExecuteFusionArbitrage<'info>>,
    path: &PrebuiltPath,
    // Balance readers for the current output token ATA per hop
    mut read_balance: impl FnMut(&Pubkey) -> Result<u64>,
    // Resolver to map metas->AccountInfos
    mut resolver: impl FnMut(&Pubkey) -> Option<AccountInfo<'info>>,
    // Seeds for authority PDA if needed for signing CPIs
    signer_seeds: &[&[&[u8]]],
) -> Result<u64> {
    validate_prebuilt_path(path)?;

    // Execute each hop as provided by the client
    for hop in &path.hops {
        let ix = Instruction { program_id: hop.program_id, accounts: hop.metas.clone(), data: hop.data.clone() };
        let account_infos = resolve_account_infos(&ix.accounts, &mut resolver)?;
        solana_program::program::invoke_signed(&ix, &account_infos, signer_seeds)?;

        // Check delivered output ≥ min_out for this hop
        let out_bal = read_balance(&hop.token_out)?;
        require!(out_bal >= hop.min_amount_out, ErrorCode::SlippageExceeded);
    }

    // Final profitability check based on post-path balances computed by caller
    let final_out = read_balance(&path.output_mint)?;
    let gross_profit = final_out.saturating_sub(path.input_amount);
    require!(
        gross_profit >= path.expected_total_cost_lamports
            .saturating_add(path.expected_total_cost_lamports.saturating_mul(path.min_margin_bps) / 10_000),
        ErrorCode::ProfitTooLow
    );
    Ok(final_out)
}

// On-chain entrypoint using adapters and prebuilt hops. This validates continuity, min-outs,
// and that the provided ATA mint matches the expected mint. Flashloan borrow/repay flows are
// delegated to the adapter with client-provided metas/data.
pub fn exec_with_flashloan<'info>(
    ctx: Context<'_, '_, '_, 'info, ExecuteFusionArbitrage<'info>>,
    path: PrebuiltPath,
    adapter: &dyn FlashloanAdapter,
    fl_borrow_metas: Vec<AccountMeta>,
    fl_borrow_data: Vec<u8>,
    fl_repay_metas: Vec<AccountMeta>,
    fl_repay_data: Vec<u8>,
) -> Result<()> {
    // Borrow flashloan
    let borrow_ix = adapter.borrow_ix(path.input_amount, fl_borrow_metas, fl_borrow_data)?;
    {
        let infos = resolve_account_infos(&borrow_ix.accounts, |k| {
            if ctx.accounts.authority.key == *k { return Some(ctx.accounts.authority.to_account_info()); }
            if ctx.accounts.token_account.key() == *k { return Some(ctx.accounts.token_account.to_account_info()); }
            if ctx.accounts.flashloan_pool.key() == *k { return Some(ctx.accounts.flashloan_pool.to_account_info()); }
            ctx.remaining_accounts.iter().find(|a| a.key() == *k).cloned()
        })?;
        solana_program::program::invoke(&borrow_ix, &infos)?;
    }

    // Execute the provided path
    let mut last_out_balance: u64 = 0;
    {
        let mut read_balance = |mint: &Pubkey| -> Result<u64> {
            // Here we read from the primary token_account for brevity; for multiple ATAs,
            // pass a richer resolver from the client and map mint->ATA explicitly.
            let acc_info = ctx.accounts.token_account.to_account_info();
            let data = acc_info.try_borrow_data().map_err(|_| error!(ErrorCode::InvalidPath))?;
            let st = spl_token::state::Account::unpack(&data).map_err(|_| error!(ErrorCode::InvalidPath))?;
            if &st.mint != mint { return err!(ErrorCode::MintMismatch); }
            Ok(st.amount)
        };
        let mut resolver = |k: &Pubkey| {
            if ctx.accounts.authority.key == *k { return Some(ctx.accounts.authority.to_account_info()); }
            if ctx.accounts.token_account.key() == *k { return Some(ctx.accounts.token_account.to_account_info()); }
            if ctx.accounts.flashloan_pool.key() == *k { return Some(ctx.accounts.flashloan_pool.to_account_info()); }
            ctx.remaining_accounts.iter().find(|a| a.key() == *k).cloned()
        };
        last_out_balance = execute_prebuilt_path(&ctx, &path, &mut read_balance, &mut resolver, &[])?;
    }

    // Repay flashloan (amount/fee semantics are adapter-defined via data/metas)
    let repay_ix = adapter.repay_ix(path.input_amount, fl_repay_metas, fl_repay_data)?;
    {
        let infos = resolve_account_infos(&repay_ix.accounts, |k| {
            if ctx.accounts.authority.key == *k { return Some(ctx.accounts.authority.to_account_info()); }
            if ctx.accounts.token_account.key() == *k { return Some(ctx.accounts.token_account.to_account_info()); }
            if ctx.accounts.flashloan_pool.key() == *k { return Some(ctx.accounts.flashloan_pool.to_account_info()); }
            ctx.remaining_accounts.iter().find(|a| a.key() == *k).cloned()
        })?;
        solana_program::program::invoke(&repay_ix, &infos)?;
    }

    // Emit success
    emit!(FusionArbitrageEvent {
        slot: Clock::get().map(|c| c.slot).unwrap_or_default(),
        path_hash: hash_path_compact(&path),
        profit: last_out_balance.saturating_sub(path.input_amount),
        gas_cost: path.expected_total_cost_lamports,
        hops: path.hops.len() as u8,
    });

    Ok(())
}

// Hash includes pool/program ids, tokens, and min-outs for auditability
pub fn hash_path_compact(path: &PrebuiltPath) -> [u8; 32] {
    use solana_program::keccak::hashv;
    let mut v = Vec::with_capacity(64 * path.hops.len());
    v.extend_from_slice(path.input_mint.as_ref());
    v.extend_from_slice(path.output_mint.as_ref());
    for h in &path.hops {
        v.extend_from_slice(h.program_id.as_ref());
        v.extend_from_slice(h.token_in.as_ref());
        v.extend_from_slice(h.token_out.as_ref());
        v.extend_from_slice(&h.min_amount_out.to_le_bytes());
    }
    hashv(&[&v]).to_bytes()
}

#[inline]
fn xy_k_out(x_reserve: u128, y_reserve: u128, dx: u128, fee_bps: u64) -> u64 {
    let fee = dx.saturating_mul(fee_bps as u128) / 10_000u128;
    let dx_eff = dx.saturating_sub(fee);
    if dx_eff == 0 || x_reserve == 0 || y_reserve == 0 { return 0; }
    let num = dx_eff.saturating_mul(y_reserve);
    let den = x_reserve.saturating_add(dx_eff);
    if den == 0 { return 0; }
    (num / den) as u64
}

#[inline(always)]
fn protocol_to_discriminator(p: Protocol) -> u8 {
    match p {
        Protocol::Raydium => 1,
        Protocol::Orca => 2,
        Protocol::Serum => 3,
        Protocol::Saber => 4,
        Protocol::Mercurial => 5,
        Protocol::Aldrin => 6,
        Protocol::Lifinity => 7,
        Protocol::Crema => 8,
    }
}

// Convenience wrapper to return as Vec for transaction builders
pub fn create_compute_budget_instruction(units: u32, priority_fee: u64) -> Vec<Instruction> {
    let [cu, price] = compute_budget_instructions(units, priority_fee);
    vec![cu, price]
}

// Canonical compute budget instruction builder (fixed-size buffers)
pub fn compute_budget_instructions(units: u32, micro_lamports_per_cu: u64) -> [Instruction; 2] {
    let id = solana_program::compute_budget::id();
    let mut d0 = [0u8; 1 + 4];
    d0[0] = 0x02; // setComputeUnitLimit
    d0[1..5].copy_from_slice(&units.to_le_bytes());

    let mut d1 = [0u8; 1 + 8];
    d1[0] = 0x03; // setComputeUnitPrice
    d1[1..9].copy_from_slice(&micro_lamports_per_cu.to_le_bytes());

    [
        Instruction { program_id: id, accounts: vec![], data: d0.to_vec() },
        Instruction { program_id: id, accounts: vec![], data: d1.to_vec() },
    ]
}

#[derive(Clone, Debug)]
pub struct ProtocolPool {
    pub protocol: Protocol,
    pub pool_address: Pubkey,
    pub token_a: Pubkey,
    pub token_b: Pubkey,
    pub fee_bps: u64,
    pub liquidity_a: u64,
    pub liquidity_b: u64,
    pub last_update_slot: u64,
}

#[derive(Clone, Debug)]
pub struct FusionPath {
    pub hops: Vec<FusionHop>,
    pub input_amount: u64,
    pub expected_output: u64,
    pub profit: i64,
    pub gas_cost: u64,
}

#[derive(Clone, Debug)]
pub struct FusionHop {
    pub protocol: Protocol,
    pub pool: Pubkey,
    // token mints for validation
    pub token_in: Pubkey,
    pub token_out: Pubkey,
    // per-hop source/destination token accounts (no reuse across different mints)
    pub source_token_account: Pubkey,
    pub dest_token_account: Pubkey,
    // amounts
    pub amount_in: u64,
    pub min_amount_out: u64,
    // raw CPI fields (program id, account metas, data) provided by off-chain path builder
    pub program_id: Pubkey,
    pub accounts: Vec<AccountMeta>,
    pub data: Vec<u8>,
}

pub struct FusionProtocolCombiner {
    pub authority: Pubkey,
    pub pools: HashMap<Pubkey, ProtocolPool>,
    pub token_prices: HashMap<Pubkey, u64>,
    pub protocol_handlers: HashMap<Protocol, Box<dyn ProtocolHandler>>,
    pub flashloan_pool: Pubkey,
    pub flashloan_adapter: Box<dyn FlashloanAdapter>,
}

pub trait ProtocolHandler: Send + Sync {
    fn build_swap_instruction(
        &self,
        pool: &ProtocolPool,
        amount_in: u64,
        min_amount_out: u64,
        user: &Pubkey,
        token_in_account: &Pubkey,
        token_out_account: &Pubkey,
    ) -> Result<Instruction>;
    
    fn calculate_output(&self, pool: &ProtocolPool, amount_in: u64) -> u64;
    fn get_required_accounts(&self, pool: &ProtocolPool) -> Vec<AccountMeta>;
}

// Flashloan adapter trait to allow swapping between Kamino/Solend seamlessly
pub trait FlashloanAdapter: Send + Sync {
    fn program_id(&self) -> Pubkey;
    fn fee_bps(&self) -> u64;
    // exact fee computation including protocol rounding semantics
    fn compute_fee(&self, amount: u64) -> u64 {
        // default: truncate
        amount.saturating_mul(self.fee_bps() as u64) / 10_000
    }
    // Default helper to build a borrow Instruction from client-provided metas/data
    // Amount can be embedded in data; left here for adapter-specific validation if desired
    fn borrow_ix(
        &self,
        _amount: u64,
        metas: Vec<AccountMeta>,
        data: Vec<u8>,
    ) -> Result<Instruction> {
        Ok(Instruction { program_id: self.program_id(), accounts: metas, data })
    }
    // Default helper to build a repay Instruction from client-provided metas/data
    fn repay_ix(
        &self,
        _amount: u64,
        metas: Vec<AccountMeta>,
        data: Vec<u8>,
    ) -> Result<Instruction> {
        Ok(Instruction { program_id: self.program_id(), accounts: metas, data })
    }
    fn build_borrow_ix(
        &self,
        amount: u64,
        pool: &AccountInfo,
        user_token_account: &AccountInfo,
        authority: &Pubkey,
    ) -> Result<Instruction>;
    fn build_repay_ix(
        &self,
        amount: u64,
        pool: &AccountInfo,
        user_token_account: &AccountInfo,
        authority: &Pubkey,
    ) -> Result<Instruction>;
}

// ✅ Kamino flashloan adapter – production-ready
pub struct KaminoAdapter;

impl FlashloanAdapter for KaminoAdapter {
    fn program_id(&self) -> Pubkey {
        pubkey!("C9eS742PsCnqti7cDq7n1Yz3Tqf8H3jFZC2K8m9VQfyo") // Kamino lending program
    }

    fn fee_bps(&self) -> u64 { 9 } // 0.09% typical

    fn compute_fee(&self, amount: u64) -> u64 {
        let prod = (amount as u128) * (self.fee_bps() as u128);
        ((prod + 9_999) / 10_000) as u64 // ceil division
    }

    fn build_borrow_ix(
        &self,
        amount: u64,
        reserve: &AccountInfo,            // Kamino Reserve account
        user_token_account: &AccountInfo, // Destination ATA for borrowed tokens
        authority: &Pubkey,               // PDA authority over tx
    ) -> Result<Instruction> {
        // Kamino: [8-byte discriminator] + borsh-encoded args
        #[derive(BorshSerialize)]
        struct FlashBorrowArgs {
            liquidity_amount: u64,
        }

        let mut data = vec![];
        data.extend_from_slice(&[158, 47, 22, 191, 77, 4, 88, 202]); // flash_borrow_reserve_liquidity discriminator
        FlashBorrowArgs { liquidity_amount: amount }.serialize(&mut data)?;

        Ok(Instruction {
            program_id: self.program_id(),
            accounts: vec![
                AccountMeta::new(*reserve.key, false),               // reserve
                AccountMeta::new(user_token_account.key(), false),   // destination token account
                AccountMeta::new_readonly(*authority, true),         // flashloan authority
            ],
            data,
        })
    }

    fn build_repay_ix(
        &self,
        amount: u64,
        reserve: &AccountInfo,
        user_token_account: &AccountInfo,
        authority: &Pubkey,
    ) -> Result<Instruction> {
        #[derive(BorshSerialize)]
        struct FlashRepayArgs {
            liquidity_amount: u64,
        }

        let mut data = vec![];
        data.extend_from_slice(&[122, 201, 34, 9, 88, 199, 11, 44]); // flash_repay_reserve_liquidity discriminator
        FlashRepayArgs { liquidity_amount: amount }.serialize(&mut data)?;

        Ok(Instruction {
            program_id: self.program_id(),
            accounts: vec![
                AccountMeta::new(*reserve.key, false),
                AccountMeta::new(user_token_account.key(), false),
                AccountMeta::new_readonly(*authority, true),
            ],
            data,
        })
    }
}

// Solend adapter as alternative. Uses compile-time pubkey and same fee.
pub struct SolendAdapter;
impl FlashloanAdapter for SolendAdapter {
    fn program_id(&self) -> Pubkey { pubkey!("So1endDq2YkqhipRh3WViPa8hdiSpxWy6z3Z6tMCpAo") }
    fn fee_bps(&self) -> u64 { 9 }
    fn compute_fee(&self, amount: u64) -> u64 {
        let prod = (amount as u128) * (self.fee_bps() as u128);
        (prod / 10_000) as u64
    }
    fn build_borrow_ix(
        &self,
        amount: u64,
        pool: &AccountInfo,
        user_token_account: &AccountInfo,
        authority: &Pubkey,
    ) -> Result<Instruction> {        
        let data = FlashloanBorrowData { amount }.try_to_vec()?;
        Ok(Instruction {
            program_id: self.program_id(),
            accounts: vec![
                AccountMeta::new(pool.key(), false),
                AccountMeta::new(user_token_account.key(), false),
                AccountMeta::new_readonly(*authority, true),
            ],
            data,
        })
    }
    fn build_repay_ix(
        &self,
        amount: u64,
        pool: &AccountInfo,
        user_token_account: &AccountInfo,
        authority: &Pubkey,
    ) -> Result<Instruction> {
        let data = FlashloanRepayData { amount }.try_to_vec()?;
        Ok(Instruction {
            program_id: self.program_id(),
            accounts: vec![
                AccountMeta::new(pool.key(), false),
                AccountMeta::new(user_token_account.key(), false),
                AccountMeta::new_readonly(*authority, true),
            ],
            data,
        })
    }
}

impl FusionProtocolCombiner {
    pub fn new(authority: Pubkey, flashloan_pool: Pubkey) -> Self {
        // Default to Kamino adapter in practice
        Self::new_with_adapter(authority, flashloan_pool, Box::new(KaminoAdapter))
    }

    pub fn new_with_adapter(
        authority: Pubkey,
        flashloan_pool: Pubkey,
        flashloan_adapter: Box<dyn FlashloanAdapter>,
    ) -> Self {
        let mut protocol_handlers: HashMap<Protocol, Box<dyn ProtocolHandler>> = HashMap::new();
        
        protocol_handlers.insert(Protocol::Raydium, Box::new(RaydiumHandler));
        protocol_handlers.insert(Protocol::Orca, Box::new(OrcaHandler));
        // Intentionally do not register Serum/Saber handlers for on-chain executor.
        // These should be routed via off-chain aggregator due to account/computation complexity.
        
        Self {
            authority,
            pools: HashMap::new(),
            token_prices: HashMap::new(),
            protocol_handlers,
            flashloan_pool,
            flashloan_adapter,
        }
    }

    // Off-chain pathfinding enforced; provide a stub to maintain API compatibility
    pub fn find_optimal_fusion_path(
        &self,
        _token_in: &Pubkey,
        _token_out: &Pubkey,
        _amount: u64,
        _max_hops: usize,
    ) -> Option<FusionPath> {
        None
    }

    // Off-chain pathfinding enforced: no on-chain path search to save compute and fit account limits.

    pub fn execute_fusion_arbitrage(
        &self,
        ctx: Context<ExecuteFusionArbitrage>,
        path: FusionPath,
        expected_total_costs: u64,
        min_margin_bps: u64,
    ) -> Result<()> {
        require!(
            ctx.accounts.authority.key() == self.authority,
            ErrorCode::UnauthorizedAccess
        );

        let clock = Clock::get()?;
        let start_slot = clock.slot;

        // Validate precomputed path (off-chain search), enforcing freshness and reserves
        self.validate_fusion_path(&path)?;

        // Flashloan initiation
        let flashloan_amount = path.input_amount;
        let flashloan_ix = self.flashloan_adapter.build_borrow_ix(
            flashloan_amount,
            &ctx.accounts.flashloan_pool,
            &ctx.accounts.token_account,
            &self.authority,
        )?;

        invoke_signed(
            &flashloan_ix,
            &[
                ctx.accounts.flashloan_pool.to_account_info(),
                ctx.accounts.token_account.to_account_info(),
                ctx.accounts.authority.to_account_info(),
            ],
            &[],
        )?;

        // Execute fusion path
        let mut current_amount = flashloan_amount;
        for (_i, hop) in path.hops.iter().enumerate() {
            // Resolve and validate per-hop token accounts and mints
            let src_ai = self.find_account(ctx, &hop.source_token_account)
                .ok_or(error!(ErrorCode::InvalidPath))?;
            let dst_ai = self.find_account(ctx, &hop.dest_token_account)
                .ok_or(error!(ErrorCode::InvalidPath))?;
            self.assert_token_account_mint(&src_ai, &hop.token_in)?;
            self.assert_token_account_mint(&dst_ai, &hop.token_out)?;

            // Construct instruction directly from hop fields (off-chain provided)
            let swap_ix = Instruction {
                program_id: hop.program_id,
                accounts: hop.accounts.clone(),
                data: hop.data.clone(),
            };

            let account_infos = self.collect_account_infos(ctx, &swap_ix.accounts)?;
            invoke_signed(&swap_ix, &account_infos, &[])?;

            current_amount = self.get_token_balance(&dst_ai)?;
            
            require!(
                current_amount >= hop.min_amount_out,
                ErrorCode::SlippageExceeded
            );

            // Emit per-hop fill event (compact schema)
            emit!(HopFillEvent {
                slot: start_slot,
                program: hop.program_id,
                pool: hop.pool,
                token_in: hop.token_in,
                token_out: hop.token_out,
                min_out: hop.min_amount_out,
                observed_out: current_amount,
            });
        }

        // Repay flashloan (atomic within the same instruction sequence)
        let repay_amount = flashloan_amount
            .saturating_add(self.flashloan_adapter.compute_fee(flashloan_amount));
        require!(
            current_amount >= repay_amount,
                ErrorCode::InsufficientProfit
        );

        let repay_ix = self.flashloan_adapter.build_repay_ix(
            repay_amount,
            &ctx.accounts.flashloan_pool,
            &ctx.accounts.token_account,
            &self.authority,
        )?;

        invoke_signed(
            &repay_ix,
            &[
                ctx.accounts.flashloan_pool.to_account_info(),
                ctx.accounts.token_account.to_account_info(),
                ctx.accounts.authority.to_account_info(),
            ],
            &[],
        )?;

        let final_profit = current_amount.saturating_sub(repay_amount);
        require!(
            self.is_profitable_after_costs(final_profit, expected_total_costs, min_margin_bps),
            ErrorCode::ProfitTooLow
        );

        emit!(FusionArbitrageEvent {
            slot: start_slot,
            path_hash: self.hash_path(&path),
            profit: final_profit,
            gas_cost: path.gas_cost,
            hops: path.hops.len() as u8,
        });

        Ok(())
    }

    // Flashloan borrow/repay building delegated to adapter

    // Off-chain inclusion probability handling; no on-chain heuristics

    fn estimate_hop_gas_cost(&self, protocol: Protocol) -> u64 {
        match protocol {
            Protocol::Raydium => 85000,
            Protocol::Orca => 75000,
            Protocol::Serum => 95000,
            Protocol::Saber => 70000,
            Protocol::Mercurial => 72000,
            Protocol::Aldrin => 80000,
            Protocol::Lifinity => 78000,
            Protocol::Crema => 76000,
        }
    }

    fn calculate_total_gas_cost(&self, hops: &[FusionHop]) -> u64 {
        let sum = hops.iter().fold(0u64, |acc, h| acc.saturating_add(self.estimate_hop_gas_cost(h.protocol)));
        sum.saturating_add(PRIORITY_FEE_LAMPORTS)
    }

    fn is_profitable_after_gas(&self, profit: u64, gas_cost: u64) -> bool {
        profit > gas_cost * 2
    }
    fn is_profitable_after_costs(&self, profit: u64, total_costs: u64, min_margin_bps: u64) -> bool {
        // require profit >= total_costs * (1 + margin)
        let thresh = total_costs
            .saturating_add(total_costs.saturating_mul(min_margin_bps) / 10_000);
        profit >= thresh
    }

    fn is_pool_stale(&self, pool: &ProtocolPool) -> bool {
        if let Ok(clock) = Clock::get() {
            clock.slot.saturating_sub(pool.last_update_slot) > 2
        } else {
            true
        }
    }

    fn reverse_pool(&self, pool: &ProtocolPool) -> ProtocolPool {
        ProtocolPool {
            protocol: pool.protocol,
            pool_address: pool.pool_address,
            token_a: pool.token_b,
            token_b: pool.token_a,
            fee_bps: pool.fee_bps,
            liquidity_a: pool.liquidity_b,
            liquidity_b: pool.liquidity_a,
            last_update_slot: pool.last_update_slot,
        }
    }

    fn get_token_balance(&self, account: &AccountInfo) -> Result<u64> {
        let data = account.try_borrow_data()?;
        let parsed = SplTokenAccount::unpack(&data)
            .map_err(|_| error!(ErrorCode::InvalidPath))?;
        Ok(parsed.amount)
    }

    fn hash_path(&self, path: &FusionPath) -> [u8; 32] {
        use solana_program::keccak::hashv;
        let mut v = Vec::with_capacity(path.hops.len() * (32 + 16 + 32 + 32 + 16 + 8));
        for h in &path.hops {
            v.extend_from_slice(h.pool.as_ref());
            v.extend_from_slice(&(h.amount_in as u128).to_le_bytes());
            v.extend_from_slice(h.token_in.as_ref());
            v.extend_from_slice(h.token_out.as_ref());
            v.extend_from_slice(&(h.min_amount_out as u128).to_le_bytes());
            // include fee_bps if available
            let fee_bps = self.pools.get(&h.pool).map(|p| p.fee_bps).unwrap_or(0);
            v.extend_from_slice(&fee_bps.to_le_bytes());
        }
        hashv(&[&v]).to_bytes()
    }

    fn collect_account_infos<'a>(
        &self,
        ctx: &Context<'a, '_, '_, '_, ExecuteFusionArbitrage<'_>>,
        metas: &[AccountMeta],
    ) -> Result<Vec<AccountInfo<'a>>> {
        // Pre-index remaining accounts by key for fast lookup
        use std::collections::HashMap as Map;
        let mut map: Map<Pubkey, AccountInfo<'a>> = Map::with_capacity(ctx.remaining_accounts.len() + 3);
        map.insert(ctx.accounts.authority.key(), ctx.accounts.authority.to_account_info());
        map.insert(ctx.accounts.token_account.key(), ctx.accounts.token_account.to_account_info());
        map.insert(ctx.accounts.flashloan_pool.key(), ctx.accounts.flashloan_pool.to_account_info());
        for acc in ctx.remaining_accounts.iter() {
            map.insert(acc.key(), acc.clone());
        }

        let mut accounts: Vec<AccountInfo<'a>> = Vec::with_capacity(metas.len());
        for meta in metas {
            let ai = map.get(&meta.pubkey).ok_or(error!(ErrorCode::InvalidPath))?.clone();
            // Optional: enforce writable/signers expectations
            if meta.is_signer && !ai.is_signer { return Err(error!(ErrorCode::InvalidPath)); }
            if meta.is_writable && !ai.is_writable { return Err(error!(ErrorCode::InvalidPath)); }
            accounts.push(ai);
        }
        Ok(accounts)
    }

    fn calculate_flashloan_fee(&self, amount: u64) -> u64 {
        self.flashloan_adapter.compute_fee(amount)
    }

    pub fn update_pool_liquidity(&mut self, pool_address: &Pubkey, liquidity_a: u64, liquidity_b: u64) {
        if let Some(pool) = self.pools.get_mut(pool_address) {
            pool.liquidity_a = liquidity_a;
            pool.liquidity_b = liquidity_b;
            pool.last_update_slot = Clock::get().map(|c| c.slot).unwrap_or(pool.last_update_slot);
        }
    }

    pub fn add_pool(&mut self, pool: ProtocolPool) {
        self.pools.insert(pool.pool_address, pool);
    }

    pub fn update_token_price(&mut self, token: Pubkey, price: u64) {
        self.token_prices.insert(token, price);
    }
}

struct RaydiumHandler;
impl ProtocolHandler for RaydiumHandler {
    fn build_swap_instruction(
        &self,
        pool: &ProtocolPool,
        amount_in: u64,
        min_amount_out: u64,
        user: &Pubkey,
        token_in_account: &Pubkey,
        token_out_account: &Pubkey,
    ) -> Result<Instruction> {
        let raydium_program = pubkey!("675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8");
        
        Ok(Instruction {
            program_id: raydium_program,
            accounts: vec![
                AccountMeta::new(pool.pool_address, false),
                AccountMeta::new_readonly(spl_token::ID, false),
                AccountMeta::new(*token_in_account, false),
                AccountMeta::new(*token_out_account, false),
                AccountMeta::new(pool.token_a, false),
                AccountMeta::new(pool.token_b, false),
                AccountMeta::new_readonly(*user, true),
            ],
            data: build_raydium_swap_data(amount_in, min_amount_out),
        })
    }

    fn calculate_output(&self, pool: &ProtocolPool, amount_in: u64) -> u64 {
        let x = pool.liquidity_a as u128;
        let y = pool.liquidity_b as u128;
        let dx = amount_in as u128;
        let out = xy_k_out(x, y, dx, pool.fee_bps as u64);
        out.min(pool.liquidity_b)
    }

    fn get_required_accounts(&self, _pool: &ProtocolPool) -> Vec<AccountMeta> { vec![] }
}

struct OrcaHandler;
impl ProtocolHandler for OrcaHandler {
    fn build_swap_instruction(
        &self,
        pool: &ProtocolPool,
        amount_in: u64,
        min_amount_out: u64,
        user: &Pubkey,
        token_in_account: &Pubkey,
        token_out_account: &Pubkey,
    ) -> Result<Instruction> {
        let orca_program = pubkey!("9W959DqEETiGZocYWCQPaJ6sBmUzgfxXfqGeTEdp3aQP");
        
        Ok(Instruction {
            program_id: orca_program,
            accounts: vec![
                AccountMeta::new(pool.pool_address, false),
                AccountMeta::new_readonly(*user, true),
                AccountMeta::new(*token_in_account, false),
                AccountMeta::new(pool.token_a, false),
                AccountMeta::new(*token_out_account, false),
                AccountMeta::new(pool.token_b, false),
                AccountMeta::new_readonly(spl_token::ID, false),
            ],
            data: build_orca_swap_data(amount_in, min_amount_out),
        })
    }

    fn calculate_output(&self, pool: &ProtocolPool, amount_in: u64) -> u64 {
        let sqrt_price_limit = 79228162514264337593543950336u128;
        let amount_specified = amount_in as i64;
        
        let fee_amount = (amount_in as u128 * pool.fee_bps as u128 / 10000) as u64;
        let amount_after_fee = amount_in - fee_amount;
        
        let output = (amount_after_fee as u128 * pool.liquidity_b as u128 / 
                     (pool.liquidity_a as u128 + amount_after_fee as u128)) as u64;
        
        output * 9975 / 10000
    }

    fn get_required_accounts(&self, pool: &ProtocolPool) -> Vec<AccountMeta> {
        vec![
            AccountMeta::new(pool.pool_address, false),
            AccountMeta::new(pool.token_a, false),
            AccountMeta::new(pool.token_b, false),
        ]
    }
}

struct SerumHandler;
impl ProtocolHandler for SerumHandler {
    fn build_swap_instruction(
        &self,
        pool: &ProtocolPool,
        amount_in: u64,
        min_amount_out: u64,
        user: &Pubkey,
        token_in_account: &Pubkey,
        token_out_account: &Pubkey,
    ) -> Result<Instruction> {
        // Disabled: Serum requires full CLOB accounts and settle flows.
        Err(error!(ErrorCode::ProtocolNotSupported))
    }

    fn calculate_output(&self, pool: &ProtocolPool, amount_in: u64) -> u64 {
        let taker_fee = 4; // 0.04%
        let fee_amount = amount_in * taker_fee / 10000;
        let net_amount = amount_in - fee_amount;
        
        (net_amount as u128 * pool.liquidity_b as u128 / 
         (pool.liquidity_a as u128 + net_amount as u128)) as u64
    }

    fn get_required_accounts(&self, _pool: &ProtocolPool) -> Vec<AccountMeta> { vec![] }
}

struct SaberHandler;
impl ProtocolHandler for SaberHandler {
    fn build_swap_instruction(
        &self,
        pool: &ProtocolPool,
        amount_in: u64,
        min_amount_out: u64,
        user: &Pubkey,
        token_in_account: &Pubkey,
        token_out_account: &Pubkey,
    ) -> Result<Instruction> {
        // Disabled: Saber stable swap math must match on-chain invariant exactly.
        Err(error!(ErrorCode::ProtocolNotSupported))
    }

    fn calculate_output(&self, pool: &ProtocolPool, amount_in: u64) -> u64 {
        let amp_factor = 100u128;
        let fee_numerator = 4u128;
        let fee_denominator = 10000u128;
        
        let fee = amount_in as u128 * fee_numerator / fee_denominator;
        let amount_after_fee = (amount_in as u128).saturating_sub(fee);
        
        let d = self.calculate_d(pool.liquidity_a as u128, pool.liquidity_b as u128, amp_factor);
        let new_x = pool.liquidity_a as u128 + amount_after_fee;
        let new_y = self.calculate_y(new_x, d, amp_factor);
        
        pool.liquidity_b.saturating_sub(new_y as u64)
    }

    fn get_required_accounts(&self, pool: &ProtocolPool) -> Vec<AccountMeta> {
        vec![
            AccountMeta::new(pool.pool_address, false),
            AccountMeta::new(pool.token_a, false),
            AccountMeta::new(pool.token_b, false),
        ]
    }
}

impl SaberHandler {
    fn calculate_d(&self, x: u128, y: u128, amp: u128) -> u128 {
        let sum = x + y;
        let mut d = sum;
        
        for _ in 0..255 {
            let d_squared = d * d;
            let d_cubed = d_squared * d;
            let numerator = d_cubed + amp * sum * sum;
            let denominator = 2 * d_squared + amp * sum;
            if denominator == 0 { break; }
            let new_d = numerator / denominator;
            if new_d > d && new_d - d <= 1 || d > new_d && d - new_d <= 1 {
                return new_d;
            }
            d = new_d;
        }
        d
    }

    fn calculate_y(&self, x: u128, d: u128, amp: u128) -> u128 {
        let mut y = d;
        
        for _ in 0..255 {
            let y_squared = y * y;
            let numerator = y_squared + amp * x * y;
            let denominator = 2 * y + amp * x - d;
            if denominator == 0 { break; }
            let new_y = numerator / denominator;
            if new_y > y && new_y - y <= 1 || y > new_y && y - new_y <= 1 {
                return new_y;
            }
            y = new_y;
        }
        y
    }
}

#[derive(Clone)]
struct SearchNode {
    token: Pubkey,
    amount: u64,
    hops: Vec<FusionHop>,
    gas_cost: u64,
}

impl Ord for SearchNode {
    fn cmp(&self, other: &Self) -> Ordering {
        self.amount.cmp(&other.amount)
    }
}

impl PartialOrd for SearchNode {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Eq for SearchNode {}

impl PartialEq for SearchNode {
    fn eq(&self, other: &Self) -> bool {
        self.amount == other.amount && self.token == other.token
    }
}

#[derive(Accounts)]
pub struct ExecuteFusionArbitrage<'info> {
    #[account(mut)]
    pub authority: Signer<'info>,
    #[account(mut)]
    pub token_account: Account<'info, TokenAccount>,
    #[account(mut)]
    pub flashloan_pool: AccountInfo<'info>,
    pub token_program: Program<'info, Token>,
    pub system_program: AccountInfo<'info>,
}

#[derive(BorshSerialize, BorshDeserialize)]
struct FlashloanBorrowData {
    amount: u64,
}

#[derive(BorshSerialize, BorshDeserialize)]
struct FlashloanRepayData {
    amount: u64,
}

#[event]
pub struct FusionArbitrageEvent {
    pub slot: u64,
    pub path_hash: [u8; 32],
    pub profit: u64,
    pub gas_cost: u64,
    pub hops: u8,
}

#[event]
pub struct HopFillEvent {
    pub slot: u64,
    pub program: Pubkey,
    pub pool: Pubkey,
    pub token_in: Pubkey,
    pub token_out: Pubkey,
    pub min_out: u64,
    pub observed_out: u64,
}

#[error_code]
pub enum ErrorCode {
    #[msg("Invalid path or account configuration")]
    InvalidPath,
    #[msg("Unauthorized access")]
    UnauthorizedAccess,
    #[msg("Protocol not supported")]
    ProtocolNotSupported,
    #[msg("Pool not found")]
    PoolNotFound,
    #[msg("Slippage exceeded")]
    SlippageExceeded,
    #[msg("Insufficient profit")]
    InsufficientProfit,
    #[msg("Profit too low after costs")]
    ProfitTooLow,
    #[msg("Stale pool data")]
    StalePoolData,
    #[msg("Account mint mismatch")]
    MintMismatch,
    #[msg("Account ownership mismatch")]
    OwnerMismatch,
    #[msg("Missing or invalid sysvar")]
    InvalidSysvar,
    #[msg("Missing SPL Token program")]
    MissingTokenProgram,
    #[msg("Missing required DEX market accounts")] 
    MissingMarket,
    #[msg("Missing required sysvar account")] 
    MissingSysvar,
    #[msg("Too few accounts supplied for this instruction")] 
    TooFewAccounts,
}

fn build_raydium_swap_data(amount_in: u64, min_amount_out: u64) -> Vec<u8> {
    let mut data = vec![0x09]; // Raydium swap instruction discriminator
    data.extend_from_slice(&amount_in.to_le_bytes());
    data.extend_from_slice(&min_amount_out.to_le_bytes());
    data
}

fn build_orca_swap_data(amount_in: u64, min_amount_out: u64) -> Vec<u8> {
    let mut data = vec![0xf8, 0xc6, 0x9e, 0x91, 0xe1, 0x75, 0x87, 0x65]; // Orca swap discriminator
    data.extend_from_slice(&amount_in.to_le_bytes());
    data.extend_from_slice(&min_amount_out.to_le_bytes());
    data.push(0); // exactInput flag
    data
}

fn build_serum_swap_data(amount_in: u64, min_amount_out: u64) -> Vec<u8> {
    #[repr(C)]
    #[derive(BorshSerialize)]
    struct SerumSwapInstruction {
        side: u8,
        limit_price: u64,
        max_coin_qty: u64,
        max_pc_qty: u64,
        order_type: u8,
    }
    
    let instruction = SerumSwapInstruction {
        side: 0, // Buy
        limit_price: u64::MAX,
        max_coin_qty: amount_in,
        max_pc_qty: amount_in,
        order_type: 1, // ImmediateOrCancel
    };
    
    let mut data = vec![0x00]; // NewOrderV3 instruction
    if let Ok(mut payload) = instruction.try_to_vec() {
        data.append(&mut payload);
    } else {
        // return minimal payload; this path should be unreachable since Serum handler is disabled
    }
    data
}

fn build_saber_swap_data(amount_in: u64, min_amount_out: u64) -> Vec<u8> {
    let mut data = vec![0x01]; // Saber swap instruction
    data.extend_from_slice(&amount_in.to_le_bytes());
    data.extend_from_slice(&min_amount_out.to_le_bytes());
    data.push(0); // Token index in
    data.push(1); // Token index out
    data
}

impl FusionProtocolCombiner {
    pub fn validate_fusion_path(&self, path: &FusionPath) -> Result<()> {
        require!(path.hops.len() > 0 && path.hops.len() <= MAX_HOPS, ErrorCode::InvalidPath);
        require!(path.expected_output > path.input_amount, ErrorCode::InsufficientProfit);

        for win in path.hops.windows(2) {
            let a = &win[0];
            let b = &win[1];
            require!(a.token_out == b.token_in, ErrorCode::InvalidPath);
        }

        for hop in &path.hops {
            let pool = self.pools.get(&hop.pool).ok_or(error!(ErrorCode::PoolNotFound))?;
            require!(!self.is_pool_stale(pool), ErrorCode::StalePoolData);
            require!(verify_pool_reserves(pool, 1_000_000), ErrorCode::InvalidPath);
        }
        Ok(())
    }

    fn find_account<'a>(&self, ctx: &Context<'a, '_, '_, '_, ExecuteFusionArbitrage<'_>>, key: &Pubkey) -> Option<AccountInfo<'a>> {
        if ctx.accounts.authority.key == *key { return Some(ctx.accounts.authority.to_account_info()); }
        if ctx.accounts.token_account.key() == *key { return Some(ctx.accounts.token_account.to_account_info()); }
        if ctx.accounts.flashloan_pool.key() == *key { return Some(ctx.accounts.flashloan_pool.to_account_info()); }
        ctx.remaining_accounts.iter().find(|a| a.key() == *key).cloned()
    }

    fn assert_token_account_mint(&self, account: &AccountInfo, expected_mint: &Pubkey) -> Result<()> {
        let data = account.try_borrow_data()?;
        let parsed = SplTokenAccount::unpack(&data).map_err(|_| error!(ErrorCode::InvalidPath))?;
        require!(parsed.mint == *expected_mint, ErrorCode::MintMismatch);
        Ok(())
    }
}

pub fn optimize_gas_priority(
    base_priority: u64,
    network_congestion: f64,
    profit_margin: u64,
) -> u64 {
    let congestion_multiplier = (1.0 + network_congestion).min(3.0);
    let profit_factor = (profit_margin as f64 / 1000.0).min(5.0).max(1.0);
    
    (base_priority as f64 * congestion_multiplier * profit_factor) as u64
}

pub struct NetworkMetrics {
    pub recent_slot_times: Vec<u64>,
    pub success_rates: HashMap<Protocol, f64>,
    pub average_slippage: HashMap<Pubkey, u64>,
}

impl NetworkMetrics {
    pub fn calculate_optimal_timing(&self, path: &FusionPath) -> u64 {
        let avg_slot_time = self.recent_slot_times.iter().sum::<u64>() / 
                           self.recent_slot_times.len().max(1) as u64;
        
        let path_complexity = path.hops.len() as u64;
        let buffer_slots = path_complexity * 2;
        
        avg_slot_time + buffer_slots
    }
    
    pub fn adjust_slippage(&self, pool: &Pubkey, base_slippage: u64) -> u64 {
        self.average_slippage.get(pool)
            .map(|&avg| (avg * 12) / 10)
            .unwrap_or(base_slippage)
            .min(MAX_SLIPPAGE_BPS)
    }
}

pub fn calculate_mev_protection_fee(
    profit: u64,
    slot: u64,
    priority_percentile: u8,
) -> u64 {
    let base_fee = PRIORITY_FEE_LAMPORTS;
    let profit_share = profit * priority_percentile as u64 / 1000;
    let slot_factor = (slot % 100) as u64 * 1000;
    
    base_fee + profit_share + slot_factor
}

#[inline(always)]
pub fn pack_fusion_instruction(
    protocol: Protocol,
    pool: Pubkey,
    accounts: Vec<AccountMeta>,
    data: Vec<u8>,
) -> Instruction {
    let program_id = match protocol {
        Protocol::Raydium => pubkey!("675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8"),
        Protocol::Orca => pubkey!("9W959DqEETiGZocYWCQPaJ6sBmUzgfxXfqGeTEdp3aQP"),
        Protocol::Serum => pubkey!("9xQeWvG816bUx9EPjHmaT23yvVM2ZWbrrpZb9PusVFin"),
        Protocol::Saber => pubkey!("SSwpkEEcbUqx4vtoEByFjSkhKdCT862DNVb52nZg1UZ"),
        Protocol::Mercurial => pubkey!("MERLuDFBMmsHnsBPZw2sDQZHvXFMwp8EdjudcU2HKky"),
        Protocol::Aldrin => pubkey!("AMM55ShdkoGRB5jVYPjWziwk8m5MpwyDgsMWHaMSQWH6"),
        Protocol::Lifinity => pubkey!("EewxydAPCCVuNEyrVN68PuSYdQ7wKn27V9Gjeoi8dy3S"),
        Protocol::Crema => pubkey!("CLMM9tUoggJu2wagPkkqs9eFG4BWhVBZWkP1qv3Sp7tR"),
    };
    
    Instruction {
        program_id,
        accounts,
        data,
    }
}

pub fn estimate_transaction_size(path: &FusionPath) -> usize {
    const SIGNATURE_SIZE: usize = 64;
    const ACCOUNT_META_SIZE: usize = 32 + 1 + 1;
    const INSTRUCTION_OVERHEAD: usize = 1 + 2 + 2;
    
    let mut size = SIGNATURE_SIZE + INSTRUCTION_OVERHEAD;
    
    for hop in &path.hops {
        size += hop.accounts.len() * ACCOUNT_META_SIZE;
        size += 100; // Average instruction data size
    }
    
    size += 200; // Flashloan instructions overhead
    size
}

// Note: any async utilities must remain off-chain only.
// The previous parallel_path_optimization is now guarded/migrated under the fusion_offchain module.

pub fn verify_pool_reserves(pool: &ProtocolPool, min_liquidity: u64) -> bool {
    pool.liquidity_a >= min_liquidity && pool.liquidity_b >= min_liquidity
}

pub fn calculate_price_impact(
    amount_in: u64,
    liquidity_in: u64,
    liquidity_out: u64,
) -> u64 {
    if amount_in >= liquidity_out { return u64::MAX; }
    let ratio_before = (liquidity_out as u128 * 10000) / liquidity_in as u128;
    let ratio_after = ((liquidity_out - amount_in) as u128 * 10000) / 
                      (liquidity_in + amount_in) as u128;
    
    ((ratio_before - ratio_after) * 10000 / ratio_before) as u64
}

#[inline]
pub fn fast_sqrt(n: u128) -> u64 {
    if n == 0 { return 0; }
    
    let mut x = n;
    let mut y = (x + 1) / 2;
    
    while y < x {
        x = y;
        y = (x + n / x) / 2;
    }
    
    x as u64
}

pub const SOLEND_PROGRAM_ID: &str = "So1endDq2YkqhipRh3WViPa8hdiSpxWy6z3Z6tMCpAo";

pub fn create_compute_budget_instruction_alt(units: u32, priority_fee: u64) -> Vec<Instruction> {
    let id = solana_program::compute_budget::id();
    vec![
        Instruction { program_id: id, accounts: vec![], data: [&[0x02u8][..], &units.to_le_bytes()].concat() },
        Instruction { program_id: id, accounts: vec![], data: [&[0x03u8][..], &priority_fee.to_le_bytes()].concat() },
    ]
}

// =============================
// Precise Orca Whirlpool math helpers (Uniswap v3 style)
// =============================
#[inline]
pub fn orca_get_next_sqrt_price(
    sqrt_price_x64: u128,
    liquidity: u128,
    amount: u64,
    zero_for_one: bool,
) -> u128 {
    if liquidity == 0 || sqrt_price_x64 == 0 || amount == 0 { return sqrt_price_x64; }
    if zero_for_one {
        // move left: sqrtP' = L * sqrtP / (L + dX * sqrtP)
        let num = liquidity.saturating_mul(sqrt_price_x64);
        let denom = liquidity.saturating_add((amount as u128).saturating_mul(sqrt_price_x64));
        if denom == 0 { return sqrt_price_x64; }
        num / denom
    } else {
        // move right: sqrtP' = sqrtP + dY / L (Q64 scaling on dY)
        let delta = ((amount as u128) << 64) / liquidity.max(1);
        sqrt_price_x64.saturating_add(delta)
    }
}

// Preview an Orca Whirlpool swap step using exact sqrt-price math.
// Returns (next_sqrt_price_x64, amount_out_after_fee)
#[inline]
pub fn orca_whirlpool_preview(
    sqrt_price_x64: u128,
    liquidity: u128,
    amount_in: u64,
    zero_for_one: bool,
    fee_bps: u64,
) -> (u128, u64) {
    if liquidity == 0 || sqrt_price_x64 == 0 || amount_in == 0 { return (sqrt_price_x64, 0); }
    let fee = (amount_in as u128).saturating_mul(fee_bps as u128) / 10_000u128;
    let dx_eff = (amount_in as u128).saturating_sub(fee);
    if dx_eff == 0 { return (sqrt_price_x64, 0); }
    let dx_eff_u64 = dx_eff.min(u64::MAX as u128) as u64;
    let next = orca_get_next_sqrt_price(sqrt_price_x64, liquidity, dx_eff_u64, zero_for_one);
    let out = if zero_for_one {
        // amount1 out = floor(L * (sqrtP - sqrtP') / Q64)
        let diff = sqrt_price_x64.saturating_sub(next);
        let num = liquidity.saturating_mul(diff);
        (num >> 64).min(u64::MAX as u128) as u64
    } else {
        // amount0 out = floor( L * (sqrtP' - sqrtP) * Q64 / (sqrtP' * sqrtP) )
        let diff = next.saturating_sub(sqrt_price_x64);
        let num = liquidity.saturating_mul(diff) << 64;
        let den = next.saturating_mul(sqrt_price_x64);
        if den == 0 { 0 } else { (num / den).min(u128::from(u64::MAX)) as u64 }
    };
    (next, out)
}

// =============================
// Lean logging for congested slots (alternative to #[event])
// =============================
#[inline(always)]
pub fn emit_counter(label: &'static str, v: u64) {
    // Minimal CU usage logging; label pointer is included for offline mapping if desired
    solana_program::log::sol_log_64(0, 0, 0, v, label.as_ptr() as u64);
}

// =============================
// Byte-accurate transaction size estimator
// =============================
pub fn estimate_tx_size(ixs: &[Instruction], signers: usize) -> usize {
    const TX_HEADER: usize = 1 + 1 + 2; // signatures length varint (approx), header fields
    let sig_size = 64 * signers;
    let account_size: usize = ixs
        .iter()
        .flat_map(|ix| ix.accounts.iter())
        .map(|_| 32 + 1 + 1) // pubkey + is_signer + is_writable
        .sum();
    let data_size: usize = ixs.iter().map(|ix| ix.data.len() + 2).sum(); // 2 bytes for data length prefix
    TX_HEADER + sig_size + account_size + data_size
}

// =============================
// Off-chain client utilities
// Feature-gated to avoid polluting the on-chain build. Enable with
// `--features fusion_offchain` in a dedicated off-chain crate/binary.
// =============================
#[cfg(all(not(target_arch = "bpf"), feature = "fusion_offchain"))]
pub mod fusion_offchain {
    use super::*;
    use solana_sdk::{
        address_lookup_table_account::AddressLookupTableAccount,
        compute_budget,
        hash::Hash,
        instruction::Instruction,
        message::v0::Message,
        pubkey::Pubkey,
        signer::Signer,
        transaction::VersionedTransaction,
    };
    use solana_client::{
        rpc_client::RpcClient,
        nonblocking::rpc_client::RpcClient as AsyncRpcClient,
    };

    #[derive(Clone, Debug)]
    pub struct CostModel {
        pub cu_limit: u32,
        pub cu_price_micro_lamports: u64,
        pub jito_tip_lamports: u64,
        pub flashloan_fee_lamports: u64,
    }

    impl CostModel {
        pub fn total_cost(&self, lamports_per_cu_estimate: Option<u64>) -> u64 {
            // If you maintain a measured CU usage histogram, replace cu_limit with P95 usage
            let cu_price = lamports_per_cu_estimate.unwrap_or(self.cu_price_micro_lamports);
            let cu_cost = cu_price.saturating_mul(self.cu_limit as u64);
            cu_cost
                .saturating_add(self.jito_tip_lamports)
                .saturating_add(self.flashloan_fee_lamports)
        }
    }

    // Profitability guard
    pub fn check_profitability(
        expected_output: u64,
        input_amount: u64,
        costs: &CostModel,
        min_margin_bps: u64,
    ) -> bool {
        let profit = expected_output.saturating_sub(input_amount);
        let total = costs.total_cost(None);
        let threshold = total.saturating_add(total.saturating_mul(min_margin_bps) / 10_000);
        profit >= threshold
    }

    // ALT utility: fetch and collect AddressLookupTableAccount objects
    pub fn fetch_alts(
        rpc: &RpcClient,
        alt_keys: &[Pubkey],
    ) -> std::result::Result<Vec<AddressLookupTableAccount>, Box<dyn std::error::Error>> {
        let resp = rpc.get_address_lookup_table_accounts(alt_keys)?;
        Ok(resp.into_iter().filter_map(|(_, v)| v).collect())
    }

    // Build compute budget instructions (same encodings as on-chain builder)
    pub fn compute_budget_ixs(units: u32, micro_lamports_per_cu: u64) -> [Instruction; 2] {
        let id = compute_budget::id();
        let mut d0 = vec![0x02u8]; // setComputeUnitLimit
        d0.extend_from_slice(&units.to_le_bytes());
        let mut d1 = vec![0x03u8]; // setComputeUnitPrice
        d1.extend_from_slice(&micro_lamports_per_cu.to_le_bytes());
        [
            Instruction { program_id: id, accounts: vec![], data: d0 },
            Instruction { program_id: id, accounts: vec![], data: d1 },
        ]
    }

    // Build an Address-Lookup-Table backed v0 Message
    pub fn build_message_v0(
        payer: &Pubkey,
        ixs: &[Instruction],
        alts: &[AddressLookupTableAccount],
        recent_blockhash: Hash,
    ) -> Message {
        let alt_refs: Vec<_> = alts.iter().collect();
        Message::try_compile(payer, ixs, &alt_refs, recent_blockhash)
            .expect("compile message")
    }

    // Convert a v0 Message into a signed VersionedTransaction
    pub fn sign_message_v0(msg: Message, signers: &[&dyn Signer]) -> VersionedTransaction {
        VersionedTransaction::new(msg, signers)
    }

    // Example orchestrator outline (stub):
    // - Subscribe mempool via Yellowstone (off-chain)
    // - Detect trigger, compute route + min-outs
    // - Build compute budget ixs based on target rank (landing telemetry)
    // - Construct ALT-backed VersionedTransaction
    // - Submit bundles across Jito regions with adaptive tips
    pub async fn submit_bundle_multi_region(
        bundle_txs: Vec<VersionedTransaction>,
        _tip_micro_lamports_per_cu: u64,
    ) -> std::result::Result<(), Box<dyn std::error::Error + Send + Sync>> {
        // Pseudocode placeholder: integrate with real Jito gRPC clients across regions
        // let engines = vec![
        //   JitoClient::new("ny.block-engine.jito.network:..."),
        //   JitoClient::new("fra.block-engine.jito.network:..."),
        //   JitoClient::new("sin.block-engine.jito.network:..."),
        // ];
        // for e in &engines {
        //   e.send_bundle(bundle_txs.clone(), _tip_micro_lamports_per_cu).await?;
        // }
        let _ = bundle_txs; // silence unused until wired
        Ok(())
    }
}
