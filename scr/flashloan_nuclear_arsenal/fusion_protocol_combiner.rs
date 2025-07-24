use anchor_lang::prelude::*;
use anchor_spl::token::{self, Token, TokenAccount, Transfer};
use solana_program::{
    instruction::Instruction,
    program::invoke_signed,
    system_instruction,
    sysvar,
    clock::Clock,
    rent::Rent,
};
use std::collections::{HashMap, BinaryHeap};
use std::cmp::Ordering;
use arrayref::array_ref;
use borsh::{BorshDeserialize, BorshSerialize};

const MAX_HOPS: usize = 5;
const MIN_PROFIT_BPS: u64 = 15;
const MAX_SLIPPAGE_BPS: u64 = 50;
const PRIORITY_FEE_LAMPORTS: u64 = 50000;
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
    pub success_probability: f64,
}

#[derive(Clone, Debug)]
pub struct FusionHop {
    pub protocol: Protocol,
    pub pool: Pubkey,
    pub token_in: Pubkey,
    pub token_out: Pubkey,
    pub amount_in: u64,
    pub min_amount_out: u64,
    pub accounts: Vec<AccountMeta>,
}

pub struct FusionProtocolCombiner {
    pub authority: Pubkey,
    pub pools: HashMap<Pubkey, ProtocolPool>,
    pub token_prices: HashMap<Pubkey, u64>,
    pub protocol_handlers: HashMap<Protocol, Box<dyn ProtocolHandler>>,
    pub flashloan_pool: Pubkey,
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

impl FusionProtocolCombiner {
    pub fn new(authority: Pubkey, flashloan_pool: Pubkey) -> Self {
        let mut protocol_handlers: HashMap<Protocol, Box<dyn ProtocolHandler>> = HashMap::new();
        
        protocol_handlers.insert(Protocol::Raydium, Box::new(RaydiumHandler));
        protocol_handlers.insert(Protocol::Orca, Box::new(OrcaHandler));
        protocol_handlers.insert(Protocol::Serum, Box::new(SerumHandler));
        protocol_handlers.insert(Protocol::Saber, Box::new(SaberHandler));
        
        Self {
            authority,
            pools: HashMap::new(),
            token_prices: HashMap::new(),
            protocol_handlers,
            flashloan_pool,
        }
    }

    pub fn find_optimal_fusion_path(
        &self,
        token_in: &Pubkey,
        token_out: &Pubkey,
        amount: u64,
        max_hops: usize,
    ) -> Option<FusionPath> {
        let mut heap = BinaryHeap::new();
        let mut best_paths: HashMap<Pubkey, (u64, Vec<FusionHop>)> = HashMap::new();
        
        heap.push(SearchNode {
            token: *token_in,
            amount,
            hops: vec![],
            gas_cost: 0,
        });

        while let Some(node) = heap.pop() {
            if node.hops.len() >= max_hops {
                continue;
            }

            if let Some((best_amount, _)) = best_paths.get(&node.token) {
                if node.amount <= *best_amount {
                    continue;
                }
            }

            best_paths.insert(node.token.clone(), (node.amount, node.hops.clone()));

            for (_, pool) in &self.pools {
                if self.is_pool_stale(pool) {
                    continue;
                }

                let (next_token, amount_out) = if pool.token_a == node.token {
                    let handler = self.protocol_handlers.get(&pool.protocol)?;
                    let output = handler.calculate_output(pool, node.amount);
                    (pool.token_b, output)
                } else if pool.token_b == node.token {
                    let handler = self.protocol_handlers.get(&pool.protocol)?;
                    let reversed_pool = self.reverse_pool(pool);
                    let output = handler.calculate_output(&reversed_pool, node.amount);
                    (pool.token_a, output)
                } else {
                    continue;
                };

                if amount_out == 0 {
                    continue;
                }

                let mut new_hops = node.hops.clone();
                let handler = self.protocol_handlers.get(&pool.protocol)?;
                
                new_hops.push(FusionHop {
                    protocol: pool.protocol,
                    pool: pool.pool_address,
                    token_in: node.token,
                    token_out: next_token,
                    amount_in: node.amount,
                    min_amount_out: amount_out * (10000 - MAX_SLIPPAGE_BPS) / 10000,
                    accounts: handler.get_required_accounts(pool),
                });

                let gas_cost = node.gas_cost + self.estimate_hop_gas_cost(pool.protocol);

                heap.push(SearchNode {
                    token: next_token,
                    amount: amount_out,
                    hops: new_hops,
                    gas_cost,
                });
            }
        }

        if let Some((final_amount, hops)) = best_paths.get(token_out) {
            let profit = (*final_amount as i64) - (amount as i64);
            let gas_cost = self.calculate_total_gas_cost(&hops);
            
            if profit > 0 && self.is_profitable_after_gas(profit as u64, gas_cost) {
                return Some(FusionPath {
                    hops,
                    input_amount: amount,
                    expected_output: *final_amount,
                    profit,
                    gas_cost,
                    success_probability: self.calculate_success_probability(&hops),
                });
            }
        }

        None
    }

    pub fn execute_fusion_arbitrage(
        &self,
        ctx: Context<ExecuteFusionArbitrage>,
        path: FusionPath,
    ) -> Result<()> {
        require!(
            ctx.accounts.authority.key() == self.authority,
            ErrorCode::UnauthorizedAccess
        );

        let clock = Clock::get()?;
        let start_slot = clock.slot;

        // Flashloan initiation
        let flashloan_amount = path.input_amount;
        let flashloan_ix = self.build_flashloan_borrow_instruction(
            flashloan_amount,
            &ctx.accounts.flashloan_pool,
            &ctx.accounts.token_account,
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
        for (i, hop) in path.hops.iter().enumerate() {
            let handler = self.protocol_handlers
                .get(&hop.protocol)
                .ok_or(ErrorCode::ProtocolNotSupported)?;

            let pool = self.pools
                .get(&hop.pool)
                .ok_or(ErrorCode::PoolNotFound)?;

            let swap_ix = handler.build_swap_instruction(
                pool,
                hop.amount_in,
                hop.min_amount_out,
                &ctx.accounts.authority.key(),
                &ctx.accounts.token_account.key(),
                &ctx.accounts.token_account.key(),
            )?;

            let account_infos = self.collect_account_infos(ctx, &hop.accounts)?;
            invoke_signed(&swap_ix, &account_infos, &[])?;

            current_amount = self.get_token_balance(&ctx.accounts.token_account)?;
            
            require!(
                current_amount >= hop.min_amount_out,
                ErrorCode::SlippageExceeded
            );
        }

        // Repay flashloan
        let repay_amount = flashloan_amount + self.calculate_flashloan_fee(flashloan_amount);
        require!(
            current_amount >= repay_amount,
                ErrorCode::InsufficientProfit
        );

        let repay_ix = self.build_flashloan_repay_instruction(
            repay_amount,
            &ctx.accounts.flashloan_pool,
            &ctx.accounts.token_account,
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
            final_profit >= path.input_amount * MIN_PROFIT_BPS / 10000,
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

    fn build_flashloan_borrow_instruction(
        &self,
        amount: u64,
        pool: &AccountInfo,
        token_account: &AccountInfo,
    ) -> Result<Instruction> {
        Ok(Instruction {
            program_id: self.get_flashloan_program_id(),
            accounts: vec![
                AccountMeta::new(pool.key(), false),
                AccountMeta::new(token_account.key(), false),
                AccountMeta::new_readonly(self.authority, true),
            ],
            data: FlashloanBorrowData { amount }.try_to_vec()?,
        })
    }

    fn build_flashloan_repay_instruction(
        &self,
        amount: u64,
        pool: &AccountInfo,
        token_account: &AccountInfo,
    ) -> Result<Instruction> {
        Ok(Instruction {
            program_id: self.get_flashloan_program_id(),
            accounts: vec![
                AccountMeta::new(pool.key(), false),
                AccountMeta::new(token_account.key(), false),
                AccountMeta::new_readonly(self.authority, true),
            ],
            data: FlashloanRepayData { amount }.try_to_vec()?,
        })
    }

    fn calculate_success_probability(&self, hops: &[FusionHop]) -> f64 {
        let base_prob = 0.95;
        let hop_penalty = 0.05;
        (base_prob - (hops.len() as f64 * hop_penalty)).max(0.5)
    }

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
        hops.iter()
            .map(|h| self.estimate_hop_gas_cost(h.protocol))
            .sum::<u64>() + PRIORITY_FEE_LAMPORTS
    }

    fn is_profitable_after_gas(&self, profit: u64, gas_cost: u64) -> bool {
        profit > gas_cost * 2
    }

    fn is_pool_stale(&self, pool: &ProtocolPool) -> bool {
        let clock = Clock::get().unwrap();
        clock.slot - pool.last_update_slot > 2
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
        let token_account = TokenAccount::try_deserialize(&mut &account.data.borrow()[..])?;
        Ok(token_account.amount)
    }

    fn hash_path(&self, path: &FusionPath) -> [u8; 32] {
        use solana_program::keccak::hashv;
        let mut data = Vec::with_capacity(path.hops.len() * 32);
        for hop in &path.hops {
            data.extend_from_slice(hop.pool.as_ref());
        }
        hashv(&[&data]).to_bytes()
    }

    fn collect_account_infos<'a>(
        &self,
        ctx: &Context<'a, '_, '_, '_, ExecuteFusionArbitrage<'_>>,
        metas: &[AccountMeta],
    ) -> Result<Vec<AccountInfo<'a>>> {
        let mut accounts = vec![
            ctx.accounts.authority.to_account_info(),
            ctx.accounts.token_account.to_account_info(),
            ctx.accounts.flashloan_pool.to_account_info(),
        ];
        
        for meta in metas {
            if let Some(account) = ctx.remaining_accounts.iter()
                .find(|a| a.key() == meta.pubkey) {
                accounts.push(account.clone());
            }
        }
        
        Ok(accounts)
    }

    fn calculate_flashloan_fee(&self, amount: u64) -> u64 {
        amount * 9 / 10000 // 0.09% fee
    }

    fn get_flashloan_program_id(&self) -> Pubkey {
        solend_program::ID
    }

    pub fn update_pool_liquidity(&mut self, pool_address: &Pubkey, liquidity_a: u64, liquidity_b: u64) {
        if let Some(pool) = self.pools.get_mut(pool_address) {
            pool.liquidity_a = liquidity_a;
            pool.liquidity_b = liquidity_b;
            pool.last_update_slot = Clock::get().unwrap().slot;
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
        let raydium_program = Pubkey::from_str("675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8").unwrap();
        
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
        let fee_amount = amount_in * pool.fee_bps / 10000;
        let amount_in_after_fee = amount_in - fee_amount;
        
        let numerator = amount_in_after_fee as u128 * pool.liquidity_b as u128;
        let denominator = pool.liquidity_a as u128 + amount_in_after_fee as u128;
        
        (numerator / denominator) as u64
    }

    fn get_required_accounts(&self, pool: &ProtocolPool) -> Vec<AccountMeta> {
        vec![
            AccountMeta::new(pool.pool_address, false),
            AccountMeta::new(pool.token_a, false),
            AccountMeta::new(pool.token_b, false),
        ]
    }
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
        let orca_program = Pubkey::from_str("9W959DqEETiGZocYWCQPaJ6sBmUzgfxXfqGeTEdp3aQP").unwrap();
        
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
        let serum_program = Pubkey::from_str("9xQeWvG816bUx9EPjHmaT23yvVM2ZWbrrpZb9PusVFin").unwrap();
        
        Ok(Instruction {
            program_id: serum_program,
            accounts: vec![
                AccountMeta::new(pool.pool_address, false),
                AccountMeta::new(*token_in_account, false),
                AccountMeta::new(*token_out_account, false),
                AccountMeta::new_readonly(*user, true),
                AccountMeta::new_readonly(spl_token::ID, false),
            ],
            data: build_serum_swap_data(amount_in, min_amount_out),
        })
    }

    fn calculate_output(&self, pool: &ProtocolPool, amount_in: u64) -> u64 {
        let taker_fee = 4; // 0.04%
        let fee_amount = amount_in * taker_fee / 10000;
        let net_amount = amount_in - fee_amount;
        
        (net_amount as u128 * pool.liquidity_b as u128 / 
         (pool.liquidity_a as u128 + net_amount as u128)) as u64
    }

    fn get_required_accounts(&self, pool: &ProtocolPool) -> Vec<AccountMeta> {
        vec![
            AccountMeta::new(pool.pool_address, false),
            AccountMeta::new_readonly(pool.token_a, false),
            AccountMeta::new_readonly(pool.token_b, false),
        ]
    }
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
        let saber_program = Pubkey::from_str("SSwpkEEcbUqx4vtoEByFjSkhKdCT862DNVb52nZg1UZ").unwrap();
        
        Ok(Instruction {
            program_id: saber_program,
            accounts: vec![
                AccountMeta::new(pool.pool_address, false),
                AccountMeta::new_readonly(*user, true),
                AccountMeta::new(*token_in_account, false),
                AccountMeta::new(*token_out_account, false),
                AccountMeta::new(pool.token_a, false),
                AccountMeta::new(pool.token_b, false),
                AccountMeta::new_readonly(spl_token::ID, false),
            ],
            data: build_saber_swap_data(amount_in, min_amount_out),
        })
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

#[error_code]
pub enum ErrorCode {
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
    #[msg("Profit too low")]
    ProfitTooLow,
    #[msg("Invalid path")]
    InvalidPath,
    #[msg("Stale pool data")]
    StalePoolData,
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
    data.extend_from_slice(&instruction.try_to_vec().unwrap());
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

pub fn validate_fusion_path(path: &FusionPath) -> Result<()> {
    require!(
        path.hops.len() > 0 && path.hops.len() <= MAX_HOPS,
        ErrorCode::InvalidPath
    );
    
    require!(
        path.expected_output > path.input_amount,
        ErrorCode::InsufficientProfit
    );
    
    for i in 0..path.hops.len() - 1 {
        require!(
            path.hops[i].token_out == path.hops[i + 1].token_in,
            ErrorCode::InvalidPath
        );
    }
    
    Ok(())
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
        Protocol::Raydium => Pubkey::from_str("675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8").unwrap(),
        Protocol::Orca => Pubkey::from_str("9W959DqEETiGZocYWCQPaJ6sBmUzgfxXfqGeTEdp3aQP").unwrap(),
        Protocol::Serum => Pubkey::from_str("9xQeWvG816bUx9EPjHmaT23yvVM2ZWbrrpZb9PusVFin").unwrap(),
        Protocol::Saber => Pubkey::from_str("SSwpkEEcbUqx4vtoEByFjSkhKdCT862DNVb52nZg1UZ").unwrap(),
        Protocol::Mercurial => Pubkey::from_str("MERLuDFBMmsHnsBPZw2sDQZHvXFMwp8EdjudcU2HKky").unwrap(),
        Protocol::Aldrin => Pubkey::from_str("AMM55ShdkoGRB5jVYPjWziwk8m5MpwyDgsMWHaMSQWH6").unwrap(),
        Protocol::Lifinity => Pubkey::from_str("EewxydAPCCVuNEyrVN68PuSYdQ7wKn27V9Gjeoi8dy3S").unwrap(),
        Protocol::Crema => Pubkey::from_str("CLMM9tUoggJu2wagPkkqs9eFG4BWhVBZWkP1qv3Sp7tR").unwrap(),
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

#[cfg(feature = "concurrent")]
pub async fn parallel_path_optimization(
    combiner: &FusionProtocolCombiner,
    tokens: Vec<(Pubkey, Pubkey)>,
    amount: u64,
) -> Vec<FusionPath> {
    use futures::future::join_all;
    
    let futures = tokens.into_iter().map(|(token_in, token_out)| {
        async move {
            combiner.find_optimal_fusion_path(&token_in, &token_out, amount, MAX_HOPS)
        }
    });
    
    let results = join_all(futures).await;
    results.into_iter().filter_map(|r| r).collect()
}

pub fn verify_pool_reserves(pool: &ProtocolPool, min_liquidity: u64) -> bool {
    pool.liquidity_a >= min_liquidity && pool.liquidity_b >= min_liquidity
}

pub fn calculate_price_impact(
    amount_in: u64,
    liquidity_in: u64,
    liquidity_out: u64,
) -> u64 {
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

pub fn create_compute_budget_instruction(units: u32, priority_fee: u64) -> Vec<Instruction> {
    vec![
        Instruction {
            program_id: Pubkey::from_str("ComputeBudget111111111111111111111111111111").unwrap(),
            accounts: vec![],
            data: vec![0x00, units.to_le_bytes().as_ref(), &[0; 4]].concat(),
        },
        Instruction {
            program_id: Pubkey::from_str("ComputeBudget111111111111111111111111111111").unwrap(),
            accounts: vec![],
            data: vec![0x01, priority_fee.to_le_bytes().as_ref()].concat(),
        },
    ]
}
