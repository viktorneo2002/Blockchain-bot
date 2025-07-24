use arrayref::{array_ref, array_refs};
use bytemuck::{Pod, Zeroable};
use solana_program::{
    account_info::AccountInfo,
    program_error::ProgramError,
    program_pack::Pack,
    pubkey::Pubkey,
};
use std::{
    mem::size_of,
    slice::from_raw_parts,
};

#[repr(C)]
#[derive(Clone, Copy, Debug, Default, Pod, Zeroable)]
pub struct TokenAccountData {
    pub mint: Pubkey,
    pub owner: Pubkey,
    pub amount: u64,
    pub delegate_option: u32,
    pub delegate: Pubkey,
    pub state: u8,
    pub is_native_option: u32,
    pub is_native: u64,
    pub delegated_amount: u64,
    pub close_authority_option: u32,
    pub close_authority: Pubkey,
}

#[repr(C)]
#[derive(Clone, Copy, Debug, Default, Pod, Zeroable)]
pub struct MintData {
    pub mint_authority_option: u32,
    pub mint_authority: Pubkey,
    pub supply: u64,
    pub decimals: u8,
    pub is_initialized: bool,
    pub freeze_authority_option: u32,
    pub freeze_authority: Pubkey,
}

#[repr(C)]
#[derive(Clone, Copy, Debug, Default, Pod, Zeroable)]
pub struct MarketStateHeader {
    pub account_flags: u64,
    pub own_address: [u64; 4],
    pub vault_signer_nonce: u64,
    pub base_mint: [u64; 4],
    pub quote_mint: [u64; 4],
    pub base_vault: [u64; 4],
    pub base_deposits_total: u64,
    pub base_fees_accrued: u64,
    pub quote_vault: [u64; 4],
    pub quote_deposits_total: u64,
    pub quote_fees_accrued: u64,
    pub quote_dust_threshold: u64,
    pub request_queue: [u64; 4],
    pub event_queue: [u64; 4],
    pub bids: [u64; 4],
    pub asks: [u64; 4],
    pub base_lot_size: u64,
    pub quote_lot_size: u64,
    pub fee_rate_bps: u64,
    pub referrer_rebate_accrued_total: u64,
}

#[repr(C)]
#[derive(Clone, Copy, Debug, Default, Pod, Zeroable)]
pub struct OrderBookSide {
    pub meta_data: MetaData,
}

#[repr(C)]
#[derive(Clone, Copy, Debug, Default, Pod, Zeroable)]
pub struct MetaData {
    pub bump_index: u32,
    pub free_list_len: u32,
    pub free_list_head: u32,
    pub root: u32,
    pub leaf_count: u32,
}

#[repr(C)]
#[derive(Clone, Copy, Debug, Default, Pod, Zeroable)]
pub struct SlabNode {
    pub tag: u32,
    pub node: SlabNodeData,
}

#[repr(C)]
#[derive(Clone, Copy)]
pub union SlabNodeData {
    pub inner_node: InnerNode,
    pub leaf_node: LeafNode,
}

#[repr(C)]
#[derive(Clone, Copy, Debug, Default, Pod, Zeroable)]
pub struct InnerNode {
    pub prefix_len: u32,
    pub key: u128,
    pub children: [u32; 2],
}

#[repr(C)]
#[derive(Clone, Copy, Debug, Default, Pod, Zeroable)]
pub struct LeafNode {
    pub owner_slot: u8,
    pub fee_tier: u8,
    pub padding: [u8; 2],
    pub key: u128,
    pub owner: [u64; 4],
    pub quantity: u64,
    pub client_order_id: u64,
}

#[repr(C)]
#[derive(Clone, Copy, Debug, Default, Pod, Zeroable)]
pub struct EventQueueHeader {
    pub head: u32,
    pub count: u32,
    pub seq_num: u32,
}

#[repr(C)]
#[derive(Clone, Copy, Debug, Default, Pod, Zeroable)]
pub struct EventNode {
    pub event_flags: u8,
    pub owner_slot: u8,
    pub fee_tier: u8,
    pub padding: [u8; 5],
    pub native_qty_released: u64,
    pub native_qty_paid: u64,
    pub native_fee_or_rebate: u64,
    pub order_id: u128,
    pub owner: [u64; 4],
    pub client_order_id: u64,
}

#[repr(C)]
#[derive(Clone, Copy, Debug, Default, Pod, Zeroable)]
pub struct OpenOrdersAccount {
    pub account_flags: u64,
    pub market: [u64; 4],
    pub owner: [u64; 4],
    pub base_token_free: u64,
    pub base_token_total: u64,
    pub quote_token_free: u64,
    pub quote_token_total: u64,
    pub free_slot_bits: u128,
    pub is_bid_bits: u128,
    pub orders: [u128; 128],
    pub client_ids: [u64; 128],
    pub referrer_rebates_accrued: u64,
}

pub const ORDERBOOK_HEADER_SIZE: usize = size_of::<OrderBookSide>();
pub const SLAB_NODE_SIZE: usize = size_of::<SlabNode>();
pub const EVENT_SIZE: usize = size_of::<EventNode>();
pub const EVENT_QUEUE_HEADER_SIZE: usize = size_of::<EventQueueHeader>();

pub trait ZeroCopyDeserialize: Sized {
    fn deserialize_unchecked(data: &[u8]) -> Result<&Self, ProgramError>;
    fn deserialize_mut_unchecked(data: &mut [u8]) -> Result<&mut Self, ProgramError>;
}

impl ZeroCopyDeserialize for TokenAccountData {
    fn deserialize_unchecked(data: &[u8]) -> Result<&Self, ProgramError> {
        if data.len() < size_of::<Self>() {
            return Err(ProgramError::InvalidAccountData);
        }
        Ok(bytemuck::from_bytes(&data[..size_of::<Self>()]))
    }

    fn deserialize_mut_unchecked(data: &mut [u8]) -> Result<&mut Self, ProgramError> {
        if data.len() < size_of::<Self>() {
            return Err(ProgramError::InvalidAccountData);
        }
        Ok(bytemuck::from_bytes_mut(&mut data[..size_of::<Self>()]))
    }
}

impl ZeroCopyDeserialize for MintData {
    fn deserialize_unchecked(data: &[u8]) -> Result<&Self, ProgramError> {
        if data.len() < size_of::<Self>() {
            return Err(ProgramError::InvalidAccountData);
        }
        Ok(bytemuck::from_bytes(&data[..size_of::<Self>()]))
    }

    fn deserialize_mut_unchecked(data: &mut [u8]) -> Result<&mut Self, ProgramError> {
        if data.len() < size_of::<Self>() {
            return Err(ProgramError::InvalidAccountData);
        }
        Ok(bytemuck::from_bytes_mut(&mut data[..size_of::<Self>()]))
    }
}

impl ZeroCopyDeserialize for MarketStateHeader {
    fn deserialize_unchecked(data: &[u8]) -> Result<&Self, ProgramError> {
        if data.len() < size_of::<Self>() {
            return Err(ProgramError::InvalidAccountData);
        }
        Ok(bytemuck::from_bytes(&data[..size_of::<Self>()]))
    }

    fn deserialize_mut_unchecked(data: &mut [u8]) -> Result<&mut Self, ProgramError> {
        if data.len() < size_of::<Self>() {
            return Err(ProgramError::InvalidAccountData);
        }
        Ok(bytemuck::from_bytes_mut(&mut data[..size_of::<Self>()]))
    }
}

impl ZeroCopyDeserialize for OpenOrdersAccount {
    fn deserialize_unchecked(data: &[u8]) -> Result<&Self, ProgramError> {
        if data.len() < size_of::<Self>() {
            return Err(ProgramError::InvalidAccountData);
        }
        Ok(bytemuck::from_bytes(&data[..size_of::<Self>()]))
    }

    fn deserialize_mut_unchecked(data: &mut [u8]) -> Result<&mut Self, ProgramError> {
        if data.len() < size_of::<Self>() {
            return Err(ProgramError::InvalidAccountData);
        }
        Ok(bytemuck::from_bytes_mut(&mut data[..size_of::<Self>()]))
    }
}

#[inline(always)]
pub fn parse_token_account(account_data: &[u8]) -> Result<&TokenAccountData, ProgramError> {
    TokenAccountData::deserialize_unchecked(account_data)
}

#[inline(always)]
pub fn parse_mint(account_data: &[u8]) -> Result<&MintData, ProgramError> {
    MintData::deserialize_unchecked(account_data)
}

#[inline(always)]
pub fn parse_market_state(account_data: &[u8]) -> Result<&MarketStateHeader, ProgramError> {
    MarketStateHeader::deserialize_unchecked(account_data)
}

#[inline(always)]
pub fn parse_open_orders(account_data: &[u8]) -> Result<&OpenOrdersAccount, ProgramError> {
    OpenOrdersAccount::deserialize_unchecked(account_data)
}

pub struct OrderBookIterator<'a> {
    data: &'a [u8],
    slab_header: &'a OrderBookSide,
    current_index: u32,
    max_nodes: u32,
}

impl<'a> OrderBookIterator<'a> {
    pub fn new(data: &'a [u8]) -> Result<Self, ProgramError> {
        if data.len() < ORDERBOOK_HEADER_SIZE {
            return Err(ProgramError::InvalidAccountData);
        }
        
        let slab_header = bytemuck::from_bytes(&data[..ORDERBOOK_HEADER_SIZE]);
        let max_nodes = ((data.len() - ORDERBOOK_HEADER_SIZE) / SLAB_NODE_SIZE) as u32;
        
        Ok(Self {
            data,
            slab_header,
            current_index: 0,
            max_nodes,
        })
    }

    #[inline(always)]
    fn get_node(&self, index: u32) -> Option<&SlabNode> {
        if index >= self.max_nodes {
            return None;
        }
        
        let offset = ORDERBOOK_HEADER_SIZE + (index as usize * SLAB_NODE_SIZE);
        if offset + SLAB_NODE_SIZE > self.data.len() {
            return None;
        }
        
        Some(bytemuck::from_bytes(&self.data[offset..offset + SLAB_NODE_SIZE]))
    }
}

impl<'a> Iterator for OrderBookIterator<'a> {
    type Item = (u128, u64, Pubkey);

    fn next(&mut self) -> Option<Self::Item> {
        let mut stack = vec![self.slab_header.meta_data.root];
        
        while let Some(node_index) = stack.pop() {
            if node_index == u32::MAX {
                continue;
            }
            
            let node = self.get_node(node_index)?;
            
            match node.tag {
                0 => continue,
                1 => {
                    let inner = unsafe { node.node.inner_node };
                    stack.push(inner.children[1]);
                    stack.push(inner.children[0]);
                }
                2 => {
                    let leaf = unsafe { node.node.leaf_node };
                    let owner_bytes: [u8; 32] = unsafe {
                        std::mem::transmute(leaf.owner)
                    };
                    return Some((leaf.key, leaf.quantity, Pubkey::new_from_array(owner_bytes)));
                }
                _ => continue,
            }
        }
        None
    }
}

pub struct EventQueueIterator<'a> {
    data: &'a [u8],
    header: &'a EventQueueHeader,
    current: u32,
    remaining: u32,
    ring_capacity: u32,
}

impl<'a> EventQueueIterator<'a> {
    pub fn new(data: &'a [u8]) -> Result<Self, ProgramError> {
        if data.len() < EVENT_QUEUE_HEADER_SIZE {
            return Err(ProgramError::InvalidAccountData);
        }
        
        let header = bytemuck::from_bytes(&data[..EVENT_QUEUE_HEADER_SIZE]);
        let ring_capacity = ((data.len() - EVENT_QUEUE_HEADER_SIZE) / EVENT_SIZE) as u32;
        
        Ok(Self {
            data,
            header,
            current: header.head,
            remaining: header.count,
            ring_capacity,
        })
    }

    #[inline(always)]
    fn get_event(&self, index: u32) -> Option<&EventNode> {
        let actual_index = index % self.ring_capacity;
        let offset = EVENT_QUEUE_HEADER_SIZE + (actual_index as usize * EVENT_SIZE);
        
        if offset + EVENT_SIZE > self.data.len() {
            return None;
        }
        
        Some(bytemuck::from_bytes(&self.data[offset..offset + EVENT_SIZE]))
    }
}

impl<'a> Iterator for EventQueueIterator<'a> {
    type Item = &'a EventNode;

    fn next(&mut self) -> Option<Self::Item> {
        if self.remaining == 0 {
            return None;
        }
        
            let event = self.get_event(self.current)?;
        self.current = self.current.wrapping_add(1);
        self.remaining -= 1;
        Some(event)
    }
}

#[repr(C)]
#[derive(Clone, Copy, Debug, Default, Pod, Zeroable)]
pub struct RaydiumAmmInfo {
    pub status: u64,
    pub nonce: u64,
    pub order_num: u64,
    pub depth: u64,
    pub coin_decimals: u64,
    pub pc_decimals: u64,
    pub state: u64,
    pub reset_flag: u64,
    pub min_size: u64,
    pub vol_max_cut_ratio: u64,
    pub amount_wave: u64,
    pub coin_lot_size: u64,
    pub pc_lot_size: u64,
    pub min_price_multiplier: u64,
    pub max_price_multiplier: u64,
    pub sys_decimal_value: u64,
    pub min_separate_numerator: u64,
    pub min_separate_denominator: u64,
    pub trade_fee_numerator: u64,
    pub trade_fee_denominator: u64,
    pub pnl_numerator: u64,
    pub pnl_denominator: u64,
    pub swap_fee_numerator: u64,
    pub swap_fee_denominator: u64,
    pub need_take_pnl_coin: u64,
    pub need_take_pnl_pc: u64,
    pub total_pnl_pc: u64,
    pub total_pnl_coin: u64,
    pub pool_total_deposit_pc: u128,
    pub pool_total_deposit_coin: u128,
    pub swap_coin_in_amount: u128,
    pub swap_pc_out_amount: u128,
    pub swap_coin2_pc_fee: u64,
    pub swap_pc_in_amount: u128,
    pub swap_coin_out_amount: u128,
    pub swap_pc2_coin_fee: u64,
    pub pool_coin_token_account: Pubkey,
    pub pool_pc_token_account: Pubkey,
    pub coin_mint_address: Pubkey,
    pub pc_mint_address: Pubkey,
    pub lp_mint_address: Pubkey,
    pub amm_open_orders: Pubkey,
    pub serum_market: Pubkey,
    pub serum_program_id: Pubkey,
    pub amm_target_orders: Pubkey,
    pub pool_withdraw_queue: Pubkey,
    pub pool_temp_lp_token_account: Pubkey,
    pub amm_owner: Pubkey,
    pub pnl_owner: Pubkey,
}

#[repr(C)]
#[derive(Clone, Copy, Debug, Default, Pod, Zeroable)]
pub struct OrcaWhirlpoolState {
    pub whirlpool_bump: [u8; 1],
    pub tick_spacing: u16,
    pub tick_spacing_seed: [u8; 2],
    pub fee_rate: u16,
    pub protocol_fee_rate: u16,
    pub liquidity: u128,
    pub sqrt_price: u128,
    pub tick_current_index: i32,
    pub protocol_fee_owed_a: u64,
    pub protocol_fee_owed_b: u64,
    pub token_mint_a: Pubkey,
    pub token_vault_a: Pubkey,
    pub fee_growth_global_a: u128,
    pub token_mint_b: Pubkey,
    pub token_vault_b: Pubkey,
    pub fee_growth_global_b: u128,
    pub reward_last_updated_timestamp: u64,
    pub reward_infos: [WhirlpoolRewardInfo; 3],
}

#[repr(C)]
#[derive(Clone, Copy, Debug, Default, Pod, Zeroable)]
pub struct WhirlpoolRewardInfo {
    pub mint: Pubkey,
    pub vault: Pubkey,
    pub authority: Pubkey,
    pub emissions_per_second_x64: u128,
    pub growth_global_x64: u128,
}

#[repr(C)]
#[derive(Clone, Copy, Debug, Default, Pod, Zeroable)]
pub struct JupiterRouteInfo {
    pub input_mint: Pubkey,
    pub output_mint: Pubkey,
    pub in_amount: u64,
    pub out_amount: u64,
    pub route_plan: u64,
    pub other_amount_threshold: u64,
    pub quote_mint: Pubkey,
    pub slippage_bps: u16,
    pub platform_fee_bps: u8,
}

impl ZeroCopyDeserialize for RaydiumAmmInfo {
    fn deserialize_unchecked(data: &[u8]) -> Result<&Self, ProgramError> {
        if data.len() < size_of::<Self>() {
            return Err(ProgramError::InvalidAccountData);
        }
        Ok(bytemuck::from_bytes(&data[..size_of::<Self>()]))
    }

    fn deserialize_mut_unchecked(data: &mut [u8]) -> Result<&mut Self, ProgramError> {
        if data.len() < size_of::<Self>() {
            return Err(ProgramError::InvalidAccountData);
        }
        Ok(bytemuck::from_bytes_mut(&mut data[..size_of::<Self>()]))
    }
}

impl ZeroCopyDeserialize for OrcaWhirlpoolState {
    fn deserialize_unchecked(data: &[u8]) -> Result<&Self, ProgramError> {
        if data.len() < size_of::<Self>() {
            return Err(ProgramError::InvalidAccountData);
        }
        Ok(bytemuck::from_bytes(&data[..size_of::<Self>()]))
    }

    fn deserialize_mut_unchecked(data: &mut [u8]) -> Result<&mut Self, ProgramError> {
        if data.len() < size_of::<Self>() {
            return Err(ProgramError::InvalidAccountData);
        }
        Ok(bytemuck::from_bytes_mut(&mut data[..size_of::<Self>()]))
    }
}

#[inline(always)]
pub fn parse_raydium_amm(account_data: &[u8]) -> Result<&RaydiumAmmInfo, ProgramError> {
    RaydiumAmmInfo::deserialize_unchecked(account_data)
}

#[inline(always)]
pub fn parse_orca_whirlpool(account_data: &[u8]) -> Result<&OrcaWhirlpoolState, ProgramError> {
    OrcaWhirlpoolState::deserialize_unchecked(account_data)
}

#[inline(always)]
pub fn extract_price_from_sqrt(sqrt_price_x64: u128, decimals_a: u8, decimals_b: u8) -> u64 {
    let sqrt_price = sqrt_price_x64 >> 64;
    let price = (sqrt_price * sqrt_price) >> 64;
    let decimal_adj = 10u64.pow((decimals_b - decimals_a) as u32);
    (price * decimal_adj) >> 64
}

#[inline(always)]
pub fn calculate_swap_amounts(
    amount_in: u64,
    reserve_in: u64,
    reserve_out: u64,
    fee_numerator: u64,
    fee_denominator: u64,
) -> u64 {
    if amount_in == 0 || reserve_in == 0 || reserve_out == 0 {
        return 0;
    }
    
    let amount_in_with_fee = (amount_in as u128) * (fee_denominator - fee_numerator) as u128;
    let numerator = amount_in_with_fee * reserve_out as u128;
    let denominator = (reserve_in as u128 * fee_denominator as u128) + amount_in_with_fee;
    
    (numerator / denominator) as u64
}

#[repr(C)]
#[derive(Clone, Copy, Debug, Default)]
pub struct InstructionData {
    pub program_id: Pubkey,
    pub data: [u8; 256],
    pub data_len: u8,
    pub accounts: [Pubkey; 32],
    pub account_count: u8,
}

impl InstructionData {
    #[inline(always)]
    pub fn parse_from_buffer(buffer: &[u8]) -> Result<Self, ProgramError> {
        if buffer.len() < 33 {
            return Err(ProgramError::InvalidInstructionData);
        }
        
        let mut instruction = Self::default();
        instruction.program_id = Pubkey::new_from_array(*array_ref![buffer, 0, 32]);
        
        let data_len = buffer[32] as usize;
        if buffer.len() < 33 + data_len {
            return Err(ProgramError::InvalidInstructionData);
        }
        
        instruction.data_len = data_len as u8;
        instruction.data[..data_len].copy_from_slice(&buffer[33..33 + data_len]);
        
        let account_start = 33 + data_len;
        if buffer.len() < account_start + 1 {
            return Err(ProgramError::InvalidInstructionData);
        }
        
        let account_count = buffer[account_start] as usize;
        instruction.account_count = account_count as u8;
        
        if buffer.len() < account_start + 1 + (account_count * 32) {
            return Err(ProgramError::InvalidInstructionData);
        }
        
        for i in 0..account_count.min(32) {
            let start = account_start + 1 + (i * 32);
            instruction.accounts[i] = Pubkey::new_from_array(*array_ref![buffer, start, 32]);
        }
        
        Ok(instruction)
    }

    #[inline(always)]
    pub fn get_data(&self) -> &[u8] {
        &self.data[..self.data_len as usize]
    }

    #[inline(always)]
    pub fn get_accounts(&self) -> &[Pubkey] {
        &self.accounts[..self.account_count as usize]
    }
}

#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct SwapInstructionData {
    pub amount_in: u64,
    pub minimum_amount_out: u64,
}

impl SwapInstructionData {
    #[inline(always)]
    pub fn unpack(input: &[u8]) -> Result<Self, ProgramError> {
        if input.len() < 16 {
            return Err(ProgramError::InvalidInstructionData);
        }
        
        let amount_in = u64::from_le_bytes(*array_ref![input, 0, 8]);
        let minimum_amount_out = u64::from_le_bytes(*array_ref![input, 8, 8]);
        
        Ok(Self {
            amount_in,
            minimum_amount_out,
        })
    }
}

pub struct FastAccountLoader;

impl FastAccountLoader {
    #[inline(always)]
    pub fn load_token_account<'a>(
        account_info: &'a AccountInfo<'a>,
    ) -> Result<&'a TokenAccountData, ProgramError> {
        if account_info.data_len() < size_of::<TokenAccountData>() {
            return Err(ProgramError::InvalidAccountData);
        }
        
        let data = account_info.try_borrow_data()?;
        TokenAccountData::deserialize_unchecked(&data)
    }

    #[inline(always)]
    pub fn load_mint<'a>(
        account_info: &'a AccountInfo<'a>,
    ) -> Result<&'a MintData, ProgramError> {
        if account_info.data_len() < size_of::<MintData>() {
            return Err(ProgramError::InvalidAccountData);
        }
        
        let data = account_info.try_borrow_data()?;
        MintData::deserialize_unchecked(&data)
    }

    #[inline(always)]
    pub fn load_market_state<'a>(
        account_info: &'a AccountInfo<'a>,
    ) -> Result<&'a MarketStateHeader, ProgramError> {
        if account_info.data_len() < size_of::<MarketStateHeader>() {
            return Err(ProgramError::InvalidAccountData);
        }
        
        let data = account_info.try_borrow_data()?;
        MarketStateHeader::deserialize_unchecked(&data)
    }

    #[inline(always)]
    pub fn load_raydium_amm<'a>(
        account_info: &'a AccountInfo<'a>,
    ) -> Result<&'a RaydiumAmmInfo, ProgramError> {
        if account_info.data_len() < size_of::<RaydiumAmmInfo>() {
            return Err(ProgramError::InvalidAccountData);
        }
        
        let data = account_info.try_borrow_data()?;
        RaydiumAmmInfo::deserialize_unchecked(&data)
    }

    #[inline(always)]
    pub fn load_orca_whirlpool<'a>(
        account_info: &'a AccountInfo<'a>,
    ) -> Result<&'a OrcaWhirlpoolState, ProgramError> {
        if account_info.data_len() < size_of::<OrcaWhirlpoolState>() {
            return Err(ProgramError::InvalidAccountData);
        }
        
        let data = account_info.try_borrow_data()?;
        OrcaWhirlpoolState::deserialize_unchecked(&data)
    }
}

#[inline(always)]
pub fn pubkey_from_u64_array(data: [u64; 4]) -> Pubkey {
    let bytes: [u8; 32] = unsafe { std::mem::transmute(data) };
    Pubkey::new_from_array(bytes)
}

#[inline(always)]
pub fn u64_array_from_pubkey(pubkey: &Pubkey) -> [u64; 4] {
    unsafe { std::mem::transmute(pubkey.to_bytes()) }
}

unsafe impl Pod for SlabNode {}
unsafe impl Zeroable for SlabNode {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_token_account_parsing() {
        let mut data = vec![0u8; size_of::<TokenAccountData>()];
        let token_account = TokenAccountData::deserialize_mut_unchecked(&mut data).unwrap();
        token_account.amount = 1000000;
        
        let parsed = parse_token_account(&data).unwrap();
        assert_eq!(parsed.amount, 1000000);
    }

    #[test]
    fn test_market_state_parsing() {
        let data = vec![0u8; size_of::<MarketStateHeader>()];
        let market = parse_market_state(&data).unwrap();
        assert_eq!(market.base_lot_size, 0);
    }

    #[test]
    fn test_swap_calculation() {
        let amount_out = calculate_swap_amounts(1000000, 100000000, 50000000, 25, 10000);
        assert!(amount_out > 0);
    }
}

#[repr(C)]
#[derive(Clone, Copy, Debug, Default, Pod, Zeroable)]
pub struct SerumMarketV3 {
    pub blob_5: [u8; 5],
    pub account_flags: u64,
    pub own_address: Pubkey,
    pub vault_signer_nonce: u64,
    pub base_mint: Pubkey,
    pub quote_mint: Pubkey,
    pub base_vault: Pubkey,
    pub base_deposits_total: u64,
    pub base_fees_accrued: u64,
    pub quote_vault: Pubkey,
    pub quote_deposits_total: u64,
    pub quote_fees_accrued: u64,
    pub quote_dust_threshold: u64,
    pub request_queue: Pubkey,
    pub event_queue: Pubkey,
    pub bids: Pubkey,
    pub asks: Pubkey,
    pub base_lot_size: u64,
    pub quote_lot_size: u64,
    pub fee_rate_bps: u64,
    pub referrer_rebate_accrued_total: u64,
    pub blob_7: [u8; 7],
}

impl ZeroCopyDeserialize for SerumMarketV3 {
    fn deserialize_unchecked(data: &[u8]) -> Result<&Self, ProgramError> {
        if data.len() < size_of::<Self>() {
            return Err(ProgramError::InvalidAccountData);
        }
        Ok(bytemuck::from_bytes(&data[..size_of::<Self>()]))
    }

    fn deserialize_mut_unchecked(data: &mut [u8]) -> Result<&mut Self, ProgramError> {
        if data.len() < size_of::<Self>() {
            return Err(ProgramError::InvalidAccountData);
        }
        Ok(bytemuck::from_bytes_mut(&mut data[..size_of::<Self>()]))
    }
}

#[inline(always)]
pub fn parse_serum_market(account_data: &[u8]) -> Result<&SerumMarketV3, ProgramError> {
    SerumMarketV3::deserialize_unchecked(account_data)
}

pub struct BatchAccountLoader<'a> {
    accounts: &'a [AccountInfo<'a>],
}

impl<'a> BatchAccountLoader<'a> {
    #[inline(always)]
    pub fn new(accounts: &'a [AccountInfo<'a>]) -> Self {
        Self { accounts }
    }

    #[inline(always)]
    pub fn get_token_account(&self, index: usize) -> Result<&TokenAccountData, ProgramError> {
        if index >= self.accounts.len() {
            return Err(ProgramError::NotEnoughAccountKeys);
        }
        FastAccountLoader::load_token_account(&self.accounts[index])
    }

    #[inline(always)]
    pub fn get_mint(&self, index: usize) -> Result<&MintData, ProgramError> {
        if index >= self.accounts.len() {
            return Err(ProgramError::NotEnoughAccountKeys);
        }
        FastAccountLoader::load_mint(&self.accounts[index])
    }

    #[inline(always)]
    pub fn get_market_state(&self, index: usize) -> Result<&MarketStateHeader, ProgramError> {
        if index >= self.accounts.len() {
            return Err(ProgramError::NotEnoughAccountKeys);
        }
        FastAccountLoader::load_market_state(&self.accounts[index])
    }

    #[inline(always)]
    pub fn get_raydium_amm(&self, index: usize) -> Result<&RaydiumAmmInfo, ProgramError> {
        if index >= self.accounts.len() {
            return Err(ProgramError::NotEnoughAccountKeys);
        }
        FastAccountLoader::load_raydium_amm(&self.accounts[index])
    }

    #[inline(always)]
    pub fn get_serum_market(&self, index: usize) -> Result<&SerumMarketV3, ProgramError> {
        if index >= self.accounts.len() {
            return Err(ProgramError::NotEnoughAccountKeys);
        }
        
        let data = self.accounts[index].try_borrow_data()?;
        SerumMarketV3::deserialize_unchecked(&data)
    }
}

#[inline(always)]
pub fn deserialize_instruction_swap(data: &[u8]) -> Result<(u64, u64), ProgramError> {
    if data.len() < 17 {
        return Err(ProgramError::InvalidInstructionData);
    }
    
    let discriminator = data[0];
    if discriminator != 9 {
        return Err(ProgramError::InvalidInstructionData);
    }
    
    let amount_in = u64::from_le_bytes(*array_ref![data, 1, 8]);
    let minimum_amount_out = u64::from_le_bytes(*array_ref![data, 9, 8]);
    
    Ok((amount_in, minimum_amount_out))
}

#[inline(always)]
pub fn calculate_price_impact_bps(
    amount_in: u64,
    amount_out: u64,
    reserve_in: u64,
    reserve_out: u64,
) -> u16 {
    if amount_in == 0 || amount_out == 0 || reserve_in == 0 || reserve_out == 0 {
        return 0;
    }
    
    let spot_price = (reserve_out as u128 * 10000) / reserve_in as u128;
    let execution_price = (amount_out as u128 * 10000) / amount_in as u128;
    
    if execution_price >= spot_price {
        return 0;
    }
    
    let impact = ((spot_price - execution_price) * 10000) / spot_price;
    impact.min(10000) as u16
}

#[inline(always)]
pub fn verify_account_discriminator(data: &[u8], expected: &[u8; 8]) -> Result<(), ProgramError> {
    if data.len() < 8 {
        return Err(ProgramError::InvalidAccountData);
    }
    
    let discriminator = array_ref![data, 0, 8];
    if discriminator != expected {
        return Err(ProgramError::InvalidAccountData);
    }
    
    Ok(())
}

pub const RAYDIUM_AMM_DISCRIMINATOR: [u8; 8] = [0xe8, 0xf5, 0x5b, 0x2f, 0xba, 0xd8, 0x37, 0xef];
pub const ORCA_WHIRLPOOL_DISCRIMINATOR: [u8; 8] = [0x63, 0xf9, 0x48, 0x11, 0x17, 0xe8, 0x40, 0x4e];
pub const OPENBOOK_MARKET_DISCRIMINATOR: [u8; 8] = [0xb6, 0x1f, 0xa9, 0x3d, 0xd5, 0x4a, 0xc7, 0xd9];

#[inline(always)]
pub fn get_pool_price(pool: &RaydiumAmmInfo) -> u64 {
    if pool.pool_coin_token_account == Pubkey::default() || pool.pool_total_deposit_coin == 0 {
        return 0;
    }
    
    let pc_amount = pool.pool_total_deposit_pc;
    let coin_amount = pool.pool_total_deposit_coin;
    
    if coin_amount == 0 {
        return 0;
    }
    
    ((pc_amount as u128 * 1_000_000_000) / coin_amount as u128) as u64
}

#[inline(always)]
pub fn get_whirlpool_price(pool: &OrcaWhirlpoolState) -> u64 {
    if pool.sqrt_price == 0 {
        return 0;
    }
    
    let price_x64 = (pool.sqrt_price as u128).pow(2) >> 64;
    (price_x64 >> 64) as u64
}

#[repr(C)]
#[derive(Clone, Copy, Debug, Default, Pod, Zeroable)]
pub struct CompactTokenAccount {
    pub amount: u64,
    pub owner: Pubkey,
}

impl CompactTokenAccount {
    #[inline(always)]
    pub fn from_full(account: &TokenAccountData) -> Self {
        Self {
            amount: account.amount,
            owner: account.owner,
        }
    }
}

#[inline(always)]
pub fn unpack_coption<T: Pod>(data: &[u8], offset: usize) -> Result<Option<T>, ProgramError> {
    let flag_offset = offset;
    let data_offset = offset + 4;
    
    if data.len() < data_offset + size_of::<T>() {
        return Err(ProgramError::InvalidAccountData);
    }
    
    let flag = u32::from_le_bytes(*array_ref![data, flag_offset, 4]);
    
    if flag == 0 {
        Ok(None)
    } else if flag == 1 {
        Ok(Some(*bytemuck::from_bytes(&data[data_offset..data_offset + size_of::<T>()])))
    } else {
        Err(ProgramError::InvalidAccountData)
    }
}

#[inline(always)]
pub fn read_pubkey_at_offset(data: &[u8], offset: usize) -> Result<Pubkey, ProgramError> {
    if data.len() < offset + 32 {
        return Err(ProgramError::InvalidAccountData);
    }
    Ok(Pubkey::new_from_array(*array_ref![data, offset, 32]))
}

#[inline(always)]
pub fn read_u64_at_offset(data: &[u8], offset: usize) -> Result<u64, ProgramError> {
    if data.len() < offset + 8 {
        return Err(ProgramError::InvalidAccountData);
    }
    Ok(u64::from_le_bytes(*array_ref![data, offset, 8]))
}

#[inline(always)]
pub fn read_u128_at_offset(data: &[u8], offset: usize) -> Result<u128, ProgramError> {
    if data.len() < offset + 16 {
        return Err(ProgramError::InvalidAccountData);
    }
    Ok(u128::from_le_bytes(*array_ref![data, offset, 16]))
}

pub trait QuickValidate {
    fn validate(&self) -> Result<(), ProgramError>;
}

impl QuickValidate for TokenAccountData {
    #[inline(always)]
    fn validate(&self) -> Result<(), ProgramError> {
        if self.state != 1 {
            return Err(ProgramError::InvalidAccountData);
        }
        Ok(())
    }
}

impl QuickValidate for MintData {
    #[inline(always)]
    fn validate(&self) -> Result<(), ProgramError> {
        if !self.is_initialized {
            return Err(ProgramError::UninitializedAccount);
        }
        Ok(())
    }
}

impl QuickValidate for RaydiumAmmInfo {
    #[inline(always)]
    fn validate(&self) -> Result<(), ProgramError> {
        if self.status != 1 || self.state != 1 {
            return Err(ProgramError::InvalidAccountData);
        }
        Ok(())
    }
}

#[inline(always)]
pub fn parallel_validate_accounts<T: QuickValidate>(accounts: &[T]) -> Result<(), ProgramError> {
    for account in accounts {
        account.validate()?;
    }
    Ok(())
}

pub struct OptimizedDataExtractor;

impl OptimizedDataExtractor {
    #[inline(always)]
    pub fn extract_critical_amm_data(amm: &RaydiumAmmInfo) -> (u128, u128, u64, u64) {
        (
            amm.pool_total_deposit_coin,
            amm.pool_total_deposit_pc,
            amm.trade_fee_numerator,
            amm.trade_fee_denominator,
        )
    }

    #[inline(always)]
    pub fn extract_critical_whirlpool_data(pool: &OrcaWhirlpoolState) -> (u128, u128, u16, i32) {
        (
            pool.liquidity,
            pool.sqrt_price,
            pool.fee_rate,
            pool.tick_current_index,
        )
    }
}

#[inline(always)]
pub fn validate_swap_accounts(
    source_account: &TokenAccountData,
    dest_account: &TokenAccountData,
    expected_source_mint: &Pubkey,
    expected_dest_mint: &Pubkey,
) -> Result<(), ProgramError> {
    if source_account.mint != *expected_source_mint {
        return Err(ProgramError::IncorrectProgramId);
    }
    if dest_account.mint != *expected_dest_mint {
        return Err(ProgramError::IncorrectProgramId);
    }
    source_account.validate()?;
    dest_account.validate()?;
    Ok(())
}
