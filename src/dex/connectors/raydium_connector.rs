use anchor_lang::prelude::*;
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::{pubkey::Pubkey, account::Account};
use anyhow::{Result, Context};
use crate::analytics::models::SpreadObservation;

#[derive(Debug)]
pub struct Fees {
    pub trade_fee_numerator: u64,
    pub trade_fee_denominator: u64,
    pub owner_trade_fee_numerator: u64,
    pub owner_trade_fee_denominator: u64,
    pub owner_withdraw_fee_numerator: u64,
    pub owner_withdraw_fee_denominator: u64,
    pub host_fee_numerator: u64,
    pub host_fee_denominator: u64,
}

#[derive(Debug)]
pub struct StateData {
    pub data: [u64; 20],
    pub free_slot_bits: u128,
    pub free_slot_head: u8,
    pub buf: [u64; 100],
}

#[derive(Debug)]
pub struct AmmInfo {
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
    pub fees: Fees,
    pub state_data: StateData,
    pub coin_vault: Pubkey,
    pub pc_vault: Pubkey,
    pub coin_vault_mint: Pubkey,
    pub pc_vault_mint: Pubkey,
    pub lp_mint: Pubkey,
    pub open_orders: Pubkey,
    pub market: Pubkey,
    pub market_program: Pubkey,
    pub target_orders: Pubkey,
    pub padding1: [u64; 8],
    pub amm_owner: Pubkey,
    pub lp_amount: u64,
    pub client_order_id: u64,
    pub recent_epoch: u64,
    pub padding2: u64,
    // Additional fields for pool amounts
    pub pool_pc_amount: u64,
    pub pool_coin_amount: u64,
}

pub struct RaydiumConnector {
    rpc_client: RpcClient,
}

impl RaydiumConnector {
    pub fn new(rpc_url: String) -> Self {
        let rpc_client = RpcClient::new(rpc_url);
        Self { rpc_client }
    }

    pub async fn get_amm_price(&self, amm_id: &Pubkey) -> Result<SpreadObservation> {
        let account = self.rpc_client.get_account(amm_id).await
            .context("Failed to fetch AMM account")?;
        
        let amm_info = self.deserialize_amm_info(&account.data)?;
        let price = self.calculate_price(
            amm_info.pool_coin_amount, 
            amm_info.pool_pc_amount, 
            amm_info.coin_decimals, 
            amm_info.pc_decimals
        );
        
        Ok(SpreadObservation {
            timestamp: chrono::Utc::now(),
            price_a: price,
            price_b: price,
            liquidity_a: amm_info.lp_amount as f64,
            liquidity_b: amm_info.lp_amount as f64,
        })
    }
    
    pub async fn get_amm_info(&self, amm_id: &Pubkey) -> Result<AmmInfo> {
        let account = self.rpc_client.get_account(amm_id).await
            .context("Failed to fetch AMM account")?;
        
        self.deserialize_amm_info(&account.data)
    }

    fn deserialize_amm_info(&self, data: &[u8]) -> Result<AmmInfo> {
        if data.len() < 752 {  // Minimum size for AmmInfo struct
            anyhow::bail!("AMM account data too short");
        }
        
        // Skip the first 8 bytes (Anchor discriminator if present)
        let data = &data[8..];
        
        // Parse the AmmInfo struct based on actual layout from Raydium AMM program
        let status = u64::from_le_bytes([data[0], data[1], data[2], data[3], data[4], data[5], data[6], data[7]]);
        let nonce = u64::from_le_bytes([data[8], data[9], data[10], data[11], data[12], data[13], data[14], data[15]]);
        let order_num = u64::from_le_bytes([data[16], data[17], data[18], data[19], data[20], data[21], data[22], data[23]]);
        let depth = u64::from_le_bytes([data[24], data[25], data[26], data[27], data[28], data[29], data[30], data[31]]);
        let coin_decimals = u64::from_le_bytes([data[32], data[33], data[34], data[35], data[36], data[37], data[38], data[39]]);
        let pc_decimals = u64::from_le_bytes([data[40], data[41], data[42], data[43], data[44], data[45], data[46], data[47]]);
        let state = u64::from_le_bytes([data[48], data[49], data[50], data[51], data[52], data[53], data[54], data[55]]);
        let reset_flag = u64::from_le_bytes([data[56], data[57], data[58], data[59], data[60], data[61], data[62], data[63]]);
        let min_size = u64::from_le_bytes([data[64], data[65], data[66], data[67], data[68], data[69], data[70], data[71]]);
        let vol_max_cut_ratio = u64::from_le_bytes([data[72], data[73], data[74], data[75], data[76], data[77], data[78], data[79]]);
        let amount_wave = u64::from_le_bytes([data[80], data[81], data[82], data[83], data[84], data[85], data[86], data[87]]);
        let coin_lot_size = u64::from_le_bytes([data[88], data[89], data[90], data[91], data[92], data[93], data[94], data[95]]);
        let pc_lot_size = u64::from_le_bytes([data[96], data[97], data[98], data[99], data[100], data[101], data[102], data[103]]);
        let min_price_multiplier = u64::from_le_bytes([data[104], data[105], data[106], data[107], data[108], data[109], data[110], data[111]]);
        let max_price_multiplier = u64::from_le_bytes([data[112], data[113], data[114], data[115], data[116], data[117], data[118], data[119]]);
        // Parse Fees struct (offset 128-192)
        let fees = Fees {
            trade_fee_numerator: u64::from_le_bytes([data[128], data[129], data[130], data[131], data[132], data[133], data[134], data[135]]),
            trade_fee_denominator: u64::from_le_bytes([data[136], data[137], data[138], data[139], data[140], data[141], data[142], data[143]]),
            owner_trade_fee_numerator: u64::from_le_bytes([data[144], data[145], data[146], data[147], data[148], data[149], data[150], data[151]]),
            owner_trade_fee_denominator: u64::from_le_bytes([data[152], data[153], data[154], data[155], data[156], data[157], data[158], data[159]]),
            owner_withdraw_fee_numerator: u64::from_le_bytes([data[160], data[161], data[162], data[163], data[164], data[165], data[166], data[167]]),
            owner_withdraw_fee_denominator: u64::from_le_bytes([data[168], data[169], data[170], data[171], data[172], data[173], data[174], data[175]]),
            host_fee_numerator: u64::from_le_bytes([data[176], data[177], data[178], data[179], data[180], data[181], data[182], data[183]]),
            host_fee_denominator: u64::from_le_bytes([data[184], data[185], data[186], data[187], data[188], data[189], data[190], data[191]]),
        };
        
        // Parse StateData struct (offset 192-1008)
        // This represents the orderbook state in Raydium AMM
        let mut state_data_array = [0u64; 20];
        for i in 0..20 {
            let offset = 192 + (i * 8);
            if offset + 8 <= data.len() {
                state_data_array[i] = u64::from_le_bytes([
                    data[offset], data[offset + 1], data[offset + 2], data[offset + 3],
                    data[offset + 4], data[offset + 5], data[offset + 6], data[offset + 7]
                ]);
            }
        }
        
        // Parse free slot management (offset 352-368)
        let free_slot_bits = if 352 + 16 <= data.len() {
            u128::from_le_bytes([
                data[352], data[353], data[354], data[355], data[356], data[357], data[358], data[359],
                data[360], data[361], data[362], data[363], data[364], data[365], data[366], data[367]
            ])
        } else {
            0
        };
        
        let free_slot_head = if 368 < data.len() { data[368] } else { 0 };
        
        // Parse orderbook buffer (offset 369-1008)
        let mut buf_array = [0u64; 100];
        for i in 0..100 {
            let offset = 369 + (i * 8);
            if offset + 8 <= data.len() {
                buf_array[i] = u64::from_le_bytes([
                    data[offset], data[offset + 1], data[offset + 2], data[offset + 3],
                    data[offset + 4], data[offset + 5], data[offset + 6], data[offset + 7]
                ]);
            }
        }
        
        let state_data = StateData {
            data: state_data_array,
            free_slot_bits,
            free_slot_head,
            buf: buf_array,
        };
        
        let coin_vault = Pubkey::new(&data[208..240]);
        let pc_vault = Pubkey::new(&data[240..272]);
        let coin_vault_mint = Pubkey::new(&data[272..304]);
        let pc_vault_mint = Pubkey::new(&data[304..336]);
        let lp_mint = Pubkey::new(&data[336..368]);
        let open_orders = Pubkey::new(&data[368..400]);
        let market = Pubkey::new(&data[400..432]);
        let market_program = Pubkey::new(&data[432..464]);
        let target_orders = Pubkey::new(&data[464..496]);
        
        let amm_owner = Pubkey::new(&data[568..600]);
        
        let lp_amount = u64::from_le_bytes([data[608], data[609], data[610], data[611], data[612], data[613], data[614], data[615]]);
        let client_order_id = u64::from_le_bytes([data[616], data[617], data[618], data[619], data[620], data[621], data[622], data[623]]);
        let recent_epoch = u64::from_le_bytes([data[624], data[625], data[626], data[627], data[628], data[629], data[630], data[631]]);
        
        // Additional fields for pool amounts (these would be at the end of the struct)
        let pool_pc_amount = u64::from_le_bytes([data[736], data[737], data[738], data[739], data[740], data[741], data[742], data[743]]);
        let pool_coin_amount = u64::from_le_bytes([data[744], data[745], data[746], data[747], data[748], data[749], data[750], data[751]]);
        
        Ok(AmmInfo {
            status,
            nonce,
            order_num,
            depth,
            coin_decimals,
            pc_decimals,
            state,
            reset_flag,
            min_size,
            vol_max_cut_ratio,
            amount_wave,
            coin_lot_size,
            pc_lot_size,
            min_price_multiplier,
            max_price_multiplier,
            sys_decimal_value,
            fees,
            state_data,
            coin_vault,
            pc_vault,
            coin_vault_mint,
            pc_vault_mint,
            lp_mint,
            open_orders,
            market,
            market_program,
            target_orders,
            padding1: [0; 8],
            amm_owner,
            lp_amount,
            client_order_id,
            recent_epoch,
            padding2: 0,
            pool_pc_amount,
            pool_coin_amount,
        })
    }

    fn calculate_price(&self, coin_amount: u64, pc_amount: u64, coin_decimals: u64, pc_decimals: u64) -> f64 {
        if coin_amount == 0 {
            return 0.0;
        }
        
        // Calculate price as pc_amount / coin_amount, adjusted for decimals
        let coin_adjustment = 10f64.powi(coin_decimals as i32);
        let pc_adjustment = 10f64.powi(pc_decimals as i32);
        
        let coin_value = coin_amount as f64 / coin_adjustment;
        let pc_value = pc_amount as f64 / pc_adjustment;
        
        if coin_value == 0.0 {
            0.0
        } else {
            pc_value / coin_value
        }
    }
}
