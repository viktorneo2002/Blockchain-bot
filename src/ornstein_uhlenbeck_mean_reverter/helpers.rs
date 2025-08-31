use rust_decimal::Decimal;
use rust_decimal::prelude::ToPrimitive;

/// Converts USD amount to token amount using oracle price
/// This is a placeholder implementation - in practice, you would get the oracle price from an on-chain source
pub fn usd_to_token_amount(usd_amount: f64, decimals: u32) -> u64 {
    // Placeholder oracle price (in practice, get this from an oracle)
    let oracle_price_usd = Decimal::new(100, 0); // $100 per token
    
    // Convert using rust_decimal for precision
    let usd_amount_dec = Decimal::try_from_f64(usd_amount).unwrap_or(Decimal::ZERO);
    let token_amount_dec = usd_amount_dec / oracle_price_usd;
    
    // Scale by decimals and convert to u64
    let scaled_amount = token_amount_dec * Decimal::new(10i64.pow(decimals), 0);
    
    scaled_amount.to_u64().unwrap_or(0)
}
