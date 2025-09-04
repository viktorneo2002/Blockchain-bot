use anchor_lang::prelude::*;

#[error_code]
pub enum ErrorCode {
    #[msg("Invalid program ID")]
    InvalidProgramId,
    #[msg("Route data too large")]
    RouteDataTooLarge,
    #[msg("Slippage exceeded")]
    SlippageExceeded,
    #[msg("Invalid account data")]
    InvalidAccountData,
    #[msg("Math overflow")]
    MathOverflow,
    #[msg("Invalid parameter")]
    InvalidParameter,
    #[msg("Protocol inactive")]
    ProtocolInactive,
    #[msg("Invalid amount")]
    InvalidAmount,
    #[msg("Aggregator paused")]
    AggregatorPaused,
    #[msg("Unsupported protocol")]
    UnsupportedProtocol,
}
