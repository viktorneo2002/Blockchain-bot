use std::{sync::Arc, time::Instant};
use anyhow::Result;
use serde::{Deserialize, Serialize};
use solana_sdk::{instruction::Instruction, pubkey::Pubkey, signature::Signature};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SendOutcome {
    pub signature: Signature,
    pub route: RouteKind,
    pub sent_at: Instant,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum RouteKind {
    PublicRpc,
    DirectTpu,
    JitoBundle,
}

#[derive(Debug, Clone)]
pub struct OpportunityMeta {
    pub target_slot: u64,
    pub leader: Option<Pubkey>,
    pub target_rank: Option<u8>,
    pub pnl_min_lamports: i64,
    pub strict_window_ms: Option<u64>,
}

#[derive(Debug, Clone)]
pub struct ExecutionPlan {
    pub instructions: Vec<Instruction>,
}

#[derive(Debug, thiserror::Error)]
pub enum ExecError {
    #[error("duplicate signature")]
    DuplicateSig,
    #[error("blockhash expired")]
    BlockhashExpired,
    #[error("compute units exceeded")]
    ComputeExceeded,
    #[error("account missing")]
    AccountMissing,
    #[error("insufficient fee or tip")]
    FeeTooLow,
    #[error("route failure: {0}")]
    RouteFailure(String),
    #[error("confirmation timeout")]
    ConfirmTimeout,
    #[error("ev guardrail rejected")]
    EvRejected,
    #[error("unknown: {0}")]
    Unknown(String),
}
