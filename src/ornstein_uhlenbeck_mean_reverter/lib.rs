//! Ornstein-Uhlenbeck Mean Reversion Trading Bot for Solana
//! 
//! This crate implements a sophisticated mean reversion trading strategy using the Ornstein-Uhlenbeck process.
//! It's designed to be production-ready with proper error handling, risk management, and Solana optimizations.

#![deny(missing_docs)]
#![warn(rust_2018_idioms)]

pub mod helpers;
pub mod token_utils;
pub mod risk_management;

// Re-export commonly used items
pub use helpers::*;
pub use token_utils::*;
pub use risk_management::{RiskConfig, RiskManager, RiskError};
