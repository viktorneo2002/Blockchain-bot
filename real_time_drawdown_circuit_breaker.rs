use anchor_lang::prelude::*;
use anchor_lang::solana_program::{
    clock::Clock,
    program_error::ProgramError,
    sysvar::Sysvar,
    hash::hash,
};
use std::collections::VecDeque;
use std::convert::TryInto;
use std::mem::size_of;

declare_id!("DDBrk7Z3A7qDtpKYrAKqGYKWdPUEqB5HLoBGbB8pJeXd");

const MAX_SNAPSHOTS: usize = 2880; // 48 hours at 1min intervals
const MIN_CHECK_INTERVAL_MS: i64 = 50;
const RECOVERY_WIN_THRESHOLD: u32 = 5;
const MAX_DRAWDOWN_ABSOLUTE: f64 = 0.35;

#[derive(Debug, Clone, Copy, PartialEq, Eq, AnchorSerialize, AnchorDeserialize)]
#[repr(u8)]
pub enum CircuitState {
    Active = 0,
    Triggered = 1,
    Cooldown = 2,
    Recovery = 3,
}

#[derive(Debug, Clone, Copy, AnchorSerialize, AnchorDeserialize)]
pub struct DrawdownThresholds {
    pub intraday_soft: u16,    // basis points
    pub intraday_hard: u16,
    pub daily_soft: u16,
    pub daily_hard: u16,
    pub weekly_soft: u16,
    pub weekly_hard: u16,
    pub absolute_max: u16,
}

impl Default for DrawdownThresholds {
    fn default() -> Self {
        Self {
            intraday_soft: 200,     // 2%
            intraday_hard: 500,     // 5%
            daily_soft: 800,        // 8%
            daily_hard: 1500,       // 15%
            weekly_soft: 2000,      // 20%
            weekly_hard: 3000,      // 30%
            absolute_max: 3500,     // 35%
        }
    }
}

#[account]
#[derive(Default)]
pub struct CircuitBreakerAccount {
    pub authority: Pubkey,
    pub state: CircuitState,
    pub thresholds: DrawdownThresholds,
    pub current_pnl: i64,
    pub peak_pnl: i64,
    pub intraday_peak: i64,
    pub daily_peak: i64,
    pub weekly_peak: i64,
    pub trough_pnl: i64,
    pub last_update: i64,
    pub last_reset: i64,
    pub cooldown_start: i64,
    pub cooldown_duration: i64,
    pub consecutive_wins: u16,
    pub soft_breaches: u16,
    pub hard_breaches: u16,
    pub emergency_stops: u16,
    pub total_trades: u32,
    pub winning_trades: u32,
    pub recovery_threshold_bps: u16,
    pub position_scale: u16,
    pub max_daily_trades: u16,
    pub snapshot_index: u16,
    pub bump: u8,
    pub _padding: [u8; 3],
}

impl CircuitBreakerAccount {
    pub const LEN: usize = 8 + size_of::<Self>();

    pub fn update_pnl(&mut self, new_pnl: i64, timestamp: i64) -> Result<()> {
        require!(new_pnl >= 0, ErrorCode::InvalidPnL);
        require!(timestamp > self.last_update, ErrorCode::StaleUpdate);
        
        self.current_pnl = new_pnl;
        self.last_update = timestamp;
        
        // Update peaks with overflow protection
        if new_pnl > self.peak_pnl {
            self.peak_pnl = new_pnl;
            self.trough_pnl = new_pnl;
        } else if new_pnl < self.trough_pnl {
            self.trough_pnl = new_pnl;
        }
        
        // Reset intraday peak at day boundary
        let day_start = timestamp - (timestamp % 86400);
        if self.last_reset < day_start {
            self.intraday_peak = new_pnl;
            self.last_reset = day_start;
        } else if new_pnl > self.intraday_peak {
            self.intraday_peak = new_pnl;
        }
        
        // Update daily peak
        if timestamp - self.last_reset >= 86400 {
            self.daily_peak = new_pnl;
        } else if new_pnl > self.daily_peak {
            self.daily_peak = new_pnl;
        }
        
        // Update weekly peak
        if timestamp - self.last_reset >= 604800 {
            self.weekly_peak = new_pnl;
        } else if new_pnl > self.weekly_peak {
            self.weekly_peak = new_pnl;
        }
        
        Ok(())
    }
    
    pub fn calculate_drawdowns(&self) -> (u16, u16, u16, u16) {
        let calc_dd = |peak: i64, current: i64| -> u16 {
            if peak <= 0 {
                return 0;
            }
            let dd = ((peak.saturating_sub(current)) as f64 / peak as f64 * 10000.0) as u16;
            dd.min(10000) // Cap at 100%
        };
        
        (
            calc_dd(self.peak_pnl, self.current_pnl),
            calc_dd(self.intraday_peak, self.current_pnl),
            calc_dd(self.daily_peak, self.current_pnl),
            calc_dd(self.weekly_peak, self.current_pnl),
        )
    }
    
    pub fn check_circuit_breaker(&mut self, timestamp: i64) -> Result<CircuitAction> {
        let (abs_dd, intra_dd, daily_dd, weekly_dd) = self.calculate_drawdowns();
        
        match self.state {
            CircuitState::Active => {
                if abs_dd >= self.thresholds.absolute_max {
                    self.emergency_stops = self.emergency_stops.saturating_add(1);
                    self.state = CircuitState::Triggered;
                    self.cooldown_start = timestamp;
                    self.cooldown_duration = 86400; // 24 hours
                    return Ok(CircuitAction::EmergencyStop);
                }
                
                if intra_dd >= self.thresholds.intraday_hard ||
                   daily_dd >= self.thresholds.daily_hard ||
                   weekly_dd >= self.thresholds.weekly_hard {
                    self.hard_breaches = self.hard_breaches.saturating_add(1);
                    self.state = CircuitState::Triggered;
                    self.cooldown_start = timestamp;
                    self.cooldown_duration = 7200; // 2 hours
                    return Ok(CircuitAction::HardStop);
                }
                
                if intra_dd >= self.thresholds.intraday_soft ||
                   daily_dd >= self.thresholds.daily_soft ||
                   weekly_dd >= self.thresholds.weekly_soft {
                    self.soft_breaches = self.soft_breaches.saturating_add(1);
                    if self.soft_breaches >= 3 {
                        self.state = CircuitState::Cooldown;
                        self.cooldown_start = timestamp;
                        self.cooldown_duration = 3600; // 1 hour
                        return Ok(CircuitAction::SoftStop);
                    }
                    return Ok(CircuitAction::ReduceSize);
                }
                
                Ok(CircuitAction::Continue)
            }
            
            CircuitState::Triggered => {
                if timestamp >= self.cooldown_start.saturating_add(self.cooldown_duration) {
                    self.state = CircuitState::Cooldown;
                    self.consecutive_wins = 0;
                    Ok(CircuitAction::EnterCooldown)
                } else {
                    Ok(CircuitAction::Blocked)
                }
            }
            
            CircuitState::Cooldown => {
                let recovery_threshold = (self.thresholds.intraday_soft as f64 * 
                                         self.recovery_threshold_bps as f64 / 10000.0) as u16;
                
                if intra_dd < recovery_threshold && 
                   self.consecutive_wins >= RECOVERY_WIN_THRESHOLD as u16 {
                    self.state = CircuitState::Recovery;
                    self.position_scale = 30; // Start at 30% size
                    Ok(CircuitAction::StartRecovery)
                } else {
                    Ok(CircuitAction::WaitForRecovery)
                }
            }
            
            CircuitState::Recovery => {
                if daily_dd >= self.thresholds.daily_soft {
                    self.state = CircuitState::Cooldown;
                    self.consecutive_wins = 0;
                    Ok(CircuitAction::BackToCooldown)
                } else if abs_dd < self.thresholds.intraday_soft / 2 {
                    self.state = CircuitState::Active;
                    self.soft_breaches = 0;
                    self.position_scale = 100;
                    Ok(CircuitAction::FullyRecovered)
                } else {
                    // Gradually increase position size
                    if self.consecutive_wins > 0 && self.consecutive_wins % 3 == 0 {
                        self.position_scale = self.position_scale.saturating_add(10).min(100);
                    }
                    Ok(CircuitAction::LimitedTrading)
                }
            }
        }
    }
    
    pub fn record_trade(&mut self, profit: i64) {
        self.total_trades = self.total_trades.saturating_add(1);
        
        if profit > 0 {
            self.winning_trades = self.winning_trades.saturating_add(1);
            self.consecutive_wins = self.consecutive_wins.saturating_add(1);
        } else {
            self.consecutive_wins = 0;
        }
    }
    
    pub fn get_position_scale(&self) -> f64 {
        match self.state {
            CircuitState::Active => {
                let (_, intra_dd, daily_dd, _) = self.calculate_drawdowns();
                
                if intra_dd > self.thresholds.intraday_soft / 2 {
                    0.5
                } else if daily_dd > self.thresholds.daily_soft / 2 {
                    0.7
                } else {
                    1.0
                }
            }
            CircuitState::Recovery => self.position_scale as f64 / 100.0,
            _ => 0.0,
        }
    }
    
    pub fn get_max_daily_trades(&self) -> u16 {
        match self.state {
            CircuitState::Active => {
                let (_, _, daily_dd, _) = self.calculate_drawdowns();
                if daily_dd > self.thresholds.daily_soft * 7 / 10 {
                    50
                } else {
                    self.max_daily_trades
                }
            }
            CircuitState::Recovery => 20,
            _ => 0,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, AnchorSerialize, AnchorDeserialize)]
#[repr(u8)]
pub enum CircuitAction {
    Continue = 0,
    ReduceSize = 1,
    SoftStop = 2,
    HardStop = 3,
    EmergencyStop = 4,
    Blocked = 5,
    EnterCooldown = 6,
    WaitForRecovery = 7,
    StartRecovery = 8,
    LimitedTrading = 9,
    BackToCooldown = 10,
    FullyRecovered = 11,
}

#[program]
pub mod real_time_drawdown_circuit_breaker {
    use super::*;

    pub fn initialize(
        ctx: Context<Initialize>,
        initial_capital: u64,
        thresholds: DrawdownThresholds,
        max_daily_trades: u16,
    ) -> Result<()> {
        require!(initial_capital > 0, ErrorCode::InvalidCapital);
        require!(max_daily_trades > 0 && max_daily_trades <= 1000, ErrorCode::InvalidConfig);
        
        let breaker = &mut ctx.accounts.breaker;
        let clock = Clock::get()?;
        
        breaker.authority = ctx.accounts.authority.key();
        breaker.state = CircuitState::Active;
        breaker.thresholds = thresholds;
        breaker.current_pnl = initial_capital as i64;
        breaker.peak_pnl = initial_capital as i64;
        breaker.intraday_peak = initial_capital as i64;
        breaker.daily_peak = initial_capital as i64;
        breaker.weekly_peak = initial_capital as i64;
        breaker.trough_pnl = initial_capital as i64;
        breaker.last_update = clock.unix_timestamp;
        breaker.last_reset = clock.unix_timestamp - (clock.unix_timestamp % 86400);
        breaker.recovery_threshold_bps = 5000; // 50% of soft threshold
        breaker.position_scale = 100;
        breaker.max_daily_trades = max_daily_trades;
        breaker.bump = *ctx.bumps.get("breaker").unwrap();
        
        emit!(CircuitBreakerInitialized {
            authority: ctx.accounts.authority.key(),
            initial_capital,
            timestamp: clock.unix_timestamp,
        });
        
        Ok(())
    }

    pub fn update_pnl(
        ctx: Context<UpdatePnL>,
        current_pnl: u64,
        last_trade_profit: Option<i64>,
    ) -> Result<()> {
        let breaker = &mut ctx.accounts.breaker;
        let clock = Clock::get()?;
        
        // Rate limiting
        require!(
            clock.unix_timestamp.saturating_sub(breaker.last_update) >= MIN_CHECK_INTERVAL_MS,
            ErrorCode::RateLimited
        );
        
        breaker.update_pnl(current_pnl as i64, clock.unix_timestamp)?;
        
        if let Some(profit) = last_trade_profit {
            breaker.record_trade(profit);
        }
        
        let action = breaker.check_circuit_breaker(clock.unix_timestamp)?;
        
        emit!(DrawdownUpdate {
            current_pnl: current_pnl as i64,
            peak_pnl: breaker.peak_pnl,
            drawdowns: [
                breaker.calculate_drawdowns().0,
                breaker.calculate_drawdowns().1,
                breaker.calculate_drawdowns().2,
                breaker.calculate_drawdowns().3,
            ],
            action: action as u8,
            state: breaker.state as u8,
            timestamp: clock.unix_timestamp,
        });
        
        Ok(())
    }

    pub fn check_trading_allowed(
        ctx: Context<CheckTrading>,
        trade_size: u64,
    ) -> Result<TradingPermission> {
        let breaker = &ctx.accounts.breaker;
        let account_balance = ctx.accounts.trading_account.lamports();
        
        require!(trade_size > 0, ErrorCode::InvalidTradeSize);
        require!(account_balance > 0, ErrorCode::InsufficientBalance);
        
        let allowed = match breaker.state {
            CircuitState::Active | CircuitState::Recovery => {
                let risk_ratio = (trade_size as f64) / (account_balance as f64);
                let max_risk = match breaker.state {
                    CircuitState::Active => {
                        let (abs_dd, _, _, _) = breaker.calculate_drawdowns();
                        if abs_dd > breaker.thresholds.daily_soft * 8 / 10 {
                            0.005
                        } else if abs_dd > breaker.thresholds.intraday_soft * 8 / 10 {
                            0.01
                        } else {
                            0.02
                        }
                    }
                    CircuitState::Recovery => 0.005,
                    _ => 0.0,
                };
                risk_ratio <= max_risk
            }
            _ => false,
        };
        
        let position_scale = breaker.get_position_scale();
        let max_daily_trades = breaker.get_max_daily_trades();
        
        Ok(TradingPermission {
            allowed,
            position_scale_bps: (position_scale * 10000.0) as u16,
            max_daily_trades,
            current_state: breaker.state as u8,
        })
    }

    pub fn emergency_stop(ctx: Context<EmergencyStop>) -> Result<()> {
        let breaker = &mut ctx.accounts.breaker;
        let clock = Clock::get()?;
        
        breaker.state = CircuitState::Triggered;
        breaker.cooldown_start = clock.unix_timestamp;
        breaker.cooldown_duration = 86400;
        breaker.emergency_stops = breaker.emergency_stops.saturating_add(1);
        
        emit!(EmergencyStopActivated {
            authority: ctx.accounts.authority.key(),
            previous_pnl: breaker.current_pnl,
            timestamp: clock.unix_timestamp,
        });
        
        Ok(())
    }

    pub fn adjust_thresholds(
        ctx: Context<AdjustThresholds>,
        new_thresholds: DrawdownThresholds,
    ) -> Result<()> {
        let breaker = &mut ctx.accounts.breaker;
        
        // Validate thresholds
        require!(new_thresholds.intraday_soft > 0 && new_thresholds.intraday_soft < 10000, ErrorCode::InvalidThreshold);
        require!(new_thresholds.intraday_hard > new_thresholds.intraday_soft, ErrorCode::InvalidThreshold);
        require!(new_thresholds.daily_soft > new_thresholds.intraday_soft, ErrorCode::InvalidThreshold);
        require!(new_thresholds.daily_hard > new_thresholds.daily_soft, ErrorCode::InvalidThreshold);
        require!(new_thresholds.weekly_soft > new_thresholds.daily_soft, ErrorCode::InvalidThreshold);
        require!(new_thresholds.weekly_hard > new_thresholds.weekly_soft, ErrorCode::InvalidThreshold);
        require!(new_thresholds.absolute_max > new_thresholds.weekly_hard && new_thresholds.absolute_max <= 5000, ErrorCode::InvalidThreshold);
        
        breaker.thresholds = new_thresholds;
        
        emit!(ThresholdsAdjusted {
            authority: ctx.accounts.authority.key(),
            new_thresholds,
            timestamp: Clock::get()?.unix_timestamp,
        });
        
        Ok(())
    }

    pub fn reset_soft_breaches(ctx: Context<ResetBreaches>) -> Result<()> {
        let breaker = &mut ctx.accounts.breaker;
        
        require!(
            breaker.state == CircuitState::Active || breaker.state == CircuitState::Recovery,
            ErrorCode::InvalidState
        );
        
        let (abs_dd, _, _, _) = breaker.calculate_drawdowns();
        require!(abs_dd < breaker.thresholds.intraday_soft / 2, ErrorCode::DrawdownTooHigh);
        
        breaker.soft_breaches = 0;
        
        Ok(())
    }

    pub fn force_recovery(ctx: Context<ForceRecovery>) -> Result<()> {
        let breaker = &mut ctx.accounts.breaker;
        let clock = Clock::get()?;
        
        require!(breaker.state == CircuitState::Cooldown, ErrorCode::InvalidState);
        require!(
            clock.unix_timestamp >= breaker.cooldown_start + breaker.cooldown_duration / 2,
            ErrorCode::CooldownNotReady
        );
        
        breaker.state = CircuitState::Recovery;
        breaker.position_scale = 20; // Start at 20% for forced recovery
        
        emit!(ForcedRecoveryInitiated {
            authority: ctx.accounts.authority.key(),
            timestamp: clock.unix_timestamp,
        });
        
        Ok(())
    }
}

#[derive(Accounts)]
pub struct Initialize<'info> {
    #[account(
        init,
        payer = authority,
        space = CircuitBreakerAccount::LEN,
        seeds = [b"circuit_breaker", authority.key().as_ref()],
        bump
    )]
    pub breaker: Account<'info, CircuitBreakerAccount>,
    #[account(mut)]
    pub authority: Signer<'info>,
    pub system_program: Program<'info, System>,
}

#[derive(Accounts)]
pub struct UpdatePnL<'info> {
    #[account(
        mut,
        seeds = [b"circuit_breaker", breaker.authority.as_ref()],
        bump = breaker.bump,
        constraint = breaker.authority == authority.key() @ ErrorCode::Unauthorized
    )]
    pub breaker: Account<'info, CircuitBreakerAccount>,
    pub authority: Signer<'info>,
}

#[derive(Accounts)]
pub struct CheckTrading<'info> {
    #[account(
        seeds = [b"circuit_breaker", breaker.authority.as_ref()],
        bump = breaker.bump
    )]
    pub breaker: Account<'info, CircuitBreakerAccount>,
    /// CHECK: Trading account balance is checked in instruction
    pub trading_account: AccountInfo<'info>,
}

#[derive(Accounts)]
pub struct EmergencyStop<'info> {
    #[account(
        mut,
        seeds = [b"circuit_breaker", breaker.authority.as_ref()],
        bump = breaker.bump,
        constraint = breaker.authority == authority.key() @ ErrorCode::Unauthorized
    )]
    pub breaker: Account<'info, CircuitBreakerAccount>,
    pub authority: Signer<'info>,
}

#[derive(Accounts)]
pub struct AdjustThresholds<'info> {
    #[account(
        mut,
        seeds = [b"circuit_breaker", breaker.authority.as_ref()],
        bump = breaker.bump,
        constraint = breaker.authority == authority.key() @ ErrorCode::Unauthorized
    )]
    pub breaker: Account<'info, CircuitBreakerAccount>,
    pub authority: Signer<'info>,
}

#[derive(Accounts)]
pub struct ResetBreaches<'info> {
    #[account(
        mut,
        seeds = [b"circuit_breaker", breaker.authority.as_ref()],
        bump = breaker.bump,
        constraint = breaker.authority == authority.key() @ ErrorCode::Unauthorized
    )]
    pub breaker: Account<'info, CircuitBreakerAccount>,
    pub authority: Signer<'info>,
}

#[derive(Accounts)]
pub struct ForceRecovery<'info> {
    #[account(
        mut,
        seeds = [b"circuit_breaker", breaker.authority.as_ref()],
        bump = breaker.bump,
        constraint = breaker.authority == authority.key() @ ErrorCode::Unauthorized
    )]
    pub breaker: Account<'info, CircuitBreakerAccount>,
    pub authority: Signer<'info>,
}

#[derive(AnchorSerialize, AnchorDeserialize)]
pub struct TradingPermission {
    pub allowed: bool,
    pub position_scale_bps: u16,
    pub max_daily_trades: u16,
    pub current_state: u8,
}

#[event]
pub struct CircuitBreakerInitialized {
    pub authority: Pubkey,
    pub initial_capital: u64,
    pub timestamp: i64,
}

#[event]
pub struct DrawdownUpdate {
    pub current_pnl: i64,
    pub peak_pnl: i64,
    pub drawdowns: [u16; 4], // absolute, intraday, daily, weekly in bps
    pub action: u8,
    pub state: u8,
    pub timestamp: i64,
}

#[event]
pub struct EmergencyStopActivated {
    pub authority: Pubkey,
    pub previous_pnl: i64,
    pub timestamp: i64,
}

#[event]
pub struct ThresholdsAdjusted {
    pub authority: Pubkey,
    pub new_thresholds: DrawdownThresholds,
    pub timestamp: i64,
}

#[event]
pub struct ForcedRecoveryInitiated {
    pub authority: Pubkey,
    pub timestamp: i64,
}

#[error_code]
pub enum ErrorCode {
    #[msg("Invalid P&L value")]
    InvalidPnL,
    #[msg("Stale update - timestamp must be greater than last update")]
    StaleUpdate,
    #[msg("Invalid initial capital")]
    InvalidCapital,
    #[msg("Invalid configuration parameters")]
    InvalidConfig,
    #[msg("Rate limited - too frequent updates")]
    RateLimited,
    #[msg("Invalid trade size")]
    InvalidTradeSize,
    #[msg("Insufficient account balance")]
    InsufficientBalance,
    #[msg("Unauthorized access")]
    Unauthorized,
    #[msg("Invalid threshold values")]
    InvalidThreshold,
    #[msg("Invalid state for operation")]
    InvalidState,
    #[msg("Drawdown too high for reset")]
    DrawdownTooHigh,
    #[msg("Cooldown period not ready")]
    CooldownNotReady,
}
