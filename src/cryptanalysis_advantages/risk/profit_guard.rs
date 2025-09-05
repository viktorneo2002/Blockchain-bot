use std::time::{Duration, Instant};

#[derive(Debug, Clone)]
pub struct ProfitGuardConfig {
    pub min_expected_pnl_lamports: i64,
    pub max_fee_per_cu: u64,
    pub max_total_fee_lamports: u64,
    pub max_per_opportunity_fee_lamports: u64,
    pub rolling_window: Duration,
    pub drawdown_limit_lamports: i64,
}

#[derive(Debug)]
pub struct ProfitGuard {
    cfg: ProfitGuardConfig,
    window_start: Instant,
    spent_in_window: u64,
    pnl_in_window: i64,
    spent_this_opportunity: u64,
}

impl ProfitGuard {
    pub fn new(cfg: ProfitGuardConfig) -> Self {
        Self {
            cfg,
            window_start: Instant::now(),
            spent_in_window: 0,
            pnl_in_window: 0,
            spent_this_opportunity: 0,
        }
    }

    pub fn reset_opportunity(&mut self) {
        self.spent_this_opportunity = 0;
    }

    fn roll_window_if_needed(&mut self) {
        if self.window_start.elapsed() > self.cfg.rolling_window {
            self.window_start = Instant::now();
            self.spent_in_window = 0;
            self.pnl_in_window = 0;
        }
    }

    pub fn update_realized(&mut self, realized_pnl: i64, fee_spent: u64) {
        self.roll_window_if_needed();
        self.pnl_in_window += realized_pnl;
        self.spent_in_window = self.spent_in_window.saturating_add(fee_spent);
    }

    pub fn approve_send(
        &mut self,
        expected_pnl_lamports: i64,
        cu_used_estimate: u64,
        lamports_per_cu: u64,
    ) -> anyhow::Result<()> {
        self.roll_window_if_needed();

        if expected_pnl_lamports < self.cfg.min_expected_pnl_lamports {
            anyhow::bail!("EV too low: {}", expected_pnl_lamports);
        }
        if lamports_per_cu > self.cfg.max_fee_per_cu {
            anyhow::bail!("cu price cap exceeded: {}", lamports_per_cu);
        }

        let estimated_fee = cu_used_estimate.saturating_mul(lamports_per_cu);
        if self.spent_in_window.saturating_add(estimated_fee) > self.cfg.max_total_fee_lamports {
            anyhow::bail!("rolling window fee cap hit");
        }
        if self.spent_this_opportunity.saturating_add(estimated_fee) > self.cfg.max_per_opportunity_fee_lamports {
            anyhow::bail!("per-opportunity fee cap hit");
        }
        if self.pnl_in_window - (self.spent_in_window as i64) < -self.cfg.drawdown_limit_lamports {
            anyhow::bail!("drawdown guard tripped");
        }

        self.spent_this_opportunity = self.spent_this_opportunity.saturating_add(estimated_fee);
        Ok(())
    }
}
