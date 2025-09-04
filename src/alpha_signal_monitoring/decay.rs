use crate::state::OpportunityState;
use yata::indicators::EMA;
use yata::prelude::*;

pub struct DecayModel {
    window_size: usize,
    ema: EMA,
}

impl DecayModel {
    pub fn new(window_size: usize) -> Self {
        Self {
            window_size,
            ema: EMA::new(window_size, &0.0).expect("Failed to create EMA indicator"),
        }
    }

    pub fn update_decay(&mut self, opp: &OpportunityState) -> f64 {
        // Calculate decay rate from opportunity history
        let decay = if opp.history.len() >= 2 {
            let last = opp.history[opp.history.len() - 1].profit_rate;
            let prev = opp.history[opp.history.len() - 2].profit_rate;
            (last - prev).abs()
        } else {
            0.0
        };
        
        // Update EMA with new decay value
        self.ema.next(&decay)
    }
}
