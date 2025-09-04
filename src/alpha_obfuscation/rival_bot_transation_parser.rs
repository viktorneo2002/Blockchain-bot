#![no_main]
use libfuzzer_sys::fuzz_target;
use rival_bot_capability_scorer::{RivalBotCapabilityScorer, BotStrategy};

fuzz_target!(|data: &[u8]| {
    // attempt to deserialize as base64 tx
    if let Ok(tx) = base64::decode(data) {
        let _ = RivalBotCapabilityScorer::detect_strategy_from_bytes(&tx);
    }
});
