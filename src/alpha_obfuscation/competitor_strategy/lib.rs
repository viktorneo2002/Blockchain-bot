// crates/fixtures/src/lib.rs
use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct FixtureTx {
    pub signature: String,
    pub encoded_tx: String, // base64
    pub meta: serde_json::Value,
    pub expected: Expected,
}

#[derive(Debug, Deserialize)]
pub struct Expected {
    pub strategy_contains: String,
    pub pnl_min_usd: f64,
    pub cu_used_min: u64,
}

pub fn load_fixture(name: &str) -> anyhow::Result<FixtureTx> {
    let p = format!("{}/data/{}.json", env!("CARGO_MANIFEST_DIR"), name);
    let s = std::fs::read_to_string(p)?;
    Ok(serde_json::from_str(&s)?)
}
