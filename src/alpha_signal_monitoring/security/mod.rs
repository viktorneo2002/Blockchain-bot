 security/mod.rs

pub fn validate_accounts(
    tx: &ParsedTransaction,
    expected_accounts: &[String],
) -> Result<()> {
    for a in expected_accounts {
        if !tx.accounts.contains(a) {
            return Err(anyhow!("Missing expected account: {}", a));
        }
    }
    Ok(())
}

// Example use
if let Err(e) = validate_accounts(&tx_data, &["RaydiumPool", "TokenA", "TokenB"]) {
    log::warn!("Security validation failed: {}", e);
    continue;
}
