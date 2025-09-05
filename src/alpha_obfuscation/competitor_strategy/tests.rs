use fixtures::load_fixture;

#[tokio::test]
async fn test_orca_swap_fixture() -> anyhow::Result<()> {
    let fx = load_fixture("tx_orca_whirlpool_swap")?;
    // build decoder with mock RPC that returns fx.encoded_tx/meta
    // run analyze_signature and assert:
    // - strategy contains "Orca"
    // - pnl_usd >= expected
    // - cu_used >= expected
    Ok(())
}
