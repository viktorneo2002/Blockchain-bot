use {
    solana_sdk::{
        commitment_config::CommitmentConfig,
        message::Message,
        pubkey::Pubkey,
        signature::Signature,
        signer::Signer,
        transaction::VersionedTransaction,
    },
    solana_client::{
        rpc_client::RpcClient,
        rpc_config::RpcSendTransactionConfig,
        rpc_response::RpcSignatureResult,
    },
    std::time::{Duration, Instant},
    tokio::time::sleep,
    rand::Rng,
    super::error::{ExecError, Result},
};

const MAX_CONFIRM_SLOTS: u64 = 8;
const MAX_RETRIES: u8 = 3;

/// Profit guard to ensure transactions remain profitable after fees
pub fn ensure_profit_after_fees(
    expected_profit: u64,
    micro_per_cu: u64,
    cu_limit: u64,
    extra_costs: u64,
) -> Result<()> {
    let priority_fee_lamports = micro_per_cu
        .saturating_mul(cu_limit)
        .saturating_div(1_000_000);
    
    let net = expected_profit
        .saturating_sub(priority_fee_lamports)
        .saturating_sub(extra_costs);
    
    // Require at least 10% of expected profit as safety margin
    if net == 0 || net < expected_profit / 10 {
        return Err(ExecError::ProfitGuard);
    }
    
    Ok(())
}

/// Categorizes RPC errors into our error types
fn categorize_rpc_error(s: &str) -> ExecError {
    if s.contains("BlockhashNotFound") || s.contains("expired") {
        ExecError::BlockhashExpired
    } else if s.contains("AccountInUse") {
        // Extract account from error message if possible
        let account = s
            .split_whitespace()
            .find(|s| s.starts_with("[") && s.ends_with("]"))
            .and_then(|s| s.trim_matches(&['[', ']'][..]).parse::<Pubkey>().ok())
            .unwrap_or_default();
        ExecError::AccountInUse(account)
    } else if s.contains("WouldExceedMaxBlockCostLimit") 
            || s.contains("PriorityFeeTooLow") 
            || s.contains("WouldExceedMaxAccountCostLimit") {
        ExecError::FeeTooLow
    } else if s.contains("Transaction simulation failed") {
        ExecError::Simulation(s.into())
    } else {
        ExecError::Rpc(s.into())
    }
}

/// Applies exponential backoff with jitter
async fn apply_backoff(attempt: u8) {
    let base = 50u64;
    let jitter = rand::thread_rng().gen_range(0..=15);
    let ms = base.saturating_mul(1 << attempt.min(6)).saturating_add(jitter);
    sleep(Duration::from_millis(ms)).await;
}

/// Sends a transaction with retries and proper error handling
pub async fn send_transaction_with_retry(
    rpc_client: &RpcClient,
    transaction: &VersionedTransaction,
    max_retries: u8,
) -> Result<Signature> {
    let mut last_err = None;
    let mut retries = 0;

    while retries <= max_retries {
        // Try to send the transaction with our custom config
        match rpc_client.send_transaction_with_config(
            transaction,
            RpcSendTransactionConfig {
                skip_preflight: false,
                preflight_commitment: Some(CommitmentConfig::processed()),
                encoding: None,
                max_retries: Some(0), // We handle retries ourselves
                min_context_slot: None,
            },
        ).await {
            Ok(signature) => {
                // Fast confirm with slot-based timeout
                match confirm_transaction_fast(rpc_client, &signature).await {
                    Ok(Some(confirmed)) => {
                        if confirmed {
                            return Ok(signature);
                        }
                        // Transaction failed on-chain, no point retrying
                        return Err(ExecError::ConfirmTimeout(signature));
                    }
                    Ok(None) => {
                        // Transaction not yet confirmed, will retry with backoff
                        last_err = Some(ExecError::ConfirmTimeout(signature));
                    }
                    Err(e) => {
                        // Error checking status, will retry with backoff
                        last_err = Some(e);
                    }
                }
            }
            Err(e) => {
                let error_str = e.to_string();
                last_err = Some(categorize_rpc_error(&error_str));
            }
        }

        // If we get here, the transaction failed or wasn't confirmed
        if let Some(ref err) = last_err {
            if !err.is_retryable() || retries >= max_retries {
                break;
            }
            apply_backoff(retries).await;
            retries += 1;
        }
    }

    Err(last_err.unwrap_or_else(|| ExecError::Other("Transaction failed after retries".into())))
}

/// Categorizes RPC errors into our error types
pub fn categorize_rpc_error(s: &str) -> ExecError {
    if s.contains("BlockhashNotFound") || s.contains("expired") {
        ExecError::BlockhashExpired
    } else if s.contains("AccountInUse") {
        // Extract account from error message if possible
        let account = s
            .split_whitespace()
            .find(|s| s.starts_with('[') && s.ends_with(']'))
            .and_then(|s| s.trim_matches(&['[', ']'][..]).parse::<Pubkey>().ok())
            .unwrap_or_default();
        ExecError::AccountInUse(account)
    } else if s.contains("WouldExceedMaxBlockCostLimit") 
            || s.contains("PriorityFeeTooLow") 
            || s.contains("WouldExceedMaxAccountCostLimit") {
        ExecError::FeeTooLow
    } else if s.contains("Transaction simulation failed") {
        ExecError::Simulation(s.into())
    } else {
        ExecError::Rpc(s.into())
    }
}

/// Applies exponential backoff with jitter
pub async fn apply_backoff(attempt: u8) {
    let base = 50u64;
    let jitter = rand::thread_rng().gen_range(0..=15);
    let ms = base.saturating_mul(1 << attempt.min(6)).saturating_add(jitter);
    sleep(Duration::from_millis(ms)).await;
}

/// Fast transaction confirmation using signature status with history
async fn confirm_transaction_fast(
    rpc_client: &RpcClient,
    signature: &Signature,
) -> Result<Option<bool>> {
    let start_slot = rpc_client
        .get_slot()
        .map_err(|e| ExecError::Rpc(e.to_string()))?;

    loop {
        // Prefer lightweight status call with search history
        match rpc_client.get_signature_statuses_with_history(&[*signature]).await {
            Ok(statuses) => {
                if let Some(Some(status)) = statuses.value.get(0) {
                    match status {
                        RpcSignatureResult {
                            err: None,
                            confirmation_status: _, 
                            confirmations: _,
                            status: _,
                            slot: _,
                        } => return Ok(Some(true)),
                        RpcSignatureResult {
                            err: Some(_),
                            confirmation_status: _,
                            confirmations: _,
                            status: _,
                            slot: _,
                        } => return Ok(Some(false)),
                    }
                }
            }
            Err(e) => {
                return Err(ExecError::Rpc(e.to_string()));
            }
        }

        // Check if we've exceeded the maximum confirmation slots
        let current_slot = rpc_client
            .get_slot()
            .map_err(|e| ExecError::Rpc(e.to_string()))?;

        if current_slot.saturating_sub(start_slot) > MAX_CONFIRM_SLOTS {
            return Ok(None);
        }

        // Wait before checking again
        sleep(Duration::from_millis(100)).await;
    }
}

/// Confirms a transaction with slot-based timeout (kept for backward compatibility)
async fn confirm_transaction(
    rpc_client: &RpcClient,
    signature: &Signature,
) -> Result<bool> {
    match confirm_transaction_fast(rpc_client, signature).await? {
        Some(status) => Ok(status),
        None => Ok(false), // Timeout
    }
}

/// Partitions items into non-conflicting groups based on their write keys
/// 
/// # Arguments
/// * `items` - The items to partition
/// * `write_keys` - A function that extracts the write keys for each item
/// 
/// # Returns
/// A vector of groups where items in the same group don't have conflicting write keys
pub fn partition_non_conflicting<T, F, K>(
    items: &[T],
    write_keys: F,
) -> Vec<Vec<T>>
where
    T: Clone,
    F: Fn(&T) -> Vec<K>,
    K: std::hash::Hash + Eq + Clone,
{
    let mut groups: Vec<(std::collections::HashSet<K>, Vec<T>)> = Vec::new();

    'outer: for item in items {
        // Get all write keys for this item
        let keys = write_keys(item);
        let key_set: std::collections::HashSet<_> = keys.into_iter().collect();

        // Try to find an existing group without conflicts
        for (group_keys, group_items) in &mut groups {
            if group_keys.is_disjoint(&key_set) {
                // No conflicts, add to this group
                group_keys.extend(key_set.into_iter());
                group_items.push(item.clone());
                continue 'outer;
            }
        }

        // If we get here, we need a new group
        groups.push((key_set, vec![item.clone()]));
    }

    // Extract just the item groups
    groups.into_iter().map(|(_, items)| items).collect()
}

/// Executes a batch of transactions in parallel where possible
/// 
/// # Arguments
/// * `rpc_client` - The RPC client to use
/// * `transactions` - The transactions to execute
/// * `concurrency_limit` - Maximum number of parallel executions
/// 
/// # Returns
/// A vector of results in the same order as the input transactions
pub async fn execute_batch<T, F, Fut>(
    items: Vec<T>,
    concurrency_limit: usize,
    processor: F,
) -> Vec<anyhow::Result<T::Output>>
where
    T: Send + 'static,
    F: Fn(T) -> Fut + Send + Sync + 'static,
    Fut: std::future::Future<Output = anyhow::Result<T::Output>> + Send,
    T: std::marker::Sized + std::fmt::Debug,
    T::Output: Send + 'static,
{
    use futures::stream::{self, StreamExt};
    use tokio::sync::Semaphore;

    let semaphore = Arc::new(Semaphore::new(concurrency_limit));
    let processor = Arc::new(processor);

    let tasks = items.into_iter().enumerate().map(|(idx, item)| {
        let processor = processor.clone();
        let semaphore = semaphore.clone();
        
        tokio::spawn(async move {
            let _permit = semaphore.acquire_owned().await?;
            let result = processor(item).await;
            result
        })
    });

    // Collect results in order
    let mut results = Vec::with_capacity(tasks.len());
    for task in tasks {
        match task.await {
            Ok(Ok(result)) => results.push(Ok(result)),
            Ok(Err(e)) => results.push(Err(e)),
            Err(e) => results.push(Err(anyhow::anyhow!("Task panicked: {}", e))),
        }
    }

    results
}
