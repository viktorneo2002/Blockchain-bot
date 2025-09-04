use crate::state::OpportunityState;
use redis::aio::Connection;
use anyhow::{Result, Context};
use serde_json;
use std::collections::HashMap;

/// Initializes a Redis async connection
pub async fn init_redis() -> Result<Connection> {
    let client = redis::Client::open("redis://127.0.0.1/")?;
    let con = client.get_async_connection().await
        .context("Failed to establish Redis connection")?;
    Ok(con)
}

/// Stores an opportunity in Redis with the given key
pub async fn store_opportunity(
    con: &mut Connection, 
    key: &str, 
    opp: &OpportunityState
) -> Result<()> {
    let json = serde_json::to_string(opp)
        .context("Failed to serialize opportunity state")?;
        
    redis::cmd("SET")
        .arg(key)
        .arg(json)
        .query_async(con)
        .await
        .context("Failed to store opportunity in Redis")?;
        
    Ok(())
}

/// Retrieves an opportunity from Redis by key
pub async fn get_opportunity(
    con: &mut Connection,
    key: &str
) -> Result<OpportunityState> {
    let json: String = redis::cmd("GET")
        .arg(key)
        .query_async(con)
        .await
        .context("Failed to retrieve opportunity from Redis")?;
        
    serde_json::from_str(&json)
        .context("Failed to deserialize opportunity state")
}

/// Updates an existing opportunity in Redis
pub async fn update_opportunity(
    con: &mut Connection,
    key: &str,
    opp: &OpportunityState
) -> Result<()> {
    store_opportunity(con, key, opp).await
}

/// Lists all opportunities matching a pattern
pub async fn list_opportunities(
    con: &mut Connection,
    pattern: &str
) -> Result<HashMap<String, OpportunityState>> {
    let keys: Vec<String> = redis::cmd("KEYS")
        .arg(pattern)
        .query_async(con)
        .await
        .context("Failed to list opportunity keys")?;
        
    let mut opportunities = HashMap::new();
    
    for key in keys {
        let opp = get_opportunity(con, &key).await?;
        opportunities.insert(key, opp);
    }
    
    Ok(opportunities)
}

#[cfg(test)]
mod tests {
    use super::*;
    use solana_program_test::*;
    use tokio::runtime::Runtime;
    
    #[tokio::test]
    async fn test_redis_operations() {
        // Initialize test Redis connection
        let mut con = init_redis().await.expect("Failed to connect to Redis");
        
        // Create test opportunity
        let test_key = "test_opportunity:1";
        let test_opp = OpportunityState::default();
        
        // Test store operation
        store_opportunity(&mut con, test_key, &test_opp)
            .await
            .expect("Failed to store opportunity");
            
        // Test get operation
        let retrieved = get_opportunity(&mut con, test_key)
            .await
            .expect("Failed to get opportunity");
            
        assert_eq!(retrieved, test_opp);
        
        // Test update operation
        let updated_opp = OpportunityState::default(); // Create different state
        update_opportunity(&mut con, test_key, &updated_opp)
            .await
            .expect("Failed to update opportunity");
            
        // Test list operation
        let opportunities = list_opportunities(&mut con, "test_opportunity:*")
            .await
            .expect("Failed to list opportunities");
            
        assert!(opportunities.contains_key(test_key));
        assert_eq!(opportunities.get(test_key), Some(&updated_opp));
    }
}
