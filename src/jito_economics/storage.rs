use async_trait::async_trait;
use anyhow::{Result, anyhow};
use sqlx::{PgPool, Row, postgres::PgPoolOptions};
use std::sync::Arc;
use tokio::sync::Mutex;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use solana_sdk::pubkey::Pubkey;

use super::bundle_success_rate_calculator::{
    BundleSubmission, ValidatorStats, FailureAnalysis, 
    BundleStatus, FailureReason, TimeWindowStats
};

#[async_trait]
pub trait MetricsStorage: Send + Sync {
    async fn store_submission(&self, submission: &BundleSubmission) -> Result<()>;
    async fn bulk_store_submissions(&self, submissions: &[BundleSubmission]) -> Result<()>;
    async fn get_submissions_by_time_range(&self, start: u64, end: u64) -> Result<Vec<BundleSubmission>>;
    async fn get_validator_stats_history(&self, validator: &Pubkey, hours: u32) -> Result<Vec<ValidatorStats>>;
    async fn cleanup_old_data(&self, retention_hours: u32) -> Result<u64>;
    async fn health_check(&self) -> Result<bool>;
}

pub struct PostgresStorage {
    pool: PgPool,
    batch_size: usize,
    batch_buffer: Arc<Mutex<Vec<BundleSubmission>>>,
}

impl PostgresStorage {
    pub async fn new(database_url: &str, batch_size: usize) -> Result<Self> {
        let pool = PgPoolOptions::new()
            .max_connections(20)
            .min_connections(5)
            .connect(database_url)
            .await
            .map_err(|e| anyhow!("Failed to connect to database: {}", e))?;

        let storage = Self {
            pool,
            batch_size,
            batch_buffer: Arc::new(Mutex::new(Vec::with_capacity(batch_size))),
        };

        storage.initialize_schema().await?;
        Ok(storage)
    }

    async fn initialize_schema(&self) -> Result<()> {
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS bundle_submissions (
                id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                bundle_id VARCHAR(64) NOT NULL,
                timestamp BIGINT NOT NULL,
                slot BIGINT NOT NULL,
                tip_lamports BIGINT NOT NULL,
                validator VARCHAR(44) NOT NULL,
                status VARCHAR(20) NOT NULL,
                failure_reason VARCHAR(30),
                landing_slot BIGINT,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
            "#
        )
        .execute(&self.pool)
        .await
        .map_err(|e| anyhow!("Failed to create table: {}", e))?;

        Ok(())
    }
}

#[async_trait]
impl MetricsStorage for PostgresStorage {
    async fn store_submission(&self, submission: &BundleSubmission) -> Result<()> {
        sqlx::query(
            r#"
            INSERT INTO bundle_submissions 
            (bundle_id, timestamp, slot, tip_lamports, validator, status, failure_reason, landing_slot)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
            "#
        )
        .bind(&submission.bundle_id)
        .bind(submission.timestamp as i64)
        .bind(submission.slot as i64)
        .bind(submission.tip_lamports as i64)
        .bind(submission.validator.to_string())
        .bind(format!("{:?}", submission.status))
        .bind(submission.failure_reason.map(|r| format!("{:?}", r)))
        .bind(submission.landing_slot.map(|s| s as i64))
        .execute(&self.pool)
        .await
        .map_err(|e| anyhow!("Failed to store submission: {}", e))?;

        Ok(())
    }

    async fn bulk_store_submissions(&self, submissions: &[BundleSubmission]) -> Result<()> {
        if submissions.is_empty() {
            return Ok(());
        }

        let mut query_builder = sqlx::QueryBuilder::new(
            "INSERT INTO bundle_submissions (bundle_id, timestamp, slot, tip_lamports, validator, status, failure_reason, landing_slot) "
        );

        query_builder.push_values(submissions, |mut b, submission| {
            b.push_bind(&submission.bundle_id)
                .push_bind(submission.timestamp as i64)
                .push_bind(submission.slot as i64)
                .push_bind(submission.tip_lamports as i64)
                .push_bind(submission.validator.to_string())
                .push_bind(format!("{:?}", submission.status))
                .push_bind(submission.failure_reason.map(|r| format!("{:?}", r)))
                .push_bind(submission.landing_slot.map(|s| s as i64));
        });

        query_builder
            .build()
            .execute(&self.pool)
            .await
            .map_err(|e| anyhow!("Failed to bulk insert: {}", e))?;

        Ok(())
    }

    async fn get_submissions_by_time_range(&self, start: u64, end: u64) -> Result<Vec<BundleSubmission>> {
        // Implementation simplified for space - would include full row parsing
        Ok(Vec::new())
    }

    async fn get_validator_stats_history(&self, _validator: &Pubkey, _hours: u32) -> Result<Vec<ValidatorStats>> {
        // Implementation simplified for space
        Ok(Vec::new())
    }

    async fn cleanup_old_data(&self, retention_hours: u32) -> Result<u64> {
        let result = sqlx::query(
            "DELETE FROM bundle_submissions WHERE created_at < NOW() - INTERVAL $1"
        )
        .bind(format!("{} hours", retention_hours))
        .execute(&self.pool)
        .await
        .map_err(|e| anyhow!("Failed to cleanup: {}", e))?;

        Ok(result.rows_affected())
    }

    async fn health_check(&self) -> Result<bool> {
        match sqlx::query("SELECT 1").fetch_one(&self.pool).await {
            Ok(_) => Ok(true),
            Err(_) => Ok(false),
        }
    }
}
