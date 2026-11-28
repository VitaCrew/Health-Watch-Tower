use std::time::Duration;

use deadpool_postgres::Pool;
use serde_json::json;
use watch_tower_core::records::RecordEnvelope;

use crate::store;

const WORKER_ID: &str = "watch-tower-api-cloud-receipts";

pub async fn run(hsm_pool: Pool, policy_pool: Pool) {
    loop {
        if let Err(error) = drain(&hsm_pool, &policy_pool).await {
            eprintln!("Watch Tower cloud receipt dispatch failed: {error:#}");
        }
        tokio::time::sleep(Duration::from_secs(5)).await;
    }
}

async fn drain(hsm_pool: &Pool, policy_pool: &Pool) -> anyhow::Result<()> {
    let receipts = avatar_hsm::store::claim_cloud_sync_receipts(hsm_pool, WORKER_ID, 50).await?;
    let mut first_error = None;
    for receipt in receipts {
        if let Err(error) = dispatch_one(hsm_pool, policy_pool, receipt).await {
            first_error.get_or_insert(error);
        }
    }
    first_error.map_or(Ok(()), Err)
}

async fn dispatch_one(
    hsm_pool: &Pool,
    policy_pool: &Pool,
    receipt: avatar_hsm::store::CloudSyncReceipt,
) -> anyhow::Result<()> {
    let envelope = RecordEnvelope {
        subject_id: receipt.subject_id,
        actor_id: "service:hsm-cloud-sync".into(),
        adapter_id: "hsm".into(),
        source_id: format!("cloud-wearable-sync:{}", receipt.provider),
        record_id: format!("snapshot:{}", receipt.request_sha256),
        revision: 1,
        run_id: receipt.run_id,
        observed_at_unix: receipt.observed_at_unix,
        idempotency_key: format!(
            "cloud-wearable-sync:{}:{}",
            receipt.provider, receipt.request_sha256
        ),
    };
    if let Err(error) =
        store::with_record_write(policy_pool, &envelope, &receipt.request_sha256, || async {
            Ok(json!({"status":"synced"}))
        })
        .await
    {
        if let Some(run_id) = envelope.run_id.as_deref() {
            store::record_etl_run_failure(
                policy_pool,
                &envelope.subject_id,
                run_id,
                &envelope.source_id,
                &receipt.request_sha256,
            )
            .await?;
        }
        return Err(error);
    }
    anyhow::ensure!(
        avatar_hsm::store::mark_cloud_sync_receipt_delivered(hsm_pool, receipt.id, WORKER_ID)
            .await?,
        "HSM cloud receipt lease expired before acknowledgement"
    );
    Ok(())
}

#[cfg(test)]
#[path = "cloud_receipts_tests.rs"]
mod tests;
