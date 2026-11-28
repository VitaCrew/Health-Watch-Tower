//! HSM-owned write boundary for validated, normalized health observations.
//!
//! Collection code may build a provider snapshot, but persistence, replay
//! validation, source-record deduplication, and personal-state rebuilding stay
//! behind this API. Watch Tower's ETL adapter calls this boundary after
//! extraction and normalization.

use anyhow::Result;
use chrono::{DateTime, Utc};
use deadpool_postgres::Pool;
use serde_json::Value;

pub use crate::wearables::Snapshot;

/// Validate and apply one normalized device observation batch to the person's
/// HSM-owned records and state. `replace_source_window` is reserved for source
/// reconciliation flows; ordinary incremental runs should pass `false`.
pub async fn apply_normalized_snapshot(
    pool: &Pool,
    subject_id: &str,
    provider: &str,
    snapshot: Snapshot,
    replace_source_window: bool,
    etl_run_id: Option<&str>,
) -> Result<Value> {
    crate::wearables::store_snapshot_with_run(
        pool,
        subject_id,
        provider,
        snapshot,
        replace_source_window,
        etl_run_id,
    )
    .await
}

/// Read the HSM-owned materialized state through the service boundary.
pub async fn current_state(pool: &Pool, subject_id: &str) -> Result<Option<(i64, Value)>> {
    let db = pool.get().await?;
    let row = db
        .query_opt(
            "SELECT sequence,state FROM avatar_hsm.state_current WHERE subject_id=$1",
            &[&subject_id],
        )
        .await?;
    Ok(row.map(|row| (row.get(0), row.get(1))))
}

/// Rebuild the projection from journal and snapshots in one HSM-owned transaction.
pub async fn replay_state(pool: &Pool, subject_id: &str) -> Result<(i64, Value)> {
    let mut db = pool.get().await?;
    let tx = db.transaction().await?;
    let state = crate::state_machine::replay(&tx, subject_id).await?;
    tx.commit().await?;
    Ok(state)
}

/// Apply a Watch Tower-validated clinical record reference to the HSM journal.
/// The clinical payload remains in Watch Tower; only a small state fact and
/// opaque provenance reference cross this owner boundary.
pub async fn apply_clinical_record(
    pool: &Pool,
    subject_id: &str,
    record_ref: &str,
    event_id: &str,
    occurred_at: DateTime<Utc>,
    consent_version: i64,
    fact: Value,
) -> Result<i64> {
    anyhow::ensure!(
        !subject_id.trim().is_empty()
            && !record_ref.trim().is_empty()
            && !event_id.trim().is_empty()
            && consent_version > 0
            && fact["record_ref"].as_str() == Some(record_ref),
        "invalid clinical HSM update"
    );
    let event = crate::state_machine::StateEvent {
        event_id: event_id.to_owned(),
        event_type: "clinical_record_committed".into(),
        event_version: 1,
        actor_id: "service:watch-tower".into(),
        occurred_at,
        source_ref: Some(record_ref.to_owned()),
        correlation_id: Some(record_ref.to_owned()),
        causation_id: None,
        consent_version: Some(consent_version.to_string()),
        rule_version: Some("clinical-record-v1".into()),
        payload: fact,
    };
    let mut db = pool.get().await?;
    let tx = db.transaction().await?;
    let sequence = crate::state_machine::append(&tx, subject_id, &event, None).await?;
    tx.commit().await?;
    Ok(sequence)
}

#[derive(Clone, Debug)]
pub struct StateChangeNotice {
    pub subject_id: String,
    pub sequence: i64,
    pub event_id: String,
    pub event_type: String,
    pub occurred_at: DateTime<Utc>,
    pub correlation_id: Option<String>,
}

/// Claim payload-free state-change notices for Watch Tower's follow-up router.
pub async fn claim_state_change_notices(
    pool: &Pool,
    worker_id: &str,
    limit: i64,
) -> Result<Vec<StateChangeNotice>> {
    anyhow::ensure!(
        (1..=120).contains(&worker_id.len())
            && worker_id
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || b"-_.".contains(&byte)),
        "invalid state-change worker ID"
    );
    let mut db = pool.get().await?;
    let tx = db.transaction().await?;
    let rows = tx
        .query(
            "WITH picked AS (
             SELECT subject_id,sequence FROM avatar_hsm.state_outbox
             WHERE delivered_at IS NULL AND (lease_until IS NULL OR lease_until < now())
             ORDER BY created_at LIMIT $1 FOR UPDATE SKIP LOCKED
         )
         UPDATE avatar_hsm.state_outbox o SET lease_owner=$2,
             lease_until=now()+interval '2 minutes',attempts=o.attempts+1
         FROM picked p JOIN avatar_hsm.state_event e USING(subject_id,sequence)
         WHERE o.subject_id=p.subject_id AND o.sequence=p.sequence
         RETURNING o.subject_id,o.sequence,o.event_id,e.event_type,e.occurred_at,e.correlation_id",
            &[&limit.clamp(1, 100), &worker_id],
        )
        .await?;
    let notices = rows
        .into_iter()
        .map(|row| StateChangeNotice {
            subject_id: row.get(0),
            sequence: row.get(1),
            event_id: row.get(2),
            event_type: row.get(3),
            occurred_at: row.get(4),
            correlation_id: row.get(5),
        })
        .collect();
    tx.commit().await?;
    Ok(notices)
}

/// Acknowledge delivery only for the worker holding the active lease.
pub async fn acknowledge_state_change_notice(
    pool: &Pool,
    subject_id: &str,
    sequence: i64,
    worker_id: &str,
) -> Result<bool> {
    let db = pool.get().await?;
    Ok(db
        .execute(
            "UPDATE avatar_hsm.state_outbox SET delivered_at=COALESCE(delivered_at,now()),
         lease_owner=NULL,lease_until=NULL WHERE subject_id=$1 AND sequence=$2
         AND lease_owner=$3 AND delivered_at IS NULL",
            &[&subject_id, &sequence, &worker_id],
        )
        .await?
        == 1)
}
