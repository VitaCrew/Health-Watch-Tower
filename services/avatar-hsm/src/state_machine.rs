//! Deterministic HSM state reducer and PostgreSQL journal boundary.
//! Payloads are normalized facts or opaque source references, never credentials
//! or raw provider documents. Append, projection, snapshots, and outbox commit
//! in the caller's transaction.

use anyhow::{bail, Context, Result};
use ring::digest::{digest, SHA256};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tokio_postgres::Transaction;

pub const REDUCER_VERSION: i32 = 1;
const SNAPSHOT_INTERVAL: i64 = 100;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct StateEvent {
    pub event_id: String,
    pub event_type: String,
    pub event_version: i32,
    pub actor_id: String,
    pub occurred_at: chrono::DateTime<chrono::Utc>,
    pub source_ref: Option<String>,
    pub correlation_id: Option<String>,
    pub causation_id: Option<String>,
    pub consent_version: Option<String>,
    pub rule_version: Option<String>,
    pub payload: Value,
}

/// Pure versioned transition shared by live writes and historical replay.
pub fn reduce(mut state: Value, sequence: i64, event: &StateEvent) -> Result<Value> {
    if sequence <= 0 || event.event_version != 1 {
        bail!("unsupported HSM event sequence or version");
    }
    if !matches!(
        event.event_type.as_str(),
        "medication_response_confirmed"
            | "acute_signal_evaluated"
            | "normalized_observation_batch_committed"
            | "confirmed_diary_fact_recorded"
            | "completed_sleep_session_recorded"
            | "clinical_record_committed"
    ) {
        bail!("unsupported HSM state event type");
    }
    let root = state
        .as_object_mut()
        .context("HSM state must be an object")?;
    let streams = root.entry("streams").or_insert_with(|| json!({}));
    let streams = streams
        .as_object_mut()
        .context("HSM streams must be an object")?;
    streams.insert(
        event.event_type.clone(),
        json!({
            "sequence": sequence,
            "event_id": event.event_id,
            "occurred_at": event.occurred_at,
            "source_ref": event.source_ref,
            "rule_version": event.rule_version,
            "fact": event.payload,
        }),
    );
    if event.event_type == "clinical_record_committed" {
        let reference = event.payload["record_ref"]
            .as_str()
            .context("clinical state event requires a record reference")?;
        let records = root.entry("clinical_records").or_insert_with(|| json!({}));
        let records = records
            .as_object_mut()
            .context("HSM clinical_records projection must be an object")?;
        records.insert(
            reference.to_owned(),
            json!({
                "sequence": sequence,
                "event_id": event.event_id,
                "record_type": event.payload["record_type"],
                "clinical_status": event.payload["clinical_status"],
                "code": event.payload["code"],
                "display": event.payload["display"],
                "occurred_at": event.payload["occurred_at"],
                "source_ref": event.source_ref,
            }),
        );
    }
    root.insert("sequence".into(), json!(sequence));
    root.insert("reducer_version".into(), json!(REDUCER_VERSION));
    Ok(state)
}

/// Append with optional optimistic sequence check and per-subject row locking.
pub async fn append(
    tx: &Transaction<'_>,
    subject_id: &str,
    event: &StateEvent,
    expected_sequence: Option<i64>,
) -> Result<i64> {
    tx.execute(
        "INSERT INTO avatar_hsm.state_current(subject_id) VALUES($1) ON CONFLICT DO NOTHING",
        &[&subject_id],
    )
    .await?;
    let current = tx
        .query_one(
            "SELECT sequence,state FROM avatar_hsm.state_current WHERE subject_id=$1 FOR UPDATE",
            &[&subject_id],
        )
        .await?;
    let previous: i64 = current.get(0);
    let state: Value = current.get(1);
    if let Some(row) = tx.query_opt(
        "SELECT sequence,event_type,event_version,actor_id,occurred_at,source_ref,correlation_id,
                causation_id,consent_version,rule_version,payload
         FROM avatar_hsm.state_event WHERE subject_id=$1 AND event_id=$2",
        &[&subject_id, &event.event_id]).await? {
        let same = row.get::<_, String>(1) == event.event_type
            && row.get::<_, i32>(2) == event.event_version
            && row.get::<_, String>(3) == event.actor_id
            && row.get::<_, chrono::DateTime<chrono::Utc>>(4) == event.occurred_at
            && row.get::<_, Option<String>>(5) == event.source_ref
            && row.get::<_, Option<String>>(6) == event.correlation_id
            && row.get::<_, Option<String>>(7) == event.causation_id
            && row.get::<_, Option<String>>(8) == event.consent_version
            && row.get::<_, Option<String>>(9) == event.rule_version
            && row.get::<_, Value>(10) == event.payload;
        if !same { bail!("HSM event ID was reused with different content"); }
        return Ok(row.get(0));
    }
    if let Some(expected) = expected_sequence {
        if expected != previous {
            bail!("HSM state sequence conflict: expected {expected}, found {previous}");
        }
    }
    let sequence = previous + 1;
    let next = reduce(state, sequence, event)?;
    let state_hash = hash(&next)?;
    tx.execute(
        "INSERT INTO avatar_hsm.state_event
         (subject_id,sequence,event_id,event_type,event_version,actor_id,occurred_at,source_ref,
          correlation_id,causation_id,consent_version,rule_version,payload)
         VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)",
        &[
            &subject_id,
            &sequence,
            &event.event_id,
            &event.event_type,
            &event.event_version,
            &event.actor_id,
            &event.occurred_at,
            &event.source_ref,
            &event.correlation_id,
            &event.causation_id,
            &event.consent_version,
            &event.rule_version,
            &event.payload,
        ],
    )
    .await?;
    tx.execute(
        "UPDATE avatar_hsm.state_current SET sequence=$2,state=$3,state_sha256=$4,
         reducer_version=$5,updated_at=now() WHERE subject_id=$1",
        &[&subject_id, &sequence, &next, &state_hash, &REDUCER_VERSION],
    )
    .await?;
    if sequence % SNAPSHOT_INTERVAL == 0 {
        tx.execute("INSERT INTO avatar_hsm.state_snapshot(subject_id,sequence,state,state_sha256,reducer_version)
                    VALUES($1,$2,$3,$4,$5)",
                   &[&subject_id, &sequence, &next, &state_hash, &REDUCER_VERSION]).await?;
    }
    tx.execute(
        "INSERT INTO avatar_hsm.state_outbox(subject_id,sequence,event_id) VALUES($1,$2,$3)",
        &[&subject_id, &sequence, &event.event_id],
    )
    .await?;
    Ok(sequence)
}

/// Rebuild the current state from the newest compatible snapshot and event tail.
pub async fn replay(tx: &Transaction<'_>, subject_id: &str) -> Result<(i64, Value)> {
    tx.execute(
        "INSERT INTO avatar_hsm.state_current(subject_id) VALUES($1) ON CONFLICT DO NOTHING",
        &[&subject_id],
    )
    .await?;
    tx.query_one(
        "SELECT sequence FROM avatar_hsm.state_current WHERE subject_id=$1 FOR UPDATE",
        &[&subject_id],
    )
    .await?;
    let snapshot = tx.query_opt(
        "SELECT sequence,state,state_sha256 FROM avatar_hsm.state_snapshot WHERE subject_id=$1 AND reducer_version=$2
         ORDER BY sequence DESC LIMIT 1", &[&subject_id, &REDUCER_VERSION]).await?;
    let (mut sequence, mut state) = if let Some(row) = snapshot {
        let sequence: i64 = row.get(0);
        let state: Value = row.get(1);
        anyhow::ensure!(
            hash(&state)? == row.get::<_, String>(2),
            "HSM snapshot hash mismatch at sequence {sequence}"
        );
        (sequence, state)
    } else {
        (
            0,
            json!({"streams": {}, "sequence": 0, "reducer_version": REDUCER_VERSION}),
        )
    };
    let events = tx.query(
        "SELECT sequence,event_id,event_type,event_version,actor_id,occurred_at,source_ref,correlation_id,
                causation_id,consent_version,rule_version,payload FROM avatar_hsm.state_event
         WHERE subject_id=$1 AND sequence>$2 ORDER BY sequence", &[&subject_id, &sequence]).await?;
    for row in events {
        sequence = row.get(0);
        let event = StateEvent {
            event_id: row.get(1),
            event_type: row.get(2),
            event_version: row.get(3),
            actor_id: row.get(4),
            occurred_at: row.get(5),
            source_ref: row.get(6),
            correlation_id: row.get(7),
            causation_id: row.get(8),
            consent_version: row.get(9),
            rule_version: row.get(10),
            payload: row.get(11),
        };
        anyhow::ensure!(
            sequence == state["sequence"].as_i64().unwrap_or(0) + 1,
            "HSM event journal sequence gap at {sequence}"
        );
        state = reduce(state, sequence, &event)?;
    }
    let state_hash = hash(&state)?;
    tx.execute(
        "INSERT INTO avatar_hsm.state_current(subject_id,sequence,state,state_sha256,reducer_version)
         VALUES($1,$2,$3,$4,$5) ON CONFLICT(subject_id) DO UPDATE SET sequence=EXCLUDED.sequence,
         state=EXCLUDED.state,state_sha256=EXCLUDED.state_sha256,reducer_version=EXCLUDED.reducer_version,
         updated_at=now()", &[&subject_id, &sequence, &state, &state_hash, &REDUCER_VERSION])
        .await.context("storing replayed HSM state")?;
    Ok((sequence, state))
}

fn hash(state: &Value) -> Result<String> {
    let bytes = serde_json::to_vec(state)?;
    Ok(digest(&SHA256, &bytes)
        .as_ref()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect())
}
