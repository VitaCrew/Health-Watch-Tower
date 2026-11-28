use anyhow::{Context, Result};
use base64::Engine;
use deadpool_postgres::{Manager, ManagerConfig, Pool, RecyclingMethod};
use serde_json::Value;
use std::future::Future;
use tokio_postgres::NoTls;
use watch_tower_core::{
    policy::{AuditEvent, ConsentGrant, ExternalIdentity, SubjectMapping},
    records::RecordEnvelope,
};

pub async fn connect(url: &str) -> Result<Pool> {
    let config: tokio_postgres::Config = url.parse().context("invalid Watch Tower database URL")?;
    let manager = Manager::from_config(
        config,
        NoTls,
        ManagerConfig {
            recycling_method: RecyclingMethod::Verified,
        },
    );
    let pool = Pool::builder(manager).max_size(8).build()?;
    let mut client = pool
        .get()
        .await
        .context("connecting Watch Tower policy store")?;
    let tx = client.transaction().await?;
    tx.query_one("SELECT pg_advisory_xact_lock(17842, 1)", &[])
        .await?;
    tx.batch_execute(include_str!("../migrations/001-policy-and-audit.sql"))
        .await
        .context("applying Watch Tower policy and audit schema")?;
    tx.batch_execute(include_str!("../migrations/002-followup-dispatch.sql"))
        .await
        .context("applying Watch Tower follow-up dispatch schema")?;
    tx.batch_execute(include_str!(
        "../migrations/003-followup-workflow-lifecycle.sql"
    ))
    .await
    .context("applying Watch Tower follow-up Workflow lifecycle schema")?;
    tx.batch_execute(include_str!("../migrations/004-record-write-ledger.sql"))
        .await
        .context("applying Watch Tower common record-write ledger")?;
    tx.batch_execute(include_str!("../migrations/005-openhuman-account-link.sql"))
        .await
        .context("applying OpenHuman account-link uniqueness")?;
    tx.batch_execute(include_str!(
        "../migrations/006-followup-record-revision.sql"
    ))
    .await
    .context("applying revision-aware follow-up dispatch")?;
    tx.batch_execute(include_str!("../migrations/007-etl-run-provenance.sql"))
        .await
        .context("applying ETL run provenance to the common receipt ledger")?;
    tx.batch_execute(include_str!("../migrations/008-etl-run-ledger.sql"))
        .await
        .context("applying payload-free ETL run ledger")?;
    tx.batch_execute(include_str!("../migrations/009-etl-run-failures.sql"))
        .await
        .context("applying ETL run failure tracking")?;
    tx.batch_execute(include_str!("../migrations/010-wearable-etl-queues.sql"))
        .await
        .context("moving provider scheduling and replay queues into Watch Tower")?;
    tx.batch_execute(include_str!("../migrations/011-wearable-etl-queues.sql"))
        .await
        .context("moving provider document and activity queues into Watch Tower")?;
    tx.batch_execute(include_str!("../migrations/012-wearable-etl-run-state.sql"))
        .await
        .context("adding Watch Tower provider run lifecycle records")?;
    tx.batch_execute(include_str!(
        "../migrations/013-withings-cleanup-leases.sql"
    ))
    .await
    .context("adding Watch Tower Withings cleanup leases")?;
    tx.batch_execute(include_str!("../migrations/014-google-tcx-queue.sql"))
        .await
        .context("moving Google TCX scheduling metadata into Watch Tower")?;
    tx.batch_execute(include_str!("../migrations/015-clinical-fhir-records.sql"))
        .await
        .context("adding Watch Tower clinical FHIR records and HSM outbox")?;
    tx.execute(
        "INSERT INTO watch_tower.schema_migration(version) VALUES (1), (2), (3), (4), (5), (6), (7), (8), (9), (10), (11), (12), (13), (14), (15) ON CONFLICT DO NOTHING",
        &[],
    )
    .await?;
    tx.commit().await?;
    Ok(pool)
}

/// Serialize writes sharing an idempotency key, reject payload changes before
/// calling the domain owner, and record only the common provenance envelope.
/// The owner adapter remains responsible for the domain record and its result.
pub async fn with_record_write<F, Fut>(
    pool: &Pool,
    envelope: &RecordEnvelope,
    request_sha256: &str,
    apply: F,
) -> Result<Value>
where
    F: FnOnce() -> Fut,
    Fut: Future<Output = Result<Value>>,
{
    anyhow::ensure!(
        envelope.is_valid()
            && request_sha256.len() == 64
            && request_sha256
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte)),
        "invalid common record-write envelope"
    );
    let lock_key = serde_json::to_string(&(
        &envelope.subject_id,
        &envelope.adapter_id,
        &envelope.idempotency_key,
    ))?;
    let mut client = pool.get().await?;
    let tx = client.transaction().await?;
    tx.query_one(
        "SELECT pg_advisory_xact_lock(hashtextextended($1,0))",
        &[&lock_key],
    )
    .await?;
    let prior_receipt = tx
        .query_opt(
            "SELECT request_sha256,source_id,record_id,revision,last_run_id::text
             FROM watch_tower.record_receipt
             WHERE subject_id=$1 AND adapter_id=$2 AND idempotency_key=$3
             FOR UPDATE",
            &[
                &envelope.subject_id,
                &envelope.adapter_id,
                &envelope.idempotency_key,
            ],
        )
        .await?;
    let count_for_run = prior_receipt
        .as_ref()
        .is_none_or(|row| row.get::<_, Option<String>>(4).as_deref() != envelope.run_id.as_deref());
    if let Some(row) = prior_receipt {
        anyhow::ensure!(
            row.get::<_, String>(0) == request_sha256
                && row.get::<_, String>(1) == envelope.source_id
                && row.get::<_, String>(2) == envelope.record_id
                && row.get::<_, i64>(3) == envelope.revision,
            "idempotency key was reused for a different record payload"
        );
    }

    let result = apply().await?;
    anyhow::ensure!(
        result["status"] != "conflict",
        "domain adapter rejected a stale record revision"
    );
    if let Some(run_id) = envelope.run_id.as_deref() {
        let changed = tx
            .execute(
                "INSERT INTO watch_tower.etl_run
                    (subject_id,run_id,source_id,status,receipt_count,latest_request_sha256)
                 VALUES($1,$2::text::uuid,$3,'succeeded',1,$4)
                 ON CONFLICT(subject_id,run_id) DO UPDATE
                SET status='succeeded',receipt_count=watch_tower.etl_run.receipt_count+$5::bigint,
                     latest_request_sha256=EXCLUDED.latest_request_sha256,last_receipt_at=now(),
                     last_failure_code=NULL
                 WHERE watch_tower.etl_run.source_id=EXCLUDED.source_id",
                &[
                    &envelope.subject_id,
                    &run_id,
                    &envelope.source_id,
                    &request_sha256,
                    &(count_for_run as i64),
                ],
            )
            .await?;
        anyhow::ensure!(changed == 1, "ETL run ID was reused for a different source");
    }
    let observed_at = envelope.observed_at_unix as f64;
    tx.query_one(
        "INSERT INTO watch_tower.record_receipt
            (subject_id,adapter_id,idempotency_key,request_sha256,source_id,record_id,revision,actor_id,observed_at,last_run_id)
         VALUES($1,$2,$3,$4,$5,$6,$7,$8,to_timestamp($9::double precision),$10::text::uuid)
         ON CONFLICT(subject_id,adapter_id,idempotency_key) DO UPDATE
         SET last_seen_at=now(),last_run_id=COALESCE(EXCLUDED.last_run_id,watch_tower.record_receipt.last_run_id)
         WHERE watch_tower.record_receipt.request_sha256=EXCLUDED.request_sha256
           AND watch_tower.record_receipt.source_id=EXCLUDED.source_id
           AND watch_tower.record_receipt.record_id=EXCLUDED.record_id
           AND watch_tower.record_receipt.revision=EXCLUDED.revision
         RETURNING idempotency_key",
        &[
            &envelope.subject_id,
            &envelope.adapter_id,
            &envelope.idempotency_key,
            &request_sha256,
            &envelope.source_id,
            &envelope.record_id,
            &envelope.revision,
            &envelope.actor_id,
            &observed_at,
            &envelope.run_id,
        ],
    )
    .await?
    .try_get::<_, String>(0)
    .map_err(|_| anyhow::anyhow!("record idempotency metadata changed during write"))?;
    tx.commit().await?;
    Ok(result)
}

/// Record only a fixed failure category; provider or health payload details are
/// deliberately excluded from the ETL execution ledger.
pub async fn record_etl_run_failure(
    pool: &Pool,
    subject_id: &str,
    run_id: &str,
    source_id: &str,
    request_sha256: &str,
) -> Result<()> {
    anyhow::ensure!(
        !subject_id.trim().is_empty()
            && !source_id.trim().is_empty()
            && request_sha256.len() == 64
            && request_sha256
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte)),
        "invalid ETL failure envelope"
    );
    let changed = pool
        .get()
        .await?
        .query_opt(
            "INSERT INTO watch_tower.etl_run
                (subject_id,run_id,source_id,status,receipt_count,failure_count,
                 last_failure_code,latest_request_sha256)
             VALUES($1,$2::text::uuid,$3,'failed',0,1,'record_dispatch_failed',$4)
             ON CONFLICT(subject_id,run_id) DO UPDATE
             SET status='failed',failure_count=watch_tower.etl_run.failure_count+1,
                 last_failure_code='record_dispatch_failed',
                 latest_request_sha256=EXCLUDED.latest_request_sha256,last_receipt_at=now()
             WHERE watch_tower.etl_run.source_id=EXCLUDED.source_id
             RETURNING run_id",
            &[&subject_id, &run_id, &source_id, &request_sha256],
        )
        .await?;
    anyhow::ensure!(
        changed.is_some(),
        "ETL run ID was reused for a different source"
    );
    Ok(())
}

/// Hold the active consent row against revocation until the complete owner
/// write and its Watch Tower receipt/follow-up work have finished. The
/// operator revocation path locks the same row FOR UPDATE, so either the
/// revocation commits first and this check denies the write, or the write
/// holds the consent through its durable effects.
pub async fn with_write_consent<F, Fut>(
    pool: &Pool,
    subject_id: &str,
    client_id: &str,
    expected_version: i64,
    apply: F,
) -> Result<Value>
where
    F: FnOnce() -> Fut,
    Fut: Future<Output = Result<Value>>,
{
    anyhow::ensure!(expected_version > 0, "invalid write-consent version");
    let mut client = pool.get().await?;
    let tx = client.transaction().await?;
    let row = tx
        .query_opt(
            "SELECT version, revoked_at IS NULL AND (expires_at IS NULL OR expires_at > now())
             FROM watch_tower.consent_grant
             WHERE subject_id=$1 AND client_id=$2 AND scope='records.write'
             ORDER BY version DESC LIMIT 1 FOR SHARE",
            &[&subject_id, &client_id],
        )
        .await?;
    let Some(row) = row else {
        anyhow::bail!("write consent is missing or was revoked");
    };
    anyhow::ensure!(
        row.get::<_, i64>(0) == expected_version && row.get::<_, bool>(1),
        "write consent changed or expired before persistence"
    );
    let result = apply().await?;
    tx.commit().await?;
    Ok(result)
}

pub async fn create_followup(
    pool: &Pool,
    request: &watch_tower_core::followups::FollowupRequest,
    assignees: &[String],
) -> Result<Value> {
    anyhow::ensure!(
        !assignees.is_empty() && assignees.iter().all(|assignee| !assignee.trim().is_empty()),
        "a follow-up requires at least one currently authorized assignee"
    );
    let mut client = pool.get().await?;
    let tx = client.transaction().await?;
    let assignees = serde_json::to_value(assignees)?;
    let row = tx.query_opt(
        "SELECT id, idempotency_key, subject_id, adapter_id, record_id, record_revision, reason_code, role, urgency, assignees, dispatch_state \
         FROM watch_tower.followup_request WHERE idempotency_key=$1 FOR UPDATE",
        &[&request.idempotency_key],
    ).await?;
    if let Some(row) = row {
        anyhow::ensure!(
            row.get::<_, String>(2) == request.subject_id
                && row.get::<_, String>(3) == request.adapter_id
                && row.get::<_, String>(4) == request.record_id
                && row.get::<_, i64>(5) == request.record_revision
                && row.get::<_, String>(6) == request.reason_code
                && row.get::<_, String>(7) == request.role
                && row.get::<_, String>(8) == request.urgency,
            "idempotency key was reused for a different follow-up"
        );
        if row.get::<_, String>(10) == "pending" {
            tx.execute(
                "UPDATE watch_tower.followup_request SET assignees=$2 WHERE id=$1",
                &[&row.get::<_, i64>(0), &assignees],
            )
            .await?;
        }
    } else {
        tx.execute(
        "INSERT INTO watch_tower.followup_request \
         (idempotency_key, subject_id, adapter_id, record_id, record_revision, reason_code, role, urgency, assignees) \
         VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9) ON CONFLICT DO NOTHING",
        &[&request.idempotency_key, &request.subject_id, &request.adapter_id, &request.record_id,
          &request.record_revision, &request.reason_code, &request.role, &request.urgency, &assignees],
        ).await.context("creating generic Watch Tower follow-up")?;
    }
    let row = tx.query_one(
        "SELECT id, idempotency_key, subject_id, adapter_id, record_id, record_revision, reason_code, role, urgency, assignees, dispatch_state \
         FROM watch_tower.followup_request WHERE idempotency_key=$1 \
         UNION ALL SELECT id, idempotency_key, subject_id, adapter_id, record_id, record_revision, reason_code, role, urgency, assignees, dispatch_state \
         FROM watch_tower.followup_request WHERE subject_id=$2 AND adapter_id=$3 AND record_id=$4 AND record_revision=$5 AND reason_code=$6 AND role=$7 AND urgency=$8 \
         LIMIT 1",
        &[&request.idempotency_key, &request.subject_id, &request.adapter_id, &request.record_id, &request.record_revision,
          &request.reason_code, &request.role, &request.urgency],
    ).await.context("reading idempotent Watch Tower follow-up")?;
    anyhow::ensure!(
        row.get::<_, String>(2) == request.subject_id
            && row.get::<_, String>(3) == request.adapter_id
            && row.get::<_, String>(4) == request.record_id
            && row.get::<_, i64>(5) == request.record_revision
            && row.get::<_, String>(6) == request.reason_code
            && row.get::<_, String>(7) == request.role
            && row.get::<_, String>(8) == request.urgency,
        "idempotency key was reused for a different follow-up"
    );
    tx.commit().await?;
    Ok(serde_json::json!({
        "id": row.get::<_, i64>(0),
        "idempotency_key": row.get::<_, String>(1),
        "subject_id": row.get::<_, String>(2),
        "adapter_id": row.get::<_, String>(3),
        "record_id": row.get::<_, String>(4),
        "record_revision": row.get::<_, i64>(5),
        "reason_code": row.get::<_, String>(6),
        "role": row.get::<_, String>(7),
        "urgency": row.get::<_, String>(8),
        "assignees": row.get::<_, serde_json::Value>(9),
        "dispatch_state": row.get::<_, String>(10),
    }))
}

pub async fn refresh_claimed_followup_assignees(
    pool: &Pool,
    idempotency_key: &str,
    worker_id: &str,
    assignees: &[String],
) -> Result<bool> {
    let client = pool.get().await?;
    let value = serde_json::to_value(assignees)?;
    let updated = client.execute(
        "UPDATE watch_tower.followup_request SET assignees=$3, \
           dispatch_state=CASE WHEN jsonb_array_length($3)=0 THEN 'pending' ELSE dispatch_state END, \
           lease_owner=CASE WHEN jsonb_array_length($3)=0 THEN NULL ELSE lease_owner END, \
           lease_until=CASE WHEN jsonb_array_length($3)=0 THEN NULL ELSE lease_until END \
         WHERE idempotency_key=$1 AND lease_owner=$2 AND dispatch_state='dispatching' AND lease_until > now()",
        &[&idempotency_key, &worker_id, &value],
    ).await?;
    Ok(updated == 1)
}

pub async fn claim_followups(pool: &Pool, worker_id: &str, limit: i64) -> Result<Vec<Value>> {
    let mut client = pool.get().await?;
    let tx = client.transaction().await?;
    let rows = tx.query(
        "WITH picked AS (\
           SELECT id FROM watch_tower.followup_request \
           WHERE (dispatch_state = 'pending' OR (dispatch_state = 'dispatching' AND lease_until < now())) \
             AND jsonb_array_length(assignees) > 0 \
           ORDER BY created_at, id LIMIT $1 FOR UPDATE SKIP LOCKED\
         ) UPDATE watch_tower.followup_request f \
           SET dispatch_state = 'dispatching', lease_owner = $2, lease_until = now() + interval '5 minutes' \
           FROM picked WHERE f.id = picked.id \
           RETURNING f.id, f.idempotency_key, f.subject_id, f.adapter_id, f.record_id, f.record_revision, f.reason_code, f.role, f.urgency, f.assignees",
        &[&limit.clamp(1, 100), &worker_id],
    ).await?;
    let results = rows
        .into_iter()
        .map(|row| {
            serde_json::json!({
                "id": row.get::<_, i64>(0),
                "idempotency_key": row.get::<_, String>(1),
                "subject_id": row.get::<_, String>(2),
                "adapter_id": row.get::<_, String>(3),
                "record_id": row.get::<_, String>(4),
                "record_revision": row.get::<_, i64>(5),
                "reason_code": row.get::<_, String>(6),
                "role": row.get::<_, String>(7),
                "urgency": row.get::<_, String>(8),
                "assignees": row.get::<_, serde_json::Value>(9),
            })
        })
        .collect();
    tx.commit().await?;
    Ok(results)
}

pub async fn bind_followup_workflow(
    pool: &Pool,
    idempotency_key: &str,
    worker_id: &str,
    workflow: &watch_tower_core::followups::WorkflowExecutionRef,
) -> Result<bool> {
    anyhow::ensure!(workflow.is_valid(), "invalid Workflow execution reference");
    anyhow::ensure!(
        workflow
            .workflow_id
            .as_deref()
            .is_some_and(|id| !id.trim().is_empty())
            && workflow.task_key == idempotency_key,
        "Workflow reference must identify this follow-up and a saved workflow"
    );
    let client = pool.get().await?;
    let updated = client.execute(
        "UPDATE watch_tower.followup_request SET dispatch_state='dispatched', lease_until=NULL, assigned_actor=$3, \
         workflow_id=$4, workflow_run_id=$5, task_key=$6, dispatched_at=now() \
         WHERE idempotency_key=$1 AND lease_owner=$2 AND dispatch_state='dispatching' AND lease_until > now()",
        &[&idempotency_key, &worker_id, &workflow.assignee_id, &workflow.workflow_id, &workflow.run_id, &workflow.task_key],
    ).await?;
    Ok(updated == 1)
}

/// Store only that OpenHuman observed a terminal Workflow run. The Workflow
/// remains the sole owner of the decision and deadline outcome.
pub async fn finish_followup_workflow(
    pool: &Pool,
    idempotency_key: &str,
    workflow_run_id: &str,
) -> Result<bool> {
    let client = pool.get().await?;
    let updated = client
        .execute(
            "UPDATE watch_tower.followup_request \
             SET dispatch_state='finished', workflow_finished_at=COALESCE(workflow_finished_at, now()) \
             WHERE idempotency_key=$1 AND workflow_run_id=$2 AND dispatch_state IN ('dispatched','finished')",
            &[&idempotency_key, &workflow_run_id],
        )
        .await?;
    Ok(updated == 1)
}

pub async fn resolve_identity(
    pool: &Pool,
    identity: &ExternalIdentity,
) -> Result<Option<SubjectMapping>> {
    let client = pool.get().await?;
    let row = client
        .query_opt(
            "SELECT subject_id, fitness_owner_id FROM watch_tower.identity_link \
         WHERE issuer = $1 AND client_id = $2 AND external_subject = $3 AND revoked_at IS NULL",
            &[&identity.issuer, &identity.client_id, &identity.subject],
        )
        .await?;
    Ok(row.map(|row| SubjectMapping {
        subject_id: row.get(0),
        fitness_owner_id: row.get(1),
    }))
}

/// Resolve the authenticated OpenHuman account assertion to its already
/// provisioned Watch Tower identity. The MCP service bearer authenticates the
/// runtime; this mapping never accepts a subject ID from the caller.
pub async fn resolve_openhuman_user_id(
    pool: &Pool,
    openhuman_user_id: &str,
) -> Result<Option<(SubjectMapping, ExternalIdentity)>> {
    anyhow::ensure!(
        (1..=255).contains(&openhuman_user_id.len())
            && openhuman_user_id
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.')),
        "invalid OpenHuman user ID assertion"
    );
    let client = pool.get().await?;
    let rows = client
        .query(
            "SELECT DISTINCT issuer,client_id,external_subject,subject_id,fitness_owner_id
             FROM watch_tower.identity_link
             WHERE openhuman_user_id=$1 AND client_id='openhuman-local' AND revoked_at IS NULL",
            &[&openhuman_user_id],
        )
        .await?;
    anyhow::ensure!(
        rows.len() <= 1,
        "OpenHuman account maps to multiple Watch Tower identities"
    );
    Ok(rows.into_iter().next().map(|row| {
        let issuer: String = row.get(0);
        let client_id: String = row.get(1);
        let subject: String = row.get(2);
        (
            SubjectMapping {
                subject_id: row.get(3),
                fitness_owner_id: row.get(4),
            },
            ExternalIdentity {
                issuer,
                subject,
                client_id,
            },
        )
    }))
}

pub async fn openhuman_assignees(
    pool: &Pool,
    subject_id: &str,
) -> Result<std::collections::HashMap<String, String>> {
    let client = pool.get().await?;
    let rows = client
        .query(
            "SELECT issuer, external_subject, openhuman_user_id FROM watch_tower.identity_link \
         WHERE subject_id=$1 AND revoked_at IS NULL AND openhuman_user_id IS NOT NULL",
            &[&subject_id],
        )
        .await?;
    let mut users = std::collections::HashMap::new();
    for row in rows {
        let issuer: String = row.get(0);
        let external_subject: String = row.get(1);
        let openhuman_user_id: String = row.get(2);
        let key = fga_user_key(&issuer, &external_subject);
        users.insert(key, openhuman_user_id);
    }
    Ok(users)
}

pub fn fga_user_key(issuer: &str, external_subject: &str) -> String {
    let digest = ring::digest::digest(
        &ring::digest::SHA256,
        format!("{issuer}\0{external_subject}").as_bytes(),
    );
    format!(
        "user:oidc_{}",
        base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(digest.as_ref())
    )
}

pub async fn current_consent(
    pool: &Pool,
    subject_id: &str,
    client_id: &str,
    scope: &str,
) -> Result<Option<ConsentGrant>> {
    let client = pool.get().await?;
    let row = client
        .query_opt(
            "SELECT grant_id, version, EXTRACT(EPOCH FROM expires_at)::bigint, \
                EXTRACT(EPOCH FROM revoked_at)::bigint FROM watch_tower.consent_grant \
         WHERE subject_id = $1 AND client_id = $2 AND scope = $3 \
         ORDER BY version DESC LIMIT 1",
            &[&subject_id, &client_id, &scope],
        )
        .await?;
    Ok(row.map(|row| {
        let expires_at: Option<i64> = row.get(2);
        let revoked_at: Option<i64> = row.get(3);
        ConsentGrant {
            grant_id: row.get(0),
            subject_id: subject_id.to_owned(),
            client_id: client_id.to_owned(),
            scope: scope.to_owned(),
            version: row.get(1),
            expires_at_unix: expires_at,
            revoked_at_unix: revoked_at,
        }
    }))
}

pub async fn begin_audit(pool: &Pool, event: &AuditEvent, client_id: &str) -> Result<i64> {
    let client = pool
        .get()
        .await
        .context("acquiring Watch Tower audit connection")?;
    let row = client.query_one(
        "INSERT INTO watch_tower.mcp_audit \
         (client_id, operation, tool_name, actor_id, subject_id, authorization_allowed, consent_version) \
         VALUES ($1, $2, $3, $4, $5, $6, $7) RETURNING id",
        &[&client_id, &event.operation, &event.resource_id, &event.actor_id,
          &event.subject_id, &event.allowed, &event.consent_version],
    ).await.context("writing Watch Tower MCP audit start")?;
    Ok(row.get(0))
}

pub async fn finish_audit(pool: &Pool, id: i64, succeeded: bool) -> Result<()> {
    let client = pool
        .get()
        .await
        .context("acquiring Watch Tower audit connection")?;
    let outcome = if succeeded { "succeeded" } else { "failed" };
    client
        .execute(
            "UPDATE watch_tower.mcp_audit SET finished_at = now(), outcome = $2 WHERE id = $1",
            &[&id, &outcome],
        )
        .await
        .context("writing Watch Tower MCP audit outcome")?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    #[ignore = "requires an isolated PostgreSQL database in WATCH_TOWER_TEST_DATABASE_URL"]
    async fn domain_conflicts_do_not_create_success_receipts() -> Result<()> {
        let url = std::env::var("WATCH_TOWER_TEST_DATABASE_URL")?;
        let pool = connect(&url).await?;
        let subject_id = format!(
            "receipt-test-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)?
                .as_nanos()
        );
        let envelope = RecordEnvelope {
            subject_id: subject_id.clone(),
            actor_id: "test-actor".into(),
            adapter_id: "hsm".into(),
            source_id: "watch-tower-test".into(),
            record_id: "record-1".into(),
            revision: 1,
            run_id: None,
            observed_at_unix: chrono::Utc::now().timestamp(),
            idempotency_key: "test-conflict-1".into(),
        };
        let result = with_record_write(&pool, &envelope, &"a".repeat(64), || async {
            Ok(serde_json::json!({"status":"conflict"}))
        })
        .await;
        assert!(result.is_err());
        let client = pool.get().await?;
        let receipt_count: i64 = client
            .query_one(
                "SELECT count(*) FROM watch_tower.record_receipt WHERE subject_id=$1",
                &[&subject_id],
            )
            .await?
            .get(0);
        assert_eq!(receipt_count, 0);
        Ok(())
    }

    #[tokio::test]
    #[ignore = "requires an isolated PostgreSQL database in WATCH_TOWER_TEST_DATABASE_URL"]
    async fn followup_replay_is_idempotent_and_new_record_revision_can_route_again() -> Result<()> {
        let url = std::env::var("WATCH_TOWER_TEST_DATABASE_URL")?;
        let pool = connect(&url).await?;
        let suffix = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_nanos();
        let subject_id = format!("followup-test-{suffix}");
        let request = |revision: i64, key: String| watch_tower_core::followups::FollowupRequest {
            idempotency_key: key,
            subject_id: subject_id.clone(),
            adapter_id: "fitness".into(),
            record_id: "set-1".into(),
            record_revision: revision,
            reason_code: "pain_reported".into(),
            role: "trainer".into(),
            urgency: "soon".into(),
        };
        let assignees = ["openhuman-user-1".to_owned()];
        let first = create_followup(
            &pool,
            &request(1, format!("followup-{suffix}-r1")),
            &assignees,
        )
        .await?;
        let replay = create_followup(
            &pool,
            &request(1, format!("followup-{suffix}-r1")),
            &assignees,
        )
        .await?;
        let later_revision = create_followup(
            &pool,
            &request(2, format!("followup-{suffix}-r2")),
            &assignees,
        )
        .await?;
        assert_eq!(first["id"], replay["id"]);
        assert_ne!(first["id"], later_revision["id"]);
        assert_eq!(replay["record_revision"], 1);
        assert_eq!(later_revision["record_revision"], 2);
        pool.get()
            .await?
            .execute(
                "DELETE FROM watch_tower.followup_request WHERE subject_id=$1",
                &[&subject_id],
            )
            .await?;
        Ok(())
    }

    #[tokio::test]
    #[ignore = "requires an isolated PostgreSQL database in WATCH_TOWER_TEST_DATABASE_URL"]
    async fn revoked_write_consent_blocks_the_owner_write_before_persistence() -> Result<()> {
        let url = std::env::var("WATCH_TOWER_TEST_DATABASE_URL")?;
        let pool = connect(&url).await?;
        let suffix = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_nanos();
        let subject_id = format!("consent-test-{suffix}");
        let client_id = format!("client-{suffix}");
        let client = pool.get().await?;
        client
            .execute(
                "INSERT INTO watch_tower.consent_grant
                 (grant_id,subject_id,client_id,scope,version,granted_by_identity)
                 VALUES ($1,$2,$3,'records.write',1,'owner:test')",
                &[&format!("grant-{suffix}"), &subject_id, &client_id],
            )
            .await?;
        let allowed = with_write_consent(&pool, &subject_id, &client_id, 1, || async {
            Ok(serde_json::json!({"saved":true}))
        })
        .await?;
        assert_eq!(allowed["saved"], true);
        client
            .execute(
                "UPDATE watch_tower.consent_grant SET revoked_at=now()
                 WHERE subject_id=$1 AND client_id=$2 AND scope='records.write'",
                &[&subject_id, &client_id],
            )
            .await?;
        let owner_write_called = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let called = owner_write_called.clone();
        let denied = with_write_consent(&pool, &subject_id, &client_id, 1, || async move {
            called.store(true, std::sync::atomic::Ordering::SeqCst);
            Ok(serde_json::json!({"saved":true}))
        })
        .await;
        assert!(denied.is_err());
        assert!(!owner_write_called.load(std::sync::atomic::Ordering::SeqCst));
        client
            .execute(
                "DELETE FROM watch_tower.consent_grant WHERE subject_id=$1 AND client_id=$2",
                &[&subject_id, &client_id],
            )
            .await?;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[ignore = "requires an isolated PostgreSQL database in WATCH_TOWER_TEST_DATABASE_URL"]
    async fn consent_revocation_waits_for_an_inflight_owner_write() -> Result<()> {
        let url = std::env::var("WATCH_TOWER_TEST_DATABASE_URL")?;
        let pool = connect(&url).await?;
        let suffix = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_nanos();
        let subject_id = format!("consent-race-test-{suffix}");
        let client_id = format!("client-{suffix}");
        let client = pool.get().await?;
        client
            .execute(
                "INSERT INTO watch_tower.consent_grant
                 (grant_id,subject_id,client_id,scope,version,granted_by_identity)
                 VALUES ($1,$2,$3,'records.write',1,'owner:test')",
                &[&format!("grant-{suffix}"), &subject_id, &client_id],
            )
            .await?;

        let (write_started_tx, write_started_rx) = tokio::sync::oneshot::channel();
        let (release_write_tx, release_write_rx) = tokio::sync::oneshot::channel();
        let write_pool = pool.clone();
        let write_subject = subject_id.clone();
        let write_client = client_id.clone();
        let write = tokio::spawn(async move {
            with_write_consent(
                &write_pool,
                &write_subject,
                &write_client,
                1,
                || async move {
                    let _ = write_started_tx.send(());
                    release_write_rx
                        .await
                        .map_err(|error| anyhow::anyhow!(error.to_string()))?;
                    Ok(serde_json::json!({"saved":true}))
                },
            )
            .await
        });
        write_started_rx.await?;

        let (revoke_started_tx, revoke_started_rx) = tokio::sync::oneshot::channel();
        let (revoke_done_tx, mut revoke_done_rx) = tokio::sync::oneshot::channel();
        let revoke_pool = pool.clone();
        let revoke_subject = subject_id.clone();
        let revoke_client = client_id.clone();
        let revoke = tokio::spawn(async move {
            let _ = revoke_started_tx.send(());
            let client = revoke_pool.get().await?;
            client
                .execute(
                    "UPDATE watch_tower.consent_grant SET revoked_at=now()
                     WHERE subject_id=$1 AND client_id=$2 AND scope='records.write'",
                    &[&revoke_subject, &revoke_client],
                )
                .await?;
            let _ = revoke_done_tx.send(());
            Ok::<(), anyhow::Error>(())
        });
        revoke_started_rx.await?;
        std::thread::sleep(std::time::Duration::from_millis(100));
        assert!(revoke_done_rx.try_recv().is_err());
        release_write_tx
            .send(())
            .map_err(|_| anyhow::anyhow!("consent race test failed to release write"))?;
        assert_eq!(write.await??["saved"], true);
        revoke.await??;
        revoke_done_rx.await?;
        client
            .execute(
                "DELETE FROM watch_tower.consent_grant WHERE subject_id=$1 AND client_id=$2",
                &[&subject_id, &client_id],
            )
            .await?;
        Ok(())
    }
}
