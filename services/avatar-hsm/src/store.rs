use anyhow::{bail, Context, Result};
use chrono::{DateTime, Utc};
use deadpool_postgres::{Manager, ManagerConfig, Pool, RecyclingMethod};
use serde_json::{json, Value};
use tokio_postgres::{Client, NoTls};

pub const SUBJECT: &str = "synthetic-mvp";

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CloudSyncReceipt {
    pub id: i64,
    pub subject_id: String,
    pub provider: String,
    pub request_sha256: String,
    pub run_id: Option<String>,
    pub observed_at_unix: i64,
}

/// Resolve a durable HSM-owned record reference for a single subject without
/// returning its health payload to Watch Tower.
pub async fn record_exists(db: &Client, subject_id: &str, record_id: &str) -> Result<bool> {
    let row = db
        .query_one(
            "SELECT EXISTS (
                SELECT 1 FROM avatar_hsm.dose WHERE subject_id=$1 AND id=$2
                UNION ALL SELECT 1 FROM avatar_hsm.sleep_session WHERE subject_id=$1 AND id=$2
                UNION ALL SELECT 1 FROM avatar_hsm.diary_entry WHERE subject_id=$1 AND id=$2 AND deleted_at IS NULL
                UNION ALL SELECT 1 FROM avatar_hsm.acute_decision WHERE subject_id=$1 AND event_id=$2
                UNION ALL SELECT 1 FROM avatar_hsm.wearable_record
                  WHERE subject_id=$1 AND concat_ws(':', provider, kind, source, source_id)=$2
            )",
            &[&subject_id, &record_id],
        )
        .await?;
    Ok(row.get(0))
}

pub async fn connect(url: &str) -> Result<Pool> {
    let config: tokio_postgres::Config =
        url.parse().context("invalid HSM database configuration")?;
    let manager = Manager::from_config(
        config,
        NoTls,
        ManagerConfig {
            recycling_method: RecyclingMethod::Verified,
        },
    );
    let pool = Pool::builder(manager).max_size(8).build()?;
    let mut client = pool.get().await.context("connecting to HSM PostgreSQL")?;
    let tx = client.transaction().await?;
    tx.query_one("SELECT pg_advisory_xact_lock(17840, 1)", &[])
        .await?;
    let have_ledger: bool = tx
        .query_one(
            "SELECT to_regclass('avatar_hsm.schema_migration') IS NOT NULL",
            &[],
        )
        .await?
        .get(0);
    if !have_ledger {
        tx.batch_execute(
            "CREATE SCHEMA IF NOT EXISTS avatar_hsm;
            CREATE TABLE avatar_hsm.schema_migration(version integer PRIMARY KEY);",
        )
        .await?;
    }
    let known: Vec<i32> = tx
        .query("SELECT version FROM avatar_hsm.schema_migration", &[])
        .await?
        .into_iter()
        .map(|row| row.get(0))
        .collect();
    for (version, sql) in [
        (1, include_str!("../migrations/001-synthetic.sql")),
        (2, include_str!("../migrations/002-fixture.sql")),
        (5, include_str!("../migrations/005-guardian-push.sql")),
        (6, include_str!("../migrations/006-diary.sql")),
        (7, include_str!("../migrations/007-device-integrations.sql")),
        (8, include_str!("../migrations/008-wearable-sync.sql")),
        (9, include_str!("../migrations/009-wearable-delivery.sql")),
        (10, include_str!("../migrations/010-fit-parts.sql")),
        (11, include_str!("../migrations/011-suunto-fit.sql")),
        (12, include_str!("../migrations/012-garmin-owner.sql")),
        (13, include_str!("../migrations/013-garmin-permissions.sql")),
        (14, include_str!("../migrations/014-garmin-backfill.sql")),
        (15, include_str!("../migrations/015-garmin-pull.sql")),
        (16, include_str!("../migrations/016-garmin-daily.sql")),
        (17, include_str!("../migrations/017-fit-details.sql")),
        (18, include_str!("../migrations/018-samsung-activity.sql")),
        (
            19,
            include_str!("../migrations/019-samsung-workout-routes.sql"),
        ),
        (20, include_str!("../migrations/020-samsung-goals.sql")),
        (
            21,
            include_str!("../migrations/021-fit-running-dynamics.sql"),
        ),
        (22, include_str!("../migrations/022-fit-swim-and-zones.sql")),
        (
            23,
            include_str!("../migrations/023-garmin-daily-activity.sql"),
        ),
        (
            24,
            include_str!("../migrations/024-garmin-offset-samples.sql"),
        ),
        (
            25,
            include_str!("../migrations/025-garmin-hrv-and-epochs.sql"),
        ),
        (
            26,
            include_str!("../migrations/026-garmin-workout-samples.sql"),
        ),
        (
            27,
            include_str!("../migrations/027-garmin-health-summaries.sql"),
        ),
        (
            28,
            include_str!("../migrations/028-apple-export-workout-routes.sql"),
        ),
        (29, include_str!("../migrations/029-healthkit-uuid.sql")),
        (30, include_str!("../migrations/030-health-connect-id.sql")),
        (
            31,
            include_str!("../migrations/031-wearable-claimed-wake.sql"),
        ),
        (
            32,
            include_str!("../migrations/032-wearable-target-days.sql"),
        ),
        (
            33,
            include_str!("../migrations/033-wearable-target-retries.sql"),
        ),
        (34, include_str!("../migrations/034-fit-dive-metrics.sql")),
        (35, include_str!("../migrations/035-google-workout-tcx.sql")),
        (
            36,
            include_str!("../migrations/036-google-tcx-route-points.sql"),
        ),
        (37, include_str!("../migrations/037-withings-notify.sql")),
        (
            38,
            include_str!("../migrations/038-withings-notify-cleanup.sql"),
        ),
        (
            39,
            include_str!("../migrations/039-oura-webhook-documents.sql"),
        ),
        (
            40,
            include_str!("../migrations/040-oura-webhook-deletions.sql"),
        ),
        (41, include_str!("../migrations/041-oura-webhook-order.sql")),
        (
            42,
            include_str!("../migrations/042-whoop-webhook-documents.sql"),
        ),
        (43, include_str!("../migrations/043-fit-events.sql")),
        (
            44,
            include_str!("../migrations/044-health-connect-routes.sql"),
        ),
        (45, include_str!("../migrations/045-healthkit-routes.sql")),
        (
            46,
            include_str!("../migrations/046-fit-developer-fields.sql"),
        ),
        (
            47,
            include_str!("../migrations/047-garmin-menstrual-cycle.sql"),
        ),
        (
            48,
            include_str!("../migrations/048-google-tcx-telemetry.sql"),
        ),
        (
            49,
            include_str!("../migrations/049-oura-polar-daily-energy.sql"),
        ),
        (
            50,
            include_str!("../migrations/050-wearable-sync-throttling.sql"),
        ),
        (51, include_str!("../migrations/051-garmin-retry-after.sql")),
        (
            52,
            include_str!("../migrations/052-garmin-sleep-details.sql"),
        ),
        (
            53,
            include_str!("../migrations/053-garmin-respiration-oxygen-averages.sql"),
        ),
        (54, include_str!("../migrations/054-whoop-daily-energy.sql")),
        (
            55,
            include_str!("../migrations/055-suunto-daily-energy.sql"),
        ),
        (
            56,
            include_str!("../migrations/056-withings-daily-activity.sql"),
        ),
        (
            57,
            include_str!("../migrations/057-ultrahuman-daily-summary.sql"),
        ),
        (58, include_str!("../migrations/058-fit-devices.sql")),
        (
            59,
            include_str!("../migrations/059-strava-webhook-documents.sql"),
        ),
        (
            60,
            include_str!("../migrations/060-wearable-oauth-linked-at.sql"),
        ),
        (61, include_str!("../migrations/061-garmin-move-iq.sql")),
        (62, include_str!("../migrations/062-fit-hrv.sql")),
        (
            63,
            include_str!("../migrations/063-fit-compressed-heart-rate.sql"),
        ),
        (
            64,
            include_str!("../migrations/064-suunto-sleep-metrics.sql"),
        ),
        (
            65,
            include_str!("../migrations/065-polar-daily-activity.sql"),
        ),
        (
            66,
            include_str!("../migrations/066-polar-activity-zones.sql"),
        ),
        (
            67,
            include_str!("../migrations/067-coros-daily-metrics.sql"),
        ),
        (
            68,
            include_str!("../migrations/068-intervals-webhook-documents.sql"),
        ),
        (
            69,
            include_str!("../migrations/069-health-connect-daily-intensity.sql"),
        ),
        (70, include_str!("../migrations/070-fit-strength.sql")),
        (71, include_str!("../migrations/071-fit-splits.sql")),
        (
            72,
            include_str!("../migrations/072-polar-physical-info.sql"),
        ),
        (73, include_str!("../migrations/073-oura-blood-oxygen.sql")),
        (74, include_str!("../migrations/074-oura-sleep-samples.sql")),
        (
            75,
            include_str!("../migrations/075-withings-sleep-samples.sql"),
        ),
        (
            76,
            include_str!("../migrations/076-withings-intraday-samples.sql"),
        ),
        (
            77,
            include_str!("../migrations/077-withings-urine-biomarkers.sql"),
        ),
        (
            78,
            include_str!("../migrations/078-oura-daily-indicators.sql"),
        ),
        (79, include_str!("../migrations/079-oura-activity-met.sql")),
        (
            80,
            include_str!("../migrations/080-fit-cycling-dynamics.sql"),
        ),
        (
            81,
            include_str!("../migrations/081-apple-workout-weather.sql"),
        ),
        (82, include_str!("../migrations/082-fit-respiration.sql")),
        (
            83,
            include_str!("../migrations/083-intervals-fit-consent.sql"),
        ),
        (
            84,
            include_str!("../migrations/084-samsung-sleep-revisions.sql"),
        ),
        (
            85,
            include_str!("../migrations/085-samsung-workout-revisions.sql"),
        ),
        (
            86,
            include_str!("../migrations/086-samsung-heart-rate-revisions.sql"),
        ),
        (
            87,
            include_str!("../migrations/087-samsung-metric-revisions.sql"),
        ),
        (
            88,
            include_str!("../migrations/088-wearable-live-sync-deadline.sql"),
        ),
        (
            90,
            include_str!("../migrations/090-public-catalog-cache.sql"),
        ),
        (91, include_str!("../migrations/091-nutrition-state.sql")),
        (
            92,
            include_str!("../migrations/092-request-scoped-subjects.sql"),
        ),
        (
            93,
            include_str!("../migrations/093-snapshot-write-receipt.sql"),
        ),
        (
            94,
            include_str!("../migrations/094-watch-tower-record-outbox.sql"),
        ),
        (95, include_str!("../migrations/095-etl-run-provenance.sql")),
        (
            96,
            include_str!("../../watch-tower-api/migrations/010-wearable-etl-queues.sql"),
        ),
        (
            97,
            include_str!("../../watch-tower-api/migrations/011-wearable-etl-queues.sql"),
        ),
        (98, include_str!("../migrations/098-hsm-state-journal.sql")),
    ] {
        if known.contains(&version) {
            continue;
        }
        tx.batch_execute(sql)
            .await
            .with_context(|| format!("applying HSM migration {version}"))?;
        tx.execute(
            "INSERT INTO avatar_hsm.schema_migration VALUES($1)",
            &[&version],
        )
        .await?;
    }
    tx.commit().await?;
    Ok(pool)
}

/// Claim payload-free cloud snapshot receipts for the Watch Tower common
/// validation ledger. The outbox remains HSM-owned and is read through this
/// owner API rather than by querying avatar_hsm from Watch Tower SQL.
pub async fn claim_cloud_sync_receipts(
    pool: &Pool,
    worker_id: &str,
    limit: i64,
) -> Result<Vec<CloudSyncReceipt>> {
    anyhow::ensure!(
        (1..=120).contains(&worker_id.len())
            && worker_id
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || b"-_.".contains(&byte)),
        "invalid cloud receipt worker ID"
    );
    let mut client = pool.get().await?;
    let tx = client.transaction().await?;
    let rows = tx
        .query(
            "WITH picked AS (
                 SELECT id FROM avatar_hsm.watch_tower_record_outbox
                 WHERE delivered_at IS NULL AND (lease_until IS NULL OR lease_until < now())
                 ORDER BY id LIMIT $1 FOR UPDATE SKIP LOCKED
             )
             UPDATE avatar_hsm.watch_tower_record_outbox AS outbox
             SET lease_owner=$2, lease_until=now()+interval '2 minutes', attempts=outbox.attempts+1
             FROM picked WHERE outbox.id=picked.id
             RETURNING outbox.id,outbox.subject_id,outbox.provider,outbox.request_sha256,outbox.run_id::text,
                       EXTRACT(EPOCH FROM outbox.observed_at)::bigint",
            &[&limit.clamp(1, 100), &worker_id],
        )
        .await?;
    let receipts = rows
        .into_iter()
        .map(|row| CloudSyncReceipt {
            id: row.get(0),
            subject_id: row.get(1),
            provider: row.get(2),
            request_sha256: row.get(3),
            run_id: row.get(4),
            observed_at_unix: row.get(5),
        })
        .collect();
    tx.commit().await?;
    Ok(receipts)
}

pub async fn mark_cloud_sync_receipt_delivered(
    pool: &Pool,
    receipt_id: i64,
    worker_id: &str,
) -> Result<bool> {
    let client = pool.get().await?;
    let changed = client
        .execute(
            "UPDATE avatar_hsm.watch_tower_record_outbox
             SET delivered_at=COALESCE(delivered_at,now()),lease_owner=NULL,lease_until=NULL
             WHERE id=$1 AND lease_owner=$2 AND delivered_at IS NULL",
            &[&receipt_id, &worker_id],
        )
        .await?;
    Ok(changed == 1)
}

pub async fn medication_candidates(db: &Client, name: &str) -> Result<Value> {
    medication_candidates_for(db, name, SUBJECT).await
}

pub async fn medication_candidates_for(db: &Client, name: &str, subject_id: &str) -> Result<Value> {
    let name = name.trim();
    if !(3..=120).contains(&name.chars().count()) {
        bail!("enter 3 to 120 characters of the medication name");
    }
    let rows = db
        .query(
            "SELECT item_seq, display_name FROM avatar_hsm.product
         WHERE display_name % $1 OR display_name ILIKE '%' || $1 || '%'
         ORDER BY similarity(display_name, $1) DESC, item_seq LIMIT 8",
            &[&name],
        )
        .await?;
    let candidates: Vec<_> = rows
        .iter()
        .map(|row| {
            json!({
                "item_seq": row.get::<_, String>(0),
                "display_name": row.get::<_, String>(1),
            })
        })
        .collect();
    Ok(
        json!({ "candidates": candidates, "requires_item_confirmation": true, "synthetic": subject_id == SUBJECT }),
    )
}

pub async fn dur(db: &Client, item_seq: Option<&str>) -> Value {
    dur_for(db, item_seq, SUBJECT).await
}

pub async fn dur_for(db: &Client, item_seq: Option<&str>, subject_id: &str) -> Value {
    let Some(code) = item_seq.filter(|item| !item.trim().is_empty()) else {
        return json!({ "status": "item_unconfirmed", "synthetic": subject_id == SUBJECT });
    };
    let checked = db
        .query_opt(
            "WITH active AS (
           SELECT r.item_seq, p.display_name, c.availability, c.dataset_version
           FROM avatar_hsm.regimen r
           JOIN avatar_hsm.product p USING (item_seq)
           LEFT JOIN avatar_hsm.dur_coverage c USING (item_seq)
           WHERE subject_id = $2 AND effective_from <= now()
             AND (effective_to IS NULL OR effective_to > now())
         )
         SELECT c.dataset_version, c.availability = 'available'
                AND NOT EXISTS (
                    SELECT FROM active
                    WHERE availability IS DISTINCT FROM 'available'
                      OR dataset_version IS DISTINCT FROM c.dataset_version
                ) AS covered,
                COALESCE((
                    SELECT jsonb_agg(jsonb_build_object(
                        'item_seq', a.item_seq, 'name', a.display_name,
                        'reason', d.reason, 'source_url', d.source_url
                    ) ORDER BY a.item_seq)
                    FROM active a JOIN avatar_hsm.dur_pair d
                    ON d.first_item_seq = LEAST($1, a.item_seq)
                   AND d.second_item_seq = GREATEST($1, a.item_seq)
                ), '[]'::jsonb) AS matches
         FROM avatar_hsm.product p LEFT JOIN avatar_hsm.dur_coverage c USING (item_seq)
         WHERE p.item_seq = $1",
            &[&code, &subject_id],
        )
        .await;
    match checked {
        Ok(None) => json!({ "status": "item_unconfirmed", "synthetic": subject_id == SUBJECT }),
        Ok(Some(row)) => {
            let matches: Value = row.get("matches");
            let covered: Option<bool> = row.get("covered");
            let status = if matches
                .as_array()
                .is_some_and(|entries| !entries.is_empty())
            {
                "contraindication_match"
            } else if covered == Some(true) {
                "no_match_in_scope"
            } else {
                "lookup_failed"
            };
            json!({
                "item_seq": code,
                "status": status,
                "dataset_version": row.get::<_, Option<String>>("dataset_version"),
                "matches": matches,
                "synthetic": subject_id == SUBJECT,
            })
        }
        Err(_) => json!({ "status": "lookup_failed", "synthetic": true }),
    }
}

pub async fn request_due(db: &Client, now: DateTime<Utc>) -> Result<Value> {
    let requested = db
        .query(
            "UPDATE avatar_hsm.dose SET requested_at = $1
         WHERE subject_id = $2 AND requested_at IS NULL
           AND eligible_at <= $1 AND expires_at > $1
         RETURNING id",
            &[&now, &SUBJECT],
        )
        .await?;
    Ok(
        json!({ "created": requested.into_iter().map(|row| row.get::<_, String>(0)).collect::<Vec<_>>() }),
    )
}

pub async fn reminders(db: &Client, now: DateTime<Utc>) -> Result<Value> {
    let row = db
        .query_one(
            "SELECT COALESCE(jsonb_agg(entry ORDER BY scheduled_at, dose_id), '[]'::jsonb)
         FROM (
           SELECT d.id AS dose_id, d.scheduled_at, jsonb_build_object(
               'dose_id', d.id, 'item_seq', p.item_seq, 'name', p.display_name,
               'scheduled_at', d.scheduled_at, 'expires_at', d.expires_at,
               'timezone', s.timezone,
               'state', COALESCE(d.response, CASE WHEN d.expires_at <= $1
                    THEN 'unconfirmed' ELSE 'awaiting_response' END)
           ) AS entry
           FROM avatar_hsm.dose d JOIN avatar_hsm.product p USING (item_seq)
           JOIN avatar_hsm.subject s ON s.id = d.subject_id
           WHERE d.subject_id = $2 AND (d.requested_at IS NOT NULL OR d.expires_at <= $1)
           ORDER BY d.scheduled_at DESC, d.id LIMIT 30
         ) entries",
            &[&now, &SUBJECT],
        )
        .await?;
    Ok(json!({ "reminders": row.get::<_, Value>(0), "synthetic": true }))
}

/// Read medication facts for OpenHuman's reminder scheduler without changing
/// HSM state. OpenHuman owns due-time evaluation, snoozing, idempotency and
/// delivery; HSM returns only the current regimen and recorded responses.
pub async fn health_status_v1(db: &Client, now: DateTime<Utc>) -> Result<Value> {
    health_status_v1_for(db, SUBJECT, now).await
}

pub async fn health_status_v1_for(
    db: &Client,
    subject_id: &str,
    now: DateTime<Utc>,
) -> Result<Value> {
    let row = db
        .query_one(
            "SELECT COALESCE(jsonb_agg(entry ORDER BY scheduled_at, dose_id), '[]'::jsonb)
         FROM (
           SELECT d.id AS dose_id, d.scheduled_at, jsonb_build_object(
               'dose_id', d.id, 'item_seq', p.item_seq, 'name', p.display_name,
               'scheduled_at', d.scheduled_at, 'eligible_at', d.eligible_at,
               'expires_at', d.expires_at, 'timezone', s.timezone,
               'response', d.response, 'responded_at', d.responded_at
           ) AS entry
           FROM avatar_hsm.dose d JOIN avatar_hsm.product p USING (item_seq)
           JOIN avatar_hsm.subject s ON s.id = d.subject_id
           WHERE d.subject_id = $2
              AND d.scheduled_at <= $1::timestamptz + interval '24 hours'
              AND d.expires_at > $1::timestamptz - interval '24 hours'
           ORDER BY d.scheduled_at, d.id
         ) entries",
            &[&now, &subject_id],
        )
        .await?;
    Ok(json!({
        "state": "ready",
        "checked_at": now,
        "medication_doses": row.get::<_, Value>(0),
        "synthetic": subject_id == SUBJECT,
    }))
}

/// Project scheduled medication doses into the common plan/actual reference
/// contract without returning medication names or other health payloads.
pub async fn plan_actual_links_for(
    db: &Client,
    subject_id: &str,
    start: DateTime<Utc>,
    end: DateTime<Utc>,
) -> Result<Value> {
    anyhow::ensure!(
        start < end && end - start <= chrono::Duration::days(366),
        "select a range of up to 366 days"
    );
    let rows = db
        .query(
            "SELECT id,scheduled_at,response,responded_at FROM avatar_hsm.dose
         WHERE subject_id=$1 AND scheduled_at >= $2 AND scheduled_at < $3
         ORDER BY scheduled_at,id",
            &[&subject_id, &start, &end],
        )
        .await?;
    let mut links = Vec::with_capacity(rows.len());
    for row in rows {
        let id: String = row.get(0);
        let scheduled_at: DateTime<Utc> = row.get(1);
        let response: Option<String> = row.get(2);
        let responded_at: Option<DateTime<Utc>> = row.get(3);
        let state = match response.as_deref() {
            Some("taken") => "completed",
            Some("skipped") => "skipped",
            Some("not_yet") => "deferred",
            _ => "planned",
        };
        let planned = watch_tower_core::plans::PlannedReference {
            subject_id: subject_id.to_owned(),
            adapter_id: "hsm".to_owned(),
            plan_id: id.clone(),
            revision: 1,
            planned_at_unix: Some(scheduled_at.timestamp()),
        };
        let actual = match (response.as_ref(), responded_at) {
            (Some(_), Some(responded_at)) => Some(watch_tower_core::plans::ActualReference {
                subject_id: subject_id.to_owned(),
                adapter_id: "hsm".to_owned(),
                record_id: id.clone(),
                revision: 1,
                occurred_at_unix: responded_at.timestamp(),
            }),
            _ => None,
        };
        let observation = watch_tower_core::plans::PlanActualObservation {
            planned,
            actual,
            target_key: None,
            state: state.to_owned(),
        };
        anyhow::ensure!(observation.is_valid(), "invalid HSM plan/actual reference");
        links.push(serde_json::json!({
            "planned": {
                "subject_id": observation.planned.subject_id,
                "adapter_id": observation.planned.adapter_id,
                "plan_id": observation.planned.plan_id,
                "revision": observation.planned.revision,
                "planned_at_unix": observation.planned.planned_at_unix
            },
            "actual": observation.actual.map(|actual| serde_json::json!({
                "subject_id": actual.subject_id,
                "adapter_id": actual.adapter_id,
                "record_id": actual.record_id,
                "revision": actual.revision,
                "occurred_at_unix": actual.occurred_at_unix
            })),
            "target_key": observation.target_key,
            "state": observation.state
        }));
    }
    Ok(serde_json::json!({"links":links}))
}

pub async fn respond(db: &Client, id: &str, response: &str, now: DateTime<Utc>) -> Result<Value> {
    if !matches!(response, "taken" | "not_yet" | "skipped") {
        bail!("invalid medication response");
    }
    let row = db
        .query_opt(
            "UPDATE avatar_hsm.dose SET response = $1,
             requested_at = COALESCE(requested_at, $2),
             responded_at = COALESCE(responded_at, $2)
         WHERE id = $3 AND subject_id = $4 AND requested_at IS NOT NULL
           AND ((response IS NULL AND expires_at > $2) OR response = $1)
         RETURNING response, responded_at",
            &[&response, &now, &id, &SUBJECT],
        )
        .await?;
    let Some(row) = row else {
        bail!("the dose request is missing, closed, or already answered")
    };
    Ok(json!({
        "dose_id": id,
        "response": row.get::<_, String>(0),
        "responded_at": row.get::<_, DateTime<Utc>>(1)
    }))
}

/// Store a user-confirmed medication fact. `not_yet` is intentionally not a
/// health fact: OpenHuman stores that snooze against its own reminder row.
pub async fn respond_v1(
    pool: &Pool,
    id: &str,
    response: &str,
    now: DateTime<Utc>,
) -> Result<Value> {
    respond_v1_for(pool, SUBJECT, id, response, now).await
}

pub async fn respond_v1_for(
    pool: &Pool,
    subject_id: &str,
    id: &str,
    response: &str,
    now: DateTime<Utc>,
) -> Result<Value> {
    respond_v1_for_at(pool, subject_id, id, response, now).await
}

/// Apply a Workflow decision using its durable acceptance time. This lets a
/// retry after a deadline persist an outcome that won the Workflow resume CAS
/// before expiry, while SQL still rejects genuinely late first decisions.
pub async fn respond_v1_for_at(
    pool: &Pool,
    subject_id: &str,
    id: &str,
    response: &str,
    accepted_at: DateTime<Utc>,
) -> Result<Value> {
    if !matches!(response, "taken" | "skipped") {
        bail!("health status accepts only confirmed taken or skipped responses");
    }
    if accepted_at > Utc::now() + chrono::Duration::seconds(30) {
        bail!("medication response acceptance time is in the future");
    }
    let mut db = pool.get().await?;
    let tx = db.transaction().await?;
    let row = tx
        .query_opt(
            "UPDATE avatar_hsm.dose SET response = $1,
             requested_at = COALESCE(requested_at, $2),
             responded_at = COALESCE(responded_at, $2)
         WHERE id = $3 AND subject_id = $4 AND eligible_at <= $2
           AND ((response IS NULL AND expires_at > $2) OR response = $1)
         RETURNING response, responded_at",
            &[&response, &accepted_at, &id, &subject_id],
        )
        .await?;
    let Some(row) = row else {
        bail!("the dose record is missing, not due, closed, or has a conflicting response")
    };
    let result = json!({
        "dose_id": id,
        "response": row.get::<_, String>(0),
        "responded_at": row.get::<_, DateTime<Utc>>(1),
        "synthetic": subject_id == SUBJECT,
    });
    crate::state_machine::append(
        &tx,
        subject_id,
        &crate::state_machine::StateEvent {
            event_id: format!("medication-response:{id}:{}", response),
            event_type: "medication_response_confirmed".into(),
            event_version: 1,
            actor_id: "service:hsm-medication-response".into(),
            occurred_at: row.get(1),
            source_ref: Some(format!("dose:{id}")),
            correlation_id: None,
            causation_id: None,
            consent_version: None,
            rule_version: None,
            payload: json!({"response": response}),
        },
        None,
    )
    .await?;
    tx.commit().await?;
    Ok(result)
}

pub async fn save_sleep(
    pool: &Pool,
    id: &str,
    start: DateTime<Utc>,
    complete: DateTime<Utc>,
) -> Result<Value> {
    save_sleep_for(pool, SUBJECT, id, start, complete).await
}

pub async fn save_sleep_for(
    pool: &Pool,
    subject_id: &str,
    id: &str,
    start: DateTime<Utc>,
    complete: DateTime<Utc>,
) -> Result<Value> {
    if start >= complete {
        bail!("a completed sleep session requires an end after its start");
    }
    let mut db = pool.get().await?;
    let tx = db.transaction().await?;
    let stored = tx
        .query_opt(
            "INSERT INTO avatar_hsm.sleep_session (id, subject_id, started_at, completed_at)
         VALUES ($1, $2, $3, $4)
         ON CONFLICT (subject_id, id) DO UPDATE SET id = EXCLUDED.id
         WHERE sleep_session.started_at = EXCLUDED.started_at
           AND sleep_session.completed_at = EXCLUDED.completed_at
         RETURNING id",
            &[&id, &subject_id, &start, &complete],
        )
        .await?;
    if stored.is_none() {
        bail!("sleep session ID already has different timestamps");
    }
    crate::state_machine::append(
        &tx,
        subject_id,
        &crate::state_machine::StateEvent {
            event_id: format!("sleep-session:{id}"),
            event_type: "completed_sleep_session_recorded".into(),
            event_version: 1,
            actor_id: "service:hsm-sleep".into(),
            occurred_at: complete,
            source_ref: Some(format!("sleep-session:{id}")),
            correlation_id: None,
            causation_id: None,
            consent_version: None,
            rule_version: None,
            payload: json!({"started_at": start, "completed_at": complete}),
        },
        None,
    )
    .await?;
    tx.commit().await?;
    Ok(json!({ "id": id, "completed_at": complete }))
}

pub async fn sleep_sessions(db: &Client) -> Result<Value> {
    sleep_sessions_for(db, SUBJECT).await
}

pub async fn sleep_sessions_for(db: &Client, subject_id: &str) -> Result<Value> {
    let row = db
        .query_one(
            "SELECT COALESCE(jsonb_agg(to_jsonb(s) ORDER BY completed_at DESC), '[]'::jsonb)
         FROM (SELECT id, started_at, completed_at FROM avatar_hsm.sleep_session
               WHERE subject_id = $1 ORDER BY completed_at DESC, id LIMIT 30) s",
            &[&subject_id],
        )
        .await?;
    Ok(
        json!({ "completed_sleep_sessions": row.get::<_, Value>(0), "synthetic": subject_id == SUBJECT }),
    )
}

#[cfg(test)]
#[path = "store_health_status_tests.rs"]
mod health_status_tests;
