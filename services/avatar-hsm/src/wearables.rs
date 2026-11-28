use anyhow::{ensure, Context, Result};
use chrono::{DateTime, Duration, NaiveDate, Utc};
use deadpool_postgres::Pool;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use crate::store::SUBJECT;

#[cfg(test)]
#[path = "wearables_etl_tests.rs"]
mod etl_tests;

pub const HEALTH_CONNECT: &str = "health_connect";
pub const HEALTHKIT: &str = "healthkit";
pub const SAMSUNG_HEALTH: &str = "samsung_health";
pub(crate) const HIGH_VOLUME_CLOUD_RECORD_LIMIT: usize = 24_000;
const SNAPSHOT_RECORD_LIMIT: usize = 4_000;

#[derive(Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeviceSource {
    provider: Option<String>,
    #[serde(default)]
    pub require_active: bool,
    pub expected_generation: Option<String>,
}

impl DeviceSource {
    pub fn mobile_provider(&self) -> Result<&str> {
        let provider = self.provider.as_deref().unwrap_or(HEALTH_CONNECT);
        ensure!(
            matches!(provider, HEALTH_CONNECT | HEALTHKIT | SAMSUNG_HEALTH),
            "unknown mobile provider"
        );
        Ok(provider)
    }
}
const KINDS: [&str; 5] = ["steps", "heart_rate", "sleep", "workout", "metric"];

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct OuraSleepDeletions {
    pub(crate) observed_at: DateTime<Utc>,
    pub(crate) ids: Vec<String>,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct SamsungSleepDeletion {
    uid: String,
    changed_at: DateTime<Utc>,
    #[serde(default)]
    metrics: Vec<String>,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct SamsungSessionChanges {
    kind: String,
    #[serde(default)]
    deletions: Vec<SamsungSleepDeletion>,
    revision: Option<SamsungSleepDeletion>,
    #[serde(default)]
    revisions: Vec<SamsungRevisionBatchItem>,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct SamsungRevisionBatchItem {
    revision: SamsungSleepDeletion,
    records: Vec<Record>,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Snapshot {
    pub provider: Option<String>,
    #[serde(default)]
    pub historical: bool,
    pub lease: String,
    pub start: DateTime<Utc>,
    pub end: DateTime<Utc>,
    pub grants: Vec<String>,
    pub metric_grants: Option<Vec<String>>,
    pub metric_grants_version: Option<u8>,
    pub samsung_activity: Option<crate::samsung_activity::ActivitySnapshot>,
    pub samsung_routes: Option<crate::samsung_routes::RouteSnapshot>,
    pub health_connect_routes: Option<crate::samsung_routes::RouteSnapshot>,
    pub healthkit_routes: Option<crate::samsung_routes::RouteSnapshot>,
    pub samsung_goals: Option<crate::samsung_goals::GoalSnapshot>,
    #[serde(default)]
    pub deleted_source_ids: Vec<String>,
    #[serde(default)]
    pub(crate) samsung_sleep_deletions: Vec<SamsungSleepDeletion>,
    pub(crate) samsung_sleep_revision: Option<SamsungSleepDeletion>,
    pub(crate) samsung_session_changes: Option<SamsungSessionChanges>,
    #[serde(skip)]
    pub(crate) google_metric_coverage: Vec<String>,
    #[serde(skip)]
    pub(crate) withings_measurement_access: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) oura_sleep_deletions: Option<OuraSleepDeletions>,
    #[serde(skip)]
    pub(crate) coros_fit_sources: crate::wearable_fit::CorosFitSources,
    pub records: Vec<Record>,
}

impl Snapshot {
    /// Carry Google Health's granted metric coverage into the HSM state writer.
    /// HSM uses this to reconcile metrics omitted after consent changes.
    pub fn set_google_metric_coverage(&mut self, coverage: Vec<String>) {
        self.google_metric_coverage = coverage;
    }

    /// Carry the current Withings metrics consent to the HSM snapshot writer.
    /// HSM uses this signal to remove stale measurements after consent is revoked.
    pub fn set_withings_measurement_access(&mut self, granted: bool) {
        self.withings_measurement_access = Some(granted);
    }

    pub fn withings_measurement_access(&self) -> Option<bool> {
        self.withings_measurement_access
    }

    pub fn mobile_provider(&self) -> Result<&'static str> {
        match self.provider.as_deref().unwrap_or(HEALTH_CONNECT) {
            HEALTH_CONNECT => Ok(HEALTH_CONNECT),
            HEALTHKIT => Ok(HEALTHKIT),
            SAMSUNG_HEALTH => Ok(SAMSUNG_HEALTH),
            _ => anyhow::bail!("unknown mobile provider"),
        }
    }
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Record {
    pub(crate) kind: String,
    pub(crate) source: String,
    pub(crate) source_id: String,
    pub(crate) started_at: DateTime<Utc>,
    pub(crate) ended_at: DateTime<Utc>,
    pub(crate) modified_at: DateTime<Utc>,
    pub(crate) payload: Value,
}

fn bounded_text(value: &str) -> bool {
    !value.trim().is_empty() && value.len() <= 256
}

fn healthkit_uuid(value: &str) -> bool {
    value.len() == 36
        && value.bytes().enumerate().all(|(index, byte)| {
            if matches!(index, 8 | 13 | 18 | 23) {
                byte == b'-'
            } else {
                byte.is_ascii_hexdigit()
            }
        })
}

pub(crate) fn validate_snapshot(snapshot: &Snapshot) -> Result<()> {
    validate_snapshot_with_limit(snapshot, SNAPSHOT_RECORD_LIMIT)
}

fn validate_snapshot_with_limit(snapshot: &Snapshot, max_records: usize) -> Result<()> {
    ensure!(
        snapshot.start < snapshot.end && snapshot.end - snapshot.start <= Duration::days(8),
        "snapshot window must be at most eight days"
    );
    ensure!(
        snapshot.end <= Utc::now() + Duration::minutes(5),
        "invalid snapshot end"
    );
    ensure!(
        snapshot.grants.len() <= KINDS.len()
            && snapshot
                .grants
                .iter()
                .all(|kind| KINDS.contains(&kind.as_str())),
        "invalid device grants"
    );
    ensure!(
        snapshot.records.len() <= max_records,
        "snapshot exceeds record limit"
    );
    let mut seen = std::collections::HashSet::new();
    for record in &snapshot.records {
        let metric_spans_window = snapshot.provider.as_deref().unwrap_or(HEALTH_CONNECT)
            == HEALTH_CONNECT
            && record.kind == "metric"
            && matches!(
                (
                    record.payload["metric"].as_str(),
                    snapshot.metric_grants_version
                ),
                (Some("menstruation_period"), Some(4..=12))
                    | (Some("skin_temperature"), Some(9..=12))
            )
            && record.ended_at - record.started_at <= Duration::days(30)
            && record.ended_at <= Utc::now() + Duration::minutes(5);
        ensure!(
            snapshot.grants.contains(&record.kind)
                && bounded_text(&record.source)
                && bounded_text(&record.source_id)
                && record.started_at >= snapshot.start
                && record.started_at < snapshot.end
                && record.ended_at >= record.started_at
                && (record.ended_at <= snapshot.end || metric_spans_window)
                && seen.insert((&record.kind, &record.source, &record.source_id)),
            "invalid or duplicated wearable record"
        );
        ensure!(record.payload.is_object(), "invalid wearable payload");
        match record.kind.as_str() {
            "steps" => ensure!(
                record.started_at < record.ended_at
                    && record.payload["count"]
                        .as_u64()
                        .is_some_and(|count| count <= 1_000_000),
                "invalid step count"
            ),
            "heart_rate" => {
                let samples = record.payload["samples"]
                    .as_array()
                    .context("missing heart rate samples")?;
                ensure!(
                    !samples.is_empty() && samples.len() <= 4_096,
                    "invalid heart rate sample count"
                );
                for sample in samples {
                    let at: DateTime<Utc> = serde_json::from_value(sample["at"].clone())?;
                    ensure!(
                        at >= record.started_at && at <= record.ended_at,
                        "heart rate outside record window"
                    );
                    ensure!(
                        sample["bpm"]
                            .as_f64()
                            .is_some_and(|bpm| (1.0..=350.0).contains(&bpm)),
                        "invalid heart rate"
                    );
                }
            }
            "sleep" => {
                ensure!(
                    record.started_at < record.ended_at,
                    "invalid sleep session interval"
                );
                let stages = record.payload["stages"]
                    .as_array()
                    .context("missing sleep stages")?;
                ensure!(stages.len() <= 500, "too many sleep stages");
                let mut previous_end = record.started_at;
                for stage in stages {
                    let start: DateTime<Utc> = serde_json::from_value(stage["start"].clone())?;
                    let end: DateTime<Utc> = serde_json::from_value(stage["end"].clone())?;
                    ensure!(
                        start >= previous_end
                            && start < end
                            && end <= record.ended_at
                            && stage["stage"].as_i64().is_some(),
                        "invalid sleep stage"
                    );
                    previous_end = end;
                }
            }
            "workout" => ensure!(
                (record.started_at < record.ended_at || record.payload["duration_unknown"] == true)
                    && (record.payload["exercise_type"].as_i64().is_some()
                        || record.payload["exercise_type"]
                            .as_str()
                            .is_some_and(bounded_text)),
                "invalid workout"
            ),
            "metric" => ensure!(
                record.payload["metric"].as_str().is_some_and(bounded_text)
                    && record.payload["provider_data"].is_object(),
                "invalid provider metric"
            ),
            _ => unreachable!("validated kind"),
        }
    }
    Ok(())
}

pub async fn issue_lease(pool: &Pool, provider: &str) -> Result<Value> {
    issue_lease_for(pool, SUBJECT, provider).await
}

pub async fn issue_active_lease(
    pool: &Pool,
    provider: &str,
    expected_generation: Option<i64>,
) -> Result<Value> {
    issue_active_lease_for(pool, SUBJECT, provider, expected_generation).await
}

pub async fn issue_active_lease_for(
    pool: &Pool,
    subject: &str,
    provider: &str,
    expected_generation: Option<i64>,
) -> Result<Value> {
    let db = pool.get().await?;
    let row = db.query_opt(
        "WITH c AS (SELECT gen_random_uuid()::text || gen_random_uuid()::text AS token)
         UPDATE avatar_hsm.device_connection d SET
           lease_hash=sha256(convert_to(c.token,'UTF8')), lease_expires_at=now()+interval '5 minutes'
         FROM c WHERE d.subject_id=$1 AND d.provider=$2 AND d.state='synced'
          AND ($3::bigint IS NULL OR d.generation=$3)
          AND (d.lease_expires_at IS NULL OR d.lease_expires_at<=now()) RETURNING c.token,d.generation::text",
        &[&subject,&provider,&expected_generation],
    ).await?.context("device connection changed, is disconnected, or is syncing")?;
    Ok(
        json!({"lease":row.get::<_,String>(0),"generation":row.get::<_,String>(1),"expires_in_seconds":300}),
    )
}

pub async fn issue_lease_for(pool: &Pool, subject: &str, provider: &str) -> Result<Value> {
    let db = pool.get().await?;
    let row = db.query_one(
        "WITH credentials AS (SELECT gen_random_uuid()::text || gen_random_uuid()::text AS token)
         INSERT INTO avatar_hsm.device_connection AS connection
             (subject_id, provider, lease_hash, lease_expires_at)
         SELECT $1, $2, sha256(convert_to(token, 'UTF8')), now() + interval '5 minutes' FROM credentials
         ON CONFLICT (subject_id, provider) DO UPDATE
           SET lease_hash = excluded.lease_hash, lease_expires_at = excluded.lease_expires_at,
               state = CASE WHEN connection.state = 'revoked' THEN 'pending' ELSE connection.state END
         RETURNING (SELECT token FROM credentials), connection.generation::text",
        &[&subject, &provider],
    ).await?;
    Ok(
        json!({"lease": row.get::<_, String>(0), "generation":row.get::<_,String>(1),"expires_in_seconds": 300}),
    )
}

pub async fn status(pool: &Pool, provider: &str) -> Result<Value> {
    status_for(pool, SUBJECT, provider).await
}

pub async fn issue_job_lease(
    pool: &Pool,
    subject: &str,
    provider: &str,
    job: &str,
) -> Result<Value> {
    let db = pool.get().await?;
    let row = db
        .query_opt(
            "WITH c AS (SELECT gen_random_uuid()::text || gen_random_uuid()::text AS token)
         UPDATE avatar_hsm.device_connection d SET
           lease_hash = sha256(convert_to(c.token, 'UTF8')), lease_expires_at = j.job_until
         FROM c, watch_tower.wearable_sync_schedule j
         WHERE d.subject_id=$1 AND d.provider=$2 AND d.state <> 'revoked'
           AND j.subject_id=d.subject_id AND j.provider=d.provider
           AND j.job_id=$3::text::uuid AND j.job_until > now()
         RETURNING c.token",
            &[&subject, &provider, &job],
        )
        .await?
        .context("connection or job is no longer active")?;
    Ok(json!({"lease":row.get::<_, String>(0)}))
}

pub async fn status_for(pool: &Pool, subject: &str, provider: &str) -> Result<Value> {
    let db = pool.get().await?;
    let row = db
        .query_opt(
            "SELECT state, grants, last_synced_at, generation::text FROM avatar_hsm.device_connection
         WHERE subject_id = $1 AND provider = $2",
            &[&subject, &provider],
        )
        .await?;
    Ok(row.map_or_else(
        || json!({"state": "not_connected", "grants": [], "last_synced_at": null, "provider": provider}),
        |row| {
            json!({
                "provider": provider,
                "state": row.get::<_, String>(0),
                "grants": row.get::<_, Vec<String>>(1),
                "last_synced_at": row.get::<_, Option<DateTime<Utc>>>(2),
                "generation": row.get::<_, String>(3),
            })
        },
    ))
}

pub async fn revoke(pool: &Pool, provider: &str) -> Result<Value> {
    revoke_for(pool, SUBJECT, provider).await
}

pub async fn revoke_for(pool: &Pool, subject: &str, provider: &str) -> Result<Value> {
    let mut db = pool.get().await?;
    let tx = db.transaction().await?;
    let affected_nutrition_days =
        nutrition_record_days(&tx, subject, provider, Utc::now(), Utc::now(), &[], true).await?;
    tx.query_one(
        "SELECT pg_advisory_xact_lock(hashtext($1),hashtext($2))",
        &[&subject, &provider],
    )
    .await?;
    revoke_locked(&tx, subject, provider).await?;
    tx.commit().await?;
    for day in affected_nutrition_days {
        crate::diary::rebuild_nutrition_day(&**db, subject, day).await?;
    }
    Ok(json!({"state": "revoked"}))
}

pub(crate) async fn revoke_locked(
    tx: &tokio_postgres::Transaction<'_>,
    subject: &str,
    provider: &str,
) -> Result<()> {
    if provider == "withings" {
        crate::withings_cleanup::queue(tx, subject).await?;
    }
    tx.execute(
        "UPDATE avatar_hsm.device_connection
         SET state = 'revoked', generation = generation + 1, last_synced_at = NULL,
             grants = '{}', lease_hash = NULL, lease_expires_at = NULL, samsung_activity_granted = false
         WHERE subject_id = $1 AND provider = $2",
        &[&subject, &provider],
    )
    .await?;
    tx.execute(
        "DELETE FROM watch_tower.wearable_sync_schedule WHERE subject_id=$1 AND provider=$2",
        &[&subject, &provider],
    )
    .await?;
    tx.execute(
        "DELETE FROM avatar_hsm.wearable_oauth_connection WHERE subject_id=$1 AND provider=$2",
        &[&subject, &provider],
    )
    .await?;
    tx.execute(
        "DELETE FROM avatar_hsm.wearable_oauth_state WHERE subject_id=$1 AND provider=$2",
        &[&subject, &provider],
    )
    .await?;
    tx.execute(
        "DELETE FROM watch_tower.wearable_fit_job WHERE subject_id=$1 AND provider=$2",
        &[&subject, &provider],
    )
    .await?;
    tx.execute(
        "DELETE FROM avatar_hsm.wearable_record WHERE subject_id = $1 AND provider = $2",
        &[&subject, &provider],
    )
    .await?;
    tx.execute(
        "DELETE FROM avatar_hsm.wearable_daily WHERE subject_id=$1 AND provider=$2",
        &[&subject, &provider],
    )
    .await?;
    if provider == SAMSUNG_HEALTH {
        tx.execute(
            "DELETE FROM avatar_hsm.samsung_session_revision WHERE subject_id=$1",
            &[&subject],
        )
        .await?;
        tx.execute(
            "DELETE FROM avatar_hsm.samsung_goals WHERE subject_id=$1",
            &[&subject],
        )
        .await?;
    }
    Ok(())
}

pub async fn commit(pool: &Pool, provider: &str, snapshot: Snapshot) -> Result<Value> {
    commit_for(pool, SUBJECT, provider, snapshot).await
}

pub async fn commit_for(
    pool: &Pool,
    subject: &str,
    provider: &str,
    snapshot: Snapshot,
) -> Result<Value> {
    let replace = !snapshot.historical;
    store_snapshot(pool, subject, provider, snapshot, replace).await
}

pub async fn store_snapshot(
    pool: &Pool,
    subject: &str,
    provider: &str,
    snapshot: Snapshot,
    replace: bool,
) -> Result<Value> {
    store_snapshot_with_run(pool, subject, provider, snapshot, replace, None).await
}

pub(crate) async fn store_snapshot_with_run(
    pool: &Pool,
    subject: &str,
    provider: &str,
    mut snapshot: Snapshot,
    replace: bool,
    run_id: Option<&str>,
) -> Result<Value> {
    let request_sha256 = snapshot_content_sha256(&snapshot)?;
    // HealthKit hides read-permission changes, so absence in a query is not a deletion.
    let replace = replace && provider != HEALTHKIT;
    let mut batched_revisions = Vec::new();
    if let Some(changes) = snapshot
        .samsung_session_changes
        .as_mut()
        .filter(|changes| !changes.revisions.is_empty())
    {
        ensure!(
            matches!(changes.kind.as_str(), "heart_rate" | "metric")
                && changes.revisions.len() <= 64
                && changes.revision.is_none()
                && changes.deletions.is_empty()
                && snapshot.records.is_empty()
                && snapshot.samsung_routes.is_none(),
            "invalid Samsung change batch"
        );
        let mut seen_uids = std::collections::HashSet::new();
        for item in std::mem::take(&mut changes.revisions) {
            ensure!(
                seen_uids.insert(item.revision.uid.clone()),
                "duplicate Samsung change UID"
            );
            let start = snapshot.records.len();
            snapshot.records.extend(item.records);
            batched_revisions.push((item.revision, start..snapshot.records.len()));
        }
    }
    if matches!(
        provider,
        "google_health" | "polar" | "withings" | "ultrahuman" | "sensorbio"
    ) {
        validate_snapshot_with_limit(&snapshot, HIGH_VOLUME_CLOUD_RECORD_LIMIT)?;
    } else {
        validate_snapshot(&snapshot)?;
    }
    ensure!(
        provider == SAMSUNG_HEALTH
            || (snapshot.samsung_activity.is_none()
                && snapshot.samsung_routes.is_none()
                && snapshot.samsung_goals.is_none()),
        "Samsung device details are only accepted for Samsung Health"
    );
    ensure!(
        provider == HEALTH_CONNECT || snapshot.health_connect_routes.is_none(),
        "Health Connect routes require a Health Connect upload"
    );
    ensure!(
        provider == HEALTHKIT || snapshot.healthkit_routes.is_none(),
        "HealthKit routes require a HealthKit upload"
    );
    let (change_kind, deletions, revision) =
        if let Some(changes) = &snapshot.samsung_session_changes {
            ensure!(
                matches!(
                    changes.kind.as_str(),
                    "sleep" | "workout" | "heart_rate" | "metric"
                ) && snapshot.samsung_sleep_revision.is_none()
                    && snapshot.samsung_sleep_deletions.is_empty()
                    && (changes.revision.is_some()
                        || !changes.deletions.is_empty()
                        || !batched_revisions.is_empty()),
                "invalid Samsung session change type"
            );
            (
                changes.kind.as_str(),
                changes.deletions.as_slice(),
                changes.revision.as_ref(),
            )
        } else {
            (
                "sleep",
                snapshot.samsung_sleep_deletions.as_slice(),
                snapshot.samsung_sleep_revision.as_ref(),
            )
        };
    let valid_change_scope = |change: &SamsungSleepDeletion| {
        if change_kind == "metric" {
            !change.metrics.is_empty()
                && change.metrics.len() <= 32
                && snapshot.metric_grants.as_ref().is_some_and(|grants| {
                    change.metrics.iter().all(|metric| grants.contains(metric))
                })
        } else {
            change.metrics.is_empty()
        }
    };
    let valid_revision = |revision: &SamsungSleepDeletion, records: &[Record]| {
        provider == SAMSUNG_HEALTH
            && snapshot.historical
            && snapshot.grants.iter().any(|kind| kind == change_kind)
            && deletions.is_empty()
            && !records.is_empty()
            && bounded_text(&revision.uid)
            && !revision.uid.chars().any(char::is_control)
            && valid_change_scope(revision)
            && revision.changed_at <= Utc::now() + Duration::minutes(5)
            && records.iter().all(|record| {
                record.kind == change_kind
                    && record.modified_at >= revision.changed_at
                    && match change_kind {
                        "heart_rate" => record.source_id == revision.uid,
                        "metric" => record.payload["metric"].as_str().is_some_and(|metric| {
                            revision.metrics.iter().any(|name| name == metric)
                                && record.source_id == format!("{}:{metric}", revision.uid)
                        }),
                        _ => {
                            record
                                .source_id
                                .strip_prefix(&revision.uid)
                                .and_then(|suffix| suffix.strip_prefix(':'))
                                .filter(|timestamp| timestamp.ends_with('Z'))
                                .and_then(|timestamp| timestamp.parse::<DateTime<Utc>>().ok())
                                == Some(record.started_at)
                        }
                    }
            })
    };
    ensure!(
        revision.is_none_or(|revision| valid_revision(revision, &snapshot.records))
            && batched_revisions.iter().all(|(revision, indexes)| {
                valid_revision(revision, &snapshot.records[indexes.clone()])
            }),
        "invalid Samsung session revision"
    );
    ensure!(
        deletions.is_empty()
            || (provider == SAMSUNG_HEALTH
                && snapshot.historical
                && snapshot.grants.iter().any(|kind| kind == change_kind)
                && snapshot.records.is_empty()
                && deletions
                    .iter()
                    .map(|deletion| deletion.metrics.len().max(1))
                    .sum::<usize>()
                    <= SNAPSHOT_RECORD_LIMIT
                && deletions.iter().all(|deletion| {
                    bounded_text(&deletion.uid)
                        && !deletion.uid.chars().any(char::is_control)
                        && valid_change_scope(deletion)
                        && deletion.changed_at >= snapshot.start
                        && deletion.changed_at <= snapshot.end
                })),
        "invalid Samsung session deletions"
    );
    ensure!(
        snapshot.deleted_source_ids.is_empty()
            || (snapshot.deleted_source_ids.len() + snapshot.records.len()
                <= SNAPSHOT_RECORD_LIMIT
                && snapshot.deleted_source_ids.iter().all(|id| match provider {
                    HEALTHKIT => healthkit_uuid(id),
                    HEALTH_CONNECT => bounded_text(id) && !id.chars().any(char::is_control),
                    _ => false,
                })),
        "invalid wearable deletion identifiers"
    );
    let metric_scope: Option<Vec<&str>> = match (
        provider,
        &snapshot.metric_grants,
        snapshot.metric_grants_version,
    ) {
        (SAMSUNG_HEALTH | HEALTH_CONNECT, None, None) => Some(vec![]),
        (_, None, None) => None,
        (HEALTH_CONNECT, Some(_), version @ (None | Some(2..=12))) => {
            let mut known = vec![
                "weight",
                "body_fat",
                "oxygen_saturation",
                "resting_heart_rate",
                "respiratory_rate",
                "hrv_rmssd",
                "vo2_max",
            ];
            if matches!(version, Some(2..=12)) {
                known.extend([
                    "height",
                    "fat_free_mass",
                    "body_water_mass",
                    "bone_mass",
                    "basal_metabolic_rate",
                    "blood_glucose",
                    "blood_pressure",
                    "body_temperature",
                    "active_energy",
                    "total_energy",
                    "distance",
                    "floors_climbed",
                    "elevation_gained",
                    "water_intake",
                    "nutrition",
                ]);
            }
            if matches!(version, Some(3..=12)) {
                known.extend(["speed", "power", "step_cadence", "cycling_cadence"]);
            }
            if matches!(version, Some(4..=12)) {
                known.extend([
                    "menstruation_period",
                    "menstruation_flow",
                    "intermenstrual_bleeding",
                    "ovulation_test",
                    "cervical_mucus",
                    "basal_body_temperature",
                ]);
            }
            if matches!(version, Some(5..=12)) {
                known.push("wheelchair_pushes");
            }
            if matches!(version, Some(6 | 7 | 10 | 11)) {
                known.push("mindfulness_session");
            }
            if matches!(version, Some(7 | 8 | 11 | 12)) {
                known.push("activity_intensity");
            }
            if matches!(version, Some(9..=12)) {
                known.push("skin_temperature");
            }
            Some(known)
        }
        (SAMSUNG_HEALTH, Some(_), version @ (None | Some(2 | 3))) => {
            let mut known = vec![
                "weight",
                "body_fat",
                "oxygen_saturation",
                "blood_pressure_systolic",
                "blood_pressure_diastolic",
                "blood_pressure_mean",
                "blood_pressure_pulse",
                "blood_glucose",
                "body_temperature",
                "skin_temperature",
                "floors_climbed",
                "energy_score",
            ];
            if version.is_some() {
                known.extend([
                    "height",
                    "basal_metabolic_rate",
                    "body_fat_mass",
                    "fat_free_percentage",
                    "fat_free_mass",
                    "muscle_percentage",
                    "skeletal_muscle_percentage",
                    "skeletal_muscle_mass",
                    "total_body_water",
                    "body_mass_index",
                    "water_intake",
                    "nutrition",
                ]);
            }
            if version == Some(3) {
                known.extend(["irregular_heart_rhythm_notification", "sleep_apnea"]);
            }
            Some(known)
        }
        _ => anyhow::bail!("unsupported wearable metric grants version"),
    };
    if let (Some(metrics), Some(supported)) = (&snapshot.metric_grants, &metric_scope) {
        ensure!(
            metrics.len() <= supported.len()
                && metrics
                    .iter()
                    .all(|name| supported.contains(&name.as_str()))
                && snapshot.records.iter().all(|record| record.kind != "metric"
                    || record.payload["metric"]
                        .as_str()
                        .is_some_and(|name| metrics.iter().any(|metric| metric == name))),
            "invalid wearable metric grants"
        );
    }
    let mut db = pool.get().await?;
    let tx = db.transaction().await?;
    tx.query_one(
        "SELECT pg_advisory_xact_lock(hashtext($1),hashtext($2))",
        &[&subject, &provider],
    )
    .await?;
    if let Some(row) = tx
        .query_opt(
            "SELECT r.request_sha256,r.result,c.state FROM avatar_hsm.snapshot_write_receipt r
             JOIN avatar_hsm.device_connection c USING (subject_id,provider)
             WHERE r.subject_id=$1 AND r.provider=$2
               AND r.lease_hash=sha256(convert_to($3,'UTF8')) FOR UPDATE OF r,c",
            &[&subject, &provider, &snapshot.lease],
        )
        .await?
    {
        ensure!(
            row.get::<_, String>(0) == request_sha256,
            "upload lease was already committed with a different snapshot"
        );
        ensure!(
            row.get::<_, String>(2) != "revoked",
            "wearable provider was revoked after this snapshot was committed"
        );
        let result: Value = row.get(1);
        tx.commit().await?;
        return Ok(result);
    }
    let nutrition_source_ids = snapshot
        .records
        .iter()
        .filter(|record| {
            record.kind == "metric"
                && nutrition_metric(
                    provider,
                    record.payload["metric"].as_str().unwrap_or_default(),
                )
        })
        .map(|record| record.source_id.clone())
        .chain(snapshot.deleted_source_ids.iter().cloned())
        .collect::<Vec<_>>();
    let nutrition_granted = if provider == HEALTHKIT {
        !nutrition_source_ids.is_empty()
    } else {
        snapshot
            .metric_grants
            .as_ref()
            .is_some_and(|metrics| metrics.iter().any(|metric| metric == "nutrition"))
    };
    let mut affected_nutrition_days = nutrition_record_days(
        &tx,
        subject,
        provider,
        snapshot.start,
        snapshot.end,
        &nutrition_source_ids,
        !nutrition_granted,
    )
    .await?;
    let updated = tx.execute(
        "UPDATE avatar_hsm.device_connection
         SET state = 'synced', last_synced_at = now(),
             grants = CASE WHEN $2='healthkit' THEN ARRAY(SELECT DISTINCT unnest(grants || $4::text[]))
                  WHEN $2 IN ('samsung_health','health_connect') AND NOT ('metric'=ANY($4::text[])) AND EXISTS (
                    SELECT 1 FROM avatar_hsm.wearable_record r WHERE r.subject_id=$1 AND r.provider=$2
                      AND r.kind='metric' AND NOT (r.payload->>'metric'=ANY($5::text[]))
                  ) THEN array_append($4::text[], 'metric') ELSE $4 END,
             lease_hash = NULL, lease_expires_at = NULL
         WHERE subject_id = $1 AND provider = $2 AND state <> 'revoked'
           AND lease_hash = sha256(convert_to($3, 'UTF8')) AND lease_expires_at > now()",
        &[&subject, &provider, &snapshot.lease, &snapshot.grants, &metric_scope],
    ).await?;
    ensure!(
        updated == 1,
        "upload lease expired, revoked, or already used"
    );
    if provider == "withings" && snapshot.withings_measurement_access == Some(false) {
        tx.execute(
            "DELETE FROM avatar_hsm.wearable_record
             WHERE subject_id=$1 AND provider='withings' AND kind='metric'
               AND (source_id LIKE 'measurements:%' OR payload->>'metric' IN ('measurements','ecg'))",
            &[&subject],
        ).await?;
    }
    if let (Some(metrics), Some(supported)) = (&snapshot.metric_grants, &metric_scope) {
        tx.execute("DELETE FROM avatar_hsm.wearable_record WHERE subject_id=$1 AND provider=$2 AND kind='metric'
            AND payload->>'metric'=ANY($4::text[]) AND NOT (payload->>'metric'=ANY($3::text[]))",
            &[&subject,&provider,&metrics,&supported]).await?;
    }
    if replace {
        let ids: Value = Value::Array(
            snapshot
                .records
                .iter()
                .map(|r| {
                    json!({
                        "kind":r.kind, "source":r.source, "source_id":r.source_id
                    })
                })
                .collect(),
        );
        if provider == "google_health" {
            // The cloud query has an exclusive upper bound. In particular, sleep
            // is queried by end time and cannot prove absence at that boundary.
            tx.execute(
                "DELETE FROM avatar_hsm.wearable_record AS old
                 WHERE subject_id=$1 AND provider='google_health'
                   AND kind=ANY($2::text[])
                   AND (kind IN ('steps','heart_rate','sleep','workout')
                        OR (kind='metric' AND payload->>'metric'=ANY($6::text[])))
                   AND started_at >= $3 AND ended_at < $4
                   AND NOT EXISTS (
                     SELECT 1 FROM jsonb_to_recordset($5) AS current(kind text, source text, source_id text)
                     WHERE current.kind=old.kind AND current.source=old.source AND current.source_id=old.source_id
                   )",
                &[&subject, &snapshot.grants, &snapshot.start, &snapshot.end, &ids, &snapshot.google_metric_coverage],
            ).await?;
        } else {
            tx.execute(
        "DELETE FROM avatar_hsm.wearable_record AS old
         WHERE subject_id = $1 AND provider = $2
           AND (kind <> 'metric' OR $7::text[] IS NULL OR payload->>'metric'=ANY($7::text[]))
           AND (kind <> ALL($3::text[]) OR
              (kind = ANY($3::text[]) AND started_at >= $4 AND started_at < $5
               AND (ended_at <= $5 OR ($2='health_connect' AND kind='metric'
                    AND payload->>'metric' IN ('menstruation_period','skin_temperature')
                    AND payload->>'metric'=ANY($7::text[])))
               AND NOT ($2='samsung_health' AND old.kind IN ('sleep','workout','heart_rate','metric') AND EXISTS (
                  SELECT 1 FROM avatar_hsm.samsung_session_revision AS revision
                  WHERE revision.subject_id=$1 AND revision.kind=avatar_hsm.samsung_record_scope(old.kind,old.payload)
                    AND NOT revision.deleted AND revision.uid=avatar_hsm.samsung_record_uid(revision.kind,old.source_id)
                    AND old.modified_at>=revision.changed_at
               ))
               AND NOT EXISTS (
                  SELECT 1 FROM jsonb_to_recordset($6) AS current(kind text, source text, source_id text)
                  WHERE current.kind = old.kind AND current.source = old.source AND current.source_id = old.source_id
              )))",
        &[&subject, &provider, &snapshot.grants, &snapshot.start, &snapshot.end, &ids, &metric_scope],
    ).await?;
        }
    }
    if provider == "polar" {
        let days: Vec<_> = snapshot
            .records
            .iter()
            .filter(|record| {
                record.kind == "metric" && record.payload["metric"] == "activity_samples"
            })
            .filter_map(|record| record.payload["provider_data"]["date"].as_str())
            .collect();
        if !days.is_empty() {
            tx.execute(
                "DELETE FROM avatar_hsm.wearable_record
                 WHERE subject_id=$1 AND provider='polar' AND source='Polar'
                   AND source_id LIKE 'activity_steps:%' AND split_part(source_id,':',2)=ANY($2)
                   AND started_at >= $3 AND started_at < $4 AND ended_at <= $4 AND modified_at <= $4",
                &[&subject,&days,&snapshot.start,&snapshot.end],
            ).await?;
        }
    }
    if provider == "coros" {
        // Compare against the previous OAuth summary before writing the new snapshot.
        crate::wearable_fit::enqueue_coros_workouts(
            &tx,
            subject,
            &snapshot.records,
            &snapshot.coros_fit_sources,
        )
        .await?;
    }
    upsert_records(&tx, subject, provider, &snapshot.records).await?;
    if let Some(deletions) = &snapshot.oura_sleep_deletions {
        ensure!(
            provider == "oura"
                && snapshot.grants.iter().any(|kind| kind == "sleep")
                && deletions.ids.len() <= SNAPSHOT_RECORD_LIMIT
                && deletions
                    .ids
                    .iter()
                    .all(|id| crate::wearable_fit::valid_id(id)),
            "invalid Oura sleep deletions"
        );
        tx.execute(
            "WITH eligible AS (
                 SELECT DISTINCT incoming.source_id FROM unnest($2::text[]) AS incoming(source_id)
                 WHERE EXISTS (
                     SELECT 1 FROM avatar_hsm.wearable_oauth_connection c
                     WHERE c.subject_id=$1 AND c.provider='oura'
                 ) AND NOT EXISTS (
                     SELECT 1 FROM avatar_hsm.wearable_record r
                     WHERE r.subject_id=$1 AND r.provider='oura' AND r.kind='sleep'
                       AND r.source='Oura' AND r.source_id=incoming.source_id AND r.modified_at>$3
                 )
             ), deleted_document AS (
                 INSERT INTO avatar_hsm.wearable_deleted_document(subject_id,provider,kind,source_id)
                 SELECT $1,'oura','sleep',source_id FROM eligible ON CONFLICT DO NOTHING
             )
             DELETE FROM avatar_hsm.wearable_record r USING eligible
             WHERE r.subject_id=$1 AND r.provider='oura' AND r.kind='sleep'
               AND r.source='Oura' AND r.source_id=eligible.source_id",
            &[&subject,&deletions.ids,&deletions.observed_at],
        ).await?;
    }
    if provider == SAMSUNG_HEALTH {
        tx.execute(
            "DELETE FROM avatar_hsm.samsung_session_revision
             WHERE subject_id=$1 AND CASE WHEN starts_with(kind,'metric:') THEN
                 substring(kind FROM 8)=ANY($3::text[])
                 AND NOT (substring(kind FROM 8)=ANY($4::text[]))
             ELSE NOT (kind=ANY($2::text[])) END",
            &[
                &subject,
                &snapshot.grants,
                &metric_scope,
                &snapshot.metric_grants,
            ],
        )
        .await?;
    }
    if !batched_revisions.is_empty() {
        let mut keep = vec![false; snapshot.records.len()];
        for (revision, indexes) in &batched_revisions {
            let rows = &snapshot.records[indexes.clone()];
            let applied = apply_samsung_session_changes(
                &tx,
                subject,
                change_kind,
                std::slice::from_ref(revision),
                rows,
            )
            .await?;
            for (record, retained) in rows.iter().zip(&mut keep[indexes.clone()]) {
                *retained = samsung_record_change_applied(record, change_kind, &applied);
            }
        }
        snapshot.records = snapshot
            .records
            .into_iter()
            .zip(keep)
            .filter_map(|(record, retained)| retained.then_some(record))
            .collect();
    } else if let Some(revision) = revision {
        let applied = apply_samsung_session_changes(
            &tx,
            subject,
            change_kind,
            std::slice::from_ref(revision),
            &snapshot.records,
        )
        .await?;
        if applied.is_empty() {
            snapshot.records.clear();
            snapshot.samsung_routes = None;
        } else if change_kind == "metric" {
            snapshot
                .records
                .retain(|record| samsung_record_change_applied(record, change_kind, &applied));
        }
    } else if !deletions.is_empty() {
        apply_samsung_session_changes(&tx, subject, change_kind, deletions, &[]).await?;
    }
    if !snapshot.deleted_source_ids.is_empty() && provider == HEALTHKIT {
        let ids: Vec<_> = snapshot
            .deleted_source_ids
            .iter()
            .map(|id| id.to_ascii_lowercase())
            .collect();
        tx.execute(
            "DELETE FROM avatar_hsm.wearable_record WHERE subject_id=$1 AND provider='healthkit'
             AND lower(source_id)=ANY($2::text[])",
            &[&subject, &ids],
        )
        .await?;
    } else if !snapshot.deleted_source_ids.is_empty() {
        tx.execute(
            "DELETE FROM avatar_hsm.wearable_record WHERE subject_id=$1 AND provider='health_connect'
             AND source_id=ANY($2::text[])",
            &[&subject, &snapshot.deleted_source_ids],
        )
        .await?;
    }
    for record in &snapshot.records {
        let intervals_fit = provider != "intervals_icu"
            || (record.payload["provider_data"]["file_type"]
                .as_str()
                .is_some_and(|format| format.eq_ignore_ascii_case("fit"))
                && !record.payload["provider_data"]["source"]
                    .as_str()
                    .is_some_and(|source| source.eq_ignore_ascii_case("strava")));
        if record.kind == "workout" && provider != "coros" && intervals_fit {
            crate::wearable_fit::enqueue(
                &tx,
                subject,
                provider,
                &record.source_id,
                record.payload["exercise_type"].as_str(),
            )
            .await?;
        }
    }
    if let Some(activity) = &snapshot.samsung_activity {
        crate::samsung_activity::store(&tx, subject, snapshot.start, snapshot.end, activity)
            .await?;
    }
    if let Some(routes) = snapshot
        .samsung_routes
        .as_ref()
        .or(snapshot.health_connect_routes.as_ref())
        .or(snapshot.healthkit_routes.as_ref())
    {
        crate::samsung_routes::store(&tx, subject, provider, &snapshot.records, routes).await?;
    }
    if matches!(provider, SAMSUNG_HEALTH | HEALTH_CONNECT | HEALTHKIT) {
        tx.execute(
            "DELETE FROM avatar_hsm.wearable_workout_route g USING avatar_hsm.wearable_record r
            WHERE g.subject_id=$1 AND r.subject_id=g.subject_id AND r.provider=g.provider
              AND r.kind=g.kind AND r.source=g.source AND r.source_id=g.source_id
              AND g.provider=$2 AND r.modified_at<>g.modified_at",
            &[&subject, &provider],
        )
        .await?;
    }
    if let Some(goals) = &snapshot.samsung_goals {
        crate::samsung_goals::store(&tx, subject, snapshot.end, goals).await?;
    }
    if provider == "whoop" {
        rebuild_whoop_steps(&tx, subject).await?;
    } else if provider == "google_health" {
        crate::google_workout_tcx::enqueue(&tx, subject, snapshot.start, snapshot.end).await?;
        rebuild_google_activity(&tx, subject).await?;
    } else if provider == "sensorbio" {
        rebuild_activity_steps(&tx, subject, provider).await?;
        rebuild_sensorbio_activity(&tx, subject).await?;
    } else if matches!(
        provider,
        "polar" | "suunto" | "withings" | "coros" | "intervals_icu"
    ) {
        if provider == "coros" {
            tx.execute(
                "DELETE FROM avatar_hsm.wearable_record AS measurement
                 USING avatar_hsm.wearable_record AS summary
                 WHERE measurement.subject_id=$1 AND measurement.provider='coros'
                   AND measurement.kind='metric'
                   AND summary.subject_id=measurement.subject_id AND summary.provider=measurement.provider
                   AND summary.source=measurement.source AND summary.started_at=measurement.started_at
                   AND summary.kind='metric' AND summary.payload->>'metric'='daily_summary'
                   AND measurement.modified_at <= summary.modified_at
                   AND measurement.payload->>'metric' IN ('resting_heart_rate','sleep_average_heart_rate')
                   AND COALESCE(summary.payload->'provider_data'->>(CASE
                         WHEN measurement.payload->>'metric'='resting_heart_rate' THEN 'rhr' ELSE 'sleepAvgHr' END), '')
                         !~ '^([1-9][0-9]?|[12][0-9][0-9]|300)$'",
                &[&subject],
            ).await?;
        }
        rebuild_activity_steps(&tx, subject, provider).await?;
    }
    rebuild_daily(&tx, subject, provider).await?;
    affected_nutrition_days.extend(
        nutrition_record_days(
            &tx,
            subject,
            provider,
            snapshot.start,
            snapshot.end,
            &nutrition_source_ids,
            false,
        )
        .await?,
    );
    for day in affected_nutrition_days {
        crate::diary::rebuild_nutrition_day(&*tx, subject, day).await?;
    }
    let result = json!({"accepted": snapshot.records.len(), "state":"synced"});
    tx.execute(
        "INSERT INTO avatar_hsm.snapshot_write_receipt
         (subject_id,provider,lease_hash,request_sha256,result)
         VALUES($1,$2,sha256(convert_to($3,'UTF8')),$4,$5)",
        &[
            &subject,
            &provider,
            &snapshot.lease,
            &request_sha256,
            &result,
        ],
    )
    .await?;
    if !matches!(provider, HEALTH_CONNECT | HEALTHKIT | SAMSUNG_HEALTH) {
        // The owner snapshot transaction commits a payload-free event for the
        // Watch Tower common receipt ledger. Mobile uploads already pass that
        // ledger synchronously through the unified MCP write boundary.
        tx.execute(
            "INSERT INTO avatar_hsm.watch_tower_record_outbox
             (subject_id,provider,request_sha256,observed_at,run_id)
             VALUES($1,$2,$3,$4,$5::text::uuid)
             ON CONFLICT(subject_id,provider,request_sha256) DO UPDATE
             SET run_id=EXCLUDED.run_id,observed_at=EXCLUDED.observed_at,
                 delivered_at=NULL,lease_owner=NULL,lease_until=NULL
             WHERE avatar_hsm.watch_tower_record_outbox.run_id IS DISTINCT FROM EXCLUDED.run_id",
            &[&subject, &provider, &request_sha256, &snapshot.end, &run_id],
        )
        .await?;
    }
    crate::state_machine::append(
        &tx,
        subject,
        &crate::state_machine::StateEvent {
            event_id: format!("wearable-batch:{provider}:{request_sha256}"),
            event_type: "normalized_observation_batch_committed".into(),
            event_version: 1,
            actor_id: "service:watch-tower-etl".into(),
            occurred_at: snapshot.end,
            source_ref: Some(format!("wearable-snapshot:{provider}:{request_sha256}")),
            correlation_id: run_id.map(str::to_owned),
            causation_id: None,
            consent_version: None,
            rule_version: None,
            payload: json!({
                "provider": provider,
                "request_sha256": request_sha256,
                "accepted_record_count": snapshot.records.len(),
                "window_start": snapshot.start,
                "window_end": snapshot.end,
            }),
        },
        None,
    )
    .await?;
    tx.commit().await?;
    Ok(result)
}

fn snapshot_content_sha256(snapshot: &Snapshot) -> Result<String> {
    let mut content = serde_json::to_value(snapshot)?;
    let object = content
        .as_object_mut()
        .context("serialized wearable snapshot must be an object")?;
    // The signed lease is authorization and changes for each upload attempt.
    // Start/end are query-window bounds, not record content. Keep the lease in
    // the lease replay check, but exclude all three from event content identity.
    object.remove("lease");
    object.remove("start");
    object.remove("end");
    let bytes = serde_json::to_vec(&content)?;
    Ok(ring::digest::digest(&ring::digest::SHA256, &bytes)
        .as_ref()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect())
}

async fn nutrition_record_days(
    tx: &tokio_postgres::Transaction<'_>,
    subject: &str,
    provider: &str,
    start: DateTime<Utc>,
    end: DateTime<Utc>,
    source_ids: &[String],
    include_all: bool,
) -> Result<std::collections::BTreeSet<NaiveDate>> {
    if !matches!(provider, HEALTH_CONNECT | SAMSUNG_HEALTH | HEALTHKIT) {
        return Ok(std::collections::BTreeSet::new());
    }
    let rows = tx.query(
        "SELECT DISTINCT (r.started_at AT TIME ZONE s.timezone)::date
         FROM avatar_hsm.wearable_record r JOIN avatar_hsm.subject s ON s.id=r.subject_id
         WHERE r.subject_id=$1 AND r.provider=$2 AND r.kind='metric'
           AND ((r.provider IN ('health_connect','samsung_health') AND r.payload->>'metric'='nutrition')
             OR (r.provider='healthkit' AND r.payload->>'metric'=ANY(ARRAY['dietary_energy','dietary_protein','dietary_carbohydrate','dietary_fat'])))
           AND ($3::boolean OR (r.started_at >= $4 AND r.started_at < $5) OR r.source_id=ANY($6::text[]))",
        &[&subject, &provider, &include_all, &start, &end, &source_ids],
    ).await?;
    Ok(rows.into_iter().map(|row| row.get(0)).collect())
}

fn nutrition_metric(provider: &str, metric: &str) -> bool {
    match provider {
        HEALTH_CONNECT | SAMSUNG_HEALTH => metric == "nutrition",
        HEALTHKIT => matches!(
            metric,
            "dietary_energy" | "dietary_protein" | "dietary_carbohydrate" | "dietary_fat"
        ),
        _ => false,
    }
}

fn samsung_record_change_applied(record: &Record, kind: &str, applied: &[String]) -> bool {
    if kind != "metric" {
        return !applied.is_empty();
    }
    record.payload["metric"].as_str().is_some_and(|metric| {
        applied
            .iter()
            .any(|scope| scope.strip_prefix("metric:") == Some(metric))
    })
}

async fn apply_samsung_session_changes(
    tx: &tokio_postgres::Transaction<'_>,
    subject: &str,
    kind: &str,
    changes: &[SamsungSleepDeletion],
    records: &[Record],
) -> Result<Vec<String>> {
    let mut scoped_changes = Vec::new();
    for change in changes {
        if kind == "metric" {
            scoped_changes.extend(change.metrics.iter().map(|metric| {
                json!({"kind":format!("metric:{metric}"),"uid":change.uid,"changed_at":change.changed_at})
            }));
        } else {
            scoped_changes
                .push(json!({"kind":kind,"uid":change.uid,"changed_at":change.changed_at}));
        }
    }
    let changes = Value::Array(scoped_changes);
    let records_json = serde_json::to_value(records)?;
    let applied = tx.query_one(
        "WITH applied AS (
             INSERT INTO avatar_hsm.samsung_session_revision AS current (subject_id,kind,uid,changed_at,deleted)
             SELECT $1,changes.kind,uid,max(changed_at),NOT EXISTS (
                SELECT 1 FROM jsonb_to_recordset($3) AS incoming(kind text, payload jsonb)
                WHERE avatar_hsm.samsung_record_scope(incoming.kind,incoming.payload)=changes.kind
             )
             FROM jsonb_to_recordset($2) AS changes(kind text,uid text,changed_at timestamptz)
             GROUP BY changes.kind,uid
             ON CONFLICT (subject_id,kind,uid) DO UPDATE SET
                changed_at=excluded.changed_at,deleted=excluded.deleted
             WHERE current.changed_at<excluded.changed_at
                OR (current.changed_at=excluded.changed_at AND current.deleted AND NOT excluded.deleted)
             RETURNING kind,uid,changed_at
         ), removed AS (
             DELETE FROM avatar_hsm.wearable_record AS old USING applied
             WHERE old.subject_id=$1 AND old.provider='samsung_health'
               AND avatar_hsm.samsung_record_scope(old.kind,old.payload)=applied.kind
               AND old.modified_at<=applied.changed_at
               AND avatar_hsm.samsung_record_uid(applied.kind,old.source_id)=applied.uid
               AND NOT EXISTS (
                   SELECT 1 FROM jsonb_to_recordset($3) AS incoming(source text, source_id text)
                   WHERE incoming.source=old.source AND incoming.source_id=old.source_id
               )
         ) SELECT COALESCE(array_agg(DISTINCT kind), '{}'::text[]) FROM applied",
        &[&subject, &changes, &records_json],
    ).await?;
    Ok(applied.get(0))
}

async fn rebuild_google_activity(
    tx: &tokio_postgres::Transaction<'_>,
    subject: &str,
) -> Result<()> {
    tx.execute(
        "DELETE FROM avatar_hsm.wearable_daily
         WHERE subject_id=$1 AND provider='google_health' AND kind='activity' AND calculated_by='provider'",
        &[&subject],
    ).await?;
    tx.execute(
        "INSERT INTO avatar_hsm.wearable_daily
           (subject_id,provider,kind,source,local_day,timezone,calculated_by,stats,modified_at)
         SELECT subject_id,provider,'activity',source,
                to_char(started_at AT TIME ZONE 'UTC','YYYY-MM-DD'),'','provider',
                jsonb_build_object('total_energy_kcal', payload#>'{provider_data,rollup,kcalSum}'),modified_at
         FROM avatar_hsm.wearable_record
         WHERE subject_id=$1 AND provider='google_health' AND kind='metric'
           AND payload->>'metric'='total_calories'
           AND payload->>'time_precision'='provider_calendar_date'
           AND jsonb_typeof(payload#>'{provider_data,rollup,kcalSum}')='number'",
        &[&subject],
    ).await?;
    Ok(())
}

async fn rebuild_sensorbio_activity(
    tx: &tokio_postgres::Transaction<'_>,
    subject: &str,
) -> Result<()> {
    tx.execute(
        "DELETE FROM avatar_hsm.wearable_daily
         WHERE subject_id=$1 AND provider='sensorbio' AND kind='activity' AND calculated_by='provider'",
        &[&subject],
    ).await?;
    tx.execute(
        "INSERT INTO avatar_hsm.wearable_daily
           (subject_id,provider,kind,source,local_day,timezone,calculated_by,stats,modified_at)
         SELECT subject_id,provider,'activity',source,detail->>'date','','provider',
                jsonb_object_agg(stat.key,stat.value),max(modified_at)
         FROM avatar_hsm.wearable_record
         CROSS JOIN LATERAL (SELECT payload->'provider_data' AS detail) d
         CROSS JOIN LATERAL jsonb_each(detail->'stats') stat
         WHERE subject_id=$1 AND provider='sensorbio' AND kind='metric'
           AND payload->>'metric' IN ('daily_activity','daily_calories')
           AND detail->>'date' ~ '^\\d{4}-\\d{2}-\\d{2}$'
         GROUP BY subject_id,provider,source,detail->>'date'",
        &[&subject],
    )
    .await?;
    Ok(())
}

async fn rebuild_activity_steps(
    tx: &tokio_postgres::Transaction<'_>,
    subject: &str,
    provider: &str,
) -> Result<()> {
    tx.execute(
        "DELETE FROM avatar_hsm.wearable_daily
         WHERE subject_id=$1 AND provider=$2 AND kind='steps' AND calculated_by='provider'",
        &[&subject, &provider],
    )
    .await?;
    tx.execute(
        "INSERT INTO avatar_hsm.wearable_daily
           (subject_id,provider,kind,source,local_day,timezone,calculated_by,stats,modified_at)
         SELECT DISTINCT ON (source, local_day)
           subject_id,provider,'steps',source,local_day,COALESCE(detail->>'timezone',''),'provider',
           jsonb_build_object('total',total,'unit','steps'),modified_at
         FROM avatar_hsm.wearable_record
         CROSS JOIN LATERAL (SELECT payload->'provider_data' AS detail) d
         CROSS JOIN LATERAL (SELECT
           CASE WHEN $2='coros' THEN to_char(started_at AT TIME ZONE 'UTC','YYYY-MM-DD')
                ELSE detail->>'date' END AS local_day,
           CASE WHEN $2='coros' THEN detail->'step'
                WHEN payload->>'metric'='activity_samples' THEN detail->'steps'->'total_steps'
                ELSE detail->'steps' END AS total) s
         WHERE subject_id=$1 AND provider=$2 AND kind='metric'
           AND (payload->>'metric'='daily_activity' OR ($2='polar' AND payload->>'metric'='activity_samples')
                  OR ($2='suunto' AND payload->>'metric'='daily_stepcount')
                  OR ($2='coros' AND payload->>'metric'='daily_summary')
                  OR ($2='intervals_icu' AND payload->>'metric'='intervals_wellness'))
           AND local_day ~ '^\\d{4}-\\d{2}-\\d{2}$'
           AND total::text ~ '^[0-9]{1,7}$'
         ORDER BY source,local_day,modified_at DESC,
                  (payload->>'metric'='activity_samples') DESC,source_id DESC",
        &[&subject,&provider],
    ).await?;
    Ok(())
}

async fn rebuild_whoop_steps(tx: &tokio_postgres::Transaction<'_>, subject: &str) -> Result<()> {
    tx.execute(
        "DELETE FROM avatar_hsm.wearable_record p USING avatar_hsm.wearable_record c
         WHERE p.subject_id=$1 AND p.provider='whoop' AND p.kind='metric' AND c.kind='metric'
           AND p.payload->>'metric' IN ('cycle_steps','total_energy')
           AND c.subject_id=p.subject_id AND c.provider=p.provider AND c.source=p.source
           AND c.source_id='cycle:' || (p.payload->'provider_data'->>'cycle_id')
           AND c.modified_at > p.modified_at",
        &[&subject],
    )
    .await?;
    tx.execute(
        "DELETE FROM avatar_hsm.wearable_daily
         WHERE subject_id=$1 AND provider='whoop' AND kind='steps' AND calculated_by='provider'",
        &[&subject],
    )
    .await?;
    tx.execute(
        "INSERT INTO avatar_hsm.wearable_daily
           (subject_id,provider,kind,source,local_day,timezone,calculated_by,stats,modified_at)
         SELECT DISTINCT ON (source, detail->>'local_day')
           subject_id,provider,'steps',source,detail->>'local_day',detail->>'timezone','provider',
           jsonb_build_object('total',detail->'value','unit','steps'),modified_at
         FROM avatar_hsm.wearable_record
         CROSS JOIN LATERAL (SELECT payload->'provider_data' AS detail) d
         WHERE subject_id=$1 AND provider='whoop' AND kind='metric' AND payload->>'metric'='cycle_steps'
         ORDER BY source,detail->>'local_day',modified_at DESC,ended_at DESC,source_id DESC",
        &[&subject],
    ).await?;
    Ok(())
}

pub(crate) async fn rebuild_daily(
    tx: &tokio_postgres::Transaction<'_>,
    subject: &str,
    provider: &str,
) -> Result<()> {
    tx.execute(
        "DELETE FROM avatar_hsm.wearable_daily WHERE subject_id = $1 AND provider = $2 AND calculated_by='hsm'",
        &[&subject, &provider],
    )
    .await?;
    tx.execute(
        "INSERT INTO avatar_hsm.wearable_daily
          (subject_id, provider, kind, source, local_day, timezone, calculated_by, stats)
         SELECT r.subject_id, r.provider, r.kind, r.source,
                (r.started_at AT TIME ZONE s.timezone)::date::text, s.timezone, 'hsm',
                jsonb_build_object('total', sum((r.payload->>'count')::bigint), 'unit', 'steps', 'sample_count', count(*))
         FROM avatar_hsm.wearable_record r JOIN avatar_hsm.subject s ON s.id = r.subject_id
         WHERE r.subject_id = $1 AND r.provider = $2 AND r.kind = 'steps'
           AND (r.started_at AT TIME ZONE s.timezone)::date = ((r.ended_at - interval '1 microsecond') AT TIME ZONE s.timezone)::date
         GROUP BY r.subject_id, r.provider, r.kind, r.source, (r.started_at AT TIME ZONE s.timezone)::date, s.timezone",
        &[&subject, &provider],
    ).await?;
    tx.execute(
        "INSERT INTO avatar_hsm.wearable_daily
           (subject_id, provider, kind, source, local_day, timezone, calculated_by, stats)
         SELECT r.subject_id, r.provider, r.kind, r.source,
                (sample.at AT TIME ZONE s.timezone)::date::text, s.timezone, 'hsm',
                jsonb_build_object('average', avg(sample.bpm), 'min', min(sample.bpm), 'max', max(sample.bpm),
                                   'unit', 'bpm', 'sample_count', count(*))
         FROM avatar_hsm.wearable_record r JOIN avatar_hsm.subject s ON s.id = r.subject_id
         CROSS JOIN LATERAL jsonb_to_recordset(r.payload->'samples') AS sample(at timestamptz, bpm numeric)
         WHERE r.subject_id = $1 AND r.provider = $2 AND r.kind = 'heart_rate'
         GROUP BY r.subject_id, r.provider, r.kind, r.source, (sample.at AT TIME ZONE s.timezone)::date, s.timezone",
        &[&subject, &provider],
    ).await?;
    Ok(())
}

pub(crate) async fn upsert_records(
    tx: &tokio_postgres::Transaction<'_>,
    subject: &str,
    provider: &str,
    records: &[Record],
) -> Result<()> {
    if records.is_empty() {
        return Ok(());
    }
    let records = serde_json::to_value(records)?;
    // A bridge without exercise details can replay the current Android revision.
    // Prefer the detailed copy for that revision, and accept real later edits.
    let exercise_details = r#"$ ? (
        @.rate_of_perceived_exertion.type() == "number" ||
        exists(@.segments[*] ? (
            @.weight_kg.type() == "number" || @.set_index.type() == "number" ||
            @.rate_of_perceived_exertion.type() == "number"
        ))
    )"#;
    tx.execute(
        "INSERT INTO avatar_hsm.wearable_record AS existing
             (subject_id,provider,kind,source,source_id,started_at,ended_at,modified_at,payload)
         SELECT $1,$2,r.kind,r.source,r.source_id,r.started_at,r.ended_at,r.modified_at,r.payload
         FROM jsonb_to_recordset($3) AS r(kind text,source text,source_id text,
               started_at timestamptz,ended_at timestamptz,modified_at timestamptz,payload jsonb)
         WHERE (r.source NOT IN ('Oura','WHOOP','Strava') OR NOT EXISTS (
            SELECT 1 FROM avatar_hsm.wearable_deleted_document d
            WHERE d.subject_id=$1 AND d.provider=$2
              AND ((d.kind=r.kind AND d.source_id=r.source_id) OR
                   ($2='whoop' AND r.kind='metric' AND r.payload->>'metric'='recovery'
                     AND d.kind='recovery_sleep' AND d.source_id=r.payload#>>'{provider_data,sleep_id}'))
         ))
           AND NOT ($2='samsung_health' AND r.kind IN ('sleep','workout','heart_rate','metric') AND EXISTS (
             SELECT 1 FROM avatar_hsm.samsung_session_revision AS revision
             WHERE revision.subject_id=$1 AND revision.kind=avatar_hsm.samsung_record_scope(r.kind,r.payload)
               AND revision.uid=avatar_hsm.samsung_record_uid(revision.kind,r.source_id)
               AND (r.modified_at<revision.changed_at OR (r.modified_at=revision.changed_at AND revision.deleted))
           ))
         ON CONFLICT (subject_id,provider,kind,source,source_id) DO UPDATE
         SET started_at=excluded.started_at,ended_at=excluded.ended_at,
             modified_at=excluded.modified_at,payload=excluded.payload
         WHERE excluded.modified_at >= existing.modified_at
           AND NOT (
             excluded.provider = 'health_connect' AND excluded.kind = 'workout'
             AND excluded.modified_at = existing.modified_at
             AND jsonb_path_exists(existing.payload, $4::text::jsonpath)
             AND NOT jsonb_path_exists(excluded.payload, $4::text::jsonpath)
           )
           AND NOT (
             excluded.provider = 'health_connect' AND excluded.kind = 'metric'
             AND excluded.modified_at = existing.modified_at
             AND existing.payload->>'metric' = 'nutrition'
             AND existing.payload#>'{provider_data,missing_values_preserved}' IS NOT DISTINCT FROM 'true'::jsonb
             AND excluded.payload#>'{provider_data,missing_values_preserved}' IS DISTINCT FROM 'true'::jsonb
           )",
        &[&subject, &provider, &records, &exercise_details],
    )
    .await?;
    Ok(())
}

pub(crate) async fn upsert_record(
    tx: &tokio_postgres::Transaction<'_>,
    subject: &str,
    provider: &str,
    record: &Record,
) -> Result<()> {
    tx.execute(
        "INSERT INTO avatar_hsm.wearable_record AS existing
            (subject_id, provider, kind, source, source_id, started_at, ended_at, modified_at, payload)
         VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
         ON CONFLICT (subject_id,provider,kind,source,source_id) DO UPDATE
         SET started_at=excluded.started_at, ended_at=excluded.ended_at,
             modified_at=excluded.modified_at, payload=excluded.payload
         WHERE excluded.modified_at >= existing.modified_at",
        &[&subject,&provider,&record.kind,&record.source,&record.source_id,&record.started_at,&record.ended_at,&record.modified_at,&record.payload],
    ).await?;
    Ok(())
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RecordQuery {
    pub kind: String,
    pub start: DateTime<Utc>,
    pub end: DateTime<Utc>,
    #[serde(default)]
    pub offset: i64,
    pub source: Option<String>,
    pub provider: Option<String>,
    pub metric: Option<String>,
}

pub async fn records(pool: &Pool, query: RecordQuery, sessions: bool) -> Result<Value> {
    records_for(pool, SUBJECT, query, sessions).await
}

pub async fn records_for(
    pool: &Pool,
    subject: &str,
    query: RecordQuery,
    sessions: bool,
) -> Result<Value> {
    let permitted = if sessions {
        &["sleep", "workout"][..]
    } else {
        &["steps", "heart_rate", "metric"][..]
    };
    ensure!(
        permitted.contains(&query.kind.as_str()),
        "invalid record kind"
    );
    ensure!(
        query.start < query.end
            && query.end - query.start <= Duration::days(31)
            && (0..=10_000).contains(&query.offset)
            && query.source.as_deref().is_none_or(bounded_text)
            && query
                .metric
                .as_deref()
                .is_none_or(|metric| query.kind == "metric" && bounded_text(metric)),
        "invalid query window or offset"
    );
    let db = pool.get().await?;
    let rows = db.query(
        "SELECT jsonb_build_object('kind', r.kind, 'provider', r.provider, 'source', r.source,
                 'source_id', r.source_id, 'started_at', r.started_at, 'ended_at', r.ended_at,
                 'modified_at', r.modified_at, 'payload', r.payload,
                 'route_point_count', CASE WHEN r.provider IN ('samsung_health','health_connect','healthkit') AND r.kind='workout' THEN (
                    SELECT jsonb_array_length(g.points) FROM avatar_hsm.wearable_workout_route g
                    WHERE g.subject_id=r.subject_id AND g.provider=r.provider AND g.kind=r.kind
                      AND g.source=r.source AND g.source_id=r.source_id
                      AND g.modified_at=r.modified_at)
                   WHEN r.provider='apple_health_export' AND r.kind='workout' THEN (
                    SELECT jsonb_array_length(g.points) FROM avatar_hsm.apple_export_workout_route g
                      WHERE g.subject_id=r.subject_id AND g.source=r.source AND g.source_id=r.source_id)
                   WHEN r.provider='google_health' AND r.kind='workout' THEN (
                      SELECT jsonb_array_length(g.route_points)
                      FROM avatar_hsm.google_workout_tcx g
                      JOIN avatar_hsm.wearable_oauth_connection o USING(subject_id,provider)
                      WHERE g.subject_id=r.subject_id AND g.source=r.source AND g.source_id=r.source_id
                        AND o.google_location_granted)
                 END,
                 'fit_state', CASE WHEN r.kind='workout' THEN (
                    SELECT f.status FROM watch_tower.wearable_fit_job f WHERE f.subject_id=r.subject_id
                      AND f.provider=r.provider AND f.activity_id=r.source_id) END,
                 'fit_files', CASE WHEN r.kind='workout' THEN COALESCE((
                    SELECT jsonb_agg(jsonb_build_object('part',f.part,'bytes',octet_length(f.data),
                         'sha256',encode(f.sha256,'hex'),'detail_state',f.detail_state) ORDER BY f.part)
                    FROM avatar_hsm.wearable_fit f WHERE f.subject_id=r.subject_id
                      AND f.provider=r.provider AND f.activity_id=r.source_id),'[]'::jsonb) END,
                 'tcx', CASE WHEN r.provider='google_health' AND r.kind='workout' THEN (
                    SELECT jsonb_build_object('state',CASE WHEN t.byte_count IS NOT NULL THEN 'stored'
                       WHEN q.attempts>=6 THEN 'unavailable' ELSE 'pending' END,
                       'bytes',t.byte_count,'sha256',encode(t.sha256,'hex'),
                       'telemetry_point_count',jsonb_array_length(t.telemetry_points),
                       'download_path',CASE WHEN t.byte_count IS NOT NULL THEN '/wearables/tcx/google_health/'||t.source_id END)
                    FROM avatar_hsm.google_workout_tcx t
                    JOIN watch_tower.google_workout_tcx_queue q USING(subject_id,source,source_id)
                    JOIN avatar_hsm.wearable_oauth_connection o USING(subject_id,provider)
                    WHERE t.subject_id=r.subject_id AND t.source=r.source AND t.source_id=r.source_id
                      AND o.google_location_granted) END,
                 'related_heart_rate', CASE WHEN r.kind = 'workout' AND 'heart_rate' = ANY(c.grants)
                   THEN (
                       SELECT jsonb_build_object('sample_count', count(*), 'min_bpm', min(sample.bpm),
                                                 'max_bpm', max(sample.bpm), 'unit', 'bpm')
                       FROM avatar_hsm.wearable_record hr
                       CROSS JOIN LATERAL jsonb_to_recordset(hr.payload->'samples') AS sample(at timestamptz, bpm numeric)
                       WHERE hr.subject_id = r.subject_id AND hr.provider = r.provider AND hr.source = r.source
                         AND hr.kind = 'heart_rate' AND hr.started_at <= r.ended_at AND hr.ended_at >= r.started_at
                         AND sample.at >= r.started_at AND sample.at < r.ended_at
                         AND NOT EXISTS (
                            SELECT 1 FROM avatar_hsm.wearable_record w
                            WHERE w.subject_id = r.subject_id AND w.provider = r.provider AND w.source = r.source
                              AND w.kind = 'workout' AND w.source_id <> r.source_id
                              AND sample.at >= w.started_at AND sample.at < w.ended_at
                         )
                   ) END)
         FROM (
             SELECT base.subject_id,base.provider,base.kind,base.source,base.source_id,
                 base.started_at,base.ended_at,base.modified_at,base.payload,
                 CASE WHEN base.provider='withings' AND base.kind='metric'
                        AND base.payload->>'metric' IN ('intraday_activity','daily_activity')
                      THEN 'steps' ELSE base.kind END AS grant_kind
             FROM avatar_hsm.wearable_record base
             UNION ALL
             SELECT derived.*, 'sleep' AS grant_kind FROM avatar_hsm.wearable_suunto_sleep_metrics derived
             WHERE $2='metric' AND ($7::text IS NULL OR $7='suunto')
             UNION ALL
             SELECT derived.*, 'metric' AS grant_kind FROM avatar_hsm.wearable_polar_activity_zones derived
             WHERE $2='metric' AND ($7::text IS NULL OR $7='polar')
               AND ($8::text IS NULL OR $8='polar_activity_zone')
             UNION ALL
             SELECT derived.*, 'metric' AS grant_kind FROM avatar_hsm.wearable_coros_daily_metrics derived
             WHERE $2='metric' AND ($7::text IS NULL OR $7='coros')
               AND ($8::text IS NULL OR $8 IN ('coros_daily_sleep_duration','coros_overnight_hrv'))
             UNION ALL
             SELECT derived.*, 'metric' AS grant_kind FROM avatar_hsm.wearable_oura_blood_oxygen derived
             WHERE $2='metric' AND ($7::text IS NULL OR $7='oura')
               AND ($8::text IS NULL OR $8 IN ('oxygen_saturation','breathing_disturbance_index'))
             UNION ALL
             SELECT subject_id,provider,kind,source,source_id,started_at,ended_at,modified_at,payload,
                    'metric' AS grant_kind
             FROM avatar_hsm.wearable_oura_daily_indicators
             WHERE $2='metric' AND ($7::text IS NULL OR $7='oura')
               AND ($8::text IS NULL OR $8=metric_name)
             UNION ALL
             SELECT derived.*, 'metric' AS grant_kind FROM avatar_hsm.wearable_oura_activity_met derived
             WHERE $2='metric' AND ($7::text IS NULL OR $7='oura')
               AND ($8::text IS NULL OR $8='activity_met')
             UNION ALL
             SELECT subject_id,provider,kind,source,source_id,started_at,ended_at,modified_at,payload,
                    'workout' AS grant_kind
             FROM avatar_hsm.wearable_apple_workout_weather
             WHERE $2='metric' AND ($7::text IS NULL OR $7 IN ('healthkit','apple_health_export'))
               AND ($8::text IS NULL OR $8=metric_name)
             UNION ALL
             SELECT subject_id,provider,kind,source,source_id,started_at,ended_at,modified_at,payload,
                    'sleep' AS grant_kind
             FROM avatar_hsm.wearable_oura_sleep_samples
             WHERE $2='metric' AND ($7::text IS NULL OR $7='oura')
               AND ($8::text IS NULL OR $8 IN ('sleep_heart_rate','hrv_rmssd'))
               AND sleep_started_at<$4 AND sleep_ended_at>$3
             UNION ALL
             SELECT subject_id,provider,kind,source,source_id,started_at,ended_at,modified_at,payload,
                    'sleep' AS grant_kind
             FROM avatar_hsm.wearable_withings_sleep_samples
             WHERE $2='metric' AND ($7::text IS NULL OR $7='withings')
               AND ($8::text IS NULL OR $8 IN ('sleep_heart_rate','respiratory_rate','hrv_rmssd','hrv_sdnn','snoring_duration',
                              'hrv_quality','bed_movement_score','chest_movement_rate',
                              'withings_breathing_index','breathing_sound_duration'))
               AND sleep_started_at<$4 AND sleep_ended_at>$3
             UNION ALL
             SELECT subject_id,provider,kind,source,source_id,started_at,ended_at,modified_at,payload,
                    'steps' AS grant_kind
             FROM avatar_hsm.wearable_withings_intraday_samples
             WHERE $2='metric' AND ($7::text IS NULL OR $7='withings')
               AND ($8::text IS NULL OR $8=metric_name)
             UNION ALL
             SELECT subject_id,provider,kind,source,source_id,started_at,ended_at,modified_at,payload,
                    'metric' AS grant_kind
             FROM avatar_hsm.wearable_withings_urine_biomarkers
             WHERE $2='metric' AND ($7::text IS NULL OR $7='withings')
               AND ($8::text IS NULL OR $8=metric_name)
         ) r
         JOIN avatar_hsm.device_connection c USING (subject_id,provider)
         WHERE r.subject_id = $1 AND c.state = 'synced' AND r.grant_kind = ANY(c.grants)
           AND r.kind = $2 AND r.started_at < $4
           AND (r.ended_at > $3 OR (r.ended_at = r.started_at AND r.started_at = $3))
           AND ($5::text IS NULL OR r.source = $5)
           AND ($7::text IS NULL OR r.provider = $7)
           AND ($8::text IS NULL OR r.payload->>'metric' = $8)
         ORDER BY r.started_at, r.provider, r.source, r.source_id LIMIT 51 OFFSET $6",
        &[&subject, &query.kind, &query.start, &query.end, &query.source, &query.offset, &query.provider, &query.metric],
    ).await?;
    let state = read_state(
        &db,
        subject,
        &query.kind,
        query.provider.as_deref(),
        query.metric.as_deref(),
        !rows.is_empty(),
    )
    .await?;
    let rows = if state == "ready" { rows } else { Vec::new() };
    let records: Vec<Value> = rows
        .iter()
        .take(50)
        .map(|row| {
            let mut record: Value = row.get(0);
            if !sessions && record["provider"] == "oura" {
                crate::cloud_wearables::manufacturers::oura_activity_classes(
                    &mut record["payload"],
                );
            }
            if !sessions && record["provider"] == "sensorbio" {
                crate::cloud_wearables::sensorbio_daily_hourly_details(&mut record["payload"]);
            }
            if sessions && query.kind == "sleep" && record["provider"] == "suunto" {
                crate::cloud_wearables::suunto_sleep_details(&mut record["payload"]);
            }
            if sessions && query.kind == "sleep" && record["provider"] == "withings" {
                crate::cloud_wearables::withings_sleep_summary(&mut record["payload"]);
            }
            if sessions && query.kind == "sleep" && record["provider"] == "polar" {
                crate::cloud_wearables::polar_sleep_details(&mut record["payload"]);
            }
            if sessions && query.kind == "workout" && record["provider"] == "suunto" {
                crate::cloud_wearables::manufacturers::suunto_workout_details(
                    &mut record["payload"],
                );
            }
            if sessions && query.kind == "workout" && record["provider"] == "withings" {
                crate::cloud_wearables::withings_workout_details(&mut record["payload"]);
            }
            if sessions && query.kind == "workout" && record["provider"] == "polar" {
                crate::cloud_wearables::manufacturers::polar_exercise_heart_rate(
                    &mut record["payload"],
                );
            }
            record
        })
        .collect();
    Ok(json!({
        "state": state, "records": records,
        "next_offset": (rows.len() > 50).then_some(query.offset + 50),
    }))
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DailyQuery {
    pub start_day: NaiveDate,
    pub end_day: NaiveDate,
    pub source: Option<String>,
    pub provider: Option<String>,
    pub kind: Option<String>,
}

pub async fn daily(pool: &Pool, query: DailyQuery) -> Result<Value> {
    daily_for(pool, SUBJECT, query).await
}

pub async fn daily_for(pool: &Pool, subject: &str, query: DailyQuery) -> Result<Value> {
    let kind = query.kind.as_deref().unwrap_or("steps");
    ensure!(
        matches!(kind, "steps" | "heart_rate" | "activity"),
        "invalid daily metric"
    );
    ensure!(
        query.start_day <= query.end_day
            && query.end_day - query.start_day <= Duration::days(30)
            && query.source.as_deref().is_none_or(bounded_text),
        "invalid day range"
    );
    let db = pool.get().await?;
    let rows = db.query(
        "SELECT jsonb_build_object('provider', d.provider, 'source', d.source, 'day', d.local_day, 'kind', d.kind,
                 'timezone', NULLIF(d.timezone,''), 'calculated_by', d.calculated_by, 'stats', d.stats)
         FROM (
            SELECT subject_id,provider,kind,source,local_day,timezone,calculated_by,stats,modified_at
            FROM avatar_hsm.wearable_daily
            UNION ALL
            SELECT subject_id,provider,kind,source,local_day,timezone,calculated_by,stats,modified_at
            FROM avatar_hsm.wearable_daily_activity_projection
            WHERE $5='activity' AND subject_id=$1 AND local_day BETWEEN $2 AND $3
              AND ($6::text IS NULL OR provider=$6)
            UNION ALL
            SELECT subject_id,provider,kind,source,local_day,timezone,calculated_by,stats,modified_at
            FROM avatar_hsm.wearable_ultrahuman_daily
            WHERE kind=$5 AND subject_id=$1 AND local_day BETWEEN $2 AND $3
              AND ($6::text IS NULL OR provider=$6)
            UNION ALL
            SELECT subject_id,provider,kind,source,local_day,timezone,calculated_by,stats,modified_at
            FROM avatar_hsm.wearable_coros_daily_energy
            WHERE $5='activity' AND subject_id=$1 AND local_day BETWEEN $2 AND $3
              AND ($6::text IS NULL OR $6='coros')
            UNION ALL
            SELECT subject_id,provider,kind,source,local_day,timezone,calculated_by,stats,modified_at
            FROM avatar_hsm.health_connect_daily_intensity($1,$2::text::date,$3::text::date,$4)
            WHERE $5='activity' AND ($6::text IS NULL OR $6='health_connect')
         ) d
         JOIN avatar_hsm.device_connection c USING (subject_id,provider)
         WHERE d.subject_id = $1 AND c.state = 'synced' AND d.kind = $5
           AND (d.kind = ANY(c.grants)
                OR (d.provider='ultrahuman' AND d.kind IN ('steps','activity') AND 'metric'=ANY(c.grants))
                OR (d.kind='activity' AND
                (c.samsung_activity_granted OR (d.provider='withings' AND 'steps'=ANY(c.grants))
                 OR (d.provider IN ('sensorbio','google_health','oura','polar','whoop','suunto','coros','health_connect') AND 'metric'=ANY(c.grants)))))
           AND d.local_day BETWEEN $2 AND $3 AND ($4::text IS NULL OR d.source = $4)
           AND ($6::text IS NULL OR d.provider = $6)
           AND (d.provider NOT IN ('garmin','polar','withings') OR NOT EXISTS (
                SELECT 1 FROM avatar_hsm.wearable_daily p WHERE p.subject_id=d.subject_id AND p.provider=d.provider
                AND p.kind=d.kind AND p.source=d.source AND p.local_day=d.local_day AND p.calculated_by='provider'
                AND (d.calculated_by='hsm' OR (p.modified_at,p.timezone)>(d.modified_at,d.timezone))))
         ORDER BY d.local_day, d.source, d.provider, d.timezone LIMIT 301",
        &[&subject, &query.start_day.to_string(), &query.end_day.to_string(), &query.source, &kind, &query.provider],
    ).await?;
    let state = read_state(
        &db,
        subject,
        kind,
        query.provider.as_deref(),
        None,
        !rows.is_empty(),
    )
    .await?;
    let rows = if state == "ready" { rows } else { Vec::new() };
    Ok(json!({
        "state": state,
        "records": rows.iter().take(300).map(|row| row.get::<_, Value>(0)).collect::<Vec<_>>(),
        "truncated": rows.len() > 300,
        "aggregation": if kind == "steps" { "totals are per source; provider-reported days and interval-derived days are available" }
            else if kind == "activity" { "activity totals use the provider's date basis; WHOOP total energy groups completed cycles by local midpoint rather than calendar-day measurement boundaries" }
            else { "heart-rate averages and ranges are per local day and source" },
    }))
}

async fn read_state(
    db: &tokio_postgres::Client,
    subject: &str,
    kind: &str,
    provider: Option<&str>,
    metric: Option<&str>,
    has_records: bool,
) -> Result<&'static str> {
    let row = db.query_one(
        "SELECT coalesce(bool_or(state='synced' AND ($2=ANY(grants)
                    OR (provider='ultrahuman' AND $2 IN ('steps','activity') AND 'metric'=ANY(grants))
                    OR (provider='suunto' AND $2='metric' AND 'sleep'=ANY(grants))
                    OR (provider='oura' AND $2='metric' AND 'sleep'=ANY(grants)
                        AND ($4::text IS NULL OR $4 IN ('sleep_heart_rate','hrv_rmssd')))
                    OR (provider='withings' AND $2='metric' AND 'sleep'=ANY(grants)
                        AND ($4::text IS NULL OR $4 IN ('sleep_heart_rate','respiratory_rate','hrv_rmssd','hrv_sdnn','snoring_duration',
                              'hrv_quality','bed_movement_score','chest_movement_rate',
                              'withings_breathing_index','breathing_sound_duration')))
                    OR ($2='activity' AND (samsung_activity_granted
                        OR (provider='withings' AND 'steps'=ANY(grants))
                        OR (provider IN ('sensorbio','google_health','oura','polar','whoop','suunto','coros','health_connect') AND 'metric'=ANY(grants)))))), false),
                coalesce(bool_or(state='synced'), false),
                coalesce(bool_or(state='pending'), false),
                coalesce(bool_or(state='revoked'), false),
                coalesce(bool_or(provider='healthkit' AND state='synced'), false)
         FROM avatar_hsm.device_connection WHERE subject_id=$1 AND ($3::text IS NULL OR provider=$3)",
        &[&subject,&kind,&provider,&metric],
    ).await?;
    Ok(if row.get(0) {
        if has_records {
            "ready"
        } else {
            "no_records"
        }
    } else if row.get(4) {
        "permission_unknown"
    } else if row.get(1) {
        "permission_missing"
    } else if row.get(2) {
        "pending"
    } else if row.get(3) {
        "revoked"
    } else {
        "not_connected"
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    #[ignore = "requires AVATAR_HSM_TEST_DATABASE_URL"]
    async fn cloud_daily_energy_preserves_provider_days_and_corrections() -> Result<()> {
        let pool = crate::store::connect(&std::env::var("AVATAR_HSM_TEST_DATABASE_URL")?).await?;
        let subject = "cloud-daily-energy-owner";
        let other = "cloud-daily-energy-other";
        let owner_token = "cloud-daily-energy-owner-012345678901234567";
        let other_token = "cloud-daily-energy-other-012345678901234567";
        let db = pool.get().await?;
        let start = "2026-04-23T00:00:00Z";
        let end = "2026-04-26T00:00:00Z";
        for (id, token) in [(subject, owner_token), (other, other_token)] {
            db.execute(
                "INSERT INTO avatar_hsm.subject VALUES ($1,'Asia/Seoul')",
                &[&id],
            )
            .await?;
            db.execute(
                "INSERT INTO avatar_hsm.wearable_profile VALUES
                 ($1,sha256(convert_to($2,'UTF8')),sha256(convert_to($2||'c','UTF8')))",
                &[&id, &token],
            )
            .await?;
        }
        let activity = |source, id, at, detail| {
            json!({
                "kind":"metric","source":source,"source_id":id,"started_at":"2026-04-23T19:00:00Z",
                "ended_at":"2026-04-23T19:00:00Z","modified_at":at,
                "payload":{"metric":"daily_activity","provider_data":detail}
            })
        };
        for (provider, rows) in [
            (
                "oura",
                vec![
                    activity(
                        "Oura",
                        "original",
                        "2026-04-24T01:00:00Z",
                        json!({"day":"2026-04-24","total_calories":2125,"active_calories":625}),
                    ),
                    activity(
                        "Oura",
                        "corrected",
                        "2026-04-24T02:00:00Z",
                        json!({"day":"2026-04-24","total_calories":2100,"active_calories":525}),
                    ),
                ],
            ),
            (
                "polar",
                vec![
                    activity(
                        "Polar",
                        "daily_activity:complete",
                        end,
                        json!({"end_time":"2026-04-24T23:59:59","calories":2500,"active_calories":1500,
                            "active_duration":"PT3H11M","inactive_duration":"PT18H23M30.5S",
                            "distance_from_steps":4590.53,"daily_activity":109.1,"inactivity_alert_count":0}),
                    ),
                    activity(
                        "Polar",
                        "daily_activity:total_only",
                        end,
                        json!({"end_time":"2026-04-25T23:59:59","calories":1800,"active_calories":"unknown"}),
                    ),
                    activity(
                        "Polar",
                        "daily_activity:invalid_split",
                        end,
                        json!({"end_time":"2026-04-23T23:59:59","calories":100,"active_calories":150}),
                    ),
                ],
            ),
        ] {
            let lease = issue_lease_for(&pool, subject, provider).await?;
            store_snapshot(&pool, subject, provider, serde_json::from_value(json!({
                "lease":lease["lease"],"start":start,"end":end,"grants":["metric"],"records":rows
            }))?, false).await?;
        }
        let queued: i64 = db
            .query_one(
                "SELECT count(*) FROM avatar_hsm.watch_tower_record_outbox
                 WHERE subject_id=$1 AND provider IN ('oura','polar')",
                &[&subject],
            )
            .await?
            .get(0);
        assert_eq!(
            queued, 2,
            "each committed cloud snapshot queues one payload-free receipt"
        );
        let mobile_queued: i64 = db
            .query_one(
                "SELECT count(*) FROM avatar_hsm.watch_tower_record_outbox
                 WHERE subject_id=$1 AND provider IN ('health_connect','healthkit','samsung_health')",
                &[&subject],
            )
            .await?
            .get(0);
        assert_eq!(
            mobile_queued, 0,
            "mobile uploads use the synchronous common MCP write path"
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let url = format!("http://{}/mcp/wearable-read", listener.local_addr()?);
        let app = std::sync::Arc::new(crate::mcp::App {
            db: pool.clone(),
            read_token: String::new(),
            control_token: String::new(),
            rules: crate::acute::rules()?,
        });
        let server = tokio::spawn(async move { axum::serve(listener, crate::router(app)).await });
        let client = reqwest::Client::new();
        let query = |token, provider, from, to| {
            client
                .post(&url)
                .bearer_auth(token)
                .json(&json!({
                    "jsonrpc":"2.0","id":1,"method":"tools/call","params":{
                        "name":"wearable_daily","arguments":{
                            "kind":"activity","provider":provider,"start_day":from,"end_day":to
                        }
                    }
                }))
                .send()
        };
        let day = "2026-04-24";
        let oura: Value = query(owner_token, "oura", day, day)
            .await?
            .error_for_status()?
            .json()
            .await?;
        let oura = &oura["result"]["structuredContent"];
        assert_eq!(oura["state"], "ready");
        assert_eq!(oura["records"].as_array().map(Vec::len), Some(1));
        assert_eq!(
            oura["records"][0]["stats"],
            json!({
                "total_energy_kcal":2100,"active_energy_kcal":525,"basal_energy_kcal":1575
            })
        );
        let polar: Value = query(owner_token, "polar", "2026-04-23", "2026-04-25")
            .await?
            .error_for_status()?
            .json()
            .await?;
        let polar = &polar["result"]["structuredContent"]["records"];
        assert_eq!(polar.as_array().map(Vec::len), Some(3));
        assert_eq!(
            polar[0]["stats"],
            json!({"total_energy_kcal":100,"active_energy_kcal":150})
        );
        assert_eq!(polar[1]["stats"]["basal_energy_kcal"], 1000);
        assert_eq!(polar[1]["stats"]["active_seconds"].as_f64(), Some(11460.0));
        assert_eq!(
            polar[1]["stats"]["inactive_seconds"].as_f64(),
            Some(66210.5)
        );
        assert_eq!(
            polar[1]["stats"]["step_distance_meters"].as_f64(),
            Some(4590.53)
        );
        assert_eq!(
            polar[1]["stats"]["activity_goal_percent"].as_f64(),
            Some(109.1)
        );
        assert_eq!(polar[1]["stats"]["inactivity_alert_count"], 0);
        assert_eq!(polar[2]["stats"], json!({"total_energy_kcal":1800}));
        let lease = issue_lease_for(&pool, subject, "polar").await?;
        let correction = activity(
            "Polar",
            "daily_activity:complete",
            "2026-04-26T01:00:00Z",
            json!({"end_time":"2026-04-24T23:59:59","active_duration":"PT",
                "inactive_duration":"P1D","distance_from_steps":10.5,"inactivity_alert_count":2.5}),
        );
        store_snapshot(&pool, subject, "polar", serde_json::from_value(json!({
            "lease":lease["lease"],"start":start,"end":end,"grants":["metric"],"records":[correction]
        }))?, false).await?;
        let revised: Value = query(owner_token, "polar", day, day)
            .await?
            .error_for_status()?
            .json()
            .await?;
        let stats = &revised["result"]["structuredContent"]["records"][0]["stats"];
        assert_eq!(stats["inactive_seconds"].as_f64(), Some(86400.0));
        assert_eq!(stats["step_distance_meters"].as_f64(), Some(10.5));
        assert!(
            stats.get("active_seconds").is_none() && stats.get("inactivity_alert_count").is_none()
        );
        assert!(stats.get("total_energy_kcal").is_none());
        let denied: Value = query(other_token, "oura", day, day)
            .await?
            .error_for_status()?
            .json()
            .await?;
        assert_eq!(denied["result"]["structuredContent"]["records"], json!([]));
        revoke_for(&pool, subject, "oura").await?;
        let revoked: Value = query(owner_token, "oura", day, day)
            .await?
            .error_for_status()?
            .json()
            .await?;
        assert_eq!(revoked["result"]["structuredContent"]["state"], "revoked");
        assert_eq!(revoked["result"]["structuredContent"]["records"], json!([]));
        server.abort();
        Ok(())
    }

    #[tokio::test]
    #[ignore = "requires isolated PostgreSQL"]
    async fn polar_zones_use_the_latest_provider_day_with_explicit_time_precision() -> Result<()> {
        let pool = crate::store::connect(&std::env::var("AVATAR_HSM_TEST_DATABASE_URL")?).await?;
        let subject = "polar-zones-owner";
        let other = "polar-zones-other";
        for user in [subject, other] {
            pool.get()
                .await?
                .execute(
                    "INSERT INTO avatar_hsm.subject VALUES($1,'Asia/Seoul')",
                    &[&user],
                )
                .await?;
        }
        let start = "2026-04-24T00:00:00Z";
        let end = "2026-04-25T00:00:00Z";
        let load =
            async |id, updated_at, samples| -> Result<()> {
                let lease = issue_lease_for(&pool, subject, "polar").await?;
                store_snapshot(&pool, subject, "polar", serde_json::from_value(json!({
                "lease":lease["lease"],"start":start,"end":end,"grants":["metric"],
                "records":[{"kind":"metric","source":"Polar","source_id":id,"started_at":start,
                    "ended_at":start,"modified_at":updated_at,
                    "payload":{"metric":"activity_samples","provider_data":{"date":"2026-04-24",
                        "activity_zones":{"samples":samples}}}}]
            }))?, false).await?;
                Ok(())
            };
        let query = async |user| {
            records_for(
                &pool,
                user,
                serde_json::from_value(json!({
                    "kind":"metric","provider":"polar","metric":"polar_activity_zone",
                    "start":start,"end":end
                }))?,
                false,
            )
            .await
        };
        load(
            "initial",
            "2026-04-25T00:00:00Z",
            json!([
                {"zone":"SEDENTARY","timestamp":"2026-04-24T06:30"},
                {"zone":"LIGHT","timestamp":"2026-04-24T06:30:00"},
                {"zone":"MODERATE","timestamp":"2026-04-24T08:00:00+02:00"},
                {"zone":"VIGOROUS","timestamp":"2026-04-31T08:00:00"},
                {"zone":"UNKNOWN","timestamp":"2026-04-24T08:00:00"}
            ]),
        )
        .await?;
        let first = query(subject).await?;
        let rows = first["records"].as_array().unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0]["payload"]["time_precision"], "instant");
        assert_eq!(rows[0]["started_at"], "2026-04-24T06:00:00+00:00");
        assert_eq!(rows[1]["payload"]["time_precision"], "provider_civil_time");
        assert_eq!(rows[1]["payload"]["provider_data"]["zone"], "LIGHT");
        assert_eq!(
            rows[1]["payload"]["provider_data"]["timestamp"],
            "2026-04-24T06:30:00"
        );
        assert_eq!(query(other).await?["records"], json!([]));
        load("correction", "2026-04-25T01:00:00Z", json!([])).await?;
        assert_eq!(query(subject).await?["records"], json!([]));
        Ok(())
    }

    #[tokio::test]
    #[ignore = "requires AVATAR_HSM_TEST_DATABASE_URL"]
    async fn healthkit_only_removes_explicitly_deleted_ids_under_its_own_lease() -> Result<()> {
        let pool = crate::store::connect(&std::env::var("AVATAR_HSM_TEST_DATABASE_URL")?).await?;
        let subject = "wearable-healthkit-deletion-fixture";
        let other = "wearable-healthkit-other-fixture";
        let id = "D1718DBB-F6C3-471B-ABEF-56BDF00A1029";
        let start = Utc::now() - Duration::days(2);
        let end = start + Duration::hours(1);
        let db = pool.get().await?;
        for subject in [subject, other] {
            db.execute(
                "INSERT INTO avatar_hsm.subject VALUES($1,'Asia/Seoul')",
                &[&subject],
            )
            .await?;
        }
        let record = json!({"kind":"steps","source":"fixture","source_id":id,"started_at":start,
            "ended_at":end,"modified_at":end,"payload":{"count":150}});
        let snapshot = |lease: Value, records, deleted| -> Result<Snapshot> {
            Ok(serde_json::from_value(
                json!({"lease":lease["lease"],"grants":["steps"],
                "start":start,"end":end+Duration::minutes(1),"records":records,"deleted_source_ids":deleted}),
            )?)
        };
        for (owner, provider) in [
            (subject, HEALTHKIT),
            (other, HEALTHKIT),
            (subject, HEALTH_CONNECT),
        ] {
            let lease = issue_lease_for(&pool, owner, provider).await?;
            commit_for(
                &pool,
                owner,
                provider,
                snapshot(lease, json!([record]), json!([]))?,
            )
            .await?;
        }
        let count = |owner, provider| {
            let db = &db;
            async move {
                Ok::<_, anyhow::Error>(db.query_one("SELECT count(*) FROM avatar_hsm.wearable_record WHERE subject_id=$1 AND provider=$2",
                    &[&owner,&provider]).await?.get::<_, i64>(0))
            }
        };
        let empty = issue_lease_for(&pool, subject, HEALTHKIT).await?;
        commit_for(
            &pool,
            subject,
            HEALTHKIT,
            snapshot(empty, json!([]), json!([]))?,
        )
        .await?;
        assert_eq!(count(subject, HEALTHKIT).await?, 1);

        let wrong = issue_lease_for(&pool, subject, SAMSUNG_HEALTH).await?;
        assert!(commit_for(
            &pool,
            subject,
            SAMSUNG_HEALTH,
            snapshot(wrong, json!([]), json!([id]))?
        )
        .await
        .is_err());
        let deletion = issue_lease_for(&pool, subject, HEALTHKIT).await?;
        assert!(commit_for(
            &pool,
            subject,
            HEALTHKIT,
            snapshot(deletion.clone(), json!([]), json!(["not-a-uuid"]))?
        )
        .await
        .is_err());
        let tombstone = snapshot(
            deletion.clone(),
            json!([]),
            json!([id.to_ascii_lowercase()]),
        )?;
        commit_for(&pool, subject, HEALTHKIT, tombstone).await?;
        assert!(commit_for(
            &pool,
            subject,
            HEALTHKIT,
            snapshot(deletion, json!([]), json!([id]))?
        )
        .await
        .is_err());
        assert_eq!(count(subject, HEALTHKIT).await?, 0);
        assert_eq!(count(other, HEALTHKIT).await?, 1);
        assert_eq!(count(subject, HEALTH_CONNECT).await?, 1);

        let old_start = start - Duration::days(45);
        let old_end = old_start + Duration::hours(1);
        let old_id = "e0bcf6f7-8fde-4e67-aa43-fb0cc417ccbb";
        let old = json!({"kind":"steps","source":"fixture","source_id":old_id,
            "started_at":old_start,"ended_at":old_end,"modified_at":old_end,"payload":{"count":42}});
        for _ in 0..2 {
            let lease = issue_lease_for(&pool, subject, HEALTHKIT).await?;
            let historical = serde_json::from_value(json!({
                "lease":lease["lease"],"grants":["steps"],"historical":true,
                "start":old_start,"end":old_end,"records":[old]
            }))?;
            commit_for(&pool, subject, HEALTHKIT, historical).await?;
        }
        let query = serde_json::from_value(json!({
            "kind":"steps","provider":HEALTHKIT,"start":old_start,"end":old_end+Duration::minutes(1)
        }))?;
        let old_samples = records_for(&pool, subject, query, false).await?;
        assert_eq!(old_samples["records"][0]["source_id"], old_id);
        assert_eq!(old_samples["records"][0]["payload"]["count"], 42);
        assert_eq!(old_samples["records"].as_array().unwrap().len(), 1);
        Ok(())
    }

    #[tokio::test]
    #[ignore = "requires AVATAR_HSM_TEST_DATABASE_URL"]
    async fn health_connect_deletions_use_exact_native_id_across_date_windows() -> Result<()> {
        let pool = crate::store::connect(&std::env::var("AVATAR_HSM_TEST_DATABASE_URL")?).await?;
        let subject = "wearable-android-deletion-fixture";
        let other = "wearable-android-other-fixture";
        let db = pool.get().await?;
        let start = Utc::now() - Duration::days(20);
        let end = start + Duration::hours(1);
        let pending = Utc::now() - Duration::minutes(5);
        for owner in [subject, other] {
            db.execute(
                "INSERT INTO avatar_hsm.subject VALUES($1,'Asia/Seoul')",
                &[&owner],
            )
            .await?;
            let lease = issue_lease_for(&pool, owner, HEALTH_CONNECT).await?;
            let records: Vec<_> = ["Native-Id", "native-id"]
                .iter()
                .map(|id| {
                    json!({"kind":"steps","source":"fixture","source_id":id,"started_at":start,
                    "ended_at":end,"modified_at":end,"payload":{"count":150}})
                })
                .collect();
            let snapshot =
                serde_json::from_value(json!({"lease":lease["lease"],"grants":["steps"],
                "start":start,"end":end,"historical":true,"records":records}))?;
            commit_for(&pool, owner, HEALTH_CONNECT, snapshot).await?;
        }
        for _ in 0..2 {
            let lease = issue_lease_for(&pool, subject, HEALTH_CONNECT).await?;
            let tombstone =
                serde_json::from_value(json!({"lease":lease["lease"],"grants":["steps"],
                "start":pending,"end":Utc::now(),"historical":true,"records":[],
                "deleted_source_ids":["Native-Id"]}))?;
            commit_for(&pool, subject, HEALTH_CONNECT, tombstone).await?;
        }
        let counts = db
            .query_one(
                "SELECT count(*) FILTER(WHERE subject_id=$1 AND source_id='Native-Id'),
             count(*) FILTER(WHERE subject_id=$1 AND source_id='native-id'),
             count(*) FILTER(WHERE subject_id=$2 AND source_id='Native-Id')
             FROM avatar_hsm.wearable_record WHERE provider='health_connect'",
                &[&subject, &other],
            )
            .await?;
        assert_eq!(
            (
                counts.get::<_, i64>(0),
                counts.get::<_, i64>(1),
                counts.get::<_, i64>(2)
            ),
            (0, 1, 1)
        );
        Ok(())
    }

    #[tokio::test]
    #[ignore = "requires isolated PostgreSQL in AVATAR_HSM_TEST_DATABASE_URL"]
    async fn samsung_complete_background_snapshot_removes_only_records_in_its_window() -> Result<()>
    {
        let pool = crate::store::connect(&std::env::var("AVATAR_HSM_TEST_DATABASE_URL")?).await?;
        let first = Utc::now() - Duration::days(3);
        let recent = first + Duration::days(1);
        let end = recent + Duration::hours(3);
        let point = |id: &str, at| {
            json!({
                "kind":"heart_rate","source":"fixture","source_id":id,
                "started_at":at,"ended_at":at,"modified_at":at,
                "payload":{"samples":[{"at":at,"bpm":76}]}
            })
        };
        let upload = async |provider: &str, start, records, historical, active| -> Result<()> {
            let lease = if active {
                issue_active_lease(&pool, provider, Some(1)).await?
            } else {
                issue_lease(&pool, provider).await?
            };
            commit(
                &pool,
                provider,
                serde_json::from_value(json!({
                    "lease":lease["lease"],"start":start,"end":end,
                    "historical":historical,"grants":["heart_rate"],"records":records
                }))?,
            )
            .await?;
            Ok(())
        };
        upload(
            SAMSUNG_HEALTH,
            first,
            vec![
                point("older-samsung", first),
                point("removed-in-samsung", recent),
            ],
            true,
            false,
        )
        .await?;
        upload(
            HEALTH_CONNECT,
            first,
            vec![point("another-provider", recent)],
            true,
            false,
        )
        .await?;
        upload(SAMSUNG_HEALTH, recent, vec![], true, true).await?;
        let stored_ids = async || -> Result<Vec<String>> {
            Ok(pool
                .get()
                .await?
                .query(
                    "SELECT source_id FROM avatar_hsm.wearable_record
                     WHERE subject_id=$1 AND source='fixture' ORDER BY source_id",
                    &[&SUBJECT],
                )
                .await?
                .iter()
                .map(|row| row.get(0))
                .collect())
        };
        assert_eq!(
            stored_ids().await?,
            ["another-provider", "older-samsung", "removed-in-samsung"]
        );
        upload(SAMSUNG_HEALTH, recent, vec![], false, true).await?;
        assert_eq!(stored_ids().await?, ["another-provider", "older-samsung"]);
        Ok(())
    }

    #[tokio::test]
    #[ignore = "requires isolated PostgreSQL in AVATAR_HSM_TEST_DATABASE_URL"]
    async fn samsung_sleep_deletion_removes_old_sessions_only() -> Result<()> {
        let pool = crate::store::connect(&std::env::var("AVATAR_HSM_TEST_DATABASE_URL")?).await?;
        let old: DateTime<Utc> = "2026-01-12T00:00:00Z".parse()?;
        let change_at = old + Duration::days(20);
        let old_id = "samsung-parent:2026-01-12T00:00:00Z";
        let newest_id = "samsung-parent:2026-01-12T01:00:00Z";
        let other_id = "samsung-parent:other:2026-01-12T00:00:00Z";
        let sleep = |id, at, revised| {
            json!({"kind":"sleep","source":"fixture","source_id":id,
                "started_at":at,"ended_at":at+Duration::minutes(15),"modified_at":revised,
                "payload":{"stages":[]}})
        };
        let upload = async |provider: &str, records: Value| -> Result<()> {
            let lease = issue_lease(&pool, provider).await?;
            commit(
                &pool,
                provider,
                serde_json::from_value(json!({
                    "lease":lease["lease"],"start":old,"end":old+Duration::days(1),
                    "historical":true,"grants":["sleep"],"records":records
                }))?,
            )
            .await?;
            Ok(())
        };
        upload(
            SAMSUNG_HEALTH,
            json!([
                sleep(old_id, old, old),
                sleep(
                    newest_id,
                    old + Duration::hours(1),
                    change_at + Duration::minutes(5)
                ),
                sleep(other_id, old, old)
            ]),
        )
        .await?;
        upload(HEALTH_CONNECT, json!([sleep(old_id, old, old)])).await?;
        for _ in 0..2 {
            let lease = issue_active_lease(&pool, SAMSUNG_HEALTH, Some(1)).await?;
            commit(
                &pool,
                SAMSUNG_HEALTH,
                serde_json::from_value(json!({
                    "lease":lease["lease"],"start":change_at-Duration::hours(1),
                    "end":change_at+Duration::hours(1),"historical":true,"grants":["sleep"],"records":[],
                    "samsung_sleep_deletions":[{"uid":"samsung-parent","changed_at":change_at}]
                }))?,
            )
            .await?;
        }
        let rows = pool.get().await?.query(
            "SELECT provider, source_id FROM avatar_hsm.wearable_record
             WHERE subject_id=$1 AND source='fixture' AND kind='sleep' ORDER BY provider, source_id",
            &[&SUBJECT],
        ).await?;
        let records: Vec<_> = rows
            .iter()
            .map(|row| (row.get::<_, &str>(0), row.get::<_, &str>(1)))
            .collect();
        assert_eq!(
            records,
            [
                (HEALTH_CONNECT, old_id),
                (SAMSUNG_HEALTH, newest_id),
                (SAMSUNG_HEALTH, other_id)
            ]
        );
        let revised_id = "samsung-parent:2026-01-12T02:00:00Z";
        let edit_at = change_at + Duration::minutes(10);
        let revision = json!({"uid":"samsung-parent","changed_at":edit_at});
        for expected in [1, 0] {
            let lease = issue_active_lease(&pool, SAMSUNG_HEALTH, Some(1)).await?;
            let result = commit(
                &pool,
                SAMSUNG_HEALTH,
                serde_json::from_value(json!({
                    "lease":lease["lease"],"start":old,"end":old+Duration::days(1),
                    "historical":true,"grants":["sleep"],
                    "samsung_sleep_revision":revision,
                    "records":[sleep(revised_id, old+Duration::hours(2), edit_at)]
                }))?,
            )
            .await?;
            assert_eq!(result["accepted"], expected);
        }
        upload(SAMSUNG_HEALTH, json!([sleep(old_id, old, old)])).await?;
        let rows = pool
            .get()
            .await?
            .query(
                "SELECT source_id FROM avatar_hsm.wearable_record
             WHERE subject_id=$1 AND provider='samsung_health' AND kind='sleep' ORDER BY source_id",
                &[&SUBJECT],
            )
            .await?;
        let ids: Vec<_> = rows.iter().map(|row| row.get::<_, &str>(0)).collect();
        assert_eq!(ids, [revised_id, other_id]);
        Ok(())
    }

    #[tokio::test]
    #[ignore = "requires AVATAR_HSM_TEST_DATABASE_URL"]
    async fn samsung_heart_rate_edits_and_deletions_use_exact_sdk_uid() -> Result<()> {
        let pool = crate::store::connect(&std::env::var("AVATAR_HSM_TEST_DATABASE_URL")?).await?;
        let first: DateTime<Utc> = "2026-04-25T08:00:00Z".parse()?;
        let changed_at = first + Duration::days(20);
        let uid = "heart-rate:device";
        let neighbor = "heart-rate:device:neighbor";
        let point = |id, at, revision, bpm| {
            json!({"kind":"heart_rate","source":"fixture","source_id":id,
                "started_at":at,"ended_at":at,"modified_at":revision,
                "payload":{"samples":[{"at":at,"bpm":bpm}]}})
        };
        let commit_changes = async |start, records: Value, changes: Value| -> Result<()> {
            let lease = issue_lease(&pool, SAMSUNG_HEALTH).await?;
            commit(
                &pool,
                SAMSUNG_HEALTH,
                serde_json::from_value(json!({
                    "lease":lease["lease"],"start":start,"end":start+Duration::hours(1),
                    "historical":true,"grants":["heart_rate"],"records":records,
                    "samsung_session_changes":changes
                }))?,
            )
            .await?;
            Ok(())
        };
        let original = point(uid, first, first, 64);
        commit_changes(
            first,
            json!([original, point(neighbor, first, first, 68)]),
            Value::Null,
        )
        .await?;
        let moved_at = first + Duration::minutes(5);
        commit_changes(
            first,
            json!([point(uid, moved_at, changed_at, 82)]),
            json!({
                "kind":"heart_rate","revision":{"uid":uid,"changed_at":changed_at}
            }),
        )
        .await?;
        let read_rows = async || -> Result<Vec<(String, Value)>> {
            Ok(pool.get().await?.query(
                "SELECT source_id,payload FROM avatar_hsm.wearable_record
                 WHERE subject_id=$1 AND provider='samsung_health' AND kind='heart_rate' ORDER BY source_id",
                &[&SUBJECT],
            ).await?.iter().map(|row| (row.get(0), row.get(1))).collect())
        };
        let revised = read_rows().await?;
        assert_eq!(revised.len(), 2);
        assert_eq!(revised[0].0, uid);
        assert_eq!(revised[0].1["samples"][0]["bpm"], 82);
        commit_changes(changed_at, json!([]), json!({
            "kind":"heart_rate","deletions":[{"uid":uid,"changed_at":changed_at+Duration::minutes(1)}]
        })).await?;
        commit_changes(first, json!([original]), Value::Null).await?;
        let remaining = read_rows().await?;
        assert_eq!(remaining.len(), 1);
        assert_eq!(remaining[0].0, neighbor);
        let newer = changed_at + Duration::minutes(2);
        commit_changes(first, json!([]), json!({
            "kind":"heart_rate",
            "revisions":[
               {"revision":{"uid":uid,"changed_at":changed_at},"records":[point(uid, moved_at, changed_at, 82)]},
               {"revision":{"uid":neighbor,"changed_at":newer},"records":[point(neighbor, first, newer, 72)]}
            ]
        })).await?;
        let after_batch = read_rows().await?;
        assert_eq!(after_batch.len(), 1);
        assert_eq!(after_batch[0].0, neighbor);
        assert_eq!(after_batch[0].1["samples"][0]["bpm"], 72);
        Ok(())
    }

    #[tokio::test]
    #[ignore = "requires AVATAR_HSM_TEST_DATABASE_URL"]
    async fn samsung_metric_group_replaces_only_its_consented_fields() -> Result<()> {
        let pool = crate::store::connect(&std::env::var("AVATAR_HSM_TEST_DATABASE_URL")?).await?;
        let at: DateTime<Utc> = "2026-04-25T08:00:00Z".parse()?;
        let changed = at + Duration::days(20);
        let uid = "fixture:metric";
        let systolic = "blood_pressure_systolic";
        let diastolic = "blood_pressure_diastolic";
        let glucose = "blood_glucose";
        let point = |metric, modified, value| {
            json!({
                "kind":"metric","source":"fixture.samsung","source_id":format!("{uid}:{metric}"),
                "started_at":at,"ended_at":at,"modified_at":modified,
                "payload":{"metric":metric,"provider_data":{"value":value}}
            })
        };
        let submit = async |start,
                            metric_grants: Value,
                            records: Value,
                            changes: Value|
               -> Result<()> {
            let lease = issue_lease(&pool, SAMSUNG_HEALTH).await?;
            commit(&pool, SAMSUNG_HEALTH, serde_json::from_value(json!({
                "lease":lease["lease"],"start":start,"end":start+Duration::hours(1),"historical":true,
                "grants":["metric"],"metric_grants_version":3,"metric_grants":metric_grants,
                "records":records,"samsung_session_changes":changes
            }))?).await?;
            Ok(())
        };
        let old = json!([
            point(systolic, at, 120),
            point(diastolic, at, 76),
            point(glucose, at, 5)
        ]);
        let grants = json!([systolic, diastolic, glucose]);
        submit(at, grants.clone(), old.clone(), Value::Null).await?;
        let revision = json!({"kind":"metric","revision":{"uid":uid,"changed_at":changed,"metrics":[systolic,diastolic]}});
        let updated = json!([point(systolic, changed, 125)]);
        assert!(submit(
            at,
            json!([systolic, glucose]),
            updated.clone(),
            revision.clone()
        )
        .await
        .is_err());
        submit(at, grants.clone(), updated, revision).await?;
        submit(at, grants.clone(), old.clone(), Value::Null).await?;
        let stored = async || -> Result<Vec<(String, i32)>> {
            Ok(pool
                .get()
                .await?
                .query(
                    "SELECT payload->>'metric',(payload#>>'{provider_data,value}')::int
                 FROM avatar_hsm.wearable_record WHERE subject_id=$1 AND provider='samsung_health'
                 ORDER BY payload->>'metric'",
                    &[&SUBJECT],
                )
                .await?
                .iter()
                .map(|row| (row.get(0), row.get(1)))
                .collect())
        };
        assert_eq!(
            stored().await?,
            [(glucose.to_owned(), 5), (systolic.to_owned(), 125)]
        );
        submit(changed, grants.clone(), json!([]), json!({
            "kind":"metric","deletions":[{"uid":uid,"changed_at":changed+Duration::minutes(1),"metrics":[systolic,diastolic]}]
        })).await?;
        submit(at, grants, old, Value::Null).await?;
        assert_eq!(stored().await?, [(glucose.to_owned(), 5)]);
        Ok(())
    }

    #[tokio::test]
    #[ignore = "requires AVATAR_HSM_TEST_DATABASE_URL"]
    async fn samsung_monitor_metrics_survive_old_clients_and_follow_current_consent() -> Result<()>
    {
        let pool = crate::store::connect(&std::env::var("AVATAR_HSM_TEST_DATABASE_URL")?).await?;
        let subject = "samsung-monitor-scope-fixture";
        let start = Utc::now() - Duration::days(3);
        let end = start + Duration::days(1);
        pool.get()
            .await?
            .execute(
                "INSERT INTO avatar_hsm.subject VALUES ($1,'Asia/Seoul')",
                &[&subject],
            )
            .await?;
        let build_snapshot = |lease: Value, start, end, version, metrics: Value, records: Value| {
            serde_json::from_value(json!({"lease":lease["lease"],"start":start,"end":end,
                "grants":if metrics.as_array().unwrap().is_empty() {vec![]} else {vec!["metric"]},
                "metric_grants_version":version,"metric_grants":metrics,"records":records}))
        };
        let monitor = |name: &str, data| {
            json!({
                "source":"com.samsung.android.shealthmonitor","source_id":format!("fixture:{name}"),
                "kind":"metric","started_at":start,"ended_at":start,"modified_at":start,
                "payload":{"metric":name,"provider_data":data}
            })
        };
        let apnea = "sleep_apnea";
        let rhythm = "irregular_heart_rhythm_notification";
        let lease = issue_lease_for(&pool, subject, SAMSUNG_HEALTH).await?;
        commit_for(
            &pool,
            subject,
            SAMSUNG_HEALTH,
            build_snapshot(
                lease,
                start,
                end,
                3,
                json!([apnea, rhythm]),
                json!([
                    monitor(
                        apnea,
                        json!({"detected_sign":"UNDEFINED","utc_offset":"+09:00"})
                    ),
                    monitor(rhythm, json!({"status":"UNDEFINED","utc_offset":"+09:00"}))
                ]),
            )?,
        )
        .await?;

        let read = |metric: &str| {
            records_for(
                &pool,
                subject,
                RecordQuery {
                    start,
                    end,
                    kind: "metric".into(),
                    provider: Some(SAMSUNG_HEALTH.into()),
                    offset: 0,
                    source: None,
                    metric: Some(metric.into()),
                },
                false,
            )
        };
        let lease = issue_lease_for(&pool, subject, SAMSUNG_HEALTH).await?;
        commit_for(
            &pool,
            subject,
            SAMSUNG_HEALTH,
            build_snapshot(lease, start, end, 2, json!([]), json!([]))?,
        )
        .await?;
        assert_eq!(
            read(apnea).await?["records"][0]["payload"]["provider_data"]["detected_sign"],
            "UNDEFINED"
        );
        assert_eq!(
            read(rhythm).await?["records"][0]["payload"]["provider_data"]["utc_offset"],
            "+09:00"
        );

        let lease = issue_lease_for(&pool, subject, SAMSUNG_HEALTH).await?;
        commit_for(
            &pool,
            subject,
            SAMSUNG_HEALTH,
            build_snapshot(
                lease,
                start + Duration::days(2),
                end + Duration::days(2),
                3,
                json!([rhythm]),
                json!([]),
            )?,
        )
        .await?;
        assert_eq!(read(apnea).await?["records"], json!([]));
        assert_eq!(
            read(rhythm).await?["records"].as_array().map(Vec::len),
            Some(1)
        );
        Ok(())
    }

    #[tokio::test]
    #[ignore = "requires AVATAR_HSM_TEST_DATABASE_URL"]
    async fn mobile_metric_revocation_removes_older_rows_only_for_the_affected_provider(
    ) -> Result<()> {
        let pool = crate::store::connect(&std::env::var("AVATAR_HSM_TEST_DATABASE_URL")?).await?;
        let subject = "mobile-metric-fixture";
        pool.get()
            .await?
            .execute(
                "INSERT INTO avatar_hsm.subject VALUES($1,'Asia/Seoul')",
                &[&subject],
            )
            .await?;
        let today: DateTime<Utc> = "2026-04-25T00:00:00Z".parse()?;
        let yesterday = today - Duration::days(1);
        let point = |name: &str, value: f64, unit: &str| {
            json!({"kind":"metric","source":"fixture.device","source_id":name,
            "started_at":yesterday,"ended_at":yesterday,"modified_at":yesterday,
            "payload":{"metric":name,"provider_data":{"value":value,"unit":unit}}})
        };
        let samsung_lease = issue_lease_for(&pool, subject, SAMSUNG_HEALTH).await?;
        let samsung = serde_json::from_value(
            json!({"lease":samsung_lease["lease"],"start":yesterday,"end":today,
                "grants":["metric"],"metric_grants":["weight","body_fat","blood_glucose"],
                "records":[point("weight",73.0,"kg"),point("body_fat",25.1,"%"),point("blood_glucose",5.8,"mmol/L")]
            }),
        )?;
        commit_for(&pool, subject, SAMSUNG_HEALTH, samsung).await?;
        let lease = issue_lease_for(&pool, subject, HEALTH_CONNECT).await?;
        let initial =
            serde_json::from_value(json!({"lease":lease["lease"],"start":yesterday,"end":today,
            "grants":["metric"],"metric_grants":["weight","oxygen_saturation"],
            "records":[point("weight",72.4,"kg"),point("oxygen_saturation",98.0,"%")] }))?;
        commit_for(&pool, subject, HEALTH_CONNECT, initial).await?;
        let lease = issue_lease_for(&pool, subject, HEALTH_CONNECT).await?;
        let reduced = serde_json::from_value(
            json!({"lease":lease["lease"],"start":today,"end":today+Duration::days(1),
            "historical":true,"grants":["metric"],"metric_grants":["oxygen_saturation"],"records":[] }),
        )?;
        commit_for(&pool, subject, HEALTH_CONNECT, reduced).await?;
        let samsung_lease = issue_lease_for(&pool, subject, SAMSUNG_HEALTH).await?;
        let samsung_reduced = serde_json::from_value(json!({"lease":samsung_lease["lease"],
            "start":today,"end":today+Duration::days(1),"historical":true,
            "grants":["metric"],"metric_grants":["blood_glucose"],"records":[]
        }))?;
        commit_for(&pool, subject, SAMSUNG_HEALTH, samsung_reduced).await?;
        let data = records_for(
            &pool,
            subject,
            RecordQuery {
                kind: "metric".into(),
                start: yesterday,
                end: today,
                offset: 0,
                source: None,
                provider: Some(HEALTH_CONNECT.into()),
                metric: None,
            },
            false,
        )
        .await?;
        assert_eq!(data["records"].as_array().map(Vec::len), Some(1));
        assert_eq!(data["records"][0]["payload"]["metric"], "oxygen_saturation");
        assert_eq!(
            data["records"][0]["payload"]["provider_data"]["value"].as_f64(),
            Some(98.0)
        );
        let samsung = records_for(
            &pool,
            subject,
            RecordQuery {
                kind: "metric".into(),
                start: yesterday,
                end: today,
                offset: 0,
                source: None,
                provider: Some(SAMSUNG_HEALTH.into()),
                metric: None,
            },
            false,
        )
        .await?;
        assert_eq!(samsung["records"].as_array().map(Vec::len), Some(1));
        assert_eq!(samsung["records"][0]["payload"]["metric"], "blood_glucose");
        let meal = json!({"kind":"metric","source":"fixture.device","source_id":"meal-1",
        "started_at":yesterday,"ended_at":yesterday,"modified_at":yesterday,
        "payload":{"metric":"nutrition","provider_data":{
          "value":510,"unit":"kcal","title":"fixture meal","nutrients":{"protein_g":32.5}
        }}});
        let lease = issue_lease_for(&pool, subject, SAMSUNG_HEALTH).await?;
        let snapshot = serde_json::from_value(
            json!({"lease":lease["lease"],"start":yesterday,"end":today,
                "grants":["metric"],"metric_grants_version":2,"metric_grants":["nutrition","water_intake"],
                "records":[meal,point("water_intake",245.5,"mL")]
            }),
        )?;
        commit_for(&pool, subject, SAMSUNG_HEALTH, snapshot).await?;
        let lease = issue_lease_for(&pool, subject, SAMSUNG_HEALTH).await?;
        let legacy =
            serde_json::from_value(json!({"lease":lease["lease"],"start":yesterday,"end":today,
                "grants":[],"metric_grants":[],"records":[]
            }))?;
        commit_for(&pool, subject, SAMSUNG_HEALTH, legacy).await?;
        let query = |metric: &str| RecordQuery {
            kind: "metric".into(),
            start: yesterday,
            end: today,
            offset: 0,
            source: None,
            provider: Some(SAMSUNG_HEALTH.into()),
            metric: Some(metric.into()),
        };
        let read_token = "mobile-metric-fixture-read-token-0123456789";
        pool.get()
            .await?
            .execute(
                "INSERT INTO avatar_hsm.wearable_profile VALUES(
            $1,sha256(convert_to($2,'UTF8')),sha256(convert_to($2||'c','UTF8')))",
                &[&subject, &read_token],
            )
            .await?;
        let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0)).await?;
        let address = listener.local_addr()?;
        let app = std::sync::Arc::new(crate::mcp::App {
            db: pool.clone(),
            read_token: String::new(),
            control_token: String::new(),
            rules: crate::acute::rules()?,
        });
        let server = tokio::spawn(async move { axum::serve(listener, crate::router(app)).await });
        let response = crate::cloud_wearables::http_client()?
            .post(format!("http://{address}/mcp/wearable-read"))
            .bearer_auth(read_token)
            .json(&json!({"jsonrpc":"2.0","id":1,"method":"tools/call","params":{
                "name":"wearable_samples","arguments":{"kind":"metric","start":yesterday,"end":today,
                    "provider":SAMSUNG_HEALTH,"metric":"nutrition"}
            }}))
            .send().await?.error_for_status()?.json::<Value>().await?;
        server.abort();
        let meals = &response["result"]["structuredContent"];
        assert_eq!(
            meals["records"][0]["payload"]["provider_data"]["nutrients"]["protein_g"],
            32.5
        );
        let lease = issue_lease_for(&pool, subject, SAMSUNG_HEALTH).await?;
        let reduced = serde_json::from_value(
            json!({"lease":lease["lease"],"start":today,"end":today+Duration::days(1),
                "historical":true,"grants":["metric"],"metric_grants_version":2,"metric_grants":["water_intake"],
                "records":[]
            }),
        )?;
        commit_for(&pool, subject, SAMSUNG_HEALTH, reduced).await?;
        let meals = records_for(&pool, subject, query("nutrition"), false).await?;
        let water = records_for(&pool, subject, query("water_intake"), false).await?;
        assert_eq!(meals["state"], "no_records");
        assert_eq!(
            water["records"][0]["payload"]["provider_data"]["value"],
            245.5
        );
        let lease = issue_lease_for(&pool, subject, HEALTH_CONNECT).await?;
        let snapshot = serde_json::from_value(
            json!({"lease":lease["lease"],"start":yesterday,"end":today,
                "grants":["metric"],"metric_grants_version":2,"metric_grants":["distance","water_intake"],
                "records":[point("distance",1575.5,"m"),point("water_intake",300.0,"mL")]
            }),
        )?;
        commit_for(&pool, subject, HEALTH_CONNECT, snapshot).await?;
        let lease = issue_lease_for(&pool, subject, HEALTH_CONNECT).await?;
        let legacy =
            serde_json::from_value(json!({"lease":lease["lease"],"start":yesterday,"end":today,
                "grants":[],"metric_grants":[],"records":[]
            }))?;
        commit_for(&pool, subject, HEALTH_CONNECT, legacy).await?;
        let query_android = |metric| RecordQuery {
            provider: Some(HEALTH_CONNECT.into()),
            ..query(metric)
        };
        let distance = records_for(&pool, subject, query_android("distance"), false).await?;
        assert_eq!(
            distance["records"][0]["payload"]["provider_data"]["value"],
            1575.5
        );
        let lease = issue_lease_for(&pool, subject, HEALTH_CONNECT).await?;
        let revoked = serde_json::from_value(
            json!({"lease":lease["lease"],"start":today,"end":today+Duration::days(1),
                "historical":true,"grants":["metric"],"metric_grants_version":2,"metric_grants":["water_intake"],
                "records":[]
            }),
        )?;
        commit_for(&pool, subject, HEALTH_CONNECT, revoked).await?;
        assert_eq!(
            records_for(&pool, subject, query_android("distance"), false).await?["state"],
            "no_records"
        );
        assert_eq!(
            records_for(&pool, subject, query_android("water_intake"), false).await?["records"][0]
                ["payload"]["provider_data"]["value"]
                .as_f64(),
            Some(300.0)
        );

        let lease = issue_lease_for(&pool, subject, HEALTH_CONNECT).await?;
        let mut series = point("speed", 0.0, "m/s");
        series["payload"]["provider_data"] = json!({
            "unit":"m/s","samples":[{"at":series["started_at"],"value":0},{"at":series["ended_at"],"value":3.2}]
        });
        let upload = serde_json::from_value(json!({
            "lease":lease["lease"],"start":yesterday,"end":today,"grants":["metric"],
            "metric_grants_version":3,"metric_grants":["speed","water_intake"],"records":[series]
        }))?;
        commit_for(&pool, subject, HEALTH_CONNECT, upload).await?;

        for version in [2, 3] {
            let lease = issue_lease_for(&pool, subject, HEALTH_CONNECT).await?;
            let without_speed = serde_json::from_value(json!({
                "lease":lease["lease"],"start":today,"end":today+Duration::days(1),"historical":true,
                "grants":["metric"],"metric_grants_version":version,"metric_grants":["water_intake"],"records":[]
            }))?;
            commit_for(&pool, subject, HEALTH_CONNECT, without_speed).await?;
            let speed = records_for(&pool, subject, query_android("speed"), false).await?;
            if version == 2 {
                assert_eq!(
                    speed["records"][0]["payload"]["provider_data"]["samples"][0]["value"],
                    0
                );
            } else {
                assert_eq!(speed["records"], json!([]));
            }
        }
        let lease = issue_lease_for(&pool, subject, HEALTH_CONNECT).await?;
        let mut period = point("menstruation_period", 0.0, "");
        let period_end = today + Duration::days(9);
        period["ended_at"] = json!(period_end);
        period["payload"]["provider_data"] =
            json!({"start_offset_seconds":32400,"end_offset_seconds":32400});
        let period_upload = serde_json::from_value(json!({
            "lease": lease["lease"],"start":yesterday,"end":today,"grants":["metric"],
            "metric_grants_version":4,"metric_grants":["menstruation_period","water_intake"],
            "records":[period]
        }))?;
        commit_for(&pool, subject, HEALTH_CONNECT, period_upload).await?;
        for version in [3, 4] {
            let lease = issue_lease_for(&pool, subject, HEALTH_CONNECT).await?;
            let sync = serde_json::from_value(json!({
                "lease":lease["lease"],"start":today,"end":today+Duration::days(1),"historical":true,
                "grants":["metric"],"metric_grants_version":version,"metric_grants":["water_intake"],"records":[]
            }))?;
            commit_for(&pool, subject, HEALTH_CONNECT, sync).await?;
            let period =
                records_for(&pool, subject, query_android("menstruation_period"), false).await?;
            if version == 3 {
                assert_eq!(
                    period["records"][0]["ended_at"],
                    json!(period_end.to_rfc3339())
                );
                assert_eq!(
                    period["records"][0]["payload"]["provider_data"]["start_offset_seconds"],
                    32400
                );
            } else {
                assert_eq!(period["records"], json!([]));
            }
        }
        let lease = issue_lease_for(&pool, subject, HEALTH_CONNECT).await?;
        let pushes = point("wheelchair_pushes", 124.0, "pushes");
        let upload = serde_json::from_value(json!({
            "lease":lease["lease"],"start":yesterday,"end":today,"grants":["metric"],
            "metric_grants_version":5,"metric_grants":["wheelchair_pushes","water_intake"],"records":[pushes]
        }))?;
        commit_for(&pool, subject, HEALTH_CONNECT, upload).await?;
        for version in [4, 5] {
            let lease = issue_lease_for(&pool, subject, HEALTH_CONNECT).await?;
            let sync = serde_json::from_value(json!({
                "lease":lease["lease"],"start":today,"end":today+Duration::days(1),"historical":true,
                "grants":["metric"],"metric_grants_version":version,"metric_grants":["water_intake"],"records":[]
            }))?;
            commit_for(&pool, subject, HEALTH_CONNECT, sync).await?;
            let pushes =
                records_for(&pool, subject, query_android("wheelchair_pushes"), false).await?;
            if version == 4 {
                assert_eq!(
                    pushes["records"][0]["payload"]["provider_data"]["value"],
                    124.0
                );
            } else {
                assert_eq!(pushes["records"], json!([]));
            }
        }
        let lease = issue_lease_for(&pool, subject, HEALTH_CONNECT).await?;
        let mut mindfulness = point("mindfulness_session", 510.0, "s");
        mindfulness["payload"]["provider_data"] =
            json!({"duration_seconds":510,"session_type_code":2});
        let upload = serde_json::from_value(json!({
            "lease":lease["lease"],"start":yesterday,"end":today,"grants":["metric"],
            "metric_grants_version":6,"metric_grants":["mindfulness_session","water_intake"],"records":[mindfulness]
        }))?;
        commit_for(&pool, subject, HEALTH_CONNECT, upload).await?;
        for version in [5, 6] {
            let lease = issue_lease_for(&pool, subject, HEALTH_CONNECT).await?;
            let sync = serde_json::from_value(json!({
                "lease":lease["lease"],"start":today,"end":today+Duration::days(1),"historical":true,
                "grants":["metric"],"metric_grants_version":version,"metric_grants":["water_intake"],"records":[]
            }))?;
            commit_for(&pool, subject, HEALTH_CONNECT, sync).await?;
            let sessions =
                records_for(&pool, subject, query_android("mindfulness_session"), false).await?;
            if version == 5 {
                assert_eq!(
                    sessions["records"][0]["payload"]["provider_data"]["duration_seconds"],
                    510
                );
            } else {
                assert_eq!(sessions["records"], json!([]));
            }
        }
        let lease = issue_lease_for(&pool, subject, HEALTH_CONNECT).await?;
        let mut intensity = point("activity_intensity", 120.0, "s");
        intensity["payload"]["provider_data"] =
            json!({"duration_seconds":120,"intensity_code":0,"start_offset_seconds":32400});
        let mindfulness = point("mindfulness_session", 510.0, "s");
        let upload = serde_json::from_value(json!({
            "lease":lease["lease"],"start":yesterday,"end":today,"grants":["metric"],
            "metric_grants_version":7,"metric_grants":["activity_intensity","mindfulness_session","water_intake"],
            "records":[intensity,mindfulness]
        }))?;
        commit_for(&pool, subject, HEALTH_CONNECT, upload).await?;
        for version in [6, 8] {
            let lease = issue_lease_for(&pool, subject, HEALTH_CONNECT).await?;
            let sync = serde_json::from_value(json!({
                "lease":lease["lease"],"start":today,"end":today+Duration::days(1),"historical":true,
                "grants":["metric"],"metric_grants_version":version,
                "metric_grants":if version==6 { vec!["water_intake","mindfulness_session"] } else { vec!["water_intake"] },
                "records":[]
            }))?;
            commit_for(&pool, subject, HEALTH_CONNECT, sync).await?;
            let records =
                records_for(&pool, subject, query_android("activity_intensity"), false).await?;
            if version == 6 {
                assert_eq!(
                    records["records"][0]["payload"]["provider_data"]["intensity_code"],
                    0
                );
            } else {
                assert_eq!(records["records"], json!([]));
            }
            let preserved =
                records_for(&pool, subject, query_android("mindfulness_session"), false).await?;
            assert_eq!(preserved["records"].as_array().map(Vec::len), Some(1));
        }
        let mut temperature = point("skin_temperature", 32.5, "°C");
        temperature["ended_at"] = json!(today + Duration::minutes(30));
        temperature["payload"]["provider_data"] = json!({
            "unit":"°C", "baseline_celsius":null, "measurement_location_code":2,
            "deltas":[{"at":yesterday,"delta_celsius":-0.25}]
        });
        let lease = issue_lease_for(&pool, subject, HEALTH_CONNECT).await?;
        commit_for(
            &pool,
            subject,
            HEALTH_CONNECT,
            serde_json::from_value(json!({
                "lease":lease["lease"],"start":yesterday,"end":today,"historical":true,
                "grants":["metric"],"metric_grants_version":9,
                "metric_grants":["skin_temperature","water_intake"],
                "records":[temperature,point("water_intake",300.0,"mL")],
            }))?,
        )
        .await?;
        for (version, count) in [(8, 1), (9, 0)] {
            let lease = issue_lease_for(&pool, subject, HEALTH_CONNECT).await?;
            commit_for(&pool, subject, HEALTH_CONNECT, serde_json::from_value(json!({
                "lease":lease["lease"],"start":today,"end":today+Duration::days(1),"historical":true,
                "grants":["metric"],"metric_grants_version":version,"metric_grants":["water_intake"],"records":[],
            }))?).await?;
            let result =
                records_for(&pool, subject, query_android("skin_temperature"), false).await?;
            assert_eq!(result["records"].as_array().map(Vec::len), Some(count));
            if count > 0 {
                let data = &result["records"][0]["payload"]["provider_data"];
                assert!(data["baseline_celsius"].is_null());
                assert_eq!(data["deltas"][0]["delta_celsius"].as_f64(), Some(-0.25));
            }
            let other = records_for(&pool, subject, query_android("water_intake"), false).await?;
            assert_eq!(other["records"].as_array().map(Vec::len), Some(1));
        }
        Ok(())
    }

    #[tokio::test]
    #[ignore = "requires an isolated database in AVATAR_HSM_TEST_DATABASE_URL"]
    async fn health_connect_daily_intensity_splits_days_deduplicates_and_reflects_deletion(
    ) -> Result<()> {
        let pool = crate::store::connect(&std::env::var("AVATAR_HSM_TEST_DATABASE_URL")?).await?;
        let subject = "health-connect-daily-intensity-fixture";
        let source = "fixture.watch";
        let db = pool.get().await?;
        db.execute(
            "INSERT INTO avatar_hsm.subject VALUES ($1,'America/New_York')",
            &[&subject],
        )
        .await?;
        let record = |source, id, start, end, intensity| {
            json!({
                "kind":"metric","source":source,"source_id":id,"started_at":start,
                "ended_at":end,"modified_at":end,"payload":{
                    "metric":"activity_intensity","provider_data":{"intensity_code":intensity}
                }
            })
        };
        let commit = |records: Value, deleted: Value| {
            let pool = &pool;
            async move {
                let lease = issue_lease_for(pool, subject, HEALTH_CONNECT).await?;
                let snapshot = serde_json::from_value(json!({
                    "lease":lease["lease"],"start":"2026-03-08T04:00:00Z","end":"2026-03-08T08:00:00Z",
                    "historical":true,"grants":["metric"],"metric_grants":["activity_intensity"],"metric_grants_version":8,
                    "records":records,"deleted_source_ids":deleted,
                }))?;
                commit_for(pool, subject, HEALTH_CONNECT, snapshot).await
            }
        };
        commit(
            json!([
                record(
                    source,
                    "midnight",
                    "2026-03-08T04:58:00Z",
                    "2026-03-08T05:06:00Z",
                    0
                ),
                record(
                    source,
                    "moderate-overlap",
                    "2026-03-08T05:02:00Z",
                    "2026-03-08T05:05:00Z",
                    0
                ),
                record(
                    source,
                    "vigorous",
                    "2026-03-08T05:01:00Z",
                    "2026-03-08T05:03:00Z",
                    1
                ),
                record(
                    source,
                    "daylight-saving",
                    "2026-03-08T06:58:00Z",
                    "2026-03-08T07:02:00Z",
                    0
                ),
                record(
                    "fixture.other",
                    "other-source",
                    "2026-03-08T05:04:00Z",
                    "2026-03-08T05:05:00Z",
                    1
                )
            ]),
            json!([]),
        )
        .await?;
        let query = |filter: Option<&str>| DailyQuery {
            start_day: NaiveDate::from_ymd_opt(2026, 3, 7).unwrap(),
            end_day: NaiveDate::from_ymd_opt(2026, 3, 8).unwrap(),
            source: filter.map(str::to_owned),
            provider: Some(HEALTH_CONNECT.into()),
            kind: Some("activity".into()),
        };
        let both = daily_for(&pool, subject, query(None)).await?;
        assert_eq!(both["records"].as_array().map(Vec::len), Some(3));
        let watched = daily_for(&pool, subject, query(Some(source))).await?;
        assert_eq!(watched["state"], "ready");
        let days = watched["records"].as_array().unwrap();
        assert_eq!(days.len(), 2);
        assert_eq!(days[0]["day"], "2026-03-07");
        assert_eq!(
            days[0]["stats"]["activity_intensity_seconds"].as_f64(),
            Some(120.0)
        );
        assert_eq!(days[1]["day"], "2026-03-08");
        assert_eq!(days[1]["timezone"], "America/New_York");
        assert_eq!(days[1]["calculated_by"], "hsm");
        assert_eq!(days[1]["stats"]["moderate_seconds"].as_f64(), Some(480.0));
        assert_eq!(days[1]["stats"]["vigorous_seconds"].as_f64(), Some(120.0));
        assert_eq!(
            days[1]["stats"]["activity_intensity_seconds"].as_f64(),
            Some(600.0)
        );
        assert_eq!(
            days[1]["stats"]["weighted_intensity_minutes"].as_f64(),
            Some(12.0)
        );
        commit(json!([]), json!(["vigorous"])).await?;
        let refreshed = daily_for(&pool, subject, query(Some(source))).await?;
        assert_eq!(
            refreshed["records"][1]["stats"]["vigorous_seconds"].as_f64(),
            Some(0.0)
        );
        assert_eq!(
            refreshed["records"][1]["stats"]["weighted_intensity_minutes"].as_f64(),
            Some(10.0)
        );
        Ok(())
    }

    #[tokio::test]
    #[ignore = "requires an isolated database in AVATAR_HSM_TEST_DATABASE_URL"]
    async fn health_connect_nutrition_preserves_missing_values_on_replay() -> Result<()> {
        let pool = crate::store::connect(&std::env::var("AVATAR_HSM_TEST_DATABASE_URL")?).await?;
        let at: DateTime<Utc> = "2026-01-02T00:00:00Z".parse()?;
        let send = |source_id: &'static str, event_at: DateTime<Utc>, data: Value, modified| {
            let pool = &pool;
            async move {
                let lease = issue_lease(pool, HEALTH_CONNECT).await?;
                let snapshot = serde_json::from_value(json!({
                    "lease": lease["lease"], "start": event_at, "end": event_at + Duration::hours(1),
                    "grants":["metric"], "metric_grants_version":2, "metric_grants":["nutrition"],
                    "records":[{"kind":"metric","source":"fixture","source_id":source_id,
                        "started_at":event_at,"ended_at":event_at,"modified_at":modified,
                        "payload":{"metric":"nutrition","provider_data":data}}]
                }))?;
                commit(pool, HEALTH_CONNECT, snapshot).await
            }
        };
        let read = || async {
            let response = records(
                &pool,
                RecordQuery {
                    kind: "metric".into(),
                    start: at,
                    end: at + Duration::hours(1),
                    offset: 0,
                    source: None,
                    provider: Some(HEALTH_CONNECT.into()),
                    metric: Some("nutrition".into()),
                },
                false,
            )
            .await?;
            Ok::<_, anyhow::Error>(response["records"][0]["payload"]["provider_data"].clone())
        };
        let legacy = json!({"title":"breakfast","value":0,"unit":"kcal","nutrients_g":{"protein":12.5,"sodium":0,"sugar":0}});
        let accurate = json!({"title":"breakfast","missing_values_preserved":true,"nutrients_g":{"protein":12.5,"sodium":0}});
        send("meal", at, legacy.clone(), at).await?;
        send("meal", at, legacy.clone(), at).await?;
        assert_eq!(read().await?, legacy);
        let replay_count: i64 = pool.get().await?.query_one(
            "SELECT count(*) FROM avatar_hsm.wearable_record WHERE subject_id=$1 AND provider=$2 AND kind='metric' AND source='fixture' AND source_id='meal'",
            &[&SUBJECT, &HEALTH_CONNECT],
        ).await?.get(0);
        assert_eq!(
            replay_count, 1,
            "replaying one device source ID must not create another nutrition record"
        );
        let day: NaiveDate = pool.get().await?.query_one(
            "SELECT ($1::timestamptz AT TIME ZONE timezone)::date FROM avatar_hsm.subject WHERE id=$2",
            &[&at,&SUBJECT],
        ).await?.get(0);
        let samsung_lease = issue_lease(&pool, SAMSUNG_HEALTH).await?;
        let samsung_duplicate = serde_json::from_value(json!({
            "lease":samsung_lease["lease"],"start":at,"end":at+Duration::hours(1),
            "grants":["metric"],"metric_grants_version":2,"metric_grants":["nutrition"],
            "records":[{"kind":"metric","source":"fixture.samsung","source_id":"same-meal-different-device-id",
                "started_at":at,"ended_at":at,"modified_at":at,
                "payload":{"metric":"nutrition","provider_data":legacy}}]
        }))?;
        commit(&pool, SAMSUNG_HEALTH, samsung_duplicate).await?;
        let cross_device_count: i32 = pool.get().await?.query_one(
            "SELECT meal_count FROM avatar_hsm.nutrition_daily_state WHERE subject_id=$1 AND local_day=$2",
            &[&SUBJECT,&day],
        ).await?.get(0);
        assert_eq!(
            cross_device_count, 1,
            "same event time, food title and nutrient values from a second device must count once"
        );
        let state = pool.get().await?.query_one(
            "SELECT meal_count,calories_kcal,protein_g FROM avatar_hsm.nutrition_daily_state WHERE subject_id=$1 AND local_day=$2",
            &[&SUBJECT,&day],
        ).await?;
        assert_eq!(state.get::<_, i32>(0), 1);
        assert_eq!(state.get::<_, Option<f64>>(1), Some(0.0));
        assert_eq!(state.get::<_, Option<f64>>(2), Some(12.5));
        send("meal", at, accurate.clone(), at).await?;
        send("meal", at, legacy, at).await?;
        assert_eq!(read().await?, accurate);
        let newer = json!({"title":"breakfast","missing_values_preserved":true,"value":0,"unit":"kcal","nutrients_g":{}});
        send("meal", at, newer.clone(), at + Duration::minutes(1)).await?;
        assert_eq!(read().await?, newer);
        let late_at = at - Duration::days(1);
        let late = json!({"title":"late breakfast","value":80,"unit":"kcal","nutrients_g":{"protein":5,"totalCarbohydrate":10,"totalFat":2}});
        send("late-meal", late_at, late, at).await?;
        let late_day: NaiveDate = pool.get().await?.query_one(
            "SELECT ($1::timestamptz AT TIME ZONE timezone)::date FROM avatar_hsm.subject WHERE id=$2",
            &[&late_at,&SUBJECT],
        ).await?.get(0);
        let late_state = pool.get().await?.query_one(
            "SELECT meal_count,calories_kcal,protein_g,carbs_g,fat_g FROM avatar_hsm.nutrition_daily_state WHERE subject_id=$1 AND local_day=$2",
            &[&SUBJECT,&late_day],
        ).await?;
        assert_eq!(late_state.get::<_, i32>(0), 1);
        assert_eq!(late_state.get::<_, Option<f64>>(1), Some(80.0));
        assert_eq!(late_state.get::<_, Option<f64>>(2), Some(5.0));
        assert_eq!(late_state.get::<_, Option<f64>>(3), Some(10.0));
        assert_eq!(late_state.get::<_, Option<f64>>(4), Some(2.0));
        let healthkit_lease = issue_lease(&pool, HEALTHKIT).await?;
        let healthkit_rows = [
            (
                "a0000000-0000-0000-0000-000000000001",
                "dietary_energy",
                120.0,
                "kcal",
            ),
            (
                "a0000000-0000-0000-0000-000000000002",
                "dietary_protein",
                8.0,
                "g",
            ),
            (
                "a0000000-0000-0000-0000-000000000003",
                "dietary_carbohydrate",
                25.0,
                "g",
            ),
            (
                "a0000000-0000-0000-0000-000000000004",
                "dietary_fat",
                4.0,
                "g",
            ),
        ];
        let records = healthkit_rows
            .iter()
            .map(|(id, metric, value, unit)| {
                json!({
                    "kind":"metric","source":"com.apple.Health","source_id":id,
                    "started_at":late_at,"ended_at":late_at,"modified_at":at,
                    "payload":{"metric":metric,"provider_data":{"value":value,"unit":unit}}
                })
            })
            .collect::<Vec<_>>();
        let healthkit_snapshot = serde_json::from_value(json!({
            "lease":healthkit_lease["lease"],"start":late_at,"end":late_at+Duration::hours(1),
            "grants":["metric"],"records":records
        }))?;
        commit(&pool, HEALTHKIT, healthkit_snapshot).await?;
        let combined = pool.get().await?.query_one(
            "SELECT meal_count,calories_kcal,protein_g,carbs_g,fat_g FROM avatar_hsm.nutrition_daily_state WHERE subject_id=$1 AND local_day=$2",
            &[&SUBJECT,&late_day],
        ).await?;
        assert_eq!(combined.get::<_, i32>(0), 2);
        assert_eq!(combined.get::<_, Option<f64>>(1), Some(200.0));
        assert_eq!(combined.get::<_, Option<f64>>(2), Some(13.0));
        assert_eq!(combined.get::<_, Option<f64>>(3), Some(35.0));
        assert_eq!(combined.get::<_, Option<f64>>(4), Some(6.0));
        Ok(())
    }

    #[tokio::test]
    #[ignore = "requires an isolated database in AVATAR_HSM_TEST_DATABASE_URL"]
    async fn snapshot_commit_query_replace_and_revoke() -> Result<()> {
        let db_url = std::env::var("AVATAR_HSM_TEST_DATABASE_URL")?;
        let pool = crate::store::connect(&db_url).await?;
        let at: DateTime<Utc> = "2026-01-01T00:00:00Z".parse()?;
        let end = at + Duration::hours(1);
        let records = json!([
            {"kind":"steps","source":"fixture","source_id":"steps","started_at":at,"ended_at":end,"modified_at":at,"payload":{"count":840}},
            {"kind":"heart_rate","source":"fixture","source_id":"hr","started_at":at,"ended_at":end,"modified_at":at,
             "payload":{"samples":[{"at":at,"bpm":80},{"at":at+Duration::minutes(10),"bpm":105}]}},
            {"kind":"sleep","source":"fixture","source_id":"sleep","started_at":at,"ended_at":end,"modified_at":at,
             "payload":{"stages":[{"start":at,"end":end,"stage":5}]}},
            {"kind":"workout","source":"fixture","source_id":"workout","started_at":at,"ended_at":end,"modified_at":at,
             "payload":{"exercise_type":1,"start_offset_seconds":32400,"end_offset_seconds":32400,
               "rate_of_perceived_exertion":0,
               "segments":[{"start":at,"end":end,"segment_type":5,"repetitions":8,
                 "weight_kg":62.5,"set_index":0,"rate_of_perceived_exertion":7.5}]}}
        ]);
        let snapshot = |lease: Value, records: Value| -> Result<Snapshot> {
            Ok(serde_json::from_value(json!({
                "lease":lease, "start":at - Duration::hours(1), "end":end + Duration::hours(1),
                "grants": KINDS, "records":records
            }))?)
        };
        let query = |kind: &str| RecordQuery {
            kind: kind.into(),
            start: at,
            end,
            offset: 0,
            source: None,
            provider: None,
            metric: None,
        };
        let lease = issue_lease(&pool, HEALTH_CONNECT).await?;
        let first_commit = commit(
            &pool,
            HEALTH_CONNECT,
            snapshot(lease["lease"].clone(), records.clone())?,
        )
        .await?;
        assert_eq!(first_commit["accepted"], 4);
        assert_eq!(
            commit(
                &pool,
                HEALTH_CONNECT,
                snapshot(lease["lease"].clone(), records.clone())?
            )
            .await?,
            first_commit
        );
        let receipt_db = pool.get().await?;
        let stored_receipt: Value = receipt_db
            .query_one(
                "SELECT result FROM avatar_hsm.snapshot_write_receipt
                 WHERE subject_id=$1 AND provider=$2
                   AND lease_hash=sha256(convert_to($3,'UTF8'))",
                &[&SUBJECT, &HEALTH_CONNECT, &lease["lease"].as_str().unwrap()],
            )
            .await?
            .get(0);
        assert_eq!(stored_receipt, first_commit);
        let mut changed_records = records.clone();
        changed_records[0]["payload"]["count"] = json!(841);
        assert!(commit(
            &pool,
            HEALTH_CONNECT,
            snapshot(lease["lease"].clone(), changed_records)?
        )
        .await
        .is_err());
        let daily_query = || DailyQuery {
            start_day: "2026-01-01".parse().unwrap(),
            end_day: "2026-01-01".parse().unwrap(),
            source: None,
            provider: None,
            kind: None,
        };
        assert_eq!(
            daily(&pool, daily_query()).await?["records"][0]["stats"]["total"],
            840
        );
        let daily_heart = daily(
            &pool,
            DailyQuery {
                kind: Some("heart_rate".into()),
                ..daily_query()
            },
        )
        .await?;
        assert_eq!(
            daily_heart["records"][0]["stats"]["average"].as_f64(),
            Some(92.5)
        );
        let sessions = self::records(&pool, query("workout"), true).await?;
        assert_eq!(sessions["records"][0]["related_heart_rate"]["max_bpm"], 105);
        assert_eq!(sessions["records"][0]["payload"], records[3]["payload"]);
        let mut legacy = records.clone();
        legacy[3]["payload"] = json!({
            "exercise_type":1, "segments":[{"start":at,"end":end,"segment_type":5,"repetitions":8}]
        });
        let lease = issue_lease(&pool, HEALTH_CONNECT).await?;
        commit(
            &pool,
            HEALTH_CONNECT,
            snapshot(lease["lease"].clone(), legacy.clone())?,
        )
        .await?;
        let sessions = self::records(&pool, query("workout"), true).await?;
        assert_eq!(sessions["records"][0]["payload"], records[3]["payload"]);

        // A real revision can explicitly remove optional data. A richer read
        // of that new revision can still add the details to a legacy upload.
        legacy[3]["modified_at"] = json!(at + Duration::minutes(1));
        let lease = issue_lease(&pool, HEALTH_CONNECT).await?;
        commit(
            &pool,
            HEALTH_CONNECT,
            snapshot(lease["lease"].clone(), legacy.clone())?,
        )
        .await?;
        let sessions = self::records(&pool, query("workout"), true).await?;
        assert_eq!(sessions["records"][0]["payload"], legacy[3]["payload"]);

        assert_eq!(
            self::records(&pool, query("sleep"), true).await?["records"][0]["payload"]["stages"][0]
                ["stage"],
            5
        );
        let mut records = records;
        records[3]["modified_at"] = legacy[3]["modified_at"].clone();
        records[3]["payload"]
            .as_object_mut()
            .unwrap()
            .remove("rate_of_perceived_exertion");
        let expected_payload = records[3]["payload"].clone();
        let lease = issue_lease(&pool, HEALTH_CONNECT).await?;
        commit(
            &pool,
            HEALTH_CONNECT,
            snapshot(lease["lease"].clone(), records)?,
        )
        .await?;
        let sessions = self::records(&pool, query("workout"), true).await?;
        assert_eq!(sessions["records"][0]["payload"], expected_payload);
        legacy[3]["payload"]["start_offset_seconds"] = json!(32400);
        legacy[3]["payload"]["end_offset_seconds"] = json!(32400);
        let lease = issue_lease(&pool, HEALTH_CONNECT).await?;
        commit(
            &pool,
            HEALTH_CONNECT,
            snapshot(lease["lease"].clone(), legacy)?,
        )
        .await?;
        let sessions = self::records(&pool, query("workout"), true).await?;
        assert_eq!(sessions["records"][0]["payload"], expected_payload);
        assert_eq!(
            self::records(&pool, query("steps"), false).await?["records"]
                .as_array()
                .unwrap()
                .len(),
            1
        );
        let lease = issue_lease(&pool, HEALTH_CONNECT).await?;
        commit(
            &pool,
            HEALTH_CONNECT,
            snapshot(lease["lease"].clone(), json!([]))?,
        )
        .await?;
        assert_eq!(daily(&pool, daily_query()).await?["state"], "no_records");
        let lease = issue_lease(&pool, HEALTH_CONNECT).await?;
        revoke(&pool, HEALTH_CONNECT).await?;
        assert!(commit(
            &pool,
            HEALTH_CONNECT,
            snapshot(lease["lease"].clone(), json!([]))?
        )
        .await
        .is_err());
        assert_eq!(
            self::records(&pool, query("steps"), false).await?["state"],
            "revoked"
        );
        let apple = issue_lease(&pool, HEALTHKIT).await?;
        let apple_payload = json!([{
            "kind":"steps","source":"fixture","source_id":"steps","started_at":at,"ended_at":end,
            "modified_at":at,"payload":{"count":250}
        }]);
        assert!(commit(
            &pool,
            HEALTH_CONNECT,
            snapshot(apple["lease"].clone(), apple_payload.clone())?
        )
        .await
        .is_err());
        let mut apple_snapshot = snapshot(apple["lease"].clone(), apple_payload)?;
        apple_snapshot.grants = vec!["steps".into()];
        commit(&pool, HEALTHKIT, apple_snapshot).await?;
        assert_eq!(
            daily(&pool, daily_query()).await?["records"][0]["stats"]["total"],
            250
        );
        assert_eq!(
            daily(
                &pool,
                DailyQuery {
                    provider: Some(HEALTH_CONNECT.into()),
                    ..daily_query()
                }
            )
            .await?["state"],
            "revoked"
        );
        assert_eq!(
            self::records(
                &pool,
                RecordQuery {
                    provider: Some(HEALTHKIT.into()),
                    ..query("heart_rate")
                },
                false
            )
            .await?["state"],
            "permission_unknown"
        );
        let app = std::sync::Arc::new(crate::mcp::App {
            db: pool.clone(),
            read_token: "fixture-read".into(),
            control_token: "fixture-control".into(),
            rules: crate::acute::rules()?,
        });
        let call = |name: &str, arguments: Value| {
            let app = app.clone();
            let message = json!({"jsonrpc":"2.0","id":1,"method":"tools/call",
                "params":{"name":name,"arguments":arguments}});
            async move {
                let mut headers = axum::http::HeaderMap::new();
                headers.insert(
                    "Authorization",
                    axum::http::HeaderValue::from_static("Bearer fixture-read"),
                );
                let response = crate::mcp::handle(
                    axum::extract::State(app),
                    axum::extract::Path("read".into()),
                    headers,
                    axum::Json(message),
                )
                .await;
                assert_eq!(response.status(), axum::http::StatusCode::OK);
                let bytes = axum::body::to_bytes(response.into_body(), 16 * 1024).await?;
                Ok::<Value, anyhow::Error>(serde_json::from_slice(&bytes)?)
            }
        };
        let result = call(
            "wearable_samples",
            json!({"kind":"steps","provider":"healthkit","start":at,"end":end}),
        )
        .await?;
        assert_eq!(
            result["result"]["structuredContent"]["records"][0]["payload"]["count"],
            250
        );
        let result = call("cloud_device_status", json!({"provider":"google_health"})).await?;
        assert!(result["result"]["structuredContent"]["configured"].is_boolean());
        let result = call("sync_cloud_device", json!({"provider":"google_health"})).await?;
        assert_eq!(result["result"]["isError"], true);
        let samsung = issue_lease(&pool, SAMSUNG_HEALTH).await?;
        let mut samsung_snapshot = snapshot(
            samsung["lease"].clone(),
            json!([{
                "kind":"steps","source":"Samsung Health","source_id":"hour:fixture","started_at":at,
                "ended_at":end,"modified_at":end,"payload":{"count":300,"aggregation":"samsung_hourly"}
            }]),
        )?;
        samsung_snapshot.provider = Some(SAMSUNG_HEALTH.into());
        samsung_snapshot.grants = vec!["steps".into()];
        assert_eq!(samsung_snapshot.mobile_provider()?, SAMSUNG_HEALTH);
        commit(&pool, SAMSUNG_HEALTH, samsung_snapshot).await?;
        let historical_lease = issue_lease(&pool, SAMSUNG_HEALTH).await?;
        let mut historical = snapshot(historical_lease["lease"].clone(), json!([]))?;
        historical.provider = Some(SAMSUNG_HEALTH.into());
        historical.historical = true;
        historical.grants = vec!["steps".into()];
        commit(&pool, SAMSUNG_HEALTH, historical).await?;
        let result = call(
            "wearable_samples",
            json!({"kind":"steps","provider":"samsung_health","start":at,"end":end}),
        )
        .await?;
        assert_eq!(
            result["result"]["structuredContent"]["records"][0]["payload"]["count"],
            300
        );
        let current_status = status(&pool, SAMSUNG_HEALTH).await?;
        let previous = current_status["generation"]
            .as_str()
            .unwrap()
            .parse::<i64>()?;
        assert_eq!(current_status["state"], "synced");
        assert!(current_status.get("lease").is_none());
        let background = issue_active_lease(&pool, SAMSUNG_HEALTH, Some(previous)).await?;
        assert!(issue_active_lease(&pool, SAMSUNG_HEALTH, Some(previous))
            .await
            .is_err());
        revoke(&pool, SAMSUNG_HEALTH).await?;
        assert!(issue_active_lease(&pool, SAMSUNG_HEALTH, Some(previous))
            .await
            .is_err());
        assert!(commit(
            &pool,
            SAMSUNG_HEALTH,
            snapshot(background["lease"].clone(), json!([]))?
        )
        .await
        .is_err());
        let reopened = issue_lease(&pool, SAMSUNG_HEALTH).await?;
        let current = reopened["generation"].as_str().unwrap().parse::<i64>()?;
        assert_eq!(current, previous + 1);
        commit(
            &pool,
            SAMSUNG_HEALTH,
            snapshot(reopened["lease"].clone(), json!([]))?,
        )
        .await?;
        assert!(issue_active_lease(&pool, SAMSUNG_HEALTH, Some(previous))
            .await
            .is_err());
        assert!(issue_active_lease(&pool, SAMSUNG_HEALTH, Some(current))
            .await
            .is_ok());
        revoke(&pool, SAMSUNG_HEALTH).await?;
        let result = call(
            "wearable_samples",
            json!({"kind":"steps","provider":"healthkit","start":at,"end":end}),
        )
        .await?;
        assert_eq!(
            result["result"]["structuredContent"]["records"][0]["payload"]["count"],
            250
        );
        Ok(())
    }
}
