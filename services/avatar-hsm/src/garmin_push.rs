use std::{collections::BTreeMap, env, sync::Arc};

use anyhow::{ensure, Context, Result};
use axum::{
    body::Bytes,
    extract::{Path, State},
    http::{HeaderMap, StatusCode},
};
use chrono::{DateTime, Duration, Utc};
use deadpool_postgres::Pool;
use serde_json::{json, Value};
use subtle::ConstantTimeEq;

use crate::{
    cloud_wearables::{self, Provider},
    garmin_pull,
    mcp::App,
    wearable_fit, wearable_oauth,
    wearable_webhooks::{account_id, hash},
    wearables::{self, Record, Snapshot},
};

#[cfg(test)]
#[path = "garmin_push_tests.rs"]
mod tests;

const PROVIDER: &str = "garmin";
pub const ACTIVITY_EXPORT: i16 = 1;
pub const HEALTH_EXPORT: i16 = 2;
pub const MAX_BODY: usize = 8 * 1024 * 1024;
const METRICS: &[&str] = &[
    "dailies",
    "epochs",
    "bodyComps",
    "hrv",
    "stressDetails",
    "allDayRespiration",
    "pulseox",
    "healthSnapshot",
    "skinTemp",
    "moveIQActivities",
    "mct",
    "userMetrics",
    "bloodPressures",
    "activityDetails",
];

fn key() -> Option<String> {
    env::var("AVATAR_GARMIN_WEBHOOK_PATH_KEY")
        .ok()
        .filter(|key| {
            key.len() >= 43
                && key
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b"-_".contains(&b))
        })
}

pub fn configured() -> bool {
    key().is_some()
}

fn permission_mask(value: &Value) -> Result<i16> {
    let mut mask = 0;
    for permission in value
        .as_array()
        .context("invalid Garmin permissions list")?
    {
        mask |= match permission.as_str().context("invalid Garmin permission")? {
            "ACTIVITY_EXPORT" => ACTIVITY_EXPORT,
            "HEALTH_EXPORT" => HEALTH_EXPORT,
            _ => 0,
        };
    }
    Ok(mask)
}

pub async fn read_permissions(client: &reqwest::Client, token: &str) -> Result<i16> {
    let permissions = cloud_wearables::body(
        client
            .get("https://apis.garmin.com/wellness-api/rest/user/permissions")
            .bearer_auth(token)
            .send()
            .await?,
    )
    .await?;
    permission_mask(&permissions)
}

pub async fn receive(
    State(app): State<Arc<App>>,
    Path(path): Path<String>,
    headers: HeaderMap,
    body: Bytes,
) -> StatusCode {
    if !key().is_some_and(|key| bool::from(key.as_bytes().ct_eq(path.as_bytes()))) {
        return StatusCode::NOT_FOUND;
    }
    let client = headers
        .get("garmin-client-id")
        .and_then(|v| v.to_str().ok());
    if !env::var("AVATAR_GARMIN_CLIENT_ID")
        .is_ok_and(|expected| !expected.is_empty() && client == Some(expected.as_str()))
    {
        return StatusCode::UNAUTHORIZED;
    }
    if body.len() > MAX_BODY {
        return StatusCode::PAYLOAD_TOO_LARGE;
    }
    let Ok(mut batch) = serde_json::from_slice::<Value>(&body)
        .map_err(anyhow::Error::from)
        .and_then(|payload| decode(&payload, Utc::now()))
    else {
        return StatusCode::UNPROCESSABLE_ENTITY;
    };
    if confirm_permission_changes(&app.db, &mut batch)
        .await
        .is_err()
    {
        return StatusCode::SERVICE_UNAVAILABLE;
    }
    match persist(&app.db, &hash(&body), batch).await {
        Ok(()) => StatusCode::OK,
        Err(_) => StatusCode::SERVICE_UNAVAILABLE,
    }
}

#[derive(Default)]
struct OwnerBatch {
    revoked: bool,
    permissions: Option<i16>,
    permission_check: Option<(String, DateTime<Utc>)>,
    records: Vec<(i16, Record)>,
    fit_files: Vec<(String, reqwest::Url)>,
    pulls: Vec<(String, i16, reqwest::Url)>,
}

pub fn summary_kind(feed: &str) -> Option<(&'static str, i16)> {
    match feed {
        "activities" => Some(("workout", ACTIVITY_EXPORT)),
        "activityDetails" => Some(("metric", ACTIVITY_EXPORT)),
        "sleeps" => Some(("sleep", HEALTH_EXPORT)),
        name if METRICS.contains(&name) => Some(("metric", HEALTH_EXPORT)),
        _ => None,
    }
}

async fn confirm_permission_changes(
    pool: &Pool,
    owners: &mut BTreeMap<Vec<u8>, OwnerBatch>,
) -> Result<()> {
    let accounts: Vec<_> = owners
        .iter()
        .filter(|(_, batch)| batch.permissions.is_some() && !batch.revoked)
        .map(|(id, _)| id.clone())
        .collect();
    if accounts.is_empty() {
        return Ok(());
    }
    let db = pool.get().await?;
    let connections = db.query("SELECT subject_id,provider_account_hash,generation::text FROM avatar_hsm.wearable_oauth_connection
        WHERE provider='garmin' AND provider_account_hash=ANY($1::bytea[])", &[&accounts]).await?;
    drop(db);
    let client = cloud_wearables::http_client()?;
    for connection in connections {
        let subject: String = connection.get(0);
        let account: Vec<u8> = connection.get(1);
        if let Some(batch) = owners.get_mut(&account) {
            let credentials =
                wearable_oauth::access(&client, pool, &subject, Provider::Garmin).await?;
            let observed_at = Utc::now();
            batch.permissions = Some(read_permissions(&client, &credentials[0]).await?);
            batch.permission_check = Some((connection.get(2), observed_at));
        }
    }
    // A permission notification with no matching active connection has no authority after a concurrent reconnect.
    for account in accounts {
        if let Some(batch) = owners
            .get_mut(&account)
            .filter(|batch| batch.permission_check.is_none())
        {
            batch.permissions = None;
        }
    }
    Ok(())
}

fn decode(payload: &Value, received: DateTime<Utc>) -> Result<BTreeMap<Vec<u8>, OwnerBatch>> {
    let mut owners = BTreeMap::<Vec<u8>, OwnerBatch>::new();
    let mut count = 0;
    for (feed, rows) in payload.as_object().context("invalid Garmin push object")? {
        let kind = match feed.as_str() {
            "deregistrations" => "revoke",
            "userPermissionsChange" => "permission",
            "activityFiles" => "fit",
            name if summary_kind(name).is_some() => "summary",
            _ => continue,
        };
        let rows = rows.as_array().context("invalid Garmin feed")?;
        count += rows.len();
        ensure!(count <= 2_000, "Garmin batch exceeds record limit");
        for row in rows {
            if kind == "fit" && row["fileType"] != "FIT" {
                continue;
            }
            let owner = account_id(&row["userId"]).context("Garmin delivery has no user ID")?;
            let item = owners.entry(hash(owner.as_bytes())).or_default();
            if kind == "revoke" {
                item.revoked = true;
            } else if kind == "permission" {
                item.permissions = Some(permission_mask(&row["permissions"])?);
            } else if kind == "fit" {
                let activity =
                    account_id(&row["activityId"]).context("Garmin FIT activity ID missing")?;
                let url = wearable_fit::garmin_source(
                    row["callbackURL"]
                        .as_str()
                        .context("Garmin FIT source missing")?,
                )?;
                item.fit_files.push((activity, url));
            } else {
                let (kind, permission) = summary_kind(feed).context("unknown Garmin feed")?;
                if let Some(callback) = row.get("callbackURL") {
                    let url = garmin_pull::source(
                        feed,
                        callback.as_str().context("invalid Garmin pull address")?,
                    )?;
                    item.pulls.push((feed.to_owned(), permission, url));
                } else {
                    item.records
                        .push((permission, decode_record(row, kind, feed, received)?));
                }
            }
        }
    }
    Ok(owners)
}

pub fn decode_record(
    row: &Value,
    kind: &str,
    feed: &str,
    received: DateTime<Utc>,
) -> Result<Record> {
    ensure!(
        row.get("callbackURL").is_none(),
        "Garmin ping requires a pull integration"
    );
    let detail = row.get("summary").unwrap_or(row);
    let time_fields = if feed == "mct" {
        &["calendarDate", "startTimeInSeconds", "periodStartDate"][..]
    } else if feed == "bloodPressures" {
        &[
            "measurementTimeInSeconds",
            "measurementTimestampGMT",
            "startTimeInSeconds",
        ][..]
    } else {
        &[
            "startTimeInSeconds",
            "measurementTimeInSeconds",
            "calendarDate",
        ][..]
    };
    let at = time_fields
        .into_iter()
        .find_map(|field| {
            let value = &detail[*field];
            if let Some(seconds) = value.as_i64() {
                return DateTime::from_timestamp(seconds, 0);
            }
            if *field == "measurementTimestampGMT" {
                return value.as_str()?.parse::<DateTime<Utc>>().ok();
            }
            value
                .as_str()?
                .parse::<chrono::NaiveDate>()
                .ok()?
                .and_hms_opt(0, 0, 0)
                .map(|time| time.and_utc())
        })
        .context("Garmin record has no usable timestamp")?;
    let length = detail["durationInSeconds"].as_i64().unwrap_or(0);
    ensure!((0..=604_800).contains(&length), "invalid Garmin duration");
    let end = at
        .checked_add_signed(Duration::seconds(length))
        .context("invalid Garmin end time")?;
    let end = if feed == "dailies" {
        end.min(received.max(at))
    } else {
        end
    };
    let source_id = if kind == "workout" || feed == "activityDetails" {
        account_id(&row["activityId"]).or_else(|| account_id(&detail["activityId"]))
    } else {
        account_id(&row["summaryId"]).or_else(|| account_id(&detail["summaryId"]))
    }
    .context("Garmin record has no stable ID")?;
    let mut summary = row.as_object().context("invalid Garmin summary")?.clone();
    for field in ["userId", "userAccessToken", "callbackURL"] {
        summary.remove(field);
        if let Some(inner) = summary.get_mut("summary").and_then(Value::as_object_mut) {
            inner.remove(field);
        }
    }
    let (id, payload) = match kind {
        "workout" => (
            source_id,
            json!({"exercise_type":detail["activityType"].as_str().unwrap_or("garmin"),"distance_meters":detail["distanceInMeters"],"duration_unknown":length==0,"provider_data": summary}),
        ),
        "sleep" => (
            source_id,
            json!({"stages":sleep_stages(&detail["sleepLevelsMap"],at,end),"provider_data":summary}),
        ),
        _ => (
            format!("{feed}:{source_id}"),
            json!({"metric":feed,"provider_data":summary}),
        ),
    };
    let modified_at = if feed == "mct" {
        crate::garmin_metrics::cycle_updated_at(detail, received).unwrap_or(received)
    } else {
        received
    };
    let snapshot = Snapshot {
        provider: None,
        historical: false,
        lease: String::new(),
        start: at,
        end: end.max(
            at.checked_add_signed(Duration::seconds(1))
                .context("invalid Garmin interval")?,
        ),
        grants: vec![kind.to_owned()],
        metric_grants: None,
        metric_grants_version: None,
        samsung_activity: None,
        samsung_routes: None,
        health_connect_routes: None,
        healthkit_routes: None,
        samsung_goals: None,
        deleted_source_ids: Vec::new(),
        samsung_sleep_deletions: Vec::new(),
        samsung_sleep_revision: None,
        samsung_session_changes: None,
        google_metric_coverage: Vec::new(),
        withings_measurement_access: None,
        oura_sleep_deletions: None,
        coros_fit_sources: Default::default(),
        records: vec![serde_json::from_value(
            json!({"kind": kind, "source":"Garmin Connect", "source_id":id,
            "started_at":at,"ended_at":end,"modified_at":modified_at,"payload":payload}),
        )?],
    };
    wearables::validate_snapshot(&snapshot)?;
    snapshot
        .records
        .into_iter()
        .next()
        .context("missing Garmin record")
}

pub fn validate_normalized_record(record: Record) -> Result<Record> {
    let snapshot = Snapshot {
        provider: None,
        historical: false,
        lease: String::new(),
        start: record.started_at,
        end: record.ended_at,
        grants: vec![record.kind.clone()],
        metric_grants: None,
        metric_grants_version: None,
        samsung_activity: None,
        samsung_routes: None,
        health_connect_routes: None,
        healthkit_routes: None,
        samsung_goals: None,
        deleted_source_ids: Vec::new(),
        samsung_sleep_deletions: Vec::new(),
        samsung_sleep_revision: None,
        samsung_session_changes: None,
        google_metric_coverage: Vec::new(),
        withings_measurement_access: None,
        oura_sleep_deletions: None,
        coros_fit_sources: Default::default(),
        records: vec![record],
    };
    wearables::validate_snapshot(&snapshot)?;
    snapshot
        .records
        .into_iter()
        .next()
        .context("missing Garmin record")
}

fn sleep_stages(levels: &Value, start: DateTime<Utc>, end: DateTime<Utc>) -> Vec<Value> {
    let mut stages = Vec::new();
    for (level, stage) in [
        ("awake", 1),
        ("light", 4),
        ("deep", 5),
        ("rem", 6),
        ("unmeasurable", 0),
    ] {
        if let Some(items) = levels[level].as_array() {
            for item in items {
                let bounds = item["startTimeInSeconds"]
                    .as_i64()
                    .zip(item["endTimeInSeconds"].as_i64())
                    .and_then(|(a, b)| {
                        Some((
                            DateTime::from_timestamp(a, 0)?,
                            DateTime::from_timestamp(b, 0)?,
                        ))
                    });
                if let Some((from, until)) =
                    bounds.filter(|(from, until)| *from >= start && from < until && *until <= end)
                {
                    stages.push((from, until, stage));
                }
            }
        }
    }
    stages.sort_unstable();
    if stages.len() > 500 || stages.windows(2).any(|pair| pair[0].1 > pair[1].0) {
        return Vec::new();
    }
    stages
        .into_iter()
        .map(|(from, until, stage)| json!({"start":from,"end":until,"stage":stage}))
        .collect()
}

async fn persist(
    pool: &Pool,
    receipt: &[u8],
    mut owners: BTreeMap<Vec<u8>, OwnerBatch>,
) -> Result<()> {
    let mut db = pool.get().await?;
    let tx = db.transaction().await?;
    let accounts: Vec<_> = owners.keys().cloned().collect();
    let connections = tx
        .query(
            "SELECT subject_id,provider_account_hash FROM avatar_hsm.wearable_oauth_connection
        WHERE provider=$1 AND provider_account_hash=ANY($2::bytea[]) ORDER BY subject_id",
            &[&PROVIDER, &accounts],
        )
        .await?;
    if connections.is_empty() {
        return Ok(());
    }
    let is_new = tx.execute("INSERT INTO watch_tower.wearable_webhook_receipt(provider,event_hash) VALUES($1,$2) ON CONFLICT DO NOTHING", &[&PROVIDER,&receipt]).await? != 0;
    for connection in connections {
        let subject: String = connection.get(0);
        let account: Vec<u8> = connection.get(1);
        let Some(batch) = owners.remove(&account) else {
            continue;
        };
        tx.query_one(
            "SELECT pg_advisory_xact_lock(hashtext($1),hashtext($2))",
            &[&subject, &PROVIDER],
        )
        .await?;
        let Some(connection) = tx.query_opt("SELECT garmin_permissions,generation::text,garmin_permissions_observed_at
            FROM avatar_hsm.wearable_oauth_connection WHERE subject_id=$1 AND provider=$2 AND provider_account_hash=$3", &[&subject,&PROVIDER,&account]).await? else {
            continue;
        };
        if batch.revoked {
            if is_new {
                wearables::revoke_locked(&tx, &subject, PROVIDER).await?;
            }
            continue;
        }
        let mut permissions: i16 = connection.get(0);
        if let Some(changed) = batch.permissions {
            let confirmed =
                batch
                    .permission_check
                    .as_ref()
                    .is_some_and(|(generation, observed)| {
                        *generation == connection.get::<_, String>(1)
                            && connection
                                .get::<_, Option<DateTime<Utc>>>(2)
                                .is_none_or(|last| *observed >= last)
                    });
            if confirmed {
                let (_, observed_at) = batch
                    .permission_check
                    .context("missing Garmin permission observation")?;
                apply_permissions(&tx, &subject, changed, observed_at).await?;
                permissions = changed;
            }
        }
        if !is_new {
            continue;
        }
        let records = batch
            .records
            .iter()
            .filter(|(required, _)| permissions & required != 0)
            .map(|(_, record)| record);
        let has_files = !batch.fit_files.is_empty() && permissions & ACTIVITY_EXPORT != 0;
        let files = batch
            .fit_files
            .into_iter()
            .filter(|_| permissions & ACTIVITY_EXPORT != 0);
        store_records(&tx, &subject, records, has_files).await?;
        for (activity, url) in files {
            wearable_fit::enqueue_garmin(&tx, &subject, &activity, &url).await?;
        }
        for (feed, permission, url) in batch.pulls {
            if permissions & permission != 0 {
                garmin_pull::enqueue(&tx, &subject, &feed, permission, &url).await?;
            }
        }
    }
    tx.commit().await?;
    Ok(())
}

pub async fn store_records<'a>(
    tx: &tokio_postgres::Transaction<'_>,
    subject: &str,
    records: impl Iterator<Item = &'a Record> + Clone,
    has_files: bool,
) -> Result<()> {
    let mut kinds = records
        .clone()
        .map(|record| record.kind.as_str())
        .collect::<Vec<_>>();
    let mut daily_changed = false;
    for record in records {
        wearables::upsert_record(tx, subject, PROVIDER, record).await?;
        if record.payload["metric"] == "activityDetails" {
            crate::garmin_workouts::store(tx, subject, record).await?;
        }
        daily_changed |= crate::garmin_metrics::project(tx, subject, record, &mut kinds).await?;
    }
    if daily_changed {
        wearables::rebuild_daily(tx, subject, PROVIDER).await?;
    }
    if !kinds.is_empty() || has_files {
        tx.execute("UPDATE avatar_hsm.device_connection SET state='synced',last_synced_at=now(),
            grants=ARRAY(SELECT DISTINCT unnest(grants || $3::text[])) WHERE subject_id=$1 AND provider=$2 AND state<>'revoked'",
            &[&subject,&PROVIDER,&kinds]).await?;
    }
    Ok(())
}

async fn apply_permissions(
    tx: &tokio_postgres::Transaction<'_>,
    subject: &str,
    permissions: i16,
    observed_at: DateTime<Utc>,
) -> Result<()> {
    let activity = permissions & ACTIVITY_EXPORT != 0;
    let health = permissions & HEALTH_EXPORT != 0;
    tx.execute("UPDATE avatar_hsm.wearable_oauth_connection SET garmin_permissions=$2,garmin_permissions_observed_at=$3 WHERE subject_id=$1 AND provider='garmin'", &[&subject, &permissions, &observed_at]).await?;
    tx.execute("UPDATE watch_tower.garmin_backfill SET status='permission_removed',job_id=NULL,lease_until=NULL
        WHERE subject_id=$1 AND status='queued' AND (permission & $2::smallint)=0", &[&subject,&permissions]).await?;
    tx.execute("DELETE FROM watch_tower.garmin_pull_job WHERE subject_id=$1 AND (permission & $2::smallint)=0", &[&subject,&permissions]).await?;
    tx.execute("DELETE FROM avatar_hsm.wearable_record WHERE subject_id=$1 AND provider='garmin'
        AND CASE WHEN kind='workout' OR (kind='metric' AND payload->>'metric'='activityDetails') THEN NOT $2::boolean ELSE NOT $3::boolean END", &[&subject,&activity,&health]).await?;
    if !activity {
        tx.execute(
            "DELETE FROM watch_tower.wearable_fit_job WHERE subject_id=$1 AND provider='garmin'",
            &[&subject],
        )
        .await?;
    }
    if !health {
        tx.execute(
            "DELETE FROM avatar_hsm.wearable_daily WHERE subject_id=$1 AND provider='garmin'",
            &[&subject],
        )
        .await?;
    }
    tx.execute("UPDATE avatar_hsm.device_connection SET grants=ARRAY(
            SELECT kind FROM avatar_hsm.wearable_record WHERE subject_id=$1 AND provider='garmin'
            UNION SELECT kind FROM avatar_hsm.wearable_daily WHERE subject_id=$1 AND provider='garmin')
        WHERE subject_id=$1 AND provider='garmin'", &[&subject]).await?;
    Ok(())
}

pub async fn disconnect(pool: &Pool, subject: &str) -> Result<Value> {
    let client = cloud_wearables::http_client()?;
    let token = wearable_oauth::access(&client, pool, subject, Provider::Garmin).await?;
    let response = client
        .delete("https://apis.garmin.com/wellness-api/rest/user/registration")
        .bearer_auth(&token[0])
        .send()
        .await?;
    ensure!(
        response.status().is_success() || response.status() == StatusCode::NOT_FOUND,
        "Garmin did not confirm disconnect"
    );
    wearables::revoke_for(pool, subject, PROVIDER).await
}
