#[cfg(test)]
use crate::{
    cloud_wearables::{self, Provider},
    wearable_oauth,
};
use crate::{wearable_fit, wearable_webhooks, wearables};
#[cfg(test)]
use anyhow::ensure;
use anyhow::Result;
#[cfg(test)]
use chrono::Datelike;
use chrono::{DateTime, NaiveDate, Utc};
use deadpool_postgres::Pool;
#[cfg(test)]
use reqwest::Client;
#[cfg(test)]
use serde_json::Map;
use serde_json::{json, Value};
#[cfg(test)]
use std::{collections::BTreeSet, time::Duration};

#[cfg(test)]
#[path = "wearable_webhook_documents_tests.rs"]
mod tests;

#[cfg(test)]
const OURA_ORIGIN: &str = "https://api.ouraring.com/v2/usercollection";
#[cfg(test)]
const WHOOP_ORIGIN: &str = "https://api.prod.whoop.com/developer/v2";
#[cfg(test)]
const STRAVA_ORIGIN: &str = "https://www.strava.com/api/v3";
#[cfg(test)]
const INTERVALS_ORIGIN: &str = "https://intervals.icu/api/v1";
#[cfg(test)]
const POLAR_ORIGIN: &str = "https://www.polaraccesslink.com/v3/users";

pub struct Target<'a> {
    kind: &'a str,
    id: &'a str,
    delete: bool,
    event_at: Option<DateTime<Utc>>,
}

pub(crate) fn strava_revocation(id: &str, at: DateTime<Utc>) -> Target<'_> {
    Target {
        kind: "authorization",
        id,
        delete: true,
        event_at: Some(at),
    }
}

pub(crate) fn strava_activity<'a>(
    id: &'a str,
    action: &str,
    at: DateTime<Utc>,
) -> Option<Target<'a>> {
    if !id.parse::<i64>().is_ok_and(|id| id > 0) || !id.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    let delete = match action {
        "create" | "update" => false,
        "delete" => true,
        _ => return None,
    };
    Some(Target {
        kind: "workout",
        id,
        delete,
        event_at: Some(at),
    })
}

pub(crate) fn intervals_activity(id: &str, at: DateTime<Utc>) -> Option<Target<'_>> {
    wearable_fit::valid_id(id).then_some(Target {
        kind: "workout",
        id,
        delete: false,
        event_at: Some(at),
    })
}

pub(crate) fn polar_physical_info(at: DateTime<Utc>) -> Target<'static> {
    Target {
        kind: "physical_info",
        id: "current",
        delete: false,
        event_at: Some(at),
    }
}

pub fn target<'a>(provider: &str, event: &'a Value) -> Option<Target<'a>> {
    if provider == "whoop" {
        let (kind, action) = event["type"].as_str()?.split_once('.')?;
        let delete = match (kind, action) {
            ("sleep" | "workout" | "recovery", "updated") => false,
            ("sleep" | "workout" | "recovery", "deleted") => true,
            _ => return None,
        };
        let id = event["id"].as_str().filter(|id| whoop_uuid(id))?;
        return Some(Target {
            kind,
            id,
            delete,
            event_at: None,
        });
    }
    let delete = match event["event_type"].as_str()? {
        "create" | "update" => false,
        "delete" => true,
        _ => return None,
    };
    let kind = event["data_type"].as_str()?;
    required_scope(kind)?;
    let id = event["object_id"]
        .as_str()
        .filter(|id| wearable_fit::valid_id(id))?;
    let event_at = match event.get("event_time") {
        None => None,
        Some(value) => {
            let at = value.as_str()?.parse::<DateTime<Utc>>().ok()?;
            if at.timestamp() < 0 || at > Utc::now() + chrono::Duration::minutes(5) {
                return None;
            }
            Some(at)
        }
    };
    Some(Target {
        kind,
        id,
        delete,
        event_at,
    })
}

fn whoop_uuid(id: &str) -> bool {
    id.len() == 36
        && id.bytes().enumerate().all(|(i, byte)| {
            if matches!(i, 8 | 13 | 18 | 23) {
                byte == b'-'
            } else {
                byte.is_ascii_hexdigit()
            }
        })
}

pub(crate) const SUPPORTED: &[(&str, &str)] = &[
    ("sleep", "daily"),
    ("workout", "workout"),
    ("session", "session"),
    ("daily_activity", "daily"),
    ("daily_sleep", "daily"),
    ("daily_readiness", "daily"),
    ("daily_stress", "daily"),
    ("daily_resilience", "daily"),
    ("sleep_time", "daily"),
    ("daily_spo2", "spo2"),
    ("daily_cardiovascular_age", "heart_health"),
    ("vo2_max", "heart_health"),
    ("enhanced_tag", "tag"),
    ("rest_mode_period", "daily"),
];

fn required_scope(kind: &str) -> Option<&'static &'static str> {
    SUPPORTED
        .iter()
        .find_map(|(key, scope)| (*key == kind).then_some(scope))
}

pub async fn enqueue(
    pool: &Pool,
    provider: &str,
    owner: &str,
    Target {
        kind,
        id,
        delete,
        event_at,
    }: Target<'_>,
    body: &[u8],
) -> Result<()> {
    // Strava timestamps have second precision. An event in the second a link
    // was established cannot be attributed to that link.
    pool.get().await?.execute(
        "WITH owners AS (
             SELECT subject_id,generation FROM avatar_hsm.wearable_oauth_connection
             WHERE provider=$7 AND provider_account_hash=$1
               AND ($7<>'intervals_icu' OR intervals_activity_granted IS DISTINCT FROM FALSE)
               AND ($7<>'strava' OR $3<>'authorization' OR linked_at IS NULL
                    OR $6>date_trunc('second',linked_at))
               AND ($7 NOT IN ('intervals_icu','polar') OR linked_at IS NULL OR $6>linked_at)
         ), receipt AS (
             INSERT INTO watch_tower.wearable_webhook_receipt(provider,event_hash)
             SELECT $7,$2 WHERE EXISTS(SELECT 1 FROM owners)
             ON CONFLICT DO NOTHING RETURNING provider
         )
         INSERT INTO watch_tower.wearable_webhook_document AS job(subject_id,provider,data_type,object_id,generation,delete_requested,event_at)
         SELECT owners.subject_id,$7,$3,$4,owners.generation,$5,$6 FROM owners,receipt
         ON CONFLICT(subject_id,provider,data_type,object_id) DO UPDATE
           SET generation=excluded.generation,delete_requested=excluded.delete_requested,
               event_at=excluded.event_at,completed_at=NULL,attempts=0,next_attempt=now(),job_id=NULL,leased_until=NULL
         WHERE job.generation<>excluded.generation OR job.event_at IS NULL OR excluded.event_at>job.event_at
            OR (excluded.event_at=job.event_at AND excluded.delete_requested AND NOT job.delete_requested)",
        &[&wearable_webhooks::hash(owner.as_bytes()),&wearable_webhooks::hash(body),&kind,&id,&delete,&event_at,&provider],
    ).await?;
    Ok(())
}

pub(crate) async fn schedule_polar_physical_info(pool: &Pool, subject: &str) -> Result<()> {
    pool.get().await?.execute(
        "INSERT INTO watch_tower.wearable_webhook_document AS job(subject_id,provider,data_type,object_id,generation,delete_requested)
         SELECT subject_id,'polar','physical_info','current',generation,false FROM avatar_hsm.wearable_oauth_connection
         WHERE subject_id=$1 AND provider='polar'
         ON CONFLICT(subject_id,provider,data_type,object_id) DO UPDATE
           SET generation=excluded.generation,
               event_at=CASE WHEN job.generation=excluded.generation THEN job.event_at END,
               completed_at=NULL,attempts=0,next_attempt=now(),job_id=NULL,leased_until=NULL
         WHERE job.generation<>excluded.generation OR job.completed_at<=now()-interval '1 day'
            OR (job.attempts>=6 AND job.next_attempt<=now()-interval '1 day')",
        &[&subject],
    ).await?;
    Ok(())
}

pub(crate) async fn current_polar_physical_info(pool: &Pool, subject: &str) -> Result<Value> {
    let row = pool
        .get()
        .await?
        .query_opt(
            "SELECT c.state, r.started_at, r.payload FROM avatar_hsm.device_connection c
         JOIN avatar_hsm.wearable_oauth_connection o USING(subject_id,provider)
         LEFT JOIN avatar_hsm.wearable_record r
           ON r.subject_id=c.subject_id AND r.provider=c.provider AND r.kind='metric'
              AND r.source='Polar' AND r.source_id='physical_info:current'
              AND c.state='synced' AND 'metric'=ANY(c.grants)
         WHERE c.subject_id=$1 AND c.provider='polar'",
            &[&subject],
        )
        .await?;
    let Some(row) = row else {
        return Ok(json!({"state":"not_connected"}));
    };
    let Some(payload) = row.get::<_, Option<Value>>(2) else {
        let state: String = row.get(0);
        return Ok(json!({"state": if state=="pending" { "pending" } else { "no_records" }}));
    };
    Ok(json!({"state":"ready","at":row.get::<_,DateTime<Utc>>(1),"payload":payload}))
}

pub(crate) struct Job {
    pub(crate) subject: String,
    pub(crate) provider: String,
    pub(crate) kind: String,
    pub(crate) object: String,
    pub(crate) id: String,
    pub(crate) delete_requested: bool,
}

#[cfg(test)]
pub(crate) async fn claim(pool: &Pool) -> Result<Option<Job>> {
    let row = pool.get().await?.query_opt(
        "WITH due AS (
            SELECT subject_id,provider,data_type,object_id FROM watch_tower.wearable_webhook_document AS pending
            WHERE completed_at IS NULL AND attempts<6 AND next_attempt<=now()
              AND (leased_until IS NULL OR leased_until<=now())
              AND NOT EXISTS (
                SELECT 1 FROM watch_tower.wearable_sync_schedule AS sync
                WHERE sync.subject_id=pending.subject_id AND sync.provider=pending.provider
                  AND sync.throttled_until>now()
              )
            ORDER BY next_attempt LIMIT 1 FOR UPDATE SKIP LOCKED
         )
         UPDATE watch_tower.wearable_webhook_document AS job
         SET job_id=gen_random_uuid(),leased_until=now()+interval '2 minutes'
         FROM due WHERE (job.subject_id,job.provider,job.data_type,job.object_id)=(due.subject_id,due.provider,due.data_type,due.object_id)
         RETURNING job.subject_id,job.data_type,job.object_id,job.job_id::text,job.delete_requested,job.provider",&[],
    ).await?;
    Ok(row.map(|row| Job {
        subject: row.get(0),
        kind: row.get(1),
        object: row.get(2),
        id: row.get(3),
        delete_requested: row.get(4),
        provider: row.get(5),
    }))
}

#[cfg(test)]
fn record_days(doc: &Value, today: NaiveDate) -> Vec<NaiveDate> {
    let earliest = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
    let mut days = BTreeSet::new();
    for field in ["day", "start_day", "end_day"] {
        if let Some(day) = doc[field].as_str().and_then(|value| value.parse().ok()) {
            days.insert(day);
        }
    }
    for field in [
        "timestamp",
        "start",
        "end",
        "bedtime_start",
        "bedtime_end",
        "start_datetime",
        "end_datetime",
        "start_date",
    ] {
        if let Some(time) = doc[field]
            .as_str()
            .and_then(|value| DateTime::parse_from_rfc3339(value).ok())
        {
            days.extend([time.date_naive(), time.with_timezone(&Utc).date_naive()]);
        }
    }
    days.into_iter()
        .filter(|day| (earliest..=today).contains(day))
        .collect()
}

pub(crate) enum Document {
    Live {
        days: Vec<NaiveDate>,
        record: Option<Box<wearables::Record>>,
    },
    PhysicalInfo(wearables::Record),
    Deleted,
    NoAccess,
    Revoked,
}

#[cfg(test)]
async fn fetch_document(
    client: &Client,
    origin: &str,
    credentials: &[String],
    job: &Job,
) -> Result<Document> {
    if job.provider == "polar" && job.kind == "physical_info" {
        return fetch_polar_physical_info(client, origin, credentials).await;
    }
    ensure!(
        wearable_fit::valid_id(&job.object),
        "invalid webhook document ID"
    );
    let granted = credentials.get(1).map_or("", String::as_str);
    let scopes_granted = |scope| {
        granted
            .split(|ch: char| ch.is_ascii_whitespace() || ch == ',')
            .any(|value| value == scope)
    };
    let (path, scopes): (&str, &[&str]) = match (job.provider.as_str(), job.kind.as_str()) {
        ("strava", "workout")
            if scopes_granted("activity:read") || scopes_granted("activity:read_all") =>
        {
            ("activities", &[])
        }
        ("strava", "workout") => return Ok(Document::NoAccess),
        ("intervals_icu", "workout")
            if cloud_wearables::manufacturers::intervals_scope(granted, "ACTIVITY") =>
        {
            ("activity", &[])
        }
        ("intervals_icu", "workout") => return Ok(Document::NoAccess),
        ("whoop", "sleep") => ("activity/sleep", &["read:sleep"]),
        ("whoop", "workout") => ("activity/workout", &["read:workout"]),
        ("whoop", "recovery") => ("activity/sleep", &["read:sleep", "read:recovery"]),
        ("oura", kind) => (
            if kind == "vo2_max" { "vO2_max" } else { kind },
            std::slice::from_ref(
                required_scope(kind).ok_or_else(|| anyhow::anyhow!("invalid Oura data type"))?,
            ),
        ),
        _ => anyhow::bail!("invalid webhook data type"),
    };
    if (job.delete_requested || !granted.trim().is_empty() || job.kind == "rest_mode_period")
        && !scopes.iter().all(|scope| scopes_granted(*scope))
    {
        return Ok(Document::NoAccess);
    }
    let observed_at = Utc::now();
    let response = client
        .get(format!("{origin}/{path}/{}", job.object))
        .bearer_auth(&credentials[0])
        .send()
        .await?;
    if job.delete_requested && response.status() == reqwest::StatusCode::NOT_FOUND {
        return Ok(Document::Deleted);
    }
    if job.kind == "rest_mode_period" && response.status() == reqwest::StatusCode::FORBIDDEN {
        return Ok(Document::NoAccess);
    }
    let doc = cloud_wearables::body(response).await?;
    ensure!(
        wearable_webhooks::account_id(&doc["id"]).as_deref() == Some(job.object.as_str()),
        "webhook document ID mismatch"
    );
    if job.provider == "oura" && job.kind == "sleep" && doc["type"] == "deleted" {
        return Ok(if scopes_granted("daily") {
            Document::Deleted
        } else {
            Document::NoAccess
        });
    }
    let today = Utc::now().date_naive();
    let mut days = record_days(&doc, today);
    if job.provider == "intervals_icu" {
        let owner = wearable_webhooks::account_id(&doc["icu_athlete_id"]);
        if owner.is_none() || owner.as_ref() != credentials.get(2) {
            return Ok(Document::NoAccess);
        }
        if let Some(day) = doc["start_date_local"]
            .as_str()
            .and_then(|local| local.get(..10))
            .and_then(|date| date.parse::<NaiveDate>().ok())
            .filter(|day| day.year() >= 1970 && *day <= today)
        {
            days.push(day);
            days.sort_unstable();
            days.dedup();
        }
    }
    if job.provider == "whoop" && job.kind == "recovery" {
        let recovery =
            fetch_whoop_recovery(client, origin, &credentials[0], &job.object, &doc).await?;
        if job.delete_requested && recovery.is_none() {
            return Ok(Document::Deleted);
        }
        if !job.delete_requested {
            let recovery =
                recovery.ok_or_else(|| anyhow::anyhow!("WHOOP recovery has not appeared"))?;
            days.extend(record_days(
                &serde_json::json!({"timestamp":recovery["created_at"]}),
                today,
            ));
            days.sort_unstable();
            days.dedup();
        }
    }
    ensure!(
        !job.delete_requested,
        "provider still serves the deleted document"
    );
    ensure!(!days.is_empty(), "webhook document date unavailable");
    let scoped_oura_summary = job.provider == "oura"
        && matches!(
            job.kind.as_str(),
            "daily_activity"
                | "daily_sleep"
                | "daily_readiness"
                | "daily_stress"
                | "daily_resilience"
                | "daily_spo2"
                | "daily_cardiovascular_age"
        )
        && scopes.iter().all(|scope| scopes_granted(*scope));
    let record = if job.provider == "oura"
        && matches!(job.kind.as_str(), "sleep" | "workout")
        && scopes.iter().all(|scope| scopes_granted(*scope))
    {
        Some(Box::new(
            cloud_wearables::manufacturers::oura_webhook_session(&doc, &job.kind, observed_at)?,
        ))
    } else if scoped_oura_summary {
        let at = cloud_wearables::manufacturers::oura_daily_timestamp(&doc)?;
        ensure!(
            at >= DateTime::from_timestamp(0, 0).unwrap()
                && at <= observed_at + chrono::Duration::minutes(5),
            "invalid Oura summary timestamp"
        );
        Some(Box::new(wearables::Record {
            kind: "metric".into(),
            source: "Oura".into(),
            source_id: format!("{}:{}", job.kind, job.object),
            started_at: at,
            ended_at: at,
            modified_at: observed_at,
            payload: json!({"metric":job.kind,"provider_data":doc}),
        }))
    } else {
        None
    };
    Ok(Document::Live { days, record })
}

#[cfg(test)]
async fn fetch_polar_physical_info(
    client: &Client,
    origin: &str,
    credentials: &[String],
) -> Result<Document> {
    let response = client
        .get(format!("{origin}/physical-info"))
        .header(reqwest::header::ACCEPT, "application/json")
        .bearer_auth(&credentials[0])
        .send()
        .await?;
    if matches!(
        response.status(),
        reqwest::StatusCode::FORBIDDEN
            | reqwest::StatusCode::NOT_FOUND
            | reqwest::StatusCode::NO_CONTENT
    ) {
        return Ok(Document::NoAccess);
    }
    let raw = cloud_wearables::body(response).await?;
    let now = Utc::now();
    let mut fields = Map::new();
    for (field, target) in [
        ("weight", "weight_kg"),
        ("height", "height_cm"),
        ("maximum_heart_rate", "maximum_heart_rate_bpm"),
        ("resting_heart_rate", "resting_heart_rate_bpm"),
        ("aerobic_threshold", "aerobic_threshold_bpm"),
        ("anaerobic_threshold", "anaerobic_threshold_bpm"),
        ("vo2_max", "vo2_max"),
    ] {
        if let Some(value) = raw[field].as_f64().filter(|value| *value > 0.0) {
            fields.insert(target.into(), json!(value));
        }
    }
    for field in [
        "weight_source",
        "training_background",
        "typical_day",
        "sleep_goal",
    ] {
        if let Some(value) = raw[field]
            .as_str()
            .filter(|value| !value.is_empty() && value.len() <= 128)
        {
            fields.insert(field.into(), json!(value));
        }
    }
    if fields.is_empty() {
        return Ok(Document::NoAccess);
    }
    let to_date = |field: &str| {
        raw[field]
            .as_str()
            .and_then(|value| value.parse::<DateTime<Utc>>().ok())
            .filter(|value| value.timestamp() >= 0 && *value <= now + chrono::Duration::minutes(5))
    };
    let created = to_date("created");
    let modified = to_date("modified");
    let (at, basis) = match (created, modified) {
        (Some(created), Some(modified)) if created > modified => (created, "created"),
        (_, Some(modified)) => (modified, "modified"),
        (Some(created), None) => (created, "created"),
        _ => (now, "retrieved"),
    };
    if let Some(created) = created {
        fields.insert("created_at".into(), json!(created));
    }
    if let Some(modified) = modified {
        fields.insert("modified_at".into(), json!(modified));
    }
    Ok(Document::PhysicalInfo(wearables::Record {
        kind: "metric".into(),
        source: "Polar".into(),
        source_id: "physical_info:current".into(),
        started_at: at,
        ended_at: at,
        modified_at: at,
        payload: json!({"metric":"physical_info","timestamp_basis":basis,"provider_data":fields}),
    }))
}

#[cfg(test)]
async fn fetch_whoop_recovery(
    client: &Client,
    origin: &str,
    token: &str,
    sleep_id: &str,
    sleep: &Value,
) -> Result<Option<Value>> {
    let cycle = sleep["cycle_id"]
        .as_u64()
        .filter(|cycle| *cycle > 0)
        .ok_or_else(|| anyhow::anyhow!("WHOOP sleep cycle ID unavailable"))?;
    let response = client
        .get(format!("{origin}/cycle/{cycle}/recovery"))
        .bearer_auth(token)
        .send()
        .await?;
    if response.status() == reqwest::StatusCode::NOT_FOUND {
        return Ok(None);
    }
    let recovery = cloud_wearables::body(response).await?;
    ensure!(
        recovery["cycle_id"].as_u64() == Some(cycle),
        "WHOOP recovery cycle does not match"
    );
    let linked = recovery["sleep_id"]
        .as_str()
        .filter(|id| whoop_uuid(id))
        .ok_or_else(|| anyhow::anyhow!("WHOOP recovery sleep ID unavailable"))?;
    Ok((linked == sleep_id).then_some(recovery))
}

pub(crate) async fn finish_owner(pool: &Pool, job: &Job, document: Document) -> Result<bool> {
    let mut db = pool.get().await?;
    let tx = db.transaction().await?;
    if matches!(document, Document::Revoked) {
        // Serialize with user unlink and OAuth callback before taking the device row lock.
        tx.query_one(
            "SELECT pg_advisory_xact_lock(hashtext($1),hashtext($2))",
            &[&job.subject, &job.provider],
        )
        .await?;
    }
    // This is also the lock taken by bulk snapshot persistence and unlink.
    let connection = tx
        .query_opt(
            "SELECT 1 FROM avatar_hsm.device_connection WHERE subject_id=$1 AND provider=$2
         AND state<>'revoked' FOR UPDATE",
            &[&job.subject, &job.provider],
        )
        .await?;
    let current_job = tx
        .query_opt(
            "SELECT (c.generation=jobs.generation) AS generation_current
         FROM watch_tower.wearable_webhook_document AS jobs
         LEFT JOIN avatar_hsm.wearable_oauth_connection c
           ON c.subject_id=jobs.subject_id AND c.provider=jobs.provider
         WHERE jobs.subject_id=$1 AND jobs.data_type=$2 AND jobs.object_id=$3 AND jobs.provider=$5
           AND jobs.job_id=$4::text::uuid AND jobs.leased_until>now()
         FOR UPDATE OF jobs",
            &[&job.subject, &job.kind, &job.object, &job.id, &job.provider],
        )
        .await?;
    let Some(current_job) = current_job else {
        return Ok(false);
    };
    if connection.is_none() || !current_job.get::<_, Option<bool>>(0).unwrap_or(false) {
        tx.commit().await?;
        return Ok(true);
    }
    if matches!(document, Document::Revoked) {
        crate::wearables::revoke_locked(&tx, &job.subject, &job.provider).await?;
        tx.commit().await?;
        return Ok(true);
    }
    if let Document::PhysicalInfo(record) = &document {
        wearables::upsert_record(&tx, &job.subject, &job.provider, record).await?;
        tx.execute(
            "UPDATE avatar_hsm.device_connection SET state='synced',last_synced_at=now(),
               grants=CASE WHEN 'metric'=ANY(grants) THEN grants ELSE array_append(grants,'metric') END
             WHERE subject_id=$1 AND provider='polar'",
            &[&job.subject],
        ).await?;
        tx.commit().await?;
        return Ok(true);
    }
    let (kind, source_id) = match job.kind.as_str() {
        "sleep" => ("sleep", job.object.clone()),
        "workout" => ("workout", job.object.clone()),
        "recovery" if job.provider == "whoop" => ("recovery_sleep", job.object.clone()),
        "session" => ("metric", format!("guided_session:{}", job.object)),
        data_type => ("metric", format!("{data_type}:{}", job.object)),
    };
    let source = match job.provider.as_str() {
        "oura" => "Oura",
        "whoop" => "WHOOP",
        "strava" => "Strava",
        "intervals_icu" => "Intervals.icu",
        "polar" => "Polar",
        _ => anyhow::bail!("invalid webhook provider"),
    };
    match document {
        Document::Live { days: _, record } => {
            tx.execute(
                "DELETE FROM avatar_hsm.wearable_deleted_document
                 WHERE subject_id=$1 AND kind=$2 AND source_id=$3 AND provider=$4",
                &[&job.subject, &kind, &source_id, &job.provider],
            )
            .await?;
            if let Some(record) = record {
                wearables::upsert_record(&tx, &job.subject, &job.provider, &record).await?;
                tx.execute(
                    "UPDATE avatar_hsm.device_connection SET state='synced',last_synced_at=now(),
                       grants=CASE WHEN $3::text=ANY(grants) THEN grants ELSE array_append(grants,$3::text) END
                     WHERE subject_id=$1 AND provider=$2",
                    &[&job.subject,&job.provider,&record.kind],
                ).await?;
            }
        }
        Document::Deleted => {
            tx.execute(
                "INSERT INTO avatar_hsm.wearable_deleted_document(subject_id,kind,source_id,provider)
                 VALUES($1,$2,$3,$4) ON CONFLICT DO NOTHING",
                &[&job.subject, &kind, &source_id,&job.provider],
            )
            .await?;
            tx.execute(
                "DELETE FROM avatar_hsm.wearable_record WHERE subject_id=$1 AND provider=$4 AND source=$5
                 AND ((kind=$2 AND source_id=$3) OR
                    ($4='whoop' AND $2='recovery_sleep' AND kind='metric' AND payload->>'metric'='recovery'
                      AND payload#>>'{provider_data,sleep_id}'=$3))",
                &[&job.subject, &kind, &source_id, &job.provider, &source],
            )
            .await?;
        }
        Document::NoAccess => {}
        Document::Revoked => unreachable!("handled before document reconciliation"),
        Document::PhysicalInfo(_) => unreachable!("handled before document reconciliation"),
    }
    tx.commit().await?;
    Ok(true)
}

#[cfg(test)]
async fn finish(pool: &Pool, job: &Job, document: Document) -> Result<()> {
    let replay_days = match &document {
        Document::Live { days, .. } => Some(days.clone()),
        _ => None,
    };
    if !finish_owner(pool, job, document).await? {
        return Ok(());
    }
    let db = pool.get().await?;
    db.execute(
        "UPDATE watch_tower.wearable_webhook_document SET completed_at=now(),job_id=NULL,leased_until=NULL
         WHERE subject_id=$1 AND data_type=$2 AND object_id=$3 AND provider=$4 AND job_id=$5::text::uuid",
        &[&job.subject,&job.kind,&job.object,&job.provider,&job.id],
    ).await?;
    if let Some(days) = replay_days {
        db.execute(
        "WITH scheduled AS (UPDATE watch_tower.wearable_sync_schedule SET next_sync=LEAST(next_sync,now())
         WHERE subject_id=$1 AND provider=$3 RETURNING subject_id,provider)
         INSERT INTO watch_tower.wearable_target_day(subject_id,provider,day)
         SELECT scheduled.subject_id,scheduled.provider,dates.day FROM scheduled
         CROSS JOIN unnest($2::date[]) AS dates(day)
         ON CONFLICT(subject_id,provider,day) DO UPDATE SET revision=excluded.revision,next_attempt=excluded.next_attempt,failures=0",
        &[&job.subject,&days,&job.provider],
    ).await?;
    }
    Ok(())
}

#[cfg(test)]
pub(crate) async fn retry_at(
    pool: &Pool,
    job: &Job,
    retry_at: Option<DateTime<Utc>>,
) -> Result<()> {
    pool.get()
        .await?
        .execute(
            "WITH connection AS (
                 SELECT 1 FROM avatar_hsm.device_connection
                 WHERE $6::timestamptz IS NOT NULL AND subject_id=$1 AND provider=$5
                 FOR UPDATE
             ), deferred AS (
                 UPDATE watch_tower.wearable_webhook_document
                 SET attempts=attempts+CASE WHEN $6::timestamptz IS NULL THEN 1 ELSE 0 END,
                     next_attempt=COALESCE($6,now()+make_interval(mins=>LEAST(120,5*(attempts+1)))),
                     job_id=NULL,leased_until=NULL
                 WHERE subject_id=$1 AND data_type=$2 AND object_id=$3
                   AND job_id=$4::text::uuid AND provider=$5
                   AND ($6 IS NULL OR EXISTS(SELECT 1 FROM connection))
                 RETURNING subject_id,provider
             )
             UPDATE watch_tower.wearable_sync_schedule AS sync
             SET throttled_until=GREATEST(sync.throttled_until,$6)
             FROM deferred WHERE $6 IS NOT NULL
               AND sync.subject_id=deferred.subject_id AND sync.provider=deferred.provider",
            &[
                &job.subject,
                &job.kind,
                &job.object,
                &job.id,
                &job.provider,
                &retry_at,
            ],
        )
        .await?;
    Ok(())
}

#[cfg(test)]
async fn retry(pool: &Pool, job: &Job, error: &anyhow::Error) -> Result<()> {
    let retry_time = error
        .downcast_ref::<cloud_wearables::ProviderThrottled>()
        .map(|throttle| throttle.retry_at);
    retry_at(pool, job, retry_time).await
}

#[cfg(test)]
async fn run(pool: &Pool, client: &Client, job: &Job) -> Result<()> {
    if job.provider == "strava" && job.kind == "authorization" {
        return finish(pool, job, Document::Revoked).await;
    }
    let provider = Provider::parse(&job.provider)?;
    let origin = match provider {
        Provider::Oura => OURA_ORIGIN,
        Provider::Whoop => WHOOP_ORIGIN,
        Provider::Strava => STRAVA_ORIGIN,
        Provider::IntervalsIcu => INTERVALS_ORIGIN,
        Provider::Polar => POLAR_ORIGIN,
        _ => anyhow::bail!("invalid webhook provider"),
    };
    let credentials = wearable_oauth::access(client, pool, &job.subject, provider).await?;
    let document = fetch_document(client, origin, &credentials, job).await?;
    finish(pool, job, document).await
}

#[cfg(test)]
pub async fn worker(pool: Pool, client: Client) {
    loop {
        if let Ok(Some(job)) = claim(&pool).await {
            if let Err(error) = run(&pool, &client, &job).await {
                let retry_time = error
                    .downcast_ref::<cloud_wearables::ProviderThrottled>()
                    .map(|value| value.retry_at);
                let _ = retry_at(&pool, &job, retry_time).await;
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
        } else {
            tokio::time::sleep(Duration::from_secs(12)).await;
        }
    }
}
