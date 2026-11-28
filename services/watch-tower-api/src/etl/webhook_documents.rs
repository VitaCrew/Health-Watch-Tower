use anyhow::{ensure, Context, Result};
use avatar_hsm::etl_adapter::{
    self, NormalizedWebhookRecord, WebhookDocumentClaim, WebhookDocumentLease,
    WebhookDocumentResult,
};
use chrono::{DateTime, Datelike, NaiveDate, Utc};
use deadpool_postgres::Pool;
use reqwest::{Client, Response, StatusCode};
use serde_json::{json, Map, Value};
use std::{collections::BTreeSet, time::Duration};

const MAX_DOCUMENT_BYTES: usize = 8 * 1024 * 1024;

#[derive(Debug)]
struct ProviderRateLimited {
    retry_at: DateTime<Utc>,
}

impl std::fmt::Display for ProviderRateLimited {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("wearable provider rate limited the webhook fetch")
    }
}

impl std::error::Error for ProviderRateLimited {}

pub async fn worker(pool: Pool, client: Client) {
    loop {
        match claim(&pool).await {
            Ok(Some(claim)) => {
                let mut lease = etl_adapter::lease_webhook_document(claim);
                let ack = etl_adapter::webhook_document_ack(&lease);
                let result = tokio::time::timeout(Duration::from_secs(90), async {
                    etl_adapter::authorize_webhook_document(&pool, &client, &mut lease).await?;
                    fetch_document(&client, &lease).await
                })
                .await;
                match result {
                    Ok(Ok(document)) => {
                        match etl_adapter::finish_webhook_document(&pool, &lease, document).await {
                            Ok(commit) if commit.lease_valid => {
                                if acknowledge(&pool, &ack, &commit.replay_days).await.is_err() {
                                    let _ = defer(&pool, &ack, None).await;
                                }
                            }
                            Ok(_) => {}
                            Err(error) => {
                                let retry_at = provider_retry_at(&error);
                                let _ = defer(&pool, &ack, retry_at).await;
                            }
                        }
                    }
                    Ok(Err(error)) => {
                        let retry_at = provider_retry_at(&error);
                        let _ = defer(&pool, &ack, retry_at).await;
                    }
                    Err(_) => {
                        let _ = defer(&pool, &ack, None).await;
                    }
                }
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
            Ok(None) | Err(_) => tokio::time::sleep(Duration::from_secs(12)).await,
        }
    }
}

async fn claim(pool: &Pool) -> Result<Option<WebhookDocumentClaim>> {
    let row = pool
        .get()
        .await?
        .query_opt(
            "WITH due AS (
            SELECT subject_id,provider,data_type,object_id
            FROM watch_tower.wearable_webhook_document AS pending
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
         FROM due
         WHERE (job.subject_id,job.provider,job.data_type,job.object_id)=
               (due.subject_id,due.provider,due.data_type,due.object_id)
         RETURNING job.subject_id,job.provider,job.data_type,job.object_id,
                   job.job_id::text,job.delete_requested",
            &[],
        )
        .await?;
    Ok(row.map(|row| WebhookDocumentClaim {
        subject_id: row.get(0),
        provider: row.get(1),
        data_type: row.get(2),
        object_id: row.get(3),
        job_id: row.get(4),
        delete_requested: row.get(5),
    }))
}

fn provider_retry_at(error: &anyhow::Error) -> Option<DateTime<Utc>> {
    error
        .downcast_ref::<ProviderRateLimited>()
        .map(|error| error.retry_at)
        .or_else(|| etl_adapter::provider_retry_at(error))
}

async fn acknowledge(
    pool: &Pool,
    ack: &etl_adapter::WebhookDocumentAck,
    replay_days: &[NaiveDate],
) -> Result<()> {
    let mut db = pool.get().await?;
    let tx = db.transaction().await?;
    let changed = tx
        .execute(
            "UPDATE watch_tower.wearable_webhook_document
         SET completed_at=now(),job_id=NULL,leased_until=NULL
         WHERE subject_id=$1 AND provider=$2 AND data_type=$3 AND object_id=$4
           AND job_id=$5::text::uuid AND leased_until>now() AND completed_at IS NULL",
            &[
                &ack.subject_id,
                &ack.provider,
                &ack.data_type,
                &ack.object_id,
                &ack.job_id,
            ],
        )
        .await?;
    if changed == 1 && !replay_days.is_empty() {
        tx.execute(
            "WITH scheduled AS (
               UPDATE watch_tower.wearable_sync_schedule SET next_sync=LEAST(next_sync,now())
               WHERE subject_id=$1 AND provider=$3 RETURNING subject_id,provider
             )
             INSERT INTO watch_tower.wearable_target_day(subject_id,provider,day)
             SELECT scheduled.subject_id,scheduled.provider,dates.day
             FROM scheduled CROSS JOIN unnest($2::date[]) AS dates(day)
             ON CONFLICT(subject_id,provider,day) DO UPDATE
                 SET revision=excluded.revision,next_attempt=excluded.next_attempt,failures=0",
            &[&ack.subject_id, &replay_days, &ack.provider],
        )
        .await?;
    }
    tx.commit().await?;
    Ok(())
}

async fn defer(
    pool: &Pool,
    ack: &etl_adapter::WebhookDocumentAck,
    retry_at: Option<DateTime<Utc>>,
) -> Result<()> {
    pool.get()
        .await?
        .execute(
            "WITH connection AS (
             SELECT 1 FROM avatar_hsm.device_connection
             WHERE $6::timestamptz IS NOT NULL AND subject_id=$1 AND provider=$2
             FOR UPDATE
         ), deferred AS (
             UPDATE watch_tower.wearable_webhook_document
             SET attempts=attempts+CASE WHEN $6::timestamptz IS NULL THEN 1 ELSE 0 END,
                 next_attempt=COALESCE($6,now()+make_interval(mins=>LEAST(120,5*(attempts+1)))),
                 job_id=NULL,leased_until=NULL
             WHERE subject_id=$1 AND provider=$2 AND data_type=$3 AND object_id=$4
               AND job_id=$5::text::uuid
               AND ($6 IS NULL OR EXISTS(SELECT 1 FROM connection))
             RETURNING subject_id,provider
         )
         UPDATE watch_tower.wearable_sync_schedule AS sync
         SET throttled_until=GREATEST(sync.throttled_until,$6)
         FROM deferred WHERE $6 IS NOT NULL
           AND sync.subject_id=deferred.subject_id AND sync.provider=deferred.provider",
            &[
                &ack.subject_id,
                &ack.provider,
                &ack.data_type,
                &ack.object_id,
                &ack.job_id,
                &retry_at,
            ],
        )
        .await?;
    Ok(())
}

async fn fetch_document(
    client: &Client,
    lease: &WebhookDocumentLease,
) -> Result<WebhookDocumentResult> {
    if lease.provider == "strava" && lease.kind == "authorization" {
        return Ok(WebhookDocumentResult::Revoked);
    }
    let credentials = &lease.credentials;
    if lease.provider == "polar" && lease.kind == "physical_info" {
        return fetch_polar_physical_info(client, credentials).await;
    }
    ensure!(valid_id(&lease.object_id), "invalid webhook document ID");
    let granted = credentials.get(1).map_or("", String::as_str);
    let has_scope = |scope: &str| {
        granted
            .split(|ch: char| ch.is_ascii_whitespace() || ch == ',')
            .any(|value| value == scope)
    };
    let (origin, path, scopes): (&str, &str, &[&str]) =
        match (lease.provider.as_str(), lease.kind.as_str()) {
            ("strava", "workout")
                if has_scope("activity:read") || has_scope("activity:read_all") =>
            {
                ("https://www.strava.com/api/v3", "activities", &[])
            }
            ("strava", "workout") => return Ok(WebhookDocumentResult::NoAccess),
            ("intervals_icu", "workout") if intervals_scope(granted) => {
                ("https://intervals.icu/api/v1", "activity", &[])
            }
            ("intervals_icu", "workout") => return Ok(WebhookDocumentResult::NoAccess),
            ("whoop", "sleep") => (
                "https://api.prod.whoop.com/developer/v2",
                "activity/sleep",
                &["read:sleep"],
            ),
            ("whoop", "workout") => (
                "https://api.prod.whoop.com/developer/v2",
                "activity/workout",
                &["read:workout"],
            ),
            ("whoop", "recovery") => (
                "https://api.prod.whoop.com/developer/v2",
                "activity/sleep",
                &["read:sleep", "read:recovery"],
            ),
            ("oura", kind) => (
                "https://api.ouraring.com/v2/usercollection",
                if kind == "vo2_max" { "vO2_max" } else { kind },
                &[oura_scope(kind)?],
            ),
            _ => anyhow::bail!("invalid webhook provider or data type"),
        };
    if (lease.delete_requested || !granted.trim().is_empty() || lease.kind == "rest_mode_period")
        && !scopes.iter().all(|scope| has_scope(scope))
    {
        return Ok(WebhookDocumentResult::NoAccess);
    }
    let observed_at = Utc::now();
    let response = client
        .get(format!("{origin}/{path}/{}", lease.object_id))
        .bearer_auth(&credentials[0])
        .send()
        .await
        .map_err(reqwest::Error::without_url)?;
    if lease.delete_requested && response.status() == StatusCode::NOT_FOUND {
        return Ok(WebhookDocumentResult::Deleted);
    }
    if lease.kind == "rest_mode_period" && response.status() == StatusCode::FORBIDDEN {
        return Ok(WebhookDocumentResult::NoAccess);
    }
    let doc = json_body(response).await?;
    ensure!(
        account_id(&doc["id"]).as_deref() == Some(lease.object_id.as_str()),
        "webhook document ID mismatch"
    );
    if lease.provider == "oura" && lease.kind == "sleep" && doc["type"] == "deleted" {
        return Ok(if has_scope("daily") {
            WebhookDocumentResult::Deleted
        } else {
            WebhookDocumentResult::NoAccess
        });
    }
    let today = Utc::now().date_naive();
    let mut days = record_days(&doc, today);
    if lease.provider == "intervals_icu" {
        if account_id(&doc["icu_athlete_id"]).as_ref() != credentials.get(2) {
            return Ok(WebhookDocumentResult::NoAccess);
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
    if lease.provider == "whoop" && lease.kind == "recovery" {
        let Some(recovery) =
            fetch_whoop_recovery(client, &credentials[0], &lease.object_id, &doc).await?
        else {
            return Ok(if lease.delete_requested {
                WebhookDocumentResult::Deleted
            } else {
                anyhow::bail!("WHOOP recovery has not appeared")
            });
        };
        if !lease.delete_requested {
            days.extend(record_days(
                &json!({"timestamp":recovery["created_at"]}),
                today,
            ));
            days.sort_unstable();
            days.dedup();
        }
    }
    ensure!(
        !lease.delete_requested,
        "provider still serves the deleted document"
    );
    ensure!(!days.is_empty(), "webhook document date unavailable");
    let scoped_oura_summary = lease.provider == "oura"
        && matches!(
            lease.kind.as_str(),
            "daily_activity"
                | "daily_sleep"
                | "daily_readiness"
                | "daily_stress"
                | "daily_resilience"
                | "daily_spo2"
                | "daily_cardiovascular_age"
        )
        && scopes.iter().all(|scope| has_scope(scope));
    let record = if lease.provider == "oura"
        && matches!(lease.kind.as_str(), "sleep" | "workout")
        && scopes.iter().all(|scope| has_scope(scope))
    {
        Some(oura_session(&doc, &lease.kind, observed_at)?)
    } else if scoped_oura_summary {
        let at = timestamp(&doc["timestamp"]).or_else(|_| timestamp(&doc["day"]))?;
        ensure!(
            at >= DateTime::from_timestamp(0, 0).unwrap()
                && at <= observed_at + chrono::Duration::minutes(5),
            "invalid Oura summary timestamp"
        );
        Some(NormalizedWebhookRecord {
            kind: "metric".into(),
            source: "Oura".into(),
            source_id: format!("{}:{}", lease.kind, lease.object_id),
            started_at: at,
            ended_at: at,
            modified_at: observed_at,
            payload: json!({"metric":lease.kind,"provider_data":doc}),
        })
    } else {
        None
    };
    Ok(WebhookDocumentResult::Live { days, record })
}

async fn fetch_polar_physical_info(
    client: &Client,
    credentials: &[String],
) -> Result<WebhookDocumentResult> {
    let response = client
        .get("https://www.polaraccesslink.com/v3/users/physical-info")
        .header(reqwest::header::ACCEPT, "application/json")
        .bearer_auth(&credentials[0])
        .send()
        .await
        .map_err(reqwest::Error::without_url)?;
    if matches!(
        response.status(),
        StatusCode::FORBIDDEN | StatusCode::NOT_FOUND | StatusCode::NO_CONTENT
    ) {
        return Ok(WebhookDocumentResult::NoAccess);
    }
    let raw = json_body(response).await?;
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
        return Ok(WebhookDocumentResult::NoAccess);
    }
    let date = |field: &str| {
        raw[field]
            .as_str()
            .and_then(|value| value.parse::<DateTime<Utc>>().ok())
            .filter(|value| value.timestamp() >= 0 && *value <= now + chrono::Duration::minutes(5))
    };
    let created = date("created");
    let modified = date("modified");
    let (at, basis) = match (created, modified) {
        (Some(a), Some(b)) if a > b => (a, "created"),
        (_, Some(b)) => (b, "modified"),
        (Some(a), None) => (a, "created"),
        _ => (now, "retrieved"),
    };
    if let Some(value) = created {
        fields.insert("created_at".into(), json!(value));
    }
    if let Some(value) = modified {
        fields.insert("modified_at".into(), json!(value));
    }
    Ok(WebhookDocumentResult::PhysicalInfo(
        NormalizedWebhookRecord {
            kind: "metric".into(),
            source: "Polar".into(),
            source_id: "physical_info:current".into(),
            started_at: at,
            ended_at: at,
            modified_at: at,
            payload: json!({"metric":"physical_info","timestamp_basis":basis,"provider_data":fields}),
        },
    ))
}

async fn fetch_whoop_recovery(
    client: &Client,
    token: &str,
    sleep_id: &str,
    sleep: &Value,
) -> Result<Option<Value>> {
    let cycle = sleep["cycle_id"]
        .as_u64()
        .filter(|id| *id > 0)
        .context("WHOOP sleep cycle ID unavailable")?;
    let response = client
        .get(format!(
            "https://api.prod.whoop.com/developer/v2/cycle/{cycle}/recovery"
        ))
        .bearer_auth(token)
        .send()
        .await
        .map_err(reqwest::Error::without_url)?;
    if response.status() == StatusCode::NOT_FOUND {
        return Ok(None);
    }
    let recovery = json_body(response).await?;
    ensure!(
        recovery["cycle_id"].as_u64() == Some(cycle),
        "WHOOP recovery cycle does not match"
    );
    let linked = recovery["sleep_id"]
        .as_str()
        .filter(|id| whoop_uuid(id))
        .context("WHOOP recovery sleep ID unavailable")?;
    Ok((linked == sleep_id).then_some(recovery))
}

async fn json_body(response: Response) -> Result<Value> {
    if response.status() == StatusCode::TOO_MANY_REQUESTS {
        let at = retry_after(
            response
                .headers()
                .get(reqwest::header::RETRY_AFTER)
                .and_then(|value| value.to_str().ok()),
            Utc::now(),
        );
        return Err(ProviderRateLimited { retry_at: at }.into());
    }
    ensure!(
        response.status().is_success(),
        "wearable provider returned HTTP {}",
        response.status().as_u16()
    );
    let mut response = response;
    let mut bytes = Vec::new();
    while let Some(chunk) = response
        .chunk()
        .await
        .map_err(reqwest::Error::without_url)?
    {
        ensure!(
            bytes.len() + chunk.len() <= MAX_DOCUMENT_BYTES,
            "webhook document exceeds response limit"
        );
        bytes.extend_from_slice(&chunk);
    }
    serde_json::from_slice(&bytes).context("invalid wearable webhook document JSON")
}

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

fn oura_scope(kind: &str) -> Result<&'static str> {
    let scope = match kind {
        "sleep" | "daily_activity" | "daily_sleep" | "daily_readiness" | "daily_stress"
        | "daily_resilience" | "sleep_time" | "rest_mode_period" => "daily",
        "workout" => "workout",
        "session" => "session",
        "daily_spo2" => "spo2",
        "daily_cardiovascular_age" | "vo2_max" => "heart_health",
        "enhanced_tag" => "tag",
        _ => anyhow::bail!("invalid Oura data type"),
    };
    Ok(scope)
}
fn intervals_scope(scopes: &str) -> bool {
    scopes.split(',').map(str::trim).any(|entry| {
        entry
            .split_once(':')
            .is_some_and(|(name, access)| name == "ACTIVITY" && matches!(access, "READ" | "WRITE"))
    })
}
fn valid_id(id: &str) -> bool {
    !id.is_empty()
        && id.len() <= 128
        && id
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"-_".contains(&b))
}
fn account_id(value: &Value) -> Option<String> {
    value
        .as_str()
        .filter(|id| !id.is_empty() && id.len() <= 256)
        .map(str::to_owned)
        .or_else(|| value.as_u64().map(|id| id.to_string()))
}
fn whoop_uuid(id: &str) -> bool {
    id.len() == 36
        && id.bytes().enumerate().all(|(i, b)| {
            if matches!(i, 8 | 13 | 18 | 23) {
                b == b'-'
            } else {
                b.is_ascii_hexdigit()
            }
        })
}
fn timestamp(value: &Value) -> Result<DateTime<Utc>> {
    if let Some(value) = value.as_str() {
        if let Ok(time) = value.parse() {
            return Ok(time);
        }
    }
    if let Some(day) = value.as_str().and_then(|s| s.parse::<NaiveDate>().ok()) {
        return Ok(day.and_hms_opt(0, 0, 0).unwrap().and_utc());
    }
    anyhow::bail!("Oura summary date unavailable")
}
fn oura_session(doc: &Value, kind: &str, at: DateTime<Utc>) -> Result<NormalizedWebhookRecord> {
    let (start, end, payload) = if kind == "sleep" {
        let start = timestamp(&doc["bedtime_start"])?;
        let end = timestamp(&doc["bedtime_end"])?;
        let (resolution, stages) = oura_stages(doc, start, end);
        (
            start,
            end,
            json!({
                "stages": stages,
                "stage_resolution_seconds": resolution,
                "provider_total_sleep_seconds": doc["total_sleep_duration"],
                "provider_data": doc
            }),
        )
    } else {
        let start = timestamp(&doc["start_datetime"])?;
        let end = timestamp(&doc["end_datetime"])?;
        let duration = (end - start).num_seconds();
        ensure!(duration >= 0, "invalid Oura workout duration");
        let exercise_type = doc["activity"]
            .as_str()
            .filter(|value| !value.trim().is_empty())
            .context("Oura workout type unavailable")?;
        (
            start,
            end,
            json!({
                "exercise_type": exercise_type,
                "title": doc["label"],
                "duration_seconds": duration,
                "distance_meters": doc["distance"],
                "calories_kcal": doc["calories"],
                "provider_data": doc
            }),
        )
    };
    ensure!(
        start < end
            && start >= DateTime::from_timestamp(0, 0).unwrap()
            && end <= at + chrono::Duration::minutes(5),
        "invalid Oura session interval"
    );
    Ok(NormalizedWebhookRecord {
        kind: kind.into(),
        source: "Oura".into(),
        source_id: doc["id"]
            .as_str()
            .context("Oura session ID unavailable")?
            .to_owned(),
        started_at: start,
        ended_at: end,
        modified_at: at,
        payload,
    })
}

fn oura_stages(doc: &Value, start: DateTime<Utc>, end: DateTime<Utc>) -> (Option<i64>, Vec<Value>) {
    for (field, resolution) in [("sleep_phase_30_sec", 30), ("sleep_phase_5_min", 300)] {
        let Some(phases) = doc[field].as_str() else {
            continue;
        };
        let mut at = start;
        let mut intervals: Vec<(i32, DateTime<Utc>, DateTime<Utc>)> = Vec::new();
        for phase in phases.bytes() {
            if at >= end || intervals.len() > 500 {
                break;
            }
            let until = (at + chrono::Duration::seconds(resolution)).min(end);
            let stage = match phase {
                b'1' => Some(5),
                b'2' => Some(4),
                b'3' => Some(6),
                b'4' => Some(1),
                _ => None,
            };
            if let Some(stage) = stage {
                match intervals.last_mut() {
                    Some((previous, _, previous_end))
                        if *previous == stage && *previous_end == at =>
                    {
                        *previous_end = until;
                    }
                    _ => intervals.push((stage, at, until)),
                }
            }
            at = until;
        }
        if (1..=500).contains(&intervals.len()) {
            return (
                Some(resolution),
                intervals
                    .into_iter()
                    .map(|(stage, start, end)| json!({"stage":stage,"start":start,"end":end}))
                    .collect(),
            );
        }
    }
    (None, Vec::new())
}

fn retry_after(header: Option<&str>, now: DateTime<Utc>) -> DateTime<Utc> {
    header
        .and_then(|value| {
            let value = value.trim();
            value
                .parse::<i64>()
                .ok()
                .and_then(chrono::Duration::try_seconds)
                .and_then(|delay| now.checked_add_signed(delay))
                .or_else(|| {
                    DateTime::parse_from_rfc2822(value)
                        .ok()
                        .map(|at| at.with_timezone(&Utc))
                })
        })
        .filter(|at| *at > now)
        .unwrap_or_else(|| now + chrono::Duration::minutes(5))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn retry_after_accepts_seconds_dates_and_defaults() {
        let now = DateTime::parse_from_rfc3339("2026-11-28T00:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        assert_eq!(
            retry_after(Some("30"), now),
            now + chrono::Duration::seconds(30)
        );
        assert_eq!(
            retry_after(Some("Sat, 28 Nov 2026 00:02:00 GMT"), now),
            now + chrono::Duration::minutes(2)
        );
        assert_eq!(
            retry_after(Some("bad"), now),
            now + chrono::Duration::minutes(5)
        );
    }
    #[test]
    fn document_ids_and_whoop_ids_are_bounded() {
        assert!(valid_id("a_12-x"));
        assert!(!valid_id("../x"));
        assert!(whoop_uuid("123e4567-e89b-12d3-a456-426614174000"));
        assert!(!whoop_uuid("not-a-uuid"));
    }

    #[test]
    fn intervals_activity_scope_matches_provider_scope_contract() {
        assert!(intervals_scope("WELLNESS:READ, ACTIVITY:READ"));
        assert!(intervals_scope("ACTIVITY:WRITE"));
        assert!(!intervals_scope("WELLNESS:READ"));
        assert!(!intervals_scope("ACTIVITY"));
    }

    #[test]
    fn oura_sleep_session_preserves_stages_and_provider_document() -> Result<()> {
        let observed = DateTime::parse_from_rfc3339("2026-11-28T01:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let doc = json!({
            "id":"sleep-1",
            "bedtime_start":"2026-11-28T00:00:00Z",
            "bedtime_end":"2026-11-28T00:02:00Z",
            "sleep_phase_30_sec":"11234",
            "total_sleep_duration":120
        });
        let record = oura_session(&doc, "sleep", observed)?;
        assert_eq!(record.kind, "sleep");
        assert_eq!(record.payload["stage_resolution_seconds"], 30);
        assert_eq!(record.payload["stages"].as_array().unwrap().len(), 3);
        assert_eq!(record.payload["provider_data"], doc);
        Ok(())
    }

    #[test]
    fn oura_workout_session_matches_snapshot_projection() -> Result<()> {
        let observed = DateTime::parse_from_rfc3339("2026-11-28T01:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let doc = json!({
            "id":"workout-1",
            "start_datetime":"2026-11-28T00:00:00Z",
            "end_datetime":"2026-11-28T00:30:00Z",
            "activity":"running",
            "label":"Easy run",
            "distance":5000.0,
            "calories":300
        });
        let record = oura_session(&doc, "workout", observed)?;
        assert_eq!(record.kind, "workout");
        assert_eq!(record.payload["exercise_type"], "running");
        assert_eq!(record.payload["duration_seconds"], 1800);
        assert_eq!(record.payload["distance_meters"], 5000.0);
        Ok(())
    }
}
