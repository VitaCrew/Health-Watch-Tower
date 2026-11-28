use std::{
    collections::{BTreeMap, BTreeSet},
    env,
    sync::Arc,
};

use crate::mcp::App;
use anyhow::{ensure, Context, Result};
use axum::{
    body::Bytes,
    extract::{Path, Query, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    Json,
};
use base64::{engine::general_purpose::STANDARD, Engine};
use chrono::{DateTime, FixedOffset, NaiveDate, Utc};
use deadpool_postgres::Pool;
use ring::{digest, hmac};
use serde_json::{json, Value};
use subtle::ConstantTimeEq;

#[cfg(test)]
#[path = "wearable_webhooks_tests.rs"]
mod tests;

pub fn account_id(value: &Value) -> Option<String> {
    value
        .as_str()
        .filter(|id| !id.is_empty() && id.len() <= 256)
        .map(str::to_owned)
        .or_else(|| value.as_u64().map(|id| id.to_string()))
}

pub fn hash(value: &[u8]) -> Vec<u8> {
    digest::digest(&digest::SHA256, value).as_ref().to_vec()
}

fn secret(name: &str) -> Result<String> {
    let value = env::var(name).context("webhook is not enabled")?;
    ensure!(value.len() >= 16, "webhook is not enabled");
    Ok(value)
}

fn header<'a>(headers: &'a HeaderMap, name: &str) -> Result<&'a str> {
    headers
        .get(name)
        .and_then(|v| v.to_str().ok())
        .context("webhook authentication required")
}

fn same(expected: &str, supplied: &str) -> bool {
    bool::from(expected.as_bytes().ct_eq(supplied.as_bytes()))
}

fn decode_hex_32(text: &str) -> Result<Vec<u8>> {
    ensure!(
        text.len() == 64 && text.is_ascii(),
        "invalid webhook signature"
    );
    (0..64)
        .step_by(2)
        .map(|i| u8::from_str_radix(&text[i..i + 2], 16).map_err(Into::into))
        .collect()
}

fn signed(key: &str, timestamp: &str, body: &[u8], signature: &[u8]) -> bool {
    let mut message = Vec::with_capacity(timestamp.len() + body.len());
    message.extend_from_slice(timestamp.as_bytes());
    message.extend_from_slice(body);
    hmac::verify(
        &hmac::Key::new(hmac::HMAC_SHA256, key.as_bytes()),
        &message,
        signature,
    )
    .is_ok()
}

fn check_age(timestamp: &str, seconds: i64) -> Result<()> {
    let epoch = timestamp
        .parse::<i64>()
        .map(|value| {
            if value > 10_000_000_000 {
                value
            } else {
                value.saturating_mul(1000)
            }
        })
        .or_else(|_| {
            chrono::DateTime::parse_from_rfc3339(timestamp).map(|at| at.timestamp_millis())
        })?;
    ensure!(
        Utc::now().timestamp_millis().abs_diff(epoch) <= (seconds * 1000) as u64,
        "webhook timestamp outside accepted window"
    );
    Ok(())
}

fn authenticate(provider: &str, headers: &HeaderMap, body: &[u8]) -> Result<()> {
    let (key, timestamp, signature) = match provider {
        "whoop" => {
            let timestamp = header(headers, "x-whoop-signature-timestamp")?;
            check_age(timestamp, 300)?;
            (
                secret("AVATAR_WHOOP_CLIENT_SECRET")?,
                timestamp,
                STANDARD.decode(header(headers, "x-whoop-signature")?)?,
            )
        }
        "oura" => {
            let timestamp = header(headers, "x-oura-timestamp")?;
            check_age(timestamp, 300)?;
            (
                secret("AVATAR_OURA_CLIENT_SECRET")?,
                timestamp,
                decode_hex_32(header(headers, "x-oura-signature")?)?,
            )
        }
        "polar" => (
            secret("AVATAR_POLAR_WEBHOOK_SECRET")?,
            "",
            decode_hex_32(header(headers, "polar-webhook-signature")?)?,
        ),
        "suunto" => (
            secret("AVATAR_SUUNTO_WEBHOOK_NOTIFICATION_SECRET")?,
            "",
            decode_hex_32(header(headers, "x-hmac-sha256-signature")?)?,
        ),
        _ => anyhow::bail!("unsupported webhook"),
    };
    ensure!(
        signed(&key, timestamp, body, &signature),
        "invalid webhook signature"
    );
    Ok(())
}

pub async fn signed_event(
    State(app): State<Arc<App>>,
    Path(provider): Path<String>,
    headers: HeaderMap,
    body: Bytes,
) -> StatusCode {
    if !matches!(provider.as_str(), "whoop" | "oura" | "polar") {
        return StatusCode::NOT_FOUND;
    }
    if body.len() > 65_536 {
        return StatusCode::PAYLOAD_TOO_LARGE;
    }
    // Polar pings the configured URL before supplying its signing key. A ping cannot queue work.
    if provider == "polar"
        && headers
            .get("polar-webhook-event")
            .and_then(|h| h.to_str().ok())
            == Some("PING")
        && serde_json::from_slice::<Value>(&body)
            .ok()
            .is_some_and(|data| data["event"] == "PING")
    {
        return StatusCode::OK;
    }
    if authenticate(&provider, &headers, &body).is_err() {
        return StatusCode::UNAUTHORIZED;
    }
    let event: Value = match serde_json::from_slice(&body) {
        Ok(value) => value,
        Err(_) => return StatusCode::BAD_REQUEST,
    };
    if provider == "polar"
        && event["timestamp"]
            .as_str()
            .is_none_or(|time| check_age(time, 86_400).is_err())
    {
        return StatusCode::BAD_REQUEST;
    }
    let Some(owner) = account_id(&event["user_id"]) else {
        return StatusCode::BAD_REQUEST;
    };
    if provider == "polar" {
        if event["event"] == "PHYSICAL_INFORMATION" {
            let Some(at) = event["timestamp"]
                .as_str()
                .and_then(|at| at.parse::<DateTime<Utc>>().ok())
            else {
                return StatusCode::BAD_REQUEST;
            };
            return reply(
                crate::wearable_webhook_documents::enqueue(
                    &app.db,
                    "polar",
                    &owner,
                    crate::wearable_webhook_documents::polar_physical_info(at),
                    &body,
                )
                .await,
            );
        }
        if let Some(id) = event["entity_id"]
            .as_str()
            .filter(|_| event["event"] == "EXERCISE")
        {
            if crate::wearable_fit::enqueue_workout_notification(
                &app.db,
                crate::cloud_wearables::Provider::Polar,
                &owner,
                id,
            )
            .await
            .is_err()
            {
                return StatusCode::SERVICE_UNAVAILABLE;
            }
        }
        let days = polar_target_days(&event, Utc::now().date_naive());
        return reply(nudge_days(&app.db, &provider, &owner, &body, &days).await);
    }
    if let Some(target) = matches!(provider.as_str(), "oura" | "whoop")
        .then(|| crate::wearable_webhook_documents::target(&provider, &event))
        .flatten()
    {
        let delivery = if provider == "oura" {
            let timestamp = header(&headers, "x-oura-timestamp").unwrap_or_default();
            [timestamp.as_bytes(), &body].concat()
        } else {
            body.to_vec()
        };
        return reply(
            crate::wearable_webhook_documents::enqueue(
                &app.db, &provider, &owner, target, &delivery,
            )
            .await,
        );
    }
    reply(nudge(&app.db, &provider, &owner, &body).await)
}

pub async fn oura_challenge(
    Path(provider): Path<String>,
    Query(query): Query<std::collections::HashMap<String, String>>,
) -> Response {
    if provider != "oura" {
        return StatusCode::NOT_FOUND.into_response();
    }
    if challenge(
        &query,
        "verification_token",
        "challenge",
        "AVATAR_OURA_WEBHOOK_VERIFY_TOKEN",
    ) {
        Json(json!({"challenge":query["challenge"]})).into_response()
    } else {
        StatusCode::FORBIDDEN.into_response()
    }
}

fn challenge(
    query: &std::collections::HashMap<String, String>,
    token: &str,
    answer: &str,
    configured: &str,
) -> bool {
    secret(configured)
        .is_ok_and(|expected| query.get(token).is_some_and(|value| same(&expected, value)))
        && query
            .get(answer)
            .is_some_and(|value| !value.is_empty() && value.len() <= 256)
}

pub async fn strava_challenge(
    Path(capability): Path<String>,
    Query(query): Query<std::collections::HashMap<String, String>>,
) -> Response {
    if !strava_capability(&capability) {
        return StatusCode::NOT_FOUND.into_response();
    }
    if query
        .get("hub.mode")
        .is_some_and(|mode| mode == "subscribe")
        && challenge(
            &query,
            "hub.verify_token",
            "hub.challenge",
            "AVATAR_STRAVA_WEBHOOK_VERIFY_TOKEN",
        )
    {
        Json(json!({"hub.challenge":query["hub.challenge"]})).into_response()
    } else {
        StatusCode::FORBIDDEN.into_response()
    }
}

fn strava_capability(path: &str) -> bool {
    secret("AVATAR_STRAVA_WEBHOOK_PATH_KEY")
        .is_ok_and(|expected| expected.len() >= 43 && same(&expected, path))
}

fn verify_strava_signature(key: &str, signature_header: &str, body: &[u8]) -> Result<()> {
    ensure!(
        signature_header.len() <= 1024,
        "invalid Strava signature header"
    );
    let mut timestamp = None;
    let mut signature = None;
    for part in signature_header.split(',') {
        let (name, value) = part
            .trim()
            .split_once('=')
            .context("invalid Strava signature field")?;
        let field = match name {
            "t" => &mut timestamp,
            "v1" => &mut signature,
            _ => continue,
        };
        ensure!(
            field.replace(value).is_none(),
            "duplicate Strava signature field"
        );
    }
    let timestamp = timestamp.context("missing Strava signature timestamp")?;
    ensure!(
        timestamp.bytes().all(|c| c.is_ascii_digit()),
        "invalid Strava timestamp"
    );
    let epoch = timestamp.parse::<i64>()?;
    ensure!(
        Utc::now().timestamp().abs_diff(epoch) <= 300,
        "Strava signature expired"
    );
    let signature = decode_hex_32(signature.context("missing Strava signature digest")?)?;
    ensure!(
        signed(key, &format!("{timestamp}."), body, &signature),
        "invalid Strava signature"
    );
    Ok(())
}

pub async fn strava_event(
    State(app): State<Arc<App>>,
    Path(capability): Path<String>,
    headers: HeaderMap,
    body: Bytes,
) -> StatusCode {
    if !strava_capability(&capability) {
        return StatusCode::NOT_FOUND;
    }
    if body.len() > 65_536 {
        return StatusCode::PAYLOAD_TOO_LARGE;
    }
    let authenticated = secret("AVATAR_STRAVA_WEBHOOK_SIGNING_SECRET").and_then(|key| {
        ensure!(
            headers.get_all("x-strava-signature").iter().count() == 1,
            "expected one Strava signature header"
        );
        verify_strava_signature(&key, header(&headers, "x-strava-signature")?, &body)
    });
    if authenticated.is_err() {
        return StatusCode::FORBIDDEN;
    }
    let event: Value = match serde_json::from_slice(&body) {
        Ok(value) => value,
        Err(_) => return StatusCode::BAD_REQUEST,
    };
    let expected = env::var("AVATAR_STRAVA_WEBHOOK_SUBSCRIPTION_ID").ok();
    let Some(event_at) = event["event_time"]
        .as_i64()
        .and_then(|at| DateTime::from_timestamp(at, 0))
    else {
        return StatusCode::FORBIDDEN;
    };
    let age = (Utc::now() - event_at).num_seconds();
    if expected.is_none()
        || account_id(&event["subscription_id"]) != expected
        || !(-300..=86_400).contains(&age)
    {
        return StatusCode::FORBIDDEN;
    }
    let Some(owner) = account_id(&event["owner_id"]) else {
        return StatusCode::BAD_REQUEST;
    };
    if event["object_type"] == "activity" {
        let Some(activity) = account_id(&event["object_id"]) else {
            return StatusCode::BAD_REQUEST;
        };
        let Some(target) = crate::wearable_webhook_documents::strava_activity(
            &activity,
            event["aspect_type"].as_str().unwrap_or_default(),
            event_at,
        ) else {
            return StatusCode::BAD_REQUEST;
        };
        return reply(
            crate::wearable_webhook_documents::enqueue(&app.db, "strava", &owner, target, &body)
                .await,
        );
    }
    if event["object_type"] == "athlete"
        && event["aspect_type"] == "update"
        && event["updates"]["authorized"] == "false"
    {
        if account_id(&event["object_id"]).as_deref() != Some(owner.as_str())
            || !owner.parse::<i64>().is_ok_and(|id| id > 0)
        {
            return StatusCode::BAD_REQUEST;
        }
        let target = crate::wearable_webhook_documents::strava_revocation(&owner, event_at);
        return reply(
            crate::wearable_webhook_documents::enqueue(&app.db, "strava", &owner, target, &body)
                .await,
        );
    }
    StatusCode::OK
}

fn has_capability(variable: &str, supplied: &str) -> bool {
    secret(variable).is_ok_and(|key| key.len() >= 43 && same(&key, supplied))
}

pub async fn withings_probe(Path(key): Path<String>) -> StatusCode {
    if has_capability("AVATAR_WITHINGS_WEBHOOK_PATH_KEY", &key) {
        StatusCode::OK
    } else {
        StatusCode::NOT_FOUND
    }
}

pub async fn withings_event(
    State(app): State<Arc<App>>,
    Path(key): Path<String>,
    axum::Form(fields): axum::Form<std::collections::BTreeMap<String, String>>,
) -> StatusCode {
    if !has_capability("AVATAR_WITHINGS_WEBHOOK_PATH_KEY", &key) {
        return StatusCode::NOT_FOUND;
    }
    let Some(owner) = fields
        .get("userid")
        .filter(|value| value.len() <= 64 && value.parse::<u64>().is_ok())
    else {
        return StatusCode::BAD_REQUEST;
    };
    let Some(category) = fields
        .get("appli")
        .filter(|value| value.parse::<u16>().is_ok())
    else {
        return StatusCode::BAD_REQUEST;
    };
    let Ok(body) = serde_json::to_vec(&json!([
        owner,
        category,
        fields.get("startdate"),
        fields.get("enddate"),
        fields.get("date")
    ])) else {
        return StatusCode::BAD_REQUEST;
    };
    let days = withings_target_days(&fields, Utc::now().date_naive())
        .map_or_else(Vec::new, |(first, last)| {
            first.iter_days().take_while(|day| *day <= last).collect()
        });
    reply(nudge_days(&app.db, "withings", &owner, &body, &days).await)
}

fn withings_target_days(
    fields: &std::collections::BTreeMap<String, String>,
    today: NaiveDate,
) -> Option<(NaiveDate, NaiveDate)> {
    match fields.get("appli")?.parse::<u16>().ok()? {
        1 | 2 | 4 | 16 | 44 | 54 | 58 | 61 | 62 | 63 => {}
        _ => return None,
    }
    let epoch = |text: &str| DateTime::from_timestamp(text.parse().ok()?, 0);
    let (start, end) = match (fields.get("startdate"), fields.get("enddate")) {
        (Some(start), Some(end)) => {
            let (start, end) = (epoch(start)?, epoch(end)?);
            if start > end {
                return None;
            }
            (start.date_naive(), end.date_naive())
        }
        (None, None) => {
            let date = fields.get("date")?;
            let day = date
                .parse::<NaiveDate>()
                .ok()
                .or_else(|| epoch(date).map(|at| at.date_naive()))?;
            (day, day)
        }
        _ => return None,
    };
    (start >= NaiveDate::from_ymd_opt(1970, 1, 1)? && end <= today && (end - start).num_days() < 31)
        .then_some((start, end))
}

pub async fn suunto_event(
    State(app): State<Arc<App>>,
    Path(key): Path<String>,
    headers: HeaderMap,
    body: Bytes,
) -> StatusCode {
    if !has_capability("AVATAR_SUUNTO_WEBHOOK_PATH_KEY", &key) {
        return StatusCode::NOT_FOUND;
    }
    if body.len() > 65_536 {
        return StatusCode::PAYLOAD_TOO_LARGE;
    }
    if authenticate("suunto", &headers, &body).is_err() {
        return StatusCode::UNAUTHORIZED;
    }
    let form = header(&headers, "content-type").is_ok_and(|value| {
        value
            .split(';')
            .next()
            .unwrap_or_default()
            .trim()
            .eq_ignore_ascii_case("application/x-www-form-urlencoded")
    });
    let now = Utc::now();
    let (owner, workout, days) = if form {
        #[derive(serde::Deserialize)]
        struct LegacyWorkout {
            username: String,
            workoutid: String,
        }
        let Ok(event) = serde_urlencoded::from_bytes::<LegacyWorkout>(&body) else {
            return StatusCode::BAD_REQUEST;
        };
        (event.username, Some(event.workoutid), Vec::new())
    } else {
        let Ok(event) = serde_json::from_slice::<Value>(&body) else {
            return StatusCode::BAD_REQUEST;
        };
        let Some(owner) = account_id(&event["username"]) else {
            return StatusCode::BAD_REQUEST;
        };
        match event["type"].as_str() {
            Some("WORKOUT_CREATED") | None => {
                let Some(key) = event["workout"]["workoutKey"]
                    .as_str()
                    .or_else(|| event["workoutid"].as_str())
                else {
                    return StatusCode::BAD_REQUEST;
                };
                (
                    owner,
                    Some(key.into()),
                    suunto_workout_days(&event["workout"], now),
                )
            }
            Some(
                "SUUNTO_247_SLEEP_CREATED"
                | "SUUNTO_247_ACTIVITY_CREATED"
                | "SUUNTO_247_RECOVERY_CREATED",
            ) => (owner, None, suunto_target_days(&event, now.date_naive())),
            // A saved route does not represent a recorded activity.
            Some("ROUTE_CREATED") => return StatusCode::OK,
            _ => return StatusCode::BAD_REQUEST,
        }
    };
    if owner.is_empty() || owner.len() > 256 || owner.chars().any(char::is_control) {
        return StatusCode::BAD_REQUEST;
    }
    if let Some(key) = workout.as_deref() {
        if !crate::wearable_fit::valid_suunto_key(key) {
            return StatusCode::BAD_REQUEST;
        }
        if crate::wearable_fit::enqueue_workout_notification(
            &app.db,
            crate::cloud_wearables::Provider::Suunto,
            &owner,
            key,
        )
        .await
        .is_err()
        {
            return StatusCode::SERVICE_UNAVAILABLE;
        }
    }
    reply(nudge_days(&app.db, "suunto", &owner, &body, &days).await)
}

fn suunto_workout_days(workout: &Value, now: DateTime<Utc>) -> Vec<NaiveDate> {
    let offset = workout["timeOffsetInMinutes"]
        .as_i64()
        .filter(|minutes| (-840..=840).contains(minutes))
        .and_then(|minutes| FixedOffset::east_opt(minutes as i32 * 60));
    ["startTime", "stopTime"]
        .into_iter()
        .filter_map(|name| {
            let millis = workout[name].as_i64()?;
            let at = DateTime::from_timestamp_millis(millis)?;
            (millis > 0 && at <= now).then_some(at)
        })
        .flat_map(|at| {
            [
                Some(at.date_naive()),
                offset.map(|zone| at.with_timezone(&zone).date_naive()),
            ]
        })
        .flatten()
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect()
}

fn suunto_target_days(event: &Value, today: NaiveDate) -> Vec<NaiveDate> {
    let kind = event["type"].as_str();
    if !matches!(
        kind,
        Some(
            "SUUNTO_247_ACTIVITY_CREATED"
                | "SUUNTO_247_SLEEP_CREATED"
                | "SUUNTO_247_RECOVERY_CREATED"
        )
    ) {
        return Vec::new();
    }
    let earliest = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
    let mut days = BTreeSet::new();
    for sample in event["samples"].as_array().into_iter().flatten() {
        let data = &sample["entryData"];
        let (bedtime, rising) = if kind == Some("SUUNTO_247_SLEEP_CREATED") {
            (data["BedtimeStart"].as_str(), data["BedtimeEnd"].as_str())
        } else {
            (None, None)
        };
        for time in [sample["timestamp"].as_str(), bedtime, rising]
            .into_iter()
            .flatten()
            .filter_map(|value| DateTime::parse_from_rfc3339(value).ok())
        {
            for day in [time.date_naive(), time.with_timezone(&Utc).date_naive()] {
                if (earliest..=today).contains(&day) {
                    days.insert(day);
                }
            }
            if days.len() > 31 {
                return Vec::new();
            }
        }
    }
    days.into_iter().collect()
}

fn polar_target_days(event: &Value, today: NaiveDate) -> Vec<NaiveDate> {
    let parse = |field: &str| event[field].as_str()?.parse::<NaiveDate>().ok();
    let (start, end) = match event["event"].as_str() {
        Some("SLEEP" | "CONTINUOUS_HEART_RATE" | "NIGHTLY_RECHARGE" | "ACTIVITY_SUMMARY") => {
            let day = parse("date");
            (day, day)
        }
        Some("SLEEP_WISE_CIRCADIAN_BEDTIME" | "SLEEP_WISE_ALERTNESS") => {
            (parse("from"), parse("to"))
        }
        _ => return Vec::new(),
    };
    let (Some(start), Some(end)) = (start, end) else {
        return Vec::new();
    };
    let earliest = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
    if start < earliest || end < start || end > today || (end - start).num_days() >= 31 {
        return Vec::new();
    }
    start.iter_days().take_while(|day| *day <= end).collect()
}

fn reply(result: Result<()>) -> StatusCode {
    if result.is_ok() {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    }
}

pub async fn nudge(pool: &Pool, provider: &str, owner: &str, body: &[u8]) -> Result<()> {
    nudge_days(pool, provider, owner, body, &[]).await
}

pub(crate) async fn nudge_days(
    pool: &Pool,
    provider: &str,
    owner: &str,
    body: &[u8],
    days: &[NaiveDate],
) -> Result<()> {
    enqueue_nudge(
        pool,
        provider,
        &[hash(owner.as_bytes())],
        &hash(body),
        days,
        None,
    )
    .await
}

pub(crate) async fn nudge_google_record(
    pool: &Pool,
    owner: &str,
    body: &[u8],
    kind: &str,
    source_id: &str,
) -> Result<()> {
    enqueue_nudge(
        pool,
        "google_health",
        &[hash(owner.as_bytes())],
        &hash(body),
        &[],
        Some((kind, source_id)),
    )
    .await
}

async fn enqueue_nudge(
    pool: &Pool,
    provider: &str,
    accounts: &[Vec<u8>],
    fingerprint: &[u8],
    days: &[NaiveDate],
    record: Option<(&str, &str)>,
) -> Result<()> {
    pool.get().await?.execute(
        "WITH receipt AS (
           INSERT INTO watch_tower.wearable_webhook_receipt AS existing(provider,event_hash)
           SELECT $1,$2 WHERE EXISTS (
             SELECT 1 FROM avatar_hsm.wearable_oauth_connection WHERE provider=$1 AND provider_account_hash=ANY($3::bytea[]))
           ON CONFLICT (provider,event_hash) DO UPDATE SET received_at=excluded.received_at
             WHERE existing.provider IN ('withings','suunto') AND existing.received_at<now()-interval '5 minutes'
           RETURNING provider
         ), targets AS (
           SELECT connection.subject_id, connection.provider,
             CASE WHEN $5::text IS NULL THEN $4::date[] ELSE ARRAY(
               SELECT DISTINCT dates.day
               FROM avatar_hsm.wearable_record AS saved
               CROSS JOIN LATERAL unnest(ARRAY[
                 (saved.started_at AT TIME ZONE 'UTC')::date,
                 (GREATEST(saved.started_at,saved.ended_at-interval '1 microsecond') AT TIME ZONE 'UTC')::date
               ]) AS dates(day)
               WHERE saved.subject_id=connection.subject_id AND saved.provider=connection.provider
                 AND saved.kind=$5 AND saved.source_id=$6
                 AND dates.day BETWEEN date '1970-01-01' AND (now() AT TIME ZONE 'UTC')::date
               ORDER BY dates.day LIMIT 31
             ) END AS days
           FROM avatar_hsm.wearable_oauth_connection connection
           JOIN receipt USING (provider)
           WHERE connection.provider_account_hash=ANY($3::bytea[])
         ), scheduled AS (
         UPDATE watch_tower.wearable_sync_schedule AS jobs SET next_sync=LEAST(jobs.next_sync,now()),
             wake_requested=jobs.wake_requested OR cardinality(targets.days)=0
         FROM targets
         WHERE jobs.subject_id=targets.subject_id AND jobs.provider=targets.provider
         RETURNING jobs.subject_id, jobs.provider, targets.days
         )
         INSERT INTO watch_tower.wearable_target_day(subject_id,provider,day)
         SELECT scheduled.subject_id,scheduled.provider,dates.day
         FROM scheduled CROSS JOIN LATERAL unnest(scheduled.days) AS dates(day)
         ON CONFLICT (subject_id,provider,day) DO UPDATE
             SET revision=excluded.revision,next_attempt=excluded.next_attempt,failures=0",
        &[&provider,&fingerprint,&accounts,&days,&record.map(|pair| pair.0),&record.map(|pair| pair.1)],
    ).await?;
    Ok(())
}

#[derive(serde::Deserialize)]
pub struct CorosSignature {
    signature: String,
    nonce: String,
    timestamp: String,
}

fn verify_coros(token: &str, signed: &CorosSignature) -> Result<Vec<u8>> {
    ensure!(
        (16..=32).contains(&token.len()) && token.bytes().all(|c| c.is_ascii_alphanumeric()),
        "invalid COROS signing token"
    );
    ensure!(
        (1..=128).contains(&signed.nonce.len())
            && signed.nonce.bytes().all(|c| c.is_ascii_graphic()),
        "invalid COROS nonce"
    );
    ensure!(
        matches!(signed.timestamp.len(), 10 | 13)
            && signed.timestamp.bytes().all(|c| c.is_ascii_digit()),
        "invalid COROS timestamp"
    );
    check_age(&signed.timestamp, 300)?;
    ensure!(
        signed.signature.len() == 40 && signed.signature.bytes().all(|b| b.is_ascii_hexdigit()),
        "invalid COROS signature"
    );
    let mut parts = [token, &signed.timestamp, &signed.nonce];
    parts.sort_unstable();
    // The signed callback protocol specified by COROS uses SHA-1 over this tuple.
    let digest = digest::digest(&digest::SHA1_FOR_LEGACY_USE_ONLY, parts.concat().as_bytes());
    let expected = digest
        .as_ref()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    ensure!(
        same(&expected, &signed.signature.to_ascii_lowercase()),
        "invalid COROS signature"
    );
    Ok(hash(digest.as_ref()))
}

fn coros_targets(body: &Value, today: NaiveDate) -> Result<BTreeMap<Vec<u8>, BTreeSet<NaiveDate>>> {
    let (entries, workouts) = match (body.get("sportDataList"), body.get("batchDailyList")) {
        (Some(items), None) => (items.as_array(), true),
        (None, Some(items)) => (items.as_array(), false),
        _ => (None, false),
    };
    let entries = entries.context("invalid COROS notification batch")?;
    ensure!(
        (1..=256).contains(&entries.len()),
        "invalid COROS notification batch size"
    );
    let earliest = today
        .checked_sub_months(chrono::Months::new(3))
        .unwrap_or(today);
    // A device ahead of UTC can already be reporting tomorrow's calendar day.
    let latest = today.succ_opt().unwrap_or(today);
    let mut owners = BTreeMap::<_, BTreeSet<_>>::new();
    for entry in entries {
        let owner = account_id(&entry["openId"]).context("missing COROS account")?;
        let days = owners.entry(hash(owner.as_bytes())).or_default();
        if workouts {
            for field in ["startTime", "endTime"] {
                if let Some(day) = entry[field]
                    .as_i64()
                    .and_then(|timestamp| DateTime::<Utc>::from_timestamp(timestamp, 0))
                    .map(|at| at.date_naive())
                    .filter(|day| (earliest..=latest).contains(day))
                {
                    days.insert(day);
                }
            }
        } else {
            days.extend(
                entry["dailyList"]
                    .as_array()
                    .into_iter()
                    .flatten()
                    .filter_map(|item| {
                        let date = item["happenDay"].as_i64()?.to_string();
                        NaiveDate::parse_from_str(&date, "%Y%m%d").ok()
                    })
                    .filter(|day| (earliest..=latest).contains(day)),
            );
        }
    }
    Ok(owners)
}

async fn enqueue_coros(
    pool: &Pool,
    owners: BTreeMap<Vec<u8>, BTreeSet<NaiveDate>>,
    fingerprint: &[u8],
) -> Result<()> {
    let (accounts, days): (Vec<_>, Vec<_>) = owners.into_iter().unzip();
    let calendar = json!(days);
    pool.get()
        .await?
        .execute(
            "WITH targets AS (
           SELECT connection.subject_id, connection.provider,
             ARRAY(SELECT value::date FROM jsonb_array_elements_text(
               $3::jsonb -> (input.ordinality::int-1))) AS days
           FROM unnest($2::bytea[]) WITH ORDINALITY AS input(account_hash,ordinality)
           JOIN avatar_hsm.wearable_oauth_connection connection
             ON connection.provider='coros' AND connection.provider_account_hash=input.account_hash
         ), receipt AS (
           INSERT INTO watch_tower.wearable_webhook_receipt(provider,event_hash)
           SELECT 'coros',$1 WHERE EXISTS (SELECT 1 FROM targets)
           ON CONFLICT DO NOTHING RETURNING provider
         ), scheduled AS (
           UPDATE watch_tower.wearable_sync_schedule AS jobs
           SET next_sync=LEAST(jobs.next_sync,now()),
               wake_requested=jobs.wake_requested OR cardinality(targets.days)=0
           FROM targets JOIN receipt USING (provider)
           WHERE jobs.subject_id=targets.subject_id AND jobs.provider=targets.provider
           RETURNING jobs.subject_id,jobs.provider,targets.days
         )
         INSERT INTO watch_tower.wearable_target_day(subject_id,provider,day)
         SELECT scheduled.subject_id,scheduled.provider,dates.day
         FROM scheduled CROSS JOIN LATERAL unnest(scheduled.days) AS dates(day)
         ON CONFLICT (subject_id,provider,day) DO UPDATE
           SET revision=excluded.revision,next_attempt=excluded.next_attempt,failures=0",
            &[&fingerprint, &accounts, &calendar],
        )
        .await?;
    Ok(())
}

pub async fn coros_probe(State(app): State<Arc<App>>) -> StatusCode {
    if app.db.get().await.is_ok() {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    }
}

pub async fn intervals_event(State(app): State<Arc<App>>, body: Bytes) -> StatusCode {
    if body.len() > 262_144 {
        return StatusCode::PAYLOAD_TOO_LARGE;
    }
    let Ok(body) = serde_json::from_slice::<Value>(&body) else {
        return StatusCode::BAD_REQUEST;
    };
    if !env::var("AVATAR_INTERVALS_ICU_WEBHOOK_SECRET").is_ok_and(|secret| {
        !secret.is_empty()
            && body["secret"]
                .as_str()
                .is_some_and(|supplied| same(&secret, supplied))
    }) {
        return StatusCode::UNAUTHORIZED;
    }
    let Some(events) = body["events"]
        .as_array()
        .filter(|events| events.len() <= 256)
    else {
        return StatusCode::BAD_REQUEST;
    };
    let now = Utc::now();
    for event in events {
        if !matches!(
            event["type"].as_str(),
            Some("ACTIVITY_UPLOADED" | "ACTIVITY_ANALYZED")
        ) {
            continue;
        }
        let (Some(owner), Some(activity), Some(at)) = (
            account_id(&event["athlete_id"]),
            account_id(&event["activity"]["id"]),
            event["timestamp"]
                .as_str()
                .and_then(|value| value.parse::<DateTime<Utc>>().ok())
                .filter(|at| at.timestamp() >= 0 && *at <= now + chrono::Duration::minutes(5)),
        ) else {
            continue;
        };
        let Some(target) = crate::wearable_webhook_documents::intervals_activity(&activity, at)
        else {
            continue;
        };
        let fingerprint = json!([owner, activity, at]).to_string();
        if crate::wearable_webhook_documents::enqueue(
            &app.db,
            "intervals_icu",
            &owner,
            target,
            fingerprint.as_bytes(),
        )
        .await
        .is_err()
        {
            return StatusCode::SERVICE_UNAVAILABLE;
        }
    }
    StatusCode::OK
}

pub async fn coros_event(
    State(app): State<Arc<App>>,
    Query(signed): Query<CorosSignature>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    if body.len() > 262_144 {
        return StatusCode::PAYLOAD_TOO_LARGE.into_response();
    }
    let authenticate = || -> Result<Vec<u8>> {
        let client = std::env::var("AVATAR_COROS_WEBHOOK_CLIENT")?;
        let shared = secret("AVATAR_COROS_WEBHOOK_SECRET")?;
        ensure!(
            !client.is_empty()
                && same(&client, header(&headers, "client")?)
                && same(&shared, header(&headers, "secret")?),
            "invalid COROS callback credentials"
        );
        verify_coros(&secret("AVATAR_COROS_WEBHOOK_SIGN_TOKEN")?, &signed)
    };
    let Ok(fingerprint) = authenticate() else {
        return StatusCode::UNAUTHORIZED.into_response();
    };
    let Ok(owners) = serde_json::from_slice::<Value>(&body)
        .map_err(Into::into)
        .and_then(|body| coros_targets(&body, Utc::now().date_naive()))
    else {
        return StatusCode::BAD_REQUEST.into_response();
    };
    if enqueue_coros(&app.db, owners, &fingerprint).await.is_err() {
        return StatusCode::SERVICE_UNAVAILABLE.into_response();
    }
    Json(json!({"result":"0000","message":"OK"})).into_response()
}
