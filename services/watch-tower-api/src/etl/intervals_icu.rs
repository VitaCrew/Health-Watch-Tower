use anyhow::{ensure, Context, Result};
use avatar_hsm::etl_adapter::Snapshot;
use chrono::{DateTime, Duration, NaiveDate, Utc};
use reqwest::{Client, Response};
use serde_json::{json, Value};

const API: &str = "https://intervals.icu";
const MAX_RESPONSE_BYTES: usize = 8 * 1024 * 1024;

#[derive(Debug)]
pub struct ProviderRateLimited {
    pub retry_at: DateTime<Utc>,
}

impl std::fmt::Display for ProviderRateLimited {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("Intervals.icu returned HTTP 429")
    }
}

impl std::error::Error for ProviderRateLimited {}

pub fn provider_retry_at(error: &anyhow::Error) -> Option<DateTime<Utc>> {
    error
        .downcast_ref::<ProviderRateLimited>()
        .map(|limited| limited.retry_at)
}

pub async fn collect_window(
    client: &Client,
    token: &str,
    scope: Option<&str>,
    lease: &str,
    start: DateTime<Utc>,
    end: DateTime<Utc>,
) -> Result<Snapshot> {
    collect_from_origin(client, token, scope, lease, start, end, API, Utc::now()).await
}

async fn collect_from_origin(
    client: &Client,
    token: &str,
    scope: Option<&str>,
    lease: &str,
    start: DateTime<Utc>,
    end: DateTime<Utc>,
    origin: &str,
    observed_at: DateTime<Utc>,
) -> Result<Snapshot> {
    ensure!(
        !token.trim().is_empty() && !lease.is_empty(),
        "Intervals.icu credential and HSM lease are required"
    );
    ensure!(
        start < end && end - start <= Duration::days(7),
        "invalid Intervals.icu collection window"
    );
    let window = Window { start, end };
    let scope = scope.filter(|scope| !scope.trim().is_empty());
    let oldest = (start - Duration::days(1)).date_naive().to_string();
    let newest = (end + Duration::days(1)).date_naive().to_string();
    let mut rows = Vec::new();
    let mut grants = Vec::new();
    if scope.is_none_or(|scope| intervals_scope(scope, "ACTIVITY")) {
        let activities = fetch(
            client,
            token,
            scope.is_some(),
            origin,
            "activities",
            &oldest,
            &newest,
        )
        .await?;
        let activities = activities
            .as_array()
            .context("Intervals.icu activities must be an array")?;
        ensure!(
            activities.len() < 2_000,
            "incomplete Intervals.icu activity history"
        );
        for activity in activities {
            if let Some(mut row) = intervals_point(activity, &window)? {
                row["modified_at"] = json!(observed_at);
                rows.push(row);
            }
        }
        grants.push("workout");
    }
    if scope.is_none_or(|scope| intervals_scope(scope, "WELLNESS")) {
        let wellness = fetch(
            client,
            token,
            scope.is_some(),
            origin,
            "wellness",
            &oldest,
            &newest,
        )
        .await?;
        for item in wellness
            .as_array()
            .context("Intervals.icu wellness must be an array")?
        {
            if let Some(row) = intervals_wellness(item, &window, observed_at)? {
                rows.push(row);
            }
        }
        grants.extend(["steps", "metric"]);
    }
    ensure!(
        rows.len() <= 4_000,
        "Intervals.icu snapshot record limit exceeded"
    );
    serde_json::from_value(json!({
        "provider":"intervals_icu","lease":lease,"start":start,"end":end,
        "grants":grants,"records":rows
    }))
    .context("failed to build normalized Intervals.icu snapshot")
}

fn intervals_scope(scope: &str, resource: &str) -> bool {
    scope.split(',').map(str::trim).any(|entry| {
        entry
            .split_once(':')
            .is_some_and(|(name, access)| name == resource && matches!(access, "READ" | "WRITE"))
    })
}

async fn fetch(
    client: &Client,
    token: &str,
    oauth: bool,
    origin: &str,
    path: &str,
    oldest: &str,
    newest: &str,
) -> Result<Value> {
    let request = client
        .get(format!("{origin}/api/v1/athlete/0/{path}"))
        .query(&[("oldest", oldest), ("newest", newest)]);
    let request = if oauth {
        request.bearer_auth(token)
    } else {
        request.basic_auth("API_KEY", Some(token))
    };
    let response = request.send().await?;
    if response.status() == reqwest::StatusCode::NO_CONTENT {
        return Ok(json!([]));
    }
    body(response).await
}

struct Window {
    start: DateTime<Utc>,
    end: DateTime<Utc>,
}

impl Window {
    fn contains(&self, start: DateTime<Utc>, end: DateTime<Utc>) -> bool {
        start >= self.start && start < self.end && end >= start && end <= self.end
    }
}

fn intervals_wellness(
    item: &Value,
    window: &Window,
    observed_at: DateTime<Utc>,
) -> Result<Option<Value>> {
    let day: NaiveDate = required_text(&item["id"])?.parse()?;
    let at = day
        .and_hms_opt(0, 0, 0)
        .context("invalid Intervals.icu wellness date")?
        .and_utc();
    if !window.contains(at, at) {
        return Ok(None);
    }
    let mut values = serde_json::Map::new();
    for name in [
        "ctl",
        "atl",
        "rampRate",
        "ctlLoad",
        "atlLoad",
        "weight",
        "restingHR",
        "hrv",
        "hrvSDNN",
        "kcalConsumed",
        "sleepSecs",
        "sleepScore",
        "sleepQuality",
        "avgSleepingHR",
        "soreness",
        "fatigue",
        "stress",
        "mood",
        "motivation",
        "injury",
        "spO2",
        "systolic",
        "diastolic",
        "hydration",
        "hydrationVolume",
        "readiness",
        "baevskySI",
        "bloodGlucose",
        "lactate",
        "bodyFat",
        "abdomen",
        "vo2max",
        "steps",
        "floorsClimbed",
        "respiration",
        "carbohydrates",
        "protein",
        "fatTotal",
    ] {
        if item[name].is_number() {
            values.insert(name.to_owned(), item[name].clone());
        }
    }
    for name in ["menstrualPhase", "menstrualPhasePredicted"] {
        if let Some(value) = item[name].as_str().filter(|value| value.len() <= 64) {
            values.insert(name.to_owned(), json!(value));
        }
    }
    values.insert("date".into(), json!(day));
    let revision = item["updated"]
        .as_str()
        .and_then(|value| {
            value.parse::<DateTime<Utc>>().ok().or_else(|| {
                DateTime::parse_from_str(value, "%Y-%m-%dT%H:%M:%S%.f%z")
                    .ok()
                    .map(|date| date.with_timezone(&Utc))
            })
        })
        .unwrap_or(observed_at);
    Ok(Some(record(
        "metric",
        "Intervals.icu",
        &format!("wellness:{day}"),
        at,
        at,
        revision,
        json!({"metric":"intervals_wellness","provider_data":values,
            "time_precision":"provider_calendar_date"}),
    )))
}

fn intervals_point(item: &Value, window: &Window) -> Result<Option<Value>> {
    let start = date(&item["start_date"])?;
    let seconds = item["elapsed_time"]
        .as_i64()
        .context("missing Intervals.icu duration")?;
    ensure!(
        (0..=604_800).contains(&seconds),
        "invalid Intervals.icu duration"
    );
    let end = start
        .checked_add_signed(Duration::seconds(seconds))
        .context("invalid Intervals.icu workout end")?;
    Ok(
        (window.contains(start, end) && start < end).then_some(record(
            "workout",
            "Intervals.icu",
            required_text(&item["id"])?,
            start,
            end,
            window.end,
            json!({"exercise_type":required_text(&item["type"])?,"title":item["name"],
            "distance_meters":item["distance"],"provider_data":{
                "file_type":item["file_type"],"source":item["source"]
            }}),
        )),
    )
}

fn required_text(value: &Value) -> Result<&str> {
    value
        .as_str()
        .filter(|text| !text.is_empty())
        .context("required Intervals.icu text field is missing")
}

fn date(value: &Value) -> Result<DateTime<Utc>> {
    Ok(required_text(value)?.parse()?)
}

fn record(
    kind: &str,
    source: &str,
    id: &str,
    start: DateTime<Utc>,
    end: DateTime<Utc>,
    modified: DateTime<Utc>,
    payload: Value,
) -> Value {
    json!({"kind":kind,"source":source,"source_id":id,"started_at":start,
        "ended_at":end,"modified_at":modified,"payload":payload})
}

async fn body(response: Response) -> Result<Value> {
    if response.status() == reqwest::StatusCode::TOO_MANY_REQUESTS {
        let retry_at = response
            .headers()
            .get(reqwest::header::RETRY_AFTER)
            .and_then(|value| value.to_str().ok())
            .filter(|value| !value.is_empty() && value.bytes().all(|byte| byte.is_ascii_digit()))
            .and_then(|value| value.parse::<i64>().ok())
            .and_then(Duration::try_seconds)
            .and_then(|delay| Utc::now().checked_add_signed(delay))
            .unwrap_or_else(|| Utc::now() + Duration::minutes(5));
        return Err(ProviderRateLimited { retry_at }.into());
    }
    ensure!(
        response.status().is_success(),
        "Intervals.icu returned HTTP {}",
        response.status().as_u16()
    );
    let mut response = response;
    let mut bytes = Vec::new();
    while let Some(chunk) = response.chunk().await? {
        ensure!(
            bytes.len() + chunk.len() <= MAX_RESPONSE_BYTES,
            "Intervals.icu response exceeds size limit"
        );
        bytes.extend_from_slice(&chunk);
    }
    serde_json::from_slice(&bytes).context("invalid Intervals.icu JSON response")
}

#[cfg(test)]
#[path = "intervals_icu_tests.rs"]
mod tests;
