use anyhow::{ensure, Context, Result};
use avatar_hsm::etl_adapter::Snapshot;
use chrono::{DateTime, Duration, Utc};
use reqwest::{Client, Response};
use serde_json::{json, Value};

#[path = "polar_series.rs"]
mod polar_series;
#[path = "polar_vendor_metrics.rs"]
mod vendor_metrics;

const API: &str = "https://www.polaraccesslink.com/v3";
const MAX_RESPONSE_BYTES: usize = 16 * 1024 * 1024;
const MAX_RECORDS: usize = 24_000;

#[derive(Debug)]
pub struct ProviderRateLimited {
    pub retry_at: DateTime<Utc>,
}

impl std::fmt::Display for ProviderRateLimited {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("Polar returned HTTP 429")
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
    lease: &str,
    start: DateTime<Utc>,
    end: DateTime<Utc>,
) -> Result<Snapshot> {
    collect_from_origin(client, token, lease, start, end, API).await
}

async fn collect_from_origin(
    client: &Client,
    token: &str,
    lease: &str,
    start: DateTime<Utc>,
    end: DateTime<Utc>,
    origin: &str,
) -> Result<Snapshot> {
    ensure!(
        !token.trim().is_empty() && !lease.is_empty(),
        "Polar credential and HSM lease are required"
    );
    ensure!(
        start < end && end - start <= Duration::days(7),
        "invalid Polar collection window"
    );
    let window = Window { start, end };
    let mut rows = exercises_and_sleep(client, token, origin, &window).await?;
    rows.extend(vendor_metrics::collect(client, token, origin, &window).await?);
    ensure!(
        rows.len() <= MAX_RECORDS,
        "Polar snapshot record limit exceeded"
    );
    serde_json::from_value(json!({
        "provider":"polar","lease":lease,"start":start,"end":end,
        "grants":["workout","sleep","heart_rate","steps","metric"],"records":rows
    }))
    .context("failed to build normalized Polar snapshot")
}

async fn exercises_and_sleep(
    client: &Client,
    token: &str,
    origin: &str,
    window: &Window,
) -> Result<Vec<Value>> {
    let exercises = body(
        client
            .get(format!("{origin}/exercises"))
            .bearer_auth(token)
            .query(&[("zones", true)])
            .send()
            .await?,
    )
    .await?;
    let mut rows = records(&exercises)?
        .iter()
        .map(|item| polar_point(item, window))
        .filter_map(Result::transpose)
        .collect::<Result<Vec<_>>>()?;
    let users_origin = format!("{origin}/users");
    let mut polar_nights = std::collections::HashMap::new();
    for (name, field) in [("sleep", "nights"), ("nightly-recharge", "recharges")] {
        let response = client
            .get(format!("{users_origin}/{name}"))
            .bearer_auth(token)
            .send()
            .await?;
        if matches!(
            response.status(),
            reqwest::StatusCode::NOT_FOUND | reqwest::StatusCode::NO_CONTENT
        ) {
            continue;
        }
        let data = body(response).await?;
        for item in records(&data[field])? {
            if name == "sleep" {
                let night = polar_series::Night::parse(item)?;
                let (from, until) = night.utc_bounds();
                let day = required_text(&item["date"])?;
                polar_nights.insert(day.to_owned(), night);
                if from < until && window.contains(from, until) {
                    rows.push(record(
                        "sleep",
                        "Polar",
                        day,
                        from,
                        until,
                        window.end,
                        json!({"stages":night.stages(&item["hypnogram"]),"provider_data":item}),
                    ));
                }
                night.heart_rate(&mut rows, day, &item["heart_rate_samples"], window);
            } else {
                let at = calendar_anchor(&item["date"])?;
                rows.extend(metric(item, name, at, at, window)?);
                if let Some(night) = item["date"].as_str().and_then(|day| polar_nights.get(day)) {
                    night.recharge(&mut rows, item, window);
                }
            }
        }
    }
    Ok(rows)
}

fn polar_point(item: &Value, window: &Window) -> Result<Option<Value>> {
    let local: chrono::NaiveDateTime = required_text(&item["start_time"])?.parse()?;
    let offset_minutes = item["start_time_utc_offset"]
        .as_i64()
        .context("missing Polar timezone offset")?;
    ensure!(
        (-1440..=1440).contains(&offset_minutes),
        "invalid Polar timezone offset"
    );
    let start = local.and_utc() - Duration::minutes(offset_minutes);
    let seconds = polar_duration(&item["duration"])
        .filter(|seconds| *seconds >= 0.001)
        .context("invalid Polar workout duration")?;
    let end = start + Duration::milliseconds((seconds * 1000.0).round() as i64);
    if !window.contains(start, end) {
        return Ok(None);
    }
    Ok(Some(record(
        "workout",
        item["device"].as_str().unwrap_or("Polar"),
        required_text(&item["id"])?,
        start,
        end,
        date(&item["upload_time"])?,
        polar_workout_payload(item, seconds, offset_minutes)?,
    )))
}

pub(super) fn normalize_workout_detail(item: &Value, requested_id: &str) -> Result<Value> {
    ensure!(
        item["id"].as_str() == Some(requested_id),
        "Polar exercise ID mismatch"
    );
    let window = Window {
        start: DateTime::<Utc>::MIN_UTC,
        end: Utc::now(),
    };
    polar_point(item, &window)?.context("Polar exercise detail unavailable")
}

fn polar_duration(input: &Value) -> Option<f64> {
    let duration = input.as_str()?.strip_prefix("PT")?;
    let mut remaining = duration;
    let mut seconds = 0.0;
    for (unit, multiplier) in [('H', 3600.0), ('M', 60.0), ('S', 1.0)] {
        if let Some((value, rest)) = remaining.split_once(unit) {
            let value = value.parse::<f64>().ok()?;
            if !value.is_finite() || value < 0.0 {
                return None;
            }
            seconds += value * multiplier;
            remaining = rest;
        }
    }
    (!duration.is_empty() && remaining.is_empty() && (0.0..=604_800.0).contains(&seconds))
        .then_some(seconds)
}

fn polar_workout_payload(item: &Value, seconds: f64, offset_minutes: i64) -> Result<Value> {
    let mut payload = json!({"exercise_type":required_text(&item["sport"])?,
        "distance_meters":item["distance"],"provider_heart_rate":item["heart_rate"],
        "provider_timezone_offset_minutes":offset_minutes,"duration_seconds":seconds,
        "provider_data":{"has_route":item["has_route"],"detailed_sport_info":item["detailed_sport_info"],
            "training_load_pro":item["training_load_pro"]}});
    for (field, target) in [
        ("calories", "calories_kcal"),
        ("training_load", "training_load"),
        ("running-index", "running_index"),
    ] {
        if let Some(value) = item[field].as_f64().filter(|value| *value >= 0.0) {
            payload[target] = json!(value);
        }
    }
    if let Some(zones) = item["heart_rate_zones"].as_array() {
        let normalized: std::collections::BTreeMap<_, _> = zones
            .iter()
            .filter_map(|zone| {
                let index = zone["index"].as_u64().filter(|index| *index <= 16)?;
                let duration =
                    polar_duration(&zone["in-zone"]).filter(|duration| *duration <= seconds)?;
                let lower = zone["lower-limit"].as_u64().filter(|limit| *limit <= 350);
                let upper = zone["upper-limit"]
                    .as_u64()
                    .filter(|limit| (1..=350).contains(limit));
                if lower.zip(upper).is_some_and(|(low, high)| low > high) {
                    return None;
                }
                Some((
                    index,
                    json!({"index":index,"duration_seconds":duration,
                    "lower_bpm":lower,"upper_bpm":upper}),
                ))
            })
            .collect();
        payload["heart_rate_zones"] = json!(normalized.into_values().collect::<Vec<_>>());
    }
    Ok(payload)
}

fn calendar_anchor(input: &Value) -> Result<DateTime<Utc>> {
    let day: chrono::NaiveDate = required_text(input)?.parse()?;
    Ok(day
        .and_hms_opt(0, 0, 0)
        .context("invalid Polar provider day")?
        .and_utc())
}

fn metric(
    item: &Value,
    name: &str,
    start: DateTime<Utc>,
    end: DateTime<Utc>,
    window: &Window,
) -> Result<Vec<Value>> {
    if !window.contains(start, end) {
        return Ok(Vec::new());
    }
    let id = item["id"]
        .as_str()
        .map(str::to_owned)
        .or_else(|| item["id"].as_i64().map(|id| id.to_string()))
        .unwrap_or_else(|| start.to_rfc3339());
    Ok(vec![record(
        "metric",
        "Polar",
        &format!("{name}:{id}"),
        start,
        end,
        window.end,
        json!({"metric":name,"provider_data":item}),
    )])
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

fn required_text(value: &Value) -> Result<&str> {
    value
        .as_str()
        .filter(|text| !text.is_empty())
        .context("required Polar text field is missing")
}

fn date(value: &Value) -> Result<DateTime<Utc>> {
    Ok(required_text(value)?.parse()?)
}

fn records(value: &Value) -> Result<&Vec<Value>> {
    value.as_array().context("missing Polar record list")
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
    if matches!(
        response.status(),
        reqwest::StatusCode::NOT_FOUND
            | reqwest::StatusCode::NO_CONTENT
            | reqwest::StatusCode::FORBIDDEN
    ) {
        anyhow::bail!(
            "optional Polar endpoint returned HTTP {}",
            response.status().as_u16()
        );
    }
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
        "Polar returned HTTP {}",
        response.status().as_u16()
    );
    let mut response = response;
    let mut bytes = Vec::new();
    while let Some(chunk) = response.chunk().await? {
        ensure!(
            bytes.len() + chunk.len() <= MAX_RESPONSE_BYTES,
            "Polar response exceeds size limit"
        );
        bytes.extend_from_slice(&chunk);
    }
    serde_json::from_slice(&bytes).context("invalid Polar JSON response")
}

#[cfg(test)]
#[path = "polar_tests.rs"]
mod tests;
