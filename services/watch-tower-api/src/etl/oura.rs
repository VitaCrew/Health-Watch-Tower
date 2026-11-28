use anyhow::{bail, ensure, Context, Result};
use avatar_hsm::etl_adapter::Snapshot;
use chrono::{DateTime, Duration, NaiveDate, Utc};
use reqwest::{Client, RequestBuilder, Response};
use serde_json::{json, Value};
use std::collections::HashSet;

const API: &str = "https://api.ouraring.com/v2/usercollection";
const MAX_RESPONSE_BYTES: usize = 8 * 1024 * 1024;
const MAX_RECORDS: usize = 4_000;

#[derive(Debug)]
pub struct ProviderRateLimited {
    pub retry_at: DateTime<Utc>,
}

impl std::fmt::Display for ProviderRateLimited {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("Oura returned HTTP 429")
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
        "Oura credential and HSM lease are required"
    );
    ensure!(
        start < end && end - start <= Duration::days(7),
        "invalid Oura collection window"
    );
    let window = Window { start, end };
    let scope = scope.filter(|scope| !scope.trim().is_empty());
    let from = (start - Duration::days(1)).format("%Y-%m-%d").to_string();
    let to = (end + Duration::days(1)).format("%Y-%m-%d").to_string();
    let mut rows = Vec::new();
    let mut grants = Vec::new();
    let mut deleted_sleep = HashSet::new();

    for (kind, required) in [("sleep", "daily"), ("workout", "workout")] {
        if !has_scope(scope, required) {
            continue;
        }
        grants.push(kind);
        let request = client
            .get(format!("{origin}/{kind}"))
            .bearer_auth(token)
            .query(&[("start_date", &from), ("end_date", &to)]);
        rows.extend(
            cursor_records(request, "data", |item| {
                if kind == "sleep" && item["type"] == "deleted" && scope.is_some() {
                    if let Some(id) = item["id"].as_str().filter(|id| valid_id(id)) {
                        deleted_sleep.insert(id.to_owned());
                    }
                    return Ok(None);
                }
                oura_point(item, kind, &window)
            })
            .await?,
        );
    }

    for (endpoint, required) in [
        ("daily_activity", "daily"),
        ("daily_sleep", "daily"),
        ("daily_readiness", "daily"),
        ("daily_stress", "daily"),
        ("daily_resilience", "daily"),
        ("sleep_time", "daily"),
        ("daily_spo2", "spo2"),
        ("daily_cardiovascular_age", "heart_health"),
        ("vO2_max", "heart_health"),
        ("session", "session"),
        ("enhanced_tag", "tag"),
        ("rest_mode_period", "daily"),
    ] {
        if !has_scope(scope, required)
            || (scope.is_none() && (required != "daily" || endpoint == "rest_mode_period"))
        {
            continue;
        }
        let request = client
            .get(format!("{origin}/{endpoint}"))
            .bearer_auth(token)
            .query(&[("start_date", &from), ("end_date", &to)]);
        let first_page = if endpoint == "rest_mode_period" {
            let response = request
                .try_clone()
                .context("Oura request cannot be paginated")?
                .send()
                .await?;
            if response.status() == reqwest::StatusCode::FORBIDDEN {
                continue;
            }
            Some(response)
        } else {
            None
        };
        rows.extend(
            cursor_records_from(request, first_page, |item| {
                let (at, until) = match endpoint {
                    "session" => (date(&item["start_datetime"])?, date(&item["end_datetime"])?),
                    "enhanced_tag" | "rest_mode_period" => {
                        required_text(&item["id"])?;
                        let at = oura_day(&item["start_day"])?;
                        (at, at)
                    }
                    _ => {
                        let at = oura_daily_timestamp(item)?;
                        (at, at)
                    }
                };
                let metric_name = match endpoint {
                    "vO2_max" => "vo2_max",
                    "session" => "guided_session",
                    name => name,
                };
                let mut row = metric(item, metric_name, at, until, &window)?;
                if let Some(row) = &mut row {
                    add_activity_classes(&mut row["payload"]);
                }
                Ok(row)
            })
            .await?,
        );
        if !grants.contains(&"metric") {
            grants.push("metric");
        }
    }

    if scope.is_some() && has_scope(scope, "heartrate") {
        let request = client
            .get(format!("{origin}/heartrate"))
            .bearer_auth(token)
            .query(&[
                ("start_datetime", start.to_rfc3339()),
                ("end_datetime", end.to_rfc3339()),
            ]);
        rows.extend(
            cursor_records(request, "data", |item| {
                let at = date(&item["timestamp"])?;
                let bpm = integer(&item["bpm"])?;
                ensure!((1..=350).contains(&bpm), "invalid Oura heart rate");
                let source = format!("Oura:{}", item["source"].as_str().unwrap_or("unknown"));
                Ok(window.contains(at, at).then_some(record(
                    "heart_rate",
                    &source,
                    &at.to_rfc3339(),
                    at,
                    at,
                    end,
                    json!({"unit":"bpm","samples":[{"at":at,"bpm":bpm}]}),
                )))
            })
            .await?,
        );
        grants.push("heart_rate");
    }
    ensure!(
        rows.len() + deleted_sleep.len() <= MAX_RECORDS,
        "Oura snapshot record limit exceeded"
    );
    for row in &mut rows {
        row["modified_at"] = json!(observed_at);
    }
    let deletions = (!deleted_sleep.is_empty()).then(|| {
        json!({
            "observed_at":observed_at,"ids":deleted_sleep.into_iter().collect::<Vec<_>>()
        })
    });
    let snapshot: Snapshot = serde_json::from_value(json!({
        "provider":"oura","lease":lease,"start":start,"end":end,
        "grants":grants,"records":rows,
        "oura_sleep_deletions":deletions
    }))
    .context("failed to build normalized Oura snapshot")?;
    Ok(snapshot)
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

fn has_scope(scope: Option<&str>, expected: &str) -> bool {
    scope.filter(|scope| !scope.is_empty()).is_none_or(|scope| {
        scope
            .split(|character: char| character.is_whitespace() || character == ',')
            .any(|granted| granted == expected)
    })
}

fn metric(
    item: &Value,
    name: &str,
    start: DateTime<Utc>,
    end: DateTime<Utc>,
    window: &Window,
) -> Result<Option<Value>> {
    if !window.contains(start, end) {
        return Ok(None);
    }
    let id = item["id"]
        .as_str()
        .map(str::to_owned)
        .or_else(|| item["id"].as_i64().map(|id| id.to_string()))
        .unwrap_or_else(|| start.to_rfc3339());
    Ok(Some(record(
        "metric",
        "Oura",
        &format!("{name}:{id}"),
        start,
        end,
        window.end,
        json!({"metric":name,"provider_data":item}),
    )))
}

fn oura_daily_timestamp(item: &Value) -> Result<DateTime<Utc>> {
    date(&item["timestamp"]).or_else(|_| oura_day(&item["day"]))
}

fn add_activity_classes(payload: &mut Value) {
    if payload["metric"] != "daily_activity" {
        return;
    }
    let raw = &payload["provider_data"];
    let Some(codes) = raw["class_5_min"].as_str() else {
        return;
    };
    let classes = ["non_wear", "rest", "inactive", "low", "medium", "high"];
    let slots: Vec<_> = codes
        .chars()
        .map(|code| {
            code.to_digit(10)
                .and_then(|code| classes.get(code as usize))
                .copied()
        })
        .collect();
    if slots.iter().any(Option::is_some) {
        payload["activity_classification"] = json!({
            "date":raw["day"],"interval_seconds":300,"slots":slots,
            "time_basis":"provider_activity_day"
        });
    }
}

fn oura_day(value: &Value) -> Result<DateTime<Utc>> {
    let day: NaiveDate = required_text(value)?.parse()?;
    Ok(day
        .and_hms_opt(0, 0, 0)
        .context("invalid Oura day")?
        .and_utc())
}

fn oura_point(item: &Value, kind: &str, window: &Window) -> Result<Option<Value>> {
    if kind == "sleep" && item["type"] == "deleted" {
        return Ok(None);
    }
    let (start, end, payload) = if kind == "sleep" {
        let start = date(&item["bedtime_start"])?;
        let end = date(&item["bedtime_end"])?;
        let (resolution, stages) = oura_stages(item, start, end);
        (
            start,
            end,
            json!({"stages":stages,"stage_resolution_seconds":resolution,
                "provider_total_sleep_seconds":item["total_sleep_duration"],"provider_data":item}),
        )
    } else {
        let start = date(&item["start_datetime"])?;
        let end = date(&item["end_datetime"])?;
        (
            start,
            end,
            json!({"exercise_type":required_text(&item["activity"])?,"title":item["label"],
                "duration_seconds":(end-start).num_seconds(),"distance_meters":item["distance"],
                "calories_kcal":item["calories"],"provider_data":item}),
        )
    };
    Ok(
        (start < end && window.contains(start, end)).then_some(record(
            kind,
            "Oura",
            required_text(&item["id"])?,
            start,
            end,
            window.end,
            payload,
        )),
    )
}

fn oura_stages(
    item: &Value,
    start: DateTime<Utc>,
    end: DateTime<Utc>,
) -> (Option<i64>, Vec<Value>) {
    for (field, resolution) in [("sleep_phase_30_sec", 30), ("sleep_phase_5_min", 300)] {
        let Some(phases) = item[field].as_str() else {
            continue;
        };
        let mut at = start;
        let mut intervals: Vec<(i32, DateTime<Utc>, DateTime<Utc>)> = Vec::new();
        for phase in phases.bytes() {
            if at >= end || intervals.len() > 500 {
                break;
            }
            let until = (at + Duration::seconds(resolution)).min(end);
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

fn required_text(value: &Value) -> Result<&str> {
    value
        .as_str()
        .filter(|text| !text.is_empty())
        .context("required Oura text field is missing")
}

fn integer(value: &Value) -> Result<u64> {
    value
        .as_u64()
        .or_else(|| value.as_str()?.parse().ok())
        .context("invalid Oura whole number")
}

fn date(value: &Value) -> Result<DateTime<Utc>> {
    Ok(required_text(value)?.parse()?)
}

fn valid_id(id: &str) -> bool {
    !id.is_empty()
        && id.len() <= 128
        && id
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || b"-_".contains(&byte))
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

async fn cursor_records(
    request: RequestBuilder,
    item_key: &str,
    mut project: impl FnMut(&Value) -> Result<Option<Value>>,
) -> Result<Vec<Value>> {
    cursor_records_from(request, None, |item| {
        if item_key != "data" {
            bail!("unsupported Oura page shape");
        }
        project(item)
    })
    .await
}

async fn cursor_records_from(
    request: RequestBuilder,
    mut first_page: Option<Response>,
    mut project: impl FnMut(&Value) -> Result<Option<Value>>,
) -> Result<Vec<Value>> {
    let mut cursor = None::<String>;
    let mut seen = HashSet::new();
    let mut rows = Vec::new();
    loop {
        let response = match first_page.take() {
            Some(response) => response,
            None => {
                let mut page = request
                    .try_clone()
                    .context("Oura request cannot be paginated")?;
                if let Some(cursor) = &cursor {
                    page = page.query(&[("next_token", cursor)]);
                }
                page.send().await?
            }
        };
        let body = body(response).await?;
        for item in body["data"]
            .as_array()
            .context("Oura response lacks data")?
        {
            if let Some(row) = project(item)? {
                rows.push(row);
            }
        }
        ensure!(
            rows.len() <= MAX_RECORDS,
            "Oura snapshot record limit exceeded"
        );
        let Some(next) = body["next_token"].as_str().filter(|next| !next.is_empty()) else {
            return Ok(rows);
        };
        ensure!(
            seen.len() < 32 && seen.insert(next.to_owned()),
            "incomplete Oura pagination"
        );
        cursor = Some(next.to_owned());
    }
}

fn retry_after(header: Option<&str>, now: DateTime<Utc>) -> DateTime<Utc> {
    header
        .filter(|value| !value.is_empty() && value.bytes().all(|byte| byte.is_ascii_digit()))
        .and_then(|value| value.parse::<i64>().ok())
        .and_then(Duration::try_seconds)
        .and_then(|delay| now.checked_add_signed(delay))
        .unwrap_or_else(|| now + Duration::minutes(5))
}

async fn body(response: Response) -> Result<Value> {
    if response.status() == reqwest::StatusCode::TOO_MANY_REQUESTS {
        let retry_at = retry_after(
            response
                .headers()
                .get(reqwest::header::RETRY_AFTER)
                .and_then(|value| value.to_str().ok()),
            Utc::now(),
        );
        return Err(ProviderRateLimited { retry_at }.into());
    }
    ensure!(
        response.status().is_success(),
        "Oura returned HTTP {}",
        response.status().as_u16()
    );
    let mut response = response;
    let mut bytes = Vec::new();
    while let Some(chunk) = response.chunk().await? {
        ensure!(
            bytes.len() + chunk.len() <= MAX_RESPONSE_BYTES,
            "Oura response exceeds size limit"
        );
        bytes.extend_from_slice(&chunk);
    }
    serde_json::from_slice(&bytes).context("invalid Oura JSON response")
}

#[cfg(test)]
#[path = "oura_tests.rs"]
mod tests;
