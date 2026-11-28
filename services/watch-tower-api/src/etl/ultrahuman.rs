use anyhow::{ensure, Context, Result};
use avatar_hsm::etl_adapter::Snapshot;
use chrono::{DateTime, Duration, NaiveDate, Utc};
use reqwest::{Client, Response};
use serde_json::{json, Value};
use std::collections::BTreeMap;

const API: &str = "https://partner.ultrahuman.com/api/partners/v1/user_data/metrics";
const MAX_RESPONSE_BYTES: usize = 8 * 1024 * 1024;
const MAX_RECORDS: usize = 24_000;

#[derive(Debug)]
pub struct ProviderRateLimited {
    pub retry_at: DateTime<Utc>,
}

impl std::fmt::Display for ProviderRateLimited {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("Ultrahuman returned HTTP 429")
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
        "Ultrahuman credential and HSM lease are required"
    );
    ensure!(
        start < end && end - start <= Duration::days(1),
        "invalid Ultrahuman collection window"
    );
    let window = Window { start, end };
    let mut day = (start - Duration::days(1)).date_naive();
    let last_day = (end + Duration::days(1)).date_naive();
    let mut records = Vec::new();
    let mut series = BTreeMap::new();
    while day <= last_day {
        let response = client
            .get(origin)
            .bearer_auth(token)
            .query(&[("date", day.to_string())])
            .send()
            .await?;
        let response = check_response(response)?;
        let body = read_json(response).await?;
        let items = body["data"]["metric_data"]
            .as_array()
            .context("Ultrahuman response lacks metric_data")?;
        for item in items {
            append_metric(&mut records, &mut series, item, day, &window)?;
        }
        ensure!(
            records.len() + series.len() <= MAX_RECORDS,
            "Ultrahuman snapshot record limit exceeded"
        );
        day = day.succ_opt().context("invalid Ultrahuman date")?;
    }
    records.extend(series.into_values());
    serde_json::from_value(json!({
        "provider":"ultrahuman",
        "lease":lease,
        "start":start,
        "end":end,
        "grants":["sleep","heart_rate","metric"],
        "records":records
    }))
    .context("failed to build normalized Ultrahuman snapshot")
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

fn append_metric(
    records: &mut Vec<Value>,
    series: &mut BTreeMap<(&'static str, DateTime<Utc>), Value>,
    item: &Value,
    day: NaiveDate,
    window: &Window,
) -> Result<()> {
    let kind = item["type"]
        .as_str()
        .filter(|value| !value.is_empty())
        .context("Ultrahuman metric type is missing")?;
    let data = &item["object"];
    if kind == "Sleep" {
        if let Some(record) = sleep_point(data, window)? {
            records.push(record);
        }
        return Ok(());
    }
    if !data.is_object() {
        return Ok(());
    }
    let anchor = day.and_hms_opt(0, 0, 0).unwrap().and_utc();
    if window.contains(anchor, anchor) {
        records.push(record(
            "metric",
            "Ultrahuman",
            &format!("{}:{}", kind, source_id(data, anchor)),
            anchor,
            anchor,
            window.end,
            json!({"metric":kind,"provider_data":data}),
        ));
    }
    let (name, unit) = match kind {
        "hr" => ("heart_rate", "bpm"),
        "hrv" => ("hrv_sample", "ms"),
        "temp" => ("temperature_sample", "°C"),
        "steps" => ("step_sample", "steps"),
        _ => return Ok(()),
    };
    for sample in data["values"].as_array().into_iter().flatten() {
        let Ok(at) = epoch_seconds(&sample["timestamp"]) else {
            continue;
        };
        let Some(value) = sample["value"].as_f64().filter(|value| value.is_finite()) else {
            continue;
        };
        if !window.contains(at, at)
            || (name == "heart_rate" && !(1.0..=350.0).contains(&value))
            || (name == "step_sample" && sample["value"].as_u64().is_none())
        {
            continue;
        }
        let (record_kind, payload) = if name == "heart_rate" {
            (
                "heart_rate",
                json!({"unit":unit,"samples":[{"at":at,"bpm":value}]}),
            )
        } else {
            (
                "metric",
                json!({"metric":name,"provider_data":{"value":sample["value"],"unit":unit}}),
            )
        };
        let source_id = format!("{name}:{}", at.timestamp_millis());
        series.insert(
            (name, at),
            record(
                record_kind,
                "Ultrahuman",
                &source_id,
                at,
                at,
                window.end,
                payload,
            ),
        );
    }
    Ok(())
}

fn source_id(data: &Value, at: DateTime<Utc>) -> String {
    data["id"]
        .as_str()
        .map(str::to_owned)
        .or_else(|| data["cycle_id"].as_i64().map(|value| value.to_string()))
        .or_else(|| data["id"].as_i64().map(|value| value.to_string()))
        .unwrap_or_else(|| at.to_rfc3339())
}

fn sleep_point(item: &Value, window: &Window) -> Result<Option<Value>> {
    if item["bedtime_start"].is_null() || item["bedtime_end"].is_null() {
        return Ok(None);
    }
    let started = epoch_seconds(&item["bedtime_start"])?;
    let ended = epoch_seconds(&item["bedtime_end"])?;
    if started >= ended || !window.contains(started, ended) {
        return Ok(None);
    }
    let id = source_id(item, started);
    Ok(Some(record(
        "sleep",
        "Ultrahuman",
        &id,
        started,
        ended,
        window.end,
        sleep_payload(item, started, ended),
    )))
}

fn sleep_payload(item: &Value, started: DateTime<Utc>, ended: DateTime<Utc>) -> Value {
    let mut intervals = Vec::new();
    for interval in item["sleep_graph"]["data"].as_array().into_iter().flatten() {
        let (Ok(start), Ok(end)) = (
            epoch_seconds(&interval["start"]),
            epoch_seconds(&interval["end"]),
        ) else {
            continue;
        };
        if start >= started && end <= ended && start < end {
            let stage = match interval["type"].as_str() {
                Some("deep_sleep") => 5,
                Some("light_sleep") => 4,
                Some("rem_sleep") => 6,
                Some("awake") => 1,
                _ => 0,
            };
            intervals.push((start, end, stage));
        }
    }
    intervals.sort_unstable();
    let mut stages = Vec::new();
    let mut previous_end = started;
    for (start, end, stage) in intervals {
        if start >= previous_end {
            previous_end = end;
            stages.push(json!({"start":start,"end":end,"stage":stage}));
        }
    }
    let truncated = stages.len() > 500;
    stages.truncate(500);
    let mut summary = json!({"stage_seconds":item["sleep_stages"]});
    for stage in item["sleep_stages"].as_array().into_iter().flatten() {
        let field = match stage["type"].as_str() {
            Some("deep_sleep") => "deep_seconds",
            Some("light_sleep") => "light_seconds",
            Some("rem_sleep") => "rem_seconds",
            Some("awake") => "awake_seconds",
            _ => continue,
        };
        if let Some(seconds) = stage["stage_time"].as_u64() {
            summary[field] = json!(seconds);
        }
    }
    for metric in item["quick_metrics"].as_array().into_iter().flatten() {
        match metric["type"].as_str() {
            Some("time_in_bed") if metric["value"].as_u64().is_some() => {
                summary["time_in_bed_seconds"] = metric["value"].clone();
            }
            Some("total_sleep") if metric["value"].as_u64().is_some() => {
                summary["total_sleep_seconds"] = metric["value"].clone();
            }
            Some("sleep_efic")
                if metric["value"]
                    .as_f64()
                    .is_some_and(|value| (0.0..=100.0).contains(&value)) =>
            {
                summary["efficiency_percent"] = metric["value"].clone();
            }
            _ => {}
        }
    }
    let mut payload =
        json!({"stages":stages,"provider_stage_summary":summary,"provider_data":item});
    if let Some(spo2) = item["spo2"]["value"]
        .as_f64()
        .filter(|value| *value > 0.0 && *value <= 100.0)
    {
        payload["sleep_metrics"] = json!({"average_spo2_percent":spo2});
    }
    if truncated {
        payload["stages_truncated"] = json!(true);
    }
    payload
}

fn epoch_seconds(value: &Value) -> Result<DateTime<Utc>> {
    let seconds = value.as_f64().context("missing Ultrahuman timestamp")?;
    ensure!(
        seconds.is_finite() && seconds > 0.0,
        "invalid Ultrahuman timestamp"
    );
    DateTime::from_timestamp_millis((seconds * 1000.0).round() as i64)
        .context("invalid Ultrahuman timestamp")
}

fn record(
    kind: &str,
    source: &str,
    id: &str,
    started: DateTime<Utc>,
    ended: DateTime<Utc>,
    modified: DateTime<Utc>,
    payload: Value,
) -> Value {
    json!({"kind":kind,"source":source,"source_id":id,"started_at":started,
        "ended_at":ended,"modified_at":modified,"payload":payload})
}

fn check_response(response: Response) -> Result<Response> {
    if response.status() == reqwest::StatusCode::TOO_MANY_REQUESTS {
        let retry_at = response
            .headers()
            .get(reqwest::header::RETRY_AFTER)
            .and_then(|header| header.to_str().ok())
            .and_then(|value| {
                value
                    .parse::<i64>()
                    .ok()
                    .and_then(Duration::try_seconds)
                    .and_then(|delay| Utc::now().checked_add_signed(delay))
            })
            .unwrap_or_else(|| Utc::now() + Duration::minutes(5));
        return Err(ProviderRateLimited { retry_at }.into());
    }
    ensure!(
        response.status().is_success(),
        "Ultrahuman returned HTTP {}",
        response.status().as_u16()
    );
    Ok(response)
}

async fn read_json(response: Response) -> Result<Value> {
    let mut response = response;
    let mut bytes = Vec::new();
    while let Some(chunk) = response.chunk().await? {
        ensure!(
            bytes.len() + chunk.len() <= MAX_RESPONSE_BYTES,
            "Ultrahuman response exceeds size limit"
        );
        bytes.extend_from_slice(&chunk);
    }
    serde_json::from_slice(&bytes).context("invalid Ultrahuman JSON response")
}

#[cfg(test)]
#[path = "ultrahuman_tests.rs"]
mod tests;
