use anyhow::{bail, ensure, Context, Result};
use avatar_hsm::etl_adapter::Snapshot;
use chrono::{DateTime, Duration as CalendarDuration, Utc};
use reqwest::{Client, Response};
use serde_json::{json, Value};
use std::{collections::HashSet, time::Duration};

const MAX_RECORDS: usize = 24_000;
const ORIGIN: &str = "https://health.googleapis.com";

#[path = "google_metrics.rs"]
mod google_metrics;
#[derive(Clone, Copy)]
struct Window {
    start: DateTime<Utc>,
    end: DateTime<Utc>,
}
impl Window {
    fn contains(&self, start: DateTime<Utc>, end: DateTime<Utc>) -> bool {
        start >= self.start && start < self.end && end >= start && end <= self.end
    }
}

#[derive(Debug)]
pub struct ProviderRateLimited {
    pub retry_at: DateTime<Utc>,
}
impl std::fmt::Display for ProviderRateLimited {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Google Health returned HTTP 429")
    }
}
impl std::error::Error for ProviderRateLimited {}
pub fn provider_retry_at(error: &anyhow::Error) -> Option<DateTime<Utc>> {
    error
        .downcast_ref::<ProviderRateLimited>()
        .map(|error| error.retry_at)
}

async fn body(response: Response) -> Result<Value> {
    let status = response.status();
    if status == reqwest::StatusCode::TOO_MANY_REQUESTS {
        let retry_at = response
            .headers()
            .get(reqwest::header::RETRY_AFTER)
            .and_then(|v| v.to_str().ok())
            .and_then(|v| {
                v.parse::<i64>()
                    .ok()
                    .and_then(CalendarDuration::try_seconds)
                    .and_then(|d| Utc::now().checked_add_signed(d))
                    .or_else(|| {
                        DateTime::parse_from_rfc2822(v)
                            .ok()
                            .map(|d| d.with_timezone(&Utc))
                    })
            })
            .unwrap_or_else(|| Utc::now() + CalendarDuration::minutes(5));
        return Err(ProviderRateLimited { retry_at }.into());
    }
    ensure!(
        status.is_success(),
        "Google Health returned HTTP {}",
        status.as_u16()
    );
    let mut response = response;
    let mut bytes = Vec::new();
    while let Some(chunk) = response
        .chunk()
        .await
        .map_err(reqwest::Error::without_url)?
    {
        ensure!(
            bytes.len() + chunk.len() <= 8 * 1024 * 1024,
            "Google Health response exceeds limit"
        );
        bytes.extend_from_slice(&chunk);
    }
    Ok(serde_json::from_slice(&bytes)?)
}
fn required_text(value: &Value) -> Result<&str> {
    value
        .as_str()
        .filter(|v| !v.is_empty())
        .context("required text field missing")
}
fn integer(value: &Value) -> Result<u64> {
    value
        .as_u64()
        .or_else(|| value.as_str()?.parse().ok())
        .context("invalid whole number")
}
fn date(value: &Value) -> Result<DateTime<Utc>> {
    Ok(required_text(value)?.parse()?)
}
fn records(value: &Value) -> Result<&Vec<Value>> {
    value.as_array().context("missing provider record list")
}
fn record(
    kind: &str,
    source: &str,
    id: &str,
    start: DateTime<Utc>,
    end: DateTime<Utc>,
    revision: DateTime<Utc>,
    payload: Value,
) -> Value {
    json!({"kind":kind,"source":source,"source_id":id,"started_at":start,"ended_at":end,"modified_at":revision,"payload":payload})
}
fn google_page_token(response: &Value) -> Result<Option<&str>> {
    ensure!(
        response.is_object() && response.get("error").is_none(),
        "invalid Google Health page"
    );
    match response.get("nextPageToken") {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(token)) => Ok((!token.is_empty()).then_some(token.as_str())),
        _ => bail!("invalid Google Health page token"),
    }
}

pub async fn collect_window(
    client: &Client,
    token: &str,
    scope: Option<&str>,
    lease: &str,
    start: DateTime<Utc>,
    end: DateTime<Utc>,
) -> Result<Snapshot> {
    collect_from_origin(client, token, scope, lease, start, end, ORIGIN).await
}

async fn collect_from_origin(
    client: &Client,
    token: &str,
    scope: Option<&str>,
    lease: &str,
    start: DateTime<Utc>,
    end: DateTime<Utc>,
    origin: &str,
) -> Result<Snapshot> {
    ensure!(
        !token.trim().is_empty() && !lease.trim().is_empty(),
        "Google Health credential and HSM lease are required"
    );
    ensure!(
        start < end && end - start <= CalendarDuration::days(1),
        "invalid Google Health collection window"
    );
    let beta = std::env::var("AVATAR_GOOGLE_HEALTH_SKIN_TEMPERATURE_BETA").as_deref() == Ok("true");
    let ((mut rows, mut grants), (metrics, coverage)) = tokio::try_join!(
        google_records(client, token, start, end, scope, origin),
        google_metrics::fetch_from(client, token, start, end, scope, origin, beta)
    )?;
    rows.extend(metrics);
    if !coverage.is_empty() {
        grants.push("metric");
    }
    ensure!(
        rows.len() <= MAX_RECORDS,
        "Google Health combined record limit exceeded"
    );
    let mut snapshot: Snapshot = serde_json::from_value(
        json!({"provider":"google_health","lease":lease,"start":start,"end":end,"grants":grants,"records":rows}),
    )?;
    snapshot.set_google_metric_coverage(coverage);
    Ok(snapshot)
}
async fn google_records(
    client: &Client,
    token: &str,
    start: DateTime<Utc>,
    end: DateTime<Utc>,
    scope: Option<&str>,
    origin: &str,
) -> Result<(Vec<Value>, Vec<&'static str>)> {
    let window = Window { start, end };
    let mut output = Vec::new();
    let mut grants = Vec::new();
    for (endpoint, field, filter_field, kind) in [
        ("steps", "steps", "steps.interval.start_time", "steps"),
        (
            "heart-rate",
            "heartRate",
            "heart_rate.sample_time.physical_time",
            "heart_rate",
        ),
        ("sleep", "sleep", "sleep.interval.end_time", "sleep"),
        (
            "exercise",
            "exercise",
            "exercise.interval.civil_start_time",
            "workout",
        ),
    ] {
        let required = match kind {
            "heart_rate" => "health_metrics_and_measurements",
            "sleep" => "sleep",
            _ => "activity_and_fitness",
        };
        if !google_metrics::granted(scope, required) {
            continue;
        }
        grants.push(kind);
        let base = format!("{origin}/v4/users/me/dataTypes/{endpoint}/dataPoints");
        let url = if matches!(kind, "sleep" | "workout") {
            format!("{base}:reconcile")
        } else {
            base
        };
        let (from, until) = if kind == "workout" {
            // Korean test profile. Include an extra civil day in either direction for travel and apply the exact UTC window below.
            (
                (start - CalendarDuration::days(1))
                    .format("%Y-%m-%dT%H:%M:%S")
                    .to_string(),
                (end + CalendarDuration::days(1))
                    .format("%Y-%m-%dT%H:%M:%S")
                    .to_string(),
            )
        } else {
            (start.to_rfc3339(), end.to_rfc3339())
        };
        let filter = format!("{filter_field} >= \"{from}\" AND {filter_field} < \"{until}\"");
        let page_size = if matches!(kind, "sleep" | "workout") {
            "25"
        } else {
            "1000"
        };
        let mut page = String::new();
        let mut seen = HashSet::new();
        loop {
            let mut request = client.get(&url).bearer_auth(token).query(&[
                ("filter", filter.as_str()),
                ("pageSize", page_size),
                (
                    "dataSourceFamily",
                    "users/me/dataSourceFamilies/google-wearables",
                ),
            ]);
            if !page.is_empty() {
                request = request.query(&[("pageToken", page.as_str())]);
            }
            let result = body(request.send().await?).await?;
            let next = google_page_token(&result)?;
            if let Some(items) = result.get("dataPoints") {
                for item in records(items)? {
                    if let Some(mut row) = google_point(item, field, kind, &window)? {
                        // Reconciled sessions omit DataSource. Read it from the same resource
                        // without introducing overlapping sessions or changing stored keys.
                        if matches!(kind, "sleep" | "workout") && !item["dataSource"].is_object() {
                            if let Some(source) = google_session_source(
                                client,
                                token,
                                origin,
                                endpoint,
                                item["dataPointName"].as_str(),
                            )
                            .await?
                            {
                                row["payload"]["provider_source"] = source;
                            }
                        }
                        output.push(row);
                    }
                }
            }
            ensure!(
                output.len() <= MAX_RECORDS,
                "Google Health snapshot exceeds the record limit"
            );
            let Some(next) = next else {
                break;
            };
            ensure!(
                seen.len() < 32 && seen.insert(next.to_owned()),
                "incomplete Google Health pagination"
            );
            page = next.to_owned();
        }
    }
    // Google interval and sample records can lack resource IDs; their source and time are used as a fallback key.
    let mut keys = HashSet::new();
    ensure!(
        output.iter().all(|row| keys.insert((
            row["kind"].clone(),
            row["source"].clone(),
            row["source_id"].clone()
        ))),
        "ambiguous repeated Google Health record"
    );
    Ok((output, grants))
}

async fn google_session_source(
    client: &Client,
    token: &str,
    origin: &str,
    endpoint: &str,
    resource_name: Option<&str>,
) -> Result<Option<Value>> {
    let Some(name) = resource_name else {
        return Ok(None);
    };
    let parts: Vec<_> = name.split('/').collect();
    let ["users", user, "dataTypes", data_type, "dataPoints", point] = parts.as_slice() else {
        return Ok(None);
    };
    let safe_segment = |segment: &str| {
        !segment.is_empty()
            && segment
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
    };
    if *data_type != endpoint || !safe_segment(user) || !safe_segment(point) {
        return Ok(None);
    }
    let Ok(response) = client
        .get(format!("{origin}/v4/{name}"))
        .bearer_auth(token)
        .timeout(Duration::from_secs(3))
        .send()
        .await
    else {
        return Ok(None);
    };
    let result = match body(response).await {
        Ok(result) => result,
        // Source metadata is optional, but the connection's request limit also applies to this lookup.
        Err(error) if error.is::<ProviderRateLimited>() => return Err(error),
        Err(_) => return Ok(None),
    };
    if result["name"].as_str() != Some(name) || !result["dataSource"].is_object() {
        return Ok(None);
    }
    Ok(result.get("dataSource").cloned())
}

fn google_point(root: &Value, field: &str, kind: &str, window: &Window) -> Result<Option<Value>> {
    let point = &root[field];
    let (start, end) = if kind == "heart_rate" {
        let at = date(&point["sampleTime"]["physicalTime"])?;
        (at, at)
    } else {
        (
            date(&point["interval"]["startTime"])?,
            date(&point["interval"]["endTime"])?,
        )
    };
    if !window.contains(start, end) {
        return Ok(None);
    }
    let origin = &root["dataSource"];
    let label = origin["device"]["displayName"]
        .as_str()
        .or_else(|| origin["platform"].as_str())
        .unwrap_or("Google Health");
    let id = root["name"]
        .as_str()
        .or_else(|| root["dataPointName"].as_str())
        .and_then(|name| name.rsplit('/').next())
        .filter(|id| !id.is_empty())
        .map(str::to_owned)
        .unwrap_or_else(|| format!("{start}/{end}"));
    let mut payload = match kind {
        "steps" => json!({"count":integer(&point["count"])?, "unit":"steps"}),
        "heart_rate" => {
            json!({"samples":[{"at":start,"bpm":integer(&point["beatsPerMinute"])?}],"unit":"bpm"})
        }
        "sleep" => {
            let stages = point["stages"].as_array().into_iter().flatten().map(|stage| {
                let value = match stage["type"].as_str().unwrap_or_default() {
                    "AWAKE" => 1, "ASLEEP" => 2, "LIGHT" => 4, "DEEP" => 5, "REM" => 6, _ => 0,
                };
                Ok::<_, anyhow::Error>(json!({"start":date(&stage["startTime"])?, "end":date(&stage["endTime"])?, "stage":value}))
            }).collect::<Result<Vec<_>>>()?;
            // Short awakenings can overlap stages and must not alter the normalized stage timeline.
            json!({"stages":stages, "provider_data":point})
        }
        "workout" => {
            let summary = &point["metricsSummary"];
            let mut payload = json!({
                "exercise_type":required_text(&point["exerciseType"])?,
                "title":point["displayName"],
                "provider_data":point
            });
            for (field, target, divisor) in [
                ("distanceMillimeters", "distance_meters", 1000.0),
                ("elevationGainMillimeters", "elevation_gain_meters", 1000.0),
                (
                    "averageSpeedMillimetersPerSecond",
                    "average_speed_meters_per_second",
                    1000.0,
                ),
                ("caloriesKcal", "calories_kcal", 1.0),
            ] {
                if let Some(value) = summary[field].as_f64() {
                    payload[target] = json!(value / divisor);
                }
            }
            for (field, target) in [
                ("steps", "workout_steps"),
                ("averageHeartRateBeatsPerMinute", "average_heart_rate_bpm"),
                ("activeZoneMinutes", "active_zone_minutes"),
            ] {
                if let Ok(value) = integer(&summary[field]) {
                    payload[target] = json!(value);
                }
            }
            if let Some(seconds) = point["activeDuration"]
                .as_str()
                .and_then(|value| value.strip_suffix('s'))
                .and_then(|value| value.parse::<f64>().ok())
                .filter(|value| value.is_finite() && *value >= 0.0)
            {
                payload["active_duration_seconds"] = json!(seconds);
            }
            payload
        }
        _ => bail!("unexpected Google Health data type"),
    };
    if origin.is_object() {
        payload["provider_source"] = origin.clone();
    }
    let revision = point["updateTime"]
        .as_str()
        .map(str::parse)
        .transpose()?
        .unwrap_or_else(Utc::now);
    Ok(Some(record(
        kind, label, &id, start, end, revision, payload,
    )))
}

#[cfg(test)]
#[path = "google_tests.rs"]
mod tests;
