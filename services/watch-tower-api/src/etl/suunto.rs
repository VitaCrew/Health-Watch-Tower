use anyhow::{bail, ensure, Context, Result};
use avatar_hsm::etl_adapter::Snapshot;
use chrono::{DateTime, Duration, NaiveDate, NaiveDateTime, Utc};
use reqwest::{Client, Response};
use serde_json::{json, Value};
use std::collections::{BTreeMap, HashSet};

const MAX_RECORDS: usize = 4_000;
const WORKOUTS_API: &str = "https://cloudapi.suunto.com/v3/workouts/";
const SAMPLES_API: &str = "https://cloudapi.suunto.com";

#[derive(Debug)]
pub struct ProviderRateLimited {
    pub retry_at: DateTime<Utc>,
}

impl std::fmt::Display for ProviderRateLimited {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Suunto returned HTTP 429")
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
    subscription: &str,
    lease: &str,
    start: DateTime<Utc>,
    end: DateTime<Utc>,
) -> Result<Snapshot> {
    collect_from_origins(
        client,
        token,
        subscription,
        lease,
        start,
        end,
        WORKOUTS_API,
        SAMPLES_API,
    )
    .await
}

async fn collect_from_origins(
    client: &Client,
    token: &str,
    subscription: &str,
    lease: &str,
    start: DateTime<Utc>,
    end: DateTime<Utc>,
    workouts_api: &str,
    samples_api: &str,
) -> Result<Snapshot> {
    ensure!(
        !token.trim().is_empty() && !subscription.trim().is_empty() && !lease.trim().is_empty(),
        "Suunto credentials and HSM lease are required"
    );
    ensure!(
        start < end && end - start <= Duration::days(1),
        "invalid Suunto collection window"
    );
    let window = Window { start, end };
    let (mut rows, samples) = tokio::try_join!(
        suunto_workouts(client, token, subscription, &window, workouts_api),
        suunto_metrics(client, token, subscription, start, end, samples_api),
    )?;
    rows.extend(samples);
    ensure!(
        rows.len() <= MAX_RECORDS,
        "Suunto snapshot record limit exceeded"
    );
    Ok(serde_json::from_value(json!({
        "provider":"suunto","lease":lease,"start":start,"end":end,
        "grants":["workout","sleep","heart_rate","steps","metric"],"records":rows
    }))?)
}

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

fn record(
    kind: &str,
    source: &str,
    id: &str,
    start: DateTime<Utc>,
    end: DateTime<Utc>,
    revision: DateTime<Utc>,
    payload: Value,
) -> Value {
    json!({"kind":kind,"source":source,"source_id":id,"started_at":start,
        "ended_at":end,"modified_at":revision,"payload":payload})
}

fn records(value: &Value) -> Result<&Vec<Value>> {
    value.as_array().context("missing provider record list")
}

fn required_text(value: &Value) -> Result<&str> {
    value
        .as_str()
        .filter(|text| !text.is_empty())
        .context("required text field missing")
}

fn integer(value: &Value) -> Result<u64> {
    value
        .as_u64()
        .or_else(|| value.as_str()?.parse().ok())
        .context("invalid whole number")
}

fn account_id(value: &Value) -> Option<String> {
    value
        .as_str()
        .map(str::to_owned)
        .or_else(|| value.as_i64().map(|id| id.to_string()))
}

fn point(value: &Value) -> Option<(DateTime<Utc>, &'static str)> {
    if let Some(number) = value.as_i64() {
        let time = if number > 10_000_000_000 {
            DateTime::from_timestamp_millis(number)
        } else {
            DateTime::from_timestamp(number, 0)
        }?;
        return Some((time, "instant"));
    }
    let text = value.as_str()?;
    if let Ok(at) = text.parse::<DateTime<Utc>>() {
        return Some((at, "instant"));
    }
    if let Ok(day) = text.parse::<NaiveDate>() {
        return Some((
            day.and_hms_opt(0, 0, 0)?.and_utc(),
            "provider_calendar_date",
        ));
    }
    Some((
        NaiveDateTime::parse_from_str(text, "%Y-%m-%dT%H:%M:%S%.f")
            .ok()?
            .and_utc(),
        "provider_civil_time",
    ))
}

fn append(
    rows: &mut Vec<Value>,
    items: &Value,
    source: &str,
    metric: &str,
    keys: &[&str],
    window: &Window,
) -> Result<()> {
    for item in records(items)? {
        let Some((at, precision)) = keys.iter().find_map(|key| point(&item[*key])) else {
            continue;
        };
        if !window.contains(at, at) {
            continue;
        }
        let id = account_id(&item["id"]).unwrap_or_else(|| at.to_rfc3339());
        rows.push(record(
            "metric",
            source,
            &format!("{metric}:{id}"),
            at,
            at,
            window.end,
            json!({"metric":metric,"time_precision":precision,"provider_data":item}),
        ));
    }
    Ok(())
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
        "Suunto returned HTTP {}",
        response.status().as_u16()
    );
    let mut response = response;
    let mut bytes = Vec::new();
    while let Some(chunk) = response.chunk().await? {
        ensure!(
            bytes.len() + chunk.len() <= 8 * 1024 * 1024,
            "Suunto response exceeds limit"
        );
        bytes.extend_from_slice(&chunk);
    }
    serde_json::from_slice(&bytes).context("invalid Suunto JSON response")
}

async fn optional(request: reqwest::RequestBuilder) -> Result<Option<Value>> {
    let response = request.send().await.map_err(reqwest::Error::without_url)?;
    if matches!(
        response.status(),
        reqwest::StatusCode::NO_CONTENT
            | reqwest::StatusCode::NOT_FOUND
            | reqwest::StatusCode::FORBIDDEN
    ) {
        return Ok(None);
    }
    body(response).await.map(Some)
}

fn retry_after(header: Option<&str>, now: DateTime<Utc>) -> DateTime<Utc> {
    header
        .and_then(|value| {
            let value = value.trim();
            value
                .parse::<i64>()
                .ok()
                .and_then(Duration::try_seconds)
                .and_then(|delay| now.checked_add_signed(delay))
                .or_else(|| {
                    DateTime::parse_from_rfc2822(value)
                        .ok()
                        .map(|at| at.with_timezone(&Utc))
                })
        })
        .filter(|at| *at > now)
        .unwrap_or_else(|| now + Duration::minutes(5))
}

async fn suunto_workouts(
    client: &Client,
    token: &str,
    subscription: &str,
    window: &Window,
    endpoint: &str,
) -> Result<Vec<Value>> {
    const PAGE_SIZE: usize = 100;
    let since = window.start.timestamp_millis().to_string();
    let until = window.end.timestamp_millis().to_string();
    let mut rows = Vec::new();
    let mut seen = HashSet::new();
    for page in 0..32 {
        let response = body(
            client
                .get(endpoint)
                .bearer_auth(token)
                .header("Ocp-Apim-Subscription-Key", subscription)
                .query(&[
                    ("since", since.clone()),
                    ("until", until.clone()),
                    ("limit", PAGE_SIZE.to_string()),
                    ("offset", (page * PAGE_SIZE).to_string()),
                    ("filter-by-modification-time", "false".into()),
                ])
                .send()
                .await
                .map_err(reqwest::Error::without_url)?,
        )
        .await?;
        let items = records(&response["payload"])?;
        let before = seen.len();
        for item in items {
            if seen.insert(required_text(&item["workoutKey"])?.to_owned()) {
                if let Some(row) = suunto_point(item, window)? {
                    rows.push(row);
                }
            }
        }
        ensure!(
            rows.len() <= MAX_RECORDS,
            "Suunto workout record limit exceeded"
        );
        if items.len() < PAGE_SIZE {
            return Ok(rows);
        }
        ensure!(seen.len() > before, "Suunto workout page repeated");
    }
    bail!("Suunto pagination exceeded the limit")
}

fn suunto_point(item: &Value, window: &Window) -> Result<Option<Value>> {
    let started = DateTime::from_timestamp_millis(
        item["startTime"]
            .as_i64()
            .context("missing Suunto start time")?,
    )
    .context("invalid Suunto start time")?;
    let (stopped, derived, paused) = if let Some(stopped) =
        item["stopTime"].as_i64().filter(|at| *at != 0)
    {
        (stopped, false, false)
    } else if let Some((stopped, paused)) = suunto_elapsed_end(item, started.timestamp_millis()) {
        (stopped, true, paused)
    } else {
        return Ok(None);
    };
    let ended = DateTime::from_timestamp_millis(stopped).context("invalid Suunto stop time")?;
    let mut payload = json!({
        "exercise_type":format!("suunto:{}", integer(&item["activityId"])?),
        "distance_meters":item["totalDistance"],
        "provider_timezone_offset_minutes":item["timeOffsetInMinutes"],
        "duration_seconds":(ended-started).num_seconds(),"provider_data":item
    });
    if derived {
        payload["end_derived_from_timer"] = json!(true);
        if paused {
            payload["end_derived_from_pauses"] = json!(true);
        }
    }
    Ok(
        (started < ended && window.contains(started, ended)).then_some(record(
            "workout",
            "Suunto",
            required_text(&item["workoutKey"])?,
            started,
            ended,
            window.end,
            payload,
        )),
    )
}

pub(super) fn normalize_workout_detail(response: &Value, requested_key: &str) -> Result<Value> {
    let item = &response["payload"];
    ensure!(
        item["workoutKey"].as_str() == Some(requested_key),
        "unexpected Suunto workout key"
    );
    let window = Window {
        start: DateTime::<Utc>::MIN_UTC,
        end: Utc::now(),
    };
    suunto_point(item, &window)?.context("invalid Suunto workout detail")
}

fn suunto_elapsed_end(item: &Value, start: i64) -> Option<(i64, bool)> {
    let max_end = start.checked_add(604_800_000)?;
    let timer = item["totalTime"]
        .as_f64()
        .filter(|seconds| *seconds > 0.0 && *seconds <= 604_800.0)?;
    let timer_end = start.checked_add((timer * 1000.0).round() as i64)?;
    let mut pauses: Vec<_> = item["extensions"]
        .as_array()
        .into_iter()
        .flatten()
        .filter(|extension| extension["type"] == "PauseMarkerExtension")
        .flat_map(|extension| extension["pauseMarkers"].as_array().into_iter().flatten())
        .filter_map(|pause| Some((pause["startTime"].as_i64()?, pause["endTime"].as_i64()?)))
        .filter(|&(from, to)| start <= from && from < to && to <= max_end)
        .collect();
    pauses.sort_unstable();
    let mut end = timer_end;
    let mut last_pause_end = start;
    for (from, to) in pauses {
        if from > end {
            break;
        }
        end += (to - from.max(last_pause_end)).max(0);
        if end > max_end {
            return None;
        }
        last_pause_end = last_pause_end.max(to);
    }
    Some((end, end != timer_end))
}

async fn suunto_metrics(
    client: &Client,
    token: &str,
    subscription: &str,
    start: DateTime<Utc>,
    end: DateTime<Utc>,
    origin: &str,
) -> Result<Vec<Value>> {
    let observed_at = Utc::now();
    let window = Window { start, end };
    let mut rows = Vec::new();
    for (path, name) in [
        ("sleep", "sleep_samples"),
        ("recovery", "recovery_samples"),
        ("activity", "activity_samples"),
    ] {
        let Some(result) = optional(
            client
                .get(format!("{origin}/247samples/{path}"))
                .bearer_auth(token)
                .header("Ocp-Apim-Subscription-Key", subscription)
                .query(&[
                    ("from", start.timestamp_millis()),
                    ("to", end.timestamp_millis()),
                ]),
        )
        .await?
        else {
            continue;
        };
        rows.extend(suunto_series(&result, name, &window)?);
    }
    let params = [
        ("startdate", start.format("%Y-%m-%dT%H:%M:%S").to_string()),
        ("enddate", end.format("%Y-%m-%dT%H:%M:%S").to_string()),
    ];
    if let Some(result) = optional(
        client
            .get(format!("{origin}/247/daily-activity-statistics"))
            .bearer_auth(token)
            .header("Ocp-Apim-Subscription-Key", subscription)
            .query(&params),
    )
    .await?
    {
        rows.extend(suunto_daily(&result, &window)?);
    }
    for row in &mut rows {
        row["modified_at"] = json!(observed_at);
    }
    ensure!(
        rows.len() <= MAX_RECORDS,
        "Suunto metric record limit exceeded"
    );
    Ok(rows)
}

fn suunto_daily(data: &Value, window: &Window) -> Result<Vec<Value>> {
    let mut rows = Vec::new();
    let mut daily = BTreeMap::<(String, String), (DateTime<Utc>, Value)>::new();
    for metric in suunto_rows(data)? {
        if let Some(name @ ("stepcount" | "energyconsumption")) = metric["Name"].as_str() {
            for source in metric["Sources"].as_array().into_iter().flatten() {
                let source_label = source["Name"]
                    .as_str()
                    .filter(|name| !name.is_empty() && name.len() < 80)
                    .map_or_else(|| "Suunto".to_string(), |name| format!("Suunto:{name}"));
                for sample in source["Samples"].as_array().into_iter().flatten() {
                    let Some((at, precision)) = point(&sample["TimeISO8601"]) else {
                        continue;
                    };
                    if !window.contains(at, at) {
                        continue;
                    }
                    let offset = sample["TimeISO8601"]
                        .as_str()
                        .and_then(|stamp| stamp.parse::<DateTime<chrono::FixedOffset>>().ok());
                    let date = offset.map_or_else(|| at.date_naive(), |stamp| stamp.date_naive());
                    let timezone = offset.map(|stamp| stamp.offset().to_string());
                    let mut detail = json!({"date":date,"timezone":timezone,"sample":sample});
                    if name == "stepcount" {
                        let Some(count) =
                            sample["Value"].as_u64().filter(|value| *value <= 1_000_000)
                        else {
                            continue;
                        };
                        detail["steps"] = json!(count);
                    } else {
                        let Some(joules) = sample["Value"].as_f64().filter(|value| *value >= 0.0)
                        else {
                            continue;
                        };
                        detail["value"] = json!(joules / 4_184.0);
                        detail["unit"] = json!("kcal");
                    }
                    let id = format!("daily_{name}:{date}");
                    let row = record(
                        "metric",
                        &source_label,
                        &id,
                        at,
                        at,
                        window.end,
                        json!({"metric":format!("daily_{name}"),"time_precision":precision,"provider_data":detail}),
                    );
                    let key = (source_label.clone(), id);
                    if daily.get(&key).is_none_or(|(previous, _)| at >= *previous) {
                        daily.insert(key, (at, row));
                    }
                }
            }
        } else {
            let Some(name) = metric["type"]
                .as_str()
                .filter(|value| !value.is_empty() && value.len() < 64)
            else {
                continue;
            };
            if metric["daily_values"].is_array() {
                append(
                    &mut rows,
                    &metric["daily_values"],
                    "Suunto",
                    &format!("daily_{name}"),
                    &["date"],
                    window,
                )?;
            }
        }
    }
    rows.extend(daily.into_values().map(|(_, row)| row));
    Ok(rows)
}

fn suunto_rows(data: &Value) -> Result<&Vec<Value>> {
    records(data.get("payload").unwrap_or(data))
}

fn suunto_series(data: &Value, name: &str, window: &Window) -> Result<Vec<Value>> {
    let mut samples = BTreeMap::new();
    let mut sleep = BTreeMap::new();
    for item in suunto_rows(data)? {
        if let Some((at, precision)) = point(&item["timestamp"]) {
            if window.contains(at, at) {
                let id = account_id(&item["entryData"]["SleepId"]);
                samples.insert((at, id), (precision, item));
            }
        }
        if name == "sleep_samples" {
            let detail = &item["entryData"];
            if let Some(id) = account_id(&detail["SleepId"]) {
                let duration = detail["Duration"].as_i64().unwrap_or(0);
                let entry = sleep.entry(id).or_insert((duration, detail));
                if duration >= entry.0 {
                    *entry = (duration, detail);
                }
            }
        }
    }
    let mut rows = Vec::new();
    for ((at, id), (precision, item)) in samples {
        let id = id.map_or_else(|| at.to_rfc3339(), |id| format!("{at}:{id}"));
        rows.push(record(
            "metric",
            "Suunto",
            &format!("{name}:{id}"),
            at,
            at,
            window.end,
            json!({"metric":name,"time_precision":precision,"provider_data":item}),
        ));
        if name == "activity_samples" && precision == "instant" {
            suunto_activity_sample(&mut rows, at, &item["entryData"], window);
        }
    }
    for (id, (_, detail)) in sleep {
        let Some(start) = detail["BedtimeStart"]
            .as_str()
            .and_then(|value| value.parse().ok())
        else {
            continue;
        };
        let Some(end) = detail["BedtimeEnd"]
            .as_str()
            .and_then(|value| value.parse().ok())
        else {
            continue;
        };
        if start < end && window.contains(start, end) {
            rows.push(record(
                "sleep",
                "Suunto",
                &format!("sleep:{id}"),
                start,
                end,
                window.end,
                json!({"stages":[],"provider_data":detail}),
            ));
        }
    }
    Ok(rows)
}

fn suunto_activity_sample(
    rows: &mut Vec<Value>,
    at: DateTime<Utc>,
    sample: &Value,
    window: &Window,
) {
    if let Some(hr) = sample["HR"].as_u64().filter(|hr| (1..=350).contains(hr)) {
        rows.push(record(
            "heart_rate",
            "Suunto",
            &format!("activity_hr:{at}"),
            at,
            at,
            window.end,
            json!({"unit":"bpm","samples":[{"at":at,"bpm":hr}]}),
        ));
    }
    for (name, unit, value) in [
        (
            "activity_step_sample",
            "steps",
            sample["StepCount"]
                .as_u64()
                .filter(|v| *v <= 1_000_000)
                .map(|v| v as f64),
        ),
        (
            "active_energy",
            "kcal",
            sample["EnergyConsumption"]
                .as_f64()
                .filter(|v| *v >= 0.0)
                .map(|v| v / 4_184.0),
        ),
        (
            "oxygen_saturation",
            "%",
            sample["SpO2"]
                .as_f64()
                .filter(|v| *v > 0.0 && *v <= 100.0)
                .map(|v| if v <= 1.0 { v * 100.0 } else { v }),
        ),
        (
            "hrv_rmssd",
            "ms",
            sample["HRV"].as_f64().filter(|v| *v > 0.0),
        ),
    ] {
        if let Some(value) = value {
            rows.push(record("metric", "Suunto", &format!("{name}:{at}"), at, at, window.end,
                json!({"metric":name,"time_precision":"instant","provider_data":{"value":value,"unit":unit}})));
        }
    }
}

#[cfg(test)]
#[path = "suunto_tests.rs"]
mod tests;
