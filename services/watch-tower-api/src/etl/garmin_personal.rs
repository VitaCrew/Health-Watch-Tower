use anyhow::{ensure, Context, Result};
use avatar_hsm::etl_adapter::Snapshot;
use chrono::{DateTime, Duration as CalendarDuration, NaiveDateTime, Utc};
use reqwest::{Client, Response};
use serde_json::{json, Value};
use std::collections::BTreeMap;

#[cfg(test)]
#[path = "garmin_personal_tests.rs"]
mod tests;

const BASE: &str = "https://personal.connectapi.garmin.com";
const MAX_RESPONSE_BYTES: usize = 16 * 1024 * 1024;
const MAX_RECORDS: usize = 24_000;

#[derive(Debug)]
pub struct ProviderRateLimited {
    pub retry_at: DateTime<Utc>,
}

impl std::fmt::Display for ProviderRateLimited {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("Garmin returned HTTP 429")
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
    key: &str,
    lease: &str,
    start: DateTime<Utc>,
    end: DateTime<Utc>,
) -> Result<Snapshot> {
    collect_from_origin(client, key, lease, start, end, BASE).await
}

async fn collect_from_origin(
    client: &Client,
    key: &str,
    lease: &str,
    start: DateTime<Utc>,
    end: DateTime<Utc>,
    origin: &str,
) -> Result<Snapshot> {
    ensure!(
        !key.trim().is_empty() && !lease.is_empty(),
        "Garmin credential and HSM lease are required"
    );
    ensure!(
        start < end && end - start <= CalendarDuration::days(7),
        "invalid Garmin collection window"
    );
    let rows = fetch(client, key, start, end, origin).await?;
    ensure!(
        rows.len() <= MAX_RECORDS,
        "Garmin snapshot record limit exceeded"
    );
    serde_json::from_value(json!({
        "provider":"garmin_personal","lease":lease,"start":start,"end":end,
        "grants":["heart_rate","sleep","workout","metric"],"records":rows
    }))
    .context("failed to build normalized Garmin snapshot")
}

async fn access(client: &Client, key: &str, origin: &str) -> Result<String> {
    let mut auth = reqwest::header::HeaderValue::from_str(&format!("ApiKey {key}"))?;
    auth.set_sensitive(true);
    let response = client
        .post(format!(
            "{origin}/personalapitoken-service/personal-api/personal-api-key/token"
        ))
        .header(reqwest::header::AUTHORIZATION, auth)
        .send()
        .await?;
    let token = response_body(response).await?;
    let token = String::from_utf8(token)?;
    let token = token.trim().to_owned();
    ensure!(
        !token.is_empty() && token.len() < 8192,
        "invalid Garmin token response"
    );
    Ok(token)
}

async fn fetch(
    client: &Client,
    key: &str,
    start: DateTime<Utc>,
    end: DateTime<Utc>,
    origin: &str,
) -> Result<Vec<Value>> {
    let token = access(client, key, origin).await?;
    let window = Window { start, end };
    let first_day = (start - CalendarDuration::days(1)).date_naive();
    let last_day = (end + CalendarDuration::days(1)).date_naive();
    let from = first_day.to_string();
    let to = last_day.to_string();
    let get = |path: &str| client.get(format!("{origin}/{path}")).bearer_auth(&token);
    let sleep = body(
        get("sleep-service/sleep/dailySleepSummariesByDate")
            .query(&[("startDate", &from), ("endDate", &to)])
            .send()
            .await?,
    )
    .await?;
    let mut rows = records(&sleep)?
        .iter()
        .map(|row| sleep_point(row, &window))
        .filter_map(Result::transpose)
        .collect::<Result<Vec<_>>>()?;
    rows.extend(vendor_metrics(client, origin, &token, start, end).await?);
    rows.extend(cycle_days_at(client, origin, &token, &window).await?);
    let daily_steps = body(
        get(&format!(
            "usersummary-service/stats/steps/daily/{from}/{to}"
        ))
        .send()
        .await?,
    )
    .await?;
    for item in records(&daily_steps)? {
        let day: chrono::NaiveDate = required_text(&item["calendarDate"])?.parse()?;
        let at = day.and_hms_opt(0, 0, 0).unwrap().and_utc();
        if window.contains(at, at) {
            rows.push(record("metric","Garmin Connect",&format!("daily-steps:{day}"),at,at,end,
                json!({"metric":"daily_steps","time_precision":"provider_calendar_date","provider_data":item})));
        }
    }
    for page in 0..32 {
        let response = body(
            get("activitylist-service/activities/search/activities")
                .query(&[
                    ("startDate", &from),
                    ("endDate", &to),
                    ("start", &(page * 100).to_string()),
                    ("limit", &"100".to_owned()),
                ])
                .send()
                .await?,
        )
        .await?;
        let activities = records(&response)?;
        rows.extend(
            activities
                .iter()
                .map(|row| workout_point(row, &window))
                .filter_map(Result::transpose)
                .collect::<Result<Vec<_>>>()?,
        );
        if activities.len() < 100 {
            break;
        }
        ensure!(page < 31, "incomplete Garmin activity pagination");
    }

    let mut day = first_day;
    let mut hours = std::collections::BTreeMap::new();
    while day <= last_day {
        let response = get("wellness-service/wellness/dailyHeartRate")
            .query(&[("date", day.to_string())])
            .send()
            .await?;
        if response.status() != reqwest::StatusCode::NO_CONTENT {
            heart_points(&body(response).await?, &window, &mut hours)?;
        }
        day = day.succ_opt().context("invalid Garmin date")?;
    }
    for (hour, points) in hours {
        let at = *points.keys().next().context("empty Garmin hour")?;
        let until = *points.keys().next_back().context("empty Garmin hour")?;
        let samples = points
            .into_iter()
            .map(|(at, bpm)| json!({"at":at,"bpm":bpm}))
            .collect::<Vec<_>>();
        ensure!(samples.len() <= 500, "Garmin hour exceeds sample limit");
        rows.push(record(
            "heart_rate",
            "Garmin Connect",
            &format!("hour:{hour}"),
            at,
            until,
            end,
            json!({"unit":"bpm","samples":samples}),
        ));
    }
    Ok(rows)
}

async fn cycle_days_at(
    client: &Client,
    origin: &str,
    token: &str,
    window: &Window,
) -> Result<Vec<Value>> {
    let mut rows = Vec::new();
    for day in window
        .start
        .date_naive()
        .iter_days()
        .take_while(|day| *day <= window.end.date_naive())
    {
        let at = day.and_hms_opt(0, 0, 0).unwrap().and_utc();
        if !window.contains(at, at) {
            continue;
        }
        // Leave the optional todayDate to Garmin so predictions use its calendar context.
        let Some(view) = optional(
            client
                .get(format!(
                    "{origin}/periodichealth-service/menstrualcycle/dayview/{day}"
                ))
                .bearer_auth(token)
                .query(&[("includeAnalysis", true)]),
        )
        .await?
        else {
            continue;
        };
        if view.is_null() {
            continue;
        }
        ensure!(view.is_object(), "invalid Garmin cycle day response");
        if let Some(reported) = view["dayLog"]["calendarDate"]["localDate"].as_str() {
            ensure!(
                reported.parse::<chrono::NaiveDate>()? == day,
                "Garmin cycle log date differs from request"
            );
        }
        for (field, metric) in [
            ("dayLog", "menstrual_cycle_day"),
            ("daySummary", "menstrual_cycle"),
            ("weightGoals", "cycle_pregnancy_weight_goals"),
        ] {
            match view.get(field) {
                None | Some(Value::Null) => {}
                Some(Value::Object(data)) if data.is_empty() => {}
                Some(data @ Value::Object(_)) => rows.push(record(
                    "metric",
                    "Garmin Connect",
                    &format!("{metric}:{day}"),
                    at,
                    at,
                    window.end,
                    json!({"metric":metric,"time_precision":"provider_calendar_date",
                           "local_day":day,"provider_data":data}),
                )),
                _ => anyhow::bail!("invalid Garmin cycle day section"),
            }
        }
    }
    Ok(rows)
}

fn gmt(value: &Value) -> Result<DateTime<Utc>> {
    let input = required_text(value)?;
    if let Ok(time) = input.parse::<DateTime<Utc>>() {
        return Ok(time);
    }
    Ok(NaiveDateTime::parse_from_str(input, "%Y-%m-%dT%H:%M:%S%.f")
        .or_else(|_| NaiveDateTime::parse_from_str(input, "%Y-%m-%d %H:%M:%S%.f"))?
        .and_utc())
}

fn sleep_point(item: &Value, window: &Window) -> Result<Option<Value>> {
    if item["sleepStartTimestampGMT"].is_null() || item["sleepEndTimestampGMT"].is_null() {
        return Ok(None);
    }
    let start = gmt(&item["sleepStartTimestampGMT"])?;
    let end = gmt(&item["sleepEndTimestampGMT"])?;
    Ok((start < end && window.contains(start, end)).then_some(record(
        "sleep", "Garmin Connect", &integer(&item["id"])?.to_string(), start, end, window.end,
        json!({"stages":[],"provider_stage_summary":{"deep_seconds":item["deepSleepSeconds"], "light_seconds":item["lightSleepSeconds"],
            "rem_seconds":item["remSleepSeconds"],"awake_seconds":item["awakeSleepSeconds"]}}),
    )))
}

fn workout_point(item: &Value, window: &Window) -> Result<Option<Value>> {
    let start = gmt(&item["startTimeGMT"])?;
    let elapsed = item["elapsedDuration"]
        .as_f64()
        .or_else(|| item["duration"].as_f64());
    let Some(duration) = elapsed.filter(|duration| (0.001..=604800.0).contains(duration)) else {
        return Ok(None);
    };
    let end = start + CalendarDuration::milliseconds((duration * 1000.0).round() as i64);
    Ok(window.contains(start, end).then_some(record(
        "workout",
        "Garmin Connect",
        &integer(&item["activityId"])?.to_string(),
        start,
        end,
        window.end,
        json!({"exercise_type":required_text(&item["activityType"]["typeKey"])?,
            "title":item["activityName"],"distance_meters":item["distance"]}),
    )))
}

type HeartHours = BTreeMap<i64, BTreeMap<DateTime<Utc>, u64>>;

fn heart_points(day: &Value, window: &Window, hours: &mut HeartHours) -> Result<()> {
    if day["heartRateValues"].is_null() {
        ensure!(
            day["calendarDate"].is_string(),
            "invalid Garmin heart rate response"
        );
        return Ok(());
    }
    for sample in records(&day["heartRateValues"])? {
        let Some(bpm) = sample[1].as_u64().filter(|bpm| (1..=350).contains(bpm)) else {
            continue;
        };
        let at = DateTime::from_timestamp_millis(
            sample[0]
                .as_i64()
                .context("missing Garmin sample timestamp")?,
        )
        .context("invalid Garmin sample timestamp")?;
        if window.contains(at, at) {
            hours
                .entry(at.timestamp().div_euclid(3600))
                .or_default()
                .insert(at, bpm);
        }
    }
    Ok(())
}

async fn vendor_metrics(
    client: &Client,
    origin: &str,
    token: &str,
    start: DateTime<Utc>,
    end: DateTime<Utc>,
) -> Result<Vec<Value>> {
    let window = Window { start, end };
    let from = start.date_naive().to_string();
    let to = end.date_naive().to_string();
    let get = |path: &str| client.get(format!("{origin}/{path}")).bearer_auth(token);
    let mut rows = Vec::new();
    for (path, key, metric) in [
        (
            format!("usersummary-service/stats/stress/daily/{from}/{to}"),
            "calendarDate",
            "daily_stress",
        ),
        (
            format!("hrv-service/hrv/daily/{from}/{to}"),
            "calendarDate",
            "daily_hrv",
        ),
        (
            "wellness-service/wellness/bodyBattery/reports/daily".to_owned(),
            "date",
            "body_battery",
        ),
    ] {
        let request = if metric == "body_battery" {
            get(&path).query(&[("startDate", &from), ("endDate", &to)])
        } else {
            get(&path)
        };
        let Some(data) = optional(request).await? else {
            continue;
        };
        let items = if metric == "daily_hrv" {
            &data["hrvSummaries"]
        } else {
            &data
        };
        append(&mut rows, items, metric, key, &window)?;
    }
    Ok(rows)
}

async fn optional(request: reqwest::RequestBuilder) -> Result<Option<Value>> {
    let response = request.send().await.map_err(|error| error.without_url())?;
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

fn append(
    rows: &mut Vec<Value>,
    items: &Value,
    metric: &str,
    key: &str,
    window: &Window,
) -> Result<()> {
    let items = match items {
        Value::Array(items) => items.as_slice(),
        _ => return Err(anyhow::anyhow!("missing Garmin {metric} record list")),
    };
    for item in items {
        let Some(day) = item[key].as_str() else {
            continue;
        };
        let day: chrono::NaiveDate = day.parse().context("invalid Garmin calendar date")?;
        let at = day.and_hms_opt(0, 0, 0).unwrap().and_utc();
        if window.contains(at, at) {
            let id = item["id"]
                .as_str()
                .map(str::to_owned)
                .or_else(|| item["id"].as_i64().map(|value| value.to_string()))
                .unwrap_or_else(|| day.to_string());
            rows.push(record("metric", "Garmin Connect", &format!("{metric}:{id}"), at, at, window.end,
                json!({"metric":metric,"time_precision":"provider_calendar_date","provider_data":item})));
        }
    }
    Ok(())
}

async fn body(response: Response) -> Result<Value> {
    let bytes = response_body(response).await?;
    serde_json::from_slice(&bytes).context("invalid Garmin JSON response")
}

async fn response_body(mut response: Response) -> Result<Vec<u8>> {
    let status = response.status();
    if status == reqwest::StatusCode::TOO_MANY_REQUESTS {
        let retry = response
            .headers()
            .get(reqwest::header::RETRY_AFTER)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.parse::<u64>().ok())
            .map(|seconds| Utc::now() + CalendarDuration::seconds(seconds.min(86_400) as i64))
            .unwrap_or_else(|| Utc::now() + CalendarDuration::minutes(5));
        return Err(ProviderRateLimited { retry_at: retry }.into());
    }
    ensure!(
        status.is_success(),
        "Garmin returned HTTP {}",
        status.as_u16()
    );
    let mut bytes = Vec::new();
    while let Some(chunk) = response.chunk().await? {
        ensure!(
            bytes.len() + chunk.len() <= MAX_RESPONSE_BYTES,
            "Garmin response exceeds limit"
        );
        bytes.extend_from_slice(&chunk);
    }
    Ok(bytes)
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
        .filter(|value| !value.is_empty())
        .context("required Garmin text missing")
}

fn integer(value: &Value) -> Result<u64> {
    value
        .as_u64()
        .or_else(|| value.as_str()?.parse().ok())
        .context("invalid Garmin integer")
}

fn records(value: &Value) -> Result<&Vec<Value>> {
    value.as_array().context("missing Garmin record list")
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
    json!({"kind":kind,"source":source,"source_id":id,"started_at":start,"ended_at":end,
        "modified_at":revision,"payload":payload})
}
