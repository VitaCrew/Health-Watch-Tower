use anyhow::{bail, ensure, Context, Result};
use avatar_hsm::etl_adapter::Snapshot;
use chrono::{DateTime, Duration, Duration as CalendarDuration, Utc};
use reqwest::{Client, Response};
use serde_json::{json, Value};
use std::collections::HashSet;

#[path = "withings_detail.rs"]
mod detail;

const SLEEP_API: &str = "https://wbsapi.withings.net/v2/sleep";
const ACTIVITY_API: &str = "https://wbsapi.withings.net/v2/measure";
const MEASURE_API: &str = "https://wbsapi.withings.net/measure";
const ECG_API: &str = "https://wbsapi.withings.net/v2/heart";
const MAX_RESPONSE_BYTES: usize = 8 * 1024 * 1024;
const MAX_RECORDS: usize = 4_000;

#[derive(Debug)]
pub struct ProviderRateLimited {
    pub retry_at: DateTime<Utc>,
}

impl std::fmt::Display for ProviderRateLimited {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("Withings returned HTTP 429")
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
    collect_from_origins(
        client,
        token,
        scope,
        lease,
        start,
        end,
        [SLEEP_API, ACTIVITY_API, MEASURE_API, ECG_API],
    )
    .await
}

async fn collect_from_origins(
    client: &Client,
    token: &str,
    scope: Option<&str>,
    lease: &str,
    start: DateTime<Utc>,
    end: DateTime<Utc>,
    origins: [&str; 4],
) -> Result<Snapshot> {
    ensure!(
        !token.trim().is_empty() && !lease.is_empty(),
        "Withings credential and HSM lease are required"
    );
    ensure!(
        start < end && end - start <= Duration::days(7),
        "invalid Withings collection window"
    );
    let window = Window { start, end };
    let observed_at = Utc::now();
    let scope = scope.filter(|value| !value.trim().is_empty());
    let activity_access = has_scope(scope, "user.activity");
    let measurement_access = has_scope(scope, "user.metrics");
    let mut rows = Vec::new();
    let mut grants = Vec::new();

    if activity_access {
        let (sleep_rows, activity_rows) = tokio::try_join!(
            sleep(client, token, &window, origins[0]),
            activity(client, token, &window, origins[1]),
        )?;
        rows = sleep_rows;
        rows.extend(activity_rows);
        grants.extend(["sleep", "workout", "heart_rate", "steps", "metric"]);
    }
    if measurement_access {
        let (measurements, ecg_rows) = tokio::try_join!(
            measurements(client, token, &window, origins[2]),
            ecg(client, token, &window, origins[3]),
        )?;
        rows.extend(measurements);
        rows.extend(ecg_rows);
        if !grants.contains(&"metric") {
            grants.push("metric");
        }
    }
    ensure!(
        rows.len() <= MAX_RECORDS,
        "Withings snapshot record limit exceeded"
    );
    for row in &mut rows {
        row["modified_at"] = json!(observed_at);
    }
    let mut snapshot: Snapshot = serde_json::from_value(json!({
        "provider":"withings","lease":lease,"start":start,"end":end,
        "grants":grants,"records":rows
    }))
    .context("failed to build normalized Withings snapshot")?;
    snapshot.set_withings_measurement_access(measurement_access);
    Ok(snapshot)
}

fn has_scope(scope: Option<&str>, expected: &str) -> bool {
    scope.is_none_or(|scope| {
        scope
            .split(|c: char| c.is_whitespace() || c == ',')
            .any(|grant| grant == expected)
    })
}

async fn sleep(
    client: &Client,
    token: &str,
    window: &Window,
    endpoint: &str,
) -> Result<Vec<Value>> {
    let oldest = (window.start - Duration::days(1))
        .format("%Y-%m-%d")
        .to_string();
    let newest = (window.end + Duration::days(1))
        .format("%Y-%m-%d")
        .to_string();
    let mut offset = 0;
    let mut output = Vec::new();
    for _ in 0..32 {
        let request = client.post(endpoint).bearer_auth(token).form(&[
            ("action", "getsummary"), ("startdateymd", oldest.as_str()), ("enddateymd", newest.as_str()),
            ("data_fields", "total_timeinbed,total_sleep_time,asleepduration,deepsleepduration,lightsleepduration,remsleepduration,wakeupduration,sleep_efficiency,sleep_latency,wakeup_latency,wakeupcount,waso,nb_rem_episodes,out_of_bed_count,hr_average,hr_min,hr_max,rr_average,rr_min,rr_max,snoring,snoringepisodecount,sleep_score,rmssd_start_avg,rmssd_end_avg,breathing_quality_assessment,night_events,mvt_score_avg,mvt_active_duration,breathing_sounds,breathing_sounds_episode_count,chest_movement_rate_wellness_average,chest_movement_rate_wellness_min,chest_movement_rate_wellness_max,chest_movement_rate_average,chest_movement_rate_min,chest_movement_rate_max,core_body_temperature_min,core_body_temperature_max,core_body_temperature_avg,core_body_temperature_status"),
            ("offset", &offset.to_string()),
        ]);
        let Some(page) = detail::page(request).await? else {
            ensure!(offset == 0, "incomplete Withings sleep summaries");
            return Ok(output);
        };
        for item in records(&page["series"])? {
            if let Some(mut row) = sleep_point(item, window)? {
                let (stages, segments) =
                    detail::sleep_segments(client, token, endpoint, item).await?;
                row["payload"]["stages"] = json!(stages);
                row["payload"]["provider_sleep_segments"] = json!(segments);
                output.push(row);
            }
        }
        if !more(&page["more"]) {
            return Ok(output);
        }
        let next = integer(&page["offset"])?;
        ensure!(next > offset, "invalid Withings sleep cursor");
        offset = next;
    }
    bail!("Withings sleep pagination exceeded the limit")
}

async fn activity(
    client: &Client,
    token: &str,
    window: &Window,
    endpoint: &str,
) -> Result<Vec<Value>> {
    let mut rows = Vec::new();
    for action in ["getactivity", "getworkouts"] {
        daily_pages(client, token, window, endpoint, action, &mut rows).await?;
    }
    let mut start = window.start;
    while start < window.end {
        let end = (start + Duration::days(1)).min(window.end);
        let request = client.post(endpoint).bearer_auth(token).form(&[
            ("action", "getintradayactivity"),
            ("startdate", &start.timestamp().to_string()),
            ("enddate", &end.timestamp().to_string()),
        ]);
        if let Some(page) = detail::page(request).await? {
            intraday(
                &mut rows,
                &page["series"],
                &Window { start, end },
                window.end,
            )?;
        }
        start = end;
    }
    Ok(rows)
}

async fn daily_pages(
    client: &Client,
    token: &str,
    window: &Window,
    endpoint: &str,
    action: &str,
    rows: &mut Vec<Value>,
) -> Result<()> {
    let from = (window.start - Duration::days(1)).date_naive().to_string();
    let to = (window.end + Duration::days(1)).date_naive().to_string();
    let mut offset = 0;
    for _ in 0..32 {
        let request = client.post(endpoint).bearer_auth(token).form(&[
            ("action", action),
            ("startdateymd", from.as_str()),
            ("enddateymd", to.as_str()),
            ("offset", &offset.to_string()),
        ]);
        let Some(page) = detail::page(request).await? else {
            ensure!(offset == 0, "incomplete Withings {action} page");
            return Ok(());
        };
        if action == "getactivity" {
            append_daily(rows, &page["activities"], "daily_activity", "date", window)?;
        } else {
            for item in records(&page["series"])? {
                if let Some(row) = workout(item, window)? {
                    rows.push(row);
                }
            }
        }
        if !more(&page["more"]) {
            return Ok(());
        }
        let next = integer(&page["offset"])?;
        ensure!(next > offset, "invalid Withings {action} cursor");
        offset = next;
    }
    bail!("incomplete Withings {action} query")
}

async fn measurements(
    client: &Client,
    token: &str,
    window: &Window,
    endpoint: &str,
) -> Result<Vec<Value>> {
    let mut offset = 0;
    let mut rows = Vec::new();
    for _ in 0..32 {
        let response = body(
            client
                .post(endpoint)
                .bearer_auth(token)
                .form(&[
                    ("action", "getmeas"),
                    ("startdate", &window.start.timestamp().to_string()),
                    ("enddate", &window.end.timestamp().to_string()),
                    ("category", "1"),
                    ("offset", &offset.to_string()),
                ])
                .send()
                .await?,
        )
        .await?;
        ensure!(
            response["status"].as_u64() == Some(0),
            "Withings measurements request failed"
        );
        let page = &response["body"];
        for item in records(&page["measuregrps"])? {
            let at = epoch(&item["date"])?;
            if !window.contains(at, at) {
                continue;
            }
            let id = integer(&item["grpid"])?;
            let mut emitted = HashSet::new();
            for measure in item["measures"].as_array().into_iter().flatten() {
                if let Some((name, unit, value)) = measurement_value(measure) {
                    if emitted.insert(name) {
                        rows.push(record("metric", "Withings", &format!("{id}:{name}"), at, at, window.end,
                            json!({"metric":name,"provider_data":{"unit":unit,"value":value,"measurement":measure}})));
                    }
                }
            }
            rows.push(record("metric", "Withings", &format!("measurements:{id}"), at, at, window.end,
                json!({"metric":"measurements","provider_data":{"id":id,"category":item["category"],"measures":item["measures"]}})));
        }
        if !more(&page["more"]) {
            return Ok(rows);
        }
        let next = integer(&page["offset"])?;
        ensure!(next > offset, "invalid Withings measurement offset");
        offset = next;
    }
    bail!("Withings measurement pagination exceeded the limit")
}

async fn ecg(client: &Client, token: &str, window: &Window, endpoint: &str) -> Result<Vec<Value>> {
    let mut offset = 0;
    let mut seen = HashSet::new();
    let mut rows = Vec::new();
    for _ in 0..32 {
        let Some(page) = detail::page(client.post(endpoint).bearer_auth(token).form(&[
            ("action", "list"),
            ("startdate", &window.start.timestamp().to_string()),
            ("enddate", &window.end.timestamp().to_string()),
            ("offset", &offset.to_string()),
        ]))
        .await?
        else {
            ensure!(offset == 0, "incomplete Withings ECG list");
            return Ok(rows);
        };
        for item in records(&page["series"])? {
            let Some(id) = item["ecg"]["signalid"].as_u64().filter(|id| *id > 0) else {
                continue;
            };
            let at = epoch(&item["timestamp"])?;
            if !window.contains(at, at) || !seen.insert(id) {
                continue;
            }
            let waveform = detail::page(
                client
                    .post(endpoint)
                    .bearer_auth(token)
                    .form(&[("action", "get"), ("signalid", &id.to_string())]),
            )
            .await?;
            if let Some(waveform) = &waveform {
                let points = waveform["signal"]
                    .as_array()
                    .context("missing Withings ECG signal")?;
                ensure!(
                    (1..=24_000).contains(&points.len())
                        && points.iter().all(Value::is_i64)
                        && waveform["sampling_frequency"]
                            .as_u64()
                            .is_some_and(|rate| rate > 0),
                    "invalid Withings ECG signal"
                );
            }
            rows.push(record(
                "metric", "Withings", &format!("ecg:{id}"), at, at, window.end,
                json!({"metric":"ecg","provider_data":{"recording":item,"waveform":waveform,"signal_unit":"uV"}}),
            ));
        }
        if !more(&page["more"]) {
            return Ok(rows);
        }
        let next = integer(&page["offset"])?;
        ensure!(next > offset, "invalid Withings ECG offset");
        offset = next;
    }
    bail!("incomplete Withings ECG query")
}

fn sleep_point(item: &Value, window: &Window) -> Result<Option<Value>> {
    let start = epoch(&item["startdate"])?;
    let end = epoch(&item["enddate"])?;
    if start >= end || !window.contains(start, end) {
        return Ok(None);
    }
    let id = item["id"]
        .as_str()
        .map(str::to_owned)
        .or_else(|| item["id"].as_u64().map(|value| value.to_string()))
        .unwrap_or_else(|| format!("{start}/{end}"));
    Ok(Some(record(
        "sleep",
        "Withings",
        &id,
        start,
        end,
        window.end,
        json!({"stages":[],"provider_stage_summary":item["data"],"provider_data":item}),
    )))
}

fn workout(item: &Value, window: &Window) -> Result<Option<Value>> {
    let start = epoch(&item["startdate"])?;
    let end = epoch(&item["enddate"])?;
    if start >= end || !window.contains(start, end) {
        return Ok(None);
    }
    Ok(Some(record(
        "workout",
        "Withings",
        &integer(&item["id"])?.to_string(),
        start,
        end,
        window.end,
        json!({"exercise_type":integer(&item["category"])?,"duration_seconds":(end-start).num_seconds(),
            "calories_kcal":item["data"]["calories"],"distance_meters":item["data"]["distance"],"provider_data":item}),
    )))
}

fn intraday(
    rows: &mut Vec<Value>,
    series: &Value,
    window: &Window,
    revision: DateTime<Utc>,
) -> Result<()> {
    if series.as_array().is_some_and(Vec::is_empty) {
        return Ok(());
    }
    for (epoch_text, sample) in series
        .as_object()
        .context("missing Withings intraday series")?
    {
        let at = DateTime::from_timestamp(epoch_text.parse()?, 0)
            .context("invalid Withings sample time")?;
        if !window.contains(at, at) {
            continue;
        }
        ensure!(sample.is_object(), "invalid Withings intraday sample");
        let id = format!("intraday:{}", at.timestamp());
        rows.push(record(
            "metric",
            "Withings",
            &id,
            at,
            at,
            revision,
            json!({"metric":"intraday_activity","provider_data":sample}),
        ));
        if let Some(bpm) = sample["heart_rate"]
            .as_f64()
            .filter(|value| (1.0..=350.0).contains(value))
        {
            rows.push(record(
                "heart_rate",
                "Withings",
                &id,
                at,
                at,
                revision,
                json!({"unit":"bpm","samples":[{"at":at,"bpm":bpm}]}),
            ));
        }
    }
    Ok(())
}

fn append_daily(
    rows: &mut Vec<Value>,
    items: &Value,
    metric: &str,
    key: &str,
    window: &Window,
) -> Result<()> {
    for item in records(items)? {
        let Some(value) = item[key].as_str() else {
            continue;
        };
        let day = value
            .parse::<chrono::NaiveDate>()
            .context("invalid Withings daily date")?;
        let at = day.and_hms_opt(0, 0, 0).unwrap().and_utc();
        if window.contains(at, at) {
            let id = item["id"]
                .as_str()
                .map(str::to_owned)
                .or_else(|| item["id"].as_i64().map(|value| value.to_string()))
                .unwrap_or_else(|| day.to_string());
            rows.push(record("metric", "Withings", &format!("{metric}:{id}"), at, at, window.end,
                json!({"metric":metric,"time_precision":"provider_calendar_date","provider_data":item})));
        }
    }
    Ok(())
}

fn measurement_value(measure: &Value) -> Option<(&'static str, &'static str, f64)> {
    let (name, unit) = match measure["type"].as_u64()? {
        1 => ("weight", "kg"),
        4 => ("height", "m"),
        5 => ("fat_free_mass", "kg"),
        6 => ("body_fat", "%"),
        8 => ("fat_mass", "kg"),
        9 => ("blood_pressure_diastolic", "mmHg"),
        10 => ("blood_pressure_systolic", "mmHg"),
        11 => ("standing_heart_rate", "bpm"),
        12 => ("temperature", "celsius"),
        54 => ("oxygen_saturation", "%"),
        71 => ("body_temperature", "celsius"),
        73 => ("skin_temperature", "celsius"),
        76 => ("muscle_mass", "kg"),
        77 => ("body_water_mass", "kg"),
        88 => ("bone_mass", "kg"),
        91 => ("pulse_wave_velocity", "m/s"),
        119 => ("blood_glucose", "mg/dL"),
        123 => ("vo2_max", "mL/(kg·min)"),
        155 => ("vascular_age", "years"),
        168 => ("extracellular_water_mass", "kg"),
        169 => ("intracellular_water_mass", "kg"),
        170 => ("visceral_fat_index", "index"),
        226 => ("basal_metabolic_rate", "kcal/day"),
        227 => ("metabolic_age", "years"),
        _ => return None,
    };
    let exponent = measure["unit"].as_i64()?;
    if !(-12..=12).contains(&exponent) {
        return None;
    }
    Some((
        name,
        unit,
        measure["value"].as_i64()? as f64 * 10_f64.powi(exponent as i32),
    ))
}

fn more(value: &Value) -> bool {
    value.as_bool() == Some(true) || value.as_u64() == Some(1)
}
fn epoch(value: &Value) -> Result<DateTime<Utc>> {
    value
        .as_i64()
        .and_then(|value| DateTime::from_timestamp(value, 0))
        .context("invalid Withings timestamp")
}
fn integer(value: &Value) -> Result<u64> {
    value
        .as_u64()
        .or_else(|| value.as_str()?.parse().ok())
        .context("invalid Withings integer")
}
fn records(value: &Value) -> Result<&Vec<Value>> {
    value.as_array().context("missing Withings record list")
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
struct Window {
    start: DateTime<Utc>,
    end: DateTime<Utc>,
}
impl Window {
    fn contains(&self, start: DateTime<Utc>, end: DateTime<Utc>) -> bool {
        start >= self.start && start < self.end && end >= start && end <= self.end
    }
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
        "Withings returned HTTP {} from {}",
        response.status().as_u16(),
        response.url()
    );
    let mut response = response;
    let mut bytes = Vec::new();
    while let Some(chunk) = response.chunk().await? {
        ensure!(
            bytes.len() + chunk.len() <= MAX_RESPONSE_BYTES,
            "Withings response exceeds limit"
        );
        bytes.extend_from_slice(&chunk);
    }
    serde_json::from_slice(&bytes).context("invalid Withings JSON response")
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
        .filter(|retry_at| *retry_at > now)
        .unwrap_or_else(|| now + Duration::minutes(5))
}

#[cfg(test)]
#[path = "withings_tests.rs"]
mod tests;
