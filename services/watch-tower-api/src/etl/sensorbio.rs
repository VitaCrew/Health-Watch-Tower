use anyhow::{bail, ensure, Context, Result};
use avatar_hsm::etl_adapter::Snapshot;
use chrono::{DateTime, Duration as CalendarDuration, NaiveDate, Utc};
use reqwest::{Client, Response};
use serde_json::{json, Map, Value};

const ORIGIN: &str = "https://api.sensorbio.com";
const SOURCE: &str = "Sensor Bio";
const MAX_RECORDS: usize = 24_000;

pub async fn collect_window(
    client: &Client,
    token: &str,
    lease: &str,
    start: DateTime<Utc>,
    end: DateTime<Utc>,
) -> Result<Snapshot> {
    collect_from_origin(client, token, lease, start, end, ORIGIN).await
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
        !token.trim().is_empty() && !lease.trim().is_empty(),
        "Sensor Bio credential and HSM lease are required"
    );
    ensure!(
        start < end && end - start <= CalendarDuration::days(1),
        "invalid Sensor Bio collection window"
    );
    let rows = fetch_at(client, token, Window { start, end }, origin).await?;
    ensure!(
        rows.len() <= MAX_RECORDS,
        "Sensor Bio snapshot record limit exceeded"
    );
    Ok(serde_json::from_value(json!({
        "provider":"sensorbio","lease":lease,"start":start,"end":end,
        "grants":["sleep","heart_rate","workout","steps","metric"],"records":rows
    }))?)
}

async fn fetch_at(
    client: &Client,
    token: &str,
    window: Window,
    origin: &str,
) -> Result<Vec<Value>> {
    let (mut rows, workouts, intervals, biometrics, sleep) = tokio::try_join!(
        daily(client, token, &window, origin),
        workouts(client, token, &window, origin),
        minute_activity(client, token, &window, origin),
        biometrics(client, token, &window, origin),
        sleep(client, token, &window, origin),
    )?;
    rows.extend(workouts);
    rows.extend(intervals);
    rows.extend(biometrics);
    rows.extend(sleep);
    ensure!(
        rows.len() <= MAX_RECORDS,
        "Sensor Bio combined record limit exceeded"
    );
    Ok(rows)
}

async fn sleep(client: &Client, token: &str, window: &Window, origin: &str) -> Result<Vec<Value>> {
    daily_sleep_at(
        client,
        token,
        Window {
            start: window.start,
            end: window.end,
        },
        origin,
    )
    .await
}

async fn biometrics(
    client: &Client,
    token: &str,
    window: &Window,
    origin: &str,
) -> Result<Vec<Value>> {
    sensorbio_at(
        client,
        token,
        window.start,
        window.end,
        &format!("{origin}/v1/biometrics"),
    )
    .await
}

async fn daily(client: &Client, token: &str, window: &Window, origin: &str) -> Result<Vec<Value>> {
    let mut day = window.start.date_naive();
    let mut output = Vec::new();
    while day <= window.end.date_naive() {
        let at = day.and_hms_opt(0, 0, 0).unwrap().and_utc();
        if window.contains(at, at) {
            let date = day.to_string();
            let request = |endpoint| {
                client
                    .get(format!("{origin}/v1/{endpoint}"))
                    .bearer_auth(token)
                    .query(&[("date", date.as_str()), ("granularity", "day")])
            };
            let observed_at = Utc::now();
            let (steps, calories, recovery) = tokio::try_join!(
                optional(request("step/details")),
                optional(request("calorie/details")),
                optional(request("scores/recovery/details")),
            )?;
            for (payload, name) in [(steps, "daily_activity"), (calories, "daily_calories")] {
                if let Some(payload) = payload {
                    output.extend(daily_record(payload, name, day, at, window.end)?);
                }
            }
            if let Some(payload) = recovery {
                output.extend(recovery_record(&payload, day, at, observed_at)?);
            }
        }
        day = day.succ_opt().context("invalid Sensor Bio calendar day")?;
    }
    Ok(output)
}

fn recovery_record(
    payload: &Value,
    day: NaiveDate,
    at: DateTime<Utc>,
    observed_at: DateTime<Utc>,
) -> Result<Option<Value>> {
    ensure!(payload.is_object(), "invalid Sensor Bio recovery detail");
    let response_day = |value: &Value| {
        let value = value.as_str()?;
        value.parse::<NaiveDate>().ok().or_else(|| {
            DateTime::parse_from_rfc3339(value)
                .ok()
                .map(|at| at.date_naive())
        })
    };
    ensure!(
        payload
            .get("date")
            .is_none_or(|date| response_day(date) == Some(day))
            && payload
                .get("granularity")
                .is_none_or(|value| value == "day"),
        "unexpected Sensor Bio recovery date or granularity"
    );
    let score = |value: &Value| value.as_f64().filter(|value| (0.0..=100.0).contains(value));
    let mut fields = Map::new();
    for field in ["score", "score_baseline"] {
        if let Some(value) = score(&payload[field]) {
            fields.insert(field.into(), json!(value));
        }
    }
    if let Some(value) = payload["score_improvement_over_baseline_percentage"].as_f64() {
        fields.insert(
            "score_improvement_over_baseline_percentage".into(),
            json!(value),
        );
    }
    if let Some(value) = payload["score_datapoints"]
        .as_array()
        .into_iter()
        .flatten()
        .find_map(|point| {
            if response_day(&point["date"]) != Some(day) {
                return None;
            }
            score(&point["value"])
        })
    {
        fields.insert(
            "score_datapoints".into(),
            json!([{"date":day,"value":value}]),
        );
    }
    if fields.is_empty() {
        return Ok(None);
    }
    fields.insert("date".into(), json!(day));
    fields.insert("granularity".into(), json!("day"));
    Ok(Some(record(
        "metric",
        SOURCE,
        &format!("recovery_details:{day}"),
        at,
        at,
        observed_at,
        json!({"metric":"recovery_details","time_precision":"provider_calendar_date","provider_data":fields}),
    )))
}

fn daily_record(
    payload: Value,
    name: &str,
    day: NaiveDate,
    at: DateTime<Utc>,
    revision: DateTime<Utc>,
) -> Result<Option<Value>> {
    let metrics = records(&payload["metrics"])?;
    if metrics.is_empty() {
        return Ok(None);
    }
    ensure!(
        payload["date"]
            .as_str()
            .and_then(|value| value.parse().ok())
            == Some(day),
        "unexpected Sensor Bio calendar day"
    );
    let field = |name: &str| {
        metrics
            .iter()
            .find(|entry| entry["name"].as_str().or_else(|| entry["type"].as_str()) == Some(name))
    };
    let nonnegative = |entry: &Value| {
        entry["value"]
            .as_f64()
            .filter(|value| value.is_finite() && *value >= 0.0)
    };
    let mut stats = Map::new();
    let mut steps = None;
    if name == "daily_activity" {
        steps = field("Steps")
            .and_then(nonnegative)
            .filter(|value| value.fract() == 0.0 && *value < 10_000_000.0)
            .map(|value| value as u64);
        if let Some(value) = steps {
            stats.insert("steps".into(), json!(value));
        }
        if let Some(distance) = field("Distance") {
            let scale = match distance["unit"]
                .as_str()
                .map(str::to_ascii_lowercase)
                .as_deref()
            {
                Some("m") => Some(1.0),
                Some("km") => Some(1_000.0),
                Some("mi" | "miles") => Some(1_609.344),
                _ => None,
            };
            if let (Some(scale), Some(value)) = (scale, nonnegative(distance)) {
                stats.insert("distance_meters".into(), json!(value * scale));
            }
        }
    } else {
        let energy = |name| {
            field(name).and_then(|entry| {
                let unit = entry["unit"].as_str().unwrap_or("");
                (unit.is_empty() || unit.eq_ignore_ascii_case("kcal"))
                    .then(|| nonnegative(entry))
                    .flatten()
            })
        };
        let resting = energy("Resting Calories").filter(|value| *value > 0.0);
        let total = energy("Total Calories");
        for (key, value) in [
            ("basal_energy_kcal", resting),
            ("total_energy_kcal", total),
            (
                "active_energy_kcal",
                total
                    .zip(resting)
                    .map(|(total, basal)| total - basal)
                    .filter(|value| *value >= 0.0),
            ),
        ] {
            if let Some(value) = value {
                stats.insert(key.into(), json!(value));
            }
        }
    }
    Ok(Some(record(
        "metric",
        SOURCE,
        &format!("{name}:{day}"),
        at,
        at,
        revision,
        json!({
            "metric":name,"time_precision":"provider_calendar_date",
            "provider_data":{"date":day,"steps":steps,"stats":stats,"raw":payload},
        }),
    )))
}

async fn minute_activity(
    client: &Client,
    token: &str,
    window: &Window,
    origin: &str,
) -> Result<Vec<Value>> {
    const PAGE_SIZE: usize = 50;
    let mut cursor = window.start.timestamp_millis().saturating_sub(1);
    let revision = Utc::now();
    let mut rows = std::collections::BTreeMap::new();
    for _ in 0..=MAX_RECORDS.div_ceil(PAGE_SIZE) {
        let Some(page) = optional(
            client
                .get(format!("{origin}/v1/steps"))
                .bearer_auth(token)
                .query(&[("last-timestamp", cursor), ("limit", PAGE_SIZE as i64)]),
        )
        .await?
        else {
            return Ok(rows.into_values().collect());
        };
        let points = records(&page["data"])?;
        let Some(last) = points.last() else {
            return Ok(rows.into_values().collect());
        };
        let next = last["timestamp"]
            .as_i64()
            .context("invalid Sensor Bio minute cursor")?;
        ensure!(next > cursor, "Sensor Bio minute cursor did not advance");
        for raw in points {
            let Some(at) = raw["timestamp"]
                .as_i64()
                .and_then(DateTime::from_timestamp_millis)
            else {
                continue;
            };
            let Some(end) = at.checked_add_signed(CalendarDuration::minutes(1)) else {
                continue;
            };
            if !window.contains(at, end) {
                continue;
            }
            let mut detail = Map::new();
            if let Some(steps) = raw["steps"].as_u64() {
                detail.insert("steps".into(), json!(steps));
            }
            for (field, key) in [
                ("distance", "distance_meters"),
                ("total_step_calories", "step_energy_kcal"),
            ] {
                if let Some(value) = raw[field]
                    .as_f64()
                    .filter(|value| value.is_finite() && *value >= 0.0)
                {
                    detail.insert(key.into(), json!(value));
                }
            }
            if detail.is_empty() {
                continue;
            }
            detail.insert("raw".into(), raw.clone());
            rows.insert(
                at,
                record(
                    "metric",
                    SOURCE,
                    &format!("activity-minute:{}", at.timestamp_millis()),
                    at,
                    end,
                    revision,
                    json!({"metric":"activity_minute","provider_data":detail}),
                ),
            );
        }
        ensure!(
            rows.len() <= MAX_RECORDS,
            "Sensor Bio minute record limit exceeded"
        );
        if next >= window.end.timestamp_millis()
            || page["links"]["next"].as_str().is_none_or(str::is_empty)
        {
            return Ok(rows.into_values().collect());
        }
        cursor = next;
    }
    bail!("Sensor Bio minute pagination did not finish")
}

async fn workouts(
    client: &Client,
    token: &str,
    window: &Window,
    origin: &str,
) -> Result<Vec<Value>> {
    let mut cursor = (window.start - CalendarDuration::days(1))
        .timestamp_millis()
        .saturating_sub(1);
    let mut rows = std::collections::BTreeMap::new();
    for _ in 0..32 {
        let Some(page) = optional(
            client
                .get(format!("{origin}/v1/activities"))
                .bearer_auth(token)
                .query(&[("last-timestamp", cursor), ("limit", 50)]),
        )
        .await?
        else {
            return Ok(rows.into_values().collect());
        };
        let batches = records(&page["data"])?;
        let Some(last) = batches.last() else {
            return Ok(rows.into_values().collect());
        };
        for batch in batches {
            for workout in batch["activities"].as_array().into_iter().flatten() {
                if let Some((id, row)) = workout_record(workout, &batch["name"], window) {
                    rows.insert(id, row);
                }
            }
        }
        ensure!(
            rows.len() <= MAX_RECORDS,
            "Sensor Bio workout record limit exceeded"
        );
        if !page["links"]["next"]
            .as_str()
            .is_some_and(|next| !next.is_empty())
        {
            return Ok(rows.into_values().collect());
        }
        let next = millis(&last["timestamp"])
            .context("missing Sensor Bio workout cursor")?
            .timestamp_millis();
        ensure!(next > cursor, "Sensor Bio workout cursor did not advance");
        if next >= (window.end + CalendarDuration::days(1)).timestamp_millis() {
            return Ok(rows.into_values().collect());
        }
        cursor = next;
    }
    bail!("Sensor Bio workout pagination did not finish")
}

fn millis(value: &Value) -> Option<DateTime<Utc>> {
    DateTime::from_timestamp_millis(value.as_f64()?.round() as i64)
}

fn workout_record(item: &Value, parent: &Value, window: &Window) -> Option<(String, Value)> {
    let start = millis(&item["start_time"])?;
    let end = millis(&item["end_time"])?;
    if start >= end || !window.contains(start, end) {
        return None;
    }
    let workout = item["likely_name"]
        .as_str()
        .filter(|label| !label.trim().is_empty() && label.len() <= 256)
        .or_else(|| {
            parent
                .as_str()
                .filter(|label| !label.trim().is_empty() && label.len() <= 256)
        })
        .unwrap_or("workout");
    let id = account_id(&item["id"])
        .unwrap_or_else(|| format!("{}:{}", start.timestamp_millis(), end.timestamp_millis()));
    let row = record(
        "workout",
        SOURCE,
        &id,
        start,
        end,
        window.end,
        json!({
            "exercise_type":workout,"duration_seconds":(end-start).num_seconds(),
            "provider_data":item,
        }),
    );
    Some((id, row))
}

async fn daily_sleep_at(
    client: &Client,
    token: &str,
    window: Window,
    sensorbio_origin: &str,
) -> Result<Vec<Value>> {
    let url = format!("{sensorbio_origin}/v1/sleep");
    let mut day = (window.start - CalendarDuration::days(1)).date_naive();
    let mut rows = Vec::new();
    while day <= (window.end + CalendarDuration::days(1)).date_naive() {
        let observed = Utc::now();
        let response = body(
            client
                .get(&url)
                .bearer_auth(token)
                .query(&[("date", day.to_string())])
                .send()
                .await?,
        )
        .await?;
        let items = &response["data"];
        let items = records(items)?;
        let first_day_row = rows.len();
        for item in items {
            if let Some(mut row) = ring_sleep_point(item, false, &window)? {
                row["modified_at"] = json!(observed);
                rows.push(row);
            }
        }
        let summary_date = day.and_hms_opt(0, 0, 0).unwrap().and_utc();
        let summary_in_window = window.contains(summary_date, summary_date);
        if rows.len() > first_day_row || (summary_in_window && !items.is_empty()) {
            let details = optional(
                client
                    .get(format!("{sensorbio_origin}/v1/sleep/details/day"))
                    .bearer_auth(token)
                    .query(&[("date", day.to_string())]),
            )
            .await?;
            if let Some(details) = details {
                ensure!(
                    details
                        .get("date")
                        .is_none_or(|value| value.as_str().and_then(|value| value.parse().ok())
                            == Some(day)),
                    "unexpected Sensor Bio sleep detail day"
                );
                for row in &mut rows[first_day_row..] {
                    sensorbio_session_details(row, &details)?;
                }
                if summary_in_window {
                    rows.extend(sensorbio_main_sleep(&details, day, summary_date, observed));
                }
            }
        }
        if summary_in_window && day <= observed.date_naive() {
            let score = body(
                client
                    .get(format!("{sensorbio_origin}/v1/scores"))
                    .bearer_auth(token)
                    .query(&[("date", day.to_string())])
                    .send()
                    .await?,
            )
            .await?;
            if score["data"].is_object() {
                rows.extend(metric(
                    &score["data"],
                    "daily_scores",
                    "Sensor Bio",
                    summary_date,
                    summary_date,
                    &window,
                )?);
            }
        }
        let limit = 4_000;
        ensure!(rows.len() <= limit, "snapshot record limit exceeded");
        day = day.succ_opt().context("invalid sleep date")?;
    }
    Ok(rows)
}

fn sensorbio_stages(
    intervals: &[Value],
    started: DateTime<Utc>,
    ended: DateTime<Utc>,
) -> Vec<Value> {
    let mut candidates = intervals
        .iter()
        .filter_map(|interval| {
            let start = sensorbio_instant(&interval["start_time"])?;
            let end = sensorbio_instant(&interval["end_time"])?.min(ended);
            let stage = match interval["status"].as_str()?.to_ascii_lowercase().as_str() {
                "awake" => 1,
                "light" => 4,
                "deep" => 5,
                "rem" => 6,
                _ => return None,
            };
            (start >= started && start < end).then_some((start, end, stage))
        })
        .collect::<Vec<_>>();
    candidates.sort_unstable();
    let mut stages: Vec<(DateTime<Utc>, DateTime<Utc>, i32)> = Vec::new();
    for (start, end, stage) in candidates {
        if let Some(previous) = stages.last_mut() {
            if start < previous.1 {
                continue;
            }
            if start == previous.1 && stage == previous.2 {
                previous.1 = end;
                continue;
            }
        }
        stages.push((start, end, stage));
    }
    stages
        .into_iter()
        .map(|(start, end, stage)| json!({"start":start,"end":end,"stage":stage}))
        .collect()
}

fn sensorbio_session_details(row: &mut Value, details: &Value) -> Result<()> {
    let start = date(&row["started_at"])?;
    let end = date(&row["ended_at"])?;
    let payload = &mut row["payload"];
    if let Some(intervals) = details["sleep_stages"].as_array() {
        payload["stages"] = json!(sensorbio_stages(intervals, start, end));
    }
    let mut matching = details["all_sleeps_time_details"]
        .as_array()
        .into_iter()
        .flatten()
        .filter(|record| {
            record["start_ts_millis"].as_i64() == Some(start.timestamp_millis())
                && record["end_ts_millis"].as_i64() == Some(end.timestamp_millis())
        });
    if let Some(detail) = matching.next().filter(|_| matching.next().is_none()) {
        let timing = sensorbio_sleep_timing(
            detail,
            "sleep_onset_ts_millis",
            "slee_wakeup_ts_millis",
            "sleep_tz_offset_in_mins",
            start,
            end,
        );
        for (name, value) in timing {
            payload["sleep_timing"][name] = value;
        }
    }
    Ok(())
}

fn sensorbio_sleep_timing(
    item: &Value,
    onset: &str,
    wake: &str,
    offset: &str,
    start: DateTime<Utc>,
    end: DateTime<Utc>,
) -> serde_json::Map<String, Value> {
    let mut timing = serde_json::Map::new();
    for (field, target) in [(onset, "sleep_onset_at"), (wake, "woke_at")] {
        if let Some(at) = sensorbio_instant(&item[field]).filter(|at| (start..=end).contains(at)) {
            timing.insert(target.into(), json!(at));
        }
    }
    if let Some(minutes) = item[offset]
        .as_i64()
        .filter(|minutes| (-840..=840).contains(minutes))
    {
        timing.insert("offset_minutes".into(), json!(minutes));
    }
    timing
}

fn sensorbio_main_sleep(
    details: &Value,
    day: chrono::NaiveDate,
    at: DateTime<Utc>,
    observed: DateTime<Utc>,
) -> Option<Value> {
    let metrics = &details["main_sleep_detail_metrics"];
    let mut normalized = serde_json::Map::new();
    for (name, target) in [
        ("awake_duration_in_mins", "awake_seconds"),
        ("deep_sleep_duration_in_mins", "deep_seconds"),
        ("light_sleep_duration_in_mins", "light_seconds"),
        ("rem_sleep_duration_in_mins", "rem_seconds"),
        ("fall_asleep_latency_in_mins", "sleep_onset_latency_seconds"),
        ("total_sleep_duration_in_mins", "total_sleep_seconds"),
        ("total_time_in_bed_in_mins", "time_in_bed_seconds"),
    ] {
        if let Some(seconds) = metrics[name]
            .as_u64()
            .and_then(|minutes| minutes.checked_mul(60))
        {
            normalized.insert(target.into(), json!(seconds));
        }
    }
    for name in ["number_of_awakenings", "sleep_score"] {
        if let Some(value) = metrics[name].as_u64() {
            normalized.insert(name.into(), json!(value));
        }
    }
    let mut percentages = serde_json::Map::new();
    for stage in ["awake", "light", "deep", "rem"] {
        if let Some(value) = details["main_sleep_stages_percentages"][stage]
            .as_f64()
            .filter(|value| (0.0..=100.0).contains(value))
        {
            percentages.insert(stage.into(), json!(value));
        }
    }
    if normalized.is_empty() && percentages.is_empty() {
        return None;
    }
    Some(record(
        "metric",
        "Sensor Bio",
        &format!("main_sleep_details:{day}"),
        at,
        at,
        observed,
        json!({"metric":"main_sleep_details","time_precision":"provider_calendar_date",
               "provider_data":{"date":day,"metrics":normalized,"stage_percentages":percentages}}),
    ))
}

fn ring_sleep_point(item: &Value, ultra: bool, window: &Window) -> Result<Option<Value>> {
    ensure!(
        !ultra,
        "Ultrahuman sleep belongs to its Watch Tower collector"
    );
    let started =
        sensorbio_instant(&item["start_timestamp"]).context("invalid Sensor Bio sleep start")?;
    let ended =
        sensorbio_instant(&item["end_timestamp"]).context("invalid Sensor Bio sleep end")?;
    if started >= ended || !window.contains(started, ended) {
        return Ok(None);
    }
    let id = item["id"]
        .as_str()
        .map(str::to_owned)
        .or_else(|| item["id"].as_i64().map(|id| id.to_string()))
        .unwrap_or_else(|| format!("{started}/{ended}"));
    let mut metrics = serde_json::Map::new();
    for (value, name, max) in [
        (&item["avg_heart_rate"], "average_heart_rate_bpm", 350.0),
        (
            &item["biometrics"]["resting_bpm"],
            "resting_heart_rate_bpm",
            350.0,
        ),
        (&item["biometrics"]["hrv"], "hrv_rmssd_ms", f64::MAX),
        (
            &item["biometrics"]["resting_hrv"],
            "resting_hrv_rmssd_ms",
            f64::MAX,
        ),
        (
            &item["biometrics"]["brpm"],
            "respiratory_rate_breaths_per_minute",
            f64::MAX,
        ),
        (&item["biometrics"]["spo2"], "spo2_percent", 100.0),
    ] {
        if let Some(value) = value.as_f64().filter(|value| *value > 0.0 && *value <= max) {
            metrics.insert(name.into(), json!(value));
        }
    }
    let timing = sensorbio_sleep_timing(
        item,
        "sleep_onset",
        "wakeup_time",
        "tz_offset_in_mins",
        started,
        ended,
    );
    let payload = json!({"stages":[],"provider_stage_summary":{
        "total_minutes":item["total_sleep_mins"],"deep_minutes":item["deep_sleep_mins"],
        "light_minutes":item["light_sleep_mins"],"rem_minutes":item["rem_sleep_mins"],"awake_minutes":item["awake_time_mins"]
    },"sleep_timing":timing,"sleep_metrics":metrics,
      "provider_data":{"biometrics":item["biometrics"],"score":item["score"]}});
    Ok(Some(record(
        "sleep", SOURCE, &id, started, ended, window.end, payload,
    )))
}

async fn sensorbio_at(
    client: &Client,
    token: &str,
    start: DateTime<Utc>,
    end: DateTime<Utc>,
    url: &str,
) -> Result<Vec<Value>> {
    let window = Window { start, end };
    let mut rows = Vec::new();
    let mut cursor = start.timestamp_millis().saturating_sub(1);
    for _ in 0..32 {
        let Some(result) = optional(
            client
                .get(url)
                .bearer_auth(token)
                .query(&[("last-timestamp", cursor), ("limit", 50)]),
        )
        .await?
        else {
            return Ok(rows);
        };
        let samples = records(&result["data"])?;
        if samples.is_empty() {
            return Ok(rows);
        }
        append(
            &mut rows,
            &result["data"],
            "Sensor Bio",
            "biometrics",
            &["timestamp"],
            &window,
        )?;
        for sample in samples {
            let at = DateTime::from_timestamp_millis(
                sample["timestamp"]
                    .as_i64()
                    .context("missing Sensor Bio timestamp")?,
            )
            .context("invalid Sensor Bio timestamp")?;
            if !window.contains(at, at) {
                continue;
            }
            let revision = window.end;
            let timestamp = at.timestamp_millis();
            if let Some(bpm) = sample["bpm"]
                .as_f64()
                .filter(|value| (1.0..=350.0).contains(value))
            {
                rows.push(record(
                    "heart_rate",
                    "Sensor Bio",
                    &format!("heart_rate:{timestamp}"),
                    at,
                    at,
                    revision,
                    json!({"unit":"bpm","samples":[{"at":at,"bpm":bpm}]}),
                ));
            }
            for (field, name, unit, max) in [
                ("hrv", "hrv_rmssd", "ms", f64::MAX),
                ("resting_hrv", "resting_hrv_rmssd", "ms", f64::MAX),
                ("resting_bpm", "resting_heart_rate", "bpm", 350.0),
                ("brpm", "respiratory_rate", "breaths/min", f64::MAX),
                ("spo2", "oxygen_saturation", "%", 100.0),
            ] {
                if let Some(value) = sample[field]
                    .as_f64()
                    .filter(|value| *value > 0.0 && *value <= max)
                {
                    rows.push(record(
                        "metric",
                        "Sensor Bio",
                        &format!("{name}:{timestamp}"),
                        at,
                        at,
                        revision,
                        json!({"metric":name,"provider_data":{"value":value,"unit":unit}}),
                    ));
                }
            }
        }
        ensure!(
            rows.len() <= MAX_RECORDS,
            "Sensor Bio biometric record limit exceeded"
        );
        let next = samples
            .last()
            .and_then(|item| item["timestamp"].as_i64())
            .context("missing Sensor Bio cursor")?;
        ensure!(next > cursor, "Sensor Bio cursor did not advance");
        if next >= end.timestamp_millis() || samples.len() < 50 {
            return Ok(rows);
        }
        cursor = next;
    }
    bail!("Sensor Bio biometric pagination exceeded the limit")
}

#[derive(Debug)]
pub struct ProviderRateLimited {
    pub retry_at: DateTime<Utc>,
}

impl std::fmt::Display for ProviderRateLimited {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("Sensor Bio returned HTTP 429")
    }
}

impl std::error::Error for ProviderRateLimited {}

pub fn provider_retry_at(error: &anyhow::Error) -> Option<DateTime<Utc>> {
    error
        .downcast_ref::<ProviderRateLimited>()
        .map(|error| error.retry_at)
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

async fn body(response: Response) -> Result<Value> {
    let status = response.status();
    if status == reqwest::StatusCode::TOO_MANY_REQUESTS {
        let retry_at = response
            .headers()
            .get(reqwest::header::RETRY_AFTER)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| {
                value
                    .parse::<i64>()
                    .ok()
                    .and_then(CalendarDuration::try_seconds)
                    .and_then(|delay| Utc::now().checked_add_signed(delay))
                    .or_else(|| {
                        DateTime::parse_from_rfc2822(value)
                            .ok()
                            .map(|at| at.with_timezone(&Utc))
                    })
            })
            .unwrap_or_else(|| Utc::now() + CalendarDuration::minutes(5));
        return Err(ProviderRateLimited { retry_at }.into());
    }
    ensure!(
        status.is_success(),
        "Sensor Bio returned HTTP {}",
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
            "Sensor Bio response exceeds limit"
        );
        bytes.extend_from_slice(&chunk);
    }
    Ok(serde_json::from_slice(&bytes)?)
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

fn records(value: &Value) -> Result<&[Value]> {
    value
        .as_array()
        .map(Vec::as_slice)
        .context("missing Sensor Bio record list")
}

fn account_id(value: &Value) -> Option<String> {
    value
        .as_str()
        .filter(|id| !id.is_empty() && id.len() <= 256)
        .map(str::to_owned)
        .or_else(|| value.as_u64().map(|id| id.to_string()))
}

fn date(value: &Value) -> Result<DateTime<Utc>> {
    value
        .as_str()
        .context("missing timestamp")?
        .parse()
        .context("invalid timestamp")
}

fn sensorbio_instant(value: &Value) -> Option<DateTime<Utc>> {
    if let Some(number) = value.as_i64() {
        return if number > 10_000_000_000 {
            DateTime::from_timestamp_millis(number)
        } else {
            DateTime::from_timestamp(number, 0)
        };
    }
    let value = value.as_str()?;
    value.parse::<DateTime<Utc>>().ok()
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

fn metric(
    item: &Value,
    name: &str,
    source: &str,
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
        source,
        &format!("{name}:{id}"),
        start,
        end,
        window.end,
        json!({"metric":name,"provider_data":item}),
    )))
}

fn append(
    rows: &mut Vec<Value>,
    items: &Value,
    source: &str,
    name: &str,
    keys: &[&str],
    window: &Window,
) -> Result<()> {
    for item in records(items)? {
        let Some(at) = keys.iter().find_map(|key| sensorbio_instant(&item[*key])) else {
            continue;
        };
        if window.contains(at, at) {
            let id = account_id(&item["id"]).unwrap_or_else(|| at.to_rfc3339());
            rows.push(record(
                "metric",
                source,
                &format!("{name}:{id}"),
                at,
                at,
                window.end,
                json!({"metric":name,"time_precision":"instant","provider_data":item}),
            ));
        }
    }
    Ok(())
}

#[cfg(test)]
#[path = "sensorbio_tests.rs"]
mod tests;
