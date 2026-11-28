use super::*;
use chrono::{NaiveDate, NaiveDateTime};

#[cfg(test)]
#[path = "vendor_metrics_tests.rs"]
mod tests;

pub(super) async fn optional(request: reqwest::RequestBuilder) -> Result<Option<Value>> {
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
    let indexed = NaiveDateTime::parse_from_str(text, "%Y-%m-%dT%H:%M:%S%.f")
        .ok()?
        .and_utc();
    Some((indexed, "provider_civil_time"))
}

#[cfg(test)]
pub(super) fn sensorbio_instant(value: &Value) -> Option<DateTime<Utc>> {
    point(value)
        .filter(|(_, precision)| *precision == "instant")
        .map(|(at, _)| at)
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
        let id = item["id"]
            .as_str()
            .map(str::to_owned)
            .or_else(|| item["id"].as_i64().map(|n| n.to_string()))
            .unwrap_or_else(|| at.to_rfc3339());
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

#[cfg(test)]
pub(super) async fn polar(
    client: &Client,
    token: &str,
    start: DateTime<Utc>,
    end: DateTime<Utc>,
) -> Result<Vec<Value>> {
    let window = Window { start, end };
    polar_at(
        client,
        token,
        &window,
        "https://www.polaraccesslink.com/v3/users",
        Utc::now().date_naive(),
    )
    .await
}

#[cfg(test)]
async fn polar_at(
    client: &Client,
    token: &str,
    window: &Window,
    base_url: &str,
    today: NaiveDate,
) -> Result<Vec<Value>> {
    let Window { start, end } = window;
    let start_day = start.date_naive();
    let end_day = end.date_naive();
    let earliest_activity = today - CalendarDuration::days(365);
    let to = end_day.to_string();
    let mut rows = Vec::new();
    for (path, name) in [
        ("continuous-heart-rate", "continuous_heart_rate"),
        ("biosensing/bodytemperature", "body_temperature"),
        ("biosensing/skintemperature", "sleep_skin_temperature"),
        ("biosensing/skincontacts", "skin_contact"),
        ("biosensing/ecg", "ecg"),
        ("biosensing/spo2", "spot_spo2"),
        ("activities", "daily_activity"),
        ("activities/samples", "activity_samples"),
        ("cardio-load/date", "cardio_load"),
        ("sleepwise/alertness/date", "sleepwise_alertness"),
        (
            "sleepwise/circadian-bedtime/date",
            "sleepwise_circadian_bedtime",
        ),
    ] {
        // These two ranges reject older dates even when the scheduled job was valid when queued.
        let from = if matches!(name, "daily_activity" | "activity_samples") {
            start_day.max(earliest_activity)
        } else {
            start_day
        };
        if from > end_day {
            continue;
        }
        let from = from.to_string();
        let mut request = client
            .get(format!("{base_url}/{path}"))
            .bearer_auth(token)
            .query(&[("from", &from), ("to", &to)]);
        if name == "daily_activity" {
            request = request.query(&[("inactivity_stamps", true)]);
        }
        let Some(result) = optional(request).await? else {
            continue;
        };
        if path.starts_with("biosensing/") {
            polar_biosensing(&mut rows, &result, name, window)?;
            continue;
        }
        if name == "continuous_heart_rate" {
            let items = if result["date"].is_string() {
                std::slice::from_ref(&result)
            } else {
                records(&result["heart_rates"])?.as_slice()
            };
            for item in items {
                super::polar_series::continuous(&mut rows, item, window)?;
            }
            continue;
        }
        if matches!(name, "sleepwise_alertness" | "sleepwise_circadian_bedtime") {
            for item in records(&result)? {
                let (Some((start, _)), Some((end, _))) = (
                    point(&item["period_start_time"]),
                    point(&item["period_end_time"]),
                ) else {
                    continue;
                };
                // AccessLink documents these fields as UTC even when they omit the Z suffix.
                if start < end && window.contains(start, end) {
                    rows.push(record(
                        "metric",
                        "Polar",
                        &format!("{name}:{start}"),
                        start,
                        end,
                        window.end,
                        json!({"metric":name,"time_precision":"instant","provider_data":item}),
                    ));
                }
            }
            continue;
        }
        append(
            &mut rows,
            &result,
            "Polar",
            name,
            &[
                "measurement_time",
                "start_time",
                "sleep_date",
                "date",
                "test_time",
                "created",
            ],
            window,
        )?;
        if name == "activity_samples" {
            for item in records(&result)? {
                super::polar_series::activity_steps(&mut rows, item, window)?;
            }
        }
    }
    Ok(rows)
}

#[cfg(test)]
fn polar_biosensing(
    rows: &mut Vec<Value>,
    response: &Value,
    name: &str,
    window: &Window,
) -> Result<()> {
    for item in records(response)? {
        let (start, end, precision) = match name {
            "body_temperature" | "skin_contact" => {
                let Some((start, _)) = point(&item["start_time"]) else {
                    continue;
                };
                let Some((end, _)) = point(&item["end_time"]) else {
                    continue;
                };
                (start, end, "instant")
            }
            "sleep_skin_temperature" => {
                let Some((at, precision)) = point(&item["sleep_date"]) else {
                    continue;
                };
                (at, at, precision)
            }
            _ => {
                let Some(at) = item["test_time"]
                    .as_i64()
                    .and_then(|time| DateTime::from_timestamp(time, 0))
                else {
                    continue;
                };
                (at, at, "instant")
            }
        };
        if name == "body_temperature" {
            for sample in item["samples"].as_array().into_iter().flatten() {
                let Some(delta) = sample["recording_time_delta_milliseconds"]
                    .as_i64()
                    .filter(|delta| *delta >= 0)
                else {
                    continue;
                };
                let Some(at) = CalendarDuration::try_milliseconds(delta)
                    .and_then(|delta| start.checked_add_signed(delta))
                else {
                    continue;
                };
                if at > end
                    || !window.contains(at, at)
                    || !sample["temperature_celsius"].is_number()
                {
                    continue;
                }
                let id = polar_biosensing_id("body_temperature_sample", start, item);
                rows.push(record(
                    "metric", "Polar", &format!("{id}:{delta}"), at, at, window.end,
                    json!({"metric":"body_temperature_sample","time_precision":"instant","provider_data":{
                        "value":sample["temperature_celsius"],"unit":"Cel","measurement_type":item["measurement_type"],
                        "sensor_location":item["sensor_location"],"recording_time_delta_milliseconds":delta
                    }}),
                ));
            }
        }
        if window.contains(start, end) {
            let mut payload =
                json!({"metric":name,"time_precision":precision,"provider_data":item});
            if name == "ecg" {
                payload["signal_unit"] = json!("mV");
                payload["elapsed_time_unit"] = json!("ms");
            }
            rows.push(record(
                "metric",
                "Polar",
                &polar_biosensing_id(name, start, item),
                start,
                end,
                window.end,
                payload,
            ));
        }
    }
    Ok(())
}

#[cfg(test)]
fn polar_biosensing_id(name: &str, start: DateTime<Utc>, item: &Value) -> String {
    [
        name,
        &start.to_rfc3339(),
        item["source_device_id"].as_str().unwrap_or_default(),
        item["measurement_type"].as_str().unwrap_or_default(),
        item["sensor_location"].as_str().unwrap_or_default(),
    ]
    .join(":")
}

#[cfg(test)]
pub(super) async fn suunto(
    client: &Client,
    token: &str,
    key: &str,
    start: DateTime<Utc>,
    end: DateTime<Utc>,
) -> Result<Vec<Value>> {
    suunto_at(
        client,
        token,
        key,
        Window { start, end },
        "https://cloudapi.suunto.com",
    )
    .await
}

#[cfg(test)]
async fn suunto_at(
    client: &Client,
    token: &str,
    key: &str,
    window: Window,
    origin: &str,
) -> Result<Vec<Value>> {
    let observed_at = Utc::now();
    let Window { start, end } = window;
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
                .header("Ocp-Apim-Subscription-Key", key)
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
            .header("Ocp-Apim-Subscription-Key", key)
            .query(&params),
    )
    .await?
    {
        rows.extend(suunto_daily(&result, &window)?);
    }
    for row in &mut rows {
        row["modified_at"] = json!(observed_at);
    }
    Ok(rows)
}

#[cfg(test)]
fn suunto_daily(data: &Value, window: &Window) -> Result<Vec<Value>> {
    let mut rows = Vec::new();
    let mut daily = std::collections::BTreeMap::<_, (DateTime<Utc>, Value)>::new();
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
                        detail["value"] = json!(joules / 4184.0);
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
                .filter(|v| !v.is_empty() && v.len() < 64)
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

#[cfg(test)]
fn suunto_rows(data: &Value) -> Result<&Vec<Value>> {
    records(data.get("payload").unwrap_or(data))
}

#[cfg(test)]
fn suunto_series(data: &Value, name: &str, window: &Window) -> Result<Vec<Value>> {
    let mut samples = std::collections::BTreeMap::new();
    let mut sleep = std::collections::BTreeMap::new();
    for item in suunto_rows(data)? {
        if let Some((at, precision)) = point(&item["timestamp"]) {
            if window.contains(at, at) {
                let id = crate::wearable_webhooks::account_id(&item["entryData"]["SleepId"]);
                samples.insert((at, id), (precision, item));
            }
        }
        if name == "sleep_samples" {
            let detail = &item["entryData"];
            if let Some(id) = crate::wearable_webhooks::account_id(&detail["SleepId"]) {
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

pub(crate) fn suunto_sleep_details(payload: &mut Value) {
    let detail = &payload["provider_data"];
    if !detail.is_object() {
        return;
    }
    let durations = [
        ("DeepSleepDuration", "deep_seconds"),
        ("LightSleepDuration", "light_seconds"),
        ("REMSleepDuration", "rem_seconds"),
        ("SleepOnsetLatencyDuration", "sleep_onset_latency_seconds"),
        (
            "WakeAfterSleepOnsetDuration",
            "wake_after_sleep_onset_seconds",
        ),
        ("WakeBeforeOffBedDuration", "wake_before_off_bed_seconds"),
    ]
    .into_iter()
    .filter_map(|(field, name)| Some((name.to_owned(), json!(detail[field].as_u64()?))))
    .collect::<serde_json::Map<_, _>>();
    let max_spo2 = detail["MaxSpo2"]
        .as_f64()
        .filter(|value| *value > 0.0 && *value <= 100.0)
        .map(|value| if value <= 1.0 { value * 100.0 } else { value });
    let metrics = json!({
        "max_spo2_percent":max_spo2,
        "average_heart_rate_bpm":detail["HRAvg"].as_f64().filter(|value| (1.0..=350.0).contains(value)),
        "minimum_heart_rate_bpm":detail["HRMin"].as_f64().filter(|value| (1.0..=350.0).contains(value)),
        "average_hrv_ms":detail["AvgHRV"].as_f64().filter(|value| *value>0.0),
        "hrv_sample_count":detail["AvgHRVSampleCount"].as_u64()
    });
    let nap = detail["IsNap"].as_bool();
    let score = detail["SleepQualityScore"]
        .as_u64()
        .filter(|score| *score <= 100);
    payload["provider_stage_summary"] = Value::Object(durations);
    payload["sleep_metrics"] = metrics;
    payload["nap"] = json!(nap);
    payload["sleep_score"] = json!(score);
}

#[cfg(test)]
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
                .map(|v| v / 4184.0),
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
            rows.push(record("metric","Suunto",&format!("{name}:{at}"),at,at,window.end,
                json!({"metric":name,"time_precision":"instant","provider_data":{"value":value,"unit":unit}}),
            ));
        }
    }
}

#[cfg(test)]
pub(super) async fn garmin(
    client: &Client,
    token: &str,
    start: DateTime<Utc>,
    end: DateTime<Utc>,
) -> Result<Vec<Value>> {
    let window = Window { start, end };
    let from = start.date_naive().to_string();
    let to = end.date_naive().to_string();
    let mut rows = Vec::new();
    let get = |path: &str| {
        client
            .get(format!("{}/{path}", super::garmin::BASE))
            .bearer_auth(token)
    };
    let stress = format!("usersummary-service/stats/stress/daily/{from}/{to}");
    if let Some(data) = optional(get(&stress)).await? {
        append(
            &mut rows,
            &data,
            "Garmin Connect",
            "daily_stress",
            &["calendarDate"],
            &window,
        )?;
    }
    if let Some(data) = optional(get(&format!("hrv-service/hrv/daily/{from}/{to}"))).await? {
        append(
            &mut rows,
            &data["hrvSummaries"],
            "Garmin Connect",
            "daily_hrv",
            &["calendarDate"],
            &window,
        )?;
    }
    if let Some(data) = optional(
        get("wellness-service/wellness/bodyBattery/reports/daily")
            .query(&[("startDate", &from), ("endDate", &to)]),
    )
    .await?
    {
        append(
            &mut rows,
            &data,
            "Garmin Connect",
            "body_battery",
            &["date"],
            &window,
        )?;
    }
    Ok(rows)
}

#[cfg(test)]
pub(super) async fn withings(
    client: &Client,
    token: &str,
    start: DateTime<Utc>,
    end: DateTime<Utc>,
) -> Result<Vec<Value>> {
    withings_at(
        client,
        token,
        &Window { start, end },
        "https://wbsapi.withings.net/v2/measure",
    )
    .await
}

#[cfg(test)]
async fn withings_at(
    client: &Client,
    token: &str,
    window: &Window,
    endpoint: &str,
) -> Result<Vec<Value>> {
    let mut rows = Vec::new();
    for action in ["getactivity", "getworkouts"] {
        withings_daily_pages(client, token, window, endpoint, action, &mut rows).await?;
    }
    // The API truncates requests wider than 24 hours.
    let mut start = window.start;
    while start < window.end {
        let end = (start + CalendarDuration::days(1)).min(window.end);
        let request = client.post(endpoint).bearer_auth(token).form(&[
            ("action", "getintradayactivity"),
            ("startdate", &start.timestamp().to_string()),
            ("enddate", &end.timestamp().to_string()),
        ]);
        if let Some(page) = super::withings_detail::page(request).await? {
            withings_intraday(
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

#[cfg(test)]
async fn withings_daily_pages(
    client: &Client,
    token: &str,
    window: &Window,
    endpoint: &str,
    action: &str,
    rows: &mut Vec<Value>,
) -> Result<()> {
    let from = (window.start - CalendarDuration::days(1))
        .date_naive()
        .to_string();
    let to = (window.end + CalendarDuration::days(1))
        .date_naive()
        .to_string();
    let mut offset = 0;
    for _ in 0..32 {
        let Some(page) =
            super::withings_detail::page(client.post(endpoint).bearer_auth(token).form(&[
                ("action", action),
                ("startdateymd", &from),
                ("enddateymd", &to),
                ("offset", &offset.to_string()),
            ]))
            .await?
        else {
            ensure!(offset == 0, "incomplete Withings {action} query");
            return Ok(());
        };
        if action == "getactivity" {
            append(
                rows,
                &page["activities"],
                "Withings",
                "daily_activity",
                &["date"],
                window,
            )?;
        } else {
            for item in records(&page["series"])? {
                rows.extend(withings_workout(item, window)?);
            }
        }
        if page["more"].as_bool() != Some(true) && page["more"].as_u64() != Some(1) {
            return Ok(());
        }
        let next = integer(&page["offset"])?;
        ensure!(next > offset, "invalid Withings {action} cursor");
        offset = next;
    }
    bail!("incomplete Withings {action} query")
}

#[cfg(test)]
fn withings_workout(item: &Value, window: &Window) -> Result<Option<Value>> {
    let timestamp = |field: &str| {
        item[field]
            .as_i64()
            .and_then(|seconds| DateTime::from_timestamp(seconds, 0))
            .context("invalid Withings workout time")
    };
    let start = timestamp("startdate")?;
    let end = timestamp("enddate")?;
    if !(start < end && window.contains(start, end)) {
        return Ok(None);
    }
    let id = integer(&item["id"])?;
    let exercise_type = integer(&item["category"])?;
    Ok(Some(record(
        "workout",
        "Withings",
        &id.to_string(),
        start,
        end,
        window.end,
        json!({
            "exercise_type": exercise_type,
            "duration_seconds": (end - start).num_seconds(),
            "calories_kcal": item["data"]["calories"],
            "distance_meters": item["data"]["distance"],
            "provider_data": item
        }),
    )))
}

pub(crate) fn withings_workout_details(payload: &mut Value) {
    let Some(data) = payload
        .pointer("/provider_data/data")
        .and_then(Value::as_object)
    else {
        return;
    };
    let duration = payload["duration_seconds"].as_f64().unwrap_or(0.0);
    let value_in = |key, minimum, maximum| {
        let raw = data.get(key)?;
        raw.as_f64()
            .filter(|value| (minimum..=maximum).contains(value))
            .map(|_| raw.clone())
    };
    let mut details = serde_json::Map::new();
    for (field, name, minimum, maximum) in [
        ("hr_average", "average_heart_rate_bpm", 1.0, 350.0),
        ("hr_min", "min_heart_rate_bpm", 1.0, 350.0),
        ("hr_max", "max_heart_rate_bpm", 1.0, 350.0),
        ("spo2_average", "average_spo2_percent", 0.0, 100.0),
        ("steps", "step_count", 0.0, f64::MAX),
        ("elevation", "floors_climbed", 0.0, f64::MAX),
        ("pool_laps", "swimming_lap_count", 0.0, f64::MAX),
        ("strokes", "swimming_stroke_count", 0.0, f64::MAX),
        ("manual_distance", "manual_distance_meters", 0.0, f64::MAX),
        ("manual_calories", "manual_calories_kcal", 0.0, f64::MAX),
        ("pause_duration", "manual_pause_seconds", 0.0, duration),
        ("algo_pause_duration", "device_pause_seconds", 0.0, duration),
    ] {
        if let Some(value) = value_in(field, minimum, maximum) {
            details.insert(name.to_owned(), value);
        }
    }
    let zones: Vec<_> = ["hr_zone_0", "hr_zone_1", "hr_zone_2", "hr_zone_3"]
        .iter()
        .enumerate()
        .filter_map(|(index, field)| {
            let seconds = value_in(*field, 0.0, duration)?;
            Some(json!({"index":index,"duration_seconds":seconds}))
        })
        .collect();
    if !zones.is_empty() {
        details.insert("heart_rate_zones".to_owned(), json!(zones));
    }
    if let Some(object) = payload.as_object_mut() {
        object.extend(details);
    }
}

#[cfg(test)]
fn withings_intraday(
    rows: &mut Vec<Value>,
    series: &Value,
    window: &Window,
    revision: DateTime<Utc>,
) -> Result<()> {
    if series.as_array().is_some_and(Vec::is_empty) {
        return Ok(());
    }
    for (epoch, sample) in series
        .as_object()
        .context("missing Withings intraday series")?
    {
        let at =
            DateTime::from_timestamp(epoch.parse()?, 0).context("invalid Withings sample time")?;
        // Keep the boundary sample only on the request whose start includes it.
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
            .filter(|bpm| (1.0..=350.0).contains(bpm))
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
        ensure!(
            rows.len() <= wearables::HIGH_VOLUME_CLOUD_RECORD_LIMIT,
            "Withings activity record limit exceeded"
        );
    }
    Ok(())
}

#[cfg(test)]
pub(super) async fn sensorbio(
    client: &Client,
    token: &str,
    start: DateTime<Utc>,
    end: DateTime<Utc>,
) -> Result<Vec<Value>> {
    sensorbio_at(
        client,
        token,
        start,
        end,
        "https://api.sensorbio.com/v1/biometrics",
    )
    .await
}

#[cfg(test)]
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
            rows.len() <= wearables::HIGH_VOLUME_CLOUD_RECORD_LIMIT,
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
