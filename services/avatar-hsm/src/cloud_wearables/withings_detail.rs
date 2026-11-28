use super::*;

#[cfg(test)]
#[path = "withings_detail_tests.rs"]
mod tests;

#[cfg(test)]
pub(super) async fn page(request: reqwest::RequestBuilder) -> Result<Option<Value>> {
    let response = request.send().await.map_err(|error| error.without_url())?;
    if response.status() == reqwest::StatusCode::NO_CONTENT {
        return Ok(None);
    }
    let mut result = body(response).await?;
    ensure!(
        result["status"].as_u64() == Some(0),
        "Withings API returned an error"
    );
    ensure!(result["body"].is_object(), "missing Withings response body");
    Ok(Some(result["body"].take()))
}

fn epoch(value: &Value) -> Result<DateTime<Utc>> {
    value
        .as_i64()
        .and_then(|value| DateTime::from_timestamp(value, 0))
        .context("invalid Withings timestamp")
}

#[cfg(test)]
pub(super) async fn sleep_segments(
    client: &Client,
    token: &str,
    endpoint: &str,
    summary: &Value,
) -> Result<(Vec<Value>, Vec<Value>)> {
    let start = epoch(&summary["startdate"])?;
    let end = epoch(&summary["enddate"])?;
    let mut segments = Vec::new();
    let mut from = start;
    while from < end {
        // The documented endpoint truncates requests wider than 24 hours.
        let to = (from + CalendarDuration::days(1)).min(end);
        let request = client.post(endpoint).bearer_auth(token).form(&[
            ("action", "get"),
            ("startdate", &from.timestamp().to_string()),
            ("enddate", &to.timestamp().to_string()),
            ("data_fields", "hr,rr,snoring,sdnn_1,rmssd,hrv_quality,mvt_score,chest_movement_rate,withings_index,breathing_sounds"),
        ]);
        if let Some(result) = page(request).await? {
            let series = &result["series"];
            let items: &[Value] = match series {
                Value::Array(items) => items,
                Value::Object(item) if item.is_empty() => &[],
                Value::Object(_) => std::slice::from_ref(series),
                _ => bail!("missing Withings sleep segments"),
            };
            for item in items {
                let segment_start = epoch(&item["startdate"])?;
                let segment_end = epoch(&item["enddate"])?;
                let same_model = summary["model_id"].is_null()
                    || item["model_id"].is_null()
                    || summary["model_id"] == item["model_id"];
                if same_model
                    && segment_start < end
                    && start < segment_end
                    && segment_start < segment_end
                {
                    segments.push((segment_start, segment_end, item.clone()));
                }
            }
        }
        from = to;
    }
    segments.sort_by_key(|(start, end, _)| (*start, *end));
    segments.dedup();
    let stages = sleep_stages(&segments, start, end);
    let raw = segments.into_iter().map(|(_, _, item)| item).collect();
    Ok((stages, raw))
}

#[cfg(test)]
fn sleep_stages(
    segments: &[(DateTime<Utc>, DateTime<Utc>, Value)],
    start: DateTime<Utc>,
    end: DateTime<Utc>,
) -> Vec<Value> {
    let mut stages = Vec::new();
    let mut previous_end = start;
    for (from, to, item) in segments {
        let (from, to) = (start.max(*from), end.min(*to));
        let provider_stage = &item["state"];
        let stage = match provider_stage.as_i64() {
            Some(0 | 15) => 1,
            Some(1) => 4,
            Some(2) => 5,
            Some(3) => 6,
            _ => continue,
        };
        // Multiple devices can supply conflicting stages. Preserve their source data.
        if from < previous_end || stages.len() == 500 {
            return Vec::new();
        }
        previous_end = to;
        stages.push(json!({"start":from,"end":to,"stage":stage,"provider_stage":provider_stage}));
    }
    stages
}

pub(crate) fn sleep_summary(payload: &mut Value) {
    let raw = &payload["provider_data"];
    let (Ok(start), Ok(end)) = (epoch(&raw["startdate"]), epoch(&raw["enddate"])) else {
        return;
    };
    let seconds = (end - start).num_seconds();
    if seconds <= 0 {
        return;
    }

    let data = &raw["data"];
    let duration = |field: &str| {
        data[field]
            .as_i64()
            .filter(|value| (0..=seconds).contains(value))
    };
    let mut metrics = serde_json::Map::new();
    for (field, name) in [
        ("total_timeinbed", "time_in_bed_seconds"),
        ("lightsleepduration", "light_seconds"),
        ("deepsleepduration", "deep_seconds"),
        ("remsleepduration", "rem_seconds"),
        ("wakeupduration", "awake_seconds"),
        ("sleep_latency", "sleep_latency_seconds"),
        ("wakeup_latency", "wakeup_latency_seconds"),
        ("waso", "wake_after_sleep_onset_seconds"),
        ("snoring", "snoring_seconds"),
        ("breathing_sounds", "breathing_sound_seconds"),
        ("mvt_active_duration", "bed_movement_seconds"),
    ] {
        if let Some(value) = duration(field) {
            metrics.insert(name.to_owned(), json!(value));
        }
    }
    for (field, name) in [
        ("wakeupcount", "wake_count"),
        ("out_of_bed_count", "out_of_bed_count"),
        ("nb_rem_episodes", "rem_episode_count"),
        ("snoringepisodecount", "snoring_episode_count"),
        (
            "breathing_sounds_episode_count",
            "breathing_sound_episode_count",
        ),
    ] {
        if let Some(count) = data[field].as_u64() {
            metrics.insert(name.to_owned(), json!(count));
        }
    }
    for (field, name, min, max) in [
        ("hr_average", "average_heart_rate_bpm", 1.0, 350.0),
        ("hr_min", "minimum_heart_rate_bpm", 1.0, 350.0),
        ("hr_max", "maximum_heart_rate_bpm", 1.0, 350.0),
        ("rr_average", "average_respiratory_rate_brpm", 0.0, f64::MAX),
        ("rr_min", "minimum_respiratory_rate_brpm", 0.0, f64::MAX),
        ("rr_max", "maximum_respiratory_rate_brpm", 0.0, f64::MAX),
        (
            "rmssd_start_avg",
            "start_average_hrv_rmssd_ms",
            0.0,
            f64::MAX,
        ),
        ("rmssd_end_avg", "end_average_hrv_rmssd_ms", 0.0, f64::MAX),
        ("mvt_score_avg", "bed_movement_average_score", 0.0, 255.0),
        (
            "core_body_temperature_min",
            "minimum_core_body_temperature_celsius",
            0.0,
            100.0,
        ),
        (
            "core_body_temperature_max",
            "maximum_core_body_temperature_celsius",
            0.0,
            100.0,
        ),
        (
            "core_body_temperature_avg",
            "average_core_body_temperature_celsius",
            0.0,
            100.0,
        ),
    ] {
        if data[field]
            .as_f64()
            .is_some_and(|value| (min..=max).contains(&value))
        {
            metrics.insert(name.to_owned(), data[field].clone());
        }
    }

    let asleep = duration("total_sleep_time")
        .map(|value| (value, "total_sleep_time"))
        .or_else(|| duration("asleepduration").map(|value| (value, "asleepduration")))
        .or_else(|| {
            let total = duration("lightsleepduration")?
                + duration("deepsleepduration")?
                + duration("remsleepduration")?;
            (total <= seconds).then_some((total, "stage_durations"))
        });
    if let Some((asleep, source)) = asleep {
        metrics.insert("asleep_seconds".to_owned(), json!(asleep));
        metrics.insert("asleep_seconds_source".to_owned(), json!(source));
    }
    let provider_ratio = data["sleep_efficiency"]
        .as_f64()
        .filter(|value| (0.0..=1.0).contains(value));
    let efficiency = provider_ratio
        .map(|ratio| (ratio, "provider_ratio"))
        .or_else(|| {
            let (asleep, _) = asleep?;
            let in_bed = duration("total_timeinbed")?;
            (in_bed > 0 && asleep <= in_bed)
                .then_some((asleep as f64 / in_bed as f64, "duration_ratio"))
        });
    if let Some((ratio, source)) = efficiency {
        metrics.insert(
            "sleep_efficiency_percent".to_owned(),
            json!((ratio * 10_000.0).round() / 100.0),
        );
        metrics.insert("sleep_efficiency_source".to_owned(), json!(source));
    }
    let events = sleep_summary_events(&data["night_events"], start, seconds);
    let score = data["sleep_score"]
        .as_f64()
        .filter(|value| (0.0..=100.0).contains(value))
        .map(|_| data["sleep_score"].clone());
    if !metrics.is_empty() {
        payload["sleep_metrics"] = Value::Object(metrics);
    }
    if !events.is_empty() {
        payload["sleep_events"] = json!(events);
    }
    if let Some(score) = score {
        payload["sleep_score"] = score;
    }
}

fn sleep_summary_events(raw: &Value, start: DateTime<Utc>, duration: i64) -> Vec<Value> {
    let mut events = Vec::new();
    for (code, kind) in [
        ("1", "got_in_bed"),
        ("2", "fell_asleep"),
        ("3", "woke_up"),
        ("4", "got_out_of_bed"),
        ("5", "manual_asleep_start"),
        ("6", "manual_awake_start"),
    ] {
        let Some(offsets) = raw[code].as_array() else {
            continue;
        };
        let mut offset = 0i64;
        for delta in offsets {
            // Each list is delta-encoded from the previous event of its own type.
            let Some(next) = delta
                .as_i64()
                .filter(|value| *value >= 0)
                .and_then(|value| offset.checked_add(value))
                .filter(|value| *value <= duration)
            else {
                break;
            };
            let Some(at) = start.checked_add_signed(CalendarDuration::seconds(next)) else {
                break;
            };
            events.push((at, kind));
            offset = next;
        }
    }
    events.sort_by_key(|(at, _)| *at);
    events
        .into_iter()
        .map(|(at, kind)| json!({"at":at,"kind":kind}))
        .collect()
}

#[cfg(test)]
pub(super) async fn ecg(
    client: &Client,
    token: &str,
    start: DateTime<Utc>,
    end: DateTime<Utc>,
) -> Result<Vec<Value>> {
    ecg_at(
        client,
        token,
        &Window { start, end },
        "https://wbsapi.withings.net/v2/heart",
    )
    .await
}

#[cfg(test)]
async fn ecg_at(
    client: &Client,
    token: &str,
    window: &Window,
    endpoint: &str,
) -> Result<Vec<Value>> {
    let mut offset = 0;
    let mut seen = HashSet::new();
    let mut rows = Vec::new();
    for _ in 0..32 {
        let request = client.post(endpoint).bearer_auth(token).form(&[
            ("action", "list"),
            ("startdate", &window.start.timestamp().to_string()),
            ("enddate", &window.end.timestamp().to_string()),
            ("offset", &offset.to_string()),
        ]);
        let Some(result) = page(request).await? else {
            ensure!(offset == 0, "incomplete Withings ECG list");
            return Ok(rows);
        };
        for item in records(&result["series"])? {
            let Some(id) = item["ecg"]["signalid"].as_u64().filter(|id| *id > 0) else {
                continue;
            };
            let at = epoch(&item["timestamp"])?;
            if !window.contains(at, at) || !seen.insert(id) {
                continue;
            }
            let request = client
                .post(endpoint)
                .bearer_auth(token)
                .form(&[("action", "get"), ("signalid", &id.to_string())]);
            let waveform = page(request).await?;
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
                "metric",
                "Withings",
                &format!("ecg:{id}"),
                at,
                at,
                window.end,
                json!({"metric":"ecg",
                    "provider_data":{"recording":item,"waveform":waveform,"signal_unit":"uV"}
                }),
            ));
        }
        if result["more"].as_bool() != Some(true) && result["more"].as_u64() != Some(1) {
            return Ok(rows);
        }
        let next = integer(&result["offset"])?;
        ensure!(next > offset, "invalid Withings ECG offset");
        offset = next;
    }
    bail!("incomplete Withings ECG query")
}
