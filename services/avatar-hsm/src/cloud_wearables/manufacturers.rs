use super::*;

#[cfg(test)]
pub(super) async fn oura(
    client: &Client,
    token: &str,
    start: DateTime<Utc>,
    end: DateTime<Utc>,
    scope: Option<&str>,
) -> Result<(Vec<Value>, Vec<&'static str>, wearables::OuraSleepDeletions)> {
    oura_at(
        client,
        token,
        Window { start, end },
        scope,
        "https://api.ouraring.com/v2/usercollection",
    )
    .await
}

#[cfg(test)]
async fn oura_at(
    client: &Client,
    token: &str,
    window: Window,
    scope: Option<&str>,
    base_url: &str,
) -> Result<(Vec<Value>, Vec<&'static str>, wearables::OuraSleepDeletions)> {
    let observed_at = Utc::now();
    let Window { start, end } = window;
    let scope = scope.filter(|scope| !scope.trim().is_empty());
    let from = (start - CalendarDuration::days(1))
        .format("%Y-%m-%d")
        .to_string();
    let to = (end + CalendarDuration::days(1))
        .format("%Y-%m-%d")
        .to_string();
    let mut rows = Vec::new();
    let mut grants = Vec::new();
    let mut deleted_sleep = HashSet::new();
    for (kind, required) in [("sleep", "daily"), ("workout", "workout")] {
        if !has_scope(scope, required) {
            continue;
        }
        grants.push(kind);
        let url = format!("{base_url}/{kind}");
        let request = client
            .get(url)
            .bearer_auth(token)
            .query(&[("start_date", &from), ("end_date", &to)]);
        rows.extend(
            cursor_records(request, "data", "next_token", "next_token", |item| {
                if kind == "sleep" && item["type"] == "deleted" && scope.is_some() {
                    if let Some(id) = item["id"]
                        .as_str()
                        .filter(|id| crate::wearable_fit::valid_id(id))
                    {
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
        if !has_scope(scope, required) {
            continue;
        }
        if scope.is_none() && (required != "daily" || endpoint == "rest_mode_period") {
            continue;
        }
        let request = client
            .get(format!("{base_url}/{endpoint}"))
            .bearer_auth(token)
            .query(&[("start_date", &from), ("end_date", &to)]);
        let first_page = if endpoint == "rest_mode_period" {
            let response = request
                .try_clone()
                .context("request cannot be paginated")?
                .send()
                .await?;
            // This endpoint may be unavailable for the connected account.
            if response.status() == reqwest::StatusCode::FORBIDDEN {
                continue;
            }
            Some(response)
        } else {
            None
        };
        rows.extend(
            cursor_records_from(
                request,
                "data",
                "next_token",
                "next_token",
                first_page,
                |item| {
                    let (at, end) = match endpoint {
                        "session" => (date(&item["start_datetime"])?, date(&item["end_datetime"])?),
                        "enhanced_tag" | "rest_mode_period" => {
                            // These documents can span sync windows or have a missing end time.
                            // Index by starting day and retain original times and any episodes.
                            required_text(&item["id"])?;
                            let at = oura_day(&item["start_day"])?;
                            (at, at)
                        }
                        _ => {
                            let at = oura_daily_timestamp(item)?;
                            (at, at)
                        }
                    };
                    let name = match endpoint {
                        "vO2_max" => "vo2_max",
                        "session" => "guided_session",
                        name => name,
                    };
                    metric(item, name, "Oura", at, end, &window)
                },
            )
            .await?,
        );
        if !grants.contains(&"metric") {
            grants.push("metric");
        }
    }
    if scope.is_some() && has_scope(scope, "heartrate") {
        let request = client
            .get(format!("{base_url}/heartrate"))
            .bearer_auth(token)
            .query(&[
                ("start_datetime", start.to_rfc3339()),
                ("end_datetime", end.to_rfc3339()),
            ]);
        rows.extend(
            cursor_records(request, "data", "next_token", "next_token", |item| {
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
                    window.end,
                    json!({"unit":"bpm","samples":[{"at":at,"bpm":bpm}]}),
                )))
            })
            .await?,
        );
        grants.push("heart_rate");
    }
    for row in &mut rows {
        row["modified_at"] = json!(observed_at);
    }
    Ok((
        rows,
        grants,
        wearables::OuraSleepDeletions {
            observed_at,
            ids: deleted_sleep.into_iter().collect(),
        },
    ))
}

pub(crate) fn oura_activity_classes(payload: &mut Value) {
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

pub(crate) fn oura_daily_timestamp(item: &Value) -> Result<DateTime<Utc>> {
    date(&item["timestamp"]).or_else(|_| oura_day(&item["day"]))
}

fn oura_day(value: &Value) -> Result<DateTime<Utc>> {
    let day: chrono::NaiveDate = required_text(value)?.parse()?;
    Ok(day.and_hms_opt(0, 0, 0).unwrap().and_utc())
}

pub(super) fn has_scope(scope: Option<&str>, expected: &str) -> bool {
    scope.filter(|scope| !scope.is_empty()).is_none_or(|scope| {
        scope
            .split(|c: char| c.is_whitespace() || c == ',')
            .any(|scope| scope == expected)
    })
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
        .or_else(|| item["cycle_id"].as_i64().map(|id| id.to_string()))
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

pub(crate) fn oura_webhook_session(
    item: &Value,
    kind: &str,
    observed_at: DateTime<Utc>,
) -> Result<wearables::Record> {
    let window = Window {
        start: DateTime::<Utc>::UNIX_EPOCH,
        end: observed_at + CalendarDuration::minutes(5),
    };
    let value = oura_point(item, kind, &window)?.context("invalid Oura session range")?;
    let mut record: wearables::Record = serde_json::from_value(value)?;
    record.modified_at = observed_at;
    Ok(record)
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
            json!({
                "stages": stages,
                "stage_resolution_seconds": resolution,
                "provider_total_sleep_seconds": item["total_sleep_duration"],
                "provider_data": item
            }),
        )
    } else {
        let start = date(&item["start_datetime"])?;
        let end = date(&item["end_datetime"])?;
        (
            start,
            end,
            json!({
                "exercise_type": required_text(&item["activity"])?,
                "title": item["label"],
                "duration_seconds": (end - start).num_seconds(),
                "distance_meters": item["distance"],
                "calories_kcal": item["calories"],
                "provider_data": item
            }),
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
            let until = (at + CalendarDuration::seconds(resolution)).min(end);
            let stage = match phase {
                b'1' => Some(5),
                b'2' => Some(4),
                b'3' => Some(6),
                b'4' => Some(1),
                _ => None,
            };
            if let Some(stage) = stage {
                match intervals.last_mut() {
                    Some((previous_stage, _, previous_end))
                        if *previous_stage == stage && *previous_end == at =>
                    {
                        *previous_end = until;
                    }
                    _ => intervals.push((stage, at, until)),
                }
            }
            at = until;
        }
        if (1..=500).contains(&intervals.len()) {
            let stages = intervals
                .into_iter()
                .map(|(stage, start, end)| json!({"stage":stage,"start":start,"end":end}))
                .collect();
            return (Some(resolution), stages);
        }
    }
    (None, Vec::new())
}

#[cfg(test)]
pub(super) async fn whoop(
    client: &Client,
    token: &str,
    start: DateTime<Utc>,
    end: DateTime<Utc>,
    scope: Option<&str>,
) -> Result<(Vec<Value>, Vec<&'static str>)> {
    whoop_at(
        client,
        token,
        Window { start, end },
        scope,
        "https://api.prod.whoop.com/developer/v2",
        Utc::now(),
    )
    .await
}

#[cfg(test)]
async fn whoop_at(
    client: &Client,
    token: &str,
    window: Window,
    scope: Option<&str>,
    origin: &str,
    now: DateTime<Utc>,
) -> Result<(Vec<Value>, Vec<&'static str>)> {
    let Window { start, end } = window;
    let scope = scope.filter(|scope| !scope.trim().is_empty());
    let mut rows = Vec::new();
    let mut grants = Vec::new();
    for kind in ["sleep", "workout"] {
        if !has_scope(scope, &format!("read:{kind}")) {
            continue;
        }
        grants.push(kind);
        let url = format!("{origin}/activity/{kind}");
        let request = client.get(url).bearer_auth(token).query(&[
            ("start", start.to_rfc3339()),
            ("end", end.to_rfc3339()),
            ("limit", "25".into()),
        ]);
        rows.extend(
            cursor_records(request, "records", "next_token", "nextToken", |item| {
                whoop_point(item, kind, &window)
            })
            .await?,
        );
    }
    for (name, required) in [("cycle", "read:cycles"), ("recovery", "read:recovery")] {
        // Older provisioned tokens were issued with only sleep and workout access.
        if scope.is_none() || !has_scope(scope, required) {
            continue;
        }
        let request = client
            .get(format!("{origin}/{name}"))
            .bearer_auth(token)
            .query(&[
                ("start", start.to_rfc3339()),
                ("end", end.to_rfc3339()),
                ("limit", "25".into()),
            ]);
        rows.extend(
            cursor_records(request, "records", "next_token", "nextToken", |item| {
                let at = date(item.get("start").unwrap_or(&item["created_at"]))?;
                let end = item
                    .get("end")
                    .filter(|v| !v.is_null())
                    .map(date)
                    .transpose()?
                    .unwrap_or(at);
                metric(item, name, "WHOOP", at, end, &window)
            })
            .await?,
        );
        if !grants.contains(&"metric") {
            grants.push("metric");
        }
        if name == "cycle" {
            grants.push("steps");
        }
    }
    let metrics: Vec<_> = rows
        .iter()
        .filter_map(|row| whoop_cycle_metrics(row, &window))
        .flatten()
        .collect();
    rows.extend(metrics);
    if scope.is_some()
        && has_scope(scope, "read:body_measurement")
        && end >= now - CalendarDuration::minutes(2)
    {
        let data = body(
            client
                .get(format!("{origin}/user/measurement/body"))
                .bearer_auth(token)
                .send()
                .await?,
        )
        .await?;
        ensure!(data.is_object(), "invalid WHOOP body measurement");
        let at = start.max(end - CalendarDuration::milliseconds(1));
        rows.push(record(
            "metric", "WHOOP", "body_measurement:current", at, at, end,
            json!({"metric":"body_measurement","time_precision":"observed_at","provider_data":data}),
        ));
        if !grants.contains(&"metric") {
            grants.push("metric");
        }
    }
    for row in &mut rows {
        row["modified_at"] = json!(now);
    }
    Ok((rows, grants))
}

#[cfg(test)]
fn whoop_cycle_metrics(row: &Value, window: &Window) -> Option<Vec<Value>> {
    if row["payload"]["metric"] != "cycle" {
        return None;
    }
    let cycle = &row["payload"]["provider_data"];
    let id = cycle["id"].as_u64()?;
    let start = date(&cycle["start"]).ok()?;
    let end = date(&cycle["end"]).ok()?;
    if start >= end {
        return None;
    }
    let offset = match cycle["timezone_offset"].as_str()? {
        "Z" => chrono::FixedOffset::east_opt(0)?,
        value => value.parse::<chrono::FixedOffset>().ok()?,
    };
    let midpoint = start + (end - start) / 2;
    let day = midpoint.with_timezone(&offset).date_naive();
    let revision = window.end;
    let mut output = Vec::new();
    if let Some(count) = cycle["step_count"]
        .as_u64()
        .filter(|count| *count <= 1_000_000)
    {
        output.push(record(
            "metric", "WHOOP", &format!("cycle_steps:{id}"), midpoint, midpoint, revision,
            json!({"metric":"cycle_steps","provider_data":{
                "cycle_id":id,"value":count,"unit":"steps","local_day":day,"timezone":offset.to_string()
            }}),
        ));
    }
    if let Some(kj) = cycle["score"]["kilojoule"]
        .as_f64()
        .filter(|kj| cycle["score_state"] == "SCORED" && *kj > 0.0)
    {
        output.push(record(
            "metric",
            "WHOOP",
            &format!("total_energy:{id}"),
            midpoint,
            midpoint,
            revision,
            json!({"metric":"total_energy","provider_data":{
                "cycle_id":id,"value":kj / 4.184,"unit":"kcal","kilojoule":kj,
                "local_day":day,"timezone":offset.to_string()
            }}),
        ));
    }
    Some(output)
}

#[cfg(test)]
fn whoop_point(item: &Value, kind: &str, window: &Window) -> Result<Option<Value>> {
    let Some(end) = item.get("end").filter(|value| !value.is_null()) else {
        return Ok(None);
    };
    let start = date(&item["start"])?;
    let end = date(end)?;
    if start >= end || !window.contains(start, end) {
        return Ok(None);
    }
    let payload = if kind == "sleep" {
        json!({"stages":[],"provider_stage_summary":item["score"]["stage_summary"],"nap":item["nap"],
            "provider_data":item})
    } else {
        json!({"exercise_type":required_text(&item["sport_name"])?,"distance_meters":item["score"]["distance_meter"],
        "provider_heart_rate":{"average_bpm":item["score"]["average_heart_rate"],"max_bpm":item["score"]["max_heart_rate"]},
        "duration_seconds":(end-start).num_seconds(),"provider_data":item})
    };
    Ok(Some(record(
        kind,
        "WHOOP",
        required_text(&item["id"])?,
        start,
        end,
        date(&item["updated_at"])?,
        payload,
    )))
}

#[cfg(test)]
async fn cursor_records(
    request: reqwest::RequestBuilder,
    item_key: &str,
    response_key: &str,
    query_key: &str,
    project: impl FnMut(&Value) -> Result<Option<Value>>,
) -> Result<Vec<Value>> {
    cursor_records_from(request, item_key, response_key, query_key, None, project).await
}

#[cfg(test)]
async fn cursor_records_from(
    request: reqwest::RequestBuilder,
    item_key: &str,
    response_key: &str,
    query_key: &str,
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
                let mut page = request.try_clone().context("request cannot be paginated")?;
                if let Some(cursor) = &cursor {
                    page = page.query(&[(query_key, cursor)]);
                }
                page.send().await?
            }
        };
        let response = body(response).await?;
        for item in records(&response[item_key])? {
            if let Some(row) = project(item)? {
                rows.push(row);
            }
        }
        ensure!(rows.len() <= 4000, "snapshot record limit exceeded");
        let Some(next) = response[response_key]
            .as_str()
            .filter(|next| !next.is_empty())
        else {
            return Ok(rows);
        };
        ensure!(
            seen.len() < 32 && seen.insert(next.to_owned()),
            "incomplete provider pagination"
        );
        cursor = Some(next.to_owned());
    }
}

#[cfg(test)]
pub(super) async fn polar(
    client: &Client,
    token: &str,
    start: DateTime<Utc>,
    end: DateTime<Utc>,
) -> Result<Vec<Value>> {
    let window = Window { start, end };
    let response = body(
        client
            .get("https://www.polaraccesslink.com/v3/exercises")
            .bearer_auth(token)
            .query(&[("zones", true)])
            .send()
            .await?,
    )
    .await?;
    let mut rows: Vec<Value> = records(&response)?
        .iter()
        .map(|item| polar_point(item, &window))
        .filter_map(Result::transpose)
        .collect::<Result<_>>()?;
    let mut polar_nights = std::collections::HashMap::new();
    for (name, field) in [("sleep", "nights"), ("nightly-recharge", "recharges")] {
        let response = client
            .get(format!("https://www.polaraccesslink.com/v3/users/{name}"))
            .bearer_auth(token)
            .send()
            .await?;
        if matches!(
            response.status(),
            reqwest::StatusCode::NOT_FOUND | reqwest::StatusCode::NO_CONTENT
        ) {
            continue;
        }
        for item in records(&body(response).await?[field])? {
            if name == "sleep" {
                let night = super::polar_series::Night::parse(item)?;
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
                night.heart_rate(&mut rows, day, &item["heart_rate_samples"], &window);
            } else {
                let at = calendar_anchor(&item["date"])?;
                rows.extend(metric(item, name, "Polar", at, at, &window)?);
                if let Some(night) = item["date"].as_str().and_then(|day| polar_nights.get(day)) {
                    night.recharge(&mut rows, item, &window);
                }
            }
        }
    }
    Ok(rows)
}

fn calendar_anchor(input: &Value) -> Result<DateTime<Utc>> {
    let day: chrono::NaiveDate = required_text(input)?.parse()?;
    Ok(day.and_hms_opt(0, 0, 0).unwrap().and_utc())
}

fn polar_point(item: &Value, window: &Window) -> Result<Option<Value>> {
    polar_point_in(item, Some(window))
}

#[cfg(test)]
pub(crate) fn polar_detail(item: &Value, requested_id: &str) -> Result<crate::wearables::Record> {
    ensure!(
        item["id"].as_str() == Some(requested_id),
        "Polar exercise ID mismatch"
    );
    let row = polar_point_in(item, None)?.context("Polar exercise detail unavailable")?;
    Ok(serde_json::from_value(row)?)
}

fn polar_point_in(item: &Value, window: Option<&Window>) -> Result<Option<Value>> {
    let local: chrono::NaiveDateTime = required_text(&item["start_time"])?.parse()?;
    let offset_minutes = item["start_time_utc_offset"]
        .as_i64()
        .context("missing Polar timezone offset")?;
    ensure!(
        (-1440..=1440).contains(&offset_minutes),
        "invalid Polar offset"
    );
    let start = local.and_utc() - CalendarDuration::minutes(offset_minutes);
    let seconds = polar_duration(&item["duration"])
        .filter(|seconds| *seconds >= 0.001)
        .context("invalid Polar duration")?;
    let end = start + CalendarDuration::milliseconds((seconds * 1000.0).round() as i64);
    if window.is_some_and(|window| !window.contains(start, end)) {
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
    "provider_data":{
        "has_route":item["has_route"],"detailed_sport_info":item["detailed_sport_info"],
        "training_load_pro":item["training_load_pro"]
    }});
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
        let normalized: std::collections::BTreeMap<_,_> = zones.iter()
            .filter_map(|zone| {
                let index = zone["index"].as_u64().filter(|index| *index <= 16)?;
                let duration = polar_duration(&zone["in-zone"]).filter(|duration| *duration <= seconds)?;
                let lower = zone["lower-limit"].as_u64().filter(|limit| *limit <= 350);
                let upper = zone["upper-limit"].as_u64().filter(|limit| (1..=350).contains(limit));
                if lower.zip(upper).is_some_and(|(low, high)| low > high) { return None; }
                Some((index,json!({"index":index,"duration_seconds":duration,"lower_bpm":lower,"upper_bpm":upper})))
            }).collect();
        payload["heart_rate_zones"] = json!(normalized.into_values().collect::<Vec<_>>());
    }
    Ok(payload)
}

pub(crate) fn polar_exercise_heart_rate(payload: &mut Value) {
    for (source, key) in [
        ("average", "average_heart_rate_bpm"),
        ("maximum", "max_heart_rate_bpm"),
    ] {
        if let Some(value) = payload["provider_heart_rate"][source]
            .as_f64()
            .filter(|value| (1.0..=350.0).contains(value))
        {
            payload[key] = json!(value);
        }
    }
}

#[cfg(test)]
pub(super) async fn intervals(
    client: &Client,
    token: &str,
    scope: Option<&str>,
    start: DateTime<Utc>,
    end: DateTime<Utc>,
) -> Result<(Vec<Value>, Vec<&'static str>)> {
    intervals_at(client, token, scope, start, end, "https://intervals.icu").await
}

pub(crate) fn intervals_scope(scope: &str, resource: &str) -> bool {
    scope.split(',').map(str::trim).any(|entry| {
        entry
            .split_once(':')
            .is_some_and(|(name, access)| name == resource && matches!(access, "READ" | "WRITE"))
    })
}

#[cfg(test)]
async fn intervals_at(
    client: &Client,
    token: &str,
    scope: Option<&str>,
    start: DateTime<Utc>,
    end: DateTime<Utc>,
    origin: &str,
) -> Result<(Vec<Value>, Vec<&'static str>)> {
    let window = Window { start, end };
    let observed_at = Utc::now();
    let oldest = (start - CalendarDuration::days(1)).date_naive().to_string();
    let newest = (end + CalendarDuration::days(1)).date_naive().to_string();
    let fetch = |path: &str| {
        let request = client
            .get(format!("{origin}/api/v1/athlete/0/{path}"))
            .query(&[("oldest", &oldest), ("newest", &newest)]);
        let request = if scope.is_some() {
            request.bearer_auth(token)
        } else {
            request.basic_auth("API_KEY", Some(token))
        };
        async move {
            let response = request.send().await?;
            if response.status() == reqwest::StatusCode::NO_CONTENT {
                return Ok(json!([]));
            }
            body(response).await
        }
    };
    let mut rows = Vec::new();
    let mut grants = Vec::new();
    if scope.is_none_or(|scope| intervals_scope(scope, "ACTIVITY")) {
        let activities = fetch("activities").await?;
        let items = records(&activities)?;
        ensure!(items.len() < 2000, "incomplete Intervals.icu history");
        for item in items {
            if let Some(mut row) = intervals_point(item, &window)? {
                row["modified_at"] = json!(observed_at);
                rows.push(row);
            }
        }
        grants.push("workout");
    }
    if scope.is_none_or(|scope| intervals_scope(scope, "WELLNESS")) {
        let wellness = fetch("wellness").await?;
        for item in records(&wellness)? {
            if let Some(row) = intervals_wellness(item, &window, observed_at)? {
                rows.push(row);
            }
        }
        grants.extend(["steps", "metric"]);
    }
    Ok((rows, grants))
}

#[cfg(test)]
fn intervals_wellness(
    item: &Value,
    window: &Window,
    observed_at: DateTime<Utc>,
) -> Result<Option<Value>> {
    let day: chrono::NaiveDate = required_text(&item["id"])?.parse()?;
    let at = day
        .and_hms_opt(0, 0, 0)
        .context("invalid wellness date")?
        .and_utc();
    if !window.contains(at, at) {
        return Ok(None);
    }
    let mut values = serde_json::Map::new();
    for name in [
        "ctl",
        "atl",
        "rampRate",
        "ctlLoad",
        "atlLoad",
        "weight",
        "restingHR",
        "hrv",
        "hrvSDNN",
        "kcalConsumed",
        "sleepSecs",
        "sleepScore",
        "sleepQuality",
        "avgSleepingHR",
        "soreness",
        "fatigue",
        "stress",
        "mood",
        "motivation",
        "injury",
        "spO2",
        "systolic",
        "diastolic",
        "hydration",
        "hydrationVolume",
        "readiness",
        "baevskySI",
        "bloodGlucose",
        "lactate",
        "bodyFat",
        "abdomen",
        "vo2max",
        "steps",
        "floorsClimbed",
        "respiration",
        "carbohydrates",
        "protein",
        "fatTotal",
    ] {
        if item[name].is_number() {
            values.insert(name.to_owned(), item[name].clone());
        }
    }
    for name in ["menstrualPhase", "menstrualPhasePredicted"] {
        if let Some(value) = item[name].as_str().filter(|value| value.len() <= 64) {
            values.insert(name.to_owned(), json!(value));
        }
    }
    values.insert("date".into(), json!(day));
    let revision = date(&item["updated"])
        .or_else(|_| {
            Ok::<_, anyhow::Error>(
                DateTime::parse_from_str(
                    required_text(&item["updated"])?,
                    "%Y-%m-%dT%H:%M:%S%.f%z",
                )?
                .with_timezone(&Utc),
            )
        })
        .unwrap_or(observed_at);
    Ok(Some(record(
        "metric",
        "Intervals.icu",
        &format!("wellness:{day}"),
        at,
        at,
        revision,
        json!({"metric":"intervals_wellness","provider_data":values,"time_precision":"provider_calendar_date"}),
    )))
}

#[cfg(test)]
fn intervals_point(item: &Value, window: &Window) -> Result<Option<Value>> {
    let start = date(&item["start_date"])?;
    let end = end_at(
        start,
        item["elapsed_time"]
            .as_i64()
            .context("missing Intervals.icu duration")?,
    )?;
    Ok(
        (window.contains(start, end) && start < end).then_some(record(
            "workout",
            "Intervals.icu",
            required_text(&item["id"])?,
            start,
            end,
            window.end,
            json!({"exercise_type":required_text(&item["type"])?, "title":item["name"],
            "distance_meters":item["distance"],
            "provider_data":{"file_type":item["file_type"],"source":item["source"]}}),
        )),
    )
}

#[cfg(test)]
#[tokio::test]
#[ignore = "requires isolated PostgreSQL"]
async fn intervals_reads_only_granted_scopes_and_keeps_the_provider_day() -> Result<()> {
    use axum::{extract::Query, http::HeaderMap, routing::get, Json, Router};
    use std::collections::HashMap;

    let router = Router::new()
        .route(
            "/api/v1/athlete/0/activities",
            get(|| async { axum::http::StatusCode::FORBIDDEN }),
        )
        .route(
            "/api/v1/athlete/0/wellness",
            get(
                |headers: HeaderMap, Query(query): Query<HashMap<String, String>>| async move {
                    assert_eq!(headers["authorization"], "Bearer fixture-wellness");
                    assert_eq!(query["oldest"], "2026-04-23");
                    assert_eq!(query["newest"], "2026-04-26");
                    Json(json!([
                        {"id":"2026-04-23","steps":666},
                        {"id":"2026-04-24","updated":"2026-04-26T02:14:22.925+0000",
                         "steps":3800,"restingHR":51,"hrv":40.5,"sleepSecs":26800,"weight":74.2},
                        {"id":"2026-04-25","steps":777}
                    ]))
                },
            ),
        );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let origin = format!("http://{}", listener.local_addr()?);
    let server = tokio::spawn(async move { axum::serve(listener, router).await });
    let start = "2026-04-24T00:00:00Z".parse::<DateTime<Utc>>()?;
    let end = start + CalendarDuration::days(1);
    let (rows, grants) = intervals_at(
        &http_client()?,
        "fixture-wellness",
        Some("WELLNESS:READ"),
        start,
        end,
        &origin,
    )
    .await?;
    assert_eq!(grants, ["steps", "metric"]);
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["modified_at"], "2026-04-26T02:14:22.925Z");
    assert_eq!(rows[0]["payload"]["provider_data"]["sleepSecs"], 26800);
    let pool = crate::store::connect(&std::env::var("AVATAR_HSM_TEST_DATABASE_URL")?).await?;
    let subject = "intervals-wellness-fixture";
    pool.get()
        .await?
        .execute(
            "INSERT INTO avatar_hsm.subject VALUES ($1,'America/Los_Angeles')",
            &[&subject],
        )
        .await?;
    let lease = wearables::issue_lease_for(&pool, subject, "intervals_icu").await?;
    let snapshot = serde_json::from_value(json!({"lease":lease["lease"],"start":start,"end":end,
        "grants":grants,"records":rows}))?;
    wearables::store_snapshot(&pool, subject, "intervals_icu", snapshot, false).await?;
    let daily = wearables::daily_for(
        &pool,
        subject,
        wearables::DailyQuery {
            start_day: start.date_naive(),
            end_day: start.date_naive(),
            kind: Some("steps".into()),
            source: None,
            provider: Some("intervals_icu".into()),
        },
    )
    .await?;
    assert_eq!(daily["state"], "ready");
    assert_eq!(daily["records"][0]["stats"]["total"], 3800);
    assert_eq!(daily["records"][0]["day"], "2026-04-24");
    let samples = wearables::records_for(
        &pool,
        subject,
        wearables::RecordQuery {
            start,
            end,
            offset: 0,
            kind: "metric".into(),
            source: None,
            provider: Some("intervals_icu".into()),
            metric: Some("intervals_wellness".into()),
        },
        false,
    )
    .await?;
    assert_eq!(
        samples["records"][0]["payload"]["provider_data"]["hrv"],
        40.5
    );
    assert_eq!(
        samples["records"][0]["payload"]["time_precision"],
        "provider_calendar_date"
    );
    server.abort();
    Ok(())
}

#[cfg(test)]
pub(super) async fn suunto(
    client: &Client,
    token: &str,
    subscription: &str,
    start: DateTime<Utc>,
    end: DateTime<Utc>,
) -> Result<Vec<Value>> {
    suunto_at(
        client,
        token,
        subscription,
        &Window { start, end },
        "https://cloudapi.suunto.com/v3/workouts/",
    )
    .await
}

#[cfg(test)]
async fn suunto_at(
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
                .await?,
        )
        .await?;
        let items = records(&response["payload"])?;
        let before = seen.len();
        for item in items {
            if seen.insert(required_text(&item["workoutKey"])?.to_owned()) {
                rows.extend(suunto_point(item, window)?);
            }
        }
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
    let mut payload = json!({"exercise_type":format!("suunto:{}", integer(&item["activityId"])?),
        "distance_meters":item["totalDistance"],"provider_timezone_offset_minutes":item["timeOffsetInMinutes"],
        "duration_seconds":(ended-started).num_seconds(),"provider_data":item});
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

pub(crate) fn suunto_workout_details(payload: &mut Value) {
    let raw = &payload["provider_data"];
    let mut metrics = serde_json::Map::new();
    for (field, target) in [
        ("stepCount", "workout_steps"),
        ("energyConsumption", "calories_kcal"),
        ("avgSpeed", "average_speed_meters_per_second"),
        ("maxSpeed", "max_speed_meters_per_second"),
        ("avgPower", "average_power_watts"),
        ("maxPower", "max_power_watts"),
        ("totalAscent", "elevation_gain_meters"),
        ("totalDescent", "elevation_loss_meters"),
        ("maxAltitude", "max_altitude_meters"),
        ("minAltitude", "min_altitude_meters"),
        ("totalTime", "active_duration_seconds"),
    ] {
        if let Some(value) = raw[field]
            .as_f64()
            .filter(|value| field.ends_with("Altitude") || *value >= 0.0)
        {
            metrics.insert(target.into(), json!(value));
        }
    }
    for (fields, target) in [
        (&["avg", "workoutAvgHR"][..], "average_heart_rate_bpm"),
        (&["hrmax", "workoutMaxHR"][..], "max_heart_rate_bpm"),
        (&["min"][..], "min_heart_rate_bpm"),
    ] {
        if let Some(value) = fields.iter().find_map(|field| {
            raw["hrdata"][field]
                .as_f64()
                .filter(|bpm| (1.0..=350.0).contains(bpm))
        }) {
            metrics.insert(target.into(), json!(value));
        }
    }
    let extension_gear = raw["extensions"]
        .as_array()
        .into_iter()
        .flatten()
        .find(|extension| extension["type"] == "SummaryExtension")
        .map(|extension| &extension["gear"]);
    if let Some(model) = std::iter::once(&raw["gear"])
        .chain(extension_gear)
        .find_map(|gear| {
            ["displayName", "name"]
                .into_iter()
                .find_map(|key| gear[key].as_str().filter(|name| !name.trim().is_empty()))
        })
    {
        metrics.insert("device_model".into(), json!(model));
    }
    if let Some(fields) = payload.as_object_mut() {
        fields.extend(metrics);
    }
}

#[cfg(test)]
pub(crate) fn suunto_detail(response: &Value, key: &str) -> Result<wearables::Record> {
    let item = &response["payload"];
    ensure!(item["workoutKey"] == key, "unexpected Suunto workout key");
    let start = DateTime::from_timestamp_millis(
        item["startTime"]
            .as_i64()
            .context("missing Suunto start time")?,
    )
    .context("invalid Suunto start time")?;
    let row = suunto_point(
        item,
        &Window {
            start,
            end: Utc::now(),
        },
    )?
    .context("invalid Suunto workout interval")?;
    Ok(serde_json::from_value(row)?)
}

pub(super) async fn coros(
    client: &Client,
    token: &str,
    open_id: &str,
    start: DateTime<Utc>,
    end: DateTime<Utc>,
) -> Result<(Vec<Value>, crate::wearable_fit::CorosFitSources)> {
    coros_at(client, token, open_id, start, end, "https://open.coros.com").await
}

pub(crate) async fn coros_at(
    client: &Client,
    token: &str,
    open_id: &str,
    start: DateTime<Utc>,
    end: DateTime<Utc>,
    base_url: &str,
) -> Result<(Vec<Value>, crate::wearable_fit::CorosFitSources)> {
    let window = Window { start, end };
    let mut sources = crate::wearable_fit::CorosFitSources::new();
    // A narrower re-read can correct data collected from a wider historical query.
    let observed_at = Utc::now();
    let today = observed_at.date_naive();
    let earliest = today
        .checked_sub_months(chrono::Months::new(3))
        .unwrap_or(today);
    // Overlap local dates without sending a range before the vendor's retention limit.
    let from = (start - CalendarDuration::days(1))
        .date_naive()
        .max(earliest);
    let to = (end + CalendarDuration::days(1)).date_naive();
    if to < from {
        return Ok((Vec::new(), sources));
    }
    let from = from.format("%Y%m%d").to_string();
    let to = to.format("%Y%m%d").to_string();
    let request = |path: &str| {
        client.get(format!("{base_url}/{path}")).query(&[
            ("token", token),
            ("openId", open_id),
            ("startDate", &from),
            ("endDate", &to),
        ])
    };
    let fetch = |path| {
        let request = request(path);
        async move {
            let response = request.send().await.map_err(reqwest::Error::without_url)?;
            body(response)
                .await
                .map_err(|error| match error.downcast::<reqwest::Error>() {
                    Ok(error) => error.without_url().into(),
                    Err(error) => error,
                })
        }
    };
    let response = fetch("v2/coros/sport/list").await?;
    ensure!(
        response["result"] == "0000",
        "COROS request did not succeed"
    );
    let mut rows = Vec::new();
    let allowed = std::env::var("AVATAR_COROS_FIT_HOSTS").unwrap_or_default();
    for item in records(&response["data"])? {
        if let Some(row) = coros_point(item, &window)? {
            if let (Some(activity), Ok(links)) = (
                row["source_id"].as_str(),
                crate::wearable_fit::coros_links(item, &allowed),
            ) {
                sources.insert(activity.to_owned(), links);
            }
            rows.push(row);
        }
    }
    let daily = fetch("coros/daily/query").await?;
    ensure!(
        daily["result"] == "0000",
        "COROS daily request did not succeed"
    );
    for item in records(&daily["data"]["dailyList"])? {
        append_coros_day(&mut rows, item, &window)?;
    }
    for row in &mut rows {
        row["modified_at"] = json!(observed_at);
    }
    Ok((rows, sources))
}

fn append_coros_day(rows: &mut Vec<Value>, item: &Value, window: &Window) -> Result<()> {
    let day =
        chrono::NaiveDate::parse_from_str(&integer(&item["happenDay"])?.to_string(), "%Y%m%d")?;
    let at = day.and_hms_opt(0, 0, 0).unwrap().and_utc();
    rows.extend(metric(item, "daily_summary", "COROS", at, at, window)?);
    for (field, name) in [
        ("rhr", "resting_heart_rate"),
        ("sleepAvgHr", "sleep_average_heart_rate"),
    ] {
        if let Some(value) = item[field]
            .as_u64()
            .filter(|value| (1..=300).contains(value))
        {
            let measurement = json!({"date":day,"value":value,"unit":"bpm"});
            rows.extend(metric(&measurement, name, "COROS", at, at, window)?);
        }
    }
    rows.extend(coros_sleep(&item["mainSleep"], "main", window));
    if let Some(naps) = item["napList"].as_array() {
        rows.extend(
            naps.iter()
                .filter_map(|nap| coros_sleep(nap, "nap", window)),
        );
    }
    if let Some(samples) = item["hrvList"].as_array() {
        for sample in samples {
            if let Ok(at) = epoch_seconds(&sample["timestamp"]) {
                rows.extend(metric(sample, "hrv_sample", "COROS", at, at, window)?);
            }
        }
    }
    Ok(())
}

fn coros_sleep(item: &Value, kind: &str, window: &Window) -> Option<Value> {
    let source_stages = item["stages"].as_array()?;
    if !(1..=500).contains(&source_stages.len()) {
        return None;
    }
    let started = epoch_seconds(&source_stages.first()?["startTimestamp"]).ok()?;
    let mut ended = started;
    let mut stages = Vec::with_capacity(source_stages.len());
    for source in source_stages {
        let start = epoch_seconds(&source["startTimestamp"]).ok()?;
        let duration = CalendarDuration::try_seconds(source["duration"].as_i64()?)?;
        let end = start.checked_add_signed(duration)?;
        if start < ended || end <= start {
            return None;
        }
        let provider_stage = source["type"].as_i64()?;
        let stage = match provider_stage {
            0 => 1,
            1 => 4,
            2 => 5,
            3 => 6,
            _ => 0,
        };
        stages.push(json!({"start":start,"end":end,"stage":stage,"provider_stage":provider_stage}));
        ended = end;
    }
    // The stages have Unix timestamps. The summary's local bounds carry no offset.
    if !window.contains(started, ended) {
        return None;
    }
    let mut summary = item.as_object()?.clone();
    summary.remove("stages");
    Some(record(
        "sleep",
        "COROS",
        &format!("{kind}:{}", started.timestamp()),
        started,
        ended,
        window.end,
        json!({"stages":stages,"interval_basis":"recorded_stages","sleep_kind":kind,
            "provider_summary":summary}),
    ))
}

fn coros_point(item: &Value, window: &Window) -> Result<Option<Value>> {
    let started = epoch_seconds(&item["startTime"])?;
    let ended = epoch_seconds(&item["endTime"])?;
    if started >= ended || !window.contains(started, ended) {
        return Ok(None);
    }
    let activity = crate::wearable_webhooks::account_id(&item["labelId"])
        .context("missing COROS workout ID")?;
    let mut provider_data = coros_workout_details(item);
    if let Some(legs) = item["triathlonItemList"].as_array() {
        provider_data["multisport_legs"] =
            Value::Array(legs.iter().map(coros_workout_details).collect());
    }
    Ok(Some(record(
        "workout",
        item["deviceName"].as_str().unwrap_or("COROS"),
        &activity,
        started,
        ended,
        window.end,
        json!({"exercise_type":format!("coros:{}:{}", integer(&item["mode"])?, integer(&item["subMode"])?),
            "distance_meters":item["distance"],"workout_steps":item["step"],"provider_data":provider_data}),
    )))
}

fn coros_workout_details(item: &Value) -> Value {
    // Retain the reported numeric fields. Download URLs belong to the FIT worker.
    let fields = [
        ("mode", "mode"),
        ("subMode", "sub_mode"),
        ("duration", "duration_seconds"),
        ("distance", "distance_meters"),
        ("step", "steps"),
        ("avgSpeed", "average_pace_seconds_per_km"),
        ("avgFrequency", "average_cadence_steps_per_minute"),
        ("calorie", "provider_calorie"),
        ("startTimezone", "start_offset_quarters"),
        ("endTimezone", "end_offset_quarters"),
    ];
    Value::Object(
        fields
            .into_iter()
            .filter_map(|(field, name)| {
                item.get(field)
                    .filter(|value| value.is_number())
                    .map(|value| (name.to_owned(), value.clone()))
            })
            .collect(),
    )
}

fn epoch_seconds(value: &Value) -> Result<DateTime<Utc>> {
    let seconds = value.as_f64().context("missing provider timestamp")?;
    ensure!(
        seconds.is_finite() && seconds > 0.0,
        "invalid provider timestamp"
    );
    DateTime::from_timestamp_millis((seconds * 1000.0).round() as i64)
        .context("invalid provider timestamp")
}

#[cfg(test)]
pub(super) async fn daily_sleep(
    client: &Client,
    token: &str,
    start: DateTime<Utc>,
    end: DateTime<Utc>,
) -> Result<Vec<Value>> {
    daily_sleep_at(
        client,
        token,
        Window { start, end },
        "https://api.sensorbio.com",
    )
    .await
}

#[cfg(test)]
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
            let details = super::vendor_metrics::optional(
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

#[cfg(test)]
fn append_ultrahuman_metric(
    rows: &mut Vec<Value>,
    series: &mut std::collections::BTreeMap<(&'static str, DateTime<Utc>), Value>,
    item: &Value,
    day: chrono::NaiveDate,
    window: &Window,
) -> Result<()> {
    let kind = required_text(&item["type"])?;
    let data = &item["object"];
    if kind == "Sleep" {
        rows.extend(ring_sleep_point(data, true, window)?);
        return Ok(());
    }
    if !data.is_object() {
        return Ok(());
    }
    let anchor = day.and_hms_opt(0, 0, 0).unwrap().and_utc();
    rows.extend(metric(data, kind, "Ultrahuman", anchor, anchor, window)?);
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

#[cfg(test)]
fn sensorbio_stages(
    intervals: &[Value],
    started: DateTime<Utc>,
    ended: DateTime<Utc>,
) -> Vec<Value> {
    let mut candidates = intervals
        .iter()
        .filter_map(|interval| {
            let start = super::vendor_metrics::sensorbio_instant(&interval["start_time"])?;
            let end = super::vendor_metrics::sensorbio_instant(&interval["end_time"])?.min(ended);
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

#[cfg(test)]
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

#[cfg(test)]
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
        if let Some(at) = super::vendor_metrics::sensorbio_instant(&item[field])
            .filter(|at| (start..=end).contains(at))
        {
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

#[cfg(test)]
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

#[cfg(test)]
fn ultrahuman_sleep_payload(item: &Value, started: DateTime<Utc>, ended: DateTime<Utc>) -> Value {
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
                    .is_some_and(|v| (0.0..=100.0).contains(&v)) =>
            {
                summary["efficiency_percent"] = metric["value"].clone();
            }
            _ => {}
        }
    }
    let spo2 = item["spo2"]["value"]
        .as_f64()
        .filter(|value| *value > 0.0 && *value <= 100.0);
    let mut payload = json!({
        "stages":stages,"provider_stage_summary":summary,"provider_data":item,
    });
    if let Some(spo2) = spo2 {
        payload["sleep_metrics"] = json!({"average_spo2_percent":spo2});
    }
    if truncated {
        payload["stages_truncated"] = json!(true);
    }
    payload
}

#[cfg(test)]
fn ring_sleep_point(item: &Value, ultra: bool, window: &Window) -> Result<Option<Value>> {
    if ultra && (item["bedtime_start"].is_null() || item["bedtime_end"].is_null()) {
        return Ok(None);
    }
    let (source, start_key, end_key) = if ultra {
        ("Ultrahuman", "bedtime_start", "bedtime_end")
    } else {
        ("Sensor Bio", "start_timestamp", "end_timestamp")
    };
    let session_time = |key: &str| {
        if ultra {
            epoch_seconds(&item[key])
        } else {
            super::vendor_metrics::sensorbio_instant(&item[key])
                .context("invalid Sensor Bio sleep time")
        }
    };
    let started = session_time(start_key)?;
    let ended = session_time(end_key)?;
    if started >= ended || !window.contains(started, ended) {
        return Ok(None);
    }
    let id = item["id"]
        .as_str()
        .map(str::to_owned)
        .or_else(|| item["id"].as_i64().map(|id| id.to_string()))
        .unwrap_or_else(|| format!("{started}/{ended}"));
    let payload = if ultra {
        #[cfg(test)]
        {
            ultrahuman_sleep_payload(item, started, ended)
        }
        #[cfg(not(test))]
        {
            anyhow::bail!("Ultrahuman normalization is owned by Watch Tower ETL")
        }
    } else {
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
        json!({"stages":[],"provider_stage_summary":{
            "total_minutes":item["total_sleep_mins"],"deep_minutes":item["deep_sleep_mins"],
            "light_minutes":item["light_sleep_mins"],"rem_minutes":item["rem_sleep_mins"],"awake_minutes":item["awake_time_mins"]
        },"sleep_timing":timing,"sleep_metrics":metrics,
          "provider_data":{"biometrics":item["biometrics"],"score":item["score"]}})
    };
    Ok(Some(record(
        "sleep", source, &id, started, ended, window.end, payload,
    )))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    #[ignore = "requires isolated PostgreSQL"]
    async fn sensorbio_sleep_daily_details_match_overnight_and_nap_by_exact_bounds() -> Result<()> {
        use axum::{
            extract::{OriginalUri, Query},
            http::HeaderMap,
            routing::get,
            Json, Router,
        };
        use std::{
            collections::HashMap,
            sync::{
                atomic::{AtomicUsize, Ordering},
                Arc,
            },
        };

        let day = "2026-04-25";
        let start: DateTime<Utc> = "2026-04-24T22:00:00Z".parse()?;
        let end: DateTime<Utc> = "2026-04-25T17:00:00Z".parse()?;
        let night_start = start + CalendarDuration::minutes(30);
        let night_end = night_start + CalendarDuration::hours(8);
        let nap_start: DateTime<Utc> = "2026-04-25T14:00:00Z".parse()?;
        let nap_end = nap_start + CalendarDuration::minutes(30);
        let calls = Arc::new(AtomicUsize::new(0));
        let api = Router::new().fallback(get(move |OriginalUri(uri): OriginalUri, headers: HeaderMap,
            Query(query): Query<HashMap<String, String>>| {
            let calls = Arc::clone(&calls);
            async move {
                assert_eq!(headers["authorization"], "Bearer fixture-sensorbio");
                assert!(!query.contains_key("user_id"));
                Json(match uri.path() {
                    "/v1/sleep" if query["date"] != day => json!({"data":[]}),
                    "/v1/sleep" => json!({"data":[
                        {"id":"night","start_timestamp":night_start.timestamp_millis(),"end_timestamp":night_end.timestamp_millis(),
                            "avg_heart_rate":57,"biometrics":{"resting_bpm":51,"resting_hrv":44.5,"hrv":47,"brpm":12.5,"spo2":96}},
                        {"id":"nap","start_timestamp":nap_start.timestamp_millis(),"end_timestamp":nap_end.timestamp_millis(),
                            "sleep_onset":(nap_start+CalendarDuration::minutes(3)).timestamp_millis(),"tz_offset_in_mins":120}
                    ]}),
                    "/v1/sleep/details/day" => {
                        assert_eq!(query["date"], day);
                        let update = calls.fetch_add(1, Ordering::SeqCst) != 0;
                        json!({"date":day,
                            "all_sleeps_time_details":[
                                {"start_ts_millis":nap_start.timestamp_millis(),"end_ts_millis":nap_end.timestamp_millis(),
                                    "slee_wakeup_ts_millis":nap_end.timestamp_millis(),"sleep_tz_offset_in_mins":180},
                                {"start_ts_millis":night_start.timestamp_millis(),"end_ts_millis":night_end.timestamp_millis(),
                                    "sleep_onset_ts_millis":(night_start+CalendarDuration::minutes(12)).timestamp_millis(),
                                    "slee_wakeup_ts_millis":(night_end-CalendarDuration::minutes(2)).timestamp_millis(),
                                    "sleep_tz_offset_in_mins": if update { -360 } else { -420 }},
                                {"start_ts_millis":night_start.timestamp_millis(),"end_ts_millis":nap_end.timestamp_millis(),
                                    "sleep_tz_offset_in_mins":0}
                            ],
                            "main_sleep_detail_metrics":{"total_time_in_bed_in_mins":480,"fall_asleep_latency_in_mins":12,
                                "number_of_awakenings":0,"sleep_score": if update {81} else {75}},
                            "main_sleep_stages_percentages":{"deep":25,"rem":20},
                            "sleep_stages":[
                                {"start_time":night_start,"end_time":night_end,"status":"deep"},
                                {"start_time":nap_start,"end_time":nap_end,"status":"light"}]})
                    }
                    "/v1/scores" => {
                        assert_eq!(query["date"], day);
                        json!({"data":{"sleep":{"value":75}}})
                    }
                    path => panic!("unexpected Sensor Bio path {path}"),
                })
            }
        }));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let origin = format!("http://{}", listener.local_addr()?);
        let server = tokio::spawn(async move { axum::serve(listener, api).await });
        let client = Client::builder().http2_prior_knowledge().build()?;
        let pool = crate::store::connect(&env::var("AVATAR_HSM_TEST_DATABASE_URL")?).await?;
        let subject = "sensorbio-sleep-detail-fixture";
        let token = "sensorbio-sleep-fixture-read-0123456789012345";
        let db = pool.get().await?;
        db.execute(
            "INSERT INTO avatar_hsm.subject VALUES($1,'America/Los_Angeles')",
            &[&subject],
        )
        .await?;
        db.execute("INSERT INTO avatar_hsm.wearable_profile VALUES($1,sha256(convert_to($2,'UTF8')),sha256(convert_to($1||'c','UTF8')))",
            &[&subject,&token]).await?;
        for (snapshot_end, offset, score) in [
            (end, -420, 75),
            (end - CalendarDuration::hours(1), -360, 81),
        ] {
            let rows = daily_sleep_at(
                &client,
                "fixture-sensorbio",
                Window {
                    start,
                    end: snapshot_end,
                },
                &origin,
            )
            .await?;
            assert_eq!(rows.len(), 4);
            let lease = wearables::issue_lease_for(&pool, subject, "sensorbio").await?;
            wearables::store_snapshot(&pool, subject, "sensorbio", serde_json::from_value(json!({
                "lease":lease["lease"],"start":start,"end":snapshot_end,"grants":["sleep","metric"],"records":rows
            }))?, false).await?;
            let query = |kind: &str, metric: Option<&str>| wearables::RecordQuery {
                kind: kind.into(),
                start,
                end: snapshot_end,
                offset: 0,
                source: None,
                provider: Some("sensorbio".into()),
                metric: metric.map(str::to_owned),
            };
            let read = wearables::records_for(&pool, subject, query("sleep", None), true).await?;
            let sessions = read["records"]
                .as_array()
                .context("missing sleep sessions")?;
            assert_eq!(sessions.len(), 2);
            let night = &sessions[0]["payload"];
            let nap = &sessions[1]["payload"];
            assert_eq!(night["sleep_timing"]["offset_minutes"], offset);
            assert_eq!(
                night["sleep_timing"]["sleep_onset_at"],
                json!(night_start + CalendarDuration::minutes(12))
            );
            assert_eq!(night["sleep_metrics"]["resting_hrv_rmssd_ms"], 44.5);
            assert_eq!(night["stages"].as_array().map(Vec::len), Some(1));
            assert_eq!(night["stages"][0]["stage"], 5);
            assert_eq!(nap["sleep_timing"]["offset_minutes"], 180);
            assert_eq!(
                nap["sleep_timing"]["sleep_onset_at"],
                json!(nap_start + CalendarDuration::minutes(3))
            );
            assert_eq!(nap["stages"].as_array().map(Vec::len), Some(1));
            assert_eq!(nap["stages"][0]["stage"], 4);
            let daily = wearables::records_for(
                &pool,
                subject,
                query("metric", Some("main_sleep_details")),
                false,
            )
            .await?;
            let details = &daily["records"][0]["payload"]["provider_data"];
            assert_eq!(details["date"], day);
            assert_eq!(details["metrics"]["sleep_score"], score);
            assert_eq!(details["metrics"]["time_in_bed_seconds"], 28_800);
            assert_eq!(details["metrics"]["number_of_awakenings"], 0);
            assert_eq!(details["stage_percentages"]["deep"].as_f64(), Some(25.0));
        }
        server.abort();
        let app = Arc::new(crate::mcp::App {
            db: pool,
            read_token: String::new(),
            control_token: String::new(),
            rules: crate::acute::rules()?,
        });
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let hsm = tokio::spawn(async move { axum::serve(listener, crate::router(app)).await });
        let reply: Value = Client::new().post(format!("http://{address}/mcp/wearable-read")).bearer_auth(token)
            .json(&json!({"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"wearable_sessions","arguments":{
                "provider":"sensorbio","kind":"sleep","start":start,"end":end
            }}}))
            .send().await?.error_for_status()?.json().await?;
        hsm.abort();
        assert_eq!(
            reply["result"]["structuredContent"]["records"][0]["payload"]["sleep_timing"]
                ["offset_minutes"],
            -360
        );
        Ok(())
    }

    #[test]
    fn sensorbio_sleep_uses_millis_and_keeps_only_the_sessions_own_stages() -> Result<()> {
        let started: DateTime<Utc> = "2026-04-25T14:00:00Z".parse()?;
        let ended = started + CalendarDuration::hours(2);
        let window = Window {
            start: started,
            end: ended,
        };
        let row = ring_sleep_point(
            &json!({"id":"session", "start_timestamp":started.timestamp_millis(),
            "end_timestamp":ended.timestamp_millis(),"score":{"value":83},"biometrics":{"bpm":56}}),
            false,
            &window,
        )?
        .context("expected a Sensor Bio sleep session")?;
        assert_eq!(row["payload"]["provider_data"]["score"]["value"], 83);
        assert_eq!(row["payload"]["provider_data"]["biometrics"]["bpm"], 56);
        let stages = sensorbio_stages(
            &[
                json!({"start_time":ended,"end_time":ended+CalendarDuration::hours(1),"status":"light"}),
                json!({"start_time":started+CalendarDuration::hours(1),"end_time":ended+CalendarDuration::minutes(1),"status":"REM"}),
                json!({"start_time":started,"end_time":started+CalendarDuration::hours(1),"status":"deep"}),
            ],
            started,
            ended,
        );
        assert_eq!(stages.len(), 2);
        assert_eq!(stages[0]["stage"], 5);
        assert_eq!(stages[1]["stage"], 6);
        assert_eq!(stages[1]["end"], json!(ended));
        Ok(())
    }

    #[tokio::test]
    #[ignore = "requires AVATAR_HSM_TEST_DATABASE_URL"]
    async fn suunto_workouts_paginate_with_bounds_and_enqueue_fit() -> Result<()> {
        use axum::{
            extract::Query,
            http::{HeaderMap, StatusCode},
            routing::get,
            Json, Router,
        };
        use std::collections::HashMap;

        let start: DateTime<Utc> = "2026-04-24T00:00:00Z".parse()?;
        let end = start + CalendarDuration::days(1);
        let app = Router::new().route("/", get(move |Query(query): Query<HashMap<String,String>>, headers: HeaderMap| async move {
            if headers.get("authorization").and_then(|value| value.to_str().ok()) != Some("Bearer fixture")
                || headers.get("ocp-apim-subscription-key").and_then(|value| value.to_str().ok()) != Some("fixture-key")
                || query.get("since") != Some(&start.timestamp_millis().to_string())
                || query.get("until") != Some(&end.timestamp_millis().to_string())
                || query.get("limit").map(String::as_str) != Some("100")
                || query.get("filter-by-modification-time").map(String::as_str) != Some("false") {
                return Err(StatusCode::BAD_REQUEST);
            }
            let workout = |id: i32, at: DateTime<Utc>| {
                let mut row = json!({
                    "workoutKey":format!("{id:024x}"),"activityId":1,"startTime":at.timestamp_millis(),
                    "stopTime":(at+CalendarDuration::minutes(10)).timestamp_millis(),"totalDistance":2000.0
                });
                if id == 100 {
                    row["stopTime"] = Value::Null;
                    row["totalTime"] = json!(600.5);
                }
                row
            };
            let data: Vec<_> = match query.get("offset").map(String::as_str) {
                Some("0") => (0..100).map(|id| workout(id, start)).collect(),
                Some("100") => vec![workout(99,start),workout(100,start),workout(101,end)],
                _ => return Err(StatusCode::BAD_REQUEST),
            };
            Ok(Json(json!({"payload":data})))
        }));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let endpoint = format!("http://{}", listener.local_addr()?);
        let server = tokio::spawn(async move { axum::serve(listener, app).await });
        let rows = suunto_at(
            &Client::new(),
            "fixture",
            "fixture-key",
            &Window { start, end },
            &endpoint,
        )
        .await?;
        server.abort();
        assert_eq!(rows.len(), 101);
        let last = format!("{:024x}", 100);
        assert_eq!(rows[100]["source_id"], last);
        assert_eq!(rows[100]["ended_at"], "2026-04-24T00:10:00.500Z");
        assert_eq!(rows[100]["payload"]["end_derived_from_timer"], true);
        let pool = crate::store::connect(&std::env::var("AVATAR_HSM_TEST_DATABASE_URL")?).await?;
        let subject = "suunto-page-fixture";
        pool.get()
            .await?
            .execute(
                "INSERT INTO avatar_hsm.subject VALUES($1,'Asia/Seoul')",
                &[&subject],
            )
            .await?;
        let lease = wearables::issue_lease_for(&pool, subject, "suunto").await?;
        wearables::store_snapshot(
            &pool,
            subject,
            "suunto",
            serde_json::from_value(json!({
                "lease":lease["lease"],"start":start,"end":end,"grants":["workout"],"records":rows
            }))?,
            false,
        )
        .await?;
        let db = pool.get().await?;
        let row = db
            .query_one(
                "SELECT count(*),bool_or(activity_id=$2) FROM watch_tower.wearable_fit_job
            WHERE subject_id=$1 AND provider='suunto'",
                &[&subject, &last],
            )
            .await?;
        assert_eq!(row.get::<_, i64>(0), 101);
        assert!(row.get::<_, bool>(1));
        db.execute(
            "DELETE FROM watch_tower.wearable_fit_job WHERE subject_id=$1 AND provider='suunto'",
            &[&subject],
        )
        .await?;
        Ok(())
    }

    #[tokio::test]
    #[ignore = "requires AVATAR_HSM_TEST_DATABASE_URL"]
    async fn whoop_cycle_steps_energy_and_current_measurements() -> Result<()> {
        use axum::{routing::get, Json, Router};
        let cycle = json!({"id":41,"start":"2026-04-24T03:00:00Z","end":"2026-04-25T03:00:00Z",
            "timezone_offset":"-05:00","step_count":8200,"score_state":"SCORED","score":{"kilojoule":4184.0}});
        let app = Router::new()
            .route(
                "/cycle",
                get(move || {
                    let cycle = cycle.clone();
                    async move { Json(json!({"records":[cycle],"next_token":null})) }
                }),
            )
            .route(
                "/user/measurement/body",
                get(|| async {
                    Json(json!({"height_meter":1.75,"weight_kilogram":70.5,"max_heart_rate":185}))
                }),
            );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let origin = format!("http://{}", listener.local_addr()?);
        let server = tokio::spawn(async move { axum::serve(listener, app).await });
        let start: DateTime<Utc> = "2026-04-23T00:00:00Z".parse()?;
        let end: DateTime<Utc> = "2026-04-26T00:00:00Z".parse()?;
        let (rows, grants) = whoop_at(
            &Client::new(),
            "fixture",
            Window { start, end },
            Some("read:cycles read:body_measurement"),
            &origin,
            end,
        )
        .await?;
        assert_eq!(grants, ["metric", "steps"]);
        assert_eq!(rows.len(), 4);
        assert_eq!(
            rows[1]["payload"]["provider_data"]["local_day"],
            "2026-04-24"
        );
        assert_eq!(rows[2]["payload"]["provider_data"]["value"], 1000.0);
        assert_eq!(rows[3]["payload"]["provider_data"]["weight_kilogram"], 70.5);
        let (historical, _) = whoop_at(
            &Client::new(),
            "fixture",
            Window { start, end },
            Some("read:cycles read:body_measurement"),
            &origin,
            end + CalendarDuration::days(1),
        )
        .await?;
        server.abort();
        assert_eq!(historical.len(), 3);

        let pool = crate::store::connect(&std::env::var("AVATAR_HSM_TEST_DATABASE_URL")?).await?;
        let subject = "whoop-cycle-fixture";
        pool.get()
            .await?
            .execute(
                "INSERT INTO avatar_hsm.subject VALUES($1,'Asia/Seoul')",
                &[&subject],
            )
            .await?;
        let commit = |lease: Value, end, records: Vec<Value>| -> Result<wearables::Snapshot> {
            Ok(serde_json::from_value(json!({
                "lease":lease["lease"],"start":start,"end":end,"grants":grants,"records":records
            }))?)
        };
        let daily = || wearables::DailyQuery {
            start_day: "2026-04-23".parse().unwrap(),
            end_day: "2026-04-25".parse().unwrap(),
            source: None,
            provider: Some("whoop".into()),
            kind: Some("steps".into()),
        };
        let daily_energy = || wearables::DailyQuery {
            kind: Some("activity".into()),
            ..daily()
        };
        let mut corrected = rows[0].clone();
        let lease = wearables::issue_lease_for(&pool, subject, "whoop").await?;
        wearables::store_snapshot(&pool, subject, "whoop", commit(lease, end, rows)?, false)
            .await?;
        let result = wearables::daily_for(&pool, subject, daily()).await?;
        assert_eq!(result["records"].as_array().map(Vec::len), Some(1));
        assert_eq!(result["records"][0]["day"], "2026-04-24");
        assert_eq!(result["records"][0]["stats"]["total"], 8200);
        assert_eq!(result["records"][0]["timezone"], "-05:00");
        assert_eq!(result["records"][0]["calculated_by"], "provider");
        let energy = wearables::daily_for(&pool, subject, daily_energy()).await?;
        assert_eq!(energy["state"], "ready");
        assert_eq!(energy["records"].as_array().map(Vec::len), Some(1));
        assert_eq!(energy["records"][0]["timezone"], "-05:00");
        assert_eq!(
            energy["records"][0]["stats"]["total_energy_kcal"].as_f64(),
            Some(1000.0)
        );
        assert_eq!(energy["records"][0]["stats"]["cycle_count"], 1);
        assert_eq!(energy["records"][0]["stats"]["day_basis"], "cycle_midpoint");
        assert!(energy["records"][0]["stats"]
            .get("active_energy_kcal")
            .is_none());

        let revision = end + CalendarDuration::hours(1);
        let mut utc_cycle = corrected.clone();
        utc_cycle["source_id"] = json!("cycle:42");
        utc_cycle["started_at"] = json!("2026-04-24T00:00:00Z");
        utc_cycle["ended_at"] = json!("2026-04-24T03:00:00Z");
        utc_cycle["modified_at"] = json!(revision);
        utc_cycle["payload"]["provider_data"] = json!({"id":42,"start":utc_cycle["started_at"],
            "end":utc_cycle["ended_at"],"timezone_offset":"Z","score_state":"SCORED",
            "score":{"kilojoule":1046.0}});
        let mut additional = whoop_cycle_metrics(
            &utc_cycle,
            &Window {
                start,
                end: revision,
            },
        )
        .context("WHOOP cycle with documented Z timezone")?;
        assert_eq!(additional.len(), 1);
        assert_eq!(
            additional[0]["payload"]["provider_data"]["timezone"],
            "+00:00"
        );
        additional.push(utc_cycle);
        let lease = wearables::issue_lease_for(&pool, subject, "whoop").await?;
        wearables::store_snapshot(
            &pool,
            subject,
            "whoop",
            commit(lease, revision, additional)?,
            false,
        )
        .await?;
        let combined = wearables::daily_for(&pool, subject, daily_energy()).await?;
        assert_eq!(combined["records"].as_array().map(Vec::len), Some(1));
        assert_eq!(combined["records"][0]["day"], "2026-04-24");
        assert!(combined["records"][0]["timezone"].is_null());
        assert_eq!(
            combined["records"][0]["stats"]["total_energy_kcal"].as_f64(),
            Some(1250.0)
        );
        assert_eq!(combined["records"][0]["stats"]["cycle_count"], 2);

        corrected["modified_at"] = json!(revision);
        corrected["payload"]["provider_data"]["step_count"] = Value::Null;
        corrected["payload"]["provider_data"]["score"] = Value::Null;
        let lease = wearables::issue_lease_for(&pool, subject, "whoop").await?;
        wearables::store_snapshot(
            &pool,
            subject,
            "whoop",
            commit(lease, revision, vec![corrected])?,
            false,
        )
        .await?;
        let result = wearables::daily_for(&pool, subject, daily()).await?;
        assert_eq!(result["state"], "no_records");
        let remaining = wearables::daily_for(&pool, subject, daily_energy()).await?;
        assert_eq!(
            remaining["records"][0]["stats"]["total_energy_kcal"].as_f64(),
            Some(250.0)
        );
        assert_eq!(remaining["records"][0]["stats"]["cycle_count"], 1);
        let energy = wearables::records_for(
            &pool,
            subject,
            wearables::RecordQuery {
                kind: "metric".into(),
                start,
                end,
                offset: 0,
                source: None,
                provider: Some("whoop".into()),
                metric: Some("total_energy".into()),
            },
            false,
        )
        .await?;
        assert_eq!(energy["records"].as_array().map(Vec::len), Some(1));
        assert_eq!(energy["records"][0]["source_id"], "total_energy:42");
        Ok(())
    }

    #[test]
    fn coros_sleep_uses_epoch_stages_and_preserves_provider_codes() -> Result<()> {
        let window = Window {
            start: "2026-11-03T00:00:00Z".parse()?,
            end: "2026-11-04T00:00:00Z".parse()?,
        };
        let at = window.start.timestamp() + 3600;
        let sleep = json!({"startTime":"2026-11-03 09:00:00","sleepDuration":25,"stages":[
            {"startTimestamp":at,"duration":1500,"type":2},
            {"startTimestamp":at+1500,"duration":300,"type":4}
        ]});
        let row = coros_sleep(&sleep, "nap", &window).context("expected COROS nap")?;
        assert_eq!(row["started_at"], "2026-11-03T01:00:00Z");
        assert_eq!(row["ended_at"], "2026-11-03T01:30:00Z");
        assert_eq!(row["payload"]["stages"][0]["stage"], 5);
        assert_eq!(row["payload"]["stages"][1]["stage"], 0);
        assert_eq!(row["payload"]["stages"][1]["provider_stage"], 4);
        assert_eq!(row["payload"]["provider_summary"]["sleepDuration"], 25);
        assert!(
            coros_sleep(&json!({"startTime":"2026-11-03 09:00:00"}), "main", &window).is_none()
        );
        Ok(())
    }

    #[tokio::test]
    #[ignore = "requires AVATAR_HSM_TEST_DATABASE_URL"]
    async fn ultrahuman_series_keep_timestamps_zero_passive_steps_and_heart_rate_daily(
    ) -> Result<()> {
        let pool = crate::store::connect(&std::env::var("AVATAR_HSM_TEST_DATABASE_URL")?).await?;
        let subject = "ultrahuman-series-fixture";
        pool.get()
            .await?
            .execute(
                "INSERT INTO avatar_hsm.subject VALUES ($1,'Asia/Seoul')",
                &[&subject],
            )
            .await?;
        let start: DateTime<Utc> = "2026-04-24T22:00:00Z".parse()?;
        let window = Window {
            start,
            end: start + CalendarDuration::hours(24),
        };
        let day = "2026-04-25".parse()?;
        let at = start.timestamp() + 1200;
        let mut rows = Vec::new();
        let mut series = std::collections::BTreeMap::new();
        // A sample may occur in the first provider day but beyond the UTC midnight anchor.
        for item in [
            json!({"type":"hr","object":{"values":[{"timestamp":at,"value":72},{"timestamp":at-1800,"value":88}]}}),
            json!({"type":"hrv","object":{"values":[{"timestamp":at,"value":31}]}}),
            json!({"type":"temp","object":{"values":[{"timestamp":at,"value":34.8}]}}),
            json!({"type":"steps","object":{"total":9120,"values":[{"timestamp":at,"value":0}]}}),
            json!({"type":"active_minutes","object":{"value":42}}),
            json!({"type":"calories_burned","object":{"value":1985.5}}),
            json!({"type":"weekly_active_minutes","object":{"value":210}}),
            json!({"type":"Sleep","object":null}),
            json!({"type":"Sleep","object":{
                "id":"sleep-fixture","bedtime_start":at,"bedtime_end":at+7200,
                "quick_metrics":[{"type":"time_in_bed","value":7200},
                    {"type":"total_sleep","value":6500},{"type":"sleep_efic","value":92}],
                "sleep_stages":[{"type":"deep_sleep","stage_time":1200},{"type":"rem_sleep","stage_time":900}],
                "sleep_graph":{"data":[
                    {"start":at+600,"end":at+1800,"type":"deep_sleep"},
                    {"start":at,"end":at+600,"type":"awake"},
                    {"start":at,"end":at+600,"type":"awake"},
                    {"start":at+1000,"end":at+1500,"type":"light_sleep"},
                    {"start":at+1900,"type":"rem_sleep"}
                ]},
                "spo2":{"value":97},"summary":[{"title":"Restfulness","score":66}]
            }}),
        ] {
            append_ultrahuman_metric(&mut rows, &mut series, &item, day, &window)?;
        }
        append_ultrahuman_metric(
            &mut rows,
            &mut series,
            &json!({"type":"hr","object":{"values":[{"timestamp":at,"value":75},{"timestamp":at+60,"value":77}]}}),
            chrono::NaiveDate::from_ymd_opt(2026, 4, 24).unwrap(),
            &window,
        )?;
        assert_eq!(series.len(), 5);
        rows.extend(series.into_values());
        let lease = wearables::issue_lease_for(&pool, subject, "ultrahuman").await?;
        wearables::store_snapshot(&pool, subject, "ultrahuman", serde_json::from_value(json!({
            "lease":lease["lease"],"start":window.start,"end":window.end,"grants":["sleep","heart_rate","metric"],"records":rows
        }))?, false).await?;
        let records = wearables::records_for(
            &pool,
            subject,
            wearables::RecordQuery {
                kind: "metric".into(),
                start,
                end: window.end,
                offset: 0,
                source: None,
                provider: Some("ultrahuman".into()),
                metric: Some("step_sample".into()),
            },
            false,
        )
        .await?;
        assert_eq!(
            records["records"][0]["payload"]["provider_data"]["value"],
            0
        );
        assert_eq!(
            records["records"][0]["started_at"],
            "2026-04-24T22:20:00+00:00"
        );
        let daily = wearables::daily_for(
            &pool,
            subject,
            wearables::DailyQuery {
                start_day: day,
                end_day: day,
                provider: Some("ultrahuman".into()),
                source: None,
                kind: Some("heart_rate".into()),
            },
        )
        .await?;
        assert_eq!(daily["records"][0]["stats"]["sample_count"], 2);
        assert_eq!(daily["records"][0]["stats"]["average"].as_f64(), Some(76.0));
        let sleep = wearables::records_for(
            &pool,
            subject,
            wearables::RecordQuery {
                kind: "sleep".into(),
                start,
                end: window.end,
                offset: 0,
                source: None,
                provider: Some("ultrahuman".into()),
                metric: None,
            },
            true,
        )
        .await?;
        let sessions = sleep["records"]
            .as_array()
            .context("missing sleep records")?;
        assert_eq!(sessions.len(), 1);
        let payload = &sessions[0]["payload"];
        assert_eq!(
            payload["provider_stage_summary"]["total_sleep_seconds"],
            6500
        );
        assert_eq!(payload["provider_stage_summary"]["efficiency_percent"], 92);
        assert_eq!(payload["provider_stage_summary"]["deep_seconds"], 1200);
        assert_eq!(
            payload["sleep_metrics"]["average_spo2_percent"].as_f64(),
            Some(97.0)
        );
        assert_eq!(payload["provider_data"]["summary"][0]["score"], 66);
        assert_eq!(
            payload["provider_data"]["sleep_graph"]["data"]
                .as_array()
                .map(Vec::len),
            Some(5)
        );
        assert_eq!(payload["stages"].as_array().map(Vec::len), Some(2));
        assert_eq!(payload["stages"][0]["stage"], 1);
        assert_eq!(payload["stages"][1]["stage"], 5);
        let query = |kind: &str| wearables::DailyQuery {
            start_day: day,
            end_day: day,
            provider: Some("ultrahuman".into()),
            source: None,
            kind: Some(kind.into()),
        };
        let total = wearables::daily_for(&pool, subject, query("steps")).await?;
        assert_eq!(total["state"], "ready");
        assert_eq!(total["records"][0]["day"], "2026-04-25");
        assert_eq!(
            total["records"][0]["stats"],
            json!({"total":9120,"unit":"steps"})
        );
        let activity = wearables::daily_for(&pool, subject, query("activity")).await?;
        assert_eq!(activity["state"], "ready");
        assert_eq!(
            activity["records"][0]["stats"],
            json!({"exercise_seconds":2520,"total_energy_kcal":1985.5})
        );
        let revised_window = Window {
            start,
            end: window.end + CalendarDuration::minutes(5),
        };
        let mut revised = Vec::new();
        let mut unused = std::collections::BTreeMap::new();
        for item in [
            json!({"type":"steps","object":{"total":0,"values":[]}}),
            json!({"type":"calories_burned","object":{"value":null}}),
        ] {
            append_ultrahuman_metric(&mut revised, &mut unused, &item, day, &revised_window)?;
        }
        let lease = wearables::issue_lease_for(&pool, subject, "ultrahuman").await?;
        wearables::store_snapshot(
            &pool,
            subject,
            "ultrahuman",
            serde_json::from_value(json!({
                "lease":lease["lease"],"start":start,"end":revised_window.end,
            "grants":["sleep","heart_rate","metric"],"records":revised
            }))?,
            false,
        )
        .await?;
        let total = wearables::daily_for(&pool, subject, query("steps")).await?;
        assert_eq!(total["records"][0]["stats"]["total"], 0);
        let activity = wearables::daily_for(&pool, subject, query("activity")).await?;
        assert_eq!(
            activity["records"][0]["stats"],
            json!({"exercise_seconds":2520})
        );
        Ok(())
    }

    #[tokio::test]
    #[ignore = "requires AVATAR_HSM_TEST_DATABASE_URL"]
    async fn coros_daily_steps_follow_the_reported_day_and_latest_values() -> Result<()> {
        let pool = crate::store::connect(&std::env::var("AVATAR_HSM_TEST_DATABASE_URL")?).await?;
        let subject = "coros-daily-fixture";
        pool.get()
            .await?
            .execute(
                "INSERT INTO avatar_hsm.subject VALUES ($1,'America/Los_Angeles')",
                &[&subject],
            )
            .await?;
        let start: DateTime<Utc> = "2026-11-03T00:00:00Z".parse()?;
        let commit = |raw: Value, revision| {
            let pool = &pool;
            async move {
                let window = Window {
                    start,
                    end: start + CalendarDuration::hours(revision),
                };
                let mut rows = Vec::new();
                append_coros_day(&mut rows, &raw, &window)?;
                let lease = wearables::issue_lease_for(pool, subject, "coros").await?;
                wearables::store_snapshot(pool, subject, "coros", serde_json::from_value(json!({
                    "lease":lease["lease"],"start":window.start,"end":window.end,"grants":["steps","metric"],"records":rows
                }))?, false).await
            }
        };
        let daily = |kind: &'static str| async {
            wearables::daily_for(
                &pool,
                subject,
                wearables::DailyQuery {
                    start_day: start.date_naive(),
                    end_day: start.date_naive(),
                    kind: Some(kind.into()),
                    source: None,
                    provider: Some("coros".into()),
                },
            )
            .await
        };
        let sample = |metric: &'static str| async {
            wearables::records_for(
                &pool,
                subject,
                wearables::RecordQuery {
                    start,
                    end: start + CalendarDuration::days(1),
                    offset: 0,
                    source: None,
                    kind: "metric".into(),
                    provider: Some("coros".into()),
                    metric: Some(metric.into()),
                },
                false,
            )
            .await
        };
        commit(
            json!({"happenDay":20261103,"step":5200,"rhr":56,"sleepAvgHr":60,
                "calorie":9553,"sleepDuration":512,"ppgHrv":50}),
            25,
        )
        .await?;
        let day = daily("steps").await?;
        assert_eq!(day["records"][0]["day"], "2026-11-03");
        assert_eq!(day["records"][0]["calculated_by"], "provider");
        assert_eq!(day["records"][0]["stats"]["total"], 5200);
        assert_eq!(
            sample("resting_heart_rate").await?["records"][0]["payload"]["provider_data"]["value"],
            56
        );
        let energy = daily("activity").await?;
        assert_eq!(energy["state"], "ready");
        assert_eq!(energy["records"][0]["day"], "2026-11-03");
        assert_eq!(energy["records"][0]["timezone"], Value::Null);
        assert_eq!(energy["records"][0]["stats"]["total_energy_kcal"], 9553);
        let duration = sample("coros_daily_sleep_duration").await?;
        assert_eq!(
            duration["records"][0]["payload"]["provider_data"],
            json!({"date":"2026-11-03","value":30720,"unit":"s"})
        );
        let hrv = sample("coros_overnight_hrv").await?;
        assert_eq!(
            hrv["records"][0]["payload"]["time_precision"],
            "provider_calendar_date"
        );
        assert_eq!(
            hrv["records"][0]["payload"]["provider_data"],
            json!({"date":"2026-11-03","value":50})
        );

        commit(
            json!({"happenDay":20261103,"step":0,"sleepAvgHr":60,"calorie":0,"sleepDuration":0}),
            26,
        )
        .await?;
        commit(
            json!({"happenDay":20261103,"step":6000,"rhr":56,"calorie":4500,"ppgHrv":44}),
            25,
        )
        .await?;
        assert_eq!(daily("steps").await?["records"][0]["stats"]["total"], 0);
        assert_eq!(
            daily("activity").await?["records"][0]["stats"]["total_energy_kcal"],
            0
        );
        assert_eq!(sample("resting_heart_rate").await?["records"], json!([]));
        assert_eq!(sample("coros_overnight_hrv").await?["records"], json!([]));
        assert_eq!(
            sample("coros_daily_sleep_duration").await?["records"][0]["payload"]["provider_data"]
                ["value"],
            0
        );
        Ok(())
    }

    #[test]
    fn imports_partner_workouts_and_sleep() -> Result<()> {
        let window = Window {
            start: "2026-04-24T00:00:00Z".parse()?,
            end: "2026-04-25T00:00:00Z".parse()?,
        };
        let start = window.start.timestamp() + 3600;
        let end = start + 900;
        let suunto = suunto_point(
            &json!({"workoutKey":"suunto-1","activityId":2,
            "startTime":start * 1000,"stopTime":end * 1000,"totalDistance":333.4}),
            &window,
        )?
        .unwrap();
        assert_eq!(suunto["payload"]["distance_meters"], 333.4);
        let coros = coros_point(
            &json!({"labelId":"coros-1","mode":8,"subMode":1,
            "deviceName":"COROS PACE","startTime":start,"endTime":end,"step":50,"avgSpeed":163,
            "avgFrequency":118,"startTimezone":32,"endTimezone":36,
            "fitUrl":"https://example.com/signed.fit?token=fixture",
            "triathlonItemList":[{"mode":9,"subMode":1,"distance":1000,"duration":500,
                "fitUrl":"https://example.com/leg.fit?token=fixture"}]}),
            &window,
        )?
        .unwrap();
        assert_eq!(coros["payload"]["exercise_type"], "coros:8:1");
        assert_eq!(
            coros["payload"]["provider_data"]["average_pace_seconds_per_km"],
            163
        );
        assert_eq!(coros["payload"]["provider_data"]["end_offset_quarters"], 36);
        assert_eq!(
            coros["payload"]["provider_data"]["multisport_legs"][0]["duration_seconds"],
            500
        );
        assert!(!coros.to_string().contains("https://"));
        let sensor = ring_sleep_point(
            &json!({"id":7,"start_timestamp":start,
            "end_timestamp":end,"rem_sleep_mins":7}),
            false,
            &window,
        )?
        .unwrap();
        assert_eq!(
            sensor["payload"]["provider_stage_summary"]["rem_minutes"],
            7
        );
        let ultra = ring_sleep_point(
            &json!({"bedtime_start":start,"bedtime_end":end,
            "sleep_graph":{"data":[{"start":start,"end":end,"type":"deep_sleep"}]}}),
            true,
            &window,
        )?
        .unwrap();
        assert_eq!(ultra["payload"]["stages"][0]["stage"], 5);
        let withings = withings_point(
            &json!({"id":1,"startdate":start,"enddate":end,
            "data":{"deepsleepduration":120}}),
            &window,
        )?
        .unwrap();
        assert_eq!(
            withings["payload"]["provider_stage_summary"]["deepsleepduration"],
            120
        );
        Ok(())
    }

    #[tokio::test]
    async fn oura_requests_authorized_metrics_and_follows_next_token() -> Result<()> {
        use axum::{
            extract::{Path, Query},
            http::{HeaderMap, StatusCode},
            routing::get,
            Json, Router,
        };
        use std::collections::HashMap;

        let app = Router::new().route(
            "/{endpoint}",
            get(
                |Path(endpoint): Path<String>,
                 Query(query): Query<HashMap<String, String>>,
                 headers: HeaderMap| async move {
                    match headers.get("authorization").and_then(|h| h.to_str().ok()) {
                        Some("Bearer oura-no-rest") if endpoint == "rest_mode_period" => {
                            return Err(StatusCode::FORBIDDEN);
                        }
                        Some("Bearer oura-no-rest" | "Bearer oura-fixture") => {}
                        _ => return Err(StatusCode::UNAUTHORIZED),
                    }
                    let data = match endpoint.as_str() {
                        "daily_resilience" | "sleep_time" | "daily_cardiovascular_age" => {
                            json!([{"id":endpoint,"day":"2026-04-24"}])
                        }
                        "vO2_max" if !query.contains_key("next_token") => {
                            return Ok(Json(json!({"data":[],"next_token":"second"})));
                        }
                        "vO2_max" if query["next_token"] == "second" => {
                            json!([{"id":"vo2","day":"2026-04-24","timestamp":"2026-04-24T04:00:00Z","vo2_max":45}])
                        }
                        "session" => {
                            json!([{"id":"session","start_datetime":"2026-04-24T05:00:00Z","end_datetime":"2026-04-24T05:10:00Z"}])
                        }
                        "enhanced_tag" => json!([
                            {"id":"hydration-tag","start_day":"2026-04-24","end_day":"2026-04-26",
                                "start_time":"2026-04-24T23:00:00","end_time":"2026-04-26T08:00:00",
                                "tag_type_code":"custom","custom_name":"Extra water"},
                            {"id":"older-tag","start_day":"2026-04-23","start_time":"2026-04-23T07:00:00"}
                        ]),
                        "rest_mode_period" if !query.contains_key("next_token") => {
                            assert!(query.contains_key("start_date"));
                            return Ok(Json(json!({"data":[],"next_token":"rest-page-2"})));
                        }
                        "rest_mode_period" if query["next_token"] == "rest-page-2" => json!([
                            {"id":"rest-1","start_day":"2026-04-24","end_day":null,
                                "start_time":"2026-04-24T16:30:00+09:00","end_time":null,
                                "episodes":[{"tags":["generic_tired"],"timestamp":"2026-04-24T16:30:00+09:00"}]}
                        ]),
                        "sleep" | "workout" | "daily_activity" | "daily_sleep" | "daily_readiness"
                        | "daily_stress" => json!([]),
                        _ => return Err(StatusCode::BAD_REQUEST),
                    };
                    Ok(Json(json!({"data":data,"next_token":null})))
                },
            ),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let origin = format!("http://{}", listener.local_addr()?);
        let server = tokio::spawn(async move { axum::serve(listener, app).await });
        let client = Client::new();
        let window = || -> Result<Window> {
            Ok(Window {
                start: "2026-04-24T00:00:00Z".parse()?,
                end: "2026-04-25T00:00:00Z".parse()?,
            })
        };
        let (baseline, _, _) = oura_at(&client, "oura-fixture", window()?, None, &origin).await?;
        assert_eq!(baseline.len(), 2);
        let (rows, grants, _) = oura_at(
            &client,
            "oura-fixture",
            window()?,
            Some("heart_health,session"),
            &origin,
        )
        .await?;
        assert_eq!(grants, ["metric"]);
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0]["payload"]["metric"], "daily_cardiovascular_age");
        assert_eq!(rows[1]["payload"]["metric"], "vo2_max");
        assert_eq!(rows[1]["payload"]["provider_data"]["vo2_max"], 45);
        assert_eq!(rows[2]["payload"]["metric"], "guided_session");
        assert_eq!(rows[2]["ended_at"], "2026-04-24T05:10:00Z");
        let (tags, grants, _) =
            oura_at(&client, "oura-fixture", window()?, Some("tag"), &origin).await?;
        assert_eq!(grants, ["metric"]);
        assert_eq!(tags.len(), 1);
        assert_eq!(tags[0]["source_id"], "enhanced_tag:hydration-tag");
        assert_eq!(tags[0]["started_at"], "2026-04-24T00:00:00Z");
        assert_eq!(
            tags[0]["payload"]["provider_data"]["end_time"],
            "2026-04-26T08:00:00"
        );
        let (daily, _, _) =
            oura_at(&client, "oura-fixture", window()?, Some("daily"), &origin).await?;
        let rest = daily
            .iter()
            .find(|row| row["source_id"] == "rest_mode_period:rest-1")
            .context("rest mode not collected")?;
        assert_eq!(rest["started_at"], "2026-04-24T00:00:00Z");
        assert!(rest["payload"]["provider_data"]["end_time"].is_null());
        assert_eq!(
            rest["payload"]["provider_data"]["episodes"][0]["tags"][0],
            "generic_tired"
        );
        let (restricted, _, _) =
            oura_at(&client, "oura-no-rest", window()?, Some("daily"), &origin).await?;
        assert_eq!(restricted.len(), baseline.len());
        assert!(restricted
            .iter()
            .all(|row| row["payload"]["metric"] != "rest_mode_period"));
        server.abort();
        Ok(())
    }

    #[tokio::test]
    #[ignore = "requires AVATAR_HSM_TEST_DATABASE_URL"]
    async fn oura_bulk_deleted_sleep_respects_newer_records_and_blocks_stale_replay() -> Result<()>
    {
        use axum::{extract::Path, routing::get, Json, Router};

        let router = Router::new().route(
            "/{endpoint}",
            get(|Path(endpoint): Path<String>| async move {
                let data = if endpoint == "sleep" {
                    json!([{"id":"removed","type":"deleted"},{"id":"newer","type":"deleted"},
                    {"id":"invalid id","type":"deleted"}])
                } else {
                    json!([])
                };
                Json(json!({"data":data,"next_token":null}))
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let origin = format!("http://{}", listener.local_addr()?);
        let provider = tokio::spawn(async move { axum::serve(listener, router).await });
        let start: DateTime<Utc> = "2026-04-24T00:00:00Z".parse()?;
        let end = start + CalendarDuration::days(1);
        let client = Client::new();
        let (_, _, legacy) =
            oura_at(&client, "fixture", Window { start, end }, None, &origin).await?;
        assert!(legacy.ids.is_empty());
        let (rows, grants, deletions) = oura_at(
            &client,
            "fixture",
            Window { start, end },
            Some("daily"),
            &origin,
        )
        .await?;
        provider.abort();
        assert!(rows.is_empty());
        assert_eq!(
            deletions
                .ids
                .iter()
                .map(String::as_str)
                .collect::<std::collections::BTreeSet<_>>(),
            ["newer", "removed"].into_iter().collect()
        );
        let pool = crate::store::connect(&std::env::var("AVATAR_HSM_TEST_DATABASE_URL")?).await?;
        let subject = "oura-bulk-delete-fixture";
        let other = "oura-bulk-delete-other";
        let db = pool.get().await?;
        for id in [subject, other] {
            db.execute("INSERT INTO avatar_hsm.subject VALUES($1,'UTC')", &[&id])
                .await?;
            db.execute(
                "INSERT INTO avatar_hsm.wearable_profile
                 VALUES($1,sha256(convert_to($1||'r','UTF8')),sha256(convert_to($1||'c','UTF8')))",
                &[&id],
            )
            .await?;
            db.execute(
                "INSERT INTO avatar_hsm.device_connection(subject_id,provider) VALUES($1,'oura')",
                &[&id],
            )
            .await?;
            db.execute(
                "INSERT INTO avatar_hsm.wearable_oauth_connection(subject_id,provider,sealed_tokens)
                 VALUES($1,'oura',''::bytea)",
                &[&id],
            ).await?;
        }
        let observed_at = deletions.observed_at;
        let session = |id, revision| {
            json!({
                "kind":"sleep","source":"Oura","source_id":id,"started_at":start+CalendarDuration::hours(3),
                "ended_at":start+CalendarDuration::hours(4),"modified_at":revision,"payload":{"stages":[]}
            })
        };
        for id in [subject, other] {
            let lease = wearables::issue_lease_for(&pool, id, "oura").await?;
            let snapshot = serde_json::from_value(
                json!({"lease":lease["lease"],"start":start,"end":end,
                "grants":["sleep"],"records":[session("removed",observed_at-CalendarDuration::seconds(1)),
                    session("newer",observed_at+CalendarDuration::seconds(1))]}),
            )?;
            wearables::store_snapshot(&pool, id, "oura", snapshot, false).await?;
        }
        let lease = wearables::issue_lease_for(&pool, subject, "oura").await?;
        let mut snapshot: wearables::Snapshot = serde_json::from_value(json!({
            "lease":lease["lease"],"start":start,"end":end,"grants":grants,"records":rows
        }))?;
        snapshot.oura_sleep_deletions = Some(deletions);
        wearables::store_snapshot(&pool, subject, "oura", snapshot, false).await?;
        let lease = wearables::issue_lease_for(&pool, subject, "oura").await?;
        let replay = serde_json::from_value(
            json!({"lease":lease["lease"],"start":start,"end":end,
            "grants":["sleep"],"records":[session("removed",observed_at-CalendarDuration::seconds(1))]}),
        )?;
        wearables::store_snapshot(&pool, subject, "oura", replay, false).await?;
        let query = || wearables::RecordQuery {
            start,
            end,
            kind: "sleep".into(),
            provider: Some("oura".into()),
            source: None,
            offset: 0,
            metric: None,
        };
        let result = wearables::records_for(&pool, subject, query(), true).await?;
        assert_eq!(result["records"].as_array().map(Vec::len), Some(1));
        assert_eq!(result["records"][0]["source_id"], "newer");
        let result = wearables::records_for(&pool, other, query(), true).await?;
        assert_eq!(result["records"].as_array().map(Vec::len), Some(2));
        Ok(())
    }

    #[tokio::test]
    #[ignore = "requires AVATAR_HSM_TEST_DATABASE_URL"]
    async fn oura_preserves_sleep_intervals_and_native_samples_in_mcp() -> Result<()> {
        let item = json!({
            "id":"sleep",
            "bedtime_start":"2026-04-24T05:00:00Z",
            "bedtime_end":"2026-04-24T05:02:50Z",
            "sleep_phase_30_sec":"112043",
            "sleep_phase_5_min":"2",
            "heart_rate":{"interval":30,"items":[64,null,66],"timestamp":"2026-04-24T14:00:00+09:00"},
            "hrv":{"interval":30,"items":[100,40,null,42.5,45,46,47,55],
                "timestamp":"2026-04-24T04:59:30Z"}
        });
        let window = Window {
            start: "2026-04-24T00:00:00Z".parse()?,
            end: "2026-04-25T00:00:00Z".parse()?,
        };
        let row = oura_point(&item, "sleep", &window)?.unwrap();
        let stages = row["payload"]["stages"].as_array().unwrap();
        assert_eq!(stages.len(), 4);
        assert_eq!(
            stages[0],
            json!({
                "stage":5,"start":"2026-04-24T05:00:00Z","end":"2026-04-24T05:01:00Z"
            })
        );
        assert_eq!(stages[2]["start"], "2026-04-24T05:02:00Z");
        assert_eq!(stages[3]["end"], "2026-04-24T05:02:50Z");
        assert_eq!(row["payload"]["stage_resolution_seconds"], 30);
        assert_eq!(
            row["payload"]["provider_data"]["heart_rate"],
            item["heart_rate"]
        );

        let mut alternate = item;
        alternate["sleep_phase_30_sec"] = Value::Null;
        let fallback = oura_point(&alternate, "sleep", &window)?.unwrap();
        assert_eq!(fallback["payload"]["stage_resolution_seconds"], 300);
        assert_eq!(fallback["payload"]["stages"][0]["stage"], 4);

        let workout = oura_point(
            &json!({"id":"exercise","activity":"running","calories":190.4,"distance":2400,
                "start_datetime":"2026-04-24T05:00:00Z","end_datetime":"2026-04-24T05:15:00Z"}),
            "workout",
            &window,
        )?
        .unwrap();
        assert_eq!(workout["payload"]["duration_seconds"], 900);
        assert_eq!(workout["payload"]["calories_kcal"], 190.4);

        let pool = crate::store::connect(&std::env::var("AVATAR_HSM_TEST_DATABASE_URL")?).await?;
        let subject = "oura-stage-test";
        let read_token = "oura-stage-fixture-read-token-0123456789abc";
        pool.get()
            .await?
            .execute(
                "INSERT INTO avatar_hsm.subject VALUES($1,'Asia/Seoul')",
                &[&subject],
            )
            .await?;
        pool.get()
            .await?
            .execute(
                "INSERT INTO avatar_hsm.wearable_profile VALUES
             ($1,sha256(convert_to($2,'UTF8')),sha256(convert_to($2||'c','UTF8')))",
                &[&subject, &read_token],
            )
            .await?;
        let oxygen_document = |day, id, spo2, bdi| -> Result<Value> {
            let item = json!({"id":id,"day":day,"spo2_percentage":{"average":spo2},
                "breathing_disturbance_index":bdi});
            let at = oura_day(&item["day"])?;
            metric(&item, "daily_spo2", "Oura", at, at, &window)?
                .context("missing Oura oxygen document")
        };
        let oxygen = oxygen_document("2026-04-24", "oxy", 98.2, 0)?;
        let invalid = oxygen_document("2026-04-24", "invalid", 101.0, -1)?;
        let readiness = json!({"id":"readiness-seoul","day":"2026-04-25",
            "timestamp":"2026-04-25T00:00:00+09:00","temperature_deviation":-0.31,
            "temperature_trend_deviation":null});
        let at = date(&readiness["timestamp"])?;
        let readiness = metric(&readiness, "daily_readiness", "Oura", at, at, &window)?
            .context("missing Oura readiness document")?;
        let cardiovascular =
            json!({"id":"cardio","day":"2026-04-24","pulse_wave_velocity":7.85,"vascular_age":47});
        let cardiovascular = metric(
            &cardiovascular,
            "daily_cardiovascular_age",
            "Oura",
            window.start,
            window.start,
            &window,
        )?
        .context("missing Oura cardiovascular document")?;
        let activity = json!({"id":"day-activity","day":"2026-04-24","class_5_min":"0?12345",
            "met":{"interval":60,"timestamp":"2026-04-24T15:00:00+09:00","items":[0.75,null,2.25,-1,3.0]}});
        let activity = metric(
            &activity,
            "daily_activity",
            "Oura",
            window.start,
            window.start,
            &window,
        )?
        .context("missing Oura activity document")?;
        let lease = wearables::issue_lease_for(&pool, subject, "oura").await?;
        wearables::store_snapshot(
            &pool,
            subject,
            "oura",
            serde_json::from_value(json!({
                "lease":lease["lease"],"start":window.start,"end":window.end,
                "grants":["sleep","workout","metric"],"records":[row,workout,oxygen,invalid,readiness,cardiovascular,activity]
            }))?,
            false,
        )
        .await?;
        let app = std::sync::Arc::new(crate::mcp::App {
            db: pool.clone(),
            read_token: String::new(),
            control_token: String::new(),
            rules: crate::acute::rules()?,
        });
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let server = tokio::spawn(async move { axum::serve(listener, crate::router(app)).await });
        let response = Client::new()
            .post(format!("http://{address}/mcp/wearable-read"))
            .bearer_auth(read_token)
            .json(
                &json!({"jsonrpc":"2.0","id":1,"method":"tools/call","params":{
                    "name":"wearable_sessions","arguments":{"kind":"sleep",
                        "start":window.start,"end":window.end,"provider":"oura"}
                }}),
            )
            .send()
            .await?
            .error_for_status()?
            .json::<Value>()
            .await?;
        let persisted = &response["result"]["structuredContent"]["records"][0]["payload"];
        assert_eq!(persisted["stages"], row["payload"]["stages"]);
        assert_eq!(
            persisted["provider_data"]["heart_rate"]["items"],
            json!([64, null, 66])
        );
        for (name, value, unit) in [
            ("oxygen_saturation", json!(98.2), "%"),
            ("breathing_disturbance_index", json!(0), "index"),
        ] {
            let response: Value = Client::new()
                .post(format!("http://{address}/mcp/wearable-read"))
                .bearer_auth(read_token)
                .json(
                    &json!({"jsonrpc":"2.0","id":1,"method":"tools/call","params":{
                        "name":"wearable_samples","arguments":{"kind":"metric","metric":name,
                           "start":window.start,"end":window.end,"provider":"oura"}
                    }}),
                )
                .send()
                .await?
                .error_for_status()?
                .json()
                .await?;
            let records = response["result"]["structuredContent"]["records"]
                .as_array()
                .unwrap();
            assert_eq!(records.len(), 1);
            let payload = &records[0]["payload"];
            assert_eq!(payload["time_precision"], "provider_calendar_date");
            assert_eq!(
                payload["provider_data"],
                json!({
                    "date":"2026-04-24","value":value,"unit":unit,"document_id":"oxy"
                })
            );
        }
        for (name, from, to, detail) in [
            (
                "skin_temperature_deviation",
                "2026-04-25T00:00:00Z",
                "2026-04-26T00:00:00Z",
                json!({"date":"2026-04-25","value":-0.31,"unit":"celsius","document_id":"readiness-seoul",
                    "provider_timestamp":"2026-04-25T00:00:00+09:00","provider_field":"temperature_deviation",
                    "measurement_context":"daily_readiness"}),
            ),
            (
                "pulse_wave_velocity",
                "2026-04-24T00:00:00Z",
                "2026-04-25T00:00:00Z",
                json!({"date":"2026-04-24","value":7.85,"unit":"m/s","document_id":"cardio",
                    "provider_field":"pulse_wave_velocity","measurement_context":"daily_cardiovascular_age"}),
            ),
        ] {
            let response: Value = Client::new()
                .post(format!("http://{address}/mcp/wearable-read"))
                .bearer_auth(read_token)
                .json(
                    &json!({"jsonrpc":"2.0","id":1,"method":"tools/call","params":{
                        "name":"wearable_samples","arguments":{"kind":"metric","metric":name,
                             "start":from,"end":to,"provider":"oura"}
                    }}),
                )
                .send()
                .await?
                .error_for_status()?
                .json()
                .await?;
            let records = response["result"]["structuredContent"]["records"]
                .as_array()
                .unwrap();
            assert_eq!(records.len(), 1, "{response}");
            assert_eq!(
                records[0]["payload"]["time_precision"],
                "provider_calendar_date"
            );
            assert_eq!(records[0]["payload"]["provider_data"], detail);
        }
        let read_activity = async |metric: &str| -> Result<Value> {
            let response: Value = Client::new()
                .post(format!("http://{address}/mcp/wearable-read"))
                .bearer_auth(read_token)
                .json(
                    &json!({"jsonrpc":"2.0","id":1,"method":"tools/call","params":{
                        "name":"wearable_samples","arguments":{"kind":"metric","metric":metric,
                        "start":window.start,"end":window.end,"provider":"oura"}
                    }}),
                )
                .send()
                .await?
                .error_for_status()?
                .json()
                .await?;
            Ok(response["result"]["structuredContent"]["records"].clone())
        };
        let classification = read_activity("daily_activity").await?;
        assert_eq!(
            classification[0]["payload"]["activity_classification"],
            json!({
                "date":"2026-04-24","interval_seconds":300,
                "slots":["non_wear",null,"rest","inactive","low","medium","high"],
                "time_basis":"provider_activity_day"
            })
        );
        let met = read_activity("activity_met").await?;
        let met = met
            .as_array()
            .context("missing activity MET measurements")?;
        assert_eq!(met.len(), 3);
        let base: DateTime<Utc> = "2026-04-24T06:00:00Z".parse()?;
        for (sample, (index, value)) in met.iter().zip([(0, 0.75), (2, 2.25), (4, 3.0)]) {
            let at = sample["started_at"]
                .as_str()
                .unwrap()
                .parse::<DateTime<Utc>>()?;
            assert_eq!(at, base + CalendarDuration::minutes(index));
            let detail = &sample["payload"]["provider_data"];
            assert_eq!(detail["unit"], "MET");
            assert_eq!(detail["sample_index"], index);
            assert_eq!(detail["value"].as_f64(), Some(value));
        }
        let lease = wearables::issue_lease_for(&pool, subject, "oura").await?;
        wearables::store_snapshot(
            &pool,
            subject,
            "oura",
            serde_json::from_value(json!({
                "lease":lease["lease"],"start":window.start,"end":window.end,
                "grants":["sleep"],"records":[row]
            }))?,
            false,
        )
        .await?;
        for (name, time, indexes, values, unit) in [
            (
                "sleep_heart_rate",
                "2026-04-24T14:00:00+09:00",
                vec![0, 2],
                vec![64.0, 66.0],
                "bpm",
            ),
            (
                "hrv_rmssd",
                "2026-04-24T04:59:30Z",
                vec![1, 3, 4, 5, 6],
                vec![40.0, 42.5, 45.0, 46.0, 47.0],
                "ms",
            ),
        ] {
            let response: Value = Client::new()
                .post(format!("http://{address}/mcp/wearable-read"))
                .bearer_auth(read_token)
                .json(&json!({"jsonrpc":"2.0","id":1,"method":"tools/call","params":{
                    "name":"wearable_samples","arguments":{"kind":"metric","metric":name,
                       "start":"2026-04-24T05:00:00Z","end":"2026-04-24T05:03:30Z","provider":"oura"}
                }}))
                .send().await?.error_for_status()?.json().await?;
            let content = &response["result"]["structuredContent"];
            assert_eq!(content["state"], "ready");
            let records = content["records"].as_array().unwrap();
            assert_eq!(records.len(), indexes.len());
            let anchor = DateTime::parse_from_rfc3339(time)?.to_utc();
            for ((sample, index), value) in records.iter().zip(indexes).zip(values) {
                let detail = &sample["payload"]["provider_data"];
                assert_eq!(detail["sleep_id"], "sleep");
                assert_eq!(detail["series_start"], time);
                assert_eq!(detail["sample_index"], index);
                assert_eq!(detail["interval_seconds"], 30);
                assert_eq!(detail["unit"], unit);
                assert_eq!(detail["value"].as_f64(), Some(value));
                let at = sample["started_at"]
                    .as_str()
                    .unwrap()
                    .parse::<DateTime<Utc>>()?;
                assert_eq!(at, anchor + CalendarDuration::seconds(30 * index));
            }
        }
        server.abort();
        Ok(())
    }

    #[tokio::test]
    #[ignore = "requires isolated PostgreSQL and the public Oura sandbox"]
    async fn oura_public_sandbox_persists_wearable_details_for_mcp() -> Result<()> {
        if std::env::var("AVATAR_HSM_TEST_OURA_SANDBOX").as_deref() != Ok("1") {
            return Ok(());
        }
        // The sandbox keeps VO₂ Max at its sample date even for requests for recent dates.
        let start = "2021-01-01T00:00:00Z".parse()?;
        let end = "2021-01-08T00:00:00Z".parse()?;
        let client = http_client()?;
        let (rows, grants, _) = oura_at(
            &client,
            "sandbox-fixture",
            Window { start, end },
            Some("daily tag heart_health spo2"),
            "https://api.ouraring.com/v2/sandbox/usercollection",
        )
        .await?;
        let pool = crate::store::connect(&std::env::var("AVATAR_HSM_TEST_DATABASE_URL")?).await?;
        let subject = "oura-public-sandbox-test";
        let read_token = "oura-public-sandbox-local-fixture-read-0123456789";
        let db = pool.get().await?;
        db.execute(
            "INSERT INTO avatar_hsm.subject VALUES($1,'Asia/Seoul')",
            &[&subject],
        )
        .await?;
        db.execute(
            "INSERT INTO avatar_hsm.wearable_profile VALUES
            ($1,sha256(convert_to($2,'UTF8')),sha256(convert_to($2||'c','UTF8')))",
            &[&subject, &read_token],
        )
        .await?;
        drop(db);
        let lease = wearables::issue_lease_for(&pool, subject, "oura").await?;
        let snapshot = serde_json::from_value(json!({
            "lease":lease["lease"],"start":start,"end":end,"grants":grants,"records":rows
        }))?;
        wearables::store_snapshot(&pool, subject, "oura", snapshot, false).await?;
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let app = std::sync::Arc::new(crate::mcp::App {
            db: pool,
            read_token: String::new(),
            control_token: String::new(),
            rules: crate::acute::rules()?,
        });
        let server = tokio::spawn(async move { axum::serve(listener, crate::router(app)).await });
        for (metric, field, derived) in [
            ("rest_mode_period", "episodes", None),
            ("enhanced_tag", "start_time", None),
            ("vo2_max", "vo2_max", None),
            (
                "oxygen_saturation",
                "value",
                Some(("daily_spo2", "/spo2_percentage/average", "%")),
            ),
            (
                "breathing_disturbance_index",
                "value",
                Some(("daily_spo2", "/breathing_disturbance_index", "index")),
            ),
            (
                "readiness_score",
                "value",
                Some(("daily_readiness", "/score", "score")),
            ),
            (
                "skin_temperature_deviation",
                "value",
                Some(("daily_readiness", "/temperature_deviation", "celsius")),
            ),
            (
                "skin_temperature_trend_deviation",
                "value",
                Some(("daily_readiness", "/temperature_trend_deviation", "celsius")),
            ),
            (
                "high_stress_duration",
                "value",
                Some(("daily_stress", "/stress_high", "s")),
            ),
            (
                "high_recovery_duration",
                "value",
                Some(("daily_stress", "/recovery_high", "s")),
            ),
            (
                "vascular_age",
                "value",
                Some(("daily_cardiovascular_age", "/vascular_age", "years")),
            ),
        ] {
            let reply = client
                .post(format!("http://{address}/mcp/wearable-read"))
                .bearer_auth(read_token)
                .json(
                    &json!({"jsonrpc":"2.0","id":1,"method":"tools/call","params":{
                        "name":"wearable_samples","arguments":{"kind":"metric","metric":metric,
                            "start":start,"end":end,"provider":"oura"}
                    }}),
                )
                .send()
                .await?
                .error_for_status()?
                .json::<Value>()
                .await?;
            let saved = reply["result"]["structuredContent"]["records"]
                .as_array()
                .with_context(|| format!("missing MCP records for {metric}"))?;
            if let Some((document, path, unit)) = derived {
                let expected: Vec<_> = rows
                    .iter()
                    .filter(|row| row["payload"]["metric"] == document)
                    .filter_map(|row| {
                        let data = &row["payload"]["provider_data"];
                        Some((&data["id"], &data["day"], data.pointer(path)?.as_f64()?))
                    })
                    .collect();
                assert!(!expected.is_empty());
                assert_eq!(saved.len(), expected.len());
                for (id, day, value) in expected {
                    assert!(saved.iter().any(|record| {
                        let detail = &record["payload"]["provider_data"];
                        detail["document_id"] == *id
                            && detail["date"] == *day
                            && detail[field].as_f64() == Some(value)
                            && detail["unit"] == unit
                    }));
                }
                continue;
            }
            let original: Vec<_> = rows
                .iter()
                .filter(|row| row["payload"]["metric"] == metric)
                .collect();
            ensure!(
                !original.is_empty(),
                "the Oura sandbox returned no {metric} records"
            );
            assert_eq!(saved.len(), original.len());
            for row in original {
                assert!(row["payload"]["provider_data"].get(field).is_some());
                assert!(saved
                    .iter()
                    .any(|saved| saved["source_id"] == row["source_id"]
                        && saved["payload"]["provider_data"] == row["payload"]["provider_data"]));
            }
        }
        let first_activity = rows
            .iter()
            .find(|row| {
                row["payload"]["metric"] == "daily_activity"
                    && row["payload"]["provider_data"]["day"] == "2021-01-01"
            })
            .context("sandbox daily activity missing")?;
        let original = &first_activity["payload"]["provider_data"];
        let met_at = date(&original["met"]["timestamp"])?;
        let reply = client
            .post(format!("http://{address}/mcp/wearable-read"))
            .bearer_auth(read_token)
            .json(
                &json!({"jsonrpc":"2.0","id":1,"method":"tools/call","params":{
                    "name":"wearable_samples","arguments":{"kind":"metric","metric":"activity_met",
                        "start":met_at,"end":met_at+CalendarDuration::seconds(1),"provider":"oura"}
                }}),
            )
            .send()
            .await?
            .error_for_status()?
            .json::<Value>()
            .await?;
        let saved = reply["result"]["structuredContent"]["records"]
            .as_array()
            .context("sandbox MET missing")?;
        assert!(saved.iter().any(|sample| {
            let detail = &sample["payload"]["provider_data"];
            detail["document_id"] == original["id"]
                && detail["sample_index"] == 0
                && detail["value"].as_f64() == original["met"]["items"][0].as_f64()
        }));
        server.abort();
        Ok(())
    }

    #[test]
    fn imports_confirmed_manufacturer_contracts() -> Result<()> {
        let window = Window {
            start: "2026-04-24T00:00:00Z".parse()?,
            end: "2026-04-25T00:00:00Z".parse()?,
        };
        let point = oura_point(
            &json!({"id":"oura-1","bedtime_start":"2026-04-24T08:00:00+09:00",
            "bedtime_end":"2026-04-24T15:00:00+09:00"}),
            "sleep",
            &window,
        )?;
        assert!(point.is_none());
        let point = oura_point(
            &json!({"id":"oura-2","bedtime_start":"2026-04-24T11:00:00+09:00",
            "bedtime_end":"2026-04-24T16:00:00+09:00","total_sleep_duration":15000}),
            "sleep",
            &window,
        )?
        .unwrap();
        assert_eq!(point["payload"]["provider_total_sleep_seconds"], 15000);
        let point = whoop_point(&json!({"id":"w","start":"2026-04-24T02:25:44.774Z","end":"2026-04-24T10:25:44.774Z",
            "updated_at":"2026-04-24T14:25:44.774Z","score":{"stage_summary":{"total_rem_sleep_time_milli":5879573}}}),
            "sleep",&window)?.unwrap();
        assert_eq!(
            point["payload"]["provider_stage_summary"]["total_rem_sleep_time_milli"],
            5879573
        );
        let point = polar_point(&json!({"id":"p","device":"Polar M400","start_time":"2026-04-24T10:40:02",
          "start_time_utc_offset":180,"upload_time":"2026-04-24T19:40:02Z","duration":"PT2H44M","sport":"RUNNING"}),&window)?.unwrap();
        assert_eq!(point["started_at"], "2026-04-24T07:40:02Z");
        assert_eq!(point["ended_at"], "2026-04-24T10:24:02Z");
        let point = intervals_point(
            &json!({"id":"i1","start_date":"2026-04-24T10:40:00Z","elapsed_time":60,"type":"Run"}),
            &window,
        )?
        .unwrap();
        assert_eq!(point["ended_at"], "2026-04-24T10:41:00Z");
        Ok(())
    }
}

#[cfg(test)]
pub(super) async fn withings(
    client: &Client,
    token: &str,
    start: DateTime<Utc>,
    end: DateTime<Utc>,
) -> Result<Vec<Value>> {
    withings_sleep_at(
        client,
        token,
        &Window { start, end },
        "https://wbsapi.withings.net/v2/sleep",
    )
    .await
}

#[cfg(test)]
pub(super) async fn withings_sleep_at(
    client: &Client,
    token: &str,
    window: &Window,
    endpoint: &str,
) -> Result<Vec<Value>> {
    let oldest = (window.start - CalendarDuration::days(1))
        .format("%Y-%m-%d")
        .to_string();
    let newest = (window.end + CalendarDuration::days(1))
        .format("%Y-%m-%d")
        .to_string();
    let mut offset = 0;
    let mut output = Vec::new();
    for _ in 0..32 {
        let request = client
                .post(endpoint)
                .bearer_auth(token)
                .form(&[
                    ("action", "getsummary"),
                    ("startdateymd", &oldest),
                    ("enddateymd", &newest),
                    (
                        "data_fields",
                        concat!(
                            "total_timeinbed,total_sleep_time,asleepduration,deepsleepduration,lightsleepduration,remsleepduration,",
                            "wakeupduration,sleep_efficiency,sleep_latency,wakeup_latency,wakeupcount,waso,nb_rem_episodes,out_of_bed_count,",
                            "hr_average,hr_min,hr_max,rr_average,rr_min,rr_max,snoring,snoringepisodecount,sleep_score,rmssd_start_avg,rmssd_end_avg,",
                            "breathing_quality_assessment,night_events,mvt_score_avg,mvt_active_duration,breathing_sounds,breathing_sounds_episode_count,",
                            "chest_movement_rate_wellness_average,chest_movement_rate_wellness_min,chest_movement_rate_wellness_max,",
                            "chest_movement_rate_average,chest_movement_rate_min,chest_movement_rate_max,",
                            "core_body_temperature_min,core_body_temperature_max,core_body_temperature_avg,core_body_temperature_status"
                        ),
                    ),
                    ("offset", &offset.to_string()),
                ]);
        let Some(payload) = super::withings_detail::page(request).await? else {
            ensure!(offset == 0, "incomplete Withings sleep summaries");
            return Ok(output);
        };
        for item in records(&payload["series"])? {
            if let Some(mut row) = withings_point(item, window)? {
                let (stages, segments) =
                    super::withings_detail::sleep_segments(client, token, endpoint, item).await?;
                row["payload"]["stages"] = json!(stages);
                row["payload"]["provider_sleep_segments"] = json!(segments);
                output.push(row);
            }
        }
        if !matches!(payload["more"], Value::Bool(true)) && payload["more"].as_u64() != Some(1) {
            return Ok(output);
        }
        let next = integer(&payload["offset"])?;
        ensure!(next > offset, "invalid Withings offset");
        offset = next;
    }
    bail!("Withings pagination exceeded the limit")
}

#[cfg(test)]
pub(super) async fn withings_measurements(
    client: &Client,
    token: &str,
    window: &Window,
) -> Result<Vec<Value>> {
    withings_measurements_at(client, token, window, "https://wbsapi.withings.net/measure").await
}

#[cfg(test)]
pub(super) async fn withings_measurements_at(
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
                    ("action", "getmeas".to_owned()),
                    ("startdate", window.start.timestamp().to_string()),
                    ("enddate", window.end.timestamp().to_string()),
                    ("category", "1".to_owned()),
                    ("offset", offset.to_string()),
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
            let at = epoch_seconds(&item["date"])?;
            let point =
                json!({"id":item["grpid"],"category":item["category"],"measures":item["measures"]});
            let Some(group) = metric(&point, "measurements", "Withings", at, at, window)? else {
                continue;
            };
            let id = group["source_id"]
                .as_str()
                .context("missing Withings group ID")?;
            let mut emitted = std::collections::HashSet::new();
            for measure in item["measures"].as_array().into_iter().flatten() {
                if let Some((name, unit, value)) = withings_measurement_value(measure) {
                    if !emitted.insert(name) {
                        continue;
                    }
                    rows.push(record(
                        "metric",
                        "Withings",
                        &format!("{id}:{name}"),
                        at,
                        at,
                        window.end,
                        json!({"metric":name,"provider_data":{"unit":unit,"value":value,"measurement":measure}}),
                    ));
                }
            }
            rows.push(group);
        }
        if page["more"].as_bool() != Some(true) && page["more"].as_u64() != Some(1) {
            return Ok(rows);
        }
        let next = integer(&page["offset"])?;
        ensure!(next > offset, "invalid Withings measurement offset");
        offset = next;
    }
    bail!("Withings measurement pagination exceeded the limit")
}

#[cfg(test)]
fn withings_measurement_value(measure: &Value) -> Option<(&'static str, &'static str, f64)> {
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
    // Withings sends an integer mantissa and a base-10 exponent.
    let value = measure["value"].as_i64()? as f64 * 10_f64.powi(exponent as i32);
    Some((name, unit, value))
}

#[cfg(test)]
fn withings_point(item: &Value, window: &Window) -> Result<Option<Value>> {
    let epoch = |field: &str| {
        DateTime::from_timestamp(
            item[field].as_i64().context("missing Withings timestamp")?,
            0,
        )
        .context("invalid Withings timestamp")
    };
    let start = epoch("startdate")?;
    let end = epoch("enddate")?;
    if start >= end || !window.contains(start, end) {
        return Ok(None);
    }
    let id = item["id"]
        .as_str()
        .map(str::to_owned)
        .or_else(|| item["id"].as_u64().map(|id| id.to_string()))
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
