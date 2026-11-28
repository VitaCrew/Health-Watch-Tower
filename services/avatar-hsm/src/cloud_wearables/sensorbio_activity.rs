#[cfg(test)]
use super::vendor_metrics::optional;
use super::*;
use chrono::NaiveDate;
use serde_json::Map;

#[cfg(test)]
const ORIGIN: &str = "https://api.sensorbio.com";
const SOURCE: &str = "Sensor Bio";

#[cfg(test)]
pub(super) async fn fetch(
    client: &Client,
    token: &str,
    start: DateTime<Utc>,
    end: DateTime<Utc>,
) -> Result<Vec<Value>> {
    fetch_at(client, token, Window { start, end }, ORIGIN).await
}

#[cfg(test)]
async fn fetch_at(
    client: &Client,
    token: &str,
    window: Window,
    origin: &str,
) -> Result<Vec<Value>> {
    let (mut rows, workouts, intervals) = tokio::try_join!(
        daily(client, token, &window, origin),
        workouts(client, token, &window, origin),
        minute_activity(client, token, &window, origin),
    )?;
    rows.extend(workouts);
    rows.extend(intervals);
    Ok(rows)
}

#[cfg(test)]
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

#[cfg(test)]
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

#[cfg(test)]
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

pub(crate) fn daily_hourly_details(payload: &mut Value) {
    let labels: &[(&str, &str)] = match payload["metric"].as_str() {
        Some("daily_activity") => &[
            ("Steps", "steps"),
            ("Distance", "distance"),
            ("Duration", "active_duration"),
            ("Calories", "step_energy"),
        ],
        Some("daily_calories") => &[
            ("Resting Calories", "basal_energy"),
            ("Step Calories", "step_energy"),
            ("Active Calories", "active_energy"),
            ("Workout Calories", "workout_energy"),
            ("Total Calories", "total_energy"),
        ],
        _ => return,
    };
    let detail = &mut payload["provider_data"];
    let raw = &detail["raw"];
    if raw.get("granularity").is_some_and(|value| value != "day") {
        return;
    }
    let Some(day) = detail["date"]
        .as_str()
        .and_then(|date| date.parse::<NaiveDate>().ok())
    else {
        return;
    };
    let Some(metrics) = raw["metrics"].as_array() else {
        return;
    };
    let mut series = Vec::new();
    for entry in metrics {
        let name = entry["type"].as_str().or_else(|| entry["name"].as_str());
        let Some((_, metric)) = labels.iter().find(|(label, _)| Some(*label) == name) else {
            continue;
        };
        let Some(timeseries) = entry["timeseries"].as_array() else {
            continue;
        };
        let mut points: Vec<_> = timeseries
            .iter()
            .filter_map(|point| {
                // These labels follow the provider's local clock. The encoded
                // offset cannot identify the instant when the bucket was recorded.
                let local_time = DateTime::parse_from_rfc3339(point["time"].as_str()?)
                    .ok()?
                    .naive_local();
                let value = point["value"].as_f64()?;
                (local_time.date() == day && value.is_finite() && value >= 0.0)
                    .then_some((local_time, &point["value"]))
            })
            .collect();
        if points.is_empty() {
            continue;
        }
        points.sort_by_key(|(time, _)| *time);
        let points: Vec<_> = points
            .into_iter()
            .map(|(time, value)| json!({"local_time":time,"value":value}))
            .collect();
        let unit = entry["unit"].as_str().filter(|unit| !unit.is_empty());
        series.push(json!({"metric":metric,"unit":unit,"points":points}));
    }
    if !series.is_empty() {
        detail["hourly"] = json!({"time_basis":"provider_local_wall_clock","series":series});
    }
}

#[cfg(test)]
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
    for _ in 0..=wearables::HIGH_VOLUME_CLOUD_RECORD_LIMIT.div_ceil(PAGE_SIZE) {
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
            rows.len() <= wearables::HIGH_VOLUME_CLOUD_RECORD_LIMIT,
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

#[cfg(test)]
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
            rows.len() <= wearables::HIGH_VOLUME_CLOUD_RECORD_LIMIT,
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

#[cfg(test)]
fn millis(value: &Value) -> Option<DateTime<Utc>> {
    DateTime::from_timestamp_millis(value.as_f64()?.round() as i64)
}

#[cfg(test)]
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
    let id = crate::wearable_webhooks::account_id(&item["id"])
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

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{
        extract::{OriginalUri, Query},
        http::{HeaderMap, StatusCode},
        response::IntoResponse,
        routing::get,
        Json, Router,
    };
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    };

    #[test]
    fn sensorbio_recovery_does_not_invent_a_score_or_accept_another_day() -> Result<()> {
        let at: DateTime<Utc> = "2026-04-25T00:00:00Z".parse()?;
        let day = at.date_naive();
        let empty = json!({"date":day,"granularity":"day","score_datapoints":[]});
        assert!(recovery_record(&empty, day, at, at)?.is_none());
        let different_day = json!({"date":"2026-04-26","granularity":"day","score":92});
        assert!(recovery_record(&different_day, day, at, at).is_err());
        let wrong_granularity = json!({"date":day,"granularity":"week","score":77});
        assert!(recovery_record(&wrong_granularity, day, at, at).is_err());
        let baseline_only =
            json!({"date":day,"granularity":"day","score_baseline":61.25,"score_datapoints":[]});
        let row = recovery_record(&baseline_only, day, at, at)?.unwrap();
        let fields = &row["payload"]["provider_data"];
        assert_eq!(fields["score_baseline"], 61.25);
        assert!(fields.get("score").is_none());
        Ok(())
    }

    #[tokio::test]
    #[ignore = "requires AVATAR_HSM_TEST_DATABASE_URL"]
    async fn sensorbio_activity_keeps_reported_daily_totals_and_paged_workouts() -> Result<()> {
        let start: DateTime<Utc> = "2026-04-25T00:00:00Z".parse()?;
        let end = start + CalendarDuration::days(1);
        let at = start.timestamp_millis();
        let calls = Arc::new(AtomicUsize::new(0));
        let app = Router::new().fallback(get(move |OriginalUri(uri): OriginalUri, headers: HeaderMap,
            Query(query): Query<std::collections::HashMap<String, String>>| {
            let calls = Arc::clone(&calls);
            async move {
            assert_eq!(headers["authorization"], "Bearer fixture");
            Json(match uri.path() {
                "/v1/step/details" => {
                    assert_eq!(query["date"], "2026-04-25");
                    assert_eq!(query["granularity"], "day");
                    json!({"date":"2026-04-25","granularity":"day","metrics":[
                        {"name":"Steps","value":0,"timeseries":[
                            {"time":"2026-04-25T08:00:00Z","value":0},
                            {"time":"2026-04-24T23:00:00Z","value":900},
                            {"time":"2026-04-25T07:00:00Z","value":0},
                            {"date":"2026-04-25","value":900}]},
                        {"type":"Distance","value":1.5,"unit":"km","timeseries":[
                            {"time":"2026-04-25T07:00:00Z","value":1.5},
                            {"time":"2026-04-25T08:00:00Z","value":-1}]}]})
                }
                "/v1/calorie/details" => {
                    assert_eq!(query["date"], "2026-04-25");
                    json!({"date":"2026-04-25","granularity":"day","metrics":[{"name":"Total Calories","value":1900,"unit":"kcal",
                        "timeseries":[{"time":"2026-04-25T07:00:00Z","value":74.5}]},
                        {"name":"Resting Calories","value":1600},{"name":"Workout Calories","value":90}]})
                }
                "/v1/scores/recovery/details" => {
                    assert_eq!(query["date"], "2026-04-25");
                    assert_eq!(query["granularity"], "day");
                    assert!(!query.contains_key("user_id"));
                    match calls.fetch_add(1, Ordering::SeqCst) {
                        0 => json!({"date":"2026-04-25","granularity":"day","score":0,
                            "score_baseline":63.5,"score_improvement_over_baseline_percentage":-100,
                            "score_datapoints":[{"date":"2026-04-24","value":85},{"date":"2026-04-25","value":0}]}),
                        1 => json!({"date":"2026-04-25","granularity":"day","score":73.5,
                            "score_datapoints":[{"date":"2026-04-25T00:00:00-07:00","value":73.5}]}),
                        _ => return StatusCode::FORBIDDEN.into_response(),
                    }
                }
                "/v1/steps" => {
                    assert_eq!(query["limit"], "50");
                    assert!(!query.contains_key("user_id"));
                    if query["last-timestamp"] == (at - 1).to_string() {
                        json!({"data":[{"timestamp":at,"steps":0,"distance":0.0,"total_step_calories":0.0}],
                            "links":{"next":"https://example.invalid/untrusted-next-link"}})
                    } else {
                        assert_eq!(query["last-timestamp"], at.to_string());
                        json!({"data":[{"timestamp":at+60000,"steps":18,"distance":10.5,"total_step_calories":0.5},
                            {"timestamp":end.timestamp_millis(),"steps":50}],
                            "links":{"next":"https://example.invalid/must-not-follow"}})
                    }
                }
                "/v1/activities" if query["last-timestamp"] != at.to_string() => json!({
                    "data":[{"timestamp":at,"name":"Run","activities":[{"id":"r1","start_time":at+1000,"end_time":at+60000}]}],
                    "links":{"next":"https://example.invalid/untrusted-next-link"}
                }),
                "/v1/activities" => json!({"data":[{"timestamp":at+1,"activities":[
                    {"id":"r2","likely_name":"Walk","start_time":at+60000,"end_time":at+90000,
                     "cardio_metrics":{"avg_bpm":110}}]}]}),
                unknown => panic!("unexpected path {unknown}"),
            }).into_response()
        }}));
        let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0)).await?;
        let origin = format!("http://{}", listener.local_addr()?);
        let server = tokio::spawn(async move { axum::serve(listener, app).await });
        let client = Client::builder().http2_prior_knowledge().build()?;
        let rows = fetch_at(&client, "fixture", Window { start, end }, &origin).await?;
        assert_eq!(rows.len(), 7);
        assert_eq!(
            rows[0]["payload"]["provider_data"]["raw"]["metrics"][0]["timeseries"][0]["value"],
            0
        );
        assert_eq!(
            rows.iter().find(|row| row["source_id"] == "r1").unwrap()["payload"]["exercise_type"],
            "Run"
        );
        let pool = crate::store::connect(&env::var("AVATAR_HSM_TEST_DATABASE_URL")?).await?;
        let subject = "sensorbio-activity-fixture";
        pool.get()
            .await?
            .execute(
                "INSERT INTO avatar_hsm.subject VALUES ($1,'America/Los_Angeles')",
                &[&subject],
            )
            .await?;
        let lease = wearables::issue_lease_for(&pool, subject, "sensorbio").await?;
        wearables::store_snapshot(&pool, subject, "sensorbio", serde_json::from_value(json!({
            "lease":lease["lease"],"start":start,"end":end,"grants":["steps","metric","workout"],"records":rows
        }))?, false).await?;
        for (kind, field, expected) in [
            ("steps", "total", 0.0),
            ("activity", "distance_meters", 1500.0),
            ("activity", "active_energy_kcal", 300.0),
        ] {
            let result = wearables::daily_for(
                &pool,
                subject,
                wearables::DailyQuery {
                    start_day: start.date_naive(),
                    end_day: start.date_naive(),
                    kind: Some(kind.into()),
                    provider: Some("sensorbio".into()),
                    source: None,
                },
            )
            .await?;
            assert_eq!(result["state"], "ready");
            assert_eq!(result["records"][0]["calculated_by"], "provider");
            assert_eq!(
                result["records"][0]["stats"][field].as_f64(),
                Some(expected)
            );
        }
        let sessions = wearables::records_for(
            &pool,
            subject,
            wearables::RecordQuery {
                kind: "workout".into(),
                start,
                end,
                offset: 0,
                source: None,
                provider: Some("sensorbio".into()),
                metric: None,
            },
            true,
        )
        .await?;
        assert_eq!(sessions["records"].as_array().map(Vec::len), Some(2));
        let recovery_query = || wearables::RecordQuery {
            kind: "metric".into(),
            start,
            end,
            offset: 0,
            source: None,
            provider: Some("sensorbio".into()),
            metric: Some("recovery_details".into()),
        };
        let recorded = wearables::records_for(&pool, subject, recovery_query(), false).await?;
        let details = &recorded["records"][0]["payload"]["provider_data"];
        assert_eq!(details["score"].as_f64(), Some(0.0));
        assert_eq!(details["score_baseline"], 63.5);
        assert_eq!(
            details["score_improvement_over_baseline_percentage"].as_f64(),
            Some(-100.0)
        );
        assert_eq!(
            details["score_datapoints"].as_array().map(Vec::len),
            Some(1)
        );
        for expected_rows in [7, 6] {
            let rows = fetch_at(&client, "fixture", Window { start, end }, &origin).await?;
            assert_eq!(rows.len(), expected_rows);
            let lease = wearables::issue_lease_for(&pool, subject, "sensorbio").await?;
            wearables::store_snapshot(&pool, subject, "sensorbio", serde_json::from_value(json!({
                "lease":lease["lease"],"start":start,"end":end,"grants":["steps","metric","workout"],"records":rows
            }))?, false).await?;
        }
        server.abort();
        let after = wearables::records_for(&pool, subject, recovery_query(), false).await?;
        assert_eq!(after["records"].as_array().map(Vec::len), Some(1));
        let details = &after["records"][0]["payload"]["provider_data"];
        assert_eq!(details["score"], 73.5);
        assert_eq!(details["score_datapoints"][0]["value"], 73.5);
        assert!(details.get("score_baseline").is_none());
        let read_token = "sensorbio-recovery-fixture-read-0123456789012345";
        pool.get().await?.execute(
            "INSERT INTO avatar_hsm.wearable_profile VALUES ($1,sha256(convert_to($2,'UTF8')),sha256(convert_to($1||'c','UTF8')))",
            &[&subject,&read_token],
        ).await?;
        let hsm = Arc::new(crate::mcp::App {
            db: pool,
            read_token: String::new(),
            control_token: String::new(),
            rules: crate::acute::rules()?,
        });
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let url = format!("http://{}/mcp/wearable-read", listener.local_addr()?);
        let server = tokio::spawn(async move { axum::serve(listener, crate::router(hsm)).await });
        let client = Client::new();
        for (metric, expected) in [
            (
                "daily_activity",
                json!([
                    {"metric":"steps","unit":null,"points":[
                        {"local_time":"2026-04-25T07:00:00","value":0},
                        {"local_time":"2026-04-25T08:00:00","value":0}]},
                    {"metric":"distance","unit":"km","points":[
                        {"local_time":"2026-04-25T07:00:00","value":1.5}]}
                ]),
            ),
            (
                "daily_calories",
                json!([
                    {"metric":"total_energy","unit":"kcal","points":[
                        {"local_time":"2026-04-25T07:00:00","value":74.5}]}
                ]),
            ),
        ] {
            let response: Value = client.post(&url).bearer_auth(read_token)
                .json(&json!({"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"wearable_samples","arguments":{
                    "provider":"sensorbio","kind":"metric","metric":metric,"start":start,"end":end
                }}}))
                .send().await?.error_for_status()?.json().await?;
            let data =
                &response["result"]["structuredContent"]["records"][0]["payload"]["provider_data"];
            assert_eq!(data["hourly"]["time_basis"], "provider_local_wall_clock");
            assert_eq!(data["hourly"]["series"], expected);
            assert!(data["raw"]["metrics"][0]["timeseries"].is_array());
        }
        let minutes: Value = client.post(&url).bearer_auth(read_token)
            .json(&json!({"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"wearable_samples","arguments":{
                "provider":"sensorbio","kind":"metric","metric":"activity_minute","start":start,"end":end
            }}}))
            .send().await?.error_for_status()?.json().await?;
        let minutes = minutes["result"]["structuredContent"]["records"]
            .as_array()
            .unwrap();
        assert_eq!(minutes.len(), 2);
        let first = minutes
            .iter()
            .find(|row| row["source_id"] == format!("activity-minute:{at}"))
            .unwrap();
        assert_eq!(first["payload"]["provider_data"]["steps"], 0);
        assert_eq!(first["started_at"], "2026-04-25T00:00:00+00:00");
        assert_eq!(first["ended_at"], "2026-04-25T00:01:00+00:00");
        let second = minutes
            .iter()
            .find(|row| row["source_id"] == format!("activity-minute:{}", at + 60000))
            .unwrap();
        assert_eq!(second["payload"]["provider_data"]["distance_meters"], 10.5);
        assert_eq!(second["payload"]["provider_data"]["step_energy_kcal"], 0.5);
        let details: Value = Client::new().post(url).bearer_auth(read_token)
            .json(&json!({"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"wearable_samples","arguments":{
                "provider":"sensorbio","kind":"metric","metric":"recovery_details","start":start,"end":end
            }}}))
            .send().await?.error_for_status()?.json().await?;
        server.abort();
        let data =
            &details["result"]["structuredContent"]["records"][0]["payload"]["provider_data"];
        assert_eq!(data["date"], "2026-04-25");
        assert_eq!(data["score"], 73.5);
        Ok(())
    }
}
