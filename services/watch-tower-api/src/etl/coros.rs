use anyhow::{ensure, Context, Result};
use avatar_hsm::etl_adapter::{self, Snapshot};
use chrono::{DateTime, Duration, Months, Utc};
use reqwest::{Client, Response};
use serde_json::{json, Value};

const API: &str = "https://open.coros.com";
const MAX_RECORDS: usize = 4_000;

#[derive(Debug)]
pub struct ProviderRateLimited {
    pub retry_at: DateTime<Utc>,
}

impl std::fmt::Display for ProviderRateLimited {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("COROS returned HTTP 429")
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
    open_id: &str,
    lease: &str,
    start: DateTime<Utc>,
    end: DateTime<Utc>,
) -> Result<Snapshot> {
    collect_from_origin(client, token, open_id, lease, start, end, API).await
}

async fn collect_from_origin(
    client: &Client,
    token: &str,
    open_id: &str,
    lease: &str,
    start: DateTime<Utc>,
    end: DateTime<Utc>,
    api: &str,
) -> Result<Snapshot> {
    ensure!(
        !token.trim().is_empty() && !open_id.trim().is_empty() && !lease.trim().is_empty(),
        "COROS credentials and HSM lease are required"
    );
    ensure!(
        start < end && end - start <= Duration::days(7),
        "invalid COROS window"
    );
    let observed_at = Utc::now();
    let today = observed_at.date_naive();
    let earliest = today.checked_sub_months(Months::new(3)).unwrap_or(today);
    let from = (start - Duration::days(1)).date_naive().max(earliest);
    let to = (end + Duration::days(1)).date_naive();
    let mut rows = Vec::new();
    let mut source_items = Vec::new();
    if to >= from {
        let from = from.format("%Y%m%d").to_string();
        let to = to.format("%Y%m%d").to_string();
        let workouts = fetch(
            client,
            api,
            "v2/coros/sport/list",
            token,
            open_id,
            &from,
            &to,
        )
        .await?;
        ensure!(workouts["result"] == "0000", "COROS workout request failed");
        let window = Window { start, end };
        for item in list(&workouts["data"])? {
            if let Some(row) = workout(item, &window)? {
                source_items.push(item.clone());
                rows.push(row);
            }
        }
        let daily = fetch(client, api, "coros/daily/query", token, open_id, &from, &to).await?;
        ensure!(daily["result"] == "0000", "COROS daily request failed");
        for item in list(&daily["data"]["dailyList"])? {
            append_day(&mut rows, item, &window)?;
        }
    }
    ensure!(
        rows.len() <= MAX_RECORDS,
        "COROS snapshot record limit exceeded"
    );
    for row in &mut rows {
        row["modified_at"] = json!(observed_at);
    }
    let mut snapshot: Snapshot = serde_json::from_value(json!({
        "provider":"coros", "lease":lease, "start":start, "end":end,
        "grants":["workout","sleep","metric","steps"], "records":rows
    }))?;
    etl_adapter::attach_coros_fit_sources(&mut snapshot, &source_items);
    Ok(snapshot)
}

async fn fetch(
    client: &Client,
    api: &str,
    path: &str,
    token: &str,
    open_id: &str,
    from: &str,
    to: &str,
) -> Result<Value> {
    let response = client
        .get(format!("{api}/{path}"))
        .query(&[
            ("token", token),
            ("openId", open_id),
            ("startDate", from),
            ("endDate", to),
        ])
        .send()
        .await
        .map_err(reqwest::Error::without_url)?;
    let response = check_response(response)?;
    let mut response = response;
    let mut bytes = Vec::new();
    while let Some(chunk) = response
        .chunk()
        .await
        .map_err(reqwest::Error::without_url)?
    {
        ensure!(
            bytes.len() + chunk.len() <= 8 * 1024 * 1024,
            "COROS response too large"
        );
        bytes.extend_from_slice(&chunk);
    }
    Ok(serde_json::from_slice(&bytes)?)
}

fn check_response(response: Response) -> Result<Response> {
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
                    .and_then(Duration::try_seconds)
                    .and_then(|delay| Utc::now().checked_add_signed(delay))
                    .or_else(|| {
                        DateTime::parse_from_rfc2822(value)
                            .ok()
                            .map(|value| value.with_timezone(&Utc))
                    })
            })
            .unwrap_or_else(|| Utc::now() + Duration::minutes(5));
        return Err(ProviderRateLimited { retry_at }.into());
    }
    ensure!(
        status.is_success(),
        "COROS returned HTTP {}",
        status.as_u16()
    );
    Ok(response)
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

fn list(value: &Value) -> Result<&[Value]> {
    value
        .as_array()
        .map(Vec::as_slice)
        .context("invalid COROS list")
}

fn timestamp(value: &Value) -> Result<DateTime<Utc>> {
    let seconds = value.as_f64().context("missing COROS timestamp")?;
    ensure!(
        seconds.is_finite() && seconds > 0.0,
        "invalid COROS timestamp"
    );
    DateTime::from_timestamp_millis((seconds * 1000.0).round() as i64)
        .context("invalid COROS timestamp")
}

fn integer(value: &Value) -> Result<u64> {
    value
        .as_u64()
        .or_else(|| value.as_str()?.parse().ok())
        .context("invalid COROS integer")
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
    at: DateTime<Utc>,
    window: &Window,
) -> Option<Value> {
    if !window.contains(at, at) {
        return None;
    }
    let id = item["id"]
        .as_str()
        .map(str::to_owned)
        .or_else(|| item["id"].as_i64().map(|id| id.to_string()))
        .unwrap_or_else(|| at.to_rfc3339());
    Some(record(
        "metric",
        source,
        &format!("{name}:{id}"),
        at,
        at,
        window.end,
        json!({"metric":name,"provider_data":item}),
    ))
}

fn append_day(rows: &mut Vec<Value>, item: &Value, window: &Window) -> Result<()> {
    let day =
        chrono::NaiveDate::parse_from_str(&integer(&item["happenDay"])?.to_string(), "%Y%m%d")?;
    let at = day.and_hms_opt(0, 0, 0).unwrap().and_utc();
    if window.contains(at, at) {
        if let Some(row) = metric(item, "daily_summary", "COROS", at, window) {
            rows.push(row);
        }
    }
    for (field, name) in [
        ("rhr", "resting_heart_rate"),
        ("sleepAvgHr", "sleep_average_heart_rate"),
    ] {
        if let Some(value) = item[field]
            .as_u64()
            .filter(|value| (1..=300).contains(value))
        {
            if let Some(row) = metric(
                &json!({"date":day,"value":value,"unit":"bpm"}),
                name,
                "COROS",
                at,
                window,
            ) {
                rows.push(row);
            }
        }
    }
    rows.extend(sleep(&item["mainSleep"], "main", window));
    if let Some(naps) = item["napList"].as_array() {
        for nap in naps {
            rows.extend(sleep(nap, "nap", window));
        }
    }
    if let Some(samples) = item["hrvList"].as_array() {
        for sample in samples {
            if let Ok(at) = timestamp(&sample["timestamp"]) {
                if let Some(row) = metric(sample, "hrv_sample", "COROS", at, window) {
                    rows.push(row);
                }
            }
        }
    }
    Ok(())
}

fn sleep(item: &Value, kind: &str, window: &Window) -> Option<Value> {
    let source_stages = item["stages"].as_array()?;
    if !(1..=500).contains(&source_stages.len()) {
        return None;
    }
    let started = timestamp(&source_stages.first()?["startTimestamp"]).ok()?;
    let mut ended = started;
    let mut stages = Vec::with_capacity(source_stages.len());
    for source in source_stages {
        let start = timestamp(&source["startTimestamp"]).ok()?;
        let duration = Duration::try_seconds(source["duration"].as_i64()?)?;
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
        json!({"stages":stages,"interval_basis":"recorded_stages","sleep_kind":kind,"provider_summary":summary}),
    ))
}

fn workout(item: &Value, window: &Window) -> Result<Option<Value>> {
    let started = timestamp(&item["startTime"])?;
    let ended = timestamp(&item["endTime"])?;
    if started >= ended || !window.contains(started, ended) {
        return Ok(None);
    }
    let id = item["labelId"]
        .as_str()
        .filter(|id| !id.is_empty() && id.len() <= 256)
        .map(str::to_owned)
        .or_else(|| item["labelId"].as_u64().map(|id| id.to_string()))
        .context("missing COROS workout ID")?;
    let details = |item: &Value| {
        [
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
        ]
        .into_iter()
        .filter_map(|(field, name)| {
            item.get(field)
                .filter(|v| v.is_number())
                .map(|v| (name.to_owned(), v.clone()))
        })
        .collect::<serde_json::Map<_, _>>()
    };
    let mut provider_data = Value::Object(details(item));
    if let Some(legs) = item["triathlonItemList"].as_array() {
        provider_data["multisport_legs"] =
            Value::Array(legs.iter().map(|leg| Value::Object(details(leg))).collect());
    }
    Ok(Some(record(
        "workout",
        item["deviceName"].as_str().unwrap_or("COROS"),
        &id,
        started,
        ended,
        window.end,
        json!({"exercise_type":format!("coros:{}:{}",integer(&item["mode"] )?,integer(&item["subMode"])?),"distance_meters":item["distance"],"workout_steps":item["step"],"provider_data":provider_data}),
    )))
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{
        body::Body,
        extract::Request,
        http::{Response as HttpResponse, StatusCode},
        routing::any,
        Router,
    };

    #[test]
    fn normalizes_workouts_and_sleep_without_carrying_signed_urls() -> Result<()> {
        let start = DateTime::parse_from_rfc3339("2026-11-27T08:00:00Z")?.with_timezone(&Utc);
        let window = Window {
            start: start - Duration::hours(1),
            end: start + Duration::days(1),
        };
        let row = workout(&json!({"labelId":"fit-123","mode":8,"subMode":1,"startTime":start.timestamp(),"endTime":(start+Duration::minutes(30)).timestamp(),"avgSpeed":163,"fitUrl":"https://private.example/signed"}), &window)?.unwrap();
        assert_eq!(row["payload"]["exercise_type"], "coros:8:1");
        assert_eq!(
            row["payload"]["provider_data"]["average_pace_seconds_per_km"],
            163
        );
        assert!(!row.to_string().contains("https://"));
        let sleep = sleep(
            &json!({"stages":[{"startTimestamp":start.timestamp(),"duration":300,"type":2}]}),
            "main",
            &window,
        )
        .unwrap();
        assert_eq!(sleep["payload"]["stages"][0]["stage"], 5);
        Ok(())
    }

    #[tokio::test]
    async fn collection_uses_coros_credentials_and_returns_normalized_snapshot() -> Result<()> {
        let workout_start: DateTime<Utc> = "2026-11-27T08:00:00Z".parse()?;
        let workout = json!({"labelId":"activity-9","mode":8,"subMode":1,
            "startTime":workout_start.timestamp(),"endTime":(workout_start+Duration::minutes(30)).timestamp(),
            "distance":1500,"fitUrl":"https://cdn.example.test/private.fit?signature=secret"});
        let router = Router::new().route(
            "/{*path}",
            any(move |request: Request| {
                let workout = workout.clone();
                async move {
                    let query = request.uri().query().unwrap_or_default();
                    if !query.contains("token=coros-test-token")
                        || !query.contains("openId=account-7")
                    {
                        return HttpResponse::builder()
                            .status(StatusCode::UNAUTHORIZED)
                            .body(Body::empty())
                            .unwrap();
                    }
                    let payload = if request.uri().path() == "/v2/coros/sport/list" {
                        json!({"result":"0000","data":[workout]})
                    } else if request.uri().path() == "/coros/daily/query" {
                        json!({"result":"0000","data":{"dailyList":[]}})
                    } else {
                        return HttpResponse::builder()
                            .status(StatusCode::NOT_FOUND)
                            .body(Body::empty())
                            .unwrap();
                    };
                    HttpResponse::builder()
                        .status(StatusCode::OK)
                        .header("content-type", "application/json")
                        .body(Body::from(serde_json::to_vec(&payload).unwrap()))
                        .unwrap()
                }
            }),
        );
        let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0)).await?;
        let api = format!("http://{}", listener.local_addr()?);
        let server = tokio::spawn(async move { axum::serve(listener, router).await });
        let snapshot = collect_from_origin(
            &Client::new(),
            "coros-test-token",
            "account-7",
            "lease-7",
            workout_start - Duration::hours(1),
            workout_start + Duration::hours(2),
            &api,
        )
        .await?;
        let snapshot = serde_json::to_value(snapshot)?;
        assert_eq!(snapshot["provider"], "coros");
        assert_eq!(snapshot["records"][0]["source_id"], "activity-9");
        assert_eq!(snapshot["records"][0]["payload"]["distance_meters"], 1500);
        assert!(!snapshot.to_string().contains("signature=secret"));
        server.abort();
        Ok(())
    }
}
