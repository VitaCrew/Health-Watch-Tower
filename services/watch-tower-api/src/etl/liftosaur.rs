use anyhow::{bail, ensure, Context, Result};
use avatar_hsm::etl_adapter::Snapshot;
use chrono::{DateTime, Duration, Utc};
use reqwest::{Client, Response};
use serde_json::{json, Value};
use std::collections::HashSet;

const API: &str = "https://api3.liftosaur.com/api/v1/history";
const MAX_PAGES: usize = 50;
const MAX_RESPONSE_BYTES: usize = 8 * 1024 * 1024;
const MAX_WORKOUT_TEXT: usize = 100_000;

#[derive(Debug)]
pub struct ProviderRateLimited {
    pub retry_at: DateTime<Utc>,
}

impl std::fmt::Display for ProviderRateLimited {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("Liftosaur returned HTTP 429")
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
    lease: &str,
    start: DateTime<Utc>,
    end: DateTime<Utc>,
) -> Result<Snapshot> {
    collect_from_endpoint(client, token, lease, start, end, API).await
}

async fn collect_from_endpoint(
    client: &Client,
    token: &str,
    lease: &str,
    start: DateTime<Utc>,
    end: DateTime<Utc>,
    endpoint: &str,
) -> Result<Snapshot> {
    ensure!(
        !token.trim().is_empty() && !lease.is_empty(),
        "Liftosaur credential and HSM lease are required"
    );
    ensure!(
        start < end && end - start <= Duration::days(7),
        "invalid Liftosaur collection window"
    );
    let window = Window { start, end };
    let mut cursor = None;
    let mut seen = HashSet::new();
    let mut output = Vec::new();
    for _ in 0..MAX_PAGES {
        let mut request = client.get(endpoint).bearer_auth(token).query(&[
            ("limit", "50"),
            ("startDate", &start.to_rfc3339()),
            ("endDate", &end.to_rfc3339()),
        ]);
        if let Some(cursor) = cursor {
            request = request.query(&[("cursor", cursor)]);
        }
        let response = check_response(request.send().await?)?;
        let body = read_json(response).await?;
        let data = &body["data"];
        let records = data["records"]
            .as_array()
            .context("Liftosaur response lacks records")?;
        for item in records {
            if let Some(record) = normalize_workout(item, &window)? {
                output.push(record);
            }
        }
        let has_more = data["hasMore"]
            .as_bool()
            .context("Liftosaur response lacks pagination status")?;
        if !has_more {
            return serde_json::from_value(json!({
                "provider":"liftosaur",
                "lease":lease,
                "start":start,
                "end":end,
                "grants":["workout"],
                "records":output
            }))
            .context("failed to build normalized Liftosaur snapshot");
        }
        let next = data["nextCursor"]
            .as_u64()
            .or_else(|| data["nextCursor"].as_str()?.parse().ok())
            .context("Liftosaur response lacks next cursor")?;
        ensure!(seen.insert(next), "Liftosaur pagination cursor repeated");
        cursor = Some(next);
    }
    bail!("Liftosaur pagination exceeded the limit")
}

struct Window {
    start: DateTime<Utc>,
    end: DateTime<Utc>,
}

fn normalize_workout(item: &Value, window: &Window) -> Result<Option<Value>> {
    let script = required_text(&item["text"])?;
    ensure!(
        script.len() <= MAX_WORKOUT_TEXT,
        "Liftosaur workout exceeds size limit"
    );
    let header = script.lines().next().context("empty Liftosaur history")?;
    let mut fields = header.split(" / ");
    let start = fields
        .next()
        .context("missing Liftosaur workout start")?
        .trim()
        .parse::<DateTime<Utc>>()?;
    let duration = fields
        .find_map(|field| field.strip_prefix("duration: "))
        .map(|field| {
            field
                .trim()
                .strip_suffix('s')
                .unwrap_or(field.trim())
                .parse::<i64>()
        })
        .transpose()?
        .unwrap_or(0);
    ensure!(
        (0..=7 * 86_400).contains(&duration),
        "unsupported workout duration"
    );
    let end = start
        .checked_add_signed(Duration::seconds(duration))
        .context("invalid Liftosaur workout end")?;
    if start < window.start || start >= window.end || end > window.end {
        return Ok(None);
    }
    let id = item["id"]
        .as_u64()
        .or_else(|| item["id"].as_str()?.parse().ok())
        .context("invalid Liftosaur workout ID")?;
    Ok(Some(json!({
        "kind":"workout",
        "source":"Liftosaur",
        "source_id":id.to_string(),
        "started_at":start,
        "ended_at":end,
        "modified_at":window.end,
        "payload":{
            "exercise_type":"strength",
            "format":"liftoscript_workout",
            "workout_text":script,
            "exercises":simple_sets(script),
            "duration_unknown":duration == 0
        }
    })))
}

fn simple_sets(script: &str) -> Vec<Value> {
    let Some((_, block)) = script.split_once("exercises: {") else {
        return Vec::new();
    };
    block
        .lines()
        .filter_map(|line| {
            let (exercise, detail) = line.trim().split_once(" / ")?;
            let recorded = detail.split(" / ").next()?;
            let sets = recorded
                .split(',')
                .map(|set| {
                    let (count_rep, load) = set.trim().split_once(' ')?;
                    let (count, reps) = count_rep.split_once('x')?;
                    let count: u16 = count.parse().ok()?;
                    let reps: u16 = reps.parse().ok()?;
                    let (amount, unit, conversion) = if let Some(weight) = load.strip_suffix("kg") {
                        (weight, "kg", 1.0)
                    } else {
                        (load.strip_suffix("lb")?, "lb", 0.45359237)
                    };
                    let source_weight: f64 = amount
                        .parse::<f64>()
                        .ok()
                        .filter(|value| (0.0..=9999.0).contains(value))?;
                    if count == 0 || reps == 0 {
                        return None;
                    }
                    Some(json!({
                        "set_count":count,
                        "reps":reps,
                        "weight":source_weight,
                        "weight_unit":unit,
                        "weight_kg":source_weight*conversion
                    }))
                })
                .collect::<Option<Vec<_>>>()?;
            Some(json!({"exercise":exercise,"sets":sets}))
        })
        .collect()
}

fn required_text(value: &Value) -> Result<&str> {
    value
        .as_str()
        .filter(|text| !text.is_empty())
        .context("required Liftosaur text field is missing")
}

async fn read_json(response: Response) -> Result<Value> {
    let mut response = response;
    let mut bytes = Vec::new();
    while let Some(chunk) = response.chunk().await? {
        ensure!(
            bytes.len() + chunk.len() <= MAX_RESPONSE_BYTES,
            "Liftosaur response exceeds size limit"
        );
        bytes.extend_from_slice(&chunk);
    }
    Ok(serde_json::from_slice(&bytes).context("invalid Liftosaur JSON response")?)
}

fn check_response(response: Response) -> Result<Response> {
    let status = response.status();
    if status == reqwest::StatusCode::TOO_MANY_REQUESTS {
        let retry_at = response
            .headers()
            .get(reqwest::header::RETRY_AFTER)
            .and_then(|header| header.to_str().ok())
            .and_then(|value| {
                value
                    .parse::<i64>()
                    .ok()
                    .and_then(Duration::try_seconds)
                    .and_then(|delay| Utc::now().checked_add_signed(delay))
                    .or_else(|| {
                        DateTime::parse_from_rfc2822(value)
                            .ok()
                            .map(|date| date.with_timezone(&Utc))
                    })
            })
            .unwrap_or_else(|| Utc::now() + Duration::minutes(5));
        return Err(ProviderRateLimited { retry_at }.into());
    }
    ensure!(
        status.is_success(),
        "Liftosaur returned HTTP {}",
        status.as_u16()
    );
    Ok(response)
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{
        extract::{Query, State},
        http::{HeaderMap, StatusCode},
        response::IntoResponse,
        routing::get,
        Json, Router,
    };
    use std::collections::HashMap;

    fn sample() -> Value {
        json!({"id":42,"text":"2026-11-27T07:05:00Z / duration: 1800s / exercises: {\nSquat / 3x5 185lb\n}"})
    }

    #[test]
    fn normalizes_liftoscript_and_converts_pounds() -> Result<()> {
        let start = "2026-11-27T00:00:00Z".parse()?;
        let end = "2026-11-28T00:00:00Z".parse()?;
        let record = normalize_workout(&sample(), &Window { start, end })?
            .context("workout should be inside the window")?;
        assert_eq!(record["payload"]["exercises"][0]["sets"][0]["reps"], 5);
        assert_eq!(
            record["payload"]["exercises"][0]["sets"][0]["weight_unit"],
            "lb"
        );
        let weight_kg = record["payload"]["exercises"][0]["sets"][0]["weight_kg"]
            .as_f64()
            .context("normalized weight is not numeric")?;
        assert!((weight_kg - 83.91458845).abs() < 1e-8);
        let snapshot: Snapshot = serde_json::from_value(json!({
            "provider":"liftosaur","lease":"opaque","start":start,"end":end,
            "grants":["workout"],"records":[record]
        }))?;
        assert_eq!(
            serde_json::to_value(snapshot)?["records"][0]["source_id"],
            "42"
        );
        Ok(())
    }

    async fn history(
        State(body): State<Value>,
        headers: HeaderMap,
        Query(query): Query<HashMap<String, String>>,
    ) -> impl IntoResponse {
        if headers
            .get("authorization")
            .and_then(|value| value.to_str().ok())
            != Some("Bearer test-token")
            || query.get("limit").map(String::as_str) != Some("50")
        {
            return (
                StatusCode::UNAUTHORIZED,
                Json(json!({"error":"bad request"})),
            );
        }
        (StatusCode::OK, Json(body))
    }

    #[tokio::test]
    async fn fetches_liftosaur_history_and_builds_snapshot() -> Result<()> {
        let body = json!({"data":{"records":[sample()],"hasMore":false}});
        let app = Router::new()
            .route("/api/v1/history", get(history))
            .with_state(body);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let server = tokio::spawn(async move { axum::serve(listener, app).await });
        let start = "2026-11-27T00:00:00Z".parse()?;
        let end = "2026-11-28T00:00:00Z".parse()?;
        let result = collect_from_endpoint(
            &Client::new(),
            "test-token",
            "opaque-lease",
            start,
            end,
            &format!("http://{address}/api/v1/history"),
        )
        .await;
        server.abort();
        let value = serde_json::to_value(result?)?;
        assert_eq!(value["provider"], "liftosaur");
        assert_eq!(value["records"].as_array().map(Vec::len), Some(1));
        Ok(())
    }
}
