use anyhow::{bail, ensure, Context, Result};
use avatar_hsm::etl_adapter::Snapshot;
use chrono::{DateTime, Duration, Utc};
use reqwest::{Client, Response};
use serde_json::{json, Value};

const API: &str = "https://api.hevyapp.com/v1/workouts";
const PAGE_SIZE: u16 = 10;
const MAX_PAGES: u16 = 50;
const MAX_RESPONSE_BYTES: usize = 8 * 1024 * 1024;

#[derive(Debug)]
pub struct ProviderRateLimited {
    pub retry_at: DateTime<Utc>,
}

impl std::fmt::Display for ProviderRateLimited {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("Hevy returned HTTP 429")
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
    api_key: &str,
    lease: &str,
    start: DateTime<Utc>,
    end: DateTime<Utc>,
) -> Result<Snapshot> {
    collect_from_endpoint(client, api_key, lease, start, end, API).await
}

async fn collect_from_endpoint(
    client: &Client,
    api_key: &str,
    lease: &str,
    start: DateTime<Utc>,
    end: DateTime<Utc>,
    endpoint: &str,
) -> Result<Snapshot> {
    ensure!(
        !api_key.trim().is_empty() && !lease.is_empty(),
        "Hevy credential and HSM lease are required"
    );
    ensure!(
        start < end && end - start <= Duration::days(7),
        "invalid Hevy collection window"
    );
    let window = Window { start, end };
    let mut output = Vec::new();
    for page in 1..=MAX_PAGES {
        let response = client
            .get(endpoint)
            .header("api-key", api_key)
            .query(&[("page", page), ("pageSize", PAGE_SIZE)])
            .send()
            .await?;
        let response = check_response(response)?;
        let body = read_json(response).await?;
        let workouts = body["workouts"]
            .as_array()
            .context("Hevy response lacks workouts")?;
        for item in workouts {
            if let Some(record) = normalize_workout(item, &window)? {
                output.push(record);
            }
        }
        let page_count = body["page_count"]
            .as_u64()
            .or_else(|| body["page_count"].as_str()?.parse().ok())
            .context("Hevy response lacks page count")?;
        if u64::from(page) >= page_count {
            return serde_json::from_value(json!({
                "provider":"hevy",
                "lease":lease,
                "start":start,
                "end":end,
                "grants":["workout"],
                "records":output
            }))
            .context("failed to build normalized Hevy snapshot");
        }
    }
    bail!("Hevy pagination exceeded the limit")
}

struct Window {
    start: DateTime<Utc>,
    end: DateTime<Utc>,
}

fn normalize_workout(item: &Value, window: &Window) -> Result<Option<Value>> {
    let start = required_text(&item["start_time"])?.parse::<DateTime<Utc>>()?;
    let end = required_text(&item["end_time"])?.parse::<DateTime<Utc>>()?;
    if start < window.start
        || start >= window.end
        || end < start
        || end > window.end
        || end == start
    {
        return Ok(None);
    }
    let exercises = item["exercises"]
        .as_array()
        .context("Hevy workout lacks exercises")?;
    let mut movements = Vec::with_capacity(exercises.len());
    for exercise in exercises {
        let sets = exercise["sets"]
            .as_array()
            .context("Hevy exercise lacks sets")?
            .iter()
            .map(|set| {
                json!({
                    "type":set["type"],
                    "reps":set["reps"],
                    "weight_kg":set["weight_kg"],
                    "duration_seconds":set["duration_seconds"],
                    "distance_meters":set["distance_meters"],
                    "rpe":set["rpe"]
                })
            })
            .collect::<Vec<_>>();
        movements.push(json!({
            "exercise":required_text(&exercise["title"])?,
            "sets":sets
        }));
    }
    let modified_at = item["updated_at"]
        .as_str()
        .map(str::parse::<DateTime<Utc>>)
        .transpose()?
        .unwrap_or(window.end);
    Ok(Some(json!({
        "kind":"workout",
        "source":"Hevy",
        "source_id":required_text(&item["id"])?,
        "started_at":start,
        "ended_at":end,
        "modified_at":modified_at,
        "payload":{
            "exercise_type":"strength",
            "title":item["title"],
            "exercises":movements
        }
    })))
}

fn required_text(value: &Value) -> Result<&str> {
    value
        .as_str()
        .filter(|text| !text.is_empty())
        .context("required Hevy text field is missing")
}

async fn read_json(response: Response) -> Result<Value> {
    let mut response = response;
    let mut bytes = Vec::new();
    while let Some(chunk) = response.chunk().await? {
        ensure!(
            bytes.len() + chunk.len() <= MAX_RESPONSE_BYTES,
            "Hevy response exceeds size limit"
        );
        bytes.extend_from_slice(&chunk);
    }
    Ok(serde_json::from_slice(&bytes).context("invalid Hevy JSON response")?)
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
        "Hevy returned HTTP {}",
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
        json!({
            "id":"workout-123",
            "title":"Strength A",
            "start_time":"2026-11-27T07:00:00Z",
            "end_time":"2026-11-27T07:45:00Z",
            "updated_at":"2026-11-27T08:00:00Z",
            "exercises":[{"title":"Squat","sets":[{"type":"normal","reps":5,"weight_kg":80.0}]}]
        })
    }

    #[test]
    fn normalizes_hevy_workout_into_the_hsm_snapshot_contract() -> Result<()> {
        let start = "2026-11-27T00:00:00Z".parse()?;
        let end = "2026-11-28T00:00:00Z".parse()?;
        let record = normalize_workout(&sample(), &Window { start, end })?
            .context("workout should be inside the window")?;
        let snapshot: Snapshot = serde_json::from_value(json!({
            "provider":"hevy",
            "lease":"opaque-lease",
            "start":start,
            "end":end,
            "grants":["workout"],
            "records":[record]
        }))?;
        assert_eq!(snapshot.records.len(), 1);
        Ok(())
    }

    #[test]
    fn excludes_workouts_outside_the_requested_window() -> Result<()> {
        let start = "2026-11-28T00:00:00Z".parse()?;
        let end = "2026-11-29T00:00:00Z".parse()?;
        assert!(normalize_workout(&sample(), &Window { start, end })?.is_none());
        Ok(())
    }

    async fn workouts(
        State(body): State<Value>,
        headers: HeaderMap,
        Query(query): Query<HashMap<String, String>>,
    ) -> impl IntoResponse {
        if headers.get("api-key").and_then(|value| value.to_str().ok()) != Some("test-key")
            || query.get("pageSize").map(String::as_str) != Some("10")
        {
            return (
                StatusCode::UNAUTHORIZED,
                Json(json!({"error":"bad request"})),
            );
        }
        (StatusCode::OK, Json(body))
    }

    #[tokio::test]
    async fn fetches_provider_page_and_returns_normalized_snapshot() -> Result<()> {
        let body = json!({"page_count":1,"workouts":[sample()]});
        let app = Router::new()
            .route("/v1/workouts", get(workouts))
            .with_state(body);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let server = tokio::spawn(async move { axum::serve(listener, app).await });
        let start = "2026-11-27T00:00:00Z".parse()?;
        let end = "2026-11-28T00:00:00Z".parse()?;
        let result = collect_from_endpoint(
            &Client::new(),
            "test-key",
            "opaque-lease",
            start,
            end,
            &format!("http://{address}/v1/workouts"),
        )
        .await;
        server.abort();
        let snapshot = serde_json::to_value(result?)?;
        assert_eq!(snapshot["provider"], "hevy");
        assert_eq!(snapshot["records"].as_array().map(Vec::len), Some(1));
        assert_eq!(snapshot["records"][0]["source_id"], "workout-123");
        Ok(())
    }
}
