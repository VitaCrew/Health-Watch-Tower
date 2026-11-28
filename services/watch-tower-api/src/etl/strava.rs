use anyhow::{bail, ensure, Context, Result};
use avatar_hsm::etl_adapter::Snapshot;
use chrono::{DateTime, Duration, Utc};
use reqwest::{Client, Response};
use serde_json::{json, Value};

const API: &str = "https://www.strava.com/api/v3/athlete/activities";
const PAGE_SIZE: u16 = 100;
const MAX_PAGES: u16 = 40;
const MAX_RESPONSE_BYTES: usize = 8 * 1024 * 1024;

#[derive(Debug)]
pub struct ProviderRateLimited {
    pub retry_at: DateTime<Utc>,
}

impl std::fmt::Display for ProviderRateLimited {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("Strava returned HTTP 429")
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
        "Strava credential and HSM lease are required"
    );
    ensure!(
        start < end && end - start <= Duration::days(7),
        "invalid Strava collection window"
    );
    let window = Window { start, end };
    let observed_at = Utc::now();
    let mut output = Vec::new();
    for page in 1..=MAX_PAGES {
        let response = client
            .get(endpoint)
            .bearer_auth(token)
            .query(&[
                ("after", start.timestamp()),
                ("before", end.timestamp()),
                ("per_page", i64::from(PAGE_SIZE)),
                ("page", i64::from(page)),
            ])
            .send()
            .await?;
        let response = check_response(response)?;
        let body = read_json(response).await?;
        let activities = body
            .as_array()
            .context("Strava response lacks activity list")?;
        for activity in activities {
            if let Some(record) = normalize_activity(activity, &window, observed_at)? {
                output.push(record);
            }
        }
        if activities.len() < usize::from(PAGE_SIZE) {
            return serde_json::from_value(json!({
                "provider":"strava",
                "lease":lease,
                "start":start,
                "end":end,
                "grants":["workout"],
                "records":output
            }))
            .context("failed to build normalized Strava snapshot");
        }
    }
    bail!("Strava pagination exceeded the limit")
}

struct Window {
    start: DateTime<Utc>,
    end: DateTime<Utc>,
}

fn normalize_activity(
    item: &Value,
    window: &Window,
    observed_at: DateTime<Utc>,
) -> Result<Option<Value>> {
    let start = required_text(&item["start_date"])?.parse::<DateTime<Utc>>()?;
    let elapsed = item["elapsed_time"]
        .as_i64()
        .context("Strava duration is missing")?;
    ensure!(
        (0..=7 * 86_400).contains(&elapsed),
        "unsupported Strava duration"
    );
    let end = start
        .checked_add_signed(Duration::seconds(elapsed))
        .context("invalid Strava workout end")?;
    if start < window.start || start >= window.end || end > window.end || start == end {
        return Ok(None);
    }
    let id = item["id"]
        .as_u64()
        .or_else(|| item["id"].as_str()?.parse().ok())
        .context("invalid Strava activity ID")?;
    Ok(Some(json!({
        "kind":"workout",
        "source":"Strava",
        "source_id":id.to_string(),
        "started_at":start,
        "ended_at":end,
        "modified_at":observed_at,
        "payload":{
            "exercise_type":required_text(&item["sport_type"])?,
            "title":item["name"],
            "distance_meters":item["distance"]
        }
    })))
}

fn required_text(value: &Value) -> Result<&str> {
    value
        .as_str()
        .filter(|text| !text.is_empty())
        .context("required Strava text field is missing")
}

async fn read_json(response: Response) -> Result<Value> {
    let mut response = response;
    let mut bytes = Vec::new();
    while let Some(chunk) = response.chunk().await? {
        ensure!(
            bytes.len() + chunk.len() <= MAX_RESPONSE_BYTES,
            "Strava response exceeds size limit"
        );
        bytes.extend_from_slice(&chunk);
    }
    Ok(serde_json::from_slice(&bytes).context("invalid Strava JSON response")?)
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
        "Strava returned HTTP {}",
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
            "id":12345,
            "start_date":"2026-11-27T07:05:00Z",
            "elapsed_time":1800,
            "sport_type":"Ride",
            "name":"Morning Ride",
            "distance":12000.0
        })
    }

    #[test]
    fn normalizes_activity_into_hsm_snapshot_contract() -> Result<()> {
        let start = "2026-11-27T00:00:00Z".parse()?;
        let end = "2026-11-28T00:00:00Z".parse()?;
        let window = Window { start, end };
        let record = normalize_activity(&sample(), &window, end)?
            .context("activity should be inside the requested window")?;
        assert_eq!(record["payload"]["exercise_type"], "Ride");
        assert_eq!(record["payload"]["distance_meters"], 12000.0);
        let snapshot: Snapshot = serde_json::from_value(json!({
            "provider":"strava","lease":"opaque","start":start,"end":end,
            "grants":["workout"],"records":[record]
        }))?;
        assert_eq!(
            serde_json::to_value(snapshot)?["records"][0]["source_id"],
            "12345"
        );
        Ok(())
    }

    async fn activities(
        State(body): State<Value>,
        headers: HeaderMap,
        Query(query): Query<HashMap<String, String>>,
    ) -> impl IntoResponse {
        if headers
            .get("authorization")
            .and_then(|value| value.to_str().ok())
            != Some("Bearer test-token")
            || query.get("per_page").map(String::as_str) != Some("100")
        {
            return (
                StatusCode::UNAUTHORIZED,
                Json(json!({"error":"bad request"})),
            );
        }
        (StatusCode::OK, Json(body))
    }

    #[tokio::test]
    async fn fetches_strava_page_and_returns_normalized_snapshot() -> Result<()> {
        let app = Router::new()
            .route("/athlete/activities", get(activities))
            .with_state(json!([sample()]));
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
            &format!("http://{address}/athlete/activities"),
        )
        .await;
        server.abort();
        let snapshot = serde_json::to_value(result?)?;
        assert_eq!(snapshot["provider"], "strava");
        assert_eq!(snapshot["records"].as_array().map(Vec::len), Some(1));
        Ok(())
    }
}
