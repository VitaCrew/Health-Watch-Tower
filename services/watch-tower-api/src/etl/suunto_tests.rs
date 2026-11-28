use super::*;
use axum::{
    body::Body,
    extract::Request,
    http::{Response as HttpResponse, StatusCode},
    routing::any,
    Router,
};

#[test]
fn normalizes_workout_activity_samples_and_rate_limit() -> Result<()> {
    let start: DateTime<Utc> = "2026-11-21T08:00:00Z".parse()?;
    let window = Window {
        start,
        end: start + Duration::days(1),
    };
    let workout = suunto_point(
        &json!({"workoutKey":"workout-1","startTime":start.timestamp_millis(),
            "stopTime":(start+Duration::minutes(45)).timestamp_millis(),"activityId":1,"totalDistance":1200}),
        &window,
    )?
    .context("expected an in-window workout")?;
    assert_eq!(workout["source_id"], "workout-1");
    assert_eq!(workout["payload"]["duration_seconds"], 2700);
    let samples = suunto_series(
        &json!({"payload":[{"timestamp":start.to_rfc3339(),"entryData":{"SleepId":"s-1","HR":74,"StepCount":32}}]}),
        "activity_samples",
        &window,
    )?;
    assert!(samples
        .iter()
        .any(|row| row["kind"] == "heart_rate" && row["payload"]["samples"][0]["bpm"] == 74));
    assert!(samples
        .iter()
        .any(|row| row["payload"]["metric"] == "activity_step_sample"
            && row["payload"]["provider_data"]["value"] == 32.0));
    let now: DateTime<Utc> = "2026-11-27T12:00:00Z".parse()?;
    assert_eq!(retry_after(Some("300"), now), now + Duration::minutes(5));
    assert_eq!(
        retry_after(Some("Fri, 01 Jan 2027 13:00:00 GMT"), now),
        "2027-01-01T13:00:00Z".parse::<DateTime<Utc>>()?
    );
    Ok(())
}

#[test]
fn normalizes_suunto_fit_detail_into_personal_workout_record() -> Result<()> {
    let start: DateTime<Utc> = "2026-11-21T08:00:00Z".parse()?;
    let response = json!({"payload":{"workoutKey":"workout-1",
        "startTime":start.timestamp_millis(),
        "stopTime":(start+Duration::minutes(45)).timestamp_millis(),
        "activityId":1,"totalDistance":1200}});
    let record = normalize_workout_detail(&response, "workout-1")?;
    assert_eq!(record["kind"], "workout");
    assert_eq!(record["source_id"], "workout-1");
    assert_eq!(record["payload"]["duration_seconds"], 2700);
    assert!(normalize_workout_detail(&response, "another-workout").is_err());
    Ok(())
}

#[tokio::test]
async fn collection_uses_hsm_credential_pair_and_returns_snapshot() -> Result<()> {
    let start: DateTime<Utc> = "2026-11-21T08:00:00Z".parse()?;
    let end = start + Duration::hours(12);
    let workout = json!({"workoutKey":"mock-workout","startTime":start.timestamp_millis(),
        "stopTime":(start+Duration::minutes(30)).timestamp_millis(),"activityId":3,"totalDistance":800});
    let router = Router::new().route(
        "/{*path}",
        any(move |request: Request| {
            let workout = workout.clone();
            async move {
                let path = request.uri().path();
                if request.headers().get("authorization").and_then(|value| value.to_str().ok())
                    != Some("Bearer suunto-test-token")
                    || request.headers().get("ocp-apim-subscription-key").and_then(|value| value.to_str().ok())
                        != Some("suunto-test-subscription")
                {
                    return HttpResponse::builder().status(StatusCode::UNAUTHORIZED).body(Body::empty()).unwrap();
                }
                let body = if path == "/v3/workouts/" {
                    json!({"payload":[workout]})
                } else if path == "/247samples/activity" {
                    json!({"payload":[{"timestamp":"2026-11-21T09:00:00Z","entryData":{"HR":76,"StepCount":24}}]})
                } else if path == "/247/daily-activity-statistics" {
                    json!({"payload":[]})
                } else if path.starts_with("/247samples/") {
                    json!({"payload":[]})
                } else {
                    return HttpResponse::builder().status(StatusCode::NOT_FOUND).body(Body::empty()).unwrap();
                };
                HttpResponse::builder().status(StatusCode::OK).header("content-type","application/json")
                    .body(Body::from(serde_json::to_vec(&body).unwrap())).unwrap()
            }
        }),
    );
    let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0)).await?;
    let origin = format!("http://{}", listener.local_addr()?);
    let server = tokio::spawn(async move { axum::serve(listener, router).await });
    let snapshot = collect_from_origins(
        &Client::new(),
        "suunto-test-token",
        "suunto-test-subscription",
        "hsm-run-lease",
        start,
        end,
        &format!("{origin}/v3/workouts/"),
        &origin,
    )
    .await?;
    let snapshot = serde_json::to_value(snapshot)?;
    assert_eq!(snapshot["provider"], "suunto");
    assert!(snapshot["records"]
        .as_array()
        .is_some_and(|rows| rows.iter().any(|row| row["kind"] == "workout")));
    assert!(snapshot["records"]
        .as_array()
        .is_some_and(|rows| rows.iter().any(|row| row["kind"] == "heart_rate")));
    server.abort();
    Ok(())
}

#[tokio::test]
async fn rate_limit_http_date_is_exposed_for_queue_deferral() -> Result<()> {
    let router = Router::new().route(
        "/{*path}",
        any(|| async {
            HttpResponse::builder()
                .status(StatusCode::TOO_MANY_REQUESTS)
                .header("retry-after", "Fri, 01 Jan 2027 13:00:00 GMT")
                .body(Body::empty())
                .unwrap()
        }),
    );
    let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0)).await?;
    let origin = format!("http://{}", listener.local_addr()?);
    let server = tokio::spawn(async move { axum::serve(listener, router).await });
    let start: DateTime<Utc> = "2026-11-21T08:00:00Z".parse()?;
    let error = collect_from_origins(
        &Client::new(),
        "suunto-test-token",
        "suunto-test-subscription",
        "hsm-run-lease",
        start,
        start + Duration::hours(12),
        &format!("{origin}/v3/workouts/"),
        &origin,
    )
    .await
    .err()
    .context("Suunto rate limit should defer this run")?;
    assert_eq!(
        provider_retry_at(&error),
        Some("2027-01-01T13:00:00Z".parse()?)
    );
    server.abort();
    Ok(())
}
