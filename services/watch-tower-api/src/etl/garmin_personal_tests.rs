use super::*;
use axum::{
    body::Body,
    extract::Request,
    http::{Response as HttpResponse, StatusCode},
    routing::{any, get},
    Router,
};

#[test]
fn projects_garmin_personal_sleep_activity_and_heart_samples() -> Result<()> {
    let window = Window {
        start: "2026-11-21T00:00:00Z".parse()?,
        end: "2026-11-22T00:00:00Z".parse()?,
    };
    let sleep = sleep_point(
        &json!({"id":7,"sleepStartTimestampGMT":"2026-11-21T00:30:00.0",
            "sleepEndTimestampGMT":"2026-11-21T08:30:00.0","deepSleepSeconds":900}),
        &window,
    )?
    .unwrap();
    assert_eq!(sleep["started_at"], "2026-11-21T00:30:00Z");
    assert_eq!(
        sleep["payload"]["provider_stage_summary"]["deep_seconds"],
        900
    );
    let workout = workout_point(
        &json!({"activityId":4,"startTimeGMT":"2026-11-21T09:00:00.0",
            "duration":60.25,"activityType":{"typeKey":"running"}}),
        &window,
    )?
    .unwrap();
    assert_eq!(workout["ended_at"], "2026-11-21T09:01:00.250Z");
    let at = window.start + CalendarDuration::hours(14);
    let mut hours = HeartHours::new();
    heart_points(
        &json!({"heartRateValues":[[at.timestamp_millis(),84],
            [(at + CalendarDuration::seconds(1)).timestamp_millis(),null]]}),
        &window,
        &mut hours,
    )?;
    assert_eq!(hours.values().next().unwrap()[&at], 84);
    Ok(())
}

#[tokio::test]
async fn collection_uses_hsm_key_once_and_builds_the_hsm_snapshot() -> Result<()> {
    let router = Router::new().route(
        "/{*path}",
        any(|request: Request| async move {
            let path = request.uri().path().to_owned();
            let auth = request
                .headers()
                .get("authorization")
                .and_then(|v| v.to_str().ok());
            if path.ends_with("/personal-api-key/token") {
                if auth != Some("ApiKey fixture-key") {
                    return HttpResponse::builder()
                        .status(StatusCode::UNAUTHORIZED)
                        .body(Body::empty())
                        .unwrap();
                }
                return HttpResponse::builder()
                    .status(StatusCode::OK)
                    .body(Body::from("fixture-token"))
                    .unwrap();
            }
            if auth != Some("Bearer fixture-token") {
                return HttpResponse::builder()
                    .status(StatusCode::UNAUTHORIZED)
                    .body(Body::empty())
                    .unwrap();
            }
            let body = if path.ends_with("dailySleepSummariesByDate") {
                serde_json::to_vec(
                    &json!([{"id":7,"sleepStartTimestampGMT":"2026-11-21T00:30:00.0",
                "sleepEndTimestampGMT":"2026-11-21T08:30:00.0","deepSleepSeconds":900}]),
                )
                .unwrap()
            } else if path.contains("/stats/stress/daily/") {
                serde_json::to_vec(&json!([{"calendarDate":"2026-11-21","overallStressLevel":22}]))
                    .unwrap()
            } else if path.contains("/hrv/daily/") {
                serde_json::to_vec(
                    &json!({"hrvSummaries":[{"calendarDate":"2026-11-21","lastNightAvg":44}]}),
                )
                .unwrap()
            } else if path.ends_with("bodyBattery/reports/daily") {
                serde_json::to_vec(&json!([{"date":"2026-11-21","charged":30}])).unwrap()
            } else if path.contains("/stats/steps/daily/") {
                serde_json::to_vec(&json!([{"calendarDate":"2026-11-21","totalSteps":7000}]))
                    .unwrap()
            } else if path.contains("activities/search/activities") {
                serde_json::to_vec(
                    &json!([{"activityId":4,"startTimeGMT":"2026-11-21T09:00:00.0",
                "duration":60.25,"activityType":{"typeKey":"running"}}]),
                )
                .unwrap()
            } else if path.ends_with("dailyHeartRate") {
                let millis = "2026-11-21T14:00:00Z"
                    .parse::<DateTime<Utc>>()
                    .unwrap()
                    .timestamp_millis();
                serde_json::to_vec(
                    &json!({"calendarDate":"2026-11-21","heartRateValues":[[millis,84]]}),
                )
                .unwrap()
            } else if path.contains("menstrualcycle/dayview/") {
                return HttpResponse::builder()
                    .status(StatusCode::NO_CONTENT)
                    .body(Body::empty())
                    .unwrap();
            } else {
                return HttpResponse::builder()
                    .status(StatusCode::NOT_FOUND)
                    .body(Body::empty())
                    .unwrap();
            };
            HttpResponse::builder()
                .status(StatusCode::OK)
                .header("content-type", "application/json")
                .body(body.into())
                .unwrap()
        }),
    );
    let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0)).await?;
    let origin = format!("http://{}", listener.local_addr()?);
    let server = tokio::spawn(async move { axum::serve(listener, router).await });
    let client = Client::new();
    let snapshot = collect_from_origin(
        &client,
        "fixture-key",
        "fixture-hsm-lease",
        "2026-11-21T00:00:00Z".parse()?,
        "2026-11-22T00:00:00Z".parse()?,
        &origin,
    )
    .await?;
    let snapshot = serde_json::to_value(snapshot)?;
    assert_eq!(snapshot["provider"], "garmin_personal");
    assert_eq!(snapshot["lease"], "fixture-hsm-lease");
    assert!(snapshot["grants"]
        .as_array()
        .unwrap()
        .iter()
        .any(|grant| grant == "sleep"));
    assert!(snapshot["grants"]
        .as_array()
        .unwrap()
        .iter()
        .any(|grant| grant == "heart_rate"));
    assert!(snapshot["records"]
        .as_array()
        .unwrap()
        .iter()
        .any(|row| row["kind"] == "workout"));
    assert!(snapshot["records"]
        .as_array()
        .unwrap()
        .iter()
        .any(|row| row["payload"]["metric"] == "daily_steps"));
    assert!(snapshot["records"]
        .as_array()
        .unwrap()
        .iter()
        .any(|row| row["kind"] == "heart_rate"));
    server.abort();
    Ok(())
}

#[tokio::test]
async fn rate_limit_retry_after_is_returned_to_the_watch_tower_queue() -> Result<()> {
    let router = Router::new().route(
        "/",
        get(|| async {
            HttpResponse::builder()
                .status(StatusCode::TOO_MANY_REQUESTS)
                .header("retry-after", "23")
                .body(Body::empty())
                .unwrap()
        }),
    );
    let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0)).await?;
    let origin = format!("http://{}", listener.local_addr()?);
    let server = tokio::spawn(async move { axum::serve(listener, router).await });
    let error = body(Client::new().get(&origin).send().await?)
        .await
        .unwrap_err();
    let retry_at = provider_retry_at(&error).context("missing Garmin Retry-After")?;
    assert!(retry_at >= Utc::now() + CalendarDuration::seconds(21));
    assert!(retry_at <= Utc::now() + CalendarDuration::seconds(24));
    server.abort();
    Ok(())
}
