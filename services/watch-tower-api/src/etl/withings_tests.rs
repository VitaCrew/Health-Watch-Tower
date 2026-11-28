use super::*;
use axum::{
    body::{to_bytes, Body},
    extract::Request,
    http::{Response as HttpResponse, StatusCode},
    routing::any,
    Router,
};

#[test]
fn retry_after_accepts_delta_seconds_and_http_dates() -> Result<()> {
    let now: DateTime<Utc> = "2026-11-27T12:00:00Z".parse()?;
    assert_eq!(retry_after(Some("300"), now), now + Duration::minutes(5));
    assert_eq!(
        retry_after(Some("Fri, 01 Jan 2027 13:00:00 GMT"), now),
        "2027-01-01T13:00:00Z".parse::<DateTime<Utc>>()?
    );
    assert_eq!(
        retry_after(Some("Thu, 01 Jan 1970 00:00:00 GMT"), now),
        now + Duration::minutes(5)
    );
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
    let start: DateTime<Utc> = "2026-11-21T00:00:00Z".parse()?;
    let end = "2026-11-22T00:00:00Z".parse()?;
    let endpoints = [
        format!("{origin}/v2/sleep"),
        format!("{origin}/v2/measure"),
        format!("{origin}/measure"),
        format!("{origin}/v2/heart"),
    ];
    let error = collect_from_origins(
        &Client::new(),
        "rate-limit-token",
        Some("user.activity"),
        "rate-limit-lease",
        start,
        end,
        [&endpoints[0], &endpoints[1], &endpoints[2], &endpoints[3]],
    )
    .await
    .err()
    .context("rate limited collection should fail")?;
    assert_eq!(
        provider_retry_at(&error),
        Some("2027-01-01T13:00:00Z".parse()?)
    );
    server.abort();
    Ok(())
}

#[test]
fn normalizes_withings_sleep_workout_measurement_and_intraday_rows() -> Result<()> {
    let window = Window {
        start: "2026-11-21T00:00:00Z".parse()?,
        end: "2026-11-22T00:00:00Z".parse()?,
    };
    let sleep = sleep_point(
        &json!({"id":42,"startdate":1795221000_i64,"enddate":1795249800_i64,
            "data":{"deepsleepduration":900}}),
        &window,
    )?
    .context("sleep should be inside the window")?;
    assert_eq!(
        sleep["payload"]["provider_stage_summary"]["deepsleepduration"],
        900
    );
    let workout = workout(
        &json!({"id":8,"category":1,"startdate":1795251600_i64,"enddate":1795255200_i64,
            "data":{"calories":30,"distance":500}}),
        &window,
    )?
    .context("workout should be inside the window")?;
    assert_eq!(workout["payload"]["duration_seconds"], 3600);
    assert_eq!(
        measurement_value(&json!({"type":1,"value":724,"unit":-1})),
        Some(("weight", "kg", 72.4))
    );
    let at = window.start + Duration::hours(12);
    let mut rows = Vec::new();
    intraday(
        &mut rows,
        &json!({(at.timestamp().to_string()):{"heart_rate":65.0}}),
        &window,
        window.end,
    )?;
    assert_eq!(rows.len(), 2);
    assert!(rows.iter().any(|row| row["kind"] == "heart_rate"));
    Ok(())
}

#[tokio::test]
async fn collector_uses_scopes_and_carries_metrics_revocation_to_hsm() -> Result<()> {
    let now = Utc::now();
    let start = now - Duration::hours(6);
    let end = now - Duration::hours(1);
    let router = Router::new().route(
        "/{*path}",
        any(move |request: Request| async move {
            let path = request.uri().path().to_owned();
            if request
                .headers()
                .get("authorization")
                .and_then(|v| v.to_str().ok())
                != Some("Bearer fixture-token")
            {
                return HttpResponse::builder()
                    .status(StatusCode::UNAUTHORIZED)
                    .body(Body::empty())
                    .unwrap();
            }
            let bytes = to_bytes(request.into_body(), 16 * 1024).await.unwrap();
            let form = String::from_utf8_lossy(&bytes);
            let response = if path.ends_with("/v2/sleep") && form.contains("getsummary") {
                json!({"status":0,"body":{"series":[],"more":false}})
            } else if path.ends_with("/v2/measure")
                && (form.contains("getactivity") || form.contains("getworkouts"))
            {
                json!({"status":0,"body":{"activities":[],"series":[],"more":false}})
            } else if path.ends_with("/v2/measure") && form.contains("getintradayactivity") {
                json!({"status":0,"body":{"series":{}}})
            } else {
                return HttpResponse::builder()
                    .status(StatusCode::NO_CONTENT)
                    .body(Body::empty())
                    .unwrap();
            };
            HttpResponse::builder()
                .status(StatusCode::OK)
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(&response).unwrap()))
                .unwrap()
        }),
    );
    let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0)).await?;
    let origin = format!("http://{}", listener.local_addr()?);
    let endpoints = [
        format!("{origin}/v2/sleep"),
        format!("{origin}/v2/measure"),
        format!("{origin}/measure"),
        format!("{origin}/v2/heart"),
    ];
    let server = tokio::spawn(async move { axum::serve(listener, router).await });
    let client = Client::new();
    let snapshot = collect_from_origins(
        &client,
        "fixture-token",
        Some("user.activity"),
        "fixture-lease",
        start,
        end,
        [&endpoints[0], &endpoints[1], &endpoints[2], &endpoints[3]],
    )
    .await?;
    assert_eq!(snapshot.withings_measurement_access(), Some(false));
    assert!(snapshot.grants.contains(&"sleep".to_owned()));
    assert!(snapshot.grants.contains(&"metric".to_owned()));
    server.abort();
    Ok(())
}

#[tokio::test]
async fn without_activity_or_metric_scopes_no_provider_calls_are_made() -> Result<()> {
    let start = Utc::now() - Duration::hours(3);
    let end = Utc::now() - Duration::hours(2);
    let snapshot = collect_from_origins(
        &Client::new(),
        "fixture-token",
        Some("user.info"),
        "fixture-lease",
        start,
        end,
        ["http://127.0.0.1:1"; 4],
    )
    .await?;
    assert!(snapshot.grants.is_empty());
    assert_eq!(snapshot.withings_measurement_access(), Some(false));
    Ok(())
}

#[tokio::test]
async fn collection_merges_sleep_activity_measurements_and_ecg() -> Result<()> {
    let start: DateTime<Utc> = "2026-11-21T00:00:00Z".parse()?;
    let end = "2026-11-22T00:00:00Z".parse()?;
    let sleep_start = "2026-11-21T01:00:00Z".parse::<DateTime<Utc>>()?.timestamp();
    let sleep_end = "2026-11-21T08:00:00Z".parse::<DateTime<Utc>>()?.timestamp();
    let workout_start = "2026-11-21T09:00:00Z".parse::<DateTime<Utc>>()?.timestamp();
    let workout_end = workout_start + 1800;
    let measurement_at = "2026-11-21T10:00:00Z".parse::<DateTime<Utc>>()?.timestamp();
    let ecg_at = "2026-11-21T11:00:00Z".parse::<DateTime<Utc>>()?.timestamp();
    let router = Router::new().route(
        "/{*path}",
        any(move |request: Request| async move {
            let path = request.uri().path().to_owned();
            if request.headers().get("authorization").and_then(|value| value.to_str().ok()) != Some("Bearer full-scope-token") {
                return HttpResponse::builder().status(StatusCode::UNAUTHORIZED).body(Body::empty()).unwrap();
            }
            let bytes = to_bytes(request.into_body(), 32 * 1024).await.unwrap();
            let form = String::from_utf8_lossy(&bytes);
            let body = if path.ends_with("/v2/sleep") && form.contains("getsummary") {
                json!({"series":[{"id":42,"startdate":sleep_start,"enddate":sleep_end,"model_id":94,
                    "data":{"deepsleepduration":900}}],"more":false})
            } else if path.ends_with("/v2/sleep") && form.contains("action=get") {
                json!({"series":[{"startdate":sleep_start,"enddate":sleep_end,"model_id":94,"state":1}],"more":false})
            } else if path.ends_with("/v2/measure") && form.contains("getactivity") {
                json!({"activities":[{"id":9,"date":"2026-11-21","steps":3000}],"more":false})
            } else if path.ends_with("/v2/measure") && form.contains("getworkouts") {
                json!({"series":[{"id":8,"category":1,"startdate":workout_start,"enddate":workout_end,
                    "data":{"calories":30,"distance":500}}],"more":false})
            } else if path.ends_with("/v2/measure") && form.contains("getintradayactivity") {
                json!({"series":{(workout_start.to_string()):{"heart_rate":65.0,"steps":10}}})
            } else if path.ends_with("/measure") && form.contains("getmeas") {
                json!({"status":0,"body":{"measuregrps":[{"date":measurement_at,"grpid":123,"category":1,
                    "measures":[{"type":1,"unit":-1,"value":724}]}],"more":false}})
            } else if path.ends_with("/v2/heart") && form.contains("action=list") {
                json!({"series":[{"timestamp":ecg_at,"ecg":{"signalid":77}}],"more":false})
            } else if path.ends_with("/v2/heart") && form.contains("action=get") {
                json!({"signal":[1_i64,2,3],"sampling_frequency":300})
            } else {
                return HttpResponse::builder().status(StatusCode::NOT_FOUND).body(Body::empty()).unwrap();
            };
            let envelope = if path == "/measure" {
                body
            } else {
                json!({"status":0,"body":body})
            };
            HttpResponse::builder().status(StatusCode::OK).header("content-type","application/json")
                .body(Body::from(serde_json::to_vec(&envelope).unwrap())).unwrap()
        }),
    );
    let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0)).await?;
    let origin = format!("http://{}", listener.local_addr()?);
    let endpoints = [
        format!("{origin}/v2/sleep"),
        format!("{origin}/v2/measure"),
        format!("{origin}/measure"),
        format!("{origin}/v2/heart"),
    ];
    let server = tokio::spawn(async move { axum::serve(listener, router).await });
    let snapshot = collect_from_origins(
        &Client::new(),
        "full-scope-token",
        Some("user.activity user.metrics"),
        "full-lease",
        start,
        end,
        [&endpoints[0], &endpoints[1], &endpoints[2], &endpoints[3]],
    )
    .await?;
    let snapshot = serde_json::to_value(snapshot)?;
    assert_eq!(snapshot["provider"], "withings");
    assert!(snapshot["grants"]
        .as_array()
        .unwrap()
        .iter()
        .any(|grant| grant == "metric"));
    let rows = snapshot["records"].as_array().unwrap();
    assert!(rows.iter().any(|row| row["kind"] == "sleep"
        && row["payload"]["stages"]
            .as_array()
            .is_some_and(|stages| !stages.is_empty())));
    assert!(rows.iter().any(|row| row["kind"] == "workout"));
    assert!(rows.iter().any(|row| row["payload"]["metric"] == "weight"
        && row["payload"]["provider_data"]["value"] == 72.4));
    assert!(rows.iter().any(|row| row["payload"]["metric"] == "ecg"
        && row["payload"]["provider_data"]["waveform"]["signal"]
            .as_array()
            .is_some_and(|signal| signal.len() == 3)));
    server.abort();
    Ok(())
}
