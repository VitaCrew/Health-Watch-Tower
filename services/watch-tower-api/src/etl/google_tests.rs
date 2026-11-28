use super::*;
use axum::{
    body::Body,
    extract::Request,
    http::{Response as HttpResponse, StatusCode},
    routing::any,
    Router,
};

#[tokio::test]
async fn collection_merges_records_and_retains_metric_coverage() -> Result<()> {
    let start: DateTime<Utc> = "2026-04-25T00:00:00Z".parse()?;
    let end = start + CalendarDuration::days(1);
    let sample = start + CalendarDuration::hours(12);
    let router = Router::new().fallback(any(move |request: Request| async move {
        if request.headers().get("authorization").and_then(|value| value.to_str().ok())
            != Some("Bearer google-test-token")
        {
            return HttpResponse::builder().status(StatusCode::UNAUTHORIZED).body(Body::empty()).unwrap();
        }
        let path = request.uri().path();
        let body = if path.ends_with("/steps/dataPoints") {
            json!({"dataPoints":[{"name":"users/me/dataTypes/steps/dataPoints/step-1","steps":{"interval":{"startTime":sample,"endTime":sample+CalendarDuration::minutes(1)},"count":42,"updateTime":sample},"dataSource":{"platform":"watch"}}]})
        } else {
            json!({"dataPoints":[],"rollupDataPoints":[]})
        };
        HttpResponse::builder().status(StatusCode::OK).header("content-type", "application/json")
            .body(Body::from(serde_json::to_vec(&body).unwrap())).unwrap()
    }));
    let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0)).await?;
    let origin = format!("http://{}", listener.local_addr()?);
    let server = tokio::spawn(async move { axum::serve(listener, router).await });
    let scope = "https://www.googleapis.com/auth/googlehealth.activity_and_fitness.readonly";
    let snapshot = collect_from_origin(
        &Client::new(),
        "google-test-token",
        Some(scope),
        "hsm-lease",
        start,
        end,
        &origin,
    )
    .await?;
    assert_eq!(snapshot.provider.as_deref(), Some("google_health"));
    let snapshot: Value = serde_json::to_value(snapshot)?;
    assert!(snapshot["records"].as_array().unwrap().iter().any(|row| {
        row["kind"] == "steps" && row["source_id"] == "step-1" && row["payload"]["count"] == 42
    }));
    assert!(snapshot["grants"]
        .as_array()
        .unwrap()
        .iter()
        .any(|grant| grant == "metric"));
    server.abort();
    Ok(())
}

#[tokio::test]
async fn rejects_windows_longer_than_one_day() {
    let start: DateTime<Utc> = "2026-04-25T00:00:00Z".parse().unwrap();
    let result = collect_from_origin(
        &Client::new(),
        "token",
        None,
        "lease",
        start,
        start + CalendarDuration::days(2),
        "http://127.0.0.1",
    )
    .await;
    let error = match result {
        Ok(_) => panic!("long collection window was accepted"),
        Err(error) => error,
    };
    assert!(error
        .to_string()
        .contains("invalid Google Health collection window"));
}
