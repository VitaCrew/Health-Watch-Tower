use super::*;
use axum::{
    body::Body,
    extract::Request,
    http::{Response as HttpResponse, StatusCode},
    routing::any,
    Router,
};

#[tokio::test]
async fn collection_merges_daily_sleep_biometrics_workouts_and_activity() -> Result<()> {
    let start: DateTime<Utc> = "2026-04-25T00:00:00Z".parse()?;
    let end = start + CalendarDuration::days(1);
    let sample_at = start + CalendarDuration::hours(12);
    let sleep_start = start + CalendarDuration::hours(2);
    let sleep_end = sleep_start + CalendarDuration::hours(7);
    let router = Router::new().fallback(any(move |request: Request| async move {
        if request.headers().get("authorization").and_then(|value| value.to_str().ok())
            != Some("Bearer sensorbio-test-token")
        {
            return HttpResponse::builder().status(StatusCode::UNAUTHORIZED).body(Body::empty()).unwrap();
        }
        let path = request.uri().path();
        let query = request.uri().query().unwrap_or_default();
        let day_request = query.contains("date=2026-04-25");
        let body = match path {
            "/v1/step/details" if day_request => json!({"date":"2026-04-25","granularity":"day","metrics":[{"name":"Steps","value":6400}]}),
            "/v1/calorie/details" if day_request => json!({"date":"2026-04-25","granularity":"day","metrics":[{"name":"Total Calories","value":1900,"unit":"kcal"},{"name":"Resting Calories","value":1500,"unit":"kcal"}]}),
            "/v1/scores/recovery/details" if day_request => json!({"date":"2026-04-25","granularity":"day","score":74.0}),
            "/v1/steps" => json!({"data":[{"timestamp":sample_at.timestamp_millis(),"steps":15,"distance":12.5,"total_step_calories":0.7}]}),
            "/v1/activities" => json!({"data":[{"timestamp":sample_at.timestamp_millis(),"name":"Run","activities":[{"id":"run-1","start_time":sample_at.timestamp_millis(),"end_time":(sample_at+CalendarDuration::minutes(30)).timestamp_millis()}]}]}),
            "/v1/sleep" if day_request => json!({"data":[{"id":"sleep-1","start_timestamp":sleep_start.timestamp_millis(),"end_timestamp":sleep_end.timestamp_millis(),"avg_heart_rate":57,"biometrics":{"resting_bpm":52,"hrv":45.0}}]}),
            "/v1/sleep" => json!({"data":[]}),
            "/v1/sleep/details/day" => json!({"date":"2026-04-25","sleep_stages":[{"start_time":sleep_start,"end_time":sleep_end,"status":"deep"}],"all_sleeps_time_details":[{"start_ts_millis":sleep_start.timestamp_millis(),"end_ts_millis":sleep_end.timestamp_millis(),"sleep_onset_ts_millis":(sleep_start+CalendarDuration::minutes(10)).timestamp_millis()}],"main_sleep_detail_metrics":{"total_time_in_bed_in_mins":420,"sleep_score":80},"main_sleep_stages_percentages":{"deep":25}}),
            "/v1/scores" => json!({"data":{"sleep":{"value":80}}}),
            "/v1/biometrics" => json!({"data":[{"id":"bio-1","timestamp":sample_at.timestamp_millis(),"bpm":72,"hrv":40.0,"spo2":97.0}]}),
            _ => return HttpResponse::builder().status(StatusCode::NOT_FOUND).body(Body::empty()).unwrap(),
        };
        HttpResponse::builder().status(StatusCode::OK).header("content-type", "application/json")
            .body(Body::from(serde_json::to_vec(&body).unwrap())).unwrap()
    }));
    let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0)).await?;
    let origin = format!("http://{}", listener.local_addr()?);
    let server = tokio::spawn(async move { axum::serve(listener, router).await });
    let snapshot = collect_from_origin(
        &Client::new(),
        "sensorbio-test-token",
        "hsm-lease",
        start,
        end,
        &origin,
    )
    .await?;
    assert_eq!(snapshot.provider.as_deref(), Some("sensorbio"));
    let snapshot: Value = serde_json::to_value(snapshot)?;
    let records = snapshot["records"].as_array().unwrap();
    assert!(records
        .iter()
        .any(|row| row["payload"]["metric"] == "daily_activity"));
    assert!(records
        .iter()
        .any(|row| row["payload"]["metric"] == "recovery_details"));
    assert!(records
        .iter()
        .any(|row| row["kind"] == "sleep" && row["payload"]["stages"][0]["stage"] == 5));
    assert!(records
        .iter()
        .any(|row| row["kind"] == "workout" && row["source_id"] == "run-1"));
    let normalized_ids: Vec<_> = records
        .iter()
        .map(|row| (row["kind"].as_str(), row["source_id"].as_str()))
        .collect();
    assert!(
        records.iter().any(|row| {
            row["kind"] == "heart_rate"
                && row["payload"]["samples"][0]["bpm"].as_f64() == Some(72.0)
        }),
        "{normalized_ids:?}"
    );
    assert!(records
        .iter()
        .any(|row| row["payload"]["metric"] == "activity_minute"));
    server.abort();
    Ok(())
}
