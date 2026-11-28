use super::*;
use axum::{
    extract::{Query, State},
    http::HeaderMap,
    routing::get,
    Json, Router,
};
use std::{collections::HashMap, sync::Arc};
use tokio::sync::Mutex;

#[test]
fn normalizes_sleep_and_deduplicated_series_into_hsm_snapshot_shape() -> Result<()> {
    let start: DateTime<Utc> = "2026-11-20T00:00:00Z".parse()?;
    let end = start + Duration::days(1);
    let window = Window { start, end };
    let at = start.timestamp() + 600;
    let mut records = Vec::new();
    let mut series = BTreeMap::new();
    for item in [
        json!({"type":"Sleep","object":{
            "id":"sleep-1","bedtime_start":at,"bedtime_end":at+3600,
            "sleep_graph":{"data":[
                {"start":at,"end":at+600,"type":"awake"},
                {"start":at+600,"end":at+1800,"type":"deep_sleep"}
            ]},
            "sleep_stages":[{"type":"deep_sleep","stage_time":1200}],
            "quick_metrics":[{"type":"total_sleep","value":3000},{"type":"sleep_efic","value":92}],
            "spo2":{"value":97}
        }}),
        json!({"type":"hr","object":{"values":[{"timestamp":at,"value":72},{"timestamp":at,"value":73}]}}),
        json!({"type":"steps","object":{"values":[{"timestamp":at,"value":0}]}}),
    ] {
        append_metric(
            &mut records,
            &mut series,
            &item,
            start.date_naive(),
            &window,
        )?;
    }
    assert_eq!(
        records.len(),
        3,
        "daily provider documents remain alongside samples"
    );
    assert_eq!(records[0]["payload"]["stages"].as_array().unwrap().len(), 2);
    assert_eq!(
        records[0]["payload"]["provider_stage_summary"]["deep_seconds"],
        1200
    );
    assert_eq!(
        records[0]["payload"]["sleep_metrics"]["average_spo2_percent"],
        97.0
    );
    assert_eq!(
        series.len(),
        2,
        "duplicate provider timestamps collapse to one sample"
    );
    let normalized = series.into_values().collect::<Vec<_>>();
    assert!(normalized
        .iter()
        .any(|row| row["payload"]["provider_data"]["value"] == 0));
    assert!(normalized.iter().any(|row| row["kind"] == "heart_rate"));
    Ok(())
}

#[tokio::test]
async fn collection_uses_transient_bearer_and_returns_normalized_snapshot() -> Result<()> {
    let day: DateTime<Utc> = "2026-11-20T00:00:00Z".parse()?;
    let sample_at = day.timestamp() + 600;
    let requests = Arc::new(Mutex::new(Vec::<String>::new()));
    let router = Router::new()
        .route(
            "/metrics",
            get({
                move |State(requests): State<Arc<Mutex<Vec<String>>>>,
                      Query(query): Query<HashMap<String, String>>,
                      headers: HeaderMap| {
                    let requests = Arc::clone(&requests);
                    async move {
                        assert_eq!(headers["authorization"], "Bearer fixture-ultrahuman-token");
                        let requested_day = query.get("date").unwrap().clone();
                        requests.lock().await.push(requested_day.clone());
                        let items = if requested_day == "2026-11-20" {
                            vec![json!({"type":"hr","object":{"values":[
                                {"timestamp":sample_at,"value":71}
                            ]}})]
                        } else {
                            vec![]
                        };
                        Json(json!({"data":{"metric_data":items}}))
                    }
                }
            }),
        )
        .with_state(Arc::clone(&requests));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let origin = format!("http://{}/metrics", listener.local_addr()?);
    let server = tokio::spawn(async move { axum::serve(listener, router).await });
    let snapshot = collect_from_origin(
        &Client::new(),
        "fixture-ultrahuman-token",
        "fixture-hsm-lease",
        day,
        day + Duration::days(1),
        &origin,
    )
    .await?;
    let snapshot = serde_json::to_value(snapshot)?;
    assert_eq!(snapshot["provider"], "ultrahuman");
    let records = snapshot["records"].as_array().unwrap();
    assert_eq!(
        records.len(),
        2,
        "daily metric and time-series record are both retained"
    );
    let heart_rate = records
        .iter()
        .find(|record| record["kind"] == "heart_rate")
        .context("missing normalized heart-rate series")?;
    assert_eq!(heart_rate["payload"]["samples"][0]["bpm"], 71.0);
    let requested = requests.lock().await.clone();
    assert_eq!(
        requested,
        ["2026-11-19", "2026-11-20", "2026-11-21", "2026-11-22"]
    );
    server.abort();
    Ok(())
}
