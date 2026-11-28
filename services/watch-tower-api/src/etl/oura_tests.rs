use super::*;
use axum::{
    extract::{Path, Query, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response as AxumResponse},
    routing::get,
    Json, Router,
};
use std::{collections::HashMap, sync::Arc};
use tokio::sync::Mutex;

#[test]
fn normalizes_sleep_stages_and_keeps_provider_activity_document() -> Result<()> {
    let start: DateTime<Utc> = "2026-11-20T00:00:00Z".parse()?;
    let end = start + Duration::days(1);
    let window = Window { start, end };
    let item = json!({
        "id":"sleep-1","bedtime_start":"2026-11-20T01:00:00Z",
        "bedtime_end":"2026-11-20T03:00:00Z","total_sleep_duration":6900,
        "sleep_phase_30_sec":"1112234"
    });
    let normalized = oura_point(&item, "sleep", &window)?.context("sleep was dropped")?;
    assert_eq!(normalized["payload"]["stage_resolution_seconds"], 30);
    assert_eq!(normalized["payload"]["stages"].as_array().unwrap().len(), 4);
    assert_eq!(normalized["payload"]["provider_total_sleep_seconds"], 6900);

    let activity = json!({"id":"activity-1","day":"2026-11-20","class_5_min":"012345"});
    let row = metric(&activity, "daily_activity", start, start, &window)?
        .context("activity metric was dropped")?;
    let mut payload = row["payload"].clone();
    add_activity_classes(&mut payload);
    assert_eq!(payload["activity_classification"]["slots"][5], "high");
    Ok(())
}

#[tokio::test]
async fn collection_paginates_scoped_data_and_preserves_sleep_deletions() -> Result<()> {
    let start: DateTime<Utc> = "2026-11-20T00:00:00Z".parse()?;
    let end = start + Duration::days(1);
    let calls = Arc::new(Mutex::new(Vec::<String>::new()));
    let router = Router::new()
        .route(
            "/{*endpoint}",
            get({
                move |Path(endpoint): Path<String>,
                      State(calls): State<Arc<Mutex<Vec<String>>>>,
                      Query(query): Query<HashMap<String, String>>,
                      headers: HeaderMap| {
                    let calls = Arc::clone(&calls);
                    async move {
                        assert_eq!(headers["authorization"], "Bearer fixture-oura-token");
                        let page = query.get("next_token").map(String::as_str).unwrap_or("");
                        calls.lock().await.push(format!("{endpoint}:{page}"));
                        if endpoint == "rest_mode_period" {
                            return AxumResponse::builder()
                                .status(StatusCode::FORBIDDEN)
                                .body(axum::body::Body::empty())
                                .unwrap();
                        }
                        let data = match (endpoint.as_str(), page) {
                            ("sleep", "") => vec![json!({
                                "id":"sleep-1","bedtime_start":"2026-11-20T01:00:00Z",
                                "bedtime_end":"2026-11-20T07:00:00Z","total_sleep_duration":20000,
                                "sleep_phase_30_sec":"1112222333444"
                            })],
                            ("sleep", "sleep-page-2") => vec![json!({"type":"deleted","id":"sleep-deleted"})],
                            ("heartrate", _) => vec![json!({"timestamp":"2026-11-20T12:00:00Z","bpm":72,"source":"awake"})],
                            _ => vec![],
                        };
                        let next_token = (endpoint == "sleep" && page.is_empty())
                            .then_some("sleep-page-2");
                        Json(json!({"data":data,"next_token":next_token})).into_response()
                    }
                }
            }),
        )
        .with_state(Arc::clone(&calls));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let origin = format!("http://{}", listener.local_addr()?);
    let server = tokio::spawn(async move { axum::serve(listener, router).await });
    let snapshot = collect_from_origin(
        &Client::new(),
        "fixture-oura-token",
        Some("daily heartrate"),
        "fixture-hsm-lease",
        start,
        end,
        &origin,
        end,
    )
    .await?;
    let snapshot = serde_json::to_value(snapshot)?;
    assert_eq!(snapshot["provider"], "oura");
    assert_eq!(snapshot["lease"], "fixture-hsm-lease");
    assert_eq!(
        snapshot["oura_sleep_deletions"]["ids"],
        json!(["sleep-deleted"])
    );
    let rows = snapshot["records"]
        .as_array()
        .context("Oura snapshot records missing")?;
    assert!(rows.iter().any(|row| row["kind"] == "sleep"));
    assert!(rows.iter().any(|row| row["kind"] == "heart_rate"));
    let requests = calls.lock().await.clone();
    assert!(requests.contains(&"sleep:".to_owned()));
    assert!(requests.contains(&"sleep:sleep-page-2".to_owned()));
    assert!(requests.contains(&"heartrate:".to_owned()));
    server.abort();
    Ok(())
}

#[tokio::test]
async fn rate_limit_retry_after_is_returned_to_the_watch_tower_queue() -> Result<()> {
    let router = Router::new().route(
        "/workout",
        get(|| async {
            (
                [(axum::http::header::RETRY_AFTER, "30")],
                StatusCode::TOO_MANY_REQUESTS,
            )
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let origin = format!("http://{}", listener.local_addr()?);
    let server = tokio::spawn(async move { axum::serve(listener, router).await });
    let start: DateTime<Utc> = "2026-11-20T00:00:00Z".parse()?;
    let before = Utc::now();
    let result = collect_from_origin(
        &Client::new(),
        "fixture-oura-token",
        Some("workout"),
        "fixture-hsm-lease",
        start,
        start + Duration::days(1),
        &origin,
        before,
    )
    .await;
    let error = match result {
        Err(error) => error,
        Ok(_) => anyhow::bail!("Oura rate limit must defer the provider job"),
    };
    let retry_at = provider_retry_at(&error).context("missing Oura retry time")?;
    assert!(retry_at >= before + Duration::seconds(29));
    assert!(retry_at <= Utc::now() + Duration::seconds(31));
    server.abort();
    Ok(())
}
