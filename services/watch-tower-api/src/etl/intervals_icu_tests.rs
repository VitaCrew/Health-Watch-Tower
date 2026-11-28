use super::*;
use axum::{extract::Query, http::HeaderMap, routing::get, Json, Router};
use std::collections::HashMap;

#[test]
fn normalizes_intervals_activity_duration_and_rejects_out_of_window_rows() -> Result<()> {
    let start: DateTime<Utc> = "2026-11-20T08:00:00Z".parse()?;
    let end = start + Duration::days(1);
    let window = Window { start, end };
    let workout = json!({"id":"run-42","start_date":"2026-11-20T09:00:00Z",
        "elapsed_time":3600,"type":"Run","name":"Morning run","distance":10000,
        "file_type":"FIT","source":"Garmin"});
    let row = intervals_point(&workout, &window)?.context("Intervals.icu workout was dropped")?;
    assert_eq!(row["ended_at"], "2026-11-20T10:00:00Z");
    assert_eq!(row["payload"]["exercise_type"], "Run");
    assert!(intervals_point(
        &json!({"id":"outside","start_date":"2026-11-21T09:00:00Z","elapsed_time":3600,"type":"Run"}),
        &window
    )?
    .is_none());
    Ok(())
}

#[tokio::test]
async fn wellness_scope_uses_oauth_and_returns_hsm_snapshot() -> Result<()> {
    let router = Router::new().route(
        "/api/v1/athlete/0/wellness",
        get(
            |headers: HeaderMap, Query(query): Query<HashMap<String, String>>| async move {
                assert_eq!(headers["authorization"], "Bearer fixture-intervals-token");
                assert_eq!(query["oldest"], "2026-11-19");
                assert_eq!(query["newest"], "2026-11-22");
                Json(json!([
                    {"id":"2026-11-19","steps":600},
                    {"id":"2026-11-20","updated":"2026-11-21T02:14:22.925+0000",
                     "steps":3800,"restingHR":51,"hrv":40.5,"sleepSecs":26800,"weight":74.2},
                    {"id":"2026-11-21","steps":700}
                ]))
            },
        ),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let origin = format!("http://{}", listener.local_addr()?);
    let server = tokio::spawn(async move { axum::serve(listener, router).await });
    let start: DateTime<Utc> = "2026-11-20T00:00:00Z".parse()?;
    let end = start + Duration::days(1);
    let snapshot = collect_from_origin(
        &Client::new(),
        "fixture-intervals-token",
        Some("WELLNESS:READ"),
        "fixture-hsm-lease",
        start,
        end,
        &origin,
        end,
    )
    .await?;
    let snapshot = serde_json::to_value(snapshot)?;
    assert_eq!(snapshot["provider"], "intervals_icu");
    assert_eq!(snapshot["grants"], json!(["steps", "metric"]));
    let rows = snapshot["records"]
        .as_array()
        .context("snapshot records missing")?;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["payload"]["provider_data"]["steps"], 3800);
    assert_eq!(rows[0]["modified_at"], "2026-11-21T02:14:22.925Z");
    server.abort();
    Ok(())
}
