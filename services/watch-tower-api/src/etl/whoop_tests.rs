use super::*;
use axum::{
    extract::{Path, Query, State},
    http::HeaderMap,
    routing::get,
    Json, Router,
};
use std::{collections::HashMap, sync::Arc};
use tokio::sync::Mutex;

#[test]
fn whoop_cycle_projection_derives_local_day_steps_and_energy() -> Result<()> {
    let start: DateTime<Utc> = "2026-11-20T06:00:00Z".parse()?;
    let end = start + Duration::days(1);
    let window = Window { start, end };
    let raw = json!({"id":42,"start":"2026-11-20T06:00:00Z","end":"2026-11-21T06:00:00Z",
        "timezone_offset":"-05:00","step_count":8100,"score_state":"SCORED",
        "score":{"kilojoule":8368.0}});
    let row = metric(&raw, "cycle", start, end, &window)?.context("cycle was dropped")?;
    let derived = whoop_cycle_metrics(&row, &window).context("cycle derivations missing")?;
    assert_eq!(derived.len(), 2);
    assert_eq!(derived[0]["payload"]["provider_data"]["value"], 8100);
    assert_eq!(
        derived[0]["payload"]["provider_data"]["local_day"],
        "2026-11-20"
    );
    assert_eq!(derived[1]["payload"]["provider_data"]["value"], 2000.0);
    Ok(())
}

#[tokio::test]
async fn collector_uses_scopes_bearer_and_builds_hsm_snapshot() -> Result<()> {
    let start: DateTime<Utc> = "2026-11-20T00:00:00Z".parse()?;
    let end = start + Duration::days(1);
    let requests = Arc::new(Mutex::new(Vec::<String>::new()));
    let router = Router::new()
        .route(
            "/{*kind}",
            get({
                move |Path(kind): Path<String>,
                      State(requests): State<Arc<Mutex<Vec<String>>>>,
                      Query(query): Query<HashMap<String, String>>,
                      headers: HeaderMap| {
                    let requests = Arc::clone(&requests);
                    async move {
                        assert_eq!(headers["authorization"], "Bearer fixture-whoop-token");
                        requests.lock().await.push(format!("{kind}:{}", query.get("nextToken").cloned().unwrap_or_default()));
                        let records = match (kind.as_str(), query.get("nextToken").map(String::as_str)) {
                            ("activity/sleep", _) => vec![json!({
                                "id":"sleep-1","start":"2026-11-20T01:00:00Z","end":"2026-11-20T09:00:00Z",
                                "updated_at":"2026-11-20T09:01:00Z","score":{"stage_summary":{"total_in_bed_time_milli":28800000}},"nap":false
                            })],
                            ("cycle", _) => vec![json!({"id":1,"start":"2026-11-20T00:00:00Z",
                                "end":"2026-11-20T23:59:00Z","timezone_offset":"Z","step_count":5000,
                                "score_state":"SCORED","score":{"kilojoule":4184.0}})],
                            _ => vec![],
                        };
                        Json(json!({"records":records}))
                    }
                }
            }),
        )
        .with_state(Arc::clone(&requests));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let origin = format!("http://{}", listener.local_addr()?);
    let server = tokio::spawn(async move { axum::serve(listener, router).await });
    let snapshot = collect_from_origin(
        &Client::new(),
        "fixture-whoop-token",
        Some("read:sleep read:cycles"),
        "fixture-hsm-lease",
        start,
        end,
        &origin,
        end,
    )
    .await?;
    let snapshot = serde_json::to_value(snapshot)?;
    assert_eq!(snapshot["provider"], "whoop");
    assert_eq!(snapshot["lease"], "fixture-hsm-lease");
    assert_eq!(snapshot["grants"], json!(["sleep", "metric", "steps"]));
    let rows = snapshot["records"]
        .as_array()
        .context("snapshot records missing")?;
    assert!(rows.iter().any(|row| row["kind"] == "sleep"));
    assert!(rows
        .iter()
        .any(|row| row["payload"]["metric"] == "cycle_steps"));
    let requested = requests.lock().await.clone();
    assert_eq!(requested, ["activity/sleep:", "cycle:"]);
    server.abort();
    Ok(())
}
