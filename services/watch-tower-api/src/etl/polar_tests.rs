use super::*;
use axum::{
    extract::{Path, Query, State},
    http::{HeaderMap, StatusCode},
    response::IntoResponse,
    routing::get,
    Json, Router,
};
use std::{collections::HashMap, sync::Arc};
use tokio::sync::Mutex;

#[test]
fn normalizes_polar_workout_zones_and_sleep_stage_samples() -> Result<()> {
    let start: DateTime<Utc> = "2026-11-19T20:00:00Z".parse()?;
    let window = Window {
        start,
        end: start + Duration::days(1),
    };
    let workout = json!({"id":"exercise-1","start_time":"2026-11-20T08:00:00",
    "start_time_utc_offset":120,"duration":"PT1H20M","sport":"RUNNING",
    "upload_time":"2026-11-20T09:30:00Z","heart_rate_zones":[
        {"index":1,"in-zone":"PT10M","lower-limit":100,"upper-limit":120}
    ]});
    let row = polar_point(&workout, &window)?.context("Polar exercise was dropped")?;
    assert_eq!(row["started_at"], "2026-11-20T06:00:00Z");
    assert_eq!(row["ended_at"], "2026-11-20T07:20:00Z");
    assert_eq!(
        row["payload"]["heart_rate_zones"][0]["duration_seconds"],
        600.0
    );

    let night = polar_series::Night::parse(&json!({
        "sleep_start_time":"2026-11-19T23:00:00+02:00",
        "sleep_end_time":"2026-11-20T07:00:00+02:00"
    }))?;
    let stages = night.stages(&json!({"23:00":0,"01:00":2,"03:00":4}));
    assert_eq!(stages.len(), 3);
    assert_eq!(stages[0]["stage"], 1);
    assert_eq!(stages[1]["stage"], 4);
    assert_eq!(stages[2]["stage"], 5);
    Ok(())
}

#[test]
fn normalizes_polar_fit_detail_into_personal_workout_record() -> Result<()> {
    let workout = json!({"id":"exercise-1","start_time":"2026-11-20T08:00:00",
        "start_time_utc_offset":120,"duration":"PT1H20M","sport":"RUNNING",
        "upload_time":"2026-11-20T09:30:00Z","distance":9000});
    let record = normalize_workout_detail(&workout, "exercise-1")?;
    assert_eq!(record["kind"], "workout");
    assert_eq!(record["source_id"], "exercise-1");
    assert_eq!(record["started_at"], "2026-11-20T06:00:00Z");
    assert_eq!(record["payload"]["duration_seconds"], 4800.0);
    assert!(normalize_workout_detail(&workout, "another-exercise").is_err());
    Ok(())
}

#[tokio::test]
async fn collector_combines_exercise_sleep_and_polar_daily_series() -> Result<()> {
    let requests = Arc::new(Mutex::new(Vec::<String>::new()));
    let router = Router::new()
        .route(
            "/{*path}",
            get({
                move |Path(path): Path<String>,
                      State(requests): State<Arc<Mutex<Vec<String>>>>,
                      Query(query): Query<HashMap<String, String>>,
                      headers: HeaderMap| {
                    let requests = Arc::clone(&requests);
                    async move {
                        assert_eq!(headers["authorization"], "Bearer fixture-polar-token");
                        requests.lock().await.push(path.clone());
                        if path.starts_with("users/")
                            && !matches!(path.as_str(), "users/sleep" | "users/nightly-recharge")
                        {
                            assert_eq!(query["from"], "2026-11-19");
                            assert_eq!(query["to"], "2026-11-21");
                        }
                        if path == "exercises" {
                            return Json(json!([{
                                "id":"exercise-1","device":"Polar Vantage","start_time":"2026-11-20T08:00:00",
                                "start_time_utc_offset":120,"duration":"PT1H","upload_time":"2026-11-20T07:00:00Z",
                                "sport":"RUNNING","distance":9000,"heart_rate":{"average":145,"maximum":170}
                            }])).into_response();
                        }
                        if path == "users/sleep" {
                            return Json(json!({"nights":[{
                                "date":"2026-11-20","sleep_start_time":"2026-11-19T23:00:00+02:00",
                                "sleep_end_time":"2026-11-20T07:00:00+02:00","hypnogram":{"23:00":0,"01:00":2},
                                "heart_rate_samples":{"23:10":72}
                            }]})).into_response();
                        }
                        if path == "users/nightly-recharge" {
                            return Json(json!({"recharges":[{
                                "date":"2026-11-20","sleep_start_time":"2026-11-19T23:00:00+02:00",
                                "sleep_end_time":"2026-11-20T07:00:00+02:00","hrv_samples":{"23:30":42.0}
                            }]})).into_response();
                        }
                        if path == "users/activities" {
                            return Json(json!([{"date":"2026-11-20","steps":{
                                "samples":[{"timestamp":"2026-11-20T12:00:00Z","steps":100}],"interval_ms":60000
                            }}])).into_response();
                        }
                        if path == "users/activities/samples" {
                            return Json(json!([{"date":"2026-11-20","steps":{
                                "samples":[{"timestamp":"2026-11-20T12:00:00Z","steps":100}],"interval_ms":60000
                            }}])).into_response();
                        }
                        if path == "users/continuous-heart-rate" {
                            return Json(json!({"heart_rates":[{"date":"2026-11-20",
                                "heart_rate_samples":[{"sample_time":"12:00:00","heart_rate":75}]}
                            ]})).into_response();
                        }
                        StatusCode::NO_CONTENT.into_response()
                    }
                }
            }),
        )
        .with_state(Arc::clone(&requests));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let origin = format!("http://{}", listener.local_addr()?);
    let server = tokio::spawn(async move { axum::serve(listener, router).await });
    let start: DateTime<Utc> = "2026-11-19T20:00:00Z".parse()?;
    let snapshot = collect_from_origin(
        &Client::new(),
        "fixture-polar-token",
        "fixture-hsm-lease",
        start,
        start + Duration::days(2),
        &origin,
    )
    .await?;
    let snapshot = serde_json::to_value(snapshot)?;
    assert_eq!(snapshot["provider"], "polar");
    assert_eq!(
        snapshot["grants"],
        json!(["workout", "sleep", "heart_rate", "steps", "metric"])
    );
    let rows = snapshot["records"]
        .as_array()
        .context("Polar snapshot records missing")?;
    for kind in ["workout", "sleep", "heart_rate", "steps", "metric"] {
        assert!(
            rows.iter().any(|row| row["kind"] == kind),
            "missing {kind} record"
        );
    }
    assert!(requests
        .lock()
        .await
        .contains(&"users/biosensing/ecg".to_owned()));
    server.abort();
    Ok(())
}
