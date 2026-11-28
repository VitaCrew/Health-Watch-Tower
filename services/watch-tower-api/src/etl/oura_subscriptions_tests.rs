use super::*;
use axum::{extract::State, http::HeaderMap, response::IntoResponse, routing::get, Json, Router};
use std::sync::Arc;
use tokio::sync::Mutex;

fn test_config(callback_url: &str) -> Config {
    Config {
        client_id: "fixture-client".into(),
        secret: "fixture-secret-0123456789".into(),
        verify: "fixture-verify-0123456789".into(),
        callback: callback(callback_url).unwrap(),
    }
}

#[test]
fn callback_requires_public_https_origin() {
    assert!(callback("https://oura.example/").is_some());
    assert!(callback("http://oura.example/").is_none());
    assert!(callback("https://localhost/").is_none());
    assert!(callback("https://oura.example/path").is_none());
}

#[tokio::test]
async fn reconcile_renews_only_our_callback_and_stops_on_rate_limit() -> Result<()> {
    let seen = Arc::new(Mutex::new(Vec::<String>::new()));
    let router = Router::new()
        .route("/subscriptions", get(list).post(create))
        .route("/subscriptions/renew/ours", axum::routing::put(renew))
        .with_state(seen.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let endpoint = format!("http://{}/subscriptions", listener.local_addr()?);
    let server = tokio::spawn(async move {
        let _ = axum::serve(listener, router).await;
    });
    let config = test_config("https://oura.example/");
    let wait = reconcile(
        &Client::new(),
        &config,
        &endpoint,
        &["sleep", "workout"],
        Utc::now(),
    )
    .await?;
    assert_eq!(wait, Duration::from_secs(780));
    assert_eq!(*seen.lock().await, ["list", "renew", "delete"]);
    server.abort();
    Ok(())
}

async fn list(headers: HeaderMap, State(seen): State<Arc<Mutex<Vec<String>>>>) -> Json<Value> {
    assert_eq!(headers["x-client-id"], "fixture-client");
    seen.lock().await.push("list".to_owned());
    Json(json!([
        {"id":"fresh","callback_url":"https://oura.example/wearables/webhooks/oura","event_type":"create","data_type":"sleep","expiration_time":"2027-04-02T00:00:00Z"},
        {"id":"ours","callback_url":"https://oura.example/wearables/webhooks/oura","event_type":"update","data_type":"sleep","expiration_time":"2026-04-02T00:00:00Z"},
        {"id":"other","callback_url":"https://other.example/callback","event_type":"create","data_type":"sleep","expiration_time":"2026-04-01T00:00:00Z"}
    ]))
}

async fn renew(headers: HeaderMap, State(seen): State<Arc<Mutex<Vec<String>>>>) -> Json<Value> {
    assert_eq!(headers["x-client-secret"], "fixture-secret-0123456789");
    seen.lock().await.push("renew".to_owned());
    Json(json!({"id":"ours"}))
}

async fn create(
    headers: HeaderMap,
    State(seen): State<Arc<Mutex<Vec<String>>>>,
    Json(payload): Json<Value>,
) -> impl IntoResponse {
    assert_eq!(headers["x-client-id"], "fixture-client");
    seen.lock()
        .await
        .push(payload["event_type"].as_str().unwrap().to_owned());
    (StatusCode::TOO_MANY_REQUESTS, [("retry-after", "780")])
}
