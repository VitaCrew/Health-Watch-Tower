use super::*;
use anyhow::Context;
use axum::{
    extract::State,
    http::{HeaderMap, StatusCode},
    routing::{get, put},
    Json, Router,
};
use std::sync::Arc;
use tokio::sync::Mutex;

const CALLBACK: &str = "https://our-fixture.example/wearables/webhooks/oura";

fn config() -> Result<Config> {
    Ok(Config {
        client_id: "synthetic-client-id".into(),
        secret: "synthetic-client-secret".into(),
        verify: "synthetic-verification-token".into(),
        callback: callback("https://our-fixture.example").context("missing callback")?,
    })
}

fn assert_auth(headers: &HeaderMap) {
    assert_eq!(headers["x-client-id"], "synthetic-client-id");
    assert_eq!(headers["x-client-secret"], "synthetic-client-secret");
}

#[tokio::test]
async fn registration_renews_only_matching_expiring_subscriptions() -> Result<()> {
    let config = config()?;
    assert_eq!(config.callback.as_str(), CALLBACK);
    assert!(callback("https://localhost/").is_none());
    assert!(callback("http://our-fixture.example/").is_none());
    let calls = Arc::new(Mutex::new(Vec::<String>::new()));
    let router = Router::new()
        .route("/subscription",get(|headers:HeaderMap,State(calls):State<Arc<Mutex<Vec<String>>>>|async move {
            assert_auth(&headers);
            let mut calls = calls.lock().await;
            let completed = !calls.is_empty();
            calls.push("list".into());
            let mut list = vec![
                json!({"id":"fresh","callback_url":CALLBACK,"event_type":"create","data_type":"sleep","expiration_time":"2026-06-01T00:00:00Z"}),
                json!({"id":"expiring","callback_url":CALLBACK,"event_type":"update","data_type":"sleep",
                    "expiration_time":if completed {"2026-06-01T00:00:00Z"} else {"2026-04-02T00:00:00Z"}}),
                json!({"id":"unrelated","callback_url":"https://other.example/callback","event_type":"delete",
                    "data_type":"sleep","expiration_time":"2026-04-01T00:00:00Z"}),
            ];
            if completed {
                list.push(json!({"id":"created","callback_url":CALLBACK,"event_type":"delete","data_type":"sleep",
                    "expiration_time":"2026-06-01T00:00:00Z"}));
            }
            Json(list)
        }).post(|headers:HeaderMap,State(calls):State<Arc<Mutex<Vec<String>>>>,Json(payload):Json<Value>|async move {
            assert_auth(&headers);
            assert_eq!(payload,json!({"callback_url":CALLBACK,"verification_token":"synthetic-verification-token",
                "event_type":"delete","data_type":"sleep"}));
            calls.lock().await.push("create-delete".into());
            (StatusCode::CREATED,Json(json!({"id":"created"})))
        }))
        .route("/subscription/renew/expiring",put(|headers:HeaderMap,State(calls):State<Arc<Mutex<Vec<String>>>>|async move {
            assert_auth(&headers);
            calls.lock().await.push("renew".into());
            Json(json!({"id":"expiring"}))
        })).with_state(calls.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let endpoint = format!("http://{}/subscription", listener.local_addr()?);
    let server = tokio::spawn(async move { axum::serve(listener, router).await });
    let now = "2026-04-01T00:00:00Z".parse()?;
    let client = cloud_wearables::http_client()?;
    for _ in 0..2 {
        assert_eq!(
            reconcile(&client, &config, &endpoint, &["sleep"], now).await?,
            SUCCESS_INTERVAL
        );
    }
    assert_eq!(
        *calls.lock().await,
        ["list", "renew", "create-delete", "list"]
    );
    server.abort();
    Ok(())
}

#[tokio::test]
async fn rate_limited_registration_stops_remaining_requests() -> Result<()> {
    let calls = Arc::new(Mutex::new(Vec::<String>::new()));
    let router=Router::new().route("/subscription",get(||async{Json(json!([]))}).post(
        |State(calls):State<Arc<Mutex<Vec<String>>>>,Json(payload):Json<Value>|async move {
            calls.lock().await.push(payload["event_type"].as_str().unwrap().to_owned());
            (StatusCode::TOO_MANY_REQUESTS,[("retry-after","780")])
        })).with_state(calls.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let endpoint = format!("http://{}/subscription", listener.local_addr()?);
    let server = tokio::spawn(async move { axum::serve(listener, router).await });
    let wait = reconcile(
        &cloud_wearables::http_client()?,
        &config()?,
        &endpoint,
        &["sleep", "workout"],
        Utc::now(),
    )
    .await?;
    assert_eq!(wait, Duration::from_secs(780));
    assert_eq!(*calls.lock().await, ["create"]);
    server.abort();
    Ok(())
}
