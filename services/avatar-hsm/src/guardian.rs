//! One consented guardian and one durable notification per synthetic fall.
use std::{env, time::Duration};

use anyhow::{ensure, Result};
use axum::{
    extract::{Path, Query, State},
    http::{header, HeaderMap, StatusCode},
    response::{Html, IntoResponse},
    routing::{get, post},
    Json, Router,
};
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
use deadpool_postgres::{Client, Pool};
use serde::Deserialize;
use serde_json::{json, Value};
use subtle::ConstantTimeEq;
use web_push_native::{
    jwt_simple::algorithms::ES256KeyPair,
    p256::elliptic_curve::sec1::ToEncodedPoint,
    p256::{PublicKey, SecretKey},
    Auth, WebPushBuilder,
};

#[derive(Clone)]
struct Guardian {
    db: Pool,
    token: String,
    private: Vec<u8>,
    public: String,
    subject: String,
}

pub async fn start(db: Pool, forbidden_tokens: &[&str]) -> Result<()> {
    if env::var_os("AVATAR_GUARDIAN_TOKEN").is_none() {
        return Ok(());
    }
    let token = crate::token("AVATAR_GUARDIAN_TOKEN")?;
    ensure!(
        !forbidden_tokens.contains(&token.as_str()),
        "guardian needs a separate credential"
    );
    let private = URL_SAFE_NO_PAD.decode(env::var("AVATAR_GUARDIAN_VAPID_KEY")?)?;
    let public = SecretKey::from_slice(&private)
        .map_err(|_| anyhow::anyhow!("invalid VAPID private key"))?
        .public_key();
    let subject = env::var("AVATAR_GUARDIAN_VAPID_SUBJECT")?;
    let subject_url = reqwest::Url::parse(&subject)?;
    ensure!(
        matches!(subject_url.scheme(), "mailto" | "https"),
        "use a VAPID contact URI"
    );
    let state = Guardian {
        db,
        token,
        private,
        public: URL_SAFE_NO_PAD.encode(public.to_encoded_point(false).as_bytes()),
        subject,
    };
    let port: u16 = env::var("AVATAR_GUARDIAN_PORT")
        .unwrap_or_else(|_| "17841".into())
        .parse()?;
    ensure!(port != 0, "guardian requires a fixed port");
    let listener = tokio::net::TcpListener::bind(("127.0.0.1", port)).await?;
    let router = Router::new()
        .route("/", get(|| async { Html(include_str!("../guardian/index.html")) }))
        .route("/app.js", get(|| async { ([("Content-Type", "text/javascript")], include_str!("../guardian/app.js")) }))
        .route("/sw.js", get(|| async { ([("Content-Type", "text/javascript"), ("Cache-Control", "no-cache")], include_str!("../guardian/sw.js")) }))
        .route("/manifest.webmanifest", get(|| async { ([("Content-Type", "application/manifest+json")], include_str!("../guardian/manifest.webmanifest")) }))
        .route("/api/alerts", get(alerts))
        .route("/api/subscription", post(subscribe))
        .route("/api/alerts/{event}/confirm", post(confirm))
        .layer(axum::middleware::from_fn(|req: axum::extract::Request, next: axum::middleware::Next| async move {
            let mut response = next.run(req).await;
            response.headers_mut().insert(header::CONTENT_SECURITY_POLICY, header::HeaderValue::from_static(
                "default-src 'self'; style-src 'unsafe-inline'; base-uri 'none'; frame-ancestors 'none'; form-action 'none'"
            ));
            response.headers_mut().insert(header::REFERRER_POLICY, header::HeaderValue::from_static("no-referrer"));
            response.headers_mut().insert(header::CACHE_CONTROL, header::HeaderValue::from_static("no-store"));
            response
        }))
        .with_state(state.clone());
    let sender = reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .redirect(reqwest::redirect::Policy::none())
        .build()?;
    tokio::spawn(async move {
        if axum::serve(listener, router).await.is_err() {
            eprintln!("Guardian web server stopped");
        }
    });
    tokio::spawn(deliver(state, sender));
    eprintln!("Guardian web app listening on loopback port {port}");
    Ok(())
}

fn authorized(headers: &HeaderMap, state: &Guardian) -> bool {
    headers
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "))
        .is_some_and(|value| bool::from(value.as_bytes().ct_eq(state.token.as_bytes())))
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Subscription {
    endpoint: String,
    keys: SubscriptionKeys,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SubscriptionKeys {
    p256dh: String,
    auth: String,
}

fn check_subscription(subscription: &Subscription) -> Result<(reqwest::Url, PublicKey, Auth)> {
    let url = reqwest::Url::parse(&subscription.endpoint)?;
    let host = url.host_str().unwrap_or_default();
    ensure!(
        url.scheme() == "https"
            && url.port().is_none()
            && url.username().is_empty()
            && url.password().is_none()
            && url.fragment().is_none()
            && (matches!(
                host,
                "fcm.googleapis.com"
                    | "updates.push.services.mozilla.com"
                    | "autopush.prod.mozaws.net"
                    | "push.apple.com"
            ) || host.ends_with(".push.apple.com"))
            && subscription.endpoint.len() <= 2048,
        "unsupported Web Push endpoint"
    );
    let key = PublicKey::from_sec1_bytes(&URL_SAFE_NO_PAD.decode(&subscription.keys.p256dh)?)
        .map_err(|_| anyhow::anyhow!("invalid subscription public key"))?;
    let auth: [u8; 16] = URL_SAFE_NO_PAD
        .decode(&subscription.keys.auth)?
        .try_into()
        .map_err(|_| anyhow::anyhow!("invalid subscription secret"))?;
    Ok((url, key, Auth::from(auth)))
}

async fn subscribe(
    State(state): State<Guardian>,
    headers: HeaderMap,
    Json(input): Json<Value>,
) -> StatusCode {
    if !authorized(&headers, &state) {
        return StatusCode::UNAUTHORIZED;
    }
    let valid = serde_json::from_value::<Subscription>(input.clone())
        .ok()
        .is_some_and(|input| check_subscription(&input).is_ok());
    if !valid {
        return StatusCode::BAD_REQUEST;
    }
    let Ok(db) = state.db.get().await else {
        return StatusCode::SERVICE_UNAVAILABLE;
    };
    match db
        .execute(
            "INSERT INTO avatar_hsm.guardian_subscription (subscription) VALUES ($1)
         ON CONFLICT (singleton) DO UPDATE SET subscription = excluded.subscription",
            &[&input],
        )
        .await
    {
        Ok(_) => StatusCode::NO_CONTENT,
        Err(_) => StatusCode::SERVICE_UNAVAILABLE,
    }
}

#[derive(Deserialize)]
struct AlertQuery {
    event: Option<String>,
}

async fn alerts(
    State(state): State<Guardian>,
    headers: HeaderMap,
    Query(query): Query<AlertQuery>,
) -> impl IntoResponse {
    if !authorized(&headers, &state) {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    let Ok(db) = state.db.get().await else {
        return StatusCode::SERVICE_UNAVAILABLE.into_response();
    };
    match read_alert(&db, query.event.as_deref()).await {
        Ok(mut value) => {
            value["vapid_public_key"] = json!(state.public);
            Json(value).into_response()
        }
        Err(_) => StatusCode::SERVICE_UNAVAILABLE.into_response(),
    }
}

async fn confirm(
    State(state): State<Guardian>,
    Path(event): Path<String>,
    headers: HeaderMap,
) -> StatusCode {
    if !authorized(&headers, &state) {
        return StatusCode::UNAUTHORIZED;
    }
    let Ok(db) = state.db.get().await else {
        return StatusCode::SERVICE_UNAVAILABLE;
    };
    match db
        .execute(
            "UPDATE avatar_hsm.guardian_alert SET confirmed_at = COALESCE(confirmed_at, now())
         WHERE event_id = $1 AND created_at > now() - interval '1 day'",
            &[&event],
        )
        .await
    {
        Ok(1) => StatusCode::NO_CONTENT,
        Ok(_) => StatusCode::NOT_FOUND,
        Err(_) => StatusCode::SERVICE_UNAVAILABLE,
    }
}

pub async fn status(db: &Client) -> Result<Value> {
    read_alert(db, None).await
}

async fn read_alert(db: &Client, event: Option<&str>) -> Result<Value> {
    let latest = db.query_opt(
        "SELECT to_jsonb(a) FROM avatar_hsm.guardian_alert a WHERE ($1::text IS NULL OR event_id=$1)
         ORDER BY created_at DESC LIMIT 1", &[&event],
    ).await?.map(|row| row.get::<_, Value>(0));
    Ok(json!({"alert": latest}))
}

pub async fn queue(db: &Client, event: &str) -> Result<Value> {
    db.execute(
        "INSERT INTO avatar_hsm.guardian_alert (event_id)
         SELECT event_id FROM avatar_hsm.health_outbox JOIN avatar_hsm.acute_decision USING (event_id)
         WHERE event_id = $1 AND signal = 'synthetic_fall'
           AND created_at > now() - interval '5 minutes'
         ON CONFLICT DO NOTHING", &[&event],
    ).await?;
    let queued = db
        .query_opt(
            "SELECT event_id FROM avatar_hsm.guardian_alert WHERE event_id = $1",
            &[&event],
        )
        .await?
        .is_some();
    Ok(json!({"event_id":event,"queued":queued}))
}

async fn deliver(state: Guardian, http: reqwest::Client) {
    let mut tick = tokio::time::interval(Duration::from_secs(2));
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        tick.tick().await;
        if deliver_one(&state, &http).await.is_err() {
            eprintln!("Guardian push queue temporarily unavailable");
        }
    }
}

async fn deliver_one(state: &Guardian, http: &reqwest::Client) -> Result<()> {
    let db = state.db.get().await?;
    db.execute(
        "UPDATE avatar_hsm.guardian_alert SET notification_state='expired'
         WHERE notification_state IN ('queued','sending') AND created_at <= now() - interval '5 minutes'",
        &[],
    ).await?;
    let Some(row) = db
        .query_opt(
            "SELECT subscription FROM avatar_hsm.guardian_subscription",
            &[],
        )
        .await?
    else {
        return Ok(());
    };
    let Some(event) = db
        .query_opt(
            "UPDATE avatar_hsm.guardian_alert SET notification_state='sending'
         WHERE event_id = (SELECT event_id FROM avatar_hsm.guardian_alert
           WHERE notification_state='queued' AND confirmed_at IS NULL ORDER BY created_at LIMIT 1)
           AND notification_state='queued' AND confirmed_at IS NULL RETURNING event_id",
            &[],
        )
        .await?
    else {
        return Ok(());
    };
    let event: String = event.get(0);
    let status = if send(state, http, row.get(0), &event).await.is_ok() {
        "submitted"
    } else {
        "failed"
    };
    db.execute(
        "UPDATE avatar_hsm.guardian_alert SET notification_state=$2 WHERE event_id=$1",
        &[&event, &status],
    )
    .await?;
    Ok(())
}

async fn send(state: &Guardian, http: &reqwest::Client, input: Value, event: &str) -> Result<()> {
    let subscription: Subscription = serde_json::from_value(input)?;
    let (endpoint, key, auth) = check_subscription(&subscription)?;
    let signing = ES256KeyPair::from_bytes(&state.private)
        .map_err(|_| anyhow::anyhow!("invalid VAPID key"))?;
    let request = WebPushBuilder::new(endpoint.as_str().parse()?, key, auth)
        .with_valid_duration(Duration::from_secs(60))
        .with_vapid(&signing, &state.subject)
        .build(serde_json::to_vec(&json!({"event_id":event}))?)
        .map_err(|_| anyhow::anyhow!("could not build Web Push message"))?;
    let (parts, body) = request.into_parts();
    let result = http
        .post(endpoint)
        .headers(parts.headers)
        .body(body)
        .send()
        .await?;
    ensure!(
        result.status().is_success(),
        "push provider rejected request"
    );
    Ok(())
}
