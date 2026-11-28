use anyhow::{ensure, Result};
use chrono::{DateTime, Utc};
use deadpool_postgres::Pool;
use reqwest::{Client, Method, Response, StatusCode, Url};
use serde::Deserialize;
use serde_json::{json, Value};
use std::{collections::HashMap, env, time::Duration};

const ENDPOINT: &str = "https://api.ouraring.com/v2/webhook/subscription";
const EVENTS: [&str; 3] = ["create", "update", "delete"];
const SUCCESS_INTERVAL: Duration = Duration::from_secs(3600);
const RETRY_INTERVAL: Duration = Duration::from_secs(300);

#[cfg(test)]
#[path = "oura_subscriptions_tests.rs"]
mod tests;

struct Config {
    client_id: String,
    secret: String,
    verify: String,
    callback: Url,
}

fn callback(base: &str) -> Option<Url> {
    let mut url = Url::parse(base).ok()?;
    let domain = url.domain()?.trim_end_matches('.');
    if url.scheme() != "https"
        || !domain.contains('.')
        || domain.ends_with(".localhost")
        || !url.username().is_empty()
        || url.password().is_some()
        || url.path() != "/"
        || url.query().is_some()
        || url.fragment().is_some()
    {
        return None;
    }
    url.set_path("/wearables/webhooks/oura");
    Some(url)
}

fn configured() -> Option<Config> {
    let client_id = env::var("AVATAR_OURA_CLIENT_ID").ok()?;
    let secret = env::var("AVATAR_OURA_CLIENT_SECRET").ok()?;
    let verify = env::var("AVATAR_OURA_WEBHOOK_VERIFY_TOKEN").ok()?;
    if client_id.trim().is_empty() || secret.len() < 16 || verify.len() < 16 {
        return None;
    }
    Some(Config {
        client_id,
        secret,
        verify,
        callback: callback(&env::var("AVATAR_WEARABLE_PUBLIC_URL").ok()?)?,
    })
}

#[derive(Deserialize)]
struct Subscription {
    id: String,
    callback_url: String,
    event_type: String,
    data_type: String,
    expiration_time: DateTime<Utc>,
}

enum Reply {
    Success(Value),
    RateLimited(Duration),
}

async fn request(
    client: &Client,
    config: &Config,
    method: Method,
    url: &str,
    payload: Option<Value>,
) -> Result<Reply> {
    let mut request = client
        .request(method, url)
        .header("x-client-id", &config.client_id)
        .header("x-client-secret", &config.secret);
    if let Some(payload) = payload {
        request = request.json(&payload);
    }
    let response = request.send().await.map_err(reqwest::Error::without_url)?;
    if response.status() == StatusCode::TOO_MANY_REQUESTS {
        let seconds = response
            .headers()
            .get("retry-after")
            .and_then(|h| h.to_str().ok())
            .and_then(|value| value.parse::<u64>().ok())
            .unwrap_or(RETRY_INTERVAL.as_secs());
        return Ok(Reply::RateLimited(Duration::from_secs(
            seconds.clamp(300, 86400),
        )));
    }
    Ok(Reply::Success(response_json(response).await?))
}

async fn response_json(response: Response) -> Result<Value> {
    ensure!(
        response.status().is_success(),
        "Oura subscription API returned HTTP {}",
        response.status().as_u16()
    );
    let mut response = response;
    let mut bytes = Vec::new();
    while let Some(chunk) = response
        .chunk()
        .await
        .map_err(reqwest::Error::without_url)?
    {
        ensure!(
            bytes.len() + chunk.len() <= 1024 * 1024,
            "Oura subscription response exceeds limit"
        );
        bytes.extend_from_slice(&chunk);
    }
    Ok(serde_json::from_slice(&bytes)?)
}

async fn reconcile(
    client: &Client,
    config: &Config,
    endpoint: &str,
    types: &[&str],
    now: DateTime<Utc>,
) -> Result<Duration> {
    let listings: Vec<Subscription> =
        match request(client, config, Method::GET, endpoint, None).await? {
            Reply::Success(data) => serde_json::from_value(data)?,
            Reply::RateLimited(after) => return Ok(after),
        };
    let renew_before = now + chrono::Duration::days(2);
    let mut existing: HashMap<(&str, &str), &Subscription> = HashMap::new();
    for entry in listings
        .iter()
        .filter(|entry| entry.callback_url == config.callback.as_str())
    {
        let pair = (entry.data_type.as_str(), entry.event_type.as_str());
        if existing
            .get(&pair)
            .is_none_or(|other| other.expiration_time < entry.expiration_time)
        {
            existing.insert(pair, entry);
        }
    }
    let mut wait = SUCCESS_INTERVAL;
    for kind in types {
        for event in EVENTS {
            let mutation =
                if let Some(subscription) = existing.get(&(*kind, event)) {
                    if subscription.expiration_time > renew_before {
                        continue;
                    }
                    ensure!(
                        subscription.id.len() <= 128
                            && subscription
                                .id
                                .bytes()
                                .all(|b| b.is_ascii_alphanumeric() || b"-_".contains(&b)),
                        "invalid Oura subscription ID"
                    );
                    request(
                        client,
                        config,
                        Method::PUT,
                        &format!("{endpoint}/renew/{}", subscription.id),
                        None,
                    )
                    .await
                } else {
                    request(client, config, Method::POST, endpoint, Some(json!({
                    "callback_url":config.callback.as_str(), "verification_token":config.verify,
                    "event_type":event, "data_type":kind,
                }))).await
                };
            match mutation {
                Ok(Reply::Success(_)) => {}
                Ok(Reply::RateLimited(after)) => return Ok(after),
                Err(_) => wait = RETRY_INTERVAL,
            }
        }
    }
    Ok(wait)
}

async fn check(pool: &Pool, client: &Client, config: &Config) -> Result<Duration> {
    let mut db = pool.get().await?;
    let tx = db.transaction().await?;
    let locked: bool = tx
        .query_one(
            "SELECT pg_try_advisory_xact_lock(hashtext('oura-webhook-subscriptions'),hashtext($1))",
            &[&config.client_id],
        )
        .await?
        .get(0);
    if !locked {
        return Ok(RETRY_INTERVAL);
    }
    let wait = tokio::time::timeout(
        Duration::from_secs(90),
        reconcile(
            client,
            config,
            ENDPOINT,
            &avatar_hsm::etl_adapter::oura_webhook_types(),
            Utc::now(),
        ),
    )
    .await??;
    tx.commit().await?;
    Ok(wait)
}

pub async fn worker(pool: Pool, client: Client) {
    let Some(config) = configured() else {
        return;
    };
    loop {
        let wait = check(&pool, &client, &config)
            .await
            .unwrap_or(RETRY_INTERVAL);
        tokio::time::sleep(wait).await;
    }
}
