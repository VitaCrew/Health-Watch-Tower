#[cfg(test)]
use crate::cloud_wearables;
use crate::{cloud_wearables::Provider, wearable_oauth, wearable_webhooks};
use anyhow::{ensure, Context, Result};
use deadpool_postgres::Pool;
use reqwest::{Client, Url};
#[cfg(test)]
use serde_json::Value;
#[cfg(test)]
use std::time::Duration;
use std::{collections::HashSet, env};

#[cfg(test)]
#[path = "withings_notify_tests.rs"]
mod tests;

#[cfg(test)]
pub(crate) const ENDPOINT: &str = "https://wbsapi.withings.net/notify";
const PROVIDER: &str = "withings";
pub(crate) const METRIC_CATEGORIES: [u16; 7] = [1, 2, 4, 54, 58, 62, 63];
pub(crate) const ACTIVITY_CATEGORIES: [u16; 2] = [16, 44];

pub struct WatchTowerTask {
    pub subject: String,
    pub generation: String,
    pub callback: String,
    pub token: String,
    pub categories: Vec<u16>,
    config: Vec<u8>,
}

fn callback(base: &str, key: &str) -> Option<Url> {
    let mut url = Url::parse(base).ok()?;
    let host = url.domain()?.trim_end_matches('.');
    if url.scheme() != "https"
        || !host.contains('.')
        || host.ends_with(".localhost")
        || !matches!(url.port(), None | Some(443 | 80))
        || url.path() != "/"
        || url.query().is_some()
        || url.fragment().is_some()
        || !url.username().is_empty()
        || url.password().is_some()
        || !(43..=128).contains(&key.len())
        || !key
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_'))
    {
        return None;
    }
    url.set_path(&format!("/wearables/webhooks/withings/{key}"));
    (url.as_str().len() <= 255).then_some(url)
}

pub(crate) fn configured_callback() -> Option<Url> {
    callback(
        &env::var("AVATAR_WEARABLE_PUBLIC_URL").ok()?,
        &env::var("AVATAR_WITHINGS_WEBHOOK_PATH_KEY").ok()?,
    )
}

fn categories(scope: &str) -> Vec<u16> {
    let scopes: HashSet<_> = scope
        .split(|ch: char| ch.is_ascii_whitespace() || ch == ',')
        .collect();
    let mut categories = Vec::new();
    if scopes.contains("user.metrics") {
        categories.extend(METRIC_CATEGORIES);
    }
    if scopes.contains("user.activity") {
        categories.extend(ACTIVITY_CATEGORIES);
    }
    categories
}

#[cfg(test)]
pub(crate) async fn notify(
    client: &Client,
    endpoint: &str,
    token: &str,
    fields: &[(&str, &str)],
) -> Result<Value> {
    let result = cloud_wearables::body(
        client
            .post(endpoint)
            .bearer_auth(token)
            .form(fields)
            .send()
            .await?,
    )
    .await?;
    ensure!(
        result["status"].as_u64() == Some(0),
        "Withings notification request failed"
    );
    Ok(result)
}

#[cfg(test)]
pub(crate) async fn registered_categories(
    client: &Client,
    endpoint: &str,
    token: &str,
    callback: &str,
) -> Result<HashSet<u16>> {
    let result = notify(client, endpoint, token, &[("action", "list")]).await?;
    let profiles = result["body"]["profiles"]
        .as_array()
        .context("invalid Withings subscription list")?;
    Ok(profiles
        .iter()
        .filter(|profile| profile["callbackurl"].as_str() == Some(callback))
        .filter_map(|profile| {
            profile["appli"]
                .as_u64()
                .and_then(|v| u16::try_from(v).ok())
                .or_else(|| profile["appli"].as_str()?.parse().ok())
        })
        .collect())
}

#[cfg(test)]
async fn subscribe_missing(
    client: &Client,
    endpoint: &str,
    token: &str,
    callback: &Url,
    wanted: &[u16],
) -> Result<()> {
    let registered = registered_categories(client, endpoint, token, callback.as_str()).await?;
    let mut all_registered = true;
    for category in wanted.iter().filter(|value| !registered.contains(value)) {
        all_registered &= notify(
            client,
            endpoint,
            token,
            &[
                ("action", "subscribe"),
                ("appli", &category.to_string()),
                ("callbackurl", callback.as_str()),
            ],
        )
        .await
        .is_ok();
    }
    ensure!(
        all_registered,
        "some Withings notification subscriptions need retry"
    );
    Ok(())
}

pub(crate) async fn lock_account(
    tx: &tokio_postgres::Transaction<'_>,
    account: &[u8],
) -> Result<()> {
    tx.query_one(
        "SELECT pg_advisory_xact_lock(hashtext('withings-account'),hashtext(encode($1::bytea,'hex')))",
        &[&account],
    ).await?;
    Ok(())
}

async fn next_account(pool: &Pool, config: &[u8]) -> Result<Option<(String, String)>> {
    Ok(pool
        .get()
        .await?
        .query_opt(
            "SELECT subject_id,generation::text FROM avatar_hsm.wearable_oauth_connection
         WHERE provider='withings' AND provider_account_hash IS NOT NULL
           AND (withings_notify_config IS DISTINCT FROM $1 OR withings_notify_next<=now())
         ORDER BY withings_notify_next,subject_id LIMIT 1",
            &[&config],
        )
        .await?
        .map(|row| (row.get(0), row.get(1))))
}

#[cfg(test)]
async fn reconcile(
    pool: &Pool,
    client: &Client,
    endpoint: &str,
    (subject, generation): (&str, &str),
    callback: &Url,
    config: &[u8],
    credentials: Result<Vec<String>>,
) -> Result<()> {
    let mut db = pool.get().await?;
    let tx = db.transaction().await?;
    // Use the same lock as local unlink and OAuth callback.
    let locked: bool = tx
        .query_one(
            "SELECT pg_try_advisory_xact_lock(hashtext($1),hashtext($2))",
            &[&subject, &PROVIDER],
        )
        .await?
        .get(0);
    if !locked {
        return Ok(());
    }
    let current = tx
        .query_opt(
            "SELECT provider_account_hash FROM avatar_hsm.wearable_oauth_connection
         WHERE subject_id=$1 AND provider='withings' AND generation=$2::text::uuid
           AND provider_account_hash IS NOT NULL
           AND (withings_notify_config IS DISTINCT FROM $3 OR withings_notify_next<=now())",
            &[&subject, &generation, &config],
        )
        .await?;
    let Some(current) = current else {
        return Ok(());
    };
    lock_account(&tx, &current.get::<_, Vec<u8>>(0)).await?;
    let state = if let Ok(credentials) = credentials {
        let wanted = categories(credentials.get(1).map_or("", String::as_str));
        if wanted.is_empty() {
            "no_scope"
        } else if matches!(
            tokio::time::timeout(
                Duration::from_secs(35),
                subscribe_missing(client, endpoint, &credentials[0], callback, &wanted)
            )
            .await,
            Ok(Ok(()))
        ) {
            "active"
        } else {
            "retrying"
        }
    } else {
        "retrying"
    };
    tx.execute(
        "UPDATE avatar_hsm.wearable_oauth_connection SET withings_notify_config=$3,
             withings_notify_state=$4,
             withings_notify_next=now()+ CASE WHEN $4='retrying' THEN interval '15 minutes' ELSE interval '1 day' END
         WHERE subject_id=$1 AND provider='withings' AND generation=$2::text::uuid",
        &[&subject,&generation,&config,&state],
    ).await?;
    tx.commit().await?;
    Ok(())
}

#[cfg(test)]
pub async fn worker(pool: Pool, client: Client) {
    let Some(callback) = configured_callback() else {
        return;
    };
    let config = wearable_webhooks::hash(callback.as_str().as_bytes());
    loop {
        if let Ok(Some((subject, generation))) = next_account(&pool, &config).await {
            let credentials =
                wearable_oauth::access(&client, &pool, &subject, Provider::Withings).await;
            let _ = reconcile(
                &pool,
                &client,
                ENDPOINT,
                (&subject, &generation),
                &callback,
                &config,
                credentials,
            )
            .await;
            tokio::time::sleep(Duration::from_secs(1)).await;
        } else {
            tokio::time::sleep(Duration::from_secs(15)).await;
        }
    }
}

/// Prepare one notification-subscription run. HSM selects a currently due
/// connection and opens its credential; Watch Tower performs provider I/O.
pub async fn prepare_watch_tower(pool: &Pool, client: &Client) -> Result<Option<WatchTowerTask>> {
    let Some(callback) = configured_callback() else {
        return Ok(None);
    };
    let config = wearable_webhooks::hash(callback.as_str().as_bytes());
    let Some((subject, generation)) = next_account(pool, &config).await? else {
        return Ok(None);
    };
    let credentials = wearable_oauth::access(client, pool, &subject, Provider::Withings).await?;
    let token = credentials
        .first()
        .cloned()
        .context("Withings token missing")?;
    Ok(Some(WatchTowerTask {
        subject,
        generation,
        callback: callback.to_string(),
        token,
        categories: categories(credentials.get(1).map_or("", String::as_str))
            .into_iter()
            .collect(),
        config,
    }))
}

/// Commit only the result of the exact connection/configuration that Watch
/// Tower processed. A reconnect or callback change makes the result stale.
pub async fn finish_watch_tower(pool: &Pool, task: &WatchTowerTask, result: &str) -> Result<bool> {
    ensure!(
        matches!(result, "active" | "no_scope" | "retrying"),
        "invalid Withings subscription result"
    );
    let mut db = pool.get().await?;
    let tx = db.transaction().await?;
    let locked: bool = tx
        .query_one(
            "SELECT pg_try_advisory_xact_lock(hashtext($1),hashtext($2))",
            &[&task.subject, &PROVIDER],
        )
        .await?
        .get(0);
    if !locked {
        return Ok(false);
    }
    let Some(row) = tx
        .query_opt(
            "SELECT provider_account_hash FROM avatar_hsm.wearable_oauth_connection
         WHERE subject_id=$1 AND provider='withings' AND generation=$2::text::uuid
           AND provider_account_hash IS NOT NULL
           AND (withings_notify_config IS DISTINCT FROM $3 OR withings_notify_next<=now())",
            &[&task.subject, &task.generation, &task.config],
        )
        .await?
    else {
        return Ok(false);
    };
    lock_account(&tx, &row.get::<_, Vec<u8>>(0)).await?;
    tx.execute(
        "UPDATE avatar_hsm.wearable_oauth_connection SET withings_notify_config=$3,
             withings_notify_state=$4,
             withings_notify_next=now()+ CASE WHEN $4='retrying' THEN interval '15 minutes' ELSE interval '1 day' END
         WHERE subject_id=$1 AND provider='withings' AND generation=$2::text::uuid",
        &[&task.subject, &task.generation, &task.config, &result],
    ).await?;
    tx.commit().await?;
    Ok(true)
}

pub async fn status(pool: &Pool, subject: &str) -> Result<&'static str> {
    let Some(callback) = configured_callback() else {
        return Ok("not_configured");
    };
    let config = wearable_webhooks::hash(callback.as_str().as_bytes());
    let row = pool
        .get()
        .await?
        .query_opt(
            "SELECT CASE WHEN provider_account_hash IS NULL THEN 'reconnect_required'
                WHEN withings_notify_config IS DISTINCT FROM $2 THEN 'pending'
                ELSE withings_notify_state END
         FROM avatar_hsm.wearable_oauth_connection WHERE subject_id=$1 AND provider='withings'",
            &[&subject, &config],
        )
        .await?;
    Ok(match row.map(|row| row.get::<_, String>(0)).as_deref() {
        None => "not_connected",
        Some("reconnect_required") => "reconnect_required",
        Some("active") => "active",
        Some("retrying") => "retrying",
        Some("no_scope") => "no_scope",
        _ => "pending",
    })
}
