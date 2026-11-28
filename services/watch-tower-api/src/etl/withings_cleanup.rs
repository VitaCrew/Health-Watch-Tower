use anyhow::{ensure, Context, Result};
use avatar_hsm::etl_adapter::{self, WithingsCleanupAuthorization};
use deadpool_postgres::Pool;
use reqwest::{Client, Response};
use serde_json::Value;
use std::time::Duration;

const ENDPOINT: &str = "https://wbsapi.withings.net/notify";
const MAX_RESPONSE_BYTES: usize = 1024 * 1024;

struct Job {
    subject: String,
    account: Vec<u8>,
    generation: String,
    id: String,
}

async fn claim(pool: &Pool) -> Result<Option<Job>> {
    let db = pool.get().await?;
    db.execute(
        "DELETE FROM watch_tower.withings_notify_cleanup WHERE expires_at<=now()",
        &[],
    )
    .await?;
    let row = db
        .query_opt(
            "WITH due AS (
             SELECT subject_id,account_hash FROM watch_tower.withings_notify_cleanup
             WHERE next_attempt<=now() AND expires_at>now()
               AND (leased_until IS NULL OR leased_until<=now())
             ORDER BY next_attempt LIMIT 1 FOR UPDATE SKIP LOCKED
         )
         UPDATE watch_tower.withings_notify_cleanup q
         SET job_id=gen_random_uuid(),leased_until=now()+interval '2 minutes'
         FROM due
         WHERE (q.subject_id,q.account_hash)=(due.subject_id,due.account_hash)
         RETURNING q.subject_id,q.account_hash,q.generation::text,q.job_id::text",
            &[],
        )
        .await?;
    Ok(row.map(|r| Job {
        subject: r.get(0),
        account: r.get(1),
        generation: r.get(2),
        id: r.get(3),
    }))
}

async fn post(
    client: &Client,
    endpoint: &str,
    token: &str,
    fields: &[(&str, &str)],
) -> Result<Value> {
    let mut response: Response = client
        .post(endpoint)
        .bearer_auth(token)
        .form(fields)
        .send()
        .await
        .map_err(reqwest::Error::without_url)?;
    ensure!(
        response.status().is_success(),
        "Withings notify API returned HTTP {}",
        response.status().as_u16()
    );
    let mut bytes = Vec::new();
    while let Some(chunk) = response
        .chunk()
        .await
        .map_err(reqwest::Error::without_url)?
    {
        ensure!(
            bytes.len() + chunk.len() <= MAX_RESPONSE_BYTES,
            "Withings cleanup response exceeds size limit"
        );
        bytes.extend_from_slice(&chunk);
    }
    let value: Value =
        serde_json::from_slice(&bytes).context("invalid Withings cleanup response")?;
    ensure!(
        value["status"].as_u64() == Some(0),
        "Withings cleanup request failed"
    );
    Ok(value)
}

async fn revoke_existing(
    client: &Client,
    endpoint: &str,
    token: &str,
    callback: &str,
) -> Result<()> {
    let response = post(client, endpoint, token, &[("action", "list")]).await?;
    let profiles = response["body"]["profiles"]
        .as_array()
        .context("invalid Withings subscription list")?;
    let registered: Vec<u16> = profiles
        .iter()
        .filter(|p| p["callbackurl"].as_str() == Some(callback))
        .filter_map(|p| {
            p["appli"]
                .as_u64()
                .and_then(|n| u16::try_from(n).ok())
                .or_else(|| p["appli"].as_str()?.parse().ok())
        })
        .collect();
    for category in registered {
        let category = category.to_string();
        post(
            client,
            endpoint,
            token,
            &[
                ("action", "revoke"),
                ("appli", category.as_str()),
                ("callbackurl", callback),
            ],
        )
        .await?;
    }
    Ok(())
}

async fn finish(pool: &Pool, job: &Job, success: bool) -> Result<()> {
    let db = pool.get().await?;
    if success {
        db.execute(
            "DELETE FROM watch_tower.withings_notify_cleanup
             WHERE subject_id=$1 AND account_hash=$2 AND generation=$3::text::uuid
               AND job_id=$4::text::uuid AND leased_until>now()",
            &[&job.subject, &job.account, &job.generation, &job.id],
        )
        .await?;
    } else {
        db.execute(
            "UPDATE watch_tower.withings_notify_cleanup SET attempts=attempts+1,
                 next_attempt=LEAST(now()+make_interval(secs=>LEAST(60*(attempts+1),900)),expires_at),
                 job_id=NULL,leased_until=NULL
             WHERE subject_id=$1 AND account_hash=$2 AND generation=$3::text::uuid
               AND job_id=$4::text::uuid AND leased_until>now()",
            &[&job.subject,&job.account,&job.generation,&job.id],
        ).await?;
    }
    Ok(())
}

async fn run_one(pool: &Pool, client: &Client, job: &Job) -> Result<()> {
    let auth = etl_adapter::authorize_withings_cleanup(
        pool,
        &job.subject,
        job.account.clone(),
        &job.generation,
        &job.id,
    )
    .await?;
    let success = match auth {
        WithingsCleanupAuthorization::Skip => true,
        WithingsCleanupAuthorization::Ready { token, callback } => tokio::time::timeout(
            Duration::from_secs(35),
            revoke_existing(client, ENDPOINT, &token, &callback),
        )
        .await
        .is_ok_and(|result| result.is_ok()),
    };
    finish(pool, job, success).await
}

pub async fn worker(pool: Pool, client: Client) {
    loop {
        match claim(&pool).await {
            Ok(Some(job)) => {
                let _ = run_one(&pool, &client, &job).await;
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
            Ok(None) | Err(_) => tokio::time::sleep(Duration::from_secs(15)).await,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{
        extract::{Form, State},
        routing::post,
        Json, Router,
    };
    use std::{collections::HashMap, sync::Arc};
    use tokio::sync::Mutex;

    #[tokio::test]
    #[ignore = "requires an isolated PostgreSQL database in WATCH_TOWER_TEST_DATABASE_URL"]
    async fn cleanup_claim_is_exclusive_and_retry_ack_is_durable() -> Result<()> {
        let url = std::env::var("WATCH_TOWER_TEST_DATABASE_URL")?;
        let _policy = crate::store::connect(&url).await?;
        let _hsm = avatar_hsm::store::connect(&url).await?;
        let pool = crate::store::connect(&url).await?;
        let db = pool.get().await?;
        let subject = "withings-cleanup-lease-fixture";
        let account = vec![7_u8; 32];
        let generation = "8d39691f-67df-4f57-88a9-cc4a884e28c7";
        db.execute(
            "INSERT INTO avatar_hsm.subject VALUES($1,'UTC') ON CONFLICT DO NOTHING",
            &[&subject],
        )
        .await?;
        db.execute(
            "INSERT INTO watch_tower.withings_notify_cleanup(subject_id,account_hash,generation,sealed_cleanup,expires_at)
             VALUES($1,$2,$3::text::uuid,decode('01','hex'),now()+interval '10 minutes')",
            &[&subject,&account,&generation],
        ).await?;
        let (first, second) = tokio::join!(claim(&pool), claim(&pool));
        let first = first?;
        let second = second?;
        assert_ne!(first.is_some(), second.is_some());
        let mut job = first.or(second).context("cleanup claim was not acquired")?;
        assert!(claim(&pool).await?.is_none());
        finish(&pool, &job, false).await?;
        let state=db.query_one("SELECT attempts,leased_until IS NULL FROM watch_tower.withings_notify_cleanup WHERE subject_id=$1",&[&subject]).await?;
        assert_eq!(state.get::<_, i32>(0), 1);
        assert!(state.get::<_, bool>(1));
        db.execute(
            "UPDATE watch_tower.withings_notify_cleanup SET next_attempt=now() WHERE subject_id=$1",
            &[&subject],
        )
        .await?;
        job = claim(&pool)
            .await?
            .context("retry claim was not reacquired")?;
        finish(&pool, &job, true).await?;
        assert_eq!(
            db.query_one(
                "SELECT count(*) FROM watch_tower.withings_notify_cleanup WHERE subject_id=$1",
                &[&subject]
            )
            .await?
            .get::<_, i64>(0),
            0
        );
        Ok(())
    }

    #[test]
    fn only_revokes_categories_for_the_exact_callback() {
        let body = serde_json::json!({"body":{"profiles":[
            {"callbackurl":"https://ours.example/hook","appli":16},
            {"callbackurl":"https://other.example/hook","appli":44}
        ]}});
        let profiles = body["body"]["profiles"].as_array().unwrap();
        let ids: Vec<u16> = profiles
            .iter()
            .filter(|p| p["callbackurl"] == "https://ours.example/hook")
            .filter_map(|p| p["appli"].as_u64().and_then(|v| u16::try_from(v).ok()))
            .collect();
        assert_eq!(ids, vec![16]);
    }

    #[tokio::test]
    async fn provider_cleanup_lists_then_revokes_only_its_callback() -> Result<()> {
        let seen = Arc::new(Mutex::new(Vec::<String>::new()));
        let router = Router::new()
            .route(
                "/notify",
                post(
                    |State(seen): State<Arc<Mutex<Vec<String>>>>,
                     Form(form): Form<HashMap<String, String>>| async move {
                        let action = form["action"].clone();
                        seen.lock().await.push(action.clone());
                        if action == "list" {
                            Json(serde_json::json!({"status":0,"body":{"profiles":[
                                {"callbackurl":"https://ours.example/hook","appli":16},
                                {"callbackurl":"https://elsewhere.example/hook","appli":44}
                            ]}}))
                        } else {
                            assert_eq!(form["callbackurl"], "https://ours.example/hook");
                            assert_eq!(form["appli"], "16");
                            Json(serde_json::json!({"status":0,"body":{}}))
                        }
                    },
                ),
            )
            .with_state(seen.clone());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let endpoint = format!("http://{}/notify", listener.local_addr()?);
        let server = tokio::spawn(async move {
            let _ = axum::serve(listener, router).await;
        });
        revoke_existing(
            &Client::new(),
            &endpoint,
            "fixture-token",
            "https://ours.example/hook",
        )
        .await?;
        assert_eq!(*seen.lock().await, ["list", "revoke"]);
        server.abort();
        Ok(())
    }
}
