use crate::{wearable_oauth, withings_notify};
use anyhow::Result;
use deadpool_postgres::Pool;
#[cfg(test)]
use reqwest::Client;
#[cfg(test)]
use std::time::Duration;
use tokio_postgres::Transaction;

#[cfg(test)]
#[path = "withings_cleanup_tests.rs"]
mod tests;

pub async fn queue(tx: &Transaction<'_>, subject: &str) -> Result<()> {
    let Some(callback) = withings_notify::configured_callback() else {
        return Ok(());
    };
    queue_for_callback(tx, subject, &callback).await
}

async fn queue_for_callback(
    tx: &Transaction<'_>,
    subject: &str,
    callback: &reqwest::Url,
) -> Result<()> {
    let Some(row) = tx
        .query_opt(
            "SELECT generation::text,provider_account_hash,sealed_tokens
         FROM avatar_hsm.wearable_oauth_connection WHERE subject_id=$1 AND provider='withings'
            AND provider_account_hash IS NOT NULL",
            &[&subject],
        )
        .await?
    else {
        return Ok(());
    };
    let generation: String = row.get(0);
    let account: Vec<u8> = row.get(1);
    // Local unlink works even when older credentials have expired or cannot be opened.
    let Ok(Some((sealed, expires))) = wearable_oauth::withings_cleanup_payload(
        subject,
        &generation,
        &account,
        row.get(2),
        callback,
    ) else {
        return Ok(());
    };
    withings_notify::lock_account(tx, &account).await?;
    tx.execute(
        "INSERT INTO watch_tower.withings_notify_cleanup(subject_id,account_hash,generation,sealed_cleanup,expires_at)
         VALUES($1,$2,$3::text::uuid,$4,LEAST($5::timestamptz,clock_timestamp()+interval '30 minutes'))
         ON CONFLICT(subject_id,account_hash) DO UPDATE SET generation=excluded.generation,
             sealed_cleanup=excluded.sealed_cleanup,expires_at=excluded.expires_at,next_attempt=now(),
             job_id=NULL,leased_until=NULL,attempts=0",
        &[&subject,&account,&generation,&sealed,&expires],
    ).await?;
    Ok(())
}

struct Job {
    subject: String,
    account: Vec<u8>,
    generation: String,
}

pub enum WatchTowerAuthorization {
    Ready { token: String, callback: String },
    Skip,
}

/// Open the short-lived cleanup payload only for a live Watch Tower lease.
/// OAuth and account ownership remain HSM decisions; provider HTTP is outside.
pub async fn authorize_watch_tower(
    pool: &Pool,
    subject: &str,
    account: &[u8],
    generation: &str,
    job_id: &str,
) -> Result<WatchTowerAuthorization> {
    let mut db = pool.get().await?;
    let tx = db.transaction().await?;
    withings_notify::lock_account(&tx, account).await?;
    let row = tx
        .query_opt(
            "SELECT q.sealed_cleanup,
                EXISTS(SELECT 1 FROM avatar_hsm.wearable_oauth_connection c
                       WHERE c.provider='withings' AND c.provider_account_hash=q.account_hash)
         FROM watch_tower.withings_notify_cleanup q
         WHERE q.subject_id=$1 AND q.account_hash=$2 AND q.generation=$3::text::uuid
           AND q.job_id=$4::text::uuid AND q.leased_until>now() AND q.expires_at>now()
         FOR UPDATE OF q",
            &[&subject, &account, &generation, &job_id],
        )
        .await?;
    let result = match row {
        None => WatchTowerAuthorization::Skip,
        Some(row) if row.get::<_, bool>(1) => WatchTowerAuthorization::Skip,
        Some(row) => {
            match wearable_oauth::open_withings_cleanup(subject, generation, account, row.get(0)) {
                Ok((token, callback)) => WatchTowerAuthorization::Ready { token, callback },
                Err(_) => WatchTowerAuthorization::Skip,
            }
        }
    };
    tx.commit().await?;
    Ok(result)
}

#[cfg(test)]
async fn next(pool: &Pool) -> Result<Option<Job>> {
    let db = pool.get().await?;
    db.execute(
        "DELETE FROM watch_tower.withings_notify_cleanup WHERE expires_at<=now()",
        &[],
    )
    .await?;
    Ok(db.query_opt(
        "SELECT subject_id,account_hash,generation::text FROM watch_tower.withings_notify_cleanup
         WHERE next_attempt<=now() AND expires_at>now() ORDER BY next_attempt LIMIT 1",
        &[],
    ).await?.map(|row| Job {subject:row.get(0),account:row.get(1),generation:row.get(2)}))
}

#[cfg(test)]
async fn revoke_existing(
    client: &Client,
    endpoint: &str,
    token: &str,
    callback: &str,
) -> Result<()> {
    let registered =
        withings_notify::registered_categories(client, endpoint, token, callback).await?;
    let mut all_revoked = true;
    for category in withings_notify::METRIC_CATEGORIES
        .into_iter()
        .chain(withings_notify::ACTIVITY_CATEGORIES)
        .filter(|value| registered.contains(value))
    {
        all_revoked &= withings_notify::notify(
            client,
            endpoint,
            token,
            &[
                ("action", "revoke"),
                ("appli", &category.to_string()),
                ("callbackurl", callback),
            ],
        )
        .await
        .is_ok();
    }
    anyhow::ensure!(
        all_revoked,
        "some Withings notification subscriptions need cleanup retry"
    );
    Ok(())
}

#[cfg(test)]
async fn run(pool: &Pool, client: &Client, endpoint: &str, job: &Job) -> Result<()> {
    let mut db = pool.get().await?;
    let tx = db.transaction().await?;
    withings_notify::lock_account(&tx, &job.account).await?;
    let Some(row) = tx
        .query_opt(
            "SELECT sealed_cleanup, EXISTS(SELECT 1 FROM avatar_hsm.wearable_oauth_connection c
             WHERE c.provider='withings' AND c.provider_account_hash=$2)
         FROM watch_tower.withings_notify_cleanup
         WHERE subject_id=$1 AND account_hash=$2 AND generation=$3::text::uuid
           AND next_attempt<=now() AND expires_at>now()",
            &[&job.subject, &job.account, &job.generation],
        )
        .await?
    else {
        return Ok(());
    };
    let mut done: bool = row.get(1);
    if !done {
        // Discard sealed data we can no longer decrypt.
        done = match wearable_oauth::open_withings_cleanup(
            &job.subject,
            &job.generation,
            &job.account,
            row.get(0),
        ) {
            Ok((token, callback)) => matches!(
                tokio::time::timeout(
                    Duration::from_secs(35),
                    revoke_existing(client, endpoint, &token, &callback)
                )
                .await,
                Ok(Ok(()))
            ),
            Err(_) => true,
        };
    }
    let sql = if done {
        "DELETE FROM watch_tower.withings_notify_cleanup
         WHERE subject_id=$1 AND account_hash=$2 AND generation=$3::text::uuid"
    } else {
        "UPDATE watch_tower.withings_notify_cleanup SET next_attempt=now()+interval '1 minute'
         WHERE subject_id=$1 AND account_hash=$2 AND generation=$3::text::uuid"
    };
    tx.execute(sql, &[&job.subject, &job.account, &job.generation])
        .await?;
    tx.commit().await?;
    Ok(())
}

#[cfg(test)]
pub async fn worker(pool: Pool, client: Client) {
    loop {
        if let Ok(Some(job)) = next(&pool).await {
            let _ = run(&pool, &client, withings_notify::ENDPOINT, &job).await;
            tokio::time::sleep(Duration::from_secs(1)).await;
        } else {
            tokio::time::sleep(Duration::from_secs(15)).await;
        }
    }
}
