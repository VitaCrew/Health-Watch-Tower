#[cfg(test)]
use anyhow::Context;
use anyhow::{ensure, Result};
use chrono::{DateTime, Utc};
use deadpool_postgres::Pool;
#[cfg(test)]
use reqwest::StatusCode;
use reqwest::{Client, Url};
use serde_json::Value;
use tokio_postgres::Transaction;

#[cfg(test)]
use crate::{cloud_wearables, garmin_backfill};
use crate::{
    cloud_wearables::Provider, garmin_push, wearable_oauth, wearable_webhooks::hash,
    wearables::Record,
};

#[cfg(test)]
#[path = "garmin_pull_tests.rs"]
mod tests;

pub fn source(feed: &str, raw: &str) -> Result<Url> {
    ensure!(raw.len() <= 4096, "Garmin pull URL exceeds size limit");
    let url = Url::parse(raw).map_err(|_| anyhow::anyhow!("invalid Garmin pull URL"))?;
    let prefix = "/wellness-api/rest/";
    let allowed_path = match feed {
        "allDayRespiration" => ["respiration", "allDayRespiration"],
        "pulseox" => ["pulseOx", "pulseox"],
        "moveIQActivities" => ["moveiq", "moveIQActivities"],
        feed => [feed, feed],
    };
    ensure!(
        url.scheme() == "https"
            && url.host_str() == Some("apis.garmin.com")
            && url.port_or_known_default() == Some(443)
            && url.username().is_empty()
            && url.password().is_none()
            && url.fragment().is_none()
            && url
                .path()
                .strip_prefix(prefix)
                .is_some_and(|path| allowed_path.contains(&path))
            && garmin_push::summary_kind(feed).is_some(),
        "untrusted Garmin pull URL"
    );
    let mut start = None;
    let mut end = None;
    let mut token_seen = false;
    for (key, value) in url.query_pairs() {
        match key.as_ref() {
            "uploadStartTimeInSeconds" if start.is_none() => start = Some(value.parse::<i64>()?),
            "uploadEndTimeInSeconds" if end.is_none() => end = Some(value.parse::<i64>()?),
            "token" if !token_seen && !value.is_empty() => token_seen = true,
            _ => anyhow::bail!("unexpected Garmin pull parameter"),
        }
    }
    ensure!(
        start
            .zip(end)
            .is_some_and(|(start, end)| start > 0 && start < end),
        "invalid Garmin upload interval"
    );
    Ok(url)
}

pub async fn enqueue(
    tx: &Transaction<'_>,
    subject: &str,
    feed: &str,
    permission: i16,
    url: &Url,
) -> Result<()> {
    let fingerprint = hash(url.as_str().as_bytes());
    let encrypted = wearable_oauth::seal_garmin_pull_url(subject, feed, url)?;
    tx.execute("INSERT INTO watch_tower.garmin_pull_job(subject_id,feed,permission,request_hash,sealed_source)
        VALUES($1,$2,$3,$4,$5) ON CONFLICT(subject_id,feed,request_hash) DO NOTHING", &[&subject,&feed,&permission,&fingerprint,&encrypted]).await?;
    Ok(())
}

#[cfg(test)]
struct Job {
    id: String,
    lease: String,
    subject: String,
    feed: String,
    account: Vec<u8>,
    sealed: Vec<u8>,
    request_hash: Vec<u8>,
    received: DateTime<Utc>,
}

pub struct AuthorizedRequest {
    pub url: Url,
    pub token: String,
    pub account: Vec<u8>,
}

pub struct GarminPullRecord {
    pub owner_id: Option<String>,
    pub kind: String,
    pub source: String,
    pub source_id: String,
    pub started_at: DateTime<Utc>,
    pub ended_at: DateTime<Utc>,
    pub modified_at: DateTime<Utc>,
    pub payload: Value,
}

/// HSM validates the live connection and consent, and releases only the
/// short-lived provider request data to Watch Tower.
pub enum Authorization {
    Ready(AuthorizedRequest),
    PermissionRemoved,
    Deferred(DateTime<Utc>),
}

pub async fn authorize(
    pool: &Pool,
    client: &Client,
    id: &str,
    lease: &str,
    subject: &str,
    feed: &str,
    sealed: Vec<u8>,
    request_hash: Vec<u8>,
) -> Result<Authorization> {
    let db = pool.get().await?;
    let row = db.query_opt(
        "SELECT c.provider_account_hash,c.garmin_permissions,c.garmin_retry_after
         FROM watch_tower.garmin_pull_job j
         JOIN avatar_hsm.wearable_oauth_connection c ON c.subject_id=j.subject_id AND c.provider=j.provider
         WHERE j.id::text=$1 AND j.lease::text=$2 AND j.lease_until>now() AND j.status='pending'
           AND j.subject_id=$3 AND j.feed=$4 AND (j.permission & c.garmin_permissions)<>0",
        &[&id, &lease, &subject, &feed],
    ).await?;
    let Some(row) = row else {
        return Ok(Authorization::PermissionRemoved);
    };
    let retry_after: Option<DateTime<Utc>> = row.get(2);
    if let Some(at) = retry_after.filter(|at| *at > Utc::now()) {
        return Ok(Authorization::Deferred(at));
    }
    let account: Vec<u8> = row.get(0);
    let raw = wearable_oauth::open_garmin_pull_url(subject, feed, sealed)?;
    ensure!(
        hash(raw.as_bytes()) == request_hash,
        "Garmin pull request hash mismatch"
    );
    let url = source(feed, &raw)?;
    let credentials = wearable_oauth::access(client, pool, subject, Provider::Garmin).await?;
    Ok(Authorization::Ready(AuthorizedRequest {
        url,
        token: credentials[0].clone(),
        account,
    }))
}

/// Commit is HSM-owned: a fresh lease and consent check guard both the
/// normalized personal records and the queue's terminal state.
pub async fn commit_records(
    pool: &Pool,
    id: &str,
    lease: &str,
    subject: &str,
    feed: &str,
    account: Vec<u8>,
    normalized: Vec<GarminPullRecord>,
) -> Result<bool> {
    let mut records = Vec::with_capacity(normalized.len());
    for record in normalized {
        if let Some(owner) = record.owner_id.as_deref() {
            ensure!(
                hash(owner.as_bytes()) == account,
                "Garmin pull returned another account"
            );
        }
        let record = Record {
            kind: record.kind,
            source: record.source,
            source_id: record.source_id,
            started_at: record.started_at,
            ended_at: record.ended_at,
            modified_at: record.modified_at,
            payload: record.payload,
        };
        records.push(garmin_push::validate_normalized_record(record)?);
    }
    let mut db = pool.get().await?;
    let tx = db.transaction().await?;
    tx.query_one(
        "SELECT pg_advisory_xact_lock(hashtext($1),hashtext('garmin'))",
        &[&subject],
    )
    .await?;
    let updated = tx.execute(
        "UPDATE watch_tower.garmin_pull_job j SET status='stored',sealed_source=NULL,lease=NULL,lease_until=NULL
         FROM avatar_hsm.wearable_oauth_connection c WHERE j.id::text=$1 AND j.lease::text=$2
           AND j.lease_until>now() AND j.status='pending' AND j.subject_id=$4 AND j.feed=$5
           AND c.subject_id=j.subject_id AND c.provider=j.provider
           AND c.provider_account_hash=$3 AND (c.garmin_permissions & j.permission)<>0",
        &[&id, &lease, &account, &subject, &feed],
    ).await?;
    if updated != 0 {
        garmin_push::store_records(&tx, subject, records.iter(), false).await?;
    }
    tx.commit().await?;
    Ok(updated != 0)
}

#[cfg(test)]
async fn claim(pool: &Pool) -> Result<Option<Job>> {
    let db = pool.get().await?;
    let row=db.query_opt("WITH due AS (
        SELECT j.id,c.provider_account_hash FROM watch_tower.garmin_pull_job j
        JOIN avatar_hsm.wearable_oauth_connection c USING(subject_id,provider)
        WHERE j.status='pending' AND j.sealed_source IS NOT NULL AND j.next_attempt<=now()
        AND (c.garmin_retry_after IS NULL OR c.garmin_retry_after<=now())
        AND (j.lease_until IS NULL OR j.lease_until<=now()) AND (j.permission & c.garmin_permissions)<>0
        AND c.provider_account_hash IS NOT NULL
        ORDER BY j.next_attempt LIMIT 1 FOR UPDATE OF j SKIP LOCKED)
        UPDATE watch_tower.garmin_pull_job j SET lease_until=now()+interval '2 minutes',lease=gen_random_uuid()
        FROM due WHERE j.id=due.id
        RETURNING j.id::text,j.lease::text,j.subject_id,j.feed,due.provider_account_hash,j.sealed_source,j.request_hash,j.received_at", &[]).await?;
    Ok(row.map(|row| Job {
        id: row.get(0),
        lease: row.get(1),
        subject: row.get(2),
        feed: row.get(3),
        account: row.get(4),
        sealed: row.get(5),
        request_hash: row.get(6),
        received: row.get(7),
    }))
}

#[cfg(test)]
fn decode_records(
    payload: &Value,
    feed: &str,
    account: &[u8],
    received: DateTime<Utc>,
) -> Result<Vec<Record>> {
    let list = payload
        .as_array()
        .or_else(|| payload[feed].as_array())
        .context("Garmin pull response must contain summaries")?;
    ensure!(
        list.len() <= 2_000,
        "Garmin pull response exceeds record limit"
    );
    let (kind, _) = garmin_push::summary_kind(feed).context("invalid Garmin pull feed")?;
    list.iter()
        .map(|row| {
            if let Some(owner) = row.get("userId") {
                let owner = crate::wearable_webhooks::account_id(owner)
                    .context("invalid Garmin pull owner")?;
                ensure!(
                    hash(owner.as_bytes()) == account,
                    "Garmin pull returned another account"
                );
            }
            garmin_push::decode_record(row, kind, feed, received)
        })
        .collect()
}

#[cfg(test)]
async fn fetch(pool: &Pool, client: &Client, job: &Job) -> Result<Vec<Record>> {
    let url = wearable_oauth::open_garmin_pull_url(&job.subject, &job.feed, job.sealed.clone())?;
    ensure!(
        hash(url.as_bytes()) == job.request_hash,
        "Garmin pull request hash mismatch"
    );
    let url = source(&job.feed, &url)?;
    let credentials = wearable_oauth::access(client, pool, &job.subject, Provider::Garmin).await?;
    let response = client
        .get(url)
        .bearer_auth(&credentials[0])
        .send()
        .await
        .map_err(|err| err.without_url())?;
    if response.status() == StatusCode::NO_CONTENT {
        return Ok(Vec::new());
    }
    let response = cloud_wearables::body(response).await?;
    decode_records(&response, &job.feed, &job.account, job.received)
}

#[cfg(test)]
async fn save(pool: &Pool, job: &Job, records: &[Record]) -> Result<()> {
    let mut db = pool.get().await?;
    let tx = db.transaction().await?;
    tx.query_one(
        "SELECT pg_advisory_xact_lock(hashtext($1),hashtext('garmin'))",
        &[&job.subject],
    )
    .await?;
    let updated=tx.execute("UPDATE watch_tower.garmin_pull_job j SET status='stored',sealed_source=NULL,lease=NULL,lease_until=NULL
        FROM avatar_hsm.wearable_oauth_connection c WHERE j.id=$1::text::uuid AND j.lease=$2::text::uuid
        AND j.lease_until>now() AND j.status='pending' AND c.subject_id=j.subject_id AND c.provider=j.provider
        AND c.provider_account_hash=$3 AND (c.garmin_permissions & j.permission)<>0", &[&job.id,&job.lease,&job.account]).await?;
    if updated != 0 {
        garmin_push::store_records(&tx, &job.subject, records.iter(), false).await?;
    }
    tx.commit().await?;
    Ok(())
}

#[cfg(test)]
async fn retry(pool: &Pool, job: &Job, error: &anyhow::Error) -> Result<()> {
    let retry_at = error
        .downcast_ref::<cloud_wearables::ProviderThrottled>()
        .map(|throttle| throttle.retry_at);
    let mut db = pool.get().await?;
    let tx = db.transaction().await?;
    if retry_at.is_some() {
        garmin_backfill::lock_account(&tx, &job.subject).await?;
    }
    let updated = tx
        .execute(
            "UPDATE watch_tower.garmin_pull_job SET
        attempts=attempts+CASE WHEN $3::timestamptz IS NULL THEN 1 ELSE 0 END,
        status=CASE WHEN $3 IS NULL AND attempts>=5 THEN 'unavailable' ELSE 'pending' END,
        sealed_source=CASE WHEN $3 IS NULL AND attempts>=5 THEN NULL ELSE sealed_source END,
        lease=NULL,lease_until=NULL,
        next_attempt=COALESCE($3,now()+make_interval(mins=>LEAST(60,2*(attempts+1))))
        WHERE id=$1::text::uuid AND lease=$2::text::uuid",
            &[&job.id, &job.lease, &retry_at],
        )
        .await?;
    if let (1, Some(retry_at)) = (updated, retry_at) {
        garmin_backfill::defer_account(&tx, &job.subject, retry_at).await?;
    }
    tx.commit().await?;
    Ok(())
}

#[cfg(test)]
pub async fn worker(pool: Pool, client: Client) {
    loop {
        let pause = if let Ok(Some(job)) = claim(&pool).await {
            let saved = match fetch(&pool, &client, &job).await {
                Ok(records) => save(&pool, &job, &records).await,
                Err(err) => Err(err),
            };
            if let Err(error) = saved {
                let _ = retry(&pool, &job, &error).await;
            }
            1
        } else {
            10
        };
        tokio::time::sleep(std::time::Duration::from_secs(pause)).await;
    }
}
