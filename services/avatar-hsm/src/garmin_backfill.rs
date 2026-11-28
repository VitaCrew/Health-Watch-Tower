use anyhow::{ensure, Context, Result};
use chrono::{DateTime, Duration, Utc};
use deadpool_postgres::Pool;
use reqwest::Client;
#[cfg(test)]
use reqwest::StatusCode;
use serde_json::{json, Value};
use tokio_postgres::Transaction;

#[cfg(test)]
use crate::cloud_wearables;
use crate::{
    cloud_wearables::Provider,
    garmin_push::{ACTIVITY_EXPORT, HEALTH_EXPORT},
    wearable_oauth,
};

const FEEDS: [(&str, i16); 5] = [
    ("activities", ACTIVITY_EXPORT),
    ("activityDetails", ACTIVITY_EXPORT),
    ("sleeps", HEALTH_EXPORT),
    ("dailies", HEALTH_EXPORT),
    ("hrv", HEALTH_EXPORT),
];

#[cfg(test)]
#[path = "garmin_backfill_tests.rs"]
mod tests;

pub fn enabled() -> bool {
    std::env::var("AVATAR_GARMIN_ENTERPRISE_BACKFILL").as_deref() == Ok("true")
}

fn now() -> DateTime<Utc> {
    let now = Utc::now();
    DateTime::from_timestamp(now.timestamp() - now.timestamp().rem_euclid(60), 0).unwrap_or(now)
}

pub async fn enqueue_initial(tx: &Transaction<'_>, subject: &str, permissions: i16) -> Result<()> {
    if enabled() {
        let end = now();
        enqueue(tx, subject, permissions, end - Duration::days(30), end).await?;
    }
    Ok(())
}

async fn enqueue(
    tx: &Transaction<'_>,
    subject: &str,
    permissions: i16,
    start: DateTime<Utc>,
    end: DateTime<Utc>,
) -> Result<u64> {
    let mut inserted = 0;
    for (feed, permission) in FEEDS {
        if permissions & permission != 0 {
            inserted += tx.execute("INSERT INTO watch_tower.garmin_backfill AS request(subject_id,start_at,end_at,feed,permission)
                VALUES($1,$2,$3,$4,$5) ON CONFLICT(subject_id,start_at,end_at,feed) DO UPDATE
                SET status='queued',attempts=0,provider_status=NULL,next_attempt=now(),requested_at=now()
                WHERE request.status='permission_removed'", &[&subject,&start,&end,&feed,&permission]).await?;
        }
    }
    Ok(inserted)
}

pub async fn schedule(
    pool: &Pool,
    subject: &str,
    start: Option<DateTime<Utc>>,
    end: Option<DateTime<Utc>>,
) -> Result<Value> {
    ensure!(enabled(), "Garmin Enterprise backfill is not enabled");
    let latest = now();
    let end = end.unwrap_or(latest);
    let start = match start {
        Some(start) => start,
        None => end
            .checked_sub_signed(Duration::days(30))
            .context("invalid Garmin request end")?,
    };
    ensure!(
        start < end && end <= Utc::now() && end - start <= Duration::days(30),
        "Garmin request window must be in the past and at most 30 days"
    );
    let mut db = pool.get().await?;
    let tx = db.transaction().await?;
    tx.query_one(
        "SELECT pg_advisory_xact_lock(hashtext($1),hashtext('garmin'))",
        &[&subject],
    )
    .await?;
    let connection = tx
        .query_opt(
            "SELECT garmin_permissions FROM avatar_hsm.wearable_oauth_connection
        WHERE subject_id=$1 AND provider='garmin'",
            &[&subject],
        )
        .await?
        .context("connect Garmin first")?;
    let permissions: i16 = connection.get(0);
    ensure!(permissions != 0, "Garmin data permission is required");
    let added = enqueue(&tx, subject, permissions, start, end).await?;
    tx.commit().await?;
    Ok(
        json!({"state":if added>0 {"queued"} else {"already_requested"},"start":start,"end":end,"feeds_added":added}),
    )
}

pub async fn status(pool: &Pool, subject: &str) -> Result<Value> {
    let db = pool.get().await?;
    let response = db.query_one("WITH latest AS (SELECT start_at,end_at FROM watch_tower.garmin_backfill
        WHERE subject_id=$1 ORDER BY requested_at DESC,start_at DESC LIMIT 1)
        SELECT jsonb_agg(jsonb_build_object('feed',b.feed,'status',b.status,'http_status',b.provider_status,
          'start',b.start_at,'end',b.end_at,'next_attempt',CASE WHEN b.status='queued'
              THEN GREATEST(b.next_attempt,c.garmin_retry_after) END) ORDER BY b.feed)
        FROM watch_tower.garmin_backfill b JOIN latest USING(start_at,end_at)
        JOIN avatar_hsm.wearable_oauth_connection c USING(subject_id,provider)
        WHERE b.subject_id=$1", &[&subject]).await?;
    Ok(
        json!({"available":enabled(),"requests":response.get::<_, Option<Value>>(0).unwrap_or_else(||json!([]))}),
    )
}

pub(crate) struct Job {
    pub(crate) subject: String,
    pub(crate) start: DateTime<Utc>,
    pub(crate) end: DateTime<Utc>,
    pub(crate) feed: String,
    pub(crate) lease: String,
}

#[cfg(test)]
pub(crate) async fn claim(pool: &Pool) -> Result<Option<Job>> {
    let db = pool.get().await?;
    let row = db.query_opt("WITH due AS (
        SELECT b.subject_id,b.start_at,b.end_at,b.feed FROM watch_tower.garmin_backfill b
        JOIN avatar_hsm.wearable_oauth_connection c USING(subject_id,provider)
        WHERE b.status='queued' AND (c.garmin_permissions & b.permission)<>0 AND b.next_attempt<=now()
        AND (c.garmin_retry_after IS NULL OR c.garmin_retry_after<=now())
        AND (b.lease_until IS NULL OR b.lease_until<=now())
        ORDER BY b.next_attempt,b.feed LIMIT 1 FOR UPDATE OF b SKIP LOCKED)
        UPDATE watch_tower.garmin_backfill b SET lease_until=now()+interval '2 minutes',job_id=gen_random_uuid()
        FROM due WHERE (b.subject_id,b.start_at,b.end_at,b.feed)=(due.subject_id,due.start_at,due.end_at,due.feed)
        RETURNING b.subject_id,b.start_at,b.end_at,b.feed,b.job_id::text", &[]).await?;
    Ok(row.map(|row| Job {
        subject: row.get(0),
        start: row.get(1),
        end: row.get(2),
        feed: row.get(3),
        lease: row.get(4),
    }))
}

#[cfg(test)]
fn request(client: &Client, job: &Job, token: &str) -> Result<reqwest::RequestBuilder> {
    ensure!(
        FEEDS.iter().any(|(feed, _)| *feed == job.feed),
        "unknown Garmin backfill feed"
    );
    Ok(client
        .get(format!(
            "https://apis.garmin.com/wellness-api/rest/backfill/{}",
            job.feed
        ))
        .bearer_auth(token)
        .query(&[
            ("summaryStartTimeInSeconds", job.start.timestamp()),
            ("summaryEndTimeInSeconds", job.end.timestamp()),
        ]))
}

#[cfg(test)]
async fn submit(pool: &Pool, client: &Client, job: &Job) -> Result<StatusCode> {
    let credentials = wearable_oauth::access(client, pool, &job.subject, Provider::Garmin).await?;
    classify_response(
        request(client, job, &credentials[0])?
            .send()
            .await
            .map_err(|err| err.without_url())?,
    )
}

pub(crate) enum Authorization {
    Ready(String),
    PermissionRemoved,
    Deferred(DateTime<Utc>),
}

pub(crate) async fn access_token(pool: &Pool, client: &Client, job: &Job) -> Result<Authorization> {
    let permission = FEEDS
        .iter()
        .find(|(feed, _)| *feed == job.feed)
        .map(|(_, permission)| *permission)
        .context("unknown Garmin backfill feed")?;
    let row = pool
        .get()
        .await?
        .query_opt(
            "SELECT (garmin_permissions & $2)<>0 AND provider_account_hash IS NOT NULL,
                    garmin_retry_after
         FROM avatar_hsm.wearable_oauth_connection
         WHERE subject_id=$1 AND provider='garmin'",
            &[&job.subject, &permission],
        )
        .await?;
    let Some(row) = row else {
        return Ok(Authorization::PermissionRemoved);
    };
    let permitted: bool = row.get(0);
    if !permitted {
        return Ok(Authorization::PermissionRemoved);
    }
    if let Some(retry_at) = row.get::<_, Option<DateTime<Utc>>>(1) {
        if retry_at > Utc::now() {
            return Ok(Authorization::Deferred(retry_at));
        }
    }
    let token = wearable_oauth::access(client, pool, &job.subject, Provider::Garmin)
        .await?
        .into_iter()
        .next()
        .context("Garmin access token is unavailable")?;
    Ok(Authorization::Ready(token))
}

#[cfg(test)]
fn classify_response(response: reqwest::Response) -> Result<StatusCode> {
    if response.status() == StatusCode::TOO_MANY_REQUESTS {
        return cloud_wearables::check_provider_response(response).map(|result| result.status());
    }
    Ok(response.status())
}

#[cfg(test)]
async fn finish(pool: &Pool, job: &Job, response: Result<StatusCode>) -> Result<()> {
    let retry_at = response
        .as_ref()
        .err()
        .and_then(|err| err.downcast_ref::<cloud_wearables::ProviderThrottled>())
        .map(|throttle| throttle.retry_at);
    let status_code = response.as_ref().ok().map(|code| code.as_u16());
    finish_classified(pool, job, status_code, retry_at).await
}

#[cfg(test)]
pub(crate) async fn finish_classified(
    pool: &Pool,
    job: &Job,
    status_code: Option<u16>,
    retry_at: Option<DateTime<Utc>>,
) -> Result<()> {
    let status = match (status_code, retry_at.is_some()) {
        (_, true) => "queued",
        (Some(202), _) => "accepted",
        (Some(409), _) => "duplicate",
        (None, _) => "queued",
        (Some(code), _) if code >= 500 => "queued",
        (Some(_), _) => "failed",
    };
    let code = status_code.map(i32::from).or_else(|| retry_at.map(|_| 429));
    let mut db = pool.get().await?;
    let tx = db.transaction().await?;
    if retry_at.is_some() {
        lock_account(&tx, &job.subject).await?;
    }
    let updated = tx.execute("UPDATE watch_tower.garmin_backfill SET
        status=CASE WHEN $8::timestamptz IS NULL AND $1='queued' AND attempts>=5 THEN 'failed' ELSE $1 END,
        provider_status=$2,attempts=attempts+CASE WHEN $8 IS NULL THEN 1 ELSE 0 END,
        next_attempt=COALESCE($8,now()+make_interval(mins=>LEAST(60,2*(attempts+1)))),job_id=NULL,lease_until=NULL
        WHERE subject_id=$3 AND start_at=$4 AND end_at=$5 AND feed=$6 AND job_id=$7::text::uuid",
        &[&status,&code,&job.subject,&job.start,&job.end,&job.feed,&job.lease,&retry_at]).await?;
    if let (1, Some(retry_at)) = (updated, retry_at) {
        defer_account(&tx, &job.subject, retry_at).await?;
    }
    tx.commit().await?;
    Ok(())
}

pub(crate) async fn lock_account(tx: &Transaction<'_>, subject: &str) -> Result<()> {
    tx.query_one(
        "SELECT pg_advisory_xact_lock(hashtext($1),hashtext('garmin'))",
        &[&subject],
    )
    .await?;
    Ok(())
}

pub(crate) async fn defer_account(
    tx: &Transaction<'_>,
    subject: &str,
    retry_at: DateTime<Utc>,
) -> Result<()> {
    tx.execute(
        "UPDATE avatar_hsm.wearable_oauth_connection
         SET garmin_retry_after=GREATEST(garmin_retry_after,$2)
         WHERE subject_id=$1 AND provider='garmin'",
        &[&subject, &retry_at],
    )
    .await?;
    Ok(())
}

#[cfg(test)]
pub async fn worker(pool: Pool, client: Client) {
    if !enabled() {
        return;
    }
    loop {
        let pause = if let Ok(Some(job)) = claim(&pool).await {
            let response = submit(&pool, &client, &job).await;
            let _ = finish(&pool, &job, response).await;
            1
        } else {
            10
        };
        tokio::time::sleep(std::time::Duration::from_secs(pause)).await;
    }
}
