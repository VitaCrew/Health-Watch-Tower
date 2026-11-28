use anyhow::{ensure, Result};
use avatar_hsm::etl_adapter::{self, GarminBackfillAuthorization, GarminBackfillJob};
use chrono::{DateTime, Duration, Utc};
use deadpool_postgres::Pool;
use reqwest::{Client, StatusCode};
use std::{env, time::Duration as StdDuration};

const FEEDS: [&str; 5] = ["activities", "activityDetails", "sleeps", "dailies", "hrv"];

struct Claimed {
    subject_id: String,
    job: GarminBackfillJob,
}

async fn claim(pool: &Pool) -> Result<Option<Claimed>> {
    let row = pool
        .get()
        .await?
        .query_opt(
            "WITH due AS (
                 SELECT subject_id,start_at,end_at,feed FROM watch_tower.garmin_backfill
                 WHERE status='queued' AND next_attempt<=now()
                   AND (lease_until IS NULL OR lease_until<=now())
                 ORDER BY next_attempt,feed LIMIT 1 FOR UPDATE SKIP LOCKED
             )
             UPDATE watch_tower.garmin_backfill AS job
             SET lease_until=now()+interval '2 minutes',job_id=gen_random_uuid()
             FROM due WHERE (job.subject_id,job.start_at,job.end_at,job.feed)=
                 (due.subject_id,due.start_at,due.end_at,due.feed)
             RETURNING job.subject_id,job.start_at,job.end_at,job.feed,job.job_id::text",
            &[],
        )
        .await?;
    Ok(row.map(|row| {
        let subject_id: String = row.get(0);
        Claimed {
            job: etl_adapter::lease_garmin_backfill(
                subject_id.clone(),
                row.get(1),
                row.get(2),
                row.get(3),
                row.get(4),
            ),
            subject_id,
        }
    }))
}

pub async fn worker(pool: Pool, client: Client) {
    if env::var("AVATAR_GARMIN_ENTERPRISE_BACKFILL").as_deref() != Ok("true") {
        return;
    }
    loop {
        match claim(&pool).await {
            Ok(Some(claimed)) => {
                let result = tokio::time::timeout(
                    StdDuration::from_secs(45),
                    submit(&pool, &client, &claimed),
                )
                .await;
                match result {
                    Ok(Ok(Some((status, retry_at)))) => {
                        let _ = finish(
                            &pool,
                            &claimed.subject_id,
                            &claimed.job,
                            Some(status),
                            retry_at,
                        )
                        .await;
                    }
                    Ok(Ok(None)) => {}
                    Ok(Err(_)) | Err(_) => {
                        let _ = finish(&pool, &claimed.subject_id, &claimed.job, None, None).await;
                    }
                }
                tokio::time::sleep(StdDuration::from_secs(1)).await;
            }
            Ok(None) | Err(_) => tokio::time::sleep(StdDuration::from_secs(10)).await,
        }
    }
}

async fn submit(
    pool: &Pool,
    client: &Client,
    claimed: &Claimed,
) -> Result<Option<(u16, Option<DateTime<Utc>>)>> {
    let token = match etl_adapter::garmin_backfill_token(pool, client, &claimed.job).await? {
        GarminBackfillAuthorization::Ready(token) => token,
        GarminBackfillAuthorization::PermissionRemoved => {
            complete_without_request(pool, &claimed.job, "permission_removed", None).await?;
            return Ok(None);
        }
        GarminBackfillAuthorization::Deferred(at) => {
            complete_without_request(pool, &claimed.job, "queued", Some(at)).await?;
            return Ok(None);
        }
    };
    let response = request(
        client,
        claimed.job.feed(),
        claimed.job.start(),
        claimed.job.end(),
        token,
    )?
    .send()
    .await
    .map_err(reqwest::Error::without_url)?;
    let status = response.status();
    let retry_at = if status == StatusCode::TOO_MANY_REQUESTS {
        Some(retry_after(
            response
                .headers()
                .get(reqwest::header::RETRY_AFTER)
                .and_then(|value| value.to_str().ok()),
            Utc::now(),
        ))
    } else {
        None
    };
    Ok(Some((status.as_u16(), retry_at)))
}

fn request(
    client: &Client,
    feed: &str,
    start: DateTime<Utc>,
    end: DateTime<Utc>,
    token: String,
) -> Result<reqwest::RequestBuilder> {
    ensure!(FEEDS.contains(&feed), "unknown Garmin backfill feed");
    ensure!(start < end, "invalid Garmin backfill window");
    Ok(client
        .get(format!(
            "https://apis.garmin.com/wellness-api/rest/backfill/{feed}"
        ))
        .bearer_auth(token)
        .query(&[
            ("summaryStartTimeInSeconds", start.timestamp()),
            ("summaryEndTimeInSeconds", end.timestamp()),
        ]))
}

async fn finish(
    pool: &Pool,
    subject_id: &str,
    job: &GarminBackfillJob,
    status_code: Option<u16>,
    retry_at: Option<DateTime<Utc>>,
) -> Result<()> {
    if let Some(at) = retry_at {
        etl_adapter::defer_garmin_backfill_account(pool, subject_id, at).await?;
    }
    let state = match (status_code, retry_at.is_some()) {
        (_, true) => "queued",
        (Some(202), _) => "accepted",
        (Some(409), _) => "duplicate",
        (None, _) => "queued",
        (Some(code), _) if code >= 500 => "queued",
        (Some(_), _) => "failed",
    };
    let provider_status = status_code.map(i32::from).or_else(|| retry_at.map(|_| 429));
    pool.get()
        .await?
        .execute(
            "UPDATE watch_tower.garmin_backfill SET
                 status=CASE WHEN $8::timestamptz IS NULL AND $1='queued' AND attempts>=5
                             THEN 'failed' ELSE $1 END,
                 provider_status=$2,
                 attempts=attempts+CASE WHEN $8::timestamptz IS NULL THEN 1 ELSE 0 END,
                 next_attempt=COALESCE($8,now()+make_interval(mins=>LEAST(60,2*(attempts+1)))),
                 job_id=NULL,lease_until=NULL
             WHERE subject_id=$3 AND start_at=$4 AND end_at=$5 AND feed=$6
               AND job_id=$7::text::uuid AND lease_until>now()",
            &[
                &state,
                &provider_status,
                &job.subject_id(),
                &job.start(),
                &job.end(),
                &job.feed(),
                &job.job_id(),
                &retry_at,
            ],
        )
        .await?;
    Ok(())
}

async fn complete_without_request(
    pool: &Pool,
    job: &GarminBackfillJob,
    status: &str,
    next_attempt: Option<DateTime<Utc>>,
) -> Result<()> {
    pool.get()
        .await?
        .execute(
            "UPDATE watch_tower.garmin_backfill SET status=$1,provider_status=NULL,
                 job_id=NULL,lease_until=NULL,next_attempt=COALESCE($2,now())
             WHERE subject_id=$3 AND start_at=$4 AND end_at=$5 AND feed=$6
               AND job_id=$7::text::uuid AND lease_until>now()",
            &[
                &status,
                &next_attempt,
                &job.subject_id(),
                &job.start(),
                &job.end(),
                &job.feed(),
                &job.job_id(),
            ],
        )
        .await?;
    Ok(())
}

fn retry_after(header: Option<&str>, now: DateTime<Utc>) -> DateTime<Utc> {
    header
        .and_then(|value| {
            let value = value.trim();
            value
                .parse::<i64>()
                .ok()
                .and_then(Duration::try_seconds)
                .and_then(|delay| now.checked_add_signed(delay))
                .or_else(|| {
                    DateTime::parse_from_rfc2822(value)
                        .ok()
                        .map(|at| at.with_timezone(&Utc))
                })
        })
        .filter(|retry_at| *retry_at > now)
        .unwrap_or_else(|| now + Duration::minutes(5))
}

#[cfg(test)]
#[path = "garmin_backfill_tests.rs"]
mod tests;
