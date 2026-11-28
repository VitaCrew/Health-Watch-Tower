use anyhow::{ensure, Context, Result};
use avatar_hsm::etl_adapter::{self, GarminPullAuthorizationResult};
use chrono::{DateTime, Duration as ChronoDuration, Utc};
use deadpool_postgres::Pool;
use reqwest::{Client, Response, StatusCode};
use serde_json::{json, Value};
use std::time::Duration;

#[cfg(test)]
#[path = "garmin_pull_tests.rs"]
mod tests;

struct Job {
    id: String,
    lease: String,
    subject: String,
    feed: String,
    sealed: Vec<u8>,
    request_hash: Vec<u8>,
    received: DateTime<Utc>,
}

async fn claim(pool: &Pool) -> Result<Option<Job>> {
    let row = pool.get().await?.query_opt(
        "WITH due AS (
             SELECT id FROM watch_tower.garmin_pull_job
             WHERE status='pending' AND sealed_source IS NOT NULL AND next_attempt<=now()
               AND (lease_until IS NULL OR lease_until<=now())
             ORDER BY next_attempt,id LIMIT 1 FOR UPDATE SKIP LOCKED
         )
         UPDATE watch_tower.garmin_pull_job j
         SET lease_until=now()+interval '2 minutes',lease=gen_random_uuid()
         FROM due WHERE j.id=due.id
         RETURNING j.id::text,j.lease::text,j.subject_id,j.feed,j.sealed_source,j.request_hash,j.received_at",
        &[],
    ).await?;
    Ok(row.map(|row| Job {
        id: row.get(0),
        lease: row.get(1),
        subject: row.get(2),
        feed: row.get(3),
        sealed: row.get(4),
        request_hash: row.get(5),
        received: row.get(6),
    }))
}

async fn run(pool: &Pool, client: &Client, job: &Job) -> Result<()> {
    let authorization = etl_adapter::authorize_garmin_pull(
        pool,
        client,
        &job.id,
        &job.lease,
        &job.subject,
        &job.feed,
        job.sealed.clone(),
        job.request_hash.clone(),
    )
    .await?;
    let authorization = match authorization {
        GarminPullAuthorizationResult::Ready(authorization) => authorization,
        GarminPullAuthorizationResult::Deferred(at) => {
            pool.get().await?.execute(
                "UPDATE watch_tower.garmin_pull_job SET next_attempt=$3,lease=NULL,lease_until=NULL
                 WHERE id::text=$1 AND lease::text=$2 AND lease_until>now()",
                &[&job.id, &job.lease, &at],
            ).await?;
            return Ok(());
        }
        GarminPullAuthorizationResult::PermissionRemoved => {
            pool.get().await?.execute(
            "UPDATE watch_tower.garmin_pull_job SET status='unavailable',sealed_source=NULL,lease=NULL,lease_until=NULL
             WHERE id::text=$1 AND lease::text=$2 AND lease_until>now()",
            &[&job.id, &job.lease],
        ).await?;
            return Ok(());
        }
    };
    let response = client
        .get(authorization.url)
        .bearer_auth(&authorization.token)
        .send()
        .await
        .map_err(reqwest::Error::without_url)?;
    let payload: Value = if response.status() == StatusCode::NO_CONTENT {
        Value::Array(Vec::new())
    } else {
        response_body(response).await?
    };
    let records = normalize_records(&payload, &job.feed, job.received)?;
    etl_adapter::commit_garmin_pull(
        pool,
        &job.id,
        &job.lease,
        &job.subject,
        &job.feed,
        authorization.account_hash,
        records,
    )
    .await?;
    Ok(())
}

fn normalize_records(
    payload: &Value,
    feed: &str,
    received: DateTime<Utc>,
) -> Result<Vec<etl_adapter::GarminPullRecord>> {
    let list = payload
        .as_array()
        .or_else(|| payload[feed].as_array())
        .context("Garmin pull response must contain summaries")?;
    ensure!(list.len() <= 2_000, "Garmin response exceeds record limit");
    let kind = etl_adapter::garmin_pull_kind(feed).context("invalid Garmin pull feed")?;
    list.iter()
        .map(|row| normalize_record(row, feed, &kind, received))
        .collect()
}

fn normalize_record(
    row: &Value,
    feed: &str,
    kind: &str,
    received: DateTime<Utc>,
) -> Result<etl_adapter::GarminPullRecord> {
    let owner_id = row.get("userId").and_then(account_id);
    ensure!(
        row.get("userId").is_none() || owner_id.is_some(),
        "invalid Garmin pull owner"
    );
    ensure!(
        row.get("callbackURL").is_none(),
        "Garmin pull returned a nested callback"
    );
    let detail = row.get("summary").unwrap_or(row);
    let time_fields: &[&str] = if feed == "mct" {
        &["calendarDate", "startTimeInSeconds", "periodStartDate"]
    } else if feed == "bloodPressures" {
        &[
            "measurementTimeInSeconds",
            "measurementTimestampGMT",
            "startTimeInSeconds",
        ]
    } else {
        &[
            "startTimeInSeconds",
            "measurementTimeInSeconds",
            "calendarDate",
        ]
    };
    let at = time_fields
        .iter()
        .find_map(|field| {
            let value = &detail[*field];
            if let Some(seconds) = value.as_i64() {
                return DateTime::from_timestamp(seconds, 0);
            }
            if *field == "measurementTimestampGMT" {
                return value.as_str()?.parse::<DateTime<Utc>>().ok();
            }
            value
                .as_str()?
                .parse::<chrono::NaiveDate>()
                .ok()?
                .and_hms_opt(0, 0, 0)
                .map(|time| time.and_utc())
        })
        .context("Garmin record has no usable timestamp")?;
    let length = detail["durationInSeconds"].as_i64().unwrap_or(0);
    ensure!((0..=604_800).contains(&length), "invalid Garmin duration");
    let mut end = at
        .checked_add_signed(ChronoDuration::seconds(length))
        .context("invalid Garmin end time")?;
    if feed == "dailies" {
        end = end.min(received.max(at));
    }
    let raw_id = if kind == "workout" || feed == "activityDetails" {
        row.get("activityId")
            .and_then(account_id)
            .or_else(|| detail.get("activityId").and_then(account_id))
    } else {
        row.get("summaryId")
            .and_then(account_id)
            .or_else(|| detail.get("summaryId").and_then(account_id))
    }
    .context("Garmin record has no stable ID")?;
    let mut summary = row.as_object().context("invalid Garmin summary")?.clone();
    for field in ["userId", "userAccessToken", "callbackURL"] {
        summary.remove(field);
        if let Some(inner) = summary.get_mut("summary").and_then(Value::as_object_mut) {
            inner.remove(field);
        }
    }
    let (source_id, payload) = match kind {
        "workout" => (
            raw_id,
            json!({"exercise_type":detail["activityType"].as_str().unwrap_or("garmin"),"distance_meters":detail["distanceInMeters"],"duration_unknown":length==0,"provider_data":summary}),
        ),
        "sleep" => (
            raw_id,
            json!({"stages":sleep_stages(&detail["sleepLevelsMap"],at,end),"provider_data":summary}),
        ),
        _ => (
            format!("{feed}:{raw_id}"),
            json!({"metric":feed,"provider_data":summary}),
        ),
    };
    let modified_at = if feed == "mct" {
        detail["lastUpdatedTimeInSeconds"]
            .as_i64()
            .filter(|seconds| *seconds > 0)
            .and_then(|seconds| DateTime::from_timestamp(seconds, 0))
            .filter(|updated| *updated <= received + ChronoDuration::minutes(5))
            .unwrap_or(received)
    } else {
        received
    };
    Ok(etl_adapter::GarminPullRecord {
        owner_id,
        kind: kind.to_owned(),
        source: "Garmin Connect".to_owned(),
        source_id,
        started_at: at,
        ended_at: end,
        modified_at,
        payload,
    })
}

fn account_id(value: &Value) -> Option<String> {
    value
        .as_str()
        .filter(|id| !id.is_empty() && id.len() <= 256)
        .map(str::to_owned)
        .or_else(|| value.as_u64().map(|id| id.to_string()))
}

fn sleep_stages(levels: &Value, start: DateTime<Utc>, end: DateTime<Utc>) -> Vec<Value> {
    let mut stages = Vec::new();
    for (level, stage) in [
        ("awake", 1),
        ("light", 4),
        ("deep", 5),
        ("rem", 6),
        ("unmeasurable", 0),
    ] {
        if let Some(items) = levels[level].as_array() {
            for item in items {
                let bounds = item["startTimeInSeconds"]
                    .as_i64()
                    .zip(item["endTimeInSeconds"].as_i64())
                    .and_then(|(a, b)| {
                        Some((
                            DateTime::from_timestamp(a, 0)?,
                            DateTime::from_timestamp(b, 0)?,
                        ))
                    });
                if let Some((from, until)) =
                    bounds.filter(|(from, until)| *from >= start && from < until && *until <= end)
                {
                    stages.push((from, until, stage));
                }
            }
        }
    }
    stages.sort_unstable();
    if stages.len() > 500 || stages.windows(2).any(|pair| pair[0].1 > pair[1].0) {
        return Vec::new();
    }
    stages
        .into_iter()
        .map(|(from, until, stage)| json!({"start":from,"end":until,"stage":stage}))
        .collect()
}

async fn retry(pool: &Pool, job: &Job, error: &anyhow::Error) -> Result<()> {
    let retry_at = error
        .downcast_ref::<ProviderRateLimited>()
        .map(|limited| limited.retry_at);
    if let Some(at) = retry_at {
        etl_adapter::defer_garmin_backfill_account(pool, &job.subject, at).await?;
    }
    pool.get()
        .await?
        .execute(
            "UPDATE watch_tower.garmin_pull_job SET
             attempts=attempts+CASE WHEN $3::timestamptz IS NULL THEN 1 ELSE 0 END,
             status=CASE WHEN $3 IS NULL AND attempts>=5 THEN 'unavailable' ELSE 'pending' END,
             sealed_source=CASE WHEN $3 IS NULL AND attempts>=5 THEN NULL ELSE sealed_source END,
             lease=NULL,lease_until=NULL,
             next_attempt=COALESCE($3,now()+make_interval(mins=>LEAST(60,2*(attempts+1))))
         WHERE id::text=$1 AND lease::text=$2 AND lease_until>now() AND status='pending'",
            &[&job.id, &job.lease, &retry_at],
        )
        .await?;
    Ok(())
}

#[derive(Debug)]
struct ProviderRateLimited {
    retry_at: DateTime<Utc>,
}

impl std::fmt::Display for ProviderRateLimited {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Garmin rate limited the pull")
    }
}
impl std::error::Error for ProviderRateLimited {}

async fn response_body(response: Response) -> Result<Value> {
    if response.status() == StatusCode::TOO_MANY_REQUESTS {
        let retry_at = response
            .headers()
            .get(reqwest::header::RETRY_AFTER)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.parse::<i64>().ok())
            .and_then(chrono::Duration::try_seconds)
            .and_then(|delay| Utc::now().checked_add_signed(delay))
            .or_else(|| {
                response
                    .headers()
                    .get(reqwest::header::RETRY_AFTER)
                    .and_then(|value| value.to_str().ok())
                    .and_then(|value| DateTime::parse_from_rfc2822(value).ok())
                    .map(|at| at.with_timezone(&Utc))
            })
            .unwrap_or_else(|| Utc::now() + chrono::Duration::minutes(5));
        return Err(ProviderRateLimited { retry_at }.into());
    }
    ensure!(
        response.status().is_success(),
        "Garmin pull returned HTTP {}",
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
            bytes.len() + chunk.len() <= 8 * 1024 * 1024,
            "Garmin response exceeds limit"
        );
        bytes.extend_from_slice(&chunk);
    }
    Ok(serde_json::from_slice(&bytes)?)
}

pub async fn worker(pool: Pool, client: Client) {
    loop {
        match claim(&pool).await {
            Ok(Some(job)) => {
                let result =
                    tokio::time::timeout(Duration::from_secs(90), run(&pool, &client, &job)).await;
                let result = match result {
                    Ok(result) => result,
                    Err(_) => Err(anyhow::anyhow!("Garmin pull timed out")),
                };
                if let Err(error) = result {
                    let _ = retry(&pool, &job, &error).await;
                }
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
            Ok(None) | Err(_) => tokio::time::sleep(Duration::from_secs(10)).await,
        }
    }
}
