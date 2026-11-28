use anyhow::{ensure, Context, Result};
#[cfg(test)]
use chrono::NaiveDate;
use chrono::{DateTime, Duration, Timelike, Utc};
use deadpool_postgres::Pool;
#[cfg(test)]
use reqwest::Client;
use serde::Deserialize;
use serde_json::{json, Value};

use crate::{
    cloud_wearables::{self, Provider},
    store::SUBJECT,
    wearable_oauth, wearables,
};

#[cfg(test)]
#[path = "wearable_sync_tests.rs"]
mod tests;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ScheduleRequest {
    pub provider: Provider,
    pub start: Option<DateTime<Utc>>,
    pub end: Option<DateTime<Utc>>,
}

pub async fn schedule(pool: &Pool, subject: &str, request: ScheduleRequest) -> Result<Value> {
    if matches!(request.provider, Provider::Garmin) {
        return crate::garmin_backfill::schedule(pool, subject, request.start, request.end).await;
    }
    let slug = request.provider.slug();
    let end = request.end.unwrap_or_else(Utc::now);
    let start = match request.start {
        Some(start) => start,
        None => end
            .checked_sub_signed(Duration::days(30))
            .context("invalid sync end")?,
    };
    let max_days = match request.provider {
        Provider::GoogleHealth
        | Provider::Oura
        | Provider::Whoop
        | Provider::Strava
        | Provider::Suunto
        | Provider::Withings
        | Provider::Sensorbio
        | Provider::Ultrahuman => 3_650,
        Provider::Polar => 365,
        _ => 90,
    };
    ensure!(
        start < end && end <= Utc::now() && end - start <= Duration::days(max_days),
        "sync range for {slug} must be in the past and at most {max_days} days"
    );
    let mut db = pool.get().await?;
    let tx = db.transaction().await?;
    tx.query_one(
        "SELECT pg_advisory_xact_lock(hashtext($1),hashtext($2))",
        &[&subject, &slug],
    )
    .await?;
    if subject == SUBJECT {
        ensure!(
            cloud_wearables::configured(request.provider).is_some(),
            "cloud provider is unconfigured"
        );
        tx.execute("INSERT INTO avatar_hsm.device_connection(subject_id,provider) VALUES($1,$2)
             ON CONFLICT (subject_id,provider) DO UPDATE SET state='pending' WHERE device_connection.state='revoked'",
             &[&subject,&slug]).await?;
    } else {
        ensure!(tx.query_opt("SELECT 1 FROM avatar_hsm.wearable_oauth_connection WHERE subject_id=$1 AND provider=$2",
             &[&subject,&slug]).await?.is_some(), "connect the provider first");
    }
    let updated = tx.execute("INSERT INTO watch_tower.wearable_sync_schedule AS j(subject_id,provider,backfill_from,backfill_to)
         VALUES($1,$2,$3,$4) ON CONFLICT (subject_id,provider) DO UPDATE
           SET backfill_from=$3,backfill_to=$4,next_sync=now(),failures=0,last_error=NULL,job_id=NULL,job_until=NULL,
               wake_requested=j.wake_requested OR j.claimed_wake,claimed_wake=false
           WHERE j.job_until IS NULL OR j.job_until <= now()",
        &[&subject,&slug,&start,&end]).await?;
    ensure!(updated == 1, "a sync is already in progress");
    tx.commit().await?;
    Ok(json!({"state":"queued","start":start,"end":end,"automatic_sync":true}))
}

/// Queue an immediate collection through the Watch Tower worker. The HSM MCP
/// surface must not fetch provider data inline, even for the synthetic owner.
pub async fn sync_now(pool: &Pool, subject: &str, provider: Provider) -> Result<Value> {
    if subject == SUBJECT && cloud_wearables::configured(provider).is_none() {
        return Ok(json!({"state":"configuration_required"}));
    }
    let end = Utc::now();
    schedule(
        pool,
        subject,
        ScheduleRequest {
            provider,
            start: Some(end - cloud_wearables::window_size(provider)),
            end: Some(end),
        },
    )
    .await
}

#[cfg(test)]
pub(crate) async fn request_live_refresh(
    pool: &Pool,
    subject: &str,
    provider: Provider,
) -> Result<bool> {
    let changed = pool
        .get()
        .await?
        .execute(
            "UPDATE watch_tower.wearable_sync_schedule
             SET wake_requested=true,next_sync=LEAST(next_sync,now())
             WHERE subject_id=$1 AND provider=$2",
            &[&subject, &provider.slug()],
        )
        .await?;
    Ok(changed == 1)
}

pub async fn status(pool: &Pool, subject: &str, provider: Provider) -> Result<Value> {
    let slug = provider.slug();
    let mut status = wearables::status_for(pool, subject, slug).await?;
    let row = pool.get().await?.query_opt("SELECT
            jsonb_build_object('next_sync', GREATEST(next_sync,throttled_until), 'backfill_from', backfill_from,
            'backfill_remaining_to', CASE WHEN backfill_to > backfill_from THEN backfill_to END,
            'failures', failures, 'last_error', CASE WHEN throttled_until>now()
                THEN 'provider_rate_limited' ELSE last_error END,
            'throttled_until', CASE WHEN throttled_until>now() THEN throttled_until END,
            'running', COALESCE(job_until > now(), false))
        FROM watch_tower.wearable_sync_schedule WHERE subject_id=$1 AND provider=$2",
        &[&subject,&slug]).await?;
    status["schedule"] = row.map_or(Value::Null, |row| row.get::<_, Value>(0));
    status["oauth_available"] =
        json!(subject != SUBJECT && wearable_oauth::oauth_available(provider));
    if matches!(provider, Provider::Garmin) {
        status["garmin_backfill"] = crate::garmin_backfill::status(pool, subject).await?;
    }
    if matches!(provider, Provider::Withings) {
        status["webhook_subscription"] =
            json!(crate::withings_notify::status(pool, subject).await?);
    }
    Ok(status)
}

/// Accept one fixed UTC hourly slot requested by OpenHuman's Cron service.
/// HSM stores the slot for idempotency, but does not choose the cadence.
pub(crate) async fn request_oura_due_sync(
    pool: &Pool,
    subject: &str,
    slot_at: DateTime<Utc>,
) -> Result<bool> {
    validate_openhuman_oura_slot(slot_at, Utc::now())?;
    let changed = pool
        .get()
        .await?
        .execute(
            "UPDATE watch_tower.wearable_sync_schedule
             SET last_openhuman_slot=$2, wake_requested=true,
                 next_sync=LEAST(next_sync,now())
             WHERE subject_id=$1 AND provider='oura'
               AND (last_openhuman_slot IS NULL OR last_openhuman_slot < $2)",
            &[&subject, &slot_at],
        )
        .await?;
    Ok(changed == 1)
}

fn validate_openhuman_oura_slot(slot_at: DateTime<Utc>, now: DateTime<Utc>) -> Result<()> {
    ensure!(
        slot_at.minute() == 0 && slot_at.second() == 0 && slot_at.nanosecond() == 0,
        "Oura sync slot must start on a UTC hour"
    );
    ensure!(
        slot_at <= now + Duration::minutes(1) && slot_at >= now - Duration::hours(2),
        "Oura sync slot is outside the accepted window"
    );
    Ok(())
}

#[cfg(test)]
mod openhuman_oura_slot_tests {
    use super::*;

    #[test]
    fn accepts_only_recent_hour_aligned_utc_slots() {
        let now = Utc::now();
        let valid = now
            .date_naive()
            .and_hms_opt(now.hour(), 0, 0)
            .unwrap()
            .and_utc();
        assert!(validate_openhuman_oura_slot(valid, now).is_ok());
        assert!(validate_openhuman_oura_slot(valid + Duration::minutes(1), now).is_err());
        assert!(validate_openhuman_oura_slot(valid - Duration::hours(3), now).is_err());
        assert!(validate_openhuman_oura_slot(valid + Duration::hours(2), now).is_err());
    }
}

#[cfg(test)]
struct Job {
    subject: String,
    provider: Provider,
    id: String,
    lower: DateTime<Utc>,
    upper: DateTime<Utc>,
    live_first: bool,
    target: Option<TargetDay>,
}

#[cfg(test)]
struct TargetDay {
    day: NaiveDate,
    revision: String,
}

#[cfg(test)]
async fn claim(pool: &Pool) -> Result<Option<Job>> {
    let mut db = pool.get().await?;
    let tx = db.transaction().await?;
    let row = tx.query_opt(
        "WITH due AS (
             SELECT subject_id,provider,wake_requested OR claimed_wake
                 OR (backfill_to>backfill_from AND next_live_sync<=now()) AS live_first
             FROM watch_tower.wearable_sync_schedule
             WHERE next_sync <= now() AND (job_until IS NULL OR job_until <= now())
               AND (throttled_until IS NULL OR throttled_until <= now())
             ORDER BY next_sync LIMIT 1 FOR UPDATE SKIP LOCKED
         )
         UPDATE watch_tower.wearable_sync_schedule AS job
         SET job_id=gen_random_uuid(), job_until=now()+interval '5 minutes',
             wake_requested=false, claimed_wake=due.live_first
         FROM due WHERE job.subject_id=due.subject_id AND job.provider=due.provider
         RETURNING job.subject_id, job.provider, job.job_id::text, job.backfill_from, job.backfill_to, job.claimed_wake",
        &[],
    ).await?;
    let Some(row) = row else {
        return Ok(None);
    };
    let subject: String = row.get(0);
    let slug: String = row.get(1);
    let target = tx
        .query_opt(
            "SELECT day,revision::text FROM watch_tower.wearable_target_day
         WHERE subject_id=$1 AND provider=$2 AND next_attempt<=now() ORDER BY next_attempt,day LIMIT 1",
            &[&subject, &slug],
        )
        .await?
        .map(|row| TargetDay {
            day: row.get(0),
            revision: row.get(1),
        });
    tx.commit().await?;
    Ok(Some(Job {
        subject,
        provider: Provider::parse(&slug)?,
        id: row.get(2),
        lower: row.get(3),
        upper: row.get(4),
        live_first: row.get(5),
        target,
    }))
}

#[cfg(test)]
async fn worker(pool: Pool, client: Client) {
    loop {
        match claim(&pool).await {
            Ok(Some(job)) => {
                if let Err(error) = run(&pool, &client, &job).await {
                    let _ = defer_job(&pool, &job, &error).await;
                }
                tokio::time::sleep(std::time::Duration::from_secs(1)).await;
            }
            Ok(None) | Err(_) => tokio::time::sleep(std::time::Duration::from_secs(10)).await,
        }
    }
}

#[cfg(test)]
async fn defer_job(pool: &Pool, job: &Job, error: &anyhow::Error) -> Result<()> {
    let Some(throttle) = error.downcast_ref::<cloud_wearables::ProviderThrottled>() else {
        return retry_job(pool, job).await;
    };
    pool.get()
        .await?
        .execute(
            "UPDATE watch_tower.wearable_sync_schedule
         SET throttled_until=GREATEST(throttled_until,$4),next_sync=now(),
             last_error='provider_rate_limited',wake_requested=wake_requested OR claimed_wake,
             claimed_wake=false,job_id=NULL,job_until=NULL
         WHERE subject_id=$1 AND provider=$2 AND job_id=$3::text::uuid",
            &[
                &job.subject,
                &job.provider.slug(),
                &job.id,
                &throttle.retry_at,
            ],
        )
        .await?;
    Ok(())
}

#[cfg(test)]
async fn retry_job(pool: &Pool, job: &Job) -> Result<()> {
    if job.target.is_some() {
        return release_job(pool, job, None, true).await;
    }
    pool.get()
        .await?
        .execute(
            "UPDATE watch_tower.wearable_sync_schedule
         SET failures=failures+1, last_error='provider_or_storage_unavailable',
             next_sync=now()+make_interval(mins=>LEAST(120, 5*(failures+1))),
             wake_requested=wake_requested OR claimed_wake, claimed_wake=false,
             job_id=NULL, job_until=NULL
         WHERE subject_id=$1 AND provider=$2 AND job_id=$3::text::uuid",
            &[&job.subject, &job.provider.slug(), &job.id],
        )
        .await?;
    Ok(())
}

#[cfg(test)]
async fn run(pool: &Pool, client: &Client, job: &Job) -> Result<()> {
    if matches!(job.provider, Provider::Polar) && job.subject != SUBJECT {
        crate::wearable_webhook_documents::schedule_polar_physical_info(pool, &job.subject).await?;
    }
    let credentials = if job.subject == SUBJECT {
        let raw =
            cloud_wearables::configured(job.provider).context("provider credentials missing")?;
        cloud_wearables::prepare(client, job.provider, raw).await?
    } else {
        wearable_oauth::access_for_sync(client, pool, &job.subject, job.provider).await?
    };
    run_batches(pool, job, |from, until, lease| {
        let credentials = &credentials;
        async move {
            cloud_wearables::fetch_range(client, job.provider, credentials, from, until, &lease)
                .await
        }
    })
    .await
}

#[cfg(test)]
async fn run_batches<F, Fut>(pool: &Pool, job: &Job, fetch: F) -> Result<()>
where
    F: Fn(DateTime<Utc>, DateTime<Utc>, Value) -> Fut,
    Fut: std::future::Future<Output = Result<wearables::Snapshot>> + Send,
{
    let slug = job.provider.slug();
    let size = cloud_wearables::window_size(job.provider);
    let backfill = job.upper > job.lower && !job.live_first && job.target.is_none();
    let now = Utc::now();
    let (start, end) = if let Some(target) = &job.target {
        target_window(target.day, now)
    } else if backfill {
        (job.lower.max(job.upper - size), job.upper)
    } else {
        (now - size, now)
    };
    let reconcile_google = matches!(job.provider, Provider::GoogleHealth) && job.target.is_some();
    let ranges = if let (Provider::GoogleHealth | Provider::Suunto, Some(target)) =
        (job.provider, &job.target)
    {
        single_day_target_windows(target.day, now)
    } else {
        vec![(start, end)]
    };
    for (from, until) in ranges {
        let lease = wearables::issue_job_lease(pool, &job.subject, slug, &job.id).await?;
        let snapshot = fetch(from, until, lease).await?;
        crate::state_owner::apply_normalized_snapshot(
            pool,
            &job.subject,
            slug,
            snapshot,
            reconcile_google,
            Some(&job.id),
        )
        .await?;
    }
    let next_boundary = if backfill && start > job.lower {
        let overlap = if matches!(job.provider, Provider::Polar | Provider::Whoop) {
            // SleepWise and WHOOP physiological cycles can span a calendar day.
            Duration::days(2)
        } else {
            Duration::hours(12)
        };
        start + overlap
    } else {
        start
    };
    finish_job(pool, job, backfill.then_some(next_boundary)).await
}

#[cfg(test)]
fn target_window(day: NaiveDate, now: DateTime<Utc>) -> (DateTime<Utc>, DateTime<Utc>) {
    let at = day.and_hms_opt(0, 0, 0).unwrap().and_utc();
    // Include complete nearby sessions whose device-local day or end time triggered the callback.
    (at - Duration::days(1), (at + Duration::days(2)).min(now))
}

#[cfg(test)]
fn single_day_target_windows(
    day: NaiveDate,
    now: DateTime<Utc>,
) -> Vec<(DateTime<Utc>, DateTime<Utc>)> {
    let midnight = day.and_hms_opt(0, 0, 0).unwrap().and_utc();
    // Keep each request at one day while covering sessions crossing either midnight.
    [-12, 0, 12]
        .into_iter()
        .filter_map(|hours| {
            let start = midnight + Duration::hours(hours);
            let end = (start + Duration::days(1)).min(now);
            (start < end).then_some((start, end))
        })
        .collect()
}

#[cfg(test)]
async fn finish_job(pool: &Pool, job: &Job, next_boundary: Option<DateTime<Utc>>) -> Result<()> {
    release_job(pool, job, next_boundary, false).await
}

#[cfg(test)]
async fn release_job(
    pool: &Pool,
    job: &Job,
    next_boundary: Option<DateTime<Utc>>,
    retry_target: bool,
) -> Result<()> {
    let slug = job.provider.slug();
    // Replaying after a crash during cursor update is safe. Record upserts use provider IDs.
    let mut db = pool.get().await?;
    let tx = db.transaction().await?;
    // Webhook delivery also locks the schedule before touching target days.
    let active = tx
        .query_opt(
            "SELECT 1 FROM watch_tower.wearable_sync_schedule
        WHERE subject_id=$1 AND provider=$2 AND job_id=$3::text::uuid FOR UPDATE",
            &[&job.subject, &slug, &job.id],
        )
        .await?;
    ensure!(active.is_some(), "sync job changed");
    if let Some(target) = &job.target {
        let sql = if retry_target {
            "UPDATE watch_tower.wearable_target_day SET failures=LEAST(failures+1,24),
             next_attempt=now()+make_interval(mins=>LEAST(120,5*(failures+1)))
             WHERE subject_id=$1 AND provider=$2 AND day=$3 AND revision=$4::text::uuid"
        } else {
            "DELETE FROM watch_tower.wearable_target_day
             WHERE subject_id=$1 AND provider=$2 AND day=$3 AND revision=$4::text::uuid"
        };
        tx.execute(sql, &[&job.subject, &slug, &target.day, &target.revision])
            .await?;
    }
    let targeted = job.target.is_some();
    let advanced = tx
        .execute(
            "UPDATE watch_tower.wearable_sync_schedule AS job SET
         backfill_to=COALESCE($4,backfill_to),
         next_live_sync=CASE WHEN $2='oura' THEN next_live_sync
             WHEN $4 IS NULL AND NOT $5 THEN now()+interval '1 hour' ELSE next_live_sync END,
         next_sync=CASE WHEN $6 OR wake_requested OR ($5 AND claimed_wake) OR COALESCE($4,backfill_to) > backfill_from
             THEN now() ELSE LEAST(
                 CASE WHEN $2='oura' THEN next_live_sync
                      WHEN $4 IS NULL AND NOT $5 THEN now()+interval '1 hour' ELSE next_live_sync END,
                 (SELECT min(next_attempt)
                 FROM watch_tower.wearable_target_day AS targets
                 WHERE targets.subject_id=job.subject_id AND targets.provider=job.provider)) END,
         wake_requested=wake_requested OR ($5 AND claimed_wake) OR ($6 AND backfill_to<=backfill_from),
         failures=CASE WHEN $6 THEN failures ELSE 0 END,
         last_error=CASE WHEN $6 THEN 'target_day_unavailable' ELSE NULL END,
         throttled_until=CASE WHEN throttled_until>now() THEN throttled_until END,
         job_id=NULL,job_until=NULL,claimed_wake=false
         WHERE subject_id=$1 AND provider=$2 AND job_id=$3::text::uuid",
            &[&job.subject, &slug, &job.id, &next_boundary, &targeted, &retry_target],
        )
        .await?;
    ensure!(advanced == 1, "sync job changed");
    tx.commit().await?;
    Ok(())
}
