use anyhow::{ensure, Context, Result};
use avatar_hsm::etl_adapter;
use chrono::{DateTime, Duration, NaiveDate, Utc};
use deadpool_postgres::Pool;
use reqwest::Client;

struct Job {
    subject: String,
    provider: String,
    id: String,
    lower: DateTime<Utc>,
    upper: DateTime<Utc>,
    live_first: bool,
    target: Option<TargetDay>,
}

struct TargetDay {
    day: NaiveDate,
    revision: String,
}

async fn claim(pool: &Pool) -> Result<Option<Job>> {
    let mut db = pool.get().await?;
    let tx = db.transaction().await?;
    let row = tx
        .query_opt(
            "WITH due AS (
                 SELECT subject_id,provider,
                        wake_requested OR claimed_wake
                            OR (backfill_to>backfill_from AND next_live_sync<=now()) AS live_first,
                        job_id::text AS expired_run_id
                 FROM watch_tower.wearable_sync_schedule
                 WHERE next_sync <= now() AND (job_until IS NULL OR job_until <= now())
                   AND (throttled_until IS NULL OR throttled_until <= now())
                 ORDER BY next_sync LIMIT 1 FOR UPDATE SKIP LOCKED
             )
             UPDATE watch_tower.wearable_sync_schedule AS job
             SET job_id=gen_random_uuid(), job_until=now()+interval '5 minutes',
                 wake_requested=false, claimed_wake=due.live_first
             FROM due WHERE job.subject_id=due.subject_id AND job.provider=due.provider
             RETURNING job.subject_id,job.provider,job.job_id::text,job.backfill_from,
                       job.backfill_to,job.claimed_wake,due.expired_run_id",
            &[],
        )
        .await?;
    let Some(row) = row else {
        return Ok(None);
    };
    let subject: String = row.get(0);
    let provider: String = row.get(1);
    if let Some(expired_run_id) = row.get::<_, Option<String>>(6) {
        tx.execute(
            "UPDATE watch_tower.wearable_etl_run SET status='failed',
                 failure_code='collection_or_state_owner_failed',finished_at=now(),updated_at=now()
             WHERE subject_id=$1 AND run_id=$2::text::uuid AND status='running'",
            &[&subject, &expired_run_id],
        )
        .await?;
    }
    let target = tx
        .query_opt(
            "SELECT day,revision::text FROM watch_tower.wearable_target_day
             WHERE subject_id=$1 AND provider=$2 AND next_attempt<=now()
             ORDER BY next_attempt,day LIMIT 1",
            &[&subject, &provider],
        )
        .await?
        .map(|row| TargetDay {
            day: row.get(0),
            revision: row.get(1),
        });
    tx.commit().await?;
    Ok(Some(Job {
        subject,
        provider,
        id: row.get(2),
        lower: row.get(3),
        upper: row.get(4),
        live_first: row.get(5),
        target,
    }))
}

pub async fn worker(pool: Pool, client: Client) {
    loop {
        match claim(&pool).await {
            Ok(Some(job)) => {
                match begin_run(&pool, &job).await {
                    Ok(()) => match run(&pool, &client, &job).await {
                        Ok(()) => {}
                        Err(error) => {
                            let _ = finish_run(&pool, &job, Some(&error)).await;
                            let _ = defer_job(&pool, &job, &error).await;
                        }
                    },
                    Err(error) => {
                        let _ = defer_job(&pool, &job, &error).await;
                    }
                }
                tokio::time::sleep(std::time::Duration::from_secs(1)).await;
            }
            Ok(None) | Err(_) => tokio::time::sleep(std::time::Duration::from_secs(10)).await,
        }
    }
}

async fn begin_run(pool: &Pool, job: &Job) -> Result<()> {
    let inserted = pool
        .get()
        .await?
        .execute(
            "INSERT INTO watch_tower.wearable_etl_run(subject_id,run_id,provider,status)
             VALUES($1,$2::text::uuid,$3,'running')
             ON CONFLICT(subject_id,run_id) DO UPDATE SET
                 status='running',failure_code=NULL,finished_at=NULL,updated_at=now()
             WHERE watch_tower.wearable_etl_run.provider=EXCLUDED.provider
               AND watch_tower.wearable_etl_run.status='running'",
            &[&job.subject, &job.id, &job.provider],
        )
        .await?;
    ensure!(
        inserted == 1,
        "ETL run identity is already terminal or conflicting"
    );
    Ok(())
}

async fn finish_run(pool: &Pool, job: &Job, error: Option<&anyhow::Error>) -> Result<()> {
    let (status, code) = match error {
        None => ("succeeded", None),
        Some(error) => (
            "failed",
            Some(
                if provider_retry_at(&job.provider, error).is_some()
                    || etl_adapter::provider_retry_at(error).is_some()
                {
                    "provider_rate_limited"
                } else {
                    "collection_or_state_owner_failed"
                },
            ),
        ),
    };
    let updated = pool
        .get()
        .await?
        .execute(
            "UPDATE watch_tower.wearable_etl_run SET status=$3,
                 failure_code=$4,finished_at=now(),updated_at=now()
             WHERE subject_id=$1 AND run_id=$2::text::uuid AND provider=$5
               AND status='running'",
            &[&job.subject, &job.id, &status, &code, &job.provider],
        )
        .await?;
    ensure!(updated == 1, "ETL run is not running");
    Ok(())
}

fn provider_retry_at(provider: &str, error: &anyhow::Error) -> Option<DateTime<Utc>> {
    match provider {
        "hevy" => super::hevy::provider_retry_at(error),
        "liftosaur" => super::liftosaur::provider_retry_at(error),
        "strava" => super::strava::provider_retry_at(error),
        "oura" => super::oura::provider_retry_at(error),
        "polar" => super::polar::provider_retry_at(error),
        "intervals_icu" => super::intervals_icu::provider_retry_at(error),
        "ultrahuman" => super::ultrahuman::provider_retry_at(error),
        "whoop" => super::whoop::provider_retry_at(error),
        "garmin_personal" => super::garmin_personal::provider_retry_at(error),
        "withings" => super::withings::provider_retry_at(error),
        "suunto" => super::suunto::provider_retry_at(error),
        "coros" => super::coros::provider_retry_at(error),
        "sensorbio" => super::sensorbio::provider_retry_at(error),
        "google_health" => super::google::provider_retry_at(error),
        _ => None,
    }
}

async fn defer_job(pool: &Pool, job: &Job, error: &anyhow::Error) -> Result<()> {
    let retry_at =
        provider_retry_at(&job.provider, error).or_else(|| etl_adapter::provider_retry_at(error));
    let Some(retry_at) = retry_at else {
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
            &[&job.subject, &job.provider, &job.id, &retry_at],
        )
        .await?;
    Ok(())
}

async fn retry_job(pool: &Pool, job: &Job) -> Result<()> {
    if job.target.is_some() {
        return release_job(pool, job, None, true).await;
    }
    pool.get()
        .await?
        .execute(
            "UPDATE watch_tower.wearable_sync_schedule
             SET failures=failures+1,last_error='provider_or_storage_unavailable',
                 next_sync=now()+make_interval(mins=>LEAST(120,5*(failures+1))),
                 wake_requested=wake_requested OR claimed_wake,claimed_wake=false,
                 job_id=NULL,job_until=NULL
             WHERE subject_id=$1 AND provider=$2 AND job_id=$3::text::uuid",
            &[&job.subject, &job.provider, &job.id],
        )
        .await?;
    Ok(())
}

async fn run(pool: &Pool, client: &Client, job: &Job) -> Result<()> {
    if job.provider == "polar" && !etl_adapter::is_synthetic_subject(&job.subject) {
        etl_adapter::schedule_polar_physical_info(pool, &job.subject).await?;
    }
    if matches!(
        job.provider.as_str(),
        "hevy"
            | "liftosaur"
            | "strava"
            | "oura"
            | "polar"
            | "intervals_icu"
            | "ultrahuman"
            | "whoop"
            | "garmin_personal"
            | "withings"
            | "suunto"
            | "coros"
            | "sensorbio"
            | "google_health"
    ) {
        run_watch_tower_collector(pool, client, job).await
    } else {
        anyhow::bail!(
            "no Watch Tower collector is registered for provider {}",
            job.provider
        )
    }
}

async fn run_watch_tower_collector(pool: &Pool, client: &Client, job: &Job) -> Result<()> {
    let credentials = match job.provider.as_str() {
        "hevy" => vec![etl_adapter::hevy_access_token(pool, client, &job.subject).await?],
        "liftosaur" => {
            vec![etl_adapter::liftosaur_access_token(pool, client, &job.subject).await?]
        }
        "strava" => vec![etl_adapter::strava_access_token(pool, client, &job.subject).await?],
        "oura" => etl_adapter::oura_credentials(pool, client, &job.subject).await?,
        "intervals_icu" => {
            etl_adapter::intervals_icu_credentials(pool, client, &job.subject).await?
        }
        "polar" => etl_adapter::polar_credentials(pool, client, &job.subject).await?,
        "ultrahuman" => {
            vec![etl_adapter::ultrahuman_access_token(pool, client, &job.subject).await?]
        }
        "whoop" => etl_adapter::whoop_credentials(pool, client, &job.subject).await?,
        "garmin_personal" => {
            etl_adapter::garmin_personal_credentials(pool, client, &job.subject).await?
        }
        "withings" => etl_adapter::withings_credentials(pool, client, &job.subject).await?,
        "suunto" => etl_adapter::suunto_credentials(pool, client, &job.subject).await?,
        "coros" => etl_adapter::coros_credentials(pool, client, &job.subject).await?,
        "sensorbio" => etl_adapter::sensorbio_credentials(pool, client, &job.subject).await?,
        "google_health" => {
            etl_adapter::google_health_credentials(pool, client, &job.subject).await?
        }
        _ => anyhow::bail!("unsupported Watch Tower collector"),
    };
    let token = credentials
        .first()
        .filter(|token| !token.trim().is_empty())
        .context("HSM returned an empty provider credential")?;
    let scope = credentials.get(1).map(String::as_str);
    let now = Utc::now();
    let backfill = job.upper > job.lower && !job.live_first && job.target.is_none();
    let size = etl_adapter::sync_window_size(&job.provider)?;
    let (start, end) = if let Some(target) = &job.target {
        target_window(target.day, now)
    } else if backfill {
        (job.lower.max(job.upper - size), job.upper)
    } else {
        (now - size, now)
    };
    let lease =
        etl_adapter::issue_collection_lease(pool, &job.subject, &job.provider, &job.id).await?;
    let lease = lease["lease"].as_str().context("HSM lease is missing")?;
    let ranges = if matches!(
        job.provider.as_str(),
        "suunto" | "sensorbio" | "google_health"
    ) {
        job.target.as_ref().map_or_else(
            || vec![(start, end)],
            |target| single_day_target_windows(target.day, now),
        )
    } else {
        vec![(start, end)]
    };
    let replace_source_window = replaces_source_window(&job.provider, job.target.is_some());
    for (from, until) in ranges {
        let snapshot = match job.provider.as_str() {
            "hevy" => super::hevy::collect_window(client, token, lease, from, until).await?,
            "liftosaur" => {
                super::liftosaur::collect_window(client, token, lease, from, until).await?
            }
            "strava" => super::strava::collect_window(client, token, lease, from, until).await?,
            "oura" => super::oura::collect_window(client, token, scope, lease, from, until).await?,
            "intervals_icu" => {
                super::intervals_icu::collect_window(client, token, scope, lease, from, until)
                    .await?
            }
            "polar" => super::polar::collect_window(client, token, lease, from, until).await?,
            "ultrahuman" => {
                super::ultrahuman::collect_window(client, token, lease, from, until).await?
            }
            "whoop" => {
                super::whoop::collect_window(client, token, scope, lease, from, until).await?
            }
            "garmin_personal" => {
                super::garmin_personal::collect_window(client, token, lease, from, until).await?
            }
            "withings" => {
                super::withings::collect_window(client, token, scope, lease, from, until).await?
            }
            "suunto" => {
                let subscription = credentials
                    .get(1)
                    .context("Suunto subscription key missing")?;
                super::suunto::collect_window(client, token, subscription, lease, from, until)
                    .await?
            }
            "coros" => {
                let open_id = credentials.get(1).context("COROS openId missing")?;
                super::coros::collect_window(client, token, open_id, lease, from, until).await?
            }
            "sensorbio" => {
                super::sensorbio::collect_window(client, token, lease, from, until).await?
            }
            "google_health" => {
                super::google::collect_window(client, token, scope, lease, from, until).await?
            }
            _ => anyhow::bail!("unsupported Watch Tower collector"),
        };
        etl_adapter::apply_normalized_snapshot(
            pool,
            &job.subject,
            &job.provider,
            snapshot,
            replace_source_window,
            Some(&job.id),
        )
        .await?;
    }
    let next_boundary = if backfill && start > job.lower {
        start
            + if matches!(job.provider.as_str(), "polar" | "whoop") {
                Duration::days(2)
            } else {
                Duration::hours(12)
            }
    } else {
        start
    };
    release_job(pool, job, backfill.then_some(next_boundary), false).await
}

fn target_window(day: NaiveDate, now: DateTime<Utc>) -> (DateTime<Utc>, DateTime<Utc>) {
    let at = day.and_hms_opt(0, 0, 0).unwrap().and_utc();
    (at - Duration::days(1), (at + Duration::days(2)).min(now))
}

fn replaces_source_window(provider: &str, has_target_day: bool) -> bool {
    provider == "google_health" && has_target_day
}

fn single_day_target_windows(
    day: NaiveDate,
    now: DateTime<Utc>,
) -> Vec<(DateTime<Utc>, DateTime<Utc>)> {
    let midnight = day.and_hms_opt(0, 0, 0).unwrap().and_utc();
    [-12, 0, 12]
        .into_iter()
        .filter_map(|hours| {
            let start = midnight + Duration::hours(hours);
            let end = (start + Duration::days(1)).min(now);
            (start < end).then_some((start, end))
        })
        .collect()
}

async fn release_job(
    pool: &Pool,
    job: &Job,
    next_boundary: Option<DateTime<Utc>>,
    retry_target: bool,
) -> Result<()> {
    let mut db = pool.get().await?;
    let tx = db.transaction().await?;
    let active = tx
        .query_opt(
            "SELECT 1 FROM watch_tower.wearable_sync_schedule
             WHERE subject_id=$1 AND provider=$2 AND job_id=$3::text::uuid FOR UPDATE",
            &[&job.subject, &job.provider, &job.id],
        )
        .await?;
    ensure!(active.is_some(), "sync job changed");
    if let Some(target) = &job.target {
        let sql = if retry_target {
            "UPDATE watch_tower.wearable_target_day
             SET failures=LEAST(failures+1,24),
                 next_attempt=now()+make_interval(mins=>LEAST(120,5*(failures+1)))
             WHERE subject_id=$1 AND provider=$2 AND day=$3 AND revision=$4::text::uuid"
        } else {
            "DELETE FROM watch_tower.wearable_target_day
             WHERE subject_id=$1 AND provider=$2 AND day=$3 AND revision=$4::text::uuid"
        };
        tx.execute(
            sql,
            &[&job.subject, &job.provider, &target.day, &target.revision],
        )
        .await?;
    }
    let targeted = job.target.is_some();
    let advanced = tx
        .execute(
            "UPDATE watch_tower.wearable_sync_schedule AS job SET
                 backfill_to=COALESCE($4,backfill_to),
                 next_live_sync=CASE WHEN $2='oura' THEN next_live_sync
                     WHEN $4 IS NULL AND NOT $5 THEN now()+interval '1 hour' ELSE next_live_sync END,
                 next_sync=CASE WHEN $6 OR wake_requested OR ($5 AND claimed_wake)
                     OR COALESCE($4,backfill_to)>backfill_from THEN now() ELSE LEAST(
                         CASE WHEN $2='oura' THEN next_live_sync
                              WHEN $4 IS NULL AND NOT $5 THEN now()+interval '1 hour' ELSE next_live_sync END,
                         (SELECT min(next_attempt) FROM watch_tower.wearable_target_day AS targets
                          WHERE targets.subject_id=job.subject_id AND targets.provider=job.provider)) END,
                 wake_requested=wake_requested OR ($5 AND claimed_wake)
                     OR ($6 AND backfill_to<=backfill_from),
                 failures=CASE WHEN $6 THEN failures ELSE 0 END,
                 last_error=CASE WHEN $6 THEN 'target_day_unavailable' ELSE NULL END,
                 throttled_until=CASE WHEN throttled_until>now() THEN throttled_until END,
                 job_id=NULL,job_until=NULL,claimed_wake=false
             WHERE subject_id=$1 AND provider=$2 AND job_id=$3::text::uuid",
            &[
                &job.subject,
                &job.provider,
                &job.id,
                &next_boundary,
                &targeted,
                &retry_target,
            ],
        )
        .await?;
    ensure!(advanced == 1, "sync job changed");
    if !retry_target {
        let completed = tx
            .execute(
                "UPDATE watch_tower.wearable_etl_run SET status='succeeded',
                     finished_at=now(),updated_at=now()
                 WHERE subject_id=$1 AND run_id=$2::text::uuid AND provider=$3
                   AND status='running'",
                &[&job.subject, &job.id, &job.provider],
            )
            .await?;
        ensure!(completed == 1, "ETL run is not running");
    }
    tx.commit().await?;
    Ok(())
}

#[cfg(test)]
#[path = "wearable_sync_tests.rs"]
mod tests;
