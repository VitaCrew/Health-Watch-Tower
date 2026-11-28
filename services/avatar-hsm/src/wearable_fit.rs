use crate::{
    cloud_wearables::{self, garmin, Provider},
    mcp::App,
    store::SUBJECT,
    wearable_oauth, wearable_webhooks,
};
use anyhow::{ensure, Context, Result};
use axum::{
    extract::{Path, Query, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
};
use base64::{engine::general_purpose::STANDARD, Engine};
use deadpool_postgres::Pool;
use reqwest::{Client, Url};
use serde::Deserialize;
#[cfg(test)]
use std::io::{Cursor, Read};
#[cfg(test)]
use std::time::Duration;
use std::{collections::BTreeMap, sync::Arc};
use subtle::ConstantTimeEq;
use tokio_postgres::Transaction;

#[cfg(test)]
#[path = "wearable_fit_tests.rs"]
mod tests;
#[cfg(test)]
pub(crate) use tests::fixture as fit_fixture;

const MAX_FILE: usize = 12 * 1024 * 1024;
pub(crate) const MAX_PARTS: usize = 16;
const MAX_BATCH: usize = 48 * 1024 * 1024;
pub(crate) type CorosFitSources = BTreeMap<String, Vec<(i16, Url)>>;

pub struct FitDownloadRequest {
    pub url: String,
    pub auth: FitDownloadAuth,
}

pub enum FitDownloadAuth {
    Bearer(String),
    Basic { username: String, password: String },
}

pub struct CorosFitDownloadPlan {
    pub activity_id: String,
    pub access_token: String,
    pub open_id: String,
    pub mode: String,
    pub submode: String,
    pub cached_sources: Vec<(i16, String)>,
}

pub struct PolarFitDownloadPlan {
    pub activity_id: String,
    pub access_token: String,
    pub fetch_summary: bool,
}

pub struct SuuntoFitDownloadPlan {
    pub workout_key: String,
    pub access_token: String,
    pub subscription_key: String,
    pub fetch_summary: bool,
}

impl FitDownloadRequest {
    pub(crate) fn bearer(url: String, token: String) -> Self {
        Self {
            url,
            auth: FitDownloadAuth::Bearer(token),
        }
    }

    pub(crate) fn basic(url: String, username: String, password: String) -> Self {
        Self {
            url,
            auth: FitDownloadAuth::Basic { username, password },
        }
    }
}

pub(crate) fn valid_id(id: &str) -> bool {
    !id.is_empty()
        && id.len() <= 128
        && id
            .bytes()
            .all(|c| c.is_ascii_alphanumeric() || b"-_".contains(&c))
}

pub async fn enqueue(
    tx: &Transaction<'_>,
    subject: &str,
    provider: &str,
    activity: &str,
    category: Option<&str>,
) -> Result<()> {
    let supported = match provider {
        "polar" | "coros" | "garmin_personal" | "intervals_icu" => valid_id(activity),
        "suunto" => valid_suunto_key(activity),
        _ => false,
    };
    if supported {
        if provider == "intervals_icu" {
            let connection = tx
                .query_opt(
                    "SELECT intervals_activity_granted FROM avatar_hsm.wearable_oauth_connection
                 WHERE subject_id=$1 AND provider='intervals_icu' FOR SHARE",
                    &[&subject],
                )
                .await?;
            if connection.and_then(|row| row.get::<_, Option<bool>>(0)) == Some(false) {
                return Ok(());
            }
        }
        tx.execute(
            "INSERT INTO watch_tower.wearable_fit_job(subject_id,provider,activity_id,exercise_type)
            VALUES($1,$2,$3,$4) ON CONFLICT DO NOTHING",
            &[&subject, &provider, &activity, &category],
        )
        .await?;
    }
    Ok(())
}

pub(crate) async fn enqueue_coros_workouts(
    tx: &Transaction<'_>,
    subject: &str,
    records: &[crate::wearables::Record],
    sources: &CorosFitSources,
) -> Result<()> {
    let workouts: Vec<_> = records
        .iter()
        .filter(|record| record.kind == "workout" && valid_id(&record.source_id))
        .map(|record| {
            let mut row = serde_json::to_value(record)?;
            if let Some(links) = sources.get(&record.source_id) {
                // A missing encryption key still permits fetching through the detail API.
                if let Ok(sealed) =
                    wearable_oauth::seal_coros_fit_sources(subject, &record.source_id, links)
                {
                    row["sealed_source"] = serde_json::json!(STANDARD.encode(sealed));
                }
            }
            Ok::<_, serde_json::Error>(row)
        })
        .collect::<std::result::Result<_, _>>()?;
    if workouts.is_empty() {
        return Ok(());
    }
    let workouts = serde_json::Value::Array(workouts);
    tx.execute(
        "WITH incoming AS (
           SELECT DISTINCT ON (source_id) source_id, source, started_at, ended_at, modified_at, payload, sealed_source
           FROM jsonb_to_recordset($2) AS r(source_id text,source text,started_at timestamptz,
             ended_at timestamptz,modified_at timestamptz,payload jsonb,sealed_source text)
           ORDER BY source_id,modified_at DESC,source
         ), eligible AS (
           SELECT current.source_id,current.payload->>'exercise_type' AS category,current.sealed_source,
             old.source_id IS NOT NULL AND
               (old.source,old.started_at,old.ended_at,old.payload) IS DISTINCT FROM
               (current.source,current.started_at,current.ended_at,current.payload) AS changed
           FROM incoming current LEFT JOIN LATERAL (
             SELECT source_id,source,started_at,ended_at,modified_at,payload
             FROM avatar_hsm.wearable_record
             WHERE subject_id=$1 AND provider='coros' AND kind='workout' AND source_id=current.source_id
             ORDER BY modified_at DESC,source LIMIT 1
           ) old ON true
           WHERE current.modified_at>=old.modified_at OR old.modified_at IS NULL
         )
         INSERT INTO watch_tower.wearable_fit_job AS job(subject_id,provider,activity_id,exercise_type,sealed_source)
         SELECT $1,'coros',source_id,category,decode(sealed_source,'base64') FROM eligible
         ON CONFLICT (subject_id,provider,activity_id) DO UPDATE
           SET exercise_type=excluded.exercise_type,status='pending',attempts=0,
               next_attempt=now(),job_id=NULL,leased_until=NULL,sealed_source=excluded.sealed_source
         WHERE job.exercise_type IS DISTINCT FROM excluded.exercise_type
           OR EXISTS (SELECT 1 FROM eligible WHERE source_id=job.activity_id AND changed)
           OR (job.status='pending' AND job.sealed_source IS NULL AND excluded.sealed_source IS NOT NULL
               AND job.attempts=0 AND job.next_attempt<=now()
               AND (job.leased_until IS NULL OR job.leased_until<=now()))",
        &[&subject, &workouts],
    ).await?;
    Ok(())
}

pub(crate) fn valid_suunto_key(key: &str) -> bool {
    key.len() == 24 && key.bytes().all(|byte| byte.is_ascii_hexdigit())
}

pub(crate) async fn enqueue_workout_notification(
    pool: &Pool,
    provider: Provider,
    owner: &str,
    activity: &str,
) -> Result<()> {
    let valid = match provider {
        Provider::Suunto => valid_suunto_key(activity),
        Provider::Polar => valid_id(activity),
        _ => false,
    };
    if !valid {
        return Ok(());
    }
    let provider = provider.slug();
    let account = wearable_webhooks::hash(owner.as_bytes());
    let mut db = pool.get().await?;
    let tx = db.transaction().await?;
    let matched = tx
        .query(
            "SELECT d.subject_id FROM avatar_hsm.device_connection d
         JOIN avatar_hsm.wearable_oauth_connection c USING (subject_id,provider)
         WHERE d.provider=$2 AND d.state<>'revoked' AND c.provider_account_hash=$1
         ORDER BY d.subject_id FOR UPDATE OF d",
            &[&account, &provider],
        )
        .await?;
    for row in matched {
        let subject: &str = row.get(0);
        // Recheck the owner after taking the same device row lock used by revocation.
        tx.execute(
            "INSERT INTO watch_tower.wearable_fit_job(subject_id,provider,activity_id)
             SELECT subject_id,provider,$2 FROM avatar_hsm.wearable_oauth_connection
             WHERE subject_id=$1 AND provider=$4 AND provider_account_hash=$3
             ON CONFLICT DO NOTHING",
            &[&subject, &activity, &account, &provider],
        )
        .await?;
    }
    tx.commit().await?;
    Ok(())
}

#[cfg(test)]
fn suunto_request(
    client: &Client,
    key: &str,
    token: &str,
    subscription: &str,
) -> Result<reqwest::RequestBuilder> {
    ensure!(valid_suunto_key(key), "invalid Suunto workout key");
    Ok(client
        .get(format!("https://cloudapi.suunto.com/v3/workouts/{key}/fit"))
        .bearer_auth(token)
        .header("Ocp-Apim-Subscription-Key", subscription))
}

pub(crate) struct Job {
    pub(crate) subject: String,
    pub(crate) provider: Provider,
    pub(crate) activity: String,
    category: Option<String>,
    pub(crate) id: String,
    sealed_source: Option<Vec<u8>>,
}

pub(crate) fn garmin_source(raw: &str) -> Result<Url> {
    ensure!(raw.len() <= 4096, "Garmin FIT URL exceeds size limit");
    let url = Url::parse(raw).map_err(|_| anyhow::anyhow!("invalid Garmin FIT URL"))?;
    ensure!(
        url.scheme() == "https"
            && url.host_str() == Some("apis.garmin.com")
            && url.port_or_known_default() == Some(443)
            && url.username().is_empty()
            && url.password().is_none()
            && url.fragment().is_none()
            && url.path() == "/wellness-api/rest/activityFile",
        "untrusted Garmin FIT URL"
    );
    Ok(url)
}

pub(crate) async fn enqueue_garmin(
    tx: &Transaction<'_>,
    subject: &str,
    activity: &str,
    url: &Url,
) -> Result<()> {
    ensure!(valid_id(activity), "invalid Garmin activity ID");
    let sealed = wearable_oauth::seal_garmin_fit_url(subject, activity, url)?;
    tx.execute("INSERT INTO watch_tower.wearable_fit_job AS job(subject_id,provider,activity_id,sealed_source)
        VALUES($1,'garmin',$2,$3) ON CONFLICT(subject_id,provider,activity_id) DO UPDATE
        SET sealed_source=excluded.sealed_source,status='pending',attempts=0,next_attempt=now(),job_id=NULL,leased_until=NULL
        WHERE job.status<>'stored' AND (job.leased_until IS NULL OR job.leased_until<=now())", &[&subject,&activity,&sealed]).await?;
    Ok(())
}

pub(crate) async fn claim(pool: &Pool) -> Result<Option<Job>> {
    let row=pool.get().await?.query_opt("WITH due AS (
        SELECT j.subject_id,j.provider,j.activity_id FROM watch_tower.wearable_fit_job j
        WHERE status='pending' AND next_attempt<=now() AND (leased_until IS NULL OR leased_until<=now())
          AND NOT EXISTS (SELECT 1 FROM avatar_hsm.wearable_oauth_connection c
                          WHERE c.subject_id=j.subject_id AND c.provider=j.provider
                            AND ((c.provider='garmin' AND c.garmin_retry_after>now())
                              OR (c.provider='intervals_icu' AND c.intervals_activity_granted IS FALSE)))
          AND NOT EXISTS (SELECT 1 FROM watch_tower.wearable_sync_schedule s
                          WHERE s.subject_id=j.subject_id AND s.provider=j.provider
                            AND s.throttled_until>now())
        ORDER BY next_attempt LIMIT 1 FOR UPDATE OF j SKIP LOCKED)
        UPDATE watch_tower.wearable_fit_job j SET job_id=gen_random_uuid(),leased_until=now()+interval '2 minutes'
        FROM due WHERE (j.subject_id,j.provider,j.activity_id)=(due.subject_id,due.provider,due.activity_id)
        RETURNING j.subject_id,j.provider,j.activity_id,j.exercise_type,j.job_id::text,j.sealed_source",&[]).await?;
    row.map(|row| {
        Ok(Job {
            subject: row.get(0),
            provider: Provider::parse(&row.get::<_, String>(1))?,
            activity: row.get(2),
            category: row.get(3),
            id: row.get(4),
            sealed_source: row.get(5),
        })
    })
    .transpose()
}

#[cfg(test)]
#[cfg(test)]
pub async fn worker(pool: Pool, client: Client) {
    loop {
        if let Ok(Some(job)) = claim(&pool).await {
            let result =
                tokio::time::timeout(Duration::from_secs(100), run_job(&pool, &client, &job)).await;
            if !matches!(result, Ok(Ok(()))) {
                let retry_at = match &result {
                    Ok(Err(error)) => error
                        .downcast_ref::<cloud_wearables::ProviderThrottled>()
                        .map(|throttle| throttle.retry_at),
                    _ => None,
                };
                let _ = defer_job(&pool, &job, retry_at).await;
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
        } else {
            tokio::time::sleep(Duration::from_secs(12)).await;
        }
    }
}

pub(crate) async fn defer_job(
    pool: &Pool,
    job: &Job,
    retry_at: Option<chrono::DateTime<chrono::Utc>>,
) -> Result<()> {
    let garmin_retry = retry_at.filter(|_| matches!(job.provider, Provider::Garmin));
    let mut db = pool.get().await?;
    let tx = db.transaction().await?;
    if garmin_retry.is_some() {
        crate::garmin_backfill::lock_account(&tx, &job.subject).await?;
    }
    if retry_at.is_some() {
        // Follow unlink's lock order before changing both the file and sync queues.
        tx.query_opt(
            "SELECT 1 FROM avatar_hsm.device_connection
             WHERE subject_id=$1 AND provider=$2 FOR UPDATE",
            &[&job.subject, &job.provider.slug()],
        )
        .await?;
    }
    let updated = tx.execute("UPDATE watch_tower.wearable_fit_job SET
                        attempts=attempts+CASE WHEN $5::timestamptz IS NULL THEN 1 ELSE 0 END,
                        status=CASE WHEN $5 IS NULL AND attempts>=5 THEN 'unavailable' ELSE 'pending' END,
                        next_attempt=COALESCE($5,now()+make_interval(hours=>LEAST(attempts+1,24))),
                        job_id=NULL,leased_until=NULL,
                        sealed_source=CASE WHEN $5 IS NULL AND attempts>=5 THEN NULL ELSE sealed_source END
                        WHERE subject_id=$1 AND provider=$2 AND activity_id=$3 AND job_id=$4::text::uuid",
                        &[&job.subject,&job.provider.slug(),&job.activity,&job.id,&retry_at]).await?;
    if let (1, Some(retry_at)) = (updated, garmin_retry) {
        crate::garmin_backfill::defer_account(&tx, &job.subject, retry_at).await?;
    }
    if let (1, Some(retry_at)) = (updated, retry_at) {
        tx.execute(
            "UPDATE watch_tower.wearable_sync_schedule
             SET throttled_until=GREATEST(throttled_until,$3)
             WHERE subject_id=$1 AND provider=$2",
            &[&job.subject, &job.provider.slug(), &retry_at],
        )
        .await?;
    }
    tx.commit().await?;
    Ok(())
}

#[cfg(test)]
async fn run_job(pool: &Pool, client: &Client, job: &Job) -> Result<()> {
    let credentials = if job.subject == SUBJECT {
        cloud_wearables::configured(job.provider).context("FIT credentials missing")?
    } else {
        wearable_oauth::access(client, pool, &job.subject, job.provider).await?
    };
    let result = match job.provider {
        Provider::Garmin => {
            let sealed = job
                .sealed_source
                .clone()
                .context("Garmin FIT source is missing")?;
            let source = wearable_oauth::open_garmin_fit_url(&job.subject, &job.activity, sealed)?;
            client
                .get(garmin_source(&source)?)
                .bearer_auth(&credentials[0])
                .send()
                .await
                .map_err(|error| error.without_url())?
        }
        Provider::Polar => {
            return polar_fit(
                pool,
                client,
                job,
                &credentials[0],
                "https://www.polaraccesslink.com",
            )
            .await;
        }
        Provider::GarminPersonal => {
            client
                .get(format!(
                    "{}/download-service/files/activity/{}",
                    garmin::BASE,
                    job.activity
                ))
                .bearer_auth(garmin::access(client, &credentials[0]).await?)
                .send()
                .await?
        }
        Provider::Suunto => {
            ensure_suunto_summary(
                pool,
                client,
                job,
                &credentials,
                "https://cloudapi.suunto.com",
            )
            .await?;
            suunto_request(client, &job.activity, &credentials[0], &credentials[1])?
                .send()
                .await
                .map_err(|error| error.without_url())?
        }
        Provider::IntervalsIcu => {
            let file =
                intervals_original(client, &job.activity, &credentials, "https://intervals.icu")
                    .await?;
            return save_parts(pool, job, vec![(0, file)]).await;
        }
        Provider::Coros => {
            let allowed = std::env::var("AVATAR_COROS_FIT_HOSTS").unwrap_or_default();
            let files = coros_fit_parts(
                client,
                job,
                &credentials,
                "https://open.coros.com",
                &allowed,
                |url| async move {
                    read_original(
                        client
                            .get(url)
                            .send()
                            .await
                            .map_err(reqwest::Error::without_url)?,
                    )
                    .await
                },
            )
            .await?;
            return save_parts(pool, job, files).await;
        }
        _ => anyhow::bail!("provider does not support FIT download"),
    };
    save_parts(pool, job, vec![(0, read_original(result).await?)]).await
}

/// Resolve the provider request in HSM, where OAuth secrets and signed Garmin
/// URLs are owned. Watch Tower receives only an ephemeral, fully prepared HTTP
/// request and sends the bytes through its ETL worker.
pub(crate) async fn watch_tower_request(
    pool: &Pool,
    client: &Client,
    job: &Job,
) -> Result<Option<FitDownloadRequest>> {
    let credentials = if job.subject == SUBJECT {
        cloud_wearables::configured(job.provider).context("FIT credentials missing")?
    } else {
        wearable_oauth::access(client, pool, &job.subject, job.provider).await?
    };
    let request = match job.provider {
        Provider::Garmin => {
            let sealed = job
                .sealed_source
                .clone()
                .context("Garmin FIT source is missing")?;
            let source = wearable_oauth::open_garmin_fit_url(&job.subject, &job.activity, sealed)?;
            FitDownloadRequest::bearer(garmin_source(&source)?.to_string(), credentials[0].clone())
        }
        Provider::GarminPersonal => FitDownloadRequest::bearer(
            format!(
                "{}/download-service/files/activity/{}",
                garmin::BASE,
                job.activity
            ),
            garmin::access(client, &credentials[0]).await?,
        ),
        Provider::IntervalsIcu => {
            ensure!(valid_id(&job.activity), "invalid Intervals.icu activity ID");
            let token = credentials
                .first()
                .context("Intervals.icu credentials missing")?;
            if let Some(scope) = credentials.get(1) {
                ensure!(
                    cloud_wearables::manufacturers::intervals_scope(scope, "ACTIVITY"),
                    "Intervals.icu activity read access is required"
                );
                FitDownloadRequest::bearer(
                    format!(
                        "https://intervals.icu/api/v1/activity/{}/file",
                        job.activity
                    ),
                    token.clone(),
                )
            } else {
                FitDownloadRequest::basic(
                    format!(
                        "https://intervals.icu/api/v1/activity/{}/file",
                        job.activity
                    ),
                    "API_KEY".to_owned(),
                    token.clone(),
                )
            }
        }
        _ => return Ok(None),
    };
    Ok(Some(request))
}

/// Return only the ephemeral credentials and allowlisted signed sources needed
/// by Watch Tower. Provider HTTP stays outside HSM; HSM keeps OAuth and URL
/// ownership checks.
pub(crate) async fn watch_tower_coros_plan(
    pool: &Pool,
    client: &Client,
    job: &Job,
) -> Result<CorosFitDownloadPlan> {
    ensure!(job.provider.slug() == "coros", "FIT job is not COROS");
    ensure!(valid_id(&job.activity), "invalid COROS activity ID");
    let lease_live = pool
        .get()
        .await?
        .query_opt(
            "SELECT 1 FROM watch_tower.wearable_fit_job
             WHERE subject_id=$1 AND provider='coros' AND activity_id=$2
               AND job_id=$3::text::uuid AND leased_until>now()",
            &[&job.subject, &job.activity, &job.id],
        )
        .await?
        .is_some();
    ensure!(lease_live, "COROS FIT job lease expired or was revoked");
    let credentials = if job.subject == SUBJECT {
        cloud_wearables::configured(job.provider).context("COROS FIT credentials missing")?
    } else {
        wearable_oauth::access(client, pool, &job.subject, job.provider).await?
    };
    let access_token = credentials
        .first()
        .context("COROS access token missing")?
        .clone();
    let open_id = credentials.get(1).context("COROS open ID missing")?.clone();
    let (mode, submode) = job
        .category
        .as_deref()
        .and_then(|value| value.strip_prefix("coros:"))
        .and_then(|value| value.split_once(':'))
        .context("missing COROS workout type")?;
    mode.parse::<i32>()?;
    submode.parse::<i32>()?;
    let allowlist = std::env::var("AVATAR_COROS_FIT_HOSTS").unwrap_or_default();
    let cached_sources = job
        .sealed_source
        .as_ref()
        .and_then(|sealed| {
            wearable_oauth::open_coros_fit_sources(&job.subject, &job.activity, sealed.clone()).ok()
        })
        .unwrap_or_default()
        .into_iter()
        .filter_map(|(part, raw)| {
            let url = Url::parse(&raw).ok()?;
            allowed_host(&url, &allowlist).then(|| (part, url.to_string()))
        })
        .collect();
    Ok(CorosFitDownloadPlan {
        activity_id: job.activity.clone(),
        access_token,
        open_id,
        mode: mode.to_owned(),
        submode: submode.to_owned(),
        cached_sources,
    })
}

async fn watch_tower_fit_credentials(
    pool: &Pool,
    client: &Client,
    job: &Job,
) -> Result<Vec<String>> {
    let lease_live = pool
        .get()
        .await?
        .query_opt(
            "SELECT 1 FROM watch_tower.wearable_fit_job
             WHERE subject_id=$1 AND provider=$2 AND activity_id=$3
               AND job_id=$4::text::uuid AND leased_until>now()",
            &[&job.subject, &job.provider.slug(), &job.activity, &job.id],
        )
        .await?
        .is_some();
    ensure!(lease_live, "FIT job lease expired or was revoked");
    if job.subject == SUBJECT {
        cloud_wearables::configured(job.provider).context("FIT credentials missing")
    } else {
        wearable_oauth::access(client, pool, &job.subject, job.provider).await
    }
}

pub(crate) async fn watch_tower_polar_plan(
    pool: &Pool,
    client: &Client,
    job: &Job,
) -> Result<PolarFitDownloadPlan> {
    ensure!(job.provider.slug() == "polar", "FIT job is not Polar");
    ensure!(valid_id(&job.activity), "invalid Polar exercise ID");
    let credentials = watch_tower_fit_credentials(pool, client, job).await?;
    let access_token = credentials
        .first()
        .context("Polar access token missing")?
        .clone();
    Ok(PolarFitDownloadPlan {
        activity_id: job.activity.clone(),
        access_token,
        fetch_summary: !has_summary(pool, job).await?,
    })
}

pub(crate) async fn watch_tower_suunto_plan(
    pool: &Pool,
    client: &Client,
    job: &Job,
) -> Result<SuuntoFitDownloadPlan> {
    ensure!(job.provider.slug() == "suunto", "FIT job is not Suunto");
    ensure!(
        valid_suunto_key(&job.activity),
        "invalid Suunto workout key"
    );
    let credentials = watch_tower_fit_credentials(pool, client, job).await?;
    let access_token = credentials
        .first()
        .context("Suunto access token missing")?
        .clone();
    let subscription_key = credentials
        .get(1)
        .context("Suunto subscription key missing")?
        .clone();
    Ok(SuuntoFitDownloadPlan {
        workout_key: job.activity.clone(),
        access_token,
        subscription_key,
        fetch_summary: !has_summary(pool, job).await?,
    })
}

pub(crate) async fn commit_watch_tower_summary(
    pool: &Pool,
    job: &Job,
    record: crate::wearables::Record,
) -> Result<()> {
    validate_watch_tower_summary(job, &record)?;
    store_summary(pool, job, &record).await
}

pub(crate) fn validate_watch_tower_summary(
    job: &Job,
    record: &crate::wearables::Record,
) -> Result<()> {
    ensure!(
        matches!(job.provider, Provider::Polar | Provider::Suunto),
        "provider FIT summary is unsupported"
    );
    ensure!(
        record.kind == "workout"
            && record.source_id == job.activity
            && !record.source.trim().is_empty()
            && record.source.len() <= 256,
        "normalized workout identity does not match the FIT job"
    );
    ensure!(
        record.started_at < record.ended_at
            && record.payload.is_object()
            && record.payload["exercise_type"]
                .as_str()
                .is_some_and(|value| !value.trim().is_empty() && value.len() <= 256),
        "normalized workout fields are invalid"
    );
    Ok(())
}

pub(crate) async fn save_watch_tower_download(
    pool: &Pool,
    job: &Job,
    bytes: Vec<u8>,
) -> Result<()> {
    save_parts(pool, job, vec![(0, bytes)]).await
}

#[cfg(test)]
async fn intervals_original(
    client: &Client,
    activity: &str,
    credentials: &[String],
    origin: &str,
) -> Result<Vec<u8>> {
    ensure!(valid_id(activity), "invalid Intervals.icu activity ID");
    let token = credentials
        .first()
        .context("Intervals.icu credentials missing")?;
    let request = client.get(format!("{origin}/api/v1/activity/{activity}/file"));
    let request = if let Some(scope) = credentials.get(1) {
        ensure!(
            cloud_wearables::manufacturers::intervals_scope(scope, "ACTIVITY"),
            "Intervals.icu activity read access is required"
        );
        request.bearer_auth(token)
    } else {
        request.basic_auth("API_KEY", Some(token))
    };
    read_original(request.send().await.map_err(reqwest::Error::without_url)?).await
}

#[cfg(test)]
async fn coros_fit_parts<F, Fut>(
    client: &Client,
    job: &Job,
    credentials: &[String],
    origin: &str,
    allowed: &str,
    download: F,
) -> Result<Vec<(i16, Vec<u8>)>>
where
    F: Fn(Url) -> Fut,
    Fut: std::future::Future<Output = Result<Vec<u8>>>,
{
    let read_parts = |links: Vec<(i16, Url)>| {
        let download = &download;
        async move {
            let mut files = Vec::new();
            let mut total = 0;
            for (part, url) in links {
                let file = download(url).await?;
                total += file.len();
                ensure!(total <= MAX_BATCH, "FIT activity exceeds total size limit");
                files.push((part, file));
            }
            Ok(files)
        }
    };
    if let Some(sealed) = &job.sealed_source {
        let links =
            wearable_oauth::open_coros_fit_sources(&job.subject, &job.activity, sealed.clone())
                .and_then(|links| {
                    ensure!(
                        !links.is_empty() && links.len() <= MAX_PARTS + 1,
                        "invalid COROS FIT batch"
                    );
                    links
                        .into_iter()
                        .map(|(part, raw)| {
                            let url = Url::parse(&raw)
                                .map_err(|_| anyhow::anyhow!("invalid COROS FIT link"))?;
                            ensure!(
                                allowed_host(&url, allowed),
                                "COROS FIT host has not been approved"
                            );
                            Ok((part, url))
                        })
                        .collect()
                });
        if let Ok(links) = links {
            match read_parts(links).await {
                Ok(files) => return Ok(files),
                Err(error) if error.is::<cloud_wearables::ProviderThrottled>() => {
                    return Err(error)
                }
                Err(_) => {}
            }
        }
    }

    let (mode, submode) = job
        .category
        .as_deref()
        .and_then(|s| s.strip_prefix("coros:"))
        .and_then(|s| s.split_once(':'))
        .context("missing COROS workout type")?;
    mode.parse::<i32>()?;
    submode.parse::<i32>()?;
    let response = client
        .get(format!("{origin}/v2/coros/sport/detail/fit"))
        .query(&[
            ("mode", mode),
            ("subMode", submode),
            ("labelId", &job.activity),
            ("token", &credentials[0]),
            ("openId", &credentials[1]),
        ])
        .send()
        .await
        .map_err(reqwest::Error::without_url)?;
    let body = cloud_wearables::body(response).await.map_err(|error| {
        match error.downcast::<reqwest::Error>() {
            Ok(error) => error.without_url().into(),
            Err(error) => error,
        }
    })?;
    let data = &body["data"];
    ensure!(
        body["result"] == "0000"
            && wearable_webhooks::account_id(&data["labelId"]).as_deref()
                == Some(job.activity.as_str()),
        "COROS FIT detail unavailable for this activity"
    );
    read_parts(coros_links(data, allowed)?).await
}

#[cfg(test)]
async fn polar_fit(
    pool: &Pool,
    client: &Client,
    job: &Job,
    token: &str,
    origin: &str,
) -> Result<()> {
    ensure_polar_summary(pool, client, job, token, origin).await?;
    let response = client
        .get(format!("{origin}/v3/exercises/{}/fit", job.activity))
        .bearer_auth(token)
        .send()
        .await
        .map_err(|error| error.without_url())?;
    save_parts(pool, job, vec![(0, read_original(response).await?)]).await
}

#[cfg(test)]
async fn ensure_suunto_summary(
    pool: &Pool,
    client: &Client,
    job: &Job,
    credentials: &[String],
    origin: &str,
) -> Result<()> {
    if has_summary(pool, job).await? {
        return Ok(());
    }
    ensure!(
        valid_suunto_key(&job.activity),
        "invalid Suunto workout key"
    );
    let response = client
        .get(format!("{origin}/v3/workouts/{}", job.activity))
        .query(&[("extensions", "PauseMarkerExtension,SummaryExtension")])
        .bearer_auth(&credentials[0])
        .header("Ocp-Apim-Subscription-Key", &credentials[1])
        .send()
        .await
        .map_err(|error| error.without_url())?;
    let body = cloud_wearables::body(response).await?;
    let record = cloud_wearables::manufacturers::suunto_detail(&body, &job.activity)?;
    store_summary(pool, job, &record).await
}

#[cfg(test)]
async fn ensure_polar_summary(
    pool: &Pool,
    client: &Client,
    job: &Job,
    token: &str,
    origin: &str,
) -> Result<()> {
    ensure!(valid_id(&job.activity), "invalid Polar exercise ID");
    if has_summary(pool, job).await? {
        return Ok(());
    }
    let response = client
        .get(format!("{origin}/v3/exercises/{}", job.activity))
        .bearer_auth(token)
        .query(&[("zones", true)])
        .send()
        .await
        .map_err(|error| error.without_url())?;
    let body = cloud_wearables::body(response).await?;
    let record = cloud_wearables::manufacturers::polar_detail(&body, &job.activity)?;
    store_summary(pool, job, &record).await
}

async fn has_summary(pool: &Pool, job: &Job) -> Result<bool> {
    Ok(pool
        .get()
        .await?
        .query_opt(
            "SELECT 1 FROM avatar_hsm.wearable_record
         WHERE subject_id=$1 AND provider=$2 AND kind='workout' AND source_id=$3 LIMIT 1",
            &[&job.subject, &job.provider.slug(), &job.activity],
        )
        .await?
        .is_some())
}

async fn store_summary(pool: &Pool, job: &Job, record: &crate::wearables::Record) -> Result<()> {
    let provider = job.provider.slug();
    let mut db = pool.get().await?;
    let tx = db.transaction().await?;
    let changed = tx
        .execute(
            "UPDATE avatar_hsm.device_connection SET state='synced',last_synced_at=now(),
         grants=ARRAY(SELECT DISTINCT unnest(grants || ARRAY['workout']::text[]))
         WHERE subject_id=$1 AND provider=$2 AND state<>'revoked'",
            &[&job.subject, &provider],
        )
        .await?;
    ensure!(changed == 1, "workout connection was revoked");
    let live = tx
        .query_opt(
            "SELECT 1 FROM watch_tower.wearable_fit_job
         WHERE subject_id=$1 AND provider=$2 AND activity_id=$3
           AND job_id=$4::text::uuid AND leased_until>now() FOR UPDATE",
            &[&job.subject, &provider, &job.activity, &job.id],
        )
        .await?;
    ensure!(live.is_some(), "workout job expired or was revoked");
    crate::wearables::upsert_record(&tx, &job.subject, provider, record).await?;
    tx.commit().await?;
    Ok(())
}

pub(crate) fn coros_links(data: &serde_json::Value, allowed: &str) -> Result<Vec<(i16, Url)>> {
    let parse = |point: &serde_json::Value| -> Result<Url> {
        let link = point["fitUrl"]
            .as_str()
            .filter(|url| !url.is_empty() && url.len() <= 500)
            .context("COROS FIT link unavailable")?;
        let url = Url::parse(link).map_err(|_| anyhow::anyhow!("invalid COROS FIT link"))?;
        ensure!(
            allowed_host(&url, allowed),
            "COROS FIT host has not been approved"
        );
        Ok(url)
    };
    let mut links = Vec::new();
    if data["fitUrl"].as_str().is_some_and(|url| !url.is_empty()) {
        links.push((0, parse(data)?));
    }
    if let Some(parts) = data
        .get("triathlonItemList")
        .filter(|value| !value.is_null())
    {
        let parts = parts.as_array().context("invalid COROS FIT parts")?;
        ensure!(parts.len() <= MAX_PARTS, "too many COROS FIT parts");
        for (index, part) in parts.iter().enumerate() {
            links.push(((index + 1) as i16, parse(part)?));
        }
    }
    ensure!(!links.is_empty(), "COROS FIT link unavailable");
    Ok(links)
}

#[cfg(test)]
async fn read_original(response: reqwest::Response) -> Result<Vec<u8>> {
    let mut response = cloud_wearables::check_provider_response(response)?;
    let mut bytes = Vec::new();
    while let Some(chunk) = response.chunk().await.map_err(|e| e.without_url())? {
        ensure!(
            bytes.len() + chunk.len() <= MAX_FILE,
            "FIT download exceeds size limit"
        );
        bytes.extend_from_slice(&chunk);
    }
    raw_fit(bytes)
}

fn allowed_host(url: &Url, allowlist: &str) -> bool {
    url.scheme() == "https"
        && url.port_or_known_default() == Some(443)
        && url.username().is_empty()
        && url.password().is_none()
        && url.fragment().is_none()
        && url.host_str().is_some_and(|host| {
            host.contains('.')
                && host.parse::<std::net::IpAddr>().is_err()
                && ![".localhost", ".local", ".internal"]
                    .iter()
                    .any(|suffix| host.ends_with(suffix))
                && allowlist
                    .split(',')
                    .any(|allowed| allowed.trim().eq_ignore_ascii_case(host))
        })
}

#[cfg(test)]
pub(crate) fn raw_fit(input: Vec<u8>) -> Result<Vec<u8>> {
    let file = if input.starts_with(b"PK\x03\x04") {
        let mut zip = zip::ZipArchive::new(Cursor::new(input))?;
        ensure!(zip.len() <= 32, "too many FIT archive entries");
        let entries: Vec<_> = zip
            .file_names()
            .collect::<std::result::Result<Vec<_>, _>>()?
            .into_iter()
            .filter(|name| name.to_ascii_lowercase().ends_with(".fit"))
            .map(|name| name.into_owned())
            .collect();
        ensure!(entries.len() == 1, "expected exactly one FIT archive entry");
        let entry = zip.by_name(&entries[0])?;
        ensure!(
            entry.size() <= MAX_FILE as u64,
            "FIT archive entry exceeds size limit"
        );
        let mut raw = Vec::new();
        entry.take(MAX_FILE as u64 + 1).read_to_end(&mut raw)?;
        raw
    } else if input.starts_with(b"\x1f\x8b") {
        let reader = flate2::read::MultiGzDecoder::new(Cursor::new(input));
        let mut raw = Vec::new();
        reader.take(MAX_FILE as u64 + 1).read_to_end(&mut raw)?;
        raw
    } else {
        input
    };
    validate_fit(&file)?;
    Ok(file)
}

fn crc(bytes: &[u8]) -> u16 {
    let mut value = 0u16;
    for byte in bytes {
        value ^= u16::from(*byte);
        for _ in 0..8 {
            value = if value & 1 == 1 {
                (value >> 1) ^ 0xa001
            } else {
                value >> 1
            };
        }
    }
    value
}

pub(crate) fn validate_fit(bytes: &[u8]) -> Result<()> {
    ensure!((14..=MAX_FILE).contains(&bytes.len()), "invalid FIT size");
    let header = usize::from(bytes[0]);
    ensure!(
        matches!(header, 12 | 14) && &bytes[8..12] == b".FIT",
        "invalid FIT header"
    );
    let body = u32::from_le_bytes(bytes[4..8].try_into()?) as usize;
    ensure!(
        body > 0 && header + body + 2 == bytes.len(),
        "incomplete FIT data"
    );
    let end = bytes.len() - 2;
    let file_crc = u16::from_le_bytes(bytes[end..].try_into()?);
    let header_crc = if header == 14 {
        u16::from_le_bytes(bytes[12..14].try_into()?)
    } else {
        0
    };
    ensure!(
        header_crc == 0 || crc(&bytes[..12]) == header_crc,
        "invalid FIT header checksum"
    );
    ensure!(
        crc(&bytes[..end]) == file_crc
            || (header == 14 && header_crc == 0 && crc(&bytes[header..end]) == file_crc),
        "invalid FIT checksum"
    );
    Ok(())
}

pub(crate) async fn save_parts(pool: &Pool, job: &Job, files: Vec<(i16, Vec<u8>)>) -> Result<()> {
    ensure!(!files.is_empty(), "FIT activity contains no files");
    let mut db = pool.get().await?;
    let tx = db.transaction().await?;
    let changed=tx.execute("UPDATE watch_tower.wearable_fit_job SET status='stored',job_id=NULL,leased_until=NULL,sealed_source=NULL
        WHERE subject_id=$1 AND provider=$2 AND activity_id=$3 AND job_id=$4::text::uuid AND leased_until>now()",
        &[&job.subject,&job.provider.slug(),&job.activity,&job.id]).await?;
    ensure!(changed == 1, "FIT job expired or was revoked");
    tx.execute("DELETE FROM avatar_hsm.wearable_fit WHERE subject_id=$1 AND provider=$2 AND activity_id=$3",
        &[&job.subject,&job.provider.slug(),&job.activity]).await?;
    for (part, file) in files {
        let hash = wearable_webhooks::hash(&file);
        tx.execute("INSERT INTO avatar_hsm.wearable_fit(subject_id,provider,activity_id,part,data,sha256) VALUES($1,$2,$3,$4,$5,$6)",
           &[&job.subject,&job.provider.slug(),&job.activity,&part,&file,&hash]).await?;
    }
    tx.commit().await?;
    Ok(())
}

#[derive(Deserialize)]
pub struct DownloadQuery {
    part: Option<i16>,
}

pub async fn user_download(
    State(app): State<Arc<App>>,
    Path((provider, activity)): Path<(String, String)>,
    Query(query): Query<DownloadQuery>,
    headers: HeaderMap,
) -> Response {
    match wearable_oauth::authenticate(&app.db, bearer(&headers), false).await {
        Ok(Some(subject)) => download(&app.db, &subject, &provider, &activity, query.part).await,
        Ok(None) => StatusCode::UNAUTHORIZED.into_response(),
        Err(_) => StatusCode::SERVICE_UNAVAILABLE.into_response(),
    }
}

pub async fn fixture_download(
    State(app): State<Arc<App>>,
    Path((provider, activity)): Path<(String, String)>,
    Query(query): Query<DownloadQuery>,
    headers: HeaderMap,
) -> Response {
    if headers.contains_key("origin") {
        return StatusCode::FORBIDDEN.into_response();
    }
    if !bool::from(app.read_token.as_bytes().ct_eq(bearer(&headers).as_bytes())) {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    download(&app.db, SUBJECT, &provider, &activity, query.part).await
}

pub(crate) fn bearer(headers: &HeaderMap) -> &str {
    headers
        .get("authorization")
        .and_then(|h| h.to_str().ok())
        .and_then(|h| h.strip_prefix("Bearer "))
        .unwrap_or_default()
}

async fn download(
    pool: &Pool,
    subject: &str,
    provider: &str,
    activity: &str,
    part: Option<i16>,
) -> Response {
    if !valid_id(activity) || part.is_some_and(|part| !(0..=MAX_PARTS as i16).contains(&part)) {
        return StatusCode::NOT_FOUND.into_response();
    }
    let result = async {
        Ok::<_, anyhow::Error>(
            pool.get()
                .await?
                .query_opt(
                    "SELECT data FROM avatar_hsm.wearable_fit
            WHERE subject_id=$1 AND provider=$2 AND activity_id=$3 AND ($4::smallint IS NULL OR part=$4)
            AND ($2 <> 'garmin' OR EXISTS (SELECT 1 FROM avatar_hsm.wearable_oauth_connection
                WHERE subject_id=$1 AND provider=$2 AND garmin_permissions & 1 <> 0))
            AND ($2 <> 'intervals_icu' OR NOT EXISTS (SELECT 1 FROM avatar_hsm.wearable_oauth_connection
                WHERE subject_id=$1 AND provider=$2 AND intervals_activity_granted IS FALSE))
            ORDER BY part LIMIT 1",
                    &[&subject, &provider, &activity, &part],
                )
                .await?,
        )
    }
    .await;
    match result {
        Ok(Some(row)) => (
            [
                ("content-type", "application/octet-stream"),
                ("content-disposition", "attachment; filename=activity.fit"),
                ("cache-control", "private, no-store"),
                ("x-content-type-options", "nosniff"),
            ],
            row.get::<_, Vec<u8>>(0),
        )
            .into_response(),
        Ok(None) => StatusCode::NOT_FOUND.into_response(),
        Err(_) => StatusCode::SERVICE_UNAVAILABLE.into_response(),
    }
}
