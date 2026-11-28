mod acute;
mod apple_export_routes;
mod apple_health_export;
mod catalog_store;
mod cloud_wearables;
mod diary;
mod drug;
mod food;
mod garmin_backfill;
mod garmin_metrics;
mod garmin_pull;
mod garmin_push;
mod garmin_workouts;
mod google_health_webhooks;
mod google_workout_tcx;
mod http;
pub mod mcp;
mod mcp_client;
#[cfg(test)]
mod oura_webhook_subscriptions;
mod samsung_activity;
mod samsung_goals;
mod samsung_routes;
pub mod state_machine;
pub mod state_owner;
pub mod store;
mod wearable_fit;
mod wearable_fit_details;
mod wearable_oauth;
mod wearable_sync;
mod wearable_webhook_documents;
mod wearable_webhooks;
mod wearables;
mod withings_cleanup;
mod withings_notify;

/// Provider adapter ports used by Watch Tower's ETL runtime. These methods
/// execute collection adapters; normalized observations are committed through
/// the HSM state-owner API. The standalone HSM host never starts these workers.
pub mod etl_adapter {
    use anyhow::{ensure, Context, Result};
    use chrono::{DateTime, Duration, Utc};
    use deadpool_postgres::Pool;
    use reqwest::Client;
    use serde_json::Value;

    pub use super::wearables::Snapshot;

    /// A stored FIT part leased to Watch Tower for CPU-heavy normalization.
    /// HSM remains the only owner that reads and updates the personal record row.
    pub struct FitDetailSource {
        pub subject_id: String,
        pub provider: String,
        pub activity_id: String,
        pub part: i16,
        pub sha256: Vec<u8>,
        pub bytes: Vec<u8>,
    }

    pub struct NormalizedWebhookRecord {
        pub kind: String,
        pub source: String,
        pub source_id: String,
        pub started_at: DateTime<Utc>,
        pub ended_at: DateTime<Utc>,
        pub modified_at: DateTime<Utc>,
        pub payload: Value,
    }

    pub enum WebhookDocumentResult {
        Live {
            days: Vec<chrono::NaiveDate>,
            record: Option<NormalizedWebhookRecord>,
        },
        PhysicalInfo(NormalizedWebhookRecord),
        Deleted,
        NoAccess,
        Revoked,
    }

    pub struct WebhookDocumentLease {
        job: super::wearable_webhook_documents::Job,
        pub provider: String,
        pub kind: String,
        pub object_id: String,
        pub delete_requested: bool,
        pub credentials: Vec<String>,
    }

    pub struct WebhookDocumentClaim {
        pub subject_id: String,
        pub provider: String,
        pub data_type: String,
        pub object_id: String,
        pub job_id: String,
        pub delete_requested: bool,
    }

    #[derive(Clone)]
    pub struct WebhookDocumentAck {
        pub subject_id: String,
        pub provider: String,
        pub data_type: String,
        pub object_id: String,
        pub job_id: String,
    }

    pub struct WebhookDocumentCommit {
        pub lease_valid: bool,
        pub replay_days: Vec<chrono::NaiveDate>,
    }

    /// One opaque HSM lease for fetching an original FIT file. The job ID and
    /// provider metadata cross the crate boundary; sealed URLs stay private.
    pub struct FitDownloadJob(pub(crate) super::wearable_fit::Job);

    impl FitDownloadJob {
        pub fn activity_id(&self) -> &str {
            &self.0.activity
        }
    }

    /// One HSM-authorized Withings subscription reconciliation. The token is
    /// short lived at this boundary and must never be persisted or logged.
    pub struct WithingsNotifyTask(pub(crate) super::withings_notify::WatchTowerTask);

    /// Leased Google exercise whose route and telemetry detail is pending.
    pub struct GoogleTcxJob(pub(crate) super::google_workout_tcx::Job);

    pub enum GoogleTcxPlan {
        Stored(Vec<u8>),
        Fetch { token: String, activity_id: String },
        NoAccess,
    }

    pub enum WithingsCleanupAuthorization {
        Ready { token: String, callback: String },
        Skip,
    }

    /// HSM-authorized Garmin backfill request. HSM retains the durable lease;
    /// Watch Tower receives only the bounded request parameters and one token.
    pub struct GarminBackfillJob(pub(crate) super::garmin_backfill::Job);

    pub enum GarminBackfillAuthorization {
        Ready(String),
        PermissionRemoved,
        Deferred(DateTime<Utc>),
    }

    pub struct GarminPullAuthorization {
        pub url: reqwest::Url,
        pub token: String,
        pub account_hash: Vec<u8>,
    }

    pub use super::garmin_pull::GarminPullRecord;

    pub fn garmin_pull_kind(feed: &str) -> Option<String> {
        super::garmin_push::summary_kind(feed).map(|(kind, _)| kind.to_owned())
    }

    pub fn oura_webhook_types() -> Vec<&'static str> {
        super::wearable_webhook_documents::SUPPORTED
            .iter()
            .map(|(kind, _)| *kind)
            .collect()
    }

    pub enum GarminPullAuthorizationResult {
        Ready(GarminPullAuthorization),
        PermissionRemoved,
        Deferred(DateTime<Utc>),
    }

    pub async fn authorize_garmin_pull(
        pool: &Pool,
        client: &Client,
        id: &str,
        lease: &str,
        subject: &str,
        feed: &str,
        sealed_source: Vec<u8>,
        request_hash: Vec<u8>,
    ) -> Result<GarminPullAuthorizationResult> {
        Ok(
            match super::garmin_pull::authorize(
                pool,
                client,
                id,
                lease,
                subject,
                feed,
                sealed_source,
                request_hash,
            )
            .await?
            {
                super::garmin_pull::Authorization::Ready(request) => {
                    GarminPullAuthorizationResult::Ready(GarminPullAuthorization {
                        url: request.url,
                        token: request.token,
                        account_hash: request.account,
                    })
                }
                super::garmin_pull::Authorization::PermissionRemoved => {
                    GarminPullAuthorizationResult::PermissionRemoved
                }
                super::garmin_pull::Authorization::Deferred(at) => {
                    GarminPullAuthorizationResult::Deferred(at)
                }
            },
        )
    }

    pub async fn commit_garmin_pull(
        pool: &Pool,
        id: &str,
        lease: &str,
        subject: &str,
        feed: &str,
        account_hash: Vec<u8>,
        records: Vec<GarminPullRecord>,
    ) -> Result<bool> {
        super::garmin_pull::commit_records(pool, id, lease, subject, feed, account_hash, records)
            .await
    }

    impl GarminBackfillJob {
        pub fn subject_id(&self) -> &str {
            &self.0.subject
        }

        pub fn feed(&self) -> &str {
            &self.0.feed
        }

        pub fn start(&self) -> DateTime<Utc> {
            self.0.start
        }

        pub fn end(&self) -> DateTime<Utc> {
            self.0.end
        }

        pub fn job_id(&self) -> &str {
            &self.0.lease
        }
    }

    pub use super::wearable_fit::{
        CorosFitDownloadPlan, FitDownloadAuth, FitDownloadRequest, PolarFitDownloadPlan,
        SuuntoFitDownloadPlan,
    };

    pub enum FitDownloadPlan {
        Http(FitDownloadRequest),
        Coros(CorosFitDownloadPlan),
        Polar(PolarFitDownloadPlan),
        Suunto(SuuntoFitDownloadPlan),
    }

    pub async fn next_fit_download(pool: &Pool) -> Result<Option<FitDownloadJob>> {
        Ok(super::wearable_fit::claim(pool).await?.map(FitDownloadJob))
    }

    impl WithingsNotifyTask {
        pub fn callback(&self) -> &str {
            &self.0.callback
        }
        pub fn token(&self) -> &str {
            &self.0.token
        }
        pub fn categories(&self) -> &[u16] {
            &self.0.categories
        }
    }

    pub async fn next_withings_notify(
        pool: &Pool,
        client: &Client,
    ) -> Result<Option<WithingsNotifyTask>> {
        Ok(super::withings_notify::prepare_watch_tower(pool, client)
            .await?
            .map(WithingsNotifyTask))
    }

    pub async fn finish_withings_notify(
        pool: &Pool,
        task: &WithingsNotifyTask,
        result: &str,
    ) -> Result<bool> {
        super::withings_notify::finish_watch_tower(pool, &task.0, result).await
    }

    pub async fn next_google_tcx(pool: &Pool) -> Result<Option<GoogleTcxJob>> {
        Ok(super::google_workout_tcx::claim(pool)
            .await?
            .map(GoogleTcxJob))
    }

    pub async fn prepare_google_tcx(
        pool: &Pool,
        client: &Client,
        job: &GoogleTcxJob,
    ) -> Result<GoogleTcxPlan> {
        if let Some(bytes) = &job.0.stored_data {
            return Ok(GoogleTcxPlan::Stored(bytes.clone()));
        }
        let leased_and_allowed = pool
            .get()
            .await?
            .query_opt(
                "SELECT 1 FROM avatar_hsm.google_workout_tcx t
             JOIN watch_tower.google_workout_tcx_queue q USING(subject_id,source,source_id)
             JOIN avatar_hsm.wearable_oauth_connection c USING(subject_id,provider)
             WHERE t.subject_id=$1 AND t.source=$2 AND t.source_id=$3
               AND q.job_id=$4::text::uuid AND q.leased_until>now()
               AND c.google_location_granted",
                &[&job.0.subject, &job.0.source, &job.0.activity, &job.0.id],
            )
            .await?
            .is_some();
        if !leased_and_allowed {
            return Ok(GoogleTcxPlan::NoAccess);
        }
        let credentials = super::wearable_oauth::access(
            client,
            pool,
            &job.0.subject,
            super::cloud_wearables::Provider::GoogleHealth,
        )
        .await?;
        if !credentials
            .get(1)
            .is_some_and(|scope| super::wearable_oauth::google_tcx_scope(scope))
        {
            return Ok(GoogleTcxPlan::NoAccess);
        }
        let token = credentials
            .first()
            .cloned()
            .context("Google Health token missing")?;
        Ok(GoogleTcxPlan::Fetch {
            token,
            activity_id: job.0.activity.clone(),
        })
    }

    pub async fn commit_google_tcx(
        pool: &Pool,
        job: &GoogleTcxJob,
        bytes: Vec<u8>,
        route: serde_json::Value,
        telemetry: serde_json::Value,
    ) -> Result<bool> {
        super::google_workout_tcx::validate_normalized_payload(&bytes, &route, &telemetry)?;
        let mut db = pool.get().await?;
        let tx = db.transaction().await?;
        let count = tx.execute(
            "UPDATE avatar_hsm.google_workout_tcx t SET data=COALESCE(t.data,$5),route_points=$6,
                 telemetry_points=$7
             WHERE t.subject_id=$1 AND t.source=$2 AND t.source_id=$3
               AND EXISTS (SELECT 1 FROM watch_tower.google_workout_tcx_queue q
                           WHERE q.subject_id=t.subject_id AND q.source=t.source AND q.source_id=t.source_id
                             AND q.job_id=$4::text::uuid AND q.leased_until>now())
               AND EXISTS (SELECT 1 FROM avatar_hsm.wearable_oauth_connection c
                           WHERE c.subject_id=t.subject_id AND c.provider=t.provider AND c.google_location_granted)",
            &[&job.0.subject,&job.0.source,&job.0.activity,&job.0.id,&bytes,&route,&telemetry],
        ).await?;
        if count == 1 {
            tx.execute(
                "UPDATE watch_tower.google_workout_tcx_queue
                 SET job_id=NULL,leased_until=NULL
                 WHERE subject_id=$1 AND source=$2 AND source_id=$3 AND job_id=$4::text::uuid",
                &[&job.0.subject, &job.0.source, &job.0.activity, &job.0.id],
            )
            .await?;
        }
        tx.commit().await?;
        Ok(count == 1)
    }

    pub async fn defer_google_tcx(
        pool: &Pool,
        job: &GoogleTcxJob,
        retry_at: Option<DateTime<Utc>>,
    ) -> Result<()> {
        super::google_workout_tcx::defer_job(pool, &job.0, retry_at).await
    }

    pub async fn authorize_withings_cleanup(
        pool: &Pool,
        subject: &str,
        account: Vec<u8>,
        generation: &str,
        job_id: &str,
    ) -> Result<WithingsCleanupAuthorization> {
        Ok(
            match super::withings_cleanup::authorize_watch_tower(
                pool, subject, &account, generation, job_id,
            )
            .await?
            {
                super::withings_cleanup::WatchTowerAuthorization::Ready { token, callback } => {
                    WithingsCleanupAuthorization::Ready { token, callback }
                }
                super::withings_cleanup::WatchTowerAuthorization::Skip => {
                    WithingsCleanupAuthorization::Skip
                }
            },
        )
    }

    pub fn lease_webhook_document(claim: WebhookDocumentClaim) -> WebhookDocumentLease {
        let job = super::wearable_webhook_documents::Job {
            subject: claim.subject_id,
            provider: claim.provider,
            kind: claim.data_type,
            object: claim.object_id,
            id: claim.job_id,
            delete_requested: claim.delete_requested,
        };
        WebhookDocumentLease {
            provider: job.provider.clone(),
            kind: job.kind.clone(),
            object_id: job.object.clone(),
            delete_requested: job.delete_requested,
            credentials: Vec::new(),
            job,
        }
    }

    pub async fn authorize_webhook_document(
        pool: &Pool,
        client: &Client,
        lease: &mut WebhookDocumentLease,
    ) -> Result<()> {
        lease.credentials = if lease.provider == "strava" && lease.kind == "authorization" {
            Vec::new()
        } else {
            let provider = super::cloud_wearables::Provider::parse(&lease.provider)?;
            super::wearable_oauth::access(client, pool, &lease.job.subject, provider).await?
        };
        Ok(())
    }

    pub async fn finish_webhook_document(
        pool: &Pool,
        lease: &WebhookDocumentLease,
        result: WebhookDocumentResult,
    ) -> Result<WebhookDocumentCommit> {
        use super::wearable_webhook_documents::Document;
        use super::wearables::Record;
        let replay_days = match &result {
            WebhookDocumentResult::Live { days, .. } => days.clone(),
            _ => Vec::new(),
        };
        let record = |record: NormalizedWebhookRecord| Record {
            kind: record.kind,
            source: record.source,
            source_id: record.source_id,
            started_at: record.started_at,
            ended_at: record.ended_at,
            modified_at: record.modified_at,
            payload: record.payload,
        };
        let document = match result {
            WebhookDocumentResult::Live {
                days,
                record: value,
            } => Document::Live {
                days,
                record: value.map(|value| Box::new(record(value))),
            },
            WebhookDocumentResult::PhysicalInfo(value) => Document::PhysicalInfo(record(value)),
            WebhookDocumentResult::Deleted => Document::Deleted,
            WebhookDocumentResult::NoAccess => Document::NoAccess,
            WebhookDocumentResult::Revoked => Document::Revoked,
        };
        let lease_valid =
            super::wearable_webhook_documents::finish_owner(pool, &lease.job, document).await?;
        Ok(WebhookDocumentCommit {
            lease_valid,
            replay_days,
        })
    }

    pub fn webhook_document_ack(lease: &WebhookDocumentLease) -> WebhookDocumentAck {
        WebhookDocumentAck {
            subject_id: lease.job.subject.clone(),
            provider: lease.job.provider.clone(),
            data_type: lease.job.kind.clone(),
            object_id: lease.job.object.clone(),
            job_id: lease.job.id.clone(),
        }
    }

    pub async fn prepare_fit_download(
        pool: &Pool,
        client: &Client,
        job: &FitDownloadJob,
    ) -> Result<FitDownloadPlan> {
        if job.0.provider.slug() == "coros" {
            return Ok(FitDownloadPlan::Coros(
                super::wearable_fit::watch_tower_coros_plan(pool, client, &job.0).await?,
            ));
        }
        if job.0.provider.slug() == "polar" {
            return Ok(FitDownloadPlan::Polar(
                super::wearable_fit::watch_tower_polar_plan(pool, client, &job.0).await?,
            ));
        }
        if job.0.provider.slug() == "suunto" {
            return Ok(FitDownloadPlan::Suunto(
                super::wearable_fit::watch_tower_suunto_plan(pool, client, &job.0).await?,
            ));
        }
        match super::wearable_fit::watch_tower_request(pool, client, &job.0).await? {
            Some(request) => Ok(FitDownloadPlan::Http(request)),
            None => anyhow::bail!("no Watch Tower FIT provider plan is available"),
        }
    }

    pub async fn save_fit_download(
        pool: &Pool,
        job: &FitDownloadJob,
        bytes: Vec<u8>,
    ) -> Result<()> {
        super::wearable_fit::save_watch_tower_download(pool, &job.0, bytes).await
    }

    pub async fn save_fit_download_parts(
        pool: &Pool,
        job: &FitDownloadJob,
        files: Vec<(i16, Vec<u8>)>,
    ) -> Result<()> {
        super::wearable_fit::save_parts(pool, &job.0, files).await
    }

    pub async fn commit_fit_summary(
        pool: &Pool,
        job: &FitDownloadJob,
        normalized_record: serde_json::Value,
    ) -> Result<()> {
        let record: super::wearables::Record = serde_json::from_value(normalized_record)
            .context("invalid normalized workout record")?;
        super::wearable_fit::validate_watch_tower_summary(&job.0, &record)?;
        super::wearable_fit::commit_watch_tower_summary(pool, &job.0, record).await
    }

    pub async fn defer_fit_download(
        pool: &Pool,
        job: &FitDownloadJob,
        retry_at: Option<DateTime<Utc>>,
    ) -> Result<()> {
        super::wearable_fit::defer_job(pool, &job.0, retry_at).await
    }

    pub fn lease_garmin_backfill(
        subject: String,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
        feed: String,
        lease: String,
    ) -> GarminBackfillJob {
        GarminBackfillJob(super::garmin_backfill::Job {
            subject,
            start,
            end,
            feed,
            lease,
        })
    }

    pub async fn garmin_backfill_token(
        pool: &Pool,
        client: &Client,
        job: &GarminBackfillJob,
    ) -> Result<GarminBackfillAuthorization> {
        Ok(
            match super::garmin_backfill::access_token(pool, client, &job.0).await? {
                super::garmin_backfill::Authorization::Ready(token) => {
                    GarminBackfillAuthorization::Ready(token)
                }
                super::garmin_backfill::Authorization::PermissionRemoved => {
                    GarminBackfillAuthorization::PermissionRemoved
                }
                super::garmin_backfill::Authorization::Deferred(at) => {
                    GarminBackfillAuthorization::Deferred(at)
                }
            },
        )
    }

    pub async fn defer_garmin_backfill_account(
        pool: &Pool,
        subject_id: &str,
        retry_at: DateTime<Utc>,
    ) -> Result<()> {
        let mut db = pool.get().await?;
        let tx = db.transaction().await?;
        super::garmin_backfill::lock_account(&tx, subject_id).await?;
        super::garmin_backfill::defer_account(&tx, subject_id, retry_at).await?;
        tx.commit().await?;
        Ok(())
    }

    pub async fn next_fit_detail_source(pool: &Pool) -> Result<Option<FitDetailSource>> {
        let db = pool.get().await?;
        let Some(row) = db.query_opt(
            "SELECT f.subject_id,f.provider,f.activity_id,f.part,f.sha256,f.data
             FROM avatar_hsm.wearable_fit f
             WHERE (NOT f.developer_fields_parsed OR NOT f.devices_parsed OR NOT f.hrv_parsed
                 OR NOT f.compressed_heart_rate_parsed OR NOT f.strength_parsed OR NOT f.splits_parsed
                 OR NOT f.cycling_dynamics_parsed OR NOT f.respiration_parsed)
               AND (f.provider<>'garmin' OR EXISTS (
                   SELECT 1 FROM avatar_hsm.wearable_oauth_connection c
                   WHERE c.subject_id=f.subject_id AND c.provider='garmin'
                     AND (c.garmin_permissions & 1)<>0))
             ORDER BY f.captured_at LIMIT 1",
            &[],
        ).await? else {
            return Ok(None);
        };
        Ok(Some(FitDetailSource {
            subject_id: row.get(0),
            provider: row.get(1),
            activity_id: row.get(2),
            part: row.get(3),
            sha256: row.get(4),
            bytes: row.get(5),
        }))
    }

    pub async fn save_fit_detail(
        pool: &Pool,
        source: &FitDetailSource,
        detail: Option<&Value>,
    ) -> Result<bool> {
        let state = if detail.is_some() {
            "ready"
        } else {
            "unavailable"
        };
        let changed = pool.get().await?.execute(
            "UPDATE avatar_hsm.wearable_fit
             SET detail_state=CASE WHEN $7::jsonb IS NULL AND detail IS NOT NULL THEN detail_state ELSE $6 END,
                 detail=COALESCE($7::jsonb,detail),events_parsed=true,developer_fields_parsed=true,
                 devices_parsed=true,hrv_parsed=true,compressed_heart_rate_parsed=true,
                 strength_parsed=true,splits_parsed=true,cycling_dynamics_parsed=true,respiration_parsed=true
             WHERE subject_id=$1 AND provider=$2 AND activity_id=$3 AND part=$4 AND sha256=$5
               AND (NOT developer_fields_parsed OR NOT devices_parsed OR NOT hrv_parsed
                 OR NOT compressed_heart_rate_parsed OR NOT strength_parsed OR NOT splits_parsed
                 OR NOT cycling_dynamics_parsed OR NOT respiration_parsed)",
            &[&source.subject_id,&source.provider,&source.activity_id,&source.part,&source.sha256,&state,&detail],
        ).await?;
        Ok(changed == 1)
    }

    pub async fn apply_normalized_snapshot(
        pool: &Pool,
        subject_id: &str,
        provider: &str,
        snapshot: Snapshot,
        replace_source_window: bool,
        etl_run_id: Option<&str>,
    ) -> Result<Value> {
        super::state_owner::apply_normalized_snapshot(
            pool,
            subject_id,
            provider,
            snapshot,
            replace_source_window,
            etl_run_id,
        )
        .await
    }

    pub fn http_client() -> Result<Client> {
        super::cloud_wearables::http_client()
    }

    pub fn validate_fit(bytes: &[u8]) -> Result<()> {
        super::wearable_fit::validate_fit(bytes)
    }

    pub fn sync_window_size(provider: &str) -> Result<Duration> {
        let provider = super::cloud_wearables::Provider::parse(provider)?;
        Ok(super::cloud_wearables::window_size(provider))
    }

    pub fn is_synthetic_subject(subject_id: &str) -> bool {
        subject_id == super::store::SUBJECT
    }

    pub fn provider_retry_at(error: &anyhow::Error) -> Option<DateTime<Utc>> {
        error
            .downcast_ref::<super::cloud_wearables::ProviderThrottled>()
            .map(|throttled| throttled.retry_at)
    }

    /// Resolve one provider credential for Watch Tower's Hevy collector.
    /// The token is held in memory for the run and is never persisted or logged
    /// by the collector; HSM remains the credential and OAuth lifecycle owner.
    pub async fn hevy_access_token(
        pool: &Pool,
        client: &Client,
        subject_id: &str,
    ) -> Result<String> {
        let credentials = if subject_id == super::store::SUBJECT {
            super::cloud_wearables::configured(super::cloud_wearables::Provider::Hevy)
                .context("Hevy credentials missing")?
        } else {
            super::wearable_oauth::access_for_sync(
                client,
                pool,
                subject_id,
                super::cloud_wearables::Provider::Hevy,
            )
            .await?
        };
        credentials
            .into_iter()
            .next()
            .filter(|token| !token.trim().is_empty())
            .context("Hevy credential missing")
    }

    /// Resolve the Liftosaur API credential through HSM's connection lifecycle.
    /// The caller must keep the token transient and never write it to logs.
    pub async fn liftosaur_access_token(
        pool: &Pool,
        client: &Client,
        subject_id: &str,
    ) -> Result<String> {
        let credentials = if subject_id == super::store::SUBJECT {
            super::cloud_wearables::configured(super::cloud_wearables::Provider::Liftosaur)
                .context("Liftosaur credentials missing")?
        } else {
            super::wearable_oauth::access_for_sync(
                client,
                pool,
                subject_id,
                super::cloud_wearables::Provider::Liftosaur,
            )
            .await?
        };
        credentials
            .into_iter()
            .next()
            .filter(|token| !token.trim().is_empty())
            .context("Liftosaur credential missing")
    }

    /// Resolve the Strava API credential through HSM's connection lifecycle.
    /// Watch Tower may hold the token only transiently while collecting.
    pub async fn strava_access_token(
        pool: &Pool,
        client: &Client,
        subject_id: &str,
    ) -> Result<String> {
        let credentials = if subject_id == super::store::SUBJECT {
            super::cloud_wearables::configured(super::cloud_wearables::Provider::Strava)
                .context("Strava credentials missing")?
        } else {
            super::wearable_oauth::access_for_sync(
                client,
                pool,
                subject_id,
                super::cloud_wearables::Provider::Strava,
            )
            .await?
        };
        credentials
            .into_iter()
            .next()
            .filter(|token| !token.trim().is_empty())
            .context("Strava credential missing")
    }

    /// Resolve the Ultrahuman credential through HSM's connection lifecycle.
    /// Watch Tower holds this token only for the active collection attempt.
    pub async fn ultrahuman_access_token(
        pool: &Pool,
        client: &Client,
        subject_id: &str,
    ) -> Result<String> {
        let credentials = if subject_id == super::store::SUBJECT {
            super::cloud_wearables::configured(super::cloud_wearables::Provider::Ultrahuman)
                .context("Ultrahuman credentials missing")?
        } else {
            super::wearable_oauth::access_for_sync(
                client,
                pool,
                subject_id,
                super::cloud_wearables::Provider::Ultrahuman,
            )
            .await?
        };
        credentials
            .into_iter()
            .next()
            .filter(|token| !token.trim().is_empty())
            .context("Ultrahuman credential missing")
    }

    /// Resolve WHOOP's access token and granted scopes for one Watch Tower run.
    /// The values remain transient in the collector and are never persisted there.
    pub async fn whoop_credentials(
        pool: &Pool,
        client: &Client,
        subject_id: &str,
    ) -> Result<Vec<String>> {
        let credentials = if subject_id == super::store::SUBJECT {
            super::cloud_wearables::configured(super::cloud_wearables::Provider::Whoop)
                .context("WHOOP credentials missing")?
        } else {
            super::wearable_oauth::access_for_sync(
                client,
                pool,
                subject_id,
                super::cloud_wearables::Provider::Whoop,
            )
            .await?
        };
        ensure!(
            credentials
                .first()
                .is_some_and(|token| !token.trim().is_empty()),
            "WHOOP credential missing"
        );
        Ok(credentials)
    }

    /// Resolve Oura's bearer token and granted scopes for one Watch Tower run.
    /// OAuth refresh and consent remain owned by HSM; the collector holds the
    /// returned credentials only in memory for the claimed ETL execution.
    pub async fn oura_credentials(
        pool: &Pool,
        client: &Client,
        subject_id: &str,
    ) -> Result<Vec<String>> {
        let credentials = if subject_id == super::store::SUBJECT {
            super::cloud_wearables::configured(super::cloud_wearables::Provider::Oura)
                .context("Oura credentials missing")?
        } else {
            super::wearable_oauth::access_for_sync(
                client,
                pool,
                subject_id,
                super::cloud_wearables::Provider::Oura,
            )
            .await?
        };
        ensure!(
            credentials
                .first()
                .is_some_and(|token| !token.trim().is_empty()),
            "Oura credential missing"
        );
        Ok(credentials)
    }

    /// Resolve the Intervals.icu API token and granted scopes for a claimed run.
    pub async fn intervals_icu_credentials(
        pool: &Pool,
        client: &Client,
        subject_id: &str,
    ) -> Result<Vec<String>> {
        let credentials = if subject_id == super::store::SUBJECT {
            super::cloud_wearables::configured(super::cloud_wearables::Provider::IntervalsIcu)
                .context("Intervals.icu credentials missing")?
        } else {
            super::wearable_oauth::access_for_sync(
                client,
                pool,
                subject_id,
                super::cloud_wearables::Provider::IntervalsIcu,
            )
            .await?
        };
        ensure!(
            credentials
                .first()
                .is_some_and(|token| !token.trim().is_empty()),
            "Intervals.icu credential missing"
        );
        Ok(credentials)
    }

    /// Resolve Polar's access token through HSM's OAuth and consent owner.
    pub async fn polar_credentials(
        pool: &Pool,
        client: &Client,
        subject_id: &str,
    ) -> Result<Vec<String>> {
        let credentials = if subject_id == super::store::SUBJECT {
            super::cloud_wearables::configured(super::cloud_wearables::Provider::Polar)
                .context("Polar credentials missing")?
        } else {
            super::wearable_oauth::access_for_sync(
                client,
                pool,
                subject_id,
                super::cloud_wearables::Provider::Polar,
            )
            .await?
        };
        ensure!(
            credentials
                .first()
                .is_some_and(|token| !token.trim().is_empty()),
            "Polar credential missing"
        );
        Ok(credentials)
    }

    /// Resolve Garmin Personal's API key through HSM's connection lifecycle.
    /// Watch Tower exchanges it for a short lived Garmin token during one run.
    pub async fn garmin_personal_credentials(
        pool: &Pool,
        client: &Client,
        subject_id: &str,
    ) -> Result<Vec<String>> {
        let credentials = if subject_id == super::store::SUBJECT {
            super::cloud_wearables::configured(super::cloud_wearables::Provider::GarminPersonal)
                .context("Garmin Personal credentials missing")?
        } else {
            super::wearable_oauth::access_for_sync(
                client,
                pool,
                subject_id,
                super::cloud_wearables::Provider::GarminPersonal,
            )
            .await?
        };
        ensure!(
            credentials
                .first()
                .is_some_and(|key| !key.trim().is_empty()),
            "Garmin Personal credential missing"
        );
        Ok(credentials)
    }

    /// Resolve the Withings token and scopes through HSM's connection owner.
    /// Watch Tower uses the scopes for extraction and reports metrics consent
    /// back with each normalized snapshot for HSM-owned stale-record cleanup.
    pub async fn withings_credentials(
        pool: &Pool,
        client: &Client,
        subject_id: &str,
    ) -> Result<Vec<String>> {
        let credentials = if subject_id == super::store::SUBJECT {
            super::cloud_wearables::configured(super::cloud_wearables::Provider::Withings)
                .context("Withings credentials missing")?
        } else {
            super::wearable_oauth::access_for_sync(
                client,
                pool,
                subject_id,
                super::cloud_wearables::Provider::Withings,
            )
            .await?
        };
        ensure!(
            credentials
                .first()
                .is_some_and(|token| !token.trim().is_empty()),
            "Withings credential missing"
        );
        Ok(credentials)
    }

    /// Resolve Suunto's bearer token and subscription key through HSM's
    /// connection owner. Watch Tower keeps both values only for the active run.
    pub async fn suunto_credentials(
        pool: &Pool,
        client: &Client,
        subject_id: &str,
    ) -> Result<Vec<String>> {
        let credentials = if subject_id == super::store::SUBJECT {
            super::cloud_wearables::configured(super::cloud_wearables::Provider::Suunto)
                .context("Suunto credentials missing")?
        } else {
            super::wearable_oauth::access_for_sync(
                client,
                pool,
                subject_id,
                super::cloud_wearables::Provider::Suunto,
            )
            .await?
        };
        ensure!(
            credentials
                .first()
                .is_some_and(|token| !token.trim().is_empty())
                && credentials.get(1).is_some_and(|key| !key.trim().is_empty()),
            "Suunto credentials missing"
        );
        Ok(credentials)
    }

    /// Resolve COROS token and account identity through HSM's connection owner.
    pub async fn coros_credentials(
        pool: &Pool,
        client: &Client,
        subject_id: &str,
    ) -> Result<Vec<String>> {
        let credentials = if subject_id == super::store::SUBJECT {
            super::cloud_wearables::configured(super::cloud_wearables::Provider::Coros)
                .context("COROS credentials missing")?
        } else {
            super::wearable_oauth::access_for_sync(
                client,
                pool,
                subject_id,
                super::cloud_wearables::Provider::Coros,
            )
            .await?
        };
        ensure!(
            credentials
                .first()
                .is_some_and(|token| !token.trim().is_empty())
                && credentials
                    .get(1)
                    .is_some_and(|open_id| !open_id.trim().is_empty()),
            "COROS credentials missing"
        );
        Ok(credentials)
    }

    /// Resolve Sensor Bio's bearer token through HSM's connection owner.
    pub async fn sensorbio_credentials(
        pool: &Pool,
        client: &Client,
        subject_id: &str,
    ) -> Result<Vec<String>> {
        let credentials = if subject_id == super::store::SUBJECT {
            super::cloud_wearables::configured(super::cloud_wearables::Provider::Sensorbio)
                .context("Sensor Bio credentials missing")?
        } else {
            super::wearable_oauth::access_for_sync(
                client,
                pool,
                subject_id,
                super::cloud_wearables::Provider::Sensorbio,
            )
            .await?
        };
        ensure!(
            credentials
                .first()
                .is_some_and(|token| !token.trim().is_empty()),
            "Sensor Bio credential missing"
        );
        Ok(credentials)
    }

    /// Resolve Google Health OAuth credentials through HSM's connection owner.
    /// OAuth refresh and consent remain HSM-owned.
    pub async fn google_health_credentials(
        pool: &Pool,
        client: &Client,
        subject_id: &str,
    ) -> Result<Vec<String>> {
        let credentials = if subject_id == super::store::SUBJECT {
            let configured =
                super::cloud_wearables::configured(super::cloud_wearables::Provider::GoogleHealth)
                    .context("Google Health credentials missing")?;
            super::cloud_wearables::prepare(
                client,
                super::cloud_wearables::Provider::GoogleHealth,
                configured,
            )
            .await?
        } else {
            super::wearable_oauth::access_for_sync(
                client,
                pool,
                subject_id,
                super::cloud_wearables::Provider::GoogleHealth,
            )
            .await?
        };
        ensure!(
            credentials
                .first()
                .is_some_and(|token| !token.trim().is_empty()),
            "Google Health credential missing"
        );
        Ok(credentials)
    }

    /// HSM validates and retains COROS FIT URLs; Watch Tower only supplies the
    /// source workout items that it already fetched for this normalized window.
    pub fn attach_coros_fit_sources(snapshot: &mut Snapshot, source_items: &[Value]) {
        let allowed = std::env::var("AVATAR_COROS_FIT_HOSTS").unwrap_or_default();
        attach_coros_fit_sources_with_allowlist(snapshot, source_items, &allowed);
    }

    fn attach_coros_fit_sources_with_allowlist(
        snapshot: &mut Snapshot,
        source_items: &[Value],
        allowed: &str,
    ) {
        let workout_ids: std::collections::HashSet<_> = snapshot
            .records
            .iter()
            .filter(|record| record.kind == "workout")
            .map(|record| record.source_id.as_str())
            .collect();
        for item in source_items {
            let Some(activity) = super::wearable_webhooks::account_id(&item["labelId"]) else {
                continue;
            };
            if workout_ids.contains(activity.as_str()) {
                if let Ok(links) = super::wearable_fit::coros_links(item, allowed) {
                    snapshot.coros_fit_sources.insert(activity, links);
                }
            }
        }
    }

    /// Issue a per-run lease that HSM will validate when Watch Tower submits
    /// the normalized batch through the state-owner API.
    pub async fn issue_collection_lease(
        pool: &Pool,
        subject_id: &str,
        provider: &str,
        job_id: &str,
    ) -> Result<Value> {
        super::wearables::issue_job_lease(pool, subject_id, provider, job_id).await
    }

    pub async fn schedule_polar_physical_info(pool: &Pool, subject_id: &str) -> Result<()> {
        super::wearable_webhook_documents::schedule_polar_physical_info(pool, subject_id).await
    }

    pub async fn provision_oauth(pool: &Pool) -> Result<()> {
        super::wearable_oauth::provision(pool).await
    }

    #[cfg(test)]
    pub async fn fitness_worker(pool: Pool, client: Client) {
        super::wearable_fit::worker(pool, client).await
    }

    #[cfg(test)]
    pub async fn webhook_document_worker(pool: Pool, client: Client) {
        super::wearable_webhook_documents::worker(pool, client).await
    }

    #[cfg(test)]
    pub async fn withings_notify_worker(pool: Pool, client: Client) {
        super::withings_notify::worker(pool, client).await
    }

    #[cfg(test)]
    pub async fn withings_cleanup_worker(pool: Pool, client: Client) {
        super::withings_cleanup::worker(pool, client).await
    }

    #[cfg(test)]
    pub async fn google_workout_worker(pool: Pool, client: Client) {
        super::google_workout_tcx::worker(pool, client).await
    }

    #[cfg(test)]
    pub async fn garmin_backfill_worker(pool: Pool, client: Client) {
        super::garmin_backfill::worker(pool, client).await
    }

    #[cfg(test)]
    pub async fn garmin_pull_worker(pool: Pool, client: Client) {
        super::garmin_pull::worker(pool, client).await
    }

    pub async fn rebuild_garmin_metrics(pool: Pool) {
        super::garmin_metrics::rebuild_existing(pool).await
    }

    pub async fn rebuild_garmin_workouts(pool: Pool) {
        super::garmin_workouts::rebuild_existing(pool).await
    }

    #[cfg(test)]
    pub async fn oura_subscription_worker(pool: Pool, client: Client) {
        super::oura_webhook_subscriptions::worker(pool, client).await
    }

    #[cfg(test)]
    #[test]
    fn coros_fit_sources_require_a_matching_workout_and_allowed_host() -> Result<()> {
        let mut snapshot: Snapshot = serde_json::from_value(serde_json::json!({
            "provider":"coros","lease":"lease","start":"2026-11-27T00:00:00Z",
            "end":"2026-11-28T00:00:00Z","grants":["workout"],"records":[{
                "kind":"workout","source":"COROS","source_id":"activity-1",
                "started_at":"2026-11-27T08:00:00Z","ended_at":"2026-11-27T08:30:00Z",
                "modified_at":"2026-11-28T00:00:00Z","payload":{"exercise_type":"coros:8:1"}
            }]
        }))?;
        let items = [
            serde_json::json!({"labelId":"activity-1","fitUrl":"https://cdn.example.com/a.fit?sig=private"}),
            serde_json::json!({"labelId":"activity-2","fitUrl":"https://cdn.example.com/b.fit?sig=private"}),
            serde_json::json!({"labelId":"activity-1","fitUrl":"https://attacker.example/b.fit"}),
        ];
        attach_coros_fit_sources_with_allowlist(&mut snapshot, &items, "cdn.example.com");
        assert_eq!(snapshot.coros_fit_sources.len(), 1);
        let links = snapshot.coros_fit_sources.get("activity-1").unwrap();
        assert_eq!(links.len(), 1);
        assert_eq!(links[0].1.host_str(), Some("cdn.example.com"));
        Ok(())
    }
}

pub fn mcp_app(
    db: deadpool_postgres::Pool,
    read_token: String,
    control_token: String,
) -> anyhow::Result<mcp::App> {
    Ok(mcp::App {
        db,
        read_token,
        control_token,
        rules: acute::rules()?,
    })
}

/// Build HSM-owned provider callback and OAuth routes for the integrated
/// Watch Tower HTTP process. AI clients continue to use the gateway's `/mcp`.
pub fn http_router(app: std::sync::Arc<mcp::App>) -> axum::Router {
    http::router(app)
}

/// Retain the standalone HSM HTTP surface for crate-local legacy tests. The
/// integrated Watch Tower runtime deliberately mounts only `http_router`.
#[cfg(test)]
pub(crate) fn router(app: std::sync::Arc<mcp::App>) -> axum::Router {
    http::test_router(app)
}
