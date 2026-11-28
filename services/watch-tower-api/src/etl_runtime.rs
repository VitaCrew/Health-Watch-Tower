use anyhow::Result;
use deadpool_postgres::Pool;

/// Watch Tower owns ETL worker lifecycle. HSM is called as the state owner and
/// retains the provider adapter implementations until their module migration.
pub async fn start(db: Pool, policy_db: Pool) -> Result<()> {
    use avatar_hsm::etl_adapter as adapter;

    adapter::provision_oauth(&db).await?;
    tokio::spawn(crate::etl::wearable_sync::worker(
        db.clone(),
        adapter::http_client()?,
    ));
    tokio::spawn(crate::etl::fit_download::worker(
        db.clone(),
        adapter::http_client()?,
    ));
    tokio::spawn(crate::etl::fit_details::worker(db.clone()));
    tokio::spawn(crate::etl::webhook_documents::worker(
        db.clone(),
        adapter::http_client()?,
    ));
    tokio::spawn(crate::etl::withings_subscriptions::worker(
        db.clone(),
        adapter::http_client()?,
    ));
    tokio::spawn(crate::etl::withings_cleanup::worker(
        db.clone(),
        adapter::http_client()?,
    ));
    tokio::spawn(crate::etl::google_tcx::worker(
        db.clone(),
        adapter::http_client()?,
    ));
    tokio::spawn(crate::etl::garmin_backfill::worker(
        db.clone(),
        adapter::http_client()?,
    ));
    tokio::spawn(crate::etl::garmin_pull::worker(
        db.clone(),
        adapter::http_client()?,
    ));
    tokio::spawn(adapter::rebuild_garmin_metrics(db.clone()));
    tokio::spawn(adapter::rebuild_garmin_workouts(db.clone()));
    tokio::spawn(crate::etl::oura_subscriptions::worker(
        db.clone(),
        adapter::http_client()?,
    ));
    tokio::spawn(crate::cloud_receipts::run(db, policy_db));
    Ok(())
}
