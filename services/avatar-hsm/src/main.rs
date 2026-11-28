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
mod mcp;
mod mcp_client;
#[cfg(test)]
mod oura_webhook_subscriptions;
mod samsung_activity;
mod samsung_goals;
mod samsung_routes;
mod state_machine;
mod state_owner;
mod store;
mod wearable_fit;
mod wearable_fit_details;
mod wearable_oauth;
mod wearable_sync;
mod wearable_webhook_documents;
mod wearable_webhooks;
mod wearables;
mod withings_cleanup;
mod withings_notify;

use std::env;
use std::sync::Arc;

use anyhow::{ensure, Context, Result};
use axum::{
    extract::DefaultBodyLimit,
    routing::{get, head, post},
    Router,
};

#[tokio::main]
async fn main() -> Result<()> {
    let arguments: Vec<_> = env::args_os().skip(1).collect();
    if !arguments.is_empty() {
        return apple_health_export::command(&arguments).await;
    }
    let read_token = token("AVATAR_HSM_READ_TOKEN")?;
    let control_token = token("AVATAR_HSM_CONTROL_TOKEN")?;
    ensure!(
        read_token != control_token,
        "separate read and control credentials are required"
    );
    let port = env::var("AVATAR_HSM_PORT")
        .unwrap_or_else(|_| "17840".into())
        .parse::<u16>()
        .context("invalid HSM port")?;
    let db_url = env::var("AVATAR_HSM_DATABASE_URL").context("set AVATAR_HSM_DATABASE_URL")?;
    let db = store::connect(&db_url).await?;
    wearable_oauth::provision(&db).await?;
    let rules = acute::rules()?;
    let listener = tokio::net::TcpListener::bind(("127.0.0.1", port)).await?;
    let app = Arc::new(mcp::App {
        db,
        read_token,
        control_token,
        rules,
    });
    eprintln!(
        "Avatar HSM adapter listening on http://{}/mcp/read and /mcp/control",
        listener.local_addr()?
    );
    axum::serve(listener, router(app))
        .with_graceful_shutdown(async {
            let _ = tokio::signal::ctrl_c().await;
        })
        .await?;
    Ok(())
}

fn router(app: Arc<mcp::App>) -> Router {
    Router::new()
        .route(
            "/wearables/webhooks/garmin/{key}",
            post(garmin_push::receive)
                .layer(axum::extract::DefaultBodyLimit::max(garmin_push::MAX_BODY)),
        )
        .route(
            "/wearables/webhooks/coros",
            get(wearable_webhooks::coros_probe).post(wearable_webhooks::coros_event),
        )
        .route(
            "/wearables/webhooks/withings/{key}",
            head(wearable_webhooks::withings_probe).post(wearable_webhooks::withings_event),
        )
        .route(
            "/wearables/webhooks/suunto/{key}",
            post(wearable_webhooks::suunto_event),
        )
        .route(
            "/wearables/webhooks/google_health",
            post(google_health_webhooks::receive),
        )
        .route(
            "/wearables/webhooks/intervals_icu",
            post(wearable_webhooks::intervals_event),
        )
        .route(
            "/wearables/webhooks/{provider}",
            get(wearable_webhooks::oura_challenge).post(wearable_webhooks::signed_event),
        )
        .route(
            "/wearables/webhooks/strava/{capability}",
            get(wearable_webhooks::strava_challenge).post(wearable_webhooks::strava_event),
        )
        .route(
            "/mcp/{surface}",
            post(mcp::handle).layer(DefaultBodyLimit::max(6 * 1024 * 1024)),
        )
        .route(
            "/wearables/oauth/{provider}/callback",
            get(wearable_oauth::callback),
        )
        .route("/wearables/connect", get(wearable_oauth::portal))
        .route("/wearables/portal.js", get(wearable_oauth::portal_script))
        .route("/wearables/portal.css", get(wearable_oauth::portal_style))
        .route(
            "/wearables/fit/{provider}/{activity}",
            get(wearable_fit::user_download),
        )
        .route(
            "/wearables/tcx/google_health/{activity}",
            get(google_workout_tcx::user_download),
        )
        .route(
            "/internal/wearables/fit/{provider}/{activity}",
            get(wearable_fit::fixture_download),
        )
        .with_state(app)
}

fn token(name: &str) -> Result<String> {
    let value = env::var(name).with_context(|| format!("set {name}"))?;
    ensure!(
        value.len() >= 32
            && value
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || b"-_".contains(&byte)),
        "{name} must be a URL-safe bearer of at least 32 characters"
    );
    Ok(value)
}

#[cfg(test)]
fn mcp_app(
    db: deadpool_postgres::Pool,
    read_token: String,
    control_token: String,
) -> Result<mcp::App> {
    Ok(mcp::App {
        db,
        read_token,
        control_token,
        rules: acute::rules()?,
    })
}
