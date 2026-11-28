mod auth;
mod clinical;
mod cloud_receipts;
mod etl;
mod etl_runtime;
mod provision;
mod store;
mod unified_mcp;

#[cfg(test)]
#[path = "etl_queue_tests.rs"]
mod etl_queue_tests;

use std::{env, net::SocketAddr, sync::Arc};

use anyhow::{ensure, Context, Result};
use axum::{
    extract::{ConnectInfo, DefaultBodyLimit},
    response::IntoResponse,
    routing::{get, post},
    Json, Router,
};

#[tokio::main]
async fn main() -> Result<()> {
    if let Some(
        command @ ("provision-person" | "revoke-consent" | "revoke-identity" | "grant-role"
        | "revoke-role"),
    ) = env::args().nth(1).as_deref()
    {
        return provision::run_from_stdin(command).await;
    }
    let database_url = env::var("AVATAR_HSM_DATABASE_URL")
        .context("set AVATAR_HSM_DATABASE_URL to the shared Watch Tower PostgreSQL database")?;
    let read_token = token("AVATAR_HSM_READ_TOKEN")?;
    let control_token = token("AVATAR_HSM_CONTROL_TOKEN")?;
    ensure!(
        read_token != control_token,
        "read and control tokens must differ"
    );
    let fitness_owner_id = env::var("FITNESS_OWNER_ID")
        .context("set FITNESS_OWNER_ID to the explicitly mapped Fitness owner")?;
    let bind = env::var("WATCH_TOWER_BIND_ADDRESS")
        .unwrap_or_else(|_| "127.0.0.1:17842".to_owned())
        .parse::<SocketAddr>()
        .context("invalid WATCH_TOWER_BIND_ADDRESS")?;
    let authenticator = Arc::new(auth::Authenticator::from_env(
        read_token.clone(),
        control_token.clone(),
        bind.ip().is_loopback(),
    )?);

    let hsm_pool = avatar_hsm::store::connect(&database_url).await?;
    let policy_pool = store::connect(&database_url).await?;
    let hsm = Arc::new(avatar_hsm::mcp_app(
        hsm_pool,
        read_token.clone(),
        control_token.clone(),
    )?);
    clinical::start_hsm_outbox_worker(hsm.db.clone(), policy_pool.clone());
    if env::var("WATCH_TOWER_START_ETL_WORKERS").as_deref() == Ok("1") {
        etl_runtime::start(hsm.db.clone(), policy_pool.clone()).await?;
    }
    let fitness = Arc::new(avatar_fitness::mcp::App {
        store: avatar_fitness::store::FitnessStore::connect(&database_url).await?,
        owner_id: fitness_owner_id,
        read_token,
        write_token: control_token,
    });

    let app = Router::new()
        .merge(avatar_hsm::http_router(hsm.clone()))
        .route(
            "/healthz",
            get(|| async { Json(serde_json::json!({"status":"ok"})) }),
        )
        .route(
            "/.well-known/oauth-protected-resource",
            get({
                let authenticator = authenticator.clone();
                move || {
                    let metadata = authenticator.protected_resource_metadata();
                    async move {
                        metadata.map(Json).map_or_else(
                            || axum::http::StatusCode::NOT_FOUND.into_response(),
                            |body| body.into_response(),
                        )
                    }
                }
            }),
        )
        .route(
            "/.well-known/oauth-protected-resource/mcp",
            get({
                let authenticator = authenticator.clone();
                move || {
                    let metadata = authenticator.protected_resource_metadata();
                    async move {
                        metadata.map(Json).map_or_else(
                            || axum::http::StatusCode::NOT_FOUND.into_response(),
                            |body| body.into_response(),
                        )
                    }
                }
            }),
        )
        .route(
            "/mcp",
            post({
                let hsm = hsm.clone();
                let fitness = fitness.clone();
                let policy_pool = policy_pool.clone();
                let authenticator = authenticator.clone();
                move |ConnectInfo(peer): ConnectInfo<SocketAddr>,
                      headers: axum::http::HeaderMap,
                      axum::Json(message): axum::Json<serde_json::Value>| {
                    let hsm = hsm.clone();
                    let fitness = fitness.clone();
                    let policy_pool = policy_pool.clone();
                    let authenticator = authenticator.clone();
                    async move {
                        unified_mcp::handle(
                            hsm,
                            fitness,
                            policy_pool,
                            authenticator,
                            peer,
                            headers,
                            message,
                        )
                        .await
                    }
                }
            })
            .layer(DefaultBodyLimit::max(6 * 1024 * 1024)),
        );
    let listener = tokio::net::TcpListener::bind(bind).await?;
    eprintln!(
        "Health Watch Tower MCP listening on http://{}/mcp",
        listener.local_addr()?
    );
    axum::serve(
        listener,
        app.into_make_service_with_connect_info::<SocketAddr>(),
    )
    .with_graceful_shutdown(async {
        let _ = tokio::signal::ctrl_c().await;
    })
    .await?;
    Ok(())
}

fn token(name: &str) -> Result<String> {
    let value = env::var(name).with_context(|| format!("set {name}"))?;
    ensure!(
        value.len() >= 32,
        "{name} must contain at least 32 characters"
    );
    Ok(value)
}
