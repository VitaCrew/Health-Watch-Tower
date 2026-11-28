use std::{env, sync::Arc};

use anyhow::{ensure, Context, Result};
use axum::{extract::DefaultBodyLimit, routing::post, Router};

use avatar_fitness::{mcp, store::FitnessStore};

#[tokio::main]
async fn main() -> Result<()> {
    let database_url = env::var("FITNESS_DATABASE_URL")
        .or_else(|_| env::var("AVATAR_HSM_DATABASE_URL"))
        .context("set FITNESS_DATABASE_URL or the shared AVATAR_HSM_DATABASE_URL")?;
    let owner_id = env::var("FITNESS_OWNER_ID")
        .context("set FITNESS_OWNER_ID to the explicitly mapped owner")?;
    let read_token = token("FITNESS_MCP_READ_TOKEN")?;
    let write_token = token("FITNESS_MCP_WRITE_TOKEN")?;
    ensure!(
        read_token != write_token,
        "separate Fitness MCP read and write credentials are required"
    );
    let port = env::var("FITNESS_MCP_PORT")
        .unwrap_or_else(|_| "17841".into())
        .parse::<u16>()
        .context("invalid Fitness MCP port")?;

    let store = FitnessStore::connect(&database_url).await?;
    let app = Arc::new(mcp::App {
        store,
        owner_id,
        read_token,
        write_token,
    });
    let router = Router::new()
        .route("/mcp/{surface}", post(mcp::handle))
        .layer(DefaultBodyLimit::max(6 * 1024 * 1024))
        .with_state(app);
    let listener = tokio::net::TcpListener::bind(("127.0.0.1", port)).await?;
    eprintln!(
        "Avatar Fitness listening on http://{}/mcp/read and /mcp/write",
        listener.local_addr()?
    );
    axum::serve(listener, router)
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
