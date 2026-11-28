use std::sync::Arc;
use std::time::Duration;

use anyhow::{bail, Context, Result};
use chrono::{DateTime, Utc};
use deadpool_postgres::Pool;
use serde_json::{json, Value};
use tokio_postgres::Client;
use zen_engine::{model::DecisionContent, Decision, DecisionEngine};

use crate::store::SUBJECT;

const RULE_VERSION: &str = "acute-synthetic-v1";

pub fn rules() -> Result<Decision> {
    let content: DecisionContent =
        serde_json::from_str(include_str!("../rules/acute-synthetic-v1.json"))?;
    let decision = DecisionEngine::default().create_decision(Arc::new(content))?;
    decision
        .validate()
        .context("invalid Acute decision graph")?;
    Ok(decision)
}

async fn evaluate(rules: &Decision, signal: &str) -> Result<bool> {
    let response = rules
        .evaluate(json!({ "kind": signal }).into())
        .await
        .map_err(|_| anyhow::anyhow!("Acute decision evaluation failed"))?;
    serde_json::to_value(response.result)?["emit"]
        .as_bool()
        .context("Acute decision did not return a boolean")
}

pub async fn record(
    pool: &Pool,
    rules: &Decision,
    id: &str,
    signal: &str,
    observed: DateTime<Utc>,
) -> Result<Value> {
    if !matches!(
        signal,
        "synthetic_fall" | "synthetic_cardiac_flag" | "synthetic_normal"
    ) {
        bail!("unsupported synthetic signal");
    }
    let owned_rules = rules.clone();
    let owned_signal = signal.to_owned();
    let emit = tokio::task::spawn_blocking(move || {
        tokio::runtime::Builder::new_current_thread()
            .build()?
            .block_on(evaluate(&owned_rules, &owned_signal))
    })
    .await??;
    let mut db = pool.get().await?;
    let tx = db.transaction().await?;
    let row = tx
        .query_opt(
            "WITH decision AS (
           INSERT INTO avatar_hsm.acute_decision AS held
             (event_id, subject_id, signal, observed_at, rule_version, emitted)
           VALUES ($1, $2, $3, $4, $5, $6)
           ON CONFLICT (event_id) DO UPDATE SET event_id = EXCLUDED.event_id
           WHERE held.subject_id = EXCLUDED.subject_id
             AND held.signal = EXCLUDED.signal AND held.observed_at = EXCLUDED.observed_at
           RETURNING event_id, rule_version, emitted
         ), queued AS (
           INSERT INTO avatar_hsm.health_outbox (event_id)
           SELECT event_id FROM decision WHERE emitted
           ON CONFLICT DO NOTHING
         )
         SELECT rule_version, emitted FROM decision",
            &[&id, &SUBJECT, &signal, &observed, &RULE_VERSION, &emit],
        )
        .await?;
    let row = row.context("event ID already exists with different input")?;
    let result = json!({
        "event_id": id,
        "rule_version": row.get::<_, String>(0),
        "emitted": row.get::<_, bool>(1),
    });
    crate::state_machine::append(
        &tx,
        SUBJECT,
        &crate::state_machine::StateEvent {
            event_id: format!("acute-decision:{id}"),
            event_type: "acute_signal_evaluated".into(),
            event_version: 1,
            actor_id: "service:hsm-acute-rules".into(),
            occurred_at: observed,
            source_ref: Some(format!("acute-decision:{id}")),
            correlation_id: None,
            causation_id: Some(id.to_owned()),
            consent_version: None,
            rule_version: Some(RULE_VERSION.into()),
            payload: json!({"signal": signal, "emitted": row.get::<_, bool>(1)}),
        },
        None,
    )
    .await?;
    tx.commit().await?;
    Ok(result)
}

pub async fn deliver_outbox(db: Pool, url: reqwest::Url, token: String) {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(8))
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .expect("default HTTP client settings");
    let mut interval = tokio::time::interval(Duration::from_secs(2));
    loop {
        interval.tick().await;
        let result = match db.get().await {
            Ok(connection) => deliver_waiting(&connection, &client, &url, &token).await,
            Err(error) => Err(error.into()),
        };
        if result.is_err() {
            eprintln!("HSM outbox read or acknowledge failed");
        }
    }
}

async fn deliver_waiting(
    db: &Client,
    http: &reqwest::Client,
    url: &reqwest::Url,
    token: &str,
) -> Result<()> {
    let rows = db
        .query(
            "UPDATE avatar_hsm.health_outbox SET tries = tries + 1,
            retry_at = now() + interval '15 seconds'
         WHERE event_id IN (
           SELECT event_id FROM avatar_hsm.health_outbox
           WHERE received_at IS NULL AND retry_at <= now() ORDER BY created_at LIMIT 8
           FOR UPDATE SKIP LOCKED
         )
         RETURNING event_id",
            &[],
        )
        .await?;
    for row in rows {
        let id: String = row.get(0);
        let result = crate::mcp_client::call(
            http,
            url,
            token,
            "submit_health_event",
            json!({"event_id": id}),
        )
        .await;
        if result.is_ok_and(|value| value["accepted"] == true) {
            db.execute(
                "UPDATE avatar_hsm.health_outbox SET received_at = COALESCE(received_at, now())
                 WHERE event_id = $1",
                &[&id],
            )
            .await?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn pinned_synthetic_zen_graph_classifies_signals() -> Result<()> {
        let decision = rules()?;
        assert!(evaluate(&decision, "synthetic_fall").await?);
        assert!(evaluate(&decision, "synthetic_cardiac_flag").await?);
        assert!(!evaluate(&decision, "synthetic_normal").await?);
        Ok(())
    }
}
