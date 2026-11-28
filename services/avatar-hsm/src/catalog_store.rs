use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use deadpool_postgres::Pool;
use serde_json::Value;

pub async fn save(pool: &Pool, items: &[Value]) -> Result<()> {
    if items.is_empty() {
        return Ok(());
    }
    let db = pool.get().await?;
    for item in items {
        let source = item["source"]
            .as_str()
            .context("catalog item lacks source")?;
        let source_id = item["source_id"]
            .as_str()
            .context("catalog item lacks source id")?;
        let license_id = item["license_id"]
            .as_str()
            .context("catalog item lacks license")?;
        let license_uri = item["license_uri"]
            .as_str()
            .context("catalog item lacks license URI")?;
        let source_uri = item["source_uri"]
            .as_str()
            .context("catalog item lacks source URI")?;
        let retrieved_at = item["retrieved_at"]
            .as_str()
            .context("catalog item lacks retrieval time")?;
        let retrieved_at =
            chrono::DateTime::parse_from_rfc3339(retrieved_at)?.with_timezone(&chrono::Utc);
        db.execute(
            "INSERT INTO avatar_catalog.reference_item
               (source,source_id,payload,license_id,license_uri,source_uri,retrieved_at)
             VALUES($1,$2,$3,$4,$5,$6,$7)
             ON CONFLICT(source,source_id) DO UPDATE SET
               payload=EXCLUDED.payload,license_id=EXCLUDED.license_id,
               license_uri=EXCLUDED.license_uri,source_uri=EXCLUDED.source_uri,
               retrieved_at=EXCLUDED.retrieved_at",
            &[
                &source,
                &source_id,
                item,
                &license_id,
                &license_uri,
                &source_uri,
                &retrieved_at,
            ],
        )
        .await?;
    }
    Ok(())
}

pub async fn find(
    pool: &Pool,
    source: &str,
    query: &str,
    fresh_after: Option<DateTime<Utc>>,
) -> Result<Vec<Value>> {
    let db = pool.get().await?;
    let rows = if let Some(fresh_after) = fresh_after {
        db.query(
            "SELECT payload FROM avatar_catalog.reference_item
             WHERE source=$1 AND (source_id=$2 OR strpos(lower(payload->>'name'),lower($2))>0)
               AND retrieved_at >= $3
             ORDER BY retrieved_at DESC LIMIT 10",
            &[&source, &query, &fresh_after],
        )
        .await?
    } else {
        db.query(
            "SELECT payload FROM avatar_catalog.reference_item
             WHERE source=$1 AND (source_id=$2 OR strpos(lower(payload->>'name'),lower($2))>0)
             ORDER BY retrieved_at DESC LIMIT 10",
            &[&source, &query],
        )
        .await?
    };
    Ok(rows.into_iter().map(|row| row.get("payload")).collect())
}
