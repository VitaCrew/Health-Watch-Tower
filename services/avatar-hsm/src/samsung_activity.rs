use std::collections::HashSet;

use anyhow::{ensure, Result};
use chrono::{DateTime, NaiveDate, Utc};
use serde::{Deserialize, Serialize};
use tokio_postgres::Transaction;

#[derive(Deserialize, Serialize)]
#[serde(tag = "permission", rename_all = "snake_case", deny_unknown_fields)]
pub enum ActivitySnapshot {
    Denied,
    Granted {
        timezone: String,
        days: Vec<ActivityDay>,
    },
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ActivityDay {
    day: NaiveDate,
    stats: ActivityTotals,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct ActivityTotals {
    #[serde(skip_serializing_if = "Option::is_none")]
    active_seconds: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    active_energy_kcal: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    total_energy_kcal: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    distance_meters: Option<f64>,
}

pub(crate) async fn store(
    tx: &Transaction<'_>,
    subject: &str,
    start: DateTime<Utc>,
    end: DateTime<Utc>,
    snapshot: &ActivitySnapshot,
) -> Result<()> {
    let ActivitySnapshot::Granted { timezone, days } = snapshot else {
        tx.execute(
            "DELETE FROM avatar_hsm.wearable_daily
            WHERE subject_id=$1 AND provider='samsung_health' AND kind='activity'",
            &[&subject],
        )
        .await?;
        tx.execute(
            "UPDATE avatar_hsm.device_connection SET samsung_activity_granted=false
            WHERE subject_id=$1 AND provider='samsung_health'",
            &[&subject],
        )
        .await?;
        return Ok(());
    };
    ensure!(
        !timezone.is_empty() && timezone.len() <= 128 && days.len() <= 9,
        "invalid Samsung activity date range"
    );
    ensure!(
        tx.query_one(
            "SELECT EXISTS(SELECT 1 FROM pg_timezone_names WHERE name=$1)",
            &[timezone]
        )
        .await?
        .get::<_, bool>(0),
        "invalid Samsung activity timezone"
    );
    let bounds = tx
        .query_one(
            "SELECT date_trunc('day', first)::date + (first <> date_trunc('day', first))::integer,
                last::date
         FROM (SELECT $1::timestamptz AT TIME ZONE $3 AS first,
                      $2::timestamptz AT TIME ZONE $3 AS last) dates",
            &[&start, &end, timezone],
        )
        .await?;
    let lower: NaiveDate = bounds.get(0);
    let upper: NaiveDate = bounds.get(1);
    let mut seen = HashSet::new();
    for day in days {
        let stats = &day.stats;
        let measurements = [
            stats.active_seconds,
            stats.active_energy_kcal,
            stats.total_energy_kcal,
            stats.distance_meters,
        ];
        ensure!(
            (lower..upper).contains(&day.day)
                && seen.insert(day.day)
                && measurements.iter().any(Option::is_some)
                && measurements
                    .iter()
                    .flatten()
                    .all(|&v| v.is_finite() && v >= 0.0),
            "invalid Samsung activity day"
        );
    }
    tx.execute(
        "DELETE FROM avatar_hsm.wearable_daily WHERE subject_id=$1 AND provider='samsung_health'
        AND kind='activity' AND timezone=$2 AND local_day >= $3 AND local_day < $4",
        &[&subject, timezone, &lower.to_string(), &upper.to_string()],
    )
    .await?;
    for day in days {
        tx.execute(
            "INSERT INTO avatar_hsm.wearable_daily
            (subject_id,provider,kind,source,local_day,timezone,calculated_by,stats)
            VALUES($1,'samsung_health','activity','Samsung Health',$2,$3,'provider',$4)",
            &[
                &subject,
                &day.day.to_string(),
                timezone,
                &serde_json::to_value(&day.stats)?,
            ],
        )
        .await?;
    }
    tx.execute(
        "UPDATE avatar_hsm.device_connection SET samsung_activity_granted=true
        WHERE subject_id=$1 AND provider='samsung_health'",
        &[&subject],
    )
    .await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wearables::{self, DailyQuery, SAMSUNG_HEALTH};
    use serde_json::json;

    #[tokio::test]
    #[ignore = "requires AVATAR_HSM_TEST_DATABASE_URL"]
    async fn samsung_activity_is_scoped_to_complete_local_days_and_readable_via_mcp() -> Result<()>
    {
        let pool = crate::store::connect(&std::env::var("AVATAR_HSM_TEST_DATABASE_URL")?).await?;
        let subject = "samsung-activity-fixture";
        let read_token = "samsung-activity-test-read-token-0123456789";
        let db = pool.get().await?;
        db.execute(
            "INSERT INTO avatar_hsm.subject VALUES($1,'Asia/Seoul')",
            &[&subject],
        )
        .await?;
        db.execute(
            "INSERT INTO avatar_hsm.wearable_profile VALUES(
            $1,sha256(convert_to($2,'UTF8')),sha256(convert_to($2||'c','UTF8')))",
            &[&subject, &read_token],
        )
        .await?;
        drop(db);

        // New York's spring transition makes March 8 a 23-hour day.
        let snapshot = |lease: &serde_json::Value, activity: serde_json::Value| {
            serde_json::from_value(json!({
                "lease": lease, "start": "2026-03-08T05:00:00Z", "end": "2026-03-09T12:00:00Z",
                "grants": [], "records": [], "samsung_activity": activity
            }))
        };
        let totals = |day: &str, distance: u32| {
            json!({
                "permission":"granted","timezone":"America/New_York","days":[{"day":day,"stats":{
                    "active_seconds":2340,"active_energy_kcal":150.4,"total_energy_kcal":1780,
                    "distance_meters":distance
                }}]
            })
        };
        let lease = wearables::issue_lease_for(&pool, subject, SAMSUNG_HEALTH).await?;
        assert!(wearables::commit_for(
            &pool,
            subject,
            SAMSUNG_HEALTH,
            snapshot(&lease["lease"], totals("2026-03-09", 2400))?
        )
        .await
        .is_err());
        wearables::commit_for(
            &pool,
            subject,
            SAMSUNG_HEALTH,
            snapshot(&lease["lease"], totals("2026-03-08", 2400))?,
        )
        .await?;
        let lease = wearables::issue_lease_for(&pool, subject, SAMSUNG_HEALTH).await?;
        wearables::commit_for(
            &pool,
            subject,
            SAMSUNG_HEALTH,
            snapshot(&lease["lease"], totals("2026-03-08", 2500))?,
        )
        .await?;
        let lease = wearables::issue_lease_for(&pool, subject, SAMSUNG_HEALTH).await?;
        wearables::commit_for(
            &pool,
            subject,
            SAMSUNG_HEALTH,
            snapshot(&lease["lease"], serde_json::Value::Null)?,
        )
        .await?;

        let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0)).await?;
        let address = listener.local_addr()?;
        let app = std::sync::Arc::new(crate::mcp::App {
            db: pool.clone(),
            read_token: String::new(),
            control_token: String::new(),
            rules: crate::acute::rules()?,
        });
        let server = tokio::spawn(async move { axum::serve(listener, crate::router(app)).await });
        let body: serde_json::Value = crate::cloud_wearables::http_client()?
            .post(format!("http://{address}/mcp/wearable-read")).bearer_auth(read_token)
            .json(&json!({"jsonrpc":"2.0","id":1,"method":"tools/call","params":{
                "name":"wearable_daily","arguments":{"kind":"activity", "provider":SAMSUNG_HEALTH,
                    "start_day":"2026-03-08","end_day":"2026-03-09"}
            }}))
            .send().await?.error_for_status()?.json().await?;
        server.abort();
        let rows = body["result"]["structuredContent"]["records"]
            .as_array()
            .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0]["calculated_by"], "provider");
        assert_eq!(rows[0]["stats"]["distance_meters"].as_f64(), Some(2500.0));
        assert_eq!(rows[0]["timezone"], "America/New_York");

        let lease = wearables::issue_lease_for(&pool, subject, SAMSUNG_HEALTH).await?;
        wearables::commit_for(
            &pool,
            subject,
            SAMSUNG_HEALTH,
            snapshot(&lease["lease"], json!({"permission":"denied"}))?,
        )
        .await?;
        let state = wearables::daily_for(
            &pool,
            subject,
            DailyQuery {
                start_day: "2026-03-08".parse()?,
                end_day: "2026-03-09".parse()?,
                kind: Some("activity".into()),
                provider: Some(SAMSUNG_HEALTH.into()),
                source: None,
            },
        )
        .await?;
        assert_eq!(state["state"], "permission_missing");
        let row = pool
            .get()
            .await?
            .query_one(
                "SELECT count(*) FROM avatar_hsm.wearable_daily
            WHERE subject_id=$1 AND provider='samsung_health' AND kind='activity'",
                &[&subject],
            )
            .await?;
        assert_eq!(row.get::<_, i64>(0), 0);
        Ok(())
    }
}
