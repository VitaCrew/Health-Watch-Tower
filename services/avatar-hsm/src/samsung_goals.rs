use std::collections::HashSet;

use anyhow::{ensure, Result};
use chrono::{DateTime, NaiveTime, Utc};
use deadpool_postgres::Pool;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tokio_postgres::Transaction;

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct GoalSnapshot {
    timezone: String,
    targets: Vec<Target>,
}

#[derive(Deserialize, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
enum Target {
    Steps { steps: u32 },
    ActiveTime { seconds: f64 },
    ActiveEnergy { kcal: f64 },
    NutritionEnergy { kcal: f64 },
    Water { milliliters: f64 },
    Bedtime { local_time: NaiveTime },
    WakeTime { local_time: NaiveTime },
}

impl Target {
    fn amount(&self) -> Option<f64> {
        match self {
            Self::ActiveTime { seconds } => Some(*seconds),
            Self::ActiveEnergy { kcal } | Self::NutritionEnergy { kcal } => Some(*kcal),
            Self::Water { milliliters } => Some(*milliliters),
            _ => None,
        }
    }
}

pub async fn store(
    tx: &Transaction<'_>,
    subject: &str,
    observed_at: DateTime<Utc>,
    snapshot: &GoalSnapshot,
) -> Result<()> {
    ensure!(
        !snapshot.timezone.is_empty()
            && snapshot.timezone.len() <= 128
            && snapshot.targets.len() <= 7,
        "invalid Samsung goals snapshot"
    );
    let mut kinds = HashSet::new();
    ensure!(
        snapshot.targets.iter().all(|target| {
            kinds.insert(std::mem::discriminant(target))
                && target
                    .amount()
                    .is_none_or(|amount| amount.is_finite() && amount >= 0.0)
        }),
        "invalid or duplicate Samsung health goal"
    );
    ensure!(
        tx.query_one(
            "SELECT EXISTS(SELECT 1 FROM pg_timezone_names WHERE name=$1)",
            &[&snapshot.timezone]
        )
        .await?
        .get::<_, bool>(0),
        "invalid Samsung goals timezone"
    );
    tx.execute(
        "INSERT INTO avatar_hsm.samsung_goals AS old (subject_id,observed_at,timezone,targets)
         VALUES($1,$2,$3,$4)
         ON CONFLICT (subject_id) DO UPDATE SET observed_at=excluded.observed_at,
             timezone=excluded.timezone,targets=excluded.targets
         WHERE excluded.observed_at >= old.observed_at",
        &[
            &subject,
            &observed_at,
            &snapshot.timezone,
            &serde_json::to_value(&snapshot.targets)?,
        ],
    )
    .await?;
    Ok(())
}

pub async fn query(pool: &Pool, subject: &str) -> Result<Value> {
    let row = pool
        .get()
        .await?
        .query_opt(
            "SELECT observed_at,timezone,targets FROM avatar_hsm.samsung_goals g
         JOIN avatar_hsm.device_connection c USING(subject_id,provider)
         WHERE g.subject_id=$1 AND c.state='synced'",
            &[&subject],
        )
        .await?;
    let Some(row) = row else {
        return Ok(json!({"provider":"samsung_health","state":"not_synced","targets":[]}));
    };
    let targets: Vec<Value> = row
        .get::<_, Value>(2)
        .as_array()
        .cloned()
        .unwrap_or_default();
    Ok(
        json!({"provider":"samsung_health","state":if targets.is_empty() {"no_goals"} else {"ready"},
        "observed_at":row.get::<_, DateTime<Utc>>(0),"timezone":row.get::<_, String>(1),"targets":targets}),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wearables::{self, SAMSUNG_HEALTH};

    #[tokio::test]
    #[ignore = "requires AVATAR_HSM_TEST_DATABASE_URL"]
    async fn latest_goals_replace_revoked_values_and_survive_legacy_uploads() -> Result<()> {
        let pool = crate::store::connect(&std::env::var("AVATAR_HSM_TEST_DATABASE_URL")?).await?;
        let subject = "samsung-goals-fixture";
        pool.get()
            .await?
            .execute(
                "INSERT INTO avatar_hsm.subject VALUES($1,'Asia/Seoul')",
                &[&subject],
            )
            .await?;
        let end: DateTime<Utc> = "2026-04-25T08:00:00Z".parse()?;
        let snapshot = |lease: &Value, until: DateTime<Utc>, goals: Value| {
            serde_json::from_value(
                json!({"lease":lease,"start":until-chrono::Duration::days(1),
                "end":until,"grants":[],"records":[],"samsung_goals":goals}),
            )
        };
        let goals = |targets: Value| json!({"timezone":"Asia/Seoul","targets":targets});
        let steps = |count: u32| json!({"kind":"steps","steps":count});
        let lease = wearables::issue_lease_for(&pool, subject, SAMSUNG_HEALTH).await?;
        assert!(wearables::commit_for(
            &pool,
            subject,
            SAMSUNG_HEALTH,
            snapshot(
                &lease["lease"],
                end,
                goals(json!([steps(5000), steps(6000)]))
            )?
        )
        .await
        .is_err());
        wearables::commit_for(&pool, subject, SAMSUNG_HEALTH, snapshot(&lease["lease"], end,
            goals(json!([steps(8000),{"kind":"bedtime","local_time":"22:30:00"},{"kind":"water","milliliters":2000.0}])) )?).await?;
        let lease = wearables::issue_lease_for(&pool, subject, SAMSUNG_HEALTH).await?;
        wearables::commit_for(
            &pool,
            subject,
            SAMSUNG_HEALTH,
            snapshot(
                &lease["lease"],
                end - chrono::Duration::hours(2),
                goals(json!([steps(7000)])),
            )?,
        )
        .await?;
        let lease = wearables::issue_lease_for(&pool, subject, SAMSUNG_HEALTH).await?;
        wearables::commit_for(
            &pool,
            subject,
            SAMSUNG_HEALTH,
            snapshot(
                &lease["lease"],
                end + chrono::Duration::hours(1),
                Value::Null,
            )?,
        )
        .await?;
        let stored = query(&pool, subject).await?;
        assert_eq!(stored["targets"][0]["steps"], 8000);
        assert_eq!(stored["targets"][1]["local_time"], "22:30:00");
        assert_eq!(
            query(&pool, "another-subject").await?["state"],
            "not_synced"
        );

        let lease = wearables::issue_lease_for(&pool, subject, SAMSUNG_HEALTH).await?;
        wearables::commit_for(
            &pool,
            subject,
            SAMSUNG_HEALTH,
            snapshot(
                &lease["lease"],
                end + chrono::Duration::hours(2),
                goals(json!([steps(9000)])),
            )?,
        )
        .await?;
        let stored = query(&pool, subject).await?;
        assert_eq!(stored["targets"], json!([steps(9000)]));
        wearables::revoke_for(&pool, subject, SAMSUNG_HEALTH).await?;
        let row = pool
            .get()
            .await?
            .query_one(
                "SELECT count(*) FROM avatar_hsm.samsung_goals
            WHERE subject_id=$1",
                &[&subject],
            )
            .await?;
        assert_eq!(row.get::<_, i64>(0), 0);
        Ok(())
    }
}
