use anyhow::{ensure, Result};
use chrono::{DateTime, Utc};
use deadpool_postgres::Pool;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tokio_postgres::Client;

use crate::store::SUBJECT;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConfirmedEntry {
    pub id: String,
    entry: Entry,
    #[serde(default)]
    occurred_at: Option<DateTime<Utc>>,
}

#[derive(Deserialize, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
enum Entry {
    Meal {
        name: String,
        amount: f64,
        unit: String,
        #[serde(default)]
        nutrients_per_basis: Option<Nutrients>,
        #[serde(default)]
        nutrient_basis_amount: Option<f64>,
        #[serde(default)]
        nutrient_basis_unit: Option<String>,
        #[serde(default)]
        source: Option<String>,
        #[serde(default)]
        source_id: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        catalog: Option<Catalog>,
    },
    Water {
        ml: u32,
    },
    Strength {
        exercise: String,
        sets: u32,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        target_sets: Option<u32>,
        reps: u32,
        kg: f64,
    },
}

#[derive(Deserialize, Serialize, Clone)]
#[serde(deny_unknown_fields)]
struct Nutrients {
    kcal: Option<f64>,
    protein_g: Option<f64>,
    carbs_g: Option<f64>,
    fat_g: Option<f64>,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Catalog {
    source: String,
    source_id: String,
}

impl Entry {
    fn validate(&self) -> Result<&'static str> {
        let text = |s: &str, max| (1..=max).contains(&s.trim().chars().count());
        match self {
            Self::Meal {
                name,
                amount,
                unit,
                nutrients_per_basis,
                nutrient_basis_amount,
                nutrient_basis_unit,
                source,
                source_id,
                catalog,
            } => {
                ensure!(
                    text(name, 120) && text(unit, 24) && *amount > 0.0 && *amount <= 100_000.0,
                    "invalid meal"
                );
                ensure!(
                    nutrient_basis_amount.is_none_or(|basis| basis > 0.0 && basis <= 100_000.0),
                    "invalid nutrient basis"
                );
                if nutrients_per_basis.is_some() {
                    ensure!(
                        nutrient_basis_amount.is_some()
                            && nutrient_basis_unit.as_deref() == Some(unit.as_str()),
                        "nutrients require a matching basis amount and unit"
                    );
                    let nutrients = nutrients_per_basis.as_ref().unwrap();
                    ensure!(
                        [
                            nutrients.kcal,
                            nutrients.protein_g,
                            nutrients.carbs_g,
                            nutrients.fat_g
                        ]
                        .into_iter()
                        .flatten()
                        .all(|value| value.is_finite() && value >= 0.0),
                        "invalid nutrient values"
                    );
                }
                ensure!(
                    source.is_none() == source_id.is_none(),
                    "catalog source and identifier must be paired"
                );
                if let Some(catalog) = catalog {
                    ensure!(
                        matches!(catalog.source.as_str(), "open_food_facts" | "usda" | "mfds")
                            && (1..=20).contains(&catalog.source_id.len())
                            && catalog
                                .source_id
                                .bytes()
                                .all(|byte| byte.is_ascii_alphanumeric()
                                    || matches!(byte, b'-' | b'_')),
                        "invalid catalog reference"
                    );
                }
                Ok("meal")
            }
            Self::Water { ml } => {
                ensure!((1..=20_000).contains(ml), "invalid water amount");
                Ok("water")
            }
            Self::Strength {
                exercise,
                sets,
                target_sets,
                reps,
                kg,
            } => {
                ensure!(
                    text(exercise, 120)
                        && (1..=100).contains(sets)
                        && target_sets.is_none_or(|target| target >= *sets && target <= 100)
                        && (1..=1000).contains(reps)
                        && (0.0..=2000.0).contains(kg),
                    "invalid workout"
                );
                Ok("strength")
            }
        }
    }
}

pub async fn save(pool: &Pool, confirmed: ConfirmedEntry) -> Result<Value> {
    save_for(pool, SUBJECT, confirmed).await
}

pub async fn save_for(pool: &Pool, subject_id: &str, confirmed: ConfirmedEntry) -> Result<Value> {
    let mut db = pool.get().await?;
    let tx = db.transaction().await?;
    let kind = confirmed.entry.validate()?;
    let mut details = serde_json::to_value(&confirmed.entry)?;
    if kind == "meal" && details["catalog"]["source"].is_string() {
        let source = details["catalog"]["source"].as_str().unwrap();
        let source_id = details["catalog"]["source_id"].as_str().unwrap();
        if let Some(row) = tx.query_opt("SELECT payload FROM avatar_catalog.reference_item WHERE source=$1 AND source_id=$2", &[&source,&source_id]).await? {
            let food: Value = row.get(0);
            if food["basis_amount"].as_f64().is_some() && food["basis_unit"] == details["unit"] {
                for field in ["nutrients_per_basis","basis_amount","basis_unit","source","source_id"] {
                    details[match field {"basis_amount"=>"nutrient_basis_amount","basis_unit"=>"nutrient_basis_unit", other=>other}] = food[field].clone();
                }
            }
        }
    }
    let occurred_at = confirmed.occurred_at.unwrap_or_else(Utc::now);
    let row = tx
        .query_opt(
            "INSERT INTO avatar_hsm.diary_entry AS entry (subject_id, id, kind, details, occurred_at)
         VALUES ($1, $2, $3, $4, $5)
         ON CONFLICT (subject_id, id) DO UPDATE SET details = entry.details
           WHERE entry.details = excluded.details AND entry.deleted_at IS NULL
         RETURNING id, details, recorded_at, revision, occurred_at",
            &[&subject_id, &confirmed.id, &kind, &details, &occurred_at],
        )
        .await?;
    let result = row
        .as_ref()
        .map(|row| record(row, subject_id))
        .ok_or_else(|| anyhow::anyhow!("record identifier already used for a different entry"))?;
    if kind == "meal" {
        let day: chrono::NaiveDate = tx.query_one("SELECT (occurred_at AT TIME ZONE (SELECT timezone FROM avatar_hsm.subject WHERE id=$1))::date FROM avatar_hsm.diary_entry WHERE subject_id=$1 AND id=$2", &[&subject_id,&confirmed.id]).await?.get(0);
        rebuild_nutrition_day(&*tx, subject_id, day).await?;
    }
    let stored_revision = row.as_ref().map(|row| row.get::<_, i64>(3)).unwrap_or(1);
    crate::state_machine::append(
        &tx,
        subject_id,
        &crate::state_machine::StateEvent {
            event_id: format!("diary-entry:{}:{stored_revision}", confirmed.id),
            event_type: "confirmed_diary_fact_recorded".into(),
            event_version: 1,
            actor_id: "service:hsm-diary".into(),
            occurred_at: row.as_ref().map(|row| row.get(4)).unwrap_or(occurred_at),
            source_ref: Some(format!("diary-entry:{}", confirmed.id)),
            correlation_id: None,
            causation_id: None,
            consent_version: None,
            rule_version: None,
            payload: json!({"kind": kind, "revision": stored_revision}),
        },
        None,
    )
    .await?;
    tx.commit().await?;
    Ok(result)
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Query {
    start: DateTime<Utc>,
    end: DateTime<Utc>,
    kind: Option<String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NutritionQuery {
    start: DateTime<Utc>,
    end: DateTime<Utc>,
}

pub async fn nutrition_summary(db: &Client, query: NutritionQuery) -> Result<Value> {
    nutrition_summary_for(db, SUBJECT, query).await
}

pub async fn nutrition_summary_for(
    db: &Client,
    subject_id: &str,
    query: NutritionQuery,
) -> Result<Value> {
    ensure!(
        query.start < query.end && query.end - query.start <= chrono::Duration::days(90),
        "select a range of up to 90 days"
    );
    let rows = db.query(
        "SELECT local_day::text,meal_count,calories_kcal,protein_g,carbs_g,fat_g,unresolved_meals,revision
         FROM avatar_hsm.nutrition_daily_state
         WHERE subject_id=$1
           AND local_day >= ($2 AT TIME ZONE (SELECT timezone FROM avatar_hsm.subject WHERE id=$1))::date
           AND local_day <= timezone((SELECT timezone FROM avatar_hsm.subject WHERE id=$1),$3::timestamptz - interval '1 microsecond')::date
         ORDER BY local_day",
        &[&subject_id, &query.start, &query.end],
    ).await?;
    Ok(json!({"days": rows.iter().map(|row| json!({
        "local_day":row.get::<_,String>(0),
        "meal_count":row.get::<_,i32>(1),
        "confirmed_kcal_subtotal":row.get::<_,Option<f64>>(2),
        "protein_g":row.get::<_,Option<f64>>(3),
        "carbs_g":row.get::<_,Option<f64>>(4),
        "fat_g":row.get::<_,Option<f64>>(5),
        "unresolved_meals":row.get::<_,i32>(6),
        "revision":row.get::<_,i64>(7)
    })).collect::<Vec<_>>(),"recomputed_at":Utc::now(),"source":"avatar_hsm.diary_entry+wearable_record","synthetic":subject_id == SUBJECT}))
}

pub(crate) async fn rebuild_nutrition_day<C: tokio_postgres::GenericClient + Sync>(
    db: &C,
    subject: &str,
    day: chrono::NaiveDate,
) -> Result<()> {
    db.execute(
        "INSERT INTO avatar_hsm.nutrition_daily_state(subject_id,local_day,meal_count,calories_kcal,protein_g,carbs_g,fat_g,unresolved_meals,revision,rebuilt_at)
         WITH device_rows AS (
           SELECT w.started_at,NULLIF(lower(btrim(d.p->>'title')),'') AS title,w.provider,w.source,w.source_id,
             CASE WHEN d.p->>'unit'='kcal' AND d.p->>'value' ~ '^[0-9]+(\\.[0-9]+)?$' THEN (d.p->>'value')::numeric END AS kcal,
             CASE WHEN COALESCE(d.p->'nutrients_g'->>'protein',d.p->'nutrients'->>'protein_g') ~ '^[0-9]+(\\.[0-9]+)?$' THEN COALESCE(d.p->'nutrients_g'->>'protein',d.p->'nutrients'->>'protein_g')::numeric END AS protein_g,
             CASE WHEN COALESCE(d.p->'nutrients_g'->>'totalCarbohydrate',d.p->'nutrients_g'->>'carbohydrates',d.p->'nutrients'->>'carbs_g') ~ '^[0-9]+(\\.[0-9]+)?$' THEN COALESCE(d.p->'nutrients_g'->>'totalCarbohydrate',d.p->'nutrients_g'->>'carbohydrates',d.p->'nutrients'->>'carbs_g')::numeric END AS carbs_g,
             CASE WHEN COALESCE(d.p->'nutrients_g'->>'totalFat',d.p->'nutrients_g'->>'fat',d.p->'nutrients'->>'fat_g') ~ '^[0-9]+(\\.[0-9]+)?$' THEN COALESCE(d.p->'nutrients_g'->>'totalFat',d.p->'nutrients_g'->>'fat',d.p->'nutrients'->>'fat_g')::numeric END AS fat_g,
             NOT (d.p->>'unit'='kcal' AND d.p->>'value' ~ '^[0-9]+(\\.[0-9]+)?$')
               AND COALESCE(d.p->'nutrients_g'->>'protein',d.p->'nutrients'->>'protein_g',d.p->'nutrients_g'->>'totalCarbohydrate',d.p->'nutrients_g'->>'carbohydrates',d.p->'nutrients'->>'carbs_g',d.p->'nutrients_g'->>'totalFat',d.p->'nutrients_g'->>'fat',d.p->'nutrients'->>'fat_g') IS NULL AS unresolved
           FROM avatar_hsm.wearable_record w
           JOIN avatar_hsm.subject s ON s.id=w.subject_id
           CROSS JOIN LATERAL (SELECT w.payload->'provider_data' AS p) d
           WHERE w.subject_id=$1 AND w.provider IN ('health_connect','samsung_health')
             AND w.kind='metric' AND w.payload->>'metric'='nutrition'
             AND (w.started_at AT TIME ZONE s.timezone)::date=$2
         ), deduplicated_device AS (
           SELECT *,CASE WHEN title IS NULL THEN 1 ELSE row_number() OVER (
             PARTITION BY started_at,title
             ORDER BY (kcal IS NOT NULL)::integer+(protein_g IS NOT NULL)::integer+(carbs_g IS NOT NULL)::integer+(fat_g IS NOT NULL)::integer DESC,
               provider,source,source_id
           ) END AS duplicate_rank FROM device_rows
         ), entries AS (
           SELECT
             (e.details->'nutrients_per_basis'->>'kcal')::numeric*(e.details->>'amount')::numeric/NULLIF((e.details->>'nutrient_basis_amount')::numeric,0) AS kcal,
             (e.details->'nutrients_per_basis'->>'protein_g')::numeric*(e.details->>'amount')::numeric/NULLIF((e.details->>'nutrient_basis_amount')::numeric,0) AS protein_g,
             (e.details->'nutrients_per_basis'->>'carbs_g')::numeric*(e.details->>'amount')::numeric/NULLIF((e.details->>'nutrient_basis_amount')::numeric,0) AS carbs_g,
             (e.details->'nutrients_per_basis'->>'fat_g')::numeric*(e.details->>'amount')::numeric/NULLIF((e.details->>'nutrient_basis_amount')::numeric,0) AS fat_g,
             (e.details->'nutrients_per_basis' IS NULL) AS unresolved
           FROM avatar_hsm.diary_entry e JOIN avatar_hsm.subject s ON s.id=e.subject_id
           WHERE e.subject_id=$1 AND e.kind='meal' AND e.deleted_at IS NULL AND (e.occurred_at AT TIME ZONE s.timezone)::date=$2
           UNION ALL
           SELECT kcal,protein_g,carbs_g,fat_g,unresolved
           FROM deduplicated_device WHERE duplicate_rank=1
         ), healthkit_totals AS (
           SELECT count(*)::integer AS record_count,
             count(*) FILTER(WHERE w.payload->>'metric'='dietary_energy')::integer AS meal_count,
             sum(CASE WHEN w.payload->>'metric'='dietary_energy' AND d.p->>'unit'='kcal' THEN (d.p->>'value')::numeric END) AS kcal,
             sum(CASE WHEN w.payload->>'metric'='dietary_protein' AND d.p->>'unit'='g' THEN (d.p->>'value')::numeric END) AS protein_g,
             sum(CASE WHEN w.payload->>'metric'='dietary_carbohydrate' AND d.p->>'unit'='g' THEN (d.p->>'value')::numeric END) AS carbs_g,
             sum(CASE WHEN w.payload->>'metric'='dietary_fat' AND d.p->>'unit'='g' THEN (d.p->>'value')::numeric END) AS fat_g
           FROM avatar_hsm.wearable_record w
           JOIN avatar_hsm.subject s ON s.id=w.subject_id
           CROSS JOIN LATERAL (SELECT w.payload->'provider_data' AS p) d
           WHERE w.subject_id=$1 AND w.provider='healthkit' AND w.kind='metric'
             AND w.payload->>'metric'=ANY(ARRAY['dietary_energy','dietary_protein','dietary_carbohydrate','dietary_fat'])
             AND (w.started_at AT TIME ZONE s.timezone)::date=$2
         ), totals AS (
           SELECT (count(e.*)+max(h.meal_count))::integer AS meal_count,
             CASE WHEN sum(e.kcal) IS NULL AND max(h.kcal) IS NULL THEN NULL ELSE (coalesce(sum(e.kcal),0)+coalesce(max(h.kcal),0))::float8 END AS calories_kcal,
             CASE WHEN sum(e.protein_g) IS NULL AND max(h.protein_g) IS NULL THEN NULL ELSE (coalesce(sum(e.protein_g),0)+coalesce(max(h.protein_g),0))::float8 END AS protein_g,
             CASE WHEN sum(e.carbs_g) IS NULL AND max(h.carbs_g) IS NULL THEN NULL ELSE (coalesce(sum(e.carbs_g),0)+coalesce(max(h.carbs_g),0))::float8 END AS carbs_g,
             CASE WHEN sum(e.fat_g) IS NULL AND max(h.fat_g) IS NULL THEN NULL ELSE (coalesce(sum(e.fat_g),0)+coalesce(max(h.fat_g),0))::float8 END AS fat_g,
             count(*) FILTER(WHERE e.unresolved)::integer AS unresolved_meals,
             max(h.record_count) AS healthkit_record_count
           FROM entries e CROSS JOIN healthkit_totals h
         )
         SELECT $1,$2,meal_count,calories_kcal,protein_g,carbs_g,fat_g,unresolved_meals,
           coalesce((SELECT revision+1 FROM avatar_hsm.nutrition_daily_state WHERE subject_id=$1 AND local_day=$2),1),now()
         FROM totals WHERE meal_count>0 OR healthkit_record_count>0
         ON CONFLICT(subject_id,local_day) DO UPDATE SET meal_count=excluded.meal_count,calories_kcal=excluded.calories_kcal,
           protein_g=excluded.protein_g,carbs_g=excluded.carbs_g,fat_g=excluded.fat_g,unresolved_meals=excluded.unresolved_meals,
           revision=excluded.revision,rebuilt_at=now()",
        &[&subject,&day]
    ).await?;
    db.execute("DELETE FROM avatar_hsm.nutrition_daily_state WHERE subject_id=$1 AND local_day=$2 AND NOT EXISTS(SELECT 1 FROM avatar_hsm.diary_entry e JOIN avatar_hsm.subject s ON s.id=e.subject_id WHERE e.subject_id=$1 AND e.kind='meal' AND e.deleted_at IS NULL AND (e.occurred_at AT TIME ZONE s.timezone)::date=$2) AND NOT EXISTS(SELECT 1 FROM avatar_hsm.wearable_record w JOIN avatar_hsm.subject s ON s.id=w.subject_id WHERE w.subject_id=$1 AND w.kind='metric' AND ((w.provider IN ('health_connect','samsung_health') AND w.payload->>'metric'='nutrition') OR (w.provider='healthkit' AND w.payload->>'metric'=ANY(ARRAY['dietary_energy','dietary_protein','dietary_carbohydrate','dietary_fat']))) AND (w.started_at AT TIME ZONE s.timezone)::date=$2)",&[&subject,&day]).await?;
    Ok(())
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DiaryChange {
    pub id: String,
    revision: i64,
    entry: Option<Entry>,
    #[serde(default)]
    occurred_at: Option<DateTime<Utc>>,
}

pub async fn change(pool: &Pool, change: DiaryChange) -> Result<Value> {
    change_for(pool, SUBJECT, change).await
}

pub async fn change_for(pool: &Pool, subject_id: &str, change: DiaryChange) -> Result<Value> {
    let mut db = pool.get().await?;
    let tx = db.transaction().await?;
    ensure!(change.revision > 0, "invalid diary revision");
    let id = &change.id;
    let revision = change.revision;
    let occurred_at = change.occurred_at;
    let replacement = change.entry;
    let replacement_kind = replacement.as_ref().map(Entry::validate).transpose()?;
    let replacement_details = replacement.as_ref().map(serde_json::to_value).transpose()?;
    let old_day: Option<chrono::NaiveDate> = tx.query_opt(
        "SELECT (occurred_at AT TIME ZONE (SELECT timezone FROM avatar_hsm.subject WHERE id=$1))::date FROM avatar_hsm.diary_entry WHERE subject_id=$1 AND id=$2 AND kind='meal' AND deleted_at IS NULL",
        &[&subject_id,&id]
    ).await?.map(|row| row.get(0));
    let changed = if let (Some(kind), Some(details)) =
        (replacement_kind, replacement_details.as_ref())
    {
        tx.query_opt(
            "UPDATE avatar_hsm.diary_entry SET details = $4, revision = revision + 1,
             occurred_at = COALESCE($6, occurred_at)
             WHERE subject_id = $1 AND id = $2 AND revision = $3 AND kind = $5 AND deleted_at IS NULL
             RETURNING id, details, recorded_at, revision",
            &[&subject_id, &id, &revision, &details, &kind, &occurred_at],
        ).await?.as_ref().map(|row| record(row, subject_id))
    } else {
        tx.query_opt(
            "UPDATE avatar_hsm.diary_entry SET deleted_at = now(), revision = revision + 1, details = '{}'::jsonb
             WHERE subject_id = $1 AND id = $2 AND revision = $3 AND deleted_at IS NULL
             RETURNING revision",
            &[&subject_id, &id, &revision],
        ).await?.map(|row| json!({"id": id, "deleted":true, "revision": row.get::<_, i64>(0)}))
    };
    let (changed, replayed) = if changed.is_some() {
        (changed, false)
    } else {
        let current = tx
            .query_opt(
                "SELECT id, details, recorded_at, revision, kind, deleted_at, occurred_at
             FROM avatar_hsm.diary_entry WHERE subject_id=$1 AND id=$2",
                &[&subject_id, &id],
            )
            .await?;
        let replay = current.and_then(|row| {
            let current_revision: i64 = row.get(3);
            let deleted_at: Option<DateTime<Utc>> = row.get(5);
            if revision.checked_add(1) != Some(current_revision) {
                return None;
            }
            if replacement.is_none() && deleted_at.is_some() {
                return Some(json!({"id":id,"deleted":true,"revision":current_revision}));
            }
            let (Some(kind), Some(details)) = (replacement_kind, replacement_details.as_ref())
            else {
                return None;
            };
            let current_occurred_at: DateTime<Utc> = row.get(6);
            if deleted_at.is_none()
                && row.get::<_, String>(4) == kind
                && row.get::<_, Value>(1) == *details
                && occurred_at.is_none_or(|expected| expected == current_occurred_at)
            {
                Some(record(&row, subject_id))
            } else {
                None
            }
        });
        (replay, true)
    };
    if changed.is_some() && !replayed {
        if let Some(day) = old_day {
            rebuild_nutrition_day(&*tx, subject_id, day).await?;
        }
        if let Some(day) = tx.query_opt("SELECT (occurred_at AT TIME ZONE (SELECT timezone FROM avatar_hsm.subject WHERE id=$1))::date FROM avatar_hsm.diary_entry WHERE subject_id=$1 AND id=$2 AND kind='meal' AND deleted_at IS NULL", &[&subject_id,&id]).await?.map(|row| row.get(0)) {
            rebuild_nutrition_day(&*tx, subject_id, day).await?;
        }
    }
    let result = changed.unwrap_or_else(|| json!({"status":"conflict"}));
    if !replayed && result["status"] != "conflict" {
        let current_revision = if result["deleted"] == true {
            result["revision"].as_i64().unwrap_or(revision + 1)
        } else {
            revision + 1
        };
        let happened_at: DateTime<Utc> = occurred_at.unwrap_or_else(Utc::now);
        crate::state_machine::append(&tx, subject_id, &crate::state_machine::StateEvent {
            event_id: format!("diary-entry:{id}:{current_revision}"),
            event_type: "confirmed_diary_fact_recorded".into(),
            event_version: 1,
            actor_id: "service:hsm-diary".into(),
            occurred_at: happened_at,
            source_ref: Some(format!("diary-entry:{id}")),
            correlation_id: None,
            causation_id: None,
            consent_version: None,
            rule_version: None,
            payload: json!({"revision": current_revision, "deleted": result["deleted"] == true}),
        }, None).await?;
    }
    tx.commit().await?;
    Ok(result)
}

pub async fn read(db: &Client, query: Query) -> Result<Value> {
    read_for(db, SUBJECT, query).await
}

pub async fn read_for(db: &Client, subject_id: &str, query: Query) -> Result<Value> {
    ensure!(
        query.start < query.end && query.end - query.start <= chrono::Duration::days(90),
        "select a range of up to 90 days"
    );
    ensure!(
        matches!(
            query.kind.as_deref(),
            None | Some("meal" | "water" | "strength")
        ),
        "unknown diary kind"
    );
    let rows = db
        .query(
            "SELECT id, details, recorded_at, revision FROM avatar_hsm.diary_entry
         WHERE subject_id = $1 AND deleted_at IS NULL AND recorded_at >= $2 AND recorded_at < $3
           AND ($4::text IS NULL OR kind = $4)
         ORDER BY recorded_at DESC, id LIMIT 101",
            &[&subject_id, &query.start, &query.end, &query.kind],
        )
        .await?;
    Ok(
        json!({ "entries": rows.iter().take(100).map(|row| record(row, subject_id)).collect::<Vec<_>>(), "truncated": rows.len() > 100, "synthetic": subject_id == SUBJECT }),
    )
}

pub async fn water_today(db: &Client) -> Result<Value> {
    water_today_for(db, SUBJECT).await
}

pub async fn water_today_for(db: &Client, subject_id: &str) -> Result<Value> {
    let row = db
        .query_one(
            "WITH today AS (
            SELECT id, timezone, (now() AT TIME ZONE timezone)::date AS local_date
            FROM avatar_hsm.subject WHERE id = $1
         )
         SELECT t.local_date::text, t.timezone, count(e.id),
                coalesce(sum((e.details->>'ml')::bigint), 0)::bigint
         FROM today t
         LEFT JOIN avatar_hsm.diary_entry e
           ON e.subject_id = t.id AND e.kind = 'water' AND e.deleted_at IS NULL
          AND e.recorded_at >= (t.local_date::timestamp AT TIME ZONE t.timezone)
          AND e.recorded_at < ((t.local_date + 1)::timestamp AT TIME ZONE t.timezone)
          AND e.recorded_at <= now()
         GROUP BY t.local_date, t.timezone",
            &[&subject_id],
        )
        .await?;
    Ok(json!({
        "local_date": row.get::<_, String>(0),
        "timezone": row.get::<_, String>(1),
        "entry_count": row.get::<_, i64>(2),
        "total_ml": row.get::<_, i64>(3),
        "synthetic": subject_id == SUBJECT,
    }))
}

fn record(row: &tokio_postgres::Row, subject_id: &str) -> Value {
    json!({
        "id": row.get::<_, String>(0),
        "entry": row.get::<_, Value>(1),
        "recorded_at": row.get::<_, DateTime<Utc>>(2),
        "revision": row.get::<_, i64>(3),
        "synthetic": subject_id == SUBJECT,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn strength_entry_preserves_actual_and_target_set_counts() -> Result<()> {
        let entry: Entry = serde_json::from_value(json!({
            "kind": "strength",
            "exercise": "Bench press",
            "sets": 2,
            "target_sets": 3,
            "reps": 10,
            "kg": 40
        }))?;
        assert_eq!(entry.validate()?, "strength");
        assert_eq!(serde_json::to_value(&entry)?["sets"], 2);
        assert_eq!(serde_json::to_value(&entry)?["target_sets"], 3);
        Ok(())
    }

    #[test]
    fn strength_entry_rejects_more_completed_sets_than_target() -> Result<()> {
        let entry: Entry = serde_json::from_value(json!({
            "kind": "strength",
            "exercise": "Bench press",
            "sets": 4,
            "target_sets": 3,
            "reps": 10,
            "kg": 40
        }))?;
        assert!(entry.validate().is_err());
        Ok(())
    }

    #[tokio::test]
    #[ignore = "requires an isolated PostgreSQL database in AVATAR_HSM_TEST_DATABASE_URL"]
    async fn approved_entries_are_idempotent_and_readable() -> Result<()> {
        let db_url = std::env::var("AVATAR_HSM_TEST_DATABASE_URL")?;
        let pool = crate::store::connect(&db_url).await?;
        let db = pool.get().await?;
        let read_token = "fixture-read";
        let control_token = "fixture-control";
        let app = std::sync::Arc::new(crate::mcp::App {
            db: pool,
            read_token: read_token.into(),
            control_token: control_token.into(),
            rules: crate::acute::rules()?,
        });
        let router = axum::Router::new()
            .route("/mcp/{surface}", axum::routing::post(crate::mcp::handle))
            .with_state(app);
        let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0)).await?;
        let address = listener.local_addr()?;
        let server = tokio::spawn(async move { axum::serve(listener, router).await });
        let http = reqwest::Client::new();
        let call = |surface: &str, bearer: &str, name: &str, arguments: Value| {
            http.post(format!("http://{address}/mcp/{surface}"))
                .bearer_auth(bearer)
                .json(&json!({
                    "jsonrpc": "2.0", "id": 1, "method": "tools/call",
                    "params": { "name": name, "arguments": arguments }
                }))
                .send()
        };
        let start: DateTime<Utc> = db.query_one("SELECT now()", &[]).await?.get(0);
        db.execute(
            "INSERT INTO avatar_catalog.reference_item(source,source_id,payload,license_id,license_uri,source_uri,retrieved_at)
            VALUES('mfds','RICE-TEST',$1,'data.go.kr-no-restriction','https://www.data.go.kr/ugs/selectPortalPolicyView.do',
             'https://www.data.go.kr/data/15127578/openapi.do',now()) ON CONFLICT DO NOTHING",
            &[&json!({"source":"mfds","source_id":"RICE-TEST","name":"현미밥","basis_amount":100,"basis_unit":"g",
                "nutrients_per_basis":{"kcal":130,"protein_g":2.6,"carbs_g":28.6,"fat_g":1.0}})]
        ).await?;
        let empty: Value = call("read", read_token, "diary_water_today", json!({}))
            .await?
            .json()
            .await?;
        assert_eq!(empty["result"]["structuredContent"]["entry_count"], 0);
        let entries = [
            json!({"kind": "water", "ml": 250}),
            json!({"kind": "meal", "name": "현미밥", "amount": 150, "unit": "g",
                "catalog":{"source":"mfds","source_id":"RICE-TEST"}}),
            json!({"kind": "strength", "exercise": "스쿼트", "sets": 2, "target_sets": 3, "reps": 10, "kg": 40}),
        ];
        for (index, entry) in entries.into_iter().enumerate() {
            let payload = json!({"id": format!("diary-test-{}-{index}", start.timestamp_micros()), "entry": entry});
            let denied: Value = call("read", read_token, "confirm_diary_entry", payload.clone())
                .await?
                .json()
                .await?;
            assert_eq!(denied["result"]["isError"], true);
            let accepted: Value = call(
                "control",
                control_token,
                "confirm_diary_entry",
                payload.clone(),
            )
            .await?
            .json()
            .await?;
            let retried: Value = call("control", control_token, "confirm_diary_entry", payload)
                .await?
                .json()
                .await?;
            assert_eq!(
                accepted["result"]["structuredContent"]["entry"]["kind"],
                entry["kind"]
            );
            assert_eq!(accepted, retried);
        }
        let end: DateTime<Utc> = db
            .query_one("SELECT now() + interval '1 second'", &[])
            .await?
            .get(0);
        let records: Value = call(
            "read",
            read_token,
            "diary_entries",
            json!({"start": start, "end": end}),
        )
        .await?
        .json()
        .await?;
        assert_eq!(
            records["result"]["structuredContent"]["entries"]
                .as_array()
                .map(Vec::len),
            Some(3)
        );
        let nutrition: Value = call(
            "read",
            read_token,
            "nutrition_daily",
            json!({"start":start,"end":end}),
        )
        .await?
        .json()
        .await?;
        let days = nutrition["result"]["structuredContent"]["days"]
            .as_array()
            .unwrap();
        let rice_day = days
            .iter()
            .find(|day| day["confirmed_kcal_subtotal"] == 195.0)
            .expect("150g of the 130kcal/100g MFDS fixture should aggregate to 195kcal");
        assert_eq!(rice_day["meal_count"], 1);
        assert_eq!(rice_day["unresolved_meals"], 0);
        let rice = records["result"]["structuredContent"]["entries"]
            .as_array()
            .unwrap()
            .iter()
            .find(|entry| entry["id"].as_str().is_some_and(|id| id.ends_with("-1")))
            .unwrap();
        let yesterday = start - chrono::Duration::hours(26);
        let move_payload = json!({
            "id":rice["id"],"revision":rice["revision"],
            "occurred_at":yesterday,
            "entry":{"kind":"meal","name":"현미밥","amount":150,"unit":"g",
                "nutrients_per_basis":{"kcal":130,"protein_g":2.6,"carbs_g":28.6,"fat_g":1.0},
                "nutrient_basis_amount":100,"nutrient_basis_unit":"g","source":"mfds","source_id":"RICE-TEST",
                "catalog":{"source":"mfds","source_id":"RICE-TEST"}}
        });
        let moved: Value = call(
            "control",
            control_token,
            "change_diary_entry",
            move_payload.clone(),
        )
        .await?
        .json()
        .await?;
        assert_eq!(moved["result"]["structuredContent"]["revision"], 2);
        let nutrition_revision_after_move: i64 = db
            .query_one(
                "SELECT revision FROM avatar_hsm.nutrition_daily_state
                 WHERE subject_id=$1 AND local_day=(($2::timestamptz) AT TIME ZONE
                    (SELECT timezone FROM avatar_hsm.subject WHERE id=$1))::date",
                &[&SUBJECT, &yesterday],
            )
            .await?
            .get(0);
        let replayed: Value = call("control", control_token, "change_diary_entry", move_payload)
            .await?
            .json()
            .await?;
        assert_eq!(moved, replayed);
        let nutrition_revision_after_replay: i64 = db
            .query_one(
                "SELECT revision FROM avatar_hsm.nutrition_daily_state
                 WHERE subject_id=$1 AND local_day=(($2::timestamptz) AT TIME ZONE
                    (SELECT timezone FROM avatar_hsm.subject WHERE id=$1))::date",
                &[&SUBJECT, &yesterday],
            )
            .await?
            .get(0);
        assert_eq!(
            nutrition_revision_after_move,
            nutrition_revision_after_replay
        );
        let today_nutrition: Value = call(
            "read",
            read_token,
            "nutrition_daily",
            json!({
                "start":start,"end":end
            }),
        )
        .await?
        .json()
        .await?;
        assert!(today_nutrition["result"]["structuredContent"]["days"]
            .as_array()
            .unwrap()
            .is_empty());
        let yesterday_nutrition: Value = call(
            "read",
            read_token,
            "nutrition_daily",
            json!({
                "start":start-chrono::Duration::hours(48),"end":start-chrono::Duration::hours(24)
            }),
        )
        .await?
        .json()
        .await?;
        assert!(yesterday_nutrition["result"]["structuredContent"]["days"]
            .as_array()
            .unwrap()
            .iter()
            .any(|day| day["confirmed_kcal_subtotal"] == 195.0));
        let water: Value = call(
            "read",
            read_token,
            "diary_entries",
            json!({"start": start, "end": end, "kind": "water"}),
        )
        .await?
        .json()
        .await?;
        assert_eq!(
            water["result"]["structuredContent"]["entries"][0]["entry"]["ml"],
            250
        );
        db.execute(
            "INSERT INTO avatar_hsm.diary_entry (subject_id, id, kind, details, recorded_at)
             SELECT id, 'previous-local-day', 'water', '{\"kind\":\"water\",\"ml\":800}',
                    (date_trunc('day', now() AT TIME ZONE timezone) AT TIME ZONE timezone) - interval '1 second'
             FROM avatar_hsm.subject WHERE id = $1",
            &[&SUBJECT],
        ).await?;
        let yesterday_excluded: Value = call("read", read_token, "diary_water_today", json!({}))
            .await?
            .json()
            .await?;
        assert_eq!(
            yesterday_excluded["result"]["structuredContent"]["entry_count"],
            1
        );
        assert_eq!(
            yesterday_excluded["result"]["structuredContent"]["total_ml"],
            250
        );
        assert_eq!(
            yesterday_excluded["result"]["structuredContent"]["timezone"],
            "Asia/Seoul"
        );
        let invalid = json!({"id": "invalid-diary-test", "entry": {"kind": "water", "ml": 0}});
        let invalid: Value = call("control", control_token, "confirm_diary_entry", invalid)
            .await?
            .json()
            .await?;
        assert_eq!(invalid["result"]["isError"], true);
        server.abort();
        Ok(())
    }
}
