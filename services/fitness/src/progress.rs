use std::collections::{BTreeMap, HashMap};

use anyhow::{bail, Context, Result};
use chrono::{DateTime, Datelike, Duration, NaiveDate, NaiveTime, TimeZone, Utc};
use chrono_tz::Tz;
use serde_json::{json, Value};
use tokio_postgres::Row;

use crate::store::FitnessStore;

const MAX_PERIOD_SETS: usize = 1000;

#[derive(Clone)]
struct ProgressSet {
    id: String,
    workout_id: String,
    position: i32,
    reps: i32,
    load_kg: Option<f64>,
    performed_at: DateTime<Utc>,
    source: String,
}

impl ProgressSet {
    fn from_row(row: &Row) -> Self {
        Self {
            id: row.get(0),
            workout_id: row.get(1),
            position: row.get(2),
            reps: row.get(3),
            load_kg: row.get(4),
            performed_at: row.get(7),
            source: row.get(8),
        }
    }
}

impl FitnessStore {
    pub async fn training_progress(
        &self,
        owner_id: &str,
        exercise_id: &str,
        variant_id: &str,
        timezone: &str,
        start_date: Option<NaiveDate>,
        end_date: Option<NaiveDate>,
        include_lifetime: bool,
    ) -> Result<Value> {
        validate_exercise_variant(exercise_id, variant_id)?;
        let zone: Tz = timezone.parse().context("invalid IANA progress timezone")?;
        let today = Utc::now().with_timezone(&zone).date_naive();
        let end = end_date.unwrap_or(today);
        let start = start_date.unwrap_or(end - Duration::days(29));
        if start > end
            || !(1970..9999).contains(&start.year())
            || !(1970..9999).contains(&end.year())
            || (end - start).num_days() >= 31
        {
            bail!("progress range must cover 1 to 31 local days");
        }
        let from = local_day_boundary(zone, start)?;
        let until = local_day_boundary(zone, end.succ_opt().context("date out of range")?)?;
        let client = self.pool.get().await?;
        let db_rows = client.query(
            "SELECT s.id,s.workout_id,s.position,s.reps,s.load_kg,s.load_mode,s.set_type,w.performed_at,w.source \
             FROM fitness.workout_sets s JOIN fitness.workout_sessions w ON w.id=s.workout_id \
             WHERE w.owner_id=$1 AND s.exercise_id=$2 AND s.variant_id=$3 AND s.deleted_at IS NULL \
               AND w.performed_at >= $4 AND w.performed_at < $5 AND s.set_type='work' AND s.reps>0 AND \
               (($3='barbell' AND s.load_kg>0 AND s.load_mode='barbell_total') OR ($3='bodyweight' AND s.load_kg IS NULL)) \
             ORDER BY w.performed_at,s.position,s.id LIMIT 1001",
            &[&owner_id,&exercise_id,&variant_id,&from,&until],
        ).await?;
        if db_rows.len() > MAX_PERIOD_SETS {
            bail!("progress range contains too many sets");
        }
        let rows: Vec<ProgressSet> = db_rows.iter().map(ProgressSet::from_row).collect();
        let loaded = variant_id == "barbell";
        let total_count: i64 = client.query_one(
            "SELECT count(*) FROM fitness.workout_sets s JOIN fitness.workout_sessions w ON w.id=s.workout_id \
             WHERE w.owner_id=$1 AND s.exercise_id=$2 AND s.variant_id=$3 AND s.deleted_at IS NULL AND w.performed_at >= $4 AND w.performed_at < $5",
            &[&owner_id,&exercise_id,&variant_id,&from,&until],
        ).await?.get(0);
        let eligible: Vec<&ProgressSet> = rows.iter().collect();
        let lifetime_best = if include_lifetime {
            client.query_opt(
                "SELECT s.id,s.workout_id,s.position,s.reps,s.load_kg,s.load_mode,s.set_type,w.performed_at,w.source \
                 FROM fitness.workout_sets s JOIN fitness.workout_sessions w ON w.id=s.workout_id \
                 WHERE w.owner_id=$1 AND s.exercise_id=$2 AND s.variant_id=$3 AND s.deleted_at IS NULL \
                   AND s.set_type='work' AND s.reps>0 AND \
                   (($3='barbell' AND s.load_kg>0 AND s.load_mode='barbell_total') OR ($3='bodyweight' AND s.load_kg IS NULL)) \
                 ORDER BY CASE WHEN $3='barbell' THEN s.load_kg END DESC,s.reps DESC,w.performed_at,s.position,s.id LIMIT 1",
                &[&owner_id,&exercise_id,&variant_id],
            ).await?.map(|row|ProgressSet::from_row(&row))
        } else {
            None
        };
        let lifetime_first = if include_lifetime {
            client.query_opt(
                "SELECT min(w.performed_at) FROM fitness.workout_sets s JOIN fitness.workout_sessions w ON w.id=s.workout_id \
                 WHERE w.owner_id=$1 AND s.exercise_id=$2 AND s.variant_id=$3 AND s.deleted_at IS NULL \
                   AND s.set_type='work' AND s.reps>0 AND \
                   (($3='barbell' AND s.load_kg>0 AND s.load_mode='barbell_total') OR ($3='bodyweight' AND s.load_kg IS NULL))",
                &[&owner_id,&exercise_id,&variant_id],
            ).await?.and_then(|row|row.get::<_,Option<DateTime<Utc>>>(0))
        } else {
            None
        };

        let mut per_day: BTreeMap<NaiveDate, Vec<&ProgressSet>> = BTreeMap::new();
        let mut source_counts: BTreeMap<String, usize> = BTreeMap::new();
        let mut workouts: HashMap<String, Vec<&ProgressSet>> = HashMap::new();
        for row in &eligible {
            let day = row.performed_at.with_timezone(&zone).date_naive();
            per_day.entry(day).or_default().push(row);
            *source_counts.entry(row.source.clone()).or_default() += 1;
            workouts
                .entry(row.workout_id.clone())
                .or_default()
                .push(row);
        }
        let mut days = Vec::new();
        for offset in 0..=(end - start).num_days() {
            let day = start + Duration::days(offset);
            let sets = per_day.get(&day).cloned().unwrap_or_default();
            days.push(json!({"date":day,"sets":sets.len(),
                "reps":if sets.is_empty(){None}else{Some(sets.iter().map(|r|r.reps as i64).sum::<i64>())},
                "volume_kg_reps":volume(&sets,loaded)}));
        }
        let period_best = best_row(&eligible, loaded).map(best_json);
        let lifetime_best = lifetime_best.as_ref().map(best_json);
        let mut recent: Vec<_> = workouts.into_iter().collect();
        recent.sort_by(|(id_a, a), (id_b, b)| {
            b[0].performed_at
                .cmp(&a[0].performed_at)
                .then_with(|| id_b.cmp(id_a))
        });
        let latest = recent
            .first()
            .map(|(id, rows)| recent_json(id, rows, loaded));
        let previous = recent
            .get(1)
            .map(|(id, rows)| recent_json(id, rows, loaded));
        Ok(json!({
            "schema_version":"1","status":if eligible.is_empty(){"empty"}else{"ready"},
            "exercise_id":exercise_id,"variant_id":variant_id,
            "load_mode":if loaded{"barbell_total"}else{"unloaded_bodyweight"},
            "best_metric":if loaded{"top_logged_load_kg.v1"}else{"top_logged_reps.v1"},
            "volume_metric":if loaded{Some("logged_work_volume_kg_reps.v1")}else{None},
            "start_date":start,"end_date":end,"timezone":timezone,"days":days,
            "eligible_set_count":eligible.len(),"excluded_set_count":total_count as usize-eligible.len(),
            "source_counts":source_counts,"volume_kg_reps":volume(&eligible,loaded),
            "lifetime_requested":include_lifetime,
            "lifetime_first_date":lifetime_first.map(|at|at.with_timezone(&zone).date_naive()),
            "period_best":period_best,"lifetime_best":lifetime_best,
            "latest":latest,"previous":previous
        }))
    }
}

fn local_day_boundary(zone: Tz, date: NaiveDate) -> Result<DateTime<Utc>> {
    let mut time = NaiveTime::MIN;
    for _ in 0..=180 {
        if let Some(local) = zone.from_local_datetime(&date.and_time(time)).earliest() {
            return Ok(local.with_timezone(&Utc));
        }
        time += Duration::minutes(1);
    }
    bail!("local date has no valid boundary")
}

fn validate_exercise_variant(exercise: &str, variant: &str) -> Result<()> {
    let valid = match exercise {
        "squat" => matches!(variant, "bodyweight" | "barbell"),
        "bench_press" | "deadlift" => variant == "barbell",
        "push_up" => variant == "bodyweight",
        _ => false,
    };
    if !valid {
        bail!("unsupported exercise and variant pair");
    }
    Ok(())
}

fn volume(rows: &[&ProgressSet], loaded: bool) -> Option<f64> {
    if !loaded || rows.is_empty() {
        return None;
    }
    let sum: f64 = rows
        .iter()
        .map(|row| row.load_kg.unwrap_or(0.0) * row.reps as f64)
        .sum();
    Some((sum * 100.0).round() / 100.0)
}

fn best_row<'a>(rows: &[&'a ProgressSet], loaded: bool) -> Option<&'a ProgressSet> {
    rows.iter().copied().max_by(|a, b| {
        let load = if loaded {
            a.load_kg
                .partial_cmp(&b.load_kg)
                .unwrap_or(std::cmp::Ordering::Equal)
        } else {
            std::cmp::Ordering::Equal
        };
        load.then_with(|| a.reps.cmp(&b.reps))
            .then_with(|| b.performed_at.cmp(&a.performed_at))
            .then_with(|| b.position.cmp(&a.position))
            .then_with(|| b.id.cmp(&a.id))
    })
}

fn best_json(row: &ProgressSet) -> Value {
    json!({"set_id":row.id,"workout_id":row.workout_id,"performed_at":row.performed_at,
        "load_kg":row.load_kg,"reps":row.reps,"source":row.source})
}

fn recent_json(id: &str, rows: &[&ProgressSet], loaded: bool) -> Value {
    let best = best_row(rows, loaded).map(best_json);
    json!({"workout_id":id,"performed_at":rows[0].performed_at,"sets":rows.len(),
        "reps":rows.iter().map(|row|row.reps as i64).sum::<i64>(),
        "volume_kg_reps":volume(rows,loaded),"best":best})
}
