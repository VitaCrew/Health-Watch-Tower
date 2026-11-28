use std::{collections::BTreeMap, time::Duration};

use anyhow::{ensure, Result};
use chrono::DateTime;
use deadpool_postgres::Pool;
use serde::Deserialize;
use serde_json::{json, Map, Value};
use tokio_postgres::Transaction;

use crate::{garmin_push::ACTIVITY_EXPORT, wearables::Record};

#[cfg(test)]
#[path = "garmin_workouts_tests.rs"]
mod tests;

fn points(record: &Record, samples: &[Value]) -> Value {
    let mut points: BTreeMap<_, Map<String, Value>> = BTreeMap::new();
    for item in samples {
        let Some(at) = item["startTimeInSeconds"]
            .as_i64()
            .and_then(|seconds| DateTime::from_timestamp(seconds, 0))
            .filter(|at| {
                *at >= record.started_at
                    && if record.ended_at > record.started_at {
                        *at <= record.ended_at
                    } else {
                        (*at - record.started_at) <= chrono::Duration::days(7)
                    }
            })
        else {
            continue;
        };
        let mut data = Map::new();
        for (field, name, bounds) in [
            ("heartRate", "heart_rate_bpm", 1.0..=350.0),
            ("speedMetersPerSecond", "speed_mps", 0.0..=f64::MAX),
            ("stepsPerMinute", "cadence_steps_per_min", 0.0..=f64::MAX),
            ("powerInWatts", "power_w", 0.0..=f64::MAX),
            ("elevationInMeters", "elevation_m", -f64::MAX..=f64::MAX),
            (
                "airTemperatureCelcius",
                "temperature_c",
                -f64::MAX..=f64::MAX,
            ),
        ] {
            if let Some(value) = item[field].as_f64().filter(|value| bounds.contains(value)) {
                data.insert(name.to_owned(), json!(value));
            }
        }
        if let Some((lat, lon)) = item["latitudeInDegree"]
            .as_f64()
            .zip(item["longitudeInDegree"].as_f64())
            .filter(|(lat, lon)| (-90.0..=90.0).contains(lat) && (-180.0..=180.0).contains(lon))
        {
            data.insert("latitude_deg".to_owned(), json!(lat));
            data.insert("longitude_deg".to_owned(), json!(lon));
        }
        if !data.is_empty() {
            points.entry(at).or_default().extend(data);
        }
    }
    json!(points
        .into_iter()
        .map(|(at, mut data)| {
            data.insert("timestamp".to_owned(), json!(at));
            data
        })
        .collect::<Vec<_>>())
}

pub async fn store(tx: &Transaction<'_>, subject: &str, record: &Record) -> Result<()> {
    let Some(samples) = record.payload["provider_data"]["samples"].as_array() else {
        return Ok(());
    };
    let points = points(record, samples);
    tx.execute(
        "INSERT INTO avatar_hsm.garmin_workout_samples AS previous(subject_id,source,source_id,modified_at,points)
         SELECT r.subject_id,r.source,r.source_id,r.modified_at,$5
         FROM avatar_hsm.wearable_record r
         JOIN avatar_hsm.wearable_oauth_connection c USING(subject_id,provider)
         WHERE r.subject_id=$1 AND r.provider='garmin' AND r.kind='metric' AND r.source=$2
           AND r.source_id=$3 AND r.modified_at=$4 AND (c.garmin_permissions & $6::smallint)<>0
         ON CONFLICT (subject_id,source,source_id) DO UPDATE
         SET modified_at=excluded.modified_at,points=excluded.points
         WHERE previous.modified_at<=excluded.modified_at",
        &[&subject,&record.source,&record.source_id,&record.modified_at,&points,&ACTIVITY_EXPORT],
    ).await?;
    Ok(())
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Query {
    source: String,
    source_id: String,
    #[serde(default)]
    offset: i64,
    limit: Option<i64>,
}

pub async fn query(pool: &Pool, subject: &str, query: Query) -> Result<Value> {
    let limit = query.limit.unwrap_or(100);
    ensure!(
        [&query.source, &query.source_id]
            .into_iter()
            .all(|value| !value.trim().is_empty() && value.len() <= 256)
            && (0..=i64::from(i32::MAX)).contains(&query.offset)
            && (1..=100).contains(&limit),
        "invalid Garmin workout sample query"
    );
    let parent = format!("activityDetails:{}", query.source_id);
    let db = pool.get().await?;
    let row = db.query_opt(
        "SELECT jsonb_array_length(s.points),
            COALESCE((SELECT jsonb_agg(value ORDER BY ord) FROM jsonb_array_elements(s.points) WITH ORDINALITY p(value,ord)
                WHERE ord>$4::bigint AND ord<=$4::bigint+$5::bigint),'[]'::jsonb)
         FROM avatar_hsm.garmin_workout_samples s
         JOIN avatar_hsm.wearable_oauth_connection c USING(subject_id,provider)
         JOIN avatar_hsm.device_connection d USING(subject_id,provider)
         WHERE s.subject_id=$1 AND s.source=$2 AND s.source_id=$3 AND d.state='synced'
           AND (c.garmin_permissions & $6::smallint)<>0",
        &[&subject,&query.source,&parent,&query.offset,&limit,&ACTIVITY_EXPORT],
    ).await?;
    let Some(row) = row else {
        return Ok(json!({"state":"not_found","records":[]}));
    };
    let total = i64::from(row.get::<_, i32>(0));
    let next = query.offset + limit;
    Ok(
        json!({"state":if total == 0 { "no_records" } else { "ready" },
        "provider":"garmin","source":query.source,"source_id":query.source_id,
        "records":row.get::<_,Value>(1),"total":total,"next_offset":(next<total).then_some(next)}),
    )
}

async fn rebuild_one(pool: &Pool) -> Result<bool> {
    let mut db = pool.get().await?;
    let tx = db.transaction().await?;
    let row = tx.query_opt(
        "SELECT r.subject_id,r.source,r.source_id,r.started_at,r.ended_at,r.modified_at,r.payload
         FROM avatar_hsm.wearable_record r JOIN avatar_hsm.wearable_oauth_connection c USING(subject_id,provider)
         WHERE r.provider='garmin' AND r.kind='metric' AND r.payload->>'metric'='activityDetails'
           AND jsonb_typeof(r.payload->'provider_data'->'samples')='array'
           AND (c.garmin_permissions & $1::smallint)<>0
           AND NOT EXISTS (SELECT 1 FROM avatar_hsm.garmin_workout_samples s
                           WHERE s.subject_id=r.subject_id AND s.source=r.source AND s.source_id=r.source_id)
         ORDER BY r.subject_id,r.source,r.source_id LIMIT 1",
        &[&ACTIVITY_EXPORT],
    ).await?;
    let Some(row) = row else {
        return Ok(false);
    };
    let subject: String = row.get(0);
    tx.query_one(
        "SELECT pg_advisory_xact_lock(hashtext($1),hashtext('garmin'))",
        &[&subject],
    )
    .await?;
    let record = Record {
        kind: "metric".to_owned(),
        source: row.get(1),
        source_id: row.get(2),
        started_at: row.get(3),
        ended_at: row.get(4),
        modified_at: row.get(5),
        payload: row.get(6),
    };
    store(&tx, &subject, &record).await?;
    tx.commit().await?;
    Ok(true)
}

pub async fn rebuild_existing(pool: Pool) {
    loop {
        match rebuild_one(&pool).await {
            Ok(true) => continue,
            Ok(false) | Err(_) => tokio::time::sleep(Duration::from_secs(60)).await,
        }
    }
}
