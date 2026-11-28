use std::collections::BTreeMap;

use anyhow::Result;
use chrono::{DateTime, Duration, FixedOffset, NaiveDate, Utc};
use deadpool_postgres::Pool;
use serde_json::{json, Map, Value};
use tokio_postgres::Transaction;

use crate::wearables::{self, Record};

const PROVIDER: &str = "garmin";

#[cfg(test)]
#[path = "garmin_metrics_tests.rs"]
mod tests;

fn summary(record: &Record) -> &Value {
    let raw = &record.payload["provider_data"];
    raw.get("summary").unwrap_or(raw)
}

fn day(record: &Record, detail: &Value) -> Option<(NaiveDate, String)> {
    let offset = detail["startTimeOffsetInSeconds"]
        .as_i64()
        .and_then(|value| i32::try_from(value).ok())
        .and_then(FixedOffset::east_opt);
    let provider_day = detail["calendarDate"]
        .as_str()
        .and_then(|day| day.parse().ok())
        .or_else(|| offset.map(|offset| record.started_at.with_timezone(&offset).date_naive()))?;
    Some((
        provider_day,
        offset.map_or_else(
            || "provider_local".to_owned(),
            |offset| format!("UTC{offset}"),
        ),
    ))
}

fn steps_count(detail: &Value) -> Option<u64> {
    detail["steps"].as_u64().filter(|count| *count <= 1_000_000)
}

fn activity_stats(detail: &Value) -> Option<Value> {
    let mut stats = Map::new();
    for (field, key) in [
        ("distanceInMeters", "distance_meters"),
        ("activeKilocalories", "active_energy_kcal"),
        ("bmrKilocalories", "basal_energy_kcal"),
        ("floorsClimbed", "floors_climbed"),
        ("activeTimeInSeconds", "active_seconds"),
        (
            "moderateIntensityDurationInSeconds",
            "moderate_intensity_seconds",
        ),
        (
            "vigorousIntensityDurationInSeconds",
            "vigorous_intensity_seconds",
        ),
        ("restingHeartRateInBeatsPerMinute", "resting_heart_rate_bpm"),
    ] {
        let value = &detail[field];
        if value.as_f64().is_some_and(|num| {
            num.is_finite()
                && num >= 0.0
                && (key != "resting_heart_rate_bpm" || (1.0..=350.0).contains(&num))
        }) {
            stats.insert(key.to_owned(), value.clone());
        }
    }
    if let Some(total) = stats
        .get("active_energy_kcal")
        .and_then(Value::as_f64)
        .zip(stats.get("basal_energy_kcal").and_then(Value::as_f64))
        .map(|(active, basal)| active + basal)
        .filter(|total| total.is_finite())
    {
        stats.insert("total_energy_kcal".to_owned(), json!(total));
    }
    (!stats.is_empty()).then_some(Value::Object(stats))
}

fn sample(
    record: &Record,
    kind: &str,
    id: String,
    started_at: DateTime<Utc>,
    ended_at: DateTime<Utc>,
    payload: Value,
) -> Record {
    Record {
        kind: kind.to_owned(),
        source: record.source.clone(),
        source_id: id,
        started_at,
        ended_at,
        modified_at: record.modified_at,
        payload,
    }
}

pub(crate) fn cycle_updated_at(detail: &Value, received: DateTime<Utc>) -> Option<DateTime<Utc>> {
    detail["lastUpdatedTimeInSeconds"]
        .as_i64()
        .filter(|seconds| *seconds > 0)
        .and_then(|seconds| DateTime::from_timestamp(seconds, 0))
        .filter(|updated| *updated <= received + Duration::minutes(5))
}

fn menstrual_cycle(record: &Record, detail: &Value) -> Option<Record> {
    let start: NaiveDate = detail["periodStartDate"].as_str()?.parse().ok()?;
    let at = start.and_hms_opt(0, 0, 0)?.and_utc();
    let reported_day = detail["calendarDate"]
        .as_str()
        .and_then(|day| day.parse::<NaiveDate>().ok());
    let actual_length = detail["cycleLength"]
        .as_i64()
        .filter(|length| (1..=366).contains(length));
    let observed_end = actual_length
        .filter(|_| detail["isPredictedCycle"].as_bool() == Some(false))
        .and_then(|length| at.checked_add_signed(Duration::days(length)))
        .filter(|end| *end <= record.started_at.max(at));
    let updated_at = cycle_updated_at(detail, record.modified_at);
    let mut projected = sample(
        record,
        "metric",
        format!("cycle:{}", record.source_id),
        at,
        observed_end.unwrap_or(at),
        json!({
            "metric":"menstrual_cycle",
            "time_precision":"provider_calendar_date",
            "provider_data":{
                "period_start_date": start,
                "reported_date": reported_day,
                "day_in_cycle": detail["dayInCycle"].as_i64(),
                "current_phase": detail["currentPhase"].as_i64(),
                "current_phase_type": detail["currentPhaseType"].as_str(),
                "length_of_current_phase_days": detail["lengthOfCurrentPhase"].as_i64(),
                "days_until_next_phase": detail["daysUntilNextPhase"].as_i64(),
                "cycle_length_days": detail["cycleLength"].as_i64(),
                "predicted_cycle_length_days": detail["predictedCycleLength"].as_i64(),
                "is_predicted_cycle": detail["isPredictedCycle"].as_bool(),
                "has_specified_cycle_length": detail["hasSpecifiedCycleLength"].as_bool(),
                "has_specified_period_length": detail["hasSpecifiedPeriodLength"].as_bool(),
                "period_length_days": detail["periodLength"].as_i64(),
                "fertile_window_start": detail["fertileWindowStart"],
                "fertile_window_length_days": detail["lengthOfFertileWindow"].as_i64(),
                "last_updated_at": updated_at,
            }
        }),
    );
    projected.modified_at = updated_at.unwrap_or(record.modified_at);
    Some(projected)
}

fn move_iq(record: &Record, detail: &Value) -> Option<Record> {
    let id = record.source_id.strip_prefix("moveIQActivities:")?;
    let start = detail["startTimeInSeconds"].as_i64()?;
    if start <= 0 || start != record.started_at.timestamp() {
        return None;
    }
    let offset = detail["startTimeOffsetInSeconds"]
        .as_i64()
        .and_then(|offset| i32::try_from(offset).ok())
        .and_then(FixedOffset::east_opt);
    let local_day = day(record, detail).map(|(day, _)| day);
    Some(sample(
        record,
        "metric",
        format!("move_iq:{id}"),
        record.started_at,
        record.ended_at,
        json!({"metric":"garmin_move_iq","provider_data":{
            "activity_type":detail["activityType"].as_str()
                .filter(|name| !name.trim().is_empty()).map(|name| name.to_ascii_lowercase()),
            "duration_seconds":detail["durationInSeconds"].as_i64(),
            "automatically_detected":true,
            "local_day":local_day,
            "utc_offset_seconds":offset.map(|offset| offset.local_minus_utc()),
            "garmin_summary_id":id
        }}),
    ))
}

fn epoch_category(detail: &Value) -> &str {
    detail["activityType"]
        .as_str()
        .filter(|name| !name.is_empty() && name.len() <= 100)
        .unwrap_or("unknown")
}

fn epoch_steps(record: &Record, detail: &Value) -> Option<Record> {
    let count = steps_count(detail)?;
    let category = epoch_category(detail);
    if record.started_at >= record.ended_at {
        return None;
    }
    let id = format!("epoch-steps:{}:{category}", record.started_at.timestamp());
    Some(sample(
        record,
        "steps",
        id,
        record.started_at,
        record.ended_at,
        json!({"count":count,"activity_type":category}),
    ))
}

async fn store_epoch_metrics(
    tx: &Transaction<'_>,
    subject: &str,
    record: &Record,
    detail: &Value,
) -> Result<()> {
    if record.started_at >= record.ended_at {
        return Ok(());
    }
    let duration = (record.ended_at - record.started_at).num_seconds() as f64;
    let category = epoch_category(detail);
    for (field, metric, unit, range) in [
        (
            "activeKilocalories",
            "epoch_active_energy",
            "kcal",
            0.0..=f64::MAX,
        ),
        (
            "activeTimeInSeconds",
            "epoch_active_time",
            "s",
            0.0..=duration,
        ),
        (
            "meanHeartRateInBeatsPerMinute",
            "epoch_average_heart_rate",
            "bpm",
            1.0..=350.0,
        ),
    ] {
        let Some(value) = detail[field].as_f64().filter(|num| range.contains(num)) else {
            continue;
        };
        let epoch = sample(
            record,
            "metric",
            format!(
                "epoch:{metric}:{}:{category}",
                record.started_at.timestamp()
            ),
            record.started_at,
            record.ended_at,
            json!({"metric":metric,"provider_data":{"value":value,"unit":unit,"activity_type":category}}),
        );
        wearables::upsert_record(tx, subject, PROVIDER, &epoch).await?;
    }
    Ok(())
}

async fn store_body_composition(
    tx: &Transaction<'_>,
    subject: &str,
    record: &Record,
    detail: &Value,
) -> Result<()> {
    let Some(at) = detail["measurementTimeInSeconds"]
        .as_i64()
        .and_then(|seconds| DateTime::from_timestamp(seconds, 0))
    else {
        return Ok(());
    };
    let offset = detail["measurementTimeOffsetInSeconds"]
        .as_i64()
        .and_then(|value| i32::try_from(value).ok())
        .and_then(FixedOffset::east_opt);
    for (field, metric, unit, divisor, max) in [
        ("weightInGrams", "weight", "kg", 1000.0, f64::MAX),
        (
            "muscleMassInGrams",
            "skeletal_muscle_mass",
            "kg",
            1000.0,
            f64::MAX,
        ),
        ("boneMassInGrams", "bone_mass", "kg", 1000.0, f64::MAX),
        ("bodyFatInPercent", "body_fat", "%", 1.0, 100.0),
        (
            "bodyWaterInPercent",
            "body_water_percentage",
            "%",
            1.0,
            100.0,
        ),
        ("bodyMassIndex", "body_mass_index", "kg/m²", 1.0, f64::MAX),
    ] {
        let Some(value) = detail[field]
            .as_f64()
            .filter(|num| *num > 0.0 && *num <= max)
        else {
            continue;
        };
        let derived = sample(
            record,
            "metric",
            format!("{metric}:{}", record.source_id),
            at,
            at,
            json!({"metric":metric,"provider_data":{"value":value/divisor,"unit":unit,
                "utc_offset_seconds":offset.map(|offset| offset.local_minus_utc())}}),
        );
        wearables::upsert_record(tx, subject, PROVIDER, &derived).await?;
    }
    Ok(())
}

async fn store_health_summary(
    tx: &Transaction<'_>,
    subject: &str,
    record: &Record,
    feed: &str,
    detail: &Value,
) -> Result<()> {
    let values;
    let fields: &[(&str, &str, &str, f64, f64)] = match feed {
        "bloodPressures" => &[
            (
                "systolic",
                "blood_pressure_systolic",
                "mmHg",
                f64::MIN_POSITIVE,
                f64::MAX,
            ),
            (
                "diastolic",
                "blood_pressure_diastolic",
                "mmHg",
                f64::MIN_POSITIVE,
                f64::MAX,
            ),
        ],
        "userMetrics" => &[
            (
                "vo2Max",
                "vo2_max",
                "mL/kg/min",
                f64::MIN_POSITIVE,
                f64::MAX,
            ),
            (
                "fitnessAge",
                "garmin_fitness_age",
                "years",
                f64::MIN_POSITIVE,
                f64::MAX,
            ),
        ],
        "skinTemp" => &[(
            "avgDeviationCelsius",
            "skin_temperature_deviation",
            "°C",
            -f64::MAX,
            f64::MAX,
        )],
        "healthSnapshot" => &[
            (
                "heart_rate",
                "health_snapshot_average_heart_rate",
                "bpm",
                1.0,
                350.0,
            ),
            (
                "rmssd_hrv",
                "health_snapshot_hrv_rmssd",
                "ms",
                0.0,
                f64::MAX,
            ),
            ("sdrr_hrv", "health_snapshot_hrv_sdrr", "ms", 0.0, f64::MAX),
            ("stress", "health_snapshot_stress", "score", 0.0, 100.0),
            (
                "pulse_ox",
                "health_snapshot_oxygen_saturation",
                "%",
                f64::MIN_POSITIVE,
                100.0,
            ),
            (
                "respiration",
                "health_snapshot_respiratory_rate",
                "breaths/min",
                f64::MIN_POSITIVE,
                f64::MAX,
            ),
        ],
        _ => return Ok(()),
    };
    let (input, start, end, context) = match feed {
        "userMetrics" | "skinTemp" => {
            let Some(day) = detail["calendarDate"]
                .as_str()
                .and_then(|value| value.parse::<NaiveDate>().ok())
            else {
                return Ok(());
            };
            // A provider date is carried explicitly. Midnight UTC is the query anchor, not a measurement time.
            let at = day.and_time(chrono::NaiveTime::MIN).and_utc();
            (
                detail,
                at,
                at,
                json!({"local_day":day,"time_basis":"provider_day"}),
            )
        }
        "healthSnapshot" => {
            let Some(entries) = detail["summaries"].as_array() else {
                return Ok(());
            };
            values = Value::Object(
                entries
                    .iter()
                    .filter_map(|entry| {
                        let kind = entry["summaryType"].as_str()?;
                        let average = entry.get("avgValue")?;
                        Some((kind.to_owned(), average.clone()))
                    })
                    .collect(),
            );
            (
                &values,
                record.started_at,
                record.ended_at,
                json!({"aggregation":"average"}),
            )
        }
        _ => {
            let offset = detail["measurementTimeOffsetInSeconds"]
                .as_i64()
                .or_else(|| detail["startTimeOffsetInSeconds"].as_i64())
                .and_then(|seconds| i32::try_from(seconds).ok())
                .and_then(FixedOffset::east_opt)
                .map(|offset| offset.local_minus_utc());
            (
                detail,
                record.started_at,
                record.started_at,
                json!({"utc_offset_seconds":offset}),
            )
        }
    };
    for &(field, metric, unit, min, max) in fields {
        let Some(value) = input[field]
            .as_f64()
            .filter(|number| (min..=max).contains(number))
        else {
            continue;
        };
        let mut data = context.clone();
        data["value"] = json!(value);
        data["unit"] = json!(unit);
        let derived = sample(
            record,
            "metric",
            format!("{metric}:{}", record.source_id),
            start,
            end,
            json!({"metric":metric,"provider_data":data}),
        );
        wearables::upsert_record(tx, subject, PROVIDER, &derived).await?;
    }
    Ok(())
}

fn daily_heart_rate(record: &Record, detail: &Value) -> Option<Record> {
    let mut points: Vec<_> = detail["timeOffsetHeartRateSamples"]
        .as_object()?
        .iter()
        .filter_map(|(offset, bpm)| {
            let offset = offset.parse::<i64>().ok()?;
            let bpm = bpm.as_u64().filter(|bpm| (1..=350).contains(bpm))?;
            let at = record
                .started_at
                .checked_add_signed(Duration::try_seconds(offset)?)?;
            (at >= record.started_at && at <= record.ended_at).then_some((at, bpm))
        })
        .collect();
    points.sort_unstable();
    points.dedup_by_key(|(at, _)| *at);
    if points.len() > 4096 {
        return None;
    }
    let start = points.first()?.0;
    let end = points.last()?.0;
    let key = day(record, detail).map_or_else(
        || record.started_at.timestamp().to_string(),
        |(day, _)| day.to_string(),
    );
    let payload = json!({"samples":points.into_iter().map(|(at,bpm)|json!({"at":at,"bpm":bpm})).collect::<Vec<_>>()});
    Some(sample(
        record,
        "heart_rate",
        format!("daily-heart:{key}"),
        start,
        end,
        payload,
    ))
}

fn offset_samples(
    record: &Record,
    values: &Value,
    maximum: f64,
    allow_zero: bool,
) -> Option<Value> {
    let mut points = BTreeMap::new();
    for (offset, value) in values.as_object()? {
        let Some((offset, value)) = offset.parse::<i64>().ok().zip(value.as_f64()) else {
            continue;
        };
        if offset < 0 || !(0.0..=maximum).contains(&value) || (value == 0.0 && !allow_zero) {
            continue;
        }
        let Some(at) = Duration::try_seconds(offset)
            .and_then(|delta| record.started_at.checked_add_signed(delta))
        else {
            continue;
        };
        if (record.ended_at > record.started_at && at > record.ended_at)
            || (record.ended_at == record.started_at && offset > 86_400)
        {
            continue;
        }
        points.insert(at, json!({"at":at,"value":value,"offset_seconds":offset}));
    }
    Some(json!(points.into_values().collect::<Vec<_>>()))
}

async fn store_offset_samples(
    tx: &Transaction<'_>,
    subject: &str,
    record: &Record,
    metric: &str,
    unit: &str,
    points: Option<Value>,
) -> Result<()> {
    let Some(points) = points else {
        return Ok(());
    };
    tx.execute(
        "WITH current_summary AS (
            SELECT 1 FROM avatar_hsm.wearable_record
            WHERE subject_id=$1 AND provider='garmin' AND kind='metric' AND source=$2
              AND source_id=$3 AND modified_at=$4
         ), points AS (
            SELECT at,value,offset_seconds FROM jsonb_to_recordset($7::jsonb)
                AS point(at timestamptz,value double precision,offset_seconds bigint)
         ), removed AS (
            DELETE FROM avatar_hsm.wearable_record r
            WHERE r.subject_id=$1 AND r.provider='garmin' AND r.kind='metric' AND r.source=$2
              AND r.payload ? 'garmin_summary_id'
              AND r.payload->>'garmin_summary_id'=$3 AND r.payload->>'metric'=$5
              AND EXISTS (SELECT 1 FROM current_summary)
              AND NOT EXISTS (SELECT 1 FROM points p WHERE r.source_id=$5||':'||$3||':'||p.offset_seconds)
         )
         INSERT INTO avatar_hsm.wearable_record AS existing
            (subject_id,provider,kind,source,source_id,started_at,ended_at,modified_at,payload)
         SELECT $1,'garmin','metric',$2,$5||':'||$3||':'||p.offset_seconds, p.at,p.at,$4,
            jsonb_build_object('metric',$5,'garmin_summary_id',$3,
                'provider_data',jsonb_build_object('value',p.value,'unit',$6::text,'offset_seconds',p.offset_seconds))
         FROM points p CROSS JOIN current_summary
         ON CONFLICT(subject_id,provider,kind,source,source_id) DO UPDATE
         SET started_at=excluded.started_at,ended_at=excluded.ended_at,modified_at=excluded.modified_at,payload=excluded.payload
         WHERE existing.modified_at<=excluded.modified_at",
        &[&subject,&record.source,&record.source_id,&record.modified_at,&metric,&unit,&points],
    ).await?;
    Ok(())
}

async fn store_summary_average(
    tx: &Transaction<'_>,
    subject: &str,
    record: &Record,
    metric: &str,
    unit: &str,
    value: Option<f64>,
) -> Result<()> {
    let current = tx
        .query_opt(
            "SELECT 1 FROM avatar_hsm.wearable_record
         WHERE subject_id=$1 AND provider='garmin' AND kind='metric' AND source=$2
           AND source_id=$3 AND modified_at=$4",
            &[
                &subject,
                &record.source,
                &record.source_id,
                &record.modified_at,
            ],
        )
        .await?;
    if current.is_none() {
        return Ok(());
    }
    let id = format!("{metric}:{}", record.source_id);
    if let Some(value) = value {
        let local_day = day(record, summary(record)).map(|(day, _)| day);
        let average = sample(
            record,
            "metric",
            id,
            record.started_at,
            record.ended_at,
            json!({
                "metric":metric,"garmin_summary_id":record.source_id,
                "provider_data":{"value":value,"unit":unit,"local_day":local_day,"aggregation":"provider_average"}
            }),
        );
        wearables::upsert_record(tx, subject, PROVIDER, &average).await?;
    } else {
        tx.execute(
            "DELETE FROM avatar_hsm.wearable_record WHERE subject_id=$1 AND provider='garmin'
             AND kind='metric' AND source=$2 AND source_id=$3 AND payload->>'metric'=$4",
            &[&subject, &record.source, &id, &metric],
        )
        .await?;
    }
    Ok(())
}

async fn project_sleep(tx: &Transaction<'_>, subject: &str, record: &Record) -> Result<()> {
    let detail = summary(record);
    let mut durations = Map::new();
    for (field, name) in [
        ("deepSleepDurationInSeconds", "deep_seconds"),
        ("lightSleepDurationInSeconds", "light_seconds"),
        ("remSleepInSeconds", "rem_seconds"),
        ("awakeDurationInSeconds", "awake_seconds"),
    ] {
        if let Some(seconds) = detail[field].as_u64() {
            durations.insert(name.to_owned(), json!(seconds));
        }
    }
    let score = detail["overallSleepScore"]["value"]
        .as_u64()
        .filter(|score| *score <= 100);
    let components: Map<String, Value> = detail["sleepScores"]
        .as_object()
        .into_iter()
        .flatten()
        .filter_map(|(name, value)| Some((name.clone(), json!(value["qualifierKey"].as_str()?))))
        .collect();
    let metadata = json!({
        "nap":false,
        "provider_stage_summary":durations,
        "sleep_score":score,
        "sleep_score_qualifier":detail["overallSleepScore"]["qualifier"].as_str(),
        "sleep_score_components":components,
        "provider_state":detail["validation"].as_str(),
    });
    let updated = tx
        .execute(
            "UPDATE avatar_hsm.wearable_record SET payload=payload || $4::jsonb
         WHERE subject_id=$1 AND provider='garmin' AND kind='sleep' AND source=$2
           AND source_id=$3 AND modified_at=$5",
            &[
                &subject,
                &record.source,
                &record.source_id,
                &metadata,
                &record.modified_at,
            ],
        )
        .await?;
    if updated == 0 {
        return Ok(());
    }
    tx.execute(
        "DELETE FROM avatar_hsm.wearable_record WHERE subject_id=$1 AND provider='garmin' AND kind='sleep'
         AND source=$2 AND payload ? 'garmin_parent_sleep_id' AND payload->>'garmin_parent_sleep_id'=$3",
        &[&subject,&record.source,&record.source_id],
    ).await?;
    let Some(naps) = detail["naps"].as_array().filter(|naps| naps.len() <= 100) else {
        return Ok(());
    };
    for nap in naps {
        let Some((at, seconds)) = nap["napStartTimeInSeconds"]
            .as_i64()
            .filter(|at| *at > 0)
            .zip(
                nap["napDurationInSeconds"]
                    .as_i64()
                    .filter(|seconds| (1..=86_400).contains(seconds)),
            )
        else {
            continue;
        };
        let Some(start) = DateTime::from_timestamp(at, 0) else {
            continue;
        };
        let Some(end) = start
            .checked_add_signed(Duration::seconds(seconds))
            .filter(|end| *end <= record.modified_at + Duration::minutes(5))
        else {
            continue;
        };
        let offset = nap["napOffsetInSeconds"]
            .as_i64()
            .and_then(|offset| i32::try_from(offset).ok())
            .and_then(FixedOffset::east_opt)
            .map(|offset| offset.local_minus_utc());
        let record = sample(
            record,
            "sleep",
            format!("nap:{}:{at}", record.source_id),
            start,
            end,
            json!({
                "stages":[], "nap":true, "garmin_parent_sleep_id":record.source_id,
                "duration_seconds":seconds, "utc_offset_seconds":offset,
                "provider_state":detail["validation"].as_str(), "provider_data":nap
            }),
        );
        wearables::upsert_record(tx, subject, PROVIDER, &record).await?;
    }
    Ok(())
}

pub async fn project(
    tx: &Transaction<'_>,
    subject: &str,
    record: &Record,
    kinds: &mut Vec<&str>,
) -> Result<bool> {
    if record.kind == "sleep" {
        project_sleep(tx, subject, record).await?;
        return Ok(false);
    }
    let detail = summary(record);
    let projected = match record.payload["metric"].as_str() {
        Some("epochs") => {
            store_epoch_metrics(tx, subject, record, detail).await?;
            epoch_steps(record, detail)
        }
        Some("dailies") => {
            if let Some((day, timezone)) = day(record, detail) {
                let daily = [
                    (
                        "steps",
                        steps_count(detail).map(|count| json!({"total":count,"unit":"steps"})),
                    ),
                    ("activity", activity_stats(detail)),
                ];
                for (kind, stats) in daily {
                    if let Some(stats) = stats {
                        tx.execute("INSERT INTO avatar_hsm.wearable_daily AS existing(subject_id,provider,kind,source,local_day,timezone,calculated_by,stats,modified_at)
                            VALUES($1,'garmin',$2,$3,$4,$5,'provider',$6,$7)
                            ON CONFLICT(subject_id,provider,kind,source,local_day,timezone,calculated_by) DO UPDATE
                            SET stats=excluded.stats,modified_at=excluded.modified_at WHERE existing.modified_at<=excluded.modified_at",
                            &[&subject,&kind,&record.source,&day.to_string(),&timezone,&stats,&record.modified_at]).await?;
                        kinds.push(kind);
                    }
                }
            }
            daily_heart_rate(record, detail)
        }
        Some("stressDetails") => {
            for (metric, primary, fallback) in [
                (
                    "garmin_stress_level",
                    "timeOffsetStressLevelValues",
                    "stressLevelValues",
                ),
                (
                    "garmin_body_battery",
                    "timeOffsetBodyBatteryValues",
                    "bodyBatteryValues",
                ),
            ] {
                let primary = detail.get(primary).filter(|value| value.is_object());
                let values = primary
                    .filter(|value| value.as_object().is_some_and(|items| !items.is_empty()))
                    .or_else(|| detail.get(fallback).filter(|value| value.is_object()))
                    .or(primary)
                    .unwrap_or(&Value::Null);
                let points = offset_samples(record, values, 100.0, true);
                store_offset_samples(tx, subject, record, metric, "score", points).await?;
            }
            None
        }
        Some("allDayRespiration") => {
            let points =
                offset_samples(record, &detail["timeOffsetEpochToBreaths"], f64::MAX, false);
            store_offset_samples(
                tx,
                subject,
                record,
                "respiratory_rate",
                "breaths/min",
                points,
            )
            .await?;
            store_summary_average(
                tx,
                subject,
                record,
                "garmin_average_waking_respiratory_rate",
                "breaths/min",
                detail["avgWakingRespirationValue"]
                    .as_f64()
                    .filter(|avg| *avg > 0.0),
            )
            .await?;
            None
        }
        Some("pulseox") => {
            let points = offset_samples(record, &detail["timeOffsetSpo2Values"], 100.0, false);
            store_offset_samples(tx, subject, record, "oxygen_saturation", "%", points).await?;
            store_summary_average(
                tx,
                subject,
                record,
                "garmin_average_oxygen_saturation",
                "%",
                detail["avgSpo2"]
                    .as_f64()
                    .filter(|avg| *avg > 0.0 && *avg <= 100.0),
            )
            .await?;
            None
        }
        Some("hrv") => {
            let points = offset_samples(record, &detail["hrvValues"], f64::MAX, true);
            store_offset_samples(tx, subject, record, "hrv_rmssd", "ms", points).await?;
            if let Some(average) = detail["lastNightAvg"]
                .as_f64()
                .filter(|value| value.is_finite() && *value >= 0.0)
            {
                let local_day = day(record, detail).map(|(day, _)| day.to_string());
                let nightly = sample(
                    record,
                    "metric",
                    format!("nightly:{}", record.source_id),
                    record.started_at,
                    record.ended_at,
                    json!({"metric":"garmin_nightly_hrv_rmssd","provider_data":{
                        "value":average,"unit":"ms","local_day":local_day
                    }}),
                );
                wearables::upsert_record(tx, subject, PROVIDER, &nightly).await?;
            }
            None
        }
        Some("bodyComps") => {
            store_body_composition(tx, subject, record, detail).await?;
            None
        }
        Some("mct") => {
            if let Some(cycle) = menstrual_cycle(record, detail) {
                wearables::upsert_record(tx, subject, PROVIDER, &cycle).await?;
            }
            None
        }
        Some("moveIQActivities") => {
            if let Some(event) = move_iq(record, detail) {
                wearables::upsert_record(tx, subject, PROVIDER, &event).await?;
            }
            None
        }
        Some(feed @ ("bloodPressures" | "userMetrics" | "skinTemp" | "healthSnapshot")) => {
            store_health_summary(tx, subject, record, feed, detail).await?;
            None
        }
        _ => None,
    };
    if let Some(projected) = projected {
        kinds.push(if projected.kind == "steps" {
            "steps"
        } else {
            "heart_rate"
        });
        wearables::upsert_record(tx, subject, PROVIDER, &projected).await?;
        return Ok(true);
    }
    Ok(false)
}

async fn rebuild_one(pool: &Pool) -> Result<bool> {
    let mut db = pool.get().await?;
    let tx = db.transaction().await?;
    let Some(subject_row)=tx.query_opt("SELECT subject_id FROM avatar_hsm.wearable_oauth_connection
        WHERE provider='garmin' AND NOT garmin_daily_projected AND (garmin_permissions & 2)<>0 ORDER BY subject_id LIMIT 1", &[]).await? else {return Ok(false);};
    let subject: String = subject_row.get(0);
    tx.query_one(
        "SELECT pg_advisory_xact_lock(hashtext($1),hashtext('garmin'))",
        &[&subject],
    )
    .await?;
    if tx.execute("UPDATE avatar_hsm.wearable_oauth_connection SET garmin_daily_projected=true
        WHERE subject_id=$1 AND provider='garmin' AND NOT garmin_daily_projected AND (garmin_permissions & 2)<>0", &[&subject]).await? != 0 {
        let rows=tx.query("SELECT source,source_id,started_at,ended_at,modified_at,payload,kind FROM avatar_hsm.wearable_record
            WHERE subject_id=$1 AND provider='garmin'
              AND ((kind='metric' AND payload->>'metric' IN ('epochs','dailies','stressDetails','allDayRespiration','pulseox','hrv','bodyComps',
                'bloodPressures','userMetrics','skinTemp','healthSnapshot','mct','moveIQActivities'))
                OR (kind='sleep' AND NOT (payload ? 'garmin_parent_sleep_id')))
            ORDER BY modified_at,source_id", &[&subject]).await?;
        let mut kinds=Vec::new();
        let mut changed=false;
        for row in rows {
            let record=Record{kind:row.get(6),source:row.get(0),source_id:row.get(1),started_at:row.get(2),ended_at:row.get(3),modified_at:row.get(4),payload:row.get(5)};
            changed |= project(&tx,&subject,&record,&mut kinds).await?;
        }
        if changed {wearables::rebuild_daily(&tx,&subject,PROVIDER).await?;}
        tx.execute("UPDATE avatar_hsm.device_connection SET grants=ARRAY(SELECT DISTINCT unnest(grants || $2::text[]))
            WHERE subject_id=$1 AND provider='garmin' AND state='synced'", &[&subject,&kinds]).await?;
    }
    tx.commit().await?;
    Ok(true)
}

pub async fn rebuild_existing(pool: Pool) {
    loop {
        match rebuild_one(&pool).await {
            Ok(true) => continue,
            Ok(false) | Err(_) => tokio::time::sleep(std::time::Duration::from_secs(60)).await,
        }
    }
}
