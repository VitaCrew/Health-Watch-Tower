use std::collections::{HashMap, HashSet};

use anyhow::{ensure, Context, Result};
use deadpool_postgres::Pool;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tokio_postgres::Transaction;

use crate::wearables::Record;

const MAX_POINTS_PER_ROUTE: usize = 2_000;
const MAX_POINTS_PER_SNAPSHOT: usize = 12_000;

#[derive(Deserialize, Serialize)]
#[serde(tag = "permission", rename_all = "snake_case", deny_unknown_fields)]
pub enum RouteSnapshot {
    Denied,
    Granted { routes: Vec<RouteUpload> },
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RouteUpload {
    source: String,
    source_id: String,
    source_point_count: i32,
    points: Vec<Point>,
}

#[derive(Deserialize, Serialize)]
struct Point(
    i64,
    f64,
    f64,
    Option<f64>,
    Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")] Option<f64>,
);

pub async fn store(
    tx: &Transaction<'_>,
    subject: &str,
    provider: &str,
    records: &[Record],
    snapshot: &RouteSnapshot,
) -> Result<()> {
    let RouteSnapshot::Granted { routes } = snapshot else {
        tx.execute(
            "DELETE FROM avatar_hsm.wearable_workout_route WHERE subject_id=$1 AND provider=$2",
            &[&subject, &provider],
        )
        .await?;
        return Ok(());
    };
    let workouts: HashMap<_, _> = records
        .iter()
        .filter(|row| row.kind == "workout")
        .map(|row| ((row.source.as_str(), row.source_id.as_str()), row))
        .collect();
    ensure!(
        routes.len() <= workouts.len()
            && routes.iter().map(|route| route.points.len()).sum::<usize>()
                <= MAX_POINTS_PER_SNAPSHOT,
        "too many Samsung workout route points"
    );
    let mut seen = HashSet::new();
    for route in routes {
        let key = (route.source.as_str(), route.source_id.as_str());
        let workout = workouts
            .get(&key)
            .context("Samsung route requires its workout")?;
        ensure!(
            seen.insert(key)
                && (1..=MAX_POINTS_PER_ROUTE).contains(&route.points.len())
                && route.source_point_count >= i32::try_from(route.points.len())?,
            "invalid Samsung workout route"
        );
        let mut last = workout.started_at.timestamp_millis();
        for &Point(timestamp, latitude, longitude, altitude, accuracy, vertical_accuracy) in
            &route.points
        {
            ensure!(
                timestamp >= last
                    && timestamp <= workout.ended_at.timestamp_millis()
                    && (-90.0..=90.0).contains(&latitude)
                    && (-180.0..=180.0).contains(&longitude)
                    && altitude.is_none_or(f64::is_finite)
                    && accuracy.is_none_or(|v| v.is_finite() && v >= 0.0)
                    && vertical_accuracy.is_none_or(|v| v.is_finite() && v >= 0.0),
                "invalid Samsung workout route point"
            );
            last = timestamp;
        }
        tx.execute(
            "INSERT INTO avatar_hsm.wearable_workout_route AS old
             (subject_id,provider,source,source_id,modified_at,source_point_count,points)
             SELECT subject_id,provider,source,source_id,modified_at,$5,$6 FROM avatar_hsm.wearable_record
              WHERE subject_id=$1 AND provider=$7 AND kind='workout'
                AND source=$2 AND source_id=$3 AND modified_at=$4
             ON CONFLICT (subject_id,provider,kind,source,source_id) DO UPDATE
             SET modified_at=excluded.modified_at, source_point_count=excluded.source_point_count, points=excluded.points
             WHERE excluded.modified_at > old.modified_at OR (excluded.modified_at = old.modified_at
               AND jsonb_array_length(excluded.points) >= jsonb_array_length(old.points)
               AND (EXISTS (SELECT 1 FROM jsonb_array_elements(excluded.points) AS point WHERE jsonb_typeof(point->5)='number')
                 OR NOT EXISTS (SELECT 1 FROM jsonb_array_elements(old.points) AS point WHERE jsonb_typeof(point->5)='number'))
               AND ($7<>'health_connect' OR NOT EXISTS (
                 SELECT 1
                 FROM jsonb_array_elements(old.points) WITH ORDINALITY AS previous(point, position)
                 JOIN jsonb_array_elements(excluded.points) WITH ORDINALITY AS incoming(point, position) USING (position)
                 CROSS JOIN generate_series(3, 5) AS field
                 WHERE previous.point->0=incoming.point->0 AND previous.point->1=incoming.point->1
                   AND previous.point->2=incoming.point->2 AND previous.point->field='0'::jsonb
                   AND coalesce(jsonb_typeof(incoming.point->field), 'null')='null'
               )))",
            &[&subject, &route.source, &route.source_id, &workout.modified_at,
              &route.source_point_count, &serde_json::to_value(&route.points)?, &provider],
        ).await?;
    }
    Ok(())
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Query {
    provider: Option<String>,
    source: String,
    source_id: String,
    #[serde(default)]
    offset: usize,
    limit: Option<usize>,
}

pub async fn query(pool: &Pool, subject: &str, query: Query) -> Result<Value> {
    let provider = query
        .provider
        .as_deref()
        .unwrap_or(crate::wearables::SAMSUNG_HEALTH);
    let limit = query.limit.unwrap_or(100);
    ensure!(
        [&query.source, &query.source_id]
            .into_iter()
            .all(|text| !text.trim().is_empty() && text.len() <= 256)
            && query.offset <= MAX_POINTS_PER_ROUTE
            && (1..=100).contains(&limit)
            && matches!(
                provider,
                crate::wearables::SAMSUNG_HEALTH
                    | crate::wearables::HEALTH_CONNECT
                    | crate::wearables::HEALTHKIT
            ),
        "invalid workout route query"
    );
    let db = pool.get().await?;
    let row = db.query_opt(
        "SELECT g.points,g.source_point_count FROM avatar_hsm.wearable_workout_route g
         JOIN avatar_hsm.wearable_record r USING (subject_id,provider,kind,source,source_id,modified_at)
         JOIN avatar_hsm.device_connection c USING (subject_id,provider)
         WHERE g.subject_id=$1 AND g.source=$2 AND g.source_id=$3 AND g.provider=$4
           AND c.state='synced' AND 'workout'=ANY(c.grants)",
        &[&subject, &query.source, &query.source_id, &provider],
    ).await?;
    let Some(row) = row else {
        return Ok(json!({"state":"not_found","records":[]}));
    };
    let points: Vec<Point> = serde_json::from_value(row.get(0))?;
    let source_point_count: i32 = row.get(1);
    let records: Vec<_> = points
        .iter()
        .skip(query.offset)
        .take(limit)
        .map(
            |Point(timestamp_ms, latitude, longitude, altitude, accuracy, vertical_accuracy)| {
                json!({
                    "timestamp_ms":timestamp_ms,"latitude":latitude,"longitude":longitude,
                    "altitude":altitude,"accuracy":accuracy,"vertical_accuracy":vertical_accuracy
                })
            },
        )
        .collect();
    let next = query.offset + records.len();
    Ok(
        json!({"state":"ready","provider":provider,"source":query.source,
        "source_id":query.source_id,"source_point_count":source_point_count,
        "stored_point_count":points.len(),"next_offset":(next < points.len()).then_some(next),"records":records}),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wearables::{self, RecordQuery, HEALTHKIT, HEALTH_CONNECT, SAMSUNG_HEALTH};

    #[tokio::test]
    #[ignore = "requires isolated PostgreSQL in AVATAR_HSM_TEST_DATABASE_URL"]
    async fn samsung_workout_change_replaces_and_deletes_its_route_atomically() -> Result<()> {
        let pool = crate::store::connect(&std::env::var("AVATAR_HSM_TEST_DATABASE_URL")?).await?;
        let subject = crate::store::SUBJECT;
        let start: chrono::DateTime<chrono::Utc> = "2026-04-25T08:00:00Z".parse()?;
        let revised_start = start + chrono::Duration::minutes(5);
        let change_time = start + chrono::Duration::days(20);
        let uid = "workout-parent";
        let original_id = "workout-parent:2026-04-25T08:00:00Z";
        let revised_id = "workout-parent:2026-04-25T08:05:00Z";
        let upload = async |id,
                            at: chrono::DateTime<chrono::Utc>,
                            revised,
                            latitude|
               -> Result<Value> {
            let lease = wearables::issue_lease(&pool, SAMSUNG_HEALTH).await?;
            wearables::commit(
                &pool,
                SAMSUNG_HEALTH,
                serde_json::from_value(json!({
                    "lease":lease["lease"], "start":start, "end":start+chrono::Duration::hours(1),
                    "historical":true, "grants":["workout"],
                    "samsung_session_changes":if revised {json!({"kind":"workout","revision":{"uid":uid,"changed_at":change_time}})} else {Value::Null},
                    "records":[{"kind":"workout","source":"fixture.samsung","source_id":id,
                        "started_at":at,"ended_at":at+chrono::Duration::minutes(20),
                        "modified_at":if revised {change_time} else {at},
                        "payload":{"exercise_type":"RUNNING","duration_seconds":1200}}],
                    "samsung_routes":{"permission":"granted","routes":[{"source":"fixture.samsung","source_id":id,
                        "source_point_count":1,"points":[[at.timestamp_millis(),latitude,127.0,null,null]]}]}
                }))?,
            )
            .await
        };
        upload(original_id, start, false, 37.0).await?;
        upload(revised_id, revised_start, true, 37.5).await?;
        assert_eq!(
            upload(revised_id, revised_start, true, 38.0).await?["accepted"],
            0
        );
        let routes = async || -> Result<Vec<(String, Value)>> {
            Ok(pool
                .get()
                .await?
                .query(
                    "SELECT source_id,points FROM avatar_hsm.wearable_workout_route
                 WHERE subject_id=$1 AND provider='samsung_health'",
                    &[&subject],
                )
                .await?
                .iter()
                .map(|row| (row.get(0), row.get(1)))
                .collect())
        };
        let rows = routes().await?;
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].0, revised_id);
        assert_eq!(rows[0].1[0][1], 37.5);
        let delete_time = change_time + chrono::Duration::minutes(1);
        let lease = wearables::issue_active_lease(&pool, SAMSUNG_HEALTH, Some(1)).await?;
        wearables::commit(
            &pool,
            SAMSUNG_HEALTH,
            serde_json::from_value(json!({
                "lease":lease["lease"],"start":change_time,"end":delete_time+chrono::Duration::minutes(1),
                "historical":true,"grants":["workout"],"records":[],
                "samsung_session_changes":{"kind":"workout","deletions":[{"uid":uid,"changed_at":delete_time}]}
            }))?,
        )
        .await?;
        upload(original_id, start, false, 37.0).await?;
        assert!(routes().await?.is_empty());
        let count: i64 = pool
            .get()
            .await?
            .query_one(
                "SELECT count(*) FROM avatar_hsm.wearable_record
             WHERE subject_id=$1 AND provider='samsung_health' AND kind='workout'",
                &[&subject],
            )
            .await?
            .get(0);
        assert_eq!(count, 0);
        Ok(())
    }

    #[tokio::test]
    #[ignore = "requires AVATAR_HSM_TEST_DATABASE_URL"]
    async fn workout_route_requires_its_owner_and_can_be_revoked_without_deleting_workout(
    ) -> Result<()> {
        verify_route(SAMSUNG_HEALTH, "samsung_routes").await?;
        verify_route(HEALTH_CONNECT, "health_connect_routes").await?;
        verify_route(HEALTHKIT, "healthkit_routes").await
    }

    async fn verify_route(provider: &str, route_field: &str) -> Result<()> {
        let pool = crate::store::connect(&std::env::var("AVATAR_HSM_TEST_DATABASE_URL")?).await?;
        let subject = format!("{provider}-workout-route-fixture");
        let subject = subject.as_str();
        let read_token = format!("{provider}-workout-route-read-token-0123456789");
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

        let start: chrono::DateTime<chrono::Utc> = "2026-04-25T08:00:00Z".parse()?;
        let end = start + chrono::Duration::minutes(30);
        let source = "fixture.samsung";
        let source_id = "5800863d-91b0-47ce-95d0-54fc6d1c3f68";
        let workout = json!({"kind":"workout","source":source,"source_id":source_id,
            "started_at":start,"ended_at":end,"modified_at":end,
            "payload":{"duration_seconds":1800,"exercise_type":"RUNNING","provider_data":{"energy_kcal":251}}});
        let snapshot = |lease: &Value, routes: Value| {
            serde_json::from_value(
                json!({"lease":lease,"start":start,"end":end+chrono::Duration::seconds(60),
                "grants":["workout"],"records":[workout],(route_field):routes}),
            )
        };
        let route = |latitude: f64| {
            json!({"permission":"granted","routes":[{
            "source":source,"source_id":source_id,"source_point_count":3,"points":[
                [start.timestamp_millis(),latitude,127.01,null,3.0],
                [end.timestamp_millis(),37.51,127.02,15.0,null]
            ]}]})
        };
        let lease = wearables::issue_lease_for(&pool, subject, provider).await?;
        assert!(wearables::commit_for(
            &pool,
            subject,
            provider,
            snapshot(&lease["lease"], route(91.0))?
        )
        .await
        .is_err());
        wearables::commit_for(
            &pool,
            subject,
            provider,
            snapshot(&lease["lease"], route(37.5))?,
        )
        .await?;
        if provider != SAMSUNG_HEALTH {
            let mut enriched = route(37.5);
            let point = &mut enriched["routes"][0]["points"][0];
            point.as_array_mut().unwrap().push(json!(-1.0));
            let lease = wearables::issue_lease_for(&pool, subject, provider).await?;
            assert!(wearables::commit_for(
                &pool,
                subject,
                provider,
                snapshot(&lease["lease"], enriched.clone())?
            )
            .await
            .is_err());
            enriched["routes"][0]["points"][0][5] = json!(6.0);
            wearables::commit_for(
                &pool,
                subject,
                provider,
                snapshot(&lease["lease"], enriched)?,
            )
            .await?;
            let lease = wearables::issue_lease_for(&pool, subject, provider).await?;
            wearables::commit_for(
                &pool,
                subject,
                provider,
                snapshot(&lease["lease"], route(37.5))?,
            )
            .await?;
        }
        let lease = wearables::issue_lease_for(&pool, subject, provider).await?;
        wearables::commit_for(
            &pool,
            subject,
            provider,
            snapshot(&lease["lease"], Value::Null)?,
        )
        .await?;
        let args = |offset: usize| {
            let mut value =
                json!({"source":source,"source_id":source_id,"offset":offset,"limit":1});
            if provider != SAMSUNG_HEALTH {
                value["provider"] = json!(provider);
            }
            value
        };
        let other = query(
            &pool,
            crate::store::SUBJECT,
            serde_json::from_value(args(0))?,
        )
        .await?;
        assert_eq!(other["state"], "not_found");

        let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0)).await?;
        let address = listener.local_addr()?;
        let app = std::sync::Arc::new(crate::mcp::App {
            db: pool.clone(),
            read_token: String::new(),
            control_token: String::new(),
            rules: crate::acute::rules()?,
        });
        let server = tokio::spawn(async move { axum::serve(listener, crate::router(app)).await });
        let body: Value = crate::cloud_wearables::http_client()?
            .post(format!("http://{address}/mcp/wearable-read"))
            .bearer_auth(read_token)
            .json(
                &json!({"jsonrpc":"2.0","id":1,"method":"tools/call","params":{
                    "name":"wearable_sessions","arguments":{"route":args(0)}
                }}),
            )
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;
        server.abort();
        let page = &body["result"]["structuredContent"];
        assert_eq!(page["provider"], provider);
        assert_eq!(page["source_point_count"], 3);
        assert_eq!(page["stored_point_count"], 2);
        assert_eq!(page["next_offset"], 1);
        assert_eq!(page["records"][0]["latitude"].as_f64(), Some(37.5));
        assert_eq!(page["records"][0]["accuracy"].as_f64(), Some(3.0));
        assert_eq!(
            page["records"][0]["vertical_accuracy"].as_f64(),
            (provider != SAMSUNG_HEALTH).then_some(6.0)
        );
        let last = query(&pool, subject, serde_json::from_value(args(1))?).await?;
        assert_eq!(last["records"][0]["timestamp_ms"], end.timestamp_millis());
        assert!(last["records"][0]["vertical_accuracy"].is_null());
        assert!(last["next_offset"].is_null());

        if provider == HEALTH_CONNECT {
            let mut sea_level = route(37.5);
            sea_level["routes"][0]["points"][0][3] = json!(0.0);
            sea_level["routes"][0]["points"][0][4] = json!(0.0);
            sea_level["routes"][0]["points"][0]
                .as_array_mut()
                .unwrap()
                .push(json!(6.0));
            let lease = wearables::issue_lease_for(&pool, subject, provider).await?;
            wearables::commit_for(
                &pool,
                subject,
                provider,
                snapshot(&lease["lease"], sea_level.clone())?,
            )
            .await?;
            sea_level["routes"][0]["points"][0][3] = Value::Null;
            sea_level["routes"][0]["points"][0][4] = Value::Null;
            let lease = wearables::issue_lease_for(&pool, subject, provider).await?;
            wearables::commit_for(
                &pool,
                subject,
                provider,
                snapshot(&lease["lease"], sea_level)?,
            )
            .await?;
            let stored = query(&pool, subject, serde_json::from_value(args(0))?).await?;
            assert_eq!(stored["records"][0]["altitude"].as_f64(), Some(0.0));
            assert_eq!(stored["records"][0]["accuracy"].as_f64(), Some(0.0));
        }

        let lease = wearables::issue_lease_for(&pool, subject, provider).await?;
        wearables::commit_for(
            &pool,
            subject,
            provider,
            snapshot(&lease["lease"], json!({"permission":"denied"}))?,
        )
        .await?;
        let page = query(&pool, subject, serde_json::from_value(args(0))?).await?;
        assert_eq!(page["state"], "not_found");
        let rows = wearables::records_for(
            &pool,
            subject,
            RecordQuery {
                kind: "workout".into(),
                start,
                end,
                offset: 0,
                source: None,
                provider: Some(provider.into()),
                metric: None,
            },
            true,
        )
        .await?;
        assert_eq!(
            rows["records"][0]["payload"]["provider_data"]["energy_kcal"],
            251
        );
        assert!(rows["records"][0]["route_point_count"].is_null());
        let row = pool
            .get()
            .await?
            .query_one(
                "SELECT count(*) FROM avatar_hsm.wearable_workout_route
            WHERE subject_id=$1",
                &[&subject],
            )
            .await?;
        assert_eq!(row.get::<_, i64>(0), 0);
        Ok(())
    }
}
