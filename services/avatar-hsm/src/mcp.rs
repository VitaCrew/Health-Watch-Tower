use std::sync::Arc;

use crate::{
    acute, cloud_wearables, diary, drug, food, store, wearable_oauth, wearable_sync, wearables,
};
use anyhow::{bail, ensure, Context, Result};
use axum::{
    extract::{Path, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    Json,
};
use chrono::{DateTime, Utc};
use deadpool_postgres::Pool;
use serde::{de::DeserializeOwned, Deserialize};
use serde_json::{json, Value};
use subtle::ConstantTimeEq;
use zen_engine::Decision;

pub struct App {
    pub db: Pool,
    pub read_token: String,
    pub control_token: String,
    pub rules: Decision,
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum Surface {
    Read,
    Control,
}

pub async fn handle(
    State(app): State<Arc<App>>,
    Path(surface): Path<String>,
    headers: HeaderMap,
    Json(message): Json<Value>,
) -> Response {
    let (surface, expected) = match surface.as_str() {
        "read" => (Surface::Read, Some(&app.read_token)),
        "control" => (Surface::Control, Some(&app.control_token)),
        "wearable-read" => (Surface::Read, None),
        "wearable-control" => (Surface::Control, None),
        _ => return StatusCode::NOT_FOUND.into_response(),
    };
    // These loopback endpoints are for first-party service clients.
    if let Some(origin) = headers.get("Origin") {
        if expected.is_some() || !origin.to_str().is_ok_and(wearable_oauth::is_portal_origin) {
            return StatusCode::FORBIDDEN.into_response();
        }
    }
    let supplied = headers
        .get("Authorization")
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "))
        .unwrap_or_default();
    let subject = if let Some(expected) = expected {
        if !bool::from(expected.as_bytes().ct_eq(supplied.as_bytes())) {
            return StatusCode::UNAUTHORIZED.into_response();
        }
        None
    } else {
        match wearable_oauth::authenticate(&app.db, supplied, surface == Surface::Control).await {
            Ok(Some(subject)) => Some(subject),
            Ok(None) => return StatusCode::UNAUTHORIZED.into_response(),
            Err(_) => return StatusCode::SERVICE_UNAVAILABLE.into_response(),
        }
    };
    let id = message["id"].clone();
    let method = message["method"].as_str().unwrap_or_default();
    if message["jsonrpc"] != "2.0"
        || method.is_empty()
        || (!id.is_null() && !id.is_number() && !id.is_string())
    {
        return rpc_error(id, -32600, "Invalid request");
    }
    if id.is_null() {
        return StatusCode::ACCEPTED.into_response();
    }
    let output = match method {
        "initialize" => {
            let offered = message["params"]["protocolVersion"]
                .as_str()
                .unwrap_or_default();
            let version = match offered {
                "2025-03-26" | "2025-06-18" | "2025-11-25" => offered,
                _ => "2025-06-18",
            };
            json!({
                "protocolVersion": version, "capabilities": { "tools": {} },
                "serverInfo": { "name": "avatar-hsm", "version": env!("CARGO_PKG_VERSION") }
            })
        }
        "ping" => json!({}),
        "tools/list" => {
            json!({ "tools": if subject.is_some() { wearable_tools(surface) } else { list(surface) } })
        }
        "tools/call" => {
            let params = &message["params"];
            let Some(tool) = params["name"].as_str() else {
                return rpc_error(id, -32602, "Missing tool name");
            };
            let result = if let Some(subject) = subject.as_deref() {
                scoped_call(&app.db, surface, subject, tool, &params["arguments"]).await
            } else {
                call(&app, surface, tool, &params["arguments"]).await
            };
            match result {
                Ok(value) => json!({
                    "content": [{"type": "text", "text": value.to_string()}],
                    "structuredContent": value
                }),
                Err(error) => {
                    let message = if error.downcast_ref::<tokio_postgres::Error>().is_some() {
                        "HSM storage unavailable".to_owned()
                    } else {
                        error.to_string()
                    };
                    json!({ "isError": true, "content": [{"type": "text", "text": message}] })
                }
            }
        }
        _ => return rpc_error(id, -32601, "Method not found"),
    };
    Json(json!({ "jsonrpc": "2.0", "id": id, "result": output })).into_response()
}

fn rpc_error(id: Value, code: i32, message: &str) -> Response {
    Json(json!({ "jsonrpc": "2.0", "id": id, "error": { "code": code, "message": message } }))
        .into_response()
}

fn tool(name: &str, description: &str, properties: Value, required: &[&str]) -> Value {
    json!({
        "name": name, "description": description,
        "inputSchema": { "type": "object", "properties": properties,
                         "required": required, "additionalProperties": false }
    })
}

fn schedule_tool() -> Value {
    let timestamp = json!({"type":"string","format":"date-time"});
    tool(
        "schedule_cloud_sync",
        "Queue a historical date range and enable recurring sync.",
        json!({
            "provider":{"type":"string"}, "start":timestamp, "end":timestamp
        }),
        &["provider"],
    )
}

fn wearable_tools(surface: Surface) -> Vec<Value> {
    let provider = json!({"provider":{"type":"string"}});
    if surface == Surface::Control {
        return vec![
            schedule_tool(),
            tool(
                "connect_cloud_oauth",
                "Start linking your cloud health account in a browser.",
                provider.clone(),
                &["provider"],
            ),
            tool(
                "revoke_cloud_device",
                "Delete your provider connection and imported wearable records.",
                provider,
                &["provider"],
            ),
        ];
    }
    list(surface)
        .into_iter()
        .filter(|tool| {
            matches!(
                tool["name"].as_str(),
                Some(
                    "cloud_device_status"
                        | "wearable_samples"
                        | "wearable_daily"
                        | "wearable_sessions"
                        | "wearable_fit_details"
                        | "wearable_goals"
                        | "polar_physical_info"
                )
            )
        })
        .collect()
}

pub async fn scoped_call(
    pool: &Pool,
    surface: Surface,
    subject: &str,
    name: &str,
    arguments: &Value,
) -> Result<Value> {
    match (surface, name) {
        (Surface::Read, "medication_candidates") => {
            let client = pool.get().await?;
            let request: Candidate = args(arguments)?;
            store::medication_candidates_for(&client, &request.name, subject).await
        }
        (Surface::Read, "check_dur") => {
            let client = pool.get().await?;
            let request: Confirmed = args(arguments)?;
            Ok(store::dur_for(&client, request.item_seq.as_deref(), subject).await)
        }
        (Surface::Read, "health_status_v1") => {
            let client = pool.get().await?;
            store::health_status_v1_for(&client, subject, Utc::now()).await
        }
        (Surface::Read, "hsm_state_v1") => Ok(crate::state_owner::current_state(pool, subject)
            .await?
            .map(|(sequence, state)| json!({"sequence": sequence, "state": state}))
            .unwrap_or_else(|| json!({"sequence": 0, "state": {"streams": {}}}))),
        (Surface::Read, "get_plan_actual_links") => {
            let client = pool.get().await?;
            let query: PlanActualLinksQuery = args(arguments)?;
            store::plan_actual_links_for(&client, subject, query.start, query.end).await
        }
        (Surface::Read, "sleep_sessions") => {
            let client = pool.get().await?;
            store::sleep_sessions_for(&client, subject).await
        }
        (Surface::Read, "diary_entries") => {
            let client = pool.get().await?;
            diary::read_for(&client, subject, args(arguments)?).await
        }
        (Surface::Read, "nutrition_daily") => {
            let client = pool.get().await?;
            diary::nutrition_summary_for(&client, subject, args(arguments)?).await
        }
        (Surface::Read, "diary_water_today") => {
            let client = pool.get().await?;
            diary::water_today_for(&client, subject).await
        }
        (Surface::Read, "cloud_device_status") => {
            wearable_sync::status(
                pool,
                subject,
                args::<cloud_wearables::Request>(arguments)?.provider,
            )
            .await
        }
        (Surface::Read, "device_status") => {
            let request = if arguments.is_null() {
                wearables::DeviceSource::default()
            } else {
                args(arguments)?
            };
            wearables::status_for(pool, subject, request.mobile_provider()?).await
        }
        (Surface::Read, "wearable_samples") => {
            wearables::records_for(pool, subject, args(arguments)?, false).await
        }
        (Surface::Read, "wearable_sessions") => sessions(pool, subject, arguments).await,
        (Surface::Read, "wearable_daily") => {
            wearables::daily_for(pool, subject, args(arguments)?).await
        }
        (Surface::Read, "wearable_fit_details") => {
            crate::wearable_fit_details::query(pool, subject, args(arguments)?).await
        }
        (Surface::Read, "wearable_goals") => crate::samsung_goals::query(pool, subject).await,
        (Surface::Read, "polar_physical_info") => {
            crate::wearable_webhook_documents::current_polar_physical_info(pool, subject).await
        }
        (Surface::Control, "connect_cloud_oauth") => {
            wearable_oauth::begin(
                pool,
                subject,
                args::<cloud_wearables::Request>(arguments)?.provider,
            )
            .await
        }
        (Surface::Control, "request_oura_due_sync") => {
            let slot = arguments
                .get("slot_at")
                .and_then(Value::as_str)
                .ok_or_else(|| anyhow::anyhow!("missing Oura UTC slot"))?;
            let slot_at = DateTime::parse_from_rfc3339(slot)?.with_timezone(&Utc);
            let queued = wearable_sync::request_oura_due_sync(pool, subject, slot_at).await?;
            Ok(
                json!({"state": if queued { "queued" } else { "already_queued_or_disconnected" }, "slot_at": slot_at}),
            )
        }
        (Surface::Control, "sync_cloud_device") => {
            let provider = args::<cloud_wearables::Request>(arguments)?.provider;
            wearable_sync::sync_now(pool, subject, provider).await
        }
        (Surface::Control, "schedule_cloud_sync") => {
            wearable_sync::schedule(pool, subject, args(arguments)?).await
        }
        (Surface::Control, "issue_health_connect_lease") => {
            let request = if arguments.is_null() {
                wearables::DeviceSource::default()
            } else {
                args(arguments)?
            };
            let expected = request
                .expected_generation
                .as_deref()
                .map(str::parse::<i64>)
                .transpose()
                .context("invalid device connection generation")?;
            ensure!(
                expected.is_none_or(|value| request.require_active && value > 0),
                "a positive generation requires an active connection"
            );
            if request.require_active {
                wearables::issue_active_lease_for(
                    pool,
                    subject,
                    request.mobile_provider()?,
                    expected,
                )
                .await
            } else {
                wearables::issue_lease_for(pool, subject, request.mobile_provider()?).await
            }
        }
        (Surface::Control, "commit_health_connect_snapshot") => {
            let snapshot: wearables::Snapshot = args(arguments)?;
            let provider = snapshot.mobile_provider()?;
            wearables::commit_for(pool, subject, provider, snapshot).await
        }
        (Surface::Control, "revoke_health_connect") => {
            let request = if arguments.is_null() {
                wearables::DeviceSource::default()
            } else {
                args(arguments)?
            };
            wearables::revoke_for(pool, subject, request.mobile_provider()?).await
        }
        (Surface::Control, "confirm_diary_entry") => {
            let confirmed: diary::ConfirmedEntry = args(arguments)?;
            valid_id(&confirmed.id)?;
            diary::save_for(pool, subject, confirmed).await
        }
        (Surface::Control, "change_diary_entry") => {
            let change: diary::DiaryChange = args(arguments)?;
            valid_id(&change.id)?;
            diary::change_for(pool, subject, change).await
        }
        (Surface::Control, "record_medication_response_v1") => {
            let response: DoseReply = args(arguments)?;
            let dose_id = valid_id(&response.dose_id)?;
            let now = response.responded_at.unwrap_or_else(Utc::now);
            let envelope = watch_tower_core::records::RecordEnvelope {
                subject_id: subject.to_owned(),
                actor_id: "authenticated-mcp-client".to_owned(),
                adapter_id: "hsm".to_owned(),
                source_id: "watch-tower-mcp".to_owned(),
                record_id: dose_id.to_owned(),
                revision: 1,
                run_id: None,
                observed_at_unix: now.timestamp(),
                idempotency_key: format!("dose-response:{subject}:{dose_id}"),
            };
            if !envelope.is_valid() {
                bail!("medication response is missing common provenance fields");
            }
            store::respond_v1_for_at(pool, subject, dose_id, &response.response, now).await
        }
        (Surface::Control, "store_sleep_session") => {
            let sleep: Sleep = args(arguments)?;
            store::save_sleep_for(
                pool,
                subject,
                valid_id(&sleep.id)?,
                sleep.started_at,
                sleep.completed_at,
            )
            .await
        }
        (Surface::Control, "revoke_cloud_device") => {
            let provider = args::<cloud_wearables::Request>(arguments)?.provider;
            if matches!(provider, cloud_wearables::Provider::Garmin) {
                return crate::garmin_push::disconnect(pool, subject).await;
            }
            wearables::revoke_for(pool, subject, provider.slug()).await
        }
        _ => bail!("tool unavailable on this wearable surface"),
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct PlanActualLinksQuery {
    start: DateTime<Utc>,
    end: DateTime<Utc>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RouteRequest {
    route: crate::samsung_routes::Query,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct AppleRouteRequest {
    apple_route: crate::apple_export_routes::Query,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct GoogleRouteRequest {
    google_route: crate::google_workout_tcx::RouteQuery,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct WorkoutSamplesRequest {
    samples: crate::garmin_workouts::Query,
}

async fn sessions(pool: &Pool, subject: &str, arguments: &Value) -> Result<Value> {
    if arguments.get("route").is_some() {
        return crate::samsung_routes::query(pool, subject, args::<RouteRequest>(arguments)?.route)
            .await;
    }
    if arguments.get("apple_route").is_some() {
        return crate::apple_export_routes::query(
            pool,
            subject,
            args::<AppleRouteRequest>(arguments)?.apple_route,
        )
        .await;
    }
    if arguments.get("google_route").is_some() {
        return crate::google_workout_tcx::query_route(
            pool,
            subject,
            args::<GoogleRouteRequest>(arguments)?.google_route,
        )
        .await;
    }
    if arguments.get("samples").is_some() {
        return crate::garmin_workouts::query(
            pool,
            subject,
            args::<WorkoutSamplesRequest>(arguments)?.samples,
        )
        .await;
    }
    wearables::records_for(pool, subject, args(arguments)?, true).await
}

pub fn list(surface: Surface) -> Vec<Value> {
    let string = json!({"type": "string"});
    let timestamp = json!({"type": "string", "format": "date-time"});
    let mut water_today = tool(
        "diary_water_today",
        "Read the sum and count of confirmed water diary entries for today in the operator's time zone. An empty result means no saved entries.",
        json!({}),
        &[],
    );
    water_today["annotations"] = json!({"readOnlyHint": true});
    let mut tools = match surface {
        Surface::Read => vec![
            tool("medication_candidates", "Find candidate synthetic products. A candidate requires item code confirmation.",
                 json!({"name": string}), &["name"]),
            tool("check_dur", "Check a confirmed product code against the synthetic current regimen. A no-match is scoped to the fixture.",
                 json!({"item_seq": string}), &[]),
            tool("health_status_v1", "Return current health facts needed by the OpenHuman scheduler. This read never creates or queues a reminder.", json!({}), &[]),
            tool("hsm_state_v1", "Read the versioned HSM state projection and journal sequence for this person.", json!({}), &[]),
            tool("get_plan_actual_links", "Internal adapter projection of scheduled dose references and recorded response references. Payload details remain in HSM.", json!({"start":timestamp,"end":timestamp}), &["start","end"]),
            tool("sleep_sessions", "List completed synthetic sleep records.", json!({}), &[]),
            tool("diary_entries", "Read confirmed synthetic meal, water or strength entries by recorded time. End is exclusive. Narrow the range if truncated.",
                 json!({"start": timestamp, "end": timestamp, "kind": {"enum": ["meal", "water", "strength"]}}), &["start", "end"]),
            tool("nutrition_daily", "Read HSM local-day energy and macro totals from confirmed meals, Health Connect or Samsung meal records, and HealthKit nutrient samples; late entries, edits, deletions and intake-date changes rebuild affected days.",
                 json!({"start": timestamp, "end": timestamp}), &["start", "end"]),
            water_today,
            tool("device_status", "Read the connection and permission status for one provider (default: Health Connect).", json!({"provider":string}), &[]),
            tool("cloud_device_status", "Read cloud device connection status.", json!({"provider":string}), &["provider"]),
            tool("food_catalog", "Search public food catalog by barcode or food name.", json!({"query":string}), &["query"]),
            tool("medication_catalog", "Search MFDS e약은요 public consumer medication information. This is general product information, not an individual prescription or dosage decision.", json!({"query":string}), &["query"]),
            tool("wearable_samples", "Read steps, heart-rate samples or provider metrics. Use metric to filter a named provider measurement.",
                 json!({"kind": {"enum":["steps","heart_rate","metric"]}, "start": timestamp, "end": timestamp, "provider": string, "source": string, "metric":string, "offset":{"type":"integer"}}), &["kind","start","end"]),
            tool("wearable_daily", "Read daily steps (default), heart rate, or provider activity totals over an inclusive local date range.",
                 json!({"start_day": string,"end_day": string,"provider":string,"source":string,"kind":{"enum":["steps","heart_rate","activity"]}}), &["start_day","end_day"]),
            {
                let detail_query = json!({"type":"object","properties":{"source":string,"source_id":string,
                    "offset":{"type":"integer","minimum":0},"limit":{"type":"integer","minimum":1,"maximum":100}},
                    "required":["source","source_id"],"additionalProperties":false});
                let mut route_query = detail_query.clone();
                route_query["properties"]["provider"] = json!({"enum":["samsung_health","health_connect","healthkit"]});
                let mut google_query = detail_query.clone();
                google_query["properties"]["section"] = json!({"enum":["route","telemetry"],"default":"route"});
                let mut sessions = tool("wearable_sessions", "Read completed sleep and workout sessions. Use route for Samsung, Health Connect, or HealthKit GPS, apple_route for Apple Health export GPS, google_route for Google Health GPS or TCX telemetry, or samples for Garmin workout measurements, by source and source_id.",
                    json!({"kind":{"enum":["sleep","workout"]},"start":timestamp,"end":timestamp,"provider":string,"source":string,"offset":{"type":"integer"},
                      "route":route_query,"apple_route":detail_query,"google_route":google_query,"samples":detail_query}), &[]);
                sessions["inputSchema"]["oneOf"] = json!([
                    {"required":["kind","start","end"],"not":{"anyOf":[{"required":["route"]},{"required":["apple_route"]},{"required":["google_route"]},{"required":["samples"]}]}},
                    {"required":["route"],"maxProperties":1},
                    {"required":["apple_route"],"maxProperties":1},
                    {"required":["google_route"],"maxProperties":1},
                    {"required":["samples"],"maxProperties":1}
                ]);
                sessions
            },
            tool("wearable_fit_details", "Page through FIT sessions, laps, splits and summaries, climb events, swim lengths, sensor samples, heart rate, untimestamped HRV, dive summaries, zone reports, devices, or strength sets and exercise titles. Records may include source-defined developer fields with their original names and units.",
                 json!({"provider":string,"source_id":string,"part":{"type":"integer"},"section":{"enum":["sessions","laps","lengths","samples","zones","dives","events","devices","hrv","compressed_heart_rate","sets","exercise_titles","splits","split_summaries","climb_events"]},"offset":{"type":"integer"},"limit":{"type":"integer","minimum":1,"maximum":100}}), &["provider","source_id"]),
            tool("wearable_goals", "Read current Samsung Health targets or device defaults. Values are goals, not recorded activity.",
                 json!({}), &[]),
            tool("polar_physical_info", "Read the current Polar physical profile, including body measurements, heart rate settings and training settings.", json!({}), &[]),
        ],
        Surface::Control => vec![
            schedule_tool(),
            tool(
                "connect_cloud_oauth",
                "Start linking your cloud health account in a browser.",
                json!({"provider":string}),
                &["provider"],
            ),
            tool("request_oura_due_sync", "Queue one UTC hourly Oura sync slot requested by OpenHuman Cron. Duplicate slots are ignored.", json!({"slot_at": timestamp}), &["slot_at"]),
            tool("sync_cloud_device", "Import from a provisioned cloud wearable account.", json!({"provider":string}), &["provider"]),
            tool("revoke_cloud_device", "Remove one cloud provider's imported records.", json!({"provider":string}), &["provider"]),
            tool("issue_health_connect_lease", "Authorize a mobile health snapshot. A continuation requires the same active connection generation.", json!({"provider":string,"require_active":{"type":"boolean"},"expected_generation":string}), &[]),
            tool("commit_health_connect_snapshot", "Commit the authorized mobile device snapshot.",
                 json!({"provider":string,"lease": string,"start": timestamp,"end": timestamp,"historical":{"type":"boolean"},"grants":{"type":"array"},
                        "metric_grants":{"type":"array","items":string},"metric_grants_version":{"type":"integer"},
                        "samsung_activity":{"type":"object"},"samsung_routes":{"type":"object"},"health_connect_routes":{"type":"object"},"healthkit_routes":{"type":"object"},
                        "samsung_goals":{"type":"object"},
                        "samsung_sleep_deletions":{"type":"array","items":{"type":"object",
                            "properties":{"uid":string,"changed_at":timestamp},"required":["uid","changed_at"]}},
                        "samsung_sleep_revision":{"type":"object",
                            "properties":{"uid":string,"changed_at":timestamp},"required":["uid","changed_at"]},
                        "samsung_session_changes":{"type":"object",
                            "properties":{"kind":{"enum":["sleep","workout","heart_rate","metric"]},"deletions":{"type":"array"},"revision":{"type":"object"},"revisions":{"type":"array"}},"required":["kind"]},
                        "records":{"type":"array"}}), &["lease","start","end","grants","records"]),
            tool("revoke_health_connect", "Disconnect the specified mobile health provider and remove its imported records.", json!({"provider":string}), &[]),
            tool("confirm_diary_entry", "Save a diary entry explicitly confirmed by the operator.",
                 json!({"id": string, "entry": {"type": "object"}}), &["id", "entry"]),
            tool("change_diary_entry", "Update the confirmed diary entry at the specified revision. Omit entry to delete it.",
                 json!({"id": string,"revision":{"type":"integer"},"entry":{"type":"object"}}), &["id","revision"]),
            tool("record_medication_response_v1", "Record an explicitly user-approved response for an existing dose. Replays of the same response are idempotent.",
                 json!({"dose_id": string, "response": {"enum": ["taken", "skipped"]},
                        "responded_at": {"type":"string", "format":"date-time",
                            "description":"Durable Workflow decision acceptance time; omit for a direct current-time response."}}),
                 &["dose_id", "response"]),
            tool("store_sleep_session", "Store a completed synthetic session.",
                 json!({"id": string, "started_at": timestamp, "completed_at": timestamp}),
                 &["id", "started_at", "completed_at"]),
        ],
    };
    if surface == Surface::Read {
        for entry in &mut tools {
            entry["annotations"] = json!({"readOnlyHint": true});
        }
    }
    tools
}

fn args<T: DeserializeOwned>(value: &Value) -> Result<T> {
    serde_json::from_value(value.clone()).map_err(|_| anyhow::anyhow!("invalid tool arguments"))
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Candidate {
    name: String,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Confirmed {
    item_seq: Option<String>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct DoseReply {
    dose_id: String,
    response: String,
    #[serde(default)]
    responded_at: Option<DateTime<Utc>>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Sleep {
    id: String,
    started_at: DateTime<Utc>,
    completed_at: DateTime<Utc>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Signal {
    event_id: String,
    kind: String,
    observed_at: DateTime<Utc>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct CheckIn {
    event_id: String,
}

fn valid_id(value: &str) -> Result<&str> {
    if !(1..=80).contains(&value.len())
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || b"-_".contains(&byte))
    {
        bail!("invalid record identifier");
    }
    Ok(value)
}

fn no_args(value: &Value) -> Result<()> {
    if !value.is_null() && !value.as_object().is_some_and(|fields| fields.is_empty()) {
        bail!("this tool accepts no arguments");
    }
    Ok(())
}

pub async fn call(app: &App, surface: Surface, name: &str, arguments: &Value) -> Result<Value> {
    match (surface, name) {
        (Surface::Read, "cloud_device_status") => {
            let provider = args::<cloud_wearables::Request>(arguments)?.provider;
            let mut status = wearable_sync::status(&app.db, store::SUBJECT, provider).await?;
            status["configured"] = json!(cloud_wearables::configured(provider).is_some());
            return Ok(status);
        }
        (Surface::Control, "schedule_cloud_sync") => {
            return wearable_sync::schedule(&app.db, store::SUBJECT, args(arguments)?).await
        }
        (Surface::Control, "sync_cloud_device") => {
            return wearable_sync::sync_now(
                &app.db,
                store::SUBJECT,
                args::<cloud_wearables::Request>(arguments)?.provider,
            )
            .await;
        }
        (Surface::Control, "request_oura_due_sync") => {
            let slot = arguments
                .get("slot_at")
                .and_then(Value::as_str)
                .ok_or_else(|| anyhow::anyhow!("missing Oura UTC slot"))?;
            let slot_at = DateTime::parse_from_rfc3339(slot)?.with_timezone(&Utc);
            let queued =
                wearable_sync::request_oura_due_sync(&app.db, store::SUBJECT, slot_at).await?;
            return Ok(
                json!({"state": if queued { "queued" } else { "already_queued_or_disconnected" }, "slot_at": slot_at}),
            );
        }
        (Surface::Control, "revoke_cloud_device") => {
            return wearables::revoke(
                &app.db,
                args::<cloud_wearables::Request>(arguments)?.provider.slug(),
            )
            .await;
        }
        (Surface::Read, "food_catalog") => {
            return food::lookup(&app.db, args(arguments)?)
                .await
                .map_err(|_| anyhow::anyhow!("food catalog unavailable"))
        }
        (Surface::Read, "medication_catalog") => {
            return drug::lookup(&app.db, args(arguments)?)
                .await
                .map_err(|_| anyhow::anyhow!("medication catalog unavailable"))
        }
        (Surface::Read, "device_status") => {
            let provider = if arguments.is_null() {
                wearables::DeviceSource::default()
            } else {
                args(arguments)?
            };
            return wearables::status(&app.db, provider.mobile_provider()?).await;
        }
        (Surface::Read, "wearable_samples") => {
            return wearables::records(&app.db, args(arguments)?, false).await;
        }
        (Surface::Read, "wearable_sessions") => {
            return sessions(&app.db, store::SUBJECT, arguments).await
        }
        (Surface::Read, "wearable_daily") => {
            return wearables::daily(&app.db, args(arguments)?).await
        }
        (Surface::Read, "wearable_fit_details") => {
            return crate::wearable_fit_details::query(&app.db, store::SUBJECT, args(arguments)?)
                .await;
        }
        (Surface::Read, "wearable_goals") => {
            return crate::samsung_goals::query(&app.db, store::SUBJECT).await
        }
        (Surface::Read, "polar_physical_info") => {
            return crate::wearable_webhook_documents::current_polar_physical_info(
                &app.db,
                store::SUBJECT,
            )
            .await
        }
        (Surface::Control, "issue_health_connect_lease") => {
            let provider = if arguments.is_null() {
                wearables::DeviceSource::default()
            } else {
                args(arguments)?
            };
            let expected = provider
                .expected_generation
                .as_deref()
                .map(str::parse::<i64>)
                .transpose()
                .context("invalid device connection generation")?;
            ensure!(
                expected.is_none_or(|value| provider.require_active && value > 0),
                "a positive generation requires an active connection"
            );
            return if provider.require_active {
                wearables::issue_active_lease(&app.db, provider.mobile_provider()?, expected).await
            } else {
                wearables::issue_lease(&app.db, provider.mobile_provider()?).await
            };
        }
        (Surface::Control, "commit_health_connect_snapshot") => {
            let snapshot: wearables::Snapshot = args(arguments)?;
            return wearables::commit(&app.db, snapshot.mobile_provider()?, snapshot).await;
        }
        (Surface::Control, "revoke_health_connect") => {
            let provider = if arguments.is_null() {
                wearables::DeviceSource::default()
            } else {
                args(arguments)?
            };
            return wearables::revoke(&app.db, provider.mobile_provider()?).await;
        }
        _ => {}
    }
    let now = Utc::now();
    let db = match app.db.get().await {
        Ok(db) => db,
        Err(_) if surface == Surface::Read && name == "check_dur" => {
            return Ok(json!({ "status": "lookup_failed", "synthetic": true }));
        }
        Err(_) => bail!("HSM storage unavailable"),
    };
    match (surface, name) {
        (Surface::Read, "medication_candidates") => {
            store::medication_candidates(&db, &args::<Candidate>(arguments)?.name).await
        }
        (Surface::Read, "check_dur") => {
            let item = if arguments.is_null() {
                None
            } else {
                args::<Confirmed>(arguments)?.item_seq
            };
            Ok(store::dur(&db, item.as_deref()).await)
        }
        (Surface::Read, "health_status_v1") => {
            no_args(arguments)?;
            store::health_status_v1(&db, now).await
        }
        (Surface::Read, "hsm_state_v1") => {
            no_args(arguments)?;
            Ok(
                crate::state_owner::current_state(&app.db, crate::store::SUBJECT)
                    .await?
                    .map(|(sequence, state)| json!({"sequence": sequence, "state": state}))
                    .unwrap_or_else(|| json!({"sequence": 0, "state": {"streams": {}}})),
            )
        }
        (Surface::Read, "sleep_sessions") => {
            no_args(arguments)?;
            store::sleep_sessions(&db).await
        }
        (Surface::Read, "diary_entries") => diary::read(&db, args(arguments)?).await,
        (Surface::Read, "nutrition_daily") => diary::nutrition_summary(&db, args(arguments)?).await,
        (Surface::Read, "diary_water_today") => {
            no_args(arguments)?;
            diary::water_today(&db).await
        }
        (Surface::Control, "confirm_diary_entry") => {
            let entry: diary::ConfirmedEntry = args(arguments)?;
            valid_id(&entry.id)?;
            diary::save(&app.db, entry).await
        }
        (Surface::Control, "change_diary_entry") => {
            let change: diary::DiaryChange = args(arguments)?;
            valid_id(&change.id)?;
            diary::change(&app.db, change).await
        }
        (Surface::Control, "record_medication_response_v1") => {
            let response: DoseReply = args(arguments)?;
            let dose_id = valid_id(&response.dose_id)?;
            let envelope = watch_tower_core::records::RecordEnvelope {
                subject_id: store::SUBJECT.to_owned(),
                actor_id: "openhuman:medication-workflow".to_owned(),
                adapter_id: "hsm".to_owned(),
                source_id: "openhuman-workflow".to_owned(),
                record_id: dose_id.to_owned(),
                revision: 1,
                run_id: None,
                observed_at_unix: now.timestamp(),
                idempotency_key: format!("dose-response:{dose_id}"),
            };
            if !envelope.is_valid() {
                bail!("medication response is missing common provenance fields");
            }
            store::respond_v1_for_at(
                &app.db,
                store::SUBJECT,
                dose_id,
                &response.response,
                response.responded_at.unwrap_or(now),
            )
            .await
        }
        (Surface::Control, "store_sleep_session") => {
            let sleep: Sleep = args(arguments)?;
            store::save_sleep(
                &app.db,
                valid_id(&sleep.id)?,
                sleep.started_at,
                sleep.completed_at,
            )
            .await
        }
        _ => bail!("tool is unavailable on this MCP surface"),
    }
}

#[cfg(test)]
#[path = "mcp_tests.rs"]
mod tests;
