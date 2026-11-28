use std::sync::Arc;

use axum::{
    extract::{Path, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    Json,
};
use chrono::{DateTime, NaiveDate, Utc};
use serde_json::{json, Value};
use subtle::ConstantTimeEq;

use crate::store::{
    ConfirmedWorkout, FitnessStore, ManualWorkout, ProfileSettings, TrainingPlanDraft,
    TrainingPlanInput, WorkoutSetInput,
};
use crate::workout_csv;

pub struct App {
    pub store: FitnessStore,
    pub owner_id: String,
    pub read_token: String,
    pub write_token: String,
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum Surface {
    Read,
    Write,
}

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct PlanActualLinksQuery {
    start: DateTime<Utc>,
    end: DateTime<Utc>,
}

pub async fn handle(
    State(app): State<Arc<App>>,
    Path(surface): Path<String>,
    headers: HeaderMap,
    Json(message): Json<Value>,
) -> Response {
    let (surface, expected) = match surface.as_str() {
        "read" => (Surface::Read, &app.read_token),
        "write" => (Surface::Write, &app.write_token),
        _ => return StatusCode::NOT_FOUND.into_response(),
    };
    if headers.get("Origin").is_some() {
        return StatusCode::FORBIDDEN.into_response();
    }
    let supplied = headers
        .get("Authorization")
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "))
        .unwrap_or_default();
    if !bool::from(expected.as_bytes().ct_eq(supplied.as_bytes())) {
        return StatusCode::UNAUTHORIZED.into_response();
    }

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

    let result = match method {
        "initialize" => {
            let offered = message["params"]["protocolVersion"]
                .as_str()
                .unwrap_or_default();
            let version = match offered {
                "2025-03-26" | "2025-06-18" | "2025-11-25" => offered,
                _ => "2025-06-18",
            };
            json!({
                "protocolVersion": version,
                "capabilities": { "tools": {} },
                "serverInfo": { "name": "avatar-fitness", "version": env!("CARGO_PKG_VERSION") }
            })
        }
        "ping" => json!({}),
        "tools/list" => json!({ "tools": tools(surface) }),
        "tools/call" => {
            let params = &message["params"];
            let Some(name) = params["name"].as_str() else {
                return rpc_error(id, -32602, "Missing tool name");
            };
            match call(&app, surface, name, &params["arguments"]).await {
                Ok(value) => json!({
                    "content": [{ "type": "text", "text": value.to_string() }],
                    "structuredContent": value
                }),
                Err(error) => json!({
                    "isError": true,
                    "content": [{ "type": "text", "text": error }]
                }),
            }
        }
        "notifications/initialized" => json!({}),
        _ => return rpc_error(id, -32601, "Method not found"),
    };
    Json(json!({ "jsonrpc": "2.0", "id": id, "result": result })).into_response()
}

fn rpc_error(id: Value, code: i32, message: &str) -> Response {
    Json(json!({
        "jsonrpc": "2.0",
        "id": id,
        "error": { "code": code, "message": message }
    }))
    .into_response()
}

fn tool(name: &str, description: &str, properties: Value, required: &[&str]) -> Value {
    json!({
        "name": name,
        "description": description,
        "inputSchema": {
            "type": "object",
            "properties": properties,
            "required": required,
            "additionalProperties": false
        }
    })
}

pub fn tools(surface: Surface) -> Vec<Value> {
    match surface {
        Surface::Read => vec![
            tool(
                "get_plan_actual_links",
                "Internal reference-only plan and actual projection for Watch Tower.",
                json!({"start":{"type":"string","format":"date-time"},"end":{"type":"string","format":"date-time"}}),
                &["start","end"],
            ),
            tool(
                "preview_workout_csv",
                "Preview Strong or Hevy workout CSV import, including duplicates and skipped rows.",
                workout_csv_schema(),
                &["csv", "timezone"],
            ),
            tool(
                "get_workout_day",
                "Read strength sets for one local date.",
                json!({ "date": { "type": "string", "format": "date" }, "timezone": { "type": "string", "minLength": 1, "maxLength": 64 }, "offset": { "type":"integer", "minimum":0, "maximum":1000000, "default":0 } }),
                &["date", "timezone"],
            ),
            tool("get_training_context", "Read saved training plans and profile settings.", json!({"plan_id":{"type":"string","pattern":"^[a-fA-F0-9]{32}$"}}), &[]),
            tool("get_training_progress", "Read daily work, personal bests and recent workouts for one strength movement.", json!({
                "exercise_id":{"type":"string","enum":["squat","bench_press","deadlift","push_up"],"default":"squat"},
                "variant_id":{"type":"string","enum":["bodyweight","barbell"]},
                "timezone":{"type":"string","minLength":1,"maxLength":64},
                "start_date":{"type":"string","format":"date"},"end_date":{"type":"string","format":"date"},
                "include_lifetime":{"type":"boolean","default":false}
            }), &["variant_id","timezone"]),
        ],
        Surface::Write => vec![
            tool(
                "import_workout_csv",
                "Import Strong or Hevy workout CSV into Fitness with idempotency and cross-source deduplication.",
                workout_csv_schema(),
                &["csv", "timezone"],
            ),
            tool(
                "record_confirmed_workout",
                "Persist an OpenHuman Workflow approved workout. The Workflow run ID is the idempotency key; no confirmation table is created.",
                json!({
                    "workflow_run_id": { "type": "string", "minLength": 1, "maxLength": 128 },
                    "performed_at": { "type": "string", "format": "date-time" },
                    "timezone": { "type": "string", "minLength": 1, "maxLength": 64 },
                    "sets": workout_sets_schema()
                }),
                &["workflow_run_id", "performed_at", "timezone", "sets"],
            ),
            tool("record_manual_workout", "Record a user-entered workout with an idempotent request key.", json!({
                "request_key":{"type":"string","pattern":"^[a-fA-F0-9]{32}$"},
                "performed_at":{"type":"string","format":"date-time"},
                "timezone":{"type":"string","minLength":1,"maxLength":64},
                "sets":workout_sets_schema()
            }), &["request_key","performed_at","timezone","sets"]),
            tool("replace_workout_set", "Replace a workout set only at its current revision.", json!({
                "set_id":{"type":"string","pattern":"^[a-fA-F0-9]{32}$"},
                "expected_revision":{"type":"integer","minimum":1},
                "replacement":workout_set_schema()
            }), &["set_id","expected_revision","replacement"]),
            tool("delete_workout_set", "Soft-delete a workout set only at its current revision.", json!({
                "set_id":{"type":"string","pattern":"^[a-fA-F0-9]{32}$"},
                "expected_revision":{"type":"integer","minimum":1}
            }), &["set_id","expected_revision"]),
            tool("create_training_plan", "Create an idempotent Fitness training plan.", json!({
                "request_key": {"type":"string","pattern":"^[a-fA-F0-9]{32}$"},
                "title": {"type":"string","minLength":1,"maxLength":120},
                "day": {"type":"string","format":"date"},
                "timezone": {"type":"string","minLength":1,"maxLength":64},
                "targets": {"type":"array","minItems":1,"maxItems":30,"items": {
                    "type":"object","additionalProperties":false,
                    "properties": {
                        "exercise_id":{"type":"string","enum":["squat","bench_press","deadlift","push_up"]},
                        "variant_id":{"type":"string","enum":["bodyweight","barbell"]},
                        "target_reps":{"type":"integer","minimum":1,"maximum":1000},
                        "rest_seconds":{"type":"integer","minimum":0,"maximum":1800},
                        "target_load_kg":{"type":["number","null"],"exclusiveMinimum":0,"maximum":2000}
                    },"required":["exercise_id","variant_id","target_reps"]
                }}
            }), &["request_key","title","day","timezone","targets"]),
            tool("revise_training_plan", "Revise a plan with optimistic revision control.", json!({
                "plan_id":{"type":"string","pattern":"^[a-fA-F0-9]{32}$"},
                "expected_revision":{"type":"integer","minimum":1},"draft":training_plan_schema()
            }), &["plan_id","expected_revision","draft"]),
            tool("delete_training_plan", "Delete a plan and its plan-to-performance links at its current revision.", json!({
                "plan_id":{"type":"string","pattern":"^[a-fA-F0-9]{32}$"},"expected_revision":{"type":"integer","minimum":1}
            }), &["plan_id","expected_revision"]),
            tool("save_training_profile", "Create or update the training profile at its current revision.", json!({
                "expected_revision":{"type":"integer","minimum":0},"settings":profile_schema()
            }), &["expected_revision","settings"]),
            tool("delete_training_profile", "Delete profile settings at their current revision.", json!({
                "expected_revision":{"type":"integer","minimum":1}
            }), &["expected_revision"]),
            tool("link_training_execution", "Link a qualifying workout set to a plan target, or pass set_id null to unlink it.", json!({
                "plan_id":{"type":"string","pattern":"^[a-fA-F0-9]{32}$"},
                "expected_revision":{"type":"integer","minimum":1},"target_position":{"type":"integer","minimum":1,"maximum":30},
                "set_id":{"type":["string","null"],"pattern":"^[a-fA-F0-9]{32}$"}
            }), &["plan_id","expected_revision","target_position","set_id"]),
        ],
    }
}

fn workout_csv_schema() -> Value {
    json!({"type":"object","additionalProperties":false,"properties":{
        "csv":{"type":"string","minLength":1,"maxLength":4194304},
        "timezone":{"type":"string","minLength":1,"maxLength":64},
        "strong_weight_unit":{"type":"string","enum":["kg","lb"]},
        "barbell_total_confirmed":{"type":"boolean","default":false}
    }})
}

fn workout_sets_schema() -> Value {
    json!({ "type":"array", "minItems":1, "maxItems":30, "items":workout_set_schema() })
}

fn training_plan_schema() -> Value {
    json!({"type":"object","additionalProperties":false,"properties":{
        "title":{"type":"string","minLength":1,"maxLength":120},"day":{"type":"string","format":"date"},
        "timezone":{"type":"string","minLength":1,"maxLength":64},"targets":{"type":"array","minItems":1,"maxItems":30,"items":{
            "type":"object","additionalProperties":false,"properties":{
                "exercise_id":{"type":"string","enum":["squat","bench_press","deadlift","push_up"]},
                "variant_id":{"type":"string","enum":["bodyweight","barbell"]},"target_reps":{"type":"integer","minimum":1,"maximum":1000},
                "rest_seconds":{"type":"integer","minimum":0,"maximum":1800},"target_load_kg":{"type":["number","null"],"exclusiveMinimum":0,"maximum":2000}
            },"required":["exercise_id","variant_id","target_reps"]}
        }},"required":["title","day","timezone","targets"]})
}

fn profile_schema() -> Value {
    json!({"type":"object","additionalProperties":false,"properties":{
        "goal":{"type":"string","minLength":1,"maxLength":120},
        "experience":{"type":"string","enum":["beginner","intermediate","advanced"]},
        "equipment":{"type":"array","maxItems":2,"items":{"type":"string","enum":["bodyweight","barbell"]}},
        "days_per_week":{"type":"integer","minimum":1,"maximum":7}
    },"required":["goal","experience","equipment","days_per_week"]})
}

fn workout_set_schema() -> Value {
    json!({ "type":"object", "additionalProperties":false, "properties": {
        "exercise_id":{"type":"string","enum":["squat","bench_press","deadlift","push_up"]},
        "variant_id":{"type":"string","enum":["bodyweight","barbell"]},
        "reps":{"type":"integer","minimum":0,"maximum":1000},
        "load_kg":{"type":["number","null"],"exclusiveMinimum":0,"maximum":2000},
        "load_mode":{"type":["string","null"],"enum":["barbell_total",null]},
        "rpe":{"type":["number","null"],"minimum":1,"maximum":10},
        "set_type":{"type":"string","enum":["work","warmup","drop","failure"],"default":"work"},
        "plan_id":{"type":"string","pattern":"^[a-fA-F0-9]{32}$"},
        "plan_revision":{"type":"integer","minimum":1},
        "target_position":{"type":"integer","minimum":1,"maximum":30}
    }, "required":["exercise_id","variant_id","reps"] })
}

pub async fn call(
    app: &App,
    surface: Surface,
    name: &str,
    arguments: &Value,
) -> Result<Value, String> {
    match (surface, name) {
        (Surface::Read, "get_plan_actual_links") => {
            let query: PlanActualLinksQuery = serde_json::from_value(arguments.clone())
                .map_err(|_| "invalid plan/actual range".to_owned())?;
            app.store
                .plan_actual_links(&app.owner_id, query.start, query.end)
                .await
                .map_err(|_| "Fitness plan/actual references unavailable".to_owned())
        }
        (Surface::Read, "preview_workout_csv") | (Surface::Write, "import_workout_csv") => {
            let raw = arguments["csv"]
                .as_str()
                .ok_or_else(|| "csv is required".to_owned())?;
            let timezone = arguments["timezone"]
                .as_str()
                .ok_or_else(|| "timezone is required".to_owned())?;
            let unit = arguments["strong_weight_unit"].as_str();
            let total = arguments["barbell_total_confirmed"]
                .as_bool()
                .unwrap_or(false);
            let batch = workout_csv::parse(raw, timezone, unit, total)
                .map_err(|_| "invalid or unsupported Strong/Hevy CSV".to_owned())?;
            if name == "preview_workout_csv" {
                app.store
                    .preview_workout_csv(&app.owner_id, &batch)
                    .await
                    .map_err(|_| "Fitness CSV preview unavailable".to_owned())
            } else {
                app.store
                    .import_workout_csv(&app.owner_id, &batch)
                    .await
                    .map_err(|_| "Fitness CSV import failed".to_owned())
            }
        }
        (Surface::Read, "get_workout_day") => {
            let date = arguments["date"]
                .as_str()
                .ok_or_else(|| "date is required".to_owned())?
                .parse::<NaiveDate>()
                .map_err(|_| "date must use YYYY-MM-DD".to_owned())?;
            let timezone = arguments["timezone"]
                .as_str()
                .ok_or_else(|| "timezone is required".to_owned())?;
            let offset = arguments["offset"].as_i64().unwrap_or(0);
            app.store
                .workout_day(&app.owner_id, date, timezone, offset)
                .await
                .map_err(|_| "Fitness storage unavailable or request invalid".to_owned())
        }
        (Surface::Read, "get_training_context") => app
            .store
            .training_context(&app.owner_id, arguments["plan_id"].as_str())
            .await
            .map_err(|_| "Fitness training context unavailable".to_owned()),
        (Surface::Read, "get_training_progress") => {
            let exercise = arguments["exercise_id"].as_str().unwrap_or("squat");
            let variant = arguments["variant_id"]
                .as_str()
                .ok_or_else(|| "variant_id is required".to_owned())?;
            let timezone = arguments["timezone"]
                .as_str()
                .ok_or_else(|| "timezone is required".to_owned())?;
            let start = arguments["start_date"]
                .as_str()
                .map(str::parse::<NaiveDate>)
                .transpose()
                .map_err(|_| "start_date must use YYYY-MM-DD".to_owned())?;
            let end = arguments["end_date"]
                .as_str()
                .map(str::parse::<NaiveDate>)
                .transpose()
                .map_err(|_| "end_date must use YYYY-MM-DD".to_owned())?;
            let lifetime = arguments["include_lifetime"].as_bool().unwrap_or(false);
            app.store
                .training_progress(
                    &app.owner_id,
                    exercise,
                    variant,
                    timezone,
                    start,
                    end,
                    lifetime,
                )
                .await
                .map_err(|_| "Fitness progress unavailable or request invalid".to_owned())
        }
        (Surface::Write, "record_confirmed_workout") => {
            let request: ConfirmedWorkout = serde_json::from_value(arguments.clone())
                .map_err(|_| "invalid confirmed workout arguments".to_owned())?;
            let receipt = app
                .store
                .record_confirmed_workout(&app.owner_id, &request)
                .await
                .map_err(|_| "Fitness workout could not be recorded".to_owned())?;
            serde_json::to_value(receipt)
                .map_err(|_| "Fitness receipt serialization failed".to_owned())
        }
        (Surface::Write, "record_manual_workout") => {
            let request: ManualWorkout = serde_json::from_value(arguments.clone())
                .map_err(|_| "invalid manual workout arguments".to_owned())?;
            let receipt = app
                .store
                .record_manual_workout(&app.owner_id, &request)
                .await
                .map_err(|_| "Fitness workout could not be recorded".to_owned())?;
            serde_json::to_value(receipt)
                .map_err(|_| "Fitness receipt serialization failed".to_owned())
        }
        (Surface::Write, "replace_workout_set") | (Surface::Write, "delete_workout_set") => {
            let set_id = arguments["set_id"]
                .as_str()
                .ok_or_else(|| "set_id is required".to_owned())?;
            let revision = arguments["expected_revision"]
                .as_i64()
                .and_then(|v| i32::try_from(v).ok())
                .ok_or_else(|| "expected_revision must be a positive integer".to_owned())?;
            let replacement = if name == "replace_workout_set" {
                Some(
                    serde_json::from_value::<WorkoutSetInput>(arguments["replacement"].clone())
                        .map_err(|_| "invalid replacement workout set".to_owned())?,
                )
            } else {
                None
            };
            app.store
                .change_workout_set(&app.owner_id, set_id, revision, replacement.as_ref())
                .await
                .map_err(|_| "workout set is unavailable or its revision changed".to_owned())
        }
        (Surface::Write, "create_training_plan") => {
            let request: TrainingPlanInput = serde_json::from_value(arguments.clone())
                .map_err(|_| "invalid training plan arguments".to_owned())?;
            app.store
                .create_training_plan(&app.owner_id, &request)
                .await
                .map_err(|_| "Fitness training plan could not be recorded".to_owned())
        }
        (Surface::Write, "revise_training_plan") => {
            let plan_id = arguments["plan_id"]
                .as_str()
                .ok_or_else(|| "plan_id is required".to_owned())?;
            let revision = arguments["expected_revision"]
                .as_i64()
                .ok_or_else(|| "expected_revision is required".to_owned())?;
            let draft: TrainingPlanDraft = serde_json::from_value(arguments["draft"].clone())
                .map_err(|_| "invalid training plan draft".to_owned())?;
            app.store
                .revise_training_plan(&app.owner_id, plan_id, revision, &draft)
                .await
                .map_err(|_| "training plan is missing or its revision changed".to_owned())
        }
        (Surface::Write, "delete_training_plan") => {
            let plan_id = arguments["plan_id"]
                .as_str()
                .ok_or_else(|| "plan_id is required".to_owned())?;
            let revision = arguments["expected_revision"]
                .as_i64()
                .ok_or_else(|| "expected_revision is required".to_owned())?;
            app.store
                .delete_training_plan(&app.owner_id, plan_id, revision)
                .await
                .map_err(|_| "training plan is missing or its revision changed".to_owned())
        }
        (Surface::Write, "save_training_profile") => {
            let revision = arguments["expected_revision"]
                .as_i64()
                .ok_or_else(|| "expected_revision is required".to_owned())?;
            let settings: ProfileSettings = serde_json::from_value(arguments["settings"].clone())
                .map_err(|_| "invalid training profile".to_owned())?;
            app.store
                .save_profile(&app.owner_id, revision, Some(&settings))
                .await
                .map_err(|_| "training profile is invalid or its revision changed".to_owned())
        }
        (Surface::Write, "delete_training_profile") => {
            let revision = arguments["expected_revision"]
                .as_i64()
                .ok_or_else(|| "expected_revision is required".to_owned())?;
            app.store
                .save_profile(&app.owner_id, revision, None)
                .await
                .map_err(|_| "training profile is missing or its revision changed".to_owned())
        }
        (Surface::Write, "link_training_execution") => {
            let plan_id = arguments["plan_id"]
                .as_str()
                .ok_or_else(|| "plan_id is required".to_owned())?;
            let revision = arguments["expected_revision"]
                .as_i64()
                .ok_or_else(|| "expected_revision is required".to_owned())?;
            let position = arguments["target_position"]
                .as_i64()
                .and_then(|v| i32::try_from(v).ok())
                .ok_or_else(|| "target_position is invalid".to_owned())?;
            let set_id = arguments["set_id"].as_str();
            app.store
                .link_training_execution(&app.owner_id, plan_id, revision, position, set_id)
                .await
                .map_err(|_| {
                    "plan execution is unavailable or conflicts with the target".to_owned()
                })
        }
        _ => Err("tool is not available on this MCP surface".to_owned()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn read_and_write_tools_are_separated_by_surface() {
        let read: Vec<String> = tools(Surface::Read)
            .iter()
            .map(|tool| tool["name"].as_str().unwrap().to_owned())
            .collect();
        let write: Vec<String> = tools(Surface::Write)
            .iter()
            .map(|tool| tool["name"].as_str().unwrap().to_owned())
            .collect();
        assert_eq!(
            read,
            [
                "preview_workout_csv",
                "get_workout_day",
                "get_training_context",
                "get_training_progress"
            ]
        );
        assert_eq!(
            write,
            [
                "import_workout_csv",
                "record_confirmed_workout",
                "record_manual_workout",
                "replace_workout_set",
                "delete_workout_set",
                "create_training_plan",
                "revise_training_plan",
                "delete_training_plan",
                "save_training_profile",
                "delete_training_profile",
                "link_training_execution"
            ]
        );
    }
}
