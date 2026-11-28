use std::sync::Arc;

use anyhow::Context;
use axum::{
    http::{header::WWW_AUTHENTICATE, HeaderMap, HeaderValue, StatusCode},
    response::{IntoResponse, Response},
    Json,
};
use deadpool_postgres::Pool;
use serde_json::{json, Value};
use std::{
    net::SocketAddr,
    time::{SystemTime, UNIX_EPOCH},
};
use watch_tower_core::policy::AuditEvent;
use watch_tower_core::records::RecordEnvelope;

use crate::auth::{Authenticator, Principal};
use crate::store;
use avatar_fitness::mcp::{self as fitness_mcp, App as FitnessApp, Surface as FitnessSurface};
use avatar_hsm::mcp::{self, App as HsmApp, Surface as HsmSurface};

pub async fn handle(
    hsm: Arc<HsmApp>,
    fitness: Arc<FitnessApp>,
    policy_pool: Pool,
    authenticator: Arc<Authenticator>,
    peer: SocketAddr,
    headers: HeaderMap,
    message: Value,
) -> Response {
    let supplied = headers
        .get("Authorization")
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "))
        .unwrap_or_default();
    if headers.get("Origin").is_some() {
        return StatusCode::FORBIDDEN.into_response();
    }

    let id = message["id"].clone();
    let method = message["method"].as_str().unwrap_or_default();
    if message["jsonrpc"] != "2.0"
        || method.is_empty()
        || (!id.is_null() && !id.is_number() && !id.is_string())
    {
        return rpc_error(id, -32600, "Invalid request");
    }
    let header_version = headers
        .get("MCP-Protocol-Version")
        .and_then(|value| value.to_str().ok());
    let body_version =
        message["params"]["_meta"]["io.modelcontextprotocol/protocolVersion"].as_str();
    let declared_version = header_version.or(body_version);
    if declared_version.is_some_and(|version| {
        !matches!(
            version,
            "2026-07-28" | "2025-11-25" | "2025-06-18" | "2025-03-26"
        )
    }) {
        return unsupported_protocol(id, declared_version.unwrap_or_default());
    }
    let modern = header_version == Some("2026-07-28") || body_version == Some("2026-07-28");
    if modern {
        let expected_name = if method == "tools/call" {
            message["params"]["name"].as_str()
        } else {
            None
        };
        let actual_name = headers
            .get("Mcp-Name")
            .and_then(|value| value.to_str().ok());
        let protocol_header = header_version.unwrap_or_default();
        let protocol_body = body_version.unwrap_or_default();
        let method_header = headers
            .get("Mcp-Method")
            .and_then(|value| value.to_str().ok());
        let capabilities =
            message["params"]["_meta"]["io.modelcontextprotocol/clientCapabilities"].is_object();
        if protocol_header != protocol_body
            || method_header != Some(method)
            || !capabilities
            || expected_name.is_some_and(|name| actual_name != Some(name))
            || (expected_name.is_none() && actual_name.is_some())
        {
            return rpc_error_status(
                id,
                -32020,
                "MCP HTTP headers or request metadata do not match",
                StatusCode::BAD_REQUEST,
            );
        }
    }
    let mut principal = match authenticator.authenticate(supplied, peer.ip()).await {
        Ok(Some(principal)) => principal,
        Ok(None) => {
            audit_denial(
                &policy_pool,
                "unauthenticated",
                "unknown",
                None,
                method,
                "authentication",
            )
            .await;
            return unauthorized(&authenticator);
        }
        Err(_) => {
            audit_denial(
                &policy_pool,
                "authentication_unavailable",
                "unknown",
                None,
                method,
                "authentication",
            )
            .await;
            return StatusCode::SERVICE_UNAVAILABLE.into_response();
        }
    };
    let openhuman_user_header = headers
        .get("X-OpenHuman-User-Id")
        .and_then(|value| value.to_str().ok());
    let service_principal = matches!(
        principal.actor_id.as_str(),
        "service:read" | "service:control"
    );
    if let Some(openhuman_user_id) = openhuman_user_header {
        if principal.identity.is_some()
            || !authenticator.permits_openhuman_user_assertion(&principal, peer.ip())
        {
            return forbidden(id, "records.read");
        }
        match store::resolve_openhuman_user_id(&policy_pool, openhuman_user_id).await {
            Ok(Some((_mapping, identity))) => principal.identity = Some(identity),
            Ok(None) => {
                audit_denial(
                    &policy_pool,
                    &principal.actor_id,
                    &principal.client_id,
                    None,
                    method,
                    "openhuman_identity_mapping",
                )
                .await;
                return forbidden(id, "records.read");
            }
            Err(_) => return StatusCode::SERVICE_UNAVAILABLE.into_response(),
        }
    } else if service_principal && authenticator.requires_openhuman_identity() {
        audit_denial(
            &policy_pool,
            &principal.actor_id,
            &principal.client_id,
            None,
            method,
            "openhuman_identity_missing",
        )
        .await;
        return forbidden(id, "records.read");
    }
    let (subject_id, fitness_owner_id) = if let Some(identity) = &principal.identity {
        match store::resolve_identity(&policy_pool, identity).await {
            Ok(Some(mapping)) => (mapping.subject_id, mapping.fitness_owner_id),
            Ok(None) => {
                audit_denial(
                    &policy_pool,
                    &principal.actor_id,
                    &principal.client_id,
                    None,
                    method,
                    "identity_mapping",
                )
                .await;
                return StatusCode::FORBIDDEN.into_response();
            }
            Err(_) => return StatusCode::SERVICE_UNAVAILABLE.into_response(),
        }
    } else {
        (
            avatar_hsm::store::SUBJECT.to_owned(),
            fitness.owner_id.clone(),
        )
    };
    let can_write = principal.can_write;
    if id.is_null() {
        if modern {
            return rpc_error_status(id, -32601, "Method not found", StatusCode::NOT_FOUND);
        }
        return StatusCode::ACCEPTED.into_response();
    }

    let hsm_read = mcp::list(HsmSurface::Read)
        .into_iter()
        .filter(|tool| {
            tool["name"].as_str().is_some_and(|name| {
                name != "get_plan_actual_links"
                    && hsm_tool_is_subject_scoped(HsmSurface::Read, name)
            })
        })
        .collect::<Vec<_>>();
    let fitness_read = fitness_mcp::tools(FitnessSurface::Read)
        .into_iter()
        .filter(|tool| tool["name"] != "get_plan_actual_links")
        .collect::<Vec<_>>();
    let common_read = common_read_tools();
    let mut hsm_write = mcp::list(HsmSurface::Control)
        .into_iter()
        .filter(|tool| {
            tool["name"]
                .as_str()
                .is_some_and(|name| hsm_tool_is_subject_scoped(HsmSurface::Control, name))
        })
        .collect::<Vec<_>>();
    if principal.client_id == "openhuman-local" {
        hsm_write.push(crate::clinical::write_tool());
        hsm_write.push(crate::clinical::bind_tool());
    }
    let mut fitness_write = fitness_mcp::tools(FitnessSurface::Write);
    for tool in hsm_write.iter_mut().chain(fitness_write.iter_mut()) {
        attach_followup_attachment(tool);
    }
    let followup_write = followup_write_tools();
    let followup_service = followup_service_tools();
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
                "protocolVersion": version,
                "capabilities": { "tools": {} },
                "serverInfo": { "name": "health-watch-tower", "version": env!("CARGO_PKG_VERSION") }
            })
        }
        "server/discover" if modern => json!({
            "resultType": "complete",
            "supportedVersions": ["2026-07-28"],
            "capabilities": { "tools": {} },
            "ttlMs": 0,
            "cacheScope": "private"
        }),
        "server/discover" => return rpc_error(id, -32601, "Method not found"),
        "ping" if !modern => json!({}),
        "tools/list" => {
            let mut tools = Vec::new();
            let read_policy = match authorize(
                &authenticator,
                &policy_pool,
                &principal,
                &subject_id,
                "read",
            )
            .await
            {
                Ok(decision) => decision,
                Err(_) => return StatusCode::SERVICE_UNAVAILABLE.into_response(),
            };
            if read_policy.0 {
                tools.extend(hsm_read.clone());
                tools.extend(fitness_read.clone());
                tools.extend(common_read.clone());
            }
            if can_write {
                let write_policy = match authorize(
                    &authenticator,
                    &policy_pool,
                    &principal,
                    &subject_id,
                    "write",
                )
                .await
                {
                    Ok(decision) => decision,
                    Err(_) => return StatusCode::SERVICE_UNAVAILABLE.into_response(),
                };
                if write_policy.0 {
                    tools.extend(hsm_write.clone());
                    tools.extend(fitness_write.clone());
                    tools.extend(followup_write.clone());
                }
            }
            if principal.actor_id == "service:control" {
                tools.extend(followup_service.clone());
            }
            json!({ "tools": tools, "resultType": "complete", "ttlMs": 0, "cacheScope": "private" })
        }
        "tools/call" => {
            let params = &message["params"];
            let Some(name) = params["name"].as_str() else {
                return rpc_error(id, -32602, "Missing tool name");
            };
            let service_tool = followup_service.iter().any(|tool| tool["name"] == name);
            let required_action = if fitness_write.iter().any(|tool| tool["name"] == name)
                || hsm_write.iter().any(|tool| tool["name"] == name)
                || followup_write.iter().any(|tool| tool["name"] == name)
                || service_tool
            {
                "write"
            } else {
                "read"
            };
            let known_tool = fitness_read
                .iter()
                .chain(fitness_write.iter())
                .chain(hsm_read.iter())
                .chain(hsm_write.iter())
                .chain(followup_write.iter())
                .chain(followup_service.iter())
                .chain(common_read.iter())
                .any(|tool| tool["name"] == name);
            let trusted_dispatcher = principal.actor_id == "service:control" && can_write;
            let tier_allows = if service_tool {
                trusted_dispatcher
            } else {
                required_action != "write" || can_write
            };
            let policy_result = if service_tool && trusted_dispatcher {
                Ok((true, None))
            } else if known_tool && tier_allows {
                authorize(
                    &authenticator,
                    &policy_pool,
                    &principal,
                    &subject_id,
                    required_action,
                )
                .await
            } else {
                Ok((false, None))
            };
            let (policy_allowed, consent_version) =
                policy_result.as_ref().copied().unwrap_or((false, None));
            let allowed = known_tool && tier_allows && policy_allowed;
            let event = AuditEvent {
                actor_id: principal.actor_id.clone(),
                subject_id: Some(subject_id.clone()),
                operation: method.to_owned(),
                resource_id: name.to_owned(),
                allowed,
                consent_version,
            };
            let audit_id =
                match store::begin_audit(&policy_pool, &event, &principal.client_id).await {
                    Ok(audit_id) => audit_id,
                    Err(_) => return StatusCode::SERVICE_UNAVAILABLE.into_response(),
                };
            if !allowed {
                if store::finish_audit(&policy_pool, audit_id, false)
                    .await
                    .is_err()
                {
                    return StatusCode::SERVICE_UNAVAILABLE.into_response();
                }
                if policy_result.is_err() {
                    return StatusCode::SERVICE_UNAVAILABLE.into_response();
                }
                if authenticator.is_external(&principal) {
                    return forbidden(id, required_action);
                }
                return rpc_error(id, -32602, "Tool is not available to this bearer");
            }
            let fitness = FitnessApp {
                store: fitness.store.clone(),
                owner_id: fitness_owner_id,
                read_token: fitness.read_token.clone(),
                write_token: fitness.write_token.clone(),
            };
            let arguments = params["arguments"].clone();
            let attached_followup = arguments.get("follow_up").cloned();
            if attached_followup.is_some() && !supports_followup_attachment(name) {
                let _ = store::finish_audit(&policy_pool, audit_id, false).await;
                return rpc_error(
                    id,
                    -32602,
                    "This tool does not support an attached follow-up",
                );
            }
            let mut domain_arguments = arguments.clone();
            if let Some(object) = domain_arguments.as_object_mut() {
                object.remove("follow_up");
            }
            let envelope =
                record_write_envelope(name, &domain_arguments, &subject_id, &principal.actor_id);
            let fingerprint = record_write_fingerprint(name, &domain_arguments);
            let apply = || async {
                let result = if name == "submit_followup" {
                    let args = &params["arguments"];
                    let request = watch_tower_core::followups::FollowupRequest {
                        idempotency_key: arg_string(args, "idempotency_key"),
                        subject_id: subject_id.clone(),
                        adapter_id: arg_string(args, "adapter_id"),
                        record_id: arg_string(args, "record_id"),
                        record_revision: args["record_revision"].as_i64().unwrap_or_default(),
                        reason_code: arg_string(args, "reason_code"),
                        role: arg_string(args, "role"),
                        urgency: arg_string(args, "urgency"),
                    };
                    dispatch_followup_request(&policy_pool, &authenticator, &hsm, &fitness, request)
                        .await
                } else if name == "get_plan_actual_timeline" {
                    get_plan_actual_timeline(&hsm, &fitness, &subject_id, &domain_arguments).await
                } else if name == "claim_followups" {
                    let args = &params["arguments"];
                    let worker_id = arg_string(args, "worker_id");
                    let limit = args["limit"].as_i64().unwrap_or(20);
                    if worker_id.trim().is_empty() {
                        Err(anyhow::anyhow!("missing follow-up dispatcher id"))
                    } else {
                        claim_authorized_followups(&policy_pool, &authenticator, &worker_id, limit)
                            .await
                    }
                } else if name == "bind_followup_workflow" {
                    let args = &params["arguments"];
                    let workflow = watch_tower_core::followups::WorkflowExecutionRef {
                        workflow_id: args["workflow_id"].as_str().map(str::to_owned),
                        run_id: arg_string(args, "run_id"),
                        task_key: arg_string(args, "task_key"),
                        assignee_id: args["assignee_id"].as_str().map(str::to_owned),
                    };
                    store::bind_followup_workflow(
                        &policy_pool,
                        &arg_string(args, "idempotency_key"),
                        &arg_string(args, "worker_id"),
                        &workflow,
                    )
                    .await
                    .and_then(|bound| {
                        if bound {
                            Ok(json!({"bound": true}))
                        } else {
                            Err(anyhow::anyhow!(
                                "follow-up dispatch lease is stale or not owned"
                            ))
                        }
                    })
                } else if name == "finish_followup_workflow" {
                    let args = &params["arguments"];
                    let key = arg_string(args, "idempotency_key");
                    let run_id = arg_string(args, "workflow_run_id");
                    store::finish_followup_workflow(&policy_pool, &key, &run_id)
                        .await
                        .map(|finished| json!({"finished": finished}))
                } else if name == "bind_fhir_patient" {
                    match consent_version {
                        Some(version) => crate::clinical::bind_patient(
                            &policy_pool,
                            &subject_id,
                            &principal.actor_id,
                            &principal.client_id,
                            version,
                            &domain_arguments,
                        ).await,
                        None => Err(anyhow::anyhow!(
                            "hospital patient binding requires an active, versioned records.write consent"
                        )),
                    }
                } else if name == "import_fhir_bundle" {
                    match consent_version {
                        Some(version) => {
                            crate::clinical::import_bundle(
                                &policy_pool,
                                &subject_id,
                                &principal.actor_id,
                                &principal.client_id,
                                version,
                                &domain_arguments,
                            )
                            .await
                        }
                        None => Err(anyhow::anyhow!(
                            "clinical import requires an active, versioned records.write consent"
                        )),
                    }
                } else if name == "get_clinical_records" {
                    crate::clinical::list_records(
                        &policy_pool,
                        &subject_id,
                        domain_arguments["limit"].as_i64().unwrap_or(25),
                    )
                    .await
                } else if fitness_read.iter().any(|tool| tool["name"] == name) {
                    fitness_mcp::call(&fitness, FitnessSurface::Read, name, &domain_arguments)
                        .await
                        .map_err(anyhow::Error::msg)
                } else if fitness_write.iter().any(|tool| tool["name"] == name) && can_write {
                    fitness_mcp::call(&fitness, FitnessSurface::Write, name, &domain_arguments)
                        .await
                        .map_err(anyhow::Error::msg)
                } else if hsm_read.iter().any(|tool| tool["name"] == name) {
                    if matches!(name, "food_catalog" | "medication_catalog") {
                        mcp::call(&hsm, HsmSurface::Read, name, &domain_arguments).await
                    } else {
                        avatar_hsm::mcp::scoped_call(
                            &hsm.db,
                            HsmSurface::Read,
                            &subject_id,
                            name,
                            &domain_arguments,
                        )
                        .await
                    }
                } else {
                    avatar_hsm::mcp::scoped_call(
                        &hsm.db,
                        HsmSurface::Control,
                        &subject_id,
                        name,
                        &domain_arguments,
                    )
                    .await
                };
                let mut result = result?;
                if let Some(intent) = attached_followup.as_ref() {
                    let envelope = envelope
                        .as_ref()
                        .context("attached follow-up requires a durable record-write envelope")?;
                    anyhow::ensure!(
                        result["status"] != "conflict",
                        "domain adapter rejected the write; follow-up was not created"
                    );
                    let record_id =
                        attached_followup_record_id(name, &domain_arguments, &result, intent)
                            .context("attached follow-up needs a durable record ID")?;
                    let request = watch_tower_core::followups::FollowupRequest {
                        idempotency_key: attached_followup_idempotency_key(
                            &subject_id,
                            &envelope.adapter_id,
                            &record_id,
                            envelope.revision,
                            &arg_string(intent, "reason_code"),
                            &arg_string(intent, "role"),
                            &arg_string(intent, "urgency"),
                        ),
                        subject_id: subject_id.clone(),
                        adapter_id: envelope.adapter_id.clone(),
                        record_id,
                        record_revision: envelope.revision,
                        reason_code: arg_string(intent, "reason_code"),
                        role: arg_string(intent, "role"),
                        urgency: arg_string(intent, "urgency"),
                    };
                    let followup = dispatch_followup_request(
                        &policy_pool,
                        &authenticator,
                        &hsm,
                        &fitness,
                        request,
                    )
                    .await?;
                    if let Some(object) = result.as_object_mut() {
                        object.insert("follow_up".into(), followup);
                    } else {
                        result = json!({"domain_result": result, "follow_up": followup});
                    }
                }
                Ok(result)
            };
            let run_write = || async {
                if let Some(envelope) = envelope.as_ref() {
                    store::with_record_write(&policy_pool, envelope, &fingerprint, apply).await
                } else {
                    apply().await
                }
            };
            let result = if required_action == "write" {
                if let Some(version) = consent_version {
                    store::with_write_consent(
                        &policy_pool,
                        &subject_id,
                        &principal.client_id,
                        version,
                        run_write,
                    )
                    .await
                } else {
                    run_write().await
                }
            } else {
                run_write().await
            }
            .map_err(|error| error.to_string());
            if store::finish_audit(&policy_pool, audit_id, result.is_ok())
                .await
                .is_err()
            {
                return StatusCode::SERVICE_UNAVAILABLE.into_response();
            }
            match result {
                Ok(value) => json!({
                    "content": [{"type": "text", "text": value.to_string()}],
                    "structuredContent": value
                }),
                Err(error) => json!({
                    "isError": true,
                    "content": [{"type": "text", "text": error.to_string()}]
                }),
            }
        }
        "notifications/initialized" => json!({}),
        _ if modern => {
            return rpc_error_status(id, -32601, "Method not found", StatusCode::NOT_FOUND);
        }
        _ => return rpc_error(id, -32601, "Method not found"),
    };
    let mut output = output;
    if modern {
        if let Some(result) = output.as_object_mut() {
            result.insert(
                "_meta".into(),
                json!({
                    "io.modelcontextprotocol/serverInfo": {
                        "name": "health-watch-tower",
                        "version": env!("CARGO_PKG_VERSION")
                    }
                }),
            );
        }
    }
    Json(json!({ "jsonrpc": "2.0", "id": id, "result": output })).into_response()
}

fn rpc_error(id: Value, code: i32, message: &str) -> Response {
    Json(json!({
        "jsonrpc": "2.0",
        "id": id,
        "error": { "code": code, "message": message }
    }))
    .into_response()
}

fn rpc_error_status(id: Value, code: i32, message: &str, status: StatusCode) -> Response {
    let mut response = rpc_error(id, code, message);
    *response.status_mut() = status;
    response
}

fn unsupported_protocol(id: Value, requested: &str) -> Response {
    Json(json!({
        "jsonrpc": "2.0",
        "id": id,
        "error": {
            "code": -32022,
            "message": "Unsupported protocol version",
            "data": {
                "supported": ["2026-07-28", "2025-11-25", "2025-06-18", "2025-03-26"],
                "requested": requested
            }
        }
    }))
    .into_response()
}

fn forbidden(id: Value, action: &str) -> Response {
    let scope = if action == "write" {
        "records.write"
    } else {
        "records.read"
    };
    let mut response =
        rpc_error_status(id, -32003, "Insufficient permission", StatusCode::FORBIDDEN);
    if let Ok(value) = HeaderValue::from_str(&format!(
        "Bearer error=\"insufficient_scope\", scope=\"{scope}\""
    )) {
        response.headers_mut().insert(WWW_AUTHENTICATE, value);
    }
    response
}

async fn authorize(
    authenticator: &Authenticator,
    pool: &Pool,
    principal: &Principal,
    subject_id: &str,
    action: &str,
) -> anyhow::Result<(bool, Option<i64>)> {
    if !authenticator.is_external(principal) {
        return Ok((action != "write" || principal.can_write, None));
    }
    let scope = if action == "write" {
        "records.write"
    } else {
        "records.read"
    };
    if !principal.has_scope(scope) {
        return Ok((false, None));
    }
    let now = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs() as i64;
    let Some(consent) =
        store::current_consent(pool, subject_id, &principal.client_id, scope).await?
    else {
        return Ok((false, None));
    };
    if !consent.is_active_at(now) {
        return Ok((false, Some(consent.version)));
    }
    let related = authenticator
        .has_relationship(principal, subject_id, action)
        .await?;
    Ok((related, Some(consent.version)))
}

async fn audit_denial(
    pool: &Pool,
    actor_id: &str,
    client_id: &str,
    subject_id: Option<&str>,
    method: &str,
    resource: &str,
) {
    let event = AuditEvent {
        actor_id: actor_id.to_owned(),
        subject_id: subject_id.map(str::to_owned),
        operation: method.to_owned(),
        resource_id: resource.to_owned(),
        allowed: false,
        consent_version: None,
    };
    if let Ok(id) = store::begin_audit(pool, &event, client_id).await {
        let _ = store::finish_audit(pool, id, false).await;
    }
}

fn unauthorized(authenticator: &Authenticator) -> Response {
    let mut response = StatusCode::UNAUTHORIZED.into_response();
    if let Some(url) = authenticator.resource_metadata_url() {
        let challenge = format!("Bearer resource_metadata=\"{url}\"");
        if let Ok(value) = HeaderValue::from_str(&challenge) {
            response.headers_mut().insert(WWW_AUTHENTICATE, value);
        }
    }
    response
}

fn arg_string(args: &Value, key: &str) -> String {
    args[key].as_str().unwrap_or_default().to_owned()
}

fn record_write_envelope(
    name: &str,
    args: &Value,
    subject_id: &str,
    actor_id: &str,
) -> Option<RecordEnvelope> {
    let (adapter_id, source_id, record_id, revision, idempotency_key) = match name {
        "import_fhir_bundle" => {
            let key = args["idempotency_key"].as_str()?;
            let source = args["source_system"].as_str()?;
            (
                "clinical",
                source,
                key.to_owned(),
                1,
                format!("clinical-import:{subject_id}:{key}"),
            )
        }
        "bind_fhir_patient" => {
            let source = args["source_system"].as_str()?;
            let reference = args["patient_reference"].as_str()?;
            (
                "clinical",
                source,
                reference.to_owned(),
                1,
                format!("clinical-patient-binding:{subject_id}:{source}:{reference}"),
            )
        }
        "record_confirmed_workout" => {
            let run_id = args["workflow_run_id"].as_str()?;
            (
                "fitness",
                "openhuman-workflow",
                run_id.to_owned(),
                1,
                format!("workflow:{run_id}"),
            )
        }
        "record_manual_workout" => {
            let key = args["request_key"].as_str()?;
            (
                "fitness",
                "manual",
                key.to_owned(),
                1,
                format!("manual:{key}"),
            )
        }
        "import_workout_csv" => {
            let batch_key = record_write_fingerprint(name, args);
            (
                "fitness",
                "workout-csv-import",
                batch_key.clone(),
                1,
                format!("csv-import:{batch_key}"),
            )
        }
        "commit_health_connect_snapshot" => {
            let snapshot_key = record_write_fingerprint(name, args);
            (
                "hsm",
                "mobile-wearable-snapshot",
                snapshot_key.clone(),
                1,
                format!("wearable-snapshot:{snapshot_key}"),
            )
        }
        "replace_workout_set" | "delete_workout_set" => {
            let id = args["set_id"].as_str()?;
            let expected_revision = args["expected_revision"].as_i64()?;
            (
                "fitness",
                "watch-tower-mcp",
                id.to_owned(),
                expected_revision.checked_add(1)?,
                format!("workout-set-change:{id}:{expected_revision}"),
            )
        }
        "create_training_plan" => {
            let key = args["request_key"].as_str()?;
            (
                "fitness",
                "watch-tower-mcp",
                key.to_owned(),
                1,
                format!("training-plan-create:{key}"),
            )
        }
        "revise_training_plan" => {
            let id = args["plan_id"].as_str()?;
            let expected_revision = args["expected_revision"].as_i64()?;
            (
                "fitness",
                "watch-tower-mcp",
                id.to_owned(),
                expected_revision.checked_add(1)?,
                format!("training-plan-revision:{id}:{expected_revision}"),
            )
        }
        "record_medication_response_v1" => {
            let id = args["dose_id"].as_str()?;
            (
                "hsm",
                "watch-tower-mcp",
                id.to_owned(),
                1,
                format!("dose-response:{subject_id}:{id}"),
            )
        }
        "confirm_diary_entry" => {
            let id = args["id"].as_str()?;
            (
                "hsm",
                "watch-tower-mcp",
                id.to_owned(),
                1,
                format!("diary-entry:{id}"),
            )
        }
        "change_diary_entry" => {
            let id = args["id"].as_str()?;
            let current_revision = args["revision"].as_i64()?;
            (
                "hsm",
                "watch-tower-mcp",
                id.to_owned(),
                current_revision.checked_add(1)?,
                format!("diary-change:{id}:{current_revision}"),
            )
        }
        "store_sleep_session" => {
            let id = args["id"].as_str()?;
            (
                "hsm",
                "watch-tower-mcp",
                id.to_owned(),
                1,
                format!("sleep-session:{id}"),
            )
        }
        _ => return None,
    };
    Some(RecordEnvelope {
        subject_id: subject_id.to_owned(),
        actor_id: actor_id.to_owned(),
        adapter_id: adapter_id.to_owned(),
        source_id: source_id.to_owned(),
        record_id,
        revision,
        run_id: None,
        observed_at_unix: SystemTime::now().duration_since(UNIX_EPOCH).ok()?.as_secs() as i64,
        idempotency_key,
    })
}

fn record_write_fingerprint(name: &str, args: &Value) -> String {
    let canonical = serde_json::to_vec(&json!({"tool": name, "arguments": args}))
        .expect("JSON values always serialize");
    sha256_hex(&canonical)
}

fn sha256_hex(value: &[u8]) -> String {
    let digest = ring::digest::digest(&ring::digest::SHA256, value);
    digest
        .as_ref()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

async fn get_plan_actual_timeline(
    hsm: &HsmApp,
    fitness: &FitnessApp,
    subject_id: &str,
    arguments: &Value,
) -> anyhow::Result<Value> {
    let start =
        chrono::DateTime::parse_from_rfc3339(arguments["start"].as_str().unwrap_or_default())?
            .with_timezone(&chrono::Utc);
    let end = chrono::DateTime::parse_from_rfc3339(arguments["end"].as_str().unwrap_or_default())?
        .with_timezone(&chrono::Utc);
    anyhow::ensure!(
        start < end && end - start <= chrono::Duration::days(366),
        "select a range of up to 366 days"
    );
    let requested_adapter = arguments["adapter_id"].as_str();
    anyhow::ensure!(
        requested_adapter.is_none_or(|adapter| matches!(adapter, "hsm" | "fitness")),
        "unsupported plan/actual adapter"
    );
    let query = json!({"start":start,"end":end});
    let fitness = FitnessApp {
        store: fitness.store.clone(),
        owner_id: fitness.owner_id.clone(),
        read_token: fitness.read_token.clone(),
        write_token: fitness.write_token.clone(),
    };
    let mut links = Vec::new();
    if requested_adapter.is_none_or(|adapter| adapter == "hsm") {
        let value = avatar_hsm::mcp::scoped_call(
            &hsm.db,
            HsmSurface::Read,
            subject_id,
            "get_plan_actual_links",
            &query,
        )
        .await?;
        append_plan_links(&mut links, &value, subject_id, subject_id)?;
    }
    if requested_adapter.is_none_or(|adapter| adapter == "fitness") {
        let value = fitness_mcp::call(
            &fitness,
            FitnessSurface::Read,
            "get_plan_actual_links",
            &query,
        )
        .await
        .map_err(anyhow::Error::msg)?;
        append_plan_links(&mut links, &value, &fitness.owner_id, subject_id)?;
    }
    Ok(json!({"items":links,"start":start,"end":end,"adapters":
        if requested_adapter.is_some() { vec![requested_adapter.unwrap()] } else { vec!["hsm","fitness"] }
    }))
}

fn append_plan_links(
    output: &mut Vec<Value>,
    source: &Value,
    expected_subject: &str,
    exposed_subject: &str,
) -> anyhow::Result<()> {
    let items = source["links"]
        .as_array()
        .context("adapter returned an invalid plan/actual projection")?;
    for item in items {
        let planned_value = &item["planned"];
        let adapter_id = planned_value["adapter_id"]
            .as_str()
            .context("plan reference is missing its adapter")?;
        let plan_id = planned_value["plan_id"]
            .as_str()
            .context("plan reference is missing its ID")?;
        let planned = watch_tower_core::plans::PlannedReference {
            subject_id: planned_value["subject_id"]
                .as_str()
                .context("plan reference is missing its subject")?
                .to_owned(),
            adapter_id: adapter_id.to_owned(),
            plan_id: plan_id.to_owned(),
            revision: planned_value["revision"].as_i64().unwrap_or_default(),
            planned_at_unix: planned_value["planned_at_unix"].as_i64(),
        };
        anyhow::ensure!(
            planned.subject_id == expected_subject,
            "adapter returned a cross-subject plan reference"
        );
        let actual = if item["actual"].is_null() {
            None
        } else {
            let value = &item["actual"];
            let actual_subject = value["subject_id"]
                .as_str()
                .context("actual reference is missing its subject")?;
            anyhow::ensure!(
                actual_subject == expected_subject,
                "adapter returned a cross-subject actual reference"
            );
            Some(watch_tower_core::plans::ActualReference {
                subject_id: exposed_subject.to_owned(),
                adapter_id: value["adapter_id"]
                    .as_str()
                    .context("actual reference is missing its adapter")?
                    .to_owned(),
                record_id: value["record_id"]
                    .as_str()
                    .context("actual reference is missing its ID")?
                    .to_owned(),
                revision: value["revision"].as_i64().unwrap_or_default(),
                occurred_at_unix: value["occurred_at_unix"].as_i64().unwrap_or_default(),
            })
        };
        let observation = watch_tower_core::plans::PlanActualObservation {
            planned: watch_tower_core::plans::PlannedReference {
                subject_id: exposed_subject.to_owned(),
                ..planned
            },
            actual,
            target_key: item["target_key"].as_str().map(str::to_owned),
            state: item["state"]
                .as_str()
                .context("plan/actual state is missing")?
                .to_owned(),
        };
        anyhow::ensure!(
            observation.is_valid(),
            "adapter returned an invalid common plan/actual reference"
        );
        output.push(json!({
            "planned":{
                "subject_id":observation.planned.subject_id,
                "adapter_id":observation.planned.adapter_id,
                "plan_id":observation.planned.plan_id,
                "revision":observation.planned.revision,
                "planned_at_unix":observation.planned.planned_at_unix
            },
            "actual":observation.actual.map(|actual| json!({
                "subject_id":actual.subject_id,
                "adapter_id":actual.adapter_id,
                "record_id":actual.record_id,
                "revision":actual.revision,
                "occurred_at_unix":actual.occurred_at_unix
            })),
            "target_key":observation.target_key,
            "state":observation.state
        }));
    }
    Ok(())
}

async fn followup_record_exists(
    hsm: &HsmApp,
    fitness: &FitnessApp,
    request: &watch_tower_core::followups::FollowupRequest,
) -> anyhow::Result<bool> {
    match request.adapter_id.as_str() {
        "fitness" => Ok(fitness
            .store
            .record_exists(&fitness.owner_id, &request.record_id)
            .await?),
        "hsm" => {
            let client = hsm.db.get().await?;
            Ok(
                avatar_hsm::store::record_exists(&client, &request.subject_id, &request.record_id)
                    .await?,
            )
        }
        adapter => anyhow::bail!("follow-up adapter is not registered: {adapter}"),
    }
}

async fn dispatch_followup_request(
    policy_pool: &Pool,
    authenticator: &Authenticator,
    hsm: &HsmApp,
    fitness: &FitnessApp,
    request: watch_tower_core::followups::FollowupRequest,
) -> anyhow::Result<Value> {
    anyhow::ensure!(request.is_valid(), "invalid generic follow-up request");
    anyhow::ensure!(
        followup_record_exists(hsm, fitness, &request).await?,
        "follow-up record reference does not exist in its owning adapter"
    );
    let identities = store::openhuman_assignees(policy_pool, &request.subject_id).await?;
    let assignees = authenticator
        .users_for_role(&request.role, &request.subject_id, &identities)
        .await?;
    store::create_followup(policy_pool, &request, &assignees).await
}

fn attached_followup_record_id(
    tool: &str,
    arguments: &Value,
    result: &Value,
    intent: &Value,
) -> Option<String> {
    intent["record_id"]
        .as_str()
        .filter(|id| !id.trim().is_empty())
        .map(str::to_owned)
        .or_else(|| match tool {
            "record_confirmed_workout" | "record_manual_workout" => {
                result["workout_id"].as_str().map(str::to_owned)
            }
            "replace_workout_set" | "delete_workout_set" => {
                arguments["set_id"].as_str().map(str::to_owned)
            }
            "create_training_plan" => result["id"]
                .as_str()
                .or_else(|| result["plan_id"].as_str())
                .or_else(|| arguments["request_key"].as_str())
                .map(str::to_owned),
            "revise_training_plan" => arguments["plan_id"].as_str().map(str::to_owned),
            "confirm_diary_entry" | "change_diary_entry" | "store_sleep_session" => {
                arguments["id"].as_str().map(str::to_owned)
            }
            "record_medication_response_v1" => arguments["dose_id"].as_str().map(str::to_owned),
            // Batch imports and wearable snapshots may contain many records;
            // the caller must choose the specific record for human review.
            "import_workout_csv" | "commit_health_connect_snapshot" => None,
            _ => None,
        })
}

fn attached_followup_idempotency_key(
    subject_id: &str,
    adapter_id: &str,
    record_id: &str,
    record_revision: i64,
    reason_code: &str,
    role: &str,
    urgency: &str,
) -> String {
    let material = json!({
        "subject_id": subject_id,
        "adapter_id": adapter_id,
        "record_id": record_id,
        "record_revision": record_revision,
        "reason_code": reason_code,
        "role": role,
        "urgency": urgency
    });
    let canonical = serde_json::to_vec(&material).expect("JSON values always serialize");
    format!("record-followup:{}", sha256_hex(&canonical))
}

async fn claim_authorized_followups(
    pool: &Pool,
    authenticator: &Authenticator,
    worker_id: &str,
    limit: i64,
) -> anyhow::Result<Value> {
    let mut items = store::claim_followups(pool, worker_id, limit).await?;
    let mut refreshed = Vec::new();
    for mut item in items.drain(..) {
        let subject = item["subject_id"].as_str().unwrap_or_default();
        let role = item["role"].as_str().unwrap_or_default();
        let identities = store::openhuman_assignees(pool, subject).await?;
        let assignees = authenticator
            .users_for_role(role, subject, &identities)
            .await?;
        let key = item["idempotency_key"].as_str().unwrap_or_default();
        anyhow::ensure!(
            store::refresh_claimed_followup_assignees(pool, key, worker_id, &assignees).await?,
            "follow-up dispatch lease expired during authorization refresh"
        );
        if !assignees.is_empty() {
            item["assignees"] = json!(assignees);
            refreshed.push(item);
        }
    }
    Ok(json!({ "items": refreshed }))
}

fn hsm_tool_is_subject_scoped(surface: HsmSurface, name: &str) -> bool {
    match surface {
        HsmSurface::Read => matches!(
            name,
            "medication_candidates"
                | "check_dur"
                | "health_status_v1"
                | "hsm_state_v1"
                | "sleep_sessions"
                | "diary_entries"
                | "nutrition_daily"
                | "diary_water_today"
                | "cloud_device_status"
                | "food_catalog"
                | "medication_catalog"
                | "wearable_samples"
                | "wearable_daily"
                | "wearable_sessions"
                | "wearable_fit_details"
                | "wearable_goals"
                | "polar_physical_info"
                | "device_status"
        ),
        HsmSurface::Control => matches!(
            name,
            "schedule_cloud_sync"
                | "connect_cloud_oauth"
                | "request_oura_due_sync"
                | "sync_cloud_device"
                | "revoke_cloud_device"
                | "issue_health_connect_lease"
                | "commit_health_connect_snapshot"
                | "revoke_health_connect"
                | "confirm_diary_entry"
                | "change_diary_entry"
                | "record_medication_response_v1"
                | "store_sleep_session"
        ),
    }
}

fn supports_followup_attachment(name: &str) -> bool {
    matches!(
        name,
        "record_confirmed_workout"
            | "record_manual_workout"
            | "import_workout_csv"
            | "replace_workout_set"
            | "delete_workout_set"
            | "create_training_plan"
            | "revise_training_plan"
            | "commit_health_connect_snapshot"
            | "confirm_diary_entry"
            | "change_diary_entry"
            | "record_medication_response_v1"
            | "store_sleep_session"
    )
}

fn attach_followup_attachment(tool: &mut Value) {
    if !tool["name"]
        .as_str()
        .is_some_and(supports_followup_attachment)
    {
        return;
    }
    tool["inputSchema"]["properties"]["follow_up"] = json!({
        "type":"object",
        "description":"Attach an owner-requested cross-domain review to this validated write. Watch Tower verifies the saved record and assigns an authorized reviewer; response and deadline remain in OpenHuman Workflow.",
        "properties":{
            "record_id":{"type":"string","minLength":1,"maxLength":160},
            "reason_code":{"type":"string","pattern":"^[a-z0-9_]{1,64}$"},
            "role":{"type":"string","enum":["caregiver","clinician","trainer","nutritionist"]},
            "urgency":{"type":"string","enum":["routine","soon","urgent"]}
        },
        "required":["reason_code","role","urgency"],
        "additionalProperties":false
    });
}

fn followup_write_tools() -> Vec<Value> {
    vec![json!({
        "name": "submit_followup",
        "description": "Create an idempotent, domain-neutral review request linked to a record verified by its owning adapter. Response and deadline state remain in OpenHuman Workflow.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "idempotency_key": {"type":"string", "minLength":1, "maxLength":160},
                "adapter_id": {"type":"string", "minLength":1, "maxLength":64},
                "record_id": {"type":"string", "minLength":1, "maxLength":160},
                "record_revision": {"type":"integer", "minimum":1},
                "reason_code": {"type":"string", "pattern":"^[a-z0-9_]{1,64}$"},
                "role": {"type":"string", "enum":["caregiver","clinician","trainer","nutritionist"]},
                "urgency": {"type":"string", "enum":["routine","soon","urgent"]}
            },
            "required":["idempotency_key","adapter_id","record_id","record_revision","reason_code","role","urgency"],
            "additionalProperties":false
        }
    })]
}

fn common_read_tools() -> Vec<Value> {
    vec![
        json!({
            "name":"get_plan_actual_timeline",
            "description":"Return one subject's common plan-to-actual references across registered adapters. Owner adapters keep all plan rules and record payloads.",
            "inputSchema":{
                "type":"object",
                "properties":{
                    "start":{"type":"string","format":"date-time"},
                    "end":{"type":"string","format":"date-time"},
                    "adapter_id":{"type":"string","enum":["hsm","fitness"]}
                },
                "required":["start","end"],
                "additionalProperties":false
            },
            "annotations":{"readOnlyHint":true}
        }),
        crate::clinical::read_tool(),
    ]
}

fn followup_service_tools() -> Vec<Value> {
    vec![
        json!({
            "name":"claim_followups",
            "description":"Claim eligible generic follow-up dispatches for the OpenHuman Workflow scheduler. Claims contain references only; Workflow owns responses and deadlines.",
            "inputSchema":{
                "type":"object",
                "properties":{
                    "worker_id":{"type":"string","minLength":1,"maxLength":120},
                    "limit":{"type":"integer","minimum":1,"maximum":100}
                },
                "required":["worker_id"],
                "additionalProperties":false
            }
        }),
        json!({
            "name":"bind_followup_workflow",
            "description":"Link a claimed follow-up request to its already-started OpenHuman Workflow execution.",
            "inputSchema":{
                "type":"object",
                "properties":{
                    "idempotency_key":{"type":"string","minLength":1},
                    "worker_id":{"type":"string","minLength":1},
                    "workflow_id":{"type":"string","minLength":1},
                    "run_id":{"type":"string","minLength":1},
                    "task_key":{"type":"string","minLength":1},
                    "assignee_id":{"type":"string"}
                },
                "required":["idempotency_key","worker_id","workflow_id","run_id","task_key"],
                "additionalProperties":false
            }
        }),
        json!({
            "name":"finish_followup_workflow",
            "description":"Record that OpenHuman observed the linked Workflow run reach a terminal state. The response and deadline outcome remain in Workflow.",
            "inputSchema":{
                "type":"object",
                "properties":{
                    "idempotency_key":{"type":"string","minLength":1},
                    "workflow_run_id":{"type":"string","minLength":1}
                },
                "required":["idempotency_key","workflow_run_id"],
                "additionalProperties":false
            }
        }),
    ]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_advertised_hsm_tool_is_request_scoped_or_internal() {
        for surface in [HsmSurface::Read, HsmSurface::Control] {
            for tool in mcp::list(surface) {
                let name = tool["name"].as_str().expect("HSM tool has a name");
                if surface == HsmSurface::Read && name == "get_plan_actual_links" {
                    continue;
                }
                assert!(
                    hsm_tool_is_subject_scoped(surface, name),
                    "HSM tool {name} is missing request-scoped authorization"
                );
            }
        }
    }

    #[test]
    fn followup_attachment_is_only_advertised_on_supported_record_writes() {
        let mut workout = json!({"name":"record_manual_workout", "inputSchema":{"properties":{}}});
        let mut read = json!({"name":"health_status_v1", "inputSchema":{"properties":{}}});
        attach_followup_attachment(&mut workout);
        attach_followup_attachment(&mut read);

        assert!(workout["inputSchema"]["properties"]["follow_up"].is_object());
        assert!(read["inputSchema"]["properties"]["follow_up"].is_null());
        assert!(supports_followup_attachment(
            "record_medication_response_v1"
        ));
        assert!(!supports_followup_attachment("health_status_v1"));
    }

    #[test]
    fn attached_followup_requires_an_explicit_target_for_batch_records() {
        let intent = json!({"record_id":"session-42"});
        let result = json!({"imported":12});
        assert_eq!(
            attached_followup_record_id("import_workout_csv", &json!({}), &result, &intent),
            Some("session-42".to_owned())
        );
        assert_eq!(
            attached_followup_record_id(
                "commit_health_connect_snapshot",
                &json!({}),
                &result,
                &json!({})
            ),
            None
        );
    }

    #[test]
    fn attached_followup_resolves_record_ids_from_owner_write_results() {
        assert_eq!(
            attached_followup_record_id(
                "record_manual_workout",
                &json!({}),
                &json!({"workout_id":"workout-1"}),
                &json!({})
            ),
            Some("workout-1".to_owned())
        );
        assert_eq!(
            attached_followup_record_id(
                "revise_training_plan",
                &json!({"plan_id":"plan-1"}),
                &json!({}),
                &json!({})
            ),
            Some("plan-1".to_owned())
        );
    }

    #[test]
    fn followup_keys_are_stable_per_record_revision_and_review_intent() {
        let first = attached_followup_idempotency_key(
            "person-1",
            "fitness",
            "set-1",
            4,
            "pain_reported",
            "trainer",
            "soon",
        );
        let retry = attached_followup_idempotency_key(
            "person-1",
            "fitness",
            "set-1",
            4,
            "pain_reported",
            "trainer",
            "soon",
        );
        let next_revision = attached_followup_idempotency_key(
            "person-1",
            "fitness",
            "set-1",
            5,
            "pain_reported",
            "trainer",
            "soon",
        );
        assert_eq!(first, retry);
        assert_ne!(first, next_revision);
    }
}
