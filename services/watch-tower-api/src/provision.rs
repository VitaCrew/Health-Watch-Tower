use std::{env, time::Duration};

use crate::store;
use anyhow::{bail, ensure, Context, Result};
use serde::Deserialize;
use serde_json::json;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ProvisionRequest {
    operation_id: String,
    issuer: String,
    client_id: String,
    external_subject: String,
    subject_id: String,
    fitness_owner_id: String,
    timezone: String,
    #[serde(default)]
    openhuman_user_id: Option<String>,
    #[serde(default)]
    consent_scopes: Vec<String>,
    granted_by_identity: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RevokeConsentRequest {
    issuer: String,
    client_id: String,
    external_subject: String,
    scope: String,
    actor_identity: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RevokeIdentityRequest {
    issuer: String,
    client_id: String,
    external_subject: String,
    actor_identity: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RoleChangeRequest {
    issuer: String,
    client_id: String,
    external_subject: String,
    role: String,
    actor_identity: String,
}

pub async fn run_from_stdin(command: &str) -> Result<()> {
    let mut input = Vec::new();
    let mut stdin = std::io::stdin();
    std::io::Read::read_to_end(&mut stdin, &mut input)
        .context("reading operator provisioning JSON from stdin")?;
    let database_url = env::var("AVATAR_HSM_DATABASE_URL")
        .context("set AVATAR_HSM_DATABASE_URL to the Watch Tower PostgreSQL database")?;
    let result = match command {
        "provision-person" => {
            let request: ProvisionRequest = serde_json::from_slice(&input)
                .context("operator provisioning input must be one JSON object")?;
            provision(&database_url, request).await?
        }
        "revoke-consent" => {
            let request: RevokeConsentRequest = serde_json::from_slice(&input)
                .context("consent revocation input must be one JSON object")?;
            revoke_consent(&database_url, request).await?
        }
        "revoke-identity" => {
            let request: RevokeIdentityRequest = serde_json::from_slice(&input)
                .context("identity revocation input must be one JSON object")?;
            revoke_identity(&database_url, request).await?
        }
        "grant-role" | "revoke-role" => {
            let request: RoleChangeRequest =
                serde_json::from_slice(&input).context("role input must be one JSON object")?;
            change_role(&database_url, request, command == "grant-role").await?
        }
        _ => bail!("unknown operator provisioning command"),
    };
    println!("{}", serde_json::to_string(&result)?);
    Ok(())
}

async fn change_role(
    database_url: &str,
    request: RoleChangeRequest,
    grant: bool,
) -> Result<serde_json::Value> {
    validate_identity(
        &request.issuer,
        &request.client_id,
        &request.external_subject,
    )?;
    ensure!(
        matches!(
            request.role.as_str(),
            "owner" | "caregiver" | "clinician" | "trainer" | "nutritionist"
        ) && valid_token(&request.actor_identity, 255),
        "invalid Watch Tower role change"
    );
    let api_url = reqwest::Url::parse(&env::var("OPENFGA_API_URL").context("set OPENFGA_API_URL")?)
        .context("OPENFGA_API_URL must be an absolute URL")?;
    let host = api_url.host_str().unwrap_or_default();
    let private_http =
        api_url.scheme() == "http" && matches!(host, "localhost" | "127.0.0.1" | "::1" | "openfga");
    ensure!(
        api_url.scheme() == "https" || private_http,
        "OpenFGA API URL must use HTTPS, loopback, or the private Compose service name"
    );
    ensure!(
        api_url.username().is_empty() && api_url.password().is_none(),
        "OpenFGA API URL must not embed credentials"
    );
    let store_id = env::var("OPENFGA_STORE_ID").context("set OPENFGA_STORE_ID")?;
    let model_id =
        env::var("OPENFGA_AUTHORIZATION_MODEL_ID").context("set OPENFGA_AUTHORIZATION_MODEL_ID")?;
    ensure!(
        valid_openfga_id(&store_id) && valid_openfga_id(&model_id),
        "invalid OpenFGA store or model ID"
    );

    let pool = store::connect(database_url).await?;
    let client = pool.get().await?;
    let row = client
        .query_opt(
            "SELECT subject_id FROM watch_tower.identity_link
             WHERE issuer=$1 AND client_id=$2 AND external_subject=$3 AND revoked_at IS NULL",
            &[
                &request.issuer,
                &request.client_id,
                &request.external_subject,
            ],
        )
        .await?
        .context("role subject has no active Watch Tower identity link")?;
    let subject_id: String = row.get(0);
    let operation = if grant { "grant-role" } else { "revoke-role" };
    let audit_id: i64 = client
        .query_one(
            "INSERT INTO watch_tower.mcp_audit
             (client_id,operation,tool_name,outcome,actor_id,subject_id,authorization_allowed)
             VALUES($1,'operator_access_change',$2,'started',$3,$4,true) RETURNING id",
            &[
                &request.client_id,
                &operation,
                &request.actor_identity,
                &subject_id,
            ],
        )
        .await?
        .get(0);
    drop(client);

    let tuple = json!({
        "user": store::fga_user_key(&request.issuer, &request.external_subject),
        "relation": request.role,
        "object": format!("person:{subject_id}"),
    });
    let body = if grant {
        json!({
            "authorization_model_id": model_id,
            "writes": {"tuple_keys": [tuple], "on_duplicate": "ignore"}
        })
    } else {
        json!({
            "authorization_model_id": model_id,
            "deletes": {"tuple_keys": [tuple], "on_missing": "ignore"}
        })
    };
    let endpoint = format!(
        "{}/stores/{store_id}/write",
        api_url.as_str().trim_end_matches('/')
    );
    let http = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()?;
    let mut outbound = http.post(endpoint).json(&body);
    if let Ok(token) = env::var("OPENFGA_API_TOKEN") {
        if !token.trim().is_empty() {
            outbound = outbound.bearer_auth(token);
        }
    }
    let fga_result = outbound
        .send()
        .await
        .and_then(reqwest::Response::error_for_status);
    let succeeded = fga_result.is_ok();
    let audit_result = pool
        .get()
        .await?
        .execute(
            "UPDATE watch_tower.mcp_audit SET outcome=$2,finished_at=now()
             WHERE id=$1 AND outcome='started'",
            &[&audit_id, if succeeded { &"succeeded" } else { &"failed" }],
        )
        .await?;
    ensure!(
        audit_result == 1,
        "role change audit record was not finalized"
    );
    fga_result.context("OpenFGA rejected the role relationship change")?;
    Ok(json!({
        "role_updated": true,
        "action": operation,
        "subject_id": subject_id,
        "role": request.role,
    }))
}

fn valid_openfga_id(value: &str) -> bool {
    (1..=128).contains(&value.len())
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
}

async fn provision(database_url: &str, request: ProvisionRequest) -> Result<serde_json::Value> {
    validate_identity(
        &request.issuer,
        &request.client_id,
        &request.external_subject,
    )?;
    ensure!(
        valid_token(&request.operation_id, 120)
            && valid_token(&request.subject_id, 160)
            && valid_token(&request.fitness_owner_id, 160)
            && valid_token(&request.granted_by_identity, 255),
        "invalid operator provisioning identifier"
    );
    ensure!(
        request.timezone.len() <= 128 && !request.timezone.trim().is_empty(),
        "invalid subject timezone"
    );
    if let Some(user_id) = request.openhuman_user_id.as_deref() {
        ensure!(valid_token(user_id, 255), "invalid OpenHuman user ID");
    }
    ensure!(
        request.consent_scopes.len() <= 2
            && request
                .consent_scopes
                .iter()
                .all(|scope| matches!(scope.as_str(), "records.read" | "records.write")),
        "consent scopes must be records.read and/or records.write"
    );

    let hsm_pool = avatar_hsm::store::connect(database_url)
        .await
        .context("applying HSM schemas before subject provisioning")?;
    drop(hsm_pool);
    let policy_pool = store::connect(database_url).await?;
    let mut client = policy_pool.get().await?;
    let tx = client.transaction().await?;
    let timezone_exists: bool = tx
        .query_one(
            "SELECT EXISTS (SELECT 1 FROM pg_timezone_names WHERE name=$1)",
            &[&request.timezone],
        )
        .await?
        .get(0);
    ensure!(timezone_exists, "unknown subject timezone");

    tx.execute(
        "INSERT INTO avatar_hsm.subject(id,timezone) VALUES($1,$2)
         ON CONFLICT(id) DO UPDATE SET timezone=EXCLUDED.timezone",
        &[&request.subject_id, &request.timezone],
    )
    .await
    .context("provisioning HSM subject")?;
    tx.execute(
        "INSERT INTO watch_tower.identity_link
            (issuer,client_id,external_subject,subject_id,fitness_owner_id,openhuman_user_id,revoked_at)
         VALUES($1,$2,$3,$4,$5,$6,NULL)
         ON CONFLICT(issuer,client_id,external_subject) DO UPDATE SET
            subject_id=EXCLUDED.subject_id,
            fitness_owner_id=EXCLUDED.fitness_owner_id,
            openhuman_user_id=COALESCE(EXCLUDED.openhuman_user_id,watch_tower.identity_link.openhuman_user_id),
            linked_at=now(),revoked_at=NULL",
        &[
            &request.issuer,
            &request.client_id,
            &request.external_subject,
            &request.subject_id,
            &request.fitness_owner_id,
            &request.openhuman_user_id,
        ],
    )
    .await
    .context("linking external identity to Watch Tower subject")?;

    for scope in &request.consent_scopes {
        let grant_id = format!("provision:{}:{scope}", request.operation_id);
        if grant_id.len() > 255 {
            bail!("operator provisioning ID is too long for consent grant IDs");
        }
        let lock_key = format!(
            "consent:{}:{}:{}",
            request.subject_id, request.client_id, scope
        );
        tx.query_one(
            "SELECT pg_advisory_xact_lock(hashtextextended($1,0))",
            &[&lock_key],
        )
        .await?;
        let existing = tx
            .query_opt(
                "SELECT subject_id,client_id,scope,granted_by_identity FROM watch_tower.consent_grant WHERE grant_id=$1 FOR UPDATE",
                &[&grant_id],
            )
            .await?;
        if let Some(row) = existing {
            ensure!(
                row.get::<_, String>(0) == request.subject_id
                    && row.get::<_, String>(1) == request.client_id
                    && row.get::<_, String>(2) == *scope
                    && row.get::<_, String>(3) == request.granted_by_identity,
                "operation_id was reused for a different consent grant"
            );
            continue;
        }
        let version: i64 = tx
            .query_one(
                "SELECT COALESCE(MAX(version),0)+1 FROM watch_tower.consent_grant
                 WHERE subject_id=$1 AND client_id=$2 AND scope=$3",
                &[&request.subject_id, &request.client_id, scope],
            )
            .await?
            .get(0);
        tx.execute(
            "INSERT INTO watch_tower.consent_grant
                (grant_id,subject_id,client_id,scope,version,granted_by_identity)
             VALUES($1,$2,$3,$4,$5,$6)",
            &[
                &grant_id,
                &request.subject_id,
                &request.client_id,
                scope,
                &version,
                &request.granted_by_identity,
            ],
        )
        .await?;
    }
    tx.execute(
        "INSERT INTO watch_tower.mcp_audit
            (client_id,operation,tool_name,outcome,finished_at,actor_id,subject_id,authorization_allowed)
         VALUES($1,'operator_provision','provision-person','succeeded',now(),$2,$3,true)",
        &[
            &request.client_id,
            &request.granted_by_identity,
            &request.subject_id,
        ],
    )
    .await?;
    tx.commit().await?;
    Ok(json!({
        "provisioned": true,
        "subject_id": request.subject_id,
        "identity_linked": true,
        "consent_scopes": request.consent_scopes,
    }))
}

fn valid_token(value: &str, maximum: usize) -> bool {
    !value.trim().is_empty() && value.len() <= maximum && !value.chars().any(char::is_control)
}

async fn revoke_consent(
    database_url: &str,
    request: RevokeConsentRequest,
) -> Result<serde_json::Value> {
    validate_identity(
        &request.issuer,
        &request.client_id,
        &request.external_subject,
    )?;
    ensure!(
        matches!(request.scope.as_str(), "records.read" | "records.write")
            && valid_token(&request.actor_identity, 255),
        "invalid consent revocation request"
    );
    let pool = store::connect(database_url).await?;
    let mut client = pool.get().await?;
    let tx = client.transaction().await?;
    let row = tx
        .query_opt(
            "SELECT grant_id,subject_id,version,revoked_at IS NOT NULL
             FROM watch_tower.consent_grant
             WHERE client_id=$1 AND scope=$2 AND subject_id=(
                SELECT subject_id FROM watch_tower.identity_link
                WHERE issuer=$3 AND client_id=$1 AND external_subject=$4 AND revoked_at IS NULL
             ) ORDER BY version DESC LIMIT 1 FOR UPDATE",
            &[
                &request.client_id,
                &request.scope,
                &request.issuer,
                &request.external_subject,
            ],
        )
        .await?
        .context("no consent grant found for the linked identity")?;
    let grant_id: String = row.get(0);
    let subject_id: String = row.get(1);
    let version: i64 = row.get(2);
    let already_revoked: bool = row.get(3);
    if !already_revoked {
        tx.execute(
            "UPDATE watch_tower.consent_grant SET revoked_at=now() WHERE grant_id=$1",
            &[&grant_id],
        )
        .await?;
    }
    audit_operator_action(
        &tx,
        &request.client_id,
        &request.actor_identity,
        &subject_id,
        "revoke-consent",
    )
    .await?;
    tx.commit().await?;
    Ok(json!({
        "revoked": !already_revoked,
        "already_revoked": already_revoked,
        "subject_id": subject_id,
        "scope": request.scope,
        "version": version,
    }))
}

async fn revoke_identity(
    database_url: &str,
    request: RevokeIdentityRequest,
) -> Result<serde_json::Value> {
    validate_identity(
        &request.issuer,
        &request.client_id,
        &request.external_subject,
    )?;
    ensure!(
        valid_token(&request.actor_identity, 255),
        "invalid operator identity"
    );
    let pool = store::connect(database_url).await?;
    let mut client = pool.get().await?;
    let tx = client.transaction().await?;
    let row = tx
        .query_opt(
            "UPDATE watch_tower.identity_link SET revoked_at=COALESCE(revoked_at,now())
             WHERE issuer=$1 AND client_id=$2 AND external_subject=$3
             RETURNING subject_id,revoked_at",
            &[
                &request.issuer,
                &request.client_id,
                &request.external_subject,
            ],
        )
        .await?
        .context("no identity link found to revoke")?;
    let subject_id: String = row.get(0);
    audit_operator_action(
        &tx,
        &request.client_id,
        &request.actor_identity,
        &subject_id,
        "revoke-identity",
    )
    .await?;
    tx.commit().await?;
    Ok(json!({
        "revoked": true,
        "subject_id": subject_id,
        "identity_link_revoked": true,
    }))
}

fn validate_identity(issuer: &str, client_id: &str, external_subject: &str) -> Result<()> {
    ensure!(
        (issuer.starts_with("https://")
            || issuer.starts_with("http://127.0.0.1")
            || issuer.starts_with("http://localhost"))
            && valid_token(issuer, 512)
            && valid_token(client_id, 200)
            && valid_token(external_subject, 255),
        "invalid external identity"
    );
    Ok(())
}

async fn audit_operator_action(
    tx: &tokio_postgres::Transaction<'_>,
    client_id: &str,
    actor_identity: &str,
    subject_id: &str,
    action: &str,
) -> Result<()> {
    tx.execute(
        "INSERT INTO watch_tower.mcp_audit
            (client_id,operation,tool_name,outcome,finished_at,actor_id,subject_id,authorization_allowed)
         VALUES($1,'operator_access_change',$2,'succeeded',now(),$3,$4,true)",
        &[&client_id, &action, &actor_identity, &subject_id],
    )
    .await?;
    Ok(())
}
