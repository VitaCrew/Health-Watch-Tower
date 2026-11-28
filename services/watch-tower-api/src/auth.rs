use std::{
    env,
    net::IpAddr,
    sync::Arc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use anyhow::{ensure, Context, Result};
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine};
use ring::signature::{RsaPublicKeyComponents, RSA_PKCS1_2048_8192_SHA256};
use serde_json::Value;
use subtle::ConstantTimeEq;
use tokio::sync::RwLock;
use watch_tower_core::policy::ExternalIdentity;

#[derive(Clone, Debug)]
pub struct Principal {
    pub actor_id: String,
    pub client_id: String,
    pub can_write: bool,
    pub identity: Option<ExternalIdentity>,
    pub scopes: Vec<String>,
    pub enforce_oauth_scopes: bool,
}

impl Principal {
    pub fn has_scope(&self, required: &str) -> bool {
        !self.enforce_oauth_scopes || self.scopes.iter().any(|scope| scope == required)
    }
}

#[derive(Clone)]
struct OidcConfig {
    issuer: String,
    audience: String,
    jwks_url: String,
    client_ids: Vec<String>,
    public_url: String,
}

#[derive(Clone)]
struct OpenFgaConfig {
    api_url: String,
    store_id: String,
    model_id: String,
    token: Option<String>,
}

#[derive(Clone)]
pub struct Authenticator {
    read_token: String,
    control_token: String,
    allow_dev_bearers: bool,
    allow_private_service_bearers: bool,
    require_openhuman_identity: bool,
    oidc: Option<OidcConfig>,
    openfga: Option<OpenFgaConfig>,
    http: reqwest::Client,
    jwks: Arc<RwLock<Option<(Instant, Value)>>>,
}

impl Authenticator {
    pub fn from_env(read_token: String, control_token: String, loopback: bool) -> Result<Self> {
        let issuer = optional_env("WATCH_TOWER_OIDC_ISSUER");
        let audience = optional_env("WATCH_TOWER_OIDC_AUDIENCE");
        let jwks_url = optional_env("WATCH_TOWER_OIDC_JWKS_URL");
        let oidc_configured = issuer.is_some() || audience.is_some() || jwks_url.is_some();
        let oidc = if oidc_configured {
            let issuer = issuer.context("set WATCH_TOWER_OIDC_ISSUER")?;
            let audience = audience.context("set WATCH_TOWER_OIDC_AUDIENCE")?;
            let jwks_url = jwks_url.context("set WATCH_TOWER_OIDC_JWKS_URL")?;
            ensure!(
                issuer.starts_with("https://") || loopback,
                "OIDC issuer must use HTTPS"
            );
            ensure!(
                jwks_url.starts_with("https://") || loopback,
                "OIDC JWKS URL must use HTTPS"
            );
            let client_ids: Vec<String> = env::var("WATCH_TOWER_OIDC_CLIENT_IDS")
                .unwrap_or_default()
                .split(',')
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .map(str::to_owned)
                .collect();
            ensure!(
                !client_ids.is_empty(),
                "set WATCH_TOWER_OIDC_CLIENT_IDS to the allowed MCP client IDs"
            );
            let public_url = env::var("WATCH_TOWER_PUBLIC_URL")
                .context("set WATCH_TOWER_PUBLIC_URL for OAuth-protected resource metadata")?;
            ensure!(
                public_url.starts_with("https://") || loopback,
                "public Watch Tower URL must use HTTPS"
            );
            Some(OidcConfig {
                issuer,
                audience,
                jwks_url,
                client_ids,
                public_url,
            })
        } else {
            None
        };
        let allow_dev_bearers = env::var("WATCH_TOWER_ALLOW_DEV_BEARERS")
            .map(|value| value == "1")
            .unwrap_or(loopback);
        let allow_private_service_bearers = env::var("WATCH_TOWER_ALLOW_PRIVATE_SERVICE_BEARERS")
            .map(|value| value == "1")
            .unwrap_or(false);
        let require_openhuman_identity = env::var("WATCH_TOWER_REQUIRE_OPENHUMAN_IDENTITY")
            .map(|value| value == "1")
            .unwrap_or(false);
        ensure!(
            allow_dev_bearers || oidc.is_some() || allow_private_service_bearers,
            "configure OIDC or explicitly enable a restricted service bearer mode"
        );
        ensure!(
            loopback || !allow_dev_bearers,
            "development bearer tokens are allowed only on loopback"
        );
        let fga_url = optional_env("OPENFGA_API_URL");
        let fga_store = optional_env("OPENFGA_STORE_ID");
        let fga_model = optional_env("OPENFGA_AUTHORIZATION_MODEL_ID");
        let fga_configured = fga_url.is_some() || fga_store.is_some() || fga_model.is_some();
        let openfga = if fga_configured {
            Some(OpenFgaConfig {
                api_url: fga_url.context("set OPENFGA_API_URL")?,
                store_id: fga_store.context("set OPENFGA_STORE_ID")?,
                model_id: fga_model.context("set OPENFGA_AUTHORIZATION_MODEL_ID")?,
                token: optional_env("OPENFGA_API_TOKEN"),
            })
        } else {
            None
        };
        let private_service_only = allow_private_service_bearers && oidc.is_none();
        ensure!(
            loopback || openfga.is_some() || private_service_only,
            "configure OpenFGA before binding Watch Tower MCP outside loopback"
        );
        ensure!(
            oidc.is_none() || openfga.is_some(),
            "configure OpenFGA whenever OIDC clients are enabled"
        );
        Ok(Self {
            read_token,
            control_token,
            allow_dev_bearers,
            allow_private_service_bearers,
            require_openhuman_identity,
            oidc,
            openfga,
            http: reqwest::Client::builder()
                .timeout(Duration::from_secs(5))
                .build()?,
            jwks: Arc::new(RwLock::new(None)),
        })
    }

    pub fn is_external(&self, principal: &Principal) -> bool {
        principal.identity.is_some()
    }

    pub fn permits_openhuman_user_assertion(&self, principal: &Principal, peer_ip: IpAddr) -> bool {
        let private_peer = match peer_ip {
            IpAddr::V4(ip) => ip.is_loopback() || ip.is_private(),
            IpAddr::V6(ip) => ip.is_loopback() || ip.is_unique_local(),
        };
        principal.identity.is_none()
            && matches!(
                principal.actor_id.as_str(),
                "service:read" | "service:control"
            )
            && private_peer
            && (self.allow_dev_bearers || self.allow_private_service_bearers)
    }

    pub fn requires_openhuman_identity(&self) -> bool {
        self.require_openhuman_identity
    }

    pub fn protected_resource_metadata(&self) -> Option<Value> {
        let oidc = self.oidc.as_ref()?;
        Some(serde_json::json!({
            "resource": format!("{}/mcp", oidc.public_url.trim_end_matches('/')),
            "authorization_servers": [oidc.issuer],
            "scopes_supported": ["records.read", "records.write"],
            "bearer_methods_supported": ["header"]
        }))
    }

    pub fn resource_metadata_url(&self) -> Option<String> {
        let oidc = self.oidc.as_ref()?;
        Some(format!(
            "{}/.well-known/oauth-protected-resource/mcp",
            oidc.public_url.trim_end_matches('/')
        ))
    }

    pub async fn has_relationship(
        &self,
        principal: &Principal,
        subject_id: &str,
        action: &str,
    ) -> Result<bool> {
        let Some(identity) = &principal.identity else {
            return Ok(true);
        };
        let Some(config) = &self.openfga else {
            return Ok(false);
        };
        ensure!(
            matches!(action, "read" | "write"),
            "unsupported authorization action"
        );
        let user = crate::store::fga_user_key(&identity.issuer, &identity.subject);
        let object = format!("person:{subject_id}");
        let url = format!(
            "{}/stores/{}/check",
            config.api_url.trim_end_matches('/'),
            config.store_id
        );
        let mut request = self.http.post(url).json(&serde_json::json!({
            "authorization_model_id": config.model_id,
            "tuple_key": { "user": user, "relation": if action == "read" { "can_read" } else { "can_write" }, "object": object }
        }));
        if let Some(token) = &config.token {
            request = request.bearer_auth(token);
        }
        let result = request
            .send()
            .await?
            .error_for_status()?
            .json::<Value>()
            .await?;
        Ok(result["allowed"].as_bool().unwrap_or(false))
    }

    pub async fn users_for_role(
        &self,
        role: &str,
        subject_id: &str,
        identity_users: &std::collections::HashMap<String, String>,
    ) -> Result<Vec<String>> {
        ensure!(
            matches!(role, "caregiver" | "clinician" | "trainer" | "nutritionist"),
            "unsupported follow-up role"
        );
        let Some(config) = &self.openfga else {
            return Ok(Vec::new());
        };
        let object = format!("person:{subject_id}");
        let url = format!(
            "{}/stores/{}/read",
            config.api_url.trim_end_matches('/'),
            config.store_id
        );
        let mut continuation = None::<String>;
        let mut users = std::collections::HashSet::new();
        for _ in 0..10 {
            let mut body = serde_json::json!({
                "authorization_model_id": config.model_id,
                "tuple_key": { "object": object, "relation": role },
                "page_size": 100
            });
            if let Some(token) = &continuation {
                body["continuation_token"] = Value::String(token.clone());
            }
            let mut request = self.http.post(&url).json(&body);
            if let Some(token) = &config.token {
                request = request.bearer_auth(token);
            }
            let response = request
                .send()
                .await?
                .error_for_status()?
                .json::<Value>()
                .await?;
            for tuple in response["tuples"].as_array().into_iter().flatten() {
                if let Some(user) = tuple.pointer("/key/user").and_then(Value::as_str) {
                    if let Some(openhuman_user_id) = identity_users.get(user) {
                        users.insert(openhuman_user_id.clone());
                    }
                }
            }
            continuation = response["continuation_token"].as_str().map(str::to_owned);
            if continuation.is_none() {
                break;
            }
        }
        Ok(users.into_iter().collect())
    }

    pub async fn authenticate(&self, bearer: &str, peer_ip: IpAddr) -> Result<Option<Principal>> {
        let private_peer = match peer_ip {
            IpAddr::V4(ip) => ip.is_loopback() || ip.is_private(),
            IpAddr::V6(ip) => ip.is_loopback() || ip.is_unique_local(),
        };
        if self.allow_dev_bearers || (self.allow_private_service_bearers && private_peer) {
            if bool::from(self.read_token.as_bytes().ct_eq(bearer.as_bytes())) {
                return Ok(Some(Principal {
                    actor_id: "service:read".into(),
                    client_id: "openhuman-local".into(),
                    can_write: false,
                    identity: None,
                    scopes: Vec::new(),
                    enforce_oauth_scopes: false,
                }));
            }
            if bool::from(self.control_token.as_bytes().ct_eq(bearer.as_bytes())) {
                return Ok(Some(Principal {
                    actor_id: "service:control".into(),
                    client_id: "openhuman-local".into(),
                    can_write: true,
                    identity: None,
                    scopes: Vec::new(),
                    enforce_oauth_scopes: false,
                }));
            }
        }
        let Some(config) = &self.oidc else {
            return Ok(None);
        };
        let Some((header_part, claims_part, signature_part)) =
            bearer.split_once('.').and_then(|(header, rest)| {
                rest.split_once('.')
                    .map(|(claims, signature)| (header, claims, signature))
            })
        else {
            return Ok(None);
        };
        let header: Value = match decode_json(header_part) {
            Some(value) => value,
            None => return Ok(None),
        };
        if header["alg"] != "RS256" {
            return Ok(None);
        }
        let Some(kid) = header["kid"].as_str() else {
            return Ok(None);
        };
        let claims: Value = match decode_json(claims_part) {
            Some(value) => value,
            None => return Ok(None),
        };
        let mut keys = match self.jwks(false).await {
            Ok(value) => value,
            Err(error) => return Err(error),
        };
        if !keys["keys"].as_array().into_iter().flatten().any(|key| {
            key["kid"].as_str() == Some(kid)
                && key["kty"] == "RSA"
                && key["use"].as_str().is_none_or(|usage| usage == "sig")
        }) {
            keys = self.jwks(true).await?;
        }
        let Some(key) = keys["keys"].as_array().into_iter().flatten().find(|key| {
            key["kid"].as_str() == Some(kid)
                && key["kty"] == "RSA"
                && key["use"].as_str().is_none_or(|usage| usage == "sig")
        }) else {
            return Ok(None);
        };
        let (Some(n), Some(e)) = (
            key["n"].as_str().and_then(decode_bytes),
            key["e"].as_str().and_then(decode_bytes),
        ) else {
            return Ok(None);
        };
        let signature = match URL_SAFE_NO_PAD.decode(signature_part) {
            Ok(value) => value,
            Err(_) => return Ok(None),
        };
        let signed = format!("{header_part}.{claims_part}");
        let public_key = RsaPublicKeyComponents { n: &n, e: &e };
        if public_key
            .verify(&RSA_PKCS1_2048_8192_SHA256, signed.as_bytes(), &signature)
            .is_err()
        {
            return Ok(None);
        }
        let now = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs() as i64;
        let issuer = claims["iss"].as_str().unwrap_or_default();
        let subject = claims["sub"].as_str().unwrap_or_default();
        let client_id = claims["azp"]
            .as_str()
            .or_else(|| claims["client_id"].as_str())
            .unwrap_or_default();
        let audience_matches = claims["aud"].as_str() == Some(config.audience.as_str())
            || claims["aud"].as_array().is_some_and(|audiences| {
                audiences
                    .iter()
                    .any(|aud| aud.as_str() == Some(config.audience.as_str()))
            });
        let expiration = claims["exp"].as_i64().unwrap_or_default();
        let not_before = claims["nbf"].as_i64().unwrap_or(0);
        if issuer != config.issuer
            || subject.is_empty()
            || client_id.is_empty()
            || !audience_matches
            || expiration <= now
            || not_before > now
            || (!config.client_ids.is_empty()
                && !config.client_ids.iter().any(|allowed| allowed == client_id))
        {
            return Ok(None);
        }
        Ok(Some(Principal {
            actor_id: format!("oidc:{}:{}", issuer.trim_end_matches('/'), subject),
            client_id: client_id.to_owned(),
            can_write: true,
            identity: Some(ExternalIdentity {
                issuer: issuer.to_owned(),
                subject: subject.to_owned(),
                client_id: client_id.to_owned(),
            }),
            scopes: token_scopes(&claims),
            enforce_oauth_scopes: true,
        }))
    }

    async fn jwks(&self, refresh: bool) -> Result<Value> {
        let cached = self.jwks.read().await;
        if !refresh {
            if let Some((fetched, keys)) = cached.as_ref() {
                if fetched.elapsed() < Duration::from_secs(300) {
                    return Ok(keys.clone());
                }
            }
        }
        drop(cached);
        let config = self.oidc.as_ref().context("OIDC is not configured")?;
        let keys = self
            .http
            .get(&config.jwks_url)
            .send()
            .await?
            .error_for_status()?
            .json::<Value>()
            .await?;
        ensure!(
            keys["keys"].as_array().is_some_and(|keys| !keys.is_empty()),
            "OIDC JWKS contains no signing keys"
        );
        *self.jwks.write().await = Some((Instant::now(), keys.clone()));
        Ok(keys)
    }
}

fn token_scopes(claims: &Value) -> Vec<String> {
    if let Some(scope) = claims["scope"].as_str() {
        return scope.split_ascii_whitespace().map(str::to_owned).collect();
    }
    claims["scp"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(Value::as_str)
        .map(str::to_owned)
        .collect()
}

#[cfg(test)]
#[path = "auth_tests.rs"]
mod tests;

fn optional_env(name: &str) -> Option<String> {
    env::var(name).ok().filter(|value| !value.trim().is_empty())
}

fn decode_json(part: &str) -> Option<Value> {
    let bytes = URL_SAFE_NO_PAD.decode(part).ok()?;
    serde_json::from_slice(&bytes).ok()
}

fn decode_bytes(part: &str) -> Option<Vec<u8>> {
    URL_SAFE_NO_PAD.decode(part).ok()
}
