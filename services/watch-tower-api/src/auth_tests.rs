use super::{token_scopes, Authenticator, OidcConfig, OpenFgaConfig, Principal};
use axum::{
    extract::State,
    routing::{get, post},
    Json, Router,
};
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine};
use ring::signature::{RsaKeyPair, RSA_PKCS1_SHA256};
use serde_json::json;
use std::{
    collections::HashMap,
    net::IpAddr,
    sync::Arc,
    time::{Instant, SystemTime, UNIX_EPOCH},
};
use tokio::sync::{Mutex, RwLock};
use watch_tower_core::policy::ExternalIdentity;

// Test-only signing key; it is never used by a runtime or deployment.
const TEST_PRIVATE_KEY_PKCS8_BASE64: &str = "MIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQCKYFFBlqcuedCDuifDharJbvezgBlMMlkSjmY5NDDetJXUXis4RbJP4bWtDk7lPJW1CFZ2ymDD8Plf3T9wbA09IaYmNiWIPWE3zjnQ0SnQYmse9PjAQmNgfyD+L8Dt7D3CiMvCb2EKIEPQQy+ZNjVEmTuSa+F2UtF8rD00KEoN+hHGgzXSNN5kXd5V2zmp9gTevqQCfBPLlBevTAgNhoNmdtQUVWdqijhrtB1LowTKBMd2NzT7aBaHn1rm9CFEjxBBoLeg2xBboV+yUmsZUvcRsFjfGxCOBUMDm/7WtNrJScBGGsWaN94vm3+ktq8eG0pFiHgx8p1eeOshWjQZygy/AgMBAAECggEABEXADIXhyJITSZXsV2GfqdOGy2CHp24giO2jo12EqsOpoqJ7tCOWw1elO+AmuHY76xiETuulvDcLBBY5OLflWr9YuXJPi1rVzxrLdzKWF2yKgSywJ4OFPPv/4UlJLC9worqFjA3TUbQ4U8xXQmRFlcYjbDxvfC9Hb5GD3EHoM3E7DkdOqVRj70Ar6SNDI6ETQ9UfyJhRytItYIYP6TGRK5UaQDdjJMswd9sW+uXHtLDQekvXbb7kOaON3QxGO3+iNOqYnXiMM9kZQYUIv0eM2KRZapIAEDTeAWIUQlu28IImV1G6iEf1GW5WDjUI+UgFnMeKkDXCQtgy8o/Kt5ZMUQKBgQDACiIG7k4BD0zgUh40IWl0YEMnFkOI2rqisP75UQLvXIT1S8TO2xnX+T1TcDz9/guNQko1wlGZYCHEIc9ofWNQ+36e+AJ7Wmp6S41J9sA9IY1YNRLxpf5gxpcWMvtP1/zSHotZ1ZKpascr3PHayADyT/YJG3YIH+oA6Vdc7hqPjwKBgQC4drAimeKHmgC3AF0nErzn7TIYcX1hXQW2L4W+bmUK4RwVZZ0jcv6bplPD2WeYSnwFXpVr0uNBLts1Z7Ig3TOAdCVRhNUUu7hux7S2UWilCfvnDi19Ecj96cM68clyHiNjwLIq7cN1PdkCjZ/dV47wbuEEyOSk2w96mdbHTvsX0QKBgERW1skUoNmrsjXZwznV8gsh/oO4XowjGmuvBO2AMMUxbXdFYWIjgkzjOzmoJWxVrZK1VmdPIW6tURbU2VRb2q4E8YhsxaVUBbRDkqIRl1Opzvzm1jj8U0vpH/ccaWZ+fE/h4sEzSZ/UidBILiEnL38LiYiwJw65HLL6VsaRTQ5tAoGAbU3XHQm2k8eLuOS8ThTqXzvMPXOyaeknI0eRpr/fm8y8SWmG7aSMfkrWw+ylWDZ6uuvK+rNh+hP7zO9/O74XcHxiSKW/ZaO+q9YmnQosSe9alxVtmvjY8qx8BQeU6Mw3CDr82Dc/ZM+bMcod+piRmKL7L0yjP5SW3X0fKccBPqECgYEAtS6tc06oW6t7lZS7RqbM2cBw9HeXg8IGH/zu2E540BznmODC1woX9hl7xRO1YG+5LEbUCLlYD0WlDL4nDat+vW348wzf4ndRpK1X0BK72ZjsgjXTNGyL3Cc+eIOpNAcTMJGxCXSJmOxg5zg6csFK/A74RvF6+HOFUyPr8O7wNgM=";
const TEST_JWK_N: &str = "imBRQZanLnnQg7onw4WqyW73s4AZTDJZEo5mOTQw3rSV1F4rOEWyT-G1rQ5O5TyVtQhWdspgw_D5X90_cGwNPSGmJjYliD1hN8450NEp0GJrHvT4wEJjYH8g_i_A7ew9wojLwm9hCiBD0EMvmTY1RJk7kmvhdlLRfKw9NChKDfoRxoM10jTeZF3eVds5qfYE3r6kAnwTy5QXr0wIDYaDZnbUFFVnaoo4a7QdS6MEygTHdjc0-2gWh59a5vQhRI8QQaC3oNsQW6FfslJrGVL3EbBY3xsQjgVDA5v-1rTayUnARhrFmjfeL5t_pLavHhtKRYh4MfKdXnjrIVo0GcoMvw";

#[test]
fn token_scopes_accept_standard_scope_string() {
    assert_eq!(
        token_scopes(&json!({"scope":"openid records.read records.write"})),
        ["openid", "records.read", "records.write"]
    );
}

#[test]
fn token_scopes_accept_array_claim_and_fail_closed_when_absent() {
    assert_eq!(
        token_scopes(&json!({"scp":["records.read", "records.write"]})),
        ["records.read", "records.write"]
    );
    assert!(token_scopes(&json!({"scope": null, "scp": 42})).is_empty());
}

#[test]
fn oauth_principal_must_have_the_scope_for_each_mcp_operation() {
    let principal = Principal {
        actor_id: "oidc:test:person".into(),
        client_id: "chatgpt-watch-tower".into(),
        can_write: true,
        identity: None,
        scopes: vec!["records.read".into()],
        enforce_oauth_scopes: true,
    };
    assert!(principal.has_scope("records.read"));
    assert!(!principal.has_scope("records.write"));
}

#[test]
fn openhuman_private_service_principal_uses_its_existing_delegation_policy() {
    let principal = Principal {
        actor_id: "service:control".into(),
        client_id: "openhuman-local".into(),
        can_write: true,
        identity: None,
        scopes: Vec::new(),
        enforce_oauth_scopes: false,
    };
    assert!(principal.has_scope("records.read"));
    assert!(principal.has_scope("records.write"));
}

#[tokio::test]
async fn authenticates_openhuman_gemini_and_chatgpt_clients_for_one_mcp_audience() {
    let authenticator = test_authenticator();
    for client_id in [
        "openhuman-watch-tower",
        "gemini-watch-tower",
        "chatgpt-watch-tower",
    ] {
        let principal = authenticator
            .authenticate(
                &signed_token(client_id, "records.read records.write"),
                IpAddr::from([127, 0, 0, 1]),
            )
            .await
            .unwrap()
            .unwrap();
        assert_eq!(principal.client_id, client_id);
        assert_eq!(
            principal.identity.as_ref().unwrap().issuer,
            "https://identity.example/realms/health"
        );
        assert!(principal.has_scope("records.read"));
        assert!(principal.has_scope("records.write"));
    }
}

#[tokio::test]
async fn oidc_authentication_rejects_unregistered_client_and_invalid_audience() {
    let authenticator = test_authenticator();
    let unknown_client = signed_token("unregistered-client", "records.read");
    assert!(authenticator
        .authenticate(&unknown_client, IpAddr::from([127, 0, 0, 1]))
        .await
        .unwrap()
        .is_none());

    let wrong_audience = signed_token_with("chatgpt-watch-tower", "records.read", "other-api");
    assert!(authenticator
        .authenticate(&wrong_audience, IpAddr::from([127, 0, 0, 1]))
        .await
        .unwrap()
        .is_none());
}

#[tokio::test]
async fn oidc_authentication_fetches_and_caches_the_signing_key_from_jwks() {
    let (jwks_url, server) = start_jwks_mock().await;
    let mut authenticator = test_authenticator();
    authenticator.oidc.as_mut().unwrap().jwks_url = jwks_url;
    authenticator.jwks = Arc::new(RwLock::new(None));

    let principal = authenticator
        .authenticate(
            &signed_token("gemini-watch-tower", "records.read"),
            IpAddr::from([127, 0, 0, 1]),
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(principal.client_id, "gemini-watch-tower");
    assert!(authenticator.jwks.read().await.is_some());
    server.abort();
}

#[tokio::test]
async fn openfga_relationship_check_uses_opaque_identity_and_subject_object() {
    let (url, state, server) = start_openfga_mock(true).await;
    let mut authenticator = test_authenticator();
    authenticator.openfga = Some(test_openfga_config(url));
    let identity = ExternalIdentity {
        issuer: "https://identity.example/realms/health".into(),
        subject: "person-oidc-42".into(),
        client_id: "chatgpt-watch-tower".into(),
    };
    let principal = Principal {
        actor_id: "oidc:health:person-42".into(),
        client_id: identity.client_id.clone(),
        can_write: true,
        identity: Some(identity.clone()),
        scopes: vec!["records.read".into()],
        enforce_oauth_scopes: true,
    };

    assert!(authenticator
        .has_relationship(&principal, "subject-42", "read")
        .await
        .unwrap());
    let requests = state.requests.lock().await;
    let tuple = &requests[0]["tuple_key"];
    assert_eq!(
        tuple["user"],
        crate::store::fga_user_key(&identity.issuer, &identity.subject)
    );
    assert_eq!(tuple["relation"], "can_read");
    assert_eq!(tuple["object"], "person:subject-42");
    assert!(!tuple["user"].as_str().unwrap().contains("person-oidc-42"));
    server.abort();
}

#[tokio::test]
async fn openfga_role_query_maps_assigned_reviewer_to_openhuman_account() {
    let (url, state, server) = start_openfga_mock(true).await;
    let mut authenticator = test_authenticator();
    authenticator.openfga = Some(test_openfga_config(url));
    let fga_user =
        crate::store::fga_user_key("https://identity.example/realms/health", "reviewer-17");
    let identity_users = HashMap::from([(fga_user, "openhuman-user-17".into())]);

    let users = authenticator
        .users_for_role("trainer", "subject-42", &identity_users)
        .await
        .unwrap();
    assert_eq!(users, ["openhuman-user-17"]);
    let requests = state.requests.lock().await;
    assert_eq!(requests[0]["tuple_key"]["object"], "person:subject-42");
    assert_eq!(requests[0]["tuple_key"]["relation"], "trainer");
    server.abort();
}

fn test_authenticator() -> Authenticator {
    Authenticator {
        read_token: "unused-read-token".into(),
        control_token: "unused-control-token".into(),
        allow_dev_bearers: false,
        allow_private_service_bearers: false,
        require_openhuman_identity: true,
        oidc: Some(OidcConfig {
            issuer: "https://identity.example/realms/health".into(),
            audience: "health-watch-tower".into(),
            jwks_url: "https://unused.invalid/jwks".into(),
            client_ids: [
                "openhuman-watch-tower",
                "gemini-watch-tower",
                "chatgpt-watch-tower",
            ]
            .into_iter()
            .map(str::to_owned)
            .collect(),
            public_url: "https://health.example".into(),
        }),
        openfga: None,
        http: reqwest::Client::new(),
        jwks: Arc::new(RwLock::new(Some((
            Instant::now(),
            serde_json::json!({"keys":[{
                "kid":"watch-tower-test-key",
                "kty":"RSA",
                "use":"sig",
                "n":TEST_JWK_N,
                "e":"AQAB"
            }]}),
        )))),
    }
}

fn test_openfga_config(api_url: String) -> OpenFgaConfig {
    OpenFgaConfig {
        api_url,
        store_id: "watchtower-store".into(),
        model_id: "watchtower-model".into(),
        token: None,
    }
}

#[derive(Clone)]
struct OpenFgaMockState {
    allowed: bool,
    requests: Arc<Mutex<Vec<serde_json::Value>>>,
}

async fn start_openfga_mock(
    allowed: bool,
) -> (String, OpenFgaMockState, tokio::task::JoinHandle<()>) {
    let state = OpenFgaMockState {
        allowed,
        requests: Arc::new(Mutex::new(Vec::new())),
    };
    let app = Router::new()
        .route("/stores/{store_id}/check", post(openfga_check))
        .route("/stores/{store_id}/read", post(openfga_read))
        .with_state(state.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    (format!("http://{address}"), state, server)
}

async fn start_jwks_mock() -> (String, tokio::task::JoinHandle<()>) {
    let modulus = TEST_JWK_N.to_owned();
    let app = Router::new().route(
        "/jwks",
        get(move || {
            let modulus = modulus.clone();
            async move {
                Json(json!({"keys":[{
                    "kid":"watch-tower-test-key",
                    "kty":"RSA",
                    "use":"sig",
                    "n":modulus,
                    "e":"AQAB"
                }]}))
            }
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    (format!("http://{address}/jwks"), server)
}

async fn openfga_check(
    State(state): State<OpenFgaMockState>,
    Json(body): Json<serde_json::Value>,
) -> Json<serde_json::Value> {
    state.requests.lock().await.push(body);
    Json(json!({"allowed":state.allowed}))
}

async fn openfga_read(
    State(state): State<OpenFgaMockState>,
    Json(body): Json<serde_json::Value>,
) -> Json<serde_json::Value> {
    state.requests.lock().await.push(body);
    Json(json!({
        "tuples":[{"key":{"user":crate::store::fga_user_key(
            "https://identity.example/realms/health", "reviewer-17"
        )}}],
        "continuation_token":""
    }))
}

fn signed_token(client_id: &str, scopes: &str) -> String {
    signed_token_with(client_id, scopes, "health-watch-tower")
}

fn signed_token_with(client_id: &str, scopes: &str, audience: &str) -> String {
    let private_key = base64::engine::general_purpose::STANDARD
        .decode(TEST_PRIVATE_KEY_PKCS8_BASE64)
        .unwrap();
    let key_pair = RsaKeyPair::from_pkcs8(&private_key).unwrap();
    let header =
        URL_SAFE_NO_PAD.encode(br#"{"alg":"RS256","kid":"watch-tower-test-key","typ":"JWT"}"#);
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let claims = serde_json::json!({
        "iss":"https://identity.example/realms/health",
        "sub":"person-oidc-42",
        "aud":audience,
        "azp":client_id,
        "iat":now,
        "exp":now + 3600,
        "scope":scopes
    });
    let claims = URL_SAFE_NO_PAD.encode(serde_json::to_vec(&claims).unwrap());
    let signing_input = format!("{header}.{claims}");
    let mut signature = vec![0; key_pair.public().modulus_len()];
    key_pair
        .sign(
            &RSA_PKCS1_SHA256,
            &ring::rand::SystemRandom::new(),
            signing_input.as_bytes(),
            &mut signature,
        )
        .unwrap();
    format!("{signing_input}.{}", URL_SAFE_NO_PAD.encode(signature))
}
