use anyhow::{ensure, Context, Result};
use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::{Html, IntoResponse, Response},
};
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine};
use deadpool_postgres::Pool;
use reqwest::{Client, Url};
use ring::{
    aead, digest,
    rand::{SecureRandom, SystemRandom},
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::{env, path::PathBuf, sync::Arc};

use crate::{
    cloud_wearables::{self, Provider},
    mcp::App,
};

#[cfg(test)]
#[path = "wearable_oauth_tests.rs"]
mod tests;

const GOOGLE_SCOPE: &str = "https://www.googleapis.com/auth/googlehealth.activity_and_fitness.readonly https://www.googleapis.com/auth/googlehealth.health_metrics_and_measurements.readonly https://www.googleapis.com/auth/googlehealth.sleep.readonly";

fn google_authorization_scope(additional: &str) -> String {
    let mut scope = GOOGLE_SCOPE.to_owned();
    for entry in ["ecg", "irn", "nutrition", "location"] {
        if additional
            .split_whitespace()
            .any(|requested| requested == entry)
        {
            scope.push_str(&format!(
                " https://www.googleapis.com/auth/googlehealth.{entry}.readonly"
            ));
        }
    }
    scope
}

#[cfg(test)]
#[test]
fn google_extra_scopes_are_opt_in_and_read_only() {
    assert_eq!(google_authorization_scope(""), GOOGLE_SCOPE);
    let scope =
        google_authorization_scope("ecg ecg irn nutrition location unknown .location.writeonly");
    for extra in ["ecg", "irn", "nutrition", "location"] {
        let suffix = format!(".{extra}.readonly");
        assert_eq!(
            scope
                .split_whitespace()
                .filter(|value| value.ends_with(&suffix))
                .count(),
            1
        );
    }
    assert_eq!(scope.split_whitespace().count(), 7);
}

pub(crate) fn google_tcx_scope(scope: &str) -> bool {
    ["activity_and_fitness", "location"]
        .into_iter()
        .all(|required| {
            scope.split_whitespace().any(|token| {
                token
                    .strip_prefix("https://www.googleapis.com/auth/googlehealth.")
                    .and_then(|suffix| suffix.strip_suffix(".readonly"))
                    == Some(required)
            })
        })
}

struct Contract {
    prefix: &'static str,
    authorize: &'static str,
    token: &'static str,
    scope: &'static str,
}

fn contract(provider: Provider) -> Option<Contract> {
    let (prefix, authorize, token, scope) = match provider {
        Provider::GoogleHealth => (
            "GOOGLE_HEALTH",
            "https://accounts.google.com/o/oauth2/v2/auth",
            "https://oauth2.googleapis.com/token",
            GOOGLE_SCOPE,
        ),
        Provider::Oura => (
            "OURA",
            "https://cloud.ouraring.com/oauth/authorize",
            "https://api.ouraring.com/oauth/token",
            "daily workout heartrate spo2 personal heart_health session tag",
        ),
        Provider::Whoop => (
            "WHOOP",
            "https://api.prod.whoop.com/oauth/oauth2/auth",
            "https://api.prod.whoop.com/oauth/oauth2/token",
            "read:sleep read:workout read:cycles read:recovery read:profile read:body_measurement offline",
        ),
        Provider::Strava => (
            "STRAVA",
            "https://www.strava.com/oauth/authorize",
            "https://www.strava.com/oauth/token",
            "activity:read_all",
        ),
        Provider::Polar => (
            "POLAR",
            "https://flow.polar.com/oauth2/authorization",
            "https://polarremote.com/v2/oauth2/token",
            "accesslink.read_all",
        ),
        Provider::Suunto => (
            "SUUNTO",
            "https://cloudapi-oauth.suunto.com/oauth/authorize",
            "https://cloudapi-oauth.suunto.com/oauth/token",
            "",
        ),
        Provider::Withings => (
            "WITHINGS",
            "https://account.withings.com/oauth2_user/authorize2",
            "https://wbsapi.withings.net/v2/oauth2",
            "user.metrics,user.activity",
        ),
        Provider::Ultrahuman => (
            "ULTRAHUMAN",
            "https://auth.ultrahuman.com/authorise",
            "https://partner.ultrahuman.com/api/partners/oauth/token",
            "ring_data",
        ),
        Provider::Sensorbio => (
            "SENSORBIO",
            "https://auth.sensorbio.com/authorize",
            "https://auth.sensorbio.com/token",
            "",
        ),
        Provider::Coros => (
            "COROS",
            "https://open.coros.com/oauth2/authorize",
            "https://open.coros.com/oauth2/accesstoken",
            "",
        ),
        Provider::IntervalsIcu => (
            "INTERVALS_ICU",
            "https://intervals.icu/oauth/authorize",
            "https://intervals.icu/api/oauth/token",
            "ACTIVITY:READ,WELLNESS:READ",
        ),
        Provider::Garmin => (
            "GARMIN",
            "https://connect.garmin.com/oauth2Confirm",
            "https://connectapi.garmin.com/di-oauth2-service/oauth/token",
            "",
        ),
        _ => return None,
    };
    Some(Contract {
        prefix,
        authorize,
        token,
        scope,
    })
}

struct Config {
    contract: Contract,
    client_id: String,
    client_secret: String,
    redirect: Url,
}

fn configuration(provider: Provider) -> Option<Config> {
    let contract = contract(provider)?;
    let client_id = env::var(format!("AVATAR_{}_CLIENT_ID", contract.prefix)).ok()?;
    let client_secret = env::var(format!("AVATAR_{}_CLIENT_SECRET", contract.prefix)).ok()?;
    let mut redirect = Url::parse(&env::var("AVATAR_WEARABLE_PUBLIC_URL").ok()?).ok()?;
    if redirect.scheme() != "https"
        || redirect.path() != "/"
        || redirect.query().is_some()
        || redirect.fragment().is_some()
        || !redirect.username().is_empty()
        || redirect.password().is_some()
        || client_id.trim().is_empty()
        || client_secret.trim().is_empty()
    {
        return None;
    }
    redirect.set_path(&format!("/wearables/oauth/{}/callback", provider.slug()));
    Some(Config {
        contract,
        client_id,
        client_secret,
        redirect,
    })
}

pub fn oauth_available(provider: Provider) -> bool {
    configuration(provider).is_some()
        && key().is_ok()
        && (!matches!(provider, Provider::Suunto)
            || env::var("AVATAR_SUUNTO_SUBSCRIPTION_KEY").is_ok_and(|key| !key.trim().is_empty()))
        && (!matches!(provider, Provider::Garmin) || crate::garmin_push::configured())
}

pub fn is_portal_origin(origin: &str) -> bool {
    env::var("AVATAR_WEARABLE_PUBLIC_URL")
        .ok()
        .and_then(|base| Url::parse(&base).ok())
        .is_some_and(|base| {
            base.scheme() == "https" && base.origin().ascii_serialization() == origin
        })
}

pub async fn portal() -> Response {
    ([
        ("cache-control","no-store"),
        ("referrer-policy","no-referrer"),
        ("content-security-policy","default-src 'none'; script-src 'self'; style-src 'self'; connect-src 'self'; frame-ancestors 'none'; base-uri 'none'"),
    ], Html(include_str!("../wearable-public/portal.html"))).into_response()
}

pub async fn portal_script() -> Response {
    (
        [
            ("content-type", "text/javascript; charset=utf-8"),
            ("x-content-type-options", "nosniff"),
        ],
        include_str!("../wearable-public/portal.js"),
    )
        .into_response()
}

pub async fn portal_style() -> Response {
    (
        [
            ("content-type", "text/css; charset=utf-8"),
            ("x-content-type-options", "nosniff"),
        ],
        include_str!("../wearable-public/portal.css"),
    )
        .into_response()
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Profile {
    subject: String,
    timezone: String,
    read_token: String,
    control_token: String,
}

pub async fn provision(pool: &Pool) -> Result<()> {
    let Some(path) = env::var_os("AVATAR_WEARABLE_PROFILES_FILE").map(PathBuf::from) else {
        return Ok(());
    };
    let profiles: Vec<Profile> =
        serde_json::from_slice(&std::fs::read(path).context("cannot read wearable profiles file")?)
            .map_err(|_| anyhow::anyhow!("invalid wearable profiles file"))?;
    let mut db = pool.get().await?;
    let tx = db.transaction().await?;
    for profile in profiles {
        ensure!(
            profile.subject.starts_with("wearable-")
                && (10..=96).contains(&profile.subject.len())
                && profile
                    .subject
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b == b'-')
                && profile.read_token != profile.control_token
                && [profile.read_token.as_str(), profile.control_token.as_str()]
                    .into_iter()
                    .all(valid_token),
            "invalid wearable profile"
        );
        let timezone_exists = tx
            .query_one(
                "SELECT EXISTS (SELECT 1 FROM pg_timezone_names WHERE name=$1)",
                &[&profile.timezone],
            )
            .await?
            .get::<_, bool>(0);
        ensure!(timezone_exists, "invalid wearable profile timezone");
        let collision = tx
            .query_one(
                "SELECT EXISTS (SELECT 1 FROM avatar_hsm.wearable_profile
            WHERE subject_id <> $1 AND (read_hash IN ($2,$3) OR control_hash IN ($2,$3)))",
                &[
                    &profile.subject,
                    &hash(&profile.read_token),
                    &hash(&profile.control_token),
                ],
            )
            .await?
            .get::<_, bool>(0);
        ensure!(!collision, "wearable profile credentials must be unique");
        tx.execute("INSERT INTO avatar_hsm.subject(id, timezone) VALUES($1,$2) ON CONFLICT (id) DO NOTHING",
            &[&profile.subject,&profile.timezone]).await?;
        tx.execute("INSERT INTO avatar_hsm.wearable_profile(subject_id, read_hash, control_hash)
            VALUES($1,$2,$3) ON CONFLICT (subject_id) DO UPDATE SET read_hash=excluded.read_hash, control_hash=excluded.control_hash",
            &[&profile.subject,&hash(&profile.read_token),&hash(&profile.control_token)]).await?;
    }
    tx.commit().await?;
    Ok(())
}

fn valid_token(token: &str) -> bool {
    (43..=128).contains(&token.len())
        && token
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"-_".contains(&b))
}

pub async fn authenticate(pool: &Pool, token: &str, write: bool) -> Result<Option<String>> {
    if !valid_token(token) {
        return Ok(None);
    }
    Ok(pool
        .get()
        .await?
        .query_opt(
            "SELECT subject_id FROM avatar_hsm.wearable_profile WHERE
         (CASE WHEN $2 THEN control_hash ELSE read_hash END)=$1",
            &[&hash(token), &write],
        )
        .await?
        .map(|row| row.get(0)))
}

fn hash(text: &str) -> Vec<u8> {
    digest::digest(&digest::SHA256, text.as_bytes())
        .as_ref()
        .to_vec()
}

fn random<const N: usize>() -> Result<[u8; N]> {
    let mut bytes = [0; N];
    SystemRandom::new()
        .fill(&mut bytes)
        .map_err(|_| anyhow::anyhow!("random generator unavailable"))?;
    Ok(bytes)
}

fn key() -> Result<aead::LessSafeKey> {
    let key = env::var("AVATAR_WEARABLE_OAUTH_KEY").context("OAuth encryption key required")?;
    let bytes = URL_SAFE_NO_PAD
        .decode(key)
        .map_err(|_| anyhow::anyhow!("invalid OAuth encryption key"))?;
    Ok(aead::LessSafeKey::new(
        aead::UnboundKey::new(&aead::AES_256_GCM, &bytes)
            .map_err(|_| anyhow::anyhow!("invalid OAuth encryption key length"))?,
    ))
}

fn aad(subject: &str, provider: Provider) -> String {
    format!("wearable-oauth-v1/{subject}/{}", provider.slug())
}

#[derive(Deserialize, Serialize)]
pub struct Tokens {
    pub access_token: String,
    pub refresh_token: Option<String>,
    #[serde(default)]
    pub scope: String,
    #[serde(default)]
    pub expires_at: Option<chrono::DateTime<chrono::Utc>>,
    #[serde(default, rename = "openId")]
    provider_user: Option<String>,
}

fn seal(
    key: &aead::LessSafeKey,
    tokens: &Tokens,
    subject: &str,
    provider: Provider,
) -> Result<Vec<u8>> {
    seal_value(key, serde_json::to_vec(tokens)?, &aad(subject, provider))
}

fn seal_value(key: &aead::LessSafeKey, mut bytes: Vec<u8>, context: &str) -> Result<Vec<u8>> {
    let nonce = random::<12>()?;
    key.seal_in_place_append_tag(
        aead::Nonce::assume_unique_for_key(nonce),
        aead::Aad::from(context),
        &mut bytes,
    )
    .map_err(|_| anyhow::anyhow!("cannot encrypt OAuth token"))?;
    Ok([nonce.as_slice(), &bytes].concat())
}

fn open(
    key: &aead::LessSafeKey,
    bytes: Vec<u8>,
    subject: &str,
    provider: Provider,
) -> Result<Tokens> {
    serde_json::from_slice(&open_value(key, bytes, &aad(subject, provider))?)
        .map_err(|_| anyhow::anyhow!("invalid OAuth token data"))
}

fn open_value(key: &aead::LessSafeKey, mut bytes: Vec<u8>, context: &str) -> Result<Vec<u8>> {
    ensure!(bytes.len() >= 28, "invalid encrypted OAuth data");
    let nonce: [u8; 12] = bytes[..12].try_into()?;
    let clear = key
        .open_in_place(
            aead::Nonce::assume_unique_for_key(nonce),
            aead::Aad::from(context),
            &mut bytes[12..],
        )
        .map_err(|_| anyhow::anyhow!("cannot decrypt OAuth token"))?;
    Ok(clear.to_vec())
}

pub(crate) fn withings_cleanup_payload(
    subject: &str,
    generation: &str,
    account: &[u8],
    tokens: Vec<u8>,
    callback: &Url,
) -> Result<Option<(Vec<u8>, chrono::DateTime<chrono::Utc>)>> {
    let cipher = key()?;
    let tokens = open(&cipher, tokens, subject, Provider::Withings)?;
    let now = chrono::Utc::now();
    let limit = now + chrono::Duration::minutes(30);
    let expires = tokens.expires_at.map_or(limit, |expiry| expiry.min(limit));
    if expires <= now {
        return Ok(None);
    }
    let sealed = seal_value(
        &cipher,
        serde_json::to_vec(&(tokens.access_token, callback.as_str()))?,
        &withings_cleanup_aad(subject, generation, account),
    )?;
    Ok(Some((sealed, expires)))
}

fn withings_cleanup_aad(subject: &str, generation: &str, account: &[u8]) -> String {
    format!(
        "withings-notify-cleanup-v1/{subject}/{generation}/{}",
        URL_SAFE_NO_PAD.encode(account)
    )
}

pub(crate) fn open_withings_cleanup(
    subject: &str,
    generation: &str,
    account: &[u8],
    sealed: Vec<u8>,
) -> Result<(String, String)> {
    Ok(serde_json::from_slice(&open_value(
        &key()?,
        sealed,
        &withings_cleanup_aad(subject, generation, account),
    )?)?)
}

#[cfg(test)]
pub(crate) fn synthetic_withings_credentials(subject: &str) -> Result<Vec<u8>> {
    seal(
        &key()?,
        &Tokens {
            access_token: "withings-cleanup-fixture-access".into(),
            refresh_token: Some("withings-cleanup-fixture-refresh".into()),
            scope: "user.activity".into(),
            expires_at: Some(chrono::Utc::now() + chrono::Duration::hours(2)),
            provider_user: None,
        },
        subject,
        Provider::Withings,
    )
}

pub fn seal_garmin_fit_url(subject: &str, activity: &str, url: &Url) -> Result<Vec<u8>> {
    seal_value(
        &key()?,
        url.as_str().as_bytes().to_vec(),
        &format!("garmin-fit-v1/{subject}/{activity}"),
    )
}

pub fn open_garmin_fit_url(subject: &str, activity: &str, sealed: Vec<u8>) -> Result<String> {
    String::from_utf8(open_value(
        &key()?,
        sealed,
        &format!("garmin-fit-v1/{subject}/{activity}"),
    )?)
    .context("invalid encrypted Garmin FIT URL")
}

pub(crate) fn seal_coros_fit_sources(
    subject: &str,
    activity: &str,
    links: &[(i16, Url)],
) -> Result<Vec<u8>> {
    let urls: Vec<_> = links
        .iter()
        .map(|(part, url)| (*part, url.as_str()))
        .collect();
    let context = serde_json::to_string(&("coros-fit-v1", subject, activity))?;
    seal_value(&key()?, serde_json::to_vec(&urls)?, &context)
}

pub(crate) fn open_coros_fit_sources(
    subject: &str,
    activity: &str,
    sealed: Vec<u8>,
) -> Result<Vec<(i16, String)>> {
    let context = serde_json::to_string(&("coros-fit-v1", subject, activity))?;
    Ok(serde_json::from_slice(&open_value(
        &key()?,
        sealed,
        &context,
    )?)?)
}

pub fn seal_garmin_pull_url(subject: &str, feed: &str, url: &Url) -> Result<Vec<u8>> {
    seal_value(
        &key()?,
        url.as_str().as_bytes().to_vec(),
        &format!("garmin-pull-v1/{subject}/{feed}"),
    )
}

pub fn open_garmin_pull_url(subject: &str, feed: &str, sealed: Vec<u8>) -> Result<String> {
    String::from_utf8(open_value(
        &key()?,
        sealed,
        &format!("garmin-pull-v1/{subject}/{feed}"),
    )?)
    .context("invalid encrypted Garmin pull URL")
}

pub async fn begin(pool: &Pool, subject: &str, provider: Provider) -> Result<Value> {
    ensure!(oauth_available(provider), "OAuth provider is unavailable");
    let config = configuration(provider).context("OAuth provider configuration required")?;
    key()?;
    let state = URL_SAFE_NO_PAD.encode(random::<32>()?);
    let verifier = URL_SAFE_NO_PAD.encode(random::<32>()?);
    let db = pool.get().await?;
    let changed = db.execute(
        "INSERT INTO avatar_hsm.wearable_oauth_state(state_hash, subject_id, provider, verifier, expires_at)
         SELECT $1,$2,$3,$4,now()+interval '10 minutes'
         WHERE NOT EXISTS (SELECT 1 FROM avatar_hsm.wearable_oauth_connection WHERE subject_id=$2 AND provider=$3)
         ON CONFLICT (subject_id,provider) DO UPDATE SET state_hash=excluded.state_hash,
             verifier=excluded.verifier, expires_at=excluded.expires_at, claimed_at=NULL",
        &[&hash(&state),&subject,&provider.slug(),&verifier],
    ).await?;
    ensure!(
        changed == 1,
        "disconnect the existing provider account first"
    );
    let mut url = Url::parse(config.contract.authorize)?;
    {
        let mut query = url.query_pairs_mut();
        query
            .append_pair("client_id", &config.client_id)
            .append_pair("redirect_uri", config.redirect.as_str())
            .append_pair("response_type", "code")
            .append_pair("state", &state);
        if matches!(provider, Provider::GoogleHealth) {
            let additional = env::var("AVATAR_GOOGLE_HEALTH_EXTRA_READ_SCOPES").unwrap_or_default();
            query
                .append_pair("scope", &google_authorization_scope(&additional))
                .append_pair("access_type", "offline")
                .append_pair("prompt", "consent");
        } else if matches!(provider, Provider::Ultrahuman)
            && env::var("AVATAR_ULTRAHUMAN_EXTENDED_METRICS").as_deref() == Ok("true")
        {
            query.append_pair("scope", "ring_data ring_extended_data");
        } else if !config.contract.scope.is_empty() {
            query.append_pair("scope", config.contract.scope);
        }
        if matches!(
            provider,
            Provider::GoogleHealth | Provider::Garmin | Provider::IntervalsIcu
        ) {
            query
                .append_pair("code_challenge_method", "S256")
                .append_pair("code_challenge", &URL_SAFE_NO_PAD.encode(hash(&verifier)));
        }
    }
    Ok(json!({"authorization_url":url.as_str(),"expires_in_seconds":600}))
}

#[derive(Deserialize)]
pub struct Callback {
    state: Option<String>,
    code: Option<String>,
    error: Option<String>,
    scope: Option<String>,
    user_id: Option<String>,
}

pub async fn callback(
    State(app): State<Arc<App>>,
    Path(slug): Path<String>,
    Query(reply): Query<Callback>,
) -> Response {
    let result = finish(&app.db, &slug, reply).await;
    let (status, page) = match result {
        Ok(()) => (StatusCode::OK, "<html lang=ko><meta charset=utf-8><title>OpenHuman</title><p>계정을 연결했습니다. OpenHuman에서 상태를 확인하세요.</p></html>"),
        Err(_) => (StatusCode::BAD_REQUEST, "<html lang=ko><meta charset=utf-8><title>OpenHuman</title><p>계정 연결을 완료하지 못했습니다. OpenHuman에서 다시 시작하세요.</p></html>"),
    };
    (
        status,
        [
            ("cache-control", "no-store"),
            ("referrer-policy", "no-referrer"),
            ("content-security-policy", "default-src 'none'"),
        ],
        Html(page),
    )
        .into_response()
}

async fn finish(pool: &Pool, slug: &str, reply: Callback) -> Result<()> {
    let provider = Provider::parse(slug)?;
    let state = reply
        .state
        .filter(|s| valid_token(s))
        .context("invalid OAuth state")?;
    let claim = pool
        .get()
        .await?
        .query_opt(
            "UPDATE avatar_hsm.wearable_oauth_state SET claimed_at=now()
         WHERE state_hash=$1 AND provider=$2 AND expires_at>now() AND claimed_at IS NULL
         RETURNING subject_id, verifier",
            &[&hash(&state), &slug],
        )
        .await?
        .context("expired or used OAuth state")?;
    ensure!(reply.error.is_none(), "OAuth denied");
    let code = reply
        .code
        .filter(|s| (1..=4096).contains(&s.len()))
        .context("missing OAuth code")?;
    let subject: String = claim.get(0);
    let verifier: String = claim.get(1);
    let config = configuration(provider).context("provider is unavailable")?;
    let client = cloud_wearables::http_client()?;
    let mut params = vec![
        ("grant_type", "authorization_code"),
        ("code", &code),
        ("redirect_uri", config.redirect.as_str()),
    ];
    if matches!(provider, Provider::IntervalsIcu) {
        params = vec![("code", &code), ("code_verifier", &verifier)];
    } else if matches!(provider, Provider::GoogleHealth | Provider::Garmin) {
        params.push(("code_verifier", &verifier));
    }
    if matches!(provider, Provider::Coros) {
        params.push((
            "user_id",
            reply
                .user_id
                .as_deref()
                .filter(|id| !id.is_empty() && id.len() <= 128)
                .context("COROS account identifier missing")?,
        ));
    }
    let mut tokens = exchange(&client, provider, &config, params).await?;
    if matches!(provider, Provider::IntervalsIcu) {
        ensure!(
            tokens.provider_user.is_some()
                && ["ACTIVITY", "WELLNESS"].iter().any(|kind| {
                    cloud_wearables::manufacturers::intervals_scope(&tokens.scope, kind)
                }),
            "Intervals.icu did not grant activity or wellness read access"
        );
    }
    if tokens.scope.is_empty() {
        tokens.scope = reply.scope.unwrap_or_default();
    }
    if !matches!(provider, Provider::Polar | Provider::IntervalsIcu) {
        ensure!(
            tokens.refresh_token.as_ref().is_some_and(|t| !t.is_empty()),
            "provider did not supply offline access"
        );
    }
    if matches!(provider, Provider::GoogleHealth) {
        let identity = cloud_wearables::body(
            client
                .get("https://health.googleapis.com/v4/users/me/identity")
                .bearer_auth(&tokens.access_token)
                .send()
                .await?,
        )
        .await?;
        ensure!(
            identity["healthUserId"]
                .as_str()
                .is_some_and(|s| !s.is_empty()),
            "Google Health account is not linked"
        );
        tokens.provider_user = crate::wearable_webhooks::account_id(&identity["healthUserId"]);
    }
    let mut garmin_permissions = 0i16;
    if matches!(provider, Provider::Garmin) {
        let identity = cloud_wearables::body(
            client
                .get("https://apis.garmin.com/wellness-api/rest/user/id")
                .bearer_auth(&tokens.access_token)
                .send()
                .await?,
        )
        .await?;
        tokens.provider_user = Some(
            crate::wearable_webhooks::account_id(&identity["userId"])
                .context("Garmin account ID missing")?,
        );
        garmin_permissions =
            crate::garmin_push::read_permissions(&client, &tokens.access_token).await?;
        ensure!(
            garmin_permissions != 0,
            "Garmin health or activity permission is required"
        );
    }
    let mut profile_retry_at = None;
    if tokens.provider_user.is_none() {
        if let Some(profile) = webhook_profile(provider) {
            match webhook_profile_id(&client, profile, &tokens.access_token, &tokens.scope).await {
                Ok(id) => tokens.provider_user = id,
                Err(error) => {
                    profile_retry_at = error
                        .downcast_ref::<cloud_wearables::ProviderThrottled>()
                        .map(|throttle| throttle.retry_at);
                }
            }
        }
    }
    if matches!(provider, Provider::Polar) {
        let response = client
            .post("https://www.polaraccesslink.com/v3/users")
            .bearer_auth(&tokens.access_token)
            .json(&json!({"member-id":subject}))
            .send()
            .await?;
        ensure!(
            response.status().is_success() || response.status() == reqwest::StatusCode::CONFLICT,
            "Polar account registration failed"
        );
    }
    let sealed = seal(&key()?, &tokens, &subject, provider)?;
    let mut db = pool.get().await?;
    let tx = db.transaction().await?;
    tx.query_one(
        "SELECT pg_advisory_xact_lock(hashtext($1),hashtext($2))",
        &[&subject, &slug],
    )
    .await?;
    tx.execute("INSERT INTO avatar_hsm.device_connection(subject_id,provider) VALUES($1,$2)
         ON CONFLICT (subject_id,provider) DO UPDATE SET state='pending' WHERE device_connection.state='revoked'",
        &[&subject,&slug]).await?;
    let active = tx.execute("DELETE FROM avatar_hsm.wearable_oauth_state
         WHERE subject_id=$1 AND provider=$2 AND state_hash=$3 AND claimed_at IS NOT NULL AND expires_at>now()",
        &[&subject,&slug,&hash(&state)]).await?;
    ensure!(active == 1, "OAuth connection changed during consent");
    let account_hash = tokens
        .provider_user
        .as_ref()
        .map(|id| crate::wearable_webhooks::hash(id.as_bytes()));
    if let (Provider::Withings, Some(account)) = (provider, &account_hash) {
        crate::withings_notify::lock_account(&tx, account).await?;
    }
    let google_location =
        matches!(provider, Provider::GoogleHealth) && google_tcx_scope(&tokens.scope);
    let intervals_activity = intervals_activity_grant(provider, &tokens.scope);
    tx.execute("INSERT INTO avatar_hsm.wearable_oauth_connection(subject_id,provider,sealed_tokens,provider_account_hash,garmin_permissions,google_location_granted,intervals_activity_granted) VALUES($1,$2,$3,$4,$5,$6,$7)",
        &[&subject,&slug,&sealed,&account_hash,&garmin_permissions,&google_location,&intervals_activity]).await?;
    if matches!(provider, Provider::Garmin) {
        crate::garmin_backfill::enqueue_initial(&tx, &subject, garmin_permissions).await?;
    } else {
        tx.execute(
            "INSERT INTO watch_tower.wearable_sync_schedule
             (subject_id,provider,backfill_from,throttled_until,next_live_sync)
             VALUES($1,$2,now()-interval '30 days',$3,
               CASE WHEN $2='oura' THEN now()+interval '50 years'
                    ELSE now()+interval '1 hour' END)",
            &[&subject, &slug, &profile_retry_at],
        )
        .await?;
    }
    tx.commit().await?;
    Ok(())
}

async fn exchange(
    client: &Client,
    provider: Provider,
    config: &Config,
    mut params: Vec<(&str, &str)>,
) -> Result<Tokens> {
    let endpoint = if matches!(provider, Provider::Coros)
        && params.contains(&("grant_type", "refresh_token"))
    {
        "https://open.coros.com/oauth2/refresh-token"
    } else {
        config.contract.token
    };
    let mut request = client.post(endpoint);
    if matches!(provider, Provider::Polar | Provider::Suunto) {
        request = request.basic_auth(&config.client_id, Some(&config.client_secret));
    } else {
        params.extend([
            ("client_id", config.client_id.as_str()),
            ("client_secret", config.client_secret.as_str()),
        ]);
    }
    if matches!(provider, Provider::Withings) {
        params.push(("action", "requesttoken"));
    }
    let mut result = cloud_wearables::body(request.form(&params).send().await?).await?;
    if matches!(provider, Provider::Withings) {
        ensure!(
            result["status"].as_u64() == Some(0),
            "Withings OAuth failed"
        );
        result = result["body"].take();
    }
    let expires = result["expires_in"]
        .as_i64()
        .or_else(|| result["expires_in"].as_str()?.parse().ok());
    let ownership = match provider {
        Provider::Strava | Provider::IntervalsIcu => {
            crate::wearable_webhooks::account_id(&result["athlete"]["id"])
        }
        Provider::Polar => crate::wearable_webhooks::account_id(&result["x_user_id"]),
        Provider::Withings => crate::wearable_webhooks::account_id(&result["userid"]),
        Provider::Suunto => result["access_token"].as_str().and_then(suunto_username),
        _ => None,
    };
    let mut tokens: Tokens =
        serde_json::from_value(result).map_err(|_| anyhow::anyhow!("invalid OAuth response"))?;
    ensure!(
        !tokens.access_token.is_empty() && tokens.access_token.len() <= 16384,
        "invalid OAuth access token"
    );
    tokens.expires_at = expires
        .filter(|seconds| (1..=31_536_000).contains(seconds))
        .map(|seconds| chrono::Utc::now() + chrono::Duration::seconds(seconds));
    if tokens.provider_user.is_none() {
        tokens.provider_user = ownership;
    }
    Ok(tokens)
}

fn suunto_username(jwt: &str) -> Option<String> {
    // Only called on the authenticated HTTPS response from the Suunto token endpoint.
    let payload = jwt
        .split('.')
        .nth(1)
        .filter(|encoded| encoded.len() <= 8192)?;
    let claims: Value = serde_json::from_slice(&URL_SAFE_NO_PAD.decode(payload).ok()?).ok()?;
    crate::wearable_webhooks::account_id(&claims["user"])
}

fn provider_credentials(provider: Provider, tokens: Tokens) -> Result<Vec<String>> {
    if matches!(provider, Provider::Coros) {
        return Ok(vec![
            tokens.access_token,
            tokens
                .provider_user
                .context("COROS account identifier missing")?,
        ]);
    }
    if matches!(provider, Provider::Suunto) {
        return Ok(vec![
            tokens.access_token,
            env::var("AVATAR_SUUNTO_SUBSCRIPTION_KEY")
                .context("Suunto subscription key required")?,
        ]);
    }
    if matches!(provider, Provider::IntervalsIcu) {
        return Ok(vec![
            tokens.access_token,
            tokens.scope,
            tokens
                .provider_user
                .context("Intervals.icu athlete ID is missing")?,
        ]);
    }
    Ok(vec![tokens.access_token, tokens.scope])
}

#[derive(Clone, Copy)]
struct WebhookProfile<'a> {
    url: &'a str,
    field: &'static str,
    required_scope: Option<&'static str>,
}

fn webhook_profile(provider: Provider) -> Option<WebhookProfile<'static>> {
    match provider {
        Provider::Oura => Some(WebhookProfile {
            url: "https://api.ouraring.com/v2/usercollection/personal_info",
            field: "id",
            required_scope: None,
        }),
        Provider::Whoop => Some(WebhookProfile {
            url: "https://api.prod.whoop.com/developer/v2/user/profile/basic",
            field: "user_id",
            required_scope: Some("read:profile"),
        }),
        _ => None,
    }
}

async fn webhook_profile_id(
    client: &Client,
    profile: WebhookProfile<'_>,
    token: &str,
    scope: &str,
) -> Result<Option<String>> {
    if profile.required_scope.is_some_and(|required| {
        !scope
            .split_ascii_whitespace()
            .any(|granted| granted == required)
    }) {
        return Ok(None);
    }
    let Ok(response) = client
        .get(profile.url)
        .bearer_auth(token)
        .timeout(std::time::Duration::from_secs(5))
        .send()
        .await
    else {
        return Ok(None);
    };
    match cloud_wearables::body(response).await {
        Ok(body) => Ok(crate::wearable_webhooks::account_id(&body[profile.field])),
        // Profile metadata is optional; retain polling if the profile is unavailable.
        Err(error) if error.is::<cloud_wearables::ProviderThrottled>() => Err(error),
        Err(_) => Ok(None),
    }
}

pub async fn access_for_sync(
    client: &Client,
    pool: &Pool,
    subject: &str,
    provider: Provider,
) -> Result<Vec<String>> {
    if let Some(profile) = webhook_profile(provider) {
        return access_with_profile(client, pool, subject, provider, profile).await;
    }
    access(client, pool, subject, provider).await
}

async fn access_with_profile(
    client: &Client,
    pool: &Pool,
    subject: &str,
    provider: Provider,
    profile: WebhookProfile<'_>,
) -> Result<Vec<String>> {
    let slug = provider.slug();
    let pending = pool
        .get()
        .await?
        .query_opt(
            "SELECT generation::text FROM avatar_hsm.wearable_oauth_connection
         WHERE subject_id=$1 AND provider=$2 AND provider_account_hash IS NULL",
            &[&subject, &slug],
        )
        .await?
        .map(|row| row.get::<_, String>(0));
    let credentials = access(client, pool, subject, provider).await?;
    if let Some(generation) = pending {
        if let Some(id) =
            webhook_profile_id(client, profile, &credentials[0], &credentials[1]).await?
        {
            // The profile may return after the connection was replaced or revoked.
            pool.get()
                .await?
                .execute(
                    "UPDATE avatar_hsm.wearable_oauth_connection SET provider_account_hash=$4
                 WHERE subject_id=$1 AND provider=$2 AND generation=$3::text::uuid
                   AND provider_account_hash IS NULL",
                    &[&subject, &slug, &generation, &hash(&id)],
                )
                .await?;
        }
    }
    Ok(credentials)
}

fn intervals_activity_grant(provider: Provider, scope: &str) -> Option<bool> {
    matches!(provider, Provider::IntervalsIcu)
        .then(|| cloud_wearables::manufacturers::intervals_scope(scope, "ACTIVITY"))
}

async fn reconcile_intervals_fit_consent(
    tx: &tokio_postgres::Transaction<'_>,
    subject: &str,
    provider: Provider,
    scope: &str,
) -> Result<()> {
    let Some(granted) = intervals_activity_grant(provider, scope) else {
        return Ok(());
    };
    let changed = tx.execute(
        "UPDATE avatar_hsm.wearable_oauth_connection SET intervals_activity_granted=$2
         WHERE subject_id=$1 AND provider='intervals_icu' AND intervals_activity_granted IS DISTINCT FROM $2",
        &[&subject, &granted],
    ).await?;
    if !granted && changed != 0 {
        // Removing the jobs cascades to stored FIT parts and their parsed details.
        tx.execute(
            "DELETE FROM watch_tower.wearable_fit_job WHERE subject_id=$1 AND provider='intervals_icu'",
            &[&subject],
        ).await?;
    }
    Ok(())
}

async fn reconcile_workout_file_consent(
    tx: &tokio_postgres::Transaction<'_>,
    subject: &str,
    provider: Provider,
    scope: &str,
) -> Result<()> {
    if !matches!(provider, Provider::GoogleHealth) {
        return reconcile_intervals_fit_consent(tx, subject, provider, scope).await;
    }
    let allowed = google_tcx_scope(scope);
    let changed = tx.execute(
        "UPDATE avatar_hsm.wearable_oauth_connection SET google_location_granted=$2
         WHERE subject_id=$1 AND provider='google_health' AND google_location_granted IS DISTINCT FROM $2",
        &[&subject, &allowed],
    ).await?;
    if !allowed && changed != 0 {
        tx.execute(
            "DELETE FROM watch_tower.google_workout_tcx_queue WHERE subject_id=$1",
            &[&subject],
        )
        .await?;
        tx.execute(
            "DELETE FROM avatar_hsm.google_workout_tcx WHERE subject_id=$1",
            &[&subject],
        )
        .await?;
    }
    Ok(())
}

pub async fn access(
    client: &Client,
    pool: &Pool,
    subject: &str,
    provider: Provider,
) -> Result<Vec<String>> {
    let mut db = pool.get().await?;
    let tx = db.transaction().await?;
    let row = tx
        .query_opt(
            "SELECT generation::text, sealed_tokens FROM avatar_hsm.wearable_oauth_connection
           WHERE subject_id=$1 AND provider=$2 FOR UPDATE",
            &[&subject, &provider.slug()],
        )
        .await?
        .context("OAuth connection is no longer active")?;
    let generation: String = row.get(0);
    let cipher = key()?;
    let previous = open(&cipher, row.get(1), subject, provider)?;
    let renewal_lead = if matches!(provider, Provider::Garmin) {
        10
    } else {
        2
    };
    if previous
        .expires_at
        .is_some_and(|expiry| expiry > chrono::Utc::now() + chrono::Duration::minutes(renewal_lead))
        || matches!(provider, Provider::Polar) && previous.refresh_token.is_none()
        || matches!(provider, Provider::IntervalsIcu)
            && previous.expires_at.is_none()
            && previous.refresh_token.is_none()
    {
        reconcile_workout_file_consent(&tx, subject, provider, &previous.scope).await?;
        tx.commit().await?;
        return provider_credentials(provider, previous);
    }
    let refresh = previous
        .refresh_token
        .context("OAuth renewal unavailable")?;
    let config = configuration(provider).context("OAuth provider configuration required")?;
    let mut tokens = exchange(
        client,
        provider,
        &config,
        vec![("grant_type", "refresh_token"), ("refresh_token", &refresh)],
    )
    .await?;
    if tokens.refresh_token.is_none() {
        tokens.refresh_token = Some(refresh);
    }
    if tokens.scope.is_empty() {
        tokens.scope = previous.scope;
    }
    if tokens.provider_user.is_none() {
        tokens.provider_user = previous.provider_user;
    }
    let encrypted = seal(&cipher, &tokens, subject, provider)?;
    let changed = tx
        .execute(
            "UPDATE avatar_hsm.wearable_oauth_connection SET sealed_tokens=$4
        WHERE subject_id=$1 AND provider=$2 AND generation=$3::text::uuid",
            &[&subject, &provider.slug(), &generation, &encrypted],
        )
        .await?;
    ensure!(changed == 1, "OAuth connection changed during refresh");
    reconcile_workout_file_consent(&tx, subject, provider, &tokens.scope).await?;
    tx.commit().await?;
    provider_credentials(provider, tokens)
}
