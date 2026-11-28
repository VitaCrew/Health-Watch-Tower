use crate::{cloud_wearables, mcp::App, wearable_webhooks};
use anyhow::{ensure, Context, Result};
use axum::{
    body::Bytes,
    extract::State,
    http::{HeaderMap, StatusCode},
};
use base64::{engine::general_purpose::STANDARD, Engine};
use chrono::{DateTime, NaiveDate, NaiveDateTime, Utc};
use ring::signature::{UnparsedPublicKey, ECDSA_P256_SHA256_ASN1};
use serde_json::Value;
use std::{
    collections::{BTreeSet, HashMap},
    sync::{Arc, OnceLock},
    time::{Duration, Instant},
};
use subtle::ConstantTimeEq;
use tokio::sync::Mutex;

#[cfg(test)]
#[path = "google_health_webhooks_tests.rs"]
mod tests;

const KEYSET_URL: &str =
    "https://www.gstatic.com/googlehealthapi/webhooks/webhooks_public_keyset.json";
#[derive(Default)]
struct CachedKeys {
    fetched: Option<Instant>,
    keys: HashMap<u32, [u8; 65]>,
}
static KEYS: OnceLock<Mutex<CachedKeys>> = OnceLock::new();

pub async fn receive(State(app): State<Arc<App>>, headers: HeaderMap, body: Bytes) -> StatusCode {
    let expected = std::env::var("AVATAR_GOOGLE_HEALTH_WEBHOOK_SECRET").unwrap_or_default();
    receive_with_secret(&app, &headers, &body, &expected).await
}

async fn receive_with_secret(
    app: &App,
    headers: &HeaderMap,
    body: &[u8],
    expected: &str,
) -> StatusCode {
    if body.len() > 262_144 {
        return StatusCode::PAYLOAD_TOO_LARGE;
    }
    let supplied = headers
        .get("authorization")
        .and_then(|h| h.to_str().ok())
        .and_then(|h| h.strip_prefix("Bearer "))
        .unwrap_or_default();
    if expected.len() < 32 || !bool::from(expected.as_bytes().ct_eq(supplied.as_bytes())) {
        return StatusCode::UNAUTHORIZED;
    }
    if body
        .iter()
        .copied()
        .find(|byte| !byte.is_ascii_whitespace())
        == Some(b'{')
        && serde_json::from_slice::<Value>(body).is_ok_and(|object| {
            object.as_object().is_some_and(|fields| fields.len() == 1)
                && object["type"] == "verification"
        })
    {
        return StatusCode::OK;
    }
    let Some(signature) = headers
        .get("google-health-api-signature")
        .and_then(|h| h.to_str().ok())
        .filter(|h| h.len() <= 256)
        .and_then(|h| STANDARD.decode(h).ok())
    else {
        return StatusCode::UNAUTHORIZED;
    };
    let Ok((key_id, der)) = signature_parts(&signature) else {
        return StatusCode::UNAUTHORIZED;
    };
    let key = match active_key(key_id).await {
        Ok(Some(key)) => key,
        Ok(None) => return StatusCode::UNAUTHORIZED,
        Err(_) => return StatusCode::SERVICE_UNAVAILABLE,
    };
    if !verify(&key, body, der) {
        return StatusCode::UNAUTHORIZED;
    }
    let Ok(events) = serde_json::from_slice::<Vec<Value>>(body) else {
        return StatusCode::BAD_REQUEST;
    };
    if !(1..=99).contains(&events.len()) {
        return StatusCode::BAD_REQUEST;
    }
    let mut validated = Vec::with_capacity(events.len());
    for event in events {
        let Some(owner) = wearable_webhooks::account_id(&event["data"]["healthUserId"]) else {
            return StatusCode::BAD_REQUEST;
        };
        let Ok(encoded) = serde_json::to_vec(&event) else {
            return StatusCode::BAD_REQUEST;
        };
        let data = &event["data"];
        let days = notification_days(data, Utc::now().date_naive());
        let record = days.is_empty().then(|| notification_record(data)).flatten();
        validated.push((owner, encoded, days, record));
    }
    for (owner, event, days, record) in validated {
        let queued = if let Some((kind, source_id)) = record {
            wearable_webhooks::nudge_google_record(&app.db, &owner, &event, kind, &source_id).await
        } else {
            wearable_webhooks::nudge_days(&app.db, "google_health", &owner, &event, &days).await
        };
        if queued.is_err() {
            return StatusCode::SERVICE_UNAVAILABLE;
        }
    }
    StatusCode::OK
}

fn notification_record(data: &Value) -> Option<(&'static str, String)> {
    if !matches!(data["operation"].as_str(), Some("UPSERT" | "DELETE")) {
        return None;
    }
    let id = data["recordId"].as_str().filter(|id| {
        (1..=192).contains(&id.len())
            && id
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.'))
    })?;
    let data_type = data["dataType"].as_str().filter(|name| {
        (1..=64).contains(&name.len())
            && name
                .bytes()
                .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
    })?;
    Some(match data_type {
        "sleep" => ("sleep", id.to_owned()),
        "exercise" => ("workout", id.to_owned()),
        "steps" => ("steps", id.to_owned()),
        "heart-rate" => ("heart_rate", id.to_owned()),
        metric => ("metric", format!("{metric}:{id}")),
    })
}

fn notification_days(data: &Value, today: NaiveDate) -> Vec<NaiveDate> {
    if !matches!(data["operation"].as_str(), Some("UPSERT" | "DELETE")) {
        return Vec::new();
    }
    let earliest = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
    let mut days = BTreeSet::new();
    for interval in data["intervals"].as_array().into_iter().flatten() {
        let utc = |key: &str| {
            interval["physicalTimeInterval"][key]
                .as_str()?
                .parse::<DateTime<Utc>>()
                .ok()
        };
        let civil = |key: &str| {
            interval["civilIso8601TimeInterval"][key]
                .as_str()?
                .parse::<NaiveDateTime>()
                .ok()
                .map(|time| time.and_utc())
        };
        let structured =
            |key: &str| civil_notification_time(&interval["civilDateTimeInterval"][key]);
        let local = civil("startTime")
            .zip(civil("endTime"))
            .or_else(|| structured("startDateTime").zip(structured("endDateTime")));
        for (start, end) in [utc("startTime").zip(utc("endTime")), local]
            .into_iter()
            .flatten()
        {
            if end < start {
                continue;
            }
            let first = start.date_naive();
            let last = if end > start {
                (end - chrono::Duration::nanoseconds(1)).date_naive()
            } else {
                first
            };
            let last = last.min(today);
            if first < earliest || first > last {
                continue;
            }
            if (last - first).num_days() >= 31 {
                return Vec::new();
            }
            days.extend(first.iter_days().take_while(|day| *day <= last));
            if days.len() > 31 {
                return Vec::new();
            }
        }
    }
    days.into_iter().collect()
}

fn civil_notification_time(value: &Value) -> Option<DateTime<Utc>> {
    let date = &value["date"];
    let day = NaiveDate::from_ymd_opt(
        date["year"].as_i64()?.try_into().ok()?,
        date["month"].as_u64()?.try_into().ok()?,
        date["day"].as_u64()?.try_into().ok()?,
    )?;
    let time = &value["time"];
    let component = |key: &str| -> Option<u32> {
        time.get(key)
            .map_or(Some(0), |value| value.as_u64()?.try_into().ok())
    };
    Some(
        day.and_hms_nano_opt(
            component("hours")?,
            component("minutes")?,
            component("seconds")?,
            component("nanos")?,
        )?
        .and_utc(),
    )
}

async fn active_key(key_id: u32) -> Result<Option<[u8; 65]>> {
    let mut cache = KEYS
        .get_or_init(|| Mutex::new(CachedKeys::default()))
        .lock()
        .await;
    let ttl = if cache.keys.contains_key(&key_id) {
        Duration::from_secs(3600)
    } else {
        Duration::from_secs(60)
    };
    if cache.fetched.is_none_or(|last| last.elapsed() >= ttl) {
        let client = cloud_wearables::http_client()?;
        let remote = cloud_wearables::body(client.get(KEYSET_URL).send().await?).await?;
        cache.keys = decode_keys(&remote)?;
        cache.fetched = Some(Instant::now());
    }
    Ok(cache.keys.get(&key_id).copied())
}

fn decode_keys(keyset: &Value) -> Result<HashMap<u32, [u8; 65]>> {
    let input = keyset["key"]
        .as_array()
        .context("invalid Google webhook keyset")?;
    ensure!(
        (1..=16).contains(&input.len()),
        "invalid Google webhook key count"
    );
    let mut keys = HashMap::new();
    for entry in input {
        if entry["status"] != "ENABLED" || entry["outputPrefixType"] != "TINK" {
            continue;
        }
        let data = &entry["keyData"];
        if data["keyMaterialType"] != "ASYMMETRIC_PUBLIC"
            || data["typeUrl"] != "type.googleapis.com/google.crypto.tink.EcdsaPublicKey"
        {
            continue;
        }
        let id: u32 = entry["keyId"]
            .as_u64()
            .context("missing Google signing key id")?
            .try_into()?;
        let encoded = data["value"]
            .as_str()
            .context("missing Google signing key")?;
        ensure!(
            encoded.len() <= 160,
            "Google signing key exceeds size limit"
        );
        let key = coordinates(&STANDARD.decode(encoded)?)?;
        ensure!(
            keys.insert(id, key).is_none(),
            "duplicate Google signing key id"
        );
    }
    ensure!(
        !keys.is_empty(),
        "Google webhook keyset did not include a supported key"
    );
    Ok(keys)
}

fn coordinates(bytes: &[u8]) -> Result<[u8; 65]> {
    // Tink EcdsaPublicKey: version=0 (omitted by protobuf), params, X, Y.
    // The accepted parameter tuple is SHA256, NIST_P256, DER.
    let payload = bytes
        .strip_prefix(&[0x12, 0x06, 0x08, 0x03, 0x10, 0x02, 0x18, 0x02])
        .context("unsupported Google key format")?;
    let (x, tail) = coordinate(payload, 0x1a)?;
    let (y, tail) = coordinate(tail, 0x22)?;
    ensure!(tail.is_empty(), "unexpected Google public key fields");
    let mut point = [0u8; 65];
    point[0] = 4;
    point[1..33].copy_from_slice(&x);
    point[33..65].copy_from_slice(&y);
    Ok(point)
}

fn coordinate(input: &[u8], tag: u8) -> Result<([u8; 32], &[u8])> {
    ensure!(
        input.len() >= 2 && input[0] == tag,
        "missing Google key coordinate"
    );
    let length = usize::from(input[1]);
    ensure!(
        (1..=33).contains(&length) && input.len() >= length + 2,
        "invalid Google key coordinate"
    );
    let mut data = &input[2..length + 2];
    if data.len() == 33 {
        data = data
            .strip_prefix(&[0])
            .context("invalid leading coordinate byte")?;
    }
    let mut output = [0u8; 32];
    output[32 - data.len()..].copy_from_slice(data);
    Ok((output, &input[length + 2..]))
}

fn signature_parts(signature: &[u8]) -> Result<(u32, &[u8])> {
    ensure!(
        signature.len() >= 6 && signature[0] == 1,
        "invalid Google Tink prefix"
    );
    Ok((
        u32::from_be_bytes(signature[1..5].try_into()?),
        &signature[5..],
    ))
}

fn verify(key: &[u8; 65], raw: &[u8], der: &[u8]) -> bool {
    UnparsedPublicKey::new(&ECDSA_P256_SHA256_ASN1, key)
        .verify(raw, der)
        .is_ok()
}
