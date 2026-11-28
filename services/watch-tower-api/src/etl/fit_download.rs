use anyhow::{ensure, Context, Result};
use avatar_hsm::etl_adapter::{
    self, CorosFitDownloadPlan, FitDownloadAuth, FitDownloadPlan, PolarFitDownloadPlan,
    SuuntoFitDownloadPlan,
};
use chrono::{DateTime, Duration, Utc};
use deadpool_postgres::Pool;
use reqwest::{Client, Response, StatusCode};
use serde_json::Value;
use std::time::Duration as StdDuration;

const MAX_FIT_BYTES: usize = 12 * 1024 * 1024;

pub async fn worker(pool: Pool, client: Client) {
    loop {
        match etl_adapter::next_fit_download(&pool).await {
            Ok(Some(job)) => {
                let result = tokio::time::timeout(
                    StdDuration::from_secs(100),
                    run_one(&pool, &client, &job),
                )
                .await;
                if !matches!(result, Ok(Ok(()))) {
                    let retry_at = match &result {
                        Ok(Err(error)) => provider_retry_at(error),
                        _ => None,
                    };
                    let _ = etl_adapter::defer_fit_download(&pool, &job, retry_at).await;
                }
                tokio::time::sleep(StdDuration::from_secs(1)).await;
            }
            Ok(None) | Err(_) => tokio::time::sleep(StdDuration::from_secs(12)).await,
        }
    }
}

async fn run_one(pool: &Pool, client: &Client, job: &etl_adapter::FitDownloadJob) -> Result<()> {
    match etl_adapter::prepare_fit_download(pool, client, job).await? {
        FitDownloadPlan::Coros(plan) => {
            let files = download_coros(client, plan).await?;
            etl_adapter::save_fit_download_parts(pool, job, files).await
        }
        FitDownloadPlan::Polar(plan) => download_polar(pool, client, job, plan).await,
        FitDownloadPlan::Suunto(plan) => download_suunto(pool, client, job, plan).await,
        FitDownloadPlan::Http(request) => {
            let mut builder = client.get(&request.url);
            builder = match request.auth {
                FitDownloadAuth::Bearer(token) => builder.bearer_auth(token),
                FitDownloadAuth::Basic { username, password } => {
                    builder.basic_auth(username, Some(password))
                }
            };
            let bytes =
                read_fit_response(builder.send().await.map_err(reqwest::Error::without_url)?)
                    .await?;
            super::fit_file::validate(&bytes).context("provider returned an invalid FIT file")?;
            etl_adapter::save_fit_download(pool, job, bytes).await
        }
    }
}

async fn download_polar(
    pool: &Pool,
    client: &Client,
    job: &etl_adapter::FitDownloadJob,
    plan: PolarFitDownloadPlan,
) -> Result<()> {
    let (summary, bytes) = fetch_polar(client, plan, "https://www.polaraccesslink.com").await?;
    if let Some(payload) = summary {
        let normalized = super::polar::normalize_workout_detail(&payload, job.activity_id())?;
        etl_adapter::commit_fit_summary(pool, job, normalized).await?;
    }
    etl_adapter::save_fit_download_parts(pool, job, vec![(0, bytes)]).await
}

async fn fetch_polar(
    client: &Client,
    plan: PolarFitDownloadPlan,
    origin: &str,
) -> Result<(Option<Value>, Vec<u8>)> {
    let summary = if plan.fetch_summary {
        let response = client
            .get(format!("{origin}/v3/exercises/{}", plan.activity_id))
            .bearer_auth(&plan.access_token)
            .query(&[("zones", "true")])
            .send()
            .await
            .map_err(reqwest::Error::without_url)?;
        Some(read_provider_json(response).await?)
    } else {
        None
    };
    let response = client
        .get(format!("{origin}/v3/exercises/{}/fit", plan.activity_id))
        .bearer_auth(&plan.access_token)
        .send()
        .await
        .map_err(reqwest::Error::without_url)?;
    let bytes = super::fit_file::normalize(read_fit_response(response).await?)?;
    Ok((summary, bytes))
}

async fn download_suunto(
    pool: &Pool,
    client: &Client,
    job: &etl_adapter::FitDownloadJob,
    plan: SuuntoFitDownloadPlan,
) -> Result<()> {
    let (summary, bytes) = fetch_suunto(client, plan, "https://cloudapi.suunto.com").await?;
    if let Some(payload) = summary {
        let normalized = super::suunto::normalize_workout_detail(&payload, job.activity_id())?;
        etl_adapter::commit_fit_summary(pool, job, normalized).await?;
    }
    etl_adapter::save_fit_download_parts(pool, job, vec![(0, bytes)]).await
}

async fn fetch_suunto(
    client: &Client,
    plan: SuuntoFitDownloadPlan,
    origin: &str,
) -> Result<(Option<Value>, Vec<u8>)> {
    let summary = if plan.fetch_summary {
        let response = client
            .get(format!("{origin}/v3/workouts/{}", plan.workout_key))
            .query(&[("extensions", "PauseMarkerExtension,SummaryExtension")])
            .bearer_auth(&plan.access_token)
            .header("Ocp-Apim-Subscription-Key", &plan.subscription_key)
            .send()
            .await
            .map_err(reqwest::Error::without_url)?;
        Some(read_provider_json(response).await?)
    } else {
        None
    };
    let response = client
        .get(format!("{origin}/v3/workouts/{}/fit", plan.workout_key))
        .bearer_auth(&plan.access_token)
        .header("Ocp-Apim-Subscription-Key", &plan.subscription_key)
        .send()
        .await
        .map_err(reqwest::Error::without_url)?;
    let bytes = super::fit_file::normalize(read_fit_response(response).await?)?;
    Ok((summary, bytes))
}

async fn read_provider_json(response: Response) -> Result<Value> {
    serde_json::from_slice(&read_fit_response(response).await?).context("invalid provider JSON")
}

async fn download_coros(
    client: &Client,
    plan: CorosFitDownloadPlan,
) -> Result<Vec<(i16, Vec<u8>)>> {
    let allowed = std::env::var("AVATAR_COROS_FIT_HOSTS").unwrap_or_default();
    if !plan.cached_sources.is_empty() {
        let mut files = Vec::new();
        let mut stale = false;
        for (part, raw_url) in &plan.cached_sources {
            let url = validated_coros_url(raw_url, &allowed)?;
            match read_fit_response(
                client
                    .get(url)
                    .send()
                    .await
                    .map_err(reqwest::Error::without_url)?,
            )
            .await
            {
                Ok(bytes) => files.push((*part, super::fit_file::normalize(bytes)?)),
                Err(error) if error.downcast_ref::<ProviderRateLimited>().is_some() => {
                    return Err(error)
                }
                Err(_) => {
                    stale = true;
                    break;
                }
            }
        }
        if !stale && !files.is_empty() {
            return Ok(files);
        }
    }

    let response = client
        .get("https://open.coros.com/v2/coros/sport/detail/fit")
        .query(&[
            ("mode", plan.mode.as_str()),
            ("subMode", plan.submode.as_str()),
            ("labelId", plan.activity_id.as_str()),
            ("token", plan.access_token.as_str()),
            ("openId", plan.open_id.as_str()),
        ])
        .send()
        .await
        .map_err(reqwest::Error::without_url)?;
    let body = serde_json::from_slice::<Value>(&read_fit_response(response).await?)?;
    let data = &body["data"];
    ensure!(body["result"] == "0000", "COROS FIT detail unavailable");
    let provider_id = data["labelId"]
        .as_str()
        .map(str::to_owned)
        .or_else(|| data["labelId"].as_u64().map(|id| id.to_string()));
    ensure!(
        provider_id.as_deref() == Some(plan.activity_id.as_str()),
        "COROS FIT detail belongs to a different activity"
    );
    let links = coros_links(data, &allowed)?;
    let mut files = Vec::with_capacity(links.len());
    let mut total_bytes = 0usize;
    for (part, url) in links {
        let bytes = read_fit_response(
            client
                .get(url)
                .send()
                .await
                .map_err(reqwest::Error::without_url)?,
        )
        .await?;
        let normalized = super::fit_file::normalize(bytes)?;
        total_bytes = total_bytes.saturating_add(normalized.len());
        ensure!(
            total_bytes <= 48 * 1024 * 1024,
            "COROS FIT activity exceeds total size limit"
        );
        files.push((part, normalized));
    }
    Ok(files)
}

fn coros_links(data: &Value, allowed: &str) -> Result<Vec<(i16, reqwest::Url)>> {
    let parse = |point: &Value| -> Result<reqwest::Url> {
        let raw = point["fitUrl"]
            .as_str()
            .filter(|value| !value.is_empty() && value.len() <= 500)
            .context("COROS FIT link unavailable")?;
        validated_coros_url(raw, allowed)
    };
    let mut links = Vec::new();
    if data["fitUrl"].as_str().is_some_and(|url| !url.is_empty()) {
        links.push((0, parse(data)?));
    }
    if let Some(parts) = data
        .get("triathlonItemList")
        .filter(|value| !value.is_null())
    {
        let parts = parts.as_array().context("invalid COROS FIT parts")?;
        ensure!(parts.len() <= 16, "too many COROS FIT parts");
        for (index, part) in parts.iter().enumerate() {
            links.push(((index + 1) as i16, parse(part)?));
        }
    }
    ensure!(!links.is_empty(), "COROS FIT link unavailable");
    Ok(links)
}

fn validated_coros_url(raw: &str, allowed: &str) -> Result<reqwest::Url> {
    let url = reqwest::Url::parse(raw).map_err(|_| anyhow::anyhow!("invalid COROS FIT link"))?;
    ensure!(
        url.scheme() == "https"
            && url.port_or_known_default() == Some(443)
            && url.username().is_empty()
            && url.password().is_none()
            && url.fragment().is_none()
            && url.host_str().is_some_and(|host| host.contains('.')
                && host.parse::<std::net::IpAddr>().is_err()
                && ![".localhost", ".local", ".internal"]
                    .iter()
                    .any(|suffix| host.ends_with(suffix))
                && allowed
                    .split(',')
                    .any(|candidate| candidate.trim().eq_ignore_ascii_case(host))),
        "COROS FIT host has not been approved"
    );
    Ok(url)
}

async fn read_fit_response(response: Response) -> Result<Vec<u8>> {
    if response.status() == StatusCode::TOO_MANY_REQUESTS {
        let retry_at = retry_after(
            response
                .headers()
                .get(reqwest::header::RETRY_AFTER)
                .and_then(|value| value.to_str().ok()),
            Utc::now(),
        );
        return Err(ProviderRateLimited { retry_at }.into());
    }
    ensure!(
        response.status().is_success(),
        "FIT provider returned HTTP {}",
        response.status().as_u16()
    );
    let mut response = response;
    let mut bytes = Vec::new();
    while let Some(chunk) = response
        .chunk()
        .await
        .map_err(reqwest::Error::without_url)?
    {
        ensure!(
            bytes.len() + chunk.len() <= MAX_FIT_BYTES,
            "FIT download exceeds size limit"
        );
        bytes.extend_from_slice(&chunk);
    }
    Ok(bytes)
}

#[derive(Debug)]
struct ProviderRateLimited {
    retry_at: DateTime<Utc>,
}

impl std::fmt::Display for ProviderRateLimited {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("FIT provider rate limited the download")
    }
}

impl std::error::Error for ProviderRateLimited {}

fn provider_retry_at(error: &anyhow::Error) -> Option<DateTime<Utc>> {
    error
        .downcast_ref::<ProviderRateLimited>()
        .map(|limited| limited.retry_at)
        .or_else(|| etl_adapter::provider_retry_at(error))
}

fn retry_after(header: Option<&str>, now: DateTime<Utc>) -> DateTime<Utc> {
    header
        .and_then(|value| {
            let value = value.trim();
            value
                .parse::<i64>()
                .ok()
                .and_then(Duration::try_seconds)
                .and_then(|delay| now.checked_add_signed(delay))
                .or_else(|| {
                    DateTime::parse_from_rfc2822(value)
                        .ok()
                        .map(|at| at.with_timezone(&Utc))
                })
        })
        .filter(|retry_at| *retry_at > now)
        .unwrap_or_else(|| now + Duration::minutes(5))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn valid_fit_fixture() -> Vec<u8> {
        let mut bytes = vec![12, 0x10, 0, 0, 1, 0, 0, 0, b'.', b'F', b'I', b'T', 0x01];
        let mut checksum = 0u16;
        for byte in &bytes {
            checksum ^= u16::from(*byte);
            for _ in 0..8 {
                checksum = if checksum & 1 == 1 {
                    (checksum >> 1) ^ 0xa001
                } else {
                    checksum >> 1
                };
            }
        }
        bytes.extend_from_slice(&checksum.to_le_bytes());
        bytes
    }

    #[test]
    fn retry_after_supports_seconds_and_http_dates() {
        let now = DateTime::parse_from_rfc3339("2026-11-28T00:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        assert_eq!(retry_after(Some("45"), now), now + Duration::seconds(45));
        assert_eq!(
            retry_after(Some("Sat, 28 Nov 2026 00:02:00 GMT"), now),
            now + Duration::minutes(2)
        );
    }

    #[test]
    fn invalid_retry_after_uses_bounded_default_delay() {
        let now = Utc::now();
        assert_eq!(
            retry_after(Some("invalid"), now),
            now + Duration::minutes(5)
        );
    }

    #[test]
    fn coros_parts_require_an_approved_https_host_and_keep_part_numbers() -> Result<()> {
        let data = serde_json::json!({
            "fitUrl":"https://cdn.example.com/main.fit?sig=one",
            "triathlonItemList":[
                {"fitUrl":"https://cdn.example.com/swim.fit?sig=two"},
                {"fitUrl":"https://cdn.example.com/run.fit?sig=three"}
            ]
        });
        let links = coros_links(&data, "cdn.example.com")?;
        assert_eq!(
            links.iter().map(|(part, _)| *part).collect::<Vec<_>>(),
            [0, 1, 2]
        );
        assert!(coros_links(
            &serde_json::json!({"fitUrl":"https://attacker.example/fit"}),
            "cdn.example.com"
        )
        .is_err());
        assert!(coros_links(
            &serde_json::json!({"fitUrl":"http://cdn.example.com/fit"}),
            "cdn.example.com"
        )
        .is_err());
        Ok(())
    }

    #[tokio::test]
    async fn polar_and_suunto_provider_requests_run_from_watch_tower() -> Result<()> {
        use axum::{
            body::Body,
            extract::{Query, State},
            http::{HeaderMap, Response as HttpResponse},
            routing::get,
            Json, Router,
        };
        use std::{collections::HashMap, sync::Arc};
        use tokio::sync::Mutex;

        let requests = Arc::new(Mutex::new(Vec::<String>::new()));
        let polar_requests = Arc::clone(&requests);
        let payload = Arc::new(valid_fit_fixture());
        let polar_payload = Arc::clone(&payload);
        let polar = Router::new()
            .route(
                "/v3/exercises/123",
                get(
                    move |State(seen): State<Arc<Mutex<Vec<String>>>>,
                          headers: HeaderMap,
                          Query(query): Query<HashMap<String, String>>| {
                        let seen = Arc::clone(&seen);
                        async move {
                            assert_eq!(headers["authorization"], "Bearer polar-token");
                            assert_eq!(query["zones"], "true");
                            seen.lock().await.push("polar-summary".into());
                            Json(
                                serde_json::json!({"id":"123","start_time":"2026-11-28T07:00:00Z"}),
                            )
                        }
                    },
                ),
            )
            .route(
                "/v3/exercises/123/fit",
                get(
                    move |State(seen): State<Arc<Mutex<Vec<String>>>>, headers: HeaderMap| {
                        let seen = Arc::clone(&seen);
                        let payload = Arc::clone(&polar_payload);
                        async move {
                            assert_eq!(headers["authorization"], "Bearer polar-token");
                            seen.lock().await.push("polar-fit".into());
                            HttpResponse::new(Body::from(payload.as_ref().clone()))
                        }
                    },
                ),
            )
            .with_state(polar_requests);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let origin = format!("http://{}", listener.local_addr()?);
        let server = tokio::spawn(async move { axum::serve(listener, polar).await });
        let (summary, bytes) = fetch_polar(
            &Client::new(),
            PolarFitDownloadPlan {
                activity_id: "123".into(),
                access_token: "polar-token".into(),
                fetch_summary: true,
            },
            &origin,
        )
        .await?;
        assert_eq!(summary.expect("Polar summary")["id"], "123");
        assert_eq!(bytes, *payload);
        assert_eq!(*requests.lock().await, ["polar-summary", "polar-fit"]);
        server.abort();

        let seen = Arc::new(Mutex::new(Vec::<String>::new()));
        let suunto_seen = Arc::clone(&seen);
        let suunto_payload = Arc::clone(&payload);
        let suunto = Router::new()
            .route(
                "/v3/workouts/0123456789abcdef01234567",
                get(move |State(seen): State<Arc<Mutex<Vec<String>>>>, headers: HeaderMap, Query(query): Query<HashMap<String, String>>| {
                    let seen = Arc::clone(&seen);
                    async move {
                        assert_eq!(headers["authorization"], "Bearer suunto-token");
                        assert_eq!(headers["ocp-apim-subscription-key"], "suunto-subscription");
                        assert_eq!(query["extensions"], "PauseMarkerExtension,SummaryExtension");
                        seen.lock().await.push("suunto-summary".into());
                        Json(serde_json::json!({"workoutKey":"0123456789abcdef01234567","startTime":"2026-11-28T07:00:00Z"}))
                    }
                }),
            )
            .route(
                "/v3/workouts/0123456789abcdef01234567/fit",
                get(move |State(seen): State<Arc<Mutex<Vec<String>>>>, headers: HeaderMap| {
                    let seen = Arc::clone(&seen);
                    let payload = Arc::clone(&suunto_payload);
                    async move {
                        assert_eq!(headers["authorization"], "Bearer suunto-token");
                        assert_eq!(headers["ocp-apim-subscription-key"], "suunto-subscription");
                        seen.lock().await.push("suunto-fit".into());
                        HttpResponse::new(Body::from(payload.as_ref().clone()))
                    }
                }),
            )
            .with_state(suunto_seen);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let origin = format!("http://{}", listener.local_addr()?);
        let server = tokio::spawn(async move { axum::serve(listener, suunto).await });
        let (summary, bytes) = fetch_suunto(
            &Client::new(),
            SuuntoFitDownloadPlan {
                workout_key: "0123456789abcdef01234567".into(),
                access_token: "suunto-token".into(),
                subscription_key: "suunto-subscription".into(),
                fetch_summary: true,
            },
            &origin,
        )
        .await?;
        assert_eq!(
            summary.expect("Suunto summary")["workoutKey"],
            "0123456789abcdef01234567"
        );
        assert_eq!(bytes, *payload);
        assert_eq!(*seen.lock().await, ["suunto-summary", "suunto-fit"]);
        server.abort();
        Ok(())
    }
}
