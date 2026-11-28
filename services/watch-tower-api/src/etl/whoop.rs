use anyhow::{ensure, Context, Result};
use avatar_hsm::etl_adapter::Snapshot;
use chrono::{DateTime, Duration, Utc};
use reqwest::{Client, RequestBuilder, Response};
use serde_json::{json, Value};

const API: &str = "https://api.prod.whoop.com/developer/v2";
const MAX_RESPONSE_BYTES: usize = 8 * 1024 * 1024;
const MAX_RECORDS: usize = 4_000;

#[derive(Debug)]
pub struct ProviderRateLimited {
    pub retry_at: DateTime<Utc>,
}

impl std::fmt::Display for ProviderRateLimited {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("WHOOP returned HTTP 429")
    }
}

impl std::error::Error for ProviderRateLimited {}

pub fn provider_retry_at(error: &anyhow::Error) -> Option<DateTime<Utc>> {
    error
        .downcast_ref::<ProviderRateLimited>()
        .map(|limited| limited.retry_at)
}

pub async fn collect_window(
    client: &Client,
    token: &str,
    scope: Option<&str>,
    lease: &str,
    start: DateTime<Utc>,
    end: DateTime<Utc>,
) -> Result<Snapshot> {
    collect_from_origin(client, token, scope, lease, start, end, API, Utc::now()).await
}

async fn collect_from_origin(
    client: &Client,
    token: &str,
    scope: Option<&str>,
    lease: &str,
    start: DateTime<Utc>,
    end: DateTime<Utc>,
    origin: &str,
    observed_at: DateTime<Utc>,
) -> Result<Snapshot> {
    ensure!(
        !token.trim().is_empty() && !lease.is_empty(),
        "WHOOP credential and HSM lease are required"
    );
    ensure!(
        start < end && end - start <= Duration::days(7),
        "invalid WHOOP collection window"
    );
    let window = Window { start, end };
    let scope = scope.filter(|scope| !scope.trim().is_empty());
    let mut rows = Vec::new();
    let mut grants = Vec::new();
    for kind in ["sleep", "workout"] {
        if !has_scope(scope, &format!("read:{kind}")) {
            continue;
        }
        grants.push(kind);
        let request = client
            .get(format!("{origin}/activity/{kind}"))
            .bearer_auth(token)
            .query(&[
                ("start", start.to_rfc3339()),
                ("end", end.to_rfc3339()),
                ("limit", "25".to_owned()),
            ]);
        rows.extend(
            cursor_records(request, "records", "next_token", "nextToken", |item| {
                whoop_point(item, kind, &window)
            })
            .await?,
        );
    }
    for (name, required) in [("cycle", "read:cycles"), ("recovery", "read:recovery")] {
        if scope.is_none() || !has_scope(scope, required) {
            continue;
        }
        let request = client
            .get(format!("{origin}/{name}"))
            .bearer_auth(token)
            .query(&[
                ("start", start.to_rfc3339()),
                ("end", end.to_rfc3339()),
                ("limit", "25".to_owned()),
            ]);
        rows.extend(
            cursor_records(request, "records", "next_token", "nextToken", |item| {
                let at = parse_date(item.get("start").unwrap_or(&item["created_at"]))?;
                let until = item
                    .get("end")
                    .filter(|value| !value.is_null())
                    .map(parse_date)
                    .transpose()?
                    .unwrap_or(at);
                metric(item, name, at, until, &window)
            })
            .await?,
        );
        if !grants.contains(&"metric") {
            grants.push("metric");
        }
        if name == "cycle" {
            grants.push("steps");
        }
    }
    let cycle_metrics = rows
        .iter()
        .filter_map(|row| whoop_cycle_metrics(row, &window))
        .flatten()
        .collect::<Vec<_>>();
    rows.extend(cycle_metrics);
    if scope.is_some()
        && has_scope(scope, "read:body_measurement")
        && end >= observed_at - Duration::minutes(2)
    {
        let data = body(
            client
                .get(format!("{origin}/user/measurement/body"))
                .bearer_auth(token)
                .send()
                .await?,
        )
        .await?;
        ensure!(data.is_object(), "invalid WHOOP body measurement");
        let at = start.max(end - Duration::milliseconds(1));
        rows.push(record(
            "metric",
            "WHOOP",
            "body_measurement:current",
            at,
            at,
            end,
            json!({"metric":"body_measurement","time_precision":"observed_at","provider_data":data}),
        ));
        if !grants.contains(&"metric") {
            grants.push("metric");
        }
    }
    for row in &mut rows {
        row["modified_at"] = json!(observed_at);
    }
    ensure!(
        rows.len() <= MAX_RECORDS,
        "WHOOP snapshot record limit exceeded"
    );
    serde_json::from_value(json!({
        "provider":"whoop","lease":lease,"start":start,"end":end,
        "grants":grants,"records":rows
    }))
    .context("failed to build normalized WHOOP snapshot")
}

struct Window {
    start: DateTime<Utc>,
    end: DateTime<Utc>,
}

impl Window {
    fn contains(&self, start: DateTime<Utc>, end: DateTime<Utc>) -> bool {
        start >= self.start && start < self.end && end >= start && end <= self.end
    }
}

fn has_scope(scope: Option<&str>, expected: &str) -> bool {
    scope.is_none_or(|scope| {
        scope
            .split(|character: char| character.is_whitespace() || character == ',')
            .any(|granted| granted == expected)
    })
}

fn whoop_point(item: &Value, kind: &str, window: &Window) -> Result<Option<Value>> {
    let Some(end) = item.get("end").filter(|value| !value.is_null()) else {
        return Ok(None);
    };
    let start = parse_date(&item["start"])?;
    let end = parse_date(end)?;
    if start >= end || !window.contains(start, end) {
        return Ok(None);
    }
    let payload = if kind == "sleep" {
        json!({"stages":[],"provider_stage_summary":item["score"]["stage_summary"],
            "nap":item["nap"],"provider_data":item})
    } else {
        json!({"exercise_type":required_text(&item["sport_name"])?,
            "distance_meters":item["score"]["distance_meter"],
            "provider_heart_rate":{"average_bpm":item["score"]["average_heart_rate"],
                "max_bpm":item["score"]["max_heart_rate"]},
            "duration_seconds":(end-start).num_seconds(),"provider_data":item})
    };
    Ok(Some(record(
        kind,
        "WHOOP",
        required_text(&item["id"])?,
        start,
        end,
        parse_date(&item["updated_at"])?,
        payload,
    )))
}

fn metric(
    item: &Value,
    name: &str,
    start: DateTime<Utc>,
    end: DateTime<Utc>,
    window: &Window,
) -> Result<Option<Value>> {
    if !window.contains(start, end) {
        return Ok(None);
    }
    let id = item["id"]
        .as_str()
        .map(str::to_owned)
        .or_else(|| item["cycle_id"].as_i64().map(|value| value.to_string()))
        .or_else(|| item["id"].as_i64().map(|value| value.to_string()))
        .unwrap_or_else(|| start.to_rfc3339());
    Ok(Some(record(
        "metric",
        "WHOOP",
        &format!("{name}:{id}"),
        start,
        end,
        window.end,
        json!({"metric":name,"provider_data":item}),
    )))
}

fn whoop_cycle_metrics(row: &Value, window: &Window) -> Option<Vec<Value>> {
    if row["payload"]["metric"] != "cycle" {
        return None;
    }
    let cycle = &row["payload"]["provider_data"];
    let id = cycle["id"].as_u64()?;
    let start = parse_date(&cycle["start"]).ok()?;
    let end = parse_date(&cycle["end"]).ok()?;
    if start >= end {
        return None;
    }
    let offset = match cycle["timezone_offset"].as_str()? {
        "Z" => chrono::FixedOffset::east_opt(0)?,
        value => value.parse::<chrono::FixedOffset>().ok()?,
    };
    let midpoint = start + (end - start) / 2;
    let day = midpoint.with_timezone(&offset).date_naive();
    let mut output = Vec::new();
    if let Some(count) = cycle["step_count"]
        .as_u64()
        .filter(|count| *count <= 1_000_000)
    {
        output.push(record(
            "metric","WHOOP",&format!("cycle_steps:{id}"),midpoint,midpoint,window.end,
            json!({"metric":"cycle_steps","provider_data":{
                "cycle_id":id,"value":count,"unit":"steps","local_day":day,"timezone":offset.to_string()
            }}),
        ));
    }
    if let Some(kj) = cycle["score"]["kilojoule"]
        .as_f64()
        .filter(|kj| cycle["score_state"] == "SCORED" && *kj > 0.0)
    {
        output.push(record(
            "metric",
            "WHOOP",
            &format!("total_energy:{id}"),
            midpoint,
            midpoint,
            window.end,
            json!({"metric":"total_energy","provider_data":{
                "cycle_id":id,"value":kj / 4.184,"unit":"kcal","kilojoule":kj,
                "local_day":day,"timezone":offset.to_string()
            }}),
        ));
    }
    Some(output)
}

fn required_text(value: &Value) -> Result<&str> {
    value
        .as_str()
        .filter(|text| !text.is_empty())
        .context("required WHOOP text field is missing")
}

fn parse_date(value: &Value) -> Result<DateTime<Utc>> {
    Ok(required_text(value)?.parse()?)
}

fn record(
    kind: &str,
    source: &str,
    id: &str,
    start: DateTime<Utc>,
    end: DateTime<Utc>,
    modified: DateTime<Utc>,
    payload: Value,
) -> Value {
    json!({"kind":kind,"source":source,"source_id":id,"started_at":start,
        "ended_at":end,"modified_at":modified,"payload":payload})
}

async fn cursor_records(
    request: RequestBuilder,
    item_key: &str,
    response_key: &str,
    query_key: &str,
    mut project: impl FnMut(&Value) -> Result<Option<Value>>,
) -> Result<Vec<Value>> {
    let mut cursor = None::<String>;
    let mut seen = std::collections::HashSet::new();
    let mut rows = Vec::new();
    loop {
        let mut page = request.try_clone().context("request cannot be paginated")?;
        if let Some(cursor) = &cursor {
            page = page.query(&[(query_key, cursor)]);
        }
        let response = body(page.send().await?).await?;
        for item in response[item_key]
            .as_array()
            .context("WHOOP response lacks records")?
        {
            if let Some(row) = project(item)? {
                rows.push(row);
            }
        }
        ensure!(
            rows.len() <= MAX_RECORDS,
            "WHOOP snapshot record limit exceeded"
        );
        let Some(next) = response[response_key]
            .as_str()
            .filter(|next| !next.is_empty())
        else {
            return Ok(rows);
        };
        ensure!(
            seen.len() < 32 && seen.insert(next.to_owned()),
            "incomplete WHOOP pagination"
        );
        cursor = Some(next.to_owned());
    }
}

fn retry_after(header: Option<&str>, now: DateTime<Utc>) -> DateTime<Utc> {
    header
        .filter(|value| !value.is_empty() && value.bytes().all(|byte| byte.is_ascii_digit()))
        .and_then(|value| value.parse::<i64>().ok())
        .and_then(Duration::try_seconds)
        .and_then(|delay| now.checked_add_signed(delay))
        .unwrap_or_else(|| now + Duration::minutes(5))
}

fn check_response(response: Response) -> Result<Response> {
    if response.status() == reqwest::StatusCode::TOO_MANY_REQUESTS {
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
        "WHOOP returned HTTP {}",
        response.status().as_u16()
    );
    Ok(response)
}

async fn body(response: Response) -> Result<Value> {
    let mut response = check_response(response)?;
    let mut bytes = Vec::new();
    while let Some(chunk) = response.chunk().await? {
        ensure!(
            bytes.len() + chunk.len() <= MAX_RESPONSE_BYTES,
            "WHOOP response exceeds size limit"
        );
        bytes.extend_from_slice(&chunk);
    }
    serde_json::from_slice(&bytes).context("invalid WHOOP JSON response")
}

#[cfg(test)]
#[path = "whoop_tests.rs"]
mod tests;
