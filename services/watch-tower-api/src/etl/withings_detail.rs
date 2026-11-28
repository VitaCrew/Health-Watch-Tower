use super::*;

pub(super) async fn page(request: reqwest::RequestBuilder) -> Result<Option<Value>> {
    let response = request.send().await.map_err(|error| error.without_url())?;
    if response.status() == reqwest::StatusCode::NO_CONTENT {
        return Ok(None);
    }
    let mut result = body(response).await?;
    ensure!(
        result["status"].as_u64() == Some(0),
        "Withings API returned an error"
    );
    ensure!(result["body"].is_object(), "missing Withings response body");
    Ok(Some(result["body"].take()))
}

fn epoch(value: &Value) -> Result<DateTime<Utc>> {
    value
        .as_i64()
        .and_then(|value| DateTime::from_timestamp(value, 0))
        .context("invalid Withings timestamp")
}

pub(super) async fn sleep_segments(
    client: &Client,
    token: &str,
    endpoint: &str,
    summary: &Value,
) -> Result<(Vec<Value>, Vec<Value>)> {
    let start = epoch(&summary["startdate"])?;
    let end = epoch(&summary["enddate"])?;
    let mut segments = Vec::new();
    let mut from = start;
    while from < end {
        // The documented endpoint truncates requests wider than 24 hours.
        let to = (from + CalendarDuration::days(1)).min(end);
        let request = client.post(endpoint).bearer_auth(token).form(&[
            ("action", "get"),
            ("startdate", &from.timestamp().to_string()),
            ("enddate", &to.timestamp().to_string()),
            ("data_fields", "hr,rr,snoring,sdnn_1,rmssd,hrv_quality,mvt_score,chest_movement_rate,withings_index,breathing_sounds"),
        ]);
        if let Some(result) = page(request).await? {
            let series = &result["series"];
            let items: &[Value] = match series {
                Value::Array(items) => items,
                Value::Object(item) if item.is_empty() => &[],
                Value::Object(_) => std::slice::from_ref(series),
                _ => bail!("missing Withings sleep segments"),
            };
            for item in items {
                let segment_start = epoch(&item["startdate"])?;
                let segment_end = epoch(&item["enddate"])?;
                let same_model = summary["model_id"].is_null()
                    || item["model_id"].is_null()
                    || summary["model_id"] == item["model_id"];
                if same_model
                    && segment_start < end
                    && start < segment_end
                    && segment_start < segment_end
                {
                    segments.push((segment_start, segment_end, item.clone()));
                }
            }
        }
        from = to;
    }
    segments.sort_by_key(|(start, end, _)| (*start, *end));
    segments.dedup();
    let stages = sleep_stages(&segments, start, end);
    let raw = segments.into_iter().map(|(_, _, item)| item).collect();
    Ok((stages, raw))
}

fn sleep_stages(
    segments: &[(DateTime<Utc>, DateTime<Utc>, Value)],
    start: DateTime<Utc>,
    end: DateTime<Utc>,
) -> Vec<Value> {
    let mut stages = Vec::new();
    let mut previous_end = start;
    for (from, to, item) in segments {
        let (from, to) = (start.max(*from), end.min(*to));
        let provider_stage = &item["state"];
        let stage = match provider_stage.as_i64() {
            Some(0 | 15) => 1,
            Some(1) => 4,
            Some(2) => 5,
            Some(3) => 6,
            _ => continue,
        };
        // Multiple devices can supply conflicting stages. Preserve their source data.
        if from < previous_end || stages.len() == 500 {
            return Vec::new();
        }
        previous_end = to;
        stages.push(json!({"start":from,"end":to,"stage":stage,"provider_stage":provider_stage}));
    }
    stages
}
