use std::time::Duration;

use anyhow::{ensure, Context, Result};
use deadpool_postgres::Pool;
use serde::Deserialize;
use serde_json::{json, Value};

const SOURCE: &str = "mfds_e_yak_eun_yo";
const SOURCE_URI: &str = "https://www.data.go.kr/data/15075057/openapi.do";
const LICENSE_URI: &str = "https://www.data.go.kr/ugs/selectPortalPolicyView.do";
const LICENSE_ID: &str = "data.go.kr:15075057:reuse-scope-no-restriction";

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Search {
    query: String,
}

pub async fn lookup(pool: &Pool, input: Search) -> Result<Value> {
    let query = input.query.trim();
    ensure!(
        (2..=100).contains(&query.chars().count()),
        "invalid medication search"
    );
    let fresh_after = chrono::Utc::now() - chrono::Duration::hours(24);
    let cached = crate::catalog_store::find(pool, SOURCE, query, Some(fresh_after)).await?;
    if !cached.is_empty() {
        return Ok(json!({"status":"ready","medications":cached}));
    }
    let Some(key) = std::env::var("AVATAR_MFDS_API_KEY")
        .ok()
        .filter(|value| !value.trim().is_empty())
    else {
        let stale = crate::catalog_store::find(pool, SOURCE, query, None).await?;
        return Ok(
            json!({"status": if stale.is_empty() {"configuration_required"} else {"stale_cache"},"medications":stale}),
        );
    };
    let http = reqwest::Client::builder()
        .timeout(Duration::from_secs(8))
        .redirect(reqwest::redirect::Policy::none())
        .user_agent("OpenHuman-HealthWatchTower/0.1")
        .build()?;
    let response = match http
        .get("https://apis.data.go.kr/1471000/DrbEasyDrugInfoService/getDrbEasyDrugList")
        .query(&[
            ("serviceKey", key.as_str()),
            ("pageNo", "1"),
            ("numOfRows", "10"),
            ("type", "json"),
            ("itemName", query),
        ])
        .send()
        .await
    {
        Ok(response) => response,
        Err(_) => {
            let stale = crate::catalog_store::find(pool, SOURCE, query, None).await?;
            return Ok(
                json!({"status": if stale.is_empty() {"unavailable"} else {"stale_cache"},"medications":stale}),
            );
        }
    };
    let body: Value = match response.error_for_status() {
        Ok(response) => match response.json().await {
            Ok(body) => body,
            Err(_) => {
                let stale = crate::catalog_store::find(pool, SOURCE, query, None).await?;
                return Ok(
                    json!({"status": if stale.is_empty() {"unavailable"} else {"stale_cache"},"medications":stale}),
                );
            }
        },
        Err(_) => {
            let stale = crate::catalog_store::find(pool, SOURCE, query, None).await?;
            return Ok(
                json!({"status": if stale.is_empty() {"unavailable"} else {"stale_cache"},"medications":stale}),
            );
        }
    };
    let rows = body["body"]["items"]
        .as_array()
        .context("invalid MFDS e약은요 response")?;
    let retrieved_at = chrono::Utc::now().to_rfc3339();
    let medications = rows
        .iter()
        .filter_map(|row| {
            let source_id = row["itemSeq"].as_str()?.trim();
            let name = row["itemName"].as_str()?.trim();
            if source_id.is_empty() || name.is_empty() {
                return None;
            }
            Some(json!({
                "source": SOURCE,
                "source_id": source_id,
                "source_uri": SOURCE_URI,
                "license_id": LICENSE_ID,
                "license_uri": LICENSE_URI,
                "retrieved_at": retrieved_at,
                "name": name,
                "manufacturer": row["entpName"],
                "efficacy": row["efcyQesitm"],
                "directions": row["useMethodQesitm"],
                "warning": row["atpnWarnQesitm"],
                "cautions": row["atpnQesitm"],
                "interactions": row["intrcQesitm"],
                "side_effects": row["seQesitm"],
                "storage": row["depositMethodQesitm"],
                "disclaimer": "일반 제품 설명입니다. 개인의 처방, 복용량, 금기 여부를 대신 판단하지 않습니다.",
            }))
        })
        .collect::<Vec<_>>();
    crate::catalog_store::save(pool, &medications).await?;
    Ok(json!({
        "status": if medications.is_empty() {"no_records"} else {"ready"},
        "medications": medications,
    }))
}
