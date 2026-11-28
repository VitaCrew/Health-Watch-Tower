use anyhow::{ensure, Context, Result};
use avatar_hsm::etl_adapter::{self, WithingsNotifyTask};
use deadpool_postgres::Pool;
use reqwest::{Client, Response};
use serde_json::Value;
use std::time::Duration;

const ENDPOINT: &str = "https://wbsapi.withings.net/notify";
const MAX_RESPONSE_BYTES: usize = 1024 * 1024;

async fn response_json(response: Response) -> Result<Value> {
    ensure!(
        response.status().is_success(),
        "Withings notification API returned HTTP {}",
        response.status().as_u16()
    );
    let mut response = response;
    let mut bytes = Vec::new();
    while let Some(chunk) = response.chunk().await? {
        ensure!(
            bytes.len() + chunk.len() <= MAX_RESPONSE_BYTES,
            "Withings subscription response exceeds size limit"
        );
        bytes.extend_from_slice(&chunk);
    }
    let value: Value =
        serde_json::from_slice(&bytes).context("invalid Withings subscription response")?;
    ensure!(
        value["status"].as_u64() == Some(0),
        "Withings subscription request failed"
    );
    Ok(value)
}

async fn notify(
    client: &Client,
    task: &WithingsNotifyTask,
    fields: &[(&str, &str)],
) -> Result<Value> {
    response_json(
        client
            .post(ENDPOINT)
            .bearer_auth(task.token())
            .form(fields)
            .send()
            .await?,
    )
    .await
}

fn registered_categories(result: &Value, callback: &str) -> Result<Vec<u16>> {
    Ok(result["body"]["profiles"]
        .as_array()
        .context("invalid Withings subscription list")?
        .iter()
        .filter(|profile| profile["callbackurl"].as_str() == Some(callback))
        .filter_map(|profile| {
            profile["appli"]
                .as_u64()
                .and_then(|n| u16::try_from(n).ok())
                .or_else(|| profile["appli"].as_str()?.parse().ok())
        })
        .collect())
}

async fn reconcile(client: &Client, task: &WithingsNotifyTask) -> Result<&'static str> {
    if task.categories().is_empty() {
        return Ok("no_scope");
    }
    let result = notify(client, task, &[("action", "list")]).await?;
    let registered = registered_categories(&result, task.callback())?;
    for category in task
        .categories()
        .iter()
        .filter(|category| !registered.contains(category))
    {
        let category = category.to_string();
        notify(
            client,
            task,
            &[
                ("action", "subscribe"),
                ("appli", category.as_str()),
                ("callbackurl", task.callback()),
            ],
        )
        .await?;
    }
    Ok("active")
}

async fn run_one(pool: &Pool, client: &Client, task: &WithingsNotifyTask) -> Result<()> {
    let result = match tokio::time::timeout(Duration::from_secs(35), reconcile(client, task)).await
    {
        Ok(Ok(state)) => state,
        Ok(Err(_)) | Err(_) => "retrying",
    };
    etl_adapter::finish_withings_notify(pool, task, result).await?;
    Ok(())
}

pub async fn worker(pool: Pool, client: Client) {
    loop {
        match etl_adapter::next_withings_notify(&pool, &client).await {
            Ok(Some(task)) => {
                let _ = run_one(&pool, &client, &task).await;
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
            Ok(None) | Err(_) => tokio::time::sleep(Duration::from_secs(15)).await,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_subscriptions_for_the_configured_callback_are_reused() {
        let result = serde_json::json!({"body":{"profiles":[
            {"callbackurl":"https://example.test/hook","appli":1},
            {"callbackurl":"https://other.test/hook","appli":"2"},
            {"callbackurl":"https://example.test/hook","appli":"54"}
        ]}});
        assert_eq!(
            registered_categories(&result, "https://example.test/hook").unwrap(),
            vec![1, 54]
        );
    }

    #[test]
    fn malformed_subscription_list_is_rejected() {
        assert!(
            registered_categories(&serde_json::json!({"body":{}}), "https://example.test").is_err()
        );
    }
}
