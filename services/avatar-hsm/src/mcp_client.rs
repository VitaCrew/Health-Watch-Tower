use anyhow::{bail, Context, Result};
use reqwest::{Client, RequestBuilder, Url};
use serde_json::{json, Value};

const VERSION: &str = "2025-06-18";
const SESSION: &str = "Mcp-Session-Id";

pub async fn call(
    client: &Client,
    url: &Url,
    token: &str,
    name: &str,
    arguments: Value,
) -> Result<Value> {
    let initialize = request(client, url, token, VERSION, None)
        .json(&json!({
            "jsonrpc": "2.0", "id": 1, "method": "initialize",
            "params": { "protocolVersion": VERSION, "capabilities": {},
                        "clientInfo": { "name": "avatar-hsm", "version": env!("CARGO_PKG_VERSION") } }
        }))
        .send().await?.error_for_status()?;
    let session = initialize.headers().get(SESSION).cloned();
    let init: Value = initialize.json().await?;
    let version = init["result"]["protocolVersion"]
        .as_str()
        .context("MCP initialization did not negotiate a version")?;
    if !matches!(version, "2025-06-18" | "2025-03-26" | "2025-11-25") {
        bail!("unsupported MCP protocol version");
    }
    request(client, url, token, version, session.as_ref())
        .json(&json!({ "jsonrpc": "2.0", "method": "notifications/initialized" }))
        .send()
        .await?
        .error_for_status()?;
    let reply: Value = request(client, url, token, version, session.as_ref())
        .json(&json!({
            "jsonrpc": "2.0", "id": 2, "method": "tools/call",
            "params": { "name": name, "arguments": arguments }
        }))
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    if reply["id"] != 2 || reply.get("error").is_some() || reply["result"]["isError"] == true {
        bail!("MCP event receipt was rejected");
    }
    reply["result"]
        .get("structuredContent")
        .cloned()
        .context("MCP event receipt lacks structured content")
}

fn request(
    client: &Client,
    url: &Url,
    token: &str,
    version: &str,
    session: Option<&reqwest::header::HeaderValue>,
) -> RequestBuilder {
    let request = client
        .post(url.clone())
        .bearer_auth(token)
        .header("Accept", "application/json, text/event-stream")
        .header("MCP-Protocol-Version", version);
    if let Some(session) = session {
        return request.header(SESSION, session.clone());
    }
    request
}
