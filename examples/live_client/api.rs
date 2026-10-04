//! REST and MCP calls to the daemon behind one normalized reply shape.

use std::{
    sync::atomic::{AtomicU64, Ordering},
    time::Duration,
};

use serde_json::{Value, json};

/// MCP protocol revision sent with every tool call.
const MCP_PROTOCOL: &str = "2026-07-28";

/// Selects the transport that carries one tool call.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Via {
    #[default]
    Rest,
    Mcp,
}

impl Via {
    pub fn parse(value: &str) -> Result<Self, String> {
        match value {
            "rest" => Ok(Self::Rest),
            "mcp" => Ok(Self::Mcp),
            other => Err(format!("unknown transport `{other}`; use rest or mcp")),
        }
    }

    pub fn name(self) -> &'static str {
        match self {
            Self::Rest => "rest",
            Self::Mcp => "mcp",
        }
    }
}

/// One tool response; `error` carries the server error code when the call was rejected.
#[derive(Debug)]
pub struct Reply {
    pub status: u16,
    pub body: Value,
    pub error: Option<String>,
}

/// Authenticated access to one running daemon.
pub struct Api {
    client: reqwest::Client,
    url: String,
    bearer: String,
    next_id: AtomicU64,
}

impl Api {
    pub fn new(url: String, bearer: String) -> Result<Self, String> {
        let client = reqwest::Client::builder()
            .no_proxy()
            .timeout(Duration::from_secs(10))
            .build()
            .map_err(|error| format!("HTTP client: {error}"))?;
        Ok(Self {
            client,
            url,
            bearer,
            next_id: AtomicU64::new(1),
        })
    }

    /// Calls `tool` over the selected transport; transport failures are errors, rejected calls are replies.
    pub async fn call(&self, via: Via, tool: &str, args: &Value) -> Result<Reply, String> {
        match via {
            Via::Rest => self.rest(tool, args).await,
            Via::Mcp => self.mcp(tool, args).await,
        }
    }

    async fn rest(&self, tool: &str, args: &Value) -> Result<Reply, String> {
        let route = rest_route(tool, args)?;
        let url = format!("{}{}", self.url, route.path);
        let request = match route.body {
            Some(body) => self.client.post(url).json(&body),
            None => self.client.get(url),
        };
        let response = request
            .bearer_auth(&self.bearer)
            .send()
            .await
            .map_err(|error| format!("{tool} over REST: {error}"))?;
        let status = response.status().as_u16();
        let text = response
            .text()
            .await
            .map_err(|error| format!("{tool} over REST: {error}"))?;
        let body = if text.is_empty() {
            Value::Null
        } else {
            serde_json::from_str(&text)
                .map_err(|_| format!("{tool} over REST: non-JSON response with status {status}"))?
        };
        let error = body["error"]["code"]
            .as_str()
            .map(str::to_owned)
            .or_else(|| (status >= 400).then(|| format!("http_{status}")));
        Ok(Reply {
            status,
            body,
            error,
        })
    }

    async fn mcp(&self, tool: &str, args: &Value) -> Result<Reply, String> {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let body = json!({"jsonrpc":"2.0","id":id,"method":"tools/call","params":{
            "name":tool,"arguments":args,
            "_meta":{"io.modelcontextprotocol/protocolVersion":MCP_PROTOCOL,"io.modelcontextprotocol/clientCapabilities":{}}
        }});
        let response = self
            .client
            .post(format!("{}/mcp", self.url))
            .bearer_auth(&self.bearer)
            .header("Accept", "application/json, text/event-stream")
            .header("MCP-Protocol-Version", MCP_PROTOCOL)
            .header("Mcp-Method", "tools/call")
            .header("Mcp-Name", tool)
            .json(&body)
            .send()
            .await
            .map_err(|error| format!("{tool} over MCP: {error}"))?;
        let status = response.status().as_u16();
        let text = response
            .text()
            .await
            .map_err(|error| format!("{tool} over MCP: {error}"))?;
        if status != 200 {
            return Ok(Reply {
                status,
                body: Value::Null,
                error: Some(format!("http_{status}")),
            });
        }
        // Streamable HTTP may answer with a single JSON body or with SSE data lines.
        let payload = if text.starts_with('{') {
            text.as_str()
        } else {
            text.lines()
                .filter_map(|line| line.strip_prefix("data: "))
                .find(|line| line.starts_with('{'))
                .ok_or_else(|| format!("{tool} over MCP: no JSON payload"))?
        };
        let value: Value = serde_json::from_str(payload)
            .map_err(|_| format!("{tool} over MCP: invalid JSON-RPC payload"))?;
        if let Some(code) = value["error"]["code"].as_i64() {
            return Ok(Reply {
                status,
                body: value["error"].clone(),
                error: Some(format!("jsonrpc_{code}")),
            });
        }
        let content = value["result"]["structuredContent"].clone();
        let error = (value["result"]["isError"] == true).then(|| {
            content["error"]["code"]
                .as_str()
                .unwrap_or("tool_error")
                .to_owned()
        });
        Ok(Reply {
            status,
            body: content,
            error,
        })
    }
}

/// REST location of one tool call; `body` is present for POST routes.
#[derive(Debug, PartialEq)]
struct Route {
    path: String,
    body: Option<Value>,
}

/// Maps a tool name and its MCP arguments to the equivalent REST route.
fn rest_route(tool: &str, args: &Value) -> Result<Route, String> {
    let get = |path: &str| {
        Ok(Route {
            path: path.to_owned(),
            body: None,
        })
    };
    let post = |path: &str| {
        Ok(Route {
            path: path.to_owned(),
            body: Some(args.clone()),
        })
    };
    let run = || -> Result<&str, String> {
        let id = args["run_id"]
            .as_str()
            .ok_or_else(|| format!("{tool} needs a string run_id"))?;
        if id.is_empty()
            || !id
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-' || byte == b'_')
        {
            return Err(format!("{tool}: run_id is not a plain identifier"));
        }
        Ok(id)
    };
    match tool {
        "agents_list" => get("/v1/agents"),
        "destinations_list" => get("/v1/destinations"),
        "agents_start" => post("/v1/runs"),
        "runs_get" => get(&format!("/v1/runs/{}", run()?)),
        "runs_result" => get(&format!("/v1/runs/{}/result", run()?)),
        "runs_events" => {
            let limit = args["limit"]
                .as_u64()
                .ok_or("runs_events needs a numeric limit")?;
            let mut path = format!("/v1/runs/{}/events?limit={limit}", run()?);
            if let Some(after) = args["after"].as_i64() {
                path.push_str(&format!("&after={after}"));
            }
            get(&path)
        }
        "runs_cancel" => Ok(Route {
            path: format!("/v1/runs/{}/cancel", run()?),
            body: Some(json!({})),
        }),
        "skills_register" => post("/v1/skills"),
        "skills_list" => post("/v1/skills/list"),
        "skills_get" => post("/v1/skills/get"),
        "skills_set_enabled" => post("/v1/skills/enabled"),
        "skills_defaults" => get("/v1/skill-defaults"),
        "skills_set_defaults" => post("/v1/skill-defaults"),
        "hooks_register" => post("/v1/hooks"),
        "hooks_list" => post("/v1/hooks/list"),
        "hooks_get" => post("/v1/hooks/get"),
        "hooks_set_enabled" => post("/v1/hooks/enabled"),
        "hooks_defaults" => get("/v1/hook-defaults"),
        "hooks_set_defaults" => post("/v1/hook-defaults"),
        "inbox_list" => post("/v1/inbox/list"),
        "inbox_ack" => post("/v1/inbox/ack"),
        "runs_deliveries" => post("/v1/deliveries/list"),
        "delivery_reconcile" => post("/v1/deliveries/reconcile"),
        other => Err(format!("no REST route for `{other}`; call it over MCP")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rest_routes_follow_the_server_table() {
        let run = json!({"run_id":"r-1"});
        assert_eq!(rest_route("runs_get", &run).unwrap().path, "/v1/runs/r-1");
        assert_eq!(
            rest_route("runs_cancel", &run).unwrap(),
            Route {
                path: "/v1/runs/r-1/cancel".into(),
                body: Some(json!({}))
            }
        );
        let events =
            rest_route("runs_events", &json!({"run_id":"r-1","limit":4,"after":7})).unwrap();
        assert_eq!(events.path, "/v1/runs/r-1/events?limit=4&after=7");
        assert!(rest_route("runs_get", &json!({"run_id":"../x"})).is_err());
        assert!(rest_route("unknown_tool", &json!({})).is_err());
    }
}
