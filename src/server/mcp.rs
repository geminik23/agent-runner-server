use std::{
    borrow::Cow,
    sync::{Arc, atomic::Ordering},
};

use rmcp::{
    ErrorData, RoleServer, ServerHandler,
    model::{
        CallToolRequestParams, CallToolResponse, CallToolResult, Implementation, ListToolsResult,
        PaginatedRequestParams, ProtocolVersion, ServerCapabilities, ServerConfig, Tool,
    },
    service::RequestContext,
};

use super::{RequestPrincipal, Shared, wire};

pub(super) const VERSIONS: [&str; 3] = ["2026-07-28", "2025-11-25", "2025-06-18"];

#[derive(Clone)]
pub(super) struct Handler {
    pub shared: Arc<Shared>,
}

fn principal(context: &RequestContext<RoleServer>) -> Result<RequestPrincipal, ErrorData> {
    context
        .extensions
        .get::<axum::http::request::Parts>()
        .and_then(|p| p.extensions.get::<RequestPrincipal>())
        .cloned()
        .ok_or_else(|| ErrorData::internal_error("missing authenticated request context", None))
}

fn tool(name: &str, max_skill_page: usize) -> Option<Tool> {
    if !wire::TOOLS.contains(&name) {
        return None;
    }
    let description = match name {
        "agents_list" => "List the profiles and workspaces currently granted to this caller.",
        "agents_start" => {
            "Durably admit a run and return its receipt, not its result. Omitted skills use defaults; an explicit ordered list replaces defaults, and an empty list disables them. Nonempty skills require SkillRead. Retry lost responses with the same idempotency_key and request. Disconnecting does not cancel the run."
        }
        "runs_get" => "Read current run status. A failed run is data, not a failed lookup.",
        "runs_result" => {
            "Read result availability and exact UTF-8 or base64 payload. Pending is a normal result; do not resubmit the work with a new key."
        }
        "runs_events" => {
            "Read a bounded page of event summaries without raw payloads. Continue with next_after; an empty page does not mean a running run has finished."
        }
        "runs_cancel" => {
            "Commit an explicit stop decision. The response does not prove cleanup has completed."
        }
        "skills_register" => {
            "Register an immutable skill version with its exact Markdown and required capabilities."
        }
        "skills_list" => "List a bounded page of this client's skill versions.",
        "skills_get" => "Read and export one exact skill version as Markdown.",
        "skills_set_enabled" => {
            "Enable or revoke an exact skill version for future admission and launch."
        }
        "skills_defaults" => "Read this client's ordered default skill versions.",
        "skills_set_defaults" => "Replace this client's ordered default skill versions.",
        "destinations_list" => "List the caller's granted completion destination identifiers.",
        "hooks_register" => "Register a versioned completion hook for a granted destination.",
        "hooks_list" => "List a bounded page of completion hook versions.",
        "hooks_get" => "Read one completion hook version.",
        "hooks_set_enabled" => "Enable or revoke a hook version.",
        "hooks_defaults" => "Read the ordered completion hook defaults.",
        "hooks_set_defaults" => "Replace the ordered completion hook defaults.",
        "inbox_list" => "Read a bounded page of completion notifications.",
        "inbox_ack" => {
            "Acknowledge receipt of an inbox notification without marking it processed by an agent."
        }
        "runs_deliveries" => "Read bounded delivery status without exposing the payload.",
        "delivery_reconcile" => {
            "Record an authorized operator's confirmed outcome for an uncertain delivery"
        }
        _ => return None,
    };
    Some(Tool::new(
        name.to_owned(),
        description,
        Arc::new(wire::schema(name, max_skill_page).as_object()?.clone()),
    ))
}

impl ServerHandler for Handler {
    fn get_info(&self) -> ServerConfig {
        ServerConfig::new(ServerCapabilities::builder().enable_tools().build())
            .with_server_info(Implementation::new("agent-runner-server", env!("CARGO_PKG_VERSION")))
            .with_instructions("Execution is asynchronous. Preserve idempotency keys across retries. MCP sessions and connections do not own durable runs. Only explicitly advertised profiles are available.")
    }
    fn supported_protocol_versions(&self) -> Cow<'static, [ProtocolVersion]> {
        Cow::Borrowed(&[
            ProtocolVersion::V_2026_07_28,
            ProtocolVersion::V_2025_11_25,
            ProtocolVersion::V_2025_06_18,
        ])
    }
    fn get_tool(&self, name: &str) -> Option<Tool> {
        tool(name, self.shared.handle.max_skill_page_size())
    }

    async fn list_tools(
        &self,
        request: Option<PaginatedRequestParams>,
        context: RequestContext<RoleServer>,
    ) -> Result<ListToolsResult, ErrorData> {
        let principal = principal(&context)?;
        if request.is_some_and(|r| r.cursor.is_some()) {
            return Err(ErrorData::invalid_params(
                "tool-list cursors are not supported",
                None,
            ));
        }
        if self.shared.access.identity(&principal.client).is_err() {
            principal.http_status.store(401, Ordering::Release);
            return Err(ErrorData::internal_error("authentication required", None));
        }
        let tools: Vec<_> = wire::TOOLS
            .iter()
            .filter(|name| {
                wire::required_scopes(name).is_some_and(|scopes| {
                    scopes.iter().all(|scope| {
                        self.shared
                            .access
                            .check_scope(&principal.client, *scope)
                            .is_ok()
                    })
                })
            })
            .filter_map(|name| tool(name, self.shared.handle.max_skill_page_size()))
            .collect();
        serde_json::from_value(serde_json::json!({"resultType":"complete", "tools":tools}))
            .map_err(|_| ErrorData::internal_error("tool metadata unavailable", None))
    }
    async fn call_tool(
        &self,
        request: CallToolRequestParams,
        context: RequestContext<RoleServer>,
    ) -> Result<CallToolResponse, ErrorData> {
        let principal = principal(&context)?;
        if !wire::TOOLS.contains(&request.name.as_ref()) {
            return Err(ErrorData::invalid_params("unknown tool", None));
        }
        let args = request
            .arguments
            .map(serde_json::Value::Object)
            .unwrap_or_else(|| serde_json::json!({}));
        let result = match wire::execute(
            &self.shared.handle,
            &principal.client,
            &request.name,
            args,
            self.shared.limits.max_response_bytes,
        )
        .await
        {
            Ok(value) => CallToolResult::structured(value),
            Err(error) => {
                if matches!(error.status.as_u16(), 401 | 403) {
                    principal
                        .http_status
                        .store(error.status.as_u16(), Ordering::Release);
                }
                CallToolResult::structured_error(error.value())
            }
        };
        Ok(CallToolResponse::Complete(result))
    }
}
