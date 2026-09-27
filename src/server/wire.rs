use axum::http::StatusCode;
use base64::{Engine, engine::general_purpose::STANDARD};
use serde::Deserialize;
use serde_json::{Value, json};

use crate::{
    access::{AccessError, AuthenticatedClient, Scope},
    application::{CancelOutcome, RunHandle, ServiceError},
    core::ExecutionMetadata,
    storage::{ResultView, StorageError},
};

pub(crate) const TOOLS: [&str; 23] = [
    "agents_list",
    "agents_start",
    "runs_get",
    "runs_result",
    "runs_events",
    "runs_cancel",
    "skills_register",
    "skills_list",
    "skills_get",
    "skills_set_enabled",
    "skills_defaults",
    "skills_set_defaults",
    "destinations_list",
    "hooks_register",
    "hooks_list",
    "hooks_get",
    "hooks_set_enabled",
    "hooks_defaults",
    "hooks_set_defaults",
    "inbox_list",
    "inbox_ack",
    "runs_deliveries",
    "delivery_reconcile",
];

pub(crate) fn required_scopes(name: &str) -> Option<&'static [Scope]> {
    match name {
        "agents_list" | "agents_start" => Some(&[Scope::RunStart]),
        "runs_get" | "runs_result" | "runs_events" => Some(&[Scope::RunRead]),
        "runs_cancel" => Some(&[Scope::RunCancel]),
        "skills_list" | "skills_get" | "skills_defaults" => Some(&[Scope::SkillRead]),
        "skills_register" | "skills_set_enabled" | "skills_set_defaults" => {
            Some(&[Scope::SkillRead, Scope::SkillWrite])
        }
        "destinations_list" | "hooks_list" | "hooks_get" | "hooks_defaults" => {
            Some(&[Scope::HookRead])
        }
        "hooks_register" | "hooks_set_enabled" | "hooks_set_defaults" => {
            Some(&[Scope::HookRead, Scope::HookWrite])
        }
        "inbox_list" => Some(&[Scope::InboxRead]),
        "inbox_ack" => Some(&[Scope::InboxAck]),
        "runs_deliveries" => Some(&[Scope::RunRead, Scope::HookRead]),
        "delivery_reconcile" => Some(&[Scope::RunRead, Scope::HookRead, Scope::DeliveryReconcile]),
        _ => None,
    }
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct ApiError {
    pub status: StatusCode,
    pub code: &'static str,
}

impl ApiError {
    pub const fn new(status: StatusCode, code: &'static str) -> Self {
        Self { status, code }
    }
    pub const fn invalid() -> Self {
        Self::new(StatusCode::BAD_REQUEST, "invalid_request")
    }
    pub fn value(self) -> Value {
        json!({"error": {"code": self.code}})
    }
}

impl From<AccessError> for ApiError {
    fn from(value: AccessError) -> Self {
        match value {
            AccessError::Unauthenticated => Self::new(StatusCode::UNAUTHORIZED, "unauthenticated"),
            AccessError::Forbidden => Self::new(StatusCode::FORBIDDEN, "forbidden"),
            AccessError::InvalidRequest => Self::invalid(),
            AccessError::Storage(e) => e.into(),
            AccessError::Unavailable => Self::new(StatusCode::SERVICE_UNAVAILABLE, "unavailable"),
            AccessError::InvalidConfiguration => {
                Self::new(StatusCode::INTERNAL_SERVER_ERROR, "invalid_configuration")
            }
        }
    }
}
impl From<StorageError> for ApiError {
    fn from(value: StorageError) -> Self {
        match value {
            StorageError::NotFound => Self::new(StatusCode::NOT_FOUND, "not_found"),
            StorageError::Expired => Self::new(StatusCode::GONE, "expired"),
            StorageError::IdempotencyConflict => {
                Self::new(StatusCode::CONFLICT, "idempotency_conflict")
            }
            StorageError::SessionBusy => Self::new(StatusCode::CONFLICT, "session_busy"),
            StorageError::SessionIncompatible => {
                Self::new(StatusCode::CONFLICT, "session_incompatible")
            }
            StorageError::SkillVersionConflict => {
                Self::new(StatusCode::CONFLICT, "skill_version_conflict")
            }
            StorageError::SkillBundleConflict => {
                Self::new(StatusCode::CONFLICT, "skill_bundle_conflict")
            }
            StorageError::SkillUnavailable => Self::new(StatusCode::CONFLICT, "skill_unavailable"),
            StorageError::SkillUseForbidden | StorageError::HookUseForbidden => {
                Self::new(StatusCode::FORBIDDEN, "forbidden")
            }
            StorageError::HookVersionConflict => {
                Self::new(StatusCode::CONFLICT, "hook_version_conflict")
            }
            StorageError::HookUnavailable => Self::new(StatusCode::CONFLICT, "hook_unavailable"),
            StorageError::DeliveryQuota => {
                Self::new(StatusCode::TOO_MANY_REQUESTS, "delivery_quota")
            }
            StorageError::QueueFull => Self::new(StatusCode::TOO_MANY_REQUESTS, "queue_full"),
            StorageError::InvalidInput("result limit" | "event limit") => {
                Self::new(StatusCode::INTERNAL_SERVER_ERROR, "stored_data_limit")
            }
            StorageError::InvalidInput(_) => Self::invalid(),
            StorageError::Database(_) => {
                Self::new(StatusCode::SERVICE_UNAVAILABLE, "storage_unavailable")
            }
            _ => Self::new(StatusCode::INTERNAL_SERVER_ERROR, "storage_invariant"),
        }
    }
}
impl From<ServiceError> for ApiError {
    fn from(value: ServiceError) -> Self {
        match value {
            ServiceError::Access(e) => e.into(),
            ServiceError::Storage(e) => e.into(),
            ServiceError::Unavailable => Self::new(StatusCode::SERVICE_UNAVAILABLE, "unavailable"),
            _ => Self::new(StatusCode::INTERNAL_SERVER_ERROR, "service_configuration"),
        }
    }
}

pub(crate) fn metadata(m: &ExecutionMetadata) -> Value {
    json!({"runtime": m.runtime, "synthetic": m.synthetic, "scenario": m.scenario})
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RunArgs {
    run_id: String,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct EventArgs {
    run_id: String,
    after: Option<i64>,
    limit: usize,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Empty {}

/// Both adapters execute the same operation and produce the same business DTO.
pub(crate) async fn execute(
    handle: &RunHandle,
    client: &AuthenticatedClient,
    name: &str,
    args: Value,
    max_response_bytes: usize,
) -> Result<Value, ApiError> {
    match name {
        "agents_list" => {
            let _: Empty = serde_json::from_value(args).map_err(|_| ApiError::invalid())?;
            let profiles = handle.allowed_profiles(client)?.into_iter().map(|(g, m)| json!({
                "profile_id": g.profile_id, "workspaces": g.workspaces, "max_input_bytes": g.max_input_bytes,
                "metadata": metadata(&m),
            })).collect::<Vec<_>>();
            Ok(json!({"profiles": profiles}))
        }
        "agents_start" => {
            let body = serde_json::to_vec(&args).map_err(|_| ApiError::invalid())?;
            let r = handle.start(client, &body).await?;
            Ok(
                json!({"run_id": r.run_id, "session_id": r.session_id, "metadata": metadata(&r.metadata), "replay": r.replay}),
            )
        }
        "runs_get" => {
            let a: RunArgs = serde_json::from_value(args).map_err(|_| ApiError::invalid())?;
            let run = handle
                .get_run(client, &a.run_id)
                .await?
                .ok_or(ApiError::new(StatusCode::NOT_FOUND, "not_found"))?;
            Ok(
                json!({"run_id": run.run_id, "state": run.state, "metadata": metadata(&run.metadata)}),
            )
        }
        "runs_result" => {
            let a: RunArgs = serde_json::from_value(args).map_err(|_| ApiError::invalid())?;
            match handle.get_result_view(client, &a.run_id).await? {
                ResultView::NotFound => Err(ApiError::new(StatusCode::NOT_FOUND, "not_found")),
                ResultView::Pending(run) => Ok(
                    json!({"run_id": run.run_id, "state": run.state, "availability": "pending", "metadata": metadata(&run.metadata)}),
                ),
                ResultView::Expired(run) => Ok(
                    json!({"run_id": run.run_id, "state": run.state, "availability": "expired", "metadata": metadata(&run.metadata)}),
                ),
                ResultView::Available { run, result } => {
                    let (encoding, data) = match String::from_utf8(result.payload) {
                        Ok(s) => ("utf8", s),
                        Err(e) => ("base64", STANDARD.encode(e.as_bytes())),
                    };
                    Ok(
                        json!({"run_id": run.run_id, "state": run.state, "availability": "available", "revision": result.revision, "encoding": encoding, "data": data, "metadata": metadata(&result.metadata)}),
                    )
                }
            }
        }
        "runs_events" => {
            let a: EventArgs = serde_json::from_value(args).map_err(|_| ApiError::invalid())?;
            let events = handle
                .get_event_page(client, &a.run_id, a.after, a.limit)
                .await?
                .ok_or(ApiError::new(StatusCode::NOT_FOUND, "not_found"))?;
            let next_after = events.last().map(|e| e.sequence).or(a.after);
            let events = events.into_iter().map(|e| json!({"event_id": e.event_id, "sequence": e.sequence, "kind": e.kind, "metadata": metadata(&e.metadata)})).collect::<Vec<_>>();
            Ok(json!({"run_id": a.run_id, "events": events, "next_after": next_after}))
        }
        "runs_cancel" => {
            let a: RunArgs = serde_json::from_value(args).map_err(|_| ApiError::invalid())?;
            let outcome = match handle.cancel(client, &a.run_id).await? {
                CancelOutcome::CancelRequested => "cancel_requested",
                CancelOutcome::TimedOut => "timed_out",
                CancelOutcome::Interrupted => "interrupted",
                CancelOutcome::AlreadyTerminal => "already_terminal",
            };
            Ok(json!({"run_id": a.run_id, "outcome": outcome}))
        }
        "skills_register"
        | "skills_list"
        | "skills_get"
        | "skills_set_enabled"
        | "skills_defaults"
        | "skills_set_defaults" => handle
            .skill_operation(client, name, args, max_response_bytes)
            .await
            .map_err(Into::into),
        "destinations_list" | "hooks_register" | "hooks_list" | "hooks_get"
        | "hooks_set_enabled" | "hooks_defaults" | "hooks_set_defaults" | "inbox_list"
        | "inbox_ack" | "runs_deliveries" | "delivery_reconcile" => handle
            .completion_operation(client, name, args, max_response_bytes)
            .await
            .map_err(Into::into),
        _ => Err(ApiError::new(StatusCode::NOT_FOUND, "unknown_operation")),
    }
}

/// Detect duplicate keys before an SDK can collapse objects into a map.
pub(crate) fn parse_json(bytes: &[u8]) -> Result<Value, ApiError> {
    crate::validation::parse_unique_json(bytes).map_err(|_| ApiError::invalid())
}

pub(crate) fn schema(name: &str, max_skill_page: usize) -> Value {
    match name {
        "agents_list" => json!({"type":"object", "additionalProperties":false}),
        "agents_start" => {
            json!({"type":"object","required":["profile_id","workspace_id","input","idempotency_key"],"additionalProperties":false,"properties":{
                "profile_id":{"type":"string"},"workspace_id":{"type":"string"},"idempotency_key":{"type":"string"},
                "result_schema":{"type":"object","description":"Optional JSON result schema subset. Each node requires a single type: object, array, string, number, boolean or null. Object keywords are properties, required and boolean additionalProperties; array supports items. Maximum schema depth is 12 and maximum nodes is 128. Validated output is limited to 262144 bytes. Unsupported types and keywords are rejected."},
                "skills":{"description":"When omitted, use defaults. When present, replace defaults with exactly this ordered list; an empty list selects no skills. Nonempty selection requires SkillRead.","type":"array","maxItems":32,"items":{"type":"object","required":["skill_id","version"],"additionalProperties":false,"properties":{"skill_id":{"type":"string"},"version":{"type":"string"}}}},
                "hooks":{"type":"array","maxItems":32,"description":"Omitted uses defaults; an explicit list replaces them.","items":{"type":"object","required":["hook_id","version"],"additionalProperties":false,"properties":{"hook_id":{"type":"string"},"version":{"type":"string"}}}},
                "input":{"type":"object","required":["request"],"additionalProperties":false,"properties":{"request":{"type":"string"}}},
                "session":{"oneOf":[{"type":"object","required":["mode"],"additionalProperties":false,"properties":{"mode":{"const":"new"}}},{"type":"object","required":["mode","session_id"],"additionalProperties":false,"properties":{"mode":{"const":"resume"},"session_id":{"type":"string"}}},{"type":"object","required":["mode","native_id"],"additionalProperties":false,"properties":{"mode":{"const":"resume_native"},"native_id":{"type":"string"}}}]}
            }})
        }
        "skills_register" => {
            json!({"type":"object","required":["skill_id","version","markdown"],"additionalProperties":false,"properties":{"skill_id":{"type":"string"},"version":{"type":"string"},"markdown":{"type":"string"},"required_capabilities":{"type":"array","maxItems":32,"items":{"type":"string"}}}})
        }
        "skills_get" => {
            json!({"type":"object","required":["skill_id","version"],"additionalProperties":false,"properties":{"skill_id":{"type":"string"},"version":{"type":"string"}}})
        }
        "skills_list" => {
            json!({"type":"object","required":["limit"],"additionalProperties":false,"properties":{"limit":{"type":"integer","minimum":1,"maximum":max_skill_page},"after":{"type":["object","null"],"required":["skill_id","version"],"additionalProperties":false,"properties":{"skill_id":{"type":"string"},"version":{"type":"string"}}}}})
        }
        "skills_set_enabled" => {
            json!({"type":"object","required":["skill_id","version","enabled"],"additionalProperties":false,"properties":{"skill_id":{"type":"string"},"version":{"type":"string"},"enabled":{"type":"boolean"}}})
        }
        "skills_defaults" => json!({"type":"object","additionalProperties":false}),
        "skills_set_defaults" => {
            json!({"type":"object","required":["skills"],"additionalProperties":false,"properties":{"skills":{"type":"array","maxItems":32,"items":{"type":"object","required":["skill_id","version"],"additionalProperties":false,"properties":{"skill_id":{"type":"string"},"version":{"type":"string"}}}}}})
        }
        "destinations_list" | "hooks_defaults" => {
            json!({"type":"object","additionalProperties":false})
        }
        "hooks_register" => {
            json!({"type":"object","required":["hook_id","version","destination_id","events"],"additionalProperties":false,"properties":{"hook_id":{"type":"string"},"version":{"type":"string"},"destination_id":{"type":"string"},"events":{"type":"array","minItems":1,"maxItems":5,"items":{"enum":["succeeded","failed","cancelled","timed_out","interrupted"]}}}})
        }
        "hooks_get" => {
            json!({"type":"object","required":["hook_id","version"],"additionalProperties":false,"properties":{"hook_id":{"type":"string"},"version":{"type":"string"}}})
        }
        "hooks_list" => {
            json!({"type":"object","required":["limit"],"additionalProperties":false,"properties":{"limit":{"type":"integer","minimum":1,"maximum":max_skill_page},"after":{"type":["object","null"],"required":["hook_id","version"],"additionalProperties":false,"properties":{"hook_id":{"type":"string"},"version":{"type":"string"}}}}})
        }
        "hooks_set_enabled" => {
            json!({"type":"object","required":["hook_id","version","enabled"],"additionalProperties":false,"properties":{"hook_id":{"type":"string"},"version":{"type":"string"},"enabled":{"type":"boolean"}}})
        }
        "hooks_set_defaults" => {
            json!({"type":"object","required":["hooks"],"additionalProperties":false,"properties":{"hooks":{"type":"array","maxItems":32,"items":{"type":"object","required":["hook_id","version"],"additionalProperties":false,"properties":{"hook_id":{"type":"string"},"version":{"type":"string"}}}}}})
        }
        "inbox_list" => {
            json!({"type":"object","required":["limit"],"additionalProperties":false,"properties":{"limit":{"type":"integer","minimum":1,"maximum":max_skill_page},"after":{"type":["integer","null"],"minimum":0}}})
        }
        "inbox_ack" => {
            json!({"type":"object","required":["inbox_id"],"additionalProperties":false,"properties":{"inbox_id":{"type":"string"}}})
        }
        "delivery_reconcile" => {
            json!({"type":"object","required":["delivery_id","outcome"],"additionalProperties":false,"properties":{"delivery_id":{"type":"string"},"outcome":{"enum":["confirmed_delivered","confirmed_not_delivered"]}}})
        }
        "runs_deliveries" => {
            json!({"type":"object","required":["run_id","limit"],"additionalProperties":false,"properties":{"run_id":{"type":"string"},"limit":{"type":"integer","minimum":1,"maximum":max_skill_page},"after":{"type":["string","null"]}}})
        }
        "runs_events" => {
            json!({"type":"object","required":["run_id","limit"],"additionalProperties":false,"properties":{"run_id":{"type":"string"},"after":{"type":["integer","null"],"minimum":0},"limit":{"type":"integer","minimum":1}}})
        }
        _ => {
            json!({"type":"object","required":["run_id"],"additionalProperties":false,"properties":{"run_id":{"type":"string"}}})
        }
    }
}
