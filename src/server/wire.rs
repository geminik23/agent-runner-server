use std::fmt;

use axum::http::StatusCode;
use base64::{Engine, engine::general_purpose::STANDARD};
use serde::{
    Deserialize,
    de::{self, MapAccess, SeqAccess, Visitor},
};
use serde_json::{Map, Value, json};

use crate::{
    access::{AccessError, AuthenticatedClient, Scope},
    application::{CancelOutcome, RunHandle, ServiceError},
    core::ExecutionMetadata,
    storage::{ResultView, StorageError},
};

pub(crate) const TOOLS: [&str; 12] = [
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
            StorageError::IdempotencyConflict => {
                Self::new(StatusCode::CONFLICT, "idempotency_conflict")
            }
            StorageError::SessionBusy => Self::new(StatusCode::CONFLICT, "session_busy"),
            StorageError::SkillVersionConflict => {
                Self::new(StatusCode::CONFLICT, "skill_version_conflict")
            }
            StorageError::SkillBundleConflict => {
                Self::new(StatusCode::CONFLICT, "skill_bundle_conflict")
            }
            StorageError::SkillUnavailable => Self::new(StatusCode::CONFLICT, "skill_unavailable"),
            StorageError::SkillUseForbidden => Self::new(StatusCode::FORBIDDEN, "forbidden"),
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
        _ => Err(ApiError::new(StatusCode::NOT_FOUND, "unknown_operation")),
    }
}

/// Detect duplicate keys before an SDK can collapse objects into a map.
pub(crate) fn parse_json(bytes: &[u8]) -> Result<Value, ApiError> {
    serde_json::from_slice::<UniqueValue>(bytes)
        .map(|v| v.0)
        .map_err(|_| ApiError::invalid())
}
struct UniqueValue(Value);
impl<'de> Deserialize<'de> for UniqueValue {
    fn deserialize<D: de::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        struct UniqueVisitor;
        impl<'de> Visitor<'de> for UniqueVisitor {
            type Value = UniqueValue;
            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("JSON without duplicate keys")
            }
            fn visit_map<A: MapAccess<'de>>(self, mut a: A) -> Result<Self::Value, A::Error> {
                let mut m = Map::new();
                while let Some(key) = a.next_key::<String>()? {
                    if m.contains_key(&key) {
                        return Err(de::Error::custom("duplicate key"));
                    }
                    m.insert(key, a.next_value::<UniqueValue>()?.0);
                }
                Ok(UniqueValue(Value::Object(m)))
            }
            fn visit_seq<A: SeqAccess<'de>>(self, mut a: A) -> Result<Self::Value, A::Error> {
                let mut v = Vec::new();
                while let Some(e) = a.next_element::<UniqueValue>()? {
                    v.push(e.0);
                }
                Ok(UniqueValue(Value::Array(v)))
            }
            fn visit_str<E: de::Error>(self, v: &str) -> Result<Self::Value, E> {
                Ok(UniqueValue(Value::String(v.into())))
            }
            fn visit_string<E: de::Error>(self, v: String) -> Result<Self::Value, E> {
                Ok(UniqueValue(Value::String(v)))
            }
            fn visit_bool<E: de::Error>(self, v: bool) -> Result<Self::Value, E> {
                Ok(UniqueValue(Value::Bool(v)))
            }
            fn visit_i64<E: de::Error>(self, v: i64) -> Result<Self::Value, E> {
                Ok(UniqueValue(v.into()))
            }
            fn visit_u64<E: de::Error>(self, v: u64) -> Result<Self::Value, E> {
                Ok(UniqueValue(v.into()))
            }
            fn visit_f64<E: de::Error>(self, v: f64) -> Result<Self::Value, E> {
                serde_json::Number::from_f64(v)
                    .map(|n| UniqueValue(Value::Number(n)))
                    .ok_or_else(|| de::Error::custom("invalid number"))
            }
            fn visit_unit<E: de::Error>(self) -> Result<Self::Value, E> {
                Ok(UniqueValue(Value::Null))
            }
        }
        d.deserialize_any(UniqueVisitor)
    }
}

pub(crate) fn schema(name: &str, max_skill_page: usize) -> Value {
    match name {
        "agents_list" => json!({"type":"object", "additionalProperties":false}),
        "agents_start" => {
            json!({"type":"object","required":["profile_id","workspace_id","input","idempotency_key"],"additionalProperties":false,"properties":{
                "profile_id":{"type":"string"},"workspace_id":{"type":"string"},"idempotency_key":{"type":"string"},
                "skills":{"description":"When omitted, use defaults. When present, replace defaults with exactly this ordered list; an empty list selects no skills. Nonempty selection requires SkillRead.","type":"array","maxItems":32,"items":{"type":"object","required":["skill_id","version"],"additionalProperties":false,"properties":{"skill_id":{"type":"string"},"version":{"type":"string"}}}},
                "input":{"type":"object","required":["request"],"additionalProperties":false,"properties":{"request":{"type":"string"}}},
                "session":{"oneOf":[{"type":"object","required":["mode"],"additionalProperties":false,"properties":{"mode":{"const":"new"}}},{"type":"object","required":["mode","session_id"],"additionalProperties":false,"properties":{"mode":{"const":"resume"},"session_id":{"type":"string"}}}]}
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
        "runs_events" => {
            json!({"type":"object","required":["run_id","limit"],"additionalProperties":false,"properties":{"run_id":{"type":"string"},"after":{"type":["integer","null"],"minimum":0},"limit":{"type":"integer","minimum":1}}})
        }
        _ => {
            json!({"type":"object","required":["run_id"],"additionalProperties":false,"properties":{"run_id":{"type":"string"}}})
        }
    }
}
