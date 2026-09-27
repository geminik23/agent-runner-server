//! Trusted completion delivery configuration.

use std::collections::HashSet;

use serde::{Deserialize, Serialize};

/// A server-owned destination. Test connectors never target remote hosts.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Destination {
    pub id: String,
    pub revision: String,
    pub kind: DestinationKind,
    #[serde(default)]
    pub endpoint: Option<String>,
    #[serde(default)]
    pub notion_parent: Option<String>,
    #[serde(default)]
    pub deduplication: bool,
    #[serde(default = "enabled")]
    pub enabled: bool,
}

fn enabled() -> bool {
    true
}

/// Only the inbox and explicitly configured test connectors are available.
#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum DestinationKind {
    Inbox,
    WebhookTest,
    NotionMock,
}

impl DestinationKind {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Inbox => "inbox",
            Self::WebhookTest => "webhook_test",
            Self::NotionMock => "notion_mock",
        }
    }
}

/// Bounds are supplied by trusted configuration rather than a client request.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeliveryConfig {
    pub max_pending: usize,
    pub max_payload_bytes: usize,
    pub max_workers: usize,
    pub max_attempts: u32,
    pub delivery_ttl_ms: u64,
    pub retention_ms: u64,
    pub lease_ms: u64,
    pub timeout_ms: u64,
    pub retry_ms: u64,
    pub poll_ms: u64,
    pub destinations: Vec<Destination>,
}

impl DeliveryConfig {
    pub(crate) fn validate(&self, identifier_limit: usize, max_event_bytes: usize) -> bool {
        let current_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .ok()
            .and_then(|duration| i64::try_from(duration.as_millis()).ok());
        if current_ms.is_none_or(|now| {
            [
                self.delivery_ttl_ms,
                self.retention_ms,
                self.lease_ms,
                self.retry_ms,
            ]
            .iter()
            .any(|value| {
                i64::try_from(*value)
                    .ok()
                    .and_then(|value| now.checked_add(value))
                    .is_none()
            })
        }) || [
            self.delivery_ttl_ms,
            self.retention_ms,
            self.lease_ms,
            self.retry_ms,
            self.timeout_ms,
            self.poll_ms,
        ]
        .iter()
        .any(|value| *value > 315_360_000_000)
            || self.max_pending == 0
            || self.max_payload_bytes == 0
            || self.max_workers == 0
            || self.max_workers > self.max_pending
            || self.destinations.len() > 32
            || i64::try_from(self.max_pending).is_err()
            || i64::try_from(self.max_payload_bytes).is_err()
            || self.max_attempts == 0
            || self.delivery_ttl_ms == 0
            || self.retention_ms == 0
            || self.timeout_ms == 0
            || self.lease_ms <= self.timeout_ms
            || self.retry_ms == 0
            || self.poll_ms == 0
            || std::time::Instant::now()
                .checked_add(std::time::Duration::from_millis(self.poll_ms))
                .is_none()
            || std::time::Instant::now()
                .checked_add(std::time::Duration::from_millis(self.timeout_ms))
                .is_none()
            || self.delivery_ttl_ms > i64::MAX as u64
            || self.retention_ms > i64::MAX as u64
            || self.lease_ms > i64::MAX as u64
            || self.retry_ms > i64::MAX as u64
            || self
                .max_pending
                .checked_mul(self.max_payload_bytes)
                .is_none_or(|bytes| i64::try_from(bytes).is_err())
            || max_event_bytes
                .checked_mul(12)
                .and_then(|n| n.checked_add(8192))
                .is_none_or(|bound| self.max_payload_bytes < bound)
        {
            return false;
        }
        let mut ids = HashSet::new();
        self.destinations.iter().all(|d| {
            if d.id.is_empty()
                || d.id.len() > identifier_limit
                || d.revision.is_empty()
                || d.revision.len() > identifier_limit
                || !ids.insert(&d.id)
            {
                return false;
            }
            match d.kind {
                DestinationKind::Inbox => {
                    d.endpoint.is_none() && d.notion_parent.is_none() && !d.deduplication
                }
                DestinationKind::WebhookTest | DestinationKind::NotionMock => {
                    let endpoint = d
                        .endpoint
                        .as_deref()
                        .and_then(|v| v.parse::<reqwest::Url>().ok());
                    let valid = d
                        .endpoint
                        .as_deref()
                        .is_some_and(|value| value.len() <= 4096)
                        && endpoint.as_ref().is_some_and(|url| {
                            url.scheme() == "http"
                                && url.port().is_some()
                                && url.username().is_empty()
                                && url.password().is_none()
                                && url.query().is_none()
                                && url.fragment().is_none()
                                && url
                                    .host_str()
                                    .and_then(|host| {
                                        host.trim_matches(['[', ']'])
                                            .parse::<std::net::IpAddr>()
                                            .ok()
                                    })
                                    .is_some_and(|ip| ip.is_loopback())
                        });
                    valid
                        && match d.kind {
                            DestinationKind::WebhookTest => d.notion_parent.is_none(),
                            DestinationKind::NotionMock => {
                                !d.deduplication
                                    && endpoint
                                        .as_ref()
                                        .is_some_and(|url| url.path() == "/v1/pages")
                                    && d.notion_parent.as_deref().is_some_and(|v| {
                                        !v.is_empty() && v.len() <= identifier_limit
                                    })
                            }
                            DestinationKind::Inbox => false,
                        }
                }
            }
        })
    }

    pub(crate) fn destination(&self, id: &str) -> Option<&Destination> {
        self.destinations.iter().find(|d| d.id == id && d.enabled)
    }
}
