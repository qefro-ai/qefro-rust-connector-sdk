//! Organization capability registration (ADR-005 Phase 1).
//!
//! Apps register metadata only — events, actions, tasks. The platform owns the
//! capability graph, workflows, and inbox. Templates are organization-owned (Phase 4).

use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashSet;

/// Error from [`validate_organization_definition`] / [`Qefro::organization`](crate::Qefro::organization).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OrganizationError {
    Message(String),
}

impl std::fmt::Display for OrganizationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            OrganizationError::Message(m) => write!(f, "{m}"),
        }
    }
}

impl std::error::Error for OrganizationError {}

fn err(msg: impl Into<String>) -> OrganizationError {
    OrganizationError::Message(msg.into())
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct OrganizationEvent {
    pub id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub label: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none", rename = "payloadSchema")]
    pub payload_schema: Option<Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct OrganizationAction {
    pub id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub label: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none", rename = "inputSchema")]
    pub input_schema: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none", rename = "outputSchema")]
    pub output_schema: Option<Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct OrganizationTaskType {
    pub id: String,
    pub label: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        rename = "suggested_workspace_type"
    )]
    pub suggested_workspace_type: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none", rename = "suggested_team")]
    pub suggested_team: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub priority: Option<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
pub struct OrganizationDefinition {
    #[serde(default)]
    pub version: Option<i64>,
    #[serde(default)]
    pub events: Option<Vec<OrganizationEvent>>,
    #[serde(default)]
    pub actions: Option<Vec<OrganizationAction>>,
    #[serde(default)]
    pub tasks: Option<Vec<OrganizationTaskType>>,
    /// Rejected in Phase 1 — templates are organization-owned (Phase 4).
    #[serde(default)]
    pub templates: Option<Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct OrganizationCapabilities {
    pub version: i64,
    pub events: Vec<OrganizationEvent>,
    pub actions: Vec<OrganizationAction>,
    pub tasks: Vec<OrganizationTaskType>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct OrganizationMetadata {
    pub events: Vec<OrganizationEvent>,
    pub actions: Vec<OrganizationAction>,
    pub tasks: Vec<OrganizationTaskType>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct OrganizationCapability {
    pub version: i64,
    pub metadata: OrganizationMetadata,
}

fn require_non_empty(value: &str, path: &str) -> Result<String, OrganizationError> {
    let t = value.trim();
    if t.is_empty() {
        return Err(err(format!("organization: {path} must be a non-empty string")));
    }
    Ok(t.to_string())
}

fn assert_opaque(id: &str, path: &str) -> Result<(), OrganizationError> {
    if id.contains('.') {
        return Err(err(format!(
            "organization: {path} must be an opaque capability id (no '.' / app prefix); got \"{id}\""
        )));
    }
    Ok(())
}

fn assert_unique(ids: &[String], kind: &str) -> Result<(), OrganizationError> {
    let mut seen = HashSet::new();
    for id in ids {
        if !seen.insert(id.clone()) {
            return Err(err(format!("organization: duplicate {kind} id \"{id}\"")));
        }
    }
    Ok(())
}

/// Validate and normalize an organization definition.
pub fn validate_organization_definition(
    def: OrganizationDefinition,
) -> Result<OrganizationCapabilities, OrganizationError> {
    if def.templates.is_some() {
        return Err(err(
            "organization: templates are organization-owned assets (Phase 4); do not publish from apps",
        ));
    }

    let version = match def.version {
        None => 1,
        Some(v) if v >= 1 => v,
        Some(_) => return Err(err("organization: version must be a positive integer")),
    };

    let mut events = def.events.unwrap_or_default();
    let mut actions = def.actions.unwrap_or_default();
    let mut tasks = def.tasks.unwrap_or_default();

    for (i, e) in events.iter_mut().enumerate() {
        e.id = require_non_empty(&e.id, &format!("events[{i}].id"))?;
        assert_opaque(&e.id, &format!("events[{i}].id"))?;
        if let Some(ref label) = e.label {
            let trimmed = label.trim();
            e.label = if trimmed.is_empty() {
                None
            } else {
                Some(trimmed.to_string())
            };
        }
    }
    for (i, a) in actions.iter_mut().enumerate() {
        a.id = require_non_empty(&a.id, &format!("actions[{i}].id"))?;
        assert_opaque(&a.id, &format!("actions[{i}].id"))?;
        if let Some(ref label) = a.label {
            let trimmed = label.trim();
            a.label = if trimmed.is_empty() {
                None
            } else {
                Some(trimmed.to_string())
            };
        }
    }
    for (i, t) in tasks.iter_mut().enumerate() {
        t.id = require_non_empty(&t.id, &format!("tasks[{i}].id"))?;
        assert_opaque(&t.id, &format!("tasks[{i}].id"))?;
        t.label = require_non_empty(&t.label, &format!("tasks[{i}].label"))?;
    }

    assert_unique(
        &events.iter().map(|e| e.id.clone()).collect::<Vec<_>>(),
        "event",
    )?;
    assert_unique(
        &actions.iter().map(|a| a.id.clone()).collect::<Vec<_>>(),
        "action",
    )?;
    assert_unique(
        &tasks.iter().map(|t| t.id.clone()).collect::<Vec<_>>(),
        "task",
    )?;

    let mut all = Vec::new();
    all.extend(events.iter().map(|e| e.id.clone()));
    all.extend(actions.iter().map(|a| a.id.clone()));
    all.extend(tasks.iter().map(|t| t.id.clone()));
    assert_unique(&all, "capability")?;

    Ok(OrganizationCapabilities {
        version,
        events,
        actions,
        tasks,
    })
}

/// Build the capabilities.list.organization wire object.
pub fn to_organization_capability(caps: &OrganizationCapabilities) -> OrganizationCapability {
    OrganizationCapability {
        version: caps.version,
        metadata: OrganizationMetadata {
            events: caps.events.clone(),
            actions: caps.actions.clone(),
            tasks: caps.tasks.clone(),
        },
    }
}

pub fn is_organization_enabled() -> bool {
    crate::customer_hub::env_flag_true("QEFRO_ORGANIZATION_ENABLED", false)
}

/// Thin get-own-capabilities client via `platform.organization`. Registry read only.
#[derive(Clone, Default)]
pub struct OrganizationContext {
    platform: Option<crate::customer_hub::PlatformCapabilities>,
}

impl OrganizationContext {
    pub fn new(platform: Option<crate::customer_hub::PlatformCapabilities>) -> Self {
        Self { platform }
    }

    pub async fn get_capabilities(&self) -> anyhow::Result<Option<Value>> {
        if !is_organization_enabled() {
            return Ok(None);
        }
        let binding = self.platform.as_ref().and_then(|p| p.organization.as_ref());
        let from_env = std::env::var("QEFRO_ORGANIZATION_URL")
            .ok()
            .map(|s| s.trim_end_matches('/').to_string());
        let base_url = binding
            .and_then(|b| b.base_url.clone())
            .or(from_env)
            .unwrap_or_default()
            .trim_end_matches('/')
            .to_string();
        let context = binding.and_then(|b| b.context.as_ref());
        if base_url.is_empty() || context.is_none() {
            return Ok(None);
        }
        let context = context.unwrap();
        if context.tenant_id.is_empty() || context.workspace_id.is_empty() {
            return Ok(None);
        }
        let solution_id = match &context.solution_id {
            Some(id) if !id.is_empty() => id.clone(),
            _ => return Ok(None),
        };
        let token = binding
            .and_then(|b| b.token.clone())
            .or_else(|| std::env::var("QEFRO_SERVICE_TOKEN").ok())
            .or_else(|| std::env::var("QEFRO_INTERNAL_TOKEN").ok())
            .or_else(|| std::env::var("QEFRO_INTERNAL_BEARER").ok())
            .unwrap_or_default();
        let encoded: String = solution_id
            .bytes()
            .map(|b| match b {
                b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                    (b as char).to_string()
                }
                _ => format!("%{b:02X}"),
            })
            .collect();
        let url = format!("{base_url}/v1/internal/organization/capabilities/{encoded}");
        let mut req = reqwest::Client::new()
            .get(&url)
            .header("content-type", "application/json")
            .header("x-qefro-tenant-id", &context.tenant_id)
            .header("x-qefro-workspace-id", &context.workspace_id);
        if let Some(ref installation_id) = context.installation_id {
            if !installation_id.is_empty() {
                req = req.header("x-qefro-installation-id", installation_id);
            }
        }
        if !token.is_empty() {
            req = req.header("authorization", format!("Bearer {token}"));
        }
        let res = req.send().await?;
        let status = res.status();
        let text = res.text().await.unwrap_or_default();
        if status.as_u16() == 404 {
            return Ok(None);
        }
        if !status.is_success() {
            return Err(anyhow::anyhow!(
                "organization.getCapabilities failed ({status}): {text}"
            ));
        }
        if text.is_empty() {
            return Ok(None);
        }
        Ok(serde_json::from_str(&text).ok())
    }
}

pub fn build_organization_context(
    platform: Option<crate::customer_hub::PlatformCapabilities>,
) -> OrganizationContext {
    OrganizationContext::new(platform)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepts_valid_definition() {
        let caps = validate_organization_definition(OrganizationDefinition {
            version: Some(1),
            events: Some(vec![OrganizationEvent {
                id: "purchase_requested".into(),
                label: Some("Purchase requested".into()),
                description: None,
                payload_schema: None,
            }]),
            actions: Some(vec![OrganizationAction {
                id: "approve_purchase".into(),
                label: Some("Approve".into()),
                description: None,
                input_schema: None,
                output_schema: None,
            }]),
            tasks: Some(vec![OrganizationTaskType {
                id: "purchase_approval".into(),
                label: "Purchase approval".into(),
                description: None,
                suggested_workspace_type: Some("finance".into()),
                suggested_team: None,
                priority: Some("high".into()),
            }]),
            templates: None,
        })
        .unwrap();
        assert_eq!(caps.events[0].id, "purchase_requested");
        let wire = to_organization_capability(&caps);
        assert_eq!(wire.metadata.actions[0].id, "approve_purchase");
    }

    #[test]
    fn rejects_prefixed_ids() {
        let err = validate_organization_definition(OrganizationDefinition {
            events: Some(vec![OrganizationEvent {
                id: "restaurant.purchase_requested".into(),
                label: None,
                description: None,
                payload_schema: None,
            }]),
            ..Default::default()
        })
        .unwrap_err();
        assert!(err.to_string().contains("opaque capability id"));
    }

    #[test]
    fn rejects_templates() {
        let err = validate_organization_definition(OrganizationDefinition {
            templates: Some(serde_json::json!([{"id": "x"}])),
            ..Default::default()
        })
        .unwrap_err();
        assert!(err.to_string().contains("templates are organization-owned"));
    }

    #[tokio::test]
    async fn organization_context_disabled_without_flag() {
        assert!(!is_organization_enabled());
        let ctx = build_organization_context(None);
        assert!(ctx.get_capabilities().await.unwrap().is_none());
    }
}
