//! Business Events an SDK connection may emit into the Qefro event bus.

use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct BusinessEventField {
    pub path: String,
    pub label: String,
    #[serde(default, skip_serializing_if = "Option::is_none", rename = "type")]
    pub field_type: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct BusinessEventDefinition {
    pub event_type: String,
    #[serde(default = "default_version")]
    pub version: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub label: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub schema: Option<Value>,
}

fn default_version() -> u32 {
    1
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct EmittedBusinessEvent {
    pub event_type: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub version: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub event_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub customer: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub data: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timestamp: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BusinessEventError {
    Message(String),
}

impl std::fmt::Display for BusinessEventError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BusinessEventError::Message(m) => write!(f, "{m}"),
        }
    }
}

impl std::error::Error for BusinessEventError {}

fn err(msg: impl Into<String>) -> BusinessEventError {
    BusinessEventError::Message(msg.into())
}

pub fn is_business_event_type(name: &str) -> bool {
    let t = name.trim().to_ascii_lowercase();
    if t.contains('(') {
        return false;
    }
    let mut chars = t.chars();
    let Some(first) = chars.next() else {
        return false;
    };
    if !first.is_ascii_lowercase() {
        return false;
    }
    let rest: String = chars.collect();
    if !rest
        .chars()
        .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_' || c == '-' || c == '.')
    {
        return false;
    }
    let parts: Vec<&str> = t.split('.').collect();
    parts.len() >= 2 && parts.iter().all(|p| !p.is_empty())
}

pub fn normalize_business_event(
    def: BusinessEventDefinition,
) -> Result<BusinessEventDefinition, BusinessEventError> {
    let event_type = def.event_type.trim().to_ascii_lowercase();
    if !is_business_event_type(&event_type) {
        return Err(err(format!(
            "businessEvent() requires a Business Event name such as quotation.created (not a capability like createQuotation); got \"{}\"",
            def.event_type
        )));
    }
    let version = if def.version > 0 { def.version } else { 1 };
    let label = def
        .label
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty());
    let description = def
        .description
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty());
    Ok(BusinessEventDefinition {
        event_type,
        version,
        label,
        description,
        schema: def.schema,
    })
}

/// Stable id within a source: `{event_type}:{entity_id}`.
pub fn stable_event_id(event_type: &str, entity_id: &str) -> Result<String, BusinessEventError> {
    let type_ = event_type.trim().to_ascii_lowercase();
    let id = entity_id.trim();
    if id.is_empty() {
        return Err(err("stableEventId requires an entity id"));
    }
    if type_.is_empty() {
        return Ok(id.to_string());
    }
    let prefix = format!("{type_}:");
    if id.starts_with(&prefix) {
        Ok(id.to_string())
    } else {
        Ok(format!("{prefix}{id}"))
    }
}

pub fn normalize_emitted_event(
    event: EmittedBusinessEvent,
    declared: Option<&BusinessEventDefinition>,
) -> Result<EmittedBusinessEvent, BusinessEventError> {
    let event_type = event.event_type.trim().to_ascii_lowercase();
    if !is_business_event_type(&event_type) {
        return Err(err(format!(
            "ctx.emit() requires a Business Event such as quotation.created, not a capability; got \"{}\"",
            event.event_type
        )));
    }
    let version = match event.version {
        Some(v) if v > 0 => v,
        _ => declared.map(|d| d.version).filter(|v| *v > 0).unwrap_or(1),
    };
    let raw_id = event.event_id.as_deref().unwrap_or("").trim().to_string();
    let event_id = if raw_id.is_empty() {
        format!("evt_{}", Uuid::new_v4())
    } else {
        stable_event_id(&event_type, &raw_id)?
    };
    Ok(EmittedBusinessEvent {
        event_type,
        version: Some(version),
        event_id: Some(event_id),
        customer: event.customer,
        data: event.data,
        timestamp: event.timestamp,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stable_ids_are_unique_per_type() {
        assert_eq!(
            stable_event_id("quotation.created", "Q-1001").unwrap(),
            "quotation.created:Q-1001"
        );
        assert_eq!(
            stable_event_id("order.created", "Q-1001").unwrap(),
            "order.created:Q-1001"
        );
        assert_eq!(
            stable_event_id("quotation.created", "quotation.created:Q-1001").unwrap(),
            "quotation.created:Q-1001"
        );
    }

    #[test]
    fn rejects_capability_names() {
        assert!(!is_business_event_type("createQuotation"));
        assert!(is_business_event_type("quotation.created"));
    }
}
