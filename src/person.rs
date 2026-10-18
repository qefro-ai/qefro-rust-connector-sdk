//! Customer Hub Person — Qefro first-party memory.
//!
//! Distinct from connector customer auth:
//! `ctx.customer` → external systems, `ctx.person` → Qefro memory.

use anyhow::{anyhow, Result};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::sync::{Arc, Mutex};

use crate::FlowTrigger;

pub const PERSON_CREATED: &str = "person.created";
pub const PERSON_UPDATED: &str = "person.updated";
pub const PERSON_STATUS_CHANGED: &str = "person.status.changed";
pub const PERSON_MERGED: &str = "person.merged";
pub const PERSON_ASSIGNED: &str = "person.assigned";
pub const PERSON_TAG_CREATED: &str = "person.tag.created";
pub const PERSON_ACTIVITY_CREATED: &str = "person.activity.created";

pub fn person_event_trigger(event: impl Into<String>, when: Option<String>) -> FlowTrigger {
    FlowTrigger::Event {
        event: event.into(),
        when,
    }
}

pub fn on_person_created(when: Option<String>) -> FlowTrigger {
    person_event_trigger(PERSON_CREATED, when)
}

pub fn on_person_updated(when: Option<String>) -> FlowTrigger {
    person_event_trigger(PERSON_UPDATED, when)
}

pub fn on_person_status_changed(when: Option<String>) -> FlowTrigger {
    person_event_trigger(PERSON_STATUS_CHANGED, when)
}

pub fn on_person_merged(when: Option<String>) -> FlowTrigger {
    person_event_trigger(PERSON_MERGED, when)
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum PersonMutation {
    Note {
        content: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        author_id: Option<String>,
    },
    Tag {
        name: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        color: Option<String>,
    },
    Activity {
        activity_type: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        source: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        payload: Option<Value>,
    },
    Assign {
        to: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        handoff: Option<bool>,
    },
    Merge {
        into: String,
    },
}

#[derive(Clone)]
pub struct PersonContext {
    current: Arc<Mutex<Option<Value>>>,
    mutations: Arc<Mutex<Vec<PersonMutation>>>,
}

impl PersonContext {
    pub fn new(snapshot: Option<&Value>, mutations: Arc<Mutex<Vec<PersonMutation>>>) -> Self {
        let current = snapshot
            .and_then(|v| v.as_object())
            .and_then(|o| o.get("id"))
            .and_then(|v| v.as_str())
            .filter(|s| !s.is_empty())
            .and_then(|_| snapshot.cloned());
        Self {
            current: Arc::new(Mutex::new(current)),
            mutations,
        }
    }

    fn require_id(&self) -> Result<String> {
        let current = self.current.lock().expect("person");
        current
            .as_ref()
            .and_then(|v| v.get("id"))
            .and_then(|v| v.as_str())
            .filter(|s| !s.is_empty())
            .map(str::to_string)
            .ok_or_else(|| anyhow!("person_not_found"))
    }

    pub fn get(&self) -> Option<Value> {
        self.current.lock().expect("person").clone()
    }

    pub fn require(&self) -> Result<Value> {
        self.get().ok_or_else(|| anyhow!("person_not_found"))
    }

    pub fn note(&self, content: &str, author_id: Option<String>) -> Result<()> {
        self.require_id()?;
        let trimmed = content.trim();
        if trimmed.is_empty() {
            return Err(anyhow!("person_note_empty"));
        }
        self.mutations
            .lock()
            .expect("mutations")
            .push(PersonMutation::Note {
                content: trimmed.to_string(),
                author_id,
            });
        Ok(())
    }

    pub fn tag(&self, name: &str, color: Option<String>) -> Result<()> {
        self.require_id()?;
        let trimmed = name.trim();
        if trimmed.is_empty() {
            return Err(anyhow!("person_tag_empty"));
        }
        self.mutations
            .lock()
            .expect("mutations")
            .push(PersonMutation::Tag {
                name: trimmed.to_string(),
                color: color.clone(),
            });
        let mut current = self.current.lock().expect("person");
        if let Some(Value::Object(obj)) = current.as_mut() {
            let mut tags = obj
                .get("tags")
                .and_then(|v| v.as_array())
                .cloned()
                .unwrap_or_default();
            tags.push(serde_json::json!({ "name": trimmed, "color": color }));
            obj.insert("tags".into(), Value::Array(tags));
        }
        Ok(())
    }

    pub fn activity(
        &self,
        activity_type: &str,
        source: Option<String>,
        payload: Option<Value>,
    ) -> Result<()> {
        self.require_id()?;
        let trimmed = activity_type.trim();
        if trimmed.is_empty() {
            return Err(anyhow!("person_activity_empty"));
        }
        self.mutations
            .lock()
            .expect("mutations")
            .push(PersonMutation::Activity {
                activity_type: trimmed.to_string(),
                source: Some(source.unwrap_or_else(|| "sdk".into())),
                payload,
            });
        Ok(())
    }

    pub fn assign(&self, to: &str, handoff: Option<bool>) -> Result<()> {
        self.require_id()?;
        let trimmed = to.trim();
        if trimmed.is_empty() {
            return Err(anyhow!("person_assign_empty"));
        }
        self.mutations
            .lock()
            .expect("mutations")
            .push(PersonMutation::Assign {
                to: trimmed.to_string(),
                handoff,
            });
        Ok(())
    }

    pub fn merge(&self, into_person_id: &str) -> Result<()> {
        self.require_id()?;
        let into = into_person_id.trim();
        if into.is_empty() {
            return Err(anyhow!("person_merge_target_empty"));
        }
        self.mutations
            .lock()
            .expect("mutations")
            .push(PersonMutation::Merge {
                into: into.to_string(),
            });
        Ok(())
    }
}
