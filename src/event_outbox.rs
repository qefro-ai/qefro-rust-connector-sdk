//! Durable local outbox for Business Events (`ctx.emit()` = persist, then deliver).

use crate::business_events::EmittedBusinessEvent;
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::time::SystemTime;

const MAX_FILES: usize = 200;

pub fn default_outbox_dir(signing_secret: &str) -> PathBuf {
    let digest = Sha256::digest(signing_secret.as_bytes());
    let shard = hex::encode(&digest[..6]);
    std::env::temp_dir().join("qefro-event-outbox").join(shard)
}

fn file_name_for(event_id: &str) -> String {
    let safe: String = event_id
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '-' || c == '_' || c == '.' {
                c
            } else {
                '_'
            }
        })
        .take(180)
        .collect();
    format!("{safe}.json")
}

#[derive(Clone)]
pub struct EventOutbox {
    dir: PathBuf,
}

impl EventOutbox {
    pub fn new(dir: impl AsRef<Path>) -> Self {
        let dir = dir.as_ref().to_path_buf();
        let _ = fs::create_dir_all(&dir);
        Self { dir }
    }

    pub fn put(&self, event: EmittedBusinessEvent) -> anyhow::Result<EmittedBusinessEvent> {
        let event_id = event
            .event_id
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .ok_or_else(|| anyhow::anyhow!("outbox.put requires event_id"))?;
        fs::create_dir_all(&self.dir)?;
        let dest = self.dir.join(file_name_for(event_id));
        let tmp = dest.with_extension("json.tmp");
        let mut body = serde_json::to_value(&event)?;
        if let Some(obj) = body.as_object_mut() {
            obj.insert(
                "queued_at".into(),
                Value::String(chrono::Utc::now().to_rfc3339()),
            );
        }
        let bytes = serde_json::to_vec(&body)?;
        {
            let mut file = fs::File::create(&tmp)?;
            file.write_all(&bytes)?;
            file.sync_all()?;
        }
        fs::rename(&tmp, &dest)?;
        self.prune();
        Ok(event)
    }

    pub fn pending(&self) -> Vec<EmittedBusinessEvent> {
        let mut out = Vec::new();
        let Ok(entries) = fs::read_dir(&self.dir) else {
            return out;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.extension().and_then(|e| e.to_str()) != Some("json") {
                continue;
            }
            let Ok(raw) = fs::read_to_string(&path) else {
                continue;
            };
            let Ok(mut value) = serde_json::from_str::<Value>(&raw) else {
                continue;
            };
            if let Some(obj) = value.as_object_mut() {
                obj.remove("queued_at");
            }
            if let Ok(event) = serde_json::from_value::<EmittedBusinessEvent>(value) {
                if !event.event_type.is_empty() {
                    out.push(event);
                }
            }
        }
        out
    }

    pub fn ack(&self, event_id: &str) {
        let id = event_id.trim();
        if id.is_empty() {
            return;
        }
        let _ = fs::remove_file(self.dir.join(file_name_for(id)));
    }

    fn prune(&self) {
        let Ok(entries) = fs::read_dir(&self.dir) else {
            return;
        };
        let mut files: Vec<(SystemTime, PathBuf)> = entries
            .flatten()
            .map(|e| e.path())
            .filter(|p| p.extension().and_then(|e| e.to_str()) == Some("json"))
            .filter_map(|p| {
                let mtime = fs::metadata(&p).ok()?.modified().ok()?;
                Some((mtime, p))
            })
            .collect();
        if files.len() <= MAX_FILES {
            return;
        }
        files.sort_by_key(|(t, _)| *t);
        let drop_n = files.len() - MAX_FILES;
        for (_, path) in files.into_iter().take(drop_n) {
            let _ = fs::remove_file(path);
        }
    }
}
