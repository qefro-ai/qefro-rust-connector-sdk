//! Durable idempotency inbox for SDK tool invocations.
//!
//! Provides effectively-once execution semantics by recording incoming request_id
//! in a durable file-based ledger. Replayed requests for completed operations return
//! the cached response without re-invoking downstream handlers.
//! Concurrent invocations return a concurrent conflict status.
//! Abandoned executions exceeding the crash recovery window return UNCERTAIN_OUTCOME
//! to fail closed and prevent unsafe duplicate mutations.

use anyhow::{anyhow, Result};
use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};

/// Default crash recovery threshold window (5 minutes).
pub const DEFAULT_CRASH_WINDOW_SECS: i64 = 300;

/// Default retention TTL (7 days).
pub const DEFAULT_RETENTION_TTL_DAYS: i64 = 7;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InboxStatus {
    Executing,
    Completed,
    #[serde(alias = "failed")]
    FailedRetryable,
    #[serde(alias = "uncertain_crash")]
    UncertainOutcome,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InboxRecord {
    pub request_id: String,
    pub tool: String,
    pub status: InboxStatus,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub response: Option<Value>,
}

#[derive(Debug, Clone)]
pub enum ClaimResult {
    /// Newly claimed by this invocation; handler should proceed.
    Claimed,
    /// Already completed previously; cached response returned.
    Cached(Value),
    /// Another worker is actively executing this request right now.
    ConcurrentExecution,
    /// Previous execution outcome is uncertain (crash or in-flight error).
    UncertainCrash(InboxRecord),
}

/// Explicit error type indicating failure occurred before any mutation began.
/// When thrown/returned, the inbox records `FailedRetryable` instead of `UncertainOutcome`.
#[derive(Debug, Clone)]
pub struct PreMutationError(pub String);

impl PreMutationError {
    pub fn new(msg: impl Into<String>) -> Self {
        Self(msg.into())
    }
}

impl std::fmt::Display for PreMutationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Pre-mutation failure (safe to retry): {}", self.0)
    }
}

impl std::error::Error for PreMutationError {}

pub fn is_pre_mutation_error(err: &anyhow::Error) -> bool {
    err.downcast_ref::<PreMutationError>().is_some()
}

pub fn is_production_env() -> bool {
    for key in ["QEFRO_ENV", "ENVIRONMENT", "ENV", "RUST_ENV"] {
        if let Ok(val) = std::env::var(key) {
            let v = val.trim().to_lowercase();
            if v == "production" || v == "prod" {
                return true;
            }
        }
    }
    false
}

pub fn default_inbox_dir(signing_secret: &str) -> PathBuf {
    if let Ok(dir) = std::env::var("QEFRO_IDEMPOTENCY_DIR") {
        let trimmed = dir.trim();
        if !trimmed.is_empty() {
            return PathBuf::from(trimmed);
        }
    }
    if is_production_env() {
        panic!(
            "Persistent idempotency directory required in production. \
            Specify QEFRO_IDEMPOTENCY_DIR or config.idempotency_dir. \
            Ephemeral temp storage is not permitted in production."
        );
    }
    let digest = Sha256::digest(signing_secret.as_bytes());
    let shard = hex::encode(&digest[..4]); // 8 hex chars
    std::env::temp_dir()
        .join("qefro-idempotency-inbox")
        .join(shard)
}

fn file_name_for(tool: &str, request_id: &str) -> String {
    let safe_tool: String = tool
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '-' || c == '_' || c == '.' {
                c
            } else {
                '_'
            }
        })
        .take(60)
        .collect();
    let safe_req: String = request_id
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '-' || c == '_' || c == '.' {
                c
            } else {
                '_'
            }
        })
        .take(120)
        .collect();
    format!("{safe_tool}__{safe_req}.json")
}

#[derive(Clone)]
pub struct IdempotencyInbox {
    dir: PathBuf,
    crash_window: Duration,
    retention_ttl: Duration,
}

impl IdempotencyInbox {
    pub fn new(dir: impl AsRef<Path>) -> Self {
        let dir = dir.as_ref().to_path_buf();
        let _ = fs::create_dir_all(&dir);
        Self {
            dir,
            crash_window: Duration::seconds(DEFAULT_CRASH_WINDOW_SECS),
            retention_ttl: Duration::days(DEFAULT_RETENTION_TTL_DAYS),
        }
    }

    pub fn with_crash_window_secs(mut self, secs: i64) -> Self {
        self.crash_window = Duration::seconds(secs);
        self
    }

    pub fn with_retention_days(mut self, days: i64) -> Self {
        self.retention_ttl = Duration::days(days);
        self
    }

    pub fn dir(&self) -> &Path {
        &self.dir
    }

    fn record_path(&self, tenant_id: Option<&str>, tool: &str, request_id: &str) -> PathBuf {
        let tenant_folder = tenant_id
            .map(|t| {
                t.chars()
                    .filter(|c| c.is_ascii_alphanumeric() || *c == '-' || *c == '_')
                    .collect::<String>()
            })
            .filter(|s| !s.is_empty())
            .unwrap_or_else(|| "default".to_string());
        self.dir
            .join(tenant_folder)
            .join(file_name_for(tool, request_id))
    }

    /// Try to atomically claim this request_id.
    pub fn claim(
        &self,
        tenant_id: Option<&str>,
        tool: &str,
        request_id: &str,
    ) -> Result<ClaimResult> {
        let path = self.record_path(tenant_id, tool, request_id);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }

        let now = Utc::now();
        let record = InboxRecord {
            request_id: request_id.to_string(),
            tool: tool.to_string(),
            status: InboxStatus::Executing,
            created_at: now,
            updated_at: now,
            expires_at: now + self.retention_ttl,
            response: None,
        };

        let bytes = serde_json::to_vec_pretty(&record)?;

        // Try atomic file creation via O_CREAT | O_EXCL
        match OpenOptions::new().write(true).create_new(true).open(&path) {
            Ok(mut file) => {
                file.write_all(&bytes)?;
                file.sync_all()?;
                Ok(ClaimResult::Claimed)
            }
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {
                // Read existing record
                let raw = fs::read_to_string(&path)
                    .map_err(|err| anyhow!("Failed reading existing idempotency record: {err}"))?;
                let existing: InboxRecord = serde_json::from_str(&raw)
                    .map_err(|err| anyhow!("Corrupt idempotency record at {:?}: {err}", path))?;

                match existing.status {
                    InboxStatus::Completed => {
                        let resp = existing
                            .response
                            .clone()
                            .unwrap_or_else(|| serde_json::json!({"status": "success"}));
                        Ok(ClaimResult::Cached(resp))
                    }
                    InboxStatus::Executing => {
                        let age = now.signed_duration_since(existing.created_at);
                        if age < self.crash_window {
                            Ok(ClaimResult::ConcurrentExecution)
                        } else {
                            // Abandoned / crash recovery window expired -> persist as UncertainOutcome
                            let _ = self.update_status(
                                tenant_id,
                                tool,
                                request_id,
                                InboxStatus::UncertainOutcome,
                                None,
                            );
                            Ok(ClaimResult::UncertainCrash(existing))
                        }
                    }
                    InboxStatus::UncertainOutcome => Ok(ClaimResult::UncertainCrash(existing)),
                    InboxStatus::FailedRetryable => {
                        // Previous attempt definitely failed before mutation; allow retry by taking over the record
                        self.update_status(
                            tenant_id,
                            tool,
                            request_id,
                            InboxStatus::Executing,
                            None,
                        )?;
                        Ok(ClaimResult::Claimed)
                    }
                }
            }
            Err(e) => Err(anyhow!("Failed creating idempotency record: {e}")),
        }
    }

    /// Mark an execution as completed with cached response.
    pub fn complete(
        &self,
        tenant_id: Option<&str>,
        tool: &str,
        request_id: &str,
        response: Value,
    ) -> Result<()> {
        self.update_status(
            tenant_id,
            tool,
            request_id,
            InboxStatus::Completed,
            Some(response),
        )
    }

    /// Mark an execution as failed before any mutation occurred (safe to retry).
    pub fn fail_retryable(
        &self,
        tenant_id: Option<&str>,
        tool: &str,
        request_id: &str,
    ) -> Result<()> {
        self.update_status(
            tenant_id,
            tool,
            request_id,
            InboxStatus::FailedRetryable,
            None,
        )
    }

    /// Mark an execution as having an uncertain outcome (in-flight error / post-start failure).
    /// Prevents duplicate external side-effects on replay.
    pub fn fail_uncertain(
        &self,
        tenant_id: Option<&str>,
        tool: &str,
        request_id: &str,
    ) -> Result<()> {
        self.update_status(
            tenant_id,
            tool,
            request_id,
            InboxStatus::UncertainOutcome,
            None,
        )
    }

    /// Mark an execution as failed (defaults to fail_uncertain to fail closed).
    pub fn fail(&self, tenant_id: Option<&str>, tool: &str, request_id: &str) -> Result<()> {
        self.fail_uncertain(tenant_id, tool, request_id)
    }

    fn update_status(
        &self,
        tenant_id: Option<&str>,
        tool: &str,
        request_id: &str,
        status: InboxStatus,
        response: Option<Value>,
    ) -> Result<()> {
        let path = self.record_path(tenant_id, tool, request_id);
        let mut record = if path.exists() {
            let raw = fs::read_to_string(&path)?;
            serde_json::from_str::<InboxRecord>(&raw).unwrap_or_else(|_| InboxRecord {
                request_id: request_id.to_string(),
                tool: tool.to_string(),
                status: status.clone(),
                created_at: Utc::now(),
                updated_at: Utc::now(),
                expires_at: Utc::now() + self.retention_ttl,
                response: None,
            })
        } else {
            InboxRecord {
                request_id: request_id.to_string(),
                tool: tool.to_string(),
                status: status.clone(),
                created_at: Utc::now(),
                updated_at: Utc::now(),
                expires_at: Utc::now() + self.retention_ttl,
                response: None,
            }
        };

        record.status = status;
        record.updated_at = Utc::now();
        if response.is_some() {
            record.response = response;
        }

        let tmp = path.with_extension("json.tmp");
        let bytes = serde_json::to_vec_pretty(&record)?;
        {
            let mut file = OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(true)
                .open(&tmp)?;
            file.write_all(&bytes)?;
            file.sync_all()?;
        }
        fs::rename(&tmp, &path)?;
        Ok(())
    }

    /// Prune expired records.
    pub fn prune(&self) {
        let now = Utc::now();
        let _ = self.prune_dir(&self.dir, now);
    }

    fn prune_dir(&self, dir: &Path, now: DateTime<Utc>) -> Result<()> {
        let Ok(entries) = fs::read_dir(dir) else {
            return Ok(());
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                let _ = self.prune_dir(&path, now);
            } else if path.extension().and_then(|e| e.to_str()) == Some("json") {
                if let Ok(raw) = fs::read_to_string(&path) {
                    if let Ok(record) = serde_json::from_str::<InboxRecord>(&raw) {
                        if record.expires_at <= now {
                            let _ = fs::remove_file(&path);
                        }
                    }
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_inbox() -> IdempotencyInbox {
        let dir = std::env::temp_dir().join(format!("qefro-test-inbox-{}", uuid::Uuid::new_v4()));
        IdempotencyInbox::new(dir)
    }

    #[test]
    fn test_claim_complete_cached() {
        let inbox = temp_inbox();
        let req_id = "req-101";

        // First claim succeeds
        match inbox
            .claim(Some("tenant-1"), "order_cancel", req_id)
            .unwrap()
        {
            ClaimResult::Claimed => {}
            other => panic!("expected Claimed, got {:?}", other),
        }

        // Concurrent claim while executing returns ConcurrentExecution
        match inbox
            .claim(Some("tenant-1"), "order_cancel", req_id)
            .unwrap()
        {
            ClaimResult::ConcurrentExecution => {}
            other => panic!("expected ConcurrentExecution, got {:?}", other),
        }

        // Complete with response
        let resp = serde_json::json!({"order_id": "ORD-1", "canceled": true});
        inbox
            .complete(Some("tenant-1"), "order_cancel", req_id, resp.clone())
            .unwrap();

        // Subsequent claim returns Cached
        match inbox
            .claim(Some("tenant-1"), "order_cancel", req_id)
            .unwrap()
        {
            ClaimResult::Cached(cached) => {
                assert_eq!(cached, resp);
            }
            other => panic!("expected Cached, got {:?}", other),
        }

        // Cleanup
        let _ = fs::remove_dir_all(inbox.dir());
    }

    #[test]
    fn test_crash_window_uncertain_outcome() {
        let inbox = temp_inbox().with_crash_window_secs(0); // 0s crash window for immediate timeout
        let req_id = "req-crashed";

        // First claim
        inbox
            .claim(Some("tenant-1"), "charge_card", req_id)
            .unwrap();

        // Small sleep so duration > 0s
        std::thread::sleep(std::time::Duration::from_millis(10));

        // Re-claiming after crash window returns UncertainCrash
        match inbox
            .claim(Some("tenant-1"), "charge_card", req_id)
            .unwrap()
        {
            ClaimResult::UncertainCrash(record) => {
                assert_eq!(record.request_id, req_id);
                assert_eq!(record.tool, "charge_card");
            }
            other => panic!("expected UncertainCrash, got {:?}", other),
        }

        let _ = fs::remove_dir_all(inbox.dir());
    }

    #[test]
    fn test_tenant_sharding_isolation() {
        let inbox = temp_inbox();
        let req_id = "req-shared-id";

        // Tenant A claims
        inbox.claim(Some("tenant-a"), "action_a", req_id).unwrap();
        inbox
            .complete(
                Some("tenant-a"),
                "action_a",
                req_id,
                serde_json::json!({"t": "a"}),
            )
            .unwrap();

        // Tenant B with same request_id does NOT collide with Tenant A
        match inbox.claim(Some("tenant-b"), "action_b", req_id).unwrap() {
            ClaimResult::Claimed => {}
            other => panic!("expected Claimed for tenant-b, got {:?}", other),
        }

        let _ = fs::remove_dir_all(inbox.dir());
    }

    #[test]
    fn test_fail_retryable_allows_reclaim() {
        let inbox = temp_inbox();
        let req_id = "req-retryable-1";

        // Claim succeeds
        assert!(matches!(
            inbox
                .claim(Some("tenant-1"), "order_cancel", req_id)
                .unwrap(),
            ClaimResult::Claimed
        ));

        // Mark as failed_retryable (e.g. before_hooks failed)
        inbox
            .fail_retryable(Some("tenant-1"), "order_cancel", req_id)
            .unwrap();

        // Re-claiming succeeds because failure was verified pre-mutation
        assert!(matches!(
            inbox
                .claim(Some("tenant-1"), "order_cancel", req_id)
                .unwrap(),
            ClaimResult::Claimed
        ));

        let _ = fs::remove_dir_all(inbox.dir());
    }

    #[test]
    fn test_fail_uncertain_blocks_reclaim() {
        let inbox = temp_inbox();
        let req_id = "req-uncertain-1";

        // Claim succeeds
        assert!(matches!(
            inbox
                .claim(Some("tenant-1"), "order_cancel", req_id)
                .unwrap(),
            ClaimResult::Claimed
        ));

        // Mark as fail_uncertain (in-flight error / unhandled handler failure)
        inbox
            .fail_uncertain(Some("tenant-1"), "order_cancel", req_id)
            .unwrap();

        // Re-claiming fails closed with UncertainCrash to prevent duplicate mutation
        assert!(matches!(
            inbox
                .claim(Some("tenant-1"), "order_cancel", req_id)
                .unwrap(),
            ClaimResult::UncertainCrash(_)
        ));

        let _ = fs::remove_dir_all(inbox.dir());
    }

    #[test]
    fn test_production_env_requires_persistent_dir() {
        struct EnvGuard;
        impl Drop for EnvGuard {
            fn drop(&mut self) {
                std::env::remove_var("QEFRO_ENV");
            }
        }
        let _guard = EnvGuard;
        std::env::set_var("QEFRO_ENV", "production");
        std::env::remove_var("QEFRO_IDEMPOTENCY_DIR");
        let result = std::panic::catch_unwind(|| {
            default_inbox_dir("secret");
        });
        assert!(
            result.is_err(),
            "default_inbox_dir must panic in production without persistent dir!"
        );
    }
}
