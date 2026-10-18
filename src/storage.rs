//! Managed document storage via storage-service (ADR-002). Used only from app tools.

use anyhow::{anyhow, Result};
use serde_json::{json, Value};

use crate::customer_hub::PlatformCapabilities;

#[derive(Clone, Default)]
pub struct StorageContext {
    platform: Option<PlatformCapabilities>,
}

impl StorageContext {
    pub fn new(platform: Option<PlatformCapabilities>) -> Self {
        Self { platform }
    }

    fn endpoint(&self) -> Result<(String, String, Value)> {
        let storage = self.platform.as_ref().and_then(|p| p.storage.as_ref());
        let from_env = std::env::var("QEFRO_STORAGE_URL")
            .ok()
            .map(|s| s.trim_end_matches('/').to_string());
        let base_url = storage
            .and_then(|s| s.base_url.clone())
            .or(from_env)
            .unwrap_or_default()
            .trim_end_matches('/')
            .to_string();
        if base_url.is_empty() {
            return Err(anyhow!(
                "ctx.storage requires platform.storage.base_url or QEFRO_STORAGE_URL"
            ));
        }
        let context = storage.and_then(|s| s.context.clone()).ok_or_else(|| {
            anyhow!("ctx.storage requires platform.storage.context on tool.invoke")
        })?;
        let token = storage
            .and_then(|s| s.token.clone())
            .or_else(|| std::env::var("QEFRO_SERVICE_TOKEN").ok())
            .or_else(|| std::env::var("QEFRO_INTERNAL_TOKEN").ok())
            .unwrap_or_default();
        Ok((base_url, token, serde_json::to_value(context)?))
    }

    async fn call(&self, op: &str, mut body: Value) -> Result<Value> {
        let (base_url, token, context) = self.endpoint()?;
        if let Some(obj) = body.as_object_mut() {
            obj.insert("context".into(), context);
        }
        let mut req = reqwest::Client::new()
            .post(format!("{base_url}/v1/internal/storage/{op}"))
            .header("content-type", "application/json")
            .json(&body);
        if !token.is_empty() {
            req = req.header("authorization", format!("Bearer {token}"));
        }
        let res = req.send().await?;
        let status = res.status();
        let text = res.text().await.unwrap_or_default();
        if !status.is_success() {
            return Err(anyhow!("storage.{op} failed ({status}): {text}"));
        }
        if text.is_empty() {
            return Ok(json!({}));
        }
        Ok(serde_json::from_str(&text).unwrap_or_else(|_| json!({})))
    }

    pub async fn insert(
        &self,
        collection: &str,
        document: Value,
        allocate_code: Option<Value>,
    ) -> Result<Value> {
        let mut body = json!({ "collection": collection, "document": document });
        if let Some(code) = allocate_code {
            body["allocate_code"] = code;
        }
        self.call("insert", body).await
    }

    pub async fn find(
        &self,
        collection: &str,
        filter: Option<Value>,
        limit: Option<u64>,
        sort: Option<Value>,
    ) -> Result<Value> {
        let mut body = json!({
            "collection": collection,
            "filter": filter.unwrap_or_else(|| json!({})),
        });
        if let Some(limit) = limit {
            body["limit"] = json!(limit);
        }
        if let Some(sort) = sort {
            body["sort"] = sort;
        }
        let out = self.call("find", body).await?;
        let items = out
            .get("items")
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default();
        let total = out
            .get("total")
            .and_then(|v| v.as_u64())
            .unwrap_or(items.len() as u64);
        Ok(json!({ "items": items, "total": total }))
    }

    pub async fn get(&self, collection: &str, id: &str) -> Result<Value> {
        self.call("get", json!({ "collection": collection, "id": id }))
            .await
    }

    pub async fn update(&self, collection: &str, id: &str, patch: Value) -> Result<Value> {
        self.call(
            "update",
            json!({ "collection": collection, "id": id, "patch": patch }),
        )
        .await
    }

    pub async fn delete(&self, collection: &str, id: &str) -> Result<Value> {
        self.call("delete", json!({ "collection": collection, "id": id }))
            .await
    }
}

pub fn build_storage_context(platform: Option<PlatformCapabilities>) -> StorageContext {
    StorageContext::new(platform)
}
