use serde::{Deserialize, Serialize};
use std::time::Duration;

use crate::models::Result;

/// JSON request/response types (matching server)
#[derive(Debug, Serialize, Deserialize)]
pub struct SetRequest {
    pub key: String,
    pub value: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct GetResponse {
    pub key: String,
    pub value: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct DeleteResponse {
    pub key: String,
    pub existed: bool,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct MessageResponse {
    pub message: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct BatchSetRequest {
    pub items: Vec<SetRequest>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct BatchGetRequest {
    pub keys: Vec<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct BatchGetResponse {
    pub items: Vec<GetResponse>,
}

/// Async HTTP client for KVS
pub struct AsyncKvsClient {
    base_url: String,
    client: reqwest::Client,
}

impl AsyncKvsClient {
    pub fn new(host: String, port: u32, timeout: Duration) -> Result<Self> {
        let base_url = format!("http://{}:{}", host, port);
        
        let client = reqwest::Client::builder()
            .timeout(timeout)
            .build()
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;

        Ok(AsyncKvsClient { base_url, client })
    }

    pub async fn health_check(&self) -> Result<bool> {
        let url = format!("{}/health", self.base_url);
        
        match self.client.get(&url).send().await {
            Ok(response) => Ok(response.status().is_success()),
            Err(_) => Ok(false),
        }
    }

    pub async fn set(&self, key: String, value: String) -> Result<()> {
        let url = format!("{}/api/keys/{}", self.base_url, key);
        
        let response = self
            .client
            .put(&url)
            .json(&serde_json::json!({ "value": value }))
            .send()
            .await
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;

        if !response.status().is_success() {
            let status = response.status();
            let error_text = response
                .text()
                .await
                .unwrap_or_else(|_| "Unknown error".to_string());
            return Err(Box::from(format!(
                "Set failed with status {}: {}",
                status, error_text
            )) as Box<dyn std::error::Error + Send + Sync>);
        }

        Ok(())
    }

    pub async fn get(&self, key: String) -> Result<Option<String>> {
        let url = format!("{}/api/keys/{}", self.base_url, key);
        
        let response = self
            .client
            .get(&url)
            .send()
            .await
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;

        if !response.status().is_success() {
            let status = response.status();
            let error_text = response
                .text()
                .await
                .unwrap_or_else(|_| "Unknown error".to_string());
            return Err(Box::from(format!(
                "Get failed with status {}: {}",
                status, error_text
            )) as Box<dyn std::error::Error + Send + Sync>);
        }

        let get_response: GetResponse = response
            .json()
            .await
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;

        Ok(get_response.value)
    }

    pub async fn remove(&self, key: String) -> Result<bool> {
        let url = format!("{}/api/keys/{}", self.base_url, key);
        
        let response = self
            .client
            .delete(&url)
            .send()
            .await
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;

        if !response.status().is_success() {
            let status = response.status();
            let error_text = response
                .text()
                .await
                .unwrap_or_else(|_| "Unknown error".to_string());
            return Err(Box::from(format!(
                "Remove failed with status {}: {}",
                status, error_text
            )) as Box<dyn std::error::Error + Send + Sync>);
        }

        let delete_response: DeleteResponse = response
            .json()
            .await
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;

        Ok(delete_response.existed)
    }

    pub async fn reset(&self) -> Result<()> {
        let url = format!("{}/api/reset", self.base_url);
        
        let response = self
            .client
            .post(&url)
            .send()
            .await
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;

        if !response.status().is_success() {
            let status = response.status();
            let error_text = response
                .text()
                .await
                .unwrap_or_else(|_| "Unknown error".to_string());
            return Err(Box::from(format!(
                "Reset failed with status {}: {}",
                status, error_text
            )) as Box<dyn std::error::Error + Send + Sync>);
        }

        Ok(())
    }

    pub async fn batch_set(&self, items: Vec<(String, String)>) -> Result<()> {
        let url = format!("{}/api/batch/set", self.base_url);
        
        let request = BatchSetRequest {
            items: items
                .into_iter()
                .map(|(key, value)| SetRequest { key, value })
                .collect(),
        };

        let response = self
            .client
            .post(&url)
            .json(&request)
            .send()
            .await
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;

        if !response.status().is_success() {
            let status = response.status();
            let error_text = response
                .text()
                .await
                .unwrap_or_else(|_| "Unknown error".to_string());
            return Err(Box::from(format!(
                "Batch set failed with status {}: {}",
                status, error_text
            )) as Box<dyn std::error::Error + Send + Sync>);
        }

        Ok(())
    }

    pub async fn batch_get(&self, keys: Vec<String>) -> Result<Vec<(String, Option<String>)>> {
        let url = format!("{}/api/batch/get", self.base_url);
        
        let request = BatchGetRequest { keys };

        let response = self
            .client
            .post(&url)
            .json(&request)
            .send()
            .await
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;

        if !response.status().is_success() {
            let status = response.status();
            let error_text = response
                .text()
                .await
                .unwrap_or_else(|_| "Unknown error".to_string());
            return Err(Box::from(format!(
                "Batch get failed with status {}: {}",
                status, error_text
            )) as Box<dyn std::error::Error + Send + Sync>);
        }

        let batch_response: BatchGetResponse = response
            .json()
            .await
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;

        Ok(batch_response
            .items
            .into_iter()
            .map(|item| (item.key, item.value))
            .collect())
    }
}
