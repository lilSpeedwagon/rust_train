use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{delete, get, post, put},
    Json, Router,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tower_http::cors::CorsLayer;
use tower_http::trace::TraceLayer;

use crate::async_storage::AsyncKvStorage;
use crate::models;

/// Application state shared across handlers
#[derive(Clone)]
pub struct AppState {
    storage: AsyncKvStorage,
}

/// JSON request/response types
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
pub struct ErrorResponse {
    pub error: String,
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

/// Custom error type for HTTP responses
pub enum ApiError {
    StorageError(String),
    NotFound,
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let (status, error_message) = match self {
            ApiError::StorageError(msg) => (StatusCode::INTERNAL_SERVER_ERROR, msg),
            ApiError::NotFound => (StatusCode::NOT_FOUND, "Key not found".to_string()),
        };

        let body = Json(ErrorResponse {
            error: error_message,
        });

        (status, body).into_response()
    }
}

impl From<Box<dyn std::error::Error + Send + Sync>> for ApiError {
    fn from(err: Box<dyn std::error::Error + Send + Sync>) -> Self {
        ApiError::StorageError(err.to_string())
    }
}

/// Health check endpoint
async fn health_check() -> impl IntoResponse {
    Json(MessageResponse {
        message: "OK".to_string(),
    })
}

/// GET /api/keys/:key - Get a value by key
async fn get_key(
    State(state): State<Arc<AppState>>,
    Path(key): Path<String>,
) -> Result<impl IntoResponse, ApiError> {
    log::debug!("GET /api/keys/{}", key);
    
    let value = state.storage.get(key.clone()).await?;
    
    Ok(Json(GetResponse { key, value }))
}

/// PUT /api/keys/:key - Set a value for a key
async fn set_key(
    State(state): State<Arc<AppState>>,
    Path(key): Path<String>,
    Json(payload): Json<serde_json::Value>,
) -> Result<impl IntoResponse, ApiError> {
    log::debug!("PUT /api/keys/{}", key);
    
    // Extract value from JSON - accept both string and object with "value" field
    let value = if let Some(v) = payload.as_str() {
        v.to_string()
    } else if let Some(v) = payload.get("value").and_then(|v| v.as_str()) {
        v.to_string()
    } else {
        // If neither, serialize the entire payload as string
        payload.to_string()
    };
    
    state.storage.set(key.clone(), value).await?;
    
    Ok((
        StatusCode::OK,
        Json(MessageResponse {
            message: format!("Key '{}' set successfully", key),
        }),
    ))
}

/// POST /api/keys - Set a value (alternative endpoint)
async fn post_key(
    State(state): State<Arc<AppState>>,
    Json(request): Json<SetRequest>,
) -> Result<impl IntoResponse, ApiError> {
    log::debug!("POST /api/keys - key: {}", request.key);
    
    state.storage.set(request.key.clone(), request.value).await?;
    
    Ok((
        StatusCode::CREATED,
        Json(MessageResponse {
            message: format!("Key '{}' set successfully", request.key),
        }),
    ))
}

/// DELETE /api/keys/:key - Remove a key
async fn delete_key(
    State(state): State<Arc<AppState>>,
    Path(key): Path<String>,
) -> Result<impl IntoResponse, ApiError> {
    log::debug!("DELETE /api/keys/{}", key);
    
    let existed = state.storage.remove(key.clone()).await?;
    
    Ok(Json(DeleteResponse { key, existed }))
}

/// POST /api/reset - Reset/clear all data
async fn reset_storage(
    State(state): State<Arc<AppState>>,
) -> Result<impl IntoResponse, ApiError> {
    log::debug!("POST /api/reset");
    
    state.storage.reset().await?;
    
    Ok(Json(MessageResponse {
        message: "Storage reset successfully".to_string(),
    }))
}

/// POST /api/batch/set - Batch set operation
async fn batch_set(
    State(state): State<Arc<AppState>>,
    Json(request): Json<BatchSetRequest>,
) -> Result<impl IntoResponse, ApiError> {
    log::debug!("POST /api/batch/set - {} items", request.items.len());
    
    for item in request.items {
        state.storage.set(item.key, item.value).await?;
    }
    
    Ok(Json(MessageResponse {
        message: "Batch set completed successfully".to_string(),
    }))
}

/// POST /api/batch/get - Batch get operation
async fn batch_get(
    State(state): State<Arc<AppState>>,
    Json(request): Json<BatchGetRequest>,
) -> Result<impl IntoResponse, ApiError> {
    log::debug!("POST /api/batch/get - {} keys", request.keys.len());
    
    let mut items = Vec::new();
    for key in request.keys {
        let value = state.storage.get(key.clone()).await?;
        items.push(GetResponse { key, value });
    }
    
    Ok(Json(BatchGetResponse { items }))
}

/// Async KV Storage Server
pub struct AsyncKvsServer {
    storage: AsyncKvStorage,
}

impl AsyncKvsServer {
    pub fn new(storage: AsyncKvStorage) -> Self {
        AsyncKvsServer { storage }
    }

    pub async fn listen(self, host: String, port: u32) -> models::Result<()> {
        let state = Arc::new(AppState {
            storage: self.storage,
        });

        // Build router with all routes
        let app = Router::new()
            .route("/health", get(health_check))
            .route("/api/keys/:key", get(get_key))
            .route("/api/keys/:key", put(set_key))
            .route("/api/keys/:key", delete(delete_key))
            .route("/api/keys", post(post_key))
            .route("/api/reset", post(reset_storage))
            .route("/api/batch/set", post(batch_set))
            .route("/api/batch/get", post(batch_get))
            .layer(CorsLayer::permissive())
            .layer(TraceLayer::new_for_http())
            .with_state(state);

        let addr = format!("{}:{}", host, port);
        log::info!("Starting async server on {}", addr);
        
        let listener = tokio::net::TcpListener::bind(&addr)
            .await
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;
        
        log::info!("Server listening on {}", addr);
        
        axum::serve(listener, app)
            .await
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;

        Ok(())
    }
}
