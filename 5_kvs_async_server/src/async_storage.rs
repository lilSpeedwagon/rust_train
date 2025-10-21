use std::path::Path;
use tokio::sync::RwLock;
use std::sync::Arc;

use crate::models::Result;
use crate::storage::KvLogStorage;

/// Async wrapper around KvLogStorage
#[derive(Clone)]
pub struct AsyncKvStorage {
    inner: Arc<RwLock<KvLogStorage>>,
}

impl AsyncKvStorage {
    pub async fn open(path: &Path) -> Result<Self> {
        // Open storage in a blocking task since file I/O is blocking
        let path_buf = path.to_path_buf();
        let storage = tokio::task::spawn_blocking(move || {
            KvLogStorage::open(&path_buf)
        })
        .await
        .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)??;

        Ok(AsyncKvStorage {
            inner: Arc::new(RwLock::new(storage)),
        })
    }

    pub async fn set(&self, key: String, value: String) -> Result<()> {
        let mut storage = self.inner.write().await;
        storage.set(key, value)
    }

    pub async fn get(&self, key: String) -> Result<Option<String>> {
        let storage = self.inner.read().await;
        
        // Clone the storage for the blocking task to avoid holding lock across await
        let storage_clone = storage.clone();
        drop(storage);
        
        storage_clone.get(key)
    }

    pub async fn remove(&self, key: String) -> Result<bool> {
        let mut storage = self.inner.write().await;
        storage.remove(key)
    }

    pub async fn reset(&self) -> Result<()> {
        let mut storage = self.inner.write().await;
        storage.reset()
    }
}
