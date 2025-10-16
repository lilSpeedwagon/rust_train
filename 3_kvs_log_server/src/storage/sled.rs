use sled;

use crate::models;
use crate::KVStorage;


pub struct SledStorage {
    db: sled::Db,
}

impl SledStorage {
    pub fn open(path: &std::path::Path) -> models::Result<SledStorage> {
        let db = sled::open(path)?;
        Ok(
            SledStorage{
                db: db,
            }
        )
    }
}

impl KVStorage for SledStorage {
    /// Set key `key` to value `value`.
    fn set(&mut self, key: String, value: String) -> models::Result<()> {
        let val_inner = sled::IVec::from(value.as_bytes());
        self.db.insert(key, val_inner)?;
        self.db.flush()?;
        Ok(())
    }

    /// Removes key `key` from the storage.
    /// Returns `true` if the key existed.
    fn remove(&mut self, key: String) -> models::Result<bool> {
        let old_value = self.db.remove(key)?;
        self.db.flush()?;
        Ok(old_value.is_some())
    }

    /// Gets value with the key `key`. Returns `None` if the key doesn't exist in the storage.
    fn get(&self, key: String) -> models::Result<Option<String>> {
        let val_opt = self.db.get(key)?;
        match val_opt {
            Some(val) => Ok(Some(String::from_utf8(val.to_vec())?)),
            None => Ok(None)
        }
    }

    /// Removes all records in the storage.
    fn reset(&mut self) -> models::Result<()> {
        self.db.clear()?;
        self.db.flush()?;
        Ok(())
    }
}
