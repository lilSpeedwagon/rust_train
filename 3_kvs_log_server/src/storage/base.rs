/// Base trait for a key value storage engines.
pub trait KVStorage {
    /// Set key `key` to value `value`.
     fn set(&mut self, key: String, value: String) -> std::result::Result<(), Box<dyn std::error::Error>>;

    /// Removes key `key` from the storage.
    /// Returns `true` if the key existed.
    fn remove(&mut self, key: String) -> std::result::Result<bool, Box<dyn std::error::Error>>;

    /// Gets value with the key `key`. Returns `None` if the key doesn't exist in the storage.
    fn get(&self, key: String) -> std::result::Result<Option<String>, Box<dyn std::error::Error>>;

    /// Removes all records in the storage.
    fn reset(&mut self) -> std::result::Result<(), Box<dyn std::error::Error>>;
}
