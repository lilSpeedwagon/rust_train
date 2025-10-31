use tempfile::TempDir;
use walkdir::WalkDir;

use rust_kvs_server::{models, storage};

// Should get previously stored value.
#[test]
fn get_stored_value() -> models::Result<()> {
    let temp_dir = TempDir::new().expect("unable to create temporary working directory");
    let mut store = storage::KvLogStorage::open(temp_dir.path())?;

    store.set("key1".to_owned(), "value1".to_owned())?;
    store.set("key2".to_owned(), "value2".to_owned())?;

    assert_eq!(store.get("key1".to_owned())?, Some("value1".to_owned()));
    assert_eq!(store.get("key2".to_owned())?, Some("value2".to_owned()));

    // Open from disk again and check persistent data.
    drop(store);
    let store = storage::KvLogStorage::open(temp_dir.path())?;
    assert_eq!(store.get("key1".to_owned())?, Some("value1".to_owned()));
    assert_eq!(store.get("key2".to_owned())?, Some("value2".to_owned()));

    Ok(())
}

// Should overwrite existent value.
#[test]
fn overwrite_value() -> models::Result<()> {
    let temp_dir = TempDir::new().expect("unable to create temporary working directory");
    let mut store = storage::KvLogStorage::open(temp_dir.path())?;

    store.set("key1".to_owned(), "value1".to_owned())?;
    assert_eq!(store.get("key1".to_owned())?, Some("value1".to_owned()));
    store.set("key1".to_owned(), "value2".to_owned())?;
    assert_eq!(store.get("key1".to_owned())?, Some("value2".to_owned()));

    // Open from disk again and check persistent data.
    drop(store);
    let mut store = storage::KvLogStorage::open(temp_dir.path())?;
    assert_eq!(store.get("key1".to_owned())?, Some("value2".to_owned()));
    store.set("key1".to_owned(), "value3".to_owned())?;
    assert_eq!(store.get("key1".to_owned())?, Some("value3".to_owned()));

    Ok(())
}

// Should get `None` when getting a non-existent key.
#[test]
fn get_non_existent_value() -> models::Result<()> {
    let temp_dir = TempDir::new().expect("unable to create temporary working directory");
    let mut store = storage::KvLogStorage::open(temp_dir.path())?;

    store.set("key1".to_owned(), "value1".to_owned())?;
    assert_eq!(store.get("key2".to_owned())?, None);

    // Open from disk again and check persistent data.
    drop(store);
    let store = storage::KvLogStorage::open(temp_dir.path())?;
    assert_eq!(store.get("key2".to_owned())?, None);

    Ok(())
}

#[test]
fn remove_non_existent_key() -> models::Result<()> {
    let temp_dir = TempDir::new().expect("unable to create temporary working directory");
    let mut store = storage::KvLogStorage::open(temp_dir.path())?;
    assert!(!store.remove("key1".to_owned()).unwrap());
    Ok(())
}

#[test]
fn remove_key() -> models::Result<()> {
    let temp_dir = TempDir::new().expect("unable to create temporary working directory");
    let mut store = storage::KvLogStorage::open(temp_dir.path())?;
    store.set("key1".to_owned(), "value1".to_owned())?;
    assert!(store.remove("key1".to_owned()).is_ok());
    assert_eq!(store.get("key1".to_owned())?, None);
    Ok(())
}

// Insert data until total size of the directory decreases.
// Test data correctness after compaction.
#[test]
fn compaction() -> models::Result<()> {
    let temp_dir = TempDir::new().expect("unable to create temporary working directory");
    let mut store = storage::KvLogStorage::open(temp_dir.path())?;

    let dir_size = || {
        let entries = WalkDir::new(temp_dir.path()).into_iter();
        let len: walkdir::Result<u64> = entries
            .map(|res| {
                res.and_then(|entry| entry.metadata())
                    .map(|metadata| metadata.len())
            })
            .sum();
        len.expect("fail to get directory size")
    };

    // We expect the compaction to be triggered after inserting values with total size
    // exceeding the max segment size.
    let expected_segment_size = 4_000_000;
    let values_count = 10;
    let value_size = expected_segment_size / values_count;
    let key = "key".to_string();

    let initial_size = dir_size();
    for idx in 0..values_count - 1 {
        let value = idx.to_string().repeat(value_size);
        store.set(key.clone(), value)?;
    }

    // The dir size is expected to be bigger.
    let new_size = dir_size();
    assert!(new_size > initial_size);

    // The last insert should trigger the compaction.
    let value = (values_count - 1).to_string().repeat(value_size);
    store.set(key.clone(), value.clone())?;

    // Wait for compaction and check the directory size.
    let mut compaction_detected = false;
    for _ in 0..10 {
        std::thread::sleep(std::time::Duration::from_millis(50));
        let compacted_size = dir_size();
        if compacted_size < new_size {
            compaction_detected = true;
            break;
        }
    }
    assert!(compaction_detected, "No compaction detected!");

    // reopen the storage and check the value.
    drop(store);
    let store = storage::KvLogStorage::open(temp_dir.path())?;
    assert_eq!(store.get(key)?, Some(value));

    Ok(())
}
