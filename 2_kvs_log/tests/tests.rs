use assert_cmd::prelude::*;
use rust_kvs_log::kv_log::KvStore;
use rust_kvs_log::models::Result;
use predicates::ord::eq;
use predicates::str::{contains, is_empty, PredicateStrExt};
use std::process::Command;
use tempfile::TempDir;
use walkdir::WalkDir;

// `kvs` with no args should exit with a non-zero code.
#[test]
fn cli_no_args() {
    Command::cargo_bin("kvs_log").unwrap().assert().failure();
}

// `kvs -V` should print the version
#[test]
fn cli_version() {
    Command::cargo_bin("kvs_log")
        .unwrap()
        .args(&["-V"])
        .assert()
        .stdout(contains(env!("CARGO_PKG_VERSION")));
}

// `kvs get <KEY>` should print "Key not found" for a non-existent key and exit with zero.
#[test]
fn cli_get_non_existent_key() {
    let temp_dir = TempDir::new().unwrap();
    Command::cargo_bin("kvs_log")
        .unwrap()
        .args(&["get", "key1"])
        .current_dir(&temp_dir)
        .assert()
        .success()
        .stdout(eq("Key not found").trim());
}

// `kvs remove <KEY>` should print "Key not found" for an empty database and exit with non-zero code.
#[test]
fn cli_remove_non_existent_key() {
    let temp_dir = TempDir::new().expect("unable to create temporary working directory");
    Command::cargo_bin("kvs_log")
        .unwrap()
        .args(&["remove", "key1"])
        .current_dir(&temp_dir)
        .assert()
        .failure()
        .stderr(eq("Key not found").trim());
}

// `kvs set <KEY> <VALUE>` should print nothing and exit with zero.
#[test]
fn cli_set() {
    let temp_dir = TempDir::new().expect("unable to create temporary working directory");
    Command::cargo_bin("kvs_log")
        .unwrap()
        .args(&["set", "key1", "value1"])
        .current_dir(&temp_dir)
        .assert()
        .success()
        .stdout(is_empty());
}

#[test]
fn cli_get_stored() -> Result<()> {
    let temp_dir = TempDir::new().expect("unable to create temporary working directory");

    let mut store = KvStore::open(temp_dir.path())?;
    store.set("key1".to_owned(), "value1".to_owned())?;
    store.set("key2".to_owned(), "value2".to_owned())?;
    drop(store);

    Command::cargo_bin("kvs_log")
        .unwrap()
        .args(&["get", "key1"])
        .current_dir(&temp_dir)
        .assert()
        .success()
        .stdout(eq("value1").trim());

    Command::cargo_bin("kvs_log")
        .unwrap()
        .args(&["get", "key2"])
        .current_dir(&temp_dir)
        .assert()
        .success()
        .stdout(eq("value2").trim());

    Ok(())
}

// `kvs remove <KEY>` should print nothing and exit with zero.
#[test]
fn cli_remove_stored() -> Result<()> {
    let temp_dir = TempDir::new().expect("unable to create temporary working directory");

    let mut store = KvStore::open(temp_dir.path())?;
    store.set("key1".to_owned(), "value1".to_owned())?;
    drop(store);

    Command::cargo_bin("kvs_log")
        .unwrap()
        .args(&["remove", "key1"])
        .current_dir(&temp_dir)
        .assert()
        .success()
        .stdout(is_empty());

    Command::cargo_bin("kvs_log")
        .unwrap()
        .args(&["get", "key1"])
        .current_dir(&temp_dir)
        .assert()
        .success()
        .stdout(eq("Key not found").trim());

    Ok(())
}

// `kvs reset` should remove all of the stored elements.
#[test]
fn cli_reset_stored() -> Result<()> {
    let temp_dir = TempDir::new().expect("unable to create temporary working directory");

    let mut store = KvStore::open(temp_dir.path())?;
    for i in 1..10 {
        store.set(format!("key{}", i).to_owned(), format!("value{}", i).to_owned())?;
    }
    drop(store);

    Command::cargo_bin("kvs_log")
        .unwrap()
        .args(&["reset"])
        .current_dir(&temp_dir)
        .assert()
        .success()
        .stdout(is_empty());

    for i in 1..10 {
        Command::cargo_bin("kvs_log")
            .unwrap()
            .args(&["get", format!("key{}", i).as_str()])
            .current_dir(&temp_dir)
            .assert()
            .success()
            .stdout(eq("Key not found").trim());
    }

    Ok(())
}

#[test]
fn cli_invalid_get() {
    Command::cargo_bin("kvs_log")
        .unwrap()
        .args(&["get"])
        .assert()
        .failure();

    Command::cargo_bin("kvs_log")
        .unwrap()
        .args(&["get", "extra", "field"])
        .assert()
        .failure();
}

#[test]
fn cli_invalid_set() {
    Command::cargo_bin("kvs_log")
        .unwrap()
        .args(&["set"])
        .assert()
        .failure();

    Command::cargo_bin("kvs_log")
        .unwrap()
        .args(&["set", "missing_field"])
        .assert()
        .failure();

    Command::cargo_bin("kvs_log")
        .unwrap()
        .args(&["set", "extra", "extra", "field"])
        .assert()
        .failure();
}

#[test]
fn cli_invalid_remove() {
    Command::cargo_bin("kvs_log")
        .unwrap()
        .args(&["remove"])
        .assert()
        .failure();

    Command::cargo_bin("kvs_log")
        .unwrap()
        .args(&["remove", "extra", "field"])
        .assert()
        .failure();
}

#[test]
fn cli_invalid_subcommand() {
    Command::cargo_bin("kvs_log")
        .unwrap()
        .args(&["unknown", "subcommand"])
        .assert()
        .failure();
}

// Should get previously stored value.
#[test]
fn get_stored_value() -> Result<()> {
    let temp_dir = TempDir::new().expect("unable to create temporary working directory");
    let mut store = KvStore::open(temp_dir.path())?;

    store.set("key1".to_owned(), "value1".to_owned())?;
    store.set("key2".to_owned(), "value2".to_owned())?;

    assert_eq!(store.get("key1".to_owned())?, Some("value1".to_owned()));
    assert_eq!(store.get("key2".to_owned())?, Some("value2".to_owned()));

    // Open from disk again and check persistent data.
    drop(store);
    let store = KvStore::open(temp_dir.path())?;
    assert_eq!(store.get("key1".to_owned())?, Some("value1".to_owned()));
    assert_eq!(store.get("key2".to_owned())?, Some("value2".to_owned()));

    Ok(())
}

// Should overwrite existent value.
#[test]
fn overwrite_value() -> Result<()> {
    let temp_dir = TempDir::new().expect("unable to create temporary working directory");
    let mut store = KvStore::open(temp_dir.path())?;

    store.set("key1".to_owned(), "value1".to_owned())?;
    assert_eq!(store.get("key1".to_owned())?, Some("value1".to_owned()));
    store.set("key1".to_owned(), "value2".to_owned())?;
    assert_eq!(store.get("key1".to_owned())?, Some("value2".to_owned()));

    // Open from disk again and check persistent data.
    drop(store);
    let mut store = KvStore::open(temp_dir.path())?;
    assert_eq!(store.get("key1".to_owned())?, Some("value2".to_owned()));
    store.set("key1".to_owned(), "value3".to_owned())?;
    assert_eq!(store.get("key1".to_owned())?, Some("value3".to_owned()));

    Ok(())
}

// Should get `None` when getting a non-existent key.
#[test]
fn get_non_existent_value() -> Result<()> {
    let temp_dir = TempDir::new().expect("unable to create temporary working directory");
    let mut store = KvStore::open(temp_dir.path())?;

    store.set("key1".to_owned(), "value1".to_owned())?;
    assert_eq!(store.get("key2".to_owned())?, None);

    // Open from disk again and check persistent data.
    drop(store);
    let store = KvStore::open(temp_dir.path())?;
    assert_eq!(store.get("key2".to_owned())?, None);

    Ok(())
}

#[test]
fn remove_non_existent_key() -> Result<()> {
    let temp_dir = TempDir::new().expect("unable to create temporary working directory");
    let mut store = KvStore::open(temp_dir.path())?;
    assert!(!store.remove("key1".to_owned()).unwrap());
    Ok(())
}

#[test]
fn remove_key() -> Result<()> {
    let temp_dir = TempDir::new().expect("unable to create temporary working directory");
    let mut store = KvStore::open(temp_dir.path())?;
    store.set("key1".to_owned(), "value1".to_owned())?;
    assert!(store.remove("key1".to_owned()).is_ok());
    assert_eq!(store.get("key1".to_owned())?, None);
    Ok(())
}

// Insert data until total size of the directory decreases.
// Test data correctness after compaction.
#[test]
fn compaction() -> Result<()> {
    let temp_dir = TempDir::new().expect("unable to create temporary working directory");
    let mut store = KvStore::open(temp_dir.path())?;

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

    let mut current_size = dir_size();
    for iter in 0..100 {
        for key_id in 0..10 {
            let key = format!("key{}", key_id);
            let value = format!("value{}-{}", key_id, iter).repeat(10000);
            store.set(key, value)?;
        }

        let new_size = dir_size();
        if new_size > current_size {
            current_size = new_size;
            continue;
        }
        // Compaction triggered.

        // Reopen and check content.
        drop(store);
        let store = KvStore::open(temp_dir.path())?;
        for key_id in 0..10 {
            let key = format!("key{}", key_id);
            assert_eq!(store.get(key)?, Some(format!("value{}-{}", key_id, iter).repeat(10000)));
        }
        return Ok(());
    }

    panic!("No compaction detected");
}
