use assert_cmd::prelude::*;
use predicates::str::contains;
use rust_kvs::kv::KvStore;
use std::process::Command;

// `kvs` with no args should exit with a non-zero code.
#[test]
fn cli_no_args() {
    Command::cargo_bin("kvs").unwrap().assert().failure();
}

// `kvs -V` should print the version
#[test]
fn cli_version() {
    Command::cargo_bin("kvs")
        .unwrap()
        .args(&["-V"])
        .assert()
        .stdout(contains(env!("CARGO_PKG_VERSION")));
}

// `kvs get <KEY>` should print "Key not found" to stdout and exit with zero code
#[test]
fn cli_get() {
    Command::cargo_bin("kvs")
        .unwrap()
        .args(&["get", "key1"])
        .assert()
        .success()
        .stdout(contains("Key not found"));
}

// `kvs set <KEY> <VALUE>` should print nothing to stdout and exit with zero code
#[test]
fn cli_set() {
    Command::cargo_bin("kvs")
        .unwrap()
        .args(&["set", "key1", "value1"])
        .assert()
        .success();
}

// `kvs remove <KEY>` should print nothing to stdout and exit with zero code
#[test]
fn cli_rm() {
    Command::cargo_bin("kvs")
        .unwrap()
        .args(&["remove", "key1"])
        .assert()
        .success();
}

#[test]
fn cli_invalid_get() {
    Command::cargo_bin("kvs")
        .unwrap()
        .args(&["get"])
        .assert()
        .failure();

    Command::cargo_bin("kvs")
        .unwrap()
        .args(&["get", "extra", "field"])
        .assert()
        .failure();
}

#[test]
fn cli_invalid_set() {
    Command::cargo_bin("kvs")
        .unwrap()
        .args(&["set"])
        .assert()
        .failure();

    Command::cargo_bin("kvs")
        .unwrap()
        .args(&["set", "missing_field"])
        .assert()
        .failure();

    Command::cargo_bin("kvs")
        .unwrap()
        .args(&["set", "extra", "extra", "field"])
        .assert()
        .failure();
}

#[test]
fn cli_invalid_rm() {
    Command::cargo_bin("kvs")
        .unwrap()
        .args(&["remove"])
        .assert()
        .failure();

    Command::cargo_bin("kvs")
        .unwrap()
        .args(&["remove", "extra", "field"])
        .assert()
        .failure();
}

#[test]
fn cli_invalid_subcommand() {
    Command::cargo_bin("kvs")
        .unwrap()
        .args(&["unknown", "subcommand"])
        .assert()
        .failure();
}

// Should get previously stored value
#[test]
fn get_stored_value() {
    let mut store = KvStore::new();

    store.set("key1".to_owned(), "value1".to_owned());
    store.set("key2".to_owned(), "value2".to_owned());

    assert_eq!(store.get("key1".to_owned()), Some("value1".to_owned()));
    assert_eq!(store.get("key2".to_owned()), Some("value2".to_owned()));
}

// Should overwrite existent value
#[test]
fn overwrite_value() {
    let mut store = KvStore::new();

    store.set("key1".to_owned(), "value1".to_owned());
    assert_eq!(store.get("key1".to_owned()), Some("value1".to_owned()));

    store.set("key1".to_owned(), "value2".to_owned());
    assert_eq!(store.get("key1".to_owned()), Some("value2".to_owned()));
}

// Should get `None` when getting a non-existent key
#[test]
fn get_non_existent_value() {
    let mut store = KvStore::new();

    store.set("key1".to_owned(), "value1".to_owned());
    assert_eq!(store.get("key2".to_owned()), None);
}

#[test]
fn remove_key() {
    let mut store = KvStore::new();

    store.set("key1".to_owned(), "value1".to_owned());
    store.remove("key1".to_owned());
    assert_eq!(store.get("key1".to_owned()), None);
}
