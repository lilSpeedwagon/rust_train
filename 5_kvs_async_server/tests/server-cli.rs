use assert_cmd::prelude::*;
use predicates::str::contains;
use std::process::Command;
use tempfile::TempDir;

// `kvs_server -V` should print the version
#[test]
fn server_cli_version() {
    let temp_dir = TempDir::new().unwrap();
    let mut cmd = Command::cargo_bin("kvs_server").unwrap();
    cmd.args(&["-V"])
        .current_dir(&temp_dir)
        .assert()
        .stdout(contains(env!("CARGO_PKG_VERSION")));
}

// Check available args for engine type.
#[test]
fn cli_wrong_engine() {
    let temp_dir = TempDir::new().unwrap();
    let mut cmd = Command::cargo_bin("kvs_server").unwrap();
    cmd.args(&["--engine", "unknown"])
        .current_dir(&temp_dir)
        .assert()
        .failure();
}

// Hostname should be a valid host address.
#[test]
fn cli_invalid_host() {
    let temp_dir = TempDir::new().unwrap();
    let mut cmd = Command::cargo_bin("kvs_server").unwrap();
    cmd.args(&["--host", "unknown"])
        .current_dir(&temp_dir)
        .assert()
        .failure();
}

// Port should be a valid integer value.
#[test]
fn cli_invalid_port() {
    let temp_dir = TempDir::new().unwrap();
    let mut cmd = Command::cargo_bin("kvs_server").unwrap();
    cmd.args(&["--port", "abc"])
        .current_dir(&temp_dir)
        .assert()
        .failure();
}

// Path should be a valid filesystem path.
#[test]
fn cli_invalid_path() {
    let temp_dir = TempDir::new().unwrap();
    let mut cmd = Command::cargo_bin("kvs_server").unwrap();
    cmd.args(&["--path", "/Volumes/nonexistent_drive/somefile"])
        .current_dir(&temp_dir)
        .assert()
        .failure();
}
