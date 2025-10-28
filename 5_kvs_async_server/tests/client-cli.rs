use assert_cmd::prelude::*;
use predicates::str::{contains};
use std::process::Command;
use tempfile::TempDir;

// `kvs_client` with no args should exit with a non-zero code.
#[test]
fn client_cli_no_args() {
    let temp_dir = TempDir::new().unwrap();
    let mut cmd = Command::cargo_bin("kvs_client").unwrap();
    cmd.current_dir(&temp_dir).assert().failure();
}

// `kvs_client` get command should take exactly one argument.
#[test]
fn client_cli_invalid_get() {
    let temp_dir = TempDir::new().unwrap();
    Command::cargo_bin("kvs_client")
        .unwrap()
        .args(&["get"])
        .current_dir(&temp_dir)
        .assert()
        .failure();

    Command::cargo_bin("kvs_client")
        .unwrap()
        .args(&["get", "extra", "field"])
        .current_dir(&temp_dir)
        .assert()
        .failure();
}

// `kvs_client` server args are expected to be hostname and port. The "get" command is used as an example.
#[test]
fn client_cli_invalid_args() {
    let temp_dir = TempDir::new().unwrap();

    Command::cargo_bin("kvs_client")
        .unwrap()
        .args(&["get", "key", "--host", "invalid-addr"])
        .current_dir(&temp_dir)
        .assert()
        .failure();

    Command::cargo_bin("kvs_client")
        .unwrap()
        .args(&["get", "key", "--port", "abc"])
        .current_dir(&temp_dir)
        .assert()
        .failure();

    Command::cargo_bin("kvs_client")
        .unwrap()
        .args(&["get", "key", "--read-timeout", "abc"])
        .current_dir(&temp_dir)
        .assert()
        .failure();

    Command::cargo_bin("kvs_client")
        .unwrap()
        .args(&["get", "key", "--unknown-flag"])
        .current_dir(&temp_dir)
        .assert()
        .failure();
}

// "set" command should take exactly 2 args.
#[test]
fn client_cli_invalid_set() {
    let temp_dir = TempDir::new().unwrap();
    Command::cargo_bin("kvs_client")
        .unwrap()
        .args(&["set"])
        .current_dir(&temp_dir)
        .assert()
        .failure();

    Command::cargo_bin("kvs_client")
        .unwrap()
        .args(&["set", "missing_field"])
        .current_dir(&temp_dir)
        .assert()
        .failure();

    Command::cargo_bin("kvs_client")
        .unwrap()
        .args(&["set", "key", "value", "extra_field"])
        .current_dir(&temp_dir)
        .assert()
        .failure();
}

// "remove" command should take exactly 1 arg.
#[test]
fn client_cli_invalid_rm() {
    let temp_dir = TempDir::new().unwrap();
    Command::cargo_bin("kvs_client")
        .unwrap()
        .args(&["remove"])
        .current_dir(&temp_dir)
        .assert()
        .failure();

    Command::cargo_bin("kvs_client")
        .unwrap()
        .args(&["remove", "extra", "field"])
        .current_dir(&temp_dir)
        .assert()
        .failure();
}

// test invalid command
#[test]
fn client_cli_invalid_subcommand() {
    let temp_dir = TempDir::new().unwrap();
    Command::cargo_bin("kvs_client")
        .unwrap()
        .args(&["unknown"])
        .current_dir(&temp_dir)
        .assert()
        .failure();
}

// `kvs_client -V` should print the version
#[test]
fn client_cli_version() {
    let temp_dir = TempDir::new().unwrap();
    let mut cmd = Command::cargo_bin("kvs_client").unwrap();
    cmd.args(&["-V"])
        .current_dir(&temp_dir)
        .assert()
        .stdout(contains(env!("CARGO_PKG_VERSION")));
}
