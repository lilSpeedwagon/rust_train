use assert_cmd::prelude::*;
use predicates::str::{contains};
use std::process::Command;
use std::time::Duration;
use tempfile::TempDir;


const HOST: &str = "127.0.0.1";
const PORT: u32 = 4009;

struct ServerGuard {
    sender: std::sync::mpsc::SyncSender<()>,
    handler: Option<std::thread::JoinHandle<()>>,
}

impl Drop for ServerGuard {
    fn drop(&mut self) {
        if let Some(handler) = self.handler.take() {
            self.sender.send(()).unwrap();
            handler.join().unwrap()
        }
    }
}


fn run_server(dir: &tempfile::TempDir, engine: &str, host: &str, port: u32) -> ServerGuard {
    let (sender, receiver) = std::sync::mpsc::sync_channel::<()>(0);
    let mut server = Command::cargo_bin("kvs_server").unwrap();
    let mut child = server
        .args(&["--engine", engine, "--host", host, "--port", &port.to_string()])
        .current_dir(&dir)
        .spawn()
        .unwrap();

    let handle = std::thread::spawn(move || {
        let _ = receiver.recv(); // wait for main thread to finish
        child.kill().expect("server exited before killed");
        print!("kill test server");
    });
    std::thread::sleep(Duration::from_secs(1));
    ServerGuard{ sender: sender, handler: Some(handle) }
}


fn run_client_cmd(dir: &tempfile::TempDir, host: &str, port: u32, args: &[&str]) -> assert_cmd::assert::Assert {
    let port_str = port.to_string();
    let mut cmd_args = vec!["--host", host, "--port", &port_str];
    cmd_args.extend_from_slice(&args);

    Command::cargo_bin("kvs_client")
        .unwrap()
        .args(cmd_args.as_slice())
        .current_dir(&dir)
        .assert()
        .success()
}


#[test]
#[serial_test::serial]
fn kvs_set_get_value() {
    let temp_dir = TempDir::new().unwrap();
    let server_guard = run_server(&temp_dir, "kvs", HOST, PORT);

    // Set some values.
    run_client_cmd(&temp_dir, HOST, PORT, &["set", "key1", "value1"])
        .stdout(contains("SET OK"));
    run_client_cmd(&temp_dir, HOST, PORT, &["set", "key2", "value2"])
        .stdout(contains("SET OK"));

    // Make sure the values are present.
    run_client_cmd(&temp_dir, HOST, PORT, &["get", "key1"])
        .stdout(contains("value1"));
    run_client_cmd(&temp_dir, HOST, PORT, &["get", "key2"])
        .stdout(contains("value2"));

    // Restart the server and check again.
    drop(server_guard);
    let _server_guard = run_server(&temp_dir, "kvs", HOST, PORT);

    run_client_cmd(&temp_dir, HOST, PORT, &["get", "key1"])
        .stdout(contains("value1"));
    run_client_cmd(&temp_dir, HOST, PORT, &["get", "key2"])
        .stdout(contains("value2"));
}

#[test]
#[serial_test::serial]
fn kvs_set_override() {
    let temp_dir = TempDir::new().unwrap();
    let server_guard = run_server(&temp_dir, "kvs", HOST, PORT);

    // Set value for key.
    run_client_cmd(&temp_dir, HOST, PORT, &["set", "key", "value1"])
        .stdout(contains("SET OK"));

    // Override the same key.
    run_client_cmd(&temp_dir, HOST, PORT, &["set", "key", "value2"])
        .stdout(contains("SET OK"));

    // Make sure the new value is saved.
    run_client_cmd(&temp_dir, HOST, PORT, &["get", "key"])
        .stdout(contains("value2"));

    // Restart the server and check again.
    drop(server_guard);
    let _server_guard = run_server(&temp_dir, "kvs", HOST, PORT);

    run_client_cmd(&temp_dir, HOST, PORT, &["get", "key"])
        .stdout(contains("value2"));
}

#[test]
#[serial_test::serial]
fn kvs_get_missing_value() {
    let temp_dir = TempDir::new().unwrap();
    let _server_guard = run_server(&temp_dir, "kvs", HOST, PORT);

    // Get for non existing keys should return NONE.
    run_client_cmd(&temp_dir, HOST, PORT, &["get", "key1"])
        .stdout(contains("GET NONE"));
    run_client_cmd(&temp_dir, HOST, PORT, &["get", "key2"])
        .stdout(contains("GET NONE"));
}

#[test]
#[serial_test::serial]
fn kvs_remove_key() {
    let temp_dir = TempDir::new().unwrap();
    let server_guard = run_server(&temp_dir, "kvs", HOST, PORT);

    // Set some values.
    run_client_cmd(&temp_dir, HOST, PORT, &["set", "key1", "value1"])
        .stdout(contains("SET OK"));
    run_client_cmd(&temp_dir, HOST, PORT, &["set", "key2", "value2"])
        .stdout(contains("SET OK"));

    // Remove.
    run_client_cmd(&temp_dir, HOST, PORT, &["remove", "key1"])
        .stdout(contains("REMOVE OK"));
    run_client_cmd(&temp_dir, HOST, PORT, &["remove", "key2"])
        .stdout(contains("REMOVE OK"));

    // Make sure the values are not present anymore.
    run_client_cmd(&temp_dir, HOST, PORT, &["get", "key1"])
        .stdout(contains("GET NONE"));
    run_client_cmd(&temp_dir, HOST, PORT, &["get", "key2"])
        .stdout(contains("GET NONE"));

    // Restart the server and check again.
    drop(server_guard);
    let _server_guard = run_server(&temp_dir, "kvs", HOST, PORT);

    run_client_cmd(&temp_dir, HOST, PORT, &["get", "key1"])
        .stdout(contains("GET NONE"));
    run_client_cmd(&temp_dir, HOST, PORT, &["get", "key2"])
        .stdout(contains("GET NONE"));
}

#[test]
#[serial_test::serial]
fn kvs_remove_missing_key() {
    let temp_dir = TempDir::new().unwrap();
    let _server_guard = run_server(&temp_dir, "kvs", HOST, PORT);

    // Remove missing keys.
    run_client_cmd(&temp_dir, HOST, PORT, &["remove", "key1"])
        .stdout(contains("REMOVE OK"));
    run_client_cmd(&temp_dir, HOST, PORT, &["remove", "key2"])
        .stdout(contains("REMOVE OK"));
}


#[test]
#[serial_test::serial]
fn kvs_reset() {
    let temp_dir = TempDir::new().unwrap();
    let server_guard = run_server(&temp_dir, "kvs", HOST, PORT);

    // Set some values.
    run_client_cmd(&temp_dir, HOST, PORT, &["set", "key1", "value1"])
        .stdout(contains("SET OK"));
    run_client_cmd(&temp_dir, HOST, PORT, &["set", "key2", "value2"])
        .stdout(contains("SET OK"));

    // Reset the storage.
    run_client_cmd(&temp_dir, HOST, PORT, &["reset"])
        .stdout(contains("RESET OK"));

    // Make sure the values are not present anymore.
    run_client_cmd(&temp_dir, HOST, PORT, &["get", "key1"])
        .stdout(contains("GET NONE"));
    run_client_cmd(&temp_dir, HOST, PORT, &["get", "key2"])
        .stdout(contains("GET NONE"));

    // Restart the server and check again.
    drop(server_guard);
    let _server_guard = run_server(&temp_dir, "kvs", HOST, PORT);

    run_client_cmd(&temp_dir, HOST, PORT, &["get", "key1"])
        .stdout(contains("GET NONE"));
    run_client_cmd(&temp_dir, HOST, PORT, &["get", "key2"])
        .stdout(contains("GET NONE"));
}
