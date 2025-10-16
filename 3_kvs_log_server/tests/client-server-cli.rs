use assert_cmd::prelude::*;
use predicates::str::{contains, is_empty};
use std::fs::{self, File};
use std::process::Command;
use std::sync::mpsc::{self, SyncSender};
use std::thread::{self, scope};
use std::time::Duration;
use tempfile::TempDir;
use scopeguard;


const HOST: &str = "127.0.0.1";
const PORT: u32 = 4009;

struct ServerGuard {
    sender: SyncSender<()>,
    handler: Option<thread::JoinHandle<()>>,
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
    let (sender, receiver) = mpsc::sync_channel::<()>(0);
    let mut server = Command::cargo_bin("kvs_server").unwrap();
    let mut child = server
        .args(&["--engine", engine, "--host", host, "--port", &port.to_string()])
        .current_dir(&dir)
        .spawn()
        .unwrap();

    let handle = thread::spawn(move || {
        let _ = receiver.recv(); // wait for main thread to finish
        child.kill().expect("server exited before killed");
        print!("kill server");
    });
    thread::sleep(Duration::from_secs(1));
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
