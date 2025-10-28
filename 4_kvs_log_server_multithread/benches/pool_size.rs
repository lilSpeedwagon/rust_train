use assert_cmd::prelude::*;
use criterion::{BenchmarkId, criterion_group, criterion_main, Criterion, PlotConfiguration};
use tempfile;

use rust_kvs_server::{client, models};

const HOST: &str = "localhost";
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


fn run_server(dir: &tempfile::TempDir, pool_type: &str, pool_size: usize, host: &str, port: u32) -> ServerGuard {
    let (sender, receiver) = std::sync::mpsc::sync_channel::<()>(0);
    let mut server = std::process::Command::cargo_bin("kvs_server").unwrap();
    let mut child = server
        .args(&["--thread-pool", pool_type, "--thread-pool-size", &pool_size.to_string(),
                "--host", host, "--port", &port.to_string(), "-l", "warning"])
        .current_dir(&dir)
        .spawn()
        .unwrap();

    let handle = std::thread::spawn(move || {
        let _ = receiver.recv(); // wait for main thread to finish
        child.kill().expect("server exited before killed");
    });
    std::thread::sleep(std::time::Duration::from_millis(500));
    ServerGuard{ sender: sender, handler: Some(handle) }
}


pub fn bench_set_pool_size(c: &mut Criterion) {
    let temp_dir = tempfile::TempDir::new().unwrap();
    let thread_nums = [1, 2, 4, 8, 16];
    let client_thread_count = 4;
    let values_count = 100;
    let values_per_clinet = values_count / client_thread_count;

    let mut group = c.benchmark_group("kvs set thread pool");
    group.sample_size(50);
    group.plot_config(PlotConfiguration::default());

    for thread_count in thread_nums.iter() {
        group.bench_with_input(
            BenchmarkId::from_parameter(thread_count),
            thread_count,
            |b, &thread_count| {
                let _server_guard = run_server(
                    &temp_dir, "shared", thread_count, &HOST.to_string(), PORT
                );

                b.iter(|| {
                    let mut client_threads = Vec::with_capacity(client_thread_count);
                    for client_idx in 0..client_thread_count {
                        let idx_range = values_per_clinet * client_idx..values_per_clinet * (client_idx + 1);
                        let thread = std::thread::spawn(|| {
                            let mut client = client::KvsClient::new();
                            for idx in idx_range {
                                let key = idx.to_string();
                                let value = format!("value_00000000000{}", idx);
                                let cmd = models::Command::Set { key: key, value: value };
                                client.connect(HOST.to_string(), PORT, std::time::Duration::from_secs(10)).unwrap();
                                let response = client.execute_one(cmd, false).unwrap();

                                assert!(response.commands.len() == 1);
                                assert!(*response.commands.first().unwrap() == models::ResponseCommand::Set{});
                            }
                        });
                        client_threads.push(thread);
                    }

                    for client_thread in client_threads {
                        client_thread.join().unwrap();
                    }
                });
            },
        );
    }
    group.finish();
}

pub fn bench_get_pool_size(c: &mut Criterion) {
    let temp_dir = tempfile::TempDir::new().unwrap();
    let thread_nums = [1, 2, 4, 8, 16];
    let client_thread_count = 4;
    let values_count = 100;
    let values_per_clinet = values_count / client_thread_count;

    let mut group = c.benchmark_group("kvs get thread pool");
    group.sample_size(50);
    group.plot_config(PlotConfiguration::default());

    for thread_count in thread_nums.iter() {
        group.bench_with_input(
            BenchmarkId::from_parameter(thread_count),
            thread_count,
            |b, &thread_count| {
                let _server_guard = run_server(
                    &temp_dir, "shared", thread_count, &HOST.to_string(), PORT
                );

                let mut client = client::KvsClient::new();
                let mut commands = Vec::with_capacity(values_count);
                client.connect(HOST.to_string(), PORT, std::time::Duration::from_secs(10)).unwrap();
                for idx in 0..values_count {
                    let key = idx.to_string();
                    let value = format!("value_00000000000{}", idx);
                    let cmd = models::Command::Set { key: key, value: value };
                    commands.push(cmd);
                }
                let response = client.execute(commands, false).unwrap();
                assert!(response.commands.len() == values_count);

                b.iter(|| {
                    let mut client_threads = Vec::with_capacity(client_thread_count);
                    for client_idx in 0..client_thread_count {
                        let idx_range = values_per_clinet * client_idx..values_per_clinet * (client_idx + 1);
                        let thread = std::thread::spawn(|| {
                            let mut client = client::KvsClient::new();
                            for idx in idx_range {
                                let key = idx.to_string();
                                let expected_value = format!("value_00000000000{}", idx);
                                let cmd = models::Command::Get { key: key };
                                client.connect(HOST.to_string(), PORT, std::time::Duration::from_secs(10)).unwrap();
                                let response = client.execute_one(cmd, false).unwrap();

                                assert!(response.commands.len() == 1);
                                assert!(
                                    *response.commands.first().unwrap() ==
                                    models::ResponseCommand::Get{value: Some(expected_value)}
                                );
                            }
                        });
                        client_threads.push(thread);
                    }

                    for client_thread in client_threads {
                        client_thread.join().unwrap();
                    }
                });
            },
        );
    }
    group.finish();
}

criterion_group!(
    benches,
    bench_set_pool_size,
    bench_get_pool_size,
);
criterion_main!(benches);
