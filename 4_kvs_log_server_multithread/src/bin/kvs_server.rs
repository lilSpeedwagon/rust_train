use clap;
use clap::{Parser, ValueEnum};
use log;
use num_cpus;
use simple_logger;

use rust_kvs_server::{models, server, storage};

#[derive(clap::Parser)]
#[command(version, about, long_about = None)]
struct Cli {
    /// Server hostname
    #[arg(short = 'H', long, default_value = "127.0.0.1")]
    host: String,
    /// Server port
    #[arg(short = 'P', long, default_value = "4000")]
    port: u32,
    /// Storage path
    #[arg(short, long, default_value = "./")]
    path: String,
    /// Set log level
    #[arg(short, long, default_value = "info")]
    log_level: LogLevel,
    /// Server handlers thread pool size. Set to 0 for auto-selection.
    #[arg(short, long, default_value_t = 0)]
    thread_pool_size: usize,
}

#[derive(Clone, ValueEnum)]
enum LogLevel {
    Debug,
    Info,
    Warning,
    Error,
}

fn main() -> models::Result<()> {
    let cli = Cli::parse();

    let log_level = match cli.log_level {
        LogLevel::Debug => log::LevelFilter::Debug,
        LogLevel::Info => log::LevelFilter::Info,
        LogLevel::Warning => log::LevelFilter::Warn,
        LogLevel::Error => log::LevelFilter::Error,
    };
    simple_logger::SimpleLogger::new().with_level(log_level).init().unwrap();

    log::info!("Starting server at {}:{} with at {}", cli.host, cli.port, cli.path);

    let mut thread_pool_size = cli.thread_pool_size;
    if thread_pool_size == 0 {
        thread_pool_size = num_cpus::get() * 2 + 1;
    }
    
    let storage_path = std::path::Path::new(&cli.path);
    let engine = storage::KvLogStorage::open(storage_path)?;

    let mut server = server::KvsServer::new(engine, thread_pool_size);
    server.listen(cli.host, cli.port)?;

    return Ok(());
}
