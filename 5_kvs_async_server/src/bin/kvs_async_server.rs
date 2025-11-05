use clap::{Parser, ValueEnum};
use log;
use simple_logger;

use rust_kvs_async_server::{models, async_server, async_storage};

#[derive(Parser)]
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
}

#[derive(Clone, ValueEnum)]
enum LogLevel {
    Debug,
    Info,
    Warning,
    Error,
}

#[tokio::main]
async fn main() -> models::Result<()> {
    let cli = Cli::parse();

    let log_level = match cli.log_level {
        LogLevel::Debug => log::LevelFilter::Debug,
        LogLevel::Info => log::LevelFilter::Info,
        LogLevel::Warning => log::LevelFilter::Warn,
        LogLevel::Error => log::LevelFilter::Error,
    };
    simple_logger::SimpleLogger::new()
        .with_level(log_level)
        .init()
        .unwrap();

    log::info!(
        "Starting async server at {}:{} with storage at {}",
        cli.host,
        cli.port,
        cli.path
    );

    let storage_path = std::path::Path::new(&cli.path);
    let storage = async_storage::AsyncKvStorage::open(storage_path).await?;

    let server = async_server::AsyncKvsServer::new(storage);
    server.listen(cli.host, cli.port).await?;

    Ok(())
}
