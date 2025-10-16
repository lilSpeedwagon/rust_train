use clap::{Parser, ValueEnum};
use log;
use simple_logger;

use rust_kvs_server::{models, server, storage};

#[derive(Parser)]
#[command(version, about, long_about = None)]
struct Cli {
    /// Server hostname
    #[arg(short = 'H', long, default_value = "127.0.0.1")]
    host: String,
    /// Server port
    #[arg(short = 'P', long, default_value = "4000")]
    port: u32,
    /// Storage engine type
    #[arg(short, long, default_value = "kvs")]
    engine: EngineType,
    /// Storage path
    #[arg(short, long, default_value = "./")]
    path: String,
    /// Set log level
    #[arg(short, long, default_value = "info")]
    log_level: LogLevel,
}

#[derive(Clone, ValueEnum)]
enum EngineType {
    /// Custom WAL-based key-value storage 
    Kvs,
    /// Sled storage
    Sled,
}

impl std::fmt::Display for EngineType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", match &self {
            EngineType::Kvs => "kvs",
            EngineType::Sled => "sled",
        })
    }
}

#[derive(Clone, ValueEnum)]
enum LogLevel {
    Debug,
    Info,
    Warning,
    Error,
}

fn main() -> models::Result<()>{
    let cli = Cli::parse();

    let log_level = match cli.log_level {
        LogLevel::Debug => log::LevelFilter::Debug,
        LogLevel::Info => log::LevelFilter::Info,
        LogLevel::Warning => log::LevelFilter::Warn,
        LogLevel::Error => log::LevelFilter::Error,
    };
    simple_logger::SimpleLogger::new().with_level(log_level).init().unwrap();

    log::info!("Starting server at {}:{} with {} engine at {}", cli.host, cli.port, cli.engine, cli.path);
    
    let storage_path = std::path::Path::new(&cli.path);
    let engine: Box<dyn storage::KVStorage> = match cli.engine {
        EngineType::Kvs => Box::new(storage::KvLogStorage::open(storage_path)?),
        EngineType::Sled => Box::new(storage::SledStorage::open(storage_path)?),
    };

    let mut server = server::KvsServer::new(engine);
    server.listen(cli.host, cli.port)?;

    return Ok(());
}
