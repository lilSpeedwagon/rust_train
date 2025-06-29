use clap::{Parser, Subcommand, ValueEnum};
use log;
use simple_logger;
use std::path::Path;
use std::time;
use std::fmt::Display;

use rust_kvs_server::{Result, KvsServer};

#[derive(Parser)]
#[command(version, about, long_about = None)]
struct Cli {
    /// Server hostname
    #[arg(short = 'H', long, default_value = "127.0.0.1")]
    host: String,
    /// Server port
    #[arg(short, long, default_value = "4000")]
    port: u32,
    /// Storage engine type
    #[arg(short, long, default_value = "kvs")]
    engine: EngineType,
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

impl Display for EngineType {
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

fn main() -> Result<()>{
    let cli = Cli::parse();

    let log_level = match cli.log_level {
        LogLevel::Debug => log::LevelFilter::Debug,
        LogLevel::Info => log::LevelFilter::Info,
        LogLevel::Warning => log::LevelFilter::Warn,
        LogLevel::Error => log::LevelFilter::Error,
    };
    simple_logger::SimpleLogger::new().with_level(log_level).init().unwrap();

    println!("starting server at {}:{} with {} engine", cli.host, cli.port, cli.engine);

    let server = KvsServer::new();
    server.listen(cli.host, cli.port)?;

    return Ok(());
}
