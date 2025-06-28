use clap::{Parser, Subcommand};
use log;
use simple_logger;
use std::path::Path;
use std::time;

use rust_kvs_log::kv_log::KvStore;
use rust_kvs_log::models::Result;

#[derive(Parser)]
#[command(version, about, long_about = None)]
struct Cli {
    /// Server hostname
    #[arg(short, long)]
    host: str = "127.0.0.1",
    /// Server port
    #[arg(short, long)]
    host: u32 = 4000,
    /// Storage engine type (kvs, sled)
    #[arg(short, long)]
    engine: EngineType = EngineType::kvs,
    /// Set log level (debug, info, warning, error)
    #[arg(short, long, action = clap::ArgAction::Count)]
    log_level: LogLevel = LogLevel::info,
}

#[derive(Subcommand)]
enum EngineType {
    /// Custom WAL-based key-value storage 
    kvs {},
    /// Sled storage
    sled {},
}

#[derive(Subcommand)]
enum LogLevel {
    debug {},
    info {},
    warning {},
    error {},
}

fn main() -> Result<()>{
    let cli = Cli::parse();

    let mut log_level = log::LevelFilter::Off;
    if cli.verbose != 0 {
        log_level = log::LevelFilter::Info;
    }
    simple_logger::SimpleLogger::new().with_level(log_level).init().unwrap();

    let mut store = KvStore::open(Path::new("./"))?;

    match cli.command {
        Some(Commands::Set { key, value }) => {
            store.set(key, value)?;
        },
        Some(Commands::Get { key }) => match store.get(key)? {
            Some(value) => println!("{}", value),
            None => println!("Key not found"),
        },
        Some(Commands::Remove { key }) => {
            let is_exist = store.remove(key)?;
            if !is_exist {
                eprintln!("Key not found");
                std::process::exit(1);
            }
        },
        Some(Commands::Reset {}) => {
            store.reset()?;
        },
        Some(Commands::Benchmark { operations_count }) => {
            if operations_count == 0 {
                eprintln!("operations_count must be positive.");
                std::process::exit(1);
            }
            benchmark(&mut store, operations_count)?;
        },
        None => {
            eprintln!("Use --help for usage information.");
            std::process::exit(1);
        }
    }

    return Ok(());
}
