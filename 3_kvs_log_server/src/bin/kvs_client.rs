use core::f32;
use std::time;

use clap::{Parser, Subcommand, ValueEnum};
use log;
use simple_logger;

use rust_kvs_server::models::{self, Result};
use rust_kvs_server::KvsClient;

#[derive(Parser)]
#[command(version, about, long_about = None)]
struct Cli {
    /// Command to run
    #[command(subcommand)]
    command: Option<Commands>,
    /// Set log level
    #[arg(short, long, default_value = "info")]
    log_level: LogLevel,
    /// Read timeout in seconds
    #[arg(short, long, default_value = "30")]
    read_timeout: f32,
}

#[derive(Subcommand)]
enum Commands {
    /// Set value `value` for the key `key`
    Set {
        /// Key to set
        key: String,
        /// Value to set for the key
        value: String,
    },
    /// Get value for the key `key`
    Get {
        /// Key to get the value for
        key: String,
    },
    /// Remove the key `key`
    Remove {
        /// Key to remove
        key: String,
    },
    /// Reset storage by removing all of the stored values
    Reset {},
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
    let timeout = time::Duration::from_secs_f32(cli.read_timeout);

    let mut client = KvsClient::new();
    client.connect(String::from("127.0.0.1"), 4000, timeout)?;

    let command = match cli.command {
        Some(Commands::Set { key, value }) => models::Command::Set { key: key, value: value },
        Some(Commands::Get { key }) => models::Command::Get { key: key },
        Some(Commands::Remove { key }) => models::Command::Remove { key: key },
        Some(Commands::Reset {}) => {
            eprintln!("Not implemented.");
            std::process::exit(1);
        },
        None => {
            eprintln!("Use --help for usage information.");
            std::process::exit(1);
        }
    };
    client.execute_one(command, true)?;

    return Ok(());
}
