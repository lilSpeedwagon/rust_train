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
    /// Server hostname
    #[arg(short = 'H', long, default_value = "127.0.0.1")]
    host: String,
    /// Server port
    #[arg(short, long, default_value = "4000")]
    port: u32,
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

    let command = match cli.command {
        Some(Commands::Set { key, value }) => models::Command::Set { key: key, value: value },
        Some(Commands::Get { key }) => models::Command::Get { key: key },
        Some(Commands::Remove { key }) => models::Command::Remove { key: key },
        Some(Commands::Reset {}) => models::Command::Reset {},
        None => {
            eprintln!("Use --help for usage information.");
            std::process::exit(1);
        }
    };

    let mut client = KvsClient::new();
    match client.connect(cli.host, cli.port, timeout) {
        Ok(_) => {},
        Err(err) => {
            eprintln!("Failed to connect: {}", err);
            std::process::exit(2);
        },
    }
    
    let exec_result = client.execute_one(command, false);
    if exec_result.is_err() {
        eprintln!("Failed to handle request: {}", exec_result.err().unwrap());
        std::process::exit(3);
    }

    let response = exec_result.unwrap();
    match response.commands.first() {
        Some(response_command) => {
            match response_command {
                models::ResponseCommand::Set {} => { log::info!("SET OK"); },
                models::ResponseCommand::Remove {} => { log::info!("REMOVE OK"); },
                models::ResponseCommand::Reset {} => { log::info!("RESET OK"); },
                models::ResponseCommand::Get { value } => {
                    match value {
                        Some(val) => log::info!("GET OK {}", val),
                        None => log::info!("GET NONE"),
                    }
                    
                },
            }
        },
        None => {
            eprintln!("Unable to get the server response");
            std::process::exit(4);
        }
    }

    return Ok(());
}
