use clap::{Parser, Subcommand};
use log;
use simple_logger;
use std::path::Path;

use rust_kvs_log::kv_log::KvStore;
use rust_kvs_log::models::Result;

#[derive(Parser)]
#[command(version, about, long_about = None)]
struct Cli {
    /// Command to run
    #[command(subcommand)]
    command: Option<Commands>,
    /// Enable verbose output
    #[arg(short, long, action = clap::ArgAction::Count)]
    verbose: u8,
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
    Reset {}
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
        None => {
            eprintln!("Use --help for usage information.");
            std::process::exit(1);
        }
    }

    return Ok(());
}
