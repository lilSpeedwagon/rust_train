use clap::{Parser, Subcommand};
use log;
use simple_logger;
use std::path::Path;

use rust_kvs_server::kv_log::KvStore;
use rust_kvs_server::models::Result;
use rust_kvs_server::KvsClient;

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
    Reset {},
}

fn main() -> Result<()>{
    let cli = Cli::parse();

    let mut log_level = log::LevelFilter::Off;
    if cli.verbose != 0 {
        log_level = log::LevelFilter::Debug;
    }
    simple_logger::SimpleLogger::new().with_level(log_level).init().unwrap();

    let data = "hello".as_bytes().to_vec();

    let mut client = KvsClient::new();
    client.connect(String::from("127.0.0.1"), 4000)?;
    let response = client.send(data)?;
    log::info!("{}", String::from_utf8(response)?);

    //let mut store = KvStore::open(Path::new("./"))?;

    // match cli.command {
    //     Some(Commands::Set { key, value }) => {},
    //     Some(Commands::Get { key }) => {},
    //     Some(Commands::Remove { key }) => {},
    //     Some(Commands::Reset {}) => {},
    //     None => {
    //         eprintln!("Use --help for usage information.");
    //         std::process::exit(1);
    //     }
    // }

    return Ok(());
}
