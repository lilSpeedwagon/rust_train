use clap::{Parser, Subcommand};
use log;
use simple_logger;
use std::time::Duration;

use rust_kvs_async_server::{async_client, models};

#[derive(Parser)]
#[command(version, about = "Async KVS Client", long_about = None)]
struct Cli {
    /// Server hostname
    #[arg(short = 'H', long, default_value = "127.0.0.1")]
    host: String,
    /// Server port
    #[arg(short = 'P', long, default_value = "4000")]
    port: u32,
    /// Request timeout in seconds
    #[arg(short = 't', long, default_value = "10")]
    timeout: u64,
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Set a key-value pair
    Set {
        /// Key to set
        key: String,
        /// Value to set
        value: String,
    },
    /// Get a value by key
    Get {
        /// Key to get
        key: String,
    },
    /// Remove a key
    Remove {
        /// Key to remove
        key: String,
    },
    /// Reset/clear all data
    Reset,
    /// Check server health
    Health,
    /// Batch set multiple key-value pairs
    BatchSet {
        /// Key-value pairs in format key1=value1 key2=value2
        #[arg(value_parser = parse_key_val)]
        pairs: Vec<(String, String)>,
    },
    /// Batch get multiple keys
    BatchGet {
        /// Keys to get
        keys: Vec<String>,
    },
}

fn parse_key_val(s: &str) -> Result<(String, String), String> {
    let parts: Vec<&str> = s.splitn(2, '=').collect();
    if parts.len() != 2 {
        return Err(format!("Invalid key=value pair: {}", s));
    }
    Ok((parts[0].to_string(), parts[1].to_string()))
}

#[tokio::main]
async fn main() -> models::Result<()> {
    simple_logger::SimpleLogger::new()
        .with_level(log::LevelFilter::Info)
        .init()
        .unwrap();

    let cli = Cli::parse();

    let client = async_client::AsyncKvsClient::new(
        cli.host.clone(),
        cli.port,
        Duration::from_secs(cli.timeout),
    )?;

    match cli.command {
        Commands::Set { key, value } => {
            client.set(key.clone(), value).await?;
            println!("Key '{}' set successfully", key);
        }
        Commands::Get { key } => {
            let value = client.get(key.clone()).await?;
            match value {
                Some(v) => println!("{}", v),
                None => {
                    eprintln!("Key not found");
                    std::process::exit(1);
                }
            }
        }
        Commands::Remove { key } => {
            let existed = client.remove(key.clone()).await?;
            if existed {
                println!("Key '{}' removed successfully", key);
            } else {
                eprintln!("Key not found");
                std::process::exit(1);
            }
        }
        Commands::Reset => {
            client.reset().await?;
            println!("Storage reset successfully");
        }
        Commands::Health => {
            let healthy = client.health_check().await?;
            if healthy {
                println!("Server is healthy");
            } else {
                eprintln!("Server is not responding");
                std::process::exit(1);
            }
        }
        Commands::BatchSet { pairs } => {
            client.batch_set(pairs.clone()).await?;
            println!("{} key(s) set successfully", pairs.len());
        }
        Commands::BatchGet { keys } => {
            let results = client.batch_get(keys).await?;
            for (key, value) in results {
                match value {
                    Some(v) => println!("{}: {}", key, v),
                    None => println!("{}: (not found)", key),
                }
            }
        }
    }

    Ok(())
}
