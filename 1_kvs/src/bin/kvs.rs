use clap::{Parser, Subcommand};
use rust_kvs::kv::KvStore;

#[derive(Parser)]
#[command(version, about, long_about = None)]
struct Cli {
    /// Command to run
    #[command(subcommand)]
    command: Option<Commands>,
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
}

fn main() {
    let cli = Cli::parse();

    let mut store = KvStore::default();

    match cli.command {
        Some(Commands::Set { key, value }) => {
            store.set(key, value);
        }
        Some(Commands::Get { key }) => match store.get(key) {
            Some(value) => println!("{}", value),
            None => println!("Key not found"),
        },
        Some(Commands::Remove { key }) => {
            store.remove(key);
        }
        None => {
            eprintln!("Use --help for usage information.");
            std::process::exit(1);
        }
    }
}
