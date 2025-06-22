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
    /// Benchmark storage operations speed by running many get and set operations
    Benchmark {
        /// Number of operations to run during the benchmark.
        operations_count: u32,
    },
}

fn benchmark(storage: &mut KvStore, operations_count: u32) -> Result<()> {
    let mut keys_to_insert = Vec::new();
    for i in 1..operations_count {
        keys_to_insert.push(format!("key{}", i).to_string());
    }

    let mut set_timings = Vec::new();
    let start_set_total = time::Instant::now();
    for key in &keys_to_insert {
        let key_to_set = key.clone();
        let single_set_start = time::Instant::now();
        storage.set(key_to_set, "value".to_string())?;
        set_timings.push(single_set_start.elapsed().as_millis());
    }
    let total_set_elapsed = start_set_total.elapsed().as_millis();

    let set_avg = set_timings.iter().sum::<u128>() / set_timings.len() as u128;
    let set_min = set_timings.iter().min().unwrap();
    let set_max = set_timings.iter().max().unwrap();
    print!("Running {} set commands. Total: {}ms. Avg {}ms; Min {}ms; Max {}ms.\n", operations_count, total_set_elapsed, set_avg, set_min, set_max);

    let mut get_timings = Vec::new();
    let start_get_total = time::Instant::now();
    for key in &keys_to_insert {
        let key_to_get = key.clone();
        let single_get_start = time::Instant::now();
        storage.get(key_to_get)?;
        get_timings.push(single_get_start.elapsed().as_millis());
    }
    let total_get_elapsed = start_get_total.elapsed().as_millis();

    let get_avg = get_timings.iter().sum::<u128>() / get_timings.len() as u128;
    let get_min = get_timings.iter().min().unwrap();
    let get_max = get_timings.iter().max().unwrap();
    print!("Running {} get commands. Total: {}ms. Avg {}ms; Min {}ms; Max {}ms.\n", operations_count, total_get_elapsed, get_avg, get_min, get_max);

    Ok(())
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
