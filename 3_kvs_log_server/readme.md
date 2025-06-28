# KVS Client-Server

A simple **log-based key value storage client and server** with command line interfaces. Client and server use a custom network protocol for communication.

All commands stored in append-only log files.
Storage maintains in-memory index storing pointers to value locations in log files. The log files grow up to
4.000.000 bytes in size and then the storage rotates write commands to the next file. To save disk space, complete files
are compacted automatically on rotation. Log file compaction preserves only the latest "set" commands for each key.

```
Usage: kvs.exe [COMMAND]

Commands:
  set        Set value `value` for the key `key`
  get        Get value for the key `key`
  remove     Remove the key `key`
  reset      Reset storage by removing all of the stored values
  benchmark  Benchmark storage operations speed by running many get and set operations
  help       Print this message or the help of the given subcommand(s)

Options:
  -v, --verbose...  Enable verbose output
  -h, --help        Print help
  -V, --version     Print version
```

Run with:

```
cargo run
```

Test with:

```
cargo test
```
