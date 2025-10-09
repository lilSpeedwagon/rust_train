# KVS Client-Server

A simple **log-based key value storage client and server** with command line interfaces. Client and server use a custom network protocol for communication.

All commands stored in append-only log files.
Storage maintains in-memory index storing pointers to value locations in log files. The log files grow up to
4.000.000 bytes in size and then the storage rotates write commands to the next file. To save disk space, complete files
are compacted automatically on rotation. Log file compaction preserves only the latest "set" commands for each key.

## Server

A simple server interface over a KVS engine.

```
Usage: kvs_server [OPTIONS]

Options:
  -H, --host <HOST>
          Server hostname
          
          [default: 127.0.0.1]

  -p, --port <PORT>
          Server port
          
          [default: 4000]

  -e, --engine <ENGINE>
          Storage engine type
          
          [default: kvs]

          Possible values:
          - kvs:  Custom WAL-based key-value storage
          - sled: Sled storage

  -l, --log-level <LOG_LEVEL>
          Set log level
          
          [default: info]
          [possible values: debug, info, warning, error]

  -h, --help
          Print help (see a summary with '-h')

  -V, --version
          Print version
```

Run in the dev mode with:

```
cargo run --bin kvs_server -- <options>
```

Test with:

```
cargo test
```

## Client

A simple KVS Server client executes a single command at a time as a command line tool and then exits.

```
Usage: kvs_client [OPTIONS] [COMMAND]

Commands:
  set     Set value `value` for the key `key`
  get     Get value for the key `key`
  remove  Remove the key `key`
  reset   Reset storage by removing all of the stored values
  help    Print this message or the help of the given subcommand(s)

Options:
  -l, --log-level <LOG_LEVEL>        Set log level [default: info] [possible values: debug, info, warning, error]
  -r, --read-timeout <READ_TIMEOUT>  Read timeout in seconds [default: 30]
  -h, --help                         Print help
  -V, --version                      Print version
```

Run in the dev mode with:

```
cargo run --bin kvs_client -- <options>
```

Test with:

```
cargo test
```

## Communication Protocol

_TBD_
