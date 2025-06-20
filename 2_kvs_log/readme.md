# KVS

A simple **log-based key value storage** with command line interface. All commands stored in append-only log files.
Storage maintains in-memory index storing pointers to value locations in log files.

```
Usage: kvs.exe [COMMAND]

Commands:
  set     Set value `value` for the key `key`
  get     Get value for the key `key`
  remove  Remove the key `key`
  help    Print this message or the help of the given subcommand(s)

Options:
  -h, --help     Print help
  -V, --version  Print version
```

Run with:

```
cargo run
```

Test with:

```
cargo test
```
