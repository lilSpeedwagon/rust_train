# Async Key-Value Storage Server

A high-performance async web server for key-value storage built with Rust, Tokio, and Axum.

## Features

- **Async/Await**: Built on Tokio for efficient concurrent request handling
- **RESTful HTTP API**: Clean REST interface for all operations
- **Persistent Storage**: Log-based storage with automatic compaction
- **Batch Operations**: Support for batch set/get operations
- **CORS Enabled**: Cross-origin requests supported
- **HTTP Tracing**: Built-in request/response logging

## Architecture

- **Server Framework**: Axum web framework
- **Runtime**: Tokio async runtime
- **Storage**: Log-based key-value storage with RwLock for thread-safety
- **Client**: Async HTTP client using reqwest

## API Endpoints

### Health Check
```
GET /health
```
Returns server health status.

### Get Value
```
GET /api/keys/:key
```
Retrieve the value for a given key.

**Response:**
```json
{
  "key": "mykey",
  "value": "myvalue"
}
```

### Set Value
```
PUT /api/keys/:key
```
Set a value for a key.

**Request Body:**
```json
{
  "value": "myvalue"
}
```

or simply:
```json
"myvalue"
```

### Set Value (Alternative)
```
POST /api/keys
```
**Request Body:**
```json
{
  "key": "mykey",
  "value": "myvalue"
}
```

### Delete Key
```
DELETE /api/keys/:key
```
Remove a key from storage.

**Response:**
```json
{
  "key": "mykey",
  "existed": true
}
```

### Reset Storage
```
POST /api/reset
```
Clear all data from storage.

### Batch Set
```
POST /api/batch/set
```
**Request Body:**
```json
{
  "items": [
    {"key": "key1", "value": "value1"},
    {"key": "key2", "value": "value2"}
  ]
}
```

### Batch Get
```
POST /api/batch/get
```
**Request Body:**
```json
{
  "keys": ["key1", "key2"]
}
```

**Response:**
```json
{
  "items": [
    {"key": "key1", "value": "value1"},
    {"key": "key2", "value": null}
  ]
}
```

## Usage

### Server

```bash
# Start server with default settings (127.0.0.1:4000)
cargo run --bin kvs_async_server

# Custom host and port
cargo run --bin kvs_async_server -- --host 0.0.0.0 --port 8080

# Specify storage path
cargo run --bin kvs_async_server -- --path /path/to/storage

# Set log level
cargo run --bin kvs_async_server -- --log-level debug
```

### Client CLI

```bash
# Set a key
cargo run --bin kvs_async_client -- set mykey myvalue

# Get a key
cargo run --bin kvs_async_client -- get mykey

# Remove a key
cargo run --bin kvs_async_client -- remove mykey

# Reset storage
cargo run --bin kvs_async_client -- reset

# Check server health
cargo run --bin kvs_async_client -- health

# Batch set
cargo run --bin kvs_async_client -- batch-set key1=value1 key2=value2

# Batch get
cargo run --bin kvs_async_client -- batch-get key1 key2

# Custom server
cargo run --bin kvs_async_client -- --host 192.168.1.100 --port 8080 get mykey
```

### Example with curl

```bash
# Set a value
curl -X PUT http://localhost:4000/api/keys/name \
  -H "Content-Type: application/json" \
  -d '{"value": "Alice"}'

# Get a value
curl http://localhost:4000/api/keys/name

# Delete a key
curl -X DELETE http://localhost:4000/api/keys/name

# Batch operations
curl -X POST http://localhost:4000/api/batch/set \
  -H "Content-Type: application/json" \
  -d '{"items": [{"key": "user1", "value": "Alice"}, {"key": "user2", "value": "Bob"}]}'

curl -X POST http://localhost:4000/api/batch/get \
  -H "Content-Type: application/json" \
  -d '{"keys": ["user1", "user2"]}'
```

## Performance

The async architecture provides:
- **Concurrent Request Handling**: Tokio efficiently handles thousands of concurrent connections
- **Non-blocking I/O**: Async operations don't block the runtime
- **Efficient Resource Usage**: Share storage across requests with RwLock
- **Batch Operations**: Reduce round-trips with batch API calls

## Comparison with Previous Versions

1. **`4_kvs_log_server_multithread`**: Used thread pool with synchronous I/O
2. **`5_kvs_async_server`** (this): Uses Tokio async runtime with HTTP REST API
   - Better scalability for I/O-bound workloads
   - Modern REST API instead of custom binary protocol
   - Easier to integrate with web applications
   - Built-in CORS and request tracing

## Dependencies

- `tokio`: Async runtime
- `axum`: Web framework
- `tower-http`: Middleware (CORS, tracing)
- `reqwest`: HTTP client
- `serde` / `serde_json`: JSON serialization
- `clap`: CLI argument parsing
- `log` / `simple_logger`: Logging

## Building

```bash
cargo build --release
```

## Testing

```bash
# Run the server in one terminal
cargo run --bin kvs_async_server

# In another terminal, test with the client
cargo run --bin kvs_async_client -- set test "hello world"
cargo run --bin kvs_async_client -- get test
cargo run --bin kvs_async_client -- remove test
```

## License

This project is part of a Rust learning series demonstrating different approaches to building networked key-value storage systems.
