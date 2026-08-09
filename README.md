# ascii-tcp

Tokio-based ASCII request/response client and server over TCP.

## Architecture

- `client::Context` executes single or batched backend payloads, retries transient failures, and reports endpoint connection transitions.
- `exchange_line` remains the low-level one-request client API.
- `server::Server` owns the listener and connection tasks; `ServerHandle` is a cloneable address and shutdown handle.
- LF, CRLF, and CR framing are supported by both client and server.

## Client

```rust,no_run
use ascii_tcp::{client::Context, parse_backend_payload};

# async fn run() -> Result<(), ascii_tcp::AsciiTcpError> {
let payload = parse_backend_payload(r#"{
	"id": "temperature",
	"request": "READ TEMP",
	"protocol": {
		"type": "ascii-tcp",
		"host": "127.0.0.1",
		"port": 4000,
		"line_ending": "lf"
	}
}"#)?;

let context = Context::from_env();
let result = context.execute_payload_once(&payload).await?;
# Ok(())
# }
```

Client environment variables:

- `ASCII_TCP_RETRY_MS` defaults to `2000`.
- `ASCII_TCP_RETRY_FOREVER` defaults to `true`; set it to `false` or `no` to disable retries.

## Server

```rust,no_run
use ascii_tcp::Server;

# async fn run() -> Result<(), ascii_tcp::AsciiTcpError> {
let server = Server::start_isolated(|request, peer| async move {
		Ok(format!("{peer} sent {request}"))
}).await?;

println!("listening on {}", server.local_addr());
server.stop_drop().await?;
# Ok(())
# }
```

`Server::start_from_env` reads `ASCII_TCP_SERVER_ADDR`, `ASCII_TCP_SERVER_IO_TIMEOUT_MS`, and `ASCII_TCP_SERVER_MAX_LINE_LENGTH`.

Run `cargo run --example standalone` for an in-process client/server exchange, or `cargo run --example backend -- examples/backend-config.json` against an external TCP endpoint.
