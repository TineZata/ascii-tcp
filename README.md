# ascii-tcp

Tokio-based ASCII request/response client and server over TCP.

## Architecture

- `client::Context` executes a single command or a batch of commands (a `Payload`),
  optionally retrying forever on transient connection failures, and reports
  connect/disconnect transitions per endpoint via callbacks.
- `exchange_line` is the low-level, one-request-per-call client function `Context` is
  built on top of; it opens a connection, writes one framed request, reads one framed
  response, and returns.
- `parse_backend_payload` / `parse_backend_command` / `parse_backend_batch` parse JSON
  into a `Payload` (`Single` or `Batch`), a single `Command`, or `BatchCommands`
  respectively. Every parsed value carries its own `HardwareProtocol` (host, port,
  line ending, timeouts), so a batch can fan out to multiple different endpoints.
- `server::Server` owns the TCP listener and one task per connection; `ServerHandle` is
  a cloneable address + shutdown handle that stays valid after `Server` is dropped or
  stopped.
- LF, CRLF, and CR line framing are supported by both client and server.

## Client

```rust,no_run
use ascii_tcp::{client::Context, parse_backend_payload};

# async fn run() -> Result<(), ascii_tcp::AsciiTcpError> {
let payload = parse_backend_payload(r#"{
	"id": "temperature",
	"request": "TEMP?",
	"protocol": {
		"type": "ascii-tcp",
		"host": "127.0.0.1",
		"port": 4000,
		"line_ending": "lf"
	}
}"#)?;

let context = Context::from_env();
context.on_connected(|endpoint| println!("connected: {endpoint}"));
context.on_disconnected(|endpoint, reason| println!("disconnected: {endpoint} ({reason})"));

// Runs once and returns whatever error occurred (no retrying).
let result = context.execute_payload_once(&payload).await?;

// Retries on transient errors (connect/IO timeout, I/O error, EOF) according to
// `ClientConfig`; never returns on a retryable error unless `retry_forever` is false.
let result = context.execute_payload(&payload).await?;
# let _ = result;
# Ok(())
# }
```

`parse_backend_payload` also accepts a `{"commands": [...]}` batch document (see
[examples/backend-config.json](examples/backend-config.json)); `execute_payload`/`execute_payload_once`
run every command in the batch sequentially against its own endpoint and return a
`CommandResultPayload::Batch`.

Client environment variables (`Context::from_env`, `ClientConfig::from_env`):

- `ASCII_TCP_RETRY_MS` — retry interval in milliseconds. Defaults to `2000`.
- `ASCII_TCP_RETRY_FOREVER` — defaults to `true`; set it to `false` or `no` (case
  insensitive) to make `execute_payload` behave like `execute_payload_once` and stop
  retrying after the first failure.

`on_connected` fires once per endpoint the first time it transitions from
unknown/disconnected to connected; it does not re-fire on every successful call.
`on_disconnected` fires on every failed attempt, with the error's `Display` text as the
reason.

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

`ServerContext` owns both the configuration and the request `Handler`
(`ServerContext::default().with_handler(...)`, then `Server::start(ctx)`); the default
handler echoes the request. `Server::start_from_env` builds a `ServerContext` via
`ServerContext::from_env`, which currently reads:

- `ASCII_TCP_SERVER_ADDR` — bind address as `IP:PORT`. Defaults to `127.0.0.1:4000`.
- `ASCII_TCP_SERVER_IO_TIMEOUT_MS` — defaults to `2000`.
- `ASCII_TCP_SERVER_MAX_LINE_LENGTH` — defaults to `8192` bytes; longer request lines
  are rejected with `AsciiTcpError::RequestTooLong`.

Run `cargo run --example backend_server -- examples/backend-config.json` to start the
localhost backend server, then in a separate terminal run
`cargo run --example backend -- examples/backend-config.json` to send commands against them.

## Typed values are an example concern, not a library one

`examples/backend_server.rs` demonstrates hosting multiple simulated servers — one per
unique endpoint in the config — each responding with a slowly changing value, so that
`examples/backend.rs` has a live target to poll.

- `Command.request` / `CommandResult.response` are plain `String`s in the library —
  the crate models *how to reach a device* (`HardwareProtocol`, line ending, timeouts),
  not *what shape its reply takes*.

