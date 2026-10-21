# ascii-tcp

Tokio-based ASCII request/response client and server over TCP.

## Typical use

Most applications (e.g. a control-panel GUI polling a device such as a PLC) follow one pattern:

1. Describe each device channel as a [`ChannelConfig`](#channelconfig) (usually a block in the app's JSON config).
2. Share one [`Context`](#context) across the app; it keeps one connection per `host:port`.
3. **Read** by polling `read_command` with `Context::execute_command_matching`, then extract the value with a [`ResponseTemplate`](#response-templates).
4. **Write** with `Context::send_command` (or `execute_command_matching` to await an acknowledgement).
5. For tests and simulators, host a fake device with [`Server`](#server).

| Most used | Purpose |
|-----------|---------|
| `ChannelConfig` | Deserialisable per-channel settings; `command(request)` builds what the client sends |
| `Context` | Shared client: `execute_command_matching`, `send_command`, `on_connected` / `on_disconnected` |
| `ResponseTemplate` | printf-style value extraction from reply lines (`"HV=%d"`) |
| `Server` / `ServerContext` | Line-oriented server driven by an async handler |
| *Less common* | `Payload` / `execute_json` for JSON batches; `exchange_line` / `send_line` one-shot helpers |

LF, CRLF and CR line framing are supported by both client and server.

## Quick start

```rust,no_run
use std::sync::Arc;
use ascii_tcp::{ChannelConfig, Context, ResponseTemplate};

# async fn run() -> Result<(), Box<dyn std::error::Error>> {
// 1. Describe the channel.
let channel: ChannelConfig = serde_json::from_str(r#"{
    "host": "192.168.30.186", "port": 6000, "line_ending": "crlf",
    "read_command": "HV?", "read_response": "HV=%d",
    "write_command": "HV={value}", "io_timeout_ms": 5000
}"#)?;

// 2. One shared context for the whole app.
let context = Arc::new(Context::default());
context.on_connected(|endpoint| println!("connected: {endpoint}"));
context.on_disconnected(|endpoint, reason| println!("disconnected: {endpoint} ({reason})"));

// 3. Read: send the poll, skipping lines that are not the reply (banners, heartbeats).
let template = ResponseTemplate::compile(channel.read_response.as_deref().unwrap())?;
let reply = context
    .execute_command_matching(&channel.command("HV?"), |line| template.captures(line).is_ok())
    .await?;
let value = template.capture_first(&reply.response)?.as_i64();

// 4. Write: `{value}` in `write_command` is yours to substitute.
let command = channel.write_command.as_deref().unwrap().replace("{value}", "1");
context.send_command(&channel.command(command)).await?;
# let _ = value;
# Ok(())
# }
```

## `ChannelConfig`

Deserialises from JSON; only `host` and `port` are required. `command(request)` builds the
`Command` the client methods take; `protocol()` and `connection()` expose the wire-format
`HardwareProtocol` and low-level `AsciiTcpConfig`.

| Field | Default | Meaning |
|-------|---------|---------|
| `read_command` | none | Poll request; omit for write-only endpoints, `""` to only listen |
| `read_response` | none | printf-style template; the first conversion is the value |
| `write_command` | none | Template where `{value}` is replaced; otherwise the raw value is sent |
| `write_expects_response` | `true` | Set `false` for fire-and-forget writes |
| `line_ending` | `lf` | `lf`, `crlf` or `cr` |
| `min_poll_interval_ms` | `500` | Minimum poll interval |
| `connect_timeout_ms` / `io_timeout_ms` | `2000` | Timeouts |
| `scale` / `offset` | `1.0` / `0.0` | Applied to numeric values |
| `response_mode` | `text` | `number`, `bool`, `text`, or `presence` (any matching line is a pulse, value `1`) |

Typical example:
```json
  "host": "192.168.1.2",
  "port": 6000,
  "read_command": "TEMP?",
  "read_response": "TEMP=%f DegC",
  "line_ending": "crlf",
  "response_mode": "number",
  "min_poll_interval_ms": 500,
  "io_timeout_ms": 5000,
  "scale": 1.0,
  "offset": 0.0
```

The library carries these fields so your app can drive polling; scaling, polling cadence and
`{value}` substitution are applied by the caller.

```rust
use ascii_tcp::{AsciiResponseMode, ChannelConfig};

let cfg: ChannelConfig = serde_json::from_str(r#"{
    "host": "127.0.0.1", "port": 4000, "line_ending": "crlf",
    "read_command": "READ TEMP", "read_response": "TEMP=%f",
    "response_mode": "number", "scale": 0.1
}"#).unwrap();
assert_eq!(cfg.response_mode, AsciiResponseMode::Number);
let command = cfg.command("READ TEMP");
```

## `Context`

Each `host:port` shares a single connection, opened on first use and reused by every
request (and every clone of the `Context`). If it drops, the next request reconnects.
Create it with `Context::default()`, `Context::from_env()` or `Context::new(ClientConfig { .. })`.



| Method | Behaviour |
|--------|-----------|
| `execute_command_matching(&cmd, accept)` | Sends one command and returns the first reply line `accept` approves; no retry, so callers see failures and own their retry cadence |
| `send_command(&cmd)` | Sends without waiting for a reply |
| `on_connected` / `on_disconnected` | Per-endpoint callbacks; `on_disconnected` fires on every failed attempt with the error text |
| `execute_payload(&payload)` / `execute_payload_once(&payload)` / `execute_json(json)` | Run JSON-described commands, with or without retry (see [Payloads and batches](#payloads-and-batches)) |

Environment variables (`Context::from_env`, `ClientConfig::from_env`); they only affect the
`execute_payload*` / `execute_json` retry loop:

- `ASCII_TCP_RETRY_MS` — retry interval in milliseconds. Defaults to `2000`.
- `ASCII_TCP_RETRY_FOREVER` — defaults to `true`; `false` or `no` (case-insensitive) disables retrying.

### Unsolicited lines and fire-and-forget

```rust,no_run
use ascii_tcp::{ChannelConfig, Context};

# async fn run(context: Context) -> Result<(), ascii_tcp::AsciiTcpError> {
let channel: ChannelConfig = serde_json::from_str(r#"{ "host": "127.0.0.1", "port": 4000 }"#)?;

// Ignore banners/heartbeats until the wanted reply arrives (or `io_timeout` elapses).
let reply = context
    .execute_command_matching(&channel.command("TEMP?"), |line| line.starts_with("TEMP="))
    .await?;

// An empty request sends nothing and only waits for a matching line (e.g. a heartbeat).
let beat = context.execute_command_matching(&channel.command(""), |line| line == "ALIVE").await?;

// Commands the endpoint never acknowledges.
context.send_command(&channel.command("RESET")).await?;
# let _ = (reply, beat);
# Ok(())
# }
```

## Response templates

`ResponseTemplate` extracts typed fields from a reply. Specifiers: `%d %i %u` (integer),
`%f %e %g` (float), `%x %X` (hex), `%s` (text), `%%` (literal `%`); `%*d` matches and discards.
Matching is anchored, so a malformed reply yields `AsciiTcpError::ResponseMismatch`.

```rust
use ascii_tcp::{parse_first_value, parse_response_template, ResponseTemplate};

// Polling loops, compiles once and reuse forever.
// Capture only TEMP3 and ignore TEMP1 and TEMP2.
let template = ResponseTemplate::compile("TEMP1=%*f,TEMP2=%*f,TEMP3=%f").unwrap();
let captures = template.captures("TEMP1=16.7,TEMP2=32.3,TEMP3=25.5").unwrap();
assert_eq!(captures[0].as_f64(), Some(25.5));

// One-shot, compiles every call. Returns first value only. 
// Only use for items you want to read once.
let serial = parse_first_value("SerialNum=%d", "SerialNum=1644356").unwrap();
assert_eq!(serial.as_i64(), Some(1644356));

// One-shot, compiles every call. Returns first value only.
// Similar to `parse_first_value` but for multiple items.
let fields = parse_response_template("POS X=%d Y=%d MODE=%s", "POS X=10 Y=-4 MODE=auto").unwrap();
assert_eq!(fields[0].as_i64(), Some(10));
assert_eq!(fields[1].as_i64(), Some(-4));
assert_eq!(fields[2].as_str(), "auto");
```

`parse_response_template` and `parse_first_value` compile on every call, so prefer
`ResponseTemplate::compile` for polling loops.

## Server

Use it to simulate a device in tests or demos. The handler maps each request line to a reply line.

```rust,no_run
use ascii_tcp::{ChannelConfig, Server, ServerContext};

# async fn run(channel: ChannelConfig) -> Result<(), ascii_tcp::AsciiTcpError> {
// Serve the same endpoint a `ChannelConfig` describes (port, line ending, timeout).
let ctx = ServerContext {
    bind_addr: ([0, 0, 0, 0], channel.port).into(),
    line_ending: channel.line_ending,
    ..ServerContext::default() // default handler echoes the request
}
.with_handler(|request, _peer| async move {
    match request.as_str() {
        "HV?" => Ok("HV=1".to_string()),
        other => Ok(other.to_string()),
    }
});
let _server = Server::start(ctx).await?;

// Random localhost port, handy in tests.
let server = Server::start_isolated(|request, peer| async move {
    Ok(format!("{peer} sent {request}"))
}).await?;
println!("listening on {}", server.local_addr());

let handle = server.handle(); // cloneable; `handle.shutdown()` works after `server` is dropped
server.stop_drop().await?;    // shut down and wait for the task
# let _ = handle;
# Ok(())
# }
```

`Server::start_from_env(handler)` builds the context with `ServerContext::from_env`:

- `ASCII_TCP_SERVER_ADDR` — bind address as `IP:PORT`. Defaults to `127.0.0.1:4000`; an invalid value is an error.
- `ASCII_TCP_SERVER_IO_TIMEOUT_MS` — defaults to `2000`.
- `ASCII_TCP_SERVER_MAX_LINE_LENGTH` — defaults to `8192` bytes; longer request lines
  are rejected with `AsciiTcpError::RequestTooLong`.

## Payloads and batches

For JSON-driven, fire-and-collect use. `parse_backend_payload`, `parse_backend_command` and
`parse_backend_batch` turn JSON into a `Payload`, `Command` or `BatchCommands`. Each command
carries its own `HardwareProtocol` (`host`, `port`, `line_ending`, optional
`connect_timeout_ms` / `io_timeout_ms`), so a batch can span several endpoints. Batches run
sequentially, abort on the first failure, and return `CommandResultPayload::Batch`
(see [examples/backend-config.json](examples/backend-config.json)).

`execute_payload` retries transient errors (timeouts, I/O, EOF) every `retry_interval` while
`retry_forever` is set; `execute_payload_once` returns the first error; `execute_json` parses and
calls `execute_payload`.

```rust,no_run
use ascii_tcp::{parse_backend_payload, CommandResultPayload, Context};

# async fn run(context: Context) -> Result<(), ascii_tcp::AsciiTcpError> {
let payload = parse_backend_payload(r#"{ "commands": [
    { "id": "temp",  "request": "TEMP?",  "protocol": { "type": "ascii-tcp", "host": "127.0.0.1", "port": 4000, "line_ending": "lf" } },
    { "id": "press", "request": "PRESS?", "protocol": { "type": "ascii-tcp", "host": "127.0.0.1", "port": 4001, "line_ending": "crlf" } }
] }"#)?;

if let CommandResultPayload::Batch(batch) = context.execute_payload(&payload).await? {
    for result in batch.responses {
        println!("{:?} -> {}", result.id, result.response);
    }
}
# Ok(())
# }
```

## One-shot helpers

`exchange_line`, `exchange_line_matching` and `send_line` open a fresh connection per call using
an `AsciiTcpConfig` (defaults: `127.0.0.1:4000`, 2 s timeouts, LF).

```rust,no_run
use ascii_tcp::{exchange_line, send_line, AsciiTcpConfig};

# async fn run() -> Result<(), ascii_tcp::AsciiTcpError> {
let config = AsciiTcpConfig::default();
let reply = exchange_line(&config, "TEMP?").await?; // reply without line ending
send_line(&config, "RESET").await?;                 // no reply expected
# let _ = reply;
# Ok(())
# }
```

## Errors

`AsciiTcpError` covers timeouts, I/O, EOF, config/template errors and oversized lines;
`is_retryable()` is `true` for the transient ones (timeouts, I/O, EOF).

## Examples

Run `cargo run --example backend_server -- examples/backend-config.json` to start simulated
servers (one per unique endpoint in the config, each returning a slowly changing value), then in
another terminal `cargo run --example backend -- examples/backend-config.json` to poll them.

`Command.request` and `CommandResult.response` are plain `String`s: the crate models how to reach
a device, not the shape of its replies. Use `ResponseTemplate` or your own parsing for typed values.

