use std::time::Duration;
use serde::{Deserialize, Serialize};

use crate::{AsciiTcpConfig, Command, HardwareProtocol, LineEnding};

/// How a read response line is interpreted.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, Default, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum AsciiResponseMode {
    /// Parses the response as a number, then applies `scale` and `offset`.
    Number,
    /// Parses the response as a boolean.
    Bool,
    /// Uses the response line as text. The default.
    #[default]
    Text,
    /// Treats receipt of any accepted response line as a pulse: the value is
    /// always `1` when a line arrives — content beyond matching `read_response`
    /// doesn't matter.
    ///
    /// Intended for heartbeat/keep-alive lines (e.g. `ALIVE`). Set `read_response`
    /// to the exact literal to watch for so other lines are discarded instead of
    /// producing a pulse. A stalled heartbeat surfaces as a disconnect once
    /// `io_timeout_ms` elapses without a match.
    Presence,
}

/// Settings for one ASCII-TCP channel: endpoint, read/write commands and value conversion.
///
/// Deserialises from the JSON block used to configure a channel, e.g.
/// `{ "host": "127.0.0.1", "port": 4000, "read_command": "READ TEMP", "line_ending": "crlf" }`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChannelConfig {
    /// Target host name or IP address.
    pub host: String,
    /// TCP port of the ASCII line protocol endpoint.
    pub port: u16,
    /// Poll request line sent on each read cycle.
    ///
    /// Omit for write-only endpoints: no polling happens. An empty string sends nothing and
    /// only listens for lines matching `read_response` (e.g. unsolicited heartbeats).
    #[serde(default)]
    pub read_command: Option<String>,
    /// Optional printf-style template describing the read response layout.
    ///
    /// The first conversion specifier (`%d`, `%f`, `%x`, `%s`) supplies the
    /// value, e.g. `"TEMP=%f"` extracts `22.5` from `TEMP=22.5`.
    /// When absent, the whole response line is parsed per `response_mode`.
    #[serde(default)]
    pub read_response: Option<String>,
    /// Optional command template used for writes.
    ///
    /// When present, `{value}` is replaced with the requested value.
    /// When absent, the raw value is sent as-is.
    #[serde(default)]
    pub write_command: Option<String>,
    /// Whether the endpoint acknowledges writes with a response line.
    ///
    /// Set to `false` for fire-and-forget commands, otherwise the write waits
    /// for a reply that never arrives and fails with an I/O timeout.
    #[serde(default = "default_true")]
    pub write_expects_response: bool,
    /// Line terminator appended to outbound requests.
    #[serde(default)]
    pub line_ending: LineEnding,
    /// Minimum poll interval in milliseconds.
    #[serde(default = "default_min_poll_interval_ms")]
    pub min_poll_interval_ms: u64,
    /// Timeout in milliseconds for establishing the TCP connection.
    #[serde(default = "default_timeout_ms")]
    pub connect_timeout_ms: u64,
    /// Timeout in milliseconds for a single request/response exchange.
    #[serde(default = "default_timeout_ms")]
    pub io_timeout_ms: u64,
    /// Scale factor applied to parsed numeric values.
    #[serde(default = "default_scale")]
    pub scale: f64,
    /// Offset applied after scaling.
    #[serde(default)]
    pub offset: f64,
    /// Response parse strategy.
    #[serde(default)]
    pub response_mode: AsciiResponseMode,
}

/// Default for `write_expects_response`: writes are acknowledged.
fn default_true() -> bool {
    true
}

fn default_min_poll_interval_ms() -> u64 {
    500
}

fn default_timeout_ms() -> u64 {
    2_000
}

fn default_scale() -> f64 {
    1.0
}

impl ChannelConfig {
    /// Wire-format protocol block for this endpoint.
    pub fn protocol(&self) -> HardwareProtocol {
        HardwareProtocol {
            kind: "ascii-tcp".to_string(),
            host: self.host.clone(),
            port: self.port,
            line_ending: self.line_ending,
            connect_timeout_ms: Some(self.connect_timeout_ms),
            io_timeout_ms: Some(self.io_timeout_ms),
        }
    }

    /// Builds a [`Command`] sending `request` to this endpoint.
    pub fn command(&self, request: impl Into<String>) -> Command {
        Command {
            id: None,
            request: request.into(),
            protocol: self.protocol(),
        }
    }

    /// Low-level connection parameters for this endpoint.
    pub fn connection(&self) -> AsciiTcpConfig {
        AsciiTcpConfig {
            host: self.host.clone(),
            port: self.port,
            connect_timeout: Duration::from_millis(self.connect_timeout_ms),
            io_timeout: Duration::from_millis(self.io_timeout_ms),
            line_ending: self.line_ending,
        }
    }
}

/// Retry behaviour for [`Context`](super::Context).
#[derive(Debug, Clone)]
pub struct ClientConfig {
    /// How long to wait between retry attempts on transient errors.
    pub retry_interval: Duration,
    /// If `true`, [`Context::execute_payload`](super::Context::execute_payload) retries on transient errors indefinitely.
    pub retry_forever: bool,
}

impl ClientConfig {
    /// Loads `ClientConfig` from `ASCII_TCP_RETRY_MS` and `ASCII_TCP_RETRY_FOREVER` environment variables.
    ///
    /// `ASCII_TCP_RETRY_MS` falls back to 2000 when unset or not a number. `ASCII_TCP_RETRY_FOREVER`
    /// is `true` unless set to `false` or `no` (case-insensitive).
    pub fn from_env() -> Self {
        let retry_interval_ms = std::env::var("ASCII_TCP_RETRY_MS")
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
            .unwrap_or(2_000);

        let retry_forever = std::env::var("ASCII_TCP_RETRY_FOREVER")
            .map(|value| !value.eq_ignore_ascii_case("false") && !value.eq_ignore_ascii_case("no"))
            .unwrap_or(true);

        Self {
            retry_interval: Duration::from_millis(retry_interval_ms),
            retry_forever,
        }
    }
}

impl Default for ClientConfig {
    /// Retries forever, every 2 seconds.
    fn default() -> Self {
        Self {
            retry_interval: Duration::from_secs(2),
            retry_forever: true,
        }
    }
}
