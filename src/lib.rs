//! Tokio-based ASCII line request/response client and server over TCP.
//!
//! * [`Context`] is the high-level client: one persistent connection per `host:port`,
//!   connect/disconnect callbacks and optional retry.
//! * [`Server`] is a line-oriented server driven by a [`RequestHandler`].
//! * [`exchange_line`] / [`send_line`] are one-shot helpers that open a fresh connection.
//! * [`ResponseTemplate`] extracts typed values from reply lines with printf-style templates.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;

use crate::client::endpoint::Endpoint;

pub mod client;
pub mod server;
pub mod template;

pub use client::{AsciiResponseMode, ChannelConfig, ClientConfig, Context};
pub use server::{RequestHandler, Server, ServerContext, ServerHandle};
pub use template::{parse_first_value, parse_response_template, Capture, ResponseTemplate, SpecKind};

/// Connection parameters for a single ASCII-TCP endpoint.
#[derive(Debug, Clone)]
pub struct AsciiTcpConfig {
    /// Remote host name or IP address, passed to `TcpStream::connect` together with `port`.
    pub host: String,
    /// TCP port of the remote endpoint.
    pub port: u16,
    /// Maximum time to wait for the TCP connection to be established
    /// before failing with [`AsciiTcpError::ConnectTimeout`].
    pub connect_timeout: Duration,
    /// Maximum time to wait for a single write or reply once connected
    /// before failing with [`AsciiTcpError::IoTimeout`].
    pub io_timeout: Duration,
    /// Terminator appended to requests and expected at the end of responses.
    pub line_ending: LineEnding,
}

impl Default for AsciiTcpConfig {
    /// `127.0.0.1:4000`, 2 s connect and I/O timeouts, LF line ending.
    fn default() -> Self {
        Self {
            host: "127.0.0.1".to_string(),
            port: 4000,
            connect_timeout: Duration::from_secs(2),
            io_timeout: Duration::from_secs(2),
            line_ending: LineEnding::Lf,
        }
    }
}

/// Line ending used to frame requests and responses.
#[derive(Debug, Clone, Copy, Default, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum LineEnding {
    /// Line feed (`\n`).
    #[default]
    Lf,
    /// Carriage return followed by line feed (`\r\n`).
    CrLf,
    /// Carriage return (`\r`).
    Cr,
}

impl LineEnding {
    /// The terminator as it appears on the wire.
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Lf => "\n",
            Self::CrLf => "\r\n",
            Self::Cr => "\r",
        }
    }
}

/// Errors that can occur during client or server operations.
#[derive(Debug, Error)]
pub enum AsciiTcpError {
    /// The TCP connection was not established within `connect_timeout`.
    #[error("connect timeout")]
    ConnectTimeout,
    /// A write or the wait for a reply exceeded `io_timeout`.
    #[error("I/O timeout")]
    IoTimeout,
    /// Underlying socket error (refused connection, reset, invalid UTF-8 on the wire, ...).
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    /// The peer closed the connection before a complete line was received.
    #[error("connection closed before response")]
    Eof,
    /// A [`HardwareProtocol`] had a `type` other than `"ascii-tcp"`.
    #[error("invalid protocol type: {0}")]
    InvalidProtocolType(String),
    /// A JSON payload could not be deserialised.
    #[error("backend config parse error: {0}")]
    ConfigParse(#[from] serde_json::Error),
    /// A request line exceeded the server's `max_line_length` (the value carried).
    #[error("request exceeds maximum line length of {0} bytes")]
    RequestTooLong(usize),
    /// The server task already finished or was already stopped.
    #[error("server task stopped")]
    ServerStopped,
    /// A response template is malformed; see [`ResponseTemplate::compile`].
    #[error("invalid response template: {0}")]
    TemplateSyntax(String),
    /// A response line does not have the layout a template describes.
    #[error("response '{response}' does not match template '{template}'")]
    ResponseMismatch { template: String, response: String },
}

impl AsciiTcpError {
    /// Returns `true` if the error is likely transient and safe to retry
    /// (timeouts, I/O errors and EOF); configuration and protocol errors are not.
    pub fn is_retryable(&self) -> bool {
        matches!(
            self,
            Self::ConnectTimeout | Self::IoTimeout | Self::Io(_) | Self::Eof
        )
    }
}

/// Wire-format protocol block parsed from JSON config.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct HardwareProtocol {
    /// Protocol discriminator; must be `"ascii-tcp"` (serialised as `type`).
    #[serde(rename = "type")]
    pub kind: String,
    /// Remote host name or IP address.
    pub host: String,
    /// Remote TCP port.
    pub port: u16,
    /// Line terminator used to frame requests and responses.
    pub line_ending: LineEnding,
    /// Connect timeout in milliseconds; defaults to 2000 when `None`.
    pub connect_timeout_ms: Option<u64>,
    /// I/O timeout in milliseconds; defaults to 2000 when `None`.
    pub io_timeout_ms: Option<u64>,
}

/// A single ASCII-TCP command with its target endpoint.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Command {
    /// Optional caller-chosen identifier, echoed back in the matching [`CommandResult`].
    pub id: Option<String>,
    /// The request line, without line ending. An empty string sends nothing and only listens.
    pub request: String,
    /// Endpoint the request is sent to.
    pub protocol: HardwareProtocol,
}

/// The response received for a single [`Command`].
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct CommandResult {
    /// The `id` of the originating [`Command`].
    pub id: Option<String>,
    /// The accepted response line, without line ending.
    pub response: String,
}

/// A set of commands to execute sequentially, parsed from `{"commands": [...]}`.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct BatchCommands {
    /// Commands, executed in order; the first failure aborts the batch.
    pub commands: Vec<Command>,
}

/// Responses from a [`BatchCommands`] execution.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct BatchResult {
    /// One response per command, in command order.
    pub responses: Vec<CommandResult>,
}

/// Either a single [`Command`] or a [`BatchCommands`]; both forms deserialise from the same JSON.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(untagged)]
pub enum Payload {
    Single(Command),
    Batch(BatchCommands),
}

/// Result of executing a [`Payload`]: a single response or a batch of responses.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(untagged)]
pub enum CommandResultPayload {
    Single(CommandResult),
    Batch(BatchResult),
}

impl AsciiTcpConfig {
    /// Builds an `AsciiTcpConfig` from a `HardwareProtocol`, returning an error if `type` is not `"ascii-tcp"`.
    pub fn from_protocol(protocol: &HardwareProtocol) -> Result<Self, AsciiTcpError> {
        if protocol.kind != "ascii-tcp" {
            return Err(AsciiTcpError::InvalidProtocolType(protocol.kind.clone()));
        }

        Ok(Self {
            host: protocol.host.clone(),
            port: protocol.port,
            connect_timeout: Duration::from_millis(protocol.connect_timeout_ms.unwrap_or(2_000)),
            io_timeout: Duration::from_millis(protocol.io_timeout_ms.unwrap_or(2_000)),
            line_ending: protocol.line_ending,
        })
    }

    /// Returns `"host:port"` as a string.
    pub fn endpoint(&self) -> String {
        format!("{}:{}", self.host, self.port)
    }
}

/// Parses a JSON string into a [`Command`].
pub fn parse_backend_command(json: &str) -> Result<Command, AsciiTcpError> {
    Ok(serde_json::from_str(json)?)
}

/// Parses a JSON string into a [`BatchCommands`].
pub fn parse_backend_batch(json: &str) -> Result<BatchCommands, AsciiTcpError> {
    Ok(serde_json::from_str(json)?)
}

/// Parses a JSON string into a [`Payload`] (single command or batch).
pub fn parse_backend_payload(json: &str) -> Result<Payload, AsciiTcpError> {
    Ok(serde_json::from_str(json)?)
}

/// Resolves a command's hardware protocol to an [`AsciiTcpConfig`]
/// (rejecting any type other than `ascii-tcp`) and gets-or-creates the `Endpoint` for `host:port`.
#[derive(Default)]
pub(crate) struct TcpBackend {
    endpoints: std::sync::Mutex<HashMap<String, Arc<Endpoint>>>,
}

impl TcpBackend {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    /// Returns the shared endpoint for the config's `host:port`, creating it on first use.
    fn endpoint(&self, config: &AsciiTcpConfig) -> Arc<Endpoint> {
        let mut endpoints = self
            .endpoints
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        endpoints.entry(config.endpoint()).or_default().clone()
    }

    /// Sends `command` on its endpoint's shared connection and returns the first line `accept` approves.
    pub(crate) async fn execute_command_matching<F>(
        &self,
        command: &Command,
        accept: F,
    ) -> Result<CommandResult, AsciiTcpError>
    where
        F: Fn(&str) -> bool,
    {
        let config = AsciiTcpConfig::from_protocol(&command.protocol)?;
        let response = self
            .endpoint(&config)
            .exchange(&config, &command.request, accept)
            .await?;
        Ok(CommandResult {
            id: command.id.clone(),
            response,
        })
    }

    /// Sends `command` on its endpoint's shared connection without waiting for a reply.
    pub(crate) async fn send_command(&self, command: &Command) -> Result<(), AsciiTcpError> {
        let config = AsciiTcpConfig::from_protocol(&command.protocol)?;
        self.endpoint(&config).send(&config, &command.request).await
    }
}

/// Sends a single request without waiting for a response.
///
/// Use this for commands the endpoint does not acknowledge; [`exchange_line`]
/// would block until `io_timeout` elapses and then fail with
/// [`AsciiTcpError::IoTimeout`].
pub async fn send_line(config: &AsciiTcpConfig, request: &str) -> Result<(), AsciiTcpError> {
    let mut stream = tokio::time::timeout(
        config.connect_timeout,
        TcpStream::connect(config.endpoint()),
    )
    .await
    .map_err(|_| AsciiTcpError::ConnectTimeout)??;

    let outbound = format!("{}{}", request, config.line_ending.as_str());
    tokio::time::timeout(config.io_timeout, async {
        stream.write_all(outbound.as_bytes()).await?;
        stream.flush().await
    })
    .await
    .map_err(|_| AsciiTcpError::IoTimeout)??;
    tracing::info!(endpoint = %config.endpoint(), request, "ascii-tcp TX (no response expected)");

    Ok(())
}

/// Executes a single request and returns the response line, without the line ending.
pub async fn exchange_line(
    config: &AsciiTcpConfig,
    request: &str,
) -> Result<String, AsciiTcpError> {
    exchange_line_matching(config, request, |_| true).await
}

/// Like [`exchange_line`], but skips lines `accept` rejects until one matches or `io_timeout` elapses.
///
/// Some endpoints emit unsolicited lines (greeting banners, heartbeats, warnings)
/// that would otherwise be mistaken for the reply.
pub async fn exchange_line_matching<F>(
    config: &AsciiTcpConfig,
    request: &str,
    accept: F,
) -> Result<String, AsciiTcpError>
where
    F: Fn(&str) -> bool,
{
    let stream = tokio::time::timeout(
        config.connect_timeout,
        TcpStream::connect(config.endpoint()),
    )
    .await
    .map_err(|_| AsciiTcpError::ConnectTimeout)??;

    let (reader, mut writer) = tokio::io::split(stream);
    let mut reader = BufReader::new(reader);

    let outbound = format!("{}{}", request, config.line_ending.as_str());
    tokio::time::timeout(config.io_timeout, async {
        writer.write_all(outbound.as_bytes()).await?;
        writer.flush().await
    })
    .await
    .map_err(|_| AsciiTcpError::IoTimeout)??;
    tracing::info!(endpoint = %config.endpoint(), request, "ascii-tcp TX");

    tokio::time::timeout(config.io_timeout, async {
        loop {
            let line = read_ascii_line(&mut reader, config.line_ending, None)
                .await?
                .ok_or(AsciiTcpError::Eof)?;
            let accepted = accept(&line);
            tracing::info!(endpoint = %config.endpoint(), line, accepted, "ascii-tcp RX");
            if accepted {
                return Ok(line);
            }
        }
    })
    .await
    .map_err(|_| AsciiTcpError::IoTimeout)?
}

/// Reads a single line from the reader, using the specified line ending as the terminator.
///
/// Returns `Ok(None)` on a clean EOF at a line boundary, [`AsciiTcpError::Eof`] on EOF in the
/// middle of a line, [`AsciiTcpError::RequestTooLong`] once the line exceeds `max_line_length`
/// (when given), and an `InvalidData` I/O error if the line is not valid UTF-8.
pub(crate) async fn read_ascii_line<R>(
    reader: &mut R,
    line_ending: LineEnding,
    max_line_length: Option<usize>,
) -> Result<Option<String>, AsciiTcpError>
where
    R: AsyncRead + Unpin,
{
    let terminator = line_ending.as_str().as_bytes();
    let mut bytes = Vec::new();

    loop {
        let byte = match reader.read_u8().await {
            Ok(byte) => byte,
            Err(error) if error.kind() == std::io::ErrorKind::UnexpectedEof && bytes.is_empty() => {
                return Ok(None);
            }
            Err(error) if error.kind() == std::io::ErrorKind::UnexpectedEof => {
                return Err(AsciiTcpError::Eof);
            }
            Err(error) => return Err(error.into()),
        };
        bytes.push(byte);

        if bytes.ends_with(terminator) {
            bytes.truncate(bytes.len() - terminator.len());
            let line = String::from_utf8(bytes)
                .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidData, error))?;
            return Ok(Some(line));
        }
        if let Some(max_line_length) = max_line_length {
            if bytes.len() > max_line_length {
                return Err(AsciiTcpError::RequestTooLong(max_line_length));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::pin::Pin;
    use std::task::{Context as TaskContext, Poll};

    use tokio::io::ReadBuf;

    use super::*;

    /// A reader whose every read fails, to exercise the non-EOF error path.
    struct FailingReader;

    impl AsyncRead for FailingReader {
        fn poll_read(
            self: Pin<&mut Self>,
            _cx: &mut TaskContext<'_>,
            _buf: &mut ReadBuf<'_>,
        ) -> Poll<std::io::Result<()>> {
            Poll::Ready(Err(std::io::Error::from(std::io::ErrorKind::ConnectionReset)))
        }
    }

    fn protocol(kind: &str) -> HardwareProtocol {
        HardwareProtocol {
            kind: kind.to_string(),
            host: "127.0.0.1".to_string(),
            port: 1,
            line_ending: LineEnding::CrLf,
            connect_timeout_ms: None,
            io_timeout_ms: None,
        }
    }

    #[tokio::test]
    async fn read_ascii_line_splits_on_each_line_ending() {
        for (input, ending) in [
            (&b"one\ntwo\n"[..], LineEnding::Lf),
            (&b"one\r\ntwo\r\n"[..], LineEnding::CrLf),
            (&b"one\rtwo\r"[..], LineEnding::Cr),
        ] {
            let mut reader = input;
            assert_eq!(read_ascii_line(&mut reader, ending, None).await.unwrap().as_deref(), Some("one"));
            assert_eq!(read_ascii_line(&mut reader, ending, None).await.unwrap().as_deref(), Some("two"));
            assert!(read_ascii_line(&mut reader, ending, None).await.unwrap().is_none());
        }
    }

    #[tokio::test]
    async fn read_ascii_line_returns_empty_line() {
        let mut reader = &b"\n"[..];
        let line = read_ascii_line(&mut reader, LineEnding::Lf, None).await.unwrap();
        assert_eq!(line.as_deref(), Some(""));
    }

    #[tokio::test]
    async fn read_ascii_line_does_not_split_on_a_partial_terminator() {
        let mut reader = &b"a\rb\r\n"[..];
        let line = read_ascii_line(&mut reader, LineEnding::CrLf, None).await.unwrap();
        assert_eq!(line.as_deref(), Some("a\rb"));
    }

    #[tokio::test]
    async fn read_ascii_line_reports_eof_in_the_middle_of_a_line() {
        let mut reader = &b"partial"[..];
        let error = read_ascii_line(&mut reader, LineEnding::Lf, None).await.unwrap_err();
        assert!(matches!(error, AsciiTcpError::Eof));
    }

    #[tokio::test]
    async fn read_ascii_line_enforces_the_maximum_length() {
        let mut reader = &b"abcde\n"[..];
        let error = read_ascii_line(&mut reader, LineEnding::Lf, Some(4)).await.unwrap_err();
        assert!(matches!(error, AsciiTcpError::RequestTooLong(4)));

        // A line of exactly the maximum length is still accepted.
        let mut reader = &b"abcd\n"[..];
        let line = read_ascii_line(&mut reader, LineEnding::Lf, Some(4)).await.unwrap();
        assert_eq!(line.as_deref(), Some("abcd"));
    }

    #[tokio::test]
    async fn read_ascii_line_rejects_invalid_utf8() {
        let mut reader = &b"\xff\xfe\n"[..];
        let error = read_ascii_line(&mut reader, LineEnding::Lf, None).await.unwrap_err();
        match error {
            AsciiTcpError::Io(error) => assert_eq!(error.kind(), std::io::ErrorKind::InvalidData),
            other => panic!("expected an I/O error, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn read_ascii_line_propagates_read_errors() {
        let error = read_ascii_line(&mut FailingReader, LineEnding::Lf, None).await.unwrap_err();
        match error {
            AsciiTcpError::Io(error) => assert_eq!(error.kind(), std::io::ErrorKind::ConnectionReset),
            other => panic!("expected an I/O error, got {other:?}"),
        }
    }

    #[test]
    fn line_ending_terminators() {
        assert_eq!(LineEnding::Lf.as_str(), "\n");
        assert_eq!(LineEnding::CrLf.as_str(), "\r\n");
        assert_eq!(LineEnding::Cr.as_str(), "\r");
    }

    #[test]
    fn tcp_backend_shares_one_endpoint_per_host_and_port() {
        let backend = TcpBackend::new();
        let config = |port| AsciiTcpConfig { port, ..AsciiTcpConfig::default() };

        let first = backend.endpoint(&config(4000));
        assert!(Arc::ptr_eq(&first, &backend.endpoint(&config(4000))));
        assert!(!Arc::ptr_eq(&first, &backend.endpoint(&config(4001))));
    }

    #[test]
    fn tcp_backend_recovers_from_a_poisoned_endpoint_map() {
        let backend = Arc::new(TcpBackend::new());
        let poisoner = backend.clone();
        let _ = std::thread::spawn(move || {
            let _guard = poisoner.endpoints.lock().unwrap();
            panic!("poison the endpoint map");
        })
        .join();

        let _ = backend.endpoint(&AsciiTcpConfig::default());
    }

    #[tokio::test]
    async fn tcp_backend_rejects_a_foreign_protocol_type() {
        let backend = TcpBackend::new();
        let command = Command {
            id: None,
            request: "READ".to_string(),
            protocol: protocol("modbus"),
        };

        let error = backend.execute_command_matching(&command, |_| true).await.unwrap_err();
        assert!(matches!(error, AsciiTcpError::InvalidProtocolType(kind) if kind == "modbus"));
        let error = backend.send_command(&command).await.unwrap_err();
        assert!(matches!(error, AsciiTcpError::InvalidProtocolType(_)));
    }
}

