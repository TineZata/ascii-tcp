use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;
use tokio::sync::{Mutex, OwnedMutexGuard};

pub mod client;
pub mod server;

pub use client::{ClientConfig, Context};
pub use server::{Server, ServerConfig, ServerHandle};

/// Connection parameters for a single ASCII-TCP endpoint.
#[derive(Debug, Clone)]
pub struct AsciiTcpConfig {
    pub host: String,
    pub port: u16,
    pub connect_timeout: Duration,
    pub io_timeout: Duration,
    pub line_ending: LineEnding,
}

impl Default for AsciiTcpConfig {
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
#[derive(Debug, Clone, Copy, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum LineEnding {
    Lf,
    CrLf,
    Cr,
}

impl LineEnding {
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
    #[error("connect timeout")]
    ConnectTimeout,
    #[error("I/O timeout")]
    IoTimeout,
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("connection closed before response")]
    Eof,
    #[error("invalid protocol type: {0}")]
    InvalidProtocolType(String),
    #[error("backend config parse error: {0}")]
    ConfigParse(#[from] serde_json::Error),
    #[error("request exceeds maximum line length of {0} bytes")]
    RequestTooLong(usize),
    #[error("server task stopped")]
    ServerStopped,
}

impl AsciiTcpError {
    /// Returns `true` if the error is likely transient and safe to retry.
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
    #[serde(rename = "type")]
    pub kind: String,
    pub host: String,
    pub port: u16,
    pub line_ending: LineEnding,
    pub connect_timeout_ms: Option<u64>,
    pub io_timeout_ms: Option<u64>,
}

/// A single ASCII-TCP command with its target endpoint.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Command {
    pub id: Option<String>,
    pub request: String,
    pub protocol: HardwareProtocol,
}

/// The response received for a single [`Command`].
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct CommandResult {
    pub id: Option<String>,
    pub response: String,
}

/// A set of commands to execute sequentially, parsed from `{"commands": [...]}`.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct BatchCommands {
    pub commands: Vec<Command>,
}

/// Responses from a [`BatchCommands`] execution.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct BatchResult {
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

#[derive(Debug, Default)]
pub(crate) struct TcpBackend {
    locks: Mutex<HashMap<String, Arc<Mutex<()>>>>,
}

impl TcpBackend {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    pub(crate) async fn exchange_line(
        &self,
        config: &AsciiTcpConfig,
        request: &str,
    ) -> Result<String, AsciiTcpError> {
        let _guard = self.acquire_endpoint(config).await;
        exchange_line(config, request).await
    }

    pub(crate) async fn execute_command(
        &self,
        command: &Command,
    ) -> Result<CommandResult, AsciiTcpError> {
        let config = AsciiTcpConfig::from_protocol(&command.protocol)?;
        let response = self.exchange_line(&config, &command.request).await?;
        Ok(CommandResult {
            id: command.id.clone(),
            response,
        })
    }

    async fn acquire_endpoint(&self, config: &AsciiTcpConfig) -> OwnedMutexGuard<()> {
        let lock = self.get_or_create_lock(&config.endpoint()).await;
        lock.lock_owned().await
    }

    async fn get_or_create_lock(&self, endpoint: &str) -> Arc<Mutex<()>> {
        let mut locks = self.locks.lock().await;
        locks
            .entry(endpoint.to_string())
            .or_insert_with(|| Arc::new(Mutex::new(())))
            .clone()
    }
}

/// Executes a single request and returns the response line, without the line ending.
pub async fn exchange_line(
    config: &AsciiTcpConfig,
    request: &str,
) -> Result<String, AsciiTcpError> {
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

    let line = tokio::time::timeout(
        config.io_timeout,
        read_ascii_line(&mut reader, config.line_ending, None),
    )
    .await
    .map_err(|_| AsciiTcpError::IoTimeout)??;

    line.ok_or(AsciiTcpError::Eof)
}

/// Reads a single line from the reader, using the specified line ending as the terminator.
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
