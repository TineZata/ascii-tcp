use std::time::Duration;

use thiserror::Error;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;

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

#[derive(Debug, Clone, Copy)]
pub enum LineEnding {
    Lf,
    CrLf,
    Cr,
}

impl LineEnding {
    fn as_str(self) -> &'static str {
        match self {
            Self::Lf => "\n",
            Self::CrLf => "\r\n",
            Self::Cr => "\r",
        }
    }
}

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
}

pub async fn exchange_line(cfg: &AsciiTcpConfig, request: &str) -> Result<String, AsciiTcpError> {
    let addr = format!("{}:{}", cfg.host, cfg.port);

    let stream = tokio::time::timeout(cfg.connect_timeout, TcpStream::connect(&addr))
        .await
        .map_err(|_| AsciiTcpError::ConnectTimeout)??;

    let (reader, mut writer) = tokio::io::split(stream);
    let mut reader = BufReader::new(reader);

    let outbound = format!("{}{}", request, cfg.line_ending.as_str());
    tokio::time::timeout(cfg.io_timeout, async {
        writer.write_all(outbound.as_bytes()).await?;
        writer.flush().await
    })
    .await
    .map_err(|_| AsciiTcpError::IoTimeout)??;

    let mut line = String::new();
    let read_n = tokio::time::timeout(cfg.io_timeout, reader.read_line(&mut line))
        .await
        .map_err(|_| AsciiTcpError::IoTimeout)??;

    if read_n == 0 {
        return Err(AsciiTcpError::Eof);
    }

    while line.ends_with('\n') || line.ends_with('\r') {
        line.pop();
    }

    Ok(line)
}
