//! Shared, persistent connections keyed by endpoint.
//!
//! [`ConnectionPool`] keeps one live TCP connection per `host:port` and holds a
//! lock across each request/response pair. Callers sharing a pool therefore
//! never overlap on the wire, which matters for transparent serial bridges that
//! accept a single client and return responses with no request correlation.
//!
//! Any failure drops the connection so the next call reconnects. That also
//! discards bytes left buffered by a timed-out exchange, which would otherwise
//! desynchronise every later response by one line.

use std::collections::HashMap;
use std::sync::Arc;

use tokio::io::{AsyncWriteExt, BufReader};
use tokio::net::TcpStream;
use tokio::sync::Mutex;

use crate::{read_ascii_line, AsciiTcpConfig, AsciiTcpError};

type Connection = BufReader<TcpStream>;
type Slot = Arc<Mutex<Option<Connection>>>;

#[derive(Default)]
pub struct ConnectionPool {
    endpoints: Mutex<HashMap<String, Slot>>,
}

impl ConnectionPool {
    pub fn new() -> Self {
        Self::default()
    }

    /// Sends `request` and returns the response line, reusing the pooled connection.
    pub async fn exchange_line(
        &self,
        config: &AsciiTcpConfig,
        request: &str,
    ) -> Result<String, AsciiTcpError> {
        let slot = self.slot(config).await;
        let mut guard = slot.lock().await;

        if let Some(mut io) = guard.take() {
            match Self::exchange(&mut io, config, request).await {
                Ok(response) => {
                    *guard = Some(io);
                    return Ok(response);
                }
                Err(error) if is_dead_socket(&error) => {}
                Err(error) => return Err(error),
            }
        }

        let mut io = Self::connect(config).await?;
        let response = Self::exchange(&mut io, config, request).await?;
        *guard = Some(io);
        Ok(response)
    }

    /// Sends `request` without waiting for a response, reusing the pooled connection.
    pub async fn send_line(
        &self,
        config: &AsciiTcpConfig,
        request: &str,
    ) -> Result<(), AsciiTcpError> {
        let slot = self.slot(config).await;
        let mut guard = slot.lock().await;

        if let Some(mut io) = guard.take() {
            match Self::write_request(&mut io, config, request).await {
                Ok(()) => {
                    *guard = Some(io);
                    return Ok(());
                }
                Err(error) if is_dead_socket(&error) => {}
                Err(error) => return Err(error),
            }
        }

        let mut io = Self::connect(config).await?;
        Self::write_request(&mut io, config, request).await?;
        *guard = Some(io);
        Ok(())
    }

    /// Drops the pooled connection for this endpoint, if any.
    pub async fn disconnect(&self, config: &AsciiTcpConfig) {
        let slot = self.slot(config).await;
        slot.lock().await.take();
    }

    /// Returns `true` when a live connection is currently pooled for this endpoint.
    pub async fn is_connected(&self, config: &AsciiTcpConfig) -> bool {
        let slot = self.slot(config).await;
        let guard = slot.lock().await;
        guard.is_some()
    }

    async fn slot(&self, config: &AsciiTcpConfig) -> Slot {
        let mut endpoints = self.endpoints.lock().await;
        endpoints.entry(config.endpoint()).or_default().clone()
    }

    async fn connect(config: &AsciiTcpConfig) -> Result<Connection, AsciiTcpError> {
        let stream = tokio::time::timeout(
            config.connect_timeout,
            TcpStream::connect(config.endpoint()),
        )
        .await
        .map_err(|_| AsciiTcpError::ConnectTimeout)??;
        stream.set_nodelay(true)?;
        Ok(BufReader::new(stream))
    }

    async fn write_request(
        io: &mut Connection,
        config: &AsciiTcpConfig,
        request: &str,
    ) -> Result<(), AsciiTcpError> {
        let outbound = format!("{}{}", request, config.line_ending.as_str());
        tokio::time::timeout(config.io_timeout, async {
            io.write_all(outbound.as_bytes()).await?;
            io.flush().await
        })
        .await
        .map_err(|_| AsciiTcpError::IoTimeout)??;
        Ok(())
    }

    async fn exchange(
        io: &mut Connection,
        config: &AsciiTcpConfig,
        request: &str,
    ) -> Result<String, AsciiTcpError> {
        Self::write_request(io, config, request).await?;
        let line = tokio::time::timeout(
            config.io_timeout,
            read_ascii_line(io, config.line_ending, None),
        )
        .await
        .map_err(|_| AsciiTcpError::IoTimeout)??;
        line.ok_or(AsciiTcpError::Eof)
    }
}

/// Whether the failure means the pooled socket is gone, so a reconnect is worth one retry.
///
/// Timeouts are excluded deliberately: a slow endpoint would otherwise be
/// retried and pay the timeout twice on every poll.
fn is_dead_socket(error: &AsciiTcpError) -> bool {
    matches!(error, AsciiTcpError::Eof | AsciiTcpError::Io(_))
}
