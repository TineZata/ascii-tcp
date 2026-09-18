//! Shared, persistent connections keyed by endpoint.
//!
//! [`ConnectionPool`] runs one connection-manager worker per `host:port`.
//! The worker owns the TCP socket and serializes requests. On failure it waits
//! before reconnecting, while immediately returning the failure to requests
//! queued during that cooldown.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use tokio::io::{AsyncWriteExt, BufReader};
use tokio::net::TcpStream;
use tokio::sync::{mpsc, oneshot, Mutex};

use crate::{read_ascii_line, AsciiTcpConfig, AsciiTcpError};

const RECONNECT_DELAY: Duration = Duration::from_secs(2);

type Connection = BufReader<TcpStream>;
type ResponseMatcher = Arc<dyn Fn(&str) -> bool + Send + Sync>;

enum DeviceRequest {
    Exchange {
        config: AsciiTcpConfig,
        request: String,
        accept: ResponseMatcher,
        respond: oneshot::Sender<Result<String, AsciiTcpError>>,
    },
    Send {
        config: AsciiTcpConfig,
        request: String,
        respond: oneshot::Sender<Result<(), AsciiTcpError>>,
    },
    Disconnect {
        respond: oneshot::Sender<()>,
    },
    IsConnected {
        respond: oneshot::Sender<bool>,
    },
}

struct DeviceHandle {
    tx: mpsc::UnboundedSender<DeviceRequest>,
}

impl DeviceHandle {
    async fn exchange(
        &self,
        config: AsciiTcpConfig,
        request: String,
        accept: ResponseMatcher,
    ) -> Result<String, AsciiTcpError> {
        let (respond, rx) = oneshot::channel();
        self.tx
            .send(DeviceRequest::Exchange { config, request, accept, respond })
            .map_err(|_| AsciiTcpError::ServerStopped)?;
        rx.await.map_err(|_| AsciiTcpError::ServerStopped)?
    }

    async fn send(&self, config: AsciiTcpConfig, request: String) -> Result<(), AsciiTcpError> {
        let (respond, rx) = oneshot::channel();
        self.tx
            .send(DeviceRequest::Send { config, request, respond })
            .map_err(|_| AsciiTcpError::ServerStopped)?;
        rx.await.map_err(|_| AsciiTcpError::ServerStopped)?
    }

    async fn disconnect(&self) {
        let (respond, rx) = oneshot::channel();
        if self.tx.send(DeviceRequest::Disconnect { respond }).is_ok() {
            let _ = rx.await;
        }
    }

    async fn is_connected(&self) -> bool {
        let (respond, rx) = oneshot::channel();
        if self.tx.send(DeviceRequest::IsConnected { respond }).is_err() {
            return false;
        }
        rx.await.unwrap_or(false)
    }
}

#[derive(Default)]
pub struct ConnectionPool {
    endpoints: Mutex<HashMap<String, Arc<DeviceHandle>>>,
}

impl ConnectionPool {
    pub fn new() -> Self {
        Self::default()
    }

    /// Sends `request` and returns the response line through the endpoint worker.
    pub async fn exchange_line(
        &self,
        config: &AsciiTcpConfig,
        request: &str,
    ) -> Result<String, AsciiTcpError> {
        self.exchange_line_matching(config, request, |_| true).await
    }

    /// Like [`Self::exchange_line`], but discards lines `accept` rejects.
    pub async fn exchange_line_matching<F>(
        &self,
        config: &AsciiTcpConfig,
        request: &str,
        accept: F,
    ) -> Result<String, AsciiTcpError>
    where
        F: Fn(&str) -> bool + Send + Sync + 'static,
    {
        self.handle(config)
            .await
            .exchange(config.clone(), request.to_string(), Arc::new(accept))
            .await
    }

    /// Sends `request` without waiting for a response through the endpoint worker.
    pub async fn send_line(
        &self,
        config: &AsciiTcpConfig,
        request: &str,
    ) -> Result<(), AsciiTcpError> {
        self.handle(config)
            .await
            .send(config.clone(), request.to_string())
            .await
    }

    /// Drops the pooled connection for this endpoint, if any.
    pub async fn disconnect(&self, config: &AsciiTcpConfig) {
        self.handle(config).await.disconnect().await;
    }

    /// Returns `true` when the endpoint worker currently holds a live connection.
    pub async fn is_connected(&self, config: &AsciiTcpConfig) -> bool {
        self.handle(config).await.is_connected().await
    }

    async fn handle(&self, config: &AsciiTcpConfig) -> Arc<DeviceHandle> {
        let endpoint = config.endpoint();
        let mut endpoints = self.endpoints.lock().await;
        endpoints
            .entry(endpoint.clone())
            .or_insert_with(|| spawn_worker(endpoint))
            .clone()
    }
}

fn spawn_worker(endpoint: String) -> Arc<DeviceHandle> {
    let (tx, rx) = mpsc::unbounded_channel();
    tokio::spawn(run_device_task(endpoint, rx));
    Arc::new(DeviceHandle { tx })
}

async fn run_device_task(endpoint: String, mut rx: mpsc::UnboundedReceiver<DeviceRequest>) {
    let mut connection = None;
    let mut cooldown_error: Option<String> = None;

    while let Some(request) = rx.recv().await {
        if let Some(error) = cooldown_error.take() {
            let retry_at = tokio::time::Instant::now() + RECONNECT_DELAY;
            respond_during_cooldown(request, &error, &mut connection);

            loop {
                tokio::select! {
                    request = rx.recv() => match request {
                        Some(request) => respond_during_cooldown(request, &error, &mut connection),
                        None => return,
                    },
                    _ = tokio::time::sleep_until(retry_at) => break,
                }
            }
            continue;
        }

        if let Some(error) = process_request(request, &endpoint, &mut connection).await {
            tracing::warn!(endpoint = %endpoint, error, "ascii-tcp connection failed; retrying in 2 s");
            connection = None;
            cooldown_error = Some(error);
        }
    }
}

fn respond_during_cooldown(
    request: DeviceRequest,
    error: &str,
    connection: &mut Option<Connection>,
) {
    match request {
        DeviceRequest::Exchange { respond, .. } => {
            let _ = respond.send(Err(cooldown_error(error)));
        }
        DeviceRequest::Send { respond, .. } => {
            let _ = respond.send(Err(cooldown_error(error)));
        }
        DeviceRequest::Disconnect { respond } => {
            *connection = None;
            let _ = respond.send(());
        }
        DeviceRequest::IsConnected { respond } => {
            let _ = respond.send(false);
        }
    }
}

async fn process_request(
    request: DeviceRequest,
    endpoint: &str,
    connection: &mut Option<Connection>,
) -> Option<String> {
    match request {
        DeviceRequest::Exchange { config, request, accept, respond } => {
            let result =
                exchange_with_retry(connection, &config, endpoint, &request, &accept).await;
            respond_result(respond, result)
        }
        DeviceRequest::Send { config, request, respond } => {
            let result = send_with_retry(connection, &config, endpoint, &request).await;
            respond_result(respond, result)
        }
        DeviceRequest::Disconnect { respond } => {
            if connection.take().is_some() {
                tracing::info!(endpoint, "ascii-tcp disconnected");
            }
            let _ = respond.send(());
            None
        }
        DeviceRequest::IsConnected { respond } => {
            let _ = respond.send(connection.is_some());
            None
        }
    }
}

fn respond_result<T>(
    respond: oneshot::Sender<Result<T, AsciiTcpError>>,
    result: Result<T, AsciiTcpError>,
) -> Option<String> {
    match result {
        Ok(value) => {
            let _ = respond.send(Ok(value));
            None
        }
        Err(error) => {
            let message = error.to_string();
            let _ = respond.send(Err(error));
            Some(message)
        }
    }
}

/// Runs one exchange, retrying once on a fresh socket when a reused connection fails.
///
/// A pooled socket can go half-open without the peer sending a FIN, so the first use
/// after the break fails even though the endpoint is reachable. Reconnecting in place
/// keeps that failure from dropping the connection every other caller shares.
async fn exchange_with_retry(
    connection: &mut Option<Connection>,
    config: &AsciiTcpConfig,
    endpoint: &str,
    request: &str,
    accept: &ResponseMatcher,
) -> Result<String, AsciiTcpError> {
    let reused = connection.is_some();
    let error = match ensure_connected(&mut *connection, config, endpoint).await {
        Ok(io) => match exchange(io, config, request, accept).await {
            Ok(response) => return Ok(response),
            Err(error) => error,
        },
        Err(error) => error,
    };

    if !reused || !error.is_retryable() {
        return Err(error);
    }

    tracing::debug!(endpoint, error = %error, "ascii-tcp retrying on a fresh connection");
    *connection = None;
    let io = ensure_connected(connection, config, endpoint).await?;
    exchange(io, config, request, accept).await
}

/// Fire-and-forget counterpart to [`exchange_with_retry`].
///
/// A write that fails on a reused socket has almost certainly not reached the peer,
/// so resending it does not risk applying the command twice.
async fn send_with_retry(
    connection: &mut Option<Connection>,
    config: &AsciiTcpConfig,
    endpoint: &str,
    request: &str,
) -> Result<(), AsciiTcpError> {
    let reused = connection.is_some();
    let error = match ensure_connected(&mut *connection, config, endpoint).await {
        Ok(io) => match write_request(io, config, request).await {
            Ok(()) => return Ok(()),
            Err(error) => error,
        },
        Err(error) => error,
    };

    if !reused || !error.is_retryable() {
        return Err(error);
    }

    tracing::debug!(endpoint, error = %error, "ascii-tcp retrying on a fresh connection");
    *connection = None;
    let io = ensure_connected(connection, config, endpoint).await?;
    write_request(io, config, request).await
}

async fn ensure_connected<'a>(
    connection: &'a mut Option<Connection>,
    config: &AsciiTcpConfig,
    endpoint: &str,
) -> Result<&'a mut Connection, AsciiTcpError> {
    if connection.is_none() {
        tracing::debug!(endpoint, "ascii-tcp connecting");
        let stream = tokio::time::timeout(config.connect_timeout, TcpStream::connect(endpoint))
            .await
            .map_err(|_| AsciiTcpError::ConnectTimeout)??;
        stream.set_nodelay(true)?;
        tracing::info!(endpoint, "ascii-tcp connected");
        *connection = Some(BufReader::new(stream));
    }
    Ok(connection.as_mut().expect("connection was initialized"))
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
    accept: &ResponseMatcher,
) -> Result<String, AsciiTcpError> {
    write_request(io, config, request).await?;
    tracing::info!(endpoint = %config.endpoint(), request, "ascii-tcp sending request");
    tokio::time::timeout(config.io_timeout, async {
        loop {
            let line = read_ascii_line(io, config.line_ending, None)
                .await?
                .ok_or(AsciiTcpError::Eof)?;
            tracing::info!(endpoint = %config.endpoint(), line, "ascii-tcp received line");
            if accept(&line) {
                return Ok(line);
            }
            // Surfaced at WARN because a response-template mismatch is otherwise indistinguishable
            // from a mute device: both stall this loop until io_timeout reports IoTimeout.
            tracing::warn!(endpoint = %config.endpoint(), line, "ascii-tcp discarding unsolicited line");
        }
    })
    .await
    .map_err(|_| AsciiTcpError::IoTimeout)?
}

fn cooldown_error(message: &str) -> AsciiTcpError {
    AsciiTcpError::Io(std::io::Error::new(std::io::ErrorKind::ConnectionAborted, message))
}