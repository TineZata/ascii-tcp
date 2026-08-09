use std::future::Future;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use tokio::io::{AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::watch;
use tokio::task::{JoinHandle, JoinSet};
use tracing::{debug, warn};

use crate::{read_ascii_line, AsciiTcpError, LineEnding};

type HandlerFuture = Pin<Box<dyn Future<Output = Result<String, AsciiTcpError>> + Send>>;
type Handler = Arc<dyn Fn(String, SocketAddr) -> HandlerFuture + Send + Sync>;

#[derive(Debug, Clone)]
pub struct ServerConfig {
    pub bind_addr: SocketAddr,
    pub io_timeout: Duration,
    pub line_ending: LineEnding,
    pub max_line_length: usize,
}

impl ServerConfig {
    pub fn from_env() -> Result<Self, AsciiTcpError> {
        let mut config = Self::default();

        if let Ok(value) = std::env::var("ASCII_TCP_SERVER_ADDR") {
            config.bind_addr = value.parse().map_err(|error| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    format!("invalid ASCII_TCP_SERVER_ADDR: {error}"),
                )
            })?;
        }
        if let Some(value) = std::env::var("ASCII_TCP_SERVER_IO_TIMEOUT_MS")
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
        {
            config.io_timeout = Duration::from_millis(value);
        }
        if let Some(value) = std::env::var("ASCII_TCP_SERVER_MAX_LINE_LENGTH")
            .ok()
            .and_then(|value| value.parse::<usize>().ok())
        {
            config.max_line_length = value;
        }

        Ok(config)
    }
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            bind_addr: SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 4000),
            io_timeout: Duration::from_secs(2),
            line_ending: LineEnding::Lf,
            max_line_length: 8 * 1024,
        }
    }
}

#[derive(Clone)]
pub struct ServerHandle {
    local_addr: SocketAddr,
    shutdown_tx: watch::Sender<bool>,
}

impl ServerHandle {
    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    pub fn tcp_port(&self) -> u16 {
        self.local_addr.port()
    }

    pub fn shutdown(&self) {
        let _ = self.shutdown_tx.send(true);
    }
}

pub struct Server {
    handle: ServerHandle,
    join: Option<JoinHandle<Result<(), AsciiTcpError>>>,
}

impl Server {
    pub async fn start<F, Fut>(config: ServerConfig, handler: F) -> Result<Self, AsciiTcpError>
    where
        F: Fn(String, SocketAddr) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<String, AsciiTcpError>> + Send + 'static,
    {
        let listener = TcpListener::bind(config.bind_addr).await?;
        let local_addr = listener.local_addr()?;
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let handler: Handler = Arc::new(move |request, peer| Box::pin(handler(request, peer)));

        let join = tokio::spawn(run_server(listener, config, handler, shutdown_rx));
        Ok(Self {
            handle: ServerHandle {
                local_addr,
                shutdown_tx,
            },
            join: Some(join),
        })
    }

    pub async fn start_from_env<F, Fut>(handler: F) -> Result<Self, AsciiTcpError>
    where
        F: Fn(String, SocketAddr) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<String, AsciiTcpError>> + Send + 'static,
    {
        Self::start(ServerConfig::from_env()?, handler).await
    }

    pub async fn start_isolated<F, Fut>(handler: F) -> Result<Self, AsciiTcpError>
    where
        F: Fn(String, SocketAddr) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<String, AsciiTcpError>> + Send + 'static,
    {
        let config = ServerConfig {
            bind_addr: SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0),
            ..ServerConfig::default()
        };
        Self::start(config, handler).await
    }

    pub fn handle(&self) -> ServerHandle {
        self.handle.clone()
    }

    pub fn local_addr(&self) -> SocketAddr {
        self.handle.local_addr()
    }

    pub fn tcp_port(&self) -> u16 {
        self.handle.tcp_port()
    }

    pub async fn stop_drop(mut self) -> Result<(), AsciiTcpError> {
        self.handle.shutdown();
        let join = self.join.take().ok_or(AsciiTcpError::ServerStopped)?;
        join.await.map_err(|_| AsciiTcpError::ServerStopped)?
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        self.handle.shutdown();
    }
}

async fn run_server(
    listener: TcpListener,
    config: ServerConfig,
    handler: Handler,
    mut shutdown_rx: watch::Receiver<bool>,
) -> Result<(), AsciiTcpError> {
    let mut connections = JoinSet::new();

    loop {
        tokio::select! {
            changed = shutdown_rx.changed() => {
                if changed.is_err() || *shutdown_rx.borrow() {
                    break;
                }
            }
            accepted = listener.accept() => {
                let (stream, peer) = accepted?;
                let handler = Arc::clone(&handler);
                let config = config.clone();
                connections.spawn(async move {
                    if let Err(error) = serve_connection(stream, peer, config, handler).await {
                        debug!(%peer, error = %error, "TCP client disconnected");
                    }
                });
            }
            completed = connections.join_next(), if !connections.is_empty() => {
                if let Some(Err(error)) = completed {
                    warn!(error = %error, "TCP connection task failed");
                }
            }
        }
    }

    connections.abort_all();
    while connections.join_next().await.is_some() {}
    Ok(())
}

async fn serve_connection(
    stream: TcpStream,
    peer: SocketAddr,
    config: ServerConfig,
    handler: Handler,
) -> Result<(), AsciiTcpError> {
    let (reader, mut writer) = stream.into_split();
    let mut reader = BufReader::new(reader);

    loop {
        let request = tokio::time::timeout(
            config.io_timeout,
            read_ascii_line(
                &mut reader,
                config.line_ending,
                Some(config.max_line_length),
            ),
        )
        .await
        .map_err(|_| AsciiTcpError::IoTimeout)??;

        let Some(request) = request else {
            return Ok(());
        };

        let response = handler(request, peer).await?;
        let outbound = format!("{}{}", response, config.line_ending.as_str());
        tokio::time::timeout(config.io_timeout, async {
            writer.write_all(outbound.as_bytes()).await?;
            writer.flush().await
        })
        .await
        .map_err(|_| AsciiTcpError::IoTimeout)??;
    }
}
