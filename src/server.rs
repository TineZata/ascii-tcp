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
type HandlerFn = dyn Fn(String, SocketAddr) -> HandlerFuture + Send + Sync;

/// The request handler: maps each request line (and the peer address) to a response line.
#[derive(Clone)]
pub struct RequestHandler(Arc<HandlerFn>);

impl RequestHandler {
    /// Wraps an async function or closure as a `Handler`.
    pub fn new<F, Fut>(handler: F) -> Self
    where
        F: Fn(String, SocketAddr) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<String, AsciiTcpError>> + Send + 'static,
    {
        Self(Arc::new(move |request, peer| Box::pin(handler(request, peer))))
    }

    fn call(&self, request: String, peer: SocketAddr) -> HandlerFuture {
        (self.0)(request, peer)
    }
}

impl Default for RequestHandler {
    /// Echoes each request back as the response.
    fn default() -> Self {
        Self::new(|request, _peer| async move { Ok(request) })
    }
}

impl std::fmt::Debug for RequestHandler {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Handler(..)")
    }
}

/// Configuration for a [`Server`].
#[derive(Debug, Clone)]
pub struct ServerContext {
    /// The address to bind the server to. Use `SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 4000)` for the default.
    pub bind_addr: SocketAddr,
    /// The I/O timeout for reading requests and writing responses.
    pub io_timeout: Duration,
    /// The line ending to use for framing requests and responses.
    pub line_ending: LineEnding,
    /// The maximum length of a request line, in bytes. Requests longer than this will be
    /// rejected with an error.
    pub max_line_length: usize,
    /// Called for every request. Defaults to echoing the request back.
    pub handler: RequestHandler,
}

impl ServerContext {
    /// Replaces the request handler.
    pub fn with_handler<F, Fut>(mut self, handler: F) -> Self
    where
        F: Fn(String, SocketAddr) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<String, AsciiTcpError>> + Send + 'static,
    {
        self.handler = RequestHandler::new(handler);
        self
    }

    /// Creates a `ServerContext` from environment variables. The following variables are supported;
    /// values that cannot be parsed are ignored, except an invalid address, which is an error:
    /// -   `ASCII_TCP_SERVER_ADDR`: The address to bind the server to, in the form `IP:PORT`.
    ///     If not set, defaults to `127.0.0.1:4000`.
    /// -   `ASCII_TCP_SERVER_IO_TIMEOUT_MS`: The I/O timeout for reading requests and writing responses,
    ///     in milliseconds. If not set, defaults to 2000.
    /// -   `ASCII_TCP_SERVER_MAX_LINE_LENGTH`: The maximum length of a request line, in bytes.
    ///     Requests longer than this will be rejected with an error. If not set, defaults to 8192 (8 KiB).
    pub fn from_env() -> Result<Self, AsciiTcpError> {
        let mut ctx = Self::default();

        if let Ok(value) = std::env::var("ASCII_TCP_SERVER_ADDR") {
            ctx.bind_addr = value.parse().map_err(|error| {
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
            ctx.io_timeout = Duration::from_millis(value);
        }
        if let Some(value) = std::env::var("ASCII_TCP_SERVER_MAX_LINE_LENGTH")
            .ok()
            .and_then(|value| value.parse::<usize>().ok())
        {
            ctx.max_line_length = value;
        }

        Ok(ctx)
    }
}

impl Default for ServerContext {
    /// Binds `127.0.0.1:4000`, 2 s I/O timeout, LF line ending, 8 KiB max line, echo handler.
    fn default() -> Self {
        Self {
            bind_addr: SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 4000),
            io_timeout: Duration::from_secs(2),
            line_ending: LineEnding::Lf,
            max_line_length: 8 * 1024,
            handler: RequestHandler::default(),
        }
    }
}

/// A cloneable address + shutdown handle that stays valid after [`Server`] is dropped or stopped.
#[derive(Clone)]
pub struct ServerHandle {
    local_addr: SocketAddr,
    shutdown_tx: watch::Sender<bool>,
}

impl ServerHandle {
    /// Returns the local address the server is bound to.
    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    /// Returns the TCP port the server is listening on.
    pub fn tcp_port(&self) -> u16 {
        self.local_addr.port()
    }

    /// Signals the server to stop accepting new connections and shut down.
    pub fn shutdown(&self) {
        let _ = self.shutdown_tx.send(true);
    }
}

/// A running ASCII-TCP server; dropping it signals shutdown but does not wait for the task to finish.
pub struct Server {
    handle: ServerHandle,
    join: Option<JoinHandle<Result<(), AsciiTcpError>>>,
}

impl Server {
    /// Starts a server bound to `ctx.bind_addr`, calling `ctx.handler` for every request.
    pub async fn start(ctx: ServerContext) -> Result<Self, AsciiTcpError> {
        let listener = TcpListener::bind(ctx.bind_addr).await?;
        let local_addr = listener.local_addr()?;
        let (shutdown_tx, shutdown_rx) = watch::channel(false);

        let join = tokio::spawn(run_server(listener, ctx, shutdown_rx));
        Ok(Self {
            handle: ServerHandle {
                local_addr,
                shutdown_tx,
            },
            join: Some(join),
        })
    }

    /// Starts a server using `ServerContext::from_env` for configuration and `handler` for requests.
    pub async fn start_from_env<F, Fut>(handler: F) -> Result<Self, AsciiTcpError>
    where
        F: Fn(String, SocketAddr) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<String, AsciiTcpError>> + Send + 'static,
    {
        Self::start(ServerContext::from_env()?.with_handler(handler)).await
    }

    /// Starts a server on a random available port on localhost, useful for tests and in-process demos.
    pub async fn start_isolated<F, Fut>(handler: F) -> Result<Self, AsciiTcpError>
    where
        F: Fn(String, SocketAddr) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<String, AsciiTcpError>> + Send + 'static,
    {
        let ctx = ServerContext {
            bind_addr: SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0),
            ..ServerContext::default()
        };
        Self::start(ctx.with_handler(handler)).await
    }

    /// Returns a cloneable `ServerHandle` for shutdown signalling without consuming `self`.
    pub fn handle(&self) -> ServerHandle {
        self.handle.clone()
    }

    /// Returns the local address the server is bound to.
    pub fn local_addr(&self) -> SocketAddr {
        self.handle.local_addr()
    }

    /// Returns the TCP port the server is listening on.
    pub fn tcp_port(&self) -> u16 {
        self.handle.tcp_port()
    }

    /// Shuts down the server and waits for its task to finish.
    ///
    /// Returns [`AsciiTcpError::ServerStopped`] if the task panicked or was cancelled.
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

/// Accept loop: spawns one task per connection until shutdown is signalled, then aborts them all.
async fn run_server(
    listener: TcpListener,
    ctx: ServerContext,
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
                let ctx = ctx.clone();
                connections.spawn(async move {
                    if let Err(error) = serve_connection(stream, peer, ctx).await {
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

/// Serves one client: reads a request line, calls the handler, writes the response line, repeat.
/// Returns `Ok(())` when the client closes the connection cleanly; any error (idle timeout,
/// oversized line, handler error, I/O error) ends the connection.
async fn serve_connection(
    stream: TcpStream,
    peer: SocketAddr,
    config: ServerContext,
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

        let response = config.handler.call(request, peer).await?;
        let outbound = format!("{}{}", response, config.line_ending.as_str());
        tokio::time::timeout(config.io_timeout, async {
            writer.write_all(outbound.as_bytes()).await?;
            writer.flush().await
        })
        .await
        .map_err(|_| AsciiTcpError::IoTimeout)??;
    }
}
