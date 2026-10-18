//! One persistent TCP connection per `host:port`, shared by every request through a `Context`.
//!
//! A reader task owns the read half and broadcasts every received line. Requests write on
//! the shared socket and wait for a line they accept, so unsolicited lines (greeting banners,
//! heartbeats) reach whoever is listening instead of being lost or mistaken for a reply.

use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;

use tokio::io::{AsyncWriteExt, BufReader};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::TcpStream;
use tokio::sync::{broadcast, Mutex, MutexGuard};
use tokio::task::JoinHandle;
use tracing::{debug, info, warn};

use crate::{read_ascii_line, AsciiTcpConfig, AsciiTcpError, LineEnding};

/// Maximum lines buffered per link for slow waiters; older lines are skipped when exceeded.
const LINE_BACKLOG: usize = 256;

static NEXT_LINK_ID: AtomicU64 = AtomicU64::new(1);

/// What the reader task broadcasts to waiting requests.
#[derive(Clone)]
enum Event {
    /// A complete line (without terminator) arrived.
    Line(String),
    /// The connection ended; no more lines will follow.
    Closed,
}

/// A persistent TCP connection managed by the endpoint.
/// 
/// The reader task continuously reads lines from the TCP connection and broadcasts them to all waiting requests.
/// A `Link` owns write half, broadcast sender event, open flag, received counter and reader task.
/// Dropping a Link aborts its reader task.
struct Link {
    /// monotonically increasing id (global atomic)
    id: u64,
    /// Owned write half of the TCP connection. This is used to send requests.
    writer: OwnedWriteHalf,
    /// Broadcast sender for events received by the reader task.
    events: broadcast::Sender<Event>,
    /// Indicates whether the connection is currently open.
    open: Arc<AtomicBool>,
    /// Lines received so far; lets a timed-out request tell a silent link from a merely unanswered command.
    /// Shared with the pending requests to track how many lines were received before they started waiting.
    received: Arc<AtomicUsize>,
    /// The reader task that continuously reads lines from the TCP connection.
    reader: JoinHandle<()>,
}

impl Link {
    fn is_open(&self) -> bool {
        self.open.load(Ordering::Acquire)
    }
}

impl Drop for Link {
    fn drop(&mut self) {
        self.reader.abort();
    }
}

/// What a request needs to wait for its reply after the link lock has been released.
struct Pending {
    /// The broadcast receiver for events from the link's reader task.
    events: broadcast::Receiver<Event>,
    /// Whether the link was reused for this request.
    reused: bool,
    /// The id of the link this request is waiting on.
    link_id: u64,
    /// The number of lines received before this request started waiting.
    received_before: usize,
}

/// The connection slot for one `host:port`: lazily connected, replaced when it breaks.
#[derive(Default)]
pub(crate) struct Endpoint {
    link: Mutex<Option<Link>>,
}

impl Endpoint {
    /// Sends `request` and returns the first received line `accept` approves.
    ///
    /// The link is held only while writing, so a request still waiting for its reply never
    /// blocks other requests on the same connection. Each waiter picks its own reply out of
    /// the shared line stream, so `accept` must be specific enough to tell replies apart.
    /// An empty `request` sends nothing and only waits for a matching line.
    pub(crate) async fn exchange<F>(
        &self,
        config: &AsciiTcpConfig,
        request: &str,
        accept: F,
    ) -> Result<String, AsciiTcpError>
    where
        F: Fn(&str) -> bool,
    {
        match self.try_exchange(config, request, &accept).await {
            Err((error, reused))
                if reused
                    && !request.is_empty()
                    && matches!(error, AsciiTcpError::Eof | AsciiTcpError::Io(_)) =>
            {
                // A reused socket can be half-open; the first use after the break fails
                // even though the endpoint is reachable.
                debug!(endpoint = %config.endpoint(), %error, "ascii-tcp retrying on a fresh connection");
                self.reset().await;
                // Retry a if the first connection attempt failed.
                // This is to counter the case where the first attempt failed due to a half-open connection.
                self.try_exchange(config, request, &accept)
                    .await
                    .map_err(|(error, _)| error)
            }
            other => other.map_err(|(error, _)| error),
        }
    }

    /// Sends `request` without waiting for a reply.
    ///
    /// A write failing on a reused socket is retried once on a fresh connection, since the
    /// old socket may have been half-open.
    pub(crate) async fn send(
        &self,
        config: &AsciiTcpConfig,
        request: &str,
    ) -> Result<(), AsciiTcpError> {
        let (mut slot, reused) = self.locked_link(config).await?;
        let first = write_request(slot.as_mut().expect("link was initialized"), config, request).await;
        let error = match first {
            Ok(()) => return Ok(()),
            Err(error) => error,
        };
        *slot = None;
        // Return early if the socket was not reused, as retrying only makes sense for reused sockets.
        if !reused {
            return Err(error);
        }

        // A write that failed on a reused socket has almost certainly not reached the peer.
        debug!(endpoint = %config.endpoint(), %error, "ascii-tcp retrying on a fresh connection");
        // Drop the old slot before attempting to acquire a fresh connection.
        drop(slot);
        // Attempt to acquire a fresh connection and retry the write.
        let (mut slot, _) = self.locked_link(config).await?;
        let result = write_request(slot.as_mut().expect("link was initialized"), config, request).await;
        if result.is_err() {
            *slot = None;
        }
        // Return the result of the retry attempt, whether it succeeded or failed.
        result
    }

    /// One attempt of `exchange`. On failure also reports whether the link was reused,
    /// which decides whether `exchange` may retry on a fresh connection.
    async fn try_exchange<F>(
        &self,
        config: &AsciiTcpConfig,
        request: &str,
        accept: &F,
    ) -> Result<String, (AsciiTcpError, bool)>
    where
        F: Fn(&str) -> bool,
    {
        let Pending { mut events, reused, link_id, received_before } = {
            let (mut slot, reused) = self.locked_link(config).await.map_err(|e| (e, false))?;
            let link = slot.as_mut().expect("link was initialized");
            let events = link.events.subscribe();
            // The reader clears `open` before it announces `Closed`, so a link still open
            // after subscribing is guaranteed to deliver that announcement.
            if !link.is_open() {
                *slot = None;
                return Err((AsciiTcpError::Eof, reused));
            }
            let link_id = link.id;
            let received_before = link.received.load(Ordering::Acquire);
            if !request.is_empty() {
                if let Err(error) = write_request(link, config, request).await {
                    *slot = None;
                    return Err((error, reused));
                }
            }
            Pending { events, reused, link_id, received_before }
        };

        let wait = tokio::time::timeout(config.io_timeout, async {
            loop {
                match events.recv().await {
                    Ok(Event::Line(line)) if accept(&line) => return Ok(line),
                    Ok(Event::Line(_)) | Err(broadcast::error::RecvError::Lagged(_)) => {}
                    Ok(Event::Closed) | Err(broadcast::error::RecvError::Closed) => {
                        return Err(AsciiTcpError::Eof)
                    }
                }
            }
        })
        .await;

        match wait {
            Ok(result) => result.map_err(|e| (e, reused)),
            Err(_) => {
                // An ignored or unknown command is not a reason to drop a connection other
                // requests are using. Only a link that was completely silent is suspect.
                if !request.is_empty() {
                    self.reset_if_silent(link_id, received_before).await;
                }
                Err((AsciiTcpError::IoTimeout, reused))
            }
        }
    }

    /// Locks the link slot (mutex), connecting first when there is no live connection.
    async fn locked_link(
        &self,
        config: &AsciiTcpConfig,
    ) -> Result<(MutexGuard<'_, Option<Link>>, bool), AsciiTcpError> {
        let mut slot = self.link.lock().await;
        if slot.as_ref().is_some_and(|link| !link.is_open()) {
            *slot = None;
        }
        let reused = slot.is_some();
        if !reused {
            *slot = Some(connect(config).await?);
        }
        Ok((slot, reused))
    }

    /// Drops the current connection, if any; the next request reconnects.
    async fn reset(&self) {
        *self.link.lock().await = None;
    }

    /// Drops link `link_id` if it has received nothing since `received_before`.
    async fn reset_if_silent(&self, link_id: u64, received_before: usize) {
        let mut slot = self.link.lock().await;
        let silent = slot.as_ref().is_some_and(|link| {
            link.id == link_id && link.received.load(Ordering::Acquire) == received_before
        });
        if silent {
            warn!("ascii-tcp link received nothing while a request waited; reconnecting");
            *slot = None;
        }
    }
}

/// Opens a connection and spawns the reader task that broadcasts incoming lines.
async fn connect(config: &AsciiTcpConfig) -> Result<Link, AsciiTcpError> {
    let endpoint = config.endpoint();
    debug!(endpoint, "ascii-tcp connecting");
    let stream = tokio::time::timeout(config.connect_timeout, TcpStream::connect(&endpoint))
        .await
        .map_err(|_| AsciiTcpError::ConnectTimeout)??;
    stream.set_nodelay(true)?;
    info!(endpoint, "ascii-tcp connected");

    let (reader, writer) = stream.into_split();
    let (events, _) = broadcast::channel(LINE_BACKLOG);
    let open = Arc::new(AtomicBool::new(true));
    let received = Arc::new(AtomicUsize::new(0));
    let reader = tokio::spawn(read_lines(
        BufReader::new(reader),
        config.line_ending,
        endpoint,
        events.clone(),
        open.clone(),
        received.clone(),
    ));

    Ok(Link {
        id: NEXT_LINK_ID.fetch_add(1, Ordering::Relaxed),
        writer,
        events,
        open,
        received,
        reader,
    })
}

/// Reader task body: broadcasts every line until EOF or a read error, then marks the link
/// closed (before announcing `Closed`, see `try_exchange`).
async fn read_lines(
    mut reader: BufReader<OwnedReadHalf>,
    line_ending: LineEnding,
    endpoint: String,
    events: broadcast::Sender<Event>,
    open: Arc<AtomicBool>,
    received: Arc<AtomicUsize>,
) {
    loop {
        match read_ascii_line(&mut reader, line_ending, None).await {
            Ok(Some(line)) => {
                info!(endpoint, line, "ascii-tcp RX");
                received.fetch_add(1, Ordering::Release);
                let _ = events.send(Event::Line(line));
            }
            Ok(None) => break,
            Err(error) => {
                warn!(endpoint, %error, "ascii-tcp read failed");
                break;
            }
        }
    }
    info!(endpoint, "ascii-tcp disconnected");
    open.store(false, Ordering::Release);
    let _ = events.send(Event::Closed);
}

/// Writes `request` plus the line terminator and flushes, bounded by `io_timeout`.
async fn write_request(
    link: &mut Link,
    config: &AsciiTcpConfig,
    request: &str,
) -> Result<(), AsciiTcpError> {
    let outbound = format!("{}{}", request, config.line_ending.as_str());
    tokio::time::timeout(config.io_timeout, async {
        link.writer.write_all(outbound.as_bytes()).await?;
        link.writer.flush().await
    })
    .await
    .map_err(|_| AsciiTcpError::IoTimeout)??;
    info!(endpoint = %config.endpoint(), request, "ascii-tcp TX");
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::future::Future;
    use std::sync::atomic::AtomicUsize;
    use std::time::Duration;

    use tokio::io::AsyncBufReadExt;
    use tokio::net::TcpListener;

    use super::*;

    /// Accepts connections forever, handing each (with its zero-based index) to `on_connection`.
    /// Returns the port and the number of connections accepted so far.
    async fn spawn_server<F, Fut>(on_connection: F) -> (u16, Arc<AtomicUsize>)
    where
        F: Fn(usize, TcpStream) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let accepted = Arc::new(AtomicUsize::new(0));
        let counter = accepted.clone();
        tokio::spawn(async move {
            loop {
                let (stream, _) = listener.accept().await.unwrap();
                let index = counter.fetch_add(1, Ordering::SeqCst);
                tokio::spawn(on_connection(index, stream));
            }
        });
        (port, accepted)
    }

    /// Echoes every line; keeps a half-closed connection open instead of closing it.
    async fn echo(stream: TcpStream) {
        let (reader, mut writer) = stream.into_split();
        let mut lines = BufReader::new(reader).lines();
        while let Ok(Some(line)) = lines.next_line().await {
            if writer.write_all(format!("{line}\n").as_bytes()).await.is_err() {
                return;
            }
        }
        std::future::pending::<()>().await;
    }

    /// Holds the connection open without ever reading or writing.
    async fn stall(_stream: TcpStream) {
        std::future::pending::<()>().await;
    }

    /// A loopback port nothing is listening on. The refusal is verified so that a port already
    /// recycled by a concurrently running test is skipped.
    fn closed_port() -> u16 {
        loop {
            let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            let addr = listener.local_addr().unwrap();
            drop(listener);
            if std::net::TcpStream::connect_timeout(&addr, Duration::from_millis(200)).is_err() {
                return addr.port();
            }
        }
    }
    fn config(port: u16) -> AsciiTcpConfig {
        AsciiTcpConfig {
            port,
            connect_timeout: Duration::from_millis(500),
            io_timeout: Duration::from_millis(300),
            ..AsciiTcpConfig::default()
        }
    }

    /// Connections accepted so far, after giving the accept loop a moment to catch up.
    async fn accepted_count(accepted: &AtomicUsize) -> usize {
        tokio::time::sleep(Duration::from_millis(50)).await;
        accepted.load(Ordering::SeqCst)
    }

    #[tokio::test]
    async fn exchange_returns_the_matching_line_over_one_shared_connection() {
        let (port, accepted) = spawn_server(|_, stream| echo(stream)).await;
        let endpoint = Endpoint::default();

        for request in ["one", "two"] {
            let reply = endpoint.exchange(&config(port), request, |line| line == request).await;
            assert_eq!(reply.unwrap(), request);
        }
        assert_eq!(accepted_count(&accepted).await, 1);
    }

    #[tokio::test]
    async fn exchange_fails_when_nothing_is_listening() {
        let port = closed_port();

        let error = Endpoint::default().exchange(&config(port), "READ", |_| true).await.unwrap_err();
        assert!(matches!(error, AsciiTcpError::Io(_) | AsciiTcpError::ConnectTimeout));
    }

    #[tokio::test]
    async fn exchange_gives_up_on_an_unreachable_host() {
        // TEST-NET-1 never answers; hosts without a route fail immediately with an I/O error instead.
        let config = AsciiTcpConfig {
            host: "192.0.2.1".to_string(),
            connect_timeout: Duration::from_millis(100),
            ..config(9)
        };

        let error = Endpoint::default().exchange(&config, "READ", |_| true).await.unwrap_err();
        assert!(matches!(error, AsciiTcpError::ConnectTimeout | AsciiTcpError::Io(_)), "got {error:?}");
    }

    #[tokio::test]
    async fn exchange_retries_on_a_fresh_connection_when_a_reused_link_dies_mid_request() {
        let (port, accepted) = spawn_server(|index, stream| async move {
            if index > 0 {
                return echo(stream).await;
            }
            // The first connection answers PING, then hangs up on anything else.
            let (reader, mut writer) = stream.into_split();
            let mut lines = BufReader::new(reader).lines();
            while let Ok(Some(line)) = lines.next_line().await {
                if line != "PING" || writer.write_all(b"PONG\n").await.is_err() {
                    return;
                }
            }
        })
        .await;
        let endpoint = Endpoint::default();

        let pong = endpoint.exchange(&config(port), "PING", |line| line == "PONG").await;
        assert_eq!(pong.unwrap(), "PONG");

        let echoed = endpoint.exchange(&config(port), "QUERY", |line| line == "QUERY").await;
        assert_eq!(echoed.unwrap(), "QUERY");
        assert_eq!(accepted_count(&accepted).await, 2);
    }

    #[tokio::test]
    async fn exchange_retries_when_the_reused_writer_is_broken() {
        let (port, accepted) = spawn_server(|_, stream| echo(stream)).await;
        let endpoint = Endpoint::default();
        endpoint.exchange(&config(port), "first", |line| line == "first").await.unwrap();

        // Writes now fail while the link still looks open.
        let mut slot = endpoint.link.lock().await;
        slot.as_mut().unwrap().writer.shutdown().await.unwrap();
        drop(slot);

        let reply = endpoint.exchange(&config(port), "second", |line| line == "second").await;
        assert_eq!(reply.unwrap(), "second");
        assert_eq!(accepted_count(&accepted).await, 2);
    }

    #[tokio::test]
    async fn exchange_drops_a_silent_link_after_a_timeout() {
        let (port, accepted) = spawn_server(|_, stream| stall(stream)).await;
        let config = AsciiTcpConfig {
            io_timeout: Duration::from_millis(100),
            ..config(port)
        };
        let endpoint = Endpoint::default();

        let error = endpoint.exchange(&config, "READ", |_| true).await.unwrap_err();

        assert!(matches!(error, AsciiTcpError::IoTimeout), "got {error:?}");
        assert!(endpoint.link.lock().await.is_none());
        assert_eq!(accepted_count(&accepted).await, 1);
    }

    #[tokio::test]
    async fn exchange_keeps_a_link_that_is_still_receiving_after_a_timeout() {
        let (port, _) = spawn_server(|_, mut stream| async move {
            for _ in 0..40 {
                if stream.write_all(b"ALIVE\n").await.is_err() {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await;
        let config = AsciiTcpConfig {
            io_timeout: Duration::from_millis(100),
            ..config(port)
        };
        let endpoint = Endpoint::default();

        let error = endpoint.exchange(&config, "READ", |line| line == "NEVER").await.unwrap_err();

        assert!(matches!(error, AsciiTcpError::IoTimeout), "got {error:?}");
        assert!(endpoint.link.lock().await.is_some());
    }

    #[tokio::test]
    async fn exchange_reports_eof_when_the_peer_sends_a_broken_line_and_hangs_up() {
        for garbage in [&b"partial"[..], &b"\xff\xfe\n"[..]] {
            let (port, _) = spawn_server(move |_, mut stream| async move {
                stream.write_all(garbage).await.unwrap();
            })
            .await;

            // An empty request only listens, so there is no retry and the error surfaces as is.
            let error = Endpoint::default().exchange(&config(port), "", |_| true).await.unwrap_err();
            assert!(matches!(error, AsciiTcpError::Eof), "got {error:?}");
        }
    }

    #[tokio::test]
    async fn exchange_survives_a_flood_of_unwanted_lines() {
        let (port, _) = spawn_server(|_, mut stream| async move {
            let mut flood = "x\n".repeat(5_000);
            flood.push_str("END\n");
            stream.write_all(flood.as_bytes()).await.unwrap();
            std::future::pending::<()>().await;
        })
        .await;
        let config = AsciiTcpConfig {
            io_timeout: Duration::from_secs(2),
            ..config(port)
        };

        let reply = Endpoint::default().exchange(&config, "", |line| line == "END").await;
        assert_eq!(reply.unwrap(), "END");
    }

    #[tokio::test]
    async fn a_waiter_fails_cleanly_when_another_request_resets_the_silent_link() {
        let (port, _) = spawn_server(|_, stream| stall(stream)).await;
        let endpoint = Arc::new(Endpoint::default());

        let impatient = {
            let endpoint = endpoint.clone();
            let config = AsciiTcpConfig { io_timeout: Duration::from_millis(100), ..config(port) };
            tokio::spawn(async move { endpoint.exchange(&config, "A", |_| false).await })
        };
        tokio::time::sleep(Duration::from_millis(30)).await;
        let patient_config = AsciiTcpConfig { io_timeout: Duration::from_millis(500), ..config(port) };
        let patient = endpoint.exchange(&patient_config, "B", |_| false).await;

        assert!(matches!(impatient.await.unwrap(), Err(AsciiTcpError::IoTimeout)));
        assert!(patient.is_err());
    }

    #[tokio::test]
    async fn send_writes_on_the_shared_connection() {
        let (port, accepted) = spawn_server(|_, stream| echo(stream)).await;
        let endpoint = Endpoint::default();

        endpoint.send(&config(port), "A").await.unwrap();
        endpoint.send(&config(port), "B").await.unwrap();

        assert_eq!(accepted_count(&accepted).await, 1);
    }

    #[tokio::test]
    async fn send_retries_on_a_fresh_connection_when_the_reused_writer_is_broken() {
        let (port, accepted) = spawn_server(|_, stream| echo(stream)).await;
        let endpoint = Endpoint::default();
        endpoint.send(&config(port), "A").await.unwrap();

        let mut slot = endpoint.link.lock().await;
        slot.as_mut().unwrap().writer.shutdown().await.unwrap();
        drop(slot);

        endpoint.send(&config(port), "B").await.unwrap();
        assert_eq!(accepted_count(&accepted).await, 2);
    }

    #[tokio::test]
    async fn send_fails_when_nothing_is_listening() {
        let port = closed_port();

        let error = Endpoint::default().send(&config(port), "A").await.unwrap_err();
        assert!(matches!(error, AsciiTcpError::Io(_) | AsciiTcpError::ConnectTimeout));
    }

    #[tokio::test]
    async fn send_reconnects_after_the_peer_closes_the_link() {
        let (port, accepted) = spawn_server(|index, mut stream| async move {
            if index == 0 {
                let _ = stream.shutdown().await;
            } else {
                echo(stream).await;
            }
        })
        .await;
        let endpoint = Endpoint::default();
        let _ = endpoint.send(&config(port), "A").await;
        tokio::time::sleep(Duration::from_millis(100)).await;

        endpoint.send(&config(port), "B").await.unwrap();
        assert_eq!(accepted_count(&accepted).await, 2);
    }
}
