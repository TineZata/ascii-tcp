//! `Server`, `ServerContext`, `ServerHandle` and `RequestHandler` behaviour, tested over real sockets.

use std::net::SocketAddr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use ascii_tcp::{AsciiTcpError, LineEnding, RequestHandler, Server, ServerContext};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;

fn localhost() -> SocketAddr {
    "127.0.0.1:0".parse().unwrap()
}

async fn connect(server: &Server) -> TcpStream {
    TcpStream::connect(server.local_addr()).await.unwrap()
}

/// Sends one LF-terminated line and reads one line back.
async fn round_trip(stream: &mut TcpStream, request: &str) -> String {
    stream.write_all(format!("{request}\n").as_bytes()).await.unwrap();
    let mut line = String::new();
    BufReader::new(stream).read_line(&mut line).await.unwrap();
    line.trim_end().to_string()
}

/// Reads until the server closes the connection; returns whatever it sent before that.
async fn read_until_closed(stream: &mut TcpStream) -> Vec<u8> {
    let mut received = Vec::new();
    let _ = tokio::time::timeout(Duration::from_secs(2), stream.read_to_end(&mut received))
        .await
        .expect("server did not close the connection");
    received
}

#[test]
fn context_defaults() {
    let ctx = ServerContext::default();
    assert_eq!(ctx.bind_addr, "127.0.0.1:4000".parse::<SocketAddr>().unwrap());
    assert_eq!(ctx.io_timeout, Duration::from_secs(2));
    assert!(matches!(ctx.line_ending, LineEnding::Lf));
    assert_eq!(ctx.max_line_length, 8 * 1024);
}

/// Starts a server with the default (echo) handler on a random port.
async fn start_echo() -> Server {
    Server::start(ServerContext { bind_addr: localhost(), ..ServerContext::default() })
        .await
        .unwrap()
}

#[tokio::test]
async fn default_handler_echoes_the_request() {
    let server = start_echo().await;
    let mut stream = connect(&server).await;

    assert_eq!(round_trip(&mut stream, "hello").await, "hello");
    server.stop_drop().await.unwrap();
}

#[tokio::test]
async fn request_handler_debug_hides_the_closure_and_default_echoes() {
    let handler = RequestHandler::default();
    assert_eq!(format!("{handler:?}"), "Handler(..)");

    let ctx = ServerContext::default();
    assert_eq!(format!("{:?}", ctx.handler), "Handler(..)");
    assert!(format!("{ctx:?}").contains("bind_addr"));
}

#[tokio::test]
async fn handler_receives_the_request_and_the_peer_address() {
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    let server = Server::start_isolated(move |request, peer| {
        let tx = tx.clone();
        async move {
            tx.send((request.clone(), peer)).unwrap();
            Ok(format!("ack {request}"))
        }
    })
    .await
    .unwrap();
    let mut stream = connect(&server).await;

    assert_eq!(round_trip(&mut stream, "ping").await, "ack ping");

    let (request, peer) = rx.recv().await.unwrap();
    assert_eq!(request, "ping");
    assert_eq!(peer, stream.local_addr().unwrap());
    server.stop_drop().await.unwrap();
}

#[tokio::test]
async fn one_connection_can_issue_many_requests_in_order() {
    let server = Server::start_isolated(|request, _| async move { Ok(request.to_uppercase()) })
        .await
        .unwrap();
    let mut stream = connect(&server).await;

    stream.write_all(b"a\nb\nc\n").await.unwrap();
    let mut lines = BufReader::new(stream).lines();
    for expected in ["A", "B", "C"] {
        assert_eq!(lines.next_line().await.unwrap().as_deref(), Some(expected));
    }
    server.stop_drop().await.unwrap();
}

#[tokio::test]
async fn many_clients_are_served_concurrently() {
    let seen = Arc::new(AtomicUsize::new(0));
    let counter = seen.clone();
    let server = Server::start_isolated(move |request, _| {
        let counter = counter.clone();
        async move {
            counter.fetch_add(1, Ordering::SeqCst);
            Ok(request)
        }
    })
    .await
    .unwrap();

    let clients = (0..8).map(|i| {
        let addr = server.local_addr();
        tokio::spawn(async move {
            let mut stream = TcpStream::connect(addr).await.unwrap();
            round_trip(&mut stream, &format!("client-{i}")).await
        })
    });
    for (i, client) in clients.enumerate() {
        assert_eq!(client.await.unwrap(), format!("client-{i}"));
    }

    assert_eq!(seen.load(Ordering::SeqCst), 8);
    server.stop_drop().await.unwrap();
}

#[tokio::test]
async fn responses_use_the_configured_line_ending() {
    for (line_ending, terminator) in [(LineEnding::CrLf, "\r\n"), (LineEnding::Cr, "\r")] {
        let server = Server::start(
            ServerContext { bind_addr: localhost(), line_ending, ..ServerContext::default() }
                .with_handler(|_, _| async { Ok("OK".to_string()) }),
        )
        .await
        .unwrap();
        let mut stream = connect(&server).await;

        stream.write_all(format!("X{terminator}").as_bytes()).await.unwrap();
        let mut reply = vec![0u8; "OK".len() + terminator.len()];
        stream.read_exact(&mut reply).await.unwrap();

        assert_eq!(reply, format!("OK{terminator}").as_bytes());
        server.stop_drop().await.unwrap();
    }
}

#[tokio::test]
async fn an_oversized_request_closes_the_connection_but_not_the_server() {
    let server = Server::start(
        ServerContext { bind_addr: localhost(), max_line_length: 8, ..ServerContext::default() },
    )
    .await
    .unwrap();

    let mut offender = connect(&server).await;
    offender.write_all(b"0123456789\n").await.unwrap();
    assert!(read_until_closed(&mut offender).await.is_empty());

    let mut well_behaved = connect(&server).await;
    assert_eq!(round_trip(&mut well_behaved, "short").await, "short");
    server.stop_drop().await.unwrap();
}

#[tokio::test]
async fn an_idle_connection_is_closed_after_the_io_timeout() {
    let server = Server::start(ServerContext {
        bind_addr: localhost(),
        io_timeout: Duration::from_millis(100),
        ..ServerContext::default()
    })
    .await
    .unwrap();
    let mut idle = connect(&server).await;

    assert!(read_until_closed(&mut idle).await.is_empty());
    server.stop_drop().await.unwrap();
}

#[tokio::test]
async fn a_handler_error_closes_only_that_connection() {
    let server = Server::start_isolated(|request, _| async move {
        if request == "FAIL" {
            Err(AsciiTcpError::ServerStopped)
        } else {
            Ok(request)
        }
    })
    .await
    .unwrap();

    let mut failing = connect(&server).await;
    failing.write_all(b"FAIL\n").await.unwrap();
    assert!(read_until_closed(&mut failing).await.is_empty());

    let mut other = connect(&server).await;
    assert_eq!(round_trip(&mut other, "fine").await, "fine");
    server.stop_drop().await.unwrap();
}

#[tokio::test]
async fn a_panicking_handler_does_not_take_the_server_down() {
    let server = Server::start_isolated(|request, _| async move {
        if request == "PANIC" {
            panic!("handler blew up");
        }
        Ok(request)
    })
    .await
    .unwrap();

    let mut crashing = connect(&server).await;
    crashing.write_all(b"PANIC\n").await.unwrap();
    assert!(read_until_closed(&mut crashing).await.is_empty());

    let mut other = connect(&server).await;
    assert_eq!(round_trip(&mut other, "alive").await, "alive");
    server.stop_drop().await.unwrap();
}

#[tokio::test]
async fn a_client_hanging_up_mid_line_is_tolerated() {
    let server = Server::start_isolated(|request, _| async move { Ok(request) }).await.unwrap();

    let mut partial = connect(&server).await;
    partial.write_all(b"no terminator").await.unwrap();
    drop(partial);

    let mut other = connect(&server).await;
    assert_eq!(round_trip(&mut other, "still here").await, "still here");
    server.stop_drop().await.unwrap();
}

#[tokio::test]
async fn start_reports_an_address_that_is_already_in_use() {
    let first = Server::start_isolated(|request, _| async move { Ok(request) }).await.unwrap();

    let result = Server::start(ServerContext {
        bind_addr: first.local_addr(),
        ..ServerContext::default()
    })
    .await;

    assert!(matches!(result, Err(AsciiTcpError::Io(_))));
    first.stop_drop().await.unwrap();
}

#[tokio::test]
async fn address_accessors_agree() {
    let server = Server::start_isolated(|request, _| async move { Ok(request) }).await.unwrap();
    let handle = server.handle();

    assert_ne!(server.tcp_port(), 0);
    assert_eq!(server.tcp_port(), server.local_addr().port());
    assert_eq!(handle.tcp_port(), server.tcp_port());
    assert_eq!(handle.local_addr(), server.local_addr());
    assert!(server.local_addr().ip().is_loopback());
    server.stop_drop().await.unwrap();
}

#[tokio::test]
async fn handle_shutdown_stops_the_server_and_outlives_it() {
    let server = Server::start_isolated(|request, _| async move { Ok(request) }).await.unwrap();
    let handle = server.handle();
    let addr = handle.local_addr();

    handle.shutdown();
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(TcpStream::connect(addr).await.is_err());

    // Shutting down again, or after the server is gone, is harmless.
    handle.shutdown();
    server.stop_drop().await.unwrap();
    handle.shutdown();
}

#[tokio::test]
async fn stop_drop_closes_established_connections() {
    let server = Server::start_isolated(|request, _| async move { Ok(request) }).await.unwrap();
    let mut stream = connect(&server).await;
    assert_eq!(round_trip(&mut stream, "x").await, "x");

    server.stop_drop().await.unwrap();

    assert!(read_until_closed(&mut stream).await.is_empty());
}

#[tokio::test]
async fn dropping_the_server_signals_shutdown() {
    let server = Server::start_isolated(|request, _| async move { Ok(request) }).await.unwrap();
    let addr = server.local_addr();

    drop(server);
    tokio::time::sleep(Duration::from_millis(100)).await;

    assert!(TcpStream::connect(addr).await.is_err());
}

#[tokio::test]
async fn with_handler_replaces_the_default_handler() {
    let ctx = ServerContext { bind_addr: localhost(), ..ServerContext::default() }
        .with_handler(|request, _| async move { Ok(request.chars().rev().collect()) });
    let server = Server::start(ctx).await.unwrap();
    let mut stream = connect(&server).await;

    assert_eq!(round_trip(&mut stream, "abc").await, "cba");
    server.stop_drop().await.unwrap();
}
