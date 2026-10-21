//! Integration tests that double as usage examples for the client and server APIs.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use ascii_tcp::{
    exchange_line, AsciiTcpConfig, AsciiTcpError, ClientConfig, Command, CommandResultPayload,
    Context, HardwareProtocol, LineEnding, Payload, Server, ServerContext, parse_first_value,
};

/// Builds the protocol block a client needs to reach `addr` over ascii-tcp.
fn protocol_for(addr: std::net::SocketAddr, line_ending: LineEnding) -> HardwareProtocol {
    HardwareProtocol {
        kind: "ascii-tcp".to_string(),
        host: addr.ip().to_string(),
        port: addr.port(),
        line_ending,
        connect_timeout_ms: Some(500),
        io_timeout_ms: Some(500),
    }
}

#[tokio::test]
async fn context_fails_to_connect_when_no_server_is_listening() {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);

    let payload = Payload::Single(Command {
        id: Some("missing-server".to_string()),
        request: "READ".to_string(),
        protocol: HardwareProtocol {
            connect_timeout_ms: Some(50),
            io_timeout_ms: Some(50),
            ..protocol_for(addr, LineEnding::Lf)
        },
    });

    let context = Context::new(ClientConfig {
        retry_interval: Duration::from_millis(1),
        retry_forever: false,
    });

    let error = context.execute_payload_once(&payload).await.unwrap_err();
    assert!(matches!(error, AsciiTcpError::Io(_) | AsciiTcpError::ConnectTimeout),
        "expected a connect-related error when no server is listening, got: {error:?}");
}

/// `Server` accepts LF, CRLF, and CR framed lines. `exchange_line` is the low-level,
/// one-request client API that talks to it directly, without going through `Context`.
#[tokio::test]
async fn server_echoes_uppercase_for_every_line_ending() {
    for line_ending in [LineEnding::Lf, LineEnding::CrLf, LineEnding::Cr] {
        let server = Server::start(
            ServerContext {
                bind_addr: "127.0.0.1:0".parse().unwrap(),
                line_ending,
                ..ServerContext::default()
            }
            .with_handler(|request, _peer| async move { Ok(request.to_uppercase()) }),
        )
        .await
        .unwrap();

        let config = AsciiTcpConfig {
            host: server.local_addr().ip().to_string(),
            port: server.tcp_port(),
            line_ending,
            ..AsciiTcpConfig::default()
        };

        assert_eq!(exchange_line(&config, "first").await.unwrap(), "FIRST");
        assert_eq!(exchange_line(&config, "second").await.unwrap(), "SECOND");

        server.stop_drop().await.unwrap();
    }
}

/// `Context` is the higher-level client API: it runs a `Payload` (a single command or a
/// batch) and reports a "connected" event the first time it reaches an endpoint, but not
/// again on subsequent successful calls to the same endpoint.
#[tokio::test]
async fn context_executes_a_single_command_and_reports_connection_once() {
    let server =
        Server::start_isolated(|request, _peer| async move { Ok(format!("response:{request}")) })
            .await
            .unwrap();

    let connected_count = Arc::new(AtomicUsize::new(0));
    let context = Context::new(ClientConfig {
        retry_interval: Duration::from_millis(1),
        retry_forever: false,
    });
    let connected_count_handle = Arc::clone(&connected_count);
    context.on_connected(move |_endpoint| {
        connected_count_handle.fetch_add(1, Ordering::SeqCst);
    });

    let payload = Payload::Single(Command {
        id: Some("temperature".to_string()),
        request: "READ".to_string(),
        protocol: protocol_for(server.local_addr(), LineEnding::Lf),
    });

    // Run the same command twice: the "connected" event should only fire once.
    for _ in 0..2 {
        let result = context.execute_payload_once(&payload).await.unwrap();
        let CommandResultPayload::Single(result) = result else {
            panic!("expected a single result because the payload held a single command");
        };
        assert_eq!(result.response, "response:READ");
    }

    assert_eq!(connected_count.load(Ordering::SeqCst), 1);
    server.stop_drop().await.unwrap();
}

/// Unlike `execute_payload_once`, `execute_payload` retries on connection failure until
/// it succeeds. Here the server doesn't start listening until after the client has
/// already tried and failed at least once, proving the retry behavior actually kicks in.
#[tokio::test]
async fn context_retries_until_the_server_comes_online() {
    // Reserve a port up front, then free it so the server can bind the same address later.
    let reserved_port = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let server_addr = reserved_port.local_addr().unwrap();
    drop(reserved_port);

    let server_task = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(100)).await;
        Server::start(
            ServerContext {
                bind_addr: server_addr,
                ..ServerContext::default()
            }
            .with_handler(|request, _peer| async move { Ok(format!("response:{request}")) }),
        )
        .await
        .unwrap()
    });

    let context = Context::new(ClientConfig {
        retry_interval: Duration::from_millis(25),
        retry_forever: true,
    });
    let disconnected_count = Arc::new(AtomicUsize::new(0));
    let disconnected_count_handle = Arc::clone(&disconnected_count);
    context.on_disconnected(move |_endpoint, _reason| {
        disconnected_count_handle.fetch_add(1, Ordering::SeqCst);
    });

    let payload = Payload::Single(Command {
        id: None,
        request: "READ".to_string(),
        protocol: HardwareProtocol {
            connect_timeout_ms: Some(25),
            ..protocol_for(server_addr, LineEnding::Lf)
        },
    });

    let result = tokio::time::timeout(Duration::from_secs(2), context.execute_payload(&payload))
        .await
        .expect("retrying operation timed out")
        .unwrap();
    let CommandResultPayload::Single(result) = result else {
        panic!("expected a single result because the payload held a single command");
    };
    assert_eq!(result.response, "response:READ");
    assert!(
        disconnected_count.load(Ordering::SeqCst) >= 2,
        "expected at least two failed connection attempts before the server came online"
    );

    server_task.await.unwrap().stop_drop().await.unwrap();
}

/// A JSON batch fans out to two servers with different line endings; responses come back in
/// command order with their ids, and a `ResponseTemplate` extracts the typed value.
#[tokio::test]
async fn context_runs_a_json_batch_against_multiple_servers() {
    let temperature = Server::start(
        ServerContext {
            bind_addr: "127.0.0.1:0".parse().unwrap(),
            line_ending: LineEnding::CrLf,
            ..ServerContext::default()
        }
        .with_handler(|request, _peer| async move {
            match request.as_str() {
                "TEMP?" => Ok("TEMP=22.5 DegC".to_string()),
                other => Ok(format!("ERR unknown {other}")),
            }
        }),
    )
    .await
    .unwrap();
    let echo = Server::start_isolated(|request, _peer| async move { Ok(request) })
        .await
        .unwrap();

    let json = format!(
        r#"{{ "commands": [
            {{ "id": "temp", "request": "TEMP?", "protocol": {{ "type": "ascii-tcp", "host": "127.0.0.1", "port": {}, "line_ending": "crlf" }} }},
            {{ "id": "echo", "request": "HELLO", "protocol": {{ "type": "ascii-tcp", "host": "127.0.0.1", "port": {}, "line_ending": "lf" }} }}
        ] }}"#,
        temperature.tcp_port(),
        echo.tcp_port()
    );

    let context = Context::new(ClientConfig {
        retry_interval: Duration::from_millis(1),
        retry_forever: false,
    });
    let CommandResultPayload::Batch(batch) = context.execute_json(&json).await.unwrap() else {
        panic!("expected a batch result because the payload held a batch");
    };

    assert_eq!(batch.responses.len(), 2);
    assert_eq!(batch.responses[0].id.as_deref(), Some("temp"));
    assert_eq!(batch.responses[1].id.as_deref(), Some("echo"));
    assert_eq!(batch.responses[1].response, "HELLO");

    let value = parse_first_value("TEMP=%f DegC", &batch.responses[0].response).unwrap();
    assert_eq!(value.as_f64(), Some(22.5));

    temperature.stop_drop().await.unwrap();
    echo.stop_drop().await.unwrap();
}
