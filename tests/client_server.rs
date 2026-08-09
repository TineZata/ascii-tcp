use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use ascii_tcp::{
    exchange_line, AsciiTcpConfig, ClientConfig, Command, CommandResultPayload, Context,
    HardwareProtocol, LineEnding, Payload, Server, ServerConfig,
};

#[tokio::test]
async fn server_supports_all_line_endings() {
    for line_ending in [LineEnding::Lf, LineEnding::CrLf, LineEnding::Cr] {
        let server = Server::start(
            ServerConfig {
                bind_addr: "127.0.0.1:0".parse().unwrap(),
                line_ending,
                ..ServerConfig::default()
            },
            |request, _peer| async move { Ok(request.to_uppercase()) },
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

#[tokio::test]
async fn context_executes_payload_and_emits_connected_once() {
    let server =
        Server::start_isolated(|request, _peer| async move { Ok(format!("response:{request}")) })
            .await
            .unwrap();
    let connected = Arc::new(AtomicUsize::new(0));
    let context = Context::new(ClientConfig {
        retry_interval: Duration::from_millis(1),
        retry_forever: false,
    });
    let connected_for_callback = Arc::clone(&connected);
    context.on_connected(move |_endpoint| {
        connected_for_callback.fetch_add(1, Ordering::SeqCst);
    });

    let command = Command {
        id: Some("temperature".to_string()),
        request: "READ".to_string(),
        protocol: HardwareProtocol {
            kind: "ascii-tcp".to_string(),
            host: server.local_addr().ip().to_string(),
            port: server.tcp_port(),
            line_ending: LineEnding::Lf,
            connect_timeout_ms: Some(500),
            io_timeout_ms: Some(500),
        },
    };

    for _ in 0..2 {
        let result = context
            .execute_payload_once(&Payload::Single(command.clone()))
            .await
            .unwrap();
        let CommandResultPayload::Single(result) = result else {
            panic!("expected a single result");
        };
        assert_eq!(result.response, "response:READ");
    }

    assert_eq!(connected.load(Ordering::SeqCst), 1);
    server.stop_drop().await.unwrap();
}

#[tokio::test]
async fn context_retries_until_server_becomes_available() {
    let port_reservation = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let server_addr = port_reservation.local_addr().unwrap();
    drop(port_reservation);

    let server_task = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(100)).await;
        Server::start(
            ServerConfig {
                bind_addr: server_addr,
                ..ServerConfig::default()
            },
            |request, _peer| async move { Ok(format!("response:{request}")) },
        )
        .await
        .unwrap()
    });

    let context = Context::new(ClientConfig {
        retry_interval: Duration::from_millis(25),
        retry_forever: true,
    });
    let disconnected = Arc::new(AtomicUsize::new(0));
    let disconnected_for_callback = Arc::clone(&disconnected);
    context.on_disconnected(move |_endpoint, _reason| {
        disconnected_for_callback.fetch_add(1, Ordering::SeqCst);
    });
    let payload = Payload::Single(Command {
        id: None,
        request: "READ".to_string(),
        protocol: HardwareProtocol {
            kind: "ascii-tcp".to_string(),
            host: server_addr.ip().to_string(),
            port: server_addr.port(),
            line_ending: LineEnding::Lf,
            connect_timeout_ms: Some(25),
            io_timeout_ms: Some(500),
        },
    });

    let result = tokio::time::timeout(Duration::from_secs(2), context.execute_payload(&payload))
        .await
        .expect("retrying operation timed out")
        .unwrap();
    let CommandResultPayload::Single(result) = result else {
        panic!("expected a single result");
    };
    assert_eq!(result.response, "response:READ");
    assert!(disconnected.load(Ordering::SeqCst) >= 2);

    server_task.await.unwrap().stop_drop().await.unwrap();
}
