//! `ChannelConfig`, `ClientConfig` and `Context`: configuration, payload execution, retries and callbacks.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use ascii_tcp::{
    AsciiResponseMode, AsciiTcpError, ChannelConfig, ClientConfig, CommandResultPayload, Context,
    LineEnding, Server,
};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpListener;

fn once() -> ClientConfig {
    ClientConfig { retry_interval: Duration::from_millis(5), retry_forever: false }
}

fn channel(port: u16) -> ChannelConfig {
    serde_json::from_str(&format!(
        r#"{{ "host": "127.0.0.1", "port": {port}, "connect_timeout_ms": 300, "io_timeout_ms": 300 }}"#
    ))
    .unwrap()
}

async fn echo_server() -> Server {
    Server::start_isolated(|request, _| async move { Ok(format!("echo:{request}")) })
        .await
        .unwrap()
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

fn single_json(port: u16, id: &str, request: &str) -> String {
    format!(
        r#"{{ "id": "{id}", "request": "{request}",
             "protocol": {{ "type": "ascii-tcp", "host": "127.0.0.1", "port": {port}, "line_ending": "lf",
                            "connect_timeout_ms": 300, "io_timeout_ms": 300 }} }}"#
    )
}

#[test]
fn channel_config_applies_documented_defaults() {
    let cfg: ChannelConfig = serde_json::from_str(r#"{ "host": "h", "port": 1 }"#).unwrap();

    assert_eq!(cfg.read_command, None);
    assert_eq!(cfg.read_response, None);
    assert_eq!(cfg.write_command, None);
    assert!(cfg.write_expects_response);
    assert!(matches!(cfg.line_ending, LineEnding::Lf));
    assert_eq!(cfg.min_poll_interval_ms, 500);
    assert_eq!(cfg.connect_timeout_ms, 2_000);
    assert_eq!(cfg.io_timeout_ms, 2_000);
    assert_eq!(cfg.scale, 1.0);
    assert_eq!(cfg.offset, 0.0);
    assert_eq!(cfg.response_mode, AsciiResponseMode::Text);
}

#[test]
fn channel_config_reads_every_field() {
    let cfg: ChannelConfig = serde_json::from_str(
        r#"{ "host": "dev", "port": 4001, "read_command": "READ", "read_response": "V=%f",
             "write_command": "SET {value}", "write_expects_response": false, "line_ending": "crlf",
             "min_poll_interval_ms": 50, "connect_timeout_ms": 10, "io_timeout_ms": 20,
             "scale": 0.1, "offset": -2.5, "response_mode": "presence" }"#,
    )
    .unwrap();

    assert_eq!(cfg.read_command.as_deref(), Some("READ"));
    assert_eq!(cfg.read_response.as_deref(), Some("V=%f"));
    assert_eq!(cfg.write_command.as_deref(), Some("SET {value}"));
    assert!(!cfg.write_expects_response);
    assert!(matches!(cfg.line_ending, LineEnding::CrLf));
    assert_eq!(cfg.min_poll_interval_ms, 50);
    assert_eq!(cfg.scale, 0.1);
    assert_eq!(cfg.offset, -2.5);
    assert_eq!(cfg.response_mode, AsciiResponseMode::Presence);

    let round_trip: ChannelConfig = serde_json::from_str(&serde_json::to_string(&cfg).unwrap()).unwrap();
    assert_eq!(round_trip.host, "dev");
    assert_eq!(round_trip.response_mode, AsciiResponseMode::Presence);
}

#[test]
fn channel_config_requires_host_and_port() {
    assert!(serde_json::from_str::<ChannelConfig>(r#"{ "port": 1 }"#).is_err());
    assert!(serde_json::from_str::<ChannelConfig>(r#"{ "host": "h" }"#).is_err());
}

#[test]
fn response_modes_use_kebab_case_names() {
    let parse = |name: &str| serde_json::from_str::<AsciiResponseMode>(&format!("\"{name}\"")).unwrap();
    assert_eq!(parse("number"), AsciiResponseMode::Number);
    assert_eq!(parse("bool"), AsciiResponseMode::Bool);
    assert_eq!(parse("text"), AsciiResponseMode::Text);
    assert_eq!(parse("presence"), AsciiResponseMode::Presence);
    assert!(serde_json::from_str::<AsciiResponseMode>("\"Number\"").is_err());
    assert_eq!(AsciiResponseMode::default(), AsciiResponseMode::Text);
}

#[test]
fn presence_mode_serialises_and_is_not_the_default() {
    assert_eq!(serde_json::to_string(&AsciiResponseMode::Presence).unwrap(), "\"presence\"");
    assert_ne!(AsciiResponseMode::Presence, AsciiResponseMode::default());

    let cfg: ChannelConfig = serde_json::from_str(
        r#"{ "host": "h", "port": 1, "read_command": "", "read_response": "ALIVE", "response_mode": "presence" }"#,
    )
    .unwrap();
    assert_eq!(cfg.response_mode, AsciiResponseMode::Presence);
    assert_eq!(cfg.read_command.as_deref(), Some(""));
    assert_eq!(cfg.read_response.as_deref(), Some("ALIVE"));
}

/// The documented heartbeat setup: an empty `read_command` only listens, and `read_response`
/// is the literal that counts as a pulse. Other lines are discarded.
#[tokio::test]
async fn presence_channel_receives_pulses_and_ignores_other_lines() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        stream.write_all(b"banner\nnoise\n").await.unwrap();
        for _ in 0..50 {
            if stream.write_all(b"ALIVE\n").await.is_err() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    });
    let cfg: ChannelConfig = serde_json::from_str(&format!(
        r#"{{ "host": "127.0.0.1", "port": {port}, "read_command": "", "read_response": "ALIVE",
             "response_mode": "presence", "io_timeout_ms": 500 }}"#
    ))
    .unwrap();
    assert_eq!(cfg.response_mode, AsciiResponseMode::Presence);
    let context = Context::new(once());
    let command = cfg.command(cfg.read_command.clone().unwrap());
    let pulse_line = cfg.read_response.clone().unwrap();

    for _ in 0..3 {
        let pulse = context
            .execute_command_matching(&command, |line| line == pulse_line)
            .await
            .unwrap();
        assert_eq!(pulse.response, "ALIVE");
    }
}

#[tokio::test]
async fn presence_channel_reports_a_stalled_heartbeat_as_a_timeout() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        stream.write_all(b"ALIVE\n").await.unwrap();
        std::future::pending::<()>().await;
    });
    let cfg: ChannelConfig = serde_json::from_str(&format!(
        r#"{{ "host": "127.0.0.1", "port": {port}, "read_command": "", "read_response": "ALIVE",
             "response_mode": "presence", "io_timeout_ms": 150 }}"#
    ))
    .unwrap();
    let context = Context::new(once());
    let command = cfg.command("");

    assert!(context.execute_command_matching(&command, |l| l == "ALIVE").await.is_ok());

    // The heartbeat has stopped: the next wait ends in an I/O timeout.
    let error = context.execute_command_matching(&command, |l| l == "ALIVE").await.unwrap_err();
    assert!(matches!(error, AsciiTcpError::IoTimeout), "got {error:?}");
}

#[test]
fn channel_config_derives_protocol_command_and_connection() {
    let cfg: ChannelConfig = serde_json::from_str(
        r#"{ "host": "dev", "port": 4001, "line_ending": "cr", "connect_timeout_ms": 11, "io_timeout_ms": 22 }"#,
    )
    .unwrap();

    let protocol = cfg.protocol();
    assert_eq!(protocol.kind, "ascii-tcp");
    assert_eq!((protocol.host.as_str(), protocol.port), ("dev", 4001));
    assert_eq!(protocol.connect_timeout_ms, Some(11));
    assert_eq!(protocol.io_timeout_ms, Some(22));
    assert!(matches!(protocol.line_ending, LineEnding::Cr));

    let command = cfg.command("READ");
    assert_eq!(command.id, None);
    assert_eq!(command.request, "READ");
    assert_eq!(command.protocol.port, 4001);

    let connection = cfg.connection();
    assert_eq!(connection.endpoint(), "dev:4001");
    assert_eq!(connection.connect_timeout, Duration::from_millis(11));
    assert_eq!(connection.io_timeout, Duration::from_millis(22));
    assert!(matches!(connection.line_ending, LineEnding::Cr));
}

#[test]
fn client_config_default_retries_forever_every_two_seconds() {
    let cfg = ClientConfig::default();
    assert_eq!(cfg.retry_interval, Duration::from_secs(2));
    assert!(cfg.retry_forever);
}

#[test]
fn context_exposes_the_config_it_was_built_with() {
    let context = Context::new(ClientConfig { retry_interval: Duration::from_millis(7), retry_forever: false });
    assert_eq!(context.config().retry_interval, Duration::from_millis(7));
    assert!(!context.config().retry_forever);

    assert!(Context::default().config().retry_forever);
}

#[tokio::test]
async fn execute_json_runs_a_single_command() {
    let server = echo_server().await;
    let context = Context::new(once());

    let result = context
        .execute_json(&single_json(server.tcp_port(), "q1", "hello"))
        .await
        .unwrap();

    let CommandResultPayload::Single(result) = result else { panic!("expected a single result") };
    assert_eq!(result.id.as_deref(), Some("q1"));
    assert_eq!(result.response, "echo:hello");
    server.stop_drop().await.unwrap();
}

#[tokio::test]
async fn execute_json_runs_a_batch_in_order() {
    let server = echo_server().await;
    let context = Context::new(once());
    let port = server.tcp_port();
    let json = format!(
        r#"{{ "commands": [{}, {}, {}] }}"#,
        single_json(port, "a", "1"),
        single_json(port, "b", "2"),
        single_json(port, "c", "3")
    );

    let result = context.execute_json(&json).await.unwrap();

    let CommandResultPayload::Batch(batch) = result else { panic!("expected a batch result") };
    let seen: Vec<_> = batch
        .responses
        .iter()
        .map(|r| (r.id.clone().unwrap(), r.response.clone()))
        .collect();
    assert_eq!(
        seen,
        [("a".into(), "echo:1".into()), ("b".into(), "echo:2".into()), ("c".into(), "echo:3".into())]
    );
    server.stop_drop().await.unwrap();
}

#[tokio::test]
async fn execute_json_rejects_malformed_json_without_retrying() {
    let context = Context::default();
    let error = context.execute_json("not json").await.unwrap_err();
    assert!(matches!(error, AsciiTcpError::ConfigParse(_)));
}

#[tokio::test]
async fn a_failing_batch_command_aborts_the_batch() {
    let server = echo_server().await;
    let context = Context::new(once());
    let port = server.tcp_port();
    let json = format!(
        r#"{{ "commands": [{}, {}, {}] }}"#,
        single_json(port, "a", "1"),
        single_json(closed_port(), "b", "2"),
        single_json(port, "c", "3")
    );

    let error = context.execute_json(&json).await.unwrap_err();

    assert!(matches!(error, AsciiTcpError::Io(_) | AsciiTcpError::ConnectTimeout), "got {error:?}");
    server.stop_drop().await.unwrap();
}

#[tokio::test]
async fn execute_payload_does_not_retry_when_retry_forever_is_off() {
    let context = Context::new(once());
    let json = single_json(closed_port(), "x", "READ");
    let payload = ascii_tcp::parse_backend_payload(&json).unwrap();

    let started = std::time::Instant::now();
    assert!(context.execute_payload(&payload).await.is_err());
    assert!(started.elapsed() < Duration::from_secs(2));
}

#[tokio::test]
async fn execute_payload_stops_retrying_on_a_non_retryable_error() {
    let context = Context::new(ClientConfig { retry_interval: Duration::from_millis(1), retry_forever: true });
    let json = single_json(1, "x", "READ").replace("ascii-tcp", "modbus");
    let payload = ascii_tcp::parse_backend_payload(&json).unwrap();

    let error = tokio::time::timeout(Duration::from_secs(2), context.execute_payload(&payload))
        .await
        .expect("a non-retryable error must not be retried")
        .unwrap_err();

    assert!(matches!(error, AsciiTcpError::InvalidProtocolType(kind) if kind == "modbus"));
}

#[tokio::test]
async fn execute_payload_once_reports_the_first_error() {
    let context = Context::new(ClientConfig { retry_interval: Duration::from_millis(1), retry_forever: true });
    let payload = ascii_tcp::parse_backend_payload(&single_json(closed_port(), "x", "READ")).unwrap();

    assert!(context.execute_payload_once(&payload).await.is_err());
}

#[tokio::test]
async fn connection_callbacks_report_each_transition() {
    let server = echo_server().await;
    let events = Arc::new(Mutex::new(Vec::<String>::new()));
    let context = Context::new(once());
    let log = events.clone();
    context.on_connected(move |endpoint| log.lock().unwrap().push(format!("up {endpoint}")));
    let log = events.clone();
    context.on_disconnected(move |endpoint, reason| log.lock().unwrap().push(format!("down {endpoint}: {reason}")));

    let port = server.tcp_port();
    let dead_port = closed_port();
    context.execute_json(&single_json(port, "a", "1")).await.unwrap();
    context.execute_json(&single_json(port, "a", "2")).await.unwrap();
    assert!(context.execute_json(&single_json(dead_port, "b", "1")).await.is_err());

    {
        let events = events.lock().unwrap();
        assert_eq!(events.len(), 2, "{events:?}");
        assert_eq!(events[0], format!("up 127.0.0.1:{port}"));
        assert!(events[1].starts_with(&format!("down 127.0.0.1:{dead_port}: ")), "{}", events[1]);
    }
    server.stop_drop().await.unwrap();
}

#[tokio::test]
async fn every_cloned_context_shares_callbacks_and_connections() {
    let server = echo_server().await;
    let connected = Arc::new(AtomicUsize::new(0));
    let context = Context::new(once());
    let counter = connected.clone();
    context.on_connected(move |_| {
        counter.fetch_add(1, Ordering::SeqCst);
    });
    let clone = context.clone();

    context.execute_json(&single_json(server.tcp_port(), "a", "1")).await.unwrap();
    clone.execute_json(&single_json(server.tcp_port(), "a", "2")).await.unwrap();

    assert_eq!(connected.load(Ordering::SeqCst), 1);
    server.stop_drop().await.unwrap();
}

#[tokio::test]
async fn an_unanswered_command_times_out_without_reporting_a_disconnect() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut lines = BufReader::new(stream).lines();
        while let Ok(Some(_)) = lines.next_line().await {}
    });
    let disconnects = Arc::new(AtomicUsize::new(0));
    let context = Context::new(once());
    let counter = disconnects.clone();
    context.on_disconnected(move |_, _| {
        counter.fetch_add(1, Ordering::SeqCst);
    });

    let error = context.execute_json(&single_json(port, "x", "IGNORED")).await.unwrap_err();

    assert!(matches!(error, AsciiTcpError::IoTimeout), "got {error:?}");
    assert_eq!(disconnects.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn execute_command_matching_picks_the_wanted_reply() {
    let server = Server::start_isolated(|request, _| async move { Ok(request) }).await.unwrap();
    let context = Context::new(once());
    let cfg = channel(server.tcp_port());

    let result = context
        .execute_command_matching(&cfg.command("VALUE=3"), |line| line.starts_with("VALUE="))
        .await
        .unwrap();

    assert_eq!(result.response, "VALUE=3");
    assert_eq!(result.id, None);

    let error = context
        .execute_command_matching(&cfg.command("VALUE=3"), |line| line == "NOPE")
        .await
        .unwrap_err();
    assert!(matches!(error, AsciiTcpError::IoTimeout));
    server.stop_drop().await.unwrap();
}

#[tokio::test]
async fn send_command_delivers_without_waiting_for_a_reply() {
    let (tx, rx) = tokio::sync::oneshot::channel();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut lines = BufReader::new(stream).lines();
        let _ = tx.send(lines.next_line().await.unwrap());
    });
    let context = Context::new(once());
    let connected = Arc::new(AtomicUsize::new(0));
    let counter = connected.clone();
    context.on_connected(move |_| {
        counter.fetch_add(1, Ordering::SeqCst);
    });

    context.send_command(&channel(port).command("FIRE")).await.unwrap();

    assert_eq!(rx.await.unwrap().as_deref(), Some("FIRE"));
    assert_eq!(connected.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn send_command_reports_connection_failures_as_disconnects() {
    let context = Context::new(once());
    let disconnects = Arc::new(AtomicUsize::new(0));
    let counter = disconnects.clone();
    context.on_disconnected(move |_, _| {
        counter.fetch_add(1, Ordering::SeqCst);
    });

    let result = context.send_command(&channel(closed_port()).command("FIRE")).await;

    assert!(result.is_err());
    assert_eq!(disconnects.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn send_command_rejects_a_foreign_protocol() {
    let context = Context::new(once());
    let mut command = channel(1).command("FIRE");
    command.protocol.kind = "modbus".to_string();

    let error = context.send_command(&command).await.unwrap_err();
    assert!(matches!(error, AsciiTcpError::InvalidProtocolType(_)));
}
