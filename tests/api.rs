//! Public data types, JSON parsing and the one-shot `send_line` / `exchange_line*` helpers.

use std::time::Duration;

use ascii_tcp::{
    exchange_line, exchange_line_matching, parse_backend_batch, parse_backend_command,
    parse_backend_payload, send_line, AsciiTcpConfig, AsciiTcpError, BatchCommands, BatchResult,
    Command, CommandResult, CommandResultPayload, HardwareProtocol, LineEnding, Payload,
};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpListener;

const COMMAND_JSON: &str = r#"{
    "id": "t1",
    "request": "READ TEMP",
    "protocol": { "type": "ascii-tcp", "host": "10.0.0.5", "port": 5000, "line_ending": "crlf",
                  "connect_timeout_ms": 100, "io_timeout_ms": 200 }
}"#;

fn hardware_protocol(kind: &str) -> HardwareProtocol {
    HardwareProtocol {
        kind: kind.to_string(),
        host: "localhost".to_string(),
        port: 4001,
        line_ending: LineEnding::Cr,
        connect_timeout_ms: None,
        io_timeout_ms: None,
    }
}

/// Binds a listener, runs `script` against the first connection and returns the port.
async fn one_shot_device<F, Fut>(script: F) -> u16
where
    F: FnOnce(tokio::net::TcpStream) -> Fut + Send + 'static,
    Fut: std::future::Future<Output = ()> + Send + 'static,
{
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        script(stream).await;
    });
    port
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
fn config_for(port: u16) -> AsciiTcpConfig {
    AsciiTcpConfig {
        port,
        connect_timeout: Duration::from_millis(500),
        io_timeout: Duration::from_millis(300),
        ..AsciiTcpConfig::default()
    }
}

#[test]
fn config_defaults_describe_a_local_lf_endpoint() {
    let config = AsciiTcpConfig::default();
    assert_eq!(config.host, "127.0.0.1");
    assert_eq!(config.port, 4000);
    assert_eq!(config.connect_timeout, Duration::from_secs(2));
    assert_eq!(config.io_timeout, Duration::from_secs(2));
    assert!(matches!(config.line_ending, LineEnding::Lf));
    assert_eq!(config.endpoint(), "127.0.0.1:4000");
}

#[test]
fn config_is_built_from_an_ascii_tcp_protocol_block() {
    let mut protocol = hardware_protocol("ascii-tcp");
    protocol.connect_timeout_ms = Some(150);
    protocol.io_timeout_ms = Some(250);

    let config = AsciiTcpConfig::from_protocol(&protocol).unwrap();

    assert_eq!(config.endpoint(), "localhost:4001");
    assert_eq!(config.connect_timeout, Duration::from_millis(150));
    assert_eq!(config.io_timeout, Duration::from_millis(250));
    assert!(matches!(config.line_ending, LineEnding::Cr));
}

#[test]
fn config_falls_back_to_two_second_timeouts() {
    let config = AsciiTcpConfig::from_protocol(&hardware_protocol("ascii-tcp")).unwrap();
    assert_eq!(config.connect_timeout, Duration::from_millis(2_000));
    assert_eq!(config.io_timeout, Duration::from_millis(2_000));
}

#[test]
fn config_rejects_other_protocol_types() {
    let error = AsciiTcpConfig::from_protocol(&hardware_protocol("modbus")).unwrap_err();
    assert!(matches!(error, AsciiTcpError::InvalidProtocolType(kind) if kind == "modbus"));
}

#[test]
fn line_ending_deserialises_from_lowercase_names_and_defaults_to_lf() {
    let parse = |json: &str| serde_json::from_str::<LineEnding>(json).unwrap();
    assert!(matches!(parse(r#""lf""#), LineEnding::Lf));
    assert!(matches!(parse(r#""crlf""#), LineEnding::CrLf));
    assert!(matches!(parse(r#""cr""#), LineEnding::Cr));
    assert!(serde_json::from_str::<LineEnding>(r#""LF""#).is_err());
    assert!(matches!(LineEnding::default(), LineEnding::Lf));
    assert_eq!(serde_json::to_string(&LineEnding::CrLf).unwrap(), r#""crlf""#);
}

#[test]
fn only_transient_errors_are_retryable() {
    let io = || AsciiTcpError::Io(std::io::Error::from(std::io::ErrorKind::ConnectionRefused));
    for error in [AsciiTcpError::ConnectTimeout, AsciiTcpError::IoTimeout, AsciiTcpError::Eof, io()] {
        assert!(error.is_retryable(), "{error} should be retryable");
    }

    let parse_error = serde_json::from_str::<Payload>("{").unwrap_err();
    for error in [
        AsciiTcpError::InvalidProtocolType("x".into()),
        AsciiTcpError::ConfigParse(parse_error),
        AsciiTcpError::RequestTooLong(8),
        AsciiTcpError::ServerStopped,
        AsciiTcpError::TemplateSyntax("x".into()),
        AsciiTcpError::ResponseMismatch { template: "t".into(), response: "r".into() },
    ] {
        assert!(!error.is_retryable(), "{error} should not be retryable");
    }
}

#[test]
fn errors_render_human_readable_messages() {
    assert_eq!(AsciiTcpError::ConnectTimeout.to_string(), "connect timeout");
    assert_eq!(AsciiTcpError::IoTimeout.to_string(), "I/O timeout");
    assert_eq!(AsciiTcpError::Eof.to_string(), "connection closed before response");
    assert_eq!(AsciiTcpError::ServerStopped.to_string(), "server task stopped");
    assert_eq!(
        AsciiTcpError::InvalidProtocolType("x".into()).to_string(),
        "invalid protocol type: x"
    );
    assert_eq!(
        AsciiTcpError::RequestTooLong(8).to_string(),
        "request exceeds maximum line length of 8 bytes"
    );
    assert_eq!(
        AsciiTcpError::TemplateSyntax("bad".into()).to_string(),
        "invalid response template: bad"
    );
    assert_eq!(
        AsciiTcpError::ResponseMismatch { template: "A=%d".into(), response: "B".into() }.to_string(),
        "response 'B' does not match template 'A=%d'"
    );
    let io = AsciiTcpError::from(std::io::Error::other("boom"));
    assert_eq!(io.to_string(), "I/O error: boom");
}

#[test]
fn parses_a_single_command() {
    let command = parse_backend_command(COMMAND_JSON).unwrap();
    assert_eq!(command.id.as_deref(), Some("t1"));
    assert_eq!(command.request, "READ TEMP");
    assert_eq!(command.protocol.host, "10.0.0.5");
    assert_eq!(command.protocol.port, 5000);
    assert_eq!(command.protocol.connect_timeout_ms, Some(100));
    assert!(matches!(command.protocol.line_ending, LineEnding::CrLf));
}

#[test]
fn optional_command_fields_may_be_absent() {
    let command = parse_backend_command(
        r#"{ "request": "X", "protocol": { "type": "ascii-tcp", "host": "h", "port": 1, "line_ending": "lf" } }"#,
    )
    .unwrap();
    assert_eq!(command.id, None);
    assert_eq!(command.protocol.connect_timeout_ms, None);
    assert_eq!(command.protocol.io_timeout_ms, None);
}

#[test]
fn parses_a_batch() {
    let json = format!(r#"{{ "commands": [{COMMAND_JSON}, {COMMAND_JSON}] }}"#);
    let batch = parse_backend_batch(&json).unwrap();
    assert_eq!(batch.commands.len(), 2);
}

#[test]
fn payload_parsing_distinguishes_single_commands_from_batches() {
    assert!(matches!(parse_backend_payload(COMMAND_JSON).unwrap(), Payload::Single(_)));

    let json = format!(r#"{{ "commands": [{COMMAND_JSON}] }}"#);
    assert!(matches!(parse_backend_payload(&json).unwrap(), Payload::Batch(batch) if batch.commands.len() == 1));
}

#[test]
fn malformed_json_is_reported_as_a_config_parse_error() {
    assert!(matches!(parse_backend_command("{").unwrap_err(), AsciiTcpError::ConfigParse(_)));
    assert!(matches!(parse_backend_batch("{}").unwrap_err(), AsciiTcpError::ConfigParse(_)));
    assert!(matches!(parse_backend_payload(r#"{"nope":1}"#).unwrap_err(), AsciiTcpError::ConfigParse(_)));
}

#[test]
fn results_and_payloads_round_trip_through_json() {
    let single = CommandResultPayload::Single(CommandResult { id: Some("a".into()), response: "1".into() });
    let json = serde_json::to_string(&single).unwrap();
    assert_eq!(json, r#"{"id":"a","response":"1"}"#);
    assert!(matches!(serde_json::from_str::<CommandResultPayload>(&json).unwrap(), CommandResultPayload::Single(_)));

    let batch = CommandResultPayload::Batch(BatchResult {
        responses: vec![CommandResult { id: None, response: "2".into() }],
    });
    let json = serde_json::to_string(&batch).unwrap();
    assert_eq!(json, r#"{"responses":[{"id":null,"response":"2"}]}"#);
    assert!(matches!(serde_json::from_str::<CommandResultPayload>(&json).unwrap(), CommandResultPayload::Batch(_)));

    let command = Command { id: None, request: "R".into(), protocol: hardware_protocol("ascii-tcp") };
    let payload = Payload::Batch(BatchCommands { commands: vec![command] });
    let json = serde_json::to_string(&payload).unwrap();
    assert!(matches!(serde_json::from_str::<Payload>(&json).unwrap(), Payload::Batch(_)));
}

#[tokio::test]
async fn exchange_line_returns_the_reply_without_its_terminator() {
    let port = one_shot_device(|stream| async move {
        let (reader, mut writer) = stream.into_split();
        let mut lines = BufReader::new(reader).lines();
        let request = lines.next_line().await.unwrap().unwrap();
        writer.write_all(format!("got {request}\n").as_bytes()).await.unwrap();
    })
    .await;

    assert_eq!(exchange_line(&config_for(port), "PING").await.unwrap(), "got PING");
}

#[tokio::test]
async fn exchange_line_matching_skips_unwanted_lines() {
    let port = one_shot_device(|mut stream| async move {
        stream.write_all(b"banner\r\nALIVE\r\nVALUE=7\r\n").await.unwrap();
        let mut sink = [0u8; 16];
        let _ = stream.read(&mut sink).await;
    })
    .await;
    let config = AsciiTcpConfig { line_ending: LineEnding::CrLf, ..config_for(port) };

    let reply = exchange_line_matching(&config, "GET", |line| line.starts_with("VALUE=")).await;
    assert_eq!(reply.unwrap(), "VALUE=7");
}

#[tokio::test]
async fn exchange_line_times_out_when_no_line_is_accepted() {
    let port = one_shot_device(|mut stream| async move {
        stream.write_all(b"noise\n").await.unwrap();
        std::future::pending::<()>().await;
    })
    .await;

    let error = exchange_line_matching(&config_for(port), "GET", |_| false).await.unwrap_err();
    assert!(matches!(error, AsciiTcpError::IoTimeout), "got {error:?}");
}

#[tokio::test]
async fn exchange_line_reports_eof_when_the_peer_hangs_up_without_replying() {
    let port = one_shot_device(|stream| async move { drop(stream) }).await;

    let error = exchange_line(&config_for(port), "GET").await.unwrap_err();
    assert!(matches!(error, AsciiTcpError::Eof | AsciiTcpError::Io(_)), "got {error:?}");
}

#[tokio::test]
async fn exchange_line_reports_a_refused_connection() {
    let port = closed_port();

    let error = exchange_line(&config_for(port), "GET").await.unwrap_err();
    assert!(matches!(error, AsciiTcpError::Io(_) | AsciiTcpError::ConnectTimeout), "got {error:?}");
}

#[tokio::test]
async fn send_line_delivers_the_request_with_its_line_ending() {
    let (tx, rx) = tokio::sync::oneshot::channel();
    let port = one_shot_device(|stream| async move {
        let mut lines = BufReader::new(stream).lines();
        let _ = tx.send(lines.next_line().await.unwrap());
    })
    .await;

    send_line(&config_for(port), "FIRE").await.unwrap();

    assert_eq!(rx.await.unwrap().as_deref(), Some("FIRE"));
}

#[tokio::test]
async fn send_line_reports_a_refused_connection() {
    let port = closed_port();

    let error = send_line(&config_for(port), "FIRE").await.unwrap_err();
    assert!(matches!(error, AsciiTcpError::Io(_) | AsciiTcpError::ConnectTimeout), "got {error:?}");
}
