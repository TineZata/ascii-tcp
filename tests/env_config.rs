//! Environment-variable driven configuration. Kept in its own test binary with a single
//! test so that mutating the process environment cannot race with other tests.

use std::time::Duration;

use ascii_tcp::{AsciiTcpError, ClientConfig, Context, LineEnding, Server, ServerContext};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;

const VARS: [&str; 5] = [
    "ASCII_TCP_RETRY_MS",
    "ASCII_TCP_RETRY_FOREVER",
    "ASCII_TCP_SERVER_ADDR",
    "ASCII_TCP_SERVER_IO_TIMEOUT_MS",
    "ASCII_TCP_SERVER_MAX_LINE_LENGTH",
];

fn clear() {
    for var in VARS {
        std::env::remove_var(var);
    }
}

fn client_config_from_env() {
    clear();
    let cfg = ClientConfig::from_env();
    assert_eq!(cfg.retry_interval, Duration::from_millis(2_000));
    assert!(cfg.retry_forever);

    std::env::set_var("ASCII_TCP_RETRY_MS", "150");
    std::env::set_var("ASCII_TCP_RETRY_FOREVER", "false");
    let cfg = ClientConfig::from_env();
    assert_eq!(cfg.retry_interval, Duration::from_millis(150));
    assert!(!cfg.retry_forever);
    assert_eq!(Context::from_env().config().retry_interval, Duration::from_millis(150));

    for off in ["FALSE", "no", "No"] {
        std::env::set_var("ASCII_TCP_RETRY_FOREVER", off);
        assert!(!ClientConfig::from_env().retry_forever, "{off}");
    }
    for on in ["true", "1", "yes", ""] {
        std::env::set_var("ASCII_TCP_RETRY_FOREVER", on);
        assert!(ClientConfig::from_env().retry_forever, "{on:?}");
    }

    std::env::set_var("ASCII_TCP_RETRY_MS", "soon");
    assert_eq!(ClientConfig::from_env().retry_interval, Duration::from_millis(2_000));
    clear();
}

fn server_context_from_env() {
    clear();
    let ctx = ServerContext::from_env().unwrap();
    assert_eq!(ctx.bind_addr.to_string(), "127.0.0.1:4000");
    assert_eq!(ctx.io_timeout, Duration::from_secs(2));
    assert_eq!(ctx.max_line_length, 8 * 1024);
    assert!(matches!(ctx.line_ending, LineEnding::Lf));

    std::env::set_var("ASCII_TCP_SERVER_ADDR", "127.0.0.1:5123");
    std::env::set_var("ASCII_TCP_SERVER_IO_TIMEOUT_MS", "750");
    std::env::set_var("ASCII_TCP_SERVER_MAX_LINE_LENGTH", "64");
    let ctx = ServerContext::from_env().unwrap();
    assert_eq!(ctx.bind_addr.to_string(), "127.0.0.1:5123");
    assert_eq!(ctx.io_timeout, Duration::from_millis(750));
    assert_eq!(ctx.max_line_length, 64);

    // Unparsable numbers are ignored, keeping the defaults.
    std::env::set_var("ASCII_TCP_SERVER_IO_TIMEOUT_MS", "slow");
    std::env::set_var("ASCII_TCP_SERVER_MAX_LINE_LENGTH", "-1");
    let ctx = ServerContext::from_env().unwrap();
    assert_eq!(ctx.io_timeout, Duration::from_secs(2));
    assert_eq!(ctx.max_line_length, 8 * 1024);

    // An unparsable address is an error.
    std::env::set_var("ASCII_TCP_SERVER_ADDR", "not an address");
    match ServerContext::from_env().unwrap_err() {
        AsciiTcpError::Io(error) => {
            assert_eq!(error.kind(), std::io::ErrorKind::InvalidInput);
            assert!(error.to_string().contains("ASCII_TCP_SERVER_ADDR"));
        }
        other => panic!("expected an I/O error, got {other:?}"),
    }
    clear();
}

async fn start_from_env() {
    clear();
    std::env::set_var("ASCII_TCP_SERVER_ADDR", "127.0.0.1:0");
    let server = Server::start_from_env(|request, _| async move { Ok(format!("env:{request}")) })
        .await
        .unwrap();

    let mut stream = TcpStream::connect(server.local_addr()).await.unwrap();
    stream.write_all(b"hi\n").await.unwrap();
    let mut line = String::new();
    BufReader::new(&mut stream).read_line(&mut line).await.unwrap();
    assert_eq!(line, "env:hi\n");
    server.stop_drop().await.unwrap();

    std::env::set_var("ASCII_TCP_SERVER_ADDR", "bogus");
    let result = Server::start_from_env(|request, _| async move { Ok(request) }).await;
    assert!(matches!(result, Err(AsciiTcpError::Io(_))));
    clear();
}

#[tokio::test]
async fn environment_configuration() {
    client_config_from_env();
    server_context_from_env();
    start_from_env().await;
}
