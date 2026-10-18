use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use ascii_tcp::{ChannelConfig, Context};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpListener;

/// Mimics a banner on every connection, `TEMP?` replies, echoed writes,
/// `DROP` closes the connection, and (optionally) a periodic unsolicited `ALIVE`.
/// Returns the port and a counter of accepted connections.
async fn spawn_device(heartbeat: bool) -> (u16, Arc<AtomicUsize>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let accepted = Arc::new(AtomicUsize::new(0));
    let counter = accepted.clone();
    tokio::spawn(async move {
        loop {
            let (stream, _) = listener.accept().await.unwrap();
            counter.fetch_add(1, Ordering::SeqCst);
            tokio::spawn(async move {
                let (reader, mut writer) = stream.into_split();
                writer.write_all(b"TCP client connected from 127.0.0.1\r\n").await.unwrap();
                let mut lines = BufReader::new(reader).lines();
                let mut beat = tokio::time::interval(Duration::from_millis(50));
                loop {
                    tokio::select! {
                        line = lines.next_line() => {
                            let Ok(Some(line)) = line else { return };
                            if line == "DROP" {
                                return;
                            }
                            if line == "IGNORE?" {
                                continue;
                            }
                            let reply = if line == "TEMP?" { "TEMP=25.2".to_string() } else { line };
                            if writer.write_all(format!("{reply}\r\n").as_bytes()).await.is_err() {
                                return;
                            }
                        }
                        _ = beat.tick(), if heartbeat => {
                            if writer.write_all(b"ALIVE\r\n").await.is_err() {
                                return;
                            }
                        }
                    }
                }
            });
        }
    });
    (port, accepted)
}

fn channel(port: u16) -> ChannelConfig {
    serde_json::from_str(&format!(
        r#"{{ "host": "127.0.0.1", "port": {port}, "line_ending": "crlf", "io_timeout_ms": 500 }}"#
    ))
    .unwrap()
}

fn wants_temp(line: &str) -> bool {
    line.starts_with("TEMP=")
}

#[tokio::test]
async fn skips_unmatched_banner_line() {
    let (port, _) = spawn_device(false).await;
    let cfg = channel(port);
    let ctx = Context::default();

    let reply = ctx.execute_command_matching(&cfg.command("TEMP?"), wants_temp).await.unwrap();
    assert_eq!(reply.response, "TEMP=25.2");
}

#[tokio::test]
async fn times_out_when_nothing_matches() {
    let (port, _) = spawn_device(false).await;
    let cfg = channel(port);
    let ctx = Context::default();

    let result = ctx.execute_command_matching(&cfg.command("TEMP?"), |l| l == "NEVER").await;
    assert!(result.is_err());
}

#[tokio::test]
async fn reuses_one_connection_for_all_requests() {
    let (port, accepted) = spawn_device(false).await;
    let cfg = channel(port);
    let ctx = Context::default();

    for _ in 0..5 {
        let reply = ctx.execute_command_matching(&cfg.command("TEMP?"), wants_temp).await.unwrap();
        assert_eq!(reply.response, "TEMP=25.2");
    }
    ctx.send_command(&cfg.command("NOREPLY")).await.unwrap();

    // Concurrent callers on cloned contexts share the same connection too.
    let ctx2 = ctx.clone();
    let cmd = cfg.command("TEMP?");
    let (a, b) = tokio::join!(
        ctx.execute_command_matching(&cmd, wants_temp),
        ctx2.execute_command_matching(&cmd, wants_temp),
    );
    assert!(a.is_ok() && b.is_ok());

    assert_eq!(accepted.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn empty_request_listens_without_sending() {
    let (port, accepted) = spawn_device(true).await;
    let cfg = channel(port);
    let ctx = Context::default();

    for _ in 0..3 {
        let beat = ctx.execute_command_matching(&cfg.command(""), |l| l == "ALIVE").await.unwrap();
        assert_eq!(beat.response, "ALIVE");
    }
    // An echoed empty line would have come back as "", and the device would only reply to real requests.
    assert_eq!(accepted.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn unanswered_request_does_not_block_others_or_drop_the_connection() {
    let (port, accepted) = spawn_device(true).await;
    let cfg = channel(port);
    let ctx = Context::default();

    ctx.execute_command_matching(&cfg.command("TEMP?"), wants_temp).await.unwrap();

    let stalled = {
        let ctx = ctx.clone();
        let cmd = cfg.command("IGNORE?");
        tokio::spawn(async move {
            ctx.execute_command_matching(&cmd, |l| l.starts_with("IGNORE=")).await
        })
    };
    tokio::time::sleep(Duration::from_millis(50)).await;

    // io_timeout is 500 ms; other requests must complete well inside that while the first still waits.
    let started = std::time::Instant::now();
    for _ in 0..3 {
        let reply = ctx.execute_command_matching(&cfg.command("TEMP?"), wants_temp).await.unwrap();
        assert_eq!(reply.response, "TEMP=25.2");
    }
    assert!(started.elapsed() < Duration::from_millis(300), "blocked for {:?}", started.elapsed());

    assert!(stalled.await.unwrap().is_err());

    // The device kept talking (ALIVE), so the unanswered command must not cost the connection.
    ctx.execute_command_matching(&cfg.command("TEMP?"), wants_temp).await.unwrap();
    assert_eq!(accepted.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn silent_link_is_dropped_after_a_timeout() {
    let (port, accepted) = spawn_device(false).await;
    let cfg = channel(port);
    let ctx = Context::default();

    ctx.execute_command_matching(&cfg.command("TEMP?"), wants_temp).await.unwrap();
    let result = ctx.execute_command_matching(&cfg.command("IGNORE?"), |l| l == "NEVER").await;
    assert!(result.is_err());

    let reply = ctx.execute_command_matching(&cfg.command("TEMP?"), wants_temp).await.unwrap();
    assert_eq!(reply.response, "TEMP=25.2");
    assert_eq!(accepted.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn reconnects_after_the_connection_drops() {
    let (port, accepted) = spawn_device(false).await;
    let cfg = channel(port);
    let ctx = Context::default();

    ctx.execute_command_matching(&cfg.command("TEMP?"), wants_temp).await.unwrap();
    ctx.send_command(&cfg.command("DROP")).await.unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;

    let reply = ctx.execute_command_matching(&cfg.command("TEMP?"), wants_temp).await.unwrap();
    assert_eq!(reply.response, "TEMP=25.2");
    assert_eq!(accepted.load(Ordering::SeqCst), 2);
}
