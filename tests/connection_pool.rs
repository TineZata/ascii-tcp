use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use ascii_tcp::{AsciiTcpConfig, ConnectionPool, LineEnding};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpListener;

/// Serves `ARM?` -> `ARM=1` and `VEL?` -> `VEL,...`, counting how many clients connect.
async fn spawn_server() -> (u16, Arc<AtomicUsize>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let accepts = Arc::new(AtomicUsize::new(0));
    let accepts_for_task = accepts.clone();

    tokio::spawn(async move {
        loop {
            let Ok((stream, _)) = listener.accept().await else {
                break;
            };
            accepts_for_task.fetch_add(1, Ordering::SeqCst);
            tokio::spawn(async move {
                let (reader, mut writer) = tokio::io::split(stream);
                let mut lines = BufReader::new(reader).lines();
                while let Ok(Some(line)) = lines.next_line().await {
                    let response = match line.trim() {
                        "ARM?" => "ARM=1\r\n".to_string(),
                        "VEL?" => "VEL,GPIO6=1,GPIO2=0,DELTA=10000(ticks at 8ns)\r\n".to_string(),
                        other => format!("ERR,{other}\r\n"),
                    };
                    if writer.write_all(response.as_bytes()).await.is_err() {
                        break;
                    }
                }
            });
        }
    });

    (port, accepts)
}

fn config(port: u16) -> AsciiTcpConfig {
    AsciiTcpConfig {
        host: "127.0.0.1".to_string(),
        port,
        connect_timeout: Duration::from_secs(2),
        io_timeout: Duration::from_secs(2),
        line_ending: LineEnding::CrLf,
    }
}

#[tokio::test]
async fn failed_connection_enters_a_shared_reconnect_cooldown() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    drop(listener);

    let pool = ConnectionPool::new();
    let cfg = config(port);

    assert!(pool.exchange_line(&cfg, "ARM?").await.is_err());

    let error = pool.exchange_line(&cfg, "ARM?").await.unwrap_err();
    match error {
        ascii_tcp::AsciiTcpError::Io(error) => {
            assert_eq!(error.kind(), std::io::ErrorKind::ConnectionAborted);
        }
        other => panic!("expected cooldown error, got {other}"),
    }
}

#[tokio::test]
async fn reuses_a_single_connection_across_many_exchanges() {
    let (port, accepts) = spawn_server().await;
    let pool = ConnectionPool::new();
    let cfg = config(port);

    for _ in 0..10 {
        assert_eq!(pool.exchange_line(&cfg, "ARM?").await.unwrap(), "ARM=1");
    }

    assert_eq!(accepts.load(Ordering::SeqCst), 1);
    assert!(pool.is_connected(&cfg).await);
}

#[tokio::test]
async fn concurrent_callers_share_one_connection_and_keep_responses_paired() {
    let (port, accepts) = spawn_server().await;
    let pool = Arc::new(ConnectionPool::new());
    let cfg = config(port);

    // Two widgets polling the same endpoint at once, as the app does.
    let arm = {
        let pool = pool.clone();
        let cfg = cfg.clone();
        tokio::spawn(async move {
            for _ in 0..25 {
                assert_eq!(pool.exchange_line(&cfg, "ARM?").await.unwrap(), "ARM=1");
            }
        })
    };
    let vel = {
        let pool = pool.clone();
        let cfg = cfg.clone();
        tokio::spawn(async move {
            for _ in 0..25 {
                let response = pool.exchange_line(&cfg, "VEL?").await.unwrap();
                assert!(response.starts_with("VEL,"), "got '{response}'");
            }
        })
    };

    arm.await.unwrap();
    vel.await.unwrap();

    assert_eq!(accepts.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn reconnects_after_the_peer_closes_an_idle_connection() {
    let (port, accepts) = spawn_server().await;
    let pool = ConnectionPool::new();
    let cfg = config(port);

    assert_eq!(pool.exchange_line(&cfg, "ARM?").await.unwrap(), "ARM=1");
    // Simulate the far end dropping the socket while idle.
    pool.disconnect(&cfg).await;
    assert!(!pool.is_connected(&cfg).await);

    assert_eq!(pool.exchange_line(&cfg, "ARM?").await.unwrap(), "ARM=1");
    assert_eq!(accepts.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn send_line_shares_the_pooled_connection() {
    let (port, accepts) = spawn_server().await;
    let pool = ConnectionPool::new();
    let cfg = config(port);

    assert_eq!(pool.exchange_line(&cfg, "ARM?").await.unwrap(), "ARM=1");
    pool.send_line(&cfg, "DISARM").await.unwrap();

    assert_eq!(accepts.load(Ordering::SeqCst), 1);
}

/// Serves `ARM?` but pushes an unsolicited event frame before the first reply.
async fn spawn_server_with_unsolicited_event() -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();

    tokio::spawn(async move {
        let Ok((stream, _)) = listener.accept().await else {
            return;
        };
        let (reader, mut writer) = tokio::io::split(stream);
        let mut lines = BufReader::new(reader).lines();
        let mut fired = false;
        while let Ok(Some(line)) = lines.next_line().await {
            if !fired {
                fired = true;
                let event = "VEL,GPIO6=5579046636,GPIO2=5565819589,DELTA=13227047(ticks at 8ns)\r\n";
                if writer.write_all(event.as_bytes()).await.is_err() {
                    break;
                }
            }
            let response = match line.trim() {
                "ARM?" => "ARM=1\r\n".to_string(),
                other => format!("ERR,{other}\r\n"),
            };
            if writer.write_all(response.as_bytes()).await.is_err() {
                break;
            }
        }
    });

    port
}

#[tokio::test]
async fn skips_unsolicited_frames_until_the_matching_reply() {
    let port = spawn_server_with_unsolicited_event().await;
    let pool = ConnectionPool::new();
    let cfg = config(port);

    let is_arm_reply = |line: &str| line.starts_with("ARM=");
    for _ in 0..5 {
        let response = pool
            .exchange_line_matching(&cfg, "ARM?", is_arm_reply)
            .await
            .unwrap();
        assert_eq!(response, "ARM=1");
    }
}

#[tokio::test]
async fn without_a_matcher_an_unsolicited_frame_desynchronises_the_stream() {
    let port = spawn_server_with_unsolicited_event().await;
    let pool = ConnectionPool::new();
    let cfg = config(port);

    // Documents why the matcher exists: the event frame is mistaken for the reply.
    let response = pool.exchange_line(&cfg, "ARM?").await.unwrap();
    assert!(response.starts_with("VEL,"), "got '{response}'");
}
