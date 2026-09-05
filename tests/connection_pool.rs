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
