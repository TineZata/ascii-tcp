use std::error::Error;
use std::time::Duration;

use ascii_tcp::{parse_backend_payload, CommandResultPayload, Context, ClientConfig};
use tracing::{error, info};
use tracing_subscriber::EnvFilter;

fn print_result(result: CommandResultPayload) {
    match result {
        CommandResultPayload::Single(result) => {
            info!(id = ?result.id, response = %result.response, "TCP response");
        }
        CommandResultPayload::Batch(batch) => {
            for result in batch.responses {
                info!(id = ?result.id, response = %result.response, "TCP response");
            }
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .with_target(false)
        .init();

    let config_path = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "examples/backend-config.json".to_string());
    let json = std::fs::read_to_string(config_path)?;
    let payload = parse_backend_payload(&json)?;
    let context = Context::new(ClientConfig {
        retry_interval: Duration::from_secs(2),
        retry_forever: true,
    });

    context.on_connected(|endpoint| info!(%endpoint, "connected"));
    context.on_disconnected(|endpoint, reason| {
        error!(%endpoint, %reason, "connection attempt failed");
    });

    loop {
        match context.execute_payload(&payload).await {
            Ok(result) => print_result(result),
            Err(error) => error!(%error, "payload execution failed"),
        }
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
}
