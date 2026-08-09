use std::error::Error;
use std::time::Duration;

use ascii_tcp::{parse_backend_payload, ClientConfig, CommandResultPayload, Context};

fn print_result(result: CommandResultPayload) {
    match result {
        CommandResultPayload::Single(result) => {
            println!("id={:?} response='{}'", result.id, result.response);
        }
        CommandResultPayload::Batch(batch) => {
            for result in batch.responses {
                println!("id={:?} response='{}'", result.id, result.response);
            }
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let config_path = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "examples/backend-config.json".to_string());
    let json = std::fs::read_to_string(config_path)?;
    let payload = parse_backend_payload(&json)?;
    let context = Context::new(ClientConfig {
        retry_interval: Duration::from_secs(2),
        retry_forever: false,
    });

    context.on_connected(|endpoint| println!("connected: {endpoint}"));
    context.on_disconnected(|endpoint, reason| {
        eprintln!("disconnected: {endpoint}: {reason}");
    });

    print_result(context.execute_payload_once(&payload).await?);
    Ok(())
}
