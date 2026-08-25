use std::error::Error;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};

use ascii_tcp::{Command, Server, ServerConfig};
use serde::Deserialize;
use tracing::info;
use tracing_subscriber::EnvFilter;

#[derive(Debug, Deserialize)]
struct BackendConfig {
    commands: Vec<ServerCommand>,
}

#[derive(Debug, Deserialize)]
struct ServerCommand {
    #[serde(flatten)]
    command: Command,
    /// Whether `backend_server` should host this command as a simulated server. Entries
    /// with `server: false` (or omitted) describe real devices and are skipped here.
    #[serde(default)]
    server: bool,
}

#[derive(Debug, Clone, Copy)]
enum Direction {
    Increase,
    Decrease,
}

impl Direction {
    fn apply(self, value: &mut f64, step: f64) {
        match self {
            Self::Increase => *value += step,
            Self::Decrease => *value -= step,
        }
    }
}

fn simulation_for(command: &Command) -> (f64, f64, Direction) {
    if command.request.eq_ignore_ascii_case("READ TEMP") {
        (100.0, 1.0, Direction::Decrease)
    } else {
        (0.0, 1.0, Direction::Increase)
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
    let config: BackendConfig = serde_json::from_str(&json)?;
    let hosted_commands: Vec<_> = config.commands.into_iter().filter(|c| c.server).collect();
    let mut servers = Vec::with_capacity(hosted_commands.len());

    for server_command in hosted_commands {
        let command_id = server_command
            .command
            .id
            .clone()
            .unwrap_or_else(|| server_command.command.request.clone());
        let (initial_value, step, direction) = simulation_for(&server_command.command);
        let protocol = server_command.command.protocol;
        let bind_addr: SocketAddr = format!("{}:{}", protocol.host, protocol.port).parse()?;
        let value = Arc::new(Mutex::new(initial_value));
        let value_for_handler = Arc::clone(&value);
        let command_id_for_handler = command_id.clone();

        let server = Server::start(
            ServerConfig {
                bind_addr,
                line_ending: protocol.line_ending,
                ..ServerConfig::default()
            },
            move |request, peer| {
                let value = Arc::clone(&value_for_handler);
                let command_id = command_id_for_handler.clone();
                async move {
                    let response = {
                        let mut value = value.lock().unwrap_or_else(|error| error.into_inner());
                        let response = *value;
                        direction.apply(&mut value, step);
                        response
                    };
                    info!(
                        %command_id,
                        %peer,
                        %request,
                        value = response,
                        "served value"
                    );
                    Ok(response.to_string())
                }
            },
        )
        .await?;

        info!(
            %command_id,
            address = %server.local_addr(),
            ?direction,
            "server started"
        );
        servers.push(server);
    }

    info!("backend servers running; stop with Ctrl+C");
    std::future::pending::<()>().await;
    drop(servers);
    Ok(())
}
