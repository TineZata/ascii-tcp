use std::error::Error;
use std::time::Duration;

use ascii_tcp::{exchange_line, AsciiTcpConfig, LineEnding};
use serde::Deserialize;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpListener;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ExampleConfig {
    id: String,
    #[serde(rename = "type")]
    widget_type: String,
    label: String,
    data_type: String,
    protocol: ProtocolConfig,
    request: String,
    #[serde(default = "default_simulate_server")]
    simulate_server: bool,
    #[serde(default = "default_simulated_response")]
    simulated_response: String,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "kebab-case")]
#[serde(deny_unknown_fields)]
enum ProtocolConfig {
    AsciiTcp(AsciiTcpProtocol),
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct AsciiTcpProtocol {
    host: String,
    port: u16,
    #[serde(default)]
    line_ending: LineEndingConfig,
}

#[derive(Debug, Clone, Copy, Deserialize, Default)]
#[serde(rename_all = "kebab-case")]
enum LineEndingConfig {
    #[default]
    Lf,
    CrLf,
    Cr,
}

fn default_simulate_server() -> bool {
    true
}

fn default_simulated_response() -> String {
    "23.75".to_string()
}

fn to_line_ending(ending: LineEndingConfig) -> LineEnding {
    match ending {
        LineEndingConfig::Lf => LineEnding::Lf,
        LineEndingConfig::CrLf => LineEnding::CrLf,
        LineEndingConfig::Cr => LineEnding::Cr,
    }
}

fn validate_config(cfg: &ExampleConfig) -> Result<(), String> {
    if cfg.id.trim().is_empty() {
        return Err("Invalid config: 'id' must be non-empty".to_string());
    }
    if cfg.widget_type.trim().is_empty() {
        return Err("Invalid config: 'type' must be non-empty".to_string());
    }
    if cfg.label.trim().is_empty() {
        return Err("Invalid config: 'label' must be non-empty".to_string());
    }
    if cfg.data_type.trim().is_empty() {
        return Err("Invalid config: 'data_type' must be non-empty".to_string());
    }
    if cfg.request.trim().is_empty() {
        return Err("Invalid config: 'request' must be non-empty".to_string());
    }
    if cfg.simulate_server && cfg.simulated_response.is_empty() {
        return Err("Invalid config: 'simulated_response' must be non-empty when 'simulate_server' is true".to_string());
    }

    match &cfg.protocol {
        ProtocolConfig::AsciiTcp(proto) => {
            if proto.host.trim().is_empty() {
                return Err("Invalid config.protocol.ascii-tcp: 'host' must be non-empty".to_string());
            }
            if proto.port == 0 {
                return Err("Invalid config.protocol.ascii-tcp: 'port' must be > 0".to_string());
            }
        }
    }

    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let args: Vec<String> = std::env::args().collect();
    let config_path = args
        .get(1)
        .cloned()
        .unwrap_or_else(|| "examples/config.json".to_string());

    let raw = std::fs::read_to_string(&config_path)?;
    let cfg_file: ExampleConfig = serde_json::from_str(&raw)?;
    validate_config(&cfg_file).map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidInput, e))?;

    let ProtocolConfig::AsciiTcp(proto) = cfg_file.protocol;
    let mut host = proto.host;
    let mut port = proto.port;

    let server_task = if cfg_file.simulate_server {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let addr = listener.local_addr()?;
        host = addr.ip().to_string();
        port = addr.port();

        let response = cfg_file.simulated_response.clone();
        Some(tokio::spawn(async move {
            let (stream, _) = listener.accept().await?;
            let (reader, mut writer) = tokio::io::split(stream);
            let mut reader = BufReader::new(reader);

            let mut line = String::new();
            let read_n = reader.read_line(&mut line).await?;
            if read_n == 0 {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "client closed before sending a request",
                ));
            }

            writer.write_all(format!("{}\n", response).as_bytes()).await?;
            writer.flush().await
        }))
    } else {
        None
    };

    let req_cfg = AsciiTcpConfig {
        host,
        port,
        connect_timeout: Duration::from_secs(2),
        io_timeout: Duration::from_secs(2),
        line_ending: to_line_ending(proto.line_ending),
    };

    let response = exchange_line(&req_cfg, &cfg_file.request).await?;
    println!(
        "id='{}' type='{}' label='{}' data_type='{}' request='{}' response='{}'",
        cfg_file.id, cfg_file.widget_type, cfg_file.label, cfg_file.data_type, cfg_file.request, response
    );

    if let Some(task) = server_task {
        task.await??;
    }

    Ok(())
}
