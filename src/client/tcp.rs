use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use tracing::{debug, info, warn};

use crate::{
    AsciiTcpError, BatchCommands, BatchResult, Command, CommandResult, CommandResultPayload,
    Payload, TcpBackend,
};

type ConnectedCallback = Arc<dyn Fn(String) + Send + Sync>;
type DisconnectedCallback = Arc<dyn Fn(String, String) + Send + Sync>;

#[derive(Clone)]
pub(crate) struct ContextInner {
    backend: Arc<TcpBackend>,
    connection_state: Arc<std::sync::Mutex<HashMap<String, bool>>>,
    on_connected_callbacks: Arc<std::sync::Mutex<Vec<ConnectedCallback>>>,
    on_disconnected_callbacks: Arc<std::sync::Mutex<Vec<DisconnectedCallback>>>,
}

impl ContextInner {
    pub(crate) fn new() -> Self {
        Self {
            backend: Arc::new(TcpBackend::new()),
            connection_state: Arc::new(std::sync::Mutex::new(HashMap::new())),
            on_connected_callbacks: Arc::new(std::sync::Mutex::new(Vec::new())),
            on_disconnected_callbacks: Arc::new(std::sync::Mutex::new(Vec::new())),
        }
    }

    pub(crate) fn on_connected<F>(&self, callback: F)
    where
        F: Fn(String) + Send + Sync + 'static,
    {
        if let Ok(mut callbacks) = self.on_connected_callbacks.lock() {
            callbacks.push(Arc::new(callback));
        }
    }

    pub(crate) fn on_disconnected<F>(&self, callback: F)
    where
        F: Fn(String, String) + Send + Sync + 'static,
    {
        if let Ok(mut callbacks) = self.on_disconnected_callbacks.lock() {
            callbacks.push(Arc::new(callback));
        }
    }

    pub(crate) async fn execute_command(
        &self,
        command: &Command,
    ) -> Result<CommandResult, AsciiTcpError> {
        let endpoint = format!("{}:{}", command.protocol.host, command.protocol.port);
        match self.backend.execute_command(command).await {
            Ok(result) => {
                self.mark_connected(&endpoint);
                Ok(result)
            }
            Err(error) => {
                self.mark_disconnected(&endpoint, &error);
                Err(error)
            }
        }
    }

    pub(crate) async fn execute_payload(
        &self,
        payload: &Payload,
    ) -> Result<CommandResultPayload, AsciiTcpError> {
        execute_payload_with(payload, |command| async move {
            self.execute_command(&command).await
        })
        .await
    }

    pub(crate) async fn execute_payload_retrying(
        &self,
        payload: &Payload,
        retry_interval: Duration,
    ) -> Result<CommandResultPayload, AsciiTcpError> {
        retry_with_interval("execute_payload", retry_interval, || {
            self.execute_payload(payload)
        })
        .await
    }

    fn mark_connected(&self, endpoint: &str) {
        let mut should_emit = false;
        if let Ok(mut state) = self.connection_state.lock() {
            let was_connected = state.get(endpoint).copied().unwrap_or(false);
            if !was_connected {
                state.insert(endpoint.to_string(), true);
                should_emit = true;
            }
        }
        if should_emit {
            info!(endpoint, "TCP connection established");
            self.emit_connected(endpoint.to_string());
        }
    }

    fn mark_disconnected(&self, endpoint: &str, error: &AsciiTcpError) {
        if let Ok(mut state) = self.connection_state.lock() {
            state.insert(endpoint.to_string(), false);
        }
        warn!(endpoint, error = %error, "TCP connection attempt failed");
        self.emit_disconnected(endpoint.to_string(), error.to_string());
    }

    fn emit_connected(&self, endpoint: String) {
        let callbacks = if let Ok(callbacks) = self.on_connected_callbacks.lock() {
            callbacks.clone()
        } else {
            debug!("connected callback lock poisoned; skipping callbacks");
            Vec::new()
        };
        for callback in callbacks {
            callback(endpoint.clone());
        }
    }

    fn emit_disconnected(&self, endpoint: String, reason: String) {
        let callbacks = if let Ok(callbacks) = self.on_disconnected_callbacks.lock() {
            callbacks.clone()
        } else {
            debug!("disconnected callback lock poisoned; skipping callbacks");
            Vec::new()
        };
        for callback in callbacks {
            callback(endpoint.clone(), reason.clone());
        }
    }
}

impl Default for ContextInner {
    fn default() -> Self {
        Self::new()
    }
}

async fn execute_batch_with<F, Fut>(
    batch: &BatchCommands,
    mut execute: F,
) -> Result<BatchResult, AsciiTcpError>
where
    F: FnMut(Command) -> Fut,
    Fut: Future<Output = Result<CommandResult, AsciiTcpError>>,
{
    let mut responses = Vec::with_capacity(batch.commands.len());
    for command in &batch.commands {
        responses.push(execute(command.clone()).await?);
    }
    Ok(BatchResult { responses })
}

async fn execute_payload_with<F, Fut>(
    payload: &Payload,
    mut execute: F,
) -> Result<CommandResultPayload, AsciiTcpError>
where
    F: FnMut(Command) -> Fut,
    Fut: Future<Output = Result<CommandResult, AsciiTcpError>>,
{
    match payload {
        Payload::Single(command) => Ok(CommandResultPayload::Single(
            execute(command.clone()).await?,
        )),
        Payload::Batch(batch) => Ok(CommandResultPayload::Batch(
            execute_batch_with(batch, execute).await?,
        )),
    }
}

async fn retry_with_interval<T, F, Fut>(
    operation: &str,
    retry_interval: Duration,
    mut execute: F,
) -> Result<T, AsciiTcpError>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, AsciiTcpError>>,
{
    loop {
        match execute().await {
            Ok(result) => return Ok(result),
            Err(error) if error.is_retryable() => {
                warn!(
                    operation,
                    error = %error,
                    retry_ms = retry_interval.as_millis(),
                    "retryable execution error"
                );
                tokio::time::sleep(retry_interval).await;
            }
            Err(error) => return Err(error),
        }
    }
}
