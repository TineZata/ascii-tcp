use crate::{parse_backend_payload, AsciiTcpError, CommandResultPayload, Payload};

use super::tcp::ContextInner;
use super::ClientConfig;

#[derive(Clone, Default)]
pub struct Context {
    inner: ContextInner,
    config: ClientConfig,
}

impl Context {
    /// Creates a new `Context` with configuration loaded from environment variables.
    /// Env variables:
    /// - `ASCII_TCP_RETRY_MS`: Retry interval in milliseconds (default: 2000)
    /// - `ASCII_TCP_RETRY_FOREVER`: Whether to retry forever on connection failure (default: true)
    pub fn from_env() -> Self {
        Self {
            inner: ContextInner::new(),
            config: ClientConfig::from_env(),
        }
    }

    /// Creates a new `Context` with the provided `ClientConfig`.
    pub fn new(config: ClientConfig) -> Self {
        Self {
            inner: ContextInner::new(),
            config,
        }
    }

    /// Registers a callback to be called when a connection is established.
    pub fn on_connected<F>(&self, callback: F)
    where
        F: Fn(String) + Send + Sync + 'static,
    {
        self.inner.on_connected(callback);
    }

    /// Registers a callback to be called when a connection is lost.
    pub fn on_disconnected<F>(&self, callback: F)
    where
        F: Fn(String, String) + Send + Sync + 'static,
    {
        self.inner.on_disconnected(callback);
    }

    /// Attempts to execute a payload, retrying on connection failure if `retry_forever` is set in the configuration.
    /// Default retry interval is 2 seconds, but can be configured via `ClientConfig`.
    pub async fn execute_payload(
        &self,
        payload: &Payload,
    ) -> Result<CommandResultPayload, AsciiTcpError> {
        if self.config.retry_forever {
            self.inner
                .execute_payload_retrying(payload, self.config.retry_interval)
                .await
        } else {
            self.inner.execute_payload(payload).await
        }
    }

    pub async fn execute_payload_once(
        &self,
        payload: &Payload,
    ) -> Result<CommandResultPayload, AsciiTcpError> {
        self.inner.execute_payload(payload).await
    }

    pub async fn execute_json(&self, json: &str) -> Result<CommandResultPayload, AsciiTcpError> {
        let payload = parse_backend_payload(json)?;
        self.execute_payload(&payload).await
    }

    pub fn config(&self) -> &ClientConfig {
        &self.config
    }
}
