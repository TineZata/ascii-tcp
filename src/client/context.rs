use crate::{parse_backend_payload, AsciiTcpError, CommandResultPayload, Payload};

use super::tcp::ContextInner;
use super::ClientConfig;

#[derive(Clone, Default)]
pub struct Context {
    inner: ContextInner,
    config: ClientConfig,
}

impl Context {
    pub fn from_env() -> Self {
        Self {
            inner: ContextInner::new(),
            config: ClientConfig::from_env(),
        }
    }

    pub fn new(config: ClientConfig) -> Self {
        Self {
            inner: ContextInner::new(),
            config,
        }
    }

    pub fn on_connected<F>(&self, callback: F)
    where
        F: Fn(String) + Send + Sync + 'static,
    {
        self.inner.on_connected(callback);
    }

    pub fn on_disconnected<F>(&self, callback: F)
    where
        F: Fn(String, String) + Send + Sync + 'static,
    {
        self.inner.on_disconnected(callback);
    }

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
