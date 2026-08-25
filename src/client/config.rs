use std::time::Duration;

/// Retry behaviour for [`Context`](super::Context).
#[derive(Debug, Clone)]
pub struct ClientConfig {
    /// How long to wait between retry attempts on transient errors.
    pub retry_interval: Duration,
    /// If `true`, [`Context::execute_payload`](super::Context::execute_payload) retries on transient errors indefinitely.
    pub retry_forever: bool,
}

impl ClientConfig {
    /// Loads `ClientConfig` from `ASCII_TCP_RETRY_MS` and `ASCII_TCP_RETRY_FOREVER` environment variables.
    pub fn from_env() -> Self {
        let retry_interval_ms = std::env::var("ASCII_TCP_RETRY_MS")
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
            .unwrap_or(2_000);

        let retry_forever = std::env::var("ASCII_TCP_RETRY_FOREVER")
            .map(|value| !value.eq_ignore_ascii_case("false") && !value.eq_ignore_ascii_case("no"))
            .unwrap_or(true);

        Self {
            retry_interval: Duration::from_millis(retry_interval_ms),
            retry_forever,
        }
    }
}

impl Default for ClientConfig {
    fn default() -> Self {
        Self {
            retry_interval: Duration::from_secs(2),
            retry_forever: true,
        }
    }
}
