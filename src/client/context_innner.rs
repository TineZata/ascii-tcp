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

/// Connection state and callbacks shared by every clone of a [`Context`](super::Context).
///
/// Connection state is tracked per `host:port`; callbacks fire on state transitions only.
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

    /// Registers a callback invoked with the endpoint when it first becomes reachable (or recovers).
    pub(crate) fn on_connected<F>(&self, callback: F)
    where
        F: Fn(String) + Send + Sync + 'static,
    {
        if let Ok(mut callbacks) = self.on_connected_callbacks.lock() {
            callbacks.push(Arc::new(callback));
        }
    }

    /// Registers a callback invoked with `(endpoint, reason)` on every connection-level failure.
    pub(crate) fn on_disconnected<F>(&self, callback: F)
    where
        F: Fn(String, String) + Send + Sync + 'static,
    {
        if let Ok(mut callbacks) = self.on_disconnected_callbacks.lock() {
            callbacks.push(Arc::new(callback));
        }
    }

    /// Executes one command, accepting the first response line.
    pub(crate) async fn execute_command(
        &self,
        command: &Command,
    ) -> Result<CommandResult, AsciiTcpError> {
        self.execute_command_matching(command, |_| true).await
    }

    /// Executes one command and updates connection state: success marks the endpoint connected,
    /// failure is recorded via `record_failure`.
    pub(crate) async fn execute_command_matching<F>(
        &self,
        command: &Command,
        accept: F,
    ) -> Result<CommandResult, AsciiTcpError>
    where
        F: Fn(&str) -> bool,
    {
        let endpoint = format!("{}:{}", command.protocol.host, command.protocol.port);
        match self.backend.execute_command_matching(command, accept).await {
            Ok(result) => {
                self.mark_connected(&endpoint);
                Ok(result)
            }
            Err(error) => {
                self.record_failure(&endpoint, &command.request, &error);
                Err(error)
            }
        }
    }

    /// Sends one command without waiting for a reply, updating connection state like `execute_command_matching`.
    pub(crate) async fn send_command(&self, command: &Command) -> Result<(), AsciiTcpError> {
        let endpoint = format!("{}:{}", command.protocol.host, command.protocol.port);
        match self.backend.send_command(command).await {
            Ok(()) => {
                self.mark_connected(&endpoint);
                Ok(())
            }
            Err(error) => {
                self.record_failure(&endpoint, &command.request, &error);
                Err(error)
            }
        }
    }

    /// Executes a single command or a batch once, failing on the first error.
    pub(crate) async fn execute_payload(
        &self,
        payload: &Payload,
    ) -> Result<CommandResultPayload, AsciiTcpError> {
        execute_payload_with(payload, |command| async move {
            self.execute_command(&command).await
        })
        .await
    }

    /// Like `execute_payload`, but sleeps `retry_interval` and starts over on retryable errors.
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

    /// Flips the endpoint to connected and fires the callbacks only on a disconnected → connected transition.
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

    /// Records a failed request. A reply timeout says nothing about the connection (the device
    /// may simply ignore that command), so only connection-level errors count as a disconnect.
    fn record_failure(&self, endpoint: &str, payload: &str, error: &AsciiTcpError) {
        if matches!(error, AsciiTcpError::IoTimeout) {
            warn!(endpoint, "no matching reply to \"line={payload}\" before I/O timeout", payload = payload);
        } else {
            self.mark_disconnected(endpoint, error);
        }
    }

    /// Flips the endpoint to disconnected and fires the callbacks on every call, so each failed attempt is reported.
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

/// Runs every command of `batch` in order, stopping at the first error.
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

/// Dispatches `payload` to `execute`, preserving its single/batch shape in the result.
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

/// Repeats `execute` until it succeeds or fails with a non-retryable error, sleeping `retry_interval` between attempts.
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

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Mutex;

    use super::*;
    use crate::{HardwareProtocol, LineEnding};

    fn command(id: &str, port: u16, kind: &str) -> Command {
        Command {
            id: Some(id.to_string()),
            request: "READ".to_string(),
            protocol: HardwareProtocol {
                kind: kind.to_string(),
                host: "127.0.0.1".to_string(),
                port,
                line_ending: LineEnding::Lf,
                connect_timeout_ms: Some(200),
                io_timeout_ms: Some(200),
            },
        }
    }

    fn ok_result(command: &Command) -> Result<CommandResult, AsciiTcpError> {
        Ok(CommandResult {
            id: command.id.clone(),
            response: format!("ok:{}", command.id.clone().unwrap_or_default()),
        })
    }

    /// Records every callback invocation on `inner` into the returned log.
    fn record_events(inner: &ContextInner) -> Arc<Mutex<Vec<String>>> {
        let log = Arc::new(Mutex::new(Vec::new()));
        let connected = log.clone();
        inner.on_connected(move |endpoint| connected.lock().unwrap().push(format!("up {endpoint}")));
        let disconnected = log.clone();
        inner.on_disconnected(move |endpoint, reason| {
            disconnected.lock().unwrap().push(format!("down {endpoint} {reason}"))
        });
        log
    }

    /// A loopback port nothing is listening on. The refusal is verified so that a port already
    /// recycled by a concurrently running test is skipped.
    fn closed_port() -> u16 {
        loop {
            let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            let addr = listener.local_addr().unwrap();
            drop(listener);
            if std::net::TcpStream::connect_timeout(&addr, Duration::from_millis(200)).is_err() {
                return addr.port();
            }
        }
    }

    fn poison<T: Send + 'static>(mutex: &Arc<std::sync::Mutex<T>>) {
        let mutex = mutex.clone();
        let _ = std::thread::spawn(move || {
            let _guard = mutex.lock().unwrap();
            panic!("poison the lock");
        })
        .join();
    }

    #[test]
    fn mark_connected_fires_only_on_the_transition() {
        let inner = ContextInner::default();
        let log = record_events(&inner);

        inner.mark_connected("a:1");
        inner.mark_connected("a:1");
        inner.mark_connected("b:2");

        assert_eq!(*log.lock().unwrap(), ["up a:1", "up b:2"]);
    }

    #[test]
    fn mark_disconnected_fires_every_time_and_allows_a_later_reconnect() {
        let inner = ContextInner::new();
        let log = record_events(&inner);

        inner.mark_connected("a:1");
        inner.mark_disconnected("a:1", &AsciiTcpError::Eof);
        inner.mark_disconnected("a:1", &AsciiTcpError::Eof);
        inner.mark_connected("a:1");

        assert_eq!(
            *log.lock().unwrap(),
            [
                "up a:1",
                "down a:1 connection closed before response",
                "down a:1 connection closed before response",
                "up a:1",
            ]
        );
    }

    #[test]
    fn record_failure_treats_a_reply_timeout_as_not_a_disconnect() {
        let inner = ContextInner::new();
        let log = record_events(&inner);

        inner.record_failure("a:1", "READ", &AsciiTcpError::IoTimeout);
        assert!(log.lock().unwrap().is_empty());

        inner.record_failure("a:1", "READ", &AsciiTcpError::ConnectTimeout);
        assert_eq!(*log.lock().unwrap(), ["down a:1 connect timeout"]);
    }

    #[test]
    fn poisoned_callback_lists_are_skipped_not_fatal() {
        let inner = ContextInner::new();
        poison(&inner.on_connected_callbacks);
        poison(&inner.on_disconnected_callbacks);

        inner.on_connected(|_| panic!("must never be registered"));
        inner.on_disconnected(|_, _| panic!("must never be registered"));
        inner.mark_connected("a:1");
        inner.mark_disconnected("a:1", &AsciiTcpError::Eof);
    }

    #[test]
    fn poisoned_connection_state_suppresses_the_connected_event_only() {
        let inner = ContextInner::new();
        let log = record_events(&inner);
        poison(&inner.connection_state);

        inner.mark_connected("a:1");
        inner.mark_disconnected("a:1", &AsciiTcpError::Eof);

        assert_eq!(*log.lock().unwrap(), ["down a:1 connection closed before response"]);
    }

    #[tokio::test]
    async fn execute_payload_with_keeps_the_payload_shape() {
        let single = Payload::Single(command("one", 1, "ascii-tcp"));
        let result = execute_payload_with(&single, |c| async move { ok_result(&c) }).await.unwrap();
        assert!(matches!(result, CommandResultPayload::Single(r) if r.response == "ok:one"));

        let batch = Payload::Batch(BatchCommands {
            commands: vec![command("a", 1, "ascii-tcp"), command("b", 1, "ascii-tcp")],
        });
        let result = execute_payload_with(&batch, |c| async move { ok_result(&c) }).await.unwrap();
        let CommandResultPayload::Batch(batch) = result else { panic!("expected a batch result") };
        let responses: Vec<_> = batch.responses.iter().map(|r| r.response.as_str()).collect();
        assert_eq!(responses, ["ok:a", "ok:b"]);
    }

    #[tokio::test]
    async fn execute_batch_with_stops_at_the_first_error() {
        let batch = BatchCommands {
            commands: vec![
                command("a", 1, "ascii-tcp"),
                command("b", 1, "ascii-tcp"),
                command("c", 1, "ascii-tcp"),
            ],
        };
        let calls = AtomicUsize::new(0);

        let error = execute_batch_with(&batch, |c| {
            calls.fetch_add(1, Ordering::SeqCst);
            async move {
                if c.id.as_deref() == Some("b") {
                    Err(AsciiTcpError::Eof)
                } else {
                    ok_result(&c)
                }
            }
        })
        .await
        .unwrap_err();

        assert!(matches!(error, AsciiTcpError::Eof));
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn retry_with_interval_retries_transient_errors_until_success() {
        let attempts = AtomicUsize::new(0);
        let result = retry_with_interval("test", Duration::from_millis(1), || {
            let attempt = attempts.fetch_add(1, Ordering::SeqCst);
            async move {
                if attempt < 2 {
                    Err(AsciiTcpError::IoTimeout)
                } else {
                    Ok(attempt)
                }
            }
        })
        .await
        .unwrap();

        assert_eq!(result, 2);
    }

    #[tokio::test]
    async fn retry_with_interval_returns_non_retryable_errors_immediately() {
        let attempts = AtomicUsize::new(0);
        let error: AsciiTcpError = retry_with_interval("test", Duration::from_millis(1), || {
            attempts.fetch_add(1, Ordering::SeqCst);
            async { Err::<(), _>(AsciiTcpError::InvalidProtocolType("x".into())) }
        })
        .await
        .unwrap_err();

        assert!(matches!(error, AsciiTcpError::InvalidProtocolType(_)));
        assert_eq!(attempts.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn execute_command_reports_a_foreign_protocol_as_a_disconnect() {
        let inner = ContextInner::new();
        let log = record_events(&inner);

        let error = inner.execute_command(&command("x", 1, "modbus")).await.unwrap_err();

        assert!(matches!(error, AsciiTcpError::InvalidProtocolType(_)));
        assert_eq!(log.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn send_command_reports_connection_failures() {
        let port = closed_port();

        let inner = ContextInner::new();
        let log = record_events(&inner);

        assert!(inner.send_command(&command("x", port, "ascii-tcp")).await.is_err());
        assert_eq!(log.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn execute_payload_retrying_gives_up_on_non_retryable_errors() {
        let inner = ContextInner::new();
        let payload = Payload::Single(command("x", 1, "modbus"));

        let error = inner
            .execute_payload_retrying(&payload, Duration::from_millis(1))
            .await
            .unwrap_err();

        assert!(matches!(error, AsciiTcpError::InvalidProtocolType(_)));
    }
}
