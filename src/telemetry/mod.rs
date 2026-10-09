//! KIP-714 client telemetry.
//!
//! Every client starts a reporter when its `metrics_push` switch is on (the
//! default for producers and consumers, off for the admin client, as Java's
//! `enable.metrics.push`). The reporter asks a broker for its metrics
//! subscription (`GetTelemetrySubscriptions`) and pushes the subscribed
//! metrics of the client's [`Metrics`](crate::metrics::Metrics) snapshot as
//! OTLP (`PushTelemetry`) at the interval the broker sets. Closing the client
//! sends one last push marked `terminating`.
//!
//! A broker without a telemetry plugin does not advertise the two APIs; the
//! reporter then stops at once, sends nothing and logs at `debug`. So does a
//! subscription that requests no metrics, apart from re-polling it.

mod otlp;
mod reporter;
#[cfg(all(test, feature = "test-broker"))]
mod tests;

use std::sync::Arc;
use std::time::Duration;

use tokio::sync::watch;
use tokio::task::JoinHandle;

use crate::client::Kafka;
use crate::error::{KrafkaError, Result};
use crate::metrics::{ClientInstanceId, MetricsSource};

/// The KIP-714 client type: the second segment of every pushed metric name.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ClientType {
    Producer,
    Consumer,
    Admin,
}

impl ClientType {
    fn name(self) -> &'static str {
        match self {
            Self::Producer => "producer",
            Self::Consumer => "consumer",
            Self::Admin => "admin",
        }
    }
}

/// What the reporter knows about the client instance id.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum InstanceId {
    /// No subscription yet.
    Pending,
    Assigned(ClientInstanceId),
    /// The cluster does not support client telemetry.
    Unavailable,
}

/// A client's telemetry: a running reporter, or nothing when the switch is
/// off. Dropping it aborts the reporter.
#[derive(Debug)]
pub(crate) struct Telemetry(Option<Running>);

#[derive(Debug)]
struct Running {
    stop: watch::Sender<bool>,
    instance_id: watch::Receiver<InstanceId>,
    task: parking_lot::Mutex<Option<JoinHandle<()>>>,
}

impl Telemetry {
    /// No reporter: the switch is off.
    pub(crate) fn disabled() -> Self {
        Self(None)
    }

    /// Start the reporter for a client, or nothing when `enabled` is false.
    /// Outside a Tokio runtime there is nothing to run it on, and the client
    /// has no reporter.
    pub(crate) fn start(
        enabled: bool,
        kafka: &Kafka,
        client_type: ClientType,
        source: Arc<MetricsSource>,
    ) -> Self {
        if !enabled {
            return Self::disabled();
        }
        let Ok(runtime) = tokio::runtime::Handle::try_current() else {
            tracing::debug!("no Tokio runtime; client telemetry not started");
            return Self::disabled();
        };
        let (stop, stop_rx) = watch::channel(false);
        let (id_tx, instance_id) = watch::channel(InstanceId::Pending);
        let reporter = reporter::Reporter::new(kafka.clone(), client_type, source);
        let task = runtime.spawn(reporter.run(stop_rx, id_tx));
        Self(Some(Running {
            stop,
            instance_id,
            task: parking_lot::Mutex::new(Some(task)),
        }))
    }

    /// Stop the reporter, letting it send its terminating push within
    /// `timeout`. Later calls return at once.
    pub(crate) async fn close(&self, timeout: Duration) {
        let Some(running) = &self.0 else {
            return;
        };
        let Some(task) = running.task.lock().take() else {
            return;
        };
        let _ = running.stop.send(true);
        // Aborted however this ends, including by the caller dropping it.
        let mut task = AbortOnDrop(task);
        if tokio::time::timeout(timeout, &mut task.0).await.is_err() {
            tracing::debug!("client telemetry did not finish its terminating push in time");
        }
    }

    /// The broker-assigned client instance id.
    ///
    /// `Ok(None)` when the cluster does not support client telemetry.
    ///
    /// # Errors
    ///
    /// [`KrafkaError::IllegalState`] when the switch is off;
    /// [`KrafkaError::Timeout`] when no broker answered within `timeout`.
    pub(crate) async fn client_instance_id(
        &self,
        timeout: Duration,
    ) -> Result<Option<ClientInstanceId>> {
        let Some(running) = &self.0 else {
            return Err(KrafkaError::illegal_state(
                "client telemetry is disabled; enable it with metrics_push(true)",
            ));
        };
        let mut rx = running.instance_id.clone();
        let wait = rx.wait_for(|id| *id != InstanceId::Pending);
        match tokio::time::timeout(timeout, wait).await {
            Ok(Ok(id)) => Ok(match *id {
                InstanceId::Assigned(id) => Some(id),
                InstanceId::Pending | InstanceId::Unavailable => None,
            }),
            // The reporter ended without an answer.
            Ok(Err(_)) => Ok(None),
            Err(_) => Err(KrafkaError::timeout("waiting for the client instance id")),
        }
    }
}

/// Aborts the task when dropped; a no-op once it has finished.
struct AbortOnDrop(JoinHandle<()>);

impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        self.0.abort();
    }
}

impl Drop for Telemetry {
    fn drop(&mut self) {
        if let Some(running) = &self.0
            && let Some(task) = running.task.lock().take()
        {
            task.abort();
        }
    }
}
