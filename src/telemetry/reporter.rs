//! The KIP-714 reporter task.
//!
//! One per client. It subscribes, pushes at the broker's interval, re-polls
//! an empty subscription at that interval, re-subscribes when the broker
//! says so, and sends one terminating push when the client closes. It stays
//! on one broker connection until that connection fails. Every log line is at
//! `debug`: on a cluster without telemetry it must be invisible.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use tokio::sync::watch;
use tracing::debug;

use super::otlp::{self, Point, Value};
use super::{ClientType, InstanceId};
use crate::client::Kafka;
use crate::error::{ErrorCode, KrafkaError, Result};
use crate::metrics::{ClientInstanceId, Kind, Metrics, MetricsSource, Section};
use crate::network::BrokerConnection;
use crate::protocol::{
    ApiKey, Compression, GetTelemetrySubscriptionsRequest, GetTelemetrySubscriptionsResponse,
    PushTelemetryRequest, PushTelemetryResponse, VersionedDecode, versions,
};

/// The KIP-714 metric name prefix of the Java clients.
const METRICS_PREFIX: &str = "org.apache.kafka";

/// Push intervals outside this range are clamped.
const MIN_PUSH_INTERVAL: Duration = Duration::from_millis(100);
const MAX_PUSH_INTERVAL: Duration = Duration::from_secs(3600);

/// Backoff between failed subscription attempts: doubling, capped.
const RETRY_BACKOFF: Duration = Duration::from_secs(1);
const MAX_RETRY_BACKOFF: Duration = Duration::from_secs(30);

/// A broker's metrics subscription.
#[derive(Debug, Clone)]
pub(super) struct Subscription {
    pub(super) instance_id: [u8; 16],
    pub(super) subscription_id: i32,
    pub(super) push_interval: Duration,
    pub(super) delta: bool,
    pub(super) compression: Vec<Compression>,
    pub(super) max_bytes: usize,
    /// Name prefixes; `"*"` selects everything, and an empty list nothing.
    pub(super) requested: Vec<String>,
}

impl Subscription {
    fn from_response(response: GetTelemetrySubscriptionsResponse, id: [u8; 16]) -> Self {
        let interval = u64::try_from(response.push_interval_ms).unwrap_or(0);
        Self {
            instance_id: if id == [0; 16] {
                response.client_instance_id
            } else {
                id
            },
            subscription_id: response.subscription_id,
            push_interval: Duration::from_millis(interval)
                .clamp(MIN_PUSH_INTERVAL, MAX_PUSH_INTERVAL),
            delta: response.delta_temporality,
            compression: response
                .accepted_compression_types
                .iter()
                .filter_map(|c| Compression::from_i8(*c))
                .collect(),
            max_bytes: usize::try_from(response.telemetry_max_bytes)
                .ok()
                .filter(|b| *b > 0)
                .unwrap_or(usize::MAX),
            requested: response.requested_metrics,
        }
    }

    fn wants(&self, name: &str) -> bool {
        self.requested
            .iter()
            .any(|prefix| prefix == "*" || name.starts_with(prefix.as_str()))
    }
}

/// What a push came to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Pushed {
    Ok,
    /// The subscription changed or the payload did not fit: subscribe again.
    Resubscribe,
    /// Try again at the next interval, on another connection if this one
    /// failed.
    Retry,
    /// The broker will never accept a push from this client.
    Stop,
}

/// Every metric `metrics` holds for `client_type`, named
/// `org.apache.kafka.<client type>.<metric>`.
pub(super) fn points(metrics: &Metrics, client_type: ClientType) -> Vec<Point> {
    let relevant = |section| match section {
        Section::Producer => client_type == ClientType::Producer,
        Section::Consumer => client_type == ClientType::Consumer,
        Section::Connection => true,
    };
    let name = |suffix: &str| format!("{METRICS_PREFIX}.{}.{suffix}", client_type.name());
    let mut out = Vec::new();
    for m in metrics.scalars() {
        if relevant(m.section) {
            out.push(Point {
                name: name(m.push),
                help: m.help,
                value: match m.kind {
                    Kind::Counter => Value::Sum(m.value),
                    Kind::Gauge => Value::Gauge(m.value),
                },
            });
        }
    }
    for m in metrics.latencies() {
        if relevant(m.section) {
            let ms = |d: Duration| d.as_secs_f64() * 1000.0;
            out.push(Point {
                name: name(&format!("{}.avg", m.push)),
                help: m.help,
                value: Value::GaugeF64(m.value.mean().map_or(0.0, ms)),
            });
            out.push(Point {
                name: name(&format!("{}.max", m.push)),
                help: m.help,
                value: Value::GaugeF64(ms(m.value.max)),
            });
        }
    }
    out
}

fn now_nanos() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| u64::try_from(d.as_nanos()).unwrap_or(u64::MAX))
}

pub(super) struct Reporter {
    kafka: Kafka,
    client_type: ClientType,
    source: Arc<MetricsSource>,
    connection: Option<Arc<BrokerConnection>>,
    /// Sum values of the last accepted push, for delta temporality.
    pushed: HashMap<String, u64>,
    /// Start of the current push window.
    window_start: u64,
}

impl Reporter {
    pub(super) fn new(kafka: Kafka, client_type: ClientType, source: Arc<MetricsSource>) -> Self {
        Self {
            kafka,
            client_type,
            source,
            connection: None,
            pushed: HashMap::new(),
            window_start: now_nanos(),
        }
    }

    pub(super) async fn run(
        mut self,
        mut stop: watch::Receiver<bool>,
        instance_id: watch::Sender<InstanceId>,
    ) {
        let Some(mut subscription) = self.subscribe([0; 16], &mut stop).await else {
            instance_id.send_replace(InstanceId::Unavailable);
            return;
        };
        instance_id.send_replace(InstanceId::Assigned(ClientInstanceId::new(
            subscription.instance_id,
        )));
        debug!(
            push_interval = ?subscription.push_interval,
            requested_metrics = ?subscription.requested,
            "client telemetry subscribed"
        );

        // KIP-714: the first push lands at a random point between 0.5 and
        // 1.5 intervals, so a fleet restarted together does not push together.
        let mut wait = subscription
            .push_interval
            .mul_f64(crate::util::with_rng(|rng| {
                rand::Rng::random_range(rng, 0.5..1.5)
            }));
        loop {
            let stopped = tokio::select! {
                _ = stop.wait_for(|stop| *stop) => true,
                () = tokio::time::sleep(wait) => false,
            };
            wait = subscription.push_interval;
            if subscription.requested.is_empty() {
                if stopped {
                    return;
                }
                // Nothing subscribed: look again in one interval.
                match self.subscribe(subscription.instance_id, &mut stop).await {
                    Some(next) => subscription = next,
                    None => return,
                }
                continue;
            }
            match self.push(&subscription, stopped).await {
                Pushed::Ok | Pushed::Retry => {}
                Pushed::Stop => return,
                Pushed::Resubscribe if !stopped => {
                    match self.subscribe(subscription.instance_id, &mut stop).await {
                        Some(next) => {
                            self.pushed.clear();
                            subscription = next;
                        }
                        None => return,
                    }
                }
                Pushed::Resubscribe => {}
            }
            if stopped {
                return;
            }
        }
    }

    /// A connection to a broker that serves both telemetry APIs: an open one
    /// if any does, else a new one to a known broker. `Ok(None)` when no
    /// broker supports them.
    async fn connection(&mut self) -> Result<Option<Arc<BrokerConnection>>> {
        let serves = |conn: &BrokerConnection| {
            conn.negotiate_api_version(
                ApiKey::GetTelemetrySubscriptions,
                versions::GET_TELEMETRY_SUBSCRIPTIONS_MAX,
                versions::GET_TELEMETRY_SUBSCRIPTIONS_MIN,
            )
            .is_some()
                && conn
                    .negotiate_api_version(
                        ApiKey::PushTelemetry,
                        versions::PUSH_TELEMETRY_MAX,
                        versions::PUSH_TELEMETRY_MIN,
                    )
                    .is_some()
        };
        if let Some(conn) = &self.connection
            && conn.is_usable()
        {
            return Ok(Some(Arc::clone(conn)));
        }
        let mut open = self.kafka.pool().open_connections();
        if open.is_empty() {
            let mut addresses: Vec<String> = self
                .kafka
                .metadata()
                .brokers()
                .iter()
                .map(|b| b.address().to_string())
                .collect();
            if addresses.is_empty() {
                addresses = self.kafka.metadata().bootstrap_servers();
            }
            let Some(address) = addresses.get(crate::util::with_rng(|rng| {
                rand::Rng::random_range(rng, 0..addresses.len().max(1))
            })) else {
                return Ok(None);
            };
            open.push(self.kafka.pool().get_connection(address).await?);
        }
        self.connection = open.into_iter().find(|conn| serves(conn));
        Ok(self.connection.clone())
    }

    /// Obtain a subscription, retrying transient failures until `stop`.
    /// `None` when the cluster does not support telemetry, the broker refuses
    /// for good, or the client is closing.
    async fn subscribe(
        &mut self,
        instance_id: [u8; 16],
        stop: &mut watch::Receiver<bool>,
    ) -> Option<Subscription> {
        let mut backoff = RETRY_BACKOFF;
        loop {
            if *stop.borrow() {
                return None;
            }
            match self.get_subscription(instance_id).await {
                Ok(Some(subscription)) => return Some(subscription),
                Ok(None) => return None,
                Err(error) if error.is_retriable() => {
                    debug!(%error, "telemetry subscription failed; retrying");
                    self.connection = None;
                }
                Err(error) => {
                    debug!(%error, "telemetry subscription refused; client telemetry stops");
                    return None;
                }
            }
            let jittered = backoff.mul_f64(crate::util::with_rng(|rng| {
                rand::Rng::random_range(rng, 0.8..1.2)
            }));
            tokio::select! {
                _ = stop.wait_for(|stop| *stop) => return None,
                () = tokio::time::sleep(jittered) => {}
            }
            backoff = (backoff * 2).min(MAX_RETRY_BACKOFF);
        }
    }

    async fn get_subscription(&mut self, instance_id: [u8; 16]) -> Result<Option<Subscription>> {
        let Some(conn) = self.connection().await? else {
            debug!("no broker supports client telemetry (KIP-714); client telemetry stops");
            return Ok(None);
        };
        let request = GetTelemetrySubscriptionsRequest {
            client_instance_id: instance_id,
        };
        let response = conn
            .send_request(ApiKey::GetTelemetrySubscriptions, 0, |buf| {
                request.encode_v0(buf)
            })
            .await?;
        let response =
            GetTelemetrySubscriptionsResponse::decode_versioned(0, &mut response.as_ref())?;
        if response.error_code != ErrorCode::None {
            return Err(KrafkaError::broker(
                response.error_code,
                "GetTelemetrySubscriptions",
            ));
        }
        Ok(Some(Subscription::from_response(response, instance_id)))
    }

    /// Push the subscribed metrics of the client's current snapshot.
    async fn push(&mut self, subscription: &Subscription, terminating: bool) -> Pushed {
        let now = now_nanos();
        let mut sums = Vec::new();
        let points: Vec<Point> = points(&self.source.snapshot(), self.client_type)
            .into_iter()
            .filter(|point| subscription.wants(&point.name))
            .map(|mut point| {
                if let Value::Sum(total) = point.value {
                    sums.push((point.name.clone(), total));
                    if subscription.delta {
                        let last = self.pushed.get(&point.name).copied().unwrap_or(0);
                        point.value = Value::Sum(total.saturating_sub(last));
                    }
                }
                point
            })
            .collect();
        if points.is_empty() && !terminating {
            debug!("no metric matches the telemetry subscription; nothing pushed");
            return Pushed::Ok;
        }
        let payload = otlp::encode(&points, subscription.delta, self.window_start, now);
        let Some((compression, payload)) = compress(&subscription.compression, payload) else {
            debug!(accepted = ?subscription.compression, "no usable telemetry compression; client telemetry stops");
            return Pushed::Stop;
        };
        if payload.len() > subscription.max_bytes {
            debug!(
                bytes = payload.len(),
                max_bytes = subscription.max_bytes,
                "telemetry payload exceeds the broker's limit; skipped"
            );
            return Pushed::Retry;
        }

        let request = PushTelemetryRequest {
            client_instance_id: subscription.instance_id,
            subscription_id: subscription.subscription_id,
            terminating,
            compression_type: compression as i8,
            metrics: payload,
        };
        let Some(conn) = (match self.connection().await {
            Ok(conn) => conn,
            Err(error) => {
                debug!(%error, "no connection for the telemetry push");
                return Pushed::Retry;
            }
        }) else {
            return Pushed::Stop;
        };
        let response = match conn
            .send_request(ApiKey::PushTelemetry, 0, |buf| request.encode_v0(buf))
            .await
            .and_then(|bytes: Bytes| {
                PushTelemetryResponse::decode_versioned(0, &mut bytes.as_ref())
            }) {
            Ok(response) => response,
            Err(error) => {
                debug!(%error, "telemetry push failed");
                self.connection = None;
                return Pushed::Retry;
            }
        };
        match response.error_code {
            ErrorCode::None => {
                self.pushed.extend(sums);
                self.window_start = now;
                Pushed::Ok
            }
            ErrorCode::UnknownSubscriptionId
            | ErrorCode::UnsupportedCompressionType
            | ErrorCode::TelemetryTooLarge => {
                debug!(error_code = ?response.error_code, "telemetry push refused; re-subscribing");
                Pushed::Resubscribe
            }
            ErrorCode::InvalidRequest
            | ErrorCode::InvalidRecord
            | ErrorCode::UnsupportedVersion => {
                debug!(error_code = ?response.error_code, "telemetry push rejected; client telemetry stops");
                Pushed::Stop
            }
            other => {
                debug!(error_code = ?other, "telemetry push failed");
                Pushed::Retry
            }
        }
    }
}

/// Compress `payload` with the first codec the broker accepts that this build
/// can encode. No list means uncompressed.
fn compress(accepted: &[Compression], payload: Vec<u8>) -> Option<(Compression, Bytes)> {
    if accepted.is_empty() {
        return Some((Compression::None, Bytes::from(payload)));
    }
    accepted.iter().find_map(|codec| match codec {
        Compression::None => Some((Compression::None, Bytes::from(payload.clone()))),
        codec if codec.is_available() => codec
            .compress_with_level(&payload, None)
            .ok()
            .map(|bytes| (*codec, bytes)),
        _ => None,
    })
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod unit_tests {
    use super::*;

    fn subscription(requested: &[&str]) -> Subscription {
        Subscription {
            instance_id: [1; 16],
            subscription_id: 1,
            push_interval: Duration::from_secs(1),
            delta: false,
            compression: vec![],
            max_bytes: usize::MAX,
            requested: requested.iter().map(|s| s.to_string()).collect(),
        }
    }

    #[test]
    fn a_dotted_prefix_selects_the_pushed_names_under_it() {
        let mut metrics = Metrics::default();
        metrics.producer.records_sent = 5;
        metrics.connections.connections_created = 1;
        let names: Vec<String> = points(&metrics, ClientType::Producer)
            .into_iter()
            .map(|p| p.name)
            .collect();
        let sub = subscription(&["org.apache.kafka.producer.record."]);
        let selected: Vec<&String> = names.iter().filter(|n| sub.wants(n)).collect();
        assert!(selected.contains(&&"org.apache.kafka.producer.record.send.total".to_string()));
        assert!(
            selected
                .iter()
                .all(|n| n.starts_with("org.apache.kafka.producer.record."))
        );
        assert!(names.iter().any(|n| !sub.wants(n)));
        assert!(names.iter().all(|n| subscription(&["*"]).wants(n)));
        assert!(names.iter().all(|n| !subscription(&[]).wants(n)));
    }

    #[test]
    fn a_consumer_pushes_no_producer_metric() {
        let names: Vec<String> = points(&Metrics::default(), ClientType::Consumer)
            .into_iter()
            .map(|p| p.name)
            .collect();
        assert!(
            names
                .iter()
                .all(|n| n.starts_with("org.apache.kafka.consumer."))
        );
        assert!(names.iter().any(|n| n.contains(".fetch.manager.")));
        assert!(!names.iter().any(|n| n.contains("record.send")));
    }

    #[test]
    fn compression_takes_the_first_usable_codec() {
        let (codec, _) = compress(&[Compression::Gzip, Compression::None], vec![1; 64]).unwrap();
        assert_eq!(codec, Compression::Gzip);
        let (codec, _) = compress(&[], vec![1; 64]).unwrap();
        assert_eq!(codec, Compression::None);
        if !Compression::Zstd.is_available() {
            assert!(compress(&[Compression::Zstd], vec![1; 64]).is_none());
        }
    }

    #[test]
    fn the_push_interval_is_clamped() {
        let response = |ms| GetTelemetrySubscriptionsResponse {
            throttle_time_ms: 0,
            error_code: ErrorCode::None,
            client_instance_id: [7; 16],
            subscription_id: 1,
            accepted_compression_types: vec![],
            push_interval_ms: ms,
            telemetry_max_bytes: 0,
            delta_temporality: true,
            requested_metrics: vec![],
        };
        let short = Subscription::from_response(response(1), [0; 16]);
        assert_eq!(short.push_interval, MIN_PUSH_INTERVAL);
        assert_eq!(short.instance_id, [7; 16]);
        assert_eq!(short.max_bytes, usize::MAX);
        let kept = Subscription::from_response(response(i32::MAX), [3; 16]);
        assert_eq!(kept.push_interval, MAX_PUSH_INTERVAL);
        assert_eq!(kept.instance_id, [3; 16]);
    }
}
