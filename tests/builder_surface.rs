//! Compile-time guarantees about the public builder surface.
//!
//! # Why this is a test and not a script
//!
//! krafka used to police these invariants with `xtask/api_parity.py`, a Python
//! script that parsed `impl` blocks and compared method names. It existed
//! because krafka had **two builders per client** — a public `*Builder` and an
//! internal `*ConfigBuilder` — and options added to one and not the other were
//! implemented, documented and uncallable.
//!
//! That script found two real defects, then failed to find a third: the
//! producer's two builders had the same method *names* but different method
//! *bodies*, and only the unused one validated the compression codec. A parity
//! check compares surfaces; the divergence had moved underneath it.
//!
//! So the duplication was deleted, and with it the reason for a parity check.
//! What remains worth guaranteeing is *reachability*, and Rust can guarantee
//! that far better than a regex can: **every line below fails to compile if the
//! method it names disappears.** No parser, no `impl`-block matching, no
//! allowlist that drifts out of date.
//!
//! This is an integration test rather than a unit test on purpose. It links
//! against krafka as an external crate, so it proves the methods are reachable
//! *by a user* — which is the property that was violated, and which a
//! `#[cfg(test)]` module inside the crate could not have proven.
//!
//! The closures are never called. Type checking is the whole assertion.

#![allow(clippy::let_underscore_untyped)]

use krafka::network::TransportConfig;

/// Every client builder must accept a [`TransportConfig`].
///
/// `TransportConfig` is the single façade over the socket- and pool-level
/// settings — TCP keepalive, the response ceiling, the in-flight cap, idle
/// eviction, the file-descriptor cap, the TLS reload interval. Before it
/// existed, all eleven were documented in detail and reachable from no client
/// builder at all: each client built its `ConnectionConfig` from four fields
/// and took the defaults for everything else.
///
/// Nothing else would notice a client that quietly dropped this method, because
/// `ConnectionConfigBuilder` is a public API in its own right that no client
/// mirrors.
#[test]
fn every_client_builder_accepts_a_transport_config() {
    let _ = |t: TransportConfig| krafka::consumer::Consumer::builder().transport(t);
    let _ = |t: TransportConfig| krafka::producer::Producer::builder().transport(t);
    let _ = |t: TransportConfig| krafka::admin::AdminClient::builder().transport(t);
    let _ = |t: TransportConfig| krafka::producer::TransactionalProducer::builder().transport(t);
    let _ =
        |t: TransportConfig| krafka::client::KrafkaClient::builder("localhost:9092").transport(t);

    #[cfg(feature = "unstable-protocol")]
    let _ = |t: TransportConfig| krafka::share_consumer::ShareConsumer::builder().transport(t);
}

/// Client builders must offer a synchronous validation terminal alongside the
/// async one.
///
/// Two terminals over one validator is what lets a configuration be checked
/// without a broker — at startup, in a unit test, or in a config-linting tool.
/// A builder with only `build()` pushes its users back to needing a live
/// cluster to find out that, say, their compression codec was not compiled in.
///
/// The return types are named explicitly so this also pins down *what*
/// `build_config` hands back.
#[test]
fn client_builders_expose_a_synchronous_validation_terminal() {
    let _: fn() -> krafka::error::Result<krafka::consumer::ConsumerConfig> =
        || krafka::consumer::Consumer::builder().build_config();
    let _: fn() -> krafka::error::Result<krafka::producer::ProducerConfig> =
        || krafka::producer::Producer::builder().build_config();
    let _: fn() -> krafka::error::Result<krafka::admin::AdminConfig> =
        || krafka::admin::AdminClient::builder().build_config();
}

/// Certificate rotation must be reachable from every long-lived client
/// (KIP-1288).
///
/// `ConnectionPool::refresh_tls` existed, was correct, and was documented with
/// *"call this after rotating certificates on disk"* — while being callable
/// from no client at all. The only route was `KrafkaClient::pool()`, an
/// accessor whose own documentation says to prefer `with_client` over reaching
/// through it.
#[test]
fn tls_rotation_is_reachable_from_every_client() {
    async fn _consumer(c: &krafka::consumer::Consumer) -> krafka::error::Result<()> {
        c.refresh_tls().await
    }
    async fn _producer(p: &krafka::producer::Producer) -> krafka::error::Result<()> {
        p.refresh_tls().await
    }
    async fn _admin(a: &krafka::admin::AdminClient) -> krafka::error::Result<()> {
        a.refresh_tls().await
    }
    async fn _client(k: &krafka::client::KrafkaClient) -> krafka::error::Result<()> {
        k.refresh_tls().await
    }
}

/// Reading metrics must not require an async context.
///
/// A Prometheus scrape handler, a signal handler and a `Drop` impl are all
/// synchronous. `Producer::metrics()` was the one accessor in the crate
/// declared `async` — with no `await` inside it — so it alone could not be
/// called from any of them.
///
/// Binding the results to concrete types is the assertion: if any of these
/// became `async` again, the value would be a `Future` and this would not
/// compile.
#[test]
fn metrics_are_readable_without_an_async_context() {
    fn _producer(p: &krafka::producer::Producer) {
        let _snapshot: krafka::producer::ProducerMetricsSnapshot = p.metrics();
        let _conn: std::sync::Arc<krafka::metrics::ConnectionMetrics> = p.connection_metrics();
    }
    fn _consumer(c: &krafka::consumer::Consumer) {
        let _m: &std::sync::Arc<krafka::metrics::ConsumerMetrics> = c.metrics();
        let _conn: std::sync::Arc<krafka::metrics::ConnectionMetrics> = c.connection_metrics();
    }
    fn _admin(a: &krafka::admin::AdminClient) {
        let _conn: std::sync::Arc<krafka::metrics::ConnectionMetrics> = a.connection_metrics();
    }
}

/// API version negotiation must be synchronous.
///
/// It is a `parking_lot::Mutex` lookup against a table populated during the
/// handshake. Declaring it `async` forced 86 call sites to `.await` a lock
/// read, and made every function that negotiates a version async by
/// contagion.
#[test]
fn api_version_negotiation_is_synchronous() {
    fn _negotiate(conn: &krafka::network::BrokerConnection) {
        let _: Option<i16> = conn.negotiate_api_version(krafka::protocol::ApiKey::Fetch, 18, 4);
        let _: Option<i16> = conn.negotiate_api_version_max(krafka::protocol::ApiKey::Produce, 13);
        let _: Option<krafka::protocol::ApiVersionRange> =
            conn.get_api_version(krafka::protocol::ApiKey::Metadata);
    }
}

/// The six client builders must offer the same *configuration* surface.
///
/// A setter present on some builders and missing from others is the same defect
/// class as §3.1's unreachable transport settings, one level up. Three real
/// gaps were found this way:
///
/// - `ShareConsumerBuilder` had `request_timeout` but not `connect_timeout`,
///   so any timeout below the 10 s default was rejected at build time with an
///   error naming a value the builder could not change. That is a functional
///   bug, not an inconsistency.
/// - `TransactionalProducerBuilder` had no KIP-899 recovery configuration at
///   all — it silently took the default.
/// - `AdminClientBuilder` hard-coded `metadata_max_age` to 5 minutes at its one
///   construction site.
///
/// `bootstrap_servers` is deliberately absent from `KrafkaClientBuilder`: it is
/// a required argument to `KrafkaClient::builder(..)`, not an optional setter.
#[test]
fn client_builders_share_one_configuration_surface() {
    use krafka::metadata::MetadataRecoveryStrategy;
    use std::time::Duration;

    macro_rules! assert_common_setters {
        ($make:expr) => {{
            let _ = |v: &str| $make.client_id(v);
            let _ = |d: Duration| $make.request_timeout(d);
            let _ = |d: Duration| $make.connect_timeout(d);
            let _ = |d: Duration| $make.metadata_max_age(d);
            let _ = |s: MetadataRecoveryStrategy| $make.metadata_recovery_strategy(s);
            let _ = |t: TransportConfig| $make.transport(t);
            let _ = |a: krafka::auth::AuthConfig| $make.auth(a);
            let _ = |u: &str, p: &str| $make.sasl_plain(u, p);
            let _ = |u: &str, p: &str| $make.sasl_scram_sha256(u, p);
            let _ = |u: &str, p: &str| $make.sasl_scram_sha512(u, p);
            let _ = |t: &str| $make.sasl_oauthbearer(t);
        }};
    }

    assert_common_setters!(krafka::consumer::Consumer::builder());
    assert_common_setters!(krafka::producer::Producer::builder());
    assert_common_setters!(krafka::admin::AdminClient::builder());
    assert_common_setters!(krafka::producer::TransactionalProducer::builder());
    assert_common_setters!(krafka::client::KrafkaClient::builder("localhost:9092"));

    #[cfg(feature = "unstable-protocol")]
    assert_common_setters!(krafka::share_consumer::ShareConsumer::builder());
}

/// Every long-lived client must offer the same operational surface.
///
/// `TransactionalProducer` and `ShareConsumer` were missing `refresh_tls`,
/// `rebootstrap` and `update_seed_brokers` — the first being a capability added
/// to their three siblings in the same review that skipped them, even though
/// both own a pool and accept a `TransportConfig` carrying
/// `tls_reload_interval`.
///
/// `AdminClient` has no `metrics()` because there is no admin-specific metrics
/// type: admin operations are one-shot, and `connection_metrics()` covers the
/// transport. That is a deliberate absence, not an oversight.
#[test]
fn every_long_lived_client_shares_one_operational_surface() {
    macro_rules! assert_lifecycle {
        ($ty:ty) => {{
            async fn _refresh(c: &$ty) -> krafka::error::Result<()> {
                c.refresh_tls().await
            }
            async fn _rebootstrap(c: &$ty) {
                c.rebootstrap().await;
            }
            fn _seeds(c: &$ty, s: Vec<String>) -> krafka::error::Result<()> {
                c.update_seed_brokers(s)
            }
            fn _conn_metrics(c: &$ty) -> std::sync::Arc<krafka::metrics::ConnectionMetrics> {
                c.connection_metrics()
            }
            fn _closed(c: &$ty) -> bool {
                c.is_closed()
            }
        }};
    }

    assert_lifecycle!(krafka::producer::Producer);
    assert_lifecycle!(krafka::consumer::Consumer);
    assert_lifecycle!(krafka::admin::AdminClient);
    assert_lifecycle!(krafka::producer::TransactionalProducer);

    #[cfg(feature = "unstable-protocol")]
    assert_lifecycle!(krafka::share_consumer::ShareConsumer);

    // Interrupting a blocked poll must exist on every consuming client.
    //
    // `ShareConsumer::wakeup()` shipped with a documented contract while
    // `Consumer` had none, so the only way to interrupt a classic consumer's
    // poll from a shutdown handler was to drop the future — which only the
    // task owning it can do. That is the asymmetry class §3.1 and §9.5 keep
    // finding, one level out from configuration.
    fn _consumer_wakeup(c: &krafka::consumer::Consumer) {
        c.wakeup();
    }
    #[cfg(feature = "unstable-protocol")]
    fn _share_wakeup(c: &krafka::share_consumer::ShareConsumer) {
        c.wakeup();
    }

    // Application metrics, where a metrics type exists.
    fn _txn(p: &krafka::producer::TransactionalProducer) {
        let _: krafka::producer::ProducerMetricsSnapshot = p.metrics();
    }
    #[cfg(feature = "unstable-protocol")]
    fn _share(c: &krafka::share_consumer::ShareConsumer) {
        let _: std::sync::Arc<krafka::metrics::ConsumerMetrics> = c.metrics();
    }
}
