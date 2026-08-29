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
// A failing `expect` in a test *is* the failure report.
#![allow(clippy::expect_used, clippy::unwrap_used)]

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
/// `TransactionalProducerBuilder` was the one client that had only `build()`,
/// which made a `validate-config` subcommand impossible for exactly-once
/// deployments — while the README promised the property for every client.
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
    let _: fn() -> krafka::error::Result<krafka::producer::TransactionalProducerConfig> =
        || krafka::producer::TransactionalProducer::builder().build_config();
}

/// Every SASL mechanism must be constructible under **both** `SASL_PLAINTEXT`
/// and `SASL_SSL` from the public API alone.
///
/// # Why a matrix and not a list of constructors
///
/// `AuthConfig` grew one `_ssl` constructor per mechanism, and the set was
/// maintained by hand. `sasl_plain_ssl` and `sasl_oauthbearer_ssl` existed;
/// `sasl_scram_sha256_ssl` and `sasl_scram_sha512_ssl` did not. Since
/// `security_protocol` and `tls_config` are private and there was no
/// `with_tls`, `SASL_SSL` + SCRAM — the default secured listener on Redpanda
/// Cloud, Aiven, Instaclustr and most Strimzi installs — was **unreachable**
/// from outside the crate. `AuthConfig::from_env` produced it only because it
/// lives inside the crate and could assign the private fields directly.
///
/// The failure was silent: `sasl_scram_sha512(..)` returned a config that
/// looked right and then attempted a cleartext SASL handshake against a TLS
/// listener.
///
/// This walks the matrix instead of enumerating constructors, so a mechanism
/// added later cannot quietly miss a cell.
#[test]
fn every_sasl_mechanism_is_reachable_over_both_transports() {
    use krafka::auth::{AuthConfig, SaslMechanism, SecurityProtocol, TlsConfig};

    /// Every mechanism krafka can authenticate with, and how to build it over
    /// cleartext. TLS is applied uniformly below.
    fn cleartext_configs() -> Vec<(SaslMechanism, AuthConfig)> {
        vec![
            (
                SaslMechanism::Plain,
                AuthConfig::sasl_plain("user", "pass").expect("valid PLAIN credentials"),
            ),
            (
                SaslMechanism::ScramSha256,
                AuthConfig::sasl_scram_sha256("user", "pass"),
            ),
            (
                SaslMechanism::ScramSha512,
                AuthConfig::sasl_scram_sha512("user", "pass"),
            ),
            (
                SaslMechanism::OAuthBearer,
                AuthConfig::sasl_oauthbearer("jwt"),
            ),
            // AWS_MSK_IAM is TLS-only by construction — MSK exposes no
            // cleartext IAM listener — so its SASL_PLAINTEXT cell is
            // deliberately absent and it is asserted separately below.
        ]
    }

    for (mechanism, cleartext) in cleartext_configs() {
        assert_eq!(
            cleartext.security_protocol(),
            &SecurityProtocol::SaslPlaintext,
            "{mechanism} must be constructible over SASL_PLAINTEXT"
        );
        assert_eq!(cleartext.sasl_mechanism(), Some(&mechanism));
        assert!(cleartext.tls_config().is_none());

        let encrypted = cleartext.with_tls(TlsConfig::new().with_ca_cert("/etc/kafka/ca.pem"));
        assert_eq!(
            encrypted.security_protocol(),
            &SecurityProtocol::SaslSsl,
            "{mechanism} must be constructible over SASL_SSL"
        );
        assert_eq!(
            encrypted.sasl_mechanism(),
            Some(&mechanism),
            "{mechanism} must survive the TLS upgrade"
        );
        assert_eq!(
            encrypted.tls_config().and_then(|t| t.ca_cert_path()),
            Some("/etc/kafka/ca.pem"),
            "{mechanism} must carry the caller's TLS settings, not defaults"
        );
    }

    // AWS_MSK_IAM: TLS-only, and its TLS settings must still be replaceable —
    // the default `TlsConfig::new()` pins no CA and overrides no SNI.
    let msk = AuthConfig::aws_msk_iam("AKID", "secret", "us-east-1")
        .with_tls(TlsConfig::new().with_sni_hostname("b-1.msk.example.com"));
    assert_eq!(msk.security_protocol(), &SecurityProtocol::SaslSsl);
    assert_eq!(msk.sasl_mechanism(), Some(&SaslMechanism::AwsMskIam));
    assert_eq!(
        msk.tls_config().and_then(|t| t.sni_hostname()),
        Some("b-1.msk.example.com")
    );

    // The dedicated `_ssl` constructors must agree with the general form, so
    // the convenience shorthand cannot drift from `with_tls`.
    assert_eq!(
        AuthConfig::sasl_scram_sha256_ssl("u", "p", TlsConfig::new()).security_protocol(),
        AuthConfig::sasl_scram_sha256("u", "p")
            .with_tls(TlsConfig::new())
            .security_protocol()
    );
    assert_eq!(
        AuthConfig::sasl_scram_sha512_ssl("u", "p", TlsConfig::new()).security_protocol(),
        AuthConfig::sasl_scram_sha512("u", "p")
            .with_tls(TlsConfig::new())
            .security_protocol()
    );

    // TLS without SASL must remain reachable through the same method.
    assert_eq!(
        AuthConfig::plaintext()
            .with_tls(TlsConfig::new())
            .security_protocol(),
        &SecurityProtocol::Ssl
    );
}

/// The two producer builders must offer the same configuration surface.
///
/// `TransactionalProducerBuilder` was missing seventeen of
/// `ProducerBuilder`'s methods. Three of them mattered:
///
/// - `build_config()` — see the test above.
/// - `compression_level()` — the headline 0.15 tuning knob, unreachable for
///   the producer that always batches and therefore always compresses.
/// - `delivery_timeout()` — the bound on how long a batch may sit in flight,
///   which matters *more* here: a stuck batch holds an open transaction and
///   blocks every `read_committed` consumer behind it.
///
/// `acks` and `idempotent` are excluded on purpose — both are fixed by the
/// transactional protocol, and a setter for either would be a way to break the
/// guarantee. Nothing else is.
///
/// The `client_builders_share_one_configuration_surface` test below covers what
/// *all six* clients share; this covers what the producer family shares.
#[test]
fn both_producer_builders_share_one_configuration_surface() {
    use krafka::producer::{Producer, ProducerRecord, TransactionalProducer};
    use krafka::protocol::Compression;
    use std::sync::Arc;
    use std::time::Duration;

    #[derive(Debug)]
    struct NoopInterceptor;
    impl krafka::interceptor::ProducerInterceptor for NoopInterceptor {
        fn on_send(
            &self,
            _record: &mut ProducerRecord,
            _ctx: &mut krafka::interceptor::RecordContext,
        ) -> krafka::interceptor::InterceptorResult {
            Ok(())
        }
    }

    #[derive(Debug)]
    struct NoopDlq;
    impl krafka::dlq::DeadLetterQueue for NoopDlq {
        fn send<'a>(
            &'a self,
            _record: ProducerRecord,
            _error: String,
        ) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send + 'a>> {
            Box::pin(async {})
        }
    }

    struct NoopStore;
    impl krafka::producer::ProducerStateStore for NoopStore {
        async fn load(
            &self,
        ) -> krafka::error::Result<Option<krafka::producer::ProducerIdentitySnapshot>> {
            Ok(None)
        }
        async fn store(
            &self,
            _snapshot: &krafka::producer::ProducerIdentitySnapshot,
        ) -> krafka::error::Result<()> {
            Ok(())
        }
    }

    macro_rules! assert_producer_setters {
        ($make:expr) => {{
            let _ = |d: Duration| $make.linger(d);
            let _ = |n: usize| $make.batch_size(n);
            let _ = |n: usize| $make.buffer_memory(n);
            let _ = |d: Duration| $make.max_block(d);
            let _ = |n: usize| $make.max_request_size(n);
            let _ = |n: u32| $make.retries(n);
            let _ = |d: Duration| $make.retry_backoff(d);
            let _ = |d: Duration| $make.delivery_timeout(d);
            let _ = |c: Compression| $make.compression(c);
            let _ = |l: Option<i32>| $make.compression_level(l);
            let _ = |t: &str, c: Compression| $make.topic_compression(t, c);
            let _ = |d: Duration| $make.metadata_topic_cache_ttl(d);
            let _ = || $make.disable_metadata_topic_cache_ttl();
            let _ = |d: Duration| $make.metadata_recovery_rebootstrap_trigger(d);
            let _ = |q: Arc<NoopDlq>| $make.dead_letter_queue(q);
            let _ = |i: Arc<NoopInterceptor>| $make.interceptor(i);
            let _ = |i: Arc<NoopInterceptor>| $make.add_interceptor(i);
            let _ = || $make.state_store(NoopStore);
            let _ = |c: &krafka::client::KrafkaClient| $make.with_client(c);
            let _ = |p: krafka::producer::UniformStickyPartitioner| $make.partitioner(p);
            let _ = |e: Arc<dyn krafka::serdes::Serializer>| $make.key_serializer(e);
            let _ = |e: Arc<dyn krafka::serdes::Serializer>| $make.value_serializer(e);
            let _ = || {
                $make.sasl_oauthbearer_provider(|| async {
                    Ok(krafka::auth::OAuthBearerToken::new("jwt"))
                })
            };

            #[cfg(feature = "socks5")]
            let _ = |p: krafka::network::ProxyConfig| $make.proxy(p);
        }};
    }

    assert_producer_setters!(Producer::builder());
    assert_producer_setters!(TransactionalProducer::builder());
}

/// Both producers must offer the same operational surface.
///
/// `Producer::flush()` existed and `TransactionalProducer` had no equivalent,
/// so code generic over "a producer" — an enum dispatching over the two — had
/// to special-case the gap, with no way to tell from the docs whether an
/// explicit pre-commit flush was unnecessary or merely unavailable.
#[test]
fn both_producers_share_one_operational_surface() {
    macro_rules! assert_producer_ops {
        ($ty:ty) => {{
            async fn _flush(p: &$ty) -> krafka::error::Result<()> {
                p.flush().await
            }
            async fn _close(p: &$ty) {
                p.close().await;
            }
            async fn _close_timeout(p: &$ty, d: std::time::Duration) -> krafka::error::Result<()> {
                p.close_with_timeout(d).await
            }
            fn _metrics(p: &$ty) -> krafka::producer::ProducerMetricsSnapshot {
                p.metrics()
            }
        }};
    }

    assert_producer_ops!(krafka::producer::Producer);
    assert_producer_ops!(krafka::producer::TransactionalProducer);
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

    // Pool sharing, on every builder that is not itself the pool's owner.
    //
    // `ShareConsumerBuilder` had no `with_client` at all, so a share consumer
    // could not join a `KrafkaClient`'s pool and always opened its own
    // connections to every broker — the connection multiplication
    // `KrafkaClient` exists to prevent.
    macro_rules! assert_shares_a_client {
        ($make:expr) => {{
            let _ = |c: &krafka::client::KrafkaClient| $make.with_client(c);
        }};
    }
    assert_shares_a_client!(krafka::consumer::Consumer::builder());
    assert_shares_a_client!(krafka::producer::Producer::builder());
    assert_shares_a_client!(krafka::admin::AdminClient::builder());
    assert_shares_a_client!(krafka::producer::TransactionalProducer::builder());
    #[cfg(feature = "unstable-protocol")]
    assert_shares_a_client!(krafka::share_consumer::ShareConsumer::builder());

    assert_common_setters!(krafka::consumer::Consumer::builder());
    assert_common_setters!(krafka::producer::Producer::builder());
    assert_common_setters!(krafka::admin::AdminClient::builder());
    assert_common_setters!(krafka::producer::TransactionalProducer::builder());
    assert_common_setters!(krafka::client::KrafkaClient::builder("localhost:9092"));

    #[cfg(feature = "unstable-protocol")]
    assert_common_setters!(krafka::share_consumer::ShareConsumer::builder());
}

/// Both consuming clients must accept the same read-side hooks and express
/// their fetch tuning in the same units.
///
/// A `ShareConsumer` hands back the same `ConsumerRecord` as a `Consumer` and
/// speaks a fetch protocol with the same four tuning dimensions, so a reader
/// who has configured one should not discover that the other simply lacks the
/// setting. It did:
///
///   - `fetch_min_bytes`, `fetch_max_bytes`, `max_records` and `batch_size` —
///     KIP-932's fetch knobs — were declared on `ShareConsumerConfig` and read
///     when the `ShareFetch` request was built, with no builder setter. Every
///     krafka share consumer sent the same four numbers.
///   - `key_deserializer` / `value_deserializer` existed only on `Consumer`, so
///     a share-group application had to decode schema framing by hand.
///   - `fetch_max_wait_ms(i32)` took raw milliseconds where every other timeout
///     in the crate takes a `Duration`.
///
/// `xtask/config_reachability.py` now catches the first class automatically by
/// walking the config structs. This test pins the cross-client half, which is a
/// judgement about which settings *should* exist on both.
#[cfg(feature = "unstable-protocol")]
#[test]
fn both_consumers_share_the_read_side_surface() {
    use std::sync::Arc;
    use std::time::Duration;

    macro_rules! assert_consumer_read_surface {
        ($make:expr) => {{
            let _ = |d: Duration| $make.fetch_max_wait(d);
            let _ = |n: i32| $make.fetch_min_bytes(n);
            let _ = |n: i32| $make.fetch_max_bytes(n);
            let _ = |n: i32| $make.max_poll_records(n);
            let _ = |n: i32| $make.max_buffered_records(n);
            let _ = |d: Arc<dyn krafka::serdes::Deserializer>| $make.key_deserializer(d);
            let _ = |d: Arc<dyn krafka::serdes::Deserializer>| $make.value_deserializer(d);
        }};
    }
    assert_consumer_read_surface!(krafka::consumer::Consumer::builder());
    assert_consumer_read_surface!(krafka::share_consumer::ShareConsumer::builder());

    // Share-group-specific acquisition tuning, which the classic consumer has
    // no analogue for.
    let _ = |n: i32| krafka::share_consumer::ShareConsumer::builder().max_records(n);
    let _ = |n: i32| krafka::share_consumer::ShareConsumer::builder().batch_size(n);
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
            // Whether `close()` may tear down the connection pool.
            //
            // A pool borrowed from a `KrafkaClient` via `with_client` belongs
            // to that client. `AdminClient` knew this and left a shared pool
            // alone; `Producer`, `Consumer` and `TransactionalProducer` called
            // `close_all()` unconditionally, so closing one of them killed
            // every sibling's connections and failed their in-flight requests
            // — the whole point of sharing a client, undone by its shutdown.
            fn _owns_pool(c: &$ty) -> bool {
                c.owns_pool()
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
