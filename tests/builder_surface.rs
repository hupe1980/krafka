//! Compile-time guarantees about the public API surface.
//!
//! Every line below fails to compile if the method it names disappears or
//! changes shape. This is an integration test so it links against krafka as
//! an external crate: it proves the methods are reachable *by a user*. The
//! closures and functions are never called; type checking is the assertion.

#![allow(clippy::expect_used, dead_code)]

use std::sync::Arc;
use std::time::Duration;

use krafka::admin::AdminClient;
use krafka::consumer::{Consumer, ConsumerRecord};
use krafka::producer::{Producer, TransactionalProducer};
use krafka::share_consumer::ShareConsumer;
use krafka::{CloseOptions, Kafka, KafkaBuilder, Record};

/// Every connection setting lives on [`KafkaBuilder`], and only there.
#[test]
fn every_connection_setting_is_on_the_kafka_builder() {
    use krafka::MetadataRecoveryStrategy;
    use krafka::auth::AuthConfig;

    let make = || Kafka::builder("localhost:9092");
    let _ = |v: &str| make().client_id(v);
    let _ = |a: AuthConfig| make().security(a);
    let _ = |d: Duration| make().request_timeout(d);
    let _ = |d: Duration| make().connect_timeout(d);
    let _ = |d: Duration| make().metadata_max_age(d);
    let _ = |s: MetadataRecoveryStrategy| make().metadata_recovery_strategy(s);
    let _ = |d: Duration| make().metadata_recovery_rebootstrap_trigger(d);
    let _ = |d: Option<Duration>| make().metadata_topic_cache_ttl(d);
    let _ = |b: bool| make().allow_auto_create_topics(b);
    let _ = |p: krafka::ProxyConfig| make().proxy(p);
    let _ = |b: bool| make().tcp_nodelay(b);
    let _ = |d: Option<Duration>| make().tcp_keepalive(d);
    let _ = |n: usize| make().max_response_size(n);
    let _ = |n: usize| make().max_in_flight_requests(n);
    let _ = |n: Option<usize>| make().socket_send_buffer(n);
    let _ = |n: Option<usize>| make().socket_receive_buffer(n);
    let _ = |d: Duration| make().connection_attempt_delay(d);
    let _ = |d: Option<Duration>| make().connections_max_idle(d);
    let _ = |n: Option<usize>| make().max_connections(n);
    let _ = |d: Option<Duration>| make().tls_reload_interval(d);
    async fn _connect(b: KafkaBuilder) -> krafka::Result<Kafka> {
        b.connect().await
    }
}

/// Every client comes from a [`Kafka`] handle; the handle owns the
/// pool-wide operations.
#[test]
fn every_client_is_built_from_the_handle() {
    async fn _clients(kafka: &Kafka) -> krafka::Result<()> {
        let _: Producer = kafka.producer().build().await?;
        let _: TransactionalProducer = kafka.producer().build_transactional("tx").await?;
        let _: Consumer = kafka.consumer("group").build().await?;
        let _: Consumer = kafka.consumer_without_group().build().await?;
        let _: ShareConsumer = kafka.share_consumer("group").build().await?;
        let _: AdminClient = kafka.admin();
        kafka.refresh_tls().await?;
        kafka.rebootstrap().await;
        kafka.update_seed_brokers(vec!["b:9092".into()])?;
        let _cheap: Kafka = kafka.clone();
        Ok(())
    }
}

/// Producer role settings, including the transaction settings that apply to
/// `build_transactional`.
#[test]
fn the_producer_builder_carries_producer_settings() {
    use krafka::Compression;
    use krafka::producer::{Acks, RoundRobinPartitioner};

    #[derive(Debug)]
    struct Noop;
    impl krafka::interceptor::ProducerInterceptor for Noop {}

    fn _setters(kafka: &Kafka) {
        let make = || kafka.producer();
        let _ = |a: Acks| make().acks(a);
        let _ = |d: Duration| make().linger(d);
        let _ = |n: usize| make().batch_size(n);
        let _ = |n: usize| make().buffer_memory(n);
        let _ = |d: Duration| make().max_block(d);
        let _ = |n: usize| make().max_request_size(n);
        let _ = |d: Duration| make().retry_backoff(d);
        let _ = |d: Duration| make().delivery_timeout(d);
        let _ = |c: Compression| make().compression(c);
        let _ = |l: Option<i32>| make().compression_level(l);
        let _ = |t: &str, c: Compression| make().topic_compression(t, c);
        let _ = |b: bool| make().idempotent(b);
        let _ = || make().interceptor(Noop);
        let _ = |i: Arc<Noop>| make().interceptor(i);
        let _ = |p: RoundRobinPartitioner| make().partitioner(p);
        let _ = |r: &str| make().client_rack(r);
        let _ = |b: bool| make().partitioner_rack_aware(b);
        let _ = |d: Duration| make().transaction_timeout(d);
        let _ = |b: bool| make().two_phase_commit(b);
    }
}

/// One send that waits, one that pipelines, on both producers.
#[test]
fn both_producers_send_records() {
    use krafka::producer::{DeliveryHandle, RecordMetadata};

    async fn _producer(p: &Producer) -> krafka::Result<()> {
        let _: RecordMetadata = p.send(Record::new("t", "v").key("k")).await?;
        let _: DeliveryHandle = p.enqueue(Record::new("t", "v")).await?;
        p.flush().await?;
        Ok(())
    }
    async fn _transactional(p: &TransactionalProducer) -> krafka::Result<()> {
        p.begin()?;
        let _: RecordMetadata = p.send(Record::new("t", "v")).await?;
        let _: DeliveryHandle = p.enqueue(Record::new("t", "v")).await?;
        p.flush().await?;
        p.commit().await?;
        p.abort().await?;
        Ok(())
    }
}

/// A produced record and a consumed one carry the same header type.
#[test]
fn produce_and_consume_share_one_header_type() {
    fn _same(record: &Record, consumed: &ConsumerRecord) -> bool {
        let produced: &krafka::Headers = &record.headers;
        let received: &krafka::Headers = &consumed.headers;
        produced == received
    }
    let _: krafka::TimestampType = krafka::TimestampType::CreateTime;
}

/// Consumer role settings.
#[test]
fn the_consumer_builder_carries_consumer_settings() {
    use krafka::consumer::{AutoOffsetReset, IsolationLevel, PartitionAssignmentStrategy};

    #[derive(Debug)]
    struct Noop;
    impl krafka::interceptor::ConsumerInterceptor for Noop {}

    struct Identity;
    impl krafka::serdes::Deserializer for Identity {
        fn deserialize(
            &self,
            _topic: &str,
            _headers: &krafka::Headers,
            payload: bytes::Bytes,
            _is_key: bool,
        ) -> krafka::Result<bytes::Bytes> {
            Ok(payload)
        }
    }

    fn _setters(kafka: &Kafka) {
        let make = || kafka.consumer("g");
        let _ = |r: AutoOffsetReset| make().auto_offset_reset(r);
        let _ = |b: bool| make().enable_auto_commit(b);
        let _ = |d: Duration| make().auto_commit_interval(d);
        let _ = |d: Duration| make().fetch_max_wait(d);
        let _ = |n: i32| make().fetch_min_bytes(n);
        let _ = |n: i32| make().fetch_max_bytes(n);
        let _ = |n: i32| make().max_poll_records(n);
        let _ = |n: i32| make().max_buffered_records(n);
        let _ = |d: Duration| make().max_poll_interval(d);
        let _ = |d: Duration| make().session_timeout(d);
        let _ = |d: Duration| make().heartbeat_interval(d);
        let _ = |l: IsolationLevel| make().isolation_level(l);
        let _ = |s: PartitionAssignmentStrategy| make().partition_assignment_strategy(s);
        let _ = |i: &str| make().group_instance_id(i);
        let _ = |r: &str| make().client_rack(r);
        let _ = |a: &str| make().group_remote_assignor(a);
        let _ = || make().interceptor(Noop);
        let _ = || make().key_deserializer(Identity);
        let _ = |d: Arc<dyn krafka::serdes::Deserializer>| make().value_deserializer(d);
    }
}

/// Share-consumer role settings.
#[test]
fn the_share_consumer_builder_carries_share_settings() {
    use krafka::share_consumer::{AcknowledgementMode, AcquireMode};

    fn _setters(kafka: &Kafka) {
        let make = || kafka.share_consumer("g");
        let _ = |m: AcknowledgementMode| make().acknowledgement_mode(m);
        let _ = |m: AcquireMode| make().acquire_mode(m);
        let _ = |d: Duration| make().fetch_max_wait(d);
        let _ = |n: i32| make().fetch_min_bytes(n);
        let _ = |n: i32| make().fetch_max_bytes(n);
        let _ = |n: i32| make().max_poll_records(n);
        let _ = |n: i32| make().batch_size(n);
        let _ = |d: Arc<dyn krafka::serdes::Deserializer>| make().key_deserializer(d);
    }
}

/// Both consumers receive the same way: `Ok(None)` once closed.
#[test]
fn both_consumers_receive_the_same_way() {
    async fn _consumer(c: &Consumer) -> krafka::Result<()> {
        let topics = vec![String::from("a")];
        c.subscribe(["a", "b"]).await?;
        c.subscribe(vec![String::from("a")]).await?;
        c.subscribe(&topics).await?;
        while let Some(record) = c.recv().await? {
            let _: ConsumerRecord = record;
        }
        let _: Vec<ConsumerRecord> = c.poll(Duration::from_millis(500)).await?;
        c.commit().await?;
        let _ = c.lag().await;
        Ok(())
    }
    async fn _share(c: &ShareConsumer) -> krafka::Result<()> {
        c.subscribe(["jobs"]).await?;
        while let Some(record) = c.recv().await? {
            c.ack(&record)?;
        }
        let _ = c.commit().await?;
        Ok(())
    }
}

/// Every client closes the same way.
#[test]
fn every_client_closes_the_same_way() {
    macro_rules! assert_close {
        ($ty:ty) => {{
            async fn _close(c: &$ty) -> krafka::Result<()> {
                c.close().await
            }
            fn _closed(c: &$ty) -> bool {
                c.is_closed()
            }
        }};
    }
    assert_close!(Producer);
    assert_close!(TransactionalProducer);
    assert_close!(Consumer);
    assert_close!(ShareConsumer);
    assert_close!(AdminClient);

    macro_rules! assert_close_with {
        ($ty:ty) => {{
            async fn _close_with(c: &$ty) -> krafka::Result<()> {
                c.close_with(CloseOptions::new().timeout(Duration::from_secs(5)))
                    .await
            }
        }};
    }
    assert_close_with!(Producer);
    assert_close_with!(TransactionalProducer);
    assert_close_with!(Consumer);
    assert_close_with!(ShareConsumer);
}

/// Every client and the `Kafka` handle return the same owned `Metrics`
/// snapshot, without an async context.
#[test]
fn metrics_are_one_snapshot_type_readable_without_an_async_context() {
    use krafka::metrics::Metrics;
    fn _kafka(k: &krafka::Kafka) -> Metrics {
        k.metrics()
    }
    fn _producer(p: &Producer) -> Metrics {
        p.metrics()
    }
    fn _transactional(p: &TransactionalProducer) -> Metrics {
        p.metrics()
    }
    fn _consumer(c: &Consumer) -> Metrics {
        c.metrics()
    }
    fn _share(c: &ShareConsumer) -> Metrics {
        c.metrics()
    }
    fn _admin(a: &AdminClient) -> Metrics {
        a.metrics()
    }
    fn _render(m: &Metrics) -> String {
        m.prometheus_text()
    }
}

/// One KIP-714 switch with the same name on every role builder, and the
/// client instance id on every client.
#[test]
fn every_client_has_the_telemetry_switch_and_instance_id() {
    use krafka::metrics::ClientInstanceId;
    use std::time::Duration;
    fn _switches(kafka: &krafka::Kafka) {
        let _ = kafka.producer().metrics_push(false);
        let _ = kafka.consumer("g").metrics_push(false);
        let _ = kafka.share_consumer("g").metrics_push(false);
        let _ = kafka.admin().metrics_push(true);
    }
    async fn _ids(
        p: &Producer,
        t: &TransactionalProducer,
        c: &Consumer,
        s: &ShareConsumer,
        a: &AdminClient,
    ) -> krafka::Result<Vec<Option<ClientInstanceId>>> {
        let d = Duration::from_secs(1);
        Ok(vec![
            p.client_instance_id(d).await?,
            t.client_instance_id(d).await?,
            c.client_instance_id(d).await?,
            s.client_instance_id(d).await?,
            a.client_instance_id(d).await?,
        ])
    }
}

/// A credential provider is an `async fn` or a closure, for every credential
/// type.
#[test]
fn credential_providers_are_async_fns_or_closures() {
    use krafka::auth::{AuthConfig, CredentialProvider, OAuthBearerToken};

    struct Vault;
    impl CredentialProvider<OAuthBearerToken> for Vault {
        async fn credentials(&self) -> krafka::Result<OAuthBearerToken> {
            Ok(OAuthBearerToken::new("t"))
        }
    }

    let _ = AuthConfig::sasl_oauthbearer_provider(Vault);
    let _ = AuthConfig::sasl_oauthbearer_provider(|| async { Ok(OAuthBearerToken::new("t")) });
    let _ = AuthConfig::aws_msk_iam_provider(|| async {
        Ok(krafka::auth::AwsMskIamCredentials::new(
            "AKID",
            "secret",
            "us-east-1",
        ))
    });
}

/// Every SASL mechanism must be constructible under both `SASL_PLAINTEXT`
/// and `SASL_SSL` from the public API alone.
#[test]
fn every_sasl_mechanism_is_reachable_over_both_transports() {
    use krafka::auth::{AuthConfig, SaslMechanism, SecurityProtocol, TlsConfig};

    let cleartext = vec![
        (SaslMechanism::Plain, AuthConfig::sasl_plain("user", "pass")),
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
    ];

    for (mechanism, cleartext) in cleartext {
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
        assert_eq!(encrypted.sasl_mechanism(), Some(&mechanism));
        assert_eq!(
            encrypted.tls_config().and_then(|t| t.ca_cert_path()),
            Some("/etc/kafka/ca.pem"),
            "{mechanism} must carry the caller's TLS settings, not defaults"
        );
    }

    // AWS_MSK_IAM is TLS-only, and its TLS settings stay replaceable.
    let msk = AuthConfig::aws_msk_iam("AKID", "secret", "us-east-1")
        .with_tls(TlsConfig::new().with_sni_hostname("b-1.msk.example.com"));
    assert_eq!(msk.security_protocol(), &SecurityProtocol::SaslSsl);
    assert_eq!(msk.sasl_mechanism(), Some(&SaslMechanism::AwsMskIam));
    assert_eq!(
        msk.tls_config().and_then(|t| t.sni_hostname()),
        Some("b-1.msk.example.com")
    );

    // TLS without SASL goes through the same method.
    assert_eq!(
        AuthConfig::plaintext()
            .with_tls(TlsConfig::new())
            .security_protocol(),
        &SecurityProtocol::Ssl
    );
}
