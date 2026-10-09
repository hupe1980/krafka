//! Integration tests against a real Apache Kafka broker in Docker.
//!
//! One broker container serves every test in this binary; each test uses its
//! own topics and groups. The tests are ignored by default:
//!
//! ```sh
//! just integration                         # apache/kafka-native:3.9.0
//! KAFKA_IMAGE=apache/kafka KAFKA_VERSION=4.3.0 just integration
//! ```

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::borrow::Cow;
use std::collections::HashMap;
use std::sync::OnceLock;
use std::time::Duration;

use testcontainers::core::{ContainerPort, ContainerState, ExecCommand, WaitFor};
use testcontainers::{Image, ImageExt, runners::AsyncRunner};

/// Time to wait after topic creation for metadata propagation.
const TOPIC_READY: Duration = Duration::from_secs(2);

// ---------------------------------------------------------------------------
// Custom Kafka image – works with `apache/kafka-native` and `apache/kafka` 3.8 – 4.x
// ---------------------------------------------------------------------------

const KAFKA_PORT: ContainerPort = ContainerPort::Tcp(9092);
const START_SCRIPT: &str = "/tmp/testcontainers_start.sh";

/// Minimal [`Image`] for `apache/kafka-native` (or `apache/kafka`) that follows
/// the same start-script pattern as Java testcontainers.
///
/// 1. The container command loops until `START_SCRIPT` exists.
/// 2. `exec_after_start` writes that script — after the host port is known —
///    exporting `KAFKA_ADVERTISED_LISTENERS` and calling `/etc/kafka/docker/run`.
/// 3. Wait condition: "Kafka Server started" appears in container logs.
#[derive(Debug, Clone)]
struct ApacheKafka {
    image: String,
    tag: String,
    env_vars: HashMap<String, String>,
}

impl ApacheKafka {
    fn new(image: impl Into<String>, tag: impl Into<String>) -> Self {
        let image = image.into();
        let tag = tag.into();
        let mut env_vars = HashMap::new();

        env_vars.insert("CLUSTER_ID".into(), "5L6g3nShT-eMCtK--X86sw".into());
        env_vars.insert("KAFKA_NODE_ID".into(), "1".into());
        env_vars.insert("KAFKA_PROCESS_ROLES".into(), "broker,controller".into());
        env_vars.insert(
            "KAFKA_LISTENERS".into(),
            format!(
                "PLAINTEXT://0.0.0.0:{},BROKER://0.0.0.0:9093,CONTROLLER://0.0.0.0:9094",
                KAFKA_PORT.as_u16()
            ),
        );
        env_vars.insert(
            "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP".into(),
            "BROKER:PLAINTEXT,PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT".into(),
        );
        env_vars.insert("KAFKA_INTER_BROKER_LISTENER_NAME".into(), "BROKER".into());
        env_vars.insert(
            "KAFKA_CONTROLLER_LISTENER_NAMES".into(),
            "CONTROLLER".into(),
        );
        env_vars.insert(
            "KAFKA_CONTROLLER_QUORUM_VOTERS".into(),
            "1@localhost:9094".into(),
        );
        env_vars.insert("KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR".into(), "1".into());
        env_vars.insert("KAFKA_OFFSETS_TOPIC_NUM_PARTITIONS".into(), "1".into());
        env_vars.insert(
            "KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR".into(),
            "1".into(),
        );
        env_vars.insert("KAFKA_TRANSACTION_STATE_LOG_MIN_ISR".into(), "1".into());
        env_vars.insert("KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS".into(), "0".into());
        env_vars.insert(
            "KAFKA_LOG_FLUSH_INTERVAL_MESSAGES".into(),
            i64::MAX.to_string(),
        );

        Self {
            image,
            tag,
            env_vars,
        }
    }
}

impl Image for ApacheKafka {
    fn name(&self) -> &str {
        &self.image
    }

    fn tag(&self) -> &str {
        &self.tag
    }

    fn ready_conditions(&self) -> Vec<WaitFor> {
        // The entrypoint waits for START_SCRIPT; readiness is checked
        // via `exec_after_start` container-level conditions instead.
        vec![]
    }

    fn entrypoint(&self) -> Option<&str> {
        Some("bash")
    }

    fn cmd(&self) -> impl IntoIterator<Item = impl Into<Cow<'_, str>>> {
        vec![
            "-c".to_string(),
            format!(
                "while [ ! -f {START_SCRIPT} ]; do sleep 0.1; done; \
                 chmod 755 {START_SCRIPT} && {START_SCRIPT}"
            ),
        ]
    }

    fn env_vars(
        &self,
    ) -> impl IntoIterator<Item = (impl Into<Cow<'_, str>>, impl Into<Cow<'_, str>>)> {
        &self.env_vars
    }

    fn expose_ports(&self) -> &[ContainerPort] {
        &[KAFKA_PORT]
    }

    fn exec_after_start(
        &self,
        cs: ContainerState,
    ) -> Result<Vec<ExecCommand>, testcontainers::TestcontainersError> {
        let host_port = cs.host_port_ipv4(KAFKA_PORT)?;
        let script = format!(
            "#!/usr/bin/env bash\n\
             export KAFKA_ADVERTISED_LISTENERS=\
             PLAINTEXT://127.0.0.1:{host_port},BROKER://localhost:9093,CONTROLLER://localhost:9094\n\
             /etc/kafka/docker/run\n"
        );
        let cmd = vec![
            "sh".to_string(),
            "-c".to_string(),
            format!("echo '{script}' > {START_SCRIPT}"),
        ];
        // Both older (3.8) and newer (3.9+/4.x) images eventually log this.
        let ready = vec![WaitFor::message_on_stdout("Kafka Server started")];
        Ok(vec![
            ExecCommand::new(cmd).with_container_ready_conditions(ready),
        ])
    }
}

static KAFKA: OnceLock<Result<String, String>> = OnceLock::new();

/// The bootstrap address of the broker shared by every test in this binary.
///
/// Image name is read from `KAFKA_IMAGE` (default: `apache/kafka-native`),
/// tag from `KAFKA_VERSION` (default: `3.9.0`). The container is started
/// once, on a thread whose runtime keeps it alive for the life of the
/// process; `just integration` removes it afterwards by its label. It is ready
/// when the broker logs "Kafka Server started".
fn kafka() -> String {
    KAFKA
        .get_or_init(|| {
            let (tx, rx) = std::sync::mpsc::channel();
            std::thread::spawn(move || {
                let rt = tokio::runtime::Builder::new_multi_thread()
                    .enable_all()
                    .build()
                    .expect("runtime for the shared broker");
                rt.block_on(async move {
                    let image = std::env::var("KAFKA_IMAGE")
                        .unwrap_or_else(|_| "apache/kafka-native".to_string());
                    let tag =
                        std::env::var("KAFKA_VERSION").unwrap_or_else(|_| "3.9.0".to_string());
                    let started = ApacheKafka::new(&image, &tag)
                        .with_labels([("krafka.test-suite", "integration_tests")])
                        .with_startup_timeout(Duration::from_secs(180))
                        .start()
                        .await;
                    match started {
                        Ok(container) => {
                            let port = container.get_host_port_ipv4(KAFKA_PORT).await;
                            tx.send(
                                port.map(|p| format!("127.0.0.1:{p}"))
                                    .map_err(|e| e.to_string()),
                            )
                            .ok();
                            std::future::pending::<()>().await;
                            drop(container);
                        }
                        Err(e) => {
                            tx.send(Err(format!("{image}:{tag}: {e}"))).ok();
                        }
                    }
                });
            });
            rx.recv()
                .unwrap_or_else(|_| Err("the broker thread exited".into()))
        })
        .clone()
        .unwrap_or_else(|e| panic!("Kafka did not start: {e}"))
}

/// Helper to poll for records with retry.
///
/// The first poll after subscribe often yields 0 records because the
/// JoinGroup/SyncGroup rebalance consumes the whole poll timeout. This helper
/// retries until at least `min_records` are collected or `max_attempts` polls
/// have been made.
async fn poll_for_records(
    consumer: &krafka::consumer::Consumer,
    min_records: usize,
    poll_timeout: Duration,
    max_attempts: usize,
) -> Vec<krafka::consumer::ConsumerRecord> {
    let mut all = Vec::new();
    for attempt in 0..max_attempts {
        let records = consumer
            .poll(poll_timeout)
            .await
            .expect("poll failed in poll_for_records");
        if records.is_empty() {
            eprintln!(
                "[poll_for_records] attempt {}/{}: 0 records (total {})",
                attempt + 1,
                max_attempts,
                all.len()
            );
        }
        all.extend(records);
        if all.len() >= min_records {
            break;
        }
    }
    all
}

/// Helper to create a topic using the admin client.
async fn create_topic(bootstrap_servers: &str, topic: &str, partitions: i32) {
    create_topic_with_configs(bootstrap_servers, topic, partitions, &[]).await;
}

/// Helper to create a topic with configuration overrides, e.g.
/// `cleanup.policy=compact` for a topic that tombstones are meaningful on.
async fn create_topic_with_configs(
    bootstrap_servers: &str,
    topic: &str,
    partitions: i32,
    configs: &[(&str, &str)],
) {
    use krafka::admin::NewTopic;

    let admin = krafka::Kafka::builder(bootstrap_servers)
        .connect()
        .await
        .expect("Failed to create admin client")
        .admin();

    let mut new_topic = NewTopic::new(topic, partitions, 1).unwrap();
    for (key, value) in configs {
        new_topic = new_topic.with_config(*key, *value);
    }

    admin
        .create_topics(vec![new_topic], Default::default())
        .await
        .expect("Failed to create topic");

    // Wait for topic to be ready
    tokio::time::sleep(TOPIC_READY).await;
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn test_producer_send_receive() {
    use krafka::consumer::AutoOffsetReset;

    let bootstrap_servers = kafka();

    // Create topic first
    let topic = "test-topic";
    create_topic(&bootstrap_servers, topic, 1).await;

    // Create producer
    let producer = krafka::Kafka::builder(&bootstrap_servers)
        .client_id("test-producer")
        .connect()
        .await
        .expect("Failed to create producer")
        .producer()
        .build()
        .await
        .expect("Failed to create producer");

    let metadata = producer
        .send(krafka::Record::new(topic, "test-value").key("test-key"))
        .await
        .expect("Failed to send message");

    assert!(metadata.offset >= 0);

    // Create consumer
    let consumer = krafka::Kafka::builder(&bootstrap_servers)
        .connect()
        .await
        .expect("Failed to create consumer")
        .consumer("test-group")
        .auto_offset_reset(AutoOffsetReset::Earliest)
        .build()
        .await
        .expect("Failed to create consumer");

    consumer
        .subscribe(&[topic])
        .await
        .expect("Failed to subscribe");

    // Poll for messages (first poll may be consumed by rebalance)
    let records = poll_for_records(&consumer, 1, Duration::from_secs(5), 5).await;

    assert!(!records.is_empty(), "Expected at least one record");

    let record = &records[0];
    assert_eq!(&*record.topic, topic);
    assert_eq!(record.key_str(), Some("test-key"));
    assert_eq!(record.value_str(), Some("test-value"));

    consumer.close().await.expect("consumer close");
    producer.close().await.unwrap();
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn test_admin_client() {
    use krafka::admin::NewTopic;

    let bootstrap_servers = kafka();

    let admin = krafka::Kafka::builder(&bootstrap_servers)
        .client_id("test-admin")
        .connect()
        .await
        .expect("Failed to create admin client")
        .admin();

    // Create a topic
    let topic_name = "admin-test-topic";
    let new_topic = NewTopic::new(topic_name, 3, 1).unwrap();

    admin
        .create_topics(vec![new_topic], Default::default())
        .await
        .expect("Failed to create topic");

    // Wait for topic to be created
    tokio::time::sleep(Duration::from_secs(1)).await;

    // List topics
    let topics = admin
        .list_topics(Default::default())
        .await
        .expect("Failed to list topics");
    assert!(
        topics.iter().any(|t| t == topic_name),
        "Topic not found in list"
    );

    // Describe cluster
    let cluster = admin
        .describe_cluster(Default::default())
        .await
        .expect("Failed to describe cluster");
    assert!(!cluster.brokers.is_empty(), "No brokers found");

    // Delete topic
    admin
        .delete_topics(
            vec![topic_name.to_string()],
            krafka::admin::DeleteTopicsOptions::default(),
        )
        .await
        .expect("Failed to delete topic");
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn test_compression_roundtrip() {
    use krafka::consumer::AutoOffsetReset;

    use krafka::Compression;

    let bootstrap_servers = kafka();

    for compression in [
        Compression::None,
        Compression::Gzip,
        Compression::Snappy,
        Compression::Lz4,
        // Zstd is not supported by the apache/kafka-native GraalVM image.
    ] {
        let topic = format!("compression-test-{:?}", compression).to_lowercase();
        create_topic(&bootstrap_servers, &topic, 1).await;
        let value = format!("test-value-for-{:?}", compression);

        // Create producer with compression
        let producer = krafka::Kafka::builder(&bootstrap_servers)
            .client_id("compression-test-producer")
            .connect()
            .await
            .expect("Failed to create producer")
            .producer()
            .compression(compression)
            .build()
            .await
            .expect("Failed to create producer");

        let metadata = producer
            .send(krafka::Record::new(
                &topic,
                bytes::Bytes::copy_from_slice(value.as_bytes()),
            ))
            .await
            .expect("Failed to send message");

        assert!(metadata.offset >= 0, "Expected valid offset");

        producer.close().await.unwrap();

        // Create consumer
        let consumer = krafka::Kafka::builder(&bootstrap_servers)
            .connect()
            .await
            .expect("Failed to create consumer")
            .consumer(format!("compression-test-group-{:?}", compression).to_lowercase())
            .auto_offset_reset(AutoOffsetReset::Earliest)
            .build()
            .await
            .expect("Failed to create consumer");

        consumer
            .subscribe(&[&topic])
            .await
            .expect("Failed to subscribe");

        let records = poll_for_records(&consumer, 1, Duration::from_secs(5), 5).await;

        assert!(
            !records.is_empty(),
            "Expected at least one record for {:?}",
            compression
        );
        assert_eq!(
            records[0].value_str(),
            Some(value.as_str()),
            "Value mismatch for {:?}",
            compression
        );
        consumer.close().await.expect("consumer close");
    }
}

/// A topic with `compression.type=snappy` makes the broker recompress an
/// uncompressed produce with snappy-java, which writes xerial framing. The
/// consumer must decode it.
#[tokio::test]
#[ignore = "requires Docker"]
async fn test_broker_recompressed_snappy_is_consumed() {
    use krafka::consumer::AutoOffsetReset;

    use krafka::Compression;

    let bootstrap_servers = kafka();
    let topic = "broker-snappy-topic";
    create_topic_with_configs(
        &bootstrap_servers,
        topic,
        1,
        &[("compression.type", "snappy")],
    )
    .await;

    let producer = krafka::Kafka::builder(&bootstrap_servers)
        .connect()
        .await
        .expect("Failed to create producer")
        .producer()
        .compression(Compression::None)
        .build()
        .await
        .expect("Failed to create producer");
    let values: Vec<String> = (0..50)
        .map(|i| format!("recompressed-{i}-{}", "x".repeat(200)))
        .collect();
    for value in &values {
        let _ = producer
            .send(krafka::Record::new(
                topic,
                bytes::Bytes::copy_from_slice(value.as_bytes()),
            ))
            .await
            .expect("Failed to send message");
    }
    producer.close().await.unwrap();

    let consumer = krafka::Kafka::builder(&bootstrap_servers)
        .connect()
        .await
        .expect("Failed to create consumer")
        .consumer("broker-snappy-group")
        .auto_offset_reset(AutoOffsetReset::Earliest)
        .build()
        .await
        .expect("Failed to create consumer");
    consumer
        .subscribe(&[topic])
        .await
        .expect("Failed to subscribe");
    let records = poll_for_records(&consumer, values.len(), Duration::from_secs(5), 10).await;
    let received: Vec<&str> = records.iter().filter_map(|r| r.value_str()).collect();
    assert_eq!(
        received,
        values.iter().map(String::as_str).collect::<Vec<_>>()
    );
    consumer.close().await.expect("consumer close");
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn test_multiple_partitions() {
    use krafka::admin::NewTopic;

    let bootstrap_servers = kafka();

    // Create topic with multiple partitions
    let admin = krafka::Kafka::builder(&bootstrap_servers)
        .connect()
        .await
        .expect("Failed to create admin client")
        .admin();

    let topic_name = "multi-partition-topic";
    let new_topic = NewTopic::new(topic_name, 6, 1).unwrap();

    admin
        .create_topics(vec![new_topic], Default::default())
        .await
        .expect("Failed to create topic");

    tokio::time::sleep(Duration::from_secs(1)).await;

    // Create producer
    let producer = krafka::Kafka::builder(&bootstrap_servers)
        .connect()
        .await
        .expect("Failed to create producer")
        .producer()
        .build()
        .await
        .expect("Failed to create producer");

    // Send messages with different keys
    let mut partition_set = std::collections::HashSet::new();
    for i in 0..100 {
        let key = format!("key-{}", i);
        let metadata = producer
            .send(
                krafka::Record::new(topic_name, "value")
                    .key(bytes::Bytes::copy_from_slice(key.as_bytes())),
            )
            .await
            .expect("Failed to send message");
        partition_set.insert(metadata.partition);
    }

    // With 100 different keys across 6 partitions, we should hit multiple partitions
    assert!(
        partition_set.len() > 1,
        "Expected messages to be sent to multiple partitions, got {:?}",
        partition_set
    );

    producer.close().await.unwrap();
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn test_consumer_group_rebalance() {
    use krafka::admin::NewTopic;
    use krafka::consumer::AutoOffsetReset;

    let bootstrap_servers = kafka();

    let topic_name = "consumer-group-test";
    let group_id = "test-consumer-group";

    // Create topic with 4 partitions
    let admin = krafka::Kafka::builder(&bootstrap_servers)
        .connect()
        .await
        .expect("Failed to create admin client")
        .admin();

    let new_topic = NewTopic::new(topic_name, 4, 1).unwrap();
    admin
        .create_topics(vec![new_topic], Default::default())
        .await
        .expect("Failed to create topic");

    tokio::time::sleep(Duration::from_secs(1)).await;

    // Produce some messages
    let producer = krafka::Kafka::builder(&bootstrap_servers)
        .connect()
        .await
        .expect("Failed to create producer")
        .producer()
        .build()
        .await
        .expect("Failed to create producer");

    for i in 0..20 {
        let key = format!("key-{}", i);
        let _ = producer
            .send(
                krafka::Record::new(
                    topic_name,
                    bytes::Bytes::copy_from_slice(format!("value-{}", i).as_bytes()),
                )
                .key(bytes::Bytes::copy_from_slice(key.as_bytes())),
            )
            .await
            .expect("Failed to send message");
    }
    producer.close().await.unwrap();

    // Create first consumer
    let consumer1 = krafka::Kafka::builder(&bootstrap_servers)
        .connect()
        .await
        .expect("Failed to create consumer1")
        .consumer(group_id)
        .auto_offset_reset(AutoOffsetReset::Earliest)
        .build()
        .await
        .expect("Failed to create consumer1");

    consumer1
        .subscribe(&[topic_name])
        .await
        .expect("Failed to subscribe consumer1");

    // Poll to join group (first poll may only do rebalance)
    let records1 = poll_for_records(&consumer1, 1, Duration::from_secs(5), 5).await;

    // Create second consumer in same group
    let consumer2 = krafka::Kafka::builder(&bootstrap_servers)
        .connect()
        .await
        .expect("Failed to create consumer2")
        .consumer(group_id)
        .auto_offset_reset(AutoOffsetReset::Earliest)
        .build()
        .await
        .expect("Failed to create consumer2");

    consumer2
        .subscribe(&[topic_name])
        .await
        .expect("Failed to subscribe consumer2");

    // Poll both consumers
    let records2 = poll_for_records(&consumer2, 0, Duration::from_secs(5), 3).await;

    // At least one consumer should have received messages
    let total_records = records1.len() + records2.len();
    assert!(
        total_records > 0,
        "Expected at least some records from consumer group"
    );
    consumer1.close().await.expect("consumer1 close");
    consumer2.close().await.expect("consumer2 close");
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn test_connection_timeout_handling() {
    // Try to connect to a non-existent broker with short timeout
    let result = async {
        krafka::Kafka::builder("127.0.0.1:19999")
            // Non-existent port
            .client_id("timeout-test")
            .connect()
            .await?
            .producer()
            .build()
            .await
    }
    .await;

    // Should fail with connection error
    assert!(
        result.is_err(),
        "Expected connection failure to non-existent broker"
    );
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn test_producer_continues_after_metadata_refresh() {
    let bootstrap_servers = kafka();

    let topic = "resilience-test-topic";
    create_topic(&bootstrap_servers, topic, 1).await;

    let producer = krafka::Kafka::builder(&bootstrap_servers)
        .client_id("resilience-test")
        .connect()
        .await
        .expect("Failed to create producer")
        .producer()
        .build()
        .await
        .expect("Failed to create producer");

    // Send multiple messages to verify producer stability
    for i in 0..5 {
        let result = producer
            .send(
                krafka::Record::new(
                    topic,
                    bytes::Bytes::copy_from_slice(format!("value-{}", i).as_bytes()),
                )
                .key(bytes::Bytes::copy_from_slice(
                    format!("key-{}", i).as_bytes(),
                )),
            )
            .await;

        assert!(result.is_ok(), "Message {} should succeed", i);

        // Small delay between sends
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    producer.close().await.unwrap();
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn test_topic_survives_a_partial_metadata_refresh_for_another_topic() {
    let bootstrap_servers = kafka();

    let topic_a = "partial-refresh-a";
    let topic_b = "partial-refresh-b";
    create_topic(&bootstrap_servers, topic_a, 1).await;
    create_topic(&bootstrap_servers, topic_b, 1).await;

    // Short ages reproduce the five-minute defaults without a five-minute test:
    // both topics go stale, and routing a record to topic A then triggers a
    // metadata refresh that names topic A alone.
    let producer = krafka::Kafka::builder(&bootstrap_servers)
        .client_id("partial-refresh-test")
        .metadata_max_age(Duration::from_millis(200))
        .metadata_topic_cache_ttl(Some(Duration::from_millis(200)))
        .connect()
        .await
        .expect("Failed to create producer")
        .producer()
        .build()
        .await
        .expect("Failed to create producer");

    let _ = producer
        .send(krafka::Record::new(topic_a, "warm-a"))
        .await
        .expect("warming topic A should succeed");
    let _ = producer
        .send(krafka::Record::new(topic_b, "warm-b"))
        .await
        .expect("warming topic B should succeed");

    tokio::time::sleep(Duration::from_millis(300)).await;

    let _ = producer
        .send(krafka::Record::new(topic_a, "refresh-a"))
        .await
        .expect("a stale topic refreshes itself");

    // Topic B still exists in the cluster. It used to be evicted by the
    // refresh above and then reported as `unknown topic` for the rest of the
    // producer's life, because nothing on the send path re-fetched it.
    let _ = producer
        .send(krafka::Record::new(topic_b, "after-refresh"))
        .await
        .expect("topic B must stay usable after a partial refresh for topic A");

    producer.close().await.unwrap();
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn test_send_to_a_nonexistent_topic_reports_the_broker_error() {
    use krafka::error::{ErrorCode, KrafkaError};

    let bootstrap_servers = kafka();

    let producer = krafka::Kafka::builder(&bootstrap_servers)
        .client_id("unknown-topic-test")
        .connect()
        .await
        .expect("Failed to create producer")
        .producer()
        // Bound the metadata wait so the test does not sit out the 60 s default.
        .max_block(Duration::from_secs(5))
        .build()
        .await
        .expect("Failed to create producer");

    let error = producer
        .send(krafka::Record::new("no-such-topic-anywhere", "v"))
        .await
        .expect_err("a topic the cluster does not have cannot be produced to");

    assert!(
        matches!(
            error,
            KrafkaError::Broker {
                code: ErrorCode::UnknownTopicOrPartition,
                ..
            }
        ),
        "expected the broker's own topic error, got: {error}"
    );

    producer.close().await.unwrap();
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn test_auto_create_topics_materialises_the_topic() {
    let bootstrap_servers = kafka();

    // The broker runs with `auto.create.topics.enable=true` (Kafka's default),
    // so the only thing standing between this send and a created topic is
    // whether the client says it is willing.
    let producer = krafka::Kafka::builder(&bootstrap_servers)
        .client_id("auto-create-test")
        .allow_auto_create_topics(true)
        .connect()
        .await
        .expect("Failed to create producer")
        .producer()
        .max_block(Duration::from_secs(20))
        .build()
        .await
        .expect("Failed to create producer");

    let metadata = producer
        .send(krafka::Record::new("created-by-the-producer", "v"))
        .await
        .expect("the broker creates the topic because the client asked it to");
    assert_eq!(metadata.topic, "created-by-the-producer");

    producer.close().await.unwrap();
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn test_standalone_subscription_picks_up_a_topic_created_later() {
    use krafka::consumer::AutoOffsetReset;

    let bootstrap_servers = kafka();

    let topic = "appears-after-subscribe";

    let consumer = krafka::Kafka::builder(&bootstrap_servers)
        .client_id("late-topic-consumer")
        .connect()
        .await
        .expect("Failed to create consumer")
        .consumer_without_group()
        .auto_offset_reset(AutoOffsetReset::Earliest)
        .build()
        .await
        .expect("Failed to create consumer");

    // Subscribing to a topic that does not exist yet is not an error, and used
    // to be a permanent silence: the partition list was resolved once, found
    // nothing, and was never revisited.
    consumer
        .subscribe(&[topic])
        .await
        .expect("subscribe should not fail on an absent topic");
    assert!(consumer.assignment().await.is_empty());

    create_topic(&bootstrap_servers, topic, 1).await;

    let producer = krafka::Kafka::builder(&bootstrap_servers)
        .connect()
        .await
        .expect("Failed to create producer")
        .producer()
        .build()
        .await
        .expect("Failed to create producer");
    let _ = producer
        .send(krafka::Record::new(topic, "after-creation"))
        .await
        .expect("produce to the new topic");
    producer.close().await.unwrap();

    let records = poll_for_records(&consumer, 1, Duration::from_millis(500), 30).await;
    assert_eq!(
        records.len(),
        1,
        "the consumer must pick up a topic created after it subscribed"
    );
    assert_eq!(records[0].value.as_deref(), Some(&b"after-creation"[..]));

    consumer.close().await.expect("close");
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn test_standalone_subscription_picks_up_added_partitions() {
    use krafka::consumer::AutoOffsetReset;

    let bootstrap_servers = kafka();

    let topic = "scaled-up-under-a-consumer";
    create_topic(&bootstrap_servers, topic, 1).await;

    let consumer = krafka::Kafka::builder(&bootstrap_servers)
        .client_id("grow-partitions-consumer")
        .metadata_max_age(Duration::from_millis(500))
        .connect()
        .await
        .expect("Failed to create consumer")
        .consumer_without_group()
        .auto_offset_reset(AutoOffsetReset::Earliest)
        .build()
        .await
        .expect("Failed to create consumer");

    consumer.subscribe(&[topic]).await.expect("subscribe");
    assert_eq!(consumer.assignment().await[topic].len(), 1);

    let admin = krafka::Kafka::builder(&bootstrap_servers)
        .connect()
        .await
        .expect("Failed to create admin client")
        .admin();
    admin
        .create_partitions([(topic, 3)], Default::default())
        .await
        .expect("Failed to add partitions");

    let mut assigned = 1;
    for _ in 0..30 {
        let _ = consumer.poll(Duration::from_millis(300)).await;
        assigned = consumer.assignment().await.get(topic).map_or(0, Vec::len);
        if assigned == 3 {
            break;
        }
    }
    assert_eq!(
        assigned, 3,
        "partitions added to a subscribed topic must be assigned"
    );

    consumer.close().await.expect("close");
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn test_consumer_handles_no_messages_gracefully() {
    use krafka::consumer::AutoOffsetReset;

    let bootstrap_servers = kafka();

    let topic = "empty-topic-test";
    create_topic(&bootstrap_servers, topic, 1).await;

    // Create producer and send one message so topic has offsets
    let producer = krafka::Kafka::builder(&bootstrap_servers)
        .client_id("create-topic")
        .connect()
        .await
        .expect("Failed to create producer")
        .producer()
        .build()
        .await
        .expect("Failed to create producer");

    let _ = producer
        .send(krafka::Record::new(topic, "setup"))
        .await
        .expect("Failed to send setup message");
    producer.close().await.unwrap();

    // Consumer starting from latest should see no new messages
    let consumer = krafka::Kafka::builder(&bootstrap_servers)
        .connect()
        .await
        .expect("Failed to create consumer")
        .consumer("empty-test-group")
        .auto_offset_reset(AutoOffsetReset::Latest)
        .build()
        .await
        .expect("Failed to create consumer");

    consumer
        .subscribe(&[topic])
        .await
        .expect("Failed to subscribe");

    // Poll should complete without error, even with no messages
    let records = poll_for_records(&consumer, 0, Duration::from_secs(2), 3).await;

    // May be empty or have the setup message depending on timing
    drop(records);
    consumer.close().await.expect("consumer close");
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn test_multiple_producers_same_topic() {
    use krafka::consumer::AutoOffsetReset;

    let bootstrap_servers = kafka();

    let topic = "multi-producer-topic";
    create_topic(&bootstrap_servers, topic, 1).await;

    // Create multiple producers
    let producer1 = krafka::Kafka::builder(&bootstrap_servers)
        .client_id("producer-1")
        .connect()
        .await
        .expect("Failed to create producer 1")
        .producer()
        .build()
        .await
        .expect("Failed to create producer 1");

    let producer2 = krafka::Kafka::builder(&bootstrap_servers)
        .client_id("producer-2")
        .connect()
        .await
        .expect("Failed to create producer 2")
        .producer()
        .build()
        .await
        .expect("Failed to create producer 2");

    // Send from both producers
    for i in 0..3 {
        let _ = producer1
            .send(
                krafka::Record::new(
                    topic,
                    bytes::Bytes::copy_from_slice(format!("p1-msg-{}", i).as_bytes()),
                )
                .key("p1"),
            )
            .await
            .expect("Producer 1 failed");

        let _ = producer2
            .send(
                krafka::Record::new(
                    topic,
                    bytes::Bytes::copy_from_slice(format!("p2-msg-{}", i).as_bytes()),
                )
                .key("p2"),
            )
            .await
            .expect("Producer 2 failed");
    }

    producer1.close().await.unwrap();
    producer2.close().await.unwrap();

    // Verify all messages were received
    let consumer = krafka::Kafka::builder(&bootstrap_servers)
        .connect()
        .await
        .expect("Failed to create consumer")
        .consumer("multi-producer-consumer")
        .auto_offset_reset(AutoOffsetReset::Earliest)
        .build()
        .await
        .expect("Failed to create consumer");

    consumer
        .subscribe(&[topic])
        .await
        .expect("Failed to subscribe");

    // Collect all messages (first poll may be consumed by rebalance)
    let all_records = poll_for_records(&consumer, 6, Duration::from_secs(3), 8).await;

    assert_eq!(all_records.len(), 6, "Expected 6 messages from 2 producers");
    consumer.close().await.expect("consumer close");
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn test_large_message_handling() {
    use krafka::consumer::AutoOffsetReset;

    let bootstrap_servers = kafka();

    let topic = "large-message-topic";
    create_topic(&bootstrap_servers, topic, 1).await;

    let producer = krafka::Kafka::builder(&bootstrap_servers)
        .client_id("large-message-producer")
        .connect()
        .await
        .expect("Failed to create producer")
        .producer()
        .build()
        .await
        .expect("Failed to create producer");

    // Create a large message (100KB)
    let large_value = vec![b'X'; 100 * 1024];

    let metadata = producer
        .send(
            krafka::Record::new(topic, bytes::Bytes::copy_from_slice(&large_value))
                .key("large-key"),
        )
        .await
        .expect("Failed to send large message");

    assert!(metadata.offset >= 0);
    producer.close().await.unwrap();

    // Verify consumer can read it
    let consumer = krafka::Kafka::builder(&bootstrap_servers)
        .connect()
        .await
        .expect("Failed to create consumer")
        .consumer("large-message-consumer")
        .auto_offset_reset(AutoOffsetReset::Earliest)
        .build()
        .await
        .expect("Failed to create consumer");

    consumer
        .subscribe(&[topic])
        .await
        .expect("Failed to subscribe");

    let records = poll_for_records(&consumer, 1, Duration::from_secs(5), 5).await;

    assert!(!records.is_empty());
    assert_eq!(
        records[0].value.as_ref().map(|v| v.len()).unwrap_or(0),
        100 * 1024
    );
    consumer.close().await.expect("consumer close");
}

// ============================================================================
// Additional Integration Tests
// ============================================================================

#[tokio::test]
#[ignore = "requires Docker"]
async fn test_message_headers() {
    use krafka::consumer::AutoOffsetReset;

    let bootstrap_servers = kafka();

    let topic = "headers-test-topic";
    create_topic(&bootstrap_servers, topic, 1).await;

    let producer = krafka::Kafka::builder(&bootstrap_servers)
        .client_id("header-test-producer")
        .connect()
        .await
        .expect("Failed to create producer")
        .producer()
        .build()
        .await
        .expect("Failed to create producer");

    // Create headers as Vec<(String, Option<Bytes>)>: `None` would be a null
    // header value, which the wire format distinguishes from zero-length.
    let headers = vec![
        (
            "trace-id".to_string(),
            Some(bytes::Bytes::from_static(b"abc123")),
        ),
        (
            "content-type".to_string(),
            Some(bytes::Bytes::from_static(b"application/json")),
        ),
    ];

    // Send message with headers
    let metadata = producer
        .send({
            let mut r = krafka::Record::new(topic, "header-value").key("header-key");
            r.headers = headers;
            r
        })
        .await
        .expect("Failed to send message with headers");

    assert!(metadata.offset >= 0);
    producer.close().await.unwrap();

    // Verify consumer receives headers
    let consumer = krafka::Kafka::builder(&bootstrap_servers)
        .connect()
        .await
        .expect("Failed to create consumer")
        .consumer("header-test-consumer")
        .auto_offset_reset(AutoOffsetReset::Earliest)
        .build()
        .await
        .expect("Failed to create consumer");

    consumer
        .subscribe(&[topic])
        .await
        .expect("Failed to subscribe");

    let records = poll_for_records(&consumer, 1, Duration::from_secs(5), 5).await;

    assert!(!records.is_empty());
    let record = &records[0];

    // Verify headers are present
    assert!(record.header("trace-id").is_some());
    consumer.close().await.expect("consumer close");
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn test_idempotent_producer() {
    let bootstrap_servers = kafka();

    let topic = "idempotent-test-topic";
    create_topic(&bootstrap_servers, topic, 1).await;

    // Create idempotent producer (enabled by default since KIP-679)
    let producer = krafka::Kafka::builder(&bootstrap_servers)
        .client_id("idempotent-producer-test")
        .connect()
        .await
        .expect("Failed to create idempotent producer")
        .producer()
        .build()
        .await
        .expect("Failed to create idempotent producer");

    // Send multiple messages
    for i in 0..5 {
        let metadata = producer
            .send(
                krafka::Record::new(
                    topic,
                    bytes::Bytes::copy_from_slice(format!("value-{}", i).as_bytes()),
                )
                .key(bytes::Bytes::copy_from_slice(
                    format!("key-{}", i).as_bytes(),
                )),
            )
            .await
            .expect("Failed to send message");

        // Idempotent producer should maintain sequence
        assert!(metadata.offset >= 0);
    }

    producer.close().await.unwrap();
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn test_null_key_and_value() {
    use krafka::consumer::AutoOffsetReset;

    let bootstrap_servers = kafka();

    let topic = "null-test-topic";
    create_topic(&bootstrap_servers, topic, 1).await;

    let producer = krafka::Kafka::builder(&bootstrap_servers)
        .client_id("null-test-producer")
        .connect()
        .await
        .expect("Failed to create producer")
        .producer()
        .build()
        .await
        .expect("Failed to create producer");

    // Send message with null key
    let metadata = producer
        .send(krafka::Record::new(topic, "value-with-null-key"))
        .await
        .expect("Failed to send message with null key");
    assert!(metadata.offset >= 0);

    // Send a null value (a tombstone) and a zero-length value, which the wire
    // format distinguishes from each other and from the record above.
    let metadata = producer
        .send(krafka::Record::tombstone(topic, "key-with-null-value"))
        .await
        .expect("Failed to send tombstone");
    assert!(metadata.offset >= 0);

    let metadata = producer
        .send(krafka::Record::new(topic, "").key("key-with-empty-value"))
        .await
        .expect("Failed to send empty value");
    assert!(metadata.offset >= 0);

    producer.close().await.unwrap();

    // Verify consumer receives the message
    let consumer = krafka::Kafka::builder(&bootstrap_servers)
        .connect()
        .await
        .expect("Failed to create consumer")
        .consumer("null-test-consumer")
        .auto_offset_reset(AutoOffsetReset::Earliest)
        .build()
        .await
        .expect("Failed to create consumer");

    consumer
        .subscribe(&[topic])
        .await
        .expect("Failed to subscribe");

    let records = poll_for_records(&consumer, 3, Duration::from_secs(10), 5).await;

    assert_eq!(records.len(), 3, "all three records should come back");

    // Verify null key is received as None
    assert!(records[0].key.is_none());
    assert!(records[0].value.is_some());

    // The tombstone must come back null, not zero-length.
    assert!(
        records[1].value.is_none(),
        "a tombstone must decode as null"
    );
    assert!(records[1].is_tombstone());

    // The zero-length value must come back present-but-empty. If the broker
    // round trip collapsed the two, this assertion and the one above cannot
    // both hold.
    assert_eq!(records[2].value.as_deref(), Some(&b""[..]));
    assert!(!records[2].is_tombstone());

    consumer.close().await.expect("consumer close");
}

/// A tombstone written to a real `cleanup.policy=compact` topic must survive
/// the broker round trip as a null value.
///
/// Compaction itself runs on the broker's own schedule, so this asserts the
/// part that is the client's contract: what krafka wrote is what a consumer
/// reads back, null and all. If the null collapsed to zero-length anywhere on
/// the produce path the broker would store an ordinary record and never delete
/// the key — which is exactly the defect this test guards.
#[tokio::test]
#[ignore = "requires Docker"]
async fn test_tombstone_round_trip_on_compacted_topic() {
    use krafka::consumer::AutoOffsetReset;
    use krafka::producer::Record;

    let bootstrap_servers = kafka();

    let topic = "compacted-tombstone-topic";
    create_topic_with_configs(
        &bootstrap_servers,
        topic,
        1,
        &[
            ("cleanup.policy", "compact"),
            // Make the active segment eligible immediately, so a broker that
            // does compact during the test still behaves correctly.
            ("min.cleanable.dirty.ratio", "0.01"),
            ("segment.ms", "100"),
            ("delete.retention.ms", "60000"),
        ],
    )
    .await;

    let producer = krafka::Kafka::builder(&bootstrap_servers)
        .client_id("tombstone-producer")
        .connect()
        .await
        .expect("Failed to create producer")
        .producer()
        .build()
        .await
        .expect("Failed to create producer");

    let value_meta = producer
        .send(krafka::Record::new(topic, "alice").key("user-42"))
        .await
        .expect("Failed to send valued record");

    let tombstone_meta = producer
        .send(
            Record::tombstone(topic, "user-42")
                .header("X-Reason", &b"gdpr-erasure"[..])
                .null_header("X-Flag"),
        )
        .await
        .expect("Failed to send tombstone");

    assert_eq!(
        value_meta.partition, tombstone_meta.partition,
        "the tombstone must share a partition with the key it deletes"
    );

    producer.close().await.unwrap();

    let consumer = krafka::Kafka::builder(&bootstrap_servers)
        .connect()
        .await
        .expect("Failed to create consumer")
        .consumer("tombstone-consumer")
        .auto_offset_reset(AutoOffsetReset::Earliest)
        .build()
        .await
        .expect("Failed to create consumer");

    consumer
        .subscribe(&[topic])
        .await
        .expect("Failed to subscribe");

    let records = poll_for_records(&consumer, 2, Duration::from_secs(10), 5).await;
    assert_eq!(records.len(), 2, "both records should come back");

    assert_eq!(records[0].value.as_deref(), Some(&b"alice"[..]));
    assert!(!records[0].is_tombstone());

    let tombstone = &records[1];
    assert_eq!(tombstone.key.as_deref(), Some(&b"user-42"[..]));
    assert_eq!(
        tombstone.value, None,
        "the broker must return the tombstone as a null value"
    );
    assert!(tombstone.is_tombstone());

    // Header nullness survives the broker too.
    assert_eq!(tombstone.headers[0].0, "X-Reason");
    assert_eq!(
        tombstone.headers[0].1.as_deref(),
        Some(&b"gdpr-erasure"[..])
    );
    assert_eq!(tombstone.headers[1].0, "X-Flag");
    assert_eq!(tombstone.headers[1].1, None);

    consumer.close().await.expect("consumer close");
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn test_multiple_topics_subscription() {
    use krafka::consumer::AutoOffsetReset;

    let bootstrap_servers = kafka();

    let topic1 = "multi-topic-1";
    let topic2 = "multi-topic-2";
    create_topic(&bootstrap_servers, topic1, 1).await;
    create_topic(&bootstrap_servers, topic2, 1).await;

    let producer = krafka::Kafka::builder(&bootstrap_servers)
        .client_id("multi-topic-producer")
        .connect()
        .await
        .expect("Failed to create producer")
        .producer()
        .build()
        .await
        .expect("Failed to create producer");

    // Send messages to both topics
    let _ = producer
        .send(krafka::Record::new(topic1, "value1").key("key1"))
        .await
        .expect("send failed");
    let _ = producer
        .send(krafka::Record::new(topic2, "value2").key("key2"))
        .await
        .expect("send failed");
    producer.close().await.unwrap();

    // Consumer subscribed to both topics
    let consumer = krafka::Kafka::builder(&bootstrap_servers)
        .connect()
        .await
        .expect("Failed to create consumer")
        .consumer("multi-topic-consumer")
        .auto_offset_reset(AutoOffsetReset::Earliest)
        .build()
        .await
        .expect("Failed to create consumer");

    consumer
        .subscribe(&[topic1, topic2])
        .await
        .expect("Failed to subscribe");

    // Collect messages from both topics (first poll may be consumed by rebalance)
    let all_records = poll_for_records(&consumer, 2, Duration::from_secs(3), 8).await;

    assert_eq!(all_records.len(), 2, "Expected 2 messages from 2 topics");

    // Verify we got messages from both topics
    let topics: std::collections::HashSet<_> = all_records.iter().map(|r| &*r.topic).collect();
    assert!(
        topics.contains(topic1) && topics.contains(topic2),
        "Should contain messages from both topics, got: {:?}",
        topics
    );
    consumer.close().await.expect("consumer close");
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn test_admin_describe_configs() {
    use krafka::admin::NewTopic;

    let bootstrap_servers = kafka();

    let admin = krafka::Kafka::builder(&bootstrap_servers)
        .client_id("config-test-admin")
        .connect()
        .await
        .expect("Failed to create admin client")
        .admin();

    // Create a topic first
    let topic_name = "config-test-topic";
    let new_topic = NewTopic::new(topic_name, 1, 1).unwrap();
    admin
        .create_topics(vec![new_topic], Default::default())
        .await
        .expect("Failed to create topic");

    tokio::time::sleep(Duration::from_secs(1)).await;

    // Describe topic configs
    use krafka::admin::ConfigResource;
    let configs = admin
        .describe_configs([ConfigResource::topic(topic_name)], Default::default())
        .await
        .expect("Failed to describe configs")
        .remove(&ConfigResource::topic(topic_name))
        .expect("the topic is answered")
        .expect("Failed to describe configs");

    // Should have some configuration entries
    assert!(!configs.is_empty(), "Expected config entries");

    // Clean up
    admin
        .delete_topics(
            vec![topic_name.to_string()],
            krafka::admin::DeleteTopicsOptions::default(),
        )
        .await
        .ok();
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn test_concurrent_producers() {
    use krafka::consumer::AutoOffsetReset;

    let bootstrap_servers = kafka();

    let topic = "concurrent-producer-topic";
    create_topic(&bootstrap_servers, topic, 3).await;

    // Spawn multiple producer tasks concurrently
    let bootstrap = bootstrap_servers.clone();
    let handles: Vec<_> = (0..3)
        .map(|i| {
            let bs = bootstrap.clone();
            tokio::spawn(async move {
                let producer = krafka::Kafka::builder(&bs)
                    .client_id(format!("concurrent-producer-{}", i))
                    .connect()
                    .await
                    .expect("Failed to create producer")
                    .producer()
                    .build()
                    .await
                    .expect("Failed to create producer");

                for j in 0..5 {
                    let _ = producer
                        .send(
                            krafka::Record::new(
                                "concurrent-producer-topic",
                                bytes::Bytes::copy_from_slice(
                                    format!("value-{}-{}", i, j).as_bytes(),
                                ),
                            )
                            .key(bytes::Bytes::copy_from_slice(
                                format!("key-{}-{}", i, j).as_bytes(),
                            )),
                        )
                        .await
                        .expect("Failed to send");
                }
                producer.close().await.unwrap();
            })
        })
        .collect();

    // Wait for all producers to complete
    for handle in handles {
        handle.await.expect("Producer task failed");
    }

    // Verify all 15 messages were received
    let consumer = krafka::Kafka::builder(&bootstrap_servers)
        .connect()
        .await
        .expect("Failed to create consumer")
        .consumer("concurrent-producer-consumer")
        .auto_offset_reset(AutoOffsetReset::Earliest)
        .build()
        .await
        .expect("Failed to create consumer");

    consumer
        .subscribe(&[topic])
        .await
        .expect("Failed to subscribe");

    let all_records = poll_for_records(&consumer, 15, Duration::from_secs(3), 10).await;

    assert_eq!(
        all_records.len(),
        15,
        "Expected 15 messages from 3 concurrent producers"
    );
    consumer.close().await.expect("consumer close");
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn test_producer_with_batching() {
    use krafka::consumer::AutoOffsetReset;

    let bootstrap_servers = kafka();

    let topic = "batch-test-topic";
    create_topic(&bootstrap_servers, topic, 1).await;

    // Create producer with batching enabled (linger > 0)
    let producer = krafka::Kafka::builder(&bootstrap_servers)
        .client_id("batch-producer")
        .connect()
        .await
        .expect("Failed to create producer")
        .producer()
        .linger(Duration::from_millis(50))
        // Enable batching
        .batch_size(16384)
        .build()
        .await
        .expect("Failed to create producer");

    // Send messages rapidly - should be batched
    for i in 0..10 {
        let _ = producer
            .send(
                krafka::Record::new(
                    topic,
                    bytes::Bytes::copy_from_slice(format!("value-{}", i).as_bytes()),
                )
                .key(bytes::Bytes::copy_from_slice(
                    format!("key-{}", i).as_bytes(),
                )),
            )
            .await
            .expect("Failed to send");
    }
    producer.close().await.unwrap();

    // Verify consumer receives all messages
    let consumer = krafka::Kafka::builder(&bootstrap_servers)
        .connect()
        .await
        .expect("Failed to create consumer")
        .consumer("batch-consumer")
        .auto_offset_reset(AutoOffsetReset::Earliest)
        .build()
        .await
        .expect("Failed to create consumer");

    consumer
        .subscribe(&[topic])
        .await
        .expect("Failed to subscribe");

    let records = poll_for_records(&consumer, 10, Duration::from_secs(5), 5).await;
    assert_eq!(records.len(), 10, "Expected 10 messages");
    consumer.close().await.expect("consumer close");
}

// Note: TransactionalProducer tests are skipped because transaction coordinator
// resolution requires connecting to broker addresses returned by FindCoordinator,
// which returns internal container addresses that don't work with testcontainers
// port mapping. TransactionalProducer has been tested manually with real Kafka clusters.

#[tokio::test]
#[ignore = "requires Docker"]
async fn test_admin_create_partitions() {
    use krafka::admin::NewTopic;

    let bootstrap_servers = kafka();

    let admin = krafka::Kafka::builder(&bootstrap_servers)
        .connect()
        .await
        .expect("Failed to create admin client")
        .admin();

    let topic_name = "partition-increase-topic";

    // Create topic with 2 partitions
    admin
        .create_topics(
            vec![NewTopic::new(topic_name, 2, 1).unwrap()],
            Default::default(),
        )
        .await
        .expect("Failed to create topic");

    tokio::time::sleep(Duration::from_secs(1)).await;

    // Verify initial partition count
    let count = admin
        .describe_topics([topic_name], Default::default())
        .await
        .expect("Failed to describe")
        .remove(topic_name)
        .and_then(Result::ok)
        .map(|t| t.partitions.len());
    assert_eq!(count, Some(2), "Expected 2 partitions initially");

    // Increase to 4 partitions
    admin
        .create_partitions([(topic_name, 4)], Default::default())
        .await
        .expect("Failed to create partitions");

    tokio::time::sleep(Duration::from_secs(1)).await;

    // Verify new partition count
    let count = admin
        .describe_topics([topic_name], Default::default())
        .await
        .expect("Failed to describe")
        .remove(topic_name)
        .and_then(Result::ok)
        .map(|t| t.partitions.len());
    assert_eq!(count, Some(4), "Expected 4 partitions after increase");

    // Clean up
    admin
        .delete_topics(
            vec![topic_name.to_string()],
            krafka::admin::DeleteTopicsOptions::default(),
        )
        .await
        .ok();
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn test_admin_alter_topic_config() {
    use krafka::admin::NewTopic;

    let bootstrap_servers = kafka();

    let admin = krafka::Kafka::builder(&bootstrap_servers)
        .connect()
        .await
        .expect("Failed to create admin client")
        .admin();

    let topic_name = "config-alter-topic";

    // Create topic
    admin
        .create_topics(
            vec![NewTopic::new(topic_name, 1, 1).unwrap()],
            Default::default(),
        )
        .await
        .expect("Failed to create topic");

    tokio::time::sleep(Duration::from_secs(1)).await;

    // Alter topic config - set retention to 1 hour
    use krafka::admin::{ConfigOp, ConfigResource};
    let resource = ConfigResource::topic(topic_name);
    let result = admin
        .incremental_alter_configs(
            [(
                resource.clone(),
                vec![ConfigOp::set("retention.ms", "3600000")],
            )],
            Default::default(),
        )
        .await
        .expect("Failed to alter config");

    assert!(
        result[&resource].is_ok(),
        "Config alteration should succeed"
    );

    // Verify the config was changed
    let topic_configs = admin
        .describe_configs([resource.clone()], Default::default())
        .await
        .expect("Failed to describe config")
        .remove(&resource)
        .expect("the topic is answered")
        .expect("Failed to describe config");

    let retention_config = topic_configs
        .iter()
        .find(|c| c.name == "retention.ms")
        .expect("retention.ms config not found");

    assert_eq!(
        retention_config.value.as_deref(),
        Some("3600000"),
        "retention.ms should be 3600000"
    );

    // Clean up
    admin
        .delete_topics(
            vec![topic_name.to_string()],
            krafka::admin::DeleteTopicsOptions::default(),
        )
        .await
        .ok();
}
#[tokio::test]
#[ignore = "requires Docker"]
async fn test_admin_describe_cluster() {
    let bootstrap_servers = kafka();

    let admin = krafka::Kafka::builder(&bootstrap_servers)
        .connect()
        .await
        .expect("Failed to create admin client")
        .admin();

    let cluster = admin
        .describe_cluster(Default::default())
        .await
        .expect("Failed to describe cluster");

    // Single-broker testcontainers setup
    assert!(
        !cluster.brokers.is_empty(),
        "Should have at least one broker"
    );
    // Note: controller_id may be None in some Kafka configurations

    let broker = &cluster.brokers[0];
    assert!(!broker.host.is_empty(), "Broker should have a host");
    assert!(broker.port > 0, "Broker should have a valid port");
    assert!(broker.id >= 0, "Broker should have a valid ID");
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn test_admin_describe_topics() {
    use krafka::admin::NewTopic;

    let bootstrap_servers = kafka();

    let admin = krafka::Kafka::builder(&bootstrap_servers)
        .connect()
        .await
        .expect("Failed to create admin client")
        .admin();

    // Create test topics
    let topic1 = "describe-topic-1";
    let topic2 = "describe-topic-2";

    admin
        .create_topics(
            vec![
                NewTopic::new(topic1, 2, 1).unwrap(),
                NewTopic::new(topic2, 3, 1).unwrap(),
            ],
            Default::default(),
        )
        .await
        .expect("Failed to create topics");

    tokio::time::sleep(Duration::from_secs(1)).await;

    // Describe the topics
    let topics = admin
        .describe_topics([topic1, topic2], Default::default())
        .await
        .expect("Failed to describe topics");

    assert_eq!(topics.len(), 2, "Should describe 2 topics");

    let t1 = topics[topic1].as_ref().expect("topic1 not found");
    let t2 = topics[topic2].as_ref().expect("topic2 not found");

    assert_eq!(t1.partitions.len(), 2, "topic1 should have 2 partitions");
    assert_eq!(t2.partitions.len(), 3, "topic2 should have 3 partitions");

    // Clean up
    admin
        .delete_topics(
            vec![topic1.to_string(), topic2.to_string()],
            krafka::admin::DeleteTopicsOptions::default(),
        )
        .await
        .ok();
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn test_producer_timestamp_propagation() {
    use krafka::consumer::AutoOffsetReset;
    use krafka::producer::Record;

    let bootstrap_servers = kafka();

    let topic = "timestamp-test-topic";
    create_topic(&bootstrap_servers, topic, 1).await;

    let producer = krafka::Kafka::builder(&bootstrap_servers)
        .client_id("timestamp-test-producer")
        .connect()
        .await
        .expect("Failed to create producer")
        .producer()
        .build()
        .await
        .expect("Failed to create producer");

    // Send with explicit timestamp
    let timestamp = 1700000000000_i64; // Unix epoch ms
    let record = Record::new(topic, b"hello".to_vec())
        .key(b"ts-key".to_vec())
        .timestamp(timestamp);
    let metadata = producer
        .send(record)
        .await
        .expect("Failed to send record with timestamp");

    assert!(metadata.offset >= 0);
    producer.close().await.unwrap();

    // Use manual partition assignment (no group coordinator) to avoid
    // a race where ListOffsets(timestamp=-2) transiently returns the high
    // watermark instead of the log start offset for freshly created
    // partitions, AND the group coordinator rejoin in poll() overwrites
    // any seek_to_beginning() the test applies.
    let consumer = krafka::Kafka::builder(&bootstrap_servers)
        .connect()
        .await
        .expect("Failed to create consumer")
        .consumer_without_group()
        .auto_offset_reset(AutoOffsetReset::Earliest)
        .build()
        .await
        .expect("Failed to create consumer");

    consumer
        .assign(topic, vec![0])
        .await
        .expect("Failed to assign");

    // Explicitly seek to offset 0 so we always start from the beginning,
    // regardless of what ListOffsets returned during assign().
    consumer
        .seek_to_beginning(topic, 0)
        .await
        .expect("seek_to_beginning failed");

    let records = poll_for_records(&consumer, 1, Duration::from_secs(5), 8).await;

    assert!(!records.is_empty(), "Expected at least one record");
    let record = &records[0];
    // With the default CreateTime policy, the timestamp should match exactly.
    // LogAppendTime would override it, so we accept either exact match or > 0.
    assert!(record.timestamp > 0, "Timestamp should be set");
    if record.timestamp != timestamp {
        // LogAppendTime override — just ensure it's a reasonable recent timestamp
        assert!(
            record.timestamp > 1_600_000_000_000,
            "Timestamp should be a reasonable epoch ms, got {}",
            record.timestamp
        );
    }
    consumer.close().await.expect("consumer close");
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn test_consumer_manual_assign() {
    use krafka::consumer::AutoOffsetReset;
    use krafka::producer::Record;

    let bootstrap_servers = kafka();

    let topic = "manual-assign-topic";
    create_topic(&bootstrap_servers, topic, 2).await;

    let producer = krafka::Kafka::builder(&bootstrap_servers)
        .connect()
        .await
        .expect("Failed to create producer")
        .producer()
        .build()
        .await
        .expect("Failed to create producer");

    // Send messages explicitly to partition 0 so the test is deterministic
    for i in 0..5 {
        let record = Record::new(topic, format!("val-{}", i).into_bytes())
            .partition(0)
            .key(format!("k-{}", i).into_bytes());
        let _ = producer.send(record).await.expect("send failed");
    }
    // Also send some to partition 1 (should NOT be received)
    for i in 0..5 {
        let record = Record::new(topic, format!("val-p1-{}", i).into_bytes())
            .partition(1)
            .key(format!("k1-{}", i).into_bytes());
        let _ = producer.send(record).await.expect("send failed");
    }
    producer.close().await.unwrap();

    // Create consumer WITHOUT group_id — manual assignment mode
    let consumer = krafka::Kafka::builder(&bootstrap_servers)
        .connect()
        .await
        .expect("Failed to create consumer")
        .consumer_without_group()
        .auto_offset_reset(AutoOffsetReset::Earliest)
        .build()
        .await
        .expect("Failed to create consumer");

    // Manually assign partition 0
    consumer
        .assign(topic, vec![0])
        .await
        .expect("Failed to assign");

    let records = poll_for_records(&consumer, 1, Duration::from_secs(5), 5).await;

    // Should have records from partition 0 only
    for record in &records {
        assert_eq!(record.partition, 0, "Should only get partition 0");
    }
    assert!(!records.is_empty(), "Expected records from partition 0");
    consumer.close().await.expect("consumer close");
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn test_admin_list_consumer_groups() {
    use krafka::consumer::AutoOffsetReset;

    let bootstrap_servers = kafka();

    let topic = "group-list-topic";
    create_topic(&bootstrap_servers, topic, 1).await;

    let group_id = "group-list-test-group";

    // Create a consumer and join a group
    let consumer = krafka::Kafka::builder(&bootstrap_servers)
        .connect()
        .await
        .expect("Failed to create consumer")
        .consumer(group_id)
        .auto_offset_reset(AutoOffsetReset::Earliest)
        .build()
        .await
        .expect("Failed to create consumer");

    consumer
        .subscribe(&[topic])
        .await
        .expect("Failed to subscribe");

    // Poll multiple times to ensure the group is actually joined (rebalance may consume first poll)
    let _ = poll_for_records(&consumer, 0, Duration::from_secs(3), 3).await;

    // Admin client should be able to list the group
    let admin = krafka::Kafka::builder(&bootstrap_servers)
        .connect()
        .await
        .expect("Failed to create admin client")
        .admin();

    let groups: Vec<_> = admin
        .list_consumer_groups(Default::default())
        .await
        .expect("Failed to list groups")
        .into_values()
        .flat_map(|per_broker| per_broker.expect("ListGroups failed on a broker"))
        .collect();

    assert!(
        groups.iter().any(|g| g.group_id == group_id),
        "Expected to find group '{}' in list: {:?}",
        group_id,
        groups.iter().map(|g| &g.group_id).collect::<Vec<_>>()
    );
    consumer.close().await.expect("consumer close");
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn test_consumer_unsubscribe() {
    use krafka::consumer::AutoOffsetReset;
    use krafka::error::{ErrorCode, KrafkaError};

    let bootstrap_servers = kafka();

    let topic = "unsub-test-topic";
    create_topic(&bootstrap_servers, topic, 1).await;

    let consumer = krafka::Kafka::builder(&bootstrap_servers)
        .connect()
        .await
        .expect("Failed to create consumer")
        .consumer("unsub-test-group")
        .auto_offset_reset(AutoOffsetReset::Earliest)
        .build()
        .await
        .expect("Failed to create consumer");

    consumer
        .subscribe(&[topic])
        .await
        .expect("Failed to subscribe");

    // Poll to join group (rebalance may consume first poll)
    let _ = poll_for_records(&consumer, 0, Duration::from_secs(3), 3).await;

    // Unsubscribe
    let unsubscribe_result = consumer.unsubscribe().await;

    // Subscription should be empty
    let subscription = consumer.subscription().await;
    assert!(
        subscription.is_empty(),
        "Subscription should be empty after unsubscribe"
    );
    if let Err(error) = unsubscribe_result {
        assert!(
            matches!(
                error,
                KrafkaError::Broker {
                    code: ErrorCode::UnknownMemberId
                        | ErrorCode::IllegalGeneration
                        | ErrorCode::RebalanceInProgress
                        | ErrorCode::NotCoordinator
                        | ErrorCode::CoordinatorLoadInProgress,
                    ..
                }
            ),
            "unsubscribe should either succeed or fail only with a bounded coordinator race after clearing local state: {error}"
        );
    }
    consumer.close().await.expect("consumer close");
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn test_producer_metrics() {
    let bootstrap_servers = kafka();

    let topic = "metrics-test-topic";
    create_topic(&bootstrap_servers, topic, 1).await;

    let producer = krafka::Kafka::builder(&bootstrap_servers)
        .client_id("metrics-test-producer")
        .connect()
        .await
        .expect("Failed to create producer")
        .producer()
        .build()
        .await
        .expect("Failed to create producer");

    // Send some messages
    for i in 0..5 {
        let _ = producer
            .send(
                krafka::Record::new(topic, "value")
                    .key(bytes::Bytes::copy_from_slice(format!("k-{}", i).as_bytes())),
            )
            .await
            .expect("send failed");
    }

    let metrics = producer.metrics();
    assert_eq!(
        metrics.producer.records_sent, 5,
        "Should have sent 5 records"
    );
    assert!(metrics.producer.bytes_sent > 0, "Should have sent bytes");
    assert_eq!(metrics.producer.errors, 0, "Should have no errors");

    producer.close().await.unwrap();
    assert!(producer.is_closed(), "Producer should be closed");
}

/// Test that sending after producer.close() returns an error (not a panic).
#[tokio::test]
#[ignore = "requires Docker"]
async fn test_send_after_producer_close() {
    let bootstrap_servers = kafka();

    let topic = "send-after-close-topic";
    create_topic(&bootstrap_servers, topic, 1).await;

    let producer = krafka::Kafka::builder(&bootstrap_servers)
        .connect()
        .await
        .expect("Failed to create producer")
        .producer()
        .build()
        .await
        .expect("Failed to create producer");

    producer.close().await.unwrap();
    assert!(producer.is_closed());

    let result = producer
        .send(krafka::Record::new(topic, "should-fail"))
        .await;
    assert!(result.is_err(), "Send after close should return an error");
}

/// Test consumer commit and verified resume from committed offset.
#[tokio::test]
#[ignore = "requires Docker"]
async fn test_consumer_commit_and_resume_verified() {
    use krafka::consumer::AutoOffsetReset;

    let bootstrap_servers = kafka();

    let topic = "commit-verify-topic";
    let group_id = "commit-verify-group";
    create_topic(&bootstrap_servers, topic, 1).await;

    let producer = krafka::Kafka::builder(&bootstrap_servers)
        .connect()
        .await
        .expect("Failed to create producer")
        .producer()
        .build()
        .await
        .expect("Failed to create producer");

    for i in 0..10 {
        let _ = producer
            .send(krafka::Record::new(
                topic,
                bytes::Bytes::copy_from_slice(format!("msg-{}", i).as_bytes()),
            ))
            .await
            .expect("send failed");
    }
    producer.close().await.unwrap();

    // First consumer: read all and commit
    {
        let consumer = krafka::Kafka::builder(&bootstrap_servers)
            .connect()
            .await
            .expect("Failed to create consumer")
            .consumer(group_id)
            .auto_offset_reset(AutoOffsetReset::Earliest)
            .enable_auto_commit(false)
            .build()
            .await
            .expect("Failed to create consumer");

        consumer
            .subscribe(&[topic])
            .await
            .expect("Failed to subscribe");

        let all = poll_for_records(&consumer, 10, Duration::from_secs(3), 8).await;
        assert_eq!(all.len(), 10, "Should read all 10 messages");
        consumer.commit().await.expect("commit failed");
        consumer.close().await.expect("consumer close");
    }

    // Second consumer: should get NO new messages (all committed)
    {
        let consumer = krafka::Kafka::builder(&bootstrap_servers)
            .connect()
            .await
            .expect("Failed to create consumer")
            .consumer(group_id)
            .auto_offset_reset(AutoOffsetReset::Earliest)
            .enable_auto_commit(false)
            .build()
            .await
            .expect("Failed to create consumer");

        consumer
            .subscribe(&[topic])
            .await
            .expect("Failed to subscribe");

        // Poll a few times to let rebalance complete, then verify no new records
        let records = poll_for_records(&consumer, 0, Duration::from_secs(3), 3).await;
        assert!(
            records.is_empty(),
            "Second consumer should get 0 records after commit, got {}",
            records.len()
        );
        consumer.close().await.expect("consumer close");
    }
}

/// Test consumer recv() streaming API.
#[tokio::test]
#[ignore = "requires Docker"]
async fn test_consumer_recv() {
    use krafka::consumer::AutoOffsetReset;

    let bootstrap_servers = kafka();

    let topic = "recv-test-topic";
    create_topic(&bootstrap_servers, topic, 1).await;

    let producer = krafka::Kafka::builder(&bootstrap_servers)
        .connect()
        .await
        .unwrap()
        .producer()
        .build()
        .await
        .unwrap();

    for i in 0..3 {
        let _ = producer
            .send(krafka::Record::new(
                topic,
                bytes::Bytes::copy_from_slice(format!("recv-msg-{}", i).as_bytes()),
            ))
            .await
            .unwrap();
    }
    producer.close().await.unwrap();

    let consumer = krafka::Kafka::builder(&bootstrap_servers)
        .connect()
        .await
        .unwrap()
        .consumer("recv-test-group")
        .auto_offset_reset(AutoOffsetReset::Earliest)
        .build()
        .await
        .unwrap();

    consumer.subscribe(&[topic]).await.unwrap();

    // Use recv() to receive individual records
    let mut received = Vec::new();
    for _ in 0..3 {
        match tokio::time::timeout(Duration::from_secs(30), consumer.recv()).await {
            Ok(Ok(Some(record))) => received.push(record),
            Ok(Ok(None)) => break,
            Ok(Err(e)) => panic!("recv error: {e}"),
            Err(_elapsed) => panic!("recv timed out before collecting expected records"),
        }
    }

    assert_eq!(received.len(), 3, "Should receive 3 records via recv()");
    consumer.close().await.expect("consumer close");
}

/// Test producer flush() forces pending messages to be sent.
#[tokio::test]
#[ignore = "requires Docker"]
async fn test_producer_flush() {
    use krafka::consumer::AutoOffsetReset;

    use std::sync::Arc;

    let bootstrap_servers = kafka();

    let topic = "flush-test-topic";
    create_topic(&bootstrap_servers, topic, 1).await;

    let producer = Arc::new(
        krafka::Kafka::builder(&bootstrap_servers)
            .connect()
            .await
            .unwrap()
            .producer()
            .linger(Duration::from_secs(30))
            .build()
            .await
            .unwrap(),
    );

    // Spawn sends in background — they block until the batch is flushed
    let mut handles = Vec::new();
    for i in 0..5 {
        let p = Arc::clone(&producer);
        let t = topic.to_string();
        handles.push(tokio::spawn(async move {
            let _ = p
                .send(krafka::Record::new(
                    &t,
                    bytes::Bytes::copy_from_slice(format!("flush-{}", i).as_bytes()),
                ))
                .await
                .unwrap();
        }));
    }

    // Give the accumulator time to receive all records
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Explicit flush should ensure all messages are sent
    producer.flush().await.expect("flush failed");

    // All spawned sends should now complete
    for h in handles {
        h.await.unwrap();
    }

    producer.close().await.unwrap();

    let consumer = krafka::Kafka::builder(&bootstrap_servers)
        .connect()
        .await
        .unwrap()
        .consumer("flush-test-group")
        .auto_offset_reset(AutoOffsetReset::Earliest)
        .build()
        .await
        .unwrap();

    consumer.subscribe(&[topic]).await.unwrap();

    let all = poll_for_records(&consumer, 5, Duration::from_secs(3), 8).await;
    assert_eq!(all.len(), 5, "All 5 flushed messages should be received");
    consumer.close().await.expect("consumer close");
}

/// Test admin describe_groups returns member information.
#[tokio::test]
#[ignore = "requires Docker"]
async fn test_admin_describe_consumer_group() {
    use krafka::consumer::AutoOffsetReset;

    let bootstrap_servers = kafka();

    let topic = "describe-group-topic";
    let group_id = "describe-group-test";
    create_topic(&bootstrap_servers, topic, 1).await;

    let consumer = krafka::Kafka::builder(&bootstrap_servers)
        .connect()
        .await
        .unwrap()
        .consumer(group_id)
        .auto_offset_reset(AutoOffsetReset::Earliest)
        .build()
        .await
        .unwrap();

    consumer.subscribe(&[topic]).await.unwrap();

    // Drive the rebalance until the consumer actually has partitions assigned.
    // On Kafka 3.9 under CI load, JoinGroup/SyncGroup can take many polls.
    let mut got_assignment = false;
    for i in 0..20 {
        let _ = consumer.poll(Duration::from_secs(3)).await;
        let assignment = consumer.assignment().await;
        if !assignment.is_empty() {
            eprintln!("Consumer got assignment after {} poll(s)", i + 1);
            got_assignment = true;
            break;
        }
    }
    assert!(
        got_assignment,
        "Consumer should have received partition assignment"
    );

    // Let the group stabilize — poll several more times so that the
    // coordinator finishes SyncGroup and at least one heartbeat succeeds.
    // Without this, Kafka 3.9 under CI load may not report the member yet.
    for _ in 0..5 {
        let _ = consumer.poll(Duration::from_secs(2)).await;
    }

    // Verify the group is listed by the broker before describing it.
    let admin = krafka::Kafka::builder(&bootstrap_servers)
        .connect()
        .await
        .unwrap()
        .admin();

    let listed: Vec<_> = admin
        .list_consumer_groups(Default::default())
        .await
        .unwrap()
        .into_values()
        .flat_map(|per_broker| per_broker.unwrap_or_default())
        .collect();
    eprintln!(
        "list_consumer_groups: [{}]",
        listed
            .iter()
            .map(|g| format!("{}({})", g.group_id, g.protocol_type))
            .collect::<Vec<_>>()
            .join(", ")
    );

    // Retry describe_consumer_groups — the broker may take a moment to
    // report the member after the rebalance completes. Keep polling the
    // consumer between attempts so it stays in the group (heartbeats).
    let mut descriptions = Vec::new();
    for attempt in 0..30 {
        // Poll first to keep the consumer alive and heartbeating
        let _ = consumer.poll(Duration::from_secs(2)).await;

        descriptions = admin
            .describe_consumer_groups([group_id], Default::default())
            .await
            .expect("describe_consumer_groups failed")
            .into_values()
            .map(|d| d.expect("describe_consumer_groups failed"))
            .collect();
        if descriptions.len() == 1 && !descriptions[0].members.is_empty() {
            eprintln!(
                "describe_consumer_groups succeeded on attempt {}/30: {} members, state={}, type={:?}",
                attempt + 1,
                descriptions[0].members.len(),
                descriptions[0].state,
                descriptions[0].group_type,
            );
            break;
        }
        eprintln!(
            "describe_consumer_groups attempt {}/30: {} members, state={}, type={:?}, retrying...",
            attempt + 1,
            descriptions.first().map_or(0, |d| d.members.len()),
            descriptions
                .first()
                .map_or("N/A".to_string(), |d| d.state.clone()),
            descriptions.first().map(|d| d.group_type.clone()),
        );
    }

    assert_eq!(descriptions.len(), 1);
    assert_eq!(descriptions[0].group_id, group_id);
    assert!(
        !descriptions[0].members.is_empty(),
        "Group should have at least 1 member"
    );
    consumer.close().await.expect("consumer close");
}

/// Test consumer close() properly leaves the group.
#[tokio::test]
#[ignore = "requires Docker"]
async fn test_consumer_close_leaves_group() {
    use krafka::consumer::AutoOffsetReset;

    let bootstrap_servers = kafka();

    let topic = "close-leaves-group-topic";
    let group_id = "close-leaves-group";
    create_topic(&bootstrap_servers, topic, 1).await;

    let consumer = krafka::Kafka::builder(&bootstrap_servers)
        .connect()
        .await
        .unwrap()
        .consumer(group_id)
        .auto_offset_reset(AutoOffsetReset::Earliest)
        .build()
        .await
        .unwrap();

    consumer.subscribe(&[topic]).await.unwrap();
    // Poll multiple times to ensure group join completes
    let _ = poll_for_records(&consumer, 0, Duration::from_secs(3), 3).await;

    // Explicitly close
    consumer.close().await.expect("consumer close");
    tokio::time::sleep(Duration::from_secs(2)).await;

    let admin = krafka::Kafka::builder(&bootstrap_servers)
        .connect()
        .await
        .unwrap()
        .admin();

    let descriptions: Vec<_> = admin
        .describe_consumer_groups([group_id], Default::default())
        .await
        .expect("describe_consumer_groups failed")
        .into_values()
        .map(|d| d.expect("describe_consumer_groups failed"))
        .collect();

    assert!(
        !descriptions.is_empty(),
        "describe_consumer_groups should return the group even after close"
    );
    assert!(
        descriptions[0].members.is_empty(),
        "After close(), group should have no active members, got {} member(s)",
        descriptions[0].members.len()
    );
}

/// Test empty value messages roundtrip correctly.
#[tokio::test]
#[ignore = "requires Docker"]
async fn test_empty_value_message() {
    use krafka::consumer::AutoOffsetReset;

    let bootstrap_servers = kafka();

    let topic = "empty-value-topic";
    create_topic(&bootstrap_servers, topic, 1).await;

    let producer = krafka::Kafka::builder(&bootstrap_servers)
        .connect()
        .await
        .unwrap()
        .producer()
        .build()
        .await
        .unwrap();

    let metadata = producer
        .send(krafka::Record::new(topic, "").key("key"))
        .await
        .unwrap();
    assert!(metadata.offset >= 0);
    producer.close().await.unwrap();

    let consumer = krafka::Kafka::builder(&bootstrap_servers)
        .connect()
        .await
        .unwrap()
        .consumer("empty-value-group")
        .auto_offset_reset(AutoOffsetReset::Earliest)
        .build()
        .await
        .unwrap();

    consumer.subscribe(&[topic]).await.unwrap();

    let records = poll_for_records(&consumer, 1, Duration::from_secs(3), 5).await;
    assert!(!records.is_empty(), "Should receive the empty-value record");
    assert_eq!(
        records[0].value.as_ref().map(|v| v.len()),
        Some(0),
        "Empty value should be preserved as zero-length"
    );
    consumer.close().await.expect("consumer close");
}

/// Test admin describe_configs returns broker configuration.
#[tokio::test]
#[ignore = "requires Docker"]
async fn test_admin_describe_broker_config() {
    use krafka::admin::ConfigResource;

    let bootstrap_servers = kafka();

    let admin = krafka::Kafka::builder(&bootstrap_servers)
        .connect()
        .await
        .unwrap()
        .admin();

    let cluster = admin.describe_cluster(Default::default()).await.unwrap();
    let broker_id = cluster.brokers[0].id;

    let configs = admin
        .describe_configs([ConfigResource::broker(broker_id)], Default::default())
        .await
        .expect("describe_configs failed")
        .remove(&ConfigResource::broker(broker_id))
        .expect("the broker is answered")
        .expect("describe_configs failed");

    assert!(!configs.is_empty(), "Broker should have config entries");

    assert!(
        configs.iter().any(|c| c.name == "log.retention.hours"
            || c.name == "log.retention.ms"
            || c.name == "num.partitions"),
        "Should contain standard broker configs"
    );
}

/// Test many-partition topic with message distribution.
#[tokio::test]
#[ignore = "requires Docker"]
async fn test_many_partitions_topic() {
    use krafka::admin::NewTopic;
    use krafka::consumer::AutoOffsetReset;

    let bootstrap_servers = kafka();

    let admin = krafka::Kafka::builder(&bootstrap_servers)
        .connect()
        .await
        .unwrap()
        .admin();

    let topic = "many-partitions-topic";
    admin
        .create_topics(
            vec![NewTopic::new(topic, 12, 1).unwrap()],
            Default::default(),
        )
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_secs(2)).await;

    let producer = krafka::Kafka::builder(&bootstrap_servers)
        .connect()
        .await
        .unwrap()
        .producer()
        .build()
        .await
        .unwrap();

    // Send 60 messages with keys to distribute across partitions
    for i in 0..60 {
        let _ = producer
            .send(
                krafka::Record::new(topic, "v")
                    .key(bytes::Bytes::copy_from_slice(format!("k-{}", i).as_bytes())),
            )
            .await
            .unwrap();
    }
    producer.close().await.unwrap();

    let consumer = krafka::Kafka::builder(&bootstrap_servers)
        .connect()
        .await
        .unwrap()
        .consumer("many-partitions-group")
        .auto_offset_reset(AutoOffsetReset::Earliest)
        .build()
        .await
        .unwrap();

    consumer.subscribe(&[topic]).await.unwrap();

    let all = poll_for_records(&consumer, 60, Duration::from_secs(3), 20).await;
    assert_eq!(all.len(), 60, "All 60 messages should be received");

    // Verify messages came from multiple partitions
    let partitions: std::collections::HashSet<_> = all.iter().map(|r| r.partition).collect();
    assert!(
        partitions.len() > 3,
        "60 keys across 12 partitions should hit many partitions, got {}",
        partitions.len()
    );
    consumer.close().await.expect("consumer close");
}

/// Test consumer pause/resume with verified assertions.
#[tokio::test]
#[ignore = "requires Docker"]
async fn test_consumer_pause_resume_verified() {
    use krafka::consumer::AutoOffsetReset;

    let bootstrap_servers = kafka();

    let topic = "pause-verify-topic";
    create_topic(&bootstrap_servers, topic, 1).await;

    let producer = krafka::Kafka::builder(&bootstrap_servers)
        .connect()
        .await
        .unwrap()
        .producer()
        .build()
        .await
        .unwrap();

    for i in 0..10 {
        let _ = producer
            .send(krafka::Record::new(
                topic,
                bytes::Bytes::copy_from_slice(format!("pv-{}", i).as_bytes()),
            ))
            .await
            .unwrap();
    }
    producer.close().await.unwrap();

    let consumer = krafka::Kafka::builder(&bootstrap_servers)
        .connect()
        .await
        .unwrap()
        .consumer("pause-verify-group")
        .auto_offset_reset(AutoOffsetReset::Earliest)
        .enable_auto_commit(false)
        .build()
        .await
        .unwrap();

    consumer.subscribe(&[topic]).await.unwrap();

    // Poll to get assignment (first poll may only complete rebalance)
    let _ = poll_for_records(&consumer, 0, Duration::from_secs(3), 3).await;

    // Pause
    consumer.pause(topic, &[0]).await;

    let paused = consumer.paused_partitions().await;
    assert!(
        paused.contains(&(topic.to_string(), 0)),
        "Partition 0 should be paused"
    );

    // Resume
    consumer.resume(topic, &[0]).await;

    let paused = consumer.paused_partitions().await;
    assert!(
        !paused.contains(&(topic.to_string(), 0)),
        "Partition 0 should no longer be paused"
    );
    consumer.close().await.expect("consumer close");
}

/// Test consumer seek with verified offset positioning.
#[tokio::test]
#[ignore = "requires Docker"]
async fn test_consumer_seek_verified() {
    use krafka::consumer::AutoOffsetReset;

    let bootstrap_servers = kafka();

    let topic = "seek-verify-topic";
    create_topic(&bootstrap_servers, topic, 1).await;

    let producer = krafka::Kafka::builder(&bootstrap_servers)
        .connect()
        .await
        .unwrap()
        .producer()
        .build()
        .await
        .unwrap();

    for i in 0..10 {
        let _ = producer
            .send(krafka::Record::new(
                topic,
                bytes::Bytes::copy_from_slice(format!("msg-{}", i).as_bytes()),
            ))
            .await
            .unwrap();
    }
    producer.close().await.unwrap();

    let consumer = krafka::Kafka::builder(&bootstrap_servers)
        .connect()
        .await
        .unwrap()
        .consumer("seek-verify-group")
        .auto_offset_reset(AutoOffsetReset::Earliest)
        .enable_auto_commit(false)
        .build()
        .await
        .unwrap();

    consumer.subscribe(&[topic]).await.unwrap();

    // First poll to get assignment (rebalance may consume first poll)
    let _ = poll_for_records(&consumer, 0, Duration::from_secs(3), 3).await;

    // Seek to offset 5
    consumer.seek(topic, 0, 5).await.expect("seek failed");

    let records = poll_for_records(&consumer, 1, Duration::from_secs(3), 5).await;

    assert!(!records.is_empty(), "Should receive records after seek");
    assert_eq!(
        records[0].value_str(),
        Some("msg-5"),
        "First record after seek to offset 5 should be msg-5"
    );
    consumer.close().await.expect("consumer close");
}

/// Test topic creation with custom configs.
#[tokio::test]
#[ignore = "requires Docker"]
async fn test_admin_create_topic_with_config() {
    use krafka::admin::NewTopic;

    let bootstrap_servers = kafka();

    let admin = krafka::Kafka::builder(&bootstrap_servers)
        .connect()
        .await
        .unwrap()
        .admin();

    let topic = "configured-topic";
    let new_topic = NewTopic::new(topic, 3, 1)
        .unwrap()
        .with_config("retention.ms", "3600000")
        .with_config("cleanup.policy", "compact");

    admin
        .create_topics(vec![new_topic], Default::default())
        .await
        .unwrap();

    tokio::time::sleep(Duration::from_secs(1)).await;

    let configs = admin
        .describe_configs(
            [krafka::admin::ConfigResource::topic(topic)],
            Default::default(),
        )
        .await
        .unwrap()
        .remove(&krafka::admin::ConfigResource::topic(topic))
        .unwrap()
        .unwrap();
    let retention = configs.iter().find(|c| c.name == "retention.ms");
    assert!(retention.is_some(), "Should have retention.ms config");
    assert_eq!(
        retention.unwrap().value.as_deref(),
        Some("3600000"),
        "retention.ms should be 3600000"
    );
}

/// Test consumer metrics are available after consuming.
#[tokio::test]
#[ignore = "requires Docker"]
async fn test_consumer_metrics() {
    use krafka::consumer::AutoOffsetReset;

    let bootstrap_servers = kafka();

    let topic = "consumer-metrics-topic";
    create_topic(&bootstrap_servers, topic, 1).await;

    let producer = krafka::Kafka::builder(&bootstrap_servers)
        .connect()
        .await
        .unwrap()
        .producer()
        .build()
        .await
        .unwrap();

    for i in 0..5 {
        let _ = producer
            .send(krafka::Record::new(
                topic,
                bytes::Bytes::copy_from_slice(format!("m-{}", i).as_bytes()),
            ))
            .await
            .unwrap();
    }
    producer.close().await.unwrap();

    let consumer = krafka::Kafka::builder(&bootstrap_servers)
        .connect()
        .await
        .unwrap()
        .consumer("consumer-metrics-group")
        .auto_offset_reset(AutoOffsetReset::Earliest)
        .build()
        .await
        .unwrap();

    consumer.subscribe(&[topic]).await.unwrap();

    let all = poll_for_records(&consumer, 5, Duration::from_secs(3), 8).await;
    let _total = all.len();

    let metrics = consumer.metrics();
    assert!(
        metrics.consumer.records_received > 0,
        "Should have received records"
    );
    assert!(
        metrics.consumer.bytes_received > 0,
        "Should have received bytes"
    );
    consumer.close().await.expect("consumer close");
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn test_offsets_for_times_and_watermarks_and_metadata() {
    use krafka::consumer::AutoOffsetReset;

    let bootstrap_servers = kafka();

    let topic = "offsets-times-topic";
    create_topic(&bootstrap_servers, topic, 2).await;

    let producer = krafka::Kafka::builder(&bootstrap_servers)
        .connect()
        .await
        .expect("Failed to create producer")
        .producer()
        .build()
        .await
        .expect("Failed to create producer");

    // Send a few messages across both partitions using different keys.
    const N: usize = 10;
    for i in 0..N {
        let key = format!("k-{}", i);
        let _ = producer
            .send(
                krafka::Record::new(
                    topic,
                    bytes::Bytes::copy_from_slice(format!("v-{}", i).as_bytes()),
                )
                .key(bytes::Bytes::copy_from_slice(key.as_bytes())),
            )
            .await
            .expect("Failed to send message");
    }
    producer.close().await.unwrap();

    let consumer = krafka::Kafka::builder(&bootstrap_servers)
        .connect()
        .await
        .expect("Failed to create consumer")
        .consumer("offsets-times-group")
        .auto_offset_reset(AutoOffsetReset::Earliest)
        .build()
        .await
        .expect("Failed to create consumer");

    // fetch_metadata(Some(topic)) should find the topic with 2 partitions.
    let md = consumer
        .fetch_metadata(Some(topic))
        .await
        .expect("fetch_metadata failed");
    assert!(!md.brokers.is_empty(), "expected at least one broker");
    let topic_info = md
        .topics
        .iter()
        .find(|t| t.name == topic)
        .expect("topic missing from fetch_metadata");
    assert_eq!(topic_info.partition_count(), 2);

    // fetch_metadata(None) should include the topic.
    let all = consumer
        .fetch_metadata(None)
        .await
        .expect("fetch_metadata(None) failed");
    assert!(all.topics.iter().any(|t| t.name == topic));

    // fetch_watermarks: low should be 0, high should be > 0 and the two
    // partitions together should account for all N messages.
    let mut total_high = 0i64;
    for p in topic_info.partitions_iter() {
        let (low, high) = consumer
            .fetch_watermarks(topic, p.partition)
            .await
            .expect("fetch_watermarks failed");
        assert_eq!(
            low, 0,
            "low watermark should be 0 for partition {}",
            p.partition
        );
        assert!(high >= 0, "high watermark should be non-negative");
        total_high += high;
    }
    assert_eq!(
        total_high, N as i64,
        "watermarks should sum to message count"
    );

    // offsets_for_times with timestamp 0 should return offset 0 for every
    // partition (all messages are at or after epoch).
    let offsets_at_zero = consumer
        .offsets_for_times_for_topic(topic, 0)
        .await
        .expect("offsets_for_times_for_topic failed");
    assert_eq!(offsets_at_zero.len(), 2);
    for result in offsets_at_zero.values() {
        let offset = result.as_ref().expect("partition offset should be Ok");
        assert_eq!(*offset, 0, "expected offset 0 at timestamp 0");
    }

    // offsets_for_times with a future timestamp should return -1 per
    // partition (no message at or after).
    let future_ts = i64::MAX / 2;
    let offsets_future = consumer
        .offsets_for_times_for_topic(topic, future_ts)
        .await
        .expect("offsets_for_times_for_topic (future) failed");
    for result in offsets_future.values() {
        let offset = result.as_ref().expect("partition offset should be Ok");
        assert_eq!(*offset, -1, "expected -1 for far-future timestamp");
    }

    // Lower-level offsets_for_times with an explicit pair list.
    let pairs: Vec<(&str, i32)> = topic_info
        .partitions_iter()
        .map(|p| (topic, p.partition))
        .collect();
    let offsets_pairs = consumer.offsets_for_times(&pairs, 0).await;
    assert_eq!(offsets_pairs.len(), 2);
    for ((t, _p), result) in &offsets_pairs {
        assert_eq!(t, topic);
        assert_eq!(*result.as_ref().expect("partition offset should be Ok"), 0);
    }

    consumer.close().await.expect("consumer close");
}

// ---------------------------------------------------------------------------
// Transactional Producer Tests
// ---------------------------------------------------------------------------

/// Committed transactions are visible to read-committed consumers.
///
/// Flow: init → begin → send → commit → consume (read_committed) → assert message present.
#[tokio::test]
#[ignore = "requires Docker"]
async fn test_transactional_producer_commit() {
    use krafka::consumer::{AutoOffsetReset, IsolationLevel};

    let bootstrap_servers = kafka();

    let topic = "txn-commit-topic";
    create_topic(&bootstrap_servers, topic, 1).await;

    let producer = krafka::Kafka::builder(&bootstrap_servers)
        .connect()
        .await
        .expect("Failed to create transactional producer")
        .producer()
        .build_transactional("txn-commit-test")
        .await
        .expect("Failed to create transactional producer");

    producer.begin().expect("begin failed");

    let metadata = producer
        .send(krafka::Record::new(topic, "committed-value").key("key"))
        .await
        .expect("send failed");
    assert!(
        metadata.offset >= 0,
        "Expected a valid offset, got {}",
        metadata.offset
    );

    producer.commit().await.expect("commit failed");
    producer.close().await.unwrap();

    // Read with read_committed isolation — should see the committed message.
    let consumer = krafka::Kafka::builder(&bootstrap_servers)
        .connect()
        .await
        .expect("Failed to create consumer")
        .consumer("txn-commit-consumer")
        .auto_offset_reset(AutoOffsetReset::Earliest)
        .isolation_level(IsolationLevel::ReadCommitted)
        .build()
        .await
        .expect("Failed to create consumer");

    consumer
        .subscribe(&[topic])
        .await
        .expect("subscribe failed");

    let records = poll_for_records(&consumer, 1, Duration::from_secs(5), 10).await;
    assert_eq!(
        records.len(),
        1,
        "Should receive exactly one committed message"
    );
    assert_eq!(
        records[0].value.as_deref(),
        Some(b"committed-value" as &[u8]),
        "Value mismatch"
    );
    consumer.close().await.expect("consumer close");
}

/// Aborted transactions are hidden from read-committed consumers.
///
/// Flow: init → begin → send → abort → begin → send → commit.
/// The read-committed consumer should receive only the committed message.
#[tokio::test]
#[ignore = "requires Docker"]
async fn test_transactional_producer_abort() {
    use krafka::consumer::{AutoOffsetReset, IsolationLevel};

    let bootstrap_servers = kafka();

    let topic = "txn-abort-topic";
    create_topic(&bootstrap_servers, topic, 1).await;

    let producer = krafka::Kafka::builder(&bootstrap_servers)
        .connect()
        .await
        .expect("Failed to create transactional producer")
        .producer()
        .build_transactional("txn-abort-test")
        .await
        .expect("Failed to create transactional producer");

    // First transaction: send and ABORT.
    producer.begin().expect("begin failed");
    let _ = producer
        .send(krafka::Record::new(topic, "aborted-value").key("key-aborted"))
        .await
        .expect("send (to-be-aborted) failed");
    producer.abort().await.expect("abort_transaction failed");

    // Second transaction: send and COMMIT.
    producer.begin().expect("begin failed");
    let _ = producer
        .send(krafka::Record::new(topic, "committed-value").key("key-committed"))
        .await
        .expect("send failed");
    producer.commit().await.expect("commit failed");

    producer.close().await.unwrap();

    // Read with read_committed — should see ONLY the committed message.
    let consumer = krafka::Kafka::builder(&bootstrap_servers)
        .connect()
        .await
        .expect("Failed to create consumer")
        .consumer("txn-abort-consumer")
        .auto_offset_reset(AutoOffsetReset::Earliest)
        .isolation_level(IsolationLevel::ReadCommitted)
        .build()
        .await
        .expect("Failed to create consumer");

    consumer
        .subscribe(&[topic])
        .await
        .expect("subscribe failed");

    // Poll enough times to drain — if the aborted message leaks we'll catch it.
    let mut all_records = Vec::new();
    for _ in 0..10 {
        let records = consumer
            .poll(Duration::from_secs(2))
            .await
            .expect("poll failed");
        all_records.extend(records);
        if !all_records.is_empty() {
            break;
        }
    }

    assert_eq!(
        all_records.len(),
        1,
        "read_committed consumer should see exactly one message (the committed one)"
    );
    assert_eq!(
        all_records[0].value.as_deref(),
        Some(b"committed-value" as &[u8]),
        "Only the committed message should be visible"
    );
    consumer.close().await.expect("consumer close");
}

/// Transactions spanning multiple partitions are committed atomically.
///
/// Sends two messages with keys that hash to different partitions within one
/// transaction. Both messages should appear in a read-committed consumer.
#[tokio::test]
#[ignore = "requires Docker"]
async fn test_transactional_producer_multi_partition() {
    use krafka::consumer::{AutoOffsetReset, IsolationLevel};

    let bootstrap_servers = kafka();

    let topic = "txn-multi-part-topic";
    create_topic(&bootstrap_servers, topic, 2).await;

    let producer = krafka::Kafka::builder(&bootstrap_servers)
        .connect()
        .await
        .expect("Failed to create transactional producer")
        .producer()
        .build_transactional("txn-multi-part-test")
        .await
        .expect("Failed to create transactional producer");

    producer.begin().expect("begin failed");

    let _ = producer
        .send(krafka::Record::new(topic, "value-alpha").key("key-alpha"))
        .await
        .expect("send alpha failed");
    let _ = producer
        .send(krafka::Record::new(topic, "value-beta").key("key-beta"))
        .await
        .expect("send beta failed");

    producer.commit().await.expect("commit failed");
    producer.close().await.unwrap();

    let consumer = krafka::Kafka::builder(&bootstrap_servers)
        .connect()
        .await
        .expect("Failed to create consumer")
        .consumer("txn-multi-part-consumer")
        .auto_offset_reset(AutoOffsetReset::Earliest)
        .isolation_level(IsolationLevel::ReadCommitted)
        .build()
        .await
        .expect("Failed to create consumer");

    consumer
        .subscribe(&[topic])
        .await
        .expect("subscribe failed");

    let records = poll_for_records(&consumer, 2, Duration::from_secs(5), 10).await;
    assert_eq!(
        records.len(),
        2,
        "Should receive both messages from the transaction"
    );

    let values: std::collections::HashSet<String> = records
        .iter()
        .filter_map(|r| {
            r.value
                .as_ref()
                .map(|v| String::from_utf8_lossy(v).into_owned())
        })
        .collect();
    assert!(values.contains("value-alpha"), "value-alpha missing");
    assert!(values.contains("value-beta"), "value-beta missing");

    consumer.close().await.expect("consumer close");
}

/// Multiple transactions in sequence: commit, abort, commit.
///
/// Verifies the producer can cycle through multiple transactions correctly and
/// that only committed messages are visible.
#[tokio::test]
#[ignore = "requires Docker"]
async fn test_transactional_producer_multiple_transactions() {
    use krafka::consumer::{AutoOffsetReset, IsolationLevel};

    let bootstrap_servers = kafka();

    let topic = "txn-multi-txn-topic";
    create_topic(&bootstrap_servers, topic, 1).await;

    let producer = krafka::Kafka::builder(&bootstrap_servers)
        .connect()
        .await
        .expect("Failed to create transactional producer")
        .producer()
        .build_transactional("txn-multi-txn-test")
        .await
        .expect("Failed to create transactional producer");

    // Txn 1: commit.
    producer.begin().expect("begin 1 failed");
    let _ = producer
        .send(krafka::Record::new(topic, "v1").key("k1"))
        .await
        .expect("send 1 failed");
    producer.commit().await.expect("commit 1 failed");

    // Txn 2: abort.
    producer.begin().expect("begin 2 failed");
    let _ = producer
        .send(krafka::Record::new(topic, "v2-aborted").key("k2"))
        .await
        .expect("send 2 failed");
    producer.abort().await.expect("abort 2 failed");

    // Txn 3: commit.
    producer.begin().expect("begin 3 failed");
    let _ = producer
        .send(krafka::Record::new(topic, "v3").key("k3"))
        .await
        .expect("send 3 failed");
    producer.commit().await.expect("commit 3 failed");

    producer.close().await.unwrap();

    let consumer = krafka::Kafka::builder(&bootstrap_servers)
        .connect()
        .await
        .expect("Failed to create consumer")
        .consumer("txn-multi-txn-consumer")
        .auto_offset_reset(AutoOffsetReset::Earliest)
        .isolation_level(IsolationLevel::ReadCommitted)
        .build()
        .await
        .expect("Failed to create consumer");

    consumer
        .subscribe(&[topic])
        .await
        .expect("subscribe failed");

    let records = poll_for_records(&consumer, 2, Duration::from_secs(5), 10).await;
    assert_eq!(
        records.len(),
        2,
        "Only committed messages should be visible"
    );

    let values: Vec<String> = records
        .iter()
        .filter_map(|r| {
            r.value
                .as_ref()
                .map(|v| String::from_utf8_lossy(v).into_owned())
        })
        .collect();
    assert!(values.contains(&"v1".to_string()), "v1 missing");
    assert!(values.contains(&"v3".to_string()), "v3 missing");
    assert!(
        !values.contains(&"v2-aborted".to_string()),
        "aborted message v2 leaked"
    );

    consumer.close().await.expect("consumer close");
}

/// Producer epoch fencing: a new producer with the same transactional ID bumps
/// the epoch, fencing any zombie producers.
///
/// Verifies that `init_transactions()` with the same `transactional_id` assigns
/// a higher epoch, and the new producer can commit successfully.
#[tokio::test]
#[ignore = "requires Docker"]
async fn test_transactional_producer_epoch_fencing() {
    use krafka::consumer::{AutoOffsetReset, IsolationLevel};

    let bootstrap_servers = kafka();

    let topic = "txn-fencing-topic";
    create_topic(&bootstrap_servers, topic, 1).await;
    let txn_id = "txn-fencing-test";

    // Producer 1: init and commit one message.
    let producer1 = krafka::Kafka::builder(&bootstrap_servers)
        .connect()
        .await
        .expect("Failed to create producer 1")
        .producer()
        .build_transactional(txn_id)
        .await
        .expect("Failed to create producer 1");

    let epoch1 = producer1.producer_epoch();

    producer1.begin().expect("begin 1 failed");
    let _ = producer1
        .send(krafka::Record::new(topic, "v1").key("k1"))
        .await
        .expect("send 1 failed");
    producer1.commit().await.expect("commit 1 failed");
    producer1.close().await.unwrap();

    // Producer 2: same transactional_id → broker bumps epoch.
    let producer2 = krafka::Kafka::builder(&bootstrap_servers)
        .connect()
        .await
        .expect("Failed to create producer 2")
        .producer()
        .build_transactional(txn_id)
        .await
        .expect("Failed to create producer 2");

    let epoch2 = producer2.producer_epoch();

    assert!(
        epoch2 > epoch1,
        "Producer 2 should have a higher epoch ({epoch2}) than producer 1 ({epoch1})"
    );

    producer2.begin().expect("begin 2 failed");
    let _ = producer2
        .send(krafka::Record::new(topic, "v2").key("k2"))
        .await
        .expect("send 2 failed");
    producer2.commit().await.expect("commit 2 failed");
    producer2.close().await.unwrap();

    // Both committed messages should be readable.
    let consumer = krafka::Kafka::builder(&bootstrap_servers)
        .connect()
        .await
        .expect("Failed to create consumer")
        .consumer("txn-fencing-consumer")
        .auto_offset_reset(AutoOffsetReset::Earliest)
        .isolation_level(IsolationLevel::ReadCommitted)
        .build()
        .await
        .expect("Failed to create consumer");

    consumer
        .subscribe(&[topic])
        .await
        .expect("subscribe failed");

    let records = poll_for_records(&consumer, 2, Duration::from_secs(5), 10).await;
    assert_eq!(
        records.len(),
        2,
        "Both committed messages should be readable after fencing"
    );

    consumer.close().await.expect("consumer close");
}

/// `send_offsets`: Consume-Transform-Produce (EOS / read-process-write).
///
/// 1. Write source messages to `src-topic` with a regular producer.
/// 2. Transactional consumer reads messages and commits offset + result
///    atomically via `send_offsets`.
/// 3. Verify the destination topic contains the transformed messages and the
///    committed consumer offset allows resumption without reprocessing.
#[tokio::test]
#[ignore = "requires Docker"]
async fn test_transactional_send_offsets() {
    use krafka::consumer::{AutoOffsetReset, IsolationLevel};
    use krafka::producer::TopicPartitionOffset;

    let bootstrap_servers = kafka();

    let src_topic = "txn-eos-src-topic";
    let dst_topic = "txn-eos-dst-topic";
    let group_id = "txn-eos-group";
    create_topic(&bootstrap_servers, src_topic, 1).await;
    create_topic(&bootstrap_servers, dst_topic, 1).await;

    // Step 1: Write source messages with a regular producer.
    let regular_producer = krafka::Kafka::builder(&bootstrap_servers)
        .connect()
        .await
        .expect("Failed to create regular producer")
        .producer()
        .build()
        .await
        .expect("Failed to create regular producer");

    for i in 0..3u32 {
        let _ = regular_producer
            .send(
                krafka::Record::new(
                    src_topic,
                    bytes::Bytes::copy_from_slice(format!("src-{i}").as_bytes()),
                )
                .key(bytes::Bytes::copy_from_slice(format!("k{i}").as_bytes())),
            )
            .await
            .expect("send to src failed");
    }
    regular_producer.close().await.unwrap();

    // Step 2: Create the read-committed source consumer (no group auto-commit).
    let src_consumer = krafka::Kafka::builder(&bootstrap_servers)
        .connect()
        .await
        .expect("Failed to create source consumer")
        .consumer(group_id)
        .auto_offset_reset(AutoOffsetReset::Earliest)
        .enable_auto_commit(false)
        .isolation_level(IsolationLevel::ReadCommitted)
        .build()
        .await
        .expect("Failed to create source consumer");

    src_consumer
        .subscribe(&[src_topic])
        .await
        .expect("subscribe failed");

    let src_records = poll_for_records(&src_consumer, 3, Duration::from_secs(5), 10).await;
    assert_eq!(src_records.len(), 3, "Should read 3 source messages");

    // Step 3: Process each message transactionally.
    let txn_producer = krafka::Kafka::builder(&bootstrap_servers)
        .connect()
        .await
        .expect("Failed to create transactional producer")
        .producer()
        .build_transactional("txn-eos-producer")
        .await
        .expect("Failed to create transactional producer");

    for record in &src_records {
        let transformed_value = record
            .value
            .as_ref()
            .map(|v| format!("processed:{}", String::from_utf8_lossy(v)))
            .unwrap_or_default();

        txn_producer.begin().expect("begin failed");

        let mut out = krafka::Record::new(dst_topic, transformed_value);
        out.key = record.key.clone();
        let _ = txn_producer.send(out).await.expect("send to dst failed");

        // Commit the consumer offset atomically with the output message.
        let offsets = [TopicPartitionOffset::new(
            src_topic,
            record.partition,
            record.offset + 1, // next offset to consume
        )];
        // KIP-447: the commit must carry the consumer's live group identity so
        // the group coordinator can fence a zombie committer. Re-read it every
        // transaction — the generation changes on every rebalance, and a cached
        // value defeats the fencing.
        let group_metadata = src_consumer
            .group_metadata()
            .await
            .expect("consumer must have joined the group before committing offsets");
        assert!(
            group_metadata.is_fenceable(),
            "consumer group metadata must carry a real generation for EOS"
        );
        txn_producer
            .send_offsets(&offsets, &group_metadata)
            .await
            .expect("send_offsets failed");

        txn_producer.commit().await.expect("commit failed");
    }

    txn_producer.close().await.unwrap();

    // Step 4: Verify destination contains the transformed messages.
    let dst_consumer = krafka::Kafka::builder(&bootstrap_servers)
        .connect()
        .await
        .expect("Failed to create dst consumer")
        .consumer("txn-eos-dst-consumer")
        .auto_offset_reset(AutoOffsetReset::Earliest)
        .isolation_level(IsolationLevel::ReadCommitted)
        .build()
        .await
        .expect("Failed to create dst consumer");

    dst_consumer
        .subscribe(&[dst_topic])
        .await
        .expect("subscribe failed");

    let dst_records = poll_for_records(&dst_consumer, 3, Duration::from_secs(5), 10).await;
    assert_eq!(
        dst_records.len(),
        3,
        "All 3 transformed messages should be in dst topic"
    );
    for r in &dst_records {
        let val = r
            .value
            .as_deref()
            .map(String::from_utf8_lossy)
            .unwrap_or_default();
        assert!(
            val.starts_with("processed:src-"),
            "Expected transformed value, got: {val}"
        );
    }

    dst_consumer.close().await.expect("consumer close");

    // Step 5: Verify committed offsets — restarting the src consumer should
    // not reprocess messages (offsets were committed transactionally).
    let resumed_consumer = krafka::Kafka::builder(&bootstrap_servers)
        .connect()
        .await
        .expect("Failed to create resumed consumer")
        .consumer(group_id)
        .auto_offset_reset(AutoOffsetReset::Latest)
        .enable_auto_commit(false)
        .isolation_level(IsolationLevel::ReadCommitted)
        .build()
        .await
        .expect("Failed to create resumed consumer");

    resumed_consumer
        .subscribe(&[src_topic])
        .await
        .expect("subscribe failed");

    // A short poll — if the offsets are committed, no messages should appear.
    let mut leftover: Vec<krafka::consumer::ConsumerRecord> = Vec::new();
    for _ in 0..3 {
        leftover.extend(
            resumed_consumer
                .poll(Duration::from_secs(1))
                .await
                .expect("poll failed"),
        );
    }
    assert!(
        leftover.is_empty(),
        "Transactionally committed offsets should prevent re-delivery; got {} leftover records",
        leftover.len()
    );

    resumed_consumer.close().await.expect("consumer close");
    src_consumer.close().await.expect("consumer close");
}
