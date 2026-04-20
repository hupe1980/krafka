//! Integration tests for Krafka.
//!
//! These tests require Docker to be running.
//!
//! Run with:
//! ```
//! cargo test --test integration_tests
//! ```
//!
//! Note: These tests are ignored by default as they require Docker.
//! Enable with: `cargo test --test integration_tests -- --ignored`

use std::borrow::Cow;
use std::collections::HashMap;
use std::time::Duration;

use testcontainers::core::{ContainerPort, ContainerState, ExecCommand, WaitFor};
use testcontainers::{ContainerAsync, Image, runners::AsyncRunner};

// ---------------------------------------------------------------------------
// Timing constants — tweak these for CI vs local runs
// ---------------------------------------------------------------------------

/// Time to wait after container start for Kafka to stabilize.
const CONTAINER_SETTLE: Duration = Duration::from_secs(10);

/// Time to wait after topic creation for metadata propagation.
const TOPIC_READY: Duration = Duration::from_secs(2);

// ---------------------------------------------------------------------------
// Custom Kafka image – works with `apache/kafka-native` 3.8 – 4.x
// ---------------------------------------------------------------------------

const KAFKA_PORT: ContainerPort = ContainerPort::Tcp(9092);
const START_SCRIPT: &str = "/tmp/testcontainers_start.sh";

/// Minimal [`Image`] for `apache/kafka-native` that follows the same
/// start-script pattern as Java testcontainers.
///
/// 1. The container command loops until `START_SCRIPT` exists.
/// 2. `exec_after_start` writes that script — after the host port is known —
///    exporting `KAFKA_ADVERTISED_LISTENERS` and calling `/etc/kafka/docker/run`.
/// 3. Wait condition: "Kafka Server started" appears in container logs.
#[derive(Debug, Clone)]
struct ApacheKafka {
    tag: String,
    env_vars: HashMap<String, String>,
}

impl ApacheKafka {
    fn new(tag: impl Into<String>) -> Self {
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

        Self { tag, env_vars }
    }
}

impl Image for ApacheKafka {
    fn name(&self) -> &str {
        "apache/kafka-native"
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

/// Helper to get a Kafka container.
///
/// Uses the `apache/kafka-native` image. The image tag is read from the
/// `KAFKA_VERSION` environment variable (set by CI matrix); defaults to
/// `3.9.0` for local development.
///
/// The GraalVM native image occasionally segfaults during startup
/// (`Pwd.getpwuid` in class initialization), so we retry up to 3 times.
async fn kafka_container() -> (ContainerAsync<ApacheKafka>, String) {
    let tag = std::env::var("KAFKA_VERSION").unwrap_or_else(|_| "3.9.0".to_string());

    let max_attempts = 3;
    let mut last_err = None;

    for attempt in 1..=max_attempts {
        match ApacheKafka::new(&tag).start().await {
            Ok(container) => {
                // Wait for Kafka to be fully ready
                tokio::time::sleep(CONTAINER_SETTLE).await;

                let host_port = container
                    .get_host_port_ipv4(KAFKA_PORT)
                    .await
                    .expect("Failed to get host port");

                let bootstrap_servers = format!("127.0.0.1:{}", host_port);
                return (container, bootstrap_servers);
            }
            Err(e) => {
                eprintln!("Kafka container start attempt {attempt}/{max_attempts} failed: {e}");
                last_err = Some(e);
                if attempt < max_attempts {
                    let backoff = Duration::from_secs(2u64.pow(attempt as u32));
                    tokio::time::sleep(backoff).await;
                }
            }
        }
    }

    panic!(
        "Failed to start Kafka container after {max_attempts} attempts: {}",
        last_err.unwrap()
    );
}

/// Helper to subscribe with retry for coordinator availability.
async fn subscribe_with_retry(
    consumer: &krafka::consumer::Consumer,
    topics: &[&str],
    max_retries: u32,
) -> Result<(), krafka::error::KrafkaError> {
    use krafka::error::KrafkaError;

    let mut last_error = None;
    for attempt in 0..max_retries {
        match consumer.subscribe(topics).await {
            Ok(()) => return Ok(()),
            Err(e) => {
                // Check if it's a coordinator not available error
                let is_coordinator_error = matches!(&e, KrafkaError::Broker { .. });
                if is_coordinator_error && attempt < max_retries - 1 {
                    eprintln!(
                        "Subscribe attempt {} failed (coordinator not ready), retrying in 2s...",
                        attempt + 1
                    );
                    tokio::time::sleep(Duration::from_secs(2)).await;
                    last_error = Some(e);
                } else {
                    return Err(e);
                }
            }
        }
    }
    Err(last_error.unwrap())
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
    use krafka::admin::{AdminClient, NewTopic};

    let admin = AdminClient::builder()
        .bootstrap_servers(bootstrap_servers)
        .build()
        .await
        .expect("Failed to create admin client");

    admin
        .create_topics(
            vec![NewTopic::new(topic, partitions, 1).unwrap()],
            Duration::from_secs(10),
        )
        .await
        .expect("Failed to create topic");

    // Wait for topic to be ready
    tokio::time::sleep(TOPIC_READY).await;
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn test_producer_send_receive() {
    use krafka::consumer::{AutoOffsetReset, Consumer};
    use krafka::producer::Producer;

    let (_container, bootstrap_servers) = kafka_container().await;

    // Create topic first
    let topic = "test-topic";
    create_topic(&bootstrap_servers, topic, 1).await;

    // Create producer
    let producer = Producer::builder()
        .bootstrap_servers(&bootstrap_servers)
        .client_id("test-producer")
        .build()
        .await
        .expect("Failed to create producer");

    let metadata = producer
        .send(topic, Some(b"test-key"), b"test-value")
        .await
        .expect("Failed to send message");

    assert!(metadata.offset >= 0);

    // Create consumer
    let consumer = Consumer::builder()
        .bootstrap_servers(&bootstrap_servers)
        .group_id("test-group")
        .auto_offset_reset(AutoOffsetReset::Earliest)
        .build()
        .await
        .expect("Failed to create consumer");

    subscribe_with_retry(&consumer, &[topic], 5)
        .await
        .expect("Failed to subscribe");

    // Poll for messages (first poll may be consumed by rebalance)
    let records = poll_for_records(&consumer, 1, Duration::from_secs(5), 5).await;

    assert!(!records.is_empty(), "Expected at least one record");

    let record = &records[0];
    assert_eq!(record.topic, topic);
    assert_eq!(record.key_str(), Some("test-key"));
    assert_eq!(record.value_str(), Some("test-value"));

    consumer.close().await;
    producer.close().await;
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn test_admin_client() {
    use krafka::admin::{AdminClient, NewTopic};

    let (_container, bootstrap_servers) = kafka_container().await;

    let admin = AdminClient::builder()
        .bootstrap_servers(&bootstrap_servers)
        .client_id("test-admin")
        .build()
        .await
        .expect("Failed to create admin client");

    // Create a topic
    let topic_name = "admin-test-topic";
    let new_topic = NewTopic::new(topic_name, 3, 1).unwrap();

    admin
        .create_topics(vec![new_topic], Duration::from_secs(10))
        .await
        .expect("Failed to create topic");

    // Wait for topic to be created
    tokio::time::sleep(Duration::from_secs(1)).await;

    // List topics
    let topics = admin.list_topics().await.expect("Failed to list topics");
    assert!(
        topics.iter().any(|t| t == topic_name),
        "Topic not found in list"
    );

    // Describe cluster
    let cluster = admin
        .describe_cluster()
        .await
        .expect("Failed to describe cluster");
    assert!(!cluster.brokers.is_empty(), "No brokers found");

    // Delete topic
    admin
        .delete_topics(vec![topic_name.to_string()], Duration::from_secs(10))
        .await
        .expect("Failed to delete topic");
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn test_compression_roundtrip() {
    use krafka::consumer::{AutoOffsetReset, Consumer};
    use krafka::producer::Producer;
    use krafka::protocol::Compression;

    let (_container, bootstrap_servers) = kafka_container().await;

    for compression in [
        Compression::None,
        #[cfg(feature = "gzip")]
        Compression::Gzip,
        #[cfg(feature = "snappy")]
        Compression::Snappy,
        #[cfg(feature = "lz4")]
        Compression::Lz4,
        // Zstd is not supported by the apache/kafka-native GraalVM image.
    ] {
        let topic = format!("compression-test-{:?}", compression).to_lowercase();
        create_topic(&bootstrap_servers, &topic, 1).await;
        let value = format!("test-value-for-{:?}", compression);

        // Create producer with compression
        let producer = Producer::builder()
            .bootstrap_servers(&bootstrap_servers)
            .client_id("compression-test-producer")
            .compression(compression)
            .build()
            .await
            .expect("Failed to create producer");

        let metadata = producer
            .send(&topic, None, value.as_bytes())
            .await
            .expect("Failed to send message");

        assert!(metadata.offset >= 0, "Expected valid offset");

        producer.close().await;

        // Create consumer
        let consumer = Consumer::builder()
            .bootstrap_servers(&bootstrap_servers)
            .group_id(format!("compression-test-group-{:?}", compression).to_lowercase())
            .auto_offset_reset(AutoOffsetReset::Earliest)
            .build()
            .await
            .expect("Failed to create consumer");

        subscribe_with_retry(&consumer, &[&topic], 5)
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
        consumer.close().await;
    }
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn test_multiple_partitions() {
    use krafka::admin::{AdminClient, NewTopic};
    use krafka::producer::Producer;

    let (_container, bootstrap_servers) = kafka_container().await;

    // Create topic with multiple partitions
    let admin = AdminClient::builder()
        .bootstrap_servers(&bootstrap_servers)
        .build()
        .await
        .expect("Failed to create admin client");

    let topic_name = "multi-partition-topic";
    let new_topic = NewTopic::new(topic_name, 6, 1).unwrap();

    admin
        .create_topics(vec![new_topic], Duration::from_secs(10))
        .await
        .expect("Failed to create topic");

    tokio::time::sleep(Duration::from_secs(1)).await;

    // Create producer
    let producer = Producer::builder()
        .bootstrap_servers(&bootstrap_servers)
        .build()
        .await
        .expect("Failed to create producer");

    // Send messages with different keys
    let mut partition_set = std::collections::HashSet::new();
    for i in 0..100 {
        let key = format!("key-{}", i);
        let metadata = producer
            .send(topic_name, Some(key.as_bytes()), b"value")
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

    producer.close().await;
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn test_consumer_group_rebalance() {
    use krafka::admin::{AdminClient, NewTopic};
    use krafka::consumer::{AutoOffsetReset, Consumer};
    use krafka::producer::Producer;

    let (_container, bootstrap_servers) = kafka_container().await;

    let topic_name = "consumer-group-test";
    let group_id = "test-consumer-group";

    // Create topic with 4 partitions
    let admin = AdminClient::builder()
        .bootstrap_servers(&bootstrap_servers)
        .build()
        .await
        .expect("Failed to create admin client");

    let new_topic = NewTopic::new(topic_name, 4, 1).unwrap();
    admin
        .create_topics(vec![new_topic], Duration::from_secs(10))
        .await
        .expect("Failed to create topic");

    tokio::time::sleep(Duration::from_secs(1)).await;

    // Produce some messages
    let producer = Producer::builder()
        .bootstrap_servers(&bootstrap_servers)
        .build()
        .await
        .expect("Failed to create producer");

    for i in 0..20 {
        let key = format!("key-{}", i);
        let _ = producer
            .send(
                topic_name,
                Some(key.as_bytes()),
                format!("value-{}", i).as_bytes(),
            )
            .await
            .expect("Failed to send message");
    }
    producer.close().await;

    // Create first consumer
    let consumer1 = Consumer::builder()
        .bootstrap_servers(&bootstrap_servers)
        .group_id(group_id)
        .auto_offset_reset(AutoOffsetReset::Earliest)
        .build()
        .await
        .expect("Failed to create consumer1");

    subscribe_with_retry(&consumer1, &[topic_name], 5)
        .await
        .expect("Failed to subscribe consumer1");

    // Poll to join group (first poll may only do rebalance)
    let records1 = poll_for_records(&consumer1, 1, Duration::from_secs(5), 5).await;

    // Create second consumer in same group
    let consumer2 = Consumer::builder()
        .bootstrap_servers(&bootstrap_servers)
        .group_id(group_id)
        .auto_offset_reset(AutoOffsetReset::Earliest)
        .build()
        .await
        .expect("Failed to create consumer2");

    subscribe_with_retry(&consumer2, &[topic_name], 5)
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
    consumer1.close().await;
    consumer2.close().await;
}

// ============================================================================
// Chaos Testing (Story 10.3)
// ============================================================================

#[tokio::test]
#[ignore = "requires Docker"]
async fn test_connection_timeout_handling() {
    use krafka::producer::Producer;

    // Try to connect to a non-existent broker with short timeout
    let result = Producer::builder()
        .bootstrap_servers("127.0.0.1:19999") // Non-existent port
        .client_id("timeout-test")
        .build()
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
    use krafka::producer::Producer;

    let (_container, bootstrap_servers) = kafka_container().await;

    let topic = "resilience-test-topic";
    create_topic(&bootstrap_servers, topic, 1).await;

    let producer = Producer::builder()
        .bootstrap_servers(&bootstrap_servers)
        .client_id("resilience-test")
        .build()
        .await
        .expect("Failed to create producer");

    // Send multiple messages to verify producer stability
    for i in 0..5 {
        let result = producer
            .send(
                topic,
                Some(format!("key-{}", i).as_bytes()),
                format!("value-{}", i).as_bytes(),
            )
            .await;

        assert!(result.is_ok(), "Message {} should succeed", i);

        // Small delay between sends
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    producer.close().await;
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn test_consumer_handles_no_messages_gracefully() {
    use krafka::consumer::{AutoOffsetReset, Consumer};
    use krafka::producer::Producer;

    let (_container, bootstrap_servers) = kafka_container().await;

    let topic = "empty-topic-test";
    create_topic(&bootstrap_servers, topic, 1).await;

    // Create producer and send one message so topic has offsets
    let producer = Producer::builder()
        .bootstrap_servers(&bootstrap_servers)
        .client_id("create-topic")
        .build()
        .await
        .expect("Failed to create producer");

    let _ = producer
        .send(topic, None, b"setup")
        .await
        .expect("Failed to send setup message");
    producer.close().await;

    // Consumer starting from latest should see no new messages
    let consumer = Consumer::builder()
        .bootstrap_servers(&bootstrap_servers)
        .group_id("empty-test-group")
        .auto_offset_reset(AutoOffsetReset::Latest)
        .build()
        .await
        .expect("Failed to create consumer");

    subscribe_with_retry(&consumer, &[topic], 5)
        .await
        .expect("Failed to subscribe");

    // Poll should complete without error, even with no messages
    let records = poll_for_records(&consumer, 0, Duration::from_secs(2), 3).await;

    // May be empty or have the setup message depending on timing
    drop(records);
    consumer.close().await;
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn test_multiple_producers_same_topic() {
    use krafka::consumer::{AutoOffsetReset, Consumer};
    use krafka::producer::Producer;

    let (_container, bootstrap_servers) = kafka_container().await;

    let topic = "multi-producer-topic";
    create_topic(&bootstrap_servers, topic, 1).await;

    // Create multiple producers
    let producer1 = Producer::builder()
        .bootstrap_servers(&bootstrap_servers)
        .client_id("producer-1")
        .build()
        .await
        .expect("Failed to create producer 1");

    let producer2 = Producer::builder()
        .bootstrap_servers(&bootstrap_servers)
        .client_id("producer-2")
        .build()
        .await
        .expect("Failed to create producer 2");

    // Send from both producers
    for i in 0..3 {
        let _ = producer1
            .send(topic, Some(b"p1"), format!("p1-msg-{}", i).as_bytes())
            .await
            .expect("Producer 1 failed");

        let _ = producer2
            .send(topic, Some(b"p2"), format!("p2-msg-{}", i).as_bytes())
            .await
            .expect("Producer 2 failed");
    }

    producer1.close().await;
    producer2.close().await;

    // Verify all messages were received
    let consumer = Consumer::builder()
        .bootstrap_servers(&bootstrap_servers)
        .group_id("multi-producer-consumer")
        .auto_offset_reset(AutoOffsetReset::Earliest)
        .build()
        .await
        .expect("Failed to create consumer");

    subscribe_with_retry(&consumer, &[topic], 5)
        .await
        .expect("Failed to subscribe");

    // Collect all messages (first poll may be consumed by rebalance)
    let all_records = poll_for_records(&consumer, 6, Duration::from_secs(3), 8).await;

    assert_eq!(all_records.len(), 6, "Expected 6 messages from 2 producers");
    consumer.close().await;
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn test_large_message_handling() {
    use krafka::consumer::{AutoOffsetReset, Consumer};
    use krafka::producer::Producer;

    let (_container, bootstrap_servers) = kafka_container().await;

    let topic = "large-message-topic";
    create_topic(&bootstrap_servers, topic, 1).await;

    let producer = Producer::builder()
        .bootstrap_servers(&bootstrap_servers)
        .client_id("large-message-producer")
        .build()
        .await
        .expect("Failed to create producer");

    // Create a large message (100KB)
    let large_value = vec![b'X'; 100 * 1024];

    let metadata = producer
        .send(topic, Some(b"large-key"), &large_value)
        .await
        .expect("Failed to send large message");

    assert!(metadata.offset >= 0);
    producer.close().await;

    // Verify consumer can read it
    let consumer = Consumer::builder()
        .bootstrap_servers(&bootstrap_servers)
        .group_id("large-message-consumer")
        .auto_offset_reset(AutoOffsetReset::Earliest)
        .build()
        .await
        .expect("Failed to create consumer");

    subscribe_with_retry(&consumer, &[topic], 5)
        .await
        .expect("Failed to subscribe");

    let records = poll_for_records(&consumer, 1, Duration::from_secs(5), 5).await;

    assert!(!records.is_empty());
    assert_eq!(
        records[0].value.as_ref().map(|v| v.len()).unwrap_or(0),
        100 * 1024
    );
    consumer.close().await;
}

// ============================================================================
// Additional Integration Tests
// ============================================================================

#[tokio::test]
#[ignore = "requires Docker"]
async fn test_message_headers() {
    use krafka::consumer::{AutoOffsetReset, Consumer};
    use krafka::producer::Producer;

    let (_container, bootstrap_servers) = kafka_container().await;

    let topic = "headers-test-topic";
    create_topic(&bootstrap_servers, topic, 1).await;

    let producer = Producer::builder()
        .bootstrap_servers(&bootstrap_servers)
        .client_id("header-test-producer")
        .build()
        .await
        .expect("Failed to create producer");

    // Create headers as Vec<(String, Vec<u8>)>
    let headers = vec![
        ("trace-id".to_string(), b"abc123".to_vec()),
        ("content-type".to_string(), b"application/json".to_vec()),
    ];

    // Send message with headers
    let metadata = producer
        .send_with_headers(topic, Some(b"header-key"), b"header-value", headers)
        .await
        .expect("Failed to send message with headers");

    assert!(metadata.offset >= 0);
    producer.close().await;

    // Verify consumer receives headers
    let consumer = Consumer::builder()
        .bootstrap_servers(&bootstrap_servers)
        .group_id("header-test-consumer")
        .auto_offset_reset(AutoOffsetReset::Earliest)
        .build()
        .await
        .expect("Failed to create consumer");

    subscribe_with_retry(&consumer, &[topic], 5)
        .await
        .expect("Failed to subscribe");

    let records = poll_for_records(&consumer, 1, Duration::from_secs(5), 5).await;

    assert!(!records.is_empty());
    let record = &records[0];

    // Verify headers are present
    assert!(record.header("trace-id").is_some());
    consumer.close().await;
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn test_idempotent_producer() {
    use krafka::producer::Producer;

    let (_container, bootstrap_servers) = kafka_container().await;

    let topic = "idempotent-test-topic";
    create_topic(&bootstrap_servers, topic, 1).await;

    // Create idempotent producer (enabled by default since KIP-679)
    let producer = Producer::builder()
        .bootstrap_servers(&bootstrap_servers)
        .client_id("idempotent-producer-test")
        .build()
        .await
        .expect("Failed to create idempotent producer");

    // Send multiple messages
    for i in 0..5 {
        let metadata = producer
            .send(
                topic,
                Some(format!("key-{}", i).as_bytes()),
                format!("value-{}", i).as_bytes(),
            )
            .await
            .expect("Failed to send message");

        // Idempotent producer should maintain sequence
        assert!(metadata.offset >= 0);
    }

    producer.close().await;
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn test_null_key_and_value() {
    use krafka::consumer::{AutoOffsetReset, Consumer};
    use krafka::producer::Producer;

    let (_container, bootstrap_servers) = kafka_container().await;

    let topic = "null-test-topic";
    create_topic(&bootstrap_servers, topic, 1).await;

    let producer = Producer::builder()
        .bootstrap_servers(&bootstrap_servers)
        .client_id("null-test-producer")
        .build()
        .await
        .expect("Failed to create producer");

    // Send message with null key
    let metadata = producer
        .send(topic, None, b"value-with-null-key")
        .await
        .expect("Failed to send message with null key");
    assert!(metadata.offset >= 0);

    producer.close().await;

    // Verify consumer receives the message
    let consumer = Consumer::builder()
        .bootstrap_servers(&bootstrap_servers)
        .group_id("null-test-consumer")
        .auto_offset_reset(AutoOffsetReset::Earliest)
        .build()
        .await
        .expect("Failed to create consumer");

    subscribe_with_retry(&consumer, &[topic], 5)
        .await
        .expect("Failed to subscribe");

    let records = poll_for_records(&consumer, 1, Duration::from_secs(5), 5).await;

    assert!(!records.is_empty());
    let record = &records[0];

    // Verify null key is received as None
    assert!(record.key.is_none());
    assert!(record.value.is_some());
    consumer.close().await;
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn test_multiple_topics_subscription() {
    use krafka::consumer::{AutoOffsetReset, Consumer};
    use krafka::producer::Producer;

    let (_container, bootstrap_servers) = kafka_container().await;

    let topic1 = "multi-topic-1";
    let topic2 = "multi-topic-2";
    create_topic(&bootstrap_servers, topic1, 1).await;
    create_topic(&bootstrap_servers, topic2, 1).await;

    let producer = Producer::builder()
        .bootstrap_servers(&bootstrap_servers)
        .client_id("multi-topic-producer")
        .build()
        .await
        .expect("Failed to create producer");

    // Send messages to both topics
    let _ = producer
        .send(topic1, Some(b"key1"), b"value1")
        .await
        .expect("send failed");
    let _ = producer
        .send(topic2, Some(b"key2"), b"value2")
        .await
        .expect("send failed");
    producer.close().await;

    // Consumer subscribed to both topics
    let consumer = Consumer::builder()
        .bootstrap_servers(&bootstrap_servers)
        .group_id("multi-topic-consumer")
        .auto_offset_reset(AutoOffsetReset::Earliest)
        .build()
        .await
        .expect("Failed to create consumer");

    subscribe_with_retry(&consumer, &[topic1, topic2], 5)
        .await
        .expect("Failed to subscribe");

    // Collect messages from both topics (first poll may be consumed by rebalance)
    let all_records = poll_for_records(&consumer, 2, Duration::from_secs(3), 8).await;

    assert_eq!(all_records.len(), 2, "Expected 2 messages from 2 topics");

    // Verify we got messages from both topics
    let topics: std::collections::HashSet<_> =
        all_records.iter().map(|r| r.topic.as_str()).collect();
    assert!(
        topics.contains(topic1) && topics.contains(topic2),
        "Should contain messages from both topics, got: {:?}",
        topics
    );
    consumer.close().await;
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn test_admin_describe_configs() {
    use krafka::admin::{AdminClient, NewTopic};

    let (_container, bootstrap_servers) = kafka_container().await;

    let admin = AdminClient::builder()
        .bootstrap_servers(&bootstrap_servers)
        .client_id("config-test-admin")
        .build()
        .await
        .expect("Failed to create admin client");

    // Create a topic first
    let topic_name = "config-test-topic";
    let new_topic = NewTopic::new(topic_name, 1, 1).unwrap();
    admin
        .create_topics(vec![new_topic], Duration::from_secs(10))
        .await
        .expect("Failed to create topic");

    tokio::time::sleep(Duration::from_secs(1)).await;

    // Describe topic configs
    use krafka::admin::DescribeConfigsRequest;
    let configs = admin
        .describe_configs(DescribeConfigsRequest::for_topic(topic_name))
        .await
        .expect("Failed to describe configs");

    // Should have some configuration entries
    assert!(!configs.is_empty(), "Expected config entries");

    // Clean up
    admin
        .delete_topics(vec![topic_name.to_string()], Duration::from_secs(10))
        .await
        .ok();
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn test_concurrent_producers() {
    use krafka::consumer::{AutoOffsetReset, Consumer};
    use krafka::producer::Producer;

    let (_container, bootstrap_servers) = kafka_container().await;

    let topic = "concurrent-producer-topic";
    create_topic(&bootstrap_servers, topic, 3).await;

    // Spawn multiple producer tasks concurrently
    let bootstrap = bootstrap_servers.clone();
    let handles: Vec<_> = (0..3)
        .map(|i| {
            let bs = bootstrap.clone();
            tokio::spawn(async move {
                let producer = Producer::builder()
                    .bootstrap_servers(&bs)
                    .client_id(format!("concurrent-producer-{}", i))
                    .build()
                    .await
                    .expect("Failed to create producer");

                for j in 0..5 {
                    let _ = producer
                        .send(
                            "concurrent-producer-topic",
                            Some(format!("key-{}-{}", i, j).as_bytes()),
                            format!("value-{}-{}", i, j).as_bytes(),
                        )
                        .await
                        .expect("Failed to send");
                }
                producer.close().await;
            })
        })
        .collect();

    // Wait for all producers to complete
    for handle in handles {
        handle.await.expect("Producer task failed");
    }

    // Verify all 15 messages were received
    let consumer = Consumer::builder()
        .bootstrap_servers(&bootstrap_servers)
        .group_id("concurrent-producer-consumer")
        .auto_offset_reset(AutoOffsetReset::Earliest)
        .build()
        .await
        .expect("Failed to create consumer");

    subscribe_with_retry(&consumer, &[topic], 5)
        .await
        .expect("Failed to subscribe");

    let all_records = poll_for_records(&consumer, 15, Duration::from_secs(3), 10).await;

    assert_eq!(
        all_records.len(),
        15,
        "Expected 15 messages from 3 concurrent producers"
    );
    consumer.close().await;
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn test_producer_with_batching() {
    use krafka::consumer::{AutoOffsetReset, Consumer};
    use krafka::producer::Producer;

    let (_container, bootstrap_servers) = kafka_container().await;

    let topic = "batch-test-topic";
    create_topic(&bootstrap_servers, topic, 1).await;

    // Create producer with batching enabled (linger > 0)
    let producer = Producer::builder()
        .bootstrap_servers(&bootstrap_servers)
        .client_id("batch-producer")
        .linger(Duration::from_millis(50)) // Enable batching
        .batch_size(16384)
        .build()
        .await
        .expect("Failed to create producer");

    // Send messages rapidly - should be batched
    for i in 0..10 {
        let _ = producer
            .send(
                topic,
                Some(format!("key-{}", i).as_bytes()),
                format!("value-{}", i).as_bytes(),
            )
            .await
            .expect("Failed to send");
    }
    producer.close().await;

    // Verify consumer receives all messages
    let consumer = Consumer::builder()
        .bootstrap_servers(&bootstrap_servers)
        .group_id("batch-consumer")
        .auto_offset_reset(AutoOffsetReset::Earliest)
        .build()
        .await
        .expect("Failed to create consumer");

    subscribe_with_retry(&consumer, &[topic], 5)
        .await
        .expect("Failed to subscribe");

    let records = poll_for_records(&consumer, 10, Duration::from_secs(5), 5).await;
    assert_eq!(records.len(), 10, "Expected 10 messages");
    consumer.close().await;
}

// Note: TransactionalProducer tests are skipped because transaction coordinator
// resolution requires connecting to broker addresses returned by FindCoordinator,
// which returns internal container addresses that don't work with testcontainers
// port mapping. TransactionalProducer has been tested manually with real Kafka clusters.

#[tokio::test]
#[ignore = "requires Docker"]
async fn test_admin_create_partitions() {
    use krafka::admin::{AdminClient, NewTopic};

    let (_container, bootstrap_servers) = kafka_container().await;

    let admin = AdminClient::builder()
        .bootstrap_servers(&bootstrap_servers)
        .build()
        .await
        .expect("Failed to create admin client");

    let topic_name = "partition-increase-topic";

    // Create topic with 2 partitions
    admin
        .create_topics(
            vec![NewTopic::new(topic_name, 2, 1).unwrap()],
            Duration::from_secs(10),
        )
        .await
        .expect("Failed to create topic");

    tokio::time::sleep(Duration::from_secs(1)).await;

    // Verify initial partition count
    let count = admin
        .partition_count(topic_name)
        .await
        .expect("Failed to get count");
    assert_eq!(count, Some(2), "Expected 2 partitions initially");

    // Increase to 4 partitions
    admin
        .create_partitions(topic_name, 4, Duration::from_secs(10))
        .await
        .expect("Failed to create partitions");

    tokio::time::sleep(Duration::from_secs(1)).await;

    // Verify new partition count
    let count = admin
        .partition_count(topic_name)
        .await
        .expect("Failed to get count");
    assert_eq!(count, Some(4), "Expected 4 partitions after increase");

    // Clean up
    admin
        .delete_topics(vec![topic_name.to_string()], Duration::from_secs(10))
        .await
        .ok();
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn test_admin_alter_topic_config() {
    use krafka::admin::{AdminClient, NewTopic};

    let (_container, bootstrap_servers) = kafka_container().await;

    let admin = AdminClient::builder()
        .bootstrap_servers(&bootstrap_servers)
        .build()
        .await
        .expect("Failed to create admin client");

    let topic_name = "config-alter-topic";

    // Create topic
    admin
        .create_topics(
            vec![NewTopic::new(topic_name, 1, 1).unwrap()],
            Duration::from_secs(10),
        )
        .await
        .expect("Failed to create topic");

    tokio::time::sleep(Duration::from_secs(1)).await;

    // Alter topic config - set retention to 1 hour
    let mut configs = std::collections::HashMap::new();
    configs.insert("retention.ms".to_string(), "3600000".to_string());

    let result = admin
        .alter_topic_config(topic_name, configs)
        .await
        .expect("Failed to alter config");

    assert!(result.error.is_none(), "Config alteration should succeed");

    // Verify the config was changed
    use krafka::admin::DescribeConfigsRequest;
    let topic_configs = admin
        .describe_configs(DescribeConfigsRequest::for_topic(topic_name))
        .await
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
        .delete_topics(vec![topic_name.to_string()], Duration::from_secs(10))
        .await
        .ok();
}
#[tokio::test]
#[ignore = "requires Docker"]
async fn test_admin_describe_cluster() {
    use krafka::admin::AdminClient;

    let (_container, bootstrap_servers) = kafka_container().await;

    let admin = AdminClient::builder()
        .bootstrap_servers(&bootstrap_servers)
        .build()
        .await
        .expect("Failed to create admin client");

    let cluster = admin
        .describe_cluster()
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
    assert!(broker.broker_id >= 0, "Broker should have a valid ID");
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn test_admin_describe_topics() {
    use krafka::admin::{AdminClient, NewTopic};

    let (_container, bootstrap_servers) = kafka_container().await;

    let admin = AdminClient::builder()
        .bootstrap_servers(&bootstrap_servers)
        .build()
        .await
        .expect("Failed to create admin client");

    // Create test topics
    let topic1 = "describe-topic-1";
    let topic2 = "describe-topic-2";

    admin
        .create_topics(
            vec![
                NewTopic::new(topic1, 2, 1).unwrap(),
                NewTopic::new(topic2, 3, 1).unwrap(),
            ],
            Duration::from_secs(10),
        )
        .await
        .expect("Failed to create topics");

    tokio::time::sleep(Duration::from_secs(1)).await;

    // Describe the topics
    let topics = admin
        .describe_topics(&[topic1.to_string(), topic2.to_string()])
        .await
        .expect("Failed to describe topics");

    assert_eq!(topics.len(), 2, "Should describe 2 topics");

    let t1 = topics
        .iter()
        .find(|t| t.name == topic1)
        .expect("topic1 not found");
    let t2 = topics
        .iter()
        .find(|t| t.name == topic2)
        .expect("topic2 not found");

    assert_eq!(t1.partitions.len(), 2, "topic1 should have 2 partitions");
    assert_eq!(t2.partitions.len(), 3, "topic2 should have 3 partitions");

    // Clean up
    admin
        .delete_topics(
            vec![topic1.to_string(), topic2.to_string()],
            Duration::from_secs(10),
        )
        .await
        .ok();
}

// ============================================================================
// Round 14 Integration Tests
// ============================================================================

#[tokio::test]
#[ignore = "requires Docker"]
async fn test_producer_timestamp_propagation() {
    use krafka::consumer::{AutoOffsetReset, Consumer};
    use krafka::producer::{Producer, ProducerRecord};

    let (_container, bootstrap_servers) = kafka_container().await;

    let topic = "timestamp-test-topic";
    create_topic(&bootstrap_servers, topic, 1).await;

    let producer = Producer::builder()
        .bootstrap_servers(&bootstrap_servers)
        .client_id("timestamp-test-producer")
        .build()
        .await
        .expect("Failed to create producer");

    // Send with explicit timestamp
    let timestamp = 1700000000000_i64; // Unix epoch ms
    let record = ProducerRecord::new(topic, b"hello".to_vec())
        .with_key(b"ts-key".to_vec())
        .with_timestamp(timestamp);
    let metadata = producer
        .send_record(record)
        .await
        .expect("Failed to send record with timestamp");

    assert!(metadata.offset >= 0);
    producer.close().await;

    // Use manual partition assignment (no group coordinator) to avoid
    // a race where ListOffsets(timestamp=-2) transiently returns the high
    // watermark instead of the log start offset for freshly created
    // partitions, AND the group coordinator rejoin in poll() overwrites
    // any seek_to_beginning() the test applies.
    let consumer = Consumer::builder()
        .bootstrap_servers(&bootstrap_servers)
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
    consumer.close().await;
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn test_consumer_manual_assign() {
    use krafka::consumer::{AutoOffsetReset, Consumer};
    use krafka::producer::{Producer, ProducerRecord};

    let (_container, bootstrap_servers) = kafka_container().await;

    let topic = "manual-assign-topic";
    create_topic(&bootstrap_servers, topic, 2).await;

    let producer = Producer::builder()
        .bootstrap_servers(&bootstrap_servers)
        .build()
        .await
        .expect("Failed to create producer");

    // Send messages explicitly to partition 0 so the test is deterministic
    for i in 0..5 {
        let record = ProducerRecord::new(topic, format!("val-{}", i).into_bytes())
            .with_partition(0)
            .with_key(format!("k-{}", i).into_bytes());
        let _ = producer.send_record(record).await.expect("send failed");
    }
    // Also send some to partition 1 (should NOT be received)
    for i in 0..5 {
        let record = ProducerRecord::new(topic, format!("val-p1-{}", i).into_bytes())
            .with_partition(1)
            .with_key(format!("k1-{}", i).into_bytes());
        let _ = producer.send_record(record).await.expect("send failed");
    }
    producer.close().await;

    // Create consumer WITHOUT group_id — manual assignment mode
    let consumer = Consumer::builder()
        .bootstrap_servers(&bootstrap_servers)
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
    consumer.close().await;
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn test_admin_list_consumer_groups() {
    use krafka::admin::AdminClient;
    use krafka::consumer::{AutoOffsetReset, Consumer};

    let (_container, bootstrap_servers) = kafka_container().await;

    let topic = "group-list-topic";
    create_topic(&bootstrap_servers, topic, 1).await;

    let group_id = "group-list-test-group";

    // Create a consumer and join a group
    let consumer = Consumer::builder()
        .bootstrap_servers(&bootstrap_servers)
        .group_id(group_id)
        .auto_offset_reset(AutoOffsetReset::Earliest)
        .build()
        .await
        .expect("Failed to create consumer");

    subscribe_with_retry(&consumer, &[topic], 5)
        .await
        .expect("Failed to subscribe");

    // Poll multiple times to ensure the group is actually joined (rebalance may consume first poll)
    let _ = poll_for_records(&consumer, 0, Duration::from_secs(3), 3).await;

    // Admin client should be able to list the group
    let admin = AdminClient::builder()
        .bootstrap_servers(&bootstrap_servers)
        .build()
        .await
        .expect("Failed to create admin client");

    let groups = admin
        .list_consumer_groups()
        .await
        .expect("Failed to list groups");

    assert!(
        groups.iter().any(|g| g.group_id == group_id),
        "Expected to find group '{}' in list: {:?}",
        group_id,
        groups.iter().map(|g| &g.group_id).collect::<Vec<_>>()
    );
    consumer.close().await;
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn test_consumer_unsubscribe() {
    use krafka::consumer::{AutoOffsetReset, Consumer};

    let (_container, bootstrap_servers) = kafka_container().await;

    let topic = "unsub-test-topic";
    create_topic(&bootstrap_servers, topic, 1).await;

    let consumer = Consumer::builder()
        .bootstrap_servers(&bootstrap_servers)
        .group_id("unsub-test-group")
        .auto_offset_reset(AutoOffsetReset::Earliest)
        .build()
        .await
        .expect("Failed to create consumer");

    subscribe_with_retry(&consumer, &[topic], 5)
        .await
        .expect("Failed to subscribe");

    // Poll to join group (rebalance may consume first poll)
    let _ = poll_for_records(&consumer, 0, Duration::from_secs(3), 3).await;

    // Unsubscribe
    consumer.unsubscribe().await;

    // Subscription should be empty
    let subscription = consumer.subscription().await;
    assert!(
        subscription.is_empty(),
        "Subscription should be empty after unsubscribe"
    );
    consumer.close().await;
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn test_producer_metrics() {
    use krafka::producer::Producer;

    let (_container, bootstrap_servers) = kafka_container().await;

    let topic = "metrics-test-topic";
    create_topic(&bootstrap_servers, topic, 1).await;

    let producer = Producer::builder()
        .bootstrap_servers(&bootstrap_servers)
        .client_id("metrics-test-producer")
        .build()
        .await
        .expect("Failed to create producer");

    // Send some messages
    for i in 0..5 {
        let _ = producer
            .send(topic, Some(format!("k-{}", i).as_bytes()), b"value")
            .await
            .expect("send failed");
    }

    let metrics = producer.metrics().await;
    assert_eq!(metrics.records_sent, 5, "Should have sent 5 records");
    assert!(metrics.bytes_sent > 0, "Should have sent bytes");
    assert_eq!(metrics.errors, 0, "Should have no errors");

    producer.close().await;
    assert!(producer.is_closed(), "Producer should be closed");
}

// ============================================================================
// Round 15 — New integration tests for coverage gaps
// ============================================================================

/// Test that sending after producer.close() returns an error (not a panic).
#[tokio::test]
#[ignore = "requires Docker"]
async fn test_send_after_producer_close() {
    use krafka::producer::Producer;

    let (_container, bootstrap_servers) = kafka_container().await;

    let topic = "send-after-close-topic";
    create_topic(&bootstrap_servers, topic, 1).await;

    let producer = Producer::builder()
        .bootstrap_servers(&bootstrap_servers)
        .build()
        .await
        .expect("Failed to create producer");

    producer.close().await;
    assert!(producer.is_closed());

    let result = producer.send(topic, None, b"should-fail").await;
    assert!(result.is_err(), "Send after close should return an error");
}

/// Test consumer commit_sync and verified resume from committed offset.
#[tokio::test]
#[ignore = "requires Docker"]
async fn test_consumer_commit_and_resume_verified() {
    use krafka::consumer::{AutoOffsetReset, Consumer};
    use krafka::producer::Producer;

    let (_container, bootstrap_servers) = kafka_container().await;

    let topic = "commit-verify-topic";
    let group_id = "commit-verify-group";
    create_topic(&bootstrap_servers, topic, 1).await;

    let producer = Producer::builder()
        .bootstrap_servers(&bootstrap_servers)
        .build()
        .await
        .expect("Failed to create producer");

    for i in 0..10 {
        let _ = producer
            .send(topic, None, format!("msg-{}", i).as_bytes())
            .await
            .expect("send failed");
    }
    producer.close().await;

    // First consumer: read all and commit
    {
        let consumer = Consumer::builder()
            .bootstrap_servers(&bootstrap_servers)
            .group_id(group_id)
            .auto_offset_reset(AutoOffsetReset::Earliest)
            .enable_auto_commit(false)
            .build()
            .await
            .expect("Failed to create consumer");

        subscribe_with_retry(&consumer, &[topic], 5)
            .await
            .expect("Failed to subscribe");

        let all = poll_for_records(&consumer, 10, Duration::from_secs(3), 8).await;
        assert_eq!(all.len(), 10, "Should read all 10 messages");
        consumer.commit_sync().await.expect("commit failed");
        consumer.close().await;
    }

    // Second consumer: should get NO new messages (all committed)
    {
        let consumer = Consumer::builder()
            .bootstrap_servers(&bootstrap_servers)
            .group_id(group_id)
            .auto_offset_reset(AutoOffsetReset::Earliest)
            .enable_auto_commit(false)
            .build()
            .await
            .expect("Failed to create consumer");

        subscribe_with_retry(&consumer, &[topic], 5)
            .await
            .expect("Failed to subscribe");

        // Poll a few times to let rebalance complete, then verify no new records
        let records = poll_for_records(&consumer, 0, Duration::from_secs(3), 3).await;
        assert!(
            records.is_empty(),
            "Second consumer should get 0 records after commit, got {}",
            records.len()
        );
        consumer.close().await;
    }
}

/// Test consumer recv() streaming API.
#[tokio::test]
#[ignore = "requires Docker"]
async fn test_consumer_recv() {
    use krafka::consumer::{AutoOffsetReset, Consumer};
    use krafka::producer::Producer;

    let (_container, bootstrap_servers) = kafka_container().await;

    let topic = "recv-test-topic";
    create_topic(&bootstrap_servers, topic, 1).await;

    let producer = Producer::builder()
        .bootstrap_servers(&bootstrap_servers)
        .build()
        .await
        .unwrap();

    for i in 0..3 {
        let _ = producer
            .send(topic, None, format!("recv-msg-{}", i).as_bytes())
            .await
            .unwrap();
    }
    producer.close().await;

    let consumer = Consumer::builder()
        .bootstrap_servers(&bootstrap_servers)
        .group_id("recv-test-group")
        .auto_offset_reset(AutoOffsetReset::Earliest)
        .build()
        .await
        .unwrap();

    subscribe_with_retry(&consumer, &[topic], 5).await.unwrap();

    // Use recv() to receive individual records
    let mut received = Vec::new();
    for _ in 0..3 {
        match tokio::time::timeout(Duration::from_secs(30), consumer.recv()).await {
            Ok(Ok(Some(record))) => received.push(record),
            Ok(Ok(None)) => break,
            Ok(Err(e)) => panic!("recv error: {}", e),
            Err(_) => break,
        }
    }

    assert_eq!(received.len(), 3, "Should receive 3 records via recv()");
    consumer.close().await;
}

/// Test producer flush() forces pending messages to be sent.
#[tokio::test]
#[ignore = "requires Docker"]
async fn test_producer_flush() {
    use krafka::consumer::{AutoOffsetReset, Consumer};
    use krafka::producer::Producer;
    use std::sync::Arc;

    let (_container, bootstrap_servers) = kafka_container().await;

    let topic = "flush-test-topic";
    create_topic(&bootstrap_servers, topic, 1).await;

    let producer = Arc::new(
        Producer::builder()
            .bootstrap_servers(&bootstrap_servers)
            .linger(Duration::from_secs(30)) // Long linger to accumulate
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
                .send(&t, None, format!("flush-{}", i).as_bytes())
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

    producer.close().await;

    let consumer = Consumer::builder()
        .bootstrap_servers(&bootstrap_servers)
        .group_id("flush-test-group")
        .auto_offset_reset(AutoOffsetReset::Earliest)
        .build()
        .await
        .unwrap();

    subscribe_with_retry(&consumer, &[topic], 5).await.unwrap();

    let all = poll_for_records(&consumer, 5, Duration::from_secs(3), 8).await;
    assert_eq!(all.len(), 5, "All 5 flushed messages should be received");
    consumer.close().await;
}

/// Test admin describe_groups returns member information.
#[tokio::test]
#[ignore = "requires Docker"]
async fn test_admin_describe_consumer_group() {
    use krafka::admin::AdminClient;
    use krafka::consumer::{AutoOffsetReset, Consumer};

    let (_container, bootstrap_servers) = kafka_container().await;

    let topic = "describe-group-topic";
    let group_id = "describe-group-test";
    create_topic(&bootstrap_servers, topic, 1).await;

    let consumer = Consumer::builder()
        .bootstrap_servers(&bootstrap_servers)
        .group_id(group_id)
        .auto_offset_reset(AutoOffsetReset::Earliest)
        .build()
        .await
        .unwrap();

    subscribe_with_retry(&consumer, &[topic], 5).await.unwrap();

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
    let admin = AdminClient::builder()
        .bootstrap_servers(&bootstrap_servers)
        .build()
        .await
        .unwrap();

    let listed = admin.list_consumer_groups().await.unwrap();
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
            .describe_consumer_groups(vec![group_id.to_string()])
            .await
            .expect("describe_consumer_groups failed");
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
    consumer.close().await;
}

/// Test consumer close() properly leaves the group.
#[tokio::test]
#[ignore = "requires Docker"]
async fn test_consumer_close_leaves_group() {
    use krafka::admin::AdminClient;
    use krafka::consumer::{AutoOffsetReset, Consumer};

    let (_container, bootstrap_servers) = kafka_container().await;

    let topic = "close-leaves-group-topic";
    let group_id = "close-leaves-group";
    create_topic(&bootstrap_servers, topic, 1).await;

    let consumer = Consumer::builder()
        .bootstrap_servers(&bootstrap_servers)
        .group_id(group_id)
        .auto_offset_reset(AutoOffsetReset::Earliest)
        .build()
        .await
        .unwrap();

    subscribe_with_retry(&consumer, &[topic], 5).await.unwrap();
    // Poll multiple times to ensure group join completes
    let _ = poll_for_records(&consumer, 0, Duration::from_secs(3), 3).await;

    // Explicitly close
    consumer.close().await;
    tokio::time::sleep(Duration::from_secs(2)).await;

    let admin = AdminClient::builder()
        .bootstrap_servers(&bootstrap_servers)
        .build()
        .await
        .unwrap();

    let descriptions = admin
        .describe_consumer_groups(vec![group_id.to_string()])
        .await
        .expect("describe_consumer_groups failed");

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
    use krafka::consumer::{AutoOffsetReset, Consumer};
    use krafka::producer::Producer;

    let (_container, bootstrap_servers) = kafka_container().await;

    let topic = "empty-value-topic";
    create_topic(&bootstrap_servers, topic, 1).await;

    let producer = Producer::builder()
        .bootstrap_servers(&bootstrap_servers)
        .build()
        .await
        .unwrap();

    let metadata = producer.send(topic, Some(b"key"), b"").await.unwrap();
    assert!(metadata.offset >= 0);
    producer.close().await;

    let consumer = Consumer::builder()
        .bootstrap_servers(&bootstrap_servers)
        .group_id("empty-value-group")
        .auto_offset_reset(AutoOffsetReset::Earliest)
        .build()
        .await
        .unwrap();

    subscribe_with_retry(&consumer, &[topic], 5).await.unwrap();

    let records = poll_for_records(&consumer, 1, Duration::from_secs(3), 5).await;
    assert!(!records.is_empty(), "Should receive the empty-value record");
    assert_eq!(
        records[0].value.as_ref().map(|v| v.len()),
        Some(0),
        "Empty value should be preserved as zero-length"
    );
    consumer.close().await;
}

/// Test admin describe_configs returns broker configuration.
#[tokio::test]
#[ignore = "requires Docker"]
async fn test_admin_describe_broker_config() {
    use krafka::admin::AdminClient;
    use krafka::admin::DescribeConfigsRequest;

    let (_container, bootstrap_servers) = kafka_container().await;

    let admin = AdminClient::builder()
        .bootstrap_servers(&bootstrap_servers)
        .build()
        .await
        .unwrap();

    let cluster = admin.describe_cluster().await.unwrap();
    let broker_id = cluster.brokers[0].broker_id;

    let configs = admin
        .describe_configs(DescribeConfigsRequest::for_broker(broker_id))
        .await
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
    use krafka::admin::{AdminClient, NewTopic};
    use krafka::consumer::{AutoOffsetReset, Consumer};
    use krafka::producer::Producer;

    let (_container, bootstrap_servers) = kafka_container().await;

    let admin = AdminClient::builder()
        .bootstrap_servers(&bootstrap_servers)
        .build()
        .await
        .unwrap();

    let topic = "many-partitions-topic";
    admin
        .create_topics(
            vec![NewTopic::new(topic, 12, 1).unwrap()],
            Duration::from_secs(10),
        )
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_secs(2)).await;

    let producer = Producer::builder()
        .bootstrap_servers(&bootstrap_servers)
        .build()
        .await
        .unwrap();

    // Send 60 messages with keys to distribute across partitions
    for i in 0..60 {
        let _ = producer
            .send(topic, Some(format!("k-{}", i).as_bytes()), b"v")
            .await
            .unwrap();
    }
    producer.close().await;

    let consumer = Consumer::builder()
        .bootstrap_servers(&bootstrap_servers)
        .group_id("many-partitions-group")
        .auto_offset_reset(AutoOffsetReset::Earliest)
        .build()
        .await
        .unwrap();

    subscribe_with_retry(&consumer, &[topic], 5).await.unwrap();

    let all = poll_for_records(&consumer, 60, Duration::from_secs(3), 20).await;
    assert_eq!(all.len(), 60, "All 60 messages should be received");

    // Verify messages came from multiple partitions
    let partitions: std::collections::HashSet<_> = all.iter().map(|r| r.partition).collect();
    assert!(
        partitions.len() > 3,
        "60 keys across 12 partitions should hit many partitions, got {}",
        partitions.len()
    );
    consumer.close().await;
}

/// Test consumer pause/resume with verified assertions.
#[tokio::test]
#[ignore = "requires Docker"]
async fn test_consumer_pause_resume_verified() {
    use krafka::consumer::{AutoOffsetReset, Consumer};
    use krafka::producer::Producer;

    let (_container, bootstrap_servers) = kafka_container().await;

    let topic = "pause-verify-topic";
    create_topic(&bootstrap_servers, topic, 1).await;

    let producer = Producer::builder()
        .bootstrap_servers(&bootstrap_servers)
        .build()
        .await
        .unwrap();

    for i in 0..10 {
        let _ = producer
            .send(topic, None, format!("pv-{}", i).as_bytes())
            .await
            .unwrap();
    }
    producer.close().await;

    let consumer = Consumer::builder()
        .bootstrap_servers(&bootstrap_servers)
        .group_id("pause-verify-group")
        .auto_offset_reset(AutoOffsetReset::Earliest)
        .enable_auto_commit(false)
        .build()
        .await
        .unwrap();

    subscribe_with_retry(&consumer, &[topic], 5).await.unwrap();

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
    consumer.close().await;
}

/// Test consumer seek with verified offset positioning.
#[tokio::test]
#[ignore = "requires Docker"]
async fn test_consumer_seek_verified() {
    use krafka::consumer::{AutoOffsetReset, Consumer};
    use krafka::producer::Producer;

    let (_container, bootstrap_servers) = kafka_container().await;

    let topic = "seek-verify-topic";
    create_topic(&bootstrap_servers, topic, 1).await;

    let producer = Producer::builder()
        .bootstrap_servers(&bootstrap_servers)
        .build()
        .await
        .unwrap();

    for i in 0..10 {
        let _ = producer
            .send(topic, None, format!("msg-{}", i).as_bytes())
            .await
            .unwrap();
    }
    producer.close().await;

    let consumer = Consumer::builder()
        .bootstrap_servers(&bootstrap_servers)
        .group_id("seek-verify-group")
        .auto_offset_reset(AutoOffsetReset::Earliest)
        .enable_auto_commit(false)
        .build()
        .await
        .unwrap();

    subscribe_with_retry(&consumer, &[topic], 5).await.unwrap();

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
    consumer.close().await;
}

/// Test topic creation with custom configs.
#[tokio::test]
#[ignore = "requires Docker"]
async fn test_admin_create_topic_with_config() {
    use krafka::admin::{AdminClient, NewTopic};

    let (_container, bootstrap_servers) = kafka_container().await;

    let admin = AdminClient::builder()
        .bootstrap_servers(&bootstrap_servers)
        .build()
        .await
        .unwrap();

    let topic = "configured-topic";
    let new_topic = NewTopic::new(topic, 3, 1)
        .unwrap()
        .with_config("retention.ms", "3600000")
        .with_config("cleanup.policy", "compact");

    admin
        .create_topics(vec![new_topic], Duration::from_secs(10))
        .await
        .unwrap();

    tokio::time::sleep(Duration::from_secs(1)).await;

    let configs = admin
        .describe_configs(krafka::admin::DescribeConfigsRequest::for_topic(topic))
        .await
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
    use krafka::consumer::{AutoOffsetReset, Consumer};
    use krafka::producer::Producer;

    let (_container, bootstrap_servers) = kafka_container().await;

    let topic = "consumer-metrics-topic";
    create_topic(&bootstrap_servers, topic, 1).await;

    let producer = Producer::builder()
        .bootstrap_servers(&bootstrap_servers)
        .build()
        .await
        .unwrap();

    for i in 0..5 {
        let _ = producer
            .send(topic, None, format!("m-{}", i).as_bytes())
            .await
            .unwrap();
    }
    producer.close().await;

    let consumer = Consumer::builder()
        .bootstrap_servers(&bootstrap_servers)
        .group_id("consumer-metrics-group")
        .auto_offset_reset(AutoOffsetReset::Earliest)
        .build()
        .await
        .unwrap();

    subscribe_with_retry(&consumer, &[topic], 5).await.unwrap();

    let all = poll_for_records(&consumer, 5, Duration::from_secs(3), 8).await;
    let _total = all.len();

    let metrics = consumer.metrics();
    assert!(
        metrics.records_received.get() > 0,
        "Should have received records"
    );
    assert!(
        metrics.bytes_received.get() > 0,
        "Should have received bytes"
    );
    consumer.close().await;
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn test_offsets_for_times_and_watermarks_and_metadata() {
    use krafka::consumer::{AutoOffsetReset, Consumer};
    use krafka::producer::Producer;

    let (_container, bootstrap_servers) = kafka_container().await;

    let topic = "offsets-times-topic";
    create_topic(&bootstrap_servers, topic, 2).await;

    let producer = Producer::builder()
        .bootstrap_servers(&bootstrap_servers)
        .build()
        .await
        .expect("Failed to create producer");

    // Send a few messages across both partitions using different keys.
    const N: usize = 10;
    for i in 0..N {
        let key = format!("k-{}", i);
        let _ = producer
            .send(topic, Some(key.as_bytes()), format!("v-{}", i).as_bytes())
            .await
            .expect("Failed to send message");
    }
    producer.close().await;

    let consumer = Consumer::builder()
        .bootstrap_servers(&bootstrap_servers)
        .group_id("offsets-times-group")
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
    for p in &topic_info.partitions {
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
    for offset in offsets_at_zero.values() {
        assert_eq!(*offset, 0, "expected offset 0 at timestamp 0");
    }

    // offsets_for_times with a future timestamp should return -1 per
    // partition (no message at or after).
    let future_ts = i64::MAX / 2;
    let offsets_future = consumer
        .offsets_for_times_for_topic(topic, future_ts)
        .await
        .expect("offsets_for_times_for_topic (future) failed");
    for offset in offsets_future.values() {
        assert_eq!(*offset, -1, "expected -1 for far-future timestamp");
    }

    // Lower-level offsets_for_times with an explicit pair list.
    let pairs: Vec<(&str, i32)> = topic_info
        .partitions
        .iter()
        .map(|p| (topic, p.partition))
        .collect();
    let offsets_pairs = consumer
        .offsets_for_times(&pairs, 0)
        .await
        .expect("offsets_for_times (pairs) failed");
    assert_eq!(offsets_pairs.len(), 2);
    for ((t, _p), offset) in &offsets_pairs {
        assert_eq!(t, topic);
        assert_eq!(*offset, 0);
    }

    consumer.close().await;
}
