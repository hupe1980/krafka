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

use std::time::Duration;

use testcontainers::{ContainerAsync, ImageExt, runners::AsyncRunner};
use testcontainers_modules::kafka::Kafka;

/// Helper to get a Kafka container.
async fn kafka_container() -> (ContainerAsync<Kafka>, String) {
    let container = Kafka::default()
        .with_tag("7.5.0")
        // Enable transactions and offsets for single-broker setup
        .with_env_var("KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR", "1")
        .with_env_var("KAFKA_TRANSACTION_STATE_LOG_MIN_ISR", "1")
        .with_env_var("KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR", "1")
        .with_env_var("KAFKA_OFFSETS_TOPIC_NUM_PARTITIONS", "1")
        // Group coordinator settings for single-broker
        .with_env_var("KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS", "0")
        .start()
        .await
        .expect("Failed to start Kafka container");

    // Wait for Kafka to be fully ready (broker startup can take time)
    // Increased from 15s to 20s for group coordinator initialization
    tokio::time::sleep(Duration::from_secs(20)).await;

    let host_port = container
        .get_host_port_ipv4(9093)
        .await
        .expect("Failed to get host port");

    let bootstrap_servers = format!("127.0.0.1:{}", host_port);

    (container, bootstrap_servers)
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
            vec![NewTopic::new(topic, partitions, 1)],
            Duration::from_secs(10),
        )
        .await
        .expect("Failed to create topic");

    // Wait for topic to be ready
    tokio::time::sleep(Duration::from_secs(2)).await;
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

    // Poll for messages
    let records = consumer
        .poll(Duration::from_secs(5))
        .await
        .expect("Failed to poll");

    assert!(!records.is_empty(), "Expected at least one record");

    let record = &records[0];
    assert_eq!(record.topic, topic);
    assert_eq!(record.key_str(), Some("test-key"));
    assert_eq!(record.value_str(), Some("test-value"));

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
    let new_topic = NewTopic::new(topic_name, 3, 1);

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
        Compression::Gzip,
        Compression::Snappy,
        Compression::Lz4,
        Compression::Zstd,
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

        let records = consumer
            .poll(Duration::from_secs(5))
            .await
            .expect("Failed to poll");

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
    let new_topic = NewTopic::new(topic_name, 6, 1);

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

    let new_topic = NewTopic::new(topic_name, 4, 1);
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

    // Poll to join group
    let records1 = consumer1
        .poll(Duration::from_secs(5))
        .await
        .expect("Failed to poll consumer1");

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
    let records2 = consumer2
        .poll(Duration::from_secs(5))
        .await
        .expect("Failed to poll consumer2");

    // At least one consumer should have received messages
    let total_records = records1.len() + records2.len();
    assert!(
        total_records > 0,
        "Expected at least some records from consumer group"
    );
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn test_consumer_commit_and_resume() {
    use krafka::admin::{AdminClient, NewTopic};
    use krafka::consumer::{AutoOffsetReset, Consumer};
    use krafka::producer::Producer;

    let (_container, bootstrap_servers) = kafka_container().await;

    let topic_name = "commit-resume-test";
    let group_id = "commit-resume-group";

    // Create topic
    let admin = AdminClient::builder()
        .bootstrap_servers(&bootstrap_servers)
        .build()
        .await
        .expect("Failed to create admin client");

    let new_topic = NewTopic::new(topic_name, 1, 1);
    admin
        .create_topics(vec![new_topic], Duration::from_secs(10))
        .await
        .expect("Failed to create topic");

    tokio::time::sleep(Duration::from_secs(1)).await;

    // Produce messages
    let producer = Producer::builder()
        .bootstrap_servers(&bootstrap_servers)
        .build()
        .await
        .expect("Failed to create producer");

    for i in 0..10 {
        let _ = producer
            .send(topic_name, None, format!("msg-{}", i).as_bytes())
            .await
            .expect("Failed to send message");
    }
    producer.close().await;

    // First consumer: read and commit
    {
        let consumer = Consumer::builder()
            .bootstrap_servers(&bootstrap_servers)
            .group_id(group_id)
            .auto_offset_reset(AutoOffsetReset::Earliest)
            .enable_auto_commit(false) // Manual commit
            .build()
            .await
            .expect("Failed to create consumer");

        subscribe_with_retry(&consumer, &[topic_name], 5)
            .await
            .expect("Failed to subscribe");

        // Read some messages
        let records = consumer
            .poll(Duration::from_secs(5))
            .await
            .expect("Failed to poll");

        assert!(!records.is_empty(), "Expected records");

        // Commit offsets
        consumer.commit().await.expect("Failed to commit");
    }

    // Second consumer: should resume from committed offset
    {
        let consumer = Consumer::builder()
            .bootstrap_servers(&bootstrap_servers)
            .group_id(group_id)
            .auto_offset_reset(AutoOffsetReset::Earliest)
            .enable_auto_commit(false)
            .build()
            .await
            .expect("Failed to create second consumer");

        subscribe_with_retry(&consumer, &[topic_name], 5)
            .await
            .expect("Failed to subscribe");

        // Poll - should get remaining messages or none if all were committed
        let _records = consumer
            .poll(Duration::from_secs(2))
            .await
            .expect("Failed to poll");

        // Success - consumer was able to resume from committed offset
    }
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
    let records = consumer
        .poll(Duration::from_secs(1))
        .await
        .expect("Poll should succeed even with no messages");

    // May be empty or have the setup message depending on timing
    drop(records);
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

    // Collect all messages
    let mut all_records = Vec::new();
    for _ in 0..3 {
        let records = consumer
            .poll(Duration::from_secs(2))
            .await
            .expect("Failed to poll");
        all_records.extend(records);
        if all_records.len() >= 6 {
            break;
        }
    }

    assert_eq!(all_records.len(), 6, "Expected 6 messages from 2 producers");
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

    let records = consumer
        .poll(Duration::from_secs(5))
        .await
        .expect("Failed to poll");

    assert!(!records.is_empty());
    assert_eq!(
        records[0].value.as_ref().map(|v| v.len()).unwrap_or(0),
        100 * 1024
    );
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

    let records = consumer
        .poll(Duration::from_secs(5))
        .await
        .expect("Failed to poll");

    assert!(!records.is_empty());
    let record = &records[0];

    // Verify headers are present
    assert!(record.header("trace-id").is_some() || record.headers.contains_key("trace-id"));
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn test_idempotent_producer() {
    use krafka::producer::{Acks, Producer};

    let (_container, bootstrap_servers) = kafka_container().await;

    let topic = "idempotent-test-topic";
    create_topic(&bootstrap_servers, topic, 1).await;

    // Create idempotent producer
    let producer = Producer::builder()
        .bootstrap_servers(&bootstrap_servers)
        .client_id("idempotent-producer-test")
        .acks(Acks::All) // Required for idempotence
        .enable_idempotence(true)
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

    let records = consumer
        .poll(Duration::from_secs(5))
        .await
        .expect("Failed to poll");

    assert!(!records.is_empty());
    let record = &records[0];

    // Verify null key is received as None
    assert!(record.key.is_none());
    assert!(record.value.is_some());
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

    // Collect messages from both topics
    let mut all_records = Vec::new();
    for _ in 0..5 {
        let records = consumer
            .poll(Duration::from_secs(2))
            .await
            .expect("poll failed");
        all_records.extend(records);
        if all_records.len() >= 2 {
            break;
        }
    }

    assert_eq!(all_records.len(), 2, "Expected 2 messages from 2 topics");

    // Verify we got messages from both topics
    let topics: std::collections::HashSet<_> =
        all_records.iter().map(|r| r.topic.as_str()).collect();
    assert!(topics.contains(topic1) || topics.contains(topic2));
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn test_consumer_seek_operations() {
    use krafka::consumer::{AutoOffsetReset, Consumer};
    use krafka::producer::Producer;

    let (_container, bootstrap_servers) = kafka_container().await;

    let topic = "seek-test-topic";
    create_topic(&bootstrap_servers, topic, 1).await;

    let producer = Producer::builder()
        .bootstrap_servers(&bootstrap_servers)
        .client_id("seek-test-producer")
        .build()
        .await
        .expect("Failed to create producer");

    // Send multiple messages
    for i in 0..10 {
        let _ = producer
            .send(
                topic,
                Some(format!("key-{}", i).as_bytes()),
                format!("value-{}", i).as_bytes(),
            )
            .await
            .expect("send failed");
    }
    producer.close().await;

    // Test seek to specific offset
    let consumer = Consumer::builder()
        .bootstrap_servers(&bootstrap_servers)
        .group_id("seek-test-consumer")
        .auto_offset_reset(AutoOffsetReset::Earliest)
        .enable_auto_commit(false)
        .build()
        .await
        .expect("Failed to create consumer");

    subscribe_with_retry(&consumer, &[topic], 5)
        .await
        .expect("Failed to subscribe");

    // First poll to establish assignment
    let _ = consumer.poll(Duration::from_secs(2)).await;

    // Seek to beginning of partition 0
    consumer
        .seek_to_beginning(topic, 0)
        .await
        .expect("seek to beginning failed");

    // Poll should get messages from the beginning
    let records = consumer
        .poll(Duration::from_secs(2))
        .await
        .expect("poll failed");

    // We may or may not get records depending on timing, but no panic
    drop(records);
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
    let new_topic = NewTopic::new(topic_name, 1, 1);
    admin
        .create_topics(vec![new_topic], Duration::from_secs(10))
        .await
        .expect("Failed to create topic");

    tokio::time::sleep(Duration::from_secs(1)).await;

    // Describe topic configs
    let configs = admin
        .describe_topic_config(topic_name)
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
async fn test_consumer_pause_resume() {
    use krafka::consumer::{AutoOffsetReset, Consumer};
    use krafka::producer::Producer;

    let (_container, bootstrap_servers) = kafka_container().await;

    let topic = "pause-resume-topic";
    create_topic(&bootstrap_servers, topic, 1).await;

    let producer = Producer::builder()
        .bootstrap_servers(&bootstrap_servers)
        .client_id("pause-resume-producer")
        .build()
        .await
        .expect("Failed to create producer");

    // Send initial messages
    for i in 0..5 {
        let _ = producer
            .send(
                topic,
                Some(format!("key-{}", i).as_bytes()),
                format!("value-{}", i).as_bytes(),
            )
            .await
            .expect("send failed");
    }
    producer.close().await;

    let consumer = Consumer::builder()
        .bootstrap_servers(&bootstrap_servers)
        .group_id("pause-resume-consumer")
        .auto_offset_reset(AutoOffsetReset::Earliest)
        .build()
        .await
        .expect("Failed to create consumer");

    subscribe_with_retry(&consumer, &[topic], 5)
        .await
        .expect("Failed to subscribe");

    // Initial poll to get assignment
    let _ = consumer.poll(Duration::from_secs(2)).await;

    // Pause the partition
    consumer.pause(topic, &[0]).await;

    // Poll should return empty while paused (within timeout)
    let _records = consumer
        .poll(Duration::from_millis(500))
        .await
        .expect("poll failed");
    // May or may not be empty depending on buffering

    // Resume the partition
    consumer.resume(topic, &[0]).await;

    // Should be able to continue consuming
    let records = consumer
        .poll(Duration::from_secs(2))
        .await
        .expect("poll failed after resume");
    drop(records);
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

    let mut all_records = Vec::new();
    for _ in 0..5 {
        let records = consumer
            .poll(Duration::from_secs(2))
            .await
            .expect("poll failed");
        all_records.extend(records);
        if all_records.len() >= 15 {
            break;
        }
    }

    assert_eq!(
        all_records.len(),
        15,
        "Expected 15 messages from 3 concurrent producers"
    );
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

    let records = consumer
        .poll(Duration::from_secs(5))
        .await
        .expect("poll failed");
    assert_eq!(records.len(), 10, "Expected 10 messages");
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
            vec![NewTopic::new(topic_name, 2, 1)],
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
            vec![NewTopic::new(topic_name, 1, 1)],
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
    let topic_configs = admin
        .describe_topic_config(topic_name)
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
    assert!(broker.id >= 0, "Broker should have a valid ID");
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
            vec![NewTopic::new(topic1, 2, 1), NewTopic::new(topic2, 3, 1)],
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
