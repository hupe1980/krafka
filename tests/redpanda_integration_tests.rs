//! Integration tests against Redpanda.
//!
//! Redpanda speaks the Kafka wire protocol, and krafka negotiates every API
//! version instead of pinning them — so compatibility is expected, not hoped
//! for. This suite pins the expectation against a real Redpanda broker:
//!
//! - produce/consume round trip (version negotiation across Produce, Fetch,
//!   Metadata, ApiVersions)
//! - consumer-group subscribe → poll → commit
//! - transactions: Redpanda does not implement KIP-890 transaction version 2,
//!   so the TV probe must land on **TV1** and the explicit
//!   `AddPartitionsToTxn` path must work end to end
//! - admin: create/list/delete topics, describe cluster
//!
//! These tests require Docker and are ignored by default:
//!
//! ```sh
//! just integration-redpanda
//! # or
//! cargo test --test redpanda_integration_tests -- --ignored --test-threads=1
//! ```
//!
//! Image is read from `REDPANDA_IMAGE` (default `redpandadata/redpanda`), tag
//! from `REDPANDA_VERSION` (default `latest`).

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::borrow::Cow;
use std::collections::HashMap;
use std::time::Duration;

use testcontainers::core::{ContainerPort, ContainerState, ExecCommand, WaitFor};
use testcontainers::{ContainerAsync, Image, runners::AsyncRunner};

/// Time to wait after container start for Redpanda to stabilize.
const CONTAINER_SETTLE: Duration = Duration::from_secs(5);

const KAFKA_PORT: ContainerPort = ContainerPort::Tcp(9092);
const START_SCRIPT: &str = "/tmp/testcontainers_start.sh";

/// Minimal [`Image`] for `redpandadata/redpanda`, using the same
/// start-script pattern as the Apache Kafka harness (and the Java
/// testcontainers Redpanda module): the advertised Kafka address needs the
/// *mapped host port*, which is only known after the container starts, so the
/// entrypoint waits for a script that `exec_after_start` writes.
#[derive(Debug, Clone)]
struct Redpanda {
    image: String,
    tag: String,
    env_vars: HashMap<String, String>,
}

impl Redpanda {
    fn new(image: impl Into<String>, tag: impl Into<String>) -> Self {
        Self {
            image: image.into(),
            tag: tag.into(),
            env_vars: HashMap::new(),
        }
    }
}

impl Image for Redpanda {
    fn name(&self) -> &str {
        &self.image
    }

    fn tag(&self) -> &str {
        &self.tag
    }

    fn ready_conditions(&self) -> Vec<WaitFor> {
        // Readiness is checked via `exec_after_start` container-level
        // conditions, once the start script has been written.
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
             /usr/bin/rpk redpanda start \
             --mode dev-container \
             --smp 1 \
             --memory 1G \
             --kafka-addr PLAINTEXT://0.0.0.0:{} \
             --advertise-kafka-addr PLAINTEXT://127.0.0.1:{host_port}\n",
            KAFKA_PORT.as_u16()
        );
        let cmd = vec![
            "sh".to_string(),
            "-c".to_string(),
            format!("echo '{script}' > {START_SCRIPT}"),
        ];
        let ready = vec![WaitFor::message_on_stderr("Successfully started Redpanda!")];
        Ok(vec![
            ExecCommand::new(cmd).with_container_ready_conditions(ready),
        ])
    }
}

/// Start a Redpanda container and return it with its bootstrap address.
async fn redpanda_container() -> (ContainerAsync<Redpanda>, String) {
    let image =
        std::env::var("REDPANDA_IMAGE").unwrap_or_else(|_| "redpandadata/redpanda".to_string());
    let tag = std::env::var("REDPANDA_VERSION").unwrap_or_else(|_| "latest".to_string());

    let max_attempts = 3;
    let mut last_err = None;
    for attempt in 1..=max_attempts {
        match Redpanda::new(&image, &tag).start().await {
            Ok(container) => {
                tokio::time::sleep(CONTAINER_SETTLE).await;
                let host_port = container
                    .get_host_port_ipv4(KAFKA_PORT)
                    .await
                    .expect("Failed to get host port");
                return (container, format!("127.0.0.1:{host_port}"));
            }
            Err(e) => {
                eprintln!("Redpanda container start attempt {attempt}/{max_attempts} failed: {e}");
                last_err = Some(e);
                if attempt < max_attempts {
                    tokio::time::sleep(Duration::from_secs(2u64.pow(attempt))).await;
                }
            }
        }
    }
    panic!(
        "Failed to start Redpanda container after {max_attempts} attempts: {}",
        last_err.unwrap()
    );
}

/// Create a topic and wait briefly for metadata propagation.
async fn create_topic(bootstrap_servers: &str, topic: &str, partitions: i32) {
    use krafka::admin::{AdminClient, NewTopic};

    let admin = AdminClient::builder()
        .bootstrap_servers(bootstrap_servers)
        .client_id("redpanda-test-admin")
        .build()
        .await
        .expect("Failed to create admin client");

    admin
        .create_topics(
            vec![NewTopic::new(topic, partitions, 1).unwrap()],
            Duration::from_secs(10),
            false,
        )
        .await
        .expect("Failed to create topic");
    admin.close().await;
    tokio::time::sleep(Duration::from_secs(1)).await;
}

/// Poll until at least `min_records` arrive or `max_polls` is exhausted.
async fn poll_for_records(
    consumer: &krafka::consumer::Consumer,
    min_records: usize,
    timeout: Duration,
    max_polls: u32,
) -> Vec<krafka::consumer::ConsumerRecord> {
    let mut records = Vec::new();
    for _ in 0..max_polls {
        let batch = consumer.poll(timeout).await.expect("poll failed");
        records.extend(batch);
        if records.len() >= min_records {
            break;
        }
    }
    records
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn redpanda_produce_consume_round_trip() {
    use krafka::consumer::{AutoOffsetReset, Consumer};
    use krafka::producer::Producer;

    let (_container, bootstrap_servers) = redpanda_container().await;
    let topic = "rp-round-trip";
    create_topic(&bootstrap_servers, topic, 1).await;

    let producer = Producer::builder()
        .bootstrap_servers(&bootstrap_servers)
        .client_id("rp-producer")
        .build()
        .await
        .expect("Failed to create producer");

    // Idempotence is on by default; Redpanda supports it.
    let metadata = producer
        .send(topic, Some(b"rp-key"), b"rp-value")
        .await
        .expect("Failed to send message");
    assert!(metadata.offset >= 0);

    let consumer = Consumer::builder()
        .bootstrap_servers(&bootstrap_servers)
        .group_id("rp-group")
        .auto_offset_reset(AutoOffsetReset::Earliest)
        .build()
        .await
        .expect("Failed to create consumer");
    consumer
        .subscribe(&[topic])
        .await
        .expect("Failed to subscribe");

    let records = poll_for_records(&consumer, 1, Duration::from_secs(5), 10).await;
    assert!(!records.is_empty(), "Expected at least one record");
    assert_eq!(records[0].key_str(), Some("rp-key"));
    assert_eq!(records[0].value_str(), Some("rp-value"));

    consumer.commit().await.expect("commit failed");
    consumer.close().await.expect("consumer close");
    producer.close().await;
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn redpanda_admin_topic_lifecycle() {
    use krafka::admin::{AdminClient, NewTopic};

    let (_container, bootstrap_servers) = redpanda_container().await;

    let admin = AdminClient::builder()
        .bootstrap_servers(&bootstrap_servers)
        .client_id("rp-admin")
        .build()
        .await
        .expect("Failed to create admin client");

    let topic = "rp-admin-topic";
    admin
        .create_topics(
            vec![NewTopic::new(topic, 3, 1).unwrap()],
            Duration::from_secs(10),
            false,
        )
        .await
        .expect("Failed to create topic");
    tokio::time::sleep(Duration::from_secs(1)).await;

    let topics = admin.list_topics().await.expect("Failed to list topics");
    assert!(topics.iter().any(|t| t == topic), "Topic not in list");

    let cluster = admin
        .describe_cluster()
        .await
        .expect("Failed to describe cluster");
    assert!(!cluster.brokers.is_empty(), "No brokers found");

    admin
        .delete_topics(vec![topic.to_string()], Duration::from_secs(10))
        .await
        .expect("Failed to delete topic");
    admin.close().await;
}

/// Redpanda does not implement KIP-890 transaction version 2 server-side, so
/// the TV probe must negotiate **TV1** and the classic explicit
/// `AddPartitionsToTxn` transaction flow must work end to end — including
/// `read_committed` visibility after the commit.
#[tokio::test]
#[ignore = "requires Docker"]
async fn redpanda_transactions_fall_back_to_tv1() {
    use krafka::consumer::{AutoOffsetReset, Consumer, IsolationLevel};
    use krafka::producer::{TransactionVersion, TransactionalProducer};

    let (_container, bootstrap_servers) = redpanda_container().await;
    let topic = "rp-txn-topic";
    create_topic(&bootstrap_servers, topic, 1).await;

    let producer = TransactionalProducer::builder()
        .bootstrap_servers(&bootstrap_servers)
        .transactional_id("rp-txn-1")
        .client_id("rp-txn-producer")
        .build()
        .await
        .expect("Failed to create transactional producer");

    producer
        .init_transactions()
        .await
        .expect("init_transactions failed");

    assert_eq!(
        producer.transaction_version(),
        TransactionVersion::V1,
        "Redpanda does not finalize transaction.version=2; the probe must \
         fall back to TV1"
    );

    producer.begin_transaction().expect("begin failed");
    let _metadata = producer
        .send(topic, Some(b"txn-key"), b"txn-value")
        .await
        .expect("transactional send failed");
    producer
        .commit_transaction()
        .await
        .expect("commit_transaction failed");

    let consumer = Consumer::builder()
        .bootstrap_servers(&bootstrap_servers)
        .group_id("rp-txn-group")
        .auto_offset_reset(AutoOffsetReset::Earliest)
        .isolation_level(IsolationLevel::ReadCommitted)
        .build()
        .await
        .expect("Failed to create consumer");
    consumer
        .subscribe(&[topic])
        .await
        .expect("Failed to subscribe");

    let records = poll_for_records(&consumer, 1, Duration::from_secs(5), 10).await;
    assert!(
        !records.is_empty(),
        "A committed transaction must be visible under read_committed"
    );
    assert_eq!(records[0].value_str(), Some("txn-value"));

    consumer.close().await.expect("consumer close");
    producer.close().await;
}
