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
//! - consumer close: the fetch-session close must be answered, so closing a
//!   consumer does not wedge a connection shared through one `Kafka` handle
//!
//! These tests require Docker and are ignored by default:
//!
//! ```sh
//! just integration-redpanda
//! # or
//! cargo test --test redpanda_integration_tests -- --ignored --test-threads=1
//! ```
//!
//! The image is the release pinned in `tests/redpanda/Dockerfile`;
//! `REDPANDA_VERSION` overrides the tag (`REDPANDA_VERSION=latest`). One
//! container serves every test in this binary.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::borrow::Cow;
use std::collections::HashMap;
use std::sync::OnceLock;
use std::time::Duration;

use testcontainers::core::{ContainerPort, ContainerState, ExecCommand, WaitFor};
use testcontainers::{Image, ImageExt, runners::AsyncRunner};

/// The pinned release: the `FROM` line of this file is the one record of it.
const PINNED: &str = include_str!("redpanda/Dockerfile");

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

/// The image the suite runs: `REDPANDA_IMAGE_REF` (set by
/// `just integration-redpanda`), else the pin with `REDPANDA_VERSION` as tag.
fn image_ref() -> (String, String) {
    let reference = std::env::var("REDPANDA_IMAGE_REF").unwrap_or_else(|_| {
        let pinned = PINNED
            .lines()
            .find_map(|l| l.strip_prefix("FROM "))
            .expect("tests/redpanda/Dockerfile has a FROM line")
            .trim()
            .to_string();
        match std::env::var("REDPANDA_VERSION") {
            Ok(tag) => format!("{}:{tag}", pinned.rsplit_once(':').unwrap().0),
            Err(_) => pinned,
        }
    });
    let (image, tag) = reference.rsplit_once(':').expect("image:tag");
    (image.to_string(), tag.to_string())
}

static REDPANDA: OnceLock<Result<String, String>> = OnceLock::new();

/// The bootstrap address of the Redpanda broker shared by every test.
///
/// Started once, on a thread whose runtime keeps the container alive for the
/// life of the process; `just integration-redpanda` removes it by its label.
fn redpanda() -> String {
    REDPANDA
        .get_or_init(|| {
            let (tx, rx) = std::sync::mpsc::channel();
            std::thread::spawn(move || {
                let rt = tokio::runtime::Builder::new_multi_thread()
                    .enable_all()
                    .build()
                    .expect("runtime for the shared broker");
                rt.block_on(async move {
                    let (image, tag) = image_ref();
                    eprintln!("Redpanda image: {image}:{tag}");
                    let started = Redpanda::new(image, tag)
                        .with_labels([("krafka.test-suite", "redpanda_integration_tests")])
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
                            tx.send(Err(e.to_string())).ok();
                        }
                    }
                });
            });
            rx.recv()
                .unwrap_or_else(|_| Err("the broker thread exited".into()))
        })
        .clone()
        .unwrap_or_else(|e| panic!("Redpanda did not start: {e}"))
}

/// Create a topic and wait briefly for metadata propagation.
async fn create_topic(bootstrap_servers: &str, topic: &str, partitions: i32) {
    use krafka::admin::NewTopic;

    let admin = krafka::Kafka::builder(bootstrap_servers)
        .client_id("redpanda-test-admin")
        .connect()
        .await
        .expect("Failed to create admin client")
        .admin();

    admin
        .create_topics(
            vec![NewTopic::new(topic, partitions, 1).unwrap()],
            Default::default(),
        )
        .await
        .expect("Failed to create topic");
    admin.close().await.unwrap();
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
    use krafka::consumer::AutoOffsetReset;

    let bootstrap_servers = redpanda();
    let topic = "rp-round-trip";
    create_topic(&bootstrap_servers, topic, 1).await;

    let producer = krafka::Kafka::builder(&bootstrap_servers)
        .client_id("rp-producer")
        .connect()
        .await
        .expect("Failed to create producer")
        .producer()
        .build()
        .await
        .expect("Failed to create producer");

    // Idempotence is on by default; Redpanda supports it.
    let metadata = producer
        .send(krafka::Record::new(topic, "rp-value").key("rp-key"))
        .await
        .expect("Failed to send message");
    assert!(metadata.offset >= 0);

    let consumer = krafka::Kafka::builder(&bootstrap_servers)
        .connect()
        .await
        .expect("Failed to create consumer")
        .consumer("rp-group")
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
    producer.close().await.unwrap();
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn redpanda_admin_topic_lifecycle() {
    use krafka::admin::NewTopic;

    let bootstrap_servers = redpanda();

    let admin = krafka::Kafka::builder(&bootstrap_servers)
        .client_id("rp-admin")
        .connect()
        .await
        .expect("Failed to create admin client")
        .admin();

    let topic = "rp-admin-topic";
    admin
        .create_topics(
            vec![NewTopic::new(topic, 3, 1).unwrap()],
            Default::default(),
        )
        .await
        .expect("Failed to create topic");
    tokio::time::sleep(Duration::from_secs(1)).await;

    let topics = admin
        .list_topics(Default::default())
        .await
        .expect("Failed to list topics");
    assert!(topics.iter().any(|t| t == topic), "Topic not in list");

    let cluster = admin
        .describe_cluster(Default::default())
        .await
        .expect("Failed to describe cluster");
    assert!(!cluster.brokers.is_empty(), "No brokers found");

    admin
        .delete_topics(
            vec![topic.to_string()],
            krafka::admin::DeleteTopicsOptions::default(),
        )
        .await
        .expect("Failed to delete topic");
    admin.close().await.unwrap();
}

/// Redpanda does not implement KIP-890 transaction version 2 server-side, so
/// the TV probe must negotiate **TV1** and the classic explicit
/// `AddPartitionsToTxn` transaction flow must work end to end — including
/// `read_committed` visibility after the commit.
#[tokio::test]
#[ignore = "requires Docker"]
async fn redpanda_transactions_fall_back_to_tv1() {
    use krafka::consumer::{AutoOffsetReset, IsolationLevel};
    use krafka::producer::TransactionVersion;

    let bootstrap_servers = redpanda();
    let topic = "rp-txn-topic";
    create_topic(&bootstrap_servers, topic, 1).await;

    let producer = krafka::Kafka::builder(&bootstrap_servers)
        .client_id("rp-txn-producer")
        .connect()
        .await
        .expect("Failed to create transactional producer")
        .producer()
        .build_transactional("rp-txn-1")
        .await
        .expect("Failed to create transactional producer");

    assert_eq!(
        producer.transaction_version(),
        TransactionVersion::V1,
        "Redpanda does not finalize transaction.version=2; the probe must \
         fall back to TV1"
    );

    producer.begin().expect("begin failed");
    let _metadata = producer
        .send(krafka::Record::new(topic, "txn-value").key("txn-key"))
        .await
        .expect("transactional send failed");
    producer.commit().await.expect("commit failed");

    let consumer = krafka::Kafka::builder(&bootstrap_servers)
        .connect()
        .await
        .expect("Failed to create consumer")
        .consumer("rp-txn-group")
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
    producer.close().await.unwrap();
}

/// Closing a consumer leaves the connection it shares usable: Redpanda must
/// answer the fetch-session close (it ignores one with `max_wait_ms = 0`).
#[tokio::test]
#[ignore = "requires Docker"]
async fn redpanda_consumer_close_does_not_wedge_shared_connection() {
    use krafka::consumer::AutoOffsetReset;

    use std::time::Instant;

    let bootstrap_servers = redpanda();
    let topic = "rp-close-shared";
    create_topic(&bootstrap_servers, topic, 1).await;

    // A short request timeout keeps a regression from hiding behind a slow
    // but eventually successful close.
    let request_timeout = Duration::from_secs(10);
    let client = krafka::Kafka::builder(&bootstrap_servers)
        .request_timeout(request_timeout)
        .connect()
        .await
        .expect("Failed to create client");
    let producer = client
        .producer()
        .build()
        .await
        .expect("Failed to create producer");
    let _ = producer
        .send(krafka::Record::new(topic, "before-close").key("k"))
        .await
        .expect("Failed to send message");

    let consumer = client
        .consumer("rp-close-shared-group")
        .auto_offset_reset(AutoOffsetReset::Earliest)
        .build()
        .await
        .expect("Failed to create consumer");
    consumer
        .subscribe(&[topic])
        .await
        .expect("Failed to subscribe");
    let records = poll_for_records(&consumer, 1, Duration::from_secs(2), 10).await;
    assert!(!records.is_empty(), "Expected at least one record");
    // One more round so the broker has an established incremental session.
    consumer
        .poll(Duration::from_millis(300))
        .await
        .expect("poll failed");

    let started = Instant::now();
    consumer.close().await.expect("consumer close");
    let close_took = started.elapsed();
    assert!(
        close_took < Duration::from_secs(5),
        "close() took {close_took:?}: the fetch-session close went unanswered"
    );

    // The shared connection must still serve requests.
    let started = Instant::now();
    let metadata = tokio::time::timeout(
        request_timeout,
        producer.send(krafka::Record::new(topic, "after-close").key("k")),
    )
    .await
    .expect("produce after consumer close hung on the shared connection")
    .expect("produce after consumer close failed");
    assert!(metadata.offset >= 1);
    assert!(
        started.elapsed() < Duration::from_secs(2),
        "produce after close took {:?}",
        started.elapsed()
    );

    producer.close().await.unwrap();
    drop(client);
}
