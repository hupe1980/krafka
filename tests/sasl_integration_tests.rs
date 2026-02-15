//! SASL authentication integration tests for Krafka.
//!
//! These tests verify end-to-end authentication through Producer, Consumer,
//! and AdminClient against a Kafka broker configured with SASL_PLAINTEXT.
//!
//! Run with:
//! ```
//! cargo test --test sasl_integration_tests -- --ignored
//! ```
//!
//! Note: These tests are ignored by default as they require Docker.

use std::borrow::Cow;
use std::collections::HashMap;
use std::time::Duration;

use testcontainers::core::{ContainerPort, ContainerState, ExecCommand, WaitFor};
use testcontainers::{ContainerAsync, Image, runners::AsyncRunner};

const KAFKA_PORT: ContainerPort = ContainerPort::Tcp(9093);
const ZOOKEEPER_PORT: ContainerPort = ContainerPort::Tcp(2181);

/// Kafka container configured with SASL_PLAINTEXT authentication.
///
/// Uses Confluent cp-kafka 7.5.0 with:
/// - SASL_PLAINTEXT on port 9093 (client-facing)
/// - PLAINTEXT on port 9092 (inter-broker)
/// - SASL/PLAIN mechanism with test credentials
#[derive(Debug, Clone)]
struct KafkaSasl {
    env_vars: HashMap<String, String>,
}

impl KafkaSasl {
    fn new() -> Self {
        let mut env_vars = HashMap::new();

        // ZooKeeper
        env_vars.insert(
            "KAFKA_ZOOKEEPER_CONNECT".to_string(),
            format!("localhost:{}", ZOOKEEPER_PORT.as_u16()),
        );

        // Listeners: SASL_PLAINTEXT for clients, BROKER (PLAINTEXT) for inter-broker
        env_vars.insert(
            "KAFKA_LISTENERS".to_string(),
            format!(
                "SASL_PLAINTEXT://0.0.0.0:{},BROKER://0.0.0.0:9092",
                KAFKA_PORT.as_u16()
            ),
        );
        env_vars.insert(
            "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP".to_string(),
            "BROKER:PLAINTEXT,SASL_PLAINTEXT:SASL_PLAINTEXT".to_string(),
        );
        env_vars.insert(
            "KAFKA_INTER_BROKER_LISTENER_NAME".to_string(),
            "BROKER".to_string(),
        );
        env_vars.insert(
            "KAFKA_ADVERTISED_LISTENERS".to_string(),
            format!(
                "SASL_PLAINTEXT://localhost:{},BROKER://localhost:9092",
                KAFKA_PORT.as_u16()
            ),
        );

        // SASL configuration
        env_vars.insert(
            "KAFKA_SASL_ENABLED_MECHANISMS".to_string(),
            "PLAIN".to_string(),
        );
        env_vars.insert(
            "KAFKA_LISTENER_NAME_SASL__PLAINTEXT_PLAIN_SASL_JAAS_CONFIG".to_string(),
            "org.apache.kafka.common.security.plain.PlainLoginModule required \
             username=\"admin\" \
             password=\"admin-secret\" \
             user_admin=\"admin-secret\" \
             user_testuser=\"testpassword\";"
                .to_string(),
        );

        // Single-broker configuration
        env_vars.insert("KAFKA_BROKER_ID".to_string(), "1".to_string());
        env_vars.insert(
            "KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR".to_string(),
            "1".to_string(),
        );
        env_vars.insert(
            "KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR".to_string(),
            "1".to_string(),
        );
        env_vars.insert(
            "KAFKA_TRANSACTION_STATE_LOG_MIN_ISR".to_string(),
            "1".to_string(),
        );
        env_vars.insert(
            "KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS".to_string(),
            "0".to_string(),
        );

        // Confluent Docker scripts require KAFKA_OPTS when SASL is enabled
        env_vars.insert(
            "KAFKA_OPTS".to_string(),
            "-Djava.security.auth.login.config=/tmp/kafka_jaas.conf".to_string(),
        );

        Self { env_vars }
    }
}

impl Image for KafkaSasl {
    fn name(&self) -> &str {
        "confluentinc/cp-kafka"
    }

    fn tag(&self) -> &str {
        "7.5.0"
    }

    fn ready_conditions(&self) -> Vec<WaitFor> {
        vec![WaitFor::message_on_stdout(
            "started (kafka.server.KafkaServer)",
        )]
    }

    fn env_vars(
        &self,
    ) -> impl IntoIterator<Item = (impl Into<Cow<'_, str>>, impl Into<Cow<'_, str>>)> {
        &self.env_vars
    }

    fn cmd(&self) -> impl IntoIterator<Item = impl Into<Cow<'_, str>>> {
        vec![
            "/bin/bash".to_owned(),
            "-c".to_owned(),
            format!(
                r#"
cat > /tmp/kafka_jaas.conf << 'EOF'
KafkaServer {{
    org.apache.kafka.common.security.plain.PlainLoginModule required
    username="admin"
    password="admin-secret"
    user_admin="admin-secret"
    user_testuser="testpassword";
}};
EOF
echo 'clientPort={zk_port}' > zookeeper.properties;
echo 'dataDir=/var/lib/zookeeper/data' >> zookeeper.properties;
echo 'dataLogDir=/var/lib/zookeeper/log' >> zookeeper.properties;
zookeeper-server-start zookeeper.properties &
. /etc/confluent/docker/bash-config &&
/etc/confluent/docker/configure &&
/etc/confluent/docker/launch"#,
                zk_port = ZOOKEEPER_PORT.as_u16()
            ),
        ]
    }

    fn expose_ports(&self) -> &[ContainerPort] {
        &[KAFKA_PORT]
    }

    fn exec_after_start(
        &self,
        cs: ContainerState,
    ) -> Result<Vec<ExecCommand>, testcontainers::TestcontainersError> {
        let mapped_port = cs.host_port_ipv4(KAFKA_PORT)?;
        let cmd = vec![
            "kafka-configs".to_string(),
            "--alter".to_string(),
            "--bootstrap-server".to_string(),
            "0.0.0.0:9092".to_string(),
            "--entity-type".to_string(),
            "brokers".to_string(),
            "--entity-name".to_string(),
            "1".to_string(),
            "--add-config".to_string(),
            format!(
                "advertised.listeners=[SASL_PLAINTEXT://127.0.0.1:{mapped_port},BROKER://localhost:9092]"
            ),
        ];
        let ready_conditions = vec![WaitFor::message_on_stdout(
            "Checking need to trigger auto leader balancing",
        )];
        Ok(vec![
            ExecCommand::new(cmd).with_container_ready_conditions(ready_conditions),
        ])
    }
}

/// Start a Kafka container with SASL_PLAINTEXT authentication.
async fn kafka_sasl_container() -> (ContainerAsync<KafkaSasl>, String) {
    let container = KafkaSasl::new()
        .start()
        .await
        .expect("Failed to start SASL Kafka container");

    // Wait for Kafka to be fully ready
    tokio::time::sleep(Duration::from_secs(20)).await;

    let host_port = container
        .get_host_port_ipv4(KAFKA_PORT)
        .await
        .expect("Failed to get host port");

    let bootstrap_servers = format!("127.0.0.1:{host_port}");
    (container, bootstrap_servers)
}

// ============================================================================
// SASL Integration Tests
// ============================================================================

#[tokio::test]
#[ignore = "requires Docker"]
async fn test_sasl_admin_client() {
    use krafka::admin::{AdminClient, NewTopic};

    let (_container, bootstrap_servers) = kafka_sasl_container().await;

    // Connect with valid SASL credentials
    let admin = AdminClient::builder()
        .bootstrap_servers(&bootstrap_servers)
        .sasl_plain("testuser", "testpassword")
        .build()
        .await
        .expect("Failed to create SASL admin client");

    // Create a topic
    let topic = "sasl-admin-test";
    admin
        .create_topics(vec![NewTopic::new(topic, 1, 1)], Duration::from_secs(10))
        .await
        .expect("Failed to create topic via SASL");

    // Verify topic exists
    let topics = admin.list_topics().await.expect("Failed to list topics");
    assert!(
        topics.iter().any(|t| t == topic),
        "Created topic should appear in topic list"
    );

    // Clean up
    admin
        .delete_topics(vec![topic.to_string()], Duration::from_secs(10))
        .await
        .ok();
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn test_sasl_producer_consumer() {
    use krafka::admin::{AdminClient, NewTopic};
    use krafka::consumer::{AutoOffsetReset, Consumer};
    use krafka::producer::Producer;

    let (_container, bootstrap_servers) = kafka_sasl_container().await;
    let topic = "sasl-produce-consume-test";

    // Create topic with admin client
    let admin = AdminClient::builder()
        .bootstrap_servers(&bootstrap_servers)
        .sasl_plain("admin", "admin-secret")
        .build()
        .await
        .expect("Failed to create admin client");

    admin
        .create_topics(vec![NewTopic::new(topic, 1, 1)], Duration::from_secs(10))
        .await
        .expect("Failed to create topic");

    tokio::time::sleep(Duration::from_secs(2)).await;

    // Produce messages with SASL auth
    let producer = Producer::builder()
        .bootstrap_servers(&bootstrap_servers)
        .client_id("sasl-producer")
        .sasl_plain("testuser", "testpassword")
        .build()
        .await
        .expect("Failed to create SASL producer");

    for i in 0..5 {
        let _ = producer
            .send(
                topic,
                Some(format!("key-{i}").as_bytes()),
                format!("value-{i}").as_bytes(),
            )
            .await
            .expect("Failed to send message via SASL");
    }

    // Consume messages with SASL auth
    let consumer = Consumer::builder()
        .bootstrap_servers(&bootstrap_servers)
        .client_id("sasl-consumer")
        .group_id("sasl-test-group")
        .auto_offset_reset(AutoOffsetReset::Earliest)
        .sasl_plain("testuser", "testpassword")
        .build()
        .await
        .expect("Failed to create SASL consumer");

    // Retry subscribe — group coordinator may not be ready immediately
    for attempt in 0..10 {
        match consumer.subscribe(&[topic]).await {
            Ok(_) => break,
            Err(e) if attempt < 9 => {
                eprintln!("Subscribe attempt {attempt} failed: {e}, retrying...");
                tokio::time::sleep(Duration::from_secs(2)).await;
            }
            Err(e) => panic!("Failed to subscribe after retries: {e}"),
        }
    }

    // Poll for messages
    let mut received = 0;
    for _ in 0..10 {
        let records = consumer
            .poll(Duration::from_secs(2))
            .await
            .unwrap_or_default();
        received += records.len();
        if received >= 5 {
            break;
        }
    }

    assert_eq!(received, 5, "Should receive all 5 messages via SASL");

    producer.close().await;
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn test_sasl_wrong_credentials_rejected() {
    let (_container, bootstrap_servers) = kafka_sasl_container().await;

    // Try connecting with wrong password — should fail during SASL handshake
    let result = krafka::admin::AdminClient::builder()
        .bootstrap_servers(&bootstrap_servers)
        .sasl_plain("testuser", "wrong-password")
        .build()
        .await;

    assert!(
        result.is_err(),
        "Connection with wrong SASL credentials should fail"
    );
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn test_sasl_no_auth_rejected() {
    let (_container, bootstrap_servers) = kafka_sasl_container().await;

    // Try connecting without any auth to a SASL-required broker
    // This should fail because the broker expects SASL handshake
    let result = krafka::admin::AdminClient::builder()
        .bootstrap_servers(&bootstrap_servers)
        .build()
        .await;

    assert!(
        result.is_err(),
        "Connection without auth to SASL-required broker should fail"
    );
}
