//! SASL integration tests against a real Apache Kafka broker in Docker.
//!
//! One KRaft broker per test binary, from `apache/kafka:$KAFKA_VERSION`
//! (default: the supported floor, 3.9.0; `KAFKA_IMAGE` overrides the image),
//! with four listeners:
//!
//! | listener    | protocol         | mechanisms                                     |
//! |-------------|------------------|------------------------------------------------|
//! | `BROKER`    | `PLAINTEXT`      | in-container only: setup and readiness          |
//! | `CLIENT`    | `SASL_PLAINTEXT` | PLAIN, SCRAM-SHA-256, SCRAM-SHA-512, OAUTHBEARER |
//! | `TLS`       | `SASL_SSL`       | the same four                                  |
//! | `PLAINONLY` | `SASL_PLAINTEXT` | PLAIN                                          |
//!
//! The TLS material (a throwaway CA and a broker certificate for `127.0.0.1`
//! and `localhost`) is generated inside the container by `keytool` at start
//! and never committed. SCRAM users are provisioned with the broker's own
//! `kafka-configs.sh`. OAUTHBEARER uses unsigned JWTs, which the broker's
//! default unsecured validator accepts.
//!
//! Every client below is configured through the public builders with the
//! mechanism's credentials and, over `SASL_SSL`, a trust anchor for the test
//! CA — nothing else. Every rejection asserts the error kind.
//!
//! ```sh
//! just integration-sasl                 # the supported floor
//! just integration-sasl-matrix          # every version in the SASL set
//! KAFKA_VERSION=4.3.1 just integration-sasl
//! ```

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::borrow::Cow;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::OnceLock;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use krafka::admin::{AdminClient, NewTopic};
use krafka::auth::{AuthConfig, TlsConfig};
use krafka::consumer::AutoOffsetReset;
use krafka::error::KrafkaError;
use testcontainers::core::{ContainerPort, ContainerState, ExecCommand, WaitFor};
use testcontainers::{ContainerAsync, Image, ImageExt, runners::AsyncRunner};

const CLIENT_PORT: ContainerPort = ContainerPort::Tcp(9093);
const TLS_PORT: ContainerPort = ContainerPort::Tcp(9094);
const PLAIN_ONLY_PORT: ContainerPort = ContainerPort::Tcp(9095);
const START_SCRIPT: &str = "/tmp/krafka_start.sh";
const SUITE_LABEL: (&str, &str) = ("krafka.test-suite", "sasl_integration_tests");

/// PLAIN user (from the JAAS configuration).
const PLAIN_USER: (&str, &str) = ("alice", "alice-secret");
/// SCRAM user with both SHA-256 and SHA-512 credentials.
const SCRAM_USER: (&str, &str) = ("bob", "bob-secret");
/// SCRAM user with a SHA-256 credential only.
const SCRAM_256_ONLY: (&str, &str) = ("carol", "carol-secret");

/// The broker start script: TLS material, then the image's own entrypoint. Written by `exec_after_start`, once the mapped host ports are
/// known for the advertised listeners.
fn start_script(client: u16, tls: u16, plain_only: u16) -> String {
    format!(
        r#"#!/usr/bin/env bash
set -euo pipefail
mkdir -p /tmp/tls && cd /tmp/tls
pw=krafka-test
keytool -genkeypair -alias ca -dname CN=krafka-test-ca -keyalg RSA -keysize 2048 -validity 2 \
  -ext bc:c -keystore ca.p12 -storetype PKCS12 -storepass $pw
keytool -exportcert -alias ca -keystore ca.p12 -storepass $pw -rfc > ca.pem
keytool -genkeypair -alias broker -dname CN=localhost -keyalg RSA -keysize 2048 -validity 2 \
  -keystore broker.p12 -storetype PKCS12 -storepass $pw
keytool -certreq -alias broker -keystore broker.p12 -storepass $pw \
  | keytool -gencert -alias ca -keystore ca.p12 -storepass $pw -validity 2 -rfc \
      -ext SAN=dns:localhost,ip:127.0.0.1 -ext EKU=serverAuth > broker.pem
keytool -importcert -noprompt -alias ca -file ca.pem -keystore broker.p12 -storepass $pw
keytool -importcert -noprompt -alias broker -file broker.pem -keystore broker.p12 -storepass $pw
export KAFKA_ADVERTISED_LISTENERS="BROKER://localhost:9092,CLIENT://127.0.0.1:{client},TLS://127.0.0.1:{tls},PLAINONLY://127.0.0.1:{plain_only},CONTROLLER://localhost:9096"
exec /etc/kafka/docker/run
"#,
    )
}

/// One JAAS configuration per listener and mechanism, as broker properties
/// (`listener.name.<listener>.<mechanism>.sasl.jaas.config`). The image maps
/// `KAFKA_*` variables to properties: `_` is `.`, `___` is `-`.
fn jaas_env() -> Vec<(String, String)> {
    let plain = format!(
        "org.apache.kafka.common.security.plain.PlainLoginModule required user_{}=\"{}\";",
        PLAIN_USER.0, PLAIN_USER.1
    );
    let scram = "org.apache.kafka.common.security.scram.ScramLoginModule required;".to_string();
    let oauth = "org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required \
                 unsecuredLoginStringClaim_sub=\"admin\";"
        .to_string();
    let mut env = Vec::new();
    for listener in ["CLIENT", "TLS", "PLAINONLY"] {
        env.push((
            format!("KAFKA_LISTENER_NAME_{listener}_PLAIN_SASL_JAAS_CONFIG"),
            plain.clone(),
        ));
        if listener == "PLAINONLY" {
            continue;
        }
        for mechanism in ["SCRAM___SHA___256", "SCRAM___SHA___512"] {
            env.push((
                format!("KAFKA_LISTENER_NAME_{listener}_{mechanism}_SASL_JAAS_CONFIG"),
                scram.clone(),
            ));
        }
        env.push((
            format!("KAFKA_LISTENER_NAME_{listener}_OAUTHBEARER_SASL_JAAS_CONFIG"),
            oauth.clone(),
        ));
    }
    env
}

#[derive(Debug, Clone)]
struct SaslKafka {
    image: String,
    tag: String,
    env_vars: HashMap<String, String>,
}

impl SaslKafka {
    fn new(image: String, tag: String) -> Self {
        let env: &[(&str, &str)] = &[
            ("CLUSTER_ID", "5L6g3nShT-eMCtK--X86sw"),
            ("KAFKA_NODE_ID", "1"),
            ("KAFKA_PROCESS_ROLES", "broker,controller"),
            (
                "KAFKA_LISTENERS",
                "BROKER://0.0.0.0:9092,CLIENT://0.0.0.0:9093,TLS://0.0.0.0:9094,\
                 PLAINONLY://0.0.0.0:9095,CONTROLLER://0.0.0.0:9096",
            ),
            (
                "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP",
                "BROKER:PLAINTEXT,CLIENT:SASL_PLAINTEXT,TLS:SASL_SSL,\
                 PLAINONLY:SASL_PLAINTEXT,CONTROLLER:PLAINTEXT",
            ),
            ("KAFKA_INTER_BROKER_LISTENER_NAME", "BROKER"),
            ("KAFKA_CONTROLLER_LISTENER_NAMES", "CONTROLLER"),
            ("KAFKA_CONTROLLER_QUORUM_VOTERS", "1@localhost:9096"),
            (
                "KAFKA_SASL_ENABLED_MECHANISMS",
                "PLAIN,SCRAM-SHA-256,SCRAM-SHA-512,OAUTHBEARER",
            ),
            (
                "KAFKA_LISTENER_NAME_PLAINONLY_SASL_ENABLED_MECHANISMS",
                "PLAIN",
            ),
            ("KAFKA_SSL_KEYSTORE_LOCATION", "/tmp/tls/broker.p12"),
            ("KAFKA_SSL_KEYSTORE_TYPE", "PKCS12"),
            ("KAFKA_SSL_KEYSTORE_PASSWORD", "krafka-test"),
            ("KAFKA_SSL_KEY_PASSWORD", "krafka-test"),
            ("KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR", "1"),
            ("KAFKA_OFFSETS_TOPIC_NUM_PARTITIONS", "1"),
            ("KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR", "1"),
            ("KAFKA_TRANSACTION_STATE_LOG_MIN_ISR", "1"),
            ("KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS", "0"),
        ];
        Self {
            image,
            tag,
            env_vars: env
                .iter()
                .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
                .chain(jaas_env())
                .collect(),
        }
    }
}

impl Image for SaslKafka {
    fn name(&self) -> &str {
        &self.image
    }

    fn tag(&self) -> &str {
        &self.tag
    }

    fn ready_conditions(&self) -> Vec<WaitFor> {
        // Readiness is the broker's own log line, checked after the start
        // script exists (`exec_after_start`).
        vec![]
    }

    fn entrypoint(&self) -> Option<&str> {
        Some("bash")
    }

    fn cmd(&self) -> impl IntoIterator<Item = impl Into<Cow<'_, str>>> {
        vec![
            "-c".to_string(),
            format!("while [ ! -f {START_SCRIPT} ]; do sleep 0.1; done; exec bash {START_SCRIPT}"),
        ]
    }

    fn env_vars(
        &self,
    ) -> impl IntoIterator<Item = (impl Into<Cow<'_, str>>, impl Into<Cow<'_, str>>)> {
        &self.env_vars
    }

    fn expose_ports(&self) -> &[ContainerPort] {
        &[CLIENT_PORT, TLS_PORT, PLAIN_ONLY_PORT]
    }

    fn exec_after_start(
        &self,
        cs: ContainerState,
    ) -> Result<Vec<ExecCommand>, testcontainers::TestcontainersError> {
        let script = start_script(
            cs.host_port_ipv4(CLIENT_PORT)?,
            cs.host_port_ipv4(TLS_PORT)?,
            cs.host_port_ipv4(PLAIN_ONLY_PORT)?,
        );
        let encoded = base64::engine::general_purpose::STANDARD.encode(script);
        let cmd = vec![
            "sh".to_string(),
            "-c".to_string(),
            format!(
                "echo {encoded} | base64 -d > {START_SCRIPT}.tmp && mv {START_SCRIPT}.tmp {START_SCRIPT}"
            ),
        ];
        let ready = vec![WaitFor::message_on_stdout("Kafka Server started")];
        Ok(vec![
            ExecCommand::new(cmd).with_container_ready_conditions(ready),
        ])
    }
}

/// The shared broker's addresses and the test CA.
#[derive(Debug, Clone)]
struct Broker {
    version: String,
    sasl_plaintext: String,
    sasl_ssl: String,
    plain_only: String,
    ca_pem: PathBuf,
}

static BROKER: OnceLock<Result<Broker, String>> = OnceLock::new();

/// The broker shared by every test in this binary.
///
/// It is started once, on a thread whose runtime keeps the container alive
/// for the life of the process; `just integration-sasl` removes it afterwards
/// by its label.
fn broker() -> Broker {
    BROKER
        .get_or_init(|| {
            let (tx, rx) = std::sync::mpsc::channel();
            std::thread::spawn(move || {
                let rt = tokio::runtime::Builder::new_multi_thread()
                    .enable_all()
                    .build()
                    .expect("runtime for the shared broker");
                rt.block_on(async move {
                    match start_broker().await {
                        Ok((container, broker)) => {
                            tx.send(Ok(broker)).ok();
                            std::future::pending::<()>().await;
                            drop(container);
                        }
                        Err(e) => {
                            tx.send(Err(e)).ok();
                        }
                    }
                });
            });
            rx.recv()
                .unwrap_or_else(|_| Err("the broker thread exited".into()))
        })
        .clone()
        .unwrap_or_else(|e| panic!("SASL broker did not start: {e}"))
}

async fn exec(container: &ContainerAsync<SaslKafka>, script: &str) -> Result<String, String> {
    let mut res = container
        .exec(ExecCommand::new(["bash", "-c", script]))
        .await
        .map_err(|e| format!("exec `{script}`: {e}"))?;
    let stdout = res.stdout_to_vec().await.map_err(|e| e.to_string())?;
    let stderr = res.stderr_to_vec().await.map_err(|e| e.to_string())?;
    match res.exit_code().await.map_err(|e| e.to_string())? {
        Some(0) | None => Ok(String::from_utf8_lossy(&stdout).into_owned()),
        Some(code) => Err(format!(
            "`{script}` exited {code}: {}{}",
            String::from_utf8_lossy(&stdout),
            String::from_utf8_lossy(&stderr)
        )),
    }
}

async fn start_broker() -> Result<(ContainerAsync<SaslKafka>, Broker), String> {
    let image = std::env::var("KAFKA_IMAGE").unwrap_or_else(|_| "apache/kafka".to_string());
    let version = std::env::var("KAFKA_VERSION").unwrap_or_else(|_| "3.9.0".to_string());
    let container = SaslKafka::new(image, version.clone())
        .with_labels([SUITE_LABEL])
        .with_startup_timeout(Duration::from_secs(180))
        .start()
        .await
        .map_err(|e| format!("start: {e}"))?;

    // SCRAM users through the broker's own tooling, over the internal
    // unauthenticated listener; one credential per request.
    let configs = "/opt/kafka/bin/kafka-configs.sh --bootstrap-server localhost:9092";
    for (user, password, mechanism) in [
        (SCRAM_USER.0, SCRAM_USER.1, "SCRAM-SHA-256"),
        (SCRAM_USER.0, SCRAM_USER.1, "SCRAM-SHA-512"),
        (SCRAM_256_ONLY.0, SCRAM_256_ONLY.1, "SCRAM-SHA-256"),
    ] {
        exec(
            &container,
            &format!(
                "{configs} --alter --entity-type users --entity-name {user} \
                 --add-config '{mechanism}=[password={password}]'"
            ),
        )
        .await?;
    }
    // Readiness for SCRAM: the broker reports both users' credentials.
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        let described = exec(
            &container,
            &format!("{configs} --describe --entity-type users"),
        )
        .await?;
        if described.contains(SCRAM_USER.0) && described.contains(SCRAM_256_ONLY.0) {
            break;
        }
        if Instant::now() > deadline {
            return Err(format!("SCRAM users not visible after 60 s: {described}"));
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }

    let ca = exec(&container, "cat /tmp/tls/ca.pem").await?;
    let ca_pem = std::env::temp_dir().join(format!("krafka-sasl-ca-{}.pem", std::process::id()));
    std::fs::write(&ca_pem, ca).map_err(|e| format!("write CA: {e}"))?;

    let port = |p| {
        let c = &container;
        async move { c.get_host_port_ipv4(p).await.map_err(|e| e.to_string()) }
    };
    let broker = Broker {
        version,
        sasl_plaintext: format!("127.0.0.1:{}", port(CLIENT_PORT).await?),
        sasl_ssl: format!("127.0.0.1:{}", port(TLS_PORT).await?),
        plain_only: format!("127.0.0.1:{}", port(PLAIN_ONLY_PORT).await?),
        ca_pem,
    };
    eprintln!("SASL broker: Kafka {} — {broker:?}", broker.version);
    Ok((container, broker))
}

// ---------------------------------------------------------------------------
// Mechanisms and transports
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy)]
enum Mechanism {
    Plain,
    ScramSha256,
    ScramSha512,
    OAuthBearer,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Transport {
    SaslPlaintext,
    SaslSsl,
}

/// An unsigned JWT (`alg: none`) for `sub`, expiring `ttl_secs` from now
/// (negative: already expired).
fn unsecured_jwt(sub: &str, ttl_secs: i64) -> String {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64;
    let header = URL_SAFE_NO_PAD.encode(br#"{"alg":"none"}"#);
    let claims = URL_SAFE_NO_PAD.encode(
        format!(
            r#"{{"sub":"{sub}","iat":{iat},"exp":{exp}}}"#,
            iat = now.min(now + ttl_secs) - 60,
            exp = now + ttl_secs
        )
        .as_bytes(),
    );
    format!("{header}.{claims}.")
}

fn tls(broker: &Broker) -> TlsConfig {
    TlsConfig::new().with_ca_cert(broker.ca_pem.to_string_lossy())
}

/// The auth configuration a user writes for `mechanism` over `transport`:
/// credentials, plus the CA as trust anchor over `SASL_SSL`.
fn auth(broker: &Broker, mechanism: Mechanism, transport: Transport) -> AuthConfig {
    let config = match mechanism {
        Mechanism::Plain => AuthConfig::sasl_plain(PLAIN_USER.0, PLAIN_USER.1),
        Mechanism::ScramSha256 => AuthConfig::sasl_scram_sha256(SCRAM_USER.0, SCRAM_USER.1),
        Mechanism::ScramSha512 => AuthConfig::sasl_scram_sha512(SCRAM_USER.0, SCRAM_USER.1),
        Mechanism::OAuthBearer => AuthConfig::sasl_oauthbearer(unsecured_jwt("dave", 3600)),
    };
    match transport {
        Transport::SaslPlaintext => config,
        Transport::SaslSsl => config.with_tls(tls(broker)),
    }
}

fn address(broker: &Broker, transport: Transport) -> &str {
    match transport {
        Transport::SaslPlaintext => &broker.sasl_plaintext,
        Transport::SaslSsl => &broker.sasl_ssl,
    }
}

async fn admin(bootstrap: &str, auth: AuthConfig) -> Result<AdminClient, KrafkaError> {
    Ok(krafka::Kafka::builder(bootstrap)
        .security(auth)
        .connect()
        .await?
        .admin())
}

/// Admin create + list, five records produced, the same five consumed in
/// order — every client authenticated with `mechanism` over `transport`.
async fn round_trip(mechanism: Mechanism, transport: Transport) {
    let broker = broker();
    let bootstrap = address(&broker, transport);
    let topic = format!("sasl-{mechanism:?}-{transport:?}").to_lowercase();

    let admin = admin(bootstrap, auth(&broker, mechanism, transport))
        .await
        .unwrap_or_else(|e| panic!("{mechanism:?}/{transport:?} admin: {e}"));
    admin
        .create_topics(
            vec![NewTopic::new(&topic, 1, 1).unwrap()],
            Default::default(),
        )
        .await
        .unwrap_or_else(|e| panic!("{mechanism:?}/{transport:?} create_topics: {e}"));
    let topics = admin
        .list_topics(Default::default())
        .await
        .expect("list_topics");
    assert!(topics.iter().any(|t| t == &topic), "{topic} not listed");
    admin.close().await.unwrap();

    let producer = async {
        krafka::Kafka::builder(bootstrap)
            .security(auth(&broker, mechanism, transport))
            .connect()
            .await?
            .producer()
            .build()
            .await
    }
    .await
    .unwrap_or_else(|e| panic!("{mechanism:?}/{transport:?} producer: {e}"));
    for i in 0..5 {
        let _ = producer
            .send(
                krafka::Record::new(
                    &topic,
                    bytes::Bytes::copy_from_slice(format!("v{i}").as_bytes()),
                )
                .key(bytes::Bytes::copy_from_slice(format!("k{i}").as_bytes())),
            )
            .await
            .unwrap_or_else(|e| panic!("{mechanism:?}/{transport:?} send {i}: {e}"));
    }
    producer.close().await.unwrap();

    let consumer = async {
        krafka::Kafka::builder(bootstrap)
            .security(auth(&broker, mechanism, transport))
            .connect()
            .await?
            .consumer_without_group()
            .auto_offset_reset(AutoOffsetReset::Earliest)
            .build()
            .await
    }
    .await
    .unwrap_or_else(|e| panic!("{mechanism:?}/{transport:?} consumer: {e}"));
    consumer.assign(&topic, vec![0]).await.expect("assign");
    let mut values = Vec::new();
    let deadline = Instant::now() + Duration::from_secs(30);
    while values.len() < 5 && Instant::now() < deadline {
        for record in consumer.poll(Duration::from_secs(2)).await.expect("poll") {
            values.push(record.value_str().unwrap_or_default().to_string());
        }
    }
    assert_eq!(
        values,
        ["v0", "v1", "v2", "v3", "v4"],
        "{mechanism:?}/{transport:?}: records not consumed in order"
    );
    consumer.close().await.expect("consumer close");
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn plain_over_sasl_plaintext() {
    round_trip(Mechanism::Plain, Transport::SaslPlaintext).await;
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn plain_over_sasl_ssl() {
    round_trip(Mechanism::Plain, Transport::SaslSsl).await;
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn scram_sha256_over_sasl_plaintext() {
    round_trip(Mechanism::ScramSha256, Transport::SaslPlaintext).await;
}

/// SCRAM over TLS with the defaults: the client sends the GS2 header `n,,`,
/// which is all Kafka's SCRAM server accepts.
#[tokio::test]
#[ignore = "requires Docker"]
async fn scram_sha256_over_sasl_ssl() {
    round_trip(Mechanism::ScramSha256, Transport::SaslSsl).await;
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn scram_sha512_over_sasl_plaintext() {
    round_trip(Mechanism::ScramSha512, Transport::SaslPlaintext).await;
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn scram_sha512_over_sasl_ssl() {
    round_trip(Mechanism::ScramSha512, Transport::SaslSsl).await;
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn oauthbearer_over_sasl_plaintext() {
    round_trip(Mechanism::OAuthBearer, Transport::SaslPlaintext).await;
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn oauthbearer_over_sasl_ssl() {
    round_trip(Mechanism::OAuthBearer, Transport::SaslSsl).await;
}

// ---------------------------------------------------------------------------
// Rejections. Each first connects with the matching good configuration on the
// same broker, so a rejection never passes because the broker is down.
// ---------------------------------------------------------------------------

async fn assert_accepted(bootstrap: &str, auth: AuthConfig) {
    let admin = admin(bootstrap, auth)
        .await
        .unwrap_or_else(|e| panic!("the paired good configuration was rejected: {e}"));
    admin.close().await.unwrap();
}

fn assert_auth_error(result: Result<AdminClient, KrafkaError>, what: &str) -> String {
    match result {
        Err(e @ KrafkaError::Auth { .. }) => format!("{e}"),
        Err(other) => panic!("{what}: expected KrafkaError::Auth, got {other:?}"),
        Ok(_) => panic!("{what}: the broker accepted it"),
    }
}

/// The error chain as one string, for asserting on the cause an error carries.
fn chain(e: &dyn std::error::Error) -> String {
    let mut text = e.to_string();
    let mut source = e.source();
    while let Some(s) = source {
        text.push_str(" <- ");
        text.push_str(&s.to_string());
        source = s.source();
    }
    text
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn scram_wrong_password_is_an_auth_error() {
    let broker = broker();
    for transport in [Transport::SaslPlaintext, Transport::SaslSsl] {
        let bootstrap = address(&broker, transport);
        assert_accepted(bootstrap, auth(&broker, Mechanism::ScramSha256, transport)).await;
        let mut bad = AuthConfig::sasl_scram_sha256(SCRAM_USER.0, "wrong-password");
        if transport == Transport::SaslSsl {
            bad = bad.with_tls(tls(&broker));
        }
        assert_auth_error(admin(bootstrap, bad).await, "SCRAM wrong password");
    }
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn plain_wrong_password_is_an_auth_error() {
    let broker = broker();
    let bootstrap = address(&broker, Transport::SaslPlaintext);
    assert_accepted(
        bootstrap,
        auth(&broker, Mechanism::Plain, Transport::SaslPlaintext),
    )
    .await;
    let bad = AuthConfig::sasl_plain(PLAIN_USER.0, "wrong-password");
    assert_auth_error(admin(bootstrap, bad).await, "PLAIN wrong password");
}

/// A user provisioned for SCRAM-SHA-256 only cannot authenticate with
/// SCRAM-SHA-512.
#[tokio::test]
#[ignore = "requires Docker"]
async fn scram_variant_without_a_credential_is_an_auth_error() {
    let broker = broker();
    let bootstrap = address(&broker, Transport::SaslPlaintext);
    assert_accepted(
        bootstrap,
        AuthConfig::sasl_scram_sha256(SCRAM_256_ONLY.0, SCRAM_256_ONLY.1),
    )
    .await;
    let other_variant = AuthConfig::sasl_scram_sha512(SCRAM_256_ONLY.0, SCRAM_256_ONLY.1);
    assert_auth_error(
        admin(bootstrap, other_variant).await,
        "SCRAM-SHA-512 for a SHA-256-only user",
    );
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn expired_oauthbearer_token_is_an_auth_error() {
    let broker = broker();
    let bootstrap = address(&broker, Transport::SaslPlaintext);
    assert_accepted(
        bootstrap,
        auth(&broker, Mechanism::OAuthBearer, Transport::SaslPlaintext),
    )
    .await;
    let expired = AuthConfig::sasl_oauthbearer(unsecured_jwt("dave", -600));
    assert_auth_error(admin(bootstrap, expired).await, "expired OAUTHBEARER token");
}

/// A mechanism the listener does not enable is refused with Kafka's
/// `UNSUPPORTED_SASL_MECHANISM` (33), carried by the `Auth` error.
#[tokio::test]
#[ignore = "requires Docker"]
async fn mechanism_the_listener_does_not_enable_is_refused() {
    let broker = broker();
    assert_accepted(
        &broker.plain_only,
        AuthConfig::sasl_plain(PLAIN_USER.0, PLAIN_USER.1),
    )
    .await;
    let scram = AuthConfig::sasl_scram_sha256(SCRAM_USER.0, SCRAM_USER.1);
    let message = assert_auth_error(
        admin(&broker.plain_only, scram).await,
        "SCRAM on a PLAIN-only listener",
    );
    assert!(
        message.contains("SCRAM-SHA-256") && message.to_lowercase().contains("not enabled")
            || message.contains("UnsupportedSaslMechanism")
            || message.contains("33"),
        "the error must name the unsupported mechanism: {message}"
    );
}

/// No SASL at all against a SASL listener: the broker closes the connection
/// on the first request after `ApiVersions`.
#[tokio::test]
#[ignore = "requires Docker"]
async fn unauthenticated_client_is_refused() {
    let broker = broker();
    let bootstrap = address(&broker, Transport::SaslPlaintext);
    assert_accepted(
        bootstrap,
        auth(&broker, Mechanism::Plain, Transport::SaslPlaintext),
    )
    .await;
    let result = krafka::Kafka::builder(bootstrap)
        .request_timeout(Duration::from_secs(10))
        .connect()
        .await;
    match result {
        Err(KrafkaError::Network(_) | KrafkaError::Timeout { .. }) => {}
        Err(other) => {
            panic!("expected the broker to drop an unauthenticated client, got {other:?}")
        }
        Ok(_) => panic!("an unauthenticated client was accepted on a SASL listener"),
    }
}

/// A client that does not trust the broker's CA fails in the TLS handshake:
/// an `Auth` error whose cause is the certificate, never a SASL rejection and
/// never a success.
#[tokio::test]
#[ignore = "requires Docker"]
async fn untrusted_ca_fails_the_tls_handshake() {
    let broker = broker();
    let bootstrap = address(&broker, Transport::SaslSsl);
    assert_accepted(
        bootstrap,
        auth(&broker, Mechanism::ScramSha256, Transport::SaslSsl),
    )
    .await;
    let untrusting =
        AuthConfig::sasl_scram_sha256(SCRAM_USER.0, SCRAM_USER.1).with_tls(TlsConfig::new());
    match admin(bootstrap, untrusting).await {
        Err(e @ KrafkaError::Auth { .. }) => {
            let text = chain(&e).to_lowercase();
            assert!(
                text.contains("certificate") || text.contains("unknownissuer"),
                "the Auth error must carry the certificate failure: {text}"
            );
        }
        Err(other) => panic!("expected a TLS failure (Auth), got {other:?}"),
        Ok(_) => panic!("a client without the test CA completed the TLS handshake"),
    }
}

/// A `SASL_PLAINTEXT` client on the `SASL_SSL` port fails within its request
/// timeout instead of hanging.
#[tokio::test]
#[ignore = "requires Docker"]
async fn plaintext_client_on_the_tls_port_fails_within_its_timeout() {
    let broker = broker();
    let bootstrap = address(&broker, Transport::SaslSsl);
    assert_accepted(
        bootstrap,
        auth(&broker, Mechanism::Plain, Transport::SaslSsl),
    )
    .await;
    let timeout = Duration::from_secs(10);
    let started = Instant::now();
    let result = krafka::Kafka::builder(bootstrap)
        .request_timeout(timeout)
        .security(AuthConfig::sasl_plain(PLAIN_USER.0, PLAIN_USER.1))
        .connect()
        .await;
    let took = started.elapsed();
    assert!(
        result.is_err(),
        "a plaintext client was accepted on the TLS port"
    );
    assert!(
        took < timeout + Duration::from_secs(10),
        "connect() took {took:?}, beyond its {timeout:?} request timeout"
    );
}
