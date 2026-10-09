//! TLS and SASL (PLAIN, SCRAM, static OAUTHBEARER) configured from the
//! environment.
//!
//! `AuthConfig::from_env` reads the security settings:
//!
//! | Variable | Values |
//! |---|---|
//! | `KAFKA_SECURITY_PROTOCOL` | `PLAINTEXT` (default), `SSL`, `SASL_PLAINTEXT`, `SASL_SSL` |
//! | `KAFKA_SASL_MECHANISM` | `PLAIN`, `SCRAM-SHA-256`, `SCRAM-SHA-512`, `OAUTHBEARER`, `AWS_MSK_IAM` |
//! | `KAFKA_SASL_USERNAME`, `KAFKA_SASL_PASSWORD` | for `PLAIN` and SCRAM |
//! | `KAFKA_SSL_CA_LOCATION` | a CA bundle to trust instead of the WebPKI roots |
//! | `KAFKA_SSL_CERTIFICATE_LOCATION`, `KAFKA_SSL_KEY_LOCATION` | a client certificate (mTLS) |
//!
//! The same settings in code, for SCRAM-SHA-512 over TLS with a private CA:
//!
//! ```rust,ignore
//! AuthConfig::sasl_scram_sha512(username, password)
//!     .with_tls(TlsConfig::new().with_ca_cert("/etc/kafka/ca.pem"))
//! ```
//!
//! The example connects, prints what it authenticated with, and describes
//! the cluster. The `oauth_oidc` and `msk_iam` examples cover token and IAM
//! authentication.
//!
//! Run with:
//! ```sh
//! KAFKA_BOOTSTRAP_SERVERS=broker:9093 \
//! KAFKA_SECURITY_PROTOCOL=SASL_SSL KAFKA_SASL_MECHANISM=SCRAM-SHA-512 \
//! KAFKA_SASL_USERNAME=alice KAFKA_SASL_PASSWORD=secret \
//! cargo run --example authentication
//! ```

use krafka::Kafka;
use krafka::auth::AuthConfig;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let bootstrap =
        std::env::var("KAFKA_BOOTSTRAP_SERVERS").unwrap_or_else(|_| "localhost:9092".into());
    let security = AuthConfig::from_env()?;
    println!(
        "security protocol {}, mechanism {:?}",
        security.security_protocol(),
        security.sasl_mechanism()
    );

    // Every client built from this handle uses its security settings.
    let kafka = Kafka::builder(bootstrap)
        .client_id("krafka-authentication-example")
        .security(security)
        .connect()
        .await?;

    let cluster = kafka.admin().describe_cluster(Default::default()).await?;
    println!(
        "connected to cluster {} with {} brokers",
        cluster.cluster_id,
        cluster.brokers.len()
    );
    Ok(())
}
