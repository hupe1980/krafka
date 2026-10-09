//! AWS MSK IAM authentication with the AWS SDK credential chain.
//!
//! `AuthConfig::aws_msk_iam_provider` calls the provider for every new broker
//! connection, so temporary credentials (an EC2 instance profile, an ECS task
//! role, EKS IRSA or pod identity, SSO) are picked up as they rotate.
//! `AwsMskIamCredentials::from_default_chain` resolves them the way the AWS
//! SDK does; it needs the `aws-msk` feature. MSK IAM always uses TLS.
//!
//! | Variable | Meaning |
//! |---|---|
//! | `KAFKA_BOOTSTRAP_SERVERS` | the cluster's IAM bootstrap brokers (port 9098) |
//! | `AWS_REGION` | the cluster's region |
//!
//! Run with:
//! ```sh
//! KAFKA_BOOTSTRAP_SERVERS=b-1.mycluster.abc123.c2.kafka.eu-central-1.amazonaws.com:9098 \
//! AWS_REGION=eu-central-1 \
//! cargo run --example msk_iam --features aws-msk
//! ```

use krafka::Kafka;
use krafka::auth::{AuthConfig, AwsMskIamCredentials};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let bootstrap = std::env::var("KAFKA_BOOTSTRAP_SERVERS")?;
    let region = std::env::var("AWS_REGION")?;

    let security = AuthConfig::aws_msk_iam_provider(move || {
        let region = region.clone();
        async move { AwsMskIamCredentials::from_default_chain(region).await }
    });
    let kafka = Kafka::builder(bootstrap)
        .client_id("krafka-msk-iam-example")
        .security(security)
        .connect()
        .await?;

    let topics = kafka.admin().list_topics(Default::default()).await?;
    println!("authenticated; {} topics", topics.len());
    Ok(())
}
