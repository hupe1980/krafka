//! Cluster administration: describe the cluster, then create, describe and
//! delete a topic.
//!
//! Needs a broker at `KAFKA_BOOTSTRAP_SERVERS` (default `localhost:9092`) on
//! which the client may create and delete topics.
//!
//! Run with:
//! ```sh
//! cargo run --example admin
//! ```

use krafka::Kafka;
use krafka::admin::NewTopic;

const TOPIC: &str = "krafka-admin-example";

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let bootstrap =
        std::env::var("KAFKA_BOOTSTRAP_SERVERS").unwrap_or_else(|_| "localhost:9092".into());
    let kafka = Kafka::builder(bootstrap)
        .client_id("krafka-admin-example")
        .connect()
        .await?;
    let admin = kafka.admin();

    let cluster = admin.describe_cluster(Default::default()).await?;
    println!(
        "cluster {} (controller {})",
        cluster.cluster_id, cluster.controller_id
    );
    for broker in &cluster.brokers {
        println!("  broker {} at {}:{}", broker.id, broker.host, broker.port);
    }

    // Results are per topic: one topic failing does not fail the call.
    let created = admin
        .create_topics([NewTopic::new(TOPIC, 3, 1)?], Default::default())
        .await?;
    for (name, result) in created {
        match result {
            Ok(()) => println!("created {name}"),
            Err(error) => println!("create {name}: {error}"),
        }
    }

    let topics = admin.list_topics(Default::default()).await?;
    println!("{} topics", topics.len());

    for (name, description) in admin.describe_topics([TOPIC], Default::default()).await? {
        let description = description?;
        println!("{name}:");
        for partition in &description.partitions {
            println!(
                "  partition {} leader={:?} replicas={:?} isr={:?}",
                partition.partition, partition.leader, partition.replicas, partition.isr
            );
        }
    }

    for (name, result) in admin.delete_topics([TOPIC], Default::default()).await? {
        result?;
        println!("deleted {name}");
    }

    admin.close().await?;
    Ok(())
}
