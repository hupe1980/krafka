//! Admin client example.
//!
//! Demonstrates how to use the Krafka admin client for cluster management.
//!
//! Run with:
//! ```
//! cargo run --example admin
//! ```

use krafka::admin::AdminClient;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize tracing
    tracing_subscriber::fmt()
        .with_env_filter("krafka=debug")
        .init();

    // Create admin client
    let admin = AdminClient::builder()
        .bootstrap_servers("localhost:9092")
        .client_id("krafka-admin-example")
        .build()
        .await?;

    println!("Admin client connected to Kafka!");

    // List all topics
    println!("\n=== Topics ===");
    let topics = admin.list_topics().await?;
    for topic in &topics {
        println!("  - {}", topic);
    }

    // Describe cluster
    println!("\n=== Cluster ===");
    let cluster = admin.describe_cluster().await?;
    println!("Controller ID: {:?}", cluster.controller_id);
    println!("Brokers:");
    for broker in &cluster.brokers {
        println!(
            "  - ID: {}, Address: {}:{}",
            broker.id, broker.host, broker.port
        );
    }

    // Describe specific topics
    if !topics.is_empty() {
        println!("\n=== Topic Details ===");
        let topic_infos = admin
            .describe_topics(&topics[..1.min(topics.len())])
            .await?;
        for info in topic_infos {
            println!("Topic: {}", info.name);
            println!("  Partitions: {}", info.partition_count());
            for partition in &info.partitions {
                println!(
                    "    Partition {}: leader={}, replicas={:?}, isr={:?}",
                    partition.partition, partition.leader, partition.replicas, partition.isr
                );
            }
        }
    }

    Ok(())
}
