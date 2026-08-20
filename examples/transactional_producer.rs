//! Transactional producer example.
//!
//! Demonstrates exactly-once semantics with transactional messaging.
//!
//! Run with:
//! ```
//! cargo run --example transactional_producer
//! ```

use krafka::producer::TransactionalProducer;
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize tracing
    tracing_subscriber::fmt()
        .with_env_filter("krafka=debug")
        .init();

    // Validate the configuration before touching the network.
    //
    // `build_config()` runs exactly the checks `build()` runs — same validator,
    // no broker — which is what makes a `validate-config` subcommand or a unit
    // test possible for an exactly-once deployment.
    let config = TransactionalProducer::builder()
        .bootstrap_servers("localhost:9092")
        .transactional_id("krafka-example-txn-1")
        .build_config()?;
    println!(
        "Configuration valid: transactional.id={}, acks={:?}, delivery_timeout={:?}",
        config.transactional_id(),
        config.acks(),
        config.delivery_timeout()
    );

    // Create transactional producer with unique ID
    // The transactional.id must be unique per producer instance
    let producer = TransactionalProducer::builder()
        .bootstrap_servers("localhost:9092")
        .transactional_id("krafka-example-txn-1")
        .client_id("krafka-transactional-producer")
        .transaction_timeout(Duration::from_secs(60))
        // Bound how long one batch may sit in flight. A stuck batch holds the
        // transaction open, and an open transaction blocks every
        // read_committed consumer at its first offset — so keep this at or
        // below transaction_timeout.
        .delivery_timeout(Duration::from_secs(45))
        .build()
        .await?;

    println!("Transactional producer created!");

    // Initialize transactions - must be called once before any transactions
    // This obtains a producer ID and epoch from the transaction coordinator
    producer.init_transactions().await?;
    println!("Transactions initialized");

    // Example 1: Successful transaction
    println!("\n=== Transaction 1: Multi-topic atomic write ===");
    producer.begin_transaction()?;

    // All these writes are atomic - either all succeed or none
    let _ = producer
        .send("orders", Some(b"order-123"), Some(b"Order created"))
        .await?;
    let _ = producer
        .send("inventory", Some(b"sku-456"), Some(b"Stock decreased"))
        .await?;
    let _ = producer
        .send(
            "notifications",
            Some(b"user-789"),
            Some(b"Order confirmation"),
        )
        .await?;

    // Optional: force the buffered batches onto the wire now, so a send
    // failure surfaces here rather than inside commit_transaction(). Not
    // required — commit_transaction() flushes first, and must, or a commit
    // marker could be written while records were still buffered.
    producer.flush().await?;

    // Commit makes all messages visible to consumers
    producer.commit_transaction().await?;
    println!("Transaction 1 committed successfully!");

    // Example 2: Aborted transaction
    println!("\n=== Transaction 2: Aborted transaction ===");
    producer.begin_transaction()?;

    let _ = producer
        .send("orders", Some(b"order-124"), Some(b"Order created"))
        .await?;

    // Simulate a business logic failure
    let validation_failed = true;
    if validation_failed {
        // Abort rolls back all messages in the transaction
        producer.abort_transaction().await?;
        println!("Transaction 2 aborted - messages discarded");
    }

    // Example 3: Error handling pattern
    println!("\n=== Transaction 3: Error handling pattern ===");
    producer.begin_transaction()?;

    match process_order(&producer).await {
        Ok(()) => {
            producer.commit_transaction().await?;
            println!("Transaction 3 committed successfully!");
        }
        Err(e) => {
            producer.abort_transaction().await?;
            println!("Transaction 3 aborted due to error: {}", e);
        }
    }

    println!("\nTransactional producer example complete!");
    Ok(())
}

async fn process_order(producer: &TransactionalProducer) -> Result<(), Box<dyn std::error::Error>> {
    // Simulate order processing
    let _ = producer
        .send("orders", Some(b"order-125"), Some(b"Order processing"))
        .await?;
    let _ = producer
        .send("payments", Some(b"payment-125"), Some(b"Payment initiated"))
        .await?;
    let _ = producer
        .send(
            "shipping",
            Some(b"shipment-125"),
            Some(b"Shipment scheduled"),
        )
        .await?;

    Ok(())
}
