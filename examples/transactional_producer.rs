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

    // Create transactional producer with unique ID
    // The transactional.id must be unique per producer instance
    let producer = TransactionalProducer::builder()
        .bootstrap_servers("localhost:9092")
        .transactional_id("krafka-example-txn-1")
        .client_id("krafka-transactional-producer")
        .transaction_timeout(Duration::from_secs(60))
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
        .send("orders", Some(b"order-123"), b"Order created")
        .await?;
    let _ = producer
        .send("inventory", Some(b"sku-456"), b"Stock decreased")
        .await?;
    let _ = producer
        .send("notifications", Some(b"user-789"), b"Order confirmation")
        .await?;

    // Commit makes all messages visible to consumers
    producer.commit_transaction().await?;
    println!("Transaction 1 committed successfully!");

    // Example 2: Aborted transaction
    println!("\n=== Transaction 2: Aborted transaction ===");
    producer.begin_transaction()?;

    let _ = producer
        .send("orders", Some(b"order-124"), b"Order created")
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
        .send("orders", Some(b"order-125"), b"Order processing")
        .await?;
    let _ = producer
        .send("payments", Some(b"payment-125"), b"Payment initiated")
        .await?;
    let _ = producer
        .send("shipping", Some(b"shipment-125"), b"Shipment scheduled")
        .await?;

    Ok(())
}
