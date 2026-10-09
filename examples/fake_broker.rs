//! Testing application code against the in-process fake broker.
//!
//! `publish_order` stands in for application code that takes a `&Producer`.
//! The fake broker runs it under two injected faults and checks what reached
//! the log:
//!
//! 1. a `Produce` answered with `NOT_LEADER_OR_FOLLOWER`, which the producer
//!    retries;
//! 2. a `Produce` that is written and whose answer is then lost
//!    (`Control::ApplyThen(Disconnect)`), which the idempotent producer
//!    retries and the broker de-duplicates: the order is in the log once.
//!
//! No external broker is needed; the program runs to completion.
//!
//! Run with:
//! ```sh
//! cargo run --example fake_broker --features test-broker
//! ```
//!
//! `krafka::testing` is unstable and outside semver.

use krafka::error::ErrorCode;
use krafka::producer::Producer;
use krafka::testing::{ApiKey, Control, FakeBroker};
use krafka::{Kafka, Record};

/// Application code under test: publish one order, keyed by its id, and
/// return the offset it was written at.
async fn publish_order(producer: &Producer, order_id: &str) -> krafka::Result<i64> {
    let metadata = producer
        .send(Record::new("orders", format!("order {order_id} created")).key(order_id.to_owned()))
        .await?;
    Ok(metadata.offset)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let broker = FakeBroker::start().await?;
    broker.create_topic("orders", 1);

    let kafka = Kafka::builder(broker.bootstrap_servers()).connect().await?;
    let producer = kafka.producer().build().await?;

    // Fault 1: the leader refuses the first Produce. The producer refreshes
    // metadata and retries.
    broker.on_once(ApiKey::Produce, |_| {
        Control::Error(ErrorCode::NotLeaderForPartition)
    });
    let offset = publish_order(&producer, "1001").await?;
    assert_eq!(offset, 0);
    assert_eq!(broker.request_count(ApiKey::Produce), 2);
    println!("NOT_LEADER_OR_FOLLOWER: retried, order 1001 at offset {offset}");

    // Fault 2: the broker appends the batch, then drops the connection before
    // answering. The retry carries the same producer id and sequence, so the
    // broker acknowledges it without writing it again.
    broker.clear_requests();
    broker.on_once(ApiKey::Produce, |_| {
        Control::ApplyThen(Box::new(Control::Disconnect))
    });
    let offset = publish_order(&producer, "1002").await?;
    assert_eq!(offset, 1);
    assert_eq!(broker.request_count(ApiKey::Produce), 2);
    assert_eq!(broker.next_offset("orders", 0), Some(2), "no duplicate");
    println!("answer lost after append: retried, order 1002 at offset {offset}, written once");

    producer.close().await?;

    let records = broker.all_records("orders")?;
    println!("log of orders-0:");
    for record in &records {
        println!("  {} {:?}", record.offset, record.value_str());
    }
    Ok(())
}
