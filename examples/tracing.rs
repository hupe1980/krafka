//! Spans through `tracing`.
//!
//! Installs a `tracing-subscriber` formatter that prints each span when it
//! closes, with its fields and duration, then produces and consumes a few
//! records. The output shows the client's spans — `send` per record, `poll`
//! per poll, `commit` per offset commit, `rebalance` per assignment change —
//! with their OpenTelemetry messaging attributes (`otel.name`, `otel.kind`,
//! `messaging.*`). An OpenTelemetry bridge such as `tracing-opentelemetry`
//! exports the same spans. `RUST_LOG` overrides the filter.
//!
//! Needs a broker at `KAFKA_BOOTSTRAP_SERVERS` (default `localhost:9092`) with
//! `auto.create.topics.enable=true` (the broker default), or an existing
//! `example-tracing` topic.
//!
//! Run with:
//! ```sh
//! cargo run --example tracing
//! ```

use std::time::Duration;

use krafka::consumer::AutoOffsetReset;
use krafka::{Kafka, Record};
use tracing_subscriber::EnvFilter;
use tracing_subscriber::fmt::format::FmtSpan;

const TOPIC: &str = "example-tracing";

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| EnvFilter::new("warn,krafka=info")),
        )
        .with_span_events(FmtSpan::CLOSE)
        .init();

    let bootstrap =
        std::env::var("KAFKA_BOOTSTRAP_SERVERS").unwrap_or_else(|_| "localhost:9092".into());
    let kafka = Kafka::builder(bootstrap)
        .client_id("krafka-tracing-example")
        // Ask the broker to create the topic on first use.
        .allow_auto_create_topics(true)
        .connect()
        .await?;

    let producer = kafka.producer().build().await?;
    for i in 0..3 {
        let _ = producer
            .send(Record::new(TOPIC, format!("event {i}")).key(format!("key-{i}")))
            .await?;
    }
    producer.close().await?;

    let consumer = kafka
        .consumer("krafka-tracing-example")
        .auto_offset_reset(AutoOffsetReset::Earliest)
        .enable_auto_commit(false)
        .build()
        .await?;
    consumer.subscribe([TOPIC]).await?;
    let mut received = 0;
    for _ in 0..10 {
        received += consumer.poll(Duration::from_secs(1)).await?.len();
        if received >= 3 {
            break;
        }
    }
    consumer.commit().await?;
    consumer.close().await?;
    Ok(())
}
