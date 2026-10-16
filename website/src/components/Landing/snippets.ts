// The two programs the landing page compares. Both consume the topic `orders` of a local Kafka
// with the consumer group `orders-svc`, decode a JSON `Order` and print its id.

/**
 * A consumer written against `rdkafka` directly. It builds as an example of a crate depending on
 * `rdkafka` 0.39 with its `tokio` feature, `serde`, `serde_json` and `tokio`.
 */
export const RAW_LOOP = `use rdkafka::Message;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{CommitMode, Consumer, StreamConsumer};
use serde::Deserialize;

#[derive(Debug, Deserialize)]
struct Order {
    id: u64,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", "localhost:9092")
        .set("group.id", "orders-svc")
        .set("enable.auto.commit", "false")
        .create()?;
    consumer.subscribe(&["orders"])?;

    loop {
        let msg = tokio::select! {
            _ = tokio::signal::ctrl_c() => break,
            msg = consumer.recv() => match msg {
                Ok(msg) => msg,
                Err(err) => {
                    eprintln!("kafka error: {err}");
                    continue;
                }
            },
        };
        match msg.payload().map(serde_json::from_slice::<Order>) {
            Some(Ok(order)) => println!("got order {}", order.id),
            Some(Err(err)) => eprintln!("bad payload: {err}"),
            None => eprintln!("empty message"),
        }
        if let Err(err) = consumer.commit_message(&msg, CommitMode::Async) {
            eprintln!("commit failed: {err}");
        }
    }
    Ok(())
}`;

/** The `kafka_quickstart` example of `ruststream-rdkafka`, without its comments. */
export const RUSTSTREAM_SERVICE = `use ruststream_rdkafka::prelude::*;
use serde::Deserialize;

#[derive(Debug, Deserialize)]
struct Order {
    id: u64,
}

#[subscriber("orders")]
async fn handle(order: &Order) -> HandlerOutcome {
    println!("got order {}", order.id);
    HandlerOutcome::ack()
}

#[ruststream::app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(
        KafkaBroker::new(["localhost:9092"]).default_group("orders-svc"),
        |b| {
            b.include(handle);
        },
    )
}`;
