//! The subscriber assertions that read what a handler received: the latest payload decoded or as
//! bytes, every payload as a list, and the batches a batch handler was called with. Every scenario
//! is a test of its own, the way a service writes it.
//!
//! ```text
//! cargo test --example testing_subscriber_payloads --features testing,macros,memory,json
//! ```

// A plain build compiles the services; the tests that use them exist only in the test build.
#![cfg_attr(not(test), allow(dead_code, unused_imports))]

use ruststream::memory::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
struct Order {
    id: u64,
}

/// Bytes published as themselves: the input of a handler that reads the bytes itself.
#[derive(Outgoing, Serialized)]
struct Wire(Vec<u8>);

/// The payload view a raw handler takes: the delivery's bytes, with no codec in between.
#[derive(Deserialized)]
struct Frame<'a>(&'a [u8]);

mod with {
    use std::error::Error;

    use ruststream::memory::prelude::*;
    use ruststream::testing::TestApp;

    use super::Order;

    #[subscriber("orders")]
    async fn accept(order: &Order) -> HandlerOutcome {
        tracing::info!(order.id, "accepted");
        HandlerOutcome::ack()
    }

    fn app() -> impl App {
        RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
            b.include(accept);
        })
    }

    // --8<-- [start:with]
    #[tokio::test]
    async fn the_latest_call_got_the_second_order() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        let broker = tb.broker::<MemoryBroker>();
        for id in [7, 8] {
            broker.message(&Order { id }).to("orders").publish().await?;
        }

        broker
            .subscriber("orders")
            .assert_called(2)
            .with(&Order { id: 8 });
        Ok(())
    }
    // --8<-- [end:with]
}

mod with_raw {
    use std::error::Error;

    use ruststream::memory::prelude::*;
    use ruststream::testing::TestApp;

    use super::Order;

    #[subscriber("orders")]
    async fn accept(order: &Order) -> HandlerOutcome {
        tracing::info!(order.id, "accepted");
        HandlerOutcome::ack()
    }

    fn app() -> impl App {
        RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
            b.include(accept);
        })
    }

    // --8<-- [start:with_raw]
    #[tokio::test]
    async fn the_order_arrived_as_json() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        let broker = tb.broker::<MemoryBroker>();
        broker
            .message(&Order { id: 7 })
            .to("orders")
            .publish()
            .await?;

        broker
            .subscriber("orders")
            .assert_called_once()
            .with_raw(br#"{"id":7}"#);
        Ok(())
    }
    // --8<-- [end:with_raw]
}

mod received {
    use std::error::Error;

    use ruststream::memory::prelude::*;
    use ruststream::testing::TestApp;

    use super::Order;

    #[subscriber("orders")]
    async fn accept(order: &Order) -> HandlerOutcome {
        tracing::info!(order.id, "accepted");
        HandlerOutcome::ack()
    }

    fn app() -> impl App {
        RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
            b.include(accept);
        })
    }

    // --8<-- [start:received]
    #[tokio::test]
    async fn the_orders_arrived_in_order() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        let broker = tb.broker::<MemoryBroker>();
        for id in [3, 1, 2] {
            broker.message(&Order { id }).to("orders").publish().await?;
        }

        let ids: Vec<u64> = broker
            .subscriber("orders")
            .received::<Order>()
            .iter()
            .map(|order| order.id)
            .collect();
        assert_eq!(ids, [3, 1, 2]);
        Ok(())
    }
    // --8<-- [end:received]
}

mod received_raw {
    use std::error::Error;

    use ruststream::memory::prelude::*;
    use ruststream::testing::TestApp;

    use super::{Frame, Wire};

    #[subscriber("frames")]
    async fn ingest(frame: &Frame<'_>) -> HandlerOutcome {
        tracing::info!(bytes = frame.0.len(), "ingested");
        HandlerOutcome::ack()
    }

    fn app() -> impl App {
        RustStream::new(AppInfo::new("frames", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
            b.include(ingest);
        })
    }

    // --8<-- [start:received_raw]
    #[tokio::test]
    async fn the_frames_arrived_untouched() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        let broker = tb.broker::<MemoryBroker>();
        for frame in [b"\x00\x01", b"\xff\xfe"] {
            broker
                .message(&Wire(frame.to_vec()))
                .to("frames")
                .publish()
                .await?;
        }

        assert_eq!(
            broker.subscriber("frames").received_raw(),
            [&b"\x00\x01"[..], &b"\xff\xfe"[..]],
        );
        Ok(())
    }
    // --8<-- [end:received_raw]
}

mod batches {
    use std::error::Error;

    use ruststream::memory::prelude::*;
    use ruststream::testing::TestApp;

    use super::Order;

    #[subscriber("orders")]
    async fn bill(orders: &[Order]) -> HandlerOutcome {
        tracing::info!(count = orders.len(), "billed");
        HandlerOutcome::ack()
    }

    fn app(broker: MemoryBroker<Retaining>) -> impl App {
        RustStream::new(AppInfo::new("billing", "0.1.0")).with_broker(broker, |b| {
            b.include(bill.batch(nonzero!(2)).start_at(MemoryPosition::start()));
        })
    }

    // --8<-- [start:batches]
    #[tokio::test]
    async fn the_orders_were_billed_two_at_a_time() -> Result<(), Box<dyn Error>> {
        // The log already holds three orders, so the subscription reads them as full batches.
        let broker = MemoryBroker::retaining(Retention::Messages(nonzero!(16)));
        let publisher = broker.publisher();
        for id in 1..=3 {
            publisher
                .message(&Order { id })
                .to("orders")
                .publish()
                .await?;
        }
        let tb = TestApp::start(app(broker)).await?;
        tb.settle().await?;

        assert_eq!(
            tb.broker::<MemoryBroker<Retaining>>()
                .subscriber("orders")
                .batches::<Order>(),
            [
                vec![Order { id: 1 }, Order { id: 2 }],
                vec![Order { id: 3 }]
            ],
        );
        Ok(())
    }
    // --8<-- [end:batches]
}

mod batches_raw {
    use std::error::Error;

    use ruststream::memory::prelude::*;
    use ruststream::testing::TestApp;

    use super::{Frame, Wire};

    #[subscriber("frames")]
    async fn ingest(frames: &[Frame<'_>]) -> HandlerOutcome {
        tracing::info!(count = frames.len(), "ingested");
        HandlerOutcome::ack()
    }

    fn app(broker: MemoryBroker<Retaining>) -> impl App {
        RustStream::new(AppInfo::new("frames", "0.1.0")).with_broker(broker, |b| {
            b.include(ingest.batch(nonzero!(2)).start_at(MemoryPosition::start()));
        })
    }

    // --8<-- [start:batches_raw]
    #[tokio::test]
    async fn the_frames_were_cut_in_pairs() -> Result<(), Box<dyn Error>> {
        // The log already holds three frames, so the subscription reads them as full batches.
        let broker = MemoryBroker::retaining(Retention::Messages(nonzero!(16)));
        let publisher = broker.publisher();
        for frame in [b"\x01", b"\x02", b"\x03"] {
            publisher
                .message(&Wire(frame.to_vec()))
                .to("frames")
                .publish()
                .await?;
        }
        let tb = TestApp::start(app(broker)).await?;
        tb.settle().await?;

        assert_eq!(
            tb.broker::<MemoryBroker<Retaining>>()
                .subscriber("frames")
                .batches_raw(),
            [vec![&b"\x01"[..], &b"\x02"[..]], vec![&b"\x03"[..]]],
        );
        Ok(())
    }
    // --8<-- [end:batches_raw]
}

mod assert_batch_sizes {
    use std::error::Error;

    use ruststream::memory::prelude::*;
    use ruststream::testing::TestApp;

    use super::Order;

    #[subscriber("orders")]
    async fn bill(orders: &[Order]) -> HandlerOutcome {
        tracing::info!(count = orders.len(), "billed");
        HandlerOutcome::ack()
    }

    fn app(broker: MemoryBroker<Retaining>) -> impl App {
        RustStream::new(AppInfo::new("billing", "0.1.0")).with_broker(broker, |b| {
            b.include(bill.batch(nonzero!(2)).start_at(MemoryPosition::start()));
        })
    }

    // --8<-- [start:assert_batch_sizes]
    #[tokio::test]
    async fn five_orders_make_three_calls() -> Result<(), Box<dyn Error>> {
        // The log already holds five orders, so the subscription reads them as full batches.
        let broker = MemoryBroker::retaining(Retention::Messages(nonzero!(16)));
        let publisher = broker.publisher();
        for id in 1..=5 {
            publisher
                .message(&Order { id })
                .to("orders")
                .publish()
                .await?;
        }
        let tb = TestApp::start(app(broker)).await?;
        tb.settle().await?;

        tb.broker::<MemoryBroker<Retaining>>()
            .subscriber("orders")
            .assert_called(3)
            .assert_batch_sizes(&[2, 2, 1])
            .settled(HandlerOutcome::ack());
        Ok(())
    }
    // --8<-- [end:assert_batch_sizes]
}

// The scenarios are the tests above; the command in the header runs them.
fn main() {}
