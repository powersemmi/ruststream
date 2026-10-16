//! The subscriber assertions that read what a handler received, against services written without
//! the `macros` feature. The test bodies are the ones of `examples/testing_subscriber_payloads.rs`:
//! the harness reads the same records whichever way the service was declared.
//!
//! ```text
//! cargo test --example manual_testing_subscriber_payloads --no-default-features --features testing,memory,json
//! ```

// A plain build compiles the services; the tests that use them exist only in the test build.
#![cfg_attr(not(test), allow(dead_code, unused_imports))]

use std::convert::Infallible;

use ruststream::memory::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, schemars::JsonSchema)]
struct Order {
    id: u64,
}

// `#[derive(Outgoing)]` with no `name`, by hand: the call site names the destination.
impl OutgoingDestination for Order {
    type Form = CallerName;
}

impl MessageHeaders for Order {
    type Contract = NoHeaders;
}

/// Bytes published as themselves: the input of a handler that reads the bytes itself. What
/// `#[derive(Outgoing, Serialized)]` writes, by hand.
struct Wire(Vec<u8>);

impl Serialized for Wire {
    type Error = Infallible;

    fn wire_bytes(&self, _buf: &mut BytesMut) -> Result<WireBytes<'_>, Infallible> {
        Ok(WireBytes::Own(&self.0))
    }
}

impl MessageWire for Wire {
    type Wire = SerializedWire;
}

impl OutgoingDestination for Wire {
    type Form = CallerName;
}

impl MessageHeaders for Wire {
    type Contract = NoHeaders;
}

/// The payload view a raw handler takes: the delivery's bytes, with no codec in between. What
/// `#[derive(Deserialized)]` writes, by hand.
struct Frame<'a>(&'a [u8]);

impl Deserialized for Frame<'_> {
    type Output<'a> = Frame<'a>;
    type Error = Infallible;

    fn from_payload(payload: &[u8]) -> Result<Frame<'_>, Self::Error> {
        Ok(Frame(payload))
    }
}

impl Input for Frame<'_> {
    type Axis = SoloDeserialized<Frame<'static>>;
}

mod with {
    use std::error::Error;
    use std::future::{Future, ready};

    use ruststream::memory::prelude::*;
    use ruststream::testing::TestApp;

    use super::Order;

    // --8<-- [start:with]
    struct Accept;

    impl Handle<Order> for Accept {
        fn handle(
            &self,
            order: &Order,
            _outs: &(),
            _ctx: &mut Context<'_>,
        ) -> impl Future<Output = Result<(), HandlerOutcome>> {
            tracing::info!(order.id, "accepted");
            ready(Ok(()))
        }
    }

    fn app() -> impl App {
        RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
            b.include(subscriber("orders", Accept).build());
        })
    }

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
    use std::future::{Future, ready};

    use ruststream::memory::prelude::*;
    use ruststream::testing::TestApp;

    use super::Order;

    // --8<-- [start:with_raw]
    struct Accept;

    impl Handle<Order> for Accept {
        fn handle(
            &self,
            order: &Order,
            _outs: &(),
            _ctx: &mut Context<'_>,
        ) -> impl Future<Output = Result<(), HandlerOutcome>> {
            tracing::info!(order.id, "accepted");
            ready(Ok(()))
        }
    }

    fn app() -> impl App {
        RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
            b.include(subscriber("orders", Accept).build());
        })
    }

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
    use std::future::{Future, ready};

    use ruststream::memory::prelude::*;
    use ruststream::testing::TestApp;

    use super::Order;

    // --8<-- [start:received]
    struct Accept;

    impl Handle<Order> for Accept {
        fn handle(
            &self,
            order: &Order,
            _outs: &(),
            _ctx: &mut Context<'_>,
        ) -> impl Future<Output = Result<(), HandlerOutcome>> {
            tracing::info!(order.id, "accepted");
            ready(Ok(()))
        }
    }

    fn app() -> impl App {
        RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
            b.include(subscriber("orders", Accept).build());
        })
    }

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
    use std::future::{Future, ready};

    use ruststream::memory::prelude::*;
    use ruststream::testing::TestApp;

    use super::{Frame, Wire};

    // --8<-- [start:received_raw]
    struct Ingest;

    impl<'p> Handle<Frame<'p>> for Ingest {
        fn handle(
            &self,
            frame: &Frame<'p>,
            _outs: &(),
            _ctx: &mut Context<'_>,
        ) -> impl Future<Output = Result<(), HandlerOutcome>> {
            tracing::info!(bytes = frame.0.len(), "ingested");
            ready(Ok(()))
        }
    }

    fn app() -> impl App {
        RustStream::new(AppInfo::new("frames", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
            b.include(subscriber("frames", Ingest).build());
        })
    }

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
    use std::future::{Future, ready};

    use ruststream::memory::prelude::*;
    use ruststream::testing::TestApp;

    use super::Order;

    // --8<-- [start:batches]
    struct Bill;

    impl Handle<[Order]> for Bill {
        fn handle(
            &self,
            orders: &[Order],
            _outs: &(),
            _ctx: &mut Context<'_>,
        ) -> impl Future<Output = Result<(), Vec<HandlerOutcome>>> {
            tracing::info!(count = orders.len(), "billed");
            ready(Ok(()))
        }
    }

    fn app(broker: MemoryBroker<Retaining>) -> impl App {
        RustStream::new(AppInfo::new("billing", "0.1.0")).with_broker(broker, |b| {
            b.include(
                subscriber("orders", Bill)
                    .batch(nonzero!(2))
                    .start_at(MemoryPosition::start())
                    .build(),
            );
        })
    }

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
    use std::future::{Future, ready};

    use ruststream::memory::prelude::*;
    use ruststream::testing::TestApp;

    use super::{Frame, Wire};

    // --8<-- [start:batches_raw]
    struct Ingest;

    impl<'p> Handle<[Frame<'p>]> for Ingest {
        fn handle(
            &self,
            frames: &[Frame<'p>],
            _outs: &(),
            _ctx: &mut Context<'_>,
        ) -> impl Future<Output = Result<(), Vec<HandlerOutcome>>> {
            tracing::info!(count = frames.len(), "ingested");
            ready(Ok(()))
        }
    }

    fn app(broker: MemoryBroker<Retaining>) -> impl App {
        RustStream::new(AppInfo::new("frames", "0.1.0")).with_broker(broker, |b| {
            b.include(
                subscriber("frames", Ingest)
                    .batch(nonzero!(2))
                    .start_at(MemoryPosition::start())
                    .build(),
            );
        })
    }

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
    use std::future::{Future, ready};

    use ruststream::memory::prelude::*;
    use ruststream::testing::TestApp;

    use super::Order;

    // --8<-- [start:assert_batch_sizes]
    struct Bill;

    impl Handle<[Order]> for Bill {
        fn handle(
            &self,
            orders: &[Order],
            _outs: &(),
            _ctx: &mut Context<'_>,
        ) -> impl Future<Output = Result<(), Vec<HandlerOutcome>>> {
            tracing::info!(count = orders.len(), "billed");
            ready(Ok(()))
        }
    }

    fn app(broker: MemoryBroker<Retaining>) -> impl App {
        RustStream::new(AppInfo::new("billing", "0.1.0")).with_broker(broker, |b| {
            b.include(
                subscriber("orders", Bill)
                    .batch(nonzero!(2))
                    .start_at(MemoryPosition::start())
                    .build(),
            );
        })
    }

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
