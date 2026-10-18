//! The subscriber assertions that count handler calls and read how they settled, against services
//! written without the `macros` feature. The test bodies are the ones of
//! `examples/testing_subscriber_calls.rs`: the harness reads the same records whichever way the
//! service was declared.
//!
//! ```text
//! cargo test --example manual_testing_subscriber_calls --no-default-features --features testing,memory,json
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

impl<M: OutSlot> OutMessages<M> for Order {
    fn outgoing() -> Vec<OutgoingMessageMetadata> {
        Vec::new()
    }
}

/// Bytes published as themselves: how a test injects a payload that is no `Order`. What
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

mod assert_called_once {
    use std::error::Error;
    use std::future::ready;

    use ruststream::memory::prelude::*;
    use ruststream::testing::TestApp;

    use super::Order;

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

    // --8<-- [start:assert_called_once]
    #[tokio::test]
    async fn the_handler_ran_once() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        tb.broker::<MemoryBroker>()
            .message(&Order { id: 7 })
            .to("orders")
            .publish()
            .await?;

        tb.broker::<MemoryBroker>()
            .subscriber("orders")
            .assert_called_once();
        Ok(())
    }
    // --8<-- [end:assert_called_once]
}

mod assert_called {
    use std::error::Error;
    use std::future::ready;

    use ruststream::memory::prelude::*;
    use ruststream::testing::TestApp;

    use super::Order;

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

    // --8<-- [start:assert_called]
    #[tokio::test]
    async fn the_handler_ran_once_per_order() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        let broker = tb.broker::<MemoryBroker>();
        for id in 1..=3 {
            broker.message(&Order { id }).to("orders").publish().await?;
        }

        broker.subscriber("orders").assert_called(3);
        Ok(())
    }
    // --8<-- [end:assert_called]
}

mod assert_not_called {
    use std::error::Error;
    use std::future::ready;

    use ruststream::memory::prelude::*;
    use ruststream::testing::TestApp;

    use super::Order;

    struct Rejected;

    impl OutSlot for Rejected {
        const NAME: &'static str = "Rejected";
        type Destination = Reads;

        fn outgoing() -> Vec<OutgoingMessageMetadata> {
            <Order as OutMessages<Self>>::outgoing()
        }
    }

    impl PublishedThrough<Rejected> for Order {}

    struct Accept;

    impl<Entry> Handle<Order, (), Outs<(Entry,)>> for Accept
    where
        Entry: OutEntry<Rejected, Wire: Publisher>,
    {
        async fn handle(
            &self,
            order: &Order,
            outs: &Outs<(Entry,)>,
            _ctx: &mut Context<'_>,
        ) -> Result<(), HandlerOutcome> {
            if order.id == 0
                && outs
                    .get(Rejected)
                    .message(order)
                    .to("rejected")
                    .publish()
                    .await
                    .is_err()
            {
                return Err(HandlerOutcome::retry());
            }
            Ok(())
        }
    }

    struct Review;

    impl Handle<Order> for Review {
        fn handle(
            &self,
            order: &Order,
            _outs: &(),
            _ctx: &mut Context<'_>,
        ) -> impl Future<Output = Result<(), HandlerOutcome>> {
            tracing::info!(order.id, "sent to review");
            ready(Ok(()))
        }
    }

    fn app() -> impl App {
        RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
            b.include(subscriber("orders", Accept).build())
                .out(Rejected, Publish)
                .build();
            b.include(subscriber("rejected", Review).build());
        })
    }

    // --8<-- [start:assert_not_called]
    #[tokio::test]
    async fn a_valid_order_is_not_reviewed() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        let broker = tb.broker::<MemoryBroker>();
        broker
            .message(&Order { id: 7 })
            .to("orders")
            .publish()
            .await?;

        broker.subscriber("orders").assert_called_once();
        broker.subscriber("rejected").assert_not_called();
        Ok(())
    }
    // --8<-- [end:assert_not_called]
}

mod settled {
    use std::error::Error;
    use std::future::ready;

    use ruststream::memory::prelude::*;
    use ruststream::testing::TestApp;

    use super::Order;

    struct Accept;

    impl Handle<Order> for Accept {
        fn handle(
            &self,
            order: &Order,
            _outs: &(),
            _ctx: &mut Context<'_>,
        ) -> impl Future<Output = Result<(), HandlerOutcome>> {
            if order.id == 0 {
                return ready(Err(HandlerOutcome::drop()));
            }
            ready(Ok(()))
        }
    }

    fn app() -> impl App {
        RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
            b.include(subscriber("orders", Accept).build());
        })
    }

    // --8<-- [start:settled]
    #[tokio::test]
    async fn an_order_without_an_id_is_dropped() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        let broker = tb.broker::<MemoryBroker>();
        broker
            .message(&Order { id: 0 })
            .to("orders")
            .publish()
            .await?;

        broker
            .subscriber("orders")
            .assert_called_once()
            .settled(HandlerOutcome::drop());
        Ok(())
    }
    // --8<-- [end:settled]
}

mod outcomes {
    use std::error::Error;
    use std::future::ready;

    use ruststream::memory::prelude::*;
    use ruststream::testing::{Outcome, TestApp};

    use super::Order;

    struct Accept;

    impl Handle<Order> for Accept {
        fn handle(
            &self,
            order: &Order,
            _outs: &(),
            _ctx: &mut Context<'_>,
        ) -> impl Future<Output = Result<(), HandlerOutcome>> {
            if order.id == 0 {
                return ready(Err(HandlerOutcome::drop()));
            }
            ready(Ok(()))
        }
    }

    fn app() -> impl App {
        RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
            b.include(subscriber("orders", Accept).build());
        })
    }

    // --8<-- [start:outcomes]
    #[tokio::test]
    async fn every_order_settled_in_turn() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        let broker = tb.broker::<MemoryBroker>();
        for id in [1, 0, 2] {
            broker.message(&Order { id }).to("orders").publish().await?;
        }

        assert_eq!(
            broker.subscriber("orders").outcomes(),
            [Outcome::Ack, Outcome::Drop, Outcome::Ack],
        );
        Ok(())
    }
    // --8<-- [end:outcomes]
}

mod assert_outcome {
    use std::error::Error;
    use std::future::ready;
    use std::time::Duration;

    use ruststream::memory::prelude::*;
    use ruststream::testing::{Outcome, TestApp};

    use super::Order;

    struct Reserve;

    impl Handle<Order> for Reserve {
        fn handle(
            &self,
            order: &Order,
            _outs: &(),
            _ctx: &mut Context<'_>,
        ) -> impl Future<Output = Result<(), HandlerOutcome>> {
            tracing::info!(order.id, "stock not there yet");
            ready(Err(HandlerOutcome::retry_after(Duration::from_secs(30))))
        }
    }

    fn app() -> impl App {
        RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
            b.include(subscriber("orders", Reserve).build());
        })
    }

    // --8<-- [start:assert_outcome]
    #[tokio::test(start_paused = true)]
    async fn a_delayed_retry_reads_as_a_nack() -> Result<(), Box<dyn Error>> {
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
            .assert_outcome(Outcome::Nack);
        Ok(())
    }
    // --8<-- [end:assert_outcome]
}

mod panicked {
    use std::error::Error;
    use std::future::ready;

    use ruststream::memory::prelude::*;
    use ruststream::testing::TestApp;

    use super::Order;

    struct Accept;

    impl Handle<Order> for Accept {
        fn handle(
            &self,
            order: &Order,
            _outs: &(),
            _ctx: &mut Context<'_>,
        ) -> impl Future<Output = Result<(), HandlerOutcome>> {
            assert_ne!(order.id, 0, "an order always has an id");
            ready(Ok(()))
        }
    }

    fn app() -> impl App {
        RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
            b.include(subscriber("orders", Accept).build());
        })
    }

    // --8<-- [start:panicked]
    #[tokio::test]
    async fn a_panic_stops_the_service() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        let broker = tb.broker::<MemoryBroker>();
        broker
            .message(&Order { id: 0 })
            .to("orders")
            .publish()
            .await?;

        broker.subscriber("orders").assert_called_once().panicked();
        tb.assert_shut_down();
        Ok(())
    }
    // --8<-- [end:panicked]
}

mod assert_last_failed_to_decode {
    use std::error::Error;
    use std::future::ready;

    use ruststream::memory::prelude::*;
    use ruststream::testing::TestApp;

    use super::{Order, Wire};

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

    // --8<-- [start:assert_last_failed_to_decode]
    #[tokio::test]
    async fn bytes_that_are_no_order_are_dropped() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        let broker = tb.broker::<MemoryBroker>();
        broker
            .message(&Wire(b"not json".to_vec()))
            .to("orders")
            .publish()
            .await?;

        broker
            .subscriber("orders")
            .assert_called_once()
            .settled(HandlerOutcome::drop())
            .assert_last_failed_to_decode();
        tb.assert_running();
        Ok(())
    }
    // --8<-- [end:assert_last_failed_to_decode]
}

// The scenarios are the tests above; the command in the header runs them.
fn main() {}
