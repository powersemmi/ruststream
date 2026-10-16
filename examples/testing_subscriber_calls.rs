//! The subscriber assertions that count handler calls and read how they settled: how often a
//! handler ran, the settlement of its latest call, the sequence of every settlement, and the two
//! failures a call can end in. Every scenario is a test of its own, the way a service writes it.
//!
//! ```text
//! cargo test --example testing_subscriber_calls --features testing,macros,memory,json
//! ```

// A plain build compiles the services; the tests that use them exist only in the test build.
#![cfg_attr(not(test), allow(dead_code, unused_imports))]

use ruststream::memory::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
struct Order {
    id: u64,
}

/// Bytes published as themselves: how a test injects a payload that is no `Order`.
#[derive(Outgoing, Serialized)]
struct Wire(Vec<u8>);

mod assert_called_once {
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

    use ruststream::memory::prelude::*;
    use ruststream::testing::TestApp;

    use super::Order;

    #[derive(OutSlot)]
    #[publishes(Order)]
    struct Rejected;

    #[subscriber("orders")]
    async fn accept(order: &Order, Out(out): Out<impl Publisher, Rejected>) -> HandlerOutcome {
        if order.id == 0 && out.message(order).to("rejected").publish().await.is_err() {
            return HandlerOutcome::retry();
        }
        HandlerOutcome::ack()
    }

    #[subscriber("rejected")]
    async fn review(order: &Order) -> HandlerOutcome {
        tracing::info!(order.id, "sent to review");
        HandlerOutcome::ack()
    }

    fn app() -> impl App {
        RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
            b.include(accept).out(Rejected, Publish).build();
            b.include(review);
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

    use ruststream::memory::prelude::*;
    use ruststream::testing::TestApp;

    use super::Order;

    #[subscriber("orders")]
    async fn accept(order: &Order) -> HandlerOutcome {
        if order.id == 0 {
            return HandlerOutcome::drop();
        }
        HandlerOutcome::ack()
    }

    fn app() -> impl App {
        RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
            b.include(accept);
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

    use ruststream::memory::prelude::*;
    use ruststream::testing::{Outcome, TestApp};

    use super::Order;

    #[subscriber("orders")]
    async fn accept(order: &Order) -> HandlerOutcome {
        if order.id == 0 {
            return HandlerOutcome::drop();
        }
        HandlerOutcome::ack()
    }

    fn app() -> impl App {
        RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
            b.include(accept);
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
    use std::time::Duration;

    use ruststream::memory::prelude::*;
    use ruststream::testing::{Outcome, TestApp};

    use super::Order;

    #[subscriber("orders")]
    async fn reserve(order: &Order) -> HandlerOutcome {
        tracing::info!(order.id, "stock not there yet");
        HandlerOutcome::retry_after(Duration::from_secs(30))
    }

    fn app() -> impl App {
        RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
            b.include(reserve);
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

    use ruststream::memory::prelude::*;
    use ruststream::testing::TestApp;

    use super::Order;

    #[subscriber("orders")]
    async fn accept(order: &Order) -> HandlerOutcome {
        assert_ne!(order.id, 0, "an order always has an id");
        HandlerOutcome::ack()
    }

    fn app() -> impl App {
        RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
            b.include(accept);
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

    use ruststream::memory::prelude::*;
    use ruststream::testing::TestApp;

    use super::{Order, Wire};

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
