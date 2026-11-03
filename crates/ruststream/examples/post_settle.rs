//! Work that runs once a delivery is settled: a continuation on the outcome with `and_after`, one
//! per element on a batch, and hooks on the context gated by the settlement. Each scenario is a
//! test.
//!
//! ```text
//! cargo test --example post_settle --features testing,macros,memory,json
//! ```

// A plain build compiles the services; the tests that use them exist only in the test build.
#![cfg_attr(not(test), allow(dead_code, unused_imports))]

use std::sync::Mutex;

use serde::{Deserialize, Serialize};

use ruststream::memory::prelude::*;
use tokio::task;

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "orders")]
struct Order {
    id: u64,
}

/// Stands in for a notification service: it records what it was told.
#[derive(Default)]
struct Notifier {
    sent: Mutex<Vec<String>>,
}

impl Notifier {
    async fn send(&self, note: String) {
        task::yield_now().await;
        if let Ok(mut sent) = self.sent.lock() {
            sent.push(note);
        }
    }

    fn sent(&self) -> Vec<String> {
        let mut sent = self
            .sent
            .lock()
            .map(|sent| sent.clone())
            .unwrap_or_default();
        // Continuations run on tasks of their own, so their order is not the delivery order.
        sent.sort();
        sent
    }
}

mod single {
    use std::convert::Infallible;
    use std::error::Error;
    use std::sync::Arc;

    use ruststream::memory::prelude::*;
    use ruststream::testing::TestApp;

    use super::{Notifier, Order};

    // --8<-- [start:single]
    #[subscriber("orders")]
    async fn accept(order: &Order, ctx: &mut Context<'_, (), Arc<Notifier>>) -> HandlerOutcome {
        let notifier = Arc::clone(ctx.state());
        let id = order.id;
        // Runs once the ack is through; losing it never brings the order back.
        HandlerOutcome::ack()
            .and_after(async move { notifier.send(format!("accepted {id}")).await })
    }
    // --8<-- [end:single]

    fn app() -> impl App<State = Arc<Notifier>> {
        RustStream::new(AppInfo::new("orders", "0.1.0"))
            .on_startup(async move |()| Ok::<_, Infallible>(Arc::new(Notifier::default())))
            .with_broker(MemoryBroker::new(), |b| b.include(accept))
    }

    #[tokio::test]
    async fn the_notice_follows_the_ack() -> Result<(), Box<dyn Error>> {
        let notifier = Arc::new(Notifier::default());
        let tb = TestApp::with_state(app(), |_| Arc::clone(&notifier)).await?;
        tb.message(&Order { id: 7 }).publish().await?;
        tb.drain().await;

        assert_eq!(notifier.sent(), ["accepted 7"]);
        Ok(())
    }
}

mod batch {
    use std::convert::Infallible;
    use std::error::Error;
    use std::sync::Arc;

    use ruststream::memory::prelude::*;
    use ruststream::testing::TestApp;

    use super::{Notifier, Order};

    // --8<-- [start:batch]
    /// Each element carries its own continuation: the dropped order gets none.
    #[subscriber("orders")]
    async fn accept(
        orders: &[Order],
        ctx: &mut Context<'_, (), Arc<Notifier>>,
    ) -> Vec<HandlerOutcome> {
        orders
            .iter()
            .map(|order| {
                if order.id == 0 {
                    return HandlerOutcome::drop();
                }
                let notifier = Arc::clone(ctx.state());
                let id = order.id;
                HandlerOutcome::ack()
                    .and_after(async move { notifier.send(format!("accepted {id}")).await })
            })
            .collect()
    }
    // --8<-- [end:batch]

    fn app(broker: MemoryBroker<Retaining>) -> impl App<State = Arc<Notifier>> {
        RustStream::new(AppInfo::new("orders", "0.1.0"))
            .on_startup(async move |()| Ok::<_, Infallible>(Arc::new(Notifier::default())))
            .with_broker(broker, |b| {
                b.include(accept.batch(nonzero!(3)).start_at(MemoryPosition::start()))
            })
    }

    #[tokio::test]
    async fn only_the_acked_orders_are_announced() -> Result<(), Box<dyn Error>> {
        // The log already holds three orders, so the subscription reads them as one batch.
        let broker = MemoryBroker::retaining(Retention::Messages(nonzero!(16)));
        let publisher = broker.publisher();
        for id in [1, 0, 2] {
            publisher.message(&Order { id }).publish().await?;
        }
        let notifier = Arc::new(Notifier::default());
        let tb = TestApp::with_state(app(broker), |_| Arc::clone(&notifier)).await?;
        tb.settle().await?;
        tb.drain().await;

        assert_eq!(notifier.sent(), ["accepted 1", "accepted 2"]);
        Ok(())
    }
}

mod hooks {
    use std::convert::Infallible;
    use std::error::Error;
    use std::sync::Arc;

    use ruststream::memory::prelude::*;
    use ruststream::testing::TestApp;

    use super::{Notifier, Order};

    // --8<-- [start:hooks]
    #[subscriber("orders")]
    async fn accept(order: &Order, ctx: &mut Context<'_, (), Arc<Notifier>>) -> HandlerOutcome {
        let id = order.id;
        let notifier = Arc::clone(ctx.state());
        ctx.after_ack(async move { notifier.send(format!("shipping {id}")).await });
        let notifier = Arc::clone(ctx.state());
        ctx.after(HandlerOutcome::drop())
            .then(async move { notifier.send(format!("rejected {id}")).await });
        let notifier = Arc::clone(ctx.state());
        ctx.after_settle(async move { notifier.send(format!("settled {id}")).await });

        if id == 0 {
            return HandlerOutcome::drop();
        }
        HandlerOutcome::ack()
    }
    // --8<-- [end:hooks]

    fn app() -> impl App<State = Arc<Notifier>> {
        RustStream::new(AppInfo::new("orders", "0.1.0"))
            .on_startup(async move |()| Ok::<_, Infallible>(Arc::new(Notifier::default())))
            .with_broker(MemoryBroker::new(), |b| b.include(accept))
    }

    #[tokio::test]
    async fn each_hook_runs_on_its_own_settlement() -> Result<(), Box<dyn Error>> {
        let notifier = Arc::new(Notifier::default());
        let tb = TestApp::with_state(app(), |_| Arc::clone(&notifier)).await?;
        tb.message(&Order { id: 7 }).publish().await?;
        tb.message(&Order { id: 0 }).publish().await?;
        tb.drain().await;

        assert_eq!(
            notifier.sent(),
            ["rejected 0", "settled 0", "settled 7", "shipping 7"],
        );
        Ok(())
    }
}

// The scenarios are the tests above; the command in the header runs them.
fn main() {}
