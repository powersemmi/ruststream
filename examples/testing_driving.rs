//! Driving the service under test: addressing a broker, building and publishing an input, and
//! moving the service forward when a publish alone does not. Every scenario is a test of its own,
//! the way a service writes it.
//!
//! ```text
//! cargo test --example testing_driving --features testing,macros,memory,json
//! ```

// A plain build compiles the services; the tests that use them exist only in the test build.
#![cfg_attr(not(test), allow(dead_code, unused_imports))]

use ruststream::memory::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "orders")]
struct Order {
    id: u64,
}

mod broker {
    use std::error::Error;

    use ruststream::memory::prelude::*;
    use ruststream::testing::TestApp;

    use super::Order;

    // --8<-- [start:broker]
    #[subscriber("orders")]
    async fn accept(order: &Order) -> HandlerOutcome {
        let _ = order.id;
        HandlerOutcome::ack()
    }

    #[subscriber("orders")]
    async fn archive(order: &Order) -> HandlerOutcome {
        let _ = order.id;
        HandlerOutcome::ack()
    }

    /// Commands go through a plain broker; the archive keeps its history on a retaining one.
    fn app() -> impl App<State = ()> {
        RustStream::new(AppInfo::new("orders", "0.1.0"))
            .with_broker(MemoryBroker::new(), |b| {
                b.include(accept);
            })
            .with_broker(
                MemoryBroker::retaining(Retention::Messages(nonzero!(1000))),
                |b| {
                    b.include(archive);
                },
            )
    }

    #[tokio::test]
    async fn each_broker_is_addressed_by_its_type() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        tb.broker::<MemoryBroker<Retaining>>()
            .message(&Order { id: 7 })
            .publish()
            .await?;

        tb.broker::<MemoryBroker<Retaining>>()
            .subscriber("orders")
            .assert_called_once();
        tb.broker::<MemoryBroker>()
            .subscriber("orders")
            .assert_not_called();
        Ok(())
    }
    // --8<-- [end:broker]
}

mod broker_named {
    use std::error::Error;

    use ruststream::memory::prelude::*;
    use ruststream::testing::TestApp;

    use super::Order;

    // --8<-- [start:broker-named]
    #[subscriber("orders")]
    async fn accept_eu(order: &Order) -> HandlerOutcome {
        let _ = order.id;
        HandlerOutcome::ack()
    }

    #[subscriber("orders")]
    async fn accept_us(order: &Order) -> HandlerOutcome {
        let _ = order.id;
        HandlerOutcome::ack()
    }

    /// One broker per region, both of the same type.
    fn app() -> impl App<State = ()> {
        RustStream::new(AppInfo::new("orders", "0.1.0"))
            .with_broker_labeled("eu", MemoryBroker::new(), |b| {
                b.include(accept_eu);
            })
            .with_broker_labeled("us", MemoryBroker::new(), |b| {
                b.include(accept_us);
            })
    }

    #[tokio::test]
    async fn an_order_stays_in_its_region() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        tb.broker_named("eu")
            .message(&Order { id: 7 })
            .publish()
            .await?;

        tb.broker_named("eu")
            .subscriber("orders")
            .assert_called_once()
            .with(&Order { id: 7 });
        tb.broker_named("us")
            .subscriber("orders")
            .assert_not_called();
        Ok(())
    }
    // --8<-- [end:broker-named]
}

mod message {
    use std::error::Error;

    use ruststream::memory::prelude::*;
    use ruststream::testing::TestApp;
    use serde::{Deserialize, Serialize};

    use super::Order;

    // --8<-- [start:message]
    /// Names no destination: whoever publishes it says where.
    #[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
    struct Refund {
        order_id: u64,
    }

    #[subscriber("orders")]
    async fn accept(order: &Order) -> HandlerOutcome {
        let _ = order.id;
        HandlerOutcome::ack()
    }

    #[subscriber("refunds")]
    async fn issue_refund(refund: &Refund) -> HandlerOutcome {
        let _ = refund.order_id;
        HandlerOutcome::ack()
    }

    fn app() -> impl App<State = ()> {
        RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
            b.include(accept);
            b.include(issue_refund);
        })
    }

    #[tokio::test]
    async fn inputs_are_built_like_the_service_builds_its_own() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        tb.message(&Order { id: 7 }).publish().await?;
        tb.broker::<MemoryBroker>()
            .message(&Refund { order_id: 7 })
            .to("refunds")
            .publish()
            .await?;

        let broker = tb.broker::<MemoryBroker>();
        broker
            .subscriber("orders")
            .assert_called_once()
            .with(&Order { id: 7 });
        broker
            .subscriber("refunds")
            .assert_called_once()
            .with(&Refund { order_id: 7 });
        Ok(())
    }
    // --8<-- [end:message]
}

mod publish {
    use std::error::Error;

    use ruststream::memory::prelude::*;
    use ruststream::testing::TestApp;
    use serde::{Deserialize, Serialize};

    use super::Order;

    // --8<-- [start:publish]
    #[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
    #[outgoing(name = "receipts")]
    struct Receipt {
        order_id: u64,
    }

    #[subscriber("orders", reply)]
    async fn issue_receipt(order: &Order) -> Receipt {
        Receipt { order_id: order.id }
    }

    #[subscriber("receipts")]
    async fn file_receipt(receipt: &Receipt) -> HandlerOutcome {
        let _ = receipt.order_id;
        HandlerOutcome::ack()
    }

    fn app() -> impl App<State = ()> {
        RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
            b.include(issue_receipt).out_reply(Publish);
            b.include(file_receipt);
        })
    }

    #[tokio::test]
    async fn the_whole_reaction_is_over_when_publish_returns() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        tb.message(&Order { id: 7 }).publish().await?;

        tb.broker::<MemoryBroker>()
            .subscriber("receipts")
            .assert_called_once()
            .with(&Receipt { order_id: 7 })
            .settled(HandlerOutcome::ack());
        Ok(())
    }
    // --8<-- [end:publish]
}

mod publish_shortcuts {
    use std::error::Error;

    use ruststream::memory::prelude::*;
    use ruststream::testing::TestApp;
    use serde::{Deserialize, Serialize};
    use serde_json::json;

    use super::Order;

    // --8<-- [start:publish-shortcuts]
    /// The header contract of a tenant's order.
    #[derive(Debug, Deserialize, Serialize, schemars::JsonSchema)]
    struct Tenant {
        tenant: String,
    }

    #[subscriber("orders")]
    async fn accept(order: &Order) -> HandlerOutcome {
        let _ = order.id;
        HandlerOutcome::ack()
    }

    #[subscriber("tenant-orders")]
    async fn accept_for_tenant(order: &Message<Tenant, Order>) -> HandlerOutcome {
        if order.headers.tenant.is_empty() {
            return HandlerOutcome::drop();
        }
        HandlerOutcome::ack()
    }

    fn app() -> impl App<State = ()> {
        RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
            b.include(accept);
            b.include(accept_for_tenant);
        })
    }

    #[tokio::test]
    async fn any_serde_value_can_be_published() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        tb.publish("orders", &json!({ "id": 7 })).await?;
        tb.broker::<MemoryBroker>()
            .publish_with_headers(
                "tenant-orders",
                &json!({ "id": 8 }),
                &Tenant {
                    tenant: "acme".to_owned(),
                },
            )
            .await?;

        let broker = tb.broker::<MemoryBroker>();
        broker
            .subscriber("orders")
            .assert_called_once()
            .with(&Order { id: 7 });
        broker
            .subscriber("tenant-orders")
            .assert_called_once()
            .settled(HandlerOutcome::ack());
        Ok(())
    }
    // --8<-- [end:publish-shortcuts]
}

mod settle {
    use std::error::Error;

    use ruststream::memory::prelude::*;
    use ruststream::testing::TestApp;

    use super::Order;

    // --8<-- [start:settle]
    #[subscriber("orders")]
    async fn accept_all(orders: &[Order]) -> HandlerOutcome {
        let _ = orders.len();
        HandlerOutcome::ack()
    }

    fn app(broker: MemoryBroker) -> impl App<State = ()> {
        RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(broker, |b| {
            b.include(accept_all.batch(nonzero!(8)));
        })
    }

    #[tokio::test]
    async fn two_orders_arrive_in_one_batch() -> Result<(), Box<dyn Error>> {
        let broker = MemoryBroker::new();
        let publisher = broker.publisher();
        let tb = TestApp::start(app(broker)).await?;
        for id in [1, 2] {
            publisher.message(&Order { id }).publish().await?;
        }
        tb.settle().await?;

        tb.broker::<MemoryBroker>()
            .subscriber("orders")
            .assert_called_once()
            .assert_batch_sizes(&[2]);
        Ok(())
    }
    // --8<-- [end:settle]
}

mod advance {
    use std::error::Error;
    use std::time::Duration;

    use ruststream::memory::prelude::*;
    use ruststream::testing::TestApp;
    use serde::{Deserialize, Serialize};

    // --8<-- [start:advance]
    const RECHECK: Duration = Duration::from_secs(5);

    #[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
    #[outgoing(name = "payments")]
    struct Payment {
        id: u64,
        cleared: bool,
    }

    #[subscriber("payments")]
    async fn reconcile(payment: &Payment) -> HandlerOutcome {
        if !payment.cleared {
            return HandlerOutcome::retry_after(RECHECK);
        }
        HandlerOutcome::ack()
    }

    fn app() -> impl App<State = ()> {
        RustStream::new(AppInfo::new("payments", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
            b.include(reconcile);
        })
    }

    #[tokio::test(start_paused = true)]
    async fn an_uncleared_payment_is_checked_again_later() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        let payment = Payment {
            id: 7,
            cleared: false,
        };
        tb.message(&payment).publish().await?;
        tb.broker::<MemoryBroker>()
            .subscriber("payments")
            .assert_called_once()
            .settled(HandlerOutcome::retry_after(RECHECK));

        tb.advance(RECHECK).await?;
        tb.broker::<MemoryBroker>()
            .subscriber("payments")
            .assert_called(2)
            .with(&payment);
        Ok(())
    }
    // --8<-- [end:advance]
}

mod drain {
    use std::convert::Infallible;
    use std::error::Error;
    use std::sync::{Arc, Mutex};

    use ruststream::memory::prelude::*;
    use ruststream::testing::TestApp;

    use super::Order;

    // --8<-- [start:drain]
    /// Stands in for a mail client: each send is a network round trip.
    #[derive(Default)]
    struct Mailer {
        sent: Mutex<Vec<u64>>,
    }

    impl Mailer {
        async fn confirm(&self, order_id: u64) {
            tokio::task::yield_now().await;
            if let Ok(mut sent) = self.sent.lock() {
                sent.push(order_id);
            }
        }
    }

    #[subscriber("orders")]
    async fn accept(order: &Order, ctx: &mut Context<'_, (), Arc<Mailer>>) -> HandlerOutcome {
        let mailer = Arc::clone(ctx.state());
        let order_id = order.id;
        // The confirmation goes out once the order is acknowledged, and never holds it back.
        HandlerOutcome::ack().and_after(async move { mailer.confirm(order_id).await })
    }

    fn app() -> impl App<State = Arc<Mailer>> {
        RustStream::new(AppInfo::new("orders", "0.1.0"))
            .on_startup(async move |()| Ok::<_, Infallible>(Arc::new(Mailer::default())))
            .with_broker(MemoryBroker::new(), |b| {
                b.include(accept);
            })
    }

    #[tokio::test]
    async fn the_confirmation_follows_the_ack() -> Result<(), Box<dyn Error>> {
        let mailer = Arc::new(Mailer::default());
        let tb = TestApp::with_state(app(), |_| Arc::clone(&mailer)).await?;
        tb.message(&Order { id: 7 }).publish().await?;
        tb.drain().await;

        let sent = mailer.sent.lock().map_err(|_| "a confirmation panicked")?;
        assert_eq!(*sent, [7]);
        Ok(())
    }
    // --8<-- [end:drain]
}

fn main() {
    println!("the scenarios are tests: cargo test --example testing_driving");
}
