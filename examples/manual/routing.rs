//! The services of `examples/routing.rs` written without the `macros` feature: the routers, the
//! merge, the layer and the codec are the same calls; only the handlers are written by hand.
//!
//! ```text
//! cargo test --example manual_routing --no-default-features --features testing,memory,json,cbor
//! ```

// A plain build compiles the services; the tests that use them exist only in the test build.
#![cfg_attr(not(test), allow(dead_code, unused_imports))]

use ruststream::memory::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, schemars::JsonSchema)]
struct Order {
    id: u64,
}

// `#[outgoing(name = "orders")]` by hand: the type fixes its destination.
impl OutgoingDestination for Order {
    type Form = FixedName;

    const DESTINATION: &'static str = "orders";
}

impl MessageHeaders for Order {
    type Contract = NoHeaders;
}

#[derive(Debug, PartialEq, Deserialize, Serialize, schemars::JsonSchema)]
struct Shipment {
    order_id: u64,
}

// `#[outgoing(name = "shipments")]` by hand: the type fixes its destination.
impl OutgoingDestination for Shipment {
    type Form = FixedName;

    const DESTINATION: &'static str = "shipments";
}

impl MessageHeaders for Shipment {
    type Contract = NoHeaders;
}

mod router {
    use std::error::Error;
    use std::future::{Future, ready};

    use ruststream::memory::prelude::*;
    use ruststream::testing::TestApp;

    use super::{Order, Shipment};

    // --8<-- [start:router]
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

    struct Dispatch;

    impl Handle<Shipment> for Dispatch {
        fn handle(
            &self,
            shipment: &Shipment,
            _outs: &(),
            _ctx: &mut Context<'_>,
        ) -> impl Future<Output = Result<(), HandlerOutcome>> {
            tracing::info!(shipment.order_id, "dispatched");
            ready(Ok(()))
        }
    }

    /// The orders module's handlers.
    fn orders() -> Router<MemoryBroker, impl RouterDef<MemoryBroker>> {
        Router::new().include(subscriber("orders", Accept).build())
    }

    /// The shipping module's handlers.
    fn shipping() -> Router<MemoryBroker, impl RouterDef<MemoryBroker>> {
        Router::new().include(subscriber("shipments", Dispatch).build())
    }

    fn app() -> impl App {
        RustStream::new(AppInfo::new("shop", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
            b.include_router(orders().merge(shipping()));
        })
    }
    // --8<-- [end:router]

    #[tokio::test]
    async fn both_modules_are_mounted() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        let broker = tb.broker::<MemoryBroker>();
        broker.message(&Order { id: 7 }).publish().await?;
        broker.message(&Shipment { order_id: 7 }).publish().await?;

        broker.subscriber("orders").assert_called_once();
        broker.subscriber("shipments").assert_called_once();
        Ok(())
    }
}

mod layer {
    use std::error::Error;
    use std::future::{Future, ready};
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use ruststream::memory::prelude::*;
    use ruststream::runtime::{BlanketLayer, Handler};
    use ruststream::testing::TestApp;

    use super::{Order, Shipment};

    // --8<-- [start:layer]
    /// Counts the deliveries of the handlers it wraps.
    #[derive(Clone)]
    struct Count(Arc<AtomicUsize>);

    struct Counted<H> {
        inner: H,
        count: Arc<AtomicUsize>,
    }

    // A router hides its handlers' types, so its layer wraps a handler of any type.
    impl BlanketLayer for Count {
        fn apply<M, C, S, H>(&self, handler: H) -> impl Handler<M, C, S> + 'static
        where
            M: Send + Sync + 'static,
            C: Send + 'static,
            S: Send + Sync + 'static,
            H: Handler<M, C, S> + 'static,
        {
            Counted {
                inner: handler,
                count: Arc::clone(&self.0),
            }
        }
    }

    impl<M, C, S, H> Handler<M, C, S> for Counted<H>
    where
        M: Send + Sync,
        C: Send,
        S: Send + Sync,
        H: Handler<M, C, S>,
    {
        async fn handle(&self, msg: &M, ctx: &mut Context<'_, C, S>) -> HandlerOutcome {
            self.count.fetch_add(1, Ordering::Relaxed);
            self.inner.handle(msg, ctx).await
        }
    }

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

    struct Dispatch;

    impl Handle<Shipment> for Dispatch {
        fn handle(
            &self,
            shipment: &Shipment,
            _outs: &(),
            _ctx: &mut Context<'_>,
        ) -> impl Future<Output = Result<(), HandlerOutcome>> {
            tracing::info!(shipment.order_id, "dispatched");
            ready(Ok(()))
        }
    }

    fn app(orders_seen: Arc<AtomicUsize>) -> impl App {
        RustStream::new(AppInfo::new("shop", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
            // The layer comes first and wraps every handler of this router.
            b.include_router(
                Router::new()
                    .layer(Count(orders_seen))
                    .include(subscriber("orders", Accept).build()),
            );
            // Mounted on the scope, outside the router's layer.
            b.include(subscriber("shipments", Dispatch).build());
        })
    }
    // --8<-- [end:layer]

    #[tokio::test]
    async fn the_layer_wraps_only_its_router() -> Result<(), Box<dyn Error>> {
        let orders_seen = Arc::new(AtomicUsize::new(0));
        let tb = TestApp::start(app(Arc::clone(&orders_seen))).await?;
        let broker = tb.broker::<MemoryBroker>();
        broker.message(&Order { id: 7 }).publish().await?;
        broker.message(&Shipment { order_id: 7 }).publish().await?;

        assert_eq!(orders_seen.load(Ordering::Relaxed), 1);
        Ok(())
    }
}

#[cfg(feature = "cbor")]
mod codec {
    use std::error::Error;
    use std::future::{Future, ready};

    use ruststream::codec::CborCodec;
    use ruststream::memory::prelude::*;
    use ruststream::testing::TestApp;

    use super::Order;

    // --8<-- [start:codec]
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

    /// The registrations after `with_codec` decode CBOR.
    fn orders() -> impl RouterDef<MemoryBroker> {
        Router::new()
            .with_codec(CborCodec)
            .include(subscriber("orders", Accept).build())
    }

    fn app() -> impl App {
        RustStream::new(AppInfo::new("shop", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
            b.include_router(orders());
        })
    }
    // --8<-- [end:codec]

    #[tokio::test]
    async fn the_router_decodes_cbor() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        let broker = tb.broker::<MemoryBroker>();
        broker
            .message(&Order { id: 7 })
            .with_codec(CborCodec)
            .publish()
            .await?;

        broker
            .subscriber("orders")
            .assert_called_once()
            .with_codec(&CborCodec, &Order { id: 7 })
            .settled(HandlerOutcome::ack());
        Ok(())
    }
}

// The scenarios are the tests above; the command in the header runs them.
fn main() {}
