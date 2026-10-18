//! The services of `examples/subscriber_source.rs` written without the `macros` feature. The first
//! argument of `subscriber` is the source: a name, the broker's descriptor, or `Unnamed` for a
//! source the mount site names.
//!
//! ```text
//! cargo test --example manual_subscriber_source --no-default-features --features testing,memory,json
//! ```

// A plain build compiles the services; the tests that use them exist only in the test build.
#![cfg_attr(not(test), allow(dead_code, unused_imports))]

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

mod name_at_mount {
    use std::error::Error;
    use std::future::ready;

    use ruststream::memory::prelude::*;
    use ruststream::testing::TestApp;

    use super::Order;

    // --8<-- [start:name_at_mount]
    struct Audit;

    impl Handle<Order> for Audit {
        fn handle(
            &self,
            order: &Order,
            _outs: &(),
            _ctx: &mut Context<'_>,
        ) -> impl Future<Output = Result<(), HandlerOutcome>> {
            tracing::info!(order.id, "audited");
            ready(Ok(()))
        }
    }

    fn app(shard: u32) -> impl App {
        RustStream::new(AppInfo::new("audit", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
            // `Unnamed<Name>` is no source yet: the mount does not compile until `.name(..)`.
            b.include(
                subscriber(Unnamed::<Name>::new(), Audit)
                    .name(format!("audit-{shard}"))
                    .build(),
            );
        })
    }
    // --8<-- [end:name_at_mount]

    #[tokio::test]
    async fn the_mount_site_names_the_subscription() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app(7)).await?;
        let broker = tb.broker::<MemoryBroker>();
        broker
            .message(&Order { id: 1 })
            .to("audit-7")
            .publish()
            .await?;

        broker
            .subscriber("audit-7")
            .assert_called_once()
            .with(&Order { id: 1 });
        Ok(())
    }
}

mod descriptor {
    use std::error::Error;
    use std::future::ready;

    use ruststream::memory::prelude::*;
    use ruststream::testing::TestApp;

    use super::Order;

    // --8<-- [start:descriptor]
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

    struct Archive;

    impl Handle<Order> for Archive {
        fn handle(
            &self,
            order: &Order,
            _outs: &(),
            _ctx: &mut Context<'_>,
        ) -> impl Future<Output = Result<(), HandlerOutcome>> {
            tracing::info!(order.id, "archived");
            ready(Ok(()))
        }
    }

    fn app() -> impl App {
        RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
            // The broker's descriptor, built here.
            b.include(subscriber(MemorySource::new("orders"), Accept).build());
            // The descriptor's kind, named at the mount site.
            b.include(
                subscriber(Unnamed::<MemorySource>::new(), Archive)
                    .name("archive")
                    .build(),
            );
        })
    }
    // --8<-- [end:descriptor]

    #[tokio::test]
    async fn each_descriptor_reads_its_subscription() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        let broker = tb.broker::<MemoryBroker>();
        broker
            .message(&Order { id: 1 })
            .to("orders")
            .publish()
            .await?;
        broker
            .message(&Order { id: 2 })
            .to("archive")
            .publish()
            .await?;

        broker
            .subscriber("orders")
            .assert_called_once()
            .with(&Order { id: 1 });
        broker
            .subscriber("archive")
            .assert_called_once()
            .with(&Order { id: 2 });
        Ok(())
    }
}

// The scenarios are the tests above; the command in the header runs them.
fn main() {}
