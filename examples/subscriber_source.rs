//! Where a subscriber reads from: a name, or the broker's own descriptor, either fixed in the
//! attribute or left to the mount site. Each scenario is a test.
//!
//! ```text
//! cargo test --example subscriber_source --features testing,macros,memory,json
//! ```

// A plain build compiles the services; the tests that use them exist only in the test build.
#![cfg_attr(not(test), allow(dead_code, unused_imports))]

use ruststream::memory::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
struct Order {
    id: u64,
}

mod name_at_mount {
    use std::error::Error;

    use ruststream::memory::prelude::*;
    use ruststream::testing::TestApp;

    use super::Order;

    // --8<-- [start:name_at_mount]
    /// The attribute leaves the name open: the mount site gives it.
    #[subscriber]
    async fn audit(order: &Order) -> HandlerOutcome {
        tracing::info!(order.id, "audited");
        HandlerOutcome::ack()
    }

    fn app(shard: u32) -> impl App {
        RustStream::new(AppInfo::new("audit", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
            b.include(audit.name(format!("audit-{shard}")));
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

    use ruststream::memory::prelude::*;
    use ruststream::testing::TestApp;

    use super::Order;

    // --8<-- [start:descriptor]
    /// The broker's descriptor, fixed in the attribute.
    #[subscriber(MemorySource::new("orders"))]
    async fn accept(order: &Order) -> HandlerOutcome {
        tracing::info!(order.id, "accepted");
        HandlerOutcome::ack()
    }

    /// The descriptor's kind in the attribute, its name at the mount site.
    #[subscriber(MemorySource)]
    async fn archive(order: &Order) -> HandlerOutcome {
        tracing::info!(order.id, "archived");
        HandlerOutcome::ack()
    }

    fn app() -> impl App {
        RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
            b.include(accept);
            b.include(archive.name("archive"));
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
