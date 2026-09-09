//! Where a reply is published, driven end to end on the in-memory broker: the reply type's own
//! declaration decides it, and the mount-site name is the default a reply type declaring none
//! takes. Both surfaces are here, because the attribute and the chain resolve the same way.
#![cfg(all(
    feature = "testing",
    feature = "macros",
    feature = "memory",
    feature = "json"
))]

use std::future::{Future, ready};

use ruststream::memory::prelude::*;
use ruststream::testing::TestApp;
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Outgoing, Serialize, PartialEq, schemars::JsonSchema)]
struct Order {
    id: u64,
}

/// A reply that fixes its destination: every mounting publishes it on `dest.declared`.
#[derive(Debug, Deserialize, Outgoing, Serialize, PartialEq, schemars::JsonSchema)]
#[outgoing(name = "dest.declared")]
struct Confirmation {
    id: u64,
}

/// A reply that declares no destination: the mount site names one.
#[derive(Debug, Deserialize, Outgoing, Serialize, PartialEq, schemars::JsonSchema)]
struct Receipt {
    id: u64,
}

#[subscriber("dest.bare", publish)]
async fn bare(order: &Order) -> Confirmation {
    Confirmation { id: order.id }
}

/// The clause names a subject the reply type contradicts, which the declaration wins.
#[subscriber("dest.contradicted", publish("dest.unused"))]
async fn contradicted(order: &Order) -> Confirmation {
    Confirmation { id: order.id }
}

#[subscriber("dest.named", publish("dest.receipts"))]
async fn named(order: &Order) -> Receipt {
    Receipt { id: order.id }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_reply_type_that_fixes_a_name_is_published_there() {
    let app = RustStream::new(AppInfo::new("reply-destination", "0.1.0")).with_broker(
        MemoryBroker::new(),
        |b| {
            b.include(bare);
        },
    );
    let tb = TestApp::start(app).await.expect("harness start");

    tb.message(&Order { id: 7 })
        .to("dest.bare")
        .publish()
        .await
        .expect("publish");

    tb.broker::<MemoryBroker>()
        .published::<Confirmation>("dest.declared")
        .assert_called_once()
        .with(&Confirmation { id: 7 });
}

/// The mount-site name is a default, so it does not redirect a reply type that fixes one.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_mount_site_name_does_not_redirect_a_fixed_name_reply() {
    let app = RustStream::new(AppInfo::new("reply-destination", "0.1.0")).with_broker(
        MemoryBroker::new(),
        |b| {
            b.include(contradicted).out(Reply, Publish);
        },
    );
    let tb = TestApp::start(app).await.expect("harness start");

    tb.message(&Order { id: 1 })
        .to("dest.contradicted")
        .publish()
        .await
        .expect("publish");

    tb.broker::<MemoryBroker>()
        .published::<Confirmation>("dest.declared")
        .assert_called_once()
        .with(&Confirmation { id: 1 });
    tb.broker::<MemoryBroker>()
        .published::<Confirmation>("dest.unused")
        .assert_not_called();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_reply_type_declaring_no_name_takes_the_mount_site_one() {
    let app = RustStream::new(AppInfo::new("reply-destination", "0.1.0")).with_broker(
        MemoryBroker::new(),
        |b| {
            b.include(named);
        },
    );
    let tb = TestApp::start(app).await.expect("harness start");

    tb.message(&Order { id: 4 })
        .to("dest.named")
        .publish()
        .await
        .expect("publish");

    tb.broker::<MemoryBroker>()
        .published::<Receipt>("dest.receipts")
        .assert_called_once()
        .with(&Receipt { id: 4 });
}

/// The same resolution on the manual chain: `.reply()` alone reads the declaration, and
/// `.to(..)` supplies the default a declaration-free reply type takes.
struct Confirm;

impl Handle<Order, Confirmation> for Confirm {
    fn handle(
        &self,
        order: &Order,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<Confirmation, HandlerOutcome>> {
        ready(Ok(Confirmation { id: order.id }))
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_chain_resolves_the_destination_the_same_way() {
    let app = RustStream::new(AppInfo::new("reply-destination", "0.1.0")).with_broker(
        MemoryBroker::new(),
        |b| {
            b.include(subscriber("chain.bare", Confirm).reply().build());
            b.include(
                subscriber("chain.contradicted", Confirm)
                    .reply()
                    .to("chain.unused")
                    .build(),
            );
        },
    );
    let tb = TestApp::start(app).await.expect("harness start");

    for subject in ["chain.bare", "chain.contradicted"] {
        tb.message(&Order { id: 2 })
            .to(subject)
            .publish()
            .await
            .expect("publish");
    }

    tb.broker::<MemoryBroker>()
        .published::<Confirmation>("dest.declared")
        .assert_called(2);
    tb.broker::<MemoryBroker>()
        .published::<Confirmation>("chain.unused")
        .assert_not_called();
}

/// The generated document reports the destination the reply is actually published to, not the
/// clause's literal.
#[cfg(feature = "asyncapi")]
#[test]
fn the_document_reports_the_resolved_destination() {
    let app = RustStream::new(AppInfo::new("reply-destination", "0.1.0")).with_broker(
        MemoryBroker::new(),
        |b| {
            b.include(contradicted).out(Reply, Publish);
        },
    );

    let spec = ruststream::asyncapi::build_spec(&app);

    assert!(
        spec.channels.contains_key("dest.declared"),
        "the declared channel must be in the document: {:?}",
        spec.channels.keys().collect::<Vec<_>>(),
    );
    assert!(
        !spec.channels.contains_key("dest.unused"),
        "the clause's unused default must not be: {:?}",
        spec.channels.keys().collect::<Vec<_>>(),
    );
}
