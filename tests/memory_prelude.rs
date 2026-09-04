//! The two vocabularies a service writes, pinned on the in-memory broker: a handler body names
//! broker capability traits and never a policy, a mount site names policies under the uniform
//! names the broker's prelude provides, and the names do not collide - `Publish` is the policy
//! type, `Publisher` the core trait. One glob carries both, because the broker prelude
//! re-exports the core one.
#![cfg(all(
    feature = "memory",
    feature = "json",
    feature = "macros",
    feature = "testing"
))]

use ruststream::memory::prelude::*;
use ruststream::testing::TestApp;
use serde::{Deserialize, Serialize};

/// The input, declared with no name of its own so the test names the subscription per publish.
#[derive(Debug, Outgoing, Serialize, Deserialize, schemars::JsonSchema)]
struct Order {
    id: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Outgoing, Serialize, Deserialize)]
#[outgoing(name = "orders.settled")]
struct Settled {
    id: u64,
}

#[derive(OutSlot)]
#[publishes(Settled)]
struct Journal;

#[derive(OutSlot)]
#[publishes(Settled)]
struct Audit;

/// The body's whole vocabulary comes from the core prelude the broker prelude re-exports: it
/// bounds each slot with the broker capability it drives, and names no policy and no broker
/// type, so the same body mounts on any broker offering those capabilities.
#[ruststream::subscriber("orders")]
async fn settle(
    order: &Order,
    Out(journal): Out<impl TransactionalPublisher, Journal, Settled>,
    Out(audit): Out<impl Publisher, Audit, Settled>,
) -> HandlerOutcome {
    let Ok(scope) = journal.begin().await else {
        return HandlerOutcome::retry();
    };
    if scope
        .message(&Settled { id: order.id })
        .publish()
        .await
        .is_err()
        || scope.commit().await.is_err()
        || audit
            .message(&Settled { id: order.id })
            .publish()
            .await
            .is_err()
    {
        return HandlerOutcome::retry();
    }
    HandlerOutcome::ack()
}

/// The mount site's whole vocabulary is the uniform policy names, so swapping brokers swaps the
/// glob and leaves this chain as it reads.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_mount_site_names_policies_under_the_uniform_names() {
    let app =
        RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
            b.include(settle)
                .out(Journal, TransactionalPublish)
                .out(Audit, Publish)
                .build();
        });
    let tb = TestApp::start(app).await.expect("harness start");

    tb.message(&Order { id: 7 })
        .to("orders")
        .publish()
        .await
        .expect("publish");
    tb.broker::<MemoryBroker>()
        .subscriber("orders")
        .assert_called_once()
        .settled(HandlerOutcome::ack());

    tb.out::<Journal>().assert_called_once();
    tb.out::<Audit>().assert_called_once();
}

/// The core trait a body bounds with, taken through the broker's live publisher.
fn publisher_is_the_core_trait<T: Publisher>() {}

/// The policy names are types with values; the capability names next to them stay traits. The
/// in-memory policies are unit structs, so a mount site writes the bare name; a broker whose
/// policy carries options names it the same way and constructs it its own way
/// (`Publish::new(..)`, `Publish::default()`).
#[test]
fn the_policy_names_are_types_and_the_capability_names_are_traits() {
    let _: Publish = Publish;
    let _: TransactionalPublish = TransactionalPublish;
    let _: Request = Request;
    publisher_is_the_core_trait::<ruststream::memory::MemoryPublisher>();
    publisher_is_the_core_trait::<ruststream::memory::MemoryRequester>();
}
