//! Out injection: a handler receives a live publisher as a parameter, paired by the runtime
//! from the source attached at the include site.
#![cfg(all(
    feature = "memory",
    feature = "macros",
    feature = "json",
    feature = "testing"
))]

mod common;

use common::{Event, Wire};

use ruststream::memory::prelude::*;
use ruststream::testing::{Outcome, TestApp};

/// The destination is computed per message: exactly the case reply publishing cannot cover and
/// the injected publisher exists for.
#[subscriber("out.in")]
async fn forward(event: &Event, Out(out): Out<impl Publisher>) -> HandlerOutcome {
    let dest = if event.id.is_multiple_of(2) {
        "out.even"
    } else {
        "out.odd"
    };
    if out.message(event).to(dest).publish().await.is_err() {
        return HandlerOutcome::retry();
    }
    HandlerOutcome::ack()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn decode_failures_are_recorded_for_out_handlers() {
    let app =
        RustStream::new(AppInfo::new("egress", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
            b.include(forward).out(DefaultSlot, Publish).build();
        });
    let tb = TestApp::start(app).await.expect("harness start");

    // Not valid JSON for `Event`: the Out wrapper fails to decode, the handler never runs, and
    // the harness must classify the delivery as a decode failure, exactly like the typed path.
    tb.broker::<MemoryBroker>()
        .message(&Wire::of(b"not json"))
        .to("out.in")
        .publish()
        .await
        .expect("publish");

    tb.broker::<MemoryBroker>()
        .subscriber("out.in")
        .assert_called_once()
        .assert_outcome(Outcome::DecodeFailed)
        .assert_last_failed_to_decode();
}

/// The destination is computed per element, off the whole batch: exactly what a reply form
/// cannot express and the injected publisher can - batch and Out compose.
#[subscriber("out.batch")]
async fn forward_batch(events: &[Event], Out(out): Out<impl Publisher>) -> HandlerOutcome {
    for event in events {
        if out
            .message(event)
            .to("out.batched")
            .publish()
            .await
            .is_err()
        {
            return HandlerOutcome::retry();
        }
    }
    HandlerOutcome::ack()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_batch_handler_composes_with_an_out_parameter() {
    let app =
        RustStream::new(AppInfo::new("out-batch", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
            b.include(forward_batch.batch(nonzero!(8)))
                .out(DefaultSlot, Publish)
                .build();
        });
    let tb = TestApp::start(app).await.expect("harness start");

    for id in [4u64, 5u64] {
        tb.message(&Event { id })
            .to("out.batch")
            .publish()
            .await
            .expect("publish");
    }

    let forwarded = tb.out::<DefaultSlot>().assert_called(2);
    assert_eq!(
        forwarded.decoded_as::<Event>().decoded(),
        [Event { id: 4 }, Event { id: 5 }],
        "forwards in delivery order",
    );
}
