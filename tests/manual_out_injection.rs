//! The macro-free counterpart of `tests/out_injection.rs`: a handler that receives a live
//! publisher as an injected parameter, written out as a definition.
//!
//! An injected publisher reaches the body through the arena: the `Handle` impl names one `Slot`
//! per marker, and the publisher type stays generic, so the attachment at the include site decides
//! it with nothing erased.
#![cfg(all(feature = "memory", feature = "json", feature = "testing"))]

mod common;

use common::{connected, expect_id};

use ruststream::codec::Codec;
use ruststream::memory::prelude::*;
use serde::{Deserialize, Serialize};

/// What crosses the two brokers. Declared here rather than taken from `common`, whose own
/// declaration rides the `Outgoing` derive: the point of this file is that the publish side
/// stands with the attribute off.
#[derive(Serialize, Deserialize, schemars::JsonSchema)]
struct Event {
    id: u64,
}

// The `Outgoing` derive by hand: no declared name, so each call site names its destination.
impl OutgoingDestination for Event {
    type Form = CallerName;
}

impl MessageHeaders for Event {
    type Contract = NoHeaders;
}

/// The handler body over its injected publisher. The slot's publisher type is a parameter of the
/// impl, so the definition stays a zero-sized value the mount site builds for free.
struct Crossing;

impl<Egress, Enc> Handle<Event, (), Outs<(Slot<DefaultSlot, Egress, Enc>,)>> for Crossing
where
    Egress: Publisher,
    Enc: Codec + Send + Sync,
{
    async fn handle(
        &self,
        event: &Event,
        outs: &Outs<(Slot<DefaultSlot, Egress, Enc>,)>,
        _ctx: &mut Context<'_>,
    ) -> Result<(), HandlerOutcome> {
        if outs
            .get(DefaultSlot)
            .message(event)
            .to("out.other")
            .publish()
            .await
            .is_err()
        {
            return Err(HandlerOutcome::retry());
        }
        Ok(())
    }
}

/// The cross-broker case: the handler consumes one broker and its injected publisher targets
/// another, through a token minted by the target broker's scope.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_bound_token_injects_a_foreign_brokers_publisher() {
    let ingress_broker = MemoryBroker::new();
    let ingress = ingress_broker.publisher();
    let other = MemoryBroker::new().bindable();
    let observer = connected(other.broker()).await;

    // --8<-- [start:cross_broker]
    let to_other = other.bind(Publish);
    let app = RustStream::new(AppInfo::new("cross", "0.1.0"))
        .with_broker(other, |b| {
            let _ = b; // the target broker may mount its own handlers here
        })
        .with_broker(ingress_broker, |b| {
            b.include(subscriber("out.crossing", Crossing).build())
                .publisher(to_other);
        });
    // --8<-- [end:cross_broker]
    let running = app.start().await.expect("startup failed");

    ingress
        .message(&Event { id: 9 })
        .to("out.crossing")
        .publish()
        .await
        .expect("publish");
    expect_id(&observer, "out.other", 9).await;

    running.shutdown().await.expect("graceful shutdown failed");
}
