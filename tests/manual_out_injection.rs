//! The macro-free counterpart of `tests/out_injection.rs`: a handler that receives a live
//! publisher as an injected parameter, written out as a definition.
//!
//! An `Out` parameter is the one place the attribute splits the declaration in two: the unit
//! struct the include site names carries the slot list (`HasSlots`) and the instantiation
//! (`BindSlots`), while the definition proper lands on a publisher-generic type, so the
//! attachment at the include site decides the publisher type with nothing erased.
#![cfg(all(feature = "memory", feature = "json", feature = "testing"))]

mod common;

use std::marker::PhantomData;

use common::{Event, connected, expect_id};

use ruststream::codec::Codec;
use ruststream::memory::{MemoryBroker, MemoryPublish};
use ruststream::prelude::*;
use ruststream::runtime::{
    AllOpen, BindSlots, Declared, Decoded, DefaultSlot, HasSlots, InjectCall, InjectDef,
    OutgoingMessageMetadata, PublishExt, Settle, SlotPublisher, SubscriberBuilder, forms,
};
use ruststream::{Broker, ConnectedBroker};

/// The whole content of a definition generic over its slot publishers: the inferred types and
/// nothing else, so the definition stays a zero-sized value the mount site builds for free.
type SlotTypes<T> = PhantomData<fn() -> T>;

/// The definition value the include site names: it carries the slot list and nothing else, so it
/// stays a zero-sized value the mount builds for free.
struct Crossing;

impl Declared for Crossing {
    type Form = forms::Out;
    type Settings = SubscriberBuilder<Self, Name, AllOpen>;

    fn declare(self) -> Self::Settings {
        SubscriberBuilder::new(self, Name::new("out.crossing"))
    }
}

impl HasSlots for Crossing {
    type Markers = (DefaultSlot,);
}

impl<Conn, Enc, Policy> BindSlots<Conn, ((Policy, Enc),)> for Crossing
where
    Conn: ConnectedBroker,
    Policy: PublishPolicy<Conn>,
{
    type Bound = CrossingDef<SlotPublisher<Policy::Live, DefaultSlot>, Enc>;
    type Extra = ((Policy, Enc),);

    fn bind(self, sources: ((Policy, Enc),)) -> (Self::Bound, Self::Extra) {
        (CrossingDef(PhantomData), sources)
    }
}

/// The definition the slot publisher and the scope codec are threaded into. It is never
/// constructed with a value in it: the injected publisher reaches the body through the
/// injections tuple, and the generics only pin its type.
struct CrossingDef<Egress, Enc>(SlotTypes<(Egress, Enc)>);

impl<Egress, Enc> InjectDef for CrossingDef<Egress, Enc>
where
    Egress: Publisher + Send + Sync + 'static,
    Enc: Codec + Send + Sync + 'static,
{
    type Input = Decoded<Event>;
    type Context = ();
    type Source = Name;
    type Injections = (Out<Egress, DefaultSlot, (), Enc>,);

    fn source(&self) -> Self::Source {
        Name::new("out.crossing")
    }

    fn outgoing(&self) -> Vec<OutgoingMessageMetadata> {
        // An unrestricted slot declares its marker's whole dictionary; the implicit one has none.
        <DefaultSlot as OutSlot>::outgoing()
    }
}

impl<State, Egress, Enc> InjectCall<State> for CrossingDef<Egress, Enc>
where
    State: Send + Sync,
    Egress: Publisher + Send + Sync + 'static,
    Enc: Codec + Send + Sync + 'static,
{
    async fn call(
        &self,
        event: &Event,
        injections: &Self::Injections,
        _ctx: &mut Context<'_, (), State>,
    ) -> Settle {
        let Out(out) = &injections.0;
        let payload = serde_json::to_vec(event).expect("serializable");
        if out.raw(&payload).to("out.other").publish().await.is_err() {
            return HandlerResult::retry().into();
        }
        HandlerResult::Ack.into()
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
    let to_other = other.bind(MemoryPublish);
    let app = RustStream::new(AppInfo::new("cross", "0.1.0"))
        .with_broker(other, |b| {
            let _ = b; // the target broker may mount its own handlers here
        })
        .with_broker(ingress_broker, |b| {
            b.include(Crossing).publisher(to_other);
        });
    // --8<-- [end:cross_broker]
    let running = app.start().await.expect("startup failed");

    ingress
        .raw(&serde_json::to_vec(&Event { id: 9 }).unwrap())
        .to("out.crossing")
        .publish()
        .await
        .expect("publish");
    expect_id(&observer, "out.other", 9).await;

    running.shutdown().await.expect("graceful shutdown failed");
}
