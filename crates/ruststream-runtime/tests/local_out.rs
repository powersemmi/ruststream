//! A definition with an `Out` slot on a router of the local family, opened against the local
//! in-memory broker and run in one future: the body publishes through the slot from a future
//! that is not `Send`.

#![cfg(feature = "json")]
// The run holds the bus's `!Send` subscriber on purpose: that is the family under test.
#![allow(clippy::future_not_send)]

use std::convert::Infallible;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;

use futures::StreamExt;
use futures::future::join;
use ruststream_core::{
    BytesMut, CallerName, LocalBroker, LocalIncomingMessage, LocalPublishPolicy, LocalPublisher,
    LocalSubscriber, LocalSubscriptionSource, MessageHeaders, NoHeaders, OutgoingDestination,
    OutgoingMessage,
};
use ruststream_memory::local::{LocalMemoryBroker, LocalMemoryPublish, LocalMemorySource};
use ruststream_runtime::{
    Context, Deserialized, Halt, HandlerOutcome, Input, LocalDetach, LocalHandle, LocalOutEntry,
    OpenRoutes, OutSlot, Outs, PublishedThrough, Reads, Router, RunRoutes, ScopeDelivery,
    ShutdownWait, SoloDeserialized, local_subscriber,
};
use serde::Serialize;
use tokio::sync::Notify;
use tokio::sync::futures::Notified;
use tokio::time::timeout;

/// The app's shutdown: a flag and the waiters it wakes.
#[derive(Default)]
struct Stop {
    flag: AtomicBool,
    notify: Notify,
}

impl Stop {
    fn stop(&self) {
        self.flag.store(true, Ordering::Relaxed);
        self.notify.notify_waiters();
    }
}

impl ShutdownWait for &Stop {
    type Wait<'a>
        = Notified<'a>
    where
        Self: 'a;

    fn is_cancelled(&self) -> bool {
        self.flag.load(Ordering::Relaxed)
    }

    fn cancelled(&self) -> Notified<'_> {
        self.notify.notified()
    }
}

impl Halt for &Stop {
    fn fail(&self, _subscription: &str, _reason: &str) {
        self.stop();
    }

    fn stream_ended(&self, _subscription: &str) {}
}

/// The runner's host: counts the work a delivery left behind, which this scenario leaves none
/// of.
#[derive(Default)]
struct CountDetached(AtomicUsize);

impl LocalDetach for CountDetached {
    fn detach<Work>(&self, _work: Work)
    where
        Work: Future<Output = ()> + 'static,
    {
        self.0.fetch_add(1, Ordering::Relaxed);
    }

    fn detach_after<Work>(&self, _delay: Duration, _work: Work)
    where
        Work: Future<Output = ()> + 'static,
    {
        self.0.fetch_add(1, Ordering::Relaxed);
    }
}

/// An order as its bytes, viewed in place: the decoded-input lane waits on the local message
/// contract, a constructed one reads the payload itself.
struct Order<'a>(&'a [u8]);

impl Deserialized for Order<'_> {
    type Output<'a> = Order<'a>;
    type Error = Infallible;

    fn from_payload(payload: &[u8]) -> Result<Order<'_>, Infallible> {
        Ok(Order(payload))
    }
}

impl Input for Order<'_> {
    type Axis = SoloDeserialized<Order<'static>>;
}

#[derive(Serialize)]
struct Audited {
    id: u64,
}

impl OutgoingDestination for Audited {
    type Form = CallerName;
}

impl MessageHeaders for Audited {
    type Contract = NoHeaders;
}

/// The slot the body publishes its audit trail through.
struct Audit;

impl OutSlot for Audit {
    const NAME: &'static str = "Audit";
    type Destination = Reads;
}

impl PublishedThrough<Audit> for Audited {}

/// Audits every order through its slot, holding a value that is not `Send` across the publish.
struct Mirror;

impl<A> LocalHandle<Order<'_>, (), Outs<(A,)>> for Mirror
where
    A: LocalOutEntry<Audit, Wire: LocalPublisher>,
{
    async fn handle(
        &self,
        order: &Order<'_>,
        outs: &Outs<(A,)>,
        _ctx: &mut Context<'_>,
    ) -> Result<(), HandlerOutcome> {
        let held = Rc::new(order.0.len() as u64);
        let sent = outs
            .get(Audit)
            .message(&Audited { id: *held })
            .to("audit")
            .publish()
            .await;
        sent.map_err(|_| HandlerOutcome::retry())?;
        drop(held);
        Ok(())
    }
}

#[tokio::test]
async fn a_body_publishes_through_its_slot_to_a_local_broker() {
    let connected = LocalMemoryBroker::<4>::new()
        .connect()
        .await
        .expect("the bus connects");
    let mut audits = LocalMemorySource::new("audit")
        .subscribe(&connected)
        .await
        .expect("the audit trail subscribes");
    let opened = Router::<LocalMemoryBroker<4>, _, _, _, _>::local()
        .include(local_subscriber(LocalMemorySource::new("orders"), Mirror).build())
        .out(Audit, LocalMemoryPublish)
        .build()
        .open(&connected, &ScopeDelivery::new())
        .await
        .expect("the route opens");
    let publisher = LocalMemoryPublish
        .pair(&connected)
        .await
        .expect("the publisher pairs");
    let stop = Stop::default();
    let host = CountDetached::default();

    let ((), audited) = join(opened.run(&stop, &Arc::new(()), &host), async {
        let order = OutgoingMessage::with_payload("orders", BytesMut::from(&b"seven"[..]));
        publisher
            .publish(order, None)
            .await
            .expect("the bus takes it");
        // A guard, not a wait: the audit is published within the same few polls, and the
        // timeout only ends a run whose publish never left.
        let audited = timeout(Duration::from_secs(5), audits.stream().next())
            .await
            .ok()
            .flatten();
        stop.stop();
        audited
    })
    .await;

    let audited = audited.expect("an audit").expect("a delivery");
    assert_eq!(LocalIncomingMessage::payload(&audited), br#"{"id":5}"#);
    assert_eq!(
        host.0.load(Ordering::Relaxed),
        0,
        "work detached by the delivery"
    );
}
