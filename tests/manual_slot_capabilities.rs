//! The broker capabilities driven through an arena entry, pinned on the manual path: a body
//! bounds the entry's wired value with the capability it needs (`TransactionalPublisher`,
//! `OwnedTransactions`, `RequestReply`) and drives that capability's typed form through the
//! entry against the in-memory broker, which carries all three natively. No broker type appears
//! in any body.
#![cfg(all(feature = "memory", feature = "json", feature = "testing"))]

use std::time::Duration;

use ruststream::codec::Codec;
use ruststream::memory::{MemoryBroker, MemoryPublish, MemoryRequest};
use ruststream::prelude::*;
use ruststream::runtime::PublishedThrough;
use ruststream::testing::TestApp;
use ruststream::{
    CallerName, FixedName, MessageHeaders, NoHeaders, OutgoingDestination, OutgoingMessage,
};
use serde::{Deserialize, Serialize};

/// The input every body here takes; it declares no name, so the test names one per publish.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
struct Order {
    id: u64,
}

impl OutgoingDestination for Order {
    type Form = CallerName;
}

impl MessageHeaders for Order {
    type Contract = NoHeaders;
}

/// A record with a declared destination, what `#[derive(Outgoing)] #[outgoing(name = ..)]`
/// writes: the publish resolves the name from the type.
macro_rules! record {
    ($name:ident, $address:literal) => {
        #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
        struct $name {
            id: u64,
        }

        impl OutgoingDestination for $name {
            type Form = FixedName;
            const ADDRESS: &'static str = $address;
        }

        impl MessageHeaders for $name {
            type Contract = NoHeaders;
        }
    };
}

record!(Settled, "ledger.settled");
record!(Audit, "ledger.audit");
record!(Quoted, "quotes.settled");

// --- the borrowed kind: a scope on the entry, settled atomically ---

struct Journal;

impl OutSlot for Journal {
    const NAME: &'static str = "Journal";
}

impl PublishedThrough<Journal> for Settled {}
impl PublishedThrough<Journal> for Audit {}

/// Settles an order inside one broker transaction opened on the entry: both records become
/// visible together on commit, or not at all when the body aborts.
struct SettleAtomically;

impl<W, E> Handle<Order, (), Outs<(Slot<Journal, W, E>,)>> for SettleAtomically
where
    W: TransactionalPublisher,
    E: Codec + Send + Sync,
{
    async fn handle(
        &self,
        order: &Order,
        outs: &Outs<(Slot<Journal, W, E>,)>,
        _ctx: &mut Context<'_>,
    ) -> Result<(), HandlerOutcome> {
        let Ok(scope) = outs.get(Journal).begin().await else {
            return Err(HandlerOutcome::retry());
        };
        if scope
            .message(&Settled { id: order.id })
            .publish()
            .await
            .is_err()
            || scope
                .message(&Audit { id: order.id })
                .publish()
                .await
                .is_err()
        {
            return Err(HandlerOutcome::retry());
        }
        if order.id == 0 {
            // An order that turns out invalid after the records were staged: nothing leaves.
            if scope.abort().await.is_err() {
                return Err(HandlerOutcome::retry());
            }
            return Err(HandlerOutcome::drop());
        }
        if scope.commit().await.is_err() {
            return Err(HandlerOutcome::retry());
        }
        Ok(())
    }
}

/// A committed scope fans both records out and keeps the slot's attribution; an aborted one
/// leaves nothing behind.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_transactional_entry_settles_its_scope_atomically() {
    let app =
        RustStream::new(AppInfo::new("ledger", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
            b.include(subscriber("ledger.orders", SettleAtomically).build())
                .out(Journal, MemoryPublish)
                .build();
        });
    let tb = TestApp::start(app).await.expect("harness start");
    let broker = tb.broker::<MemoryBroker>();

    tb.message(&Order { id: 7 })
        .to("ledger.orders")
        .publish()
        .await
        .expect("publish");
    broker
        .subscriber("ledger.orders")
        .assert_called_once()
        .settled(HandlerOutcome::ack());

    broker
        .published::<Settled>("ledger.settled")
        .assert_called_once()
        .with(&Settled { id: 7 });
    broker
        .published::<Audit>("ledger.audit")
        .assert_called_once()
        .with(&Audit { id: 7 });
    // The scope publishes through the entry's attributed publisher, so the slot sees both.
    assert_eq!(tb.out::<Journal>().messages().len(), 2);

    tb.message(&Order { id: 0 })
        .to("ledger.orders")
        .publish()
        .await
        .expect("publish");
    broker
        .subscriber("ledger.orders")
        .assert_called(2)
        .settled(HandlerOutcome::drop());

    // Aborted: the staged records never reached the broker.
    broker
        .published::<Settled>("ledger.settled")
        .assert_called_once();
    broker
        .published::<Audit>("ledger.audit")
        .assert_called_once();
}

// --- the owned kind: an independent transaction value, buffered outside the slot ---

struct Ledger;

impl OutSlot for Ledger {
    const NAME: &'static str = "Ledger";
}

impl PublishedThrough<Ledger> for Settled {}

/// Settles an order through an owned transaction opened on the entry.
struct SettleOwned;

impl<W, E> Handle<Order, (), Outs<(Slot<Ledger, W, E>,)>> for SettleOwned
where
    W: OwnedTransactions,
    E: Codec + Send + Sync,
{
    async fn handle(
        &self,
        order: &Order,
        outs: &Outs<(Slot<Ledger, W, E>,)>,
        _ctx: &mut Context<'_>,
    ) -> Result<(), HandlerOutcome> {
        let Ok(mut txn) = outs.get(Ledger).transaction().await else {
            return Err(HandlerOutcome::retry());
        };
        if txn
            .message(&Settled { id: order.id })
            .publish()
            .await
            .is_err()
            || txn.commit().await.is_err()
        {
            return Err(HandlerOutcome::retry());
        }
        Ok(())
    }
}

/// The owned transaction's buffer settles outside the slot: the record lands in the broker's
/// publish log and is not attributed to the slot (the documented capture boundary).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_owned_transactional_entry_commits_its_buffer() {
    let app = RustStream::new(AppInfo::new("ledger-owned", "0.1.0")).with_broker(
        MemoryBroker::new(),
        |b| {
            b.include(subscriber("ledger.orders", SettleOwned).build())
                .out(Ledger, MemoryPublish)
                .build();
        },
    );
    let tb = TestApp::start(app).await.expect("harness start");

    tb.message(&Order { id: 4 })
        .to("ledger.orders")
        .publish()
        .await
        .expect("publish");
    tb.settle().await.expect("settle");

    tb.broker::<MemoryBroker>()
        .published::<Settled>("ledger.settled")
        .assert_called_once()
        .with(&Settled { id: 4 });
    tb.out::<Ledger>().assert_not_called();
}

// --- request / reply: a correlated round trip through the entry ---

/// What the requester sends; the responder decodes it as its input.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
struct Ask {
    id: u64,
}

/// What the responder answers with, to the inbox the request names; it declares no name of its
/// own because the destination is only known per request.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct Answer {
    price: u64,
}

impl OutgoingDestination for Answer {
    type Form = CallerName;
}

impl MessageHeaders for Answer {
    type Contract = NoHeaders;
}

struct Answers;

impl OutSlot for Answers {
    const NAME: &'static str = "Answers";
}

impl PublishedThrough<Answers> for Answer {}

/// The responder: a plain publishing body that answers where the request's `reply-to` points.
struct Respond;

impl<W, E> Handle<Ask, (), Outs<(Slot<Answers, W, E>,)>> for Respond
where
    W: Publisher,
    E: Codec + Send + Sync,
{
    async fn handle(
        &self,
        ask: &Ask,
        outs: &Outs<(Slot<Answers, W, E>,)>,
        ctx: &mut Context<'_>,
    ) -> Result<(), HandlerOutcome> {
        let Some(reply_to) = ctx.headers().reply_to().map(str::to_owned) else {
            return Err(HandlerOutcome::drop());
        };
        if outs
            .get(Answers)
            .message(&Answer { price: ask.id * 10 })
            .to(reply_to)
            .publish()
            .await
            .is_err()
        {
            return Err(HandlerOutcome::retry());
        }
        Ok(())
    }
}

struct Quotes;

impl OutSlot for Quotes {
    const NAME: &'static str = "Quotes";
}

impl PublishedThrough<Quotes> for Quoted {}

/// The requester: asks for a quote through the entry and publishes the answered price through
/// the same entry - the plain builder comes with the `Publisher` supertrait of the bound.
struct AskQuote;

impl<W, E> Handle<Order, (), Outs<(Slot<Quotes, W, E>,)>> for AskQuote
where
    W: RequestReply,
    E: Codec + Send + Sync,
{
    async fn handle(
        &self,
        order: &Order,
        outs: &Outs<(Slot<Quotes, W, E>,)>,
        _ctx: &mut Context<'_>,
    ) -> Result<(), HandlerOutcome> {
        let ask = serde_json::to_vec(&Ask { id: order.id }).expect("an ask serializes");
        let Ok(reply) = outs
            .get(Quotes)
            .request(
                OutgoingMessage::new("quotes.ask", &ask),
                Duration::from_secs(2),
            )
            .await
        else {
            return Err(HandlerOutcome::retry());
        };
        let Ok(answer) = serde_json::from_slice::<Answer>(reply.payload()) else {
            return Err(HandlerOutcome::drop());
        };
        if outs
            .get(Quotes)
            .message(&Quoted {
                id: order.id + answer.price,
            })
            .publish()
            .await
            .is_err()
        {
            return Err(HandlerOutcome::retry());
        }
        Ok(())
    }
}

/// The request correlates with the responder's answer, and both the request and the follow-up
/// publish are attributed to the requesting slot.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_request_reply_entry_correlates_its_reply() {
    let app =
        RustStream::new(AppInfo::new("quotes", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
            b.include(subscriber("quotes.ask", Respond).build())
                .out(Answers, MemoryPublish)
                .build();
            b.include(subscriber("quotes.orders", AskQuote).build())
                .out(Quotes, MemoryRequest)
                .build();
        });
    let tb = TestApp::start(app).await.expect("harness start");
    let broker = tb.broker::<MemoryBroker>();

    tb.message(&Order { id: 4 })
        .to("quotes.orders")
        .publish()
        .await
        .expect("publish");
    broker
        .subscriber("quotes.orders")
        .assert_called_once()
        .settled(HandlerOutcome::ack());

    broker
        .published::<Quoted>("quotes.settled")
        .assert_called_once()
        .with(&Quoted { id: 44 });
    tb.out::<Answers>().assert_called_once();
    // The request itself and the quoted price both left through the requesting entry.
    assert_eq!(tb.out::<Quotes>().messages().len(), 2);
}
