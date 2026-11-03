//! The buffer a publish to a transport that only reads the payload encodes into: it is the
//! thread's own, reused by the next publish, and never shared between two publishes in flight.

// A test drives the publish on its own thread; the cold path carries no Send.
#![allow(clippy::future_not_send)]
#![cfg(feature = "json")]

use std::convert::Infallible;
use std::future::ready;
use std::io;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use allocation_counter::measure;
use futures::future::join;
use ruststream::codec::{Codec, JsonCodec};
use ruststream::runtime::PublishExt;
use ruststream::{
    Lend, Outgoing, OutgoingFor, OutgoingMessage, PublishMessageError, PublishOutcome, Publisher,
    Transaction, TransactionalPublisher,
};
use ruststream_core::OwnedTransactions;
use serde::Serialize;
use tokio::runtime::{Builder, Runtime};
use tokio::task::yield_now;

/// A runtime on this thread alone, so a count taken while it runs holds every task it polls.
fn runtime() -> Runtime {
    Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("a current-thread runtime builds")
}

/// A message under a destination of its own, with a body of any size.
#[derive(Outgoing, Serialize)]
#[outgoing(name = "orders.created")]
struct Order {
    id: u64,
    note: String,
}

impl Order {
    fn new(id: u64, note_len: usize) -> Self {
        Self {
            id,
            note: "n".repeat(note_len),
        }
    }

    /// The bytes a publish of this message puts on the wire.
    fn wire(&self) -> Vec<u8> {
        JsonCodec.encode(self).expect("an order encodes").to_vec()
    }
}

/// What a recording transport saw, in order.
type Log = Arc<Mutex<Vec<Vec<u8>>>>;

/// A transport that reads the payload and keeps a copy of it, once the publish has been in flight
/// across a yield: a buffer shared with another publish would change under it.
#[derive(Clone, Default)]
struct Recorder {
    log: Log,
    refuse: Arc<AtomicBool>,
}

impl Recorder {
    fn log(&self) -> Vec<Vec<u8>> {
        self.log.lock().expect("recorder log").clone()
    }
}

impl Publisher for Recorder {
    type Payload = Lend;
    type Error = io::Error;
    type Options = ();

    fn publish<'msg>(
        &self,
        msg: OutgoingMessage<'msg, &'msg [u8]>,
        _options: Option<&()>,
    ) -> impl Future<Output = PublishOutcome<'msg, io::Error, Lend>> {
        let log = Arc::clone(&self.log);
        let refuse = self.refuse.swap(false, Ordering::SeqCst);
        async move {
            yield_now().await;
            if refuse {
                return Err(PublishMessageError::with_message(
                    io::Error::other("refused"),
                    msg,
                ));
            }
            log.lock()
                .expect("recorder log")
                .push(msg.payload().to_vec());
            Ok(())
        }
    }
}

/// A transport that reads the payload and keeps nothing, so a count around a publish holds the
/// framework's allocations alone. It also stands for a transactional one.
#[derive(Clone, Default)]
struct Quiet(Arc<AtomicUsize>);

impl Quiet {
    fn bytes(&self) -> usize {
        self.0.load(Ordering::Relaxed)
    }
}

impl Publisher for Quiet {
    type Payload = Lend;
    type Error = Infallible;
    type Options = ();

    fn publish<'msg>(
        &self,
        msg: OutgoingMessage<'msg, &'msg [u8]>,
        _options: Option<&()>,
    ) -> impl Future<Output = PublishOutcome<'msg, Infallible, Lend>> {
        self.0.fetch_add(msg.payload().len(), Ordering::Relaxed);
        ready(Ok(()))
    }
}

impl TransactionalPublisher for Quiet {
    fn begin_transaction(&self) -> impl Future<Output = Result<(), Infallible>> {
        ready(Ok(()))
    }

    fn commit(&self) -> impl Future<Output = Result<(), Infallible>> {
        ready(Ok(()))
    }

    fn abort(&self) -> impl Future<Output = Result<(), Infallible>> {
        ready(Ok(()))
    }
}

/// An owned transaction over [`Quiet`]'s counter.
struct QuietTxn(Quiet);

impl Transaction for QuietTxn {
    type Payload = Lend;
    type Error = Infallible;
    type Options = ();

    fn publish<'msg>(
        &mut self,
        msg: OutgoingFor<'msg, Lend>,
        options: Option<&()>,
    ) -> impl Future<Output = PublishOutcome<'msg, Infallible, Lend>> {
        self.0.publish(msg, options)
    }

    fn commit(self) -> impl Future<Output = Result<(), Infallible>> {
        ready(Ok(()))
    }

    fn abort(self) -> impl Future<Output = Result<(), Infallible>> {
        ready(Ok(()))
    }
}

impl OwnedTransactions for Quiet {
    type Transaction = QuietTxn;

    fn transaction(&self) -> impl Future<Output = Result<QuietTxn, Infallible>> {
        ready(Ok(QuietTxn(self.clone())))
    }
}

/// Runs `work` to completion on `runtime` and answers how many allocations this thread made
/// meanwhile.
fn allocations_of<Work: Future>(runtime: &Runtime, work: Work) -> (Work::Output, u64) {
    let mut output = None;
    let counted = measure(|| output = Some(runtime.block_on(work)));
    (
        output.expect("the measured closure ran the work"),
        counted.count_total,
    )
}

#[test]
fn a_publish_to_a_reading_transport_reuses_the_threads_buffer() {
    let runtime = runtime();
    let quiet = Quiet::default();
    let order = Order::new(1, 64);
    // The first publish gives the thread its buffer; what follows is the steady state.
    runtime
        .block_on(quiet.message(&order).publish())
        .expect("the transport never refuses");
    runtime.block_on(async {});

    let (outcome, spent) = allocations_of(&runtime, quiet.message(&order).publish());

    outcome.expect("the transport never refuses");
    assert_eq!(
        spent, 0,
        "the second publish encodes into the buffer the first left"
    );
    assert_eq!(quiet.bytes(), 2 * order.wire().len());
}

#[test]
fn a_publish_in_a_borrowed_transaction_reuses_the_threads_buffer() {
    let runtime = runtime();
    let quiet = Quiet::default();
    let order = Order::new(1, 64);
    let mut scope = runtime.block_on(async {
        let mut scope = quiet.begin().await.expect("begin");
        scope.publish("orders", &order).await.expect("warm up");
        scope
    });

    let (outcome, spent) = allocations_of(&runtime, scope.publish("orders", &order));

    outcome.expect("the scope takes the message");
    assert_eq!(
        spent, 0,
        "the scope encodes into the buffer the first publish left"
    );
    runtime.block_on(scope.commit()).expect("commit");
}

#[test]
fn a_publish_in_an_owned_transaction_reuses_the_threads_buffer() {
    let runtime = runtime();
    let quiet = Quiet::default();
    let order = Order::new(1, 64);
    let mut txn = runtime.block_on(async {
        let mut txn = quiet.owned_transaction().await.expect("transaction");
        txn.publish("orders", &order).await.expect("warm up");
        txn
    });

    let (outcome, spent) = allocations_of(&runtime, txn.publish("orders", &order));

    outcome.expect("the transaction takes the message");
    assert_eq!(
        spent, 0,
        "the transaction encodes into the buffer the first publish left"
    );
    runtime.block_on(txn.commit()).expect("commit");
}

#[tokio::test]
async fn a_message_never_carries_the_bytes_of_the_one_before_it() {
    let recorder = Recorder::default();
    let (large, small) = (Order::new(1, 400), Order::new(2, 3));

    for order in [&large, &small, &large] {
        recorder
            .message(order)
            .publish()
            .await
            .expect("the recorder takes the message");
    }

    assert_eq!(recorder.log(), [large.wire(), small.wire(), large.wire()]);
}

#[tokio::test]
async fn two_publishes_in_flight_on_one_thread_keep_their_own_bytes() {
    let recorder = Recorder::default();
    let (first, second) = (Order::new(1, 300), Order::new(2, 5));

    let (a, b) = join(
        recorder.message(&first).publish(),
        recorder.message(&second).publish(),
    )
    .await;
    a.expect("the first publish");
    b.expect("the second publish");

    let mut seen = recorder.log();
    seen.sort();
    let mut expected = vec![first.wire(), second.wire()];
    expected.sort();
    assert_eq!(seen, expected);
}

#[tokio::test]
async fn a_refused_publish_returns_its_message_and_the_next_publish_is_intact() {
    let recorder = Recorder::default();
    let kept = Recorder::default();
    let order = Order::new(7, 200);

    recorder.refuse.store(true, Ordering::SeqCst);
    let refused = recorder
        .message(&order)
        .publish()
        .await
        .expect_err("the recorder refuses once");
    let unsent = refused.unsent().expect("the refusal returns the message");
    kept.message(unsent)
        .to("elsewhere")
        .publish()
        .await
        .expect("the other transport takes it");
    let next = Order::new(8, 50);
    recorder
        .message(&next)
        .publish()
        .await
        .expect("the publish after a refusal works");

    assert_eq!(
        kept.log(),
        [order.wire()],
        "the message comes back as it was"
    );
    assert_eq!(recorder.log(), [next.wire()]);
}
