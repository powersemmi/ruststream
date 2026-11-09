//! What a part of a subscription does when it panics outside the handler: the dispatch loop, a
//! worker of its pool and a dedicated thread each fail the service through the fail-fast path,
//! naming the subscription and the part. A panic inside the handler is the handler's: dispatch
//! catches it and settles the delivery under the subscription's failure policy.

use std::fmt::{self, Display, Formatter};
use std::sync::Arc;
use std::thread;

use crate::shutdown::Shutdown;

/// A part of a subscription the runner runs, named the way its failure reads.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Part {
    /// The loop that reads the subscription's stream.
    Loop,
    /// A worker of its pool, a task of the app's runtime.
    Worker,
    /// One of its dedicated threads, by index.
    Thread(usize),
}

impl Part {
    /// Fails the service through the fail-fast path: this part of `subscription` panicked, so the
    /// subscription no longer consumes the way it was declared.
    pub(crate) fn panicked(self, shutdown: &Shutdown, subscription: &str) {
        shutdown.signal(subscription, &format!("{self} panicked"));
    }
}

impl Display for Part {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Loop => f.write_str("its dispatch loop"),
            Self::Worker => f.write_str("a worker"),
            Self::Thread(index) => write!(f, "its dedicated thread {index}"),
        }
    }
}

/// Fails the service when the loop or the worker that holds it panics.
///
/// The part holds it for as long as it runs, and nothing touches it per delivery. tokio drops a
/// task's future inside the guard that catches its panic, so a part that panicked drops its watch
/// while its thread unwinds; an end, a shutdown and an abort drop it on a thread that does not.
/// A runtime dropped while its own thread unwinds drops the parts it still runs on that thread,
/// and they read as panicked too: the process is going down on that panic.
#[derive(Debug)]
pub(crate) struct Watch {
    shutdown: Shutdown,
    subscription: Arc<str>,
    part: Part,
}

impl Watch {
    /// A watch over `part` of `subscription`, failing the service `shutdown` stops.
    #[must_use]
    pub(crate) fn new(shutdown: &Shutdown, subscription: &Arc<str>, part: Part) -> Self {
        Self {
            shutdown: shutdown.clone(),
            subscription: Arc::clone(subscription),
            part,
        }
    }
}

impl Drop for Watch {
    fn drop(&mut self) {
        if thread::panicking() {
            self.part.panicked(&self.shutdown, &self.subscription);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::future::ready;
    use std::num::NonZeroUsize;
    use std::sync::Arc;
    use std::task::Poll;
    use std::time::Duration;

    use bytes::Bytes;
    use futures::{Stream, StreamExt, stream};
    use ruststream_core::BatchStream;
    use tokio::task::JoinHandle;
    use tokio::time::timeout;

    use super::super::fixtures::{PlainMessage, Settlement, StreamFault, Unscoped, batched};
    use super::super::{spawn_batch_dispatch, spawn_dispatch_workers};
    use crate::detach::Hosted;
    use crate::shutdown::Shutdown;
    use crate::{
        BatchHandler, BatchSubscriber, Context, DispatchFailure, FailurePolicies, Handler,
        HandlerOutcome, HeaderMap, SendHost, Subscriber, Workers,
    };

    /// A subscription whose stream panics the first time its loop reads it: a client library
    /// that broke under the loop.
    struct BrokenStream;

    impl Subscriber for BrokenStream {
        type Message = PlainMessage;
        type Error = StreamFault;

        fn stream(&mut self) -> impl Stream<Item = Result<PlainMessage, StreamFault>> + Send + '_ {
            stream::poll_fn(|_| -> Poll<Option<Result<PlainMessage, StreamFault>>> {
                panic!("the client library broke")
            })
        }
    }

    impl BatchSubscriber for BrokenStream {
        type Batch = Vec<PlainMessage>;

        fn batches(
            &mut self,
            size: NonZeroUsize,
        ) -> impl BatchStream<Vec<PlainMessage>, Error = StreamFault> + Send + '_ {
            batched(self.stream(), size)
        }
    }

    /// One delivery whose settle panics on the part of the subscription that took it, then a
    /// subscription that waits for more: nothing but the failure stops the service.
    struct BrokenSettle;

    impl Subscriber for BrokenSettle {
        type Message = PlainMessage;
        type Error = StreamFault;

        fn stream(&mut self) -> impl Stream<Item = Result<PlainMessage, StreamFault>> + Send + '_ {
            let delivery = PlainMessage {
                payload: Bytes::from_static(b"order"),
                headers: HeaderMap::new(),
                settlement: Settlement::Panics,
            };
            stream::once(ready(Ok(delivery))).chain(stream::pending())
        }
    }

    impl BatchSubscriber for BrokenSettle {
        type Batch = Vec<PlainMessage>;

        fn batches(
            &mut self,
            size: NonZeroUsize,
        ) -> impl BatchStream<Vec<PlainMessage>, Error = StreamFault> + Send + '_ {
            batched(self.stream(), size)
        }
    }

    /// Acks every delivery: the settle is where a delivery of [`BrokenSettle`] breaks.
    struct Acks;

    impl Handler<PlainMessage, (), ()> for Acks {
        fn handle(
            &self,
            _msg: &PlainMessage,
            _ctx: &mut Context<'_, (), ()>,
        ) -> impl Future<Output = HandlerOutcome> + Send {
            ready(HandlerOutcome::ack())
        }
    }

    /// A batch handler whose batches cannot be loaded: dispatch loads a batch on the part of the
    /// subscription that took it, before the handler's guard.
    struct Unloadable;

    impl BatchHandler<Vec<PlainMessage>, (), ()> for Unloadable {
        type Scratch = ();
        type Taken = Vec<PlainMessage>;

        fn taken(size: NonZeroUsize) -> Vec<PlainMessage> {
            Vec::with_capacity(size.get())
        }

        fn load(_taken: &mut Vec<PlainMessage>, _slot: &mut Vec<PlainMessage>) {
            panic!("the batch could not be loaded");
        }

        async fn handle_batch<Host: SendHost<()>>(
            &self,
            _batch: &mut Vec<PlainMessage>,
            _scratch: &mut (),
            _ctx: &mut Context<'_, (), ()>,
            _host: &Host,
        ) {
        }
    }

    /// Starts a subscription of `orders` with `spawn` and waits for the service to fail while it
    /// runs: nothing else stops it. Hands back the failure it recorded, or `None` where the
    /// service ran on.
    async fn failure_of<Spawn>(spawn: Spawn) -> Option<String>
    where
        Spawn: FnOnce(Shutdown, DispatchFailure<Shutdown>) -> JoinHandle<()>,
    {
        let shutdown = Shutdown::new();
        let failure = DispatchFailure::new(FailurePolicies::default(), shutdown.clone());
        let joined = spawn(shutdown.clone(), failure);
        let stopped = timeout(Duration::from_secs(5), shutdown.cancelled()).await;
        // A loop that ran on goes down here, so it does not outlive the test.
        joined.abort();
        let _ = joined.await;
        stopped.ok()?;
        shutdown.peek_failure()
    }

    /// Starts `subscriber` under `form`, handled one delivery at a time.
    fn single<Sub>(
        subscriber: Sub,
        form: Workers,
    ) -> impl FnOnce(Shutdown, DispatchFailure<Shutdown>) -> JoinHandle<()>
    where
        Sub: Subscriber<Message = PlainMessage> + Send + 'static,
    {
        move |shutdown, failure| {
            spawn_dispatch_workers(
                subscriber,
                Arc::new(Acks),
                shutdown,
                Arc::from("orders"),
                Arc::new(()),
                Arc::new(Hosted::empty()),
                failure,
                form,
            )
            .expect("the dispatch starts")
        }
    }

    /// Starts `subscriber` under `form`, handled in batches of one.
    fn batches<Sub>(
        subscriber: Sub,
        form: Workers,
    ) -> impl FnOnce(Shutdown, DispatchFailure<Shutdown>) -> JoinHandle<()>
    where
        Sub: BatchSubscriber<Message = PlainMessage, Batch = Vec<PlainMessage>> + Send + 'static,
    {
        move |shutdown, failure| {
            spawn_batch_dispatch::<_, _, (), _, _>(
                subscriber,
                Arc::new(Unloadable),
                shutdown,
                Arc::from("orders"),
                Arc::new(()),
                Arc::new(Hosted::empty()),
                failure,
                form,
                crate::nonzero!(1usize),
            )
            .expect("the dispatch starts")
        }
    }

    /// Every form a subscription runs under, and the part of it that takes the only delivery:
    /// the loop itself, a worker of its pool, or the first of its dedicated threads.
    const FORMS: [(Workers, &str); 5] = [
        (Workers::sequential(), "its dispatch loop"),
        (Workers::pool(crate::nonzero!(2usize)), "a worker"),
        (Workers::keyed(crate::nonzero!(2usize)), "a worker"),
        (
            Workers::threads(crate::nonzero!(2usize)),
            "its dedicated thread 0",
        ),
        (
            Workers::threads_keyed(crate::nonzero!(2usize)),
            "its dedicated thread 0",
        ),
    ];

    /// The batch counterpart of [`FORMS`]: keyed workers do not apply to batches.
    const BATCH_FORMS: [(Workers, &str); 3] = [
        (Workers::sequential(), "its dispatch loop"),
        (Workers::pool(crate::nonzero!(2usize)), "a worker"),
        (
            Workers::threads(crate::nonzero!(2usize)),
            "its dedicated thread 0",
        ),
    ];

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_loop_whose_stream_panics_fails_the_service() {
        // The loop reads the stream whatever the form, so the loop is what panics.
        for (form, _) in FORMS {
            let failure = failure_of(single(BrokenStream, form)).await;
            assert_eq!(
                failure.as_deref(),
                Some("orders: its dispatch loop panicked"),
                "{form:?}",
            );
        }
        for (form, _) in BATCH_FORMS {
            let failure = failure_of(batches(BrokenStream, form)).await;
            assert_eq!(
                failure.as_deref(),
                Some("orders: its dispatch loop panicked"),
                "batches under {form:?}",
            );
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn the_part_that_panics_on_a_delivery_fails_the_service() {
        for (form, part) in FORMS {
            let failure = failure_of(single(BrokenSettle, form)).await;
            assert_eq!(
                failure,
                Some(format!("orders: {part} panicked")),
                "{form:?}"
            );
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn the_part_that_panics_on_a_batch_fails_the_service() {
        for (form, part) in BATCH_FORMS {
            let failure = failure_of(batches(BrokenSettle, form)).await;
            assert_eq!(
                failure,
                Some(format!("orders: {part} panicked")),
                "batches under {form:?}",
            );
        }
    }
}
