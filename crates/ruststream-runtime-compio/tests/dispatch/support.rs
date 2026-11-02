//! The doubles and the launcher the dispatch tests share.

use std::convert::Infallible;
use std::future::ready;
use std::num::NonZeroUsize;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context as TaskContext, Poll};
use std::thread::{self, ThreadId};
use std::time::Duration;

use bytes::Bytes;
use futures::channel::mpsc;
use futures::{Stream, StreamExt};
use ruststream_core::{AckError, HeaderMap, IncomingMessage, Settle, Subscriber};
use ruststream_runtime::{
    Context, Delivery, DispatchFailure, FailurePolicies, Handler, HandlerOutcome, NoRetry,
};
use ruststream_runtime_compio::dispatch::{Fabric, Hosted};
use ruststream_runtime_compio::{Cores, Stop, Tasks};

/// A delivery that settles at once, with the key and the position the test gave it.
#[derive(Debug)]
pub(crate) struct Msg {
    pub(crate) payload: Bytes,
    pub(crate) key: Option<Bytes>,
    pub(crate) headers: HeaderMap,
}

impl Msg {
    pub(crate) fn numbered(position: usize, key: Option<&'static str>) -> Self {
        Self {
            payload: Bytes::copy_from_slice(&position.to_be_bytes()),
            key: key.map(|key| Bytes::from_static(key.as_bytes())),
            headers: HeaderMap::new(),
        }
    }

    pub(crate) fn position(&self) -> usize {
        usize::from_be_bytes(
            self.payload
                .as_ref()
                .try_into()
                .expect("a numbered delivery"),
        )
    }
}

impl IncomingMessage for Msg {
    fn payload(&self) -> &[u8] {
        &self.payload
    }

    fn headers(&self) -> &HeaderMap {
        &self.headers
    }

    fn partition_key(&self) -> Option<&[u8]> {
        self.key.as_deref()
    }
}

impl Settle for Msg {
    fn ack(self) -> impl Future<Output = Result<(), AckError>> {
        ready(Ok(()))
    }

    fn nack(self, _requeue: bool) -> impl Future<Output = Result<(), AckError>> {
        ready(Ok(()))
    }
}

/// The threads a subscriber's stream was polled on.
pub(crate) type Polled = Arc<Mutex<Vec<ThreadId>>>;

/// A subscriber over a channel the test feeds, recording the thread of each poll.
pub(crate) struct Channel {
    items: Option<mpsc::UnboundedReceiver<Msg>>,
    pub(crate) polled: Polled,
}

/// The test's end of a [`Channel`].
pub(crate) fn channel() -> (mpsc::UnboundedSender<Msg>, Channel) {
    let (sender, items) = mpsc::unbounded();
    (
        sender,
        Channel {
            items: Some(items),
            polled: Polled::default(),
        },
    )
}

pub(crate) struct Recorded<Inner> {
    inner: Inner,
    polled: Polled,
}

impl<Inner: Stream + Unpin> Stream for Recorded<Inner> {
    type Item = Result<Inner::Item, Infallible>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut TaskContext<'_>) -> Poll<Option<Self::Item>> {
        if let Ok(mut polled) = self.polled.lock() {
            polled.push(thread::current().id());
        }
        Pin::new(&mut self.inner)
            .poll_next(cx)
            .map(|item| item.map(Ok))
    }
}

impl Subscriber for Channel {
    type Message = Msg;
    type Error = Infallible;

    fn stream(&mut self) -> impl Stream<Item = Result<Msg, Infallible>> + Send + '_ {
        Recorded {
            inner: self.items.take().expect("the stream is opened once"),
            polled: Arc::clone(&self.polled),
        }
    }
}

/// A handler that runs `before(msg)` on the thread of the delivery and awaits what it returns.
pub(crate) struct Reporting<Before>(pub Before);

impl<Before, Wait> Handler<Msg, (), ()> for Reporting<Before>
where
    Before: Fn(&Msg) -> Wait + Send + Sync,
    Wait: Future<Output = ()> + Send,
{
    fn handle(
        &self,
        msg: &Msg,
        _ctx: &mut Context<'_, (), ()>,
    ) -> impl Future<Output = HandlerOutcome> + Send {
        let waiting = (self.0)(msg);
        async move {
            waiting.await;
            HandlerOutcome::ack()
        }
    }
}

/// What a delivery looked like to the handler that ran it.
#[derive(Debug)]
pub(crate) struct Seen {
    pub(crate) position: usize,
    pub(crate) key: Option<Bytes>,
    pub(crate) thread: ThreadId,
}

/// A handler that reports each delivery to the test and waits for nothing.
pub(crate) fn reporting(
    seen: mpsc::UnboundedSender<Seen>,
) -> Arc<Reporting<impl Fn(&Msg) -> std::future::Ready<()> + Send + Sync>> {
    Arc::new(Reporting(move |msg: &Msg| {
        let _ = seen.unbounded_send(Seen {
            position: msg.position(),
            key: msg.key.clone(),
            thread: thread::current().id(),
        });
        ready(())
    }))
}

/// The next `count` reports.
pub(crate) async fn collect(seen: &mut mpsc::UnboundedReceiver<Seen>, count: usize) -> Vec<Seen> {
    let mut all = Vec::with_capacity(count);
    for _ in 0..count {
        all.push(seen.next().await.expect("the handlers report"));
    }
    all
}

pub(crate) fn hosted(tasks: Tasks) -> Arc<Hosted<(), NoRetry>> {
    Arc::new(Hosted::new(Delivery::unscoped(), None, tasks))
}

pub(crate) fn failure(stop: &Stop) -> DispatchFailure<Stop> {
    DispatchFailure::new(FailurePolicies::default(), stop.clone())
}

pub(crate) fn nonzero(count: usize) -> NonZeroUsize {
    NonZeroUsize::new(count).expect("a count")
}

/// Runs `main` on core 0 of `count` cores, every other core serving its mailbox, and returns what
/// `main` returned once every core has ended. Core 0 serves its own mailbox beside `main`.
pub(crate) fn on_cores<Out, Main>(count: usize, main: Main) -> Out
where
    Out: Send,
    Main: AsyncFnOnce(Fabric, Stop, Tasks) -> Out + Send,
{
    on_cores_stopping(count, Stop::default(), main)
}

/// [`on_cores`] under a stop signal the test made, to choose what a stream that ends does.
pub(crate) fn on_cores_stopping<Out, Main>(count: usize, stop: Stop, main: Main) -> Out
where
    Out: Send,
    Main: AsyncFnOnce(Fabric, Stop, Tasks) -> Out + Send,
{
    let fabric = Fabric::open(count);
    let tasks = Tasks::default();
    let main = Mutex::new(Some(main));
    let outs = Cores::new(nonzero(count))
        .unpinned()
        .without_signals()
        .run(&stop, async |core, stop| {
            if core.is_main() {
                let serving = compio::runtime::spawn({
                    let (fabric, stop) = (fabric.clone(), stop.clone());
                    async move { fabric.serve(0, &stop).await }
                });
                let main = main
                    .lock()
                    .expect("the lock")
                    .take()
                    .expect("core 0 runs once");
                // A scenario that deadlocks fails here rather than hanging the run.
                let out = compio::time::timeout(
                    Duration::from_secs(10),
                    main(fabric.clone(), stop.clone(), tasks.clone()),
                )
                .await
                .expect("the scenario finished in time");
                stop.cancel();
                let _ = serving.await;
                Ok::<_, Infallible>(Some(out))
            } else {
                fabric.serve(core.index(), &stop).await;
                Ok(None)
            }
        })
        .expect("the cores run");
    outs.into_iter().flatten().next().expect("core 0 returned")
}
