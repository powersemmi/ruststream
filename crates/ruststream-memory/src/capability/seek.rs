//! Repositioning a retaining broker's subscription: the positions, the seeker and the handoff
//! the subscriber applies inside its own poll.

use std::{
    fmt,
    future::ready,
    num::NonZeroU64,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
};

use futures::task::AtomicWaker;

use crate::log::NameLog;
use crate::{
    Bus, LogMode, MemoryDelivery, MemoryError, MemoryMessage, MemorySubscriber, Retaining,
    SubscriptionShared, log,
};
#[cfg(doc)]
use ruststream_core::Positioned;
use ruststream_core::{Seekable, Seeker};
#[cfg(feature = "testing")]
use ruststream_runtime::testing::Coordinator;

/// Shared between a subscriber's polling side and the seekers minted off it.
///
/// A seek is a handoff, not an in-place mutation: the seeker records the target and wakes the
/// stream, and the subscriber applies it inside its own poll, where `&mut` access to the
/// receiver is available (see [`MemorySubscriber::apply_pending_seek`]).
#[derive(Default)]
pub(crate) struct SeekControl {
    /// The requested log position, taken by the subscriber inside its next poll.
    pending: Mutex<Option<usize>>,
    /// Deliveries stamped below this log position are stale pre-seek copies (a requeue racing
    /// the seek) and are dropped by the polling side.
    watermark: AtomicUsize,
    /// Wakes the subscriber's stream task after `pending` is set.
    pub(crate) waker: AtomicWaker,
}

impl SeekControl {
    /// The stale-delivery cutoff, read on the polling side for every delivery.
    ///
    /// Acquire pairs with the Release store in [`Seeker::seek`]: a poll that observes a
    /// delivery enqueued after a seek also observes that seek's watermark.
    pub(crate) fn watermark(&self) -> usize {
        self.watermark.load(Ordering::Acquire)
    }

    /// Records a reposition to `target` for the subscriber's next poll, and reports whether one
    /// was already pending.
    fn request(&self, target: usize) -> bool {
        // Watermark first (Release, paired with the Acquire load in the delivery filter), then
        // the pending target: a poll that takes the target must see its watermark.
        self.watermark.store(target, Ordering::Release);
        self.pending
            .lock()
            .expect("memory broker mutex poisoned")
            .replace(target)
            .is_some()
    }

    /// Takes the reposition a seeker recorded, if one is pending.
    #[inline]
    fn take(&self) -> Option<usize> {
        self.pending
            .lock()
            .expect("memory broker mutex poisoned")
            .take()
    }
}

/// A position in one name's publish log on a retaining in-memory broker.
///
/// Construct it with [`start`](Self::start), [`sequence`](Self::sequence) or [`end`](Self::end),
/// or capture the position of a delivered [`MemoryMessage`] with [`Positioned::position`].
/// Sequence numbers are absolute and stable: the `n`-th message published to a name keeps
/// position `n` while the broker retains it, whatever the retention bound has evicted before it,
/// and seeking to a captured position redelivers exactly that message.
///
/// The two constructed positions follow the broker's own vocabulary: [`start`](Self::start) is
/// the oldest message still retained, [`end`](Self::end) is the tip, past everything published so
/// far. A sequence the retention bound has already evicted is refused with
/// [`MemoryError::PositionEvicted`]; one past the tip resumes with the next publish.
///
/// # Examples
///
/// ```
/// # mod demo {
/// use ruststream::memory::prelude::*;
/// use serde::Deserialize;
///
/// #[derive(Deserialize)]
/// struct Entry {
///     id: u64,
/// }
///
/// /// Rebuilds its view from everything the broker still holds, on every start.
/// #[subscriber("audit", start_at(MemoryPosition::start()))]
/// async fn rebuild(entry: &Entry) -> HandlerOutcome {
///     tracing::info!(entry.id, "replayed");
///     HandlerOutcome::ack()
/// }
///
/// fn app() -> RustStream {
///     // The last 1024 messages of every name stay replayable.
///     let broker = MemoryBroker::retaining(Retention::Messages(nonzero!(1024)));
///     RustStream::new(AppInfo::new("audit", "0.1.0")).with_broker(broker, |b| {
///         b.include(rebuild)
///     })
/// }
/// # }
/// # fn main() {}
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[must_use]
pub enum MemoryPosition {
    /// The oldest message the name still retains.
    Start,
    /// The zero-based `n`-th message published to the name.
    Sequence(usize),
    /// The tip of the log: everything published so far is behind it.
    End,
}

impl MemoryPosition {
    /// The oldest retained message: seeking here replays everything the broker still holds for
    /// the name.
    pub const fn start() -> Self {
        Self::Start
    }

    /// The position of the zero-based `n`-th message published to the name.
    pub const fn sequence(n: usize) -> Self {
        Self::Sequence(n)
    }

    /// The end of the log: seeking here skips everything published so far and resumes with
    /// the next publish (the memory analog of a "latest" position).
    pub const fn end() -> Self {
        Self::End
    }
}

/// A clonable handle repositioning one [`MemorySubscriber`], minted by
/// [`Seekable::seeker`] before the subscription's stream is opened.
///
/// The scope is that one subscription instance: other subscribers of the same name are
/// unaffected. After the bus shuts down, [`seek`](Seeker::seek) reports
/// [`MemoryError::ShutDown`] like any other aliased handle.
///
/// # Examples
///
/// ```
/// # mod demo {
/// use ruststream::Seeker;
/// use ruststream::memory::prelude::*;
/// use serde::Deserialize;
///
/// #[derive(Deserialize)]
/// struct Job {
///     id: u64,
///     poisoned_until: Option<usize>,
/// }
///
/// /// Skips forward past a region the producer marked poisoned.
/// #[subscriber("jobs")]
/// async fn work(job: &Job, Ctx(seeker): Ctx<SeekHandle>) -> HandlerOutcome {
///     if let Some(resume_at) = job.poisoned_until {
///         if seeker
///             .seek(MemoryPosition::sequence(resume_at))
///             .await
///             .is_err()
///         {
///             return HandlerOutcome::retry();
///         }
///     }
///     tracing::info!(job.id, "done");
///     HandlerOutcome::ack()
/// }
///
/// fn app() -> RustStream {
///     let broker = MemoryBroker::retaining(Retention::Messages(nonzero!(1024)));
///     RustStream::new(AppInfo::new("jobs", "0.1.0")).with_broker(broker, |b| {
///         b.include(work)
///     })
/// }
/// # }
/// # fn main() {}
/// ```
#[derive(Clone)]
pub struct MemorySeeker {
    // The subscription's shared block, which every delivery already holds: minting a seeker off
    // a delivery, as the per-delivery context does, is one reference count.
    subscription: Arc<SubscriptionShared>,
}

impl MemorySeeker {
    /// The seeker of the subscription `msg` came from: the reference count the delivery holds,
    /// taken once more, so a per-delivery context allocates nothing.
    ///
    /// # Panics
    ///
    /// Panics on a request-reply inbox message, which has no subscription behind it; no dispatch
    /// loop, and therefore no context, ever builds off one.
    pub(super) fn of(msg: &MemoryMessage<Retaining>) -> Self {
        Self {
            subscription: Arc::clone(
                msg.subscription
                    .as_ref()
                    .expect("a seek context builds only off subscription deliveries"),
            ),
        }
    }
}

impl fmt::Debug for MemorySeeker {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MemorySeeker")
            .field("name", &self.subscription.name)
            .finish_non_exhaustive()
    }
}

/// Seeking is native on a retaining broker: it keeps a bounded publish log per name, so a
/// subscription can re-enqueue any retained suffix of it. A discarding broker keeps no log, and
/// its subscriptions carry no seek surface at all.
impl Seekable for MemorySubscriber<Retaining> {
    type Seeker = MemorySeeker;

    fn seeker(&self) -> MemorySeeker {
        MemorySeeker {
            subscription: Arc::clone(&self.subscription),
        }
    }
}

impl Seeker for MemorySeeker {
    type Position = MemoryPosition;
    type Error = MemoryError;

    /// Records the reposition and wakes the subscription; the stream applies it at the top of
    /// its next poll, so once this resolves the next delivery reflects the new position.
    ///
    /// Under a [`TestApp`](https://docs.rs/ruststream/latest/ruststream/testing/struct.TestApp.html) the reposition counts as in-flight work
    /// until the subscription applies it, so driving the service to a standstill waits for the
    /// replay instead of racing the poll that produces it.
    ///
    /// # Errors
    ///
    /// Returns [`MemoryError::ShutDown`] through a handle aliasing a shut-down bus, and
    /// [`MemoryError::PositionEvicted`] for a sequence the retention bound has already dropped.
    fn seek(&self, to: MemoryPosition) -> impl Future<Output = Result<(), MemoryError>> {
        // The liveness check and the handoff happen under the bus lock (then the log lock,
        // fanout's order): a seek that returns Ok happened strictly before any shutdown, and
        // the resolved position is consistent with the log at that instant.
        let bus = self
            .subscription
            .state
            .subscribers
            .lock()
            .expect("memory broker mutex poisoned");
        if !matches!(&*bus, Bus::Live(_)) {
            return ready(Err(MemoryError::ShutDown));
        }
        let log = self
            .subscription
            .state
            .log
            .lock()
            .expect("memory broker mutex poisoned");
        let (oldest, tip) = log
            .name(&self.subscription.name)
            .map_or((0, 0), |name| (name.first_seq(), name.next_seq()));
        drop(log);
        let target = match to.resolve(oldest, tip) {
            Ok(target) => target,
            Err(evicted) => return ready(Err(evicted)),
        };
        let replaced = self.subscription.seek.request(target);
        drop(bus);
        // Under the harness, hold the reaction open from the moment the reposition is recorded
        // until the stream applies it: the replay it will enqueue is not counted yet, and a
        // quiescence wait landing in that window would return on an empty in-flight count.
        // One token per pending reposition, released when the subscription applies it: a second
        // seek before the first is applied replaces the target and owes nothing more.
        #[cfg(feature = "testing")]
        if !replaced && let Some(coordinator) = self.subscription.state.coordinator() {
            coordinator.enqueued();
        }
        #[cfg(not(feature = "testing"))]
        let _ = replaced;
        self.subscription.seek.waker.wake();
        ready(Ok(()))
    }
}

impl MemoryPosition {
    /// The absolute log position this one names in a log holding `oldest` up to `tip`.
    ///
    /// # Errors
    ///
    /// Returns [`MemoryError::PositionEvicted`] for a sequence below `oldest`.
    fn resolve(self, oldest: usize, tip: usize) -> Result<usize, MemoryError> {
        match self {
            Self::Start => Ok(oldest),
            Self::End => Ok(tip),
            // Past the tip is a position the name has not reached yet: the subscription waits
            // there rather than failing, and the next publish opens it.
            Self::Sequence(n) if n >= oldest => Ok(n.min(tip)),
            Self::Sequence(n) => Err(MemoryError::PositionEvicted {
                requested: n,
                oldest,
            }),
        }
    }
}

impl<Log: LogMode> MemorySubscriber<Log> {
    /// Applies a reposition requested through a [`MemorySeeker`], if one is pending.
    ///
    /// Runs at the top of the subscriber's own poll (the stream and batch paths both call it),
    /// which is what makes `&mut` access to the receiver possible: everything queued before
    /// the seek is drained, then the retained log from the resolved target on is re-enqueued.
    /// Holding both bus locks makes the swap atomic against `fanout`, which stamps and sends
    /// under the same locks in the same order.
    pub(crate) fn apply_pending_seek(&mut self) {
        // A discarding subscription hands out no seeker, so nothing can ever be pending: the
        // constant drops the whole body, locks included, from its poll.
        if !log::retains::<Log>() {
            return;
        }
        // Take the target in its own statement so the pending guard is released before the
        // bus lock is acquired; `Seeker::seek` nests the locks in the other direction.
        let pending = self.subscription.seek.take();
        let Some(target) = pending else { return };
        self.replay_from(target);
    }

    /// Swaps what is queued for the retained log from `target` on, under both bus locks.
    fn replay_from(&mut self, target: usize) {
        #[cfg(feature = "testing")]
        let coordinator = self.coordinator();
        // The seek counted itself in flight when it recorded this target; the token goes back
        // however this returns, so a quiescence wait cannot observe the gap between the request
        // and the replay, and a seek that raced shutdown still drains the in-flight count.
        #[cfg(feature = "testing")]
        let seek_token = SeekToken(coordinator.clone());

        let bus = self
            .subscription
            .state
            .subscribers
            .lock()
            .expect("memory broker mutex poisoned");
        // A seek that raced shutdown: the subscription is dead, replay nothing.
        if !matches!(&*bus, Bus::Live(_)) {
            return;
        }
        let log = self
            .subscription
            .state
            .log
            .lock()
            .expect("memory broker mutex poisoned");
        let retained = log.name(self.subscription.name.as_str());

        // The replay is counted in flight BEFORE the drain releases the queued deliveries:
        // decrementing first would let the in-flight count touch zero mid-swap, and a
        // concurrent quiescence wait (`TestApp::settle`) could observe that instant and return
        // before the replayed deliveries were processed.
        #[cfg(feature = "testing")]
        if let Some(coordinator) = &coordinator {
            for _ in retained.into_iter().flat_map(|log| log.replay_from(target)) {
                coordinator.enqueued();
            }
        }

        while self.rx.try_recv().is_ok() {
            // Every drained delivery was counted in flight when it was enqueued.
            #[cfg(feature = "testing")]
            if let Some(coordinator) = &coordinator {
                coordinator.consumed();
            }
        }

        if let Some(retained) = retained {
            self.subscription.requeue_replay(retained, target);
        }
        drop(log);
        drop(bus);
        #[cfg(feature = "testing")]
        drop(seek_token);
    }
}

impl SubscriptionShared {
    /// Enqueues the retained messages from `target` on to this subscription, each as a fresh
    /// delivery.
    fn requeue_replay(&self, retained: &NameLog, target: usize) {
        for (seq, entry) in retained.replay_from(target) {
            let delivery = MemoryDelivery {
                // The block the fanout built, name and payload and headers together: a
                // replayed delivery is one reference count, not a rebuilt message.
                shared: entry.shared(),
                seq,
                // A replay is a fresh delivery of what the log holds, not a redelivery of
                // the copy a handler saw, so its count starts where a fanout's does.
                deliveries: NonZeroU64::MIN,
            };
            // The send cannot fail: this subscriber holds both ends of its own channel.
            let _ = self.requeue.send(delivery);
        }
    }
}

/// The in-flight token one pending reposition holds, released when the subscription has applied
/// it (whichever way [`apply_pending_seek`](MemorySubscriber::apply_pending_seek) returns).
#[cfg(feature = "testing")]
struct SeekToken(Option<Coordinator>);

#[cfg(feature = "testing")]
impl Drop for SeekToken {
    fn drop(&mut self) {
        if let Some(coordinator) = &self.0 {
            coordinator.consumed();
        }
    }
}

#[cfg(test)]
mod tests {
    use ruststream_core::Settle;
    use std::future;
    use std::time::Duration;

    use futures::{Stream, StreamExt};
    use tokio::sync::oneshot;
    use tokio::time::timeout;

    use super::*;
    use crate::capability::tests::replaying;
    use crate::{MemoryBroker, MemorySource, Retention};
    #[cfg(feature = "testing")]
    use ruststream_core::Subscribe;
    use ruststream_core::{
        BatchSubscriber, Broker, ConnectedBroker, IncomingMessage, OutgoingMessage, Positioned,
        Publisher, StartAt, Subscriber, SubscriptionSource, nonzero,
    };
    #[cfg(feature = "testing")]
    use ruststream_runtime::testing::TestableBroker;

    #[test]
    fn a_seekers_debug_form_names_its_subject() {
        let seeker = replaying().subscribe("seek.debug").seeker();
        let rendered = format!("{seeker:?}");
        // A seeker is only meaningful against its name, so Debug has to carry it.
        assert!(rendered.contains("seek.debug"), "{rendered}");
    }

    #[tokio::test]
    async fn seek_back_redelivers_from_the_captured_position() {
        let broker = replaying();
        let mut sub = broker.subscribe("seek.back");
        let seeker = sub.seeker();
        let publisher = broker.publisher();
        for payload in [b"a", b"b", b"c"] {
            publisher
                .publish(OutgoingMessage::new("seek.back", payload), None)
                .await
                .unwrap();
        }

        let mut stream = std::pin::pin!(sub.stream());
        let mut positions = Vec::new();
        for _ in 0..3 {
            let msg = stream.next().await.unwrap().unwrap();
            positions.push(msg.position());
            msg.ack().await.unwrap();
        }

        seeker.seek(positions[1]).await.unwrap();
        let redelivered = stream.next().await.unwrap().unwrap();
        assert_eq!(redelivered.payload(), b"b");
        // The replayed copy reports the same position as the original delivery.
        assert_eq!(redelivered.position(), positions[1]);
        redelivered.ack().await.unwrap();
        let tail = stream.next().await.unwrap().unwrap();
        assert_eq!(tail.payload(), b"c");
        tail.ack().await.unwrap();
        assert!(futures::poll!(stream.next()).is_pending());
    }

    /// A captured position the retention bound has dropped cannot be honoured: the seek says so
    /// instead of quietly starting somewhere else, which would look like a replay that lost its
    /// first messages.
    #[tokio::test]
    async fn seeking_to_an_evicted_position_reports_it() {
        let broker = MemoryBroker::retaining(Retention::Messages(nonzero!(2)));
        let mut sub = broker.subscribe("seek.evicted");
        let seeker = sub.seeker();
        let publisher = broker.publisher();
        for payload in [b"a", b"b", b"c", b"d"] {
            publisher
                .publish(OutgoingMessage::new("seek.evicted", payload), None)
                .await
                .unwrap();
        }

        assert_eq!(
            seeker.seek(MemoryPosition::sequence(0)).await,
            Err(MemoryError::PositionEvicted {
                requested: 0,
                oldest: 2,
            }),
        );

        // The start position means the oldest message still retained, so it always replays.
        seeker.seek(MemoryPosition::start()).await.unwrap();
        let mut stream = std::pin::pin!(sub.stream());
        let replayed = stream.next().await.unwrap().unwrap();
        assert_eq!(replayed.payload(), b"c");
        assert_eq!(replayed.position(), MemoryPosition::sequence(2));
        replayed.ack().await.unwrap();
    }

    #[tokio::test]
    async fn stale_requeue_racing_a_seek_is_dropped() {
        let broker = replaying();
        let mut sub = broker.subscribe("seek.stale");
        let seeker = sub.seeker();
        let publisher = broker.publisher();
        for payload in [b"a", b"b", b"c"] {
            publisher
                .publish(OutgoingMessage::new("seek.stale", payload), None)
                .await
                .unwrap();
        }

        let mut stream = std::pin::pin!(sub.stream());
        let held = stream.next().await.unwrap().unwrap();
        assert_eq!(held.payload(), b"a");

        seeker.seek(MemoryPosition::sequence(2)).await.unwrap();
        let skipped_to = stream.next().await.unwrap().unwrap();
        assert_eq!(skipped_to.payload(), b"c");
        skipped_to.ack().await.unwrap();

        // The requeue lands after the seek; its copy is below the watermark and is dropped
        // instead of resurrecting a delivery the reposition already skipped.
        held.nack(true).await.unwrap();
        assert!(futures::poll!(stream.next()).is_pending());

        publisher
            .publish(OutgoingMessage::new("seek.stale", b"d"), None)
            .await
            .unwrap();
        let live = stream.next().await.unwrap().unwrap();
        assert_eq!(live.payload(), b"d");
        live.ack().await.unwrap();
    }

    #[tokio::test]
    async fn seek_past_the_end_skips_the_queue_and_resumes_with_the_next_publish() {
        let broker = replaying();
        let mut sub = broker.subscribe("seek.end");
        let seeker = sub.seeker();
        let publisher = broker.publisher();
        for payload in [b"a", b"b"] {
            publisher
                .publish(OutgoingMessage::new("seek.end", payload), None)
                .await
                .unwrap();
        }

        // The target is clamped to the log end at seek time, so everything queued is skipped
        // and the next publish is not filtered away.
        seeker.seek(MemoryPosition::sequence(10)).await.unwrap();
        let mut stream = std::pin::pin!(sub.stream());
        assert!(futures::poll!(stream.next()).is_pending());

        publisher
            .publish(OutgoingMessage::new("seek.end", b"c"), None)
            .await
            .unwrap();
        let live = stream.next().await.unwrap().unwrap();
        assert_eq!(live.payload(), b"c");
        live.ack().await.unwrap();
    }

    #[tokio::test]
    async fn start_at_replays_the_log_into_a_fresh_subscription() {
        let broker = replaying();
        let connected = broker.connect().await.unwrap();
        let publisher = connected.publisher();
        for payload in [b"a", b"b"] {
            publisher
                .publish(OutgoingMessage::new("start.replay", payload), None)
                .await
                .unwrap();
        }

        // The subscription opens after both publishes; the start position replays them.
        let mut sub = StartAt::new(MemorySource::new("start.replay"), MemoryPosition::start())
            .subscribe(&connected)
            .await
            .unwrap();
        let mut stream = std::pin::pin!(sub.stream());
        let first = stream.next().await.unwrap().unwrap();
        assert_eq!(first.payload(), b"a");
        first.ack().await.unwrap();
        let second = stream.next().await.unwrap().unwrap();
        assert_eq!(second.payload(), b"b");
        second.ack().await.unwrap();
        assert!(futures::poll!(stream.next()).is_pending());
    }

    #[tokio::test]
    async fn start_at_end_skips_history_and_sees_the_next_publish() {
        let broker = replaying();
        let connected = broker.connect().await.unwrap();
        let publisher = connected.publisher();
        publisher
            .publish(OutgoingMessage::new("start.end", b"old"), None)
            .await
            .unwrap();

        let mut sub = StartAt::new(MemorySource::new("start.end"), MemoryPosition::end())
            .subscribe(&connected)
            .await
            .unwrap();
        let mut stream = std::pin::pin!(sub.stream());
        assert!(futures::poll!(stream.next()).is_pending());

        publisher
            .publish(OutgoingMessage::new("start.end", b"new"), None)
            .await
            .unwrap();
        let live = stream.next().await.unwrap().unwrap();
        assert_eq!(live.payload(), b"new");
        live.ack().await.unwrap();
    }

    #[tokio::test]
    async fn seeker_errors_after_shutdown() {
        let broker = replaying();
        let sub = broker.subscribe("seek.down");
        let seeker = sub.seeker();
        let connected = broker.connect().await.unwrap();
        connected.shutdown().await.unwrap();

        assert_eq!(
            seeker.seek(MemoryPosition::start()).await,
            Err(MemoryError::ShutDown),
        );
    }

    #[tokio::test]
    async fn batches_replay_after_a_seek() {
        let broker = replaying();
        let mut sub = broker.subscribe("seek.batch");
        let seeker = sub.seeker();
        let publisher = broker.publisher();
        for payload in [b"a", b"b", b"c"] {
            publisher
                .publish(OutgoingMessage::new("seek.batch", payload), None)
                .await
                .unwrap();
        }

        let mut stream = std::pin::pin!(sub.batches(nonzero!(64)));
        let batch = stream.next().await.unwrap().unwrap();
        assert_eq!(batch.len(), 3);
        for msg in batch {
            msg.ack().await.unwrap();
        }

        seeker.seek(MemoryPosition::start()).await.unwrap();
        let replayed = stream.next().await.unwrap().unwrap();
        let payloads: Vec<&[u8]> = replayed.iter().map(IncomingMessage::payload).collect();
        assert_eq!(
            payloads,
            [b"a".as_slice(), b"b".as_slice(), b"c".as_slice()]
        );
        for msg in replayed {
            msg.ack().await.unwrap();
        }
    }

    // The wake path is the point of the capability: a dispatch loop parked on an empty
    // subscription must observe a seek without waiting for an unrelated publish, so this test
    // parks a real task (the oneshot fires on its first Pending poll) before seeking.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn seek_wakes_a_parked_stream() {
        let broker = replaying();
        let mut sub = broker.subscribe("seek.wake");
        let seeker = sub.seeker();
        let publisher = broker.publisher();
        publisher
            .publish(OutgoingMessage::new("seek.wake", b"a"), None)
            .await
            .unwrap();
        {
            let mut stream = std::pin::pin!(sub.stream());
            stream.next().await.unwrap().unwrap().ack().await.unwrap();
        }

        let (parked_tx, parked_rx) = oneshot::channel();
        let handle = tokio::spawn(async move {
            let mut stream = std::pin::pin!(sub.stream());
            let mut parked_tx = Some(parked_tx);
            future::poll_fn(move |cx| {
                let polled = stream.as_mut().poll_next(cx);
                if polled.is_pending()
                    && let Some(tx) = parked_tx.take()
                {
                    let _ = tx.send(());
                }
                polled
            })
            .await
        });

        parked_rx.await.unwrap();
        seeker.seek(MemoryPosition::start()).await.unwrap();
        let replayed = timeout(Duration::from_secs(5), handle)
            .await
            .expect("a seek must wake the parked stream, not wait for the next publish")
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(replayed.payload(), b"a");
        replayed.ack().await.unwrap();
    }

    #[tokio::test]
    async fn batches_drop_stale_requeues_after_a_seek() {
        let broker = replaying();
        let mut sub = broker.subscribe("seek.batch.stale");
        let seeker = sub.seeker();
        let publisher = broker.publisher();
        for payload in [b"a", b"b", b"c"] {
            publisher
                .publish(OutgoingMessage::new("seek.batch.stale", payload), None)
                .await
                .unwrap();
        }

        let mut stream = std::pin::pin!(sub.batches(nonzero!(2)));
        let mut batch = stream.next().await.unwrap().unwrap();
        assert_eq!(batch.len(), 2);
        let held_b = batch.pop().unwrap();
        let held_a = batch.pop().unwrap();
        assert_eq!(held_a.payload(), b"a");
        assert_eq!(held_b.payload(), b"b");

        // Clamped to the log end: queued "c" is drained and nothing is replayed.
        seeker.seek(MemoryPosition::sequence(10)).await.unwrap();
        assert!(futures::poll!(stream.next()).is_pending());

        // A stale copy at the head of the queue: the first-element loop filters it out.
        held_b.nack(true).await.unwrap();
        assert!(futures::poll!(stream.next()).is_pending());

        // A stale copy behind a live delivery: the batch-fill loop filters it out.
        publisher
            .publish(OutgoingMessage::new("seek.batch.stale", b"d"), None)
            .await
            .unwrap();
        held_a.nack(true).await.unwrap();
        let live = stream.next().await.unwrap().unwrap();
        let payloads: Vec<&[u8]> = live.iter().map(IncomingMessage::payload).collect();
        assert_eq!(payloads, [b"d".as_slice()]);
        for msg in live {
            msg.ack().await.unwrap();
        }
        assert!(futures::poll!(stream.next()).is_pending());
    }

    #[cfg(feature = "testing")]
    #[tokio::test]
    async fn seek_keeps_the_coordinator_in_flight_count_balanced() {
        let broker = replaying();
        let connected = broker.connect().await.unwrap();
        let coordinator = Coordinator::new(64);
        connected.install_coordinator(coordinator.clone());

        // Subscribed after the install, so every delivery carries the coordinator.
        let mut sub = connected.subscribe("seek.balance").await.unwrap();
        let seeker = sub.seeker();
        let publisher = connected.publisher();
        for payload in [b"a", b"b"] {
            publisher
                .publish(OutgoingMessage::new("seek.balance", payload), None)
                .await
                .unwrap();
        }

        let mut stream = std::pin::pin!(sub.stream());
        // Held unsettled so its requeue can race the seek below.
        let held = stream.next().await.unwrap().unwrap();
        assert_eq!(held.payload(), b"a");

        // Drains queued "b" (one consumed) and replays the log suffix (one enqueued).
        seeker.seek(MemoryPosition::sequence(1)).await.unwrap();
        let replayed = stream.next().await.unwrap().unwrap();
        assert_eq!(replayed.payload(), b"b");
        replayed.ack().await.unwrap();

        // The stale requeue of "a" is dropped by the watermark filter (one consumed).
        held.nack(true).await.unwrap();
        assert!(futures::poll!(stream.next()).is_pending());

        // Every enqueue across publish, drain, replay, requeue, and filter is balanced, so
        // the harness's quiescence wait resolves instead of hanging.
        coordinator.drive().await.unwrap();
    }

    #[tokio::test]
    async fn seek_scope_is_one_subscriber_instance() {
        let broker = replaying();
        let mut seeking = broker.subscribe("seek.scope");
        let mut bystander = broker.subscribe("seek.scope");
        let seeker = seeking.seeker();
        let publisher = broker.publisher();
        for payload in [b"a", b"b"] {
            publisher
                .publish(OutgoingMessage::new("seek.scope", payload), None)
                .await
                .unwrap();
        }

        let mut seeking_stream = std::pin::pin!(seeking.stream());
        let mut bystander_stream = std::pin::pin!(bystander.stream());
        for _ in 0..2 {
            let msg = seeking_stream.next().await.unwrap().unwrap();
            msg.ack().await.unwrap();
            let msg = bystander_stream.next().await.unwrap().unwrap();
            msg.ack().await.unwrap();
        }

        seeker.seek(MemoryPosition::start()).await.unwrap();
        let replayed = seeking_stream.next().await.unwrap().unwrap();
        assert_eq!(replayed.payload(), b"a");
        replayed.ack().await.unwrap();
        let tail = seeking_stream.next().await.unwrap().unwrap();
        assert_eq!(tail.payload(), b"b");
        tail.ack().await.unwrap();

        // The sibling subscription of the same name is unaffected by the replay.
        assert!(futures::poll!(bystander_stream.next()).is_pending());
    }
}
