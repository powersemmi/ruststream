//! The store of one registration's retry copies: `CAP` records polled by the registration's own
//! loop on the clock, so a copy waits as data rather than as a future of its own.

// The store holds publishers that are not `Send`, and its driver runs on the app's one task.
#![allow(clippy::future_not_send)]

#[cfg(ruststream_alloc)]
use alloc::string::String;
#[cfg(ruststream_alloc)]
use alloc::sync::Arc;
use core::array;
use core::cell::RefCell;
use core::future::poll_fn;
#[cfg(ruststream_alloc)]
use core::future::ready;
use core::mem;
use core::pin::Pin;
use core::task::{Poll, Waker};
use core::time::Duration;

#[cfg(ruststream_alloc)]
use bytes::Bytes;
use embassy_sync::blocking_mutex::Mutex;
#[cfg(not(ruststream_alloc))]
use ruststream_core::{BufferFull, Cause};
use ruststream_core::{Headers, PayloadBuf, Settle};
#[cfg(not(ruststream_alloc))]
use ruststream_runtime::BoxError;
#[cfg(not(ruststream_alloc))]
use ruststream_runtime::FailureSlot;
#[cfg(ruststream_alloc)]
use ruststream_runtime::NoFreeRecordError;
use ruststream_runtime::{
    Clock, Continuation, CopyPublisher, Detach, FailedMessage, KeptCopies, KeptDelivery,
    PrepareCopy, PrepareKept, Reaction, Refusal, publish_done, publish_lost,
};

use crate::clock::{EmbassyClock, Schedule};
use crate::flag::Flag;
use crate::frames::FrameLock;

/// A copy's destination when it is not the subscription: owned with `alloc`, in a fixed room
/// without. The names that reach a store are held whole by the mount (a literal) or built at
/// startup into the same room, so a destination that fits there always fits here.
#[cfg(ruststream_alloc)]
type Destination = String;
/// See the `alloc` form.
#[cfg(not(ruststream_alloc))]
type Destination = heapless::String<64>;

/// The subscription's name as the store keeps it for diagnostics.
#[cfg(ruststream_alloc)]
type Subscription = Arc<str>;
/// See the `alloc` form: a name past the room is cut, which only a diagnostic reads.
#[cfg(not(ruststream_alloc))]
type Subscription = heapless::String<64>;

/// A copy of a payload for a continuation to read: shared bytes with `alloc`, the fixed buffer
/// without.
#[cfg(ruststream_alloc)]
type Copied = Bytes;
/// See the `alloc` form.
#[cfg(not(ruststream_alloc))]
type Copied = PayloadBuf;

/// `name` cut on a character to the room of a [`Subscription`], for a diagnostic.
#[cfg(not(ruststream_alloc))]
fn diagnostic_name(name: &str) -> Subscription {
    let mut end = name.len().min(64);
    while !name.is_char_boundary(end) {
        end -= 1;
    }
    let mut cut = Subscription::new();
    if let Some(head) = name.get(..end) {
        // Cannot fail: `end` is within the room.
        let _ = cut.push_str(head);
    }
    cut
}

/// The records of a store, behind the frame lock.
type Records<At, Options, const CAP: usize> = RefCell<[Record<At, Options>; CAP]>;

/// Where one record is in its life.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Held<At> {
    /// Free for the next copy.
    Free,
    /// Taken by a failing delivery that has not settled yet.
    Taken,
    /// Holding a copy due at the instant.
    Due(At),
    /// Being published.
    Sending,
}

/// A copy that took no record because the store was full: prepared in buffers of its own, which
/// allocate, and published at once.
pub(crate) struct Spilled<Options> {
    payload: PayloadBuf,
    headers: Headers,
    options: Option<Options>,
    /// The payload did not fit the buffer: the copy is refused when it is sent, as a record's is.
    #[cfg(not(ruststream_alloc))]
    oversize: bool,
}

/// One copy: its bytes in a buffer the record keeps from copy to copy, its headers, the
/// per-message settings the mount site's transforms wrote on it, and the handler's `.error`
/// continuation when the outcome that made the copy carried one.
struct Record<At, Options> {
    held: Held<At>,
    payload: PayloadBuf,
    headers: Headers,
    options: Option<Options>,
    /// Whether the delivery's bytes did not fit the payload buffer: the copy cannot leave.
    #[cfg(not(ruststream_alloc))]
    oversize: bool,
    /// Where the copy is bound when it is not the subscription: the buffer is reused from copy
    /// to copy, like the payload's.
    destination: Destination,
    /// Whether the copy is bound for `destination`.
    to_destination: bool,
    /// What a failed publish of this copy is handed to.
    reaction: Option<Reaction>,
}

/// The retry copies of one registration, in `CAP` records.
///
/// A failing delivery takes a record and fills it before the delivery settles; a copy that finds
/// every record taken is the "no free record" failure (see [`KeptCopies::full`]).
/// [`drive`](Self::drive), joined with the registration's lanes, publishes each copy once it is
/// due: it waits for the earliest one on a single timer, so a wake costs one timer poll however
/// many copies wait. A record reuses its buffer, so once a buffer has grown to the largest copy,
/// keeping one allocates nothing.
///
/// A copy the broker refuses is lost, and its record is free again at once: the failure goes to
/// the handler's `.error` continuation, or to a warning without one. The store's size and the
/// broker's capacity are the service's to size for its load.
pub(crate) struct CopyStore<Path: CopyPublisher, const CAP: usize, Timers: Clock = EmbassyClock> {
    path: Path,
    /// The subscription the copies are of, for diagnostics.
    subscription: Subscription,
    records: Mutex<FrameLock, Records<Timers::Instant, Path::Options, CAP>>,
    /// Set when a record was armed since the driver last looked.
    armed: Flag,
    /// Set once the registration's lanes stopped: the driver ends with the last copy sent.
    closed: Flag,
    waker: Mutex<FrameLock, RefCell<Option<Waker>>>,
    clock: Timers,
    /// The name of the type the handler receives, which a lost copy's diagnostic calls its
    /// message.
    input: &'static str,
}

impl<Path: CopyPublisher, const CAP: usize> CopyStore<Path, CAP> {
    /// A store of `subscription`'s copies on embassy's clock, leaving through `path`.
    #[cfg(ruststream_alloc)]
    pub(crate) fn new(path: Path, subscription: &Arc<str>) -> Self {
        Self::on_clock(path, Arc::clone(subscription), EmbassyClock)
    }

    /// A store of `subscription`'s copies on embassy's clock, leaving through `path`.
    #[cfg(not(ruststream_alloc))]
    pub(crate) fn new(path: Path, subscription: &str) -> Self {
        Self::on_clock(path, diagnostic_name(subscription), EmbassyClock)
    }
}

impl<Path: CopyPublisher, const CAP: usize, Timers: Schedule> CopyStore<Path, CAP, Timers> {
    /// A store on `clock`.
    pub(crate) fn on_clock(path: Path, subscription: Subscription, clock: Timers) -> Self {
        Self {
            path,
            subscription,
            records: Mutex::new(RefCell::new(array::from_fn(|_| Self::empty_record()))),
            armed: Flag::new(),
            closed: Flag::new(),
            waker: Mutex::new(RefCell::new(None)),
            clock,
            input: "message",
        }
    }

    /// Names the type the handler receives, as the diagnostic of a lost copy calls its messages.
    pub(crate) fn input(mut self, input: &'static str) -> Self {
        self.input = input;
        self
    }

    fn empty_record() -> Record<Timers::Instant, Path::Options> {
        Record {
            held: Held::Free,
            payload: PayloadBuf::new(),
            headers: Headers::new(),
            options: None,
            #[cfg(not(ruststream_alloc))]
            oversize: false,
            destination: Destination::new(),
            to_destination: false,
            reaction: None,
        }
    }

    /// Runs `with` on the record at `index`.
    fn record<Out>(
        &self,
        index: usize,
        with: impl FnOnce(&mut Record<Timers::Instant, Path::Options>) -> Out,
    ) -> Out {
        self.records
            .lock(|records| with(&mut records.borrow_mut()[index]))
    }

    /// Stops the driver once every copy the store holds has been published.
    pub(crate) fn close(&self) {
        self.closed.raise();
        self.wake();
    }

    /// Publishes every copy once it is due, until [`close`](Self::close) and the last copy sent.
    /// What a failed publish's `.error` continuation makes of it runs on `host`, beside the
    /// driver.
    #[cfg(ruststream_alloc)]
    pub(crate) async fn drive<Host: Detach>(&self, host: &Host) {
        let mut timer: Option<(Timers::Instant, Timers::Sleep)> = None;
        while let Some(record) = self.next_due(&mut timer).await {
            if let Some(reaction) = self.send(record).await {
                host.detach_boxed(reaction);
            }
        }
    }

    /// Publishes every copy once it is due, until [`close`](Self::close) and the last copy sent.
    /// What a failed publish's `.error` continuation makes of it runs on `host`, beside the
    /// driver.
    #[cfg(not(ruststream_alloc))]
    pub(crate) async fn drive<Host: Detach>(&self, host: &Host) {
        let mut timer: Option<(Timers::Instant, Timers::Sleep)> = None;
        while let Some(record) = self.next_due(&mut timer).await {
            if let Some(follow) = self.send(record).await {
                host.detach(follow).await;
            }
        }
    }

    /// Waits for the next due copy, `None` once the store is closed and empty.
    async fn next_due(
        &self,
        timer: &mut Option<(Timers::Instant, Timers::Sleep)>,
    ) -> Option<usize> {
        poll_fn(|cx| {
            self.waker.lock(|waker| {
                let mut waker = waker.borrow_mut();
                if !waker
                    .as_ref()
                    .is_some_and(|known| known.will_wake(cx.waker()))
                {
                    *waker = Some(cx.waker().clone());
                }
            });
            // A new copy may be due before the one the timer waits for.
            if self.armed.take() {
                *timer = None;
            }
            if let Some((_, sleep)) = timer.as_mut() {
                if Pin::new(sleep).poll(cx).is_pending() {
                    return Poll::Pending;
                }
                *timer = None;
            }
            let now = self.clock.now();
            let mut earliest: Option<Timers::Instant> = None;
            let mut busy = false;
            for index in 0..CAP {
                match self.record(index, |record| record.held) {
                    Held::Due(at) if at <= now => return Poll::Ready(Some(index)),
                    Held::Due(at) => earliest = Some(earliest.map_or(at, |next| next.min(at))),
                    Held::Taken | Held::Sending => busy = true,
                    Held::Free => {}
                }
            }
            if let Some(at) = earliest {
                let mut sleep = self.clock.sleep_until(at);
                // Registers this task's waker with the clock.
                if Pin::new(&mut sleep).poll(cx).is_ready() {
                    cx.waker().wake_by_ref();
                } else {
                    *timer = Some((at, sleep));
                }
                return Poll::Pending;
            }
            if self.closed.is_raised() && !busy {
                Poll::Ready(None)
            } else {
                Poll::Pending
            }
        })
        .await
    }

    /// Publishes the copy in `record` to the destination it is bound for, the subscription
    /// unless [`send_now`](KeptCopies::send_now) bound it elsewhere, and frees the record.
    ///
    /// A copy that its broker refuses is lost: what the handler's `.error` continuation makes of
    /// the failure is returned for the caller to run (without `alloc`, run here, on a failure slot
    /// of the call), and without a continuation a warning names the failure. The headers are
    /// copied for the continuation only where one is registered.
    async fn send(&self, record: usize) -> Option<Continuation> {
        let (payload, headers, options, destination, reaction, oversize) =
            self.record(record, |held| {
                held.held = Held::Sending;
                (
                    mem::take(&mut held.payload),
                    mem::take(&mut held.headers),
                    held.options.take(),
                    held.to_destination
                        .then(|| mem::take(&mut held.destination)),
                    held.reaction.take(),
                    Self::take_oversize(held),
                )
            });
        let target = destination
            .as_deref()
            .unwrap_or_else(|| self.path.address());
        #[cfg(ruststream_alloc)]
        let follow = {
            let _ = oversize;
            let sent = self
                .path
                .publish_copy(target, &payload, headers, options.as_ref())
                .await;
            match sent {
                Ok(()) => publish_done(reaction),
                Err(refusal) => self.lost(reaction, target, refusal, |_, _| {
                    // The record is reused, so a lending publisher's bytes are copied.
                    Bytes::copy_from_slice(&payload)
                }),
            }
        };
        #[cfg(not(ruststream_alloc))]
        let (follow, mut failed) = {
            let mut failed = FailureSlot::<1>::new();
            let sent = if oversize {
                Err(Refusal::new(BufferFull))
            } else {
                self.path
                    .publish_copy(target, &payload, headers, options.as_ref())
                    .await
            };
            let follow = match sent {
                Ok(()) => publish_done(reaction),
                Err(refusal) => {
                    // The record is reused, so a lending publisher's bytes are copied.
                    self.lost(
                        reaction,
                        target,
                        refusal,
                        |_, _| payload.clone(),
                        &mut failed,
                    );
                    None
                }
            };
            (follow, failed)
        };
        self.give_back(record, payload, destination);
        #[cfg(not(ruststream_alloc))]
        failed.react().await;
        follow
    }

    /// Whether the record's bytes did not fit, cleared for the next copy.
    #[cfg(ruststream_alloc)]
    const fn take_oversize(_record: &mut Record<Timers::Instant, Path::Options>) -> bool {
        false
    }

    /// Whether the record's bytes did not fit, cleared for the next copy.
    #[cfg(not(ruststream_alloc))]
    fn take_oversize(record: &mut Record<Timers::Instant, Path::Options>) -> bool {
        mem::take(&mut record.oversize)
    }

    /// Reports a copy the publisher refused. The message is read out of the refusal only for a
    /// registered error: the publisher returns it, so nothing is copied for a copy nobody watches.
    #[cfg(ruststream_alloc)]
    fn lost(
        &self,
        reaction: Option<Reaction>,
        target: &str,
        refusal: Refusal,
        lent: impl FnOnce(usize, usize) -> Copied,
    ) -> Option<Continuation> {
        if reaction.is_none() {
            return publish_lost(
                None,
                &self.subscription,
                target,
                self.input,
                refusal.into_error(),
                || FailedMessage::new(target, None, Headers::new()),
            );
        }
        let (err, failed) = refusal.into_message(target, lent);
        publish_lost(
            reaction,
            &self.subscription,
            target,
            self.input,
            err,
            || failed,
        )
    }

    /// Reports a copy the publisher refused, into `slot` for the caller to react to.
    #[cfg(not(ruststream_alloc))]
    fn lost(
        &self,
        reaction: Option<Reaction>,
        target: &str,
        refusal: Refusal,
        lent: impl FnOnce(usize, usize) -> Copied,
        slot: &mut FailureSlot<1>,
    ) {
        if reaction.is_none() {
            publish_lost(
                None,
                &self.subscription,
                target,
                self.input,
                refusal.into_error(),
                slot,
                || FailedMessage::new(target, None, Headers::new()),
            );
            return;
        }
        let (err, failed) = refusal.into_message(target, lent);
        publish_lost(
            reaction,
            &self.subscription,
            target,
            self.input,
            err,
            slot,
            || failed,
        );
    }

    /// Frees `record`, keeping `payload`'s buffer for the next copy.
    fn give_back(&self, record: usize, mut payload: PayloadBuf, destination: Option<Destination>) {
        payload.clear();
        self.record(record, |held| {
            held.payload = payload;
            if let Some(destination) = destination {
                held.destination = destination;
            }
            held.to_destination = false;
            held.held = Held::Free;
        });
        self.wake();
    }

    fn wake(&self) {
        self.waker.lock(|waker| {
            if let Some(waker) = waker.borrow().as_ref() {
                waker.wake_by_ref();
            }
        });
    }
}

impl<Path: CopyPublisher, const CAP: usize, Timers: Schedule> KeptCopies
    for CopyStore<Path, CAP, Timers>
{
    type Spilled = Spilled<Path::Options>;

    fn take_record(&self) -> Option<usize> {
        self.records.lock(|records| {
            let mut records = records.borrow_mut();
            let index = records
                .iter()
                .position(|record| record.held == Held::Free)?;
            records[index].held = Held::Taken;
            Some(index)
        })
    }

    fn send_spilled(
        &self,
        spilled: Spilled<Path::Options>,
        destination: Option<&str>,
        reaction: Option<Reaction>,
    ) -> impl Future<Output = Option<Continuation>> {
        let Spilled {
            payload,
            headers,
            options,
            #[cfg(not(ruststream_alloc))]
            oversize,
        } = spilled;
        async move {
            let target = destination.unwrap_or_else(|| self.path.address());
            #[cfg(not(ruststream_alloc))]
            let sent = if oversize {
                Err(Refusal::new(BufferFull))
            } else {
                self.path
                    .publish_copy(target, &payload, headers, options.as_ref())
                    .await
            };
            #[cfg(ruststream_alloc)]
            let sent = self
                .path
                .publish_copy(target, &payload, headers, options.as_ref())
                .await;
            #[cfg(ruststream_alloc)]
            {
                match sent {
                    Ok(()) => publish_done(reaction),
                    Err(refusal) => self.lost(reaction, target, refusal, |_, _| payload.freeze()),
                }
            }
            #[cfg(not(ruststream_alloc))]
            {
                match sent {
                    Ok(()) => publish_done(reaction),
                    Err(refusal) => {
                        let mut failed = FailureSlot::<1>::new();
                        self.lost(
                            reaction,
                            target,
                            refusal,
                            |_, _| payload.clone(),
                            &mut failed,
                        );
                        failed.react().await;
                        None
                    }
                }
            }
        }
    }

    fn full(
        &self,
        payload: Option<Copied>,
        headers: Headers,
        reaction: Option<Reaction>,
    ) -> impl Future<Output = Option<Continuation>> {
        let target = self.path.address();
        #[cfg(ruststream_alloc)]
        {
            ready(publish_lost(
                reaction,
                &self.subscription,
                target,
                self.input,
                NoFreeRecordError::new(CAP).into(),
                || FailedMessage::new(target, payload, headers),
            ))
        }
        #[cfg(not(ruststream_alloc))]
        async move {
            // The failure is reacted to where it stands, on a slot of this call, so a copy that
            // finds the store full never waits for another one's reaction.
            let mut failed = FailureSlot::<1>::new();
            // The cause names the failure and carries the capacity as its code, because an error
            // of this variant is a `Copy` cause rather than the typed error with a message.
            let cause = Cause::new(
                "no free record in the store of retry copies",
                i32::try_from(CAP).unwrap_or(i32::MAX),
            );
            publish_lost(
                reaction,
                &self.subscription,
                target,
                self.input,
                BoxError::from(cause),
                &mut failed,
                || FailedMessage::new(target, payload, headers),
            );
            failed.react().await;
            None
        }
    }

    fn fill(&self, record: usize, payload: &[u8], headers: Headers) {
        self.record(record, |held| {
            held.payload.clear();
            #[cfg(ruststream_alloc)]
            held.payload.extend_from_slice(payload);
            // A payload past the buffer cannot leave: the record remembers, and the copy is
            // refused when its turn comes.
            #[cfg(not(ruststream_alloc))]
            {
                held.oversize = held.payload.extend_from_slice(payload).is_err();
            }
            held.headers = headers;
            held.options = None;
        });
    }

    fn arm(&self, record: usize, delay: Duration, reaction: Option<Reaction>) {
        let due = self.clock.deadline(delay);
        self.record(record, |held| {
            held.reaction = reaction;
            held.held = Held::Due(due);
        });
        self.armed.raise();
        self.wake();
    }

    fn send_now(
        &self,
        record: usize,
        destination: Option<&str>,
        reaction: Option<Reaction>,
    ) -> impl Future<Output = Option<Continuation>> {
        self.record(record, |held| {
            held.reaction = reaction;
            held.to_destination = destination.is_some();
            if let Some(destination) = destination {
                held.destination.clear();
                // A destination is held whole by the mount or built into this room at startup.
                #[cfg(ruststream_alloc)]
                held.destination.push_str(destination);
                #[cfg(not(ruststream_alloc))]
                {
                    held.oversize |= held.destination.push_str(destination).is_err();
                }
            }
        });
        self.send(record)
    }

    fn free(&self, record: usize) {
        let payload = self.record(record, |held| {
            held.reaction = None;
            #[cfg(not(ruststream_alloc))]
            {
                held.oversize = false;
            }
            mem::take(&mut held.payload)
        });
        self.give_back(record, payload, None);
    }
}

impl<Cx, Path: PrepareCopy<Cx>, const CAP: usize, Timers: Schedule> PrepareKept<Cx>
    for CopyStore<Path, CAP, Timers>
{
    fn prepare<M: Settle>(
        &self,
        record: usize,
        destination: Option<&str>,
        delivery: &KeptDelivery<'_, M, Cx>,
    ) {
        let destination = destination.unwrap_or_else(|| self.path.address());
        self.record(record, |held| {
            self.path.prepare(
                &mut held.payload,
                &mut held.headers,
                &mut held.options,
                destination,
                delivery,
            );
        });
    }

    fn spill<M: Settle>(
        &self,
        payload: &[u8],
        headers: Headers,
        destination: Option<&str>,
        delivery: &KeptDelivery<'_, M, Cx>,
    ) -> Spilled<Path::Options> {
        #[cfg(ruststream_alloc)]
        let mut spilled = Spilled {
            payload: PayloadBuf::from(payload),
            headers,
            options: None,
        };
        // A payload past the buffer cannot be held whole, and a cut copy would pass for the
        // original: the copy remembers, and is refused when it is sent.
        #[cfg(not(ruststream_alloc))]
        let mut spilled = {
            let mut copy = PayloadBuf::new();
            let oversize = copy.extend_from_slice(payload).is_err();
            Spilled {
                payload: copy,
                headers,
                options: None,
                oversize,
            }
        };
        self.path.prepare(
            &mut spilled.payload,
            &mut spilled.headers,
            &mut spilled.options,
            destination.unwrap_or_else(|| self.path.address()),
            delivery,
        );
        spilled
    }
}

#[cfg(all(test, ruststream_alloc))]
mod tests {
    use alloc::boxed::Box;
    use alloc::string::{String, ToString};
    use alloc::sync::Arc;
    use alloc::vec::Vec;
    use core::cell::{Cell, RefCell};
    use core::future::ready;
    use core::pin::{Pin, pin};
    use core::sync::atomic::{AtomicU32, AtomicU64, Ordering};
    use core::task::{Context, Poll, Waker};
    use core::time::Duration;

    use ruststream_core::{HeaderMap, Lend, OutgoingMessage, PublishMessageError};
    use ruststream_runtime::{
        Clock, CopyPublisher, Detach, ErrorHook, Failures, KeptCopies, Reaction, Refusal,
    };

    // The store on embassy's clock polls embassy's timer, whose queue lives in the executor.
    use embassy_executor as _;

    use super::CopyStore;
    use crate::clock::Schedule;

    /// A clock the test moves by hand, counting how often its timers are polled and its time
    /// is read.
    #[derive(Clone, Default)]
    struct Probe {
        now: Arc<AtomicU64>,
        polls: Arc<AtomicU32>,
        reads: Arc<AtomicU32>,
    }

    impl Probe {
        fn advance_to(&self, at: u64) {
            self.now.store(at, Ordering::Relaxed);
        }

        fn polls(&self) -> u32 {
            self.polls.load(Ordering::Relaxed)
        }

        fn reads(&self) -> u32 {
            self.reads.load(Ordering::Relaxed)
        }
    }

    /// A timer of the probe clock.
    struct ProbeSleep {
        at: u64,
        clock: Probe,
    }

    impl Future for ProbeSleep {
        type Output = ();

        fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<()> {
            self.clock.polls.fetch_add(1, Ordering::Relaxed);
            if self.clock.now.load(Ordering::Relaxed) >= self.at {
                Poll::Ready(())
            } else {
                Poll::Pending
            }
        }
    }

    impl Clock for Probe {
        type Instant = u64;
        type Sleep = ProbeSleep;

        fn now(&self) -> u64 {
            self.reads.fetch_add(1, Ordering::Relaxed);
            self.now.load(Ordering::Relaxed)
        }

        fn sleep(&self, duration: Duration) -> ProbeSleep {
            self.sleep_until(self.deadline(duration))
        }

        fn sleep_until(&self, at: u64) -> ProbeSleep {
            ProbeSleep {
                at,
                clock: self.clone(),
            }
        }
    }

    impl Schedule for Probe {
        fn deadline(&self, after: Duration) -> u64 {
            self.now() + u64::try_from(after.as_millis()).unwrap_or(u64::MAX)
        }
    }

    /// The app's host beside the store, counting the continuations a failed publish leaves.
    #[derive(Default)]
    struct Beside {
        boxed: Cell<usize>,
    }

    impl Detach for Beside {
        fn detach_boxed(&self, _work: Pin<Box<dyn Future<Output = ()>>>) {
            self.boxed.set(self.boxed.get() + 1);
        }

        fn detach<Work>(&self, _work: Work)
        where
            Work: Future<Output = ()> + 'static,
        {
        }

        fn detach_after<Work>(&self, _delay: Duration, _work: Work)
        where
            Work: Future<Output = ()> + 'static,
        {
        }
    }

    /// A copy path that records what it publishes.
    #[derive(Default)]
    struct Recorded(RefCell<Vec<(String, Vec<u8>)>>);

    impl CopyPublisher for &Recorded {
        type Options = ();

        fn address(&self) -> &'static str {
            "jobs"
        }

        fn publish_copy(
            &self,
            destination: &str,
            payload: &[u8],
            _headers: HeaderMap,
            _options: Option<&()>,
        ) -> impl Future<Output = Result<(), Refusal>> {
            self.0
                .borrow_mut()
                .push((destination.into(), payload.to_vec()));
            ready(Ok(()))
        }
    }

    /// A copy path whose broker refuses every copy.
    struct Refusing;

    impl CopyPublisher for Refusing {
        type Options = ();

        fn address(&self) -> &'static str {
            "jobs"
        }

        fn publish_copy(
            &self,
            destination: &str,
            payload: &[u8],
            headers: HeaderMap,
            _options: Option<&()>,
        ) -> impl Future<Output = Result<(), Refusal>> {
            // A publisher answers a refusal with the message it was handed, as the broker
            // crates' do.
            let msg = OutgoingMessage::assembled(destination, payload, headers);
            ready(Err(Refusal::returned::<Lend>(
                PublishMessageError::with_message("the subscription is full", msg),
            )))
        }
    }

    /// A reaction that only needs to exist.
    fn reaction() -> Reaction {
        Reaction::new(ErrorHook::new(async move |_failures: Failures| {}), None)
    }

    fn keep<Path: CopyPublisher, const CAP: usize>(
        store: &CopyStore<Path, CAP, Probe>,
        payload: &[u8],
        delay: u64,
    ) {
        let record = store.take_record().expect("a free record");
        store.fill(record, payload, HeaderMap::new());
        store.arm(record, Duration::from_millis(delay), None);
    }

    #[test]
    fn waiting_copies_cost_one_timer_per_wake_and_leave_once_due() {
        let clock = Probe::default();
        let sent = Recorded::default();
        let store = CopyStore::<_, 3, _>::on_clock(&sent, Arc::from("jobs"), clock.clone());
        for (payload, delay) in [(b"a", 10), (b"b", 20), (b"c", 30)] {
            keep(&store, payload, delay);
        }
        let beside = Beside::default();
        let mut driver = pin!(store.drive(&beside));
        let mut cx = Context::from_waker(Waker::noop());
        assert_eq!(driver.as_mut().poll(&mut cx), Poll::Pending);
        let (polled, read) = (clock.polls(), clock.reads());
        for _ in 0..10 {
            // The registration's lanes wake the task on every delivery.
            assert_eq!(driver.as_mut().poll(&mut cx), Poll::Pending);
        }
        assert_eq!(
            (clock.polls() - polled, clock.reads() - read),
            (10, 0),
            "timer polls and scans of the records over ten wakes with three copies waiting"
        );
        clock.advance_to(20);
        assert_eq!(driver.as_mut().poll(&mut cx), Poll::Pending);
        assert_eq!(
            *sent.0.borrow(),
            [
                ("jobs".into(), b"a".to_vec()),
                ("jobs".into(), b"b".to_vec())
            ],
            "copies published once due, to the subscription's address"
        );
        assert!(
            store.take_record().is_some() && store.take_record().is_some(),
            "the published copies gave their records back"
        );
        assert!(
            store.take_record().is_none(),
            "the waiting copy keeps its record"
        );
    }

    #[test]
    fn a_closed_store_ends_once_its_last_copy_left() {
        let clock = Probe::default();
        let sent = Recorded::default();
        let store = CopyStore::<_, 3, _>::on_clock(&sent, Arc::from("jobs"), clock.clone());
        keep(&store, b"a", 5);
        let beside = Beside::default();
        let mut driver = pin!(store.drive(&beside));
        let mut cx = Context::from_waker(Waker::noop());
        store.close();
        assert_eq!(
            driver.as_mut().poll(&mut cx),
            Poll::Pending,
            "a closed store waits for the copy it holds"
        );
        clock.advance_to(5);
        assert_eq!(driver.as_mut().poll(&mut cx), Poll::Ready(()));
        assert_eq!(
            sent.0.borrow().len(),
            1,
            "the last copy left before the end"
        );
    }

    #[test]
    fn a_freed_record_is_room_again_and_sends_nothing() {
        let clock = Probe::default();
        let sent = Recorded::default();
        let store = CopyStore::<_, 3, _>::on_clock(&sent, Arc::from("jobs"), clock);
        let taken = [(); 3].map(|()| store.take_record().expect("a free record"));
        assert!(store.take_record().is_none(), "three records, all taken");
        store.fill(taken[0], b"a", HeaderMap::new());
        store.free(taken[0]);
        assert_eq!(store.take_record(), Some(taken[0]), "the freed record");
        store.close();
        let beside = Beside::default();
        let mut driver = pin!(store.drive(&beside));
        let mut cx = Context::from_waker(Waker::noop());
        for record in taken {
            store.free(record);
        }
        assert_eq!(driver.as_mut().poll(&mut cx), Poll::Ready(()));
        assert!(sent.0.borrow().is_empty(), "nothing published");
    }

    /// A copy path whose broker refuses the first `refusals` copies and takes the rest.
    struct Flaky {
        refusals: Cell<u32>,
        sent: Recorded,
    }

    impl CopyPublisher for &Flaky {
        type Options = ();

        fn address(&self) -> &'static str {
            "jobs"
        }

        fn publish_copy(
            &self,
            destination: &str,
            payload: &[u8],
            _headers: HeaderMap,
            _options: Option<&()>,
        ) -> impl Future<Output = Result<(), Refusal>> {
            if self.refusals.get() > 0 {
                self.refusals.set(self.refusals.get() - 1);
                return ready(Err(Refusal::new("the subscription is full")));
            }
            self.sent
                .0
                .borrow_mut()
                .push((destination.into(), payload.to_vec()));
            ready(Ok(()))
        }
    }

    #[test]
    fn a_copy_its_broker_refuses_is_lost_and_its_record_is_free_at_once() {
        let clock = Probe::default();
        let flaky = Flaky {
            refusals: Cell::new(1),
            sent: Recorded::default(),
        };
        let store = CopyStore::<_, 3, _>::on_clock(&flaky, Arc::from("jobs"), clock.clone());
        keep(&store, b"a", 5);
        let beside = Beside::default();
        let mut driver = pin!(store.drive(&beside));
        let mut cx = Context::from_waker(Waker::noop());
        assert_eq!(driver.as_mut().poll(&mut cx), Poll::Pending);
        clock.advance_to(5);
        assert_eq!(driver.as_mut().poll(&mut cx), Poll::Pending);
        assert!(
            flaky.sent.0.borrow().is_empty(),
            "refused, and not tried again"
        );
        assert_eq!(beside.boxed.get(), 0, "no continuation was registered");
        assert!(
            [(); 3]
                .map(|()| store.take_record())
                .iter()
                .all(Option::is_some),
            "the refused copy gave its record back"
        );
    }

    #[test]
    fn a_refused_copy_hands_its_error_to_the_continuation_its_record_held() {
        let clock = Probe::default();
        let store = CopyStore::<_, 3, _>::on_clock(Refusing, Arc::from("jobs"), clock.clone());
        let record = store.take_record().expect("a free record");
        store.fill(record, b"a", HeaderMap::new());
        store.arm(record, Duration::from_millis(5), Some(reaction()));
        let beside = Beside::default();
        let mut driver = pin!(store.drive(&beside));
        let mut cx = Context::from_waker(Waker::noop());
        assert_eq!(driver.as_mut().poll(&mut cx), Poll::Pending);
        clock.advance_to(5);
        assert_eq!(driver.as_mut().poll(&mut cx), Poll::Pending);
        assert_eq!(
            beside.boxed.get(),
            1,
            "the continuation runs beside the driver"
        );
    }

    #[test]
    fn a_dead_letter_copy_the_broker_refuses_hands_its_error_to_the_continuation() {
        let store = CopyStore::<_, 3, _>::on_clock(Refusing, Arc::from("jobs"), Probe::default());
        let record = store.take_record().expect("a free record");
        store.fill(record, b"a", HeaderMap::new());
        let mut sending = pin!(store.send_now(record, Some("jobs.dead"), Some(reaction()),));
        let mut cx = Context::from_waker(Waker::noop());
        assert!(
            matches!(sending.as_mut().poll(&mut cx), Poll::Ready(Some(_))),
            "the failure comes back as a continuation for the caller to run"
        );
    }

    #[test]
    fn a_full_store_reports_no_free_record_with_the_copy_it_could_not_keep() {
        let store = CopyStore::<_, 1, _>::on_clock(Refusing, Arc::from("jobs"), Probe::default());
        assert!(store.take_record().is_some());
        assert!(store.take_record().is_none(), "every record is taken");
        let reaction = embassy_futures::block_on(store.full(
            Some(bytes::Bytes::from_static(b"a")),
            HeaderMap::new(),
            Some(Reaction::new(
                ErrorHook::new(async move |failures: Failures| {
                    for failed in failures {
                        assert_eq!(
                            failed.error().to_string(),
                            "no free record in the store of retry copies (capacity 1)"
                        );
                        assert_eq!(
                            failed.message().payload().map(AsRef::as_ref),
                            Some(&b"a"[..])
                        );
                        assert_eq!(failed.message().destination(), "jobs");
                    }
                }),
                None,
            )),
        ));
        embassy_futures::block_on(reaction.expect("a continuation"));
        assert!(
            embassy_futures::block_on(store.full(None, HeaderMap::new(), None)).is_none(),
            "without a continuation the failure is a warning and nothing runs"
        );
    }

    /// The allocations of publishing one warm copy of `payload` with two headers through `path`,
    /// with a reaction registered or none.
    fn allocations_of_a_send<Path: CopyPublisher>(path: Path, with_reaction: bool) -> u64 {
        let store = CopyStore::<_, 1, _>::on_clock(path, Arc::from("jobs"), Probe::default());
        let mut headers = HeaderMap::new();
        headers.insert("a", "1");
        headers.insert("b", "2");
        let mut measured = 0;
        // The first pass warms the record's buffers; the second is the one read.
        for pass in 0..2 {
            let record = store.take_record().expect("a free record");
            store.fill(record, &[7; 64], headers.clone());
            let reaction = with_reaction.then(reaction);
            let info = allocation_counter::measure(|| {
                let follow = embassy_futures::block_on(store.send_now(record, None, reaction));
                drop(follow);
            });
            if pass == 1 {
                measured = info.count_total;
            }
        }
        measured
    }

    #[test]
    fn a_refused_copy_costs_a_registered_error_the_payload_and_nothing_else() {
        let plain = allocations_of_a_send(Refusing, false);
        let watched = allocations_of_a_send(Refusing, true);
        assert_eq!(
            watched,
            plain + 3,
            "the publisher hands the message back with its headers, so no header map is cloned; a \
             registered error costs the payload the record lends, the destination of the failed \
             message and the continuation that runs the error: {plain} against {watched}"
        );
    }

    #[test]
    fn a_published_copy_costs_a_registered_reaction_nothing() {
        let sent = Recorded::default();
        let plain = allocations_of_a_send(&sent, false);
        let watched = allocations_of_a_send(&sent, true);
        assert_eq!(
            watched, plain,
            "a publish consumes the headers and nothing is cloned for a reaction: a refused \
             publish returns the message instead"
        );
    }

    #[test]
    fn a_closed_store_waits_for_a_record_a_settle_has_taken() {
        let clock = Probe::default();
        let sent = Recorded::default();
        let store = CopyStore::<_, 3, _>::on_clock(&sent, Arc::from("jobs"), clock);
        let taken = store.take_record().expect("a free record");
        let beside = Beside::default();
        let mut driver = pin!(store.drive(&beside));
        let mut cx = Context::from_waker(Waker::noop());
        store.close();

        assert_eq!(
            driver.as_mut().poll(&mut cx),
            Poll::Pending,
            "the settle that took the record may still arm a copy in it"
        );
        store.free(taken);
        assert_eq!(driver.as_mut().poll(&mut cx), Poll::Ready(()));
    }
}

#[cfg(all(test, not(ruststream_alloc)))]
mod fixed_tests {
    use core::cell::{Cell, RefCell};
    use core::fmt::Write;
    use core::future::{Future, ready};
    use core::pin::{Pin, pin};
    use core::sync::atomic::{AtomicUsize, Ordering};
    use core::task::{Context, Poll, Waker};
    use core::time::Duration;

    use ruststream_core::{
        AckError, Headers, IncomingMessage, PAYLOAD_CAPACITY, PayloadBuf, Settle,
    };
    use ruststream_runtime::{
        Clock, Continuation, CopyPublisher, Detach, ErrorHook, Failures, KeptCopies, KeptDelivery,
        PrepareCopy, PrepareKept, Reaction, Refusal,
    };

    use super::{CopyStore, diagnostic_name};
    use crate::clock::Schedule;

    /// A clock the test moves by hand.
    #[derive(Default)]
    struct Hand(Cell<u64>);

    /// A timer of the hand clock.
    struct HandSleep<'hand> {
        at: u64,
        hand: &'hand Hand,
    }

    impl Future for HandSleep<'_> {
        type Output = ();

        fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<()> {
            if self.hand.0.get() >= self.at {
                Poll::Ready(())
            } else {
                Poll::Pending
            }
        }
    }

    impl<'hand> Clock for &'hand Hand {
        type Instant = u64;
        type Sleep = HandSleep<'hand>;

        fn now(&self) -> u64 {
            self.0.get()
        }

        fn sleep(&self, duration: Duration) -> HandSleep<'hand> {
            self.sleep_until(self.deadline(duration))
        }

        fn sleep_until(&self, at: u64) -> HandSleep<'hand> {
            HandSleep { at, hand: self }
        }
    }

    impl Schedule for &Hand {
        fn deadline(&self, after: Duration) -> u64 {
            self.0.get() + u64::try_from(after.as_millis()).unwrap_or(u64::MAX)
        }
    }

    /// A copy path that takes every copy and remembers the last one.
    #[derive(Default)]
    struct Taking {
        sent: Cell<usize>,
        last: RefCell<heapless::Vec<u8, 16>>,
        destination: RefCell<heapless::String<16>>,
    }

    impl CopyPublisher for &Taking {
        type Options = ();

        fn address(&self) -> &'static str {
            "jobs"
        }

        fn publish_copy(
            &self,
            destination: &str,
            payload: &[u8],
            _headers: Headers,
            _options: Option<&()>,
        ) -> impl Future<Output = Result<(), Refusal>> {
            self.sent.set(self.sent.get() + 1);
            let mut last = self.last.borrow_mut();
            last.clear();
            let _ = last.extend_from_slice(&payload[..payload.len().min(16)]);
            let mut named = self.destination.borrow_mut();
            named.clear();
            let _ = named.push_str(destination);
            ready(Ok(()))
        }
    }

    /// The app's host beside the store: it takes every continuation and runs none.
    struct Beside;

    impl Detach for Beside {
        fn detach(&self, _work: Continuation) -> impl Future<Output = ()> {
            ready(())
        }

        fn detach_after<Work>(&self, _delay: Duration, _work: Work)
        where
            Work: Future<Output = ()> + 'static,
        {
        }
    }

    /// A reaction that counts the failures it reads into `seen`.
    fn counting(seen: &'static AtomicUsize) -> Reaction {
        Reaction::new(
            ErrorHook::new(async move |failures: &Failures| {
                seen.fetch_add(failures.len(), Ordering::SeqCst);
            }),
            None,
        )
    }

    #[test]
    fn two_copies_failing_back_to_back_on_a_full_store_both_reach_their_reaction() {
        static SEEN: AtomicUsize = AtomicUsize::new(0);
        let (hand, path) = (Hand::default(), Taking::default());
        let store = CopyStore::<_, 1, _>::on_clock(&path, diagnostic_name("jobs"), &hand);
        assert!(store.take_record().is_some());
        assert!(store.take_record().is_none(), "every record is taken");
        for _ in 0..2 {
            let follow = embassy_futures::block_on(store.full(
                Some(PayloadBuf::new()),
                Headers::new(),
                Some(counting(&SEEN)),
            ));
            assert!(follow.is_none(), "the reaction ran where the failure stood");
        }
        assert_eq!(
            SEEN.load(Ordering::SeqCst),
            2,
            "both failed copies reached a reaction"
        );
    }

    #[test]
    fn a_full_store_reports_no_free_record_with_the_copy_it_could_not_keep() {
        static CHECKED: AtomicUsize = AtomicUsize::new(0);
        let (hand, path) = (Hand::default(), Taking::default());
        let store = CopyStore::<_, 1, _>::on_clock(&path, diagnostic_name("jobs"), &hand);
        assert!(store.take_record().is_some());
        let mut payload = PayloadBuf::new();
        payload.extend_from_slice(b"a").expect("fits");
        let reaction = Reaction::new(
            ErrorHook::new(async move |failures: &Failures| {
                for failed in failures {
                    let mut said = heapless::String::<96>::new();
                    write!(said, "{}", failed.error()).expect("fits");
                    assert_eq!(
                        said.as_str(),
                        "no free record in the store of retry copies (code 1)"
                    );
                    assert_eq!(
                        failed.message().payload().map(AsRef::as_ref),
                        Some(&b"a"[..])
                    );
                    assert_eq!(failed.message().destination(), Some("jobs"));
                    CHECKED.fetch_add(1, Ordering::SeqCst);
                }
            }),
            None,
        );
        embassy_futures::block_on(store.full(Some(payload), Headers::new(), Some(reaction)));
        assert_eq!(
            CHECKED.load(Ordering::SeqCst),
            1,
            "the reaction read the failure"
        );
        assert_eq!(path.sent.get(), 0, "nothing was published");
    }

    #[test]
    fn a_copy_laid_out_in_a_record_leaves_once_due_and_frees_the_record() {
        let (hand, path) = (Hand::default(), Taking::default());
        let store = CopyStore::<_, 1, _>::on_clock(&path, diagnostic_name("jobs"), &hand);
        let record = store.take_record().expect("a free record");
        store.fill(record, b"abc", Headers::new());
        store.arm(record, Duration::from_millis(5), None);
        let mut driver = pin!(store.drive(&Beside));
        let mut cx = Context::from_waker(Waker::noop());
        assert_eq!(driver.as_mut().poll(&mut cx), Poll::Pending);
        assert_eq!(path.sent.get(), 0, "not due yet");
        hand.0.set(5);
        assert_eq!(driver.as_mut().poll(&mut cx), Poll::Pending);
        assert_eq!(path.sent.get(), 1);
        assert_eq!(path.last.borrow().as_slice(), b"abc");
        assert_eq!(path.destination.borrow().as_str(), "jobs");
        assert!(
            store.take_record().is_some(),
            "the sent copy gave its record back"
        );
    }

    #[test]
    fn a_dead_letter_copy_leaves_at_once_for_its_destination() {
        let (hand, path) = (Hand::default(), Taking::default());
        let store = CopyStore::<_, 1, _>::on_clock(&path, diagnostic_name("jobs"), &hand);
        let record = store.take_record().expect("a free record");
        store.fill(record, b"dead", Headers::new());
        let follow = embassy_futures::block_on(store.send_now(record, Some("jobs.dead"), None));
        assert!(follow.is_none());
        assert_eq!(path.destination.borrow().as_str(), "jobs.dead");
        assert_eq!(path.last.borrow().as_slice(), b"dead");
        assert!(
            store.take_record().is_some(),
            "the sent copy gave its record back"
        );
    }

    #[test]
    fn a_payload_past_the_record_is_refused_to_the_reaction_and_never_published() {
        static SEEN: AtomicUsize = AtomicUsize::new(0);
        let (hand, path) = (Hand::default(), Taking::default());
        let store = CopyStore::<_, 1, _>::on_clock(&path, diagnostic_name("jobs"), &hand);
        let record = store.take_record().expect("a free record");
        store.fill(record, &[7; PAYLOAD_CAPACITY + 1], Headers::new());
        embassy_futures::block_on(store.send_now(record, None, Some(counting(&SEEN))));
        assert_eq!(path.sent.get(), 0, "an oversize copy cannot leave");
        assert_eq!(
            SEEN.load(Ordering::SeqCst),
            1,
            "the reaction read the refusal"
        );
    }

    /// A delivery with nothing to say, the one a copy is made of.
    struct Delivered(Headers);

    impl IncomingMessage for Delivered {
        fn payload(&self) -> &[u8] {
            b""
        }

        fn headers(&self) -> &Headers {
            &self.0
        }
    }

    impl Settle for Delivered {
        async fn ack(self) -> Result<(), AckError> {
            Ok(())
        }

        async fn nack(self, _requeue: bool) -> Result<(), AckError> {
            Ok(())
        }
    }

    /// The mount site's transforms, none: the copy leaves as it was filled.
    impl PrepareCopy<()> for &Taking {
        fn prepare<M: Settle>(
            &self,
            _payload: &mut PayloadBuf,
            _headers: &mut Headers,
            _options: &mut Option<()>,
            _destination: &str,
            _delivery: &KeptDelivery<'_, M, ()>,
        ) {
        }
    }

    #[test]
    fn a_payload_past_the_buffer_of_a_spilled_copy_is_refused_to_the_reaction_and_never_published()
    {
        static SEEN: AtomicUsize = AtomicUsize::new(0);
        let (hand, path) = (Hand::default(), Taking::default());
        let store = CopyStore::<_, 1, _>::on_clock(&path, diagnostic_name("jobs"), &hand);
        let delivered = Delivered(Headers::new());
        let spilled = PrepareKept::<()>::spill(
            &store,
            &[7; PAYLOAD_CAPACITY + 1],
            Headers::new(),
            Some("jobs.dead"),
            &KeptDelivery {
                subscription: "jobs",
                message: &delivered,
                build_cx: |_: &Delivered| (),
            },
        );
        let follow = embassy_futures::block_on(store.send_spilled(
            spilled,
            Some("jobs.dead"),
            Some(counting(&SEEN)),
        ));
        assert!(follow.is_none());
        assert_eq!(path.sent.get(), 0, "an oversize copy cannot leave");
        assert_eq!(
            SEEN.load(Ordering::SeqCst),
            1,
            "the reaction read the refusal"
        );
    }

    #[test]
    fn a_spilled_copy_that_fits_the_buffer_leaves_whole() {
        let (hand, path) = (Hand::default(), Taking::default());
        let store = CopyStore::<_, 1, _>::on_clock(&path, diagnostic_name("jobs"), &hand);
        let delivered = Delivered(Headers::new());
        let spilled = PrepareKept::<()>::spill(
            &store,
            &[7; PAYLOAD_CAPACITY],
            Headers::new(),
            Some("jobs.dead"),
            &KeptDelivery {
                subscription: "jobs",
                message: &delivered,
                build_cx: |_: &Delivered| (),
            },
        );
        embassy_futures::block_on(store.send_spilled(spilled, Some("jobs.dead"), None));
        assert_eq!(path.sent.get(), 1);
        assert_eq!(path.destination.borrow().as_str(), "jobs.dead");
    }
}
