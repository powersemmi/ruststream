//! The store of one registration's retry copies: `CAP` records polled by the registration's own
//! loop on the clock, so a copy waits as data rather than as a future of its own.

// The store holds publishers that are not `Send`, and its driver runs on the app's one task.
#![allow(clippy::future_not_send)]

use alloc::string::String;
use alloc::sync::Arc;
use core::array;
use core::cell::{Cell, RefCell};
use core::future::poll_fn;
use core::mem;
use core::pin::Pin;
use core::task::{Poll, Waker};
use core::time::Duration;

use bytes::{Bytes, BytesMut};
use ruststream_core::{HeaderMap, LocalSettle};
use ruststream_runtime::{
    Clock, Continuation, CopyPublisher, FailedMessage, KeptCopies, KeptDelivery, LocalDetach,
    NoFreeRecordError, PrepareCopy, PrepareKept, Reaction, publish_done, publish_lost,
};

use crate::clock::{EmbassyClock, Schedule};

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
    payload: BytesMut,
    headers: HeaderMap,
    options: Option<Options>,
}

/// One copy: its bytes in a buffer the record keeps from copy to copy, its headers, the
/// per-message settings the mount site's transforms wrote on it, and the handler's `.error`
/// continuation when the outcome that made the copy carried one.
struct Record<At, Options> {
    held: Held<At>,
    payload: BytesMut,
    headers: HeaderMap,
    options: Option<Options>,
    /// Where the copy is bound when it is not the subscription: the buffer is reused from copy
    /// to copy, like the payload's.
    destination: String,
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
    subscription: Arc<str>,
    records: RefCell<[Record<Timers::Instant, Path::Options>; CAP]>,
    /// Set when a record was armed since the driver last looked.
    armed: Cell<bool>,
    /// Set once the registration's lanes stopped: the driver ends with the last copy sent.
    closed: Cell<bool>,
    waker: RefCell<Option<Waker>>,
    clock: Timers,
    /// The name of the type the handler receives, which a lost copy's diagnostic calls its
    /// message.
    input: &'static str,
}

impl<Path: CopyPublisher, const CAP: usize> CopyStore<Path, CAP> {
    /// A store of `subscription`'s copies on embassy's clock, leaving through `path`.
    pub(crate) fn new(path: Path, subscription: Arc<str>) -> Self {
        Self::on_clock(path, subscription, EmbassyClock)
    }
}

impl<Path: CopyPublisher, const CAP: usize, Timers: Schedule> CopyStore<Path, CAP, Timers> {
    /// A store on `clock`.
    pub(crate) fn on_clock(path: Path, subscription: Arc<str>, clock: Timers) -> Self {
        Self {
            path,
            subscription,
            records: RefCell::new(array::from_fn(|_| Self::empty_record())),
            armed: Cell::new(false),
            closed: Cell::new(false),
            waker: RefCell::new(None),
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
            payload: BytesMut::new(),
            headers: HeaderMap::new(),
            options: None,
            destination: String::new(),
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
        with(&mut self.records.borrow_mut()[index])
    }

    /// Stops the driver once every copy the store holds has been published.
    pub(crate) fn close(&self) {
        self.closed.set(true);
        self.wake();
    }

    /// Publishes every copy once it is due, until [`close`](Self::close) and the last copy sent.
    /// What a failed publish's `.error` continuation makes of it runs on `host`, beside the
    /// driver.
    pub(crate) async fn drive<Host: LocalDetach>(&self, host: &Host) {
        let mut timer: Option<(Timers::Instant, Timers::Sleep)> = None;
        while let Some(record) = self.next_due(&mut timer).await {
            if let Some(reaction) = self.send(record).await {
                host.detach_boxed(reaction);
            }
        }
    }

    /// Waits for the next due copy, `None` once the store is closed and empty.
    async fn next_due(
        &self,
        timer: &mut Option<(Timers::Instant, Timers::Sleep)>,
    ) -> Option<usize> {
        poll_fn(|cx| {
            let mut waker = self.waker.borrow_mut();
            if !waker
                .as_ref()
                .is_some_and(|known| known.will_wake(cx.waker()))
            {
                *waker = Some(cx.waker().clone());
            }
            drop(waker);
            // A new copy may be due before the one the timer waits for.
            if self.armed.replace(false) {
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
            if self.closed.get() && !busy {
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
    /// the failure is returned for the caller to run, and without a continuation a warning names
    /// the failure. The headers are copied for the continuation only where one is registered.
    async fn send(&self, record: usize) -> Option<Continuation> {
        let (payload, headers, options, destination, reaction) = self.record(record, |held| {
            held.held = Held::Sending;
            (
                mem::take(&mut held.payload),
                mem::take(&mut held.headers),
                held.options.take(),
                held.to_destination
                    .then(|| mem::take(&mut held.destination)),
                held.reaction.take(),
            )
        });
        let target = destination
            .as_deref()
            .unwrap_or_else(|| self.path.address());
        let failed_headers = reaction.is_some().then(|| headers.clone());
        let sent = self
            .path
            .publish_copy(target, &payload, headers, options.as_ref())
            .await;
        let follow = match sent {
            Ok(()) => publish_done(reaction),
            Err(err) => publish_lost(
                reaction,
                &self.subscription,
                target,
                self.input,
                err,
                || {
                    FailedMessage::new(
                        target,
                        Bytes::copy_from_slice(&payload),
                        failed_headers.unwrap_or_default(),
                    )
                },
            ),
        };
        self.give_back(record, payload, destination);
        follow
    }

    /// Frees `record`, keeping `payload`'s buffer for the next copy.
    fn give_back(&self, record: usize, mut payload: BytesMut, destination: Option<String>) {
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
        if let Some(waker) = self.waker.borrow().as_ref() {
            waker.wake_by_ref();
        }
    }
}

impl<Path: CopyPublisher, const CAP: usize, Timers: Schedule> KeptCopies
    for CopyStore<Path, CAP, Timers>
{
    type Spilled = Spilled<Path::Options>;

    fn capacity(&self) -> usize {
        CAP
    }

    fn input(&self) -> &'static str {
        self.input
    }

    fn take_record(&self) -> Option<usize> {
        let mut records = self.records.borrow_mut();
        let index = records
            .iter()
            .position(|record| record.held == Held::Free)?;
        records[index].held = Held::Taken;
        Some(index)
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
        } = spilled;
        async move {
            let target = destination.unwrap_or_else(|| self.path.address());
            let failed_headers = reaction.is_some().then(|| headers.clone());
            let sent = self
                .path
                .publish_copy(target, &payload, headers, options.as_ref())
                .await;
            match sent {
                Ok(()) => publish_done(reaction),
                Err(err) => publish_lost(
                    reaction,
                    &self.subscription,
                    target,
                    self.input,
                    err,
                    || {
                        FailedMessage::new(
                            target,
                            payload.freeze(),
                            failed_headers.unwrap_or_default(),
                        )
                    },
                ),
            }
        }
    }

    fn full(
        &self,
        payload: Option<Bytes>,
        headers: HeaderMap,
        reaction: Option<Reaction>,
    ) -> Option<Continuation> {
        let target = self.path.address();
        publish_lost(
            reaction,
            &self.subscription,
            target,
            self.input,
            NoFreeRecordError::new(CAP).into(),
            || FailedMessage::new(target, payload.unwrap_or_default(), headers),
        )
    }

    fn fill(&self, record: usize, payload: &[u8], headers: HeaderMap) {
        self.record(record, |held| {
            held.payload.clear();
            held.payload.extend_from_slice(payload);
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
        self.armed.set(true);
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
                held.destination.push_str(destination);
            }
        });
        self.send(record)
    }

    fn free(&self, record: usize) {
        let payload = self.record(record, |held| {
            held.reaction = None;
            mem::take(&mut held.payload)
        });
        self.give_back(record, payload, None);
    }
}

impl<Cx, Path: PrepareCopy<Cx>, const CAP: usize, Timers: Schedule> PrepareKept<Cx>
    for CopyStore<Path, CAP, Timers>
{
    fn prepare<M: LocalSettle>(
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

    fn spill<M: LocalSettle>(
        &self,
        payload: &[u8],
        headers: HeaderMap,
        destination: Option<&str>,
        delivery: &KeptDelivery<'_, M, Cx>,
    ) -> Spilled<Path::Options> {
        let mut spilled = Spilled {
            payload: BytesMut::from(payload),
            headers,
            options: None,
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

#[cfg(test)]
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

    use ruststream_core::HeaderMap;
    use ruststream_runtime::{
        BoxError, Clock, CopyPublisher, ErrorHook, KeptCopies, LocalDetach, Reaction,
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

    impl LocalDetach for Beside {
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
        ) -> impl Future<Output = Result<(), BoxError>> {
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
            _destination: &str,
            _payload: &[u8],
            _headers: HeaderMap,
            _options: Option<&()>,
        ) -> impl Future<Output = Result<(), BoxError>> {
            ready(Err(BoxError::from("the subscription is full")))
        }
    }

    /// A reaction that only needs to exist.
    fn reaction() -> Reaction {
        Reaction::new(ErrorHook::new(async move |_error, _lost| {}), None)
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
        ) -> impl Future<Output = Result<(), BoxError>> {
            if self.refusals.get() > 0 {
                self.refusals.set(self.refusals.get() - 1);
                return ready(Err(BoxError::from("the subscription is full")));
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
        let reaction = store.full(
            Some(bytes::Bytes::from_static(b"a")),
            HeaderMap::new(),
            Some(Reaction::new(
                ErrorHook::new(async move |error, lost| {
                    assert_eq!(
                        error.to_string(),
                        "no free record in the store of retry copies (capacity 1)"
                    );
                    assert_eq!(lost.payload().as_ref(), b"a");
                    assert_eq!(lost.destination(), "jobs");
                }),
                None,
            )),
        );
        embassy_futures::block_on(reaction.expect("a continuation"));
        assert!(
            store.full(None, HeaderMap::new(), None).is_none(),
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
    fn a_failed_publish_copies_the_message_only_for_a_registered_error() {
        let plain = allocations_of_a_send(Refusing, false);
        let watched = allocations_of_a_send(Refusing, true);
        assert!(
            watched >= plain + 2,
            "the failed message (payload and headers) is copied for the continuation alone: \
             {plain} against {watched}"
        );
    }

    #[test]
    fn a_published_copy_costs_a_registered_reaction_one_header_map_and_no_payload() {
        let sent = Recorded::default();
        let plain = allocations_of_a_send(&sent, false);
        let watched = allocations_of_a_send(&sent, true);
        assert_eq!(
            watched,
            plain + 1,
            "a publish consumes the headers, so a registered reaction keeps one map; the payload \
             is copied only once the publish failed"
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
