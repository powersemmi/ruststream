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

use bytes::BytesMut;
use embassy_sync::waitqueue::WakerRegistration;
use ruststream_core::{HeaderMap, LocalSettle};
use ruststream_runtime::{
    BoxError, Clock, CopyPublisher, KeptCopies, KeptDelivery, PrepareCopy, PrepareKept,
};
use tracing::{debug, warn};

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
    /// The spare record, free, and offered to a delivery that waits for a record.
    Offered,
}

/// How long a copy its broker refused waits before it is published again: nothing tells the store
/// when room appears, so it asks on a timer, short enough to be invisible to a retry delay.
const REFUSED_RETRY: Duration = Duration::from_millis(50);

/// One copy: its bytes in a buffer the record keeps from copy to copy, its headers, and the
/// per-message settings the mount site's transforms wrote on it.
struct Record<At, Options> {
    held: Held<At>,
    payload: BytesMut,
    headers: HeaderMap,
    options: Option<Options>,
    /// The headers as they were before the last publish took them, so a refused publish can put
    /// them back: overwritten in place per publish, which allocates only while it grows.
    backup: HeaderMap,
    /// Where the copy is bound when it is not the subscription: the buffer is reused from copy
    /// to copy, like the payload's.
    destination: String,
    /// Whether the copy is bound for `destination`.
    to_destination: bool,
}

/// The retry copies of one registration, in `CAP` records.
///
/// A failing delivery takes a record and fills it before the delivery settles; a full store
/// holds the delivery until a record frees (back-pressure). [`drive`](Self::drive), joined with the
/// registration's lanes, publishes each copy once it is due: it waits for the earliest one on a
/// single timer, so a wake costs one timer poll however many copies wait. A record reuses its
/// buffer, so once a buffer has grown to the largest copy, keeping one allocates nothing.
///
/// A copy the broker refuses when it is due (a subscription holding every message it has room
/// for) stays in its record and is published again after a short wait, so it is never dropped
/// while the store is open. That can wait on a delivery that holds the room and itself waits for a
/// record: the store then keeps one record beyond `CAP`, the spare, and offers it to that delivery.
/// The delivery's copy goes into the spare, its original is dropped, and the room the original
/// held is what the refused copy needs. The spare costs one record's memory per store and
/// nothing per delivery.
pub(crate) struct CopyStore<Path: CopyPublisher, const CAP: usize, Timers: Clock = EmbassyClock> {
    path: Path,
    /// The subscription the copies are of, for diagnostics.
    subscription: Arc<str>,
    records: RefCell<[Record<Timers::Instant, Path::Options>; CAP]>,
    /// The record at index `CAP`: taken only by a delivery the store offers it to.
    spare: RefCell<Record<Timers::Instant, Path::Options>>,
    /// Set when a record was armed since the driver last looked.
    armed: Cell<bool>,
    /// Set once the registration's lanes stopped: the driver ends with the last copy sent.
    closed: Cell<bool>,
    waker: RefCell<Option<Waker>>,
    /// The app task's waker, registered by every delivery waiting for a free record: the lanes
    /// are children of one task and are polled with its waker, so one slot serves them all.
    waiting: RefCell<WakerRegistration>,
    clock: Timers,
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
            spare: RefCell::new(Self::empty_record()),
            armed: Cell::new(false),
            closed: Cell::new(false),
            waker: RefCell::new(None),
            waiting: RefCell::new(WakerRegistration::new()),
            clock,
        }
    }

    fn empty_record() -> Record<Timers::Instant, Path::Options> {
        Record {
            held: Held::Free,
            payload: BytesMut::new(),
            headers: HeaderMap::new(),
            options: None,
            backup: HeaderMap::new(),
            destination: String::new(),
            to_destination: false,
        }
    }

    /// Runs `with` on the record at `index`; `CAP` is the spare.
    fn record<Out>(
        &self,
        index: usize,
        with: impl FnOnce(&mut Record<Timers::Instant, Path::Options>) -> Out,
    ) -> Out {
        if index == CAP {
            with(&mut self.spare.borrow_mut())
        } else {
            with(&mut self.records.borrow_mut()[index])
        }
    }

    /// Stops the driver once every copy the store holds has been published.
    pub(crate) fn close(&self) {
        self.closed.set(true);
        self.wake();
    }

    /// Publishes every copy once it is due, until [`close`](Self::close) and the last copy sent.
    pub(crate) async fn drive(&self) {
        let mut timer: Option<(Timers::Instant, Timers::Sleep)> = None;
        while let Some(record) = self.next_due(&mut timer).await {
            if let Err(err) = self.send(record).await {
                warn!(
                    target: "ruststream::dispatch",
                    subscription = %self.subscription,
                    destination = %self.path.address(),
                    error = %err,
                    "publishing the retry copy failed; message lost",
                );
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
            for index in 0..=CAP {
                match self.record(index, |record| record.held) {
                    Held::Due(at) if at <= now => return Poll::Ready(Some(index)),
                    Held::Due(at) => earliest = Some(earliest.map_or(at, |next| next.min(at))),
                    Held::Taken | Held::Sending => busy = true,
                    Held::Free | Held::Offered => {}
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
    /// A copy that its broker refuses stays in the record and is tried again after
    /// [`REFUSED_RETRY`], unless the store is closed; the call then returns `Ok`. The headers are
    /// kept in the record for that, overwritten in place, so a refusal does not lose them and a
    /// publish allocates nothing once the record has held headers of that size.
    async fn send(&self, record: usize) -> Result<(), BoxError> {
        let (payload, headers, options, destination) = self.record(record, |held| {
            held.held = Held::Sending;
            held.backup.clone_from(&held.headers);
            (
                mem::take(&mut held.payload),
                mem::take(&mut held.headers),
                held.options.take(),
                held.to_destination
                    .then(|| mem::take(&mut held.destination)),
            )
        });
        let target = destination
            .as_deref()
            .unwrap_or_else(|| self.path.address());
        let sent = self
            .path
            .publish_copy(target, &payload, headers, options.as_ref())
            .await;
        match sent {
            Err(err) if !self.closed.get() => {
                debug!(
                    target: "ruststream::dispatch",
                    subscription = %self.subscription,
                    destination = %target,
                    error = %err,
                    "the retry copy was refused; keeping it for another try",
                );
                self.keep_refused(record, payload, options, destination);
                Ok(())
            }
            sent => {
                self.give_back(record, payload, destination);
                sent
            }
        }
    }

    /// Puts a refused copy back in `record`, due again shortly, and offers the spare to a
    /// delivery waiting for a record: the room it holds is what the copy needs.
    fn keep_refused(
        &self,
        record: usize,
        payload: BytesMut,
        options: Option<Path::Options>,
        destination: Option<String>,
    ) {
        let retry_at = self.clock.deadline(REFUSED_RETRY);
        self.record(record, |held| {
            held.payload = payload;
            mem::swap(&mut held.headers, &mut held.backup);
            held.options = options;
            if let Some(destination) = destination {
                held.destination = destination;
            }
            held.held = Held::Due(retry_at);
        });
        self.armed.set(true);
        let offered = self.spare.borrow().held == Held::Free && self.waiting.borrow().occupied();
        if offered {
            self.spare.borrow_mut().held = Held::Offered;
            self.waiting.borrow_mut().wake();
        }
        self.wake();
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
        // A record is free again: the delivery waiting for one takes it, not the spare.
        if record != CAP && self.spare.borrow().held == Held::Offered {
            self.spare.borrow_mut().held = Held::Free;
        }
        self.waiting.borrow_mut().wake();
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
    fn take_record(&self) -> Option<usize> {
        let mut records = self.records.borrow_mut();
        let index = records
            .iter()
            .position(|record| record.held == Held::Free)?;
        records[index].held = Held::Taken;
        Some(index)
    }

    fn wait_record(&self) -> impl Future<Output = usize> {
        poll_fn(|cx| {
            if let Some(record) = self.take_record() {
                return Poll::Ready(record);
            }
            if self.spare.borrow().held == Held::Offered {
                self.spare.borrow_mut().held = Held::Taken;
                return Poll::Ready(CAP);
            }
            self.waiting.borrow_mut().register(cx.waker());
            Poll::Pending
        })
    }

    fn fill(&self, record: usize, payload: &[u8], headers: HeaderMap) {
        self.record(record, |held| {
            held.payload.clear();
            held.payload.extend_from_slice(payload);
            held.headers = headers;
            held.options = None;
        });
    }

    fn arm(&self, record: usize, delay: Duration) {
        let due = self.clock.deadline(delay);
        self.record(record, |held| held.held = Held::Due(due));
        self.armed.set(true);
        self.wake();
    }

    fn send_now(
        &self,
        record: usize,
        destination: Option<&str>,
    ) -> impl Future<Output = Result<(), BoxError>> {
        self.record(record, |held| {
            held.to_destination = destination.is_some();
            if let Some(destination) = destination {
                held.destination.clear();
                held.destination.push_str(destination);
            }
        });
        self.send(record)
    }

    fn free(&self, record: usize) {
        let payload = self.record(record, |held| mem::take(&mut held.payload));
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
}

#[cfg(test)]
mod tests {
    use alloc::string::String;
    use alloc::sync::Arc;
    use alloc::task::Wake;
    use alloc::vec::Vec;
    use core::cell::{Cell, RefCell};
    use core::future::ready;
    use core::pin::{Pin, pin};
    use core::sync::atomic::{AtomicU32, AtomicU64, Ordering};
    use core::task::{Context, Poll, Waker};
    use core::time::Duration;

    use ruststream_core::HeaderMap;
    use ruststream_runtime::{BoxError, Clock, CopyPublisher, KeptCopies};

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

    fn keep<Path: CopyPublisher, const CAP: usize>(
        store: &CopyStore<Path, CAP, Probe>,
        payload: &[u8],
        delay: u64,
    ) {
        let record = store.take_record().expect("a free record");
        store.fill(record, payload, HeaderMap::new());
        store.arm(record, Duration::from_millis(delay));
    }

    #[test]
    fn waiting_copies_cost_one_timer_per_wake_and_leave_once_due() {
        let clock = Probe::default();
        let sent = Recorded::default();
        let store = CopyStore::<_, 3, _>::on_clock(&sent, Arc::from("jobs"), clock.clone());
        for (payload, delay) in [(b"a", 10), (b"b", 20), (b"c", 30)] {
            keep(&store, payload, delay);
        }
        let mut driver = pin!(store.drive());
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
        let mut driver = pin!(store.drive());
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
    fn a_full_store_wakes_the_waiting_delivery_once_a_record_frees() {
        struct Woken(AtomicU32);

        impl Wake for Woken {
            fn wake(self: Arc<Self>) {
                self.0.fetch_add(1, Ordering::Relaxed);
            }
        }

        let sent = Recorded::default();
        let store = CopyStore::<_, 1, _>::on_clock(&sent, Arc::from("jobs"), Probe::default());
        let taken = store.take_record().expect("a free record");
        let woken = Arc::new(Woken(AtomicU32::new(0)));
        let waker = Waker::from(Arc::clone(&woken));
        let mut cx = Context::from_waker(&waker);
        let mut waiting = pin!(store.wait_record());

        assert_eq!(waiting.as_mut().poll(&mut cx), Poll::Pending);
        assert_eq!(waiting.as_mut().poll(&mut cx), Poll::Pending);
        assert_eq!(woken.0.load(Ordering::Relaxed), 0, "nothing freed yet");
        store.free(taken);
        assert_eq!(
            woken.0.load(Ordering::Relaxed),
            1,
            "woken once, however often polled"
        );
        assert_eq!(waiting.as_mut().poll(&mut cx), Poll::Ready(taken));
    }

    #[test]
    fn lanes_waiting_on_one_store_each_get_a_record_as_they_free() {
        let sent = Recorded::default();
        let store = CopyStore::<_, 2, _>::on_clock(&sent, Arc::from("jobs"), Probe::default());
        let taken = [(); 2].map(|()| store.take_record().expect("a free record"));
        // Two lanes of one task: both are polled with the task's one waker.
        let mut cx = Context::from_waker(Waker::noop());
        let mut first = pin!(store.wait_record());
        let mut second = pin!(store.wait_record());
        assert_eq!(first.as_mut().poll(&mut cx), Poll::Pending);
        assert_eq!(second.as_mut().poll(&mut cx), Poll::Pending);

        store.free(taken[0]);
        let got = first.as_mut().poll(&mut cx);
        assert_eq!(
            got,
            Poll::Ready(taken[0]),
            "the first lane takes the freed record"
        );
        assert_eq!(
            second.as_mut().poll(&mut cx),
            Poll::Pending,
            "no record is left"
        );
        store.free(taken[1]);
        assert_eq!(second.as_mut().poll(&mut cx), Poll::Ready(taken[1]));
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
        let mut driver = pin!(store.drive());
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
    fn a_copy_its_broker_refuses_stays_kept_and_leaves_once_the_broker_takes_it() {
        let clock = Probe::default();
        let flaky = Flaky {
            refusals: Cell::new(2),
            sent: Recorded::default(),
        };
        let store = CopyStore::<_, 3, _>::on_clock(&flaky, Arc::from("jobs"), clock.clone());
        keep(&store, b"a", 5);
        let mut driver = pin!(store.drive());
        let mut cx = Context::from_waker(Waker::noop());
        assert_eq!(driver.as_mut().poll(&mut cx), Poll::Pending);
        clock.advance_to(5);
        assert_eq!(driver.as_mut().poll(&mut cx), Poll::Pending);
        assert!(flaky.sent.0.borrow().is_empty(), "refused once");
        assert!(
            [(); 3]
                .map(|()| store.take_record())
                .iter()
                .filter(|taken| taken.is_some())
                .count()
                == 2,
            "the refused copy keeps its record"
        );
        clock.advance_to(100);
        assert_eq!(driver.as_mut().poll(&mut cx), Poll::Pending);
        clock.advance_to(200);
        assert_eq!(driver.as_mut().poll(&mut cx), Poll::Pending);
        assert_eq!(
            *flaky.sent.0.borrow(),
            [("jobs".into(), b"a".to_vec())],
            "published once the broker took it"
        );
    }

    #[test]
    fn a_dead_letter_copy_the_broker_refuses_stays_kept_for_its_destination() {
        let clock = Probe::default();
        let flaky = Flaky {
            refusals: Cell::new(1),
            sent: Recorded::default(),
        };
        let store = CopyStore::<_, 3, _>::on_clock(&flaky, Arc::from("jobs"), clock.clone());
        let record = store.take_record().expect("a free record");
        store.fill(record, b"a", HeaderMap::new());
        let mut driver = pin!(store.drive());
        let mut cx = Context::from_waker(Waker::noop());
        let mut sending = pin!(store.send_now(record, Some("jobs.dead")));
        assert!(
            matches!(sending.as_mut().poll(&mut cx), Poll::Ready(Ok(()))),
            "a refused copy is kept, not reported lost"
        );
        assert!(flaky.sent.0.borrow().is_empty(), "refused once");
        assert_eq!(driver.as_mut().poll(&mut cx), Poll::Pending);
        clock.advance_to(100);
        assert_eq!(driver.as_mut().poll(&mut cx), Poll::Pending);
        assert_eq!(
            *flaky.sent.0.borrow(),
            [("jobs.dead".into(), b"a".to_vec())],
            "published to the destination it was bound for"
        );
    }

    #[test]
    fn a_copy_refused_after_the_store_closed_is_lost_and_its_record_freed() {
        let clock = Probe::default();
        let store = CopyStore::<_, 3, _>::on_clock(Refusing, Arc::from("jobs"), clock.clone());
        keep(&store, b"a", 5);
        let mut driver = pin!(store.drive());
        let mut cx = Context::from_waker(Waker::noop());
        store.close();
        assert_eq!(driver.as_mut().poll(&mut cx), Poll::Pending);
        clock.advance_to(5);

        assert_eq!(
            driver.as_mut().poll(&mut cx),
            Poll::Ready(()),
            "a closed store holds nothing once the refused copy is gone"
        );
        assert!(
            [(); 3]
                .map(|()| store.take_record())
                .iter()
                .all(Option::is_some),
            "the refused copy gave its record back"
        );
    }

    #[test]
    fn a_delivery_waiting_for_a_record_is_offered_the_spare_when_a_due_copy_is_refused() {
        let clock = Probe::default();
        let flaky = Flaky {
            refusals: Cell::new(1),
            sent: Recorded::default(),
        };
        let store = CopyStore::<_, 1, _>::on_clock(&flaky, Arc::from("jobs"), clock.clone());
        keep(&store, b"a", 5);
        let mut cx = Context::from_waker(Waker::noop());
        let mut waiting = pin!(store.wait_record());
        assert_eq!(waiting.as_mut().poll(&mut cx), Poll::Pending);
        let mut driver = pin!(store.drive());
        assert_eq!(driver.as_mut().poll(&mut cx), Poll::Pending);
        clock.advance_to(5);
        assert_eq!(driver.as_mut().poll(&mut cx), Poll::Pending);

        let Poll::Ready(spare) = waiting.as_mut().poll(&mut cx) else {
            panic!("the waiting delivery gets the spare record");
        };
        store.fill(spare, b"b", HeaderMap::new());
        store.arm(spare, Duration::from_millis(5));
        clock.advance_to(100);
        assert_eq!(driver.as_mut().poll(&mut cx), Poll::Pending);
        assert_eq!(
            *flaky.sent.0.borrow(),
            [
                ("jobs".into(), b"a".to_vec()),
                ("jobs".into(), b"b".to_vec())
            ],
            "both copies left, the refused one kept meanwhile"
        );
    }

    #[test]
    fn a_closed_store_waits_for_a_record_a_settle_has_taken() {
        let clock = Probe::default();
        let sent = Recorded::default();
        let store = CopyStore::<_, 3, _>::on_clock(&sent, Arc::from("jobs"), clock);
        let taken = store.take_record().expect("a free record");
        let mut driver = pin!(store.drive());
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
