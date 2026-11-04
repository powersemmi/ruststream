//! The failure plane of a build without `alloc`.
//!
//! A failed publish leaves its message in the worker's [`FailureSlot`], and the handler's `.error`
//! closure reads it there: the closure takes `&Failures`, a view of the slot's fixed buffer, so
//! the failures are stored once and the closure's own room stays that of any continuation.
//! The worker awaits [`FailureSlot::react`] after the delivery is settled and before it takes the
//! next one.

use core::cell::{Cell, RefCell};
use core::future::poll_fn;
use core::task::{Poll, Waker};
use core::{fmt, iter};

use dyn_utils::DynObject;
use futures::future::join;
use heapless::{Vec, VecView};
use ruststream_core::diag::warn;

use super::{Failed, FailedMessage};
use crate::handler::{Continuation, MaybeSend};
use crate::inline::{InlineFuture, ROOM_ALIGN, Room};
use crate::lifecycle::BoxError;

/// The failures an `.error` closure reads: one for a single delivery, every failed element for a
/// uniform batch, in the batch's order. A view of the worker's fixed buffer.
pub type Failures = VecView<Failed>;

/// The room of an `.error` closure, in bytes. A closure whose captures do not fit is a compile
/// error that names the closure and this room.
const HOOK_ROOM: usize = 128;

/// The room of the future an `.error` closure returns, in bytes: the closure's captures move into
/// it, beside whatever the body keeps across an `.await`.
const REACTION_ROOM: usize = 256;

/// The future of one reaction, which borrows the failures it reads.
type ReactionFuture<'failures> = InlineFuture<'failures, Room<REACTION_ROOM>>;

/// The `.error` closure, erased and held in place.
#[dyn_utils::dyn_object]
trait Reactor {
    fn react<'failures>(&mut self, failures: &'failures Failures) -> ReactionFuture<'failures>;
}

/// The closure as the one registration holds it. `Option` because a one-shot closure moves out of
/// the object on its call, and the object cannot be consumed through the vtable.
struct Owned<React>(Option<React>);

impl<React> Reactor for Owned<React>
where
    React: for<'failures> AsyncFnOnce(&'failures Failures) + MaybeSend + 'static,
{
    fn react<'failures>(&mut self, failures: &'failures Failures) -> ReactionFuture<'failures> {
        // `ErrorHook::react` consumes the hook, so the closure is there exactly once.
        let react = self.0.take().expect("an `.error` closure reacts once");
        InlineFuture::new(react(failures))
    }
}

/// The `.error` continuation of one [`HandlerOutcome`](crate::HandlerOutcome), until a publish
/// the runtime makes for it fails. Machinery; never named directly.
#[doc(hidden)]
pub struct ErrorHook {
    react: DynObject<dyn Reactor, Room<HOOK_ROOM>>,
    /// Whether the closure is the one a uniform batch outcome shares between its elements, riding
    /// the last element until the mount's fan takes it.
    shared: bool,
}

impl ErrorHook {
    /// A hook that runs `react` on the failures.
    ///
    /// A closure whose captures take more than the room, or are aligned to more than
    /// [`ROOM_ALIGN`] bytes, is a compile error that names the closure's size and the room.
    #[must_use]
    pub fn new<React>(react: React) -> Self
    where
        React: for<'failures> AsyncFnOnce(&'failures Failures) + MaybeSend + 'static,
    {
        const {
            assert!(
                align_of::<Owned<React>>() <= ROOM_ALIGN,
                "an `.error` closure may not be aligned to more than 8 bytes: keep a value of a \
                 larger alignment in a `static` and capture a reference to it"
            );
            let _spare = HOOK_ROOM - size_of::<Owned<React>>();
        }
        Self {
            react: DynObject::new(Owned(Some(react))),
            shared: false,
        }
    }

    /// This hook as the one a uniform batch outcome shares between its elements.
    pub(crate) const fn shared(mut self) -> Self {
        self.shared = true;
        self
    }

    /// Whether a uniform batch outcome shares this hook between its elements.
    pub(crate) const fn is_shared(&self) -> bool {
        self.shared
    }

    /// The reaction to `failures`, which the caller awaits.
    pub fn react(mut self, failures: &Failures) -> ReactionFuture<'_> {
        self.react.react(failures)
    }
}

impl fmt::Debug for ErrorHook {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ErrorHook").finish_non_exhaustive()
    }
}

/// The worker's fixed buffer of failures, and the reaction waiting to read them.
///
/// `ELEMENTS` is the most failures one reaction reads: 1 for a single delivery, the mount's batch
/// size for a batch. The worker owns it for the duration of a delivery.
#[doc(hidden)]
pub struct FailureSlot<const ELEMENTS: usize = 1> {
    failures: Vec<Failed, ELEMENTS>,
    hook: Option<ErrorHook>,
}

impl<const ELEMENTS: usize> FailureSlot<ELEMENTS> {
    /// An empty slot.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            failures: Vec::new(),
            hook: None,
        }
    }

    /// Whether a reaction waits in the slot.
    #[must_use]
    pub const fn is_pending(&self) -> bool {
        self.hook.is_some()
    }

    /// Leaves `failures` in the slot for `hook` to read.
    ///
    /// # Panics
    ///
    /// When the slot already holds a reaction or the failures do not fit. Neither happens in a
    /// build the runtime assembled: a delivery carries one [`Reaction`], and the one publish the
    /// runtime makes for it (the reply, which settles the delivery by ack, or the retry or
    /// dead-letter copy, which settles it by nack, never both) consumes it; a batch's slot is
    /// as large as the batch.
    pub(crate) fn park(&mut self, hook: ErrorHook, failures: impl IntoIterator<Item = Failed>) {
        assert!(
            self.hook.is_none(),
            "a delivery carries one reaction, which one publish consumes"
        );
        for failed in failures {
            assert!(
                self.failures.push(failed).is_ok(),
                "the failure buffer is as large as the batch"
            );
        }
        self.hook = Some(hook);
    }

    /// Runs the reaction that waits in the slot on the failures it holds, then empties the slot.
    /// Without a waiting reaction it returns at once.
    pub async fn react(&mut self) {
        if let Some(hook) = self.hook.take() {
            hook.react(self.failures.as_view()).await;
        }
        self.failures.clear();
    }
}

impl<const ELEMENTS: usize> Default for FailureSlot<ELEMENTS> {
    fn default() -> Self {
        Self::new()
    }
}

impl<const ELEMENTS: usize> fmt::Debug for FailureSlot<ELEMENTS> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FailureSlot")
            .field("failures", &self.failures.len())
            .field("pending", &self.hook.is_some())
            .finish()
    }
}

/// What a publish the runtime makes for a delivery hands its result to: the handler's `.error`
/// continuation for a failure, and the `.then` continuation registered beside it for a success.
///
/// With an `.error` registered, `.then` waits for the publish: it runs if the publish succeeded
/// and never if it failed, and `.error` runs if it failed and never if it succeeded. A delivery
/// that registered no `.error` has no reaction: its `.then` runs once the delivery is settled.
///
/// An element of a uniform batch outcome carries a part instead: the closure and the `.then`
/// stay in the mount's [`BatchFan`], and the part names the element, so the publish reports to
/// the fan. Machinery; never named directly.
#[doc(hidden)]
pub struct Reaction {
    share: Share,
}

/// Who holds the closure: the delivery's own reaction, or the fan of its batch.
enum Share {
    Whole {
        error: ErrorHook,
        then: Option<Continuation>,
    },
    Part(usize),
}

impl fmt::Debug for Reaction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Reaction").finish_non_exhaustive()
    }
}

/// What a delivery that made no publish owes its reaction.
pub(crate) enum Unpublished {
    /// The `.then` registered beside `.error`, to run once the delivery is settled.
    Then(Option<Continuation>),
    /// The element of a batch outcome: it reports to the fan that no publish failed.
    Part(usize),
}

impl Reaction {
    /// A reaction handing a failure to `error` and a success to `then`.
    #[must_use]
    pub const fn new(error: ErrorHook, then: Option<Continuation>) -> Self {
        Self {
            share: Share::Whole { error, then },
        }
    }

    /// The share of the batch element at `index` in the outcome its mount's [`BatchFan`] gathers.
    #[must_use]
    pub const fn part(index: usize) -> Self {
        Self {
            share: Share::Part(index),
        }
    }

    /// The element this reaction reports for, when it is a part of a batch outcome.
    #[must_use]
    pub const fn part_index(&self) -> Option<usize> {
        match self.share {
            Share::Part(index) => Some(index),
            Share::Whole { .. } => None,
        }
    }

    /// What a delivery that made no publish owes: its `.then`, or the report of its part.
    pub(crate) fn into_unpublished(self) -> Unpublished {
        match self.share {
            Share::Whole { then, .. } => Unpublished::Then(then),
            Share::Part(index) => Unpublished::Part(index),
        }
    }

    /// What follows a publish that succeeded: the `.then` continuation, if one was registered.
    /// A part has none: the fan holds it, and the holder of the part reports to the fan.
    #[must_use]
    pub fn succeeded(self) -> Option<Continuation> {
        match self.share {
            Share::Whole { then, .. } => then,
            Share::Part(_) => None,
        }
    }

    /// What follows a publish that failed: the failure waits in `slot` for the `.error` closure.
    /// A part has no closure to wait for: its holder reports to the fan.
    fn failed<const ELEMENTS: usize>(
        self,
        error: BoxError,
        message: FailedMessage,
        slot: &mut FailureSlot<ELEMENTS>,
    ) {
        if let Share::Whole { error: hook, .. } = self.share {
            slot.park(hook, iter::once(Failed::new(error, message)));
        }
    }
}

impl Reaction {
    /// What follows a batch publish that lost several replies: they wait in `slot`, in the
    /// batch's order, for the `.error` closure to read them all in one call. A part has no
    /// closure to wait for: its holder reports to the fan.
    pub(crate) fn failed_all<const ELEMENTS: usize>(
        self,
        lost: impl IntoIterator<Item = Failed>,
        slot: &mut FailureSlot<ELEMENTS>,
    ) {
        if let Share::Whole { error: hook, .. } = self.share {
            slot.park(hook, lost);
        }
    }
}

/// What follows a publish that succeeded: the `.then` continuation registered beside `.error`,
/// returned for the caller to run beside the delivery.
#[doc(hidden)]
#[must_use]
pub fn publish_done(reaction: Option<Reaction>) -> Option<Continuation> {
    reaction.and_then(Reaction::succeeded)
}

/// What happens to a copy or reply whose publish failed: the handler's `.error` closure reads the
/// failure from `slot`, which the worker awaits with [`FailureSlot::react`], or, with none, a
/// warning says the message is lost.
///
/// `message` builds the failed message, and is called only when there is a closure to hand it to.
#[doc(hidden)]
pub fn publish_lost<const ELEMENTS: usize>(
    reaction: Option<Reaction>,
    subscription: &str,
    destination: &str,
    input: &str,
    error: BoxError,
    slot: &mut FailureSlot<ELEMENTS>,
    message: impl FnOnce() -> FailedMessage,
) {
    // A part is the fan's: the store that holds the fan reports it there, and one that reaches
    // this function has no closure to hand its failure to.
    let Some(reaction) = reaction.filter(|reaction| reaction.part_index().is_none()) else {
        warn!(
            target: "ruststream::dispatch",
            subscription = %subscription,
            destination = %destination,
            message_type = %input,
            error = %error,
            "publishing the message failed; message lost",
        );
        return;
    };
    reaction.failed(error, message(), slot);
}

/// Where the elements of one batch outcome report their publishes: the worker for an element that
/// made none, the store for a copy it published or failed to publish.
///
/// The element that reports last runs the `.error` closure on every failure and, beside it, the
/// outcome's `.then` when an element's publish succeeded. Machinery; never named directly.
#[doc(hidden)]
pub trait Gathers {
    /// Takes the outcome's closure and `.then` for `elements` elements, waiting while an earlier
    /// outcome is still gathering.
    fn begin(
        &self,
        error: ErrorHook,
        then: Option<Continuation>,
        elements: usize,
    ) -> impl Future<Output = ()>;

    /// Reports the element at `index`: `Ok` for a publish that went out or none at all, the
    /// error and the message for one that failed. The last element runs the closure and the
    /// follow-up before it returns.
    fn report(
        &self,
        index: usize,
        outcome: Result<(), (BoxError, FailedMessage)>,
    ) -> impl Future<Output = ()>;
}

/// The gatherer of a store that has none: a single delivery has no batch outcome.
#[doc(hidden)]
#[derive(Debug, Clone, Copy)]
pub enum NoFan {}

// No value of the type exists, so no method is ever called.
#[allow(clippy::uninhabited_references, clippy::unused_async_trait_impl)]
impl Gathers for NoFan {
    async fn begin(&self, _error: ErrorHook, _then: Option<Continuation>, _elements: usize) {
        match *self {}
    }

    async fn report(&self, _index: usize, _outcome: Result<(), (BoxError, FailedMessage)>) {
        match *self {}
    }
}

/// What a batch outcome has gathered so far.
struct Gathered<const ELEMENTS: usize> {
    error: Option<ErrorHook>,
    then: Option<Continuation>,
    /// The failures reported so far, kept in the order of their elements.
    failures: Vec<Failed, ELEMENTS>,
    /// The element each failure belongs to, in step with `failures`.
    order: Vec<usize, ELEMENTS>,
    /// Elements that have not reported yet.
    pending: usize,
    /// Whether an element's publish went out, or none was made.
    sent: bool,
}

/// The `.error` and `.then` of one uniform batch outcome, shared by its elements: the closure
/// runs once, on every failure in the batch's order, after the last element has reported, and
/// the `.then` is handed back once if any element succeeded.
///
/// A mount holds one, for batches of at most `ELEMENTS`: the worker reports the elements that made
/// no publish and the store reports the copies it published. The closure reads the failures in
/// the fan's own buffer, so they are stored once. An outcome that registers an `.error` while an
/// earlier one still gathers waits in [`begin`](Gathers::begin), which is the limit of one
/// failure set per mount. Machinery; never named directly.
#[doc(hidden)]
pub struct BatchFan<const ELEMENTS: usize> {
    state: RefCell<Gathered<ELEMENTS>>,
    /// Set from `begin` until the closure has run and the failures are cleared.
    busy: Cell<bool>,
    /// The worker waiting in `begin`.
    waiting: RefCell<Option<Waker>>,
}

impl<const ELEMENTS: usize> BatchFan<ELEMENTS> {
    /// A fan with nothing to gather.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            state: RefCell::new(Gathered {
                error: None,
                then: None,
                failures: Vec::new(),
                order: Vec::new(),
                pending: 0,
                sent: false,
            }),
            busy: Cell::new(false),
            waiting: RefCell::new(None),
        }
    }
}

impl<const ELEMENTS: usize> Default for BatchFan<ELEMENTS> {
    fn default() -> Self {
        Self::new()
    }
}

impl<const ELEMENTS: usize> fmt::Debug for BatchFan<ELEMENTS> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BatchFan")
            .field("busy", &self.busy.get())
            .finish_non_exhaustive()
    }
}

impl<const ELEMENTS: usize> Gathers for BatchFan<ELEMENTS> {
    async fn begin(&self, error: ErrorHook, then: Option<Continuation>, elements: usize) {
        let mut hand_over = Some((error, then));
        poll_fn(|cx| {
            if self.busy.get() {
                let mut waiting = self.waiting.borrow_mut();
                if !waiting
                    .as_ref()
                    .is_some_and(|known| known.will_wake(cx.waker()))
                {
                    *waiting = Some(cx.waker().clone());
                }
                return Poll::Pending;
            }
            if let Some((error, then)) = hand_over.take() {
                self.busy.set(true);
                let mut gathered = self.state.borrow_mut();
                gathered.error = Some(error);
                gathered.then = then;
                gathered.pending = elements;
                gathered.sent = false;
            }
            Poll::Ready(())
        })
        .await;
    }

    // The borrow of the buffer lives across the closure on purpose, see below.
    #[allow(clippy::await_holding_refcell_ref)]
    async fn report(&self, index: usize, outcome: Result<(), (BoxError, FailedMessage)>) {
        let last = {
            let mut gathered = self.state.borrow_mut();
            match outcome {
                Ok(()) => gathered.sent = true,
                Err((error, message)) => {
                    let at = gathered
                        .order
                        .iter()
                        .position(|&earlier| earlier > index)
                        .unwrap_or(gathered.order.len());
                    // Neither can fail: an element reports once, and the buffers are as large as
                    // the batch.
                    let _ = gathered.order.insert(at, index);
                    let _ = gathered.failures.insert(at, Failed::new(error, message));
                }
            }
            gathered.pending = gathered.pending.saturating_sub(1);
            gathered.pending == 0
        };
        if !last {
            return;
        }
        // The last element runs the closure on the failures the fan holds and the outcome's
        // `.then` beside it, `.error` polled first, then frees the fan for the next outcome as
        // soon as the closure is done. They run in this future, not in layers of its own: a
        // registration's chain of futures is as deep as the recursion limit allows.
        let (error, then) = {
            let mut gathered = self.state.borrow_mut();
            let sent = gathered.sent;
            (gathered.error.take(), gathered.then.take().filter(|_| sent))
        };
        let react = async {
            {
                // Held across the closure's suspension points: the fan is busy, so nothing
                // writes the buffer meanwhile.
                let gathered = self.state.borrow();
                if let Some(error) = error.filter(|_| !gathered.failures.is_empty()) {
                    error.react(gathered.failures.as_view()).await;
                }
            }
            {
                let mut gathered = self.state.borrow_mut();
                gathered.failures.clear();
                gathered.order.clear();
            }
            self.busy.set(false);
            if let Some(waker) = self.waiting.borrow_mut().take() {
                waker.wake();
            }
        };
        let follow = async {
            if let Some(then) = then {
                then.await;
            }
        };
        join(react, follow).await;
    }
}

#[cfg(test)]
mod tests {
    use core::pin::pin;
    use core::sync::atomic::{AtomicUsize, Ordering};
    use core::task::{Context, Waker};

    use super::{
        BatchFan, ErrorHook, FailedMessage, FailureSlot, Failures, Gathers, Reaction, publish_lost,
    };
    use crate::HeaderSet;
    use crate::handler::Continuation;
    use crate::lifecycle::BoxError;

    /// Polls `work` once and returns whether it finished.
    fn finishes(work: impl Future<Output = ()>) -> bool {
        let mut work = pin!(work);
        work.as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_ready()
    }

    /// Folds the failures' destinations, `d1`, `d2`..., into the digits of `trail`, in the order
    /// the closure reads them.
    fn read_into(trail: &'static AtomicUsize, failures: &Failures) {
        for failed in failures {
            let digit = failed
                .message()
                .destination()
                .and_then(|name| name.strip_prefix('d'));
            let digit = digit
                .and_then(|digit| digit.parse::<usize>().ok())
                .unwrap_or(0);
            // One thread reads and writes it, so a plain load and store is the whole update.
            let at = trail.load(Ordering::SeqCst);
            trail.store(at * 10 + digit, Ordering::SeqCst);
        }
    }

    #[test]
    fn an_error_closure_reads_the_failure_the_slot_holds() {
        static TRAIL: AtomicUsize = AtomicUsize::new(0);
        let mut slot = FailureSlot::<1>::new();
        let hook = ErrorHook::new(async |failures: &Failures| read_into(&TRAIL, failures));
        let reaction = Reaction::new(hook, None);

        publish_lost(
            Some(reaction),
            "orders",
            "d3",
            "Order",
            BoxError::msg("down"),
            &mut slot,
            || FailedMessage::new("d3", None, HeaderSet::default()),
        );
        assert!(slot.is_pending());
        assert!(finishes(slot.react()));

        assert_eq!(TRAIL.load(Ordering::SeqCst), 3);
        assert!(!slot.is_pending());
    }

    #[test]
    fn a_publish_with_no_reaction_leaves_nothing_in_the_slot() {
        let mut slot = FailureSlot::<1>::new();

        publish_lost(
            None,
            "orders",
            "dest",
            "Order",
            BoxError::msg("down"),
            &mut slot,
            || unreachable!("no closure waits for the message"),
        );

        assert!(!slot.is_pending());
    }

    #[test]
    fn leaving_a_failure_and_reacting_to_it_allocates_nothing() {
        static SEEN: AtomicUsize = AtomicUsize::new(0);
        let counted = allocation_counter::measure(|| {
            let mut slot = FailureSlot::<1>::new();
            let hook = ErrorHook::new(async |failures: &Failures| read_into(&SEEN, failures));
            publish_lost(
                Some(Reaction::new(hook, None)),
                "orders",
                "d1",
                "Order",
                BoxError::msg("down"),
                &mut slot,
                || FailedMessage::new("d1", None, HeaderSet::default()),
            );
            assert!(finishes(slot.react()));
        });

        assert_eq!(counted.count_total, 0);
        assert_eq!(SEEN.load(Ordering::SeqCst), 1);
    }

    /// A report of a failed copy bound for `to`.
    fn lost(to: &str) -> Result<(), (BoxError, FailedMessage)> {
        Err((
            BoxError::msg("down"),
            FailedMessage::new(to, None, HeaderSet::default()),
        ))
    }

    /// Pending once, then ready: a closure that suspends while it reads the fan's buffer.
    struct YieldOnce(bool);

    impl Future for YieldOnce {
        type Output = ();

        fn poll(mut self: core::pin::Pin<&mut Self>, cx: &mut Context<'_>) -> core::task::Poll<()> {
            if self.0 {
                return core::task::Poll::Ready(());
            }
            self.0 = true;
            cx.waker().wake_by_ref();
            core::task::Poll::Pending
        }
    }

    #[test]
    fn a_fan_gathers_reports_from_two_places_at_two_times_and_runs_the_closure_once() {
        static TRAIL: AtomicUsize = AtomicUsize::new(0);
        static AFTER: AtomicUsize = AtomicUsize::new(0);
        let mut cx = Context::from_waker(Waker::noop());
        let fan = BatchFan::<4>::new();
        let error = ErrorHook::new(async |failures: &Failures| {
            // Suspends while it borrows the fan's own buffer.
            YieldOnce(false).await;
            read_into(&TRAIL, failures);
        });
        let then = Continuation::new(async {
            AFTER.fetch_add(1, Ordering::SeqCst);
        });
        assert!(
            pin!(fan.begin(error, Some(then), 4))
                .as_mut()
                .poll(&mut cx)
                .is_ready()
        );

        // The worker reports the element that published nothing; the store reports the copies as
        // they come due, out of the batch's order.
        assert!(
            pin!(fan.report(1, Ok(())))
                .as_mut()
                .poll(&mut cx)
                .is_ready()
        );
        assert!(
            pin!(fan.report(3, lost("d4")))
                .as_mut()
                .poll(&mut cx)
                .is_ready()
        );
        assert!(
            pin!(fan.report(0, lost("d1")))
                .as_mut()
                .poll(&mut cx)
                .is_ready()
        );
        assert_eq!(
            TRAIL.load(Ordering::SeqCst),
            0,
            "the last element has not reported"
        );

        // A second outcome waits while the first one runs its closure.
        let mut second = pin!(fan.begin(ErrorHook::new(async |_: &Failures| {}), None, 1));
        let mut last = pin!(fan.report(2, lost("d3")));
        assert!(
            last.as_mut().poll(&mut cx).is_pending(),
            "suspended inside the closure"
        );
        assert!(
            second.as_mut().poll(&mut cx).is_pending(),
            "the fan is busy"
        );
        assert!(
            last.as_mut().poll(&mut cx).is_ready(),
            "the closure finished"
        );
        assert!(
            second.as_mut().poll(&mut cx).is_ready(),
            "free once the closure has run"
        );

        assert_eq!(
            TRAIL.load(Ordering::SeqCst),
            134,
            "in the order of the batch"
        );
        assert_eq!(AFTER.load(Ordering::SeqCst), 1);
    }

    /// Pending until `flag` is set by someone else.
    struct Until(&'static AtomicUsize);

    impl Future for Until {
        type Output = ();

        fn poll(self: core::pin::Pin<&mut Self>, cx: &mut Context<'_>) -> core::task::Poll<()> {
            if self.0.load(Ordering::SeqCst) > 0 {
                return core::task::Poll::Ready(());
            }
            cx.waker().wake_by_ref();
            core::task::Poll::Pending
        }
    }

    #[test]
    fn the_error_closure_and_the_follow_up_of_a_batch_run_together() {
        static SIGNAL: AtomicUsize = AtomicUsize::new(0);
        let mut cx = Context::from_waker(Waker::noop());
        let fan = BatchFan::<2>::new();
        // The closure finishes only after the follow-up has run.
        let error = ErrorHook::new(async |_: &Failures| Until(&SIGNAL).await);
        let then = Continuation::new(async {
            SIGNAL.fetch_add(1, Ordering::SeqCst);
        });
        assert!(
            pin!(fan.begin(error, Some(then), 2))
                .as_mut()
                .poll(&mut cx)
                .is_ready()
        );
        assert!(
            pin!(fan.report(0, Ok(())))
                .as_mut()
                .poll(&mut cx)
                .is_ready()
        );

        let mut last = pin!(fan.report(1, lost("d2")));
        let mut polls = 0;
        while last.as_mut().poll(&mut cx).is_pending() {
            polls += 1;
            assert!(polls < 4, "the closure waits for a follow-up nobody runs");
        }

        assert_eq!(SIGNAL.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn a_batch_where_every_element_failed_has_no_follow_up() {
        static SEEN: AtomicUsize = AtomicUsize::new(0);
        static FOLLOWED: AtomicUsize = AtomicUsize::new(0);
        let mut cx = Context::from_waker(Waker::noop());
        let fan = BatchFan::<2>::new();
        let error = ErrorHook::new(async |failures: &Failures| read_into(&SEEN, failures));
        let then = Continuation::new(async {
            FOLLOWED.fetch_add(1, Ordering::SeqCst);
        });
        assert!(
            pin!(fan.begin(error, Some(then), 2))
                .as_mut()
                .poll(&mut cx)
                .is_ready()
        );

        assert!(
            pin!(fan.report(0, lost("d1")))
                .as_mut()
                .poll(&mut cx)
                .is_ready()
        );
        let mut last = pin!(fan.report(1, lost("d2")));
        assert!(
            last.as_mut().poll(&mut cx).is_ready(),
            "no suspension in the closure"
        );

        assert_eq!(FOLLOWED.load(Ordering::SeqCst), 0);
        assert_eq!(SEEN.load(Ordering::SeqCst), 12);
    }

    #[test]
    fn a_batch_without_a_failure_does_not_run_the_closure_and_runs_the_follow_up() {
        static FOLLOWED: AtomicUsize = AtomicUsize::new(0);
        let mut cx = Context::from_waker(Waker::noop());
        let fan = BatchFan::<2>::new();
        let error = ErrorHook::new(async |_: &Failures| panic!("nothing failed"));
        let then = Continuation::new(async {
            FOLLOWED.fetch_add(1, Ordering::SeqCst);
        });
        assert!(
            pin!(fan.begin(error, Some(then), 2))
                .as_mut()
                .poll(&mut cx)
                .is_ready()
        );

        assert!(
            pin!(fan.report(0, Ok(())))
                .as_mut()
                .poll(&mut cx)
                .is_ready()
        );
        let mut last = pin!(fan.report(1, Ok(())));
        assert!(last.as_mut().poll(&mut cx).is_ready(), "no suspension");

        assert_eq!(FOLLOWED.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn a_part_that_reaches_a_publisher_without_its_fan_leaves_nothing_to_react_to() {
        let mut slot = FailureSlot::<1>::new();

        publish_lost(
            Some(Reaction::part(2)),
            "orders",
            "dest",
            "Order",
            BoxError::msg("down"),
            &mut slot,
            || unreachable!("a part has no closure to hand the message to"),
        );

        assert!(!slot.is_pending());
        assert_eq!(Reaction::part(2).part_index(), Some(2));
    }
}
