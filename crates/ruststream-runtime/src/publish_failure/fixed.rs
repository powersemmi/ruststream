//! The failure plane of a build without `alloc`.
//!
//! A failed publish leaves its message in the lane's [`FailureSlot`], and the handler's `.error`
//! closure reads it there: the closure takes `&Failures`, a view of the slot's fixed buffer, so
//! the failures are stored once and the closure's own room stays that of any continuation.
//! The lane awaits [`FailureSlot::react`] after the delivery is settled and before it takes the
//! next one.

use core::{fmt, iter};

use dyn_utils::DynObject;
use dyn_utils::storage::Raw;
use heapless::{Vec, VecView};
use ruststream_core::diag::warn;

use super::{Failed, FailedMessage};
use crate::handler::{Continuation, MaybeSend};
use crate::inline::{InlineFuture, Room};
use crate::lifecycle::BoxError;

/// The failures an `.error` closure reads: one for a single delivery, every failed element for a
/// uniform batch, in the batch's order. A view of the lane's fixed buffer.
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
pub struct ErrorHook(DynObject<dyn Reactor, Raw<HOOK_ROOM>>);

impl ErrorHook {
    /// A hook that runs `react` on the failures.
    ///
    /// A closure whose captures take more than the room, or are aligned more than a pointer, is a
    /// compile error that names the closure's size and the room.
    #[must_use]
    pub fn new<React>(react: React) -> Self
    where
        React: for<'failures> AsyncFnOnce(&'failures Failures) + MaybeSend + 'static,
    {
        const {
            assert!(
                align_of::<Owned<React>>() <= align_of::<usize>(),
                "an `.error` closure may not be aligned more than a pointer"
            );
            let _spare = HOOK_ROOM - size_of::<Owned<React>>();
        }
        Self(DynObject::new(Owned(Some(react))))
    }

    /// The reaction to `failures`, which the caller awaits.
    pub fn react(mut self, failures: &Failures) -> ReactionFuture<'_> {
        self.0.react(failures)
    }
}

impl fmt::Debug for ErrorHook {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ErrorHook").finish_non_exhaustive()
    }
}

/// The lane's fixed buffer of failures, and the reaction waiting to read them.
///
/// `ELEMENTS` is the most failures one reaction reads: 1 for a single delivery, the mount's batch
/// size for a batch. The lane owns it for the duration of a delivery.
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
/// Machinery; never named directly.
#[doc(hidden)]
pub struct Reaction {
    error: ErrorHook,
    then: Option<Continuation>,
}

impl fmt::Debug for Reaction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Reaction").finish_non_exhaustive()
    }
}

impl Reaction {
    /// A reaction handing a failure to `error` and a success to `then`.
    #[must_use]
    pub const fn new(error: ErrorHook, then: Option<Continuation>) -> Self {
        Self { error, then }
    }

    /// The continuation registered beside `.error`, which a delivery that made no publish runs
    /// once it is settled.
    pub(crate) fn into_then(self) -> Option<Continuation> {
        self.then
    }

    /// What follows a publish that succeeded: the `.then` continuation, if one was registered.
    #[must_use]
    pub fn succeeded(self) -> Option<Continuation> {
        self.then
    }

    /// What follows a publish that failed: the failure waits in `slot` for the `.error` closure.
    fn failed<const ELEMENTS: usize>(
        self,
        error: BoxError,
        message: FailedMessage,
        slot: &mut FailureSlot<ELEMENTS>,
    ) {
        slot.park(self.error, iter::once(Failed::new(error, message)));
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
/// failure from `slot`, which the lane awaits with [`FailureSlot::react`], or, with none, a
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
    let Some(reaction) = reaction else {
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

#[cfg(test)]
mod tests {
    use core::pin::pin;
    use core::sync::atomic::{AtomicUsize, Ordering};
    use core::task::{Context, Waker};

    use super::{ErrorHook, FailedMessage, FailureSlot, Failures, Reaction, publish_lost};
    use crate::HeaderSet;
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
}
