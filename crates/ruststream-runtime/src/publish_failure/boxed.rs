//! The failure plane of a build with `alloc`: the closures and the failures live on the heap.

use alloc::boxed::Box;
use alloc::sync::Arc;
use alloc::vec::{self, Vec};
use core::cell::RefCell;
use core::error::Error as StdError;
use core::fmt;
use core::iter::FusedIterator;
use core::mem;
#[cfg(feature = "std")]
use core::panic::AssertUnwindSafe;
#[cfg(feature = "std")]
use futures::FutureExt;
use futures::future::join;
#[cfg(not(feature = "std"))]
use ruststream_core::__alloc::spin::Mutex as SpinMutex;
use ruststream_core::IfSend;
use ruststream_core::diag::warn;
#[cfg(feature = "std")]
use std::panic::resume_unwind;
#[cfg(feature = "std")]
use std::sync::{Mutex, PoisonError};

use super::{Failed, FailedMessage};
use crate::handler::{Continuation, ContinuationFuture, MaybeSend};
use crate::lifecycle::BoxError;

/// The publishes that failed for one `.error` continuation: one [`Failed`] for a single delivery,
/// every failed element for a uniform batch outcome, in the batch's order.
///
/// A single delivery's holds its one failure in place; a batch allocates its list only when an
/// element failed.
#[derive(Debug)]
pub struct Failures(Repr);

#[derive(Debug)]
enum Repr {
    One(Option<Failed>),
    Many(vec::IntoIter<Failed>),
}

impl Failures {
    /// The failure of a single delivery.
    pub(crate) const fn one(failed: Failed) -> Self {
        Self(Repr::One(Some(failed)))
    }

    /// The failures of a batch.
    pub(crate) fn many(failures: Vec<Failed>) -> Self {
        Self(Repr::Many(failures.into_iter()))
    }
}

impl Iterator for Failures {
    type Item = Failed;

    fn next(&mut self) -> Option<Failed> {
        match &mut self.0 {
            Repr::One(failed) => failed.take(),
            Repr::Many(failures) => failures.next(),
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let left = match &self.0 {
            Repr::One(failed) => usize::from(failed.is_some()),
            Repr::Many(failures) => failures.len(),
        };
        (left, Some(left))
    }
}

impl ExactSizeIterator for Failures {}

impl FusedIterator for Failures {}

/// The `.error` closure, erased: `Send` where the core's `send` feature is on.
trait Reactor: MaybeSend {
    fn react(self: Box<Self>, failures: Failures) -> Continuation;
}

/// The closure as the one registration holds it.
struct Owned<React>(React);

impl<React, Reaction> Reactor for Owned<React>
where
    React: FnOnce(Failures) -> Reaction + MaybeSend + 'static,
    Reaction: ContinuationFuture,
{
    fn react(self: Box<Self>, failures: Failures) -> Continuation {
        // Built inside the future, so the closure's own synchronous part runs when the caller
        // polls the continuation, after the delivery is settled.
        Box::pin(async move { (self.0)(failures).await })
    }
}

/// How a value is changed under a lock held for the change alone.
trait Guarded<Value> {
    fn guard(value: Value) -> Self;

    fn with<Out>(&self, change: impl FnOnce(&mut Value) -> Out) -> Out;
}

#[cfg(feature = "std")]
impl<Value> Guarded<Value> for Mutex<Value> {
    fn guard(value: Value) -> Self {
        Self::new(value)
    }

    fn with<Out>(&self, change: impl FnOnce(&mut Value) -> Out) -> Out {
        change(&mut self.lock().unwrap_or_else(PoisonError::into_inner))
    }
}

#[cfg(not(feature = "std"))]
impl<Value> Guarded<Value> for SpinMutex<Value> {
    fn guard(value: Value) -> Self {
        Self::new(value)
    }

    fn with<Out>(&self, change: impl FnOnce(&mut Value) -> Out) -> Out {
        change(&mut self.lock())
    }
}

impl<Value> Guarded<Value> for RefCell<Value> {
    fn guard(value: Value) -> Self {
        Self::new(value)
    }

    fn with<Out>(&self, change: impl FnOnce(&mut Value) -> Out) -> Out {
        change(&mut self.borrow_mut())
    }
}

/// The lock of a shared value: a mutex where the core's `send` feature is on, which keeps the
/// value shareable without it being `Sync`; a cell otherwise. The choice follows the feature as
/// the build unifies it, so it agrees with the `Send` bounds of the contract.
#[cfg(feature = "std")]
type Exclusive<Value> = Mutex<Value>;

/// See the `std` form: a build without `std` has no mutex of its own, so it takes a spin lock,
/// held only for the change, never across an await.
#[cfg(not(feature = "std"))]
type Exclusive<Value> = SpinMutex<Value>;

/// A value shared by the elements of a batch and changed under a lock held for the change alone.
struct Lock<Value>(IfSend<Exclusive<Value>, RefCell<Value>>);

impl<Value> Lock<Value> {
    fn new(value: Value) -> Self {
        Self(<IfSend<Exclusive<Value>, RefCell<Value>> as Guarded<
            Value,
        >>::guard(value))
    }

    fn with<Out>(&self, change: impl FnOnce(&mut Value) -> Out) -> Out {
        Guarded::with(&self.0, change)
    }
}

/// What the elements of a uniform batch outcome report to: the outcome's one `.error` closure and
/// `and_after` continuation run once, when the last element's publish has been reported.
struct Gather {
    state: Lock<Gathered>,
}

struct Gathered {
    /// Elements whose publish has not been reported yet.
    pending: usize,
    /// Whether any element's publish succeeded.
    sent: bool,
    /// The failures reported so far, with the index of their element. Allocated by the first.
    failures: Vec<(usize, Failed)>,
    react: Option<Box<dyn Reactor>>,
    after: Option<Continuation>,
}

impl Gather {
    #[allow(clippy::arc_with_non_send_sync)] // see `ErrorHook::fan_out`
    fn new(elements: usize, react: Box<dyn Reactor>, after: Option<Continuation>) -> Arc<Self> {
        Arc::new(Self {
            state: Lock::new(Gathered {
                pending: elements,
                sent: false,
                failures: Vec::new(),
                react: Some(react),
                after,
            }),
        })
    }

    /// Records one element's publish. The call that reports the last one gets the continuation
    /// that runs the outcome's `and_after` (when any publish succeeded) and then its `.error`
    /// (when any failed); every other call gets a continuation that does nothing.
    fn report(&self, failure: Option<(usize, Failed)>) -> Continuation {
        let done = self.state.with(|gathered| {
            match failure {
                Some(failure) => gathered.failures.push(failure),
                None => gathered.sent = true,
            }
            gathered.pending -= 1;
            if gathered.pending > 0 {
                return None;
            }
            let mut failures = mem::take(&mut gathered.failures);
            failures.sort_by_key(|(index, _)| *index);
            Some((
                gathered.sent.then(|| gathered.after.take()).flatten(),
                gathered.react.take().filter(|_| !failures.is_empty()),
                failures,
            ))
        });
        let Some((after, react, failures)) = done else {
            return Box::pin(async {});
        };
        let error = async move {
            if let Some(react) = react {
                let failures = failures.into_iter().map(|(_, failed)| failed).collect();
                react.react(Failures::many(failures)).await;
            }
        };
        match after {
            None => Box::pin(error),
            Some(after) => Box::pin(alongside(error, after)),
        }
    }
}

/// Runs the outcome's `.error` beside its `.and_after`, `.error` polled first: a follow-up that
/// is slow, never finishes or panics does not keep the failures from `.error`. A panic is raised
/// again once `.error` is done.
async fn alongside(error: impl Future<Output = ()>, after: Continuation) {
    #[cfg(feature = "std")]
    {
        let ((), followed) = join(error, AssertUnwindSafe(after).catch_unwind()).await;
        if let Err(panic) = followed {
            resume_unwind(panic);
        }
    }
    #[cfg(not(feature = "std"))]
    {
        join(error, after).await;
    }
}

/// One element's share of a uniform outcome's `.error`: it reports the element's failure.
struct Part {
    gather: Arc<Gather>,
    index: usize,
}

impl Reactor for Part {
    fn react(self: Box<Self>, failures: Failures) -> Continuation {
        let failed = failures.into_iter().next();
        self.gather
            .report(failed.map(|failed| (self.index, failed)))
    }
}

/// The part of a uniform outcome's continuation one element carries: it reports a publish that
/// succeeded.
fn sent_part(gather: Arc<Gather>) -> Continuation {
    Box::pin(async move { gather.report(None).await })
}

/// A uniform batch outcome handed out element by element.
pub(crate) struct Fan {
    gather: Arc<Gather>,
    next: usize,
}

impl Fan {
    /// The share of the next element: a hook for its failure and a continuation for its success.
    pub(crate) fn element(&mut self) -> (ErrorHook, Continuation) {
        let index = self.next;
        self.next += 1;
        let part = Part {
            gather: Arc::clone(&self.gather),
            index,
        };
        (
            ErrorHook(Box::new(part)),
            sent_part(Arc::clone(&self.gather)),
        )
    }
}

/// The `.error` continuation of one [`HandlerOutcome`](crate::HandlerOutcome), until a publish
/// the runtime makes for it fails. Machinery; never named directly.
#[doc(hidden)]
pub struct ErrorHook(Box<dyn Reactor>);

impl ErrorHook {
    #[must_use]
    pub fn new<React, Reaction>(react: React) -> Self
    where
        React: FnOnce(Failures) -> Reaction + MaybeSend + 'static,
        Reaction: ContinuationFuture,
    {
        Self(Box::new(Owned(react)))
    }

    /// Hands the failure to the handler's continuation, which the caller runs.
    pub fn react(self, error: BoxError, message: FailedMessage) -> Continuation {
        self.0.react(Failures::one(Failed::new(error, message)))
    }

    /// Hands the failure of a batch of messages that left in one publish to the handler's
    /// continuation, which the caller runs: one [`Failed`] per message, all with the one cause.
    pub(crate) fn react_all(self, error: BoxError, messages: Vec<FailedMessage>) -> Continuation {
        let cause = Arc::new(error);
        let failures = messages
            .into_iter()
            .map(|message| Failed::shared(Arc::clone(&cause), message))
            .collect();
        self.0.react(Failures::many(failures))
    }

    /// Splits the hook of a uniform batch outcome over its `elements`: the closure runs once,
    /// with every element's failure, after the last element's publish has been reported.
    // Without the `tokio` feature the runner is single-threaded and the closure carries no `Send`,
    // so the `Arc` is never shared across threads.
    #[cfg_attr(not(feature = "tokio"), allow(clippy::arc_with_non_send_sync))]
    pub(crate) fn fan_out(self, elements: usize, after: Option<Continuation>) -> Fan {
        Fan {
            gather: Gather::new(elements, self.0, after),
            next: 0,
        }
    }
}

impl fmt::Debug for ErrorHook {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ErrorHook").finish_non_exhaustive()
    }
}

/// The one error of a publish that carried several messages, owned by whoever takes a
/// [`Failed`] apart; the original stays its `source`.
#[derive(Debug)]
pub(super) struct SharedCause(pub(super) Arc<BoxError>);

impl fmt::Display for SharedCause {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&**self.0, f)
    }
}

impl StdError for SharedCause {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        Some(&**self.0)
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
pub struct Reaction(Box<Registered>);

/// What a [`Reaction`] holds, behind the one pointer an outcome without it pays eight bytes for.
struct Registered {
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
    pub fn new(error: ErrorHook, then: Option<Continuation>) -> Self {
        Self(Box::new(Registered { error, then }))
    }

    /// The continuation registered beside `.error`, which a delivery that made no publish runs
    /// once it is settled.
    pub(crate) fn into_then(self) -> Option<Continuation> {
        self.0.then
    }

    /// What follows a publish that succeeded: the `.then` continuation, if one was registered.
    #[must_use]
    pub fn succeeded(self) -> Option<Continuation> {
        self.0.then
    }

    /// What follows the one publish of a batch that failed: the `.error` continuation, told of
    /// every message the publish carried.
    pub(crate) fn failed_all(self, error: BoxError, messages: Vec<FailedMessage>) -> Continuation {
        self.0.error.react_all(error, messages)
    }

    /// What follows a publish that failed: the `.error` continuation, given the cause.
    fn failed(self, error: BoxError, message: FailedMessage) -> Continuation {
        self.0.error.react(error, message)
    }
}

/// What follows a publish that succeeded: the `.then` continuation registered beside `.error`,
/// returned for the caller to run beside the delivery.
#[doc(hidden)]
#[must_use]
pub fn publish_done(reaction: Option<Reaction>) -> Option<Continuation> {
    reaction.and_then(Reaction::succeeded)
}

/// What happens to a copy or reply whose publish failed: the handler's `.error` continuation,
/// returned for the caller to run, or, with none, a warning and the message lost.
///
/// `message` builds the failed message, and is called only when there is a continuation to hand
/// it to.
#[doc(hidden)]
pub fn publish_lost(
    reaction: Option<Reaction>,
    subscription: &str,
    destination: &str,
    input: &str,
    error: BoxError,
    message: impl FnOnce() -> FailedMessage,
) -> Option<Continuation> {
    if let Some(reaction) = reaction {
        return Some(reaction.failed(error, message()));
    }
    warn!(
        target: "ruststream::dispatch",
        subscription = %subscription,
        destination = %destination,
        message_type = %input,
        error = %error,
        "publishing the message failed; message lost",
    );
    None
}

#[cfg(test)]
mod tests {
    use alloc::borrow::ToOwned;
    use alloc::format;
    use alloc::string::ToString;
    use alloc::vec;
    use core::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::io;

    use super::*;
    use crate::HeaderSet;
    use crate::copied::CopiedPayload;

    /// An error whose source is the I/O error it wraps.
    #[derive(Debug)]
    struct Layered(io::Error);

    impl fmt::Display for Layered {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str("layered")
        }
    }

    impl StdError for Layered {
        fn source(&self) -> Option<&(dyn StdError + 'static)> {
            Some(&self.0)
        }
    }

    fn lost() -> (BoxError, FailedMessage) {
        (
            Box::new(io::Error::other("down")),
            FailedMessage::new("dest", Some(CopiedPayload::new()), HeaderSet::new()),
        )
    }

    /// The closure's own synchronous part is user code: it runs when the continuation is polled,
    /// after the delivery is settled, and never while the runtime builds the continuation.
    #[test]
    fn the_error_closure_runs_only_when_its_continuation_is_polled() {
        let ran = Arc::new(AtomicBool::new(false));
        let flag = Arc::clone(&ran);
        let hook = ErrorHook::new(move |_failures: Failures| {
            flag.store(true, Ordering::SeqCst);
            async {}
        });
        let (error, message) = lost();

        let continuation = hook.react(error, message);

        assert!(!ran.load(Ordering::SeqCst));
        continuation.now_or_never().expect("ready");
        assert!(ran.load(Ordering::SeqCst));
    }

    #[test]
    fn the_hooks_of_a_reaction_print_without_their_closures() {
        let hook = ErrorHook::new(async |_failures: Failures| {});
        let reaction = Reaction::new(ErrorHook::new(async |_failures: Failures| {}), None);

        assert_eq!(
            (format!("{hook:?}"), format!("{reaction:?}")),
            ("ErrorHook { .. }".to_owned(), "Reaction { .. }".to_owned())
        );
    }

    /// The failures of one batch publish share a cause, and the cause's own source stays reachable
    /// through each of them.
    #[test]
    fn the_failures_of_a_batch_publish_keep_the_source_of_their_shared_cause() {
        let rooted = Arc::new(AtomicUsize::new(0));
        let record = Arc::clone(&rooted);
        let hook = ErrorHook::new(move |failures: Failures| {
            for failed in failures {
                let source = failed.error().source().map(ToString::to_string);
                if source.as_deref() == Some("root") {
                    record.fetch_add(1, Ordering::SeqCst);
                }
            }
            async {}
        });
        let cause = Layered(io::Error::other("root"));
        let messages = vec![
            FailedMessage::new("a", Some(CopiedPayload::new()), HeaderSet::new()),
            FailedMessage::new("b", Some(CopiedPayload::new()), HeaderSet::new()),
        ];

        hook.react_all(Box::new(cause), messages)
            .now_or_never()
            .expect("ready");

        assert_eq!(rooted.load(Ordering::SeqCst), 2);
    }

    /// A failure of a batch publish, taken apart, still leads to the original error through its
    /// source.
    #[test]
    fn a_shared_cause_taken_apart_keeps_the_original_error_as_its_source() {
        let reached = Arc::new(AtomicUsize::new(0));
        let record = Arc::clone(&reached);
        let hook = ErrorHook::new(move |failures: Failures| {
            for failed in failures {
                let (cause, _message) = failed.into_parts();
                let original = cause
                    .source()
                    .and_then(|source| source.downcast_ref::<Layered>());
                record.fetch_add(usize::from(original.is_some()), Ordering::SeqCst);
            }
            async {}
        });
        let messages = vec![
            FailedMessage::new("a", Some(CopiedPayload::new()), HeaderSet::new()),
            FailedMessage::new("b", Some(CopiedPayload::new()), HeaderSet::new()),
        ];

        hook.react_all(Box::new(Layered(io::Error::other("root"))), messages)
            .now_or_never()
            .expect("ready");

        assert_eq!(reached.load(Ordering::SeqCst), 2);
    }
}
