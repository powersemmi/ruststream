//! What a handler's `.error` continuation receives when a publish the runtime makes for its
//! outcome fails, and the reaction the runtime takes when the outcome carries none.

use alloc::borrow::ToOwned;
use alloc::boxed::Box;
use alloc::string::String;
use alloc::sync::Arc;
use alloc::vec::{self, Vec};
use bytes::{Bytes, BytesMut};
#[cfg(not(feature = "tokio"))]
use core::cell::RefCell;
use core::convert::Infallible;
use core::error::Error as StdError;
use core::fmt;
use core::iter::FusedIterator;
use core::mem;
#[cfg(feature = "tokio")]
use std::sync::{Mutex, PoisonError};
use thiserror::Error;
use tracing::warn;

use crate::handler::{Continuation, ContinuationFuture, MaybeSend};
use crate::lifecycle::BoxError;
use crate::{
    CallerName, HeaderMap, MessageHeaders, MessageWire, NoHeaders, OutgoingDestination, Serialized,
    SerializedWire, WireBytes,
};

/// The error of a retry copy the embedded runner has nowhere to hold: every record of the mount's
/// store is taken.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
#[error("no free record in the store of retry copies (capacity {capacity})")]
pub struct NoFreeRecordError {
    capacity: usize,
}

impl NoFreeRecordError {
    /// The error of a store of `capacity` records, all taken.
    #[doc(hidden)]
    #[must_use]
    pub const fn new(capacity: usize) -> Self {
        Self { capacity }
    }

    /// How many records the store has.
    #[must_use]
    pub const fn capacity(&self) -> usize {
        self.capacity
    }
}

/// The message a runtime publish failed to send: the destination it was bound for, its bytes and
/// its headers.
///
/// A handler's `.error` continuation receives it inside a [`Failed`], beside the error. The runtime builds
/// it only for an outcome that registered `.error`, so a failure nobody asked about copies
/// nothing.
///
/// # Examples
///
/// ```
/// # #[cfg(feature = "json")]
/// # mod demo {
/// use std::time::Duration;
///
/// use ruststream::prelude::*;
/// use ruststream::runtime::FailedMessage;
/// use serde::Deserialize;
///
/// #[derive(Deserialize)]
/// struct Order {
///     id: u64,
/// }
///
/// fn record(order: u64, lost: &FailedMessage) {
///     tracing::error!(order, destination = lost.destination(), bytes = lost.payload().len(), "retry lost");
/// }
///
/// #[subscriber("orders")]
/// async fn accept(order: &Order) -> HandlerOutcome {
///     let id = order.id;
///     HandlerOutcome::retry_after(Duration::from_secs(5))
///         .error(async move |failures: Failures| {
///             for failed in failures {
///                 record(id, failed.message());
///             }
///         })
/// }
/// # }
/// # fn main() {}
/// ```
#[derive(Debug, Clone)]
pub struct FailedMessage {
    destination: String,
    payload: Bytes,
    headers: HeaderMap,
}

impl FailedMessage {
    /// A message bound for `destination`.
    #[doc(hidden)]
    #[must_use]
    pub fn new(destination: &str, payload: Bytes, headers: HeaderMap) -> Self {
        Self {
            destination: destination.to_owned(),
            payload,
            headers,
        }
    }

    /// Where the message was going.
    #[must_use]
    pub fn destination(&self) -> &str {
        &self.destination
    }

    /// The message's bytes, as they were to leave.
    #[must_use]
    pub const fn payload(&self) -> &Bytes {
        &self.payload
    }

    /// The message's headers, the retry count included.
    #[must_use]
    pub const fn headers(&self) -> &HeaderMap {
        &self.headers
    }

    /// Splits the message into its destination, bytes and headers.
    #[must_use]
    pub fn into_parts(self) -> (String, Bytes, HeaderMap) {
        (self.destination, self.payload, self.headers)
    }
}

/// The message a runtime publish failed to send is a [`Serialized`] value: its bytes leave as they
/// are, so another `Out` publishes it unchanged.
impl Serialized for FailedMessage {
    type Error = Infallible;

    fn wire_bytes(&self, _buf: &mut BytesMut) -> Result<WireBytes<'_>, Infallible> {
        Ok(WireBytes::Own(&self.payload))
    }
}

// Left out of the compiler's suggestions: a user who forgot a derive is not pointed at this type.
#[diagnostic::do_not_recommend]
impl MessageWire for FailedMessage {
    type Wire = SerializedWire;
}

// The destination the message was going to belongs to the failure, not to another publish: the
// call site that republishes it names where it goes now.
impl OutgoingDestination for FailedMessage {
    type Form = CallerName;
}

// Its headers travel with it, as a map the call site hands over with `with_headers`.
impl MessageHeaders for FailedMessage {
    type Contract = NoHeaders;
}

/// One publish the runtime made for a delivery and that failed: its cause and the message that
/// did not go out.
///
/// The message is the one the runtime tried to send, with its bytes and headers as they were;
/// republish it through another `Out`, store it or inspect it. Nothing is decoded again.
///
/// # Examples
///
/// ```
/// # #[cfg(feature = "json")]
/// # mod demo {
/// use std::time::Duration;
///
/// use ruststream::prelude::*;
/// use serde::Deserialize;
///
/// #[derive(Deserialize)]
/// struct Order {
///     id: u64,
/// }
///
/// #[subscriber("orders")]
/// async fn accept(order: &Order) -> HandlerOutcome {
///     let id = order.id;
///     HandlerOutcome::retry_after(Duration::from_secs(5)).error(async move |failures: Failures| {
///         for failed in failures {
///             tracing::error!(
///                 order = id,
///                 error = %failed.error(),
///                 destination = failed.message().destination(),
///                 bytes = failed.message().payload().len(),
///                 "the retry was lost",
///             );
///         }
///     })
/// }
/// # }
/// # fn main() {}
/// ```
#[derive(Debug)]
pub struct Failed {
    error: BoxError,
    message: FailedMessage,
}

impl Failed {
    /// A failure of `message`, caused by `error`.
    #[doc(hidden)]
    #[must_use]
    pub const fn new(error: BoxError, message: FailedMessage) -> Self {
        Self { error, message }
    }

    /// Why the publish failed: the broker publisher's own error, the encode error, or the
    /// "no free record" kind.
    #[must_use]
    pub fn error(&self) -> &(dyn StdError + Send + Sync + 'static) {
        &*self.error
    }

    /// The message that did not go out.
    #[must_use]
    pub const fn message(&self) -> &FailedMessage {
        &self.message
    }

    /// The message that did not go out, taken by value.
    #[must_use]
    pub fn into_message(self) -> FailedMessage {
        self.message
    }

    /// The cause and the message, taken apart.
    #[must_use]
    pub fn into_parts(self) -> (BoxError, FailedMessage) {
        (self.error, self.message)
    }
}

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

/// The `.error` closure, erased: `Send` where the runtime crate's `tokio` feature is on.
#[cfg(feature = "tokio")]
trait Reactor: Send {
    fn react(self: Box<Self>, failures: Failures) -> Continuation;
}

/// See the `tokio` form: without the feature the closure carries no `Send`.
#[cfg(not(feature = "tokio"))]
trait Reactor {
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
        Box::pin((self.0)(failures))
    }
}

/// A value shared by the elements of a batch and changed under a lock held for the change alone.
/// Under Tokio the lock is a mutex, which keeps the value shareable without it being `Sync`;
/// without `Send` a cell suffices.
#[cfg(feature = "tokio")]
struct Lock<Value>(Mutex<Value>);

#[cfg(feature = "tokio")]
impl<Value> Lock<Value> {
    const fn new(value: Value) -> Self {
        Self(Mutex::new(value))
    }

    fn with<Out>(&self, change: impl FnOnce(&mut Value) -> Out) -> Out {
        change(&mut self.0.lock().unwrap_or_else(PoisonError::into_inner))
    }
}

/// See the `tokio` form.
#[cfg(not(feature = "tokio"))]
struct Lock<Value>(RefCell<Value>);

#[cfg(not(feature = "tokio"))]
impl<Value> Lock<Value> {
    const fn new(value: Value) -> Self {
        Self(RefCell::new(value))
    }

    fn with<Out>(&self, change: impl FnOnce(&mut Value) -> Out) -> Out {
        change(&mut self.0.borrow_mut())
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
        Box::pin(async move {
            if let Some(after) = after {
                after.await;
            }
            if let Some(react) = react {
                let failures = failures.into_iter().map(|(_, failed)| failed).collect();
                react.react(Failures::many(failures)).await;
            }
        })
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
        let cause = Arc::new(SharedCause(error));
        let failures = messages
            .into_iter()
            .map(|message| Failed::new(Box::new(Arc::clone(&cause)), message))
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

/// The one error of a publish that carried several messages, which each of their [`Failed`]
/// reports.
#[derive(Debug)]
struct SharedCause(BoxError);

impl fmt::Display for SharedCause {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.0, f)
    }
}

impl StdError for SharedCause {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        self.0.source()
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
    /// The headers of the failed copy, kept here (boxed with the registration) so the task that
    /// publishes the copy carries one pointer for them and nothing when no `.error` was set.
    #[cfg(feature = "tokio")]
    headers: HeaderMap,
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
        Self(Box::new(Registered {
            error,
            then,
            #[cfg(feature = "tokio")]
            headers: HeaderMap::new(),
        }))
    }

    /// The continuation registered beside `.error`, which a delivery that made no publish runs
    /// once it is settled.
    /// Keeps the headers the failed copy carries, for the [`FailedMessage`] a failure builds.
    #[cfg(feature = "tokio")]
    pub(crate) fn hold_headers(&mut self, headers: HeaderMap) {
        self.0.headers = headers;
    }

    /// Takes the headers kept with [`hold_headers`](Self::hold_headers).
    #[cfg(feature = "tokio")]
    pub(crate) fn take_headers(&mut self) -> HeaderMap {
        mem::take(&mut self.0.headers)
    }

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
