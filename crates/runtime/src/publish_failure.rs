//! What a handler's `.error` continuation receives when a publish the runtime makes for its
//! outcome fails, and the reaction the runtime takes when the outcome carries none.

#[cfg(ruststream_alloc)]
use alloc::boxed::Box;
#[cfg(ruststream_alloc)]
use alloc::string::String;
#[cfg(ruststream_alloc)]
use alloc::sync::Arc;
use core::error::Error as StdError;
use core::fmt;
use core::mem;

use ruststream_core::{EncodeBuf, KeptPayload};
use thiserror::Error;

use crate::copied::{CopiedName, CopiedPayload, copy_name};
use crate::lifecycle::BoxError;
use crate::publish::SinkRefusal;
use crate::{
    CallerName, HeaderSet, MessageHeaders, MessageWire, NoHeaders, Outgoing, OutgoingDestination,
    OutgoingFor, PayloadForm, PublishMessageError, Serialized, SerializedWire, WireBytes,
};

/// The error of sending on a [`FailedMessage`] whose bytes are gone: the publisher that refused
/// it consumed the buffer and gave nothing back, so there is no message left to send.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
#[error("the failed message has no bytes to send: its publisher consumed them")]
#[non_exhaustive]
pub struct MissingPayloadError;

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

/// The destination a failed message reports: the name with `alloc`, and without it `None` where
/// the name was longer than the fixed room.
#[cfg(ruststream_alloc)]
pub(crate) type DestinationName = CopiedName;
/// The destination a failed message reports: the name with `alloc`, and without it `None` where
/// the name was longer than the fixed room.
#[cfg(not(ruststream_alloc))]
pub(crate) type DestinationName = Option<CopiedName>;

/// A destination name as a failed message reports it.
#[cfg(ruststream_alloc)]
fn destination_name(name: &str) -> DestinationName {
    String::from(name)
}

/// A destination name as a failed message reports it: `None` past the fixed room.
#[cfg(not(ruststream_alloc))]
fn destination_name(name: &str) -> DestinationName {
    copy_name(name)
}

/// The name a refused message went to: `declared` unless a naming transform recorded another.
#[cfg(ruststream_alloc)]
#[expect(clippy::unnecessary_wraps, reason = "one signature for both variants")]
fn sent_name<'name>(
    sent: Option<&'name DestinationName>,
    declared: &'name str,
) -> Option<&'name str> {
    Some(sent.map_or(declared, String::as_str))
}

/// The name a refused message went to: `declared` unless a naming transform recorded another,
/// and `None` where that other name was past the fixed room, so the message is not reported at a
/// destination it never went to.
#[cfg(not(ruststream_alloc))]
fn sent_name<'name>(
    sent: Option<&'name DestinationName>,
    declared: &'name str,
) -> Option<&'name str> {
    sent.map_or(Some(declared), Option::as_deref)
}

/// The message a runtime publish failed to send: the destination it was bound for, its bytes and
/// its headers.
///
/// The bytes are `None` only where the publisher took the payload buffer and did not give it back;
/// the destination and the headers the runtime held are then all there is.
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
///     let bytes = lost.payload().map_or(0, |payload| payload.len());
///     tracing::error!(order, destination = lost.destination(), bytes, "retry lost");
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
    #[cfg(ruststream_alloc)]
    destination: CopiedName,
    // Without `alloc` a name past the fixed room is lost, and the message says so with `None`:
    // an empty name reported as the destination would send a republish to the wrong place.
    #[cfg(not(ruststream_alloc))]
    destination: Option<CopiedName>,
    payload: Option<CopiedPayload>,
    headers: HeaderSet,
}

impl FailedMessage {
    /// A message bound for `destination`.
    #[doc(hidden)]
    #[must_use]
    pub fn new(destination: &str, payload: Option<CopiedPayload>, headers: HeaderSet) -> Self {
        Self {
            #[cfg(ruststream_alloc)]
            destination: copy_name(destination).unwrap_or_default(),
            #[cfg(not(ruststream_alloc))]
            destination: copy_name(destination),
            payload,
            headers,
        }
    }

    /// Where the message was going.
    #[must_use]
    #[cfg(ruststream_alloc)]
    pub fn destination(&self) -> &str {
        &self.destination
    }

    /// Where the message was going; `None` where the name was longer than the fixed room of a
    /// build without `alloc`.
    #[must_use]
    #[cfg(not(ruststream_alloc))]
    pub fn destination(&self) -> Option<&str> {
        self.destination.as_deref()
    }

    /// The message's bytes, as they were to leave, or `None` when the publisher consumed the
    /// buffer and did not give it back.
    #[must_use]
    pub const fn payload(&self) -> Option<&CopiedPayload> {
        self.payload.as_ref()
    }

    /// The message's headers, the retry count included.
    #[must_use]
    pub const fn headers(&self) -> &HeaderSet {
        &self.headers
    }

    /// Splits the message into its destination, bytes and headers.
    #[must_use]
    pub fn into_parts(self) -> (DestinationName, Option<CopiedPayload>, HeaderSet) {
        (self.destination, self.payload, self.headers)
    }
}

/// The message a runtime publish failed to send is a [`Serialized`] value: its bytes leave as they
/// are, so another `Out` publishes it unchanged. A message whose bytes the publisher consumed is
/// refused with [`MissingPayloadError`] rather than sent empty.
impl Serialized for FailedMessage {
    type Error = MissingPayloadError;

    fn wire_bytes(&self, _buf: &mut EncodeBuf) -> Result<WireBytes<'_>, MissingPayloadError> {
        self.payload
            .as_deref()
            .map(WireBytes::Own)
            .ok_or(MissingPayloadError)
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

/// A publish the runtime made that failed, with whatever the publisher handed back.
///
/// The paths that publish for a handler (a reply, a retry copy, a dead-letter copy) return it, and
/// the `.error` continuation's [`Failed`] is built from it: the message that comes back is the
/// one that was sent, moved, with no copy made beforehand. Machinery; never named directly.
#[doc(hidden)]
#[derive(Debug)]
pub struct Refusal {
    error: BoxError,
    unsent: Option<Held>,
}

/// A refused message as the refusal holds it: boxed with `alloc`, so a publish that went out
/// returns a pointer-sized result through every layer of the reply and the copy, with the message
/// moved into the box only on a refusal. Without a heap the refusal holds it in place.
#[derive(Debug)]
struct Held {
    #[cfg(ruststream_alloc)]
    unsent: Box<Unsent>,
    #[cfg(not(ruststream_alloc))]
    unsent: Unsent,
}

impl Held {
    fn new(unsent: Unsent) -> Self {
        Self {
            #[cfg(ruststream_alloc)]
            unsent: Box::new(unsent),
            #[cfg(not(ruststream_alloc))]
            unsent,
        }
    }

    fn release(self) -> Unsent {
        #[cfg(ruststream_alloc)]
        {
            *self.unsent
        }
        #[cfg(not(ruststream_alloc))]
        {
            self.unsent
        }
    }
}

/// The message a refused publish handed back, as its owner reads it.
#[derive(Debug)]
struct Unsent {
    // Where the message was sent, recorded only when a naming transform changed it from the
    // destination the caller declared: the common refusal allocates no name of its own.
    sent: Option<DestinationName>,
    payload: KeptPayload,
    headers: HeaderSet,
}

impl Refusal {
    /// A refusal that carries no message.
    #[must_use]
    pub fn new(error: impl Into<BoxError>) -> Self {
        Self {
            error: error.into(),
            unsent: None,
        }
    }

    /// The refusal of a publish in the form `Form`, with the message the publisher returned.
    ///
    /// A lent payload stays where it was lent from; whoever owns the bytes resolves it in
    /// [`into_message`](Self::into_message).
    pub fn returned<Form: PayloadForm>(
        failed: PublishMessageError<impl Into<BoxError>, OutgoingFor<'_, Form>>,
    ) -> Self {
        let (error, message) = failed.into_parts();
        Self {
            error: error.into(),
            unsent: message.map(|message| {
                let (_, payload, headers) = message.into_owned_parts();
                Held::new(Unsent {
                    // Handed straight to the publisher: the destination is the one the caller
                    // named.
                    sent: None,
                    payload: Form::kept(payload),
                    headers,
                })
            }),
        }
    }

    /// The refusal of a sink, whose message is already owned.
    #[must_use]
    pub(crate) fn from_sink<Error: Into<BoxError>>(refusal: SinkRefusal<Error>) -> Self {
        let (error, unsent) = refusal.into_parts();
        Self {
            error: error.into(),
            unsent: unsent.map(|(destination, payload, headers)| {
                Held::new(Unsent {
                    #[cfg(ruststream_alloc)]
                    sent: Some(destination),
                    #[cfg(not(ruststream_alloc))]
                    sent: Some(copy_name(&destination)),
                    payload,
                    headers,
                })
            }),
        }
    }

    /// Puts the message the publisher returned back into `out`, which then reads as the message
    /// that was sent, and gives the cause.
    ///
    /// For a path that returns its error alone and keeps `out`: the stages above read the
    /// message from `out` once the error reaches them. A refusal with no message leaves `out`
    /// empty.
    pub fn restore<Form: PayloadForm>(self, out: &mut Outgoing<'_>) -> BoxError {
        if let Some(Unsent {
            payload, headers, ..
        }) = self.unsent.map(Held::release)
        {
            Form::restored(payload, out);
            *out.headers_mut() = headers;
        } else {
            // The client consumed the buffer: nothing of the message is left to read.
            out.consume();
        }
        self.error
    }

    /// The refusal of a publish that failed with `error` while `out` held the message: what it
    /// holds is the message that was sent, and moves into the refusal.
    ///
    /// A buffer the stages wrote moves as it is; bytes `out` only lends stay where they are, for
    /// the owner of the lender to take in [`into_message`](Self::into_message).
    pub fn of(error: BoxError, out: &mut Outgoing<'_>) -> Self {
        if out.is_consumed() {
            return Self::new(error);
        }
        let payload = out.take_owned_payload().map_or_else(
            || KeptPayload::Lent {
                addr: out.payload().as_ptr() as usize,
                len: out.payload().len(),
            },
            KeptPayload::Owned,
        );
        Self {
            error,
            unsent: Some(Held::new(Unsent {
                sent: None,
                payload,
                headers: mem::take(out.headers_mut()),
            })),
        }
    }

    /// Records that the message went to `sent` rather than the `declared` destination, as it
    /// does when a naming transform redirected it. The name is copied only then.
    #[must_use]
    pub(crate) fn sent_to(mut self, declared: &str, sent: &str) -> Self {
        if sent != declared
            && let Some(unsent) = self.unsent.as_mut()
        {
            unsent.unsent.sent = Some(destination_name(sent));
        }
        self
    }

    /// The refusal as a sink reports it: the cause wrapped by `wrap`, and the message where it
    /// was sent, `destination` unless the refusal recorded another.
    #[must_use]
    pub(crate) fn into_sink<Error>(
        self,
        destination: &str,
        wrap: impl FnOnce(BoxError) -> Error,
    ) -> SinkRefusal<Error> {
        let error = wrap(self.error);
        match self.unsent.map(Held::release) {
            Some(Unsent {
                sent,
                payload,
                headers,
            }) => match sent_name(sent.as_ref(), destination) {
                Some(name) => SinkRefusal::with_message(error, name, payload, headers),
                None => SinkRefusal::new(error),
            },
            None => SinkRefusal::new(error),
        }
    }

    /// The cause.
    #[must_use]
    pub fn error(&self) -> &BoxError {
        &self.error
    }

    /// The cause, without the message.
    #[must_use]
    pub fn into_error(self) -> BoxError {
        self.error
    }

    /// The cause and the message where it was sent: `destination`, unless the refusal recorded
    /// another.
    ///
    /// `lent` takes the bytes a lending publisher was shown, given where they were: the caller
    /// that owns them moves them out instead of copying. A refusal with no message (the client
    /// consumed the buffer), or whose lent bytes `lent` could not return whole, yields a message
    /// with no bytes.
    #[must_use]
    pub fn into_message(
        self,
        destination: &str,
        lent: impl FnOnce(usize, usize) -> CopiedPayload,
    ) -> (BoxError, FailedMessage) {
        let message = match self.unsent.map(Held::release) {
            Some(Unsent {
                sent,
                payload,
                headers,
            }) => {
                let payload = match payload {
                    KeptPayload::Owned(bytes) => Some(bytes),
                    // Fewer bytes than were lent means the owner could not keep them (a copy
                    // has a fixed room without `alloc`): the message then has no bytes, so a
                    // republish is refused rather than sent short.
                    KeptPayload::Lent { addr, len } => {
                        Some(lent(addr, len)).filter(|kept| kept.len() == len)
                    }
                };
                FailedMessage {
                    destination: sent.unwrap_or_else(|| destination_name(destination)),
                    payload,
                    headers,
                }
            }
            None => FailedMessage::new(destination, None, HeaderSet::new()),
        };
        (self.error, message)
    }
}

impl fmt::Display for Refusal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.error, f)
    }
}

impl From<BoxError> for Refusal {
    fn from(error: BoxError) -> Self {
        Self::new(error)
    }
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
///                 bytes = failed.message().payload().map_or(0, |payload| payload.len()),
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
    error: FailureCause,
    message: FailedMessage,
}

/// The error of a [`Failed`]: its own, or with `alloc` the one error of a publish that carried
/// several messages, shared by the failure of each.
#[derive(Debug)]
enum FailureCause {
    Owned(BoxError),
    #[cfg(ruststream_alloc)]
    Shared(Arc<BoxError>),
}

impl Failed {
    /// A failure of `message`, caused by `error`.
    #[doc(hidden)]
    #[must_use]
    pub const fn new(error: BoxError, message: FailedMessage) -> Self {
        Self {
            error: FailureCause::Owned(error),
            message,
        }
    }

    /// A failure of `message` whose error the failures of the rest of its publish share.
    #[cfg(ruststream_alloc)]
    fn shared(error: Arc<BoxError>, message: FailedMessage) -> Self {
        Self {
            error: FailureCause::Shared(error),
            message,
        }
    }

    /// Why the publish failed: the broker publisher's own error, the encode error, or the
    /// "no free record" kind.
    #[must_use]
    pub fn error(&self) -> &(dyn StdError + Send + Sync + 'static) {
        match &self.error {
            #[cfg(ruststream_alloc)]
            FailureCause::Owned(error) => &**error,
            #[cfg(not(ruststream_alloc))]
            FailureCause::Owned(error) => error.as_error(),
            #[cfg(ruststream_alloc)]
            FailureCause::Shared(error) => &***error,
        }
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
        #[cfg(ruststream_alloc)]
        let error = match self.error {
            FailureCause::Owned(error) => error,
            FailureCause::Shared(error) => Box::new(SharedCause(error)),
        };
        #[cfg(not(ruststream_alloc))]
        let FailureCause::Owned(error) = self.error;
        (error, self.message)
    }
}

#[cfg(ruststream_alloc)]
mod boxed;
#[cfg(ruststream_alloc)]
use boxed::SharedCause;
#[cfg(not(ruststream_alloc))]
mod fixed;

#[cfg(ruststream_alloc)]
pub(crate) use boxed::Fan;
#[cfg(ruststream_alloc)]
pub use boxed::{ErrorHook, Failures, Reaction, publish_done, publish_lost};
#[cfg(not(ruststream_alloc))]
pub(crate) use fixed::Unpublished;
#[cfg(not(ruststream_alloc))]
pub use fixed::{BatchFan, FailureSlot, Gathers, NoFan};
#[cfg(not(ruststream_alloc))]
pub use fixed::{ErrorHook, Failures, Reaction, publish_done, publish_lost};

#[cfg(test)]
#[cfg(ruststream_alloc)]
mod tests {

    use super::*;

    #[test]
    fn a_publish_that_went_out_returns_a_result_of_the_cause_alone() {
        assert!(size_of::<Result<(), Refusal>>() <= size_of::<BoxError>() + size_of::<usize>());
    }

    #[test]
    fn a_failed_message_splits_into_its_destination_bytes_and_headers() {
        let message = FailedMessage::new(
            "dest",
            Some(CopiedPayload::from_static(b"body")),
            HeaderSet::new(),
        );

        let (destination, payload, headers) = message.into_parts();

        assert_eq!(
            (destination.as_str(), payload.as_deref(), headers.len()),
            ("dest", Some(&b"body"[..]), 0)
        );
    }

    #[test]
    fn the_no_free_record_error_reports_the_capacity_of_its_store() {
        assert_eq!(NoFreeRecordError::new(3).capacity(), 3);
    }
}

#[cfg(test)]
#[cfg(not(ruststream_alloc))]
mod fixed_tests {
    use super::FailedMessage;
    use crate::HeaderSet;
    use crate::copied::NAME_CAPACITY;

    #[test]
    fn a_destination_that_fits_is_reported() {
        let message = FailedMessage::new("orders", None, HeaderSet::default());
        assert_eq!(message.destination(), Some("orders"));
    }

    #[test]
    fn a_destination_past_the_room_is_reported_as_none_not_as_empty() {
        let long = [b'd'; NAME_CAPACITY + 1];
        let long = core::str::from_utf8(&long).expect("ascii");
        let message = FailedMessage::new(long, None, HeaderSet::default());
        assert_eq!(message.destination(), None);
    }
}
