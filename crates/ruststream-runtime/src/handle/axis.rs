//! The input axis of the [`Handle`](super::Handle) trait: what the `In` parameter may be, and
//! what each spelling means to the mount machinery.
//!
//! The form rule is uniform - `&T` is one message, `&[T]` a batch of them - and the lane is the
//! type's own business: a `serde` type rides the codec, a [`Deserialized`] type constructs
//! itself from the payload bytes, a [`Message<H, P>`](Message) pair decodes its typed header
//! contract in the same stage, and a type on the carried lane is lent by the delivery that holds
//! it ([`Carries`](crate::Carries)). The spellings:
//!
//! | `In` | delivery | body parameter |
//! |---|---|---|
//! | `T` | one decoded message | `&T` |
//! | `Message<H, P>` | one decoded message + typed headers | `&Message<H, P>` |
//! | `F<'_>` where `F` is [`Deserialized`] | one payload, self-constructed | `&F<'_>` |
//! | `T` on the carried lane | the value one delivery holds | `&T` |
//! | `S` where `S` is an [`InMessages`](crate::InMessages) set | one decoded message, as the variant the set picks | `&S` |
//! | `[T]` | a batch of decoded messages | `&[T]` |
//! | `[Message<H, P>]` | a batch with typed headers per element | `&[Message<H, P>]` |
//! | `[F<'_>]` where `F` is [`Deserialized`] | a batch of self-constructed payloads | `&[F<'_>]` |
//! | `[T]` on the carried lane | the values one batch holds, as one slice | `&[T]` |
//! | `[S]` where `S` is an `InMessages` set | a batch of decoded messages, each as its variant | `&[S]` |
//!
//! Every projection the machinery needs (the decode kind, the verdict family, the schema of the
//! generated document) hangs off the lifetime-free [`Axis`] marker, so definitions can carry the
//! axis without carrying a borrow.

use core::marker::PhantomData;
use std::{error, fmt};

use serde::de::DeserializeOwned;

use crate::input::{Carried, Decoded, DecodedPair, InputKind, Provided};

use super::docs::DocState;
use super::verdict::{Batched, OneByOne, VerdictFamily};
use crate::router::forms;
use crate::router::forms::{BatchOut, CarriedBatch, Out, RawBatch, RawSubscribing, Subscribing};

/// One incoming message together with its decoded typed header contract.
///
/// As a [`Handle`](super::Handle) input (`Message<OrderHeaders, Order>`), the core decodes the
/// payload and the headers in the same stage, under the same
/// [`on_failure(decode = ..)`](crate::FailurePolicies) policy as the payload alone, and
/// the body receives ready values. The pairing is structural: each consumer declares the
/// contract it reads, so two subscribers of one subject may read different header sets.
///
/// As a reply type (`Handle<In, Message<EventHeaders, Confirmation>>`), the reply's headers ride
/// the value: the runtime serializes the contract into the outgoing headers and the payload
/// through the reply codec.
///
/// # Examples
///
/// ```
/// # #[cfg(feature = "json")]
/// # mod demo {
/// use ruststream::memory::prelude::*;
/// use serde::Deserialize;
///
/// #[derive(Deserialize, schemars::JsonSchema)]
/// struct OrderHeaders {
///     tenant: String,
/// }
///
/// #[derive(Deserialize, schemars::JsonSchema)]
/// struct Order {
///     id: u64,
/// }
///
/// struct Bill;
///
/// /// The payload and its header contract arrive decoded together.
/// impl Handle<Message<OrderHeaders, Order>> for Bill {
///     async fn handle(
///         &self,
///         msg: &Message<OrderHeaders, Order>,
///         _outs: &(),
///         _ctx: &mut Context<'_>,
///     ) -> Result<(), HandlerOutcome> {
///         tracing::info!(order = msg.body.id, tenant = msg.headers.tenant, "billed");
///         Ok(())
///     }
/// }
///
/// fn app() -> RustStream {
///     RustStream::new(AppInfo::new("billing", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
///         b.include(subscriber("orders", Bill).build());
///     })
/// }
/// # }
/// # fn main() {}
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Message<H, P> {
    /// The decoded header contract.
    pub headers: H,
    /// The decoded payload.
    pub body: P,
}

impl<H, P> Message<H, P> {
    /// Pairs a header contract with a payload.
    #[must_use]
    pub const fn new(headers: H, body: P) -> Self {
        Self { headers, body }
    }
}

/// A type that deserializes itself from the payload bytes: `Deserialize` means the framework's
/// codec does it, `Deserialized` means it is already done by the user's own type.
///
/// The core hands [`from_payload`](Self::from_payload) the delivery's bytes exactly as they
/// arrived - no codec runs, no decode policy of a codec applies, and nothing is copied: the
/// output borrows the broker's buffer. A failed construction is settled by the subscriber's
/// [`on_failure(decode = ..)`](crate::FailurePolicies) policy, the same rung a codec
/// decode failure lands on. This is the lane a zero-copy wire format (flatbuffers, capnp, a
/// hand-rolled frame) rides without pretending to be `serde`.
///
/// # Implementing by hand
///
/// `#[derive(Deserialized)]` (under the `macros` feature) covers a newtype or single-field
/// struct over `&'a [u8]`. Any other shape is a pair of short impls: the construction, and the
/// [`Input`] spelling that routes the type onto the self-deserializing lane. The batch spelling
/// comes for free - `&[Frame<'_>]` bodies mount off the same two impls:
///
/// ```
/// use ruststream::prelude::*;
///
/// struct Frame<'a>(&'a [u8]);
///
/// impl Deserialized for Frame<'_> {
///     type Output<'a> = Frame<'a>;
///     type Error = core::convert::Infallible;
///
///     fn from_payload(payload: &[u8]) -> Result<Frame<'_>, Self::Error> {
///         Ok(Frame(payload))
///     }
/// }
///
/// impl Input for Frame<'_> {
///     type Axis = SoloDeserialized<Frame<'static>>;
/// }
/// # fn check() -> Result<(), Box<dyn std::error::Error>> {
/// let frame = Frame::from_payload(b"bytes")?;
/// assert_eq!(frame.0, b"bytes");
/// # Ok(())
/// # }
/// # check().unwrap();
/// ```
pub trait Deserialized: Sized {
    /// This type at the payload's lifetime (`Frame<'a>` for a borrowing `Frame`, `Self` for an
    /// owning one).
    type Output<'a>: Send + Sync;

    /// The construction failure; `Infallible` for a plain view over the bytes.
    type Error: fmt::Display;

    /// Constructs the value from one delivery's payload, borrowing it.
    ///
    /// # Errors
    ///
    /// A constructor that validates (a flatbuffers verifier, a length check) reports the bad
    /// payload here; the subscriber's decode failure policy settles the delivery.
    fn from_payload(payload: &[u8]) -> Result<Self::Output<'_>, Self::Error>;
}

/// What a `#[wire(decode = ..)]` function returns, with its error erased so the derive need not
/// name a type it cannot see. Machinery behind the derive.
///
/// Only the fallible shape is admitted, which every self-describing format's reader has anyway:
/// a bare `T` and `Result<T, E>` overlap the moment the decoded type is itself a result, so the
/// two cannot both be accepted.
#[doc(hidden)]
pub trait DecodeOutcome<T> {
    /// Normalizes the outcome into an erased construction failure.
    ///
    /// # Errors
    ///
    /// Returns the format's own error, boxed.
    fn finish(self) -> Result<T, Box<dyn error::Error + Send + Sync>>;
}

impl<T, E: error::Error + Send + Sync + 'static> DecodeOutcome<T> for Result<T, E> {
    fn finish(self) -> Result<T, Box<dyn error::Error + Send + Sync>> {
        self.map_err(Into::into)
    }
}

/// A [`Handle`](super::Handle) input spelling: a decoded `T`, a [`Deserialized`] type, a
/// [`Message<H, P>`](Message) pair, a value the delivery already holds
/// ([`Carries`](crate::Carries)), or a batch (slice) of any of them.
#[diagnostic::on_unimplemented(
    message = "`{Self}` is not a handler input",
    note = "a body's input is `&T` (T: DeserializeOwned), `&F<'_>` (F: Deserialized - derive it \
            for a raw-payload type), `&Message<H, P>`, a value a broker's deliveries carry (the \
            broker crate's derive puts its type on that lane), or a batch: `&[T]`, `&[F<'_>]`, \
            `&[Message<H, P>]`"
)]
pub trait Input {
    /// The lifetime-free marker every machinery projection hangs off.
    type Axis: Axis;
}

/// The lifetime-free projection of one [`Input`] spelling. Machinery; never named in user code.
#[doc(hidden)]
pub trait Axis: Send + Sync + 'static {
    /// The verdict family ([`OneByOne`] / [`Batched`]).
    type Family: VerdictFamily;

    /// The element's decode kind (for a batch, the kind of one element).
    type Kind: InputKind;

    /// The mount form of the axis's plain (no reply, no injections) definition.
    type EagerForm;

    /// The mount form of the axis's slot-carrying (no reply) definition.
    type SlotForm;

    /// The serialized payload schema under the documentation state `Doc`.
    #[must_use]
    fn payload_schema<Doc: AxisDocs<Self>>() -> Option<String> {
        Doc::payload_schema()
    }
}

/// What one documentation state reports for one axis: the payload schema, and the headers
/// schema for a pair axis. Machinery behind documented-by-default; the `Doc` obligations
/// ([`Documentable`](super::Documentable) on the message types) live in these impls.
#[doc(hidden)]
pub trait AxisDocs<A: Axis + ?Sized> {
    /// The serialized JSON Schema of the payload type.
    fn payload_schema() -> Option<String>;

    /// The serialized JSON Schema of the typed header contract, for a pair axis.
    #[must_use]
    fn headers_schema() -> Option<String> {
        None
    }
}

/// The single decoded input: `In = T`.
#[derive(Debug)]
pub struct Solo<T>(PhantomData<T>);

/// The single self-deserializing input: `In = F<'_>` for a [`Deserialized`] `F`.
///
/// The parameter is the family's lifetime-free representative (`Frame<'static>`), which every
/// lifetime of the input projects to.
#[derive(Debug)]
pub struct SoloDeserialized<F>(PhantomData<F>);

/// The single pair input: `In = Message<H, P>`.
#[derive(Debug)]
pub struct SoloPair<H, P>(PhantomData<(H, P)>);

/// The single carried input: `In = T` for a `T` the subscription's deliveries lend
/// ([`Carries<T>`](crate::Carries)). A broker crate's derive selects it for the type, with
/// `impl Input for Row { type Axis = SoloCarried<Row>; }`.
///
/// # Examples
///
/// A handler of one carried value and one of a batch, over a broker crate whose derive puts `SendEmail` on this lane:
///
/// ```
/// # mod demo {
/// # mod db_queue {
/// #     // The broker crate's side: its broker, its descriptor, its deliveries and pages of rows.
/// #     use std::num::NonZeroUsize;
/// #     use futures::Stream;
/// #     use ruststream::memory::{ConnectedMemoryBroker, MemoryError};
/// #     use ruststream::runtime::{Input, SoloCarried};
/// #     use ruststream::{AckError, AddressedCopies, BatchSubscriber, Carries, CarriesBatch};
/// #     use ruststream::{HeaderMap, IncomingMessage, RedeliveryAddress, RedeliveryAddressed};
/// #     use ruststream::{Subscriber, SubscriptionSource};
/// #     use super::SendEmail;
/// #     pub mod prelude {
/// #         pub use ruststream::prelude::*;
/// #         pub use super::{QueueBroker, QueueTable};
/// #     }
/// #     pub type QueueBroker = ruststream::memory::MemoryBroker;
/// #     #[derive(Clone)]
/// #     pub struct QueueTable(&'static str);
/// #     impl QueueTable { pub fn new(table: &'static str) -> Self { Self(table) } }
/// #     pub struct Claims;
/// #     pub struct Claimed { bytes: Vec<u8>, headers: HeaderMap, row: Option<SendEmail> }
/// #     pub struct Page { rows: Vec<SendEmail>, claims: Vec<Claimed> }
/// #     impl SubscriptionSource<ConnectedMemoryBroker> for QueueTable {
/// #         type Subscriber = Claims;
/// #         type Copies = AddressedCopies;
/// #         fn name(&self) -> &str { self.0 }
/// #         async fn subscribe(self, _: &ConnectedMemoryBroker) -> Result<Claims, MemoryError> {
/// #             Ok(Claims)
/// #         }
/// #     }
/// #     impl RedeliveryAddressed<ConnectedMemoryBroker> for QueueTable {
/// #         async fn redelivery_address(&self, _: &ConnectedMemoryBroker)
/// #             -> Result<RedeliveryAddress, MemoryError> {
/// #             Ok(RedeliveryAddress::new(self.0))
/// #         }
/// #     }
/// #     impl Subscriber for Claims {
/// #         type Message = Claimed;
/// #         type Error = MemoryError;
/// #         fn stream(&mut self) -> impl Stream<Item = Result<Claimed, MemoryError>> + Send + '_ {
/// #             futures::stream::empty()
/// #         }
/// #     }
/// #     impl BatchSubscriber for Claims {
/// #         type Batch = Page;
/// #         fn batches(&mut self, _: NonZeroUsize)
/// #             -> impl Stream<Item = Result<Page, MemoryError>> + Send + '_ {
/// #             futures::stream::empty()
/// #         }
/// #     }
/// #     impl Carries<SendEmail> for Claimed {
/// #         fn carried(&self) -> Option<&SendEmail> { self.row.as_ref() }
/// #     }
/// #     impl CarriesBatch<SendEmail> for Page {
/// #         fn carried(&self) -> &[SendEmail] { &self.rows }
/// #     }
/// #     impl IntoIterator for Page {
/// #         type Item = Claimed;
/// #         type IntoIter = std::vec::IntoIter<Claimed>;
/// #         fn into_iter(self) -> Self::IntoIter { self.claims.into_iter() }
/// #     }
/// #     impl IncomingMessage for Claimed {
/// #         fn payload(&self) -> &[u8] { &self.bytes }
/// #         fn headers(&self) -> &HeaderMap { &self.headers }
/// #         async fn ack(self) -> Result<(), AckError> { Ok(()) }
/// #         async fn nack(self, _: bool) -> Result<(), AckError> { Ok(()) }
/// #     }
/// #     // What the broker crate's derive writes.
/// #     impl Input for SendEmail { type Axis = SoloCarried<Self>; }
/// # }
/// use db_queue::prelude::*;
///
/// /// One row of the queue table, as the database driver reads it.
/// // The broker crate's derive, beside `Clone`, puts the type on this lane.
/// #[derive(Clone)]
/// struct SendEmail {
///     to: String,
/// }
///
/// #[subscriber(QueueTable::new("email_jobs"))]
/// async fn send(email: &SendEmail) -> HandlerOutcome {
///     tracing::info!(to = %email.to, "sent");
///     HandlerOutcome::ack()
/// }
///
/// /// One SMTP session per page of rows.
/// #[subscriber(QueueTable::new("newsletter_jobs"))]
/// async fn send_newsletter(emails: &[SendEmail]) -> HandlerOutcome {
///     tracing::info!(count = emails.len(), "sent a page");
///     HandlerOutcome::ack()
/// }
///
/// fn app() -> RustStream {
///     RustStream::new(AppInfo::new("mailer", "0.1.0")).with_broker(QueueBroker::new(), |b| {
///         b.include(send);
///         b.include(send_newsletter.batch(nonzero!(64)));
///     })
/// }
/// # }
/// # fn main() {}
/// ```
#[derive(Debug)]
pub struct SoloCarried<T>(PhantomData<T>);

/// The single message-set input: `In = S` for an [`InMessages`](crate::InMessages)
/// enum `S`.
///
/// The set's variants each wrap one model, and the set decodes the delivery as the variant its
/// rule picks, through that model's own lane. `#[derive(InMessages)]` selects the axis for the
/// enum, with `impl Input for OrderEvent { type Axis = SoloSet<OrderEvent>; }`.
#[derive(Debug)]
pub struct SoloSet<S>(PhantomData<S>);

/// The decoded batch input: `In = [T]`.
#[derive(Debug)]
pub struct Batch<T>(PhantomData<T>);

/// The message-set batch input: `In = [S]` for an [`InMessages`](crate::InMessages)
/// enum `S`. See [`SoloSet`].
#[derive(Debug)]
pub struct BatchSet<S>(PhantomData<S>);

/// The self-deserializing batch input: `In = [F<'_>]` for a [`Deserialized`] `F`. See
/// [`SoloDeserialized`] for the parameter.
#[derive(Debug)]
pub struct BatchDeserialized<F>(PhantomData<F>);

/// The pair batch input: `In = [Message<H, P>]`.
#[derive(Debug)]
pub struct BatchPair<H, P>(PhantomData<(H, P)>);

/// The carried batch input: `In = [T]` for a `T` the subscription's batches lend as one slice
/// ([`CarriesBatch<T>`](crate::CarriesBatch)). See [`SoloCarried`].
#[derive(Debug)]
pub struct BatchCarried<T>(PhantomData<T>);

impl<T: DeserializeOwned + Send + Sync + 'static> Input for T {
    type Axis = Solo<T>;
}

impl<H, P> Input for Message<H, P>
where
    H: DeserializeOwned + Send + Sync + 'static,
    P: DeserializeOwned + Send + Sync + 'static,
{
    type Axis = SoloPair<H, P>;
}

/// The batch axis of one single-delivery axis: what `[T]` rides given what `T` rides.
///
/// The one slice `Input` impl projects through this, so every single spelling's batch comes for
/// free - a `Deserialized` type's own `Input` impl (or derive) makes `&[Frame<'_>]` bodies
/// mountable without a second impl, which the orphan rule would forbid downstream anyway
/// (`[T]` is nobody's local type). Machinery; never named in user code.
#[doc(hidden)]
pub trait BatchedFrom {
    /// The batch counterpart.
    type Batch: Axis;
}

impl<T: Send + Sync + 'static> BatchedFrom for Solo<T> {
    type Batch = Batch<T>;
}

impl<F: Send + Sync + 'static> BatchedFrom for SoloDeserialized<F> {
    type Batch = BatchDeserialized<F>;
}

impl<S: Send + Sync + 'static> BatchedFrom for SoloSet<S> {
    type Batch = BatchSet<S>;
}

impl<H: Send + Sync + 'static, P: Send + Sync + 'static> BatchedFrom for SoloPair<H, P> {
    type Batch = BatchPair<H, P>;
}

impl<T: Clone + Send + Sync + 'static> BatchedFrom for SoloCarried<T> {
    type Batch = BatchCarried<T>;
}

// No overlap with the decoded blanket above: that one is implicitly `Sized`, a slice is not.
impl<E> Input for [E]
where
    E: Input,
    E::Axis: BatchedFrom,
{
    type Axis = <E::Axis as BatchedFrom>::Batch;
}

impl<T: Send + Sync + 'static> Axis for Solo<T> {
    type Family = OneByOne;
    type Kind = Decoded<T>;
    type EagerForm = Subscribing;
    type SlotForm = Out;
}

impl<F: Send + Sync + 'static> Axis for SoloDeserialized<F> {
    type Family = OneByOne;
    type Kind = Provided<F>;
    type EagerForm = RawSubscribing;
    type SlotForm = Out;
}

// A set is a typed input: it decodes with the codec of the mount into an owned value, so it
// rides the decoded kind and mounts in the forms a decoded input does.
impl<S: Send + Sync + 'static> Axis for SoloSet<S> {
    type Family = OneByOne;
    type Kind = Decoded<S>;
    type EagerForm = Subscribing;
    type SlotForm = Out;
}

impl<H: Send + Sync + 'static, P: Send + Sync + 'static> Axis for SoloPair<H, P> {
    type Family = OneByOne;
    type Kind = DecodedPair<H, P>;
    type EagerForm = Subscribing;
    type SlotForm = Out;
}

impl<T: Clone + Send + Sync + 'static> Axis for SoloCarried<T> {
    type Family = OneByOne;
    type Kind = Carried<T>;
    type EagerForm = Subscribing;
    type SlotForm = Out;
}

impl<T: Send + Sync + 'static> Axis for Batch<T> {
    type Family = Batched;
    type Kind = Decoded<T>;
    type EagerForm = forms::Batch;
    type SlotForm = BatchOut;
}

impl<F: Send + Sync + 'static> Axis for BatchDeserialized<F> {
    type Family = Batched;
    type Kind = Provided<F>;
    type EagerForm = RawBatch;
    type SlotForm = BatchOut;
}

impl<S: Send + Sync + 'static> Axis for BatchSet<S> {
    type Family = Batched;
    type Kind = Decoded<S>;
    type EagerForm = forms::Batch;
    type SlotForm = BatchOut;
}

impl<H: Send + Sync + 'static, P: Send + Sync + 'static> Axis for BatchPair<H, P> {
    type Family = Batched;
    type Kind = DecodedPair<H, P>;
    type EagerForm = forms::Batch;
    type SlotForm = BatchOut;
}

impl<T: Clone + Send + Sync + 'static> Axis for BatchCarried<T> {
    type Family = Batched;
    type Kind = Carried<T>;
    type EagerForm = CarriedBatch;
    type SlotForm = BatchOut;
}

impl<T: Send + Sync + 'static, Doc: DocState<T>> AxisDocs<Solo<T>> for Doc {
    fn payload_schema() -> Option<String> {
        Doc::schema()
    }
}

// A message set has no payload schema of its own: each of its models carries one, and the
// document lists them one by one. No documentation state demands anything of the set.
impl<S: Send + Sync + 'static, Doc> AxisDocs<SoloSet<S>> for Doc {
    fn payload_schema() -> Option<String> {
        None
    }
}

// A self-deserializing payload has no serde model, so no schema is demanded or produced in any
// documentation state.
impl<F, Doc> AxisDocs<SoloDeserialized<F>> for Doc
where
    F: Send + Sync + 'static,
{
    fn payload_schema() -> Option<String> {
        None
    }
}

// A carried value is the service's own type, so it documents itself exactly like a decoded one.
impl<T: Clone + Send + Sync + 'static, Doc: DocState<T>> AxisDocs<SoloCarried<T>> for Doc {
    fn payload_schema() -> Option<String> {
        Doc::schema()
    }
}

impl<H, P, Doc> AxisDocs<SoloPair<H, P>> for Doc
where
    H: Send + Sync + 'static,
    P: Send + Sync + 'static,
    Doc: DocState<P> + DocState<H>,
{
    fn payload_schema() -> Option<String> {
        <Doc as DocState<P>>::schema()
    }

    fn headers_schema() -> Option<String> {
        <Doc as DocState<H>>::schema()
    }
}

impl<T: Send + Sync + 'static, Doc: DocState<T>> AxisDocs<Batch<T>> for Doc {
    fn payload_schema() -> Option<String> {
        Doc::schema()
    }
}

impl<S: Send + Sync + 'static, Doc> AxisDocs<BatchSet<S>> for Doc {
    fn payload_schema() -> Option<String> {
        None
    }
}

impl<F, Doc> AxisDocs<BatchDeserialized<F>> for Doc
where
    F: Send + Sync + 'static,
{
    fn payload_schema() -> Option<String> {
        None
    }
}

impl<T: Clone + Send + Sync + 'static, Doc: DocState<T>> AxisDocs<BatchCarried<T>> for Doc {
    fn payload_schema() -> Option<String> {
        Doc::schema()
    }
}

impl<H, P, Doc> AxisDocs<BatchPair<H, P>> for Doc
where
    H: Send + Sync + 'static,
    P: Send + Sync + 'static,
    Doc: DocState<P> + DocState<H>,
{
    fn payload_schema() -> Option<String> {
        <Doc as DocState<P>>::schema()
    }

    fn headers_schema() -> Option<String> {
        <Doc as DocState<H>>::schema()
    }
}

/// A single-delivery axis: the plain, raw and pair spellings of one message at a time. The
/// bound behind the forms that make no sense for a batch (a reply's destination is one message's
/// business).
#[doc(hidden)]
pub trait SoloAxis: Axis<Family = OneByOne> {}

impl<T: Send + Sync + 'static> SoloAxis for Solo<T> {}
impl<F: Send + Sync + 'static> SoloAxis for SoloDeserialized<F> {}
impl<S: Send + Sync + 'static> SoloAxis for SoloSet<S> {}
impl<H: Send + Sync + 'static, P: Send + Sync + 'static> SoloAxis for SoloPair<H, P> {}
impl<T: Clone + Send + Sync + 'static> SoloAxis for SoloCarried<T> {}

/// A batch axis: the slice spellings. The bound behind `.batch(..)`.
#[doc(hidden)]
pub trait BatchedAxis: Axis<Family = Batched> {}

impl<T: Send + Sync + 'static> BatchedAxis for Batch<T> {}
impl<F: Send + Sync + 'static> BatchedAxis for BatchDeserialized<F> {}
impl<S: Send + Sync + 'static> BatchedAxis for BatchSet<S> {}
impl<H: Send + Sync + 'static, P: Send + Sync + 'static> BatchedAxis for BatchPair<H, P> {}
impl<T: Clone + Send + Sync + 'static> BatchedAxis for BatchCarried<T> {}
