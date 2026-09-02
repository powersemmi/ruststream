//! The input axis of the [`Handle`](super::Handle) trait: what the `In` parameter may be, and
//! what each spelling means to the mount machinery.
//!
//! The form rule is uniform - `&T` is one message, `&[T]` a page of them - and the lane is the
//! type's own business: a `serde` type rides the codec, a [`Deserialized`] type constructs
//! itself from the payload bytes, and a [`Message<H, P>`](Message) pair decodes its typed
//! header contract in the same stage. The spellings:
//!
//! | `In` | delivery | body parameter |
//! |---|---|---|
//! | `T` | one decoded message | `&T` |
//! | `Message<H, P>` | one decoded message + typed headers | `&Message<H, P>` |
//! | `F<'_>` where `F` is [`Deserialized`] | one payload, self-constructed | `&F<'_>` |
//! | `[T]` | a page of decoded messages | `&[T]` |
//! | `[Message<H, P>]` | a page with typed headers per element | `&[Message<H, P>]` |
//! | `[F<'_>]` where `F` is [`Deserialized`] | a page of self-constructed payloads | `&[F<'_>]` |
//!
//! Every projection the machinery needs (the decode kind, the verdict family, the schema of the
//! generated document) hangs off the lifetime-free [`Axis`] marker, so definitions can carry the
//! axis without carrying a borrow.

use serde::de::DeserializeOwned;

use crate::runtime::input::{Decoded, DecodedPair, InputKind, Provided};

use super::docs::DocState;
use super::verdict::{OneByOne, Paged, VerdictFamily};

/// One incoming message together with its decoded typed header contract.
///
/// As a [`Handle`](super::Handle) input (`Message<OrderHeaders, Order>`), the core decodes the
/// payload and the headers in the same stage, under the same
/// [`on_failure(decode = ..)`](crate::runtime::FailurePolicies) policy as the payload alone, and
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
/// use ruststream::runtime::Message;
///
/// # #[derive(Debug)]
/// # struct OrderHeaders { tenant: String }
/// # #[derive(Debug)]
/// # struct Order { id: u64 }
/// # fn check() -> Result<(), Box<dyn std::error::Error>> {
/// let msg = Message::new(OrderHeaders { tenant: "acme".into() }, Order { id: 7 });
/// assert_eq!(msg.headers.tenant, "acme");
/// assert_eq!(msg.body.id, 7);
/// # Ok(())
/// # }
/// # check().unwrap();
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
/// [`on_failure(decode = ..)`](crate::runtime::FailurePolicies) policy, the same rung a codec
/// decode failure lands on. This is the lane a zero-copy wire format (flatbuffers, capnp, a
/// hand-rolled frame) rides without pretending to be `serde`.
///
/// # Implementing by hand
///
/// `#[derive(Deserialized)]` (under the `macros` feature) covers a newtype or single-field
/// struct over `&'a [u8]`. Any other shape is a pair of short impls: the construction, and the
/// [`Input`] spelling that routes the type onto the self-deserializing lane. The page spelling
/// comes for free - `&[Frame<'_>]` bodies mount off the same two impls:
///
/// ```
/// use ruststream::runtime::{Deserialized, Input, SoloDeserialized};
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
    type Error: std::fmt::Display;

    /// Constructs the value from one delivery's payload, borrowing it.
    ///
    /// # Errors
    ///
    /// A constructor that validates (a flatbuffers verifier, a length check) reports the bad
    /// payload here; the subscriber's decode failure policy settles the delivery.
    fn from_payload(payload: &[u8]) -> Result<Self::Output<'_>, Self::Error>;
}

/// A [`Handle`](super::Handle) input spelling: a decoded `T`, a [`Deserialized`] type, a
/// [`Message<H, P>`](Message) pair, or a page (slice) of any of them.
#[diagnostic::on_unimplemented(
    message = "`{Self}` is not a handler input",
    note = "a body's input is `&T` (T: DeserializeOwned), `&F<'_>` (F: Deserialized - derive it \
            for a raw-payload type), `&Message<H, P>`, or a page: `&[T]`, `&[F<'_>]`, \
            `&[Message<H, P>]`"
)]
pub trait Input {
    /// The lifetime-free marker every machinery projection hangs off.
    type Axis: Axis;
}

/// The lifetime-free projection of one [`Input`] spelling. Machinery; never named in user code.
#[doc(hidden)]
pub trait Axis: Send + Sync + 'static {
    /// The verdict family ([`OneByOne`] / [`Paged`]).
    type Family: VerdictFamily;

    /// The element's decode kind (for a page, the kind of one element).
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
pub struct Solo<T>(core::marker::PhantomData<T>);

/// The single self-deserializing input: `In = F<'_>` for a [`Deserialized`] `F`.
///
/// The parameter is the family's lifetime-free representative (`Frame<'static>`), which every
/// lifetime of the input projects to.
#[derive(Debug)]
pub struct SoloDeserialized<F>(core::marker::PhantomData<F>);

/// The single pair input: `In = Message<H, P>`.
#[derive(Debug)]
pub struct SoloPair<H, P>(core::marker::PhantomData<(H, P)>);

/// The decoded page input: `In = [T]`.
#[derive(Debug)]
pub struct Page<T>(core::marker::PhantomData<T>);

/// The self-deserializing page input: `In = [F<'_>]` for a [`Deserialized`] `F`. See
/// [`SoloDeserialized`] for the parameter.
#[derive(Debug)]
pub struct PageDeserialized<F>(core::marker::PhantomData<F>);

/// The pair page input: `In = [Message<H, P>]`.
#[derive(Debug)]
pub struct PagePair<H, P>(core::marker::PhantomData<(H, P)>);

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

/// The page axis of one single-delivery axis: what `[T]` rides given what `T` rides.
///
/// The one slice `Input` impl projects through this, so every single spelling's page comes for
/// free - a `Deserialized` type's own `Input` impl (or derive) makes `&[Frame<'_>]` bodies
/// mountable without a second impl, which the orphan rule would forbid downstream anyway
/// (`[T]` is nobody's local type). Machinery; never named in user code.
#[doc(hidden)]
pub trait PagedFrom {
    /// The page counterpart.
    type Page: Axis;
}

impl<T: Send + Sync + 'static> PagedFrom for Solo<T> {
    type Page = Page<T>;
}

impl<F: Send + Sync + 'static> PagedFrom for SoloDeserialized<F> {
    type Page = PageDeserialized<F>;
}

impl<H: Send + Sync + 'static, P: Send + Sync + 'static> PagedFrom for SoloPair<H, P> {
    type Page = PagePair<H, P>;
}

// No overlap with the decoded blanket above: that one is implicitly `Sized`, a slice is not.
impl<E> Input for [E]
where
    E: Input,
    E::Axis: PagedFrom,
{
    type Axis = <E::Axis as PagedFrom>::Page;
}

impl<T: Send + Sync + 'static> Axis for Solo<T> {
    type Family = OneByOne;
    type Kind = Decoded<T>;
    type EagerForm = crate::runtime::router::forms::Subscribing;
    type SlotForm = crate::runtime::router::forms::Out;
}

impl<F: Send + Sync + 'static> Axis for SoloDeserialized<F> {
    type Family = OneByOne;
    type Kind = Provided<F>;
    type EagerForm = crate::runtime::router::forms::RawSubscribing;
    type SlotForm = crate::runtime::router::forms::Out;
}

impl<H: Send + Sync + 'static, P: Send + Sync + 'static> Axis for SoloPair<H, P> {
    type Family = OneByOne;
    type Kind = DecodedPair<H, P>;
    type EagerForm = crate::runtime::router::forms::Subscribing;
    type SlotForm = crate::runtime::router::forms::Out;
}

impl<T: Send + Sync + 'static> Axis for Page<T> {
    type Family = Paged;
    type Kind = Decoded<T>;
    type EagerForm = crate::runtime::router::forms::Batch;
    type SlotForm = crate::runtime::router::forms::BatchOut;
}

impl<F: Send + Sync + 'static> Axis for PageDeserialized<F> {
    type Family = Paged;
    type Kind = Provided<F>;
    type EagerForm = crate::runtime::router::forms::RawBatch;
    type SlotForm = crate::runtime::router::forms::BatchOut;
}

impl<H: Send + Sync + 'static, P: Send + Sync + 'static> Axis for PagePair<H, P> {
    type Family = Paged;
    type Kind = DecodedPair<H, P>;
    type EagerForm = crate::runtime::router::forms::Batch;
    type SlotForm = crate::runtime::router::forms::BatchOut;
}

impl<T: Send + Sync + 'static, Doc: DocState<T>> AxisDocs<Solo<T>> for Doc {
    fn payload_schema() -> Option<String> {
        Doc::schema()
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

impl<T: Send + Sync + 'static, Doc: DocState<T>> AxisDocs<Page<T>> for Doc {
    fn payload_schema() -> Option<String> {
        Doc::schema()
    }
}

impl<F, Doc> AxisDocs<PageDeserialized<F>> for Doc
where
    F: Send + Sync + 'static,
{
    fn payload_schema() -> Option<String> {
        None
    }
}

impl<H, P, Doc> AxisDocs<PagePair<H, P>> for Doc
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
/// bound behind the forms that make no sense for a page (a reply's destination is one message's
/// business).
#[doc(hidden)]
pub trait SoloAxis: Axis<Family = OneByOne> {}

impl<T: Send + Sync + 'static> SoloAxis for Solo<T> {}
impl<F: Send + Sync + 'static> SoloAxis for SoloDeserialized<F> {}
impl<H: Send + Sync + 'static, P: Send + Sync + 'static> SoloAxis for SoloPair<H, P> {}

/// A page axis: the slice spellings. The bound behind `.batch(..)`.
#[doc(hidden)]
pub trait PagedAxis: Axis<Family = Paged> {}

impl<T: Send + Sync + 'static> PagedAxis for Page<T> {}
impl<F: Send + Sync + 'static> PagedAxis for PageDeserialized<F> {}
impl<H: Send + Sync + 'static, P: Send + Sync + 'static> PagedAxis for PagePair<H, P> {}
