//! The input axis of the [`Handle`](super::Handle) trait: what the `In` parameter may be, and
//! what each spelling means to the mount machinery.
//!
//! The spellings and their meanings:
//!
//! | `In` | delivery | body parameter |
//! |---|---|---|
//! | `T` | one decoded message | `&T` |
//! | `Payload<'_>` | one raw payload | `&Payload<'_>` (derefs to `&[u8]`) |
//! | `Message<H, P>` | one decoded message + typed headers | `&Message<H, P>` |
//! | `[T]` | a page of decoded messages | `&[T]` |
//! | `[Payload<'_>]` | a page of raw payloads | `&[Payload<'_>]` |
//! | `[Message<H, P>]` | a page with typed headers per element | `&[Message<H, P>]` |
//!
//! The raw single spelling is a wrapper rather than `[u8]` itself because `[u8]` is also `[T]`
//! at `T = u8`, and coherence cannot split a slice of bytes from a page of decoded `u8`
//! elements; the wrapper keeps the two meanings apart at zero cost (it borrows the payload), on the single and the page
//! spelling alike.
//!
//! Every projection the machinery needs (the decode kind, the verdict family, the schema of the
//! generated document) hangs off the lifetime-free [`Axis`] marker, so definitions can carry the
//! axis without carrying a borrow.

use serde::de::DeserializeOwned;

use crate::runtime::input::{Decoded, DecodedPair, InputKind, RawBytes};

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

/// One raw payload, borrowed from the delivery: the input spelling of a byte-level body.
///
/// Dereferences to `&[u8]`; nothing is decoded and nothing is copied.
///
/// # Examples
///
/// ```
/// use ruststream::runtime::Payload;
///
/// # fn check() {
/// let bytes = [7u8, 9];
/// let payload = Payload::new(&bytes);
/// assert_eq!(&payload[..], &bytes);
/// # }
/// # check();
/// ```
#[derive(Debug, Clone, Copy)]
pub struct Payload<'a>(&'a [u8]);

impl<'a> Payload<'a> {
    /// Wraps one delivery's payload bytes.
    #[must_use]
    pub const fn new(bytes: &'a [u8]) -> Self {
        Self(bytes)
    }
}

impl std::ops::Deref for Payload<'_> {
    type Target = [u8];

    fn deref(&self) -> &[u8] {
        self.0
    }
}

impl AsRef<[u8]> for Payload<'_> {
    fn as_ref(&self) -> &[u8] {
        self.0
    }
}

/// A [`Handle`](super::Handle) input spelling. See the [module docs](self) for the closed set.
#[diagnostic::on_unimplemented(
    message = "`{Self}` is not a handler input",
    note = "a body's input is `&T` (T: DeserializeOwned), `&Payload<'_>` (raw bytes), \
            `&Message<H, P>`, or a page: `&[T]`, `&[Payload<'_>]`, `&[Message<H, P>]`"
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

    /// The serialized payload schema under the documentation state `Doc`.
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
    fn headers_schema() -> Option<String> {
        None
    }
}

/// The single decoded input: `In = T`.
pub struct Solo<T>(core::marker::PhantomData<T>);

/// The single raw input: `In = Payload<'_>`.
pub struct SoloBytes;

/// The single pair input: `In = Message<H, P>`.
pub struct SoloPair<H, P>(core::marker::PhantomData<(H, P)>);

/// The decoded page input: `In = [T]`.
pub struct Page<T>(core::marker::PhantomData<T>);

/// The raw page input: `In = [Payload<'_>]`.
pub struct PageBytes;

/// The pair page input: `In = [Message<H, P>]`.
pub struct PagePair<H, P>(core::marker::PhantomData<(H, P)>);

impl<T: DeserializeOwned + Send + Sync + 'static> Input for T {
    type Axis = Solo<T>;
}

impl<'a> Input for Payload<'a> {
    type Axis = SoloBytes;
}

impl<H, P> Input for Message<H, P>
where
    H: DeserializeOwned + Send + Sync + 'static,
    P: DeserializeOwned + Send + Sync + 'static,
{
    type Axis = SoloPair<H, P>;
}

impl<T: DeserializeOwned + Send + Sync + 'static> Input for [T] {
    type Axis = Page<T>;
}

impl<'a> Input for [Payload<'a>] {
    type Axis = PageBytes;
}

impl<H, P> Input for [Message<H, P>]
where
    H: DeserializeOwned + Send + Sync + 'static,
    P: DeserializeOwned + Send + Sync + 'static,
{
    type Axis = PagePair<H, P>;
}

impl<T: Send + Sync + 'static> Axis for Solo<T> {
    type Family = OneByOne;
    type Kind = Decoded<T>;
    type EagerForm = crate::runtime::router::forms::Subscribing;
}

impl Axis for SoloBytes {
    type Family = OneByOne;
    type Kind = RawBytes;
    type EagerForm = crate::runtime::router::forms::RawSubscribing;
}

impl<H: Send + Sync + 'static, P: Send + Sync + 'static> Axis for SoloPair<H, P> {
    type Family = OneByOne;
    type Kind = DecodedPair<H, P>;
    type EagerForm = crate::runtime::router::forms::Subscribing;
}

impl<T: Send + Sync + 'static> Axis for Page<T> {
    type Family = Paged;
    type Kind = Decoded<T>;
    type EagerForm = crate::runtime::router::forms::Batch;
}

impl Axis for PageBytes {
    type Family = Paged;
    type Kind = RawBytes;
    type EagerForm = crate::runtime::router::forms::RawBatch;
}

impl<H: Send + Sync + 'static, P: Send + Sync + 'static> Axis for PagePair<H, P> {
    type Family = Paged;
    type Kind = DecodedPair<H, P>;
    type EagerForm = crate::runtime::router::forms::Batch;
}

impl<T: Send + Sync + 'static, Doc: DocState<T>> AxisDocs<Solo<T>> for Doc {
    fn payload_schema() -> Option<String> {
        Doc::schema()
    }
}

impl<Doc> AxisDocs<SoloBytes> for Doc {
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

impl<Doc> AxisDocs<PageBytes> for Doc {
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
impl SoloAxis for SoloBytes {}
impl<H: Send + Sync + 'static, P: Send + Sync + 'static> SoloAxis for SoloPair<H, P> {}

/// A page axis: the slice spellings. The bound behind `.batch(..)`.
#[doc(hidden)]
pub trait PagedAxis: Axis<Family = Paged> {}

impl<T: Send + Sync + 'static> PagedAxis for Page<T> {}
impl PagedAxis for PageBytes {}
impl<H: Send + Sync + 'static, P: Send + Sync + 'static> PagedAxis for PagePair<H, P> {}
