//! Message-type metadata for `AsyncAPI` generation.

/// Metadata about a message type, used to name and describe it in an `AsyncAPI` document.
///
/// Derive with `#[derive(Message)]` (requires the `macros` feature): the derive uses the type's
/// name as [`NAME`](Self::NAME) and its doc comment as [`DESCRIPTION`](Self::DESCRIPTION).
///
/// # Examples
///
/// ```
/// use ruststream::Message;
///
/// struct Order;
/// impl Message for Order {
///     const NAME: &'static str = "Order";
/// }
///
/// assert_eq!(Order::NAME, "Order");
/// ```
pub trait Message {
    /// The message name, used as the `AsyncAPI` component name.
    const NAME: &'static str;

    /// An optional description, typically from the type's doc comment.
    const DESCRIPTION: Option<&'static str> = None;
}

/// Declares a message type's typed header contract.
///
/// `#[derive(Message)]` implements it: `#[message(headers(Meta))]` on the type sets the
/// contract to [`WithHeaders<Meta>`], and a derive without the attribute sets [`NoHeaders`].
/// The contract types both message documentation (the `AsyncAPI` headers schema of the
/// message) and the typed publish path: a slot's
/// [`publish_typed`](crate::runtime::TypedSlot::publish_typed) compiles only with the
/// headers the contract declares (none for [`NoHeaders`]).
///
/// The two contract shapes are a closed vocabulary ([`HeadersContract`] is sealed), so the
/// publish machinery can branch on them at compile time without negative reasoning.
///
/// # Examples
///
/// ```
/// use ruststream::{MessageHeaders, WithHeaders};
///
/// struct ChunkMeta {
///     task_id: u64,
/// }
///
/// struct ChunkDone;
///
/// // What `#[derive(Message)]` + `#[message(headers(ChunkMeta))]` generates:
/// impl MessageHeaders for ChunkDone {
///     type Contract = WithHeaders<ChunkMeta>;
/// }
/// ```
#[diagnostic::on_unimplemented(
    message = "`{Self}` does not declare a header contract",
    note = "derive `Message` on the type: `#[message(headers(Meta))]` declares typed headers, \
            a derive without the attribute declares none"
)]
pub trait MessageHeaders {
    /// The contract shape: [`NoHeaders`], or [`WithHeaders<H>`](WithHeaders) with the typed
    /// header struct `H`.
    type Contract: HeadersContract;
}

/// The header contract of a message that carries no typed headers.
///
/// See [`MessageHeaders`]; a `#[derive(Message)]` without a `#[message(headers(..))]` attribute
/// declares this contract.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
pub struct NoHeaders;

// Bare collection payloads are common message bodies (a batch as `Vec<Item>`, a plain text
// message) but cannot derive `Message`, and the orphan rule keeps downstream crates from
// implementing this foreign trait on them - so the contract lives here: none. A collection
// payload that needs a header contract wraps in a `#[derive(Message)]` newtype.
impl<T> MessageHeaders for Vec<T> {
    type Contract = NoHeaders;
}

impl MessageHeaders for String {
    type Contract = NoHeaders;
}

/// The header contract of a message whose headers are the typed struct `H`.
///
/// See [`MessageHeaders`]; `#[message(headers(H))]` on a `#[derive(Message)]` type declares
/// this contract. `H` is serialized into the header map on the typed publish path and lifted
/// into the message's `AsyncAPI` headers schema when it implements [`schemars::JsonSchema`].
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
pub struct WithHeaders<H>(core::marker::PhantomData<fn() -> H>);

/// The closed set of header contract shapes: [`NoHeaders`] and [`WithHeaders<H>`](WithHeaders).
///
/// Sealed: the publish machinery matches on exactly these two shapes, so the vocabulary cannot
/// grow outside the crate.
pub trait HeadersContract: sealed::Sealed {}

impl HeadersContract for NoHeaders {}
impl<H> HeadersContract for WithHeaders<H> {}

mod sealed {
    /// Seals [`HeadersContract`](super::HeadersContract): the two shapes are the whole
    /// vocabulary.
    pub trait Sealed {}

    impl Sealed for super::NoHeaders {}
    impl<H> Sealed for super::WithHeaders<H> {}
}
