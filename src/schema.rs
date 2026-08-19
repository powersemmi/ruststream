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

/// Declares where a message type is sent.
///
/// `#[derive(Outgoing)]` implements it from the type's `#[outgoing(name = ..)]` attribute, and
/// [`Form`](Self::Form) is what the publish builder reads to decide which destination position
/// the call site has to fill:
///
/// * `#[outgoing(name = "orders.done")]` declares [`FixedName`]: the destination is resolved by
///   the declaration, and the builder offers no `to(..)` at all.
/// * `#[outgoing(name = "orders.{tenant}.v1")]` declares [`NameTemplate`]: the builder's `to()`
///   opens one setter per placeholder, and the publish terminal appears once every one of them
///   is bound.
/// * a derive with no `name` declares [`CallerName`]: the builder takes the destination as
///   `to("orders.archived")`.
///
/// In every case the destination exists before the publish does, so an under-specified call site
/// is a compile error rather than a runtime surprise.
///
/// # Examples
///
/// ```
/// use ruststream::{FixedName, OutgoingDestination};
///
/// struct OrderDone;
///
/// // What `#[derive(Outgoing)]` + `#[outgoing(name = "orders.done")]` generates:
/// impl OutgoingDestination for OrderDone {
///     type Form = FixedName;
///     const ADDRESS: &'static str = "orders.done";
/// }
///
/// assert_eq!(OrderDone::ADDRESS, "orders.done");
/// ```
#[diagnostic::on_unimplemented(
    message = "`{Self}` does not declare how it is sent",
    note = "derive it on the message type: `#[derive(Outgoing)]` alone lets the call site name \
            the destination, `#[outgoing(name = \"orders.done\")]` fixes it, and \
            `#[outgoing(name = \"orders.{{tenant}}.v1\")]` opens one setter per placeholder"
)]
pub trait OutgoingDestination {
    /// The declared form: [`FixedName`], [`NameTemplate`] or [`CallerName`].
    type Form: DestinationForm;

    /// The declared address: the literal name, the template with its placeholders in braces, or
    /// the empty string when the type declares none.
    const ADDRESS: &'static str = "";

    /// The template's placeholder names, in the order they appear in [`ADDRESS`](Self::ADDRESS).
    /// Empty for the other two forms; the generated document turns it into the channel's
    /// parameters block.
    const PARAMETERS: &'static [&'static str] = &[];
}

/// The destination form of a type declaring one literal name: the address is fixed, so the
/// publish builder resolves it without asking the call site.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
pub struct FixedName;

/// The destination form of a type declaring a name template: the call site binds every
/// placeholder through the setters generated from it, and the address is rendered per publish.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
pub struct NameTemplate;

/// The destination form of a type declaring no name: the call site names the destination.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
pub struct CallerName;

/// The closed set of destination forms: [`FixedName`], [`NameTemplate`] and [`CallerName`].
///
/// Sealed, like [`HeadersContract`]: the publish builder branches on exactly these three shapes
/// at compile time, so the vocabulary cannot grow outside the crate.
pub trait DestinationForm: sealed::Sealed + Default {}

impl DestinationForm for FixedName {}
impl DestinationForm for NameTemplate {}
impl DestinationForm for CallerName {}

mod sealed {
    /// Seals [`HeadersContract`](super::HeadersContract) and
    /// [`DestinationForm`](super::DestinationForm): each vocabulary is closed.
    pub trait Sealed {}

    impl Sealed for super::NoHeaders {}
    impl<H> Sealed for super::WithHeaders<H> {}
    impl Sealed for super::FixedName {}
    impl Sealed for super::NameTemplate {}
    impl Sealed for super::CallerName {}
}
