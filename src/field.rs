//! Compile-time field selectors: typed keys that read (and optionally write) one field of a
//! source by monomorphization, with no hashing, boxing, or downcasting.
//!
//! A key is a zero-sized type implementing [`Field`] for the sources that carry its field. The
//! runtime threads the source (a per-delivery context value or the shared application state) and
//! resolves `key.get(src)` to a direct field access. A key middleware writes also implements
//! [`FieldMut`]. Because a key implements [`Field`] only for the sources that actually carry its
//! field, applying an inapplicable key is a compile error rather than a runtime miss.

/// A compile-time key reading one typed field out of `Src`.
///
/// Implemented by a zero-sized selector type for each source that carries the field. Resolution is
/// monomorphized to a direct field read: no hash, no `Box`, no downcast. The key is taken by value
/// (selectors are `Copy` zero-sized types), and the read borrows from the source for the call.
///
/// # Examples
///
/// ```
/// use ruststream::runtime::{Context, HandlerOutcome};
/// use ruststream::{Field, IncomingMessage};
///
/// /// A broker's per-delivery context.
/// struct Delivery {
///     sequence: u64,
/// }
///
/// /// The broker's key for the sequence number a delivery carries.
/// #[derive(Clone, Copy)]
/// struct Sequence;
///
/// impl Field<Delivery> for Sequence {
///     type Value<'a> = u64;
///     fn get(self, src: &Delivery) -> u64 {
///         src.sequence
///     }
/// }
///
/// async fn audit<M: IncomingMessage>(
///     _msg: &M,
///     ctx: &mut Context<'_, Delivery>,
/// ) -> HandlerOutcome {
///     tracing::info!(sequence = ctx.context(Sequence), "audited");
///     HandlerOutcome::ack()
/// }
/// ```
pub trait Field<Src: ?Sized> {
    /// The value read through this key, borrowed from the source for `'a`.
    type Value<'a>
    where
        Src: 'a;

    /// Reads this key's field out of `src`.
    fn get(self, src: &Src) -> Self::Value<'_>;
}

/// A [`Field`]-style key that also names its context type and yields an owned value.
///
/// It powers the [`Ctx`](crate::runtime::Ctx) extractor: because the key carries its context
/// as an associated type, a handler taking `Ctx(value): Ctx<Key>` needs no `&mut Context`
/// parameter at all - the `#[subscriber]` macro projects the subscription's context type from
/// the first `Ctx` key it sees. The value is owned (`'static`): extractor values bind before the
/// handler body runs, so borrowing from the context is not an option. Borrowed
/// keys keep working through [`Field`] and `ctx.context(KEY)`.
///
/// Broker crates implement it next to [`Field`] on the same zero-sized keys; a key typically
/// implements both, so it works as a context read and as an extractor.
///
/// # Examples
///
/// ```
/// # #[cfg(all(feature = "macros", feature = "json"))]
/// # mod demo {
/// use ruststream::ContextField;
/// use ruststream::prelude::*;
/// use serde::Deserialize;
///
/// /// A broker's per-delivery context.
/// struct Delivery {
///     sequence: u64,
/// }
///
/// /// The broker's key for the sequence number a delivery carries.
/// #[derive(Clone, Copy, Default)]
/// struct Sequence;
///
/// impl ContextField for Sequence {
///     type Context = Delivery;
///     type Value = u64;
///     fn read(self, src: &Delivery) -> u64 {
///         src.sequence
///     }
/// }
///
/// #[derive(Deserialize)]
/// struct Order {
///     id: u64,
/// }
///
/// #[subscriber("orders")]
/// async fn audit(order: &Order, Ctx(sequence): Ctx<Sequence>) -> HandlerOutcome {
///     tracing::info!(order.id, sequence, "audited");
///     HandlerOutcome::ack()
/// }
/// # }
/// # fn main() {}
/// ```
pub trait ContextField: Default {
    /// The per-delivery context type this key reads from.
    type Context;

    /// The owned value the key yields.
    type Value: Send + 'static;

    /// Reads this key's field out of `src`. Named apart from [`Field::get`] so a key
    /// implementing both traits stays unambiguous to call.
    fn read(self, src: &Self::Context) -> Self::Value;
}

/// A [`Field`] key middleware can also write, for per-delivery scratch values.
///
/// The read side ([`Field::get`]) is typically `Option<&T>` (the value may not have been set yet),
/// while [`set`](FieldMut::set) takes the owned `T`.
///
/// # Examples
///
/// ```
/// use ruststream::{Field, FieldMut};
///
/// /// A broker's per-delivery context, with a slot middleware fills in.
/// #[derive(Default)]
/// struct Delivery {
///     user: Option<u64>,
/// }
///
/// /// The key an authentication middleware writes and a handler reads.
/// #[derive(Clone, Copy)]
/// struct User;
///
/// impl Field<Delivery> for User {
///     type Value<'a> = Option<&'a u64>;
///     fn get(self, src: &Delivery) -> Option<&u64> {
///         src.user.as_ref()
///     }
/// }
///
/// impl FieldMut<Delivery> for User {
///     type Owned = u64;
///     fn set(self, src: &mut Delivery, value: u64) {
///         src.user = Some(value);
///     }
/// }
/// ```
pub trait FieldMut<Src: ?Sized>: Field<Src> {
    /// The owned value written through this key.
    type Owned;

    /// Writes `value` into `src` under this key.
    fn set(self, src: &mut Src, value: Self::Owned);
}

/// Builds a handler's per-delivery context value from the broker message.
///
/// The runtime calls this once per delivery to construct the typed context the handler reads its
/// broker fields off (by [`Field`] key). A broker with per-delivery fields implements it for its
/// own context type, reading the fields off its concrete message; the blanket `impl` for `()`
/// gives the zero-field default, so a broker that exposes nothing needs no implementation.
///
/// The built context is an owned value (it reads its fields out of the message rather than
/// borrowing it), so it does not tie the handler's context type to the delivery lifetime.
///
/// # Examples
///
/// ```
/// use ruststream::BuildContext;
///
/// /// A broker's message, as its client hands it over.
/// struct Record {
///     offset: u64,
/// }
///
/// /// The broker's per-delivery context: built once per delivery, before the handler runs.
/// struct RecordContext {
///     offset: u64,
/// }
///
/// impl BuildContext<Record> for RecordContext {
///     fn build(msg: &Record) -> Self {
///         Self { offset: msg.offset }
///     }
/// }
/// ```
pub trait BuildContext<M: ?Sized> {
    /// Builds the context value by reading fields out of `msg`.
    fn build(msg: &M) -> Self;
}

impl<M: ?Sized> BuildContext<M> for () {
    fn build(_msg: &M) -> Self {}
}

/// Builds a batch handler's context value from the batch's first delivery.
///
/// The batch counterpart of [`BuildContext`]: the runtime calls this once per dispatched batch.
/// A batch spans many deliveries, so only subscription-scoped data - handles every delivery of
/// the subscription shares, like a reposition handle - belongs in a batch context type;
/// per-delivery fields (a position, a header) live on the batch's elements instead. Keeping the
/// batch context a separate type from the broker's per-delivery one is what makes that
/// distinction hold at compile time: a per-delivery context type simply does not implement
/// this, so a batch body cannot name it.
///
/// The blanket `impl` for `()` gives the zero-field default, so a broker with no
/// subscription-scoped data needs no implementation.
///
/// # Examples
///
/// ```
/// use ruststream::BuildBatchContext;
///
/// /// A broker's message, as its client hands it over.
/// struct Entry {
///     stream: &'static str,
/// }
///
/// /// Subscription-scoped: every delivery of a batch comes from one stream, so the first one
/// /// says which.
/// struct StreamBatchContext {
///     stream: &'static str,
/// }
///
/// impl BuildBatchContext<Entry> for StreamBatchContext {
///     fn build(first: &Entry) -> Self {
///         Self {
///             stream: first.stream,
///         }
///     }
/// }
/// ```
pub trait BuildBatchContext<M: ?Sized> {
    /// Builds the context value by reading subscription-scoped fields out of the batch's first
    /// delivery.
    fn build(first: &M) -> Self;
}

impl<M: ?Sized> BuildBatchContext<M> for () {
    fn build(_first: &M) -> Self {}
}
