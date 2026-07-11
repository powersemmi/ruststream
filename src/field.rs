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
/// use ruststream::Field;
///
/// struct Delivery {
///     sequence: u64,
/// }
///
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
/// let delivery = Delivery { sequence: 7 };
/// assert_eq!(Sequence.get(&delivery), 7);
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
/// the first `Ctx` key it sees. The value is owned (`'static`) on purpose: extractor values
/// bind before the handler body runs, so borrowing from the context is not an option. Borrowed
/// keys keep working through [`Field`] and `ctx.context(KEY)`.
///
/// Broker crates implement it next to [`Field`] on the same zero-sized keys; a key typically
/// implements both, so it works as a context read and as an extractor.
///
/// # Examples
///
/// ```
/// use ruststream::ContextField;
///
/// struct Delivery {
///     sequence: u64,
/// }
///
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
/// let delivery = Delivery { sequence: 7 };
/// assert_eq!(Sequence.read(&delivery), 7);
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
/// #[derive(Default)]
/// struct Ctx {
///     user: Option<u64>,
/// }
///
/// #[derive(Clone, Copy)]
/// struct User;
///
/// impl Field<Ctx> for User {
///     type Value<'a> = Option<&'a u64>;
///     fn get(self, src: &Ctx) -> Option<&u64> {
///         src.user.as_ref()
///     }
/// }
///
/// impl FieldMut<Ctx> for User {
///     type Owned = u64;
///     fn set(self, src: &mut Ctx, value: u64) {
///         src.user = Some(value);
///     }
/// }
///
/// let mut ctx = Ctx::default();
/// User.set(&mut ctx, 42);
/// assert_eq!(User.get(&ctx), Some(&42));
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
/// borrowing it), so it does not tie the handler's context type to the delivery lifetime; broker
/// metadata is typically `Copy` (offsets, sequence numbers), so this is a stack copy.
///
/// # Examples
///
/// ```
/// use ruststream::BuildContext;
///
/// struct Msg {
///     offset: u64,
/// }
///
/// // A broker context carrying one field, built from the message.
/// struct OrdersContext {
///     offset: u64,
/// }
///
/// impl BuildContext<Msg> for OrdersContext {
///     fn build(msg: &Msg) -> Self {
///         Self { offset: msg.offset }
///     }
/// }
///
/// let msg = Msg { offset: 9 };
/// let cx = OrdersContext::build(&msg);
/// assert_eq!(cx.offset, 9);
/// ```
pub trait BuildContext<M: ?Sized> {
    /// Builds the context value by reading fields out of `msg`.
    fn build(msg: &M) -> Self;
}

impl<M: ?Sized> BuildContext<M> for () {
    fn build(_msg: &M) -> Self {}
}
