//! A `#[subscriber]` body as the function item the attribute leaves in place.
//!
//! The attribute emits the handler twice under one name: a braced struct in the type namespace,
//! which carries the declaration (source, settings, reply), and the body itself as an `async fn`
//! in the value namespace, which is what `include(handler)` passes. The function's future is
//! nameable through the `Fn` traits, so the runner that needs it `Send` states that at its mount
//! site ([`Handle`] for [`FnBody`]), and a runner that polls on one thread asks nothing
//! ([`LocalHandle`] for [`FnBody`]). The expansion itself is the same for both.

use core::marker::PhantomData;

use super::reply::DeclaresReply;
use super::{Handle, Input, LocalHandle, Verdict};
use crate::context::Context;
use crate::settings::{CapsBatches, Declared};
use crate::slot::{
    BindSlots, BindSlotsBody, HasSlots, LocalBindSlots, LocalBindSlotsBody, LocalHasSlots,
};
use crate::{ConnectedBroker, LocalConnectedBroker};

/// The first argument of a `#[subscriber]` body: names the declaration that goes with the body.
///
/// Zero-sized; the body never reads it.
pub struct BodyTag<M>(PhantomData<fn() -> M>);

impl<M> BodyTag<M> {
    /// The tag of the declaration `M`.
    #[must_use]
    pub const fn new() -> Self {
        Self(PhantomData)
    }
}

impl<M> Default for BodyTag<M> {
    fn default() -> Self {
        Self::new()
    }
}

/// One call of a body function with the borrows of one delivery; names the future the call
/// returns, which is what lets a runner bound it.
pub trait BodyCall<M, I, O, X>: Fn(BodyTag<M>, I, O, X) -> Self::Fut {
    /// The future one call returns.
    type Fut: Future<Output = Self::Out>;
    /// What the future resolves to.
    type Out;
}

impl<M, I, O, X, F, Fut> BodyCall<M, I, O, X> for F
where
    F: Fn(BodyTag<M>, I, O, X) -> Fut,
    Fut: Future,
{
    type Fut = Fut;
    type Out = Fut::Output;
}

/// A body function bound to its declaration: the handler the mount machinery drives.
pub struct FnBody<M, F> {
    body: F,
    declaration: PhantomData<fn() -> M>,
}

impl<M, F: Clone> Clone for FnBody<M, F> {
    fn clone(&self) -> Self {
        Self {
            body: self.body.clone(),
            declaration: PhantomData,
        }
    }
}

impl<M, F: Copy> Copy for FnBody<M, F> {}

impl<M, F> FnBody<M, F> {
    /// Binds `body` to the declaration `M`.
    pub const fn new(body: F) -> Self {
        Self {
            body,
            declaration: PhantomData,
        }
    }
}

// The multi-thread runner's contract: the body's future is `Send` for every borrow of a
// delivery. A body that holds a value which is not `Send` across an await fails here, at the
// mount on that runner, and nowhere else.
impl<M, F, In, R, O, C, S> Handle<In, R, O, C, S> for FnBody<M, F>
where
    In: ?Sized + Input,
    F: Send + Sync,
    F: for<'i, 'o, 'c, 'x> BodyCall<
            M,
            &'i In,
            &'o O,
            &'c mut Context<'x, C, S>,
            Out = Verdict<In, R>,
        >,
    for<'i, 'o, 'c, 'x> <F as BodyCall<M, &'i In, &'o O, &'c mut Context<'x, C, S>>>::Fut: Send,
{
    fn handle(
        &self,
        input: &In,
        outs: &O,
        ctx: &mut Context<'_, C, S>,
    ) -> impl Future<Output = Verdict<In, R>> + Send {
        (self.body)(BodyTag::new(), input, outs, ctx)
    }
}

impl<M, F, In, R, O, C, S> LocalHandle<In, R, O, C, S> for FnBody<M, F>
where
    In: ?Sized + Input,
    F: for<'i, 'o, 'c, 'x> BodyCall<
            M,
            &'i In,
            &'o O,
            &'c mut Context<'x, C, S>,
            Out = Verdict<In, R>,
        >,
{
    fn handle(
        &self,
        input: &In,
        outs: &O,
        ctx: &mut Context<'_, C, S>,
    ) -> impl Future<Output = Verdict<In, R>> {
        (self.body)(BodyTag::new(), input, outs, ctx)
    }
}

/// The declaration half of a `#[subscriber]` handler, generic over the body it wraps: what
/// [`Declared`] is for a definition that is its own body.
pub trait DeclareBody<Body>: Sized {
    /// See [`Declared::Form`].
    type Form;
    /// See [`Declared::Settings`].
    type Settings;
    /// Applies the settings the attribute fixed to the definition over `body`.
    fn declare_body(body: Body) -> Self::Settings;
}

/// A body function together with its declaration, as `include` receives it.
pub struct FnDecl<M, F>(FnBody<M, F>);

impl<M, F> Declared for FnDecl<M, F>
where
    M: DeclareBody<FnBody<M, F>>,
{
    type Form = M::Form;
    type Settings = M::Settings;

    fn declare(self) -> Self::Settings {
        M::declare_body(self.0)
    }
}

/// The marker of a value that is already a [`Declared`] definition.
pub struct AsDeclared;

/// The marker of a `#[subscriber]` body function, recording the declaration it names and the
/// argument types one call takes.
pub struct AsBodyFn<M, I, O, X>(PhantomData<fn() -> (M, I, O, X)>);

/// A value `include` accepts: a [`Declared`] definition, or a `#[subscriber]` body function.
///
/// The marker `Mk` keeps the two impls apart and is inferred at the call.
#[diagnostic::on_unimplemented(
    message = "`{Self}` is not a subscriber definition",
    note = "pass a `#[subscriber]` handler, or a definition built with `subscriber(..)`"
)]
pub trait IntoDeclared<Mk>: Sized {
    /// The definition the value declares.
    type Decl: Declared;
    /// Converts the value.
    fn into_declared(self) -> Self::Decl;
}

impl<D: Declared> IntoDeclared<AsDeclared> for D {
    type Decl = D;

    fn into_declared(self) -> D {
        self
    }
}

impl<F, M, I, O, X, Fut> IntoDeclared<AsBodyFn<M, I, O, X>> for F
where
    F: Fn(BodyTag<M>, I, O, X) -> Fut,
    Fut: Future,
    M: DeclareBody<FnBody<M, F>>,
{
    type Decl = FnDecl<M, F>;

    fn into_declared(self) -> FnDecl<M, F> {
        FnDecl(FnBody::new(self))
    }
}

macro_rules! opaque_debug {
    ($($ty:ident $(<$($p:ident),+>)?),+ $(,)?) => {$(
        impl$(<$($p),+>)? core::fmt::Debug for $ty$(<$($p),+>)? {
            fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
                f.debug_struct(stringify!($ty)).finish_non_exhaustive()
            }
        }
    )+};
}

opaque_debug!(BodyTag<M>, FnBody<M, F>, FnDecl<M, F>, AsDeclared, AsBodyFn<M, I, O, X>);

// A body function stands in for its declaration wherever the mount chain reads one: the slots it
// declares, the reply it gives, the batch step it takes, and how its slots bind.
impl<M: HasSlots, F> HasSlots for FnBody<M, F> {
    type Markers = M::Markers;
}

impl<M: LocalHasSlots, F> LocalHasSlots for FnBody<M, F> {
    type Markers = M::Markers;
}

impl<M: CapsBatches, F> CapsBatches for FnBody<M, F> {}

impl<M: DeclaresReply, F> DeclaresReply for FnBody<M, F> {
    type Reply = M::Reply;
    type Context = M::Context;
}

impl<M, F, C, Sources> BindSlots<C, Sources> for FnBody<M, F>
where
    C: ConnectedBroker,
    M: BindSlotsBody<C, Sources, Self>,
{
    type Bound = M::Bound;
    type Extra = M::Extra;

    fn bind(self, sources: Sources) -> (Self::Bound, Self::Extra) {
        M::bind_body(self, sources)
    }
}

impl<M, F, C, Sources> LocalBindSlots<C, Sources> for FnBody<M, F>
where
    C: LocalConnectedBroker,
    M: LocalBindSlotsBody<C, Sources, Self>,
{
    type Bound = M::Bound;
    type Extra = M::Extra;

    fn bind_local(self, sources: Sources) -> (Self::Bound, Self::Extra) {
        M::bind_local_body(self, sources)
    }
}

// A settings builder over a slot-carrying body reads the body's form off it, as it read the
// unit definition's before.
impl<M, F> Declared for FnBody<M, F>
where
    M: DeclareBody<Self>,
{
    type Form = M::Form;
    type Settings = M::Settings;

    fn declare(self) -> Self::Settings {
        M::declare_body(self)
    }
}
