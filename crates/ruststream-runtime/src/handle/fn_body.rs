//! A `#[subscriber]` body as the function item the attribute leaves in place.
//!
//! The attribute emits the handler twice under one name: a braced struct in the type namespace,
//! which carries the declaration (source, settings, reply), and the body itself as an `async fn`
//! in the value namespace, which is what `include(handler)` passes. The function's future is
//! nameable through the `Fn` traits, so the runner that needs it `Send` states that at its mount
//! site ([`Handle`] for [`FnBody`]), and a runner that polls on one thread asks nothing
//! ([`LocalHandle`] for [`FnBody`]). The expansion itself is the same for both.

use core::marker::PhantomData;

use super::{Handle, Input, LocalHandle, Verdict};
use crate::context::Context;
use crate::settings::Declared;

/// The first argument of a `#[subscriber]` body: names the declaration that goes with the body.
///
/// Zero-sized; the body never reads it.
pub struct Tag<M>(PhantomData<fn() -> M>);

impl<M> Tag<M> {
    /// The tag of the declaration `M`.
    #[must_use]
    pub const fn new() -> Self {
        Self(PhantomData)
    }
}

impl<M> Default for Tag<M> {
    fn default() -> Self {
        Self::new()
    }
}

/// One call of a body function with the borrows of one delivery; names the future the call
/// returns, which is what lets a runner bound it.
pub trait BodyCall<M, I, O, X>: Fn(Tag<M>, I, O, X) -> Self::Fut {
    /// The future one call returns.
    type Fut: Future<Output = Self::Out>;
    /// What the future resolves to.
    type Out;
}

impl<M, I, O, X, F, Fut> BodyCall<M, I, O, X> for F
where
    F: Fn(Tag<M>, I, O, X) -> Fut,
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
        (self.body)(Tag::new(), input, outs, ctx)
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
        (self.body)(Tag::new(), input, outs, ctx)
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
    F: Fn(Tag<M>, I, O, X) -> Fut,
    Fut: Future,
    M: DeclareBody<FnBody<M, F>>,
{
    type Decl = FnDecl<M, F>;

    fn into_declared(self) -> FnDecl<M, F> {
        FnDecl(FnBody::new(self))
    }
}
