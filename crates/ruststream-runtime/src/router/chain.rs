//! What a `with_broker` closure returns: the mount chain, finished into the routes its runner
//! consumes.

use super::builder::Router;
use super::builders::{RouterCommit, RouterWith};
use super::routes::RetryDestinationsDeclared;

/// What the closure of `with_broker` returns: the chain its scope opened, finished into the
/// routes the app mounts.
///
/// Implemented by [`Router`] and by an open mount chain ([`RouterWith`]), so a scope ends on
/// `.include(def)` or on a step such as `.out_reply(policy)` alike. Each runner implements its
/// own consumption of the finished routes; the chain, its name and its diagnostic are the same
/// under every runner. Machinery; you never implement it.
///
/// # Examples
///
/// See `RustStream::with_broker` under the `tokio` runner and `EmbeddedApp::with_broker` under the
/// `embassy` runner: the closure there returns this chain.
#[diagnostic::on_unimplemented(
    message = "the `with_broker` closure must return the mount chain",
    label = "`{Self}` is not a mount chain",
    note = "return the scope or the chain its `include` opens: `|b| b.include(a).include(b)`, \
            every `Out` slot bound with `.out(marker, policy)`; a broker that mounts nothing is \
            `|b| b`"
)]
pub trait MountChain {
    /// The finished routes.
    type Routes;

    /// Finishes the chain: commits the registration a step left open.
    fn finish(self) -> Self::Routes;
}

impl<B, Routes, RouteCodec, Layers, Pipe> MountChain
    for Router<B, Routes, RouteCodec, Layers, Pipe>
{
    type Routes = Self;

    fn finish(self) -> Self {
        self
    }
}

impl<Mount, R, Def, Attach, Last> MountChain for RouterWith<Mount, R, Def, Attach, Last>
where
    Attach: RouterCommit<Mount, R, Def, Out: RetryDestinationsDeclared>,
{
    type Routes = Attach::Out;

    fn finish(self) -> Attach::Out {
        self.build()
    }
}
