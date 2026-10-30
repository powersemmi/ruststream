//! How a delivery lends its headers to the handler's context: on the first read where the
//! delivery can be shared across threads, at once where it cannot.

use crate::context::{FromDelivery, HeaderSource};
use crate::{HeaderMap, LocalIncomingMessage};

/// How the body hands a delivery's headers to the handler's [`Context`](crate::Context): a
/// runner picks it by the thread bounds its broker's deliveries meet.
///
/// The context is `Send`, so what it reads the headers through is `Sync`. A delivery that is
/// `Sync` itself is that source, and the broker's accessor runs only when something reads a
/// header ([`HeadersOnDemand`]); one that is not answers its map before the handler runs
/// ([`HeadersAtOnce`]).
#[doc(hidden)]
pub trait LendHeaders<M>: Copy {
    /// What the context reads the headers through.
    type Source<'m>: HeaderSource + Sync
    where
        M: 'm;

    /// The source for `msg`.
    fn source(msg: &M) -> Self::Source<'_>;
}

/// The delivery is the source, asked on the first read: the runner's choice where its deliveries
/// are `Sync`.
#[doc(hidden)]
#[derive(Clone, Copy, Debug, Default)]
pub struct HeadersOnDemand;

impl<M: LocalIncomingMessage + Sync> LendHeaders<M> for HeadersOnDemand {
    type Source<'m>
        = FromDelivery<'m, M>
    where
        M: 'm;

    #[inline]
    fn source(msg: &M) -> FromDelivery<'_, M> {
        FromDelivery(msg)
    }
}

/// The delivery answers its map before the handler runs: the runner's choice where its
/// deliveries stay on one thread.
#[doc(hidden)]
#[derive(Clone, Copy, Debug, Default)]
pub struct HeadersAtOnce;

impl<M: LocalIncomingMessage> LendHeaders<M> for HeadersAtOnce {
    type Source<'m>
        = &'m HeaderMap
    where
        M: 'm;

    #[inline]
    fn source(msg: &M) -> &HeaderMap {
        msg.headers()
    }
}
