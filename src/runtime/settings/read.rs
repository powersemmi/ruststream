//! What the chain fixed so far, read back by a broker's own mount step.

use std::num::NonZeroUsize;

use crate::runtime::dispatch::Workers;
use crate::{ConnectedBroker, SubscriptionSource};

use super::SubscriberBuilder;
#[cfg(doc)]
use super::SubscriberSettings;

/// What the chain fixed so far, for a broker's own mount step to read.
///
/// A step reads each value as it stands when the step is called: the attribute's settings are
/// fixed before any mount-site step, and a mount-site setting is visible to the steps chained
/// after it.
impl<Def, Src, State, DefCodec> SubscriberBuilder<Def, Src, State, DefCodec> {
    /// The dispatch concurrency fixed so far: by `workers(..)` or `threads(..)` in the
    /// attribute, or by [`workers`](SubscriberSettings::workers),
    /// [`threads`](SubscriberSettings::threads) and their keyed forms earlier in the chain.
    ///
    /// A step chained before the concurrency is named sees the default,
    /// [`Workers::sequential`], even when a later step names another one.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[cfg(all(feature = "macros", feature = "memory", feature = "json"))]
    /// # mod demo {
    /// use ruststream::memory::prelude::*;
    /// use ruststream::runtime::{Declared, Placement, SubscriberBuilder};
    ///
    /// /// A broker crate's mount step: each dedicated thread opens a connection pool of its own,
    /// /// and the database grants `limit` connections in all.
    /// pub trait Pools: Declared {
    ///     fn pools(self, per_pool: usize, limit: usize) -> Result<Self::Settings, TooManyConnections>
    ///     where
    ///         Self::Settings: PoolStep,
    ///     {
    ///         self.declare().apply_pools(per_pool, limit)
    ///     }
    /// }
    ///
    /// impl<Def: Declared> Pools for Def {}
    ///
    /// pub trait PoolStep: Sized {
    ///     fn apply_pools(self, per_pool: usize, limit: usize) -> Result<Self, TooManyConnections>;
    /// }
    ///
    /// impl<Def, State, DefCodec> PoolStep for SubscriberBuilder<Def, MemorySource, State, DefCodec> {
    ///     fn apply_pools(self, per_pool: usize, limit: usize) -> Result<Self, TooManyConnections> {
    ///         let dispatch = self.dispatch();
    ///         let pools = match dispatch.placement() {
    ///             Placement::Threads => dispatch.count().get(),
    ///             _ => 1,
    ///         };
    ///         if pools * per_pool > limit {
    ///             return Err(TooManyConnections(pools * per_pool));
    ///         }
    ///         Ok(self)
    ///     }
    /// }
    ///
    /// #[derive(Debug)]
    /// pub struct TooManyConnections(usize);
    ///
    /// #[subscriber(MemorySource)]
    /// async fn scan(order: &u64) -> HandlerOutcome {
    ///     tracing::info!(order, "scanned");
    ///     HandlerOutcome::ack()
    /// }
    ///
    /// fn app() -> Result<RustStream, TooManyConnections> {
    ///     // The step comes after `threads(4)`, so it counts four pools of two connections.
    ///     let mounted = scan.name("scans").threads(nonzero!(4)).pools(2, 16)?;
    ///     Ok(RustStream::new(AppInfo::new("scans", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
    ///         b.include(mounted);
    ///     }))
    /// }
    /// # }
    /// # fn main() {}
    /// ```
    #[must_use]
    pub const fn dispatch(&self) -> Workers {
        self.workers
    }

    /// The batch size [`batch`](SubscriberSettings::batch) named so far, `None` while it is
    /// still open: a single-message registration never has one, and a batch registration has
    /// one once the chain named it.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[cfg(all(feature = "macros", feature = "memory", feature = "json"))]
    /// # mod demo {
    /// use std::num::NonZeroUsize;
    ///
    /// use ruststream::memory::prelude::*;
    /// use ruststream::runtime::{Declared, SubscriberBuilder};
    ///
    /// /// A broker crate's mount step: the server pages rows, so a batch must fit in one page.
    /// pub trait Pages: Declared {
    ///     fn page_of(self, rows: usize) -> Result<Self::Settings, PageTooSmall>
    ///     where
    ///         Self::Settings: PageStep,
    ///     {
    ///         self.declare().apply_page_of(rows)
    ///     }
    /// }
    ///
    /// impl<Def: Declared> Pages for Def {}
    ///
    /// pub trait PageStep: Sized {
    ///     fn apply_page_of(self, rows: usize) -> Result<Self, PageTooSmall>;
    /// }
    ///
    /// impl<Def, State, DefCodec> PageStep for SubscriberBuilder<Def, MemorySource, State, DefCodec> {
    ///     fn apply_page_of(self, rows: usize) -> Result<Self, PageTooSmall> {
    ///         let batch = self.batch_size().map_or(1, NonZeroUsize::get);
    ///         if batch > rows {
    ///             return Err(PageTooSmall { batch, rows });
    ///         }
    ///         Ok(self)
    ///     }
    /// }
    ///
    /// #[derive(Debug)]
    /// pub struct PageTooSmall {
    ///     batch: usize,
    ///     rows: usize,
    /// }
    ///
    /// #[subscriber(MemorySource)]
    /// async fn settle(orders: &[u64]) -> HandlerOutcome {
    ///     tracing::info!(count = orders.len(), "settled a batch");
    ///     HandlerOutcome::ack()
    /// }
    ///
    /// fn app() -> Result<RustStream, PageTooSmall> {
    ///     let mounted = settle.name("orders").batch(nonzero!(64)).page_of(100)?;
    ///     Ok(RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
    ///         b.include(mounted);
    ///     }))
    /// }
    /// # }
    /// # fn main() {}
    /// ```
    #[must_use]
    pub const fn batch_size(&self) -> Option<NonZeroUsize> {
        self.batch_size
    }

    /// The name of the subscription this registration opens, as the source reports it to the
    /// connected broker `Conn`.
    ///
    /// Available once the source is named: an unnamed definition (`#[subscriber]`,
    /// `#[subscriber(Kind)]` before [`name`](SubscriberSettings::name)) has no source to ask, so
    /// the call does not compile there. `Conn` is inferred when the source serves one connected
    /// form; a source that serves several (one per log mode, one per database) names it.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[cfg(all(feature = "macros", feature = "memory", feature = "json"))]
    /// # mod demo {
    /// use ruststream::memory::ConnectedMemoryBroker;
    /// use ruststream::memory::prelude::*;
    /// use ruststream::runtime::{Declared, SubscriberBuilder};
    ///
    /// /// A broker crate's mount step: the server allows names of up to 32 bytes, and a long
    /// /// one is refused while the chain is built, with the name in the error.
    /// pub trait ShortNames: Declared {
    ///     fn short_name(self) -> Result<Self::Settings, NameTooLong>
    ///     where
    ///         Self::Settings: ShortNameStep,
    ///     {
    ///         self.declare().apply_short_name()
    ///     }
    /// }
    ///
    /// impl<Def: Declared> ShortNames for Def {}
    ///
    /// pub trait ShortNameStep: Sized {
    ///     fn apply_short_name(self) -> Result<Self, NameTooLong>;
    /// }
    ///
    /// impl<Def, State, DefCodec> ShortNameStep
    ///     for SubscriberBuilder<Def, MemorySource, State, DefCodec>
    /// {
    ///     fn apply_short_name(self) -> Result<Self, NameTooLong> {
    ///         // The memory source serves both log modes, so the step names the one it reads for.
    ///         let name = self.subscription_name::<ConnectedMemoryBroker>();
    ///         if name.len() > 32 {
    ///             return Err(NameTooLong(name.to_owned()));
    ///         }
    ///         Ok(self)
    ///     }
    /// }
    ///
    /// #[derive(Debug)]
    /// pub struct NameTooLong(String);
    ///
    /// #[subscriber(MemorySource)]
    /// async fn audit(order: &u64) -> HandlerOutcome {
    ///     tracing::info!(order, "audited");
    ///     HandlerOutcome::ack()
    /// }
    ///
    /// fn app(tenant: &str) -> Result<RustStream, NameTooLong> {
    ///     let mounted = audit.name(format!("audit-{tenant}")).short_name()?;
    ///     Ok(RustStream::new(AppInfo::new("audit", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
    ///         b.include(mounted);
    ///     }))
    /// }
    /// # }
    /// # fn main() {}
    /// ```
    #[must_use]
    pub fn subscription_name<Conn>(&self) -> &str
    where
        Conn: ConnectedBroker,
        Src: SubscriptionSource<Conn>,
    {
        self.source.name()
    }
}
