//! The concurrency policy of one subscription's dispatch loop: how many deliveries run at
//! once, and where.

use std::num::NonZeroUsize;

/// Concurrency policy for one subscriber's dispatch loop, declared with the `workers(..)` or
/// `threads(..)` macro argument (or [`Workers::sequential`] by default).
///
/// - `workers(n)`: `n` long-lived workers, tasks of the runtime the app runs on, process up to
///   `n` deliveries of the subscriber concurrently; a free worker takes the next one.
///   Back-pressure holds: the stream is not polled while `n` deliveries are in flight. Global
///   processing order is lost by design.
/// - `workers(n, by_key)`: the same `n` workers as sequential lanes; a delivery goes to the lane
///   picked by hashing its [`partition_key`](crate::IncomingMessage::partition_key), so per-key
///   ordering is preserved. Messages without a key rotate over the lanes.
/// - `threads(n)`: `n` dedicated threads of the subscription's own, each with a current-thread
///   runtime, for a handler that computes. The subscription's loop stays on the app's runtime and
///   spreads deliveries over the threads round-robin; decoding, the handler and settling run on
///   the thread a delivery was handed to, and only there. The app's runtime stays free for I/O,
///   timers and the other subscriptions.
/// - `threads(n, by_key)`: the same threads as sequential lanes by partition key.
///
/// The workers or threads and the queues that feed them are made when the subscription starts:
/// handing a delivery to one allocates nothing.
///
/// The default is sequential dispatch (`workers(1)`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Workers {
    pub(super) count: NonZeroUsize,
    pub(super) by_key: bool,
    pub(super) placement: Placement,
}

/// Where a subscription's deliveries run, as [`Workers::placement`] reports it.
///
/// A broker's own mount step reads it when the placement decides a resource of its own: a
/// connection pool per dedicated thread, say. Non-exhaustive, so a later placement is an
/// additive change; a step matches the placement it cares about.
///
/// # Examples
///
/// ```
/// # #[cfg(feature = "json")]
/// # mod demo {
/// use ruststream::memory::prelude::*;
/// use ruststream::runtime::{Declared, Placement, SubscriberBuilder};
///
/// /// A broker crate's mount step: a thread opens a pool of its own, a delivery on the app's
/// /// runtime shares one, and the database grants `limit` connections in all.
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
/// #[subscriber(MemorySource::new("scans"), threads(4))]
/// async fn scan(order: &u64) -> HandlerOutcome {
///     tracing::info!(order, "scanned");
///     HandlerOutcome::ack()
/// }
///
/// fn app() -> Result<RustStream, TooManyConnections> {
///     // Four threads, two connections each: eight of the database's sixteen.
///     let mounted = scan.pools(2, 16)?;
///     Ok(RustStream::new(AppInfo::new("scans", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
///         b.include(mounted);
///     }))
/// }
/// # }
/// # fn main() {}
/// ```
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Placement {
    /// As tasks of the runtime the app runs on.
    Runtime,
    /// On dedicated threads of the subscription's own.
    Threads,
}

impl Workers {
    /// Sequential dispatch: one delivery at a time, in stream order. The default.
    #[must_use]
    pub const fn sequential() -> Self {
        Self {
            count: NonZeroUsize::MIN,
            by_key: false,
            placement: Placement::Runtime,
        }
    }

    /// A pool of up to `count` concurrent deliveries. One behaves like sequential dispatch.
    #[must_use]
    pub const fn pool(count: NonZeroUsize) -> Self {
        Self {
            count,
            by_key: false,
            placement: Placement::Runtime,
        }
    }

    /// `count` sequential lanes keyed by the message
    /// [`partition_key`](crate::IncomingMessage::partition_key): per-key ordering is preserved.
    /// One lane behaves like sequential dispatch.
    #[must_use]
    pub const fn keyed(count: NonZeroUsize) -> Self {
        Self {
            count,
            by_key: true,
            placement: Placement::Runtime,
        }
    }

    /// `count` dedicated threads for a CPU-bound handler: the subscription's deliveries are
    /// handled on threads of its own, each running a current-thread runtime, so the handler's
    /// computation holds up none of the app runtime's threads. The router-chain spelling of
    /// `threads(n)`.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[cfg(feature = "json")]
    /// # mod demo {
    /// use ruststream::memory::prelude::*;
    /// use serde::Deserialize;
    ///
    /// #[derive(Deserialize)]
    /// struct Image {
    ///     id: u64,
    /// }
    ///
    /// #[subscriber("images.resize")]
    /// async fn resize(image: &Image) -> HandlerOutcome {
    ///     tracing::info!(image.id, "resized");
    ///     HandlerOutcome::ack()
    /// }
    ///
    /// fn images() -> Router<MemoryBroker, impl RouterDef<MemoryBroker>> {
    ///     // Resizing computes, so it runs on eight threads of its own.
    ///     Router::new()
    ///         .include(resize)
    ///         .workers(Workers::threads(nonzero!(8)))
    /// }
    ///
    /// fn app() -> RustStream {
    ///     RustStream::new(AppInfo::new("images", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
    ///         b.include_router(images());
    ///     })
    /// }
    /// # }
    /// # fn main() {}
    /// ```
    #[must_use]
    pub const fn threads(count: NonZeroUsize) -> Self {
        Self {
            count,
            by_key: false,
            placement: Placement::Threads,
        }
    }

    /// `count` dedicated threads as sequential lanes keyed by the message
    /// [`partition_key`](crate::IncomingMessage::partition_key): a key always lands on the same
    /// thread, so per-key ordering is preserved. The router-chain spelling of
    /// `threads(n, by_key)`.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[cfg(feature = "json")]
    /// # mod demo {
    /// use ruststream::memory::prelude::*;
    /// use serde::Deserialize;
    ///
    /// #[derive(Deserialize)]
    /// struct Image {
    ///     id: u64,
    /// }
    ///
    /// #[subscriber("images.resize")]
    /// async fn resize(image: &Image) -> HandlerOutcome {
    ///     tracing::info!(image.id, "resized");
    ///     HandlerOutcome::ack()
    /// }
    ///
    /// fn images() -> Router<MemoryBroker, impl RouterDef<MemoryBroker>> {
    ///     // One tenant's images stay in order: a key always lands on the same thread.
    ///     Router::new()
    ///         .include(resize)
    ///         .workers(Workers::threads_keyed(nonzero!(4)))
    /// }
    ///
    /// fn app() -> RustStream {
    ///     RustStream::new(AppInfo::new("images", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
    ///         b.include_router(images());
    ///     })
    /// }
    /// # }
    /// # fn main() {}
    /// ```
    #[must_use]
    pub const fn threads_keyed(count: NonZeroUsize) -> Self {
        Self {
            count,
            by_key: true,
            placement: Placement::Threads,
        }
    }

    /// How many deliveries (or batches) are handled at once: the workers, the keyed lanes or the
    /// dedicated threads. One for sequential dispatch.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[cfg(feature = "json")]
    /// # mod demo {
    /// use ruststream::memory::prelude::*;
    /// use ruststream::runtime::{Declared, SubscriberBuilder};
    ///
    /// /// A broker crate's mount step: one prefetch window per delivery in flight.
    /// pub trait Prefetch: Declared {
    ///     fn prefetch(self, per_delivery: usize) -> Self::Settings
    ///     where
    ///         Self::Settings: PrefetchStep,
    ///     {
    ///         self.declare().apply_prefetch(per_delivery)
    ///     }
    /// }
    ///
    /// impl<Def: Declared> Prefetch for Def {}
    ///
    /// pub trait PrefetchStep: Sized {
    ///     fn apply_prefetch(self, per_delivery: usize) -> Self;
    /// }
    ///
    /// impl<Def, State, DefCodec> PrefetchStep
    ///     for SubscriberBuilder<Def, MemorySource, State, DefCodec>
    /// {
    ///     fn apply_prefetch(self, per_delivery: usize) -> Self {
    ///         let window = self.dispatch().count().get() * per_delivery;
    ///         tracing::info!(window, "prefetch window");
    ///         self
    ///     }
    /// }
    ///
    /// #[subscriber(MemorySource)]
    /// async fn audit(order: &u64) -> HandlerOutcome {
    ///     tracing::info!(order, "audited");
    ///     HandlerOutcome::ack()
    /// }
    ///
    /// fn app() -> RustStream {
    ///     RustStream::new(AppInfo::new("audit", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
    ///         // The step sees the eight workers named before it: a window of 80.
    ///         b.include(audit.name("orders").workers(nonzero!(8)).prefetch(10));
    ///     })
    /// }
    /// # }
    /// # fn main() {}
    /// ```
    #[must_use]
    pub const fn count(&self) -> NonZeroUsize {
        self.count
    }

    /// Whether deliveries go to sequential lanes by their
    /// [`partition_key`](crate::IncomingMessage::partition_key) rather than to whichever worker
    /// is free.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[cfg(feature = "json")]
    /// # mod demo {
    /// use ruststream::memory::prelude::*;
    /// use ruststream::runtime::{Declared, SubscriberBuilder};
    ///
    /// /// A broker crate's mount step: keyed lanes keep a key's order, so the broker can ask the
    /// /// server for unordered delivery only when nothing reads the key.
    /// pub trait Ordering: Declared {
    ///     fn log_ordering(self) -> Self::Settings
    ///     where
    ///         Self::Settings: OrderingStep,
    ///     {
    ///         self.declare().apply_log_ordering()
    ///     }
    /// }
    ///
    /// impl<Def: Declared> Ordering for Def {}
    ///
    /// pub trait OrderingStep: Sized {
    ///     fn apply_log_ordering(self) -> Self;
    /// }
    ///
    /// impl<Def, State, DefCodec> OrderingStep
    ///     for SubscriberBuilder<Def, MemorySource, State, DefCodec>
    /// {
    ///     fn apply_log_ordering(self) -> Self {
    ///         let ordered = self.dispatch().by_key();
    ///         tracing::info!(ordered, "per-key ordering");
    ///         self
    ///     }
    /// }
    ///
    /// #[subscriber(MemorySource::new("orders"), workers(4, by_key))]
    /// async fn audit(order: &u64) -> HandlerOutcome {
    ///     tracing::info!(order, "audited");
    ///     HandlerOutcome::ack()
    /// }
    ///
    /// fn app() -> RustStream {
    ///     RustStream::new(AppInfo::new("audit", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
    ///         b.include(audit.log_ordering());
    ///     })
    /// }
    /// # }
    /// # fn main() {}
    /// ```
    #[must_use]
    pub const fn by_key(&self) -> bool {
        self.by_key
    }

    /// Where the deliveries run: as tasks of the app's runtime, or on dedicated threads of the
    /// subscription's own.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[cfg(feature = "json")]
    /// # mod demo {
    /// use ruststream::memory::prelude::*;
    /// use ruststream::runtime::{Declared, Placement, SubscriberBuilder};
    ///
    /// /// A broker crate's mount step: a dedicated thread keeps a connection of its own.
    /// pub trait Sessions: Declared {
    ///     fn sessions(self) -> Self::Settings
    ///     where
    ///         Self::Settings: SessionStep,
    ///     {
    ///         self.declare().apply_sessions()
    ///     }
    /// }
    ///
    /// impl<Def: Declared> Sessions for Def {}
    ///
    /// pub trait SessionStep: Sized {
    ///     fn apply_sessions(self) -> Self;
    /// }
    ///
    /// impl<Def, State, DefCodec> SessionStep
    ///     for SubscriberBuilder<Def, MemorySource, State, DefCodec>
    /// {
    ///     fn apply_sessions(self) -> Self {
    ///         let per_thread = self.dispatch().placement() == Placement::Threads;
    ///         tracing::info!(per_thread, "one session per thread");
    ///         self
    ///     }
    /// }
    ///
    /// #[subscriber(MemorySource::new("images"), threads(2))]
    /// async fn resize(image: &u64) -> HandlerOutcome {
    ///     tracing::info!(image, "resized");
    ///     HandlerOutcome::ack()
    /// }
    ///
    /// fn app() -> RustStream {
    ///     RustStream::new(AppInfo::new("images", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
    ///         b.include(resize.sessions());
    ///     })
    /// }
    /// # }
    /// # fn main() {}
    /// ```
    #[must_use]
    pub const fn placement(&self) -> Placement {
        self.placement
    }

    /// One worker on the app's runtime is indistinguishable from the sequential loop; one
    /// dedicated thread is still a thread of its own.
    pub(crate) const fn is_sequential(&self) -> bool {
        self.count.get() == 1 && matches!(self.placement, Placement::Runtime)
    }

    /// The same concurrency as tasks of the runtime the loop runs on: what a `threads(n)`
    /// subscription becomes under the test harness, which runs every subscription on the test's
    /// own runtime.
    #[cfg(feature = "testing")]
    pub(crate) const fn on_runtime(self) -> Self {
        Self {
            placement: Placement::Runtime,
            ..self
        }
    }
}

impl Default for Workers {
    fn default() -> Self {
        Self::sequential()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_default_worker_policy_is_sequential() {
        assert!(Workers::default().is_sequential());
        // One worker of either shape is the sequential loop, not a pool of one.
        assert!(Workers::pool(NonZeroUsize::new(1).unwrap()).is_sequential());
        assert!(!Workers::pool(NonZeroUsize::new(2).unwrap()).is_sequential());
    }
}
