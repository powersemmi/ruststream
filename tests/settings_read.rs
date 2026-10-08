//! A broker's own mount step reads what the chain fixed before it: the concurrency, the batch
//! size and the subscription's name, whether the attribute or an earlier step fixed them.
//!
//! The step here plays a database broker that opens one connection pool per dedicated thread and
//! refuses a registration whose pools would not fit the database's connection limit. The refusal
//! is what the tests read: it carries the numbers the step derived from the chain.
#![cfg(all(feature = "macros", feature = "memory", feature = "json"))]

mod common;

use std::future::{Future, ready};
use std::num::NonZeroUsize;

use common::Order;
use ruststream::memory::{ConnectedMemoryBroker, MemorySource};
use ruststream::runtime::{
    Context, Declared, Handle, HandlerOutcome, Placement, SubscriberBuilder, SubscriberSettings,
    subscriber,
};
use ruststream::{SubscriptionSource, nonzero, subscriber};

/// What the broker-like step refuses: the registration's pools need more connections than the
/// database grants.
#[derive(Debug, PartialEq, Eq)]
struct PoolLimitError {
    subscription: String,
    needed: usize,
}

/// The broker crate's mount steps, chainable on a definition and on the settings builder alike:
/// the same shape as the core's own [`SubscriberSettings`].
trait PoolSettings: Declared {
    /// Checks that `per_pool` connections for each pool the subscription opens fit in `limit`.
    fn pools(self, per_pool: usize, limit: usize) -> Result<Self::Settings, PoolLimitError>
    where
        Self::Settings: PoolStep,
    {
        self.declare().apply_pools(per_pool, limit)
    }

    /// Checks that one batch of rows fits in a page of `max_rows`.
    fn page_of(self, max_rows: usize) -> Result<Self::Settings, PoolLimitError>
    where
        Self::Settings: PoolStep,
    {
        self.declare().apply_page_of(max_rows)
    }
}

impl<Def: Declared> PoolSettings for Def {}

/// The machinery behind [`PoolSettings`]: the step itself, on the settings builder.
trait PoolStep: Sized {
    fn apply_pools(self, per_pool: usize, limit: usize) -> Result<Self, PoolLimitError>;

    fn apply_page_of(self, max_rows: usize) -> Result<Self, PoolLimitError>;
}

impl<Def, Src, State, DefCodec> PoolStep for SubscriberBuilder<Def, Src, State, DefCodec>
where
    Src: SubscriptionSource<ConnectedMemoryBroker>,
{
    fn apply_pools(self, per_pool: usize, limit: usize) -> Result<Self, PoolLimitError> {
        let dispatch = self.dispatch();
        // A thread runs its own pool; deliveries on the app's runtime share one.
        let pools = if dispatch.placement() == Placement::Threads {
            dispatch.count().get()
        } else {
            1
        };
        let needed = pools * per_pool;
        if needed <= limit {
            return Ok(self);
        }
        Err(PoolLimitError {
            subscription: self.subscription_name().to_owned(),
            needed,
        })
    }

    fn apply_page_of(self, max_rows: usize) -> Result<Self, PoolLimitError> {
        let rows = self.batch_size().map_or(1, NonZeroUsize::get);
        if rows <= max_rows {
            return Ok(self);
        }
        Err(PoolLimitError {
            subscription: self.subscription_name().to_owned(),
            needed: rows,
        })
    }
}

#[subscriber(MemorySource::new("scans"), threads(4))]
async fn scan(order: &Order) -> HandlerOutcome {
    let _ = order.id;
    HandlerOutcome::ack()
}

#[subscriber(MemorySource)]
async fn audit(order: &Order) -> HandlerOutcome {
    let _ = order.id;
    HandlerOutcome::ack()
}

#[subscriber(MemorySource)]
async fn paginate(orders: &[Order]) -> HandlerOutcome {
    let _ = orders.len();
    HandlerOutcome::ack()
}

#[test]
fn a_step_reads_the_threads_the_attribute_fixed() {
    let refused = scan.pools(2, 7).map(drop);
    assert_eq!(
        refused,
        Err(PoolLimitError {
            subscription: "scans".to_owned(),
            needed: 8,
        }),
    );
    assert!(scan.pools(2, 8).is_ok());
}

#[test]
fn a_step_reads_the_threads_the_mount_site_fixed() {
    let refused = audit
        .name("audits")
        .threads(nonzero!(3))
        .pools(2, 5)
        .map(drop);
    assert_eq!(
        refused,
        Err(PoolLimitError {
            subscription: "audits".to_owned(),
            needed: 6,
        }),
    );
}

#[test]
fn a_step_before_the_threads_sees_the_default() {
    // Sequential dispatch runs on the app's runtime: one shared pool, which fits. The threads
    // come after the step, so it never counted them.
    let built = audit
        .name("audits")
        .pools(2, 2)
        .map(|built| built.threads(nonzero!(4)));
    let built = built.expect("the default dispatch fits one pool");
    assert_eq!(built.dispatch().count(), nonzero!(4));
}

#[test]
fn keyed_workers_stay_on_the_runtime() {
    let built = audit.name("audits").workers_by_key(nonzero!(6));
    let dispatch = built.dispatch();
    assert!(dispatch.by_key());
    assert_eq!(dispatch.placement(), Placement::Runtime);
    assert!(built.pools(2, 2).is_ok());
}

#[test]
fn a_step_reads_the_batch_size_once_named() {
    let unsized_batch = paginate.name("pages");
    assert_eq!(unsized_batch.batch_size(), None);

    let refused = paginate
        .name("pages")
        .batch(nonzero!(16))
        .page_of(10)
        .map(drop);
    assert_eq!(
        refused,
        Err(PoolLimitError {
            subscription: "pages".to_owned(),
            needed: 16,
        }),
    );
}

/// A hand-written body, mounted through the manual chain.
struct Audit;

impl Handle<Order> for Audit {
    fn handle(
        &self,
        order: &Order,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        let _ = order.id;
        ready(Ok(()))
    }
}

#[test]
fn a_step_reads_a_hand_written_definition_alike() {
    let refused = subscriber(MemorySource::new("manual"), Audit)
        .threads_by_key(nonzero!(5))
        .build()
        .pools(1, 4)
        .map(drop);
    assert_eq!(
        refused,
        Err(PoolLimitError {
            subscription: "manual".to_owned(),
            needed: 5,
        }),
    );
}
