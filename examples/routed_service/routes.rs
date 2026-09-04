//! Wiring: collect the handlers into two [`Router`]s, one per concern. Keeping registration in its
//! own module lets the handlers stay broker-agnostic - a router binds to a concrete broker only
//! when [`main`](crate::main) mounts it with `include_router`.
//!
//! Each router carries the metrics consume layer with [`Router::layer`], scoping that metric to the
//! routed handlers; a router knows its own handler types, so it wraps them directly. (The layer is
//! also a `BlanketLayer`, so it could ride the application-wide stack instead - here it stays per
//! router to keep metrics local and to exercise `Router::layer`.) The publish-side metric is added
//! once on the application in [`main`](crate::main).

use ruststream::memory::prelude::*;
use ruststream::metrics::Metrics;

use crate::domain::Repository;
use crate::observability::StampSource;
use crate::{orders, payments};

/// The order-lifecycle router: a publishing handler that replies to `confirmations`, plus the
/// cancellation handler.
///
/// `confirm` needs a publisher for its reply; `.out(Reply, Publish)` names the position and the
/// policy, and `.transform(StampSource)` composes a static publish transform onto it that stamps a
/// provenance header on every confirmation - reply settings live on this chain, not in the
/// `publish("..")` decorator (which only names the destination).
/// The reply wiring is a publish policy stack, pure declaration: the runtime pairs it with the
/// connected broker at startup, so the router borrows no broker. `on_cancel` has no reply, so it
/// is mounted with `include`. The router is a consuming builder, so the calls chain and each
/// attachment closes with `.build()`; the registration list is opaque, hence `impl RouterDef`.
///
/// `use<>` opts out of capturing the `metrics` borrow: the router owns its layer (`Arc`-backed),
/// so the caller can still mutate the scope to mount it.
pub(crate) fn orders(metrics: &Metrics) -> impl RouterDef<MemoryBroker, Repository> + use<> {
    Router::new()
        .layer(metrics.consume_layer())
        .include(orders::confirm)
        .out(Reply, Publish)
        .transform(StampSource)
        .build()
        .include(orders::on_cancel)
}

/// The payments router: a charge handler spread across keyed worker lanes, plus a batch handler
/// that settles cleared payments through a transactional publisher.
///
/// `.transactional()` marks the wiring: the batch registration then publishes a batch's replies
/// inside one broker transaction, visible atomically on commit. It type-checks because
/// the `TransactionalPublish` policy pairs into a transactional publisher; a broker without
/// transactions fails to compile at the registration. `.batch(n)` is the batch size the
/// subscription opens with, which every batch mount owes.
pub(crate) fn payments(metrics: &Metrics) -> impl RouterDef<MemoryBroker, Repository> + use<> {
    Router::new()
        .layer(metrics.consume_layer())
        .include(payments::process_payment)
        .include(payments::settle.batch(nonzero!(64)))
        .out(Reply, TransactionalPublish)
        .transactional()
        .build()
}
