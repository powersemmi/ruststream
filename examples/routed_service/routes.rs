//! Wiring: collect the handlers into two [`Router`]s, one per concern. Keeping registration in its
//! own module lets the handlers stay broker-agnostic - a router binds to a concrete broker only
//! when [`main`](crate::main) mounts it with `include_router`.
//!
//! Each router carries the metrics consume layer with [`Router::layer`], scoping that metric to the
//! routed handlers; a router knows its own handler types, so it wraps them directly. (The layer is
//! also a `BlanketLayer`, so it could ride the application-wide stack instead - here it stays per
//! router to keep metrics local and to exercise `Router::layer`.) The publish-side metric is added
//! once on the application in [`main`](crate::main).

use ruststream::memory::MemoryBroker;
use ruststream::metrics::Metrics;
use ruststream::runtime::{Router, RouterDef, TypedPublisher};

use crate::domain::Repository;
use crate::observability::StampSource;
use crate::{orders, payments};

/// The order-lifecycle router: a publishing handler that replies to `confirmations`, plus the
/// cancellation handler.
///
/// `confirm` needs a publisher for its reply; `TypedPublisher::new` pairs the broker's publisher
/// with the default codec, and `.layer(StampSource)` composes a static publish layer onto it that
/// stamps a provenance header on every confirmation - publisher settings live here, on the
/// `TypedPublisher`, not in the `publish("..")` decorator (which only names the destination).
/// `include_publishing` reuses the publisher's codec to decode the order. `on_cancel` has no reply,
/// so it is mounted with `include`. The router is a consuming builder, so the calls chain; the
/// registration list is opaque, hence `impl RouterDef`.
///
/// `use<>` opts out of capturing the `broker`/`metrics` borrows: the router owns its publisher and
/// layer (both `Arc`-backed), so it borrows neither argument past this call, and the caller can
/// still mutate the scope to mount it.
pub(crate) fn orders(
    broker: &MemoryBroker,
    metrics: &Metrics,
) -> impl RouterDef<MemoryBroker, Repository> + use<> {
    let confirmations = TypedPublisher::new(broker.publisher()).transform(StampSource);

    Router::new()
        .layer(metrics.consume_layer())
        .include_publishing(orders::confirm, confirmations)
        .include(orders::on_cancel)
}

/// The payments router: a charge handler spread across keyed worker lanes, plus a batch handler
/// that settles cleared payments through a transactional publisher.
///
/// `.transactional()` exists only because the in-memory `MemoryPublisher` implements
/// `TransactionalPublisher`; with it, `include_batch_publishing` buffers a page's replies and
/// commits them atomically.
pub(crate) fn payments(
    broker: &MemoryBroker,
    metrics: &Metrics,
) -> impl RouterDef<MemoryBroker, Repository> + use<> {
    let settlements = TypedPublisher::new(broker.publisher()).transactional();

    Router::new()
        .layer(metrics.consume_layer())
        .include(payments::process_payment)
        .include_batch_publishing(payments::settle, settlements)
}
