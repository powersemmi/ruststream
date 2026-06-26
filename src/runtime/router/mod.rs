//! [`Router`]: a broker-agnostic, statically-typed group of handler registrations.
//!
//! A `Router` collects subscriber registrations without a live broker, so a set of handlers can be
//! defined in its own module and mounted later. It is a consuming builder: each `include`/
//! `subscribe`/`handle` call takes the router by value and returns a new type carrying the added
//! registration, so the full registration list lives in the type. A builder function therefore
//! returns an opaque [`RouterDef`] rather than naming that type.
//!
//! Bind it to a broker by passing it to
//! [`BrokerScope::include_router`](crate::runtime::BrokerScope::include_router) inside
//! [`RustStream::with_broker`](crate::runtime::RustStream::with_broker). Nothing connects or
//! subscribes until the application runs. Unlike a hand-rolled callback group, the app's global
//! [`layer`](crate::runtime::RustStream::layer) stack DOES reach router handlers: each is wrapped
//! with the app's [`BlanketLayer`](crate::runtime::BlanketLayer) global when the router is mounted.

mod builder;
mod include;
mod routes;
mod sink;

pub use builder::Router;
pub use routes::{RouterDef, RouterHandlers};
pub use sink::RouterSink;

use crate::{Subscriber, SubscriptionSource};

use crate::runtime::batch::{BatchDef, TypedBatch};
use crate::runtime::subscriber_def::SubscriberDef;
use crate::runtime::typed::Typed;

use routes::{BatchPublishingRoute, BatchRoute, PublishingRoute, SubscribeRoute};

/// The message a source's subscriber yields, for broker `B`. Tames the long projection in bounds
/// and return types.
type SourceMessage<B, S> = <<S as SubscriptionSource<B>>::Subscriber as Subscriber>::Message;

/// The route a [`SubscriberDef`] `D` mounted on source `S` (decoded with `C`) becomes. Names the
/// otherwise unwieldy registration type.
type TypedRoute<B, S, D, C> = SubscribeRoute<
    S,
    Typed<SourceMessage<B, S>, <D as SubscriberDef>::Input, C, <D as SubscriberDef>::Handler>,
>;

/// The router that mounting a [`SubscriberDef`] `D` on source `S` (decoded with `C`) onto `R`
/// produces. `RC` / `RL` are the router's own codec and layer parameters, carried unchanged.
type IncludedRouter<B, S, D, C, RC, RL, R> = Router<B, (TypedRoute<B, S, D, C>, R), RC, RL>;

/// The route a [`BatchDef`] `D` mounted on source `S` (decoded with `C`) becomes.
type BatchTypedRoute<B, S, D, C> = BatchRoute<
    S,
    TypedBatch<SourceMessage<B, S>, <D as BatchDef>::Input, C, <D as BatchDef>::Handler>,
>;

/// The router that mounting a [`BatchDef`] `D` on source `S` (decoded with `C`) onto `R`
/// produces. `RC` / `RL` are the router's own codec and layer parameters, carried unchanged.
type IncludedBatchRouter<B, S, D, C, RC, RL, R> =
    Router<B, (BatchTypedRoute<B, S, D, C>, R), RC, RL>;

/// The router that mounting a publishing [`PublishingDef`](crate::runtime::PublishingDef) `D` on
/// source `S` (decoded with `C`, replying through a `P`/`PC`/`PL` publisher) onto `R` produces.
/// `RC` / `RL` are the router's own codec and layer parameters, carried unchanged.
type PublishingRouter<B, S, D, C, P, PC, PL, RC, RL, R> =
    Router<B, (PublishingRoute<S, D, C, P, PC, PL>, R), RC, RL>;

/// The router that mounting a batch publishing
/// [`BatchPublishingDef`](crate::runtime::BatchPublishingDef) `D` on source `S` (decoded with `C`,
/// replying through the [`ReplyPublisher`](crate::runtime::ReplyPublisher) `RP`) onto `R` produces.
type BatchPublishingRouter<B, S, D, C, RP, RC, RL, R> =
    Router<B, (BatchPublishingRoute<S, D, C, RP>, R), RC, RL>;

/// The router that a [`Router::subscribe_batch`] closure registration produces: the slice
/// handler `H` is wrapped in a [`TypedBatch`] decoding elements to `T` with `C`.
type SubscribedBatchRouter<B, S, T, C, H, RC, RL, R> =
    Router<B, (BatchRoute<S, TypedBatch<SourceMessage<B, S>, T, C, H>>, R), RC, RL>;

/// The router that [`Router::merge`] produces: the merged router becomes one registration in the
/// list.
type MergedRouter<B, R2, C2, L2, RC, RL, R> = Router<B, (Router<B, R2, C2, L2>, R), RC, RL>;
