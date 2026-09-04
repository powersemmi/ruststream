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
mod builders;
mod form_eager;
mod form_out;
mod form_publish;
pub mod forms;
mod include;
mod mount;
mod routes;
mod routes_inject;
mod routes_publish;
mod sink;

pub use builder::Router;
pub use builders::{MapPublisher, RouterOut, RouterPublishing, RouterPublishingOut, RouterWith};
#[doc(hidden)]
pub use builders::RouterCommit;
// The typed default-reply token is machinery, but the macro expansion names it in generated
// types (the default attach of a sealed reply definition), so it is public and hidden.
#[doc(hidden)]
pub use mount::DefaultReply;
pub use mount::IncludeDef;
pub(crate) use mount::InputCodec;
#[doc(hidden)]
pub use mount::{ReplyAttachment, RouterMount};
pub use routes::{RouterDef, RouterHandlers};
pub use sink::RouterSink;

use crate::runtime::batch::{BatchDef, DeserializedBatch, TypedBatch};
use crate::runtime::subscriber_def::SubscriberDef;
use crate::runtime::typed::Typed;

use routes::{BatchRoute, SubscribeRoute};
use routes_inject::{BatchInjectRoute, InjectRoute};
use routes_publish::{BatchPublishingRoute, PublishingRoute, RawReplyRoute};

pub(crate) use crate::runtime::SourceMessage;

/// The route a [`SubscriberDef`] `D` mounted on source `S` (decoded with `C`) becomes. Names the
/// otherwise unwieldy registration type.
pub(crate) type TypedRoute<B, S, D, C> = SubscribeRoute<
    S,
    Typed<SourceMessage<B, S>, <D as SubscriberDef>::Input, C, <D as SubscriberDef>::Handler>,
    <D as SubscriberDef>::Context,
>;

/// The router that mounting a [`SubscriberDef`] `D` on source `S` (decoded with `C`) onto `R`
/// produces. `RC` / `RL` / `RP` are the router's own codec, layer and slot-pipeline parameters,
/// carried unchanged.
pub(crate) type IncludedRouter<B, S, D, C, RC, RL, RP, R> =
    Router<B, (TypedRoute<B, S, D, C>, R), RC, RL, RP>;

/// The route a [`BatchDef`] `D` mounted on source `S` (decoded with `C`) becomes.
pub(crate) type BatchTypedRoute<B, S, D, C> = BatchRoute<
    S,
    TypedBatch<SourceMessage<B, S>, <D as BatchDef>::Input, C, <D as BatchDef>::Handler>,
    <D as BatchDef>::Context,
>;

/// The router that mounting a [`BatchDef`] `D` on source `S` (decoded with `C`) onto `R`
/// produces. See [`IncludedRouter`] for the carried parameters.
pub(crate) type IncludedBatchRouter<B, S, D, C, RC, RL, RP, R> =
    Router<B, (BatchTypedRoute<B, S, D, C>, R), RC, RL, RP>;

/// The route a self-deserializing [`BatchDef`] `D` mounted on source `S` becomes: no codec is
/// involved, so the adapter carries the message type, the element family `F` and the handler.
type DeserializedBatchRoute<B, S, D, F> = BatchRoute<
    S,
    DeserializedBatch<SourceMessage<B, S>, F, <D as BatchDef>::Handler>,
    <D as BatchDef>::Context,
>;

/// The router that mounting a self-deserializing [`BatchDef`] `D` on source `S` onto `R`
/// produces.
type IncludedRawBatchRouter<B, S, D, F, RC, RL, RP, R> =
    Router<B, (DeserializedBatchRoute<B, S, D, F>, R), RC, RL, RP>;

/// The router that mounting an injected definition `D` on source `S` (decoded with `C`,
/// resolving its startup injections against the attachment `E`) onto `R` produces.
type InjectedRouter<B, S, D, C, E, RC, RL, RP, R> =
    Router<B, (InjectRoute<S, D, C, E>, R), RC, RL, RP>;

/// The batch counterpart of [`InjectedRouter`].
type BatchInjectedRouter<B, S, D, C, E, RC, RL, RP, R> =
    Router<B, (BatchInjectRoute<S, D, C, E>, R), RC, RL, RP>;

/// The router that mounting a publishing [`PublishingDef`](crate::runtime::PublishingDef) `D` on
/// source `S` (decoded with `C`, replying through the policy `RP` and resolving its startup
/// injections against the attachment `E`) onto `R` produces. See [`IncludedRouter`] for the
/// carried parameters.
type PublishingRouter<B, S, D, C, Reply, E, RC, RL, RP, R> =
    Router<B, (PublishingRoute<S, D, C, Reply, E>, R), RC, RL, RP>;

/// The byte-reply counterpart of [`PublishingRouter`].
type RawReplyRouter<B, S, D, C, Reply, E, RC, RL, RP, R> =
    Router<B, (RawReplyRoute<S, D, C, Reply, E>, R), RC, RL, RP>;

/// The router that mounting a batch publishing
/// [`BatchPublishingDef`](crate::runtime::BatchPublishingDef) `D` on source `S` (decoded with `C`,
/// replying through the policy `Reply`) onto `R` produces.
type BatchPublishingRouter<B, S, D, C, Reply, E, RC, RL, RP, R> =
    Router<B, (BatchPublishingRoute<S, D, C, Reply, E>, R), RC, RL, RP>;

/// The router that [`Router::merge`] produces: the merged router becomes one registration in the
/// list.
type MergedRouter<B, R2, C2, L2, P2, RC, RL, RP, R> =
    Router<B, (Router<B, R2, C2, L2, P2>, R), RC, RL, RP>;
