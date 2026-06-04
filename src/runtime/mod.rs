//! Application object, middleware and dispatch.

mod app;
mod context;
mod dispatch;
mod dynstack;
mod handler;
mod lifecycle;
mod metadata;
mod middleware;
mod publish;
mod publisher_registry;
mod publishing;
mod router;
mod subscriber_def;
mod typed;

pub use app::{AppInfo, BrokerScope, RustStream, RustStreamError};
pub use context::{Context, State};
pub use dynstack::{DynMiddleware, DynStack, DynStackHandler, Next};
pub use handler::{Handler, HandlerResult, IntoHandlerResult};
pub use metadata::HandlerMetadata;
pub use middleware::{HandlerExt, Identity, Layer, Stack, layers};
pub use publish::{Outgoing, PublishMiddleware, PublishNext, ScopedPublisher, TypedPublisher};
pub use publisher_registry::ErasedPublisher;
pub use publishing::{PublishingDef, PublishingHandler};
pub use router::Router;
pub use subscriber_def::SubscriberDef;
pub use typed::{DecodeFailure, Typed, typed};
