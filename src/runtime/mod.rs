//! Application object, middleware and dispatch.

mod app;
mod batch;
mod batch_publishing;
pub mod cli;
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
pub use batch::{BatchDef, BatchResult, IntoBatchResult, SliceHandler, TypedBatch};
pub use batch_publishing::{BatchPublishingDef, BatchPublishingHandler};
pub use context::{Context, State};
pub use dispatch::{RETRY_COUNT_HEADER, Workers};
pub use dynstack::{DynMiddleware, DynStack, DynStackHandler, Next};
pub use handler::{Handler, HandlerResult, IntoHandlerResult};
pub use metadata::HandlerMetadata;
pub use middleware::{BlanketLayer, HandlerExt, Identity, Layer, Stack, layers};
pub use publish::{
    Outgoing, PublishIdentity, PublishLayer, PublishMiddleware, PublishNext, PublishStack,
    ReplyPublisher, ScopedPublisher, Transactional, TypedPublisher,
};
pub use publisher_registry::ErasedPublisher;
pub use publishing::{PublishingDef, PublishingHandler};
pub use router::{Router, RouterDef, RouterSink};
pub use subscriber_def::SubscriberDef;
pub use typed::{DecodeFailure, Typed, typed};
