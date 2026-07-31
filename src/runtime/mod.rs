//! Application object, middleware and dispatch.

mod app;
mod batch;
mod batch_publishing;
pub mod cli;
mod context;
mod dispatch;
mod dynstack;
mod extract;
mod failure;
mod handler;
mod inject;
mod lifecycle;
mod metadata;
mod middleware;
mod publish;
mod publish_source;
mod publisher_registry;
mod publishing;
mod raw;
mod router;
mod subscriber_def;
mod typed;

pub use app::{
    App, AppInfo, BrokerScope, HealthProbe, HealthState, IncludeBatchPublishing, IncludeDef,
    IncludeOut, IncludePublishing, RunningApp, RustStream, RustStreamError, Setup, Wired, forms,
};
#[cfg(feature = "testing")]
pub(crate) use app::{LifecycleHook, RegisteredBroker, Starter, TestParts};
pub use batch::{BatchDef, BatchResult, IntoBatchResult, SliceHandler, TypedBatch};
pub use batch_publishing::{BatchPublishingCall, BatchPublishingDef, BatchPublishingHandler};
pub use context::{After, Context};
pub use dispatch::{RETRY_COUNT_HEADER, Workers};
pub use dynstack::{DynMiddleware, DynStack, DynStackHandler, Next};
pub use extract::{Ctx, FromContext, FromRef, State};
#[cfg(feature = "testing")]
pub(crate) use failure::ErrorShutdown;
pub use failure::{FailurePolicies, FailurePolicy};
pub use handler::{Handler, HandlerResult, IntoSettle, Settle};
pub use inject::{FromStartup, InjectCall, InjectDef, InjectHandler, Out, Seek};
#[cfg(feature = "testing")]
pub(crate) use lifecycle::ConnectedLifecycle;
#[doc(hidden)]
pub use lifecycle::ConnectedSlot;
pub use metadata::HandlerMetadata;
pub use middleware::{BlanketLayer, HandlerExt, Identity, Layer, Stack, layers};
pub use publish::{
    BatchPublishTransform, BatchPublishTransformStack, BatchTransformIdentity, ForBatch, Outgoing,
    PublishContext, PublishDynLayer, PublishDynNext, PublishDynStack, PublishIdentity,
    PublishLayer, PublishNext, PublishPipeline, PublishStack, PublishTransform,
    PublishTransformIdentity, PublishTransformStack, ReplyPublisher, ReplyWiring,
    TransactionPublishError, TransactionScope, Transactional, TypedPublisher, TypedTransaction,
    for_batch,
};
pub use publish_source::{Bindable, Bound, BrokerRegistration};
pub use publisher_registry::ErasedPublisher;
pub use publishing::{PublishingCall, PublishingDef, PublishingHandler};
pub use raw::{
    RawPublishingCall, RawPublishingDef, RawPublishingHandler, RawReplyCall, RawReplyDef,
    RawReplyHandler, RawSubscriberDef,
};
pub use router::{Router, RouterDef, RouterHandlers, RouterSink};
pub use subscriber_def::SubscriberDef;
pub use typed::{Typed, typed};
