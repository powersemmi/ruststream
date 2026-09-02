//! Application object, middleware and dispatch.

mod app;
mod batch;
mod batch_inject;
mod batch_publishing;
pub mod cli;
mod context;
mod dispatch;
mod dynstack;
mod extract;
mod failure;
mod handle;
mod handler;
mod inject;
mod input;
mod lifecycle;
mod metadata;
mod middleware;
mod publish;
mod publish_source;
mod publisher_registry;
mod publishing;
mod router;
mod settings;
mod slot;
mod subscriber_def;
mod typed;

/// The subscriber a source opens, for broker `B` (the source resolves against the broker's
/// connected form). Tames the long projection in bounds.
pub(crate) type SourceSubscriber<B, S> =
    <S as crate::SubscriptionSource<crate::Connected<B>>>::Subscriber;

/// The message that subscriber yields. See [`SourceSubscriber`].
pub(crate) type SourceMessage<B, S> = <SourceSubscriber<B, S> as crate::Subscriber>::Message;

pub use app::{
    App, AppInfo, BrokerScope, HealthProbe, HealthState, IncludeBatchOut, IncludeBatchPublishing,
    IncludeBatchPublishingOut, IncludeOut, IncludePublishing, IncludePublishingOut, IncludeSlots,
    IncludeSlotsWithReply, IncludeWith, RunningApp, RustStream, RustStreamError, Setup, SlotCommit,
    Wired,
};
#[cfg(feature = "testing")]
pub(crate) use app::{LifecycleHook, RegisteredBroker, Starter, TestParts};
// The definition-trait dispatch SPI the retired legacy emission used to implement in user
// crates is internal machinery now: the modules stay, but only what the crate's own mounts
// reach through this path is re-exported.
pub(crate) use batch::{RawSliceHandler, SliceHandler};
#[doc(hidden)]
pub use batch::{page_verdict, uniform_page};
pub use context::{After, Context};
pub use dispatch::{RETRY_COUNT_HEADER, Workers};
pub use dynstack::{DynMiddleware, DynStack, DynStackHandler, Next};
pub use extract::{Ctx, FromContext, FromRef, Headers, State};
#[cfg(feature = "testing")]
pub(crate) use failure::ErrorShutdown;
pub use failure::{FailurePolicies, FailurePolicy};
#[doc(hidden)]
pub use handle::{
    Axis, AxisDocs, BareReply, DeclaredDest, DefaultReplyAttach, DocState, Docs, EncodedReply,
    HandleValue, IsDocumented, NamedDest, OneByOne, Page, PageBytes, PagePair, Paged, PagedAxis,
    Probed, ProbedDocs, ProbedReplyDef, ReplyPublisherForm, ReplyValue, Sealed, Solo, SoloAxis,
    SoloBytes, SoloPair, VerdictFamily, probed_def, probed_reply_def,
};
pub use handle::{
    Bare, Documentable, Documented, Handle, Input, IntoSource, Message, Outs, Payload, Publish,
    Slot, Undocumented, ValueBuilder, Verdict, subscriber,
};
#[doc(hidden)]
pub use handle::{
    EntryMarkers, OutPos, ReplyDest, ReplyFormFor, ReplyHeadersSchema, ReplyShape,
    ReplySlotFormFor, SealedBatchPublishing, SealedBatchPublishingOut, SealedPublishing,
    SealedPublishingOut, SealedRawReply, SealedRawReplyOut, SelectSlot, SplitAttach,
    UnbuiltDefinition,
};
#[doc(hidden)]
pub use handler::IntoOutcome;
pub use handler::{Handler, HandlerOutcome};
// The status half of `HandlerOutcome`, for the crate's own policies, dispatch and test seams.
// The consumers outside `runtime` are all feature-gated (metrics, otel, testing), so a minimal
// build leaves this re-export unused.
#[allow(unused_imports)]
pub(crate) use handler::HandlerResult;
pub use inject::Out;
pub(crate) use input::RawBytes;
#[cfg(feature = "testing")]
pub(crate) use lifecycle::ConnectedLifecycle;
#[doc(hidden)]
pub use lifecycle::ConnectedSlot;
pub use metadata::{HandlerMetadata, OutgoingMessageMetadata};
pub use middleware::{BlanketLayer, HandlerExt, Identity, Layer, Stack, layers};
pub use publish::{
    BatchPublishTransform, BatchPublishTransformStack, BatchTransformIdentity, BoundSegment,
    CallCodec, ForBatch, HeaderSource, HeadersUnset, MapHeaders, MessageBody, MissingSegment,
    Outgoing, PublishAt, PublishBuilder, PublishCodec, PublishContext, PublishDynLayer,
    PublishDynNext, PublishDynStack, PublishError, PublishExt, PublishHeaders, PublishIdentity,
    PublishLayer, PublishNext, PublishPipeline, PublishSink, PublishStack, PublishTransform,
    PublishTransformIdentity, PublishTransformStack, RawBody, ReplyPublisher, ReplyWiring,
    ResolvedName, SatisfiesContract, SuppliedName, TemplateAddress, TransactionPublishError,
    TransactionScope, Transactional, TypedHeaders, TypedPublisher, TypedTransaction, for_batch,
};
// The builder's entry points, for the surfaces outside `runtime` that offer one: the test
// harness injects through the same positions as a live publish.
#[cfg(all(
    feature = "testing",
    any(feature = "json", feature = "cbor", feature = "msgpack")
))]
pub(crate) use publish::message_of;
#[cfg(feature = "testing")]
pub(crate) use publish::raw_of;
pub use publish_source::{Bindable, Bound, BrokerRegistration};
pub use publisher_registry::ErasedPublisher;
pub use router::{
    DefaultBareReply, IncludeDef, Router, RouterBatchOut, RouterBatchPublishing,
    RouterBatchPublishingOut, RouterDef, RouterHandlers, RouterOut, RouterPublishing,
    RouterPublishingOut, RouterRawReply, RouterRawReplyOut, RouterSink, RouterSlots,
    RouterSlotsWithReply, RouterWith, forms,
};
#[doc(hidden)]
pub use router::{DefaultReply, RouterCommit, RouterMount, RouterSlotCommit};
pub use settings::{
    AllOpen, BufferedStep, Declared, FailureStep, Fixed, MapSourceStep, NameStep, Open,
    StartAtStep, SubscriberBuilder, SubscriberSettings, WorkersStep,
};
#[doc(hidden)]
pub use settings::{DefinitionInputCodec, MountsWith};
#[doc(hidden)]
pub use slot::{BindSlot, InitSlots, IntoSlotSource, MissingSlot, SlotPos, WithSource};
pub use slot::{
    BindSlots, ContainsMessage, DefaultSlot, HasSlots, OutMessages, OutSlot, PublishedThrough,
    SlotPublisher, Unrestricted,
};
pub use typed::{Typed, typed};
