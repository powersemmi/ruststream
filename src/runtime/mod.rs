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
    App, AppInfo, BrokerScope, HealthProbe, HealthState, Mounting, RunningApp, RustStream,
    RustStreamError, Setup, Wired,
};
#[doc(hidden)]
pub use app::{IncludeMount, OnBuild, OnDrop, ScopeCommit, ScopeTerminal};
#[cfg(feature = "testing")]
pub(crate) use app::{LifecycleHook, RegisteredBroker, Starter, TestParts};
// The definition-trait dispatch SPI the retired legacy emission used to implement in user
// crates is internal machinery now: the modules stay, but only what the crate's own mounts
// reach through this path is re-exported.
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
    Axis, AxisDocs, DeclaredDest, DocState, Docs, HandleValue, IsDocumented, NamedDest, OneByOne,
    Page, PagePair, Paged, PagedAxis, Probed, ProbedDocs, ProbedReplyDef, ReplyValue, Sealed, Solo,
    SoloAxis, SoloPair, VerdictFamily, probed_def, probed_reply_def,
};
pub use handle::{
    Deserialized, Documentable, Documented, EncodedReply, Handle, Input, IntoSource, Message, Outs,
    PageDeserialized, ReplyShape, Serialized, SerializedReply, Slot, SoloDeserialized,
    Undocumented, ValueBuilder, Verdict, subscriber,
};
#[doc(hidden)]
pub use handle::{
    EntryMarkers, OutPos, ReplyDest, ReplyFormFor, ReplyHeadersSchema, ReplyRoute, SelectSlot,
    UnbuiltDefinition, WireDocs,
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
// Public and hidden: a hand-written low-level def (`SubscriberDef` / `BatchDef`) names its
// input kind, and the self-deserializing one is how such a def opts onto the byte transport.
#[doc(hidden)]
pub use input::Provided;
#[cfg(feature = "testing")]
pub(crate) use lifecycle::ConnectedLifecycle;
#[doc(hidden)]
pub use lifecycle::ConnectedSlot;
pub use metadata::{HandlerMetadata, OutgoingMessageMetadata};
pub use middleware::{BlanketLayer, HandlerExt, Identity, Layer, Stack, layers};
// The reply wiring a mount site's chain builds, the live sinks it pairs into, and the step traits
// the chain resolves through: the chain names them for the user, so none of it is spelled in
// service code.
#[doc(hidden)]
pub use publish::{
    AddBatchReplyTransform, AddReplyTransform, Admits, AnyDeclared, CodecSlotOpen, Direct,
    InTransaction, LowerOutTransforms, MapReplyPolicy, NameReplyCodec, PublishingDirectly,
    RawReplyWiring, ReplyPublisher, ReplyWiring, Transactional, TransactionalReply, TypedPublisher,
    WireBytes, WirePayload,
};
pub use publish::{
    BatchPublishTransform, BatchPublishTransformStack, BatchTransformIdentity, BoundSegment,
    CallCodec, EncodedWire, ForBatch, HeaderSource, HeadersUnset, MapHeaders, MessageBody,
    MessageWire, MissingSegment, OutPipeline, OutTransform, OutTransformIdentity,
    OutTransformStack, Outgoing, PipelinePublishError, PublishAt, PublishBuilder, PublishCodec,
    PublishContext, PublishDynLayer, PublishDynNext, PublishDynStack, PublishError, PublishExt,
    PublishHeaders, PublishIdentity, PublishLayer, PublishNext, PublishPipeline, PublishSink,
    PublishStack, PublishTransform, PublishTransformIdentity, PublishTransformStack, ResolvedName,
    SatisfiesContract, SerializedWire, SuppliedName, TemplateAddress, TransactionPublishError,
    TransactionScope, TypedHeaders, TypedTransaction, UnnamedCodec, for_batch,
};
// The builder's entry point, for the surfaces outside `runtime` that offer one: the test harness
// injects through the same positions as a live publish.
#[cfg(feature = "testing")]
pub(crate) use publish::message_of;
pub use publish_source::{Bindable, Bound, BrokerRegistration};
pub use publisher_registry::ErasedPublisher;
#[doc(hidden)]
pub use router::{DefaultReply, ReplyAttachment, RouterCommit, RouterMount};
pub use router::{
    IncludeDef, MapPublisher, Router, RouterDef, RouterHandlers, RouterOut, RouterPublishing,
    RouterPublishingOut, RouterSink, RouterWith, forms,
};
pub use settings::{
    AllOpen, BatchStep, Declared, FailureStep, Fixed, MapSourceStep, NameStep, Open, PageSized,
    StartAtStep, SubscriberBuilder, SubscriberSettings, WorkersStep,
};
#[doc(hidden)]
pub use settings::{CapsPages, DefinitionInputCodec, MountsWith};
#[doc(hidden)]
pub use slot::{
    BatchTransformLast, BindAt, BindSlot, CodecAt, CodecLast, InitSlots, IntoSlotSource,
    MapPolicyAt, MapPolicyLast, MissingSlot, NamedStep, NoOutBound, NoReply, OutAttachment,
    ReplyLast, ReplyOpen, ReplyStep, SlotCodec, SlotPos, TransactionalLast, TransformAt,
    TransformLast, WithSource,
};
pub use slot::{
    BindSlots, ContainsMessage, DefaultSlot, HasSlots, OutMessages, OutSlot, PublishedThrough,
    Reply, SlotPublisher, Unrestricted,
};
pub use typed::{Typed, typed};
