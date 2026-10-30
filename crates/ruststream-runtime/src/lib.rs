//! The delivery body of [RustStream](https://docs.rs/ruststream), shared by every runner: the
//! router and its mount chain, the dispatch, the publish pipeline, the codecs and the in-process
//! dispatch the test harness drives. A runner crate (`ruststream-runtime-tokio`) owns the app's
//! lifecycle and spawns this body on its runtime.
//!
//! Services and broker crates use these items through the `ruststream` facade, which re-exports
//! them under the paths its documentation teaches:
//!
//! - the crate root as [`ruststream::runtime`](https://docs.rs/ruststream/latest/ruststream/runtime/);
//! - [`codec`] as [`ruststream::codec`](https://docs.rs/ruststream/latest/ruststream/codec/);
//! - [`testing`], the broker contract of the test harness, as part of
//!   [`ruststream::testing`](https://docs.rs/ruststream/latest/ruststream/testing/).
//!
//! # Features
//!
//! The crate is `#![no_std]`. Its features:
//!
//! - `alloc`: the delivery body, whose headers, payloads, names and errors live on the heap. A
//!   device with an allocator turns on this feature alone; without it the crate holds the traits
//!   a runner implements ([`Clock`], [`ShutdownWait`]).
//! - `std` (default): `alloc`, the standard library's side of the dependencies, the broker
//!   wrapper [`Bindable`], and the panic policy's catching part. Without `std` a panic aborts.
//! - `json`, `msgpack`, `cbor`, `asyncapi`, `otel`, `testing`, `tokio` and `poll-diagnostics`
//!   turn on `std`; `metrics` turns on `alloc`.

#![no_std]

#[cfg(feature = "alloc")]
extern crate alloc;
#[cfg(any(feature = "std", test))]
extern crate std;

// The contract, under the names the runtime's modules reach it by (`crate::Broker`,
// `crate::bindable`, ...).
#[cfg(feature = "alloc")]
#[allow(clippy::wildcard_imports)]
use ruststream_core::*;

/// Whether this build's delivery body records what the test harness or OpenTelemetry reads: a
/// batch then allocates the records on top of what its form allocates. Read by the runners'
/// allocation tests, which hold the production body to its count. Machinery; never named
/// directly.
#[doc(hidden)]
pub const INSTRUMENTED_BODY: bool = cfg!(any(feature = "testing", feature = "otel"));

#[cfg(feature = "alloc")]
pub mod codec;
pub mod testing;
#[cfg(test)]
mod testkit;

/// Re-exported for the [`register_testable_broker!`] macro's expansion; not a stable API.
#[cfg(feature = "testing")]
#[doc(hidden)]
pub use inventory;

#[cfg(feature = "alloc")]
mod batch;
#[cfg(feature = "alloc")]
mod batch_inject;
#[cfg(feature = "alloc")]
mod batch_publishing;
#[cfg(feature = "alloc")]
mod context;
#[cfg(feature = "alloc")]
mod dispatch;
#[cfg(feature = "alloc")]
mod dynstack;
#[cfg(feature = "alloc")]
mod extract;
#[cfg(feature = "alloc")]
mod failure;
#[cfg(feature = "alloc")]
mod handle;
#[cfg(feature = "alloc")]
mod handler;
mod host;
#[cfg(feature = "alloc")]
mod in_messages;
#[cfg(feature = "alloc")]
mod inject;
#[cfg(feature = "alloc")]
mod input;
#[cfg(feature = "alloc")]
mod lifecycle;
#[cfg(feature = "tokio")]
mod main_runtime;
#[cfg(feature = "alloc")]
mod metadata;
#[cfg(feature = "alloc")]
mod middleware;
#[cfg(feature = "poll-diagnostics")]
pub(crate) mod poll_diagnostics;
#[cfg(feature = "alloc")]
mod publish;
#[cfg(feature = "alloc")]
mod publishing;
#[cfg(feature = "alloc")]
mod redelivery;
#[cfg(feature = "alloc")]
mod retry;
#[cfg(feature = "alloc")]
mod router;
#[cfg(feature = "asyncapi")]
mod schema_doc;
#[cfg(feature = "asyncapi")]
#[doc(hidden)]
pub use schema_doc::{join_headers, schema_json};
#[cfg(feature = "alloc")]
mod settings;
#[cfg(feature = "alloc")]
mod slot;
#[cfg(feature = "alloc")]
mod subscriber_def;
#[cfg(feature = "alloc")]
mod typed;

/// The subscriber a source opens, for broker `B` (the source resolves against the broker's
/// connected form). Tames the long projection in bounds.
#[cfg(feature = "alloc")]
pub(crate) type SourceSubscriber<B, S> = <S as SubscriptionSource<Connected<B>>>::Subscriber;

/// The message that subscriber yields. See [`SourceSubscriber`].
#[cfg(feature = "alloc")]
pub(crate) type SourceMessage<B, S> = <SourceSubscriber<B, S> as Subscriber>::Message;

/// The connected form of broker `B` of either family: the same type as
/// [`Connected`] wherever `B` is a [`Broker`].
#[cfg(feature = "alloc")]
pub(crate) type LocalConnected<B> = <B as LocalBroker>::Connected;

/// The subscriber a source opens on a broker of either family: the same type as
/// [`SourceSubscriber`] wherever both are named.
#[cfg(feature = "alloc")]
pub(crate) type LocalSourceSubscriber<B, S> =
    <S as LocalSubscriptionSource<LocalConnected<B>>>::Subscriber;

/// The message that subscriber yields. See [`LocalSourceSubscriber`].
#[cfg(feature = "alloc")]
pub(crate) type LocalSourceMessage<B, S> =
    <LocalSourceSubscriber<B, S> as LocalSubscriber>::Message;

/// The batch that subscriber yields, where it batches. See [`SourceSubscriber`].
#[cfg(feature = "alloc")]
pub(crate) type SourceBatch<B, S> = <SourceSubscriber<B, S> as BatchSubscriber>::Batch;

// The definition-trait dispatch SPI the retired legacy emission used to implement in user
// crates is internal machinery now: the modules stay, but only what the crate's own mounts
// reach through this path is re-exported.
#[cfg(feature = "alloc")]
#[doc(hidden)]
pub use batch::{
    BatchHandler, LocalBatch, LocalBatchHandler, TakenBatch, batch_verdict, uniform_batch,
};
#[cfg(feature = "alloc")]
pub use context::{After, Context};
#[cfg(feature = "alloc")]
#[doc(hidden)]
pub use dispatch::{
    Delivery, HeadersAtOnce, HeadersOnDemand, LendHeaders, Lent, Slot as DeliverySlot, Turn,
    dispatch, placed, run_batch, run_sequential,
};
#[cfg(feature = "alloc")]
pub use dispatch::{Placement, RETRY_COUNT_HEADER, Workers};
#[cfg(feature = "alloc")]
pub use dynstack::{DynMiddleware, DynStack, DynStackHandler, Next};
#[cfg(feature = "alloc")]
pub use extract::{Ctx, CtxKey, FromContext, FromRef, Headers, State};
#[cfg(feature = "alloc")]
#[doc(hidden)]
pub use failure::DispatchFailure;
#[cfg(feature = "alloc")]
pub use failure::{FailurePolicies, FailurePolicy};
#[cfg(feature = "alloc")]
#[doc(hidden)]
pub use handle::{
    Axis, AxisDocs, Batch, BatchCarried, BatchPair, Batched, BatchedAxis, DeclaredDest,
    DecodeOutcome, DocState, Docs, HandleValue, IsDocumented, NamedDest, OneByOne, Probed,
    ProbedDeclaredReplyDef, ProbedDocs, ProbedReplyDef, ReplyValue, ResolvedDest, Sealed, Solo,
    SoloAxis, SoloCarried, SoloPair, VerdictFamily, probed_declared_reply_def, probed_def,
    probed_reply_def,
};
#[cfg(feature = "alloc")]
pub use handle::{
    BatchDeserialized, BatchSet, Deserialized, Documentable, Documented, EncodedReply, Handle,
    Input, IntoSource, LocalHandle, LocalOutEntry, LocalSlot, Message, OutEntry, Outs, ReplyShape,
    Serialized, SerializedReply, Slot, SoloDeserialized, SoloSet, Undocumented, ValueBuilder,
    Verdict, local_subscriber, subscriber,
};
#[cfg(feature = "alloc")]
#[doc(hidden)]
pub use handle::{
    DeclaresReply, EntryMarkers, LocalEntryMarkers, LocalOutPos, OutPos, RedirectableReply,
    ReplyDest, ReplyDestination, ReplyFormFor, ReplyHeadersSchema, ReplyRoute, ResolveDestination,
    SelectSlot, SlotDictionary, UnbuiltDefinition, WireDocs, declared_reply_destination,
    reply_destination,
};
#[cfg(feature = "alloc")]
#[doc(hidden)]
pub use handler::IntoOutcome;
#[cfg(feature = "alloc")]
pub use handler::{Handler, HandlerOutcome, LocalHandler};
pub use host::{Clock, ShutdownWait};
#[doc(hidden)]
pub use host::{Detach, Halt, HostDetach, LocalDetach, OnOneThread};
#[cfg(feature = "tokio")]
pub use main_runtime::MainRuntime;
// The status half of `HandlerOutcome`, for the feature modules outside `runtime` that read it.
#[cfg(any(feature = "metrics", feature = "otel", feature = "testing"))]
#[doc(hidden)]
pub use handler::HandlerResult;
#[cfg(feature = "alloc")]
pub use in_messages::{
    CodecDiscriminator, DecodeSet, InMessages, InVariant, MessageDeclaration, VariantOf,
};
#[cfg(feature = "alloc")]
#[doc(hidden)]
pub use in_messages::{
    VariantLane, every_variant_failed, header_missing, header_unknown, no_variant_chosen,
};
#[cfg(feature = "alloc")]
pub use inject::Out;
// Public and hidden: a hand-written low-level def (`SubscriberDef` / `BatchDef`) names its
// input kind, and the self-deserializing one is how such a def opts onto the byte transport.
#[cfg(feature = "std")]
#[doc(hidden)]
pub use crate::bindable::ConnectedSlot;
#[cfg(feature = "alloc")]
#[doc(hidden)]
pub use input::{Provided, TypedLane};
#[cfg(feature = "std")]
#[doc(hidden)]
pub use lifecycle::BrokerCell;
#[cfg(feature = "testing")]
#[doc(hidden)]
pub use lifecycle::InProcessConnect;
#[cfg(feature = "alloc")]
#[doc(hidden)]
pub use lifecycle::{BoxError, BoxFuture, BrokerLifecycle, ConnectedLifecycle};
#[cfg(feature = "alloc")]
pub use metadata::{
    HandlerMetadata, HeaderDiscriminator, IncomingMessageMetadata, OutgoingKind,
    OutgoingMessageMetadata,
};
#[cfg(feature = "alloc")]
pub use middleware::{BlanketLayer, HandlerExt, Identity, Layer, LocalBlanketLayer, Stack, layers};
#[cfg(feature = "poll-diagnostics")]
#[doc(hidden)]
pub use poll_diagnostics::{BUCKET_BOUNDS_NS, PollStats};
#[cfg(feature = "poll-diagnostics")]
pub use poll_diagnostics::{PollDiagnostics, PollReport};
// The reply wiring a mount site's chain builds, the live sinks it pairs into, and the step traits
// the chain resolves through: the chain names them for the user, so none of it is spelled in
// service code.
#[cfg(feature = "alloc")]
#[doc(hidden)]
pub use publish::{
    AddBatchReplyTransform, AddReplyTransform, Admits, AnyDeclared, CodecSlotOpen,
    DestinationSettled, DestinationUse, Direct, Either, EncodeOutcome, FitsOffer, FitsTaken,
    InTransaction, LowerOutTransforms, MapReplyPolicy, NameReplyCodec, NamedDestinationSend,
    NamesDestination, NamingOffered, NamingUntaken, NarrowToUse, PayloadError, PublishingDirectly,
    RawReplyWiring, ReplyPublisher, ReplyWiring, SendOnlyPolicy, SlotStackUse, SlotTransforms,
    Transactional, TransactionalReply, TypedPublisher, WireBytes, WirePayload,
};
#[cfg(feature = "alloc")]
pub use publish::{
    BatchPublishTransform, BatchPublishTransformStack, BatchTransformIdentity, BoundSegment,
    CallCodec, ContextKind, EncodedWire, ForBatch, ForReply, ForSlot, HeaderSource, HeadersUnset,
    LocalPublishSink, MapHeaders, MessageBody, MessageWire, MissingSegment, Names, OutPipeline,
    Outgoing, OutgoingName, PipelinePublishError, PublishAt, PublishBuilder, PublishCodec,
    PublishContext, PublishDynLayer, PublishDynNext, PublishDynStack, PublishError, PublishExt,
    PublishHeaders, PublishIdentity, PublishLayer, PublishNext, PublishPipeline, PublishSink,
    PublishStack, PublishTransform, PublishTransformIdentity, PublishTransformStack, Reads,
    ResolvedName, SatisfiesContract, SerializePayloadError, SerializedWire, SlotContext,
    SuppliedName, TemplateAddress, TransactionPublishError, TransactionScope, TypedHeaders,
    TypedTransaction, UnnamedCodec, for_batch,
};
// The builder's entry point, for the surfaces outside `runtime` that offer one: the test harness
// injects through the same positions as a live publish.
#[cfg(feature = "std")]
pub use crate::bindable::{Bindable, Bound, BrokerRegistration};
#[cfg(feature = "alloc")]
#[doc(hidden)]
pub use inject::{InjectHandler, LocalInject};
#[cfg(feature = "alloc")]
#[doc(hidden)]
pub use publish::message_of;
#[cfg(feature = "alloc")]
#[doc(hidden)]
pub use publishing::{LocalPublishing, PublishingHandler};
#[cfg(feature = "alloc")]
#[doc(hidden)]
pub use redelivery::{
    CopiesAddressed, CopyPathAddress, CopyPathPairing, PublishesCopiesHere, RetryPairing,
    RetrySetup, ScopeDelivery, open_mounted_subscriber,
};
#[cfg(feature = "alloc")]
pub use retry::Retry;
#[cfg(feature = "alloc")]
#[doc(hidden)]
pub use retry::{
    Absent, CapOpen, DeadLetterOpen, DeclareCap, DeclareDeadLetter, DeclareMount, Declaring,
    DestinationDeclared, DestinationLast, DestinationOpen, DestinationUndeclared, FixedDestination,
    OpenDestination, Present, Retried, RetryChain, RetryMount, RetryOffer, RetryOpen, RetryPos,
    RetryStackUse, RouteDeclaring, RoutePosition, SettlesDestination, StepOpen, StepTaken,
};
#[cfg(feature = "alloc")]
#[doc(hidden)]
pub use router::{
    AttachDeclaration, AttachRetry, BatchPublishInjectMount, BatchPublishMount, DefaultReply,
    LayerFits, MountSink, OpenLayered, OpenRoutes, Opened, OpenedBatchRoute, OpenedRoute, PoolFits,
    PublishInjectMount, PublishMount, RawReplyInjectMount, RawReplyMount, ReplyAttachment,
    RetryDestinationsDeclared, RouteMetadata, RouteSubscription, RouterBroker, RouterCommit,
    RouterMount, RouterPipe, RunRoutes, RunsBatches, TakesWorkers,
};
#[cfg(feature = "alloc")]
#[doc(hidden)]
pub use router::{
    Dispatched, LocalDispatch, LocalPlanned, LocalRoute, OpenedDispatch, SequentialPlan,
};
#[cfg(feature = "alloc")]
pub use router::{
    IncludeDef, LocalPipeline, MapPublisher, Router, RouterDef, RouterHandlers, RouterOut,
    RouterPublishing, RouterPublishingOut, RouterWith, forms,
};
#[cfg(feature = "alloc")]
pub use settings::{
    AllOpen, BatchSized, BatchStep, Declared, FailureStep, Fixed, MapSourceStep, NameStep, Open,
    StartAtStep, SubscriberBuilder, SubscriberSettings, WorkersStep,
};
#[cfg(feature = "alloc")]
#[doc(hidden)]
pub use settings::{CapsBatches, DefinitionInputCodec, MountsWith, Sequential};
#[cfg(feature = "alloc")]
#[doc(hidden)]
pub use slot::{
    AdmitsAt, AdmitsSlotAt, BatchTransformLast, BindAt, BindSlot, Both, CodecAt, CodecLast,
    DestinationOffer, InitSlots, IntoSlotSource, MapPolicyAt, MapPolicyLast, MissingSlot,
    MountOffer, NamedStep, NoOutBound, NoReply, OutAttachment, OutPosition, PositionOptions,
    ReplyLast, ReplyOffer, ReplyOpen, ReplyStep, SlotCodec, SlotPolicy, SlotPos, TransactionalLast,
    TransformAt, TransformLast, WithSource,
};
#[cfg(feature = "alloc")]
pub use slot::{
    BindSlots, ContainsMessage, DefaultSlot, HasSlots, ListOffer, LocalBindSlots, LocalHasSlots,
    OutMessages, OutSlot, PublishedThrough, Reply, SlotPublisher, Unrestricted,
};
#[cfg(feature = "alloc")]
#[doc(hidden)]
pub use typed::LocalTyped;
#[cfg(feature = "alloc")]
pub use typed::{Typed, typed};
