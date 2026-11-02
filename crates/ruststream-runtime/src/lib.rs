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
//! - `alloc`: the heap form of the delivery body, whose headers, payloads, names, continuations
//!   and errors live on the heap. Without it the body builds in its fixed form: they live in
//!   storage of a fixed size, the codec is a `SliceCodec`, and nothing is allocated.
//! - `std` (default): `alloc`, the standard library's side of the dependencies, the broker
//!   wrapper [`Bindable`], and the panic policy's catching part. Without `std` a panic aborts.
//! - `asyncapi`, `otel`, `testing`, `tokio` and `poll-diagnostics` turn on `std`; `metrics` turns
//!   on `alloc`.
//! - `json`, `msgpack` and `cbor`, the codecs, turn on `alloc` and write through `std::io::Write`
//!   on every target with an operating system. On a bare-metal target `cbor` builds without
//!   `std`, and `json` and `msgpack` fail the build with a message that says so.

#![no_std]
// A build without `alloc` holds what the heap boxes inline (a failure, an error hook, a batch of
// outcomes), so the error of a fallible call and the biggest variant of an enum are large by
// design, and both sit on the failure path, not on the delivery's.
#![cfg_attr(
    not(ruststream_alloc),
    allow(clippy::result_large_err, clippy::large_enum_variant)
)]
// Without the `tokio` feature a continuation carries no `Send`, so the futures of the Tokio
// runner's worker are not `Send` either; the lint would flag every one of them.
#![cfg_attr(not(feature = "tokio"), allow(clippy::future_not_send))]

// The harness (`testing`) keeps its bookkeeping in `std` collections and shared pointers: it is
// code of the test build, and it does not turn the framework's `alloc` on.
#[cfg(any(ruststream_alloc, feature = "testing"))]
extern crate alloc;
#[cfg(any(feature = "std", feature = "testing", test))]
extern crate std;
// The tests of a build without `alloc` still build their fixtures with the standard library.
#[cfg(test)]
#[cfg(not(ruststream_alloc))]
extern crate alloc;

// The contract, under the names the runtime's modules reach it by (`crate::Broker`,
// `crate::bindable`, ...).
#[allow(clippy::wildcard_imports)]
use ruststream_core::*;

/// The headers of a message in this build: `HeaderMap` with `alloc`, `FixedHeaders` without.
pub(crate) use ruststream_core::Headers as HeaderSet;

/// Whether this build's delivery body records what the test harness or OpenTelemetry reads: a
/// batch then allocates the records on top of what its form allocates. Read by the runners'
/// allocation tests, which hold the production body to its count. Machinery; never named
/// directly.
#[doc(hidden)]
pub const INSTRUMENTED_BODY: bool = cfg!(any(feature = "testing", feature = "otel"));

pub mod codec;
pub mod testing;
#[cfg(test)]
#[cfg(ruststream_alloc)]
mod testkit;

/// Re-exported for the [`register_testable_broker!`] macro's expansion; not a stable API.
#[cfg(feature = "testing")]
#[doc(hidden)]
pub use inventory;

#[cfg(ruststream_alloc)]
mod app_info;
mod batch;
mod batch_inject;
mod batch_publishing;
mod context;
mod coop;
mod copied;
#[cfg(feature = "compio")]
mod core_ctx;
mod dispatch;
// Boxed futures per layer: the stack serves the heap variant.
#[cfg(ruststream_alloc)]
mod described;
#[cfg(ruststream_alloc)]
mod dynstack;
mod extract;
mod failure;
mod handle;
mod handler;
#[cfg(ruststream_alloc)]
mod health_state;
mod host;
mod in_messages;
mod inject;
// Under `alloc` the module is compiled by its own tests alone, where nothing re-exports it.
#[cfg(not(ruststream_alloc))]
mod bare_meta;
#[cfg(any(not(ruststream_alloc), test))]
#[cfg_attr(ruststream_alloc, allow(unreachable_pub))]
#[cfg_attr(not(ruststream_alloc), allow(dead_code))]
// the delivery body takes it up once it builds without `alloc`
mod inline;
mod input;
mod lifecycle;
#[cfg(feature = "tokio")]
mod main_runtime;
#[cfg(ruststream_alloc)]
mod metadata;
mod middleware;
#[cfg(feature = "poll-diagnostics")]
pub(crate) mod poll_diagnostics;
mod publish;
mod publish_failure;
mod publishing;
// The redelivery path serves the local contract, whose futures carry no Send.
#[allow(clippy::future_not_send)]
mod redelivery;
#[cfg(any(not(ruststream_alloc), test))]
#[cfg_attr(not(ruststream_alloc), allow(dead_code))]
// the delivery context takes it up once the body builds without `alloc`
mod registrations;
mod retry;
mod router;
#[cfg(feature = "asyncapi")]
mod schema_doc;
#[cfg(feature = "asyncapi")]
#[doc(hidden)]
pub use schema_doc::{join_headers, schema_json};
mod settings;
mod shared;
mod slot;
mod subscriber_def;
mod typed;

/// The subscriber a source opens, for broker `B` (the source resolves against the broker's
/// connected form). Tames the long projection in bounds.
pub(crate) type SourceSubscriber<B, S> = <S as SubscriptionSource<Connected<B>>>::Subscriber;

/// The message that subscriber yields. See [`SourceSubscriber`].
pub(crate) type SourceMessage<B, S> = <SourceSubscriber<B, S> as Subscriber>::Message;

/// The message that subscriber yields on a broker of either family. See [`SourceSubscriber`].
pub(crate) type LocalSourceMessage<B, S> = <SourceSubscriber<B, S> as Subscriber>::Message;

/// The batch that subscriber yields, where it batches. See [`SourceSubscriber`].
#[cfg(ruststream_alloc)]
pub(crate) type SourceBatch<B, S> = <SourceSubscriber<B, S> as BatchSubscriber>::Batch;

// The definition-trait dispatch SPI the retired legacy emission used to implement in user
// crates is internal machinery now: the modules stay, but only what the crate's own mounts
// reach through this path is re-exported.
#[cfg(ruststream_alloc)]
pub use app_info::AppInfo;
#[doc(hidden)]
pub use batch::{
    BATCH_CAPACITY, BatchHandler, BatchOutcomes, BatchVec, TakenBatch, batch_verdict, uniform_batch,
};
#[cfg(not(ruststream_alloc))]
pub use batch::{BatchSize, FitsBatch, Outcomes};
pub use context::{After, AfterError, AfterThen, Context};
#[cfg(feature = "compio")]
pub use core_ctx::{Core, CoreGuard};
#[cfg(ruststream_alloc)]
pub use described::Described;
#[cfg(feature = "std")]
#[doc(hidden)]
pub use dispatch::rings::{Feed, Lane, RING, lane_of, open as open_rings};
#[doc(hidden)]
pub use dispatch::{
    Delivery, EncodeRoom, Gated, HeadersAtOnce, HeadersOnDemand, LendHeaders, Lent, Parks,
    Slot as DeliverySlot, Turn, dispatch, idle, placed, run_batch, run_sequential,
};
pub use dispatch::{Placement, RETRY_COUNT_HEADER, Workers};
#[cfg(ruststream_alloc)]
pub use dynstack::{DynMiddleware, DynStack, DynStackHandler, Next};
pub use extract::{Ctx, CtxKey, FromContext, FromRef, Headers, State};
#[doc(hidden)]
pub use failure::DispatchFailure;
pub use failure::{FailurePolicies, FailurePolicy};
#[doc(hidden)]
pub use handle::{
    AnyOutPos, MaybeSendSink, SelectEntry, SlotAdmit, SlotEntry, SlotPublish, SlotTransactions,
};
#[doc(hidden)]
pub use handle::{AsBodyFn, BodyCall, BodyTag, DeclareBody, FnBody, FnDecl};
pub use handle::{AsDeclared, IntoDeclared};
#[doc(hidden)]
pub use handle::{
    Axis, AxisDocs, Batch, BatchCarried, BatchPair, Batched, BatchedAxis, ConstructError,
    DeclaredDest, DecodeOutcome, DocState, Docs, HandleValue, IsDocumented, NamedDest, OneByOne,
    Probed, ProbedDeclaredReplyDef, ProbedDocs, ProbedReplyDef, ReplyValue, ResolvedDest, Sealed,
    Solo, SoloAxis, SoloCarried, SoloPair, VerdictFamily, probed_declared_reply_def, probed_def,
    probed_reply_def,
};
pub use handle::{
    BatchDeserialized, BatchSet, Deserialized, Documentable, Documented, EncodedReply, Handle,
    Input, IntoSource, Message, OutEntry, Outs, ReplyShape, Serialized, SerializedReply, Slot,
    SoloDeserialized, SoloSet, Undocumented, ValueBuilder, Verdict, subscriber,
};
#[doc(hidden)]
pub use handle::{
    DeclaresReply, EntryMarkers, OutPos, RedirectableReply, ReplyDest, ReplyDestination,
    ReplyFormFor, ReplyHeadersSchema, ReplyRoute, ResolveDestination, SelectSlot, SlotDictionary,
    UnbuiltDefinition, WireDocs, declared_reply_destination, reply_destination,
};
#[doc(hidden)]
pub use handler::IntoOutcome;
pub use handler::{ContinuationFuture, Handler, HandlerOutcome};
#[cfg(ruststream_alloc)]
pub use health_state::{HealthState, SubscriptionCount};
#[cfg(ruststream_alloc)]
#[doc(hidden)]
pub use host::Copying;
#[doc(hidden)]
pub use host::SendHost;
pub use host::{Clock, ShutdownWait};
#[doc(hidden)]
pub use host::{Detach, Halt, HostDetach};
#[doc(hidden)]
pub use host::{Keeping, KeptCopies, KeptDelivery, NoKept, PrepareKept};
#[cfg(not(ruststream_alloc))]
#[doc(hidden)]
pub use inline::{CONTINUATION_ROOM, Continuation, InlineFuture, Room};
#[cfg(feature = "tokio")]
pub use main_runtime::MainRuntime;
#[cfg(not(ruststream_alloc))]
pub use publish_failure::{BatchFan, FailureSlot, Gathers, NoFan};
#[doc(hidden)]
pub use publish_failure::{ErrorHook, Reaction, Refusal, publish_done, publish_lost};
pub use publish_failure::{
    Failed, FailedMessage, Failures, MissingPayloadError, NoFreeRecordError,
};
#[cfg(any(not(ruststream_alloc), test))]
pub use registrations::RegistrationsFull;
#[doc(hidden)]
pub use slot::BindSlotsBody;
// The status half of `HandlerOutcome`, for the feature modules outside `runtime` that read it.
#[cfg(ruststream_alloc)]
#[doc(hidden)]
pub use handler::Continuation;
// The bound a body that clones its entry into a continuation asks of it.
#[cfg(any(feature = "metrics", feature = "otel", feature = "testing"))]
#[doc(hidden)]
pub use handler::HandlerResult;
pub use in_messages::{InMessages, InVariant, MessageDeclaration, VariantOf};
#[doc(hidden)]
pub use in_messages::{
    NamedByFrame, VariantLane, every_variant_failed, frame_names_no_variant, header_missing,
    header_unknown, no_variant_chosen,
};
pub use inject::Out;
#[doc(hidden)]
pub use ruststream_core::{MaybeSend, MaybeSendSync, MaybeSync};
// Public and hidden: a hand-written low-level def (`SubscriberDef` / `BatchDef`) names its
// input kind, and the self-deserializing one is how such a def opts onto the byte transport.
#[cfg(feature = "std")]
#[doc(hidden)]
pub use crate::bindable::ConnectedSlot;
#[cfg(not(ruststream_alloc))]
pub(crate) use bare_meta::Meta;
#[doc(hidden)]
pub use input::{Provided, TypedLane};
#[doc(hidden)]
pub use lifecycle::BoxError;
#[cfg(feature = "std")]
#[doc(hidden)]
pub use lifecycle::BrokerCell;
#[cfg(all(ruststream_alloc, feature = "testing"))]
#[doc(hidden)]
pub use lifecycle::InProcessConnect;
#[cfg(ruststream_alloc)]
#[doc(hidden)]
pub use lifecycle::LocalBoxFuture;
#[cfg(ruststream_alloc)]
#[doc(hidden)]
pub use lifecycle::{BoxFuture, BrokerLifecycle, ConnectedLifecycle};
#[cfg(ruststream_alloc)]
pub(crate) use metadata::HandlerMetadata as Meta;
#[cfg(ruststream_alloc)]
pub use metadata::{
    HandlerMetadata, HeaderDiscriminator, IncomingMessageMetadata, OutgoingKind,
    OutgoingMessageMetadata,
};
#[cfg(ruststream_tracing)]
pub use middleware::layers;
pub use middleware::{BlanketLayer, HandlerExt, Identity, Layer, Stack};
#[cfg(feature = "poll-diagnostics")]
#[doc(hidden)]
pub use poll_diagnostics::{BUCKET_BOUNDS_NS, PollStats};
#[cfg(feature = "poll-diagnostics")]
pub use poll_diagnostics::{PollDiagnostics, PollReport};
// The reply wiring a mount site's chain builds, the live sinks it pairs into, and the step traits
// the chain resolves through: the chain names them for the user, so none of it is spelled in
// service code.
#[doc(hidden)]
pub use publish::EncodeOutcome;
pub use publish::SerializePayloadError;
#[doc(hidden)]
pub use publish::{
    AddBatchReplyTransform, AddReplyTransform, Admits, AnyDeclared, BatchRefusal, CodecSlotOpen,
    DestinationSettled, DestinationUse, Direct, Either, FitsOffer, FitsTaken, InTransaction,
    LowerOutTransforms, MapReplyPolicy, NameReplyCodec, NamedDestinationSend, NamesDestination,
    NamingOffered, NamingUntaken, NarrowToUse, PayloadError, PublishingDirectly, RawReplyWiring,
    ReplyPublisher, ReplyWiring, SendOnlyPolicy, SinkOutcome, SinkRefusal, SlotStackUse,
    SlotTransforms, Transactional, TransactionalReply, TypedPublisher, WireBytes, WirePayload,
};
pub use publish::{
    BatchPublishTransform, BatchPublishTransformStack, BatchTransformIdentity, BoundSegment,
    CallCodec, ContextKind, EncodedWire, ForBatch, ForReply, ForSlot, HeaderSource, HeadersUnset,
    MapHeaders, MessageBody, MessageWire, MissingSegment, Names, OutPipeline, Outgoing,
    OutgoingName, PipelinePublishError, PublishAt, PublishBuilder, PublishCodec, PublishContext,
    PublishError, PublishExt, PublishHeaders, PublishIdentity, PublishLayer, PublishNext,
    PublishPipeline, PublishSink, PublishStack, PublishTransform, PublishTransformIdentity,
    PublishTransformStack, Reads, ResolvedName, SatisfiesContract, SerializedWire, SlotContext,
    SuppliedName, TemplateAddress, TransactionPublishError, TransactionScope, TypedHeaders,
    TypedTransaction, UnnamedCodec, for_batch,
};
#[cfg(ruststream_alloc)]
pub use publish::{PublishDynLayer, PublishDynNext, PublishDynStack};
// The builder's entry point, for the surfaces outside `runtime` that offer one: the test harness
// injects through the same positions as a live publish.
#[cfg(feature = "std")]
pub use crate::bindable::{Bindable, Bound, BrokerRegistration};
#[doc(hidden)]
pub use inject::InjectHandler;
#[doc(hidden)]
pub use publish::message_of;
#[doc(hidden)]
pub use publishing::PublishingHandler;
#[doc(hidden)]
pub use redelivery::{
    AddressedCopy, AppliesDeclaration, BoundRetry, CopiesAddressed, CopyPathAddress,
    CopyPathPairing, CopyPublisher, DeclaredOn, KeepCopies, KeptAddress, KeptTransforms,
    LocalRetrySlot, NamedRetry, NoCopies, NoCopyPath, NoPairing, NoRetry, OpenedSubscription,
    PairCopies, PairSlot, PrepareCopy, PublishesCopiesHere, RetryCopies, RetryLeaf, RetryParts,
    RetryRoute, RetrySetup, RetrySlot, ScopeDelivery, SlotAddress, Undeclared, UnnamedRetry,
    open_mounted_subscriber,
};
pub use retry::Retry;
#[doc(hidden)]
pub use retry::{
    Absent, CapOpen, DeadLetterOpen, DeclareCap, DeclareDeadLetter, DeclareMount, Declaring,
    DestinationDeclared, DestinationLast, DestinationOpen, DestinationUndeclared, FixedDestination,
    OpenDestination, Present, Retried, RetryChain, RetryMount, RetryOffer, RetryOpen, RetryPos,
    RetryStackUse, RouteDeclaring, RoutePosition, SettlesDestination, StepOpen, StepTaken,
};
#[cfg(ruststream_alloc)]
pub use router::RouterHandlers;
#[doc(hidden)]
pub use router::{
    AttachDeclaration, AttachRetry, Balance, BatchPublishInjectMount, BatchPublishMount, BodyFits,
    ChainHead, DefaultReply, LayerFits, OpenLayered, OpenRoutes, OpenedBatchRoute, OpenedRoute,
    Plain, Poison, PoisonAt, PoisonMode, Poisoned, PoolFits, PublishInjectMount, PublishMount,
    RawReplyInjectMount, RawReplyMount, ReplyAttachment, RetryDestinationsDeclared, RouteMetadata,
    RouteSubscription, RouterBroker, RouterCommit, RouterMount, RouterPipe, RunRoutes, RunsBatches,
    SplitHead, TakesWorkers,
};
#[doc(hidden)]
pub use router::{
    BatchWidth, Dispatched, OneTaskBatchDispatch, OneTaskBatchRoute, OneTaskDispatch,
    OneTaskPlanned, OneTaskRetriedRoute, OneTaskRoute, OneTaskRouteSource, OpenBatchRoute,
    OpenedBatchDispatch, OpenedDispatch, ReplyRoom, SequentialBatchPlan, SequentialPlan,
    UnboundRetry,
};
pub use router::{
    IncludeDef, LocalPipeline, MapPublisher, MountChain, Router, RouterOut, RouterPublishing,
    RouterPublishingOut, RouterWith, forms,
};
// The multi-thread runner's mount plane: a sink that collects starters whose closures own `Arc`s.
#[cfg(ruststream_alloc)]
pub use router::RouterDef;
#[cfg(ruststream_alloc)]
#[doc(hidden)]
pub use router::{MountSink, Opened};
pub use settings::{
    AllOpen, BatchSized, BatchStep, Declared, FailureStep, Fixed, MapSourceStep, NameStep, Open,
    StartAtStep, SubscriberBuilder, SubscriberSettings, WorkersStep,
};
#[doc(hidden)]
pub use settings::{CapsBatches, CloneSource, DefinitionInputCodec, MountsWith, Sequential};
#[doc(hidden)]
pub use slot::{
    AdmitsAt, AdmitsSlotAt, AnyTransform, BatchTransformLast, BindAt, BindSlot, Both, CodecAt,
    CodecLast, DestinationOffer, InitSlots, IntoSlotSource, MapPolicyAt, MapPolicyLast,
    MissingSlot, MountOffer, NamedStep, NoOutBound, NoReply, OutAttachment, OutPosition,
    PositionOptions, ReplyLast, ReplyOffer, ReplyOpen, ReplyProbe, ReplyStep, SlotCodec,
    SlotPolicy, SlotPos, SlotProbe, TransactionalLast, TransformAt, TransformLast,
    TransformPosition, TransformProbe, WithSource,
};
pub use slot::{
    BindSlots, ContainsMessage, DefaultSlot, HasSlots, ListOffer, OutMessages, OutSlot,
    PublishedThrough, Reply, SlotPublisher, Unrestricted,
};
pub use typed::{Typed, typed};
