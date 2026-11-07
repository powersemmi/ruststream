#![doc = include_str!("README.md")]

// The surface of the runtime crate this module shows, item by item: a glob would also inline the
// machinery the runtime exports for the other crates of the workspace.
// The broker wrapper keeps its slot in a `std` mutex, so it stands where the runtime has the
// standard library: with the tokio runner.
#[cfg(feature = "tokio")]
#[doc(hidden)]
pub use ruststream_runtime::ConnectedSlot;
#[doc(hidden)]
pub use ruststream_runtime::IntoOutcome;
pub use ruststream_runtime::Out;
#[cfg(not(ruststream_alloc))]
pub use ruststream_runtime::Outcomes;
pub use ruststream_runtime::Retry;
#[cfg(ruststream_tracing)]
pub use ruststream_runtime::layers;
#[doc(hidden)]
pub use ruststream_runtime::{
    Absent, CapOpen, DeadLetterOpen, DeclareCap, DeclareDeadLetter, DeclareMount, Declaring,
    DestinationDeclared, DestinationLast, DestinationOpen, DestinationUndeclared, FixedDestination,
    OpenDestination, Present, Retried, RetryChain, RetryMount, RetryOffer, RetryOpen, RetryPos,
    RetryStackUse, RouteDeclaring, RoutePosition, SettlesDestination, StepOpen, StepTaken,
};
#[doc(hidden)]
pub use ruststream_runtime::{
    AddBatchReplyTransform, AddReplyTransform, Admits, AnyDeclared, CodecSlotOpen,
    DestinationSettled, DestinationUse, Direct, Either, EncodeOutcome, FitsOffer, FitsTaken,
    InTransaction, LowerOutTransforms, MapReplyPolicy, NameReplyCodec, NamedDestinationSend,
    NamesDestination, NamingOffered, NamingUntaken, NarrowToUse, PayloadError, PublishingDirectly,
    RawReplyWiring, ReplyPublisher, ReplyWiring, SendOnlyPolicy, SlotStackUse, SlotTransforms,
    Transactional, TransactionalReply, TypedPublisher, WireBytes, WirePayload,
};
#[doc(hidden)]
pub use ruststream_runtime::{
    AdmitsAt, AdmitsSlotAt, BatchTransformLast, BindAt, BindSlot, Both, CodecAt, CodecLast,
    DestinationOffer, InitSlots, IntoSlotSource, MapPolicyAt, MapPolicyLast, MissingSlot,
    MountOffer, NamedStep, NoOutBound, NoReply, OutAttachment, OutPosition, PositionOptions,
    ReplyLast, ReplyOffer, ReplyOpen, ReplyStep, SlotCodec, SlotPolicy, SlotPos, TransactionalLast,
    TransformAt, TransformLast, WithSource,
};
pub use ruststream_runtime::{After, AfterError, AfterThen, Context};
pub use ruststream_runtime::{
    AllOpen, BatchSized, BatchStep, Declared, FailureStep, Fixed, MapSourceStep, NameStep, Open,
    StartAtStep, SubscriberBuilder, SubscriberSettings, WorkersStep,
};
#[doc(hidden)]
pub use ruststream_runtime::{AnyContext, ContextOf, FoldedContext, JoinSlot};
#[doc(hidden)]
pub use ruststream_runtime::{
    AnyOutPos, BindSlotsBody, MaybeSendSink, MaybeSendSync, SelectEntry, SlotEntry, SlotPublish,
    SlotTransactions,
};
#[doc(hidden)]
pub use ruststream_runtime::{AsBodyFn, BodyCall, BodyTag, DeclareBody, FnBody, FnDecl};
pub use ruststream_runtime::{AsDeclared, IntoDeclared};
#[doc(hidden)]
pub use ruststream_runtime::{
    AttachDeclaration, AttachRetry, BatchPublishInjectMount, BatchPublishMount, ChainHead,
    DefaultReply, PublishInjectMount, PublishMount, RawReplyInjectMount, RawReplyMount,
    ReplyAttachment, RetryDestinationsDeclared, RouteMetadata, RouteSubscription, RouterBroker,
    RouterCommit, RouterMount, SplitHead,
};
#[doc(hidden)]
pub use ruststream_runtime::{
    Axis, AxisDocs, Batch, BatchCarried, BatchPair, Batched, BatchedAxis, DeclaredDest,
    DecodeOutcome, DocState, Docs, HandleValue, IsDocumented, NamedDest, OneByOne, Probed,
    ProbedDeclaredReplyDef, ProbedDocs, ProbedReplyDef, ReplyValue, ResolvedDest, Sealed, Solo,
    SoloAxis, SoloCarried, SoloPair, VerdictFamily, probed_declared_reply_def, probed_def,
    probed_reply_def,
};
pub use ruststream_runtime::{
    BatchDeserialized, BatchSet, ConstructError, Deserialized, Documentable, Documented,
    EncodedReply, Handle, Input, IntoSource, Message, OutEntry, Outs, ReplyShape, Serialized,
    SerializedReply, Slot, SoloDeserialized, SoloSet, Undocumented, ValueBuilder, Verdict,
    subscriber,
};
pub use ruststream_runtime::{
    BatchPublishTransform, BatchPublishTransformStack, BatchTransformIdentity, BoundSegment,
    CallCodec, ContextKind, EncodedWire, ForBatch, ForReply, ForSlot, HeaderSource, HeadersUnset,
    MapHeaders, MessageBody, MessageWire, MissingSegment, Names, OutPipeline, Outgoing,
    OutgoingName, PipelinePublishError, PublishAt, PublishBuilder, PublishCodec, PublishContext,
    PublishError, PublishExt, PublishHeaders, PublishIdentity, PublishLayer, PublishNext,
    PublishPipeline, PublishSink, PublishStack, PublishTransform, PublishTransformIdentity,
    PublishTransformStack, Reads, ResolvedName, SatisfiesContract, SerializePayloadError,
    SerializedWire, SlotContext, SuppliedName, TemplateAddress, TransactionPublishError,
    TransactionScope, TypedHeaders, TypedTransaction, UnnamedCodec, WithPlaces, for_batch,
};
pub use ruststream_runtime::{
    BindSlots, ContainsMessage, DefaultSlot, HasSlots, ListOffer, OutMessages, OutSlot,
    PublishedThrough, Reply, SlotPublisher, Unrestricted,
};
#[cfg(feature = "tokio")]
pub use ruststream_runtime::{Bindable, Bound, BrokerRegistration};
pub use ruststream_runtime::{BlanketLayer, HandlerExt, Identity, Layer, Stack};
#[doc(hidden)]
pub use ruststream_runtime::{
    BoundRetry, CopiesAddressed, CopyPathAddress, CopyPathPairing, PublishesCopiesHere,
    RetryCopies, RetrySetup, RetrySlot,
};
#[doc(hidden)]
pub use ruststream_runtime::{CapsBatches, DefinitionInputCodec, MountsWith};
pub use ruststream_runtime::{Clock, ShutdownWait};
pub use ruststream_runtime::{Ctx, CtxKey, FromContext, FromRef, Headers, State};
#[doc(hidden)]
pub use ruststream_runtime::{
    DeclaresReply, EntryMarkers, OutPos, RedirectableReply, ReplyDest, ReplyDestination,
    ReplyFormFor, ReplyHeadersSchema, ReplyRoute, ResolveDestination, SelectSlot,
    UnbuiltDefinition, WireDocs, declared_reply_destination, reply_destination,
};
#[cfg(ruststream_alloc)]
pub use ruststream_runtime::{DynMiddleware, DynStack, DynStackHandler, Next};
pub use ruststream_runtime::{
    Failed, FailedMessage, Failures, MissingPayloadError, NoFreeRecordError,
};
pub use ruststream_runtime::{FailurePolicies, FailurePolicy};
pub use ruststream_runtime::{Handler, HandlerOutcome};
#[cfg(ruststream_alloc)]
pub use ruststream_runtime::{
    HandlerMetadata, HeaderDiscriminator, IncomingMessageMetadata, OutgoingKind,
    OutgoingMessageMetadata,
};
pub use ruststream_runtime::{InMessages, InVariant, MessageDeclaration, VariantOf};
pub use ruststream_runtime::{
    IncludeDef, LocalPipeline, MapPublisher, MountChain, Router, RouterOut, RouterPublishing,
    RouterPublishingOut, RouterWith, forms,
};
#[doc(hidden)]
pub use ruststream_runtime::{KeepsPlaces, KeptPlaces};
#[doc(hidden)]
pub use ruststream_runtime::{
    NamedByFrame, VariantLane, every_variant_failed, frame_names_no_variant, header_missing,
    header_unknown, no_variant_chosen,
};
pub use ruststream_runtime::{Placement, RETRY_COUNT_HEADER, Workers};
#[cfg(feature = "poll-diagnostics")]
pub use ruststream_runtime::{PollDiagnostics, PollReport};
#[doc(hidden)]
pub use ruststream_runtime::{Provided, TypedLane};
#[cfg(ruststream_alloc)]
pub use ruststream_runtime::{PublishDynLayer, PublishDynNext, PublishDynStack};
#[cfg(ruststream_alloc)]
pub use ruststream_runtime::{RouterDef, RouterHandlers};
pub use ruststream_runtime::{Typed, typed};
#[doc(hidden)]
pub use ruststream_runtime::{batch_verdict, uniform_batch};

// The runner the `tokio` feature selects: the app object, its lifecycle and the entry point.
#[cfg(feature = "tokio")]
pub use ruststream_runtime_tokio::Tokio;
#[cfg(feature = "tokio")]
pub use ruststream_runtime_tokio::{
    App, AppInfo, BrokerScope, HealthProbe, HealthState, RunningApp, RustStream, RustStreamError,
    Setup, SubscriptionCount, SubscriptionEnd, Wired,
};
#[cfg(feature = "tokio")]
pub use ruststream_runtime_tokio::{MainRuntime, RouterSink};
#[cfg(feature = "tokio")]
#[doc(hidden)]
pub use ruststream_runtime_tokio::{
    MountRoutes, ScopeRegistrations, ScopedHandle, ScopedRouter, ScopedStartup,
};

#[cfg(feature = "tokio")]
pub mod cli;

#[cfg(feature = "compio")]
pub mod compio;

// The embedded runner's cross-broker tokens, beside the multi-thread runner's `Bindable` and
// `Bound`: a publish policy bound to another broker of the same app.
#[cfg(all(feature = "embassy", ruststream_alloc))]
pub use ruststream_runtime_embassy::{Bindable as EmbeddedBindable, Bound as EmbeddedBound};

// The runner the `embassy` feature selects: every item of the embedded runner a service names is
// re-exported here, at the path its tokio counterpart takes under `tokio`.
#[cfg(feature = "embassy")]
#[doc(hidden)]
pub use ruststream_runtime_embassy::{
    BrokerMount, CloseAll, EmbeddedPlan, HookList, Hooks, Keyed, NoHooks, OneWorker, Pool, Room,
    SoFar, StartMounts, WorkerSet,
};
#[cfg(feature = "embassy")]
pub use ruststream_runtime_embassy::{
    Control, EmbassyClock, EmbeddedApp, Health, MountSteps, Mounts, Parts, RunError, Scope,
    Stopping,
};
