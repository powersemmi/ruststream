#![doc = include_str!("README.md")]

// The surface of the runtime crate this module shows, item by item: a glob would also inline the
// machinery the runtime exports for the other crates of the workspace.
#[doc(hidden)]
pub use ruststream_runtime::ConnectedSlot;
#[doc(hidden)]
pub use ruststream_runtime::IntoOutcome;
pub use ruststream_runtime::MainRuntime;
pub use ruststream_runtime::Out;
pub use ruststream_runtime::Retry;
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
pub use ruststream_runtime::{After, Context};
pub use ruststream_runtime::{
    AllOpen, BatchSized, BatchStep, Declared, FailureStep, Fixed, MapSourceStep, NameStep, Open,
    StartAtStep, SubscriberBuilder, SubscriberSettings, WorkersStep,
};
pub use ruststream_runtime::{
    App, AppInfo, BrokerScope, HealthProbe, HealthState, Mounting, MountingSlots, RunningApp,
    RustStream, RustStreamError, Setup, SubscriptionCount, SubscriptionEnd, Wired,
};
#[doc(hidden)]
pub use ruststream_runtime::{
    AttachDeclaration, AttachRetry, BatchPublishInjectMount, BatchPublishMount, DefaultReply,
    PublishInjectMount, PublishMount, RawReplyInjectMount, RawReplyMount, ReplyAttachment,
    RetryDestinationsDeclared, RouteMetadata, RouteSubscription, RouterBroker, RouterCommit,
    RouterMount,
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
    BatchDeserialized, BatchSet, Deserialized, Documentable, Documented, EncodedReply, Handle,
    Input, IntoSource, Message, OutEntry, Outs, ReplyShape, Serialized, SerializedReply, Slot,
    SoloDeserialized, SoloSet, Undocumented, ValueBuilder, Verdict, subscriber,
};
pub use ruststream_runtime::{
    BatchPublishTransform, BatchPublishTransformStack, BatchTransformIdentity, BoundSegment,
    CallCodec, ContextKind, EncodedWire, ForBatch, ForReply, ForSlot, HeaderSource, HeadersUnset,
    MapHeaders, MessageBody, MessageWire, MissingSegment, Names, OutPipeline, Outgoing,
    OutgoingName, PipelinePublishError, PublishAt, PublishBuilder, PublishCodec, PublishContext,
    PublishDynLayer, PublishDynNext, PublishDynStack, PublishError, PublishExt, PublishHeaders,
    PublishIdentity, PublishLayer, PublishNext, PublishPipeline, PublishSink, PublishStack,
    PublishTransform, PublishTransformIdentity, PublishTransformStack, Reads, ResolvedName,
    SatisfiesContract, SerializePayloadError, SerializedWire, SlotContext, SuppliedName,
    TemplateAddress, TransactionPublishError, TransactionScope, TypedHeaders, TypedTransaction,
    UnnamedCodec, for_batch,
};
pub use ruststream_runtime::{
    BindSlots, ContainsMessage, DefaultSlot, HasSlots, ListOffer, OutMessages, OutSlot,
    PublishedThrough, Reply, SlotPublisher, Unrestricted,
};
pub use ruststream_runtime::{Bindable, Bound, BrokerRegistration};
pub use ruststream_runtime::{BlanketLayer, HandlerExt, Identity, Layer, Stack, layers};
#[doc(hidden)]
pub use ruststream_runtime::{CapsBatches, DefinitionInputCodec, MountsWith};
pub use ruststream_runtime::{
    CodecDiscriminator, DecodeSet, InMessages, InVariant, MessageDeclaration, VariantOf,
};
#[doc(hidden)]
pub use ruststream_runtime::{
    CopiesAddressed, CopyPathAddress, CopyPathPairing, PublishesCopiesHere, RetryPairing,
    RetrySetup,
};
pub use ruststream_runtime::{Ctx, CtxKey, FromContext, FromRef, Headers, State};
#[doc(hidden)]
pub use ruststream_runtime::{
    DeclaresReply, EntryMarkers, OutPos, RedirectableReply, ReplyDest, ReplyDestination,
    ReplyFormFor, ReplyHeadersSchema, ReplyRoute, ResolveDestination, SelectSlot,
    UnbuiltDefinition, WireDocs, declared_reply_destination, reply_destination,
};
pub use ruststream_runtime::{DynMiddleware, DynStack, DynStackHandler, Next};
pub use ruststream_runtime::{FailurePolicies, FailurePolicy};
pub use ruststream_runtime::{Handler, HandlerOutcome};
pub use ruststream_runtime::{
    HandlerMetadata, HeaderDiscriminator, IncomingMessageMetadata, OutgoingKind,
    OutgoingMessageMetadata,
};
pub use ruststream_runtime::{
    IncludeDef, MapPublisher, Router, RouterDef, RouterHandlers, RouterOut, RouterPublishing,
    RouterPublishingOut, RouterSink, RouterWith, forms,
};
#[doc(hidden)]
pub use ruststream_runtime::{IncludeMount, ScopeCommit};
pub use ruststream_runtime::{Placement, RETRY_COUNT_HEADER, Workers};
#[cfg(feature = "poll-diagnostics")]
pub use ruststream_runtime::{PollDiagnostics, PollReport};
#[doc(hidden)]
pub use ruststream_runtime::{Provided, TypedLane};
pub use ruststream_runtime::{Typed, typed};
#[doc(hidden)]
pub use ruststream_runtime::{
    VariantLane, every_variant_failed, header_missing, header_unknown, no_variant_chosen,
};
#[doc(hidden)]
pub use ruststream_runtime::{batch_verdict, uniform_batch};

pub mod cli;
