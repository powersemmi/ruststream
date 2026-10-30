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

extern crate alloc;

// The contract, under the names the runtime's modules reach it by (`crate::Broker`,
// `crate::bindable`, ...).
#[allow(clippy::wildcard_imports)]
use ruststream_core::*;

pub mod codec;
pub mod testing;
#[cfg(test)]
mod testkit;

/// Re-exported for the [`register_testable_broker!`] macro's expansion; not a stable API.
#[cfg(feature = "testing")]
#[doc(hidden)]
pub use inventory;

mod batch;
mod batch_inject;
mod batch_publishing;
mod context;
mod dispatch;
mod dynstack;
mod extract;
mod failure;
mod handle;
mod handler;
mod host;
mod in_messages;
mod inject;
mod input;
mod lifecycle;
#[cfg(feature = "tokio")]
mod main_runtime;
mod metadata;
mod middleware;
#[cfg(feature = "poll-diagnostics")]
pub(crate) mod poll_diagnostics;
mod publish;
mod publishing;
mod redelivery;
mod retry;
mod router;
#[cfg(feature = "asyncapi")]
mod schema_doc;
#[cfg(feature = "asyncapi")]
#[doc(hidden)]
pub use schema_doc::{join_headers, schema_json};
mod settings;
mod slot;
mod subscriber_def;
mod typed;

/// The subscriber a source opens, for broker `B` (the source resolves against the broker's
/// connected form). Tames the long projection in bounds.
pub(crate) type SourceSubscriber<B, S> = <S as SubscriptionSource<Connected<B>>>::Subscriber;

/// The message that subscriber yields. See [`SourceSubscriber`].
pub(crate) type SourceMessage<B, S> = <SourceSubscriber<B, S> as Subscriber>::Message;

/// The batch that subscriber yields, where it batches. See [`SourceSubscriber`].
pub(crate) type SourceBatch<B, S> = <SourceSubscriber<B, S> as BatchSubscriber>::Batch;

// The definition-trait dispatch SPI the retired legacy emission used to implement in user
// crates is internal machinery now: the modules stay, but only what the crate's own mounts
// reach through this path is re-exported.
#[doc(hidden)]
pub use batch::{BatchHandler, TakenBatch, batch_verdict, uniform_batch};
pub use context::{After, Context};
#[doc(hidden)]
pub use dispatch::{
    Delivery, Lent, Slot as DeliverySlot, Turn, dispatch, placed, run_batch, run_sequential,
};
pub use dispatch::{Placement, RETRY_COUNT_HEADER, Workers};
pub use dynstack::{DynMiddleware, DynStack, DynStackHandler, Next};
pub use extract::{Ctx, CtxKey, FromContext, FromRef, Headers, State};
#[doc(hidden)]
pub use failure::DispatchFailure;
pub use failure::{FailurePolicies, FailurePolicy};
#[doc(hidden)]
pub use handle::{
    Axis, AxisDocs, Batch, BatchCarried, BatchPair, Batched, BatchedAxis, DeclaredDest,
    DecodeOutcome, DocState, Docs, HandleValue, IsDocumented, NamedDest, OneByOne, Probed,
    ProbedDeclaredReplyDef, ProbedDocs, ProbedReplyDef, ReplyValue, ResolvedDest, Sealed, Solo,
    SoloAxis, SoloCarried, SoloPair, VerdictFamily, probed_declared_reply_def, probed_def,
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
    ReplyFormFor, ReplyHeadersSchema, ReplyRoute, ResolveDestination, SelectSlot,
    UnbuiltDefinition, WireDocs, declared_reply_destination, reply_destination,
};
#[doc(hidden)]
pub use handler::IntoOutcome;
pub use handler::{Handler, HandlerOutcome};
pub use host::{Clock, ShutdownWait};
#[doc(hidden)]
pub use host::{Detach, Halt};
#[cfg(feature = "tokio")]
pub use main_runtime::MainRuntime;
// The status half of `HandlerOutcome`, for the feature modules outside `runtime` that read it.
#[cfg(any(feature = "metrics", feature = "otel", feature = "testing"))]
#[doc(hidden)]
pub use handler::HandlerResult;
pub use in_messages::{
    CodecDiscriminator, DecodeSet, InMessages, InVariant, MessageDeclaration, VariantOf,
};
#[doc(hidden)]
pub use in_messages::{
    VariantLane, every_variant_failed, header_missing, header_unknown, no_variant_chosen,
};
pub use inject::Out;
// Public and hidden: a hand-written low-level def (`SubscriberDef` / `BatchDef`) names its
// input kind, and the self-deserializing one is how such a def opts onto the byte transport.
#[doc(hidden)]
pub use crate::bindable::ConnectedSlot;
#[doc(hidden)]
pub use input::{Provided, TypedLane};
#[cfg(feature = "testing")]
#[doc(hidden)]
pub use lifecycle::InProcessConnect;
#[doc(hidden)]
pub use lifecycle::{BoxError, BoxFuture, BrokerCell, BrokerLifecycle, ConnectedLifecycle};
pub use metadata::{
    HandlerMetadata, HeaderDiscriminator, IncomingMessageMetadata, OutgoingKind,
    OutgoingMessageMetadata,
};
pub use middleware::{BlanketLayer, HandlerExt, Identity, Layer, Stack, layers};
#[cfg(feature = "poll-diagnostics")]
#[doc(hidden)]
pub use poll_diagnostics::{BUCKET_BOUNDS_NS, PollStats};
#[cfg(feature = "poll-diagnostics")]
pub use poll_diagnostics::{PollDiagnostics, PollReport};
// The reply wiring a mount site's chain builds, the live sinks it pairs into, and the step traits
// the chain resolves through: the chain names them for the user, so none of it is spelled in
// service code.
#[doc(hidden)]
pub use publish::{
    AddBatchReplyTransform, AddReplyTransform, Admits, AnyDeclared, CodecSlotOpen,
    DestinationSettled, DestinationUse, Direct, Either, EncodeOutcome, FitsOffer, FitsTaken,
    InTransaction, LowerOutTransforms, MapReplyPolicy, NameReplyCodec, NamedDestinationSend,
    NamesDestination, NamingOffered, NamingUntaken, NarrowToUse, PayloadError, PublishingDirectly,
    RawReplyWiring, ReplyPublisher, ReplyWiring, SendOnlyPolicy, SlotStackUse, SlotTransforms,
    Transactional, TransactionalReply, TypedPublisher, WireBytes, WirePayload,
};
pub use publish::{
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
// The builder's entry point, for the surfaces outside `runtime` that offer one: the test harness
// injects through the same positions as a live publish.
pub use crate::bindable::{Bindable, Bound, BrokerRegistration};
#[doc(hidden)]
pub use publish::message_of;
#[doc(hidden)]
pub use redelivery::{
    CopiesAddressed, CopyPathAddress, CopyPathPairing, PublishesCopiesHere, RetryPairing,
    RetrySetup, ScopeDelivery, open_mounted_subscriber,
};
pub use retry::Retry;
#[doc(hidden)]
pub use retry::{
    Absent, CapOpen, DeadLetterOpen, DeclareCap, DeclareDeadLetter, DeclareMount, Declaring,
    DestinationDeclared, DestinationLast, DestinationOpen, DestinationUndeclared, FixedDestination,
    OpenDestination, Present, Retried, RetryChain, RetryMount, RetryOffer, RetryOpen, RetryPos,
    RetryStackUse, RouteDeclaring, RoutePosition, SettlesDestination, StepOpen, StepTaken,
};
#[doc(hidden)]
pub use router::{
    AttachDeclaration, AttachRetry, BatchPublishInjectMount, BatchPublishMount, DefaultReply,
    MountSink, Opened, PublishInjectMount, PublishMount, RawReplyInjectMount, RawReplyMount,
    ReplyAttachment, RetryDestinationsDeclared, RouteMetadata, RouteSubscription, RouterBroker,
    RouterCommit, RouterMount,
};
pub use router::{
    IncludeDef, MapPublisher, Router, RouterDef, RouterHandlers, RouterOut, RouterPublishing,
    RouterPublishingOut, RouterWith, forms,
};
pub use settings::{
    AllOpen, BatchSized, BatchStep, Declared, FailureStep, Fixed, MapSourceStep, NameStep, Open,
    StartAtStep, SubscriberBuilder, SubscriberSettings, WorkersStep,
};
#[doc(hidden)]
pub use settings::{CapsBatches, DefinitionInputCodec, MountsWith};
#[doc(hidden)]
pub use slot::{
    AdmitsAt, AdmitsSlotAt, BatchTransformLast, BindAt, BindSlot, Both, CodecAt, CodecLast,
    DestinationOffer, InitSlots, IntoSlotSource, MapPolicyAt, MapPolicyLast, MissingSlot,
    MountOffer, NamedStep, NoOutBound, NoReply, OutAttachment, OutPosition, PositionOptions,
    ReplyLast, ReplyOffer, ReplyOpen, ReplyStep, SlotCodec, SlotPolicy, SlotPos, TransactionalLast,
    TransformAt, TransformLast, WithSource,
};
pub use slot::{
    BindSlots, ContainsMessage, DefaultSlot, HasSlots, ListOffer, OutMessages, OutSlot,
    PublishedThrough, Reply, SlotPublisher, Unrestricted,
};
pub use typed::{Typed, typed};
