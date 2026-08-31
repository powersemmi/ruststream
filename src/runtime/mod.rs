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
pub use batch::{
    BatchDef, BatchResult, BatchWithHeadersDef, IntoBatchResult, RawBatch, RawSliceHandler,
    SliceHandler, SliceHandlerWithHeaders, TypedBatch, TypedBatchWithHeaders,
};
pub use batch_inject::{BatchInjectCall, BatchInjectDef, BatchInjectHandler};
pub use batch_publishing::{BatchPublishingCall, BatchPublishingDef, BatchPublishingHandler};
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
    HandleValue, IsDocumented, NamedDest, OneByOne, Page, PageBody, PageBytes, PagePair, Paged,
    PagedAxis, ReplyValue, Sealed, Solo, SoloAxis, SoloBody, SoloBytes, SoloPair, VerdictFamily,
    VerdictFor,
};
pub use handle::{
    Documentable, Documented, Handle, Input, IntoSource, IntoVerdict, Message, OutStack, Outs,
    Payload, Publish, SeekContext, Slot, Undocumented, ValueBuilder, subscriber,
};
#[doc(hidden)]
pub use handle::{
    EntryMarkers, OutPos, ReplyDest, ReplyFormFor, ReplyHeadersSchema, ReplyShape,
    ReplySlotFormFor, SealedBatchPublishing, SealedBatchPublishingOut, SealedPublishing,
    SealedPublishingOut, SealedRawReply, SealedRawReplyOut, SelectSlot, SplitAttach,
    UnbuiltDefinition,
};
pub use handler::{Handler, HandlerResult, IntoSettle, Settle};
pub use inject::{FromStartup, InjectCall, InjectDef, InjectHandler, Out, Seek};
pub use input::{DecodeWith, Decoded, InputKind, RawBytes};
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
pub use publishing::{EncodeReply, PublishingCall, PublishingDef, PublishingHandler, ReplySink};
pub use router::{
    IncludeDef, Router, RouterBatchOut, RouterBatchPublishing, RouterBatchPublishingOut, RouterDef,
    RouterHandlers, RouterOut, RouterPublishing, RouterPublishingOut, RouterRawReply,
    RouterRawReplyOut, RouterSink, RouterSlots, RouterSlotsWithReply, RouterWith, forms,
};
#[doc(hidden)]
pub use router::{RouterCommit, RouterMount, RouterSlotCommit};
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
    SlotPublisher, TypedSlot, Unrestricted,
};
pub use subscriber_def::SubscriberDef;
pub use typed::{Typed, typed};
