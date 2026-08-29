//! The plain, batch, and raw value definitions: a handler bound to a source, nothing else.

use std::borrow::Cow;
use std::fmt;
use std::marker::PhantomData;

use serde::de::DeserializeOwned;

use crate::Unnamed;
use crate::runtime::batch::{
    BatchDef, BatchWithHeadersDef, RawSliceHandler, SliceHandler, SliceHandlerWithHeaders,
};
use crate::runtime::handler::Handler;
use crate::runtime::input::{Decoded, RawBytes};
use crate::runtime::router::{IncludeDef, forms};
use crate::runtime::settings::{AllOpen, SubscriberBuilder};
use crate::runtime::subscriber_def::SubscriberDef;

use super::IntoSource;

/// The documentation a value definition carries: what the builder's opt-ins (`describe`,
/// `documented`, `documented_headers`, `message`) collected, read back by the definition
/// traits' metadata methods. Machinery; you never name it.
#[doc(hidden)]
#[derive(Debug, Default)]
pub struct Docs {
    pub(crate) description: Option<Cow<'static, str>>,
    pub(crate) schema: Option<fn() -> Option<String>>,
    /// The reply type's schema; captured only by the publishing forms' `documented`.
    pub(crate) reply_schema: Option<fn() -> Option<String>>,
    pub(crate) headers_schema: Option<fn() -> Option<String>>,
    pub(crate) message_name: Option<&'static str>,
    pub(crate) message_description: Option<&'static str>,
    /// The reply side of the generated send operation; captured only by the publishing forms'
    /// `reply_message` / `reply_headers` opt-ins.
    pub(crate) reply_message_name: Option<&'static str>,
    pub(crate) reply_message_description: Option<&'static str>,
    pub(crate) reply_headers_schema: Option<fn() -> Option<String>>,
}

impl Docs {
    pub(crate) const fn none() -> Self {
        Self {
            description: None,
            schema: None,
            reply_schema: None,
            headers_schema: None,
            message_name: None,
            message_description: None,
            reply_message_name: None,
            reply_message_description: None,
            reply_headers_schema: None,
        }
    }

    /// The reply entry of the generated send operations, carrying everything the reply opt-ins
    /// captured.
    pub(crate) fn reply_outgoing(
        &self,
        channel: String,
        message_type: &'static str,
    ) -> crate::runtime::metadata::OutgoingMessageMetadata {
        crate::runtime::metadata::OutgoingMessageMetadata::new(channel, message_type)
            .with_payload_schema(self.reply_schema())
            .with_message_name(self.reply_message_name)
            .with_message_description(self.reply_message_description)
            .with_headers_schema(self.reply_headers_schema.and_then(|capture| capture()))
    }

    pub(crate) fn description(&self) -> Option<&str> {
        self.description.as_deref()
    }

    pub(crate) fn schema(&self) -> Option<String> {
        self.schema.and_then(|capture| capture())
    }

    pub(crate) fn reply_schema(&self) -> Option<String> {
        self.reply_schema.and_then(|capture| capture())
    }

    pub(crate) fn headers_schema(&self) -> Option<String> {
        self.headers_schema.and_then(|capture| capture())
    }
}

/// A value definition carrying the documentation opt-ins: what the builder's `describe`,
/// `documented`, `documented_headers` and `message` methods write to. Machinery; implemented by
/// every value definition, never named by a user.
#[doc(hidden)]
pub trait DocumentedValue {
    /// The payload type the definition decodes (`[u8]` on a raw input).
    type Payload: ?Sized;

    /// The reply type the definition publishes (`()` on a form with no reply).
    type Reply;

    fn docs_mut(&mut self) -> &mut Docs;
}

/// Forwards the shared metadata reads from a value definition's stored [`Docs`].
macro_rules! docs_metadata {
    () => {
        fn description(&self) -> Option<&str> {
            self.docs.description()
        }

        fn input_schema(&self) -> Option<String> {
            self.docs.schema()
        }

        fn headers_schema(&self) -> Option<String> {
            self.docs.headers_schema()
        }

        fn message_name(&self) -> Option<&'static str> {
            self.docs.message_name
        }

        fn message_description(&self) -> Option<&'static str> {
            self.docs.message_description
        }
    };
}
pub(super) use docs_metadata;

impl<D: DocumentedValue, Src, State, DC> SubscriberBuilder<D, Src, State, DC> {
    /// Sets the handler's human description for the generated `AsyncAPI` document, the
    /// value-path counterpart of the attribute reading the handler's doc comment.
    #[must_use]
    pub fn describe(self, text: impl Into<Cow<'static, str>>) -> Self {
        self.map_def(|mut def| {
            def.docs_mut().description = Some(text.into());
            def
        })
    }

    /// Reports the payload schemas (the input's, and the reply's on a publishing form) in the
    /// generated `AsyncAPI` document.
    ///
    /// The attribute path captures schemas automatically by probing the concrete type; a
    /// generic constructor cannot probe, so the value path opts in where the bound is provable.
    #[cfg(feature = "asyncapi")]
    #[must_use]
    pub fn documented(self) -> Self
    where
        D::Payload: schemars::JsonSchema,
        D::Reply: schemars::JsonSchema,
    {
        self.map_def(|mut def| {
            let docs = def.docs_mut();
            docs.schema = Some(super::schema_json_of::<D::Payload>);
            docs.reply_schema = Some(super::schema_json_of::<D::Reply>);
            def
        })
    }

    /// Reports `Hdr` as this subscriber's typed header contract in the generated `AsyncAPI`
    /// document, the value-path counterpart of the schema the attribute lifts off a
    /// `Headers<Hdr>` parameter.
    #[cfg(feature = "asyncapi")]
    #[must_use]
    pub fn documented_headers<Hdr: schemars::JsonSchema>(self) -> Self {
        self.map_def(|mut def| {
            def.docs_mut().headers_schema = Some(super::schema_json_of::<Hdr>);
            def
        })
    }

    /// Reports the input type's [`Message`](crate::Message) name and description in the
    /// generated `AsyncAPI` document, the value-path counterpart of the attribute probing the
    /// impl.
    #[must_use]
    pub fn message(self) -> Self
    where
        D::Payload: crate::Message,
    {
        self.map_def(|mut def| {
            let docs = def.docs_mut();
            docs.message_name = Some(<D::Payload as crate::Message>::NAME);
            docs.message_description = <D::Payload as crate::Message>::DESCRIPTION;
            def
        })
    }

    /// Reports the reply type's [`Message`](crate::Message) name and description on the
    /// generated send operation. Available on the publishing forms, whose reply type carries
    /// the impl.
    #[must_use]
    pub fn reply_message(self) -> Self
    where
        D::Reply: crate::Message,
    {
        self.map_def(|mut def| {
            let docs = def.docs_mut();
            docs.reply_message_name = Some(<D::Reply as crate::Message>::NAME);
            docs.reply_message_description = <D::Reply as crate::Message>::DESCRIPTION;
            def
        })
    }

    /// Reports the reply type's declared header contract
    /// ([`MessageHeaders`](crate::MessageHeaders)) as the send operation's headers schema.
    /// Available on the publishing forms.
    #[cfg(feature = "asyncapi")]
    #[must_use]
    pub fn reply_headers(self) -> Self
    where
        D::Reply: crate::MessageHeaders,
        <D::Reply as crate::MessageHeaders>::Contract: crate::__private::ContractSchema,
    {
        self.map_def(|mut def| {
            def.docs_mut().reply_headers_schema = Some(
                <<D::Reply as crate::MessageHeaders>::Contract as crate::__private::ContractSchema>::schema_json,
            );
            def
        })
    }
}

/// A plain subscriber definition built from a value: what `subscriber(source, handler)`
/// returns, wrapped in the settings builder.
///
/// `C` is the broker's typed per-delivery context the handler reads (`()` unless its impl
/// names one). You rarely name this type: construct it with [`subscriber`] and mount it with
/// `include`.
pub struct SubscriberValue<T, H, C = ()> {
    pub(crate) handler: H,
    pub(crate) docs: Docs,
    pub(crate) _types: PhantomData<fn() -> (T, C)>,
}

impl<T, H, C> fmt::Debug for SubscriberValue<T, H, C> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SubscriberValue").finish_non_exhaustive()
    }
}

impl<T, H, C> IncludeDef for SubscriberValue<T, H, C> {
    type Form = forms::Subscribing;
}

impl<T, H, C> DocumentedValue for SubscriberValue<T, H, C> {
    type Payload = T;
    type Reply = ();

    fn docs_mut(&mut self) -> &mut Docs {
        &mut self.docs
    }
}

impl<T, H, C> SubscriberDef for SubscriberValue<T, H, C>
where
    T: Send + Sync + 'static,
{
    type Input = Decoded<T>;
    type Context = C;
    type Handler = H;
    // The stored value never builds a source: the settings builder wrapping it carries the real
    // one, and this placeholder is no `SubscriptionSource` at all, exactly like an unnamed
    // attribute definition's.
    type Source = Unnamed<crate::Name>;

    fn source(&self) -> Self::Source {
        Unnamed::new()
    }

    docs_metadata!();

    fn into_handler(self) -> H {
        self.handler
    }
}

/// Binds `handler` to the subscription `source` as a plain (single-delivery, decoded)
/// definition; mount it with `include`.
///
/// The message type (and the broker context, when the impl names one) comes from the handler's
/// [`Handler`] impl; decoding follows the mount surface's codec ladder, with
/// [`codec`](SubscriberBuilder::codec) as the per-definition override. Chain the declarative
/// settings ([`workers`](crate::runtime::SubscriberSettings::workers),
/// [`on_failure`](crate::runtime::SubscriberSettings::on_failure),
/// [`start_at`](crate::runtime::SubscriberSettings::start_at), ...) on the result.
///
/// The handler bound here is over the unit app state, which every state-generic handler (and
/// closure) satisfies; a handler implemented for one concrete state type takes
/// [`subscriber_in`] instead.
///
/// # Examples
///
/// ```
/// # #[cfg(all(feature = "memory", feature = "json"))]
/// # mod demo {
/// use std::future::{Future, ready};
///
/// use ruststream::memory::MemoryBroker;
/// use ruststream::prelude::*;
/// # #[derive(serde::Deserialize)]
/// # struct Order { id: u64 }
///
/// struct Handle;
///
/// impl Handler<Order> for Handle {
///     fn handle(&self, order: &Order, _ctx: &mut Context<'_>) -> impl Future<Output = Settle> + Send {
///         println!("got order {}", order.id);
///         ready(HandlerResult::ack().into())
///     }
/// }
///
/// fn app() -> RustStream {
///     RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
///         b.include(subscriber("orders", Handle).workers(nonzero!(4)));
///     })
/// }
/// # }
/// ```
#[must_use]
pub fn subscriber<Src, T, C, H>(
    source: Src,
    handler: H,
) -> SubscriberBuilder<SubscriberValue<T, H, C>, Src::Source, AllOpen>
where
    Src: IntoSource,
    T: DeserializeOwned + Send + Sync + 'static,
    H: Handler<T, C>,
{
    SubscriberBuilder::new(
        SubscriberValue {
            handler,
            docs: Docs::none(),
            _types: PhantomData,
        },
        source.into_source(),
    )
}

/// [`subscriber`] for a handler implemented for one concrete app state type: the state is read
/// off that impl (`impl Handler<Order, (), AppState> for ..`), and the mount checks it against
/// the app's.
///
/// The plain constructor anchors its bound on the unit state so state-generic handlers infer;
/// this one leaves the state to the impl, which a state-generic handler cannot pin - each
/// constructor serves its own shape.
#[must_use]
pub fn subscriber_in<Src, T, C, St, H>(
    source: Src,
    handler: H,
) -> SubscriberBuilder<SubscriberValue<T, H, C>, Src::Source, AllOpen>
where
    Src: IntoSource,
    T: DeserializeOwned + Send + Sync + 'static,
    H: Handler<T, C, St>,
{
    SubscriberBuilder::new(
        SubscriberValue {
            handler,
            docs: Docs::none(),
            _types: PhantomData,
        },
        source.into_source(),
    )
}

/// A batch subscriber definition built from a value: what `batch(source, handler)` returns.
pub struct BatchValue<T, H> {
    pub(crate) handler: H,
    pub(crate) docs: Docs,
    pub(crate) _types: PhantomData<fn() -> T>,
}

impl<T, H> fmt::Debug for BatchValue<T, H> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BatchValue").finish_non_exhaustive()
    }
}

impl<T, H> IncludeDef for BatchValue<T, H> {
    type Form = forms::Batch;
}

impl<T, H> DocumentedValue for BatchValue<T, H> {
    type Payload = T;
    type Reply = ();

    fn docs_mut(&mut self) -> &mut Docs {
        &mut self.docs
    }
}

impl<T, H> BatchDef for BatchValue<T, H>
where
    T: Send + Sync + 'static,
{
    type Input = Decoded<T>;
    type Handler = H;
    type Source = Unnamed<crate::Name>;

    fn source(&self) -> Self::Source {
        Unnamed::new()
    }

    docs_metadata!();

    fn into_handler(self) -> H {
        self.handler
    }
}

/// Binds `handler` to the batch subscription `source`: the handler settles whole pages
/// (`&[T]`).
///
/// The source's subscriber must batch - natively, or through
/// [`buffered`](crate::runtime::SubscriberSettings::buffered). Mount it with `include`. A
/// handler implemented for one concrete app state type takes [`batch_in`] instead.
///
/// # Examples
///
/// ```
/// # #[cfg(all(feature = "memory", feature = "json"))]
/// # mod demo {
/// use std::future::{Future, ready};
/// use std::time::Duration;
///
/// use ruststream::memory::MemoryBroker;
/// use ruststream::prelude::*;
/// # #[derive(serde::Deserialize)]
/// # struct Order { id: u64 }
///
/// struct SettlePage;
///
/// impl SliceHandler<Order> for SettlePage {
///     fn handle_slice(
///         &self,
///         orders: &[Order],
///         _ctx: &mut Context<'_>,
///     ) -> impl Future<Output = BatchResult> + Send {
///         println!("settling {} orders", orders.len());
///         ready(BatchResult::Uniform(HandlerResult::ack()))
///     }
/// }
///
/// fn app() -> RustStream {
///     RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
///         b.include(batch("orders", SettlePage).buffered(nonzero!(128), Duration::from_millis(20)));
///     })
/// }
/// # }
/// ```
#[must_use]
pub fn batch<Src, T, H>(
    source: Src,
    handler: H,
) -> SubscriberBuilder<BatchValue<T, H>, Src::Source, AllOpen>
where
    Src: IntoSource,
    T: DeserializeOwned + Send + Sync + 'static,
    H: SliceHandler<T>,
{
    SubscriberBuilder::new(
        BatchValue {
            handler,
            docs: Docs::none(),
            _types: PhantomData,
        },
        source.into_source(),
    )
}

/// [`batch`] for a slice handler implemented for one concrete app state type. See
/// [`subscriber_in`] for the split.
#[must_use]
pub fn batch_in<Src, T, St, H>(
    source: Src,
    handler: H,
) -> SubscriberBuilder<BatchValue<T, H>, Src::Source, AllOpen>
where
    Src: IntoSource,
    T: DeserializeOwned + Send + Sync + 'static,
    H: SliceHandler<T, St>,
{
    SubscriberBuilder::new(
        BatchValue {
            handler,
            docs: Docs::none(),
            _types: PhantomData,
        },
        source.into_source(),
    )
}

/// A batch definition whose handler also reads a typed header contract per element: what
/// `batch_with_headers(source, handler)` returns.
pub struct BatchWithHeadersValue<T, Hdr, H> {
    pub(crate) handler: H,
    pub(crate) docs: Docs,
    pub(crate) _types: PhantomData<fn() -> (T, Hdr)>,
}

impl<T, Hdr, H> fmt::Debug for BatchWithHeadersValue<T, Hdr, H> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BatchWithHeadersValue")
            .finish_non_exhaustive()
    }
}

impl<T, Hdr, H> IncludeDef for BatchWithHeadersValue<T, Hdr, H> {
    type Form = forms::BatchWithHeaders;
}

impl<T, Hdr, H> DocumentedValue for BatchWithHeadersValue<T, Hdr, H> {
    type Payload = T;
    type Reply = ();

    fn docs_mut(&mut self) -> &mut Docs {
        &mut self.docs
    }
}

impl<T, Hdr, H> BatchDef for BatchWithHeadersValue<T, Hdr, H>
where
    T: Send + Sync + 'static,
    Hdr: DeserializeOwned + Send + Sync + 'static,
{
    type Input = Decoded<T>;
    type Handler = H;
    type Source = Unnamed<crate::Name>;

    fn source(&self) -> Self::Source {
        Unnamed::new()
    }

    docs_metadata!();

    fn into_handler(self) -> H {
        self.handler
    }
}

impl<T, Hdr, H> BatchWithHeadersDef for BatchWithHeadersValue<T, Hdr, H>
where
    T: Send + Sync + 'static,
    Hdr: DeserializeOwned + Send + Sync + 'static,
{
    type Headers = Hdr;
}

/// Binds a header-reading slice `handler` to the batch subscription `source`: each element
/// arrives with its parsed header contract.
///
/// The two slices are aligned by construction (an element whose payload or headers fail to
/// materialize is settled by the decode policy and never reaches the handler). The value-path
/// counterpart of a `batch(..)` attribute with a `Headers<Vec<H>>` parameter; chain
/// [`documented_headers`](SubscriberBuilder::documented_headers) to report the contract's
/// schema.
#[must_use]
pub fn batch_with_headers<Src, T, Hdr, H>(
    source: Src,
    handler: H,
) -> SubscriberBuilder<BatchWithHeadersValue<T, Hdr, H>, Src::Source, AllOpen>
where
    Src: IntoSource,
    T: DeserializeOwned + Send + Sync + 'static,
    Hdr: DeserializeOwned + Send + Sync + 'static,
    H: SliceHandlerWithHeaders<T, Hdr>,
{
    SubscriberBuilder::new(
        BatchWithHeadersValue {
            handler,
            docs: Docs::none(),
            _types: PhantomData,
        },
        source.into_source(),
    )
}

/// [`batch_with_headers`] for a handler implemented for one concrete app state type. See
/// [`subscriber_in`] for the split.
#[must_use]
pub fn batch_with_headers_in<Src, T, Hdr, St, H>(
    source: Src,
    handler: H,
) -> SubscriberBuilder<BatchWithHeadersValue<T, Hdr, H>, Src::Source, AllOpen>
where
    Src: IntoSource,
    T: DeserializeOwned + Send + Sync + 'static,
    Hdr: DeserializeOwned + Send + Sync + 'static,
    H: SliceHandlerWithHeaders<T, Hdr, St>,
{
    SubscriberBuilder::new(
        BatchWithHeadersValue {
            handler,
            docs: Docs::none(),
            _types: PhantomData,
        },
        source.into_source(),
    )
}

/// A raw subscriber definition built from a value: what `raw(source, handler)` returns.
///
/// No decode, no codec - the handler borrows the payload bytes as delivered. `C` is the
/// broker's typed per-delivery context (`()` unless the impl names one).
pub struct RawValue<H, C = ()> {
    pub(crate) handler: H,
    pub(crate) docs: Docs,
    pub(crate) _types: PhantomData<fn() -> C>,
}

impl<H, C> fmt::Debug for RawValue<H, C> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RawValue").finish_non_exhaustive()
    }
}

impl<H, C> IncludeDef for RawValue<H, C> {
    type Form = forms::RawSubscribing;
}

impl<H, C> DocumentedValue for RawValue<H, C> {
    type Payload = [u8];
    type Reply = ();

    fn docs_mut(&mut self) -> &mut Docs {
        &mut self.docs
    }
}

impl<H, C> SubscriberDef for RawValue<H, C> {
    type Input = RawBytes;
    type Context = C;
    type Handler = H;
    type Source = Unnamed<crate::Name>;

    fn source(&self) -> Self::Source {
        Unnamed::new()
    }

    docs_metadata!();

    fn into_handler(self) -> H {
        self.handler
    }
}

/// Binds a byte-level `handler` to the subscription `source`: the payload reaches it as
/// `&[u8]`, undecoded.
///
/// Mount it with `include`. A handler implemented for one concrete app state type takes
/// [`raw_in`] instead.
///
/// # Examples
///
/// ```
/// # #[cfg(feature = "memory")]
/// # mod demo {
/// use std::future::{Future, ready};
///
/// use ruststream::memory::MemoryBroker;
/// use ruststream::prelude::*;
///
/// struct Inspect;
///
/// impl Handler<[u8]> for Inspect {
///     fn handle(&self, payload: &[u8], _ctx: &mut Context<'_>) -> impl Future<Output = Settle> + Send {
///         println!("{} bytes", payload.len());
///         ready(HandlerResult::ack().into())
///     }
/// }
///
/// fn app() -> RustStream {
///     RustStream::new(AppInfo::new("frames", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
///         b.include(raw("frames", Inspect));
///     })
/// }
/// # }
/// ```
#[must_use]
pub fn raw<Src, C, H>(
    source: Src,
    handler: H,
) -> SubscriberBuilder<RawValue<H, C>, Src::Source, AllOpen>
where
    Src: IntoSource,
    H: Handler<[u8], C>,
{
    SubscriberBuilder::new(
        RawValue {
            handler,
            docs: Docs::none(),
            _types: PhantomData,
        },
        source.into_source(),
    )
}

/// [`raw`] for a byte-level handler implemented for one concrete app state type. See
/// [`subscriber_in`] for the split.
#[must_use]
pub fn raw_in<Src, C, St, H>(
    source: Src,
    handler: H,
) -> SubscriberBuilder<RawValue<H, C>, Src::Source, AllOpen>
where
    Src: IntoSource,
    H: Handler<[u8], C, St>,
{
    SubscriberBuilder::new(
        RawValue {
            handler,
            docs: Docs::none(),
            _types: PhantomData,
        },
        source.into_source(),
    )
}

/// A raw batch definition built from a value: what `raw_batch(source, handler)` returns. The
/// handler borrows a page of undecoded payloads (`&[&[u8]]`).
pub struct RawBatchValue<H> {
    pub(crate) handler: H,
    pub(crate) docs: Docs,
}

impl<H> fmt::Debug for RawBatchValue<H> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RawBatchValue").finish_non_exhaustive()
    }
}

impl<H> IncludeDef for RawBatchValue<H> {
    type Form = forms::RawBatch;
}

impl<H> DocumentedValue for RawBatchValue<H> {
    type Payload = [u8];
    type Reply = ();

    fn docs_mut(&mut self) -> &mut Docs {
        &mut self.docs
    }
}

impl<H> BatchDef for RawBatchValue<H> {
    type Input = RawBytes;
    type Handler = H;
    type Source = Unnamed<crate::Name>;

    fn source(&self) -> Self::Source {
        Unnamed::new()
    }

    docs_metadata!();

    fn into_handler(self) -> H {
        self.handler
    }
}

/// Binds a raw slice `handler` to the batch subscription `source`: a whole page of payloads,
/// no decode step anywhere.
///
/// Mount it with `include`. A handler implemented for one concrete app state type takes
/// [`raw_batch_in`] instead.
///
/// # Examples
///
/// ```
/// # #[cfg(feature = "memory")]
/// # mod demo {
/// use std::future::{Future, ready};
///
/// use ruststream::memory::MemoryBroker;
/// use ruststream::prelude::*;
///
/// struct Ingest;
///
/// impl RawSliceHandler for Ingest {
///     fn handle_slice(
///         &self,
///         frames: &[&[u8]],
///         _ctx: &mut Context<'_>,
///     ) -> impl Future<Output = BatchResult> + Send {
///         println!("ingesting {} frames", frames.len());
///         ready(BatchResult::Uniform(HandlerResult::ack()))
///     }
/// }
///
/// fn app() -> RustStream {
///     RustStream::new(AppInfo::new("frames", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
///         b.include(raw_batch("frames", Ingest));
///     })
/// }
/// # }
/// ```
#[must_use]
pub fn raw_batch<Src, H>(
    source: Src,
    handler: H,
) -> SubscriberBuilder<RawBatchValue<H>, Src::Source, AllOpen>
where
    Src: IntoSource,
    H: RawSliceHandler,
{
    SubscriberBuilder::new(
        RawBatchValue {
            handler,
            docs: Docs::none(),
        },
        source.into_source(),
    )
}

/// [`raw_batch`] for a raw slice handler implemented for one concrete app state type. See
/// [`subscriber_in`] for the split.
#[must_use]
pub fn raw_batch_in<Src, St, H>(
    source: Src,
    handler: H,
) -> SubscriberBuilder<RawBatchValue<H>, Src::Source, AllOpen>
where
    Src: IntoSource,
    H: RawSliceHandler<St>,
{
    SubscriberBuilder::new(
        RawBatchValue {
            handler,
            docs: Docs::none(),
        },
        source.into_source(),
    )
}
