//! The plain, batch, and raw value definitions: a handler bound to a source, nothing else.

use std::borrow::Cow;
use std::fmt;
use std::marker::PhantomData;

use serde::de::DeserializeOwned;

use crate::Unnamed;
use crate::runtime::batch::{BatchDef, RawSliceHandler, SliceHandler};
use crate::runtime::handler::Handler;
use crate::runtime::input::{Decoded, RawBytes};
use crate::runtime::router::{IncludeDef, forms};
use crate::runtime::settings::{AllOpen, SubscriberBuilder};
use crate::runtime::subscriber_def::SubscriberDef;

use super::IntoSource;

/// The documentation a value definition carries: what `describe` and `documented` collected,
/// read back by the definition traits' metadata methods.
pub(crate) struct Docs {
    pub(crate) description: Option<Cow<'static, str>>,
    pub(crate) schema: Option<fn() -> Option<String>>,
    /// The reply type's schema; captured only by the publishing form's `documented`.
    pub(crate) reply_schema: Option<fn() -> Option<String>>,
}

impl Docs {
    pub(crate) const fn none() -> Self {
        Self {
            description: None,
            schema: None,
            reply_schema: None,
        }
    }

    pub(crate) fn description(&self) -> Option<&str> {
        self.description.as_deref()
    }

    pub(crate) fn schema(&self) -> Option<String> {
        self.schema.and_then(|capture| capture())
    }
}

/// Implements the `describe` opt-in on the builder over one value definition type.
macro_rules! impl_describe {
    ($value:ident $(< $($param:ident),+ >)?) => {
        impl<$($($param,)+)? Src, State> SubscriberBuilder<$value$(<$($param),+>)?, Src, State> {
            /// Sets the handler's human description for the generated `AsyncAPI` document, the
            /// value-path counterpart of the attribute reading the handler's doc comment.
            #[must_use]
            pub fn describe(self, text: impl Into<Cow<'static, str>>) -> Self {
                self.map_def(|mut def| {
                    def.docs.description = Some(text.into());
                    def
                })
            }
        }
    };
}

/// A plain subscriber definition built from a value: what `subscriber(source, handler)` returns,
/// wrapped in the settings builder.
///
/// You rarely name this type: construct it with [`subscriber`] and mount it with `include`.
pub struct SubscriberValue<T, H> {
    pub(crate) handler: H,
    pub(crate) docs: Docs,
    pub(crate) _input: PhantomData<fn() -> T>,
}

impl<T, H> fmt::Debug for SubscriberValue<T, H> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SubscriberValue").finish_non_exhaustive()
    }
}

impl<T, H> IncludeDef for SubscriberValue<T, H> {
    type Form = forms::Subscribing;
}

impl<T, H> SubscriberDef for SubscriberValue<T, H>
where
    T: Send + Sync + 'static,
{
    type Input = Decoded<T>;
    type Context = ();
    type Handler = H;
    // The stored value never builds a source: the settings builder wrapping it carries the real
    // one, and this placeholder is no `SubscriptionSource` at all, exactly like an unnamed
    // attribute definition's.
    type Source = Unnamed<crate::Name>;

    fn source(&self) -> Self::Source {
        Unnamed::new()
    }

    fn description(&self) -> Option<&str> {
        self.docs.description()
    }

    fn input_schema(&self) -> Option<String> {
        self.docs.schema()
    }

    fn into_handler(self) -> H {
        self.handler
    }
}

impl_describe!(SubscriberValue<T, H>);

#[cfg(feature = "asyncapi")]
impl<T, H, Src, State> SubscriberBuilder<SubscriberValue<T, H>, Src, State> {
    /// Reports the input type's JSON Schema in the generated `AsyncAPI` document.
    ///
    /// The attribute path captures schemas automatically by probing the concrete type; a generic
    /// constructor cannot probe, so the value path opts in where the bound is provable.
    #[must_use]
    pub fn documented(self) -> Self
    where
        T: schemars::JsonSchema,
    {
        self.map_def(|mut def| {
            def.docs.schema = Some(super::schema_json_of::<T>);
            def
        })
    }
}

/// Binds `handler` to the subscription `source` as a plain (single-delivery, decoded)
/// definition; mount it with `include`.
///
/// The message type comes from the handler's [`Handler<T>`] impl; decoding follows the mount
/// surface's codec ladder. Chain the declarative settings
/// ([`workers`](crate::runtime::SubscriberSettings::workers),
/// [`on_failure`](crate::runtime::SubscriberSettings::on_failure),
/// [`start_at`](crate::runtime::SubscriberSettings::start_at), ...) on the result.
///
/// The handler bound here is over the unit app state, which every state-generic handler (and
/// closure) satisfies; a handler implemented for one concrete state type keeps the explicit
/// route of implementing [`SubscriberDef`] itself.
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
pub fn subscriber<Src, T, H>(
    source: Src,
    handler: H,
) -> SubscriberBuilder<SubscriberValue<T, H>, Src::Source, AllOpen>
where
    Src: IntoSource,
    T: DeserializeOwned + Send + Sync + 'static,
    H: Handler<T>,
{
    SubscriberBuilder::new(
        SubscriberValue {
            handler,
            docs: Docs::none(),
            _input: PhantomData,
        },
        source.into_source(),
    )
}

/// A batch subscriber definition built from a value: what `batch(source, handler)` returns.
pub struct BatchValue<T, H> {
    pub(crate) handler: H,
    pub(crate) docs: Docs,
    pub(crate) _input: PhantomData<fn() -> T>,
}

impl<T, H> fmt::Debug for BatchValue<T, H> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BatchValue").finish_non_exhaustive()
    }
}

impl<T, H> IncludeDef for BatchValue<T, H> {
    type Form = forms::Batch;
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

    fn description(&self) -> Option<&str> {
        self.docs.description()
    }

    fn input_schema(&self) -> Option<String> {
        self.docs.schema()
    }

    fn into_handler(self) -> H {
        self.handler
    }
}

impl_describe!(BatchValue<T, H>);

#[cfg(feature = "asyncapi")]
impl<T, H, Src, State> SubscriberBuilder<BatchValue<T, H>, Src, State> {
    /// Reports the element type's JSON Schema in the generated `AsyncAPI` document. See
    /// [`documented`](SubscriberBuilder::documented) on the plain form.
    #[must_use]
    pub fn documented(self) -> Self
    where
        T: schemars::JsonSchema,
    {
        self.map_def(|mut def| {
            def.docs.schema = Some(super::schema_json_of::<T>);
            def
        })
    }
}

/// Binds `handler` to the batch subscription `source`: the handler settles whole pages
/// (`&[T]`).
///
/// The source's subscriber must batch - natively, or through
/// [`buffered`](crate::runtime::SubscriberSettings::buffered). Mount it with `include`.
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
            _input: PhantomData,
        },
        source.into_source(),
    )
}

/// A raw subscriber definition built from a value: what `raw(source, handler)` returns. No
/// decode, no codec - the handler borrows the payload bytes as delivered.
pub struct RawValue<H> {
    pub(crate) handler: H,
    pub(crate) docs: Docs,
}

impl<H> fmt::Debug for RawValue<H> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RawValue").finish_non_exhaustive()
    }
}

impl<H> IncludeDef for RawValue<H> {
    type Form = forms::RawSubscribing;
}

impl<H> SubscriberDef for RawValue<H> {
    type Input = RawBytes;
    type Context = ();
    type Handler = H;
    type Source = Unnamed<crate::Name>;

    fn source(&self) -> Self::Source {
        Unnamed::new()
    }

    fn description(&self) -> Option<&str> {
        self.docs.description()
    }

    fn into_handler(self) -> H {
        self.handler
    }
}

impl_describe!(RawValue<H>);

/// Binds a byte-level `handler` to the subscription `source`: the payload reaches it as
/// `&[u8]`, undecoded. Mount it with `include`.
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
pub fn raw<Src, H>(source: Src, handler: H) -> SubscriberBuilder<RawValue<H>, Src::Source, AllOpen>
where
    Src: IntoSource,
    H: Handler<[u8]>,
{
    SubscriberBuilder::new(
        RawValue {
            handler,
            docs: Docs::none(),
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

impl<H> BatchDef for RawBatchValue<H> {
    type Input = RawBytes;
    type Handler = H;
    type Source = Unnamed<crate::Name>;

    fn source(&self) -> Self::Source {
        Unnamed::new()
    }

    fn description(&self) -> Option<&str> {
        self.docs.description()
    }

    fn into_handler(self) -> H {
        self.handler
    }
}

impl_describe!(RawBatchValue<H>);

/// Binds a raw slice `handler` to the batch subscription `source`: a whole page of payloads,
/// no decode step anywhere. Mount it with `include`.
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
