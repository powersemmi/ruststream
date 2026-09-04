//! W3C Trace Context propagation and consumer spans (part of the `otel` feature).
//!
//! Distributed tracing for a RustStream service, built on the publish-path context from the typed
//! [`Context`] machinery: a trace / correlation id flows from an incoming
//! message onto the replies it produces, so a trace spans the whole consume-transform-produce chain.
//!
//! Two pieces, wired like [`metrics`](crate::metrics):
//!
//! - [`OpenTelemetry::consume_layer`] - a consume-side [`Layer`] that, per delivery, reads the
//!   incoming [W3C trace context](https://www.w3.org/TR/trace-context/) headers, opens a `tracing`
//!   span for the handler, and stamps the *consumer's* span onto the working headers so a reply
//!   published from that handler becomes its child.
//! - [`OpenTelemetry::propagation`] - a static [`PublishTransform`] that copies the working
//!   `traceparent` onto every reply. Reuse it on a batch publisher with
//!   [`for_batch`](crate::runtime::for_batch).
//!
//! The header parsing and formatting is the OpenTelemetry SDK's
//! [`TraceContextPropagator`], so it follows the spec strictly (for example, uppercase hex ids are
//! rejected). This module is the propagation half of the story: it carries the W3C context and
//! emits `tracing` spans. Export is the other half -
//! [`OtelBuilder::init`](crate::otel::OtelBuilder::init) wires these spans into OTLP, or install
//! your own subscriber, exactly as [`logging`](crate::logging) leaves the subscriber to the user.
//! Propagation itself is broker-agnostic and works with no subscriber at all.
//!
//! # Examples
//!
//! ```
//! # #[cfg(all(feature = "memory", feature = "macros", feature = "json"))]
//! # mod demo {
//! use ruststream::memory::prelude::*;
//! use ruststream::otel::OpenTelemetry;
//! # use ruststream::subscriber;
//! # #[derive(serde::Deserialize, schemars::JsonSchema)]
//! # struct Order { id: u64 }
//! # #[derive(serde::Serialize, schemars::JsonSchema)]
//! # struct Confirmation { id: u64 }
//! # #[subscriber("orders", publish("confirmations"))]
//! # async fn confirm(order: &Order) -> Confirmation { Confirmation { id: order.id } }
//!
//! fn app() -> RustStream {
//!     let otel = OpenTelemetry::new();
//!     RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
//!         // Replies carry the delivery's trace context.
//!         b.include(confirm).out(Reply, Publish).transform(otel.propagation());
//!     })
//! }
//! # }
//! ```

use opentelemetry::Context as OtelContext;
use opentelemetry::propagation::{Extractor, Injector, TextMapPropagator};
use opentelemetry::trace::{SpanContext, TraceContextExt, TraceFlags, TraceState};
use opentelemetry_sdk::propagation::TraceContextPropagator;
use opentelemetry_sdk::trace::{IdGenerator, RandomIdGenerator};
use tracing::Instrument;

use crate::HeaderMap;
use crate::runtime::{
    BlanketLayer, Context, Handler, HandlerOutcome, Layer, Outgoing, PublishContext,
    PublishTransform,
};

/// The HTTP header carrying the W3C trace context.
const TRACEPARENT: &str = "traceparent";
/// The HTTP header carrying vendor-specific trace state, propagated verbatim.
const TRACESTATE: &str = "tracestate";

/// Reads the W3C context headers off crate [`HeaderMap`] for the SDK propagator.
struct HeaderExtractor<'a>(&'a HeaderMap);

impl Extractor for HeaderExtractor<'_> {
    fn get(&self, key: &str) -> Option<&str> {
        self.0.get_str(key)
    }

    fn keys(&self) -> Vec<&str> {
        // `TraceContextPropagator` only calls `get`; this allocates solely under a composite
        // propagator that actually walks the keys.
        self.0.iter().map(|(name, _)| name).collect()
    }
}

/// Writes the W3C context headers the SDK propagator produces onto crate [`HeaderMap`].
struct HeaderInjector<'a>(&'a mut HeaderMap);

impl Injector for HeaderInjector<'_> {
    fn set(&mut self, key: &str, value: String) {
        // The SDK always writes `tracestate`, even when empty; skip the noise header so a
        // delivery without one does not grow it.
        if !value.is_empty() {
            self.0.insert(key, value);
        }
    }
}

/// The OpenTelemetry tracing integration: hands out the consume-side span [`Layer`] and the
/// publish-side propagation [`PublishTransform`].
///
/// Cheap to construct and clone (it holds no state); make one and reuse it across the app.
///
/// # Examples
///
/// ```
/// use ruststream::otel::OpenTelemetry;
///
/// let otel = OpenTelemetry::new();
/// let _consume = otel.consume_layer();
/// let _publish = otel.propagation();
/// ```
#[derive(Debug, Clone, Copy, Default)]
pub struct OpenTelemetry;

impl OpenTelemetry {
    /// Creates the integration.
    ///
    /// # Examples
    ///
    /// ```
    /// use ruststream::otel::OpenTelemetry;
    ///
    /// let _otel = OpenTelemetry::new();
    /// ```
    #[must_use]
    pub const fn new() -> Self {
        Self
    }

    /// The consume-side [`Layer`]: opens a span per delivery and stamps the consumer's trace context
    /// onto the working headers. Add it with
    /// [`RustStream::layer`](crate::runtime::RustStream::layer).
    ///
    /// # Examples
    ///
    /// ```
    /// use ruststream::otel::OpenTelemetry;
    ///
    /// let _layer = OpenTelemetry::new().consume_layer();
    /// ```
    #[must_use]
    pub const fn consume_layer(&self) -> OpenTelemetryLayer {
        OpenTelemetryLayer
    }

    /// The publish-side [`PublishTransform`]: copies the delivery's `traceparent` onto every
    /// reply. Compose it onto a reply's wiring with the mount site's `.transform(..)` step (or,
    /// on a page reply, `.batch_transform(for_batch(..))` - see
    /// [`for_batch`](crate::runtime::for_batch)).
    ///
    /// # Examples
    ///
    /// ```
    /// use ruststream::otel::OpenTelemetry;
    ///
    /// let _propagation = OpenTelemetry::new().propagation();
    /// ```
    #[must_use]
    pub const fn propagation(&self) -> TracePropagation {
        TracePropagation
    }
}

/// The consume-side [`Layer`] handed out by [`OpenTelemetry::consume_layer`].
#[derive(Debug, Clone, Copy, Default)]
pub struct OpenTelemetryLayer;

impl<H> Layer<H> for OpenTelemetryLayer {
    type Handler = OpenTelemetryHandler<H>;

    fn layer(&self, inner: H) -> Self::Handler {
        OpenTelemetryHandler { inner }
    }
}

// Lets the span layer ride the app-wide stack and reach handlers mounted through a router (whose
// concrete types the router hides), the same way `MetricsLayer` does. The wrapped handler reads
// the trace context off the typed `Context`, so it applies to any handler.
impl BlanketLayer for OpenTelemetryLayer {
    fn apply<M, C, S, H>(&self, handler: H) -> impl Handler<M, C, S> + 'static
    where
        M: Send + Sync + 'static,
        C: Send + 'static,
        S: Send + Sync + 'static,
        H: Handler<M, C, S> + 'static,
    {
        self.layer(handler)
    }
}

/// The handler produced by [`OpenTelemetryLayer::layer`]: opens a span and stamps the consumer's
/// trace context onto the working headers before running the wrapped handler.
#[derive(Debug, Clone, Copy, Default)]
pub struct OpenTelemetryHandler<H> {
    inner: H,
}

impl<M, C, S, H> Handler<M, C, S> for OpenTelemetryHandler<H>
where
    M: Sync,
    C: Send,
    S: Send + Sync,
    H: Handler<M, C, S>,
{
    fn handle(
        &self,
        msg: &M,
        ctx: &mut Context<'_, C, S>,
    ) -> impl Future<Output = HandlerOutcome> + Send {
        // The concrete propagator, not `opentelemetry::global`: the global defaults to a no-op
        // until `Otel::init` installs one, and propagation must keep working with no SDK setup
        // at all. The type is a ZST, so constructing it per delivery is free.
        let propagator = TraceContextPropagator::new();
        let extracted =
            propagator.extract_with_context(&OtelContext::new(), &HeaderExtractor(ctx.headers()));
        let parent = extracted.span().span_context().clone();
        // Continue the incoming trace (or start one): same trace, fresh span id - the consumer's
        // own span. The propagator already rejected any malformed / all-zero header.
        let ids = RandomIdGenerator::default();
        let consumer = if parent.is_valid() {
            SpanContext::new(
                parent.trace_id(),
                ids.new_span_id(),
                parent.trace_flags(),
                false,
                parent.trace_state().clone(),
            )
        } else {
            SpanContext::new(
                ids.new_trace_id(),
                ids.new_span_id(),
                TraceFlags::SAMPLED,
                false,
                TraceState::default(),
            )
        };
        let span = tracing::info_span!(
            "ruststream.consume",
            otel.kind = "consumer",
            subscription = %ctx.name(),
            trace_id = %consumer.trace_id(),
            span_id = %consumer.span_id(),
        );
        // PublishBuilder the consumer's span as the working `traceparent` so a reply from this handler
        // becomes its child.
        propagator.inject_context(
            &OtelContext::new().with_remote_span_context(consumer),
            &mut HeaderInjector(ctx.headers_mut()),
        );
        self.inner.handle(msg, ctx).instrument(span)
    }
}

/// The publish-side [`PublishTransform`] handed out by [`OpenTelemetry::propagation`].
///
/// Copies the originating delivery's `traceparent` (and `tracestate`) onto the reply, so the trace
/// continues across the publish. Generic over the handler context, so it mounts on any publisher.
#[derive(Debug, Clone, Copy, Default)]
pub struct TracePropagation;

impl<C> PublishTransform<C> for TracePropagation {
    fn apply(&self, out: &mut Outgoing<'_>, cx: &PublishContext<'_, C>) {
        if let Some(traceparent) = cx.headers().get_str(TRACEPARENT) {
            out.headers_mut()
                .insert(TRACEPARENT, traceparent.as_bytes().to_vec());
            if let Some(tracestate) = cx.headers().get_str(TRACESTATE) {
                out.headers_mut()
                    .insert(TRACESTATE, tracestate.as_bytes().to_vec());
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const HEADER: &str = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";

    #[test]
    fn context_round_trips_through_headers() {
        let mut headers = HeaderMap::new();
        headers.insert(TRACEPARENT, HEADER);
        let propagator = TraceContextPropagator::new();
        let cx = propagator.extract_with_context(&OtelContext::new(), &HeaderExtractor(&headers));
        assert!(cx.span().span_context().is_valid());

        let mut out = HeaderMap::new();
        propagator.inject_context(&cx, &mut HeaderInjector(&mut out));
        assert_eq!(out.get_str(TRACEPARENT), Some(HEADER));
        assert!(
            !out.contains(TRACESTATE),
            "an empty tracestate must not be written"
        );
    }

    #[test]
    fn tracestate_rides_extraction_and_injection() {
        let mut headers = HeaderMap::new();
        headers.insert(TRACEPARENT, HEADER);
        headers.insert(TRACESTATE, "vendor=opaque");
        let propagator = TraceContextPropagator::new();
        let cx = propagator.extract_with_context(&OtelContext::new(), &HeaderExtractor(&headers));

        let mut out = HeaderMap::new();
        propagator.inject_context(&cx, &mut HeaderInjector(&mut out));
        assert_eq!(out.get_str(TRACESTATE), Some("vendor=opaque"));
    }

    #[test]
    fn extractor_lists_the_header_names() {
        let mut headers = HeaderMap::new();
        headers.insert(TRACEPARENT, HEADER);
        assert_eq!(HeaderExtractor(&headers).keys(), vec![TRACEPARENT]);
    }

    #[test]
    fn the_publish_transform_carries_the_incoming_trace_onto_the_reply() {
        use crate::runtime::{Outgoing, PublishContext};

        let mut incoming = HeaderMap::new();
        incoming.insert(TRACEPARENT, HEADER);
        incoming.insert(TRACESTATE, "vendor=opaque");
        let cx = PublishContext::new("orders", &incoming, &());

        let mut out = Outgoing::new("replies", b"body".as_slice());
        PublishTransform::apply(&TracePropagation, &mut out, &cx);

        // Without both headers the downstream span would start a new trace instead of continuing.
        assert_eq!(out.headers().get_str(TRACEPARENT), Some(HEADER));
        assert_eq!(out.headers().get_str(TRACESTATE), Some("vendor=opaque"));
    }

    #[test]
    fn a_delivery_without_a_trace_leaves_the_reply_untouched() {
        use crate::runtime::{Outgoing, PublishContext};

        let incoming = HeaderMap::new();
        let cx = PublishContext::new("orders", &incoming, &());

        let mut out = Outgoing::new("replies", b"body".as_slice());
        PublishTransform::apply(&TracePropagation, &mut out, &cx);
        assert!(!out.headers().contains(TRACEPARENT));
    }
}
