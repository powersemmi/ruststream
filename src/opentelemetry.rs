//! W3C Trace Context propagation and consumer spans (part of the `otel` feature).
//!
//! Distributed tracing for a RustStream service, built on the publish-path context from the typed
//! [`Context`](crate::runtime::Context) system: a trace / correlation id flows from an incoming
//! message onto the replies it produces, so a trace spans the whole consume-transform-produce chain.
//!
//! Two pieces, wired like [`metrics`](crate::metrics):
//!
//! - [`OpenTelemetry::consume_layer`] - a consume-side [`Layer`] that, per delivery, reads the
//!   incoming [W3C `traceparent`](https://www.w3.org/TR/trace-context/) header, opens a `tracing`
//!   span for the handler, and stamps the *consumer's* span onto the working headers so a reply
//!   published from that handler becomes its child.
//! - [`OpenTelemetry::propagation`] - a static [`PublishTransform`] that copies the working
//!   `traceparent` onto every reply. Reuse it on a batch publisher with
//!   [`for_batch`](crate::runtime::for_batch).
//!
//! This module is the propagation half of the story: it carries the W3C context and emits
//! `tracing` spans. Export is the other half - [`Otel::init`](crate::otel::Otel::init) wires
//! these spans into OTLP, or install your own subscriber, exactly as
//! [`logging`](crate::logging) leaves the subscriber to the user. Propagation itself is
//! broker-agnostic and works with no subscriber at all.
//!
//! # Examples
//!
//! ```
//! # #[cfg(all(feature = "memory", feature = "json"))]
//! # {
//! use ruststream::memory::MemoryBroker;
//! use ruststream::opentelemetry::OpenTelemetry;
//! use ruststream::runtime::TypedPublisher;
//!
//! let otel = OpenTelemetry::new();
//! let broker = MemoryBroker::new();
//! // Replies carry the delivery's trace context.
//! let publisher = TypedPublisher::new(broker.publisher()).transform(otel.propagation());
//! # let _ = publisher;
//! # }
//! ```

use std::collections::hash_map::RandomState;
use std::hash::{BuildHasher, Hasher};
use std::sync::LazyLock;
use std::sync::atomic::{AtomicU64, Ordering};

use tracing::Instrument;

use crate::runtime::{
    BlanketLayer, Context, Handler, Layer, Outgoing, PublishContext, PublishTransform, Settle,
};

/// The HTTP header carrying the W3C trace context.
const TRACEPARENT: &str = "traceparent";
/// The HTTP header carrying vendor-specific trace state, propagated verbatim.
const TRACESTATE: &str = "tracestate";

/// A [W3C trace context](https://www.w3.org/TR/trace-context/): the `trace-id`, the current
/// `span-id` (the `parent-id` field of the `traceparent` header), and the trace flags.
///
/// Parsed from and formatted to the `traceparent` header (`00-<trace-id>-<span-id>-<flags>`); the
/// version is always `00`. A reply published from a handler carries the consumer's context, so a
/// downstream service sees the consumer span as the reply's parent.
///
/// # Examples
///
/// ```
/// use ruststream::opentelemetry::TraceContext;
///
/// let tc = TraceContext::parse("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01")
///     .expect("valid traceparent");
/// assert!(tc.sampled());
/// assert_eq!(
///     tc.to_header(),
///     "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
/// );
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct TraceContext {
    trace_id: [u8; 16],
    span_id: [u8; 8],
    flags: u8,
}

impl TraceContext {
    /// Starts a fresh, sampled trace with a new `trace-id` and `span-id`.
    ///
    /// Used when an incoming message carries no (valid) `traceparent`.
    ///
    /// # Examples
    ///
    /// ```
    /// use ruststream::opentelemetry::TraceContext;
    ///
    /// let root = TraceContext::root();
    /// assert!(root.sampled());
    /// assert_eq!(root.to_header().len(), "00-".len() + 32 + 1 + 16 + 1 + 2);
    /// ```
    #[must_use]
    pub fn root() -> Self {
        let mut trace_id = [0u8; 16];
        let mut span_id = [0u8; 8];
        fill_random(&mut trace_id);
        fill_random(&mut span_id);
        Self {
            trace_id,
            span_id,
            flags: 0x01,
        }
    }

    /// The child of this context: the same trace, a fresh `span-id`. The consumer's own span.
    ///
    /// # Examples
    ///
    /// ```
    /// use ruststream::opentelemetry::TraceContext;
    ///
    /// let parent = TraceContext::root();
    /// let child = parent.child();
    /// assert_eq!(parent.trace_id(), child.trace_id());
    /// assert_ne!(parent.span_id(), child.span_id());
    /// ```
    #[must_use]
    pub fn child(&self) -> Self {
        let mut span_id = [0u8; 8];
        fill_random(&mut span_id);
        Self {
            trace_id: self.trace_id,
            span_id,
            flags: self.flags,
        }
    }

    /// Parses a `traceparent` header value, or `None` if it is malformed or names the all-zero
    /// (invalid) `trace-id` / `span-id`.
    ///
    /// # Examples
    ///
    /// ```
    /// use ruststream::opentelemetry::TraceContext;
    ///
    /// assert!(TraceContext::parse("not-a-traceparent").is_none());
    /// assert!(
    ///     TraceContext::parse("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01").is_some()
    /// );
    /// ```
    #[must_use]
    pub fn parse(header: &str) -> Option<Self> {
        let mut parts = header.split('-');
        let version = parts.next()?;
        let trace = parts.next()?;
        let span = parts.next()?;
        let flags = parts.next()?;
        if parts.next().is_some() || version != "00" {
            return None;
        }
        let mut trace_id = [0u8; 16];
        let mut span_id = [0u8; 8];
        read_hex(trace, &mut trace_id)?;
        read_hex(span, &mut span_id)?;
        if trace_id == [0u8; 16] || span_id == [0u8; 8] {
            return None;
        }
        let mut flag_byte = [0u8; 1];
        read_hex(flags, &mut flag_byte)?;
        Some(Self {
            trace_id,
            span_id,
            flags: flag_byte[0],
        })
    }

    /// Formats this context as a `traceparent` header value.
    ///
    /// # Examples
    ///
    /// ```
    /// use ruststream::opentelemetry::TraceContext;
    ///
    /// let header = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";
    /// assert_eq!(TraceContext::parse(header).unwrap().to_header(), header);
    /// ```
    #[must_use]
    pub fn to_header(&self) -> String {
        let mut out = String::with_capacity(55);
        out.push_str("00-");
        write_hex(&self.trace_id, &mut out);
        out.push('-');
        write_hex(&self.span_id, &mut out);
        out.push('-');
        write_hex(&[self.flags], &mut out);
        out
    }

    /// The 16-byte `trace-id`, hex-encoded - the identity of the whole trace.
    ///
    /// # Examples
    ///
    /// ```
    /// use ruststream::opentelemetry::TraceContext;
    ///
    /// let tc = TraceContext::parse("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01")
    ///     .unwrap();
    /// assert_eq!(tc.trace_id(), "4bf92f3577b34da6a3ce929d0e0e4736");
    /// ```
    #[must_use]
    pub fn trace_id(&self) -> String {
        let mut out = String::with_capacity(32);
        write_hex(&self.trace_id, &mut out);
        out
    }

    /// The 8-byte `span-id`, hex-encoded - the identity of this span within the trace.
    ///
    /// # Examples
    ///
    /// ```
    /// use ruststream::opentelemetry::TraceContext;
    ///
    /// let tc = TraceContext::parse("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01")
    ///     .unwrap();
    /// assert_eq!(tc.span_id(), "00f067aa0ba902b7");
    /// ```
    #[must_use]
    pub fn span_id(&self) -> String {
        let mut out = String::with_capacity(16);
        write_hex(&self.span_id, &mut out);
        out
    }

    /// Whether the sampled flag (bit 0) is set.
    ///
    /// # Examples
    ///
    /// ```
    /// use ruststream::opentelemetry::TraceContext;
    ///
    /// let sampled = TraceContext::parse("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01")
    ///     .unwrap();
    /// assert!(sampled.sampled());
    /// let off = TraceContext::parse("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-00")
    ///     .unwrap();
    /// assert!(!off.sampled());
    /// ```
    #[must_use]
    pub const fn sampled(&self) -> bool {
        self.flags & 0x01 != 0
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
/// use ruststream::opentelemetry::OpenTelemetry;
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
    /// use ruststream::opentelemetry::OpenTelemetry;
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
    /// use ruststream::opentelemetry::OpenTelemetry;
    ///
    /// let _layer = OpenTelemetry::new().consume_layer();
    /// ```
    #[must_use]
    pub const fn consume_layer(&self) -> OpenTelemetryLayer {
        OpenTelemetryLayer
    }

    /// The publish-side [`PublishTransform`]: copies the delivery's `traceparent` onto every reply. Bake
    /// it onto a [`TypedPublisher`](crate::runtime::TypedPublisher) with
    /// [`transform`](crate::runtime::TypedPublisher::transform) (or, for a batch publisher, via
    /// [`for_batch`](crate::runtime::for_batch)).
    ///
    /// # Examples
    ///
    /// ```
    /// use ruststream::opentelemetry::OpenTelemetry;
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
// concrete types the router hides), the same way `MetricsLayer` does. The wrapped handler reads the
// trace context off the typed `Context`, not the broker message, so it needs no `IncomingMessage`
// bound and applies to any handler.
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
    fn handle(&self, msg: &M, ctx: &mut Context<'_, C, S>) -> impl Future<Output = Settle> + Send {
        // Continue the incoming trace (or start one), then publish the consumer's own span as the
        // working `traceparent` so a reply from this handler becomes its child.
        let consumer = ctx
            .headers()
            .get_str(TRACEPARENT)
            .and_then(TraceContext::parse)
            .map_or_else(TraceContext::root, |incoming| incoming.child());
        let span = tracing::info_span!(
            "ruststream.consume",
            otel.kind = "consumer",
            subscription = %ctx.name(),
            trace_id = %consumer.trace_id(),
            span_id = %consumer.span_id(),
        );
        ctx.headers_mut().insert(TRACEPARENT, consumer.to_header());
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

/// Fills `buf` with unpredictable bytes for a fresh trace- or span-id.
///
/// Uses a per-process randomly seeded [`RandomState`] hashing a monotonic counter, so ids are
/// well-distributed and unique within the process without pulling a random-number dependency (trace
/// ids are identifiers, not secrets).
fn fill_random(buf: &mut [u8]) {
    static SEED: LazyLock<RandomState> = LazyLock::new(RandomState::new);
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    for chunk in buf.chunks_mut(8) {
        let mut hasher = SEED.build_hasher();
        hasher.write_u64(COUNTER.fetch_add(1, Ordering::Relaxed));
        let bytes = hasher.finish().to_be_bytes();
        chunk.copy_from_slice(&bytes[..chunk.len()]);
    }
}

/// Writes `bytes` as lowercase hex into `out`.
fn write_hex(bytes: &[u8], out: &mut String) {
    for &byte in bytes {
        out.push(char::from_digit(u32::from(byte >> 4), 16).expect("nibble is < 16"));
        out.push(char::from_digit(u32::from(byte & 0x0f), 16).expect("nibble is < 16"));
    }
}

/// Reads `out.len()` bytes of lowercase / uppercase hex from `s` into `out`, or `None` on a length
/// or digit mismatch.
fn read_hex(s: &str, out: &mut [u8]) -> Option<()> {
    if s.len() != out.len() * 2 {
        return None;
    }
    for (index, byte) in out.iter_mut().enumerate() {
        *byte = u8::from_str_radix(s.get(index * 2..index * 2 + 2)?, 16).ok()?;
    }
    Some(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn root_is_sampled_and_unique() {
        let a = TraceContext::root();
        let b = TraceContext::root();
        assert!(a.sampled());
        assert_ne!(a.trace_id(), b.trace_id());
        assert_ne!(a.span_id(), b.span_id());
    }

    #[test]
    fn child_keeps_trace_changes_span() {
        let parent = TraceContext::root();
        let child = parent.child();
        assert_eq!(parent.trace_id(), child.trace_id());
        assert_ne!(parent.span_id(), child.span_id());
        assert_eq!(parent.sampled(), child.sampled());
    }

    #[test]
    fn parse_rejects_malformed() {
        assert!(TraceContext::parse("").is_none());
        assert!(TraceContext::parse("00-tooshort-00f067aa0ba902b7-01").is_none());
        // All-zero ids are invalid per the spec.
        assert!(
            TraceContext::parse("00-00000000000000000000000000000000-00f067aa0ba902b7-01")
                .is_none()
        );
        // Unsupported version.
        assert!(
            TraceContext::parse("01-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01")
                .is_none()
        );
    }

    #[test]
    fn round_trips_through_the_header() {
        let header = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";
        let parsed = TraceContext::parse(header).expect("valid");
        assert_eq!(parsed.to_header(), header);
    }
}
