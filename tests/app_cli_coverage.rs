//! The parts of the app runtime that only the real run machinery reaches: what the lifecycle
//! logs, the signal-driven `run`, the failure paths (a refused subscription, a broker that will
//! not shut down), the graceful-shutdown timeout, and the registration builders for a handler
//! that combines a reply destination with `Out` slots.
//!
//! These drive `start` / `run` / `run_until` directly rather than the `TestApp` harness, which
//! bypasses that machinery by design; the harness is used where the subject is a registration.
#![cfg(all(
    feature = "memory",
    feature = "macros",
    feature = "json",
    feature = "cbor",
    feature = "testing",
    feature = "logging"
))]

mod common;

use std::future::{pending, ready};
use std::io;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use ruststream::codec::{CborCodec, Codec};
use ruststream::memory::{
    ConnectedMemoryBroker, MemoryBroker, MemoryError, MemoryPublish, MemorySubscriber,
};
use ruststream::runtime::{
    AppInfo, Context, DefaultSlot, Handle, HandlerOutcome, HealthState, IntoSource, Out, Outgoing,
    Payload, PublishContext, PublishError, PublishExt, PublishTransform, RustStream,
    RustStreamError, TypedPublisher, subscriber as subscriber_def,
};
use ruststream::testing::TestApp;
use ruststream::{
    Broker, ConnectedBroker, DescribeServer, Publisher, ServerSpec, SubscriptionSource, subscriber,
};
use tokio::sync::Notify;
use tokio::time::timeout;
use tracing_subscriber::EnvFilter;
use tracing_subscriber::fmt::MakeWriter;

use common::{Order, Receipt, order_bytes};

// ---------------------------------------------------------------------------------------------
// Captured logs: the lifecycle's structured fields are only evaluated when a subscriber is
// interested, so what the service reports about itself is observable only with one installed.

/// A `MakeWriter` appending every formatted event to a shared buffer.
#[derive(Clone)]
struct LogBuffer(Arc<Mutex<Vec<u8>>>);

impl LogBuffer {
    fn text(&self) -> String {
        let bytes = self.0.lock().expect("log buffer").clone();
        String::from_utf8(bytes).expect("utf-8 logs")
    }
}

/// One event's bytes, appended to the shared buffer as a unit. The formatter writes an event in
/// several calls, so writing straight into the shared buffer would let concurrent tests interleave
/// fragments and split a line an assertion is looking for.
struct EventWriter {
    shared: Arc<Mutex<Vec<u8>>>,
    event: Vec<u8>,
}

impl io::Write for EventWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.event.extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl Drop for EventWriter {
    fn drop(&mut self) {
        // A destructor must not panic, so a poisoned buffer drops the line instead.
        if let Ok(mut shared) = self.shared.lock() {
            shared.extend_from_slice(&self.event);
        }
    }
}

impl<'a> MakeWriter<'a> for LogBuffer {
    type Writer = EventWriter;

    fn make_writer(&'a self) -> Self::Writer {
        EventWriter {
            shared: Arc::clone(&self.0),
            event: Vec::new(),
        }
    }
}

static LOGS: OnceLock<LogBuffer> = OnceLock::new();

/// Installs the capturing subscriber the first time any test asks for it (the global slot takes
/// one subscriber per process) and hands back the shared buffer.
fn logs() -> &'static LogBuffer {
    LOGS.get_or_init(|| {
        let buffer = LogBuffer(Arc::new(Mutex::new(Vec::new())));
        let subscriber = tracing_subscriber::fmt()
            .with_env_filter(EnvFilter::new("ruststream=debug"))
            .with_ansi(false)
            .with_writer(buffer.clone())
            .finish();
        tracing::subscriber::set_global_default(subscriber)
            .expect("this binary installs no other subscriber");
        buffer
    })
}

/// The single captured line containing every marker, for an assertion that must not be satisfied
/// by another test's events in the shared buffer.
fn line_with<'a>(text: &'a str, markers: &[&str]) -> &'a str {
    text.lines()
        .find(|line| markers.iter().all(|marker| line.contains(marker)))
        .unwrap_or_else(|| panic!("no logged line with {markers:?} in:\n{text}"))
}

#[subscriber("cov.logged")]
async fn logged(_order: &Order) -> HandlerOutcome {
    HandlerOutcome::ack()
}

/// The lifecycle log is the operator's only view of startup, so it must name the service, each
/// broker (by label when registered with one, by type otherwise) and each subscriber.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_lifecycle_logs_name_the_service_brokers_and_subscribers() {
    let logs = logs();
    let app = RustStream::new(AppInfo::new("cov-logged-service", "0.3.0"))
        .register_broker(MemoryBroker::new())
        .with_broker_labeled("cov-logged-ingress", MemoryBroker::new(), |b| {
            b.include(logged);
        });

    let running = app.start().await.expect("startup failed");
    running.shutdown().await.expect("graceful shutdown failed");

    let text = logs.text();
    let starting = line_with(&text, &["starting service", "cov-logged-service"]);
    assert!(starting.contains("version=0.3.0"), "{starting}");
    assert!(starting.contains("brokers=2"), "{starting}");
    assert!(starting.contains("subscribers=1"), "{starting}");

    line_with(
        &text,
        &["broker connected", r#"broker="cov-logged-ingress""#],
    );
    // The unlabeled broker falls back to its own type name.
    line_with(&text, &["broker connected", "MemoryBroker"]);
    line_with(&text, &["subscriber started", "subscriber=cov.logged"]);
    line_with(
        &text,
        &["broker shut down", r#"broker="cov-logged-ingress""#],
    );
}

// ---------------------------------------------------------------------------------------------
// run / run_until: the two foreground forms.

static FAIL_FAST_READY: Notify = Notify::const_new();

/// Default policy: a panic fails fast, tearing the running service down.
#[subscriber("cov.failfast")]
async fn explodes(order: &Order) -> HandlerOutcome {
    // The test publishes ids other than u32::MAX, so this always panics; the trailing expression
    // keeps the body typed.
    assert_eq!(order.id, u32::MAX, "handler exploded");
    HandlerOutcome::ack()
}

/// `run_until`'s second arm: the caller's shutdown future never resolves, so only the service
/// tearing itself down can end the run - and the reason must survive into the returned error.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn run_until_returns_when_the_service_tears_itself_down() {
    let broker = MemoryBroker::new();
    let publisher = broker.publisher();
    let app = RustStream::new(AppInfo::new("cov-failfast", "0.1.0"))
        .shutdown_timeout(Duration::from_secs(5))
        .after_startup(async move |_state| {
            FAIL_FAST_READY.notify_one();
            Ok::<(), io::Error>(())
        })
        .with_broker(broker, |b| b.include(explodes));

    let run = tokio::spawn(app.run_until(pending()));
    FAIL_FAST_READY.notified().await;
    publisher
        .raw(&order_bytes(1))
        .to("cov.failfast")
        .publish()
        .await
        .expect("publish failed");

    let outcome = timeout(Duration::from_secs(10), run)
        .await
        .expect("run_until never returned on the fail-fast teardown")
        .expect("join failed");
    let err = outcome.expect_err("the fail-fast reason must surface");
    assert!(matches!(err, RustStreamError::Dispatch(_)), "got: {err:?}");
}

static SIGNAL_READY: Notify = Notify::const_new();

#[subscriber("cov.signalled")]
async fn signalled(_order: &Order) -> HandlerOutcome {
    HandlerOutcome::ack()
}

/// Sends `SIGTERM` to this process through the shell (the crate takes no libc dependency).
#[cfg(unix)]
fn raise_sigterm() {
    let status = std::process::Command::new("sh")
        .arg("-c")
        .arg(format!("kill -TERM {}", std::process::id()))
        .status()
        .expect("sending a signal to this process");
    assert!(status.success(), "kill -TERM failed: {status:?}");
}

/// `run` (the form `#[ruststream::app]` generates) shuts the service down gracefully on a
/// termination signal rather than letting the process die on the default disposition.
#[cfg(unix)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn run_shuts_down_gracefully_on_a_termination_signal() {
    // Registering a listener up front replaces the process-wide default disposition, so a signal
    // that lands before `run` has installed its own handler cannot kill the test binary.
    let _guard = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        .expect("SIGTERM registration");

    let app = RustStream::new(AppInfo::new("cov-signal", "0.1.0"))
        .shutdown_timeout(Duration::from_secs(5))
        .after_startup(async move |_state| {
            SIGNAL_READY.notify_one();
            Ok::<(), io::Error>(())
        })
        .with_broker(MemoryBroker::new(), |b| b.include(signalled));

    let mut run = tokio::spawn(app.run());
    SIGNAL_READY.notified().await;

    // The startup hook resolves just before the signal handlers are installed, and a signal
    // delivered in that window is missed by a stream registered afterwards; there is nothing to
    // observe the registration by, so the signal is re-sent until the run returns.
    let outcome = timeout(Duration::from_secs(30), async {
        loop {
            raise_sigterm();
            if let Ok(joined) = timeout(Duration::from_millis(50), &mut run).await {
                break joined;
            }
        }
    })
    .await
    .expect("run ignored the termination signal")
    .expect("join failed");
    outcome.expect("the signalled shutdown must be graceful");
}

// ---------------------------------------------------------------------------------------------
// Startup and teardown failures.

/// A subscription descriptor the broker refuses to open, for the startup unwind.
#[derive(Clone)]
struct RefusedSubscription;

impl SubscriptionSource<ConnectedMemoryBroker> for RefusedSubscription {
    type Subscriber = MemorySubscriber;

    // The returned lifetime is fixed by the trait, so it cannot be narrowed to `&'static str`.
    #[allow(clippy::unnecessary_literal_bound)]
    fn name(&self) -> &str {
        "cov.refused"
    }

    fn subscribe(
        self,
        _connected: &ConnectedMemoryBroker,
    ) -> impl Future<Output = Result<Self::Subscriber, MemoryError>> {
        ready(Err(MemoryError::ShutDown))
    }
}

// What a broker crate writes next to its own descriptor, so the value constructors accept it.
impl IntoSource for RefusedSubscription {
    type Source = Self;

    fn into_source(self) -> Self {
        self
    }
}

/// The body behind the refused subscription: it never runs, so it only has to exist.
struct NeverDelivered;

impl<'p> Handle<Payload<'p>> for NeverDelivered {
    fn handle(
        &self,
        _frame: &Payload<'p>,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        ready(Ok(()))
    }
}

/// A subscription that cannot be opened aborts startup instead of running a service that is
/// silently deaf, and the brokers connected so far are torn down on the way out.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_refused_subscription_aborts_startup_and_unwinds_the_broker() {
    let logs = logs();
    let broker = MemoryBroker::new();
    let publisher = broker.publisher();
    let app = RustStream::new(AppInfo::new("cov-refused", "0.1.0")).with_broker_labeled(
        "cov-refused-ingress",
        broker,
        |b| {
            b.include(subscriber_def(RefusedSubscription, NeverDelivered).build());
        },
    );

    let err = app
        .start()
        .await
        .expect_err("the refused subscription must abort startup");
    assert!(matches!(err, RustStreamError::Subscribe(_)), "got: {err:?}");
    let refused = publisher
        .raw(b"x")
        .to("cov.refused")
        .publish()
        .await
        .expect_err("the connected broker must not survive the failed startup");
    assert!(
        matches!(refused, PublishError::Publish(MemoryError::ShutDown)),
        "got: {refused:?}",
    );
    line_with(
        &logs.text(),
        &[
            "broker shut down after a startup failure",
            r#"broker="cov-refused-ingress""#,
        ],
    );
}

/// A broker that connects but refuses to shut down, for the teardown failure paths.
struct StubbornBroker;

/// The connected form of [`StubbornBroker`].
struct ConnectedStubborn;

impl Broker for StubbornBroker {
    type Error = io::Error;
    type Connected = ConnectedStubborn;

    fn connect(self) -> impl Future<Output = Result<Self::Connected, Self::Error>> {
        ready(Ok(ConnectedStubborn))
    }
}

impl ConnectedBroker for ConnectedStubborn {
    type Error = io::Error;
    type Closed = ();

    fn shutdown(self) -> impl Future<Output = Result<(), Self::Error>> {
        ready(Err(io::Error::other("teardown refused")))
    }
}

// Self-describing, so the broker can be registered under a label (the labeled path is what makes
// the unwind log name the broker by its service identity rather than by its type).
impl DescribeServer for StubbornBroker {
    fn describe_server(&self) -> ServerSpec {
        ServerSpec::new("stubborn:0", "memory")
    }
}

/// A broker whose connect always fails, for the partial-startup unwind.
struct UnreachableBroker;

/// Uninhabited connected form: [`UnreachableBroker::connect`] never produces one.
enum NeverConnected {}

impl Broker for UnreachableBroker {
    type Error = io::Error;
    type Connected = NeverConnected;

    fn connect(self) -> impl Future<Output = Result<Self::Connected, Self::Error>> {
        ready(Err(io::Error::other("dial refused")))
    }
}

impl ConnectedBroker for NeverConnected {
    type Error = io::Error;
    type Closed = ();

    // The connected form is uninhabited, so the body diverges: there is no value a `ready(..)`
    // rewrite could carry, only the divergence wrapped in one more layer.
    #[allow(clippy::unused_async_trait_impl)]
    async fn shutdown(self) -> Result<(), Self::Error> {
        match self {}
    }
}

/// A broker that fails to shut down turns the graceful teardown into an error and leaves the
/// health probe reporting the failure, so an orchestrator sees a non-zero exit and stops routing
/// traffic to the process.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_broker_that_refuses_to_shut_down_fails_the_teardown_and_the_probe() {
    let app =
        RustStream::new(AppInfo::new("cov-stubborn", "0.1.0")).register_broker(StubbornBroker);

    let running = app.start().await.expect("startup failed");
    let health = running.health();
    assert!(health.is_running());

    let err = running
        .shutdown()
        .await
        .expect_err("a refused broker shutdown must surface");
    assert!(matches!(err, RustStreamError::Shutdown(_)), "got: {err:?}");
    let state = health.state();
    assert!(
        matches!(&state, HealthState::Failed { reason } if reason.contains("teardown refused")),
        "got: {state:?}",
    );
}

/// The unwind of a failed startup is best effort: a broker that refuses to shut down is logged,
/// and the original startup error stays the caller's answer.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_refused_shutdown_during_the_startup_unwind_is_logged_not_returned() {
    let logs = logs();
    let app = RustStream::new(AppInfo::new("cov-unwind", "0.1.0"))
        .with_broker_labeled("cov-unwind-egress", StubbornBroker, |_b| {})
        .register_broker(UnreachableBroker);

    let err = app
        .start()
        .await
        .expect_err("the second broker cannot connect");
    assert!(matches!(err, RustStreamError::Connect(_)), "got: {err:?}");
    line_with(
        &logs.text(),
        &[
            "broker shutdown failed during the startup unwind",
            r#"broker="cov-unwind-egress""#,
            "teardown refused",
        ],
    );
}

// ---------------------------------------------------------------------------------------------
// The graceful-shutdown timeout: both what it bounds (handlers, post-settle continuations).

static HUNG_HANDLER: Notify = Notify::const_new();

#[subscriber("cov.hung")]
async fn hung(_order: &Order) -> HandlerOutcome {
    HUNG_HANDLER.notify_one();
    pending::<()>().await;
    HandlerOutcome::ack()
}

/// A handler that never returns must not hold the service up forever: the drain gives up after
/// the configured timeout and aborts what is left in flight.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_shutdown_timeout_aborts_a_handler_that_never_returns() {
    let broker = MemoryBroker::new();
    let publisher = broker.publisher();
    let app = RustStream::new(AppInfo::new("cov-hung", "0.1.0"))
        // Short on purpose: the timeout firing is the subject, not a wait for something else.
        .shutdown_timeout(Duration::from_millis(50))
        .with_broker(broker, |b| b.include(hung));

    let running = app.start().await.expect("startup failed");
    publisher
        .raw(&order_bytes(1))
        .to("cov.hung")
        .publish()
        .await
        .expect("publish failed");
    HUNG_HANDLER.notified().await;

    timeout(Duration::from_secs(10), running.shutdown())
        .await
        .expect("the drain ignored its timeout and hung on the handler")
        .expect("an aborted handler is not a shutdown failure");
}

static HUNG_CONTINUATION: Notify = Notify::const_new();

#[subscriber("cov.continuation")]
async fn with_continuation(_order: &Order) -> HandlerOutcome {
    HandlerOutcome::ack().and_after(async {
        HUNG_CONTINUATION.notify_one();
        pending::<()>().await;
    })
}

/// Post-settle continuations are drained under the same timeout; abandoning one is not a
/// shutdown failure (they are at-most-once side effects, and the message is already settled).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_shutdown_timeout_abandons_a_continuation_that_never_returns() {
    let broker = MemoryBroker::new();
    let publisher = broker.publisher();
    let app = RustStream::new(AppInfo::new("cov-continuation", "0.1.0"))
        .shutdown_timeout(Duration::from_millis(50))
        .with_broker(broker, |b| b.include(with_continuation));

    let running = app.start().await.expect("startup failed");
    publisher
        .raw(&order_bytes(1))
        .to("cov.continuation")
        .publish()
        .await
        .expect("publish failed");
    HUNG_CONTINUATION.notified().await;

    timeout(Duration::from_secs(10), running.shutdown())
        .await
        .expect("the drain ignored its timeout and hung on the continuation")
        .expect("an abandoned continuation is not a shutdown failure");
}

// ---------------------------------------------------------------------------------------------
// The builder surface: the labeled-codec registration and the include builders.

static LABELED_SEEN: Mutex<Vec<u32>> = Mutex::new(Vec::new());

#[subscriber("cov.labeled")]
async fn labeled(order: &Order) -> HandlerOutcome {
    LABELED_SEEN.lock().expect("seen").push(order.id);
    HandlerOutcome::ack()
}

/// `with_broker_labeled_codec` combines both halves: the label becomes the broker's `AsyncAPI`
/// server entry, and the scope codec (not the default) decodes the scope's handlers.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_labeled_scope_records_its_server_and_decodes_with_its_own_codec() {
    let app = RustStream::new(AppInfo::new("cov-labeled", "0.1.0")).with_broker_labeled_codec(
        "cov-egress",
        MemoryBroker::new(),
        CborCodec,
        |b| {
            b.include(labeled);
        },
    );
    assert!(
        app.servers().contains_key("cov-egress"),
        "the label registers the broker's server: {:?}",
        app.servers().keys().collect::<Vec<_>>(),
    );
    // The Debug form is the operator's view of a half-built service.
    let rendered = format!("{app:?}");
    assert!(rendered.starts_with("RustStream"), "{rendered}");
    assert!(rendered.contains("cov-labeled"), "{rendered}");
    assert!(rendered.contains("brokers: 1"), "{rendered}");
    assert!(rendered.contains("handlers: 1"), "{rendered}");

    let tb = TestApp::start(app).await.expect("harness start");
    // CBOR bytes: the scope codec decodes them, the default (JSON) codec could not.
    let payload = CborCodec.encode(&Order { id: 11 }).expect("cbor encode");
    tb.broker::<MemoryBroker>()
        .raw(&payload)
        .to("cov.labeled")
        .publish()
        .await
        .expect("raw publish");

    tb.broker::<MemoryBroker>()
        .subscriber("cov.labeled")
        .assert_called_once()
        .settled(HandlerOutcome::ack());
    assert_eq!(*LABELED_SEEN.lock().expect("seen"), vec![11]);
}

/// Stamps every outgoing reply, so a test can prove which reply source was used.
struct Envelope;

impl<C> PublishTransform<C> for Envelope {
    fn apply(&self, out: &mut Outgoing<'_>, _cx: &PublishContext<'_, C>) {
        out.headers_mut().insert("x-envelope", b"1".to_vec());
    }
}

#[subscriber("cov.audit.in", publish_raw("cov.audit.out"))]
async fn audited_relay(frame: &[u8], Out(audit): Out<impl Publisher>) -> Vec<u8> {
    audit
        .raw(frame)
        .to("cov.audit.copy")
        .publish()
        .await
        .expect("the slot publisher is live");
    frame.to_vec()
}

/// A byte-reply handler with an `Out` slot: the slot is bound explicitly and the reply leaves
/// through the broker's default publish policy, with no `.publisher(..)` in the chain.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_raw_reply_handler_with_a_slot_defaults_its_reply_publisher() {
    let app =
        RustStream::new(AppInfo::new("cov-audit", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
            b.include(audited_relay)
                .out(DefaultSlot, MemoryPublish)
                .build();
        });
    let tb = TestApp::start(app).await.expect("harness start");

    tb.broker::<MemoryBroker>()
        .raw(b"frame")
        .to("cov.audit.in")
        .publish()
        .await
        .expect("raw publish");

    tb.out::<DefaultSlot>()
        .assert_called_once()
        .with_raw(b"frame");
    let replies = tb
        .broker::<MemoryBroker>()
        .published::<Vec<u8>>("cov.audit.out");
    replies.assert_called_once().with_raw(b"frame");
}

#[subscriber("cov.gate.in", publish("cov.gate.out"))]
async fn gate(order: &Order, Out(audit): Out<impl Publisher>) -> Receipt {
    audit
        .raw(&order_bytes(order.id))
        .to("cov.gate.copy")
        .publish()
        .await
        .expect("the slot publisher is live");
    Receipt { id: order.id }
}

/// The reply side of a publishing handler with slots is overridable: `.publisher(..)` replaces
/// the default reply source, and the rest of the chain still binds the slots.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_publishing_handler_with_a_slot_takes_an_explicit_reply_publisher() {
    let app =
        RustStream::new(AppInfo::new("cov-gate", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
            b.include(gate)
                .publisher(TypedPublisher::new(MemoryPublish).transform(Envelope))
                .out(DefaultSlot, MemoryPublish)
                .build();
        });
    let tb = TestApp::start(app).await.expect("harness start");

    tb.message(&Order { id: 3 })
        .to("cov.gate.in")
        .publish()
        .await
        .expect("publish");

    tb.out::<DefaultSlot>().assert_called_once();
    let replies = tb
        .broker::<MemoryBroker>()
        .published::<Receipt>("cov.gate.out");
    let replies = replies.assert_called_once();
    assert_eq!(
        replies.messages()[0].headers().get("x-envelope"),
        Some(b"1".as_slice()),
        "the reply must leave through the publisher named in the chain",
    );
}

#[subscriber("cov.debug.in", publish("cov.debug.out"))]
async fn debug_reply(order: &Order) -> Receipt {
    Receipt { id: order.id }
}

#[subscriber("cov.debug.slot")]
async fn debug_slot(_order: &Order, Out(out): Out<impl Publisher>) -> HandlerOutcome {
    let _ = out;
    HandlerOutcome::ack()
}

/// Each registration builder is `Debug`, and none of them leaks the scope it borrows.
#[test]
fn the_include_builders_render_a_debug_form() {
    let _app =
        RustStream::new(AppInfo::new("cov-debug", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
            let reply = b.include(debug_reply);
            assert_debug_form(&reply, "IncludeWith");
            reply.publisher(TypedPublisher::new(MemoryPublish));

            let slots = b.include(debug_slot);
            assert_debug_form(&slots, "IncludeSlots");
            slots.publisher(MemoryPublish);

            let both = b.include(gate);
            assert_debug_form(&both, "IncludeSlotsWithReply");
            both.out(DefaultSlot, MemoryPublish).build();
        });
}

fn assert_debug_form<T: std::fmt::Debug>(value: &T, expected: &str) {
    let rendered = format!("{value:?}");
    assert!(rendered.starts_with(expected), "{rendered}");
    assert!(
        !rendered.contains("BrokerScope"),
        "the builder must not render the scope it borrows: {rendered}",
    );
}
