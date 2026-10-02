//! Where a reply is published, driven end to end on the in-memory broker: the reply type's own
//! declaration decides it, and the mount-site name is the default a reply type declaring none
//! takes. Both surfaces are here, because the attribute and the chain resolve the same way.
//!
//! A mount-site name that a declaration overrides is reported at startup. Those tests capture the
//! log with a subscriber installed for the test's own thread, so they run on the current-thread
//! runtime, where the harness starts every registration on that thread.
#![cfg(all(
    feature = "testing",
    feature = "macros",
    feature = "memory",
    feature = "json"
))]

use std::future::{Future, ready};
use std::io;
use std::sync::{Arc, Mutex, PoisonError};

use ruststream::memory::prelude::*;
use ruststream::nonzero;
use ruststream::testing::TestApp;
use serde::{Deserialize, Serialize};
use tracing::Level;
use tracing::subscriber::DefaultGuard;
use tracing_subscriber::fmt::MakeWriter;

#[derive(Debug, Deserialize, Outgoing, Serialize, PartialEq, schemars::JsonSchema)]
struct Order {
    id: u64,
}

/// A reply that fixes its destination: every mounting publishes it on `dest.declared`.
#[derive(Debug, Deserialize, Outgoing, Serialize, PartialEq, schemars::JsonSchema)]
#[outgoing(name = "dest.declared")]
struct Confirmation {
    id: u64,
}

/// A reply that declares no destination: the mount site names one.
#[derive(Debug, Deserialize, Outgoing, Serialize, PartialEq, schemars::JsonSchema)]
struct Receipt {
    id: u64,
}

#[subscriber("dest.bare", reply)]
async fn bare(order: &Order) -> Confirmation {
    Confirmation { id: order.id }
}

/// The clause names a subject the reply type contradicts, which the declaration wins.
#[subscriber("dest.contradicted", reply("dest.unused"))]
async fn contradicted(order: &Order) -> Confirmation {
    Confirmation { id: order.id }
}

#[subscriber("dest.named", reply("dest.receipts"))]
async fn named(order: &Order) -> Receipt {
    Receipt { id: order.id }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_reply_type_that_fixes_a_name_is_published_there() {
    let app = RustStream::new(AppInfo::new("reply-destination", "0.1.0")).with_broker(
        MemoryBroker::new(),
        |b| {
            b.include(bare);
        },
    );
    let tb = TestApp::start(app).await.expect("harness start");

    tb.message(&Order { id: 7 })
        .to("dest.bare")
        .publish()
        .await
        .expect("publish");

    tb.broker::<MemoryBroker>()
        .published::<Confirmation>("dest.declared")
        .assert_called_once()
        .with(&Confirmation { id: 7 });
}

/// The mount-site name is a default, so it does not redirect a reply type that fixes one.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_mount_site_name_does_not_redirect_a_fixed_name_reply() {
    let app = RustStream::new(AppInfo::new("reply-destination", "0.1.0")).with_broker(
        MemoryBroker::new(),
        |b| {
            b.include(contradicted).out(Reply, Publish);
        },
    );
    let tb = TestApp::start(app).await.expect("harness start");

    tb.message(&Order { id: 1 })
        .to("dest.contradicted")
        .publish()
        .await
        .expect("publish");

    tb.broker::<MemoryBroker>()
        .published::<Confirmation>("dest.declared")
        .assert_called_once()
        .with(&Confirmation { id: 1 });
    tb.broker::<MemoryBroker>()
        .published::<Confirmation>("dest.unused")
        .assert_not_called();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_reply_type_declaring_no_name_takes_the_mount_site_one() {
    let app = RustStream::new(AppInfo::new("reply-destination", "0.1.0")).with_broker(
        MemoryBroker::new(),
        |b| {
            b.include(named);
        },
    );
    let tb = TestApp::start(app).await.expect("harness start");

    tb.message(&Order { id: 4 })
        .to("dest.named")
        .publish()
        .await
        .expect("publish");

    tb.broker::<MemoryBroker>()
        .published::<Receipt>("dest.receipts")
        .assert_called_once()
        .with(&Receipt { id: 4 });
}

/// The same resolution on the manual chain: `.reply()` alone reads the declaration, and
/// `.to(..)` supplies the default a declaration-free reply type takes.
struct Confirm;

impl Handle<Order, Confirmation> for Confirm {
    fn handle(
        &self,
        order: &Order,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<Confirmation, HandlerOutcome>> {
        ready(Ok(Confirmation { id: order.id }))
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_chain_resolves_the_destination_the_same_way() {
    let app = RustStream::new(AppInfo::new("reply-destination", "0.1.0")).with_broker(
        MemoryBroker::new(),
        |b| {
            b.include(subscriber("chain.bare", Confirm).reply().build());
            b.include(
                subscriber("chain.contradicted", Confirm)
                    .reply()
                    .to("chain.unused")
                    .build(),
            );
        },
    );
    let tb = TestApp::start(app).await.expect("harness start");

    for subject in ["chain.bare", "chain.contradicted"] {
        tb.message(&Order { id: 2 })
            .to(subject)
            .publish()
            .await
            .expect("publish");
    }

    tb.broker::<MemoryBroker>()
        .published::<Confirmation>("dest.declared")
        .assert_called(2);
    tb.broker::<MemoryBroker>()
        .published::<Confirmation>("chain.unused")
        .assert_not_called();
}

/// The generated document reports the destination the reply is actually published to, not the
/// clause's literal.
#[cfg(feature = "asyncapi")]
#[test]
fn the_document_reports_the_resolved_destination() {
    let app = RustStream::new(AppInfo::new("reply-destination", "0.1.0")).with_broker(
        MemoryBroker::new(),
        |b| {
            b.include(contradicted).out(Reply, Publish);
        },
    );

    let spec = ruststream::asyncapi::build_spec(&app);

    assert!(
        spec.channels.contains_key("dest.declared"),
        "the declared channel must be in the document: {:?}",
        spec.channels.keys().collect::<Vec<_>>(),
    );
    assert!(
        !spec.channels.contains_key("dest.unused"),
        "the clause's unused default must not be: {:?}",
        spec.channels.keys().collect::<Vec<_>>(),
    );
}

/// The startup warning's message, as the log renders it.
const IGNORED: &str = "the mount-site reply name is ignored";

/// The log lines written while the guard lives, as text.
#[derive(Clone, Default)]
struct Captured(Arc<Mutex<Vec<u8>>>);

impl Captured {
    fn install(&self) -> DefaultGuard {
        let subscriber = tracing_subscriber::fmt()
            .with_writer(self.clone())
            .with_ansi(false)
            .with_max_level(Level::WARN)
            .finish();
        tracing::subscriber::set_default(subscriber)
    }

    fn text(&self) -> String {
        let bytes = self.0.lock().unwrap_or_else(PoisonError::into_inner);
        String::from_utf8_lossy(&bytes).into_owned()
    }

    /// The warning lines, one per registration that reported an ignored name.
    fn ignored(&self) -> Vec<String> {
        self.text()
            .lines()
            .filter(|line| line.contains(IGNORED))
            .map(str::to_owned)
            .collect()
    }
}

impl io::Write for Captured {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl<'a> MakeWriter<'a> for Captured {
    type Writer = Self;

    fn make_writer(&'a self) -> Self::Writer {
        self.clone()
    }
}

/// Starts `app` with the log captured and returns what startup wrote.
async fn startup_log<State>(app: RustStream<State>) -> Captured
where
    State: Send + Sync + 'static,
{
    let log = Captured::default();
    let _guard = log.install();
    let _tb = TestApp::start(app).await.expect("harness start");
    log
}

/// Asserts that `line` names the subscription, the reply type, the type's destination and the
/// ignored mount-site name.
fn assert_names(line: &str, subscription: &str, reply_type: &str, declared: &str, ignored: &str) {
    for field in [
        format!("subscription={subscription}"),
        format!("destination={declared}"),
        format!("ignored={ignored}"),
    ] {
        assert!(line.contains(&field), "`{field}` missing from: {line}");
    }
    assert!(
        line.contains("reply_type="),
        "reply type missing from: {line}"
    );
    assert!(
        line.contains(reply_type),
        "`{reply_type}` missing from: {line}"
    );
}

/// The attribute names a subject the reply type overrides: startup says so, naming both.
#[tokio::test]
async fn an_attribute_name_the_type_overrides_is_reported_at_startup() {
    let app = RustStream::new(AppInfo::new("reply-destination", "0.1.0")).with_broker(
        MemoryBroker::new(),
        |b| {
            b.include(contradicted);
        },
    );

    let log = startup_log(app).await;

    let warnings = log.ignored();
    assert_eq!(warnings.len(), 1, "{}", log.text());
    assert_names(
        &warnings[0],
        "dest.contradicted",
        "Confirmation",
        "dest.declared",
        "dest.unused",
    );
}

/// The same on the manual chain: `.to(..)` names a subject the reply type overrides.
#[tokio::test]
async fn a_chain_name_the_type_overrides_is_reported_at_startup() {
    let app = RustStream::new(AppInfo::new("reply-destination", "0.1.0")).with_broker(
        MemoryBroker::new(),
        |b| {
            b.include(
                subscriber("chain.contradicted", Confirm)
                    .reply()
                    .to("chain.unused")
                    .build(),
            );
        },
    );

    let log = startup_log(app).await;

    let warnings = log.ignored();
    assert_eq!(warnings.len(), 1, "{}", log.text());
    assert_names(
        &warnings[0],
        "chain.contradicted",
        "Confirmation",
        "dest.declared",
        "chain.unused",
    );
}

#[subscriber("batch.contradicted", reply("batch.unused"))]
async fn confirm_all(orders: &[Order]) -> Vec<Confirmation> {
    orders
        .iter()
        .map(|order| Confirmation { id: order.id })
        .collect()
}

/// A batch reply resolves per element type, so the batch form reports the same way.
#[tokio::test]
async fn a_batch_reply_name_the_type_overrides_is_reported_at_startup() {
    let app = RustStream::new(AppInfo::new("reply-destination", "0.1.0")).with_broker(
        MemoryBroker::new(),
        |b| {
            b.include(confirm_all.batch(nonzero!(4)));
        },
    );

    let log = startup_log(app).await;

    let warnings = log.ignored();
    assert_eq!(warnings.len(), 1, "{}", log.text());
    assert_names(
        &warnings[0],
        "batch.contradicted",
        "Confirmation",
        "dest.declared",
        "batch.unused",
    );
}

/// A reply that carries its own bytes and fixes its destination.
#[derive(Outgoing, Serialized)]
#[outgoing(name = "dest.frames")]
struct Frame(Vec<u8>);

#[subscriber("raw.contradicted", reply("raw.unused"))]
async fn frame(order: &Order) -> Frame {
    Frame(order.id.to_be_bytes().to_vec())
}

/// A byte-for-byte reply travels its own route, which reports the same way.
#[tokio::test]
async fn a_serialized_reply_name_the_type_overrides_is_reported_at_startup() {
    let app = RustStream::new(AppInfo::new("reply-destination", "0.1.0")).with_broker(
        MemoryBroker::new(),
        |b| {
            b.include(frame);
        },
    );

    let log = startup_log(app).await;

    let warnings = log.ignored();
    assert_eq!(warnings.len(), 1, "{}", log.text());
    assert_names(
        &warnings[0],
        "raw.contradicted",
        "Frame",
        "dest.frames",
        "raw.unused",
    );
}

/// The clause repeats the type's own destination.
#[subscriber("dest.agreed", reply("dest.declared"))]
async fn agreed(order: &Order) -> Confirmation {
    Confirmation { id: order.id }
}

/// Nothing is ignored where the mount site names the type's own destination, on either surface.
#[tokio::test]
async fn a_mount_site_name_equal_to_the_declared_one_is_not_reported() {
    let app = RustStream::new(AppInfo::new("reply-destination", "0.1.0")).with_broker(
        MemoryBroker::new(),
        |b| {
            b.include(agreed);
            b.include(
                subscriber("chain.agreed", Confirm)
                    .reply()
                    .to("dest.declared")
                    .build(),
            );
        },
    );

    let log = startup_log(app).await;

    assert_eq!(log.ignored(), Vec::<String>::new(), "{}", log.text());
}

/// Nothing is ignored where the mount site names nothing, on either surface.
#[tokio::test]
async fn a_bare_publish_is_not_reported() {
    let app = RustStream::new(AppInfo::new("reply-destination", "0.1.0")).with_broker(
        MemoryBroker::new(),
        |b| {
            b.include(bare);
            b.include(subscriber("chain.bare", Confirm).reply().build());
        },
    );

    let log = startup_log(app).await;

    assert_eq!(log.ignored(), Vec::<String>::new(), "{}", log.text());
}

/// A reply type declaring no destination takes the mount-site name, so nothing is ignored.
#[tokio::test]
async fn a_name_a_type_declaring_none_takes_is_not_reported() {
    let app = RustStream::new(AppInfo::new("reply-destination", "0.1.0")).with_broker(
        MemoryBroker::new(),
        |b| {
            b.include(named);
        },
    );

    let log = startup_log(app).await;

    assert_eq!(log.ignored(), Vec::<String>::new(), "{}", log.text());
}

#[derive(OutSlot)]
#[publishes(Order)]
struct Audit;

#[subscriber("slots.contradicted", reply("slots.unused"))]
async fn confirm_and_audit(order: &Order, Out(audit): Out<impl Publisher, Audit>) -> Confirmation {
    let _ = audit.message(order).to("slots.audit").publish().await;
    Confirmation { id: order.id }
}

/// A handler that also carries a slot mounts its reply through the slot arena, which reports
/// the same way.
#[tokio::test]
async fn a_slot_carrying_handler_reports_an_overridden_name_at_startup() {
    let app = RustStream::new(AppInfo::new("reply-destination", "0.1.0")).with_broker(
        MemoryBroker::new(),
        |b| {
            b.include(confirm_and_audit).out(Audit, Publish).build();
        },
    );

    let log = startup_log(app).await;

    let warnings = log.ignored();
    assert_eq!(warnings.len(), 1, "{}", log.text());
    assert_names(
        &warnings[0],
        "slots.contradicted",
        "Confirmation",
        "dest.declared",
        "slots.unused",
    );
}

#[subscriber("slots.batch", reply("slots.batch.unused"))]
async fn confirm_and_audit_all(
    orders: &[Order],
    Out(audit): Out<impl Publisher, Audit>,
) -> Vec<Confirmation> {
    for order in orders {
        let _ = audit.message(order).to("slots.audit").publish().await;
    }
    orders
        .iter()
        .map(|order| Confirmation { id: order.id })
        .collect()
}

/// The batch form of a slot-carrying handler reports the same way.
#[tokio::test]
async fn a_slot_carrying_batch_handler_reports_an_overridden_name_at_startup() {
    let app = RustStream::new(AppInfo::new("reply-destination", "0.1.0")).with_broker(
        MemoryBroker::new(),
        |b| {
            b.include(confirm_and_audit_all.batch(nonzero!(4)))
                .out(Audit, Publish)
                .build();
        },
    );

    let log = startup_log(app).await;

    let warnings = log.ignored();
    assert_eq!(warnings.len(), 1, "{}", log.text());
    assert_names(
        &warnings[0],
        "slots.batch",
        "Confirmation",
        "dest.declared",
        "slots.batch.unused",
    );
}
