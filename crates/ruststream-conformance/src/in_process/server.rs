//! The opt-in suites that hold the in-process transport to the server.

use std::{fmt, pin::pin, time::Duration};

use futures::{Stream, StreamExt};
use tokio::time::timeout;

use super::{DELIVERY_TIMEOUT, QUIET, connect_in_process, settle_ack};
use crate::helpers::unique_subject;
use ruststream_core::{
    Connected, ConnectedBroker, IncomingMessage, OutgoingMessage, Publisher, SendBroker,
    SendSource, SendSubscribe, Settle, Subscribe, Subscriber, SubscriptionSource,
};
use ruststream_runtime::testing::{Backlog, InProcess, TestableBroker};

/// How long a probe against the server waits for a delivery: a live subscription can take seconds
/// to be assigned its partitions or its queue.
const SERVER_TIMEOUT: Duration = Duration::from_secs(30);
/// How long a probe against the server waits before it concludes a refused publish was not
/// delivered after all.
const SERVER_QUIET: Duration = Duration::from_millis(500);

/// Which transport a probe runs against.
#[derive(Debug, Clone, Copy)]
enum Transport {
    /// [`Broker::connect`](ruststream_core::Broker::connect): the real server.
    Server,
    /// [`InProcess::connect_in_process`].
    InProcess,
}

impl Transport {
    async fn connect<B: InProcess + SendBroker>(self, broker: B) -> B::Connected {
        match self {
            Self::Server => broker
                .connect()
                .await
                .unwrap_or_else(|err| panic!("the broker must connect to its server: {err}")),
            Self::InProcess => connect_in_process(broker).await,
        }
    }

    const fn delivery_timeout(self) -> Duration {
        match self {
            Self::Server => SERVER_TIMEOUT,
            Self::InProcess => DELIVERY_TIMEOUT,
        }
    }

    const fn quiet(self) -> Duration {
        match self {
            Self::Server => SERVER_QUIET,
            Self::InProcess => QUIET,
        }
    }
}

impl fmt::Display for Transport {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Server => "against the server",
            Self::InProcess => "in process",
        })
    }
}

/// The next delivery on `stream`, within what `transport` allows.
async fn next_on<S, M, E>(stream: &mut S, transport: Transport, label: &str) -> M
where
    S: Stream<Item = Result<M, E>> + Unpin,
    M: Settle,
    E: fmt::Debug,
{
    timeout(transport.delivery_timeout(), stream.next())
        .await
        .unwrap_or_else(|_| panic!("{label} {transport}: no delivery arrived"))
        .unwrap_or_else(|| panic!("{label} {transport}: the stream ended"))
        .unwrap_or_else(|err| panic!("{label} {transport}: the stream yielded an error: {err:?}"))
}

/// Checks that the in-process transport's [`TestableBroker::backlog`] declaration is what the real
/// server does with a message published before a subscription opens.
///
/// [`run_suite`](crate::harness::run_suite) holds the in-process transport to its declaration; this
/// holds the declaration to the server. On each transport, a fresh subject gets one message through
/// the publisher `make_publisher` builds, then a subscription opened by name, then a second
/// message. Under [`Backlog::Delivered`] the subscription receives both, the earlier one first;
/// under [`Backlog::Missed`] it receives the later one only. The declaration is read off the
/// in-process connected form, configured by `make_broker` as the server's is. A server may refuse
/// the first publish (a name nothing declared yet); the in-process transport must refuse it too,
/// and on both the subscription must still open and receive the later message alone.
///
/// It connects with [`Broker::connect`](ruststream_core::Broker::connect), so run it where the broker's live suite runs.
///
/// # Examples
///
/// ```no_run
/// # async fn run() {
/// use ruststream::conformance::in_process;
/// use ruststream::memory::MemoryBroker;
///
/// in_process::backlog_matches_server(MemoryBroker::new, |connected| connected.publisher()).await;
/// # }
/// ```
///
/// # Panics
///
/// Panics when either transport delivers the earlier message against the declaration, when a
/// publish or the subscription fails, or when the later message does not arrive.
pub async fn backlog_matches_server<B, MkBroker, Pub, MkPub>(
    make_broker: MkBroker,
    make_publisher: MkPub,
) where
    B: InProcess + SendBroker,
    B::Connected: SendSubscribe + TestableBroker,
    MkBroker: Fn() -> B,
    Pub: Publisher,
    MkPub: Fn(&Connected<B>) -> Pub,
{
    let declared = {
        let connected = connect_in_process(make_broker()).await;
        let declared = connected.backlog();
        connected.shutdown().await.expect("shutdown failed");
        declared
    };

    // Why the first publish may fail: a server that refuses a publish to a name nothing has
    // declared yet keeps no backlog either, and the in-process transport must refuse it too.
    let mut server_refused = None;
    for transport in [Transport::Server, Transport::InProcess] {
        let connected = transport.connect(make_broker()).await;
        let subject = unique_subject("conformance.backlog");
        let publisher = make_publisher(&connected);
        let before = publisher
            .publish(
                OutgoingMessage::new(subject.as_str(), b"before-subscribe".as_slice()),
                None,
            )
            .await
            .map_err(|err| err.to_string());
        // A refused first publish still leaves the subscription and the later message to check:
        // the subscription must open and receive what is published once it is there.
        let refused_before = refused_before(transport, before, &mut server_refused, &subject);
        let publish = async |payload: &[u8]| {
            publisher
                .publish(OutgoingMessage::new(subject.as_str(), payload), None)
                .await
                .unwrap_or_else(|err| {
                    panic!(
                        "backlog_matches_server {transport}: a publish to {subject:?} failed: {err}"
                    )
                });
        };

        let mut subscriber = Subscribe::subscribe(&connected, &subject)
            .await
            .unwrap_or_else(|err| {
                panic!(
                    "backlog_matches_server {transport}: subscribing to {subject:?} failed: {err}"
                )
            });
        publish(b"after-subscribe").await;

        read_backlog(
            &mut pin!(subscriber.stream()),
            transport,
            declared,
            refused_before,
        )
        .await;
        drop(subscriber);
        connected.shutdown().await.expect("shutdown failed");
    }
}

/// Whether the publish before the subscription opened was refused, held to what the server did:
/// `server_refused` records the server's refusal, and the in-process transport must refuse
/// exactly where the server refused.
fn refused_before(
    transport: Transport,
    before: Result<(), String>,
    server_refused: &mut Option<String>,
    subject: &str,
) -> bool {
    match (transport, before, server_refused.as_deref()) {
        (Transport::Server, Err(reason), _) => {
            *server_refused = Some(reason);
            true
        }
        (Transport::InProcess, Ok(()), Some(reason)) => panic!(
            "backlog_matches_server: the server refused a publish to a name no subscription \
             had opened ({reason}), and the in-process transport accepted it; a test passes \
             where production fails"
        ),
        (Transport::InProcess, Err(_), Some(_)) => true,
        (Transport::InProcess, Err(reason), None) => panic!(
            "backlog_matches_server in process: a publish to {subject:?} was refused \
             ({reason}), and the server accepted it; a test fails where production succeeds"
        ),
        (_, Ok(()), _) => false,
    }
}

/// Reads what a subscription opened after the first publish receives: the earlier message first
/// where the transport keeps a backlog and the publish was taken, then the later one.
async fn read_backlog<S, M, E>(
    stream: &mut S,
    transport: Transport,
    declared: Backlog,
    refused_before: bool,
) where
    S: Stream<Item = Result<M, E>> + Unpin,
    M: Settle,
    E: fmt::Debug,
{
    let first = next_on(stream, transport, "backlog_matches_server").await;
    let expected: &[u8] = match declared {
        Backlog::Delivered if !refused_before => b"before-subscribe",
        _ => b"after-subscribe",
    };
    assert_eq!(
        first.payload(),
        expected,
        "backlog_matches_server {transport}: {}, so a subscription opened after a publish \
         must first receive {:?}",
        if refused_before {
            "the publish before the subscription opened was refused".to_owned()
        } else {
            format!("the in-process transport declares `Backlog::{declared:?}`")
        },
        String::from_utf8_lossy(expected),
    );
    settle_ack(first).await;
    if expected == b"before-subscribe" {
        let second = next_on(stream, transport, "backlog_matches_server").await;
        assert_eq!(
            second.payload(),
            b"after-subscribe",
            "backlog_matches_server {transport}: the message published after the \
             subscription opened comes next",
        );
        settle_ack(second).await;
    }
}

/// One thing the real server refuses, which [`refuses_like_the_server`] checks the in-process
/// transport refuses too.
///
/// `Src` is the broker's subscription descriptor; a list with no subscription probe names it with
/// a turbofish (`Refusal::<MySource>::Publish { .. }`).
///
/// # Examples
///
/// ```no_run
/// # async fn run() {
/// use ruststream::conformance::in_process::{self, Refusal};
/// use ruststream::memory::{MemoryBroker, MemorySource};
///
/// // What the server refuses, and so what the in-process transport must refuse too.
/// in_process::refuses_like_the_server(
///     MemoryBroker::new,
///     |connected| connected.publisher(),
///     [
///         Refusal::Subscription {
///             source: MemorySource::new("orders.*"),
///         },
///         Refusal::PayloadOver {
///             name: "orders".to_owned(),
///             limit: 1024 * 1024,
///         },
///     ],
/// )
/// .await;
/// # }
/// ```
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum Refusal<Src> {
    /// A publish to `name` of exactly `limit` payload bytes is accepted and delivered, one byte
    /// more is refused. The check opens a subscription to `name` by name before it publishes.
    PayloadOver {
        /// A destination both transports accept publishes to.
        name: String,
        /// The largest payload the server accepts there, in bytes.
        limit: usize,
    },
    /// A publish to `name` is refused: a name the server's grammar rejects, or a destination it
    /// does not have.
    Publish {
        /// The refused destination.
        name: String,
    },
    /// Opening `source` is refused: an invalid name, a declaration the server rejects.
    Subscription {
        /// The refused subscription.
        source: Src,
    },
    /// With `open` subscribed, opening `refused` is refused: a redeclaration that conflicts with
    /// the first one, a second consumer where the server admits one.
    Conflicting {
        /// The subscription opened first, which the server accepts.
        open: Src,
        /// The subscription the server refuses beside it.
        refused: Src,
    },
}

/// Checks that the in-process transport refuses what the real server refuses.
///
/// Each [`Refusal`] runs on a fresh connection to the server first, then on a fresh in-process
/// connection, and both must refuse it. Then a subscription to one name, a second one to it and
/// one to another name must be admitted or refused alike by both, since the in-process checks of
/// [`run_suite`](crate::harness::run_suite) read routing over what the transport admits. A probe the server accepts fails as such, before the
/// in-process transport is blamed: it proves nothing about it. [`Refusal::PayloadOver`] also
/// requires a payload of exactly the limit to be accepted and delivered on both, so a transport
/// that refuses everything, or refuses one byte early, fails too.
///
/// `make_broker` builds the broker configured as a deployment configures it, and `make_publisher`
/// the publisher the probes publish through. It connects with [`Broker::connect`](ruststream_core::Broker::connect), so run it where
/// the broker's live suite runs.
///
/// # Examples
///
/// ```no_run
/// # async fn run() {
/// use ruststream::conformance::in_process::{self, Refusal};
/// use ruststream::memory::{MemoryBroker, MemorySource};
///
/// in_process::refuses_like_the_server(
///     MemoryBroker::new,
///     |connected| connected.publisher(),
///     [Refusal::Subscription {
///         source: MemorySource::new("orders.*"),
///     }],
/// )
/// .await;
/// # }
/// ```
///
/// # Panics
///
/// Panics when `refusals` is empty, when the server accepts a probe, when the in-process
/// transport accepts one the server refused, and when the two admit different subscriptions.
pub async fn refuses_like_the_server<B, MkBroker, Src, Pub, MkPub>(
    make_broker: MkBroker,
    make_publisher: MkPub,
    refusals: impl IntoIterator<Item = Refusal<Src>> + Send,
) where
    B: InProcess + SendBroker,
    B::Connected: SendSubscribe + TestableBroker,
    MkBroker: Fn() -> B + Send,
    Src: SendSource<Connected<B>> + Clone + Send + Sync,
    Pub: Publisher,
    MkPub: Fn(&Connected<B>) -> Pub + Send + Sync,
{
    let refusals: Vec<Refusal<Src>> = refusals.into_iter().collect();
    assert!(
        !refusals.is_empty(),
        "pass at least one probe the server refuses; an empty list passes whatever the in-process \
         transport accepts",
    );
    for refusal in &refusals {
        let mut refused = [false; 2];
        for (leg, transport) in [Transport::Server, Transport::InProcess]
            .into_iter()
            .enumerate()
        {
            let connected = transport.connect(make_broker()).await;
            refused[leg] = probe(&connected, refusal, transport, &make_publisher).await;
            connected.shutdown().await.expect("shutdown failed");
        }
        let what = describe::<Connected<B>, Src>(refusal);
        match refused {
            [true, true] => {}
            [true, false] => panic!(
                "refuses_like_the_server: {what}: the server refused it and the in-process \
                 transport accepted it; a test passes where production fails"
            ),
            [false, true] => panic!(
                "refuses_like_the_server: {what}: the in-process transport refused it and the \
                 server accepted it; a test fails where production succeeds"
            ),
            [false, false] => panic!(
                "refuses_like_the_server: {what}: the server accepted it, so it proves nothing; \
                 pass a probe the server refuses"
            ),
        }
    }
    admits_like_the_server(make_broker).await;
}

/// Opens a subscription to one name, a second one to it and one to another name, on each
/// transport: the in-process scenarios check routing over the subscriptions the transport admits,
/// so what it admits must be what the server admits.
async fn admits_like_the_server<B, MkBroker>(make_broker: MkBroker)
where
    B: InProcess + SendBroker,
    B::Connected: SendSubscribe + TestableBroker,
    MkBroker: Fn() -> B + Send,
{
    let mut admitted = [[false; 3]; 2];
    for (leg, transport) in [Transport::Server, Transport::InProcess]
        .into_iter()
        .enumerate()
    {
        let connected = transport.connect(make_broker()).await;
        let name = unique_subject("conformance.admitted");
        let other = unique_subject("conformance.admitted.elsewhere");
        let mut opened = Vec::new();
        for (slot, subject) in [&name, &name, &other].into_iter().enumerate() {
            if let Ok(subscriber) = Subscribe::subscribe(&connected, subject).await {
                admitted[leg][slot] = true;
                opened.push(subscriber);
            }
        }
        drop(opened);
        connected.shutdown().await.expect("shutdown failed");
    }
    let [server, in_process] = admitted;
    assert_eq!(
        in_process, server,
        "refuses_like_the_server: a subscription to a name, a second one to it and one to another \
         name are admitted as {in_process:?} in process and as {server:?} by the server; the \
         in-process transport must admit what the server admits, or a test passes or fails where \
         production does the opposite",
    );
}

/// The probe as a failure names it.
fn describe<C, Src>(refusal: &Refusal<Src>) -> String
where
    C: ConnectedBroker,
    Src: SubscriptionSource<C>,
{
    match refusal {
        Refusal::PayloadOver { name, limit } => format!(
            "a payload of {} bytes to {name:?}, one over the limit",
            limit + 1
        ),
        Refusal::Publish { name } => format!("a publish to {name:?}"),
        Refusal::Subscription { source } => format!("the subscription {:?}", source.name()),
        Refusal::Conflicting { open, refused } => format!(
            "the subscription {:?} beside {:?}",
            refused.name(),
            open.name()
        ),
    }
}

/// Runs one refusal probe on `connected`, answering whether it was refused.
async fn probe<C, Src, Pub, MkPub>(
    connected: &C,
    refusal: &Refusal<Src>,
    transport: Transport,
    make_publisher: &MkPub,
) -> bool
where
    C: SendSubscribe,
    Src: SubscriptionSource<C> + Clone + Sync,
    Pub: Publisher,
    MkPub: Fn(&C) -> Pub + Sync,
{
    match refusal {
        Refusal::PayloadOver { name, limit } => {
            probe_payload_over(connected, name, *limit, transport, make_publisher).await
        }
        Refusal::Publish { name } => make_publisher(connected)
            .publish(
                OutgoingMessage::new(name.as_str(), b"refused".as_slice()),
                None,
            )
            .await
            .is_err(),
        Refusal::Subscription { source } => source.clone().subscribe(connected).await.is_err(),
        Refusal::Conflicting { open, refused } => {
            let first = open.clone().subscribe(connected).await.unwrap_or_else(|err| {
                panic!(
                    "refuses_like_the_server {transport}: the subscription {:?} opened first was \
                     refused: {err}",
                    open.name(),
                )
            });
            let refused = refused.clone().subscribe(connected).await.is_err();
            drop(first);
            refused
        }
    }
}

/// Runs a payload-size probe on `connected`: a payload of exactly `limit` bytes to `name` is
/// delivered, and one byte more is refused, answering whether it was.
async fn probe_payload_over<C, Pub, MkPub>(
    connected: &C,
    name: &str,
    limit: usize,
    transport: Transport,
    make_publisher: &MkPub,
) -> bool
where
    C: SendSubscribe,
    Pub: Publisher,
    MkPub: Fn(&C) -> Pub + Sync,
{
    let mut subscriber = Subscribe::subscribe(connected, name)
        .await
        .unwrap_or_else(|err| {
            panic!("refuses_like_the_server {transport}: subscribing to {name:?} failed: {err}")
        });
    let publisher = make_publisher(connected);
    let at_limit = vec![b'x'; limit];
    if let Err(err) = publisher
        .publish(OutgoingMessage::new(name, at_limit.as_slice()), None)
        .await
    {
        panic!(
            "refuses_like_the_server {transport}: a payload of exactly {limit} bytes to \
             {name:?} was refused: {err}"
        );
    }
    let over = vec![b'x'; limit + 1];
    let refused = publisher
        .publish(OutgoingMessage::new(name, over.as_slice()), None)
        .await
        .is_err();
    let mut stream = pin!(subscriber.stream());
    let delivered = next_on(&mut stream, transport, "refuses_like_the_server").await;
    assert_eq!(
        delivered.payload().len(),
        limit,
        "refuses_like_the_server {transport}: the payload of exactly the limit must be \
         delivered first",
    );
    settle_ack(delivered).await;
    if refused {
        assert!(
            timeout(transport.quiet(), stream.next()).await.is_err(),
            "refuses_like_the_server {transport}: the payload over the limit was refused, \
             and delivered anyway",
        );
    }
    refused
}
