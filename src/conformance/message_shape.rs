//! What a message carries through the broker's own publisher: its place in the order, its
//! headers, its key and its per-message settings.
//!
//! [`harness::lifecycle`](super::harness::lifecycle) runs the part every broker owes with no input
//! of its own: messages published one after another arrive in that order on one subscription, and
//! headers come back byte for byte, or the publish that carries them is refused. The rest needs
//! something only the broker can supply, so each part is a function the broker crate calls:
//!
//! * [`keyed_order`]: a keyed message reports its key on delivery, and one key keeps its order;
//!   the broker says where a key goes.
//! * [`publish_options`]: [`Publisher::Options`] resolve over the policy's defaults, and a value
//!   the transport cannot honour is a publish error; the broker supplies the values and a way to
//!   read their effect off a delivery.
//! * [`publishes_without_credentials`] and [`describes_addresses_without_credentials`]: the
//!   credential scan of [`harness::describes_without_credentials`] applied to a publish policy's
//!   bindings and to a server configured with several addresses.
//!
//! Each runs live and, through [`InProcessBroker`](super::harness::InProcessBroker), in process.
//!
//! [`harness::describes_without_credentials`]: super::harness::describes_without_credentials

use std::{fmt, time::Duration};

use bytes::Bytes;
use futures::{Stream, StreamExt};
use tokio::time::timeout;

use super::helpers::unique_subject;
#[cfg(feature = "asyncapi")]
use crate::DescribeServer;
#[cfg(feature = "asyncapi")]
use crate::asyncapi::build_spec;
#[cfg(feature = "asyncapi")]
use crate::runtime::{AppInfo, RustStream};
use crate::{
    AckError, Broker, Connected, ConnectedBroker, HeaderMap, IncomingMessage, OutgoingMessage,
    PublishPolicy, Publisher, Subscriber, SubscriptionSource, runtime::RETRY_COUNT_HEADER,
};

/// How long a check waits for a delivery it expects. Long enough for a live consumer that joins a
/// group on its first poll; a correct broker never comes near it, it only bounds a failing run.
const DELIVERY_TIMEOUT: Duration = Duration::from_secs(10);
/// How many messages the order check publishes one after another.
const ORDERED: usize = 32;
/// How many entries the header check puts on one message.
const MANY_HEADERS: usize = 24;
/// How many messages [`keyed_order`] publishes under each key.
const PER_KEY: usize = 8;
/// The keys [`keyed_order`] interleaves.
const KEYS: [&str; 3] = [
    "conformance-key-a",
    "conformance-key-b",
    "conformance-key-c",
];
/// A W3C trace context as the tracing layer writes it.
const TRACEPARENT: (&str, &str) = (
    "traceparent",
    "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
);
/// Bytes that are not UTF-8 (a lone continuation byte, a truncated sequence, `0xff`), with a NUL
/// and line breaks a text encoding would mangle.
const BINARY_VALUE: &[u8] = &[0x00, 0x80, b'\r', b'\n', 0xfe, 0xff, 0xc3];

/// The message type a subscriber yields.
type SubscriberMessage<S> = <S as Subscriber>::Message;

/// Messages published one after another through the broker's own publisher arrive in that order,
/// and headers survive the transport byte for byte, or the publish carrying them is refused.
///
/// Run first by [`harness::lifecycle`](super::harness::lifecycle), on a connection of its own
/// that it closes before the ladder opens its own. The factories come back so the ladder goes on
/// with them: holding them by reference across the awaits would make the ladder's future `Send`
/// only for factories that are also `Sync`.
pub(crate) async fn publisher_carries<B, MkBroker, Src, MkSrc, Pub, MkPub>(
    make_broker: MkBroker,
    make_source: MkSrc,
    make_publisher: MkPub,
) -> (MkBroker, MkSrc, MkPub)
where
    B: Broker,
    MkBroker: Fn() -> B,
    Src: SubscriptionSource<Connected<B>> + Send,
    Src::Subscriber: Send,
    MkSrc: Fn(&str) -> Src,
    Pub: Publisher,
    MkPub: Fn(&Connected<B>) -> Pub,
{
    // Under the lifecycle prefix, because this runs inside `lifecycle` and a broker whose
    // subscriptions need a resource of their own (a stream, a queue) declares it for that prefix.
    let subject = unique_subject("conformance.lifecycle");
    let connected = make_broker()
        .connect()
        .await
        .expect("message shape: broker must connect after synchronous construction");
    let mut subscriber = make_source(&subject)
        .subscribe(&connected)
        .await
        .expect("message shape: subscription source must open against the connected form");
    let publisher = make_publisher(&connected);
    {
        let mut stream = std::pin::pin!(subscriber.stream());
        publish_order(&publisher, &mut stream, &subject).await;
        header_round_trip(&publisher, &mut stream, &subject).await;
    }

    // Closed before the ladder subscribes: a consumer group that still counts this subscription
    // as a member would hold the ladder's subscription back until the member timed out.
    drop(subscriber);
    super::lifecycle::shutdown_within(connected, "lifecycle").await;
    (make_broker, make_source, make_publisher)
}

async fn publish_order<Pub, S, M, E>(publisher: &Pub, stream: &mut S, subject: &str)
where
    Pub: Publisher,
    S: Stream<Item = Result<M, E>> + Unpin,
    M: IncomingMessage,
    E: fmt::Debug,
{
    for index in 0..ORDERED {
        let payload = format!("order:{index:04}");
        publisher
            .publish(OutgoingMessage::new(subject, payload.as_bytes()), None)
            .await
            .unwrap_or_else(|err| {
                panic!(
                    "publish order: publish {index} through the broker's publisher failed: {err}"
                )
            });
    }
    for index in 0..ORDERED {
        let expected = format!("order:{index:04}");
        let msg = expect_delivery(stream, &format!("publish order: waiting for {expected}")).await;
        assert_eq!(
            String::from_utf8_lossy(msg.payload()),
            expected,
            "publish order: messages published one after another through the broker's own \
             publisher must arrive in publish order on one subscription",
        );
        settle(msg, "publish order").await;
    }
}

/// One message's worth of headers, and what the check calls it in a failure.
struct HeaderCase {
    label: &'static str,
    headers: HeaderMap,
}

fn header_cases() -> [HeaderCase; 4] {
    let mut many = HeaderMap::new();
    for index in 0..MANY_HEADERS {
        many.insert(
            format!("x-conformance-entry-{index:02}"),
            format!("value-{index}"),
        );
    }
    let mut empty = HeaderMap::new();
    empty.insert("x-conformance-empty", Bytes::new());
    empty.insert("x-conformance-present", "yes");
    let mut binary = HeaderMap::new();
    binary.insert("x-conformance-binary", Bytes::from_static(BINARY_VALUE));
    let mut framework = HeaderMap::new();
    framework.insert(RETRY_COUNT_HEADER, "2");
    framework.insert(TRACEPARENT.0, TRACEPARENT.1);
    [
        HeaderCase {
            label: "many entries",
            headers: many,
        },
        HeaderCase {
            label: "an empty value",
            headers: empty,
        },
        HeaderCase {
            label: "a non-UTF-8 value",
            headers: binary,
        },
        HeaderCase {
            label: "the framework's retry count and trace context",
            headers: framework,
        },
    ]
}

async fn header_round_trip<Pub, S, M, E>(publisher: &Pub, stream: &mut S, subject: &str)
where
    Pub: Publisher,
    S: Stream<Item = Result<M, E>> + Unpin,
    M: IncomingMessage,
    E: fmt::Debug,
{
    let mut accepted = Vec::new();
    let mut refused = Vec::new();
    for (index, case) in header_cases().into_iter().enumerate() {
        let payload = format!("headers:{index}");
        let msg =
            OutgoingMessage::new(subject, payload.as_bytes()).with_headers(case.headers.clone());
        // Refusing is the honest answer of a transport that cannot carry these headers; what the
        // check holds it to is that a refused message never arrives.
        match publisher.publish(msg, None).await {
            Ok(()) => accepted.push((payload, case)),
            Err(_) => refused.push((payload, case.label)),
        }
    }
    publisher
        .publish(
            OutgoingMessage::new(subject, b"headers:end".as_slice()),
            None,
        )
        .await
        .unwrap_or_else(|err| {
            panic!("header round trip: a publish with no headers must succeed, got: {err}")
        });

    for (payload, case) in accepted {
        let label = format!(
            "header round trip: waiting for the accepted publish carrying {}",
            case.label
        );
        let msg = expect_delivery(stream, &label).await;
        if let Some((_, label)) = refused.iter().find(|(p, _)| p.as_bytes() == msg.payload()) {
            panic!(
                "header round trip: the publish carrying {label} was refused, and the message \
                 arrived anyway"
            );
        }
        assert_eq!(
            String::from_utf8_lossy(msg.payload()),
            payload,
            "header round trip: the messages carrying headers must arrive in publish order",
        );
        for (name, value) in case.headers.iter() {
            assert_eq!(
                msg.headers().get(name),
                Some(value),
                "header round trip ({}): header {name:?} must arrive byte for byte; a transport \
                 that cannot carry it must refuse the publish instead of dropping or rewriting it",
                case.label,
            );
        }
        settle(msg, "header round trip").await;
    }
    let last = expect_delivery(
        stream,
        "header round trip: waiting for the publish with no headers that follows the rest",
    )
    .await;
    if let Some((_, label)) = refused.iter().find(|(p, _)| p.as_bytes() == last.payload()) {
        panic!(
            "header round trip: the publish carrying {label} was refused, and the message arrived \
             anyway"
        );
    }
    assert_eq!(
        last.payload(),
        b"headers:end",
        "header round trip: the message with no headers must arrive after the ones before it",
    );
    settle(last, "header round trip").await;
}

/// Verifies that a keyed message reports its key on delivery and that one key keeps its order.
///
/// Three keys are interleaved, eight messages each, through the broker's own publisher. Every
/// delivery on the one subscription must report the key it was published under through
/// [`IncomingMessage::partition_key`], and the messages of one key must arrive in the order they
/// were published. The order between different keys is free: a partitioned transport spreads
/// keys over partitions and reads them back in any interleaving.
///
/// `subject` is the name the check subscribes and publishes under. Build it with
/// [`unique_subject`], so a rerun against one server reads only its own messages, in whatever form
/// the transport needs for its keys to exist (an SQS FIFO queue is a name ending in `.fifo`).
///
/// `key_with` is where the broker says how a publish carries a key: it writes the key into the
/// message's headers, or returns the [`Publisher::Options`] that carry it, or both. A transport
/// with no key has nothing to report here and does not run this check; one that has keys and
/// reports `None` fails it, because the runtime's keyed worker lanes (`workers(n, by_key)`) read
/// the key from there.
///
/// # Examples
///
/// ```no_run
/// # #[cfg(feature = "memory")]
/// # async fn run() {
/// use ruststream::Bytes;
/// use ruststream::conformance::{helpers::unique_subject, message_shape};
/// use ruststream::memory::{MemoryBroker, MemorySource, PARTITION_KEY_HEADER};
///
/// message_shape::keyed_order(
///     MemoryBroker::new,
///     &unique_subject("conformance.keyed"),
///     |name| MemorySource::new(name),
///     |connected| connected.publisher(),
///     // The in-memory broker carries a key in a header, and has no per-message options.
///     |key, headers| {
///         headers.insert(PARTITION_KEY_HEADER, Bytes::copy_from_slice(key));
///         None
///     },
/// )
/// .await;
/// # }
/// ```
///
/// # Panics
///
/// Panics with a descriptive message when a keyed publish fails, a delivery reports another key
/// or none, or a key's messages arrive out of order.
pub async fn keyed_order<B, MkBroker, Src, MkSrc, Pub, MkPub, KeyWith>(
    make_broker: MkBroker,
    subject: &str,
    make_source: MkSrc,
    make_publisher: MkPub,
    key_with: KeyWith,
) where
    B: Broker,
    MkBroker: Fn() -> B,
    Src: SubscriptionSource<Connected<B>> + Send,
    Src::Subscriber: Send,
    MkSrc: Fn(&str) -> Src,
    Pub: Publisher,
    MkPub: Fn(&Connected<B>) -> Pub,
    KeyWith: Fn(&[u8], &mut HeaderMap) -> Option<Pub::Options>,
{
    let connected = make_broker()
        .connect()
        .await
        .expect("keyed order: broker must connect after synchronous construction");
    let mut subscriber = make_source(subject)
        .subscribe(&connected)
        .await
        .expect("keyed order: subscription source must open against the connected form");
    let publisher = make_publisher(&connected);

    for seq in 0..PER_KEY {
        for key in KEYS {
            let payload = format!("{key}:{seq:02}");
            let mut headers = HeaderMap::new();
            let options = key_with(key.as_bytes(), &mut headers);
            let msg = OutgoingMessage::new(subject, payload.as_bytes()).with_headers(headers);
            publisher
                .publish(msg, options.as_ref())
                .await
                .unwrap_or_else(|err| panic!("keyed order: publishing {payload} failed: {err}"));
        }
    }

    {
        let mut next = [0usize; KEYS.len()];
        let mut stream = std::pin::pin!(subscriber.stream());
        for _ in 0..PER_KEY * KEYS.len() {
            let msg = expect_delivery(&mut stream, "keyed order").await;
            let payload = String::from_utf8_lossy(msg.payload()).into_owned();
            let (key, seq) = payload
                .rsplit_once(':')
                .and_then(|(key, seq)| Some((key, seq.parse::<usize>().ok()?)))
                .unwrap_or_else(|| panic!("keyed order: a delivery nobody published: {payload:?}"));
            let slot = KEYS
                .iter()
                .position(|known| *known == key)
                .unwrap_or_else(|| panic!("keyed order: a delivery nobody published: {payload:?}"));
            assert_eq!(
                msg.partition_key(),
                Some(key.as_bytes()),
                "keyed order: {payload} was published under key {key:?}; the delivery must report \
                 that key from partition_key()",
            );
            assert_eq!(
                seq, next[slot],
                "keyed order: the messages of key {key:?} must arrive in publish order",
            );
            next[slot] += 1;
            settle(msg, "keyed order").await;
        }
    }
    drop(subscriber);

    let _closed = connected
        .shutdown()
        .await
        .expect("keyed order: broker must shut down cleanly");
}

/// The per-message settings [`publish_options`] publishes with, and what each must look like on
/// the delivery.
///
/// `Options` is the broker's [`Publisher::Options`]; `Observed` is whatever the broker reads off
/// a delivery to see a setting's effect (a priority, a `QoS`, an ordering key).
///
/// # Examples
///
/// ```
/// use ruststream::conformance::message_shape::OptionCases;
///
/// /// A broker's options: every field optional, as the contract asks.
/// #[derive(Clone)]
/// struct Priority {
///     priority: Option<u8>,
/// }
///
/// // The policy under test defaults to priority 5; a call may lower it to 1; 42 is past the
/// // transport's range and must be refused.
/// let cases = OptionCases::new(Some(5))
///     .overrides(Priority { priority: Some(1) }, Some(1))
///     .refuses(Priority { priority: Some(42) });
/// # let _ = cases;
/// ```
#[derive(Debug, Clone)]
pub struct OptionCases<Options, Observed> {
    policy_default: Observed,
    overrides: Vec<(Options, Observed)>,
    refused: Vec<Options>,
}

impl<Options, Observed> OptionCases<Options, Observed> {
    /// What a publish with no options (`None`) must show: the setting the policy fixed.
    ///
    /// Configure the policy with a value the transport would not pick by itself, so a publisher
    /// that forgets the policy cannot pass by landing on the transport's own default.
    ///
    /// # Examples
    ///
    /// ```
    /// use ruststream::conformance::message_shape::OptionCases;
    ///
    /// let cases: OptionCases<(), &str> = OptionCases::new("policy default");
    /// # let _ = cases;
    /// ```
    #[must_use]
    pub const fn new(policy_default: Observed) -> Self {
        Self {
            policy_default,
            overrides: Vec::new(),
            refused: Vec::new(),
        }
    }

    /// Adds a call whose `options` must win over the policy, and what the delivery then shows.
    ///
    /// Set one field and leave the rest unset in some of them: a field the call leaves unset
    /// keeps the policy's value, and `observed` says so.
    ///
    /// # Examples
    ///
    /// ```
    /// use ruststream::conformance::message_shape::OptionCases;
    ///
    /// let cases = OptionCases::new(5u8).overrides(Some(1u8), 1);
    /// # let _ = cases;
    /// ```
    #[must_use]
    pub fn overrides(mut self, options: Options, observed: Observed) -> Self {
        self.overrides.push((options, observed));
        self
    }

    /// Adds a value the transport cannot honour: the publish carrying it must fail.
    ///
    /// A broker whose every value is honourable adds none.
    ///
    /// # Examples
    ///
    /// ```
    /// use ruststream::conformance::message_shape::OptionCases;
    ///
    /// let cases = OptionCases::new(5u8).overrides(Some(1u8), 1).refuses(Some(42));
    /// # let _ = cases;
    /// ```
    #[must_use]
    pub fn refuses(mut self, options: Options) -> Self {
        self.refused.push(options);
        self
    }
}

/// Verifies that a publisher's per-message settings resolve over its policy, and that a value
/// the transport cannot honour fails the publish.
///
/// The live publisher comes from pairing `policy` with the connected broker, the way the runtime
/// builds it. Then, through that publisher:
///
/// * a publish with `None` shows the policy's setting ([`OptionCases::new`]);
/// * each override shows its own setting, and the next publish with `None` shows the policy's
///   setting again, so a call adjusts only itself ([`OptionCases::overrides`]);
/// * each refused value fails the publish, and the message never arrives
///   ([`OptionCases::refuses`]); substituting a default for it would be a silent fallback.
///
/// `observe` reads the setting's effect off a delivery, in whatever form the broker exposes it.
///
/// # Examples
///
/// The in-memory broker has no per-message settings, so its run is a single unit case. A broker
/// with settings passes its own policy, configured away from the transport default, and cases
/// that exercise every field.
///
/// ```no_run
/// # #[cfg(feature = "memory")]
/// # async fn run() {
/// use ruststream::conformance::message_shape::{self, OptionCases};
/// use ruststream::memory::{MemoryBroker, MemoryPublish, MemorySource};
///
/// message_shape::publish_options(
///     MemoryBroker::new,
///     |name| MemorySource::new(name),
///     MemoryPublish,
///     OptionCases::new(()).overrides((), ()),
///     |_delivery| (),
/// )
/// .await;
/// # }
/// ```
///
/// # Panics
///
/// Panics with a descriptive message when `cases` has no override (a check with nothing to
/// override proves nothing), when pairing or a publish that must succeed fails, when a delivery
/// shows another setting than expected, or when a refused value is accepted or arrives.
pub async fn publish_options<B, MkBroker, Src, MkSrc, Policy, Observed, Observe>(
    make_broker: MkBroker,
    make_source: MkSrc,
    policy: Policy,
    cases: OptionCases<<Policy::Live as Publisher>::Options, Observed>,
    observe: Observe,
) where
    B: Broker,
    MkBroker: Fn() -> B,
    Src: SubscriptionSource<Connected<B>> + Send,
    Src::Subscriber: Send,
    MkSrc: Fn(&str) -> Src,
    Policy: PublishPolicy<Connected<B>>,
    Policy::Live: Publisher,
    Observed: PartialEq + fmt::Debug + Sync,
    Observe: Fn(&SubscriberMessage<Src::Subscriber>) -> Observed + Sync,
{
    assert!(
        !cases.overrides.is_empty(),
        "publish options: pass at least one override; with none the check cannot tell a \
         publisher that ignores the call's options from one that honours them",
    );
    let subject = unique_subject("conformance.options");
    let connected = make_broker()
        .connect()
        .await
        .expect("publish options: broker must connect after synchronous construction");
    let mut subscriber = make_source(&subject)
        .subscribe(&connected)
        .await
        .expect("publish options: subscription source must open against the connected form");
    let publisher = policy
        .pair(&connected)
        .await
        .expect("publish options: the policy must pair with the connected broker");
    {
        let mut stream = std::pin::pin!(subscriber.stream());

        let (publisher, observe, subject) = (&publisher, &observe, subject.as_str());
        let mut round = 0usize;
        let mut expect = async move |options: Option<&<Policy::Live as Publisher>::Options>,
                                     expected: &Observed,
                                     what: &str| {
            let payload = format!("options:{round}");
            round += 1;
            publisher
                .publish(OutgoingMessage::new(subject, payload.as_bytes()), options)
                .await
                .unwrap_or_else(|err| panic!("publish options: {what}: the publish failed: {err}"));
            let msg = expect_delivery(&mut stream, &format!("publish options: {what}")).await;
            assert_eq!(
                String::from_utf8_lossy(msg.payload()),
                payload,
                "publish options: {what}: a message arrived out of order, or one that was refused",
            );
            let observed = observe(&msg);
            assert_eq!(
                &observed, expected,
                "publish options: {what}: the delivery shows another setting",
            );
            settle(msg, "publish options").await;
        };

        expect(
            None,
            &cases.policy_default,
            "a publish with no options takes the policy's",
        )
        .await;
        for (index, (options, observed)) in cases.overrides.iter().enumerate() {
            let what = format!("override {index} must win over the policy");
            expect(Some(options), observed, &what).await;
            let what =
                format!("the publish after override {index} takes the policy's setting again");
            expect(None, &cases.policy_default, &what).await;
        }
        for (index, options) in cases.refused.iter().enumerate() {
            let refused = publisher
                .publish(
                    OutgoingMessage::new(subject, b"options:refused".as_slice()),
                    Some(options),
                )
                .await;
            assert!(
                refused.is_err(),
                "publish options: refused value {index} was accepted; a value the transport cannot \
                 honour must fail the publish, never fall back to another setting",
            );
        }
        // The refused publishes sit before this one, so a refused message that arrived anyway is the
        // next delivery and fails the payload assertion.
        expect(
            None,
            &cases.policy_default,
            "the publish after the refused values",
        )
        .await;
    }
    drop(subscriber);

    let _closed = connected
        .shutdown()
        .await
        .expect("publish options: broker must shut down cleanly");
}

/// Fails when a publish policy's bindings carry `secret`.
///
/// A policy describes the positions it is bound on (a reply, an [`Out`](crate::runtime::Out)
/// slot, a dead-letter destination) in the generated document, which is published and shared.
/// Configure `policy` the way a deployment would, with a password you pass as `secret`, and this
/// scans its channel, operation and message bindings and its reply address location.
///
/// # Examples
///
/// ```
/// # #[cfg(all(feature = "asyncapi", feature = "memory"))]
/// # fn demo() {
/// use ruststream::conformance::message_shape;
/// use ruststream::memory::{ConnectedMemoryBroker, MemoryPublish};
///
/// message_shape::publishes_without_credentials::<ConnectedMemoryBroker, _>(
///     &MemoryPublish,
///     "hunter2",
/// );
/// # }
/// ```
///
/// # Panics
///
/// Panics when `secret` appears in any binding or the reply address location, and when `secret`
/// is empty (a scan for nothing passes for the wrong reason).
#[cfg(feature = "asyncapi")]
pub fn publishes_without_credentials<C, Policy>(policy: &Policy, secret: &str)
where
    C: ConnectedBroker,
    Policy: PublishPolicy<C>,
{
    assert!(
        !secret.is_empty(),
        "pass the password the policy was configured with; scanning for an empty string passes \
         whatever the policy does",
    );
    let channel = "conformance.credentials";
    let described = serde_json::to_string(&serde_json::json!({
        "channel": policy.channel_bindings(channel),
        "operation": policy.operation_bindings(channel),
        "message": policy.message_bindings(channel),
        "reply": policy.reply_address_location(),
    }))
    .expect("a binding body is serialized once at construction, so it serializes again here");
    assert!(
        !described.contains(secret),
        "the publish policy's bindings carry the password. A published document is shared: keep \
         credentials out of every binding body. Got: {described}",
    );
}

/// Fails when a broker configured with several addresses describes any of them with its
/// credentials.
///
/// A broker that connects to a cluster describes its addresses in one server description, and
/// each address is a URL that may carry a user and a password. `make_broker` receives three such
/// URLs under `scheme`, each with the same user and password, and builds the broker the way a
/// deployment would from them (joining them into one setting where the broker takes a list).
/// The document built from its [`DescribeServer`] must carry none of the userinfo, whichever
/// addresses it names, and must name at least one of them with its port (what
/// [`ServerSpec::host_from_url`](crate::ServerSpec::host_from_url) returns for it): a
/// description that names no address passes the scan for the wrong reason.
///
/// # Examples
///
/// ```
/// # #[cfg(all(feature = "asyncapi", feature = "memory"))]
/// # fn demo() {
/// use std::future::Future;
///
/// use ruststream::conformance::message_shape;
/// use ruststream::memory::{ConnectedMemoryBroker, MemoryBroker, MemoryError};
/// use ruststream::{Broker, DescribeServer, ServerSpec};
///
/// /// A broker configured with a list of addresses, described the way the contract asks.
/// struct Cluster {
///     addrs: Vec<String>,
/// }
///
/// impl Broker for Cluster {
///     type Error = MemoryError;
///     type Connected = ConnectedMemoryBroker;
///
///     fn connect(self) -> impl Future<Output = Result<Self::Connected, Self::Error>> + Send {
///         MemoryBroker::new().connect()
///     }
/// }
///
/// impl DescribeServer for Cluster {
///     fn describe_server(&self) -> ServerSpec {
///         let hosts: Vec<String> =
///             self.addrs.iter().map(|addr| ServerSpec::host_from_url(addr)).collect();
///         ServerSpec::new(hosts.join(","), "nats")
///     }
/// }
///
/// message_shape::describes_addresses_without_credentials(
///     |addrs| Cluster { addrs: addrs.iter().map(|addr| (*addr).to_owned()).collect() },
///     "nats",
/// );
/// # }
/// ```
///
/// # Panics
///
/// Panics when the document carries the user or the password of any address, or names none of
/// the addresses.
#[cfg(feature = "asyncapi")]
pub fn describes_addresses_without_credentials<B, MkBroker>(make_broker: MkBroker, scheme: &str)
where
    B: DescribeServer,
    MkBroker: FnOnce(&[&str]) -> B,
{
    const USER: &str = "conformance-user";
    const SECRET: &str = "conformance-secret-7f3a";
    const HOSTS: [&str; 3] = [
        "conformance-a.invalid:4101",
        "conformance-b.invalid:4102",
        "conformance-c.invalid:4103",
    ];

    let urls = HOSTS.map(|host| format!("{scheme}://{USER}:{SECRET}@{host}"));
    let addrs = urls.each_ref().map(String::as_str);
    let broker = make_broker(&addrs);
    let app = RustStream::new(AppInfo::new("conformance", "0.0.0"))
        .server("broker", broker.describe_server());
    let document = build_spec(&app)
        .to_json()
        .expect("the generated document must serialize");

    for leaked in [SECRET, USER] {
        assert!(
            !document.contains(leaked),
            "the server description of a broker configured with several addresses carries their \
             userinfo. Describe each address through ServerSpec::host_from_url. Got: {document}",
        );
    }
    assert!(
        HOSTS.iter().any(|host| document.contains(host)),
        "the server description names none of the configured addresses, so the scan for their \
         credentials proves nothing. Describe each address through ServerSpec::host_from_url. \
         Got: {document}",
    );
}

/// Awaits the next delivery, failing the check on a timeout, an ended stream or an error.
async fn expect_delivery<S, M, E>(stream: &mut S, label: &str) -> M
where
    S: Stream<Item = Result<M, E>> + Unpin,
    M: IncomingMessage,
    E: fmt::Debug,
{
    timeout(DELIVERY_TIMEOUT, stream.next())
        .await
        .unwrap_or_else(|_| panic!("{label}: no delivery within {DELIVERY_TIMEOUT:?}"))
        .unwrap_or_else(|| panic!("{label}: the subscription ended before the expected delivery"))
        .unwrap_or_else(|err| panic!("{label}: the subscription yielded an error: {err:?}"))
}

/// Acknowledges `msg`, accepting a transport that cannot acknowledge.
async fn settle<M: IncomingMessage>(msg: M, label: &str) {
    match msg.ack().await {
        Ok(()) | Err(AckError::Unsupported) => {}
        Err(other) => panic!("{label}: ack must succeed or be unsupported, got: {other:?}"),
    }
}

#[cfg(all(test, feature = "memory"))]
mod tests;
