//! Each check against the in-memory broker, which passes, and against a broken double of it,
//! which the check must fail: a check no broker can fail proves nothing.

use std::{
    future::Future,
    mem,
    sync::{Mutex, PoisonError},
};

use bytes::{Bytes, BytesMut};

use super::{OptionCases, keyed_order, publish_options, publisher_carries};
use crate::conformance::helpers::unique_subject;
use crate::memory::{
    ConnectedMemoryBroker, MemoryBroker, MemoryError, MemoryPublish, MemoryPublisher, MemorySource,
    PARTITION_KEY_HEADER,
};
use crate::{
    HeaderMap, IncomingMessage, OutgoingMessage, PairError, PublishPolicy, Publisher, Take,
};

/// What a double reports when it refuses, alongside what the memory broker reports.
#[derive(Debug, thiserror::Error)]
enum DoubleError {
    #[error(transparent)]
    Memory(#[from] MemoryError),
    #[error("the double refuses this publish: {0}")]
    Refused(&'static str),
}

/// A message a double holds back, owned.
struct Held {
    name: String,
    payload: BytesMut,
    headers: HeaderMap,
}

/// How a double breaks the message on its way to the memory broker.
#[derive(Clone, Copy)]
enum Fault {
    /// Holds messages back and releases each run of `window` in reverse.
    Reverses { window: usize },
    /// Publishes every message with no headers, and reports success.
    DropsHeaders,
    /// Rewrites every header value as lossy UTF-8 text, and reports success.
    RewritesAsText,
    /// Refuses every publish that carries headers. The honest answer of a headerless transport.
    RefusesHeaders,
    /// Reports a publish with headers as refused, and delivers it anyway.
    RefusesAndDelivers,
    /// Drops the partition key header.
    DropsKey,
}

struct Faulty {
    inner: MemoryPublisher,
    fault: Fault,
    held: Mutex<Vec<Held>>,
}

impl Faulty {
    fn new(inner: MemoryPublisher, fault: Fault) -> Self {
        Self {
            inner,
            fault,
            held: Mutex::new(Vec::new()),
        }
    }

    async fn forward(&self, held: Held) -> Result<(), DoubleError> {
        let msg = OutgoingMessage::produced(&held.name, held.payload).with_headers(held.headers);
        Ok(self.inner.publish(msg, None).await?)
    }
}

impl Publisher for Faulty {
    type Payload = Take;
    type Error = DoubleError;
    type Options = ();

    fn publish(
        &self,
        msg: OutgoingMessage<'_, BytesMut>,
        _options: Option<&()>,
    ) -> impl Future<Output = Result<(), DoubleError>> + Send {
        let (name, payload, headers) = msg.into_parts();
        let held = Held {
            name: name.to_owned(),
            payload,
            headers,
        };
        async move {
            match self.fault {
                Fault::Reverses { window } => {
                    let run = {
                        let mut queue = self.held.lock().unwrap_or_else(PoisonError::into_inner);
                        queue.push(held);
                        if queue.len() < window {
                            return Ok(());
                        }
                        mem::take(&mut *queue)
                    };
                    for held in run.into_iter().rev() {
                        self.forward(held).await?;
                    }
                    Ok(())
                }
                Fault::DropsHeaders => {
                    self.forward(Held {
                        headers: HeaderMap::new(),
                        ..held
                    })
                    .await
                }
                Fault::RewritesAsText => {
                    let mut text = HeaderMap::new();
                    for (key, value) in held.headers.iter() {
                        text.insert(key.to_owned(), String::from_utf8_lossy(value).into_owned());
                    }
                    self.forward(Held {
                        headers: text,
                        ..held
                    })
                    .await
                }
                Fault::RefusesHeaders if !held.headers.is_empty() => {
                    Err(DoubleError::Refused("this transport carries no headers"))
                }
                Fault::RefusesAndDelivers if !held.headers.is_empty() => {
                    self.forward(held).await?;
                    Err(DoubleError::Refused("reported refused, delivered anyway"))
                }
                Fault::DropsKey => {
                    let mut held = held;
                    held.headers.remove(PARTITION_KEY_HEADER);
                    self.forward(held).await
                }
                Fault::RefusesHeaders | Fault::RefusesAndDelivers => self.forward(held).await,
            }
        }
    }
}

// `make_source` / `make_publisher` stay closures: their bounds are higher-ranked, so a method
// path, which binds one lifetime, does not type-check.
#[allow(clippy::redundant_closure, clippy::redundant_closure_for_method_calls)]
async fn carries(fault: Fault) {
    let _factories = publisher_carries(
        MemoryBroker::new,
        |name| MemorySource::new(name),
        move |connected| Faulty::new(connected.publisher(), fault),
    )
    .await;
}

#[allow(clippy::redundant_closure, clippy::redundant_closure_for_method_calls)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn memory_broker_carries_order_and_headers() {
    let _factories = publisher_carries(
        MemoryBroker::new,
        |name| MemorySource::new(name),
        |connected| connected.publisher(),
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_transport_that_refuses_headers_passes() {
    carries(Fault::RefusesHeaders).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[should_panic(expected = "publish order")]
async fn a_publisher_that_reorders_fails() {
    carries(Fault::Reverses { window: 2 }).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[should_panic(expected = "must arrive byte for byte")]
async fn a_publisher_that_drops_headers_fails() {
    carries(Fault::DropsHeaders).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[should_panic(expected = "header round trip (a non-UTF-8 value)")]
async fn a_publisher_that_rewrites_binary_values_as_text_fails() {
    carries(Fault::RewritesAsText).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[should_panic(expected = "was refused, and the message arrived anyway")]
async fn a_publisher_that_reports_refused_and_delivers_fails() {
    carries(Fault::RefusesAndDelivers).await;
}

#[allow(clippy::redundant_closure, clippy::redundant_closure_for_method_calls)]
async fn keyed(fault: Fault) {
    keyed_order(
        MemoryBroker::new,
        &unique_subject("conformance.keyed"),
        |name| MemorySource::new(name),
        move |connected| Faulty::new(connected.publisher(), fault),
        |key, headers| {
            headers.insert(PARTITION_KEY_HEADER, Bytes::copy_from_slice(key));
            None
        },
    )
    .await;
}

#[allow(clippy::redundant_closure, clippy::redundant_closure_for_method_calls)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn memory_broker_keeps_keys_and_their_order() {
    keyed_order(
        MemoryBroker::new,
        &unique_subject("conformance.keyed"),
        |name| MemorySource::new(name),
        |connected| connected.publisher(),
        |key, headers| {
            headers.insert(PARTITION_KEY_HEADER, Bytes::copy_from_slice(key));
            None
        },
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[should_panic(expected = "must report that key")]
async fn a_publisher_that_drops_the_key_fails() {
    keyed(Fault::DropsKey).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[should_panic(expected = "must arrive in publish order")]
async fn a_publisher_that_reorders_one_key_fails() {
    // Six is two messages of each of the three keys, so every key comes out reversed.
    keyed(Fault::Reverses { window: 6 }).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[should_panic(expected = "keyed order: publishing")]
async fn a_transport_that_refuses_the_key_header_fails() {
    keyed(Fault::RefusesHeaders).await;
}

/// A priority, the one per-message setting of the options double.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct Priority {
    priority: Option<u8>,
}

/// The highest priority the options double's transport honours.
const MAX_PRIORITY: u8 = 9;

/// How the options double resolves a priority.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Resolve {
    /// Call over policy, and a value past the range refused.
    Honest,
    /// The transport's own default whenever the call names nothing.
    IgnoresPolicy,
    /// The policy's value, whatever the call names.
    IgnoresCall,
    /// The last call's value keeps applying to publishes that name nothing.
    Sticky,
    /// A value past the range becomes the highest one, and the publish succeeds.
    Clamps,
}

#[derive(Debug, Clone, Copy)]
struct PriorityPublish {
    priority: u8,
    resolve: Resolve,
}

struct PriorityPublisher {
    inner: MemoryPublisher,
    policy: u8,
    resolve: Resolve,
    last: Mutex<Option<u8>>,
}

impl PublishPolicy<ConnectedMemoryBroker> for PriorityPublish {
    type Live = PriorityPublisher;

    async fn pair(self, connected: &ConnectedMemoryBroker) -> Result<Self::Live, PairError> {
        Ok(PriorityPublisher {
            inner: MemoryPublish.pair(connected).await?,
            policy: self.priority,
            resolve: self.resolve,
            last: Mutex::new(None),
        })
    }
}

impl PriorityPublisher {
    fn resolve(&self, call: Option<u8>) -> Result<u8, DoubleError> {
        let last = {
            let mut last = self.last.lock().unwrap_or_else(PoisonError::into_inner);
            let before = *last;
            if call.is_some() {
                *last = call;
            }
            before
        };
        let resolved = match (self.resolve, call) {
            (Resolve::IgnoresPolicy, None) => 0,
            (Resolve::Sticky, None) => last.unwrap_or(self.policy),
            (Resolve::IgnoresCall, _) | (_, None) => self.policy,
            (_, Some(value)) => value,
        };
        match resolved {
            value if value <= MAX_PRIORITY => Ok(value),
            _ if self.resolve == Resolve::Clamps => Ok(MAX_PRIORITY),
            _ => Err(DoubleError::Refused("priority past the transport's range")),
        }
    }
}

impl Publisher for PriorityPublisher {
    type Payload = Take;
    type Error = DoubleError;
    type Options = Priority;

    async fn publish(
        &self,
        msg: OutgoingMessage<'_, BytesMut>,
        options: Option<&Priority>,
    ) -> Result<(), DoubleError> {
        let priority = self.resolve(options.and_then(|options| options.priority))?;
        let (name, payload, mut headers) = msg.into_parts();
        headers.insert("priority", priority.to_string());
        let msg = OutgoingMessage::produced(name, payload).with_headers(headers);
        Ok(self.inner.publish(msg, None).await?)
    }
}

#[allow(clippy::redundant_closure, clippy::redundant_closure_for_method_calls)]
async fn options(resolve: Resolve) {
    publish_options(
        MemoryBroker::new,
        |name| MemorySource::new(name),
        PriorityPublish {
            priority: 5,
            resolve,
        },
        OptionCases::new(Some(5))
            .overrides(Priority { priority: Some(1) }, Some(1))
            .overrides(Priority { priority: None }, Some(5))
            .refuses(Priority { priority: Some(42) }),
        |delivery| {
            delivery
                .headers()
                .get_str("priority")
                .and_then(|value| value.parse::<u8>().ok())
        },
    )
    .await;
}

#[allow(clippy::redundant_closure, clippy::redundant_closure_for_method_calls)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn memory_broker_resolves_its_options() {
    publish_options(
        MemoryBroker::new,
        |name| MemorySource::new(name),
        MemoryPublish,
        OptionCases::new(()).overrides((), ()),
        |_delivery| (),
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn options_resolved_over_the_policy_pass() {
    options(Resolve::Honest).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[should_panic(expected = "a publish with no options takes the policy's")]
async fn a_publisher_that_ignores_the_policy_fails() {
    options(Resolve::IgnoresPolicy).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[should_panic(expected = "override 0 must win over the policy")]
async fn a_publisher_that_ignores_the_call_fails() {
    options(Resolve::IgnoresCall).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[should_panic(expected = "the publish after override 0 takes the policy's setting again")]
async fn a_publisher_whose_call_sticks_fails() {
    options(Resolve::Sticky).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[should_panic(expected = "refused value 0 was accepted")]
async fn a_publisher_that_clamps_an_unhonourable_value_fails() {
    options(Resolve::Clamps).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[should_panic(expected = "pass at least one override")]
async fn options_with_no_override_are_rejected() {
    publish_options(
        MemoryBroker::new,
        |name: &str| MemorySource::new(name),
        MemoryPublish,
        OptionCases::new(()),
        |_delivery| (),
    )
    .await;
}

#[cfg(feature = "asyncapi")]
mod credentials {
    use std::future::Future;

    use serde_json::json;

    use super::super::{describes_addresses_without_credentials, publishes_without_credentials};
    use crate::asyncapi::{Binding, Bindings};
    use crate::memory::{ConnectedMemoryBroker, MemoryBroker, MemoryError, MemoryPublish};
    use crate::{Broker, DescribeServer, PairError, PublishPolicy, ServerSpec};

    /// A policy that puts its connection URL, password and all, into its operation binding.
    struct LeakyPublish {
        url: &'static str,
    }

    impl PublishPolicy<ConnectedMemoryBroker> for LeakyPublish {
        type Live = crate::memory::MemoryPublisher;

        async fn pair(self, connected: &ConnectedMemoryBroker) -> Result<Self::Live, PairError> {
            MemoryPublish.pair(connected).await
        }

        fn operation_bindings(&self, _channel: &str) -> Bindings {
            Binding::extension("x-leaky", &json!({ "url": self.url }))
                .map_or_else(|_| Bindings::new(), |binding| Bindings::new().with(binding))
        }
    }

    #[test]
    fn a_clean_policy_passes() {
        publishes_without_credentials::<ConnectedMemoryBroker, _>(&MemoryPublish, "hunter2");
    }

    #[test]
    #[should_panic(expected = "the publish policy's bindings carry the password")]
    fn a_policy_that_binds_its_password_fails() {
        publishes_without_credentials::<ConnectedMemoryBroker, _>(
            &LeakyPublish {
                url: "amqp://svc:hunter2@broker:5672",
            },
            "hunter2",
        );
    }

    /// How the cluster double describes its addresses.
    #[derive(Clone, Copy)]
    enum Describe {
        EveryHost,
        FirstStrippedOnly,
        FirstOnly,
        Nothing,
    }

    struct Cluster {
        addrs: Vec<String>,
        describe: Describe,
    }

    impl Broker for Cluster {
        type Error = MemoryError;
        type Connected = ConnectedMemoryBroker;

        fn connect(self) -> impl Future<Output = Result<Self::Connected, Self::Error>> + Send {
            MemoryBroker::new().connect()
        }
    }

    impl DescribeServer for Cluster {
        fn describe_server(&self) -> ServerSpec {
            let hosts: Vec<String> = match self.describe {
                Describe::EveryHost => self
                    .addrs
                    .iter()
                    .map(|addr| ServerSpec::host_from_url(addr))
                    .collect(),
                Describe::FirstStrippedOnly => self
                    .addrs
                    .iter()
                    .enumerate()
                    .map(|(index, addr)| match index {
                        0 => ServerSpec::host_from_url(addr),
                        _ => addr.clone(),
                    })
                    .collect(),
                Describe::FirstOnly => self
                    .addrs
                    .first()
                    .map(|addr| ServerSpec::host_from_url(addr))
                    .into_iter()
                    .collect(),
                Describe::Nothing => Vec::new(),
            };
            ServerSpec::new(hosts.join(","), "nats")
        }
    }

    fn cluster(describe: Describe) {
        describes_addresses_without_credentials(
            |addrs| Cluster {
                addrs: addrs.iter().map(|addr| (*addr).to_owned()).collect(),
                describe,
            },
            "nats",
        );
    }

    #[test]
    fn a_cluster_described_host_by_host_passes() {
        cluster(Describe::EveryHost);
    }

    #[test]
    #[should_panic(expected = "carries their userinfo")]
    fn a_cluster_that_strips_only_the_first_address_fails() {
        cluster(Describe::FirstStrippedOnly);
    }

    #[test]
    fn a_cluster_described_by_its_first_address_passes() {
        cluster(Describe::FirstOnly);
    }

    #[test]
    #[should_panic(expected = "names none of the configured addresses")]
    fn a_cluster_that_describes_no_address_fails() {
        cluster(Describe::Nothing);
    }
}
