//! Request / reply over an in-process inbox: the requester and the error a request ends with.

use std::{
    fmt,
    future::ready,
    marker::PhantomData,
    sync::{Arc, atomic::Ordering},
    time::Duration,
};

use thiserror::Error;
use tokio::{sync::mpsc, time::timeout};

use crate::{Discarding, MemoryMessage, MemoryState};
use ruststream_core::{BytesMut, OutgoingMessage, Publisher, RequestReply, Str, Take};

/// Errors returned by [`MemoryRequester`] operations.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum RequestError {
    /// No reply arrived in the request inbox before the timeout elapsed.
    #[error("no reply to \"{subject}\" within {timeout:?}")]
    Timeout {
        /// The subject the request was published to.
        subject: String,
        /// How long the requester waited for a reply.
        timeout: Duration,
    },
    /// The operation ran through a handle aliasing a bus that was shut down
    /// ([`ConnectedBroker::shutdown`](ruststream_core::ConnectedBroker::shutdown)) and not revived by a
    /// sibling clone's [`Broker::connect`](ruststream_core::Broker::connect).
    #[error("the memory broker is shut down")]
    ShutDown,
}

/// Publisher with native request / reply support, returned by
/// [`MemoryBroker::requester`](crate::MemoryBroker::requester).
///
/// [`request`](RequestReply::request) publishes the message with a unique in-process inbox in
/// the `reply-to` header and resolves on the first message delivered to that inbox. A responder
/// reads `reply-to` from the request headers and publishes its reply there. Plain
/// [`publish`](Publisher::publish) is the usual fire-and-forget fanout.
///
/// # Examples
///
/// ```
/// # mod demo {
/// use std::time::Duration;
///
/// use ruststream::OutgoingMessage;
/// use ruststream::memory::prelude::*;
/// use serde::{Deserialize, Serialize};
///
/// #[derive(Deserialize)]
/// struct Order {
///     id: u64,
/// }
///
/// #[derive(Serialize)]
/// struct Ask {
///     id: u64,
/// }
///
/// #[derive(Deserialize)]
/// struct Quote {
///     price: u64,
/// }
///
/// #[derive(OutSlot)]
/// struct Pricing;
///
/// #[subscriber("orders")]
/// async fn price(order: &Order, Out(pricing): Out<impl RequestReply, Pricing>) -> HandlerOutcome {
///     let Ok(ask) = serde_json::to_vec(&Ask { id: order.id }) else {
///         return HandlerOutcome::drop();
///     };
///     // The reply comes back on an inbox the request names in its `reply-to` header.
///     let request = OutgoingMessage::new("quotes.ask", ask.as_slice());
///     let Ok(reply) = pricing.request(request, Duration::from_secs(2)).await else {
///         return HandlerOutcome::retry();
///     };
///     match serde_json::from_slice::<Quote>(reply.payload()) {
///         Ok(quote) => {
///             tracing::info!(order.id, quote.price, "priced");
///             HandlerOutcome::ack()
///         }
///         Err(_) => HandlerOutcome::drop(),
///     }
/// }
///
/// fn app() -> RustStream {
///     RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
///         b.include(price).out(Pricing, Request).build();
///     })
/// }
/// # }
/// # fn main() {}
/// ```
#[derive(Clone)]
pub struct MemoryRequester {
    state: Arc<MemoryState>,
}

impl MemoryRequester {
    pub(crate) fn new(state: Arc<MemoryState>) -> Self {
        Self { state }
    }
}

impl fmt::Debug for MemoryRequester {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MemoryRequester").finish_non_exhaustive()
    }
}

impl Publisher for MemoryRequester {
    // The same bus, kept the same way: see `MemoryPublisher`.
    type Payload = Take;
    type Error = RequestError;
    type Options = ();

    fn publish(
        &self,
        msg: OutgoingMessage<'_, BytesMut>,
        _options: Option<&Self::Options>,
    ) -> impl Future<Output = Result<(), Self::Error>> {
        let (name, payload, headers) = msg.into_parts();
        ready(
            self.state
                .fanout(name, payload.freeze(), headers)
                .map_err(|_| RequestError::ShutDown),
        )
    }
}

impl RequestReply for MemoryRequester {
    /// A reply arrives on a single-use inbox rather than a subscription, so it carries no
    /// position and no seeker whichever broker the requester came from.
    type Reply = MemoryMessage<Discarding>;

    async fn request(
        &self,
        msg: OutgoingMessage<'_, BytesMut>,
        wait: Duration,
    ) -> Result<Self::Reply, Self::Error> {
        let id = self.state.inbox_seq.fetch_add(1, Ordering::Relaxed);
        let inbox = format!("_inbox.{id}");
        let (tx, mut rx) = mpsc::unbounded_channel();
        self.state
            .register(&inbox, tx.clone())
            .map_err(|_| RequestError::ShutDown)?;

        // The name is the caller's buffer, so it outlives the message consumed here; the inbox
        // is written into the request's own map rather than into a copy of it.
        let (subject, payload, mut headers) = msg.into_parts();
        headers.insert(Str::from_static("reply-to"), inbox.clone());
        if self
            .state
            .fanout(subject, payload.freeze(), headers)
            .is_err()
        {
            self.state.unregister(&inbox);
            return Err(RequestError::ShutDown);
        }

        let outcome = timeout(wait, rx.recv()).await;
        self.state.unregister(&inbox);
        match outcome {
            Ok(Some(reply)) => Ok(MemoryMessage {
                delivery: Some(reply),
                subscription: None,
                // The inbox reply is consumed here, not by a dispatch loop, and its enqueue was not
                // counted (see `fanout`), so it carries no coordinator.
                #[cfg(feature = "testing")]
                coordinator: None,
                mode: PhantomData,
            }),
            // `tx` is held on this stack frame, so the channel cannot report closed.
            Ok(None) => unreachable!("request inbox closed while its sender is held"),
            Err(_) => Err(RequestError::Timeout {
                subject: subject.to_owned(),
                timeout: wait,
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use futures::StreamExt;

    use super::*;
    use crate::capability::tests::replaying;
    use crate::{Bus, MemoryBroker};
    use ruststream_core::{Broker, ConnectedBroker, IncomingMessage, Subscriber};

    #[tokio::test(start_paused = true)]
    async fn a_request_without_a_responder_times_out_naming_the_subject() {
        let broker = MemoryBroker::new();
        // Subscribed but never answering: the request must expire instead of hanging.
        let _service = broker.subscribe("svc.silent");
        let requester = broker.requester();

        let outcome = requester
            .request(
                OutgoingMessage::new("svc.silent", b"ping".as_slice()),
                Duration::from_secs(5),
            )
            .await;

        match outcome {
            Err(RequestError::Timeout { subject, timeout }) => {
                assert_eq!(subject, "svc.silent");
                assert_eq!(timeout, Duration::from_secs(5));
            }
            other => panic!("expected a timeout naming the subject, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn requester_errors_after_shutdown() {
        let broker = MemoryBroker::new();
        let requester = broker.requester();
        let connected = broker.connect().await.unwrap();
        connected.shutdown().await.unwrap();

        let publish = Publisher::publish(
            &requester,
            OutgoingMessage::new("svc.echo", b"ping".as_slice()),
            None,
        )
        .await;
        assert!(
            matches!(publish, Err(RequestError::ShutDown)),
            "{publish:?}"
        );

        let request = requester
            .request(
                OutgoingMessage::new("svc.echo", b"ping".as_slice()),
                Duration::from_millis(50),
            )
            .await;
        assert!(
            matches!(request, Err(RequestError::ShutDown)),
            "a request against a dead bus must fail fast, not time out",
        );
    }

    #[tokio::test]
    async fn request_resolves_on_reply() {
        let broker = MemoryBroker::new();
        let mut service = broker.subscribe("svc.echo");
        let publisher = broker.publisher();
        let requester = broker.requester();

        let respond = async {
            let mut stream = std::pin::pin!(service.stream());
            let msg = stream.next().await.unwrap().unwrap();
            assert_eq!(msg.payload(), b"ping");
            let reply_to = msg.headers().reply_to().unwrap().to_owned();
            publisher
                .publish(OutgoingMessage::new(&reply_to, b"pong"), None)
                .await
                .unwrap();
            msg.ack().await.unwrap();
        };
        let request = requester.request(
            OutgoingMessage::new("svc.echo", b"ping".as_slice()),
            Duration::from_secs(1),
        );

        let (reply, ()) = futures::join!(request, respond);
        assert_eq!(reply.unwrap().payload(), b"pong");

        // The single-use inbox must be unregistered once the request resolves.
        let inbox_leaked = match &*broker.state.subscribers.lock().unwrap() {
            Bus::Live(registry) => registry
                .names
                .keys()
                .any(|name| name.starts_with("_inbox.")),
            Bus::ShutDown(_) => false,
        };
        assert!(!inbox_leaked);
    }

    /// A request inbox is used once and never subscribed again, so what a retaining broker recorded
    /// under it goes with the registration. Otherwise every exchange would leave one name, and one
    /// reply, in the log for the life of the process.
    #[tokio::test]
    async fn a_resolved_request_leaves_no_log_entry_for_its_inbox() {
        let broker = replaying();
        let mut service = broker.subscribe("svc.echo");
        let publisher = broker.publisher();
        let requester = broker.requester();

        let respond = async {
            let mut stream = std::pin::pin!(service.stream());
            let msg = stream.next().await.unwrap().unwrap();
            let reply_to = msg.headers().reply_to().unwrap().to_owned();
            publisher
                .publish(OutgoingMessage::new(&reply_to, b"pong"), None)
                .await
                .unwrap();
            msg.ack().await.unwrap();
        };
        let request = requester.request(
            OutgoingMessage::new("svc.echo", b"ping".as_slice()),
            Duration::from_secs(1),
        );
        let (reply, ()) = futures::join!(request, respond);
        assert_eq!(reply.unwrap().payload(), b"pong");

        let (inboxes, kept_the_subject) = {
            let log = broker.state.log.lock().unwrap();
            let inboxes: Vec<String> = log
                .recorded_names()
                .into_iter()
                .filter(|name| name.starts_with("_inbox."))
                .map(str::to_owned)
                .collect();
            (inboxes, log.name("svc.echo").is_some())
        };

        assert_eq!(inboxes, Vec::<String>::new());
        // The request subject itself is a real name, and stays.
        assert!(kept_the_subject);
    }

    // Paused time needs the current-thread runtime; the test spawns nothing, so the timeout
    // auto-advances instead of sleeping for real.
    #[tokio::test(start_paused = true)]
    async fn request_times_out_without_responder() {
        let broker = MemoryBroker::new();
        let requester = broker.requester();

        let outcome = requester
            .request(
                OutgoingMessage::new("svc.void", b"ping".as_slice()),
                Duration::from_millis(5),
            )
            .await;
        assert!(matches!(outcome, Err(RequestError::Timeout { .. })));

        let inbox_leaked = match &*broker.state.subscribers.lock().unwrap() {
            Bus::Live(registry) => registry
                .names
                .keys()
                .any(|name| name.starts_with("_inbox.")),
            Bus::ShutDown(_) => false,
        };
        assert!(!inbox_leaked);
    }
}
