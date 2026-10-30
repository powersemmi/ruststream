//! Requesters whose correlation or shutdown is broken.

use std::time::Duration;

use futures::StreamExt;
use tokio::sync::Mutex as AsyncMutex;
use tokio::time::{sleep, timeout};

use crate::capabilities::request_reply;
use crate::helpers::unique_subject;
use ruststream::memory::{
    ConnectedMemoryBroker, MemoryBroker, MemoryMessage, MemoryPublisher, MemoryRequester,
    MemorySource, MemorySubscriber, RequestError,
};
use ruststream_core::{
    BytesMut, OutgoingMessage, Publisher, RequestReply, Subscribe, Subscriber, Take,
};

/// Publishes a request with `reply_to` in its headers, through a plain publisher.
async fn publish_request(
    publisher: &MemoryPublisher,
    msg: OutgoingMessage<'_, BytesMut>,
    reply_to: &str,
) -> Result<(), RequestError> {
    let (name, payload, mut headers) = msg.into_parts();
    headers.insert("reply-to", reply_to.to_owned());
    // Set the way a requester that matches replies by it sets one, so the responder echoes it.
    headers.insert("correlation-id", unique_subject("faulty.correlation"));
    publisher
        .publish(
            OutgoingMessage::with_payload(name, payload).with_headers(headers),
            None,
        )
        .await
        .map_err(|_| RequestError::ShutDown)
}

/// Waits for the next message on `inbox`, as the reply.
async fn next_reply(
    inbox: &mut MemorySubscriber,
    subject: &str,
    wait: Duration,
) -> Result<MemoryMessage, RequestError> {
    let mut stream = std::pin::pin!(inbox.stream());
    match timeout(wait, stream.next()).await {
        Ok(Some(Ok(reply))) => Ok(reply),
        Ok(_) => Err(RequestError::ShutDown),
        Err(_) => Err(RequestError::Timeout {
            subject: subject.to_owned(),
            timeout: wait,
        }),
    }
}

/// The requester fault a double has.
#[derive(Debug, Clone, Copy)]
enum RequestFault {
    /// Every request shares one long-lived reply subscription and takes whatever arrives on it
    /// next, a late reply included.
    SharedInbox,
    /// Every request opens its own reply subscription, but under one fixed name, so two requests
    /// in flight at once each see both replies.
    FixedInboxName,
    /// A request whose publish fails waits out its timeout instead of returning the error.
    WaitsAfterShutdown,
}

struct FaultyRequester {
    connected: ConnectedMemoryBroker,
    publisher: MemoryPublisher,
    requester: MemoryRequester,
    inbox_name: String,
    shared: AsyncMutex<Option<MemorySubscriber>>,
    fault: RequestFault,
}

impl FaultyRequester {
    fn new(connected: &ConnectedMemoryBroker, fault: RequestFault) -> Self {
        Self {
            connected: connected.clone(),
            publisher: connected.publisher(),
            requester: connected.requester(),
            inbox_name: unique_subject("faulty.inbox"),
            shared: AsyncMutex::new(None),
            fault,
        }
    }
}

impl Publisher for FaultyRequester {
    type Payload = Take;
    type Error = RequestError;
    type Options = ();

    async fn publish(
        &self,
        msg: OutgoingMessage<'_, BytesMut>,
        options: Option<&()>,
    ) -> Result<(), RequestError> {
        self.requester.publish(msg, options).await
    }
}

impl RequestReply for FaultyRequester {
    type Reply = MemoryMessage;

    // The shared inbox's lock is held across the whole request on purpose: one reply
    // subscription serves every request in turn, which is the fault that double has.
    #[allow(clippy::significant_drop_tightening)]
    async fn request(
        &self,
        msg: OutgoingMessage<'_, BytesMut>,
        wait: Duration,
    ) -> Result<MemoryMessage, RequestError> {
        let subject = msg.name().to_owned();
        match self.fault {
            RequestFault::SharedInbox => {
                let mut shared = self.shared.lock().await;
                if shared.is_none() {
                    let inbox = Subscribe::subscribe(&self.connected, &self.inbox_name)
                        .await
                        .map_err(|_| RequestError::ShutDown)?;
                    *shared = Some(inbox);
                }
                let inbox = shared.as_mut().expect("the shared inbox was just opened");
                publish_request(&self.publisher, msg, &self.inbox_name).await?;
                next_reply(inbox, &subject, wait).await
            }
            RequestFault::FixedInboxName => {
                let mut inbox = Subscribe::subscribe(&self.connected, &self.inbox_name)
                    .await
                    .map_err(|_| RequestError::ShutDown)?;
                publish_request(&self.publisher, msg, &self.inbox_name).await?;
                next_reply(&mut inbox, &subject, wait).await
            }
            RequestFault::WaitsAfterShutdown => match self.requester.request(msg, wait).await {
                Err(RequestError::ShutDown) => {
                    sleep(wait).await;
                    Err(RequestError::ShutDown)
                }
                other => other,
            },
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[should_panic(expected = "a reply that arrived after its request timed out resolved the next")]
async fn request_reply_catches_a_late_reply_handed_to_the_next_request() {
    request_reply(
        MemoryBroker::new,
        |name| MemorySource::new(name),
        |broker| FaultyRequester::new(broker, RequestFault::SharedInbox),
        |broker| broker.publisher(),
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[should_panic(expected = "two requests in flight at once must each resolve with the reply")]
async fn request_reply_catches_replies_crossed_between_concurrent_requests() {
    request_reply(
        MemoryBroker::new,
        |name| MemorySource::new(name),
        |broker| FaultyRequester::new(broker, RequestFault::FixedInboxName),
        |broker| broker.publisher(),
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[should_panic(expected = "a request after shutdown: still pending")]
async fn request_reply_catches_a_request_that_waits_after_shutdown() {
    request_reply(
        MemoryBroker::new,
        |name| MemorySource::new(name),
        |broker| FaultyRequester::new(broker, RequestFault::WaitsAfterShutdown),
        |broker| broker.publisher(),
    )
    .await;
}
