//! Type-erased publisher, used by the broker-agnostic `retry_after` fallback.
//!
//! The deferred-redelivery fallback (see [`BrokerScope::retry_via`](super::BrokerScope::retry_via))
//! re-publishes a message to its own source subject through a publisher whose concrete type does
//! not matter to the runtime, so it is held erased behind [`ErasedPublisher`]. To share a publisher
//! with handlers, put it in the typed application state instead and read it with
//! [`Context::state`](super::Context::state).

use std::fmt;
use std::future::Future;

use thiserror::Error;

use crate::{OutgoingMessage, Publisher};

use super::lifecycle::{BoxError, BoxFuture};
use super::publish::PublishSink;

/// A publisher with its concrete type and error erased.
///
/// Blanket-implemented for every [`Publisher`], so any broker's publisher can be held as
/// `Arc<dyn ErasedPublisher>` for the `retry_after` fallback.
///
/// Sealed: erasure is the runtime's own machinery and the blanket impl already covers every
/// publisher, so there is nothing outside this crate to implement it for. That is what keeps
/// the method list free to follow the publish builder - growing or retiring a method here
/// reaches no downstream implementor.
pub trait ErasedPublisher: sealed::Sealed + Send + Sync {
    /// Publishes one message, with whatever destination, payload and headers it carries.
    ///
    /// # Errors
    ///
    /// Returns the underlying publisher's error, boxed, if the broker rejects the publish.
    fn publish_erased<'a>(
        &'a self,
        msg: OutgoingMessage<'a>,
    ) -> BoxFuture<'a, Result<(), BoxError>>;
}

mod sealed {
    use crate::Publisher;

    /// Seals [`ErasedPublisher`](super::ErasedPublisher).
    pub trait Sealed {}

    impl<P: Publisher> Sealed for P {}
}

impl<P: Publisher> ErasedPublisher for P {
    fn publish_erased<'a>(
        &'a self,
        msg: OutgoingMessage<'a>,
    ) -> BoxFuture<'a, Result<(), BoxError>> {
        Box::pin(async move { self.publish(msg).await.map_err(|e| Box::new(e) as BoxError) })
    }
}

/// The publish builder's sink over an erased publisher.
///
/// The blanket [`PublishSink`] impl covers `&P` of a concrete [`Publisher`]; the runtime's
/// deferred-redelivery fallback holds a `dyn ErasedPublisher` instead, which no broker type
/// names, so it reaches the same builder through this wrapper.
pub(crate) struct ErasedSink<'a>(pub(crate) &'a dyn ErasedPublisher);

impl PublishSink for ErasedSink<'_> {
    type Error = ErasedPublishError;

    fn send(
        &mut self,
        msg: OutgoingMessage<'_>,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send {
        let publisher = self.0;
        async move {
            publisher
                .publish_erased(msg)
                .await
                .map_err(ErasedPublishError)
        }
    }
}

/// The error of a publish through an erased publisher: the broker's own, boxed.
#[derive(Debug, Error)]
#[error("{0}")]
pub(crate) struct ErasedPublishError(BoxError);

impl fmt::Debug for ErasedSink<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ErasedSink").finish_non_exhaustive()
    }
}

#[cfg(all(test, feature = "memory"))]
mod tests {
    use futures::StreamExt;

    use super::*;
    use crate::memory::MemoryBroker;
    use crate::runtime::publish::raw_of;
    use crate::{Headers, IncomingMessage, Subscriber};

    /// The erased sink reaches the publish builder, which is how the deferred-retry fallback
    /// republishes: one message, headers included, through the same positions as everywhere.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn the_erased_sink_publishes_through_the_builder() {
        let broker = MemoryBroker::new();
        let mut subscriber = broker.subscribe("erased.builder");
        let publisher = broker.publisher();
        let erased: &dyn ErasedPublisher = &publisher;

        let mut headers = Headers::new();
        headers.insert("k", "v");
        raw_of(ErasedSink(erased), b"deferred")
            .with_headers(headers)
            .to("erased.builder")
            .publish()
            .await
            .expect("the erased builder publish failed");

        let mut stream = std::pin::pin!(subscriber.stream());
        let delivery = stream.next().await.unwrap().expect("delivery");
        assert_eq!(delivery.payload(), b"deferred");
        assert_eq!(delivery.headers().get_str("k"), Some("v"));
        delivery.ack().await.expect("ack failed");
    }

    /// The rendered error of an erased publish carries the broker's own message, so a failed
    /// deferred republish is diagnosable from the log line alone.
    #[tokio::test]
    async fn the_erased_error_carries_the_broker_message() {
        let error = ErasedPublishError(Box::new(std::io::Error::other("broker refused")));
        assert!(error.to_string().contains("broker refused"), "got: {error}");
        assert!(
            format!("{:?}", ErasedSink(&MemoryBroker::new().publisher())).starts_with("ErasedSink")
        );
    }
}
