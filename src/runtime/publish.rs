//! Outgoing message and the publish middleware pipeline.
//!
//! When a handler's reply is published (via `#[subscriber(.., publish(..))]`), it flows through a
//! chain of [`PublishMiddleware`] before reaching the broker publisher. Middleware transform the
//! payload (for example, wrap it in a Confluent / Avro envelope) and enrich the headers
//! (content-type, schema id), or observe it (publish metrics). The chain is symmetric to the
//! consume-side [`DynStack`](super::DynStack).

use std::{future::Future, pin::Pin, sync::Arc};

use serde::Serialize;

use crate::codec::Codec;
use crate::{Headers, Publisher};

use super::lifecycle::BoxError;
use super::publisher_registry::ErasedPublisher;

type PublishFut<'a> = Pin<Box<dyn Future<Output = Result<(), BoxError>> + Send + 'a>>;

/// An owned, mutable outgoing message flowing through the publish pipeline.
///
/// Middleware may change the [`topic`](Self::topic), transform the
/// [`payload`](Self::payload_mut), and enrich the [`headers`](Self::headers_mut) before the
/// message is sent.
#[derive(Debug, Clone)]
pub struct Outgoing {
    topic: String,
    payload: Vec<u8>,
    headers: Headers,
}

impl Outgoing {
    /// Creates an outgoing message with no headers.
    #[must_use]
    pub fn new(topic: impl Into<String>, payload: impl Into<Vec<u8>>) -> Self {
        Self {
            topic: topic.into(),
            payload: payload.into(),
            headers: Headers::new(),
        }
    }

    /// The destination topic.
    #[must_use]
    pub fn topic(&self) -> &str {
        &self.topic
    }

    /// Sets the destination topic.
    pub fn set_topic(&mut self, topic: impl Into<String>) {
        self.topic = topic.into();
    }

    /// The payload bytes.
    #[must_use]
    pub fn payload(&self) -> &[u8] {
        &self.payload
    }

    /// The payload bytes, mutably (for envelope wrapping).
    pub fn payload_mut(&mut self) -> &mut Vec<u8> {
        &mut self.payload
    }

    /// Replaces the payload.
    pub fn set_payload(&mut self, payload: impl Into<Vec<u8>>) {
        self.payload = payload.into();
    }

    /// The outgoing headers.
    #[must_use]
    pub fn headers(&self) -> &Headers {
        &self.headers
    }

    /// The outgoing headers, mutably.
    pub fn headers_mut(&mut self) -> &mut Headers {
        &mut self.headers
    }
}

/// Middleware that transforms (or observes) an [`Outgoing`] message before it is published.
///
/// Each middleware inspects / mutates `out`, then calls [`PublishNext::run`] to continue; the chain
/// ends in the actual broker publish.
pub trait PublishMiddleware: Send + Sync {
    /// Handle the outgoing message, calling `next` to continue the pipeline.
    fn on_publish<'a>(&'a self, out: &'a mut Outgoing, next: PublishNext<'a>) -> PublishFut<'a>;
}

/// A cursor over the remaining publish middleware, ending in the broker publisher.
pub struct PublishNext<'a> {
    rest: &'a [Arc<dyn PublishMiddleware>],
    publisher: &'a dyn ErasedPublisher,
}

impl<'a> PublishNext<'a> {
    /// Runs the next middleware, or sends the message if the pipeline is exhausted.
    #[must_use]
    pub fn run(self, out: &'a mut Outgoing) -> PublishFut<'a> {
        match self.rest.split_first() {
            Some((middleware, rest)) => middleware.on_publish(
                out,
                PublishNext {
                    rest,
                    publisher: self.publisher,
                },
            ),
            None => self
                .publisher
                .publish_message(out.topic(), out.payload(), out.headers()),
        }
    }
}

impl std::fmt::Debug for PublishNext<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PublishNext")
            .field("remaining", &self.rest.len())
            .finish_non_exhaustive()
    }
}

/// Runs `out` through `pipeline`, then publishes it via `publisher`.
pub(crate) fn run_publish<'a>(
    pipeline: &'a [Arc<dyn PublishMiddleware>],
    publisher: &'a dyn ErasedPublisher,
    out: &'a mut Outgoing,
) -> PublishFut<'a> {
    PublishNext {
        rest: pipeline,
        publisher,
    }
    .run(out)
}

/// A publisher bound to a destination topic and a [`Codec`], ready to send typed values.
///
/// This is the publish-side counterpart to a subscriber: it carries *where* (the topic) and *how*
/// (the codec) a reply is sent, so a handler — or the [`#[subscriber(.., publish)]`](macro) reply
/// form — just hands it a value. Construct it from a broker's byte publisher:
///
/// ```
/// # #[cfg(all(feature = "memory", feature = "json"))]
/// # {
/// use ruststream::codec::JsonCodec;
/// use ruststream::memory::MemoryBroker;
/// use ruststream::runtime::Publication;
///
/// let broker = MemoryBroker::new();
/// let out = Publication::new(broker.publisher(), "responses", JsonCodec);
/// assert_eq!(out.topic(), "responses");
/// # }
/// ```
///
/// [macro]: crate::subscriber
pub struct Publication<P, C> {
    publisher: P,
    topic: String,
    codec: C,
}

impl<P, C> Publication<P, C> {
    /// Binds `publisher` to `topic`, encoding values with `codec`.
    #[must_use]
    pub fn new(publisher: P, topic: impl Into<String>, codec: C) -> Self {
        Self {
            publisher,
            topic: topic.into(),
            codec,
        }
    }

    /// The destination topic replies are sent to.
    #[must_use]
    pub fn topic(&self) -> &str {
        &self.topic
    }
}

impl<P: Publisher, C: Codec> Publication<P, C> {
    /// Encodes `value` and publishes it to the bound topic, through `pipeline`.
    pub(crate) async fn publish<T: Serialize + Sync>(
        &self,
        value: &T,
        pipeline: &[Arc<dyn PublishMiddleware>],
    ) -> Result<(), BoxError> {
        let bytes = self
            .codec
            .encode(value)
            .map_err(|e| Box::new(e) as BoxError)?;
        let mut out = Outgoing::new(self.topic.clone(), bytes.to_vec());
        run_publish(pipeline, &self.publisher, &mut out).await
    }
}

impl<P, C> std::fmt::Debug for Publication<P, C> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Publication")
            .field("topic", &self.topic)
            .finish_non_exhaustive()
    }
}
