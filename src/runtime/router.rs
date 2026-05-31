//! The [`Router`]: spawns one task per registered handler, drives subscriber streams, and
//! reacts to a single graceful-shutdown signal.

use std::sync::Arc;

use crate::{IncomingMessage, Subscriber};
use futures::StreamExt;
use thiserror::Error;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::{error, warn};

use super::{
    handler::{Handler, HandlerResult},
    metadata::HandlerMetadata,
};

/// Errors surfaced by the router itself (handler-level errors are signalled via
/// [`HandlerResult::Nack`]).
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum RouterError {
    /// A spawned subscriber task panicked or was aborted.
    #[error("subscriber task failed: {0}")]
    Join(#[source] tokio::task::JoinError),
}

/// Coordinates message dispatch across one or more subscriber tasks.
///
/// Each call to [`Router::handle`] spawns a long-lived task that pulls messages off the
/// subscriber and invokes the registered [`Handler`]. Construct with [`Router::new`], register
/// handlers with [`handle`], then drive lifecycle
/// with [`run`] or release the [`shutdown_handle`] elsewhere. Dropping the router cancels
/// all subscriber tasks.
///
/// [`handle`]: Self::handle
/// [`run`]: Self::run
/// [`shutdown_handle`]: Self::shutdown_handle
pub struct Router {
    tasks: Vec<JoinHandle<()>>,
    handlers: Vec<HandlerMetadata>,
    shutdown: CancellationToken,
}

impl Default for Router {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Debug for Router {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Router")
            .field("handlers", &self.handlers.len())
            .field(
                "tasks_running",
                &self.tasks.iter().filter(|t| !t.is_finished()).count(),
            )
            .finish_non_exhaustive()
    }
}

impl Router {
    /// Creates an empty router with a fresh shutdown token.
    #[must_use]
    pub fn new() -> Self {
        Self {
            tasks: Vec::new(),
            handlers: Vec::new(),
            shutdown: CancellationToken::new(),
        }
    }

    /// Registers a handler bound to the given subscriber. Spawns one Tokio task that drives
    /// the subscriber stream until the shutdown token is triggered or the stream ends.
    pub fn handle<S, H>(&mut self, mut subscriber: S, handler: H, metadata: HandlerMetadata)
    where
        S: Subscriber + Send + 'static,
        H: Handler<S::Message> + 'static,
    {
        self.handlers.push(metadata);
        let shutdown = self.shutdown.clone();
        let handler = Arc::new(handler);
        let task = tokio::spawn(async move {
            let mut stream = std::pin::pin!(subscriber.stream());
            loop {
                tokio::select! {
                    () = shutdown.cancelled() => break,
                    next = stream.next() => match next {
                        Some(Ok(msg)) => dispatch(&*handler, msg).await,
                        Some(Err(err)) => {
                            error!(
                                target: "ruststream::dispatch",
                                error = %err,
                                "subscriber stream error",
                            );
                        }
                        None => break,
                    }
                }
            }
        });
        self.tasks.push(task);
    }

    /// Returns a token that can be cloned and used to trigger shutdown from anywhere.
    #[must_use]
    pub fn shutdown_handle(&self) -> CancellationToken {
        self.shutdown.clone()
    }

    /// Triggers shutdown of every subscriber task.
    pub fn shutdown(&self) {
        self.shutdown.cancel();
    }

    /// Returns metadata for every registered handler, in registration order. Useful for the
    /// `AsyncAPI` generator and runtime introspection.
    #[must_use]
    pub fn handlers(&self) -> &[HandlerMetadata] {
        &self.handlers
    }

    /// Awaits all spawned subscriber tasks. Returns once every task has finished, either by
    /// reaching the end of its stream or because shutdown was triggered.
    ///
    /// # Errors
    ///
    /// Returns [`RouterError::Join`] if any subscriber task panicked.
    pub async fn run(self) -> Result<(), RouterError> {
        let results = futures::future::join_all(self.tasks).await;
        for result in results {
            result.map_err(RouterError::Join)?;
        }
        Ok(())
    }
}

async fn dispatch<H, M>(handler: &H, msg: M)
where
    H: Handler<M>,
    M: IncomingMessage,
{
    let outcome = handler.handle(&msg).await;
    let ack_result = match outcome {
        HandlerResult::Ack => msg.ack().await,
        HandlerResult::Nack { requeue } => msg.nack(requeue).await,
    };
    if let Err(err) = ack_result {
        warn!(
            target: "ruststream::dispatch",
            error = %err,
            "ack / nack failed",
        );
    }
}
