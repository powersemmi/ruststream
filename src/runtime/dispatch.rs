//! The per-subscriber dispatch loop: pulls messages off one subscriber and invokes its handler
//! until shutdown is signalled or the stream ends. Lifted out of the former `Router` so
//! [`RustStream`](super::RustStream) can own task spawning directly.

use std::collections::HashMap;
use std::sync::Arc;

use futures::StreamExt;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, warn};

use crate::{IncomingMessage, Subscriber};

use super::context::{Context, State};
use super::handler::{Handler, HandlerResult};
use super::publish::PublishMiddleware;
use super::publisher_registry::ErasedPublisher;

/// Named publishers registered on the application, resolvable from a [`Context`] by name.
pub(crate) type Publishers = HashMap<String, Arc<dyn ErasedPublisher>>;

/// Per-scope publish context threaded into every delivery's [`Context`]: the named-publisher
/// registry and the scope's publish middleware pipeline. A handler resolves a publisher with
/// [`Context::publisher`] and its sends run through `pipeline`, the same chain as a macro reply.
pub(crate) struct Delivery {
    pub(crate) publishers: Publishers,
    pub(crate) pipeline: Arc<[Arc<dyn PublishMiddleware>]>,
}

impl Delivery {
    /// An empty delivery context: no publishers, no pipeline. For tests.
    #[cfg(test)]
    pub(crate) fn empty() -> Self {
        Self {
            publishers: HashMap::new(),
            pipeline: Arc::from([]),
        }
    }
}

impl std::fmt::Debug for Delivery {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Delivery")
            .field("publishers", &self.publishers.len())
            .field("layers", &self.pipeline.len())
            .finish_non_exhaustive()
    }
}

/// Spawns a task that drives `subscriber` through `handler` until `shutdown` is triggered or the
/// stream terminates. Each delivery is given a [`Context`] built from `name`, the message headers,
/// shared `state`, and the `delivery` publish context.
pub(crate) fn spawn_dispatch<S, H>(
    mut subscriber: S,
    handler: Arc<H>,
    shutdown: CancellationToken,
    name: Arc<str>,
    state: Arc<State>,
    delivery: Arc<Delivery>,
) -> JoinHandle<()>
where
    S: Subscriber + Send + 'static,
    H: Handler<S::Message> + 'static,
{
    tokio::spawn(async move {
        let mut stream = std::pin::pin!(subscriber.stream());
        loop {
            tokio::select! {
                () = shutdown.cancelled() => break,
                next = stream.next() => match next {
                    Some(Ok(msg)) => dispatch(&*handler, msg, &name, &state, &delivery).await,
                    Some(Err(err)) => {
                        error!(
                            target: "ruststream::dispatch",
                            error = %err,
                            "subscriber stream error",
                        );
                    }
                    None => {
                        debug!(
                            target: "ruststream::dispatch",
                            subscriber = %name,
                            "subscriber stream ended",
                        );
                        break;
                    }
                }
            }
        }
    })
}

async fn dispatch<H, M>(handler: &H, msg: M, name: &str, state: &State, delivery: &Delivery)
where
    H: Handler<M>,
    M: IncomingMessage,
{
    let mut ctx = Context::new(name, msg.headers().clone(), state, delivery);
    let outcome = handler.handle(&msg, &mut ctx).await;
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
