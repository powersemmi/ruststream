//! The per-subscriber dispatch loop: pulls messages off one subscriber and invokes its handler
//! until shutdown is signalled or the stream ends. Lifted out of the former `Router` so
//! [`RustStream`](super::RustStream) can own task spawning directly.

use std::sync::Arc;

use futures::StreamExt;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::{error, warn};

use crate::{IncomingMessage, Subscriber};

use super::handler::{Handler, HandlerResult};

/// Spawns a task that drives `subscriber` through `handler` until `shutdown` is triggered or the
/// stream terminates.
pub(crate) fn spawn_dispatch<S, H>(
    mut subscriber: S,
    handler: Arc<H>,
    shutdown: CancellationToken,
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
    })
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
