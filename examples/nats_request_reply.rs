//! Request-reply over Core NATS: the publisher implements the `RequestReply` capability.
//!
//! `request` publishes with a reply inbox and resolves with the reply message, bounded by the
//! caller's timeout. Start a responder first (any NATS service; here the `nats` CLI), then run
//! the example - after startup it sends one request and prints the reply:
//!
//! ```text
//! nats reply questions 'pong'          # in another terminal
//! cargo run --example nats_request_reply --features macros -- run
//! ```

use std::time::Duration;

use ruststream::runtime::{AppInfo, RustStream};
use ruststream::{IncomingMessage, OutgoingMessage, RequestReply};
use ruststream_nats::{NatsBroker, NatsError};

#[ruststream::app]
fn app() -> RustStream {
    let broker = NatsBroker::new("nats://localhost:4222");
    // Built before connect: the publisher resolves the live connection on first use.
    let requester = broker.publisher();
    RustStream::new(AppInfo::new("requester", "0.1.0"))
        .after_startup(move |_state| async move {
            // --8<-- [start:request]
            let reply = requester
                .request(
                    OutgoingMessage::new("questions", b"what is the answer?"),
                    Duration::from_secs(2),
                )
                .await?;
            println!("reply: {}", String::from_utf8_lossy(reply.payload()));
            // --8<-- [end:request]
            Ok::<_, NatsError>(())
        })
        .register_broker(broker)
}
