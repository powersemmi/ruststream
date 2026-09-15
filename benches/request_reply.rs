// The harness macros generate the group module, its items and the paths between them, and a
// benchmark function takes its setup value by value because the harness owns the drop; the
// crate's lints are written for the library surface, not for generated benchmark scaffolding.
#![allow(
    missing_docs,
    unused_qualifications,
    unreachable_pub,
    clippy::must_use_candidate,
    clippy::needless_pass_by_value
)]
//! A request round trip: the requester waits for the answer on a single-use inbox, and the reply
//! goes back to the inbox the request named, which a transform on the reply position reads off
//! the delivery.

mod common;

use std::hint::black_box;
use std::time::Duration;

use common::{Latch, MESSAGES, Order, Queue};
use futures::StreamExt;
use gungraun::{library_benchmark, library_benchmark_group, main};
use ruststream::memory::prelude::*;
use ruststream::memory::{MemoryBroker, MemoryRequester};
use ruststream::runtime::{
    ForReply, Names, Outgoing as OutgoingMessageView, PublishContext, PublishTransform, RunningApp,
};
use ruststream::{IncomingMessage, OutgoingMessage, RequestReply, Subscriber};
use serde::Serialize;
use tokio::runtime::Runtime;

/// How long a request waits for its reply. Nothing here is slow, so the value only bounds a
/// hang; it never expires in a healthy run.
const REPLY_TIMEOUT: Duration = Duration::from_secs(5);

/// The reply declares no destination, which is what lets the mount site's transform name one per
/// delivery; the clause's name is the fallback.
#[derive(Debug, Serialize, Outgoing)]
struct Answer {
    id: u64,
}

/// Answers where the request asked to be answered.
struct ReplyTo;

impl<C, Options> PublishTransform<ForReply<C>, Options> for ReplyTo {
    type Destination = Names;

    fn apply(
        &self,
        out: &mut OutgoingMessageView<'_>,
        _options: &mut Option<Options>,
        cx: &PublishContext<'_, C>,
    ) {
        if let Some(to) = cx.headers().get("reply-to")
            && let Ok(to) = std::str::from_utf8(to)
        {
            out.set_name(to.to_owned());
        }
    }
}

#[subscriber("orders", publish("confirmations"))]
async fn answer(order: &Order) -> Answer {
    Answer {
        id: black_box(order.id),
    }
}

/// The request side: a requester and the service that answers it.
struct Requests {
    runtime: Runtime,
    requester: MemoryRequester,
    messages: usize,
    _app: RunningApp,
}

fn app(messages: usize) -> Requests {
    let runtime = common::runtime();
    let broker = MemoryBroker::new();
    let app = RustStream::new(AppInfo::new("bench", "0.0.0")).with_broker(broker.clone(), |b| {
        b.include(answer).out_reply(Publish).transform(ReplyTo);
    });
    let app = runtime.block_on(app.start()).expect("the service starts");
    Requests {
        runtime,
        requester: broker.requester(),
        messages,
        _app: app,
    }
}

/// Decodes the request and encodes the answer, which is what the reply position does.
fn step(payload: &[u8], latch: &Latch) -> Vec<u8> {
    let order: Order = serde_json::from_slice(payload).expect("a decodable body");
    latch.arrived();
    serde_json::to_vec(&Answer {
        id: black_box(order.id),
    })
    .expect("an encodable reply")
}

#[library_benchmark(config = common::config(20001))]
#[bench::round_trip(app(MESSAGES))]
fn service(requests: Requests) {
    let Requests {
        runtime,
        requester,
        messages,
        ..
    } = requests;
    let body = common::json_body(1, 0);
    common::measure(|| {
        runtime.block_on(async {
            for _ in 0..messages {
                let reply = requester
                    .request(OutgoingMessage::new(common::INPUT, &body), REPLY_TIMEOUT)
                    .await
                    .expect("a reply");
                black_box(reply.payload()[0]);
            }
        });
    });
}

#[library_benchmark(config = common::config(20002))]
#[bench::round_trip(common::queue_unfilled(MESSAGES))]
fn by_hand(queue: Queue) {
    let Queue {
        runtime,
        mut subscriber,
        publisher,
        requester,
        messages,
    } = queue;
    let latch = Latch::default();
    latch.expect(messages);
    let request = common::json_body(1, 0);
    common::measure(|| {
        runtime.block_on(async {
            // The answering loop is driven beside the requester rather than after it: a requester
            // waiting for a reply cannot also be serving the queue.
            let serving = async {
                let mut stream = std::pin::pin!(subscriber.stream());
                for _ in 0..messages {
                    let message = stream
                        .next()
                        .await
                        .expect("a delivery")
                        .expect("a delivery");
                    let inbox = message
                        .headers()
                        .get("reply-to")
                        .and_then(|value| std::str::from_utf8(value).ok())
                        .expect("the inbox the requester named")
                        .to_owned();
                    let body = step(message.payload(), &latch);
                    common::send_by_hand(&publisher, &inbox, &body, &[]).await;
                    message.ack().await.expect("the ack");
                }
            };
            let requesting = async {
                for _ in 0..messages {
                    let reply = requester
                        .request(OutgoingMessage::new(common::INPUT, &request), REPLY_TIMEOUT)
                        .await
                        .expect("a reply");
                    black_box(reply.payload()[0]);
                }
            };
            futures::join!(serving, requesting);
        });
    });
}

library_benchmark_group!(name = request_reply; benchmarks = service, by_hand);
main!(library_benchmark_groups = request_reply);
