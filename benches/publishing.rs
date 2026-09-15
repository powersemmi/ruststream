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
//! The sending side: what a reply, a slot publish, a typed header contract and a request round
//! trip cost over the same in-process transport.
//!
//! Per-message publish settings are not in these numbers. They are the broker's own type
//! (`Publisher::Options`), and the in-memory broker declares `()`, so the core can only measure
//! the position where a setting would be resolved, never the resolving. A broker crate measures
//! that with its own options type.

mod common;

use std::convert::Infallible;
use std::hint::black_box;
use std::time::Duration;

use common::{Latch, MESSAGES};
use futures::StreamExt;
use gungraun::{library_benchmark, library_benchmark_group, main};
use ruststream::memory::prelude::*;
use ruststream::memory::{
    MemoryBroker, MemoryMessage, MemoryPublisher, MemoryRequester, MemorySubscriber,
};
use ruststream::runtime::{
    BrokerScope, ContextKind, ForReply, Identity, Names, Outgoing as OutgoingMessageView,
    PublishContext, PublishTransform, Reads, RunningApp,
};
use ruststream::{HeaderMap, IncomingMessage, OutgoingMessage, RequestReply, Subscriber};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use tokio::runtime::Runtime;

/// How long a request waits for its reply. Nothing here is slow, so the value only bounds a
/// hang; it never expires in a healthy run.
const REPLY_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Debug, Deserialize)]
struct Order {
    id: u64,
    quantity: u32,
}

/// A reply with a destination of its own: the mount site adds nothing to it.
#[derive(Debug, Serialize, Outgoing)]
#[outgoing(name = "confirmations")]
struct Confirmation {
    id: u64,
}

/// An event the call site names a destination for.
#[derive(Debug, Serialize, Outgoing)]
struct Event {
    id: u64,
}

/// The header contract the typed scenarios write and read.
#[derive(Debug, Serialize, Deserialize, JsonSchema)]
struct Meta {
    task_id: u64,
    chunk_no: u32,
}

/// The same event with a contract attached: the publish does not compile without the headers.
#[derive(Debug, Serialize, Outgoing, JsonSchema)]
#[outgoing(name = "events", headers = Meta)]
struct Stamped {
    id: u64,
}

#[derive(OutSlot)]
#[publishes(Event)]
struct Audit;

#[derive(OutSlot)]
#[publishes(Stamped)]
struct Stamps;

/// One transform on the publish path: a constant header, which is the cheapest step there is, so
/// what the pair shows is the position rather than the work inside it.
struct Stamp;

impl<K: ContextKind, Options> PublishTransform<K, Options> for Stamp {
    type Destination = Reads;

    fn apply(
        &self,
        out: &mut OutgoingMessageView<'_>,
        _options: &mut Option<Options>,
        _cx: &K::View<'_>,
    ) {
        out.headers_mut().insert("x-bench", b"1".to_vec());
    }
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

// --- the service side -------------------------------------------------------------------

#[subscriber("orders", publish)]
async fn confirm(order: &Order, ctx: &mut Context<'_, (), Latch>) -> Confirmation {
    ctx.state().arrived();
    Confirmation {
        id: black_box(order.id),
    }
}

#[subscriber("orders")]
async fn audit(
    order: &Order,
    ctx: &mut Context<'_, (), Latch>,
    Out(out): Out<impl Publisher, Audit>,
) -> HandlerOutcome {
    ctx.state().arrived();
    if out
        .message(&Event {
            id: black_box(order.id),
        })
        .to("events")
        .publish()
        .await
        .is_err()
    {
        return HandlerOutcome::retry();
    }
    HandlerOutcome::ack()
}

#[subscriber("orders")]
async fn stamp(
    order: &Order,
    ctx: &mut Context<'_, (), Latch>,
    Out(out): Out<impl Publisher, Stamps>,
) -> HandlerOutcome {
    ctx.state().arrived();
    let meta = Meta {
        task_id: order.id,
        chunk_no: order.quantity,
    };
    if out
        .message(&Stamped {
            id: black_box(order.id),
        })
        .with_headers(&meta)
        .publish()
        .await
        .is_err()
    {
        return HandlerOutcome::retry();
    }
    HandlerOutcome::ack()
}

#[subscriber("orders")]
async fn read_meta(
    order: &Order,
    ctx: &mut Context<'_, (), Latch>,
    Headers(meta): Headers<Meta>,
    Out(out): Out<impl Publisher, Audit>,
) -> HandlerOutcome {
    ctx.state().arrived();
    black_box(meta.task_id);
    if out
        .message(&Event {
            id: black_box(order.id),
        })
        .to("events")
        .publish()
        .await
        .is_err()
    {
        return HandlerOutcome::retry();
    }
    HandlerOutcome::ack()
}

/// The reply of a request round trip. It declares no destination, which is what lets the
/// mount site's transform name one per delivery; the clause's name is the fallback.
#[derive(Debug, Serialize, Outgoing)]
struct Answer {
    id: u64,
}

/// The replying half of a request round trip: the reply goes back to the inbox the requester
/// named, which is what the `ReplyTo` transform reads off the delivery.
#[subscriber("orders", publish("confirmations"))]
async fn answer(order: &Order) -> Answer {
    Answer {
        id: black_box(order.id),
    }
}

// --- setups -----------------------------------------------------------------------------

/// A started service whose queue is full, with the latch its handlers count down.
struct Service {
    runtime: Runtime,
    latch: Latch,
    _app: RunningApp,
}

/// The request side: a requester and the service that answers it.
struct Requests {
    runtime: Runtime,
    requester: MemoryRequester,
    messages: usize,
    _app: RunningApp,
}

/// The hand-written side: the subscription, a publisher for what the loop sends on, and the
/// count. The subscription is opened before the queue is filled, because an in-memory
/// subscription receives what is published after it, exactly as a broker's does.
struct Queue {
    runtime: Runtime,
    subscriber: MemorySubscriber,
    publisher: MemoryPublisher,
    requester: MemoryRequester,
    messages: usize,
}

fn service<Mount>(messages: usize, headers: bool, mount: Mount) -> Service
where
    Mount: FnOnce(&mut BrokerScope<MemoryBroker, Identity, (), Latch>),
{
    let runtime = common::runtime();
    let latch = Latch::default();
    let broker = MemoryBroker::new();
    let state = latch.clone();
    let app = RustStream::new(AppInfo::new("bench", "0.0.0"))
        .on_startup(async move |()| Ok::<_, Infallible>(state))
        .with_broker(broker.clone(), mount);
    let running = runtime.block_on(app.start()).expect("the service starts");
    latch.expect(messages);
    fill(&broker.publisher(), &runtime, messages, headers);
    Service {
        runtime,
        latch,
        _app: running,
    }
}

/// Publishes the inputs, with the header contract on the wire when the scenario reads it.
fn fill(publisher: &MemoryPublisher, runtime: &Runtime, messages: usize, headers: bool) {
    if headers {
        common::fill_with_headers(
            publisher,
            runtime,
            "orders",
            messages,
            &[("task_id", "17"), ("chunk_no", "3")],
        );
    } else {
        common::fill(publisher, runtime, "orders", messages, 0);
    }
}

fn replying(messages: usize) -> Service {
    service(messages, false, |b| {
        b.include(confirm);
    })
}

fn auditing(messages: usize) -> Service {
    service(messages, false, |b| {
        b.include(audit)
            .out(Audit, Publish)
            .transform(Stamp)
            .build();
    })
}

fn stamping(messages: usize) -> Service {
    service(messages, false, |b| {
        b.include(stamp).out(Stamps, Publish).build();
    })
}

fn reading(messages: usize) -> Service {
    service(messages, true, |b| {
        b.include(read_meta).out(Audit, Publish).build();
    })
}

fn requesting(messages: usize) -> Requests {
    let runtime = common::runtime();
    let broker = MemoryBroker::new();
    let app = RustStream::new(AppInfo::new("bench", "0.0.0")).with_broker(broker.clone(), |b| {
        b.include(answer).out_reply(Publish).transform(ReplyTo);
    });
    let running = runtime.block_on(app.start()).expect("the service starts");
    Requests {
        runtime,
        requester: broker.requester(),
        messages,
        _app: running,
    }
}

fn queue(messages: usize, headers: bool) -> Queue {
    let queue = unfilled(messages);
    fill(&queue.publisher, &queue.runtime, messages, headers);
    queue
}

/// The same subscription with nothing in the queue yet: the request scenarios publish their
/// input from the measured body, one request at a time.
fn unfilled(messages: usize) -> Queue {
    let runtime = common::runtime();
    let broker = MemoryBroker::new();
    let subscriber = broker.subscribe("orders");
    Queue {
        runtime,
        subscriber,
        publisher: broker.publisher(),
        requester: broker.requester(),
        messages,
    }
}

// --- the hand-written steps ---------------------------------------------------------------

/// Decodes the delivery and encodes the answer, which is what the reply position does.
fn reply_by_hand(message: &MemoryMessage, latch: &Latch) -> Vec<u8> {
    let order: Order = serde_json::from_slice(message.payload()).expect("a decodable body");
    latch.arrived();
    serde_json::to_vec(&Confirmation {
        id: black_box(order.id),
    })
    .expect("an encodable reply")
}

fn event_by_hand(message: &MemoryMessage, latch: &Latch) -> Vec<u8> {
    let order: Order = serde_json::from_slice(message.payload()).expect("a decodable body");
    latch.arrived();
    serde_json::to_vec(&Event {
        id: black_box(order.id),
    })
    .expect("an encodable event")
}

/// The same read a typed contract does, written out: two header lookups and two parses.
fn meta_by_hand(message: &MemoryMessage) -> Meta {
    let headers = message.headers();
    let task_id = headers
        .get("task_id")
        .and_then(|value| std::str::from_utf8(value).ok())
        .and_then(|value| value.parse().ok())
        .expect("the task id header");
    let chunk_no = headers
        .get("chunk_no")
        .and_then(|value| std::str::from_utf8(value).ok())
        .and_then(|value| value.parse().ok())
        .expect("the chunk number header");
    Meta { task_id, chunk_no }
}

/// Publishes what the loop produced under `name`, with the headers the scenario's service
/// version puts on the message.
async fn send_by_hand(
    publisher: &MemoryPublisher,
    name: &str,
    body: &[u8],
    headers: &[(&str, &str)],
) {
    let mut map = HeaderMap::new();
    for (key, value) in headers {
        map.insert(*key, (*value).to_owned());
    }
    publisher
        .publish(OutgoingMessage::new(name, body).with_headers(map), None)
        .await
        .expect("an in-process publish");
}

// --- the measured bodies ------------------------------------------------------------------

#[library_benchmark(config = common::config(5991))]
#[bench::json(replying(MESSAGES))]
fn reply(service: Service) {
    common::measure(|| service.runtime.block_on(service.latch.drained()));
}

#[library_benchmark(config = common::config(5001))]
#[bench::json(queue(MESSAGES, false))]
fn reply_hand(queue: Queue) {
    let Queue {
        runtime,
        mut subscriber,
        publisher,
        messages,
        ..
    } = queue;
    let latch = Latch::default();
    latch.expect(messages);
    common::measure(|| {
        runtime.block_on(async {
            let mut stream = std::pin::pin!(subscriber.stream());
            for _ in 0..messages {
                let message = stream
                    .next()
                    .await
                    .expect("a delivery")
                    .expect("a delivery");
                let body = reply_by_hand(&message, &latch);
                send_by_hand(&publisher, "confirmations", &body, &[]).await;
                message.ack().await.expect("the ack");
            }
        });
    });
}

#[library_benchmark(config = common::config(14991))]
#[bench::one_transform(auditing(MESSAGES))]
fn out_slot(service: Service) {
    common::measure(|| service.runtime.block_on(service.latch.drained()));
}

#[library_benchmark(config = common::config(11001))]
#[bench::one_transform(queue(MESSAGES, false))]
fn out_slot_hand(queue: Queue) {
    let Queue {
        runtime,
        mut subscriber,
        publisher,
        messages,
        ..
    } = queue;
    let latch = Latch::default();
    latch.expect(messages);
    common::measure(|| {
        runtime.block_on(async {
            let mut stream = std::pin::pin!(subscriber.stream());
            for _ in 0..messages {
                let message = stream
                    .next()
                    .await
                    .expect("a delivery")
                    .expect("a delivery");
                let body = event_by_hand(&message, &latch);
                send_by_hand(&publisher, "events", &body, &[("x-bench", "1")]).await;
                message.ack().await.expect("the ack");
            }
        });
    });
}

#[library_benchmark(config = common::config(15991))]
#[bench::write(stamping(MESSAGES))]
fn typed_headers_write(service: Service) {
    common::measure(|| service.runtime.block_on(service.latch.drained()));
}

#[library_benchmark(config = common::config(15001))]
#[bench::write(queue(MESSAGES, false))]
fn typed_headers_write_hand(queue: Queue) {
    let Queue {
        runtime,
        mut subscriber,
        publisher,
        messages,
        ..
    } = queue;
    let latch = Latch::default();
    latch.expect(messages);
    common::measure(|| {
        runtime.block_on(async {
            let mut stream = std::pin::pin!(subscriber.stream());
            for _ in 0..messages {
                let message = stream
                    .next()
                    .await
                    .expect("a delivery")
                    .expect("a delivery");
                let body = event_by_hand(&message, &latch);
                let headers = [("task_id", "17"), ("chunk_no", "3")];
                send_by_hand(&publisher, "events", &body, &headers).await;
                message.ack().await.expect("the ack");
            }
        });
    });
}

#[library_benchmark(config = common::config(5991))]
#[bench::read(reading(MESSAGES))]
fn typed_headers_read(service: Service) {
    common::measure(|| service.runtime.block_on(service.latch.drained()));
}

#[library_benchmark(config = common::config(5001))]
#[bench::read(queue(MESSAGES, true))]
fn typed_headers_read_hand(queue: Queue) {
    let Queue {
        runtime,
        mut subscriber,
        publisher,
        messages,
        ..
    } = queue;
    let latch = Latch::default();
    latch.expect(messages);
    common::measure(|| {
        runtime.block_on(async {
            let mut stream = std::pin::pin!(subscriber.stream());
            for _ in 0..messages {
                let message = stream
                    .next()
                    .await
                    .expect("a delivery")
                    .expect("a delivery");
                black_box(meta_by_hand(&message).task_id);
                let body = event_by_hand(&message, &latch);
                send_by_hand(&publisher, "events", &body, &[]).await;
                message.ack().await.expect("the ack");
            }
        });
    });
}

#[library_benchmark(config = common::config(20001))]
#[bench::round_trip(requesting(MESSAGES))]
fn request_reply(requests: Requests) {
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
                    .request(OutgoingMessage::new("orders", &body), REPLY_TIMEOUT)
                    .await
                    .expect("a reply");
                black_box(reply.payload()[0]);
            }
        });
    });
}

#[library_benchmark(config = common::config(20002))]
#[bench::round_trip(unfilled(MESSAGES))]
fn request_reply_hand(queue: Queue) {
    let Queue {
        runtime,
        mut subscriber,
        publisher,
        requester,
        messages,
    } = queue;
    let latch = Latch::default();
    latch.expect(messages);
    let body = common::json_body(1, 0);
    common::measure(|| {
        runtime.block_on(async {
            // The answering loop runs as a task of its own, the way the dispatcher does on the
            // other half of the pair: a requester waiting for a reply cannot also be serving the
            // queue.
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
                    let body = reply_by_hand(&message, &latch);
                    send_by_hand(&publisher, &inbox, &body, &[]).await;
                    message.ack().await.expect("the ack");
                }
            };
            let requesting = async {
                for _ in 0..messages {
                    let reply = requester
                        .request(OutgoingMessage::new("orders", &body), REPLY_TIMEOUT)
                        .await
                        .expect("a reply");
                    black_box(reply.payload()[0]);
                }
            };
            futures::join!(serving, requesting);
        });
    });
}

library_benchmark_group!(
    name = publishing;
    benchmarks = reply, reply_hand, out_slot, out_slot_hand, typed_headers_write,
        typed_headers_write_hand, typed_headers_read, typed_headers_read_hand, request_reply,
        request_reply_hand
);
main!(library_benchmark_groups = publishing);
