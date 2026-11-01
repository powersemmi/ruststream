//! The message a runtime publish failed to send, as the `.error` continuation receives it: the
//! very message the publisher handed back, not one the runtime kept a copy of beforehand.

// A test drives the app on its own task; the app's cold path carries no Send.
#![allow(clippy::future_not_send)]
#![cfg(all(feature = "json", feature = "tokio"))]

use std::convert::Infallible;
use std::future::ready;
use std::io;
use std::sync::{Arc, Mutex};

use ruststream::memory::ConnectedMemoryBroker;
use ruststream::memory::LogMode;
use ruststream::memory::prelude::*;
use ruststream::runtime::RETRY_COUNT_HEADER;
use ruststream::testing::TestApp;
use ruststream::{BytesMut, Lend, OutgoingMessage, PairError, Take};
use ruststream_core::{PublishMessageError, PublishOutcome};
use ruststream_runtime::{Failed, Outgoing};
use serde::{Deserialize, Serialize};

use super::error::{Job, Queue, RETRY_DELAY};

/// A publisher whose broker is down and that stamps the message it hands back, as a client that
/// adds its own header before it fails does.
#[derive(Debug, Clone, Copy)]
struct StampingDown;

impl Publisher for StampingDown {
    type Payload = Lend;
    type Error = io::Error;
    type Options = ();

    fn publish<'msg>(
        &self,
        msg: OutgoingMessage<'msg, &'msg [u8]>,
        _options: Option<&()>,
    ) -> impl Future<Output = PublishOutcome<'msg, io::Error, Lend>> {
        let (name, payload, mut headers) = msg.into_parts();
        headers.insert("x-client", "stamped");
        let stamped = OutgoingMessage::with_payload(name, payload).with_headers(headers);
        ready(Err(PublishMessageError::with_message(
            io::Error::other("broker down"),
            stamped,
        )))
    }
}

#[derive(Debug, Clone, Copy)]
struct StampingDownPublish;

impl<Log: LogMode> PublishPolicy<ConnectedMemoryBroker<Log>> for StampingDownPublish {
    type Live = StampingDown;

    fn pair(
        self,
        _connected: &ConnectedMemoryBroker<Log>,
    ) -> impl Future<Output = Result<StampingDown, PairError>> {
        ready(Ok(StampingDown))
    }
}

/// What the continuation of a handler recorded about the message that failed.
#[derive(Clone, Default, FromRef)]
struct Seen {
    log: Arc<Mutex<Log>>,
}

#[derive(Default)]
struct Log {
    headers: Vec<(String, String)>,
    payloads: Vec<Vec<u8>>,
    consumed: Vec<bool>,
}

fn record(log: &Mutex<Log>, failed: &Failed) {
    let message = failed.message();
    {
        let mut log = log.lock().expect("log");
        for (key, value) in message.headers().iter() {
            log.headers
                .push((key.to_owned(), String::from_utf8_lossy(value).into_owned()));
        }
        log.consumed.push(message.payload().is_none());
        log.payloads
            .push(message.payload().map(|bytes| bytes.to_vec()).unwrap_or_default());
    }
}

impl Seen {
    fn header(&self, key: &str) -> Option<String> {
        let log = self.log.lock().expect("log");
        log.headers
            .iter()
            .find(|(name, _)| name == key)
            .map(|(_, value)| value.clone())
    }
}

#[subscriber(Queue { name: "relay-retry" })]
async fn retrying(_job: &Job, State(seen): State<Arc<Mutex<Log>>>) -> HandlerOutcome {
    HandlerOutcome::retry_after(RETRY_DELAY).error(async move |failures: Failures| {
        for failed in failures {
            record(&seen, &failed);
        }
    })
}

/// The copy the publisher refused comes back to `.error` as the publisher returned it: its own
/// header is there, beside the retry count the runtime wrote.
#[tokio::test(start_paused = true)]
async fn a_refused_copy_reaches_error_as_the_publisher_returned_it() {
    let seen = Seen::default();
    let startup = seen.clone();
    let app = RustStream::new(AppInfo::new("failed", "0.1.0"))
        .on_startup(async move |()| Ok::<_, Infallible>(startup))
        .with_broker(MemoryBroker::new(), |b| {
            b.include(retrying).out_retry(StampingDownPublish)
        });
    let tb = TestApp::start(app).await.expect("harness start");

    tb.broker::<MemoryBroker>()
        .message(&Job { id: 3 })
        .to("relay-retry")
        .publish()
        .await
        .expect("publish");
    tb.advance(RETRY_DELAY).await.expect("advance");

    assert_eq!(seen.header("x-client").as_deref(), Some("stamped"));
    assert_eq!(seen.header(RETRY_COUNT_HEADER).as_deref(), Some("1"));
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema, Outgoing)]
#[outgoing(name = "answers")]
struct Answer {
    id: u32,
}

/// A reply transform: stamps every reply it sees with the header `x-stamp`.
#[derive(Clone)]
struct Stamp;

impl<K: ContextKind, Options> PublishTransform<K, Options> for Stamp {
    type Destination = Reads;

    fn apply(&self, out: &mut Outgoing<'_>, _options: &mut Option<Options>, _cx: &K::View<'_>) {
        out.headers_mut().insert("x-stamp", "yes");
    }
}

#[subscriber("ask-stamped", reply)]
async fn ask(job: &Job, ctx: &mut Context, State(seen): State<Arc<Mutex<Log>>>) -> Answer {
    ctx.after(HandlerOutcome::ack())
        .error(async move |failures: Failures| {
            for failed in failures {
                record(&seen, &failed);
            }
        });
    Answer { id: job.id }
}

/// A reply the broker refuses comes back to `.error` as it was sent: the bytes the codec wrote
/// and the header a publish transform stamped on it.
#[tokio::test(start_paused = true)]
async fn a_refused_reply_reaches_error_with_what_the_transforms_wrote() {
    let seen = Seen::default();
    let startup = seen.clone();
    let app = RustStream::new(AppInfo::new("failed", "0.1.0"))
        .on_startup(async move |()| Ok::<_, Infallible>(startup))
        .with_broker(MemoryBroker::new(), |b| {
            b.include(ask)
                .out_reply(StampingDownPublish)
                .transform(Stamp)
        });
    let tb = TestApp::start(app).await.expect("harness start");

    tb.broker::<MemoryBroker>()
        .message(&Job { id: 5 })
        .to("ask-stamped")
        .publish()
        .await
        .expect("publish");
    tb.settle().await.expect("settle");

    assert_eq!(seen.header("x-stamp").as_deref(), Some("yes"));
    assert_eq!(seen.header("x-client").as_deref(), Some("stamped"));
    assert_eq!(
        seen.log.lock().expect("log").payloads,
        [br#"{"id":5}"#.to_vec()],
    );
}

/// A publisher that takes the buffer and gives nothing back when it fails.
#[derive(Debug, Clone, Copy)]
struct ConsumingDown;

impl Publisher for ConsumingDown {
    type Payload = Take;
    type Error = io::Error;
    type Options = ();

    fn publish<'msg>(
        &self,
        _msg: OutgoingMessage<'msg, BytesMut>,
        _options: Option<&()>,
    ) -> impl Future<Output = PublishOutcome<'msg, io::Error, Take>> {
        ready(Err(PublishMessageError::new(io::Error::other("broker down"))))
    }
}

#[derive(Debug, Clone, Copy)]
struct ConsumingDownPublish;

impl<Log: LogMode> PublishPolicy<ConnectedMemoryBroker<Log>> for ConsumingDownPublish {
    type Live = ConsumingDown;

    fn pair(
        self,
        _connected: &ConnectedMemoryBroker<Log>,
    ) -> impl Future<Output = Result<ConsumingDown, PairError>> {
        ready(Ok(ConsumingDown))
    }
}

/// A reply whose buffer the publisher consumed reaches `.error` with no bytes, not an empty
/// payload that reads as a real one.
#[tokio::test(start_paused = true)]
async fn a_consumed_buffer_reaches_error_as_no_payload() {
    let seen = Seen::default();
    let startup = seen.clone();
    let app = RustStream::new(AppInfo::new("failed", "0.1.0"))
        .on_startup(async move |()| Ok::<_, Infallible>(startup))
        .with_broker(MemoryBroker::new(), |b| {
            b.include(ask).out_reply(ConsumingDownPublish)
        });
    let tb = TestApp::start(app).await.expect("harness start");

    tb.broker::<MemoryBroker>()
        .message(&Job { id: 6 })
        .to("ask-stamped")
        .publish()
        .await
        .expect("publish");
    tb.settle().await.expect("settle");

    assert_eq!(seen.log.lock().expect("log").consumed, [true]);
}
