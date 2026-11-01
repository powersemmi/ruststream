//! A broker wrapped with `bindable()` before registration: the tokens it mints pair against the
//! one instance the wrapper registered.
#![cfg(all(feature = "memory", feature = "json"))]

use futures::StreamExt;

use ruststream::Settle;
use ruststream::memory::{MemoryBroker, MemoryPublish};
use ruststream::runtime::{AppInfo, RustStream};
use ruststream::{Broker, IncomingMessage, OutgoingMessage, Publisher, Subscriber};

/// Cloning a token duplicates the binding, not the broker: both clones pair against the one
/// instance the wrapper registered.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_cloned_token_pairs_against_the_same_broker() {
    let bindable = MemoryBroker::new().bindable();
    let mut inbox = bindable.broker().subscribe("bound.out");
    let token = bindable.bind(MemoryPublish);
    let spare = token.clone();

    assert!(
        format!("{bindable:?}").contains("MemoryBroker"),
        "the wrapper names the broker it holds",
    );
    assert!(
        format!("{token:?}").contains("MemoryPublish"),
        "the token names the policy it carries",
    );

    let app = RustStream::new(AppInfo::new("bound", "0.1.0")).with_broker(bindable, |b| b);
    let running = app.start().await.expect("startup failed");

    for (token, payload) in [(token, b"one"), (spare, b"two")] {
        let publisher = token.live().await.expect("pairing after startup failed");
        publisher
            .publish(OutgoingMessage::new("bound.out", payload), None)
            .await
            .expect("publish through the paired token failed");
    }

    let mut stream = std::pin::pin!(inbox.stream());
    let mut seen = Vec::new();
    for _ in 0..2 {
        let msg = stream
            .next()
            .await
            .expect("delivery missing")
            .expect("memory subscriber never errors");
        seen.push(msg.payload().to_vec());
        msg.ack().await.expect("ack failed");
    }
    assert_eq!(
        seen,
        vec![b"one".to_vec(), b"two".to_vec()],
        "both clones reach the broker the wrapper registered",
    );

    running.shutdown().await.expect("shutdown failed");
}
