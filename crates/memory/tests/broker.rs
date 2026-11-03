//! Publishing through `MemoryBroker`: what a publish costs the broker.

#![cfg(feature = "std")]

use allocation_counter::measure;
use futures::{FutureExt, StreamExt};
use ruststream_core::{BytesMut, IncomingMessage, OutgoingMessage, Publisher, Settle, Subscriber};
use ruststream_memory::MemoryBroker;

fn buffer_with_room(bytes: &[u8]) -> BytesMut {
    let mut buffer = BytesMut::with_capacity(256);
    buffer.extend_from_slice(bytes);
    buffer
}

/// A buffer with spare capacity used to freeze into a second block next to the shared delivery.
#[test]
fn a_publish_costs_the_one_shared_delivery_block() {
    let broker = MemoryBroker::new();
    let mut subscriber = broker.subscribe("orders");
    let publisher = broker.publisher();
    let mut stream = std::pin::pin!(subscriber.stream());

    // The channel allocates a block of slots now and then; a publish inside one is what a
    // service pays per message.
    for _ in 0..10 {
        publisher
            .publish(
                OutgoingMessage::with_payload("orders", buffer_with_room(b"warm")),
                None,
            )
            .now_or_never()
            .unwrap()
            .unwrap();
        let warm = stream.next().now_or_never().unwrap().unwrap().unwrap();
        warm.ack().now_or_never().unwrap().unwrap();
    }

    let message = OutgoingMessage::with_payload("orders", buffer_with_room(b"payload"));
    let info = measure(|| {
        publisher
            .publish(message, None)
            .now_or_never()
            .unwrap()
            .unwrap();
    });
    assert_eq!(info.count_total, 1);

    let delivered = stream.next().now_or_never().unwrap().unwrap().unwrap();
    assert_eq!(delivered.payload(), b"payload");
}
