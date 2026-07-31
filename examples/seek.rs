//! Repositioning a live subscription with the `Seekable` capability, on the memory broker.
//!
//! The seeker handle is minted before the subscription's stream is opened; while the stream
//! runs, it can replay the log from a position captured off a delivered message or jump
//! forward past a region that should be skipped.
//!
//! ```text
//! cargo run --example seek --features memory
//! ```

use std::error::Error;

use futures::StreamExt;
use ruststream::memory::{MemoryBroker, MemoryPosition};
use ruststream::{
    IncomingMessage, OutgoingMessage, Positioned, Publisher, Seekable, Seeker, Subscriber,
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let broker = MemoryBroker::new();

    // --8<-- [start:seeker]
    let mut subscriber = broker.subscribe("audit");
    // Minted before `stream` borrows the subscriber; clonable and usable while the stream runs.
    let seeker = subscriber.seeker();
    // --8<-- [end:seeker]

    let publisher = broker.publisher();
    for i in 0..4u8 {
        publisher
            .publish(OutgoingMessage::new("audit", &[i]))
            .await?;
    }

    let mut stream = std::pin::pin!(subscriber.stream());

    // --8<-- [start:capture]
    // Capture the position of a delivery worth returning to; seeking to a captured position
    // redelivers exactly that message.
    let first = stream.next().await.expect("delivered")?;
    let replay_from = first.position();
    first.ack().await?;
    // --8<-- [end:capture]

    for _ in 0..3 {
        let msg = stream.next().await.expect("delivered")?;
        msg.ack().await?;
    }

    // --8<-- [start:seek]
    // Replay: the captured message is delivered again, then the rest of the log in order.
    seeker.seek(replay_from).await?;
    let replayed = stream.next().await.expect("replayed")?;
    assert_eq!(replayed.payload(), &[0]);
    replayed.ack().await?;
    // --8<-- [end:seek]

    // --8<-- [start:skip]
    // Jump forward with a constructed position: everything queued before it is skipped.
    seeker.seek(MemoryPosition::sequence(3)).await?;
    let skipped_to = stream.next().await.expect("delivered")?;
    assert_eq!(skipped_to.payload(), &[3]);
    skipped_to.ack().await?;
    // --8<-- [end:skip]

    println!("ok: replayed from a captured position and skipped forward");
    Ok(())
}
