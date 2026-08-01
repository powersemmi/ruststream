use ruststream::memory::MemorySeeker;
use ruststream::runtime::{HandlerResult, Seek};
use ruststream::subscriber;
use serde::Deserialize;

#[derive(Deserialize)]
struct Order {
    id: u32,
}

// One subscription, one seeker: a second Seek parameter has nothing extra to bind to.
#[subscriber("orders")]
async fn handle(
    order: &Order,
    Seek(_a): Seek<MemorySeeker>,
    Seek(_b): Seek<MemorySeeker>,
) -> HandlerResult {
    let _ = order.id;
    HandlerResult::Ack
}

fn main() {}
