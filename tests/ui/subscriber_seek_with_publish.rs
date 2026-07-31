use ruststream::memory::MemorySeeker;
use ruststream::runtime::Seek;
use ruststream::subscriber;
use serde::Deserialize;

#[derive(Deserialize)]
struct Order {
    id: u32,
}

// A Seek parameter only composes with the plain subscriber form for now.
#[subscriber("orders", publish("echo"))]
async fn handle(order: &Order, Seek(_seeker): Seek<MemorySeeker>) -> u32 {
    order.id
}

fn main() {}
