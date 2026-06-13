use ruststream::subscriber;

// `batch(..)` wraps exactly one source; it is a marker, not a multi-argument constructor.
#[subscriber(batch("a", "b"))]
async fn handle(orders: &[u8]) {}

fn main() {}
