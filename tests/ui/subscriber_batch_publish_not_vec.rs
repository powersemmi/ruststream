use ruststream::subscriber;

// A batch publishing handler replies with a `Vec`; a scalar return is rejected.
#[subscriber(batch("orders"), publish("done"))]
async fn handle(orders: &[u8]) -> u8 {
    0
}

fn main() {}
