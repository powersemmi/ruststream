use ruststream::subscriber;

// A subscriber has at most one reply destination; a second `publish(..)` is rejected.
#[subscriber("orders", publish("a"), publish("b"))]
async fn handle(order: &u8) {}

fn main() {}
