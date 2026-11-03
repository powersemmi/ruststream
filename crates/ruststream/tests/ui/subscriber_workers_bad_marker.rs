use ruststream::subscriber;

// The optional second `workers(..)` argument must be the literal `by_key` marker.
#[subscriber("orders", workers(8, nope))]
async fn handle(order: &u8) {}

fn main() {}
