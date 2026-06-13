use ruststream::subscriber;

// Only `publish(..)` and `workers(..)` clauses follow the source; anything else is rejected.
#[subscriber("orders", frobnicate)]
async fn handle(order: &u8) {}

fn main() {}
