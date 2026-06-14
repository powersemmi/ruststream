use ruststream::subscriber;

// Only `publish(..)`, `workers(..)`, and `on_failure(..)` clauses follow the source; anything else is rejected.
#[subscriber("orders", frobnicate)]
async fn handle(order: &u8) {}

fn main() {}
