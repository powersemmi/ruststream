use ruststream::subscriber;

// A subscriber has at most one reply destination; a second `reply(..)` is rejected.
#[subscriber("orders", reply("a"), reply("b"))]
async fn handle(order: &u8) {}

fn main() {}
