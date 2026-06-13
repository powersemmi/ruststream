use ruststream::subscriber;

// `publish(..)` takes the reply topic as a string literal; an integer is rejected.
#[subscriber("orders", publish(123))]
async fn handle(order: &u8) {}

fn main() {}
