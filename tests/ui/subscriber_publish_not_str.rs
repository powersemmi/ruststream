use ruststream::subscriber;

// `publish(..)` takes the reply topic as a string literal or a `&'static str` expression; an
// integer fails the destination's type.
#[subscriber("orders", publish(123))]
async fn handle(order: &u8) -> u8 {
    *order
}

fn main() {}
