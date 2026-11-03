use ruststream::subscriber;

// No thread at all is not a policy.
#[subscriber("orders", threads(0))]
async fn handle(order: &u8) {
    let _ = order;
}

fn main() {}
