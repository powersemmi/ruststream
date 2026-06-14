use ruststream::subscriber;

// `on_failure` expects known policy values.
#[subscriber("orders", on_failure(panic = unknown))]
async fn bad_panic(_order: &u8) {}

fn main() {}
