use ruststream::subscriber;

// `on_failure` takes the keyword vocabulary or a `FailurePolicy` expression; a name that is
// neither resolves to nothing.
#[subscriber("orders", on_failure(panic = unknown))]
async fn bad_panic(_order: &u8) {}

fn main() {}
