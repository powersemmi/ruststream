use ruststream::subscriber;

// The retired `publish` spelling opens the attribute here, with the source left to the default:
// it is a clause keyword, not a subscription source, so it meets the list of accepted clauses.
#[subscriber(publish("confirmations"))]
async fn handle(order: &u8) {}

fn main() {}
