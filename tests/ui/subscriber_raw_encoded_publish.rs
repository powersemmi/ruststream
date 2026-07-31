use ruststream::subscriber;

// A raw handler's reply is bytes and never encoded: the encoded publish(..) clause is rejected
// with the fix - publish_raw(..).
#[subscriber("frames", raw, publish("out"))]
async fn handle(frame: &[u8]) -> Vec<u8> {
    frame.to_vec()
}

fn main() {}
