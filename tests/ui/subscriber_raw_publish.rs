use ruststream::subscriber;

// A raw handler has no typed reply to encode, so publish(..) is rejected with it.
#[subscriber("frames", raw, publish("out"))]
async fn handle(frame: &[u8]) -> u32 {
    0
}

fn main() {}
