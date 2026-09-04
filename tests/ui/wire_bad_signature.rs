use ruststream::{BytesMut, Serialized};

// A path that exists but is not a writer for this type: the expansion calls it, so the mismatch
// is an ordinary type error at the call rather than a silently wrong encoding.
#[derive(Serialized)]
#[wire(encode = write_order)]
struct Order {
    id: u64,
}

fn write_order(id: u64, buf: &mut BytesMut) {
    buf.extend_from_slice(&id.to_be_bytes());
}

fn main() {}
