use ruststream::{BytesMut, Deserialized, Serialized};

// Both derives read the one attribute, so a type that declares only its writer cannot also be a
// self-deserializing input: the missing half is named where it is missing.
#[derive(Serialized, Deserialized)]
#[wire(encode = write_order)]
struct Order {
    id: u64,
}

fn write_order(order: &Order, buf: &mut BytesMut) {
    buf.extend_from_slice(&order.id.to_be_bytes());
}

fn main() {}
