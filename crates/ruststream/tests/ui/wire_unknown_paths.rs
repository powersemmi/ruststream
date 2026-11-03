use ruststream::{Deserialized, Serialized};

// The attribute names the format's functions and nothing checks them until they are called, so a
// misspelt path has to fail at the path, naming what is missing.
#[derive(Serialized, Deserialized)]
#[wire(encode = wire_format::write, decode = wire_format::read)]
struct Order {
    id: u64,
}

fn main() {}
