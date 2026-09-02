use ruststream::Deserialized;

// The mnemonic's typo guard, the other direction: the participle derive is for a payload view,
// so a plain decoded-looking struct cannot take it by accident.
#[derive(Deserialized)]
struct Order {
    id: u64,
}

fn main() {}
