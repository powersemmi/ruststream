use ruststream::Serialized;
use serde::Serialize;

// One value cannot ride both wires: `Serialize` puts a type on the codec lane through the
// blanket wire impls, so the derive's own serialized-wire impls collide with them - the
// conflict fires at the definition instead of a publish site silently picking a lane.
#[derive(Serialize, Serialized)]
struct Export(Vec<u8>);

fn main() {}
