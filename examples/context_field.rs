//! The per-delivery context's compile-time field keys, from the Context guide: a broker-supplied
//! context type and the zero-sized `Field` key that reads one of its fields. A real broker crate
//! ships these; here they stand alone so the mechanism compiles on its own.
//!
//! ```text
//! cargo run --example context_field
//! ```

// --8<-- [start:field]
use ruststream::Field;

// A broker crate ships its per-delivery context and the keys that read its fields; an application
// reads a field by key from a handler taking `&mut Context<'_, Delivery>`.
struct Delivery {
    offset: u64,
}

#[derive(Clone, Copy)]
struct Offset;

impl Field<Delivery> for Offset {
    type Value<'a> = u64;
    fn get(self, d: &Delivery) -> u64 {
        d.offset
    }
}
// --8<-- [end:field]

fn main() {
    // In a handler the key resolves through the context as `ctx.context(Offset)`; standalone, the
    // same key reads the field directly off the context value.
    let delivery = Delivery { offset: 42 };
    assert_eq!(Offset.get(&delivery), 42);
    println!("offset = {}", Offset.get(&delivery));
}
