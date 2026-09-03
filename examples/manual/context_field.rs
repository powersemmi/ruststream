//! The per-delivery context's compile-time field keys without the `macros` feature: a
//! broker-supplied context type and the zero-sized `Field` key that reads one of its fields. No
//! attribute is involved on either path - a broker crate ships the key, and a handler reads
//! through it with `ctx.context(Offset)` whether the handler was generated or written by hand.
//! Here the key stands alone so the mechanism compiles on its own.
//!
//! ```text
//! cargo run --example manual_context_field --no-default-features
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
    // In a hand-written handler the key resolves through the context as `ctx.context(Offset)` -
    // the same call the generated handler makes; standalone, the same key reads the field
    // directly off the context value.
    let delivery = Delivery { offset: 42 };
    assert_eq!(Offset.get(&delivery), 42);
    println!("offset = {}", Offset.get(&delivery));
}
