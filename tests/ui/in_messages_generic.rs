use ruststream::prelude::*;

// A set names its models: a generic parameter would leave them open.
#[derive(InMessages)]
enum OrderEvent<T> {
    Created(T),
}

fn main() {}
