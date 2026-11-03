use ruststream::Outgoing;
use serde::Serialize;

// A placeholder is the setter that binds it, so it has to be named.
#[derive(Outgoing, Serialize)]
#[outgoing(name = "orders.{}.v1")]
struct Anonymous {
    id: u32,
}

// One setter cannot bind two segments.
#[derive(Outgoing, Serialize)]
#[outgoing(name = "orders.{tenant}.{tenant}")]
struct Repeated {
    id: u32,
}

// The declared name is read at compile time, so it is a literal.
#[derive(Outgoing, Serialize)]
#[outgoing(name = 7)]
struct NotAString {
    id: u32,
}

// Every parameter is `key = value`.
#[derive(Outgoing, Serialize)]
#[outgoing(headers)]
struct BareParameter {
    id: u32,
}

// And only the two the derive knows.
#[derive(Outgoing, Serialize)]
#[outgoing(topic = "orders")]
struct UnknownParameter {
    id: u32,
}

fn main() {}
