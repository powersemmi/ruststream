//! A `#![no_std]` crate that uses every derive and the `#[subscriber]` attribute.
//!
//! A crate without the standard library prelude resolves no `::std` path and no `String`, `Vec`,
//! `Box` or `format!` of its own, so it compiles only while the macros emit those through the
//! facade.

#![no_std]
// Nothing calls these items: they exist to be compiled.
#![allow(dead_code)]

extern crate alloc;

use alloc::vec::Vec;
use core::fmt;

use ruststream::prelude::*;
use ruststream::{Deserialized, FromRef, InMessages, OutMessages, Serialized};
use serde::{Deserialize, Serialize};

/// An order as it arrives.
#[derive(Debug, Deserialize, MessageInfo)]
struct Order {
    /// The order's identifier.
    id: u64,
}

/// The typed headers an accepted order carries.
#[derive(Debug, Serialize, Deserialize)]
struct AcceptedMeta {
    /// The tenant the order belongs to.
    tenant: u32,
}

/// The reply to an order, at a fixed name with a header contract.
#[derive(Debug, Serialize, Outgoing)]
#[outgoing(name = "orders.accepted", headers = AcceptedMeta)]
struct Accepted {
    /// The accepted order.
    id: u64,
}

/// A notice published under a templated name, rendered per publish.
#[derive(Debug, Serialize, Outgoing)]
#[outgoing(name = "orders.{tenant}.shipped")]
struct Shipped {
    /// The shipped order.
    id: u64,
}

/// A frame that reads its own bytes.
#[derive(Deserialized)]
struct Frame<'a>(&'a [u8]);

/// A frame that writes its own bytes.
#[derive(Outgoing, Serialized)]
#[outgoing(name = "frames.out")]
struct Export(Vec<u8>);

/// A reading that writes and reads its own two bytes.
#[derive(Debug, Deserialized, Serialized, Outgoing)]
#[outgoing(name = "readings.out")]
#[wire(encode = write_reading, decode = read_reading)]
struct Reading {
    celsius: i16,
}

/// A payload too short to hold a reading.
#[derive(Debug)]
struct ShortReading;

impl fmt::Display for ShortReading {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("a reading takes two bytes")
    }
}

impl core::error::Error for ShortReading {}

fn write_reading(reading: &Reading, buf: &mut BytesMut) {
    buf.extend_from_slice(&reading.celsius.to_be_bytes());
}

fn read_reading(payload: &[u8]) -> Result<Reading, ShortReading> {
    let bytes = payload.first_chunk::<2>().ok_or(ShortReading)?;
    Ok(Reading {
        celsius: i16::from_be_bytes(*bytes),
    })
}

/// The slot the order handler publishes notices through.
#[derive(OutSlot)]
#[publishes(Shipped)]
struct Notices;

/// The messages the slot sends, as a declared set.
#[derive(OutMessages)]
enum NoticeSet {
    /// A shipped order.
    Shipped(Shipped),
}

/// The events one subscription receives, chosen by a header.
#[derive(InMessages)]
#[in_messages(header = "event")]
enum OrderEvent {
    /// A new order.
    #[in_messages(value = "created")]
    Created(Order),
}

/// A count the state hands to handlers.
#[derive(Clone, Debug, Default)]
struct Seen(u64);

/// The application state.
#[derive(Clone, Debug, Default, FromRef)]
struct AppState {
    /// What the handlers have seen.
    seen: Seen,
}

/// Answers one order.
#[subscriber("orders", reply)]
async fn accept(order: &Order) -> Accepted {
    Accepted { id: order.id }
}

/// Answers a batch of orders.
#[subscriber("orders.batch", reply)]
async fn accept_batch(orders: &[Order]) -> Vec<Accepted> {
    orders
        .iter()
        .map(|order| Accepted { id: order.id })
        .collect()
}

/// Publishes through a slot.
#[subscriber("orders.ship")]
async fn ship(
    order: &Order,
    Out(notices): Out<impl Publisher, Notices, NoticeSet>,
) -> HandlerOutcome {
    let _ = (order, notices);
    HandlerOutcome::ack()
}

/// Receives a declared set.
#[subscriber("orders.events")]
async fn on_event(event: &OrderEvent) -> HandlerOutcome {
    let OrderEvent::Created(_order) = event;
    HandlerOutcome::ack()
}

/// Relays raw frames.
#[subscriber("frames", reply("frames.out"))]
async fn relay(frame: &Frame<'_>) -> Export {
    Export(frame.0.to_vec())
}

/// Answers a reading with the same reading.
#[subscriber("readings", reply)]
async fn echo(reading: &Reading) -> Reading {
    Reading {
        celsius: reading.celsius,
    }
}

/// The application, with the entry point `#[ruststream::app]` writes beside it.
#[ruststream::app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("probe", "0.1.0"))
}
