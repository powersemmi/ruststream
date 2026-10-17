//! The shipping side of the shop: packing a paid order into parcels, and booking a courier.

use ruststream::codec::JsonCodec;
use ruststream::memory::prelude::*;
use ruststream::schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::notifications::{Notice, Notices};
use crate::orders::Line;
use crate::payments::{INTERNAL, Paid};
use crate::state::Shop;

/// How many items fit into one parcel.
const PARCEL_CAPACITY: u32 = 10;

/// One parcel of a packed order: the SKUs in it and how many of each.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
pub(crate) struct Parcel {
    pub(crate) items: Vec<(String, u32)>,
}

/// A packed order, waiting for a courier.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize, JsonSchema, Outgoing)]
#[outgoing(name = "shipping.packed")]
pub(crate) struct Packed {
    pub(crate) order: u64,
    pub(crate) customer: String,
    pub(crate) parcels: Vec<Parcel>,
}

/// Fills parcels line by line: the computation a real packer does with weights and box sizes.
pub(crate) fn plan(lines: &[Line]) -> Vec<Parcel> {
    let mut parcels: Vec<Parcel> = Vec::new();
    let mut room = 0;
    for line in lines {
        let mut left = line.quantity;
        while left > 0 {
            if room == 0 {
                parcels.push(Parcel { items: Vec::new() });
                room = PARCEL_CAPACITY;
            }
            let take = left.min(room);
            if let Some(parcel) = parcels.last_mut() {
                parcel.items.push((line.sku.clone(), take));
            }
            left -= take;
            room -= take;
        }
    }
    parcels
}

/// Stands in for the courier's booking API: a call over the network that returns a tracking code.
async fn book_courier(packed: &Packed) -> String {
    tokio::task::yield_now().await;
    format!("TRK-{}-{}", packed.order, packed.parcels.len())
}

// --8<-- [start:pack]
/// Packs a paid order into parcels.
// Packing is computation: two threads of its own keep it off the app's runtime, which serves I/O.
#[subscriber("payments.settled", threads(2), reply)]
async fn pack(paid: &Paid) -> Packed {
    Packed {
        order: paid.order,
        customer: paid.customer.clone(),
        parcels: plan(&paid.lines),
    }
}
// --8<-- [end:pack]

// --8<-- [start:dispatch]
/// Books a courier for a packed order and tells the customer.
// Booking waits on the courier's API, so four bookings run at once.
#[subscriber("shipping.packed", workers(4))]
async fn dispatch(packed: &Packed, Out(notices): Out<impl Publisher, Notices>) -> HandlerOutcome {
    let tracking = book_courier(packed).await;
    let notice = Notice {
        customer: packed.customer.clone(),
        text: format!("Order {} is on its way: {tracking}.", packed.order),
    };
    match notices.message(&notice).publish().await {
        Ok(()) => HandlerOutcome::ack(),
        Err(_) => HandlerOutcome::retry(),
    }
}
// --8<-- [end:dispatch]

// --8<-- [start:routes]
/// The shipping side's subscribers. Both read the shop's own topics, so both decode CBOR.
pub(crate) fn routes() -> impl RouterDef<MemoryBroker, Shop> {
    Router::new()
        .with_codec(INTERNAL)
        .include(pack)
        // A reply encodes with the default codec unless its position names one.
        .out_reply(Publish)
        .codec(INTERNAL)
        .build()
        .include(dispatch)
        // A slot encodes with the router's codec unless it names one; notices stay JSON.
        .out(Notices, Publish)
        .codec(JsonCodec)
        .build()
}
// --8<-- [end:routes]

#[cfg(test)]
mod tests {
    use super::{Line, Parcel, plan};

    fn line(sku: &str, quantity: u32) -> Line {
        Line {
            sku: sku.to_owned(),
            quantity,
            price_cents: 100,
        }
    }

    #[test]
    fn a_line_larger_than_a_parcel_spills_into_the_next() {
        let parcels = plan(&[line("mug", 4), line("tea", 12)]);
        assert_eq!(
            parcels,
            [
                Parcel {
                    items: vec![("mug".to_owned(), 4), ("tea".to_owned(), 6)],
                },
                Parcel {
                    items: vec![("tea".to_owned(), 6)],
                },
            ]
        );
    }
}
