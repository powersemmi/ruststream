//! `by_key`: core 0 hashes the key and hands the delivery to the worker the key maps to.

use std::collections::BTreeMap;
use std::sync::Arc;

use futures::channel::mpsc;
use ruststream_runtime::{Workers, key_index};
use ruststream_runtime_compio::dispatch::{Layout, Mount, spawn_dispatch_workers};

use crate::support::{Msg, channel, collect, failure, hosted, nonzero, on_cores, reporting};

/// Two keys that map to different workers of two.
fn keys_on_different_workers() -> (&'static str, &'static str) {
    const KEYS: [&str; 8] = ["a", "b", "c", "d", "e", "f", "g", "h"];
    let first = KEYS[0];
    let other = KEYS
        .into_iter()
        .find(|key| key_index(key.as_bytes(), 2) != key_index(first.as_bytes(), 2))
        .expect("a key on the other worker");
    (first, other)
}

#[test]
fn a_key_keeps_its_order_and_its_core_across_workers() {
    let (first, other) = keys_on_different_workers();
    let seen = on_cores(3, async |fabric, stop, tasks| {
        let workers = Workers::keyed(nonzero(2));
        let layout = Layout::plan(
            3,
            &[Mount {
                name: "orders",
                workers,
            }],
        )
        .expect("fits");
        let (feed, subscriber) = channel();
        let (seen, mut reports) = mpsc::unbounded();
        let looped = spawn_dispatch_workers(
            subscriber,
            reporting(seen),
            stop.clone(),
            Arc::from("orders"),
            Arc::new(()),
            hosted(tasks),
            failure(&stop),
            workers,
            &fabric,
            layout.cores(0),
        )
        .expect("the workers start");
        for position in 0..200 {
            let key = if position % 2 == 0 { first } else { other };
            feed.unbounded_send(Msg::numbered(position, Some(key)))
                .expect("the loop reads");
        }
        let seen = collect(&mut reports, 200).await;
        drop(looped);
        seen
    });
    let mut by_key: BTreeMap<_, Vec<_>> = BTreeMap::new();
    for seen in &seen {
        by_key
            .entry(seen.key.clone())
            .or_default()
            .push((seen.position, seen.thread));
    }
    assert_eq!(by_key.len(), 2);
    let mut threads = Vec::new();
    for (key, handled) in &by_key {
        assert!(
            handled.windows(2).all(|pair| pair[0].0 < pair[1].0),
            "the deliveries of {key:?} were handled out of order"
        );
        assert!(
            handled.iter().all(|(_, thread)| *thread == handled[0].1),
            "the key {key:?} was handled on more than one core"
        );
        threads.push(handled[0].1);
    }
    assert_ne!(threads[0], threads[1], "both keys went to the same core");
}
