//! A message set's handler with an `Out` slot: one delivery at a time with a reply, and as a
//! batch with and without one.

use super::*;

/// A batch of a set mounts with an `Out` slot and no reply, like a batch of one model.
mod batch_slots {
    use super::*;

    #[derive(InMessages)]
    enum OrderEvent {
        Created(OrderCreated),
        Cancelled(OrderCancelled),
    }

    /// Each element of the batch leaves through the slot as what it was.
    #[subscriber("bslot.orders")]
    async fn on_batch(events: &[OrderEvent], Out(out): Out<impl Publisher>) -> HandlerOutcome {
        for event in events {
            let seen = match event {
                OrderEvent::Created(created) => Seen::of("created", created.id),
                OrderEvent::Cancelled(cancelled) => Seen::of("cancelled", cancelled.id),
            };
            if out.message(&seen).to("bslot.seen").publish().await.is_err() {
                return HandlerOutcome::retry();
            }
        }
        HandlerOutcome::ack()
    }

    #[tokio::test]
    async fn a_batch_of_a_set_publishes_through_its_slot() -> Result<(), Box<dyn Error>> {
        let broker = MemoryBroker::retaining(Retention::Messages(nonzero!(8)));
        let publisher = broker.publisher();
        publisher
            .message(&Wire(serde_json::to_vec(&OrderCreated { id: 15 })?))
            .to("bslot.orders")
            .publish()
            .await?;
        publisher
            .message(&cancelled_bytes(16))
            .to("bslot.orders")
            .publish()
            .await?;
        let app = RustStream::new(AppInfo::new("bslot", "0.1.0")).with_broker(broker, |b| {
            b.include(
                on_batch
                    .batch(nonzero!(2))
                    .start_at(MemoryPosition::start()),
            )
            .out(DefaultSlot, Publish)
            .build();
        });
        let tb = TestApp::start(app).await?;
        tb.settle().await?;

        let broker = tb.broker::<MemoryBroker<Retaining>>();
        broker.subscriber("bslot.orders").assert_batch_sizes(&[2]);
        assert_eq!(
            broker.published::<Seen>("bslot.seen").decoded(),
            vec![Seen::of("created", 15), Seen::of("cancelled", 16)]
        );
        Ok(())
    }
}

/// A set handler that replies and publishes through a slot, one delivery at a time and as a
/// batch: the reply and the slot each see what the set decoded.
mod reply_and_slot {
    use super::*;

    #[derive(InMessages)]
    enum OrderEvent {
        Created(OrderCreated),
        Cancelled(OrderCancelled),
    }

    fn seen(event: &OrderEvent) -> Seen {
        match event {
            OrderEvent::Created(created) => Seen::of("created", created.id),
            OrderEvent::Cancelled(cancelled) => Seen::of("cancelled", cancelled.id),
        }
    }

    /// Replies with what it saw and leaves an audit copy through its slot.
    #[subscriber("rslot.orders", reply("rslot.seen"))]
    async fn on_event(
        event: &OrderEvent,
        Out(audit): Out<impl Publisher>,
    ) -> Result<Seen, HandlerOutcome> {
        let seen = seen(event);
        audit
            .message(&seen)
            .to("rslot.audit")
            .publish()
            .await
            .map_err(|_| HandlerOutcome::retry())?;
        Ok(seen)
    }

    /// The same for a whole batch: one reply and one audit copy per element.
    #[subscriber("rslot.batch", reply("rslot.batch.seen"))]
    async fn on_batch(
        events: &[OrderEvent],
        Out(audit): Out<impl Publisher>,
    ) -> Result<Vec<Seen>, HandlerOutcome> {
        let mut replies = Vec::with_capacity(events.len());
        for event in events {
            let seen = seen(event);
            audit
                .message(&seen)
                .to("rslot.batch.audit")
                .publish()
                .await
                .map_err(|_| HandlerOutcome::retry())?;
            replies.push(seen);
        }
        Ok(replies)
    }

    #[tokio::test]
    async fn a_set_handler_replies_and_publishes_through_its_slot() -> Result<(), Box<dyn Error>> {
        let broker = MemoryBroker::retaining(Retention::Messages(nonzero!(8)));
        let publisher = broker.publisher();
        publisher
            .message(&Wire(serde_json::to_vec(&OrderCreated { id: 21 })?))
            .to("rslot.batch")
            .publish()
            .await?;
        publisher
            .message(&cancelled_bytes(22))
            .to("rslot.batch")
            .publish()
            .await?;
        let app = RustStream::new(AppInfo::new("rslot", "0.1.0")).with_broker(broker, |b| {
            b.include(on_event)
                .out_reply(Publish)
                .out(DefaultSlot, Publish)
                .build();
            b.include(
                on_batch
                    .batch(nonzero!(2))
                    .start_at(MemoryPosition::start()),
            )
            .out_reply(Publish)
            .out(DefaultSlot, Publish)
            .build();
        });
        let tb = TestApp::start(app).await?;
        let broker = tb.broker::<MemoryBroker<Retaining>>();
        broker
            .publish("rslot.orders", &OrderCreated { id: 20 })
            .await?;
        tb.settle().await?;

        assert_eq!(
            broker.published::<Seen>("rslot.seen").decoded(),
            vec![Seen::of("created", 20)]
        );
        assert_eq!(
            broker.published::<Seen>("rslot.audit").decoded(),
            vec![Seen::of("created", 20)]
        );
        let batch = vec![Seen::of("created", 21), Seen::of("cancelled", 22)];
        assert_eq!(
            broker.published::<Seen>("rslot.batch.seen").decoded(),
            batch
        );
        assert_eq!(
            broker.published::<Seen>("rslot.batch.audit").decoded(),
            batch
        );
        Ok(())
    }
}
