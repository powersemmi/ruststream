---
title: Settlement
description: "How a delivery is settled: what the handler returns, what a failure policy decides, and where a retry ends."
---

import {Diagram} from '@site/src/components/Diagrams';

export const words = {
  "title": "From a delivery to its settlement: decode, handler, outcome, retry and dead letter",
  "text": "The diagram as text",
  "delivery": "A delivery",
  "decode": ["Decode", "the payload"],
  "handler": ["Handler", "runs"],
  "outcome": ["Outcome", "what the body returned"],
  "decodePolicy": ["Does not decode:", "drop by default"],
  "panicPolicy": ["Panics:", "fail_fast by default,", "the service stops"],
  "ack": ["ack()", "acknowledged"],
  "drop": ["drop()", "rejected"],
  "retry": ["retry()", "now"],
  "retryAfter": ["retry_after", "later"],
  "capCheck": "The attempt cap is checked first",
  "belowCap": ["Below the cap:", "requeued, or a", "copy to the", "retry address"],
  "deadLetter": ["At the cap:", "a copy to the", "dead letter"],
  "reject": ["At the cap,", "no dead letter:", "rejected"],
  "after": ["After settling:", "and_after and hooks run"],
  "decoded": "decoded",
  "returns": "returns"
};

# Settlement

How a delivery is settled: what the handler returns, what a failure policy decides, and where a
retry ends.

<Diagram name="settlement" words={words} />

A handler's [outcome](../components/subscribers/messages/handler-outcome.md) is a value. `Ok`
acknowledges, and an error outcome says how to settle. A plain `Result<(), E>` that fails is
dropped, not retried.

A payload that does not decode and a handler that panics go through the
[failure policy](../components/subscribers/failure-policy.md). A policy other than `fail_fast`
settles the delivery like the outcome of the same name, and `skip` acknowledges it. `fail_fast`
stops the service and leaves the delivery unsettled, so a broker that redelivers hands it back after
the restart.

An immediate [`retry()`](../components/subscribers/messages/handler-outcome.md) with no [attempt
cap](../components/subscribers/retry-cap.md) and no dead letter declared is the broker's own
requeue. Under a declaration, a broker that counts its redeliveries still requeues, and the count is
the broker's. On a broker that counts nothing, the runtime publishes a copy to the subscription's
[retry address](../components/subscribers/retry-destination.md) instead and drops the original. The
copy carries its attempt count in a header.

A delayed [`retry_after`](../components/subscribers/messages/retry-after.md) waits on the broker's
own delayed redelivery where it has one. Elsewhere the runtime publishes the copy after the delay,
with or without a declaration.

A declared cap is checked before either retry. At the cap, a delivery with a declared dead letter is
copied there, and one without is rejected. A broker that moves a spent delivery itself receives the
declaration at startup and applies it on its own.

A settlement that the broker refuses is logged, and the loop moves on.
