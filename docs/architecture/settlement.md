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
  "redeliver": ["Below the cap: the broker", "redelivers, or a copy goes", "to the retry address"],
  "deadLetter": ["At the cap: a copy to the", "dead letter, or rejected"],
  "after": ["After settling:", "and_after and hooks run"],
  "decoded": "decoded",
  "returns": "returns",
  "atCap": "at the cap"
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

A retry goes back to the broker when the broker redelivers by itself and counts the attempts. Else
the runtime publishes a copy to the subscription's
[retry address](../components/subscribers/retry-destination.md) and drops the original. The copy
carries its attempt count in a header. [`retry_after`](../components/subscribers/messages/retry-after.md)
waits on the broker's own delayed redelivery where it has one, and otherwise publishes the copy
after the delay.

The [attempt cap](../components/subscribers/retry-cap.md) is checked before a retry. At the cap, a
delivery with a declared dead letter is copied there, and one without is rejected.

A settlement that the broker refuses is logged, and the loop moves on.
