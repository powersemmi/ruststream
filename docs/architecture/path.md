---
title: Path
description: "The stages one delivery passes, from the broker's stream to its settlement, in the order the runtime runs them."
---

import {Diagram} from '@site/src/components/Diagrams';

export const words = {
  "title": "The path of a delivery that is answered, from the broker's stream to its settlement",
  "text": "The diagram as text",
  "stream": ["Subscription stream", "opened once, at startup"],
  "loop": ["Dispatch loop", "the delivery in a reused slot"],
  "layers": ["Consume layers", "the first one added is outermost"],
  "decode": ["Decode", "the mounting's codec, or the bytes"],
  "body": ["Handler", "async fn under #[subscriber]"],
  "encode": ["Reply", "encoded into the loop's buffer"],
  "transforms": ["Publish transforms", "in the order the chain names them"],
  "publishLayers": ["Publish layers", "around every publish"],
  "publisher": ["Publisher", "sends the reply to the broker"],
  "settle": ["Settlement", "ack; a failed reply goes to .error"]
};

# Path

The stages one delivery passes, from the broker's stream to its settlement, in the order the
runtime runs them.

<Diagram name="path" words={words} />

The [subscription](../components/subscribers/subscription-source.md) and the reply's publisher are
resolved once, when the app starts, against the connected broker. Nothing on this path looks a
subscription or a publisher up per message.

[Consume layers](../components/subscribers/middleware/layer.md) wrap the decode, so a layer sees a
delivery even when its payload does not decode. A payload that does not decode never reaches the
handler: the [failure policy](../components/subscribers/failure-policy.md) settles it.

A handler with no reply settles right after its body. A handler that returns an error outcome
skips the reply. When the reply fails to publish, the runtime still acknowledges the delivery,
and the handler does not run again. The failure and the unsent reply reach the handler's
[`.error` continuation](../components/subscribers/messages/error.md), and are logged when it has
none.

The [reply](../components/subscribers/publishing/replies.md) goes where its type declares. A
publisher that only reads the bytes is lent the dispatch loop's own buffer, so a reply allocates
nothing. A publisher that keeps the bytes gets a buffer of its own.
