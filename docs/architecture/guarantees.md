---
title: Guarantees
description: "Mistakes that do not compile, and the error the compiler prints for each."
---

import {Diagram} from '@site/src/components/Diagrams';

export const words = {
  "title": "Mistakes on the left, the error the compiler prints for each on the right",
  "text": "The diagram as text",
  "writes": "You write",
  "answers": "The compiler answers",
  "reply": ["a reply type", "without", "#[derive(Outgoing)]"],
  "replyError": ["`Receipt` does not declare", "where it is published"],
  "naming": ["a naming transform", "on a reply that", "names its destination"],
  "namingError": ["`Receipt` does not let", "a transform name", "the destination"],
  "codec": ["a mounting with", "no codec feature", "enabled"],
  "codecError": ["no codec is available", "to decode this", "subscriber's input"],
  "capability": ["Out<impl RequestReply>", "on a publisher", "without it"],
  "capabilityError": ["`MemoryPublisher` does not", "support request / reply", "messaging"],
  "concurrency": ["workers(4) and", "threads(2) on", "one subscriber"],
  "concurrencyError": ["workers(..) and threads(..)", "fill the same position:", "name one of them"],
  "destination": ["a publish with", "no destination"],
  "destinationError": ["this publish has", "no destination yet"],
  "headers": ["a publish without", "the headers its", "message declares"],
  "headersError": ["the headers of this publish", "do not match the message's", "contract"]
};

# Guarantees

Mistakes that do not compile, and the error the compiler prints for each.

<Diagram name="guarantees" words={words} />

A mistake the compiler can see is a compile error, not a check at startup or on each message. A
check per message would cost every delivery, and a check at startup finds the mistake only where
the service first runs. Each error names the type or the step at fault.

Each row is a [reply](../components/subscribers/publishing/replies.md),
[transform](../components/subscribers/publishing/transforms/naming-transform.md),
[codec](../components/subscribers/codecs/index.md),
[publisher capability](../components/subscribers/publishing/publishers/index.md),
[dispatch](../components/subscribers/dispatch/index.md),
[publish](../components/subscribers/publishing/publish-builder.md) and
[typed header](../components/subscribers/typed-headers/index.md) rule, in that order. A setting
fixed once on a mount chain cannot be fixed again, and a capability the broker lacks is a missing
method or an unmet bound.

The [lifecycle](lifecycle.md) is typed too: a broker that is not connected has no method to
subscribe or publish with, and a closed one has been consumed.
