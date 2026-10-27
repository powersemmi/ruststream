---
title: Message sets in AsyncAPI
sidebar_label: Message sets
sidebar_position: 6
description: "A subscription over an InMessages set lists one message per variant, each with its own name, description, headers and content type."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Message sets

A subscription over an [`InMessages`](../../subscribers/message-sets.md) set lists one message per variant, each with its own name, description, headers and content type.

The `receive` operation names every model of the set, and the channel carries one message component per model.
A `serde` model reports the content type of the codec of the mount and its payload schema; a model that reads its own bytes reports neither, by design, and is not counted by [`messages_without_schema`](api:ruststream::asyncapi::Spec::messages_without_schema).
Under the header rule each message's headers schema carries the header with its `const` value, the counterpart of an OpenAPI discriminator mapping, so a reader of the document knows which header picks which message.
A model with a header contract of its own keeps it, and the rule's header joins its properties; a `Headers<T>` parameter of the handler joins every model's schema the same way.
A model that two sets name with different header values keeps the first value in the document, and generation logs a warning, as for two header contracts on one type.
A dead-letter or retry destination of such a subscription lists the same models.
The example runs with `cargo test --example in_messages --features testing,macros,memory,asyncapi`.

## Example

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/in_messages.rs#set"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/in_messages.rs#set"
```

</TabItem>
</Tabs>
