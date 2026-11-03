---
sidebar_position: 7
title: Slice codecs
description: "PostcardCodec and JsonCoreCodec encode into a slice and decode from one, so a build without alloc has a codec; each is on with its own feature."
---

# Slice codecs

A `SliceCodec` encodes into a slice the runtime lends and decodes from a slice, so it needs no allocator.
It is the codec of the embedded runtime built without `alloc`, in place of `Codec`, which hands out a growing buffer.

`PostcardCodec` is on with the `postcard` feature.
It is a compact binary format, `application/octet-stream`, and both ends need the same Rust types.
`JsonCoreCodec` is on with the `json-core` feature.
It writes the JSON that `serde_json` writes, `application/json`, over `serde-json-core`, and its decoder does not unescape strings, so a message type holds numbers, booleans, arrays and fixed-capacity strings.
A value that does not fit the buffer it is encoded into is `CodecFailure::BufferFull`, and the publish fails like any other: the handler's [`.error`](../messages/error.md) hears of it.

The mount names the codec: `b.with_codec(PostcardCodec)` for the handlers of a scope, and `.out_reply(policy).codec(PostcardCodec)` for a reply.
A reply is encoded into a buffer that `.encode_capacity::<N>()` sizes, 256 bytes unless the mount names more.
A message that carries its own bytes needs no codec, with the [byte lanes](./bytes.md).
A codec of your own implements `SliceCodec`.

## Example

The mount of a device service that decodes and replies in postcard:

```rust reference="crates/ruststream/tests/doc_device.rs#mount"
```
