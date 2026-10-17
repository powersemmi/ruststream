---
description: "The publisher capabilities a broker offers, one trait each, starting with Publisher."
---

# Publishers

The publisher capabilities a broker offers, one trait each, starting with [`Publisher`](api:ruststream::Publisher).

A handler names the capability its slot needs, never the broker's publisher type.
A slot bound to a policy whose live publisher lacks that capability does not compile.
A broker offers only the capabilities its transport has: Kafka-like brokers carry one transaction per producer, and request and reply need a broker that correlates answers.
