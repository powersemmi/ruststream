# Redis

The Redis broker lives in its own crate and repository,
[`ruststream-fred`](https://github.com/powersemmi/ruststream-fred) (named after its client,
[`fred`](https://crates.io/crates/fred)). It covers Redis Streams with consumer groups across
standalone, cluster, and sentinel topologies, authentication and TLS on every topology, and an
in-process test broker under its `testing` feature.

Its full documentation, including installation, subscription options, the acknowledgement model, and
testing, is published at
[powersemmi.github.io/ruststream-fred](https://powersemmi.github.io/ruststream-fred/).
