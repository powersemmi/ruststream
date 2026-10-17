---
description: "Shared state is one value built at startup that every handler reads, as a State<T> parameter or through a dependency-injection container."
---

# State

Shared state is one value built at startup that every handler reads, as a [`State<T>`](api:ruststream::runtime::State) parameter or through a dependency-injection container.

- [Struct](./any-struct.md): any struct as state, read as `State<T>`.
- [froodi](./froodi.md): a container with RustStream support.
- [shaku](./shaku.md): a container wired by hand.
