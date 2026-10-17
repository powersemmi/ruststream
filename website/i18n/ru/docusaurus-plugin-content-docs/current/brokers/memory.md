---
description: "MemoryBroker - полноценный брокер внутри процесса сервиса: ему не нужен сервер, а включает его фича memory крейта ruststream."
---

# Memory

`MemoryBroker` - полноценный брокер, который работает внутри процесса сервиса. Он входит в фичу
`memory` крейта `ruststream` и не требует сервера, поэтому подходит для очереди, которая нужна
одному приложению. С него начинается шаблон `cargo generate` по умолчанию.

```toml
ruststream = { version = "0.7", features = ["macros", "memory", "json"] }
```

Как с ним работать, описано в компонентах [Memory broker](../components/memory/index.md): сам
брокер и его прелюдия, [хранимый журнал](../components/memory/retention.md) для повторного чтения и
[подписки по шаблону](../components/memory/patterns.md).
