# CLI

Командная утилита `ruststream` вызывает `cargo` с подкомандами фреймворка. Заготовку нового проекта
создаёт не она, а `cargo generate` (см. [Заготовки проектов](#scaffolding) ниже).

```bash
cargo install ruststream --features cli
```

Сервис на RustStream - обычный бинарник на Rust, у которого `main` сгенерирован макросом
`#[ruststream::app]`. Команды `run` и `asyncapi gen` запускают `cargo run` для нужного крейта.

## Команды

```bash
ruststream run                         # cargo run -- run, против ./Cargo.toml
ruststream run -p ./my-service         # против другого крейта
ruststream run --release               # сборка в release
ruststream asyncapi gen                # напечатать AsyncAPI-документ
ruststream asyncapi gen -o spec.json   # записать его в файл
ruststream asyncapi gen --yaml         # YAML вместо JSON
```

`run` и `asyncapi gen` принимают `-p/--manifest-path` - путь к крейту сервиса. По умолчанию это
текущий каталог.

## Сгенерированная точка входа

`#[ruststream::app]` превращает функцию-билдер в `main`, который понимает `run` и `asyncapi gen`:

=== "Макросы"

    ```rust
    use ruststream::memory::MemoryBroker;
    use ruststream::runtime::{AppInfo, RustStream};

    --8<-- "examples/quickstart.rs:app"
    ```

=== "Вручную"

    ```rust
    use ruststream::memory::MemoryBroker;
    use ruststream::prelude::*;

    --8<-- "examples/manual/quickstart.rs:app"
    ```

`ruststream run` и обычный `cargo run -- run` запускают сервис одинаково.

## Заготовки проектов {#scaffolding}

Новый проект создаёт [`cargo generate`](https://github.com/cargo-generate/cargo-generate) по
шаблону. Команду и то, какой проект получается, описывает
[быстрый старт](../getting-started/quickstart.md).

Шаблон принадлежит крейту брокера, для которого он написан. Стартовый шаблон на in-memory брокере
лежит в этом репозитории. Свои шаблоны поставляет каждый репозиторий брокера, обычно по одному
на транспорт или топологию, например `nats` и `nats-js`. Как написать шаблон для нового брокера,
описывает [контракт шаблонов](../broker-authors/templates.md).
