# CLI

Командная утилита `ruststream` вызывает `cargo` с подкомандами фреймворка. Заготовка нового проекта
создаётся через `cargo generate` (см. [Заготовки проектов](#scaffolding) ниже), поэтому
собственной команды `new` у утилиты нет.

```bash
cargo install ruststream --features cli
```

Сервис на RustStream - это обычный бинарник на Rust, у которого `main` сгенерирован макросом
`#[ruststream::app]`. CLI не заглядывает внутрь него: `run` и `asyncapi gen` просто запускают
`cargo run` для нужного крейта.

## Команды

```bash
ruststream run                         # cargo run -- run, против ./Cargo.toml
ruststream run -p ./my-service         # против другого крейта
ruststream run --release               # сборка в release
ruststream asyncapi gen                # напечатать AsyncAPI-документ
ruststream asyncapi gen -o spec.json   # записать его в файл
ruststream asyncapi gen --yaml         # YAML вместо JSON
```

`run` и `asyncapi gen` принимают `-p/--manifest-path` (по умолчанию - текущий каталог), чтобы
указать на крейт вне рабочего каталога.

## Сгенерированная точка входа

`#[ruststream::app]` превращает функцию-билдер в `main`, понимающий `run` и `asyncapi gen`, так что
шаблонного кода рантайма писать не приходится:

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

Раз диспетчеризация находится в сгенерированном бинарнике, `ruststream run` и обычный
`cargo run -- run` запускают сервис одинаково. `ruststream run` - это удобная обёртка: она находит
крейт и передаёт команду в `cargo`.

## Заготовки проектов {#scaffolding}

Новые проекты создаёт не эта утилита, а [`cargo generate`](https://github.com/cargo-generate/cargo-generate)
по шаблону; сама команда и то, какой проект получается, описаны в
[быстром старте](../getting-started/quickstart.md). Шаблон принадлежит крейту брокера, который он
связывает: стартовый шаблон на in-memory брокере лежит в этом репозитории, а каждый репозиторий
брокера поставляет свои, обычно по одному на транспорт или топологию (например, `nats` против
`nats-js`). Как написать шаблон для нового брокера, описывает
[контракт шаблонов](../broker-authors/templates.md).
