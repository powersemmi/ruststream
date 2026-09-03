# Быстрый старт

Самый быстрый путь к работающему сервису - сгенерировать заготовку через `cargo generate`.

## Заготовка проекта

```bash
cargo install cargo-generate
cargo generate --git https://github.com/powersemmi/ruststream templates/memory --name my-service
cd my-service
```

Для генерации нужен только `cargo generate`, CLI `ruststream` не требуется. `templates/memory` - это
стартовый шаблон на in-memory брокере (внешний брокер не нужен); свой шаблон поставляет каждый крейт
брокера (например, `--git https://github.com/powersemmi/ruststream-nats templates/nats`). Команда
создаёт идиоматичный проект из нескольких файлов:

```
my-service/
├── Cargo.toml
└── src/
    ├── main.rs      # #[ruststream::app] строит сервис и монтирует роутер
    ├── orders.rs    # обработчики как функции #[subscriber] (один публикует ответ)
    └── routes.rs    # собирает обработчики в Router
```

## Запуск

`#[ruststream::app]` генерирует `main`, поэтому бинарник уже понимает команды фреймворка:

```bash
cargo run -- run                # или: ruststream run, если установлен CLI
```

`cargo run -- run` поднимает рантайм tokio и держит сервис запущенным, пока вы не нажмёте
++ctrl+c++ (команда CLI `ruststream run` - удобная обёртка над ней).
Заготовка работает на in-memory брокере, так что внешние зависимости ей не нужны.

## Генерация AsyncAPI-документа

```bash
cargo run -- asyncapi gen
```

Команда печатает AsyncAPI-документ в формате JSON; флаги вывода (`-o`, `--yaml`) и сам документ
разобраны в [руководстве по AsyncAPI](../guides/asyncapi.md).

## Как выглядит точка входа

=== "Макросы"

    ```rust title="src/main.rs"
    --8<-- "examples/tutorial/main.rs:main"
    ```

=== "Вручную"

    ```rust title="src/main.rs"
    --8<-- "examples/manual/tutorial/main.rs:main"
    ```

Вы пишете функцию, которая собирает сервис, а макрос превращает её в `main`, разбирающий команды
`run` и `asyncapi gen`.

## Что дальше

- Разобраться в каждой части по [учебнику](tutorial.md).
- Изучить формы обработчиков в разделе [Подписчики](../guides/subscribers.md).
- Управлять всем из [CLI](../guides/cli.md).
