# Быстрый старт

Самый быстрый путь к работающему сервису - сгенерировать заготовку через `cargo generate`.

## Заготовка проекта

```bash
cargo install cargo-generate
cargo generate --git https://github.com/powersemmi/ruststream templates/memory --name my-service
cd my-service
```

Для генерации нужен только `cargo generate`. `templates/memory` - стартовый шаблон на in-memory
брокере. Крейт брокера, у которого шаблон есть, разворачивается так же: указывают его репозиторий
и путь к шаблону (например,
`--git https://github.com/powersemmi/ruststream-nats templates/nats`); какие шаблоны есть у
брокера, сказано в его собственной документации. `cargo generate` создаёт идиоматичный проект из
нескольких файлов:

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

`cargo run -- run` запускает рантайм tokio. Сервис работает, пока вы не нажмёте ++ctrl+c++.
Внешний брокер для запуска не нужен.

## Генерация AsyncAPI-документа

```bash
cargo run -- asyncapi gen
```

Команда печатает AsyncAPI-документ в формате JSON. Флаги вывода (`-o`, `--yaml`) и сам документ
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

Вы пишете функцию, которая собирает сервис, а `#[ruststream::app]` превращает её в `main`.

## Что дальше

- Разобраться в каждой части по [учебнику](tutorial.md).
- Изучить формы обработчиков в разделе [Подписчики](../guides/subscribers.md).
- Управлять сервисом из [CLI](../guides/cli.md).
