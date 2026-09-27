# Справочник API

Справочник по Rust API находится на [docs.rs](https://docs.rs/ruststream). Его генерирует rustdoc.
Каждый модуль с фичей описывает в нём себя сам.

Модуль с фичей виден только в сборке с этой фичей. docs.rs собирает крейт со всеми фичами, их
список - на странице [docs.rs/ruststream (все фичи)](https://docs.rs/crate/ruststream/latest/features).

Командная утилита `ruststream` входит в тот же крейт, за фичей `cli`. Её описывает модуль
[`runtime::cli`](https://docs.rs/ruststream/latest/ruststream/runtime/cli/index.html).

Этот сайт охватывает остальное: установку, учебник, брокеры и контракт для авторов адаптаций
библиотек брокеров.

## Локальная сборка справочника

Справочник можно собрать и открыть локально, со всеми фичами:

```bash
cargo doc --all-features --open
```

## Ключевые точки входа

С этих типов удобно начинать чтение справочника:

| Элемент | Модуль | Назначение |
|---|---|---|
| `RustStream` | `ruststream::runtime` | объект приложения |
| `RunningApp` | `ruststream::runtime` | дескриптор запущенного сервиса: готовность, сигнал отказа в режиме fail-fast, штатная остановка |
| `Router` | `ruststream::runtime` | группа обработчиков, которая получает брокер при монтировании |
| `Handle`, `subscriber` | `ruststream::runtime` | ручная регистрация: трейт тела обработчика и его привязка к источнику подписки |
| `FromContext`, `State`, `FromRef` | `ruststream::runtime` / `ruststream` | параметры-экстракторы обработчика и derive для внедрения состояния |
| `Broker`, `Subscribe`, `Subscriber`, `Publisher`, `IncomingMessage` | `ruststream` | контракт брокера |
| `SubscriptionSource`, `Name` | `ruststream` | дескрипторы подписки |
| `JsonCodec`, `MsgpackCodec`, `CborCodec` | `ruststream::codec` | кодеки формата передачи |
| `build_spec` | `ruststream::asyncapi` | генерация документа AsyncAPI |
| `Metrics` | `ruststream::metrics` | метрики Prometheus |
| `TestApp` | `ruststream::testing` | обвязка для тестов приложения внутри процесса |
| `TestableBroker` | `ruststream::testing` | контракт, который реализует внутрипроцессный транспорт брокера |
| `harness::run_suite` | `ruststream::conformance` | набор проверок для авторов адаптаций библиотек брокеров |
