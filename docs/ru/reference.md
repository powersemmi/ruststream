# Справочник API

Полный справочник по Rust API генерирует rustdoc, а публикует его docs.rs. Этот сайт объясняет
концепции и даёт руководства.

- **[ruststream на docs.rs](https://docs.rs/ruststream)** - сам крейт. Модули рантайма, кодеков,
  AsyncAPI, метрик и conformance видны в сборке со всеми фичами:
  [docs.rs/ruststream (все фичи)](https://docs.rs/crate/ruststream/latest/features).

Командная утилита `ruststream` поставляется в том же крейте за фичей `cli`. См.
[руководство по CLI](guides/cli.md).

## Локальная сборка справочника

```bash
cargo doc --all-features --open
```

## Ключевые точки входа

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
| `TestApp` | `ruststream::testing` | внутрипроцессная обвязка для юнит-тестов приложения |
| `TestableBroker` | `ruststream::testing` | контракт тестового транспорта брокера |
| `harness::run_suite` | `ruststream::conformance` | набор проверок для авторов брокеров |
