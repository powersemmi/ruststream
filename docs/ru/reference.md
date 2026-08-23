# Справочник API

Полный справочник по Rust API генерирует rustdoc, а публикуется он на docs.rs. Этот сайт объясняет
концепции и даёт руководства; источник истины по каждому типу, трейту и сигнатуре функции - docs.rs.

- **[ruststream на docs.rs](https://docs.rs/ruststream)** - сам крейт. Соберите его со всеми фичами,
  чтобы увидеть модули рантайма, кодеков, AsyncAPI, метрик и conformance:
  [docs.rs/ruststream (все фичи)](https://docs.rs/crate/ruststream/latest/features).

Командная утилита `ruststream` - это фича `cli` того же крейта, а не отдельный крейт; см.
[руководство по CLI](guides/cli.md).

## Локальная сборка справочника

```bash
cargo doc --all-features --open
```

## Ключевые точки входа

| Элемент | Модуль | Назначение |
|---|---|---|
| `RustStream` | `ruststream::runtime` | объект приложения |
| `RunningApp` | `ruststream::runtime` | запущенный сервис: готовность, сигнал fail-fast, мягкая остановка |
| `Router` | `ruststream::runtime` | группа обработчиков с отложенным связыванием |
| `FromContext`, `State`, `FromRef` | `ruststream::runtime` / `ruststream` | параметры-экстракторы обработчика и derive для внедрения состояния |
| `Broker`, `Subscribe`, `Subscriber`, `Publisher`, `IncomingMessage` | `ruststream` | контракт брокера |
| `SubscriptionSource`, `Name` | `ruststream` | дескрипторы подписки |
| `JsonCodec`, `MsgpackCodec`, `CborCodec` | `ruststream::codec` | кодеки формата передачи |
| `build_spec` | `ruststream::asyncapi` | генерация AsyncAPI |
| `Metrics` | `ruststream::metrics` | метрики Prometheus |
| `TestApp` | `ruststream::testing` | обвязка для юнит-тестов приложения прямо в процессе |
| `TestableBroker` | `ruststream::testing` | контракт тестового транспорта брокера |
| `harness::run_suite` | `ruststream::conformance` | набор проверок conformance |
