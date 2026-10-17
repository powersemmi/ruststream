---
title: Тестирование сервиса с TestApp
description: "TestApp запускает приложение магазина в процессе: тест публикует заказ и проверяет, что сделал каждый подписчик и что получила каждая тема."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Тестирование сервиса с TestApp

[`TestApp`](api:ruststream::testing::TestApp) запускает приложение магазина в процессе: тест публикует заказ и проверяет, что сделал каждый подписчик и что получила каждая тема.

Тест запускает то же приложение, что и `main`, без изменений, через [`TestApp::start`](../../components/application/testing/starting-and-stopping/start.md).
Брокер подключается внутри процесса, и каждая подписка работает на рантайме теста.
Публикация из теста возвращает управление, когда все запущенные ею доставки завершены, поэтому следующая строка уже может проверять результат.
Проверки читают две записи: что получил каждый [подписчик](../../components/application/testing/subscriber-assertions/index.md) и каким исходом завершил доставку, и что было [опубликовано](../../components/application/testing/published-assertions/index.md) в каждую тему.
Когда время в тесте приостановлено, [`advance`](../../components/application/testing/driving/advance.md) сдвигает его вперёд, поэтому задержка в секунду или в час не занимает времени.
Для тестирования нужна фича `testing` крейта, обычно в dev-зависимостях.

## Заказ от начала до конца

Тест оформляет заказ с заголовками запроса и прослеживает его до покупателя.
Запрос оплаты передаётся в CBOR, поэтому проверка декодирует его через [`with_codec`](api:ruststream::testing::PublishedAssertions::with_codec).
`accept` и `dispatch` отправляют по уведомлению, и `send` подтверждает оба.

<Tabs groupId="api">
<TabItem value="macros" label="Макросы">

```rust title="main.rs" reference="examples/guide/main.rs#journey"
```

</TabItem>
<TabItem value="manual" label="Вручную">

```rust title="main.rs" reference="examples/manual/guide/main.rs#journey"
```

</TabItem>
</Tabs>

## Заказ, который дальше не идёт

Заказ без строк останавливается на `accept`: оплата не списывается, и покупатель не получает уведомлений.

<Tabs groupId="api">
<TabItem value="macros" label="Макросы">

```rust title="main.rs" reference="examples/guide/main.rs#empty"
```

</TabItem>
<TabItem value="manual" label="Вручную">

```rust title="main.rs" reference="examples/manual/guide/main.rs#empty"
```

</TabItem>
</Tabs>

## Задержка без ожидания

Провайдер, который принимает одно списание в секунду, отказывает второму запросу с `Busy`.
[`with_state`](api:ruststream::testing::TestApp::with_state) запускает приложение с этим провайдером вместо того, который создаёт `on_startup`.
Функция `test_broker` из `service.rs` возвращает брокер, на котором тесты собирают магазин, поэтому брокер в тестах назван в одном месте.
Время в тесте с самого начала приостановлено, и `advance(BACKOFF)` возвращает отклонённый запрос.

<Tabs groupId="api">
<TabItem value="macros" label="Макросы">

```rust title="payments.rs" reference="examples/guide/payments.rs#busy"
```

</TabItem>
<TabItem value="manual" label="Вручную">

```rust title="payments.rs" reference="examples/manual/guide/payments.rs#busy"
```

</TabItem>
</Tabs>

## Тема недоставленных сообщений

Провайдер, который никогда не отвечает, делает каждую доставку неудачной.
Пятая доставка уходит в `payments.dead` в том виде, в каком пришла.

<Tabs groupId="api">
<TabItem value="macros" label="Макросы">

```rust title="payments.rs" reference="examples/guide/payments.rs#dead_letter"
```

</TabItem>
<TabItem value="manual" label="Вручную">

```rust title="payments.rs" reference="examples/manual/guide/payments.rs#dead_letter"
```

</TabItem>
</Tabs>

Запустите тесты обеих форм:

```bash
cargo test --example guide --features testing,macros,memory,json,cbor,asyncapi
cargo test --example manual_guide --no-default-features --features testing,memory,json,cbor,asyncapi
```

Тесты в обоих каталогах одинаковые, и они проходят на обеих формах магазина.
