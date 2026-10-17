---
title: Разделяемое состояние и контекст
description: "Платёжный и почтовый клиенты создаются один раз при запуске и доступны подписчикам через State. Контекст сообщает подписчику, какую доставку он обрабатывает."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Разделяемое состояние и контекст

Платёжный и почтовый клиенты создаются один раз при запуске и доступны подписчикам через `State`. Контекст сообщает подписчику, какую доставку он обрабатывает.

Подписчики сервиса совместно используют клиенты, пулы и кеши.
Приложение создаёт их в [`on_startup`](api:ruststream::runtime::RustStream::on_startup) до первой подписки, а ошибка в этом хуке прерывает запуск.
Значение, которое возвращает хук, - это [состояние](../../components/application/state/any-struct.md) приложения, одно на весь сервис.
Подписчик получает одно поле состояния через параметр [`State<T>`](api:ruststream::runtime::State).
[Контекст](../../components/subscribers/context/delivery-context.md) - часть, своя для каждой доставки: имя подписки, заголовки, поля брокера для этой доставки.
Подписчик получает его через параметр `&mut Context` сразу после сообщения.

## Состояние

`Shop` содержит платёжный клиент и почтовый клиент, каждый за `Arc`, поэтому копирование состояния обходится дёшево.
[`FromRef`](api:ruststream::runtime::FromRef) извлекает из состояния каждое поле: через него разрешается `State<Arc<Gateway>>`.
`open` - это хук, который приложение передаёт в `on_startup` в главе 5.

<Tabs groupId="api">
<TabItem value="macros" label="Макросы">

```rust title="state.rs" reference="examples/guide/state.rs#state"
```

</TabItem>
<TabItem value="manual" label="Вручную">

```rust title="state.rs" reference="examples/manual/guide/state.rs#state"
```

</TabItem>
</Tabs>

Ручная форма содержит две реализации, которые генерирует derive-макрос, по одной на поле.

## Состояние и контекст в одном подписчике

`send` отправляет уведомление по электронной почте через почтовый клиент из состояния.
Несуществующий почтовый ящик не появится при повторной попытке, поэтому отклонённое уведомление отбрасывается, а предупреждение называет подписку из контекста.

<Tabs groupId="api">
<TabItem value="macros" label="Макросы">

```rust title="notifications.rs" reference="examples/guide/notifications.rs#send"
```

</TabItem>
<TabItem value="manual" label="Вручную">

```rust title="notifications.rs" reference="examples/manual/guide/notifications.rs#send"
```

</TabItem>
</Tabs>

В ручной форме тип состояния - последний параметр `Handle`, и монтирование сверяет его с типом состояния приложения.
Тело читает поле через [`from_context`](api:ruststream::runtime::FromContext). Макрос делает то же самое для параметра `State`.
[Собственные экстракторы](../../components/subscribers/context/from-context.md) - это ваши типы, которые читаются тем же способом.
