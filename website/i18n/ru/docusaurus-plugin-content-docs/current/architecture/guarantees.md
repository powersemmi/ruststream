---
title: Гарантии
description: "Ошибки, которые не компилируются, и сообщение компилятора для каждой."
---

import {Diagram} from '@site/src/components/Diagrams';

export const words = {
  "title": "Слева ошибка в коде, справа сообщение компилятора о ней",
  "text": "Схема текстом",
  "writes": "Вы пишете",
  "answers": "Компилятор отвечает",
  "reply": ["тип ответа", "без", "#[derive(Outgoing)]"],
  "replyError": ["`Receipt` does not declare", "where it is published"],
  "naming": ["именующее", "преобразование", "на ответе с адресом"],
  "namingError": ["`Receipt` does not let", "a transform name", "the destination"],
  "codec": ["монтирование", "без включённой", "фичи кодека"],
  "codecError": ["no codec is available", "to decode this", "subscriber's input"],
  "capability": ["Out<impl RequestReply>", "у издателя без", "этой совместимости"],
  "capabilityError": ["`MemoryPublisher` does not", "support request / reply", "messaging"],
  "concurrency": ["workers(4) и", "threads(2) на одном", "подписчике"],
  "concurrencyError": ["workers(..) and threads(..)", "fill the same position:", "name one of them"],
  "destination": ["публикация", "без адреса"],
  "destinationError": ["this publish has", "no destination yet"],
  "headers": ["публикация без", "заголовков, которые", "объявило сообщение"],
  "headersError": ["the headers of this publish", "do not match the message's", "contract"]
};

# Гарантии

Ошибки, которые не компилируются, и сообщение компилятора для каждой.

<Diagram name="guarantees" words={words} />

Ошибку, которую видит компилятор, RustStream делает ошибкой компиляции, а не проверкой при старте
или на каждом сообщении. Проверка на каждом сообщении стоила бы каждой доставке, а проверка при
старте находит ошибку только там, где сервис впервые запустили. Каждое сообщение называет тип или
шаг, в котором ошибка.

Строки схемы относятся к правилам [ответов](../components/subscribers/publishing/replies.md),
[преобразований](../components/subscribers/publishing/transforms/naming-transform.md),
[кодеков](../components/subscribers/codecs/index.md),
[совместимости издателя](../components/subscribers/publishing/publishers/index.md),
[диспетчеризации](../components/subscribers/dispatch/index.md),
[публикации](../components/subscribers/publishing/publish-builder.md) и
[типизированных заголовков](../components/subscribers/typed-headers/index.md), по порядку.
Настройку, однажды заданную в цепочке монтирования, нельзя задать второй раз, а совместимость,
которой у брокера нет, оборачивается отсутствующим методом или невыполненным ограничением.

[Жизненный цикл](lifecycle.md) тоже типизирован: у неподключённого брокера нет методов для
подписки и публикации, а закрытый поглощён.
