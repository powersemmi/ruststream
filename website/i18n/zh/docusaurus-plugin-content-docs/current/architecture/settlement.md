---
title: 确认
description: "一条消息如何确认：处理器返回什么，失败策略如何决定，重试在哪里结束。"
---

import {Diagram} from '@site/src/components/Diagrams';

export const words = {
  "title": "从消息到确认：解码、处理器、结果、重试和死信",
  "text": "以文字阅读本图",
  "delivery": "一条消息",
  "decode": ["解码", "消息内容"],
  "handler": ["处理器", "运行"],
  "outcome": ["结果", "函数体的返回值"],
  "decodePolicy": ["解码失败：", "默认 drop"],
  "panicPolicy": ["发生 panic：", "默认 fail_fast，", "服务停止"],
  "ack": ["ack()", "已确认"],
  "drop": ["drop()", "已拒绝"],
  "retry": ["retry()", "立即"],
  "retryAfter": ["retry_after", "稍后"],
  "redeliver": ["未到上限：由 Broker", "重新投递，或把副本", "发往重试地址"],
  "deadLetter": ["到达上限：副本发往", "死信，或直接拒绝"],
  "after": ["确认之后：", "运行 and_after 和钩子"],
  "decoded": "已解码",
  "returns": "返回",
  "atCap": "到达上限"
};

# 确认

一条消息如何确认：处理器返回什么，失败策略如何决定，重试在哪里结束。

<Diagram name="settlement" words={words} />

处理器的[结果](../components/subscribers/messages/handler-outcome.md)是一个值。`Ok` 表示确认，错误结果说明如何处置这条消息。普通的 `Result<(), E>` 出错时，消息会被丢弃，而不是重试。

无法解码的内容和发生 panic 的处理器都交给[失败策略](../components/subscribers/failure-policy.md)。除 `fail_fast` 之外的策略，处置方式与同名的结果相同；`skip` 则确认这条消息。`fail_fast` 停止服务，不确认这条消息，能重新投递的 Broker 会在重启之后把它交回来。

如果 Broker 自己会重新投递并统计次数，重试就交还给 Broker。否则运行时把一份副本发布到订阅的[重试地址](../components/subscribers/retry-destination.md)，并拒绝原消息。副本在消息头里带着尝试次数。[`retry_after`](../components/subscribers/messages/retry-after.md) 在 Broker 支持延迟重投时使用它，否则在延迟之后发布副本。

重试之前会检查[尝试上限](../components/subscribers/retry-cap.md)。到达上限时，声明了死信地址的消息会复制到那里，没有声明的则被拒绝。

Broker 拒绝确认时，运行时会记录日志，然后继续处理下一条。
