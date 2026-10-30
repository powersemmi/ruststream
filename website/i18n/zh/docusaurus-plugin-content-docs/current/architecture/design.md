---
title: 设计
description: "核心不认识任何 Broker：Broker crate 实现核心的 trait，并持有该 Broker 的全部设置。"
---

import {Diagram} from '@site/src/components/Diagrams';

export const words = {
  "title": "服务、与 Broker 无关的核心，以及实现核心 trait 的 Broker crate",
  "text": "以文字阅读本图",
  "service": "你的服务",
  "handlers": ["#[subscriber]", "处理器"],
  "app": ["#[ruststream::app]", "构建函数"],
  "core": "ruststream",
  "runtime": ["运行时：路由、分发、", "确认、发布"],
  "codecs": ["编解码器：JSON、CBOR、", "MessagePack 或不用，", "在编译期选定"],
  "modules": ["特性：内存 Broker、", "AsyncAPI、指标、", "OpenTelemetry、测试"],
  "traits": ["Broker, Subscriber,", "Publisher, sources,", "policies"],
  "capabilities": ["能力：批量、事务、", "请求与应答、", "定位读取"],
  "crates": "Broker crate",
  "nats": ["ruststream-nats", "subject、stream、", "策略、设置、", "挂载步骤"],
  "kafka": ["ruststream-rdkafka", "topic、消费组、", "策略、设置、", "挂载步骤"],
  "mounted": "挂载到",
  "implements": "实现",
  "wraps": "封装"
};

# 设计

核心不认识任何 Broker：Broker crate 实现核心的 trait，并持有该 Broker 的全部设置。

<Diagram name="design" words={words} />

核心不依赖任何 Broker 客户端。必需的 trait 只有三个：连接的生命周期、以 `Stream` 形式交付消息的订阅者，以及发布者。批量、事务、请求与应答、定位读取，以及消息本身已携带的值，都是能力 trait；只有客户端支持时，Broker 才实现它们。

Broker 的专有概念都在它自己的 crate 里：订阅形式的描述符、发布者的策略、单条消息的设置，以及读取挂载链设置的挂载步骤。核心里没有任何 Broker 专属的设置，也没有它们的默认值。

编解码器是一个类型：挂载时指定的[编解码器](../components/subscribers/codecs/index.md)，或由启用的特性选出的默认编解码器。没有启用任何编解码器特性时就没有默认值，需要编解码器的挂载无法通过编译。

trait、运行时和每个可选层都是独立的 crate，`ruststream` 在同一套路径下重新导出它们。分层由编译器保证：契约 crate `ruststream-core` 不依赖框架中的任何其他 crate。它以 `#![no_std]` 构建，只需要内存分配器，因此设备也可以依赖它；`ruststream` 会打开它的 `std` 特性。`ruststream` 的 cargo 特性打开一个可选层：[内存 Broker](../components/memory/index.md)、AsyncAPI、指标、日志、OpenTelemetry、[测试](../components/application/testing/index.md)和命令行。特性只会增加功能，不会改变其他特性的行为。
