---
title: 路径
description: "一条消息从 Broker 的流到确认所经过的各个阶段，按运行时执行它们的顺序排列。"
---

import {Diagram} from '@site/src/components/Diagrams';

export const words = {
  "title": "一条需要应答的消息的路径：从 Broker 的流到确认",
  "text": "以文字阅读本图",
  "stream": ["订阅的流", "启动时打开一次"],
  "loop": ["分发循环", "消息放进复用的槽位"],
  "layers": ["消费层", "最先添加的在最外层"],
  "decode": ["解码", "挂载的编解码器，或原始字节"],
  "body": ["订阅者的处理器", "#[subscriber] 下的 async fn"],
  "encode": ["应答", "编码进循环自己的缓冲区"],
  "transforms": ["发布变换", "按挂载链写下的顺序"],
  "publishLayers": ["发布层", "包裹每一次发布"],
  "publisher": ["发布者", "把应答发给 Broker"],
  "settle": ["确认", "ack；应答失败则重试"]
};

# 路径

一条消息从 Broker 的流到确认所经过的各个阶段，按运行时执行它们的顺序排列。

<Diagram name="path" words={words} />

[订阅](../components/subscribers/subscription-source.md)和应答的发布者在应用启动时、针对已连接的 Broker 解析一次。这条路径上没有任何按消息查找订阅或发布者的步骤。

[消费层](../components/subscribers/middleware/layer.md)包在解码之外，所以即使消息内容无法解码，层也能看到这条消息。无法解码的内容到不了处理器，由[失败策略](../components/subscribers/failure-policy.md)决定如何处置。

不带应答的处理器在函数体返回后立即确认。返回错误结果的处理器不发布应答。应答发布失败时，这条消息以重试的方式确认，请求不会丢失。

[应答](../components/subscribers/publishing/replies.md)发往其类型声明的地址。只读取字节的发布者会借用分发循环自己的缓冲区，应答不做任何内存分配。需要保留字节的发布者会得到一个属于自己的缓冲区。
