---
title: 保证
description: "无法通过编译的错误，以及编译器对每一种错误给出的提示。"
---

import {Diagram} from '@site/src/components/Diagrams';

export const words = {
  "title": "左边是代码里的错误，右边是编译器对它的提示",
  "text": "以文字阅读本图",
  "writes": "你写的",
  "answers": "编译器的回答",
  "reply": ["应答类型", "没有", "#[derive(Outgoing)]"],
  "replyError": ["`Receipt` does not declare", "where it is published"],
  "naming": ["在已声明地址的", "应答上使用", "命名变换"],
  "namingError": ["`Receipt` does not let", "a transform name", "the destination"],
  "codec": ["没有启用", "任何编解码器", "特性的挂载"],
  "codecError": ["no codec is available", "to decode this", "subscriber's input"],
  "capability": ["Out<impl RequestReply>", "用在不具备该", "能力的发布者上"],
  "capabilityError": ["`MemoryPublisher` does not", "support request / reply", "messaging"],
  "concurrency": ["同一个订阅者上", "同时写 workers(4)", "和 threads(2)"],
  "concurrencyError": ["workers(..) and threads(..)", "fill the same position:", "name one of them"],
  "destination": ["没有目的地", "的发布"],
  "destinationError": ["this publish has", "no destination yet"],
  "headers": ["缺少消息所声明", "的消息头的发布"],
  "headersError": ["the headers of this publish", "do not match the message's", "contract"]
};

# 保证

无法通过编译的错误，以及编译器对每一种错误给出的提示。

<Diagram name="guarantees" words={words} />

编译器能看出的错误，RustStream 就让它成为编译错误，而不是启动时或每条消息上的检查。每条消息上的检查会让每次投递都付出代价，启动时的检查则只有在服务第一次运行的地方才能发现错误。每条提示都会指出出错的类型或步骤。

图中各行依次对应[应答](../components/subscribers/publishing/replies.md)、[变换](../components/subscribers/publishing/transforms/naming-transform.md)、[编解码器](../components/subscribers/codecs/index.md)、[发布者能力](../components/subscribers/publishing/publishers/index.md)、[分发](../components/subscribers/dispatch/index.md)、[发布](../components/subscribers/publishing/publish-builder.md)和[类型化消息头](../components/subscribers/typed-headers/index.md)的规则。挂载链上已经设定过的设置不能再设定一次；Broker 不具备的能力，表现为缺少方法或约束不满足。

[生命周期](lifecycle.md)同样是类型化的：未连接的 Broker 没有用来订阅或发布的方法，已关闭的 Broker 已经被消耗掉了。
