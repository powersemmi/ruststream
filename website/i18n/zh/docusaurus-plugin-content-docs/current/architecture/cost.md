---
title: 开销
description: "框架自身代码在每条消息上的开销，以指令数和内存分配次数计，以及这些开销花在哪里。"
---

import {CoreCost} from '@site/src/components/Benchmarks';

export const labels = {"code": "每条消息的指令数与内存分配", "table": "以表格显示数字", "scenario": "场景", "instructions": "指令数", "allocations": "内存分配", "cold": "冷启动", "measured": "测量于"};

# 开销

框架自身代码在每条消息上的开销，以指令数和内存分配次数计，以及这些开销花在哪里。

<CoreCost labels={labels} />

这些数字是进程内 Broker 上每条消息的稳态开销，不包含任何传输。鼠标悬停或聚焦时，该行会显示冷启动：启动服务并处理第一条消息一共花掉的开销，只付一次。

接收一条消息不做任何内存分配。循环为消息保留一个槽位和一个编码缓冲区，每个阶段都是在编译期确定的泛型类型，途中没有动态调用。在运行时组装的 [`DynStack`](../components/subscribers/middleware/dyn-stack.md) 每层每条消息花费一个装箱的 future。[`and_after`](../components/subscribers/messages/and-after.md) 后续任务是一个装箱的 future，只有设置了它的消息才需要付出。

如果 Broker 客户端只读取字节，应答不做任何内存分配：它借用循环的缓冲区。进程内 Broker 会保留交给它的内容，所以它的应答要付出编解码器写入的那块缓冲区。其余发布场景分配的，是进程内 Broker 为每条消息保留的内容，以及场景自己构建的东西：一个消息头的值，或一个请求的一次性收件箱。

解码中最贵的是消息内容的大小：对比 1 KB 的消息体和小结构体。[基准测试](benchmarks/index.md)公布了每个场景及其方法，也包括与各 Broker 裸客户端的对照。
