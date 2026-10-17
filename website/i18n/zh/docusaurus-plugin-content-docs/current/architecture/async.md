---
title: 异步
description: "运行中的服务持有哪些任务，一条消息怎样经过它们，以及关闭时按什么顺序停下它们。"
---

import {Diagram} from '@site/src/components/Diagrams';

export const topology = {
  "title": "应用运行时上各种分发形式的任务，以及旁边的专用线程",
  "text": "以文字阅读本图",
  "broker": "已连接 Broker 的订阅流",
  "runtime": "应用运行时：Tokio，默认多线程",
  "loop": ["默认", "一个任务", "完成所有步骤"],
  "pool": ["workers(n)", "一个循环任务", "分发消息"],
  "feeder": ["threads(n)", "一个投递任务", "填充环形队列"],
  "workerTasks": ["n 个 worker 任务", "各有一格收件箱"],
  "rings": ["n 个环形队列", "各有八个槽位"],
  "tracker": ["受跟踪的任务：and_after、", "钩子、retry_after 定时器"],
  "osThreads": ["n 个系统线程", "各自运行一个", "Tokio 运行时"],
  "poll": "poll_next",
  "inbox": "send",
  "push": "push",
  "pop": "pop",
  "spawn": "spawn"
};

export const timeline = {
  "title": "一条消息的各个步骤，放在执行该步骤的任务那一列",
  "text": "以文字阅读本图",
  "loopLane": "循环任务",
  "handlerLane": "处理任务",
  "laterLane": "稍后，受跟踪",
  "pollStep": ["轮询", "流"],
  "context": ["构建", "上下文"],
  "layers": "消费层",
  "decode": "解码",
  "body": ["处理器", "函数体"],
  "reply": ["发布", "应答"],
  "settle": "确认",
  "next": ["轮询", "下一条"],
  "nextTurn": "下一轮",
  "after": ["后续任务", "与定时器"],
  "spawn": "启动"
};

export const shutdown = {
  "title": "优雅关闭依次做了什么",
  "text": "以文字阅读本图",
  "stop": ["信号、fail-fast", "或 RunningApp::shutdown"],
  "onShutdown": ["运行 on_shutdown 钩子，", "此时 Broker 仍在连接"],
  "cancel": ["各循环不再", "接收新消息"],
  "finish": ["手上的消息", "处理完并确认"],
  "drain": ["受跟踪的后续任务", "和重试定时器执行完"],
  "brokers": ["Broker 按注册的", "相反顺序关闭"],
  "afterShutdown": ["运行 after_shutdown 钩子，", "此时 Broker 已关闭"]
};

# 异步

运行中的服务持有哪些任务，一条消息怎样经过它们，以及关闭时按什么顺序停下它们。

<Diagram name="asyncTopology" words={topology} />

服务运行在 Tokio 上。[`#[ruststream::app]`](../components/application/app-macro.md) 构建一个多线程运行时；`flavor = "current_thread"` 改用单线程运行时，`worker_threads = n` 设定线程数。

订阅者是消息的 `Stream`，背压来自轮询本身。默认的循环在上一条消息确认之后才轮询下一条。[`workers(n)`](../components/subscribers/dispatch/workers.md) 同时处理的消息不超过 `n` 条。`workers(n, by_key)` 把同一个键的所有消息交给同一个 worker，所以同一个键内保持顺序。[`threads(n)`](../components/subscribers/dispatch/threads.md) 最多预读 `n × 9` 条消息：每个线程手上一条，环形队列里八条。

专用线程在自己的单线程运行时上完成一条消息的全部处理，从解码到确认。处理器通过 [`Ctx<MainRuntime>`](../components/subscribers/context/ctx.md) 交给 [`MainRuntime`](api:ruststream::runtime::MainRuntime) 的工作，会回到应用运行时上执行。

## 一条消息 {/* #one-delivery */}

<Diagram name="asyncTimeline" words={timeline} />

默认情况下，处理任务就是循环任务本身；使用 `workers(n)` 时是一个 worker 任务；使用 `threads(n)` 时是一个专用线程。[`and_after`](../components/subscribers/messages/and-after.md) 后续任务、确认之后的钩子和 [`retry_after`](../components/subscribers/messages/retry-after.md) 定时器在确认之后启动，作为应用跟踪的任务运行。它们最多执行一次：此时消息已经确认。

## 关闭 {/* #shutdown */}

<Diagram name="asyncShutdown" words={shutdown} />

[关闭超时](../components/application/running/shutdown.md)限制等待的时长。超时之后，仍在运行的循环会被中止，尚未完成的受跟踪任务则任其自行结束。不设超时时，关闭会等待全部任务完成。在专用线程上用普通 `tokio::spawn` 启动的任务不受跟踪，随线程一起结束。
