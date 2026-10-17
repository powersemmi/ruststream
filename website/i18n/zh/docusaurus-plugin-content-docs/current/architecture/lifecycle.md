---
title: 生命周期
description: "Broker 的各个状态都是类型：配置、连接和已关闭的连接；进入每个状态的调用都会消耗前一个状态。"
---

import {Diagram} from '@site/src/components/Diagrams';

export const words = {
  "title": "Broker 的状态作为类型，以及让它从一个状态进入下一个状态的应用步骤",
  "text": "以文字阅读本图",
  "brokerSide": "Broker",
  "appSide": "应用",
  "unconnected": ["B::new(config)", "同步，无 I/O"],
  "connected": ["B::Connected", "subscribe, publish"],
  "closed": ["Closed", "关闭报告"],
  "builder": ["构建函数", "注册 Broker", "和处理器"],
  "running": ["RunningApp", "已连接，", "循环在运行"],
  "stopped": ["已停止", "循环已排空，", "Broker 已关闭"],
  "connect": "connect(self)",
  "shutdown": "shutdown(self)",
  "start": "start()",
  "stop": "shutdown()",
  "afterShutdown": ["比 Broker 连接活得更久的发布者", "会返回错误，而不是默默成功"]
};

# 生命周期

Broker 的各个状态都是类型：配置、连接和已关闭的连接；进入每个状态的调用都会消耗前一个状态。

<Diagram name="lifecycle" words={words} />

创建 Broker 不做任何 I/O，所以无论用哪个 Broker，[`#[ruststream::app]`](../components/application/app-macro.md) 的构建函数都保持同步。所有网络调用都等到 `start()` 才发生。

订阅和发布只是已连接形式的方法。在 `connect` 之前持有 Broker 的代码没有可用来订阅的东西，而 `shutdown` 会消耗它关闭的连接，所以这类错误无法通过编译。

[`start()`](../components/application/running/start.md) 按注册顺序连接 Broker，在各自已连接的 Broker 上打开每个订阅，然后运行 `after_startup` 钩子。途中任何一步失败，已经打开的部分会按相反顺序关闭。[`shutdown()`](../components/application/running/shutdown.md) 停下各循环，等它们排空，再按注册的相反顺序关闭 Broker。每个[钩子](../components/application/lifecycle.md)在哪一步运行，画在[异步](async.md#shutdown)一页上。

早先交出去的发布者可能比连接活得更久。关闭之后再用它会返回错误。一致性测试套件要求每个 Broker crate 都遵守这条规则。
