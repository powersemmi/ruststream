---
title: 基准测试
description: "RustStream 在每条消息上的开销：与各 Broker 裸客户端对照的吞吐量，以及框架自身代码的指令数和内存分配次数。"
---

import {BenchmarkTabs, MovedSections} from '@site/src/components/Benchmarks';

export const labels = {"loading": "正在读取已公布的结果...", "scenario": "场景", "raw": "裸客户端", "adapter": "Broker crate", "framework": "RustStream", "overhead": "额外开销", "against": "（比客户端 {percent}）", "indistinguishable": "无法区分", "brokerBound": "受 Broker 限制", "measured": "测量于", "details": "完整结果与方法论", "pending": "尚未公布结果：{brokers}。", "instructions": "指令数", "allocations": "内存分配", "cold": "冷启动", "table": "以表格显示数字", "axis": "各循环比裸客户端最佳一轮低多少", "rounds": "各轮，从最佳到最慢", "best": "最佳一轮", "median": "中位一轮", "noise": "轮次的波动范围", "comparison": "与裸客户端对照", "code": "代码的开销", "broker": "Broker", "plugin": "{plugin} 插件", "pluginRaw": "不用插件", "pluginAdapter": "手写的插件", "pluginFramework": "crate 自带的插件", "pluginAxis": "各循环比不用插件的应用最佳一轮低多少", "pluginAgainst": "（比不用插件 {percent}）", "pluginOverhead": "比手写插件", "pluginOverHand": "（比手写插件 {percent}）"};

# 基准测试

RustStream 在每条消息上的开销：一是与各 Broker 裸客户端的对照，二是框架自身代码的开销。

Broker 客户端和你的处理器之间隔着一层框架，每条消息都要为它花时间：订阅流、解码、分发、ack。
本页把这份代价公布两次，用两种测量回答两个不同的问题。

第一种是在真实 Broker 上、与裸客户端对照的吞吐量，同一台机器，同样的工作。它说明已部署的服务
付出了多少。

第二种是代码本身的开销：每条消息的指令数和内存分配次数。在 `ruststream` 里，这是框架自身代码的
开销，数字里不含 Broker。它说明代码变动之后有什么变了，而且足够精确，可以让一个把消息变贵的
pull request 通不过。

每个 crate 按同一套[方法论](methodology.md)测量自己，并[公布](publishing.md)自己的数字。
本页从各 crate 仓库的 `main` 分支读取这些数字，每个 crate 占一个标签页。新的测量结果一经合并，
就会出现在它的标签页里。这些数字能说明什么、不能说明什么，见[数字](numbers.md)。

## 与裸客户端对照 {/* #against-a-raw-client */}

Broker 标签页里的第一张图是对照图。每个场景画出三个循环，每个循环跑三个交替轮次，坐标轴的零点是裸客户端的最佳一轮。一个循环画成
一条横条，从最佳一轮延伸到最慢的一轮；圆点标出最佳一轮，缺口标出中位一轮。圆点在零点右侧，
表示比裸客户端慢。最佳的一轮最接近未受干扰的开销，中位的一轮则是重跑一次通常会得到的那一轮。

零点周围的阴影带是 RustStream 的 `无法区分` 判定的依据：最佳一轮到最差一轮之间的波动范围。
最佳一轮落在带内，就与裸客户端无法区分。空心圆点标出 crate 判定为无法区分的循环，每行旁边的
判定由该 crate 自己公布。鼠标悬停或键盘聚焦时，该行
显示自己的数字；图表下方的表格列出全部数字：最佳一轮，括号里是中位数。

“Broker crate”是该 crate 自己的消费者和发布者，不带运行时，因此两项差值可以分开读：crate 比
客户端多付多少，运行时又在其上多付多少。

如果 crate 还提供插件，插件的场景画在单独的一张图上，用自己的坐标轴。图中的三个循环是同一个应用的
三种写法：不用插件、手写插件、使用 crate 自带的插件。插件的表格和 crate 自带插件旁边的判定，还给出它比手写插件多付出多少。

## 代码的开销 {/* #cost-of-the-code */}

代码开销图是每个标签页的最后一张图，也是核心标签页唯一的一张图。它展示稳态下每条消息的指令数和内存分配次数。这些是绝对数值，不是对照：框架相对于 Broker 客户端
多付出多少，看对照图。`ruststream` 在进程内传输上测量框架自身的代码。Broker crate
在真实的传输上测量自己，数字里包含什么，写在它的基准测试页面上。

每个 crate 各用一条坐标轴，因为 Broker crate 的数字里还包含它的客户端的工作。“冷启动”是启动
服务并处理第一条消息一共花掉的指令数和内存分配次数；这笔开销一个服务只付一次，不按消息计。
鼠标悬停或聚焦时该行会显示它，图表下方的表格也列出它。每个 crate 图表上方那一行先写跑出这些数字的机器，一直写到内存，再写这次测量的 crate
版本、`ruststream` 版本和测量日期。只有知道两台机器上编译的是同一份代码，指令数才能互相比较。

## 结果 {/* #results */}

<BenchmarkTabs labels={labels} />

<MovedSections />
