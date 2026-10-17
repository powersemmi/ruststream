---
description: "crate 如何公布基准测试结果：一份放在稳定路径上的 JSON 文档，由基准测试页面读取。"
---

# 发布

crate 如何公布基准测试结果：一份放在稳定路径上的 JSON 文档，由[基准测试](index.md)页面读取。

Broker crate 用 `just bench` 对着自己 compose 文件里的 Broker 运行自己的基准程序。它把结果
提交到自己的仓库：一个供人阅读的页面，和一份供基准测试页面读取的 JSON 文档。

## 稳定路径 {/* #the-stable-path */}

该文件位于 Broker 仓库的 `docs/benchmarks/results.json`，紧挨着解释它的页面。基准测试页面从
`main` 分支读取它，所以新的测量结果合并后立即可见。

```text
https://raw.githubusercontent.com/powersemmi/<crate>/main/docs/benchmarks/results.json
```

## 文档 {/* #the-document */}

`schema` 是文档的版本：3 为每个循环报告三轮（`best`、`median` 和 `worst`），2 增加了 `code`
一节，模式 1 的文档报告的是中位数及其两端。表格里印出 `best`，括号里是 `median`；`worst` 只读
不印，判定规则依据的是最佳一轮与最差一轮之间的距离。模式 3 的文档若没有 `median`，括号里仍是
最差的一轮，这也正是该字段出现之前的印法。`unit` 是该行每个数值旁边的短标签，所以填 `msg/s`，
而不是一句话。
`verdict` 按[波动规则](methodology.md#the-report)取 `measured` 或 `indistinguishable`。`adapter` 和
`adapter_overhead_percent` 是该 crate 自己的消费者和发布者对同一客户端测得的结果，不带运行时；
`adapter_verdict` 把同一条波动规则
用在 crate 与裸客户端的那项差值上；crate 没有给出它时，页面就按已公布的波动范围自己套同一条
规则，使同一行的两列不会对“什么是可见的”给出相反的说法。`environment` 里还可以带 `round_trip`，即 `broker_bound`
算式所依据的探测值，读者可以自己重算。`overhead_percent` 是框架相对裸客户端的端到端开销，
两种情况都记录，
只在判定为 `measured` 时展示。`broker_bound` 标记那些由 Broker 而不是消费者决定节奏的运行。

`environment` 描述机器和构建。`cpu`、`architecture`、`cpu_frequency`、`cores`、`memory` 和
`memory_speed` 说的是机器，`profile` 和 `features` 说的是基准测试用什么构建。机器不公布的字段写
成 `unknown`，不去猜：内存速率来自 DMI 表，而多数系统只让 root 读它。除 `cpu`、`os` 和 `rustc`
之外都是可选的，因此 schema 为 1 的文档照样可读。

`code` 供代码开销图使用，每个场景一条记录。`framework` 是稳态下每条消息的量，`cold` 则是启动服务加
第一条消息的全部开销，没有除以任何东西。`gated` 说明该场景出现回归时，该 crate 的基准测试命令
是否失败。只公布 `scenarios` 的 crate 声明 `schema` 为 1，只显示对照图。
自己不测量 Broker 的 crate 则根本不写 `scenarios`，而不是留下一个空数组，只显示代码开销图。

有的 crate 在与裸客户端对照之外，单独运行一次代码测量。
这样的 crate 会把这次运行的日期和版本写进 `code_measured`：`measured_at`、`crate_version` 和
`core_version`。代码开销图上方那一行的日期和版本取自这里；文档里没有这一字段时，取顶层的对应字段。

无法加载的文档，或者 `schema` 无法识别的文档，会让所属 crate 的标签页只显示“尚未公布结果”那一行。
这样，公布环节一旦出问题就看得见，不会悄无声息地消失。

```json
{
  "schema": 3,
  "crate": "ruststream-nats",
  "crate_version": "0.7.0",
  "core_version": "0.7.0",
  "measured_at": "2026-08-20",
  "environment": {
    "cpu": "AMD Ryzen 9 5950X 16-Core Processor",
    "architecture": "x86_64 (x86-64-v3)",
    "cpu_frequency": "base 3400 MHz, max 4900 MHz",
    "cores": "16 physical, 32 logical",
    "memory": "62.7 GiB",
    "memory_speed": "DDR4, 3600 MT/s",
    "os": "Linux 6.16.7",
    "broker": "nats:2.10-alpine in Docker on localhost",
    "rustc": "1.90.0",
    "valgrind": "3.25.1",
    "profile": "bench, inheriting release (opt-level = 3, lto = false, codegen-units = 16)",
    "features": "--no-default-features --features memory,macros,json",
    "rustflags": "-C target-cpu=native"
  },
  "scenarios": [
    {
      "name": "core NATS, 512 B JSON, ack each",
      "unit": "msg/s",
      "messages": 200000,
      "pairs": 3,
      "raw": { "best": 129604, "median": 128470, "worst": 126980 },
      "adapter": { "best": 129310, "median": 128040, "worst": 126700 },
      "framework": { "best": 129020, "median": 127640, "worst": 126100 },
      "overhead_percent": 0.4,
      "adapter_overhead_percent": 0.3,
      "adapter_verdict": "indistinguishable",
      "verdict": "indistinguishable",
      "broker_bound": true
    }
  ],
  "code": [
    {
      "name": "consume, JSON decode into a small struct",
      "messages": 1000,
      "framework": { "instructions": 2839.8, "allocations": 0.0 },
      "cold": { "instructions": 19219, "allocations": 26 },
      "gated": true
    }
  ]
}
```
