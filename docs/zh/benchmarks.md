# 基准测试

Broker 客户端和你的处理器之间隔着一层框架，每条消息都要为它花时间：订阅流、解码、分发、ack。
本页公布这份代价。对照的是裸客户端，它在同一台机器上做同样的工作。

每个 Broker crate 自己测量自己，并公布自己的数字。本页读取这些数字，放在一起展示。
这里不保存副本，因此某个 Broker 重新测量之后，下一次发布自己的文档时就会改变下面的表格。

## 结果 { #results }

数值是交替配对的中位数，括号里是观察到的波动范围。

<div id="benchmark-results" data-benchmark-labels='{"loading":"正在读取已公布的结果...","broker":"Broker","scenario":"场景","raw":"裸客户端","framework":"RustStream","overhead":"额外开销","indistinguishable":"无法区分","brokerBound":"受 Broker 限制","measured":"测量于","details":"完整结果与方法论","pending":"尚未公布结果：{brokers}。"}'></div>

## 这个数字是什么 { #what-the-number-is }

每一行都由维护该 Broker crate 的人测出，在自己的机器上，对着 localhost 上的 Broker。
因此行与行之间不可比：一行的绝对吞吐量说明不了另一行的任何事情。可比的只有同一行里的两列，
本页要做的正是这一比较。

localhost 上的 Broker，对框架来说是最苛刻的环境。这里没有网络延迟。每条消息的绝对开销不变，
因此占的比例比隔着真实网络时更大。应把该百分比读作已部署服务所付代价的上界，而不是典型值。

“受 Broker 限制”标记表示，裸客户端在整个运行的大部分时间里都在等套接字。框架的工作于是发生在
原本就在等待的时间里，测出的差异趋近于零。对这类负载来说这是真实结果：饱和的消费者就是这样。
但这样的数字是分发开销的下界，不是对它的测量，切勿读成“免费”。

## 方法论 { #methodology }

每个 Broker crate 都遵循下面的流程。这样一来，不同 Broker 公布的数字含义相同。偏离流程的
Broker 会在自己的页面上说明。

### 配对 { #the-pair }

一次运行是一对二进制程序，两者只有一个区别：消息经由 RustStream 到达，还是直接经由 Broker
客户端到达。

- **同一个客户端，同一套客户端配置。** 预取、ack 模式、consumer group、持久化、连接数和
  Broker 特有的调优，两边完全一致。框架一侧通过 RustStream 配置 Broker，最终生成的客户端设置
  仍然必须相同。
- **ack 的位置相同。** RustStream 在处理器返回之后 ack，因此裸循环也在同一位置 ack。在裸运行
  的末尾批量 ack，测的是另一种协议，而不是另一个框架。
- **解码成同一个类型。** 裸的一侧用同样的编解码器把载荷反序列化成同一个结构体，并通过
  `std::hint::black_box` 访问其中一个字段。省掉这一步，最容易得出错误的数字：解码结果没有人
  用，优化器就会把解码删掉，裸的一侧于是悄悄不再解码。
- **载荷逐字节相同。** 两边消费的消息体来自同一个生成器。
- **运行时相同。** tokio 的 flavor、工作线程数和同时在处理的消息条数，两边一致。
- **构建相同。** 构建配置、`RUSTFLAGS` 和分配器一致，可观测性 feature（`logging`、`metrics`、
  `otel`）要么两边都关，要么两边都开。环境里带着 `-C target-cpu=native` 的机器，产出的数字
  别的机器复现不了，因此这些标志与结果一起公布。

### 运行 { #the-run }

- **消费者在第一条消息发布之前就已连接。** 否则一侧消费的是 Broker 里已有的积压，另一侧收到
  的是实时投递，而在多数 Broker 里这是两条不同的路径。
- **每次运行都用自己的名字。** subject、队列、流或 consumer group 每次运行都新建，这样第 N 次
  运行绝不会看到第 N-1 次留下的东西。
- **计时窗口从收到第一条消息开始，到最后一条完成 ack 结束。** 测量的运行之前，先跑一次预热
  并丢弃结果。建立连接、注册消费者和最初的内存分配属于启动开销，不属于每条消息的开销。
- **消息条数要让一次运行至少持续五秒**，这样启动阶段的瞬态和计时器精度都落在噪声范围内。
- **配对是交替的，不是分块的。** 裸、框架、裸、框架，如此往复，至少十一对，丢弃第一对。先把
  一侧全部跑完再跑另一侧，会把机器的全部漂移（发热、后台负载、页缓存）算到跑在后面的那一侧
  头上。

### 报告 { #the-report }

- **两侧都报告中位数和波动范围**，统计的是保留下来的那些配对。单次运行的单个数字不算结果。
- **小于波动范围的差异公布为 `无法区分`，** 而不是一个百分比：低于运行间噪声的数值，读起来
  像是从未测到过的精度。
- **饱和的消费者要标注出来。** 当裸的一侧整个运行都在等 Broker 时，该行带上 `broker-bound`。
- **环境与数字一起公布**：CPU 型号与核心数、内核、Broker 如何启动（镜像、容器、主机）、rustc
  版本、各 crate 版本、构建配置和编译标志。没有这些，一个数字既无法复现，也无法判断它是否过时。

## 如何公布结果 { #publishing-results }

Broker crate 用 `just bench` 对着自己 compose 文件里的 Broker 运行自己的基准程序。它把结果
公布在自己的文档站点上：一个供人阅读的页面，和一份供本页读取的 JSON 文档。

### 稳定路径 { #the-stable-path }

```text
https://powersemmi.github.io/<crate>/latest/benchmarks/results.json
```

该文件位于 Broker 仓库的 `docs/benchmarks/results.json`。文档构建原样拷贝它，部署把它放到
`latest` 别名下，紧挨着解释它的页面。各 Broker 站点与本站点同源，因此本页直接读取它们。

### 文档 { #the-document }

```json
{
  "schema": 1,
  "crate": "ruststream-nats",
  "crate_version": "0.7.0",
  "core_version": "0.7.0",
  "measured_at": "2026-08-20",
  "environment": {
    "cpu": "AMD Ryzen 9 5950X, 16 cores",
    "os": "Linux 6.16.7",
    "broker": "nats:2.10-alpine in Docker on localhost",
    "rustc": "1.90.0",
    "profile": "release, lto = thin, codegen-units = 1",
    "rustflags": "-C target-cpu=native"
  },
  "scenarios": [
    {
      "name": "core NATS, 512 B JSON, ack each",
      "unit": "msg/s",
      "messages": 200000,
      "pairs": 11,
      "raw": { "median": 128412, "min": 126980, "max": 129604 },
      "framework": { "median": 127905, "min": 126100, "max": 129020 },
      "overhead_percent": 0.4,
      "verdict": "indistinguishable",
      "broker_bound": true
    }
  ]
}
```

`schema` 是这份文档的版本。`unit` 是该行每个数值旁边的短标签，所以填 `msg/s`，而不是一句话。
`verdict` 按上面的规则取 `measured` 或 `indistinguishable`。`overhead_percent` 两种情况都记录，
只在判定为 `measured` 时展示。`broker_bound` 标记那些由 Broker 而不是消费者决定节奏的运行。

无法加载的文档，或者 `schema` 无法识别的文档，会让自己的 Broker 留在“尚未公布结果”那一行。
这样，公布环节一旦出问题就看得见，不会悄无声息地消失。
