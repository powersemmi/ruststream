# 基准测试

在 Broker 客户端和你的处理器之间加一层框架，每条消息都要付出代价：订阅流、解码、分发、ack。这个页面
公布这份代价有多大，对照的是在同一台机器上做同样工作的裸客户端。

每个 Broker crate 自己测自己，并公布自己的数字；这个页面把它们读进来，放在一起展示。这里不保存任何
副本，因此某个 Broker 重新测量之后，下一次文档部署就会改变你在下面读到的内容。

## 结果 { #results }

数字是交替配对多次运行后的中位数，括号里是观察到的波动范围。

<div id="benchmark-results" data-benchmark-labels='{"loading":"正在读取已公布的结果...","broker":"Broker","scenario":"场景","raw":"裸客户端","framework":"RustStream","overhead":"额外开销","indistinguishable":"无法区分","brokerBound":"受 Broker 限制","measured":"测量于","details":"完整结果与方法论","pending":"尚未公布结果：{brokers}。"}'></div>

## 这个数字是什么 { #what-the-number-is }

这项测量只回答一个问题：Broker 客户端与你的处理器之间的那一层，每条消息要花多少。它不是 Broker 之间
的比较。每一行都是由维护该 Broker crate 的人在自己的机器上、对着 localhost 上的 Broker 测出来的，所以
一行的绝对吞吐量说明不了另一行的任何事情；可比的只有同一行里的两列，而这正是本页要做的比较。

Broker 跑在 localhost 上，对框架所占的那部分工作来说是最苛刻的环境。这里没有网络延迟供每条消息的开销
藏身，因此同样的绝对开销，在这里占的比例比隔着真实网络时更大。请把这个百分比读作已部署服务所付代价的
上界，而不是一个典型值。

标着 `broker-bound` 的行，意思是裸客户端在整个运行中大部分时间都在等待套接字。框架的工作于是发生在
本来就在等待的时间里，测出来的差异会塌缩到零附近。对那种负载来说这是真实结果 - 饱和的消费者就是这个
样子 - 但它是分发开销的下界，而不是对它的测量，不能读成"免费"。

## 方法论 { #methodology }

每个 Broker crate 都遵循下面的流程，好让为某个 Broker 公布的数字，与为另一个 Broker 公布的数字含义
相同。偏离流程的 Broker 会在自己的页面上说明。

### 配对 { #the-pair }

一次运行是一对二进制程序，它们之间只有一个区别：消息是经由 RustStream 到达，还是直接经由 Broker
客户端到达。

- **同一个客户端，同一套客户端配置。** 预取、ack 模式、consumer group、持久化、连接数以及任何
  Broker 特有的调优，两边完全一致。框架一侧通过 RustStream 配置 Broker；最终生成的客户端设置仍然
  必须相同。
- **ack 的位置相同。** RustStream 在处理器返回之后 ack，因此裸循环也在同一位置 ack。在裸运行的末尾
  批量 ack，测的是另一种协议，而不是另一个框架。
- **解码成同一个类型。** 裸的一侧用同样的编解码器把载荷反序列化成同一个结构体，并通过
  `std::hint::black_box` 访问其中一个字段。省掉这一步是拿到错误数字最容易的方式：结果没人使用的解码
  会被优化器删掉，于是这场比较悄悄变成了解码与什么都不做之间的比较。
- **载荷逐字节相同。** 两边消费的消息体由同一个生成器产生。
- **运行时相同。** tokio 的 flavor、工作线程数以及在途消息数固定且相等。
- **构建相同。** 编译配置、`RUSTFLAGS` 和分配器一致，可观测性 feature（`logging`、`metrics`、`otel`）
  要么两边都关，要么两边都开。环境里带着 `-C target-cpu=native` 的机器产出的数字，别的机器复现不了，
  所以这些编译标志要和结果一起公布。

### 运行 { #the-run }

- **消费者在任何消息发布之前就已连接。** 否则一侧在消化 Broker 里已有的积压，另一侧收到的是实时投递，
  而在多数 Broker 里这是两条不同的路径。
- **每次运行都用自己的名字。** 每次运行都新建 subject、队列、流或 consumer group，这样第 N 次运行绝不
  会看到第 N-1 次留下的东西。
- **计时窗口从收到第一条消息开始，到最后一条完成 ack 结束。** 被测量的那些运行之前，先跑一次并丢弃
  结果的预热：建立连接、注册消费者以及最初的内存分配属于启动开销，不属于每条消息的开销。
- **消息条数要让一次运行至少持续五秒**，这样启动阶段的瞬态和计时器精度都留在噪声里。
- **配对是交替的，不是分块的。** 裸、框架、裸、框架，如此往复，至少十一对，丢弃第一对。先把一侧全部
  跑完再跑另一侧，会把机器的全部漂移（发热、后台负载、页缓存）算到第二个跑的那一侧头上。

### 报告 { #the-report }

- **两侧都报告中位数和波动范围**，基于保留下来的那些配对。单次运行的单个数字不算结果。
- **小于波动范围的差异公布为 `无法区分`，** 而不是一个百分比。正是这条规则让页面保持诚实：低于运行间
  噪声的数值，读起来像是从未测到过的精度。
- **饱和的消费者会被标注。** 当裸的一侧整个运行都在等 Broker 时，该行带上 `broker-bound`，它的数字
  被理解为下界。
- **环境与数字一起公布**：CPU 型号与核心数、内核、Broker 如何启动（镜像、容器、主机）、rustc 版本、
  各 crate 版本、构建配置以及编译标志。没有这些，一个数字既无法复现，也无法判断它是否过时。

## 如何公布结果 { #publishing-results }

Broker crate 用 `just bench` 对着自己 compose 文件里的 Broker 跑自己的测试程序，并把结果作为文档站点
的一部分公布：一个供人阅读的页面，以及一份供本页读取的 JSON 文档。

### 稳定路径 { #the-stable-path }

```text
https://powersemmi.github.io/<crate>/latest/benchmarks/results.json
```

该文件位于 Broker 仓库的 `docs/benchmarks/results.json`，因此文档构建会原样拷贝它，部署会把它放到
`latest` 别名下，紧挨着解释它的那个页面。各 Broker 站点与本站点同源，所以本页直接读取它们。

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

`schema` 是这份文档的版本，读取方遇到不认识的版本时，会把该 Broker 显示为尚未公布，而不是去猜。
`unit` 是一个短标签，页面把它渲染在该行每个数值旁边，所以写 `msg/s`，而不是一句话。`verdict` 按上面
的规则取 `measured` 或 `indistinguishable`；`overhead_percent` 两种情况下都记录，但只在判定为
`measured` 时展示。`broker_bound` 标记那些节奏由 Broker 而非消费者决定的运行。

加载失败、或者带着无法识别的 `schema` 的文档，会让它的 Broker 留在"尚未公布结果"那一行。这是有意为
之：publish 出问题应当可见，而不是悄悄消失。
