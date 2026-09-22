# 基准测试

Broker 客户端和你的处理器之间隔着一层框架，每条消息都要为它花时间：订阅流、解码、分发、ack。
本页把这份代价公布两次，用两种测量回答两个不同的问题。

第一种是在真实 Broker 上、与裸客户端对照的吞吐量，同一台机器，同样的工作。它说明已部署的服务
付出了多少。

第二种是框架自身代码的开销：每条消息的指令数和内存分配次数，数字里不含 Broker。它说明框架变动
之后有什么变了，而且足够精确，可以让一个把消息变贵的 pull request 通不过。

每个 crate 自己测量自己，并公布自己的数字。本页读取这些数字，放在一起展示。这里不保存副本，
因此某个 crate 重新测量之后，下一次发布自己的文档时就会改变下面的表格。

## 结果 { #results }

### 与裸客户端对照 { #against-a-raw-client }

数值是三个交替轮次中的最佳值，括号里是这三轮的中位数。最佳的一轮最接近未受干扰的开销，中位的
一轮则是重跑一次通常会得到的那一轮。最差的一轮同样公布，但不进入单元格。`无法区分` 的判定要
用到它：判定依据的是最佳一轮到最差一轮之间的波动范围。“Broker crate”一列是该 crate 自己的
消费者和发布者，不带运行时，因此两项差值可以分开读：crate 比客户端多付多少，运行时又在其上
多付多少。

<div id="benchmark-results" data-benchmark-labels='{"loading": "正在读取已公布的结果...", "broker": "Broker", "scenario": "场景", "raw": "裸客户端", "adapter": "Broker crate", "framework": "RustStream", "overhead": "额外开销", "against": "（比客户端 {percent}）", "indistinguishable": "无法区分", "brokerBound": "受 Broker 限制", "measured": "测量于", "details": "完整结果与方法论", "pending": "尚未公布结果：{brokers}。", "crate": "Crate", "instructions": "指令数", "allocations": "内存分配", "cold": "冷启动"}'></div>

### 代码的开销 { #cost-of-the-code }

稳态下每条消息的指令数和内存分配次数，在进程内传输上测得。这是框架自身代码的绝对数值，不是
对照：框架相对于 Broker 客户端多付出多少，看上面那张表，那里的客户端是真实的。

<div id="benchmark-code"></div>

“冷启动”一列是启动服务并处理第一条消息一共花掉的指令数和内存分配次数；这笔开销一个服务只付一
次，不按消息计。表格下面那一行是跑出这些数字的机器，一直写到内存：只有知道两台机器上编译的是
同一份代码，指令数才能互相比较。

## 这些数字是什么 { #what-the-numbers-are }

### 与裸客户端的对照 { #the-comparison-against-a-raw-client }

每一行都由维护该 Broker crate 的人测出，在自己的机器上，对着 localhost 上的 Broker。
因此行与行之间不可比：一行的绝对吞吐量说明不了另一行的任何事情。可比的只有同一行里的两列，
本页要做的正是这一比较。

localhost 上的 Broker，对框架来说是最苛刻的环境。这里没有网络延迟。每条消息的绝对开销不变，
因此占的比例比隔着真实网络时更大。应把该百分比读作已部署服务所付代价的上界，而不是典型值。

“受 Broker 限制”标记表示，裸客户端在整个运行的大部分时间里都在等套接字。框架的工作于是发生在
原本就在等待的时间里，测出的差异趋近于零。对这类负载来说这是真实结果：饱和的消费者就是这样。
但这样的数字是分发开销的下界，不是对它的测量，切勿读成“免费”。

### 框架代码的开销 { #the-cost-of-the-code }

指令数是精确的。同一个二进制程序跑两次得到同一个数字，快一倍的机器也得到同一个数字，因此这张
表的各行之间可比，与另一台机器上测出的同一行也可比。它说明不了的是时间：同样的指令数，在缓存
未命中处要花更多时间，表格旁边那对按时钟计时的数字正是为此而测。

这个数字就是本 crate 自身的工作，此外别无他物：用户写的服务，跑在进程内队列上，把载荷解码成
一个类型、读一个字段、确认投递。里面没有 Broker，所以只有框架的代码变了，数字才会变 —— 这正是
两个百分点可以算作缺陷而不是噪声的原因。

每条消息的数字都是稳态。启动服务的开销只付一次：连接、建立订阅，以及它们背后的首批内存分配。
把它摊到一次运行的消息上，等于把一次性的价钱当成每条消息的价钱公布出去。因此每个场景测两次，
一千条和两千条，一条消息的开销就是两次运行之差；冷启动单独测，只投递一条消息。

内存分配按每条消息统计，投递路径上是零：服务跑起来之后，消息从队列走到处理器函数体，框架一次
也没有向分配器要过内存。发布路径上的数字，就是 Broker 为持有交给它的消息所要的那几次分配，此外
再无其他。

## 方法论 { #methodology }

每个 Broker crate 都遵循下面的流程。这样一来，不同 Broker 公布的数字含义相同。偏离流程的
Broker 会在自己的页面上说明。

### 三个循环 { #the-three-loops }

一次运行把同一个场景测三遍，三者只有一个区别：消息由什么承载。

- **裸客户端。** Broker 自己的客户端，直接驱动。
- **Broker crate。** 该 crate 自己的消费者和发布者 —— 它的订阅描述符、它产出的订阅者流、它的
  确认、它的发布者 —— 由基准测试里的一个循环驱动，上面没有运行时。
- **RustStream。** 用户写的那整个服务：处理器、应用、分发。

第一项差值是这个 Broker crate 比它所包装的客户端多付出多少，这由 crate 自己负责。第二项是
运行时在其上多付出多少，而且是在这个 Broker 上；它按 Broker 分别公布，因为各个 crate 与运行时
相接的方式不同：流如何产出、投递是否成批到达、背压如何传到消费者。

- **同一个客户端，同一套客户端配置。** 预取、ack 模式、consumer group、持久化、连接数和
  Broker 特有的调优，三个循环完全一致。框架一侧通过 RustStream 配置 Broker，最终生成的客户端设置
  仍然必须相同。
- **ack 的位置相同。** RustStream 在处理器返回之后 ack，因此另外两个循环也在同一位置 ack。在裸运行
  的末尾批量 ack，测的是另一种协议，而不是另一个框架。
- **解码成同一个类型。** 没有处理器的那两个循环用同样的编解码器把载荷反序列化成同一个结构体，并通过
  `std::hint::black_box` 访问其中一个字段。省掉这一步，最容易得出错误的数字：解码结果没有人
  用，优化器就会把解码删掉，裸的一侧于是悄悄不再解码。
- **载荷逐字节相同。** 每个循环消费的消息体都来自同一个生成器。
- **运行时相同。** tokio 的 flavor、工作线程数和同时在处理的消息条数，三者一致。
- **构建相同。** 构建配置、`RUSTFLAGS` 和分配器一致，可观测性 feature（`logging`、`metrics`、
  `otel`）要么两边都关，要么两边都开。环境里带着 `-C target-cpu=native` 的机器，产出的数字
  别的机器复现不了，因此这些标志与结果一起公布。

### 运行 { #the-run }

- **消费者在第一条消息发布之前就已连接。** 否则一侧消费的是 Broker 里已有的积压，另一侧收到
  的是实时投递，而在多数 Broker 里这是两条不同的路径。
- **每次运行都用自己的名字。** subject、队列、流或 consumer group 每次运行都新建，这样第 N 次
  运行绝不会看到第 N-1 次留下的东西。
- **计时窗口从收到第一条消息开始，到最后一条完成 ack 结束。** 测量的运行之前，先跑一次校准
  运行并丢弃结果。建立连接、注册消费者和最初的内存分配属于启动开销，不属于每条消息的开销。
- **消息条数要让一次运行至少持续五秒**，这样启动阶段的瞬态和计时器精度都落在噪声范围内。
- **配对是交替的，不是分块的。** 裸、框架、裸、框架，如此往复，共三轮。先把一侧全部跑完再跑
  另一侧，会把机器的全部漂移（发热、后台负载、页缓存）算到跑在后面的那一侧头上。

### 报告 { #the-report }

- **每个循环都报告三轮：最佳、中位和最差。** 机器上的噪声只会让运行变慢，所以最快的一轮最接近
  未受干扰的开销，中位的一轮是典型的一轮，最慢的一轮则说明机器离安静有多远。单次运行的单个
  数字不算结果。
- **表格里印出最佳的一轮，括号里是中位数。** 最差的一轮留在文档里，不进入单元格：它的用处是
  波动范围，也就是最佳的一轮到最差的一轮之间的距离。
- **小于该波动范围的差异公布为 `无法区分`，** 而不是一个百分比：低于运行间噪声的数值，读起来
  像是从未测到过的精度。
- **饱和的消费者要标注出来。** 当裸的一侧整个运行都在等 Broker 时，该行带上 `broker-bound`。
- **环境与数字一起公布**：CPU 型号与核心数、内核、Broker 如何启动（镜像、容器、主机）、rustc
  版本、各 crate 版本、构建配置和编译标志。没有这些，一个数字既无法复现，也无法判断它是否过时。

### 代码的测量 { #the-code-measurement }

第二张表由 crate 仓库里的 `just bench` 生成。它需要 valgrind，以及与该 crate 所依赖版本一致的
基准测试 runner。`just bench 5000` 让每个场景在五千次投递上测量，而不是一千次：数字更稳，
运行更久；发布的文档和 CI 的阈值仍按默认值测量。

- **每个场景都是用户写的那个服务**，由真实运行时启动，测试用的 harness 不参与编译，测的因此
  就是发布出去的代码。这里没有手写对照：它要读的队列正是本 crate 自己的进程内 Broker，那样
  crate 就成了自己跟自己比。与别人写的客户端相比，是第一张表的事，由 Broker crate 来做。
- **传输是进程内的。** 被测的是框架自身的代码，数字不应随套接字、服务器负载或网络而变。
- **队列在测量区间打开之前就已填满。** 场景测的是稳态投递，不含连接、订阅建立，以及它们背后的
  首批内存分配。
- **采集只覆盖测量区间，不越界。** 准备和收尾跑在同一个进程里，走的是同一份框架代码，把它们也
  算进去的测量，会把填满队列的开销当成清空队列的开销报出来。
- **每个场景跑三次，各有各的用处。** 一条消息、一千条、两千条。后两次之差是服务跑起来之后一条
  消息的开销，只投递一条的那次就是冷启动。运行途中不需要关掉任何东西，正是这一点让这个办法对
  内存分配计数器也成立 —— 它根本没有开关。
- **每个场景三个数字。** callgrind 给出的指令数是精确的，作为门禁；DHAT 给出的内存分配次数是
  精确的，作为门禁；单独一次运行给出的时钟时间有噪声，仅供参考。
- **门禁看变化，不看数值。** 带 `run-bench` 标签的 pull request 与目标分支上同一套基准测试的
  结果对照：受门禁的场景里指令数多出两个百分点以上，就通不过；超出该场景声明的内存分配次数，
  同样通不过。表格会作为评论出现在 pull request 上。冷启动和时钟时间只打印出来。

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

`schema` 是这份文档的版本：3 为每个循环报告三轮（`best`、`median` 和 `worst`），2 增加了 `code`
一节，模式 1 的文档报告的是中位数及其两端。表格里印出 `best`，括号里是 `median`；`worst` 只读
不印，判定规则依据的是最佳一轮与最差一轮之间的距离。模式 3 的文档若没有 `median`，括号里仍是
最差的一轮，这也正是该字段出现之前的印法。`unit` 是该行每个数值旁边的短标签，所以填 `msg/s`，
而不是一句话。
`verdict` 按上面的规则取 `measured` 或 `indistinguishable`；`adapter_verdict` 把同一条波动规则
用在 crate 与裸客户端的那项差值上；crate 没有给出它时，页面就按已公布的波动范围自己套同一条
规则，使同一行的两列不会对“什么是可见的”给出相反的说法。`environment` 里还可以带 `round_trip`，即 `broker_bound`
算式所依据的探测值，读者可以自己重算。`overhead_percent` 是框架相对裸客户端的端到端开销，
两种情况都记录，
只在判定为 `measured` 时展示。`broker_bound` 标记那些由 Broker 而不是消费者决定节奏的运行。

`environment` 描述机器和构建。`cpu`、`architecture`、`cpu_frequency`、`cores`、`memory` 和
`memory_speed` 说的是机器，`profile` 和 `features` 说的是基准测试用什么构建。机器不公布的字段写
成 `unknown`，不去猜：内存速率来自 DMI 表，而多数系统只让 root 读它。除 `cpu`、`os` 和 `rustc`
之外都是可选的，因此 schema 为 1 的文档照样可读。

`code` 是第二张表，每个场景一条记录。`framework` 是稳态下每条消息的量，`cold` 则是启动服务加
第一条消息的全部开销，没有除以任何东西。`gated` 说明该场景出现回归时 CI 是否失败。只公布
`scenarios` 的 crate 声明 `schema` 为 1，仍然保留自己在第一张表里的行。

无法加载的文档，或者 `schema` 无法识别的文档，会让自己的 Broker 留在“尚未公布结果”那一行。
这样，公布环节一旦出问题就看得见，不会悄无声息地消失。
