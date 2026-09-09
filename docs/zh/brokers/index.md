# Broker

处理器、路由器、编解码器和中间件都不依赖 Broker。把服务换到另一个 Broker，你只需要改 `with_broker`
里的一行。

框架自带一个完整的内存 Broker，用于单个应用内部的队列。基于外部服务的 Broker 是独立的 crate，加进
依赖即可使用。

每个 Broker crate 都有自己的文档站点，链接在下表的“文档”列和 **Broker** 菜单里。

| Broker | Crate | 传输 | 文档 |
|---|---|---|---|
| [内存](memory.md) | `ruststream`（feature `memory`） | 进程内队列，无需外部服务 | 本站 |
| NATS | [`ruststream-nats`](https://github.com/powersemmi/ruststream-nats) | Core NATS 与 JetStream | [powersemmi.github.io/ruststream-nats](https://powersemmi.github.io/ruststream-nats/) |
| Redis | [`ruststream-fred`](https://github.com/powersemmi/ruststream-fred) | Redis Streams（单机、集群、哨兵） | [powersemmi.github.io/ruststream-fred](https://powersemmi.github.io/ruststream-fred/) |
| RabbitMQ | [`ruststream-lapin`](https://github.com/powersemmi/ruststream-lapin) | AMQP 0.9.1（队列、交换机、发布者确认、direct reply-to） | [powersemmi.github.io/ruststream-lapin](https://powersemmi.github.io/ruststream-lapin/) |
| Kafka | [`ruststream-rdkafka`](https://github.com/powersemmi/ruststream-rdkafka) | Apache Kafka（消费者组、提交跟踪、事务、精确一次管道） | [powersemmi.github.io/ruststream-rdkafka](https://powersemmi.github.io/ruststream-rdkafka/) |
| AMQP 1.0 | [`ruststream-amqp`](https://github.com/powersemmi/ruststream-amqp) | ActiveMQ Artemis、RabbitMQ 4.x、Azure Service Bus，以及更广泛的 AMQP 1.0 家族（请求-响应、事务） | [powersemmi.github.io/ruststream-amqp](https://powersemmi.github.io/ruststream-amqp/) |
| Google Cloud Pub/Sub | [`ruststream-gcp-pubsub`](https://github.com/powersemmi/ruststream-gcp-pubsub) | 基于官方客户端的 Pub/Sub（排序键、精确一次确认、死信策略） | [powersemmi.github.io/ruststream-gcp-pubsub](https://powersemmi.github.io/ruststream-gcp-pubsub/) |
| AWS SQS / SNS | [`ruststream-sqs-sns`](https://github.com/powersemmi/ruststream-sqs-sns) | SQS 队列配合 SNS 扇出（FIFO 组、可见性管理、原生的延迟重试） | [powersemmi.github.io/ruststream-sqs-sns](https://powersemmi.github.io/ruststream-sqs-sns/) |
| Apache Pulsar | [`ruststream-pulsar`](https://github.com/powersemmi/ruststream-pulsar) | Pulsar 主题与主题模式（订阅模式、死信策略、重新定位） | [powersemmi.github.io/ruststream-pulsar](https://powersemmi.github.io/ruststream-pulsar/) |
| MQTT 5 | [`ruststream-rumqttc`](https://github.com/powersemmi/ruststream-rumqttc) | MQTT v5（QoS 等级、共享组、保留消息） | [powersemmi.github.io/ruststream-rumqttc](https://powersemmi.github.io/ruststream-rumqttc/) |
| ZeroMQ | [`ruststream-zeromq`](https://github.com/powersemmi/ruststream-zeromq) | 无 Broker 的 PUSH/PULL、PUB/SUB，以及基于 TCP 和 IPC 的 DEALER/ROUTER 请求-响应 | [powersemmi.github.io/ruststream-zeromq](https://powersemmi.github.io/ruststream-zeromq/) |
| 流文件 / stdio | [`ruststream-sea-file`](https://github.com/powersemmi/ruststream-sea-file) | 持久、可重放的流文件与 shell 管道；零基础设施，支持完整的重新定位 | [powersemmi.github.io/ruststream-sea-file](https://powersemmi.github.io/ruststream-sea-file/) |
| AWS Kinesis | [`ruststream-kinesis`](https://github.com/powersemmi/ruststream-kinesis) | Kinesis 数据流（分片租约、检查点、重新定位） | [powersemmi.github.io/ruststream-kinesis](https://powersemmi.github.io/ruststream-kinesis/) |

要为别的传输实现 Broker，参见[编写一个 Broker](../broker-authors/index.md)。

## 切换 Broker { #switching-brokers }

每个 Broker 都是同步构造的，运行时在应用启动时连接它。下面的例子里，只有构造 Broker 的那一行不同。

=== "内存"

    <!-- inline-rust: side-by-side broker-switch comparison; the NATS half depends on the external ruststream-nats crate and has no in-repo compiled home, so both halves stay inline to read in parallel -->
    ```rust
    use ruststream::memory::MemoryBroker;
    use ruststream::runtime::{AppInfo, RustStream};

    #[ruststream::app]
    fn app() -> RustStream {
        RustStream::new(AppInfo::new("orders", "0.1.0"))
            .with_broker(MemoryBroker::new(), |b| b.include_router(routes::orders()))
    }
    ```

=== "NATS"

    <!-- inline-rust: NATS half of the broker-switch comparison; depends on the external ruststream-nats crate, no in-repo compiled home -->
    ```rust
    use ruststream::runtime::{AppInfo, RustStream};
    use ruststream_nats::NatsBroker;

    #[ruststream::app]
    fn app() -> RustStream {
        RustStream::new(AppInfo::new("orders", "0.1.0"))
            .with_broker(NatsBroker::new("nats://localhost:4222"), |b| {
                b.include_router(routes::orders())
            })
    }
    ```

=== "Redis"

    <!-- inline-rust: Redis half of the broker-switch comparison; depends on the external ruststream-fred crate, no in-repo compiled home -->
    ```rust
    use ruststream::runtime::{AppInfo, RustStream};
    use ruststream_fred::RedisBroker;

    #[ruststream::app]
    fn app() -> RustStream {
        RustStream::new(AppInfo::new("orders", "0.1.0"))
            .with_broker(RedisBroker::standalone("redis://localhost:6379"), |b| {
                b.include_router(routes::orders())
            })
    }
    ```

=== "RabbitMQ"

    <!-- inline-rust: RabbitMQ half of the broker-switch comparison; depends on the external ruststream-lapin crate, no in-repo compiled home -->
    ```rust
    use ruststream::runtime::{AppInfo, RustStream};
    use ruststream_lapin::LapinBroker;

    #[ruststream::app]
    fn app() -> RustStream {
        RustStream::new(AppInfo::new("orders", "0.1.0"))
            .with_broker(LapinBroker::new("amqp://localhost:5672"), |b| {
                b.include_router(routes::orders())
            })
    }
    ```

=== "Kafka"

    <!-- inline-rust: Kafka half of the broker-switch comparison; depends on the external ruststream-rdkafka crate, no in-repo compiled home -->
    ```rust
    use ruststream::runtime::{AppInfo, RustStream};
    use ruststream_rdkafka::KafkaBroker;

    #[ruststream::app]
    fn app() -> RustStream {
        RustStream::new(AppInfo::new("orders", "0.1.0"))
            .with_broker(
                KafkaBroker::new(["localhost:9092"]).default_group("orders"),
                |b| {
                    b.include_router(routes::orders())
                },
            )
    }
    ```

每个 Broker crate 在自己的文档里说明连接选项。订阅需要 Broker 专有的选项时（消费者组、
durable 名称），你可以在 `#[subscriber(..)]` 属性里给出该 Broker 的描述符；参见
[Broker 专有的描述符](../guides/subscribers.md#broker-specific-descriptors)。
