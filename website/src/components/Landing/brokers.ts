import type {ComponentType, SVGProps} from 'react';
import Nats from '@site/static/img/brokers/nats.svg';
import Redis from '@site/static/img/brokers/redis.svg';
import RabbitMq from '@site/static/img/brokers/rabbitmq.svg';
import Kafka from '@site/static/img/brokers/kafka.svg';
import Amqp from '@site/static/img/brokers/amqp.svg';
import PubSub from '@site/static/img/brokers/pubsub.svg';
import Sqs from '@site/static/img/brokers/sqs.svg';
import Pulsar from '@site/static/img/brokers/pulsar.svg';
import Mqtt from '@site/static/img/brokers/mqtt.svg';
import ZeroMq from '@site/static/img/brokers/zeromq.svg';
import Files from '@site/static/img/brokers/files.svg';
import Kinesis from '@site/static/img/brokers/kinesis.svg';
import Sql from '@site/static/img/brokers/sql.svg';
import Memory from '@site/static/img/brokers/memory.svg';

export interface Broker {
  /** The translation id of the name; proper names keep their spelling in every language. */
  id: string;
  name: string;
  crate: string;
  href: string;
  Icon: ComponentType<SVGProps<SVGSVGElement>>;
}

const site = (crate: string) => `https://powersemmi.github.io/${crate}/`;

export const BROKERS: Broker[] = [
  {id: 'kafka', name: 'Kafka', crate: 'ruststream-rdkafka', Icon: Kafka},
  {id: 'nats', name: 'NATS', crate: 'ruststream-nats', Icon: Nats},
  {id: 'rabbitmq', name: 'RabbitMQ', crate: 'ruststream-lapin', Icon: RabbitMq},
  {id: 'redis', name: 'Redis / Valkey', crate: 'ruststream-fred', Icon: Redis},
  {id: 'amqp', name: 'AMQP 1.0', crate: 'ruststream-amqp', Icon: Amqp},
  {id: 'pubsub', name: 'Google Pub/Sub', crate: 'ruststream-gcp-pubsub', Icon: PubSub},
  {id: 'sqs', name: 'SQS / SNS', crate: 'ruststream-sqs-sns', Icon: Sqs},
  {id: 'pulsar', name: 'Pulsar', crate: 'ruststream-pulsar', Icon: Pulsar},
  {id: 'mqtt', name: 'MQTT 5', crate: 'ruststream-rumqttc', Icon: Mqtt},
  {id: 'zeromq', name: 'ZeroMQ', crate: 'ruststream-zeromq', Icon: ZeroMq},
  {id: 'kinesis', name: 'Kinesis', crate: 'ruststream-kinesis', Icon: Kinesis},
  {id: 'files', name: 'Files / stdio', crate: 'ruststream-sea-file', Icon: Files},
  {id: 'sql', name: 'SQL databases', crate: 'ruststream-sqlx', Icon: Sql},
].map((broker) => ({...broker, href: site(broker.crate)}));

export const MEMORY_BROKER: Broker = {
  id: 'memory',
  name: 'In-memory',
  crate: 'ruststream',
  href: 'https://docs.rs/ruststream/latest/ruststream/memory/index.html',
  Icon: Memory,
};
