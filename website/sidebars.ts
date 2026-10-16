import type {SidebarsConfig} from '@docusaurus/plugin-content-docs';

// Each broker crate is documented on its own site; the sidebar links straight there.
const brokerSites: Array<[string, string]> = [
  ['NATS', 'ruststream-nats'],
  ['Redis', 'ruststream-fred'],
  ['RabbitMQ', 'ruststream-lapin'],
  ['Kafka', 'ruststream-rdkafka'],
  ['AMQP 1.0', 'ruststream-amqp'],
  ['Google Cloud Pub/Sub', 'ruststream-gcp-pubsub'],
  ['AWS SQS / SNS', 'ruststream-sqs-sns'],
  ['Apache Pulsar', 'ruststream-pulsar'],
  ['MQTT 5', 'ruststream-rumqttc'],
  ['ZeroMQ', 'ruststream-zeromq'],
  ['Stream files / stdio', 'ruststream-sea-file'],
  ['AWS Kinesis', 'ruststream-kinesis'],
  ['SQL databases', 'ruststream-sqlx'],
];

const sidebars: SidebarsConfig = {
  docs: [
    {
      type: 'category',
      label: 'Getting started',
      collapsible: false,
      items: [
        'getting-started/installation',
        'getting-started/quickstart',
        'getting-started/tutorial',
      ],
    },
    {
      type: 'category',
      label: 'Brokers',
      collapsible: false,
      items: [
        'brokers/index',
        'brokers/memory',
        ...brokerSites.map(([label, repo]) => ({
          type: 'link' as const,
          label,
          href: `https://powersemmi.github.io/${repo}/`,
        })),
      ],
    },
    {type: 'doc', id: 'benchmarks', label: 'Benchmarks'},
    {
      type: 'category',
      label: 'Broker authors',
      collapsible: false,
      items: [
        'broker-authors/index',
        'broker-authors/example-nats',
        'broker-authors/conformance',
        'broker-authors/templates',
      ],
    },
    {type: 'doc', id: 'reference', label: 'API reference'},
  ],
};

export default sidebars;
