import React, {type ReactNode} from 'react';
import Translate, {translate} from '@docusaurus/Translate';
import {BROKERS, MEMORY_BROKER, type Broker} from './brokers';
import styles from './brokers.module.css';

// Names that are words rather than product names read in the reader's language.
function brokerName(broker: Broker): string {
  switch (broker.id) {
    case 'files':
      return translate({id: 'landing.brokers.files', message: 'Files / stdio'});
    case 'sql':
      return translate({id: 'landing.brokers.sql', message: 'SQL databases'});
    case 'memory':
      return translate({id: 'landing.brokers.memory', message: 'In-memory'});
    default:
      return broker.name;
  }
}

function BrokerCell({broker}: {broker: Broker}): ReactNode {
  const {Icon} = broker;
  return (
    <a className={styles.cell} href={broker.href}>
      <Icon className={styles.icon} aria-hidden="true" focusable="false" />
      <span className={styles.name}>{brokerName(broker)}</span>
      <span className={styles.crate}>{broker.crate}</span>
    </a>
  );
}

/** The brokers RustStream runs on, each linking to its crate's documentation. */
export default function BrokerStrip(): ReactNode {
  return (
    <section className={styles.strip} aria-labelledby="landing-brokers">
      <h2 id="landing-brokers" className={styles.label}>
        <Translate id="landing.brokers.label">One handler, fourteen transports</Translate>
      </h2>
      {[...BROKERS, MEMORY_BROKER].map((broker) => (
        <BrokerCell key={broker.id} broker={broker} />
      ))}
    </section>
  );
}
