import React, {type ReactNode} from 'react';
import Translate from '@docusaurus/Translate';
import SectionHead from './SectionHead';
import shared from './landing.module.css';
import styles from './grid.module.css';
import CodeLink from './CodeLink';

interface Principle {
  title: ReactNode;
  body: ReactNode;
}

const PRINCIPLES: Principle[] = [
  {
    title: (
      <Translate id="landing.principles.agnostic.title">A core with no broker in it</Translate>
    ),
    body: (
      <Translate
        id="landing.principles.agnostic.body"
        values={{suite: <CodeLink api="ruststream/conformance/index.html">conformance</CodeLink>}}>
        {
          'The core is traits and types. Each broker is its own crate, held to the contract by the {suite} suite.'
        }
      </Translate>
    ),
  },
  {
    title: (
      <Translate id="landing.principles.config.title">
        Broker settings stay with the broker
      </Translate>
    ),
    body: (
      <Translate
        id="landing.principles.config.body"
        values={{config: <CodeLink to="/brokers/">Config</CodeLink>}}>
        {
          'Each broker crate owns its {config}. A client upgrade changes that crate, not the framework.'
        }
      </Translate>
    ),
  },
  {
    title: <Translate id="landing.principles.compile.title">Misuse does not compile</Translate>,
    body: (
      <Translate
        id="landing.principles.compile.body"
        values={{
          ack: (
            <CodeLink api="ruststream/trait.IncomingMessage.html#tymethod.ack">ack(self)</CodeLink>
          ),
        }}>
        {
          '{ack} consumes the delivery, so a second ack is a compile error. Out-of-order lifecycle calls fail the same way.'
        }
      </Translate>
    ),
  },
  {
    title: <Translate id="landing.principles.stream.title">Subscribers are streams</Translate>,
    body: (
      <Translate
        id="landing.principles.stream.body"
        values={{stream: <CodeLink api="ruststream/trait.Subscriber.html">Stream</CodeLink>}}>
        {
          'A subscriber is a {stream}, so back-pressure comes with it. The runtime builds handlers on top of it.'
        }
      </Translate>
    ),
  },
  {
    title: (
      <Translate id="landing.principles.generic.title">
        Generic, with no dyn in the contract
      </Translate>
    ),
    body: (
      <Translate
        id="landing.principles.generic.body"
        values={{
          rpitit: <CodeLink api="ruststream/trait.Broker.html">async fn in trait</CodeLink>,
        }}>
        {
          'The contract uses associated types and native {rpitit}. The runtime erases types only where a service needs it.'
        }
      </Translate>
    ),
  },
  {
    title: (
      <Translate id="landing.principles.capabilities.title">
        Capabilities, not a fat interface
      </Translate>
    ),
    body: (
      <Translate id="landing.principles.capabilities.body">
        Batches, transactions, request-reply, partitions and seeking are separate traits. A broker
        implements the ones it supports.
      </Translate>
    ),
  },
];

/** The design commitments, one per cell. */
export default function Principles(): ReactNode {
  return (
    <section className={shared.section} aria-labelledby="landing-principles">
      <SectionHead
        id="landing-principles"
        eyebrow={<Translate id="landing.principles.eyebrow">Design</Translate>}
        title={<Translate id="landing.principles.title">What the framework commits to</Translate>}
      />
      <ul className={styles.grid}>
        {PRINCIPLES.map((principle, index) => (
          <li key={index} className={styles.cell}>
            <span className={styles.index}>{String(index + 1).padStart(2, '0')}</span>
            <h3 className={styles.cellTitle}>{principle.title}</h3>
            <p className={styles.cellBody}>{principle.body}</p>
          </li>
        ))}
      </ul>
    </section>
  );
}
