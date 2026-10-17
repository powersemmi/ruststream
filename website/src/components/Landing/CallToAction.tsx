import React, {type ReactNode} from 'react';
import Link from '@docusaurus/Link';
import Translate from '@docusaurus/Translate';
import {openAskAi} from '@site/src/components/AskAi';
import Quickstart from './Quickstart';
import {
  BookIcon,
  BracesIcon,
  ConnectionIcon,
  DocumentIcon,
  DownloadIcon,
  FlaskIcon,
  GaugeIcon,
  GlobeIcon,
  RocketIcon,
  ServerIcon,
} from './Icons';
import shared from './landing.module.css';
import grid from './grid.module.css';
import styles from './cta.module.css';
import CodeLink from './CodeLink';

interface Next {
  to: string;
  icon: ReactNode;
  title: ReactNode;
  body: ReactNode;
}

const NEXT: Next[] = [
  {
    to: '/guides/getting-started/installation/',
    icon: <DownloadIcon />,
    title: <Translate id="landing.next.installation">Installation</Translate>,
    body: <Translate id="landing.next.installation.body">Features and crate setup.</Translate>,
  },
  {
    to: '/guides/getting-started/quickstart/',
    icon: <RocketIcon />,
    title: <Translate id="landing.next.quickstart">Quick start</Translate>,
    body: (
      <Translate
        id="landing.next.quickstart.body"
        // The whole card is a link to the quick start, so the term stays plain code inside it.
        values={{command: <code>cargo generate</code>}}>
        {'Scaffold a service with {command}.'}
      </Translate>
    ),
  },
  {
    to: '/guides/getting-started/tutorial/',
    icon: <BookIcon />,
    title: <Translate id="landing.next.tutorial">Tutorial</Translate>,
    body: <Translate id="landing.next.tutorial.body">Build a service step by step.</Translate>,
  },
  {
    to: '/brokers/',
    icon: <ConnectionIcon />,
    title: <Translate id="landing.next.brokers">Brokers</Translate>,
    body: (
      <Translate id="landing.next.brokers.body">
        The in-memory broker and the broker crates.
      </Translate>
    ),
  },
  {
    to: 'https://docs.rs/ruststream/latest/ruststream/trait.Broker.html',
    icon: <ServerIcon />,
    title: <Translate id="landing.next.brokerAuthors">Broker authors</Translate>,
    body: (
      <Translate id="landing.next.brokerAuthors.body">
        Implement the contract and pass conformance.
      </Translate>
    ),
  },
  {
    to: 'https://docs.rs/ruststream/latest/ruststream/testing/index.html',
    icon: <FlaskIcon />,
    title: <Translate id="landing.next.testing">Testing</Translate>,
    body: (
      <Translate id="landing.next.testing.body">
        Test handlers in process, with no server.
      </Translate>
    ),
  },
  {
    to: 'https://docs.rs/ruststream/latest/ruststream/runtime/index.html#running-beside-another-server',
    icon: <GlobeIcon />,
    title: <Translate id="landing.next.http">HTTP frameworks</Translate>,
    body: (
      <Translate id="landing.next.http.body">
        Run beside axum, with a transactional outbox.
      </Translate>
    ),
  },
  {
    to: 'https://docs.rs/ruststream/latest/ruststream/asyncapi/index.html',
    icon: <DocumentIcon />,
    title: 'AsyncAPI',
    body: (
      <Translate id="landing.next.asyncapi.body">
        The document a service generates from its own declarations.
      </Translate>
    ),
  },
  {
    to: 'https://docs.rs/ruststream/latest/ruststream/',
    icon: <BracesIcon />,
    title: <Translate id="landing.next.reference">API reference</Translate>,
    body: <Translate id="landing.next.reference.body">Every type, on docs.rs.</Translate>,
  },
  {
    to: '/architecture/benchmarks/',
    icon: <GaugeIcon />,
    title: <Translate id="landing.next.benchmarks">Benchmarks</Translate>,
    body: (
      <Translate id="landing.next.benchmarks.body">
        The cost of the code, and each broker against its raw client.
      </Translate>
    ),
  },
];

/** The closing section: start here, or ask a question. */
export default function CallToAction(): ReactNode {
  return (
    <section className={`${shared.section} ${styles.cta}`} aria-labelledby="landing-cta">
      <div className={styles.head}>
        <h2 id="landing-cta" className={styles.title}>
          <Translate
            id="landing.cta.title"
            values={{
              accent: (
                <span className={shared.accentWord}>
                  <Translate id="landing.cta.titleAccent">one handler</Translate>
                </span>
              ),
            }}>
            {'Start with {accent}'}
          </Translate>
        </h2>
        <p className={shared.lede}>
          <Translate
            id="landing.cta.lede"
            values={{
              command: <CodeLink to="/components/application/cli/">cargo generate</CodeLink>,
            }}>
            {'Scaffold a service with {command} in a minute, or ask the documentation a question.'}
          </Translate>
        </p>
        <div className={`${shared.actions} ${styles.actions}`}>
          <Link className={shared.button} to="/guides/getting-started/quickstart/">
            <Translate id="landing.hero.quickStart">Quick start</Translate>
          </Link>
          <button type="button" className={shared.buttonGhost} onClick={() => openAskAi()}>
            <Translate id="askAi.button">Ask AI</Translate>
          </button>
        </div>
      </div>
      <Quickstart />
      <ul className={`${grid.grid} ${grid.four}`}>
        {NEXT.map((next) => (
          <li key={next.to} className={grid.cellWrap}>
            <Link to={next.to} className={`${grid.cell} ${grid.cellLink}`}>
              <span className={grid.icon}>{next.icon}</span>
              <h3 className={grid.cellTitle}>{next.title}</h3>
              <p className={grid.cellBody}>{next.body}</p>
            </Link>
          </li>
        ))}
      </ul>
    </section>
  );
}
