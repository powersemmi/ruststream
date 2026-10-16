import React, {type ReactNode} from 'react';
import Link from '@docusaurus/Link';
import Translate, {translate} from '@docusaurus/Translate';
import FlowCanvas from './FlowCanvas';
import shared from './landing.module.css';
import styles from './hero.module.css';

export default function Hero(): ReactNode {
  return (
    <header className={styles.hero}>
      <div>
        <span className={styles.badge}>
          <span className={styles.badgeDot} aria-hidden="true" />
          <Translate id="landing.hero.badge">Open source · Apache-2.0 · 100% safe Rust</Translate>
        </span>
        <h1 className={styles.headline}>
          <Translate
            id="landing.hero.title"
            values={{
              accent: (
                <span className={shared.accentWord}>
                  <Translate id="landing.hero.titleAccent">any broker</Translate>
                </span>
              ),
            }}>
            {'Async messaging for Rust, on {accent}'}
          </Translate>
        </h1>
        <p className={styles.subtitle}>
          <Translate id="landing.hero.subtitle" values={{subscriber: <code>#[subscriber]</code>}}>
            {
              'Write {subscriber} handlers once, run them on Kafka, NATS, RabbitMQ and ten more brokers, and let the compiler catch misuse before it ships.'
            }
          </Translate>
        </p>
        <div className={shared.actions}>
          <Link className={shared.button} to="/guides/getting-started/quickstart/">
            <Translate id="landing.hero.quickStart">Quick start</Translate>
          </Link>
          <Link className={shared.buttonGhost} href="https://github.com/powersemmi/ruststream">
            GitHub
          </Link>
        </div>
      </div>
      <div className={styles.graphic}>
        <FlowCanvas
          label={translate({
            id: 'landing.hero.canvasLabel',
            message:
              'An animation of messages leaving the brokers, passing through the RustStream core and reaching the handlers.',
          })}
        />
        <div className={styles.legend} aria-hidden="true">
          <span>
            <Translate id="landing.hero.legendBrokers">Brokers</Translate>
          </span>
          <span>RustStream</span>
          <span>
            <Translate id="landing.hero.legendHandlers">Handlers</Translate>
          </span>
        </div>
      </div>
    </header>
  );
}
