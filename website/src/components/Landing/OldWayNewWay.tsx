import React, {type ReactNode, useEffect, useRef, useState} from 'react';
import CodeBlock from '@theme/CodeBlock';
import Translate, {translate} from '@docusaurus/Translate';
import SectionHead from './SectionHead';
import Terminal, {type TerminalLine} from './Terminal';
import {RAW_LOOP, RUSTSTREAM_SERVICE} from './snippets';
import shared from './landing.module.css';
import styles from './compare.module.css';

// A run of the `kafka_quickstart` example against a local Kafka, with the `logging` feature on;
// timestamps cut to the time of day. The order was published from a second terminal.
const RUN: TerminalLine[] = [
  {kind: 'command', text: 'cargo run -- run'},
  {
    kind: 'log',
    text: '04:24:54.781 INFO ruststream::lifecycle starting service service=orders version=0.1.0 brokers=1 subscribers=1',
  },
  {
    kind: 'log',
    text: '04:24:54.886 INFO ruststream::lifecycle broker connected broker="ruststream_rdkafka::broker::KafkaBroker"',
  },
  {
    kind: 'log',
    text: '04:24:57.902 INFO ruststream::dispatch subscriber started subscriber=orders input="kafka_quickstart::Order"',
  },
  {kind: 'log', text: '04:24:57.902 INFO ruststream::lifecycle service running subscribers=1'},
  {kind: 'note', text: '# {"id":1} is published to orders'},
  {kind: 'output', text: 'got order 1'},
  {kind: 'log', text: '04:25:03.779 INFO ruststream::lifecycle shutdown signal received'},
  {kind: 'log', text: '04:25:03.886 INFO ruststream::lifecycle service stopped'},
];

type Stroke = 'drawn' | 'waiting' | 'drawing';

// The cross over the raw loop draws itself when the panel comes into view. Before the script
// runs, and under reduced motion, it is simply there.
function useStroke(): [React.RefObject<HTMLDivElement | null>, Stroke] {
  const ref = useRef<HTMLDivElement>(null);
  const [stroke, setStroke] = useState<Stroke>('drawn');
  useEffect(() => {
    const element = ref.current;
    if (!element || window.matchMedia('(prefers-reduced-motion: reduce)').matches) {
      return undefined;
    }
    setStroke('waiting');
    const observer = new IntersectionObserver(
      ([entry]) => {
        if (entry?.isIntersecting) {
          setStroke('drawing');
          observer.disconnect();
        }
      },
      {threshold: 0.35},
    );
    observer.observe(element);
    return () => observer.disconnect();
  }, []);
  return [ref, stroke];
}

function Chores({items, mark}: {items: ReactNode[]; mark: 'cross' | 'check'}): ReactNode {
  return (
    <ul className={styles.chores}>
      {items.map((item, index) => (
        <li key={index} className={styles[mark]}>
          {item}
        </li>
      ))}
    </ul>
  );
}

export default function OldWayNewWay(): ReactNode {
  const [panel, stroke] = useStroke();
  return (
    <section className={shared.section} aria-labelledby="landing-compare">
      <SectionHead
        id="landing-compare"
        eyebrow={<Translate id="landing.compare.eyebrow">The same consumer, twice</Translate>}
        title={
          <Translate
            id="landing.compare.title"
            values={{
              accent: (
                <span className={shared.accentWord}>
                  <Translate id="landing.compare.titleAccent">the handler</Translate>
                </span>
              ),
            }}>
            {'Write {accent}, not the loop'}
          </Translate>
        }
        lede={
          <Translate id="landing.compare.lede">
            Both programs read orders from Kafka, decode them and print the id. On the left the
            client is driven by hand; on the right the runtime drives it.
          </Translate>
        }
      />
      <div className={styles.labels} aria-hidden="true">
        <span className={styles.oldLabel}>
          <Translate id="landing.compare.oldLabel">The raw loop</Translate>
        </span>
        <span className={styles.arrow} />
        <span className={styles.newLabel}>
          <Translate id="landing.compare.newLabel">The RustStream way</Translate>
        </span>
      </div>
      <div className={styles.grid}>
        <div className={styles.column}>
          <div ref={panel} className={`${shared.window} ${styles.old}`} data-stroke={stroke}>
            <div className={shared.windowBar}>
              <span className={shared.dots} aria-hidden="true">
                <span />
                <span />
                <span />
              </span>
              <span>src/main.rs · rdkafka</span>
              <span className={`${shared.windowTag} ${styles.danger}`}>
                <Translate id="landing.compare.byHand">By hand</Translate>
              </span>
            </div>
            <div className={styles.oldCode}>
              <CodeBlock language="rust">{RAW_LOOP}</CodeBlock>
            </div>
            <svg
              className={styles.scribble}
              viewBox="0 0 100 100"
              preserveAspectRatio="none"
              aria-hidden="true">
              <path
                pathLength={1}
                d="M22 12 C 40 20, 60 8, 78 18 L 24 40 C 45 36, 60 44, 80 46 L 20 70 C 40 66, 62 74, 82 72 L 30 92"
              />
            </svg>
          </div>
          <Chores
            mark="cross"
            items={[
              <Translate key="config" id="landing.compare.old.config">
                Client settings and commit mode, by hand
              </Translate>,
              <Translate key="decode" id="landing.compare.old.decode">
                Decoding and every error branch, by hand
              </Translate>,
              <Translate key="shutdown" id="landing.compare.old.shutdown">
                Signals and shutdown, by hand
              </Translate>,
              <Translate key="again" id="landing.compare.old.again">
                All of it again for the next topic and the next broker
              </Translate>,
            ]}
          />
        </div>
        <div className={styles.column}>
          <div className={shared.window}>
            <div className={shared.windowBar}>
              <span className={shared.dots} aria-hidden="true">
                <span />
                <span />
                <span />
              </span>
              <span>src/main.rs · ruststream-rdkafka</span>
            </div>
            <CodeBlock language="rust">{RUSTSTREAM_SERVICE}</CodeBlock>
          </div>
          <Terminal
            title={translate({id: 'landing.compare.terminal', message: 'terminal'})}
            lines={RUN}
          />
          <p className={styles.caption}>
            <Translate
              id="landing.compare.caption"
              values={{example: <code>kafka_quickstart</code>, feature: <code>logging</code>}}>
              {
                'A real run of the {example} example against a local Kafka, with the {feature} feature on.'
              }
            </Translate>
          </p>
          <Chores
            mark="check"
            items={[
              <Translate key="decode" id="landing.compare.new.decode">
                Decodes each payload with the codec the build selects
              </Translate>,
              <Translate key="settle" id="landing.compare.new.settle">
                Settles each message by the outcome the handler returns
              </Translate>,
              <Translate key="signals" id="landing.compare.new.signals">
                Stops on Ctrl+C or SIGTERM and closes the broker
              </Translate>,
              <Translate
                key="docs"
                id="landing.compare.new.docs"
                values={{command: <code>asyncapi gen</code>, harness: <code>TestApp</code>}}>
                {
                  'Prints its AsyncAPI document with {command} and runs under {harness} with no Kafka'
                }
              </Translate>,
            ]}
          />
        </div>
      </div>
    </section>
  );
}
