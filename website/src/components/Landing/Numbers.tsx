import React, {type ReactNode} from 'react';
import Link from '@docusaurus/Link';
import Translate, {translate} from '@docusaurus/Translate';
import useDocusaurusContext from '@docusaurus/useDocusaurusContext';
import results from '../../../../docs/benchmarks/results.json';
import SectionHead from './SectionHead';
import {selectNumbers, type HeadlineScenario, type Results} from './numbers';
import shared from './landing.module.css';
import styles from './grid.module.css';

function scenarioLabel(scenario: HeadlineScenario): string {
  switch (scenario) {
    case 'consume, JSON decode into a small struct':
      return translate({
        id: 'landing.numbers.json',
        message: 'Consume a message and decode its JSON into a struct',
      });
    case 'consume on the byte lane, no codec':
      return translate({id: 'landing.numbers.bytes', message: 'Consume the raw bytes, no codec'});
    case 'consume through a middleware stack of four':
      return translate({
        id: 'landing.numbers.middleware',
        message: 'Consume through four middleware layers',
      });
    case 'reply to a transport that reads the payload':
      return translate({
        id: 'landing.numbers.reply',
        message: 'Encode and publish a reply',
      });
  }
}

/** Instructions and allocations per message, from the core's published benchmark results. */
export default function Numbers(): ReactNode {
  const {i18n} = useDocusaurusContext();
  const numbers = selectNumbers(
    results as Results,
    i18n.localeConfigs[i18n.currentLocale]?.htmlLang ?? 'en',
  );
  return (
    <section className={shared.section} aria-labelledby="landing-numbers">
      <SectionHead
        id="landing-numbers"
        eyebrow={<Translate id="landing.numbers.eyebrow">Cost per message</Translate>}
        title={
          <Translate
            id="landing.numbers.title"
            values={{
              accent: (
                <span className={shared.accentWord}>
                  <Translate id="landing.numbers.titleAccent">Zero allocations</Translate>
                </span>
              ),
            }}>
            {'{accent} per message'}
          </Translate>
        }
        lede={
          <Translate id="landing.numbers.lede">
            The framework's own code per message in the steady state, counted by the benchmarks on
            every change. The broker's client is not in these numbers.
          </Translate>
        }
      />
      <ul className={`${styles.grid} ${styles.four}`}>
        {numbers.rows.map((row) => (
          <li key={row.scenario} className={styles.cell}>
            <span className={styles.big}>{row.instructions}</span>
            <span className={styles.unit}>
              <Translate id="landing.numbers.instructions">instructions</Translate>
            </span>
            <span className={`${styles.unit} ${styles.zero}`}>
              <Translate id="landing.numbers.allocations" values={{count: row.allocations}}>
                {'{count} allocations'}
              </Translate>
            </span>
            <p className={styles.cellBody}>{scenarioLabel(row.scenario)}</p>
          </li>
        ))}
      </ul>
      <p className={styles.note}>
        <Translate
          id="landing.numbers.note"
          values={{
            version: <code>ruststream {numbers.version}</code>,
            date: numbers.measuredAt,
            cpu: numbers.cpu,
            link: (
              <Link to="/architecture/benchmarks/">
                <Translate id="landing.numbers.link">every scenario and the method</Translate>
              </Link>
            ),
          }}>
          {
            'Measured on {version}, {date}, {cpu}. The benchmarks page has {link}, including the paths that do allocate.'
          }
        </Translate>
      </p>
    </section>
  );
}
