import {useEffect, useState, type CSSProperties, type ReactNode} from 'react';
import useDocusaurusContext from '@docusaurus/useDocusaurusContext';
import {
  BROKERS,
  CORE,
  SCHEMAS,
  codePanels,
  codeRows,
  comparisonPanels,
  comparisonRows,
  coreFirst,
  detailsUrl,
  pendingLine,
  resultsUrl,
  type Broker,
  type CodeBar,
  type CodePanel,
  type ComparisonPanel,
  type Labels,
  type Lane,
  type Loaded,
  type Loop,
  type Results,
  type Tick,
} from './model';
import styles from './styles.module.css';

/*
 * The two summary charts of the Benchmarks page, each with the table it is drawn from. The
 * numbers are fetched in the reader's browser, not copied into this repository: a crate
 * remeasures on its own schedule, and its document on `main` shows as soon as it is merged.
 * GitHub serves those files to any origin.
 */

const TIMEOUT_MS = 8000;

async function load(broker: Broker): Promise<Results | null> {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), TIMEOUT_MS);
  try {
    const response = await fetch(resultsUrl(broker), {signal: controller.signal});
    if (!response.ok) {
      return null;
    }
    const results = (await response.json()) as Results | null;
    return results && SCHEMAS.includes(results.schema) ? results : null;
  } catch {
    return null;
  } finally {
    clearTimeout(timer);
  }
}

// Both charts read the same documents; one round of fetches serves them for the page's life.
let documents: Promise<Loaded[]> | null = null;
const loadAll = (): Promise<Loaded[]> =>
  (documents ??= Promise.all(
    [...BROKERS, CORE].map(async (broker) => ({broker, results: await load(broker)})),
  ));

function useDocuments(): Loaded[] | null {
  const [loaded, setLoaded] = useState<Loaded[] | null>(null);
  useEffect(() => {
    let current = true;
    void loadAll().then((result) => {
      if (current) {
        setLoaded(result);
      }
    });
    return () => {
      current = false;
    };
  }, []);
  return loaded;
}

function useLang(): string {
  const {i18n} = useDocusaurusContext();
  return i18n.localeConfigs[i18n.currentLocale]?.htmlLang ?? i18n.currentLocale;
}

// Positions arrive as percentages of the plot, so every mark is laid out by CSS alone.
const at = (left: number, right?: number): CSSProperties =>
  right === undefined ? {left: `${left}%`} : {left: `${left}%`, width: `${right - left}%`};

const LOOP_CLASS: Record<Loop, string | undefined> = {
  raw: styles.raw,
  adapter: styles.adapter,
  framework: styles.framework,
};

const LOOPS: Loop[] = ['raw', 'adapter', 'framework'];

function Grid({ticks, zero}: {ticks: Tick[]; zero?: number}): ReactNode {
  return (
    <>
      {ticks.map((tick) => (
        <span key={tick.value} className={styles.gridline} style={at(tick.position)} />
      ))}
      {zero !== undefined && <span className={styles.zero} style={at(zero)} />}
    </>
  );
}

function TickLabels({ticks}: {ticks: Tick[]}): ReactNode {
  return (
    <div className={styles.ticks} aria-hidden="true">
      {ticks.map((tick) => (
        <span key={tick.value} className={styles.tick} style={at(tick.position)}>
          {tick.label}
        </span>
      ))}
    </div>
  );
}

function Swatch({loop}: {loop: Loop}): ReactNode {
  return <span className={`${styles.swatch} ${LOOP_CLASS[loop]}`} aria-hidden="true" />;
}

function LaneMarks({lane}: {lane: Lane}): ReactNode {
  return (
    <div className={`${styles.lane} ${LOOP_CLASS[lane.loop]}`}>
      <span className={styles.range} style={at(lane.from, lane.to)} />
      {lane.median !== undefined && <span className={styles.median} style={at(lane.median)} />}
      <span className={`${styles.dot} ${lane.inside ? styles.hollow : ''}`} style={at(lane.best)} />
    </div>
  );
}

function ComparisonLegend({labels}: {labels: Labels}): ReactNode {
  return (
    <ul className={styles.legend}>
      {LOOPS.map((loop) => (
        <li key={loop}>
          <Swatch loop={loop} />
          {labels[loop]}
        </li>
      ))}
      <li>
        <span className={`${styles.keyRange} ${styles.adapter}`} aria-hidden="true" />
        {labels.rounds}
      </li>
      <li>
        <span className={`${styles.keyDot} ${styles.adapter}`} aria-hidden="true" />
        {labels.best}
      </li>
      <li>
        <span className={`${styles.keyMedian} ${styles.adapter}`} aria-hidden="true" />
        {labels.median}
      </li>
      <li>
        <span className={styles.keyBand} aria-hidden="true" />
        {labels.noise}
      </li>
      <li>
        <span
          className={`${styles.keyDot} ${styles.hollow} ${styles.adapter}`}
          aria-hidden="true"
        />
        {labels.indistinguishable}
      </li>
    </ul>
  );
}

function ComparisonChart({panel, labels}: {panel: ComparisonPanel; labels: Labels}): ReactNode {
  return (
    <section className={styles.panel}>
      <p className={styles.panelHead}>
        <strong>{panel.broker.name}</strong> - {panel.provenance}.{' '}
        <a href={detailsUrl(panel.broker)}>{labels.details}</a>
      </p>
      <div className={`${styles.row} ${styles.axisRow}`}>
        <TickLabels ticks={panel.ticks} />
      </div>
      {panel.rows.map((row) => (
        <div key={row.scenario} className={styles.row} tabIndex={0}>
          <span className={styles.name}>{row.scenario}</span>
          <div className={styles.plot} aria-hidden="true">
            <Grid ticks={panel.ticks} zero={panel.zero} />
            {row.lanes
              .filter((lane) => lane.loop === 'framework' && lane.band)
              .map((lane) => (
                <span
                  key="band"
                  className={styles.band}
                  style={at(lane.band!.from, lane.band!.to)}
                />
              ))}
            {row.lanes.map((lane) => (
              <LaneMarks key={lane.loop} lane={lane} />
            ))}
          </div>
          <ul className={styles.verdicts}>
            {row.lanes
              .filter((lane) => lane.verdict !== undefined)
              .map((lane) => (
                <li key={lane.loop}>
                  <Swatch loop={lane.loop} />
                  {lane.verdict}
                </li>
              ))}
          </ul>
          <dl className={styles.tooltip} aria-hidden="true">
            {row.lanes.map((lane) => (
              <div key={lane.loop}>
                <dt>
                  <Swatch loop={lane.loop} />
                  {labels[lane.loop]}
                </dt>
                <dd>
                  <strong>{lane.value}</strong>
                </dd>
              </div>
            ))}
          </dl>
        </div>
      ))}
    </section>
  );
}

/** Throughput against each broker's raw client, as every broker crate measures it. */
export function BenchmarkResults({labels}: {labels: Labels}): ReactNode {
  const loaded = useDocuments();
  const lang = useLang();
  if (!loaded) {
    return <p>{labels.loading}</p>;
  }
  // The core publishes no comparison, so this chart waits for the broker crates only.
  const brokers = loaded.filter((entry) => entry.broker !== CORE);
  const published = brokers.filter((entry) => entry.results?.scenarios?.length);
  const pending = pendingLine(
    brokers.filter((entry) => !published.includes(entry)),
    labels,
  );
  return (
    <>
      {published.length > 0 && (
        <figure className={styles.chart}>
          <figcaption className={styles.caption}>{labels.axis}</figcaption>
          <ComparisonLegend labels={labels} />
          {comparisonPanels(published, lang, labels).map((panel) => (
            <ComparisonChart key={panel.broker.repo} panel={panel} labels={labels} />
          ))}
          <details className={styles.table}>
            <summary>{labels.table}</summary>
            <table>
              <thead>
                <tr>
                  <th>{labels.broker}</th>
                  <th>{labels.scenario}</th>
                  <th>{labels.raw}</th>
                  <th>{labels.adapter}</th>
                  <th>{labels.framework}</th>
                  <th>{labels.overhead}</th>
                </tr>
              </thead>
              <tbody>
                {comparisonRows(published, lang, labels).map((row, index) => (
                  <tr key={index}>
                    <td>{row.broker}</td>
                    <td>{row.scenario}</td>
                    <td>{row.raw}</td>
                    <td>{row.adapter}</td>
                    <td>{row.framework}</td>
                    <td>{row.overhead}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </details>
        </figure>
      )}
      {pending && <p>{pending}</p>}
    </>
  );
}

function Bar({bar}: {bar: CodeBar}): ReactNode {
  return (
    <div className={styles.measure}>
      <div className={styles.track} aria-hidden="true">
        <span className={styles.bar} style={{width: `${bar.width}%`}} />
      </div>
      <span className={styles.value}>{bar.value}</span>
    </div>
  );
}

function CodeChart({panel, labels}: {panel: CodePanel; labels: Labels}): ReactNode {
  return (
    <section className={styles.codePanel}>
      <p className={styles.panelHead}>
        <strong>{panel.broker.name}</strong> - {panel.machine}
      </p>
      <div className={`${styles.codeRow} ${styles.axisRow}`}>
        {[
          [labels.instructions, panel.instructions] as const,
          [labels.allocations, panel.allocations] as const,
        ].map(([name, ticks]) => (
          <div key={name} className={styles.measure}>
            <div>
              <span className={styles.measureName}>{name}</span>
              <TickLabels ticks={ticks} />
            </div>
          </div>
        ))}
      </div>
      {panel.rows.map((row) => (
        <div key={row.scenario} className={styles.codeRow} tabIndex={0}>
          <span className={styles.name}>{row.scenario}</span>
          <Bar bar={row.instructions} />
          <Bar bar={row.allocations} />
          <dl className={styles.tooltip} aria-hidden="true">
            {[
              [labels.instructions, row.instructions.value],
              [labels.allocations, row.allocations.value],
              [labels.cold, row.cold],
            ].map(([name, value]) => (
              <div key={name}>
                <dt>{name}</dt>
                <dd>
                  <strong>{value}</strong>
                </dd>
              </div>
            ))}
          </dl>
        </div>
      ))}
    </section>
  );
}

/** Instructions and allocations per message, as the core and every broker crate measure them. */
export function BenchmarkCode({labels}: {labels: Labels}): ReactNode {
  const loaded = useDocuments();
  const lang = useLang();
  if (!loaded) {
    return <p>{labels.loading}</p>;
  }
  const published = coreFirst(loaded).filter((entry) => entry.results?.code?.length);
  if (!published.length) {
    return <p>{pendingLine([{broker: CORE, results: null}], labels)}</p>;
  }
  return (
    <figure className={styles.chart}>
      {codePanels(published, lang, labels).map((panel) => (
        <CodeChart key={panel.broker.repo} panel={panel} labels={labels} />
      ))}
      <details className={styles.table}>
        <summary>{labels.table}</summary>
        <table>
          <thead>
            <tr>
              <th>{labels.crate}</th>
              <th>{labels.scenario}</th>
              <th>{labels.instructions}</th>
              <th>{labels.allocations}</th>
              <th>{labels.cold}</th>
            </tr>
          </thead>
          <tbody>
            {codeRows(published, lang).map((row, index) => (
              <tr key={index}>
                <td>{row.crate}</td>
                <td>{row.scenario}</td>
                <td>{row.instructions}</td>
                <td>{row.allocations}</td>
                <td>{row.cold}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </details>
    </figure>
  );
}
