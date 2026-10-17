import {useEffect, useState, type CSSProperties, type ReactNode} from 'react';
import {useHistory, useLocation} from '@docusaurus/router';
import useDocusaurusContext from '@docusaurus/useDocusaurusContext';
import TabItem from '@theme/TabItem';
import Tabs from '@theme/Tabs';
import {movedSection} from './anchors';
import {
  BROKERS,
  CORE,
  CRATE_TABS,
  SCHEMAS,
  detailsUrl,
  resultsUrl,
  tabContent,
  type Broker,
  type CodeBar,
  type CodePanel,
  type CodeRow,
  type ComparisonGroup,
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
 * The results of the Benchmarks page: a tab per crate, each with its two charts and the tables
 * they are drawn from. The numbers are fetched in the reader's browser, not copied into this
 * repository: a crate remeasures on its own schedule, and its document on `main` shows as soon
 * as it is merged. GitHub serves those files to any origin.
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

function ComparisonChart({
  panel,
  labels,
  provenance,
}: {
  panel: ComparisonPanel;
  labels: Labels;
  provenance: boolean;
}): ReactNode {
  return (
    <section className={styles.panel}>
      {provenance && (
        <p className={styles.panelHead}>
          {panel.provenance}. <a href={detailsUrl(panel.broker)}>{labels.details}</a>
        </p>
      )}
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

/**
 * Throughput against the raw loop, as the crate measures it: one chart per group, so a plugin
 * whose overhead is on another scale gets an axis of its own. The run is named once, above the
 * first chart.
 */
function Comparison({
  group: {title, labels, panel, rows},
  provenance,
}: {
  group: ComparisonGroup;
  provenance: boolean;
}): ReactNode {
  return (
    <figure className={styles.chart}>
      <figcaption className={styles.caption}>
        <strong>{title}</strong>
        <span>{labels.axis}</span>
      </figcaption>
      <ComparisonLegend labels={labels} />
      <ComparisonChart panel={panel} labels={labels} provenance={provenance} />
      <details className={styles.table}>
        <summary>{labels.table}</summary>
        <table>
          <thead>
            <tr>
              <th>{labels.scenario}</th>
              <th>{labels.raw}</th>
              <th>{labels.adapter}</th>
              <th>{labels.framework}</th>
              <th>{labels.overhead}</th>
            </tr>
          </thead>
          <tbody>
            {rows.map((row) => (
              <tr key={row.scenario}>
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
      <p className={styles.panelHead}>{panel.machine}</p>
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

/** Instructions and allocations per message, as the crate measures them. */
function Code({
  panel,
  rows,
  labels,
}: {
  panel: CodePanel;
  rows: CodeRow[];
  labels: Labels;
}): ReactNode {
  return (
    <figure className={styles.chart}>
      <figcaption className={styles.caption}>
        <strong>{labels.code}</strong>
      </figcaption>
      <CodeChart panel={panel} labels={labels} />
      <details className={styles.table}>
        <summary>{labels.table}</summary>
        <table>
          <thead>
            <tr>
              <th>{labels.scenario}</th>
              <th>{labels.instructions}</th>
              <th>{labels.allocations}</th>
              <th>{labels.cold}</th>
            </tr>
          </thead>
          <tbody>
            {rows.map((row) => (
              <tr key={row.scenario}>
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

function CrateResults({
  entry,
  labels,
  lang,
}: {
  entry: Loaded | undefined;
  labels: Labels;
  lang: string;
}): ReactNode {
  if (!entry) {
    return <p>{labels.loading}</p>;
  }
  const {comparison, code, pending} = tabContent(entry, lang, labels);
  return (
    <>
      {comparison.map((group, index) => (
        <Comparison key={group.title} group={group} provenance={index === 0} />
      ))}
      {code && <Code {...code} labels={labels} />}
      {pending && <p>{pending}</p>}
    </>
  );
}

/**
 * The results, a tab per crate with the core first. The selected tab is in the address
 * (`?crate=nats`) and is remembered for the next visit; every tab is in the served page.
 */
export function BenchmarkTabs({labels}: {labels: Labels}): ReactNode {
  const loaded = useDocuments();
  const lang = useLang();
  return (
    <div className={styles.results}>
      <Tabs queryString="crate" groupId="benchmark-crate" className={styles.tabs}>
        {CRATE_TABS.map(({value, broker}) => (
          <TabItem key={value} value={value} label={broker.name}>
            <CrateResults
              entry={loaded?.find((candidate) => candidate.broker === broker)}
              labels={labels}
              lang={lang}
            />
          </TabItem>
        ))}
      </Tabs>
    </div>
  );
}

/** Sends a link to a section that left the Benchmarks page on to the page that took it over. */
export function MovedSections(): null {
  const history = useHistory();
  const {pathname, hash} = useLocation();
  useEffect(() => {
    const target = movedSection(hash);
    if (target) {
      history.replace(`${pathname.replace(/\/?$/, '/')}${target}`);
    }
  }, [history, pathname, hash]);
  return null;
}
