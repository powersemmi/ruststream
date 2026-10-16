import {useEffect, useState, type ReactNode} from 'react';
import useDocusaurusContext from '@docusaurus/useDocusaurusContext';
import {
  BROKERS,
  CORE,
  SCHEMAS,
  codeRows,
  comparisonRows,
  detailsUrl,
  machineLine,
  pendingLine,
  provenanceLine,
  resultsUrl,
  type Broker,
  type Labels,
  type Loaded,
  type Results,
} from './model';

/*
 * The two summary tables of the Benchmarks page. The numbers are fetched in the reader's
 * browser, not copied into this repository: a crate remeasures on its own schedule, and its
 * document on `main` shows as soon as it is merged. GitHub serves those files to any origin.
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

// Both tables read the same documents; one round of fetches serves them for the page's life.
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

/** Throughput against each broker's raw client, as every broker crate measures it. */
export function BenchmarkResults({labels}: {labels: Labels}): ReactNode {
  const loaded = useDocuments();
  const lang = useLang();
  if (!loaded) {
    return <p>{labels.loading}</p>;
  }
  // The core publishes no comparison, so the first table waits for the broker crates only.
  const brokers = loaded.filter((entry) => entry.broker !== CORE);
  const published = brokers.filter((entry) => entry.results?.scenarios?.length);
  const pending = pendingLine(
    brokers.filter((entry) => !published.includes(entry)),
    labels,
  );
  return (
    <>
      {published.length > 0 && (
        <>
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
          <ul>
            {published.map((entry) => (
              <li key={entry.broker.repo}>
                <strong>{entry.broker.name}</strong> - {provenanceLine(entry, labels)}.{' '}
                <a href={detailsUrl(entry.broker)}>{labels.details}</a>
              </li>
            ))}
          </ul>
        </>
      )}
      {pending && <p>{pending}</p>}
    </>
  );
}

/** Instructions and allocations per message, as the core and every broker crate measure them. */
export function BenchmarkCode({labels}: {labels: Labels}): ReactNode {
  const loaded = useDocuments();
  const lang = useLang();
  if (!loaded) {
    return <p>{labels.loading}</p>;
  }
  // The core leads the table: its number is the framework's own code.
  const ordered = [
    ...loaded.filter((entry) => entry.broker === CORE),
    ...loaded.filter((entry) => entry.broker !== CORE),
  ];
  const published = ordered.filter((entry) => entry.results?.code?.length);
  if (!published.length) {
    return <p>{pendingLine([{broker: CORE, results: null}], labels)}</p>;
  }
  return (
    <>
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
      <ul>
        {published.map((entry) => (
          <li key={entry.broker.repo}>
            <strong>{entry.broker.name}</strong> - {machineLine(entry, labels)}
          </li>
        ))}
      </ul>
    </>
  );
}
