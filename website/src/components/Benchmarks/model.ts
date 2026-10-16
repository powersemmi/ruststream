/*
 * The rules the Benchmarks page reads a results document by. Each crate publishes its own
 * document; these functions turn one into the text of a table cell or a line, so the
 * components only lay them out. Prose is never written here: every word comes from the labels
 * the page passes, so a translated page controls its own wording.
 */

export interface Broker {
  name: string;
  repo: string;
}

/** The words of the page, keyed by role. */
export type Labels = Record<
  | 'loading'
  | 'broker'
  | 'scenario'
  | 'raw'
  | 'adapter'
  | 'framework'
  | 'overhead'
  | 'against'
  | 'indistinguishable'
  | 'brokerBound'
  | 'measured'
  | 'details'
  | 'pending'
  | 'crate'
  | 'instructions'
  | 'allocations'
  | 'cold'
  | 'table'
  | 'axis'
  | 'rounds'
  | 'best'
  | 'median'
  | 'noise',
  string
>;

/** One loop: schema 3 carries three rounds, schema 1 a median with its range. */
export interface Measurement {
  best?: number;
  median?: number;
  worst?: number;
  min?: number;
  max?: number;
}

export interface Scenario {
  name: string;
  unit: string;
  raw?: Measurement;
  adapter?: Measurement;
  framework?: Measurement;
  overhead_percent?: number;
  adapter_overhead_percent?: number;
  verdict?: string;
  adapter_verdict?: string;
  broker_bound?: boolean;
}

export interface CodeScenario {
  name: string;
  framework: {instructions?: number; allocations?: number};
  cold?: {instructions?: number; allocations?: number};
}

interface Run {
  crate_version?: string;
  core_version?: string;
  measured_at?: string;
}

export interface Results extends Run {
  schema: number;
  crate: string;
  environment?: Record<string, string | undefined>;
  scenarios?: Scenario[];
  code?: CodeScenario[];
  code_measured?: Run;
}

export interface Loaded {
  broker: Broker;
  results: Results | null;
}

// The catalog, not the results: which repositories to ask. A broker whose document cannot be
// read for any reason is listed as pending rather than dropped, so a broken publish is visible.
export const BROKERS: Broker[] = [
  {name: 'NATS', repo: 'ruststream-nats'},
  {name: 'Redis', repo: 'ruststream-fred'},
  {name: 'RabbitMQ', repo: 'ruststream-lapin'},
  {name: 'Kafka', repo: 'ruststream-rdkafka'},
  {name: 'AMQP 1.0', repo: 'ruststream-amqp'},
  {name: 'Google Cloud Pub/Sub', repo: 'ruststream-gcp-pubsub'},
  {name: 'AWS SQS / SNS', repo: 'ruststream-sqs-sns'},
  {name: 'Apache Pulsar', repo: 'ruststream-pulsar'},
  {name: 'MQTT 5', repo: 'ruststream-rumqttc'},
  {name: 'ZeroMQ', repo: 'ruststream-zeromq'},
  {name: 'Stream files / stdio', repo: 'ruststream-sea-file'},
  {name: 'AWS Kinesis', repo: 'ruststream-kinesis'},
  {name: 'SQL databases', repo: 'ruststream-sqlx'},
];

// The core measures no broker of its own, so it appears in the code table only.
export const CORE: Broker = {name: 'RustStream', repo: 'ruststream'};

const REPO = 'https://github.com/powersemmi/';
const PAGE = '/blob/main/docs/benchmarks.md';
// Schema 2 added the `code` section and schema 3 reports each loop as three rounds. A future
// revision may reorder or retype fields, and rendering it as one of these would print wrong
// numbers rather than none.
export const SCHEMAS = [1, 2, 3];
// What a cell shows when the document does not carry that measurement at all.
const EMPTY = '-';

/** Where a crate's results document is read from: its repository's `main` branch. */
export const resultsUrl = (broker: Broker): string =>
  `https://raw.githubusercontent.com/powersemmi/${broker.repo}/main/docs/benchmarks/results.json`;

export const detailsUrl = (broker: Broker): string => `${REPO}${broker.repo}${PAGE}`;

const number = (value: number | undefined, lang: string): string =>
  typeof value === 'number' ? value.toLocaleString(lang, {maximumFractionDigits: 1}) : EMPTY;

export function sideCell(measurement: Measurement | undefined, unit: string, lang: string): string {
  if (!measurement) {
    return EMPTY;
  }
  if (typeof measurement.best === 'number') {
    const best = `${number(measurement.best, lang)} ${unit}`;
    // The parenthesis is the typical round: the median where the document carries one, else
    // the worst round, which is what such a document printed before the field existed.
    const typical = typeof measurement.median === 'number' ? measurement.median : measurement.worst;
    return typeof typical === 'number' ? `${best} (${number(typical, lang)})` : best;
  }
  const median = `${number(measurement.median, lang)} ${unit}`;
  if (typeof measurement.min !== 'number' || typeof measurement.max !== 'number') {
    return median;
  }
  return `${median} (${number(measurement.min, lang)}-${number(measurement.max, lang)})`;
}

const signed = (percent: number): string => `${percent >= 0 ? '+' : ''}${percent}%`;

export function overheadCell(scenario: Scenario, labels: Labels): string {
  // A difference smaller than the run-to-run spread is a verdict, never a percentage.
  let value =
    scenario.verdict === 'indistinguishable'
      ? labels.indistinguishable
      : signed(scenario.overhead_percent ?? 0);
  if (scenario.broker_bound) {
    value += ` (${labels.brokerBound})`;
  }
  return value;
}

// The figure a comparison is drawn from: the best round, or a first-schema median.
const figure = (measurement: Measurement | undefined): number | undefined =>
  typeof measurement?.best === 'number' ? measurement.best : measurement?.median;

function spread(measurement: Measurement | undefined): number | null {
  if (typeof measurement?.best === 'number' && typeof measurement.worst === 'number') {
    return measurement.best - measurement.worst;
  }
  if (typeof measurement?.min !== 'number' || typeof measurement.max !== 'number') {
    return null;
  }
  return measurement.max - measurement.min;
}

// For a document that left `adapter_verdict` out: the difference against the wider spread of
// the two loops it is drawn from.
function indistinguishable(scenario: Scenario): boolean {
  if (scenario.adapter_verdict) {
    return false;
  }
  const ours = spread(scenario.adapter);
  const theirs = spread(scenario.raw);
  if (ours === null || theirs === null) {
    return false;
  }
  return Math.abs(figure(scenario.raw)! - figure(scenario.adapter)!) < Math.max(ours, theirs);
}

/** What the crate costs over the client it wraps, as the document's verdict states it. */
export function adapterVerdict(scenario: Scenario, labels: Labels): string {
  // A crate that measured only the two ends has no middle loop to judge.
  if (!scenario.adapter) {
    return EMPTY;
  }
  // The overhead column's rule, applied here too, so the two loops of a row never disagree
  // about what is visible.
  if (scenario.adapter_verdict === 'indistinguishable' || indistinguishable(scenario)) {
    return labels.indistinguishable;
  }
  if (typeof scenario.adapter_overhead_percent === 'number') {
    return signed(scenario.adapter_overhead_percent);
  }
  return EMPTY;
}

export function adapterCell(scenario: Scenario, lang: string, labels: Labels): string {
  if (!scenario.adapter) {
    return EMPTY;
  }
  const value = sideCell(scenario.adapter, scenario.unit, lang);
  const verdict = adapterVerdict(scenario, labels);
  return verdict === EMPTY ? value : `${value} ${labels.against.replace('{percent}', verdict)}`;
}

export interface ComparisonRow {
  broker: string;
  scenario: string;
  raw: string;
  adapter: string;
  framework: string;
  overhead: string;
}

export function comparisonRows(published: Loaded[], lang: string, labels: Labels): ComparisonRow[] {
  return published.flatMap(({broker, results}) =>
    (results?.scenarios ?? []).map((scenario, index) => ({
      // The broker names one row however many scenarios it measured.
      broker: index === 0 ? broker.name : '',
      scenario: scenario.name,
      raw: sideCell(scenario.raw, scenario.unit, lang),
      adapter: adapterCell(scenario, lang, labels),
      framework: sideCell(scenario.framework, scenario.unit, lang),
      overhead: overheadCell(scenario, labels),
    })),
  );
}

export interface CodeRow {
  crate: string;
  scenario: string;
  instructions: string;
  allocations: string;
  cold: string;
}

export function codeRows(published: Loaded[], lang: string): CodeRow[] {
  return published.flatMap(({broker, results}) =>
    (results?.code ?? []).map((scenario, index) => ({
      crate: index === 0 ? broker.name : '',
      scenario: scenario.name,
      instructions: number(scenario.framework.instructions, lang),
      allocations: number(scenario.framework.allocations, lang),
      cold: coldCell(scenario, lang),
    })),
  );
}

// Two numbers in one cell: what starting cost in instructions, and in allocations.
const coldCell = (scenario: CodeScenario, lang: string): string =>
  scenario.cold
    ? `${number(scenario.cold.instructions, lang)} / ${number(scenario.cold.allocations, lang)}`
    : EMPTY;

/** The core first, its number being the framework's own code, then the catalog order. */
export const coreFirst = (loaded: Loaded[]): Loaded[] => [
  ...loaded.filter((entry) => entry.broker === CORE),
  ...loaded.filter((entry) => entry.broker !== CORE),
];

/** What a comparison was measured on, after the broker's name. */
export function provenanceLine({results}: Loaded, labels: Labels): string {
  const environment = results?.environment ?? {};
  return [
    `${results?.crate} ${results?.crate_version} on ruststream ${results?.core_version}`,
    environment.cpu,
    environment.os,
    environment.broker,
    `${labels.measured} ${results?.measured_at}`,
  ]
    .filter(Boolean)
    .join(', ');
}

// The machine a set of code numbers was taken on, in the order the fields are documented.
const MACHINE = [
  'cpu',
  'architecture',
  'cpu_frequency',
  'cores',
  'memory',
  'memory_speed',
  'os',
  'rustc',
  'valgrind',
  'profile',
  'features',
];

/** The machine and the code a set of code numbers counted, after the crate's name. */
export function machineLine({broker, results}: Loaded, labels: Labels): string {
  const environment = results?.environment ?? {};
  // A crate that measures its code apart from the comparison dates that run in
  // `code_measured`; the top-level date and versions then belong to the comparison.
  const run: Run = results?.code_measured ?? results ?? {};
  const parts = MACHINE.map((field) => environment[field]).filter(Boolean);
  // Rows taken on different releases do not compare. The core's own row names one release:
  // the crate it measures is the core.
  const versions = [
    run.crate_version && `${results?.crate} ${run.crate_version}`,
    broker !== CORE && run.core_version && `ruststream ${run.core_version}`,
  ].filter(Boolean);
  return `${parts.join(', ')}. ${[...versions, `${labels.measured} ${run.measured_at}`].join(', ')}.`;
}

/** The crates with no readable document, or `null` when every one published. */
export function pendingLine(loaded: Loaded[], labels: Labels): string | null {
  if (!loaded.length) {
    return null;
  }
  // A template rather than a prefix: spacing and punctuation around the list differ by language.
  return labels.pending.replace('{brokers}', loaded.map((entry) => entry.broker.name).join(', '));
}

/*
 * The geometry of the two charts. Every position is a percentage of the plot's width, so the
 * components lay marks out with CSS alone and the page draws the same at any width.
 */

export interface Tick {
  value: number;
  label: string;
  position: number;
}

export interface Axis {
  ticks: Tick[];
  place(value: number): number;
}

// A step of one, two or five times a power of ten reads as a round number in every language.
const STEPS = [1, 2, 5, 10];

/** A linear axis over `low..high`, widened to round ticks at both ends. */
export function niceAxis(
  low: number,
  high: number,
  label: (value: number) => string,
  count = 5,
): Axis {
  // A set of equal values still needs a width to be drawn against.
  const top = high > low ? high : low + 1;
  const rough = (top - low) / (count - 1);
  const power = 10 ** Math.floor(Math.log10(rough));
  const step = STEPS.map((factor) => factor * power).find((candidate) => candidate >= rough)!;
  const min = Math.floor(low / step) * step;
  const max = Math.ceil(top / step) * step;
  const place = (value: number): number => ((value - min) / (max - min)) * 100;
  const ticks: Tick[] = [];
  for (let index = 0; min + index * step <= max + step / 2; index += 1) {
    // Repeated addition of a fractional step drifts; the tick is the round number it means.
    const value = Number((min + index * step).toPrecision(12));
    ticks.push({value, label: label(value), position: place(value)});
  }
  return {ticks, place};
}

export type Loop = 'raw' | 'adapter' | 'framework';

/** One loop of one scenario: where its rounds fall, and what the document concluded. */
export interface Lane {
  loop: Loop;
  // The rounds from the best to the slowest, then the round the comparison is drawn from.
  from: number;
  to: number;
  best: number;
  median?: number;
  // The spread a verdict rests on: a best round inside it is indistinguishable from the raw one.
  band?: {from: number; to: number};
  inside: boolean;
  value: string;
  verdict?: string;
}

export interface ComparisonPanel {
  broker: Broker;
  provenance: string;
  zero: number;
  ticks: Tick[];
  rows: {scenario: string; unit: string; lanes: Lane[]}[];
}

// The narrowest axis a panel draws, in percent either side of the raw client: a difference of
// a fraction of a percent is not drawn as if it filled the plot.
const MIN_SPAN = 2;

interface Extent {
  loop: Loop;
  measurement: Measurement;
  from: number;
  to: number;
  best: number;
  median?: number;
  band?: number;
}

// How far below the raw client's figure a loop falls, in percent: the overhead the page prints.
function extent(loop: Loop, measurement: Measurement, raw: Measurement): Extent {
  const reference = figure(raw)!;
  const below = (value: number): number => ((reference - value) / reference) * 100;
  const third = typeof measurement.best === 'number';
  const fastest = third ? measurement.best! : measurement.max ?? measurement.median!;
  const slowest = third
    ? measurement.worst ?? measurement.best!
    : measurement.min ?? measurement.median!;
  const ours = spread(measurement);
  const theirs = spread(raw);
  return {
    loop,
    measurement,
    from: below(fastest),
    to: below(slowest),
    best: below(figure(measurement)!),
    median:
      third && typeof measurement.median === 'number' ? below(measurement.median) : undefined,
    band:
      loop === 'raw' || ours === null || theirs === null
        ? undefined
        : (Math.max(Math.abs(ours), Math.abs(theirs)) / reference) * 100,
  };
}

/** Per broker, each scenario's three loops placed against the raw client's figure. */
export function comparisonPanels(
  published: Loaded[],
  lang: string,
  labels: Labels,
): ComparisonPanel[] {
  return published
    .filter((entry) => entry.results?.scenarios?.length)
    .map((entry) => {
      const scenarios = entry.results!.scenarios!.filter((scenario) => figure(scenario.raw));
      const extents = scenarios.map((scenario) =>
        (['raw', 'adapter', 'framework'] as const)
          .filter((loop) => scenario[loop] && figure(scenario[loop]) !== undefined)
          .map((loop) => extent(loop, scenario[loop]!, scenario.raw!)),
      );
      const reach = extents
        .flat()
        .flatMap((lane) => [lane.from, lane.to, lane.band ?? 0, -(lane.band ?? 0)]);
      const axis = niceAxis(
        Math.min(-MIN_SPAN, ...reach),
        Math.max(MIN_SPAN, ...reach),
        (value) => `${value > 0 ? '+' : ''}${value.toLocaleString(lang)}%`,
      );
      return {
        broker: entry.broker,
        provenance: provenanceLine(entry, labels),
        zero: axis.place(0),
        ticks: axis.ticks,
        rows: scenarios.map((scenario, index) => ({
          scenario: scenario.name,
          unit: scenario.unit,
          lanes: extents[index]!.map((lane): Lane => {
            const verdict =
              lane.loop === 'framework'
                ? overheadCell(scenario, labels)
                : lane.loop === 'adapter'
                  ? adapterVerdict(scenario, labels)
                  : undefined;
            const inside =
              lane.loop === 'framework'
                ? scenario.verdict === 'indistinguishable'
                : lane.loop === 'adapter' && verdict === labels.indistinguishable;
            return {
              loop: lane.loop,
              from: axis.place(lane.from),
              to: axis.place(lane.to),
              best: axis.place(lane.best),
              median: lane.median === undefined ? undefined : axis.place(lane.median),
              band:
                lane.band === undefined
                  ? undefined
                  : {from: axis.place(-lane.band), to: axis.place(lane.band)},
              inside,
              value: sideCell(lane.measurement, scenario.unit, lang),
              verdict,
            };
          }),
        })),
      };
    });
}

export interface CodeBar {
  value: string;
  width: number;
}

export interface CodePanel {
  broker: Broker;
  machine: string;
  instructions: Tick[];
  allocations: Tick[];
  rows: {scenario: string; instructions: CodeBar; allocations: CodeBar; cold: string}[];
}

/**
 * Per crate, the instructions and the allocations of each scenario as bars. Each crate has an
 * axis of its own: a broker crate counts its client's work over a real transport, so its
 * number does not compare with another crate's.
 */
export function codePanels(published: Loaded[], lang: string, labels: Labels): CodePanel[] {
  const compact = new Intl.NumberFormat(lang, {notation: 'compact'});
  const label = (value: number): string => compact.format(value);
  return coreFirst(published)
    .filter((entry) => entry.results?.code?.length)
    .map((entry) => {
      const code = entry.results!.code!;
      const measure = (pick: (scenario: CodeScenario) => number | undefined) => {
        // Three ticks: a measure shares a narrow column with its neighbour and its values.
        const top = Math.max(0, ...code.map((scenario) => pick(scenario) ?? 0));
        const axis = niceAxis(0, top, label, 3);
        const bar = (scenario: CodeScenario): CodeBar => {
          const value = pick(scenario);
          const width = typeof value === 'number' ? axis.place(value) : 0;
          return {value: number(value, lang), width};
        };
        return {ticks: axis.ticks, bar};
      };
      const instructions = measure((scenario) => scenario.framework.instructions);
      const allocations = measure((scenario) => scenario.framework.allocations);
      return {
        broker: entry.broker,
        machine: machineLine(entry, labels),
        instructions: instructions.ticks,
        allocations: allocations.ticks,
        rows: code.map((scenario) => ({
          scenario: scenario.name,
          instructions: instructions.bar(scenario),
          allocations: allocations.bar(scenario),
          cold: coldCell(scenario, lang),
        })),
      };
    });
}
