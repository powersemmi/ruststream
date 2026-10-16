import fs from 'node:fs';
import path from 'node:path';
import {describe, expect, it} from 'vitest';
import {
  CORE,
  BROKERS,
  adapterCell,
  adapterVerdict,
  codePanels,
  codeRows,
  comparisonPanels,
  niceAxis,
  machineLine,
  overheadCell,
  pendingLine,
  provenanceLine,
  sideCell,
  type Labels,
  type Loaded,
  type Results,
  type Scenario,
} from './model';

const labels: Labels = {
  loading: 'Loading published results...',
  broker: 'Broker',
  scenario: 'Scenario',
  raw: 'Raw client',
  adapter: 'Broker crate',
  framework: 'RustStream',
  overhead: 'Overhead',
  against: '({percent} over the client)',
  indistinguishable: 'indistinguishable',
  brokerBound: 'broker-bound',
  measured: 'measured',
  details: 'Full results and methodology',
  pending: 'No results published yet: {brokers}.',
  crate: 'Crate',
  instructions: 'Instructions',
  allocations: 'Allocations',
  cold: 'Cold start',
  table: 'Show as a table',
  axis: "Throughput below the raw client's best round",
  rounds: 'rounds, best to slowest',
  best: 'best round',
  median: 'median round',
  noise: 'spread of the rounds',
};

const core = JSON.parse(
  fs.readFileSync(path.resolve(__dirname, '../../../../docs/benchmarks/results.json'), 'utf8'),
) as Results;

const scenario = (fields: Partial<Scenario>): Scenario => ({
  name: 'publish',
  unit: 'msg/s',
  raw: {best: 1000, median: 900, worst: 800},
  framework: {best: 950, median: 850, worst: 700},
  overhead_percent: 5,
  verdict: 'measured',
  ...fields,
});

describe('a measurement cell', () => {
  it('shows the best round with the median round in parentheses', () => {
    expect(sideCell({best: 12345.67, median: 12000, worst: 11000}, 'msg/s', 'en')).toBe(
      '12,345.7 msg/s (12,000)',
    );
  });

  it('falls back to the worst round when the document has no median', () => {
    expect(sideCell({best: 10, worst: 8}, 'msg/s', 'en')).toBe('10 msg/s (8)');
  });

  it('shows a first-schema median with its range', () => {
    expect(sideCell({median: 10, min: 9, max: 11}, 'msg/s', 'en')).toBe('10 msg/s (9-11)');
  });

  it('is a dash when the document carries no measurement', () => {
    expect(sideCell(undefined, 'msg/s', 'en')).toBe('-');
  });
});

describe('the overhead cell', () => {
  it('signs a measured difference', () => {
    expect(overheadCell(scenario({}), labels)).toBe('+5%');
  });

  it('names a difference inside the spread instead of printing it', () => {
    expect(overheadCell(scenario({verdict: 'indistinguishable', broker_bound: true}), labels)).toBe(
      'indistinguishable (broker-bound)',
    );
  });
});

describe('the broker crate cell', () => {
  it('is empty when the crate measured only the two ends', () => {
    expect(adapterCell(scenario({}), 'en', labels)).toBe('-');
  });

  it('carries the crate overhead the document states', () => {
    const row = scenario({
      adapter: {best: 980, median: 870, worst: 860},
      adapter_overhead_percent: 2,
      adapter_verdict: 'measured',
    });
    expect(adapterCell(row, 'en', labels)).toBe('980 msg/s (870) (+2% over the client)');
  });

  it('applies the spread rule when the document left the verdict out', () => {
    const row = scenario({
      raw: {best: 1000, median: 900, worst: 800},
      adapter: {best: 990, median: 900, worst: 850},
      adapter_overhead_percent: 1,
    });
    expect(adapterCell(row, 'en', labels)).toBe(
      '990 msg/s (900) (indistinguishable over the client)',
    );
  });
});

describe('the cost of the code', () => {
  const loaded: Loaded[] = [{broker: CORE, results: core}];

  it('names the crate on its first row only', () => {
    const rows = codeRows(loaded, 'en');
    expect(rows[0]!.crate).toBe('RustStream');
    expect(rows[0]!.instructions).toBe('1,673');
    expect(rows[0]!.cold).toBe('15,079 / 26');
    expect(rows[1]!.crate).toBe('');
  });

  it('names the machine and the core release once for the core', () => {
    const line = machineLine(loaded[0]!, labels);
    expect(line).toContain('AMD Ryzen 9 7900X 12-Core Processor');
    expect(line).toContain('ruststream 0.7.0, measured 2026-10-06.');
    expect(line.match(/ruststream 0\.7\.0/g)).toHaveLength(1);
  });
});

describe('the catalog', () => {
  it('lists every broker without results as pending', () => {
    const loaded: Loaded[] = BROKERS.slice(0, 2).map((broker) => ({broker, results: null}));
    expect(pendingLine(loaded, labels)).toBe('No results published yet: NATS, Redis.');
  });

  it('dates a published comparison in its provenance line', () => {
    const results = {...core, scenarios: [scenario({})], crate: 'ruststream-nats'};
    expect(provenanceLine({broker: BROKERS[0]!, results}, labels)).toContain(
      'ruststream-nats 0.7.0 on ruststream 0.7.0',
    );
  });
});

describe('an axis', () => {
  it('rounds the domain out to a step of one, two or five', () => {
    const axis = niceAxis(0, 17402.8, (value) => String(value));
    expect(axis.ticks.map((tick) => tick.value)).toEqual([0, 5000, 10000, 15000, 20000]);
    expect(axis.ticks.map((tick) => tick.position)).toEqual([0, 25, 50, 75, 100]);
    expect(axis.place(17402.8)).toBeCloseTo(87.01, 2);
  });

  it('spans both signs when the values do', () => {
    const axis = niceAxis(-0.9, 27.4, (value) => String(value));
    expect(axis.ticks[0]!.value).toBe(-10);
    expect(axis.ticks.at(-1)!.value).toBe(30);
    expect(axis.place(0)).toBe(25);
  });

  it('keeps a flat set of values drawable', () => {
    const axis = niceAxis(0, 0, (value) => String(value));
    expect(axis.ticks.at(-1)!.value).toBeGreaterThan(0);
    expect(axis.place(0)).toBe(0);
  });
});

// A scenario as ruststream-amqp published it, and one as ruststream-rdkafka did.
const amqp: Scenario = {
  name: 'queue, 512 B JSON, accept each',
  unit: 'msg/s',
  raw: {best: 167723, median: 164801, worst: 161723},
  adapter: {best: 164367, median: 163325, worst: 159164},
  framework: {best: 163903, median: 161911, worst: 154002},
  overhead_percent: 2.3,
  adapter_overhead_percent: 2.0,
  verdict: 'indistinguishable',
  adapter_verdict: 'indistinguishable',
  broker_bound: false,
};
const kafka: Scenario = {
  name: 'consumer group, auto-commit, 512 B JSON',
  unit: 'msg/s',
  raw: {best: 1495104, median: 1481752, worst: 1273707},
  adapter: {best: 1356061, median: 1345987, worst: 1172435},
  framework: {best: 1193123, median: 1190254, worst: 1087504},
  overhead_percent: 20.2,
  adapter_overhead_percent: 9.3,
  verdict: 'measured',
  adapter_verdict: 'indistinguishable',
};
const published = (...scenarios: Scenario[]): Loaded => ({
  broker: BROKERS[4]!,
  results: {...core, crate: 'ruststream-amqp', scenarios},
});

describe('the comparison chart', () => {
  it('draws the raw client, the crate and the runtime in that order', () => {
    const [panel] = comparisonPanels([published(amqp)], 'en', labels);
    expect(panel!.rows[0]!.lanes.map((lane) => lane.loop)).toEqual(['raw', 'adapter', 'framework']);
  });

  it('places each loop by how far it falls below the raw best round', () => {
    const [panel] = comparisonPanels([published(kafka)], 'en', labels);
    const [raw, , framework] = panel!.rows[0]!.lanes;
    // The raw best round is the zero of the axis, and the runtime sits 20.2% to its right.
    expect(raw!.best).toBe(panel!.zero);
    const axis = panel!.ticks;
    const percent = (position: number): number =>
      axis[0]!.value +
      ((axis.at(-1)!.value - axis[0]!.value) * position) / 100;
    expect(percent(framework!.best)).toBeCloseTo(20.2, 1);
    expect(percent(framework!.to)).toBeCloseTo(27.26, 1);
  });

  it('draws the spread a verdict rests on around the zero of each compared loop', () => {
    const [panel] = comparisonPanels([published(amqp)], 'en', labels);
    const framework = panel!.rows[0]!.lanes[2]!;
    const width = panel!.ticks.at(-1)!.value - panel!.ticks[0]!.value;
    // The wider of the two spreads: the runtime's 9,901 msg/s over a raw best of 167,723.
    expect(((framework.band!.to - framework.band!.from) * width) / 200).toBeCloseTo(5.9, 1);
  });

  it('keeps the verdict of the document', () => {
    const [panel] = comparisonPanels([published(amqp, kafka)], 'en', labels);
    const [quiet, loud] = panel!.rows;
    expect(quiet!.lanes[2]).toMatchObject({inside: true, verdict: 'indistinguishable'});
    expect(loud!.lanes[2]).toMatchObject({inside: false, verdict: '+20.2%'});
    expect(loud!.lanes[1]).toMatchObject({inside: true, verdict: 'indistinguishable'});
  });

  it('leaves the crate lane out when the crate measured only the two ends', () => {
    const [panel] = comparisonPanels([published({...amqp, adapter: undefined})], 'en', labels);
    expect(panel!.rows[0]!.lanes.map((lane) => lane.loop)).toEqual(['raw', 'framework']);
  });

  it('does not magnify a difference of a fraction of a percent', () => {
    const flat: Scenario = {
      ...amqp,
      raw: {best: 1000, median: 1000, worst: 999},
      adapter: {best: 999, median: 999, worst: 999},
      framework: {best: 1000, median: 999, worst: 998},
    };
    const [panel] = comparisonPanels([published(flat)], 'en', labels);
    expect(panel!.ticks[0]!.value).toBeLessThanOrEqual(-2);
    expect(panel!.ticks.at(-1)!.value).toBeGreaterThanOrEqual(2);
  });

  it('reads a first-schema median with its range', () => {
    const first: Scenario = {
      ...amqp,
      raw: {median: 100, min: 90, max: 110},
      adapter: undefined,
      framework: {median: 95, min: 85, max: 100},
    };
    const [panel] = comparisonPanels([published(first)], 'en', labels);
    const framework = panel!.rows[0]!.lanes[1]!;
    expect(framework.from).toBeLessThan(framework.best);
    expect(framework.to).toBeGreaterThan(framework.best);
    expect(framework.median).toBeUndefined();
  });
});

describe('the crate verdict', () => {
  it('is the overhead the document states, or its absence', () => {
    expect(adapterVerdict(kafka, labels)).toBe('indistinguishable');
    expect(adapterVerdict({...kafka, adapter_verdict: 'measured'}, labels)).toBe('+9.3%');
    expect(adapterVerdict({...kafka, adapter: undefined}, labels)).toBe('-');
  });
});

describe('the cost chart', () => {
  const nats: Loaded = {
    broker: BROKERS[0]!,
    results: {
      ...core,
      crate: 'ruststream-nats',
      code: [{name: 'pull', framework: {instructions: 9897.4, allocations: 7.1}}],
    },
  };

  it('leads with the core however the documents arrive', () => {
    const panels = codePanels([nats, {broker: CORE, results: core}], 'en', labels);
    expect(panels.map((panel) => panel.broker.name)).toEqual(['RustStream', 'NATS']);
  });

  it('scales each crate on its own axis', () => {
    const [ours] = codePanels([{broker: CORE, results: core}], 'en', labels);
    const decode = ours!.rows[1]!;
    expect(decode.instructions.value).toBe('17,402.8');
    expect(decode.instructions.width).toBeCloseTo(87.01, 2);
    expect(ours!.instructions.at(-1)!.label).toBe('20K');
  });

  it('draws no bar for a message that allocates nothing', () => {
    const [ours] = codePanels([{broker: CORE, results: core}], 'en', labels);
    expect(ours!.rows[0]!.allocations).toEqual({value: '0', width: 0});
    expect(ours!.rows[0]!.cold).toBe('15,079 / 26');
  });

  it('drops a crate with no code section', () => {
    const empty: Loaded = {broker: BROKERS[1]!, results: null};
    expect(codePanels([empty], 'en', labels)).toEqual([]);
  });
});
