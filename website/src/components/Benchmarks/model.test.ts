import fs from 'node:fs';
import path from 'node:path';
import {describe, expect, it} from 'vitest';
import {
  CORE,
  BROKERS,
  adapterCell,
  codeRows,
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
