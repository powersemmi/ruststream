import {describe, expect, it} from 'vitest';
import published from '../../../../docs/benchmarks/results.json';
import {HEADLINE_SCENARIOS, selectNumbers, type Results} from './numbers';

const fixture: Results = {
  crate_version: '0.7.0',
  core_version: '0.7.0',
  measured_at: '2026-10-06',
  environment: {cpu: 'AMD Ryzen 9 7900X 12-Core Processor'},
  code: [
    {
      name: 'consume, JSON decode into a small struct',
      framework: {instructions: 1673.0, allocations: 0.0},
    },
    {name: 'consume on the byte lane, no codec', framework: {instructions: 826.4, allocations: 0}},
    {
      name: 'consume through a middleware stack of four',
      framework: {instructions: 1768.0, allocations: 0.0},
    },
    {
      name: 'reply to a transport that reads the payload',
      framework: {instructions: 12106.6, allocations: 0.016},
    },
    {name: 'request and reply, one round trip', framework: {instructions: 9361.6, allocations: 13}},
  ],
};

describe('selectNumbers', () => {
  it('formats the headline scenarios in their order, whole instructions and exact allocations', () => {
    const numbers = selectNumbers(fixture, 'en');
    expect(numbers.rows).toEqual([
      {scenario: HEADLINE_SCENARIOS[0], instructions: '1,673', allocations: '0'},
      {scenario: HEADLINE_SCENARIOS[1], instructions: '826', allocations: '0'},
      {scenario: HEADLINE_SCENARIOS[2], instructions: '1,768', allocations: '0'},
      {scenario: HEADLINE_SCENARIOS[3], instructions: '12,107', allocations: '0.016'},
    ]);
    expect(numbers).toMatchObject({
      version: '0.7.0',
      measuredAt: '2026-10-06',
      cpu: 'AMD Ryzen 9 7900X 12-Core Processor',
    });
  });

  it('groups digits the way the reader locale does', () => {
    expect(selectNumbers(fixture, 'ru').rows[3]!.instructions).toBe('12 107');
    expect(selectNumbers(fixture, 'zh-CN').rows[3]!.allocations).toBe('0.016');
  });

  it('fails the build when a headline scenario is missing from the results', () => {
    const trimmed = {...fixture, code: fixture.code.slice(1)};
    expect(() => selectNumbers(trimmed, 'en')).toThrow(/consume, JSON decode into a small struct/);
  });

  it('reads the results the site publishes', () => {
    expect(selectNumbers(published as Results, 'en').rows).toHaveLength(HEADLINE_SCENARIOS.length);
  });
});
