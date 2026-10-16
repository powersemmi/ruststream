import {describe, expect, it} from 'vitest';
import {exampleSource} from './source';

describe('exampleSource', () => {
  it('drops the snippet markers and the blank lines they leave behind', () => {
    const file = [
      'use serde::Deserialize;',
      '',
      '// --8<-- [start:handler]',
      'struct Order;',
      '// --8<-- [end:handler]',
      '',
      '// --8<-- [start:app]',
      'fn app() {}',
      '// --8<-- [end:app]',
      '',
    ].join('\n');
    expect(exampleSource(file)).toBe(
      ['use serde::Deserialize;', '', 'struct Order;', '', 'fn app() {}'].join('\n'),
    );
  });

  it('keeps a comment that only mentions the marker syntax', () => {
    expect(exampleSource('let s = "--8<--";\n')).toBe('let s = "--8<--";');
  });
});
