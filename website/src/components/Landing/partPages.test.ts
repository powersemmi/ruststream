import {existsSync} from 'node:fs';
import path from 'node:path';
import {describe, expect, it} from 'vitest';
import {PART_PAGES, docFor} from './partPages';

const docs = path.resolve(__dirname, '../../../../docs');

describe('PART_PAGES', () => {
  it('sends every part of the diagram to a page the docs tree holds', () => {
    for (const [part, page] of Object.entries(PART_PAGES)) {
      const found = docFor(page).some((file) => existsSync(path.join(docs, file)));
      expect(found, `${part} -> ${page}`).toBe(true);
    }
  });
});

describe('docFor', () => {
  it('reads a route as a page or as the index of its category', () => {
    expect(docFor('/components/subscribers/')).toEqual([
      'components/subscribers.md',
      'components/subscribers/index.md',
    ]);
  });
});
