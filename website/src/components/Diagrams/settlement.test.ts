import {expect, it} from 'vitest';
import {settlement} from './settlement';
import type {Words} from './words';

// Every word is its own key, so the test reads the layout and never a page's wording.
const words = new Proxy({}, {get: (_, key) => String(key)}) as Words;

it('decides on the attempt cap before a retry goes anywhere', () => {
  const {links} = settlement(words);
  const from = (id: string): string[] =>
    links.filter((link) => link.from === id).map((link) => link.to);
  expect(from('retry')).toEqual(['capCheck']);
  expect(from('retryAfter')).toEqual(['capCheck']);
  expect(from('capCheck')).toEqual(['belowCap', 'deadLetter', 'reject']);
  // The answers are alternatives: none of them leads on to another.
  expect([...from('belowCap'), ...from('deadLetter'), ...from('reject')]).toEqual([]);
});
